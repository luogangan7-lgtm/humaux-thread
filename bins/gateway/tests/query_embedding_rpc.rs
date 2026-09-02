//! ADR-0012 query-embedding RPC boundary test — spawns the real
//! `humaux-retrieval-worker` axum app (`humaux_retrieval_worker::rpc`) in-process on a
//! temporary Unix domain socket and drives it through the real
//! `humaux_gateway::retrieval_embedding_client::GatewayRetrievalEmbeddingClient`.
//!
//! A separate-binary (two real OS processes) test is a later card; this proves the wire
//! protocol, the peer-credential authentication ordering, and the DB-backed idempotency
//! contract without process-spawn plumbing.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use sha2::Digest;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use uuid::Uuid;

use humaux_adapters::postgres::{RetrievalWorkerDbPool, RuntimeDbPool};
use humaux_adapters::retrieval_embedding_rpc::{
    GatewayRetrievalEmbeddingRegistrations, RegisterCall,
};
use humaux_application::retrieval_embedding_port::{
    RetrievalEmbeddingInput, RetrievalEmbeddingOutcome, RetrievalEmbeddingPort,
};
use humaux_domain::error::ErrorCode;
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId};
use humaux_domain::ids::{TenantId, UserId, WorkspaceId};
use humaux_gateway::retrieval_embedding_client::GatewayRetrievalEmbeddingClient;
use humaux_infra_cell::{
    CallerId, CellId, IntraCellResource, IntraCellResourceRegistry, ResourceEntry,
};
use humaux_local_secret_scan::{LocalSecretScanner, LocalSecretScannerConfig};
use humaux_local_secret_scan::{SealedRetrievalCard, SealedRetrievalQuery};
use humaux_retrieval_provider::adapters::TestDoubleProvider;
use humaux_retrieval_provider::contract::{
    CalibrationProfileId, EmbeddingBatch, EmbeddingModelDescriptor, EmbeddingProvider, ModelId,
    RerankModelDescriptor, RerankScoreSemantics, RetrievalQueryCallContext,
};
use humaux_retrieval_worker::rpc::{PeerIdentity, RpcState, router};

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing required test env {name}"))
}

/// `std::env::temp_dir()` (a long per-user `/var/folders/...` path on macOS) plus a
/// descriptive name overflows `sockaddr_un`'s ~104-byte path limit — `/tmp` directly with a
/// short random suffix stays well under it on every platform this test runs on.
fn temp_socket_path(tag: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("/tmp/hq-{tag}-{}.sock", Uuid::now_v7().simple()))
}

/// Learns this test process's own real uid via a local self-connected socket pair's peer
/// credential — no `libc`/`nix` dependency needed just for one integer.
async fn own_uid() -> u32 {
    let path = temp_socket_path("probe");
    let listener = UnixListener::bind(&path).expect("bind uid probe socket");
    let client = UnixStream::connect(&path).await.expect("connect uid probe");
    let (server_side, _) = listener.accept().await.expect("accept uid probe");
    let uid = server_side.peer_cred().expect("peer credential").uid();
    drop(client);
    drop(server_side);
    let _ = std::fs::remove_file(&path);
    uid
}

fn scanner() -> LocalSecretScanner {
    LocalSecretScanner::new(LocalSecretScannerConfig {
        executable: required("HUMAUX_TEST_GITLEAKS_BIN").into(),
        expected_version: required("HUMAUX_TEST_GITLEAKS_VERSION"),
        expected_executable_sha256: required("HUMAUX_TEST_GITLEAKS_SHA256"),
        timeout: Duration::from_secs(5),
        max_payload_bytes: 64 * 1024,
        finding_exit_code: 1,
    })
    .expect("valid scanner config")
}

fn embedding_model() -> EmbeddingModelDescriptor {
    EmbeddingModelDescriptor {
        model_id: ModelId("test-embedding-model".to_owned()),
        model_revision: "v1".to_owned(),
        dimension_options: vec![4],
        max_input_tokens: 1_000,
        batch_supported: true,
        dense_supported: true,
        sparse_supported: false,
    }
}

fn rerank_model() -> RerankModelDescriptor {
    RerankModelDescriptor {
        model_id: ModelId("unused-rerank-model".to_owned()),
        model_revision: "v1".to_owned(),
        max_documents: 10,
        max_input_tokens: 1_000,
        score_semantics: RerankScoreSemantics::RawLogit,
        calibration_profile: CalibrationProfileId("unused".to_owned()),
    }
}

/// Wraps [`TestDoubleProvider`] with a call counter — proves the idempotency test's "provider
/// called once" half; `TestDoubleProvider` itself does no network/DB I/O, so a counter is the
/// only way to observe how many times the real embedding call actually ran.
struct CountingEmbedder {
    inner: TestDoubleProvider,
    calls: AtomicUsize,
}

#[async_trait]
impl EmbeddingProvider for CountingEmbedder {
    fn model(&self) -> &EmbeddingModelDescriptor {
        self.inner.model()
    }

    async fn embed_queries(
        &self,
        context: &RetrievalQueryCallContext<'_>,
        dimension: u32,
        queries: &[SealedRetrievalQuery],
    ) -> Result<EmbeddingBatch, ErrorCode> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.embed_queries(context, dimension, queries).await
    }

    async fn embed_cards(
        &self,
        tenant_id: TenantId,
        dimension: u32,
        cards: &[SealedRetrievalCard],
    ) -> Result<EmbeddingBatch, ErrorCode> {
        self.inner.embed_cards(tenant_id, dimension, cards).await
    }
}

async fn spawn_worker(expected_gateway_uid: u32, embedder: Arc<dyn EmbeddingProvider>) -> String {
    let socket_path = temp_socket_path("worker");
    let calls = RetrievalWorkerDbPool::connect(&required("HUMAUX_RETRIEVAL_WORKER_PG_DSN"))
        .await
        .expect("retrieval worker db pool");
    let state = Arc::new(RpcState {
        expected_gateway_uid,
        calls,
        scanner: Arc::new(scanner()),
        embedder,
        dimension: 4,
        provider_id: "test-provider".to_owned(),
    });
    let listener = UnixListener::bind(&socket_path).expect("bind worker rpc socket");
    let app = router(state);
    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<PeerIdentity>(),
        )
        .await;
    });
    // Give the spawned server a moment to start accepting.
    tokio::time::sleep(Duration::from_millis(50)).await;
    socket_path.to_string_lossy().into_owned()
}

fn cell_registry() -> IntraCellResourceRegistry {
    let cell_id = CellId(Uuid::now_v7());
    let caller = CallerId("gateway-test".to_owned());
    let mut entries = BTreeMap::new();
    entries.insert(
        IntraCellResource::RETRIEVAL_EMBEDDING_RPC,
        ResourceEntry::new(
            "unix-socket",
            0,
            cell_id,
            vec![],
            BTreeSet::from([caller.clone()]),
            false,
        )
        .expect("valid resource entry"),
    );
    IntraCellResourceRegistry::new(entries, cell_id, caller)
}

fn authorization(workspace_id: WorkspaceId) -> AuthorizationScope {
    AuthorizationScope::new(
        TenantId::new(),
        PrincipalId::new(),
        Some(UserId::new()),
        BoundedSet::new([workspace_id]).expect("bounded workspace set"),
    )
}

fn deadline_unix_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_millis(),
    )
    .expect("deadline fits i64")
        + 30_000
}

fn profile_fingerprint() -> String {
    format!("sha256:{}", "a".repeat(64))
}

async fn runtime_pool() -> Arc<RuntimeDbPool> {
    Arc::new(
        RuntimeDbPool::connect(&required("HUMAUX_GATEWAY_PG_DSN"))
            .await
            .expect("runtime db pool"),
    )
}

#[tokio::test]
async fn happy_path_embeds_through_the_real_worker_app() {
    let uid = own_uid().await;
    let embedder: Arc<dyn EmbeddingProvider> = Arc::new(CountingEmbedder {
        inner: TestDoubleProvider::new(embedding_model(), rerank_model()),
        calls: AtomicUsize::new(0),
    });
    let socket_path = spawn_worker(uid, embedder).await;
    let client = GatewayRetrievalEmbeddingClient::new(
        runtime_pool().await,
        socket_path,
        cell_registry(),
        Duration::from_secs(30),
    );
    let workspace_id = WorkspaceId::new();
    let authorization = authorization(workspace_id);
    let fingerprint = profile_fingerprint();
    let outcome = client
        .embed_query(RetrievalEmbeddingInput {
            authorization: &authorization,
            workspace_id,
            request_id: Uuid::now_v7(),
            logical_call_id: Uuid::now_v7(),
            attempt_no: 1,
            profile_fingerprint: &fingerprint,
            dimension: 4,
            query: "find the frozen contract",
            deadline_unix_ms: deadline_unix_ms(),
        })
        .await
        .expect("happy path embeds");
    match outcome {
        RetrievalEmbeddingOutcome::Embedded {
            vector,
            provider_id,
            model_id,
            dimension,
            ..
        } => {
            assert_eq!(vector.len(), 4);
            assert_eq!(provider_id, "test-provider");
            assert_eq!(model_id, "test-embedding-model");
            assert_eq!(dimension, 4);
        }
        other => panic!("expected Embedded, got {other:?}"),
    }
}

#[tokio::test]
async fn wrong_uid_is_rejected_before_the_body_is_read() {
    let uid = own_uid().await;
    // Deliberately wrong expected uid — the worker must 403 the connection before its JSON
    // extractor ever runs, and must never claim the registration row.
    let wrong_uid = uid.wrapping_add(1);
    let embedder: Arc<dyn EmbeddingProvider> =
        Arc::new(TestDoubleProvider::new(embedding_model(), rerank_model()));
    let socket_path = spawn_worker(wrong_uid, embedder).await;
    let client = GatewayRetrievalEmbeddingClient::new(
        runtime_pool().await,
        socket_path,
        cell_registry(),
        Duration::from_secs(30),
    );
    let workspace_id = WorkspaceId::new();
    let authorization = authorization(workspace_id);
    let fingerprint = profile_fingerprint();
    let result = client
        .embed_query(RetrievalEmbeddingInput {
            authorization: &authorization,
            workspace_id,
            request_id: Uuid::now_v7(),
            logical_call_id: Uuid::now_v7(),
            attempt_no: 1,
            profile_fingerprint: &fingerprint,
            dimension: 4,
            query: "this must never reach the embedder",
            deadline_unix_ms: deadline_unix_ms(),
        })
        .await;
    assert!(
        matches!(result, Ok(RetrievalEmbeddingOutcome::Unavailable { .. })),
        "wrong uid must degrade the dense lane, not error the caller, got {result:?}"
    );
}

/// Raw single request/response over the worker's UDS, hand-rolled HTTP/1.1 (no `hyper`/
/// `reqwest` — G80-3 confines those crates to `crates/infra-network`) — duplicated from
/// `retrieval_embedding_client.rs`'s private `dial` on purpose: this test needs to drive the
/// *same* `call_id` twice, which the real port (mints a fresh `call_id` every call) cannot do.
async fn raw_rpc_call(
    socket_path: &str,
    call_id: Uuid,
    tenant_hint: Uuid,
    query: &str,
) -> serde_json::Value {
    let mut stream = UnixStream::connect(socket_path)
        .await
        .expect("connect worker socket");
    let body = serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "call_id": call_id,
        "tenant_hint": tenant_hint,
        "query": query,
    }))
    .expect("serialize wire request");
    let head = format!(
        "POST /internal/v1/retrieval/query-embedding HTTP/1.1\r\n\
         Host: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(head.as_bytes())
        .await
        .expect("write request head");
    stream.write_all(&body).await.expect("write request body");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let header_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response has header terminator");
    let status_line = std::str::from_utf8(&raw[..header_end])
        .expect("utf8 headers")
        .lines()
        .next()
        .expect("status line");
    assert!(
        status_line.contains(" 200 "),
        "raw rpc call must succeed: {status_line}"
    );
    serde_json::from_slice(&raw[header_end + 4..]).expect("valid json envelope")
}

#[tokio::test]
async fn same_call_id_twice_calls_the_provider_once() {
    let uid = own_uid().await;
    let counting = Arc::new(CountingEmbedder {
        inner: TestDoubleProvider::new(embedding_model(), rerank_model()),
        calls: AtomicUsize::new(0),
    });
    let embedder: Arc<dyn EmbeddingProvider> = counting.clone();
    let socket_path = spawn_worker(uid, embedder).await;

    let pool = runtime_pool().await;
    let call_id = Uuid::now_v7();
    let tenant_id = TenantId::new();
    let query = "idempotent replay query";
    let query_sha256: [u8; 32] = sha2::Sha256::digest(query.as_bytes()).into();
    let registered_call_id = GatewayRetrievalEmbeddingRegistrations::new(&pool)
        .register(&RegisterCall {
            call_id,
            tenant_id: tenant_id.0,
            principal_id: Uuid::now_v7(),
            user_id: Uuid::now_v7(),
            workspace_id: Uuid::now_v7(),
            request_id: Uuid::now_v7(),
            logical_call_id: Uuid::now_v7(),
            attempt_no: 1,
            profile_fingerprint: profile_fingerprint(),
            query_sha256,
            ttl: Duration::from_secs(30),
        })
        .await
        .expect("register call");
    assert_eq!(
        registered_call_id, call_id,
        "fresh registration keeps its own call_id"
    );

    let first = raw_rpc_call(&socket_path, call_id, tenant_id.0, query).await;
    assert_eq!(first["outcome"], "EMBEDDED", "first call: {first}");
    let second = raw_rpc_call(&socket_path, call_id, tenant_id.0, query).await;
    assert_eq!(second["outcome"], "EMBEDDED", "replay: {second}");
    assert_eq!(
        first["vector"], second["vector"],
        "replay must return the stored vector"
    );
    assert_eq!(
        counting.calls.load(Ordering::SeqCst),
        1,
        "duplicate call_id must not call the provider twice"
    );
}

/// Fault-injection for the replay-integrity gap in
/// `RetrievalWorkerEmbeddingCalls::load_and_claim`: a COMPLETED row replayed with a
/// `query_sha256` that no longer matches the stored one must 409 (ADR-0012 §2 "query mismatch
/// -> 409, zero side effects"), never silently hand back the stale vector under
/// `outcome: "EMBEDDED"`. Red before the fix (the `state == "COMPLETED"` branch returned the
/// replay before the sha256 comparison ever ran); green now that the comparison gates every
/// branch.
#[tokio::test]
async fn completed_replay_with_mismatched_query_is_rejected_not_served_stale() {
    let uid = own_uid().await;
    let embedder: Arc<dyn EmbeddingProvider> =
        Arc::new(TestDoubleProvider::new(embedding_model(), rerank_model()));
    let socket_path = spawn_worker(uid, embedder).await;

    let pool = runtime_pool().await;
    let call_id = Uuid::now_v7();
    let tenant_id = TenantId::new();
    let original_query = "original query that gets embedded and completed";
    let query_sha256: [u8; 32] = sha2::Sha256::digest(original_query.as_bytes()).into();
    GatewayRetrievalEmbeddingRegistrations::new(&pool)
        .register(&RegisterCall {
            call_id,
            tenant_id: tenant_id.0,
            principal_id: Uuid::now_v7(),
            user_id: Uuid::now_v7(),
            workspace_id: Uuid::now_v7(),
            request_id: Uuid::now_v7(),
            logical_call_id: Uuid::now_v7(),
            attempt_no: 1,
            profile_fingerprint: profile_fingerprint(),
            query_sha256,
            ttl: Duration::from_secs(30),
        })
        .await
        .expect("register call");

    let completed = raw_rpc_call(&socket_path, call_id, tenant_id.0, original_query).await;
    assert_eq!(completed["outcome"], "EMBEDDED", "first call: {completed}");

    // Same call_id, a different query — a mismatch must 409 even though the row is now
    // COMPLETED, not fall into the replay branch and serve the original query's vector back
    // under this different query.
    let mismatched_query = "a completely different query, same call_id";
    let mut stream = UnixStream::connect(&socket_path)
        .await
        .expect("connect worker socket");
    let body = serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "call_id": call_id,
        "tenant_hint": tenant_id.0,
        "query": mismatched_query,
    }))
    .expect("serialize wire request");
    let head = format!(
        "POST /internal/v1/retrieval/query-embedding HTTP/1.1\r\n\
         Host: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(head.as_bytes())
        .await
        .expect("write request head");
    stream.write_all(&body).await.expect("write request body");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let header_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response has header terminator");
    let status_line = std::str::from_utf8(&raw[..header_end])
        .expect("utf8 headers")
        .lines()
        .next()
        .expect("status line");
    assert!(
        status_line.contains(" 409 "),
        "a completed row replayed with a mismatched query must 409, got: {status_line}"
    );
}

#[tokio::test]
async fn worker_down_returns_a_typed_error_not_a_panic() {
    let socket_path = temp_socket_path("nobody-home")
        .to_string_lossy()
        .into_owned();
    let client = GatewayRetrievalEmbeddingClient::new(
        runtime_pool().await,
        socket_path,
        cell_registry(),
        Duration::from_secs(30),
    );
    let workspace_id = WorkspaceId::new();
    let authorization = authorization(workspace_id);
    let fingerprint = profile_fingerprint();
    let result = client
        .embed_query(RetrievalEmbeddingInput {
            authorization: &authorization,
            workspace_id,
            request_id: Uuid::now_v7(),
            logical_call_id: Uuid::now_v7(),
            attempt_no: 1,
            profile_fingerprint: &fingerprint,
            dimension: 4,
            query: "worker is not running",
            deadline_unix_ms: deadline_unix_ms(),
        })
        .await;
    assert!(
        matches!(result, Ok(RetrievalEmbeddingOutcome::Unavailable { .. })),
        "worker-down must degrade the dense lane, not error or panic: {result:?}"
    );
}

/// ADR-0012 §2 "never mint a new `call_id` after an ambiguous outcome — a caller retry must
/// reuse the same logical call": drives the real `GatewayRetrievalEmbeddingClient::embed_query`
/// (not a hand-rolled RPC call) twice with the same `(logical_call_id, attempt_no)`, proving
/// the retry replays the first registration/provider call instead of registering — and
/// dialing the worker for — a second one.
#[tokio::test]
async fn gateway_retry_with_same_logical_call_id_replays_not_reregisters() {
    let uid = own_uid().await;
    let counting = Arc::new(CountingEmbedder {
        inner: TestDoubleProvider::new(embedding_model(), rerank_model()),
        calls: AtomicUsize::new(0),
    });
    let embedder: Arc<dyn EmbeddingProvider> = counting.clone();
    let socket_path = spawn_worker(uid, embedder).await;
    let client = GatewayRetrievalEmbeddingClient::new(
        runtime_pool().await,
        socket_path,
        cell_registry(),
        Duration::from_secs(30),
    );
    let workspace_id = WorkspaceId::new();
    let authorization = authorization(workspace_id);
    let fingerprint = profile_fingerprint();
    let logical_call_id = Uuid::now_v7();

    let input = |deadline| RetrievalEmbeddingInput {
        authorization: &authorization,
        workspace_id,
        request_id: Uuid::now_v7(),
        logical_call_id,
        attempt_no: 1,
        profile_fingerprint: &fingerprint,
        dimension: 4,
        query: "retry must reuse the same registration",
        deadline_unix_ms: deadline,
    };

    let first = client
        .embed_query(input(deadline_unix_ms()))
        .await
        .expect("first attempt embeds");
    let second = client
        .embed_query(input(deadline_unix_ms()))
        .await
        .expect("retried attempt replays");
    assert_eq!(
        first, second,
        "a retry with the same logical_call_id/attempt_no must replay the identical outcome"
    );
    assert_eq!(
        counting.calls.load(Ordering::SeqCst),
        1,
        "a retry must never dispatch a second real provider call"
    );
}
