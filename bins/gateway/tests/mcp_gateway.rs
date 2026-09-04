//! Native MCP -> Gateway actual PostgreSQL acceptance.
//!
//! The database fixture owns only isolated seed and cleanup rows. Every request below travels
//! through the loopback native MCP adapter and a real `role_gateway` runtime pool.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::{SocketAddr, TcpListener as StdTcpListener, TcpStream as StdTcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use humaux_adapters::{
    forget_repo,
    postgres::RuntimeDbPool,
    qdrant::{
        Distance, PointId, QdrantOperation, QdrantPointPayload, ShardingMethod,
        create_collection_body, ha_profile_for, tenant_index_body, upsert,
    },
    quota_repo::RatePolicy,
};
use humaux_domain::{
    authority::{AuthorityClass, AuthorityStatus},
    context::ContextBudget,
    dataclass::DataClass,
    identity::VisibilityClass,
    ids::{TenantId, WorkspaceId},
    memory::MemoryType,
};
use humaux_gateway::{
    context::ContextBootstrap,
    guard::{GatewayGuard, GuardRatePolicies, GuardSettings},
    mcp_application::GatewayMcpApplication,
    recall::{SemanticRecallRuntime, SemanticRecallVersions},
    remember::{RememberEventKind, RememberPolicy},
};
use humaux_infra_cell::{
    CallerId, CellId, HttpIntraCellTransport, IntraCellHttpTransport, IntraCellMethod,
    IntraCellRequest, IntraCellResource, IntraCellResourceRegistry, IntraCellResponse,
    ResourceEntry, authorize_cell_access,
};
use humaux_local_secret_scan::{LocalSecretScanner, LocalSecretScannerConfig};
use humaux_projection::card::EgressDisposition;
use humaux_projection::stream::StreamKey;
use humaux_protocol::{
    edge::{TrustedProxyConfig, compute_api_key_hash},
    mcp::{McpAdapter, McpHttpConfig, ToolName},
    mcp_catalog::CanonicalCatalog,
};
use humaux_retrieval_provider::{
    adapters::TestDoubleProvider,
    contract::{
        CalibrationProfileId, EmbeddingModelDescriptor, ModelId, RerankModelDescriptor,
        RerankScoreSemantics,
    },
};
use humaux_testkit::run_db_fixture;
use postgres::Row;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
use uuid::Uuid;

// This binary uses only a focused subset of the shared fixture API; other acceptance binaries
// exercise the remaining helpers.
#[allow(dead_code)]
#[path = "../../../crates/adapters/tests/support/operation_receipt_fixture.rs"]
mod operation_receipt_fixture;

use operation_receipt_fixture::{
    Fixture, Handle, SYNTHETIC_CREDENTIAL_PEPPER, ScopedContextRecord, SyntheticCredentialScopes,
    SyntheticServiceCredential,
};

const HOST: &str = "mcp.test";
const ORIGIN: &str = "https://mcp.test";
static CONTEXT_METRIC_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn final_completeness_count() -> u64 {
    use humaux_retrieval::completeness::retrieval_completeness_total_count;
    let reasons = [
        "none",
        "ledger_not_closed",
        "predicate_not_enumerable",
        "census_failed",
        "lane_failed",
        "index_count_unavailable",
        "a2_overshoot_beyond_pending",
        "mandatory_context_overflow",
        "count_unknown",
        "count_scope_mismatch",
        "pipeline_count_mismatch",
    ];
    [
        "exact",
        "facet_complete",
        "semantic_bounded",
        "cannot_establish",
    ]
    .into_iter()
    .flat_map(|class| {
        reasons
            .into_iter()
            .map(move |reason| retrieval_completeness_total_count(class, reason))
    })
    .sum()
}

fn rate() -> RatePolicy {
    RatePolicy::new(100, 100).expect("explicit fixture rate")
}

fn guard(runtime: RuntimeDbPool) -> Arc<GatewayGuard> {
    Arc::new(
        GatewayGuard::new(
            runtime,
            GuardSettings {
                credential_pepper: SYNTHETIC_CREDENTIAL_PEPPER.to_vec(),
                trusted_proxies: TrustedProxyConfig {
                    trusted_proxy_cidrs: vec![],
                    max_forwarded_hops: 1,
                },
                global_denylist: vec![],
                global_emergency_allowlist: vec![],
                tenant_network: BTreeMap::new(),
                rates: GuardRatePolicies {
                    preauth_ip: rate(),
                    credential: rate(),
                    user: rate(),
                    tenant: rate(),
                    operation: rate(),
                },
                reservation_ttl: Duration::from_secs(30),
                handler_timeout: Duration::from_secs(5),
                finalize_timeout: Duration::from_secs(2),
                replay_ttl: Duration::from_secs(60),
            },
        )
        .expect("explicit guard settings"),
    )
}

fn application(handle: &Handle, runtime: RuntimeDbPool) -> GatewayMcpApplication {
    application_with_budget(
        handle,
        runtime,
        ContextBudget::new(2_048, 1_024).expect("trusted budget"),
    )
}

fn application_with_budget(
    handle: &Handle,
    runtime: RuntimeDbPool,
    budget: ContextBudget,
) -> GatewayMcpApplication {
    let policy = RememberPolicy::new(
        StreamKey::new(
            TenantId(handle.tenant_id),
            "workspace",
            handle.workspace_id,
            "knowledge",
            "ingest",
            "v1",
        ),
        handle.reasoning_domain_id,
        Duration::from_secs(60),
        DataClass::Internal,
        VisibilityClass::WorkspaceShared,
    )
    .expect("trusted fixture remember policy");
    let context_bootstrap = ContextBootstrap::new(
        budget,
        humaux_contracts::retrieval_config::resolve_registered_retrieval_profile(
            &Default::default(),
        )
        .expect("registered profile"),
        &policy,
    )
    .expect("actual executable identity");
    GatewayMcpApplication::new(
        CanonicalCatalog::load().expect("closed canonical MCP catalog"),
        guard(runtime),
        policy,
        RememberEventKind::UserMessage,
        context_bootstrap,
    )
    .with_confirm_token_ttl(Duration::from_secs(300))
    .expect("positive fixture confirm-token TTL")
    .with_undo_window(Duration::from_secs(86_400))
    .expect("positive fixture undo window")
}

fn semantic_scanner() -> Arc<LocalSecretScanner> {
    Arc::new(
        LocalSecretScanner::new(LocalSecretScannerConfig {
            executable: PathBuf::from(
                std::env::var("HUMAUX_TEST_GITLEAKS_BIN")
                    .expect("semantic Gateway fixture requires HUMAUX_TEST_GITLEAKS_BIN"),
            ),
            expected_version: std::env::var("HUMAUX_TEST_GITLEAKS_VERSION")
                .expect("semantic Gateway fixture requires HUMAUX_TEST_GITLEAKS_VERSION"),
            expected_executable_sha256: std::env::var("HUMAUX_TEST_GITLEAKS_SHA256")
                .expect("semantic Gateway fixture requires HUMAUX_TEST_GITLEAKS_SHA256"),
            timeout: Duration::from_secs(5),
            max_payload_bytes: 64 * 1024,
            finding_exit_code: 1,
        })
        .expect("pinned semantic Gateway scanner"),
    )
}

fn semantic_provider() -> Arc<TestDoubleProvider> {
    Arc::new(TestDoubleProvider::new(
        EmbeddingModelDescriptor {
            model_id: ModelId("gateway-test-embedding".to_owned()),
            model_revision: "embed-v1".to_owned(),
            dimension_options: vec![4],
            max_input_tokens: 4_096,
            batch_supported: true,
            dense_supported: true,
            sparse_supported: false,
        },
        RerankModelDescriptor {
            model_id: ModelId("gateway-unused-reranker".to_owned()),
            model_revision: "unused-v1".to_owned(),
            max_documents: 5,
            max_input_tokens: 4_096,
            score_semantics: RerankScoreSemantics::RawLogit,
            calibration_profile: CalibrationProfileId("unused-v1".to_owned()),
        },
    ))
}

fn semantic_vector(text: &str) -> Vec<f32> {
    let mut vector = vec![0.0; 4];
    for (index, byte) in text.bytes().enumerate() {
        vector[index % 4] += f32::from(byte) / 255.0;
    }
    vector
}

fn qdrant_port() -> u16 {
    std::env::var("HUMAUX_TEST_QDRANT_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(6333)
}

fn semantic_qdrant_registry(cell: CellId, caller: CallerId) -> IntraCellResourceRegistry {
    let mut entries = BTreeMap::new();
    entries.insert(
        IntraCellResource::QDRANT_REST,
        ResourceEntry::new(
            "127.0.0.1",
            qdrant_port(),
            cell,
            vec!["127.0.0.1/32".parse().expect("loopback CIDR")],
            BTreeSet::from([caller.clone()]),
            false,
        )
        .expect("loopback Qdrant resource"),
    );
    // ADR-0012 §决定3: `GatewayRetrievalEmbeddingClient` mints a permit against this same
    // registry every call, even though its actual dial bypasses `IntraCellHttpTransport`.
    entries.insert(
        IntraCellResource::RETRIEVAL_EMBEDDING_RPC,
        ResourceEntry::new(
            "unix-socket",
            0,
            cell,
            vec![],
            BTreeSet::from([caller.clone()]),
            false,
        )
        .expect("valid RPC resource entry"),
    );
    IntraCellResourceRegistry::new(entries, cell, caller)
}

/// `std::env::temp_dir()` overflows `sockaddr_un`'s ~104-byte limit on macOS — mirrors
/// `tests/query_embedding_rpc.rs`'s identical helper (separate test binary, no shared module).
fn semantic_rpc_socket_path(tag: &str) -> PathBuf {
    PathBuf::from(format!("/tmp/hgm-{tag}-{}.sock", Uuid::now_v7().simple()))
}

/// Learns this test process's own real uid via a local self-connected socket pair's peer
/// credential — mirrors `tests/query_embedding_rpc.rs`'s identical helper.
async fn semantic_own_uid() -> u32 {
    let path = semantic_rpc_socket_path("uid-probe");
    let listener = tokio::net::UnixListener::bind(&path).expect("bind uid probe socket");
    let client = tokio::net::UnixStream::connect(&path)
        .await
        .expect("connect uid probe");
    let (server_side, _) = listener.accept().await.expect("accept uid probe");
    let uid = server_side.peer_cred().expect("peer credential").uid();
    drop(client);
    drop(server_side);
    let _ = std::fs::remove_file(&path);
    uid
}

/// Spawns the real `humaux-retrieval-worker` RPC app in-process on a temporary UDS, backed by
/// `semantic_provider()` — the port double these semantic tests now drive
/// `SemanticRecallRuntime` through, instead of holding an `EmbeddingProvider` directly (mirrors
/// `tests/query_embedding_rpc.rs`'s `spawn_worker`).
async fn spawn_semantic_worker(expected_gateway_uid: u32) -> String {
    let socket_path = semantic_rpc_socket_path("worker");
    let calls = humaux_adapters::postgres::RetrievalWorkerDbPool::connect(
        &std::env::var("HUMAUX_RETRIEVAL_WORKER_PG_DSN")
            .expect("semantic Gateway fixture requires HUMAUX_RETRIEVAL_WORKER_PG_DSN"),
    )
    .await
    .expect("retrieval worker db pool");
    let state = Arc::new(humaux_retrieval_worker::rpc::RpcState {
        expected_gateway_uid,
        calls,
        scanner: semantic_scanner(),
        embedder: semantic_provider(),
        dimension: 4,
        provider_id: "gateway-test-provider".to_owned(),
    });
    let listener = tokio::net::UnixListener::bind(&socket_path).expect("bind worker rpc socket");
    let app = humaux_retrieval_worker::rpc::router(state);
    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<humaux_retrieval_worker::rpc::PeerIdentity>(),
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    socket_path.to_string_lossy().into_owned()
}

async fn create_semantic_collection(
    transport: &HttpIntraCellTransport,
    registry: &IntraCellResourceRegistry,
    collection: &str,
) {
    let permit = authorize_cell_access(
        registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(60),
    )
    .expect("semantic Qdrant permit");
    for (path, body) in [
        (
            format!("/collections/{collection}"),
            create_collection_body(4, Distance::Cosine, 1, 1, 1, ShardingMethod::Auto),
        ),
        (
            format!("/collections/{collection}/index"),
            tenant_index_body(),
        ),
    ] {
        let response = transport
            .execute(
                &permit,
                IntraCellRequest {
                    method: IntraCellMethod::Put,
                    path,
                    json_body: Some(body),
                    headers: Vec::new(),
                },
            )
            .await
            .expect("real Qdrant setup request");
        assert!(
            (200..300).contains(&response.status),
            "real Qdrant setup returned {}",
            response.status
        );
    }
}

async fn delete_semantic_collection(
    transport: &HttpIntraCellTransport,
    registry: &IntraCellResourceRegistry,
    collection: &str,
) {
    let permit = authorize_cell_access(
        registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(60),
    )
    .expect("semantic Qdrant cleanup permit");
    let response = transport
        .execute(
            &permit,
            IntraCellRequest {
                method: IntraCellMethod::Delete,
                path: format!("/collections/{collection}"),
                json_body: None,
                headers: Vec::new(),
            },
        )
        .await
        .expect("real Qdrant cleanup request");
    assert!(
        (200..300).contains(&response.status),
        "real Qdrant cleanup returned {}",
        response.status
    );
}

struct ServingSwitchAfterQuery {
    inner: Arc<HttpIntraCellTransport>,
    owner: std::sync::Mutex<Option<postgres::Client>>,
    tenant_id: Uuid,
    workspace_id: Uuid,
    triggered: AtomicBool,
}

#[async_trait::async_trait]
impl IntraCellHttpTransport for ServingSwitchAfterQuery {
    async fn execute(
        &self,
        permit: &humaux_infra_cell::CellAccessPermit,
        request: IntraCellRequest,
    ) -> Result<IntraCellResponse, humaux_infra_cell::IntraCellError> {
        let switch_after_response = request.method == IntraCellMethod::Post
            && request.path.contains("/points/query")
            && !self.triggered.swap(true, Ordering::AcqRel);
        let response = self.inner.execute(permit, request).await?;
        if switch_after_response {
            let mut owner = self
                .owner
                .lock()
                .expect("serving-switch owner mutex")
                .take()
                .expect("one-shot serving-switch owner");
            let tenant_id = self.tenant_id;
            let workspace_id = self.workspace_id;
            std::thread::spawn(move || {
                let mut transaction = owner.transaction().expect("begin serving switch");
                transaction
                    .execute(
                        "UPDATE projection.stream_checkpoints SET serving=false \
                         WHERE tenant_id=$1 AND scope_kind='workspace' AND scope_id=$2 \
                           AND domain='knowledge' AND projection_kind='ingest' AND projection_version='v1'",
                        &[&tenant_id, &workspace_id],
                    )
                    .expect("retire serving v1 after Qdrant query");
                transaction
                    .execute(
                        "INSERT INTO projection.stream_checkpoints \
                           (tenant_id,scope_kind,scope_id,domain,projection_kind,projection_version,serving) \
                         VALUES($1,'workspace',$2,'knowledge','ingest','v2',true)",
                        &[&tenant_id, &workspace_id],
                    )
                    .expect("activate serving v2 after Qdrant query");
                transaction.commit().expect("commit serving switch");
            })
            .join()
            .expect("serving-switch thread");
        }
        Ok(response)
    }
}

struct SemanticProjectionCleanup {
    owner: postgres::Client,
    tenant_id: Uuid,
}

impl Drop for SemanticProjectionCleanup {
    fn drop(&mut self) {
        for (sql, table) in [
            (
                "DELETE FROM projection.private_memory_points WHERE tenant_id=$1",
                "projection.private_memory_points",
            ),
            (
                "DELETE FROM projection.tenant_placements WHERE tenant_id=$1",
                "projection.tenant_placements",
            ),
        ] {
            let result = self.owner.execute(sql, &[&self.tenant_id]);
            if let Err(error) = result {
                let missing_fixture_table = error.as_db_error().is_some_and(|error| {
                    error.code() == &postgres::error::SqlState::UNDEFINED_TABLE
                });
                if !missing_fixture_table && !std::thread::panicking() {
                    panic!("cleanup semantic {table} rows: {error}");
                }
            }
        }
    }
}

/// §17.3 per-tenant placement row a real `recall.search` call now resolves per request
/// (`adapters::placement_repo::tenant_placement`) — an in-memory `TenantPlacementRow` is no
/// longer enough, `SemanticRecallRuntime` holds none.
fn seed_tenant_placement(handle: &mut Handle, collection: &str) {
    handle
        .admin
        .execute(
            "INSERT INTO projection.tenant_placements \
               (tenant_id,projection_family,collection_name,shard_key,placement_class, \
                point_count,bytes_estimate,promotion_state) \
             VALUES ($1,'private_memory_v1',$2,NULL,'SHARED_FALLBACK',2,0,'STABLE')",
            &[&handle.tenant_id, &collection],
        )
        .expect("owner seeds tenant placement row");
}

fn seed_semantic_registry_row(
    handle: &mut Handle,
    record: &ScopedContextRecord,
    point_id: Uuid,
) -> time::OffsetDateTime {
    handle
        .admin
        .execute(
            "INSERT INTO projection.private_memory_points \
               (point_id,tenant_id,scope_kind,scope_id,domain,projection_kind, \
                projection_version,embedding_version,memory_id,source_updated_at,body_sha256) \
             SELECT $1,$2,'workspace',$3,'knowledge','ingest','v1','embed-v1',memory_id, \
                    updated_at,sha256(convert_to(content::text,'UTF8')) \
             FROM private.memory_records WHERE tenant_id=$2 AND memory_id=$4",
            &[
                &point_id,
                &handle.tenant_id,
                &handle.workspace_id,
                &record.memory_id,
            ],
        )
        .expect("owner registers opaque Qdrant point identity");
    time::OffsetDateTime::now_utc()
}

fn seed_semantic_checkpoint(handle: &mut Handle) {
    handle
        .admin
        .execute(
            "INSERT INTO projection.stream_checkpoints \
               (tenant_id,scope_kind,scope_id,domain,projection_kind,projection_version, \
                issued_highwater,evidence_highwater,knowledge_highwater,projection_highwater,serving) \
             VALUES($1,'workspace',$2,'knowledge','ingest','v1',0,0,0,0,true)",
            &[&handle.tenant_id, &handle.workspace_id],
        )
        .expect("owner seeds the trusted serving version");
}

fn semantic_payload(
    handle: &Handle,
    source_updated_at: time::OffsetDateTime,
) -> humaux_adapters::qdrant::IndexablePayload {
    QdrantPointPayload {
        tenant_id: TenantId(handle.tenant_id),
        workspace_id: WorkspaceId(handle.workspace_id),
        visibility_class: VisibilityClass::WorkspaceShared,
        visibility_user_id: None,
        visibility_workspace_id: Some(WorkspaceId(handle.workspace_id)),
        object_type: "memory_record".to_owned(),
        memory_type: MemoryType::Note,
        status: AuthorityStatus::Active,
        authority: AuthorityClass::ProjectConstraint,
        created_at: source_updated_at,
        effective_at: source_updated_at,
        embedding_version: "embed-v1".to_owned(),
        projection_version: "v1".to_owned(),
        source_stream_seq: 0,
        data_class: DataClass::Internal,
        egress_disposition: EgressDisposition::Allowed,
    }
    .into_indexable()
    .expect("non-secret semantic fixture payload")
}

async fn recall_call(address: SocketAddr, bearer: Option<&str>, arguments: Value) -> (u16, Value) {
    let body = rpc(1, "tools/call", call_params("recall", arguments));
    match bearer {
        Some(bearer) => raw_request(address, &tool_call_headers("recall", bearer), &body).await,
        None => {
            let headers = [
                ("MCP-Protocol-Version", "2026-07-28"),
                ("Mcp-Method", "tools/call"),
                ("Mcp-Name", "recall"),
            ];
            raw_request(address, &headers, &body).await
        }
    }
}

async fn start(application: GatewayMcpApplication) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let catalog = CanonicalCatalog::load()
        .expect("catalog")
        .trusted_catalog()
        .expect("trusted catalog");
    let adapter = McpAdapter::new(
        Arc::new(application),
        catalog,
        McpHttpConfig::new(vec![HOST.into()], vec![ORIGIN.into()], 64 * 1024)
            .expect("explicit listener configuration"),
    );
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let address = listener.local_addr().expect("loopback address");
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            adapter
                .router()
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .expect("serve loopback MCP");
    });
    (address, server)
}

async fn raw_request(address: SocketAddr, headers: &[(&str, &str)], body: &str) -> (u16, Value) {
    try_raw_request(address, headers, body)
        .await
        .expect("raw loopback MCP request")
}

async fn try_raw_request(
    address: SocketAddr,
    headers: &[(&str, &str)],
    body: &str,
) -> Result<(u16, Value), String> {
    let mut stream = TcpStream::connect(address)
        .await
        .map_err(|_| "connect loopback MCP".to_owned())?;
    let mut request = format!(
        "POST /mcp HTTP/1.1\r\nConnection: close\r\nAccept: application/json, text/event-stream\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
        body.len()
    );
    if !headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("host"))
    {
        request.push_str(&format!("Host: {HOST}\r\n"));
    }
    if !headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("origin"))
    {
        request.push_str(&format!("Origin: {ORIGIN}\r\n"));
    }
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    request.push_str(body);
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|_| "write loopback MCP request".to_owned())?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .map_err(|_| "read loopback MCP response".to_owned())?;
    let response = String::from_utf8(response).map_err(|_| "non-UTF8 HTTP response".to_owned())?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| "missing HTTP response body".to_owned())?;
    let status = head
        .lines()
        .next()
        .ok_or_else(|| "missing HTTP status line".to_owned())?
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| "missing HTTP status value".to_owned())?
        .parse()
        .map_err(|_| "non-numeric HTTP status".to_owned())?;
    Ok((
        status,
        serde_json::from_str(body).unwrap_or_else(|_| json!({"raw": body})),
    ))
}

struct CommitAckWitness {
    receipt_insert_sent: AtomicBool,
    commit_ack_dropped: AtomicBool,
}

struct CommitAckProxy {
    address: SocketAddr,
    witness: Arc<CommitAckWitness>,
    stopped: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl CommitAckProxy {
    fn port(&self) -> u16 {
        self.address.port()
    }

    async fn shutdown(self) -> Result<(bool, bool), String> {
        let Self {
            witness,
            stopped,
            task,
            ..
        } = self;
        stopped.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .map_err(|_| "COMMIT-ACK proxy did not stop within 5s".to_owned())
            .and_then(|result| result.map_err(|_| "COMMIT-ACK proxy task panicked".to_owned()))?;
        Ok((
            witness.receipt_insert_sent.load(Ordering::Acquire),
            witness.commit_ack_dropped.load(Ordering::Acquire),
        ))
    }
}

async fn start_commit_ack_proxy() -> Result<CommitAckProxy, String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|_| "bind COMMIT-ACK proxy".to_owned())?;
    let address = listener
        .local_addr()
        .map_err(|_| "read COMMIT-ACK proxy address".to_owned())?;
    let witness = Arc::new(CommitAckWitness {
        receipt_insert_sent: AtomicBool::new(false),
        commit_ack_dropped: AtomicBool::new(false),
    });
    let stopped = Arc::new(AtomicBool::new(false));
    let task_witness = Arc::clone(&witness);
    let task_stopped = Arc::clone(&stopped);
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        while !task_stopped.load(Ordering::Acquire) {
            while connections.try_join_next().is_some() {}
            if let Ok(Ok((client, _))) =
                tokio::time::timeout(Duration::from_millis(50), listener.accept()).await
            {
                let witness = Arc::clone(&task_witness);
                connections.spawn(async move {
                    let _ = proxy_postgres_connection(client, witness).await;
                });
            }
        }
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    });
    Ok(CommitAckProxy {
        address,
        witness,
        stopped,
        task,
    })
}

async fn proxy_postgres_connection(
    client: TcpStream,
    witness: Arc<CommitAckWitness>,
) -> Result<(), String> {
    // Upstream = the gateway role DSN's authority (host:port), never a machine-local literal —
    // the fixture already validated host == 127.0.0.1 and the port matches HUMAUX_TEST_PG_DSN.
    let upstream_authority = std::env::var("HUMAUX_GATEWAY_PG_DSN")
        .ok()
        .and_then(|dsn| dsn.rsplit_once('@').map(|(_, rest)| rest.to_owned()))
        .and_then(|rest| {
            rest.split_once('/')
                .map(|(authority, _)| authority.to_owned())
        })
        .expect("HUMAUX_GATEWAY_PG_DSN must be postgres://<user>:<pw>@<host>:<port>/<db>");
    let upstream = TcpStream::connect(upstream_authority.as_str())
        .await
        .map_err(|_| "connect isolated PostgreSQL from proxy".to_owned())?;
    let receipt_insert_sent = Arc::new(AtomicBool::new(false));
    let (client_read, client_write) = client.into_split();
    let (postgres_read, postgres_write) = upstream.into_split();
    tokio::select! {
        result = forward_client_to_postgres(client_read, postgres_write, Arc::clone(&receipt_insert_sent), Arc::clone(&witness)) => result,
        result = forward_postgres_to_client(postgres_read, client_write, receipt_insert_sent, witness) => result,
    }
}

async fn forward_client_to_postgres(
    mut client: tokio::net::tcp::OwnedReadHalf,
    mut postgres: tokio::net::tcp::OwnedWriteHalf,
    receipt_insert_sent: Arc<AtomicBool>,
    witness: Arc<CommitAckWitness>,
) -> Result<(), String> {
    const RECEIPT_INSERT: &[u8] = b"INSERT INTO control.operation_receipts";
    let mut buffer = [0_u8; 8_192];
    let mut tail = Vec::new();
    loop {
        let read = client
            .read(&mut buffer)
            .await
            .map_err(|_| "read PostgreSQL client bytes".to_owned())?;
        if read == 0 {
            return Ok(());
        }
        postgres
            .write_all(&buffer[..read])
            .await
            .map_err(|_| "forward PostgreSQL client bytes".to_owned())?;
        if !receipt_insert_sent.load(Ordering::Acquire) {
            tail.extend_from_slice(&buffer[..read]);
            if tail
                .windows(RECEIPT_INSERT.len())
                .any(|bytes| bytes == RECEIPT_INSERT)
            {
                receipt_insert_sent.store(true, Ordering::Release);
                witness.receipt_insert_sent.store(true, Ordering::Release);
            }
            let keep = RECEIPT_INSERT.len().saturating_sub(1);
            if tail.len() > keep {
                tail.drain(..tail.len() - keep);
            }
        }
    }
}

async fn forward_postgres_to_client(
    mut postgres: tokio::net::tcp::OwnedReadHalf,
    mut client: tokio::net::tcp::OwnedWriteHalf,
    receipt_insert_sent: Arc<AtomicBool>,
    witness: Arc<CommitAckWitness>,
) -> Result<(), String> {
    loop {
        let mut tag = [0_u8; 1];
        match postgres.read_exact(&mut tag).await {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(_) => return Err("read PostgreSQL backend tag".into()),
        }
        let mut length = [0_u8; 4];
        postgres
            .read_exact(&mut length)
            .await
            .map_err(|_| "read PostgreSQL backend length".to_owned())?;
        let length = u32::from_be_bytes(length);
        if !(4..=1_048_576).contains(&length) {
            return Err("invalid PostgreSQL backend frame length".into());
        }
        let mut payload = vec![0_u8; (length - 4) as usize];
        postgres
            .read_exact(&mut payload)
            .await
            .map_err(|_| "read PostgreSQL backend payload".to_owned())?;
        if tag == *b"C"
            && payload == b"COMMIT\0"
            && receipt_insert_sent.load(Ordering::Acquire)
            && !witness.commit_ack_dropped.swap(true, Ordering::AcqRel)
        {
            return Ok(());
        }
        client
            .write_all(&tag)
            .await
            .map_err(|_| "forward PostgreSQL backend tag".to_owned())?;
        client
            .write_all(&length.to_be_bytes())
            .await
            .map_err(|_| "forward PostgreSQL backend length".to_owned())?;
        client
            .write_all(&payload)
            .await
            .map_err(|_| "forward PostgreSQL backend payload".to_owned())?;
    }
}

async fn stop_server(server: tokio::task::JoinHandle<()>) -> Result<(), String> {
    server.abort();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .map_err(|_| "MCP loopback server did not stop within 5s".to_owned())
        .map(|_| ())
}

fn metadata() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientInfo": {"name": "gateway-acceptance", "version": "1"},
        "io.modelcontextprotocol/clientCapabilities": {},
    })
}

fn rpc(id: u64, method: &str, params: Value) -> String {
    json!({"jsonrpc":"2.0", "id": id, "method": method, "params": params}).to_string()
}

fn operation_headers<'a>(method: &'a str, bearer: &'a str) -> [(&'a str, &'a str); 3] {
    [
        ("MCP-Protocol-Version", "2026-07-28"),
        ("Mcp-Method", method),
        ("Authorization", bearer),
    ]
}

fn tool_call_headers<'a>(tool: &'a str, bearer: &'a str) -> [(&'a str, &'a str); 4] {
    [
        ("MCP-Protocol-Version", "2026-07-28"),
        ("Mcp-Method", "tools/call"),
        ("Mcp-Name", tool),
        ("Authorization", bearer),
    ]
}

fn counts(handle: &mut Handle) -> (i64, i64, i64, i64, i64) {
    let row: Row = handle
        .admin
        .query_one(
            "SELECT \
               (SELECT count(*) FROM private.evidence_objects WHERE tenant_id=$1), \
               (SELECT count(*) FROM ops.outbox WHERE tenant_id=$1), \
               (SELECT count(*) FROM control.operation_receipts WHERE tenant_id=$1), \
               (SELECT count(*) FROM control.usage_reservations WHERE tenant_id=$1), \
               (SELECT count(*) FROM control.audit_events WHERE tenant_id=$1)",
            &[&handle.tenant_id],
        )
        .expect("owner reads isolated outcomes");
    (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4))
}

fn durable_counts(counts: (i64, i64, i64, i64, i64)) -> (i64, i64, i64, i64) {
    (counts.0, counts.1, counts.2, counts.3)
}

fn blocking_counts(handle: &mut Handle) -> (i64, i64, i64, i64, i64) {
    tokio::task::block_in_place(|| counts(handle))
}

fn receipt_outcome_counts(handle: &mut Handle) -> (i64, i64, i64) {
    let row: Row = handle
        .admin
        .query_one(
            "SELECT \
               count(*) FILTER (WHERE a.action='MCP_REQUEST_FINISHED' AND a.result='OK' \
                                  AND a.audit_event_id IN (SELECT audit_event_id FROM control.operation_receipts WHERE tenant_id=$1)), \
               count(*) FILTER (WHERE a.action='MCP_REQUEST_OUTCOME_UNKNOWN' AND a.result='UNKNOWN'), \
               count(*) FILTER (WHERE a.action='MCP_REQUEST_FINISHED' AND a.result='DEPENDENCY_UNAVAILABLE') \
             FROM control.audit_events a \
             WHERE a.tenant_id=$1 AND a.resource_id='remember.put'",
            &[&handle.tenant_id],
        )
        .expect("owner reads isolated receipt outcomes");
    (row.get(0), row.get(1), row.get(2))
}

fn call_params(name: &str, arguments: Value) -> Value {
    json!({"name": name, "arguments": arguments, "_meta": metadata()})
}

async fn assert_authenticated_controls_do_not_create_durable_rows(
    handle: &mut Handle,
    address: SocketAddr,
    bearer: &str,
) {
    let before = blocking_counts(handle);
    for (id, method, params) in [
        (
            1,
            "initialize",
            json!({"protocolVersion":"2026-07-28", "capabilities":{}, "clientInfo":{"name":"gateway-acceptance","version":"1"}, "_meta": metadata()}),
        ),
        (2, "tools/list", json!({"_meta": metadata()})),
        (3, "ping", json!({"_meta": metadata()})),
    ] {
        let headers = operation_headers(method, bearer);
        let (status, response) = raw_request(address, &headers, &rpc(id, method, params)).await;
        assert_eq!(status, 200, "{method}: {response}");
    }
    assert_eq!(
        durable_counts(blocking_counts(handle)),
        durable_counts(before),
        "authenticated control traffic must not create Evidence/outbox/receipt/BMO rows"
    );
}

async fn assert_workspace_context_row(
    handle: &mut Handle,
    address: SocketAddr,
    bearer: &str,
) -> (i64, i64, i64, i64, i64) {
    let context_row =
        tokio::task::block_in_place(|| handle.seed_workspace_visible_context_record());
    let context = call_params("context", json!({"workspace_id": handle.workspace_id}));
    let headers = tool_call_headers("context", bearer);
    let (status, response) = raw_request(address, &headers, &rpc(4, "tools/call", context)).await;
    assert_eq!(status, 200, "scoped context: {response}");
    assert_context_response(
        &response,
        context_row.memory_id,
        5,
        &std::env::current_exe().expect("test binary"),
    );
    blocking_counts(handle)
}

fn assert_context_response(
    response: &Value,
    memory_id: Uuid,
    top_k: u32,
    binary: &std::path::Path,
) {
    let value = assert_tool_response(response, ToolName::Context);
    let handoff = &value["handoff"];
    let content = &value["content"];
    let manifest: Vec<_> = handoff["mandatory"]
        .as_array()
        .expect("mandatory IDs")
        .iter()
        .chain(handoff["pinned"].as_array().expect("pinned IDs"))
        .map(|row| row["memory_id"].clone())
        .collect();
    assert_eq!(manifest, vec![json!(memory_id)]);
    assert_eq!(
        content["mandatory"]["returned"],
        handoff["counts"]["mandatory_returned"]
    );
    assert_read_envelope(
        content,
        memory_id,
        top_k,
        binary,
        humaux_retrieval::request::RetrievalIntent::trusted_context(),
    );
}

fn assert_memory_response(response: &Value, memory_id: Uuid, top_k: u32, binary: &std::path::Path) {
    let content = assert_tool_response(response, ToolName::Memory);
    assert_read_envelope(
        content,
        memory_id,
        top_k,
        binary,
        humaux_retrieval::request::RetrievalIntent::trusted_memory_get(
            humaux_domain::authority::MemoryId(memory_id),
        ),
    );
    assert_eq!(content["mandatory"], json!({"state":"not_run"}));
    assert_eq!(content["pinned"], json!({"state":"not_run"}));
    assert_eq!(content["completeness"]["lanes"], json!({"direct_get":"ok"}));
    assert_eq!(
        content["provenance"]["profile"]["lanes"],
        json!(["direct_get"])
    );
    assert!(content["completeness"]["exact"].is_null());
}

fn assert_tool_response(response: &Value, tool: ToolName) -> &Value {
    let result = &response["result"];
    assert_ne!(result["isError"], true, "read business result: {response}");
    let value = &result["structuredContent"];
    CanonicalCatalog::load()
        .expect("catalog")
        .validate_output(tool, value)
        .expect("actual response satisfies its advertised schema");
    let text: Value =
        serde_json::from_str(result["content"][0]["text"].as_str().expect("text mirror"))
            .expect("JSON text mirror");
    assert_eq!(&text, value, "structured and text results must agree");
    value
}

fn assert_read_envelope(
    content: &Value,
    memory_id: Uuid,
    top_k: u32,
    binary: &std::path::Path,
    intent: humaux_retrieval::request::RetrievalIntent,
) {
    let items = content["items"].as_array().expect("body items");
    assert_eq!(
        items
            .iter()
            .map(|row| row["memory_id"].clone())
            .collect::<Vec<_>>(),
        vec![json!(memory_id)]
    );
    assert_eq!(
        items[0]["content"],
        json!({"fixture":"operation receipt scoped context"})
    );
    assert_read_envelope_metadata(content, 1, top_k, binary, intent);
}

fn assert_read_envelope_metadata(
    content: &Value,
    returned: usize,
    top_k: u32,
    binary: &std::path::Path,
    intent: humaux_retrieval::request::RetrievalIntent,
) {
    use std::io::Read;
    assert_eq!(content["completeness"]["class"], "cannot_establish");
    assert_eq!(content["completeness"]["returned"], returned);
    assert!(content["completeness"]["known_lower_bound"].is_null());
    assert_eq!(content["grounding"]["current"], returned);
    assert_eq!(content["grounding"]["not_judged"], 0);
    assert!(content["pipeline"]["projection"]["visible"].is_null());
    assert!(content["pipeline"]["projection"]["completeness_ratio"].is_null());
    assert!(content["pipeline"]["evidence"]["persisted"].is_null());
    assert!(content["pipeline"]["knowledge"]["eligible"].is_null());
    assert_eq!(content["freshness"]["class"], "unknown");
    let provenance = &content["provenance"];
    for field in [
        "projection_version",
        "embedding_model_id",
        "rerank_model_id",
        "card_builder_version",
    ] {
        assert_eq!(provenance[field], json!({"status":"not_applicable"}));
    }
    assert_eq!(provenance["profile"]["top_k"], top_k);
    assert_eq!(provenance["profile"]["cand_k"], (top_k * 5).min(200));
    let profile = humaux_contracts::retrieval_config::resolve_registered_retrieval_profile(
        &BTreeMap::from([("retrieval.profile.top_k".to_owned(), top_k.to_string())]),
    )
    .expect("actual registered profile");
    let request = humaux_retrieval::request::build_request(intent, &profile)
        .expect("sole trusted request constructor");
    assert_eq!(
        provenance["profile_fingerprint"],
        request.profile_fingerprint()
    );
    assert_eq!(
        provenance["profile"]["cand_k_formula"],
        request.cand_k_formula()
    );
    let mut input = std::fs::File::open(binary).expect("actual executing binary");
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 65536];
    loop {
        let len = input.read(&mut buffer).expect("read executing binary");
        if len == 0 {
            break;
        }
        digest.update(&buffer[..len]);
    }
    assert_eq!(
        provenance["binary_build"],
        format!("sha256:{:x}", digest.finalize())
    );
}

fn assert_enumeration_response(response: &Value, expected: &[Uuid]) -> Option<String> {
    let value = assert_tool_response(response, ToolName::Memory);
    let content = &value["content"];
    let items = content["items"].as_array().expect("enumerated bodies");
    let actual: Vec<Uuid> = items
        .iter()
        .map(|item| {
            Uuid::parse_str(item["memory_id"].as_str().expect("memory ID"))
                .expect("typed memory ID")
        })
        .collect();
    assert_eq!(
        actual, expected,
        "manifest order and membership: {response}"
    );
    for item in items {
        assert_eq!(
            item["content"],
            json!({"fixture":"operation receipt scoped context"})
        );
    }
    assert_read_envelope_metadata(
        content,
        expected.len(),
        5,
        &std::env::current_exe().expect("test binary"),
        humaux_retrieval::request::RetrievalIntent::trusted_memory_enumerate(),
    );
    assert_eq!(content["mandatory"], json!({"state":"not_run"}));
    assert_eq!(content["pinned"], json!({"state":"not_run"}));
    let cursor = value["pagination"]["next_cursor"]
        .as_str()
        .map(str::to_owned);
    assert_eq!(content["completeness"]["truncated"], cursor.is_some());
    cursor
}

async fn enumerate_call(address: SocketAddr, bearer: &str, arguments: Value) -> (u16, Value) {
    raw_request(
        address,
        &tool_call_headers("memory", bearer),
        &rpc(1, "tools/call", call_params("memory", arguments)),
    )
    .await
}

async fn assert_remember_replay(
    handle: &mut Handle,
    address: SocketAddr,
    bearer: &str,
    after_context: (i64, i64, i64, i64, i64),
) -> (i64, i64, i64, i64, i64) {
    let key = format!("mcp-replay-{}", Uuid::now_v7());
    let args = json!({
        "operation": "put",
        "content": "native MCP atomic evidence",
        "idempotency_key": key,
        "workspace_id": handle.workspace_id,
    });
    let headers = tool_call_headers("remember", bearer);
    let (status, first) = raw_request(
        address,
        &headers,
        &rpc(5, "tools/call", call_params("remember", args.clone())),
    )
    .await;
    assert_eq!(status, 200, "first remember: {first}");
    let first_content = &first["result"]["structuredContent"];
    let evidence_id = first_content["evidence_id"].clone();
    assert!(evidence_id.is_string(), "accepted evidence id: {first}");
    assert_eq!(first_content["replayed"], false, "first response: {first}");
    let after_first = blocking_counts(handle);
    assert_eq!(
        (
            after_first.0 - after_context.0,
            after_first.1 - after_context.1,
            after_first.2 - after_context.2,
            after_first.3 - after_context.3,
        ),
        (1, 1, 1, 1),
        "one remember call adds one Evidence/outbox/receipt/BMO row after the billed context read"
    );

    let (status, replay) = raw_request(
        address,
        &headers,
        &rpc(6, "tools/call", call_params("remember", args)),
    )
    .await;
    assert_eq!(status, 200, "replay: {replay}");
    assert_eq!(
        replay["result"]["structuredContent"]["evidence_id"],
        evidence_id
    );
    assert_eq!(replay["result"]["structuredContent"]["replayed"], true);
    assert_eq!(
        durable_counts(blocking_counts(handle)),
        durable_counts(after_first)
    );
    after_first
}

async fn assert_host_origin_rejections(
    handle: &mut Handle,
    address: SocketAddr,
    credential: &SyntheticServiceCredential,
) {
    let bad_origin_headers = [
        ("MCP-Protocol-Version", "2026-07-28"),
        ("Mcp-Method", "tools/call"),
        ("Mcp-Name", "remember"),
        ("Authorization", credential.bearer.as_str()),
        ("Origin", "https://bad.test"),
    ];
    let before_bad_origin = blocking_counts(handle);
    let (status, response) = raw_request(address, &bad_origin_headers, &rpc(10, "tools/call", call_params("remember", json!({"operation":"put","content":"never reached","idempotency_key":"origin-rejected"})))).await;
    assert_eq!(
        status, 403,
        "host/origin boundary precedes business dispatch: {response}"
    );
    assert_eq!(
        durable_counts(blocking_counts(handle)),
        durable_counts(before_bad_origin)
    );

    let bad_host_headers = [
        ("MCP-Protocol-Version", "2026-07-28"),
        ("Mcp-Method", "tools/call"),
        ("Mcp-Name", "remember"),
        ("Authorization", credential.bearer.as_str()),
        ("Host", "bad.test"),
    ];
    let before_bad_host = blocking_counts(handle);
    let (status, response) = raw_request(address, &bad_host_headers, &rpc(11, "tools/call", call_params("remember", json!({"operation":"put","content":"never reached","idempotency_key":"host-rejected"})))).await;
    assert_eq!(
        status, 403,
        "host boundary precedes business dispatch: {response}"
    );
    assert_eq!(
        durable_counts(blocking_counts(handle)),
        durable_counts(before_bad_host)
    );
}

async fn assert_request_boundary_denials(
    handle: &mut Handle,
    address: SocketAddr,
    credential: &SyntheticServiceCredential,
) {
    let headers = tool_call_headers("remember", &credential.bearer);
    let foreign = call_params(
        "remember",
        json!({
            "operation":"put", "content":"must not route", "idempotency_key":format!("foreign-{}", Uuid::now_v7()), "workspace_id":Uuid::now_v7()
        }),
    );
    let foreign_before = blocking_counts(handle);
    let (status, response) = raw_request(address, &headers, &rpc(8, "tools/call", foreign)).await;
    assert_eq!(
        status, 403,
        "foreign workspace must be rejected before write: {response}"
    );
    assert_eq!(
        durable_counts(blocking_counts(handle)),
        durable_counts(foreign_before)
    );

    let no_auth = [
        ("MCP-Protocol-Version", "2026-07-28"),
        ("Mcp-Method", "tools/call"),
        ("Mcp-Name", "remember"),
    ];
    let unauth_before = blocking_counts(handle);
    let (status, response) = raw_request(
        address,
        &no_auth,
        &rpc(9, "tools/call", call_params("remember", json!({"operation":"put","content":"unauthenticated","idempotency_key":"missing-auth"}))),
    )
    .await;
    assert_eq!(
        status, 401,
        "missing bearer must map to HTTP 401: {response}"
    );
    assert_eq!(
        durable_counts(blocking_counts(handle)),
        durable_counts(unauth_before)
    );

    assert_host_origin_rejections(handle, address, credential).await;

    tokio::task::block_in_place(|| handle.revoke_service_credential(credential));
    let revoked_before = blocking_counts(handle);
    let (status, response) = raw_request(
        address,
        &headers,
        &rpc(
            12,
            "tools/call",
            call_params(
                "remember",
                json!({"operation":"put", "content":"revoked", "idempotency_key":"revoked-request"}),
            ),
        ),
    )
    .await;
    assert_eq!(
        status, 401,
        "revoked credential is read fresh on each HTTP request: {response}"
    );
    assert_eq!(
        durable_counts(blocking_counts(handle)),
        durable_counts(revoked_before)
    );
}

async fn assert_authenticated_transport_negatives(
    handle: &mut Handle,
    address: SocketAddr,
    credential: &SyntheticServiceCredential,
) {
    let headers = tool_call_headers("remember", &credential.bearer);
    let unsupported = call_params(
        "remember",
        json!({
            "operation":"begin_batch", "client_batch_id":Uuid::now_v7(), "declared_count":1
        }),
    );
    let no_bmo_before = blocking_counts(handle);
    let (status, denied) = raw_request(address, &headers, &rpc(7, "tools/call", unsupported)).await;
    assert_eq!(
        status, 200,
        "valid but unimplemented action is an MCP error: {denied}"
    );
    assert_eq!(
        denied["result"]["isError"], true,
        "unimplemented action must be a tool error, not a JSON-RPC transport error: {denied}"
    );
    assert_eq!(
        denied["result"]["structuredContent"]["code"], "DEPENDENCY_UNAVAILABLE",
        "unimplemented action must fail closed with the canonical unavailable code: {denied}"
    );
    let no_bmo_after = blocking_counts(handle);
    assert_eq!(durable_counts(no_bmo_after), durable_counts(no_bmo_before));
    assert!(no_bmo_after.4 > no_bmo_before.4, "denial is audited");

    assert_request_boundary_denials(handle, address, credential).await;
}

fn assert_ack_loss_durable_outcome(handle: &mut Handle, before: (i64, i64, i64, i64, i64)) {
    let after = counts(handle);
    assert_eq!(
        (
            after.0 - before.0,
            after.1 - before.1,
            after.2 - before.2,
            after.3 - before.3,
        ),
        (1, 1, 1, 1),
        "receipt INSERT and COMMIT completed before its acknowledgement was discarded"
    );
    assert_eq!(
        receipt_outcome_counts(handle),
        (1, 1, 0),
        "one atomic finished-OK and one UNKNOWN observation; no terminal unavailable audit"
    );
}

#[test]
fn native_mcp_gateway_real_auth_atomic_replay_and_fail_closed_acceptance() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>(
        "native_mcp_gateway_real_auth_atomic_replay_and_fail_closed_acceptance",
        |mut handle| {
            handle.assert_gateway_login();
            let prefix = format!("mcp{}", &Uuid::now_v7().simple().to_string()[..12]);
            let wire = format!("{prefix}.{}", "a".repeat(32));
            let credential = handle.seed_synthetic_service_credential_and_window(
                SyntheticCredentialScopes::RememberWriteAndContextRead,
                &prefix,
                &wire,
                &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
                4,
            );
            let runtime_handle = handle.rt.handle().clone();
            let runtime = runtime_handle
                .block_on(handle.fresh_runtime())
                .expect("fresh checked gateway pool");
            let app = application(&handle, runtime);
            let runtime = handle.rt.handle().clone();

            runtime.block_on(async {
                let (address, server) = start(app).await;
                assert_authenticated_controls_do_not_create_durable_rows(
                    &mut handle,
                    address,
                    &credential.bearer,
                )
                .await;
                let after_context =
                    assert_workspace_context_row(&mut handle, address, &credential.bearer).await;
                assert_remember_replay(&mut handle, address, &credential.bearer, after_context)
                    .await;
                assert_authenticated_transport_negatives(&mut handle, address, &credential).await;

                server.abort();
            });
        },
    );
}

#[test]
#[ignore = "requires the isolated request-guard PostgreSQL fixture, pinned scanner and disposable Qdrant"]
#[allow(clippy::too_many_lines)] // Keep the real Gateway, Qdrant, PG and serving-race causal chain together.
fn native_gateway_semantic_recall_real_qdrant_pg_and_ryw_acceptance() {
    run_db_fixture::<Fixture, _>(
        "native_gateway_semantic_recall_real_qdrant_pg_and_ryw_acceptance",
        |mut handle| {
            handle.assert_gateway_login();
            let _registry_cleanup = SemanticProjectionCleanup {
                owner: handle.owner_client().expect("semantic cleanup owner"),
                tenant_id: handle.tenant_id,
            };
            let prefix = format!("recall{}", &Uuid::now_v7().simple().to_string()[..12]);
            let wire = format!("{prefix}.{}", "r".repeat(32));
            let credential = handle.seed_synthetic_service_credential_and_window(
                SyntheticCredentialScopes::RememberWriteAndContextRead,
                &prefix,
                &wire,
                &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
                16,
            );
            let first = handle.seed_workspace_visible_context_record();
            let second = handle.seed_workspace_visible_context_record();
            let first_point = Uuid::new_v4();
            let second_point = Uuid::new_v4();
            let first_updated = seed_semantic_registry_row(&mut handle, &first, first_point);
            let second_updated = seed_semantic_registry_row(&mut handle, &second, second_point);
            seed_semantic_checkpoint(&mut handle);

            let cell = CellId(Uuid::now_v7());
            let registry =
                semantic_qdrant_registry(cell, CallerId("gateway-semantic-acceptance".to_owned()));
            let transport = Arc::new(
                HttpIntraCellTransport::new(
                    registry.clone(),
                    Duration::from_secs(10),
                    humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES,
                )
                .expect("semantic Qdrant transport"),
            );
            let collection = format!("gateway_semantic_{}", Uuid::now_v7().simple());
            seed_tenant_placement(&mut handle, &collection);
            let runtime_handle = handle.rt.handle().clone();
            let runtime = runtime_handle
                .block_on(handle.fresh_runtime())
                .expect("fresh semantic Gateway runtime");
            let gateway_uid = runtime_handle.block_on(semantic_own_uid());
            let socket_path = runtime_handle.block_on(spawn_semantic_worker(gateway_uid));
            let embedding_port: Arc<
                dyn humaux_application::retrieval_embedding_port::RetrievalEmbeddingPort,
            > = Arc::new(
                humaux_gateway::retrieval_embedding_client::GatewayRetrievalEmbeddingClient::new(
                    Arc::new(
                        runtime_handle
                            .block_on(RuntimeDbPool::connect(
                                &std::env::var("HUMAUX_GATEWAY_PG_DSN").expect(
                                    "semantic Gateway fixture requires HUMAUX_GATEWAY_PG_DSN",
                                ),
                            ))
                            .expect("gateway runtime pool for the embedding client"),
                    ),
                    socket_path,
                    registry.clone(),
                    Duration::from_secs(30),
                ),
            );
            let semantic = SemanticRecallRuntime::new(
                semantic_scanner(),
                embedding_port.clone(),
                transport.clone(),
                registry.clone(),
                SemanticRecallVersions {
                    embedding_version: "embed-v1".to_owned(),
                    dimension: 4,
                },
                Duration::from_secs(10),
            )
            .expect("trusted semantic runtime");
            let app = application(&handle, runtime).with_semantic_recall(semantic);
            let query = "operation receipt scoped context";
            let workspace_id = handle.workspace_id;
            let tenant_id = handle.tenant_id;
            runtime_handle.block_on(async {
                create_semantic_collection(&transport, &registry, &collection).await;
                let permit = authorize_cell_access(
                    &registry,
                    IntraCellResource::QDRANT_REST,
                    Duration::from_secs(60),
                )
                .expect("semantic upsert permit");
                let first_payload = semantic_payload(&handle, first_updated);
                let second_payload = semantic_payload(&handle, second_updated);
                let vector = semantic_vector(query);
                upsert(
                    transport.as_ref(),
                    &permit,
                    &collection,
                    &[
                        (PointId::Uuid(first_point), &first_payload, vector.clone()),
                        (PointId::Uuid(second_point), &second_payload, vector),
                    ],
                    ha_profile_for(QdrantOperation::NormalImmutableUpsert),
                )
                .await
                .expect("real Qdrant semantic points");

                let (address, server) = start(app).await;
                let (status, unauthenticated) = recall_call(
                    address,
                    None,
                    json!({"query":query,"workspace_id":workspace_id,"mode":"semantic"}),
                )
                .await;
                assert_eq!(status, 401, "unauthenticated recall: {unauthenticated}");

                let foreign_workspace = tokio::task::block_in_place(|| handle.seed_workspace());
                let (status, denied) = recall_call(
                    address,
                    Some(&credential.bearer),
                    json!({"query":query,"workspace_id":foreign_workspace,"mode":"semantic"}),
                )
                .await;
                assert_eq!(status, 403, "workspace denial: {denied}");
                assert_eq!(denied["error"]["data"]["code"], "FORBIDDEN");

                let (status, no_token) = recall_call(
                    address,
                    Some(&credential.bearer),
                    json!({"query":query,"workspace_id":workspace_id,"mode":"semantic"}),
                )
                .await;
                assert_eq!(status, 200, "no-token semantic recall: {no_token}");
                let no_token = assert_tool_response(&no_token, ToolName::Recall);
                let returned = no_token["items"].as_array().expect("semantic items");
                assert_eq!(returned.len(), 2);
                let returned_ids = returned
                    .iter()
                    .map(|item| item["memory_id"].as_str().expect("memory id").to_owned())
                    .collect::<BTreeSet<String>>();
                assert_eq!(
                    returned_ids,
                    BTreeSet::from([first.memory_id.to_string(), second.memory_id.to_string()])
                );
                assert_eq!(no_token["provenance"]["projection_version"]["id"], "v1");
                assert_eq!(
                    no_token["provenance"]["embedding_model_id"]["id"],
                    "gateway-test-embedding@embed-v1"
                );
                assert_eq!(
                    no_token["provenance"]["rerank_model_id"],
                    json!({"status":"not_applicable"})
                );
                assert_eq!(
                    no_token["provenance"]["card_builder_version"],
                    json!({"status":"not_applicable"})
                );
                assert_eq!(no_token["completeness"]["reranked_count"], 0);
                assert_eq!(no_token["provenance"]["profile"]["lanes"], json!(["dense"]));

                let remember_args = json!({
                    "operation":"put",
                    "content":"semantic write awaiting projection",
                    "idempotency_key":format!("semantic-ryw-{}",Uuid::now_v7()),
                    "workspace_id":workspace_id,
                });
                let (status, remember) = raw_request(
                    address,
                    &tool_call_headers("remember", &credential.bearer),
                    &rpc(2, "tools/call", call_params("remember", remember_args)),
                )
                .await;
                assert_eq!(status, 200, "remember before RYW recall: {remember}");
                let consistency_token = remember["result"]["structuredContent"]
                    ["consistency_token"]
                    .as_str()
                    .expect("opaque consistency token")
                    .to_owned();
                let evidence_id = remember["result"]["structuredContent"]["evidence_id"]
                    .as_str()
                    .expect("remember evidence id")
                    .to_owned();
                let (status, with_token) = recall_call(
                    address,
                    Some(&credential.bearer),
                    json!({
                        "query":query,
                        "workspace_id":workspace_id,
                        "mode":"semantic",
                        "consistency_token":consistency_token.clone(),
                    }),
                )
                .await;
                assert_eq!(status, 200, "token semantic recall: {with_token}");
                let with_token = assert_tool_response(&with_token, ToolName::Recall);
                assert_eq!(with_token["items"].as_array().expect("RYW items").len(), 3);
                assert!(with_token["items"].as_array().unwrap().iter().any(|item| {
                    item["kind"] == "temporary_evidence" && item["evidence_id"] == evidence_id
                }));

                // §15.5 / ADR-0020 §7: read-your-writes is a *lower* bound only. A recall
                // carrying a consistency_token minted *before* a later, higher-seq lifecycle
                // write on the same workspace stream (here a supersede+restore) must still
                // resolve immediately — the pre-restore token's lower bound was satisfied the
                // moment it was issued, and restore only advances the stream head past it. RYW
                // never promises a stale token observes a *newer* write; to see the restored
                // state a caller uses restore's own (higher) token. This is the acceptance the
                // card requires "the answer this card must state and test" and closes the gap
                // where ADR-0020 §7 claimed RYW was tested but no recall ran with a pre-restore
                // token.
                let ryw_target = tokio::task::block_in_place(|| {
                    handle.seed_workspace_visible_context_record()
                });
                let ryw_successor = tokio::task::block_in_place(|| {
                    handle.seed_workspace_visible_context_record()
                });
                drive_supersede(
                    address,
                    &credential.bearer,
                    50,
                    ryw_target.memory_id,
                    ryw_successor.memory_id,
                )
                .await;
                let ryw_restore_token =
                    mint_restore_token(address, &credential.bearer, 52, ryw_target.memory_id).await;
                let (status, restored) = restore_call(
                    address,
                    &credential.bearer,
                    53,
                    ryw_target.memory_id,
                    Some(&ryw_restore_token),
                )
                .await;
                assert_eq!(status, 200, "pre-restore RYW: confirmed restore: {restored}");
                assert_ne!(
                    restored["result"]["isError"], true,
                    "the higher-seq restore succeeded: {restored}"
                );
                // The same pre-restore token (a *lower* stream seq than the restore just issued)
                // still returns immediately and obeys the RYW lower bound — same three items,
                // never blocked or rejected as not-yet-served.
                let (status, stale_token_recall) = recall_call(
                    address,
                    Some(&credential.bearer),
                    json!({
                        "query":query,
                        "workspace_id":workspace_id,
                        "mode":"semantic",
                        "consistency_token":consistency_token,
                    }),
                )
                .await;
                assert_eq!(
                    status, 200,
                    "pre-restore token recall returns, never blocks: {stale_token_recall}"
                );
                let stale_token_recall = assert_tool_response(&stale_token_recall, ToolName::Recall);
                assert_eq!(
                    stale_token_recall["items"]
                        .as_array()
                        .expect("pre-restore RYW items")
                        .len(),
                    3,
                    "the pre-restore token's lower bound is still met after a higher-seq restore"
                );
                assert!(stale_token_recall["items"].as_array().unwrap().iter().any(|item| {
                    item["kind"] == "temporary_evidence" && item["evidence_id"] == evidence_id
                }));

                tokio::task::block_in_place(|| {
                    handle
                        .admin
                        .execute(
                            "UPDATE private.memory_records SET content=$1,updated_at=clock_timestamp() \
                             WHERE tenant_id=$2 AND memory_id=$3",
                            &[&json!({"fixture":"source changed"}), &tenant_id, &first.memory_id],
                        )
                        .expect("mutate authoritative source fence");
                    handle
                        .admin
                        .execute(
                            "UPDATE private.memory_records SET status='revoked',updated_at=clock_timestamp() \
                             WHERE tenant_id=$1 AND memory_id=$2",
                            &[&tenant_id, &second.memory_id],
                        )
                        .expect("revoke authoritative semantic source");
                });
                let (status, invalidated) = recall_call(
                    address,
                    Some(&credential.bearer),
                    json!({"query":query,"workspace_id":workspace_id,"mode":"semantic"}),
                )
                .await;
                assert_eq!(status, 200, "invalidated semantic recall: {invalidated}");
                assert!(
                    assert_tool_response(&invalidated, ToolName::Recall)["items"]
                        .as_array()
                        .expect("invalidated items")
                        .is_empty(),
                    "changed and revoked PG sources must invalidate stale Qdrant candidates"
                );

                server.abort();
                let switch_transport = Arc::new(ServingSwitchAfterQuery {
                    inner: transport.clone(),
                    owner: std::sync::Mutex::new(Some(tokio::task::block_in_place(|| {
                        handle.owner_client().expect("serving-switch owner")
                    }))),
                    tenant_id,
                    workspace_id,
                    triggered: AtomicBool::new(false),
                });
                let race_runtime = handle
                    .fresh_runtime()
                    .await
                    .expect("fresh serving-race Gateway runtime");
                let race_semantic = SemanticRecallRuntime::new(
                    semantic_scanner(),
                    embedding_port.clone(),
                    switch_transport.clone(),
                    registry.clone(),
                    SemanticRecallVersions {
                        embedding_version: "embed-v1".to_owned(),
                        dimension: 4,
                    },
                    Duration::from_secs(10),
                )
                .expect("trusted serving-race runtime");
                let race_app = application(&handle, race_runtime).with_semantic_recall(race_semantic);
                let (race_address, race_server) = start(race_app).await;
                let (status, raced) = recall_call(
                    race_address,
                    Some(&credential.bearer),
                    json!({"query":query,"workspace_id":workspace_id,"mode":"semantic"}),
                )
                .await;
                assert_eq!(status, 200, "serving-switch race is a tool error: {raced}");
                assert_eq!(raced["result"]["isError"], true);
                assert_eq!(
                    raced["result"]["structuredContent"]["code"],
                    "DEPENDENCY_UNAVAILABLE",
                    "the final RR snapshot must reject candidates queried under retired v1"
                );
                assert!(switch_transport.triggered.load(Ordering::Acquire));
                race_server.abort();
                delete_semantic_collection(&transport, &registry, &collection).await;
            });
        },
    );
}

fn assert_context_governance(value: &Value, versioned: Uuid, unversioned: Uuid, overflow: bool) {
    CanonicalCatalog::load()
        .expect("catalog")
        .validate_output(ToolName::Context, value)
        .expect("typed Context output");
    let handoff = &value["handoff"];
    let content = &value["content"];
    assert!(
        handoff["not_judged"]
            .as_array()
            .expect("NotJudged IDs")
            .contains(&json!(versioned))
    );
    assert!(
        handoff["needs_verification"]
            .as_array()
            .expect("verification IDs")
            .iter()
            .any(|item| item["memory_id"] == json!(unversioned)
                && item["state"] == "recheck_required")
    );
    assert_eq!(handoff["counts"]["overflow"], overflow);
    assert_eq!(content["mandatory"]["overflow"], overflow);
    assert_eq!(handoff["counts"]["mandatory_expected"], 2);
    assert_eq!(content["mandatory"]["expected"], 2);
    assert_eq!(content["mandatory"]["returned"], u64::from(!overflow));
    assert_eq!(
        handoff["counts"]["mandatory_missing"],
        if overflow { 2 } else { 1 }
    );
    assert_eq!(
        content["mandatory"]["missing"], handoff["overflow_manifest"],
        "missing IDs are the overflow manifest, not the missing count"
    );
    assert_eq!(
        content["grounding"]["current"], 0,
        "NotJudged must never become Current"
    );
    if overflow {
        assert_eq!(content["items"], json!([]));
        assert_eq!(
            content["grounding"]["not_judged"], 0,
            "aggregate only returned bodies"
        );
        assert_eq!(
            content["completeness"]["reason"],
            "mandatory_context_overflow"
        );
        assert!(
            handoff["overflow_manifest"]
                .as_array()
                .expect("overflow IDs")
                .contains(&json!(versioned))
        );
    } else {
        assert_eq!(content["items"].as_array().expect("actual bodies").len(), 1);
        assert_eq!(content["items"][0]["memory_id"], json!(versioned));
        assert_eq!(content["grounding"]["not_judged"], 1);
    }
}

#[test]
#[allow(clippy::too_many_lines)] // One scoped fixture compares normal, rejected and overflow Context responses.
fn native_mcp_context_preserves_governance_on_overflow_and_rejects_unimplemented_routes() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>(
        "native_mcp_context_preserves_governance_on_overflow_and_rejects_unimplemented_routes",
        |mut handle| {
            let prefix = format!("ctx{}", &Uuid::now_v7().simple().to_string()[..12]);
            let wire = format!("{prefix}.{}", "d".repeat(32));
            let credential = handle.seed_synthetic_service_credential_and_window(
                SyntheticCredentialScopes::RememberWriteAndContextRead,
                &prefix,
                &wire,
                &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
                4,
            );
            let versioned = handle.seed_workspace_visible_context_record();
            let unversioned = handle.seed_workspace_visible_context_record();
            handle.admin.execute(
                "UPDATE private.memory_evidence SET grounding_mode='LIVE', recorded_version='fixture-v1' WHERE memory_id=$1",
                &[&versioned.memory_id],
            ).expect("fixture LIVE edge with recorded version");
            handle.admin.execute(
                "UPDATE private.memory_evidence SET grounding_mode='LIVE', recorded_version=NULL WHERE memory_id=$1",
                &[&unversioned.memory_id],
            ).expect("fixture LIVE edge requiring verification");
            let runtime_handle = handle.rt.handle().clone();
            let runtime = runtime_handle
                .block_on(handle.fresh_runtime())
                .expect("checked runtime");
            let app = application(&handle, runtime);
            runtime_handle.block_on(async {
                let (address, server) = start(app).await;
                let headers = tool_call_headers("context", &credential.bearer);
                let params = call_params("context", json!({"workspace_id":handle.workspace_id}));
                let (status, response) =
                    raw_request(address, &headers, &rpc(1, "tools/call", params.clone())).await;
                assert_eq!(status, 200, "Context governance: {response}");
                let normal = &response["result"]["structuredContent"];
                assert_context_governance(
                    normal,
                    versioned.memory_id,
                    unversioned.memory_id,
                    false,
                );
                let (status, inferred) = raw_request(
                    address,
                    &headers,
                    &rpc(5, "tools/call", call_params("context", json!({}))),
                )
                .await;
                assert_eq!(status, 200, "credential-bound workspace: {inferred}");
                assert_context_governance(
                    &inferred["result"]["structuredContent"],
                    versioned.memory_id,
                    unversioned.memory_id,
                    false,
                );
                for arguments in [
                    json!({"workspace_id":handle.workspace_id,"query":"explicitly unsupported"}),
                    json!({"workspace_id":handle.workspace_id,"limit":1}),
                    json!({"workspace_id":handle.workspace_id,"task_id":Uuid::now_v7()}),
                ] {
                    CanonicalCatalog::load()
                        .expect("catalog")
                        .validate(ToolName::Context, &arguments)
                        .expect("unsupported route still has valid input schema");
                    let before = durable_counts(blocking_counts(&mut handle));
                    let (status, denied) = raw_request(
                        address,
                        &headers,
                        &rpc(2, "tools/call", call_params("context", arguments)),
                    )
                    .await;
                    assert_eq!(status, 200, "valid unimplemented Context request: {denied}");
                    assert_eq!(denied["result"]["isError"], true);
                    assert_eq!(
                        denied["result"]["structuredContent"]["code"],
                        "DEPENDENCY_UNAVAILABLE"
                    );
                    assert_eq!(durable_counts(blocking_counts(&mut handle)), before);
                }
                let before = durable_counts(blocking_counts(&mut handle));
                let (status, invalid) = raw_request(
                    address,
                    &headers,
                    &rpc(
                        6,
                        "tools/call",
                        call_params(
                            "context",
                            json!({"workspace_id":handle.workspace_id,"project_id":Uuid::now_v7()}),
                        ),
                    ),
                )
                .await;
                assert_eq!(
                    status, 400,
                    "project_id is not a Context schema field: {invalid}"
                );
                assert_eq!(invalid["error"]["data"]["code"], "INVALID_INPUT");
                assert_eq!(durable_counts(blocking_counts(&mut handle)), before);
                let (status, denied) = raw_request(
                    address,
                    &headers,
                    &rpc(
                        3,
                        "tools/call",
                        call_params("context", json!({"workspace_id":Uuid::now_v7()})),
                    ),
                )
                .await;
                assert_eq!(
                    status, 403,
                    "cross-workspace Context must fail before body read: {denied}"
                );
                stop_server(server)
                    .await
                    .expect("stop normal Context server");

                let runtime = handle
                    .fresh_runtime()
                    .await
                    .expect("checked overflow runtime");
                let app = application_with_budget(
                    &handle,
                    runtime,
                    ContextBudget::new(0, 0).expect("zero budget"),
                );
                let (address, server) = start(app).await;
                let (status, response) =
                    raw_request(address, &headers, &rpc(4, "tools/call", params)).await;
                assert_eq!(status, 200, "Context overflow: {response}");
                let overflow = &response["result"]["structuredContent"];
                assert_context_governance(
                    overflow,
                    versioned.memory_id,
                    unversioned.memory_id,
                    true,
                );
                assert_eq!(
                    overflow["handoff"]["unavailable_selectors"],
                    normal["handoff"]["unavailable_selectors"]
                );
                stop_server(server)
                    .await
                    .expect("stop overflow Context server");
            });
        },
    );
}

fn memory_stream_key(handle: &Handle) -> StreamKey {
    StreamKey::new(
        TenantId(handle.tenant_id),
        "workspace",
        handle.workspace_id,
        "knowledge",
        "ingest",
        "v1",
    )
}

fn seed_done_stream_identity(handle: &mut Handle, record: &ScopedContextRecord) -> StreamKey {
    let key = memory_stream_key(handle);
    handle
        .admin
        .execute(
            "INSERT INTO projection.stream_checkpoints \
             (tenant_id,scope_kind,scope_id,domain,projection_kind,projection_version,issued_highwater) \
             VALUES($1,$2,$3,$4,$5,$6,1)",
            &[
                &key.tenant_id.0,
                &key.scope_kind,
                &key.scope_id,
                &key.domain,
                &key.projection_kind,
                &key.projection_version,
            ],
        )
        .expect("owner seeds tombstone checkpoint identity");
    let commit_seq: i64 = handle
        .admin
        .query_one("SELECT nextval('ops.commit_seq_seq')", &[])
        .expect("owner allocates fixture commit sequence")
        .get(0);
    handle
        .admin
        .execute(
            "INSERT INTO projection.stream_log \
             (tenant_id,scope_kind,scope_id,domain,projection_kind,projection_version, \
              stream_seq,commit_seq,state,settled_at) \
             VALUES($1,$2,$3,$4,$5,$6,1,$7,'DONE',clock_timestamp())",
            &[
                &key.tenant_id.0,
                &key.scope_kind,
                &key.scope_id,
                &key.domain,
                &key.projection_kind,
                &key.projection_version,
                &commit_seq,
            ],
        )
        .expect("owner seeds DONE stream identity for legal tombstone");
    handle
        .admin
        .execute(
            "INSERT INTO ops.outbox(tenant_id,commit_seq,stream_seq,event_type,evidence_id) \
             VALUES($1,$2,1,'EVIDENCE_ACCEPTED',$3)",
            &[&handle.tenant_id, &commit_seq, &record.evidence_id],
        )
        .expect("owner seeds matching outbox evidence identity");
    key
}

fn set_record_user_visibility(handle: &mut Handle, record: &ScopedContextRecord, user_id: Uuid) {
    let mut txn = handle
        .admin
        .transaction()
        .expect("begin user-private fixture mutation");
    for (table, id) in [
        ("private.memory_records", record.memory_id),
        ("private.evidence_objects", record.evidence_id),
    ] {
        txn.execute(
            &format!(
                "UPDATE {table} SET visibility_class='USER_PRIVATE', \
                 visibility_user_id=$2,visibility_workspace_id=NULL WHERE {id_column}=$1",
                id_column = if table == "private.memory_records" {
                    "memory_id"
                } else {
                    "evidence_id"
                },
            ),
            &[&id, &user_id],
        )
        .expect("make fixture row user-private");
    }
    txn.commit().expect("commit user-private fixture mutation");
}

fn set_record_workspace_visibility(
    handle: &mut Handle,
    record: &ScopedContextRecord,
    workspace_id: Uuid,
) {
    let mut txn = handle
        .admin
        .transaction()
        .expect("begin workspace-private fixture mutation");
    for (table, id) in [
        ("private.memory_records", record.memory_id),
        ("private.evidence_objects", record.evidence_id),
    ] {
        txn.execute(
            &format!(
                "UPDATE {table} SET visibility_class='WORKSPACE_SHARED', \
                 visibility_user_id=NULL,visibility_workspace_id=$2 WHERE {id_column}=$1",
                id_column = if table == "private.memory_records" {
                    "memory_id"
                } else {
                    "evidence_id"
                },
            ),
            &[&id, &workspace_id],
        )
        .expect("make fixture row workspace-shared");
    }
    txn.commit()
        .expect("commit workspace-private fixture mutation");
}

fn assert_memory_not_found(response: &Value) {
    let result = &response["result"];
    assert_eq!(
        result["isError"], true,
        "memory.get object error: {response}"
    );
    let structured = &result["structuredContent"];
    assert_eq!(
        structured["code"], "NOT_FOUND",
        "object error code: {response}"
    );
    assert!(
        structured["items"].is_null()
            || structured["items"]
                .as_array()
                .is_some_and(|items| items.is_empty()),
        "NotFound must not expose item rows: {response}"
    );
    assert!(
        !response
            .to_string()
            .contains("operation receipt scoped context"),
        "NotFound must not expose fixture content: {response}"
    );
    assert_eq!(
        result["content"][0]["text"], structured["code"],
        "MCP tool errors mirror the code as plain text"
    );
}

async fn assert_memory_object_not_found(
    address: SocketAddr,
    headers: &[(&str, &str)],
    request_id: u64,
    memory_id: Uuid,
) {
    let (status, response) = raw_request(
        address,
        headers,
        &rpc(
            request_id,
            "tools/call",
            call_params("memory", json!({"action":"get","memory_id":memory_id})),
        ),
    )
    .await;
    assert_eq!(
        status, 200,
        "object denial remains an MCP result: {response}"
    );
    assert_memory_not_found(&response);
}

#[test]
#[allow(clippy::too_many_lines)] // One real HTTP fixture preserves the complete object-error matrix.
fn native_mcp_memory_get_authorization_and_lifecycle_matrix() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>(
        "native_mcp_memory_get_authorization_and_lifecycle_matrix_foreign",
        |mut foreign| {
            let foreign_memory = foreign.seed_workspace_visible_context_record();
            let foreign_workspace = foreign.workspace_id;
            run_db_fixture::<Fixture, _>(
                "native_mcp_memory_get_authorization_and_lifecycle_matrix",
                |mut handle| {
                    handle.assert_gateway_login();
                    let prefix = format!("mget{}", &Uuid::now_v7().simple().to_string()[..12]);
                    let wire = format!("{prefix}.{}", "f".repeat(32));
                    let credential = handle.seed_synthetic_service_credential_and_window(
                        SyntheticCredentialScopes::ContextRead,
                        &prefix,
                        &wire,
                        &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
                        32,
                    );
                    let visible = handle.seed_workspace_visible_context_record();
                    let own_private = handle.seed_workspace_visible_context_record();
                    let own_user = handle.user_id;
                    set_record_user_visibility(&mut handle, &own_private, own_user);
                    let peer_user = handle.seed_peer_user();
                    let peer_private = handle.seed_workspace_visible_context_record();
                    set_record_user_visibility(&mut handle, &peer_private, peer_user);
                    let hidden_workspace = handle.seed_workspace();
                    let workspace_hidden = handle.seed_workspace_visible_context_record();
                    set_record_workspace_visibility(
                        &mut handle,
                        &workspace_hidden,
                        hidden_workspace,
                    );
                    let supporting_hidden = handle.seed_workspace_visible_context_record();
                    let hidden_source = handle.seed_workspace_visible_context_record();
                    set_record_workspace_visibility(&mut handle, &hidden_source, hidden_workspace);
                    handle
                        .admin
                        .execute(
                            "INSERT INTO private.memory_evidence(memory_id,evidence_id,role,grounding_mode) \
                             VALUES($1,$2,'SUPPORTING','SNAPSHOT')",
                            &[&supporting_hidden.memory_id, &hidden_source.evidence_id],
                        )
                        .expect("owner links hidden supporting Evidence");
                    let secret = handle.seed_workspace_visible_context_record();
                    handle
                        .admin
                        .execute(
                            "UPDATE private.evidence_objects SET data_class='SECRET_MATERIAL' WHERE evidence_id=$1",
                            &[&secret.evidence_id],
                        )
                        .expect("owner marks secret source");
                    let expired = handle.seed_workspace_visible_context_record();
                    let revoked = handle.seed_workspace_visible_context_record();
                    let superseded = handle.seed_workspace_visible_context_record();
                    for (record, sql) in [
                        (
                            &expired,
                            "UPDATE private.memory_records SET status='expired' WHERE memory_id=$1",
                        ),
                        (
                            &revoked,
                            "UPDATE private.memory_records SET status='revoked' WHERE memory_id=$1",
                        ),
                        (
                            &superseded,
                            "UPDATE private.memory_records SET status='superseded',superseded_by=memory_id WHERE memory_id=$1",
                        ),
                    ] {
                        handle
                            .admin
                            .execute(sql, &[&record.memory_id])
                            .expect("owner seeds lifecycle state");
                    }
                    let tombstoned = handle.seed_workspace_visible_context_record();
                    let tombstone_key = seed_done_stream_identity(&mut handle, &tombstoned);
                    let tombstoned_once = handle.rt.block_on(forget_repo::tombstone(
                        &handle.maintenance,
                        &tombstone_key,
                        1,
                    ));
                    assert!(tombstoned_once.expect("maintenance tombstone"));
                    let runtime_handle = handle.rt.handle().clone();
                    let runtime = runtime_handle
                        .block_on(handle.fresh_runtime())
                        .expect("checked matrix runtime");
                    let app = application(&handle, runtime);
                    let own_workspace = handle.workspace_id;
                    runtime_handle.block_on(async {
                        let (address, server) = start(app).await;
                        let headers = tool_call_headers("memory", &credential.bearer);
                        for arguments in [
                            json!({"action":"get","memory_id":visible.memory_id}),
                            json!({"action":"get","memory_id":visible.memory_id,"workspace_id":own_workspace}),
                        ] {
                            let (status, response) = raw_request(
                                address,
                                &headers,
                                &rpc(1, "tools/call", call_params("memory", arguments)),
                            )
                            .await;
                            assert_eq!(status, 200, "visible memory.get: {response}");
                            assert_memory_response(
                                &response,
                                visible.memory_id,
                                5,
                                &std::env::current_exe().expect("test binary"),
                            );
                        }
                        let (status, response) = raw_request(
                            address,
                            &headers,
                            &rpc(2, "tools/call", call_params("memory", json!({"action":"get","memory_id":own_private.memory_id}))),
                        )
                        .await;
                        assert_eq!(status, 200, "owner reads own private memory: {response}");
                        assert_memory_response(
                            &response,
                            own_private.memory_id,
                            5,
                            &std::env::current_exe().expect("test binary"),
                        );
                        let (status, page) = enumerate_call(address, &credential.bearer,
                            json!({"action":"enumerate","limit":100})).await;
                        assert_eq!(status, 200, "initial authorized universe: {page}");
                        let mut expected = [visible.memory_id, own_private.memory_id];
                        expected.sort_unstable_by(|a,b| b.cmp(a));
                        assert!(assert_enumeration_response(&page, &expected).is_none());
                        let before = final_completeness_count();
                        for (request_id, memory_id) in [
                            (3, Uuid::now_v7()),
                            (4, foreign_memory.memory_id),
                            (5, peer_private.memory_id),
                            (6, workspace_hidden.memory_id),
                            (7, supporting_hidden.memory_id),
                            (8, secret.memory_id),
                            (9, expired.memory_id),
                            (10, revoked.memory_id),
                            (11, superseded.memory_id),
                            (12, tombstoned.memory_id),
                        ] {
                            assert_memory_object_not_found(address, &headers, request_id, memory_id).await;
                        }
                        let (status, bad_id) = raw_request(
                            address,
                            &headers,
                            &rpc(13, "tools/call", call_params("memory", json!({"action":"get","memory_id":"bad"}))),
                        )
                        .await;
                        assert_eq!(status, 400, "bad UUID is request-invalid: {bad_id}");
                        assert_eq!(bad_id["error"]["data"]["code"], "INVALID_INPUT");
                        let (status, foreign_scope) = raw_request(
                            address,
                            &headers,
                            &rpc(14, "tools/call", call_params("memory", json!({"action":"get","memory_id":visible.memory_id,"workspace_id":foreign_workspace}))),
                        )
                        .await;
                        assert_eq!(status, 403, "foreign workspace rejects before object lookup: {foreign_scope}");
                        assert_eq!(
                            final_completeness_count(),
                            before,
                            "object denials and request failures emit no final metric",
                        );
                        tokio::task::block_in_place(|| handle.revoke_service_credential(&credential));
                        let (status, revoked_credential) = raw_request(
                            address,
                            &headers,
                            &rpc(15, "tools/call", call_params("memory", json!({"action":"get","memory_id":visible.memory_id}))),
                        )
                        .await;
                        assert_eq!(status, 401, "credential revocation is fresh: {revoked_credential}");
                        assert_eq!(
                            final_completeness_count(),
                            before,
                            "revoked credentials emit no final metric",
                        );
                        stop_server(server).await.expect("stop matrix server");
                    });
                },
            );
        },
    );
}

#[test]
fn native_mcp_context_records_final_metric_only_after_quota_settlement() {
    assert_local_read_final_metric_after_settlement(
        "context",
        "context.assemble",
        ReadSettlementFault::QuotaLock,
    );
}

#[test]
fn native_mcp_memory_get_records_final_metric_only_after_quota_settlement() {
    assert_local_read_final_metric_after_settlement(
        "memory",
        "memory.get",
        ReadSettlementFault::QuotaLock,
    );
}

#[test]
fn native_mcp_memory_get_records_no_final_metric_when_success_audit_fails() {
    assert_local_read_final_metric_after_settlement(
        "memory",
        "memory.get",
        ReadSettlementFault::AuditConstraint,
    );
}

#[test]
fn native_mcp_memory_enumerate_records_final_metric_only_after_quota_settlement() {
    assert_local_read_final_metric_after_settlement(
        "memory",
        "memory.enumerate",
        ReadSettlementFault::QuotaLock,
    );
}

#[test]
fn native_mcp_memory_enumerate_records_no_final_metric_when_success_audit_fails() {
    assert_local_read_final_metric_after_settlement(
        "memory",
        "memory.enumerate",
        ReadSettlementFault::AuditConstraint,
    );
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadSettlementFault {
    QuotaLock,
    AuditConstraint,
}

struct AuditConstraintCleanup {
    owner: postgres::Client,
    name: Option<String>,
}

impl AuditConstraintCleanup {
    fn clear(&mut self) -> Result<(), postgres::Error> {
        if let Some(name) = &self.name {
            self.owner.batch_execute(&format!(
                "ALTER TABLE control.audit_events DROP CONSTRAINT IF EXISTS {name}"
            ))?;
            self.name = None;
        }
        Ok(())
    }
}

impl Drop for AuditConstraintCleanup {
    fn drop(&mut self) {
        if let Err(error) = self.clear() {
            eprintln!("fixture audit constraint cleanup failed: {error}");
        }
    }
}

#[allow(clippy::too_many_lines)] // Keep real DB barriers, cleanup and the accepted control in one causal scope.
fn assert_local_read_final_metric_after_settlement(
    tool: &'static str,
    operation: &'static str,
    fault: ReadSettlementFault,
) {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>(operation, |mut handle| {
        let prefix = format!("settle{}", &Uuid::now_v7().simple().to_string()[..12]);
        let wire = format!("{prefix}.{}", "e".repeat(32));
        let credential = handle.seed_synthetic_service_credential_and_window(
            SyntheticCredentialScopes::RememberWriteAndContextRead,
            &prefix,
            &wire,
            &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
            4,
        );
        let memory = handle.seed_workspace_visible_context_record();
        let runtime_handle = handle.rt.handle().clone();
        let runtime = runtime_handle
            .block_on(handle.fresh_runtime())
            .expect("checked settlement runtime");
        let app = application(&handle, runtime);
        let tenant_id = handle.tenant_id;
        let audit_constraint = format!("direct_get_audit_{}", tenant_id.simple());
        // Declared outside block_on: the synchronous owner also cleans up on async unwind.
        let mut audit_cleanup =
            (fault == ReadSettlementFault::AuditConstraint).then(|| AuditConstraintCleanup {
                owner: handle.owner_client().expect("checked audit cleanup owner"),
                name: Some(audit_constraint.clone()),
            });
        let arguments = match operation {
            "context.assemble" => json!({"workspace_id":handle.workspace_id}),
            "memory.get" => json!({"action":"get","memory_id":memory.memory_id}),
            "memory.enumerate" => json!({"action":"enumerate"}),
            _ => panic!("unsupported local-read test route"),
        };
        let params = call_params(tool, arguments);
        let mut settlement_blocker = postgres::Client::connect(
            &handle.gateway_application_dsn("local-read-settlement-barrier"),
            postgres::NoTls,
        )
        .expect("isolated settlement lock uses actual gateway login");
        let blocker_pid: i32 = settlement_blocker
            .query_one("SELECT pg_backend_pid()", &[])
            .expect("settlement blocker identity")
            .get(0);
        runtime_handle.block_on(async {
            let before = final_completeness_count();
            let mut settlement = tokio::task::block_in_place(|| settlement_blocker.transaction())
                .expect("begin isolated settlement lock");
            tokio::task::block_in_place(|| {
                settlement.query_one(
                    "SELECT set_config('humaux.tenant_id',$1,true)",
                    &[&tenant_id.to_string()],
                )
            })
            .expect("scope settlement lock to fixture tenant");
            let mut barrier = tokio::task::block_in_place(|| handle.admin.transaction())
                .expect("begin isolated local-read barrier");
            tokio::task::block_in_place(|| {
                barrier.batch_execute("LOCK TABLE private.memory_records IN ACCESS EXCLUSIVE MODE")
            })
            .expect("pause actual local-read materialization");
            let (address, server) = start(app).await;
            let bearer = credential.bearer.clone();
            let body = rpc(1, "tools/call", params.clone());
            let mut request = tokio::spawn(async move {
                try_raw_request(address, &tool_call_headers(tool, &bearer), &body).await
            });

            // Synchronize on the real PG read, not elapsed sleep: admission has committed
            // its reservation and the actual role_gateway body query is blocked here.
            let observed = tokio::task::block_in_place(|| {
                reserved_read_at_read_barrier(&mut barrier, tenant_id, operation)
            });
            let locked = tokio::task::block_in_place(|| {
                observed
                    .as_ref()
                        .map_err(Clone::clone)
                        .and_then(|reservation_id| {
                            match fault {
                                ReadSettlementFault::QuotaLock => settlement
                                    .query_one(
                                        "SELECT reservation_id FROM control.usage_reservations \
                             WHERE tenant_id=$1 AND reservation_id=$2 FOR UPDATE",
                                        &[&tenant_id, reservation_id],
                                    )
                                    .map(|_| ())
                                    .map_err(|_| "lock isolated local-read reservation".to_owned()),
                                ReadSettlementFault::AuditConstraint => barrier
                                    .batch_execute(&format!(
                                        "ALTER TABLE control.audit_events ADD CONSTRAINT {audit_constraint} \
                                         CHECK (tenant_id <> '{tenant_id}'::uuid OR action <> 'MCP_REQUEST_FINISHED') NOT VALID"
                                    ))
                                    .map_err(|_| "install fixture-only success-audit failure".to_owned()),
                            }
                        })
                });
                let release_read = tokio::task::block_in_place(|| match fault {
                    ReadSettlementFault::QuotaLock => barrier.rollback(),
                    ReadSettlementFault::AuditConstraint => barrier.commit(),
                });
                let reached_settlement = if fault == ReadSettlementFault::QuotaLock
                    && locked.is_ok() && release_read.is_ok() {
                    Some(tokio::task::block_in_place(|| {
                        wait_for_gateway_blocked_by(&mut handle.admin, blocker_pid)
                    }))
                } else {
                    None
                };
            let response = tokio::time::timeout(Duration::from_secs(8), &mut request).await;
            if response.is_err() {
                request.abort();
                let _ = request.await;
            }
                let release_settlement = tokio::task::block_in_place(|| settlement.rollback());
                let stopped = stop_server(server).await;
                let release_audit_constraint = audit_cleanup.as_mut().map(|cleanup| {
                    tokio::task::block_in_place(|| cleanup.clear())
                });
                // Always release both PG locks and the server before asserting. No immutable
                // reservation fields, trigger or role grants are weakened by either fault.
                release_read.expect("release isolated local-read barrier");
                release_settlement.expect("release isolated settlement lock");
                if let Some(cleanup) = release_audit_constraint {
                    cleanup.expect("remove only the fixture's added audit constraint");
                }
                let reservation_id = observed.expect("observed actual local read");
                locked.expect("installed the fault before the read could prepare output");
                if fault == ReadSettlementFault::QuotaLock {
                    reached_settlement.expect("read barrier transferred to settlement")
                        .expect("prepared output reached actual quota settlement");
                }
            stopped.expect("stop settlement-failure server");
            let (status, failed) = response
                .expect("settlement response before timeout")
                .expect("settlement request task")
                .expect("settlement HTTP response");
            assert_eq!(status, 200, "post-handler settlement response: {failed}");
            assert_eq!(failed["result"]["isError"], true);
            assert_eq!(
                failed["result"]["structuredContent"]["code"],
                match fault {
                    ReadSettlementFault::QuotaLock => "DEPENDENCY_UNAVAILABLE",
                    ReadSettlementFault::AuditConstraint => "INTERNAL",
                }
            );
            assert_eq!(
                final_completeness_count(),
                before,
                    "a prepared output rejected by quota or audit must emit no final metric"
            );
            let settled = tokio::task::block_in_place(|| {
                handle.admin.query_one(
                    "SELECT r.status,w.reserved,w.consumed \
                         FROM control.usage_reservations r JOIN control.quota_windows w \
                         USING(tenant_id,entitlement_key,window_start) \
                         WHERE r.tenant_id=$1 AND r.reservation_id=$2",
                    &[&tenant_id, &reservation_id],
                )
            })
            .expect("read actual settlement outcome");
            assert_eq!(settled.get::<_, String>(0), "RESERVED");
            assert_eq!(settled.get::<_, i64>(1), 1);
            assert_eq!(settled.get::<_, i64>(2), 0);

            let runtime = handle
                .fresh_runtime()
                .await
                .expect("checked success runtime");
            let (address, server) = start(application(&handle, runtime)).await;
            let response = try_raw_request(
                address,
                &tool_call_headers(tool, &credential.bearer),
                &rpc(2, "tools/call", params),
            )
            .await;
            stop_server(server)
                .await
                .expect("stop successful local-read server");
            let (status, accepted) = response.expect("successful read HTTP response");
            assert_eq!(status, 200, "accepted local read: {accepted}");
            if operation == "memory.enumerate" {
                assert!(assert_enumeration_response(&accepted, &[memory.memory_id]).is_none());
            } else {
                let assert_response = if tool == "memory" { assert_memory_response } else { assert_context_response };
                assert_response(&accepted, memory.memory_id, 5, &std::env::current_exe().expect("test binary"));
            }
            assert_eq!(
                final_completeness_count(),
                before + 1,
                "accepted output emits once"
            );
        });
    });
}

fn reserved_read_at_read_barrier(
    barrier: &mut postgres::Transaction<'_>,
    tenant_id: Uuid,
    operation: &str,
) -> Result<Uuid, String> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let row = barrier
            .query_opt(
                "SELECT reservation_id FROM control.usage_reservations \
             WHERE tenant_id=$1 AND operation=$2 AND status='RESERVED' \
             AND EXISTS (SELECT 1 FROM pg_locks l JOIN pg_stat_activity a USING(pid) \
                 WHERE l.relation='private.memory_records'::regclass \
                   AND l.mode='AccessShareLock' AND NOT l.granted \
                   AND a.datname=current_database() AND a.usename='role_gateway')",
                &[&tenant_id, &operation],
            )
            .map_err(|_| "read isolated local-read barrier state".to_owned())?;
        if let Some(row) = row {
            return Ok(row.get(0));
        }
        if Instant::now() >= deadline {
            return Err("actual local read did not reach PG barrier within 2s".to_owned());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_gateway_blocked_by(
    admin: &mut postgres::Client,
    blocker_pid: i32,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let blocked: bool = admin
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity a \
             WHERE a.datname=current_database() AND a.usename='role_gateway' \
               AND $1=ANY(pg_blocking_pids(a.pid)))",
                &[&blocker_pid],
            )
            .map_err(|_| "observe actual Context settlement lock".to_owned())?
            .get(0);
        if blocked {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("prepared Context did not reach settlement lock within 2s".to_owned());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn native_mcp_gateway_commit_ack_loss_is_unknown_and_replays_the_committed_receipt() {
    run_db_fixture::<Fixture, _>(
        "native_mcp_gateway_commit_ack_loss_is_unknown_and_replays_the_committed_receipt",
        |mut handle| {
            handle.assert_gateway_login();
            let prefix = format!("ack{}", &Uuid::now_v7().simple().to_string()[..12]);
            let wire = format!("{prefix}.{}", "b".repeat(32));
            let credential = handle.seed_synthetic_service_credential_and_window(
                SyntheticCredentialScopes::RememberWrite,
                &prefix,
                &wire,
                &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
                4,
            );
            let before = counts(&mut handle);
            let runtime_handle = handle.rt.handle().clone();
            let proxy = runtime_handle
                .block_on(start_commit_ack_proxy())
                .expect("start COMMIT-ACK proxy");
            let proxied_runtime =
                match runtime_handle.block_on(handle.runtime_via_loopback_proxy(proxy.port())) {
                    Ok(pool) => pool,
                    Err(error) => {
                        let _ = runtime_handle.block_on(proxy.shutdown());
                        panic!("connect checked gateway runtime through proxy: {error}");
                    }
                };
            let app = application(&handle, proxied_runtime);
            let key = format!("ack-loss-{}", Uuid::now_v7());
            let arguments = json!({
                "operation": "put",
                "content": "COMMIT acknowledgement must not decide durable truth",
                "idempotency_key": key,
                "workspace_id": handle.workspace_id,
            });
            let runtime = handle.rt.handle().clone();
            let responses = runtime.block_on(async {
                let (address, server) = start(app).await;
                let headers = tool_call_headers("remember", &credential.bearer);
                let first = try_raw_request(
                    address,
                    &headers,
                    &rpc(
                        101,
                        "tools/call",
                        call_params("remember", arguments.clone()),
                    ),
                )
                .await;
                let retry = try_raw_request(
                    address,
                    &headers,
                    &rpc(102, "tools/call", call_params("remember", arguments)),
                )
                .await;
                let server_stop = stop_server(server).await;
                let proxy_stop = proxy.shutdown().await;
                (first, retry, server_stop, proxy_stop)
            });
            let (first, retry, server_stop, proxy_stop) = responses;
            assert!(server_stop.is_ok(), "{server_stop:?}");
            let (receipt_insert_sent, commit_ack_dropped) = proxy_stop.expect("proxy joins");
            assert!(
                receipt_insert_sent,
                "proxy must observe the actual receipt INSERT bytes; first={first:?}; retry={retry:?}"
            );
            assert!(
                commit_ack_dropped,
                "proxy must discard PostgreSQL CommandComplete(COMMIT); first={first:?}; retry={retry:?}"
            );

            let (first_status, first_response) =
                first.expect("first HTTP response after dropped ACK");
            assert_eq!(
                first_status, 200,
                "MCP tool error response: {first_response}"
            );
            assert_eq!(
                first_response["result"]["structuredContent"]["code"], "DEPENDENCY_UNAVAILABLE",
                "a lost COMMIT acknowledgement is retryable, not a terminal failure: {first_response}"
            );
            assert_ack_loss_durable_outcome(&mut handle, before);
            let after_operations = counts(&mut handle);

            let (retry_status, retry_response) = retry.expect("fresh retry HTTP response");
            assert_eq!(retry_status, 200, "retry: {retry_response}");
            let replayed = &retry_response["result"]["structuredContent"];
            assert_eq!(
                replayed["replayed"], true,
                "retry must use the committed receipt: {retry_response}"
            );
            assert!(
                replayed["evidence_id"].is_string(),
                "retry returns the original Evidence: {retry_response}"
            );
            assert_eq!(
                durable_counts(counts(&mut handle)),
                durable_counts(after_operations)
            );
        },
    );
}

const GATEWAY_ENV_PREFIX: &str = "HUMAUX_GATEWAY_";
const GATEWAY_PROCESS_START_TIMEOUT: Duration = Duration::from_secs(5);
const GATEWAY_PROCESS_STOP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct GatewayProcessConfig {
    values: BTreeMap<String, String>,
    address: SocketAddr,
}

impl GatewayProcessConfig {
    fn for_fixture(handle: &Handle, address: SocketAddr) -> Self {
        let mut values = BTreeMap::from([
            ("HUMAUX_GATEWAY_BIND_ADDR".into(), address.to_string()),
            ("HUMAUX_GATEWAY_ALLOWED_HOSTS".into(), HOST.into()),
            ("HUMAUX_GATEWAY_ALLOWED_ORIGINS".into(), ORIGIN.into()),
            (
                "HUMAUX_GATEWAY_MAX_REQUEST_BODY_BYTES".into(),
                "65536".into(),
            ),
            (
                "HUMAUX_GATEWAY_PG_DSN".into(),
                handle.gateway_dsn_for_process().to_owned(),
            ),
            (
                "HUMAUX_GATEWAY_CREDENTIAL_PEPPER_HEX".into(),
                hex::encode(SYNTHETIC_CREDENTIAL_PEPPER),
            ),
            ("HUMAUX_GATEWAY_TRUSTED_PROXY_CIDRS".into(), String::new()),
            ("HUMAUX_GATEWAY_MAX_FORWARDED_HOPS".into(), "1".into()),
            ("HUMAUX_GATEWAY_GLOBAL_DENYLIST".into(), String::new()),
            (
                "HUMAUX_GATEWAY_GLOBAL_EMERGENCY_ALLOWLIST".into(),
                String::new(),
            ),
            ("HUMAUX_GATEWAY_RESERVATION_TTL_SECONDS".into(), "30".into()),
            ("HUMAUX_GATEWAY_HANDLER_TIMEOUT_SECONDS".into(), "5".into()),
            ("HUMAUX_GATEWAY_FINALIZE_TIMEOUT_SECONDS".into(), "2".into()),
            ("HUMAUX_GATEWAY_REPLAY_TTL_SECONDS".into(), "60".into()),
            (
                "HUMAUX_GATEWAY_CONFIRM_TOKEN_TTL_SECONDS".into(),
                "300".into(),
            ),
            ("HUMAUX_GATEWAY_UNDO_WINDOW_SECONDS".into(), "86400".into()),
            (
                "HUMAUX_GATEWAY_REMEMBER_TENANT_ID".into(),
                handle.tenant_id.to_string(),
            ),
            (
                "HUMAUX_GATEWAY_REMEMBER_WORKSPACE_ID".into(),
                handle.workspace_id.to_string(),
            ),
            (
                "HUMAUX_GATEWAY_REMEMBER_SCOPE_KIND".into(),
                "workspace".into(),
            ),
            ("HUMAUX_GATEWAY_REMEMBER_DOMAIN".into(), "knowledge".into()),
            (
                "HUMAUX_GATEWAY_REMEMBER_PROJECTION_KIND".into(),
                "ingest".into(),
            ),
            (
                "HUMAUX_GATEWAY_REMEMBER_PROJECTION_VERSION".into(),
                "v1".into(),
            ),
            (
                "HUMAUX_GATEWAY_REMEMBER_REASONING_DOMAIN_ID".into(),
                handle.reasoning_domain_id.to_string(),
            ),
            (
                "HUMAUX_GATEWAY_REMEMBER_TOKEN_TTL_SECONDS".into(),
                "60".into(),
            ),
            (
                "HUMAUX_GATEWAY_REMEMBER_DATA_CLASS".into(),
                "INTERNAL".into(),
            ),
            (
                "HUMAUX_GATEWAY_REMEMBER_VISIBILITY_CLASS".into(),
                "WORKSPACE_SHARED".into(),
            ),
            (
                "HUMAUX_GATEWAY_REMEMBER_EVENT_KIND".into(),
                "USER_MESSAGE".into(),
            ),
            ("HUMAUX_GATEWAY_CONTEXT_TOTAL_TOKENS".into(), "2048".into()),
            (
                "HUMAUX_GATEWAY_CONTEXT_MANDATORY_TOKENS".into(),
                "1024".into(),
            ),
        ]);
        for name in ["PREAUTH_IP", "CREDENTIAL", "USER", "TENANT", "OPERATION"] {
            values.insert(format!("HUMAUX_GATEWAY_RATE_{name}_CAPACITY"), "100".into());
            values.insert(
                format!("HUMAUX_GATEWAY_RATE_{name}_REFILL_PER_SECOND"),
                "100".into(),
            );
        }
        // Retrieval settings deliberately use their registered defaults unless a case overrides them.
        Self { values, address }
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_humaux-gateway"));
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with(GATEWAY_ENV_PREFIX) {
                command.env_remove(key);
            }
        }
        command
            .envs(&self.values)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command
    }
}

struct GatewayProcess {
    child: Option<Child>,
    address: SocketAddr,
}

impl GatewayProcess {
    fn start(config: &GatewayProcessConfig) -> Result<Self, String> {
        let child = config
            .command()
            .spawn()
            .map_err(|_| "spawn gateway binary".to_owned())?;
        let mut process = Self {
            child: Some(child),
            address: config.address,
        };
        process.wait_for_listener()?;
        Ok(process)
    }

    fn wait_for_listener(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + GATEWAY_PROCESS_START_TIMEOUT;
        loop {
            if StdTcpStream::connect_timeout(&self.address, Duration::from_millis(25)).is_ok() {
                return Ok(());
            }
            let status = self
                .child
                .as_mut()
                .ok_or_else(|| "gateway binary is already reaped".to_owned())?
                .try_wait()
                .map_err(|_| "inspect gateway binary".to_owned())?;
            if let Some(status) = status {
                self.child.take();
                return status
                    .success()
                    .then_some(())
                    .ok_or_else(|| "gateway binary rejected startup configuration".into());
            }
            if Instant::now() >= deadline {
                return Err("gateway binary did not listen before deadline".into());
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn terminate(&mut self) -> Result<(), String> {
        #[cfg(unix)]
        {
            let status = Command::new("/bin/kill")
                .args([
                    "-TERM",
                    &self
                        .child
                        .as_ref()
                        .ok_or_else(|| "gateway binary is already reaped".to_owned())?
                        .id()
                        .to_string(),
                ])
                .status()
                .map_err(|_| "send SIGTERM to gateway binary".to_owned())?;
            if !status.success() {
                return Err("gateway binary rejected SIGTERM".into());
            }
        }
        #[cfg(not(unix))]
        self.child
            .as_mut()
            .ok_or_else(|| "gateway binary is already reaped".to_owned())?
            .kill()
            .map_err(|_| "stop gateway binary".to_owned())?;

        let deadline = Instant::now() + GATEWAY_PROCESS_STOP_TIMEOUT;
        let status = self.wait_for_exit(deadline)?;
        self.child.take();
        status
            .success()
            .then_some(())
            .ok_or_else(|| "gateway binary did not shut down cleanly".into())
    }

    fn wait_for_exit(&mut self, deadline: Instant) -> Result<std::process::ExitStatus, String> {
        loop {
            if let Some(status) = self
                .child
                .as_mut()
                .ok_or_else(|| "gateway binary is already reaped".to_owned())?
                .try_wait()
                .map_err(|_| "inspect gateway binary shutdown".to_owned())?
            {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                self.cleanup();
                return Err("gateway binary ignored bounded shutdown".into());
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn cleanup(&mut self) {
        if let Some(mut child) = self.child.take()
            && child.try_wait().ok().flatten().is_none()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for GatewayProcess {
    fn drop(&mut self) {
        self.cleanup();
    }
}

fn unused_loopback_address() -> SocketAddr {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
    let address = listener.local_addr().expect("reserved loopback address");
    drop(listener);
    address
}

fn assert_binary_rejects_before_listening(config: GatewayProcessConfig) {
    match GatewayProcess::start(&config) {
        Err(error) => assert_eq!(error, "gateway binary rejected startup configuration"),
        Ok(_) => panic!("invalid gateway configuration must not listen"),
    }
}

#[test]
#[allow(clippy::too_many_lines)] // One real process witnesses bootstrap, MCP operations and SIGTERM shutdown.
fn gateway_binary_real_bootstrap_mcp_interaction_and_sigterm_acceptance() {
    run_db_fixture::<Fixture, _>(
        "gateway_binary_real_bootstrap_mcp_interaction_and_sigterm_acceptance",
        |mut handle| {
            handle.assert_gateway_login();
            let prefix = format!("proc{}", &Uuid::now_v7().simple().to_string()[..12]);
            let wire = format!("{prefix}.{}", "c".repeat(32));
            let credential = handle.seed_synthetic_service_credential_and_window(
                SyntheticCredentialScopes::RememberWriteAndContextRead,
                &prefix,
                &wire,
                &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
                4,
            );
            let context_row = handle.seed_workspace_visible_context_record();
            let mut config = GatewayProcessConfig::for_fixture(&handle, unused_loopback_address());
            config
                .values
                .insert("HUMAUX_GATEWAY_RETRIEVAL_PROFILE_TOP_K".into(), "7".into());

            let mut missing = config.clone();
            missing.values.remove("HUMAUX_GATEWAY_PG_DSN");
            assert_binary_rejects_before_listening(missing);
            let mut unknown = config.clone();
            unknown
                .values
                .insert("HUMAUX_GATEWAY_UNKNOWN".into(), "rejected".into());
            assert_binary_rejects_before_listening(unknown);

            let mut process = GatewayProcess::start(&config).expect("gateway binary starts");
            let runtime = handle.rt.handle().clone();
            runtime.block_on(async {
                let context_headers = tool_call_headers("context", &credential.bearer);
                let remember_headers = tool_call_headers("remember", &credential.bearer);
                let context = call_params("context", json!({"workspace_id": handle.workspace_id}));
                let (status, response) = raw_request(
                    process.address,
                    &context_headers,
                    &rpc(1, "tools/call", context),
                )
                .await;
                assert_eq!(status, 200, "binary context response: {response}");
                assert_context_response(
                    &response,
                    context_row.memory_id,
                    7,
                    std::path::Path::new(env!("CARGO_BIN_EXE_humaux-gateway")),
                );

                let (status, response) = raw_request(
                    process.address,
                    &tool_call_headers("memory", &credential.bearer),
                    &rpc(
                        7,
                        "tools/call",
                        call_params(
                            "memory",
                            json!({"action":"get","memory_id":context_row.memory_id}),
                        ),
                    ),
                )
                .await;
                assert_eq!(status, 200, "binary memory.get response: {response}");
                assert_memory_response(
                    &response,
                    context_row.memory_id,
                    7,
                    std::path::Path::new(env!("CARGO_BIN_EXE_humaux-gateway")),
                );

                let args = json!({
                    "operation": "put",
                    "content": "real binary MCP receipt",
                    "idempotency_key": format!("binary-replay-{}", Uuid::now_v7()),
                    "workspace_id": handle.workspace_id,
                });
                let (status, response) = raw_request(
                    process.address,
                    &remember_headers,
                    &rpc(2, "tools/call", call_params("remember", args.clone())),
                )
                .await;
                assert_eq!(status, 200, "binary remember response: {response}");
                let evidence_id = response["result"]["structuredContent"]["evidence_id"].clone();
                assert!(
                    evidence_id.is_string(),
                    "binary remember evidence: {response}"
                );
                assert_eq!(response["result"]["structuredContent"]["replayed"], false);
                let after_first = blocking_counts(&mut handle);

                let (status, replay) = raw_request(
                    process.address,
                    &remember_headers,
                    &rpc(3, "tools/call", call_params("remember", args)),
                )
                .await;
                assert_eq!(status, 200, "binary replay response: {replay}");
                assert_eq!(
                    replay["result"]["structuredContent"]["evidence_id"],
                    evidence_id
                );
                assert_eq!(replay["result"]["structuredContent"]["replayed"], true);
                assert_eq!(
                    durable_counts(blocking_counts(&mut handle)),
                    durable_counts(after_first)
                );
            });
            process.terminate().expect("binary exits after SIGTERM");
        },
    );
}

fn enumeration_credential(handle: &mut Handle, suffix: &str) -> SyntheticServiceCredential {
    let prefix = format!("enum{suffix}{}", Uuid::new_v4().simple());
    let wire = format!("{prefix}.{}", "f".repeat(32));
    handle.seed_synthetic_service_credential(
        SyntheticCredentialScopes::ContextRead,
        &prefix,
        &wire,
        &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
    )
}

async fn assert_enumeration_cursor_denials(
    address: SocketAddr,
    credential: &SyntheticServiceCredential,
    peer: &SyntheticServiceCredential,
    other_workspace: Uuid,
    cursor: &str,
) {
    use humaux_domain::selection::{Cursor, cursor_mac_key};
    let before = final_completeness_count();
    let (status, denied) = enumerate_call(
        address,
        &peer.bearer,
        json!({"action":"enumerate","cursor":cursor,"limit":2}),
    )
    .await;
    assert_eq!(
        status, 200,
        "another principal cannot reuse a snapshot: {denied}"
    );
    assert_memory_not_found(&denied);
    let (status, denied) = enumerate_call(
        address,
        &credential.bearer,
        json!({"action":"enumerate","cursor":cursor,"workspace_id":other_workspace}),
    )
    .await;
    assert_eq!(status, 403, "credential-bound workspace check: {denied}");
    let mut forged = Cursor::decode(cursor).expect("real issued cursor");
    forged.last_ordinal += 1;
    for arguments in [
        json!({"action":"enumerate","cursor":forged.encode()}),
        json!({"action":"enumerate","cursor":"not-a-cursor"}),
        json!({"action":"enumerate","limit":0}),
        json!({"action":"enumerate","limit":101}),
        json!({"action":"enumerate","ttl":999999}),
    ] {
        let (status, denied) = enumerate_call(address, &credential.bearer, arguments).await;
        assert_eq!(status, 400, "invalid enumeration request: {denied}");
        assert_eq!(denied["error"]["data"]["code"], "INVALID_INPUT");
    }
    let issued = Cursor::decode(cursor).expect("issued cursor");
    let expired = Cursor::sign(
        issued.snapshot_id,
        issued.tenant_id,
        issued.query_fingerprint,
        issued.last_ordinal,
        time::OffsetDateTime::now_utc().unix_timestamp() - 1,
        &cursor_mac_key(SYNTHETIC_CREDENTIAL_PEPPER),
    );
    let (status, denied) = enumerate_call(
        address,
        &credential.bearer,
        json!({"action":"enumerate","cursor":expired.encode()}),
    )
    .await;
    assert_eq!(status, 200, "valid but expired snapshot: {denied}");
    assert_memory_not_found(&denied);
    assert_eq!(
        final_completeness_count(),
        before,
        "denials publish no final outcome metric"
    );
}

#[test]
fn native_mcp_memory_enumeration_freezes_snapshot_and_binds_cursor() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>(
        "native_mcp_memory_enumeration_freezes_snapshot_and_binds_cursor",
        |mut handle| {
            let credential = enumeration_credential(&mut handle, "owner");
            let peer = enumeration_credential(&mut handle, "peer");
            handle.seed_current_entitlement_and_window(64);
            let other_workspace = handle.seed_workspace();
            let mut expected: Vec<Uuid> = (0..5)
                .map(|_| handle.seed_workspace_visible_context_record().memory_id)
                .collect();
            expected.sort_unstable_by(|a, b| b.cmp(a));
            let runtime_handle = handle.rt.handle().clone();
            let runtime = runtime_handle
                .block_on(handle.fresh_runtime())
                .expect("checked enumeration runtime");
            let app = application(&handle, runtime);
            runtime_handle.block_on(async {
                let (address, server) = start(app).await;
                let (status, first) = enumerate_call(
                    address,
                    &credential.bearer,
                    json!({"action":"enumerate","limit":1}),
                )
                .await;
                assert_eq!(status, 200, "first snapshot page: {first}");
                let first_cursor =
                    assert_enumeration_response(&first, &expected[..1]).expect("next page");
                let snapshot =
                    first["result"]["structuredContent"]["pagination"]["snapshot_id"].clone();
                let inserted: Vec<Uuid> = tokio::task::block_in_place(|| {
                    (0..20)
                        .map(|_| handle.seed_workspace_visible_context_record().memory_id)
                        .collect()
                });
                assert!(
                    inserted.iter().all(|id| *id > expected[0]),
                    "G20-1 inserts must sort ahead of frozen IDs"
                );
                assert_enumeration_cursor_denials(
                    address,
                    &credential,
                    &peer,
                    other_workspace,
                    &first_cursor,
                )
                .await;
                let mut cursor = Some(first_cursor);
                let mut offset = 1;
                for _ in 0..4 {
                    let Some(next) = cursor.take() else {
                        break;
                    };
                    let (status, page) = enumerate_call(
                        address,
                        &credential.bearer,
                        json!({"action":"enumerate","limit":2,"cursor":next}),
                    )
                    .await;
                    assert_eq!(status, 200, "frozen continuation: {page}");
                    let end = (offset + 2).min(expected.len());
                    cursor = assert_enumeration_response(&page, &expected[offset..end]);
                    assert_eq!(
                        page["result"]["structuredContent"]["pagination"]["snapshot_id"],
                        snapshot
                    );
                    offset = end;
                }
                assert!(cursor.is_none(), "finite snapshot must terminate");
                assert_eq!(
                    offset,
                    expected.len(),
                    "every original ID returned exactly once"
                );
                expected.extend(inserted);
                expected.sort_unstable_by(|a, b| b.cmp(a));
                let (status, fresh) = enumerate_call(
                    address,
                    &credential.bearer,
                    json!({"action":"enumerate","limit":100}),
                )
                .await;
                assert_eq!(
                    status, 200,
                    "fresh snapshot sees concurrent inserts: {fresh}"
                );
                assert!(assert_enumeration_response(&fresh, &expected).is_none());
                assert_ne!(
                    fresh["result"]["structuredContent"]["pagination"]["snapshot_id"],
                    snapshot
                );
                stop_server(server).await.expect("stop pagination server");
            });
        },
    );
}

#[test]
fn native_mcp_memory_enumeration_fails_whole_revoked_page_and_restarts() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>(
        "native_mcp_memory_enumeration_fails_whole_revoked_page_and_restarts",
        |mut handle| {
            let credential = enumeration_credential(&mut handle, "revoke");
            handle.seed_current_entitlement_and_window(64);
            let hidden_workspace = handle.seed_workspace();
            let hidden_source = handle.seed_workspace_visible_context_record();
            set_record_workspace_visibility(&mut handle, &hidden_source, hidden_workspace);
            let records: Vec<_> = (0..3)
                .map(|_| handle.seed_workspace_visible_context_record())
                .collect();
            let mut expected: Vec<Uuid> = records.iter().map(|r| r.memory_id).collect();
            expected.sort_unstable_by(|a, b| b.cmp(a));
            let target = records
                .iter()
                .find(|r| r.memory_id == expected[1])
                .expect("second page target");
            let fresh_expected = [expected[0], expected[2]];
            let runtime_handle = handle.rt.handle().clone();
            let runtime = runtime_handle
                .block_on(handle.fresh_runtime())
                .expect("checked revocation runtime");
            let app = application(&handle, runtime);
            runtime_handle.block_on(async {
            let (address, server) = start(app).await;
            for kind in 0..3 {
                let (status, first) = enumerate_call(address, &credential.bearer,
                    json!({"action":"enumerate","limit":1})).await;
                assert_eq!(status, 200, "authorized initial page: {first}");
                let cursor = assert_enumeration_response(&first, &expected[..1]).expect("continuation");
                tokio::task::block_in_place(|| match kind {
                    0 => handle.admin.execute("UPDATE private.memory_records SET status='revoked' WHERE memory_id=$1", &[&target.memory_id]),
                    1 => handle.admin.execute("UPDATE private.evidence_objects SET data_class='SECRET_MATERIAL' WHERE evidence_id=$1", &[&target.evidence_id]),
                    _ => handle.admin.execute("INSERT INTO private.memory_evidence(memory_id,evidence_id,role,grounding_mode) VALUES($1,$2,'SUPPORTING','SNAPSHOT')", &[&target.memory_id,&hidden_source.evidence_id]),
                }).expect("tighten current-page authorization or lifecycle");
                let before = final_completeness_count();
                let (status, denied) = enumerate_call(address, &credential.bearer,
                    json!({"action":"enumerate","limit":2,"cursor":cursor})).await;
                assert_eq!(status, 200, "revoked page is a uniform business failure: {denied}");
                assert_memory_not_found(&denied);
                assert!(denied["result"]["structuredContent"].get("pagination").is_none());
                assert_eq!(final_completeness_count(), before, "rejected page publishes no final metric");
                let (status, fresh) = enumerate_call(address, &credential.bearer,
                    json!({"action":"enumerate","limit":100})).await;
                assert_eq!(status, 200, "fresh snapshot recovers without invisible item: {fresh}");
                assert!(assert_enumeration_response(&fresh, &fresh_expected).is_none());
                tokio::task::block_in_place(|| match kind {
                    0 => handle.admin.execute("UPDATE private.memory_records SET status='active' WHERE memory_id=$1", &[&target.memory_id]),
                    1 => handle.admin.execute("UPDATE private.evidence_objects SET data_class='INTERNAL' WHERE evidence_id=$1", &[&target.evidence_id]),
                    _ => handle.admin.execute("DELETE FROM private.memory_evidence WHERE memory_id=$1 AND evidence_id=$2", &[&target.memory_id,&hidden_source.evidence_id]),
                }).expect("restore isolated fixture for next authorization case");
            }
            tokio::task::block_in_place(|| handle.admin.execute(
                "UPDATE private.memory_records SET status='revoked' WHERE tenant_id=$1", &[&handle.tenant_id],
            )).expect("empty current authorized universe");
            let (status, empty) = enumerate_call(address, &credential.bearer, json!({"action":"enumerate"})).await;
            assert_eq!(status, 200, "empty authorized snapshot is valid: {empty}");
            assert!(assert_enumeration_response(&empty, &[]).is_none());
            stop_server(server).await.expect("stop revoke server");
        });
        },
    );
}

// ---------------------------------------------------------------------------------------
// §33.10 rule 9 confirm gate + §36 memory.supersede (ADR-0018)
// ---------------------------------------------------------------------------------------

fn assert_tool_error(response: &Value, code: &str) {
    let result = &response["result"];
    assert_eq!(result["isError"], true, "expected {code}: {response}");
    assert_eq!(
        result["structuredContent"]["code"], code,
        "tool error code: {response}"
    );
}

async fn supersede_call(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    target: Uuid,
    successor: Uuid,
    token: Option<&str>,
) -> (u16, Value) {
    let mut arguments = json!({
        "action": "supersede",
        "memory_id": target,
        "replacement_memory_id": successor,
    });
    if let Some(token) = token {
        arguments["confirm_token"] = Value::String(token.to_owned());
    }
    raw_request(
        address,
        &tool_call_headers("memory", bearer),
        &rpc(request_id, "tools/call", call_params("memory", arguments)),
    )
    .await
}

/// First call: must be a success-shaped result carrying the token, never a mutation.
async fn mint_supersede_token(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    target: Uuid,
    successor: Uuid,
) -> String {
    let (status, response) =
        supersede_call(address, bearer, request_id, target, successor, None).await;
    assert_eq!(status, 200, "first call is an MCP result: {response}");
    let result = &response["result"];
    assert_ne!(
        result["isError"], true,
        "confirmation is not an error: {response}"
    );
    let structured = &result["structuredContent"];
    assert_eq!(structured["confirmation_required"], true, "{response}");
    assert_eq!(structured["operation"], "memory.supersede", "{response}");
    assert_eq!(
        structured["target"]["memory_id"],
        target.to_string(),
        "{response}"
    );
    assert_eq!(
        structured["target"]["replacement_memory_id"],
        successor.to_string(),
        "{response}"
    );
    assert!(structured["expires_at"].is_string(), "{response}");
    let text: Value =
        serde_json::from_str(result["content"][0]["text"].as_str().expect("text mirror"))
            .expect("JSON text mirror");
    assert_eq!(&text, structured, "structured and text results must agree");
    let token = structured["confirm_token"]
        .as_str()
        .expect("confirm_token string")
        .to_owned();
    assert_eq!(token.len(), 43, "base64url of 32 bytes: {token}");
    token
}

/// (status, superseded_by, superseded_at IS NOT NULL, G59-4 holds) for one row.
fn memory_state(handle: &mut Handle, memory_id: Uuid) -> (String, Option<Uuid>, bool, bool) {
    let row = handle
        .admin
        .query_one(
            "SELECT status, superseded_by, superseded_at IS NOT NULL, \
                    (status='superseded') = (superseded_by IS NOT NULL) \
             FROM private.memory_records WHERE memory_id=$1",
            &[&memory_id],
        )
        .expect("owner reads memory lifecycle state");
    (row.get(0), row.get(1), row.get(2), row.get(3))
}

fn token_consumed(handle: &mut Handle, wire: &str) -> Option<bool> {
    let digest = humaux_domain::confirm::ConfirmToken::decode(wire)
        .expect("wire token decodes")
        .sha256()
        .to_vec();
    handle
        .admin
        .query_opt(
            "SELECT consumed_at IS NOT NULL FROM control.confirm_tokens WHERE nonce_sha256=$1",
            &[&digest],
        )
        .expect("owner reads token row")
        .map(|row| row.get(0))
}

fn lifecycle_ticket_count(handle: &mut Handle, evidence_id: Uuid) -> i64 {
    handle
        .admin
        .query_one(
            "SELECT count(*) FROM ops.outbox o \
             JOIN projection.stream_log s ON s.tenant_id=o.tenant_id AND s.commit_seq=o.commit_seq \
             WHERE o.tenant_id=$1 AND o.evidence_id=$2 AND o.event_type='MEMORY_LIFECYCLE' \
               AND s.state='ISSUED' AND s.stream_seq=o.stream_seq",
            &[&handle.tenant_id, &evidence_id],
        )
        .expect("owner counts lifecycle tickets")
        .get(0)
}

fn all_rows_satisfy_g59_4(handle: &mut Handle) -> bool {
    handle
        .admin
        .query_one(
            "SELECT bool_and((status='superseded') = (superseded_by IS NOT NULL)) \
             FROM private.memory_records WHERE tenant_id=$1",
            &[&handle.tenant_id],
        )
        .expect("owner checks G59-4")
        .get(0)
}

#[test]
#[allow(clippy::too_many_lines)] // One real HTTP fixture carries the whole (a)-(g) acceptance matrix.
fn native_mcp_memory_supersede_confirm_gate_acceptance() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>("native_mcp_memory_supersede_confirm_gate", |mut handle| {
        handle.assert_gateway_login();
        let prefix = format!("msup{}", &Uuid::now_v7().simple().to_string()[..12]);
        let wire = format!("{prefix}.{}", "e".repeat(32));
        let credential = handle.seed_synthetic_service_credential_and_window(
            SyntheticCredentialScopes::RememberWriteAndContextRead,
            &prefix,
            &wire,
            &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
            64,
        );
        let peer_user = handle.seed_peer_user();
        let peer_prefix = format!("mpee{}", &Uuid::now_v7().simple().to_string()[..12]);
        let peer_wire = format!("{peer_prefix}.{}", "d".repeat(32));
        let peer = handle.seed_synthetic_service_credential(
            SyntheticCredentialScopes::RememberWriteAndContextRead,
            &peer_prefix,
            &peer_wire,
            &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &peer_wire),
        );
        handle
            .admin
            .execute(
                "UPDATE control.api_keys SET user_id=$2 WHERE api_key_id=$1",
                &[&peer.api_key_id, &peer_user],
            )
            .expect("owner rebinds peer credential to the peer user");

        let a = handle.seed_workspace_visible_context_record();
        let b = handle.seed_workspace_visible_context_record();
        let c = handle.seed_workspace_visible_context_record();
        let d = handle.seed_workspace_visible_context_record();
        let e = handle.seed_workspace_visible_context_record();
        let fault_fn = format!(
            "public.humaux_test_supersede_fault_{}",
            Uuid::now_v7().simple()
        );

        let runtime_handle = handle.rt.handle().clone();
        let runtime = runtime_handle
            .block_on(handle.fresh_runtime())
            .expect("checked supersede runtime");
        let app = application(&handle, runtime);
        runtime_handle.block_on(async {
            let (address, server) = start(app).await;
            let bearer = credential.bearer.as_str();
            let before = blocking_counts(&mut handle);

            // (a) first call: confirmation_required + token, no mutation.
            let token_ab = mint_supersede_token(address, bearer, 1, a.memory_id, b.memory_id).await;
            tokio::task::block_in_place(|| {
                assert_eq!(memory_state(&mut handle, a.memory_id), ("active".into(), None, false, true));
                assert_eq!(token_consumed(&mut handle, &token_ab), Some(false));
                let after = counts(&mut handle);
                assert_eq!(durable_counts(after), durable_counts(before), "no durable write besides the token row");
                assert_eq!(lifecycle_ticket_count(&mut handle, a.evidence_id), 0);
            });

            // Fault injection: the UPDATE fails inside the gated transaction -> the token
            // consume must roll back with it (partial-write sentinel).
            tokio::task::block_in_place(|| {
                handle
                    .admin
                    .batch_execute(&format!(
                        "CREATE FUNCTION {fault_fn}() RETURNS trigger LANGUAGE plpgsql AS $$ \
                         BEGIN RAISE EXCEPTION 'injected supersede fault' USING ERRCODE='23514'; END $$; \
                         CREATE TRIGGER humaux_test_supersede_fault BEFORE UPDATE OF status \
                         ON private.memory_records FOR EACH ROW WHEN (NEW.status = 'superseded') \
                         EXECUTE FUNCTION {fault_fn}();"
                    ))
                    .expect("owner installs supersede fault");
            });
            let (status, faulted) =
                supersede_call(address, bearer, 2, a.memory_id, b.memory_id, Some(&token_ab)).await;
            tokio::task::block_in_place(|| {
                handle
                    .admin
                    .batch_execute(&format!(
                        "DROP TRIGGER humaux_test_supersede_fault ON private.memory_records; \
                         DROP FUNCTION {fault_fn}();"
                    ))
                    .expect("owner removes supersede fault");
            });
            assert_eq!(status, 200, "faulted write is a tool error: {faulted}");
            assert_tool_error(&faulted, "CONFLICT");
            tokio::task::block_in_place(|| {
                assert_eq!(token_consumed(&mut handle, &token_ab), Some(false), "token consume rolled back with the failed UPDATE");
                assert_eq!(memory_state(&mut handle, a.memory_id), ("active".into(), None, false, true));
                assert_eq!(lifecycle_ticket_count(&mut handle, a.evidence_id), 0);
                assert_eq!(durable_counts(counts(&mut handle)), durable_counts(before), "reservation/outbox rolled back");
            });

            // (b) same token now executes: one commit flips status/superseded_by/superseded_at.
            let (status, done) =
                supersede_call(address, bearer, 3, a.memory_id, b.memory_id, Some(&token_ab)).await;
            assert_eq!(status, 200, "confirmed supersede: {done}");
            let result = &done["result"];
            assert_ne!(result["isError"], true, "{done}");
            let structured = &result["structuredContent"];
            assert_eq!(structured["memory_id"], a.memory_id.to_string(), "{done}");
            assert_eq!(structured["replacement_memory_id"], b.memory_id.to_string(), "{done}");
            assert!(structured["superseded_at"].is_string(), "{done}");
            let stream_seq = structured["stream_seq"].as_i64().expect("stream_seq");
            assert!(stream_seq >= 1, "{done}");
            tokio::task::block_in_place(|| {
                assert_eq!(
                    memory_state(&mut handle, a.memory_id),
                    ("superseded".into(), Some(b.memory_id), true, true)
                );
                assert_eq!(memory_state(&mut handle, b.memory_id), ("active".into(), None, false, true));
                assert_eq!(token_consumed(&mut handle, &token_ab), Some(true));
                assert_eq!(lifecycle_ticket_count(&mut handle, a.evidence_id), 1, "MEMORY_LIFECYCLE ticket issued once");
                // §77: the mint and the executed write share (action, resource_id, result);
                // only the CONFIRMATION_MINTED risk tag tells an auditor which one mutated.
                let (minted, executed): (i64, i64) = {
                    let row = handle
                        .admin
                        .query_one(
                            "SELECT count(*) FILTER (WHERE $2 = ANY(risk_tags)), \
                                    count(*) FILTER (WHERE NOT ($2 = ANY(risk_tags))) \
                             FROM control.audit_events WHERE tenant_id=$1 \
                               AND resource_id='memory.supersede' AND action='MCP_REQUEST_FINISHED' AND result='OK'",
                            &[&handle.tenant_id, &humaux_domain::confirm::RISK_TAG_CONFIRMATION_MINTED],
                        )
                        .expect("owner counts finished audits");
                    (row.get(0), row.get(1))
                };
                assert_eq!((minted, executed), (1, 1), "one tagged mint audit, one untagged executed-write audit");
                let consumed: i64 = handle
                    .admin
                    .query_one(
                        "SELECT count(*) FROM control.usage_reservations WHERE tenant_id=$1 \
                         AND operation='memory.supersede' AND status='CONSUMED'",
                        &[&handle.tenant_id],
                    )
                    .expect("owner counts consumed reservations")
                    .get(0);
                assert_eq!(consumed, 1, "exactly one BMO consumed, in the same commit");
            });

            // (c) replaying the consumed token is rejected.
            let (status, replay) =
                supersede_call(address, bearer, 4, a.memory_id, b.memory_id, Some(&token_ab)).await;
            assert_eq!(status, 200, "{replay}");
            assert_tool_error(&replay, "CONFLICT");

            // (f) superseding an already-superseded row: a fresh token is minted (no row read
            // on the first call), the confirmed call is CONFLICT and changes nothing.
            let token_again = mint_supersede_token(address, bearer, 5, a.memory_id, c.memory_id).await;
            let (status, again) =
                supersede_call(address, bearer, 6, a.memory_id, c.memory_id, Some(&token_again)).await;
            assert_eq!(status, 200, "{again}");
            assert_tool_error(&again, "CONFLICT");
            tokio::task::block_in_place(|| {
                assert_eq!(
                    memory_state(&mut handle, a.memory_id),
                    ("superseded".into(), Some(b.memory_id), true, true)
                );
                assert_eq!(token_consumed(&mut handle, &token_again), Some(false), "pair-rule rejection rolls the consume back");
            });

            // (d) an expired token is rejected.
            let token_expired = mint_supersede_token(address, bearer, 7, c.memory_id, b.memory_id).await;
            tokio::task::block_in_place(|| {
                let digest = humaux_domain::confirm::ConfirmToken::decode(&token_expired)
                    .expect("wire token")
                    .sha256()
                    .to_vec();
                handle
                    .admin
                    .batch_execute("ALTER TABLE control.confirm_tokens DISABLE TRIGGER confirm_token_single_use")
                    .expect("owner pauses single-use guard for expiry seeding");
                let expired = handle
                    .admin
                    .execute(
                        "UPDATE control.confirm_tokens SET expires_at = issued_at + interval '1 microsecond' \
                         WHERE nonce_sha256=$1",
                        &[&digest],
                    )
                    .expect("owner expires token");
                handle
                    .admin
                    .batch_execute("ALTER TABLE control.confirm_tokens ENABLE TRIGGER confirm_token_single_use")
                    .expect("owner restores single-use guard");
                assert_eq!(expired, 1);
            });
            let (status, expired) =
                supersede_call(address, bearer, 8, c.memory_id, b.memory_id, Some(&token_expired)).await;
            assert_eq!(status, 200, "{expired}");
            assert_tool_error(&expired, "CONFLICT");
            tokio::task::block_in_place(|| {
                assert_eq!(memory_state(&mut handle, c.memory_id), ("active".into(), None, false, true));
                assert_eq!(token_consumed(&mut handle, &token_expired), Some(false));
            });

            // (e) a token minted for (C -> B) is rejected against D, against a different
            // successor E (the confirmed action is the pair, not the target alone), against
            // another operation's binding, and under another user's credential; it still
            // works for (C -> B) afterwards.
            let token_c = mint_supersede_token(address, bearer, 9, c.memory_id, b.memory_id).await;
            let (status, wrong_target) =
                supersede_call(address, bearer, 10, d.memory_id, b.memory_id, Some(&token_c)).await;
            assert_eq!(status, 200, "{wrong_target}");
            assert_tool_error(&wrong_target, "CONFLICT");
            let (status, wrong_successor) =
                supersede_call(address, bearer, 17, c.memory_id, e.memory_id, Some(&token_c)).await;
            assert_eq!(status, 200, "{wrong_successor}");
            assert_tool_error(&wrong_successor, "CONFLICT");
            let (status, wrong_user) =
                supersede_call(address, &peer.bearer, 11, c.memory_id, b.memory_id, Some(&token_c)).await;
            assert_eq!(status, 200, "{wrong_user}");
            assert_tool_error(&wrong_user, "CONFLICT");
            tokio::task::block_in_place(|| {
                let digest = humaux_domain::confirm::ConfirmToken::decode(&token_c)
                    .expect("wire token")
                    .sha256()
                    .to_vec();
                let mut gateway = handle.gateway_client().expect("actual gateway login");
                let mut txn = gateway.transaction().expect("gateway txn");
                txn.execute(
                    "SELECT set_config('humaux.tenant_id',$1,true), set_config('humaux.user_id',$2,true)",
                    &[&handle.tenant_id.to_string(), &handle.user_id.to_string()],
                )
                .expect("gateway RLS context");
                let other_operation = txn
                    .execute(
                        "UPDATE control.confirm_tokens SET consumed_at = clock_timestamp() \
                         WHERE nonce_sha256=$1 AND tenant_id=$2 AND user_id=$3 \
                           AND operation='memory.archive' AND target_id=$4 AND successor_id=$5 \
                           AND consumed_at IS NULL AND expires_at > clock_timestamp()",
                        &[&digest, &handle.tenant_id, &handle.user_id, &c.memory_id, &b.memory_id],
                    )
                    .expect("gateway probes the operation binding");
                txn.rollback().expect("probe rollback");
                assert_eq!(other_operation, 0, "token is bound to memory.supersede, not another operation");
                assert_eq!(memory_state(&mut handle, c.memory_id), ("active".into(), None, false, true));
                assert_eq!(memory_state(&mut handle, d.memory_id), ("active".into(), None, false, true));
                assert_eq!(memory_state(&mut handle, e.memory_id), ("active".into(), None, false, true));
                assert_eq!(token_consumed(&mut handle, &token_c), Some(false));
            });
            let (status, c_done) =
                supersede_call(address, bearer, 12, c.memory_id, b.memory_id, Some(&token_c)).await;
            assert_eq!(status, 200, "{c_done}");
            assert_ne!(c_done["result"]["isError"], true, "{c_done}");
            tokio::task::block_in_place(|| {
                assert_eq!(
                    memory_state(&mut handle, c.memory_id),
                    ("superseded".into(), Some(b.memory_id), true, true)
                );
            });

            // (g) concurrent double-supersede of D with two live tokens: exactly one wins,
            // the loser's consume rolls back, G59-4 holds on every row throughout. The two
            // gated transactions are made to overlap in PostgreSQL by holding the tenant's
            // quota window row from an owner session until both writers are queued on it —
            // the same-IP preflight limiter (`pg_try_advisory_xact_lock`, fail-closed 429) would
            // otherwise reject a byte-simultaneous second HTTP request before it reaches the DB.
            let token_d1 = mint_supersede_token(address, bearer, 13, d.memory_id, e.memory_id).await;
            let token_d2 = mint_supersede_token(address, bearer, 14, d.memory_id, e.memory_id).await;
            let mut window_lock = tokio::task::block_in_place(|| {
                let mut client = handle.owner_client().expect("owner window-lock client");
                client
                    .batch_execute("BEGIN")
                    .expect("owner window-lock txn");
                let locked = client
                    .execute(
                        "SELECT 1 FROM control.quota_windows WHERE tenant_id=$1 FOR UPDATE",
                        &[&handle.tenant_id],
                    )
                    .expect("owner holds the quota window row");
                assert!(locked >= 1, "fixture tenant has a quota window to hold");
                client
            });
            let left = tokio::spawn({
                let bearer = bearer.to_owned();
                let token = token_d1.clone();
                let (target, successor) = (d.memory_id, e.memory_id);
                async move { supersede_call(address, &bearer, 15, target, successor, Some(&token)).await }
            });
            tokio::time::sleep(Duration::from_millis(400)).await;
            let right = tokio::spawn({
                let bearer = bearer.to_owned();
                let token = token_d2.clone();
                let (target, successor) = (d.memory_id, e.memory_id);
                async move { supersede_call(address, &bearer, 16, target, successor, Some(&token)).await }
            });
            tokio::time::sleep(Duration::from_millis(400)).await;
            tokio::task::block_in_place(|| {
                let queued: i64 = window_lock
                    .query_one(
                        "SELECT count(*) FROM pg_stat_activity WHERE usename='role_gateway' \
                         AND wait_event_type='Lock' AND state='active'",
                        &[],
                    )
                    .expect("owner observes queued gateway writers")
                    .get(0);
                assert_eq!(
                    queued, 2,
                    "both gated transactions are open and queued on the DB"
                );
                window_lock
                    .batch_execute("COMMIT")
                    .expect("release quota window row");
                // The blocking client is dropped off the async worker (postgres 0.19 drives
                // its own runtime on drop).
                drop(window_lock);
            });
            let (left, right) = tokio::join!(left, right);
            let outcomes = [left.expect("left writer"), right.expect("right writer")];
            let winners = outcomes
                .iter()
                .filter(|(status, response)| *status == 200 && response["result"]["isError"] != true)
                .count();
            assert_eq!(winners, 1, "exactly one concurrent supersede wins: {outcomes:?}");
            for (status, response) in &outcomes {
                assert_eq!(*status, 200, "{response}");
                if response["result"]["isError"] == true {
                    assert_tool_error(response, "CONFLICT");
                }
            }
            tokio::task::block_in_place(|| {
                assert_eq!(
                    memory_state(&mut handle, d.memory_id),
                    ("superseded".into(), Some(e.memory_id), true, true)
                );
                assert_eq!(lifecycle_ticket_count(&mut handle, d.evidence_id), 1, "one ticket for one supersede");
                let consumed = [&token_d1, &token_d2]
                    .into_iter()
                    .filter(|token| token_consumed(&mut handle, token) == Some(true))
                    .count();
                assert_eq!(consumed, 1, "the loser's token consume rolled back with its transaction");
                assert!(all_rows_satisfy_g59_4(&mut handle), "G59-4 never violated");
            });

            stop_server(server).await.expect("stop supersede server");
        });
    });
}

// ---------------------------------------------------------------------------------------
// §36 memory.restore behind the same confirm gate (ADR-0020)
// ---------------------------------------------------------------------------------------

async fn restore_call(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    target: Uuid,
    token: Option<&str>,
) -> (u16, Value) {
    let mut arguments = json!({ "action": "restore", "memory_id": target });
    if let Some(token) = token {
        arguments["confirm_token"] = Value::String(token.to_owned());
    }
    raw_request(
        address,
        &tool_call_headers("memory", bearer),
        &rpc(request_id, "tools/call", call_params("memory", arguments)),
    )
    .await
}

/// First call: a success-shaped `confirmation_required` naming memory.restore, no successor.
async fn mint_restore_token(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    target: Uuid,
) -> String {
    let (status, response) = restore_call(address, bearer, request_id, target, None).await;
    assert_eq!(status, 200, "first call is an MCP result: {response}");
    let result = &response["result"];
    assert_ne!(
        result["isError"], true,
        "confirmation is not an error: {response}"
    );
    let structured = &result["structuredContent"];
    assert_eq!(structured["confirmation_required"], true, "{response}");
    assert_eq!(structured["operation"], "memory.restore", "{response}");
    assert_eq!(
        structured["target"]["memory_id"],
        target.to_string(),
        "{response}"
    );
    assert!(
        structured["target"].get("replacement_memory_id").is_none(),
        "restore confirmation names no successor: {response}"
    );
    structured["confirm_token"]
        .as_str()
        .expect("confirm_token string")
        .to_owned()
}

fn assert_restore_conflict(response: &Value, reason: u64, label: &str) {
    let structured = &response["result"]["structuredContent"];
    assert_ne!(
        response["result"]["isError"], true,
        "a refused restore is a success-shaped CONFLICT-with-reason: {response}"
    );
    assert_eq!(structured["code"], "CONFLICT", "{response}");
    assert_eq!(structured["reason"], reason, "{response}");
    assert_eq!(structured["reason_label"], label, "{response}");
}

/// (op, undoes_event_id) of the memory's current head lifecycle event, if any.
fn lifecycle_head(handle: &mut Handle, memory_id: Uuid) -> Option<(String, Option<Uuid>)> {
    handle
        .admin
        .query_opt(
            "SELECT e.op, e.undoes_event_id FROM private.memory_records m \
             JOIN ops.memory_lifecycle_events e ON e.event_id = m.lifecycle_head_event_id \
             WHERE m.memory_id = $1",
            &[&memory_id],
        )
        .expect("owner reads lifecycle head")
        .map(|row| (row.get(0), row.get(1)))
}

fn lifecycle_op_count(handle: &mut Handle, memory_id: Uuid, op: &str) -> i64 {
    handle
        .admin
        .query_one(
            "SELECT count(*) FROM ops.memory_lifecycle_events WHERE memory_id = $1 AND op = $2",
            &[&memory_id, &op],
        )
        .expect("owner counts lifecycle events")
        .get(0)
}

fn latest_supersede_event_id(handle: &mut Handle, memory_id: Uuid) -> Uuid {
    handle
        .admin
        .query_one(
            "SELECT event_id FROM ops.memory_lifecycle_events \
             WHERE memory_id = $1 AND op = 'SUPERSEDE' ORDER BY event_seq DESC LIMIT 1",
            &[&memory_id],
        )
        .expect("owner reads SUPERSEDE event id")
        .get(0)
}

/// Drives supersede's confirm gate to completion, returning the executed stream_seq.
async fn drive_supersede(
    address: SocketAddr,
    bearer: &str,
    base_request_id: u64,
    target: Uuid,
    successor: Uuid,
) -> i64 {
    let token = mint_supersede_token(address, bearer, base_request_id, target, successor).await;
    let (status, done) = supersede_call(
        address,
        bearer,
        base_request_id + 1,
        target,
        successor,
        Some(&token),
    )
    .await;
    assert_eq!(status, 200, "confirmed supersede: {done}");
    assert_ne!(done["result"]["isError"], true, "{done}");
    done["result"]["structuredContent"]["stream_seq"]
        .as_i64()
        .expect("supersede stream_seq")
}

#[test]
#[allow(clippy::too_many_lines)] // One real HTTP fixture carries the whole (a)-(g) restore matrix.
fn native_mcp_memory_restore_confirm_gate_acceptance() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>("native_mcp_memory_restore_confirm_gate", |mut handle| {
        handle.assert_gateway_login();
        let prefix = format!("mres{}", &Uuid::now_v7().simple().to_string()[..12]);
        let wire = format!("{prefix}.{}", "f".repeat(32));
        let credential = handle.seed_synthetic_service_credential_and_window(
            SyntheticCredentialScopes::RememberWriteAndContextRead,
            &prefix,
            &wire,
            &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
            96,
        );

        let a = handle.seed_workspace_visible_context_record();
        let b = handle.seed_workspace_visible_context_record();
        let c = handle.seed_workspace_visible_context_record();
        let d = handle.seed_workspace_visible_context_record();
        let e = handle.seed_workspace_visible_context_record();
        let f = handle.seed_workspace_visible_context_record();
        let g = handle.seed_workspace_visible_context_record();
        let h = handle.seed_workspace_visible_context_record();
        let fault_fn = format!(
            "public.humaux_test_restore_fault_{}",
            Uuid::now_v7().simple()
        );

        let runtime_handle = handle.rt.handle().clone();
        let runtime = runtime_handle
            .block_on(handle.fresh_runtime())
            .expect("checked restore runtime");
        let app = application(&handle, runtime);
        runtime_handle.block_on(async {
            let (address, server) = start(app).await;
            let bearer = credential.bearer.as_str();

            // (a) supersede A->B, then restore A inside the window.
            let supersede_seq = drive_supersede(address, bearer, 1, a.memory_id, b.memory_id).await;
            let supersede_event = tokio::task::block_in_place(|| {
                assert_eq!(
                    memory_state(&mut handle, a.memory_id),
                    ("superseded".into(), Some(b.memory_id), true, true)
                );
                assert_eq!(lifecycle_op_count(&mut handle, a.memory_id, "SUPERSEDE"), 1,
                    "card-1 supersede now also appends a SUPERSEDE lifecycle event");
                latest_supersede_event_id(&mut handle, a.memory_id)
            });
            let token_a = mint_restore_token(address, bearer, 3, a.memory_id).await;
            let (status, done) = restore_call(address, bearer, 4, a.memory_id, Some(&token_a)).await;
            assert_eq!(status, 200, "confirmed restore: {done}");
            let structured = &done["result"]["structuredContent"];
            assert_ne!(done["result"]["isError"], true, "{done}");
            assert_eq!(structured["memory_id"], a.memory_id.to_string(), "{done}");
            assert!(structured["restored_at"].is_string(), "{done}");
            assert!(structured["consistency_token"].is_string(), "{done}");
            let restore_seq = structured["stream_seq"].as_i64().expect("restore stream_seq");
            assert!(
                restore_seq > supersede_seq,
                "restore issues a NEW higher stream seq ({restore_seq} > {supersede_seq}), never revives the old row"
            );
            tokio::task::block_in_place(|| {
                assert_eq!(
                    memory_state(&mut handle, a.memory_id),
                    ("active".into(), None, false, true),
                    "A is active again with superseded_by NULL and G59-4 satisfied"
                );
                assert_eq!(
                    lifecycle_head(&mut handle, a.memory_id),
                    Some(("RESTORE".to_owned(), Some(supersede_event))),
                    "head is a RESTORE naming the SUPERSEDE it undoes"
                );
                assert_eq!(token_consumed(&mut handle, &token_a), Some(true));
            });

            // (b) restore after the window -> CONFLICT reason 1001, mutates nothing.
            drive_supersede(address, bearer, 5, c.memory_id, b.memory_id).await;
            tokio::task::block_in_place(|| {
                handle
                    .admin
                    .execute(
                        "UPDATE ops.memory_lifecycle_events \
                         SET undo_deadline = clock_timestamp() - interval '1 second' \
                         WHERE memory_id = $1 AND op = 'SUPERSEDE'",
                        &[&c.memory_id],
                    )
                    .expect("owner expires the undo window");
            });
            let token_c = mint_restore_token(address, bearer, 7, c.memory_id).await;
            let (status, expired) =
                restore_call(address, bearer, 8, c.memory_id, Some(&token_c)).await;
            assert_eq!(status, 200, "{expired}");
            assert_restore_conflict(&expired, 1001, "UNDO_WINDOW_EXPIRED");
            tokio::task::block_in_place(|| {
                assert_eq!(
                    memory_state(&mut handle, c.memory_id),
                    ("superseded".into(), Some(b.memory_id), true, true)
                );
                assert_eq!(token_consumed(&mut handle, &token_c), Some(false),
                    "a refused restore rolls the token consume back");
                assert_eq!(lifecycle_op_count(&mut handle, c.memory_id, "RESTORE"), 0);
            });

            // (c) restore of a memory whose successor was itself superseded -> 1004.
            drive_supersede(address, bearer, 9, d.memory_id, e.memory_id).await;
            drive_supersede(address, bearer, 11, e.memory_id, f.memory_id).await;
            let token_d = mint_restore_token(address, bearer, 13, d.memory_id).await;
            let (status, advanced) =
                restore_call(address, bearer, 14, d.memory_id, Some(&token_d)).await;
            assert_eq!(status, 200, "{advanced}");
            assert_restore_conflict(&advanced, 1004, "TARGET_ADVANCED");
            tokio::task::block_in_place(|| {
                assert_eq!(
                    memory_state(&mut handle, d.memory_id),
                    ("superseded".into(), Some(e.memory_id), true, true)
                );
            });

            // (d) restore when the head is already a RESTORE -> 1202.
            let token_a2 = mint_restore_token(address, bearer, 15, a.memory_id).await;
            let (status, again) =
                restore_call(address, bearer, 16, a.memory_id, Some(&token_a2)).await;
            assert_eq!(status, 200, "{again}");
            assert_restore_conflict(&again, 1202, "NOT_REVERSIBLE");
            tokio::task::block_in_place(|| {
                assert_eq!(
                    memory_state(&mut handle, a.memory_id),
                    ("active".into(), None, false, true)
                );
                assert_eq!(token_consumed(&mut handle, &token_a2), Some(false));
            });

            // (e) replay of the same confirmed restore -> the original success, one RESTORE event.
            let supersede_seq_g = drive_supersede(address, bearer, 17, g.memory_id, h.memory_id).await;
            let token_g = mint_restore_token(address, bearer, 19, g.memory_id).await;
            let (status, first) = restore_call(address, bearer, 20, g.memory_id, Some(&token_g)).await;
            assert_eq!(status, 200, "{first}");
            let first_seq = first["result"]["structuredContent"]["stream_seq"]
                .as_i64()
                .expect("first restore stream_seq");
            assert!(first_seq > supersede_seq_g);
            let (status, replay) = restore_call(address, bearer, 21, g.memory_id, Some(&token_g)).await;
            assert_eq!(status, 200, "{replay}");
            assert_ne!(replay["result"]["isError"], true, "replay is the original success: {replay}");
            assert_eq!(
                replay["result"]["structuredContent"]["stream_seq"].as_i64(),
                Some(first_seq),
                "replay returns the original event's stream seq, never a second restore"
            );
            tokio::task::block_in_place(|| {
                assert_eq!(lifecycle_op_count(&mut handle, g.memory_id, "RESTORE"), 1,
                    "an idempotent replay appends no second RESTORE event");
                assert_eq!(
                    memory_state(&mut handle, g.memory_id),
                    ("active".into(), None, false, true)
                );
            });

            // (f) an invisible / non-existent target -> NOT_FOUND (never an existence oracle).
            let ghost = Uuid::now_v7();
            let token_ghost = mint_restore_token(address, bearer, 22, ghost).await;
            let (status, missing) =
                restore_call(address, bearer, 23, ghost, Some(&token_ghost)).await;
            assert_eq!(status, 200, "{missing}");
            assert_tool_error(&missing, "NOT_FOUND");

            // Fault injection: the authority UPDATE fails inside the gated transaction -> the
            // RESTORE event append and the token consume must roll back with it (no orphan event).
            let m = tokio::task::block_in_place(|| handle.seed_workspace_visible_context_record());
            drive_supersede(address, bearer, 24, m.memory_id, b.memory_id).await;
            let token_m = mint_restore_token(address, bearer, 26, m.memory_id).await;
            tokio::task::block_in_place(|| {
                handle
                    .admin
                    .batch_execute(&format!(
                        "CREATE FUNCTION {fault_fn}() RETURNS trigger LANGUAGE plpgsql AS $$ \
                         BEGIN RAISE EXCEPTION 'injected restore fault' USING ERRCODE='23514'; END $$; \
                         CREATE TRIGGER humaux_test_restore_fault BEFORE UPDATE OF status \
                         ON private.memory_records FOR EACH ROW WHEN (NEW.status = 'active') \
                         EXECUTE FUNCTION {fault_fn}();"
                    ))
                    .expect("owner installs restore fault");
            });
            let (status, faulted) =
                restore_call(address, bearer, 27, m.memory_id, Some(&token_m)).await;
            tokio::task::block_in_place(|| {
                handle
                    .admin
                    .batch_execute(&format!(
                        "DROP TRIGGER humaux_test_restore_fault ON private.memory_records; \
                         DROP FUNCTION {fault_fn}();"
                    ))
                    .expect("owner removes restore fault");
            });
            assert_eq!(status, 200, "{faulted}");
            assert_tool_error(&faulted, "CONFLICT");
            tokio::task::block_in_place(|| {
                assert_eq!(
                    memory_state(&mut handle, m.memory_id),
                    ("superseded".into(), Some(b.memory_id), true, true),
                    "the authority flip rolled back"
                );
                assert_eq!(lifecycle_op_count(&mut handle, m.memory_id, "RESTORE"), 0,
                    "no orphaned RESTORE event survived the rolled-back UPDATE");
                assert_eq!(token_consumed(&mut handle, &token_m), Some(false));
            });

            // (g) concurrent double-restore of one superseded memory: exactly one wins, the
            // loser's consume + event roll back, G59-4 holds throughout (same window-lock
            // overlap trick as the supersede race).
            let n = tokio::task::block_in_place(|| handle.seed_workspace_visible_context_record());
            drive_supersede(address, bearer, 28, n.memory_id, b.memory_id).await;
            let token_n1 = mint_restore_token(address, bearer, 30, n.memory_id).await;
            let token_n2 = mint_restore_token(address, bearer, 31, n.memory_id).await;
            let mut window_lock = tokio::task::block_in_place(|| {
                let mut client = handle.owner_client().expect("owner window-lock client");
                client.batch_execute("BEGIN").expect("owner window-lock txn");
                let locked = client
                    .execute(
                        "SELECT 1 FROM control.quota_windows WHERE tenant_id=$1 FOR UPDATE",
                        &[&handle.tenant_id],
                    )
                    .expect("owner holds the quota window row");
                assert!(locked >= 1, "fixture tenant has a quota window to hold");
                client
            });
            let left = tokio::spawn({
                let bearer = bearer.to_owned();
                let token = token_n1.clone();
                let target = n.memory_id;
                async move { restore_call(address, &bearer, 32, target, Some(&token)).await }
            });
            tokio::time::sleep(Duration::from_millis(400)).await;
            let right = tokio::spawn({
                let bearer = bearer.to_owned();
                let token = token_n2.clone();
                let target = n.memory_id;
                async move { restore_call(address, &bearer, 33, target, Some(&token)).await }
            });
            tokio::time::sleep(Duration::from_millis(400)).await;
            tokio::task::block_in_place(|| {
                let queued: i64 = window_lock
                    .query_one(
                        "SELECT count(*) FROM pg_stat_activity WHERE usename='role_gateway' \
                         AND wait_event_type='Lock' AND state='active'",
                        &[],
                    )
                    .expect("owner observes queued gateway writers")
                    .get(0);
                assert_eq!(queued, 2, "both gated restore transactions are open and queued");
                window_lock.batch_execute("COMMIT").expect("release quota window row");
                drop(window_lock);
            });
            let (left, right) = tokio::join!(left, right);
            let outcomes = [left.expect("left restore"), right.expect("right restore")];
            let winners = outcomes
                .iter()
                .filter(|(status, response)| {
                    *status == 200
                        && response["result"]["isError"] != true
                        && response["result"]["structuredContent"].get("restored_at").is_some()
                })
                .count();
            assert_eq!(winners, 1, "exactly one concurrent restore wins: {outcomes:?}");
            tokio::task::block_in_place(|| {
                assert_eq!(
                    memory_state(&mut handle, n.memory_id),
                    ("active".into(), None, false, true)
                );
                assert_eq!(lifecycle_op_count(&mut handle, n.memory_id, "RESTORE"), 1,
                    "one restore event for one winning restore");
                let consumed = [&token_n1, &token_n2]
                    .into_iter()
                    .filter(|token| token_consumed(&mut handle, token) == Some(true))
                    .count();
                assert_eq!(consumed, 1, "the loser's token consume rolled back");
                assert!(all_rows_satisfy_g59_4(&mut handle), "G59-4 never violated");
            });

            stop_server(server).await.expect("stop restore server");
        });
    });
}

// ---------------------------------------------------------------------------------------
// §36 memory.pin / memory.unpin behind the same confirm gate (ADR-0019)
// ---------------------------------------------------------------------------------------

async fn binding_call(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    action: &str,
    memory_id: Uuid,
    token: Option<&str>,
) -> (u16, Value) {
    let mut arguments = json!({ "action": action, "memory_id": memory_id });
    if let Some(token) = token {
        arguments["confirm_token"] = Value::String(token.to_owned());
    }
    raw_request(
        address,
        &tool_call_headers("memory", bearer),
        &rpc(request_id, "tools/call", call_params("memory", arguments)),
    )
    .await
}

/// First call of pin/unpin: a success-shaped `confirmation_required` result, never a write.
async fn mint_binding_token(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    action: &str,
    memory_id: Uuid,
) -> String {
    let (status, response) =
        binding_call(address, bearer, request_id, action, memory_id, None).await;
    assert_eq!(status, 200, "first call is an MCP result: {response}");
    let structured = assert_tool_response(&response, ToolName::Memory);
    assert_eq!(structured["confirmation_required"], true, "{response}");
    assert_eq!(
        structured["operation"],
        format!("memory.{action}"),
        "{response}"
    );
    assert_eq!(
        structured["target"]["memory_id"],
        memory_id.to_string(),
        "{response}"
    );
    assert!(
        structured["target"].get("replacement_memory_id").is_none(),
        "{response}"
    );
    let token = structured["confirm_token"]
        .as_str()
        .expect("confirm_token string")
        .to_owned();
    assert_eq!(token.len(), 43, "base64url of 32 bytes: {token}");
    token
}

/// Active (revoked_at IS NULL) PINNED rows for one memory: `(count, binding_id of the first)`.
fn pinned_rows(handle: &mut Handle, memory_id: Uuid) -> (i64, Option<Uuid>) {
    let row = handle
        .admin
        .query_one(
            "SELECT count(*), min(context_binding_id::text)::uuid FROM private.context_bindings \
             WHERE tenant_id=$1 AND memory_id=$2 AND mode='PINNED' AND revoked_at IS NULL",
            &[&handle.tenant_id, &memory_id],
        )
        .expect("owner counts pinned rows");
    (row.get(0), row.get(1))
}

/// The row versions of a memory and its evidence links/objects: unpin must leave all of them
/// exactly as they were (§36: "unpin 只撤 binding，不改 Evidence/Memory").
fn memory_and_evidence_versions(
    handle: &mut Handle,
    memory_id: Uuid,
) -> (String, String, Vec<String>) {
    let memory = handle
        .admin
        .query_one(
            "SELECT status, xmin::text FROM private.memory_records WHERE memory_id=$1",
            &[&memory_id],
        )
        .expect("owner reads memory row version");
    let evidence: Vec<String> = handle
        .admin
        .query(
            "SELECT me.xmin::text || ':' || eo.xmin::text FROM private.memory_evidence me \
             JOIN private.evidence_objects eo ON eo.evidence_id = me.evidence_id \
             WHERE me.memory_id=$1 ORDER BY me.evidence_id",
            &[&memory_id],
        )
        .expect("owner reads evidence row versions")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    (memory.get(0), memory.get(1), evidence)
}

/// libpq-standard `options=-c role=X` (same helper `crates/adapters/tests/consolidate_snapshot.rs`
/// uses): a real `role_consolidation_worker` LOGIN for the §11.8 exclusion witness.
fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

/// Runs the real consolidation selector (`select_and_materialize_inputs`, whose WHERE carries
/// the one `ACTIVE_NO_AUTO_MUTATE_BINDING` predicate) for the fixture workspace and returns
/// the selected memory ids.
async fn consolidation_selected_inputs(handle: &mut Handle) -> Vec<Uuid> {
    let admin_dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("fixture ran, so the DSN is set");
    let pool = humaux_adapters::postgres::ConsolidationDbPool::connect(&dsn_as_role(
        &admin_dsn,
        "role_consolidation_worker",
    ))
    .await
    .expect("actual role_consolidation_worker login");
    let run_id = humaux_adapters::consolidate_repo::create_run(
        &pool,
        handle.tenant_id,
        handle.reasoning_domain_id,
        Some(handle.workspace_id),
    )
    .await
    .expect("consolidation run");
    let selected: Vec<Uuid> = humaux_adapters::consolidate_repo::select_and_materialize_inputs(
        &pool,
        run_id,
        handle.tenant_id,
        handle.reasoning_domain_id,
        Some(handle.workspace_id),
        1_000,
    )
    .await
    .expect("consolidation selection")
    .into_iter()
    .map(|input| input.memory_id.into_inner().0)
    .collect();
    // The fixture teardown does not know consolidation tables; drop the witness run's rows
    // so its memory_records deletes are not blocked by `memory_consolidation_inputs`' FK.
    tokio::task::block_in_place(|| {
        handle
            .admin
            .batch_execute(&format!(
                "DELETE FROM private.memory_consolidation_inputs WHERE run_id='{run_id}'; \
                 DELETE FROM private.memory_consolidation_runs WHERE run_id='{run_id}'"
            ))
            .expect("owner removes the witness consolidation run");
    });
    selected
}

async fn assemble_pinned_ids(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    workspace: Uuid,
) -> Vec<Uuid> {
    let (status, response) = raw_request(
        address,
        &tool_call_headers("context", bearer),
        &rpc(
            request_id,
            "tools/call",
            call_params("context", json!({"workspace_id": workspace})),
        ),
    )
    .await;
    assert_eq!(status, 200, "context.assemble: {response}");
    let value = assert_tool_response(&response, ToolName::Context);
    value["handoff"]["pinned"]
        .as_array()
        .expect("pinned IDs")
        .iter()
        .map(|row| Uuid::parse_str(row["memory_id"].as_str().expect("memory_id")).expect("uuid"))
        .collect()
}

#[test]
#[allow(clippy::too_many_lines)] // One real HTTP fixture carries the whole pin/unpin acceptance matrix.
fn native_mcp_memory_pin_unpin_confirm_gate_acceptance() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>("native_mcp_memory_pin_unpin_confirm_gate", |mut handle| {
        handle.assert_gateway_login();
        let prefix = format!("mpin{}", &Uuid::now_v7().simple().to_string()[..12]);
        let wire = format!("{prefix}.{}", "f".repeat(32));
        let credential = handle.seed_synthetic_service_credential_and_window(
            SyntheticCredentialScopes::RememberWriteAndContextRead,
            &prefix,
            &wire,
            &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
            64,
        );
        // x: a workspace-visible memory with NO mandatory binding (the fixture's MANDATORY row
        // is revoked) and an authority no mandatory selector picks up, so the only way it can
        // reach the Context is the PINNED lane. y: a second record for the misbound-token case.
        let x = handle.seed_workspace_visible_context_record();
        let y = handle.seed_workspace_visible_context_record();
        for record in [&x, &y] {
            handle
                .admin
                .execute(
                    "UPDATE private.context_bindings SET revoked_at = now() WHERE context_binding_id=$1",
                    &[&record.binding_id],
                )
                .expect("owner revokes the fixture's mandatory binding");
            handle
                .admin
                .execute(
                    "UPDATE private.memory_records SET authority_class='ExplicitTaskContext' WHERE memory_id=$1",
                    &[&record.memory_id],
                )
                .expect("owner raises authority above the pinned-lane floor");
        }

        let runtime_handle = handle.rt.handle().clone();
        let runtime = runtime_handle
            .block_on(handle.fresh_runtime())
            .expect("checked pin runtime");
        let app = application(&handle, runtime);
        runtime_handle.block_on(async {
            let (address, server) = start(app).await;
            let bearer = credential.bearer.as_str();
            let workspace = handle.workspace_id;

            // Baseline: nothing pinned, the Context has no pinned rows, consolidation sees x.
            assert_eq!(assemble_pinned_ids(address, bearer, 1, workspace).await, Vec::<Uuid>::new());
            assert!(consolidation_selected_inputs(&mut handle).await.contains(&x.memory_id));
            let before = blocking_counts(&mut handle);

            // (a) pin without a token: confirmation_required, no binding row, no durable write
            // besides the token row + its tagged audit.
            let token_x = mint_binding_token(address, bearer, 2, "pin", x.memory_id).await;
            tokio::task::block_in_place(|| {
                assert_eq!(pinned_rows(&mut handle, x.memory_id), (0, None), "mint inserts no binding");
                assert_eq!(token_consumed(&mut handle, &token_x), Some(false));
                assert_eq!(durable_counts(counts(&mut handle)), durable_counts(before));
            });

            // Negative gate: a token minted for y is rejected on x (target binding), and a pin
            // token is rejected by unpin (operation binding). Both: CONFLICT, no row, token unconsumed.
            // Fault-injection sentinel: stub `consume_in_txn` to always pass and these go red.
            let token_y = mint_binding_token(address, bearer, 3, "pin", y.memory_id).await;
            let (status, wrong_target) =
                binding_call(address, bearer, 4, "pin", x.memory_id, Some(&token_y)).await;
            assert_eq!(status, 200, "{wrong_target}");
            assert_tool_error(&wrong_target, "CONFLICT");
            let (status, wrong_op) =
                binding_call(address, bearer, 5, "unpin", x.memory_id, Some(&token_x)).await;
            assert_eq!(status, 200, "{wrong_op}");
            assert_tool_error(&wrong_op, "CONFLICT");
            let garbage = "A".repeat(43);
            let (status, unknown) =
                binding_call(address, bearer, 6, "pin", x.memory_id, Some(&garbage)).await;
            assert_eq!(status, 200, "{unknown}");
            assert_tool_error(&unknown, "CONFLICT");
            tokio::task::block_in_place(|| {
                assert_eq!(pinned_rows(&mut handle, x.memory_id), (0, None));
                assert_eq!(pinned_rows(&mut handle, y.memory_id), (0, None));
                assert_eq!(token_consumed(&mut handle, &token_x), Some(false));
                assert_eq!(token_consumed(&mut handle, &token_y), Some(false));
            });

            // (b) pin with its token: one PINNED row, revoked_at NULL, token consumed; the
            // read side surfaces it through the existing PinnedLane with zero read-side change.
            let (status, pinned) =
                binding_call(address, bearer, 7, "pin", x.memory_id, Some(&token_x)).await;
            assert_eq!(status, 200, "{pinned}");
            let structured = assert_tool_response(&pinned, ToolName::Memory);
            assert_eq!(structured["memory_id"], x.memory_id.to_string());
            assert_eq!(structured["mode"], "PINNED");
            assert_eq!(structured["state"], "pinned");
            assert_eq!(structured["inserted"], true);
            let binding_id = Uuid::parse_str(structured["binding_id"].as_str().expect("binding_id")).expect("uuid");
            tokio::task::block_in_place(|| {
                assert_eq!(pinned_rows(&mut handle, x.memory_id), (1, Some(binding_id)));
                let row = handle
                    .admin
                    .query_one(
                        "SELECT mode, scope_kind, scope_id, created_by, revoked_at IS NULL \
                         FROM private.context_bindings WHERE context_binding_id=$1",
                        &[&binding_id],
                    )
                    .expect("owner reads the binding");
                let (mode, scope_kind, scope_id, created_by, active): (String, String, Uuid, Uuid, bool) =
                    (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
                assert_eq!((mode.as_str(), scope_kind.as_str(), scope_id, created_by, active),
                    ("PINNED", "WORKSPACE", workspace, handle.user_id, true));
                assert_eq!(token_consumed(&mut handle, &token_x), Some(true));
                let (minted, executed): (i64, i64) = {
                    let row = handle
                        .admin
                        .query_one(
                            "SELECT count(*) FILTER (WHERE $2 = ANY(risk_tags)), \
                                    count(*) FILTER (WHERE NOT ($2 = ANY(risk_tags))) \
                             FROM control.audit_events WHERE tenant_id=$1 \
                               AND resource_id='memory.pin' AND action='MCP_REQUEST_FINISHED' AND result='OK'",
                            &[&handle.tenant_id, &humaux_domain::confirm::RISK_TAG_CONFIRMATION_MINTED],
                        )
                        .expect("owner counts finished audits");
                    (row.get(0), row.get(1))
                };
                assert_eq!((minted, executed), (2, 1), "two tagged mints (x, y), one executed write");
            });
            assert_eq!(assemble_pinned_ids(address, bearer, 8, workspace).await, vec![x.memory_id]);
            // §11.8: the pinned memory is no longer an automatic consolidation input.
            assert!(!consolidation_selected_inputs(&mut handle).await.contains(&x.memory_id));

            // (c) idempotent: a second confirmed pin returns the same row, inserts nothing.
            let token_again = mint_binding_token(address, bearer, 9, "pin", x.memory_id).await;
            let (status, again) =
                binding_call(address, bearer, 10, "pin", x.memory_id, Some(&token_again)).await;
            assert_eq!(status, 200, "{again}");
            let structured = assert_tool_response(&again, ToolName::Memory);
            assert_eq!(structured["binding_id"], binding_id.to_string());
            assert_eq!(structured["inserted"], false);
            tokio::task::block_in_place(|| {
                assert_eq!(pinned_rows(&mut handle, x.memory_id), (1, Some(binding_id)));
                assert_eq!(token_consumed(&mut handle, &token_again), Some(true));
            });

            // (c') Envelope-rollback witness (ADR-0019 D-B): the binding write runs on the
            // confirm transaction, so a rejection *after* it leaves the PINNED row active and
            // the token unconsumed. The owner holds a row lock on the binding so the
            // envelope's UPDATE waits past its 1s reservation lease; once released, the
            // revoke lands, finalize finds the lease expired (Released, not Consumed) and
            // the whole envelope — revoke included — rolls back with CONFLICT. A binding
            // writer on its own connection would have committed the revoke here.
            let rollback_token = mint_binding_token(address, bearer, 17, "unpin", x.memory_id).await;
            let request_id = Uuid::now_v7();
            let mut metadata = humaux_domain::audit::AuditMetadata::new();
            metadata.insert("role", "member").expect("allowlisted audit metadata");
            let finished_audit = humaux_domain::audit::AuditEvent {
                event_id: humaux_domain::audit::AuditEventId::new(),
                ts: std::time::SystemTime::now(),
                tenant_id: TenantId(handle.tenant_id),
                actor_type: "user".into(),
                actor_id: handle.principal_id.to_string(),
                action: humaux_domain::audit::McpAuditAction::McpRequestFinished.as_str().into(),
                resource_type: "mcp".into(),
                resource_id: "memory.unpin".into(),
                result: "OK".into(),
                request_id: request_id.to_string(),
                trace_id: format!("pin-rollback-{request_id}"),
                client_ip: "127.0.0.1".into(),
                user_agent_hash: "pin-rollback-witness".into(),
                risk_tags: vec![],
                before_fingerprint: None,
                after_fingerprint: None,
                metadata,
            };
            let request = humaux_adapters::context_repo::BindingWriteRequest {
                request_id,
                request_fingerprint: "0".repeat(64),
                reservation_ttl: Duration::from_secs(1),
                memory: humaux_domain::authority::MemoryId(x.memory_id),
                workspace: WorkspaceId(workspace),
                claim: humaux_adapters::confirm_token_repo::ConfirmationClaim {
                    op: humaux_domain::confirm::DestructiveOp::MemoryUnpin,
                    target_id: x.memory_id,
                    successor_id: None,
                    nonce_sha256: humaux_domain::confirm::ConfirmToken::decode(&rollback_token)
                        .expect("wire token decodes")
                        .sha256(),
                },
                finished_audit,
            };
            let mut row_lock = tokio::task::block_in_place(|| {
                let mut txn = handle.admin.transaction().expect("owner lock transaction");
                txn.execute(
                    "SELECT 1 FROM private.context_bindings WHERE context_binding_id=$1 FOR UPDATE",
                    &[&binding_id],
                )
                .expect("owner locks the PINNED row");
                txn
            });
            let (rejected, ()) = tokio::join!(
                humaux_adapters::context_repo::unpin_confirmed(&handle.runtime, &handle.auth, request),
                async move {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    tokio::task::block_in_place(|| {
                        let held = row_lock
                            .query_one(
                                "SELECT count(*) FROM pg_stat_activity \
                                 WHERE wait_event_type = 'Lock' \
                                   AND query LIKE 'UPDATE private.context_bindings SET revoked_at%'",
                                &[],
                            )
                            .expect("owner reads lock waiters")
                            .get::<_, i64>(0);
                        assert_eq!(held, 1, "the envelope's revoke is waiting on the owner's row lock");
                        row_lock.commit().expect("owner releases the row lock");
                    });
                }
            );
            assert_eq!(
                rejected.map(|outcome| outcome.binding_id),
                Err(humaux_domain::error::ErrorCode::Conflict),
                "a lease expired at finalize is rejected"
            );
            tokio::task::block_in_place(|| {
                assert_eq!(
                    pinned_rows(&mut handle, x.memory_id),
                    (1, Some(binding_id)),
                    "a post-write rejection must roll the binding write back with the envelope"
                );
                assert_eq!(token_consumed(&mut handle, &rollback_token), Some(false));
            });

            // (d) unpin: first call mints, the confirmed call sets revoked_at on the binding
            // row only — Memory and Evidence row versions are byte-identical before/after.
            let versions_before = tokio::task::block_in_place(|| memory_and_evidence_versions(&mut handle, x.memory_id));
            let unpin_token = mint_binding_token(address, bearer, 11, "unpin", x.memory_id).await;
            tokio::task::block_in_place(|| assert_eq!(pinned_rows(&mut handle, x.memory_id), (1, Some(binding_id))));
            let (status, unpinned) =
                binding_call(address, bearer, 12, "unpin", x.memory_id, Some(&unpin_token)).await;
            assert_eq!(status, 200, "{unpinned}");
            let structured = assert_tool_response(&unpinned, ToolName::Memory);
            assert_eq!(structured["binding_id"], binding_id.to_string());
            assert_eq!(structured["state"], "unpinned");
            tokio::task::block_in_place(|| {
                assert_eq!(pinned_rows(&mut handle, x.memory_id), (0, None));
                let revoked: bool = handle
                    .admin
                    .query_one(
                        "SELECT revoked_at IS NOT NULL FROM private.context_bindings WHERE context_binding_id=$1",
                        &[&binding_id],
                    )
                    .expect("owner reads revoked_at")
                    .get(0);
                assert!(revoked, "unpin is a soft revoke of the same row");
                assert_eq!(memory_and_evidence_versions(&mut handle, x.memory_id), versions_before, "unpin touched Memory/Evidence");
                assert_eq!(token_consumed(&mut handle, &unpin_token), Some(true));
            });
            assert_eq!(assemble_pinned_ids(address, bearer, 13, workspace).await, Vec::<Uuid>::new());
            assert!(consolidation_selected_inputs(&mut handle).await.contains(&x.memory_id));

            // (e) unpin of a non-pinned memory: CONFLICT, and the token is not consumed.
            let unpin_again = mint_binding_token(address, bearer, 14, "unpin", x.memory_id).await;
            let (status, conflict) =
                binding_call(address, bearer, 15, "unpin", x.memory_id, Some(&unpin_again)).await;
            assert_eq!(status, 200, "{conflict}");
            assert_tool_error(&conflict, "CONFLICT");
            tokio::task::block_in_place(|| {
                assert_eq!(token_consumed(&mut handle, &unpin_again), Some(false), "rejection rolls the consume back");
            });

            // (f) replaying a consumed token is CONFLICT and pins nothing.
            let (status, replay) =
                binding_call(address, bearer, 16, "pin", x.memory_id, Some(&token_x)).await;
            assert_eq!(status, 200, "{replay}");
            assert_tool_error(&replay, "CONFLICT");
            tokio::task::block_in_place(|| assert_eq!(pinned_rows(&mut handle, x.memory_id), (0, None)));

            stop_server(server).await.expect("stop pin server");
        });
    });
}
