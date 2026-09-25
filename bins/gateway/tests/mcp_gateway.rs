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
    membership_repo::{
        self, AdminAction, MembershipOutcome, MembershipRepoError, MembershipRequest,
    },
    operation_receipt::{self, AtomicRememberRequest},
    postgres::RuntimeDbPool,
    qdrant::{
        Distance, PointId, QdrantOperation, QdrantPointPayload, ShardingMethod,
        create_collection_body, ha_profile_for, subject_index_body, tenant_index_body, upsert,
    },
    quota_repo::RatePolicy,
};
use humaux_domain::{
    affect::{AffectAnnotation, AffectKind, BasisPoints, EmotionLabel},
    audit::{AuditEvent, AuditEventId, AuditMetadata, McpAuditAction},
    authority::{AuthorityClass, AuthorityStatus},
    context::{ContextBudget, SelectorId, SelectorOutcome},
    dataclass::DataClass,
    error::ErrorCode,
    evidence::{EvidenceOriginClass, payload_sha256},
    identity::{
        AuthorizationScope, BoundedSet, MembershipConflict, MembershipMutation, MembershipRole,
        MembershipState, PrincipalId, VisibilityClass,
    },
    ids::{Scope, TaskId, TenantId, UserId, WorkspaceId},
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
        "mandatory_not_satisfied",
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

/// Provisions one (tenant, workspace) pair the way ops does for the process's `v1` stream
/// (§16.2 / `xtask projection-serve`): the `v1` checkpoint row exists and is the family's
/// `serving` row. ADR-0031: every read route admits a pair only through that serving read, so
/// a fixture pair that was never provisioned reads `DEPENDENCY_UNAVAILABLE` — exactly what the
/// multi-pair acceptance asserts for its unprovisioned control. Idempotent: a row a test seeded
/// itself (`seed_semantic_checkpoint`, `seed_done_stream_identity`) or a later serving version
/// (`v2` after the §16.3 switch) is left as is.
fn provision_stream_pair(handle: &Handle, workspace_id: Uuid) {
    // `application()` is also built inside `block_on` bodies; the sync `postgres` client
    // spins its own runtime, so enter blocking mode the way the harness does for `handle.admin`.
    tokio::task::block_in_place(|| provision_stream_pair_blocking(handle, workspace_id));
}

fn provision_stream_pair_blocking(handle: &Handle, workspace_id: Uuid) {
    let mut owner = handle
        .owner_client()
        .expect("owner provisions the fixture stream pair");
    owner
        .execute(
            "INSERT INTO projection.stream_checkpoints \
               (tenant_id,scope_kind,scope_id,domain,projection_kind,projection_version) \
             VALUES($1,'workspace',$2,'knowledge','ingest','v1') ON CONFLICT DO NOTHING",
            &[&handle.tenant_id, &workspace_id],
        )
        .expect("owner seeds the v1 checkpoint row");
    owner
        .execute(
            "UPDATE projection.stream_checkpoints SET serving=true \
             WHERE tenant_id=$1 AND scope_kind='workspace' AND scope_id=$2 \
               AND domain='knowledge' AND projection_kind='ingest' AND projection_version='v1' \
               AND NOT EXISTS (SELECT 1 FROM projection.stream_checkpoints \
                               WHERE tenant_id=$1 AND scope_kind='workspace' AND scope_id=$2 \
                                 AND domain='knowledge' AND projection_kind='ingest' AND serving)",
            &[&handle.tenant_id, &workspace_id],
        )
        .expect("owner promotes v1 to serving when the family has no serving row");
}

fn application_with_budget(
    handle: &Handle,
    runtime: RuntimeDbPool,
    budget: ContextBudget,
) -> GatewayMcpApplication {
    // The in-process fixture app's bootstrap pair is provisioned like a deployed one.
    provision_stream_pair(handle, handle.workspace_id);
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
    .with_mood_half_life(MOOD_HALF_LIFE)
    .expect("positive fixture mood half-life")
}

/// §8.5.1 / ADR-0030 D-B fixture policy (`HUMAUX_GATEWAY_MOOD_HALF_LIFE_SECONDS` in the binary
/// fixture below): six hours, so a MOOD observed two half-lives ago reads at a quarter.
const MOOD_HALF_LIFE: Duration = Duration::from_secs(21_600);

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
        // §6.1.3/ADR-0029: the `subject_ids` uuid payload index the any-of prefilter uses.
        (
            format!("/collections/{collection}/index"),
            subject_index_body(),
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

/// Card 18 / §23.1②: the projection ledger of record for the points the semantic fixture
/// upserts by hand. [`seed_semantic_checkpoint`] leaves `issued_highwater = 0`, which was
/// harmless only while every read route passed `visible: None`; with the live count wired, a
/// Qdrant face carrying `n` points over a ledger that issued nothing is a genuine A2 overshoot
/// and the envelope would (correctly) refuse to state a ratio. These `n` settled `DONE` rows
/// are that ledger. No `ops.outbox` row is written for them on purpose: §15.5's RYW overlay
/// joins through `ops.outbox`, so — exactly like the already-projected points they stand for —
/// they must not appear as overlay items.
fn seed_semantic_projection_ledger(handle: &mut Handle, n: i64) {
    for seq in 1..=n {
        seed_semantic_projection_ledger_row(handle, seq);
    }
}

/// One settled `DONE` seq plus the highwater that admits it. Used on its own to force §23.1②'s
/// A2 `<` side: a row with no Qdrant point behind it is exactly an invisible loss.
fn seed_semantic_projection_ledger_row(handle: &mut Handle, seq: i64) {
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
             VALUES($1,'workspace',$2,'knowledge','ingest','v1',$3,$4,'DONE',clock_timestamp())",
            &[&handle.tenant_id, &handle.workspace_id, &seq, &commit_seq],
        )
        .expect("owner seeds the semantic fixture's settled ledger row");
    set_semantic_issued_highwater(handle, seq);
}

/// Removes one seeded ledger row again (and lowers the highwater with it), so a leg that forced
/// an A2 fault hands the fixture back healthy instead of leaking it into every later assertion.
fn drop_semantic_projection_ledger_row(handle: &mut Handle, seq: i64) {
    handle
        .admin
        .execute(
            "DELETE FROM projection.stream_log \
              WHERE tenant_id=$1 AND scope_kind='workspace' AND scope_id=$2 \
                AND domain='knowledge' AND projection_kind='ingest' \
                AND projection_version='v1' AND stream_seq=$3",
            &[&handle.tenant_id, &handle.workspace_id, &seq],
        )
        .expect("owner removes the forced invisible-loss ledger row");
    set_semantic_issued_highwater(handle, seq - 1);
}

fn set_semantic_issued_highwater(handle: &mut Handle, highwater: i64) {
    handle
        .admin
        .execute(
            "UPDATE projection.stream_checkpoints SET issued_highwater=$3 \
             WHERE tenant_id=$1 AND scope_kind='workspace' AND scope_id=$2 \
               AND domain='knowledge' AND projection_kind='ingest' AND projection_version='v1'",
            &[&handle.tenant_id, &handle.workspace_id, &highwater],
        )
        .expect("owner sets the semantic fixture's issued highwater");
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

/// §6.1.3/ADR-0029 fixture: one subject row (owner-side) for the semantic A/B matrix.
fn seed_subject_row(handle: &mut Handle, name: &str) -> Uuid {
    handle
        .admin
        .query_one(
            "INSERT INTO private.subjects (tenant_id, kind, display_name) \
             VALUES ($1, 'ORGANISATION', $2) RETURNING subject_id",
            &[&handle.tenant_id, &name],
        )
        .expect("owner seeds subject")
        .get(0)
}

fn link_memory_subject(handle: &mut Handle, memory_id: Uuid, subject_id: Uuid) {
    handle
        .admin
        .execute(
            "INSERT INTO private.memory_subjects \
               (tenant_id, memory_id, subject_id, relation, source_kind, confidence_bp) \
             VALUES ($1, $2, $3, 'ABOUT', 'DECLARED', 10000)",
            &[&handle.tenant_id, &memory_id, &subject_id],
        )
        .expect("owner links memory to subject");
}

/// §8.5.1/ADR-0030 fixture: one owner-side `private.memory_affects` row in the shape
/// `affect_repo::annotate` writes (provenance = the record's PRIMARY Evidence). `observed_ago_secs`
/// backdates `observed_at` so a MOOD row reads decayed through the real read path.
#[allow(clippy::too_many_arguments)] // one row's worth of fixture columns
fn seed_affect_row(
    handle: &mut Handle,
    record: &ScopedContextRecord,
    kind: &str,
    label: &str,
    valence: i16,
    arousal: i16,
    intensity: i16,
    target_subject: Option<Uuid>,
    observed_ago_secs: f64,
    half_life_seconds: Option<i32>,
) -> Uuid {
    handle
        .admin
        .query_one(
            "INSERT INTO private.memory_affects \
               (tenant_id, memory_id, affect_kind, label, valence_bp, arousal_bp, intensity_bp, \
                confidence_bp, evidence_id, target_subject_id, observed_at, half_life_seconds) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, 10000, $8, $9, \
                     now() - make_interval(secs => $10), $11) RETURNING affect_id",
            &[
                &handle.tenant_id,
                &record.memory_id,
                &kind,
                &label,
                &valence,
                &arousal,
                &intensity,
                &record.evidence_id,
                &target_subject,
                &observed_ago_secs,
                &half_life_seconds,
            ],
        )
        .expect("owner seeds affect row")
        .get(0)
}

/// The Qdrant-side twin of [`seed_affect_row`] for the worker-less harness (the projection
/// worker would build exactly this from the PG row).
fn affect_annotation(
    kind: AffectKind,
    label: EmotionLabel,
    valence: i16,
    arousal: i16,
    intensity: i16,
) -> AffectAnnotation {
    AffectAnnotation {
        kind,
        label: Some(label),
        valence: Some(BasisPoints::signed(valence).expect("fixture valence")),
        arousal: Some(BasisPoints::signed(arousal).expect("fixture arousal")),
        dominance: None,
        intensity: BasisPoints::unit(intensity).expect("fixture intensity"),
        confidence: BasisPoints::MAX,
        target_subject: None,
        target_scope: None,
    }
}

fn returned_memory_ids(structured: &Value) -> BTreeSet<String> {
    structured["items"]
        .as_array()
        .expect("semantic items")
        .iter()
        .map(|item| item["memory_id"].as_str().expect("memory id").to_owned())
        .collect()
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
    // §23.3④ / ADR-0041 D-H: `context.assemble` reads `projection.stream_log` for the
    // request's own six-column `StreamKey` in the manifest's snapshot. This fixture has no
    // semantic runtime, so §23.1②'s missing index count still holds the class at
    // `cannot_establish` — but the reason is no longer `count_unknown`, and these five are
    // real `0`s of an empty ledger where they used to be `null`s.
    assert_ne!(
        content["completeness"]["reason"], "count_unknown",
        "the pipeline half is established now — whatever still holds this fixture at \
         cannot_establish (a failed mandatory lane, or card 18's missing index count), it is \
         no longer a missing count: {content}"
    );
    for (block, field) in [
        ("evidence", "persisted"),
        ("knowledge", "eligible"),
        ("knowledge", "processed"),
        ("knowledge", "waiting_key"),
        ("knowledge", "failed"),
    ] {
        assert_eq!(content["pipeline"][block][field], 0, "{content}");
        assert_eq!(
            content["pipeline"][block]["count_scope"], "stream_ledger",
            "{content}"
        );
    }
    assert_eq!(
        content["pipeline"]["evidence"]["expected"],
        Value::Null,
        "§23.1①: no batch_id ⇒ expected stays null: {content}"
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
    // §23.3④ / ADR-0041 D-D: `memory.get` is the ONE read route whose pipeline counts stay
    // unknown, and it is not an oversight — a DirectGet establishes no census, `classify()`
    // maps `DirectGet` to `Exact`, and §22.0 makes `exact` without a `predicate_id` a hard
    // 5xx. Filling these five with numbers (recall and context.assemble now do, D-H) is what
    // would put this route on that path. Pinned here so that move cannot happen quietly.
    assert!(content["pipeline"]["evidence"]["persisted"].is_null());
    assert!(content["pipeline"]["knowledge"]["eligible"].is_null());
    assert_eq!(
        content["pipeline"]["evidence"]["count_scope"],
        "authorized_view"
    );
    assert_eq!(
        content["pipeline"]["knowledge"]["count_scope"],
        "authorized_view"
    );
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
    // §22.4: neither of these two routes enumerates an authorized universe, so neither
    // publishes a proven lower bound. The pipeline half now differs between them and is
    // asserted by each caller (ADR-0041 D-D vs D-H).
    assert!(content["completeness"]["known_lower_bound"].is_null());
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
    assert_eq!(content["grounding"]["current"], returned);
    assert_eq!(content["grounding"]["not_judged"], 0);
    // No semantic runtime in these fixtures ⇒ §23.1②'s index count is legitimately
    // unavailable, so no route here can state a ratio (card 18). The census / pipeline-count
    // half differs per route and is asserted by each caller (ADR-0041 D-D/D-E).
    assert!(content["pipeline"]["projection"]["visible"].is_null());
    assert!(content["pipeline"]["projection"]["completeness_ratio"].is_null());
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
    // §22.1 / §23.3④ (ADR-0041): this route DID count its own authorized universe in the
    // manifest's snapshot, so §22.4's proven lower bound exists even while §23.1②'s missing
    // index count keeps the class at cannot_establish — and both pipeline blocks are real
    // `stream_ledger` readings of this fixture's (empty) stream ledger, not `null`s.
    assert_eq!(
        content["completeness"]["known_lower_bound"],
        expected.len(),
        "§22.4 'at least N': {response}"
    );
    for block in ["evidence", "knowledge"] {
        assert_eq!(
            content["pipeline"][block]["count_scope"], "stream_ledger",
            "{response}"
        );
    }
    assert!(
        content["pipeline"]["evidence"]["persisted"].is_u64(),
        "{response}"
    );
    for field in ["eligible", "processed", "waiting_key", "failed"] {
        assert!(
            content["pipeline"]["knowledge"][field].is_u64(),
            "{response}"
        );
    }
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
#[ignore = "lane(a:request_guard) requires the isolated request-guard PostgreSQL fixture, pinned scanner and disposable Qdrant"]
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
                // 48 covered the assertion legs; ADR-0041 D-I's 2 x 30 timed calls need the
                // headroom, and `quota_repo::issue_window` refuses to re-issue mid-test.
                // Nothing in this test asserts exhaustion — the ceiling is not the subject.
                200,
            );
            let first = handle.seed_workspace_visible_context_record();
            let second = handle.seed_workspace_visible_context_record();
            // ADR-0024 D-C: a semantically-matching row that will be ARCHIVED. Its Qdrant point
            // stays (points are not deleted on archive); the PG hydrate gate is the SOLE place
            // recall excludes it, so recall.search returning only {first, second} is what a
            // flipped `include_archived` at the recall hydrate call would break.
            // §6.1.3/ADR-0029 D-C A/B matrix (worker-less harness: projected points are seeded
            // directly, exactly as the two rows above). PG linkage of record: first→{A},
            // second→{B}, third→{A,B}. Qdrant payloads: first [A], second [A,B] (claims A but PG
            // says only B — the hydrate re-check witness), third [A,B].
            let third = handle.seed_workspace_visible_context_record();
            let subject_a = seed_subject_row(&mut handle, "Acme (A)");
            let subject_b = seed_subject_row(&mut handle, "Bolt (B)");
            link_memory_subject(&mut handle, first.memory_id, subject_a);
            link_memory_subject(&mut handle, second.memory_id, subject_b);
            link_memory_subject(&mut handle, third.memory_id, subject_a);
            link_memory_subject(&mut handle, third.memory_id, subject_b);
            // §8.5.1/ADR-0030 D-D affect matrix (PG rows of record). first: EMOTION FRUSTRATION
            // about A (never decays). second: NO row (its Qdrant payload will claim FRUSTRATION —
            // the hydrate re-check witness). third: MOOD CALM observed two half-lives ago, so its
            // read-time effective intensity is 8200 / 4 = 2050 while the row keeps 8200.
            seed_affect_row(
                &mut handle,
                &first,
                "EMOTION",
                "FRUSTRATION",
                -8_000,
                5_000,
                9_000,
                Some(subject_a),
                0.0,
                None,
            );
            seed_affect_row(
                &mut handle,
                &third,
                "MOOD",
                "CALM",
                6_000,
                -4_000,
                8_200,
                None,
                2.0 * MOOD_HALF_LIFE.as_secs_f64(),
                Some(i32::try_from(MOOD_HALF_LIFE.as_secs()).expect("fixture half-life fits i32")),
            );
            let first_point = Uuid::new_v4();
            let second_point = Uuid::new_v4();
            let third_point = Uuid::new_v4();
            let first_updated = seed_semantic_registry_row(&mut handle, &first, first_point);
            let second_updated = seed_semantic_registry_row(&mut handle, &second, second_point);
            let third_updated = seed_semantic_registry_row(&mut handle, &third, third_point);
            seed_semantic_checkpoint(&mut handle);
            // Card 18: three points, three settled ledger rows — §23.1②'s A2 has both sides.
            seed_semantic_projection_ledger(&mut handle, 3);

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
                let sa = humaux_domain::subject::SubjectId(subject_a);
                let sb = humaux_domain::subject::SubjectId(subject_b);
                let frustration =
                    affect_annotation(AffectKind::Emotion, EmotionLabel::Frustration, -8_000, 5_000, 9_000);
                let first_payload = semantic_payload(&handle, first_updated)
                    .with_subject_ids(vec![sa])
                    .with_affects(vec![frustration.clone()]);
                // Tampered/stale payload: claims FRUSTRATION, PG holds no affect row.
                let second_payload = semantic_payload(&handle, second_updated)
                    .with_subject_ids(vec![sa, sb])
                    .with_affects(vec![frustration]);
                let third_payload = semantic_payload(&handle, third_updated)
                    .with_subject_ids(vec![sa, sb])
                    .with_affects(vec![affect_annotation(
                        AffectKind::Mood,
                        EmotionLabel::Calm,
                        6_000,
                        -4_000,
                        8_200,
                    )]);
                let vector = semantic_vector(query);
                upsert(
                    transport.as_ref(),
                    &permit,
                    &collection,
                    &[
                        (PointId::Uuid(first_point), &first_payload, vector.clone()),
                        (PointId::Uuid(second_point), &second_payload, vector.clone()),
                        (PointId::Uuid(third_point), &third_payload, vector),
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
                assert_eq!(returned.len(), 3);
                assert_eq!(
                    returned_memory_ids(no_token),
                    BTreeSet::from([
                        first.memory_id.to_string(),
                        second.memory_id.to_string(),
                        third.memory_id.to_string(),
                    ])
                );

                // Card 18 / §23.1② live witness. Until this card every successful read on every
                // deployment answered `cannot_establish` / `index_count_unavailable`, because all
                // three read routes passed `visible: None` into `build_projection_block`. A
                // healthy projection must now state a real count and a real ratio.
                let projection = &no_token["pipeline"]["projection"];
                assert_eq!(projection["visible"], 3, "live Qdrant count: {no_token}");
                assert_eq!(
                    projection["completeness_ratio"], 1.0,
                    "A2 closed over a whole stream ⇒ ratio 1.0, never null: {no_token}"
                );
                assert_eq!(projection["current"], true, "{no_token}");
                assert_ne!(
                    no_token["completeness"]["reason"], "index_count_unavailable",
                    "the reason this card exists to remove — it was on EVERY successful read, \
                     healthy projection or not: {no_token}"
                );
                assert!(
                    no_token["completeness"]["degradations"]
                        .as_array()
                        .expect("degradations")
                        .is_empty(),
                    "nothing is lost here: {no_token}"
                );
                // Card 19 follow-up / ADR-0041 D-H: the other half. Card 18 made §23.1②'s
                // ratio real and this read still answered `cannot_establish/count_unknown`,
                // because no read route read `evidence.persisted` or the four `knowledge.*`
                // counts in `CountScope::StreamLedger`. `recall.search` now does, in the same
                // RR snapshot that closed its ledger and hydrated its bodies.
                //
                // §59 freezes the class vocabulary at four and §22.5 owns the degrade
                // direction; `classify()`'s frozen map sends this route's
                // `PlannerDecision::Class(_)` to `SemanticBounded` (never `Exact` — a
                // dense-recall answer has no enumerable universe, and §22.2/§22.3 never
                // defined a partial claim for it). So with A1/A2 closed, a live `visible`
                // count and a §23.3④ chain that closes, `semantic_bounded` with `reason =
                // null` is the ONE honest answer here — and §22.4 scopes `known_lower_bound`
                // to `cannot_establish`, so it stays null.
                assert_eq!(
                    no_token["completeness"]["class"], "semantic_bounded",
                    "§59/§22.5: a bounded semantic read with every reading present is \
                     `semantic_bounded`, not `cannot_establish`: {no_token}"
                );
                assert_eq!(
                    no_token["completeness"]["reason"],
                    Value::Null,
                    "§22.4 attaches a reason to `cannot_establish` alone: {no_token}"
                );
                assert_eq!(
                    no_token["completeness"]["exact"],
                    Value::Null,
                    "§22.0: only an `exact` class carries the enumeration block: {no_token}"
                );
                assert_eq!(
                    no_token["completeness"]["known_lower_bound"],
                    Value::Null,
                    "§22.4 scopes 'at least N' to cannot_establish: {no_token}"
                );
                // §23.3④: real readings in the declared universe, not `0`s standing in for
                // unknowns ("禁止填 `0`、返回条数、`issued_highwater` 或其他块的值充数") —
                // this fixture's ledger genuinely holds three settled rows.
                assert_eq!(
                    no_token["pipeline"]["evidence"]["count_scope"], "stream_ledger",
                    "{no_token}"
                );
                assert_eq!(
                    no_token["pipeline"]["knowledge"]["count_scope"], "stream_ledger",
                    "{no_token}"
                );
                assert_eq!(
                    no_token["pipeline"]["evidence"]["expected"],
                    Value::Null,
                    "§23.1①: no batch_id ⇒ expected stays null, never backfilled: {no_token}"
                );
                assert_eq!(no_token["pipeline"]["evidence"]["persisted"], 3, "{no_token}");
                assert_eq!(no_token["pipeline"]["knowledge"]["eligible"], 3, "{no_token}");
                assert_eq!(no_token["pipeline"]["knowledge"]["processed"], 3, "{no_token}");
                assert_eq!(no_token["pipeline"]["knowledge"]["waiting_key"], 0, "{no_token}");
                assert_eq!(no_token["pipeline"]["knowledge"]["failed"], 0, "{no_token}");
                assert_eq!(no_token["pipeline"]["projection"]["expected"], 3, "{no_token}");

                // A2 InvisibleLoss, forced: one more settled ledger row with no point behind it.
                // The ratio must MOVE and stay a number — "cannot_establish" would hide the loss
                // and a null ratio would be the pre-card-18 answer wearing a new reason.
                tokio::task::block_in_place(|| {
                    seed_semantic_projection_ledger_row(&mut handle, 4);
                });
                let (status, lossy) = recall_call(
                    address,
                    Some(&credential.bearer),
                    json!({"query":query,"workspace_id":workspace_id,"mode":"semantic"}),
                )
                .await;
                assert_eq!(status, 200, "invisible-loss recall: {lossy}");
                let lossy = assert_tool_response(&lossy, ToolName::Recall);
                let lossy_projection = &lossy["pipeline"]["projection"];
                assert_eq!(lossy_projection["visible"], 3, "{lossy}");
                assert_eq!(lossy_projection["done"], 4, "{lossy}");
                let ratio = lossy_projection["completeness_ratio"]
                    .as_f64()
                    .expect("an invisible loss is measured, never null");
                assert!((ratio - 0.75).abs() < 1e-9, "3/4, not null: {lossy}");
                assert_eq!(lossy_projection["current"], false, "{lossy}");
                assert_eq!(
                    lossy["completeness"]["degradations"],
                    json!(["PROJECTION_INVISIBLE_LOSS"]),
                    "{lossy}"
                );
                // Restore the fixture's healthy A2 for every leg below (which assert items, not
                // completeness): the extra ledger row is what made it lossy.
                tokio::task::block_in_place(|| {
                    drop_semantic_projection_ledger_row(&mut handle, 4);
                });

                // Card 18 / §23.1②: the OTHER two read routes. `recall.search` holds its own
                // Qdrant transport; `memory.get` / `memory.enumerate` / `context.assemble` are
                // PG-only and reach the index through `ContextBootstrap`'s borrowed count face
                // (`GatewayMcpApplication::with_semantic_recall`). Without a witness here,
                // deleting that attach — or the `Some(serving_version)` argument on either route
                // — leaves every other test in this file green, because every other fixture
                // builds the app WITHOUT a semantic runtime and so legitimately reports `None`.
                // This is the only fixture in the suite where the face exists at all.
                //
                // Card 19 follow-up / ADR-0041 D-H rides along: each route's final class is
                // pinned here too, because the three are now genuinely different and each one
                // is mandated rather than tolerated. `memory.enumerate` is `exact` (§22.0, a
                // registered predicate over a counted universe); `memory.get` stays
                // `cannot_establish/count_unknown` BY CONSTRUCTION (ADR-0041 D-D: `classify()`
                // maps DirectGet to `Exact`, §22.0 makes `exact` without a `predicate_id` a
                // hard 5xx, so its unknown counts are the only legal answer for an object
                // read) — whoever "finishes the job" by filling memory.get's five counts turns
                // this row red instead of shipping a 500.
                //
                // `context.assemble` used to be the route that could not reach
                // `semantic_bounded`, and the blocker was never its pipeline half: §22.4's lane
                // trigger is checked inside `classify()` BEFORE `planner_output` is read at
                // all, and this route's mandatory lane was `failed` on EVERY deployment because
                // `context_repo` emitted `SelectorOutcome::Unavailable` for two of §25's five
                // selectors. Card 22 corrected ADR-0041 D-I's recorded cause (it was NOT "no
                // WHERE clause" — the probe answers before the dispatch, so that arm was
                // unreachable code), and card 22b (ADR-0045) closed it: migration 0172 added
                // `private.memory_records.facet` as a STORED generated column and the task
                // selector's dependency moved to `private.context_bindings`, so all five
                // selectors run. This row therefore no longer pins a class literal — see the
                // tuple below and the `unavailable_selectors == []` probe after the loop.
                for (route, tool, arguments, class, reason) in [
                    (
                        "memory.get",
                        "memory",
                        json!({"action":"get","memory_id":first.memory_id,"workspace_id":workspace_id}),
                        "cannot_establish",
                        json!("count_unknown"),
                    ),
                    (
                        "memory.enumerate",
                        "memory",
                        json!({"action":"enumerate","workspace_id":workspace_id,"limit":100}),
                        "exact",
                        Value::Null,
                    ),
                    (
                        // Card 22b (ADR-0045): this route's class is no longer pinned to a
                        // literal here. §22.4's lane trigger used to decide it on EVERY
                        // deployment — two selectors were column-unavailable, so the answer was
                        // always cannot_establish/lane_failed regardless of anything else this
                        // fixture set up. With all five selectors running, the class is decided
                        // by the planner/pipeline legs this same fixture pins row by row above,
                        // and pinning a second literal here would just re-assert those.
                        // The empty `class` selects the "must not be lane_failed" branch below,
                        // which is the actual card-22b acceptance.
                        "context.assemble",
                        "context",
                        json!({"workspace_id":workspace_id}),
                        "",
                        Value::Null,
                    ),
                ] {
                    let (status, response) =
                        tool_call(address, tool, &credential.bearer, arguments).await;
                    assert_eq!(status, 200, "{route}: {response}");
                    let value = assert_tool_response(
                        &response,
                        if tool == "memory" {
                            ToolName::Memory
                        } else {
                            ToolName::Context
                        },
                    );
                    // `memory.get` returns the `Envelope` at the top level; `memory.enumerate`
                    // wraps it under `content` (next to `pagination`) and `context.assemble`
                    // under `content` (next to `handoff`). Pick the object that actually carries
                    // the envelope rather than hard-coding three shapes.
                    let value = if value["pipeline"].is_null() {
                        &value["content"]
                    } else {
                        value
                    };
                    assert!(
                        !value["pipeline"].is_null(),
                        "{route}: no envelope found in the response shape"
                    );
                    let projection = &value["pipeline"]["projection"];
                    assert_eq!(
                        projection["visible"], 3,
                        "{route} must report the SAME live Qdrant count recall.search does —                          it reads the same stream on the same serving version: {value}"
                    );
                    assert_eq!(
                        projection["completeness_ratio"], 1.0,
                        "{route} reported a null ratio on a healthy projection before card 18:                          {value}"
                    );
                    assert_eq!(projection["current"], true, "{route}: {value}");
                    assert_ne!(
                        value["completeness"]["reason"], "index_count_unavailable",
                        "{route}: {value}"
                    );
                    if class.is_empty() {
                        // Card 22b acceptance: whatever the class is, it must not be the
                        // structural lane failure this card removed.
                        assert_ne!(
                            value["completeness"]["reason"], "lane_failed",
                            "{route}: §25's five selectors all run since migration 0172 + \
                             ADR-0045; a lane_failed here means a selector went Unavailable \
                             again: {value}"
                        );
                    } else {
                        assert_eq!(
                            value["completeness"]["class"], class,
                            "{route} must answer {class}: {value}"
                        );
                        assert_eq!(
                            value["completeness"]["reason"], reason,
                            "{route}: {value}"
                        );
                    }
                    // The two routes that moved carry real `stream_ledger` readings of this
                    // fixture's three settled rows; `memory.get` carries none, on purpose.
                    if route != "memory.get" {
                        assert_ne!(
                            value["completeness"]["reason"], "count_unknown",
                            "{route}: ADR-0041 D-H removed the missing-count reason from the \
                             two routes that can have counts: {value}"
                        );
                    }
                    let expect_counts = route != "memory.get";
                    assert_eq!(
                        value["pipeline"]["evidence"]["persisted"],
                        if expect_counts { json!(3) } else { Value::Null },
                        "{route}: {value}"
                    );
                    assert_eq!(
                        value["pipeline"]["knowledge"]["eligible"],
                        if expect_counts { json!(3) } else { Value::Null },
                        "{route}: {value}"
                    );
                    assert_eq!(
                        value["pipeline"]["knowledge"]["processed"],
                        if expect_counts { json!(3) } else { Value::Null },
                        "{route}: {value}"
                    );
                    assert_eq!(
                        value["pipeline"]["evidence"]["count_scope"],
                        if expect_counts { "stream_ledger" } else { "authorized_view" },
                        "§23.3④ pins the scope label to the numbers: {route}: {value}"
                    );
                    assert_eq!(
                        value["pipeline"]["knowledge"]["count_scope"],
                        if expect_counts { "stream_ledger" } else { "authorized_view" },
                        "{route}: {value}"
                    );
                }

                // Card 22 pinned the CAUSE, not just the symptom; card 22b (ADR-0045) closed
                // the cause and this block is the readback of that, in the same place.
                //
                // Before: `handoff.unavailable_selectors` named the two §25 selectors whose
                // `required_columns` did not exist, and §22.4's lane trigger therefore answered
                // `lane_failed` on EVERY deployment. Migration 0172 landed
                // `private.memory_records.facet` (a STORED generated column) and the registry's
                // task dependency moved to `private.context_bindings`, so all five selectors
                // run. Asserting the EMPTY list — not "not the old two" — is what keeps this
                // honest: a selector that goes unavailable for any new reason turns it red and
                // names the object it could not find.
                {
                    let (status, response) = tool_call(
                        address,
                        "context",
                        &credential.bearer,
                        json!({"workspace_id":workspace_id}),
                    )
                    .await;
                    assert_eq!(status, 200, "context.assemble cause probe: {response}");
                    let value = assert_tool_response(&response, ToolName::Context);
                    assert_eq!(
                        value["handoff"]["unavailable_selectors"],
                        json!([]),
                        "all five §25 selectors must be column-available since 0172 + ADR-0045; \
                         a non-empty list names the probed object that is missing — fix that \
                         object, do not loosen this: {value}"
                    );
                    assert_ne!(
                        value["content"]["completeness"]["reason"], "lane_failed",
                        "the lane no longer fails structurally: {value}"
                    );
                }

                // ADR-0041 D-I speed record: what the two `projection.stream_log`
                // aggregates cost these two routes end to end. Same fixture, same three
                // settled rows; run once with the wiring and once with the counts stubbed
                // back to `None` for the before/after pair.
                let mut recall_samples = Vec::new();
                let mut context_samples = Vec::new();
                for _ in 0..30 {
                    let start = std::time::Instant::now();
                    let (status, timed) = recall_call(
                        address,
                        Some(&credential.bearer),
                        json!({"query":query,"workspace_id":workspace_id,"mode":"semantic"}),
                    )
                    .await;
                    recall_samples.push(start.elapsed());
                    assert_eq!(status, 200, "timed recall: {timed}");
                    let start = std::time::Instant::now();
                    let (status, timed) = tool_call(
                        address,
                        "context",
                        &credential.bearer,
                        json!({"workspace_id":workspace_id}),
                    )
                    .await;
                    context_samples.push(start.elapsed());
                    assert_eq!(status, 200, "timed context.assemble: {timed}");
                }
                recall_samples.sort_unstable();
                context_samples.sort_unstable();
                eprintln!(
                    "ADR-0041 D-I p50/p95 (n=30, ms): recall.search {}/{} · context.assemble \
                     {}/{}",
                    recall_samples[14].as_millis(),
                    recall_samples[28].as_millis(),
                    context_samples[14].as_millis(),
                    context_samples[28].as_millis()
                );

                // ------------------------------------------------------------------
                // Card 19 / ADR-0041: the §22.1 EXACT census, live.
                //
                // This is the ONLY fixture in the suite where every input `class = exact`
                // needs exists at once: a real Qdrant `visible` count (card 18 — without it
                // §23.1②'s ratio is null and the envelope is correctly
                // `cannot_establish/index_count_unavailable` whatever the census says) AND a
                // stream ledger whose §23.3④ evidence/knowledge counts close. Every
                // assertion below was `cannot_establish` / `null` before this card.
                // ------------------------------------------------------------------
                // The whole `structuredContent` (`{content, pagination}`), schema-validated:
                // the census assertions read `content`, the cursor legs read `pagination`.
                let enumerate_result =
                    |page: &Value| -> Value { assert_tool_response(page, ToolName::Memory).clone() };
                let (status, whole) = enumerate_call(
                    address,
                    &credential.bearer,
                    json!({"action":"enumerate","workspace_id":workspace_id,"limit":100}),
                )
                .await;
                assert_eq!(status, 200, "unpaginated authorized enumeration: {whole}");
                let whole = enumerate_result(&whole)["content"].clone();
                let completeness = &whole["completeness"];
                assert_eq!(
                    completeness["class"], "exact",
                    "§22.1: a registered predicate, a real count(*) in the page's own snapshot, \
                     a closed ledger and a live index count leave exactly one honest class: \
                     {whole}"
                );
                assert_eq!(completeness["reason"], Value::Null, "{whole}");
                let returned = completeness["returned"].as_u64().expect("returned");
                assert_eq!(returned, 3, "the fixture's three active memories: {whole}");
                let exact = &completeness["exact"];
                assert_eq!(
                    exact["predicate_id"], "authorized_memory_enumeration_v1",
                    "§22.0: the wire block names the predicate its denominator came from: \
                     {whole}"
                );
                assert_eq!(
                    exact["total"], 3,
                    "total is its own count(*), not the page length: {whole}"
                );
                assert_eq!(exact["returned"], 3, "{whole}");
                assert_eq!(exact["coverage"], 1.0, "unpaginated ⇒ coverage 1.0: {whole}");
                assert_eq!(exact["truncated"], false, "{whole}");
                assert_eq!(exact["excluded_secret"], 0, "{whole}");
                assert_eq!(
                    completeness["known_lower_bound"], Value::Null,
                    "§22.4 scopes the lower bound to cannot_establish — an exact answer does \
                     not also publish 'at least N': {whole}"
                );
                // §23.3④: the two pipeline blocks are real `stream_ledger` readings now, and
                // the chain they must satisfy is `evidence.persisted == knowledge.eligible ==
                // projection.expected` with `processed + waiting_key + failed == eligible`.
                let pipeline = &whole["pipeline"];
                assert_eq!(pipeline["evidence"]["count_scope"], "stream_ledger", "{whole}");
                assert_eq!(
                    pipeline["knowledge"]["count_scope"], "stream_ledger",
                    "{whole}"
                );
                assert_eq!(
                    pipeline["evidence"]["expected"], Value::Null,
                    "§23.1①: no batch_id ⇒ expected stays null, never backfilled from \
                     persisted: {whole}"
                );
                assert_eq!(pipeline["evidence"]["persisted"], 3, "{whole}");
                assert_eq!(pipeline["knowledge"]["eligible"], 3, "{whole}");
                assert_eq!(pipeline["knowledge"]["processed"], 3, "{whole}");
                assert_eq!(pipeline["knowledge"]["waiting_key"], 0, "{whole}");
                assert_eq!(pipeline["knowledge"]["failed"], 0, "{whole}");
                assert_eq!(pipeline["projection"]["expected"], 3, "{whole}");

                // §23.1④ negative control: the count and the page must come out of ONE
                // snapshot. A memory inserted between page 1 and page 2 must NOT move the
                // frozen manifest's total — if the census were re-counted per page (or taken
                // outside the minting transaction) this insert would raise it to 4 and the
                // ratio would drift across pages of one immutable manifest.
                let (status, first_page) = enumerate_call(
                    address,
                    &credential.bearer,
                    json!({"action":"enumerate","workspace_id":workspace_id,"limit":2}),
                )
                .await;
                assert_eq!(status, 200, "page 1 of 2: {first_page}");
                let first_page = enumerate_result(&first_page);
                assert_eq!(
                    first_page["content"]["completeness"]["class"], "exact",
                    "{first_page}"
                );
                assert_eq!(
                    first_page["content"]["completeness"]["exact"]["total"], 3,
                    "{first_page}"
                );
                assert_eq!(
                    first_page["content"]["completeness"]["exact"]["returned"], 2,
                    "{first_page}"
                );
                assert_eq!(
                    first_page["content"]["completeness"]["exact"]["truncated"], true,
                    "§22.1: 2 of 3 returned with nothing excluded IS truncated: {first_page}"
                );
                let page_cursor = first_page["pagination"]["next_cursor"]
                    .as_str()
                    .expect("a 2-of-3 page continues")
                    .to_owned();
                let concurrent = tokio::task::block_in_place(|| {
                    handle.seed_workspace_visible_context_record()
                });
                let (status, second_page) = enumerate_call(
                    address,
                    &credential.bearer,
                    json!({"action":"enumerate","workspace_id":workspace_id,"limit":2,
                           "cursor":page_cursor}),
                )
                .await;
                assert_eq!(status, 200, "frozen continuation: {second_page}");
                let second_page = enumerate_result(&second_page)["content"].clone();
                assert_eq!(second_page["completeness"]["class"], "exact", "{second_page}");
                assert_eq!(
                    second_page["completeness"]["exact"]["total"], 3,
                    "§22.1/§23.1④: the manifest's denominator was frozen with the manifest — a \
                     concurrent insert does not move it: {second_page}"
                );
                assert_eq!(
                    second_page["completeness"]["exact"]["returned"], 1,
                    "{second_page}"
                );
                // A fresh manifest, by contrast, MUST see the insert — otherwise the frozen
                // total above would be proving staleness rather than snapshot discipline.
                let (status, fresh) = enumerate_call(
                    address,
                    &credential.bearer,
                    json!({"action":"enumerate","workspace_id":workspace_id,"limit":100}),
                )
                .await;
                assert_eq!(status, 200, "fresh manifest: {fresh}");
                let fresh = enumerate_result(&fresh)["content"].clone();
                assert_eq!(
                    fresh["completeness"]["exact"]["total"], 4,
                    "a NEW snapshot counts the concurrent insert: {fresh}"
                );

                // §22.4 trigger 4, injected: a manifest whose frozen readout is gone (a
                // pre-0165 snapshot, or a mint whose census failed) degrades to
                // cannot_establish/census_failed and publishes NO total — never a silent 0.
                let (status, degrade_first) = enumerate_call(
                    address,
                    &credential.bearer,
                    json!({"action":"enumerate","workspace_id":workspace_id,"limit":2}),
                )
                .await;
                assert_eq!(status, 200, "census-failure page 1: {degrade_first}");
                let degrade_first = enumerate_result(&degrade_first);
                let degrade_cursor = degrade_first["pagination"]["next_cursor"]
                    .as_str()
                    .expect("continuation for the census-failure leg")
                    .to_owned();
                let degrade_snapshot = Uuid::parse_str(
                    degrade_first["pagination"]["snapshot_id"]
                        .as_str()
                        .expect("snapshot id"),
                )
                .expect("snapshot uuid");
                tokio::task::block_in_place(|| {
                    handle
                        .admin
                        .execute(
                            "UPDATE ops.selection_snapshots SET census_predicate_id=NULL, \
                               census_total=NULL, census_excluded_secret=NULL \
                             WHERE selection_snapshot_id=$1",
                            &[&degrade_snapshot],
                        )
                        .expect("owner drops this manifest's frozen census readout");
                });
                let (status, censusless) = enumerate_call(
                    address,
                    &credential.bearer,
                    json!({"action":"enumerate","workspace_id":workspace_id,"limit":2,
                           "cursor":degrade_cursor}),
                )
                .await;
                assert_eq!(status, 200, "censusless continuation: {censusless}");
                let censusless = enumerate_result(&censusless)["content"].clone();
                assert_eq!(
                    censusless["completeness"]["class"], "cannot_establish",
                    "{censusless}"
                );
                assert_eq!(
                    censusless["completeness"]["reason"], "census_failed",
                    "{censusless}"
                );
                assert_eq!(
                    censusless["completeness"]["exact"], Value::Null,
                    "§22.1: no census ⇒ no total, not a total of 0: {censusless}"
                );
                assert_eq!(
                    censusless["completeness"]["known_lower_bound"], Value::Null,
                    "nothing was counted, so there is no proven lower bound either: \
                     {censusless}"
                );

                // §23.3④ pipeline fault, injected — the census debt card 18 folded in here,
                // now measurable in both directions. ONE more issued ticket that has not
                // settled yet (`ISSUED`, seq 4, watermark 4) keeps §23.1②'s A1 closed
                // (`done + open_gaps + pending == expected`) and keeps A2 closed, so the
                // projection block still states a ratio — but the knowledge partition no
                // longer covers its own base (`processed + waiting_key + failed = 3 != 4 =
                // eligible`), which is precisely the reading §23.3④ says must make the whole
                // envelope cannot_establish. Work in flight is not "processed", and reporting
                // it as such is the "填数充数" that section forbids.
                tokio::task::block_in_place(|| {
                    let commit_seq: i64 = handle
                        .admin
                        .query_one("SELECT nextval('ops.commit_seq_seq')", &[])
                        .expect("owner allocates the in-flight commit sequence")
                        .get(0);
                    handle
                        .admin
                        .execute(
                            "INSERT INTO projection.stream_log \
                               (tenant_id,scope_kind,scope_id,domain,projection_kind, \
                                projection_version,stream_seq,commit_seq,state) \
                             VALUES($1,'workspace',$2,'knowledge','ingest','v1',4,$3,'ISSUED')",
                            &[&handle.tenant_id, &handle.workspace_id, &commit_seq],
                        )
                        .expect("owner issues one not-yet-settled ticket");
                    handle
                        .admin
                        .execute(
                            "UPDATE projection.stream_checkpoints SET issued_highwater=4 \
                             WHERE tenant_id=$1 AND scope_kind='workspace' AND scope_id=$2 \
                               AND domain='knowledge' AND projection_kind='ingest' \
                               AND projection_version='v1'",
                            &[&handle.tenant_id, &handle.workspace_id],
                        )
                        .expect("owner admits the in-flight ticket");
                });
                let (status, inflight) = enumerate_call(
                    address,
                    &credential.bearer,
                    json!({"action":"enumerate","workspace_id":workspace_id,"limit":100}),
                )
                .await;
                assert_eq!(status, 200, "in-flight pipeline: {inflight}");
                let inflight = enumerate_result(&inflight)["content"].clone();
                assert_eq!(
                    inflight["pipeline"]["projection"]["completeness_ratio"], 0.75,
                    "A1/A2 still hold — this fault is in the knowledge layer, not §23.1②: \
                     {inflight}"
                );
                assert_eq!(
                    inflight["pipeline"]["knowledge"]["eligible"], 4,
                    "{inflight}"
                );
                assert_eq!(
                    inflight["pipeline"]["knowledge"]["processed"], 3,
                    "{inflight}"
                );
                assert_eq!(
                    inflight["completeness"]["class"], "cannot_establish",
                    "stop establishing the census and the class goes back where card 18 left \
                     it: {inflight}"
                );
                assert_eq!(
                    inflight["completeness"]["reason"], "pipeline_count_mismatch",
                    "§23.3④'s own reason for a known-but-contradictory chain: {inflight}"
                );
                assert_eq!(inflight["completeness"]["exact"], Value::Null, "{inflight}");
                assert_eq!(
                    inflight["completeness"]["known_lower_bound"], 4,
                    "§22.4: the census counted, a later trigger blocked the class — what \
                     survives is 'at least N', never a silent 0: {inflight}"
                );
                // The SAME fault on the two routes ADR-0041 D-H moved. For `recall.search`
                // this is the inversion of the whole wiring: the counts it now reads are the
                // ONLY reason it left `cannot_establish`, so a ledger whose knowledge
                // partition stops covering its own base must put it straight back — with
                // §23.3④'s own reason (`pipeline_count_mismatch`: the counts are known here
                // and they disagree, which is a different verdict from `count_unknown`) and
                // never with a `semantic_bounded` that outlives its evidence.
                //
                // Card 22b (ADR-0045): `context.assemble` is no longer held at `lane_failed`
                // by §22.4's lane trigger (all five selectors run), so `classify()` reaches the
                // pipeline leg and this fault now moves its class exactly like recall's —
                // §23.3④'s `pipeline_count_mismatch`. Before card 22b the class was pinned to
                // `lane_failed` here and was insensitive to the fault by construction; that
                // insensitivity was the structural defect, not a guarantee. The two blocks
                // must move as well — a route that kept reporting `eligible = 3` here would be
                // publishing a reading it did not take.
                for (route, tool, class, reason, arguments) in [
                    ("recall.search", "recall", "cannot_establish", "pipeline_count_mismatch",
                     json!({"query":query,"workspace_id":workspace_id,"mode":"semantic"})),
                    ("context.assemble", "context", "cannot_establish", "pipeline_count_mismatch",
                     json!({"workspace_id":workspace_id})),
                ] {
                    let (status, faulted) =
                        tool_call(address, tool, &credential.bearer, arguments).await;
                    assert_eq!(status, 200, "{route} under the in-flight fault: {faulted}");
                    let faulted = assert_tool_response(
                        &faulted,
                        if tool == "recall" { ToolName::Recall } else { ToolName::Context },
                    );
                    let faulted = if faulted["pipeline"].is_null() {
                        &faulted["content"]
                    } else {
                        faulted
                    };
                    assert_eq!(
                        faulted["pipeline"]["projection"]["completeness_ratio"], 0.75,
                        "A1/A2 still hold — the fault is in the knowledge layer: {faulted}"
                    );
                    assert_eq!(faulted["pipeline"]["evidence"]["persisted"], 4, "{faulted}");
                    assert_eq!(faulted["pipeline"]["knowledge"]["eligible"], 4, "{faulted}");
                    assert_eq!(faulted["pipeline"]["knowledge"]["processed"], 3, "{faulted}");
                    assert_eq!(
                        faulted["completeness"]["class"], class,
                        "{route}: {faulted}"
                    );
                    assert_eq!(
                        faulted["completeness"]["reason"], reason,
                        "{route}: {faulted}"
                    );
                    assert_eq!(
                        faulted["completeness"]["known_lower_bound"],
                        Value::Null,
                        "{route} enumerates nothing, so it proves no lower bound either: \
                         {faulted}"
                    );
                }

                // Hand the fixture back exactly as the legs below expect it: the in-flight
                // ticket gone, the watermark back on its three settled rows, the
                // concurrent-insert control row out of the authorized universe.
                tokio::task::block_in_place(|| {
                    handle
                        .admin
                        .execute(
                            "DELETE FROM projection.stream_log \
                              WHERE tenant_id=$1 AND scope_kind='workspace' AND scope_id=$2 \
                                AND domain='knowledge' AND projection_kind='ingest' \
                                AND projection_version='v1' AND stream_seq=4",
                            &[&handle.tenant_id, &handle.workspace_id],
                        )
                        .expect("owner withdraws the in-flight ticket");
                    handle
                        .admin
                        .execute(
                            "UPDATE projection.stream_checkpoints SET issued_highwater=3 \
                             WHERE tenant_id=$1 AND scope_kind='workspace' AND scope_id=$2 \
                               AND domain='knowledge' AND projection_kind='ingest' \
                               AND projection_version='v1'",
                            &[&handle.tenant_id, &handle.workspace_id],
                        )
                        .expect("owner restores the fixture watermark");
                    handle
                        .admin
                        .execute(
                            "UPDATE private.memory_records SET status='revoked' \
                             WHERE memory_id=$1",
                            &[&concurrent.memory_id],
                        )
                        .expect("owner removes the concurrent-insert control row");
                });

                // §6.1.3/ADR-0029 D-C: subject-scoped recall through the real Gateway + real
                // Qdrant prefilter + real PG hydrate re-check. `second` carries A in its Qdrant
                // payload but has no PG link to A, so it passes the prefilter and must be dropped
                // by `final_memory_ids_about_in_txn` — Qdrant is a prefilter, never the authority.
                for (subjects, expected, label) in [
                    (vec![subject_a], vec![&first, &third], "A: A-only + A+B, never B-only"),
                    (vec![subject_b], vec![&second, &third], "B: B-only + A+B, never A-only"),
                    (vec![subject_a, subject_b], vec![&first, &second, &third], "A or B: all"),
                ] {
                    let (status, scoped) = recall_call(
                        address,
                        Some(&credential.bearer),
                        json!({
                            "query":query,
                            "workspace_id":workspace_id,
                            "mode":"semantic",
                            "subject_ids":subjects,
                        }),
                    )
                    .await;
                    assert_eq!(status, 200, "subject-scoped recall {label}: {scoped}");
                    let scoped = assert_tool_response(&scoped, ToolName::Recall);
                    assert_eq!(
                        returned_memory_ids(scoped),
                        expected
                            .iter()
                            .map(|r| r.memory_id.to_string())
                            .collect::<BTreeSet<_>>(),
                        "{label}: {scoped}"
                    );
                }
                let (status, unknown_subject) = recall_call(
                    address,
                    Some(&credential.bearer),
                    json!({
                        "query":query,
                        "workspace_id":workspace_id,
                        "mode":"semantic",
                        "subject_ids":[Uuid::new_v4()],
                    }),
                )
                .await;
                assert_eq!(status, 200, "unknown subject: {unknown_subject}");
                assert!(
                    assert_tool_response(&unknown_subject, ToolName::Recall)["items"]
                        .as_array()
                        .expect("items")
                        .is_empty(),
                    "a subject nothing is about yields zero rows, never the unscoped set"
                );
                let (status, malformed) = recall_call(
                    address,
                    Some(&credential.bearer),
                    json!({
                        "query":query,
                        "workspace_id":workspace_id,
                        "mode":"semantic",
                        "subject_ids":["not-a-uuid"],
                    }),
                )
                .await;
                assert_eq!(status, 400, "malformed subject id is INVALID_INPUT: {malformed}");

                // §8.5.1/ADR-0030 D-D: affect-filtered recall through the real Gateway + real
                // Qdrant flat-array prefilter + real PG re-check per annotation on the read-time
                // effective intensity. `second` passes every Qdrant prefilter it claims and is
                // dropped by `final_memory_ids_about_in_txn` (no PG row); `third`'s MOOD passes
                // the raw-intensity prefilter (8200) and is dropped by the effective test (2050).
                let affect_recall = |affect: Value| {
                    recall_call(
                        address,
                        Some(&credential.bearer),
                        json!({
                            "query":query,
                            "workspace_id":workspace_id,
                            "mode":"semantic",
                            "affect":affect,
                        }),
                    )
                };
                for (affect, expected, label) in [
                    (json!({"labels_any":["FRUSTRATION"]}), vec![&first], "label any-of: tampered second dropped"),
                    (json!({"valence":[-10000,-1]}), vec![&first], "negative valence interval"),
                    (json!({"kinds":["MOOD"]}), vec![&third], "kind MOOD"),
                    (json!({"min_effective_intensity":5000}), vec![&first], "min effective: decayed mood out, emotion in"),
                    (json!({"labels_any":["JOY"]}), vec![], "a label nothing carries"),
                    (json!({"kinds":["EMOTION"],"labels_any":["FRUSTRATION"],"arousal":[0,10000]}), vec![&first], "ANDed clauses"),
                ] {
                    let (status, scoped) = affect_recall(affect.clone()).await;
                    assert_eq!(status, 200, "affect recall {label}: {scoped}");
                    let scoped = assert_tool_response(&scoped, ToolName::Recall);
                    assert_eq!(
                        returned_memory_ids(scoped),
                        expected
                            .iter()
                            .map(|r| r.memory_id.to_string())
                            .collect::<BTreeSet<_>>(),
                        "{label} ({affect}): {scoped}"
                    );
                }
                let (status, inverted) = affect_recall(json!({"valence":[1,-1]})).await;
                assert_eq!(status, 400, "lo > hi is INVALID_INPUT: {inverted}");
                // Mood-congruent late rerank: a permutation of the visible set — all three still
                // return, the closest annotation comes first, `reranked_count` names the count.
                for ((valence, arousal), expected_first, label) in [
                    ((-8_000, 5_000), &first, "frustrated reader → the FRUSTRATION memory first"),
                    ((6_000, -4_000), &third, "calm reader → the CALM memory first"),
                ] {
                    let (status, ranked) = recall_call(
                        address,
                        Some(&credential.bearer),
                        json!({
                            "query":query,
                            "workspace_id":workspace_id,
                            "mode":"semantic",
                            "mood_congruence":{"valence":valence,"arousal":arousal},
                        }),
                    )
                    .await;
                    assert_eq!(status, 200, "mood recall {label}: {ranked}");
                    let ranked = assert_tool_response(&ranked, ToolName::Recall);
                    let items = ranked["items"].as_array().expect("items");
                    assert_eq!(items.len(), 3, "{label}: rerank never narrows: {ranked}");
                    assert_eq!(items[0]["memory_id"], expected_first.memory_id.to_string(), "{label}: {ranked}");
                    assert_eq!(ranked["completeness"]["reranked_count"], 3, "{label}: {ranked}");
                }
                // Card E1 speed goal: recall p50 with / without the affect filter (numbers only;
                // card 24 sets the baseline).
                let mut plain = Vec::new();
                let mut filtered = Vec::new();
                for _ in 0..5 {
                    let started = Instant::now();
                    let (status, _) = recall_call(
                        address,
                        Some(&credential.bearer),
                        json!({"query":query,"workspace_id":workspace_id,"mode":"semantic"}),
                    )
                    .await;
                    assert_eq!(status, 200);
                    plain.push(started.elapsed());
                    let started = Instant::now();
                    let (status, _) = affect_recall(json!({"labels_any":["FRUSTRATION"]})).await;
                    assert_eq!(status, 200);
                    filtered.push(started.elapsed());
                }
                plain.sort();
                filtered.sort();
                eprintln!(
                    "recall p50 without affect filter = {} ms; with affect filter = {} ms (n=5 each)",
                    plain[2].as_millis(),
                    filtered[2].as_millis()
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
                assert_eq!(with_token["items"].as_array().expect("RYW items").len(), 4);
                assert!(with_token["items"].as_array().unwrap().iter().any(|item| {
                    item["kind"] == "temporary_evidence" && item["evidence_id"] == evidence_id
                }));

                // §6.1.3/ADR-0029 D-A, overlay leg (placed BEFORE the supersede/restore pair below:
                // a token minted after two lifecycle tickets bound to one Evidence trips the
                // harness's ConflictingOverlayEvidence, the worker-less limitation the card names).
                // The subject scope also governs what the RYW
                // overlay may carry. A just-written, not-yet-projected Evidence declared about B
                // rides in on a B-scoped recall as `temporary_evidence` and must NOT ride in on
                // an A-scoped one — nor may the earlier undeclared write (`evidence_id`, about
                // nobody) appear under either scope. Without the `evidence_subjects` re-check in
                // `read_materialize::load_overlay` the A-scoped call returned B's raw body.
                let (status, about_b) = remember_with_subjects(
                    address,
                    &credential.bearer,
                    3,
                    workspace_id,
                    "semantic write about B awaiting projection",
                    vec![subject_b],
                    Vec::new(),
                )
                .await;
                assert_eq!(status, 200, "remember about B: {about_b}");
                let about_b_token = about_b["result"]["structuredContent"]["consistency_token"]
                    .as_str()
                    .expect("about-B consistency token")
                    .to_owned();
                let about_b_evidence = about_b["result"]["structuredContent"]["evidence_id"]
                    .as_str()
                    .expect("about-B evidence id")
                    .to_owned();
                for (subjects, memories, overlay, label) in [
                    (
                        vec![subject_a],
                        vec![&first, &third],
                        BTreeSet::new(),
                        "A-scoped RYW: no overlay Evidence about B or about nobody",
                    ),
                    (
                        vec![subject_b],
                        vec![&second, &third],
                        BTreeSet::from([about_b_evidence.clone()]),
                        "B-scoped RYW: exactly the Evidence declared about B",
                    ),
                ] {
                    let (status, scoped) = recall_call(
                        address,
                        Some(&credential.bearer),
                        json!({
                            "query":query,
                            "workspace_id":workspace_id,
                            "mode":"semantic",
                            "subject_ids":subjects,
                            "consistency_token":about_b_token.clone(),
                        }),
                    )
                    .await;
                    assert_eq!(status, 200, "{label}: {scoped}");
                    let scoped = assert_tool_response(&scoped, ToolName::Recall);
                    let items = scoped["items"].as_array().expect("scoped RYW items");
                    let by_kind = |kind: &str, field: &str| -> BTreeSet<String> {
                        items
                            .iter()
                            .filter(|item| item["kind"] == kind)
                            .map(|item| item[field].as_str().expect(field).to_owned())
                            .collect()
                    };
                    assert_eq!(
                        by_kind("memory", "memory_id"),
                        memories
                            .iter()
                            .map(|r| r.memory_id.to_string())
                            .collect::<BTreeSet<_>>(),
                        "{label}: {scoped}"
                    );
                    assert_eq!(
                        by_kind("temporary_evidence", "evidence_id"),
                        overlay,
                        "{label}: {scoped}"
                    );
                    assert_eq!(items.len(), memories.len() + overlay.len(), "{label}: {scoped}");
                }

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
                    4,
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
                             WHERE tenant_id=$1 AND memory_id=ANY($2)",
                            &[&tenant_id, &vec![second.memory_id, third.memory_id]],
                        )
                        .expect("revoke authoritative semantic sources");
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

/// §15.5 through the MCP surface, in the exact request shape `cargo xtask soak`'s post-drain
/// replay sends (ADR-0038): a recall carrying a `consistency_token` the same run minted must
/// **answer** — a body with items, never `DEPENDENCY_UNAVAILABLE` — and the write that token
/// names must ride in as its overlay item.
///
/// The second and third legs pin the defect card 16's soak actually hit, which is why this is a
/// test of its own rather than one more assertion inside the acceptance test above. That replay
/// also sent `"limit": <live point count>`; §55.1 reserves candidate depth to the registered
/// profile, so the only value a caller may send is that profile's own `top_k` echoed back, and
/// the gateway refuses anything else **before** the embedding step. It refused silently, and
/// that is how a `limit` mismatch was read for a day as an embedding fault two steps further
/// down `recall::search`. So: the soak's shape answers, the shape it used to send is a clean
/// `INVALID_INPUT`, and the profile's own `top_k` is still accepted (the check is an equality,
/// not a blanket refusal of the field the schema advertises).
///
/// The accepted `top_k` is read off the first response's `provenance.profile.top_k` rather than
/// written as a literal — §78.1: a test that hard-codes the profile depth stops grading the
/// profile the moment it moves.
#[test]
#[ignore = "lane(a:request_guard) requires the isolated request-guard PostgreSQL fixture, pinned scanner and disposable Qdrant"]
#[allow(clippy::too_many_lines)] // One real-deployment fixture; splitting it would hide the causal chain.
fn recall_with_a_consistency_token_answers_and_a_caller_chosen_limit_is_refused() {
    run_db_fixture::<Fixture, _>(
        "recall_with_a_consistency_token_answers_and_a_caller_chosen_limit_is_refused",
        |mut handle| {
            handle.assert_gateway_login();
            let _registry_cleanup = SemanticProjectionCleanup {
                owner: handle.owner_client().expect("semantic cleanup owner"),
                tenant_id: handle.tenant_id,
            };
            let prefix = format!("ryw{}", &Uuid::now_v7().simple().to_string()[..12]);
            let wire = format!("{prefix}.{}", "r".repeat(32));
            let credential = handle.seed_synthetic_service_credential_and_window(
                SyntheticCredentialScopes::RememberWriteAndContextRead,
                &prefix,
                &wire,
                &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
                48,
            );
            let seeded = handle.seed_workspace_visible_context_record();
            let point = Uuid::new_v4();
            let updated = seed_semantic_registry_row(&mut handle, &seeded, point);
            seed_semantic_checkpoint(&mut handle);

            let cell = CellId(Uuid::now_v7());
            let registry = semantic_qdrant_registry(cell, CallerId("gateway-ryw-limit".to_owned()));
            let transport = Arc::new(
                HttpIntraCellTransport::new(
                    registry.clone(),
                    Duration::from_secs(10),
                    humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES,
                )
                .expect("semantic Qdrant transport"),
            );
            let collection = format!("gateway_ryw_limit_{}", Uuid::now_v7().simple());
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
                                &std::env::var("HUMAUX_GATEWAY_PG_DSN")
                                    .expect("fixture requires HUMAUX_GATEWAY_PG_DSN"),
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
                embedding_port,
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
            runtime_handle.block_on(async {
                create_semantic_collection(&transport, &registry, &collection).await;
                let permit = authorize_cell_access(
                    &registry,
                    IntraCellResource::QDRANT_REST,
                    Duration::from_secs(60),
                )
                .expect("semantic upsert permit");
                upsert(
                    transport.as_ref(),
                    &permit,
                    &collection,
                    &[(
                        PointId::Uuid(point),
                        &semantic_payload(&handle, updated),
                        semantic_vector(query),
                    )],
                    ha_profile_for(QdrantOperation::NormalImmutableUpsert),
                )
                .await
                .expect("real Qdrant semantic point");

                let (address, server) = start(app).await;
                let (status, remember) = raw_request(
                    address,
                    &tool_call_headers("remember", &credential.bearer),
                    &rpc(
                        1,
                        "tools/call",
                        call_params(
                            "remember",
                            json!({
                                "operation":"put",
                                "content":"soak-shaped write awaiting projection",
                                "idempotency_key":format!("ryw-limit-{}", Uuid::now_v7()),
                                "workspace_id":workspace_id,
                            }),
                        ),
                    ),
                )
                .await;
                assert_eq!(status, 200, "remember before the RYW recall: {remember}");
                let token = remember["result"]["structuredContent"]["consistency_token"]
                    .as_str()
                    .expect("opaque consistency token")
                    .to_owned();
                let evidence_id = remember["result"]["structuredContent"]["evidence_id"]
                    .as_str()
                    .expect("remember evidence id")
                    .to_owned();

                // Leg 1 — the shape the soak's replay sends: token, no caller `limit`.
                let (status, answered) = recall_call(
                    address,
                    Some(&credential.bearer),
                    json!({
                        "query":query,
                        "workspace_id":workspace_id,
                        "mode":"semantic",
                        "consistency_token":token.clone(),
                    }),
                )
                .await;
                assert_eq!(status, 200, "token recall must answer: {answered}");
                assert_ne!(
                    answered["result"]["structuredContent"]["code"], "DEPENDENCY_UNAVAILABLE",
                    "a valid consistency_token must not degrade the lane: {answered}"
                );
                let answered = assert_tool_response(&answered, ToolName::Recall);
                let items = answered["items"].as_array().expect("RYW items");
                assert!(
                    items.iter().any(|item| {
                        item["kind"] == "temporary_evidence" && item["evidence_id"] == evidence_id
                    }),
                    "§15.5 overlay must carry the write the token names: {answered}"
                );
                let profile_top_k = answered["provenance"]["profile"]["top_k"]
                    .as_u64()
                    .expect("envelope reports the registered profile depth");

                // Leg 2 — the shape it used to send. §55.1: not the caller's number to pick.
                let (status, refused) = recall_call(
                    address,
                    Some(&credential.bearer),
                    json!({
                        "query":query,
                        "workspace_id":workspace_id,
                        "mode":"semantic",
                        "consistency_token":token.clone(),
                        "limit":profile_top_k + 1,
                    }),
                )
                .await;
                assert_eq!(
                    status, 400,
                    "a caller-chosen limit is INVALID_INPUT, not a dependency failure: {refused}"
                );

                // Leg 3 — the profile's own depth echoed back is still accepted.
                let (status, echoed) = recall_call(
                    address,
                    Some(&credential.bearer),
                    json!({
                        "query":query,
                        "workspace_id":workspace_id,
                        "mode":"semantic",
                        "consistency_token":token,
                        "limit":profile_top_k,
                    }),
                )
                .await;
                assert_eq!(status, 200, "limit == profile top_k must answer: {echoed}");
                assert_tool_response(&echoed, ToolName::Recall);

                server.abort();
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
                // `task_id` is SERVED since the card-22b review fix (it is the §25.4.A(7)
                // TaskId the two binding-backed selectors read), so it is no longer on the
                // fail-closed list above. It is resolved instead: this fixture seeds no
                // `coord.tasks` row, so a fresh uuid names no task of this tenant and the
                // route answers NOT_FOUND rather than assembling as if no task was asked for.
                let (status, unresolved_task) = raw_request(
                    address,
                    &headers,
                    &rpc(
                        77,
                        "tools/call",
                        call_params(
                            "context",
                            json!({"workspace_id":handle.workspace_id,"task_id":Uuid::now_v7()}),
                        ),
                    ),
                )
                .await;
                assert_eq!(
                    status, 200,
                    "task_id is a served argument now: {unresolved_task}"
                );
                assert_tool_error(&unresolved_task, "NOT_FOUND");

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
        // Fresh planner statistics for private.memory_records: 0150 added archived_at + a partial
        // index, and test-fixture churn drifts the stats, so a cold plan of the gateway read can
        // run slowly enough to miss the 2s read-barrier deadline below. ANALYZE takes only a
        // ShareUpdateExclusiveLock and commits before the barrier transaction takes ACCESS
        // EXCLUSIVE — it changes plan cost, never the read's correctness or the settlement path.
        handle
            .admin
            .batch_execute("ANALYZE private.memory_records")
            .expect("refresh memory_records statistics before the read-barrier timing");
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
        // `barrier` is one long transaction, and PostgreSQL freezes `pg_stat_activity` at its
        // first access within a transaction: a gateway backend that connected after the first
        // poll (the read now runs on a connection opened after the ADR-0031 serving read) would
        // stay invisible to the join below forever without discarding that snapshot.
        barrier
            .batch_execute("SELECT pg_stat_clear_snapshot()")
            .map_err(|_| "refresh backend activity snapshot".to_owned())?;
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
/// Must exceed `bins/gateway/src/main.rs`'s `DRAIN_ANNOUNCE_WINDOW` (5s): a graceful stop now
/// deliberately keeps accepting for that window so `/readyz` can answer 503 to a supervisor that
/// dials a fresh connection per poll.
const GATEWAY_PROCESS_STOP_TIMEOUT: Duration = Duration::from_secs(30);

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
                "HUMAUX_GATEWAY_MOOD_HALF_LIFE_SECONDS".into(),
                "21600".into(),
            ),
            // ADR-0031 D-B: bind the WRITE route only (read routes derive their stream per
            // request from the credential's tenant + requested workspace).
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

    /// SIGTERM only — the process is expected to keep accepting for its drain window, which is
    /// what `gateway_readyz_answers_503_on_a_fresh_connection_while_draining` asserts.
    fn signal_terminate(&mut self) -> Result<(), String> {
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
        Ok(())
    }

    fn await_clean_exit(&mut self) -> Result<(), String> {
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

/// Card 15 / ADR-0037: a plain unauthenticated `GET`, returning the HTTP status line's code.
/// Deliberately NOT `raw_request` — the whole point of `/livez` and `/readyz` is that a
/// supervisor reaches them with no bearer token, no MCP session and no boundary headers, so the
/// probe used to assert them must not carry any either.
fn supervision_probe(address: SocketAddr, path: &str) -> u16 {
    supervision_probe_opt(address, path)
        .unwrap_or_else(|| panic!("{path} did not answer on a fresh connection"))
}

/// Like [`supervision_probe`] but returns `None` when the connection is refused or dropped —
/// the difference a supervisor actually sees, and the whole subject of
/// `gateway_readyz_answers_503_on_a_fresh_connection_while_draining`.
fn supervision_probe_opt(address: SocketAddr, path: &str) -> Option<u16> {
    use std::io::{Read, Write};
    let mut stream = StdTcpStream::connect_timeout(&address, Duration::from_secs(5)).ok()?;
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    response.split_whitespace().nth(1)?.parse().ok()
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

            provision_stream_pair(&handle, handle.workspace_id);
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
            // Card 15 / ADR-0037 D4: the supervision surface answers on the SAME live process
            // that just served real MCP traffic, with no credential of any kind.
            //
            // 注错: drop `.merge(supervision_routes(..))` from `bins/gateway/src/main.rs` ⇒ the
            // MCP boundary answers 404 for both paths and these two assertions go red.
            assert_eq!(
                supervision_probe(process.address, "/livez"),
                200,
                "/livez must answer 200 on a live gateway"
            );
            assert_eq!(
                supervision_probe(process.address, "/readyz"),
                200,
                "/readyz must answer 200 once bootstrap completed and the listener is accepting"
            );

            // ADR-0037 D4, the half that was asserted in prose only: after SIGTERM the process
            // must keep ACCEPTING long enough for a supervisor that opens a fresh TCP connection
            // per poll to read `503 draining`. A refused connection here is the failure mode the
            // 503 exists to rule out — it is indistinguishable from a crash, and Baseline §4.4
            // tells the supervisor to restart a crash and NOT to restart a drain.
            //
            // 注错: delete the `sleep(DRAIN_ANNOUNCE_WINDOW)` from the shutdown future in
            // `bins/gateway/src/main.rs` ⇒ the accept loop closes in the same instant readiness
            // flips, every poll below is refused, and this assertion goes red with "connection
            // refused ... never 503".
            process
                .signal_terminate()
                .expect("SIGTERM to the gateway binary");
            let deadline = Instant::now() + Duration::from_secs(4);
            let mut observed: Vec<Option<u16>> = Vec::new();
            let draining = loop {
                let seen = supervision_probe_opt(process.address, "/readyz");
                observed.push(seen);
                if seen == Some(503) {
                    break true;
                }
                if Instant::now() >= deadline {
                    break false;
                }
                thread::sleep(Duration::from_millis(50));
            };
            assert!(
                draining,
                "/readyz must answer 503 on a FRESH connection while draining; a supervisor                  polling over the pod IP saw {observed:?} instead (None = connection refused,                  which it cannot tell from a crash)"
            );
            // /livez keeps answering while the process is still up: "draining" is a readiness
            // statement, not a liveness one — a supervisor must not restart it for being down.
            assert_eq!(
                supervision_probe(process.address, "/livez"),
                200,
                "/livez must stay 200 while the process drains"
            );
            process
                .await_clean_exit()
                .expect("binary exits zero after the drain window");
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

/// Card 19 / ADR-0041, the half of the census that is observable WITHOUT a semantic runtime.
/// These fixtures have none, so §23.1②'s `visible` is legitimately unavailable and the class
/// stays `cannot_establish/index_count_unavailable` (card 18's assertion, unchanged). What the
/// census still proves here: §22.4's lower bound exists, and §23.3④'s two pipeline blocks are
/// real `stream_ledger` readings of this fixture's empty stream ledger instead of `null`s.
fn assert_census_without_index_count(response: &Value, returned: u64) {
    let envelope = &response["result"]["structuredContent"]["content"];
    assert_eq!(
        envelope["completeness"]["reason"], "index_count_unavailable",
        "{response}"
    );
    assert_eq!(
        envelope["completeness"]["known_lower_bound"], returned,
        "§22.4: a census that counted before a later trigger blocked the class still proves \
         'at least N': {response}"
    );
    assert_eq!(
        envelope["completeness"]["exact"],
        Value::Null,
        "§22.0: a non-exact class never carries the enumeration block: {response}"
    );
    for (block, field) in [
        ("evidence", "persisted"),
        ("knowledge", "eligible"),
        ("knowledge", "processed"),
        ("knowledge", "waiting_key"),
        ("knowledge", "failed"),
    ] {
        assert_eq!(
            envelope["pipeline"][block][field], 0,
            "§23.3④: {block}.{field} is a real reading of an empty ledger, not the `null` that \
             made every read cannot_establish/count_unknown: {response}"
        );
        assert_eq!(
            envelope["pipeline"][block]["count_scope"], "stream_ledger",
            "{response}"
        );
    }
}

/// Waits until a minting transaction is demonstrably parked on `context_repo`'s census barrier
/// (ADR-0041 D-G) — `pg_locks` reassembles a bigint advisory key as `classid << 32 | objid`, and
/// the caller keeps its key below 2^31 so `classid` is `0` — then commits one more authorized
/// memory into that window and releases. The returned row is therefore younger than the mint's
/// snapshot and older than the census statement that follows it: the only insert that can tell
/// "counted in the page's snapshot" apart from "counted in a fresh transaction right after it".
fn commit_into_mint_window(handle: &mut Handle, barrier_key: i64) -> Uuid {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let parked: i64 = handle
            .admin
            .query_one(
                "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted \
                   AND ((classid::bigint << 32) | objid::bigint) = $1",
                &[&barrier_key],
            )
            .expect("owner observes the parked minting transaction")
            .get(0);
        if parked > 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the minting transaction never reached the census barrier"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let raced = handle.seed_workspace_visible_context_record().memory_id;
    handle
        .admin
        .query_one("SELECT pg_advisory_unlock($1)", &[&barrier_key])
        .expect("release the mint barrier");
    raced
}

/// §22.1/§23.1④ read at the source: the readout frozen ON the manifest row this response
/// paginates (migration 0165), asserted as one all-or-nothing triple.
fn assert_frozen_census(handle: &mut Handle, response: &Value, total: i64, why: &str) {
    let snapshot_id = Uuid::parse_str(
        response["result"]["structuredContent"]["pagination"]["snapshot_id"]
            .as_str()
            .expect("snapshot id"),
    )
    .expect("snapshot uuid");
    let row = handle
        .admin
        .query_one(
            "SELECT census_predicate_id,census_total,census_excluded_secret \
               FROM ops.selection_snapshots WHERE selection_snapshot_id=$1",
            &[&snapshot_id],
        )
        .expect("owner reads the frozen census readout");
    assert_eq!(
        (
            row.get::<_, String>(0),
            row.get::<_, i64>(1),
            row.get::<_, i64>(2)
        ),
        ("authorized_memory_enumeration_v1".to_owned(), total, 0),
        "{why}"
    );
}

/// Card 19 speed record (ADR-0041, §"Speed"): the two call shapes this card created — a
/// minting page that runs the census, and a continuation that reads the frozen readout instead
/// of counting. Each iteration bills two billable reads, so the fixture's entitlement must
/// cover 2n on top of its own legs.
async fn record_enumerate_latency(address: SocketAddr, bearer: &str) {
    let mut minting = Vec::new();
    let mut continuation = Vec::new();
    for _ in 0..30 {
        let start = std::time::Instant::now();
        let (status, page) =
            enumerate_call(address, bearer, json!({"action":"enumerate","limit":2})).await;
        minting.push(start.elapsed());
        assert_eq!(status, 200, "timed minting page: {page}");
        let cursor = page["result"]["structuredContent"]["pagination"]["next_cursor"]
            .as_str()
            .expect("timed page continues")
            .to_owned();
        let start = std::time::Instant::now();
        let (status, page) = enumerate_call(
            address,
            bearer,
            json!({"action":"enumerate","limit":2,"cursor":cursor}),
        )
        .await;
        continuation.push(start.elapsed());
        assert_eq!(status, 200, "timed continuation page: {page}");
    }
    minting.sort();
    continuation.sort();
    eprintln!(
        "memory.enumerate p50/p95 (n=30, ms): minting (census counted) {}/{} · continuation \
         (frozen readout, no count) {}/{}",
        minting[14].as_millis(),
        minting[28].as_millis(),
        continuation[14].as_millis(),
        continuation[28].as_millis()
    );
}

#[test]
// §23.1④ is ONE causal chain — freeze a manifest, insert 20 rows underneath it, page the frozen
// manifest to exhaustion, then mint a fresh one — and the control only controls anything while
// its steps stay in that order in one body. The three reusable pieces (the census witness, the
// frozen readout, the latency record) are already helpers above; what is left is the chain.
#[allow(clippy::too_many_lines)]
fn native_mcp_memory_enumeration_freezes_snapshot_and_binds_cursor() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>(
        "native_mcp_memory_enumeration_freezes_snapshot_and_binds_cursor",
        |mut handle| {
            let credential = enumeration_credential(&mut handle, "owner");
            let peer = enumeration_credential(&mut handle, "peer");
            // 64 covered the snapshot/cursor legs; the card-19 latency record at the end of
            // this test bills 60 more billable reads (30 minting + 30 continuation).
            handle.seed_current_entitlement_and_window(160);
            let other_workspace = handle.seed_workspace();
            // Acceptance gate, "another user's private memories never raise the authorized
            // count": a row in THIS workspace's raw universe, owned by a peer and flipped to
            // USER_PRIVATE. It must stay out of the page AND out of the denominator, so the
            // frozen readout below is 5 while the table holds 6 active rows.
            let peer_user = handle.seed_peer_user();
            let peer_private = handle.seed_workspace_visible_context_record();
            set_record_user_visibility(&mut handle, &peer_private, peer_user);
            let mut expected: Vec<Uuid> = (0..5)
                .map(|_| handle.seed_workspace_visible_context_record().memory_id)
                .collect();
            expected.sort_unstable_by(|a, b| b.cmp(a));
            let raw_universe: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM private.memory_records \
                      WHERE tenant_id=$1 AND status='active' AND superseded_by IS NULL",
                    &[&handle.tenant_id],
                )
                .expect("owner counts the unfiltered universe the census counts over")
                .get(0);
            assert_eq!(
                raw_universe, 6,
                "the peer's USER_PRIVATE row is really in the table — so a denominator of 5 \
                 below is authorization narrowing, not a missing fixture row"
            );
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
                assert_census_without_index_count(&first, 1);
                tokio::task::block_in_place(|| {
                    assert_frozen_census(
                        &mut handle,
                        &first,
                        5,
                        "§22.1: total came from count(*) over the whole AUTHORIZED universe in \
                         the minting snapshot — not from the 1-row page it returned, and not \
                         from the 6 rows the table holds: the peer's USER_PRIVATE memory never \
                         raises the authorized count",
                    );
                });
                let inserted: Vec<Uuid> = tokio::task::block_in_place(|| {
                    (0..20)
                        .map(|_| handle.seed_workspace_visible_context_record().memory_id)
                        .collect()
                });
                tokio::task::block_in_place(|| {
                    assert_frozen_census(
                        &mut handle,
                        &first,
                        5,
                        "§23.1④ negative control: 20 concurrent inserts must not move an \
                         immutable manifest's denominator",
                    );
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
                // ...and the fresh manifest's own frozen denominator DID move, so the
                // assertion above is snapshot discipline rather than a stale number.
                tokio::task::block_in_place(|| {
                    assert_frozen_census(
                        &mut handle,
                        &fresh,
                        25,
                        "a NEW manifest counts the 20 concurrent inserts",
                    );
                });
                record_enumerate_latency(address, &credential.bearer).await;
                stop_server(server).await.expect("stop pagination server");
            });
        },
    );
}

#[test]
// §22.1/§23.1④'s load-bearing claim is that `total` is counted in the SAME snapshot the page is
// taken from. An insert that lands after a mint has already returned cannot witness that — a
// census taken in its own transaction right after the mint commits answers with the same
// pre-insert number, so the "concurrent insert does not move the total" control above kills
// "re-count per page" and nothing else. The only insert that separates the two is one committed
// INSIDE the mint's window, which is what `arm_census_mint_barrier` (ADR-0041 D-G) buys: the
// mint parks between its id list and its census, a second connection commits a fourth memory,
// and the census that follows must still say 3 because its snapshot predates that commit. Move
// the census out of that transaction and this test goes red (frozen readout absent or 4, reason
// `census_failed`, lower bound gone) while the control above stays green.
fn native_mcp_memory_enumeration_counts_in_the_page_snapshot() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>(
        "native_mcp_memory_enumeration_counts_in_the_page_snapshot",
        |mut handle| {
            let credential = enumeration_credential(&mut handle, "snapshot");
            handle.seed_current_entitlement_and_window(8);
            let mut expected: Vec<Uuid> = (0..3)
                .map(|_| handle.seed_workspace_visible_context_record().memory_id)
                .collect();
            expected.sort_unstable_by(|a, b| b.cmp(a));
            // Positive and below 2^31 so PostgreSQL's split of a bigint advisory key leaves
            // `classid = 0`, which is what the `pg_locks` predicate below reassembles.
            let barrier_key = i64::from(
                u32::from_be_bytes(
                    Uuid::new_v4().as_bytes()[..4]
                        .try_into()
                        .expect("four bytes of key entropy"),
                ) >> 1,
            ) + 1;
            let runtime_handle = handle.rt.handle().clone();
            let runtime = runtime_handle
                .block_on(handle.fresh_runtime())
                .expect("checked same-snapshot runtime");
            let app = application(&handle, runtime);
            runtime_handle.block_on(async {
                let (address, server) = start(app).await;
                tokio::task::block_in_place(|| {
                    handle
                        .admin
                        .query_one("SELECT pg_advisory_lock($1)", &[&barrier_key])
                        .expect("second connection holds the mint barrier");
                });
                humaux_adapters::context_repo::arm_census_mint_barrier(barrier_key);
                let bearer = credential.bearer.clone();
                let minting = tokio::spawn(async move {
                    enumerate_call(address, &bearer, json!({"action":"enumerate","limit":100}))
                        .await
                });
                let raced = tokio::task::block_in_place(|| {
                    commit_into_mint_window(&mut handle, barrier_key)
                });
                humaux_adapters::context_repo::arm_census_mint_barrier(0);
                let (status, page) = minting.await.expect("minting page task");
                assert_eq!(status, 200, "raced minting page: {page}");
                assert!(
                    assert_enumeration_response(&page, &expected).is_none(),
                    "the page itself is the pre-race snapshot"
                );
                assert_census_without_index_count(&page, 3);
                tokio::task::block_in_place(|| {
                    assert_frozen_census(
                        &mut handle,
                        &page,
                        3,
                        "§22.1 同一事务快照: the census ran in the transaction that took the id \
                         list, so a row committed after that snapshot and before the count is \
                         invisible to it — a census in any younger transaction would have said 4",
                    );
                });
                // Proof the race actually happened: the row was committed BEFORE the census
                // statement ran, and the very next manifest counts it.
                let (status, fresh) = enumerate_call(
                    address,
                    &credential.bearer,
                    json!({"action":"enumerate","limit":100}),
                )
                .await;
                assert_eq!(status, 200, "fresh manifest after the race: {fresh}");
                let mut after = expected.clone();
                after.push(raced);
                after.sort_unstable_by(|a, b| b.cmp(a));
                assert!(assert_enumeration_response(&fresh, &after).is_none());
                tokio::task::block_in_place(|| {
                    assert_frozen_census(
                        &mut handle,
                        &fresh,
                        4,
                        "the raced insert was committed and visible — so the 3 above is snapshot \
                         discipline, not a row that never landed",
                    );
                });
                stop_server(server)
                    .await
                    .expect("stop same-snapshot server");
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

// ===========================================================================================
// §Q4 memory.correct behind the same confirm gate (ADR-0025). A user correction: new Evidence
// + new Memory version + SUPERSEDE(USER_CORRECTION); the original Evidence is never edited.
// ===========================================================================================

async fn correct_call(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    target: Uuid,
    text: &str,
    token: Option<&str>,
) -> (u16, Value) {
    let mut arguments = json!({ "action": "correct", "memory_id": target, "text": text });
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

/// First call: a success-shaped `confirmation_required` naming memory.correct, no successor.
async fn mint_correct_token(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    target: Uuid,
) -> String {
    let (status, response) = correct_call(address, bearer, request_id, target, "x", None).await;
    assert_eq!(status, 200, "first call is an MCP result: {response}");
    let structured = &response["result"]["structuredContent"];
    assert_ne!(response["result"]["isError"], true, "{response}");
    assert_eq!(structured["confirmation_required"], true, "{response}");
    assert_eq!(structured["operation"], "memory.correct", "{response}");
    assert_eq!(
        structured["target"]["memory_id"],
        target.to_string(),
        "{response}"
    );
    assert!(
        structured["target"].get("replacement_memory_id").is_none(),
        "correct confirmation names no successor: {response}"
    );
    structured["confirm_token"]
        .as_str()
        .expect("confirm_token string")
        .to_owned()
}

/// (payload_sha256 bytes, events.payload) of one Evidence — the "original body" the correction
/// must never touch (acceptance: byte-for-byte unchanged).
fn evidence_body(handle: &mut Handle, evidence_id: Uuid) -> (Vec<u8>, Option<Value>) {
    let sha: Vec<u8> = handle
        .admin
        .query_one(
            "SELECT payload_sha256 FROM private.evidence_objects WHERE evidence_id = $1",
            &[&evidence_id],
        )
        .expect("owner reads evidence hash")
        .get(0);
    let payload: Option<Value> = handle
        .admin
        .query_opt(
            "SELECT payload FROM private.events WHERE event_id = $1",
            &[&evidence_id],
        )
        .expect("owner reads event body")
        .map(|row| row.get(0));
    (sha, payload)
}

#[test]
#[allow(clippy::too_many_lines)] // One HTTP fixture carries the whole correction acceptance gate.
fn native_mcp_memory_correct_writes_new_version_and_never_edits_evidence() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>("native_mcp_memory_correct", |mut handle| {
        handle.assert_gateway_login();
        let prefix = format!("mcor{}", &Uuid::now_v7().simple().to_string()[..12]);
        let wire = format!("{prefix}.{}", "c".repeat(32));
        let credential = handle.seed_synthetic_service_credential_and_window(
            SyntheticCredentialScopes::RememberWriteAndContextRead,
            &prefix,
            &wire,
            &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
            96,
        );
        let m = handle.seed_workspace_visible_context_record();

        // The original Evidence body, captured before the correction runs.
        let body_before = tokio::task::block_in_place(|| evidence_body(&mut handle, m.evidence_id));

        let runtime_handle = handle.rt.handle().clone();
        let runtime = runtime_handle
            .block_on(handle.fresh_runtime())
            .expect("correct runtime");
        let app = application(&handle, runtime);
        runtime_handle.block_on(async {
            let (address, server) = start(app).await;
            let bearer = credential.bearer.as_str();

            // Correct M1 with new content.
            let token = mint_correct_token(address, bearer, 1, m.memory_id).await;
            let (status, done) =
                correct_call(address, bearer, 2, m.memory_id, "the corrected fact", Some(&token))
                    .await;
            assert_eq!(status, 200, "confirmed correct: {done}");
            assert_ne!(done["result"]["isError"], true, "{done}");
            let structured = &done["result"]["structuredContent"];
            let m2 = Uuid::parse_str(structured["memory_id"].as_str().expect("M2 id"))
                .expect("M2 uuid");
            let e2 = Uuid::parse_str(structured["evidence_id"].as_str().expect("E2 id"))
                .expect("E2 uuid");
            assert_eq!(structured["superseded"], m.memory_id.to_string(), "{done}");
            assert_ne!(m2, m.memory_id, "correction is a NEW version, not in place");
            assert_ne!(e2, m.evidence_id, "correction writes NEW Evidence");
            assert!(structured["consistency_token"].is_string(), "{done}");
            assert!(structured["superseded_at"].is_string(), "{done}");

            tokio::task::block_in_place(|| {
                // M1 is superseded by M2, G59-4 holds.
                assert_eq!(
                    memory_state(&mut handle, m.memory_id),
                    ("superseded".into(), Some(m2), true, true),
                    "M1 -> superseded_by = M2"
                );
                // M2 is a new active version, type inherited, authority via policy (not inherited).
                let (status, mtype, aclass): (String, String, String) = {
                    let row = handle
                        .admin
                        .query_one(
                            "SELECT status, memory_type, authority_class \
                             FROM private.memory_records WHERE memory_id = $1",
                            &[&m2],
                        )
                        .expect("owner reads M2");
                    (row.get(0), row.get(1), row.get(2))
                };
                assert_eq!(status, "active", "M2 is active");
                assert_eq!(mtype, "NOTE", "M2 inherits M1's memory_type");
                assert_eq!(
                    aclass, "UserCorrection",
                    "DirectUserInput correction reaches UserCorrection, not M1's ProjectConstraint"
                );
                // M2's PRIMARY evidence is E2, and E2 is a DirectUserInput correction body.
                let (prim_evidence, origin): (Uuid, String) = {
                    let row = handle
                        .admin
                        .query_one(
                            "SELECT me.evidence_id, eo.origin_class \
                             FROM private.memory_evidence me \
                             JOIN private.evidence_objects eo ON eo.evidence_id = me.evidence_id \
                             WHERE me.memory_id = $1 AND me.role = 'PRIMARY'",
                            &[&m2],
                        )
                        .expect("owner reads M2 PRIMARY evidence");
                    (row.get(0), row.get(1))
                };
                assert_eq!(prim_evidence, e2, "M2 <- E2 PRIMARY");
                assert_eq!(origin, "DirectUserInput", "E2 origin is DirectUserInput");
                let (e2_kind, e2_payload): (String, Value) = {
                    let row = handle
                        .admin
                        .query_one(
                            "SELECT event_kind, payload FROM private.events WHERE event_id = $1",
                            &[&e2],
                        )
                        .expect("owner reads E2 body");
                    (row.get(0), row.get(1))
                };
                assert_eq!(e2_kind, "USER_CORRECTION");
                assert_eq!(e2_payload, json!("the corrected fact"), "E2 carries the new text");

                // ACCEPTANCE: the ORIGINAL Evidence body is unchanged byte-for-byte. If the
                // adapter were changed to UPDATE M1's Evidence text in place, this goes red.
                let body_after = evidence_body(&mut handle, m.evidence_id);
                assert_eq!(
                    body_after, body_before,
                    "the correction must never edit historical Evidence in place (spec :8568)"
                );

                // The correction record: a SUPERSEDE(USER_CORRECTION) naming M2 + E2 + the actor.
                let (op, reason, replacement, corr_evidence, has_deadline, actor_nonnil): (
                    String,
                    Option<String>,
                    Option<Uuid>,
                    Option<Uuid>,
                    bool,
                    bool,
                ) = {
                    let row = handle
                        .admin
                        .query_one(
                            "SELECT op, reason_code, replacement_memory_id, correction_evidence_id, \
                                    undo_deadline IS NOT NULL, actor_principal_id IS NOT NULL \
                             FROM ops.memory_lifecycle_events \
                             WHERE memory_id = $1 AND op = 'SUPERSEDE' \
                             ORDER BY event_seq DESC LIMIT 1",
                            &[&m.memory_id],
                        )
                        .expect("owner reads the correction event");
                    (
                        row.get(0),
                        row.get(1),
                        row.get(2),
                        row.get(3),
                        row.get(4),
                        row.get(5),
                    )
                };
                assert_eq!(op, "SUPERSEDE");
                assert_eq!(reason.as_deref(), Some("USER_CORRECTION"));
                assert_eq!(replacement, Some(m2), "correction names M2");
                assert_eq!(corr_evidence, Some(e2), "correction names E2");
                assert!(has_deadline, "a correction is restorable within the undo window");
                assert!(actor_nonnil, "correction names the actor");
                assert_eq!(
                    lifecycle_head(&mut handle, m.memory_id).map(|(op, _)| op),
                    Some("SUPERSEDE".to_owned())
                );
            });

            // Replay of the same confirmed call returns the original success, no second version.
            let (status, replay) =
                correct_call(address, bearer, 3, m.memory_id, "the corrected fact", Some(&token))
                    .await;
            assert_eq!(status, 200, "{replay}");
            assert_eq!(
                replay["result"]["structuredContent"]["memory_id"],
                m2.to_string(),
                "replay is idempotent (same M2), never a second correction"
            );
            tokio::task::block_in_place(|| {
                let versions: i64 = handle
                    .admin
                    .query_one(
                        "SELECT count(*) FROM private.memory_records \
                         WHERE tenant_id = $1 AND status = 'active' AND memory_id = $2",
                        &[&handle.tenant_id, &m2],
                    )
                    .expect("count M2")
                    .get(0);
                assert_eq!(versions, 1, "exactly one M2");
            });

            // ACCEPTANCE: the correction is itself restorable via card 3 (memory.restore undoes
            // the SUPERSEDE(USER_CORRECTION) within the window).
            let restore_token = mint_restore_token(address, bearer, 5, m.memory_id).await;
            let (status, restored) =
                restore_call(address, bearer, 6, m.memory_id, Some(&restore_token)).await;
            assert_eq!(status, 200, "confirmed restore of a correction: {restored}");
            assert_ne!(restored["result"]["isError"], true, "{restored}");
            assert_eq!(
                restored["result"]["structuredContent"]["memory_id"],
                m.memory_id.to_string()
            );
            tokio::task::block_in_place(|| {
                assert_eq!(
                    memory_state(&mut handle, m.memory_id),
                    ("active".into(), None, false, true),
                    "M1 is active again after restoring the correction"
                );
                assert_eq!(
                    lifecycle_head(&mut handle, m.memory_id).map(|(op, _)| op),
                    Some("RESTORE".to_owned()),
                    "head is a RESTORE undoing the correction's SUPERSEDE"
                );
                // Undoing a correction must also deactivate the correction-minted M2, else BOTH
                // the restored original and the corrected text stay active (dual-active). M2 is
                // reversed symmetrically: now superseded_by = M1.
                assert_eq!(
                    memory_state(&mut handle, m2),
                    ("superseded".into(), Some(m.memory_id), true, true),
                    "M2 is deactivated (superseded_by = M1) after the correction is undone"
                );
                // Exactly one active version of this fact remains, and it is M1.
                let active_versions: i64 = handle
                    .admin
                    .query_one(
                        "SELECT count(*) FROM private.memory_records \
                         WHERE tenant_id = $1 AND status = 'active' \
                           AND memory_id IN ($2, $3)",
                        &[&handle.tenant_id, &m.memory_id, &m2],
                    )
                    .expect("count active versions after undo")
                    .get(0);
                assert_eq!(
                    active_versions, 1,
                    "restoring a correction leaves exactly one active version (M1), never two"
                );
            });

            stop_server(server).await.expect("server shutdown");
        });
    });
}

// ===========================================================================================
// §36/§10.1 memory.confirm / memory.reject behind the same confirm gate (ADR-0026, Card 6).
// Promote a distill candidate the §10.1 ceiling rejected into UserConfirmed Evidence + a new
// Memory; the candidate queue is the thing that makes memory.confirm reachable.
// ===========================================================================================

/// Inserts one PENDING `private.distill_candidates` row (owner) and returns its id + the hex of
/// its `candidate_sha256`. The sha is over the exact body bytes, so the confirm lock matches the
/// echoed hex regardless of jsonb round-tripping.
fn seed_pending_candidate(
    handle: &mut Handle,
    source_evidence_id: Uuid,
    requested_class: &str,
    memory_type: &str,
    body: &Value,
) -> (Uuid, String) {
    let bytes = serde_json::to_vec(body).expect("serialize candidate body");
    let mut digest = Sha256::new();
    digest.update(&bytes);
    let sha: [u8; 32] = digest.finalize().into();
    let sha_vec = sha.to_vec();
    let requested_class = requested_class.to_owned();
    let memory_type = memory_type.to_owned();
    let candidate_id: Uuid = handle
        .admin
        .query_one(
            r#"INSERT INTO private.distill_candidates
                 (tenant_id, source_evidence_id, candidate_body, candidate_sha256, rejection_reason,
                  requested_class, memory_type, confidence, data_class, visibility_class,
                  visibility_workspace_id, reasoning_domain_id, state, expires_at)
               VALUES ($1,$2,$3,$4,'origin_authority_ceiling',$5,$6,0.7,'INTERNAL','WORKSPACE_SHARED',
                       $7,$8,'PENDING', clock_timestamp() + interval '7 days')
               RETURNING candidate_id"#,
            &[
                &handle.tenant_id,
                &source_evidence_id,
                body,
                &sha_vec,
                &requested_class,
                &memory_type,
                &handle.workspace_id,
                &handle.reasoning_domain_id,
            ],
        )
        .expect("owner seeds pending candidate")
        .get(0);
    (candidate_id, hex::encode(sha))
}

/// Seeds a second ACTIVE tenant and one PENDING candidate under it (reusing tenant A's
/// evidence/workspace/reasoning-domain — no FK enforces a tenant match), returning the foreign
/// `(tenant_id, candidate_id, sha hex)`. For the ADR-0026 cross-tenant RLS refusal gate: a
/// confirm under tenant A must resolve this row to NOT_FOUND via RLS, not application code.
fn seed_foreign_tenant_candidate(
    handle: &mut Handle,
    source_evidence_id: Uuid,
    body: &Value,
) -> (Uuid, Uuid, String) {
    let bytes = serde_json::to_vec(body).expect("serialize candidate body");
    let mut digest = Sha256::new();
    digest.update(&bytes);
    let sha: [u8; 32] = digest.finalize().into();
    let sha_vec = sha.to_vec();
    let foreign_tenant = Uuid::new_v4();
    handle
        .admin
        .execute(
            "INSERT INTO control.tenants(tenant_id,name,state) VALUES($1,$2,'ACTIVE')",
            &[&foreign_tenant, &format!("card6-foreign-{foreign_tenant}")],
        )
        .expect("seed foreign tenant");
    let candidate_id: Uuid = handle
        .admin
        .query_one(
            r#"INSERT INTO private.distill_candidates
                 (tenant_id, source_evidence_id, candidate_body, candidate_sha256, rejection_reason,
                  requested_class, memory_type, confidence, data_class, visibility_class,
                  visibility_workspace_id, reasoning_domain_id, state, expires_at)
               VALUES ($1,$2,$3,$4,'origin_authority_ceiling','ProjectDecision','DECISION',0.7,
                       'INTERNAL','WORKSPACE_SHARED',$5,$6,'PENDING',
                       clock_timestamp() + interval '7 days')
               RETURNING candidate_id"#,
            &[
                &foreign_tenant,
                &source_evidence_id,
                body,
                &sha_vec,
                &handle.workspace_id,
                &handle.reasoning_domain_id,
            ],
        )
        .expect("seed foreign-tenant candidate")
        .get(0);
    (foreign_tenant, candidate_id, hex::encode(sha))
}

async fn confirm_call(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    candidate_id: Uuid,
    candidate_sha256: &str,
    token: Option<&str>,
) -> (u16, Value) {
    let mut arguments = json!({
        "action": "confirm",
        "candidate_id": candidate_id,
        "candidate_sha256": candidate_sha256,
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

async fn reject_call(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    candidate_id: Uuid,
    token: Option<&str>,
) -> (u16, Value) {
    let mut arguments = json!({ "action": "reject", "candidate_id": candidate_id });
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

/// Extracts the confirm_token from a first-call `confirmation_required` naming the op + candidate.
fn mint_candidate_token(operation: &str, candidate_id: Uuid, first_call: (u16, Value)) -> String {
    let (status, response) = first_call;
    assert_eq!(status, 200, "first call is an MCP result: {response}");
    let structured = &response["result"]["structuredContent"];
    assert_ne!(response["result"]["isError"], true, "{response}");
    assert_eq!(structured["confirmation_required"], true, "{response}");
    assert_eq!(structured["operation"], operation, "{response}");
    assert_eq!(
        structured["target"]["candidate_id"],
        candidate_id.to_string(),
        "{response}"
    );
    assert!(
        structured["target"].get("memory_id").is_none(),
        "candidate confirmation names no memory_id: {response}"
    );
    structured["confirm_token"]
        .as_str()
        .expect("confirm_token string")
        .to_owned()
}

#[test]
#[allow(clippy::too_many_lines)] // One HTTP fixture carries the whole confirm acceptance gate.
fn native_mcp_memory_confirm_promotes_candidate_to_user_confirmed_evidence() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>("native_mcp_memory_confirm", |mut handle| {
        handle.assert_gateway_login();
        let prefix = format!("mcfm{}", &Uuid::now_v7().simple().to_string()[..12]);
        let wire = format!("{prefix}.{}", "c".repeat(32));
        let credential = handle.seed_synthetic_service_credential_and_window(
            SyntheticCredentialScopes::RememberWriteAndContextRead,
            &prefix,
            &wire,
            &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
            96,
        );
        let m = handle.seed_workspace_visible_context_record();
        let body = json!({
            "title": "Adopt Postgres for the job queue",
            "key_claim": "The team decided to adopt Postgres for the job queue.",
        });
        let (candidate_id, sha_hex) = tokio::task::block_in_place(|| {
            seed_pending_candidate(
                &mut handle,
                m.evidence_id,
                "ProjectDecision",
                "DECISION",
                &body,
            )
        });

        let runtime_handle = handle.rt.handle().clone();
        let runtime = runtime_handle
            .block_on(handle.fresh_runtime())
            .expect("confirm runtime");
        let app = application(&handle, runtime);
        runtime_handle.block_on(async {
            let (address, server) = start(app).await;
            let bearer = credential.bearer.as_str();

            // 1. A confirmed call with a made-up token is rejected (confirm needs a real token).
            let bogus = "A".repeat(43);
            let (status, denied) =
                confirm_call(address, bearer, 1, candidate_id, &sha_hex, Some(&bogus)).await;
            assert_eq!(status, 200, "{denied}");
            assert_eq!(
                denied["result"]["isError"], true,
                "bogus token rejected: {denied}"
            );

            // 1a. M1 sentinel: a WRONG candidate_sha256 is NOT_FOUND. The lock binds the exact
            //     reviewed body (candidate_id + sha), so a mismatched sha resolves to no row — it
            //     never falls back to the right candidate. The lock's NOT_FOUND precedes token
            //     consume, so the real candidate stays PENDING for step 2.
            let wrong_sha = "0".repeat(64);
            let first = confirm_call(address, bearer, 10, candidate_id, &wrong_sha, None).await;
            let wrong_token = mint_candidate_token("memory.confirm", candidate_id, first);
            let (status, mismatched) = confirm_call(
                address,
                bearer,
                11,
                candidate_id,
                &wrong_sha,
                Some(&wrong_token),
            )
            .await;
            assert_eq!(status, 200, "{mismatched}");
            assert_tool_error(&mismatched, "NOT_FOUND");

            // 1b. Cross-tenant acceptance gate: another tenant's candidate is refused by RLS
            //     (0 rows -> NOT_FOUND), never by application code. Seed tenant B + a tenant-B
            //     candidate, then confirm it under tenant A's credential.
            let (foreign_tenant, foreign_candidate, foreign_sha) =
                tokio::task::block_in_place(|| {
                    seed_foreign_tenant_candidate(&mut handle, m.evidence_id, &body)
                });
            let first =
                confirm_call(address, bearer, 12, foreign_candidate, &foreign_sha, None).await;
            let foreign_token = mint_candidate_token("memory.confirm", foreign_candidate, first);
            let (status, cross) = confirm_call(
                address,
                bearer,
                13,
                foreign_candidate,
                &foreign_sha,
                Some(&foreign_token),
            )
            .await;
            assert_eq!(status, 200, "{cross}");
            assert_tool_error(&cross, "NOT_FOUND");

            // 1c. M4 sentinel: an EXPIRED (past-deadline) PENDING candidate is a 1102 CONFLICT,
            //     never a confirm. Seed a second tenant-A candidate and push its deadline past.
            let (expired_candidate, expired_sha) = tokio::task::block_in_place(|| {
                let (cid, sha) = seed_pending_candidate(
                    &mut handle,
                    m.evidence_id,
                    "ProjectDecision",
                    "DECISION",
                    &body,
                );
                handle
                    .admin
                    .execute(
                        "UPDATE private.distill_candidates \
                         SET expires_at = clock_timestamp() - interval '1 hour' \
                         WHERE candidate_id = $1",
                        &[&cid],
                    )
                    .expect("expire the candidate");
                (cid, sha)
            });
            let first =
                confirm_call(address, bearer, 14, expired_candidate, &expired_sha, None).await;
            let expired_token = mint_candidate_token("memory.confirm", expired_candidate, first);
            let (status, expired) = confirm_call(
                address,
                bearer,
                15,
                expired_candidate,
                &expired_sha,
                Some(&expired_token),
            )
            .await;
            assert_eq!(status, 200, "{expired}");
            let s = &expired["result"]["structuredContent"];
            assert_eq!(s["code"], "CONFLICT", "expired is a CONFLICT: {expired}");
            assert_eq!(s["reason"], 1102, "CANDIDATE_EXPIRED: {expired}");

            // 2. Mint a token (first call, no token), then confirm the candidate.
            let first = confirm_call(address, bearer, 2, candidate_id, &sha_hex, None).await;
            let token = mint_candidate_token("memory.confirm", candidate_id, first);
            let (status, done) =
                confirm_call(address, bearer, 3, candidate_id, &sha_hex, Some(&token)).await;
            assert_eq!(status, 200, "confirmed: {done}");
            assert_ne!(done["result"]["isError"], true, "{done}");
            let structured = &done["result"]["structuredContent"];
            let m2 =
                Uuid::parse_str(structured["memory_id"].as_str().expect("M id")).expect("M uuid");
            let e2 =
                Uuid::parse_str(structured["evidence_id"].as_str().expect("E id")).expect("E uuid");
            assert_eq!(
                structured["candidate_id"],
                candidate_id.to_string(),
                "{done}"
            );
            assert!(structured["consistency_token"].is_string(), "{done}");

            tokio::task::block_in_place(|| {
                // The candidate is CONFIRMED and names the new memory.
                let (state, confirmed): (String, Option<Uuid>) = {
                    let row = handle
                        .admin
                        .query_one(
                            "SELECT state, confirmed_memory_id FROM private.distill_candidates \
                             WHERE candidate_id = $1",
                            &[&candidate_id],
                        )
                        .expect("owner reads candidate");
                    (row.get(0), row.get(1))
                };
                assert_eq!(state, "CONFIRMED", "candidate is CONFIRMED");
                assert_eq!(confirmed, Some(m2), "candidate names the new memory");

                // M is a new active memory: type from the candidate, authority via UserConfirmed
                // ceiling (ProjectDecision <= UserCorrection), never the source origin's lower one.
                let (mstatus, mtype, aclass): (String, String, String) = {
                    let row = handle
                        .admin
                        .query_one(
                            "SELECT status, memory_type, authority_class \
                             FROM private.memory_records WHERE memory_id = $1",
                            &[&m2],
                        )
                        .expect("owner reads M");
                    (row.get(0), row.get(1), row.get(2))
                };
                assert_eq!(mstatus, "active", "M is active");
                assert_eq!(mtype, "DECISION", "M carries the candidate's memory_type");
                assert_eq!(
                    aclass, "ProjectDecision",
                    "UserConfirmed basis authorizes the requested ProjectDecision"
                );

                // M's PRIMARY evidence is E2, a UserConfirmed manual note traceable to the source.
                let (prim, origin, kind): (Uuid, String, String) = {
                    let row = handle
                        .admin
                        .query_one(
                            "SELECT me.evidence_id, eo.origin_class, ev.event_kind \
                             FROM private.memory_evidence me \
                             JOIN private.evidence_objects eo ON eo.evidence_id = me.evidence_id \
                             JOIN private.events ev ON ev.event_id = me.evidence_id \
                             WHERE me.memory_id = $1 AND me.role = 'PRIMARY'",
                            &[&m2],
                        )
                        .expect("owner reads M PRIMARY evidence");
                    (row.get(0), row.get(1), row.get(2))
                };
                assert_eq!(prim, e2, "M <- E2 PRIMARY");
                assert_eq!(origin, "UserConfirmed", "E2 origin is UserConfirmed");
                assert_eq!(kind, "MANUAL_NOTE", "E2 is a manual note subtype");
            });

            // 3. Re-confirming the now-CONFIRMED candidate is refused with 1101 (needs a fresh
            //    token — the first was consumed).
            let first = confirm_call(address, bearer, 4, candidate_id, &sha_hex, None).await;
            let token2 = mint_candidate_token("memory.confirm", candidate_id, first);
            let (status, again) =
                confirm_call(address, bearer, 5, candidate_id, &sha_hex, Some(&token2)).await;
            assert_eq!(status, 200, "{again}");
            let s = &again["result"]["structuredContent"];
            assert_eq!(
                s["code"], "CONFLICT",
                "already-confirmed is a CONFLICT: {again}"
            );
            assert_eq!(s["reason"], 1101, "CANDIDATE_ALREADY_CONFIRMED: {again}");

            // The fixture cleans up by tenant A's id only; drop tenant B (cascades its candidate).
            tokio::task::block_in_place(|| {
                handle
                    .admin
                    .execute(
                        "DELETE FROM control.tenants WHERE tenant_id = $1",
                        &[&foreign_tenant],
                    )
                    .expect("clean up foreign tenant");
            });

            stop_server(server).await.expect("server shutdown");
        });
    });
}

#[test]
fn native_mcp_memory_reject_and_enumerate_candidates() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>("native_mcp_memory_reject", |mut handle| {
        handle.assert_gateway_login();
        let prefix = format!("mcrj{}", &Uuid::now_v7().simple().to_string()[..12]);
        let wire = format!("{prefix}.{}", "c".repeat(32));
        let credential = handle.seed_synthetic_service_credential_and_window(
            SyntheticCredentialScopes::RememberWriteAndContextRead,
            &prefix,
            &wire,
            &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
            96,
        );
        let m = handle.seed_workspace_visible_context_record();
        let body = json!({"title": "Prefer Rust", "key_claim": "Prefer Rust for new services."});
        let (candidate_id, _sha) = tokio::task::block_in_place(|| {
            seed_pending_candidate(
                &mut handle,
                m.evidence_id,
                "UserPreference",
                "PREFERENCE",
                &body,
            )
        });

        let runtime_handle = handle.rt.handle().clone();
        let runtime = runtime_handle
            .block_on(handle.fresh_runtime())
            .expect("reject runtime");
        let app = application(&handle, runtime);
        runtime_handle.block_on(async {
            let (address, server) = start(app).await;
            let bearer = credential.bearer.as_str();

            // Enumerate: the pending candidate is listed.
            let (status, listed) = enumerate_call(
                address,
                bearer,
                json!({ "action": "enumerate", "candidates": true }),
            )
            .await;
            assert_eq!(status, 200, "{listed}");
            let cands = listed["result"]["structuredContent"]["candidates"]
                .as_array()
                .expect("candidates array");
            assert!(
                cands
                    .iter()
                    .any(|c| c["candidate_id"] == candidate_id.to_string()),
                "pending candidate is enumerated: {listed}"
            );

            // Reject it (mint token, then reject).
            let first = reject_call(address, bearer, 1, candidate_id, None).await;
            let token = mint_candidate_token("memory.reject", candidate_id, first);
            let (status, done) = reject_call(address, bearer, 2, candidate_id, Some(&token)).await;
            assert_eq!(status, 200, "rejected: {done}");
            assert_ne!(done["result"]["isError"], true, "{done}");
            assert_eq!(
                done["result"]["structuredContent"]["state"], "rejected",
                "{done}"
            );

            tokio::task::block_in_place(|| {
                let state: String = handle
                    .admin
                    .query_one(
                        "SELECT state FROM private.distill_candidates WHERE candidate_id = $1",
                        &[&candidate_id],
                    )
                    .expect("owner reads candidate")
                    .get(0);
                assert_eq!(state, "REJECTED", "candidate is REJECTED");
            });

            // A rejected candidate no longer enumerates.
            let (_status, listed) = enumerate_call(
                address,
                bearer,
                json!({ "action": "enumerate", "candidates": true }),
            )
            .await;
            let cands = listed["result"]["structuredContent"]["candidates"]
                .as_array()
                .expect("candidates array");
            assert!(
                !cands
                    .iter()
                    .any(|c| c["candidate_id"] == candidate_id.to_string()),
                "rejected candidate is not enumerated: {listed}"
            );

            stop_server(server).await.expect("server shutdown");
        });
    });
}

// ===========================================================================================
// §36 memory.archive / memory.unarchive confirm gate (ADR-0024, Q3).
// ===========================================================================================

async fn archive_call(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    action: &str,
    target: Uuid,
    token: Option<&str>,
) -> (u16, Value) {
    let mut arguments = json!({ "action": action, "memory_id": target });
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

/// First call: a success-shaped `confirmation_required` naming memory.{archive,unarchive}.
async fn mint_archive_token(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    action: &str,
    target: Uuid,
) -> String {
    let (status, response) = archive_call(address, bearer, request_id, action, target, None).await;
    assert_eq!(status, 200, "first call is an MCP result: {response}");
    let structured = &response["result"]["structuredContent"];
    assert_ne!(response["result"]["isError"], true, "{response}");
    assert_eq!(structured["confirmation_required"], true, "{response}");
    assert_eq!(
        structured["operation"],
        format!("memory.{action}"),
        "{response}"
    );
    assert_eq!(
        structured["target"]["memory_id"],
        target.to_string(),
        "{response}"
    );
    assert!(
        structured["target"].get("replacement_memory_id").is_none(),
        "archive confirmation names no successor: {response}"
    );
    structured["confirm_token"]
        .as_str()
        .expect("confirm_token string")
        .to_owned()
}

/// Drives archive/unarchive to completion; returns the executed structuredContent.
async fn drive_archive(
    address: SocketAddr,
    bearer: &str,
    base_request_id: u64,
    action: &str,
    target: Uuid,
) -> Value {
    let token = mint_archive_token(address, bearer, base_request_id, action, target).await;
    let (status, done) = archive_call(
        address,
        bearer,
        base_request_id + 1,
        action,
        target,
        Some(&token),
    )
    .await;
    assert_eq!(status, 200, "confirmed {action}: {done}");
    assert_ne!(done["result"]["isError"], true, "{done}");
    done["result"]["structuredContent"].clone()
}

/// `archived_at IS NOT NULL` for one row, read by the owner.
fn is_archived(handle: &mut Handle, memory_id: Uuid) -> bool {
    handle
        .admin
        .query_one(
            "SELECT archived_at IS NOT NULL FROM private.memory_records WHERE memory_id = $1",
            &[&memory_id],
        )
        .expect("owner reads archived_at")
        .get(0)
}

/// The `archived` flag on a memory.get item (absent -> false, per skip-when-false wire shape).
async fn memory_get_archived(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    target: Uuid,
) -> (bool, bool) {
    let (status, response) = raw_request(
        address,
        &tool_call_headers("memory", bearer),
        &rpc(
            request_id,
            "tools/call",
            call_params("memory", json!({"action":"get","memory_id":target})),
        ),
    )
    .await;
    assert_eq!(status, 200, "memory.get: {response}");
    let content = assert_tool_response(&response, ToolName::Memory);
    let items = content["items"].as_array().expect("get items");
    let returned = items.iter().any(|i| i["memory_id"] == target.to_string());
    let archived = items
        .iter()
        .find(|i| i["memory_id"] == target.to_string())
        .and_then(|i| i["archived"].as_bool())
        .unwrap_or(false);
    (returned, archived)
}

#[test]
#[allow(clippy::too_many_lines)] // One real HTTP fixture carries the whole archive/unarchive matrix.
fn native_mcp_memory_archive_confirm_gate_acceptance() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>("native_mcp_memory_archive_confirm_gate", |mut handle| {
        handle.assert_gateway_login();
        let prefix = format!("marc{}", &Uuid::now_v7().simple().to_string()[..12]);
        let wire = format!("{prefix}.{}", "a".repeat(32));
        let credential = handle.seed_synthetic_service_credential_and_window(
            SyntheticCredentialScopes::RememberWriteAndContextRead,
            &prefix,
            &wire,
            &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
            64,
        );
        let m = handle.seed_workspace_visible_context_record();

        let runtime_handle = handle.rt.handle().clone();
        let runtime = runtime_handle
            .block_on(handle.fresh_runtime())
            .expect("checked archive runtime");
        let app = application(&handle, runtime);
        runtime_handle.block_on(async {
            let (address, server) = start(app).await;
            let bearer = credential.bearer.as_str();

            // Before archive: get returns the row, archived flag absent (false).
            let (returned, archived) = memory_get_archived(address, bearer, 1, m.memory_id).await;
            assert!(
                returned && !archived,
                "live memory.get: returned, not archived"
            );

            // Archive through the confirm gate -> archived_at set, one ARCHIVE lifecycle event.
            let done = drive_archive(address, bearer, 2, "archive", m.memory_id).await;
            assert_eq!(done["memory_id"], m.memory_id.to_string(), "{done}");
            assert!(done["archived_at"].is_string(), "{done}");
            assert!(done.get("unarchived_at").is_none(), "{done}");
            assert!(done["stream_seq"].as_i64().is_some(), "{done}");
            tokio::task::block_in_place(|| {
                assert!(is_archived(&mut handle, m.memory_id), "archived_at is set");
                // Q3: archive is NOT a status transition — status/G59-4 untouched.
                assert_eq!(
                    memory_state(&mut handle, m.memory_id),
                    ("active".into(), None, false, true),
                    "archive leaves status='active' and G59-4 intact"
                );
                assert_eq!(lifecycle_op_count(&mut handle, m.memory_id, "ARCHIVE"), 1);
            });

            // memory.get still returns the row, now archived:true (D-C).
            let (returned, archived) = memory_get_archived(address, bearer, 4, m.memory_id).await;
            assert!(returned && archived, "memory.get returns archived:true");

            // memory.enumerate excludes the archived row by default (D-C).
            let (status, page) = enumerate_call(
                address,
                bearer,
                json!({"action":"enumerate","workspace_id":handle.workspace_id}),
            )
            .await;
            assert_eq!(status, 200, "enumerate: {page}");
            let listed = assert_tool_response(&page, ToolName::Memory)["content"]["items"]
                .as_array()
                .expect("enumerate items")
                .iter()
                .any(|i| i["memory_id"] == m.memory_id.to_string());
            assert!(
                !listed,
                "enumerate default output excludes the archived row"
            );

            // memory.restore refuses an ARCHIVE head as NOT_REVERSIBLE (1202): the recorded
            // decision is that archive is undone by memory.unarchive, never by memory.restore.
            let restore_token = mint_restore_token(address, bearer, 6, m.memory_id).await;
            let (status, refused) =
                restore_call(address, bearer, 7, m.memory_id, Some(&restore_token)).await;
            assert_eq!(status, 200, "{refused}");
            assert_restore_conflict(&refused, 1202, "NOT_REVERSIBLE");
            tokio::task::block_in_place(|| {
                assert!(
                    is_archived(&mut handle, m.memory_id),
                    "refused restore mutates nothing"
                );
            });

            // Idempotency: archiving an already-archived row is ALREADY_IN_STATE (1201).
            let dup_token = mint_archive_token(address, bearer, 8, "archive", m.memory_id).await;
            let (status, dup) =
                archive_call(address, bearer, 9, "archive", m.memory_id, Some(&dup_token)).await;
            assert_eq!(status, 200, "{dup}");
            assert_restore_conflict(&dup, 1201, "ALREADY_IN_STATE");

            // Unarchive -> archived_at cleared, a RESTORE event undoing the ARCHIVE head.
            let archive_event = tokio::task::block_in_place(|| {
                handle
                    .admin
                    .query_one(
                        "SELECT event_id FROM ops.memory_lifecycle_events \
                         WHERE memory_id = $1 AND op = 'ARCHIVE' ORDER BY event_seq DESC LIMIT 1",
                        &[&m.memory_id],
                    )
                    .expect("owner reads ARCHIVE event id")
                    .get::<_, Uuid>(0)
            });
            let done = drive_archive(address, bearer, 10, "unarchive", m.memory_id).await;
            assert!(done["unarchived_at"].is_string(), "{done}");
            assert!(done.get("archived_at").is_none(), "{done}");
            tokio::task::block_in_place(|| {
                assert!(
                    !is_archived(&mut handle, m.memory_id),
                    "archived_at cleared"
                );
                assert_eq!(
                    lifecycle_head(&mut handle, m.memory_id),
                    Some(("RESTORE".to_owned(), Some(archive_event))),
                    "head is a RESTORE naming the ARCHIVE it undoes"
                );
            });

            // Visible again: get archived:false, enumerate includes it.
            let (returned, archived) = memory_get_archived(address, bearer, 12, m.memory_id).await;
            assert!(returned && !archived, "unarchived memory.get: not archived");
            let (status, page) = enumerate_call(
                address,
                bearer,
                json!({"action":"enumerate","workspace_id":handle.workspace_id}),
            )
            .await;
            assert_eq!(status, 200, "enumerate after unarchive: {page}");
            let listed = assert_tool_response(&page, ToolName::Memory)["content"]["items"]
                .as_array()
                .expect("enumerate items")
                .iter()
                .any(|i| i["memory_id"] == m.memory_id.to_string());
            assert!(listed, "enumerate includes the unarchived row again");

            // Unarchive of a live row is ALREADY_IN_STATE (1201), symmetric with archive.
            let dup_token = mint_archive_token(address, bearer, 14, "unarchive", m.memory_id).await;
            let (status, dup) = archive_call(
                address,
                bearer,
                15,
                "unarchive",
                m.memory_id,
                Some(&dup_token),
            )
            .await;
            assert_eq!(status, 200, "{dup}");
            assert_restore_conflict(&dup, 1201, "ALREADY_IN_STATE");

            server.abort();
        });
    });
}

/// Regression (ADR-0024 D-B): a supersede between archive and unarchive moves the lifecycle
/// head to the SUPERSEDE event, yet the unarchive RESTORE must still name the ARCHIVE event it
/// undoes — never the intervening SUPERSEDE. Before the fix, unarchive read
/// `lifecycle_head_event_id` (then the SUPERSEDE) and recorded a false undo edge.
#[test]
#[allow(clippy::too_many_lines)]
fn native_mcp_memory_unarchive_undoes_archive_not_intervening_supersede() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>(
        "native_mcp_memory_unarchive_undoes_archive",
        |mut handle| {
            handle.assert_gateway_login();
            let prefix = format!("muar{}", &Uuid::now_v7().simple().to_string()[..12]);
            let wire = format!("{prefix}.{}", "b".repeat(32));
            let credential = handle.seed_synthetic_service_credential_and_window(
                SyntheticCredentialScopes::RememberWriteAndContextRead,
                &prefix,
                &wire,
                &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
                80,
            );
            let m = handle.seed_workspace_visible_context_record();
            let successor = handle.seed_workspace_visible_context_record();

            let runtime_handle = handle.rt.handle().clone();
            let runtime = runtime_handle
                .block_on(handle.fresh_runtime())
                .expect("interleaved archive runtime");
            let app = application(&handle, runtime);
            runtime_handle.block_on(async {
                let (address, server) = start(app).await;
                let bearer = credential.bearer.as_str();

                // Archive (head -> ARCHIVE), then supersede while archived: archived_at stays set
                // (supersede's arbiter is WHERE status='active', no archived filter), head -> SUPERSEDE.
                drive_archive(address, bearer, 2, "archive", m.memory_id).await;
                drive_supersede(address, bearer, 4, m.memory_id, successor.memory_id).await;

                let (archive_event, supersede_event) = tokio::task::block_in_place(|| {
                    assert!(
                        is_archived(&mut handle, m.memory_id),
                        "still archived after supersede"
                    );
                    assert_eq!(
                        lifecycle_head(&mut handle, m.memory_id).map(|(op, _)| op),
                        Some("SUPERSEDE".to_owned()),
                        "supersede moved the head off ARCHIVE"
                    );
                    let archive_event = handle
                        .admin
                        .query_one(
                            "SELECT event_id FROM ops.memory_lifecycle_events \
                         WHERE memory_id = $1 AND op = 'ARCHIVE' ORDER BY event_seq DESC LIMIT 1",
                            &[&m.memory_id],
                        )
                        .expect("owner reads ARCHIVE event id")
                        .get::<_, Uuid>(0);
                    (
                        archive_event,
                        latest_supersede_event_id(&mut handle, m.memory_id),
                    )
                });
                assert_ne!(archive_event, supersede_event, "distinct events");

                // Unarchive: the RESTORE undoes the ARCHIVE event, never the intervening SUPERSEDE.
                drive_archive(address, bearer, 6, "unarchive", m.memory_id).await;
                tokio::task::block_in_place(|| {
                    assert!(
                        !is_archived(&mut handle, m.memory_id),
                        "archived_at cleared"
                    );
                    assert_eq!(
                        lifecycle_head(&mut handle, m.memory_id),
                        Some(("RESTORE".to_owned(), Some(archive_event))),
                        "unarchive RESTORE undoes the ARCHIVE event, not the SUPERSEDE"
                    );
                });

                server.abort();
            });
        },
    );
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
        let x_seed = handle.seed_workspace_visible_context_record();
        let y_seed = handle.seed_workspace_visible_context_record();
        // x: a workspace-visible memory with NO mandatory binding (the fixture's MANDATORY row
        // is revoked). y: a second record for the misbound-token case.
        //
        // card 22c (ADR-0046): this loop used to ALSO run
        // `UPDATE ... SET authority_class='ExplicitTaskContext'`, to lift x above the pinned
        // floor without matching `project_active_constraints_v1`. That write is now refused by
        // `memory_records_stored_authority_v2_check` (I-STORE) — and it was never a legal state
        // to begin with: §10.1 rule 2 says no Evidence origin can produce a stored 6, so the
        // production write path could not have created this row. The fixture was manufacturing
        // an authority the system cannot mint. What that exposes about PINNED delivery is
        // recorded at the delivery assertion below, not papered over here.
        for record in [&x_seed, &y_seed] {
            handle
                .admin
                .execute(
                    "UPDATE private.context_bindings SET revoked_at = now() WHERE context_binding_id=$1",
                    &[&record.binding_id],
                )
                .expect("owner revokes the fixture's mandatory binding");
        }
        let (x, y) = (x_seed, y_seed);

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
            // card 22c (ADR-0046, §25.4.B(6) + that ADR's open debt): this asserted
            // `vec![x.memory_id]`, and it only held because the fixture had raised x to a stored
            // `ExplicitTaskContext` — the one class that clears the PINNED floor without being
            // claimed by `project_active_constraints_v1`, and a class §10.1 rule 2 says no
            // origin can produce. With that fixture write refused (I-STORE), the pre-existing
            // defect is visible: the PINNED lane borrows its floor from
            // `explicit_mandatory_bindings_v1` (`ProjectConstraint`), every row at that class is
            // already a Mandatory row, and `PinnedLane::excluding_mandatory` therefore empties
            // the lane for every legally-storable memory. Changing that floor is a policy
            // decision with no ruling behind it, so it is REPORTED (ADR-0046 open debt), not
            // guessed at here. What `memory.pin` itself guarantees — the PINNED row, active,
            // scoped, idempotent — is asserted above and below and is unchanged.
            assert_eq!(
                assemble_pinned_ids(address, bearer, 8, workspace).await,
                Vec::<Uuid>::new(),
                "see ADR-0046 open debt: the PINNED lane has no deliverable class left"
            );
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
                // card 22b: pin/unpin carry no TASK dimension and no replacement (the
                // validator rejects either on this pair).
                task: None,
                replaces_binding_id: None,
                // card 22c: `purpose` is a memory.bind field; unpin carries none.
                purpose: None,
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
                Err(ErrorCode::Conflict),
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

// ---------------------------------------------------------------------------------------
// card 22b / §25.4.A: memory.bind, the facet projection, and the two selectors that used to
// be column-unavailable. ADR-0045.
// ---------------------------------------------------------------------------------------

/// §78.1 registered GOLDEN (ruling §五.2), 12 entries, written out by hand. It is NOT computed
/// from `humaux_domain::memory::facet_for` on purpose: a witness that calls the production
/// mapping cannot notice the production mapping changing.
const FACET_GOLDEN: [(&str, Option<&str>); 12] = [
    ("FACT", None),
    ("PREFERENCE", None),
    ("DECISION", Some("decisions")),
    ("REJECTION", Some("decisions")),
    ("STATE", Some("state")),
    ("ISSUE", Some("issues")),
    ("LESSON", None),
    ("CONSTRAINT", Some("constraints")),
    ("PROCEDURE", None),
    ("OUTCOME", None),
    ("REFERENCE", None),
    ("NOTE", None),
];

fn golden_facet(memory_type: &str) -> Option<&'static str> {
    FACET_GOLDEN
        .into_iter()
        .find(|(label, _)| *label == memory_type)
        .expect("memory_type outside the closed set")
        .1
}

/// A real `coord.tasks` row of this tenant. §25.4.A(7)'s TaskId is RESOLVED
/// (`context_repo::resolve_task_in_txn`) on both the bind write path and the assemble read
/// path, so a fixture task is a row now, not just a fresh uuid.
fn seed_task(handle: &mut Handle, title: &str) -> Uuid {
    handle
        .admin
        .query_one(
            "INSERT INTO coord.tasks(tenant_id,title) VALUES($1,$2) RETURNING task_id",
            &[&handle.tenant_id, &title],
        )
        .expect("owner seeds a coord task")
        .get(0)
}

/// A workspace-shared memory that `authorize_mandatory` can actually grant: `ProjectConstraint`
/// authority with a `TenantAdmin` Evidence origin, which is the one §10.1 ceiling cell that
/// reaches `ProjectConstraint` for any memory type. No binding row — `memory.bind` writes it.
/// card 22c (ADR-0046) positive-control target: a **legitimate** `UserCorrection(4)` memory
/// backed by `UserConfirmed` Evidence — readable, grounded, BehaviorEligible.
///
/// This is the target the ruling names (§二.4 rejects a 5-floor precisely because it would
/// exclude it). Its stored authority stays 4 forever; only a verified task authorization can
/// make ONE task's context instance of it usable at `ExplicitTaskContext`.
fn seed_task_instruction_memory(handle: &mut Handle) -> Uuid {
    // The ruling's own named target: a legitimate `UserConfirmed` origin (BEHAVIOR_ELIGIBLE per
    // §10.1 row 1) stored at `UserCorrection`(4).
    seed_task_instruction_memory_with_origin(handle, "UserConfirmed", "UserCorrection")
}

/// Same shape, with the §10.1 row of the evidence origin (and the stored class it caps) as
/// arguments — `ToolResult`/`PrivateKnowledge` is the DATA_ONLY control.
fn seed_task_instruction_memory_with_origin(
    handle: &mut Handle,
    origin_class: &str,
    authority_class: &str,
) -> Uuid {
    let mut txn = handle
        .admin
        .transaction()
        .expect("begin task-instruction seed");
    let evidence_id: Uuid = txn
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id,evidence_kind,payload_sha256,data_class,origin_class, \
                visibility_class,visibility_workspace_id,reasoning_domain_id) \
             VALUES($1,'EVENT',$2,'INTERNAL',$5,'WORKSPACE_SHARED',$3,$4) \
             RETURNING evidence_id",
            &[
                &handle.tenant_id,
                &Sha256::digest(Uuid::now_v7().as_bytes()).to_vec(),
                &handle.workspace_id,
                &handle.reasoning_domain_id,
                &origin_class,
            ],
        )
        .expect("owner seeds the origin evidence")
        .get(0);
    let confidence: f32 = 0.9;
    let memory_id: Uuid = txn
        .query_one(
            "INSERT INTO private.memory_records \
               (tenant_id,memory_type,content,visibility_class,visibility_workspace_id, \
                authority_class,confidence,status,asserted_at) \
             VALUES($1,'NOTE',$2,'WORKSPACE_SHARED',$3,$5,$4,'active', \
                    clock_timestamp()) RETURNING memory_id",
            &[
                &handle.tenant_id,
                &json!({"fixture": "card 22c task instruction target"}),
                &handle.workspace_id,
                &confidence,
                &authority_class,
            ],
        )
        .expect("owner seeds the target at its ceiling")
        .get(0);
    txn.execute(
        "INSERT INTO private.memory_evidence(memory_id,evidence_id,role,grounding_mode) \
         VALUES($1,$2,'PRIMARY','SNAPSHOT')",
        &[&memory_id, &evidence_id],
    )
    .expect("link evidence");
    txn.commit().expect("commit task-instruction seed");
    memory_id
}

fn seed_bindable_memory(handle: &mut Handle, memory_type: &str) -> Uuid {
    // One transaction: §8.6's DEFERRED `check_memory_has_evidence` trigger fires at COMMIT, so a
    // Memory row committed before its `memory_evidence` link is an orphan and is refused.
    let mut txn = handle
        .admin
        .transaction()
        .expect("begin bindable-memory seed");
    let evidence_id: Uuid = txn
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id,evidence_kind,payload_sha256,data_class,origin_class, \
                visibility_class,visibility_workspace_id,reasoning_domain_id) \
             VALUES($1,'EVENT',$2,'INTERNAL','TenantAdmin','WORKSPACE_SHARED',$3,$4) \
             RETURNING evidence_id",
            &[
                &handle.tenant_id,
                &Sha256::digest(Uuid::now_v7().as_bytes()).to_vec(),
                &handle.workspace_id,
                &handle.reasoning_domain_id,
            ],
        )
        .expect("owner seeds tenant-admin evidence")
        .get(0);
    let confidence: f32 = 0.9;
    let memory_id: Uuid = txn
        .query_one(
            &format!(
                "INSERT INTO private.memory_records \
                   (tenant_id,memory_type,content,visibility_class,visibility_workspace_id, \
                    authority_class,confidence,status,asserted_at) \
                 VALUES($1,'{memory_type}',$2,'WORKSPACE_SHARED',$3,'ProjectConstraint',$4, \
                        'active',clock_timestamp()) RETURNING memory_id"
            ),
            &[
                &handle.tenant_id,
                &json!({"fixture": "card 22b selector witness", "memory_type": memory_type}),
                &handle.workspace_id,
                &confidence,
            ],
        )
        .expect("owner seeds bindable memory")
        .get(0);
    txn.execute(
        "INSERT INTO private.memory_evidence(memory_id,evidence_id,role,grounding_mode) \
         VALUES($1,$2,'PRIMARY','SNAPSHOT')",
        &[&memory_id, &evidence_id],
    )
    .expect("owner links witness memory to evidence");
    txn.commit().expect("commit bindable-memory seed");
    memory_id
}

/// card 22c (ADR-0046): the two `memory.bind` purposes, as wire literals. Only `ADOPT` writes
/// a `private.task_binding_grants` row; `REFERENCE` creates the binding and nothing else.
const ADOPT: &str = "ADOPT_TASK_INSTRUCTION";
const REFERENCE: &str = "REFERENCE_ONLY";

// The four binding ops' full wire shape in one helper: action + memory + task + the two
// optional arguments (`confirm_token`, `replaces_binding_id`). Splitting it would hide which
// call carries which optional argument, which is exactly what the replacement tests assert.
#[allow(clippy::too_many_arguments)]
async fn task_binding_call(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    action: &str,
    memory_id: Uuid,
    task_id: Uuid,
    token: Option<&str>,
    replaces_binding_id: Option<Uuid>,
    // card 22c (ADR-0046): REQUIRED on bind, refused on unbind. `None` on a bind is the
    // INVALID_INPUT control — there is no default purpose.
    purpose: Option<&str>,
) -> (u16, Value) {
    let mut arguments = json!({ "action": action, "memory_id": memory_id, "task_id": task_id });
    if let Some(purpose) = purpose {
        arguments["purpose"] = Value::String(purpose.to_owned());
    }
    if let Some(token) = token {
        arguments["confirm_token"] = Value::String(token.to_owned());
    }
    if let Some(replaced) = replaces_binding_id {
        arguments["replaces_binding_id"] = Value::String(replaced.to_string());
    }
    raw_request(
        address,
        &tool_call_headers("memory", bearer),
        &rpc(request_id, "tools/call", call_params("memory", arguments)),
    )
    .await
}

/// The full two-call confirm flow of `memory.bind`: mint, then execute. Returns the binding id.
async fn bind_through_the_gate(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    memory_id: Uuid,
    task_id: Uuid,
    purpose: &str,
) -> Uuid {
    let (status, minted) = task_binding_call(
        address,
        bearer,
        request_id,
        "bind",
        memory_id,
        task_id,
        None,
        None,
        Some(purpose),
    )
    .await;
    assert_eq!(status, 200, "bind mint: {minted}");
    let structured = assert_tool_response(&minted, ToolName::Memory);
    assert_eq!(structured["confirmation_required"], true, "{minted}");
    assert_eq!(structured["operation"], "memory.bind", "{minted}");
    let token = structured["confirm_token"]
        .as_str()
        .expect("confirm_token string")
        .to_owned();

    let (status, bound) = task_binding_call(
        address,
        bearer,
        request_id + 1,
        "bind",
        memory_id,
        task_id,
        Some(&token),
        None,
        Some(purpose),
    )
    .await;
    assert_eq!(status, 200, "bind execute: {bound}");
    let structured = assert_tool_response(&bound, ToolName::Memory);
    assert_eq!(structured["memory_id"], memory_id.to_string(), "{bound}");
    assert_eq!(structured["mode"], "MANDATORY", "{bound}");
    assert_eq!(structured["state"], "bound", "{bound}");
    assert_eq!(structured["inserted"], true, "{bound}");
    assert_eq!(structured["scope"]["kind"], "TASK", "{bound}");
    assert_eq!(structured["scope"]["id"], task_id.to_string(), "{bound}");
    // card 22c: the write path reports what it wrote, and it must agree with what was asked
    // for on the INSERT path (the idempotent path is allowed to disagree — see the
    // REFERENCE_ONLY-then-ADOPT control in the acceptance test).
    assert_eq!(structured["purpose"], purpose, "{bound}");
    assert_eq!(
        structured["task_authorization"],
        if purpose == "ADOPT_TASK_INSTRUCTION" {
            "granted"
        } else {
            "none"
        },
        "the reported authorization must match what this purpose writes: {bound}"
    );
    Uuid::parse_str(structured["binding_id"].as_str().expect("binding_id")).expect("uuid")
}

/// Per-selector `(nominated, admitted)` id sets, read back **before the lane merges them**
/// through the production registry + selector adapter (`context_repo::selector_outcomes`).
///
/// The merged `MandatoryLane` dedupes by memory identity, so "Facets(W) is exactly these three"
/// is unreadable there — the State row nominated by two selectors survives as one. Ruling §五.3
/// is explicit about why that matters: with only the final union asserted, breaking the STATE
/// arm of the generated expression is masked by another lane putting the same row back.
async fn selector_id_sets(
    dsn: &str,
    tenant_id: Uuid,
    user_id: Uuid,
    workspace_id: Uuid,
    task_id: Option<Uuid>,
) -> BTreeMap<SelectorId, (BTreeSet<Uuid>, BTreeSet<Uuid>)> {
    // The production selector path runs on the request pool, so read it back as the real
    // `role_gateway` (same `options=-c role=X` helper the consolidation witness uses) — a
    // superuser readback would bypass the RLS these selectors depend on.
    let pool = RuntimeDbPool::connect(&dsn_as_role(dsn, "role_gateway"))
        .await
        .expect("runtime pool for the selector readback");
    let authorization = AuthorizationScope::new(
        TenantId(tenant_id),
        PrincipalId::new(),
        Some(UserId(user_id)),
        BoundedSet::new([WorkspaceId(workspace_id)]).expect("bounded workspace grant set"),
    );
    let scope = Scope {
        tenant_id: TenantId(tenant_id),
        user_id: Some(UserId(user_id)),
        workspace_id: Some(WorkspaceId(workspace_id)),
        repository_id: None,
        task_id: task_id.map(TaskId),
        run_id: None,
        agent_id: None,
    };
    let outcomes = humaux_adapters::context_repo::selector_outcomes(&pool, &authorization, &scope)
        .await
        .expect("per-selector readback");
    outcomes
        .into_iter()
        .map(|outcome| match outcome {
            SelectorOutcome::Ran {
                id,
                candidate_ids,
                rows,
                ..
            } => (
                id,
                (
                    candidate_ids.into_iter().map(|m| m.0).collect(),
                    rows.iter().map(|row| row.memory_id().0).collect(),
                ),
            ),
            SelectorOutcome::Unavailable { id, missing_object } => {
                panic!("{id:?} is still column-unavailable ({missing_object}) — card 22b was supposed to close exactly this");
            }
        })
        .collect()
}

/// card 22c (ADR-0046): the task selector's admitted rows with **both** authorities, plus the
/// per-obligation report. Read through the same production path the lane uses.
async fn task_selector_readback(
    dsn: &str,
    tenant_id: Uuid,
    user_id: Uuid,
    workspace_id: Uuid,
    task_id: Uuid,
) -> (
    Vec<(Uuid, AuthorityClass, AuthorityClass)>,
    Vec<humaux_adapters::context_repo::TaskObligationReport>,
) {
    let pool = RuntimeDbPool::connect(&dsn_as_role(dsn, "role_gateway"))
        .await
        .expect("runtime pool for the task readback");
    let authorization = AuthorizationScope::new(
        TenantId(tenant_id),
        PrincipalId::new(),
        Some(UserId(user_id)),
        BoundedSet::new([WorkspaceId(workspace_id)]).expect("bounded workspace grant set"),
    );
    let scope = Scope {
        tenant_id: TenantId(tenant_id),
        user_id: Some(UserId(user_id)),
        workspace_id: Some(WorkspaceId(workspace_id)),
        repository_id: None,
        task_id: Some(TaskId(task_id)),
        run_id: None,
        agent_id: None,
    };
    let outcomes = humaux_adapters::context_repo::selector_outcomes(&pool, &authorization, &scope)
        .await
        .expect("per-selector readback");
    let rows = outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            SelectorOutcome::Ran { id, rows, .. } if *id == SelectorId::TaskExplicitContextV1 => {
                Some(
                    rows.iter()
                        .map(|row| (row.memory_id().0, row.authority(), row.source_authority())),
                )
            }
            _ => None,
        })
        .flatten()
        .collect();
    let report =
        humaux_adapters::context_repo::task_obligation_report(&pool, &authorization, &scope)
            .await
            .expect("obligation report");
    (rows, report)
}

#[test]
#[allow(clippy::too_many_lines)] // One HTTP fixture carries the whole card-22b acceptance.
fn native_mcp_memory_bind_task_and_facet_selector_exact_sets_acceptance() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>(
        "native_mcp_memory_bind_task_and_facet_selectors",
        |mut handle| {
            handle.assert_gateway_login();
            let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("fixture ran, so the DSN is set");
            let prefix = format!("mbind{}", &Uuid::now_v7().simple().to_string()[..11]);
            let wire = format!("{prefix}.{}", "e".repeat(32));
            let credential = handle.seed_synthetic_service_credential_and_window(
                SyntheticCredentialScopes::RememberWriteAndContextRead,
                &prefix,
                &wire,
                &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
                64,
            );

            // Ruling §五.1's fixture. Two DIFFERENT tasks, and a third memory whose type repeats
            // S's: without `state_u`, a selector that filtered by TYPE instead of by TASK would
            // produce the same {S} for Task(T) by luck.
            let task_t = seed_task(&mut handle, "card 22b task T");
            let task_u = seed_task(&mut handle, "card 22b task U");
            let s = seed_bindable_memory(&mut handle, "STATE");
            let d = seed_bindable_memory(&mut handle, "DECISION");
            let state_u = seed_bindable_memory(&mut handle, "STATE");
            // A NOTE row: facet NULL, so it must stay OUT of the facets selector no matter how
            // visible it is. The negative control for "facet IS NOT NULL is actually filtering".
            let note = seed_bindable_memory(&mut handle, "NOTE");

            // The stored facet of each seeded row against the independent GOLDEN, through the real
            // generated column. Mutant `Rejection -> NULL`, or a dropped STATE arm, turns this red.
            tokio::task::block_in_place(|| {
                for (memory_id, memory_type) in [
                    (s, "STATE"),
                    (d, "DECISION"),
                    (state_u, "STATE"),
                    (note, "NOTE"),
                ] {
                    let facet: Option<String> = handle
                        .admin
                        .query_one(
                            "SELECT facet FROM private.memory_records WHERE memory_id=$1",
                            &[&memory_id],
                        )
                        .expect("owner reads the generated facet")
                        .get(0);
                    assert_eq!(
                        facet.as_deref(),
                        golden_facet(memory_type),
                        "generated facet for {memory_type} disagrees with the §78.1 GOLDEN"
                    );
                }
            });

            let runtime_handle = handle.rt.handle().clone();
            let runtime = runtime_handle
                .block_on(handle.fresh_runtime())
                .expect("checked bind runtime");
            let app = application(&handle, runtime);
            runtime_handle.block_on(async {
            let (address, server) = start(app).await;
            let bearer = credential.bearer.as_str();
            let workspace = handle.workspace_id;
            let user_id = handle.user_id;

            // (a) the gate: the first call mints and writes nothing, and a token minted for a
            // DIFFERENT op does not execute a bind. There is no raw INSERT anywhere here.
            let (status, minted) =
                task_binding_call(address, bearer, 1, "bind", s, task_t, None, None, Some(ADOPT)).await;
            assert_eq!(status, 200, "bind mint must be a success-shaped result: {minted}");
            let minted = assert_tool_response(&minted, ToolName::Memory);
            assert_eq!(minted["confirmation_required"], true, "{minted}");
            assert_eq!(minted["operation"], "memory.bind", "{minted}");
            // §25.4.A(7)/(8): the token is bound to the PAIR, so the task is named in target.
            assert_eq!(minted["target"]["memory_id"], s.to_string(), "{minted}");
            assert_eq!(minted["target"]["task_id"], task_t.to_string(), "{minted}");
            let stray = mint_binding_token(address, bearer, 2, "pin", s).await;
            let (status, wrong_op) =
                task_binding_call(address, bearer, 3, "bind", s, task_t, Some(&stray), None, Some(ADOPT)).await;
            assert_eq!(status, 200, "{wrong_op}");
            assert_tool_error(&wrong_op, "CONFLICT");
            // The pair binding, load-bearing: a confirmation minted for (S, T) must not execute
            // (S, U). Dropping `successor_id` from the gate turns this green-by-accident.
            let pair_token = minted["confirm_token"].as_str().expect("token").to_owned();
            let (status, wrong_task) =
                task_binding_call(address, bearer, 4, "bind", s, task_u, Some(&pair_token), None, Some(ADOPT)).await;
            assert_eq!(status, 200, "{wrong_task}");
            assert_tool_error(&wrong_task, "CONFLICT");
            tokio::task::block_in_place(|| {
                let bound: i64 = handle
                    .admin
                    .query_one(
                        "SELECT count(*) FROM private.context_bindings \
                         WHERE tenant_id=$1 AND mode='MANDATORY' AND scope_kind='TASK' \
                           AND revoked_at IS NULL",
                        &[&handle.tenant_id],
                    )
                    .expect("owner counts task bindings")
                    .get(0);
                assert_eq!(bound, 0, "a pin confirmation must not execute a bind");
            });

            // (b) the three bindings, each THROUGH memory.bind's confirm flow.
            let binding_s = bind_through_the_gate(address, bearer, 10, s, task_t, ADOPT).await;
            bind_through_the_gate(address, bearer, 20, d, task_u, ADOPT).await;
            bind_through_the_gate(address, bearer, 30, state_u, task_u, ADOPT).await;

            // (c) per-selector EXACT id sets, before the merge.
            let sets = selector_id_sets(&dsn, handle.tenant_id, user_id, workspace, Some(task_t)).await;

            let facets = sets
                .get(&SelectorId::RequiredCurrentStateFacetsV1)
                .expect("facets selector ran");
            let expected_facets: BTreeSet<Uuid> = [s, d, state_u].into_iter().collect();
            assert_eq!(facets.1, expected_facets, "Facets(W) admitted set");
            assert_eq!(
                facets.0, facets.1,
                "§25.4.A(10): facets nominated == admitted here, so unresolved is empty"
            );
            assert!(!facets.1.contains(&note), "a NULL-facet memory must not be nominated");
            // observed facets, derived from the exact admitted set through the INDEPENDENT
            // golden — §25.4.A(10): the observed set never defines the required set, so this is
            // a readback of what came out, not a completeness claim.
            let observed: BTreeSet<&str> = [(s, "STATE"), (d, "DECISION"), (state_u, "STATE")]
                .into_iter()
                .filter(|(id, _)| facets.1.contains(id))
                .filter_map(|(_, memory_type)| golden_facet(memory_type))
                .collect();
            assert_eq!(
                observed,
                ["state", "decisions"].into_iter().collect::<BTreeSet<&str>>()
            );

            let explicit = sets
                .get(&SelectorId::ExplicitMandatoryBindingsV1)
                .expect("explicit bindings selector ran");
            // THE task filter, load-bearing: task U's two targets are bound MANDATORY and are
            // equally visible, and exactly one of the three comes back. Dropping `scope_id`
            // from the predicate returns all three.
            assert_eq!(
                explicit.1,
                [s].into_iter().collect::<BTreeSet<Uuid>>(),
                "ExplicitMandatoryBindings(T) must carry task T's target only"
            );
            assert_eq!(explicit.0, explicit.1, "no unresolved binding obligations");

            // The task-scoped read with NO task in scope: the same bindings, zero admitted.
            let no_task = selector_id_sets(&dsn, handle.tenant_id, user_id, workspace, None).await;
            assert!(
                no_task[&SelectorId::ExplicitMandatoryBindingsV1].1.is_empty(),
                "a request without a task must not inherit a TASK binding"
            );
            assert_eq!(
                no_task[&SelectorId::RequiredCurrentStateFacetsV1].1, expected_facets,
                "the facets selector is workspace-scoped and does not depend on the task"
            );

            // ===== card 22c (ADR-0046) — the task lane's positive and negative controls =====
            //
            // ADR-0045's "Open debt" assertion lived here: the task selector ran but admitted
            // nothing, because a stored `ExplicitTaskContext` had no producer. Card 22c's
            // ruling says that premise was wrong — 6 is not a content property — so the debt is
            // closed by REWRITING this, not by relaxing it.
            //
            // The binding of `s` above was made with purpose=ADOPT_TASK_INSTRUCTION, so it
            // carries an authorization. `s` is `ProjectConstraint`; the ruling's own named
            // target is a `UserCorrection(4)` memory, so the positive control uses one of those
            // (`adopt`) and the negative control an identical one bound REFERENCE_ONLY
            // (`reference`) — same memory shape, same task, same visibility, one difference.
            // The seed uses the blocking admin client; inside the async body it must run
            // under `block_in_place` like every other seed here, or the sync `postgres`
            // driver tries to start a runtime inside the runtime.
            let adopt = tokio::task::block_in_place(|| seed_task_instruction_memory(&mut handle));
            let reference =
                tokio::task::block_in_place(|| seed_task_instruction_memory(&mut handle));
            let binding_adopt =
                bind_through_the_gate(address, bearer, 60, adopt, task_t, ADOPT).await;
            let binding_reference =
                bind_through_the_gate(address, bearer, 70, reference, task_t, REFERENCE).await;

            let (task_rows, report) =
                task_selector_readback(&dsn, handle.tenant_id, user_id, workspace, task_t).await;

            // N: every obligation of this task is reported, authorized or not. THREE of them —
            // an INNER JOIN on the grants table, or any authority filter in the nominated CTE,
            // makes this 2 and turns "one obligation is unauthorized" into "there is no such
            // obligation" (ruling §六.3).
            let nominated: BTreeSet<Uuid> = report.iter().map(|r| r.memory_id).collect();
            assert_eq!(
                nominated,
                [s, adopt, reference].into_iter().collect::<BTreeSet<Uuid>>(),
                "N must carry every TASK/MANDATORY obligation of this task: {report:?}"
            );
            assert_eq!(report.len(), 3, "one report row per binding: {report:?}");

            // A: exactly the two authorized ones. `reference` is nominated and REJECTED, by name.
            let admitted: BTreeSet<Uuid> = report
                .iter()
                .filter(|r| r.admitted)
                .map(|r| r.memory_id)
                .collect();
            assert_eq!(
                admitted,
                [s, adopt].into_iter().collect::<BTreeSet<Uuid>>(),
                "A must be exactly the obligations with a live authorization: {report:?}"
            );
            let rejected: Vec<_> = report.iter().filter(|r| !r.admitted).collect();
            assert_eq!(rejected.len(), 1, "{report:?}");
            assert_eq!(rejected[0].memory_id, reference);
            assert_eq!(
                rejected[0].reject.map(humaux_domain::context::TaskContextReject::wire),
                Some("MISSING_TASK_AUTHORIZATION"),
                "a binding without an authorization is rejected BY NAME, not filtered away"
            );
            assert_eq!(rejected[0].context_binding_id, binding_reference);

            // R: the admitted rows, with BOTH authorities visible. This is the whole ruling in
            // two assertions — the effective context authority is 6, and the STORED authority is
            // untouched at 4 (I-STORE). A mutant that writes 6 onto the row, or that reports the
            // stored class as the effective one, turns one of these red.
            let by_id: BTreeMap<Uuid, (AuthorityClass, AuthorityClass)> = task_rows
                .iter()
                .map(|(id, effective, source)| (*id, (*effective, *source)))
                .collect();
            assert_eq!(
                by_id.keys().copied().collect::<BTreeSet<Uuid>>(),
                admitted,
                "R == A here (no budget pressure in this fixture)"
            );
            assert_eq!(
                by_id[&adopt],
                (
                    AuthorityClass::ExplicitTaskContext,
                    AuthorityClass::UserCorrection
                ),
                "effective_context_authority = ExplicitTaskContext, source_authority = UserCorrection"
            );
            assert_eq!(by_id[&s].0, AuthorityClass::ExplicitTaskContext);
            assert_eq!(by_id[&s].1, AuthorityClass::ProjectConstraint);

            // I-STORE, read back from the row itself: nothing in this flow raised a stored class.
            tokio::task::block_in_place(|| {
                for (memory_id, expected) in
                    [(adopt, "UserCorrection"), (reference, "UserCorrection"), (s, "ProjectConstraint")]
                {
                    let stored: String = handle
                        .admin
                        .query_one(
                            "SELECT authority_class FROM private.memory_records WHERE memory_id=$1",
                            &[&memory_id],
                        )
                        .expect("owner rereads the stored authority")
                        .get(0);
                    assert_eq!(
                        stored, expected,
                        "I-STORE: a task authorization must not change the stored authority"
                    );
                }
                // And the authorization row itself exists for exactly the ADOPT bindings.
                let granted: i64 = handle
                    .admin
                    .query_one(
                        "SELECT count(*) FROM private.task_binding_grants                          WHERE tenant_id=$1 AND context_binding_id = ANY($2::uuid[])                            AND revoked_at IS NULL",
                        &[&handle.tenant_id, &vec![binding_adopt, binding_reference]],
                    )
                    .expect("owner counts live authorizations")
                    .get(0);
                assert_eq!(granted, 1, "REFERENCE_ONLY must write no authorization");
            });

            // The confirm token covers `purpose` (ADR-0046 D-D): a confirmation minted for
            // REFERENCE_ONLY cannot execute an ADOPT_TASK_INSTRUCTION bind. Without this, the
            // purpose would be a permission-carrying parameter the token does not cover — the
            // exact card-22b review finding.
            let swap_target =
                tokio::task::block_in_place(|| seed_task_instruction_memory(&mut handle));
            let (status, minted_reference) = task_binding_call(
                address, bearer, 80, "bind", swap_target, task_t, None, None, Some(REFERENCE),
            )
            .await;
            assert_eq!(status, 200, "{minted_reference}");
            let reference_token = assert_tool_response(&minted_reference, ToolName::Memory)
                ["confirm_token"]
                .as_str()
                .expect("confirm_token")
                .to_owned();
            let (status, swapped) = task_binding_call(
                address,
                bearer,
                81,
                "bind",
                swap_target,
                task_t,
                Some(&reference_token),
                None,
                Some(ADOPT),
            )
            .await;
            assert_eq!(status, 200, "{swapped}");
            assert_tool_error(&swapped, "CONFLICT");
            // A bind with no purpose at all is INVALID_INPUT — there is no default. The
            // contract itself requires `purpose` on the bind branch (memory.schema.json), so
            // the refusal is §52.1 protocol-level (400 / -32602) before the business arm at
            // `parse_binding_arguments` ever sees it; that arm stays as defence in depth.
            let (status, no_purpose) = task_binding_call(
                address, bearer, 82, "bind", swap_target, task_t, None, None, None,
            )
            .await;
            assert_protocol_invalid_input(status, &no_purpose);
            tokio::task::block_in_place(|| {
                let leaked: i64 = handle
                    .admin
                    .query_one(
                        "SELECT count(*) FROM private.context_bindings                          WHERE tenant_id=$1 AND memory_id=$2 AND revoked_at IS NULL",
                        &[&handle.tenant_id, &swap_target],
                    )
                    .expect("owner counts bindings for the swap target")
                    .get(0);
                assert_eq!(leaked, 0, "a refused bind must write nothing");
            });

            // §10.1 row 4/5 on the WRITE path (ruling §四.4's `require_behavior_eligible_target`,
            // review finding P1). A `ToolResult` origin can never carry BEHAVIOR_ELIGIBLE content,
            // and `PrivateKnowledge` is exactly its ceiling — so it clears `authorize_mandatory`
            // and, before this gate existed, was minted a live ADOPT_TASK_INSTRUCTION grant plus a
            // `UserConfirmed` authorization Evidence. The read side rejected it
            // (UNTRUSTED_INSTRUCTION), but the transaction still committed a durable record
            // asserting the user approved this content AS AN INSTRUCTION. The authorization must
            // not be mintable, not merely unreadable.
            let data_only = tokio::task::block_in_place(|| {
                seed_task_instruction_memory_with_origin(
                    &mut handle,
                    "ToolResult",
                    "PrivateKnowledge",
                )
            });
            let (status, minted_data_only) = task_binding_call(
                address, bearer, 83, "bind", data_only, task_t, None, None, Some(ADOPT),
            )
            .await;
            assert_eq!(status, 200, "{minted_data_only}");
            let data_only_token = assert_tool_response(&minted_data_only, ToolName::Memory)
                ["confirm_token"]
                .as_str()
                .expect("confirm_token")
                .to_owned();
            let (status, refused) = task_binding_call(
                address,
                bearer,
                84,
                "bind",
                data_only,
                task_t,
                Some(&data_only_token),
                None,
                Some(ADOPT),
            )
            .await;
            assert_eq!(status, 403, "a DATA_ONLY target must be refused: {refused}");
            assert_eq!(refused["error"]["data"]["code"], "FORBIDDEN", "{refused}");
            tokio::task::block_in_place(|| {
                let written: i64 = handle
                    .admin
                    .query_one(
                        "SELECT count(*) FROM private.context_bindings b                          LEFT JOIN private.task_binding_grants g                            ON g.tenant_id = b.tenant_id AND g.context_binding_id = b.context_binding_id                          WHERE b.tenant_id=$1 AND b.memory_id=$2",
                        &[&handle.tenant_id, &data_only],
                    )
                    .expect("owner counts what the refused bind left behind")
                    .get(0);
                assert_eq!(
                    written, 0,
                    "no binding, and above all no authorization row, for a DATA_ONLY target"
                );
            });
            // And the gate is scoped to the authorization, not to the bind: the same target
            // still binds REFERENCE_ONLY (that purpose asserts nothing about behaviour
            // eligibility, and the read side rejects the obligation by name anyway).
            let binding_data_only =
                bind_through_the_gate(address, bearer, 85, data_only, task_t, REFERENCE).await;
            let (_, data_only_report) =
                task_selector_readback(&dsn, handle.tenant_id, user_id, workspace, task_t).await;
            let data_only_row = data_only_report
                .iter()
                .find(|r| r.context_binding_id == binding_data_only)
                .expect("the REFERENCE_ONLY obligation is nominated");
            assert!(!data_only_row.admitted, "{data_only_report:?}");
            assert_eq!(
                data_only_row
                    .reject
                    .map(humaux_domain::context::TaskContextReject::wire),
                Some("MISSING_TASK_AUTHORIZATION"),
                "{data_only_report:?}"
            );

            // `memory.unbind` revokes the authorization with the binding (I-NONINHERIT's
            // revocation arm): the obligation disappears and so does its grant.
            let (status, minted) = task_binding_call(
                address, bearer, 90, "unbind", adopt, task_t, None, None, None,
            )
            .await;
            assert_eq!(status, 200, "{minted}");
            let unbind_token = assert_tool_response(&minted, ToolName::Memory)["confirm_token"]
                .as_str()
                .expect("confirm_token")
                .to_owned();
            let (status, unbound) = task_binding_call(
                address, bearer, 91, "unbind", adopt, task_t, Some(&unbind_token), None, None,
            )
            .await;
            assert_eq!(status, 200, "{unbound}");
            tokio::task::block_in_place(|| {
                let live: i64 = handle
                    .admin
                    .query_one(
                        "SELECT count(*) FROM private.task_binding_grants                          WHERE tenant_id=$1 AND context_binding_id=$2 AND revoked_at IS NULL",
                        &[&handle.tenant_id, &binding_adopt],
                    )
                    .expect("owner rereads the authorization")
                    .get(0);
                assert_eq!(live, 0, "unbind must revoke the authorization in the same call");
            });
            let (after_rows, after_report) =
                task_selector_readback(&dsn, handle.tenant_id, user_id, workspace, task_t).await;
            assert!(
                !after_rows.iter().any(|(id, _, _)| *id == adopt),
                "a revoked authorization leaves the lane immediately"
            );
            assert!(
                !after_report.iter().any(|r| r.memory_id == adopt),
                "and its obligation is gone with the binding"
            );

            // (d) the route class. Not `cannot_establish/lane_failed` any more, and the lane
            // names no unavailable selector.
            let (status, assembled) = raw_request(
                address,
                &tool_call_headers("context", bearer),
                &rpc(40, "tools/call", call_params("context", json!({"workspace_id": workspace}))),
            )
            .await;
            assert_eq!(status, 200, "context.assemble: {assembled}");
            let value = assert_tool_response(&assembled, ToolName::Context);
            assert_eq!(
                value["handoff"]["unavailable_selectors"],
                json!([]),
                "all five §25 selectors must run now: {value}"
            );
            assert_ne!(
                value["content"]["completeness"]["reason"], "lane_failed",
                "the lane no longer fails: {value}"
            );
            // Deliberately NOT asserting completeness == complete (ruling §五.3): two selectors
            // behaving is not a completeness proof.

            // (d2) the TASK on the WIRE (card-22b review fix). Before this, `context.assemble`
            // hard-coded `task_id: None` and rejected the argument as unsupported, so the task
            // dimension existed only through the adapter's `selector_outcomes` entry point. Now
            // the same argument the schema always carried reaches the Scope, and §25.4.A(7)'s
            // resolution runs: a uuid that names no task of this tenant is NOT_FOUND, never a
            // silently task-less assemble that would read as "this task has no obligations".
            let (status, with_task) = raw_request(
                address,
                &tool_call_headers("context", bearer),
                &rpc(
                    41,
                    "tools/call",
                    call_params(
                        "context",
                        json!({"workspace_id": workspace, "task_id": task_t}),
                    ),
                ),
            )
            .await;
            assert_eq!(status, 200, "task-scoped assemble: {with_task}");
            let scoped = assert_tool_response(&with_task, ToolName::Context);
            assert_eq!(
                scoped["handoff"]["unavailable_selectors"],
                json!([]),
                "the task-scoped route runs all five selectors too: {scoped}"
            );
            assert_ne!(
                scoped["content"]["completeness"]["reason"], "lane_failed",
                "{scoped}"
            );

            // (d3) card 23 / ADR-0047: an unmet Mandatory obligation moves `completeness`.
            // `reference` above is bound REFERENCE_ONLY, so it is nominated and rejected by
            // name (MISSING_TASK_AUTHORIZATION) — the lane runs and comes back short. Before
            // this the shortfall lived only in `counts.mandatory_missing` while the class still
            // claimed a sound answer, which is why card 22c's own negative control could assert
            // nothing stronger than `reason != "lane_failed"` above.
            //
            // Asserted as the implication in BOTH directions on the live envelope, on both
            // assemble routes: dropping the `mandatory_missing > 0` branch in
            // `retrieval::completeness::classify` reds the first half whenever the live lane is
            // short, and a branch that fires on a full lane reds the second. A one-sided
            // `if missing > 0 { … }` would be a gate that passes by not running.
            for (label, envelope) in [("workspace", &value), ("task-scoped", &scoped)] {
                let missing = envelope["handoff"]["counts"]["mandatory_missing"]
                    .as_u64()
                    .unwrap_or_else(|| panic!("{label}: mandatory_missing must be a number: {envelope}"));
                let class = envelope["content"]["completeness"]["class"].as_str();
                let reason = envelope["content"]["completeness"]["reason"].as_str();
                eprintln!(
                    "card23 witness [{label}]: mandatory_missing={missing} class={class:?} reason={reason:?}"
                );
                if missing > 0 {
                    assert_eq!(
                        (class, reason),
                        (Some("cannot_establish"), Some("mandatory_not_satisfied")),
                        "{label}: {missing} unmet Mandatory obligation(s) but the envelope still                          claims an establishable answer: {envelope}"
                    );
                } else {
                    assert_ne!(
                        reason,
                        Some("mandatory_not_satisfied"),
                        "{label}: nothing is missing, so this reason must not be claimed: {envelope}"
                    );
                }
            }

            let (status, unresolvable) = raw_request(
                address,
                &tool_call_headers("context", bearer),
                &rpc(
                    42,
                    "tools/call",
                    call_params(
                        "context",
                        json!({"workspace_id": workspace, "task_id": Uuid::now_v7()}),
                    ),
                ),
            )
            .await;
            assert_eq!(status, 200, "{unresolvable}");
            assert_tool_error(&unresolvable, "NOT_FOUND");

            // (e) unbind revokes the binding row and nothing else; the selector drops it.
            let (status, minted) =
                task_binding_call(address, bearer, 50, "unbind", s, task_t, None, None, None).await;
            assert_eq!(status, 200, "{minted}");
            let token = assert_tool_response(&minted, ToolName::Memory)["confirm_token"]
                .as_str()
                .expect("confirm_token")
                .to_owned();
            let (status, unbound) =
                task_binding_call(address, bearer, 51, "unbind", s, task_t, Some(&token), None, None).await;
            assert_eq!(status, 200, "{unbound}");
            let structured = assert_tool_response(&unbound, ToolName::Memory);
            assert_eq!(structured["state"], "unbound", "{unbound}");
            assert_eq!(structured["binding_id"], binding_s.to_string(), "{unbound}");
            tokio::task::block_in_place(|| {
                let (status_after, facet_after): (String, Option<String>) = {
                    let row = handle
                        .admin
                        .query_one(
                            "SELECT status, facet FROM private.memory_records WHERE memory_id=$1",
                            &[&s],
                        )
                        .expect("owner rereads the memory");
                    (row.get(0), row.get(1))
                };
                assert_eq!(status_after, "active", "unbind must not touch the Memory");
                assert_eq!(facet_after.as_deref(), Some("state"));
            });
            let after = selector_id_sets(&dsn, handle.tenant_id, user_id, workspace, Some(task_t)).await;
            assert!(
                after[&SelectorId::ExplicitMandatoryBindingsV1].1.is_empty(),
                "a revoked binding must leave the lane immediately"
            );
            assert_eq!(
                after[&SelectorId::RequiredCurrentStateFacetsV1].1, expected_facets,
                "unbind touches the binding only — the type-driven selector is unchanged"
            );

            stop_server(server).await.expect("stop bind server");
        });
        },
    );
}

/// Executes an already-minted `memory.bind` that carries `replaces_binding_id` (§25.4.A(9)).
async fn bind_replacing(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    memory_id: Uuid,
    task_id: Uuid,
    replaced: Uuid,
) -> (u16, Value) {
    let (status, minted) = task_binding_call(
        address,
        bearer,
        request_id,
        "bind",
        memory_id,
        task_id,
        None,
        None,
        Some(ADOPT),
    )
    .await;
    assert_eq!(status, 200, "replacement mint: {minted}");
    let token = assert_tool_response(&minted, ToolName::Memory)["confirm_token"]
        .as_str()
        .expect("confirm_token")
        .to_owned();
    task_binding_call(
        address,
        bearer,
        request_id + 1,
        "bind",
        memory_id,
        task_id,
        Some(&token),
        Some(replaced),
        Some(ADOPT),
    )
    .await
}

/// Whether a binding row is still active, read as the owner.
fn binding_is_active(handle: &mut Handle, binding_id: Uuid) -> bool {
    handle
        .admin
        .query_one(
            "SELECT revoked_at IS NULL FROM private.context_bindings WHERE context_binding_id=$1",
            &[&binding_id],
        )
        .expect("owner reads the binding row")
        .get(0)
}

/// §25.4.A(9) A->B replacement, and the blast radius the argument must NOT have.
///
/// `replaces_binding_id` is the only binding id `memory.bind` takes from the wire, and the
/// confirm token binds (tenant, user, op, memory, task) — not this argument. So the revoke it
/// drives has to be scoped to the same (TASK = this task, MANDATORY) dimension the token does
/// cover. The three negative legs are the regression: before the fix each of them revoked the
/// named row and returned `state: "bound"` — another task's MANDATORY binding, or any PINNED
/// row in the tenant, destroyed by one legitimately-minted bind confirmation.
#[test]
#[allow(clippy::too_many_lines)] // One HTTP fixture carries all seven replacement legs.
fn native_mcp_memory_bind_replacement_is_scoped_to_the_authorized_binding() {
    run_db_fixture::<Fixture, _>("native_mcp_memory_bind_replacement_scope", |mut handle| {
        handle.assert_gateway_login();
        let prefix = format!("mrepl{}", &Uuid::now_v7().simple().to_string()[..11]);
        let wire = format!("{prefix}.{}", "e".repeat(32));
        let credential = handle.seed_synthetic_service_credential_and_window(
            SyntheticCredentialScopes::RememberWriteAndContextRead,
            &prefix,
            &wire,
            &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
            64,
        );
        let task_t = seed_task(&mut handle, "replacement task T");
        let task_u = seed_task(&mut handle, "replacement task U");
        let a = seed_bindable_memory(&mut handle, "STATE");
        let b = seed_bindable_memory(&mut handle, "STATE");
        let c = seed_bindable_memory(&mut handle, "DECISION");
        let victim_u = seed_bindable_memory(&mut handle, "CONSTRAINT");
        let victim_pinned = seed_bindable_memory(&mut handle, "ISSUE");

        let runtime_handle = handle.rt.handle().clone();
        let runtime = runtime_handle
            .block_on(handle.fresh_runtime())
            .expect("checked replacement runtime");
        let app = application(&handle, runtime);
        runtime_handle.block_on(async {
            let (address, server) = start(app).await;
            let bearer = credential.bearer.as_str();

            let binding_a = bind_through_the_gate(address, bearer, 10, a, task_t, ADOPT).await;
            let binding_u =
                bind_through_the_gate(address, bearer, 20, victim_u, task_u, ADOPT).await;
            // A PINNED row of the same tenant, written through memory.pin's own confirm flow.
            let pin_token = mint_binding_token(address, bearer, 30, "pin", victim_pinned).await;
            let (status, pinned) =
                binding_call(address, bearer, 31, "pin", victim_pinned, Some(&pin_token)).await;
            assert_eq!(status, 200, "pin: {pinned}");
            let binding_pinned = Uuid::parse_str(
                assert_tool_response(&pinned, ToolName::Memory)["binding_id"]
                    .as_str()
                    .expect("binding_id"),
            )
            .expect("uuid");

            // (1) another TASK's MANDATORY binding is out of reach.
            let (status, cross_task) =
                bind_replacing(address, bearer, 40, b, task_t, binding_u).await;
            assert_eq!(status, 200, "{cross_task}");
            assert_tool_error(&cross_task, "CONFLICT");
            // (2) a PINNED/WORKSPACE row is out of reach.
            let (status, cross_mode) =
                bind_replacing(address, bearer, 42, b, task_t, binding_pinned).await;
            assert_eq!(status, 200, "{cross_mode}");
            assert_tool_error(&cross_mode, "CONFLICT");
            // (3) a binding id that is not a binding at all.
            let (status, nonexistent) =
                bind_replacing(address, bearer, 44, b, task_t, Uuid::now_v7()).await;
            assert_eq!(status, 200, "{nonexistent}");
            assert_tool_error(&nonexistent, "CONFLICT");
            // (4) replacing a binding with itself would revoke the very row the idempotent
            //     `ReturnExisting` arm reports back as bound.
            let (status, itself) = bind_replacing(address, bearer, 46, a, task_t, binding_a).await;
            assert_eq!(status, 200, "{itself}");
            assert_tool_error(&itself, "CONFLICT");

            tokio::task::block_in_place(|| {
                assert!(
                    binding_is_active(&mut handle, binding_u),
                    "task U's binding survived"
                );
                assert!(
                    binding_is_active(&mut handle, binding_pinned),
                    "the PINNED row survived"
                );
                assert!(
                    binding_is_active(&mut handle, binding_a),
                    "A's binding survived"
                );
                let active: i64 = handle
                    .admin
                    .query_one(
                        "SELECT count(*) FROM private.context_bindings \
                         WHERE tenant_id=$1 AND scope_kind='TASK' AND scope_id=$2 \
                           AND mode='MANDATORY' AND revoked_at IS NULL",
                        &[&handle.tenant_id, &task_t],
                    )
                    .expect("owner counts task T's bindings")
                    .get(0);
                assert_eq!(
                    active, 1,
                    "no refused replacement created B's binding either"
                );
            });

            // (5) the authorized replacement: A revoked and B created in ONE transaction,
            //     exactly one active MANDATORY row left on task T.
            let (status, replaced) =
                bind_replacing(address, bearer, 48, b, task_t, binding_a).await;
            assert_eq!(status, 200, "{replaced}");
            let structured = assert_tool_response(&replaced, ToolName::Memory);
            assert_eq!(structured["state"], "bound", "{replaced}");
            assert_eq!(structured["inserted"], true, "{replaced}");
            assert_eq!(structured["memory_id"], b.to_string(), "{replaced}");
            let binding_b = Uuid::parse_str(structured["binding_id"].as_str().expect("binding_id"))
                .expect("uuid");
            tokio::task::block_in_place(|| {
                assert!(
                    !binding_is_active(&mut handle, binding_a),
                    "A must be revoked"
                );
                assert!(
                    binding_is_active(&mut handle, binding_b),
                    "B must be active"
                );
                let rows: Vec<Uuid> = handle
                    .admin
                    .query(
                        "SELECT context_binding_id FROM private.context_bindings \
                         WHERE tenant_id=$1 AND scope_kind='TASK' AND scope_id=$2 \
                           AND mode='MANDATORY' AND revoked_at IS NULL",
                        &[&handle.tenant_id, &task_t],
                    )
                    .expect("owner lists task T's active bindings")
                    .iter()
                    .map(|row| row.get(0))
                    .collect();
                assert_eq!(rows, vec![binding_b], "exactly one row, and it is B's");
            });

            // (6) replaying the same replacement: A is already revoked, zero rows affected,
            //     so the second call is a Conflict and C never gets bound.
            let (status, replay) = bind_replacing(address, bearer, 50, c, task_t, binding_a).await;
            assert_eq!(status, 200, "{replay}");
            assert_tool_error(&replay, "CONFLICT");
            tokio::task::block_in_place(|| {
                let bound_c: i64 = handle
                    .admin
                    .query_one(
                        "SELECT count(*) FROM private.context_bindings \
                         WHERE tenant_id=$1 AND memory_id=$2 AND revoked_at IS NULL",
                        &[&handle.tenant_id, &c],
                    )
                    .expect("owner counts C's bindings")
                    .get(0);
                assert_eq!(
                    bound_c, 0,
                    "a failed replacement must not create the new binding"
                );
            });

            // (7) §25.4.A(7): a TaskId that resolves to no task of this tenant cannot carry a
            //     binding at all — the confirm flow runs, the write refuses with NOT_FOUND, and
            //     no row is written under the invented scope.
            let ghost_task = Uuid::now_v7();
            let (status, minted) = task_binding_call(
                address,
                bearer,
                52,
                "bind",
                c,
                ghost_task,
                None,
                None,
                Some(ADOPT),
            )
            .await;
            assert_eq!(status, 200, "{minted}");
            let token = assert_tool_response(&minted, ToolName::Memory)["confirm_token"]
                .as_str()
                .expect("confirm_token")
                .to_owned();
            let (status, unresolved) = task_binding_call(
                address,
                bearer,
                53,
                "bind",
                c,
                ghost_task,
                Some(&token),
                None,
                Some(ADOPT),
            )
            .await;
            assert_eq!(status, 200, "{unresolved}");
            assert_tool_error(&unresolved, "NOT_FOUND");
            tokio::task::block_in_place(|| {
                let ghost_rows: i64 = handle
                    .admin
                    .query_one(
                        "SELECT count(*) FROM private.context_bindings \
                         WHERE tenant_id=$1 AND scope_kind='TASK' AND scope_id=$2",
                        &[&handle.tenant_id, &ghost_task],
                    )
                    .expect("owner counts bindings under the ghost task")
                    .get(0);
                assert_eq!(ghost_rows, 0, "no binding under an unresolvable task");
            });

            stop_server(server).await.expect("stop replacement server");
        });
    });
}

// ===========================================================================================
// §6.1.3 subject registry (card 7 D-E1/D-E2) / declaration / read-back / confirm + correct
// linkage (ADR-0028, card 8).
// ===========================================================================================

async fn memory_call(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    args: Value,
) -> (u16, Value) {
    raw_request(
        address,
        &tool_call_headers("memory", bearer),
        &rpc(request_id, "tools/call", call_params("memory", args)),
    )
    .await
}

/// `memory.subject_register` through the real MCP surface (D-E1), asserting the `Subject` result
/// shape; returns the new subject id.
async fn register_subject(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    kind: &str,
    display_name: &str,
    roles: &[&str],
) -> Uuid {
    let (status, response) = memory_call(
        address,
        bearer,
        request_id,
        json!({
            "action": "subject_register",
            "kind": kind,
            "display_name": display_name,
            "roles": roles,
        }),
    )
    .await;
    assert_eq!(status, 200, "{response}");
    let structured = assert_tool_response(&response, ToolName::Memory);
    assert_eq!(structured["kind"], kind, "{response}");
    assert_eq!(structured["display_name"], display_name, "{response}");
    assert_eq!(structured["roles"], json!(roles), "{response}");
    assert_eq!(structured["keys"], json!([]), "{response}");
    Uuid::parse_str(structured["subject_id"].as_str().expect("subject_id")).expect("uuid")
}

/// §52.1: `INVALID_INPUT` is a protocol-level refusal (HTTP 400, JSON-RPC `-32602`) — the same
/// layer every other INVALID_INPUT assertion in this suite reads.
fn assert_protocol_invalid_input(status: u16, response: &Value) {
    assert_eq!(status, 400, "{response}");
    assert_eq!(response["error"]["code"], -32602, "{response}");
    assert_eq!(
        response["error"]["data"]["code"], "INVALID_INPUT",
        "{response}"
    );
    assert!(response.get("result").is_none(), "{response}");
}

async fn remember_with_subjects(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    workspace_id: Uuid,
    content: &str,
    subject_ids: Vec<Uuid>,
    subject_keys: Vec<(&str, &str)>,
) -> (u16, Value) {
    let mut args = json!({
        "operation": "put",
        "content": content,
        "idempotency_key": format!("card8-{}", Uuid::now_v7()),
        "workspace_id": workspace_id,
    });
    if !subject_ids.is_empty() {
        args["subject_ids"] = json!(subject_ids);
    }
    if !subject_keys.is_empty() {
        args["subject_keys"] = json!(
            subject_keys
                .iter()
                .map(|(kind, value)| json!({"kind": kind, "value": value}))
                .collect::<Vec<_>>()
        );
    }
    raw_request(
        address,
        &tool_call_headers("remember", bearer),
        &rpc(request_id, "tools/call", call_params("remember", args)),
    )
    .await
}

fn evidence_declarations(handle: &mut Handle, evidence_id: Uuid) -> Vec<(Uuid, String)> {
    handle
        .admin
        .query(
            "SELECT subject_id, source_kind FROM private.evidence_subjects \
             WHERE evidence_id = $1 ORDER BY source_kind",
            &[&evidence_id],
        )
        .expect("read evidence declarations")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

fn memory_links(handle: &mut Handle, memory_id: Uuid) -> Vec<(Uuid, String)> {
    handle
        .admin
        .query(
            "SELECT subject_id, source_kind FROM private.memory_subjects \
             WHERE memory_id = $1 ORDER BY source_kind",
            &[&memory_id],
        )
        .expect("read memory links")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// Owner-side row counts that a refused write must leave untouched.
fn count_rows(handle: &mut Handle, sql: &str, id: Uuid) -> i64 {
    handle
        .admin
        .query_one(sql, &[&id])
        .expect("owner counts rows")
        .get(0)
}

const EVIDENCE_OF_TENANT: &str =
    "SELECT count(*) FROM private.evidence_objects WHERE tenant_id = $1";
const SUBJECTS_OF_TENANT: &str = "SELECT count(*) FROM private.subjects WHERE tenant_id = $1";
const KEYS_OF_SUBJECT: &str = "SELECT count(*) FROM private.subject_keys WHERE subject_id = $1";
const DECLARATIONS_OF_SUBJECT: &str =
    "SELECT count(*) FROM private.evidence_subjects WHERE subject_id = $1";

fn listed_subject_ids(listed: &Value) -> BTreeSet<String> {
    listed["result"]["structuredContent"]["subjects"]
        .as_array()
        .expect("subjects array")
        .iter()
        .map(|s| s["subject_id"].as_str().expect("id").to_owned())
        .collect()
}

#[test]
#[allow(clippy::too_many_lines)] // One HTTP fixture pair carries the whole card-8 gateway gate.
fn native_mcp_subject_registry_declaration_and_linkage_acceptance() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // Tenant B: its own fixture, credential and server — D-E2's "the other credential sees
    // zero" is proven through a second real MCP surface, never through owner SQL.
    run_db_fixture::<Fixture, _>("native_mcp_subjects_foreign", |mut foreign| {
        foreign.assert_gateway_login();
        let fprefix = format!("mcsf{}", &Uuid::now_v7().simple().to_string()[..12]);
        let fwire = format!("{fprefix}.{}", "d".repeat(32));
        let foreign_credential = foreign.seed_synthetic_service_credential_and_window(
            SyntheticCredentialScopes::RememberWriteAndContextRead,
            &fprefix,
            &fwire,
            &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &fwire),
            96,
        );
        let foreign_rt = foreign.rt.handle().clone();
        let foreign_runtime = foreign_rt
            .block_on(foreign.fresh_runtime())
            .expect("foreign subjects runtime");
        let (foreign_address, foreign_server) =
            foreign_rt.block_on(start(application(&foreign, foreign_runtime)));
        let foreign_bearer = foreign_credential.bearer.clone();
        let foreign_person = foreign_rt.block_on(register_subject(
            foreign_address,
            &foreign_bearer,
            1,
            "PERSON",
            "Grace Hopper",
            &[],
        ));

        run_db_fixture::<Fixture, _>("native_mcp_subjects", |mut handle| {
            handle.assert_gateway_login();
            let prefix = format!("mcsj{}", &Uuid::now_v7().simple().to_string()[..12]);
            let wire = format!("{prefix}.{}", "c".repeat(32));
            let credential = handle.seed_synthetic_service_credential_and_window(
                SyntheticCredentialScopes::RememberWriteAndContextRead,
                &prefix,
                &wire,
                &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
                96,
            );
            let tenant_a = handle.tenant_id;
            let workspace = handle.workspace_id;
            let m = handle.seed_workspace_visible_context_record();
            let body = json!({
                "title": "Renewal",
                "key_claim": "Analytical Engines Ltd renews in Q4.",
            });
            let (candidate_id, sha_hex) = tokio::task::block_in_place(|| {
                seed_pending_candidate(
                    &mut handle,
                    m.evidence_id,
                    "ProjectDecision",
                    "DECISION",
                    &body,
                )
            });

            let runtime_handle = handle.rt.handle().clone();
            let runtime = runtime_handle
                .block_on(handle.fresh_runtime())
                .expect("subjects runtime");
            let app = application(&handle, runtime);
            runtime_handle.block_on(async {
                let (address, server) = start(app).await;
                let bearer = credential.bearer.as_str();

                // 1. Registry through the MCP surface (D-E1): a Person, an Organisation with a
                //    CUSTOMER role, and a CRM key on the organisation.
                let person = register_subject(address, bearer, 1, "PERSON", "Ada Lovelace", &[]).await;
                let org = register_subject(
                    address,
                    bearer,
                    2,
                    "ORGANISATION",
                    "Analytical Engines Ltd",
                    &["CUSTOMER"],
                )
                .await;
                let link_key = |request_id: u64, subject_id: Uuid, kind: &str, value: &str| {
                    memory_call(
                        address,
                        bearer,
                        request_id,
                        json!({
                            "action": "subject_link_key",
                            "subject_id": subject_id,
                            "kind": kind,
                            "value": value,
                        }),
                    )
                };
                let (status, linked) = link_key(3, org, "CRM", "CRM-2001").await;
                assert_eq!(status, 200, "{linked}");
                let structured = assert_tool_response(&linked, ToolName::Memory);
                assert_eq!(structured["subject_id"], org.to_string(), "{linked}");
                assert_eq!(
                    structured["keys"],
                    json!([{"kind": "CRM", "value": "CRM-2001"}]),
                    "{linked}"
                );
                // The same key twice is CONFLICT (0153 UNIQUE(tenant, kind, value)).
                let (status, duplicate) = link_key(4, org, "CRM", "CRM-2001").await;
                assert_eq!(status, 200, "{duplicate}");
                assert_tool_error(&duplicate, "CONFLICT");
                // An unknown key kind never reaches the database.
                let (status, bad_kind) = link_key(5, org, "LDAP", "cn=ada").await;
                assert_protocol_invalid_input(status, &bad_kind);
                // Cross-tenant: tenant B's subject is unknown under tenant A's RLS.
                let (status, foreign_link) = link_key(6, foreign_person, "CRM", "CRM-9999").await;
                assert_protocol_invalid_input(status, &foreign_link);
                tokio::task::block_in_place(|| {
                    assert_eq!(count_rows(&mut handle, KEYS_OF_SUBJECT, org), 1);
                    assert_eq!(count_rows(&mut handle, KEYS_OF_SUBJECT, foreign_person), 0);
                    assert_eq!(count_rows(&mut handle, SUBJECTS_OF_TENANT, tenant_a), 2);
                });

                // 2. Registry read-back scoped to tenant A: exactly its own two, org with key +
                //    role; tenant B's registry is absent under RLS.
                let (status, listed) = enumerate_call(
                    address,
                    bearer,
                    json!({ "action": "enumerate", "subjects": true }),
                )
                .await;
                assert_eq!(status, 200, "{listed}");
                assert_eq!(
                    listed_subject_ids(&listed),
                    BTreeSet::from([person.to_string(), org.to_string()]),
                    "tenant A lists exactly its own registry: {listed}"
                );
                let org_entry = listed["result"]["structuredContent"]["subjects"]
                    .as_array()
                    .expect("subjects")
                    .iter()
                    .find(|s| s["subject_id"] == org.to_string())
                    .expect("org listed")
                    .clone();
                assert_eq!(org_entry["kind"], "ORGANISATION", "{listed}");
                assert_eq!(org_entry["keys"][0]["kind"], "CRM", "{listed}");
                assert_eq!(org_entry["keys"][0]["value"], "CRM-2001", "{listed}");
                assert_eq!(org_entry["roles"][0], "CUSTOMER", "{listed}");

                // 3. remember.put with an explicit id (rule 1) and an exact key (rule 2): the
                //    accepted Evidence carries both declarations, committed with it.
                let evidence_before =
                    tokio::task::block_in_place(|| count_rows(&mut handle, EVIDENCE_OF_TENANT, tenant_a));
                let (status, accepted) = remember_with_subjects(
                    address,
                    bearer,
                    7,
                    workspace,
                    "Ada Lovelace says CRM-2001 renews in Q4.",
                    vec![person],
                    vec![("CRM", "CRM-2001")],
                )
                .await;
                assert_eq!(status, 200, "{accepted}");
                assert_ne!(accepted["result"]["isError"], true, "{accepted}");
                let evidence_id = Uuid::parse_str(
                    accepted["result"]["structuredContent"]["evidence_id"]
                        .as_str()
                        .expect("evidence_id"),
                )
                .expect("uuid");
                tokio::task::block_in_place(|| {
                    assert_eq!(
                        evidence_declarations(&mut handle, evidence_id),
                        vec![
                            (person, "DECLARED".to_owned()),
                            (org, "EXTERNAL_KEY".to_owned())
                        ],
                        "remember.put declaration rows carry the rule that produced them"
                    );
                    assert_eq!(count_rows(&mut handle, EVIDENCE_OF_TENANT, tenant_a), evidence_before + 1);
                });

                // 4. Cross-tenant declaration: tenant B's subject id is unknown under tenant A's
                //    RLS → INVALID_INPUT BEFORE acceptance — no Evidence row, no link, nothing
                //    metered (ruling item 4).
                let (status, denied) = remember_with_subjects(
                    address,
                    bearer,
                    8,
                    workspace,
                    "about someone else's customer",
                    vec![foreign_person],
                    vec![],
                )
                .await;
                assert_protocol_invalid_input(status, &denied);
                // 5. Unknown key: INVALID_INPUT, no Evidence, and NEVER an auto-registered
                //    subject (§6.1.3; the mutation that replaces the reject with an INSERT dies here).
                let (status, unknown_key) = remember_with_subjects(
                    address,
                    bearer,
                    9,
                    workspace,
                    "about an account nobody registered",
                    vec![],
                    vec![("CRM", "CRM-NOPE")],
                )
                .await;
                assert_protocol_invalid_input(status, &unknown_key);
                tokio::task::block_in_place(|| {
                    assert_eq!(
                        count_rows(&mut handle, EVIDENCE_OF_TENANT, tenant_a),
                        evidence_before + 1,
                        "a refused declaration accepts no Evidence"
                    );
                    assert_eq!(count_rows(&mut handle, DECLARATIONS_OF_SUBJECT, foreign_person), 0);
                    assert_eq!(count_rows(&mut handle, SUBJECTS_OF_TENANT, tenant_a), 2);
                    let nope: i64 = handle
                        .admin
                        .query_one(
                            "SELECT count(*) FROM private.subject_keys WHERE key_value = 'CRM-NOPE'",
                            &[],
                        )
                        .expect("count")
                        .get(0);
                    assert_eq!(nope, 0, "an unknown key is never auto-registered");
                });

                // 6. memory.confirm: an unknown key is refused before the token is consumed;
                //    the same token then confirms with an explicit subject (DECLARED).
                let first = confirm_call(address, bearer, 10, candidate_id, &sha_hex, None).await;
                let token = mint_candidate_token("memory.confirm", candidate_id, first);
                let (status, refused) = memory_call(
                    address,
                    bearer,
                    11,
                    json!({
                        "action": "confirm",
                        "candidate_id": candidate_id,
                        "candidate_sha256": sha_hex,
                        "confirm_token": token,
                        "subject_keys": [{"kind": "CRM", "value": "CRM-NOPE"}],
                    }),
                )
                .await;
                assert_protocol_invalid_input(status, &refused);
                tokio::task::block_in_place(|| {
                    assert_eq!(token_consumed(&mut handle, &token), Some(false), "token intact");
                    let state: String = handle
                        .admin
                        .query_one(
                            "SELECT state FROM private.distill_candidates WHERE candidate_id = $1",
                            &[&candidate_id],
                        )
                        .expect("candidate state")
                        .get(0);
                    assert_eq!(state, "PENDING", "nothing written by the refused confirm");
                });
                let (status, confirmed) = memory_call(
                    address,
                    bearer,
                    12,
                    json!({
                        "action": "confirm",
                        "candidate_id": candidate_id,
                        "candidate_sha256": sha_hex,
                        "confirm_token": token,
                        "subject_ids": [org],
                        "subject_keys": [],
                    }),
                )
                .await;
                assert_eq!(status, 200, "{confirmed}");
                assert_ne!(confirmed["result"]["isError"], true, "{confirmed}");
                let structured = &confirmed["result"]["structuredContent"];
                let new_memory = Uuid::parse_str(structured["memory_id"].as_str().expect("memory_id"))
                    .expect("uuid");
                assert_eq!(
                    structured["subject_ids"],
                    json!([org]),
                    "confirm result names the linked subject: {confirmed}"
                );
                tokio::task::block_in_place(|| {
                    assert_eq!(memory_links(&mut handle, new_memory), vec![(org, "DECLARED".to_owned())]);
                });

                // 7. Read side (D-D): memory.get carries the item's subjects; memory.enumerate
                //    accepts an exact subject_id predicate.
                let (status, got) = memory_call(
                    address,
                    bearer,
                    13,
                    json!({ "action": "get", "memory_id": new_memory }),
                )
                .await;
                assert_eq!(status, 200, "{got}");
                let envelope = assert_tool_response(&got, ToolName::Memory);
                assert_eq!(envelope["items"][0]["memory_id"], new_memory.to_string(), "{got}");
                assert_eq!(envelope["items"][0]["subjects"], json!([org]), "{got}");
                for (subject, expected) in [(org, vec![new_memory]), (person, vec![])] {
                    let (status, page) = enumerate_call(
                        address,
                        bearer,
                        json!({ "action": "enumerate", "subject_id": subject }),
                    )
                    .await;
                    assert_eq!(status, 200, "{page}");
                    let content = &assert_tool_response(&page, ToolName::Memory)["content"];
                    let ids: Vec<Uuid> = content["items"]
                        .as_array()
                        .expect("items")
                        .iter()
                        .map(|i| Uuid::parse_str(i["memory_id"].as_str().expect("id")).expect("uuid"))
                        .collect();
                    assert_eq!(ids, expected, "exact subject predicate for {subject}: {page}");
                    for item in content["items"].as_array().expect("items") {
                        assert_eq!(item["subjects"], json!([subject]), "{page}");
                    }
                }

                // 8. memory.correct (ruling item 5): a cross-tenant declaration is refused with
                //    the token intact; the same token then corrects with an explicit subject and
                //    M2 carries M1's link as INHERITED plus the explicit one as DECLARED.
                let correct_token = mint_correct_token(address, bearer, 14, new_memory).await;
                let correct = |request_id: u64, subject_ids: Vec<Uuid>| {
                    memory_call(
                        address,
                        bearer,
                        request_id,
                        json!({
                            "action": "correct",
                            "memory_id": new_memory,
                            "text": "Analytical Engines Ltd renews in Q1 (Ada Lovelace).",
                            "confirm_token": correct_token,
                            "subject_ids": subject_ids,
                        }),
                    )
                };
                let (status, refused) = correct(15, vec![foreign_person]).await;
                assert_protocol_invalid_input(status, &refused);
                tokio::task::block_in_place(|| {
                    assert_eq!(token_consumed(&mut handle, &correct_token), Some(false));
                    assert_eq!(memory_links(&mut handle, new_memory), vec![(org, "DECLARED".to_owned())]);
                });
                let (status, corrected) = correct(16, vec![person]).await;
                assert_eq!(status, 200, "{corrected}");
                assert_ne!(corrected["result"]["isError"], true, "{corrected}");
                let structured = &corrected["result"]["structuredContent"];
                let m2 = Uuid::parse_str(structured["memory_id"].as_str().expect("M2")).expect("uuid");
                let reported: BTreeSet<String> = structured["subject_ids"]
                    .as_array()
                    .expect("subject_ids")
                    .iter()
                    .map(|v| v.as_str().expect("uuid").to_owned())
                    .collect();
                assert_eq!(
                    reported,
                    BTreeSet::from([org.to_string(), person.to_string()]),
                    "correct result names inherited + explicit subjects: {corrected}"
                );
                tokio::task::block_in_place(|| {
                    assert_eq!(
                        memory_links(&mut handle, m2),
                        vec![(person, "DECLARED".to_owned()), (org, "INHERITED".to_owned())],
                        "M2: explicit DECLARED + inherited from M1 via the 0154 trigger"
                    );
                });

                stop_server(server).await.expect("server shutdown");
            });
        });

        // Tenant B, after everything tenant A did: still exactly its own subject.
        let (status, listed) = foreign_rt.block_on(enumerate_call(
            foreign_address,
            &foreign_bearer,
            json!({ "action": "enumerate", "subjects": true }),
        ));
        assert_eq!(status, 200, "{listed}");
        assert_eq!(
            listed_subject_ids(&listed),
            BTreeSet::from([foreign_person.to_string()]),
            "tenant B lists exactly its own registry: {listed}"
        );
        foreign_rt
            .block_on(stop_server(foreign_server))
            .expect("foreign server shutdown");
    });
}

// ===========================================================================================
// §8.5.1 / ADR-0030 (card E1): affect annotation axis through the real MCP surface.
// ===========================================================================================

async fn annotate_call(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    memory_id: Uuid,
    affects: Value,
) -> (u16, Value) {
    memory_call(
        address,
        bearer,
        request_id,
        json!({ "action": "annotate_affect", "memory_id": memory_id, "affects": affects }),
    )
    .await
}

/// `(kind, label, valence_bp, intensity_bp, target_subject_id)` rows for one memory, owner view.
#[allow(clippy::type_complexity)] // test helper returns the full affect witness tuple; a named alias here would only rename the shape the assertions read.
fn affect_rows(
    handle: &mut Handle,
    memory_id: Uuid,
) -> Vec<(String, Option<String>, Option<i16>, i16, Option<Uuid>)> {
    handle
        .admin
        .query(
            "SELECT affect_kind, label, valence_bp, intensity_bp, target_subject_id \
             FROM private.memory_affects WHERE memory_id = $1 ORDER BY created_at, affect_id",
            &[&memory_id],
        )
        .expect("read affect rows")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4)))
        .collect()
}

/// `remember.put {affects}` (ADR-0030 D-C, main-line ruling 2): the declaration rides with the
/// Evidence in remember_in_txn's transaction.
async fn remember_with_affects(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    workspace_id: Uuid,
    content: &str,
    affects: Value,
) -> (u16, Value) {
    let args = json!({
        "operation": "put",
        "content": content,
        "idempotency_key": format!("cardE1-{}", Uuid::now_v7()),
        "workspace_id": workspace_id,
        "affects": affects,
    });
    raw_request(
        address,
        &tool_call_headers("remember", bearer),
        &rpc(request_id, "tools/call", call_params("remember", args)),
    )
    .await
}

/// `(kind, label, valence_bp, intensity_bp, target_subject_id, half_life_seconds)` rows of the
/// 0157 write-side carrier for one Evidence, owner view.
#[allow(clippy::type_complexity)] // test helper returns the full carrier witness tuple.
fn evidence_affect_rows(
    handle: &mut Handle,
    evidence_id: Uuid,
) -> Vec<(
    String,
    Option<String>,
    Option<i16>,
    i16,
    Option<Uuid>,
    Option<i32>,
)> {
    handle
        .admin
        .query(
            "SELECT affect_kind, label, valence_bp, intensity_bp, target_subject_id, half_life_seconds \
             FROM private.evidence_affects WHERE evidence_id = $1 ORDER BY created_at, affect_id",
            &[&evidence_id],
        )
        .expect("read evidence affect rows")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5)))
        .collect()
}

fn affect_by_kind<'a>(affects: &'a [Value], kind: &str) -> &'a Value {
    affects
        .iter()
        .find(|a| a["kind"] == kind)
        .unwrap_or_else(|| panic!("no {kind} annotation in {affects:?}"))
}

#[test]
#[allow(clippy::too_many_lines)] // One HTTP fixture carries the whole card-E1 governance gate.
fn native_mcp_affect_annotation_governance_acceptance() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>("native_mcp_affects", |mut handle| {
        handle.assert_gateway_login();
        let prefix = format!("mcaf{}", &Uuid::now_v7().simple().to_string()[..12]);
        let wire = format!("{prefix}.{}", "e".repeat(32));
        let credential = handle.seed_synthetic_service_credential_and_window(
            SyntheticCredentialScopes::RememberWriteAndContextRead,
            &prefix,
            &wire,
            &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
            96,
        );
        let record = handle.seed_workspace_visible_context_record();
        let m1 = record.memory_id;
        let runtime_handle = handle.rt.handle().clone();
        let runtime = runtime_handle
            .block_on(handle.fresh_runtime())
            .expect("affects runtime");
        let app = application(&handle, runtime);
        runtime_handle.block_on(async {
            let (address, server) = start(app).await;
            let bearer = credential.bearer.as_str();
            let person = register_subject(address, bearer, 1, "PERSON", "Ada Lovelace", &[]).await;

            // 1. annotate: an EMOTION (FRUSTRATION, negative valence, high intensity) about the
            //    person + a MOOD (ANXIETY) observed two half-lives ago.
            let two_half_lives_ago = (time::OffsetDateTime::now_utc() - 2 * MOOD_HALF_LIFE)
                .format(&time::format_description::well_known::Rfc3339)
                .expect("rfc3339");
            let (status, annotated) = annotate_call(
                address,
                bearer,
                2,
                m1,
                json!([
                    {"kind":"EMOTION","label":"FRUSTRATION","valence":-8000,"arousal":5000,
                     "dominance":-4000,"intensity":9000,"confidence":10000,
                     "target_subject_id":person},
                    {"kind":"MOOD","label":"ANXIETY","valence":-3000,"arousal":2000,
                     "intensity":8200,"confidence":7000,"observed_at":two_half_lives_ago},
                ]),
            )
            .await;
            assert_eq!(status, 200, "{annotated}");
            let structured = assert_tool_response(&annotated, ToolName::Memory);
            assert_eq!(structured["memory_id"], m1.to_string(), "{annotated}");
            assert_eq!(structured["affect_ids"].as_array().expect("affect_ids").len(), 2);
            assert!(structured["stream_seq"].is_u64() && structured["commit_seq"].is_u64());
            tokio::task::block_in_place(|| {
                assert_eq!(
                    affect_rows(&mut handle, m1),
                    vec![
                        ("EMOTION".to_owned(), Some("FRUSTRATION".to_owned()), Some(-8000), 9000, Some(person)),
                        ("MOOD".to_owned(), Some("ANXIETY".to_owned()), Some(-3000), 8200, None),
                    ]
                );
            });

            // 2. Read side (D-D): memory.get carries both rows with the read-time effective
            //    intensity — the EMOTION never decays, the MOOD reads at a quarter while the row
            //    keeps its raw value (D-B: decay is derived, never written).
            let (status, got) = memory_call(address, bearer, 3, json!({"action":"get","memory_id":m1})).await;
            assert_eq!(status, 200, "{got}");
            let envelope = assert_tool_response(&got, ToolName::Memory);
            let affects = envelope["items"][0]["affects"].as_array().expect("affects");
            assert_eq!(affects.len(), 2, "{got}");
            let emotion = affect_by_kind(affects, "EMOTION");
            assert_eq!(emotion["label"], "FRUSTRATION");
            assert_eq!(emotion["valence"], -8000);
            assert_eq!(emotion["intensity"], 9000);
            assert_eq!(emotion["effective_intensity"], 9000, "an EMOTION never decays");
            assert_eq!(emotion["target_subject_id"], person.to_string());
            assert_eq!(emotion["evidence_id"], record.evidence_id.to_string());
            assert!(emotion["half_life_seconds"].is_null());
            let mood = affect_by_kind(affects, "MOOD");
            assert_eq!(mood["intensity"], 8200, "raw intensity is the stored fact");
            assert_eq!(mood["effective_intensity"], 2050, "two half-lives: 8200 / 4");
            assert_eq!(mood["valence"], -3000);
            assert_eq!(mood["half_life_seconds"], MOOD_HALF_LIFE.as_secs());
            let (status, page) = enumerate_call(address, bearer, json!({"action":"enumerate"})).await;
            assert_eq!(status, 200, "{page}");
            let content = &assert_tool_response(&page, ToolName::Memory)["content"];
            let listed = content["items"]
                .as_array()
                .expect("items")
                .iter()
                .find(|i| i["memory_id"] == m1.to_string())
                .expect("annotated memory enumerated");
            assert_eq!(listed["affects"].as_array().expect("affects").len(), 2, "{page}");

            // 3. Refusals write nothing: unknown target subject (never auto-registered), a label
            //    or basis-point value outside the closed sets (fail closed, never clamped), an
            //    unknown memory.
            for (affects, label) in [
                (json!([{"kind":"EMOTION","intensity":1,"confidence":1,"target_subject_id":Uuid::new_v4()}]), "unknown subject"),
                (json!([{"kind":"EMOTION","intensity":1,"confidence":1,"target_subject_key":{"kind":"CRM","value":"CRM-NOPE"}}]), "unknown key"),
                (json!([{"kind":"EMOTION","label":"BOREDOM","intensity":1,"confidence":1}]), "label outside the closed set"),
                (json!([{"kind":"MOOD","valence":10001,"intensity":1,"confidence":1}]), "valence over range"),
                (json!([{"kind":"EMOTION","intensity":-1,"confidence":1}]), "negative intensity"),
                (json!([]), "empty list"),
            ] {
                let (status, refused) = annotate_call(address, bearer, 4, m1, affects).await;
                assert_protocol_invalid_input(status, &refused);
                assert!(refused.to_string().contains("INVALID_INPUT"), "{label}: {refused}");
            }
            let (status, missing) = annotate_call(
                address,
                bearer,
                5,
                Uuid::new_v4(),
                json!([{"kind":"EMOTION","intensity":1,"confidence":1}]),
            )
            .await;
            assert_eq!(status, 200, "{missing}");
            assert_tool_error(&missing, "NOT_FOUND");
            tokio::task::block_in_place(|| {
                assert_eq!(affect_rows(&mut handle, m1).len(), 2, "refusals wrote nothing");
                let subjects: i64 = handle
                    .admin
                    .query_one(
                        "SELECT count(*) FROM private.subjects WHERE tenant_id = $1",
                        &[&handle.tenant_id],
                    )
                    .expect("count")
                    .get(0);
                assert_eq!(subjects, 1, "an unknown target subject is never auto-registered");
            });

            // 4. memory.correct re-supplies the new version's affects (D-E) INSIDE
            //    correct_atomically's transaction (main-line ruling 1): an affect whose target
            //    key is unknown is refused before the token is consumed — M1 stays the active
            //    head with its two rows, no M2, the token is intact — and the same token then
            //    corrects: M2 carries only the new CALM row under ONE MEMORY_LIFECYCLE ticket,
            //    M1's two rows ride with the superseded version, G59-4 holds.
            let token = mint_correct_token(address, bearer, 6, m1).await;
            let (status, refused) = memory_call(
                address,
                bearer,
                7,
                json!({
                    "action":"correct","memory_id":m1,"text":"Actually the renewal went fine.",
                    "confirm_token":token,
                    "affects":[{"kind":"EMOTION","intensity":1,"confidence":1,
                                "target_subject_key":{"kind":"CRM","value":"CRM-NOPE"}}],
                }),
            )
            .await;
            assert_protocol_invalid_input(status, &refused);
            tokio::task::block_in_place(|| {
                assert_eq!(token_consumed(&mut handle, &token), Some(false), "token intact");
                assert_eq!(affect_rows(&mut handle, m1).len(), 2);
                let head: (String, Option<Uuid>) = handle
                    .admin
                    .query_one(
                        "SELECT status, superseded_by FROM private.memory_records WHERE memory_id = $1",
                        &[&m1],
                    )
                    .map(|r| (r.get(0), r.get(1)))
                    .expect("M1");
                assert_eq!(head, ("active".to_owned(), None), "no M2 was minted by a refused correction");
            });
            let (status, corrected) = memory_call(
                address,
                bearer,
                7,
                json!({
                    "action":"correct","memory_id":m1,"text":"Actually the renewal went fine.",
                    "confirm_token":token,
                    "affects":[{"kind":"EMOTION","label":"CALM","valence":5000,"arousal":-2000,
                                "intensity":4000,"confidence":9000}],
                }),
            )
            .await;
            assert_eq!(status, 200, "{corrected}");
            assert_ne!(corrected["result"]["isError"], true, "{corrected}");
            let structured = &corrected["result"]["structuredContent"];
            let m2 = Uuid::parse_str(structured["memory_id"].as_str().expect("M2")).expect("uuid");
            let e2 = Uuid::parse_str(structured["evidence_id"].as_str().expect("E2")).expect("uuid");
            assert_eq!(structured["affect_ids"].as_array().expect("affect_ids").len(), 1, "{corrected}");
            tokio::task::block_in_place(|| {
                assert_eq!(
                    affect_rows(&mut handle, m2),
                    vec![("EMOTION".to_owned(), Some("CALM".to_owned()), Some(5000), 4000, None)],
                    "M2 carries exactly the re-supplied affect"
                );
                let tickets: i64 = handle
                    .admin
                    .query_one(
                        "SELECT count(*) FROM ops.outbox WHERE evidence_id = $1 AND event_type = 'MEMORY_LIFECYCLE'",
                        &[&e2],
                    )
                    .expect("tickets")
                    .get(0);
                assert_eq!(tickets, 1, "one correction = one MEMORY_LIFECYCLE ticket, affects included");
                assert_eq!(affect_rows(&mut handle, m1).len(), 2, "M1's rows ride with the superseded version");
                let g59_4: bool = handle
                    .admin
                    .query_one(
                        "SELECT (superseded_by IS NOT NULL) = (status = 'superseded') AND superseded_by = $2 \
                         FROM private.memory_records WHERE memory_id = $1",
                        &[&m1, &m2],
                    )
                    .expect("G59-4")
                    .get(0);
                assert!(g59_4, "G59-4: superseded_by ⇔ status='superseded', naming M2");
            });
            // A superseded version is not the head: annotating it is CONFLICT, nothing written.
            let (status, stale) = annotate_call(
                address,
                bearer,
                8,
                m1,
                json!([{"kind":"EMOTION","intensity":1,"confidence":1}]),
            )
            .await;
            assert_eq!(status, 200, "{stale}");
            assert_tool_error(&stale, "CONFLICT");
            let (status, got2) = memory_call(address, bearer, 9, json!({"action":"get","memory_id":m2})).await;
            assert_eq!(status, 200, "{got2}");
            let affects2 = assert_tool_response(&got2, ToolName::Memory)["items"][0]["affects"]
                .as_array()
                .expect("affects")
                .clone();
            assert_eq!(affects2.len(), 1, "{got2}");
            assert_eq!(affects2[0]["label"], "CALM");

            // 5. remember.put {affects} (D-C, main-line ruling 2): the declaration lands on the
            //    0157 evidence_affects carrier in the Evidence's own transaction (the Distill-born
            //    memory inherits it through the memory_evidence PRIMARY trigger — proven under
            //    role_private_worker in crates/adapters/tests/memory_affects.rs); an unknown
            //    target key refuses the whole put before the Evidence is accepted.
            let (tenant, workspace) = (handle.tenant_id, handle.workspace_id);
            let evidence_before =
                tokio::task::block_in_place(|| count_rows(&mut handle, EVIDENCE_OF_TENANT, tenant));
            let (status, accepted) = remember_with_affects(
                address,
                bearer,
                10,
                workspace,
                "The customer declined the renewal; I was frustrated with Ada.",
                json!([
                    {"kind":"EMOTION","label":"FRUSTRATION","valence":-8000,"arousal":5000,
                     "intensity":9000,"confidence":10000,"target_subject_id":person},
                    {"kind":"MOOD","label":"ANXIETY","valence":-3000,"intensity":8200,"confidence":7000},
                ]),
            )
            .await;
            assert_eq!(status, 200, "{accepted}");
            assert_ne!(accepted["result"]["isError"], true, "{accepted}");
            let declared_evidence = Uuid::parse_str(
                accepted["result"]["structuredContent"]["evidence_id"]
                    .as_str()
                    .expect("evidence_id"),
            )
            .expect("uuid");
            tokio::task::block_in_place(|| {
                assert_eq!(
                    evidence_affect_rows(&mut handle, declared_evidence),
                    vec![
                        ("EMOTION".to_owned(), Some("FRUSTRATION".to_owned()), Some(-8000), 9000, Some(person), None),
                        ("MOOD".to_owned(), Some("ANXIETY".to_owned()), Some(-3000), 8200, None, Some(i32::try_from(MOOD_HALF_LIFE.as_secs()).expect("i32"))),
                    ],
                    "remember.put wrote the carrier rows with the Evidence (MOOD stamped with the write-time half-life)"
                );
                assert_eq!(count_rows(&mut handle, EVIDENCE_OF_TENANT, tenant), evidence_before + 1);
            });
            let (status, refused) = remember_with_affects(
                address,
                bearer,
                11,
                workspace,
                "about an account nobody registered",
                json!([{"kind":"EMOTION","intensity":1,"confidence":1,
                        "target_subject_key":{"kind":"CRM","value":"CRM-NOPE"}}]),
            )
            .await;
            assert_protocol_invalid_input(status, &refused);
            tokio::task::block_in_place(|| {
                assert_eq!(
                    count_rows(&mut handle, EVIDENCE_OF_TENANT, tenant),
                    evidence_before + 1,
                    "a refused affect declaration accepts no Evidence"
                );
            });

            // 6. Subject ERASE (§37) is terminal for the affect about the person — on the memory
            //    row AND on the carrier; the MOOD rows (about nobody) and M2's row survive.
            tokio::task::block_in_place(|| {
                handle
                    .admin
                    .execute("DELETE FROM private.subjects WHERE subject_id = $1", &[&person])
                    .expect("erase subject");
                assert_eq!(
                    affect_rows(&mut handle, m1),
                    vec![("MOOD".to_owned(), Some("ANXIETY".to_owned()), Some(-3000), 8200, None)],
                    "the affect targeting the erased person cascaded"
                );
                assert_eq!(affect_rows(&mut handle, m2).len(), 1);
                assert_eq!(
                    evidence_affect_rows(&mut handle, declared_evidence).len(),
                    1,
                    "the carrier row targeting the erased person cascaded too"
                );
            });

            stop_server(server).await.expect("server shutdown");
        });
    });
}

// ---------------------------------------------------------------------------------------
// ADR-0031 (card 10): one gateway process, N provisioned (tenant, workspace) pairs, the
// stream identity derived per request — never compared against the bootstrap constant and
// never cached process-wide — and admitted only through the family's serving projection.
// ---------------------------------------------------------------------------------------

/// One (tenant, workspace) pair as the multi-pair acceptance drives it.
#[derive(Clone)]
struct StreamPair {
    label: &'static str,
    workspace_id: Uuid,
    bearer: String,
    memory_id: Uuid,
}

/// The four read routes for one bearer / workspace / target, in a fixed order.
const PAIR_READ_ROUTES: [&str; 4] = ["memory.get", "memory.enumerate", "context", "recall"];

/// The MCP tool name and arguments one read route sends for a workspace / target.
fn route_request(
    route: &str,
    workspace_id: Uuid,
    memory_id: Uuid,
    query: &str,
) -> (&'static str, Value) {
    match route {
        "memory.get" => (
            "memory",
            json!({"action":"get","memory_id":memory_id,"workspace_id":workspace_id}),
        ),
        "memory.enumerate" => (
            "memory",
            json!({"action":"enumerate","workspace_id":workspace_id,"limit":100}),
        ),
        "context" => ("context", json!({"workspace_id":workspace_id})),
        _ => (
            "recall",
            json!({"query":query,"workspace_id":workspace_id,"mode":"semantic"}),
        ),
    }
}

async fn tool_call(
    address: SocketAddr,
    tool: &str,
    bearer: &str,
    arguments: Value,
) -> (u16, Value) {
    raw_request(
        address,
        &tool_call_headers(tool, bearer),
        &rpc(1, "tools/call", call_params(tool, arguments)),
    )
    .await
}

/// One read-route call. A 429 here is the guard's per-bucket `pg_try_advisory_xact_lock`
/// refusing a *concurrent* request on the same bucket (§72.2 bounded rate accounting), not a
/// stream/identity outcome — the interleaved leg retries it a bounded number of times so the
/// assertion stays about cross-pair bleed, which a retry can never mask.
async fn route_call(
    address: SocketAddr,
    route: &str,
    bearer: &str,
    workspace_id: Uuid,
    memory_id: Uuid,
    query: &str,
) -> (u16, Value) {
    let mut attempt = 0_u32;
    loop {
        let (tool, arguments) = route_request(route, workspace_id, memory_id, query);
        let response = tool_call(address, tool, bearer, arguments).await;
        if response.0 != 429 || attempt >= 40 {
            return response;
        }
        attempt += 1;
        tokio::time::sleep(Duration::from_millis(20 * u64::from(attempt))).await;
    }
}

async fn pair_reads(
    address: SocketAddr,
    bearer: &str,
    workspace_id: Uuid,
    memory_id: Uuid,
    query: &str,
) -> Vec<(u16, Value)> {
    let mut responses = Vec::with_capacity(PAIR_READ_ROUTES.len());
    for route in PAIR_READ_ROUTES {
        responses.push(route_call(address, route, bearer, workspace_id, memory_id, query).await);
    }
    responses
}

/// Every route succeeded and returned exactly the pair's own memory — nothing from the other
/// pairs, nothing extra.
fn assert_pair_reads(pair: &StreamPair, responses: &[(u16, Value)]) {
    let own = BTreeSet::from([pair.memory_id.to_string()]);
    for (route, (status, response)) in PAIR_READ_ROUTES.iter().zip(responses) {
        assert_eq!(*status, 200, "{} {route}: {response}", pair.label);
        let ids: BTreeSet<String> = match *route {
            "memory.get" => assert_tool_response(response, ToolName::Memory)["items"]
                .as_array()
                .expect("get items")
                .iter()
                .map(|item| item["memory_id"].as_str().expect("memory id").to_owned())
                .collect(),
            "memory.enumerate" => {
                assert_tool_response(response, ToolName::Memory)["content"]["items"]
                    .as_array()
                    .expect("enumerate items")
                    .iter()
                    .map(|item| item["memory_id"].as_str().expect("memory id").to_owned())
                    .collect()
            }
            "context" => assert_tool_response(response, ToolName::Context)["handoff"]["mandatory"]
                .as_array()
                .expect("mandatory ids")
                .iter()
                .map(|row| row["memory_id"].as_str().expect("memory id").to_owned())
                .collect(),
            _ => returned_memory_ids(assert_tool_response(response, ToolName::Recall)),
        };
        assert_eq!(
            ids, own,
            "{} {route} must return only its own memory: {response}",
            pair.label
        );
    }
}

/// Every route answered as a tool error carrying `code` — no envelope, no items.
fn assert_pair_tool_errors(label: &str, responses: &[(u16, Value)], code: &str) {
    for (route, (status, response)) in PAIR_READ_ROUTES.iter().zip(responses) {
        assert_eq!(*status, 200, "{label} {route}: {response}");
        assert_eq!(
            response["result"]["isError"], true,
            "{label} {route}: {response}"
        );
        assert_eq!(
            response["result"]["structuredContent"]["code"], code,
            "{label} {route}: {response}"
        );
    }
}

/// Runs `seed` with the fixture handle temporarily pointed at `workspace_id` — the
/// fixture's owner-side seeders (`seed_synthetic_service_credential`,
/// `seed_workspace_visible_context_record`, the semantic registry/checkpoint rows) all key off
/// `handle.workspace_id`, so this is how a second workspace of the SAME tenant is seeded
/// without a second fixture.
fn with_workspace<R>(
    handle: &mut Handle,
    workspace_id: Uuid,
    seed: impl FnOnce(&mut Handle) -> R,
) -> R {
    let own = std::mem::replace(&mut handle.workspace_id, workspace_id);
    let out = seed(handle);
    handle.workspace_id = own;
    out
}

/// One seeded pair: credential bound to `handle.workspace_id`, one WORKSPACE_SHARED memory
/// with its mandatory binding, its Qdrant point registered, and (when `provisioned`) the
/// family's `v1` serving checkpoint.
fn seed_pair(
    handle: &mut Handle,
    label: &'static str,
    tag: &str,
    provisioned: bool,
) -> (StreamPair, Uuid, humaux_adapters::qdrant::IndexablePayload) {
    let prefix = format!("{tag}{}", &Uuid::now_v7().simple().to_string()[..12]);
    let wire = format!("{prefix}.{}", tag.repeat(32));
    let credential = handle.seed_synthetic_service_credential(
        SyntheticCredentialScopes::RememberWriteAndContextRead,
        &prefix,
        &wire,
        &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
    );
    let record = handle.seed_workspace_visible_context_record();
    let point = Uuid::new_v4();
    let updated = seed_semantic_registry_row(handle, &record, point);
    if provisioned {
        seed_semantic_checkpoint(handle);
    }
    (
        StreamPair {
            label,
            workspace_id: handle.workspace_id,
            bearer: credential.bearer,
            memory_id: record.memory_id,
        },
        point,
        semantic_payload(handle, updated),
    )
}

fn percentile_p50(samples: &mut [Duration]) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

/// Card 10 acceptance gate: ONE gateway process (bootstrap write stream = pair A) serves three
/// distinct provisioned (tenant, workspace) pairs on every read route with strictly disjoint
/// data. Pair B shares pair A's TENANT — the configuration this card newly enables, where the
/// enumerate candidate predicate is tenant-wide and isolation rests on the per-request
/// workspace half of the stream identity (a tenant-level regression cannot mask a workspace
/// one here); pair C is another tenant. Interleaved concurrent requests for all three never
/// bleed; a pair-A credential naming B's or C's workspace is `FORBIDDEN`; a same-tenant
/// memory named under the wrong workspace is `NOT_FOUND`; a same-tenant workspace with
/// membership but no serving projection is `DEPENDENCY_UNAVAILABLE` on all four routes (never
/// a synthetic empty stream, ADR-0031 D-A); a body `tenant_id` is rejected by the closed
/// schemas before dispatch; and the §34.0.1 receipt key pins to (tenant, principal,
/// scope_kind, scope_id, operation, idempotency_key) — no `projection_version` — so pairs'
/// replays never collide (ADR-0031 D-C, ADR-0032 D-C).
#[test]
#[ignore = "lane(a:request_guard) requires the isolated request-guard PostgreSQL fixture, pinned scanner and disposable Qdrant"]
#[allow(clippy::too_many_lines)] // ADR-0031: one live oracle keeps three pairs, four routes, 18 interleaved tasks and the cross-pair/unprovisioned refusals causally ordered against ONE gateway process (same precedent as the real-Qdrant live test).
fn native_mcp_one_process_serves_three_stream_pairs_per_request() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>(
        "native_mcp_one_process_serves_three_stream_pairs_per_request_pair_c",
        |mut pair_c_handle| {
            let _c_cleanup = SemanticProjectionCleanup {
                owner: pair_c_handle.owner_client().expect("pair C cleanup owner"),
                tenant_id: pair_c_handle.tenant_id,
            };
            pair_c_handle.seed_current_entitlement_and_window(400);
            let (pair_c, c_point, c_payload) = seed_pair(&mut pair_c_handle, "pair C", "c", true);
            run_db_fixture::<Fixture, _>(
                "native_mcp_one_process_serves_three_stream_pairs_per_request_pair_a",
                |mut handle| {
                    handle.assert_gateway_login();
                    let _a_cleanup = SemanticProjectionCleanup {
                        owner: handle.owner_client().expect("pair A cleanup owner"),
                        tenant_id: handle.tenant_id,
                    };
                    handle.seed_current_entitlement_and_window(600);
                    let (pair_a, a_point, a_payload) = seed_pair(&mut handle, "pair A", "a", true);
                    // Pair B: the SAME tenant as A, a second workspace with its own credential.
                    let b_workspace = handle.seed_workspace();
                    let (pair_b, b_point, b_payload) =
                        with_workspace(&mut handle, b_workspace, |handle| {
                            seed_pair(handle, "pair B", "b", true)
                        });
                    // Pair D: same tenant, real membership, but NO serving projection — the
                    // unprovisioned control for the serving gate.
                    let d_workspace = handle.seed_workspace();
                    let (pair_d, _d_point, _d_payload) =
                        with_workspace(&mut handle, d_workspace, |handle| {
                            seed_pair(handle, "pair D", "d", false)
                        });
                    assert_eq!(pair_a.workspace_id, handle.workspace_id);
                    assert_ne!(pair_a.workspace_id, pair_b.workspace_id);
                    assert_ne!(handle.tenant_id, pair_c_handle.tenant_id);

                    // ADR-0031 D-C / ADR-0032 D-C: the receipt key is (tenant, principal,
                    // scope_kind, scope_id, operation, idempotency_key) + request_fingerprint;
                    // projection_version is a payload column, never part of the key, so N
                    // pairs per process cannot collide (migration 0158).
                    let key_columns: Vec<String> = handle
                        .admin
                        .query(
                            "SELECT a.attname::text FROM pg_constraint c \
                             CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord) \
                             JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum \
                             WHERE c.conrelid = 'control.operation_receipts'::regclass \
                               AND c.contype = 'p' ORDER BY k.ord",
                            &[],
                        )
                        .expect("receipt primary key columns")
                        .iter()
                        .map(|row| row.get::<_, String>(0))
                        .collect();
                    assert_eq!(
                        key_columns,
                        [
                            "tenant_id",
                            "principal_id",
                            "scope_kind",
                            "scope_id",
                            "operation",
                            "idempotency_key"
                        ],
                        "§34.0.1 receipt key must be per (tenant, principal, stream scope) and carry no projection_version"
                    );

                    // Both tenants are placed in ONE shared collection: disjointness must come
                    // from the per-request tenant + workspace filters, not from physical
                    // separation (A and B are one tenant, so they share the placement too).
                    let cell = CellId(Uuid::now_v7());
                    let registry = semantic_qdrant_registry(
                        cell,
                        CallerId("gateway-multi-pair-acceptance".to_owned()),
                    );
                    let transport = Arc::new(
                        HttpIntraCellTransport::new(
                            registry.clone(),
                            Duration::from_secs(10),
                            humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES,
                        )
                        .expect("semantic Qdrant transport"),
                    );
                    let collection = format!("gateway_multi_pair_{}", Uuid::now_v7().simple());
                    seed_tenant_placement(&mut handle, &collection);
                    seed_tenant_placement(&mut pair_c_handle, &collection);
                    let runtime_handle = handle.rt.handle().clone();
                    let runtime = runtime_handle
                        .block_on(handle.fresh_runtime())
                        .expect("fresh Gateway runtime");
                    let gateway_uid = runtime_handle.block_on(semantic_own_uid());
                    let socket_path = runtime_handle.block_on(spawn_semantic_worker(gateway_uid));
                    let embedding_port: Arc<
                        dyn humaux_application::retrieval_embedding_port::RetrievalEmbeddingPort,
                    > = Arc::new(
                        humaux_gateway::retrieval_embedding_client::GatewayRetrievalEmbeddingClient::new(
                            Arc::new(
                                runtime_handle
                                    .block_on(RuntimeDbPool::connect(
                                        &std::env::var("HUMAUX_GATEWAY_PG_DSN")
                                            .expect("fixture requires HUMAUX_GATEWAY_PG_DSN"),
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
                        embedding_port,
                        transport.clone(),
                        registry.clone(),
                        SemanticRecallVersions {
                            embedding_version: "embed-v1".to_owned(),
                            dimension: 4,
                        },
                        Duration::from_secs(10),
                    )
                    .expect("trusted semantic runtime");
                    // The ONE process: its bootstrap write stream is pair A's.
                    let app = application(&handle, runtime).with_semantic_recall(semantic);
                    let query = "operation receipt scoped context";
                    let pairs = [pair_a.clone(), pair_b.clone(), pair_c.clone()];
                    runtime_handle.block_on(async {
                        create_semantic_collection(&transport, &registry, &collection).await;
                        let permit = authorize_cell_access(
                            &registry,
                            IntraCellResource::QDRANT_REST,
                            Duration::from_secs(60),
                        )
                        .expect("semantic upsert permit");
                        let vector = semantic_vector(query);
                        upsert(
                            transport.as_ref(),
                            &permit,
                            &collection,
                            &[
                                (PointId::Uuid(a_point), &a_payload, vector.clone()),
                                (PointId::Uuid(b_point), &b_payload, vector.clone()),
                                (PointId::Uuid(c_point), &c_payload, vector),
                            ],
                            ha_profile_for(QdrantOperation::NormalImmutableUpsert),
                        )
                        .await
                        .expect("real Qdrant points for all pairs");
                        let (address, server) = start(app).await;

                        // Sequential sanity for each pair, plus p50 for the card's speed record
                        // (uncontended: no 429 retry ever fires here).
                        for pair in &pairs {
                            assert_pair_reads(
                                pair,
                                &pair_reads(
                                    address,
                                    &pair.bearer,
                                    pair.workspace_id,
                                    pair.memory_id,
                                    query,
                                )
                                .await,
                            );
                            for route in PAIR_READ_ROUTES {
                                let mut samples = Vec::new();
                                for _ in 0..5 {
                                    let started = Instant::now();
                                    let (status, response) = route_call(
                                        address,
                                        route,
                                        &pair.bearer,
                                        pair.workspace_id,
                                        pair.memory_id,
                                        query,
                                    )
                                    .await;
                                    assert_eq!(status, 200, "{} {route}: {response}", pair.label);
                                    samples.push(started.elapsed());
                                }
                                eprintln!(
                                    "card10 p50 {} {route}: {:?} (n={})",
                                    pair.label,
                                    percentile_p50(&mut samples),
                                    samples.len()
                                );
                            }
                        }

                        // Interleaved: all pairs, all four routes, concurrently on the one
                        // process. A process-wide (rather than per-request) StreamKey would
                        // serve some of these requests under another pair's identity — for
                        // pair B that is caught by the WORKSPACE half alone (same tenant).
                        let mut tasks = JoinSet::new();
                        for round in 0..6_u8 {
                            for pair in pairs.iter().cloned() {
                                tasks.spawn(async move {
                                    let responses = pair_reads(
                                        address,
                                        &pair.bearer,
                                        pair.workspace_id,
                                        pair.memory_id,
                                        query,
                                    )
                                    .await;
                                    (round, pair, responses)
                                });
                            }
                        }
                        let mut completed = 0;
                        while let Some(joined) = tasks.join_next().await {
                            let (round, pair, responses) = joined.expect("interleaved read task");
                            assert_pair_reads(&pair, &responses);
                            completed += 1;
                            let _ = round;
                        }
                        assert_eq!(completed, 18, "every interleaved task must report");

                        // Cross-pair: pair A's credential naming another pair's workspace —
                        // same tenant (B) or not (C) — is refused by membership narrowing on
                        // every route, before any stream/object read.
                        for other in [&pair_b, &pair_c] {
                            for (route, (status, response)) in PAIR_READ_ROUTES.iter().zip(
                                pair_reads(
                                    address,
                                    &pair_a.bearer,
                                    other.workspace_id,
                                    other.memory_id,
                                    query,
                                )
                                .await,
                            ) {
                                assert_eq!(status, 403, "A→{} {route}: {response}", other.label);
                                assert_eq!(
                                    response["error"]["data"]["code"], "FORBIDDEN",
                                    "A→{} {route}: {response}",
                                    other.label
                                );
                            }
                        }
                        // ... and another pair's memory named under pair A's own workspace
                        // stays an invisible target (NOT_FOUND, never an existence oracle) —
                        // for B this is the same-tenant case the tenant filter cannot catch.
                        for other in [&pair_b, &pair_c] {
                            let (status, response) = tool_call(
                                address,
                                "memory",
                                &pair_a.bearer,
                                json!({"action":"get","memory_id":other.memory_id,"workspace_id":pair_a.workspace_id}),
                            )
                            .await;
                            assert_eq!(
                                status, 200,
                                "cross-pair get ({}) is a tool error: {response}",
                                other.label
                            );
                            assert_memory_not_found(&response);
                        }
                        // Pair D: membership and a real credential, but the family has no
                        // serving projection — every route fails closed, none fabricates a
                        // complete ledger over the tenant's rows.
                        assert_pair_tool_errors(
                            "unprovisioned same-tenant pair D",
                            &pair_reads(
                                address,
                                &pair_d.bearer,
                                pair_d.workspace_id,
                                pair_d.memory_id,
                                query,
                            )
                            .await,
                            "DEPENDENCY_UNAVAILABLE",
                        );
                        // A workspace outside the credential's membership on any route still
                        // fails closed (403).
                        let unprovisioned = Uuid::now_v7();
                        for (route, (status, response)) in PAIR_READ_ROUTES.iter().zip(
                            pair_reads(
                                address,
                                &pair_b.bearer,
                                unprovisioned,
                                pair_b.memory_id,
                                query,
                            )
                            .await,
                        ) {
                            assert_eq!(status, 403, "non-member workspace {route}: {response}");
                        }
                        // The tenant is the principal's, never an argument: a body `tenant_id`
                        // (pair C's) dies at the closed schema before any dispatch could read it.
                        for route in PAIR_READ_ROUTES {
                            let (tool, mut arguments) =
                                route_request(route, pair_a.workspace_id, pair_a.memory_id, query);
                            arguments["tenant_id"] = json!(pair_c_handle.tenant_id);
                            let (status, response) =
                                tool_call(address, tool, &pair_a.bearer, arguments).await;
                            assert_eq!(status, 400, "body tenant_id {route}: {response}");
                            assert_eq!(
                                response["error"]["data"]["code"], "INVALID_INPUT",
                                "body tenant_id {route}: {response}"
                            );
                        }

                        stop_server(server).await.expect("stop multi-pair server");
                        delete_semantic_collection(&transport, &registry, &collection).await;
                    });
                },
            );
        },
    );
}

/// Card 11 speed record (运行速度快 is an acceptance goal): `remember.put` p50 on the plain
/// wire shape, uncontended, printed for the ADR's before/after table. No assertion beyond
/// every call committing.
#[test]
fn native_mcp_remember_put_p50_speed_record() {
    run_db_fixture::<Fixture, _>("native_mcp_remember_put_p50_speed_record", |mut handle| {
        handle.assert_gateway_login();
        let prefix = format!("p50{}", &Uuid::now_v7().simple().to_string()[..12]);
        let wire = format!("{prefix}.{}", "e".repeat(32));
        let credential = handle.seed_synthetic_service_credential_and_window(
            SyntheticCredentialScopes::RememberWrite,
            &prefix,
            &wire,
            &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
            64,
        );
        let runtime_handle = handle.rt.handle().clone();
        let runtime = runtime_handle
            .block_on(handle.fresh_runtime())
            .expect("fresh checked gateway pool");
        let app = application(&handle, runtime);
        let workspace = handle.workspace_id;
        runtime_handle.block_on(async {
            let (address, server) = start(app).await;
            let mut samples = Vec::new();
            for n in 0..9_u8 {
                let started = Instant::now();
                let (status, response) = tool_call(
                    address,
                    "remember",
                    &credential.bearer,
                    json!({
                        "operation": "put",
                        "content": format!("speed record {n}"),
                        "idempotency_key": format!("p50-{n}-{}", Uuid::now_v7()),
                        "workspace_id": workspace,
                    }),
                )
                .await;
                samples.push(started.elapsed());
                assert_eq!(status, 200, "{response}");
                assert_ne!(response["result"]["isError"], true, "{response}");
            }
            eprintln!(
                "card11 p50 remember.put: {:?} (n={})",
                percentile_p50(&mut samples),
                samples.len()
            );
            stop_server(server).await.expect("stop p50 server");
        });
    });
}

/// One evidence row's classification as stored, read back by the owner (RLS bypass) so the
/// assertion is about what was written, not about what the caller may read.
fn stored_evidence(
    handle: &mut Handle,
    evidence_id: Uuid,
) -> (String, Option<Uuid>, Option<Uuid>, String, String) {
    let row = handle
        .admin
        .query_one(
            "SELECT e.visibility_class, e.visibility_user_id, e.visibility_workspace_id, e.data_class, ev.event_kind \
             FROM private.evidence_objects e JOIN private.events ev ON ev.event_id = e.evidence_id \
             WHERE e.evidence_id = $1 AND e.tenant_id = $2",
            &[&evidence_id, &handle.tenant_id],
        )
        .expect("owner reads the stored evidence classification");
    (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4))
}

/// `(issued_highwater, stream_log rows, outbox tickets)` of one workspace's `v1` stream.
fn stream_ledger(handle: &mut Handle, workspace_id: Uuid) -> (i64, i64, i64) {
    let row = handle
        .admin
        .query_one(
            "SELECT \
               coalesce((SELECT issued_highwater FROM projection.stream_checkpoints \
                 WHERE tenant_id=$1 AND scope_kind='workspace' AND scope_id=$2 \
                   AND domain='knowledge' AND projection_kind='ingest' AND projection_version='v1'), 0), \
               (SELECT count(*) FROM projection.stream_log \
                 WHERE tenant_id=$1 AND scope_kind='workspace' AND scope_id=$2 \
                   AND domain='knowledge' AND projection_kind='ingest' AND projection_version='v1'), \
               (SELECT count(*) FROM ops.outbox o JOIN projection.stream_log s \
                   ON s.tenant_id=o.tenant_id AND s.commit_seq=o.commit_seq \
                 WHERE o.tenant_id=$1 AND o.event_type='EVIDENCE_ACCEPTED' \
                   AND s.scope_kind='workspace' AND s.scope_id=$2)",
            &[&handle.tenant_id, &workspace_id],
        )
        .expect("owner reads the stream ledger");
    (row.get(0), row.get(1), row.get(2))
}

/// One evidence row's `reasoning_domain_id` as stored, read back by the owner (RLS bypass).
fn stored_reasoning_domain(handle: &mut Handle, evidence_id: Uuid) -> Uuid {
    handle
        .admin
        .query_one(
            "SELECT reasoning_domain_id FROM private.evidence_objects \
             WHERE evidence_id = $1 AND tenant_id = $2",
            &[&evidence_id, &handle.tenant_id],
        )
        .expect("owner reads the stored reasoning domain")
        .get(0)
}

/// The §34.0.1 receipt-transaction inputs for a direct `remember_atomically` call on
/// `workspace_id`'s stream, mirroring what `remember::command` + the guard build for one
/// `remember.put` (the adapter receipt fixture's shape).
fn direct_receipt_request(
    handle: &Handle,
    principal: Uuid,
    workspace_id: Uuid,
    key: &str,
    content: &str,
) -> AtomicRememberRequest {
    let request_id = Uuid::new_v4();
    let mut metadata = AuditMetadata::new();
    metadata
        .insert("role", "member")
        .expect("allowlisted audit metadata");
    AtomicRememberRequest {
        request_id,
        idempotency_key: key.into(),
        request_fingerprint: hex::encode(Sha256::digest(content.as_bytes())),
        workspace_id: Some(WorkspaceId(workspace_id)),
        reservation_ttl: Duration::from_secs(30),
        replay_ttl: Duration::from_secs(60),
        command: humaux_adapters::remember::RememberCommand {
            tenant_id: handle.tenant_id,
            authorization_user_id: Some(handle.user_id),
            scope_kind: "workspace".into(),
            scope_id: workspace_id,
            domain: "knowledge".into(),
            projection_kind: "ingest".into(),
            projection_version: "v1".into(),
            consistency_token_expires_at: time::OffsetDateTime::now_utc() + Duration::from_secs(45),
            batch_id: None,
            payload_sha256: payload_sha256(content.as_bytes()),
            data_class: "INTERNAL".into(),
            origin_class: EvidenceOriginClass::AuthenticatedAgent,
            origin_principal_id: Some(principal),
            origin_connector_id: None,
            visibility_class: "WORKSPACE_SHARED".into(),
            visibility_user_id: None,
            visibility_workspace_id: Some(workspace_id),
            reasoning_domain_id: handle.reasoning_domain_id,
            occurred_at: None,
            event_kind: "USER_MESSAGE".into(),
            event_payload: json!({"content": content}),
            subjects: humaux_domain::subject::SubjectDeclaration::default(),
            affects: Vec::new(),
            mood_half_life: None,
        },
        finished_audit: AuditEvent {
            event_id: AuditEventId::new(),
            ts: std::time::SystemTime::now(),
            tenant_id: TenantId(handle.tenant_id),
            actor_type: "user".into(),
            actor_id: principal.to_string(),
            action: McpAuditAction::McpRequestFinished.as_str().into(),
            resource_type: "mcp".into(),
            resource_id: "remember.put".into(),
            result: "OK".into(),
            request_id: request_id.to_string(),
            trace_id: format!("card11-{request_id}"),
            client_ip: "127.0.0.1".into(),
            user_agent_hash: "card11-fixture".into(),
            risk_tags: vec!["fixture".into()],
            before_fingerprint: None,
            after_fingerprint: None,
            metadata,
        },
    }
}

/// Card 11 acceptance gate (ADR-0032): ONE gateway process (bootstrap write pair = A).
/// D-B — two `remember.put` calls in the same session land `USER_PRIVATE` and
/// `WORKSPACE_SHARED` rows (per-call `data_class` / `event_kind` too, defaults when absent); a
/// third asking for `TENANT_SHARED` as a plain member is `INVALID_INPUT` with nothing written
/// or metered, and lands with NULL user/workspace once the member is an owner (promoted with
/// the lower-case spelling: 0160 canonicalizes the closed role set, so the gate is never a
/// silent deny for a writer's spelling; a value outside the set is a CHECK violation); an
/// unknown class dies at the closed schema. D-A — a second (same-tenant) workspace's
/// credential writes to ITS stream: the family's checkpoint row is created by that first
/// write, its stream_seq and outbox ticket are issued against B, A's ledger is untouched;
/// credential A naming B is `FORBIDDEN`; and pair C — ANOTHER tenant on the same process —
/// fails closed (`DEPENDENCY_UNAVAILABLE`, nothing written) until its user owns a reasoning
/// domain, then lands on C's stream under C's OWN reasoning domain (never the process's boot
/// constant, §11.2.1), with A's and B's ledgers untouched, and the 0159 composite FK refuses a
/// cross-tenant domain even for a writer that bypasses the resolve. D-C — the same principal,
/// the same `idempotency_key`, two streams, CONCURRENTLY: two committed writes (never a
/// `CONFLICT`), each replayable by its own stream; dropping the scope from the receipt key
/// makes this leg red. Plus the p50 record.
#[test]
#[allow(clippy::too_many_lines)] // one live oracle keeps D-A (three pairs, two tenants), D-B and D-C causally ordered against ONE process (same precedent as the replay acceptance)
fn native_mcp_remember_put_per_call_visibility_and_per_request_stream() {
    run_db_fixture::<Fixture, _>(
        "native_mcp_remember_put_per_call_visibility_and_per_request_stream_tenant_c",
        |mut tenant_c| {
            // Pair C: another tenant with its own workspace, credential and reasoning domain,
            // served by the process built on tenant A below (same nesting as the ADR-0031
            // three-pair gate).
            let prefix_c = format!("vcc{}", &Uuid::now_v7().simple().to_string()[..12]);
            let wire_c = format!("{prefix_c}.{}", "c".repeat(32));
            let credential_c = tenant_c.seed_synthetic_service_credential_and_window(
                SyntheticCredentialScopes::RememberWrite,
                &prefix_c,
                &wire_c,
                &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire_c),
                64,
            );
            let workspace_c = tenant_c.workspace_id;
            run_db_fixture::<Fixture, _>(
                "native_mcp_remember_put_per_call_visibility_and_per_request_stream",
                |mut handle| {
                    assert_ne!(handle.tenant_id, tenant_c.tenant_id);
                    assert_ne!(handle.reasoning_domain_id, tenant_c.reasoning_domain_id);
                    handle.assert_gateway_login();
                    let prefix_a = format!("vca{}", &Uuid::now_v7().simple().to_string()[..12]);
                    let wire_a = format!("{prefix_a}.{}", "a".repeat(32));
                    let credential_a = handle.seed_synthetic_service_credential_and_window(
                        SyntheticCredentialScopes::RememberWrite,
                        &prefix_a,
                        &wire_a,
                        &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire_a),
                        64,
                    );
                    let workspace_a = handle.workspace_id;
                    // Pair B: the SAME tenant, a second workspace, its own credential bound to it.
                    let workspace_b = handle.seed_workspace();
                    let prefix_b = format!("vcb{}", &Uuid::now_v7().simple().to_string()[..12]);
                    let wire_b = format!("{prefix_b}.{}", "b".repeat(32));
                    let credential_b = with_workspace(&mut handle, workspace_b, |handle| {
                        handle.seed_synthetic_service_credential(
                            SyntheticCredentialScopes::RememberWrite,
                            &prefix_b,
                            &wire_b,
                            &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire_b),
                        )
                    });
                    let runtime_handle = handle.rt.handle().clone();
                    let runtime = runtime_handle
                        .block_on(handle.fresh_runtime())
                        .expect("fresh checked gateway pool");
                    let direct_pool = runtime_handle
                        .block_on(handle.fresh_runtime())
                        .expect("second checked gateway pool for the direct receipt leg");
                    let app = application(&handle, runtime);
                    assert_eq!(
                        stream_ledger(&mut handle, workspace_a),
                        (0, 0, 0),
                        "pair A starts provisioned and empty"
                    );
                    let user_id = handle.user_id;
                    let tenant_id = handle.tenant_id;

                    runtime_handle.block_on(async {
                let (address, server) = start(app).await;
                let put = |bearer: String, arguments: Value| async move {
                    tool_call(address, "remember", &bearer, arguments).await
                };
                let accepted = |response: &Value| -> Uuid {
                    assert_ne!(response["result"]["isError"], true, "{response}");
                    let content = &response["result"]["structuredContent"];
                    assert_eq!(content["replayed"], false, "{response}");
                    Uuid::parse_str(content["evidence_id"].as_str().expect("evidence_id"))
                        .expect("uuid evidence_id")
                };

                // ---- D-B: per-call classification in one session ----
                let (status, private) = put(
                    credential_a.bearer.clone(),
                    json!({
                        "operation": "put", "content": "card11 private note",
                        "idempotency_key": format!("c11-private-{}", Uuid::now_v7()),
                        "workspace_id": workspace_a,
                        "visibility_class": "USER_PRIVATE", "data_class": "PRIVATE",
                        "event_kind": "MANUAL_NOTE",
                    }),
                )
                .await;
                assert_eq!(status, 200, "{private}");
                let private_evidence = accepted(&private);
                let (status, shared) = put(
                    credential_a.bearer.clone(),
                    json!({
                        "operation": "put", "content": "card11 shared note",
                        "idempotency_key": format!("c11-shared-{}", Uuid::now_v7()),
                        "workspace_id": workspace_a,
                        "visibility_class": "WORKSPACE_SHARED",
                    }),
                )
                .await;
                assert_eq!(status, 200, "{shared}");
                let shared_evidence = accepted(&shared);
                // Defaults when absent: the process env (WORKSPACE_SHARED / INTERNAL / USER_MESSAGE).
                let (status, defaulted) = put(
                    credential_a.bearer.clone(),
                    json!({
                        "operation": "put", "content": "card11 defaulted note",
                        "idempotency_key": format!("c11-default-{}", Uuid::now_v7()),
                        "workspace_id": workspace_a,
                    }),
                )
                .await;
                assert_eq!(status, 200, "{defaulted}");
                let defaulted_evidence = accepted(&defaulted);
                let before_refusals = blocking_counts(&mut handle);
                // A plain member may not widen to the tenant audience: INVALID_INPUT (the
                // §52 invalid-params mapping, judged inside the receipt transaction), nothing
                // written or metered — the failure audit row is the only durable trace.
                let (status, refused) = put(
                    credential_a.bearer.clone(),
                    json!({
                        "operation": "put", "content": "card11 tenant note as member",
                        "idempotency_key": format!("c11-tenant-member-{}", Uuid::now_v7()),
                        "workspace_id": workspace_a,
                        "visibility_class": "TENANT_SHARED",
                    }),
                )
                .await;
                assert_eq!(status, 400, "{refused}");
                assert_eq!(
                    refused["error"]["data"]["code"], "INVALID_INPUT",
                    "member TENANT_SHARED: {refused}"
                );
                // Outside the closed set: the catalog schema refuses it before dispatch.
                let (status, unknown) = put(
                    credential_a.bearer.clone(),
                    json!({
                        "operation": "put", "content": "card11 unknown class",
                        "idempotency_key": format!("c11-unknown-{}", Uuid::now_v7()),
                        "workspace_id": workspace_a,
                        "visibility_class": "EVERYONE",
                    }),
                )
                .await;
                assert_eq!(status, 400, "{unknown}");
                assert_eq!(
                    unknown["error"]["data"]["code"], "INVALID_INPUT",
                    "{unknown}"
                );
                assert_eq!(
                    durable_counts(blocking_counts(&mut handle)),
                    durable_counts(before_refusals),
                    "refused puts leave no Evidence, receipt or consumed reservation"
                );
                // OWNER: the tenant audience is reachable, bound to neither user nor workspace.
                // Promoted with the lower-case spelling the older seeds use: 0160 folds it to
                // the canonical `OWNER` the gate compares against, and a value outside the
                // closed set is refused by the CHECK (so the role column is a closed set the
                // gate can be exact against, §78.2).
                tokio::task::block_in_place(|| {
                    handle
                        .admin
                        .execute(
                            "UPDATE control.memberships SET role='owner' WHERE tenant_id=$1 AND user_id=$2",
                            &[&tenant_id, &user_id],
                        )
                        .expect("owner promotes the fixture member");
                    let stored: String = handle
                        .admin
                        .query_one(
                            "SELECT role FROM control.memberships WHERE tenant_id=$1 AND user_id=$2",
                            &[&tenant_id, &user_id],
                        )
                        .expect("owner reads the promoted role")
                        .get(0);
                    assert_eq!(stored, "OWNER", "0160 canonicalizes the role spelling on write");
                    let outside = handle.admin.execute(
                        "UPDATE control.memberships SET role='viewer' WHERE tenant_id=$1 AND user_id=$2",
                        &[&tenant_id, &user_id],
                    );
                    assert_eq!(
                        outside
                            .expect_err("a role outside the closed set is a CHECK violation")
                            .code(),
                        Some(&postgres::error::SqlState::CHECK_VIOLATION)
                    );
                });
                let (status, tenant) = put(
                    credential_a.bearer.clone(),
                    json!({
                        "operation": "put", "content": "card11 tenant note as owner",
                        "idempotency_key": format!("c11-tenant-owner-{}", Uuid::now_v7()),
                        "workspace_id": workspace_a,
                        "visibility_class": "TENANT_SHARED",
                    }),
                )
                .await;
                assert_eq!(status, 200, "{tenant}");
                let tenant_evidence = accepted(&tenant);
                tokio::task::block_in_place(|| {
                    assert_eq!(
                        stored_evidence(&mut handle, private_evidence),
                        (
                            "USER_PRIVATE".into(),
                            Some(user_id),
                            None,
                            "PRIVATE".into(),
                            "MANUAL_NOTE".into()
                        )
                    );
                    assert_eq!(
                        stored_evidence(&mut handle, shared_evidence),
                        (
                            "WORKSPACE_SHARED".into(),
                            None,
                            Some(workspace_a),
                            "INTERNAL".into(),
                            "USER_MESSAGE".into()
                        )
                    );
                    assert_eq!(
                        stored_evidence(&mut handle, defaulted_evidence),
                        (
                            "WORKSPACE_SHARED".into(),
                            None,
                            Some(workspace_a),
                            "INTERNAL".into(),
                            "USER_MESSAGE".into()
                        )
                    );
                    assert_eq!(
                        stored_evidence(&mut handle, tenant_evidence),
                        (
                            "TENANT_SHARED".into(),
                            None,
                            None,
                            "INTERNAL".into(),
                            "USER_MESSAGE".into()
                        )
                    );
                });

                // ---- D-A: pair B writes land on B's stream, created by that first write ----
                assert_eq!(
                    tokio::task::block_in_place(|| stream_ledger(&mut handle, workspace_b)),
                    (0, 0, 0),
                    "pair B has no checkpoint row before its first write"
                );
                let (status, on_b) = put(
                    credential_b.bearer.clone(),
                    json!({
                        "operation": "put", "content": "card11 note on pair B",
                        "idempotency_key": format!("c11-pair-b-{}", Uuid::now_v7()),
                        "workspace_id": workspace_b,
                    }),
                )
                .await;
                assert_eq!(status, 200, "{on_b}");
                let b_evidence = accepted(&on_b);
                tokio::task::block_in_place(|| {
                    assert_eq!(
                        stored_evidence(&mut handle, b_evidence),
                        (
                            "WORKSPACE_SHARED".into(),
                            None,
                            Some(workspace_b),
                            "INTERNAL".into(),
                            "USER_MESSAGE".into()
                        )
                    );
                    assert_eq!(
                        stream_ledger(&mut handle, workspace_b),
                        (1, 1, 1),
                        "B's first write creates B's checkpoint row and issues B's ticket"
                    );
                    assert_eq!(
                        stream_ledger(&mut handle, workspace_a),
                        (4, 4, 4),
                        "A's ledger counts only A's four accepted writes"
                    );
                });
                // Credential A is not a member of B: FORBIDDEN before any write.
                let (status, forbidden) = put(
                    credential_a.bearer.clone(),
                    json!({
                        "operation": "put", "content": "card11 A naming B",
                        "idempotency_key": format!("c11-a-on-b-{}", Uuid::now_v7()),
                        "workspace_id": workspace_b,
                    }),
                )
                .await;
                assert_eq!(status, 403, "{forbidden}");
                assert_eq!(
                    forbidden["error"]["data"]["code"], "FORBIDDEN",
                    "{forbidden}"
                );

                // ---- D-A across tenants: pair C is ANOTHER tenant on the same process ----
                // C's fixture domain is neither the process's configured domain (A's) nor
                // owned by C's user: the tenant cannot take a write yet — fail closed
                // (§11.2.1 "无法确定 processing principal"), nothing written, no stream row,
                // no ticket; the boot constant is never stamped onto C's Evidence.
                let before_c = blocking_counts(&mut tenant_c);
                let (status, unresolved) = put(
                    credential_c.bearer.clone(),
                    json!({
                        "operation": "put", "content": "card11 tenant C before its domain is bound",
                        "idempotency_key": format!("c11-pair-c-unbound-{}", Uuid::now_v7()),
                        "workspace_id": workspace_c,
                    }),
                )
                .await;
                assert_eq!(status, 200, "{unresolved}");
                assert_eq!(unresolved["result"]["isError"], true, "{unresolved}");
                assert_eq!(
                    unresolved["result"]["structuredContent"]["code"], "DEPENDENCY_UNAVAILABLE",
                    "a tenant without a reasoning domain fails closed: {unresolved}"
                );
                assert_eq!(
                    durable_counts(blocking_counts(&mut tenant_c)),
                    durable_counts(before_c),
                    "an unresolved reasoning domain leaves no Evidence, receipt or consumed reservation"
                );
                tokio::task::block_in_place(|| {
                    assert_eq!(stream_ledger(&mut tenant_c, workspace_c), (0, 0, 0));
                    // §11.2.1 ingress: user input is processed under that user's own domain.
                    tenant_c
                        .admin
                        .execute(
                            "UPDATE control.private_reasoning_domains SET owner_user_id=$1 \
                             WHERE tenant_id=$2 AND reasoning_domain_id=$3",
                            &[
                                &tenant_c.user_id,
                                &tenant_c.tenant_id,
                                &tenant_c.reasoning_domain_id,
                            ],
                        )
                        .expect("owner binds C's reasoning domain to C's user");
                });
                let (status, on_c) = put(
                    credential_c.bearer.clone(),
                    json!({
                        "operation": "put", "content": "card11 note on tenant C",
                        "idempotency_key": format!("c11-pair-c-{}", Uuid::now_v7()),
                        "workspace_id": workspace_c,
                    }),
                )
                .await;
                assert_eq!(status, 200, "{on_c}");
                let c_evidence = accepted(&on_c);
                tokio::task::block_in_place(|| {
                    assert_eq!(
                        stored_evidence(&mut tenant_c, c_evidence),
                        (
                            "WORKSPACE_SHARED".into(),
                            None,
                            Some(workspace_c),
                            "INTERNAL".into(),
                            "USER_MESSAGE".into()
                        )
                    );
                    assert_eq!(
                        stored_reasoning_domain(&mut tenant_c, c_evidence),
                        tenant_c.reasoning_domain_id,
                        "C's Evidence is processed under C's own reasoning domain, not A's boot constant"
                    );
                    assert_eq!(
                        stream_ledger(&mut tenant_c, workspace_c),
                        (1, 1, 1),
                        "C's first write creates C's checkpoint row and issues C's ticket"
                    );
                    assert_eq!(
                        stream_ledger(&mut handle, workspace_a),
                        (4, 4, 4),
                        "A's ledger is untouched by another tenant's write"
                    );
                    assert_eq!(stream_ledger(&mut handle, workspace_b), (1, 1, 1));
                    // 0159: the database itself refuses a cross-tenant reasoning domain, even
                    // for a writer that bypasses the resolve — the owner client bypasses RLS,
                    // so the composite FK is the only thing that can say no here.
                    let cross = handle.admin.execute(
                        "INSERT INTO private.evidence_objects \
                           (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                            visibility_class, reasoning_domain_id) \
                         VALUES ($1, 'EVENT', sha256(convert_to(gen_random_uuid()::text,'UTF8')), \
                                 'INTERNAL', 'DirectUserInput', 'TENANT_SHARED', $2)",
                        &[&tenant_id, &tenant_c.reasoning_domain_id],
                    );
                    assert_eq!(
                        cross
                            .expect_err("tenant A Evidence under tenant C's reasoning domain")
                            .code(),
                        Some(&postgres::error::SqlState::FOREIGN_KEY_VIOLATION)
                    );
                });
                // The tenant is the principal's: credential C naming A's workspace is not a
                // member there — FORBIDDEN before any write.
                let (status, forbidden_c) = put(
                    credential_c.bearer.clone(),
                    json!({
                        "operation": "put", "content": "card11 C naming A",
                        "idempotency_key": format!("c11-c-on-a-{}", Uuid::now_v7()),
                        "workspace_id": workspace_a,
                    }),
                )
                .await;
                assert_eq!(status, 403, "{forbidden_c}");
                assert_eq!(
                    forbidden_c["error"]["data"]["code"], "FORBIDDEN",
                    "{forbidden_c}"
                );

                // ---- D-C: same principal, same key, two streams, concurrently ----
                let principal = credential_a.api_key_id;
                let two_pairs = AuthorizationScope::new(
                    TenantId(tenant_id),
                    PrincipalId(principal),
                    Some(UserId(user_id)),
                    BoundedSet::new([WorkspaceId(workspace_a), WorkspaceId(workspace_b)])
                        .expect("two-workspace scope"),
                );
                let key = format!("c11-cross-stream-{}", Uuid::now_v7());
                let content_a = "card11 cross-stream on A";
                let content_b = "card11 cross-stream on B";
                let (first_a, first_b) = tokio::join!(
                    operation_receipt::remember_atomically(
                        &direct_pool,
                        &two_pairs,
                        direct_receipt_request(&handle, principal, workspace_a, &key, content_a),
                    ),
                    operation_receipt::remember_atomically(
                        &direct_pool,
                        &two_pairs,
                        direct_receipt_request(&handle, principal, workspace_b, &key, content_b),
                    ),
                );
                let first_a = first_a.expect("same key on stream A commits");
                let first_b =
                    first_b.expect("same key on stream B commits concurrently, never CONFLICT");
                assert!(!first_a.replayed && !first_b.replayed);
                assert_ne!(first_a.accepted.evidence_id, first_b.accepted.evidence_id);
                // Each stream replays its own receipt.
                let replay_a = operation_receipt::remember_atomically(
                    &direct_pool,
                    &two_pairs,
                    direct_receipt_request(&handle, principal, workspace_a, &key, content_a),
                )
                .await
                .expect("stream A replays");
                let replay_b = operation_receipt::remember_atomically(
                    &direct_pool,
                    &two_pairs,
                    direct_receipt_request(&handle, principal, workspace_b, &key, content_b),
                )
                .await
                .expect("stream B replays");
                assert!(replay_a.replayed && replay_b.replayed);
                assert_eq!(replay_a.accepted.evidence_id, first_a.accepted.evidence_id);
                assert_eq!(replay_b.accepted.evidence_id, first_b.accepted.evidence_id);
                let receipts: i64 = tokio::task::block_in_place(|| {
                    handle
                        .admin
                        .query_one(
                            "SELECT count(*) FROM control.operation_receipts \
                             WHERE tenant_id=$1 AND principal_id=$2 AND idempotency_key=$3",
                            &[&tenant_id, &principal, &key],
                        )
                        .expect("owner counts receipts")
                        .get(0)
                });
                assert_eq!(receipts, 2, "one receipt per stream for the same caller key");

                // ---- speed record for the ADR (uncontended, per-call class) ----
                let mut samples = Vec::new();
                for n in 0..9_u8 {
                    let started = Instant::now();
                    let (status, response) = put(
                        credential_a.bearer.clone(),
                        json!({
                            "operation": "put", "content": format!("card11 speed {n}"),
                            "idempotency_key": format!("c11-p50-{n}-{}", Uuid::now_v7()),
                            "workspace_id": workspace_a,
                            "visibility_class": "USER_PRIVATE",
                        }),
                    )
                    .await;
                    samples.push(started.elapsed());
                    assert_eq!(status, 200, "{response}");
                    accepted(&response);
                }
                eprintln!(
                    "card11 p50 remember.put (per-call class): {:?} (n={})",
                    percentile_p50(&mut samples),
                    samples.len()
                );
                stop_server(server).await.expect("stop card 11 server");
            });
                },
            );
        },
    );
}

// ============================================================================
// Card 12 / ADR-0033: membership lifecycle → security epoch → bearer rejected on the very
// next request (no token TTL wait), last OWNER protected, audit rows present.
// ============================================================================

/// `(SUCCESS rows, DENIED rows)` — §77: applied and refused membership requests both audit.
fn membership_audit_counts(handle: &mut Handle) -> (i64, i64) {
    let row = handle
        .admin
        .query_one(
            "SELECT count(*) FILTER (WHERE result='SUCCESS'), \
                    count(*) FILTER (WHERE result='DENIED'), count(*) \
             FROM control.audit_events \
             WHERE tenant_id=$1 AND action LIKE 'MEMBERSHIP_%' AND actor_type='ADMIN'",
            &[&handle.tenant_id],
        )
        .expect("owner counts membership audit rows");
    let (success, denied, total): (i64, i64, i64) = (row.get(0), row.get(1), row.get(2));
    assert_eq!(success + denied, total);
    (success, denied)
}

const MEMBERSHIP_ADMIN: AdminAction<'static> = AdminAction {
    actor: "card12-e2e",
    reason: "gateway e2e: suspend rejects bearer on next request",
    ticket: "OPS-12-E2E",
    trace_id: "trace-card12-e2e",
    step_up_auth_context: "test-fixture:maintenance-dsn",
};

fn user_security_epoch(handle: &mut Handle, user_id: Uuid) -> i64 {
    handle
        .admin
        .query_one(
            "SELECT security_epoch FROM control.users WHERE user_id=$1",
            &[&user_id],
        )
        .expect("owner reads user epoch")
        .get(0)
}

/// The admin path under the fixture's real `role_maintenance` pool. Awaited inside the
/// test's runtime (never `rt.block_on` from within it).
async fn membership_apply(
    handle: &Handle,
    user_id: Uuid,
    request: MembershipRequest,
) -> Result<MembershipOutcome, MembershipRepoError> {
    membership_repo::apply(
        &handle.maintenance,
        TenantId(handle.tenant_id),
        UserId(user_id),
        request,
        MEMBERSHIP_ADMIN,
    )
    .await
}

/// Binds a fresh synthetic credential to `user_id` with the user's *current* epoch snapshot
/// (what a credential issued after the lifecycle event would carry).
fn bind_credential_to_user(
    handle: &mut Handle,
    credential: &SyntheticServiceCredential,
    user_id: Uuid,
    user_epoch: i64,
) {
    handle
        .admin
        .execute(
            "UPDATE control.api_keys SET user_id=$2, user_security_epoch=$3 WHERE api_key_id=$1",
            &[&credential.api_key_id, &user_id, &user_epoch],
        )
        .expect("owner binds credential to user");
}

#[test]
#[allow(clippy::too_many_lines)] // one serialized lifecycle story against one gateway
fn native_mcp_membership_suspend_rejects_bearer_on_next_request_and_last_owner_protected() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>(
        "native_mcp_membership_suspend_rejects_bearer_on_next_request",
        |mut handle| {
            handle.assert_gateway_login();
            // Quota window for the tenant (the peer's calls draw on it too).
            let prefix = format!("mown{}", &Uuid::now_v7().simple().to_string()[..12]);
            let wire = format!("{prefix}.{}", "a".repeat(32));
            let _owner_credential = handle.seed_synthetic_service_credential_and_window(
                SyntheticCredentialScopes::ContextRead,
                &prefix,
                &wire,
                &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
                64,
            );
            let peer_user = handle.seed_peer_user();
            let peer_prefix = format!("mpe1{}", &Uuid::now_v7().simple().to_string()[..12]);
            let peer_wire = format!("{peer_prefix}.{}", "b".repeat(32));
            let peer = handle.seed_synthetic_service_credential(
                SyntheticCredentialScopes::ContextRead,
                &peer_prefix,
                &peer_wire,
                &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &peer_wire),
            );
            bind_credential_to_user(&mut handle, &peer, peer_user, 0);
            // ADR-0035 (card 13): the peer reads a WORKSPACE_SHARED memory in the fixture workspace
            // while ACTIVE — its ACTIVE WorkspaceMembership on that workspace comes from
            // `seed_peer_user`. Suspending the tenant membership still 401s the next request.
            let visible = handle.seed_workspace_visible_context_record();
            let owner_user = handle.user_id;
            assert_eq!(membership_audit_counts(&mut handle), (0, 0));

            let runtime_handle = handle.rt.handle().clone();
            let runtime = runtime_handle
                .block_on(handle.fresh_runtime())
                .expect("checked membership runtime");
            let app = application(&handle, runtime);
            runtime_handle.block_on(async {
                let (address, server) = start(app).await;
                let get = || {
                    rpc(
                        1,
                        "tools/call",
                        call_params(
                            "memory",
                            json!({"action":"get","memory_id":visible.memory_id}),
                        ),
                    )
                };
                let call = |bearer: String| async move {
                    let (status, response) =
                        raw_request(address, &tool_call_headers("memory", &bearer), &get()).await;
                    (status, response)
                };

                // (1) peer's bearer works while ACTIVE.
                let (status, response) = call(peer.bearer.clone()).await;
                assert_eq!(status, 200, "active peer memory.get: {response}");
                assert_tool_response(&response, ToolName::Memory);

                // (2) suspend ⇒ epoch bumped in the same transaction ⇒ next request 401.
                let suspended = membership_apply(
                    &handle,
                    peer_user,
                    MembershipRequest::Mutate(MembershipMutation::Suspend),
                )
                .await
                .expect("suspend peer");
                assert_eq!(suspended.state, MembershipState::Suspended);
                assert_eq!(suspended.user_security_epoch, Some(1));
                tokio::task::block_in_place(|| {
                    assert_eq!(user_security_epoch(&mut handle, peer_user), 1);
                    assert_eq!(membership_audit_counts(&mut handle), (1, 0));
                });
                let (status, response) = call(peer.bearer.clone()).await;
                assert_eq!(
                    status, 401,
                    "suspended peer must be rejected immediately: {response}"
                );

                // (3) re-activate ⇒ the membership is ACTIVE again, but the OLD bearer stays
                // dead: its epoch snapshot (0) no longer matches the user's live epoch (1) —
                // §6.3's whole point (no TTL wait, no revival). A credential issued after the
                // reinstatement (snapshot = live epoch) works.
                let activated = membership_apply(
                    &handle,
                    peer_user,
                    MembershipRequest::Mutate(MembershipMutation::Activate),
                )
                .await
                .expect("re-activate peer");
                assert_eq!(activated.state, MembershipState::Active);
                assert_eq!(
                    activated.user_security_epoch, None,
                    "activation never bumps"
                );
                let (status, response) = call(peer.bearer.clone()).await;
                assert_eq!(
                    status, 401,
                    "old epoch snapshot stays invalid after reinstatement: {response}"
                );
                let fresh = tokio::task::block_in_place(|| {
                    let fresh_prefix =
                        format!("mpe2{}", &Uuid::now_v7().simple().to_string()[..12]);
                    let fresh_wire = format!("{fresh_prefix}.{}", "c".repeat(32));
                    let fresh = handle.seed_synthetic_service_credential(
                        SyntheticCredentialScopes::ContextRead,
                        &fresh_prefix,
                        &fresh_wire,
                        &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &fresh_wire),
                    );
                    let live = user_security_epoch(&mut handle, peer_user);
                    bind_credential_to_user(&mut handle, &fresh, peer_user, live);
                    fresh
                });
                let (status, response) = call(fresh.bearer.clone()).await;
                assert_eq!(
                    status, 200,
                    "credential issued under the live epoch works: {response}"
                );
                assert_tool_response(&response, ToolName::Memory);

                // (4) remove ⇒ bumped again ⇒ the fresh bearer dies on the very next call.
                let removed = membership_apply(
                    &handle,
                    peer_user,
                    MembershipRequest::Mutate(MembershipMutation::Remove),
                )
                .await
                .expect("remove peer");
                assert_eq!(
                    (removed.state, removed.user_security_epoch),
                    (MembershipState::Removed, Some(2))
                );
                let (status, response) = call(fresh.bearer.clone()).await;
                assert_eq!(
                    status, 401,
                    "removed member rejected on the next request: {response}"
                );

                // (5) last OWNER protected: promote the fixture user (seeded MEMBER) to the
                // tenant's only OWNER, then removal/suspension is CONFLICT and nothing moves.
                membership_apply(
                    &handle,
                    owner_user,
                    MembershipRequest::Mutate(MembershipMutation::ChangeRole(
                        MembershipRole::Owner,
                    )),
                )
                .await
                .expect("promote fixture user to OWNER");
                let (epoch_before, audits_before) = tokio::task::block_in_place(|| {
                    (
                        user_security_epoch(&mut handle, owner_user),
                        membership_audit_counts(&mut handle),
                    )
                });
                for mutation in [MembershipMutation::Remove, MembershipMutation::Suspend] {
                    let refused =
                        membership_apply(&handle, owner_user, MembershipRequest::Mutate(mutation))
                            .await;
                    assert!(
                        matches!(
                            refused,
                            Err(MembershipRepoError::Conflict(MembershipConflict::LastOwner))
                        ),
                        "{mutation:?}: {refused:?}"
                    );
                }
                tokio::task::block_in_place(|| {
                    assert_eq!(user_security_epoch(&mut handle, owner_user), epoch_before);
                    // Applied rows: suspend, activate, remove, change_role = 4; the two
                    // refused last-OWNER attempts each leave a DENIED row (§77 "全部审计").
                    assert_eq!(audits_before, (4, 0));
                    assert_eq!(membership_audit_counts(&mut handle), (4, 2));
                    let denied: Vec<(String, String, String)> = handle
                        .admin
                        .query(
                            "SELECT action, request_id, metadata->>'refusal' \
                             FROM control.audit_events \
                             WHERE tenant_id=$1 AND action LIKE 'MEMBERSHIP_%' AND result='DENIED' \
                             ORDER BY audit_seq",
                            &[&handle.tenant_id],
                        )
                        .expect("owner reads DENIED rows")
                        .iter()
                        .map(|r| (r.get(0), r.get(1), r.get(2)))
                        .collect();
                    assert_eq!(
                        denied,
                        vec![
                            (
                                "MEMBERSHIP_REMOVE".into(),
                                MEMBERSHIP_ADMIN.ticket.into(),
                                "LAST_OWNER".into()
                            ),
                            (
                                "MEMBERSHIP_SUSPEND".into(),
                                MEMBERSHIP_ADMIN.ticket.into(),
                                "LAST_OWNER".into()
                            ),
                        ]
                    );
                });

                // (6) Card 22 (card 21's folded debt): every §6.3 refusal carries its TYPED
                // §52 D-B `ConflictReason` **code**, not just a SCREAMING_SNAKE label, and the
                // code reaches a reader.
                //
                // Membership mutation has no MCP route — `SUPPORTED_OPERATION_KEYS` names the
                // whole wired surface and no membership key is on it (§33 「工具面交付实况」),
                // so there is no `structuredContent.reason` to put it in. The §77 audit row IS
                // the observable surface of a refused membership mutation, so that is where
                // `metadata.refusal_code` lands (`membership_repo::audit_row`).
                //
                // Three live refusals, one per variant, each with its own code. Fault
                // injection: replace `error.conflict_reason().map(ConflictReason::code)` with a
                // constant, a `None`, or a second hand-typed table and the distinctness +
                // per-row equality assertions below go red — a label-only row cannot satisfy
                // them, which is exactly the state card 21 left behind.
                let mut expected_codes = Vec::new();
                for (label, user, mutation, conflict) in [
                    (
                        "TRANSITION_NOT_ALLOWED",
                        peer_user,
                        MembershipMutation::Activate,
                        MembershipConflict::TransitionNotAllowed,
                    ),
                    (
                        "ALREADY_IN_STATE",
                        owner_user,
                        MembershipMutation::ChangeRole(MembershipRole::Owner),
                        MembershipConflict::AlreadyInState,
                    ),
                ] {
                    let refused =
                        membership_apply(&handle, user, MembershipRequest::Mutate(mutation)).await;
                    let Err(error) = refused else {
                        panic!("{label}: expected a refusal, got {refused:?}");
                    };
                    assert_eq!(error.error_code(), ErrorCode::Conflict, "{label}");
                    assert_eq!(
                        error.conflict_reason(),
                        Some(conflict.reason()),
                        "{label}: the repo error must carry the typed reason"
                    );
                    assert!(
                        format!("{error}").contains(&conflict.reason().code().to_string()),
                        "{label}: Display must print the numeric code, not only the label: {error}"
                    );
                    expected_codes.push((
                        label.to_string(),
                        i32::from(conflict.reason().code()).to_string(),
                    ));
                }
                // LAST_OWNER's two DENIED rows are already on the table from (5).
                expected_codes.insert(
                    0,
                    (
                        "LAST_OWNER".to_string(),
                        i32::from(MembershipConflict::LastOwner.reason().code()).to_string(),
                    ),
                );
                expected_codes.insert(0, expected_codes[0].clone());
                tokio::task::block_in_place(|| {
                    let rows: Vec<(String, Option<String>)> = handle
                        .admin
                        .query(
                            "SELECT metadata->>'refusal', metadata->>'refusal_code' \
                             FROM control.audit_events \
                             WHERE tenant_id=$1 AND action LIKE 'MEMBERSHIP_%' AND result='DENIED' \
                             ORDER BY audit_seq",
                            &[&handle.tenant_id],
                        )
                        .expect("owner reads DENIED rows")
                        .iter()
                        .map(|r| (r.get(0), r.get(1)))
                        .collect();
                    let expected: Vec<(String, Option<String>)> = expected_codes
                        .iter()
                        .map(|(label, code)| (label.clone(), Some(code.clone())))
                        .collect();
                    assert_eq!(
                        rows, expected,
                        "every DENIED membership row carries its label AND its typed reason code"
                    );
                    // A mapping that answers one constant (or drops to NULL) cannot produce
                    // three distinct codes for three distinct refusals.
                    let distinct: BTreeSet<Option<String>> =
                        rows.iter().map(|(_, code)| code.clone()).collect();
                    assert_eq!(
                        distinct.len(),
                        3,
                        "1201 / 1203 / 1204 must be three different numbers on the wire: {rows:?}"
                    );
                });
                stop_server(server).await.expect("server stops");
            });
        },
    );
}

/// One `memory.get` for `memory_id`, optionally routed to `workspace_id` (ADR-0035 e2e).
async fn memory_get_in(
    address: SocketAddr,
    bearer: &str,
    request_id: u64,
    memory_id: Uuid,
    workspace_id: Option<Uuid>,
) -> (u16, Value) {
    let mut arguments = json!({"action":"get","memory_id":memory_id});
    if let Some(workspace_id) = workspace_id {
        arguments["workspace_id"] = Value::String(workspace_id.to_string());
    }
    raw_request(
        address,
        &tool_call_headers("memory", bearer),
        &rpc(request_id, "tools/call", call_params("memory", arguments)),
    )
    .await
}

/// Card 13 acceptance gate (ADR-0035, §6.1.1): `AuthorizationScope.allowed_workspace_ids` is
/// derived per request from the on-behalf-of user's live ACTIVE *WorkspaceMembership* set
/// (`control.workspace_memberships`, migration 0162 — a tenant membership alone grants NO
/// workspace; the peer user's memberships {A, B} are seeded explicitly below) intersected with
/// the credential's optional bound workspace. One unbound PAT therefore sees WORKSPACE_SHARED
/// memories of its two member workspaces in one session without a second credential; a workspace
/// the user is not a member of (another tenant's, or a non-existent one)
/// is `FORBIDDEN` before any object lookup; a same-tenant memory named under the wrong
/// workspace stays `NOT_FOUND`; a credential carrying an explicit `workspace_id` still narrows
/// to exactly that one (the binding is a default route and a ceiling on the live set, never an
/// authority — replace the intersection with a union and this branch goes red); and after
/// card 12's suspension the very next request is 401 on both workspaces.
#[test]
#[allow(clippy::too_many_lines)] // one causally ordered story against ONE gateway process
fn native_mcp_workspace_membership_scope() {
    let _metrics = CONTEXT_METRIC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    run_db_fixture::<Fixture, _>("native_mcp_workspace_membership_scope_foreign", |foreign| {
        let foreign_workspace = foreign.workspace_id;
        run_db_fixture::<Fixture, _>("native_mcp_workspace_membership_scope", |mut handle| {
            handle.assert_gateway_login();
            let workspace_a = handle.workspace_id;
            let workspace_b = handle.seed_workspace();
            // Both pairs are provisioned (serving projection, ADR-0031 D-A); the
            // bootstrap pair A is provisioned by `application()` itself.
            provision_stream_pair(&handle, workspace_b);
            // The multi-workspace human: an ACTIVE member of the tenant (§6.3), never
            // the fixture owner (its credentials keep the single-workspace shape).
            let peer_user = handle.seed_peer_user();
            let bound_prefix = format!("mwb{}", &Uuid::now_v7().simple().to_string()[..12]);
            let bound_wire = format!("{bound_prefix}.{}", "g".repeat(32));
            let bound = handle.seed_synthetic_service_credential_and_window(
                SyntheticCredentialScopes::ContextRead,
                &bound_prefix,
                &bound_wire,
                &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &bound_wire),
                128,
            );
            bind_credential_to_user(&mut handle, &bound, peer_user, 0);
            let unbound_prefix = format!("mwu{}", &Uuid::now_v7().simple().to_string()[..12]);
            let unbound_wire = format!("{unbound_prefix}.{}", "h".repeat(32));
            let unbound = handle.seed_synthetic_service_credential(
                SyntheticCredentialScopes::ContextRead,
                &unbound_prefix,
                &unbound_wire,
                &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &unbound_wire),
            );
            bind_credential_to_user(&mut handle, &unbound, peer_user, 0);
            handle
                .admin
                .execute(
                    "UPDATE control.api_keys SET workspace_id=NULL WHERE api_key_id=$1",
                    &[&unbound.api_key_id],
                )
                .expect("owner unbinds the credential's workspace");
            // ADR-0035: the peer user's live WorkspaceMemberships are {A, B}. Membership on A (the
            // fixture default workspace) comes from `seed_peer_user`; add B so the unbound PAT reads
            // both in one session while the bound credential (bound to A) still narrows to A. A
            // tenant membership alone would grant neither.
            handle
                .admin
                .execute(
                    "INSERT INTO control.workspace_memberships\
                     (tenant_id,workspace_id,user_id,role,state) \
                     VALUES($1,$2,$3,'MEMBER','ACTIVE')",
                    &[&handle.tenant_id, &workspace_b, &peer_user],
                )
                .expect("owner seeds peer workspace_b membership");
            let a_memory = handle.seed_workspace_visible_context_record();
            let b_memory = with_workspace(&mut handle, workspace_b, |handle| {
                handle.seed_workspace_visible_context_record()
            });

            let runtime_handle = handle.rt.handle().clone();
            let runtime = runtime_handle
                .block_on(handle.fresh_runtime())
                .expect("checked multi-workspace runtime");
            let app = application(&handle, runtime);
            runtime_handle.block_on(async {
                let (address, server) = start(app).await;

                // Speed record (卡片 acceptance goal): p50 of memory.get on the
                // single-workspace (bound) credential shape — the route every
                // existing credential takes, measured before/after this card.
                let mut samples = Vec::new();
                for n in 0..9_u64 {
                    let started = Instant::now();
                    let (status, response) = memory_get_in(
                        address,
                        &bound.bearer,
                        100 + n,
                        a_memory.memory_id,
                        Some(workspace_a),
                    )
                    .await;
                    samples.push(started.elapsed());
                    assert_eq!(status, 200, "bound memory.get: {response}");
                    assert_tool_response(&response, ToolName::Memory);
                }
                eprintln!(
                    "card13 p50 memory.get (bound credential): {:?} (n={})",
                    percentile_p50(&mut samples),
                    samples.len()
                );

                // (1) One unbound PAT, one session: both member workspaces readable.
                let (status, response) = memory_get_in(
                    address,
                    &unbound.bearer,
                    1,
                    a_memory.memory_id,
                    Some(workspace_a),
                )
                .await;
                assert_eq!(status, 200, "unbound PAT reads workspace A: {response}");
                assert_memory_response(
                    &response,
                    a_memory.memory_id,
                    5,
                    &std::env::current_exe().expect("test binary"),
                );
                let (status, response) = memory_get_in(
                    address,
                    &unbound.bearer,
                    2,
                    b_memory.memory_id,
                    Some(workspace_b),
                )
                .await;
                assert_eq!(
                    status, 200,
                    "same credential reads workspace B without reissue: {response}"
                );
                assert_memory_response(
                    &response,
                    b_memory.memory_id,
                    5,
                    &std::env::current_exe().expect("test binary"),
                );
                // D-B: enumerate accepts any member workspace and stays workspace-exact.
                let (status, page) = enumerate_call(
                    address,
                    &unbound.bearer,
                    json!({"action":"enumerate","limit":100,"workspace_id":workspace_b}),
                )
                .await;
                assert_eq!(status, 200, "enumerate in workspace B: {page}");
                assert!(assert_enumeration_response(&page, &[b_memory.memory_id]).is_none());
                let (status, page) = enumerate_call(
                    address,
                    &unbound.bearer,
                    json!({"action":"enumerate","limit":100,"workspace_id":workspace_a}),
                )
                .await;
                assert_eq!(status, 200, "enumerate in workspace A: {page}");
                assert!(assert_enumeration_response(&page, &[a_memory.memory_id]).is_none());

                // (2) Narrowing to A hides B's memory (NOT_FOUND, never an oracle);
                // a workspace the user is not a member of is FORBIDDEN before lookup.
                let (status, response) = memory_get_in(
                    address,
                    &unbound.bearer,
                    3,
                    b_memory.memory_id,
                    Some(workspace_a),
                )
                .await;
                assert_eq!(status, 200, "wrong-workspace object denial: {response}");
                assert_memory_not_found(&response);
                for (request_id, outside) in [(4, foreign_workspace), (5, Uuid::now_v7())] {
                    let (status, response) = memory_get_in(
                        address,
                        &unbound.bearer,
                        request_id,
                        a_memory.memory_id,
                        Some(outside),
                    )
                    .await;
                    assert_eq!(
                        status, 403,
                        "non-member workspace {outside} is FORBIDDEN: {response}"
                    );
                }
                // An unbound PAT has no default route: the request must name one.
                let (status, response) =
                    memory_get_in(address, &unbound.bearer, 6, a_memory.memory_id, None).await;
                assert_eq!(
                    status, 200,
                    "no default route stays a tool error: {response}"
                );
                assert_tool_error(&response, "DEPENDENCY_UNAVAILABLE");

                // (3) A credential carrying workspace_id narrows to exactly that one:
                // the live set {A, B} ∩ {A} = {A}. Union instead of intersection ⇒ red.
                let (status, response) = memory_get_in(
                    address,
                    &bound.bearer,
                    7,
                    b_memory.memory_id,
                    Some(workspace_b),
                )
                .await;
                assert_eq!(
                    status, 403,
                    "bound credential must not widen to a second member workspace: {response}"
                );
                let (status, response) =
                    memory_get_in(address, &bound.bearer, 8, a_memory.memory_id, None).await;
                assert_eq!(
                    status, 200,
                    "bound workspace is the default route: {response}"
                );
                assert_memory_response(
                    &response,
                    a_memory.memory_id,
                    5,
                    &std::env::current_exe().expect("test binary"),
                );

                // (4) Suspension (card 12) empties the scope on the very next request:
                // the membership is no longer ACTIVE and the epoch snapshot is stale.
                let suspended = membership_apply(
                    &handle,
                    peer_user,
                    MembershipRequest::Mutate(MembershipMutation::Suspend),
                )
                .await
                .expect("suspend the multi-workspace user");
                assert_eq!(suspended.state, MembershipState::Suspended);
                for (request_id, bearer, memory_id, workspace) in [
                    (9, &unbound.bearer, a_memory.memory_id, workspace_a),
                    (10, &unbound.bearer, b_memory.memory_id, workspace_b),
                    (11, &bound.bearer, a_memory.memory_id, workspace_a),
                ] {
                    let (status, response) =
                        memory_get_in(address, bearer, request_id, memory_id, Some(workspace))
                            .await;
                    assert_eq!(
                        status, 401,
                        "suspended member sees nothing on the next request: {response}"
                    );
                }
                stop_server(server)
                    .await
                    .expect("stop multi-workspace server");
            });
        });
    });
}
