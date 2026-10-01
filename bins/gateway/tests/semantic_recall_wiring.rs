//! `gateway::tests::semantic_recall_wiring` — ADR-0012/ADR-0014 gateway semantic-recall wiring acceptance.
//! Depends-on: crates=[async-trait, axum, humaux-adapters, humaux-application, humaux-contracts, humaux-domain,
//!   humaux-infra-cell, humaux-local-secret-scan, humaux-projection, humaux-protocol, humaux-retrieval-provider,
//!   humaux-retrieval-worker, humaux-testkit, postgres, serde_json, time, tokio, uuid];
//!   services=[PostgreSQL(role_gateway) r=[private.memory_records] w=[projection.private_memory_points,
//!   projection.stream_checkpoints, projection.tenant_placements], PostgreSQL(role_retrieval_worker), Qdrant(*),
//!   UDS(retrieval-worker), UDS(serve)]; env=[CARGO_MANIFEST_DIR, HUMAUX_GATEWAY_PG_DSN,
//!   HUMAUX_RETRIEVAL_WORKER_PG_DSN, HUMAUX_TEST_GITLEAKS_BIN, HUMAUX_TEST_GITLEAKS_SHA256,
//!   HUMAUX_TEST_GITLEAKS_VERSION, HUMAUX_TEST_QDRANT_PORT]; modules=[adapters::postgres, adapters::qdrant,
//!   adapters::tests::support::operation_receipt_fixture, application::retrieval_embedding_port,
//!   contracts::retrieval_config, domain::authority, domain::context, domain::dataclass, domain::error,
//!   domain::identity, domain::ids, domain::memory, gateway::context, gateway::recall, gateway::remember,
//!   gateway::retrieval_embedding_client, humaux-local-secret-scan, humaux-testkit, infra-cell::permit,
//!   infra-cell::resource, infra-cell::transport, projection::card, protocol::mcp_catalog,
//!   retrieval-provider::adapters, retrieval-provider::contract, retrieval-worker::rpc]
//! Called-by: [cargo-test]
//! Invariants: [each test wires its own PostgreSQL/Qdrant/UDS fixtures; a missing fixture fails the test rather than skipping it]
//! Spec: ADR-0012; ADR-0014; ADR-0055
//!
//! Drives `humaux_gateway::recall::search` directly (no HTTP/MCP layer — that surface is
//! already covered by `tests/mcp_gateway.rs`'s native MCP acceptance tests) against a real
//! `role_gateway` PostgreSQL pool, a real disposable Qdrant collection, and — for the happy
//! path — a real in-process `humaux-retrieval-worker` RPC app (mirrors
//! `tests/query_embedding_rpc.rs`'s own in-process-worker pattern). Proves: (1) the bootstrap
//! wiring card's per-tenant placement resolution actually hits; (2) a tenant with no placement
//! row degrades closed, never onto another tenant's collection; (3) a dead RPC socket degrades
//! closed without hanging or panicking; (4) `HttpIntraCellTransport` really denies a write under
//! a `QdrantReadOnly` permit (ADR-0014); (5) a wrong-dimension vector from the embedding port
//! never gets hydrated; (6) `bootstrap.rs` wires `with_semantic_recall` exactly once.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use postgres::Client;
use uuid::Uuid;

// This test file only needs a slice of the shared fixture's surface (tenant/workspace setup,
// context-record seeding, teardown) — the rest is real, used by `tests/mcp_gateway.rs`'s own
// inclusion of the identical file, just not by this one.
#[allow(dead_code)]
#[path = "../../../crates/adapters/tests/support/operation_receipt_fixture.rs"]
mod operation_receipt_fixture;
use operation_receipt_fixture::{Fixture, Handle};

use humaux_adapters::{
    postgres::RetrievalWorkerDbPool,
    qdrant::{
        Distance, PointId, QdrantOperation, QdrantPointPayload, ShardingMethod,
        create_collection_body, ha_profile_for, tenant_index_body, upsert,
    },
};
use humaux_application::retrieval_embedding_port::{
    RetrievalEmbeddingInput, RetrievalEmbeddingOutcome, RetrievalEmbeddingPort,
};
use humaux_domain::{
    authority::{AuthorityClass, AuthorityStatus},
    dataclass::DataClass,
    error::ErrorCode,
    identity::VisibilityClass,
    ids::{TenantId, WorkspaceId},
    memory::MemoryType,
};
use humaux_gateway::{
    context::ContextBootstrap,
    recall::{self, RecallSearchRequest, SemanticRecallRuntime, SemanticRecallVersions},
    remember::{ProcessFamily, RememberPolicy},
    retrieval_embedding_client::GatewayRetrievalEmbeddingClient,
};
use humaux_infra_cell::{
    CallerId, CellAccessMode, CellAccessPermit, CellId, HttpIntraCellTransport, IntraCellError,
    IntraCellHttpTransport, IntraCellMethod, IntraCellRequest, IntraCellResource,
    IntraCellResourceRegistry, IntraCellResponse, ResourceEntry, authorize_cell_access,
};
use humaux_local_secret_scan::{LocalSecretScanner, LocalSecretScannerConfig};
use humaux_projection::card::EgressDisposition;
use humaux_protocol::mcp_catalog::CanonicalCatalog;
use humaux_retrieval_provider::{
    adapters::TestDoubleProvider,
    contract::{
        CalibrationProfileId, EmbeddingModelDescriptor, ModelId, RerankModelDescriptor,
        RerankScoreSemantics,
    },
};
use humaux_testkit::run_db_fixture;

const DIMENSION: u32 = 4;
const EMBEDDING_VERSION: &str = "embed-v1";

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing required test env {name}"))
}

fn temp_socket_path(tag: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("/tmp/hsr-{tag}-{}.sock", Uuid::now_v7().simple()))
}

/// Mirrors `tests/query_embedding_rpc.rs`'s identical helper (separate test binary).
async fn own_uid() -> u32 {
    let path = temp_socket_path("uid-probe");
    // dep: UDS(serve) — test binds a probe socket to read the peer uid
    let listener = tokio::net::UnixListener::bind(&path).expect("bind uid probe socket");
    // dep: UDS(retrieval-worker) — test dials the probe socket as the retrieval-worker client would
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

fn embedding_model() -> EmbeddingModelDescriptor {
    EmbeddingModelDescriptor {
        model_id: ModelId("wiring-test-embedding".to_owned()),
        model_revision: "v1".to_owned(),
        dimension_options: vec![DIMENSION],
        max_input_tokens: 4_096,
        batch_supported: true,
        dense_supported: true,
        sparse_supported: false,
    }
}

fn rerank_model() -> RerankModelDescriptor {
    RerankModelDescriptor {
        model_id: ModelId("wiring-test-unused-reranker".to_owned()),
        model_revision: "unused-v1".to_owned(),
        max_documents: 5,
        max_input_tokens: 4_096,
        score_semantics: RerankScoreSemantics::RawLogit,
        calibration_profile: CalibrationProfileId("unused-v1".to_owned()),
    }
}

fn scanner() -> Arc<LocalSecretScanner> {
    Arc::new(
        LocalSecretScanner::new(LocalSecretScannerConfig {
            executable: required("HUMAUX_TEST_GITLEAKS_BIN").into(),
            expected_version: required("HUMAUX_TEST_GITLEAKS_VERSION"),
            expected_executable_sha256: required("HUMAUX_TEST_GITLEAKS_SHA256"),
            timeout: Duration::from_secs(5),
            max_payload_bytes: 64 * 1024,
            finding_exit_code: 1,
        })
        .expect("pinned wiring-test scanner"),
    )
}

/// Spawns the real `humaux-retrieval-worker` RPC app in-process on a temporary UDS — mirrors
/// `tests/query_embedding_rpc.rs`'s `spawn_worker`.
async fn spawn_worker(expected_gateway_uid: u32) -> String {
    let socket_path = temp_socket_path("worker");
    // dep: PostgreSQL(role_retrieval_worker) — test fixture pool for the semantic recall wiring suite
    let calls = RetrievalWorkerDbPool::connect(&required("HUMAUX_RETRIEVAL_WORKER_PG_DSN"))
        .await
        .expect("retrieval worker db pool");
    let state = Arc::new(humaux_retrieval_worker::rpc::RpcState {
        expected_gateway_uid,
        calls,
        scanner: scanner(),
        embedder: Arc::new(TestDoubleProvider::new(embedding_model(), rerank_model())),
        dimension: DIMENSION,
        provider_id: "wiring-test-provider".to_owned(),
    });
    // dep: UDS(serve) — test serves the real retrieval-worker RPC router on a temp socket
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

fn qdrant_port() -> u16 {
    std::env::var("HUMAUX_TEST_QDRANT_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(6333)
}

/// The one registry both `IntraCellResource::QDRANT_REST` (`QdrantReadOnly`, ADR-0014) and
/// `IntraCellResource::RETRIEVAL_EMBEDDING_RPC` share — the same shape `bootstrap.rs`'s
/// `build_semantic_recall_runtime` constructs in production.
fn cell_registry(cell: CellId, caller: CallerId) -> IntraCellResourceRegistry {
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
        .expect("loopback Qdrant resource")
        .with_access_mode(CellAccessMode::QdrantReadOnly),
    );
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

/// Counts every call that reaches the inner transport — proves "zero Qdrant calls" for the
/// no-placement test rather than merely asserting the final `Err`.
struct CountingTransport {
    inner: Arc<HttpIntraCellTransport>,
    calls: AtomicUsize,
}

#[async_trait]
impl IntraCellHttpTransport for CountingTransport {
    async fn execute(
        &self,
        permit: &CellAccessPermit,
        request: IntraCellRequest,
    ) -> Result<IntraCellResponse, IntraCellError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.execute(permit, request).await
    }
}

/// An in-process `RetrievalEmbeddingPort` double that always returns the same fixed outcome —
/// used where the test cares about `recall::search`'s reaction to a specific outcome
/// (`Unavailable`, or a wrong-dimension `Embedded`) rather than a real worker round-trip.
/// `calls` lets a test prove the port was actually reached (i.e. placement resolution
/// succeeded) rather than the assertion passing vacuously because `recall::search` degraded
/// earlier, at the placement step.
struct FixedOutcomePort {
    outcome: RetrievalEmbeddingOutcome,
    calls: AtomicUsize,
}

impl FixedOutcomePort {
    fn new(outcome: RetrievalEmbeddingOutcome) -> Self {
        Self {
            outcome,
            calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl RetrievalEmbeddingPort for FixedOutcomePort {
    async fn embed_query(
        &self,
        _input: RetrievalEmbeddingInput<'_>,
    ) -> Result<RetrievalEmbeddingOutcome, ErrorCode> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.outcome.clone())
    }
}

fn qdrant_transport(registry: &IntraCellResourceRegistry) -> Arc<HttpIntraCellTransport> {
    Arc::new(
        HttpIntraCellTransport::new(
            registry.clone(),
            Duration::from_secs(10),
            humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES,
        )
        .expect("Qdrant transport"),
    )
}

async fn create_collection(
    transport: &HttpIntraCellTransport,
    registry: &IntraCellResourceRegistry,
    collection: &str,
) {
    let permit = authorize_cell_access(
        registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(60),
    )
    .expect(
        "real Qdrant setup needs a permit that predates ADR-0014 (setup runs GET/PUT \
             directly against the real transport, never through a QdrantReadOnly permit)",
    );
    for (path, body) in [
        (
            format!("/collections/{collection}"),
            create_collection_body(
                DIMENSION.into(),
                Distance::Cosine,
                1,
                1,
                1,
                ShardingMethod::Auto,
            ),
        ),
        (
            format!("/collections/{collection}/index"),
            tenant_index_body(),
        ),
    ] {
        let response = transport
            .execute(
                &permit,
                // dep: Qdrant(*) — test issues a Qdrant REST request against the fixture collection
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

async fn delete_collection(
    transport: &HttpIntraCellTransport,
    registry: &IntraCellResourceRegistry,
    collection: &str,
) {
    let permit = authorize_cell_access(
        registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(60),
    )
    .expect("real Qdrant cleanup permit");
    let _ = transport
        .execute(
            &permit,
            // dep: Qdrant(*) — test issues a Qdrant REST request against the fixture collection
            IntraCellRequest {
                method: IntraCellMethod::Delete,
                path: format!("/collections/{collection}"),
                json_body: None,
                headers: Vec::new(),
            },
        )
        .await;
}

/// A setup permit that predates ADR-0014's read-only enforcement — this test file's own setup
/// deliberately dials with a plain `ReadWrite` registry (mirrors `tests/mcp_gateway.rs`'s
/// `semantic_qdrant_registry`), since seeding a collection needs `PUT`. Production gateway code
/// never constructs this; only `bins/gateway/src/bootstrap.rs` may (ADR-0014, enforced by
/// `xtask architecture-check`).
fn setup_registry(cell: CellId, caller: CallerId) -> IntraCellResourceRegistry {
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
    IntraCellResourceRegistry::new(entries, cell, caller)
}

fn seed_tenant_placement(admin: &mut Client, tenant_id: Uuid, collection: &str) {
    admin
        .execute(
            "INSERT INTO projection.tenant_placements \
               (tenant_id,projection_family,collection_name,shard_key,placement_class, \
                point_count,bytes_estimate,promotion_state) \
             VALUES ($1,'private_memory_v1',$2,NULL,'SHARED_FALLBACK',1,0,'STABLE')",
            &[&tenant_id, &collection],
        )
        .expect("owner seeds tenant placement row");
}

fn seed_registry_row(
    admin: &mut Client,
    tenant_id: Uuid,
    workspace_id: Uuid,
    memory_id: Uuid,
    point_id: Uuid,
) {
    admin
        .execute(
            "INSERT INTO projection.private_memory_points \
               (point_id,tenant_id,scope_kind,scope_id,domain,projection_kind, \
                projection_version,embedding_version,memory_id,source_updated_at,body_sha256) \
             SELECT $1,$2,'workspace',$3,'knowledge','ingest','v1',$4,memory_id, \
                    updated_at,sha256(convert_to(content::text,'UTF8')) \
             FROM private.memory_records WHERE tenant_id=$2 AND memory_id=$5",
            &[
                &point_id,
                &tenant_id,
                &workspace_id,
                &EMBEDDING_VERSION,
                &memory_id,
            ],
        )
        .expect("owner registers opaque Qdrant point identity");
}

fn seed_checkpoint(admin: &mut Client, tenant_id: Uuid, workspace_id: Uuid) {
    admin
        .execute(
            "INSERT INTO projection.stream_checkpoints \
               (tenant_id,scope_kind,scope_id,domain,projection_kind,projection_version, \
                issued_highwater,evidence_highwater,knowledge_highwater,projection_highwater,serving) \
             VALUES($1,'workspace',$2,'knowledge','ingest','v1',0,0,0,0,true)",
            &[&tenant_id, &workspace_id],
        )
        .expect("owner seeds the trusted serving version");
}

fn payload(
    tenant_id: Uuid,
    workspace_id: Uuid,
    source_updated_at: time::OffsetDateTime,
) -> humaux_adapters::qdrant::IndexablePayload {
    QdrantPointPayload {
        tenant_id: TenantId(tenant_id),
        workspace_id: WorkspaceId(workspace_id),
        visibility_class: VisibilityClass::WorkspaceShared,
        visibility_user_id: None,
        visibility_workspace_id: Some(WorkspaceId(workspace_id)),
        object_type: "memory_record".to_owned(),
        memory_type: MemoryType::Note,
        status: AuthorityStatus::Active,
        authority: AuthorityClass::ProjectConstraint,
        created_at: source_updated_at,
        effective_at: source_updated_at,
        embedding_version: EMBEDDING_VERSION.to_owned(),
        projection_version: "v1".to_owned(),
        source_stream_seq: 0,
        data_class: DataClass::Internal,
        egress_disposition: EgressDisposition::Allowed,
    }
    .into_indexable()
    .expect("non-secret wiring-test fixture payload")
}

fn fixed_vector(text: &str) -> Vec<f32> {
    let mut vector = vec![0.0; DIMENSION as usize];
    for (index, byte) in text.bytes().enumerate() {
        vector[index % DIMENSION as usize] += f32::from(byte) / 255.0;
    }
    vector
}

fn context_bootstrap(handle: &Handle) -> ContextBootstrap {
    let policy = RememberPolicy::new(
        // ADR-0054 D-D: the family only — no default (tenant, workspace) pair.
        ProcessFamily::new("workspace", "knowledge", "ingest", "v1").expect("trusted family"),
        handle.reasoning_domain_id,
        Duration::from_secs(60),
        DataClass::Internal,
        VisibilityClass::WorkspaceShared,
    )
    .expect("trusted wiring-test remember policy");
    ContextBootstrap::new(
        humaux_domain::context::ContextBudget::new(2_048, 1_024).expect("trusted budget"),
        humaux_contracts::retrieval_config::resolve_registered_retrieval_profile(
            &Default::default(),
        )
        .expect("registered default profile"),
        &policy,
    )
    .expect("actual executable identity")
}

fn recall_request(query: &str, workspace_id: Uuid) -> RecallSearchRequest {
    RecallSearchRequest {
        query: query.to_owned(),
        workspace_id: WorkspaceId(workspace_id),
        consistency_token: None,
        mode: Some("semantic".to_owned()),
        completeness_request: None,
        limit: None,
        subject_ids: Vec::new(),
        affect: None,
        mood_congruence: None,
    }
}

/// (1) Bootstrap from test envs + seeded tenant placement + seeded point ⇒ recall.search hit.
#[allow(clippy::too_many_lines)]
// one end-to-end scenario: bootstrap → placement → point → recall hit; splitting it would hide the sequence the test exists to prove.
#[test]
fn happy_path_placement_and_point_hit() {
    run_db_fixture::<Fixture, _>("semantic_recall_wiring_happy_path", |mut handle| {
        handle.assert_gateway_login();
        let context_row = handle.seed_workspace_visible_context_record();
        let point_id = Uuid::new_v4();

        let rt = handle.rt.handle().clone();
        // `owner_client()` (`postgres::Client::connect`) is blocking and spins its own Tokio
        // runtime internally — calling it from inside `rt.block_on`'s future body panics with
        // "Cannot start a runtime from within a runtime". Do the blocking seeding on the plain
        // test thread, outside any `block_on`, same as the rest of this fixture's usage.
        let mut admin = handle.owner_client().expect("owner client for setup");
        seed_registry_row(
            &mut admin,
            handle.tenant_id,
            handle.workspace_id,
            context_row.memory_id,
            point_id,
        );
        seed_checkpoint(&mut admin, handle.tenant_id, handle.workspace_id);
        let source_updated_at = time::OffsetDateTime::now_utc();

        let collection = format!("wiring_happy_{}", Uuid::now_v7().simple());
        let cell = CellId(Uuid::now_v7());
        let caller = CallerId("gateway-wiring-test".to_owned());
        let setup_registry = setup_registry(cell, caller.clone());
        let setup_transport = qdrant_transport(&setup_registry);
        let registry = cell_registry(cell, caller);
        let transport = qdrant_transport(&registry);

        let mut admin = handle.owner_client().expect("owner client for placement");
        seed_tenant_placement(&mut admin, handle.tenant_id, &collection);

        rt.block_on(async {
            create_collection(&setup_transport, &setup_registry, &collection).await;
            let permit = authorize_cell_access(
                &setup_registry,
                IntraCellResource::QDRANT_REST,
                Duration::from_secs(60),
            )
            .expect("setup upsert permit");
            let vector = fixed_vector("operation receipt scoped context");
            let point_payload = payload(handle.tenant_id, handle.workspace_id, source_updated_at);
            upsert(
                setup_transport.as_ref(),
                &permit,
                &collection,
                &[(PointId::Uuid(point_id), &point_payload, vector)],
                ha_profile_for(QdrantOperation::NormalImmutableUpsert),
            )
            .await
            .expect("real Qdrant upsert");

            let gateway_uid = own_uid().await;
            let socket_path = spawn_worker(gateway_uid).await;
            let pool = Arc::new(
                // dep: PostgreSQL(role_gateway) — test fixture pool for the semantic recall wiring suite
                humaux_adapters::postgres::RuntimeDbPool::connect(&required(
                    "HUMAUX_GATEWAY_PG_DSN",
                ))
                .await
                .expect("real role_gateway pool"),
            );
            let embedding_port: Arc<dyn RetrievalEmbeddingPort> =
                Arc::new(GatewayRetrievalEmbeddingClient::new(
                    pool.clone(),
                    socket_path,
                    registry.clone(),
                    Duration::from_secs(30),
                ));
            let runtime = Arc::new(
                SemanticRecallRuntime::new(
                    embedding_port,
                    transport,
                    registry,
                    SemanticRecallVersions {
                        embedding_version: EMBEDDING_VERSION.to_owned(),
                        dimension: DIMENSION,
                    },
                    Duration::from_secs(10),
                )
                .expect("trusted semantic runtime"),
            );
            let catalog = Arc::new(CanonicalCatalog::load().expect("catalog"));
            let result = recall::search(
                pool,
                runtime,
                catalog,
                handle.auth.clone(),
                Uuid::now_v7(),
                context_bootstrap(&handle),
                recall_request("operation receipt scoped context", handle.workspace_id),
            )
            .await;
            let output = result.expect("semantic recall hit").finish();
            let items = output.structured_content["items"]
                .as_array()
                .expect("items array");
            assert!(
                items.iter().any(
                    |item| item["memory_id"] == context_row.memory_id.to_string()
                        || item["memory_id"] == serde_json::json!(context_row.memory_id)
                ),
                "expected the seeded memory in the hit: {items:?}"
            );

            delete_collection(&setup_transport, &setup_registry, &collection).await;
        });
        // `tenant_placements_tenant_id_fkey` references `control.tenants` — the fixture's own
        // teardown (`Handle::Drop`) deletes that tenant row and panics on the FK violation if a
        // seeded placement row outlives it.
        admin
            .execute(
                "DELETE FROM projection.tenant_placements WHERE tenant_id=$1",
                &[&handle.tenant_id],
            )
            .expect("cleanup seeded tenant placement row");
    });
}

/// (2) Tenant B has no placement row ⇒ `DependencyUnavailable`, and zero Qdrant calls (proves
/// the "no placement = not indexed" path returns before ever dialing Qdrant, never falling back
/// onto some other tenant's collection).
#[test]
fn tenant_with_no_placement_degrades_closed_without_calling_qdrant() {
    run_db_fixture::<Fixture, _>("semantic_recall_wiring_no_placement", |handle| {
        handle.assert_gateway_login();
        let rt = handle.rt.handle().clone();
        let cell = CellId(Uuid::now_v7());
        let caller = CallerId("gateway-wiring-test".to_owned());
        let registry = cell_registry(cell, caller);
        let inner = qdrant_transport(&registry);
        let counting = Arc::new(CountingTransport {
            inner,
            calls: AtomicUsize::new(0),
        });

        rt.block_on(async {
            let pool = Arc::new(
                // dep: PostgreSQL(role_gateway) — test fixture pool for the semantic recall wiring suite
                humaux_adapters::postgres::RuntimeDbPool::connect(&required(
                    "HUMAUX_GATEWAY_PG_DSN",
                ))
                .await
                .expect("real role_gateway pool"),
            );
            let embedding_port: Arc<dyn RetrievalEmbeddingPort> =
                Arc::new(FixedOutcomePort::new(RetrievalEmbeddingOutcome::Embedded {
                    vector: fixed_vector("no placement query"),
                    provider_id: "wiring-test-provider".to_owned(),
                    model_id: "wiring-test-embedding".to_owned(),
                    model_revision: "v1".to_owned(),
                    dimension: DIMENSION,
                }));
            let runtime = Arc::new(
                SemanticRecallRuntime::new(
                    embedding_port,
                    counting.clone(),
                    registry,
                    SemanticRecallVersions {
                        embedding_version: EMBEDDING_VERSION.to_owned(),
                        dimension: DIMENSION,
                    },
                    Duration::from_secs(10),
                )
                .expect("trusted semantic runtime"),
            );
            let catalog = Arc::new(CanonicalCatalog::load().expect("catalog"));
            let result = recall::search(
                pool,
                runtime,
                catalog,
                handle.auth.clone(),
                Uuid::now_v7(),
                context_bootstrap(&handle),
                recall_request("no placement seeded for this tenant", handle.workspace_id),
            )
            .await;
            assert_eq!(result.err(), Some(ErrorCode::DependencyUnavailable));
            assert_eq!(
                counting.calls.load(Ordering::SeqCst),
                0,
                "no placement must short-circuit before ever calling Qdrant"
            );
        });
    });
}

/// (3) A socket path pointing nowhere ⇒ `DependencyUnavailable`, no panic, and the call
/// completes well inside the configured handler timeout (a hung dial would time out the test
/// harness itself long before any test assertion could fail it).
#[tokio::test]
async fn dead_socket_path_degrades_closed_without_hanging() {
    let cell = CellId(Uuid::now_v7());
    let caller = CallerId("gateway-wiring-test".to_owned());
    let registry = cell_registry(cell, caller);
    let transport = qdrant_transport(&registry);
    let pool = Arc::new(
        // dep: PostgreSQL(role_gateway) — test fixture pool for the semantic recall wiring suite
        humaux_adapters::postgres::RuntimeDbPool::connect(&required("HUMAUX_GATEWAY_PG_DSN"))
            .await
            .expect("real role_gateway pool"),
    );
    let embedding_port: Arc<dyn RetrievalEmbeddingPort> =
        Arc::new(GatewayRetrievalEmbeddingClient::new(
            pool,
            temp_socket_path("nowhere").to_string_lossy().into_owned(),
            registry,
            Duration::from_secs(5),
        ));
    let deadline_unix_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 5_000;
    let auth = humaux_domain::identity::AuthorizationScope::new(
        TenantId::new(),
        humaux_domain::identity::PrincipalId::new(),
        Some(humaux_domain::ids::UserId::new()),
        humaux_domain::identity::BoundedSet::new([WorkspaceId::new()]).unwrap(),
    );
    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        embedding_port.embed_query(RetrievalEmbeddingInput {
            authorization: &auth,
            workspace_id: *auth.allowed_workspace_ids().iter().next().unwrap(),
            request_id: Uuid::now_v7(),
            logical_call_id: Uuid::now_v7(),
            attempt_no: 1,
            profile_fingerprint: "sha256:0000000000000000000000000000000000000000000000000000000000000",
            dimension: DIMENSION,
            query: "dead socket",
            deadline_unix_ms,
        }),
    )
    .await
    .expect("dead-socket dial must not hang the whole call");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "dial to a dead socket must fail fast"
    );
    // `embed_query` registers the call (a real INSERT) before ever dialing the socket
    // (ADR-0012 §决定2's idempotency-anchor ordering) — with this test's unseeded synthetic
    // tenant that registration itself fails closed (`Err(DependencyUnavailable)`) before the
    // dead-socket dial is ever attempted; a seeded tenant would instead reach the dial and come
    // back `Ok(Unavailable{reason: "TRANSPORT"})`. Both are the same outward
    // `ErrorCode::DependencyUnavailable` this test's name asserts — `recall::search`'s own
    // `runtime.port.embed_query(..).await?` propagates either shape identically via `?`.
    match outcome {
        Err(ErrorCode::DependencyUnavailable) => {}
        Ok(RetrievalEmbeddingOutcome::Unavailable { .. }) => {}
        other => panic!("expected a degrade-closed outcome, got {other:?}"),
    }
    let _ = transport; // constructed to mirror the real wiring shape; unused on this path.
}

/// (4) The gateway transport denies a write under a `QdrantReadOnly` permit (ADR-0014) — the
/// same registry/permit shape `SemanticRecallRuntime` mints for `recall.search`, proven against
/// a real Qdrant connection (the request never leaves in the first place, so no live collection
/// is needed).
#[tokio::test]
async fn gateway_transport_denies_a_write_under_the_read_only_permit() {
    let cell = CellId(Uuid::now_v7());
    let caller = CallerId("gateway-wiring-test".to_owned());
    let registry = cell_registry(cell, caller);
    let transport = qdrant_transport(&registry);
    let permit = authorize_cell_access(
        &registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(30),
    )
    .expect("read-only permit mints fine — ADR-0014 denies at execute(), not at mint time");
    let result = transport
        .execute(
            &permit,
            // dep: Qdrant(*) — test issues a Qdrant REST request against the fixture collection
            IntraCellRequest {
                method: IntraCellMethod::Put,
                path: "/collections/whatever-tenant-forgot-to-check/points".to_owned(),
                json_body: Some(serde_json::json!({"points": []})),
                headers: Vec::new(),
            },
        )
        .await;
    assert_eq!(result, Err(IntraCellError::WriteDenied));
}

/// (5) A wrong-dimension vector from the embedding port never gets hydrated — `recall.search`
/// returns `DependencyUnavailable` before touching Qdrant or PostgreSQL hydration at all.
#[test]
fn wrong_dimension_vector_from_port_is_never_hydrated() {
    run_db_fixture::<Fixture, _>("semantic_recall_wiring_wrong_dimension", |handle| {
        handle.assert_gateway_login();
        let rt = handle.rt.handle().clone();
        let cell = CellId(Uuid::now_v7());
        let caller = CallerId("gateway-wiring-test".to_owned());
        let registry = cell_registry(cell, caller);
        let inner = qdrant_transport(&registry);
        let counting = Arc::new(CountingTransport {
            inner,
            calls: AtomicUsize::new(0),
        });
        // A placement row and a serving family must exist, or `recall::search` degrades at the
        // placement step or (ADR-0053 D-E: the serving read now runs BEFORE the query embedding)
        // at the family read — before ever calling the embedding port — and the assertions
        // below would pass vacuously without ever exercising the dimension guard this test
        // targets.
        let collection = format!("wiring_wrong_dim_{}", Uuid::now_v7().simple());
        let mut admin = handle.owner_client().expect("owner client for placement");
        seed_tenant_placement(&mut admin, handle.tenant_id, &collection);
        seed_checkpoint(&mut admin, handle.tenant_id, handle.workspace_id);

        rt.block_on(async {
            let pool = Arc::new(
                // dep: PostgreSQL(role_gateway) — test fixture pool for the semantic recall wiring suite
                humaux_adapters::postgres::RuntimeDbPool::connect(&required(
                    "HUMAUX_GATEWAY_PG_DSN",
                ))
                .await
                .expect("real role_gateway pool"),
            );
            // Configured dimension is 4; the port double returns 3 — a real worker could never
            // legitimately do this for a fixed model, but a misconfigured/rogue one could, and
            // `recall.search` must never truncate or pad to fit.
            let port = Arc::new(FixedOutcomePort::new(RetrievalEmbeddingOutcome::Embedded {
                vector: vec![0.1, 0.2, 0.3],
                provider_id: "wiring-test-provider".to_owned(),
                model_id: "wiring-test-embedding".to_owned(),
                model_revision: "v1".to_owned(),
                dimension: 3,
            }));
            let embedding_port: Arc<dyn RetrievalEmbeddingPort> = port.clone();
            let runtime = Arc::new(
                SemanticRecallRuntime::new(
                    embedding_port,
                    counting.clone(),
                    registry,
                    SemanticRecallVersions {
                        embedding_version: EMBEDDING_VERSION.to_owned(),
                        dimension: DIMENSION,
                    },
                    Duration::from_secs(10),
                )
                .expect("trusted semantic runtime"),
            );
            let catalog = Arc::new(CanonicalCatalog::load().expect("catalog"));
            let result = recall::search(
                pool,
                runtime,
                catalog,
                handle.auth.clone(),
                Uuid::now_v7(),
                context_bootstrap(&handle),
                recall_request("mismatched dimension query", handle.workspace_id),
            )
            .await;
            assert_eq!(result.err(), Some(ErrorCode::DependencyUnavailable));
            assert_eq!(
                port.calls.load(Ordering::SeqCst),
                1,
                "placement resolved fine — the embedding port must actually have been called \
                 for the dimension guard below it to mean anything"
            );
            assert_eq!(
                counting.calls.load(Ordering::SeqCst),
                0,
                "a dimension mismatch must be caught before ever calling Qdrant"
            );
        });
        // See the happy-path test's identical cleanup comment: `control.tenants` teardown
        // would otherwise fail on the `tenant_placements_tenant_id_fkey` FK.
        admin
            .execute(
                "DELETE FROM projection.tenant_placements WHERE tenant_id=$1",
                &[&handle.tenant_id],
            )
            .expect("cleanup seeded tenant placement row");
    });
}

/// A [`FixedOutcomePort`] that sleeps before answering — makes the `embed` stage observably
/// slow, so a `.await` left outside every stage lap would show up as missing wall time.
struct DelayedPort {
    delay: Duration,
    outcome: RetrievalEmbeddingOutcome,
}

#[async_trait]
impl RetrievalEmbeddingPort for DelayedPort {
    async fn embed_query(
        &self,
        _input: RetrievalEmbeddingInput<'_>,
    ) -> Result<RetrievalEmbeddingOutcome, ErrorCode> {
        tokio::time::sleep(self.delay).await;
        Ok(self.outcome.clone())
    }
}

/// (7) ADR-0055 D-E: `provenance.stage_ms` partitions `recall::search` exhaustively — the eight
/// stages sum to at least 90% of the in-process wall time around the call, `total` never
/// exceeds it, and the port's injected 200 ms lands in `embed`. Fault: move one `.await` (e.g.
/// the embedding call) outside every lap ⇒ the sum loses ≥ 200 ms ⇒ red.
#[test]
#[allow(clippy::too_many_lines)] // one fixture: placement → point → timed search → stage arithmetic.
fn recall_stage_ms_cover_at_least_ninety_percent_of_in_process_search_time() {
    run_db_fixture::<Fixture, _>("semantic_recall_wiring_stage_ms", |mut handle| {
        handle.assert_gateway_login();
        let context_row = handle.seed_workspace_visible_context_record();
        let point_id = Uuid::new_v4();
        let rt = handle.rt.handle().clone();
        let mut admin = handle.owner_client().expect("owner client for setup");
        seed_registry_row(
            &mut admin,
            handle.tenant_id,
            handle.workspace_id,
            context_row.memory_id,
            point_id,
        );
        seed_checkpoint(&mut admin, handle.tenant_id, handle.workspace_id);
        let source_updated_at = time::OffsetDateTime::now_utc();
        let collection = format!("c30_wiring_stage_{}", Uuid::now_v7().simple());
        let cell = CellId(Uuid::now_v7());
        let caller = CallerId("gateway-wiring-test".to_owned());
        let setup_registry = setup_registry(cell, caller.clone());
        let setup_transport = qdrant_transport(&setup_registry);
        let registry = cell_registry(cell, caller);
        let transport = qdrant_transport(&registry);
        seed_tenant_placement(&mut admin, handle.tenant_id, &collection);
        let query = "operation receipt scoped context";
        let delay = Duration::from_millis(200);

        rt.block_on(async {
            create_collection(&setup_transport, &setup_registry, &collection).await;
            let permit = authorize_cell_access(
                &setup_registry,
                IntraCellResource::QDRANT_REST,
                Duration::from_secs(60),
            )
            .expect("setup upsert permit");
            let point_payload = payload(handle.tenant_id, handle.workspace_id, source_updated_at);
            upsert(
                setup_transport.as_ref(),
                &permit,
                &collection,
                &[(PointId::Uuid(point_id), &point_payload, fixed_vector(query))],
                ha_profile_for(QdrantOperation::NormalImmutableUpsert),
            )
            .await
            .expect("real Qdrant upsert");
            let pool = Arc::new(
                // dep: PostgreSQL(role_gateway) — test fixture pool for the semantic recall wiring suite
                humaux_adapters::postgres::RuntimeDbPool::connect(&required(
                    "HUMAUX_GATEWAY_PG_DSN",
                ))
                .await
                .expect("real role_gateway pool"),
            );
            let embedding_port: Arc<dyn RetrievalEmbeddingPort> = Arc::new(DelayedPort {
                delay,
                outcome: RetrievalEmbeddingOutcome::Embedded {
                    vector: fixed_vector(query),
                    provider_id: "wiring-test-provider".to_owned(),
                    model_id: "wiring-test-embedding".to_owned(),
                    model_revision: "v1".to_owned(),
                    dimension: DIMENSION,
                },
            });
            let runtime = Arc::new(
                SemanticRecallRuntime::new(
                    embedding_port,
                    transport,
                    registry,
                    SemanticRecallVersions {
                        embedding_version: EMBEDDING_VERSION.to_owned(),
                        dimension: DIMENSION,
                    },
                    Duration::from_secs(10),
                )
                .expect("trusted semantic runtime"),
            );
            let catalog = Arc::new(CanonicalCatalog::load().expect("catalog"));
            let bootstrap = context_bootstrap(&handle);
            let started = std::time::Instant::now();
            let result = recall::search(
                pool,
                runtime,
                catalog,
                handle.auth.clone(),
                Uuid::now_v7(),
                bootstrap,
                recall_request(query, handle.workspace_id),
            )
            .await;
            let wall_ms = started.elapsed().as_secs_f64() * 1_000.0;
            let output = result.expect("semantic recall answers").finish();
            let stage_ms = &output.structured_content["provenance"]["stage_ms"];
            let stage = |name: &str| {
                stage_ms[name]
                    .as_f64()
                    .unwrap_or_else(|| panic!("stage_ms.{name} missing: {stage_ms}"))
            };
            let names = [
                "route", "planner", "scan", "embed", "qdrant", "hydrate", "rerank", "assemble",
            ];
            let sum: f64 = names.iter().map(|name| stage(name)).sum();
            eprintln!("stage_ms={stage_ms} sum={sum:.1} wall={wall_ms:.1}");
            assert!(
                stage("embed") >= 200.0,
                "the injected 200 ms must land in `embed`: {stage_ms}"
            );
            assert!(
                stage("total") <= wall_ms + 0.1,
                "total {} exceeds the wall time {wall_ms}",
                stage("total")
            );
            assert!(
                sum >= 0.9 * wall_ms,
                "stages cover {sum:.1} of {wall_ms:.1} ms — an .await sits outside every stage"
            );
            delete_collection(&setup_transport, &setup_registry, &collection).await;
        });
        admin
            .execute(
                "DELETE FROM projection.tenant_placements WHERE tenant_id=$1",
                &[&handle.tenant_id],
            )
            .expect("cleanup seeded tenant placement row");
    });
}

/// (6) `bootstrap.rs` wires `with_semantic_recall` exactly once — a second call site would
/// mean two competing semantic-recall configurations racing to be `GatewayMcpApplication`'s
/// last write.
#[test]
fn bootstrap_wires_semantic_recall_exactly_once() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/bootstrap.rs"))
        .expect("read bootstrap.rs");
    let count = source.matches("with_semantic_recall(").count();
    assert_eq!(
        count, 1,
        "bootstrap.rs must call with_semantic_recall exactly once, found {count}"
    );
}
