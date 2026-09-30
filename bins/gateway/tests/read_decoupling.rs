//! `gateway::tests::read_decoupling` — ADR-0053 D-E wire tests: PG-authoritative reads without a serving version, the
//!   B-shaped read, and the PROVISIONING write gate.
//! Depends-on: crates=[async-trait, axum, humaux-adapters, humaux-application, humaux-contracts, humaux-domain,
//!   humaux-infra-cell, humaux-local-secret-scan, humaux-protocol, humaux-testkit, serde_json,
//!   tokio, uuid]; services=[HTTP(gateway), PostgreSQL(owner) r=[ops.commit_seq_seq, ops.outbox, private.evidence_objects]
//!   w=[control.workspaces, projection.stream_checkpoints, projection.stream_log, projection.tenant_placements],
//!   Qdrant(*)]; env=[HUMAUX_TEST_GITLEAKS_BIN, HUMAUX_TEST_GITLEAKS_SHA256, HUMAUX_TEST_GITLEAKS_VERSION,
//!   HUMAUX_TEST_QDRANT_PORT]; modules=[adapters::postgres, adapters::qdrant, adapters::quota_repo,
//!   adapters::tests::support::operation_receipt_fixture, application::retrieval_embedding_port,
//!   contracts::retrieval_config, domain::context, domain::dataclass, domain::error, domain::identity, gateway::context, gateway::guard, gateway::mcp_application, gateway::recall, gateway::remember,
//!   humaux-local-secret-scan, humaux-testkit, infra-cell::permit, infra-cell::resource, infra-cell::transport,
//!   protocol::edge, protocol::mcp, protocol::mcp_catalog]
//! Called-by: [cargo-test]
//! Invariants: [each test runs against the isolated operation-receipt fixture tenant (torn down by the fixture); the
//!   embedding port is a counting double, so "no embedding call on B" is observed; the READY test deletes its own
//!   Qdrant collection on drop]
//! Spec: Baseline §16.2; §22.4; §23.1; §52.3; ADR-0031; ADR-0053
//!
//! Every test drives ONE in-process gateway over real loopback MCP against the isolated
//! operation-receipt PostgreSQL fixture (its own tenant, torn down by the fixture). The
//! semantic lane is wired with a counting embedding port (no provider, no retrieval worker), so
//! "no embedding call on the B path" is an observed count, not an inference; the one test that
//! needs a real index face creates and deletes its own Qdrant collection.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use humaux_adapters::{
    postgres::RuntimeDbPool,
    qdrant::{
        Distance, ShardingMethod, create_collection_body, subject_index_body, tenant_index_body,
    },
    quota_repo::RatePolicy,
};
use humaux_application::retrieval_embedding_port::{
    RetrievalEmbeddingInput, RetrievalEmbeddingOutcome, RetrievalEmbeddingPort,
};
use humaux_domain::{
    context::ContextBudget, dataclass::DataClass, error::ErrorCode, identity::VisibilityClass,
};
use humaux_gateway::{
    context::ContextBootstrap,
    guard::{GatewayGuard, GuardRatePolicies, GuardSettings},
    mcp_application::GatewayMcpApplication,
    recall::{SemanticRecallRuntime, SemanticRecallVersions},
    remember::{ProcessFamily, RememberEventKind, RememberPolicy},
};
use humaux_infra_cell::{
    CallerId, CellId, DEFAULT_MAX_RESPONSE_BYTES, HttpIntraCellTransport, IntraCellHttpTransport,
    IntraCellMethod, IntraCellRequest, IntraCellResource, IntraCellResourceRegistry, ResourceEntry,
    authorize_cell_access,
};
use humaux_local_secret_scan::{LocalSecretScanner, LocalSecretScannerConfig};
use humaux_protocol::{
    edge::{TrustedProxyConfig, compute_api_key_hash},
    mcp::{McpAdapter, McpHttpConfig, ToolName},
    mcp_catalog::CanonicalCatalog,
};
use humaux_testkit::run_db_fixture;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use uuid::Uuid;

#[allow(dead_code)]
#[path = "../../../crates/adapters/tests/support/operation_receipt_fixture.rs"]
mod operation_receipt_fixture;

use operation_receipt_fixture::{
    Fixture, Handle, SYNTHETIC_CREDENTIAL_PEPPER, SyntheticCredentialScopes,
};

const HOST: &str = "mcp.test";
const ORIGIN: &str = "https://mcp.test";
const DIMENSION: u32 = 4;
const QUERY: &str = "operation receipt scoped context";

// ----------------------------------------------------------------------------------------------
// One in-process gateway
// ----------------------------------------------------------------------------------------------

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

/// The embedding seam, counted: the B path must never call it.
struct CountingPort {
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl RetrievalEmbeddingPort for CountingPort {
    async fn embed_query(
        &self,
        _input: RetrievalEmbeddingInput<'_>,
    ) -> Result<RetrievalEmbeddingOutcome, ErrorCode> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(RetrievalEmbeddingOutcome::Embedded {
            vector: vec![0.5; DIMENSION as usize],
            provider_id: "read-decoupling-test".to_owned(),
            model_id: "counting-port".to_owned(),
            model_revision: "v1".to_owned(),
            dimension: DIMENSION,
        })
    }
}

fn scanner() -> Arc<LocalSecretScanner> {
    Arc::new(
        LocalSecretScanner::new(LocalSecretScannerConfig {
            executable: PathBuf::from(
                std::env::var("HUMAUX_TEST_GITLEAKS_BIN")
                    .expect("read_decoupling requires HUMAUX_TEST_GITLEAKS_BIN"),
            ),
            expected_version: std::env::var("HUMAUX_TEST_GITLEAKS_VERSION")
                .expect("read_decoupling requires HUMAUX_TEST_GITLEAKS_VERSION"),
            expected_executable_sha256: std::env::var("HUMAUX_TEST_GITLEAKS_SHA256")
                .expect("read_decoupling requires HUMAUX_TEST_GITLEAKS_SHA256"),
            timeout: Duration::from_secs(5),
            max_payload_bytes: 64 * 1024,
            finding_exit_code: 1,
        })
        .expect("pinned scanner"),
    )
}

fn qdrant_registry() -> IntraCellResourceRegistry {
    let cell = CellId(Uuid::now_v7());
    let caller = CallerId("gateway-read-decoupling".to_owned());
    let port = std::env::var("HUMAUX_TEST_QDRANT_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(6333);
    let mut entries = BTreeMap::new();
    entries.insert(
        IntraCellResource::QDRANT_REST,
        ResourceEntry::new(
            "127.0.0.1",
            port,
            cell,
            vec!["127.0.0.1/32".parse().expect("loopback CIDR")],
            BTreeSet::from([caller.clone()]),
            false,
        )
        .expect("loopback Qdrant resource"),
    );
    IntraCellResourceRegistry::new(entries, cell, caller)
}

struct Gateway {
    port: Arc<CountingPort>,
    transport: Arc<HttpIntraCellTransport>,
    registry: IntraCellResourceRegistry,
    app: GatewayMcpApplication,
}

fn gateway(handle: &Handle) -> Gateway {
    let policy = RememberPolicy::new(
        // ADR-0054 D-D: the family only — no default (tenant, workspace) pair.
        ProcessFamily::new("workspace", "knowledge", "ingest", "v1").expect("trusted family"),
        handle.reasoning_domain_id,
        Duration::from_secs(60),
        DataClass::Internal,
        VisibilityClass::WorkspaceShared,
    )
    .expect("trusted fixture remember policy");
    let bootstrap = ContextBootstrap::new(
        ContextBudget::new(2_048, 1_024).expect("trusted budget"),
        humaux_contracts::retrieval_config::resolve_registered_retrieval_profile(
            &Default::default(),
        )
        .expect("registered profile"),
        &policy,
    )
    .expect("actual executable identity");
    let runtime = handle
        .rt
        .block_on(handle.fresh_runtime())
        .expect("fresh Gateway runtime");
    let registry = qdrant_registry();
    let transport = Arc::new(
        HttpIntraCellTransport::new(
            registry.clone(),
            Duration::from_secs(10),
            DEFAULT_MAX_RESPONSE_BYTES,
        )
        .expect("Qdrant transport"),
    );
    let port = Arc::new(CountingPort {
        calls: AtomicUsize::new(0),
    });
    let semantic = SemanticRecallRuntime::new(
        scanner(),
        port.clone(),
        transport.clone(),
        registry.clone(),
        SemanticRecallVersions {
            embedding_version: "embed-v1".to_owned(),
            dimension: DIMENSION,
        },
        Duration::from_secs(10),
    )
    .expect("semantic runtime");
    let app = GatewayMcpApplication::new(
        CanonicalCatalog::load().expect("closed canonical MCP catalog"),
        guard(runtime),
        policy,
        RememberEventKind::UserMessage,
        bootstrap,
    )
    .with_confirm_token_ttl(Duration::from_secs(300))
    .expect("positive confirm-token TTL")
    .with_undo_window(Duration::from_secs(86_400))
    .expect("positive undo window")
    .with_mood_half_life(Duration::from_secs(21_600))
    .expect("positive mood half-life")
    .with_semantic_recall(semantic);
    Gateway {
        port,
        transport,
        registry,
        app,
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

async fn tool_call(address: SocketAddr, tool: &str, bearer: &str, arguments: Value) -> Value {
    let body = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
    "name": tool, "arguments": arguments, "_meta": {
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientInfo": {"name": "read-decoupling", "version": "1"},
        "io.modelcontextprotocol/clientCapabilities": {},
    }}})
    .to_string();
    // dep: HTTP(gateway) — the test dials the in-process gateway's loopback listener
    let mut stream = TcpStream::connect(address).await.expect("connect gateway");
    let request = format!(
        "POST /mcp HTTP/1.1\r\nConnection: close\r\nAccept: application/json, text/event-stream\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\nHost: {HOST}\r\nOrigin: {ORIGIN}\r\n\
         MCP-Protocol-Version: 2026-07-28\r\nMcp-Method: tools/call\r\nMcp-Name: {tool}\r\n\
         Authorization: {bearer}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.expect("write");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.expect("read");
    let response = String::from_utf8(response).expect("utf8");
    let (head, body) = response.split_once("\r\n\r\n").expect("http body");
    assert!(head.starts_with("HTTP/1.1 200"), "{head}\n{body}");
    serde_json::from_str(body).expect("json-rpc body")
}

/// The tool succeeded; its structured content satisfies the advertised schema.
fn ok_content(response: &Value, tool: ToolName) -> Value {
    let result = &response["result"];
    assert_ne!(result["isError"], true, "expected success: {response}");
    let value = result["structuredContent"].clone();
    CanonicalCatalog::load()
        .expect("catalog")
        .validate_output(tool, &value)
        .expect("response satisfies its advertised schema");
    value
}

fn tool_error_code(response: &Value) -> &str {
    assert_eq!(
        response["result"]["isError"], true,
        "expected tool error: {response}"
    );
    response["result"]["structuredContent"]["code"]
        .as_str()
        .expect("error code")
}

// ----------------------------------------------------------------------------------------------
// Fixture pairs
// ----------------------------------------------------------------------------------------------

/// How a seeded workspace's `(knowledge, ingest)` family looks.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Family {
    /// No checkpoint row: never provisioned.
    Uninitialized,
    /// Checkpoint row, no serving version (the onboarding PROVISIONING window / a LEGACY pair
    /// never activated).
    Unserved,
    /// Checkpoint row serving `v1` (a READY family).
    Serving,
}

struct Pair {
    workspace_id: Uuid,
    bearer: String,
    memory_id: Option<Uuid>,
}

/// A second workspace of the fixture tenant with its own credential, the family in `family`'s
/// state and, when `with_memory`, one WORKSPACE_SHARED memory with its mandatory binding.
fn seed_pair(handle: &mut Handle, tag: &str, family: Family, with_memory: bool) -> Pair {
    let workspace_id = handle.seed_workspace();
    let own = std::mem::replace(&mut handle.workspace_id, workspace_id);
    let prefix = format!("{tag}{}", &Uuid::now_v7().simple().to_string()[..12]);
    let wire = format!("{prefix}.{}", tag.repeat(32));
    let credential = handle.seed_synthetic_service_credential(
        SyntheticCredentialScopes::RememberWriteAndContextRead,
        &prefix,
        &wire,
        &compute_api_key_hash(SYNTHETIC_CREDENTIAL_PEPPER, &wire),
    );
    let memory_id = with_memory.then(|| handle.seed_workspace_visible_context_record().memory_id);
    if family != Family::Uninitialized {
        handle
            .admin
            .execute(
                "INSERT INTO projection.stream_checkpoints \
                   (tenant_id,scope_kind,scope_id,domain,projection_kind,projection_version,serving) \
                 VALUES($1,'workspace',$2,'knowledge','ingest','v1',$3)",
                &[
                    &handle.tenant_id,
                    &workspace_id,
                    &(family == Family::Serving),
                ],
            )
            .expect("owner seeds the family checkpoint");
    }
    handle.workspace_id = own;
    Pair {
        workspace_id,
        bearer: credential.bearer,
        memory_id,
    }
}

fn seed_placement(handle: &mut Handle, collection: &str) {
    handle
        .admin
        .execute(
            "INSERT INTO projection.tenant_placements \
               (tenant_id,projection_family,collection_name,shard_key,placement_class, \
                point_count,bytes_estimate,promotion_state) \
             VALUES ($1,'private_memory_v1',$2,NULL,'SHARED_FALLBACK',0,0,'STABLE')",
            &[&handle.tenant_id, &collection],
        )
        .expect("owner seeds the tenant placement");
}

fn get_args(pair: &Pair, memory_id: Uuid) -> Value {
    json!({"action":"get","memory_id":memory_id,"workspace_id":pair.workspace_id})
}

fn enumerate_args(pair: &Pair) -> Value {
    json!({"action":"enumerate","workspace_id":pair.workspace_id,"limit":100})
}

fn recall_args(pair: &Pair) -> Value {
    json!({"query":QUERY,"workspace_id":pair.workspace_id,"mode":"semantic"})
}

fn context_args(pair: &Pair) -> Value {
    json!({"workspace_id":pair.workspace_id})
}

/// ADR-0053 D-E's B shape, every field that differs from a normal empty result.
fn assert_b_shape(content: &Value, label: &str) {
    assert_eq!(content["items"], json!([]), "{label}: {content}");
    let completeness = &content["completeness"];
    assert_eq!(
        completeness["class"], "cannot_establish",
        "{label}: {content}"
    );
    assert_eq!(
        completeness["reason"], "no_serving_projection",
        "{label}: {content}"
    );
    assert_eq!(completeness["exact"], Value::Null, "{label}");
    assert_eq!(completeness["known_lower_bound"], Value::Null, "{label}");
    assert_eq!(
        completeness["lanes"],
        json!({"semantic": "failed"}),
        "{label}"
    );
    let projection = &content["pipeline"]["projection"];
    assert_eq!(projection["visible"], Value::Null, "{label}");
    assert_eq!(projection["completeness_ratio"], Value::Null, "{label}");
    assert_eq!(projection["current"], false, "{label}");
    assert_eq!(
        content["provenance"]["projection_version"],
        json!({"status": "cannot_establish"}),
        "{label}"
    );
    assert_eq!(
        content["provenance"]["embedding_model_id"],
        json!({"status": "not_applicable"}),
        "{label}"
    );
    assert_eq!(content["freshness"]["class"], "unknown", "{label}");
}

// ----------------------------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------------------------

#[test]
fn memory_get_on_initialized_unserved_family_is_pg_served() {
    run_db_fixture::<Fixture, _>(
        "memory_get_on_initialized_unserved_family_is_pg_served",
        |mut handle| {
            handle.seed_current_entitlement_and_window(100);
            let pair = seed_pair(&mut handle, "g", Family::Unserved, true);
            let gw = gateway(&handle);
            handle.rt.block_on(async {
                let (address, server) = start(gw.app).await;
                let memory_id = pair.memory_id.expect("seeded memory");
                let response =
                    tool_call(address, "memory", &pair.bearer, get_args(&pair, memory_id)).await;
                let content = ok_content(&response, ToolName::Memory);
                assert_eq!(
                    content["items"][0]["memory_id"],
                    memory_id.to_string(),
                    "{content}"
                );
                // The index was not counted (no serving version): honest, never `exact`.
                assert_eq!(content["pipeline"]["projection"]["visible"], Value::Null);
                assert_ne!(content["completeness"]["class"], "exact");
                server.abort();
            });
        },
    );
}

#[test]
fn memory_enumerate_on_initialized_unserved_family_reports_index_count_unavailable_not_complete() {
    run_db_fixture::<Fixture, _>(
        "memory_enumerate_on_initialized_unserved_family_reports_index_count_unavailable_not_complete",
        |mut handle| {
            handle.seed_current_entitlement_and_window(100);
            let pair = seed_pair(&mut handle, "e", Family::Unserved, true);
            let gw = gateway(&handle);
            handle.rt.block_on(async {
                let (address, server) = start(gw.app).await;
                let response =
                    tool_call(address, "memory", &pair.bearer, enumerate_args(&pair)).await;
                let content = ok_content(&response, ToolName::Memory);
                let items = content["content"]["items"].as_array().expect("items");
                assert_eq!(items.len(), 1, "PG-served: the memory is listed: {content}");
                let completeness = &content["content"]["completeness"];
                assert_eq!(completeness["class"], "cannot_establish", "{content}");
                assert_eq!(
                    completeness["reason"], "index_count_unavailable",
                    "{content}"
                );
                assert_eq!(
                    completeness["exact"],
                    Value::Null,
                    "never a complete census: {content}"
                );
                server.abort();
            });
        },
    );
}

#[test]
fn memory_get_on_uninitialized_pair_stays_dependency_unavailable() {
    run_db_fixture::<Fixture, _>(
        "memory_get_on_uninitialized_pair_stays_dependency_unavailable",
        |mut handle| {
            handle.seed_current_entitlement_and_window(100);
            let pair = seed_pair(&mut handle, "u", Family::Uninitialized, true);
            let gw = gateway(&handle);
            handle.rt.block_on(async {
                let (address, server) = start(gw.app).await;
                let memory_id = pair.memory_id.expect("seeded memory");
                for (tool, args) in [
                    ("memory", get_args(&pair, memory_id)),
                    ("memory", enumerate_args(&pair)),
                    ("context", context_args(&pair)),
                    ("recall", recall_args(&pair)),
                ] {
                    let response = tool_call(address, tool, &pair.bearer, args).await;
                    assert_eq!(
                        tool_error_code(&response),
                        "DEPENDENCY_UNAVAILABLE",
                        "{tool}: {response}"
                    );
                }
                server.abort();
            });
        },
    );
}

#[test]
fn recall_on_unactivated_family_returns_b_shape_without_embedding_call() {
    run_db_fixture::<Fixture, _>(
        "recall_on_unactivated_family_returns_b_shape_without_embedding_call",
        |mut handle| {
            handle.seed_current_entitlement_and_window(100);
            seed_placement(&mut handle, "read_decoupling_never_dialled");
            let pair = seed_pair(&mut handle, "r", Family::Unserved, true);
            let gw = gateway(&handle);
            let port = gw.port.clone();
            handle.rt.block_on(async {
                let (address, server) = start(gw.app).await;
                let response = tool_call(address, "recall", &pair.bearer, recall_args(&pair)).await;
                let content = ok_content(&response, ToolName::Recall);
                assert_b_shape(&content, "recall");
                assert_eq!(content["completeness"]["candidate_count"], 0);
                assert_eq!(
                    port.calls.load(Ordering::SeqCst),
                    0,
                    "no provider egress on B"
                );
                server.abort();
            });
        },
    );
}

#[test]
fn context_on_unactivated_family_returns_b_shape() {
    run_db_fixture::<Fixture, _>(
        "context_on_unactivated_family_returns_b_shape",
        |mut handle| {
            handle.seed_current_entitlement_and_window(100);
            let pair = seed_pair(&mut handle, "c", Family::Unserved, true);
            let gw = gateway(&handle);
            handle.rt.block_on(async {
                let (address, server) = start(gw.app).await;
                let response =
                    tool_call(address, "context", &pair.bearer, context_args(&pair)).await;
                let value = ok_content(&response, ToolName::Context);
                assert_b_shape(&value["content"], "context");
                assert!(
                    value["handoff"]["snapshot_token_sha256"].is_string(),
                    "{value}"
                );
                server.abort();
            });
        },
    );
}

/// The B shape is a protocol state for optional lanes: whatever the ledger says, it never
/// claims `exact` or `current`. Pinned over a non-empty ledger (one issued, settled ticket) so
/// a B built from invented zeros would not pass either.
#[test]
fn b_shape_is_never_exact_or_current() {
    run_db_fixture::<Fixture, _>("b_shape_is_never_exact_or_current", |mut handle| {
        handle.seed_current_entitlement_and_window(100);
        seed_placement(&mut handle, "read_decoupling_never_dialled");
        let pair = seed_pair(&mut handle, "b", Family::Unserved, true);
        let commit_seq: i64 = handle
            .admin
            .query_one("SELECT nextval('ops.commit_seq_seq')", &[])
            .unwrap()
            .get(0);
        handle
            .admin
            .execute(
                "INSERT INTO projection.stream_log \
                   (tenant_id,scope_kind,scope_id,domain,projection_kind,projection_version, \
                    stream_seq,commit_seq,state,settled_at) \
                 VALUES($1,'workspace',$2,'knowledge','ingest','v1',1,$3,'DONE',clock_timestamp())",
                &[&handle.tenant_id, &pair.workspace_id, &commit_seq],
            )
            .unwrap();
        handle
            .admin
            .execute(
                "UPDATE projection.stream_checkpoints SET issued_highwater=1, projection_highwater=1 \
                 WHERE tenant_id=$1 AND scope_id=$2",
                &[&handle.tenant_id, &pair.workspace_id],
            )
            .unwrap();
        let gw = gateway(&handle);
        handle.rt.block_on(async {
            let (address, server) = start(gw.app).await;
            let recall = ok_content(
                &tool_call(address, "recall", &pair.bearer, recall_args(&pair)).await,
                ToolName::Recall,
            );
            let context = ok_content(
                &tool_call(address, "context", &pair.bearer, context_args(&pair)).await,
                ToolName::Context,
            )["content"]
                .clone();
            for (label, content) in [("recall", recall), ("context", context)] {
                assert_b_shape(&content, label);
                assert_eq!(
                    content["pipeline"]["projection"]["expected"], 1,
                    "real ledger: {content}"
                );
                assert_eq!(
                    content["pipeline"]["projection"]["done"], 1,
                    "real ledger: {content}"
                );
                assert_ne!(content["completeness"]["class"], "exact");
                assert_ne!(content["pipeline"]["projection"]["current"], true);
            }
            server.abort();
        });
    });
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one READY workspace across the four read routes"
)]
fn empty_ready_workspace_answers_not_found_exact_current_and_empty_context() {
    run_db_fixture::<Fixture, _>(
        "empty_ready_workspace_answers_not_found_exact_current_and_empty_context",
        |mut handle| {
            handle.seed_current_entitlement_and_window(100);
            let collection = format!("read_decoupling_{}", Uuid::now_v7().simple());
            seed_placement(&mut handle, &collection);
            let pair = seed_pair(&mut handle, "y", Family::Serving, false);
            let gw = gateway(&handle);
            let (transport, registry, port) =
                (gw.transport.clone(), gw.registry.clone(), gw.port.clone());
            handle.rt.block_on(async {
                qdrant_collection(&transport, &registry, &collection).await;
                let _cleanup = CollectionCleanup(collection.clone());
                let (address, server) = start(gw.app).await;
                let get = tool_call(
                    address,
                    "memory",
                    &pair.bearer,
                    get_args(&pair, Uuid::now_v7()),
                )
                .await;
                assert_eq!(tool_error_code(&get), "NOT_FOUND", "{get}");
                let enumerate = ok_content(
                    &tool_call(address, "memory", &pair.bearer, enumerate_args(&pair)).await,
                    ToolName::Memory,
                );
                assert_eq!(
                    enumerate["content"]["completeness"]["class"], "exact",
                    "{enumerate}"
                );
                assert_eq!(
                    enumerate["content"]["completeness"]["exact"]["total"], 0,
                    "{enumerate}"
                );
                let recall = ok_content(
                    &tool_call(address, "recall", &pair.bearer, recall_args(&pair)).await,
                    ToolName::Recall,
                );
                assert_eq!(recall["items"], json!([]), "{recall}");
                assert_eq!(
                    recall["pipeline"]["projection"]["current"], true,
                    "{recall}"
                );
                assert_eq!(recall["pipeline"]["projection"]["visible"], 0, "{recall}");
                assert_eq!(
                    port.calls.load(Ordering::SeqCst),
                    1,
                    "a READY family embeds"
                );
                let context = ok_content(
                    &tool_call(address, "context", &pair.bearer, context_args(&pair)).await,
                    ToolName::Context,
                );
                assert_eq!(context["content"]["items"], json!([]), "{context}");
                assert_ne!(
                    context["content"]["completeness"]["reason"],
                    "no_serving_projection"
                );
                server.abort();
            });
        },
    );
}

/// Deletes this test's own collection on drop (also on a failed assertion), over a plain
/// loopback HTTP/1.0 request so it needs no runtime.
struct CollectionCleanup(String);

impl Drop for CollectionCleanup {
    fn drop(&mut self) {
        use std::io::Write as _;
        let port: u16 = std::env::var("HUMAUX_TEST_QDRANT_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(6333);
        // dep: Qdrant(*) — delete this test's own collection
        if let Ok(mut stream) = std::net::TcpStream::connect(("127.0.0.1", port)) {
            let _ = write!(
                stream,
                "DELETE /collections/{} HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n",
                self.0
            );
            let _ = std::io::Read::read_to_end(&mut stream, &mut Vec::new());
        }
    }
}

/// Creates this test's own collection (dimension 4, both payload indexes).
async fn qdrant_collection(
    transport: &HttpIntraCellTransport,
    registry: &IntraCellResourceRegistry,
    collection: &str,
) {
    let permit = authorize_cell_access(
        registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(60),
    )
    .expect("Qdrant permit");
    let requests = vec![
        (
            IntraCellMethod::Put,
            format!("/collections/{collection}"),
            Some(create_collection_body(
                u64::from(DIMENSION),
                Distance::Cosine,
                1,
                1,
                1,
                ShardingMethod::Auto,
            )),
        ),
        (
            IntraCellMethod::Put,
            format!("/collections/{collection}/index?wait=true"),
            Some(tenant_index_body()),
        ),
        (
            IntraCellMethod::Put,
            format!("/collections/{collection}/index?wait=true"),
            Some(subject_index_body()),
        ),
    ];
    for (method, path, json_body) in requests {
        let response = transport
            .execute(
                &permit,
                // dep: Qdrant(*) — this test's own collection
                IntraCellRequest {
                    method,
                    path,
                    json_body,
                    headers: Vec::new(),
                },
            )
            .await
            .expect("Qdrant request");
        assert!(
            (200..300).contains(&response.status),
            "Qdrant returned {}",
            response.status
        );
    }
}

/// ADR-0053 D-B: `remember.put` on a PROVISIONING workspace is a definite, rolled-back refusal —
/// the §52 `CONFLICT` (never `DEPENDENCY_UNAVAILABLE`, which the guard reads as outcome unknown),
/// and no Evidence/outbox/stream row commits.
#[test]
fn remember_put_on_provisioning_workspace_is_tool_error_conflict() {
    run_db_fixture::<Fixture, _>(
        "remember_put_on_provisioning_workspace_is_tool_error_conflict",
        |mut handle| {
            handle.seed_current_entitlement_and_window(100);
            let pair = seed_pair(&mut handle, "w", Family::Unserved, false);
            handle
                .admin
                .execute(
                    "UPDATE control.workspaces SET lifecycle='PROVISIONING' WHERE workspace_id=$1",
                    &[&pair.workspace_id],
                )
                .unwrap();
            let counts = |handle: &mut Handle| -> (i64, i64, i64) {
                let row = handle
                .admin
                .query_one(
                    "SELECT (SELECT count(*) FROM private.evidence_objects WHERE tenant_id=$1), \
                            (SELECT count(*) FROM ops.outbox WHERE tenant_id=$1), \
                            (SELECT count(*) FROM projection.stream_log WHERE tenant_id=$1)",
                    &[&handle.tenant_id],
                )
                .unwrap();
                (row.get(0), row.get(1), row.get(2))
            };
            let before = counts(&mut handle);
            let gw = gateway(&handle);
            let response = handle.rt.block_on(async {
                let (address, server) = start(gw.app).await;
                let response = tool_call(
                    address,
                    "remember",
                    &pair.bearer,
                    json!({"operation":"put","content":"written while provisioning",
                       "idempotency_key": format!("c28-{}", Uuid::now_v7()),
                       "workspace_id": pair.workspace_id}),
                )
                .await;
                server.abort();
                response
            });
            assert_eq!(tool_error_code(&response), "CONFLICT", "{response}");
            assert_eq!(counts(&mut handle), before, "nothing commits");
            // …and after READY the same write lands (the gate is the lifecycle, nothing else).
            handle
                .admin
                .execute(
                    "UPDATE control.workspaces SET lifecycle='READY' WHERE workspace_id=$1",
                    &[&pair.workspace_id],
                )
                .unwrap();
            let gw = gateway(&handle);
            let response = handle.rt.block_on(async {
                let (address, server) = start(gw.app).await;
                let response = tool_call(
                    address,
                    "remember",
                    &pair.bearer,
                    json!({"operation":"put","content":"written when ready",
                       "idempotency_key": format!("c28-{}", Uuid::now_v7()),
                       "workspace_id": pair.workspace_id}),
                )
                .await;
                server.abort();
                response
            });
            assert_ne!(response["result"]["isError"], true, "{response}");
            assert_eq!(counts(&mut handle).2, before.2 + 1);
        },
    );
}
