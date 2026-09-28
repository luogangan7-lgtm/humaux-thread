//! `retrieval-worker::main` — `humaux-retrieval-worker` 进程入口（最小必要进程集见 §4.2；admin 探针契约见 §4.4）。
//! Depends-on: crates=[async-trait, axum, humaux-adapters, humaux-domain, humaux-infra-cell,
//!   humaux-local-secret-scan, humaux-projection, humaux-retrieval-provider, tokio, uuid];
//!   services=[PostgreSQL(role_retrieval_worker), Qdrant(*), UDS(serve)]; env=[HUMAUX_RETRIEVAL_WORKER_BATCH,
//!   HUMAUX_RETRIEVAL_WORKER_CALLER, HUMAUX_RETRIEVAL_WORKER_CELL_ID, HUMAUX_RETRIEVAL_WORKER_DIMENSION,
//!   HUMAUX_RETRIEVAL_WORKER_EGRESS_PROCESSOR_ID, HUMAUX_RETRIEVAL_WORKER_EMBEDDING_MODEL,
//!   HUMAUX_RETRIEVAL_WORKER_EMBEDDING_PROVIDER, HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION,
//!   HUMAUX_RETRIEVAL_WORKER_GATEWAY_UID, HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN,
//!   HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256, HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION,
//!   HUMAUX_RETRIEVAL_WORKER_MAX_INPUT_TOKENS, HUMAUX_RETRIEVAL_WORKER_MODEL_REVISION,
//!   HUMAUX_RETRIEVAL_WORKER_PG_DSN, HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR, HUMAUX_RETRIEVAL_WORKER_QDRANT_COLLECTION,
//!   HUMAUX_RETRIEVAL_WORKER_QDRANT_HOST, HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT, HUMAUX_RETRIEVAL_WORKER_QDRANT_TLS,
//!   HUMAUX_RETRIEVAL_WORKER_REGION, HUMAUX_RETRIEVAL_WORKER_RPC_SOCKET_PATH, HUMAUX_RETRIEVAL_WORKER_SCOPE_ID,
//!   HUMAUX_RETRIEVAL_WORKER_SCOPE_KIND, HUMAUX_RETRIEVAL_WORKER_TENANT_ID]; modules=[adapters::disclosure,
//!   adapters::postgres, adapters::projection_worker, adapters::qdrant, domain::egress, domain::error, domain::ids,
//!   humaux-local-secret-scan, infra-cell::permit, infra-cell::resource, infra-cell::transport, projection::serving,
//!   retrieval-provider::adapters, retrieval-provider::contract, retrieval-worker::rpc]
//! Called-by: [process(humaux-retrieval-worker)]
//! Invariants: [the UDS server binds only the configured socket path; a peer without kernel peer-credential auth is refused before any request is read]
//! Spec: Baseline §4.2; §4.4; §17.3; ADR-0012; ADR-0037
//!
//! §4.2 (line 818): the owning process of `humaux_adapters::projection_worker::run_once` —
//! there is no separate `projection-worker` process. Env wiring mirrors
//! `bins/public-worker/src/main.rs`'s pattern (`required`/`parse`, one Qdrant
//! `IntraCellResource` entry, `--run-once` flag), extended with the Postgres/Qdrant/embedder
//! config `run_once` needs.
//!
//! Card 15 / ADR-0037 adds `--readyz` (one live round trip to each dependency this process
//! cannot work without — see [`readyz`]) and graceful SIGTERM/Ctrl-C shutdown for `--serve-rpc`
//! (axum's `with_graceful_shutdown`: the accept loop stops, in-flight embedding calls finish).

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    process::ExitCode,
    time::Duration,
};

use async_trait::async_trait;
use humaux_adapters::disclosure::DisclosureSource;
use humaux_adapters::postgres::RetrievalWorkerDbPool;
use humaux_adapters::projection_worker::{CardEmbedder, ProjectionWorkerDeps, run_once};
use humaux_adapters::qdrant::{
    PlacementClass, PromotionState, RetrievalFamily, TenantPlacementRow,
};
use humaux_domain::egress::ProcessorId;
use humaux_domain::error::ErrorCode;
use humaux_domain::ids::TenantId;
use humaux_infra_cell::{
    CallerId, CellAccessPermit, CellId, HttpIntraCellTransport, IntraCellHttpTransport,
    IntraCellMethod, IntraCellRequest, IntraCellResource, IntraCellResourceRegistry, ResourceEntry,
    authorize_cell_access,
};
use humaux_local_secret_scan::{LocalSecretScanner, LocalSecretScannerConfig, SealedRetrievalCard};
use humaux_projection::serving::StreamFamily;
use humaux_retrieval_provider::adapters::embedding_provider_for;
use humaux_retrieval_provider::contract::{EmbeddingModelDescriptor, EmbeddingProvider, ModelId};
use std::sync::Arc;
use uuid::Uuid;

fn required(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("missing required configuration: {name}"))
}

fn parse<T: std::str::FromStr>(name: &str) -> Result<T, String> {
    required(name)?
        .parse()
        .map_err(|_| format!("invalid configuration: {name}"))
}

fn usage() -> &'static str {
    "usage: humaux-retrieval-worker (--readyz | --run-once | --serve-rpc)"
}

/// The §17 retrieval family this process projects into. Single point of truth for BOTH the
/// §17.3 placement below and — through [`RetrievalFamily::ticket_family`] — the §15.1 ticket
/// triple `run_once` polls for, so the two can never name different families (card 21).
const PROJECTION_FAMILY: RetrievalFamily = RetrievalFamily::PrivateMemoryV1;

/// Local wrapper making a real [`EmbeddingProvider`] satisfy [`CardEmbedder`] — see that
/// trait's doc in `crates/adapters/src/projection_worker.rs` for why the orphan rule forces
/// this indirection here rather than a blanket impl on the provider type in
/// `humaux-adapters` (neither trait nor type is local to that crate; both are local, or the
/// wrapper is, only here — the one crate that depends on both with no cycle).
struct EmbedderAdapter(Arc<dyn EmbeddingProvider>);

#[async_trait]
impl CardEmbedder for EmbedderAdapter {
    async fn embed_cards(
        &self,
        tenant_id: TenantId,
        dimension: u32,
        cards: &[SealedRetrievalCard],
        memory_ids: &[Uuid],
    ) -> Result<Vec<Vec<f32>>, ErrorCode> {
        Ok(self
            .0
            .embed_cards_for_memories(tenant_id, dimension, cards, memory_ids)
            .await?
            .vectors)
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(Outcome::NotApplicable(reason)) => {
            println!("humaux-retrieval-worker: not_applicable: {reason}");
            ExitCode::from(2)
        }
        Err(Outcome::Failed(error)) => {
            eprintln!("humaux-retrieval-worker: {error}");
            ExitCode::from(2)
        }
    }
}

enum Outcome {
    NotApplicable(String),
    Failed(String),
}

impl From<String> for Outcome {
    fn from(value: String) -> Self {
        Outcome::Failed(value)
    }
}

async fn run() -> Result<(), Outcome> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 1 {
        return Err(Outcome::Failed(usage().to_owned()));
    }
    match args[0].as_str() {
        "--readyz" => readyz().await,
        "--run-once" => run_once_mode().await,
        "--serve-rpc" => rpc_mode::run().await,
        _ => Err(Outcome::Failed(usage().to_owned())),
    }
}

/// `--readyz`: one live round trip per dependency this process cannot work without, each named
/// when it is down (§4.4 坑5 applied to readiness — "unreachable" is never reported as a
/// healthy-but-empty reading). Uses no configuration key `--run-once`/`--serve-rpc` do not
/// already need.
///
/// 1. `role_retrieval_worker` connects AND `current_user` matches (§6.2.3 assertion E).
/// 2. The Qdrant §83.4 Layer 1B cell resource answers a real HTTP call, made through the SAME
///    registry/permit/transport the projection path uses — not a bare TCP dial, so a
///    misconfigured CIDR or caller allowlist shows up here rather than at the first write.
async fn readyz() -> Result<(), Outcome> {
    let dsn = required("HUMAUX_RETRIEVAL_WORKER_PG_DSN")?;
    // dep: PostgreSQL(role_retrieval_worker) — readyz: one live connect as role_retrieval_worker
    RetrievalWorkerDbPool::connect(&dsn).await.map_err(|e| {
        Outcome::Failed(format!(
            "not ready — missing object: PostgreSQL as role_retrieval_worker ({e})"
        ))
    })?;
    let (permit, transport) = build_cell_access().await?;
    let status = transport
        .execute(
            &permit,
            // dep: Qdrant(*) — opens the retrieval-worker's Qdrant client
            IntraCellRequest {
                method: IntraCellMethod::Get,
                path: "/".to_owned(),
                json_body: None,
                headers: Vec::new(),
            },
        )
        .await
        .map_err(|e| {
            Outcome::Failed(format!(
                "not ready — missing object: the Qdrant cell resource ({e:?})"
            ))
        })?
        .status;
    if status >= 500 {
        return Err(Outcome::Failed(format!(
            "not ready — missing object: the Qdrant cell resource answered {status}"
        )));
    }
    println!("humaux-retrieval-worker: ready db=role_retrieval_worker qdrant_status={status}");
    Ok(())
}

async fn run_once_mode() -> Result<(), Outcome> {
    // Gate: the embedding-model descriptor is config-driven (§78.1 bans a hardcoded model/
    // dim/endpoint), and the catalog that would otherwise supply it does not exist yet
    // (`crates/retrieval-provider/src/contract.rs`'s own T7.1 scope note).
    // ponytail: descriptor from env until the embedding-model catalog lands (spec silent;
    // tracked in coord task 7e6da2f9).
    for var in [
        "HUMAUX_RETRIEVAL_WORKER_EMBEDDING_MODEL",
        "HUMAUX_RETRIEVAL_WORKER_MODEL_REVISION",
        "HUMAUX_RETRIEVAL_WORKER_DIMENSION",
    ] {
        if env::var(var).is_err() {
            return Err(Outcome::NotApplicable(format!("missing {var}")));
        }
    }

    let tenant_id = TenantId(parse::<Uuid>("HUMAUX_RETRIEVAL_WORKER_TENANT_ID")?);
    let scope_kind = required("HUMAUX_RETRIEVAL_WORKER_SCOPE_KIND")?;
    let scope_id = parse::<Uuid>("HUMAUX_RETRIEVAL_WORKER_SCOPE_ID")?;
    // §78.1 / card 21: the ticket family triple is DERIVED from the retrieval family this
    // process already projects into (`RetrievalFamily::PrivateMemoryV1`, five lines below in
    // `placement`), not read from three env values an operator had to keep equal to the
    // consolidation worker's three literals. `ticket_family()` is `None` only for the §17
    // families that are not `stream_log` producers — this process projects the private-memory
    // one, so `None` is a wiring bug, not a deployment shape.
    let ticket_family = PROJECTION_FAMILY
        .ticket_family()
        .ok_or_else(|| format!("{PROJECTION_FAMILY:?} is not a §15.1 ticket-stream family"))?;
    let domain = ticket_family.domain();
    let projection_kind = ticket_family.projection_kind();
    let projection_version = ticket_family.projection_version().to_owned();
    let embedding_version = required("HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION")?;
    let dimension = parse::<u32>("HUMAUX_RETRIEVAL_WORKER_DIMENSION")?;
    if dimension == 0 {
        return Err(Outcome::Failed(
            "invalid configuration: HUMAUX_RETRIEVAL_WORKER_DIMENSION".to_owned(),
        ));
    }
    let batch = parse::<i64>("HUMAUX_RETRIEVAL_WORKER_BATCH")?;
    if batch <= 0 {
        return Err(Outcome::Failed(
            "invalid configuration: HUMAUX_RETRIEVAL_WORKER_BATCH".to_owned(),
        ));
    }

    let (permit, transport) = build_cell_access().await?;

    let dsn = required("HUMAUX_RETRIEVAL_WORKER_PG_DSN")?;
    // dep: PostgreSQL(role_retrieval_worker) — reconnects the pool after a lease-loop error
    let pool = RetrievalWorkerDbPool::connect(&dsn)
        .await
        .map_err(|_| "retrieval worker database role connection failed".to_owned())?;

    // ponytail: `TenantPlacementRow` is not yet read from `projection.tenant_placements` (the
    // write path is blocked on the same missing HTTP client `qdrant.rs`'s module doc already
    // names) — a single shared-fallback placement at the configured collection until that
    // lands (tracked in coord task 7e6da2f9).
    let placement = TenantPlacementRow {
        tenant_id,
        projection_family: PROJECTION_FAMILY,
        collection_name: required("HUMAUX_RETRIEVAL_WORKER_QDRANT_COLLECTION")?,
        shard_key: None,
        placement_class: PlacementClass::SharedFallback,
        point_count: 0,
        bytes_estimate: 0,
        promotion_state: PromotionState::Stable,
    };

    let scanner = build_scanner()?;
    let embedder = build_embedder(&dsn, dimension).await?;

    let deps = ProjectionWorkerDeps {
        pool,
        embedder,
        scanner: std::sync::Arc::new(scanner),
        transport,
        permit,
        placement,
        family: StreamFamily::new(tenant_id, scope_kind, scope_id, domain, projection_kind),
        embedding_version,
        projection_version,
        dimension,
        // Card 21 fix pass: the SAME §7.4 identity the embedding provider discloses under —
        // read once, here, and handed to both legs. `advance_prefix` writes it into
        // `projection.stream_checkpoints.projection_processor_id` (migration 0171), so this
        // worker's checkpoint names this worker. A second env var for "the projection
        // identity" would be a fourth hand-aligned copy of the thing this card deleted.
        processor_id: egress_processor_id()?,
    };

    run_once(&deps, batch as usize)
        .await
        .map_err(|error| format!("run_once failed: {error}"))?;
    Ok(())
}

/// Wires the Qdrant `IntraCellResource` entry, its permit, and the HTTP transport —
/// mirrors `bins/public-worker/src/main.rs`'s identical block.
async fn build_cell_access()
-> Result<(CellAccessPermit, std::sync::Arc<HttpIntraCellTransport>), Outcome> {
    let cell_id = CellId(parse::<Uuid>("HUMAUX_RETRIEVAL_WORKER_CELL_ID")?);
    let caller = CallerId(required("HUMAUX_RETRIEVAL_WORKER_CALLER")?);
    let host = required("HUMAUX_RETRIEVAL_WORKER_QDRANT_HOST")?;
    let port = parse::<u16>("HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT")?;
    if port == 0 {
        return Err(Outcome::Failed(
            "invalid configuration: HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT".to_owned(),
        ));
    }
    let cidr = required("HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR")?
        .parse()
        .map_err(|_| "invalid configuration: HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR".to_owned())?;
    let tls = parse::<bool>("HUMAUX_RETRIEVAL_WORKER_QDRANT_TLS")?;

    let mut entries = BTreeMap::new();
    entries.insert(
        IntraCellResource::QDRANT_REST,
        ResourceEntry::new(
            host,
            port,
            cell_id,
            vec![cidr],
            BTreeSet::from([caller.clone()]),
            tls,
        )
        .map_err(|_| "invalid Qdrant cell resource configuration".to_owned())?,
    );
    let registry = IntraCellResourceRegistry::new(entries, cell_id, caller);
    let permit = authorize_cell_access(
        &registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(60),
    )
    .map_err(|_| "retrieval worker is not authorized for the Qdrant cell".to_owned())?;
    let transport = HttpIntraCellTransport::new(
        registry.clone(),
        Duration::from_secs(10),
        humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES,
    )
    .map_err(|_| "could not construct Qdrant cell transport".to_owned())?;
    Ok((permit, std::sync::Arc::new(transport)))
}

fn build_scanner() -> Result<LocalSecretScanner, Outcome> {
    LocalSecretScanner::new(LocalSecretScannerConfig {
        executable: required("HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN")?.into(),
        expected_version: required("HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION")?,
        expected_executable_sha256: required("HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256")?,
        timeout: Duration::from_secs(5),
        max_payload_bytes: 64 * 1024,
        finding_exit_code: 1,
    })
    .map_err(|_| {
        "invalid local secret scanner configuration"
            .to_owned()
            .into()
    })
}

/// This process's §7 egress identity, read from configuration exactly the way the private
/// worker reads its own (`HUMAUX_PRIVATE_WORKER_EGRESS_PROCESSOR_ID`) — deployment identity,
/// never tenant data.
///
/// Card 21: before this, the value was `ProcessorId(Uuid::nil())`, so **every**
/// `ops.data_disclosures` row this worker wrote carried `processor_id` all-zeros. §7.4's
/// ledger exists to answer "which processor received this private data"; a column that is the
/// same constant for every row answers nothing, and §7.3's deletion/revocation propagation
/// (which queries by processor) had no row it could find. `Uuid::nil()` is refused explicitly
/// rather than accepted as "unset": a nil id is exactly the unattributed state this fixes, and
/// §78.1 gives it no default.
fn egress_processor_id() -> Result<ProcessorId, String> {
    let id = parse::<Uuid>("HUMAUX_RETRIEVAL_WORKER_EGRESS_PROCESSOR_ID")?;
    if id.is_nil() {
        return Err(
            "invalid configuration: HUMAUX_RETRIEVAL_WORKER_EGRESS_PROCESSOR_ID must not be \
             the nil UUID (§7.4: a disclosure row must name a real processor)"
                .to_owned(),
        );
    }
    Ok(ProcessorId(id))
}

/// ponytail: a per-memory `DisclosureSource` attribution needs the model catalog the "ponytail:
/// descriptor from env" note above already names as missing — a fixed batch-level
/// `DisclosureSource` stands in until that registry exists (tracked in coord task 7e6da2f9).
/// It is only ever reached by `embed_cards` (no memory ids); the projection path this binary
/// actually drives calls `embed_cards_for_memories`, which builds one real
/// `DisclosureSource::Memory` per card. Shared by both `--run-once` (wrapped as `CardEmbedder`)
/// and `--serve-rpc` (used directly as `EmbeddingProvider`).
async fn build_embedding_provider(
    dsn: &str,
    dimension: u32,
) -> Result<Arc<dyn EmbeddingProvider>, Outcome> {
    let model = EmbeddingModelDescriptor {
        model_id: ModelId(required("HUMAUX_RETRIEVAL_WORKER_EMBEDDING_MODEL")?),
        model_revision: required("HUMAUX_RETRIEVAL_WORKER_MODEL_REVISION")?,
        dimension_options: vec![dimension],
        max_input_tokens: parse::<u32>("HUMAUX_RETRIEVAL_WORKER_MAX_INPUT_TOKENS")?,
        batch_supported: true,
        dense_supported: true,
        sparse_supported: false,
    };
    // dep: PostgreSQL(role_retrieval_worker) — opens a pool for the RPC server's request handling
    let embedder_pool = RetrievalWorkerDbPool::connect(dsn).await.map_err(|_| {
        "retrieval worker database role connection failed (embedder pool)".to_owned()
    })?;
    // §78.1: the provider is configuration, not code — the id selects the adapter inside
    // `humaux_retrieval_provider::adapters` (§19 Gate 3/7 keeps the concrete type there).
    let provider = embedding_provider_for(
        &required("HUMAUX_RETRIEVAL_WORKER_EMBEDDING_PROVIDER")?,
        embedder_pool,
        egress_processor_id()?,
        model,
        required("HUMAUX_RETRIEVAL_WORKER_REGION")?,
        DisclosureSource::Memory(Uuid::nil()),
    )
    .map_err(|_| "could not construct the configured embedding provider".to_owned())?;
    Ok(provider)
}

async fn build_embedder(
    dsn: &str,
    dimension: u32,
) -> Result<std::sync::Arc<dyn CardEmbedder>, Outcome> {
    let provider = build_embedding_provider(dsn, dimension).await?;
    Ok(std::sync::Arc::new(EmbedderAdapter(provider)))
}

/// ADR-0012 `--serve-rpc` entry point: the Unix-domain-socket query-embedding RPC listener,
/// alongside (never instead of) the existing `--run-once` projection loop above.
mod rpc_mode {
    use std::sync::Arc;

    use humaux_adapters::postgres::RetrievalWorkerDbPool;

    use super::{Outcome, build_embedding_provider, build_scanner, parse, required};
    use humaux_retrieval_worker::rpc::{RpcState, router};

    pub async fn run() -> Result<(), Outcome> {
        for var in [
            "HUMAUX_RETRIEVAL_WORKER_EMBEDDING_MODEL",
            "HUMAUX_RETRIEVAL_WORKER_MODEL_REVISION",
            "HUMAUX_RETRIEVAL_WORKER_DIMENSION",
        ] {
            if std::env::var(var).is_err() {
                return Err(Outcome::NotApplicable(format!("missing {var}")));
            }
        }
        let dimension = parse::<u32>("HUMAUX_RETRIEVAL_WORKER_DIMENSION")?;
        if dimension == 0 {
            return Err(Outcome::Failed(
                "invalid configuration: HUMAUX_RETRIEVAL_WORKER_DIMENSION".to_owned(),
            ));
        }
        let gateway_uid = parse::<u32>("HUMAUX_RETRIEVAL_WORKER_GATEWAY_UID")?;
        let socket_path = required("HUMAUX_RETRIEVAL_WORKER_RPC_SOCKET_PATH")?;
        let dsn = required("HUMAUX_RETRIEVAL_WORKER_PG_DSN")?;

        let embedder: Arc<dyn humaux_retrieval_provider::contract::EmbeddingProvider> =
            build_embedding_provider(&dsn, dimension).await?;
        let scanner = Arc::new(build_scanner()?);
        // dep: PostgreSQL(role_retrieval_worker) — opens a pool for the readiness check
        let calls = RetrievalWorkerDbPool::connect(&dsn).await.map_err(|_| {
            "retrieval worker database role connection failed (rpc pool)".to_owned()
        })?;

        let state = Arc::new(RpcState {
            expected_gateway_uid: gateway_uid,
            calls,
            scanner,
            embedder,
            dimension,
            // §78.1: same configured provider id the embedding provider was built from.
            provider_id: required("HUMAUX_RETRIEVAL_WORKER_EMBEDDING_PROVIDER")?,
        });

        let _ = std::fs::remove_file(&socket_path);
        // dep: UDS(serve) — binds the ADR-0012 embedding RPC socket
        let listener = tokio::net::UnixListener::bind(&socket_path).map_err(|error| {
            format!("failed to bind retrieval embedding RPC socket {socket_path}: {error}")
        })?;
        eprintln!("humaux-retrieval-worker RPC listening on {socket_path}");
        // Card 15 / ADR-0037: same graceful pattern the gateway has had. This listener holds no
        // lease, so "drain" here means exactly what axum's shutdown does — stop accepting, let
        // in-flight embedding calls finish, then return zero.
        // Both handlers are registered HERE, before `axum::serve` is even constructed:
        // `tokio::signal::ctrl_c()` would only register SIGINT on the shutdown future's first
        // poll (inside `serve`), so a Ctrl-C arriving between `bind` and that first poll would
        // hit SIGINT's default disposition and kill the listener without draining.
        #[cfg(unix)]
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|e| format!("cannot install the SIGTERM handler: {e}"))?;
        #[cfg(unix)]
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .map_err(|e| format!("cannot install the SIGINT handler: {e}"))?;
        let shutdown = async move {
            #[cfg(unix)]
            tokio::select! {
                _ = interrupt.recv() => {}
                _ = terminate.recv() => {}
            }
            #[cfg(not(unix))]
            let _ = tokio::signal::ctrl_c().await;
            eprintln!("humaux-retrieval-worker: signal received, RPC listener draining");
        };
        axum::serve(
            listener,
            router(state)
                .into_make_service_with_connect_info::<humaux_retrieval_worker::rpc::PeerIdentity>(
                ),
        )
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(|error| format!("retrieval embedding RPC server failed: {error}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod config_tests {
    use super::{PROJECTION_FAMILY, egress_processor_id};
    use humaux_domain::ticket_family::TicketFamily;
    use uuid::Uuid;

    /// Card 21, ProcessorId leg. Two deployments of this binary configured with two identities
    /// get two distinct [`ProcessorId`](humaux_domain::egress::ProcessorId)s, and neither is
    /// the nil placeholder every `ops.data_disclosures` row used to carry. The env var is
    /// process-global, so the two reads are sequenced rather than parallel — this test is
    /// `#[ignore]`-free but must stay in one thread's control of that variable, which is why it
    /// reads both values inside one test instead of two.
    ///
    /// Fault injection: restore `ProcessorId(Uuid::nil())` in `build_embedding_provider` and
    /// the rehearsal's `disclosure_processor_attributed` assertion goes red (this unit test
    /// pins the parser; the rehearsal pins the row).
    #[test]
    fn two_workers_get_distinct_non_nil_processor_ids() {
        const VAR: &str = "HUMAUX_RETRIEVAL_WORKER_EGRESS_PROCESSOR_ID";
        let a = Uuid::from_u128(0x2016);
        let b = Uuid::from_u128(0x2017);

        // SAFETY: single-threaded test body; the variable is restored/removed before returning.
        unsafe { std::env::set_var(VAR, a.to_string()) };
        let first = egress_processor_id().expect("worker A identity");
        unsafe { std::env::set_var(VAR, b.to_string()) };
        let second = egress_processor_id().expect("worker B identity");

        assert_eq!(first.0, a);
        assert_eq!(second.0, b);
        assert_ne!(first.0, second.0);
        assert!(!first.0.is_nil() && !second.0.is_nil());

        // The nil UUID is refused, not silently accepted as "unset".
        unsafe { std::env::set_var(VAR, Uuid::nil().to_string()) };
        let refused = egress_processor_id().expect_err("nil processor id must be refused");
        assert!(refused.contains("must not be the nil UUID"), "{refused}");
        assert!(refused.contains("§7.4"), "{refused}");

        unsafe { std::env::remove_var(VAR) };
        let missing = egress_processor_id().expect_err("absent processor id must be refused");
        assert!(missing.contains(VAR), "{missing}");
    }

    /// Card 21, ticket-family leg. The triple this process polls for is derived from the §17
    /// family it projects into — there is no `HUMAUX_RETRIEVAL_WORKER_DOMAIN` /
    /// `_PROJECTION_KIND` / `_PROJECTION_VERSION` to keep aligned with the consolidation
    /// worker's issuer any more. Fault injection: point `PROJECTION_FAMILY` at
    /// `PublicKnowledgeV1` and `--run-once` fails at startup instead of polling a stream
    /// nobody writes.
    #[test]
    fn ticket_triple_is_derived_from_the_projection_family() {
        let family = PROJECTION_FAMILY
            .ticket_family()
            .expect("the projected family is a §15.1 ticket stream");
        assert_eq!(family, TicketFamily::PrivateMemory);
        assert_eq!(family.domain(), "private_memory");
        assert_eq!(family.projection_kind(), "PRIVATE_MEMORY");
        assert_eq!(family.projection_version(), "v1");
        assert_eq!(
            PROJECTION_FAMILY.collection_name(),
            family.collection_name()
        );
    }
}
