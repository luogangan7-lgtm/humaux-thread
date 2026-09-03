//! `humaux-retrieval-worker` 进程入口（最小必要进程集见 §4.2；admin 探针契约见 §4.4）。
//!
//! §4.2 (line 818): the owning process of `humaux_adapters::projection_worker::run_once` —
//! there is no separate `projection-worker` process. Env wiring mirrors
//! `bins/public-worker/src/main.rs`'s pattern (`required`/`parse`, one Qdrant
//! `IntraCellResource` entry, `--run-once` flag), extended with the Postgres/Qdrant/embedder
//! config `run_once` needs.

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
    CallerId, CellAccessPermit, CellId, HttpIntraCellTransport, IntraCellResource,
    IntraCellResourceRegistry, ResourceEntry, authorize_cell_access,
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
    "usage: humaux-retrieval-worker (--run-once | --serve-rpc)"
}

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
        "--run-once" => run_once_mode().await,
        "--serve-rpc" => rpc_mode::run().await,
        _ => Err(Outcome::Failed(usage().to_owned())),
    }
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
    let domain = required("HUMAUX_RETRIEVAL_WORKER_DOMAIN")?;
    let projection_kind = required("HUMAUX_RETRIEVAL_WORKER_PROJECTION_KIND")?;
    let projection_version = required("HUMAUX_RETRIEVAL_WORKER_PROJECTION_VERSION")?;
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
    let pool = RetrievalWorkerDbPool::connect(&dsn)
        .await
        .map_err(|_| "retrieval worker database role connection failed".to_owned())?;

    // ponytail: `TenantPlacementRow` is not yet read from `projection.tenant_placements` (the
    // write path is blocked on the same missing HTTP client `qdrant.rs`'s module doc already
    // names) — a single shared-fallback placement at the configured collection until that
    // lands (tracked in coord task 7e6da2f9).
    let placement = TenantPlacementRow {
        tenant_id,
        projection_family: RetrievalFamily::PrivateMemoryV1,
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

/// ponytail: the Processor Registry (§7) and a per-memory `DisclosureSource` attribution both
/// need the model catalog the "ponytail: descriptor from env" note above already names as
/// missing — a fixed nil processor id and a fixed batch-level `DisclosureSource` stand in
/// until that registry exists (tracked in coord task 7e6da2f9).
/// ponytail: the Processor Registry (§7) and a per-memory `DisclosureSource` attribution both
/// need the model catalog the "ponytail: descriptor from env" note above already names as
/// missing — a fixed nil processor id and a fixed batch-level `DisclosureSource` stand in
/// until that registry exists (tracked in coord task 7e6da2f9). Shared by both `--run-once`
/// (wrapped as `CardEmbedder`) and `--serve-rpc` (used directly as `EmbeddingProvider`).
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
    let embedder_pool = RetrievalWorkerDbPool::connect(dsn).await.map_err(|_| {
        "retrieval worker database role connection failed (embedder pool)".to_owned()
    })?;
    // §78.1: the provider is configuration, not code — the id selects the adapter inside
    // `humaux_retrieval_provider::adapters` (§19 Gate 3/7 keeps the concrete type there).
    let provider = embedding_provider_for(
        &required("HUMAUX_RETRIEVAL_WORKER_EMBEDDING_PROVIDER")?,
        embedder_pool,
        ProcessorId(Uuid::nil()),
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
        let listener = tokio::net::UnixListener::bind(&socket_path).map_err(|error| {
            format!("failed to bind retrieval embedding RPC socket {socket_path}: {error}")
        })?;
        eprintln!("humaux-retrieval-worker RPC listening on {socket_path}");
        axum::serve(
            listener,
            router(state)
                .into_make_service_with_connect_info::<humaux_retrieval_worker::rpc::PeerIdentity>(
                ),
        )
        .await
        .map_err(|error| format!("retrieval embedding RPC server failed: {error}"))?;
        Ok(())
    }
}
