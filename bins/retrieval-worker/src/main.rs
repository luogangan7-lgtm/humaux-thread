//! `retrieval-worker::main` — `humaux-retrieval-worker` 进程入口（最小必要进程集见 §4.2；admin 探针契约见 §4.4）。
//! Depends-on: crates=[async-trait, axum, humaux-adapters, humaux-domain, humaux-infra-cell,
//!   humaux-local-secret-scan, humaux-retrieval-provider, humaux-telemetry, tokio, uuid];
//!   services=[HTTP(loopback), PostgreSQL(role_retrieval_worker), Qdrant(*), UDS(serve)]; env=[CARGO_PKG_VERSION,
//!   HUMAUX_BUILD_GIT_SHA, HUMAUX_RETRIEVAL_WORKER_BACKOFF_BASE_SECS,
//!   HUMAUX_RETRIEVAL_WORKER_BACKOFF_MAX_SECS, HUMAUX_RETRIEVAL_WORKER_BATCH, HUMAUX_RETRIEVAL_WORKER_CALLER,
//!   HUMAUX_RETRIEVAL_WORKER_CELL_ID, HUMAUX_RETRIEVAL_WORKER_DIMENSION, HUMAUX_RETRIEVAL_WORKER_EGRESS_PROCESSOR_ID,
//!   HUMAUX_RETRIEVAL_WORKER_EMBEDDING_MODEL, HUMAUX_RETRIEVAL_WORKER_EMBEDDING_PROVIDER,
//!   HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION, HUMAUX_RETRIEVAL_WORKER_GATEWAY_UID,
//!   HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN, HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256,
//!   HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION, HUMAUX_RETRIEVAL_WORKER_LEASE_SECS,
//!   HUMAUX_RETRIEVAL_WORKER_MAX_ATTEMPTS, HUMAUX_RETRIEVAL_WORKER_MAX_INPUT_TOKENS,
//!   HUMAUX_RETRIEVAL_WORKER_MODEL_REVISION, HUMAUX_RETRIEVAL_WORKER_PER_TENANT_CAP, HUMAUX_RETRIEVAL_WORKER_PG_DSN,
//!   HUMAUX_RETRIEVAL_WORKER_POLL_INTERVAL_SECS, HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR,
//!   HUMAUX_RETRIEVAL_WORKER_QDRANT_HOST, HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT, HUMAUX_RETRIEVAL_WORKER_QDRANT_TLS,
//!   HUMAUX_RETRIEVAL_WORKER_REGION, HUMAUX_RETRIEVAL_WORKER_RPC_SOCKET_PATH,
//!   HUMAUX_RETRIEVAL_WORKER_SERVE_METRICS_ADDR, HUMAUX_RETRIEVAL_WORKER_SERVE_RPC_METRICS_ADDR];
//!   modules=[adapters::disclosure,
//!   adapters::postgres, adapters::projection_worker, adapters::qdrant, adapters::stream_repo, domain::egress,
//!   domain::error, domain::ids, humaux-local-secret-scan, infra-cell::permit, infra-cell::resource,
//!   infra-cell::transport, retrieval-provider::adapters, retrieval-provider::contract, retrieval-provider::metrics,
//!   retrieval-worker::rpc, telemetry::metrics]
//! Called-by: [process(humaux-retrieval-worker)]
//! Invariants: [the UDS server binds only the configured socket path; a peer without kernel peer-credential auth is
//!   refused before any request is read; --serve / --run-once read no tenant, workspace or collection from the
//!   environment (ADR-0052: the claim supplies ticket and placement); a missing/zero pass key exits non-zero before
//!   any claim; --serve observes SIGTERM/SIGINT only between passes and a DB outage is a logged failed pass, not an
//!   exit; each resident mode reads only its own *_METRICS_ADDR key (required, loopback) and --readyz / --run-once
//!   open no listener; --metrics-families reads no configuration]
//! Spec: Baseline §4.2; §4.4; §17.3; §41.2; ADR-0012; ADR-0037; ADR-0052; ADR-0061 D-B; ADR-0061 D-C
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
//!
//! Card 27 / ADR-0052 adds `--serve`: the resident, tenant-free projection runner — one
//! `projection_worker::run_claimed_pass` every `HUMAUX_RETRIEVAL_WORKER_POLL_INTERVAL_SECS`, the
//! consolidation worker's `--serve` shape ([`Shutdown`], the signal observed between passes only,
//! so every claimed ticket is settled, retried or released before exit). `--run-once` is one such
//! pass (nothing claimable = exit 0). Neither reads a tenant, workspace or collection from the
//! environment: the claim hands each ticket its placement.
//!
//! Card 34 / ADR-0061 D-B: each resident mode serves `/metrics` and `/status` on its own loopback ops listener,
//! `HUMAUX_RETRIEVAL_WORKER_SERVE_RPC_METRICS_ADDR` / `HUMAUX_RETRIEVAL_WORKER_SERVE_METRICS_ADDR` — one key per
//! mode, because both modes run at once from one environment. `/metrics` carries the four
//! `retrieval_provider_*` families (D-C); `--metrics-families` prints the same render at zero state.

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    process::ExitCode,
    sync::atomic::AtomicBool,
    time::Duration,
};

use async_trait::async_trait;
use humaux_adapters::disclosure::DisclosureSource;
use humaux_adapters::postgres::RetrievalWorkerDbPool;
use humaux_adapters::projection_worker::{
    CardEmbedder, PassConfig, PassOutcome, SharedProjectionDeps, run_claimed_pass,
};
use humaux_adapters::qdrant::RetrievalFamily;
use humaux_adapters::stream_repo::{Backoff, ClaimFamily};
use humaux_domain::egress::ProcessorId;
use humaux_domain::error::ErrorCode;
use humaux_domain::ids::TenantId;
use humaux_infra_cell::{
    CallerId, CellId, HttpIntraCellTransport, IntraCellHttpTransport, IntraCellMethod,
    IntraCellRequest, IntraCellResource, IntraCellResourceRegistry, ResourceEntry,
    authorize_cell_access,
};
use humaux_local_secret_scan::{LocalSecretScanner, LocalSecretScannerConfig, SealedRetrievalCard};
use humaux_retrieval_provider::adapters::embedding_provider_for;
use humaux_retrieval_provider::contract::{EmbeddingModelDescriptor, EmbeddingProvider, ModelId};
use humaux_retrieval_provider::metrics as provider_metrics;
use humaux_telemetry::metrics::{
    OpsListener, families, parse_ops_addr, process_routes, serve_loopback, write_family,
    write_histogram,
};
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
    "usage: humaux-retrieval-worker (--readyz | --run-once | --serve | --serve-rpc | --metrics-families)"
}

/// §78.1 / ADR-0061 D-B: `--serve-rpc`'s own ops listener address (loopback only, no default).
const SERVE_RPC_METRICS_ADDR: &str = "HUMAUX_RETRIEVAL_WORKER_SERVE_RPC_METRICS_ADDR";
/// §78.1 / ADR-0061 D-B: `--serve`'s own ops listener address (loopback only, no default).
const SERVE_METRICS_ADDR: &str = "HUMAUX_RETRIEVAL_WORKER_SERVE_METRICS_ADDR";

/// The four §41.2 `retrieval_provider_*` families this process counts (ADR-0061 D-C), each seeded over its
/// closed label product by the `snapshot_*` functions. `/metrics` and `--metrics-families` both call this.
fn render_metrics(out: &mut String) {
    let counter = |out: &mut String, family, rows: Vec<(Vec<&'static str>, u64)>| {
        let samples: Vec<_> = rows
            .iter()
            .map(|(labels, n)| (labels.as_slice(), *n as f64))
            .collect();
        write_family(out, family, &samples);
    };
    counter(
        out,
        &families::RETRIEVAL_PROVIDER_REQUESTS_TOTAL,
        provider_metrics::snapshot_requests_total(),
    );
    let latency = provider_metrics::snapshot_latency_seconds();
    let samples: Vec<_> = latency
        .iter()
        .map(|(labels, n, sum)| (labels.as_slice(), *n, *sum))
        .collect();
    write_histogram(out, &families::RETRIEVAL_PROVIDER_LATENCY_SECONDS, &samples);
    counter(
        out,
        &families::RETRIEVAL_PROVIDER_TOKENS_TOTAL,
        provider_metrics::snapshot_tokens_total(),
    );
    counter(
        out,
        &families::RETRIEVAL_PROVIDER_COST_TOTAL,
        provider_metrics::snapshot_cost_total(),
    );
}

/// Binds `key`'s loopback ops listener for resident `mode` (ADR-0061 D-B). The handle must live as long as the
/// mode: dropping it closes the port.
fn ops_listener(key: &'static str, mode: &'static str) -> Result<OpsListener, String> {
    let addr = parse_ops_addr(&required(key)?)
        .map_err(|reason| format!("invalid configuration: {key}: {reason}"))?;
    let routes = process_routes(
        "humaux-retrieval-worker",
        mode,
        env!("CARGO_PKG_VERSION"),
        option_env!("HUMAUX_BUILD_GIT_SHA"),
        render_metrics,
    );
    // dep: HTTP(loopback) — this mode's /metrics + /status listener
    serve_loopback(key, addr, routes).map_err(|e| e.to_string())
}

/// Lifetime of each Qdrant permit (pre-existing value; ADR-0052 D-F mints one per ticket, so a
/// resident process never outlives the permit it writes with).
const PERMIT_TTL: Duration = Duration::from_secs(60);

/// ADR-0052 D-F: the seven pass keys (§78, required, no defaults — ADR-0036 precedent: the typed
/// registry has no retrieval-worker table). Read through `lookup` so the unit tests exercise the
/// real parser without touching process-global environment.
#[derive(Debug, Clone, PartialEq)]
struct ServeConfig {
    batch: i64,
    poll_interval: Duration,
    lease_secs: u64,
    per_tenant_cap: i64,
    max_attempts: i32,
    backoff_base_secs: u64,
    backoff_max_secs: u64,
}

impl ServeConfig {
    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let positive = |name: &str| -> Result<u64, String> {
            let raw =
                lookup(name).ok_or_else(|| format!("missing required configuration: {name}"))?;
            match raw.parse::<u64>() {
                Ok(n) if n > 0 => Ok(n),
                _ => Err(format!(
                    "invalid configuration: {name} (a positive integer)"
                )),
            }
        };
        let narrow = |name: &str, n: u64| -> Result<i64, String> {
            i64::try_from(n).map_err(|_| format!("invalid configuration: {name}"))
        };
        let batch = positive("HUMAUX_RETRIEVAL_WORKER_BATCH")?;
        let config = Self {
            batch: narrow("HUMAUX_RETRIEVAL_WORKER_BATCH", batch)?,
            poll_interval: Duration::from_secs(positive(
                "HUMAUX_RETRIEVAL_WORKER_POLL_INTERVAL_SECS",
            )?),
            lease_secs: positive("HUMAUX_RETRIEVAL_WORKER_LEASE_SECS")?,
            per_tenant_cap: narrow(
                "HUMAUX_RETRIEVAL_WORKER_PER_TENANT_CAP",
                positive("HUMAUX_RETRIEVAL_WORKER_PER_TENANT_CAP")?,
            )?,
            max_attempts: i32::try_from(positive("HUMAUX_RETRIEVAL_WORKER_MAX_ATTEMPTS")?)
                .map_err(|_| "invalid configuration: HUMAUX_RETRIEVAL_WORKER_MAX_ATTEMPTS")?,
            backoff_base_secs: positive("HUMAUX_RETRIEVAL_WORKER_BACKOFF_BASE_SECS")?,
            backoff_max_secs: positive("HUMAUX_RETRIEVAL_WORKER_BACKOFF_MAX_SECS")?,
        };
        if config.backoff_max_secs < config.backoff_base_secs {
            return Err(
                "invalid configuration: HUMAUX_RETRIEVAL_WORKER_BACKOFF_MAX_SECS must be \
                 >= HUMAUX_RETRIEVAL_WORKER_BACKOFF_BASE_SECS"
                    .to_owned(),
            );
        }
        Ok(config)
    }

    /// The pass knobs under this process's own lease owner. `claim` is derived from
    /// [`PROJECTION_FAMILY`] (card 21: no triple in the environment).
    fn pass(&self, claim: ClaimFamily, lease_owner: String) -> PassConfig {
        PassConfig {
            claim,
            lease_owner,
            lease_secs: self.lease_secs as f64,
            batch: self.batch,
            per_tenant_cap: self.per_tenant_cap,
            max_attempts: self.max_attempts,
            backoff: Backoff {
                base_secs: self.backoff_base_secs as f64,
                max_secs: self.backoff_max_secs as f64,
            },
        }
    }
}

/// The §17 retrieval family this process projects into. Single point of truth for BOTH the
/// §17.3 placement family the ADR-0052 claim joins and — through
/// [`RetrievalFamily::ticket_family`] — the §15.1 ticket triple it claims, so the two can never
/// name different families (card 21).
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
        // ADR-0061 D-C: before any configuration is read.
        "--metrics-families" => {
            let mut out = String::new();
            render_metrics(&mut out);
            print!("{out}");
            Ok(())
        }
        "--readyz" => readyz().await,
        "--run-once" => projection_mode(false).await,
        "--serve" => projection_mode(true).await,
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
    let (registry, transport) = build_cell_access()?;
    let permit = authorize_cell_access(&registry, IntraCellResource::QDRANT_REST, PERMIT_TTL)
        .map_err(|_| "retrieval worker is not authorized for the Qdrant cell".to_owned())?;
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

/// `--run-once` (`resident == false`: one pass, then exit — nothing claimable exits 0) and
/// `--serve` (the same pass every poll interval until SIGTERM/SIGINT). ADR-0052 D-F.
async fn projection_mode(resident: bool) -> Result<(), Outcome> {
    // ADR-0061 D-B: only the resident mode is scraped; a one-shot `--run-once` opens no listener.
    let _ops = if resident {
        Some(ops_listener(SERVE_METRICS_ADDR, "serve")?)
    } else {
        None
    };
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
    // §78.1 / card 21: the ticket triple is DERIVED from the retrieval family this process
    // projects into; `None` would be a wiring bug, not a deployment shape.
    let claim = ClaimFamily::of(PROJECTION_FAMILY)
        .ok_or_else(|| format!("{PROJECTION_FAMILY:?} is not a §15.1 ticket-stream family"))?;
    let config = ServeConfig::from_lookup(|name| env::var(name).ok())?;
    let dimension = parse::<u32>("HUMAUX_RETRIEVAL_WORKER_DIMENSION")?;
    if dimension == 0 {
        return Err(Outcome::Failed(
            "invalid configuration: HUMAUX_RETRIEVAL_WORKER_DIMENSION".to_owned(),
        ));
    }
    let embedding_version = required("HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION")?;
    let dsn = required("HUMAUX_RETRIEVAL_WORKER_PG_DSN")?;
    let processor_id = egress_processor_id()?;
    let (registry, transport) = build_cell_access()?;
    let scanner = Arc::new(build_scanner()?);
    // Per-process owner: every settle/retry/release is fenced on it plus the claim's attempts,
    // so two resident workers never both write one ticket.
    let pass = config.pass(claim, format!("humaux-retrieval-worker/{}", Uuid::now_v7()));

    // Both handlers are installed before the first pass (the consolidation worker's reason:
    // a signal during pass one must not hit the default disposition mid-pass).
    let mut shutdown = Shutdown::install()?;
    // Connected lazily and kept: a database that is down at start (or later) is a logged failed
    // pass in `--serve`, retried next poll — never a crash loop the supervisor has to absorb.
    let mut shared: Option<SharedProjectionDeps> = None;
    loop {
        if shared.is_none() {
            match connect_shared(
                &dsn,
                dimension,
                &embedding_version,
                processor_id,
                &registry,
                &transport,
                &scanner,
            )
            .await
            {
                Ok(deps) => shared = Some(deps),
                Err(error) if resident => {
                    eprintln!("humaux-retrieval-worker: projection pass failed: {error}")
                }
                Err(error) => return Err(Outcome::Failed(error)),
            }
        }
        if let Some(deps) = &shared {
            match run_claimed_pass(deps, &pass).await {
                Ok(outcome) => println!("humaux-retrieval-worker: {}", pass_line(&outcome)),
                Err(error) if resident => {
                    eprintln!("humaux-retrieval-worker: projection pass failed: {error}")
                }
                Err(error) => {
                    return Err(Outcome::Failed(format!("projection pass failed: {error}")));
                }
            }
        }
        if !resident {
            return Ok(());
        }
        // ADR-0037: the signal is only ever observed BETWEEN passes. A pass settles, retries or
        // releases every ticket it claimed before it returns, so exiting here never leaves a live
        // lease only expiry could free. Shutdown latency is one pass: the supervisor's grace must
        // exceed BATCH × worst-case ticket time (docs/ops/supervision.md).
        tokio::select! {
            () = tokio::time::sleep(config.poll_interval) => {}
            () = shutdown.recv() => {
                eprintln!("humaux-retrieval-worker: signal received between passes, exiting");
                return Ok(());
            }
        }
    }
}

/// One line per pass (ADR-0052 D-F); `claim_ms` is the claim-latency sample the ADR reports.
fn pass_line(o: &PassOutcome) -> String {
    format!(
        "projection pass claimed={} done={} skipped={} failed={} retried={} refunded={} \
         pending={} lost_lease={} placement_missing={} placement_invalid={} claim_ms={}",
        o.claimed,
        o.done,
        o.skipped,
        o.failed,
        o.retried,
        o.refunded,
        o.pending,
        o.lost_lease,
        o.placement_missing,
        o.placement_invalid,
        o.claim_ms
    )
}

/// The database-bound half of [`SharedProjectionDeps`]: the worker pool and the embedder (whose
/// disclosure ledger needs its own pool). Everything else was validated before the loop.
async fn connect_shared(
    dsn: &str,
    dimension: u32,
    embedding_version: &str,
    processor_id: ProcessorId,
    registry: &IntraCellResourceRegistry,
    transport: &Arc<HttpIntraCellTransport>,
    scanner: &Arc<LocalSecretScanner>,
) -> Result<SharedProjectionDeps, String> {
    // dep: PostgreSQL(role_retrieval_worker) — the projection runner's claim/settle pool
    let pool = RetrievalWorkerDbPool::connect(dsn)
        .await
        .map_err(|e| format!("retrieval worker database role connection failed: {e}"))?;
    let embedder = build_embedder(dsn, dimension)
        .await
        .map_err(|outcome| match outcome {
            Outcome::NotApplicable(reason) | Outcome::Failed(reason) => reason,
        })?;
    let transport: Arc<dyn IntraCellHttpTransport> = transport.clone();
    let registry = registry.clone();
    Ok(SharedProjectionDeps {
        pool,
        embedder,
        scanner: scanner.clone(),
        transport,
        // ADR-0052 D-F: one permit per ticket, so no permit outlives its PERMIT_TTL.
        mint_permit: Arc::new(move || {
            authorize_cell_access(&registry, IntraCellResource::QDRANT_REST, PERMIT_TTL).ok()
        }),
        embedding_version: embedding_version.to_owned(),
        dimension,
        // Card 21: the SAME §7.4 identity the embedding provider discloses under; advance_prefix
        // writes it into projection.stream_checkpoints.projection_processor_id (0171).
        processor_id,
        // The pass's background lease heartbeat waits on this process's runtime timer.
        sleep: Arc::new(|period| Box::pin(tokio::time::sleep(period))),
        dependency_down: AtomicBool::new(false),
    })
}

/// SIGTERM/Ctrl-C, as one awaitable — copied from `bins/consolidation-worker/src/main.rs` (a
/// binary cannot import another binary). Both handlers are installed eagerly: `ctrl_c()` would
/// register SIGINT only on its first poll, i.e. after pass one, and a Ctrl-C during that pass
/// would kill the process with its claimed tickets still leased.
struct Shutdown {
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
}

impl Shutdown {
    fn install() -> Result<Self, String> {
        Ok(Self {
            #[cfg(unix)]
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|e| format!("cannot install the SIGTERM handler: {e}"))?,
            #[cfg(unix)]
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .map_err(|e| format!("cannot install the SIGINT handler: {e}"))?,
        })
    }

    async fn recv(&mut self) {
        #[cfg(unix)]
        tokio::select! {
            _ = self.interrupt.recv() => {}
            _ = self.terminate.recv() => {}
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Wires the Qdrant `IntraCellResource` registry and the HTTP transport — mirrors
/// `bins/public-worker/src/main.rs`'s identical block. Returns the registry, not a permit:
/// ADR-0052 D-F mints one permit per ticket from it.
fn build_cell_access() -> Result<
    (
        IntraCellResourceRegistry,
        std::sync::Arc<HttpIntraCellTransport>,
    ),
    Outcome,
> {
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
    authorize_cell_access(&registry, IntraCellResource::QDRANT_REST, PERMIT_TTL)
        .map_err(|_| "retrieval worker is not authorized for the Qdrant cell".to_owned())?;
    let transport = HttpIntraCellTransport::new(
        registry.clone(),
        Duration::from_secs(10),
        humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES,
    )
    .map_err(|_| "could not construct Qdrant cell transport".to_owned())?;
    Ok((registry, std::sync::Arc::new(transport)))
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

/// This process's §7 egress identity, read from configuration — deployment identity, never tenant
/// data. (The private worker instead holds a recipient list, `HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS`,
/// and takes each call's recipient from its admitted route, ADR-0060 D-C / D-L.)
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

    use super::{
        Outcome, SERVE_RPC_METRICS_ADDR, build_embedding_provider, build_scanner, ops_listener,
        parse, required,
    };
    use humaux_retrieval_worker::rpc::{RpcState, router};

    pub async fn run() -> Result<(), Outcome> {
        let _ops = ops_listener(SERVE_RPC_METRICS_ADDR, "serve-rpc")?;
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
            pg_dsn: dsn,
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
    use super::{ClaimFamily, PROJECTION_FAMILY, ServeConfig, egress_processor_id};
    use humaux_domain::ticket_family::TicketFamily;
    use std::collections::HashMap;
    use uuid::Uuid;

    const PASS_KEYS: [&str; 7] = [
        "HUMAUX_RETRIEVAL_WORKER_BATCH",
        "HUMAUX_RETRIEVAL_WORKER_POLL_INTERVAL_SECS",
        "HUMAUX_RETRIEVAL_WORKER_LEASE_SECS",
        "HUMAUX_RETRIEVAL_WORKER_PER_TENANT_CAP",
        "HUMAUX_RETRIEVAL_WORKER_MAX_ATTEMPTS",
        "HUMAUX_RETRIEVAL_WORKER_BACKOFF_BASE_SECS",
        "HUMAUX_RETRIEVAL_WORKER_BACKOFF_MAX_SECS",
    ];

    fn full() -> HashMap<String, String> {
        PASS_KEYS
            .iter()
            .zip(["16", "1", "60", "8", "6", "30", "300"])
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// ADR-0052 D-F / §78: every pass key is required, positive, and has no default. Each key
    /// in turn is removed, zeroed and made non-numeric; each refusal names that key. Fault
    /// injection: give any key a default (`.unwrap_or("1")`) ⇒ its missing case goes red.
    #[test]
    fn serve_config_refuses_each_missing_or_zero_key() {
        let ok = ServeConfig::from_lookup(|k| full().get(k).cloned()).expect("full config");
        assert_eq!(ok.batch, 16);
        assert_eq!(ok.per_tenant_cap, 8);
        assert_eq!(ok.max_attempts, 6);
        for key in PASS_KEYS {
            for bad in [None, Some("0"), Some("-3"), Some("x")] {
                let mut env = full();
                match bad {
                    None => {
                        env.remove(key);
                    }
                    Some(v) => {
                        env.insert(key.to_string(), v.to_string());
                    }
                }
                let err = ServeConfig::from_lookup(|k| env.get(k).cloned())
                    .expect_err("a missing or non-positive pass key is refused");
                assert!(err.contains(key), "{key} {bad:?}: {err}");
            }
        }
        let mut inverted = full();
        inverted.insert(PASS_KEYS[6].to_string(), "10".to_string());
        let err = ServeConfig::from_lookup(|k| inverted.get(k).cloned())
            .expect_err("max backoff below base backoff is refused");
        assert!(err.contains("BACKOFF_MAX_SECS"), "{err}");
    }

    /// ADR-0052 D-F: `--run-once` / `--serve` take no tenant, workspace or collection from the
    /// environment. Structural half: the pass config parses with those keys absent, and the pass
    /// it builds carries no tenant. Source half: none of the four deleted keys is read anywhere in
    /// this binary (the names are assembled here so this test cannot match itself). Fault
    /// injection: restore `parse::<Uuid>("…TENANT_ID")` in the projection mode ⇒ red.
    #[test]
    fn run_once_reads_no_tenant_pin() {
        let claim = ClaimFamily::of(PROJECTION_FAMILY).expect("a ticket family");
        let pass = ServeConfig::from_lookup(|k| full().get(k).cloned())
            .expect("no tenant key needed")
            .pass(claim.clone(), "owner".to_owned());
        assert_eq!(pass.claim, claim);
        assert_eq!(claim.projection_version, "v1");
        let source = include_str!("main.rs");
        for suffix in ["TENANT_ID", "SCOPE_KIND", "SCOPE_ID", "QDRANT_COLLECTION"] {
            let name = format!("{}{suffix}", "HUMAUX_RETRIEVAL_WORKER_");
            assert!(!source.contains(&name), "{name} is still read");
        }
    }

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
