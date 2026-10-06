//! `maintenance::rebuild_cli` — the one-shot arms `projection rebuild` and `projection verify` (ADR-0064 D-E, D-F):
//!   flag parsing, the two pools and the Qdrant face, the per-stream loop and the one JSON receipt. Every step and
//!   every verdict lives in `adapters::rebuild`.
//! Depends-on: crates=[humaux-adapters, serde_json, tokio, uuid]; services=[PostgreSQL(role_maintenance),
//!   PostgreSQL(role_retrieval_worker), Qdrant(*)]; env=[HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_MAINTENANCE_QDRANT_CIDR,
//!   HUMAUX_MAINTENANCE_QDRANT_HOST, HUMAUX_MAINTENANCE_QDRANT_PORT, HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION,
//!   HUMAUX_RETRIEVAL_WORKER_PG_DSN]; modules=[adapters::postgres, adapters::provisioning, adapters::rebuild,
//!   maintenance::main]
//! Called-by: [maintenance::main]
//! Invariants: [every flag is required with no default except the D-G consent `--allow-reembed` (§78.1); flags are
//!   parsed before any connection (a missing flag is exit 2 with nothing read); exactly one receipt object per stream
//!   in `streams`; exit 0 only when every stream closed (rebuild) or reads (verify) `equivalent`, 3 when any stream
//!   was refused, 1 otherwise; `verify` writes nothing; the worker's label, its DSN and the Qdrant face are read
//!   under their own names (the card-35 peer-key precedent), never copied into a maintenance key]
//! Spec: Baseline §16.2; §44; §78.1; ADR-0053 D-F; ADR-0064 D-E; ADR-0064 D-F; ADR-0064 D-G
//!
//! Why the retrieval worker's DSN: X and the E4 payloads are what the projector builds, and only
//! `role_retrieval_worker` sees every memory of a tenant (0140); `role_maintenance`'s view is narrowed by the
//! visibility and subject policies (0012/0155). See `adapters::rebuild`'s module doc.

use std::time::Duration;

use humaux_adapters::postgres::RetrievalWorkerDbPool;
use humaux_adapters::provisioning::QdrantFace;
use humaux_adapters::rebuild::{self, RebuildDeps, RebuildOptions, Stream, StreamOutcome, Verdict};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{Args, Failure, Output, Result, env, env_parsed, pool};

/// The worker's label, read under its own name (ADR-0064 D-E step 1).
const WORKER_LABEL: &str = "HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION";
/// The projector's DSN, read under its own name (module doc).
const WORKER_DSN: &str = "HUMAUX_RETRIEVAL_WORKER_PG_DSN";

/// ADR-0064 D-E step 5: the poll step inside the operator's `--wait-seconds` bound while the resident retrieval
/// worker projects the generation (not a schedule; the bound is the operator's).
const POLL: Duration = Duration::from_secs(1);

/// `(--tenant <uuid> [--workspace <uuid>] | --all)`.
enum Scope {
    One {
        tenant: Uuid,
        workspace: Option<Uuid>,
    },
    All,
}

fn scope(args: &Args) -> Result<Scope> {
    let all = args.0.iter().any(|a| a == "--all");
    match (all, args.get("--tenant")) {
        (true, None) if args.get("--workspace").is_none() => Ok(Scope::All),
        (false, Some(_)) => Ok(Scope::One {
            tenant: args.parsed("--tenant")?,
            workspace: match args.get("--workspace") {
                Some(_) => Some(args.parsed("--workspace")?),
                None => None,
            },
        }),
        _ => Err(Failure::Usage(
            "exactly one of --tenant <uuid> [--workspace <uuid>] or --all".to_owned(),
        )),
    }
}

/// The two pools, the Qdrant face and the bound label every stream uses.
struct Wiring {
    maintenance: humaux_adapters::postgres::MaintenanceDbPool,
    reader: RetrievalWorkerDbPool,
    face: QdrantFace,
    worker: rebuild::WorkerLabel,
}

async fn wiring() -> Result<Wiring> {
    let maintenance = pool().await?;
    // dep: PostgreSQL(role_retrieval_worker) — the projector-view reads of the verifier
    let reader = RetrievalWorkerDbPool::connect(&env(WORKER_DSN)?)
        .await
        .map_err(|e| Failure::Infra(format!("connect {WORKER_DSN}: {e}")))?;
    // dep: Qdrant(*) — the operator's face on the placement collections
    let face = QdrantFace::new(
        &env("HUMAUX_MAINTENANCE_QDRANT_HOST")?,
        env_parsed("HUMAUX_MAINTENANCE_QDRANT_PORT")?,
        &env("HUMAUX_MAINTENANCE_QDRANT_CIDR")?,
    )?;
    let worker = rebuild::worker_label(&maintenance, &env(WORKER_LABEL)?).await?;
    Ok(Wiring {
        maintenance,
        reader,
        face,
        worker,
    })
}

impl Wiring {
    fn deps(&self) -> RebuildDeps<'_> {
        RebuildDeps {
            maintenance: &self.maintenance,
            reader: &self.reader,
            qdrant: &self.face,
            worker: &self.worker,
        }
    }

    /// The streams of the scope (`--all`: every tenant's).
    async fn streams(&self, scope: &Scope) -> Result<Vec<Stream>> {
        let tenants = match scope {
            Scope::One { tenant, workspace } => {
                return Ok(rebuild::streams(&self.maintenance, *tenant, *workspace).await?);
            }
            Scope::All => rebuild::all_tenants(&self.maintenance).await?,
        };
        let mut streams = Vec::new();
        for tenant in tenants {
            streams.extend(rebuild::streams(&self.maintenance, tenant, None).await?);
        }
        Ok(streams)
    }
}

/// The receipt over every stream object: exit 3 if any refused, 1 if any other was not `equivalent`.
fn receipt(command: &str, outcomes: Vec<StreamOutcome>) -> Output {
    let refused = outcomes.iter().any(|o| o.refused);
    let failed = outcomes
        .iter()
        .any(|o| !o.refused && o.verdict != Verdict::Equivalent);
    let streams: Vec<Value> = outcomes.into_iter().map(|o| o.receipt).collect();
    let mut output = Output::ok(json!({
        "command": command,
        "outcome": if refused { "refused" } else if failed { "not_equivalent" } else { "equivalent" },
        "streams": streams,
    }));
    output.refused = refused;
    output.failed = failed;
    output
}

/// `projection rebuild (--tenant <uuid> [--workspace <uuid>] | --all) --batch <n> --wait-seconds <n>
/// [--allow-reembed <max_points>]` (ADR-0064 D-E).
pub(crate) async fn rebuild(args: &Args) -> Result<Output> {
    let scope = scope(args)?;
    let batch: i32 = args.parsed("--batch")?;
    let wait = Duration::from_secs(args.parsed("--wait-seconds")?);
    if batch <= 0 {
        return Err(Failure::Usage("--batch: must be > 0".to_owned()));
    }
    let allow_reembed: Option<u64> = match args.get("--allow-reembed") {
        Some(_) => Some(args.parsed("--allow-reembed")?),
        None => None,
    };
    let wiring = wiring().await?;
    let deps = wiring.deps();
    let pump = || -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(tokio::time::sleep(POLL))
    };
    let opts = RebuildOptions {
        batch,
        wait,
        allow_reembed,
        require_stored_vector: false,
        pump: &pump,
    };
    let mut outcomes = Vec::new();
    for stream in wiring.streams(&scope).await? {
        outcomes.push(rebuild::rebuild_stream(&deps, &stream, &opts).await?);
    }
    Ok(receipt("projection rebuild", outcomes))
}

/// `projection verify (--tenant <uuid> [--workspace <uuid>] | --all)` (ADR-0064 D-F, read-only).
pub(crate) async fn verify(args: &Args) -> Result<Output> {
    let scope = scope(args)?;
    let wiring = wiring().await?;
    let deps = wiring.deps();
    let mut outcomes = Vec::new();
    for stream in wiring.streams(&scope).await? {
        let report = rebuild::verify_stream(&deps, &stream, None, false).await?;
        outcomes.push(StreamOutcome {
            verdict: report.verdict,
            refused: false,
            receipt: report.json,
        });
    }
    Ok(receipt("projection verify", outcomes))
}
