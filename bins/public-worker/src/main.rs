//! `public-worker::main` — `humaux-public-worker` explicit bounded Phase 9 runner (§14 / §31).
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-infra-cell, tokio, uuid];
//!   services=[PostgreSQL(role_public_worker), Qdrant(*)]; env=[HUMAUX_PUBLIC_WORKER_CALLER,
//!   HUMAUX_PUBLIC_WORKER_CELL_ID, HUMAUX_PUBLIC_WORKER_LEASE_OWNER, HUMAUX_PUBLIC_WORKER_LIMIT,
//!   HUMAUX_PUBLIC_WORKER_PG_DSN, HUMAUX_PUBLIC_WORKER_QDRANT_CIDR, HUMAUX_PUBLIC_WORKER_QDRANT_COLLECTION,
//!   HUMAUX_PUBLIC_WORKER_QDRANT_HOST, HUMAUX_PUBLIC_WORKER_QDRANT_PORT, HUMAUX_PUBLIC_WORKER_QDRANT_TLS,
//!   HUMAUX_PUBLIC_WORKER_TENANT_ID]; modules=[adapters::postgres, adapters::public_projection,
//!   adapters::public_repo, domain::ids, infra-cell::permit, infra-cell::resource, infra-cell::transport]
//! Called-by: [process(humaux-public-worker)]
//! Invariants: [a worker that cannot reach PostgreSQL or Qdrant exits non-zero rather than silently skipping its lease]
//! Spec: Baseline §4.2; §4.4; §31; ADR-0037
//!
//! Phase10 resident evolution is not enabled — this binary has no poll loop, so its whole
//! lifetime is one bounded pass.
//!
//! Card 15 / ADR-0037:
//! * `--readyz` — one live round trip to each dependency this process cannot work without
//!   ([`readyz`]), then exit.
//! * SIGTERM/Ctrl-C during a pass is LATCHED, never acted on ([`ShutdownLatch`]). A pass holds
//!   a real `ops.outbox` lease; cancelling it mid-flight would leave that row PROCESSING with a
//!   live lease only expiry could free — exactly what the card forbids. For a bounded one-shot,
//!   "drain" means "let the pass settle its lease and exit zero", which is what happens here;
//!   the latch exists so the operator sees that decision in the log instead of inferring it
//!   from silence.

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    process::ExitCode,
    time::Duration,
};

use humaux_adapters::{
    postgres::PublicWorkerDbPool, public_projection::PublicProjectionAdapter, public_repo,
};
use humaux_domain::ids::TenantId;
use humaux_infra_cell::{
    CallerId, CellId, HttpIntraCellTransport, IntraCellHttpTransport, IntraCellMethod,
    IntraCellRequest, IntraCellResource, IntraCellResourceRegistry, ResourceEntry,
    authorize_cell_access,
};
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
    "usage: humaux-public-worker (--readyz | --run-once | --run-anonymous-once)"
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("humaux-public-worker: {error}");
            ExitCode::from(2)
        }
    }
}

/// SIGTERM/Ctrl-C latch. BOTH handlers are installed synchronously inside `install()`, before
/// the pass starts (so neither signal is ever lost) and the flag is only READ after the pass has
/// settled — see the module doc for why a bounded, lease-holding pass must not be cancelled.
///
/// `tokio::signal::ctrl_c()` is deliberately not used for the SIGINT half: it registers its
/// handler on the future's FIRST POLL, which happens inside the spawned task — i.e. whenever the
/// scheduler first gets to it, which may be after the pass already claimed an `ops.outbox` lease.
/// A Ctrl-C in that window would hit SIGINT's default disposition and kill the process holding
/// the lease. `SignalKind::interrupt()` is registered here, on the caller's thread, instead.
struct ShutdownLatch(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl ShutdownLatch {
    fn install() -> Result<Self, String> {
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        #[cfg(unix)]
        {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .map_err(|e| format!("cannot install the SIGTERM handler: {e}"))?;
            let mut interrupt =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                    .map_err(|e| format!("cannot install the SIGINT handler: {e}"))?;
            let flag = flag.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = interrupt.recv() => {}
                    _ = terminate.recv() => {}
                }
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
            });
        }
        Ok(Self(flag))
    }

    fn requested(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// `--readyz`: one live round trip per dependency this process cannot work without, each named
/// when it is down (§4.4 坑5 applied to readiness). Uses no configuration key the run modes do
/// not already need, except that the anonymous/tenant choice is irrelevant to readiness —
/// neither `HUMAUX_PUBLIC_WORKER_TENANT_ID` nor `..._LEASE_OWNER` is read here, because a
/// readiness probe must not claim anything.
///
/// 1. `role_public_worker` connects AND `current_user` matches (§6.2.3 assertion E).
/// 2. The Qdrant §83.4 Layer 1B cell resource answers a real HTTP call through the SAME
///    registry/permit/transport the projection path uses.
async fn readyz() -> Result<(), String> {
    let dsn = required("HUMAUX_PUBLIC_WORKER_PG_DSN")?;
    // dep: PostgreSQL(role_public_worker) — opens the role_public_worker pool for the projection loop
    PublicWorkerDbPool::connect(&dsn).await.map_err(|e| {
        format!("not ready — missing object: PostgreSQL as role_public_worker ({e})")
    })?;
    let (permit, transport) = build_cell_access()?;
    let status = transport
        .execute(
            &permit,
            // dep: Qdrant(*) — opens the public-visibility Qdrant client
            IntraCellRequest {
                method: IntraCellMethod::Get,
                path: "/".to_owned(),
                json_body: None,
                headers: Vec::new(),
            },
        )
        .await
        .map_err(|e| format!("not ready — missing object: the Qdrant cell resource ({e:?})"))?
        .status;
    if status >= 500 {
        return Err(format!(
            "not ready — missing object: the Qdrant cell resource answered {status}"
        ));
    }
    println!("humaux-public-worker: ready db=role_public_worker qdrant_status={status}");
    Ok(())
}

/// The Qdrant `IntraCellResource` entry, its permit, and the HTTP transport — one builder for
/// the run modes and `--readyz`, so readiness can never probe a different resource than the one
/// the pass will use.
fn build_cell_access()
-> Result<(humaux_infra_cell::CellAccessPermit, HttpIntraCellTransport), String> {
    let cell_id = CellId(parse::<Uuid>("HUMAUX_PUBLIC_WORKER_CELL_ID")?);
    let caller = CallerId(required("HUMAUX_PUBLIC_WORKER_CALLER")?);
    let host = required("HUMAUX_PUBLIC_WORKER_QDRANT_HOST")?;
    let port = parse::<u16>("HUMAUX_PUBLIC_WORKER_QDRANT_PORT")?;
    if port == 0 {
        return Err("invalid configuration: HUMAUX_PUBLIC_WORKER_QDRANT_PORT".to_owned());
    }
    let cidr = required("HUMAUX_PUBLIC_WORKER_QDRANT_CIDR")?
        .parse()
        .map_err(|_| "invalid configuration: HUMAUX_PUBLIC_WORKER_QDRANT_CIDR".to_owned())?;
    let tls = parse::<bool>("HUMAUX_PUBLIC_WORKER_QDRANT_TLS")?;

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
    .map_err(|_| "public worker is not authorized for the Qdrant cell".to_owned())?;
    let transport = HttpIntraCellTransport::new(
        registry,
        Duration::from_secs(10),
        humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES,
    )
    .map_err(|_| "could not construct Qdrant cell transport".to_owned())?;
    Ok((permit, transport))
}

async fn run() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 1
        || !matches!(
            args[0].as_str(),
            "--readyz" | "--run-once" | "--run-anonymous-once"
        )
    {
        return Err(usage().to_owned());
    }
    if args[0] == "--readyz" {
        return readyz().await;
    }
    let shutdown = ShutdownLatch::install()?;
    let tenant_id = if args[0] == "--run-once" {
        Some(TenantId(parse::<Uuid>("HUMAUX_PUBLIC_WORKER_TENANT_ID")?))
    } else {
        None
    };
    let lease_owner = required("HUMAUX_PUBLIC_WORKER_LEASE_OWNER")?;
    if lease_owner.trim().is_empty() {
        return Err("invalid configuration: HUMAUX_PUBLIC_WORKER_LEASE_OWNER".to_owned());
    }
    let limit = parse::<i64>("HUMAUX_PUBLIC_WORKER_LIMIT")?;
    if limit <= 0 {
        return Err("invalid configuration: HUMAUX_PUBLIC_WORKER_LIMIT".to_owned());
    }
    let collection = required("HUMAUX_PUBLIC_WORKER_QDRANT_COLLECTION")?;
    let (permit, transport) = build_cell_access()?;
    let projection = PublicProjectionAdapter::new(&transport, &permit, &collection)
        .map_err(|_| "invalid Qdrant collection configuration".to_owned())?;
    let dsn = required("HUMAUX_PUBLIC_WORKER_PG_DSN")?;
    // dep: PostgreSQL(role_public_worker) — reconnects the pool after a lease-loop error
    let pool = PublicWorkerDbPool::connect(&dsn)
        .await
        .map_err(|_| "public worker database role connection failed".to_owned())?;

    if let Some(tenant_id) = tenant_id {
        public_repo::drain_outbox(&pool, tenant_id, limit)
            .await
            .map_err(|_| "public outbox drain failed".to_owned())?;
        public_repo::run_once(&pool, tenant_id, &lease_owner, limit, &projection)
            .await
            .map_err(|_| "public run-once failed".to_owned())?;
    } else {
        public_repo::run_anonymous_once(&pool, &lease_owner, limit, &projection)
            .await
            .map_err(|_| "anonymous public run-once failed".to_owned())?;
    }
    if shutdown.requested() {
        eprintln!(
            "humaux-public-worker: signal received during the bounded pass; the pass was allowed \
             to settle its outbox lease before exit"
        );
    }
    Ok(())
}
