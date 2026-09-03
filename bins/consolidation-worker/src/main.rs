//! `humaux-consolidation-worker` process entry (§4.2 minimal process set; §4.4 admin probe
//! contract; §11.7/§11.8 T4.6+T4.7). The actual orchestration ([`run_once`] and friends) lives
//! in `src/lib.rs` — see that module's doc comment for why: a binary-only crate has no target
//! `tests/*.rs` can link against, so splitting it out is what makes `tests/run_once_e2e.rs`
//! and `tests/consolidation_hop_e2e.rs` possible at all.
//!
//! `--run-once` wires the real §11.8 inference hop
//! ([`humaux_consolidation_worker::inference_client::UdsInferenceClient`]) for one explicit
//! `(tenant_id, reasoning_domain_id)` pair — same "no config-loading infrastructure exists yet"
//! scaffold shape every other `bins/*/src/main.rs` in this workspace already uses (`required`/
//! `parse` env helpers, no discovery of *which* tenants have pending work). A real resident
//! loop that enumerates pending `(tenant_id, reasoning_domain_id)` pairs across tenants cannot
//! be built the way this binary's own DB role works: `role_consolidation_worker`'s RLS session
//! context is set per-call from a caller-supplied `tenant_id`
//! (`consolidate_repo::set_tenant_local`), so a cross-tenant "which tenants have pending
//! inputs" query would return zero rows under any single session's `humaux.tenant_id` — the
//! same reason `role_gateway`/`role_retrieval_worker` never self-discover tenants either
//! (`ops.jobs`' per-tenant claim is the existing pattern; no consolidation-shaped job type
//! exists yet). This binary polls the one pair it was given, on an interval, rather than
//! inventing that missing enumeration mechanism.
//!
//! `build_rollup` is [`humaux_consolidation_worker::build_rollup`]: the private worker's typed
//! JSON reply parsed fail-closed against this run's own materialized `(memory_id, evidence_id)`
//! pairs (`humaux_adapters::consolidation_reasoner::parse_rollup_output`, shared with the
//! private worker). §11.6's guard holds by construction — the closure still has no DB
//! capability and can only pick sources from ids the run itself recorded.

use std::env;
use std::process::ExitCode;
use std::time::Duration;

use humaux_adapters::consolidate_repo::PublishOutcome;
use humaux_adapters::postgres::ConsolidationDbPool;
use humaux_application::consolidate::{ReasoningRouteBindingId, ReasoningRouteBindingVersion};
use humaux_consolidation_worker::{
    RunOnceError, build_rollup, inference_client::UdsInferenceClient, run_once_bound,
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
    "usage: humaux-consolidation-worker (--probe-connection | --run-once)"
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match args.first().map(String::as_str) {
        None => {
            probe_connection().await;
            ExitCode::SUCCESS
        }
        Some("--probe-connection") => {
            probe_connection().await;
            ExitCode::SUCCESS
        }
        Some("--run-once") => match run_once_mode().await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("humaux-consolidation-worker: {error}");
                ExitCode::from(2)
            }
        },
        Some(_) => {
            eprintln!("humaux-consolidation-worker: {}", usage());
            ExitCode::from(2)
        }
    }
}

/// Original Phase 4 scaffold probe, preserved as the argument-less default so an existing
/// deploy invocation with no flags keeps behaving exactly as before this task.
async fn probe_connection() {
    match env::var("CONSOLIDATION_WORKER_PG_DSN") {
        Err(_) => {
            println!(
                "humaux-consolidation-worker: CONSOLIDATION_WORKER_PG_DSN not set, not wired yet (Phase 4 scaffold)"
            );
        }
        Ok(dsn) => match ConsolidationDbPool::connect(&dsn).await {
            Ok(_pool) => {
                println!("humaux-consolidation-worker: connected as role_consolidation_worker")
            }
            Err(e) => eprintln!("humaux-consolidation-worker: {e}"),
        },
    }
}

/// One explicit `(tenant_id, reasoning_domain_id)` target, polled on
/// `HUMAUX_CONSOLIDATION_WORKER_POLL_INTERVAL_SECS` until the process is killed — see module
/// doc for why this is a single pinned target, not a cross-tenant enumeration.
async fn run_once_mode() -> Result<(), String> {
    let dsn = required("CONSOLIDATION_WORKER_PG_DSN")?;
    let tenant_id = parse::<Uuid>("HUMAUX_CONSOLIDATION_WORKER_TENANT_ID")?;
    let reasoning_domain_id = parse::<Uuid>("HUMAUX_CONSOLIDATION_WORKER_REASONING_DOMAIN_ID")?;
    let binding_id =
        ReasoningRouteBindingId(parse::<Uuid>("HUMAUX_CONSOLIDATION_WORKER_BINDING_ID")?);
    let binding_version =
        ReasoningRouteBindingVersion(parse::<i64>("HUMAUX_CONSOLIDATION_WORKER_BINDING_VERSION")?);
    let workspace_id = match env::var("HUMAUX_CONSOLIDATION_WORKER_WORKSPACE_ID") {
        Ok(raw) if !raw.is_empty() => Some(raw.parse::<Uuid>().map_err(|_| {
            "invalid configuration: HUMAUX_CONSOLIDATION_WORKER_WORKSPACE_ID".to_owned()
        })?),
        _ => None,
    };
    let socket_path = required("HUMAUX_CONSOLIDATION_WORKER_RPC_SOCKET_PATH")?;
    let max_inputs = parse::<i64>("HUMAUX_CONSOLIDATION_WORKER_MAX_INPUTS")?;
    if max_inputs <= 0 {
        return Err("invalid configuration: HUMAUX_CONSOLIDATION_WORKER_MAX_INPUTS".to_owned());
    }
    let call_ttl = Duration::from_secs(parse::<u64>("HUMAUX_CONSOLIDATION_WORKER_CALL_TTL_SECS")?);
    let dial_timeout = Duration::from_secs(parse::<u64>(
        "HUMAUX_CONSOLIDATION_WORKER_DIAL_TIMEOUT_SECS",
    )?);
    let poll_interval = Duration::from_secs(parse::<u64>(
        "HUMAUX_CONSOLIDATION_WORKER_POLL_INTERVAL_SECS",
    )?);

    let pool = ConsolidationDbPool::connect(&dsn)
        .await
        .map_err(|e| format!("consolidation worker database role connection failed: {e}"))?;

    loop {
        let outcome = run_once_bound(
            &pool,
            |run_id| {
                UdsInferenceClient::new(
                    &pool,
                    socket_path.clone(),
                    tenant_id,
                    call_ttl,
                    dial_timeout,
                    run_id,
                )
            },
            tenant_id,
            reasoning_domain_id,
            binding_id,
            binding_version,
            workspace_id,
            max_inputs,
            build_rollup,
        )
        .await;
        match outcome {
            Ok(PublishOutcome::NoOutput) => {
                println!("humaux-consolidation-worker: no eligible inputs this pass")
            }
            Ok(PublishOutcome::StaleInput) => {
                println!("humaux-consolidation-worker: stale input, will retry next pass")
            }
            Ok(PublishOutcome::Published { rollup_id }) => {
                println!("humaux-consolidation-worker: published rollup {rollup_id}")
            }
            Err(RunOnceError::Reasoning(error)) => {
                eprintln!("humaux-consolidation-worker: inference hop failed this pass: {error}")
            }
            Err(RunOnceError::Repo(error)) => {
                eprintln!("humaux-consolidation-worker: repository error this pass: {error}")
            }
        }
        tokio::time::sleep(poll_interval).await;
    }
}
