//! `humaux-consolidation-worker` process entry (§4.2 minimal process set; §4.4 admin probe
//! contract; §11.7/§11.8 T4.6+T4.7). The actual orchestration ([`run_once`] and friends) lives
//! in `src/lib.rs` — see that module's doc comment for why: a binary-only crate has no target
//! `tests/*.rs` can link against, so splitting it out is what makes `tests/run_once_e2e.rs`
//! and `tests/consolidation_hop_e2e.rs` possible at all.
//!
//! Modes:
//! * no flag / `--probe-connection`: the original Phase 4 typed-role probe.
//! * `--run-once`: ONE bounded cross-tenant dispatch pass, then exit — including when there was
//!   nothing to claim (before ADR-0036 this flag looped forever on an env-pinned pair and never
//!   exited on an empty tenant, which is what the deployment report flagged).
//! * `--serve`: the same pass on `HUMAUX_CONSOLIDATION_WORKER_POLL_INTERVAL_SECS` until killed.
//!
//! ADR-0036 (card 14): there is no tenant id, reasoning domain, or route binding in this
//! binary's environment any more. `role_consolidation_worker`'s RLS session context is still set
//! per call from a caller-supplied tenant_id — which is exactly why "which tenants have pending
//! work" cannot be a plain query from this process — so discovery goes through the owner
//! SECURITY DEFINER `ops.claim_derived_work` (migration 0164), which is the ONE cross-tenant read
//! of `ops.jobs` in the workspace. Everything after the claim runs under the claimed job's own
//! tenant context through the ordinary repos, and the route binding is resolved per tenant
//! through the narrow 0147 resolver (`consolidate_repo::resolve_consolidate_binding`).
//!
//!
//! `build_rollup` is [`humaux_consolidation_worker::build_rollup`]: the private worker's typed
//! JSON reply parsed fail-closed against this run's own materialized `(memory_id, evidence_id)`
//! pairs (`humaux_adapters::consolidation_reasoner::parse_rollup_output`, shared with the
//! private worker). §11.6's guard holds by construction — the closure still has no DB
//! capability and can only pick sources from ids the run itself recorded.

use std::env;
use std::process::ExitCode;
use std::time::Duration;

use humaux_adapters::postgres::ConsolidationDbPool;
use humaux_consolidation_worker::{
    DispatchConfig, dispatch_pass, inference_client::UdsInferenceClient,
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
    "usage: humaux-consolidation-worker (--probe-connection | --run-once | --serve)"
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
        Some(mode @ ("--run-once" | "--serve")) => match dispatch_mode(mode == "--serve").await {
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

/// ADR-0036 `--run-once` (one bounded pass, then exit — `claimed == 0` exits zero promptly
/// instead of spinning) / `--serve` (the same pass on
/// `HUMAUX_CONSOLIDATION_WORKER_POLL_INTERVAL_SECS` until the process is killed).
async fn dispatch_mode(resident: bool) -> Result<(), String> {
    let dsn = required("CONSOLIDATION_WORKER_PG_DSN")?;
    let socket_path = required("HUMAUX_CONSOLIDATION_WORKER_RPC_SOCKET_PATH")?;
    let call_ttl = Duration::from_secs(parse::<u64>("HUMAUX_CONSOLIDATION_WORKER_CALL_TTL_SECS")?);
    let dial_timeout = Duration::from_secs(parse::<u64>(
        "HUMAUX_CONSOLIDATION_WORKER_DIAL_TIMEOUT_SECS",
    )?);
    let config = DispatchConfig {
        // Per-process owner: every terminal transition is fenced on it plus the claim's
        // `attempt`, so two resident workers never both settle one job.
        lease_owner: format!("humaux-consolidation-worker/{}", Uuid::now_v7()),
        lease_seconds: parse::<u64>("HUMAUX_CONSOLIDATION_WORKER_LEASE_SECS")? as f64,
        batch: parse::<i64>("HUMAUX_CONSOLIDATION_WORKER_BATCH")?,
        max_inputs: parse::<i64>("HUMAUX_CONSOLIDATION_WORKER_MAX_INPUTS")?,
        max_attempts: parse::<i32>("HUMAUX_CONSOLIDATION_WORKER_MAX_ATTEMPTS")?,
    };
    config
        .validate()
        .map_err(|field| format!("invalid configuration: HUMAUX_CONSOLIDATION_WORKER_{field}"))?;
    let poll_interval = if resident {
        Some(Duration::from_secs(parse::<u64>(
            "HUMAUX_CONSOLIDATION_WORKER_POLL_INTERVAL_SECS",
        )?))
    } else {
        None
    };

    let pool = ConsolidationDbPool::connect(&dsn)
        .await
        .map_err(|e| format!("consolidation worker database role connection failed: {e}"))?;

    loop {
        let report = dispatch_pass(
            &pool,
            |tenant_id, run_id| {
                UdsInferenceClient::new(
                    &pool,
                    socket_path.clone(),
                    tenant_id,
                    call_ttl,
                    dial_timeout,
                    run_id,
                )
            },
            &config,
        )
        .await;
        match report {
            Ok(report) => println!(
                "humaux-consolidation-worker: dispatch pass claimed={} published={} no_output={} stale={} not_ready={} deferred={} dead={} lost_lease={}",
                report.claimed,
                report.published,
                report.no_output,
                report.stale_input,
                report.not_ready,
                report.deferred,
                report.dead,
                report.lost_lease
            ),
            // Resident mode: one failed pass (a DB blip) is logged and retried on the next poll;
            // `--run-once` surfaces it as the exit status.
            Err(error) if poll_interval.is_some() => {
                eprintln!("humaux-consolidation-worker: dispatch pass failed: {error}")
            }
            Err(error) => return Err(format!("dispatch pass failed: {error}")),
        }
        let Some(interval) = poll_interval else {
            return Ok(());
        };
        tokio::time::sleep(interval).await;
    }
}
