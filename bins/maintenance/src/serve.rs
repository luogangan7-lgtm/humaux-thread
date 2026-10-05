//! `maintenance::serve` — `humaux-maintenance --serve`, the resident maintenance daemon (ADR-0062): every cycle
//!   runs each due task over one tenant page, one transaction and one door call per tenant, and serves its
//!   readiness on its own loopback ops listener (the eighth `(job, mode)` pair, ADR-0061 D-B); and `sweep once`,
//!   the same tasks for one page with cadence ignored (D-Q).
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-telemetry, serde_json, tokio, uuid];
//!   services=[PostgreSQL(role_maintenance)]; env=[CARGO_PKG_VERSION, HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS,
//!   HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_MAINTENANCE_SERVE_CONFIRM_TOKENS_CONSUMED_RETENTION_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_CONFIRM_TOKENS_EVERY_SECONDS, HUMAUX_MAINTENANCE_SERVE_CONFIRM_TOKENS_LIMIT,
//!   HUMAUX_MAINTENANCE_SERVE_CYCLE_SECONDS, HUMAUX_MAINTENANCE_SERVE_JOBS_DEAD_RETENTION_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_JOBS_DONE_RETENTION_SECONDS, HUMAUX_MAINTENANCE_SERVE_JOBS_EVERY_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_JOBS_LIMIT, HUMAUX_MAINTENANCE_SERVE_LOST_AFTER_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_LOST_EVERY_SECONDS, HUMAUX_MAINTENANCE_SERVE_LOST_LIMIT,
//!   HUMAUX_MAINTENANCE_SERVE_METRICS_ADDR, HUMAUX_MAINTENANCE_SERVE_PROVIDER_BUDGETS_EVERY_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_PROVIDER_BUDGETS_LIMIT, HUMAUX_MAINTENANCE_SERVE_QUOTA_RESERVATIONS_EVERY_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_QUOTA_RESERVATIONS_LIMIT, HUMAUX_MAINTENANCE_SERVE_RATE_BUCKETS_EVERY_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_RATE_BUCKETS_IDLE_SECONDS, HUMAUX_MAINTENANCE_SERVE_RATE_BUCKETS_LIMIT,
//!   HUMAUX_MAINTENANCE_SERVE_REDRIVE_COOLDOWN_SECONDS, HUMAUX_MAINTENANCE_SERVE_REDRIVE_EVERY_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_REDRIVE_LIMIT, HUMAUX_MAINTENANCE_SERVE_REISSUE_COOLDOWN_SECONDS, HUMAUX_MAINTENANCE_SERVE_REISSUE_EVERY_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_REISSUE_LIMIT, HUMAUX_MAINTENANCE_SERVE_SNAPSHOTS_EVERY_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_SNAPSHOTS_LIMIT,
//!   HUMAUX_MAINTENANCE_SERVE_TENANTS_PER_RUN, HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS];
//!   modules=[adapters::confirm_token_repo, adapters::maintenance_repo, adapters::membership_repo, adapters::postgres,
//!   adapters::provider_budget, adapters::quota_repo, adapters::stream_repo, domain::ids, telemetry::metrics,
//!   maintenance::main, maintenance::resident]
//! Called-by: [maintenance::main]
//! Invariants: [every key is required with no code default and checked before any connection; LOST_AFTER must
//!   exceed the gateway's projection-lag key; only the first connect is boot-fatal, every later failure is logged,
//!   kept for /status and answered 503 while the loop goes on; every call is bounded by CYCLE_SECONDS on the server
//!   and the client; the signal latch is checked between calls, so SIGTERM never waits out a cycle; it never
//!   samples health and never holds a cross-tenant write path; every purge is one owner door statement with its
//!   receipt, never a DELETE of its own; a reissue is one owner door call per tenant (ticket, carrier and marker in
//!   one transaction); a re-drive is one owner door call per tenant with its §77 row in the same transaction, under
//!   the daemon's system identity (`--serve`) or the operator's fields (`sweep once`); `sweep once` requires the §77
//!   fields like every writing subcommand]
//! Spec: Baseline §4.2; §6.2.1; §15.2; §41.2; §77; §78.1; ADR-0037; ADR-0043; ADR-0057 D-F; ADR-0057 D-H;
//!   ADR-0061 D-B; ADR-0062 D-A..D-D; ADR-0062 D-E..D-J; ADR-0062 D-L; ADR-0062 D-N; ADR-0062 D-P; ADR-0062 D-Q
//!
//! Metrics: `maintenance_task_runs_total{task,outcome}` and `maintenance_task_rows_total{task}` (ADR-0062 D-S),
//!   counted once per tenant door call through `adapters::maintenance_repo::count_task_call`, served by `/metrics`
//!   only while ready and printed at zero by `--serve --metrics-families`; `/status` carries the last finished cycle
//!   per task.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use humaux_adapters::maintenance_repo::{
    self, JobRetention, MaintenanceTask as Task, TaskOutcome, count_task_call,
};
use humaux_adapters::membership_repo::AdminAction;
use humaux_adapters::postgres::MaintenanceDbPool;
use humaux_adapters::{confirm_token_repo, provider_budget, quota_repo, stream_repo};
use humaux_domain::ids::TenantId;
use humaux_telemetry::metrics::{Routes, families, write_family, write_single_label};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::resident::{self, Last, Latch, with_statement_timeout};
use crate::{Admin, Args, Failure, Output, Result, env, env_parsed};

const METRICS_ADDR: &str = "HUMAUX_MAINTENANCE_SERVE_METRICS_ADDR";
/// §78.1 / ADR-0062 D-B: the tick, the per-statement timeout and the cycle budget (seconds, > 0).
const CYCLE_SECONDS: &str = "HUMAUX_MAINTENANCE_SERVE_CYCLE_SECONDS";
/// ADR-0062 D-D: tenants per task run (one `control.maintenance_tenant_page` page).
const TENANTS_PER_RUN: &str = "HUMAUX_MAINTENANCE_SERVE_TENANTS_PER_RUN";
/// §15.2 orphan threshold (seconds, > 0).
const LOST_AFTER_SECONDS: &str = "HUMAUX_MAINTENANCE_SERVE_LOST_AFTER_SECONDS";
/// ADR-0057 D-F peer key, read under the gateway's own name: one value, two readers.
const PROJECTION_LAG_SECONDS: &str = "HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS";
const PG_DSN: &str = "HUMAUX_MAINTENANCE_PG_DSN";
/// ADR-0062 D-G: seconds a consumed, expired confirm token is kept as audit (>= 0).
const CONFIRM_TOKENS_CONSUMED_RETENTION_SECONDS: &str =
    "HUMAUX_MAINTENANCE_SERVE_CONFIRM_TOKENS_CONSUMED_RETENTION_SECONDS";
/// ADR-0062 D-I: seconds a full rate bucket must be idle before it is purged (>= 0).
const RATE_BUCKETS_IDLE_SECONDS: &str = "HUMAUX_MAINTENANCE_SERVE_RATE_BUCKETS_IDLE_SECONDS";
/// ADR-0062 D-J: seconds a DONE job is kept after its last activity (>= 0).
const JOBS_DONE_RETENTION_SECONDS: &str = "HUMAUX_MAINTENANCE_SERVE_JOBS_DONE_RETENTION_SECONDS";
/// ADR-0062 D-J: seconds a DEAD or FAILED job is kept after its last activity (>= 0).
const JOBS_DEAD_RETENTION_SECONDS: &str = "HUMAUX_MAINTENANCE_SERVE_JOBS_DEAD_RETENTION_SECONDS";
/// ADR-0062 D-B / D-J peer key, read under the private worker's own name: the jobs door keeps every job with a
/// provider call inside the window `ops.admit_distill_budget` counts.
const DISTILL_BUDGET_WINDOW_SECS: &str = "HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS";
/// ADR-0062 D-N: seconds a ticket's latest timestamp must lie in the past before its memory is reissued (> 0).
const REISSUE_COOLDOWN_SECONDS: &str = "HUMAUX_MAINTENANCE_SERVE_REISSUE_COOLDOWN_SECONDS";
/// ADR-0062 D-P: seconds since a schema-failed death's last counted provider call before it is re-driven (> 0).
const REDRIVE_COOLDOWN_SECONDS: &str = "HUMAUX_MAINTENANCE_SERVE_REDRIVE_COOLDOWN_SECONDS";
/// ADR-0062 D-P: the §77 identity of a re-drive the resident daemon makes on its own (`sweep once` takes the
/// operator's fields instead). No operator, ticket or step-up exists for a scheduled task; the trace id is minted per
/// process at boot.
const SYSTEM_ACTOR: &str = "system:humaux-maintenance --serve";
const SYSTEM_REASON: &str = "ADR-0062 D-P";
const SYSTEM_STEP_UP: &str = "none: scheduled task (ADR-0062 D-P)";

/// ADR-0062 D-C: a task's seconds between runs (>= CYCLE_SECONDS). Literal per key so env_vars.md lists each one.
const fn every_key(task: Task) -> &'static str {
    match task {
        Task::Lost => "HUMAUX_MAINTENANCE_SERVE_LOST_EVERY_SECONDS",
        Task::QuotaReservations => "HUMAUX_MAINTENANCE_SERVE_QUOTA_RESERVATIONS_EVERY_SECONDS",
        Task::ProviderBudgets => "HUMAUX_MAINTENANCE_SERVE_PROVIDER_BUDGETS_EVERY_SECONDS",
        Task::ConfirmTokens => "HUMAUX_MAINTENANCE_SERVE_CONFIRM_TOKENS_EVERY_SECONDS",
        Task::Snapshots => "HUMAUX_MAINTENANCE_SERVE_SNAPSHOTS_EVERY_SECONDS",
        Task::RateBuckets => "HUMAUX_MAINTENANCE_SERVE_RATE_BUCKETS_EVERY_SECONDS",
        Task::Jobs => "HUMAUX_MAINTENANCE_SERVE_JOBS_EVERY_SECONDS",
        Task::Reissue => "HUMAUX_MAINTENANCE_SERVE_REISSUE_EVERY_SECONDS",
        Task::Redrive => "HUMAUX_MAINTENANCE_SERVE_REDRIVE_EVERY_SECONDS",
    }
}

/// ADR-0062 D-C: the LIMIT of a task's per-tenant statement (> 0).
const fn limit_key(task: Task) -> &'static str {
    match task {
        Task::Lost => "HUMAUX_MAINTENANCE_SERVE_LOST_LIMIT",
        Task::QuotaReservations => "HUMAUX_MAINTENANCE_SERVE_QUOTA_RESERVATIONS_LIMIT",
        Task::ProviderBudgets => "HUMAUX_MAINTENANCE_SERVE_PROVIDER_BUDGETS_LIMIT",
        Task::ConfirmTokens => "HUMAUX_MAINTENANCE_SERVE_CONFIRM_TOKENS_LIMIT",
        Task::Snapshots => "HUMAUX_MAINTENANCE_SERVE_SNAPSHOTS_LIMIT",
        Task::RateBuckets => "HUMAUX_MAINTENANCE_SERVE_RATE_BUCKETS_LIMIT",
        Task::Jobs => "HUMAUX_MAINTENANCE_SERVE_JOBS_LIMIT",
        Task::Reissue => "HUMAUX_MAINTENANCE_SERVE_REISSUE_LIMIT",
        Task::Redrive => "HUMAUX_MAINTENANCE_SERVE_REDRIVE_LIMIT",
    }
}

/// Which resident shape reads the keys: `sweep once` ignores cadence, so it reads no `*_EVERY_SECONDS`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Serve,
    Once,
}

struct Config {
    cycle: Duration,
    tenants_per_run: i32,
    lost_after: Duration,
    /// ADR-0062 D-G: how long a consumed, expired confirm token stays as audit.
    confirm_retention: Duration,
    /// ADR-0062 D-I: how long a full bucket must have been idle before it goes.
    bucket_idle: Duration,
    jobs: JobRetention,
    /// ADR-0062 D-N: how long a ticket must have been quiet before its memory is reissued.
    reissue_cooldown: Duration,
    /// ADR-0062 D-P: how long after its last counted provider call a schema-failed death is re-driven.
    redrive_cooldown: Duration,
    /// Per [`Task::ALL`] index: (every, limit); every is zero under [`Mode::Once`].
    tasks: [(Duration, i32); Task::ALL.len()],
}

fn positive<T: std::str::FromStr + PartialOrd + Default>(key: &str) -> Result<T> {
    let value: T = env_parsed(key)?;
    if value <= T::default() {
        return Err(Failure::Usage(format!("{key} must be > 0")));
    }
    Ok(value)
}

fn seconds(key: &str) -> Result<Duration> {
    Ok(Duration::from_secs(env_parsed(key)?))
}

impl Config {
    /// Every key, in a fixed order, before any connection (§78.1: a missing key is named, never defaulted).
    fn read(mode: Mode) -> Result<Self> {
        let cycle = Duration::from_secs(positive(CYCLE_SECONDS)?);
        let tenants_per_run = positive(TENANTS_PER_RUN)?;
        let mut tasks = [(Duration::ZERO, 0); Task::ALL.len()];
        for (slot, task) in tasks.iter_mut().zip(Task::ALL) {
            let every = match mode {
                Mode::Serve => seconds(every_key(task))?,
                Mode::Once => Duration::ZERO,
            };
            if mode == Mode::Serve && every < cycle {
                return Err(Failure::Usage(format!(
                    "{} must be >= {CYCLE_SECONDS}",
                    every_key(task)
                )));
            }
            *slot = (every, positive(limit_key(task))?);
        }
        let lost_after: u64 = positive(LOST_AFTER_SECONDS)?;
        let lag: u64 = env_parsed(PROJECTION_LAG_SECONDS)?;
        // ADR-0057 D-F: a stalled ticket must read as projection lag before the sweep turns it LOST.
        if lost_after <= lag {
            return Err(Failure::Usage(format!(
                "{LOST_AFTER_SECONDS} ({lost_after}) must be greater than {PROJECTION_LAG_SECONDS} ({lag})"
            )));
        }
        Ok(Self {
            cycle,
            tenants_per_run,
            lost_after: Duration::from_secs(lost_after),
            confirm_retention: seconds(CONFIRM_TOKENS_CONSUMED_RETENTION_SECONDS)?,
            bucket_idle: seconds(RATE_BUCKETS_IDLE_SECONDS)?,
            jobs: JobRetention {
                done: seconds(JOBS_DONE_RETENTION_SECONDS)?,
                dead: seconds(JOBS_DEAD_RETENTION_SECONDS)?,
                budget_window: Duration::from_secs(positive(DISTILL_BUDGET_WINDOW_SECS)?),
            },
            reissue_cooldown: Duration::from_secs(positive(REISSUE_COOLDOWN_SECONDS)?),
            redrive_cooldown: Duration::from_secs(positive(REDRIVE_COOLDOWN_SECONDS)?),
            tasks,
        })
    }
}

/// One task's share of one cycle.
#[derive(Default)]
struct Run {
    ran: bool,
    tenants: u64,
    affected: u64,
    failed: u64,
    first_error: Option<String>,
}

impl Run {
    fn fail(&mut self, error: String) {
        self.failed += 1;
        self.first_error.get_or_insert(error);
    }
}

/// In-memory rotation state (ADR-0062 L13: a restart begins a new rotation and runs every task once).
#[derive(Default, Clone, Copy)]
struct Cursor {
    after: Option<Uuid>,
    last_run: Option<Instant>,
}

/// What `/status` shows and the readiness verdict reads.
struct Shared {
    last: Mutex<Last>,
    cycles: Mutex<(u64, bool, Value)>,
}

/// One door call for one tenant: exactly one transaction with one statement (the callee's).
async fn call(
    task: Task,
    pool: &MaintenanceDbPool,
    tenant: Uuid,
    limit: i32,
    cfg: &Config,
    audit: &AdminAction<'_>,
) -> std::result::Result<u64, String> {
    // The system tenant (nil id, home of the pre-auth rate buckets) can hold no reservation: both producers refuse
    // it (quota_repo::validate_auth, provider_budget::validate_request), and so do both reapers: nothing to call.
    if tenant.is_nil() && matches!(task, Task::QuotaReservations | Task::ProviderBudgets) {
        return Ok(0);
    }
    match task {
        // dep: PostgreSQL(role_maintenance) — projection.stream_log ISSUED -> LOST (adapters::stream_repo::sweep_lost)
        Task::Lost => stream_repo::sweep_lost(pool, tenant, cfg.lost_after, i64::from(limit))
            .await
            .map_err(|e| e.to_string()),
        // dep: PostgreSQL(role_maintenance) — control.reap_quota_reservations (0113 owner definer)
        Task::QuotaReservations => quota_repo::reap_expired(pool, TenantId(tenant), limit)
            .await
            .map(|n| n.unsigned_abs())
            .map_err(|e| e.to_string()),
        // dep: PostgreSQL(role_maintenance) — ops.reap_expired_retrieval_provider_budget (0117 owner definer)
        Task::ProviderBudgets => {
            provider_budget::reap_expired_provider_budgets(pool, TenantId(tenant), limit)
                .await
                .map(|n| u64::from(n.unsigned_abs()))
                .map_err(|e| e.to_string())
        }
        // dep: PostgreSQL(role_maintenance) — control.sweep_confirm_tokens (0218 owner purge door)
        Task::ConfirmTokens => {
            confirm_token_repo::sweep_expired(pool, tenant, cfg.confirm_retention, limit)
                .await
                .map(i64::unsigned_abs)
                .map_err(|e| e.to_string())
        }
        // dep: PostgreSQL(role_maintenance) — ops.purge_expired_selection_snapshots (0218 owner purge door)
        Task::Snapshots => maintenance_repo::purge_expired_selection_snapshots(pool, tenant, limit)
            .await
            .map(i64::unsigned_abs)
            .map_err(|e| e.to_string()),
        // dep: PostgreSQL(role_maintenance) — control.purge_idle_rate_buckets (0218 owner purge door)
        Task::RateBuckets => {
            maintenance_repo::purge_idle_rate_buckets(pool, tenant, cfg.bucket_idle, limit)
                .await
                .map(i64::unsigned_abs)
                .map_err(|e| e.to_string())
        }
        // dep: PostgreSQL(role_maintenance) — ops.purge_terminal_jobs (0218 owner purge door)
        Task::Jobs => maintenance_repo::purge_terminal_jobs(pool, tenant, cfg.jobs, limit)
            .await
            .map(i64::unsigned_abs)
            .map_err(|e| e.to_string()),
        // dep: PostgreSQL(role_maintenance) — projection.reissue_unsettled_tickets (0220 owner definer)
        Task::Reissue => {
            maintenance_repo::reissue_unsettled_tickets(pool, tenant, cfg.reissue_cooldown, limit)
                .await
                .map(i64::unsigned_abs)
                .map_err(|e| e.to_string())
        }
        // dep: PostgreSQL(role_maintenance) — ops.auto_redrive_schema_failed (0221 owner definer) + §77 row
        Task::Redrive => maintenance_repo::auto_redrive_schema_failed(
            pool,
            tenant,
            cfg.redrive_cooldown,
            limit,
            audit,
        )
        .await
        .map(i64::unsigned_abs)
        .map_err(|e| e.to_string()),
    }
}

/// ADR-0062 D-B: one cycle. Due tasks run from `first` in D-C order; each processes one tenant page; calls stop
/// once the cycle budget is spent or the latch is set, leaving the unvisited tenants under the cursor.
async fn cycle(
    pool: &MaintenanceDbPool,
    cfg: &Config,
    cursors: &mut [Cursor; Task::ALL.len()],
    first: usize,
    latch: &Latch,
    audit: &AdminAction<'_>,
) -> Vec<(Task, Run)> {
    let started = Instant::now();
    let spent = || latch.is_set() || started.elapsed() >= cfg.cycle;
    let mut runs = Vec::with_capacity(Task::ALL.len());
    for offset in 0..Task::ALL.len() {
        let index = (first + offset) % Task::ALL.len();
        let (task, (every, limit)) = (Task::ALL[index], cfg.tasks[index]);
        let cursor = &mut cursors[index];
        let mut run = Run::default();
        if spent() || cursor.last_run.is_some_and(|at| at.elapsed() < every) {
            runs.push((task, run));
            continue;
        }
        run.ran = true;
        cursor.last_run = Some(Instant::now());
        // ponytail: per-task tenant page rotation, latency = ceil(tenants / TENANTS_PER_RUN) x EVERY; a "tenants
        // with work" definer per task if rotation latency matters (ADR-0062 L1).
        let page = tokio::time::timeout(
            cfg.cycle,
            maintenance_repo::tenant_page(pool, cursor.after, cfg.tenants_per_run),
        )
        .await;
        let page = match page {
            Ok(Ok(page)) => page,
            failed => {
                // ADR-0062 D-S: a run whose page cannot be read counts as one failed call, so an unreachable
                // database shows on the counter, not only as 503.
                count_task_call(task, None);
                run.fail(match failed {
                    Ok(Err(e)) => format!("tenant page: {e}"),
                    _ => format!("the tenant page did not answer within {CYCLE_SECONDS}"),
                });
                runs.push((task, run));
                continue;
            }
        };
        let mut finished = true;
        for &tenant in &page {
            if spent() {
                finished = false;
                break;
            }
            let done =
                match tokio::time::timeout(cfg.cycle, call(task, pool, tenant, limit, cfg, audit))
                    .await
                {
                    Ok(Ok(n)) => Ok(n),
                    Ok(Err(e)) => Err(format!("tenant {tenant}: {e}")),
                    Err(_) => Err(format!("tenant {tenant}: no answer within {CYCLE_SECONDS}")),
                };
            // ADR-0062 D-S: one count per tenant door call, after it committed or failed.
            count_task_call(task, done.as_ref().ok().copied());
            match done {
                Ok(n) => run.affected += n,
                Err(e) => run.fail(e),
            }
            run.tenants += 1;
            cursor.after = Some(tenant);
        }
        if finished && page.len() < usize::try_from(cfg.tenants_per_run).unwrap_or(usize::MAX) {
            cursor.after = None;
        }
        runs.push((task, run));
    }
    runs
}

fn report(runs: &[(Task, Run)]) -> Value {
    Value::Array(
        runs.iter()
            .map(|(task, run)| {
                json!({
                    "task": task.label(),
                    "ran": run.ran,
                    "tenants": run.tenants,
                    "affected": run.affected,
                    "failed": run.failed,
                    "error": run.first_error,
                })
            })
            .collect(),
    )
}

/// ADR-0062 D-S: the two §41.2 families this mode exports, every closed `task` x `outcome` value seeded (ADR-0061
/// D-A). `/metrics` and `--serve --metrics-families` both call this.
pub(crate) fn render_metrics() -> String {
    let mut out = String::new();
    let runs: Vec<(Task, TaskOutcome)> = Task::ALL
        .iter()
        .flat_map(|&t| TaskOutcome::ALL.map(|o| (t, o)))
        .collect();
    let labels: Vec<[&'static str; 2]> = runs.iter().map(|(t, o)| [t.label(), o.label()]).collect();
    let samples: Vec<(&[&'static str], f64)> = runs
        .iter()
        .zip(&labels)
        .map(|(&(t, o), l)| {
            (
                &l[..],
                maintenance_repo::maintenance_task_runs_total(t, o) as f64,
            )
        })
        .collect();
    write_family(&mut out, &families::MAINTENANCE_TASK_RUNS_TOTAL, &samples);
    let names = Task::ALL.map(Task::label);
    write_single_label(
        &mut out,
        &families::MAINTENANCE_TASK_ROWS_TOTAL,
        &names,
        |i| maintenance_repo::maintenance_task_rows_total(Task::ALL[i]) as f64,
    );
    out
}

/// ADR-0062 D-A: 503 until the first cycle finished clean, when the last finished cycle had a failed call, or when
/// no clean cycle finished within 3 x CYCLE; never 200 with stale counters.
fn ready(shared: &Shared, cycle: Duration) -> std::result::Result<String, String> {
    let bound = format!("3 x {CYCLE_SECONDS}");
    resident::verdict(
        &shared.last,
        "maintenance cycle",
        3 * cycle,
        &bound,
        render_metrics,
    )
}

fn status(shared: &Shared, cycle: Duration, started: SystemTime) -> String {
    let (cycles, in_cycle, last_cycle) = shared
        .cycles
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    let ready = ready(shared, cycle).err();
    // ADR-0061 D-B: the identity document every resident mode serves, then this mode's cycle state.
    let mut doc = resident::status_document("--serve", started);
    doc.insert("cycles".into(), json!(cycles));
    doc.insert("in_cycle".into(), json!(in_cycle));
    doc.insert("last_cycle".into(), last_cycle);
    doc.insert(
        "readiness".into(),
        ready.map_or_else(
            || json!({"state": "pass"}),
            |e| json!({"state": "fail", "error": e.trim_end()}),
        ),
    );
    Value::Object(doc).to_string()
}

/// `--serve`: runs cycles until SIGTERM / SIGINT, serving readiness and the last cycle on the ops listener.
pub(crate) async fn serve() -> Result<Output> {
    let started = SystemTime::now();
    let addr = resident::ops_addr(METRICS_ADDR)?;
    let cfg = Config::read(Mode::Serve)?;
    let dsn = env(PG_DSN)?;
    let mut latch = Latch::install()?;
    // dep: PostgreSQL(role_maintenance) — the daemon's pool; every statement bounded by CYCLE_SECONDS
    let pool = MaintenanceDbPool::connect(&with_statement_timeout(&dsn, cfg.cycle))
        .await
        .map_err(|e| Failure::Infra(format!("connect {PG_DSN}: {e}")))?;
    let shared = Arc::new(Shared {
        last: Last::pending(),
        cycles: Mutex::new((0, false, Value::Null)),
    });
    let (for_metrics, for_status, cycle_len) =
        (Arc::clone(&shared), Arc::clone(&shared), cfg.cycle);
    let routes = Routes {
        metrics: Box::new(move || ready(&for_metrics, cycle_len)),
        status: Box::new(move || Ok(status(&for_status, cycle_len, started))),
    };
    let listener = resident::listen(METRICS_ADDR, addr, routes)?;
    eprintln!(
        "humaux-maintenance: --serve on {} every {} s",
        listener.local_addr(),
        cfg.cycle.as_secs()
    );
    let trace_id = Uuid::now_v7().to_string();
    let audit = AdminAction {
        actor: SYSTEM_ACTOR,
        reason: SYSTEM_REASON,
        ticket: SYSTEM_REASON,
        trace_id: &trace_id,
        step_up_auth_context: SYSTEM_STEP_UP,
    };
    let mut cursors = [Cursor::default(); Task::ALL.len()];
    let mut ticker = tokio::time::interval(cfg.cycle);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut cycles: u64 = 0;
    loop {
        tokio::select! {
            _ = latch.wait() => break,
            _ = ticker.tick() => {}
        }
        shared
            .cycles
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .1 = true;
        let first = usize::try_from(cycles % Task::ALL.len() as u64).unwrap_or(0);
        let runs = cycle(&pool, &cfg, &mut cursors, first, &latch, &audit).await;
        cycles += 1;
        let failure = runs
            .iter()
            .filter(|(_, r)| r.failed > 0)
            .map(|(task, run)| {
                format!(
                    "{}: {} failed call(s), first: {}",
                    task.label(),
                    run.failed,
                    run.first_error.as_deref().unwrap_or("")
                )
            });
        let failure: Vec<String> = failure.collect();
        if !failure.is_empty() {
            eprintln!("humaux-maintenance: cycle {cycles}: {}", failure.join("; "));
        }
        *shared.cycles.lock().unwrap_or_else(PoisonError::into_inner) =
            (cycles, false, report(&runs));
        let mut last = shared.last.lock().unwrap_or_else(PoisonError::into_inner);
        if failure.is_empty() {
            *last = Last {
                ok_at: Some(Instant::now()),
                error: None,
            };
        } else {
            last.error = Some(failure.join("; "));
        }
        drop(last);
        if latch.is_set() {
            break;
        }
    }
    drop(listener);
    Ok(Output::ok(json!({
        "command": "--serve",
        "outcome": "stopped",
        "cycles": cycles,
    })))
}

/// ADR-0062 D-Q: `sweep once`, the manual override and test seam. Every task runs over one tenant page from the
/// start of the rotation, cadence ignored, under the same cycle budget; one receipt with the per-task counts. A
/// failed call makes the run exit 1 with the receipt still printed.
pub(crate) async fn sweep_once(args: &Args) -> Result<Output> {
    // §77: every writing subcommand names its operator; the REDRIVE audit row (ADR-0062 D-P) takes these fields.
    let admin = Admin::from(args)?;
    let cfg = Config::read(Mode::Once)?;
    let dsn = env(PG_DSN)?;
    let latch = Latch::install()?;
    // dep: PostgreSQL(role_maintenance) — the one-shot pool; every statement bounded by CYCLE_SECONDS
    let pool = MaintenanceDbPool::connect(&with_statement_timeout(&dsn, cfg.cycle))
        .await
        .map_err(|e| Failure::Infra(format!("connect {PG_DSN}: {e}")))?;
    let mut cursors = [Cursor::default(); Task::ALL.len()];
    let runs = cycle(&pool, &cfg, &mut cursors, 0, &latch, &admin.action()).await;
    let failed = runs.iter().any(|(_, run)| run.failed > 0);
    let mut output = Output::ok(json!({
        "command": "sweep once",
        "outcome": if failed { "failed" } else { "swept" },
        "tasks": report(&runs),
    }));
    output.failed = failed;
    Ok(output)
}
