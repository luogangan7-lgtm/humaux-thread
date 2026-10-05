//! `adapters::maintenance_repo` — the resident maintenance daemon's own SQL (ADR-0062): the cross-tenant tenant-id
//!   page every task walks, three of the four owner purge doors (snapshots, rate buckets, terminal jobs; the
//!   confirm-token door lives with its table in `confirm_token_repo::sweep_expired`), the Q-drain reissue door
//!   (D-N) and the schema-failed distill auto re-drive with its §77 row (D-P). The per-tenant sweeps it drives besides
//!   live with their tables (`stream_repo::sweep_lost`, `quota_repo::reap_expired`,
//!   `provider_budget::reap_expired_provider_budgets`). It also owns the closed D-C task enum and the two D-S counters
//!   (`maintenance_task_runs_total{task,outcome}`, `maintenance_task_rows_total{task}`, one emit each in
//!   [`count_task_call`]), which `humaux-maintenance --serve` renders (ADR-0061 D-C).
//! Depends-on: crates=[serde_json, sqlx]; services=[PostgreSQL(role_maintenance) x=[
//!   control.maintenance_tenant_page, control.purge_idle_rate_buckets, ops.auto_redrive_schema_failed,
//!   ops.purge_expired_selection_snapshots, ops.purge_terminal_jobs, projection.reissue_unsettled_tickets]]; env=[];
//!   modules=[adapters::membership_repo, adapters::postgres, adapters::provisioning, humaux-adapters]
//! Called-by: [maintenance::serve, tests]
//! Invariants: [the page returns tenant ids only, strictly after the cursor in tenant_id order, at most `limit`;
//!   the definer refuses limit <= 0 (22023), so no call can read every tenant at once; every door call is one
//!   transaction holding the tenant GUC and exactly one statement, so the delete and its receipt (or a reissued
//!   ticket, its outbox carrier and its marker; a re-drive and its §77 row) commit together or not at all; every
//!   age is sent as an interval and compared with the DB clock inside the door]
//! Spec: Baseline §4.2; §6.2.1; §6.2.2; §15.2; ADR-0057 D-H; ADR-0062 D-D; ADR-0062 D-E; ADR-0062 D-H;
//!   ADR-0062 D-I; ADR-0062 D-J; ADR-0062 D-N; ADR-0062 D-P; ADR-0062 D-S; §41.2; §77

use std::time::Duration;

use serde_json::json;
use sqlx::Row;
use sqlx::postgres::{PgArguments, Postgres};
use sqlx::query::QueryScalar;
use sqlx::types::Uuid;

use crate::membership_repo::AdminAction;
use crate::postgres::MaintenanceDbPool;
use crate::provisioning::{
    self, AUDIT_RESULT_SUCCESS, ProvisioningError, REDRIVE_RESOURCE, REDRIVE_RISK_TAG,
};

/// §77 action of the daemon's automatic re-drive rows (ADR-0062 D-P); the operator's is `DISTILL_REQUEUE_DEAD`.
const AUTO_REDRIVE_ACTION: &str = "DISTILL_AUTO_REDRIVE";

/// ADR-0062 D-D: up to `limit` tenant ids strictly after `after` (`None` starts a rotation). A page shorter than
/// `limit` ends the rotation.
pub async fn tenant_page(
    pool: &MaintenanceDbPool,
    after: Option<Uuid>,
    limit: i32,
) -> Result<Vec<Uuid>, sqlx::Error> {
    // dep: PostgreSQL(role_maintenance) — control.maintenance_tenant_page (0214 owner definer, EXECUTE role_maintenance only)
    sqlx::query_scalar("SELECT control.maintenance_tenant_page($1, $2)")
        .bind(after)
        .bind(limit)
        .fetch_all(pool.pool())
        .await
}

/// One door call: the tenant GUC, then the door's single statement, then commit (ADR-0062 D-E: the door asserts
/// the GUC and names that tenant in every victim predicate).
async fn in_tenant(
    pool: &MaintenanceDbPool,
    tenant: Uuid,
    door: QueryScalar<'_, Postgres, i64, PgArguments>,
) -> Result<i64, sqlx::Error> {
    // dep: PostgreSQL(role_maintenance) — one transaction per tenant door call
    let mut txn = pool.pool().begin().await?;
    sqlx::query("SELECT set_config('humaux.tenant_id', $1, true)")
        .bind(tenant.to_string())
        .execute(&mut *txn)
        .await?;
    let removed = door.fetch_one(&mut *txn).await?;
    txn.commit().await?;
    Ok(removed)
}

/// ADR-0062 D-H: deletes at most `limit` DB-expired selection snapshots of `tenant` with their items; returns the
/// number of snapshots removed.
pub async fn purge_expired_selection_snapshots(
    pool: &MaintenanceDbPool,
    tenant: Uuid,
    limit: i32,
) -> Result<i64, sqlx::Error> {
    // dep: PostgreSQL(role_maintenance) — ops.purge_expired_selection_snapshots (0218 owner purge door)
    let door = sqlx::query_scalar("SELECT ops.purge_expired_selection_snapshots($1)").bind(limit);
    in_tenant(pool, tenant, door).await
}

/// ADR-0062 D-I: deletes at most `limit` rate buckets of `tenant` idle for longer than `idle` that would be full now
/// (a recreated bucket answers exactly as the deleted one would).
pub async fn purge_idle_rate_buckets(
    pool: &MaintenanceDbPool,
    tenant: Uuid,
    idle: Duration,
    limit: i32,
) -> Result<i64, sqlx::Error> {
    // dep: PostgreSQL(role_maintenance) — control.purge_idle_rate_buckets (0218 owner purge door)
    let door =
        sqlx::query_scalar("SELECT control.purge_idle_rate_buckets(make_interval(secs => $1), $2)")
            .bind(idle.as_secs_f64())
            .bind(limit);
    in_tenant(pool, tenant, door).await
}

/// The three ages the terminal-jobs door compares with the DB clock (ADR-0062 D-J).
#[derive(Debug, Clone, Copy)]
pub struct JobRetention {
    /// A DONE job is kept until its last activity is older than this.
    pub done: Duration,
    /// A DEAD or FAILED job is kept until its last activity is older than this.
    pub dead: Duration,
    /// The private worker's distill budget window: a job with a provider call inside it is always kept.
    pub budget_window: Duration,
}

/// ADR-0062 D-J: deletes at most `limit` terminal jobs of `tenant` past `retention`, keeping budget-window,
/// contribution-linked and R4-redrivable jobs.
pub async fn purge_terminal_jobs(
    pool: &MaintenanceDbPool,
    tenant: Uuid,
    retention: JobRetention,
    limit: i32,
) -> Result<i64, sqlx::Error> {
    // dep: PostgreSQL(role_maintenance) — ops.purge_terminal_jobs (0218 owner purge door)
    let door = sqlx::query_scalar(
        "SELECT ops.purge_terminal_jobs(make_interval(secs => $1), make_interval(secs => $2), \
         make_interval(secs => $3), $4)",
    )
    .bind(retention.done.as_secs_f64())
    .bind(retention.dead.as_secs_f64())
    .bind(retention.budget_window.as_secs_f64())
    .bind(limit);
    in_tenant(pool, tenant, door).await
}

/// ADR-0062 D-N: issues at most `limit` fresh lifecycle tickets for the memories of `tenant` in Q (latest ticket
/// FAILED / LOST / RETIRED_FAILED, none in flight, indexable), one per stream and PRIMARY evidence, on the latest
/// ticket's own stream, once `cooldown` has passed; deterministic failure classes once. Returns the tickets issued.
pub async fn reissue_unsettled_tickets(
    pool: &MaintenanceDbPool,
    tenant: Uuid,
    cooldown: Duration,
    limit: i32,
) -> Result<i64, sqlx::Error> {
    // ponytail: the door repeats 0189's "latest ticket per memory" join instead of sharing a terms function (ADR-0062
    // L9); T-N1 ties the two (a reissue drains Q to 0). Share one definer if a third consumer appears.
    // dep: PostgreSQL(role_maintenance) — projection.reissue_unsettled_tickets (0220 owner definer)
    let door = sqlx::query_scalar(
        "SELECT projection.reissue_unsettled_tickets($1, make_interval(secs => $2), $3)",
    )
    .bind(tenant)
    .bind(cooldown.as_secs_f64())
    .bind(limit);
    in_tenant(pool, tenant, door).await
}

/// ADR-0062 D-P: re-drives at most `limit` DEAD `FAILED_OUTPUT_SCHEMA` distill jobs of `tenant` whose last counted
/// provider call is older than `cooldown`, each at most once ever, through the operator's re-drive door (same output
/// channel); a refused job (`evidence_gone` / `outbox_settled`) is marked and reported, never retried. When the door
/// returned rows, one §77 row in the same transaction names them under `audit`'s fields. Returns the jobs re-armed.
pub async fn auto_redrive_schema_failed(
    pool: &MaintenanceDbPool,
    tenant: Uuid,
    cooldown: Duration,
    limit: i32,
    audit: &AdminAction<'_>,
) -> Result<i64, ProvisioningError> {
    // dep: PostgreSQL(role_maintenance) — one transaction: tenant GUC, the 0221 door, the §77 row
    let mut txn = pool.pool().begin().await?;
    provisioning::set_tenant(&mut txn, tenant).await?;
    // dep: PostgreSQL(role_maintenance) — ops.auto_redrive_schema_failed (0221 owner definer)
    let rows = sqlx::query(
        "SELECT job_id, evidence_id, skipped \
         FROM ops.auto_redrive_schema_failed($1, make_interval(secs => $2), $3)",
    )
    .bind(tenant)
    .bind(cooldown.as_secs_f64())
    .bind(limit)
    .fetch_all(&mut *txn)
    .await?;
    if rows.is_empty() {
        txn.commit().await?;
        return Ok(0);
    }
    let (mut redriven, mut skipped, mut ids) = (Vec::new(), Vec::new(), Vec::new());
    for row in &rows {
        let job_id: Uuid = row.try_get("job_id")?;
        ids.push(job_id.to_string());
        match row.try_get::<Option<String>, _>("skipped")? {
            None => redriven.push(json!({
                "job_id": job_id,
                "evidence_id": row.try_get::<Option<Uuid>, _>("evidence_id")?,
            })),
            Some(reason) => skipped.push(json!({ "job_id": job_id, "reason": reason })),
        }
    }
    let rearmed = i64::try_from(redriven.len()).unwrap_or(i64::MAX);
    // dep: PostgreSQL(role_maintenance) — control.audit_event_insert via provisioning::audit_tagged (§77)
    provisioning::audit_tagged(
        &mut txn,
        tenant,
        (AUTO_REDRIVE_ACTION, REDRIVE_RISK_TAG),
        REDRIVE_RESOURCE,
        &ids.join(","),
        AUDIT_RESULT_SUCCESS,
        audit,
        json!({ "redriven": redriven, "skipped": skipped }),
    )
    .await?;
    txn.commit().await?;
    Ok(rearmed)
}

/// ADR-0062 D-C: the daemon's closed task list, in cycle order; also the `task` label of the D-S counters. Adding one
/// is one variant, its door and its two keys (`T_EVERY_SECONDS`, `T_LIMIT`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceTask {
    /// `stream_repo::sweep_lost` (D-L).
    Lost,
    /// `quota_repo::reap_expired`.
    QuotaReservations,
    /// `provider_budget::reap_expired_provider_budgets`.
    ProviderBudgets,
    /// `confirm_token_repo::sweep_expired` (D-G).
    ConfirmTokens,
    /// [`purge_expired_selection_snapshots`] (D-H).
    Snapshots,
    /// [`purge_idle_rate_buckets`] (D-I).
    RateBuckets,
    /// [`purge_terminal_jobs`] (D-J).
    Jobs,
    /// [`reissue_unsettled_tickets`] (D-N).
    Reissue,
    /// [`auto_redrive_schema_failed`] (D-P).
    Redrive,
}

impl MaintenanceTask {
    /// Every task, in D-C cycle order (the counters' slot order).
    pub const ALL: [Self; 9] = [
        Self::Lost,
        Self::QuotaReservations,
        Self::ProviderBudgets,
        Self::ConfirmTokens,
        Self::Snapshots,
        Self::RateBuckets,
        Self::Jobs,
        Self::Reissue,
        Self::Redrive,
    ];

    /// The `/status`, receipt and §41.2 `task` label value (lowercase D-C name; the frozen value set of §41.2).
    pub const fn label(self) -> &'static str {
        match self {
            Self::Lost => "lost",
            Self::QuotaReservations => "quota_reservations",
            Self::ProviderBudgets => "provider_budgets",
            Self::ConfirmTokens => "confirm_tokens",
            Self::Snapshots => "snapshots",
            Self::RateBuckets => "rate_buckets",
            Self::Jobs => "jobs",
            Self::Reissue => "reissue",
            Self::Redrive => "redrive",
        }
    }

    const fn slot(self) -> usize {
        self as usize
    }
}

/// The §41.2 `outcome` label of `maintenance_task_runs_total` (closed set `ok | failed`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskOutcome {
    /// The door call committed (whatever it affected).
    Ok,
    /// The call returned an error or timed out (the cycle's 503 reason).
    Failed,
}

impl TaskOutcome {
    /// Both outcomes, in render order.
    pub const ALL: [Self; 2] = [Self::Ok, Self::Failed];

    /// The label value.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Failed => "failed",
        }
    }
}

/// One process-local counter per closed label tuple (ADR-0061 D-A: seeded at 0, no metrics crate).
struct TaskCounters<const N: usize>([crate::Counter; N]);

impl<const N: usize> TaskCounters<N> {
    const fn new() -> Self {
        Self([const { crate::Counter::new() }; N])
    }

    fn inc(&self, slot: usize, by: u64) {
        self.0[slot].inc(by);
    }
}

const TASKS: usize = MaintenanceTask::ALL.len();
// §41.2 rows `maintenance_task_runs_total{task,outcome}` / `maintenance_task_rows_total{task}` (§4.2 每次门调用收尾 ·
// 各 1); consumers §39 stage liveness, §42 MaintenanceTaskFailing, §53 growth. Rendered by `humaux-maintenance
// --serve` only (ADR-0061 D-C, ADR-0062 D-S). Runs slot = task * 2 + outcome.
static MAINTENANCE_TASK_RUNS_TOTAL: TaskCounters<{ TASKS * 2 }> = TaskCounters::new();
static MAINTENANCE_TASK_ROWS_TOTAL: TaskCounters<TASKS> = TaskCounters::new();

/// ADR-0062 D-S: counts one finished door call of `task` (one tenant): `affected` is `Some(rows)` when it
/// committed, `None` when it failed. Public so the G80-6 witnesses drive the emit without a database; the one
/// production caller is `maintenance::serve`'s cycle, once per tenant call.
pub fn count_task_call(task: MaintenanceTask, affected: Option<u64>) {
    let outcome = if affected.is_some() {
        TaskOutcome::Ok
    } else {
        TaskOutcome::Failed
    };
    // labels: task,outcome
    MAINTENANCE_TASK_RUNS_TOTAL.inc(task.slot() * 2 + outcome as usize, 1);
    // labels: task
    MAINTENANCE_TASK_ROWS_TOTAL.inc(task.slot(), affected.unwrap_or(0));
}

/// This process's `maintenance_task_runs_total{task,outcome}`.
pub fn maintenance_task_runs_total(task: MaintenanceTask, outcome: TaskOutcome) -> u64 {
    MAINTENANCE_TASK_RUNS_TOTAL.0[task.slot() * 2 + outcome as usize].get()
}

/// This process's `maintenance_task_rows_total{task}`.
pub fn maintenance_task_rows_total(task: MaintenanceTask) -> u64 {
    MAINTENANCE_TASK_ROWS_TOTAL.0[task.slot()].get()
}
