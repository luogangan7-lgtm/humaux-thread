//! `adapters::maintenance_repo` — the resident maintenance daemon's own SQL (ADR-0062): the cross-tenant tenant-id
//!   page every task walks, three of the four owner purge doors (snapshots, rate buckets, terminal jobs; the
//!   confirm-token door lives with its table in `confirm_token_repo::sweep_expired`), the Q-drain reissue door
//!   (D-N) and the schema-failed distill auto re-drive with its §77 row (D-P). The per-tenant sweeps it drives besides
//!   live with their tables (`stream_repo::sweep_lost`, `quota_repo::reap_expired`,
//!   `provider_budget::reap_expired_provider_budgets`). It also owns the closed D-C task enum and the two D-S counters
//!   (`maintenance_task_runs_total{task,outcome}`, `maintenance_task_rows_total{task}`, one emit each in
//!   [`count_task_call`]), which `humaux-maintenance --serve` renders (ADR-0061 D-C). For §48.1 (ADR-0063) it owns
//!   the closed table_key enum, the daemon's PARTITIONS proposer with the `partition_horizon_months{table}` gauge
//!   (D-K), and the one-shot superuser executor's calls of the owner functions with the COPY export (D-I, D-J); no
//!   drop predicate lives here.
//! Depends-on: crates=[hex, humaux-domain, serde, serde_json, sha2, sqlx, time]; services=[PostgreSQL(role_maintenance)
//!   r=[control.partition_registry, control.retention_policies] w=[control.partition_registry] x=[
//!   control.maintenance_tenant_page, control.purge_idle_rate_buckets, ops.auto_redrive_schema_failed,
//!   ops.purge_expired_selection_snapshots, ops.purge_terminal_jobs, projection.reissue_unsettled_tickets],
//!   PostgreSQL(owner) r=[control.partition_registry, control.retention_policies] x=[control.partition_create_month,
//!   control.partition_drop, control.partition_drop_check, control.partition_drop_statements,
//!   control.retention_policy_approve]]; env=[];
//!   modules=[adapters::membership_repo, adapters::postgres, adapters::provisioning, domain::audit, humaux-adapters]
//! Called-by: [maintenance::retention, maintenance::serve, tests]
//! Invariants: [the page returns tenant ids only, strictly after the cursor in tenant_id order, at most `limit`;
//!   the definer refuses limit <= 0 (22023), so no call can read every tenant at once; every door call is one
//!   transaction holding the tenant GUC and exactly one statement, so the delete and its receipt (or a reissued
//!   ticket, its outbox carrier and its marker; a re-drive and its §77 row) commit together or not at all; every
//!   age is sent as an interval and compared with the DB clock inside the door; the proposer writes only the two
//!   proposal columns and a failed run resets the horizon family; every executor transaction takes lock_timeout
//!   and the HXRETAIN key first, never sets row_security, and commits its effect with its §77 row or not at all;
//!   the executor never selects a leaf, never CASCADEs and never detaches CONCURRENTLY]
//! Spec: Baseline §4.2; §6.2.1; §6.2.2; §15.2; §48.1; ADR-0057 D-H; ADR-0062 D-D; ADR-0062 D-E; ADR-0062 D-H;
//!   ADR-0062 D-I; ADR-0062 D-J; ADR-0062 D-N; ADR-0062 D-P; ADR-0062 D-S; ADR-0063 D-F..D-K; §41.2; §77

use std::path::Path;
use std::time::Duration;

use humaux_domain::audit::SYSTEM_TENANT_ID;
use serde::Serialize;
use serde_json::json;
use sqlx::Row;
use sqlx::postgres::{PgArguments, Postgres};
use sqlx::query::QueryScalar;
use sqlx::types::Uuid;
use time::OffsetDateTime;

use crate::membership_repo::AdminAction;
use crate::postgres::{MaintenanceDbPool, RetentionExecutor};
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
    /// [`propose_partitions`] (ADR-0063 D-K): cluster-level, no tenant page and no LIMIT key; one run = one count.
    Partitions,
}

impl MaintenanceTask {
    /// Every task, in D-C cycle order (the counters' slot order).
    pub const ALL: [Self; 10] = [
        Self::Lost,
        Self::QuotaReservations,
        Self::ProviderBudgets,
        Self::ConfirmTokens,
        Self::Snapshots,
        Self::RateBuckets,
        Self::Jobs,
        Self::Reissue,
        Self::Redrive,
        Self::Partitions,
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
            Self::Partitions => "partitions",
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

/// ADR-0062 D-S: counts one finished door call of `task` (one tenant; one run for the cluster-level
/// [`MaintenanceTask::Partitions`], ADR-0063 D-K): `affected` is `Some(rows)` when it committed, `None` when it
/// failed. Public so the G80-6 witnesses drive the emit without a database; the one production caller is
/// `maintenance::serve`'s cycle, once per tenant call (once per run for PARTITIONS).
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

// ============================================================================
// §48.1 partitions and retention (ADR-0063): the daemon's PARTITIONS proposer and horizon gauge (D-K, role_maintenance)
// and the one-shot superuser executor (D-H..D-J, `humaux-maintenance retention …`). Every drop predicate lives in
// `control.partition_drop_check`; this side calls the owner functions and reports what they return.
// ============================================================================

/// ADR-0063 D-C: the closed `table_key` set of `control.partition_registry`, in render order. `retention_policies`
/// accepts four of them (EVENTS and AUDIT_EVENTS wait for card 37 / a separate approval line, D-G); the contract test
/// `bins/maintenance/tests/retention.rs` reconciles both CHECKs with this enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionTable {
    /// `ops.model_call_ledger` (`called_at`).
    ModelCallLedger,
    /// `ops.stage_runs` (`started_at`).
    StageRuns,
    /// `private.messages` (`created_at`).
    Messages,
    /// `ops.maintenance_receipts` (`ran_at`).
    MaintenanceReceipts,
    /// `private.events` (`recorded_at`).
    Events,
    /// `control.audit_events` (`occurred_at`).
    AuditEvents,
}

impl PartitionTable {
    /// Every key, in the gauge's sample order.
    pub const ALL: [Self; 6] = [
        Self::ModelCallLedger,
        Self::StageRuns,
        Self::Messages,
        Self::MaintenanceReceipts,
        Self::Events,
        Self::AuditEvents,
    ];

    /// The `table_key` value (the DB CHECKs' spelling, SCREAMING_SNAKE).
    pub const fn key(self) -> &'static str {
        match self {
            Self::ModelCallLedger => "MODEL_CALL_LEDGER",
            Self::StageRuns => "STAGE_RUNS",
            Self::Messages => "MESSAGES",
            Self::MaintenanceReceipts => "MAINTENANCE_RECEIPTS",
            Self::Events => "EVENTS",
            Self::AuditEvents => "AUDIT_EVENTS",
        }
    }

    /// The §41.2 `table` label of `partition_horizon_months` (lowercase key).
    pub const fn label(self) -> &'static str {
        match self {
            Self::ModelCallLedger => "model_call_ledger",
            Self::StageRuns => "stage_runs",
            Self::Messages => "messages",
            Self::MaintenanceReceipts => "maintenance_receipts",
            Self::Events => "events",
            Self::AuditEvents => "audit_events",
        }
    }

    /// The variant whose [`Self::key`] is `key`.
    pub fn parse(key: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|t| t.key() == key)
    }
}

/// `partition_horizon_months{table}` (ADR-0063 D-F, D-K): `None` = the family renders no sample (never ran, or the
/// last PARTITIONS run failed); a failed run never leaves the last value standing (card 34 D-A: never stale).
struct HorizonGauge(std::sync::Mutex<Option<[i64; PartitionTable::ALL.len()]>>);

impl HorizonGauge {
    fn slot(&self) -> std::sync::MutexGuard<'_, Option<[i64; PartitionTable::ALL.len()]>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn set(&self, months: [i64; PartitionTable::ALL.len()]) {
        *self.slot() = Some(months);
    }

    fn reset(&self) {
        *self.slot() = None;
    }
}

static PARTITION_HORIZON_MONTHS: HorizonGauge = HorizonGauge(std::sync::Mutex::new(None));

/// ADR-0063 D-K: publishes one successful PARTITIONS run's horizon, one value per [`PartitionTable::ALL`] slot
/// (`-1` for a key without an attached leaf). The family's only `.set()`; public so the G80-6 witness drives it
/// without a database; the one production caller is `maintenance::serve`'s cycle.
pub fn set_partition_horizon_months(months: [i64; PartitionTable::ALL.len()]) {
    // labels: table
    PARTITION_HORIZON_MONTHS.set(months);
}

/// ADR-0063 D-K / D-F: a failed PARTITIONS run removes every series, so `PartitionHorizonAbsent` fires instead of a
/// stale value keeping the horizon alerts quiet.
pub fn reset_partition_horizon_months() {
    PARTITION_HORIZON_MONTHS.reset();
}

/// This process's `partition_horizon_months`, `None` while the family has no sample.
pub fn partition_horizon_months() -> Option<[i64; PartitionTable::ALL.len()]> {
    *PARTITION_HORIZON_MONTHS.slot()
}

/// One successful PARTITIONS run (ADR-0063 D-K).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionsRun {
    /// Registry rows whose proposal columns this run (re)wrote.
    pub proposed: u64,
    /// Whole months from the current UTC month start to the last attached upper bound, minus 1, per
    /// [`PartitionTable::ALL`] slot; `-1` for a key without an attached leaf.
    pub horizon_months: [i64; PartitionTable::ALL.len()],
}

/// ADR-0063 D-K, as role_maintenance in one transaction: (1) marks candidacy only — expired, non-newest attached
/// leaves of each table_key's latest policy once it is effective, re-proposing a row proposed under another revision
/// or before `effective_at` (holds are tenant-blind here under FORCE RLS and belong to the executor); (2) reads the
/// horizon of every closed key.
pub async fn propose_partitions(pool: &MaintenanceDbPool) -> Result<PartitionsRun, sqlx::Error> {
    // dep: PostgreSQL(role_maintenance) — one transaction: the proposal UPDATE and the horizon read
    let mut txn = pool.pool().begin().await?;
    // ADR-0063 D-J step 3 / D-K: the same cutoff and newest-leaf terms as `control.partition_drop_check`, in UTC
    // whatever the session TimeZone; `effective_at <= clock_timestamp()` keeps a future policy unproposed (review
    // finding 7), and `proposed_at < effective_at` re-proposes an early or forged proposal.
    // dep: PostgreSQL(role_maintenance) — control.partition_registry column UPDATE (proposed_at, proposed_policy_revision)
    let proposed = sqlx::query(
        "WITH p AS (SELECT DISTINCT ON (table_key) table_key, policy_revision, retention_months, effective_at \
                      FROM control.retention_policies ORDER BY table_key, policy_revision DESC) \
         UPDATE control.partition_registry r \
            SET proposed_at = clock_timestamp(), proposed_policy_revision = p.policy_revision \
           FROM p \
          WHERE r.table_key = p.table_key AND r.state = 'ATTACHED' \
            AND p.retention_months IS NOT NULL AND p.effective_at <= clock_timestamp() \
            AND r.upper_bound <= ((date_trunc('month', now(), 'UTC') AT TIME ZONE 'UTC') \
                                  - make_interval(months => p.retention_months)) AT TIME ZONE 'UTC' \
            AND EXISTS (SELECT 1 FROM control.partition_registry n \
                         WHERE n.table_key = r.table_key AND n.state = 'ATTACHED' AND n.upper_bound > r.upper_bound) \
            AND (r.proposed_policy_revision IS DISTINCT FROM p.policy_revision OR r.proposed_at < p.effective_at)",
    )
    .execute(&mut *txn)
    .await?
    .rows_affected();
    let keys: Vec<&str> = PartitionTable::ALL.iter().map(|t| t.key()).collect();
    // dep: PostgreSQL(role_maintenance) — control.partition_registry ⋈ pg_inherits: the attached horizon per key
    // ADR-0063 "Registry by name" (0231): a registry row joins its leaf by schema-qualified name, never by a stored OID
    // (a logical restore renumbers relations); catalog reads only, so no schema USAGE is needed.
    let rows = sqlx::query(
        "SELECT k.table_key, \
                coalesce(((extract(year FROM max(r.upper_bound) AT TIME ZONE 'UTC') * 12 \
                           + extract(month FROM max(r.upper_bound) AT TIME ZONE 'UTC')) \
                          - (extract(year FROM now() AT TIME ZONE 'UTC') * 12 \
                             + extract(month FROM now() AT TIME ZONE 'UTC')) - 1)::bigint, -1) \
           FROM unnest($1::text[]) k(table_key) \
           LEFT JOIN (control.partition_registry r \
                      JOIN (pg_inherits i JOIN pg_class l ON l.oid = i.inhrelid \
                            JOIN pg_namespace n ON n.oid = l.relnamespace) \
                        ON format('%I.%I', n.nspname, l.relname) = r.leaf_name) \
                  ON r.table_key = k.table_key AND r.state = 'ATTACHED' \
          GROUP BY 1",
    )
    .bind(&keys)
    .fetch_all(&mut *txn)
    .await?;
    txn.commit().await?;
    let mut horizon_months = [-1; PartitionTable::ALL.len()];
    for row in rows {
        let key: String = row.try_get(0)?;
        let slot = PartitionTable::ALL
            .iter()
            .position(|t| t.key() == key)
            .ok_or_else(|| sqlx::Error::Protocol(format!("unknown table_key {key}")))?;
        horizon_months[slot] = row.try_get(1)?;
    }
    Ok(PartitionsRun {
        proposed,
        horizon_months,
    })
}

/// ADR-0063 D-H: the run-once key every `retention …` transaction takes first, ASCII "HXRETAIN" (the
/// `xtask::migrate` HXMIGRAT style); shared by approve, create-partitions and execute.
pub const RETENTION_ADVISORY_LOCK: i64 = i64::from_be_bytes(*b"HXRETAIN");

/// §77 risk tag of the three executor audit rows (ADR-0063 D-I).
const RETENTION_RISK_TAG: &str = "retention";

/// `to_char` mask of every bound in a receipt: RFC 3339 UTC, whatever the session TimeZone.
const UTC_MASK: &str = "YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"";

type Txn<'a> = sqlx::Transaction<'a, Postgres>;

/// `retention refused: <code>` (SQLSTATE P0001) from the owner functions ⇒ [`ProvisioningError::Refused`] with the
/// bare code; anything else keeps the provisioning mapping.
fn refusal(error: sqlx::Error) -> ProvisioningError {
    if let sqlx::Error::Database(database) = &error
        && let Some(code) = database.message().strip_prefix("retention refused: ")
    {
        return ProvisioningError::Refused(code.to_owned());
    }
    error.into()
}

/// ADR-0063 D-J step 1: `lock_timeout` for this transaction, then the run-once key; `busy` when another executor holds
/// it.
async fn begin_executor(
    exec: &RetentionExecutor,
    lock_timeout: Duration,
) -> Result<Txn<'_>, ProvisioningError> {
    // dep: PostgreSQL(owner) — one executor transaction (superuser principal, never resident)
    let mut txn = exec.pool().begin().await?;
    sqlx::query("SELECT set_config('lock_timeout', $1, true)")
        .bind(format!("{}ms", lock_timeout.as_millis()))
        .execute(&mut *txn)
        .await?;
    let got: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(RETENTION_ADVISORY_LOCK)
        .fetch_one(&mut *txn)
        .await?;
    if !got {
        return Err(ProvisioningError::Refused("busy".to_owned()));
    }
    Ok(txn)
}

/// ADR-0063 D-I: the §77 row of one executor effect, under the system tenant, in the effect's own transaction right
/// before COMMIT. The client never sets `row_security`, so the owner definer's FORCE-RLS insert runs with it on
/// (review finding 5).
async fn audit_effect(
    txn: &mut Txn<'_>,
    action: &str,
    (resource_type, resource_id): (&str, &str),
    admin: &AdminAction<'_>,
    metadata: serde_json::Value,
) -> Result<Uuid, ProvisioningError> {
    provisioning::set_tenant(txn, SYSTEM_TENANT_ID.0).await?;
    // dep: PostgreSQL(owner) — control.audit_event_insert via provisioning::audit_tagged (§77)
    provisioning::audit_tagged(
        txn,
        SYSTEM_TENANT_ID.0,
        (action, RETENTION_RISK_TAG),
        resource_type,
        resource_id,
        AUDIT_RESULT_SUCCESS,
        admin,
        metadata,
    )
    .await
}

/// `retention approve`'s receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ApproveReceipt {
    /// The new policy row.
    pub policy_id: Uuid,
    /// Its revision: the table_key's next one.
    pub policy_revision: i32,
    /// The `RETENTION_POLICY_APPROVED` §77 row.
    pub audit_event_id: Uuid,
}

/// ADR-0063 D-I `retention approve`: the next revision of `table`'s policy (`months: None` = keep forever, which is
/// how a policy is withdrawn) through `control.retention_policy_approve`, with its §77 row in the same transaction.
pub async fn retention_approve(
    exec: &RetentionExecutor,
    table: PartitionTable,
    months: Option<i32>,
    effective_at: OffsetDateTime,
    lock_timeout: Duration,
    admin: &AdminAction<'_>,
) -> Result<ApproveReceipt, ProvisioningError> {
    provisioning::require_admin(admin)?;
    let mut txn = begin_executor(exec, lock_timeout).await?;
    // dep: PostgreSQL(owner) — control.retention_policy_approve (0224 owner definer, no runtime EXECUTE)
    let row = sqlx::query(
        "SELECT policy_id, policy_revision FROM control.retention_policy_approve($1, $2, $3, $4)",
    )
    .bind(table.key())
    .bind(months)
    .bind(effective_at)
    .bind(admin.actor)
    .fetch_one(&mut *txn)
    .await
    .map_err(|e| match &e {
        // ADR-0063 D-G: EVENTS / AUDIT_EVENTS (and any other CHECK) are refused by the table, never by this client.
        sqlx::Error::Database(d) if d.code().as_deref() == Some("23514") => {
            ProvisioningError::Refused(format!("policy_check:{}", d.constraint().unwrap_or("")))
        }
        _ => refusal(e),
    })?;
    let (policy_id, policy_revision): (Uuid, i32) = (row.try_get(0)?, row.try_get(1)?);
    let audit_event_id = audit_effect(
        &mut txn,
        "RETENTION_POLICY_APPROVED",
        ("retention_policies", &policy_id.to_string()),
        admin,
        json!({ "table_key": table.key(), "retention_months": months, "policy_revision": policy_revision,
                "effective_at": effective_at.unix_timestamp() }),
    )
    .await?;
    txn.commit().await?;
    Ok(ApproveReceipt {
        policy_id,
        policy_revision,
        audit_event_id,
    })
}

/// One leaf `retention create-partitions` created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CreatedLeaf {
    /// Its table_key.
    pub table_key: &'static str,
    /// Its UTC month start (RFC 3339).
    pub month_start: String,
}

/// `retention create-partitions`' receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CreatePartitionsReceipt {
    /// The leaves this run created (empty: the horizon was already closed, nothing written, no audit row).
    pub created: Vec<CreatedLeaf>,
    /// The `PARTITIONS_CREATED` §77 row when something was created.
    pub audit_event_id: Option<Uuid>,
}

/// ADR-0063 D-F / D-I `retention create-partitions --months-ahead n`: for every table_key,
/// `control.partition_create_month` until its last attached upper bound reaches the UTC month start + (n + 1); the
/// creator refuses a gap or an overlap itself and answers `exists` for a month already there.
pub async fn create_partitions(
    exec: &RetentionExecutor,
    months_ahead: i32,
    lock_timeout: Duration,
    admin: &AdminAction<'_>,
) -> Result<CreatePartitionsReceipt, ProvisioningError> {
    provisioning::require_admin(admin)?;
    let mut txn = begin_executor(exec, lock_timeout).await?;
    let mut created = Vec::new();
    for table in PartitionTable::ALL {
        loop {
            // dep: PostgreSQL(owner) — control.partition_registry: the next month and whether it is still short
            let row = sqlx::query(
                "SELECT coalesce(max(upper_bound), date_trunc('month', now(), 'UTC')), \
                        coalesce(max(upper_bound) < date_add(date_trunc('month', now(), 'UTC'), \
                                                             make_interval(months => $2 + 1), 'UTC'), true) \
                   FROM control.partition_registry WHERE table_key = $1 AND state = 'ATTACHED'",
            )
            .bind(table.key())
            .bind(months_ahead)
            .fetch_one(&mut *txn)
            .await?;
            let (next, short): (OffsetDateTime, bool) = (row.try_get(0)?, row.try_get(1)?);
            if !short {
                break;
            }
            // dep: PostgreSQL(owner) — control.partition_create_month (0224 owner definer, no runtime EXECUTE)
            let outcome: String =
                sqlx::query_scalar("SELECT control.partition_create_month($1, $2)")
                    .bind(table.key())
                    .bind(next)
                    .fetch_one(&mut *txn)
                    .await
                    .map_err(refusal)?;
            if outcome != "created" {
                return Err(ProvisioningError::Refused(format!(
                    "create_month_{outcome}:{}",
                    table.key()
                )));
            }
            let month_start: String =
                sqlx::query_scalar("SELECT to_char($1::timestamptz AT TIME ZONE 'UTC', $2)")
                    .bind(next)
                    .bind(UTC_MASK)
                    .fetch_one(&mut *txn)
                    .await?;
            created.push(CreatedLeaf {
                table_key: table.key(),
                month_start,
            });
        }
    }
    let audit_event_id = if created.is_empty() {
        None
    } else {
        Some(
            audit_effect(
                &mut txn,
                "PARTITIONS_CREATED",
                (
                    "partition_registry",
                    &format!("months_ahead={months_ahead}"),
                ),
                admin,
                json!({ "created": created }),
            )
            .await?,
        )
    };
    txn.commit().await?;
    Ok(CreatePartitionsReceipt {
        created,
        audit_event_id,
    })
}

/// One `retention execute` request (ADR-0063 D-I): ids are the only selectors; no table or leaf name is accepted.
#[derive(Debug, Clone)]
pub struct DropRequest<'a> {
    /// The approved policy (must be its table_key's latest, effective, not keep-forever revision).
    pub policy_id: Uuid,
    /// The one leaf to drop; `None` only with `dry_run` (the due-leaf listing).
    pub registry_id: Option<Uuid>,
    /// Where the pre-drop COPY export is written (required, no default).
    pub export_dir: &'a Path,
    /// `lock_timeout` of the whole transaction.
    pub lock_timeout: Duration,
    /// Print what would happen, then roll back.
    pub dry_run: bool,
}

/// A leaf as `control.partition_drop_check` saw it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CheckedLeaf {
    /// Its registry row.
    pub registry_id: Uuid,
    /// Its schema-qualified name.
    pub leaf: String,
    /// RFC 3339 UTC; `None` = MINVALUE.
    pub lower_bound: Option<String>,
    /// RFC 3339 UTC.
    pub upper_bound: String,
    /// Its row count.
    pub row_count: i64,
}

/// One due leaf of a `--dry-run` listing with its `partition_drop_check` verdict (`ok` or the refusal code).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DueLeaf {
    /// The leaf.
    #[serde(flatten)]
    pub leaf: CheckedLeaf,
    /// `ok` or the refusal code.
    pub verdict: String,
}

/// A drop receipt: the registry row's receipt columns plus what the run executed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DropReceipt {
    /// The leaf that was dropped.
    #[serde(flatten)]
    pub leaf: CheckedLeaf,
    /// The policy it was dropped under.
    pub policy_id: Uuid,
    /// That policy's revision.
    pub policy_revision: i32,
    /// The export file.
    pub export_path: String,
    /// Its sha256 (lowercase hex).
    pub export_sha256: String,
    /// The statements `control.partition_drop` executed (empty when read back from the registry).
    pub statements: Vec<String>,
    /// The `PARTITION_DROPPED` §77 row (`None` when read back from the registry).
    pub audit_event_id: Option<Uuid>,
}

/// What one `retention execute` did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DropOutcome {
    /// `--dry-run` without `--registry-id`: the due leaves (every attached leaf of the policy's table whose verdict
    /// is not `not_due`), each checked under its own savepoint; nothing written.
    Listed(Vec<DueLeaf>),
    /// `--dry-run --registry-id`: the check passed; the exact statements and the export path a run would use.
    DryRun {
        /// The checked leaf.
        leaf: CheckedLeaf,
        /// `control.partition_drop_statements`, the builder `partition_drop` executes.
        statements: Vec<String>,
        /// The export file a run would write.
        export_path: String,
    },
    /// Exported, detached, dropped, receipted and audited in one transaction.
    Dropped(DropReceipt),
    /// The named row was already DROPPED: its stored receipt (D-I; never the next month).
    AlreadyDropped(DropReceipt),
}

/// `SELECT … FROM control.partition_drop_check($1, $2)` with UTC bounds.
async fn drop_check(
    txn: &mut Txn<'_>,
    policy_id: Uuid,
    registry_id: Uuid,
) -> Result<CheckedLeaf, ProvisioningError> {
    // dep: PostgreSQL(owner) — control.partition_drop_check (0224 superuser-only invoker, row_security = off)
    let row = sqlx::query(
        "SELECT c.leaf, to_char(c.lower_bound AT TIME ZONE 'UTC', $3), to_char(c.upper_bound AT TIME ZONE 'UTC', $3), \
                c.row_count FROM control.partition_drop_check($1, $2) c",
    )
    .bind(policy_id)
    .bind(registry_id)
    .bind(UTC_MASK)
    .fetch_one(&mut **txn)
    .await
    .map_err(refusal)?;
    Ok(CheckedLeaf {
        registry_id,
        leaf: row.try_get(0)?,
        lower_bound: row.try_get(1)?,
        upper_bound: row.try_get(2)?,
        row_count: row.try_get(3)?,
    })
}

/// The stored receipt of a DROPPED registry row.
async fn stored_receipt(
    exec: &RetentionExecutor,
    registry_id: Uuid,
) -> Result<DropReceipt, ProvisioningError> {
    // dep: PostgreSQL(owner) — control.partition_registry receipt columns
    let row = sqlx::query(
        "SELECT leaf_name, to_char(lower_bound AT TIME ZONE 'UTC', $2), to_char(upper_bound AT TIME ZONE 'UTC', $2), \
                rows_dropped, drop_policy_id, drop_policy_revision, export_path, export_sha256 \
           FROM control.partition_registry WHERE registry_id = $1 AND state = 'DROPPED'",
    )
    .bind(registry_id)
    .bind(UTC_MASK)
    .fetch_one(exec.pool())
    .await?;
    Ok(DropReceipt {
        leaf: CheckedLeaf {
            registry_id,
            leaf: row.try_get(0)?,
            lower_bound: row.try_get(1)?,
            upper_bound: row.try_get(2)?,
            row_count: row.try_get(3)?,
        },
        policy_id: row.try_get(4)?,
        policy_revision: row.try_get(5)?,
        export_path: row.try_get(6)?,
        export_sha256: row.try_get(7)?,
        statements: Vec::new(),
        audit_event_id: None,
    })
}

/// ADR-0063 D-J step 8: `COPY <leaf> TO STDOUT` (text format, exact round trip) over the executor's own connection,
/// inside its transaction on the SHARE-locked leaf, into `<path>.tmp` (mode 0600), fsynced, renamed, then the
/// directory fsynced; returns (lines, sha256).
async fn export_leaf(
    txn: &mut Txn<'_>,
    leaf: &str,
    path: &Path,
) -> Result<(i64, String), ProvisioningError> {
    use sha2::{Digest, Sha256};
    use std::io::Write;

    let io = |e: std::io::Error| {
        ProvisioningError::InvalidInput(format!("export {}: {e}", path.display()))
    };
    let tmp = path.with_extension("copy.tmp");
    // ADR-0063 L8: the superuser COPY bypasses RLS, so the file holds every tenant's rows of the leaf and is the only
    // backup of them: owner-only from creation (never the umask's 0644), also when a crashed run left a `.tmp`.
    let mut file = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)
            .map_err(io)?
    };
    // `mode` applies only when the file is created; a leftover `.tmp` keeps its old bits otherwise.
    file.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(0o600))
        .map_err(io)?;
    let (mut lines, mut hasher) = (0_i64, Sha256::new());
    // `leaf` is the registry's regclass text as partition_drop_check returned it (quoted where needed).
    // dep: PostgreSQL(owner) — COPY <leaf> TO STDOUT (superuser: bypasses RLS without any row_security setting)
    let mut stream = txn.copy_out_raw(&format!("COPY {leaf} TO STDOUT")).await?;
    // `poll_next` of the boxed `dyn Stream` needs no futures crate (trait-object method).
    while let Some(chunk) = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        let chunk = chunk?;
        // Text COPY ends every row with one newline and escapes embedded ones, so newlines = rows.
        lines += i64::try_from(chunk.iter().filter(|&&b| b == b'\n').count()).unwrap_or(i64::MAX);
        hasher.update(&chunk);
        file.write_all(&chunk).map_err(io)?;
    }
    drop(stream);
    file.sync_all().map_err(io)?;
    drop(file);
    std::fs::rename(&tmp, path).map_err(io)?;
    // ADR-0063 D-J step 8: the create and the rename are durable only once the directory entry is; this runs before
    // `control.partition_drop` commits, so a crash after the DROP never loses the only copy of the rows.
    let dir = path
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(io)?;
    Ok((lines, hex::encode(hasher.finalize())))
}

/// ADR-0063 D-J `retention execute`: lock_timeout + HXRETAIN, `partition_drop_check`, the verified COPY export, then
/// `control.partition_drop` (which re-derives every predicate itself, DETACHes, DROPs RESTRICT and writes the receipt),
/// then the §77 row, all in one transaction. Never CASCADE, never DETACH CONCURRENTLY, never another leaf than the
/// one named.
pub async fn retention_execute(
    exec: &RetentionExecutor,
    request: &DropRequest<'_>,
    admin: &AdminAction<'_>,
) -> Result<DropOutcome, ProvisioningError> {
    provisioning::require_admin(admin)?;
    // D-I: a real run names its one leaf; refused before any SQL.
    if request.registry_id.is_none() && !request.dry_run {
        return Err(ProvisioningError::InvalidInput(
            "registry_id_required".to_owned(),
        ));
    }
    let mut txn = begin_executor(exec, request.lock_timeout).await?;
    let Some(registry_id) = request.registry_id else {
        return list_due(&mut txn, request.policy_id)
            .await
            .map(DropOutcome::Listed);
    };
    let leaf = match drop_check(&mut txn, request.policy_id, registry_id).await {
        Ok(leaf) => leaf,
        // D-I: a lost COMMIT acknowledgement re-runs into the drop that happened, never into the next month.
        Err(ProvisioningError::Refused(code)) if code == "not_attached" => {
            drop(txn);
            return stored_receipt(exec, registry_id)
                .await
                .map(DropOutcome::AlreadyDropped);
        }
        Err(e) => return Err(e),
    };
    let export_path = request
        .export_dir
        .join(format!("{}__{registry_id}.copy", leaf.leaf));
    if request.dry_run {
        // dep: PostgreSQL(owner) — control.partition_drop_statements (the one builder partition_drop executes)
        let statements: Vec<String> =
            sqlx::query_scalar("SELECT unnest(control.partition_drop_statements($1))")
                .bind(registry_id)
                .fetch_all(&mut *txn)
                .await
                .map_err(refusal)?;
        txn.rollback().await?;
        return Ok(DropOutcome::DryRun {
            leaf,
            statements,
            export_path: export_path.display().to_string(),
        });
    }
    let (lines, export_sha256) = export_leaf(&mut txn, &leaf.leaf, &export_path).await?;
    if lines != leaf.row_count {
        return Err(ProvisioningError::Refused("export_mismatch".to_owned()));
    }
    let export = export_path.display().to_string();
    // dep: PostgreSQL(owner) — control.partition_drop (0224 superuser-only invoker): re-check, DETACH, DROP RESTRICT,
    // registry receipt
    let statements: Vec<String> =
        sqlx::query_scalar("SELECT unnest(control.partition_drop($1, $2, $3, $4, $5))")
            .bind(request.policy_id)
            .bind(registry_id)
            .bind(lines)
            .bind(&export_sha256)
            .bind(&export)
            .fetch_all(&mut *txn)
            .await
            .map_err(refusal)?;
    let policy_revision: i32 = sqlx::query_scalar(
        "SELECT policy_revision FROM control.retention_policies WHERE policy_id = $1",
    )
    .bind(request.policy_id)
    .fetch_one(&mut *txn)
    .await?;
    let mut receipt = DropReceipt {
        leaf,
        policy_id: request.policy_id,
        policy_revision,
        export_path: export,
        export_sha256,
        statements,
        audit_event_id: None,
    };
    receipt.audit_event_id = Some(
        audit_effect(
            &mut txn,
            "PARTITION_DROPPED",
            ("partition_registry", &registry_id.to_string()),
            admin,
            serde_json::to_value(&receipt).unwrap_or_default(),
        )
        .await?,
    );
    txn.commit().await?;
    Ok(DropOutcome::Dropped(receipt))
}

/// D-I `--dry-run` without `--registry-id`: every attached leaf of the policy's table checked under its own savepoint
/// (one refusal does not abort the listing); a `not_due` leaf is left out. The caller's transaction is rolled back.
async fn list_due(txn: &mut Txn<'_>, policy_id: Uuid) -> Result<Vec<DueLeaf>, ProvisioningError> {
    // dep: PostgreSQL(owner) — control.partition_registry ⋈ control.retention_policies: the policy's attached leaves
    let rows = sqlx::query(
        "SELECT r.registry_id, r.leaf_name, to_char(r.lower_bound AT TIME ZONE 'UTC', $2), \
                to_char(r.upper_bound AT TIME ZONE 'UTC', $2) \
           FROM control.retention_policies p JOIN control.partition_registry r ON r.table_key = p.table_key \
          WHERE p.policy_id = $1 AND r.state = 'ATTACHED' ORDER BY r.upper_bound",
    )
    .bind(policy_id)
    .bind(UTC_MASK)
    .fetch_all(&mut **txn)
    .await?;
    let mut due = Vec::new();
    for row in rows {
        let registry_id: Uuid = row.try_get(0)?;
        sqlx::query("SAVEPOINT c36_verdict")
            .execute(&mut **txn)
            .await?;
        let verdict = drop_check(txn, policy_id, registry_id).await;
        let (verdict, row_count) = match verdict {
            Ok(leaf) => {
                sqlx::query("RELEASE SAVEPOINT c36_verdict")
                    .execute(&mut **txn)
                    .await?;
                ("ok".to_owned(), leaf.row_count)
            }
            Err(ProvisioningError::Refused(code)) => {
                sqlx::query("ROLLBACK TO SAVEPOINT c36_verdict")
                    .execute(&mut **txn)
                    .await?;
                if code == "not_due" {
                    continue;
                }
                let leaf: String = row.try_get(1)?;
                // dep: PostgreSQL(owner) — count of a refused leaf (superuser, RLS bypassed)
                let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {leaf}"))
                    .fetch_one(&mut **txn)
                    .await?;
                (code, count)
            }
            Err(e) => return Err(e),
        };
        due.push(DueLeaf {
            leaf: CheckedLeaf {
                registry_id,
                leaf: row.try_get(1)?,
                lower_bound: row.try_get(2)?,
                upper_bound: row.try_get(3)?,
                row_count,
            },
            verdict,
        });
    }
    Ok(due)
}
