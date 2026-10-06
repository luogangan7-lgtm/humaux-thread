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
//!   drop predicate lives here. For card 37 (ADR-0064 D-J, D-V, 10.11 C/E) it owns the backup arms' append-only
//!   receipts (`ops.backup_receipts`), the label identity (`ops.backup_sets`) and `backup status`'s read-only facts;
//!   for the restore drill and `restore pitr` (ADR-0064 D-L..D-O, S5) the witnesses, the newest VERIFIED set, the
//!   both-sides check reads (catalog, migrations, per-tenant facts, isolation probes) and the drill receipt; for the
//!   daemon's DR_EVIDENCE task (ADR-0064 D-K / 10.11 D, S6) the one statement over the receipts, the newest
//!   succeeded drill and the WAL latch, and the D-V estimate ratio both the backup arm and the daemon use.
//! Depends-on: crates=[hex, humaux-domain, serde, serde_json, sha2, sqlx, time]; services=[PostgreSQL(role_maintenance)
//!   r=[control.partition_registry, control.retention_policies, ops.backup_receipts, ops.backup_sets,
//!   ops.jobs, ops.model_call_ledger, ops.restore_witnesses, ops.wal_archive_failures,
//!   private.evidence_objects, private.memory_records, projection.embedding_fingerprints,
//!   projection.private_memory_points, projection.rebuild_tickets, projection.stream_log]
//!   w=[control.partition_registry, ops.backup_receipts, ops.backup_sets, ops.restore_drills, ops.restore_witnesses,
//!   ops.wal_archive_failures] x=[
//!   control.maintenance_tenant_page, control.purge_idle_rate_buckets, ops.auto_redrive_schema_failed,
//!   ops.purge_expired_selection_snapshots, ops.purge_terminal_jobs, projection.reissue_unsettled_tickets],
//!   PostgreSQL(owner) r=[control.partition_registry, control.retention_policies] x=[control.partition_create_month,
//!   control.partition_drop, control.partition_drop_check, control.partition_drop_statements,
//!   control.retention_policy_approve], PostgreSQL(role_gateway) r=[private.evidence_objects, private.memory_records,
//!   projection.private_memory_points], PostgreSQL(role_retrieval_worker) r=[projection.memory_vectors]]; env=[];
//!   modules=[adapters::membership_repo, adapters::postgres, adapters::provisioning, domain::audit, humaux-adapters]
//! Called-by: [adapters::rebuild, maintenance::backup, maintenance::drill, maintenance::retention, maintenance::serve,
//!   tests]
//! Invariants: [the page returns tenant ids only, strictly after the cursor in tenant_id order, at most `limit`;
//!   the definer refuses limit <= 0 (22023), so no call can read every tenant at once; every door call is one
//!   transaction holding the tenant GUC and exactly one statement, so the delete and its receipt (or a reissued
//!   ticket, its outbox carrier and its marker; a re-drive and its §77 row) commit together or not at all; every
//!   age is sent as an interval and compared with the DB clock inside the door; the proposer writes only the two
//!   proposal columns and a failed run resets the horizon family; every executor transaction takes lock_timeout
//!   and the HXRETAIN key first, never sets row_security, and commits its effect with its §77 row or not at all;
//!   the executor never selects a leaf, never CASCADEs and never detaches CONCURRENTLY; the backup receipts are only
//!   ever INSERTed (no UPDATE / DELETE path) and a VERIFIED claim the table refuses surfaces as the database error;
//!   DR_EVIDENCE writes at most one latch row per run and only when no row since the newest VERIFIED start exists]
//! Spec: Baseline §4.2; §6.2.1; §6.2.2; §15.2; §48.1; ADR-0057 D-H; ADR-0062 D-D; ADR-0062 D-E; ADR-0062 D-H;
//!   ADR-0062 D-I; ADR-0062 D-J; ADR-0062 D-N; ADR-0062 D-P; ADR-0062 D-S; ADR-0063 D-F..D-K; ADR-0064 D-J;
//!   ADR-0064 D-K; ADR-0064 D-V; §41.2; §44; §77

use std::collections::BTreeSet;
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
use crate::postgres::{MaintenanceDbPool, RetentionExecutor, RetrievalWorkerDbPool, RuntimeDbPool};
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
    /// [`dr_evidence`] (ADR-0064 D-K / 10.11 D): cluster-level like PARTITIONS; one run = one count.
    DrEvidence,
}

impl MaintenanceTask {
    /// Every task, in D-C cycle order (the counters' slot order).
    pub const ALL: [Self; 11] = [
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
        Self::DrEvidence,
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
            Self::DrEvidence => "dr_evidence",
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
/// [`MaintenanceTask::Partitions`] and [`MaintenanceTask::DrEvidence`]): `affected` is `Some(rows)` when it committed, `None` when it
/// failed. Public so the G80-6 witnesses drive the emit without a database; the one production caller is
/// `maintenance::serve`'s cycle, once per tenant call (once per run for PARTITIONS and DR_EVIDENCE).
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

// ---- ADR-0064 D-J / D-V / 10.11 A, C, E (card 37 S4): the backup arms' receipts, local_only ----
// Owner of every statement below: `humaux-maintenance backup check|run|verify|status` (maintenance::backup), as
// role_maintenance (0234: INSERT, SELECT on ops.backup_sets / ops.backup_receipts / ops.wal_archive_failures; no
// UPDATE, no DELETE: the receipts are append-only). Cluster-level tables: no tenant GUC, no RLS.

/// One `ops.backup_receipts` row as a backup arm writes it. The arm CLAIMS `VERIFIED` by leaving `failure` empty;
/// the table refuses the claim unless verify exited 0 and the verified manifest is the label's first-verified one
/// (`backup_receipts_verified_derived`, FK to `ops.backup_sets`) and the row is shaped (`backup_receipts_shape`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackupReceipt {
    /// pgBackRest's set label; `None` for a refusal before any write (10.11 A).
    pub backup_label: Option<String>,
    /// The set's start / stop as pgBackRest's `info` reports them.
    pub backup_started_at: Option<OffsetDateTime>,
    /// See `backup_started_at`.
    pub backup_stopped_at: Option<OffsetDateTime>,
    /// sha256 of the set's `backup.manifest` pulled back now with `repo-get`.
    pub manifest_sha256: Option<Vec<u8>>,
    /// `pgbackrest verify --set` exit code; `None` when verify did not run.
    pub verify_exit: Option<i32>,
    /// The manifest sha256 this verification proved: set only when every verify step passed.
    pub verified_manifest_sha256: Option<Vec<u8>>,
    /// The named failure; `None` claims VERIFIED.
    pub failure: Option<String>,
    /// Live `du -sk` of the repository (backup arms only; D-K reads the newest non-NULL one).
    pub repo_bytes: Option<i64>,
    /// Live `df -Pk` free bytes of the repository filesystem.
    pub repo_free_bytes: Option<i64>,
    /// The set's repository size from `info` (`backup[].info.repository.size`, spike SP-11).
    pub set_repo_bytes: Option<i64>,
    /// `HUMAUX_MAINTENANCE_BACKUP_REPO_MAX_BYTES` as the arm used it (10.11 C: the daemon reads no budget key).
    pub repo_max_bytes: Option<i64>,
    /// `HUMAUX_MAINTENANCE_BACKUP_MIN_FREE_BYTES` as the arm used it.
    pub min_free_bytes: Option<i64>,
    /// The D-V estimate the budget precheck used.
    pub estimate_bytes: Option<i64>,
}

/// Appends one receipt (`backup_type` is always `full` in card 37, D-V); returns its id. A VERIFIED claim the table
/// refuses comes back as the database error (23514 / 23503), never as a quietly FAILED row.
pub async fn insert_backup_receipt(
    pool: &MaintenanceDbPool,
    receipt: &BackupReceipt,
) -> Result<Uuid, sqlx::Error> {
    let outcome = if receipt.failure.is_none() {
        "VERIFIED"
    } else {
        "FAILED"
    };
    // dep: PostgreSQL(role_maintenance) — ops.backup_receipts INSERT (0234)
    sqlx::query_scalar(
        "INSERT INTO ops.backup_receipts (backup_label, backup_type, backup_started_at, backup_stopped_at, \
           manifest_sha256, verify_exit, verified_manifest_sha256, outcome, failure, repo_bytes, repo_free_bytes, \
           set_repo_bytes, repo_max_bytes, min_free_bytes, estimate_bytes) \
         VALUES ($1, 'full', $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) RETURNING receipt_id",
    )
    .bind(&receipt.backup_label)
    .bind(receipt.backup_started_at)
    .bind(receipt.backup_stopped_at)
    .bind(&receipt.manifest_sha256)
    .bind(receipt.verify_exit)
    .bind(&receipt.verified_manifest_sha256)
    .bind(outcome)
    .bind(&receipt.failure)
    .bind(receipt.repo_bytes)
    .bind(receipt.repo_free_bytes)
    .bind(receipt.set_repo_bytes)
    .bind(receipt.repo_max_bytes)
    .bind(receipt.min_free_bytes)
    .bind(receipt.estimate_bytes)
    .fetch_one(pool.pool())
    .await
}

/// ADR-0064 D-J step 2: the manifest sha256 of `label`'s FIRST verification, if it was ever verified.
pub async fn first_verified_manifest(
    pool: &MaintenanceDbPool,
    label: &str,
) -> Result<Option<Vec<u8>>, sqlx::Error> {
    // dep: PostgreSQL(role_maintenance) — ops.backup_sets SELECT (0234)
    sqlx::query_scalar("SELECT manifest_sha256 FROM ops.backup_sets WHERE backup_label = $1")
        .bind(label)
        .fetch_optional(pool.pool())
        .await
}

/// ADR-0064 D-J step 2: records `manifest_sha256` as `label`'s identity after its first clean verification; a later
/// call for the same label writes nothing (the first verification stays the identity).
pub async fn bind_backup_set(
    pool: &MaintenanceDbPool,
    label: &str,
    manifest_sha256: &[u8],
) -> Result<(), sqlx::Error> {
    // dep: PostgreSQL(role_maintenance) — ops.backup_sets INSERT (0234)
    sqlx::query(
        "INSERT INTO ops.backup_sets (backup_label, manifest_sha256) VALUES ($1, $2) \
         ON CONFLICT (backup_label) DO NOTHING",
    )
    .bind(label)
    .bind(manifest_sha256)
    .execute(pool.pool())
    .await
    .map(|_| ())
}

/// ADR-0064 D-V: the newest VERIFIED set's repository bytes, the basis of the next backup's estimate (`None` before
/// the first verified backup).
pub async fn newest_verified_set_repo_bytes(
    pool: &MaintenanceDbPool,
) -> Result<Option<i64>, sqlx::Error> {
    // dep: PostgreSQL(role_maintenance) — ops.backup_receipts SELECT (0234)
    sqlx::query_scalar(
        "SELECT set_repo_bytes FROM ops.backup_receipts \
         WHERE outcome = 'VERIFIED' AND set_repo_bytes IS NOT NULL ORDER BY recorded_at DESC LIMIT 1",
    )
    .fetch_optional(pool.pool())
    .await
}

/// The latest verification verdict of one backup set (the newest receipt under its label).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetVerdict {
    /// pgBackRest's set label.
    pub label: String,
    /// The newest receipt of the label is VERIFIED.
    pub verified: bool,
    /// The set's start as the receipt recorded it.
    pub started_at: Option<OffsetDateTime>,
    /// The set's stop as the receipt recorded it.
    pub stopped_at: Option<OffsetDateTime>,
}

/// What `backup status` reads from the database, in one READ ONLY snapshot (status writes nothing, T-J4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupStatusFacts {
    /// Every labelled set's latest verdict (D-K's `latest` CTE: `DISTINCT ON (backup_label)`, newest first).
    pub sets: Vec<SetVerdict>,
    /// The oldest `ops.wal_archive_failures` row newer than the newest VERIFIED start (10.11 D's `v`): the latch.
    pub latched_since: Option<OffsetDateTime>,
    /// The newest `ops.wal_archive_failures` row (the PITR window starts after it, 10.11 E).
    pub newest_failure: Option<OffsetDateTime>,
    /// `pg_stat_archiver.last_failed_time` when it is newer than `last_archived_time`: failing right now.
    pub failing_now_since: Option<OffsetDateTime>,
}

/// ADR-0064 10.11 D / E: the facts `backup status` prints, read-only.
pub async fn backup_status_facts(
    pool: &MaintenanceDbPool,
) -> Result<BackupStatusFacts, sqlx::Error> {
    // dep: PostgreSQL(role_maintenance) — one READ ONLY snapshot of the receipts, the latch and pg_stat_archiver
    let mut txn = pool.pool().begin().await?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *txn)
        .await?;
    let rows = sqlx::query(
        "SELECT backup_label, outcome = 'VERIFIED', backup_started_at, backup_stopped_at FROM ( \
           SELECT DISTINCT ON (backup_label) backup_label, outcome, backup_started_at, backup_stopped_at \
           FROM ops.backup_receipts WHERE backup_label IS NOT NULL \
           ORDER BY backup_label, recorded_at DESC) latest ORDER BY backup_started_at",
    )
    .fetch_all(&mut *txn)
    .await?;
    let sets = rows
        .iter()
        .map(|r| {
            Ok(SetVerdict {
                label: r.try_get(0)?,
                verified: r.try_get(1)?,
                started_at: r.try_get(2)?,
                stopped_at: r.try_get(3)?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;
    let row = sqlx::query(
        "WITH v AS (SELECT coalesce(max(backup_started_at), '-infinity'::timestamptz) AS v \
                    FROM ops.backup_receipts WHERE outcome = 'VERIFIED') \
         SELECT (SELECT min(observed_at) FROM ops.wal_archive_failures, v WHERE observed_at > v.v), \
                (SELECT max(observed_at) FROM ops.wal_archive_failures), \
                (SELECT a.last_failed_time FROM pg_stat_archiver a \
                  WHERE a.last_failed_time > coalesce(a.last_archived_time, '-infinity'::timestamptz))",
    )
    .fetch_one(&mut *txn)
    .await?;
    let facts = BackupStatusFacts {
        sets,
        latched_since: row.try_get(0)?,
        newest_failure: row.try_get(1)?,
        failing_now_since: row.try_get(2)?,
    };
    txn.commit().await?;
    Ok(facts)
}

// ---- ADR-0064 D-K / 10.11 D (card 37 S6): the daemon's DR_EVIDENCE statement ----
// Owner: `humaux-maintenance --serve`'s cluster-level DR_EVIDENCE task (maintenance::serve), as role_maintenance
// (0234: SELECT on ops.backup_receipts, INSERT + SELECT on ops.wal_archive_failures; ops.restore_drills SELECT by the
// 0011 ops default; pg_stat_archiver is readable by PUBLIC, spike SP-12). One statement, no tenant GUC, no LIMIT: it
// reads the latest receipt per label, the newest succeeded drill and the newest measuring receipt, and writes at
// most one latch row.

/// ADR-0064 D-V: the next set's estimate is the newest VERIFIED set's repository bytes × 1.25, kept as the exact
/// ratio 5/4. One definition for the backup arm's precheck and the daemon's headroom.
pub const ESTIMATE_RATIO: (i64, i64) = (5, 4);

/// [`ESTIMATE_RATIO`] applied to one set's repository bytes.
pub const fn estimate_from_set_bytes(set_repo_bytes: i64) -> i64 {
    set_repo_bytes * ESTIMATE_RATIO.0 / ESTIMATE_RATIO.1
}

/// The newest receipt that measured the repository (a budget refusal qualifies, 10.11 D / F4), with the limits the
/// arm used then; the daemon reads no budget key (10.11 C / F10).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeasuredBudget {
    /// `du -sk` of the repository at that run.
    pub repo_bytes: i64,
    /// `HUMAUX_MAINTENANCE_BACKUP_REPO_MAX_BYTES` as that run used it.
    pub repo_max_bytes: Option<i64>,
    /// `HUMAUX_MAINTENANCE_BACKUP_MIN_FREE_BYTES` as that run used it.
    pub min_free_bytes: Option<i64>,
    /// The next set's estimate: newest VERIFIED `set_repo_bytes` × 1.25, else that receipt's `estimate_bytes`.
    pub estimate_bytes: Option<i64>,
}

/// One DR_EVIDENCE reading from the database (ADR-0064 10.11 D). Timestamps are unix seconds.
#[derive(Debug, Clone, PartialEq)]
pub struct DrEvidence {
    /// `max(backup_stopped_at)` over the labels whose latest receipt is VERIFIED; `None` before the first.
    pub backup_last_success: Option<f64>,
    /// `finished_at` of the newest drill the table derived as succeeded.
    pub restore_drill_last_success: Option<f64>,
    /// `None` before the first measuring receipt.
    pub budget: Option<MeasuredBudget>,
    /// The latch (a row observed after the newest VERIFIED start) or archiving failing now.
    pub wal_archive_failing: bool,
}

/// ADR-0064 10.11 D: the DR_EVIDENCE statement. `last_failed` / `last_archived` override `pg_stat_archiver`'s two
/// columns and exist only for T-K3' (a dev cluster with archiving off cannot fail an archive); production binds
/// `None, None`. A latch row is written when a failure is newer than the newest VERIFIED start `v` and no row since
/// `v` exists ("any failure since the newest VERIFIED start", so a queue-max drop that PostgreSQL counts as archived
/// stays latched); it resolves only when a VERIFIED full starts after it.
pub async fn dr_evidence(
    pool: &MaintenanceDbPool,
    last_failed: Option<OffsetDateTime>,
    last_archived: Option<OffsetDateTime>,
) -> Result<DrEvidence, sqlx::Error> {
    // dep: PostgreSQL(role_maintenance) — ops.backup_receipts + ops.restore_drills + pg_stat_archiver SELECT,
    // ops.wal_archive_failures SELECT/INSERT (0234), one statement
    let row = sqlx::query(
        "WITH latest AS ( \
           SELECT DISTINCT ON (backup_label) outcome, backup_stopped_at FROM ops.backup_receipts \
           WHERE backup_label IS NOT NULL ORDER BY backup_label, recorded_at DESC), \
         v AS (SELECT coalesce(max(backup_started_at), '-infinity'::timestamptz) AS v \
               FROM ops.backup_receipts WHERE outcome = 'VERIFIED'), \
         a AS (SELECT coalesce($1::timestamptz, last_failed_time) AS last_failed_time, \
                      coalesce($2::timestamptz, last_archived_time) AS last_archived_time FROM pg_stat_archiver), \
         ins AS (INSERT INTO ops.wal_archive_failures (observed_at) \
                 SELECT clock_timestamp() FROM a, v \
                 WHERE a.last_failed_time > v.v \
                   AND NOT EXISTS (SELECT 1 FROM ops.wal_archive_failures f WHERE f.observed_at > v.v) \
                 RETURNING observed_at), \
         measured AS (SELECT repo_bytes, repo_max_bytes, min_free_bytes, estimate_bytes FROM ops.backup_receipts \
                      WHERE repo_bytes IS NOT NULL ORDER BY recorded_at DESC LIMIT 1) \
         SELECT (SELECT extract(epoch FROM max(backup_stopped_at))::float8 FROM latest WHERE outcome = 'VERIFIED'), \
                (SELECT extract(epoch FROM max(finished_at))::float8 FROM ops.restore_drills WHERE succeeded), \
                m.repo_bytes, m.repo_max_bytes, m.min_free_bytes, m.estimate_bytes, \
                (SELECT set_repo_bytes FROM ops.backup_receipts \
                  WHERE outcome = 'VERIFIED' AND set_repo_bytes IS NOT NULL ORDER BY recorded_at DESC LIMIT 1), \
                EXISTS (SELECT 1 FROM ins) \
                OR EXISTS (SELECT 1 FROM ops.wal_archive_failures, v WHERE observed_at > v.v) \
                OR coalesce((SELECT a.last_failed_time > coalesce(a.last_archived_time, '-infinity'::timestamptz) \
                             FROM a), false) \
         FROM (SELECT 1) one LEFT JOIN measured m ON true",
    )
    .bind(last_failed)
    .bind(last_archived)
    .fetch_one(pool.pool())
    .await?;
    let repo_bytes: Option<i64> = row.try_get(2)?;
    let verified_set: Option<i64> = row.try_get(6)?;
    let budget = match repo_bytes {
        Some(repo_bytes) => Some(MeasuredBudget {
            repo_bytes,
            repo_max_bytes: row.try_get(3)?,
            min_free_bytes: row.try_get(4)?,
            estimate_bytes: verified_set
                .map(estimate_from_set_bytes)
                .or(row.try_get(5)?),
        }),
        None => None,
    };
    Ok(DrEvidence {
        backup_last_success: row.try_get(0)?,
        restore_drill_last_success: row.try_get(1)?,
        budget,
        wal_archive_failing: row.try_get(7)?,
    })
}

// ---- ADR-0064 D-L..D-O, D-U (card 37 S5): the restore drill's and `restore pitr`'s SQL ----
// Owner of every statement below: `humaux-maintenance restore drill | restore pitr` (maintenance::drill). Source side
// (HUMAUX_MAINTENANCE_PG_DSN, role_maintenance): the newest VERIFIED set, the witnesses A/B (0234 INSERT, SELECT),
// the source's migration rows at T, the Evidence digests, the receipt (0234 INSERT on ops.restore_drills). Drill
// side (the restored cluster through `docker compose port`, neutralised passwords): the same reads plus the
// isolation probes as role_gateway / role_retrieval_worker. Every tenant-scoped read sets the tenant GUC in its own
// transaction (RLS on every table with a tenant_id); every read is READ ONLY except the witness and receipt INSERTs.

/// The set a drill or `restore pitr` restores: the newest set whose LATEST receipt is VERIFIED (D-K's rule).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedSet {
    /// pgBackRest's set label.
    pub label: String,
    /// The label's identity (`ops.backup_sets.manifest_sha256`, the first verification's manifest).
    pub manifest_sha256: Vec<u8>,
    /// The set's stop as its receipt recorded it.
    pub stopped_at: Option<OffsetDateTime>,
}

/// ADR-0064 D-L step 1: the newest set whose latest receipt is VERIFIED, with its identity; `None` when no set
/// qualifies (the drill then refuses `no_verified_backup`, it never falls back to an unverified set).
pub async fn newest_verified_set(
    pool: &MaintenanceDbPool,
) -> Result<Option<VerifiedSet>, sqlx::Error> {
    // dep: PostgreSQL(role_maintenance) — ops.backup_receipts + ops.backup_sets SELECT (0234)
    let row = sqlx::query(
        "SELECT l.backup_label, s.manifest_sha256, l.backup_stopped_at FROM ( \
           SELECT DISTINCT ON (backup_label) backup_label, outcome, backup_stopped_at \
             FROM ops.backup_receipts WHERE backup_label IS NOT NULL \
            ORDER BY backup_label, recorded_at DESC) l \
           JOIN ops.backup_sets s ON s.backup_label = l.backup_label \
          WHERE l.outcome = 'VERIFIED' ORDER BY l.backup_stopped_at DESC NULLS LAST LIMIT 1",
    )
    .fetch_optional(pool.pool())
    .await?;
    row.map(|r| {
        Ok(VerifiedSet {
            label: r.try_get(0)?,
            manifest_sha256: r.try_get(1)?,
            stopped_at: r.try_get(2)?,
        })
    })
    .transpose()
}

/// ADR-0064 D-L step 2: one committed witness row (`kind` 'A' or 'B'); its `written_at` and the WAL file the
/// insert position is in after the commit (`pg_walfile_name(pg_current_wal_insert_lsn())`).
pub async fn write_witness(
    pool: &MaintenanceDbPool,
    drill_id: Uuid,
    kind: &str,
) -> Result<(OffsetDateTime, String), sqlx::Error> {
    // dep: PostgreSQL(role_maintenance) — ops.restore_witnesses INSERT (0234)
    let written: OffsetDateTime = sqlx::query_scalar(
        "INSERT INTO ops.restore_witnesses (drill_id, kind) VALUES ($1, $2) RETURNING written_at",
    )
    .bind(drill_id)
    .bind(kind)
    .fetch_one(pool.pool())
    .await?;
    // dep: PostgreSQL(role_maintenance) — WAL position after the commit (PUBLIC functions)
    let walfile: String = sqlx::query_scalar("SELECT pg_walfile_name(pg_current_wal_insert_lsn())")
        .fetch_one(pool.pool())
        .await?;
    Ok((written, walfile))
}

/// ADR-0064 D-L step 2: `clock_timestamp()` read in its own transaction, between the two witness commits (T).
pub async fn clock(pool: &MaintenanceDbPool) -> Result<OffsetDateTime, sqlx::Error> {
    // dep: PostgreSQL(role_maintenance) — the database clock
    sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool.pool())
        .await
}

/// ADR-0064 D-N (b): the witness kinds of `drill_id` this pool sees (`{A}` = the restore at T; `B` = the source).
pub async fn witness_kinds(
    pool: &MaintenanceDbPool,
    drill_id: Uuid,
) -> Result<Vec<String>, sqlx::Error> {
    // dep: PostgreSQL(role_maintenance) — ops.restore_witnesses SELECT (0234)
    sqlx::query_scalar("SELECT kind FROM ops.restore_witnesses WHERE drill_id = $1 ORDER BY kind")
        .bind(drill_id)
        .fetch_all(pool.pool())
        .await
}

/// The cluster facts a drill reads through role_maintenance (D-N (c), (e), (j)). D-N (d) is not here:
/// `ops.schema_migrations` is owner-only (0201 D-C), so the drill reads it as the image superuser over each
/// container's socket.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterFacts {
    /// `server_version_num`.
    pub server_version_num: i32,
    /// D-N (e): tenant tables (`relkind` r/p with a `tenant_id` column) lacking `relrowsecurity AND
    /// relforcerowsecurity` or lacking a policy.
    pub rls_unforced: i64,
    /// D-N (j): `archived_count + failed_count` of `pg_stat_archiver` (0 when `--archive-mode=off` held).
    pub archiver_attempts: i64,
}

/// ADR-0064 D-N (c), (e), (j) in one READ ONLY snapshot.
pub async fn cluster_facts(pool: &MaintenanceDbPool) -> Result<ClusterFacts, sqlx::Error> {
    // dep: PostgreSQL(role_maintenance) — catalog and pg_stat_archiver (READ ONLY)
    let mut txn = pool.pool().begin().await?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *txn)
        .await?;
    let row = sqlx::query(
        "SELECT current_setting('server_version_num')::int, \
                (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                  WHERE c.relkind IN ('r', 'p') AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
                    AND NOT c.relispartition \
                    AND EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid \
                                 AND a.attname = 'tenant_id' AND NOT a.attisdropped) \
                    AND (NOT (c.relrowsecurity AND c.relforcerowsecurity) \
                         OR NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = c.oid))), \
                (SELECT archived_count + failed_count FROM pg_stat_archiver)",
    )
    .fetch_one(&mut *txn)
    .await?;
    txn.commit().await?;
    Ok(ClusterFacts {
        server_version_num: row.try_get(0)?,
        rls_unforced: row.try_get(1)?,
        archiver_attempts: row.try_get(2)?,
    })
}

/// What one tenant holds, read as role_maintenance under the tenant GUC (D-N (f) ranking, (h), restored state).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TenantFacts {
    /// `private.memory_records` rows visible under the GUC (the (f) ranking).
    pub memories: i64,
    /// D-N (h): sha256 over the sorted `evidence_id:payload_sha256` pairs, `created_at <= at` when `at` was given.
    pub evidence_digest: Vec<u8>,
    /// Restored in-flight state the drill leaves untouched (R-37 quarantine): ISSUED tickets that are not
    /// generation tickets, non-terminal `ops.jobs`, RESERVED `ops.model_call_ledger` rows.
    pub issued_g1: i64,
    /// See `issued_g1`.
    pub open_jobs: i64,
    /// See `issued_g1`.
    pub reserved_calls: i64,
    /// Distinct `embedding_version` of live registry rows.
    pub labels: BTreeSet<String>,
    /// D-L step 0 / 10.11 H: live registry rows × (4 × dimension + 2,048) bytes.
    pub vector_bytes: i64,
}

/// ADR-0064 D-N (f), (h) and the restored-state counts for one tenant, in one READ ONLY transaction.
pub async fn tenant_facts(
    pool: &MaintenanceDbPool,
    tenant: Uuid,
    at: Option<OffsetDateTime>,
) -> Result<TenantFacts, sqlx::Error> {
    // dep: PostgreSQL(role_maintenance) — per-tenant READ ONLY reads under the tenant GUC (0012 RLS)
    let mut txn = pool.pool().begin().await?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *txn)
        .await?;
    sqlx::query("SELECT set_config('humaux.tenant_id', $1::text, true)")
        .bind(tenant)
        .execute(&mut *txn)
        .await?;
    let row = sqlx::query(
        "SELECT (SELECT count(*) FROM private.memory_records WHERE tenant_id = $1), \
                (SELECT sha256(convert_to(coalesce(string_agg(evidence_id::text || ':' || encode(payload_sha256, 'hex'), \
                         ',' ORDER BY evidence_id), ''), 'UTF8')) \
                   FROM private.evidence_objects \
                  WHERE tenant_id = $1 AND ($2::timestamptz IS NULL OR created_at <= $2)), \
                (SELECT count(*) FROM projection.stream_log sl WHERE sl.tenant_id = $1 AND sl.state = 'ISSUED' \
                    AND NOT EXISTS (SELECT 1 FROM projection.rebuild_tickets rt \
                                     WHERE rt.tenant_id = sl.tenant_id AND rt.scope_kind = sl.scope_kind \
                                       AND rt.scope_id = sl.scope_id AND rt.domain = sl.domain \
                                       AND rt.projection_kind = sl.projection_kind \
                                       AND rt.projection_version = sl.projection_version \
                                       AND rt.stream_seq = sl.stream_seq)), \
                (SELECT count(*) FROM ops.jobs WHERE tenant_id = $1 AND status NOT IN ('DONE', 'FAILED', 'DEAD')), \
                (SELECT count(*) FROM ops.model_call_ledger WHERE tenant_id = $1 AND status = 'RESERVED'), \
                ARRAY(SELECT DISTINCT embedding_version FROM projection.private_memory_points \
                       WHERE tenant_id = $1 AND projection_live), \
                (SELECT coalesce(sum(4 * coalesce(ef.dimension, 0) + 2048), 0)::bigint \
                   FROM projection.private_memory_points p \
                   LEFT JOIN projection.embedding_fingerprints ef ON ef.fingerprint_sha256 = p.fingerprint_sha256 \
                  WHERE p.tenant_id = $1 AND p.projection_live)",
    )
    .bind(tenant)
    .bind(at)
    .fetch_one(&mut *txn)
    .await?;
    txn.commit().await?;
    Ok(TenantFacts {
        memories: row.try_get(0)?,
        evidence_digest: row.try_get(1)?,
        issued_g1: row.try_get(2)?,
        open_jobs: row.try_get(3)?,
        reserved_calls: row.try_get(4)?,
        labels: row.try_get::<Vec<String>, _>(5)?.into_iter().collect(),
        vector_bytes: row.try_get(6)?,
    })
}

/// ADR-0064 D-N (f), as role_gateway under `tenant`'s GUC: `(other-tenant rows visible in memory_records,
/// evidence_objects and private_memory_points, own memory_records)`.
pub async fn gateway_isolation(
    pool: &RuntimeDbPool,
    tenant: Uuid,
) -> Result<(i64, i64), sqlx::Error> {
    // dep: PostgreSQL(role_gateway) — READ ONLY isolation probe under the tenant GUC (0012 RLS)
    let mut txn = pool.pool().begin().await?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *txn)
        .await?;
    sqlx::query("SELECT set_config('humaux.tenant_id', $1::text, true)")
        .bind(tenant)
        .execute(&mut *txn)
        .await?;
    let row = sqlx::query(
        "SELECT (SELECT count(*) FROM private.memory_records WHERE tenant_id <> $1) \
              + (SELECT count(*) FROM private.evidence_objects WHERE tenant_id <> $1) \
              + (SELECT count(*) FROM projection.private_memory_points WHERE tenant_id <> $1), \
                (SELECT count(*) FROM private.memory_records WHERE tenant_id = $1)",
    )
    .bind(tenant)
    .fetch_one(&mut *txn)
    .await?;
    txn.commit().await?;
    Ok((row.try_get(0)?, row.try_get(1)?))
}

/// ADR-0064 D-N (f), as role_retrieval_worker (the only tenant-scoped reader of vectors) under `tenant`'s GUC:
/// other-tenant rows visible in `projection.memory_vectors`.
pub async fn vector_isolation(
    pool: &RetrievalWorkerDbPool,
    tenant: Uuid,
) -> Result<i64, sqlx::Error> {
    // dep: PostgreSQL(role_retrieval_worker) — READ ONLY isolation probe under the tenant GUC (0232 RLS)
    let mut txn = pool.pool().begin().await?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *txn)
        .await?;
    sqlx::query("SELECT set_config('humaux.tenant_id', $1::text, true)")
        .bind(tenant)
        .execute(&mut *txn)
        .await?;
    let n: i64 =
        sqlx::query_scalar("SELECT count(*) FROM projection.memory_vectors WHERE tenant_id <> $1")
            .bind(tenant)
            .fetch_one(&mut *txn)
            .await?;
    txn.commit().await?;
    Ok(n)
}

/// One `ops.restore_drills` row (D-O as 0234 shaped it). `succeeded` is NOT a field: the receipt claims it only by
/// leaving `failure` empty, and `restore_drills_succeeded_derived` refuses a claim any check contradicts.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DrillReceipt {
    /// The drill id (`restore_drill_id`).
    pub drill_id: Uuid,
    /// The restored set.
    pub backup_label: Option<String>,
    /// Its identity as the drill read it.
    pub backup_manifest_sha256: Option<Vec<u8>>,
    /// T (between the witnesses).
    pub target_time: Option<OffsetDateTime>,
    /// Drill start / end.
    pub started_at: Option<OffsetDateTime>,
    /// See `started_at`.
    pub finished_at: Option<OffsetDateTime>,
    /// D-N (a).
    pub manifest_matches: Option<bool>,
    /// D-N (b).
    pub witness_a_present: Option<bool>,
    /// D-N (b).
    pub witness_b_absent: Option<bool>,
    /// D-N (c).
    pub server_version_matches: Option<bool>,
    /// D-N (d).
    pub migrations_drift: Option<i32>,
    /// D-N (e).
    pub rls_unforced: Option<i32>,
    /// D-N (f).
    pub isolation_violations: Option<i32>,
    /// D-N (f).
    pub isolation_pairs: Option<i32>,
    /// D-N (h).
    pub payload_digest_mismatches: Option<i32>,
    /// D-M: refused embedder attempts.
    pub provider_calls: Option<i64>,
    /// D-N (j).
    pub drill_archiver_attempts: Option<i64>,
    /// D-N (g).
    pub rebuild_equivalent: Option<bool>,
    /// D-N (g).
    pub rebuild_points: Option<i64>,
    /// D-G.
    pub legacy_points_without_vector: Option<i64>,
    /// D-N (g): memories distilled but unprojected at T.
    pub unprojected_at_target: Option<i64>,
    /// R-37 quarantine counts.
    pub restored_in_flight: Option<serde_json::Value>,
    /// D-O: labelled resources left after destroy.
    pub residue: Option<i32>,
    /// D-Q RTO.
    pub rto_seconds: Option<f64>,
    /// Every phase, timed.
    pub phase_seconds: Option<serde_json::Value>,
    /// The first named failure; `None` claims success.
    pub failure: Option<String>,
    /// 10.11 H.
    pub repo_intact: Option<bool>,
}

/// ADR-0064 D-O step 9: the receipt, written AFTER destroy (residue known). A success claim the table refuses comes
/// back as the database error (23514), never as a quietly failed row.
pub async fn insert_drill_receipt(
    pool: &MaintenanceDbPool,
    r: &DrillReceipt,
) -> Result<bool, sqlx::Error> {
    // dep: PostgreSQL(role_maintenance) — ops.restore_drills INSERT (0234)
    sqlx::query_scalar(
        "INSERT INTO ops.restore_drills (restore_drill_id, succeeded, backup_label, backup_manifest_sha256, \
           target_time, started_at, finished_at, manifest_matches, witness_a_present, witness_b_absent, \
           server_version_matches, migrations_drift, rls_unforced, isolation_violations, isolation_pairs, \
           payload_digest_mismatches, provider_calls, drill_archiver_attempts, rebuild_equivalent, rebuild_points, \
           legacy_points_without_vector, unprojected_at_target, restored_in_flight, residue, rto_seconds, \
           phase_seconds, failure, repo_intact) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, \
           $23, $24, $25::float8::numeric, $26, $27, $28) RETURNING succeeded",
    )
    .bind(r.drill_id)
    .bind(r.failure.is_none())
    .bind(&r.backup_label)
    .bind(&r.backup_manifest_sha256)
    .bind(r.target_time)
    .bind(r.started_at)
    .bind(r.finished_at)
    .bind(r.manifest_matches)
    .bind(r.witness_a_present)
    .bind(r.witness_b_absent)
    .bind(r.server_version_matches)
    .bind(r.migrations_drift)
    .bind(r.rls_unforced)
    .bind(r.isolation_violations)
    .bind(r.isolation_pairs)
    .bind(r.payload_digest_mismatches)
    .bind(r.provider_calls)
    .bind(r.drill_archiver_attempts)
    .bind(r.rebuild_equivalent)
    .bind(r.rebuild_points)
    .bind(r.legacy_points_without_vector)
    .bind(r.unprojected_at_target)
    .bind(&r.restored_in_flight)
    .bind(r.residue)
    .bind(r.rto_seconds)
    .bind(&r.phase_seconds)
    .bind(&r.failure)
    .bind(r.repo_intact)
    .fetch_one(pool.pool())
    .await
}
