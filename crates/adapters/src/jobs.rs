//! `adapters::jobs` — `ops.jobs` SKIP LOCKED claim, lease heartbeat, and terminal-state transitions (§31 Durable Jobs
//!   / §61 SKIP LOCKED Claim SQL).
//! Depends-on: crates=[serde_json, sqlx]; services=[PostgreSQL(any) r=[ops.claim_derived_work] w=[ops.jobs] x=[ops.admit_distill_budget, ops.begin_call, ops.claim_derived_work, ops.claim_derived_work_v2, ops.distill_slots_all_bound, ops.finish_derived_work_v2, ops.renew_lease]]; env=[CARGO_MANIFEST_DIR]; modules=[adapters::postgres]
//! Called-by: [adapters::consolidate_repo, adapters::distill_repo, humaux-consolidation-worker, humaux-private-worker, private-worker::distill, private-worker::main, tests]
//! Invariants: [one tenant per call: each table-level function opens a transaction and sets humaux.tenant_id before
//!   touching FORCE-RLS ops.jobs; claims use SKIP LOCKED with a lease; a PG error returns to the caller with no job
//!   state change; distill claims go only through the four ADR-0058 owner definers, which filter by tenant and job
//!   themselves; for DERIVED_DISTILL the attempt moves at begin_call, never at claim, a request is admitted only
//!   within the tenant's §72.3 budget (same transaction as begin_call), and a WAITING_KEY finish reverts its attempt]
//! Spec: Baseline §31; §6.1; §48.2; §62; §32; §61; §67.2; §11; ADR-0058
//!
//! `ops.jobs` already carries every column §31 lists (`migrations/0008_ops_core.sql`) and its
//! `status` CHECK constraint already enumerates all seven states — this module adds no
//! migration, only the query layer.
//!
//! Every function here operates on one tenant at a time: `ops.jobs` has `FORCE ROW LEVEL
//! SECURITY` (§6.1/§48.2, `jobs_tenant_isolation` policy keyed on
//! `current_setting('humaux.tenant_id', true)`), and no runtime role is `BYPASSRLS`
//! (§48.2 "所有 runtime role: NOBYPASSRLS"). Each call opens its own transaction, sets
//! `SET LOCAL humaux.tenant_id` (cleared automatically on commit/rollback, §62), then runs its
//! query — mirroring the transaction-scoped tenant context §62 requires for every application
//! transaction. Tenant fairness *across* tenants exists for `DERIVED_DISTILL` only: ADR-0058's
//! [`claim_distill`] serves the least-recently-served tenant through four provider slots (§67.2);
//! cost weighting (§32 DRR) is not built (ADR-0058 L2).
//!
//! Enqueueing (`INSERT INTO ops.jobs`) is out of this module's scope. One consequence worth
//! flagging for that future task: §61's claim SQL filters `next_retry_at <= now()` verbatim,
//! and `next_retry_at` has no column default — a `PENDING` row inserted without an explicit
//! `next_retry_at` has `NULL` there forever, and `NULL <= now()` is `NULL` (false), so it would
//! never be claimed. The enqueue path must set `next_retry_at = now()` (or later) at INSERT
//! time; [`claim`] implements §61 verbatim rather than silently patching around a missing
//! upstream default.

use sqlx::Row;
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

use crate::postgres::{ConsolidationDbPool, PrivateWorkerDbPool, RuntimeDbPool};

/// DB-layer failure from any function in this module. Adapter-local, not one of the
/// workspace's two frozen domain error enums (§52) — same reasoning as
/// `email::outbox::OutboxError` and `postgres::PoolInitError`.
#[derive(Debug)]
pub enum JobsError {
    Db(sqlx::Error),
    /// A `status` value came back from the database that is not one of the seven §31 states —
    /// only reachable if `ops.jobs_status_check` itself has drifted from this enum (§78.2).
    UnknownStatus(String),
}

impl From<sqlx::Error> for JobsError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

impl std::fmt::Display for JobsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(e) => write!(f, "ops.jobs DB error: {e}"),
            Self::UnknownStatus(s) => {
                write!(f, "ops.jobs.status {s:?} matches no JobStatus variant")
            }
        }
    }
}

impl std::error::Error for JobsError {}

/// §31 Job state — verbatim seven-variant list, in `ops.jobs_status_check`'s order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Processing,
    WaitingKey,
    RetryWait,
    Done,
    Failed,
    Dead,
}

impl JobStatus {
    pub const ALL: [JobStatus; 7] = [
        Self::Pending,
        Self::Processing,
        Self::WaitingKey,
        Self::RetryWait,
        Self::Done,
        Self::Failed,
        Self::Dead,
    ];

    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Processing => "PROCESSING",
            Self::WaitingKey => "WAITING_KEY",
            Self::RetryWait => "RETRY_WAIT",
            Self::Done => "DONE",
            Self::Failed => "FAILED",
            Self::Dead => "DEAD",
        }
    }

    fn parse(s: &str) -> Result<Self, JobsError> {
        Self::ALL
            .into_iter()
            .find(|v| v.as_db_str() == s)
            .ok_or_else(|| JobsError::UnknownStatus(s.to_string()))
    }
}

/// One `ops.jobs` row as returned by `RETURNING j.*` (§61) — field order matches
/// `migrations/0008_ops_core.sql`'s `CREATE TABLE ops.jobs`.
#[derive(Debug, Clone)]
pub struct ClaimedJob {
    pub job_id: Uuid,
    pub tenant_id: Uuid,
    pub job_type: String,
    pub priority: i32,
    pub status: JobStatus,
    pub attempt: i32,
    pub next_retry_at: Option<OffsetDateTime>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<OffsetDateTime>,
    pub idempotency_key: String,
    /// Pipeline-job-only typed column (§15.2/§31 frozen: never JSON payload string-scanning).
    pub stream_key: Option<String>,
    /// Pipeline-job-only typed column (§15.2/§31 frozen).
    pub stream_seq: Option<i64>,
    pub payload: serde_json::Value,
    pub last_error_class: Option<String>,
    pub created_at: OffsetDateTime,
}

impl ClaimedJob {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, JobsError> {
        let status: String = row.try_get("status")?;
        Ok(Self {
            job_id: row.try_get("job_id")?,
            tenant_id: row.try_get("tenant_id")?,
            job_type: row.try_get("job_type")?,
            priority: row.try_get("priority")?,
            status: JobStatus::parse(&status)?,
            attempt: row.try_get("attempt")?,
            next_retry_at: row.try_get("next_retry_at")?,
            lease_owner: row.try_get("lease_owner")?,
            lease_expires_at: row.try_get("lease_expires_at")?,
            idempotency_key: row.try_get("idempotency_key")?,
            stream_key: row.try_get("stream_key")?,
            stream_seq: row.try_get("stream_seq")?,
            payload: row.try_get("payload")?,
            last_error_class: row.try_get("last_error_class")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

/// Sets `humaux.tenant_id` for the remainder of `txn` (`SET LOCAL` — auto-cleared at
/// commit/rollback, §62). `tenant_id`'s `Display` only ever emits the canonical
/// `8-4-4-4-12` lowercase-hex form (the `uuid` crate has no other formatter), so this string
/// build carries no injectable characters — unlike a user-supplied string, a `Uuid` value is
/// not attacker-controlled input. `SET LOCAL` does not accept a bind parameter in the
/// extended query protocol, which is why this is a formatted `simple` statement rather than
/// `sqlx::query(..).bind(..)` like every other statement in this module.
async fn set_tenant_local(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
) -> Result<(), JobsError> {
    sqlx::query(&format!("SET LOCAL humaux.tenant_id = '{tenant_id}'"))
        .execute(&mut **txn)
        .await?;
    Ok(())
}

/// Shared §61 claim implementation. `scope` only changes the closed job-type predicate;
/// leasing, attempt increment, and row locking stay identical for every typed pool.
#[derive(Clone, Copy)]
enum ClaimScope {
    Generic,
    Contribution,
}

impl ClaimScope {
    const fn predicate(self) -> &'static str {
        match self {
            Self::Generic => {
                // `DERIVED_` is excluded because those rows belong to the two derived-layer
                // workers' own cross-tenant dispatch loop ([`claim_derived_work_*`], 0164) —
                // a generic runtime claim must not steal a job whose lease the consolidation /
                // private worker is the only process able to settle.
                "LEFT(job_type, 7) <> 'PUBLIC_' AND LEFT(job_type, 8) <> 'DERIVED_' \
                 AND job_type <> 'CONTRIBUTION_EXECUTE'"
            }
            Self::Contribution => "job_type = 'CONTRIBUTION_EXECUTE'",
        }
    }
}

async fn claim_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    lease_owner: &str,
    lease_seconds: f64,
    limit: i64,
    scope: ClaimScope,
) -> Result<Vec<ClaimedJob>, JobsError> {
    set_tenant_local(txn, tenant_id).await?;
    let sql = format!(
        "WITH picked AS ( \
           SELECT job_id \
           FROM ops.jobs \
           WHERE status IN ('PENDING', 'RETRY_WAIT') \
             AND next_retry_at <= clock_timestamp() \
             AND {} \
           ORDER BY priority DESC, next_retry_at, created_at \
           FOR UPDATE SKIP LOCKED \
           LIMIT $1 \
         ) \
         UPDATE ops.jobs j \
         SET status = 'PROCESSING', \
             lease_owner = $2, \
             lease_expires_at = clock_timestamp() + make_interval(secs => $3), \
             attempt = attempt + 1 \
         FROM picked \
         WHERE j.job_id = picked.job_id \
         RETURNING j.*",
        scope.predicate()
    );
    let rows = sqlx::query(&sql)
        .bind(limit)
        .bind(lease_owner)
        .bind(lease_seconds)
        .fetch_all(&mut **txn)
        .await?;
    let claimed = rows
        .iter()
        .map(ClaimedJob::from_row)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(claimed)
}

/// Claims only non-public jobs. `attempt` increments on every successful claim and is the
/// monotonic lease fencing token returned in [`ClaimedJob`].
pub async fn claim(
    pool: &RuntimeDbPool,
    tenant_id: Uuid,
    lease_owner: &str,
    lease_seconds: f64,
    limit: i64,
) -> Result<Vec<ClaimedJob>, JobsError> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await?;
    let claimed = claim_in_txn(
        &mut txn,
        tenant_id,
        lease_owner,
        lease_seconds,
        limit,
        ClaimScope::Generic,
    )
    .await?;
    txn.commit().await?;
    Ok(claimed)
}

/// Claims only the exact private Phase 9 contribution job kind. Keeping this on the private
/// worker pool makes the job-type boundary explicit: generic runtime workers cannot steal it,
/// and the private worker cannot broaden its claim to another private job namespace.
pub async fn private_claim(
    pool: &PrivateWorkerDbPool,
    tenant_id: Uuid,
    lease_owner: &str,
    lease_seconds: f64,
    limit: i64,
) -> Result<Vec<ClaimedJob>, JobsError> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await?;
    let claimed = claim_in_txn(
        &mut txn,
        tenant_id,
        lease_owner,
        lease_seconds,
        limit,
        ClaimScope::Contribution,
    )
    .await?;
    txn.commit().await?;
    Ok(claimed)
}

/// §78.2 closed set: the derived-layer job types `migrations/0164_derived_work_dispatch.sql`'s
/// enqueue triggers emit and `ops.claim_derived_work` accepts. Pinned against the migration's own
/// `ARRAY[...]` guard by [`contract_tests::derived_job_type_matches_claim_function_guard`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedJobType {
    /// One accepted Evidence waiting for the Distill hop (`ops.outbox` `EVIDENCE_ACCEPTED`).
    Distill,
    /// One new memory making its `(tenant, reasoning_domain)` pair eligible for a rollup pass.
    Consolidate,
}

impl DerivedJobType {
    pub const ALL: [DerivedJobType; 2] = [Self::Distill, Self::Consolidate];

    pub const fn as_db_str(self) -> &'static str {
        match self {
            Self::Distill => "DERIVED_DISTILL",
            Self::Consolidate => "DERIVED_CONSOLIDATE",
        }
    }
}

/// How a derived-layer worker settles a job it claimed, expressed only in the four columns
/// §6.2.2 grants both worker roles on `ops.jobs` (`status`, `lease_owner`, `lease_expires_at`,
/// `next_retry_at` — the last one added for `role_consolidation_worker` by 0164 so a released job
/// can back off) — deliberately NOT [`fail`]'s shape, which also writes `last_error_class`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedWorkOutcome {
    /// The pass committed its business writes; the job is finished.
    Done,
    /// Environmental failure — release the lease so a later dispatch pass re-claims it, with
    /// `next_retry_at` pushed out by [`retry_backoff_seconds`] so the release is a backoff and
    /// not a spin.
    Retry,
    /// Retry budget exhausted; park the row so the loop stops spending on it.
    Dead,
}

impl DerivedWorkOutcome {
    const fn as_db_status(self) -> &'static str {
        match self {
            Self::Done => "DONE",
            Self::Retry => "PENDING",
            Self::Dead => "DEAD",
        }
    }
}

/// The cross-tenant claim (0164, ADR-0036). This is the ONE statement in the workspace that reads
/// `ops.jobs` across tenants, and it is not this process's own SQL: `ops.claim_derived_work` is a
/// SECURITY DEFINER function owned by `role_migration_owner`, whose non-spoofable `current_user`
/// arm on `jobs_tenant_isolation` is what lets it see other tenants' rows. The worker gets back
/// only the rows it just claimed, and every read/write it then performs goes through the ordinary
/// per-tenant repos with [`set_tenant_local`]'s context installed from `ClaimedJob::tenant_id`.
async fn claim_derived_work_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    kinds: &[DerivedJobType],
    lease_owner: &str,
    lease_seconds: f64,
    limit: i64,
) -> Result<Vec<ClaimedJob>, JobsError> {
    let kinds: Vec<String> = kinds
        .iter()
        .map(|k| k.as_db_str().to_owned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let rows = sqlx::query("SELECT * FROM ops.claim_derived_work($1, $2, $3, $4)")
        .bind(&kinds)
        .bind(lease_owner)
        .bind(lease_seconds)
        .bind(limit)
        .fetch_all(&mut **txn)
        .await?;
    rows.iter().map(ClaimedJob::from_row).collect()
}

/// `role_consolidation_worker`'s entry point to [`claim_derived_work_in_txn`].
pub async fn claim_derived_work_consolidation(
    pool: &ConsolidationDbPool,
    kinds: &[DerivedJobType],
    lease_owner: &str,
    lease_seconds: f64,
    limit: i64,
) -> Result<Vec<ClaimedJob>, JobsError> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await?;
    let claimed =
        claim_derived_work_in_txn(&mut txn, kinds, lease_owner, lease_seconds, limit).await?;
    txn.commit().await?;
    Ok(claimed)
}

/// The cap of [`retry_backoff_seconds`]. ADR-0058: `ops.claim_derived_work_v2`'s T6 re-queue
/// (0193) applies the same capped schedule in SQL; the contract test pins the two to one value.
pub const RETRY_BACKOFF_CAP_SECONDS: f64 = 300.0;

/// Capped exponential backoff for a job this pass is handing back, in seconds.
///
/// `attempt` is the number of times the row has already been tried (for `DERIVED_DISTILL`: admitted
/// provider calls, ADR-0058 D-F); `lease_seconds` is the base because it is the operator's
/// existing "how long is one attempt worth" dial — a released job that becomes eligible again
/// sooner than one lease could ever complete is just a spin. Without this the row kept the
/// enqueue trigger's `next_retry_at` (already in the past), so every poll burned one attempt with
/// zero delay and a tenant whose environment was not ready yet exhausted `max_attempts` in
/// seconds (ADR-0036 D5).
pub fn retry_backoff_seconds(lease_seconds: f64, attempt: i32) -> f64 {
    let doublings = attempt.clamp(1, 16) - 1;
    (lease_seconds * f64::from(2i32.pow(u32::try_from(doublings).unwrap_or(0))))
        .min(RETRY_BACKOFF_CAP_SECONDS)
}

/// ADR-0058 D-A closed set (§78.2): the call state of a claimed `DERIVED_DISTILL` job while it is
/// `PROCESSING`, in `ops.jobs_dispatch_state_check`'s order (pinned by contract test).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchState {
    /// Claimed, no provider request admitted yet (R-32 CLAIMED_NOT_DISPATCHED).
    Claimed,
    /// `ops.begin_call` admitted a request; the slot is held until a finish or `hard_deadline`.
    DispatchIntent,
    /// The lease expired while a request was admitted: the outcome is unknown and the slot is
    /// kept until `hard_deadline` (ADR-0058 D-G).
    ExecutionUncertain,
}

impl DispatchState {
    /// Every variant, in the CHECK constraint's order.
    pub const ALL: [DispatchState; 3] = [
        Self::Claimed,
        Self::DispatchIntent,
        Self::ExecutionUncertain,
    ];

    /// The `ops.jobs.dispatch_state` spelling.
    pub const fn as_db_str(self) -> &'static str {
        match self {
            Self::Claimed => "CLAIMED",
            Self::DispatchIntent => "DISPATCH_INTENT",
            Self::ExecutionUncertain => "EXECUTION_UNCERTAIN",
        }
    }
}

/// ADR-0058 D-E/D-F/D-H closed set (§78.2): how `ops.finish_derived_work_v2` settles a distill
/// claim, pinned against the function's own `ARRAY[...]` guard by contract test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistillFinish {
    /// The business writes committed in the same transaction; the job is finished.
    Done,
    /// A counted call failed; back off by the given seconds.
    Retry,
    /// Nothing was dispatched (no usable binding, domain mismatch, refused first call); backs off,
    /// and parks as `WAITING_KEY` once not ready for the park age.
    NotReady,
    /// §11: provider 401 — parked, the call's attempt reverted, never DEAD.
    WaitingKey,
    /// Attempts exhausted or a fail-closed refusal; terminal with its class.
    Dead,
}

impl DistillFinish {
    /// Every variant, in the SQL guard's order.
    pub const ALL: [DistillFinish; 5] = [
        Self::Done,
        Self::Retry,
        Self::NotReady,
        Self::WaitingKey,
        Self::Dead,
    ];

    /// The `p_outcome` spelling `ops.finish_derived_work_v2` accepts.
    pub const fn as_db_str(self) -> &'static str {
        match self {
            Self::Done => "DONE",
            Self::Retry => "RETRY",
            Self::NotReady => "NOT_READY",
            Self::WaitingKey => "WAITING_KEY",
            Self::Dead => "DEAD",
        }
    }
}

/// One `DERIVED_DISTILL` job as the v2 claim returned it (`PROCESSING` / `CLAIMED`).
#[derive(Debug, Clone)]
pub struct DistillClaim {
    /// The claimed job.
    pub job_id: Uuid,
    /// Its tenant; every later read/write installs this tenant (§62).
    pub tenant_id: Uuid,
    /// ADR-0058 D-E fencing token of this claim.
    pub claim_generation: i32,
    /// Provider requests admitted so far (counted at `begin_call`, never at claim).
    pub attempt: i32,
    /// Claims that expired before any request was admitted (D-F pre-dispatch cap).
    pub abandoned_claims: i32,
    /// The 0164 enqueue payload (`reasoning_domain_id`, `evidence_id`).
    pub payload: serde_json::Value,
    /// Lease end as claimed.
    pub lease_expires_at: OffsetDateTime,
    /// End of this claim: the lease never passes it.
    pub hard_deadline: OffsetDateTime,
    /// Class of the previous settle, if any.
    pub last_error_class: Option<String>,
    /// Since when the job has been not ready (D-H park age), if it is.
    pub not_ready_since: Option<OffsetDateTime>,
    /// The ledger row of the claim's last admitted request — after a T6 re-queue, the call whose
    /// outcome stayed unknown.
    pub dispatch_model_call_id: Option<Uuid>,
}

impl DistillClaim {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, JobsError> {
        Ok(Self {
            job_id: row.try_get("job_id")?,
            tenant_id: row.try_get("tenant_id")?,
            claim_generation: row.try_get("claim_generation")?,
            attempt: row.try_get("attempt")?,
            abandoned_claims: row.try_get("abandoned_claims")?,
            payload: row.try_get("payload")?,
            lease_expires_at: row.try_get("lease_expires_at")?,
            hard_deadline: row.try_get("hard_deadline")?,
            last_error_class: row.try_get("last_error_class")?,
            not_ready_since: row.try_get("not_ready_since")?,
            dispatch_model_call_id: row.try_get("dispatch_model_call_id")?,
        })
    }
}

/// The exact distill lease a seat holds: identity plus the claim generation (ADR-0058 D-E).
#[derive(Debug, Clone, Copy)]
pub struct DistillLease<'a> {
    /// The claimed job.
    pub job_id: Uuid,
    /// Its tenant.
    pub tenant_id: Uuid,
    /// This process's `lease_owner`.
    pub lease_owner: &'a str,
    /// The generation the claim returned; a superseded one renews, dispatches and settles nothing.
    pub claim_generation: i32,
}

impl<'a> DistillLease<'a> {
    /// The lease exactly as [`claim_distill`] handed it back.
    pub fn of(claim: &DistillClaim, lease_owner: &'a str) -> Self {
        Self {
            job_id: claim.job_id,
            tenant_id: claim.tenant_id,
            lease_owner,
            claim_generation: claim.claim_generation,
        }
    }
}

/// ADR-0058 T1: claims at most one `DERIVED_DISTILL` job through the tenant-fair, slot-bounded
/// owner definer (which also sweeps expired claims). `None` = no free slot or no READY work.
pub async fn claim_distill(
    pool: &PrivateWorkerDbPool,
    lease_owner: &str,
    lease_seconds: f64,
    hard_deadline_seconds: f64,
) -> Result<Option<DistillClaim>, JobsError> {
    // dep: PostgreSQL(any) — executes ops.claim_derived_work_v2 against the pool
    let row = sqlx::query("SELECT * FROM ops.claim_derived_work_v2($1, $2, $3)")
        .bind(lease_owner)
        .bind(lease_seconds)
        .bind(hard_deadline_seconds)
        .fetch_optional(pool.pool())
        .await?;
    row.as_ref().map(DistillClaim::from_row).transpose()
}

/// ADR-0058 R5: whether every provider slot was bound when read (MVCC, no lock) — the drain's
/// reason for an empty [`claim_distill`] answer (`no_slot`), as opposed to no READY job it could
/// take (`no_work`).
pub async fn distill_slots_all_bound(pool: &PrivateWorkerDbPool) -> Result<bool, JobsError> {
    // dep: PostgreSQL(any) — executes ops.distill_slots_all_bound against the pool
    Ok(sqlx::query_scalar("SELECT ops.distill_slots_all_bound()")
        .fetch_one(pool.pool())
        .await?)
}

/// ADR-0058 T7: generation-fenced heartbeat. `Ok(None)` = the lease is lost (superseded
/// generation, expired lease, or `hard_deadline` reached); `Err` = transient DB failure.
pub async fn renew_distill_lease(
    pool: &PrivateWorkerDbPool,
    lease: &DistillLease<'_>,
    lease_seconds: f64,
) -> Result<Option<OffsetDateTime>, JobsError> {
    // dep: PostgreSQL(any) — executes ops.renew_lease against the pool
    let until: Option<OffsetDateTime> =
        sqlx::query_scalar("SELECT ops.renew_lease($1, $2, $3, $4, $5)")
            .bind(lease.job_id)
            .bind(lease.tenant_id)
            .bind(lease.lease_owner)
            .bind(lease.claim_generation)
            .bind(lease_seconds)
            .fetch_one(pool.pool())
            .await?;
    Ok(until)
}

/// §72.3 tenant distill budget (ADR-0058 D-T): at most `max_calls` admitted requests of one tenant
/// whose `ops.distill_calls.begun_at` lies in the last `window_seconds`. Deployment configuration
/// (`HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_*`, §78.1), passed into `ops.admit_distill_budget`.
// ponytail: one deployment-wide window/limit counting requests, not tokens (ADR-0058 L18); per-plan
// limits from the entitlement snapshot and ledger-token weighting when card 38 adds the breaker.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DistillCallBudget {
    /// Length of the sliding window, seconds (> 0).
    pub window_seconds: f64,
    /// Admitted requests of one tenant allowed inside the window (>= 1).
    pub max_calls: i32,
}

/// What [`begin_distill_call`] decided about one provider request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallAdmission {
    /// The request may leave; the job's attempt count after counting it.
    Admitted(i32),
    /// The claim's fence refused it (owner, generation, lease, slot or deadline): no request.
    Refused,
    /// The tenant's §72.3 budget is spent for now: no request, nothing counted, no row written.
    OverBudget,
}

/// ADR-0058 T2 behind the D-T budget: ONE transaction runs `ops.admit_distill_budget` (which holds
/// the tenant's budget lock to commit) and then `ops.begin_call` (attempt + 1, `DISPATCH_INTENT`,
/// one `ops.distill_calls` row naming `model_call_id`). Every request of a claim passes here — the
/// first one, a re-ask and the T6 resend after `EXECUTION_UNCERTAIN` (main-line ruling E1 guard d).
/// `min_remaining_seconds` is the HTTP window plus one lease for the post-call legs (D-J).
pub async fn begin_distill_call(
    pool: &PrivateWorkerDbPool,
    lease: &DistillLease<'_>,
    model_call_id: Uuid,
    min_remaining_seconds: f64,
    budget: DistillCallBudget,
) -> Result<CallAdmission, JobsError> {
    // dep: PostgreSQL(any) — opens the admission transaction
    let mut txn = pool.pool().begin().await?;
    // dep: PostgreSQL(any) — executes ops.admit_distill_budget inside the admission transaction
    let within: bool = sqlx::query_scalar("SELECT ops.admit_distill_budget($1, $2, $3)")
        .bind(lease.tenant_id)
        .bind(budget.window_seconds)
        .bind(budget.max_calls)
        .fetch_one(&mut *txn)
        .await?;
    if !within {
        txn.rollback().await?;
        return Ok(CallAdmission::OverBudget);
    }
    // dep: PostgreSQL(any) — executes ops.begin_call inside the admission transaction
    let attempt: Option<i32> = sqlx::query_scalar("SELECT ops.begin_call($1, $2, $3, $4, $5, $6)")
        .bind(lease.job_id)
        .bind(lease.tenant_id)
        .bind(lease.lease_owner)
        .bind(lease.claim_generation)
        .bind(model_call_id)
        .bind(min_remaining_seconds)
        .fetch_one(&mut *txn)
        .await?;
    txn.commit().await?;
    Ok(attempt.map_or(CallAdmission::Refused, CallAdmission::Admitted))
}

/// ADR-0058 T3 inside a CALLER-owned transaction: the first statement of the worker's settle
/// transaction, so the business writes and the settle commit or roll back together. `false` =
/// the generation was superseded (or the job already settled): the caller must roll back.
/// Frees the claim's slot. `error_class` is ignored for [`DistillFinish::Done`].
pub(crate) async fn finish_distill_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    lease: &DistillLease<'_>,
    outcome: DistillFinish,
    error_class: Option<&str>,
    backoff_seconds: f64,
    park_seconds: f64,
) -> Result<bool, JobsError> {
    // dep: PostgreSQL(any) — executes ops.finish_derived_work_v2 against the pool
    let settled: bool =
        sqlx::query_scalar("SELECT ops.finish_derived_work_v2($1, $2, $3, $4, $5, $6, $7, $8)")
            .bind(lease.job_id)
            .bind(lease.tenant_id)
            .bind(lease.lease_owner)
            .bind(lease.claim_generation)
            .bind(outcome.as_db_str())
            .bind(error_class)
            .bind(backoff_seconds)
            .bind(park_seconds)
            .fetch_one(&mut **txn)
            .await?;
    Ok(settled)
}

/// ADR-0058 T3 for a settle that carries no business write (RETRY, NOT_READY, WAITING_KEY, a
/// DEAD whose outbox row is settled elsewhere): [`finish_distill_in_txn`] in its own transaction.
pub async fn finish_distill(
    pool: &PrivateWorkerDbPool,
    lease: &DistillLease<'_>,
    outcome: DistillFinish,
    error_class: Option<&str>,
    backoff_seconds: f64,
    park_seconds: f64,
) -> Result<bool, JobsError> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await?;
    let settled = finish_distill_in_txn(
        &mut txn,
        lease,
        outcome,
        error_class,
        backoff_seconds,
        park_seconds,
    )
    .await?;
    txn.commit().await?;
    Ok(settled)
}

/// The `lease_owner` + `attempt` pair IS the fence: the claim bumps `attempt` and rewrites
/// `lease_owner`, so a settle from a worker whose job somebody else re-claimed matches no row.
/// There is deliberately NO `lease_expires_at > clock_timestamp()` predicate here — it added
/// nothing the fencing token did not already guarantee, and it actively rejected the settle of
/// the ONE worker still legitimately holding the job whenever its own run outlived the lease,
/// leaving the row PROCESSING-with-expired-lease for the next pass to redo (ADR-0036 D4: the
/// double-rollup path).
async fn settle_derived_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    lease: &DerivedLease<'_>,
    outcome: DerivedWorkOutcome,
    lease_seconds: f64,
) -> Result<bool, JobsError> {
    set_tenant_local(txn, lease.tenant_id).await?;
    let result = sqlx::query(
        "UPDATE ops.jobs \
         SET status = $5, \
             lease_owner = CASE WHEN $5 = 'DONE' THEN lease_owner ELSE NULL END, \
             lease_expires_at = CASE WHEN $5 = 'DONE' THEN lease_expires_at ELSE NULL END, \
             next_retry_at = CASE WHEN $5 = 'PENDING' \
                                  THEN clock_timestamp() + make_interval(secs => $6) \
                                  ELSE next_retry_at END \
         WHERE job_id = $1 AND tenant_id = $2 AND lease_owner = $3 AND attempt = $4 \
           AND status = 'PROCESSING'",
    )
    .bind(lease.job_id)
    .bind(lease.tenant_id)
    .bind(lease.lease_owner)
    .bind(lease.attempt)
    .bind(outcome.as_db_status())
    .bind(retry_backoff_seconds(lease_seconds, lease.attempt))
    .execute(&mut **txn)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// The exact lease a derived-layer worker holds: identity plus the monotonic fencing token the
/// claim returned. Grouped so the settle/heartbeat calls stay under clippy's argument-count lint.
pub struct DerivedLease<'a> {
    pub tenant_id: Uuid,
    pub job_id: Uuid,
    pub lease_owner: &'a str,
    /// `attempt` as the claim returned it — a stale token settles nothing.
    pub attempt: i32,
}

impl<'a> DerivedLease<'a> {
    /// The lease exactly as [`claim_derived_work_consolidation`]
    /// handed it back, so no call site can retype the fencing token by hand.
    pub fn of(job: &ClaimedJob, lease_owner: &'a str) -> Self {
        Self {
            tenant_id: job.tenant_id,
            job_id: job.job_id,
            lease_owner,
            attempt: job.attempt,
        }
    }
}

/// Post-claim lease refresh, under NORMAL RLS with the claimed job's own tenant installed —
/// that is the whole point of the split: only the discovery read is privileged.
pub async fn heartbeat_derived_consolidation(
    pool: &ConsolidationDbPool,
    lease: &DerivedLease<'_>,
    lease_seconds: f64,
) -> Result<bool, JobsError> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await?;
    let changed = heartbeat_in_txn(
        &mut txn,
        lease.tenant_id,
        lease.job_id,
        lease.lease_owner,
        lease.attempt,
        lease_seconds,
    )
    .await?;
    txn.commit().await?;
    Ok(changed)
}

pub async fn settle_derived_consolidation(
    pool: &ConsolidationDbPool,
    lease: &DerivedLease<'_>,
    outcome: DerivedWorkOutcome,
    lease_seconds: f64,
) -> Result<bool, JobsError> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await?;
    let settled = settle_derived_in_txn(&mut txn, lease, outcome, lease_seconds).await?;
    txn.commit().await?;
    Ok(settled)
}

/// The terminal `DONE` transition inside a CALLER-owned business transaction, so the business
/// write and the job's settle commit or roll back together.
///
/// This is what makes "the result is written exactly once" structural rather than timing-
/// dependent: `consolidate_repo::publish_rollup` writes the rollup and settles the job in ONE
/// transaction, so a worker whose job somebody else re-claimed mid-run (its `attempt` bumped)
/// rolls its own rollup back instead of publishing a second one over the same inputs. `false`
/// means the caller must abort — not retry — its business write.
///
/// `DONE` only: a released (`PENDING`) job needs the backoff argument, and no business write
/// should be committing alongside a release.
pub(crate) async fn settle_derived_done_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    lease: &DerivedLease<'_>,
) -> Result<bool, JobsError> {
    settle_derived_in_txn(txn, lease, DerivedWorkOutcome::Done, 0.0).await
}

/// Locks and verifies one exact live lease inside a caller-owned business transaction.
/// A `false` result means the caller must not perform its final business write.
#[allow(dead_code)] // Consumed by the Phase 9 workflow finalizer in a sibling adapter module.
pub(crate) async fn lock_current_lease(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    job_id: Uuid,
    lease_owner: &str,
    attempt: i32,
) -> Result<bool, JobsError> {
    set_tenant_local(txn, tenant_id).await?;
    let locked = sqlx::query_scalar::<_, i32>(
        "SELECT 1 FROM ops.jobs \
         WHERE job_id = $1 AND tenant_id = $2 AND lease_owner = $3 AND attempt = $4 \
           AND status = 'PROCESSING' AND lease_expires_at > clock_timestamp() \
         FOR UPDATE",
    )
    .bind(job_id)
    .bind(tenant_id)
    .bind(lease_owner)
    .bind(attempt)
    .fetch_optional(&mut **txn)
    .await?;
    Ok(locked.is_some())
}

/// Extends a held lease (worker heartbeat). Only takes effect while `job_id` is still
/// `PROCESSING` under `lease_owner` — if a maintenance reaper already reclaimed the lease
/// (expired, requeued to another owner) this returns `Ok(false)` instead of resurrecting a
/// lease the caller no longer legitimately holds, so a late heartbeat from a straggling
/// worker is observably a no-op, not a silent success.
pub async fn heartbeat(
    pool: &RuntimeDbPool,
    tenant_id: Uuid,
    job_id: Uuid,
    lease_owner: &str,
    attempt: i32,
    lease_seconds: f64,
) -> Result<bool, JobsError> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await?;
    let changed = heartbeat_in_txn(
        &mut txn,
        tenant_id,
        job_id,
        lease_owner,
        attempt,
        lease_seconds,
    )
    .await?;
    txn.commit().await?;
    Ok(changed)
}

async fn heartbeat_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    job_id: Uuid,
    lease_owner: &str,
    attempt: i32,
    lease_seconds: f64,
) -> Result<bool, JobsError> {
    set_tenant_local(txn, tenant_id).await?;
    let result = sqlx::query(
        "UPDATE ops.jobs \
         SET lease_expires_at = clock_timestamp() + make_interval(secs => $5) \
         WHERE job_id = $1 AND tenant_id = $2 AND lease_owner = $3 AND attempt = $4 \
           AND status = 'PROCESSING' AND lease_expires_at > clock_timestamp()",
    )
    .bind(job_id)
    .bind(tenant_id)
    .bind(lease_owner)
    .bind(attempt)
    .bind(lease_seconds)
    .execute(&mut **txn)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// `PROCESSING` -> `DONE` under the exact still-live fencing token.
pub async fn complete(
    pool: &RuntimeDbPool,
    tenant_id: Uuid,
    job_id: Uuid,
    lease_owner: &str,
    attempt: i32,
) -> Result<bool, JobsError> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await?;
    let done = complete_in_txn(&mut txn, tenant_id, job_id, lease_owner, attempt).await?;
    txn.commit().await?;
    Ok(done)
}

/// Completes a job in the caller's transaction after its business writes. The exact token,
/// owner, status, tenant, and live lease are all rechecked at the final transition.
pub(crate) async fn complete_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    job_id: Uuid,
    lease_owner: &str,
    attempt: i32,
) -> Result<bool, JobsError> {
    set_tenant_local(txn, tenant_id).await?;
    let result = sqlx::query(
        "UPDATE ops.jobs SET status = 'DONE' \
         WHERE job_id = $1 AND tenant_id = $2 AND lease_owner = $3 AND attempt = $4 \
           AND status = 'PROCESSING' AND lease_expires_at > clock_timestamp()",
    )
    .bind(job_id)
    .bind(tenant_id)
    .bind(lease_owner)
    .bind(attempt)
    .execute(&mut **txn)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// [`fail`] input, grouped so the function itself stays under clippy's argument-count lint —
/// `job_id`/`lease_owner` identify the row, the rest is the caller's per-job/per-error retry
/// policy (§31 "所有 Job 必须 ... bounded/retryable": which knobs are policy, not this
/// module's business).
pub struct FailInput<'a> {
    pub job_id: Uuid,
    pub lease_owner: &'a str,
    /// Monotonic token returned by the claim this worker is finalizing.
    pub attempt: i32,
    pub error_class: &'a str,
    /// `false` -> this error class is permanent, skip straight to `FAILED` regardless of
    /// remaining budget.
    pub retryable: bool,
    /// `RETRY_WAIT` is only reachable while `attempt < max_attempts`; at or past it, `DEAD`.
    pub max_attempts: i32,
    pub retry_after_seconds: f64,
}

/// `PROCESSING` -> one of `FAILED` / `RETRY_WAIT` / `DEAD` under the exact live token.
pub async fn fail(
    pool: &RuntimeDbPool,
    tenant_id: Uuid,
    input: FailInput<'_>,
) -> Result<Option<JobStatus>, JobsError> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await?;
    let status = fail_in_txn(&mut txn, tenant_id, input).await?;
    txn.commit().await?;
    Ok(status)
}

async fn fail_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    input: FailInput<'_>,
) -> Result<Option<JobStatus>, JobsError> {
    set_tenant_local(txn, tenant_id).await?;
    let row = sqlx::query(
        "UPDATE ops.jobs \
         SET status = CASE \
                        WHEN NOT $6 THEN 'FAILED' \
                        WHEN attempt >= $7 THEN 'DEAD' \
                        ELSE 'RETRY_WAIT' \
                      END, \
             next_retry_at = CASE \
                        WHEN NOT $6 THEN next_retry_at \
                        WHEN attempt >= $7 THEN next_retry_at \
                        ELSE clock_timestamp() + make_interval(secs => $8) \
                      END, \
             last_error_class = $5 \
         WHERE job_id = $1 AND tenant_id = $2 AND lease_owner = $3 AND attempt = $4 \
           AND status = 'PROCESSING' AND lease_expires_at > clock_timestamp() \
         RETURNING status",
    )
    .bind(input.job_id)
    .bind(tenant_id)
    .bind(input.lease_owner)
    .bind(input.attempt)
    .bind(input.error_class)
    .bind(input.retryable)
    .bind(input.max_attempts)
    .bind(input.retry_after_seconds)
    .fetch_optional(&mut **txn)
    .await?;
    row.map(|r| JobStatus::parse(&r.try_get::<String, _>("status")?))
        .transpose()
}

/// `PROCESSING` -> `WAITING_KEY`. Does not touch `attempt` (§31 "WAITING_KEY 不消耗 retry") —
/// pairs with [`resume_from_waiting_key`], which also never touches it; the counter only ever
/// moves inside [`claim`].
pub async fn mark_waiting_key(
    pool: &RuntimeDbPool,
    tenant_id: Uuid,
    job_id: Uuid,
    lease_owner: &str,
    attempt: i32,
) -> Result<bool, JobsError> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, tenant_id).await?;
    let result = sqlx::query(
        "UPDATE ops.jobs SET status = 'WAITING_KEY' \
         WHERE job_id = $1 AND tenant_id = $2 AND lease_owner = $3 AND attempt = $4 \
           AND status = 'PROCESSING' AND lease_expires_at > clock_timestamp()",
    )
    .bind(job_id)
    .bind(tenant_id)
    .bind(lease_owner)
    .bind(attempt)
    .execute(&mut *txn)
    .await?;
    txn.commit().await?;
    Ok(result.rows_affected() > 0)
}

/// `WAITING_KEY` -> `PENDING`, releasing the lease and setting `next_retry_at = now()` so the
/// row is immediately visible to the next [`claim`] (see this module's doc comment on why
/// `next_retry_at` must be non-`NULL` to ever be claimable). Does not touch `attempt` — the
/// full `PROCESSING -> WAITING_KEY -> PENDING` round trip this function completes leaves
/// `attempt` exactly where [`claim`] last left it, which is the observable form of "WAITING_KEY
/// 不消耗 retry" (only a subsequent [`claim`] call, not this transition, would move it).
pub async fn resume_from_waiting_key(
    pool: &RuntimeDbPool,
    tenant_id: Uuid,
    job_id: Uuid,
) -> Result<bool, JobsError> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, tenant_id).await?;

    let result = sqlx::query(
        "UPDATE ops.jobs \
         SET status = 'PENDING', lease_owner = NULL, lease_expires_at = NULL, next_retry_at = now() \
         WHERE job_id = $1 AND status = 'WAITING_KEY'",
    )
    .bind(job_id)
    .execute(&mut *txn)
    .await?;
    txn.commit().await?;
    Ok(result.rows_affected() > 0)
}

/// §78.2 "DB enum 与 Rust enum 走 contract test 对账": [`JobStatus::ALL`] must list exactly
/// `ops.jobs_status_check`'s values, in order, both directions. Runs against the real
/// migration file text, not a live DB, so it always executes.
#[cfg(test)]
mod contract_tests {
    use super::*;

    const MIGRATION_SQL: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations/0008_ops_core.sql"
    ));

    /// Scopes to just the `CREATE TABLE ops.jobs ( ... )` block — `0008_ops_core.sql` also
    /// defines `ops.outbox`, which has its own unrelated `status IN (...)` CHECK earlier in
    /// the file, so a whole-file search for `status IN` would grab the wrong table's list.
    fn jobs_table_sql() -> &'static str {
        let start = MIGRATION_SQL
            .find("CREATE TABLE ops.jobs (")
            .expect("migration must define ops.jobs");
        let end = MIGRATION_SQL[start..]
            .find(");\n")
            .expect("unterminated ops.jobs table definition")
            + start;
        &MIGRATION_SQL[start..end]
    }

    fn check_values(table_sql: &str, column: &str) -> Vec<String> {
        let needle = format!("{column} IN");
        let after_needle = table_sql
            .find(&needle)
            .unwrap_or_else(|| panic!("ops.jobs has no `{column} IN (...)` CHECK clause"))
            + needle.len();
        let open = table_sql[after_needle..]
            .find('(')
            .expect("CHECK IN clause missing opening paren")
            + after_needle
            + 1;
        let close = table_sql[open..]
            .find(')')
            .expect("unterminated CHECK IN (...) clause")
            + open;
        table_sql[open..close]
            .split(',')
            .map(|s| s.trim().trim_matches('\'').to_string())
            .collect()
    }

    const DISPATCH_MIGRATION_SQL: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations/0164_derived_work_dispatch.sql"
    ));

    /// §78.2: [`DerivedJobType::ALL`] must equal the closed `ARRAY[...]` guard inside
    /// `ops.claim_derived_work` verbatim, in order. Reads the migration text so it always runs.
    #[test]
    fn derived_job_type_matches_claim_function_guard() {
        let needle = "p_job_types <@ ARRAY[";
        let open = DISPATCH_MIGRATION_SQL
            .find(needle)
            .expect("0164 must guard p_job_types with a closed ARRAY[...] literal")
            + needle.len();
        let close = DISPATCH_MIGRATION_SQL[open..]
            .find(']')
            .expect("unterminated ARRAY[...] guard")
            + open;
        let db: Vec<String> = DISPATCH_MIGRATION_SQL[open..close]
            .split(',')
            .map(|s| s.trim().trim_matches('\'').to_string())
            .collect();
        let rust: Vec<String> = DerivedJobType::ALL
            .iter()
            .map(|t| t.as_db_str().to_string())
            .collect();
        assert_eq!(
            db, rust,
            "DerivedJobType::ALL must list exactly ops.claim_derived_work's accepted types"
        );
    }

    const CUTOVER_MIGRATION_SQL: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations/0193_private_worker_off_claim_v1.sql"
    ));

    /// ADR-0058 D-L: 0193 replaces the v1 claim with a body that refuses `DERIVED_DISTILL` before
    /// any row is read, and takes the private worker off its EXECUTE list.
    #[test]
    fn v1_claim_refuses_distill_after_the_cutover() {
        let v1 = &CUTOVER_MIGRATION_SQL[CUTOVER_MIGRATION_SQL
            .find("CREATE OR REPLACE FUNCTION ops.claim_derived_work(")
            .expect("0193 must replace ops.claim_derived_work")..];
        let refusal = v1
            .find("IF 'DERIVED_DISTILL' = ANY (p_job_types) THEN")
            .expect("0193 must refuse DERIVED_DISTILL in the v1 claim");
        assert!(
            refusal < v1.find("RETURN QUERY").expect("v1 claim body"),
            "the refusal must run before the claim statement"
        );
        assert!(CUTOVER_MIGRATION_SQL.contains(
            "REVOKE EXECUTE ON FUNCTION ops.claim_derived_work(text[], text, double precision, bigint)\nFROM role_private_worker;"
        ));
        assert_eq!(
            DerivedJobType::Distill.as_db_str(),
            "DERIVED_DISTILL",
            "the refused literal is the Rust spelling"
        );
    }

    /// ADR-0058 E1 guard (b): the T6 re-queue uses the same cap as [`retry_backoff_seconds`].
    #[test]
    fn t6_requeue_uses_the_retry_backoff_cap() {
        let cap = format!("16) - 1), {})", RETRY_BACKOFF_CAP_SECONDS as i64);
        assert!(
            CUTOVER_MIGRATION_SQL.contains(&cap),
            "0193's T6 backoff must be capped at RETRY_BACKOFF_CAP_SECONDS ({cap})"
        );
        assert_eq!(retry_backoff_seconds(30.0, 1), 30.0);
        assert_eq!(retry_backoff_seconds(30.0, 3), 120.0);
        assert_eq!(retry_backoff_seconds(30.0, 9), RETRY_BACKOFF_CAP_SECONDS);
    }

    /// Both enqueue triggers must emit a `job_type` the claim guard accepts — otherwise a row is
    /// written that nothing can ever claim.
    #[test]
    fn enqueue_triggers_emit_only_claimable_job_types() {
        for kind in DerivedJobType::ALL {
            assert!(
                DISPATCH_MIGRATION_SQL.contains(&format!("'{}', 'PENDING'", kind.as_db_str())),
                "0164 has no enqueue site for {}",
                kind.as_db_str()
            );
        }
    }

    /// The `DERIVED_` namespace must stay outside the generic claim: `role_gateway`'s
    /// [`claim`] cannot settle a derived job (it has no route to the derived repos), so stealing
    /// one would wedge the tenant's derived layer until the lease expired.
    #[test]
    fn generic_claim_excludes_the_derived_namespace() {
        let predicate = ClaimScope::Generic.predicate();
        assert!(predicate.contains("LEFT(job_type, 8) <> 'DERIVED_'"));
        for kind in DerivedJobType::ALL {
            assert!(kind.as_db_str().starts_with("DERIVED_"));
        }
    }

    #[test]
    fn job_status_matches_check_constraint() {
        let db = check_values(jobs_table_sql(), "status");
        let rust: Vec<String> = JobStatus::ALL
            .iter()
            .map(|s| s.as_db_str().to_string())
            .collect();
        assert_eq!(
            db, rust,
            "JobStatus::ALL must list every status in DB order"
        );
    }
}
