//! `jobs` — `ops.jobs` SKIP LOCKED claim, lease heartbeat, and terminal-state transitions
//! (§31 Durable Jobs / §61 SKIP LOCKED Claim SQL).
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
//! transaction. Tenant fairness *across* tenants (round-robining `claim` calls with cost
//! weighting) is §32's Tenant Fair Scheduler, a separate task; this module is the primitive it
//! will call once per eligible tenant.
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

use crate::postgres::{PrivateWorkerDbPool, RuntimeDbPool};

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
                "LEFT(job_type, 7) <> 'PUBLIC_' AND job_type <> 'CONTRIBUTION_EXECUTE'"
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
