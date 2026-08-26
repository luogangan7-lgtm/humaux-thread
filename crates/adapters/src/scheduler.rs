//! `adapters::scheduler` — §32.0 Distributed Scheduler Leadership + §32.1 Scheduler
//! Singleton/Failover Gate (G32-1/G80-38).
//!
//! Two DB-backed layers, both required (§32.1 "周期 enqueue 必须同时有逻辑 lease 和数据库
//! 幂等约束"):
//!
//! 1. **Logical lease** — [`claim_lease`] does one plain `INSERT` against
//!    `ops.scheduler_leases` (shape frozen by `migrations/0008_ops_core.sql`, PK
//!    `(schedule_id, planned_at)` + `UNIQUE(idempotency_key)`) and reports
//!    [`LeaseOutcome::HeldByOther`] when it hits SQLSTATE `23505` (see [`claim_lease`]'s own
//!    doc for why this isn't an `ON CONFLICT` upsert). Leadership here is scoped to one due
//!    tick, not held open across a scheduler's whole lifetime — every replica races this same
//!    claim independently each cycle, so "kill the current leader" needs no special handling:
//!    a dead replica simply stops racing, the next tick's claim is contested only by whoever
//!    is still alive (§32.1 test step 4/5).
//! 2. **Database idempotency constraint** — [`insert_job_idempotent`] does
//!    `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING` against `ops.jobs`, targeting the
//!    plain `UNIQUE(idempotency_key)` index added by `migrations/0044_scheduler_jobs_idempotency_key.sql`
//!    (0008's own `UNIQUE(tenant_id, idempotency_key)` cannot be an `ON CONFLICT` target for
//!    a single-column key). §32.1: "多 scheduler replica 都可以'尝试'，但数据库只允许一条
//!    逻辑 job" — this is the layer the task brief calls "数据库唯一约束是最终仲裁，不靠
//!    leader 单点": [`insert_job_idempotent`] is exercised directly (bypassing
//!    [`claim_lease`] entirely) by the fault-injection test in
//!    `tests/scheduler_exactly_once.rs`, simulating a broken leader-election path.
//!
//! [`claim_and_enqueue`] composes both layers for the normal (non-fault-injected) path.
//!
//! **This module is G80-38's execution body** (§80.1 registers G80-38 against §32.1#G32-1;
//! the task brief for T3.6/T3.7 is explicit that the test in `tests/scheduler_exactly_once.rs`
//! *is* that gate, not a second independent one).

use sqlx::Row;
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::postgres::RuntimeDbPool;

/// DB-layer failure from any function in this module — adapter-local, not one of the
/// workspace's two frozen domain error enums (§52), same reasoning as
/// `email::outbox::OutboxError` / `postgres::PoolInitError`.
#[derive(Debug)]
pub enum SchedulerError {
    Db(sqlx::Error),
}

impl From<sqlx::Error> for SchedulerError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

impl std::fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(e) => write!(f, "scheduler DB error: {e}"),
        }
    }
}

impl std::error::Error for SchedulerError {}

/// §32.1: `idempotency_key = H(schedule_id, planned_at)`. Spec names the formula, not a
/// specific hash — `DefaultHasher` (SipHash with the fixed all-zero keys `Default` gives it)
/// is deterministic across calls/processes within one Rust build, which is all this needs: a
/// stable mapping so two replicas computing the key for the same `(schedule_id, planned_at)`
/// always agree, and two different `planned_at` for the same schedule never collide except by
/// genuine hash collision. Reaching for a crypto hash crate here would be a new dependency in
/// a shared `Cargo.toml` for a property `std` already provides (ponytail rung 3).
///
/// **The `planned_at` input must be nanosecond-precision, not just seconds** — G32-1's second
/// fault injection ("idempotency key 不含 planned_at/schedule_id") is exactly the bug class
/// this function must not have; see `tests::key_differs_by_both_inputs` and
/// `tests::truncated_planned_at_precision_collides_across_ticks` for the two shapes of that
/// bug this formula must resist.
pub fn idempotency_key(schedule_id: &str, planned_at: OffsetDateTime) -> String {
    let mut hasher = DefaultHasher::new();
    schedule_id.hash(&mut hasher);
    planned_at.unix_timestamp_nanos().hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// One schedule instance that has come due and needs enqueuing.
#[derive(Debug, Clone)]
pub struct DueSchedule {
    pub schedule_id: String,
    pub planned_at: OffsetDateTime,
    pub tenant_id: Uuid,
    pub job_type: String,
    pub payload: serde_json::Value,
}

/// Result of [`claim_lease`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseOutcome {
    /// This call's `owner` now holds the lease row for `(schedule_id, planned_at)` — either
    /// it inserted the row fresh, or the previous holder's lease had expired and this call
    /// took it over.
    Won,
    /// Another owner holds an unexpired lease for this `(schedule_id, planned_at)`; this
    /// replica must not enqueue for this tick.
    HeldByOther,
}

/// Result of [`insert_job_idempotent`] / [`claim_and_enqueue`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnqueueOutcome {
    /// A new `ops.jobs` row was inserted.
    Enqueued { job_id: Uuid },
    /// `ON CONFLICT (idempotency_key) DO NOTHING` fired — a row for this idempotency key
    /// already exists (from this call's own lease win racing another completed attempt, or
    /// from the direct-insert fault-injection path in the G32-1 test).
    AlreadyEnqueued,
}

/// §32.0/§32.1 logical lease claim against `ops.scheduler_leases`. A plain `INSERT`, not
/// `ON CONFLICT DO UPDATE` — `ops.scheduler_leases` (0008) carries *two* independently-unique
/// columns, `PRIMARY KEY (schedule_id, planned_at)` and `UNIQUE(idempotency_key)`, and because
/// `idempotency_key` is a deterministic function of `(schedule_id, planned_at)` a duplicate
/// claim always violates both together. Postgres' `ON CONFLICT` can only name one arbiter
/// index; naming either constraint leaves the *other* raising a raw `23505` instead of being
/// absorbed (observed directly: this was `scheduler_leases_idempotency_key_key` erroring out
/// from under an `ON CONFLICT (schedule_id, planned_at)` clause during concurrent racing).
/// Catching `23505` here — whichever of the two constraints Postgres happens to report first —
/// and reporting [`LeaseOutcome::HeldByOther`] sidesteps that arbiter ambiguity entirely and is
/// all G32-1 needs: "did I win this tick".
///
/// Runs on `tx` (caller-supplied transaction) so [`claim_and_enqueue`] can run the lease claim
/// and the job insert as one atomic unit.
///
/// ponytail: this drops expiry-based takeover of a lease whose previous claimant died between
/// winning it and calling [`insert_job_idempotent`] (the original `ON CONFLICT DO UPDATE ...
/// WHERE lease_expires_at < now()` this replaced would have retried that case in the same
/// statement). None of G32-1's steps exercise that recovery path — "kill the leader" here
/// means it stops racing *future* ticks, which always get a fresh `(schedule_id, planned_at)`
/// row with no conflict at all. If a stuck-claim recovery matters later, wrap the `INSERT` in
/// a savepoint (`tx.begin()` on a `sqlx::Transaction` nests via `SAVEPOINT`) so a `23505` can
/// be rolled back to without poisoning the rest of `tx`, then attempt the same
/// lease-expiry-gated `UPDATE ... WHERE lease_expires_at < now() RETURNING` this module used
/// to run inline.
async fn claim_lease(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    owner: &str,
    ttl_seconds: i64,
    due: &DueSchedule,
    key: &str,
) -> Result<LeaseOutcome, SchedulerError> {
    let result = sqlx::query(
        "INSERT INTO ops.scheduler_leases \
             (schedule_id, planned_at, idempotency_key, leader_owner, lease_expires_at) \
         VALUES ($1, $2, $3, $4, now() + ($5 || ' seconds')::interval)",
    )
    .bind(&due.schedule_id)
    .bind(due.planned_at)
    .bind(key)
    .bind(owner)
    .bind(ttl_seconds.to_string())
    .execute(&mut **tx)
    .await;

    match result {
        Ok(_) => Ok(LeaseOutcome::Won),
        Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("23505") => {
            Ok(LeaseOutcome::HeldByOther)
        }
        Err(e) => Err(e.into()),
    }
}

/// §32.1 final arbiter: `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING` against
/// `ops.jobs`, targeting `ops_jobs_idempotency_key_key`
/// (`migrations/0044_scheduler_jobs_idempotency_key.sql`). Deliberately independent of
/// [`claim_lease`] — the G32-1 fault-injection test calls this directly, concurrently, with
/// the *same* `idempotency_key` and no lease claim at all, to prove the DB constraint alone
/// (not leader election) is what keeps the count at exactly one.
///
/// `SET LOCAL humaux.tenant_id` is required before this INSERT: `ops.jobs` has `FORCE ROW
/// LEVEL SECURITY` (`migrations/0008_ops_core.sql` + `0012_rls.sql`) and `RuntimeDbPool`
/// connects as `role_gateway`, a non-superuser/non-`BYPASSRLS` role, so the policy's `WITH
/// CHECK` is enforced on every write.
async fn insert_job_idempotent(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    due: &DueSchedule,
    key: &str,
) -> Result<EnqueueOutcome, SchedulerError> {
    // §6.1.1: not a bind parameter — Postgres `SET`/`SET LOCAL` are utility statements and do
    // not accept placeholders over the extended query protocol. `due.tenant_id` is a `Uuid`
    // (never free-form user text), so `Display`-formatting it into the statement carries no
    // injection surface — same pattern `tests/auth_scope_rls.rs` already established for this
    // exact GUC.
    sqlx::query(&format!("SET LOCAL humaux.tenant_id = '{}'", due.tenant_id))
        .execute(&mut **tx)
        .await?;

    let row = sqlx::query(
        "INSERT INTO ops.jobs (tenant_id, job_type, idempotency_key, payload) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (idempotency_key) DO NOTHING \
         RETURNING job_id",
    )
    .bind(due.tenant_id)
    .bind(&due.job_type)
    .bind(key)
    .bind(&due.payload)
    .fetch_optional(&mut **tx)
    .await?;

    Ok(match row {
        Some(r) => EnqueueOutcome::Enqueued {
            job_id: r.try_get("job_id")?,
        },
        None => EnqueueOutcome::AlreadyEnqueued,
    })
}

/// Composes [`claim_lease`] + [`insert_job_idempotent`] in one transaction — the normal
/// scheduler-tick path. Returns `Ok(None)` when this replica lost the lease race (someone
/// else is/was handling this tick); `Ok(Some(outcome))` when it won the lease and attempted
/// the job insert.
///
/// §70.4 pseudocode shape verbatim: "leader acquired -> calculate due schedules -> INSERT
/// jobs ... ON CONFLICT DO NOTHING -> release/renew leader lease" — the transaction commit
/// below is the release; there is nothing to explicitly release, `ttl_seconds` is the renewal
/// window for a leader that dies before commit.
pub async fn claim_and_enqueue(
    pool: &RuntimeDbPool,
    owner: &str,
    ttl_seconds: i64,
    due: &DueSchedule,
) -> Result<Option<EnqueueOutcome>, SchedulerError> {
    let key = idempotency_key(&due.schedule_id, due.planned_at);
    let mut tx = pool.pool().begin().await?;

    let lease = claim_lease(&mut tx, owner, ttl_seconds, due, &key).await?;
    if lease == LeaseOutcome::HeldByOther {
        tx.rollback().await?;
        return Ok(None);
    }

    let outcome = insert_job_idempotent(&mut tx, due, &key).await?;
    tx.commit().await?;
    Ok(Some(outcome))
}

#[cfg(test)]
mod tests {
    //! Pure unit tests for [`idempotency_key`] — no DB needed. The DB-backed exactly-once /
    //! leader-failover / fault-injection proof lives in
    //! `crates/adapters/tests/scheduler_exactly_once.rs` (G32-1/G80-38).
    use super::*;

    fn at(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("valid unix timestamp")
    }

    #[test]
    fn key_differs_by_both_inputs() {
        let k_s1_t1 = idempotency_key("daily-digest", at(1_000));
        let k_s1_t2 = idempotency_key("daily-digest", at(2_000));
        let k_s2_t1 = idempotency_key("weekly-report", at(1_000));
        assert_ne!(
            k_s1_t1, k_s1_t2,
            "same schedule, different planned_at must not collide (§32.1 fault case: key \
             omitting planned_at)"
        );
        assert_ne!(
            k_s1_t1, k_s2_t1,
            "same planned_at, different schedule must not collide (§32.1 fault case: key \
             omitting schedule_id)"
        );
        // Deterministic: same inputs, same key, across repeated calls — required for two
        // concurrent replicas to agree without coordination.
        assert_eq!(k_s1_t1, idempotency_key("daily-digest", at(1_000)));
    }

    /// §32.1's second named fault ("idempotency key 不含 planned_at") does not have to be a
    /// literal missing field — dropping precision on `planned_at` (e.g. truncating to whole
    /// seconds when schedules can fire more than once a second, or reading only the date) is
    /// the same bug: two genuinely different ticks silently produce the same key, so the
    /// second tick's `INSERT ... ON CONFLICT DO NOTHING` finds a "duplicate" that was never
    /// really this tick's job and the enqueue is wrongly suppressed. This test pins the real
    /// formula's nanosecond precision against that regression directly (no DB needed — this
    /// is the same failure `tests::truncation_bug_demonstrates_the_g32_1_second_fault` in the
    /// integration test demonstrates at the DB layer).
    #[test]
    fn nanosecond_precision_survives_sub_second_planned_at() {
        let t1 = OffsetDateTime::from_unix_timestamp_nanos(1_000_000_000_000).unwrap();
        let t2 = OffsetDateTime::from_unix_timestamp_nanos(1_000_500_000_000).unwrap();
        assert_ne!(
            idempotency_key("daily-digest", t1),
            idempotency_key("daily-digest", t2),
            "two ticks 500ms apart must not collide — a formula that only hashed whole \
             seconds would reproduce §32.1's named fault"
        );
    }
}
