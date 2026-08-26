//! `adapters::forget_repo` — T4.8 §37/§37.2 physical IO: the sole `UPDATE
//! projection.stream_log SET state = 'TOMBSTONED'` in this workspace
//! ([`tombstone`]), the §37 `DeletionPlan` step-completion writer ([`record_step`]), §65's
//! idempotent purge-replay loop ([`replay_pending_steps`]), the current-computed
//! `tombstoned_unpurged_over_sla` gauge ([`tombstoned_unpurged_over_sla`]), and — §23.4 G23-2 —
//! the two PG-side overlay reads a literal/EXACT-channel lane needs
//! ([`count_excluding_tombstoned`], [`state_of`]), sharing this module's own `state <>
//! 'TOMBSTONED'` knowledge instead of a caller re-deriving it.
//!
//! `humaux_application::forget` decides *what* to delete and *in what order* (pure, no IO,
//! §3/§78.3); this module is the only place that decision turns into SQL — same split as
//! `stream_repo.rs`'s `advance_prefix`/`sweep_lost` wrapping `humaux_projection::stream`.
//!
//! Every function opens its own transaction and issues `SET LOCAL humaux.tenant_id` before
//! touching `projection.stream_log` / `control.deletion_requests` / `ops.deletion_plan_steps`
//! — all three carry a `tenant_id` column and a `FORCE`d RLS policy (0007/0012/0054); same
//! technique as `stream_repo::set_tenant_local` (each file keeps its own copy on purpose, see
//! that module's doc).
//!
//! Runs under [`MaintenanceDbPool`] (`role_maintenance`) — the only role
//! `stream_log_guard_state_transition` (0011) permits to make the `* -> TOMBSTONED` edge, and
//! the only role granted `EXECUTE` on 0054's two `SECURITY DEFINER` bookkeeping functions.

use sqlx::Row;
use sqlx::types::Uuid;

use humaux_application::forget::DeletionStep;
use humaux_projection::stream::StreamKey;

use crate::postgres::MaintenanceDbPool;

type Txn<'c> = sqlx::Transaction<'c, sqlx::Postgres>;

/// DB-layer failure. Not one of §52's two frozen domain error enums — same "adapter-local,
/// not domain" reasoning as `stream_repo::StreamRepoError`.
#[derive(Debug)]
pub enum ForgetRepoError {
    Db(sqlx::Error),
}

impl From<sqlx::Error> for ForgetRepoError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

impl std::fmt::Display for ForgetRepoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(e) => write!(f, "forget_repo DB error: {e}"),
        }
    }
}

impl std::error::Error for ForgetRepoError {}

async fn set_tenant_local(txn: &mut Txn<'_>, tenant_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(&format!("SET LOCAL humaux.tenant_id = '{tenant_id}'"))
        .execute(&mut **txn)
        .await?;
    Ok(())
}

/// §37.2's sole entry point: `state -> TOMBSTONED` for exactly one `projection.stream_log`
/// row, identified by `key` (the six §15.1 key columns) and `seq` (`stream_seq`).
///
/// **This literal `UPDATE ... SET state = 'TOMBSTONED'` must never be duplicated anywhere
/// else in this workspace** (§37.2: "没有第二个函数能改这张表的 `state`"; enforced today by
/// the DB-side `stream_log_guard_state_transition` trigger + `role_maintenance`-only GRANT,
/// pending a workspace-wide grep-based architecture-check — see this crate's caller-side
/// T4.8 report for that gate's wiring status).
///
/// Idempotent: the `AND state <> 'TOMBSTONED'` guard makes a replayed call after a crash a
/// true no-op (`rows_affected() == 0`, `Ok(false)`), never a second UPDATE attempt against an
/// already-terminal row — the DB trigger's own `OLD.state = NEW.state` early-return would
/// have allowed a redundant same-state UPDATE too, but this guard avoids even issuing it.
///
/// Returns `Ok(true)` iff this call is the one that performed the transition (§37 step 1);
/// `Ok(false)` means the row was already `TOMBSTONED` (replay) or did not exist for this key.
pub async fn tombstone(
    pool: &MaintenanceDbPool,
    key: &StreamKey,
    seq: u64,
) -> Result<bool, ForgetRepoError> {
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, key.tenant_id.0).await?;

    let result = sqlx::query(
        "UPDATE projection.stream_log \
            SET state = 'TOMBSTONED' \
          WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
            AND projection_kind = $5 AND projection_version = $6 AND stream_seq = $7 \
            AND state <> 'TOMBSTONED'",
    )
    .bind(key.tenant_id.0)
    .bind(&key.scope_kind)
    .bind(key.scope_id)
    .bind(&key.domain)
    .bind(&key.projection_kind)
    .bind(&key.projection_version)
    .bind(seq as i64)
    .execute(&mut *txn)
    .await?;
    txn.commit().await?;

    Ok(result.rows_affected() > 0)
}

/// §22.1 PostgreSQL EXACT channel / §23.4 G23-2 literal lane: rows in `[seq_lo, seq_hi]` whose
/// `state` is not `TOMBSTONED` — the production predicate a PG-side lane applies to stay
/// overlay-consistent with [`tombstone`]'s own state transition, instead of a caller
/// re-deriving `state <> 'TOMBSTONED'` itself at each call site (§23.1②: the overlay predicate
/// belongs in one place). Shares [`tombstone`]'s key shape and `role_maintenance` scoping.
pub async fn count_excluding_tombstoned(
    pool: &MaintenanceDbPool,
    key: &StreamKey,
    seq_lo: u64,
    seq_hi: u64,
) -> Result<i64, ForgetRepoError> {
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, key.tenant_id.0).await?;

    let row = sqlx::query(
        "SELECT count(*) AS n FROM projection.stream_log \
          WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
            AND projection_kind = $5 AND projection_version = $6 \
            AND stream_seq BETWEEN $7 AND $8 AND state <> 'TOMBSTONED'",
    )
    .bind(key.tenant_id.0)
    .bind(&key.scope_kind)
    .bind(key.scope_id)
    .bind(&key.domain)
    .bind(&key.projection_kind)
    .bind(&key.projection_version)
    .bind(seq_lo as i64)
    .bind(seq_hi as i64)
    .fetch_one(&mut *txn)
    .await?;
    txn.commit().await?;

    Ok(row.try_get::<i64, _>("n")?)
}

/// §23.4 G23-2 literal lane's per-row read: the current `state` for one `stream_seq`, `None`
/// if the row does not exist for this key. Shares [`tombstone`]'s key shape and
/// `role_maintenance` scoping — the production entry point a literal-lookup lane calls instead
/// of hand-writing the same `SELECT state FROM projection.stream_log WHERE ...` per caller.
pub async fn state_of(
    pool: &MaintenanceDbPool,
    key: &StreamKey,
    seq: u64,
) -> Result<Option<String>, ForgetRepoError> {
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, key.tenant_id.0).await?;

    let row = sqlx::query(
        "SELECT state FROM projection.stream_log \
          WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
            AND projection_kind = $5 AND projection_version = $6 AND stream_seq = $7",
    )
    .bind(key.tenant_id.0)
    .bind(&key.scope_kind)
    .bind(key.scope_id)
    .bind(&key.domain)
    .bind(&key.projection_kind)
    .bind(&key.projection_version)
    .bind(seq as i64)
    .fetch_optional(&mut *txn)
    .await?;
    txn.commit().await?;

    Ok(row.map(|r| r.try_get::<String, _>("state")).transpose()?)
}

/// §37/§65 step-completion writer: calls `ops.record_deletion_plan_step` (0054, `SECURITY
/// DEFINER`) so `role_maintenance` never needs a direct table grant on
/// `ops.deletion_plan_steps` (see 0054's file header for why). Idempotent — a replay of an
/// already-recorded step returns `Ok(false)`, never a duplicate row or an error
/// (`ON CONFLICT DO NOTHING` inside the function).
pub async fn record_step(
    pool: &MaintenanceDbPool,
    tenant_id: Uuid,
    deletion_request_id: Uuid,
    step: DeletionStep,
    outcome: &str,
    detail: Option<&str>,
) -> Result<bool, ForgetRepoError> {
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, tenant_id).await?;

    let row =
        sqlx::query("SELECT ops.record_deletion_plan_step($1, $2, $3, $4, $5, $6) AS inserted")
            .bind(deletion_request_id)
            .bind(tenant_id)
            .bind(step.as_db_str())
            .bind(step.ordinal() as i16)
            .bind(outcome)
            .bind(detail)
            .fetch_one(&mut *txn)
            .await?;
    txn.commit().await?;

    Ok(row.try_get::<bool, _>("inserted")?)
}

/// Steps already recorded for one `deletion_request_id`, as the pure
/// [`DeletionStep`](humaux_application::forget::DeletionStep) set
/// [`humaux_application::forget::DeletionPlan::pending`] takes.
async fn completed_steps(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    deletion_request_id: Uuid,
) -> Result<std::collections::BTreeSet<DeletionStep>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT step FROM ops.deletion_plan_steps \
          WHERE tenant_id = $1 AND deletion_request_id = $2",
    )
    .bind(tenant_id)
    .bind(deletion_request_id)
    .fetch_all(&mut **txn)
    .await?;
    Ok(rows
        .iter()
        .filter_map(|r| DeletionStep::from_db_str(r.get::<&str, _>("step")))
        .collect())
}

/// §65 purge-replay job: for every step in `plan` not yet recorded for
/// `deletion_request_id` (per [`humaux_application::forget::DeletionPlan::pending`]),
/// `execute` it and record completion. Idempotent under interruption — a crash between two
/// steps loses nothing: the next call recomputes `pending` from `ops.deletion_plan_steps` and
/// only (re)runs what is still missing, never repeats a recorded step (§65 "幂等：中断后重放
/// 不重复删").
///
/// `execute` performs the actual step-2..8 side effect (Authority rows / Relations /
/// Qdrant points / …); this function's own job is sequencing + idempotent bookkeeping, not
/// those subsystems themselves (Qdrant/object-store adapters are still §0.x placeholders
/// elsewhere in this crate — see `crates/adapters/src/qdrant.rs`'s own doc). `execute`
/// returning `Ok(outcome)` where `outcome != "DONE"` (e.g. `"EXTERNAL_PENDING"`) is still
/// recorded — a step that cannot fully complete yet is not an error, it is one of §37's
/// named non-`DONE` step outcomes.
///
/// Returns the steps this call actually executed (empty on a full replay of an
/// already-completed plan — the observable "already done, skipped" signal).
pub async fn replay_pending_steps<F>(
    pool: &MaintenanceDbPool,
    tenant_id: Uuid,
    deletion_request_id: Uuid,
    plan: &humaux_application::forget::DeletionPlan,
    mut execute: F,
) -> Result<Vec<DeletionStep>, ForgetRepoError>
where
    F: FnMut(DeletionStep) -> (&'static str, Option<String>),
{
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, tenant_id).await?;
    let done = completed_steps(&mut txn, tenant_id, deletion_request_id).await?;
    txn.commit().await?;

    let mut executed = Vec::new();
    for step in plan.pending(&done) {
        let (outcome, detail) = execute(step);
        let newly_recorded = record_step(
            pool,
            tenant_id,
            deletion_request_id,
            step,
            outcome,
            detail.as_deref(),
        )
        .await?;
        if newly_recorded {
            executed.push(step);
        }
    }
    Ok(executed)
}

/// §41.2 `tombstoned_unpurged_over_sla` (gauge, §65 job收尾 takes the reading) — current-
/// computed, never a materialized column (§37.2's own "现算不落列" reasoning extends to this
/// metric, not only to `deleted`). Counts `TOMBSTONED` `stream_log` rows, settled longer than
/// `sla` ago, whose matching `deletion_requests` row has no recorded `QDRANT_POINTS` step yet
/// (step 5, the physical purge — §37: "字节还在...它必须有自己的出口").
pub async fn tombstoned_unpurged_over_sla(
    pool: &MaintenanceDbPool,
    tenant_id: Uuid,
    sla: std::time::Duration,
) -> Result<i64, ForgetRepoError> {
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, tenant_id).await?;

    let row = sqlx::query(
        "SELECT count(*) AS n \
           FROM projection.stream_log s \
           JOIN control.deletion_requests d \
             ON d.tenant_id = s.tenant_id AND d.scope_kind = s.scope_kind \
            AND d.scope_id = s.scope_id AND d.domain = s.domain \
            AND d.projection_kind = s.projection_kind \
            AND d.projection_version = s.projection_version AND d.stream_seq = s.stream_seq \
          WHERE s.tenant_id = $1 \
            AND s.state = 'TOMBSTONED' \
            AND s.settled_at < now() - (interval '1 second' * $2::bigint) \
            AND NOT EXISTS ( \
                SELECT 1 FROM ops.deletion_plan_steps p \
                 WHERE p.tenant_id = $1 AND p.deletion_request_id = d.deletion_request_id \
                   AND p.step = 'QDRANT_POINTS' \
            )",
    )
    .bind(tenant_id)
    .bind(sla.as_secs() as i64)
    .fetch_one(&mut *txn)
    .await?;
    txn.commit().await?;

    Ok(row.try_get::<i64, _>("n")?)
}
