//! `adapters::stream_repo` — §15 `projection.stream_log` / `stream_checkpoints` SQL: T3.3's
//! `advance_prefix` (three/four independent reads + monotonic watermark write, both through
//! [`RetrievalWorkerDbPool`]) and T3.4's `ISSUED -> LOST` patrol (through
//! [`MaintenanceDbPool`]). The consistency arithmetic itself lives in
//! `humaux_projection::stream` (no IO, unit-tested there); this module only fetches the
//! numbers, calls it, and writes the result.
//!
//! Every function here opens its own transaction and issues `SET LOCAL humaux.tenant_id`
//! before touching `projection.stream_log` / `stream_checkpoints` — both carry a `tenant_id`
//! column and 0012's blanket policy `FORCE`s row-level security for every role in the §6.2.0
//! frozen eight, including `role_retrieval_worker` and `role_maintenance` (neither is
//! `BYPASSRLS`, §11's role DDL). Without this, every query below silently sees zero rows
//! rather than erroring — same technique as `jobs::set_tenant_local` /
//! `remember::set_tenant_local` / `retrieve::set_tenant_local`. One consequence: [`sweep_lost`]
//! cannot be the single queue-wide statement §15.2's spec SQL block shows — RLS has no
//! cross-tenant exception for any non-superuser role (confirmed against `control.tenants`
//! itself, which also carries `tenant_id` and gets the same blanket policy), so a caller that
//! wants to patrol every tenant must loop tenant ids and call this once per tenant.

use sqlx::Row;
use sqlx::types::Uuid;

use humaux_projection::stream::{Inconsistent, StreamKey, StreamLedgerSnapshot};
use humaux_retrieval::completeness::{
    LedgerClosure,
    ledger::{self, LedgerReads},
};

use crate::postgres::{MaintenanceDbPool, RetrievalWorkerDbPool};
use crate::retrieve::SETTLED_OK_SQL_LIST;

type Txn<'c> = sqlx::Transaction<'c, sqlx::Postgres>;

/// DB-layer failure. Not one of the workspace's two frozen domain error enums (§52) — same
/// "adapter-local, not domain" reasoning as `email::OutboxError` / `postgres::PoolInitError`.
#[derive(Debug)]
pub enum StreamRepoError {
    Db(sqlx::Error),
}

impl From<sqlx::Error> for StreamRepoError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

impl std::fmt::Display for StreamRepoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(e) => write!(f, "stream_repo DB error: {e}"),
        }
    }
}

impl std::error::Error for StreamRepoError {}

/// [`advance_prefix`]'s failure: either the DB round trip itself failed, or it succeeded and
/// the §15.4 identity did not hold (`Inconsistent` — see `humaux_projection::stream`'s doc for
/// what the caller owning completeness classification does with this).
#[derive(Debug)]
pub enum AdvanceError {
    Db(sqlx::Error),
    Inconsistent,
}

impl From<sqlx::Error> for AdvanceError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

impl From<Inconsistent> for AdvanceError {
    fn from(_: Inconsistent) -> Self {
        Self::Inconsistent
    }
}

impl std::fmt::Display for AdvanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(e) => write!(f, "advance_prefix DB error: {e}"),
            Self::Inconsistent => write!(f, "§15.4 ledger identity violated (Inconsistent)"),
        }
    }
}

impl std::error::Error for AdvanceError {}

/// §15.1's six-column key `WHERE` clause, shared verbatim by every query below so the
/// `$1..$6` binding order below (`bind_key`) can't drift out of sync per query. A query may
/// reference `$1..$6` more than once in its text (e.g. the `contiguous_done_prefix` query in
/// [`fetch_snapshot_in_txn`]) without binding them twice — PostgreSQL parameters are
/// positional, not per-occurrence.
const KEY_WHERE: &str = "tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
     AND projection_kind = $5 AND projection_version = $6";

type PgQuery<'q> = sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>;

/// Binds the six §15.1 key columns in `KEY_WHERE`'s fixed `$1..$6` order.
fn bind_key<'q>(query: PgQuery<'q>, key: &'q StreamKey) -> PgQuery<'q> {
    query
        .bind(key.tenant_id.0)
        .bind(&key.scope_kind)
        .bind(key.scope_id)
        .bind(&key.domain)
        .bind(&key.projection_kind)
        .bind(&key.projection_version)
}

/// Sets `humaux.tenant_id` for the remainder of `txn` (§6.1 RLS context) — same technique and
/// same non-bind-parameter rationale as `jobs::set_tenant_local` / `retrieve::set_tenant_local`
/// (a `Uuid`'s `Display` only ever emits the canonical lowercase-hex form, so this formatted
/// string carries no injectable characters).
async fn set_tenant_local(txn: &mut Txn<'_>, tenant_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(&format!("SET LOCAL humaux.tenant_id = '{tenant_id}'"))
        .execute(&mut **txn)
        .await?;
    Ok(())
}

/// §15.4's independent reads, one round trip each, run inside an already-open (and already
/// tenant-scoped) transaction — see this module's doc for why they are never combined into a
/// single query, and for why the caller must have already called [`set_tenant_local`].
async fn fetch_snapshot_in_txn(
    txn: &mut Txn<'_>,
    key: &StreamKey,
) -> Result<StreamLedgerSnapshot, sqlx::Error> {
    // (1) expected = stream_checkpoints.issued_highwater. No row yet ⇒ nothing has ever been
    // issued for this key ⇒ 0 (the same key would then also read 0 for every other number
    // below — a `remember` cannot write a stream_log row before its checkpoint row exists,
    // §15.1's "同一事务" seq-issuance block).
    let expected: u64 = bind_key(
        sqlx::query(&format!(
            "SELECT issued_highwater FROM projection.stream_checkpoints WHERE {KEY_WHERE}"
        )),
        key,
    )
    .fetch_optional(&mut **txn)
    .await?
    .map(|row| row.try_get::<i64, _>("issued_highwater"))
    .transpose()?
    .unwrap_or(0) as u64;

    // (2) done + pending, one aggregate query over stream_log (§15.2 state dichotomy).
    let agg_row = bind_key(
        sqlx::query(&format!(
            "SELECT \
               count(*) FILTER (WHERE state IN ({SETTLED_OK_SQL_LIST})) AS done, \
               count(*) FILTER (WHERE state IN ('ISSUED','PROCESSING','WAITING_KEY','RETRY_WAIT')) AS pending \
             FROM projection.stream_log WHERE {KEY_WHERE}"
        )),
        key,
    )
    .fetch_one(&mut **txn)
    .await?;
    let done: u64 = agg_row.try_get::<i64, _>("done")? as u64;
    let pending: u64 = agg_row.try_get::<i64, _>("pending")? as u64;

    // (3) open_gaps — the processing_gaps VIEW, never re-derived from stream_log directly
    // here (§15.1: the view is the sole gap-count source).
    let open_gaps: u64 = bind_key(
        sqlx::query(&format!(
            "SELECT count(*) AS n FROM projection.processing_gaps WHERE {KEY_WHERE}"
        )),
        key,
    )
    .fetch_one(&mut **txn)
    .await?
    .try_get::<i64, _>("n")? as u64;

    // (4) max_stream_seq — independent of (1)'s `expected`, on purpose (see module doc).
    let max_stream_seq: u64 = bind_key(
        sqlx::query(&format!(
            "SELECT COALESCE(MAX(stream_seq), 0) AS n FROM projection.stream_log WHERE {KEY_WHERE}"
        )),
        key,
    )
    .fetch_one(&mut **txn)
    .await?
    .try_get::<i64, _>("n")? as u64;

    // (5) contiguous_done_prefix — §15.4's formula, computed in SQL against the raw rows (the
    // worked example this pins: 100 FAILED / 101 DONE ⇒ 99, not 101 — the gap at 100 blocks
    // the prefix even though a higher seq settled OK). `KEY_WHERE` appears twice in this
    // query's text but is still only bound once (six placeholders total, see `KEY_WHERE`'s
    // doc).
    let contiguous_done_prefix: u64 = bind_key(
        sqlx::query(&format!(
            "WITH first_gap AS ( \
               SELECT MIN(stream_seq) AS s FROM projection.stream_log \
                WHERE {KEY_WHERE} AND state NOT IN ({SETTLED_OK_SQL_LIST}) \
             ) \
             SELECT COALESCE( \
               (SELECT s - 1 FROM first_gap WHERE s IS NOT NULL), \
               (SELECT COALESCE(MAX(stream_seq), 0) FROM projection.stream_log WHERE {KEY_WHERE}) \
             ) AS n"
        )),
        key,
    )
    .fetch_one(&mut **txn)
    .await?
    .try_get::<i64, _>("n")? as u64;

    Ok(StreamLedgerSnapshot {
        expected,
        done,
        pending,
        open_gaps,
        max_stream_seq,
        contiguous_done_prefix,
    })
}

/// Closes one stream ledger inside a caller-owned, tenant-scoped transaction.
///
/// The four A1 inputs remain `fetch_snapshot_in_txn`'s independent reads. This helper adds
/// only the separately reported settled subsets and delegates all closure arithmetic to
/// [`ledger::close`]. Callers establish isolation and RLS GUCs before invoking it.
pub(crate) async fn close_ledger_in_txn(
    txn: &mut Txn<'_>,
    key: &StreamKey,
) -> Result<LedgerClosure, sqlx::Error> {
    let snapshot = fetch_snapshot_in_txn(txn, key).await?;
    // §23.1② A2 is `visible + deleted + skipped == done`, so EVERY state inside `done` must
    // land in exactly one of the three left-hand terms or A2 is permanently one short. 0167's
    // `RETIRED_FAILED` is inside `done` (A1 — `done + open_gaps + pending == expected` — leaves
    // it nowhere else: a retired row is neither pending nor an open gap), and by construction it
    // is never in the index (0167 header: "It was never indexed either"), so it belongs on the
    // `skipped` term — the "settled, will never be visible, not a deletion" bucket
    // `SKIPPED_BY_POLICY` already defines. Counting it anywhere else, or nowhere, makes every
    // recall on a stream that ever had one retirement abstain with `ProjectionInvisibleLoss`
    // forever, which is the read-side failure 0167's own header analysed only for the §15.4
    // prefix. §23.1② amended with this citation.
    let row = bind_key(
        sqlx::query(&format!(
            "SELECT count(*) FILTER (WHERE state = 'TOMBSTONED') AS deleted, \
                    count(*) FILTER (WHERE state IN ('SKIPPED_BY_POLICY','RETIRED_FAILED')) \
                      AS skipped \
             FROM projection.stream_log WHERE {KEY_WHERE}"
        )),
        key,
    )
    .fetch_one(&mut **txn)
    .await?;
    let deleted = row.try_get::<i64, _>("deleted")? as u64;
    let skipped = row.try_get::<i64, _>("skipped")? as u64;
    Ok(ledger::close(LedgerReads {
        expected: snapshot.expected,
        done: snapshot.done,
        deleted,
        skipped,
        open_gaps: snapshot.open_gaps,
        pending: snapshot.pending,
    }))
}

/// Public, read-only entry point for [`fetch_snapshot_in_txn`]: opens its own tenant-scoped
/// transaction, reads the snapshot, and commits (a read-only transaction still needs an
/// explicit `commit`/`rollback` — `sqlx::Transaction` rolls back on drop otherwise, which
/// would work here too but `commit` is the established convention this module's sibling files
/// use for read-only txns as well).
pub async fn fetch_ledger_snapshot(
    pool: &RetrievalWorkerDbPool,
    key: &StreamKey,
) -> Result<StreamLedgerSnapshot, StreamRepoError> {
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, key.tenant_id.0).await?;
    let snapshot = fetch_snapshot_in_txn(&mut txn, key).await?;
    txn.commit().await?;
    Ok(snapshot)
}

/// Public, read-only entry point for [`close_ledger_in_txn`] — the §23.1② A1/A2 inputs as the
/// envelope path reads them, same transaction shape [`fetch_ledger_snapshot`] uses. The
/// in-request callers (`retrieve`/`context_repo`) already hold their own transaction; this is
/// for an out-of-band reader (ops, and the closure tests that must exercise the REAL counting
/// query rather than a fixture's copy of it — a copy is how the `RETIRED_FAILED` A2 hole got
/// past every existing envelope test).
pub async fn fetch_ledger_closure(
    pool: &RetrievalWorkerDbPool,
    key: &StreamKey,
) -> Result<LedgerClosure, StreamRepoError> {
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, key.tenant_id.0).await?;
    let closure = close_ledger_in_txn(&mut txn, key).await?;
    txn.commit().await?;
    Ok(closure)
}

/// §15.4 `advance_prefix`: fetches the independent snapshot, validates it
/// (`humaux_projection::stream::advance_prefix`), and — only if it validates — writes
/// `stream_checkpoints.projection_highwater` monotonically (`WHERE projection_highwater <=
/// $n`, never regressing a watermark that has already moved past `n` — a concurrent/stale
/// call landing after a newer one is a silent no-op here, not a corruption signal). Read and
/// write share one transaction, so no other writer can move the checkpoint between this
/// function's own read and write.
pub async fn advance_prefix(
    pool: &RetrievalWorkerDbPool,
    key: &StreamKey,
) -> Result<u64, AdvanceError> {
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, key.tenant_id.0).await?;
    let snapshot = fetch_snapshot_in_txn(&mut txn, key).await?;
    let n = humaux_projection::stream::advance_prefix(snapshot)?;

    bind_key(
        sqlx::query(&format!(
            "UPDATE projection.stream_checkpoints SET projection_highwater = $7 \
             WHERE {KEY_WHERE} AND projection_highwater <= $7"
        )),
        key,
    )
    .bind(n as i64)
    .execute(&mut *txn)
    .await?;
    txn.commit().await?;

    Ok(n)
}

/// §15.2/§15.4 audited retirement (migration `0167`, ADR-0042): moves this family's exhausted
/// `FAILED` tickets **of one named failure class** into `RETIRED_FAILED`, and returns the
/// `stream_seq`s actually retired.
///
/// The write itself is `projection.retire_failed_ticket(...)`, the owner SECURITY DEFINER
/// function 0167 creates — `role_maintenance` holds nothing but EXECUTE on it, and the 0011/0167
/// transition trigger admits `FAILED -> RETIRED_FAILED` only when `current_user` is the owner, so
/// this is the only path in the workspace that can produce that state. Two statements on purpose
/// (read the candidates, then retire them one by one): the function is per-ticket because the
/// audit is per-ticket, and a "retire everything that failed" statement is precisely the blanket
/// action 0167's header rejects.
///
/// `failure_class` must equal the row's own `error_class` — naming the wrong class retires
/// nothing rather than something else. Runs under [`MaintenanceDbPool`] with the tenant GUC
/// installed by this function, which is also what scopes the definer's own write (0167 sets no
/// tenant context of its own, deliberately).
pub async fn retire_failed(
    pool: &MaintenanceDbPool,
    key: &StreamKey,
    failure_class: &str,
) -> Result<Vec<i64>, StreamRepoError> {
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, key.tenant_id.0).await?;

    let candidates: Vec<i64> = bind_key(
        sqlx::query(&format!(
            "SELECT stream_seq FROM projection.stream_log \
              WHERE {KEY_WHERE} AND state = 'FAILED' \
                AND error_class IS NOT DISTINCT FROM $7 \
              ORDER BY stream_seq"
        )),
        key,
    )
    .bind(failure_class)
    .fetch_all(&mut *txn)
    .await?
    .iter()
    .map(|row| row.try_get::<i64, _>("stream_seq"))
    .collect::<Result<_, _>>()?;

    let mut retired = Vec::with_capacity(candidates.len());
    for stream_seq in candidates {
        let n: i64 = bind_key(
            sqlx::query(
                "SELECT projection.retire_failed_ticket($1,$2,$3,$4,$5,$6,$7,$8)::bigint AS n",
            ),
            key,
        )
        .bind(stream_seq)
        .bind(failure_class)
        .fetch_one(&mut *txn)
        .await?
        .try_get("n")?;
        if n == 1 {
            retired.push(stream_seq);
        }
    }
    txn.commit().await?;

    Ok(retired)
}

/// §15.2 `ISSUED -> LOST` patrol for one tenant: an orphaned ticket (no matching in-flight
/// `ops.jobs` row, §31's typed `stream_key`/`stream_seq` locator, encoded via
/// [`StreamKey::stream_key_text`](humaux_projection::stream::StreamKey::stream_key_text) —
/// this `UPDATE` builds the identical `:`-joined text in SQL so it matches whatever a job
/// writer bound via that Rust function) older than `sla` becomes an explicit gap.
/// `WAITING_KEY` / `RETRY_WAIT` rows are structurally excluded — the `WHERE s.state =
/// 'ISSUED'` guard means this statement can never touch them regardless of how long they have
/// sat (§15.2: "永远不能仅因为墙钟时间而转 LOST"). Runs under [`MaintenanceDbPool`] — the only
/// role `stream_log_guard_state_transition` (0011) permits to make this transition.
///
/// Scoped to one `tenant_id` (see this module's doc for why: RLS has no cross-tenant carve-out
/// for `role_maintenance`) — a periodic patrol job loops `control.tenants` under a connection
/// that *can* enumerate tenants (out of this function's scope) and calls this once per tenant.
/// Returns the number of rows swept for that tenant.
pub async fn sweep_lost(
    pool: &MaintenanceDbPool,
    tenant_id: Uuid,
    sla: std::time::Duration,
) -> Result<u64, StreamRepoError> {
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, tenant_id).await?;

    let result = sqlx::query(
        "UPDATE projection.stream_log s \
            SET state = 'LOST', error_class = 'ORPHANED_PIPELINE_ITEM' \
          WHERE s.tenant_id = $1 \
            AND s.state = 'ISSUED' \
            AND s.issued_at < now() - (interval '1 second' * $2::bigint) \
            AND NOT EXISTS ( \
                SELECT 1 FROM ops.jobs j \
                 WHERE j.tenant_id = $1 \
                   AND j.stream_key = ( \
                         s.tenant_id::text || ':' || s.scope_kind || ':' || s.scope_id::text \
                         || ':' || s.domain || ':' || s.projection_kind || ':' \
                         || s.projection_version \
                       ) \
                   AND j.stream_seq = s.stream_seq \
                   AND j.status IN ('PENDING','PROCESSING','WAITING_KEY','RETRY_WAIT') \
            )",
    )
    .bind(tenant_id)
    .bind(sla.as_secs() as i64)
    .execute(&mut *txn)
    .await?;
    txn.commit().await?;

    Ok(result.rows_affected())
}
