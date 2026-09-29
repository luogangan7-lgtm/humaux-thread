//! `adapters::stream_repo` — §15 `projection.stream_log` / `stream_checkpoints` SQL: T3.3's `advance_prefix`
//!   (three/four independent reads + monotonic watermark write, both through [`RetrievalWorkerDbPool`]) and T3.4's
//!   `ISSUED -> LOST` patrol (through [`MaintenanceDbPool`]).
//! Depends-on: crates=[humaux-domain, humaux-projection, humaux-retrieval, humaux-testkit, sqlx, tokio];
//!   services=[PostgreSQL(any)
//!   r=[ops.jobs, projection.processing_gaps] w=[projection.stream_checkpoints, projection.stream_log]
//!   x=[projection.retire_failed_ticket], PostgreSQL(role_maintenance), PostgreSQL(role_retrieval_worker)
//!   x=[projection.claim_issued_tickets, projection.unplaced_issued_tickets]]; env=[HUMAUX_TEST_PG_DSN];
//!   modules=[adapters::placement_repo, adapters::postgres, adapters::qdrant, adapters::retrieve, domain::egress,
//!   projection::stream, retrieval::completeness]
//! Called-by: [adapters::context_repo, adapters::projection_worker, adapters::retrieve, retrieval-worker::main, tests,
//!   xtask::projection_serve]
//! Invariants: [every function opens its own transaction and sets humaux.tenant_id before touching FORCE-RLS stream
//!   tables (otherwise it would silently see zero rows) — except the two ADR-0052 definer calls, which are the only
//!   cross-tenant reads/claims of stream_log; every settle/retry/release write is fenced on (lease_owner, attempts), so
//!   a worker whose lease was reclaimed writes 0 rows; consistency arithmetic lives in humaux_projection::stream]
//! Spec: Baseline §6.2.0; §11; §15.2; ADR-0052
//!
//! The consistency arithmetic itself lives in
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

use humaux_domain::egress::ProcessorId;
use humaux_projection::stream::{Inconsistent, StreamKey, StreamLedgerSnapshot};
use humaux_retrieval::completeness::{
    LedgerClosure,
    ledger::{self, LedgerReads},
};

use crate::postgres::{MaintenanceDbPool, RetrievalWorkerDbPool};
use crate::qdrant::{RetrievalFamily, TenantPlacementRow};
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
    // dep: PostgreSQL(role_retrieval_worker) — transaction entry for `fetch_ledger_snapshot`
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
    // dep: PostgreSQL(role_retrieval_worker) — transaction entry for `fetch_ledger_closure`
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
///
/// `processor` is the §7.4 [`ProcessorId`] of the worker making the call, written into
/// `projection_processor_id` (migration 0171) in the SAME statement as the watermark — card
/// 21's "a checkpoint written by one worker is attributed to it", which before 0171 had no
/// column to land in. It is deliberately not a separate UPDATE: an attribution that can be
/// written without the watermark (or a watermark without its attribution) is an attribution
/// that will eventually name the wrong process. The monotonic `WHERE` fences both: a call whose
/// computed prefix is behind the stored watermark writes neither.
pub async fn advance_prefix(
    pool: &RetrievalWorkerDbPool,
    key: &StreamKey,
    processor: ProcessorId,
) -> Result<u64, AdvanceError> {
    // dep: PostgreSQL(role_retrieval_worker) — transaction entry for `advance_prefix`
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, key.tenant_id.0).await?;
    let snapshot = fetch_snapshot_in_txn(&mut txn, key).await?;
    let n = humaux_projection::stream::advance_prefix(snapshot)?;

    bind_key(
        sqlx::query(&format!(
            "UPDATE projection.stream_checkpoints \
             SET projection_highwater = $7, projection_processor_id = $8 \
             WHERE {KEY_WHERE} AND projection_highwater <= $7"
        )),
        key,
    )
    .bind(n as i64)
    .bind(processor.0)
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
    // dep: PostgreSQL(role_maintenance) — transaction entry for `retire_failed`
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
    // dep: PostgreSQL(role_maintenance) — transaction entry for `sweep_lost`
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

/// ADR-0052 D-B: "RETRY" is not a state. A ticket backing off after a transient failure is
/// `ISSUED` with a spent attempt, a future `next_attempt_at` and no lease — this predicate, named
/// once here and used by the rehearsal and the tests, is the whole definition. The state stays
/// `ISSUED` so the 0011/0167 transition guard and the §6.2.2 verbatim triple for
/// `role_retrieval_worker` do not change.
pub const RETRY_PREDICATE: &str =
    "state = 'ISSUED' AND attempts >= 1 AND next_attempt_at > now() AND lease_owner IS NULL";

/// One ticket [`claim_issued`] leased, with the tenant's §17.3 placement row the claim joined
/// (ADR-0052 D-C: the worker needs no placement lookup and no placement cache).
#[derive(Debug, Clone)]
pub struct ClaimedTicket {
    pub key: StreamKey,
    pub stream_seq: i64,
    pub commit_seq: i64,
    /// `attempts` after the claim's increment — the fence every later write of this ticket uses.
    pub attempts: i32,
    pub placement: TenantPlacementRow,
}

/// Who may settle, retry or release a ticket (ADR-0052 D-D). Every such write carries
/// `lease_owner IS NOT DISTINCT FROM <owner> AND attempts = <attempts>`: a worker whose lease
/// expired and was re-claimed by another (which bumped `attempts`) writes 0 rows — reported as a
/// lost lease, never an error. No `lease_expires_at > now()` in the fence (ADR-0036 D4: a settle
/// that lands just after expiry, before anyone re-claimed, is still the only writer).
#[derive(Debug, Clone, Copy)]
pub struct TicketFence<'a> {
    /// The claim's lease owner; `None` for the legacy unleased single-key read (`run_once`).
    pub lease_owner: Option<&'a str>,
    /// `attempts` as the claim (or the legacy read) returned it.
    pub attempts: i32,
}

/// ADR-0052 D-E backoff: the n-th transient failure waits `min(base * 2^(n-1), max)` seconds.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    pub base_secs: f64,
    pub max_secs: f64,
}

/// The §15.1 ticket triple a claim takes plus the §17.3 placement family its tenants must be
/// placed in. [`ClaimFamily::of`] derives both from one [`RetrievalFamily`] (card 21's single
/// source); the fields are open so a test can claim a throwaway `projection_version` that no real
/// ticket carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimFamily {
    pub domain: String,
    pub projection_kind: String,
    pub projection_version: String,
    pub placement: RetrievalFamily,
}

impl ClaimFamily {
    /// `None` for a §17 family that is not a `projection.stream_log` producer.
    pub fn of(placement: RetrievalFamily) -> Option<Self> {
        let ticket = placement.ticket_family()?;
        Some(Self {
            domain: ticket.domain().to_owned(),
            projection_kind: ticket.projection_kind().to_owned(),
            projection_version: ticket.projection_version().to_owned(),
            placement,
        })
    }
}

/// A ticket [`claim_issued`] leased whose placement row this build could not parse (a
/// CHECK-constrained column carrying a value it does not know — deploy skew, e.g. a newer
/// migration's `promotion_state`). The caller parks it ([`release_for_retry`] with
/// `spend = false`); it is never processed without a placement and never counts toward
/// `transient_exhausted`. The stream key comes as its raw columns — the caller types it.
#[derive(Debug, Clone)]
pub struct UnplaceableTicket {
    pub tenant_id: Uuid,
    pub scope_kind: String,
    pub scope_id: Uuid,
    pub domain: String,
    pub projection_kind: String,
    pub projection_version: String,
    pub stream_seq: i64,
    pub attempts: i32,
}

/// What one [`claim_issued`] leased, split per row (review 2026-09-29 P1: one unparsable row
/// used to fail the whole already-committed claim and strand every co-claimed ticket).
#[derive(Debug, Default)]
pub struct ClaimBatch {
    pub tickets: Vec<ClaimedTicket>,
    pub unplaceable: Vec<UnplaceableTicket>,
}

/// ADR-0052 D-C: leases up to `limit` ISSUED tickets of `family`'s triple across every tenant that
/// has a placement row for `family.placement` — at most `per_tenant_cap` per tenant, one family never to two workers —
/// through the owner definer `projection.claim_issued_tickets` (migration 0176, EXECUTE:
/// `role_retrieval_worker` only). Autocommit on purpose: the function takes a transaction-scoped
/// advisory lock and requires READ COMMITTED, so the lock is released the moment the claim commits.
/// The leases are committed before any row is parsed, so rows are parsed one by one
/// ([`parse_claim_rows`]): a bad row costs only itself.
pub async fn claim_issued(
    pool: &RetrievalWorkerDbPool,
    family: &ClaimFamily,
    lease_owner: &str,
    lease_seconds: f64,
    limit: i64,
    per_tenant_cap: i64,
) -> Result<ClaimBatch, StreamRepoError> {
    let rows = sqlx::query(
        "SELECT * FROM projection.claim_issued_tickets($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(&family.domain)
    .bind(&family.projection_kind)
    .bind(&family.projection_version)
    .bind(family.placement.as_db_str())
    .bind(lease_owner)
    .bind(lease_seconds)
    .bind(limit)
    .bind(per_tenant_cap)
    // dep: PostgreSQL(role_retrieval_worker) — the cross-tenant ticket claim definer (0176)
    .fetch_all(pool.pool())
    .await?;
    Ok(parse_claim_rows(&rows))
}

/// Per-row parse of the claim's result. A row whose placement does not parse becomes an
/// [`UnplaceableTicket`]; a row whose stream key itself does not parse (only possible if the
/// definer's `RETURNS TABLE` drifted from this build) is logged and left to its lease expiry —
/// it cannot even be addressed for a release.
fn parse_claim_rows(rows: &[sqlx::postgres::PgRow]) -> ClaimBatch {
    let mut batch = ClaimBatch::default();
    for row in rows {
        let head = (|| -> Result<UnplaceableTicket, sqlx::Error> {
            Ok(UnplaceableTicket {
                tenant_id: row.try_get("tenant_id")?,
                scope_kind: row.try_get("scope_kind")?,
                scope_id: row.try_get("scope_id")?,
                domain: row.try_get("domain")?,
                projection_kind: row.try_get("projection_kind")?,
                projection_version: row.try_get("projection_version")?,
                stream_seq: row.try_get("stream_seq")?,
                attempts: row.try_get("attempts")?,
            })
        })();
        let (head, commit_seq) = match head.and_then(|h| Ok((h, row.try_get("commit_seq")?))) {
            Ok(parsed) => parsed,
            Err(error) => {
                eprintln!("stream_repo: claimed row without a readable stream key: {error}");
                continue;
            }
        };
        match crate::placement_repo::from_row(row) {
            // The claim's one `tenant_id` column is the ticket's and, by the join, the
            // placement's; the parsed placement row carries it typed.
            Ok(placement) => batch.tickets.push(ClaimedTicket {
                key: StreamKey::new(
                    placement.tenant_id,
                    head.scope_kind,
                    head.scope_id,
                    head.domain,
                    head.projection_kind,
                    head.projection_version,
                ),
                stream_seq: head.stream_seq,
                commit_seq,
                attempts: head.attempts,
                placement,
            }),
            Err(_) => batch.unplaceable.push(head),
        }
    }
    batch
}

/// ADR-0052 D-C: per tenant, the ISSUED workspace tickets of `family`'s triple that
/// [`claim_issued`] will never take because the tenant has no placement row — the worker's
/// `placement_missing` line. Read-only owner definer `projection.unplaced_issued_tickets` (0176).
pub async fn unplaced_issued(
    pool: &RetrievalWorkerDbPool,
    family: &ClaimFamily,
) -> Result<Vec<(Uuid, i64)>, StreamRepoError> {
    let rows = sqlx::query("SELECT * FROM projection.unplaced_issued_tickets($1, $2, $3, $4)")
        .bind(&family.domain)
        .bind(&family.projection_kind)
        .bind(&family.projection_version)
        .bind(family.placement.as_db_str())
        // dep: PostgreSQL(role_retrieval_worker) — the cross-tenant unplaced-ticket count definer (0176)
        .fetch_all(pool.pool())
        .await?;
    rows.iter()
        .map(|row| Ok((row.try_get("tenant_id")?, row.try_get("tickets")?)))
        .collect()
}

/// ADR-0052 D-D heartbeat (ADR-0043's per-row cadence): extends every live lease `lease_owner`
/// holds on `key`'s family by `lease_seconds`, under the family's own tenant GUC and the role's
/// existing table-level UPDATE (no definer, no new grant). Returns the `stream_seq`s renewed; a
/// ticket missing from it lost its lease (expired and possibly re-claimed) and must not settle.
pub async fn renew_family_leases(
    pool: &RetrievalWorkerDbPool,
    key: &StreamKey,
    lease_owner: &str,
    lease_seconds: f64,
) -> Result<Vec<i64>, StreamRepoError> {
    // dep: PostgreSQL(role_retrieval_worker) — transaction entry for `renew_family_leases`
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, key.tenant_id.0).await?;
    let renewed = bind_key(
        sqlx::query(&format!(
            "UPDATE projection.stream_log \
                SET lease_expires_at = clock_timestamp() + make_interval(secs => $8) \
              WHERE {KEY_WHERE} AND state = 'ISSUED' AND lease_owner = $7 \
                AND lease_expires_at > clock_timestamp() \
             RETURNING stream_seq"
        )),
        key,
    )
    .bind(lease_owner)
    .bind(lease_seconds)
    .fetch_all(&mut *txn)
    .await?
    .iter()
    .map(|row| row.try_get::<i64, _>("stream_seq"))
    .collect::<Result<Vec<_>, _>>()?;
    txn.commit().await?;
    Ok(renewed)
}

/// ADR-0052 D-D retry: a transient failure returns the ticket to the pool — lease cleared, the
/// failure class kept in `error_class` (the next DONE settle writes NULL over it) and, with a
/// `backoff`, `next_attempt_at = now + min(base * 2^(attempts-1), max)` so the claim skips it
/// until then. `spend` says whether this failure counts toward `max_attempts`: a leased ticket
/// already spent its attempt at the claim, so `spend = false` gives it back — but never below 1,
/// so a ticket that has failed is always visibly a retry ([`RETRY_PREDICATE`]) and backs off at
/// least `base`. The legacy unleased read (`fence.lease_owner == None`) spends it here. Returns
/// `false` when the fence matched nothing (the lease was lost).
pub async fn release_for_retry(
    pool: &RetrievalWorkerDbPool,
    key: &StreamKey,
    stream_seq: i64,
    fence: TicketFence<'_>,
    error_class: &str,
    backoff: Option<Backoff>,
    spend: bool,
) -> Result<bool, StreamRepoError> {
    // attempts after this write = greatest(attempts + delta, 1): the claim's increment is undone
    // unless the failure is charged, and the unleased read charges here.
    let delta: i32 = i32::from(spend) - i32::from(fence.lease_owner.is_some());
    // dep: PostgreSQL(role_retrieval_worker) — transaction entry for `release_for_retry`
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, key.tenant_id.0).await?;
    let n = bind_key(
        sqlx::query(&format!(
            "UPDATE projection.stream_log \
                SET lease_owner = NULL, lease_expires_at = NULL, error_class = $10, \
                    attempts = greatest(attempts + $11, 1), \
                    next_attempt_at = CASE WHEN $12::double precision IS NULL THEN NULL \
                      ELSE clock_timestamp() + make_interval(secs => least( \
                        $12::double precision * power(2, greatest(attempts + $11, 1) - 1), \
                        $13::double precision)) END \
              WHERE {KEY_WHERE} AND stream_seq = $7 AND state = 'ISSUED' \
                AND lease_owner IS NOT DISTINCT FROM $8 AND attempts = $9"
        )),
        key,
    )
    .bind(stream_seq)
    .bind(fence.lease_owner)
    .bind(fence.attempts)
    .bind(error_class)
    .bind(delta)
    .bind(backoff.map(|b| b.base_secs))
    .bind(backoff.map(|b| b.max_secs))
    .execute(&mut *txn)
    .await?
    .rows_affected();
    txn.commit().await?;
    Ok(n == 1)
}

/// ADR-0052 D-D pending release: the ticket's Evidence is still being distilled (the claim's
/// distill-closed predicate lost a race). The lease is cleared and the claim's attempt is given
/// back, so a distill delay never counts toward `transient_exhausted`. Returns `false` when the
/// fence matched nothing (the lease was lost).
pub async fn release_pending(
    pool: &RetrievalWorkerDbPool,
    key: &StreamKey,
    stream_seq: i64,
    fence: TicketFence<'_>,
) -> Result<bool, StreamRepoError> {
    // dep: PostgreSQL(role_retrieval_worker) — transaction entry for `release_pending`
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, key.tenant_id.0).await?;
    let n = bind_key(
        sqlx::query(&format!(
            "UPDATE projection.stream_log \
                SET lease_owner = NULL, lease_expires_at = NULL, next_attempt_at = NULL, \
                    attempts = greatest(attempts - 1, 0) \
              WHERE {KEY_WHERE} AND stream_seq = $7 AND state = 'ISSUED' \
                AND lease_owner IS NOT DISTINCT FROM $8 AND attempts = $9"
        )),
        key,
    )
    .bind(stream_seq)
    .bind(fence.lease_owner)
    .bind(fence.attempts)
    .execute(&mut *txn)
    .await?
    .rows_affected();
    txn.commit().await?;
    Ok(n == 1)
}

#[cfg(test)]
mod claim_parse_tests {
    use super::parse_claim_rows;
    use crate::postgres::RetrievalWorkerDbPool;
    use humaux_testkit::{ExternalDep, skip_or_fail};

    /// Review 2026-09-29 P1: the claim's leases are committed before its rows are parsed, so one
    /// row whose placement this build cannot parse (deploy skew: the CHECK constraints admit
    /// only values this build knows today) must cost only itself. Two rows shaped exactly like
    /// `projection.claim_issued_tickets`' `RETURNS TABLE`, one with an unknown
    /// `promotion_state`: the good one is a ticket, the bad one an addressable
    /// `UnplaceableTicket`. Fault injection: collect with `?` as before ⇒ both are lost.
    #[test]
    fn one_unparsable_placement_row_costs_only_itself() {
        const TEST: &str = "one_unparsable_placement_row_costs_only_itself";
        let Ok(dsn) = std::env::var("HUMAUX_TEST_PG_DSN") else {
            skip_or_fail(
                TEST,
                "missing object: HUMAUX_TEST_PG_DSN",
                ExternalDep::Postgres,
            );
            return;
        };
        let sep = if dsn.contains('?') { '&' } else { '?' };
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let rows = rt.block_on(async {
            // dep: PostgreSQL(role_retrieval_worker) — a literal SELECT shaped like the claim's rows
            let pool = RetrievalWorkerDbPool::connect(&format!(
                "{dsn}{sep}options=-c%20role%3Drole_retrieval_worker"
            ))
            .await
            .expect("role_retrieval_worker connects");
            sqlx::query(
                "SELECT v.tenant_id, 'workspace'::text AS scope_kind, v.scope_id, \
                        'private_memory'::text AS domain, 'PRIVATE_MEMORY'::text AS projection_kind, \
                        'v1'::text AS projection_version, v.stream_seq, v.stream_seq + 100 AS commit_seq, \
                        v.attempts, 'private_memory_v1'::text AS projection_family, \
                        'c'::text AS collection_name, NULL::text AS shard_key, \
                        'SHARED_FALLBACK'::text AS placement_class, 0::bigint AS point_count, \
                        0::bigint AS bytes_estimate, v.promotion_state \
                   FROM (VALUES \
                     ('00000000-0000-0000-0000-00000000000a'::uuid, '00000000-0000-0000-0000-0000000000a1'::uuid, 1::bigint, 1, 'STABLE'), \
                     ('00000000-0000-0000-0000-00000000000b'::uuid, '00000000-0000-0000-0000-0000000000b1'::uuid, 7::bigint, 3, 'NOT_A_KNOWN_STATE') \
                   ) AS v(tenant_id, scope_id, stream_seq, attempts, promotion_state)",
            )
            // dep: PostgreSQL(role_retrieval_worker) — fabricated claim-shaped rows
            .fetch_all(pool.pool())
            .await
            .expect("fabricated rows")
        });
        let batch = parse_claim_rows(&rows);
        assert_eq!(batch.tickets.len(), 1, "{batch:?}");
        assert_eq!(batch.tickets[0].stream_seq, 1);
        assert_eq!(batch.tickets[0].commit_seq, 101);
        assert_eq!(batch.unplaceable.len(), 1, "{batch:?}");
        let bad = &batch.unplaceable[0];
        assert_eq!((bad.stream_seq, bad.attempts), (7, 3));
        assert_eq!(
            bad.tenant_id.to_string(),
            "00000000-0000-0000-0000-00000000000b"
        );
    }
}
