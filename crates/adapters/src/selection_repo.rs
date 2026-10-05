//! `adapters::selection_repo` — §20.4 Stable Selection / Pagination Contract SQL (T6.3, G20-1/G80-32).
//! Depends-on: crates=[humaux-domain, sqlx]; services=[PostgreSQL(role_retrieval_worker) r=[private.memory_records]
//!   w=[ops.selection_snapshot_items, ops.selection_snapshots]]; env=[]; modules=[adapters::postgres, domain::error,
//!   domain::selection]
//! Called-by: [adapters::context_repo, tests]
//! Invariants: [page 1 materializes the whole enumeration in one REPEATABLE READ transaction; later pages read only
//!   the immutable manifest behind a MAC-signed cursor; a mismatched query or unknown snapshot is a typed error;
//!   every page read, gateway and worker path alike, is the one MANIFEST_PAGE_SQL statement, which sees a snapshot
//!   with its whole manifest or no snapshot, and a snapshot the DB clock has expired is refused (and never continued
//!   into a new segment) even when the host-clock cursor check passed; a manifest is one INSERT statement;
//!   a gateway manifest stores at most its cap and records continues_before when it truncates]
//! Spec: Baseline §20.4; §22.1; ADR-0062 D-H; ADR-0062 D-K
//!
//! The worker API uses [`RetrievalWorkerDbPool`]; crate-private manifest
//! helpers also let `context_repo` authorize and materialize Gateway pages in one RR.
//!
//! [`begin_enumeration_snapshot`] is the Mode B "page 1" recipe: one `REPEATABLE READ
//! READ WRITE` transaction inserts the `ops.selection_snapshots` row, runs exactly one
//! `SELECT` against `private.memory_records` (the same single-transaction-snapshot technique
//! `consolidate_repo::select_and_materialize_inputs` already uses for Mode A — see that
//! module's doc comment), and materializes every matching row into
//! `ops.selection_snapshot_items` with a fixed `ordinal` — all before this function returns
//! the first page. [`fetch_enumeration_page`] never touches `private.memory_records` again:
//! every later page reads only the now-immutable manifest, ordered by `ordinal`, guarded by a
//! [`Cursor`] whose MAC that manifest's own creation signed (`humaux_domain::selection`).
//!
//! §20.4's own worked example is exactly this shape ("`memory.enumerate` / EXACT / Export
//! browser 不可能跨分钟保持 DB transaction") — this enumerates a tenant's `active`/
//! `TENANT_SHARED` memories, the same visibility slice `consolidate_repo::
//! select_and_materialize_inputs` selects from for Mode A, ordered `memory_id DESC` for the
//! identical reason that module's doc comment gives (UUIDv7 time-ordering makes a
//! concurrently-inserted row always sort first — the shape `tests/selection_snapshot.rs`'s
//! G20-1 fault injection needs).

use sqlx::Row;
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

use humaux_domain::selection::{Cursor, CursorError, query_fingerprint};

use crate::postgres::RetrievalWorkerDbPool;

/// §20.1's `predicate_id` for the one enumeration query this module runs — bound into every
/// snapshot's `query_fingerprint` (`humaux_domain::selection::query_fingerprint`) so a cursor
/// from a different predicate can never validate against this manifest.
pub const ENUMERATE_ACTIVE_MEMORY_RECORDS_V1: &str = "enumerate_active_memory_records_v1";

/// DB-layer failure. Adapter-local, not one of the workspace's two frozen domain error enums
/// (§52) — same reasoning as `consolidate_repo::ConsolidateRepoError` / `jobs::JobsError`.
#[derive(Debug)]
pub enum SelectionRepoError {
    Db(sqlx::Error),
    /// §20.4's cursor checks (`humaux_domain::selection::Cursor::validate`) — cross-tenant
    /// reuse, a forged/tampered field, or an expired cursor.
    Cursor(CursorError),
    /// The cursor's `snapshot_id` names no row this tenant can see — either it never existed
    /// under this tenant (RLS makes a cross-tenant id indistinguishable from a nonexistent
    /// one, which is the point) or the maintenance purge door (ADR-0062 D-H) has since removed it.
    SnapshotNotFound,
    /// The `ops.selection_snapshots` row's own `query_fingerprint` no longer matches the
    /// cursor's — defense in depth alongside the MAC (see this crate's `0083` migration
    /// comment): should be unreachable given a validly-MAC'd cursor, since the fingerprint is
    /// itself inside the MAC, but checked explicitly rather than assumed.
    QueryMismatch,
}

impl From<sqlx::Error> for SelectionRepoError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

impl From<CursorError> for SelectionRepoError {
    fn from(e: CursorError) -> Self {
        Self::Cursor(e)
    }
}

impl std::fmt::Display for SelectionRepoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(e) => write!(f, "selection_repo DB error: {e}"),
            Self::Cursor(e) => write!(f, "selection_repo cursor rejected: {e:?}"),
            Self::SnapshotNotFound => write!(f, "selection_repo: snapshot not found for tenant"),
            Self::QueryMismatch => write!(
                f,
                "selection_repo: cursor query_fingerprint does not match snapshot row"
            ),
        }
    }
}

impl std::error::Error for SelectionRepoError {}

/// One page of a §20.4 Mode B enumeration. `next_cursor` is `None` once the manifest is
/// exhausted — the caller has no way to keep paging past the snapshot's own universe.
#[derive(Debug, Clone)]
pub struct SnapshotPage {
    pub snapshot_id: Uuid,
    pub items: Vec<Uuid>,
    pub next_cursor: Option<Cursor>,
}

async fn set_tenant_local(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
) -> Result<(), SelectionRepoError> {
    // Same technique as every other `adapters::*_repo::set_tenant_local` (`tenant_id` is a
    // `Uuid`, never attacker-controlled text — not a SQL injection surface).
    sqlx::query(&format!("SET LOCAL humaux.tenant_id = '{tenant_id}'"))
        .execute(&mut **txn)
        .await?;
    Ok(())
}

/// ADR-0062 D-K: the whole manifest is one statement; `ids` order becomes `ordinal` 0..N-1, so the caller's
/// `memory_id DESC` order is the page order. Replaces a per-id INSERT loop (N round trips inside the minting
/// REPEATABLE READ transaction, P1-14).
async fn insert_manifest_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    snapshot_id: Uuid,
    tenant_id: Uuid,
    ids: &[Uuid],
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO ops.selection_snapshot_items (selection_snapshot_id, tenant_id, item_id, ordinal) \
         SELECT $1, $2, u.id, u.ord - 1 FROM unnest($3::uuid[]) WITH ORDINALITY AS u(id, ord)",
    )
    .bind(snapshot_id)
    .bind(tenant_id)
    .bind(ids)
    .execute(&mut **txn)
    .await?;
    Ok(())
}

/// Bundles the three fields both `begin_enumeration_snapshot` and `fetch_enumeration_page`
/// already know about a snapshot before reading its manifest — keeps
/// `fetch_page_from_manifest` under clippy's `too_many_arguments` threshold without inventing
/// a type anyone outside this module needs.
struct SnapshotMeta<'a> {
    snapshot_id: Uuid,
    query_fingerprint: &'a str,
    expires_at: OffsetDateTime,
}

fn build_page(
    snapshot_id: Uuid,
    tenant_id: Uuid,
    query_fingerprint: &str,
    expires_at_unix: i64,
    rows: &[(Uuid, i64)],
    requested_page_size: i64,
    mac_key: &[u8],
) -> SnapshotPage {
    let items: Vec<Uuid> = rows.iter().map(|(item, _)| *item).collect();
    let last_ordinal: Option<i64> = rows.last().map(|(_, ordinal)| *ordinal);
    // A full page might still be the last one (universe size an exact multiple of
    // page_size) — that costs one extra empty-page round trip in the rare exact-multiple
    // case, never a missed or duplicated row, so it is not worth a second COUNT query to
    // avoid (ladder rung 1: this task's gate does not ask for that precision).
    let next_cursor = if items.len() as i64 == requested_page_size {
        last_ordinal.map(|ordinal| {
            Cursor::sign(
                snapshot_id,
                tenant_id,
                query_fingerprint.to_string(),
                ordinal,
                expires_at_unix,
                mac_key,
            )
        })
    } else {
        None
    };
    SnapshotPage {
        snapshot_id,
        items,
        next_cursor,
    }
}

/// §20.4 Mode B, page 1. Opens one `REPEATABLE READ READ WRITE` transaction (`READ ONLY`
/// rejects the `INSERT`s below with SQLSTATE 25006 — same frozen recipe
/// `consolidate_repo::select_and_materialize_inputs` documents), creates the snapshot row
/// (establishing the transaction view), runs one source `SELECT` against `private.memory_records`,
/// and materializes every matching id into `ops.selection_snapshot_items` before committing.
/// The first page is then read back from that now-immutable manifest.
pub async fn begin_enumeration_snapshot(
    pool: &RetrievalWorkerDbPool,
    tenant_id: Uuid,
    ttl_seconds: f64,
    page_size: i64,
    mac_key: &[u8],
) -> Result<SnapshotPage, SelectionRepoError> {
    let fingerprint = query_fingerprint(ENUMERATE_ACTIVE_MEMORY_RECORDS_V1, tenant_id);

    // dep: PostgreSQL(role_retrieval_worker) — transaction entry for `begin_enumeration_snapshot`
    let mut txn = pool.pool().begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ WRITE")
        .execute(&mut *txn)
        .await?;
    set_tenant_local(&mut txn, tenant_id).await?;

    let snapshot_row = sqlx::query(
        "INSERT INTO ops.selection_snapshots (tenant_id, query_fingerprint, expires_at) \
         VALUES ($1, $2, now() + make_interval(secs => $3)) \
         RETURNING selection_snapshot_id, expires_at",
    )
    .bind(tenant_id)
    .bind(&fingerprint)
    .bind(ttl_seconds)
    .fetch_one(&mut *txn)
    .await?;
    let snapshot_id: Uuid = snapshot_row.get(0);
    let expires_at: OffsetDateTime = snapshot_row.get(1);

    // The snapshot-row INSERT above established the REPEATABLE READ view. This sole
    // source SELECT and the later manifest INSERTs use that same transaction view.
    // `ORDER BY memory_id DESC` matches
    // `consolidate_repo::select_and_materialize_inputs`'s ordering and reasoning verbatim:
    // `memory_id` is UUIDv7 (time-ordered), so a row concurrently inserted while this
    // transaction is open always sorts first under DESC — the exact "并发插入 ... 更靠前的
    // 行" shape §20.4's G20-1 fault injection asks for.
    let rows = sqlx::query(
        "SELECT memory_id FROM private.memory_records \
         WHERE tenant_id = $1 AND status = 'active' AND visibility_class = 'TENANT_SHARED' \
         ORDER BY memory_id DESC",
    )
    .bind(tenant_id)
    .fetch_all(&mut *txn)
    .await?;

    let ids: Vec<Uuid> = rows.iter().map(|row| row.get(0)).collect();
    insert_manifest_in_txn(&mut txn, snapshot_id, tenant_id, &ids).await?;
    txn.commit().await?;

    let meta = SnapshotMeta {
        snapshot_id,
        query_fingerprint: &fingerprint,
        expires_at,
    };
    fetch_page_from_manifest(pool, tenant_id, &meta, -1, page_size, mac_key).await
}

/// §20.4 Mode B, page 2+. Validates the cursor (`humaux_domain::selection::Cursor::validate`
/// — cross-tenant reuse, MAC tampering, expiry, in that order) before issuing any query, then
/// reads only `ops.selection_snapshot_items` — the live `private.memory_records` table is
/// never touched again for this snapshot.
pub async fn fetch_enumeration_page(
    pool: &RetrievalWorkerDbPool,
    tenant_id: Uuid,
    cursor: &Cursor,
    page_size: i64,
    mac_key: &[u8],
) -> Result<SnapshotPage, SelectionRepoError> {
    let now_unix = OffsetDateTime::now_utc().unix_timestamp();
    cursor.validate(tenant_id, mac_key, now_unix)?;

    let expires_at = OffsetDateTime::from_unix_timestamp(cursor.expires_at_unix)
        .map_err(|_| SelectionRepoError::Cursor(CursorError::Malformed("bad expires_at_unix")))?;

    let meta = SnapshotMeta {
        snapshot_id: cursor.snapshot_id,
        query_fingerprint: &cursor.query_fingerprint,
        expires_at,
    };
    fetch_page_from_manifest(
        pool,
        tenant_id,
        &meta,
        cursor.last_ordinal,
        page_size,
        mac_key,
    )
    .await
}

/// ADR-0062 D-H: the one page statement, read by every page of both enumeration paths (the gateway's
/// `memory.enumerate` and the worker API). The snapshot row, its DB-clock liveness and the page of items come from
/// one MVCC snapshot, and the purge door deletes a snapshot with its items in one statement, so a read sees the
/// whole manifest or no snapshot row: never a found snapshot with an empty page. `live` uses the purge's own clock
/// (`expires_at < now()` there). Public so the race test runs the production text.
pub const MANIFEST_PAGE_SQL: &str = "SELECT s.query_fingerprint, s.expires_at > now() AS live, s.continues_before, \
     i.item_id, i.ordinal \
     FROM ops.selection_snapshots s \
     LEFT JOIN LATERAL (SELECT item_id, ordinal FROM ops.selection_snapshot_items \
                         WHERE selection_snapshot_id = s.selection_snapshot_id AND ordinal > $2 \
                         ORDER BY ordinal LIMIT $3) i ON true \
     WHERE s.selection_snapshot_id = $1 \
     ORDER BY i.ordinal";

/// One page of a manifest as [`MANIFEST_PAGE_SQL`] read it, before either caller maps it to its own error type.
struct ManifestRead {
    query_fingerprint: String,
    live: bool,
    continues_before: Option<Uuid>,
    items: Vec<(Uuid, i64)>,
}

/// The one caller of [`MANIFEST_PAGE_SQL`]: `None` when no snapshot row is visible (RLS makes a cross-tenant id read
/// like a nonexistent or purged one).
async fn read_manifest_page(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    snapshot_id: Uuid,
    after_ordinal: i64,
    page_size: i64,
) -> Result<Option<ManifestRead>, sqlx::Error> {
    let rows = sqlx::query(MANIFEST_PAGE_SQL)
        .bind(snapshot_id)
        .bind(after_ordinal)
        .bind(page_size)
        .fetch_all(&mut **txn)
        .await?;
    let Some(first) = rows.first() else {
        return Ok(None);
    };
    // LEFT JOIN: a snapshot with no item past `after_ordinal` is one row with a NULL item.
    let mut items = Vec::with_capacity(rows.len());
    for row in &rows {
        if let Some(item) = row.try_get::<Option<Uuid>, _>("item_id")? {
            items.push((item, row.try_get::<i64, _>("ordinal")?));
        }
    }
    Ok(Some(ManifestRead {
        query_fingerprint: first.try_get("query_fingerprint")?,
        live: first.try_get("live")?,
        continues_before: first.try_get("continues_before")?,
        items,
    }))
}

/// The worker API's page read (page 1 right after materialization with `after_ordinal = -1`, page 2+ with the
/// previous page's `last_ordinal`) through [`read_manifest_page`]: no row is `SnapshotNotFound`; a snapshot the DB
/// clock has expired is `Cursor(Expired)` even when the host-clock cursor check passed; a fingerprint that no longer
/// matches is `QueryMismatch` — defense in depth alongside the MAC, per this module's `QueryMismatch` doc.
async fn fetch_page_from_manifest(
    pool: &RetrievalWorkerDbPool,
    tenant_id: Uuid,
    meta: &SnapshotMeta<'_>,
    after_ordinal: i64,
    page_size: i64,
    mac_key: &[u8],
) -> Result<SnapshotPage, SelectionRepoError> {
    // dep: PostgreSQL(role_retrieval_worker) — transaction entry for `fetch_page_from_manifest`
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, tenant_id).await?;
    let read = read_manifest_page(&mut txn, meta.snapshot_id, after_ordinal, page_size).await?;
    txn.commit().await?;

    let Some(read) = read else {
        return Err(SelectionRepoError::SnapshotNotFound);
    };
    if !read.live {
        return Err(SelectionRepoError::Cursor(CursorError::Expired));
    }
    if read.query_fingerprint != meta.query_fingerprint {
        return Err(SelectionRepoError::QueryMismatch);
    }
    Ok(build_page(
        meta.snapshot_id,
        tenant_id,
        meta.query_fingerprint,
        meta.expires_at.unix_timestamp(),
        &read.items,
        page_size,
        mac_key,
    ))
}

/// The gateway's page read (`memory.enumerate`, every page) through [`read_manifest_page`] in the caller's
/// REPEATABLE READ transaction. A snapshot that is gone, DB-clock expired (ADR-0062 D-H: the authority, the same
/// clock as the purge), or minted under another fingerprint is `NotFound`, the gateway's answer for an expired or
/// foreign cursor.
async fn authorized_page_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    meta: &SnapshotMeta<'_>,
    after_ordinal: i64,
    page_size: i64,
    mac_key: &[u8],
) -> Result<SnapshotPage, humaux_domain::error::ErrorCode> {
    let read = read_manifest_page(txn, meta.snapshot_id, after_ordinal, page_size)
        .await
        .map_err(|_| humaux_domain::error::ErrorCode::DependencyUnavailable)?
        .filter(|read| read.live && read.query_fingerprint == meta.query_fingerprint)
        .ok_or(humaux_domain::error::ErrorCode::NotFound)?;
    let last_ordinal = read
        .items
        .last()
        .map_or(after_ordinal, |(_, ordinal)| *ordinal);
    // ADR-0062 D-K: a short page on a capped manifest still answers a cursor (at the manifest's end), which
    // [`continuation_bound_in_txn`] turns into the next segment; an uncapped manifest ends at its short page.
    let next_cursor = (read.items.len() == page_size as usize || read.continues_before.is_some())
        .then(|| {
            Cursor::sign(
                meta.snapshot_id,
                tenant_id,
                meta.query_fingerprint.to_owned(),
                last_ordinal,
                meta.expires_at.unix_timestamp(),
                mac_key,
            )
        });
    Ok(SnapshotPage {
        snapshot_id: meta.snapshot_id,
        items: read.items.into_iter().map(|(item, _)| item).collect(),
        next_cursor,
    })
}

/// What one authorized manifest is minted under: its query identity, lifetime and size cap.
pub(crate) struct ManifestSpec<'a> {
    /// `context_repo`'s enumeration fingerprint (caller identity + filter), checked on every later page.
    pub fingerprint: &'a str,
    /// `HUMAUX_GATEWAY_ENUMERATION_TTL_SECONDS` (§78.1, ADR-0062 D-K).
    pub ttl: std::time::Duration,
    /// `HUMAUX_GATEWAY_ENUMERATION_MANIFEST_CAP` (> 0): the most item rows one manifest stores.
    pub cap: usize,
}

/// Creates an immutable authorization-filtered manifest in the caller's RR READ WRITE transaction. `item_ids` is
/// the whole authorized universe in `memory_id DESC` order; the manifest stores at most `spec.cap` of them and, when
/// it truncates, records the last stored id as `continues_before` (ADR-0062 D-K), the next segment's keyset bound.
pub(crate) async fn begin_authorized_snapshot_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    spec: &ManifestSpec<'_>,
    page_size: i64,
    mac_key: &[u8],
    item_ids: &[Uuid],
) -> Result<SnapshotPage, humaux_domain::error::ErrorCode> {
    let stored = &item_ids[..item_ids.len().min(spec.cap)];
    let continues_before = (stored.len() < item_ids.len())
        .then(|| stored.last().copied())
        .flatten();
    let row = sqlx::query("INSERT INTO ops.selection_snapshots(tenant_id,query_fingerprint,expires_at,continues_before) VALUES($1,$2,now()+make_interval(secs=>$3),$4) RETURNING selection_snapshot_id,expires_at")
        .bind(tenant_id).bind(spec.fingerprint).bind(spec.ttl.as_secs_f64()).bind(continues_before).fetch_one(&mut **txn).await
        .map_err(|_| humaux_domain::error::ErrorCode::DependencyUnavailable)?;
    let meta = SnapshotMeta {
        snapshot_id: row
            .try_get("selection_snapshot_id")
            .map_err(|_| humaux_domain::error::ErrorCode::Internal)?,
        query_fingerprint: spec.fingerprint,
        expires_at: row
            .try_get("expires_at")
            .map_err(|_| humaux_domain::error::ErrorCode::Internal)?,
    };
    insert_manifest_in_txn(txn, meta.snapshot_id, tenant_id, stored)
        .await
        .map_err(|_| humaux_domain::error::ErrorCode::DependencyUnavailable)?;
    authorized_page_in_txn(txn, tenant_id, &meta, -1, page_size, mac_key).await
}

/// The §20.4 cursor checks of the gateway path, in order: MAC/tenant/expiry on the host clock (an expired cursor is
/// `NotFound`, anything forged is `InvalidInput`), then the caller's own enumeration fingerprint.
fn validate_trusted_cursor(
    cursor: &Cursor,
    tenant_id: Uuid,
    fingerprint: &str,
    mac_key: &[u8],
) -> Result<OffsetDateTime, humaux_domain::error::ErrorCode> {
    cursor
        .validate(
            tenant_id,
            mac_key,
            OffsetDateTime::now_utc().unix_timestamp(),
        )
        .map_err(|error| match error {
            CursorError::Expired => humaux_domain::error::ErrorCode::NotFound,
            _ => humaux_domain::error::ErrorCode::InvalidInput,
        })?;
    if cursor.query_fingerprint != fingerprint {
        return Err(humaux_domain::error::ErrorCode::NotFound);
    }
    OffsetDateTime::from_unix_timestamp(cursor.expires_at_unix)
        .map_err(|_| humaux_domain::error::ErrorCode::InvalidInput)
}

/// ADR-0062 D-K: `Some(bound)` when `cursor` stands at the end of a live capped manifest, i.e. the next page is the
/// first page of a new segment `memory_id < bound`; `None` when it is an ordinary page of the frozen manifest, or
/// the snapshot is gone or DB-clock expired (ADR-0062 D-H), which the page read then refuses. One statement, run
/// before the caller picks its transaction mode (a new segment mints, so it needs READ WRITE).
pub(crate) async fn continuation_bound_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    cursor: &Cursor,
    fingerprint: &str,
    mac_key: &[u8],
) -> Result<Option<Uuid>, humaux_domain::error::ErrorCode> {
    validate_trusted_cursor(cursor, tenant_id, fingerprint, mac_key)?;
    let bound: Option<Option<Uuid>> = sqlx::query_scalar(
        "SELECT s.continues_before FROM ops.selection_snapshots s \
          WHERE s.selection_snapshot_id = $1 AND s.tenant_id = $2 AND s.expires_at > now() \
            AND NOT EXISTS (SELECT 1 FROM ops.selection_snapshot_items i \
                             WHERE i.selection_snapshot_id = s.selection_snapshot_id AND i.ordinal > $3)",
    )
    .bind(cursor.snapshot_id)
    .bind(tenant_id)
    .bind(cursor.last_ordinal)
    .fetch_optional(&mut **txn)
    .await
    .map_err(|_| humaux_domain::error::ErrorCode::DependencyUnavailable)?;
    Ok(bound.flatten())
}

/// Validates a trusted cursor and returns its frozen manifest page in the caller's RR transaction.
pub(crate) async fn fetch_authorized_snapshot_page_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    cursor: &Cursor,
    fingerprint: &str,
    page_size: i64,
    mac_key: &[u8],
) -> Result<SnapshotPage, humaux_domain::error::ErrorCode> {
    let expires_at = validate_trusted_cursor(cursor, tenant_id, fingerprint, mac_key)?;
    authorized_page_in_txn(
        txn,
        tenant_id,
        &SnapshotMeta {
            snapshot_id: cursor.snapshot_id,
            query_fingerprint: fingerprint,
            expires_at,
        },
        cursor.last_ordinal,
        page_size,
        mac_key,
    )
    .await
}
