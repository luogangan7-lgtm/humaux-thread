//! `adapters::selection_repo` — §20.4 Stable Selection / Pagination Contract SQL (T6.3,
//! G20-1/G80-32). The worker API uses [`RetrievalWorkerDbPool`]; crate-private manifest
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
    /// one, which is the point) or `role_maintenance`'s future sweep job has since removed it.
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
    rows: Vec<sqlx::postgres::PgRow>,
    requested_page_size: i64,
    mac_key: &[u8],
) -> SnapshotPage {
    let items: Vec<Uuid> = rows.iter().map(|r| r.get::<Uuid, _>(0)).collect();
    let last_ordinal: Option<i64> = rows.last().map(|r| r.get::<i64, _>(1));
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

    for (ordinal, row) in rows.iter().enumerate() {
        let item_id: Uuid = row.get(0);
        sqlx::query(
            "INSERT INTO ops.selection_snapshot_items \
               (selection_snapshot_id, tenant_id, item_id, ordinal) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(snapshot_id)
        .bind(tenant_id)
        .bind(item_id)
        .bind(ordinal as i64)
        .execute(&mut *txn)
        .await?;
    }
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

/// Shared read path for both page 1 (called right after materialization, `after_ordinal =
/// -1`) and page 2+ (called with the previous page's `last_ordinal`). Confirms the snapshot
/// row is still visible under this tenant (RLS-scoped — a cross-tenant or nonexistent
/// `snapshot_id` both read back zero rows, hence one shared `SnapshotNotFound`) and that its
/// `query_fingerprint` still matches before reading the manifest — defense in depth alongside
/// the MAC, per this module's `QueryMismatch` doc.
async fn fetch_page_from_manifest(
    pool: &RetrievalWorkerDbPool,
    tenant_id: Uuid,
    meta: &SnapshotMeta<'_>,
    after_ordinal: i64,
    page_size: i64,
    mac_key: &[u8],
) -> Result<SnapshotPage, SelectionRepoError> {
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, tenant_id).await?;

    let snapshot_fp: Option<String> = sqlx::query_scalar(
        "SELECT query_fingerprint FROM ops.selection_snapshots WHERE selection_snapshot_id = $1",
    )
    .bind(meta.snapshot_id)
    .fetch_optional(&mut *txn)
    .await?;
    let Some(snapshot_fp) = snapshot_fp else {
        return Err(SelectionRepoError::SnapshotNotFound);
    };
    if snapshot_fp != meta.query_fingerprint {
        return Err(SelectionRepoError::QueryMismatch);
    }

    let rows = sqlx::query(
        "SELECT item_id, ordinal FROM ops.selection_snapshot_items \
         WHERE selection_snapshot_id = $1 AND ordinal > $2 \
         ORDER BY ordinal LIMIT $3",
    )
    .bind(meta.snapshot_id)
    .bind(after_ordinal)
    .bind(page_size)
    .fetch_all(&mut *txn)
    .await?;
    txn.commit().await?;

    Ok(build_page(
        meta.snapshot_id,
        tenant_id,
        meta.query_fingerprint,
        meta.expires_at.unix_timestamp(),
        rows,
        page_size,
        mac_key,
    ))
}

async fn authorized_page_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    meta: &SnapshotMeta<'_>,
    after_ordinal: i64,
    page_size: i64,
    mac_key: &[u8],
) -> Result<SnapshotPage, humaux_domain::error::ErrorCode> {
    let snapshot_fp: Option<String> = sqlx::query_scalar(
        "SELECT query_fingerprint FROM ops.selection_snapshots WHERE selection_snapshot_id=$1 AND tenant_id=$2",
    )
    .bind(meta.snapshot_id)
    .bind(tenant_id)
    .fetch_optional(&mut **txn)
    .await
    .map_err(|_| humaux_domain::error::ErrorCode::DependencyUnavailable)?;
    if snapshot_fp.as_deref() != Some(meta.query_fingerprint) {
        return Err(humaux_domain::error::ErrorCode::NotFound);
    }
    let rows = sqlx::query(
        "SELECT item_id,ordinal FROM ops.selection_snapshot_items WHERE selection_snapshot_id=$1 AND tenant_id=$2 AND ordinal>$3 ORDER BY ordinal LIMIT $4",
    )
    .bind(meta.snapshot_id)
    .bind(tenant_id)
    .bind(after_ordinal)
    .bind(page_size)
    .fetch_all(&mut **txn)
    .await
    .map_err(|_| humaux_domain::error::ErrorCode::DependencyUnavailable)?;
    let items = rows
        .iter()
        .map(|row| {
            row.try_get("item_id")
                .map_err(|_| humaux_domain::error::ErrorCode::Internal)
        })
        .collect::<Result<Vec<Uuid>, _>>()?;
    let next_cursor = if items.len() == page_size as usize {
        rows.last()
            .map(|row| {
                row.try_get("ordinal")
                    .map_err(|_| humaux_domain::error::ErrorCode::Internal)
            })
            .transpose()?
            .map(|ordinal| {
                Cursor::sign(
                    meta.snapshot_id,
                    tenant_id,
                    meta.query_fingerprint.to_owned(),
                    ordinal,
                    meta.expires_at.unix_timestamp(),
                    mac_key,
                )
            })
    } else {
        None
    };
    Ok(SnapshotPage {
        snapshot_id: meta.snapshot_id,
        items,
        next_cursor,
    })
}

/// Creates an immutable authorization-filtered manifest in the caller's RR READ WRITE transaction.
pub(crate) async fn begin_authorized_snapshot_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    fingerprint: &str,
    ttl: std::time::Duration,
    page_size: i64,
    mac_key: &[u8],
    item_ids: &[Uuid],
) -> Result<SnapshotPage, humaux_domain::error::ErrorCode> {
    let row = sqlx::query("INSERT INTO ops.selection_snapshots(tenant_id,query_fingerprint,expires_at) VALUES($1,$2,now()+make_interval(secs=>$3)) RETURNING selection_snapshot_id,expires_at")
        .bind(tenant_id).bind(fingerprint).bind(ttl.as_secs_f64()).fetch_one(&mut **txn).await
        .map_err(|_| humaux_domain::error::ErrorCode::DependencyUnavailable)?;
    let meta = SnapshotMeta {
        snapshot_id: row
            .try_get("selection_snapshot_id")
            .map_err(|_| humaux_domain::error::ErrorCode::Internal)?,
        query_fingerprint: fingerprint,
        expires_at: row
            .try_get("expires_at")
            .map_err(|_| humaux_domain::error::ErrorCode::Internal)?,
    };
    for (ordinal, id) in item_ids.iter().enumerate() {
        sqlx::query("INSERT INTO ops.selection_snapshot_items(selection_snapshot_id,tenant_id,item_id,ordinal) VALUES($1,$2,$3,$4)")
            .bind(meta.snapshot_id).bind(tenant_id).bind(id).bind(ordinal as i64).execute(&mut **txn).await
            .map_err(|_| humaux_domain::error::ErrorCode::DependencyUnavailable)?;
    }
    authorized_page_in_txn(txn, tenant_id, &meta, -1, page_size, mac_key).await
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
    let expires_at = OffsetDateTime::from_unix_timestamp(cursor.expires_at_unix)
        .map_err(|_| humaux_domain::error::ErrorCode::InvalidInput)?;
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
