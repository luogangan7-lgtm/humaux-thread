//! `adapters::batch` — T3.1 `begin_batch` (§15.6.1 / §34.2 / §60 transaction A).
//!
//! **This is the only writer of `private.ingest_tickets.batch_id`/`ordinal`/`redeemed_event_id
//! = NULL` rows in the workspace.** [`begin_batch`] runs against [`BatchIssuerDbPool`]
//! (`role_batch_issuer`) — a pool this module never shares with [`crate::remember`]'s
//! [`crate::postgres::RuntimeDbPool`] (`role_gateway`), by construction (§34.2 "两个 Pool 使用
//! 不同 CredentialRef"). `role_batch_issuer` holds `INSERT`/`SELECT` on `ingest_tickets` and
//! nothing on `private.events` / `ops.outbox` / `projection.stream_log` (§60.1's authorization
//! table, live in `migrations/0011_roles_and_grants.sql`) — this module could not write a
//! matching Evidence row even if it tried, which is the other half of "自发票权限" being
//! structurally impossible, not just undesired.
//!
//! §15.6.1 freezes what this function may and may not do: `declared_count` is the caller's own
//! number, taken as given (no re-derivation from a census or a prior write); `expected` is
//! frozen the instant this transaction commits, before a single Evidence exists (§60.1 point 1
//! — "分母外生"). There is no `end_batch` (§15.6: "让被测对象声明'我写完了'等于让它自己定
//! 分母，正是坑 1 的形状") — the only way a batch's ticket count ever changes after this call is
//! the §15.2-style `EXPIRED` sweep (out of this task's scope, see this module's `mod tests` doc).

use sqlx::Row;
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

use crate::postgres::BatchIssuerDbPool;

/// DB-layer failure. Adapter-local, not one of the workspace's two frozen domain error enums
/// (§52) — same reasoning as `email::OutboxError` / `jobs::JobsError` / `postgres::PoolInitError`.
#[derive(Debug)]
pub enum BatchError {
    Db(sqlx::Error),
}

impl From<sqlx::Error> for BatchError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

impl std::fmt::Display for BatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(e) => write!(f, "begin_batch DB error: {e}"),
        }
    }
}

impl std::error::Error for BatchError {}

/// `begin_batch(scope, client_batch_id, declared_count)` input (§34.2). `scope_kind`/
/// `scope_id` mirror `private.ingest_tickets`' own columns — a generic (kind, id) locator, not
/// the six-column `projection.stream_log` key (`humaux_projection::stream::StreamKey`, a
/// different, wider identity `crate::remember` binds separately).
#[derive(Debug, Clone)]
pub struct BeginBatchCommand {
    pub tenant_id: Uuid,
    pub scope_kind: String,
    pub scope_id: Uuid,
    /// Caller-chosen idempotency key for this batch (§15.6.1 "整批重放幂等" — the same
    /// `(tenant_id, client_batch_id)` calling `begin_batch` again must not mint a second
    /// `batch_id` or double the ticket rows, see [`begin_batch`]'s doc).
    pub client_batch_id: String,
    /// `N` — frozen as `expected` the moment this call commits (§15.6.1 point 3). Caller's own
    /// number, never re-derived here.
    pub declared_count: u32,
    pub expires_at: OffsetDateTime,
}

/// [`begin_batch`]'s result (§34.2 / §60 `BatchIssued`).
#[derive(Debug, Clone, Copy)]
pub struct BatchIssued {
    pub batch_id: Uuid,
    /// §34.2 frozen semantics: the **persisted** ticket COUNT for this `client_batch_id` after
    /// transaction A commits — `declared_count` on a fresh issue, and the *same* value again on
    /// a full replay (a crash-retry must learn the batch exists, not read `0`). It is read back
    /// from `count(*) FROM ingest_tickets WHERE (tenant_id, client_batch_id)`, never derived from
    /// the current call's `rows_affected()` (which is 0 on replay). Still **not** `expected`
    /// itself — the frozen denominator is read at recall time — but `issued` is now the authoritative
    /// post-commit row count, not call-local bookkeeping.
    pub issued: u32,
    /// The batch window fixed at first issue (read back from the persisted rows, §34.2) — a
    /// replay with a different caller-supplied `expires_at` still returns the original window.
    pub expires_at: OffsetDateTime,
}

/// §60 transaction A, §15.6.1 verbatim: one `role_batch_issuer` transaction, one `INSERT ...
/// SELECT generate_series` writing `1..declared_count` dense ordinals with `redeemed_event_id
/// IS NULL` / `state = 'ISSUED'`, `ON CONFLICT (tenant_id, client_batch_id, ordinal) DO NOTHING`.
///
/// Idempotent replay (§60 "整批重放幂等"): a second call with the same `(tenant_id,
/// client_batch_id)` first looks up whether any ticket already carries that pair — if so, reuses
/// that row's `batch_id` (never mints a second one for the same logical batch) and the `ON
/// CONFLICT DO NOTHING` insert is a no-op for every ordinal that already exists, so the ticket
/// count stays at whatever it already was (never doubles) and `issued` (read back post-insert)
/// reports that stable count, not this call's `rows_affected()`. A fresh
/// `(tenant_id, client_batch_id)` mints a new `batch_id` via `SELECT uuidv7()` — the same
/// function `private.ingest_tickets.ticket_id`'s own `DEFAULT` calls, kept in Postgres rather
/// than minted client-side so this module needs no `uuid` crate dependency of its own (`uuid`'s
/// `v7` feature is already enabled workspace-wide by `humaux-domain`, but relying on that
/// cross-crate feature unification for a value this function must get right — the batch
/// identity — would be a silent coupling; asking Postgres is the same UUIDv7 generator the
/// schema already trusts, with no such coupling).
pub async fn begin_batch(
    pool: &BatchIssuerDbPool,
    cmd: BeginBatchCommand,
) -> Result<BatchIssued, BatchError> {
    let mut txn = pool.pool().begin().await?;

    // §62: forced RLS on private.ingest_tickets keys off humaux.tenant_id. `role_batch_issuer`
    // is not BYPASSRLS (§48.2), so without this the INSERT below would violate the policy's
    // WITH CHECK and fail — this is defense in depth alongside the application-level tenant
    // check, not a substitute for it (§6.1.1).
    sqlx::query(&format!("SET LOCAL humaux.tenant_id = '{}'", cmd.tenant_id))
        .execute(&mut *txn)
        .await?;

    // Serialize concurrent same-key calls for the life of this txn. Without it two overlapping
    // first-calls each fail the existence SELECT (neither committed under READ COMMITTED), each
    // mint a *different* batch_id, and the loser's INSERT fully conflicts — leaving it to return
    // a phantom batch_id that matches no persisted row (§34.2: the returned batch_id must be
    // usable for a subsequent remember). The lock makes the loser wait, then see the winner's
    // committed rows. Keyed on (tenant_id, client_batch_id) so unrelated batches never contend.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("{}:{}", cmd.tenant_id, cmd.client_batch_id))
        .execute(&mut *txn)
        .await?;

    let existing: Option<Uuid> = sqlx::query(
        "SELECT batch_id FROM private.ingest_tickets \
         WHERE tenant_id = $1 AND client_batch_id = $2 LIMIT 1",
    )
    .bind(cmd.tenant_id)
    .bind(&cmd.client_batch_id)
    .fetch_optional(&mut *txn)
    .await?
    .map(|row| row.try_get::<Uuid, _>("batch_id"))
    .transpose()?;

    let batch_id = match existing {
        Some(id) => id,
        None => sqlx::query("SELECT uuidv7() AS batch_id")
            .fetch_one(&mut *txn)
            .await?
            .try_get("batch_id")?,
    };

    sqlx::query(
        "INSERT INTO private.ingest_tickets \
           (tenant_id, scope_kind, scope_id, batch_id, client_batch_id, ordinal, expires_at) \
         SELECT $1, $2, $3, $4, $5, gs, $6 \
         FROM generate_series(1, $7::int) AS gs \
         ON CONFLICT (tenant_id, client_batch_id, ordinal) DO NOTHING",
    )
    .bind(cmd.tenant_id)
    .bind(&cmd.scope_kind)
    .bind(cmd.scope_id)
    .bind(batch_id)
    .bind(&cmd.client_batch_id)
    .bind(cmd.expires_at)
    .bind(cmd.declared_count as i32)
    .execute(&mut *txn)
    .await?;

    // Read back the *persisted* batch identity/size/window — never call inputs. On a full
    // replay `rows_affected()` is 0 (all conflicted), but §34.2 freezes `issued` = the ticket
    // COUNT for this client_batch_id (a retry after a crash must see issued=declared_count, not
    // 0) and `expires_at` = the window fixed at first issue, not this caller's value. The
    // advisory lock guarantees exactly one batch_id survives, so the GROUP BY yields one row.
    let persisted = sqlx::query(
        "SELECT batch_id, count(*)::bigint AS issued, max(expires_at) AS expires_at \
         FROM private.ingest_tickets \
         WHERE tenant_id = $1 AND client_batch_id = $2 \
         GROUP BY batch_id",
    )
    .bind(cmd.tenant_id)
    .bind(&cmd.client_batch_id)
    .fetch_one(&mut *txn)
    .await?;

    let issued: i64 = persisted.try_get("issued")?;
    let out = BatchIssued {
        batch_id: persisted.try_get("batch_id")?,
        issued: issued as u32,
        expires_at: persisted.try_get("expires_at")?,
    };

    txn.commit().await?;
    Ok(out)
}

// Fault-injection / functional coverage lives in
// `crates/adapters/tests/outbox_batch_remember.rs` (§79 "DB integration 测试走隔离
// schema/事务" plus real role-scoped connections — exercising `role_batch_issuer` vs
// `role_gateway` grant separation needs two real connections, not a pool-independent unit
// test).
