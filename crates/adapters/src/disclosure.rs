//! `adapters::disclosure` — `ops.data_disclosures` two-phase ledger writer (§7.4, T4.2).
//!
//! §7.4: "`ops.data_disclosures` 是唯一权威出境账本... 每一次，不采样". This module is the
//! sole write path to both `ops.data_disclosures` and its normalized source relation
//! `ops.data_disclosure_sources` (`migrations/0047_data_disclosure_ledger.sql`):
//! `reserve_private`/`reserve_retrieval` insert one row into each — the disclosure row and at
//! least one [`DisclosureSource`] row, in the same transaction — *before* an
//! [`ExternalCall`](humaux_domain::egress::ExternalCall) is made; `finalize_private`/
//! `finalize_retrieval` record the outcome after it returns. There is no batching/sampling
//! knob anywhere in this file — a caller that skips reserve() for "just this one call"
//! produces no ledger row at all, which is the §7.0 violation the whole permit topology
//! exists to make structurally hard, not something this module tries to detect after the
//! fact. `reserve`'s empty-`sources` rejection ([`DisclosureError::NoSources`]) closes the
//! same-shaped gap for sources specifically: an unattributed disclosure row would be exactly
//! as unusable to §7.4's deletion/revocation propagation as no row at all.
//!
//! Two runtime roles legitimately write here today, matching §83.4's private-data purposes:
//! [`PrivateWorkerDbPool`] (§11 `USER_REASONING`) and [`RetrievalWorkerDbPool`] (§19
//! `RETRIEVAL_EMBEDDING`/`RETRIEVAL_RERANK`). Both §6.2.1 domain-default grants on `ops.*`
//! already cover SELECT/INSERT/UPDATE (this table is not one of §6.2.2's 14 point-named
//! tables), so no new GRANT is needed beyond the migration itself. Each pool's public
//! function opens its own transaction via `pool.pool().begin()` (matching `jobs::claim`'s
//! own pattern) and hands it to a shared private `_in_txn` helper — same shape as
//! `jobs.rs`'s per-call `SET LOCAL` + query, just split so the SQL body isn't copy-pasted
//! per pool type. Neither this file nor `postgres.rs` names the pool handle type here: the
//! `_in_txn` helpers take `&mut sqlx::Transaction<'_, sqlx::Postgres>`, never
//! `sqlx::PgPool`, so this module makes zero raw-pool-type mentions itself
//! (§6.2.3/G80-40 static check: `postgres.rs` is the only file allowed to name it).

use sqlx::Row;
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

use humaux_domain::egress::{AuthorizedEgressPayload, EgressPermit, PrivateDataPurpose};

use crate::postgres::{MaintenanceDbPool, PrivateWorkerDbPool, RetrievalWorkerDbPool};

// §7.3/§7.4: `data_class` and `payload_bytes` are read off `permit`/`payload` themselves, not
// taken as free-standing caller-supplied arguments — `EgressPermit::data_class()` is what
// `domain::egress::authorize` actually approved (§7.5.1's fail-closed check already ran there),
// and `payload.bytes().len()` is what was actually authorized to go out (`payload.sha256()` is
// checked against `permit.payload_sha256()` below for the same reason `ExternalCall::call` must
// — a mismatched payload here would let the ledger record a class/size the permit never covered).
// `region` remains a caller-supplied argument: §7.3 does not freeze it onto `EgressPermit`.

/// DB-layer failure from any function in this module. Adapter-local, not one of the
/// workspace's two frozen domain error enums (§52) — same reasoning as `jobs::JobsError`.
#[derive(Debug)]
pub enum DisclosureError {
    Db(sqlx::Error),
    /// §7.3: "Permit 不能被拿去发送另一份正文" — `payload`'s own digest does not match
    /// `permit.payload_sha256()`, so `reserve`'s caller is trying to ledger a body the permit
    /// never authorized.
    PayloadMismatch,
    /// §7.4 "来源关系规范化": `reserve` was called with zero sources — an empty slice is a
    /// caller bug, not a disclosure with nothing to attribute, and must not silently produce
    /// an unattributed ledger row (see `data_disclosure_sources`'s no-write-path finding).
    NoSources,
}

impl From<sqlx::Error> for DisclosureError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

impl std::fmt::Display for DisclosureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(e) => write!(f, "ops.data_disclosures DB error: {e}"),
            Self::PayloadMismatch => write!(
                f,
                "reserve() payload digest does not match the EgressPermit's authorized digest (§7.3)"
            ),
            Self::NoSources => write!(
                f,
                "reserve() requires at least one ops.data_disclosure_sources row (§7.4)"
            ),
        }
    }
}

impl std::error::Error for DisclosureError {}

/// §7.4 `outcome` — the three-way finalize() result, verbatim the `ops.data_disclosures`
/// CHECK literals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisclosureOutcome {
    Success,
    Failed,
    Denied,
}

impl DisclosureOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "SUCCESS",
            Self::Failed => "FAILED",
            Self::Denied => "DENIED",
        }
    }
}

/// §7.4 `deletion_capability` — "processor 侧能不能删". `Unknown` is the column's own
/// `DEFAULT`, not a placeholder invented here — a disclosure reserved before the processor's
/// deletion capability is known (the common case at reserve()-time) starts `Unknown` and is
/// updated once §37's retention/deletion-propagation path learns the real answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletionCapability {
    Supported,
    Unsupported,
    Unknown,
}

impl DeletionCapability {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Supported => "SUPPORTED",
            Self::Unsupported => "UNSUPPORTED",
            Self::Unknown => "UNKNOWN",
        }
    }
}

fn purpose_as_db_str(purpose: PrivateDataPurpose) -> &'static str {
    match purpose {
        PrivateDataPurpose::UserReasoning => "USER_REASONING",
        PrivateDataPurpose::RetrievalEmbedding => "RETRIEVAL_EMBEDDING",
        PrivateDataPurpose::RetrievalRerank => "RETRIEVAL_RERANK",
    }
}

/// One `ops.data_disclosures` row's narrower-than-tenant scope (§7.4 "谁的数据"), mirroring
/// `projection.stream_checkpoints`' `(scope_kind, scope_id)` shape. `None` records a
/// tenant-wide disclosure with no narrower scope.
#[derive(Debug, Clone, Copy)]
pub struct DisclosureScope {
    pub scope_kind: &'static str,
    pub scope_id: Uuid,
}

/// One `ops.data_disclosure_sources` row's `(source_kind, id)` pair — §7.4 "四个具体 ID 恰好
/// 一个非 NULL". An enum makes the kind/id pairing structurally exhaustive: there is no
/// `DisclosureSource` value that names one `source_kind` while carrying a different kind's id,
/// the exact mismatch shape the table's own `data_disclosure_sources_kind_matches_id` CHECK
/// exists to reject at the DB layer.
#[derive(Debug, Clone, Copy)]
pub enum DisclosureSource {
    Evidence(Uuid),
    Memory(Uuid),
    Rollup(Uuid),
    Release(Uuid),
}

impl DisclosureSource {
    /// `(source_kind wire string, evidence_id, memory_id, rollup_id, release_id)` — the exact
    /// five-column shape `ops.data_disclosure_sources` binds per row.
    #[allow(clippy::type_complexity)]
    fn columns(
        self,
    ) -> (
        &'static str,
        Option<Uuid>,
        Option<Uuid>,
        Option<Uuid>,
        Option<Uuid>,
    ) {
        match self {
            Self::Evidence(id) => ("EVIDENCE", Some(id), None, None, None),
            Self::Memory(id) => ("MEMORY", None, Some(id), None, None),
            Self::Rollup(id) => ("ROLLUP", None, None, Some(id), None),
            Self::Release(id) => ("PUBLIC_RELEASE", None, None, None, Some(id)),
        }
    }
}

async fn set_tenant_local(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
) -> Result<(), DisclosureError> {
    sqlx::query(&format!("SET LOCAL humaux.tenant_id = '{tenant_id}'"))
        .execute(&mut **txn)
        .await?;
    Ok(())
}

/// §7.4 reserve() — inserts the disclosure row **before** the [`ExternalCall`] it records is
/// made, with `finalized_at`/`outcome` left NULL. Returns the minted `disclosure_id` so the
/// caller can pass it to [`finalize`] once the call returns (success or failure — every
/// reservation must be finalized one way or another; an unfinalized row past 60s is exactly
/// what §53 INV-3 watches for, see [`open_reservations_older_than`]).
///
/// `permit` supplies `grant_id`/`tenant_id`/`processor`/`purpose`/`payload_sha256`/`data_class`
/// (all already bound and vetted at [`domain::egress::authorize`] time); `region`/`scope`
/// remain caller-supplied (§7.3 does not freeze either onto `EgressPermit`). `payload_bytes` is
/// derived from `payload` itself rather than trusted from the caller, and `payload`'s own
/// digest must match `permit.payload_sha256()` (§7.3) or this call fails closed with
/// [`DisclosureError::PayloadMismatch`] before any row is written.
///
/// `sources` becomes this same reservation's `ops.data_disclosure_sources` rows, in one
/// transaction with the ledger insert — an empty slice is rejected
/// ([`DisclosureError::NoSources`]) rather than silently producing an unattributed disclosure
/// (§7.4 "来源关系规范化": deletion/revocation propagation only ever queries this relation).
async fn reserve_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    permit: &EgressPermit,
    region: &str,
    payload: &AuthorizedEgressPayload,
    scope: Option<DisclosureScope>,
    sources: &[DisclosureSource],
) -> Result<Uuid, DisclosureError> {
    if payload.sha256() != permit.payload_sha256() {
        return Err(DisclosureError::PayloadMismatch);
    }
    if sources.is_empty() {
        return Err(DisclosureError::NoSources);
    }

    let tenant_id = permit.tenant_id().0;
    set_tenant_local(txn, tenant_id).await?;

    let row = sqlx::query(
        "INSERT INTO ops.data_disclosures \
           (grant_id, tenant_id, scope_kind, scope_id, processor_id, region, \
            data_class, purpose, payload_sha256, payload_bytes) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
         RETURNING disclosure_id",
    )
    .bind(permit.grant_id())
    .bind(tenant_id)
    .bind(scope.map(|s| s.scope_kind))
    .bind(scope.map(|s| s.scope_id))
    .bind(permit.processor().0)
    .bind(region)
    .bind(permit.data_class().as_str())
    .bind(purpose_as_db_str(permit.purpose()))
    .bind(permit.payload_sha256().to_vec())
    .bind(payload.bytes().len() as i64)
    .fetch_one(&mut **txn)
    .await?;
    let disclosure_id: Uuid = row.get("disclosure_id");

    for (ordinal, source) in sources.iter().enumerate() {
        let (source_kind, evidence_id, memory_id, rollup_id, release_id) = source.columns();
        sqlx::query(
            "INSERT INTO ops.data_disclosure_sources \
               (tenant_id, disclosure_id, source_kind, evidence_id, memory_id, rollup_id, \
                release_id, ordinal) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(tenant_id)
        .bind(disclosure_id)
        .bind(source_kind)
        .bind(evidence_id)
        .bind(memory_id)
        .bind(rollup_id)
        .bind(release_id)
        .bind(ordinal as i32)
        .execute(&mut **txn)
        .await?;
    }

    Ok(disclosure_id)
}

/// §7.4 finalize() — records the outcome of the [`ExternalCall`] a prior [`reserve`] reserved
/// a ledger row for. `tenant_id` re-scopes the same RLS-guarded transaction `reserve` used
/// (mirrors `jobs::heartbeat`'s per-call `SET LOCAL` pattern — there is no long-lived
/// transaction spanning reserve()..finalize()). Returns `Ok(false)` if `disclosure_id` does
/// not exist under this tenant or was already finalized (the guard trigger in
/// `migrations/0047_data_disclosure_ledger.sql` would reject a genuine re-finalize attempt;
/// this surfaces that as a normal `bool`, not a DB error, matching `jobs::complete`'s
/// lease-miss convention) rather than as a write.
async fn finalize_in_txn(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    disclosure_id: Uuid,
    outcome: DisclosureOutcome,
    deletion_capability: DeletionCapability,
) -> Result<bool, DisclosureError> {
    set_tenant_local(txn, tenant_id).await?;

    let result = sqlx::query(
        "UPDATE ops.data_disclosures \
         SET finalized_at = now(), outcome = $2, deletion_capability = $3 \
         WHERE disclosure_id = $1 AND finalized_at IS NULL",
    )
    .bind(disclosure_id)
    .bind(outcome.as_str())
    .bind(deletion_capability.as_str())
    .execute(&mut **txn)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// [`reserve_in_txn`] for the §11 `USER_REASONING` egress path ([`PrivateWorkerDbPool`]).
pub async fn reserve_private(
    pool: &PrivateWorkerDbPool,
    permit: &EgressPermit,
    region: &str,
    payload: &AuthorizedEgressPayload,
    scope: Option<DisclosureScope>,
    sources: &[DisclosureSource],
) -> Result<Uuid, DisclosureError> {
    let mut txn = pool.pool().begin().await?;
    let disclosure_id = reserve_in_txn(&mut txn, permit, region, payload, scope, sources).await?;
    txn.commit().await?;
    Ok(disclosure_id)
}

/// [`reserve_in_txn`] for the §19 `RETRIEVAL_EMBEDDING`/`RETRIEVAL_RERANK` egress path
/// ([`RetrievalWorkerDbPool`]).
pub async fn reserve_retrieval(
    pool: &RetrievalWorkerDbPool,
    permit: &EgressPermit,
    region: &str,
    payload: &AuthorizedEgressPayload,
    scope: Option<DisclosureScope>,
    sources: &[DisclosureSource],
) -> Result<Uuid, DisclosureError> {
    let mut txn = pool.pool().begin().await?;
    let disclosure_id = reserve_in_txn(&mut txn, permit, region, payload, scope, sources).await?;
    txn.commit().await?;
    Ok(disclosure_id)
}

/// [`finalize_in_txn`] for [`PrivateWorkerDbPool`].
pub async fn finalize_private(
    pool: &PrivateWorkerDbPool,
    tenant_id: Uuid,
    disclosure_id: Uuid,
    outcome: DisclosureOutcome,
    deletion_capability: DeletionCapability,
) -> Result<bool, DisclosureError> {
    let mut txn = pool.pool().begin().await?;
    let changed = finalize_in_txn(
        &mut txn,
        tenant_id,
        disclosure_id,
        outcome,
        deletion_capability,
    )
    .await?;
    txn.commit().await?;
    Ok(changed)
}

/// [`finalize_in_txn`] for [`RetrievalWorkerDbPool`].
pub async fn finalize_retrieval(
    pool: &RetrievalWorkerDbPool,
    tenant_id: Uuid,
    disclosure_id: Uuid,
    outcome: DisclosureOutcome,
    deletion_capability: DeletionCapability,
) -> Result<bool, DisclosureError> {
    let mut txn = pool.pool().begin().await?;
    let changed = finalize_in_txn(
        &mut txn,
        tenant_id,
        disclosure_id,
        outcome,
        deletion_capability,
    )
    .await?;
    txn.commit().await?;
    Ok(changed)
}

/// One row of §53 INV-3's own observation shape: "`reserved_at` 有而 `finalized_at` 无且超
/// 60s ⇒ 立即红". This function is that query — INV-3's actual alerting/polling loop lives
/// wherever §53's invariant runner is wired (out of this task's scope), but the SQL shape
/// itself is owned here, next to the table it reads, not re-derived a second time elsewhere.
#[derive(Debug, Clone)]
pub struct StaleReservation {
    pub disclosure_id: Uuid,
    pub tenant_id: Uuid,
    pub reserved_at: OffsetDateTime,
}

/// Every `tenant_id` disclosure still unfinalized after `staleness_seconds` (§53 INV-3's
/// frozen threshold is 60s; passed as a parameter rather than hardcoded so a fault-injection
/// test can pass `0` and observe an immediate hit without waiting a minute).
///
/// Scoped to one tenant per call — same shape as `stream_repo`'s `ISSUED -> LOST` patrol
/// (`crates/adapters/src/stream_repo.rs`, T3.4): `ops.data_disclosures` carries `FORCE ROW
/// LEVEL SECURITY` (§6.1), so a query that never sets `humaux.tenant_id` sees zero rows
/// under any non-superuser role, cross-tenant or not — there is no RLS-exempt "see every
/// tenant" mode for a runtime role to use here. §53's actual invariant runner (out of this
/// task's scope) is the thing that iterates every tenant and calls this once per tenant,
/// exactly like the LOST patrol's own caller does. Runs under [`MaintenanceDbPool`] — the
/// role §6.2.1 gives read-only visibility across every domain, matching this query's
/// observability-only purpose (it never mutates a row).
pub async fn open_reservations_older_than(
    pool: &MaintenanceDbPool,
    tenant_id: Uuid,
    staleness_seconds: f64,
) -> Result<Vec<StaleReservation>, DisclosureError> {
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, tenant_id).await?;

    let rows = sqlx::query(
        "SELECT disclosure_id, tenant_id, reserved_at \
         FROM ops.data_disclosures \
         WHERE finalized_at IS NULL \
           AND reserved_at < now() - make_interval(secs => $1) \
         ORDER BY reserved_at",
    )
    .bind(staleness_seconds)
    .fetch_all(&mut *txn)
    .await?;
    txn.commit().await?;

    Ok(rows
        .iter()
        .map(|r| StaleReservation {
            disclosure_id: r.get("disclosure_id"),
            tenant_id: r.get("tenant_id"),
            reserved_at: r.get("reserved_at"),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    // §78.2: these three pin the literal spelling only — both sides are compiled Rust, so none
    // of them can detect the live CHECK constraint drifting. The real DB-vs-Rust contract test
    // is `wire_strings_match_live_db_check_constraints` in
    // `crates/adapters/tests/disclosure_ledger.rs`, which has the Postgres connection these do
    // not (same split `dataclass.rs`'s `as_str_literals_are_stable` makes for `DataClass`).
    #[test]
    fn outcome_wire_strings_are_stable() {
        assert_eq!(DisclosureOutcome::Success.as_str(), "SUCCESS");
        assert_eq!(DisclosureOutcome::Failed.as_str(), "FAILED");
        assert_eq!(DisclosureOutcome::Denied.as_str(), "DENIED");
    }

    #[test]
    fn deletion_capability_wire_strings_are_stable() {
        assert_eq!(DeletionCapability::Supported.as_str(), "SUPPORTED");
        assert_eq!(DeletionCapability::Unsupported.as_str(), "UNSUPPORTED");
        assert_eq!(DeletionCapability::Unknown.as_str(), "UNKNOWN");
    }

    #[test]
    fn purpose_wire_strings_are_stable() {
        assert_eq!(
            purpose_as_db_str(PrivateDataPurpose::UserReasoning),
            "USER_REASONING"
        );
        assert_eq!(
            purpose_as_db_str(PrivateDataPurpose::RetrievalEmbedding),
            "RETRIEVAL_EMBEDDING"
        );
        assert_eq!(
            purpose_as_db_str(PrivateDataPurpose::RetrievalRerank),
            "RETRIEVAL_RERANK"
        );
    }
}
