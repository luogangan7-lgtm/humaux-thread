//! `adapters::placement_repo` — the gateway's read-only lookup of §17.3 `projection.tenant_placements` (migration
//!   0068).
//! Depends-on: crates=[humaux-domain, sqlx]; services=[PostgreSQL(any) r=[projection.tenant_placements]]; env=[]; modules=[adapters::postgres, adapters::qdrant, domain::error, domain::ids]
//! Called-by: [adapters::stream_repo, gateway::recall]
//! Invariants: [SELECT-only on projection.tenant_placements under role_gateway; a missing row means not indexed yet
//!   and the caller must answer DependencyUnavailable, never fall back to another tenant's placement]
//! Spec: Baseline §6.2.1
//!
//! `role_gateway` holds only `SELECT` on this table (§6.2.1 domain default; `adapters::qdrant`'s
//! own module doc names itself the sole writer, worker-side) — this module never issues an
//! `INSERT`/`UPDATE`/`DELETE` against it. It exists so `bins/gateway/src/recall.rs` can resolve
//! a tenant's placement per request instead of holding one fixed `TenantPlacementRow` for the
//! life of the process (a single-tenant assumption the previous `SemanticRecallRuntime` shape
//! made): a row's absence means "not indexed yet for this tenant/family", which the caller must
//! treat as `DependencyUnavailable`, never a cross-tenant fallback to somebody else's placement.

use sqlx::Row;
use sqlx::postgres::PgRow;

use humaux_domain::error::ErrorCode;
use humaux_domain::ids::TenantId;

use crate::postgres::RuntimeDbPool;
use crate::qdrant::{PlacementClass, PromotionState, RetrievalFamily, TenantPlacementRow};

/// Looks up the one placement row (migration 0068's primary key is `(tenant_id,
/// projection_family)`) for `tenant_id`/`family`, under the requesting tenant's own RLS
/// binding. `Ok(None)` — not an error — means this tenant has no placement for `family` yet;
/// the caller (`bins/gateway/src/recall.rs`) turns that into `ErrorCode::DependencyUnavailable`
/// rather than inventing or borrowing a placement.
pub async fn tenant_placement(
    pool: &RuntimeDbPool,
    tenant_id: TenantId,
    family: RetrievalFamily,
) -> Result<Option<TenantPlacementRow>, ErrorCode> {
    // `set_config(..., true)` is transaction-local (`is_local = true`): issued outside an
    // explicit transaction it reverts before the next statement runs, so the following SELECT
    // would see `humaux.tenant_id` unset and RLS would deny every row. Both statements must
    // run inside one transaction (mirrors `retrieval_embedding_rpc::set_tenant_local`).
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    sqlx::query("SELECT set_config('humaux.tenant_id', $1, true)")
        .bind(tenant_id.0.to_string())
        .execute(&mut *txn)
        .await
        .map_err(db_error)?;
    let row = sqlx::query(
        "SELECT tenant_id, projection_family, collection_name, shard_key, placement_class, \
                point_count, bytes_estimate, promotion_state \
         FROM projection.tenant_placements \
         WHERE tenant_id = $1 AND projection_family = $2",
    )
    .bind(tenant_id.0)
    .bind(family.as_db_str())
    .fetch_optional(&mut *txn)
    .await
    .map_err(db_error)?;
    txn.commit().await.map_err(db_error)?;
    row.as_ref().map(from_row).transpose()
}

/// The one parser of a `projection.tenant_placements` row shape. `pub(crate)` so
/// `stream_repo::claim_issued` (ADR-0052 D-C) parses the placement columns the claim returns
/// with this same function instead of a second copy of the three enum parsers.
pub(crate) fn from_row(row: &PgRow) -> Result<TenantPlacementRow, ErrorCode> {
    let projection_family: String = row
        .try_get("projection_family")
        .map_err(|_| ErrorCode::Internal)?;
    let placement_class: String = row
        .try_get("placement_class")
        .map_err(|_| ErrorCode::Internal)?;
    let promotion_state: String = row
        .try_get("promotion_state")
        .map_err(|_| ErrorCode::Internal)?;
    Ok(TenantPlacementRow {
        tenant_id: TenantId(row.try_get("tenant_id").map_err(|_| ErrorCode::Internal)?),
        projection_family: parse_family(&projection_family)?,
        collection_name: row
            .try_get("collection_name")
            .map_err(|_| ErrorCode::Internal)?,
        shard_key: row.try_get("shard_key").map_err(|_| ErrorCode::Internal)?,
        placement_class: parse_placement_class(&placement_class)?,
        point_count: row
            .try_get("point_count")
            .map_err(|_| ErrorCode::Internal)?,
        bytes_estimate: row
            .try_get("bytes_estimate")
            .map_err(|_| ErrorCode::Internal)?,
        promotion_state: parse_promotion_state(&promotion_state)?,
    })
}

fn parse_family(value: &str) -> Result<RetrievalFamily, ErrorCode> {
    RetrievalFamily::ALL
        .into_iter()
        .find(|family| family.as_db_str() == value)
        .ok_or(ErrorCode::Internal)
}

fn parse_placement_class(value: &str) -> Result<PlacementClass, ErrorCode> {
    PlacementClass::ALL
        .into_iter()
        .find(|class| class.as_db_str() == value)
        .ok_or(ErrorCode::Internal)
}

fn parse_promotion_state(value: &str) -> Result<PromotionState, ErrorCode> {
    PromotionState::ALL
        .into_iter()
        .find(|state| state.as_db_str() == value)
        .ok_or(ErrorCode::Internal)
}

fn db_error(error: sqlx::Error) -> ErrorCode {
    match error {
        sqlx::Error::RowNotFound => ErrorCode::NotFound,
        sqlx::Error::Database(ref db) => match db.code().as_deref() {
            Some("42501") => ErrorCode::Forbidden,
            Some("23505" | "40001" | "40P01" | "55P03") => ErrorCode::Conflict,
            Some("22023" | "22P02" | "22003" | "23514") => ErrorCode::InvalidInput,
            Some("23503") => ErrorCode::TenantBoundary,
            _ => ErrorCode::Internal,
        },
        _ => ErrorCode::DependencyUnavailable,
    }
}
