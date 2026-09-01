//! Persistent §19.2 provider-budget admission backed by the four narrow SQL functions from
//! migration 0117.  Callers must first create a matching `ModelCallLedger` reservation; this
//! module never creates a second provider-attempt identity.

use std::time::Duration;

use humaux_domain::{egress::PrivateDataPurpose, error::ErrorCode, ids::TenantId};
use sqlx::{
    Row,
    types::{Uuid, time::OffsetDateTime},
};

use crate::{
    model_call_ledger::{FinalizeCall, ModelCallOutcome},
    postgres::{MaintenanceDbPool, RetrievalWorkerDbPool},
};

/// Inputs bound to one already-reserved `ops.model_call_ledger` row.  Provider/model/region
/// remain strings because their canonical registry identities are local to retrieval-provider;
/// this adapter validates them before handing them to the database contract.
#[derive(Debug, Clone, Copy)]
pub struct ProviderBudgetRequest<'a> {
    pub tenant_id: TenantId,
    pub model_call_id: Uuid,
    pub provider_id: &'a str,
    pub model_id: &'a str,
    pub region: &'a str,
    pub purpose: PrivateDataPurpose,
    pub estimated_tokens: u64,
    pub ttl: Duration,
}

/// Opaque durable reservation reference.  `status` is returned only for an idempotent replay;
/// callers may settle a newly reserved row only after finalizing the paired ModelCallLedger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderBudgetReservation {
    pub reservation_id: Uuid,
    pub reserved_at: OffsetDateTime,
    pub expires_at: OffsetDateTime,
    pub status: ProviderBudgetReservationStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderBudgetReservationStatus {
    Reserved,
    Consumed,
    Released,
    Expired,
}

impl ProviderBudgetReservationStatus {
    fn parse(value: &str) -> Result<Self, ErrorCode> {
        match value {
            "RESERVED" => Ok(Self::Reserved),
            "CONSUMED" => Ok(Self::Consumed),
            "RELEASED" => Ok(Self::Released),
            "EXPIRED" => Ok(Self::Expired),
            _ => Err(ErrorCode::Internal),
        }
    }
}

fn purpose(purpose: PrivateDataPurpose) -> Result<&'static str, ErrorCode> {
    match purpose {
        PrivateDataPurpose::RetrievalEmbedding => Ok("embedding"),
        PrivateDataPurpose::RetrievalRerank => Ok("rerank"),
        _ => Err(ErrorCode::InvalidInput),
    }
}

fn ttl_micros(ttl: Duration) -> Result<i64, ErrorCode> {
    let micros = i64::try_from(ttl.as_micros()).map_err(|_| ErrorCode::InvalidInput)?;
    (micros > 0)
        .then_some(micros)
        .ok_or(ErrorCode::InvalidInput)
}

fn validate_request(
    request: &ProviderBudgetRequest<'_>,
) -> Result<(i64, i64, &'static str), ErrorCode> {
    let tokens = i64::try_from(request.estimated_tokens).map_err(|_| ErrorCode::InvalidInput)?;
    if request.tenant_id.0.is_nil()
        || request.model_call_id.is_nil()
        || tokens <= 0
        || request.provider_id.trim().is_empty()
        || request.model_id.trim().is_empty()
        || request.region.trim().is_empty()
    {
        return Err(ErrorCode::InvalidInput);
    }
    Ok((tokens, ttl_micros(request.ttl)?, purpose(request.purpose)?))
}

async fn set_tenant_local(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: TenantId,
) -> Result<(), ErrorCode> {
    sqlx::query("SELECT set_config('humaux.tenant_id', $1, true)")
        .bind(tenant_id.0.to_string())
        .execute(&mut **txn)
        .await
        .map_err(map_db_error)?;
    Ok(())
}

fn map_db_error(error: sqlx::Error) -> ErrorCode {
    match &error {
        sqlx::Error::Database(database) => match database.code().as_deref() {
            Some("P0002") => ErrorCode::CostBudgetExceeded,
            Some("P0003") => ErrorCode::Conflict,
            _ => ErrorCode::Internal,
        },
        _ => ErrorCode::Internal,
    }
}

/// Atomically admits all four active canonical tiers in the database.  The SQL function owns
/// the sliding-window aggregate and ordered advisory locks; this wrapper only supplies typed
/// identity and a checked role-specific pool.
pub async fn reserve_provider_budget(
    pool: &RetrievalWorkerDbPool,
    request: &ProviderBudgetRequest<'_>,
) -> Result<ProviderBudgetReservation, ErrorCode> {
    let (tokens, ttl, purpose) = validate_request(request)?;
    let mut txn = pool.pool().begin().await.map_err(map_db_error)?;
    set_tenant_local(&mut txn, request.tenant_id).await?;
    let row = sqlx::query(
        "SELECT reservation_id, reserved_at, expires_at, status \
         FROM ops.reserve_retrieval_provider_budget($1,$2,$3,$4,$5,$6,$7,$8)",
    )
    .bind(request.tenant_id.0)
    .bind(request.model_call_id)
    .bind(request.provider_id)
    .bind(request.model_id)
    .bind(request.region)
    .bind(purpose)
    .bind(tokens)
    .bind(ttl)
    .fetch_one(&mut *txn)
    .await
    .map_err(map_db_error)?;
    let reservation = ProviderBudgetReservation {
        reservation_id: row
            .try_get("reservation_id")
            .map_err(|_| ErrorCode::Internal)?,
        reserved_at: row
            .try_get("reserved_at")
            .map_err(|_| ErrorCode::Internal)?,
        expires_at: row.try_get("expires_at").map_err(|_| ErrorCode::Internal)?,
        status: ProviderBudgetReservationStatus::parse(
            &row.try_get::<String, _>("status")
                .map_err(|_| ErrorCode::Internal)?,
        )?,
    };
    txn.commit().await.map_err(map_db_error)?;
    Ok(reservation)
}

/// Settles one durable reservation after the paired ModelCallLedger reached a terminal state.
/// Records the server-owned send-start fact immediately before transport invocation.
pub async fn mark_provider_budget_dispatched(
    pool: &RetrievalWorkerDbPool,
    tenant_id: TenantId,
    reservation_id: Uuid,
) -> Result<OffsetDateTime, ErrorCode> {
    if tenant_id.0.is_nil() || reservation_id.is_nil() {
        return Err(ErrorCode::InvalidInput);
    }
    let mut txn = pool.pool().begin().await.map_err(map_db_error)?;
    set_tenant_local(&mut txn, tenant_id).await?;
    let dispatched_at =
        sqlx::query_scalar("SELECT ops.mark_retrieval_provider_budget_dispatched($1, $2)")
            .bind(tenant_id.0)
            .bind(reservation_id)
            .fetch_one(&mut *txn)
            .await
            .map_err(map_db_error)?;
    txn.commit().await.map_err(map_db_error)?;
    Ok(dispatched_at)
}

/// Settles from the durable dispatch fact: only an undispatched failed call can release.
pub async fn settle_provider_budget(
    pool: &RetrievalWorkerDbPool,
    tenant_id: TenantId,
    reservation_id: Uuid,
) -> Result<ProviderBudgetReservationStatus, ErrorCode> {
    if tenant_id.0.is_nil() || reservation_id.is_nil() {
        return Err(ErrorCode::InvalidInput);
    }
    let mut txn = pool.pool().begin().await.map_err(map_db_error)?;
    set_tenant_local(&mut txn, tenant_id).await?;
    let status: String = sqlx::query_scalar("SELECT ops.settle_retrieval_provider_budget($1, $2)")
        .bind(tenant_id.0)
        .bind(reservation_id)
        .fetch_one(&mut *txn)
        .await
        .map_err(map_db_error)?;
    txn.commit().await.map_err(map_db_error)?;
    ProviderBudgetReservationStatus::parse(&status)
}

/// Finalizes the paired ModelCallLedger and settles its provider-budget reservation in one
/// identity-bound PostgreSQL function. SQL locks reservation then ledger, verifies their exact
/// pair before either write, and derives the terminal budget state from `dispatched_at`.
pub async fn finalize_and_settle_provider_budget(
    pool: &RetrievalWorkerDbPool,
    tenant_id: TenantId,
    reservation_id: Uuid,
    model_call_id: Uuid,
    outcome: ModelCallOutcome,
    finalize: &FinalizeCall,
) -> Result<ProviderBudgetReservationStatus, ErrorCode> {
    if tenant_id.0.is_nil() || reservation_id.is_nil() || model_call_id.is_nil() {
        return Err(ErrorCode::InvalidInput);
    }
    let mut txn = pool.pool().begin().await.map_err(map_db_error)?;
    set_tenant_local(&mut txn, tenant_id).await?;
    let status: String = sqlx::query_scalar(
        "SELECT ops.finalize_retrieval_provider_budget(\
           $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
    )
    .bind(tenant_id.0)
    .bind(reservation_id)
    .bind(model_call_id)
    .bind(outcome.as_str())
    .bind(finalize.input_tokens)
    .bind(finalize.billable_tokens)
    .bind(finalize.candidate_count)
    .bind(finalize.candidate_tokens)
    .bind(finalize.cache_hit)
    .bind(finalize.latency_ms)
    .bind(finalize.actual_cost)
    .bind(&finalize.error_class)
    .bind(&finalize.provider_request_id)
    .fetch_one(&mut *txn)
    .await
    .map_err(map_db_error)?;
    let status = ProviderBudgetReservationStatus::parse(&status)?;
    txn.commit().await.map_err(map_db_error)?;
    Ok(status)
}

/// Reconciles terminal or expired reservations for one tenant. Terminal ledger facts settle
/// immediately; an expired dispatched reservation is conservatively consumed even if its
/// ledger finalization was lost, while only an expired undispatched reservation can expire.
/// Allocation rows remain append-only for audit.
pub async fn reap_expired_provider_budgets(
    pool: &MaintenanceDbPool,
    tenant_id: TenantId,
    limit: i32,
) -> Result<i32, ErrorCode> {
    if tenant_id.0.is_nil() || limit <= 0 {
        return Err(ErrorCode::InvalidInput);
    }
    let mut txn = pool.pool().begin().await.map_err(map_db_error)?;
    set_tenant_local(&mut txn, tenant_id).await?;
    let reaped: i32 =
        sqlx::query_scalar("SELECT ops.reap_expired_retrieval_provider_budget($1, $2)")
            .bind(tenant_id.0)
            .bind(limit)
            .fetch_one(&mut *txn)
            .await
            .map_err(map_db_error)?;
    txn.commit().await.map_err(map_db_error)?;
    Ok(reaped)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_retrieval_purposes_cross_the_budget_boundary() {
        assert_eq!(
            purpose(PrivateDataPurpose::RetrievalEmbedding),
            Ok("embedding")
        );
        assert_eq!(purpose(PrivateDataPurpose::RetrievalRerank), Ok("rerank"));
        assert_eq!(
            purpose(PrivateDataPurpose::UserReasoning),
            Err(ErrorCode::InvalidInput)
        );
    }

    #[test]
    fn zero_ttl_is_rejected_before_database_access() {
        assert_eq!(ttl_micros(Duration::ZERO), Err(ErrorCode::InvalidInput));
    }
}
