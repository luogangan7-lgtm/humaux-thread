//! `adapters::request_guard_repo` — RequestGuard's first PostgreSQL persistence seam: effective-entitlement reads and
//!   FORCE-RLS operational-audit writes.
//! Depends-on: crates=[humaux-domain, serde_json, sqlx]; services=[PostgreSQL(role_gateway)
//!   r=[control.entitlement_snapshots] x=[control.audit_event_insert]]; env=[]; modules=[adapters::postgres,
//!   adapters::quota_repo, domain::audit, domain::error, domain::identity]
//! Called-by: [adapters::confirm_token_repo, adapters::context_repo, adapters::distill_repo, adapters::memory_governance_repo, adapters::operation_receipt, gateway::guard, tests]
//! Invariants: [policy stays in RequestGuard; this reads the effective entitlement snapshot and commits quota
//!   transitions together with the §77 audit row in one transaction; a missing entitlement is EntitlementRequired]
//! Spec: Baseline §77
//!
//! Policy stays in RequestGuard. This adapter reads the effective snapshot and commits
//! read-request quota transitions together with §77's audit writer, preserving the rule
//! that runtime pools and SQL transactions never escape the adapter layer.

use humaux_domain::{
    audit::{AuditEvent, AuditEventId, McpAuditAction, SYSTEM_TENANT_ID},
    error::ErrorCode,
    identity::AuthorizationScope,
};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use sqlx::{Row, types::Uuid};

use crate::{
    postgres::RuntimeDbPool,
    quota_repo::{self, QuotaReservation, ReservationStatus, ReserveResult},
};

type Txn<'c> = sqlx::Transaction<'c, sqlx::Postgres>;

/// Read-only database facts from the sole effective-entitlement projection (§76).
///
/// This is intentionally not `application::entitlement::EntitlementSnapshot`: `project` is
/// that type's sole constructor. RequestGuard only needs the already-projected facts.
#[derive(Clone, Debug, PartialEq)]
pub struct EffectiveEntitlementFacts {
    pub effective: Value,
    pub source_grant_ids: Vec<Uuid>,
    pub computed_at: OffsetDateTime,
    pub observed_at: OffsetDateTime,
}

/// Closed tenant source for an audit event. Callers cannot pass an unverified UUID.
#[derive(Clone, Copy, Debug)]
pub enum AuditTenant<'a> {
    Authenticated(&'a AuthorizationScope),
    System,
}

fn db_error(error: sqlx::Error) -> ErrorCode {
    match error {
        sqlx::Error::RowNotFound => ErrorCode::NotFound,
        sqlx::Error::Database(ref db) => match db.code().as_deref() {
            Some("23503") => ErrorCode::TenantBoundary,
            Some("42501") => ErrorCode::Forbidden,
            Some("23505") | Some("40001") | Some("40P01") => ErrorCode::Conflict,
            Some("22023") | Some("22P02") | Some("23514") => ErrorCode::InvalidInput,
            _ => ErrorCode::Internal,
        },
        _ => ErrorCode::DependencyUnavailable,
    }
}

async fn set_authorization_local(
    txn: &mut Txn<'_>,
    authorization: &AuthorizationScope,
) -> Result<(), ErrorCode> {
    sqlx::query("SELECT set_config('humaux.tenant_id', $1, true)")
        .bind(authorization.tenant_id().0.to_string())
        .execute(&mut **txn)
        .await
        .map_err(db_error)?;
    sqlx::query("SELECT set_config('humaux.user_id', $1, true)")
        .bind(
            authorization
                .user_id()
                .map(|id| id.0.to_string())
                .unwrap_or_else(|| Uuid::nil().to_string()),
        )
        .execute(&mut **txn)
        .await
        .map_err(db_error)?;
    Ok(())
}

async fn set_system_tenant_local(txn: &mut Txn<'_>) -> Result<(), ErrorCode> {
    sqlx::query(
        "SELECT set_config('humaux.tenant_id', $1, true), set_config('humaux.user_id', $2, true)",
    )
    .bind(SYSTEM_TENANT_ID.0.to_string())
    .bind(Uuid::nil().to_string())
    .execute(&mut **txn)
    .await
    .map_err(db_error)?;
    Ok(())
}

/// Reads only the projected §76 entitlement facts in a fresh tenant-scoped transaction.
///
/// No row is a real absence of entitlement, never a default plan. A non-object `effective`
/// payload is corrupt control data and fails closed rather than being interpreted loosely.
pub async fn read_effective_entitlements(
    pool: &RuntimeDbPool,
    authorization: &AuthorizationScope,
) -> Result<EffectiveEntitlementFacts, ErrorCode> {
    // dep: PostgreSQL(role_gateway) — transaction entry for `read_effective_entitlements`
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    set_authorization_local(&mut txn, authorization).await?;
    let row = sqlx::query(
        "SELECT effective, source_grant_ids, computed_at, clock_timestamp() AS observed_at \
         FROM control.entitlement_snapshots WHERE tenant_id = $1",
    )
    .bind(authorization.tenant_id().0)
    .fetch_optional(&mut *txn)
    .await
    .map_err(db_error)?;
    txn.commit().await.map_err(db_error)?;

    let Some(row) = row else {
        return Err(ErrorCode::EntitlementRequired);
    };
    let effective: Value = row.try_get("effective").map_err(|_| ErrorCode::Internal)?;
    if !effective.is_object() {
        return Err(ErrorCode::Internal);
    }
    Ok(EffectiveEntitlementFacts {
        effective,
        source_grant_ids: row
            .try_get("source_grant_ids")
            .map_err(|_| ErrorCode::Internal)?,
        computed_at: row
            .try_get("computed_at")
            .map_err(|_| ErrorCode::Internal)?,
        observed_at: row
            .try_get("observed_at")
            .map_err(|_| ErrorCode::Internal)?,
    })
}

/// Calls §77's only audit writer inside a fresh FORCE-RLS-scoped transaction.
///
/// `AuditTenant` fixes the tenant to an authenticated scope or the reserved system tenant;
/// the event's tenant must agree before the audit writer is invoked.
pub async fn audit_event_insert(
    pool: &RuntimeDbPool,
    tenant: AuditTenant<'_>,
    event: &AuditEvent,
) -> Result<AuditEventId, ErrorCode> {
    // dep: PostgreSQL(role_gateway) — transaction entry for `audit_event_insert`
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    let id = audit_event_insert_in_txn(&mut txn, tenant, event).await?;
    txn.commit().await.map_err(db_error)?;
    Ok(id)
}

/// The sole audit writer in an existing local business transaction; never commits it.
pub async fn audit_event_insert_in_txn(
    txn: &mut Txn<'_>,
    tenant: AuditTenant<'_>,
    event: &AuditEvent,
) -> Result<AuditEventId, ErrorCode> {
    let expected_tenant = match tenant {
        AuditTenant::Authenticated(authorization) => authorization.tenant_id(),
        AuditTenant::System => SYSTEM_TENANT_ID,
    };
    if event.tenant_id != expected_tenant {
        return Err(ErrorCode::TenantBoundary);
    }
    let client_ip = event
        .client_ip
        .parse::<std::net::IpAddr>()
        .map_err(|_| ErrorCode::InvalidInput)?;
    let metadata = Value::Object(
        event
            .metadata
            .iter()
            .map(|(key, value)| (key.to_owned(), Value::String(value.to_owned())))
            .collect(),
    );

    match tenant {
        AuditTenant::Authenticated(authorization) => {
            set_authorization_local(txn, authorization).await?
        }
        AuditTenant::System => set_system_tenant_local(txn).await?,
    }
    let event_id: Uuid = sqlx::query_scalar(
        "SELECT control.audit_event_insert( \
           $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::inet, $13, $14, $15, $16, $17)",
    )
    .bind(event.event_id.0)
    .bind(OffsetDateTime::from(event.ts))
    .bind(event.tenant_id.0)
    .bind(&event.actor_type)
    .bind(&event.actor_id)
    .bind(&event.action)
    .bind(&event.resource_type)
    .bind(&event.resource_id)
    .bind(&event.result)
    .bind(&event.request_id)
    .bind(&event.trace_id)
    .bind(client_ip.to_string())
    .bind(&event.user_agent_hash)
    .bind(&event.risk_tags)
    .bind(&event.before_fingerprint)
    .bind(&event.after_fingerprint)
    .bind(metadata)
    .fetch_one(&mut **txn)
    .await
    .map_err(|error| match db_error(error) {
        // Typed audit fields have already been checked. Internal SQL/constraint failures
        // cannot turn an otherwise valid request into a client-input error.
        ErrorCode::InvalidInput => ErrorCode::Internal,
        code => code,
    })?;
    Ok(AuditEventId(event_id))
}

fn validate_request_audit(
    auth: &AuthorizationScope,
    event: &AuditEvent,
    action: McpAuditAction,
) -> Result<Uuid, ErrorCode> {
    if event.tenant_id != auth.tenant_id() || event.actor_id != auth.principal().0.to_string() {
        return Err(ErrorCode::TenantBoundary);
    }
    let request_id = Uuid::parse_str(&event.request_id).map_err(|_| ErrorCode::InvalidInput)?;
    if request_id.is_nil()
        || event.action != action.as_str()
        || event.resource_type != "MCP_OPERATION"
    {
        return Err(ErrorCode::InvalidInput);
    }
    Ok(request_id)
}

/// Reserve one read BMO and its audit atomically; a failed audit leaves no reservation.
pub async fn reserve_read_with_audit(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    operation: &str,
    request_fingerprint: &str,
    ttl: std::time::Duration,
    reserved_audit: &AuditEvent,
) -> Result<QuotaReservation, ErrorCode> {
    let request_id =
        validate_request_audit(auth, reserved_audit, McpAuditAction::McpQuotaReserved)?;
    if reserved_audit.resource_id != operation || reserved_audit.result != "OK" {
        return Err(ErrorCode::InvalidInput);
    }
    // dep: PostgreSQL(role_gateway) — transaction entry for `reserve_read_with_audit`
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    let reservation = match quota_repo::reserve_bmo_in_txn(
        &mut txn,
        auth,
        request_id,
        operation,
        request_fingerprint,
        ttl,
    )
    .await?
    {
        ReserveResult::Created(reservation) => reservation,
        ReserveResult::Existing(_) => return Err(ErrorCode::Conflict),
    };
    audit_event_insert_in_txn(&mut txn, AuditTenant::Authenticated(auth), reserved_audit).await?;
    txn.commit().await.map_err(db_error)?;
    Ok(reservation)
}

/// Settle a local read with both quota and finished audits in the same transaction.
/// False means the read did not consume a BMO; the caller must withhold a success when
/// consumption was requested but its lease expired. A failed audit rolls back settlement.
pub async fn settle_read_with_audit(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    reservation: Option<&QuotaReservation>,
    consume: bool,
    finished_audit: &AuditEvent,
) -> Result<bool, ErrorCode> {
    let request_id =
        validate_request_audit(auth, finished_audit, McpAuditAction::McpRequestFinished)?;
    if reservation.is_some_and(|r| r.request_id() != request_id) {
        return Err(ErrorCode::Conflict);
    }
    // dep: PostgreSQL(role_gateway) — transaction entry for `settle_read_with_audit`
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    let mut consumed = false;
    if let Some(reservation) = reservation {
        let status =
            quota_repo::finish_reservation_in_txn(&mut txn, auth, reservation, consume).await?;
        consumed = status == ReservationStatus::Consumed;
        let mut quota_audit = finished_audit.clone();
        quota_audit.event_id = AuditEventId::new();
        quota_audit.action = if consumed {
            McpAuditAction::McpQuotaConsumed
        } else {
            McpAuditAction::McpQuotaReleased
        }
        .as_str()
        .to_owned();
        audit_event_insert_in_txn(&mut txn, AuditTenant::Authenticated(auth), &quota_audit).await?;
    }
    let mut finished = finished_audit.clone();
    if consume && reservation.is_some() && !consumed {
        finished.result = ErrorCode::Conflict.as_str().to_owned();
    }
    audit_event_insert_in_txn(&mut txn, AuditTenant::Authenticated(auth), &finished).await?;
    if let Some(reservation) = reservation.filter(|_| consumed) {
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *txn)
            .await
            .map_err(db_error)?;
        if now >= reservation.expires_at() {
            return Err(ErrorCode::Conflict);
        }
    }
    txn.commit().await.map_err(db_error)?;
    Ok(consumed || reservation.is_none())
}
