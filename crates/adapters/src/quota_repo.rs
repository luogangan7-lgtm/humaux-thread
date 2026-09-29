//! `adapters::quota_repo` — §72.2.1: durable, tenant-bound quota reservations and independent rate buckets.
//! Depends-on: crates=[humaux-domain, sqlx, time, uuid]; services=[PostgreSQL(any) w=[control.quota_windows,
//!   control.rate_buckets, control.usage_reservations] x=[control.issue_quota_window,
//!   control.reap_quota_reservations], PostgreSQL(role_gateway), PostgreSQL(role_maintenance)]; env=[];
//!   modules=[adapters::postgres, domain::audit, domain::error, domain::identity, domain::ids]
//! Called-by: [adapters::context_repo, adapters::distill_repo, adapters::memory_governance_repo, adapters::operation_receipt, adapters::request_guard_repo, gateway::bootstrap, gateway::guard, maintenance::main, tests, xtask::e2e_seed]
//! Invariants: [the gateway only consumes existing quota windows; only role_maintenance can issue or reap them;
//!   exhaustion is QuotaExhausted/RateLimited and a PG error DependencyUnavailable, never an allow]
//! Spec: none
//!
//! The gateway consumes existing windows; only maintenance can invoke their issuer.

use std::{net::IpAddr, time::Duration};

use humaux_domain::{
    audit::SYSTEM_TENANT_ID, error::ErrorCode, identity::AuthorizationScope, ids::TenantId,
};
use sqlx::{Row, postgres::PgRow, types::time::OffsetDateTime};
use uuid::Uuid;

use crate::postgres::{MaintenanceDbPool, RuntimeDbPool};

pub const BMO_ENTITLEMENT: &str = "mcp.billable_operations.per_period";
type Txn<'a> = sqlx::Transaction<'a, sqlx::Postgres>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaWindow {
    pub window_start: OffsetDateTime,
    pub window_end: OffsetDateTime,
    pub hard_limit: i64,
    pub reserved: i64,
    pub consumed: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReservationStatus {
    Reserved,
    Consumed,
    Released,
}

impl ReservationStatus {
    fn parse(value: &str) -> Result<Self, ErrorCode> {
        match value {
            "RESERVED" => Ok(Self::Reserved),
            "CONSUMED" => Ok(Self::Consumed),
            "RELEASED" => Ok(Self::Released),
            _ => Err(ErrorCode::Internal),
        }
    }
}

/// Only a successfully committed new reservation permits dispatch. Never deserialize
/// this from tool arguments or treat an idempotent replay as permission to run again.
#[derive(Debug)]
pub struct QuotaReservation {
    reservation_id: Uuid,
    request_id: Uuid,
    tenant_id: TenantId,
    principal_id: Uuid,
    expires_at: OffsetDateTime,
    valid_for: Duration,
}

impl QuotaReservation {
    pub fn id(&self) -> Uuid {
        self.reservation_id
    }
    pub fn request_id(&self) -> Uuid {
        self.request_id
    }
    pub fn expires_at(&self) -> OffsetDateTime {
        self.expires_at
    }
    /// Database-computed lease duration, capped at the current period boundary.
    /// A caller subtracts the reservation round-trip with its monotonic clock.
    pub fn valid_for(&self) -> Duration {
        self.valid_for
    }
}

#[derive(Debug)]
pub enum ReserveResult {
    Created(QuotaReservation),
    /// This adapter does not retain business responses. The caller must return an
    /// explicit conflict/in-progress result, not execute or bill a replay again.
    Existing(ReservationStatus),
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

fn field<T>(row: &PgRow, name: &str) -> Result<T, ErrorCode>
where
    T: for<'r> sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get(name).map_err(|_| ErrorCode::Internal)
}

async fn bind_tenant(
    txn: &mut Txn<'_>,
    tenant: TenantId,
    user: Option<Uuid>,
) -> Result<(), ErrorCode> {
    sqlx::query(
        "SELECT set_config('humaux.tenant_id', $1, true), set_config('humaux.user_id', $2, true)",
    )
    .bind(tenant.0.to_string())
    .bind(user.unwrap_or_else(Uuid::nil).to_string())
    .execute(&mut **txn)
    .await
    .map_err(db_error)?;
    Ok(())
}

fn validate_auth(auth: &AuthorizationScope) -> Result<(), ErrorCode> {
    if auth.tenant_id().0.is_nil() || auth.principal().0.is_nil() {
        return Err(ErrorCode::Unauthorized);
    }
    Ok(())
}

fn valid_key(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=96).contains(&bytes.len())
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_' || *b == b'.')
}

fn window(row: &PgRow) -> Result<QuotaWindow, ErrorCode> {
    Ok(QuotaWindow {
        window_start: field(row, "window_start")?,
        window_end: field(row, "window_end")?,
        hard_limit: field(row, "hard_limit")?,
        reserved: field(row, "reserved")?,
        consumed: field(row, "consumed")?,
    })
}

/// The fixed definer reads the projected snapshot. No caller-supplied limit, clock, or
/// period is accepted, and the checked maintenance pool never becomes a gateway pool.
pub async fn issue_window(
    pool: &MaintenanceDbPool,
    tenant: TenantId,
) -> Result<QuotaWindow, ErrorCode> {
    if tenant.0.is_nil() {
        return Err(ErrorCode::InvalidInput);
    }
    // dep: PostgreSQL(role_maintenance) — transaction entry for `issue_window`
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    bind_tenant(&mut txn, tenant, None).await?;
    let row = sqlx::query("SELECT * FROM control.issue_quota_window($1, $2)")
        .bind(tenant.0)
        .bind(BMO_ENTITLEMENT)
        .fetch_one(&mut *txn)
        .await
        .map_err(db_error)?;
    let result = window(&row)?;
    txn.commit().await.map_err(db_error)?;
    Ok(result)
}

/// Claims one BMO atomically. Retries are bound to principal, operation, and the exact
/// request fingerprint; a different request cannot borrow the original reservation.
pub async fn reserve_bmo(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    request_id: Uuid,
    operation: &str,
    request_fingerprint: &str,
    ttl: Duration,
) -> Result<ReserveResult, ErrorCode> {
    // dep: PostgreSQL(role_gateway) — transaction entry for `reserve_bmo`
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    let result = reserve_bmo_in_txn(
        &mut txn,
        auth,
        request_id,
        operation,
        request_fingerprint,
        ttl,
    )
    .await?;
    txn.commit().await.map_err(db_error)?;
    Ok(result)
}

fn validate_reservation_request(
    request_id: Uuid,
    operation: &str,
    request_fingerprint: &str,
    ttl: Duration,
) -> Result<i64, ErrorCode> {
    let ttl_micros = i64::try_from(ttl.as_micros()).map_err(|_| ErrorCode::InvalidInput)?;
    if request_id.is_nil()
        || !valid_key(operation)
        || ttl_micros <= 0
        || request_fingerprint.len() != 64
        || !request_fingerprint
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ErrorCode::InvalidInput);
    }
    Ok(ttl_micros)
}

/// Same reservation SQL for a local business transaction; never commits the caller's work.
pub async fn reserve_bmo_in_txn(
    txn: &mut Txn<'_>,
    auth: &AuthorizationScope,
    request_id: Uuid,
    operation: &str,
    request_fingerprint: &str,
    ttl: Duration,
) -> Result<ReserveResult, ErrorCode> {
    validate_auth(auth)?;
    let ttl_micros = validate_reservation_request(request_id, operation, request_fingerprint, ttl)?;
    bind_tenant(txn, auth.tenant_id(), auth.user_id().map(|id| id.0)).await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("quota-request:{}:{request_id}", auth.tenant_id().0))
        .execute(&mut **txn)
        .await
        .map_err(db_error)?;
    if let Some(row) = sqlx::query(
        "SELECT principal_id, operation, request_fingerprint, status FROM control.usage_reservations \
         WHERE tenant_id = $1 AND request_id = $2 FOR UPDATE")
        .bind(auth.tenant_id().0).bind(request_id).fetch_optional(&mut **txn).await.map_err(db_error)? {
        if field::<Uuid>(&row, "principal_id")? != auth.principal().0
            || field::<String>(&row, "operation")? != operation
            || field::<String>(&row, "request_fingerprint")? != request_fingerprint {
            return Err(ErrorCode::Conflict);
        }
        let status = ReservationStatus::parse(&field::<String>(&row, "status")?)?;
        return Ok(ReserveResult::Existing(status));
    }
    let rows = sqlx::query(
        "SELECT window_start FROM control.quota_windows WHERE tenant_id = $1 AND entitlement_key = $2 \
         AND window_start <= clock_timestamp() AND window_end > clock_timestamp() \
         ORDER BY window_start LIMIT 2 FOR UPDATE")
        .bind(auth.tenant_id().0).bind(BMO_ENTITLEMENT)
        .fetch_all(&mut **txn).await.map_err(db_error)?;
    let start: OffsetDateTime = match rows.as_slice() {
        [row] => field(row, "window_start")?,
        [] => return Err(ErrorCode::QuotaExhausted),
        _ => return Err(ErrorCode::Conflict),
    };
    // Recheck the database clock after any row-lock wait. Numeric arithmetic cannot wrap
    // a corrupt/near-i64-limit counter into an apparent positive allowance.
    let row = sqlx::query(
        "UPDATE control.quota_windows SET reserved = reserved + 1 \
         WHERE tenant_id = $1 AND entitlement_key = $2 AND window_start = $3 \
         AND window_end > clock_timestamp() \
         AND hard_limit::numeric - reserved::numeric - consumed::numeric >= 1 \
         RETURNING window_end, clock_timestamp() AS reserved_at",
    )
    .bind(auth.tenant_id().0)
    .bind(BMO_ENTITLEMENT)
    .bind(start)
    .fetch_optional(&mut **txn)
    .await
    .map_err(db_error)?
    .ok_or(ErrorCode::QuotaExhausted)?;
    let reserved_at: OffsetDateTime = field(&row, "reserved_at")?;
    let end: OffsetDateTime = field(&row, "window_end")?;
    let expires_at = reserved_at
        .checked_add(time::Duration::microseconds(ttl_micros))
        .ok_or(ErrorCode::InvalidInput)?
        .min(end);
    let reservation_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO control.usage_reservations \
         (reservation_id, request_id, tenant_id, principal_id, entitlement_key, window_start, \
          operation, request_fingerprint, units, status, created_at, expires_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, 'RESERVED', $9, $10)",
    )
    .bind(reservation_id)
    .bind(request_id)
    .bind(auth.tenant_id().0)
    .bind(auth.principal().0)
    .bind(BMO_ENTITLEMENT)
    .bind(start)
    .bind(operation)
    .bind(request_fingerprint)
    .bind(reserved_at)
    .bind(expires_at)
    .execute(&mut **txn)
    .await
    .map_err(db_error)?;
    if !sqlx::query_scalar::<_, bool>("SELECT clock_timestamp() < $1")
        .bind(expires_at)
        .fetch_one(&mut **txn)
        .await
        .map_err(db_error)?
    {
        return Err(ErrorCode::Conflict);
    }
    Ok(ReserveResult::Created(QuotaReservation {
        reservation_id,
        request_id,
        tenant_id: auth.tenant_id(),
        principal_id: auth.principal().0,
        expires_at,
        valid_for: (expires_at - reserved_at)
            .try_into()
            .map_err(|_| ErrorCode::Internal)?,
    }))
}

/// Finalization and window counters commit together. Terminal transitions are idempotent
/// and never reopened. An expired reservation is released, not charged after its lease.
pub async fn finish_reservation(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    reservation: &QuotaReservation,
    consume: bool,
) -> Result<ReservationStatus, ErrorCode> {
    // dep: PostgreSQL(role_gateway) — transaction entry for `finish_reservation`
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    let status = finish_reservation_in_txn(&mut txn, auth, reservation, consume).await?;
    txn.commit().await.map_err(db_error)?;
    if consume && status != ReservationStatus::Consumed {
        return Err(ErrorCode::Conflict);
    }
    Ok(status)
}

/// Returns the actual terminal status without committing. A caller coupling a business
/// write to quota must require Consumed and roll the entire transaction back otherwise.
pub async fn finish_reservation_in_txn(
    txn: &mut Txn<'_>,
    auth: &AuthorizationScope,
    reservation: &QuotaReservation,
    consume: bool,
) -> Result<ReservationStatus, ErrorCode> {
    validate_auth(auth)?;
    if auth.tenant_id() != reservation.tenant_id || auth.principal().0 != reservation.principal_id {
        return Err(ErrorCode::Forbidden);
    }
    bind_tenant(txn, auth.tenant_id(), auth.user_id().map(|id| id.0)).await?;
    let row = sqlx::query(
        "SELECT principal_id, request_id, window_start, units, status, expires_at \
         FROM control.usage_reservations WHERE tenant_id = $1 AND reservation_id = $2 FOR UPDATE",
    )
    .bind(auth.tenant_id().0)
    .bind(reservation.reservation_id)
    .fetch_one(&mut **txn)
    .await
    .map_err(db_error)?;
    if field::<Uuid>(&row, "principal_id")? != auth.principal().0
        || field::<Uuid>(&row, "request_id")? != reservation.request_id
    {
        return Err(ErrorCode::Forbidden);
    }
    let status = ReservationStatus::parse(&field::<String>(&row, "status")?)?;
    let desired = if consume {
        ReservationStatus::Consumed
    } else {
        ReservationStatus::Released
    };
    if status != ReservationStatus::Reserved {
        if status != desired {
            return Err(ErrorCode::Conflict);
        }
        return Ok(status);
    }
    let start: OffsetDateTime = field(&row, "window_start")?;
    let units: i64 = field(&row, "units")?;
    // Serialize with reservation/reaper and window writers before checking lease time.
    sqlx::query("SELECT 1 FROM control.quota_windows WHERE tenant_id = $1 AND entitlement_key = $2 AND window_start = $3 FOR UPDATE")
        .bind(auth.tenant_id().0).bind(BMO_ENTITLEMENT).bind(start)
        .fetch_one(&mut **txn).await.map_err(db_error)?;
    let alive: bool = sqlx::query_scalar("SELECT clock_timestamp() < $1")
        .bind(field::<OffsetDateTime>(&row, "expires_at")?)
        .fetch_one(&mut **txn)
        .await
        .map_err(db_error)?;
    let charge = consume && alive;
    sqlx::query("UPDATE control.quota_windows SET reserved = reserved - $4, consumed = consumed + $5 WHERE tenant_id = $1 AND entitlement_key = $2 AND window_start = $3")
        .bind(auth.tenant_id().0).bind(BMO_ENTITLEMENT).bind(start).bind(units).bind(if charge { units } else { 0 })
        .execute(&mut **txn).await.map_err(db_error)?;
    let finalized = sqlx::query("UPDATE control.usage_reservations SET status = $3, finished_at = clock_timestamp() WHERE tenant_id = $1 AND reservation_id = $2 AND (NOT $4 OR expires_at > clock_timestamp())")
        .bind(auth.tenant_id().0).bind(reservation.reservation_id).bind(if charge { "CONSUMED" } else { "RELEASED" })
        .bind(charge)
        .execute(&mut **txn).await.map_err(db_error)?;
    if finalized.rows_affected() != 1 {
        return Err(ErrorCode::Conflict); // Roll back both counters; the reaper releases it.
    }
    Ok(if charge {
        ReservationStatus::Consumed
    } else {
        ReservationStatus::Released
    })
}

pub async fn reap_expired(
    pool: &MaintenanceDbPool,
    tenant: TenantId,
    limit: i32,
) -> Result<i64, ErrorCode> {
    if tenant.0.is_nil() || limit <= 0 {
        return Err(ErrorCode::InvalidInput);
    }
    // dep: PostgreSQL(role_maintenance) — transaction entry for `reap_expired`
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    bind_tenant(&mut txn, tenant, None).await?;
    let reaped = sqlx::query_scalar("SELECT control.reap_quota_reservations($1, $2)")
        .bind(tenant.0)
        .bind(limit)
        .fetch_one(&mut *txn)
        .await
        .map_err(db_error)?;
    txn.commit().await.map_err(db_error)?;
    Ok(reaped)
}

/// Bootstrap supplies policy from the Config Registry; no hardcoded limit or fallback.
#[derive(Debug, Clone, Copy)]
pub struct RatePolicy {
    capacity: i64,
    refill_per_second: i64,
}

impl RatePolicy {
    pub fn new(capacity: i64, refill_per_second: i64) -> Result<Self, ErrorCode> {
        if capacity <= 0 || refill_per_second <= 0 {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            capacity,
            refill_per_second,
        })
    }
}

pub enum RateSubject<'a> {
    /// The trusted network adapter determines this IP, never a raw forwarding header.
    PreauthIp(IpAddr),
    Credential {
        auth: &'a AuthorizationScope,
        credential_id: Uuid,
    },
    User(&'a AuthorizationScope),
    Tenant(&'a AuthorizationScope),
}

/// Each bucket is a short independent transaction. A later entitlement/quota failure
/// cannot refund an already admitted abuse-limit attempt. PG failures fail closed.
pub async fn consume_rate(
    pool: &RuntimeDbPool,
    subject: RateSubject<'_>,
    operation: &str,
    bucket_key: &str,
    policy: RatePolicy,
) -> Result<(), ErrorCode> {
    if !valid_key(operation) || !valid_key(bucket_key) {
        return Err(ErrorCode::InvalidInput);
    }
    let (tenant, user, kind, id) = match subject {
        RateSubject::PreauthIp(ip) => (SYSTEM_TENANT_ID, None, "ip", ip.to_string()),
        RateSubject::Credential {
            auth,
            credential_id,
        } => {
            validate_auth(auth)?;
            if credential_id.is_nil() {
                return Err(ErrorCode::InvalidInput);
            }
            (
                auth.tenant_id(),
                auth.user_id().map(|id| id.0),
                "credential",
                credential_id.to_string(),
            )
        }
        RateSubject::User(auth) => {
            validate_auth(auth)?;
            let user = auth.user_id().ok_or(ErrorCode::Forbidden)?;
            (auth.tenant_id(), Some(user.0), "user", user.0.to_string())
        }
        RateSubject::Tenant(auth) => {
            validate_auth(auth)?;
            (
                auth.tenant_id(),
                auth.user_id().map(|id| id.0),
                "tenant",
                auth.tenant_id().0.to_string(),
            )
        }
    };
    // dep: PostgreSQL(role_gateway) — transaction entry for `consume_rate`
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    bind_tenant(&mut txn, tenant, user).await?;
    // Concurrent consumers of ONE bucket are serialized by waiting, not by refusing. Two
    // simultaneous requests from one client IP (a NAT; the soak's two lanes on 127.0.0.1) both
    // land on the pre-auth `ip` bucket, and with `pg_try_advisory_xact_lock` the loser was
    // answered RATE_LIMITED while the bucket held 99 of 100 tokens (card 24 rehearsal4,
    // 2026-09-26: 1–2 of ~125 calls per soak) — a lock outcome reported as the rate verdict
    // §72.3 reserves for "秒/分钟级速率超限". The critical section is one row update; the wait
    // is bounded by `lock_timeout`, and a timeout fails closed through `db_error`, never as a
    // rate decision.
    sqlx::query("SET LOCAL lock_timeout = '2s'")
        .execute(&mut *txn)
        .await
        .map_err(db_error)?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!(
            "rate:{}:{kind}:{id}:{operation}:{bucket_key}",
            tenant.0
        ))
        .execute(&mut *txn)
        .await
        .map_err(db_error)?;
    sqlx::query(
        "INSERT INTO control.rate_buckets (tenant_id, subject_kind, subject_id, operation, bucket_key, capacity, tokens, refill_per_second) \
         VALUES ($1, $2, $3, $4, $5, $6, $6, $7) ON CONFLICT DO NOTHING")
        .bind(tenant.0).bind(kind).bind(&id).bind(operation).bind(bucket_key)
        .bind(policy.capacity).bind(policy.refill_per_second)
        .execute(&mut *txn).await.map_err(db_error)?;
    let allowed: bool = sqlx::query_scalar(
        "WITH current_bucket AS MATERIALIZED ( \
           SELECT *, clock_timestamp() AS tick FROM control.rate_buckets \
           WHERE tenant_id = $1 AND subject_kind = $2 AND subject_id = $3 AND operation = $4 AND bucket_key = $5 FOR UPDATE \
         ), refill AS MATERIALIZED ( \
           SELECT LEAST($6::bigint::numeric, tokens + GREATEST(EXTRACT(EPOCH FROM (tick - updated_at)), 0) * refill_per_second) AS available, \
                  GREATEST(tick, updated_at) AS next_tick FROM current_bucket \
         ) UPDATE control.rate_buckets SET capacity = $6, \
             tokens = refill.available - CASE WHEN refill.available >= 1 THEN 1 ELSE 0 END, \
             refill_per_second = $7, updated_at = refill.next_tick, version = version + 1 \
           FROM refill WHERE tenant_id = $1 AND subject_kind = $2 AND subject_id = $3 AND operation = $4 AND bucket_key = $5 \
           RETURNING refill.available >= 1")
        .bind(tenant.0).bind(kind).bind(&id).bind(operation).bind(bucket_key)
        .bind(policy.capacity).bind(policy.refill_per_second)
        .fetch_one(&mut *txn).await.map_err(db_error)?;
    txn.commit().await.map_err(db_error)?;
    if allowed {
        Ok(())
    } else {
        Err(ErrorCode::RateLimited)
    }
}
