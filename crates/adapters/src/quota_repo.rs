//! `adapters::quota_repo` — §72.2.1: durable, tenant-bound quota reservations and independent rate buckets.
//! Depends-on: crates=[humaux-domain, sqlx, time, uuid]; services=[PostgreSQL(any) w=[control.quota_windows,
//!   control.rate_buckets, control.usage_reservations] x=[control.issue_quota_window,
//!   control.reap_quota_reservations], PostgreSQL(role_gateway), PostgreSQL(role_maintenance)]; env=[];
//!   modules=[adapters::postgres, domain::audit, domain::error, domain::identity, domain::ids]
//! Called-by: [adapters::context_repo, adapters::distill_repo, adapters::memory_governance_repo,
//!   adapters::operation_receipt, adapters::request_guard_repo, gateway::bootstrap, gateway::guard,
//!   maintenance::main, maintenance::serve, tests, xtask::e2e_seed, xtask::load]
//! Invariants: [the gateway only consumes existing quota windows; only role_maintenance can issue or reap them;
//!   exhaustion is QuotaExhausted/RateLimited and a PG error DependencyUnavailable, never an allow; one request's
//!   rate buckets are one transaction whose advisory locks are taken in tier order (tenant, user, credential/shared,
//!   credential/operation), the only rate-lock statement in the workspace]
//! Spec: Baseline §72.2.1; §73.2; ADR-0062 E5; ADR-0065 D-D, D-E
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

/// Whose bucket a [`RateCharge`] draws on.
pub enum RateSubject<'a> {
    /// The trusted network adapter determines this IP, never a raw forwarding header. The `u8` is the IPv6
    /// prefix length the address is keyed by (`HUMAUX_GATEWAY_RATE_PREAUTH_IPV6_PREFIX_BITS`, 1..=128).
    PreauthIp(IpAddr, u8),
    Credential {
        auth: &'a AuthorizationScope,
        credential_id: Uuid,
    },
    User(&'a AuthorizationScope),
    Tenant(&'a AuthorizationScope),
}

/// The `operation` of the buckets every authenticated request draws on, as opposed to a per-operation bucket.
/// ADR-0065 D-D: a credential charge on it is lock tier 2, a credential charge on any other operation tier 3.
pub const SHARED_RATE_OPERATION: &str = "mcp";

/// The pre-auth `ip` bucket's `subject_id`: the full address for IPv4, the `/{bits}` network for IPv6.
// §73.2 (ADR-0062 E5, SEC-6; ADR-0065 D-E): canonicalise first, so `::ffff:a.b.c.d` keys as the IPv4 address it is;
// one IPv6 end site owns a whole prefix, so a /128 key would hand it 2^(128-bits) independent buckets. Ruling W-10:
// loopback and link-local addresses key as themselves (/128). `bits` outside 1..=128 is clamped here; the
// repository refuses it before a key is built. Authenticated keys (credential/user/tenant) are untouched.
#[must_use]
pub fn preauth_ip_subject(ip: IpAddr, ipv6_prefix_bits: u8) -> String {
    match ip.to_canonical() {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => {
            let bits = if v6.is_loopback() || v6.is_unicast_link_local() {
                128
            } else {
                ipv6_prefix_bits.clamp(1, 128)
            };
            let net =
                std::net::Ipv6Addr::from(u128::from(v6) & (u128::MAX << (128 - u32::from(bits))));
            format!("{net}/{bits}")
        }
    }
}

/// One bucket a [`consume_rate_batch`] call draws a token from.
pub struct RateCharge<'a> {
    /// Whose bucket.
    pub subject: RateSubject<'a>,
    /// [`SHARED_RATE_OPERATION`] or the canonical operation key (`^[a-z][a-z0-9_.]{0,95}$`).
    pub operation: &'a str,
    /// The bucket's name within the subject and operation (same pattern).
    pub bucket_key: &'a str,
    /// Capacity and refill from the Config Registry.
    pub policy: RatePolicy,
}

/// A charge resolved to its row key and its advisory-lock key.
struct Bucket {
    /// ADR-0065 D-D lock tier: tenant 0, user 1, credential/shared 2, credential/operation 3, pre-auth ip 4.
    tier: u8,
    tenant: TenantId,
    user: Option<Uuid>,
    kind: &'static str,
    id: String,
    lock_key: String,
}

fn resolve(charge: &RateCharge<'_>) -> Result<Bucket, ErrorCode> {
    let (operation, bucket_key) = (charge.operation, charge.bucket_key);
    if !valid_key(operation) || !valid_key(bucket_key) {
        return Err(ErrorCode::InvalidInput);
    }
    let (tier, tenant, user, kind, id) = match charge.subject {
        RateSubject::PreauthIp(ip, bits) => {
            if !(1..=128).contains(&bits) {
                return Err(ErrorCode::InvalidInput);
            }
            (
                4,
                SYSTEM_TENANT_ID,
                None,
                "ip",
                preauth_ip_subject(ip, bits),
            )
        }
        RateSubject::Credential {
            auth,
            credential_id,
        } => {
            validate_auth(auth)?;
            if credential_id.is_nil() {
                return Err(ErrorCode::InvalidInput);
            }
            (
                if operation == SHARED_RATE_OPERATION {
                    2
                } else {
                    3
                },
                auth.tenant_id(),
                auth.user_id().map(|id| id.0),
                "credential",
                credential_id.to_string(),
            )
        }
        RateSubject::User(auth) => {
            validate_auth(auth)?;
            let user = auth.user_id().ok_or(ErrorCode::Forbidden)?;
            (
                1,
                auth.tenant_id(),
                Some(user.0),
                "user",
                user.0.to_string(),
            )
        }
        RateSubject::Tenant(auth) => {
            validate_auth(auth)?;
            (
                0,
                auth.tenant_id(),
                auth.user_id().map(|id| id.0),
                "tenant",
                auth.tenant_id().0.to_string(),
            )
        }
    };
    let lock_key = rate_lock_key(&tenant.0.to_string(), kind, &id, operation, bucket_key);
    Ok(Bucket {
        tier,
        tenant,
        user,
        kind,
        id,
        lock_key,
    })
}

/// ADR-0065 D-D: the order a batch takes its advisory locks in — tier, then the lock key. Every rate lock is taken
/// in this one total order, so no two batches can wait on each other in a cycle, whatever their input order.
fn lock_order(buckets: &[Bucket]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..buckets.len()).collect();
    order.sort_by(|&a, &b| {
        (buckets[a].tier, &buckets[a].lock_key).cmp(&(buckets[b].tier, &buckets[b].lock_key))
    });
    order
}

/// The text whose `hashtextextended(.., 0)` is a rate bucket's advisory-lock key (ADR-0065 D-D). Public so the load
/// harness (`xtask::load`) can name the tenant lock whose waits it measures without a second copy of the format.
#[must_use]
pub fn rate_lock_key(
    tenant: &str,
    kind: &str,
    id: &str,
    operation: &str,
    bucket_key: &str,
) -> String {
    format!("rate:{tenant}:{kind}:{id}:{operation}:{bucket_key}")
}

/// ADR-0065 D-D: in the rate path a wait the server ended — 55P03 (lock_timeout), 57014 (statement_timeout), 40P01
/// (deadlock detector) — is the store being unavailable: 503, never a rate verdict (card 24) nor a conflict. Other
/// paths keep [`db_error`] (the 57014 arm elsewhere is card 38b, E-7).
fn rate_db_error(error: sqlx::Error) -> ErrorCode {
    match &error {
        sqlx::Error::Database(db)
            if matches!(db.code().as_deref(), Some("55P03" | "57014" | "40P01")) =>
        {
            ErrorCode::DependencyUnavailable
        }
        _ => db_error(error),
    }
}

/// ADR-0065 D-D: every bucket of one request in ONE transaction. All charges share one tenant and user binding
/// (`InvalidInput` otherwise). Locks are taken in [`lock_order`] under `lock_timeout`; the tokens are then taken in
/// INPUT order, and the first denial commits what was taken before it and returns `(RateLimited, its index)` —
/// later charges are not taken, the same spend as one transaction per bucket in input order. Any other code carries
/// the index of the charge in progress (0 for begin / binding / commit). PG failures fail closed.
pub async fn consume_rate_batch(
    pool: &RuntimeDbPool,
    charges: &[RateCharge<'_>],
    lock_timeout: Duration,
) -> Result<(), (ErrorCode, usize)> {
    let buckets = charges
        .iter()
        .enumerate()
        .map(|(i, charge)| resolve(charge).map_err(|code| (code, i)))
        .collect::<Result<Vec<_>, _>>()?;
    let first = buckets.first().ok_or((ErrorCode::InvalidInput, 0))?;
    if let Some(i) = buckets
        .iter()
        .position(|b| (b.tenant, b.user) != (first.tenant, first.user))
    {
        return Err((ErrorCode::InvalidInput, i));
    }
    // A zero lock_timeout is PostgreSQL's "wait forever"; the key is a positive bound (bootstrap refuses 0).
    let lock_ms = u64::try_from(lock_timeout.as_millis())
        .ok()
        .filter(|ms| *ms > 0)
        .ok_or((ErrorCode::InvalidInput, 0))?;
    let at = |i: usize| move |error: sqlx::Error| (rate_db_error(error), i);
    // dep: PostgreSQL(role_gateway) — transaction entry for `consume_rate_batch`
    let mut txn = pool.pool().begin().await.map_err(at(0))?;
    bind_tenant(&mut txn, first.tenant, first.user)
        .await
        .map_err(|code| (code, 0))?;
    // Concurrent consumers of ONE bucket are serialized by waiting, not by refusing (card 24 rehearsal4,
    // 2026-09-26: a try-lock loser was answered RATE_LIMITED with 99 of 100 tokens left). The wait is bounded by
    // the registered lock_timeout (§78.1; SET LOCAL takes no bind parameter, set_config does).
    sqlx::query("SELECT set_config('lock_timeout', $1, true)")
        .bind(format!("{lock_ms}ms"))
        .execute(&mut *txn)
        .await
        .map_err(at(0))?;
    for i in lock_order(&buckets) {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(&buckets[i].lock_key)
            .execute(&mut *txn)
            .await
            .map_err(at(i))?;
    }
    let column = |f: fn(&Bucket, &RateCharge<'_>) -> String| -> Vec<String> {
        buckets.iter().zip(charges).map(|(b, c)| f(b, c)).collect()
    };
    sqlx::query(
        "INSERT INTO control.rate_buckets (tenant_id, subject_kind, subject_id, operation, bucket_key, capacity, tokens, refill_per_second) \
         SELECT $1, k, s, o, b, c, c, r FROM unnest($2::text[], $3::text[], $4::text[], $5::text[], $6::bigint[], $7::bigint[]) AS t(k, s, o, b, c, r) \
         ON CONFLICT DO NOTHING")
        .bind(first.tenant.0)
        .bind(column(|b, _| b.kind.to_owned()))
        .bind(column(|b, _| b.id.clone()))
        .bind(column(|_, c| c.operation.to_owned()))
        .bind(column(|_, c| c.bucket_key.to_owned()))
        .bind(charges.iter().map(|c| c.policy.capacity).collect::<Vec<_>>())
        .bind(charges.iter().map(|c| c.policy.refill_per_second).collect::<Vec<_>>())
        .execute(&mut *txn).await.map_err(at(0))?;
    for (i, (bucket, charge)) in buckets.iter().zip(charges).enumerate() {
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
            .bind(first.tenant.0).bind(bucket.kind).bind(&bucket.id).bind(charge.operation).bind(charge.bucket_key)
            .bind(charge.policy.capacity).bind(charge.policy.refill_per_second)
            .fetch_one(&mut *txn).await.map_err(at(i))?;
        if !allowed {
            txn.commit().await.map_err(at(i))?;
            return Err((ErrorCode::RateLimited, i));
        }
    }
    txn.commit().await.map_err(at(0))?;
    Ok(())
}

/// One bucket: a batch of one ([`consume_rate_batch`]). The pre-auth `ip` bucket runs alone, before
/// authentication and under the SYSTEM tenant (ADR-0065 D-D rejected d). A later entitlement/quota failure cannot
/// refund an already admitted abuse-limit attempt.
pub async fn consume_rate(
    pool: &RuntimeDbPool,
    subject: RateSubject<'_>,
    operation: &str,
    bucket_key: &str,
    policy: RatePolicy,
    lock_timeout: Duration,
) -> Result<(), ErrorCode> {
    consume_rate_batch(
        pool,
        &[RateCharge {
            subject,
            operation,
            bucket_key,
            policy,
        }],
        lock_timeout,
    )
    .await
    .map_err(|(code, _)| code)
}

#[cfg(test)]
mod tests {
    use super::*;
    use humaux_domain::{
        identity::{BoundedSet, PrincipalId},
        ids::UserId,
    };

    fn auth() -> AuthorizationScope {
        AuthorizationScope::new(
            TenantId(Uuid::from_u128(1)),
            PrincipalId(Uuid::from_u128(2)),
            Some(UserId(Uuid::from_u128(3))),
            BoundedSet::new([]).expect("empty workspace set"),
        )
    }

    fn policy() -> RatePolicy {
        RatePolicy::new(1, 1).expect("policy")
    }

    /// The four post-auth charges of one request, in the guard's input order (credential/shared, user, tenant,
    /// credential/operation), each tagged with the tier it must be locked in.
    fn charges(auth: &AuthorizationScope) -> Vec<(u8, RateCharge<'_>)> {
        let credential = || RateSubject::Credential {
            auth,
            credential_id: auth.principal().0,
        };
        let charge = |subject, operation, bucket_key| RateCharge {
            subject,
            operation,
            bucket_key,
            policy: policy(),
        };
        vec![
            (2, charge(credential(), SHARED_RATE_OPERATION, "credential")),
            (
                1,
                charge(RateSubject::User(auth), SHARED_RATE_OPERATION, "user"),
            ),
            (
                0,
                charge(RateSubject::Tenant(auth), SHARED_RATE_OPERATION, "tenant"),
            ),
            // "enumerate" sorts before "mcp": the tier, not the key, puts it last.
            (3, charge(credential(), "enumerate", "operation")),
        ]
    }

    fn permutations(n: usize) -> Vec<Vec<usize>> {
        if n == 0 {
            return vec![vec![]];
        }
        let mut out = Vec::new();
        for rest in permutations(n - 1) {
            for at in 0..=rest.len() {
                let mut p = rest.clone();
                p.insert(at, n - 1);
                out.push(p);
            }
        }
        out
    }

    /// ADR-0065 D-D: whatever the input order, the locks are taken tenant → user → credential/shared →
    /// credential/operation. Fault: the sort in `lock_order` removed ⇒ input order ⇒ red.
    #[test]
    fn lock_order_is_tier_order_for_every_input_permutation() {
        let auth = auth();
        let perms = permutations(4);
        assert_eq!(perms.len(), 24);
        for perm in perms {
            let mut all: Vec<Option<(u8, RateCharge<'_>)>> =
                charges(&auth).into_iter().map(Some).collect();
            let (tiers, input): (Vec<u8>, Vec<RateCharge<'_>>) = perm
                .iter()
                .map(|&i| all[i].take().expect("each charge once"))
                .unzip();
            let buckets: Vec<Bucket> = input
                .iter()
                .map(|c| resolve(c).expect("valid charge"))
                .collect();
            let locked: Vec<u8> = lock_order(&buckets).into_iter().map(|i| tiers[i]).collect();
            assert_eq!(locked, [0, 1, 2, 3], "input order {perm:?}");
            let tier_of: Vec<u8> = buckets.iter().map(|b| b.tier).collect();
            assert_eq!(
                tier_of, tiers,
                "resolve assigns the D-D tier, input order {perm:?}"
            );
        }
    }

    /// ADR-0065 D-E + ruling W-10: the IPv6 prefix comes from the key; IPv4-mapped addresses key as IPv4; loopback
    /// and link-local key as themselves. Fault: `bits` ignored (a fixed 64) ⇒ red.
    #[test]
    fn preauth_ipv6_prefix_bits_from_config() {
        let ip = |s: &str| s.parse::<IpAddr>().expect("ip literal");
        let (a, b) = (ip("2001:db8:1:200::1"), ip("2001:db8:1:2ff::1"));
        assert_eq!(preauth_ip_subject(a, 56), "2001:db8:1:200::/56");
        assert_eq!(
            preauth_ip_subject(a, 56),
            preauth_ip_subject(b, 56),
            "56 shares a /56"
        );
        let split = "2001:db8:1:200::/64";
        assert_eq!(preauth_ip_subject(a, 64), split);
        assert_ne!(
            preauth_ip_subject(a, 64),
            preauth_ip_subject(b, 64),
            "64 splits it"
        );
        assert_eq!(preauth_ip_subject(a, 128), "2001:db8:1:200::1/128");
        assert_eq!(
            preauth_ip_subject(ip("::ffff:203.0.113.9"), 56),
            "203.0.113.9"
        );
        assert_eq!(preauth_ip_subject(ip("203.0.113.9"), 56), "203.0.113.9");
        assert_eq!(preauth_ip_subject(ip("::1"), 56), "::1/128");
        assert_eq!(preauth_ip_subject(ip("fe80::1:2"), 56), "fe80::1:2/128");
        for bits in [0, 129] {
            let charge = RateCharge {
                subject: RateSubject::PreauthIp(a, bits),
                operation: SHARED_RATE_OPERATION,
                bucket_key: "preauth",
                policy: policy(),
            };
            assert_eq!(
                resolve(&charge).err(),
                Some(ErrorCode::InvalidInput),
                "bits {bits}"
            );
        }
    }
}
