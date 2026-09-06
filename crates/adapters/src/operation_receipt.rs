//! §34.0.1 local `remember.put`: one transaction for business, BMO, audit, and receipt.
//! A failed COMMIT response is an unknown outcome, never permission to repeat a write.

use std::time::Duration;

use humaux_domain::{
    audit::{AuditEvent, AuditEventId, McpAuditAction},
    error::ErrorCode,
    evidence::EvidenceOriginClass,
    identity::{AuthorizationScope, VisibilityClass, VisibilityDescriptor, can_read},
    ids::{UserId, WorkspaceId},
};
use sqlx::{Row, postgres::PgRow};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    postgres::RuntimeDbPool,
    quota_repo::{self, ReservationStatus, ReserveResult},
    remember::{self, RememberAccepted, RememberCommand, RememberError},
    request_guard_repo::{self, AuditTenant},
    retrieve::{TokenClaims, issue_consistency_token},
};

const OPERATION: &str = "remember.put";
type Txn<'c> = sqlx::Transaction<'c, sqlx::Postgres>;

/// Trusted application inputs. This is not a deserializable MCP request or a credential.
pub struct AtomicRememberRequest {
    pub request_id: Uuid,
    pub idempotency_key: String,
    pub request_fingerprint: String,
    pub workspace_id: Option<WorkspaceId>,
    pub reservation_ttl: Duration,
    pub replay_ttl: Duration,
    pub command: RememberCommand,
    pub finished_audit: AuditEvent,
}

pub struct AtomicRememberResult {
    pub accepted: RememberAccepted,
    pub replayed: bool,
}

fn db_error(error: sqlx::Error) -> ErrorCode {
    match error {
        sqlx::Error::RowNotFound => ErrorCode::NotFound,
        sqlx::Error::Database(ref database) => match database.code().as_deref() {
            Some("42501") => ErrorCode::Forbidden,
            Some("23503") => ErrorCode::TenantBoundary,
            Some("23505" | "40001" | "40P01" | "55P03") => ErrorCode::Conflict,
            Some("22023" | "22P02" | "22003") => ErrorCode::InvalidInput,
            _ => ErrorCode::Internal,
        },
        _ => ErrorCode::DependencyUnavailable,
    }
}

fn remember_error(error: RememberError) -> ErrorCode {
    match error {
        RememberError::ConsistencyTokenExpiryNotFuture => ErrorCode::Conflict,
        RememberError::BatchExhausted => ErrorCode::Conflict,
        // §6.1.3: an unresolvable subject declaration is INVALID_INPUT with nothing written.
        RememberError::Subject(code) | RememberError::Affect(code) => code,
        RememberError::Db(error) => db_error(error),
    }
}

fn field<T>(row: &PgRow, name: &str) -> Result<T, ErrorCode>
where
    T: for<'r> sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get(name).map_err(|_| ErrorCode::Internal)
}

fn valid_key(key: &str) -> bool {
    (1..=128).contains(&key.len())
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
}

fn visibility_is_bound_to_route(
    authorization_user_id: Option<Uuid>,
    workspace_id: Option<WorkspaceId>,
    visibility_class: &str,
    visibility_user_id: Option<Uuid>,
    visibility_workspace_id: Option<Uuid>,
) -> bool {
    match visibility_class {
        "USER_PRIVATE" => {
            visibility_user_id == authorization_user_id && visibility_workspace_id.is_none()
        }
        "WORKSPACE_SHARED" => {
            workspace_id.is_some()
                && visibility_user_id.is_none()
                && visibility_workspace_id == workspace_id.map(|workspace| workspace.0)
        }
        "TENANT_SHARED" => visibility_user_id.is_none() && visibility_workspace_id.is_none(),
        _ => false,
    }
}

fn validate(
    auth: &AuthorizationScope,
    request: &AtomicRememberRequest,
) -> Result<AuthorizationScope, ErrorCode> {
    let cmd = &request.command;
    if auth.tenant_id().0.is_nil() || auth.principal().0.is_nil() || auth.user_id().is_none() {
        return Err(ErrorCode::Unauthorized);
    }
    if !valid_key(&request.idempotency_key)
        || request.request_id.is_nil()
        || request.reservation_ttl.is_zero()
        || request.replay_ttl.is_zero()
        || request.request_fingerprint.len() != 64
        || !request
            .request_fingerprint
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || cmd.batch_id.is_some()
    {
        return Err(ErrorCode::InvalidInput);
    }
    if cmd.tenant_id != auth.tenant_id().0
        || cmd.authorization_user_id != auth.user_id().map(|id| id.0)
        || cmd.origin_principal_id != Some(auth.principal().0)
        || cmd.origin_class != EvidenceOriginClass::AuthenticatedAgent
    {
        return Err(ErrorCode::TenantBoundary);
    }
    let effective_auth = match request.workspace_id {
        Some(workspace) if cmd.scope_kind == "workspace" && cmd.scope_id == workspace.0 => {
            auth.narrow(workspace)?
        }
        None if cmd.scope_kind == "tenant" && cmd.scope_id == auth.tenant_id().0 => auth.clone(),
        _ => return Err(ErrorCode::TenantBoundary),
    };
    if !visibility_is_bound_to_route(
        effective_auth.user_id().map(|user| user.0),
        request.workspace_id,
        &cmd.visibility_class,
        cmd.visibility_user_id,
        cmd.visibility_workspace_id,
    ) {
        return Err(ErrorCode::TenantBoundary);
    }
    let event = &request.finished_audit;
    if event.tenant_id != effective_auth.tenant_id()
        || event.actor_id != effective_auth.principal().0.to_string()
        || event.request_id != request.request_id.to_string()
        || event.action != "MCP_REQUEST_FINISHED"
        || event.resource_id != OPERATION
        || event.result != "OK"
    {
        return Err(ErrorCode::InvalidInput);
    }
    Ok(effective_auth)
}

/// The same logical key must be supplied after an HTTP/COMMIT outcome is unknown.
/// Replays still need a fresh authorized scope from RequestGuard before entering here.
pub async fn remember_atomically(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    request: AtomicRememberRequest,
) -> Result<AtomicRememberResult, ErrorCode> {
    let auth = validate(auth, &request)?;
    let replay_duration =
        time::Duration::try_from(request.replay_ttl).map_err(|_| ErrorCode::InvalidInput)?;
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    let prior = lock_and_find_receipt(&mut txn, &auth, &request).await?;
    if let Some(prior) = prior {
        let accepted = replay(&mut txn, &auth, &request, &prior).await?;
        txn.commit()
            .await
            .map_err(|_| ErrorCode::DependencyUnavailable)?;
        return Ok(AtomicRememberResult {
            accepted,
            replayed: true,
        });
    }
    let reservation = match quota_repo::reserve_bmo_in_txn(
        &mut txn,
        &auth,
        request.request_id,
        OPERATION,
        &request.request_fingerprint,
        request.reservation_ttl,
    )
    .await?
    {
        ReserveResult::Created(reservation) => reservation,
        ReserveResult::Existing(_) => return Err(ErrorCode::Conflict),
    };
    let mut quota_audit = request.finished_audit.clone();
    quota_audit.event_id = AuditEventId::new();
    quota_audit.action = McpAuditAction::McpQuotaReserved.as_str().to_owned();
    request_guard_repo::audit_event_insert_in_txn(
        &mut txn,
        AuditTenant::Authenticated(&auth),
        &quota_audit,
    )
    .await?;

    let token_expires_at = request.command.consistency_token_expires_at;
    let pending = remember::remember_in_txn(&mut txn, request.command)
        .await
        .map_err(remember_error)?;
    if quota_repo::finish_reservation_in_txn(&mut txn, &auth, &reservation, true).await?
        != ReservationStatus::Consumed
    {
        return Err(ErrorCode::Conflict); // Drops the transaction, including Evidence/outbox.
    }
    quota_audit.event_id = AuditEventId::new();
    quota_audit.action = McpAuditAction::McpQuotaConsumed.as_str().to_owned();
    request_guard_repo::audit_event_insert_in_txn(
        &mut txn,
        AuditTenant::Authenticated(&auth),
        &quota_audit,
    )
    .await?;
    let audit_id = request_guard_repo::audit_event_insert_in_txn(
        &mut txn,
        AuditTenant::Authenticated(&auth),
        &request.finished_audit,
    )
    .await?;
    let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut *txn)
        .await
        .map_err(db_error)?;
    let replay_expires_at = now
        .checked_add(replay_duration)
        .ok_or(ErrorCode::InvalidInput)?;
    let stream = pending.stream_key();
    sqlx::query(
        "INSERT INTO control.operation_receipts \
         (tenant_id,principal_id,operation,idempotency_key,request_fingerprint,user_id,workspace_id, \
          request_id,reservation_id,evidence_id,scope_kind,scope_id,domain,projection_kind, \
          projection_version,stream_seq,commit_seq,audit_event_id,replay_expires_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)",
    )
    .bind(auth.tenant_id().0).bind(auth.principal().0).bind(OPERATION)
    .bind(&request.idempotency_key).bind(&request.request_fingerprint)
    .bind(auth.user_id().map(|id| id.0)).bind(request.workspace_id.map(|id| id.0))
    .bind(request.request_id).bind(reservation.id()).bind(pending.accepted().evidence_id)
    .bind(&stream.scope_kind).bind(stream.scope_id).bind(&stream.domain)
    .bind(&stream.projection_kind).bind(&stream.projection_version)
    .bind(pending.stream_seq()).bind(pending.commit_seq()).bind(audit_id.0).bind(replay_expires_at)
    .execute(&mut *txn).await.map_err(db_error)?;
    let finalized_at: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut *txn)
        .await
        .map_err(db_error)?;
    if finalized_at >= reservation.expires_at() || finalized_at >= token_expires_at {
        return Err(ErrorCode::Conflict);
    }
    // A COMMIT error cannot establish rollback. Only a subsequent receipt lookup
    // resolves durability; callers must not publish a terminal failure or refund.
    txn.commit()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    Ok(AtomicRememberResult {
        accepted: pending.into_accepted(),
        replayed: false,
    })
}

async fn lock_and_find_receipt(
    txn: &mut Txn<'_>,
    auth: &AuthorizationScope,
    request: &AtomicRememberRequest,
) -> Result<Option<PgRow>, ErrorCode> {
    sqlx::query(
        "SELECT set_config('humaux.tenant_id',$1,true), set_config('humaux.user_id',$2,true)",
    )
    .bind(auth.tenant_id().0.to_string())
    .bind(
        auth.user_id()
            .map(|id| id.0)
            .unwrap_or_else(Uuid::nil)
            .to_string(),
    )
    .execute(&mut **txn)
    .await
    .map_err(db_error)?;
    let locked: bool =
        sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))")
            .bind(format!(
                "operation-receipt:{}:{}:{OPERATION}:{}",
                auth.tenant_id().0,
                auth.principal().0,
                request.idempotency_key
            ))
            .fetch_one(&mut **txn)
            .await
            .map_err(db_error)?;
    if !locked {
        return Err(ErrorCode::Conflict);
    }
    sqlx::query(
        "SELECT *, clock_timestamp() AS observed_at FROM control.operation_receipts \
         WHERE tenant_id=$1 AND principal_id=$2 AND operation=$3 AND idempotency_key=$4",
    )
    .bind(auth.tenant_id().0)
    .bind(auth.principal().0)
    .bind(OPERATION)
    .bind(&request.idempotency_key)
    .fetch_optional(&mut **txn)
    .await
    .map_err(db_error)
}

async fn replay(
    txn: &mut Txn<'_>,
    auth: &AuthorizationScope,
    request: &AtomicRememberRequest,
    prior: &PgRow,
) -> Result<RememberAccepted, ErrorCode> {
    if field::<String>(prior, "request_fingerprint")? != request.request_fingerprint
        || field::<Option<Uuid>>(prior, "user_id")? != auth.user_id().map(|id| id.0)
        || field::<Option<Uuid>>(prior, "workspace_id")? != request.workspace_id.map(|id| id.0)
        || field::<String>(prior, "status")? != "COMMITTED"
        || field::<String>(prior, "scope_kind")? != request.command.scope_kind
        || field::<Uuid>(prior, "scope_id")? != request.command.scope_id
        || field::<String>(prior, "domain")? != request.command.domain
        || field::<String>(prior, "projection_kind")? != request.command.projection_kind
        || field::<OffsetDateTime>(prior, "replay_expires_at")?
            <= field::<OffsetDateTime>(prior, "observed_at")?
    {
        return Err(ErrorCode::Conflict);
    }
    let evidence_id = field::<Option<Uuid>>(prior, "evidence_id")?.ok_or(ErrorCode::NotFound)?;
    let evidence = sqlx::query(
        "SELECT e.tenant_id,e.visibility_class,e.visibility_user_id,e.visibility_workspace_id \
         FROM private.evidence_objects e \
         JOIN projection.stream_log s ON s.tenant_id=e.tenant_id \
         WHERE e.evidence_id=$1 AND e.tenant_id=$2 AND s.scope_kind=$3 AND s.scope_id=$4 \
           AND s.domain=$5 AND s.projection_kind=$6 AND s.projection_version=$7 \
           AND s.stream_seq=$8 AND s.commit_seq=$9 AND s.state<>'TOMBSTONED'",
    )
    .bind(evidence_id)
    .bind(auth.tenant_id().0)
    .bind(field::<String>(prior, "scope_kind")?)
    .bind(field::<Uuid>(prior, "scope_id")?)
    .bind(field::<String>(prior, "domain")?)
    .bind(field::<String>(prior, "projection_kind")?)
    .bind(field::<String>(prior, "projection_version")?)
    .bind(field::<i64>(prior, "stream_seq")?)
    .bind(field::<i64>(prior, "commit_seq")?)
    .fetch_optional(&mut **txn)
    .await
    .map_err(db_error)?
    .ok_or(ErrorCode::NotFound)?;
    let class = match field::<String>(&evidence, "visibility_class")?.as_str() {
        "USER_PRIVATE" => VisibilityClass::UserPrivate,
        "WORKSPACE_SHARED" => VisibilityClass::WorkspaceShared,
        "TENANT_SHARED" => VisibilityClass::TenantShared,
        _ => return Err(ErrorCode::Internal),
    };
    let visibility = VisibilityDescriptor {
        class,
        user_id: field::<Option<Uuid>>(&evidence, "visibility_user_id")?.map(UserId),
        workspace_id: field::<Option<Uuid>>(&evidence, "visibility_workspace_id")?.map(WorkspaceId),
    };
    if field::<Uuid>(&evidence, "tenant_id")? != auth.tenant_id().0 || !can_read(auth, &visibility)
    {
        return Err(ErrorCode::NotFound);
    }
    let issued_at = OffsetDateTime::now_utc();
    let expires_at = request
        .command
        .consistency_token_expires_at
        .min(field::<OffsetDateTime>(prior, "replay_expires_at")?);
    if expires_at <= issued_at {
        return Err(ErrorCode::Conflict);
    }
    let scope_kind: String = field(prior, "scope_kind")?;
    let scope_id: Uuid = field(prior, "scope_id")?;
    let consistency_token = issue_consistency_token(&TokenClaims {
        tenant_id: auth.tenant_id().0,
        workspace_id: if scope_kind == "workspace" {
            Some(scope_id)
        } else {
            None
        },
        scope_kind,
        scope_id,
        domain: field(prior, "domain")?,
        projection_kind: field(prior, "projection_kind")?,
        projection_version: field(prior, "projection_version")?,
        stream_seq: field(prior, "stream_seq")?,
        commit_seq: field(prior, "commit_seq")?,
        issued_at,
        expires_at,
    });
    Ok(RememberAccepted {
        evidence_id,
        processing_handle: evidence_id.to_string(),
        consistency_token,
        ticket_ordinal: None,
        batch_remaining: None,
        status: "accepted",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotency_keys_are_bounded_opaque_identifiers() {
        assert!(valid_key("legacy:01234567-89ab-cdef-0123-456789abcdef"));
        for key in ["", "with space", "\n", "租户", "a/b", &"a".repeat(129)] {
            assert!(!valid_key(key));
        }
    }

    #[test]
    fn private_and_workspace_visibility_must_follow_the_authorized_route() {
        let actor = Uuid::new_v4();
        let another_user = Uuid::new_v4();
        let workspace = WorkspaceId(Uuid::new_v4());
        let another_workspace = Uuid::new_v4();

        assert!(visibility_is_bound_to_route(
            Some(actor),
            Some(workspace),
            "USER_PRIVATE",
            Some(actor),
            None,
        ));
        assert!(!visibility_is_bound_to_route(
            Some(actor),
            Some(workspace),
            "USER_PRIVATE",
            Some(another_user),
            None,
        ));
        assert!(visibility_is_bound_to_route(
            Some(actor),
            Some(workspace),
            "WORKSPACE_SHARED",
            None,
            Some(workspace.0),
        ));
        assert!(!visibility_is_bound_to_route(
            Some(actor),
            Some(workspace),
            "WORKSPACE_SHARED",
            None,
            Some(another_workspace),
        ));
    }
}
