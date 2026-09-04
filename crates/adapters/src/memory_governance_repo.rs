//! `adapters::memory_governance_repo` — §36 `memory.supersede`, the first confirm-gated
//! governance write (ADR-0018, D-C).
//!
//! One `role_gateway` transaction, in this order, every step rolling the whole thing back:
//! reserve BMO -> consume the confirm token ([`crate::confirm_token_repo::consume_in_txn`],
//! same transaction by construction) -> visibility of both rows (`context_repo::readable_memory_ids`,
//! the workspace's own `can_read` judge) -> successor rule (`application::supersede::check_pair`)
//! -> `UPDATE private.memory_records SET status='superseded', superseded_by, superseded_at
//! WHERE status='active'` (G59-4 holds on the new row; 0 rows = Conflict). That WHERE clause
//! is the *only* judge of the target's status — nothing pre-reads it — so "already
//! superseded" (acceptance (f)) and the concurrent double-supersede (g) are both decided by
//! the predicate PostgreSQL re-evaluates under the row lock, never by a stale SELECT.
//! -> `MEMORY_LIFECYCLE` ticket on the memory's stream family (§7/§15: one stream_log seq +
//! one outbox row, so projection re-derives the point's visibility; see ADR-0018 for what
//! the worker does with it) -> quota CONSUMED -> audits -> COMMIT.
//!
//! No `control.operation_receipts` row: that table's trigger binds a receipt to a
//! `remember.put`-shaped Evidence/stream reference. Replaying a consumed token is `Conflict`
//! (acceptance (c)); the mutation itself is idempotent in effect.

use std::time::Duration;

use humaux_domain::{
    audit::{AuditEvent, AuditEventId, McpAuditAction},
    authority::{AuthorityStatus, MemoryId},
    confirm::{DestructiveOp, RISK_TAG_CONFIRMATION_MINTED},
    error::ErrorCode,
    identity::AuthorizationScope,
};
use humaux_projection::stream::StreamKey;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    confirm_token_repo::{self, ConfirmationClaim},
    context_repo,
    postgres::RuntimeDbPool,
    quota_repo::{self, ReservationStatus, ReserveResult},
    remember::{self, RememberError},
    request_guard_repo::{self, AuditTenant},
};

const OP: DestructiveOp = DestructiveOp::MemorySupersede;

/// Trusted application inputs (built by the gateway gate, never deserialized from MCP).
pub struct SupersedeRequest {
    pub request_id: Uuid,
    pub request_fingerprint: String,
    pub reservation_ttl: Duration,
    pub target: MemoryId,
    pub successor: MemoryId,
    /// Stream family the lifecycle ticket is issued on (the workspace's projection stream,
    /// same key `memory.get`/`recall` read their ledger from).
    pub stream: StreamKey,
    pub claim: ConfirmationClaim,
    pub finished_audit: AuditEvent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupersedeOutcome {
    pub superseded_at: OffsetDateTime,
    pub stream_seq: i64,
    pub commit_seq: i64,
}

fn db_error(error: sqlx::Error) -> ErrorCode {
    match error {
        sqlx::Error::RowNotFound => ErrorCode::NotFound,
        sqlx::Error::Database(ref db) => match db.code().as_deref() {
            Some("42501") => ErrorCode::Forbidden,
            Some("23503") => ErrorCode::TenantBoundary,
            Some("23505" | "40001" | "40P01" | "55P03" | "23514") => ErrorCode::Conflict,
            Some("22023" | "22P02" | "22003") => ErrorCode::InvalidInput,
            _ => ErrorCode::Internal,
        },
        _ => ErrorCode::DependencyUnavailable,
    }
}

fn remember_error(error: RememberError) -> ErrorCode {
    match error {
        RememberError::Db(error) => db_error(error),
        RememberError::ConsistencyTokenExpiryNotFuture | RememberError::BatchExhausted => {
            ErrorCode::Conflict
        }
    }
}

fn status(wire: &str) -> Result<AuthorityStatus, ErrorCode> {
    Ok(match wire {
        "active" => AuthorityStatus::Active,
        "superseded" => AuthorityStatus::Superseded,
        "revoked" => AuthorityStatus::Revoked,
        "expired" => AuthorityStatus::Expired,
        _ => return Err(ErrorCode::Internal),
    })
}

/// Pure input contract: the claim must be this operation on exactly this (target, successor)
/// pair, the stream family must be the caller's tenant, and the success audit must describe
/// exactly this operation as an executed write (never carrying the mint tag).
fn validate(auth: &AuthorizationScope, request: &SupersedeRequest) -> Result<(), ErrorCode> {
    if auth.tenant_id().0.is_nil() || auth.principal().0.is_nil() || auth.user_id().is_none() {
        return Err(ErrorCode::Unauthorized);
    }
    if request.request_id.is_nil()
        || request.reservation_ttl.is_zero()
        || request.request_fingerprint.len() != 64
        || !request
            .request_fingerprint
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || request.target == request.successor
    {
        return Err(ErrorCode::InvalidInput);
    }
    if request.claim.op != OP
        || request.claim.target_id != request.target.0
        || request.claim.successor_id != Some(request.successor.0)
    {
        return Err(ErrorCode::Conflict);
    }
    if request.stream.tenant_id != auth.tenant_id() {
        return Err(ErrorCode::TenantBoundary);
    }
    let event = &request.finished_audit;
    if event.tenant_id != auth.tenant_id()
        || event.actor_id != auth.principal().0.to_string()
        || event.request_id != request.request_id.to_string()
        || event.action != McpAuditAction::McpRequestFinished.as_str()
        || event.resource_id != OP.operation_key()
        || event.result != "OK"
        || event
            .risk_tags
            .iter()
            .any(|tag| tag == RISK_TAG_CONFIRMATION_MINTED)
    {
        return Err(ErrorCode::InvalidInput);
    }
    Ok(())
}

/// Visibility of both rows + current `status` of the successor, inside the gated
/// transaction. A row the caller cannot read (RLS, `can_read`, hidden Evidence) is
/// `NotFound`, never a hint. The target's status is not read here (see module doc).
async fn successor_status(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    auth: &AuthorizationScope,
    target: MemoryId,
    successor: MemoryId,
) -> Result<AuthorityStatus, ErrorCode> {
    let ids = [target.0, successor.0];
    let readable = context_repo::readable_memory_ids(txn, auth, &ids).await?;
    if !ids.iter().all(|id| readable.contains(id)) {
        return Err(ErrorCode::NotFound);
    }
    let wire: String = sqlx::query_scalar(
        "SELECT status FROM private.memory_records WHERE tenant_id = $1 AND memory_id = $2",
    )
    .bind(auth.tenant_id().0)
    .bind(successor.0)
    .fetch_optional(&mut **txn)
    .await
    .map_err(db_error)?
    .ok_or(ErrorCode::NotFound)?;
    status(&wire)
}

/// §7/§15 projection consequence: one `MEMORY_LIFECYCLE` ticket (stream_log ISSUED row +
/// outbox row bound to the target's PRIMARY Evidence) on the memory's stream family, via
/// the same §60 writers `remember` uses. Returns `(stream_seq, commit_seq)`.
async fn issue_lifecycle_ticket(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    auth: &AuthorizationScope,
    stream: &StreamKey,
    target: MemoryId,
) -> Result<(i64, i64), ErrorCode> {
    let evidence_id: Uuid = sqlx::query_scalar(
        "SELECT evidence_id FROM private.memory_evidence WHERE memory_id = $1 \
         ORDER BY (role = 'PRIMARY') DESC, ordinal ASC LIMIT 1",
    )
    .bind(target.0)
    .fetch_optional(&mut **txn)
    .await
    .map_err(db_error)?
    .ok_or(ErrorCode::NotFound)?;
    let commit_seq = remember::next_commit_seq(txn)
        .await
        .map_err(remember_error)?;
    let stream_seq = remember::issue_stream_log_row(txn, stream, commit_seq)
        .await
        .map_err(remember_error)?;
    remember::insert_outbox(
        txn,
        auth.tenant_id().0,
        commit_seq,
        stream_seq,
        remember::MEMORY_LIFECYCLE,
        evidence_id,
    )
    .await
    .map_err(remember_error)?;
    Ok((stream_seq, commit_seq))
}

/// D-C, atomically. See the module doc for the step order.
pub async fn supersede_atomically(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    request: SupersedeRequest,
) -> Result<SupersedeOutcome, ErrorCode> {
    validate(auth, &request)?;
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    confirm_token_repo::set_authorization_local(&mut txn, auth).await?;

    let reservation = match quota_repo::reserve_bmo_in_txn(
        &mut txn,
        auth,
        request.request_id,
        OP.operation_key(),
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
        AuditTenant::Authenticated(auth),
        &quota_audit,
    )
    .await?;

    // Token first: a replayed/expired/misbound token must never reach the row lookup.
    confirm_token_repo::consume_in_txn(&mut txn, auth, &request.claim).await?;

    let successor_status =
        successor_status(&mut txn, auth, request.target, request.successor).await?;
    humaux_application::supersede::check_pair(request.target, request.successor, successor_status)?;

    let superseded_at: Option<OffsetDateTime> = sqlx::query_scalar(
        "UPDATE private.memory_records \
            SET status = 'superseded', superseded_by = $2, superseded_at = clock_timestamp() \
          WHERE tenant_id = $3 AND memory_id = $1 AND status = 'active' \
          RETURNING superseded_at",
    )
    .bind(request.target.0)
    .bind(request.successor.0)
    .bind(auth.tenant_id().0)
    .fetch_optional(&mut *txn)
    .await
    .map_err(db_error)?;
    let superseded_at = superseded_at.ok_or(ErrorCode::Conflict)?;

    let (stream_seq, commit_seq) =
        issue_lifecycle_ticket(&mut txn, auth, &request.stream, request.target).await?;

    if quota_repo::finish_reservation_in_txn(&mut txn, auth, &reservation, true).await?
        != ReservationStatus::Consumed
    {
        return Err(ErrorCode::Conflict);
    }
    quota_audit.event_id = AuditEventId::new();
    quota_audit.action = McpAuditAction::McpQuotaConsumed.as_str().to_owned();
    request_guard_repo::audit_event_insert_in_txn(
        &mut txn,
        AuditTenant::Authenticated(auth),
        &quota_audit,
    )
    .await?;
    request_guard_repo::audit_event_insert_in_txn(
        &mut txn,
        AuditTenant::Authenticated(auth),
        &request.finished_audit,
    )
    .await?;
    let finalized_at: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut *txn)
        .await
        .map_err(db_error)?;
    if finalized_at >= reservation.expires_at() {
        return Err(ErrorCode::Conflict);
    }
    // A COMMIT error cannot establish rollback (§34.0.1); the caller reports retryable.
    txn.commit()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    Ok(SupersedeOutcome {
        superseded_at,
        stream_seq,
        commit_seq,
    })
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use humaux_domain::{
        audit::AuditMetadata,
        identity::{BoundedSet, PrincipalId},
        ids::{TenantId, UserId, WorkspaceId},
    };

    use super::*;

    fn scope() -> AuthorizationScope {
        AuthorizationScope::new(
            TenantId(Uuid::now_v7()),
            PrincipalId(Uuid::now_v7()),
            Some(UserId(Uuid::now_v7())),
            BoundedSet::new([WorkspaceId(Uuid::now_v7())]).unwrap(),
        )
    }

    fn request(auth: &AuthorizationScope) -> SupersedeRequest {
        let request_id = Uuid::now_v7();
        let target = MemoryId::new();
        let successor = MemoryId::new();
        SupersedeRequest {
            request_id,
            request_fingerprint: "a".repeat(64),
            reservation_ttl: Duration::from_secs(30),
            target,
            successor,
            stream: StreamKey::new(
                auth.tenant_id(),
                "workspace",
                Uuid::now_v7(),
                "knowledge",
                "ingest",
                "v1",
            ),
            claim: ConfirmationClaim {
                op: OP,
                target_id: target.0,
                successor_id: Some(successor.0),
                nonce_sha256: [9u8; 32],
            },
            finished_audit: AuditEvent {
                event_id: AuditEventId::new(),
                ts: SystemTime::now(),
                tenant_id: auth.tenant_id(),
                actor_type: "SERVICE_CREDENTIAL".into(),
                actor_id: auth.principal().0.to_string(),
                action: McpAuditAction::McpRequestFinished.as_str().into(),
                resource_type: "MCP_OPERATION".into(),
                resource_id: OP.operation_key().into(),
                result: "OK".into(),
                request_id: request_id.to_string(),
                trace_id: String::new(),
                client_ip: "127.0.0.1".into(),
                user_agent_hash: String::new(),
                risk_tags: vec![],
                before_fingerprint: None,
                after_fingerprint: None,
                metadata: AuditMetadata::new(),
            },
        }
    }

    #[test]
    fn supersede_contract_binds_claim_target_stream_and_audit() {
        let auth = scope();
        assert_eq!(validate(&auth, &request(&auth)), Ok(()));

        let mut misbound = request(&auth);
        misbound.claim.target_id = Uuid::now_v7();
        assert_eq!(validate(&auth, &misbound), Err(ErrorCode::Conflict));

        // A token confirmed for (C -> B) must never execute (C -> E).
        let mut retargeted = request(&auth);
        retargeted.claim.successor_id = Some(Uuid::now_v7());
        assert_eq!(validate(&auth, &retargeted), Err(ErrorCode::Conflict));
        let mut unbound = request(&auth);
        unbound.claim.successor_id = None;
        assert_eq!(validate(&auth, &unbound), Err(ErrorCode::Conflict));

        let mut mint_tagged = request(&auth);
        mint_tagged
            .finished_audit
            .risk_tags
            .push(RISK_TAG_CONFIRMATION_MINTED.to_owned());
        assert_eq!(
            validate(&auth, &mint_tagged),
            Err(ErrorCode::InvalidInput),
            "an executed write is never audited as a mint"
        );

        let mut self_supersede = request(&auth);
        self_supersede.successor = self_supersede.target;
        assert_eq!(
            validate(&auth, &self_supersede),
            Err(ErrorCode::InvalidInput)
        );

        let mut foreign_stream = request(&auth);
        foreign_stream.stream = StreamKey::new(
            TenantId(Uuid::now_v7()),
            "workspace",
            Uuid::now_v7(),
            "knowledge",
            "ingest",
            "v1",
        );
        assert_eq!(
            validate(&auth, &foreign_stream),
            Err(ErrorCode::TenantBoundary)
        );

        let mut wrong_audit = request(&auth);
        wrong_audit.finished_audit.resource_id = "memory.get".into();
        assert_eq!(validate(&auth, &wrong_audit), Err(ErrorCode::InvalidInput));

        let headless = AuthorizationScope::new(
            auth.tenant_id(),
            auth.principal(),
            None,
            auth.allowed_workspace_ids().clone(),
        );
        assert_eq!(
            validate(&headless, &request(&auth)),
            Err(ErrorCode::Unauthorized)
        );
    }

    #[test]
    fn status_wire_forms_match_the_g59_check_list() {
        for (wire, expected) in [
            ("active", AuthorityStatus::Active),
            ("superseded", AuthorityStatus::Superseded),
            ("revoked", AuthorityStatus::Revoked),
            ("expired", AuthorityStatus::Expired),
        ] {
            assert_eq!(status(wire), Ok(expected));
        }
        assert_eq!(status("ACTIVE"), Err(ErrorCode::Internal));
    }
}
