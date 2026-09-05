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
//!
//! ADR-0020 adds the lifecycle-event side: `supersede_atomically` now ALSO appends a
//! `SUPERSEDE` row to `ops.memory_lifecycle_events` (with its `undo_deadline`) and stamps the
//! memory's `lifecycle_head_event_id`, and `restore_atomically` undoes a SUPERSEDE within the
//! window — the inverse authority flip (`superseded -> active`, clearing `superseded_by`)
//! plus a `RESTORE` event that names the SUPERSEDE it reverses, a fresh MEMORY_LIFECYCLE
//! ticket (new stream seq; the old TOMBSTONED/consumed row is never revived), and a new
//! consistency_token bound to that seq. Restore's idempotency is the lifecycle table's own
//! `UNIQUE(tenant, actor, idempotency_key)`: a replay returns the original event instead of
//! `Conflict` (unlike supersede), so a lost-response retry is safe.

use std::time::Duration;

use humaux_domain::{
    audit::{AuditEvent, AuditEventId, McpAuditAction},
    authority::{AuthorityStatus, MemoryId},
    confirm::{DestructiveOp, RISK_TAG_CONFIRMATION_MINTED},
    error::{ConflictReason, ErrorCode},
    evidence::{EvidenceOriginClass, EvidencePayloadSha256},
    identity::AuthorizationScope,
    ids::{Scope, WorkspaceId},
    lifecycle::{LifecycleOp, LifecycleReason, RestoreTargetState, restore_allowed},
    memory::MemoryType,
    subject::SubjectDeclaration,
};
use humaux_projection::stream::StreamKey;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    confirm_token_repo::{self, ConfirmationClaim},
    context_repo,
    distill_repo::{self, LoadedEvidence, NewMemory},
    postgres::RuntimeDbPool,
    quota_repo::{self, ReservationStatus, ReserveResult},
    remember::{self, RememberCommand, RememberError},
    request_guard_repo::{self, AuditTenant},
    retrieve::{self, TokenClaims},
};

const OP: DestructiveOp = DestructiveOp::MemorySupersede;
const RESTORE_OP: DestructiveOp = DestructiveOp::MemoryRestore;
const CORRECT_OP: DestructiveOp = DestructiveOp::MemoryCorrect;

/// The one lifecycle-event writer, wrapping owner `ops.append_memory_lifecycle` (0149). The
/// idempotency key is the confirm token's `sha256(nonce)` in hex: a client that retries the
/// same confirmed call presents the same token, so the same event is returned instead of a
/// second append. `undo_window_secs = Some(n)` stamps `undo_deadline = clock_timestamp() +
/// n` (the transition is undoable for that window, §78.1); `None` leaves it NULL (RESTORE and
/// other terminal ops). Returns the new `event_id`, or `Conflict` when a concurrent racer
/// inserted the same (tenant, actor, key) first.
#[allow(clippy::too_many_arguments)] // ADR-0020: one INSERT's worth of explicit lifecycle facts; a struct would hide the binding the reviewer must read.
async fn append_lifecycle_event(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    auth: &AuthorizationScope,
    op: LifecycleOp,
    reason: Option<LifecycleReason>,
    memory_id: Uuid,
    replacement: Option<Uuid>,
    // §Q4/ADR-0025: the DirectUserInput Evidence a `memory.correct` SUPERSEDE is grounded on;
    // `None` for every non-correction transition (plain supersede/restore/archive).
    correction_evidence: Option<Uuid>,
    undoes_event: Option<Uuid>,
    undo_window_secs: Option<f64>,
    idempotency_key: &str,
    request_fingerprint: &str,
    stream_seq: i64,
    commit_seq: i64,
) -> Result<Uuid, ErrorCode> {
    let event_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT ops.append_memory_lifecycle($1, 'MEMORY', $2, $3, $4, $5, $6, $7, $8, \
            CASE WHEN $9::float8 IS NULL THEN NULL \
                 ELSE clock_timestamp() + make_interval(secs => $9) END, \
            $10, $11, $12, $13)",
    )
    .bind(auth.tenant_id().0)
    .bind(memory_id)
    .bind(op.as_db_str())
    .bind(reason.map(LifecycleReason::as_db_str))
    .bind(auth.principal().0)
    .bind(replacement)
    .bind(correction_evidence)
    .bind(undoes_event)
    .bind(undo_window_secs)
    .bind(idempotency_key)
    .bind(request_fingerprint)
    .bind(stream_seq)
    .bind(commit_seq)
    .fetch_one(&mut **txn)
    .await
    .map_err(db_error)?;
    event_id.ok_or(ErrorCode::Conflict)
}

/// The idempotency key for a confirmed lifecycle write: the token's digest, in hex. Stable
/// across a client retry of the same confirmed call, unique per issued token.
fn lifecycle_idempotency_key(claim: &ConfirmationClaim) -> String {
    hex::encode(claim.nonce_sha256)
}

/// §15.5 consistency_token from the workspace stream key + the new lifecycle stream seq.
fn build_consistency_token(
    stream: &StreamKey,
    stream_seq: i64,
    commit_seq: i64,
    ttl: Duration,
) -> Result<String, ErrorCode> {
    let issued_at = OffsetDateTime::now_utc();
    let ttl = time::Duration::try_from(ttl).map_err(|_| ErrorCode::InvalidInput)?;
    let expires_at = issued_at.checked_add(ttl).ok_or(ErrorCode::InvalidInput)?;
    Ok(retrieve::issue_consistency_token(&TokenClaims {
        tenant_id: stream.tenant_id.0,
        workspace_id: (stream.scope_kind == "workspace").then_some(stream.scope_id),
        scope_kind: stream.scope_kind.clone(),
        scope_id: stream.scope_id,
        domain: stream.domain.clone(),
        projection_kind: stream.projection_kind.clone(),
        projection_version: stream.projection_version.clone(),
        stream_seq,
        commit_seq,
        issued_at,
        expires_at,
    }))
}

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
    /// §78.1 undo window: how long the resulting SUPERSEDE stays restorable. Stamped onto the
    /// lifecycle event's `undo_deadline` at write time (ADR-0020 D-A/D-D).
    pub undo_window: Duration,
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
        RememberError::Subject(code) => code,
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

    // The MEMORY_LIFECYCLE ticket and the append run before the arbiter UPDATE; a 0-row
    // UPDATE (already superseded / lost the concurrent race) rolls both back with the txn.
    let (stream_seq, commit_seq) =
        issue_lifecycle_ticket(&mut txn, auth, &request.stream, request.target).await?;
    let event_id = append_lifecycle_event(
        &mut txn,
        auth,
        LifecycleOp::Supersede,
        Some(LifecycleReason::ExplicitSupersede),
        request.target.0,
        Some(request.successor.0),
        None, // correction_evidence: a plain supersede is not a correction
        None, // undoes_event
        Some(request.undo_window.as_secs_f64()),
        &lifecycle_idempotency_key(&request.claim),
        &request.request_fingerprint,
        stream_seq,
        commit_seq,
    )
    .await?;

    let superseded_at: Option<OffsetDateTime> = sqlx::query_scalar(
        "UPDATE private.memory_records \
            SET status = 'superseded', superseded_by = $2, superseded_at = clock_timestamp(), \
                lifecycle_head_event_id = $4 \
          WHERE tenant_id = $3 AND memory_id = $1 AND status = 'active' \
          RETURNING superseded_at",
    )
    .bind(request.target.0)
    .bind(request.successor.0)
    .bind(auth.tenant_id().0)
    .bind(event_id)
    .fetch_optional(&mut *txn)
    .await
    .map_err(db_error)?;
    let superseded_at = superseded_at.ok_or(ErrorCode::Conflict)?;

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

// ===========================================================================================
// memory.restore — undo a SUPERSEDE within the window (ADR-0020 D-C).
// ===========================================================================================

/// Trusted application inputs for `memory.restore` (built by the gateway gate).
pub struct RestoreRequest {
    pub request_id: Uuid,
    pub request_fingerprint: String,
    pub reservation_ttl: Duration,
    pub target: MemoryId,
    pub stream: StreamKey,
    pub claim: ConfirmationClaim,
    pub finished_audit: AuditEvent,
    /// §15.5 lifetime of the returned consistency_token (the workspace remember policy's TTL).
    pub consistency_token_ttl: Duration,
}

/// A successful restore: the memory is `active` again, on a new stream seq, with a new token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreDone {
    pub restored_at: OffsetDateTime,
    pub stream_seq: i64,
    pub commit_seq: i64,
    pub consistency_token: String,
}

/// The outcome of a restore attempt. `Refused` is a business conflict surfaced as
/// `structuredContent {code:"CONFLICT", reason:<u16>}` (ADR-0020 D-B), not an `ErrorCode`
/// (§52.1 keeps 18): the transaction rolled back, nothing mutated, the token stays unconsumed.
/// Genuine infra/visibility failures still return `Err(ErrorCode)` (NotFound, Conflict, ...).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreResult {
    Restored(RestoreDone),
    Refused(ConflictReason),
}

/// Pure input contract, mirroring [`validate`]: the claim is a `memory.restore` on exactly
/// this target with no successor, the stream is the caller's tenant, and the audit describes
/// exactly this executed operation (never carrying the mint tag).
fn validate_restore(auth: &AuthorizationScope, request: &RestoreRequest) -> Result<(), ErrorCode> {
    if auth.tenant_id().0.is_nil() || auth.principal().0.is_nil() || auth.user_id().is_none() {
        return Err(ErrorCode::Unauthorized);
    }
    if request.request_id.is_nil()
        || request.reservation_ttl.is_zero()
        || request.consistency_token_ttl.is_zero()
        || request.request_fingerprint.len() != 64
        || !request
            .request_fingerprint
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ErrorCode::InvalidInput);
    }
    if request.claim.op != RESTORE_OP
        || request.claim.target_id != request.target.0
        || request.claim.successor_id.is_some()
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
        || event.resource_id != RESTORE_OP.operation_key()
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

/// The memory's head-transition facts, read inside the gated transaction.
struct HeadState {
    memory_status: AuthorityStatus,
    head_event_id: Option<Uuid>,
    head_op: Option<LifecycleOp>,
    /// The head event's reason. A correction is a `SUPERSEDE` whose reason is
    /// `USER_CORRECTION`; undoing one must also deactivate the replacement `M2`.
    head_reason: Option<LifecycleReason>,
    /// The SUPERSEDE successor (`M2`), if the head is a SUPERSEDE.
    successor_id: Option<Uuid>,
    successor_status: Option<AuthorityStatus>,
    window_expired: bool,
}

/// Visibility of the target + its head lifecycle event + (for a SUPERSEDE head) the
/// successor's current status, all under the caller's RLS. A row the caller cannot read is
/// `NotFound`, never a hint.
async fn head_state(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    auth: &AuthorizationScope,
    target: MemoryId,
) -> Result<HeadState, ErrorCode> {
    let readable = context_repo::readable_memory_ids(txn, auth, &[target.0]).await?;
    if !readable.contains(&target.0) {
        return Err(ErrorCode::NotFound);
    }
    let row = sqlx::query(
        "SELECT m.status AS memory_status, m.lifecycle_head_event_id, e.op AS head_op, \
                e.reason_code AS head_reason, e.replacement_memory_id, \
                (e.undo_deadline IS NULL OR e.undo_deadline <= clock_timestamp()) AS window_expired \
         FROM private.memory_records m \
         LEFT JOIN ops.memory_lifecycle_events e ON e.event_id = m.lifecycle_head_event_id \
         WHERE m.tenant_id = $1 AND m.memory_id = $2",
    )
    .bind(auth.tenant_id().0)
    .bind(target.0)
    .fetch_optional(&mut **txn)
    .await
    .map_err(db_error)?
    .ok_or(ErrorCode::NotFound)?;
    use sqlx::Row;
    let memory_status = status(
        &row.try_get::<String, _>("memory_status")
            .map_err(|_| ErrorCode::Internal)?,
    )?;
    let head_event_id: Option<Uuid> = row
        .try_get("lifecycle_head_event_id")
        .map_err(|_| ErrorCode::Internal)?;
    let head_op = row
        .try_get::<Option<String>, _>("head_op")
        .map_err(|_| ErrorCode::Internal)?
        .map(|op| LifecycleOp::parse_db(&op).ok_or(ErrorCode::Internal))
        .transpose()?;
    let head_reason = row
        .try_get::<Option<String>, _>("head_reason")
        .map_err(|_| ErrorCode::Internal)?
        .map(|reason| LifecycleReason::parse_db(&reason).ok_or(ErrorCode::Internal))
        .transpose()?;
    let replacement: Option<Uuid> = row
        .try_get("replacement_memory_id")
        .map_err(|_| ErrorCode::Internal)?;
    let window_expired: bool = row
        .try_get("window_expired")
        .map_err(|_| ErrorCode::Internal)?;

    let successor_status = match (head_op, replacement) {
        (Some(LifecycleOp::Supersede), Some(successor)) => sqlx::query_scalar::<_, String>(
            "SELECT status FROM private.memory_records WHERE tenant_id = $1 AND memory_id = $2",
        )
        .bind(auth.tenant_id().0)
        .bind(successor)
        .fetch_optional(&mut **txn)
        .await
        .map_err(db_error)?
        .map(|wire| status(&wire))
        .transpose()?,
        _ => None,
    };

    Ok(HeadState {
        memory_status,
        head_event_id,
        head_op,
        head_reason,
        successor_id: replacement,
        successor_status,
        window_expired,
    })
}

/// D-C, atomically. Idempotency pre-check (replay before consume) -> consume token ->
/// read head + `restore_allowed` (refusal rolls back) -> reserve BMO -> new ticket -> append
/// RESTORE event -> `UPDATE ... WHERE status='superseded'` (the sole arbiter) -> settle.
// ADR-0020 D-C: one gated transaction whose step ORDER is the contract (idempotency before
// consume, arbiter UPDATE last). Splitting it into helpers would scatter that ordering across
// functions and hide the rollback boundary the reviewer must read as one sequence.
#[allow(clippy::too_many_lines)]
pub async fn restore_atomically(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    request: RestoreRequest,
) -> Result<RestoreResult, ErrorCode> {
    validate_restore(auth, &request)?;
    let idempotency_key = lifecycle_idempotency_key(&request.claim);
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    confirm_token_repo::set_authorization_local(&mut txn, auth).await?;

    // Idempotency: a replayed confirmed call returns the original success (D-E), never a
    // second BMO/consume/mutation. A different request under the same key is ALREADY_IN_STATE.
    use sqlx::Row;
    if let Some(row) = sqlx::query(
        "SELECT op, request_fingerprint, stream_seq, commit_seq, created_at \
         FROM ops.memory_lifecycle_events \
         WHERE tenant_id = $1 AND actor_principal_id = $2 AND idempotency_key = $3",
    )
    .bind(auth.tenant_id().0)
    .bind(auth.principal().0)
    .bind(&idempotency_key)
    .fetch_optional(&mut *txn)
    .await
    .map_err(db_error)?
    {
        let op: String = row.try_get("op").map_err(|_| ErrorCode::Internal)?;
        let fingerprint: String = row
            .try_get("request_fingerprint")
            .map_err(|_| ErrorCode::Internal)?;
        if op != LifecycleOp::Restore.as_db_str() || fingerprint != request.request_fingerprint {
            return Ok(RestoreResult::Refused(ConflictReason::ALREADY_IN_STATE));
        }
        let (Some(stream_seq), Some(commit_seq)) = (
            row.try_get::<Option<i64>, _>("stream_seq")
                .map_err(|_| ErrorCode::Internal)?,
            row.try_get::<Option<i64>, _>("commit_seq")
                .map_err(|_| ErrorCode::Internal)?,
        ) else {
            return Err(ErrorCode::Internal);
        };
        let restored_at: OffsetDateTime =
            row.try_get("created_at").map_err(|_| ErrorCode::Internal)?;
        let consistency_token = build_consistency_token(
            &request.stream,
            stream_seq,
            commit_seq,
            request.consistency_token_ttl,
        )?;
        return Ok(RestoreResult::Restored(RestoreDone {
            restored_at,
            stream_seq,
            commit_seq,
            consistency_token,
        }));
    }

    // Token first (same as supersede): a replayed/expired/misbound token is Conflict.
    confirm_token_repo::consume_in_txn(&mut txn, auth, &request.claim).await?;

    let head = head_state(&mut txn, auth, request.target).await?;
    if let Err(reason) = restore_allowed(RestoreTargetState {
        head_op: head.head_op,
        memory_status: head.memory_status,
        successor_status: head.successor_status,
        window_expired: head.window_expired,
    }) {
        // Refusal: return the reason and let the transaction roll back (token consume undone).
        return Ok(RestoreResult::Refused(reason));
    }
    let undoes_event = head.head_event_id.ok_or(ErrorCode::Internal)?;

    let reservation = match quota_repo::reserve_bmo_in_txn(
        &mut txn,
        auth,
        request.request_id,
        RESTORE_OP.operation_key(),
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

    // New ticket (new stream seq) + RESTORE event before the arbiter UPDATE; a 0-row UPDATE
    // rolls both back.
    let (stream_seq, commit_seq) =
        issue_lifecycle_ticket(&mut txn, auth, &request.stream, request.target).await?;
    let event_id = append_lifecycle_event(
        &mut txn,
        auth,
        LifecycleOp::Restore,
        None,
        request.target.0,
        None, // replacement
        None, // correction_evidence
        Some(undoes_event),
        None,
        &idempotency_key,
        &request.request_fingerprint,
        stream_seq,
        commit_seq,
    )
    .await?;

    // Sole arbiter (mirrors supersede's WHERE status='active'): PostgreSQL re-evaluates
    // status='superseded' under the row lock, so the concurrent restore/supersede race and
    // "already active" both resolve here, never on a stale pre-read. G59-4 stays true.
    // Only columns role_gateway holds a grant for (§6.2.2): status/superseded_by/
    // superseded_at/lifecycle_head_event_id — never updated_at. Return clock_timestamp()
    // as the restore instant rather than a column the gateway cannot write.
    let restored_at: Option<OffsetDateTime> = sqlx::query_scalar(
        "UPDATE private.memory_records \
            SET status = 'active', superseded_by = NULL, superseded_at = NULL, \
                lifecycle_head_event_id = $3 \
          WHERE tenant_id = $2 AND memory_id = $1 AND status = 'superseded' \
          RETURNING clock_timestamp()",
    )
    .bind(request.target.0)
    .bind(auth.tenant_id().0)
    .bind(event_id)
    .fetch_optional(&mut *txn)
    .await
    .map_err(db_error)?;
    let restored_at = restored_at.ok_or(ErrorCode::Conflict)?;

    // Undoing a correction is not complete until the correction-minted replacement M2 is also
    // deactivated: M2 has no independent existence (it was born only to carry the corrected
    // body), so leaving it active would surface BOTH the restored original and the corrected
    // text to recall/get/enumerate. Symmetric reversal — the correction made M1 superseded_by
    // M2, the undo makes M2 superseded_by M1 (G59-4's biconditional holds either way). Guarded
    // WHERE status='active' under the row lock: 0 rows means the successor advanced concurrently
    // (restore_allowed pre-checked it Active), so the whole undo rolls back as a Conflict rather
    // than half-undoing. Plain (ExplicitSupersede) supersedes are untouched — M2 is then an
    // independent user memory that must stay active.
    if head.head_op == Some(LifecycleOp::Supersede)
        && head.head_reason == Some(LifecycleReason::UserCorrection)
    {
        let successor = head.successor_id.ok_or(ErrorCode::Internal)?;
        let deactivated: Option<Uuid> = sqlx::query_scalar(
            "UPDATE private.memory_records \
                SET status = 'superseded', superseded_by = $2, superseded_at = clock_timestamp() \
              WHERE tenant_id = $3 AND memory_id = $1 AND status = 'active' \
              RETURNING memory_id",
        )
        .bind(successor)
        .bind(request.target.0)
        .bind(auth.tenant_id().0)
        .fetch_optional(&mut *txn)
        .await
        .map_err(db_error)?;
        deactivated.ok_or(ErrorCode::Conflict)?;
    }

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
    let consistency_token = build_consistency_token(
        &request.stream,
        stream_seq,
        commit_seq,
        request.consistency_token_ttl,
    )?;
    txn.commit()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    Ok(RestoreResult::Restored(RestoreDone {
        restored_at,
        stream_seq,
        commit_seq,
        consistency_token,
    }))
}

// ===========================================================================================
// memory.archive / memory.unarchive — flip private.memory_records.archived_at (ADR-0024 Q3).
// ===========================================================================================

/// Trusted application inputs for `memory.archive` / `memory.unarchive` (built by the gateway
/// gate). `op` is `MemoryArchive` or `MemoryUnarchive`; the claim carries the same op.
pub struct ArchiveRequest {
    pub request_id: Uuid,
    pub request_fingerprint: String,
    pub reservation_ttl: Duration,
    pub target: MemoryId,
    pub stream: StreamKey,
    pub claim: ConfirmationClaim,
    pub finished_audit: AuditEvent,
    /// `MemoryArchive` sets `archived_at`; `MemoryUnarchive` clears it.
    pub op: DestructiveOp,
}

/// A successful archive/unarchive: the flag flipped, on a new MEMORY_LIFECYCLE stream seq.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveDone {
    /// The instant the flag flipped (`archived_at` for archive; the clear instant for unarchive).
    pub changed_at: OffsetDateTime,
    pub stream_seq: i64,
    pub commit_seq: i64,
}

/// The outcome of an archive/unarchive attempt. `Refused(ALREADY_IN_STATE)` is a success-shaped
/// business conflict (D-B): archive of an archived Memory / unarchive of a live one — the
/// transaction rolled back, nothing mutated, the token stays unconsumed. Infra/visibility
/// failures still return `Err(ErrorCode)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveResult {
    Done(ArchiveDone),
    Refused(ConflictReason),
}

/// Pure input contract, mirroring [`validate_restore`]: the claim is `op` on exactly this
/// target with no successor, the stream is the caller's tenant, and the audit describes exactly
/// this executed operation (never carrying the mint tag).
fn validate_archive(auth: &AuthorizationScope, request: &ArchiveRequest) -> Result<(), ErrorCode> {
    if !matches!(
        request.op,
        DestructiveOp::MemoryArchive | DestructiveOp::MemoryUnarchive
    ) {
        return Err(ErrorCode::InvalidInput);
    }
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
    {
        return Err(ErrorCode::InvalidInput);
    }
    if request.claim.op != request.op
        || request.claim.target_id != request.target.0
        || request.claim.successor_id.is_some()
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
        || event.resource_id != request.op.operation_key()
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

/// Visibility of the target + its current `archived_at` and its NEWEST `ARCHIVE` lifecycle
/// event, under the caller's RLS. A row the caller cannot read is `NotFound`, never a hint.
///
/// The ARCHIVE event (not `lifecycle_head_event_id`) is what an unarchive RESTORE undoes: a
/// supersede between the archive and the unarchive moves the head to that SUPERSEDE, so using
/// the head would point the RESTORE's `undoes_event_id` at the wrong event.
async fn archive_state(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    auth: &AuthorizationScope,
    target: MemoryId,
) -> Result<(bool, Option<Uuid>), ErrorCode> {
    let readable = context_repo::readable_memory_ids(txn, auth, &[target.0]).await?;
    if !readable.contains(&target.0) {
        return Err(ErrorCode::NotFound);
    }
    use sqlx::Row;
    let row = sqlx::query(
        "SELECT (archived_at IS NOT NULL) AS archived, \
                (SELECT event_id FROM ops.memory_lifecycle_events \
                  WHERE tenant_id = $1 AND memory_id = $2 AND op = 'ARCHIVE' \
                  ORDER BY event_seq DESC LIMIT 1) AS archive_event \
         FROM private.memory_records WHERE tenant_id = $1 AND memory_id = $2",
    )
    .bind(auth.tenant_id().0)
    .bind(target.0)
    .fetch_optional(&mut **txn)
    .await
    .map_err(db_error)?
    .ok_or(ErrorCode::NotFound)?;
    Ok((
        row.try_get("archived").map_err(|_| ErrorCode::Internal)?,
        row.try_get("archive_event")
            .map_err(|_| ErrorCode::Internal)?,
    ))
}

/// D-B, atomically. One gated transaction shared by both directions (the `op` and the
/// `archived_at IS [NOT] NULL` arbiter predicate are the only differences — same one-arm reuse
/// as pin/unpin). Step order: idempotency pre-check (replay before consume), then consume
/// token, then visibility + current archived state, then `archive_allowed` (ALREADY_IN_STATE
/// rolls back), then reserve BMO, then a new MEMORY_LIFECYCLE ticket, then append ARCHIVE
/// (reason USER_ARCHIVE, no deadline) or RESTORE (undoes the newest ARCHIVE event), then the
/// `archived_at` UPDATE (sole arbiter, 0 rows = Conflict), then settle.
// ADR-0024 D-B: one gated transaction whose step ORDER is the contract (idempotency before
// consume, arbiter UPDATE last). Splitting it would scatter that ordering and hide the rollback
// boundary the reviewer must read as one sequence.
#[allow(clippy::too_many_lines)]
pub async fn archive_or_unarchive_atomically(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    request: ArchiveRequest,
) -> Result<ArchiveResult, ErrorCode> {
    validate_archive(auth, &request)?;
    let op = request.op;
    let idempotency_key = lifecycle_idempotency_key(&request.claim);
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    confirm_token_repo::set_authorization_local(&mut txn, auth).await?;

    // Idempotency: a replayed confirmed call returns the original success, never a second
    // BMO/consume/mutation. A different request under the same key is ALREADY_IN_STATE.
    use sqlx::Row;
    if let Some(row) = sqlx::query(
        "SELECT op, request_fingerprint, stream_seq, commit_seq, created_at \
         FROM ops.memory_lifecycle_events \
         WHERE tenant_id = $1 AND actor_principal_id = $2 AND idempotency_key = $3",
    )
    .bind(auth.tenant_id().0)
    .bind(auth.principal().0)
    .bind(&idempotency_key)
    .fetch_optional(&mut *txn)
    .await
    .map_err(db_error)?
    {
        let stored_op: String = row.try_get("op").map_err(|_| ErrorCode::Internal)?;
        let fingerprint: String = row
            .try_get("request_fingerprint")
            .map_err(|_| ErrorCode::Internal)?;
        let expected_op = match op {
            DestructiveOp::MemoryArchive => LifecycleOp::Archive,
            _ => LifecycleOp::Restore,
        };
        if stored_op != expected_op.as_db_str() || fingerprint != request.request_fingerprint {
            return Ok(ArchiveResult::Refused(ConflictReason::ALREADY_IN_STATE));
        }
        let (Some(stream_seq), Some(commit_seq)) = (
            row.try_get::<Option<i64>, _>("stream_seq")
                .map_err(|_| ErrorCode::Internal)?,
            row.try_get::<Option<i64>, _>("commit_seq")
                .map_err(|_| ErrorCode::Internal)?,
        ) else {
            return Err(ErrorCode::Internal);
        };
        let changed_at: OffsetDateTime =
            row.try_get("created_at").map_err(|_| ErrorCode::Internal)?;
        return Ok(ArchiveResult::Done(ArchiveDone {
            changed_at,
            stream_seq,
            commit_seq,
        }));
    }

    // Token first (same as supersede/restore): a replayed/expired/misbound token is Conflict.
    confirm_token_repo::consume_in_txn(&mut txn, auth, &request.claim).await?;

    let (currently_archived, archive_event) = archive_state(&mut txn, auth, request.target).await?;
    if let Err(reason) = humaux_application::archive::archive_allowed(op, currently_archived) {
        // ALREADY_IN_STATE: return the reason and let the transaction roll back (consume undone).
        return Ok(ArchiveResult::Refused(reason));
    }

    let reservation = match quota_repo::reserve_bmo_in_txn(
        &mut txn,
        auth,
        request.request_id,
        op.operation_key(),
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

    // New ticket (new stream seq) + lifecycle event before the arbiter UPDATE; a 0-row UPDATE
    // (a concurrent racer flipped the flag first) rolls both back.
    let (stream_seq, commit_seq) =
        issue_lifecycle_ticket(&mut txn, auth, &request.stream, request.target).await?;
    let (lifecycle_op, reason, undoes) = match op {
        DestructiveOp::MemoryArchive => (
            LifecycleOp::Archive,
            Some(LifecycleReason::UserArchive),
            None,
        ),
        // Unarchive undoes the memory's newest ARCHIVE event (D-B): a RESTORE naming that
        // event, no fresh reason. NOT the current lifecycle head — a supersede between the
        // archive and this unarchive moves the head to that SUPERSEDE, and naming it would
        // record a false undo edge in the append-only log.
        _ => (
            LifecycleOp::Restore,
            None,
            Some(archive_event.ok_or(ErrorCode::Internal)?),
        ),
    };
    let event_id = append_lifecycle_event(
        &mut txn,
        auth,
        lifecycle_op,
        reason,
        request.target.0,
        None, // replacement
        None, // correction_evidence
        undoes,
        // No undo_deadline: archive is unarchivable any time; the RESTORE undo is terminal.
        None,
        &idempotency_key,
        &request.request_fingerprint,
        stream_seq,
        commit_seq,
    )
    .await?;

    // Sole arbiter (mirrors supersede's WHERE status='active'): PostgreSQL re-evaluates the
    // `archived_at IS [NOT] NULL` predicate under the row lock, so the concurrent
    // archive/unarchive race and "already in that state" both resolve here, never on a stale
    // pre-read. Only columns role_gateway holds a grant for (§6.2.2): archived_at +
    // lifecycle_head_event_id — never status/updated_at. G59-4 is untouched (archive is not a
    // status transition). Return clock_timestamp() as the change instant.
    let arbiter = match op {
        DestructiveOp::MemoryArchive => {
            "UPDATE private.memory_records \
                SET archived_at = clock_timestamp(), lifecycle_head_event_id = $3 \
              WHERE tenant_id = $2 AND memory_id = $1 AND archived_at IS NULL \
              RETURNING archived_at"
        }
        _ => {
            "UPDATE private.memory_records \
                SET archived_at = NULL, lifecycle_head_event_id = $3 \
              WHERE tenant_id = $2 AND memory_id = $1 AND archived_at IS NOT NULL \
              RETURNING clock_timestamp()"
        }
    };
    let changed_at: Option<OffsetDateTime> = sqlx::query_scalar(arbiter)
        .bind(request.target.0)
        .bind(auth.tenant_id().0)
        .bind(event_id)
        .fetch_optional(&mut *txn)
        .await
        .map_err(db_error)?;
    let changed_at = changed_at.ok_or(ErrorCode::Conflict)?;

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
    txn.commit()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    Ok(ArchiveResult::Done(ArchiveDone {
        changed_at,
        stream_seq,
        commit_seq,
    }))
}

// ===========================================================================================
// memory.correct — a user correction: one transaction inserts a new DirectUserInput Evidence
// + a new Memory version and supersedes the original (ADR-0025, §Q4 / spec :8568).
// ===========================================================================================

/// Trusted application inputs for `memory.correct` (built by the gateway gate). The corrected
/// content arrives already paired with its `EvidencePayloadSha256` (the gateway hashed the raw
/// bytes through `evidence::payload_sha256`, the sole constructor) so the Evidence digest is
/// never re-derived here.
pub struct CorrectRequest {
    pub request_id: Uuid,
    pub request_fingerprint: String,
    pub reservation_ttl: Duration,
    /// The Memory being corrected, `M1`.
    pub target: MemoryId,
    /// The corrected content, stored verbatim as `E2`'s event payload and `M2`'s content.
    pub content: serde_json::Value,
    /// Digest of the exact raw bytes of `content` (§48.0①), for `E2`.
    pub payload_sha256: EvidencePayloadSha256,
    /// Stream family the lifecycle ticket + the new Evidence are issued on.
    pub stream: StreamKey,
    pub claim: ConfirmationClaim,
    pub finished_audit: AuditEvent,
    /// §78.1 undo window: how long the resulting SUPERSEDE (reason USER_CORRECTION) stays
    /// restorable via `memory.restore` (a correction's undo IS card 3's restore of that
    /// SUPERSEDE).
    pub undo_window: Duration,
    /// §15.5 consistency_token lifetime for the token returned on `M2`'s new stream seq.
    pub consistency_token_ttl: Duration,
    /// §6.1.3 rules 1/2 (ADR-0028): explicit subjects `M2` is about, on top of what it inherits
    /// from `M1` (the 0154 trigger). Resolved under the tenant's RLS BEFORE the token is
    /// consumed — unknown ⇒ `INVALID_INPUT`, nothing written, token intact — and applied inside
    /// this same transaction.
    pub subjects: SubjectDeclaration,
}

#[derive(Debug, Clone)]
pub struct CorrectDone {
    /// The new active Memory version, `M2`.
    pub new_memory_id: MemoryId,
    /// The corrected (now-superseded) Memory, `M1`.
    pub superseded: MemoryId,
    /// The new DirectUserInput Evidence, `E2`.
    pub evidence_id: Uuid,
    pub superseded_at: OffsetDateTime,
    pub stream_seq: i64,
    pub commit_seq: i64,
    pub consistency_token: String,
    /// §6.1.3: every subject `M2` is linked to after the hook ran (inherited from `M1` +
    /// explicit), in link order.
    pub subject_ids: Vec<Uuid>,
}

/// The `M1` facts a correction copies onto `E2`/`M2` (visibility + reasoning domain + type).
struct CorrectionSource {
    memory_type: MemoryType,
    reasoning_domain_id: Uuid,
    data_class: String,
    visibility_class: String,
    visibility_user_id: Option<Uuid>,
    visibility_workspace_id: Option<Uuid>,
}

/// §78.2 read-side inverse of `distill_repo::memory_type_db_str`, reusing that sole write-side
/// mapping (no third copy of the CHECK strings).
fn memory_type_from_wire(wire: &str) -> Option<MemoryType> {
    [
        MemoryType::Fact,
        MemoryType::Preference,
        MemoryType::Decision,
        MemoryType::Rejection,
        MemoryType::State,
        MemoryType::Issue,
        MemoryType::Lesson,
        MemoryType::Constraint,
        MemoryType::Procedure,
        MemoryType::Outcome,
        MemoryType::Reference,
        MemoryType::Note,
    ]
    .into_iter()
    .find(|t| distill_repo::memory_type_db_str(*t) == wire)
}

/// Pure input contract for `memory.correct`: the claim must be this operation on exactly this
/// target with NO successor (M2 is minted inside this transaction, so the token carries none),
/// the stream family must be the caller's tenant, and the success audit must describe exactly
/// this operation as an executed write.
fn validate_correct(auth: &AuthorizationScope, request: &CorrectRequest) -> Result<(), ErrorCode> {
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
    {
        return Err(ErrorCode::InvalidInput);
    }
    if request.claim.op != CORRECT_OP
        || request.claim.target_id != request.target.0
        || request.claim.successor_id.is_some()
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
        || event.resource_id != CORRECT_OP.operation_key()
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

/// Loads `M1`'s visibility/reasoning-domain/type facts to copy onto `E2`/`M2`, after checking
/// `M1` is readable to the caller (RLS + `can_read`). A row the caller cannot read is
/// `NotFound`, never a hint (same rule as supersede's `successor_status`).
async fn correction_source(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    auth: &AuthorizationScope,
    target: MemoryId,
) -> Result<CorrectionSource, ErrorCode> {
    let readable = context_repo::readable_memory_ids(txn, auth, &[target.0]).await?;
    if !readable.contains(&target.0) {
        return Err(ErrorCode::NotFound);
    }
    use sqlx::Row;
    let row = sqlx::query(
        "SELECT mr.memory_type, eo.reasoning_domain_id, eo.data_class, eo.visibility_class, \
                eo.visibility_user_id, eo.visibility_workspace_id \
         FROM private.memory_records mr \
         JOIN private.memory_evidence me ON me.memory_id = mr.memory_id \
         JOIN private.evidence_objects eo ON eo.evidence_id = me.evidence_id \
         WHERE mr.tenant_id = $1 AND mr.memory_id = $2 \
         ORDER BY (me.role = 'PRIMARY') DESC, me.ordinal ASC LIMIT 1",
    )
    .bind(auth.tenant_id().0)
    .bind(target.0)
    .fetch_optional(&mut **txn)
    .await
    .map_err(db_error)?
    .ok_or(ErrorCode::NotFound)?;
    let memory_type_wire: String = row
        .try_get("memory_type")
        .map_err(|_| ErrorCode::Internal)?;
    let memory_type = memory_type_from_wire(&memory_type_wire).ok_or(ErrorCode::Internal)?;
    Ok(CorrectionSource {
        memory_type,
        reasoning_domain_id: row
            .try_get("reasoning_domain_id")
            .map_err(|_| ErrorCode::Internal)?,
        data_class: row.try_get("data_class").map_err(|_| ErrorCode::Internal)?,
        visibility_class: row
            .try_get("visibility_class")
            .map_err(|_| ErrorCode::Internal)?,
        visibility_user_id: row
            .try_get("visibility_user_id")
            .map_err(|_| ErrorCode::Internal)?,
        visibility_workspace_id: row
            .try_get("visibility_workspace_id")
            .map_err(|_| ErrorCode::Internal)?,
    })
}

/// §Q4/ADR-0025, atomically. Order: reserve BMO -> consume token -> read + copy M1's facts ->
/// insert Evidence E2 (DirectUserInput) via remember's own issuers -> issue a MEMORY_LIFECYCLE
/// ticket bound to E2 (NOT an EVIDENCE_ACCEPTED distill trigger — M2 is materialized here) ->
/// materialize M2 via the distill version-insert -> append a SUPERSEDE(USER_CORRECTION) event
/// naming M2 + E2 with the undo deadline -> `UPDATE ... WHERE status='active'` (sole arbiter) ->
/// quota CONSUMED + audits -> COMMIT. The original Evidence's `events` row is never touched.
/// A replay of the same confirmed call returns the original success (idempotent on the
/// lifecycle table's UNIQUE(tenant, actor, idempotency_key), like restore).
#[allow(clippy::too_many_lines)] // one confirmed correction transaction, read top to bottom like supersede/restore
pub async fn correct_atomically(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    request: CorrectRequest,
) -> Result<CorrectDone, ErrorCode> {
    validate_correct(auth, &request)?;
    let idempotency_key = lifecycle_idempotency_key(&request.claim);
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    confirm_token_repo::set_authorization_local(&mut txn, auth).await?;

    // Idempotency (D-B): a replayed confirmed call returns the original CorrectDone — never a
    // second E2/M2 — resolved before any BMO/consume/insert. A different request under the
    // same key is Conflict.
    use sqlx::Row;
    if let Some(row) = sqlx::query(
        "SELECT op, reason_code, request_fingerprint, replacement_memory_id, \
                correction_evidence_id, stream_seq, commit_seq, created_at \
         FROM ops.memory_lifecycle_events \
         WHERE tenant_id = $1 AND actor_principal_id = $2 AND idempotency_key = $3",
    )
    .bind(auth.tenant_id().0)
    .bind(auth.principal().0)
    .bind(&idempotency_key)
    .fetch_optional(&mut *txn)
    .await
    .map_err(db_error)?
    {
        let op: String = row.try_get("op").map_err(|_| ErrorCode::Internal)?;
        let reason: Option<String> = row
            .try_get("reason_code")
            .map_err(|_| ErrorCode::Internal)?;
        let fingerprint: String = row
            .try_get("request_fingerprint")
            .map_err(|_| ErrorCode::Internal)?;
        if op != LifecycleOp::Supersede.as_db_str()
            || reason.as_deref() != Some(LifecycleReason::UserCorrection.as_db_str())
            || fingerprint != request.request_fingerprint
        {
            return Err(ErrorCode::Conflict);
        }
        let (Some(replacement), Some(evidence_id)) = (
            row.try_get::<Option<Uuid>, _>("replacement_memory_id")
                .map_err(|_| ErrorCode::Internal)?,
            row.try_get::<Option<Uuid>, _>("correction_evidence_id")
                .map_err(|_| ErrorCode::Internal)?,
        ) else {
            return Err(ErrorCode::Internal);
        };
        let (Some(stream_seq), Some(commit_seq)) = (
            row.try_get::<Option<i64>, _>("stream_seq")
                .map_err(|_| ErrorCode::Internal)?,
            row.try_get::<Option<i64>, _>("commit_seq")
                .map_err(|_| ErrorCode::Internal)?,
        ) else {
            return Err(ErrorCode::Internal);
        };
        let superseded_at: OffsetDateTime =
            row.try_get("created_at").map_err(|_| ErrorCode::Internal)?;
        let consistency_token = build_consistency_token(
            &request.stream,
            stream_seq,
            commit_seq,
            request.consistency_token_ttl,
        )?;
        let subject_ids = crate::subject_repo::memory_subject_ids_in_txn(
            &mut txn,
            auth.tenant_id().0,
            replacement,
        )
        .await
        .map_err(db_error)?;
        return Ok(CorrectDone {
            new_memory_id: MemoryId(replacement),
            superseded: request.target,
            evidence_id,
            superseded_at,
            stream_seq,
            commit_seq,
            consistency_token,
            subject_ids,
        });
    }

    let reservation = match quota_repo::reserve_bmo_in_txn(
        &mut txn,
        auth,
        request.request_id,
        CORRECT_OP.operation_key(),
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

    // §6.1.3 rules 1/2 under this tenant's RLS, before the token is consumed: an unknown (or
    // another tenant's) subject is INVALID_INPUT with nothing written and the token intact.
    let explicit_subjects = crate::subject_repo::resolve_declaration_in_txn(
        &mut txn,
        auth.tenant_id().0,
        &request.subjects,
    )
    .await?;

    // Token first: a replayed/expired/misbound token never reaches the row work.
    confirm_token_repo::consume_in_txn(&mut txn, auth, &request.claim).await?;

    let source = correction_source(&mut txn, auth, request.target).await?;

    // E2: a new DirectUserInput Evidence, visibility + reasoning-domain copied from M1, through
    // remember's own evidence/event issuers (never a second hand-written INSERT).
    let now = OffsetDateTime::now_utc();
    let expires_at = now
        .checked_add(
            time::Duration::try_from(request.consistency_token_ttl)
                .map_err(|_| ErrorCode::InvalidInput)?,
        )
        .ok_or(ErrorCode::InvalidInput)?;
    let cmd = RememberCommand {
        tenant_id: auth.tenant_id().0,
        authorization_user_id: auth.user_id().map(|u| u.0),
        scope_kind: request.stream.scope_kind.clone(),
        scope_id: request.stream.scope_id,
        domain: request.stream.domain.clone(),
        projection_kind: request.stream.projection_kind.clone(),
        projection_version: request.stream.projection_version.clone(),
        consistency_token_expires_at: expires_at,
        batch_id: None,
        payload_sha256: request.payload_sha256,
        data_class: source.data_class.clone(),
        // §Q4/§10.1: a user correction is DirectUserInput — its authority is NOT inherited from
        // M1, it is derived from this origin's ceiling.
        origin_class: EvidenceOriginClass::DirectUserInput,
        origin_principal_id: Some(auth.principal().0),
        origin_connector_id: None,
        visibility_class: source.visibility_class.clone(),
        visibility_user_id: source.visibility_user_id,
        visibility_workspace_id: source.visibility_workspace_id,
        reasoning_domain_id: source.reasoning_domain_id,
        occurred_at: None,
        event_kind: "USER_CORRECTION".to_owned(),
        event_payload: request.content.clone(),
        subjects: humaux_domain::subject::SubjectDeclaration::default(),
    };
    let evidence_id = remember::create_evidence_object(&mut txn, &cmd)
        .await
        .map_err(remember_error)?;
    remember::insert_event_subtype(&mut txn, evidence_id, &cmd)
        .await
        .map_err(remember_error)?;

    // One MEMORY_LIFECYCLE ticket bound to E2 (NOT EVIDENCE_ACCEPTED — no distill round trip;
    // D-C: M2 is materialized directly below, projection resolves it via memory_evidence(E2)).
    let commit_seq = remember::next_commit_seq(&mut txn)
        .await
        .map_err(remember_error)?;
    let stream_seq = remember::issue_stream_log_row(&mut txn, &request.stream, commit_seq)
        .await
        .map_err(remember_error)?;
    remember::insert_outbox(
        &mut txn,
        auth.tenant_id().0,
        commit_seq,
        stream_seq,
        remember::MEMORY_LIFECYCLE,
        evidence_id,
    )
    .await
    .map_err(remember_error)?;

    // M2: materialize the new version through the distill version-insert (memory_records +
    // PRIMARY memory_evidence(E2)). Authority via OriginBoundAuthorityPolicy for
    // DirectUserInput (not inherited).
    let scope = Scope {
        tenant_id: auth.tenant_id(),
        user_id: auth.user_id(),
        workspace_id: (request.stream.scope_kind == "workspace")
            .then_some(WorkspaceId(request.stream.scope_id)),
        repository_id: None,
        task_id: None,
        run_id: None,
        agent_id: None,
    };
    let class = humaux_application::correct::authorize_correction(source.memory_type, &scope)
        .map_err(|_| ErrorCode::Internal)?;
    let loaded = LoadedEvidence {
        evidence_id,
        reasoning_domain_id: source.reasoning_domain_id,
        origin_class: EvidenceOriginClass::DirectUserInput,
        origin_class_wire: "DirectUserInput".to_owned(),
        origin_principal_id: Some(auth.principal().0),
        data_class: source.data_class.clone(),
        visibility_class: source.visibility_class.clone(),
        visibility_user_id: source.visibility_user_id,
        visibility_workspace_id: source.visibility_workspace_id,
        occurred_at: None,
        payload_sha256: hex::decode(request.payload_sha256.to_hex())
            .map_err(|_| ErrorCode::Internal)?,
        event_kind: "USER_CORRECTION".to_owned(),
        payload: request.content.clone(),
        rls_user_id: auth.user_id().map_or_else(Uuid::nil, |u| u.0),
    };
    let new_memory_id = distill_repo::insert_memory(
        &mut txn,
        auth.tenant_id().0,
        &loaded,
        &NewMemory {
            content: &request.content,
            memory_type: source.memory_type,
            class,
            // A direct user correction is asserted at full confidence.
            confidence: 1.0,
        },
    )
    .await
    .map_err(db_error)?;

    // The SUPERSEDE(USER_CORRECTION) record: names M2 + E2, carries the undo deadline. Written
    // before the arbiter UPDATE so a 0-row UPDATE rolls it back with the txn.
    let event_id = append_lifecycle_event(
        &mut txn,
        auth,
        LifecycleOp::Supersede,
        Some(LifecycleReason::UserCorrection),
        request.target.0,
        Some(new_memory_id),
        Some(evidence_id),
        None,
        Some(request.undo_window.as_secs_f64()),
        &idempotency_key,
        &request.request_fingerprint,
        stream_seq,
        commit_seq,
    )
    .await?;

    // Sole arbiter (mirrors supersede): PostgreSQL re-evaluates status='active' under the row
    // lock. 0 rows (already superseded / lost the race) = Conflict, rolling E2/M2 back.
    let superseded_at: Option<OffsetDateTime> = sqlx::query_scalar(
        "UPDATE private.memory_records \
            SET status = 'superseded', superseded_by = $2, superseded_at = clock_timestamp(), \
                lifecycle_head_event_id = $4 \
          WHERE tenant_id = $3 AND memory_id = $1 AND status = 'active' \
          RETURNING superseded_at",
    )
    .bind(request.target.0)
    .bind(new_memory_id)
    .bind(auth.tenant_id().0)
    .bind(event_id)
    .fetch_optional(&mut *txn)
    .await
    .map_err(db_error)?;
    let superseded_at = superseded_at.ok_or(ErrorCode::Conflict)?;

    // §6.1.3: the arbiter UPDATE above fired the 0154 trigger (M2 inherited M1's links);
    // the caller's explicit subjects go through the same hook in this same transaction.
    if !explicit_subjects.is_empty() {
        crate::subject_repo::link_memory_in_txn(
            &mut txn,
            auth.tenant_id().0,
            new_memory_id,
            &explicit_subjects,
        )
        .await
        .map_err(db_error)?;
    }
    let subject_ids =
        crate::subject_repo::memory_subject_ids_in_txn(&mut txn, auth.tenant_id().0, new_memory_id)
            .await
            .map_err(db_error)?;

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
    let consistency_token = build_consistency_token(
        &request.stream,
        stream_seq,
        commit_seq,
        request.consistency_token_ttl,
    )?;
    txn.commit()
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    Ok(CorrectDone {
        new_memory_id: MemoryId(new_memory_id),
        superseded: request.target,
        evidence_id,
        superseded_at,
        stream_seq,
        commit_seq,
        consistency_token,
        subject_ids,
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
            undo_window: Duration::from_secs(86_400),
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

    fn restore_request(auth: &AuthorizationScope) -> RestoreRequest {
        let request_id = Uuid::now_v7();
        let target = MemoryId::new();
        RestoreRequest {
            request_id,
            request_fingerprint: "b".repeat(64),
            reservation_ttl: Duration::from_secs(30),
            target,
            stream: StreamKey::new(
                auth.tenant_id(),
                "workspace",
                Uuid::now_v7(),
                "knowledge",
                "ingest",
                "v1",
            ),
            claim: ConfirmationClaim {
                op: RESTORE_OP,
                target_id: target.0,
                successor_id: None,
                nonce_sha256: [4u8; 32],
            },
            consistency_token_ttl: Duration::from_secs(60),
            finished_audit: AuditEvent {
                event_id: AuditEventId::new(),
                ts: SystemTime::now(),
                tenant_id: auth.tenant_id(),
                actor_type: "SERVICE_CREDENTIAL".into(),
                actor_id: auth.principal().0.to_string(),
                action: McpAuditAction::McpRequestFinished.as_str().into(),
                resource_type: "MCP_OPERATION".into(),
                resource_id: RESTORE_OP.operation_key().into(),
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

    /// The restore contract binds the claim to exactly this `memory.restore` target with no
    /// successor, and refuses a mint-tagged or misbound audit — the same lifecycle-write
    /// input guard the supersede path has, mirrored for the undo direction.
    #[test]
    fn restore_validate_binds_claim_target_and_audit_lifecycle() {
        let auth = scope();
        assert_eq!(validate_restore(&auth, &restore_request(&auth)), Ok(()));

        // A restore token can never carry a successor (it undoes one memory, names none).
        let mut with_successor = restore_request(&auth);
        with_successor.claim.successor_id = Some(Uuid::now_v7());
        assert_eq!(
            validate_restore(&auth, &with_successor),
            Err(ErrorCode::Conflict)
        );

        let mut misbound = restore_request(&auth);
        misbound.claim.target_id = Uuid::now_v7();
        assert_eq!(validate_restore(&auth, &misbound), Err(ErrorCode::Conflict));

        // A supersede token must never execute a restore.
        let mut wrong_op = restore_request(&auth);
        wrong_op.claim.op = OP;
        assert_eq!(validate_restore(&auth, &wrong_op), Err(ErrorCode::Conflict));

        let mut mint_tagged = restore_request(&auth);
        mint_tagged
            .finished_audit
            .risk_tags
            .push(RISK_TAG_CONFIRMATION_MINTED.to_owned());
        assert_eq!(
            validate_restore(&auth, &mint_tagged),
            Err(ErrorCode::InvalidInput)
        );

        let mut wrong_audit = restore_request(&auth);
        wrong_audit.finished_audit.resource_id = "memory.supersede".into();
        assert_eq!(
            validate_restore(&auth, &wrong_audit),
            Err(ErrorCode::InvalidInput)
        );

        let headless = AuthorizationScope::new(
            auth.tenant_id(),
            auth.principal(),
            None,
            auth.allowed_workspace_ids().clone(),
        );
        assert_eq!(
            validate_restore(&headless, &restore_request(&auth)),
            Err(ErrorCode::Unauthorized)
        );
    }

    /// The lifecycle idempotency key is the confirm token's digest in hex — stable across a
    /// client's retry of the same confirmed call, so a replayed restore is deduplicated.
    #[test]
    fn lifecycle_idempotency_key_is_token_digest_hex() {
        let claim = ConfirmationClaim {
            op: RESTORE_OP,
            target_id: Uuid::now_v7(),
            successor_id: None,
            nonce_sha256: [0xabu8; 32],
        };
        assert_eq!(lifecycle_idempotency_key(&claim), "ab".repeat(32));
    }
}
