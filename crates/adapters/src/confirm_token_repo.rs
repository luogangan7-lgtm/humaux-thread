//! `adapters::confirm_token_repo` — `control.confirm_tokens` (migration 0148, ADR-0018).
//! Depends-on: crates=[humaux-domain, sqlx, uuid]; services=[PostgreSQL(any) w=[control.confirm_tokens] x=[control.sweep_confirm_tokens, control.assert_write_scope]]; env=[]; modules=[adapters::postgres, adapters::request_guard_repo, domain::audit, domain::confirm, domain::error, domain::identity, domain::ids]
//! Called-by: [adapters::affect_repo, adapters::context_repo, adapters::distill_repo, adapters::memory_governance_repo, adapters::subject_repo, gateway::guard, tests, xtask::confirm_sweep]
//! Invariants: [tokens store only sha256(nonce) and are consumed by one UPDATE inside the caller's transaction;
//!   replayed, expired or mis-bound tokens — including one minted for another workspace — are one indistinguishable
//!   Conflict, never a silent success; every write transaction rechecks its principal and its one workspace
//!   through control.assert_write_scope before it writes]
//! Spec: Baseline §33.10; ADR-0018; ADR-0054
//!
//! Two writes, both `role_gateway`, both FORCE-RLS tenant-scoped:
//! - [`mint_with_audit`]: first call of a §33.10 two-step destructive action. One
//!   transaction = token row + the operation's `MCP_REQUEST_FINISHED` audit; no other
//!   durable write (D-B). Stores only `sha256(nonce)`. The audit row carries
//!   `RISK_TAG_CONFIRMATION_MINTED` so it never counts as an executed destructive write.
//! - [`consume_in_txn`]: second call. One `UPDATE ... WHERE <full binding> AND consumed_at
//!   IS NULL AND expires_at > now RETURNING` inside the *caller's* transaction, so the
//!   token is consumed atomically with the mutation it gates. Zero rows — replayed, expired,
//!   or bound to another (tenant, workspace, user, operation, target, successor) — is one
//!   indistinguishable `Conflict` (D-B: never a silent success, never an existence oracle).
//! - [`set_write_authorization_local`] (ADR-0054 D-B): the one write-scope door every
//!   governance / subject / affect write transaction opens with — the RLS GUCs plus the
//!   in-transaction recheck `control.assert_write_scope(principal, workspace)` (stale principal ⇒
//!   `Unauthorized`, workspace not granted ⇒ `Forbidden`, membership rows held FOR SHARE).

use std::time::Duration;

use humaux_domain::{
    audit::{AuditEvent, McpAuditAction},
    confirm::{DestructiveOp, RISK_TAG_CONFIRMATION_MINTED},
    error::ErrorCode,
    identity::AuthorizationScope,
    ids::WorkspaceId,
};
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    postgres::{MaintenanceDbPool, RuntimeDbPool},
    request_guard_repo::{self, AuditTenant},
};

type Txn<'c> = sqlx::Transaction<'c, sqlx::Postgres>;

/// A presented token's server-side binding, carried from the gateway gate into the
/// adapter transaction that consumes it. Holds only the digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmationClaim {
    pub op: DestructiveOp,
    pub target_id: Uuid,
    /// The operation's second argument (`memory.supersede`: `replacement_memory_id`). Part of
    /// the binding: a token confirms one (target, successor) pair, not a target alone.
    pub successor_id: Option<Uuid>,
    pub nonce_sha256: [u8; 32],
}

/// The only consume predicate: the full binding, unconsumed, unexpired. `successor_id` uses
/// `IS NOT DISTINCT FROM` so a NULL-bound token matches only a NULL claim; `workspace_id` uses
/// plain `=` so a pre-0187 (NULL) row never matches (ADR-0054 D-C). Pinned by
/// `consume_predicate_is_the_full_binding` — the owner trigger in 0148 masks a dropped
/// expiry/consumed clause at the DB layer, so the adapter predicate needs its own witness.
const CONSUME_SQL: &str = "UPDATE control.confirm_tokens SET consumed_at = clock_timestamp() \
     WHERE nonce_sha256 = $1 AND tenant_id = $2 AND user_id = $3 \
       AND operation = $4 AND target_id = $5 AND successor_id IS NOT DISTINCT FROM $6 \
       AND workspace_id = $7 \
       AND consumed_at IS NULL AND expires_at > clock_timestamp() \
     RETURNING confirm_token_id";

/// What the first call hands back to the client alongside the token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MintedConfirmation {
    pub expires_at: OffsetDateTime,
}

fn db_error(error: sqlx::Error) -> ErrorCode {
    match error {
        sqlx::Error::RowNotFound => ErrorCode::NotFound,
        sqlx::Error::Database(ref db) => match db.code().as_deref() {
            Some("42501") => ErrorCode::Forbidden,
            // control.assert_write_scope: the principal went stale since authentication.
            Some("28000") => ErrorCode::Unauthorized,
            Some("23503") => ErrorCode::TenantBoundary,
            Some("23505" | "40001" | "40P01" | "55P03" | "23514") => ErrorCode::Conflict,
            Some("22023" | "22P02" | "22003") => ErrorCode::InvalidInput,
            _ => ErrorCode::Internal,
        },
        _ => ErrorCode::DependencyUnavailable,
    }
}

pub(crate) async fn set_authorization_local(
    txn: &mut Txn<'_>,
    auth: &AuthorizationScope,
) -> Result<(), ErrorCode> {
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
    Ok(())
}

/// ADR-0054 D-B: a write scope names exactly one workspace — the narrowed request route. An
/// unnarrowed scope (an unbound PAT's whole member set) or an empty one is `Forbidden`.
pub(crate) fn sole_workspace(auth: &AuthorizationScope) -> Result<WorkspaceId, ErrorCode> {
    let set = auth.allowed_workspace_ids();
    match (set.len(), set.iter().next()) {
        (1, Some(workspace)) => Ok(*workspace),
        _ => Err(ErrorCode::Forbidden),
    }
}

/// ADR-0054 D-B: the GUCs of [`set_authorization_local`] plus the in-transaction recheck of
/// the principal and its one workspace (`control.assert_write_scope`, 0188). Every governance /
/// subject / affect write transaction opens with this; reads keep [`set_authorization_local`].
pub(crate) async fn set_write_authorization_local(
    txn: &mut Txn<'_>,
    auth: &AuthorizationScope,
) -> Result<WorkspaceId, ErrorCode> {
    let workspace = sole_workspace(auth)?;
    set_authorization_local(txn, auth).await?;
    // dep: PostgreSQL(any) — control.assert_write_scope recheck inside the caller's write transaction
    sqlx::query("SELECT control.assert_write_scope($1, $2)")
        .bind(auth.principal().0)
        .bind(workspace.0)
        .execute(&mut **txn)
        .await
        .map_err(db_error)?;
    Ok(workspace)
}

/// Pure input contract of [`mint_with_audit`]: a token is always bound to a real user
/// (D-A), lives for a positive server-policy TTL, binds a successor distinct from the target,
/// and is audited as exactly this operation *tagged as a mint* (never as an executed write).
fn validate_mint(
    auth: &AuthorizationScope,
    op: DestructiveOp,
    target_id: Uuid,
    successor_id: Option<Uuid>,
    ttl: Duration,
    finished_audit: &AuditEvent,
) -> Result<(Uuid, f64), ErrorCode> {
    let user_id = auth.user_id().ok_or(ErrorCode::Unauthorized)?.0;
    if auth.tenant_id().0.is_nil() || auth.principal().0.is_nil() || user_id.is_nil() {
        return Err(ErrorCode::Unauthorized);
    }
    if ttl.is_zero() || ttl > Duration::from_secs(86_400 * 366) || successor_id == Some(target_id) {
        return Err(ErrorCode::InvalidInput);
    }
    if finished_audit.tenant_id != auth.tenant_id()
        || finished_audit.actor_id != auth.principal().0.to_string()
        || finished_audit.action != McpAuditAction::McpRequestFinished.as_str()
        || finished_audit.resource_id != op.operation_key()
        || finished_audit.result != "OK"
        || !finished_audit
            .risk_tags
            .iter()
            .any(|tag| tag == RISK_TAG_CONFIRMATION_MINTED)
    {
        return Err(ErrorCode::InvalidInput);
    }
    Ok((user_id, ttl.as_secs_f64()))
}

/// D-B first call: token row + finished audit in one commit; nothing else durable.
#[allow(clippy::too_many_arguments)] // ADR-0018 D-A binding is 8 explicit facts (pool, scope, op, target, successor, nonce, ttl, audit); bundling them into a struct would hide the binding the reviewer must read.
pub async fn mint_with_audit(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    op: DestructiveOp,
    target_id: Uuid,
    successor_id: Option<Uuid>,
    ttl: Duration,
    nonce_sha256: [u8; 32],
    finished_audit: &AuditEvent,
) -> Result<MintedConfirmation, ErrorCode> {
    let (user_id, ttl_seconds) =
        validate_mint(auth, op, target_id, successor_id, ttl, finished_audit)?;
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    let workspace = set_write_authorization_local(&mut txn, auth).await?;
    let expires_at: OffsetDateTime = sqlx::query_scalar(
        "INSERT INTO control.confirm_tokens \
           (tenant_id, user_id, operation, target_id, successor_id, nonce_sha256, expires_at, \
            workspace_id) \
         VALUES ($1, $2, $3, $4, $7, $5, clock_timestamp() + make_interval(secs => $6), $8) \
         RETURNING expires_at",
    )
    .bind(auth.tenant_id().0)
    .bind(user_id)
    .bind(op.operation_key())
    .bind(target_id)
    .bind(nonce_sha256.as_slice())
    .bind(ttl_seconds)
    .bind(successor_id)
    .bind(workspace.0)
    .fetch_one(&mut *txn)
    .await
    .map_err(db_error)?;
    request_guard_repo::audit_event_insert_in_txn(
        &mut txn,
        AuditTenant::Authenticated(auth),
        finished_audit,
    )
    .await?;
    txn.commit().await.map_err(db_error)?;
    Ok(MintedConfirmation { expires_at })
}

/// §33.10 rule 9 / card 21 (card 1 review P2): the retention sweep — the ONLY caller-side door
/// to `control.sweep_confirm_tokens(interval)` (migration 0169, forward-fixed by 0170).
///
/// Why a function call and not a `DELETE` here: `role_maintenance` holds no DELETE on the table
/// (§6.2.1 bans that verb for every non-owner role, globally — 0169 granted it, `xtask
/// rls-check` went red, 0170 revoked it). The predicate lives once, inside the owner SECURITY
/// DEFINER function, rather than in every operator's shell history:
///
/// > deletable ⇔ `expires_at < now()` AND (`consumed_at IS NULL` OR `consumed_at < now() -
/// > retention`)
///
/// i.e. a row that can no longer gate anything AND is no longer wanted as audit. A
/// recently-consumed token is deliberately KEPT: the §9 audit answer "which confirm token
/// authorized this destructive call" has to outlive the call. `retention` is the deployment's
/// policy, never a literal in here or in the function (§78.1).
///
/// Per-tenant by construction: `control.confirm_tokens` FORCEs RLS and its policy is
/// `tenant_id = current_setting('humaux.tenant_id')`, which the definer owner is subject to as
/// well — a call with no tenant context matches nothing and deletes nothing. A deployment
/// patrolling every tenant loops `control.tenants` and calls this once per tenant, exactly like
/// [`crate::stream_repo::sweep_lost`]. Returns the number of rows deleted for that tenant.
pub async fn sweep_expired(
    pool: &MaintenanceDbPool,
    tenant_id: Uuid,
    retention: Duration,
) -> Result<i64, ErrorCode> {
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    // Same technique and rationale as `stream_repo::set_tenant_local` (a `Uuid`'s `Display`
    // only ever emits canonical lowercase hex, so this formatted string carries nothing
    // injectable) — `SET LOCAL` takes no bind parameters.
    sqlx::query(&format!("SET LOCAL humaux.tenant_id = '{tenant_id}'"))
        .execute(&mut *txn)
        .await
        .map_err(db_error)?;
    let deleted: i64 =
        sqlx::query_scalar("SELECT control.sweep_confirm_tokens(make_interval(secs => $1))")
            .bind(retention.as_secs_f64())
            .fetch_one(&mut *txn)
            .await
            .map_err(db_error)?;
    txn.commit().await.map_err(db_error)?;
    Ok(deleted)
}

/// D-A/D-B second call: verify + consume in the caller's transaction. Exactly one row may
/// match the full binding — including the scope's one workspace (ADR-0054) — while unconsumed
/// and unexpired; anything else is `Conflict`. The write-scope recheck runs first.
pub async fn consume_in_txn(
    txn: &mut Txn<'_>,
    auth: &AuthorizationScope,
    claim: &ConfirmationClaim,
) -> Result<(), ErrorCode> {
    let user_id = auth.user_id().ok_or(ErrorCode::Unauthorized)?.0;
    let workspace = set_write_authorization_local(txn, auth).await?;
    let consumed: Option<Uuid> = sqlx::query_scalar(CONSUME_SQL)
        .bind(claim.nonce_sha256.as_slice())
        .bind(auth.tenant_id().0)
        .bind(user_id)
        .bind(claim.op.operation_key())
        .bind(claim.target_id)
        .bind(claim.successor_id)
        .bind(workspace.0)
        .fetch_optional(&mut **txn)
        .await
        .map_err(db_error)?;
    consumed.map(|_| ()).ok_or(ErrorCode::Conflict)
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use humaux_domain::{
        audit::{AuditEventId, AuditMetadata, SYSTEM_TENANT_ID},
        identity::{BoundedSet, PrincipalId},
        ids::{TenantId, UserId, WorkspaceId},
    };

    use super::*;

    fn scope(user: Option<Uuid>) -> AuthorizationScope {
        AuthorizationScope::new(
            TenantId(Uuid::now_v7()),
            PrincipalId(Uuid::now_v7()),
            user.map(UserId),
            BoundedSet::new([WorkspaceId(Uuid::now_v7())]).unwrap(),
        )
    }

    fn audit(auth: &AuthorizationScope, resource: &str, result: &str) -> AuditEvent {
        AuditEvent {
            event_id: AuditEventId::new(),
            ts: SystemTime::now(),
            tenant_id: auth.tenant_id(),
            actor_type: "SERVICE_CREDENTIAL".into(),
            actor_id: auth.principal().0.to_string(),
            action: McpAuditAction::McpRequestFinished.as_str().into(),
            resource_type: "MCP_OPERATION".into(),
            resource_id: resource.into(),
            result: result.into(),
            request_id: Uuid::now_v7().to_string(),
            trace_id: String::new(),
            client_ip: "127.0.0.1".into(),
            user_agent_hash: String::new(),
            risk_tags: vec![RISK_TAG_CONFIRMATION_MINTED.to_owned()],
            before_fingerprint: None,
            after_fingerprint: None,
            metadata: AuditMetadata::new(),
        }
    }

    #[test]
    fn mint_contract_requires_user_positive_ttl_pair_and_tagged_audit() {
        let op = DestructiveOp::MemorySupersede;
        let auth = scope(Some(Uuid::now_v7()));
        let (target, successor) = (Uuid::now_v7(), Some(Uuid::now_v7()));
        let ttl = Duration::from_secs(60);
        let ok = audit(&auth, op.operation_key(), "OK");
        let mint = |auth: &AuthorizationScope, successor, ttl, event: &AuditEvent| {
            validate_mint(auth, op, target, successor, ttl, event).err()
        };
        assert_eq!(mint(&auth, successor, ttl, &ok), None);
        assert_eq!(mint(&auth, None, ttl, &ok), None);
        assert_eq!(
            mint(&scope(None), successor, ttl, &ok),
            Some(ErrorCode::Unauthorized)
        );
        assert_eq!(
            mint(&auth, successor, Duration::ZERO, &ok),
            Some(ErrorCode::InvalidInput)
        );
        assert_eq!(
            mint(&auth, Some(target), ttl, &ok),
            Some(ErrorCode::InvalidInput),
            "a token never binds a self-supersede"
        );
        assert_eq!(
            mint(&auth, successor, ttl, &audit(&auth, "memory.get", "OK")),
            Some(ErrorCode::InvalidInput)
        );
        assert_eq!(
            mint(
                &auth,
                successor,
                ttl,
                &audit(&auth, op.operation_key(), "CONFLICT")
            ),
            Some(ErrorCode::InvalidInput)
        );
        let mut untagged = ok.clone();
        untagged.risk_tags.clear();
        assert_eq!(
            mint(&auth, successor, ttl, &untagged),
            Some(ErrorCode::InvalidInput),
            "a mint audit indistinguishable from an executed write is refused"
        );
        let mut foreign = ok.clone();
        foreign.tenant_id = SYSTEM_TENANT_ID;
        assert_eq!(
            mint(&auth, successor, ttl, &foreign),
            Some(ErrorCode::InvalidInput)
        );
    }

    /// Fault-injection witness for the adapter predicate: the 0148 owner trigger rejects a
    /// consume past `expires_at` on its own, so a live test cannot tell "adapter refused"
    /// from "trigger refused". Every clause of the binding is pinned here instead.
    #[test]
    fn consume_predicate_is_the_full_binding() {
        for clause in [
            "nonce_sha256 = $1",
            "tenant_id = $2",
            "user_id = $3",
            "operation = $4",
            "target_id = $5",
            "successor_id IS NOT DISTINCT FROM $6",
            "workspace_id = $7",
            "consumed_at IS NULL",
            "expires_at > clock_timestamp()",
            "RETURNING confirm_token_id",
        ] {
            assert!(
                CONSUME_SQL.contains(clause),
                "consume predicate lost `{clause}`"
            );
        }
        assert!(
            CONSUME_SQL
                .starts_with("UPDATE control.confirm_tokens SET consumed_at = clock_timestamp()")
        );
    }

    /// ADR-0054 D-B: the token and the recheck bind exactly one workspace — an unnarrowed or
    /// empty scope is refused before any statement.
    #[test]
    fn sole_workspace_requires_a_singleton_scope() {
        let (tenant, principal, user) = (
            TenantId(Uuid::now_v7()),
            PrincipalId(Uuid::now_v7()),
            Some(UserId(Uuid::now_v7())),
        );
        let (w1, w2) = (WorkspaceId(Uuid::now_v7()), WorkspaceId(Uuid::now_v7()));
        let with = |set: Vec<WorkspaceId>| {
            AuthorizationScope::new(tenant, principal, user, BoundedSet::new(set).unwrap())
        };
        assert_eq!(sole_workspace(&with(vec![w1])), Ok(w1));
        assert_eq!(
            sole_workspace(&with(vec![w1, w2])),
            Err(ErrorCode::Forbidden)
        );
        assert_eq!(sole_workspace(&with(Vec::new())), Err(ErrorCode::Forbidden));
    }
}
