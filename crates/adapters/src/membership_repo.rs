//! `adapters::membership_repo` — the §6.3 membership lifecycle admin path (ADR-0033, card 12).
//! Depends-on: crates=[humaux-domain, serde_json, sqlx, uuid]; services=[PostgreSQL(any) r=[control.users] w=[control.memberships] x=[control.audit_event_insert, control.bump_user_security_epoch]]; env=[]; modules=[adapters::postgres, domain::error, domain::identity, domain::ids]
//! Called-by: [adapters::provisioning, maintenance::main, tests, xtask::e2e_seed, xtask::member]
//! Invariants: [sole writer of control.memberships, on role_maintenance from xtask member only; one transaction locks
//!   the target and the tenant's other ACTIVE OWNERs so the last OWNER cannot be removed; a domain refusal writes
//!   nothing]
//! Spec: Baseline §6.3; §77; §78.1
//!
//! The only code in the workspace that writes `control.memberships`. It runs under
//! [`MaintenanceDbPool`] (`role_maintenance` — migration 0161 grants that role, and only that
//! role, INSERT + column-level UPDATE(state, role, updated_at)) and is reached from
//! `xtask member`, never from an MCP tool: membership mutation is an operator action, not an
//! agent-facing verb.
//!
//! One request = one transaction, always in this order and never split:
//! 1. `SET LOCAL humaux.tenant_id` (FORCE RLS on `control.memberships` / `control.audit_events`);
//! 2. lock the target row (`FOR UPDATE`) and, for a mutation that can leave the ACTIVE-OWNER
//!    set, lock the tenant's other ACTIVE OWNER rows too — the last-OWNER count is then
//!    serialized against a concurrent removal instead of being a racy read;
//! 3. `MembershipSnapshot::apply` — the domain machine decides; a refusal leaves the
//!    transaction with no membership write at all;
//! 4. `UPDATE control.memberships` (or the INSERT for an invite);
//! 5. `control.bump_user_security_epoch(user)` when §6.3 requires it (suspend / remove / role
//!    change) — the owner SECURITY DEFINER increment, the only writer of that column;
//! 6. `control.audit_event_insert(...)` — the §77 audit row, same transaction, **whether the
//!    request was applied (`result = SUCCESS`) or refused (`result = DENIED`)**: §77 lists
//!    role change among the high-risk actions and says "全部审计"; a refused last-OWNER
//!    removal is exactly the security event an auditor needs to see, so a refusal commits its
//!    audit row and nothing else.
//!
//! A failure anywhere rolls the whole thing back: the state change and the epoch bump commit
//! together or not at all (the atomicity test in `tests/membership_lifecycle.rs` injects a
//! failing trigger on each of steps 5 and 6 and asserts neither side moved).
//!
//! Every row carries §77's "Sensitive Admin Action 必须包含" seven: actor (`actor_id`), subject
//! tenant/user (`tenant_id`, `metadata.user_id`), reason (`metadata.reason`), request/ticket
//! (`request_id` = the ticket, also `metadata.ticket`), before/after high-level metadata
//! (`before_fingerprint`/`after_fingerprint` = `STATE/ROLE`, plus from/to state and role in
//! `metadata`), `trace_id`, and `metadata.step_up_auth_context` — all supplied by the operator
//! through [`AdminAction`], none defaulted (§78.1).

use humaux_domain::error::{ConflictReason, ErrorCode};
use humaux_domain::identity::{
    MembershipConflict, MembershipMutation, MembershipRole, MembershipSnapshot, MembershipState,
    MembershipTransition,
};
use humaux_domain::ids::{TenantId, UserId};
use serde_json::json;
use sqlx::Row;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use crate::postgres::MaintenanceDbPool;

type Txn<'a> = sqlx::Transaction<'a, sqlx::Postgres>;

/// What the admin path may ask for: create a new `INVITED` row, or move an existing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipRequest {
    /// `INSERT ... state='INVITED'` with this role. The user row must already exist (users
    /// are minted by the §74 identity path, never here).
    Invite(MembershipRole),
    /// A §6.3 transition on the existing row.
    Mutate(MembershipMutation),
}

/// §77 "Sensitive Admin Action 必须包含" — the operator-supplied half of the audit row: who
/// acts, why, under which request/ticket, correlated to which trace, and what step-up
/// authentication gated the action. The admin path has no principal of its own, so none of
/// these can be derived; each must be non-empty (an empty reason is a missing reason, §78.1
/// forbids inventing a default). The other half (subject tenant/user, before/after) the repo
/// takes from the row it locks.
///
/// Not `domain::audit::SensitiveAdminAction`: that type carries the *finished* record
/// (subject + before/after `AuditMetadata`, whose closed key allowlist is `plan` /
/// `previous_role` / `role` and lives outside this card's files), not the operator input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdminAction<'a> {
    pub actor: &'a str,
    pub reason: &'a str,
    /// Request/ticket reference — written as the row's `request_id`.
    pub ticket: &'a str,
    /// Correlation id for the operator's run; `xtask member` accepts one or mints one per
    /// invocation and prints it, so a refused attempt and its retry can be tied together.
    pub trace_id: &'a str,
    /// Opaque evidence of the step-up authentication that gated this action (§77 / §73).
    pub step_up_auth_context: &'a str,
}

impl AdminAction<'_> {
    fn validate(self) -> Result<(), MembershipRepoError> {
        let all_present = [
            self.actor,
            self.reason,
            self.ticket,
            self.trace_id,
            self.step_up_auth_context,
        ]
        .iter()
        .all(|field| !field.trim().is_empty());
        if all_present {
            Ok(())
        } else {
            Err(MembershipRepoError::InvalidInput)
        }
    }
}

/// The committed result of one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipOutcome {
    pub membership_id: Uuid,
    pub state: MembershipState,
    pub role: MembershipRole,
    /// The user's `security_epoch` after this request — `Some` only when the request bumped
    /// it (§6.3: suspend / remove / role change).
    pub user_security_epoch: Option<i64>,
    /// The §77 audit row appended in the same transaction.
    pub audit_event_id: Uuid,
}

/// Why a request did not commit a membership change. `Conflict` and `NotFound` are decided
/// before any membership write and still commit their `DENIED` audit row; `InvalidInput`
/// and `Db` commit nothing.
#[derive(Debug)]
pub enum MembershipRepoError {
    /// The domain machine refused the edge (`ErrorCode::Conflict`).
    Conflict(MembershipConflict),
    /// No membership for `(tenant, user)` (or, for an invite, the user row does not exist).
    NotFound,
    /// A §77 Sensitive-Admin-Action field of [`AdminAction`] is empty.
    InvalidInput,
    /// The database refused / the transaction failed; nothing committed.
    Db(sqlx::Error),
}

impl MembershipRepoError {
    /// The §52 wire code for this failure.
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Self::Conflict(conflict) => conflict.error_code(),
            Self::NotFound => ErrorCode::NotFound,
            Self::InvalidInput => ErrorCode::InvalidInput,
            Self::Db(sqlx::Error::Database(database)) => match database.code().as_deref() {
                Some("42501") => ErrorCode::Forbidden,
                // The only FKs this path can trip are control.tenants (invite / audit row)
                // and control.users: a referenced row that does not exist is NOT_FOUND.
                Some("23503") => ErrorCode::NotFound,
                _ => ErrorCode::DependencyUnavailable,
            },
            Self::Db(_) => ErrorCode::DependencyUnavailable,
        }
    }

    /// Whether this is a decision the request itself was refused with (audited as
    /// `DENIED`), as opposed to a failure of the path (`InvalidInput` / `Db`, no row).
    const fn is_refusal(&self) -> bool {
        matches!(self, Self::Conflict(_) | Self::NotFound)
    }

    /// The `metadata.refusal` spelling of a refused request.
    fn refusal_str(&self) -> &'static str {
        match self {
            Self::Conflict(conflict) => conflict.as_str(),
            Self::NotFound => "NOT_FOUND",
            Self::InvalidInput | Self::Db(_) => "",
        }
    }

    /// The typed §52 D-B `CONFLICT` sub-reason of a refused request, as the numeric code a
    /// client switches on — `metadata.refusal_code` on the §77 audit row, and the number
    /// [`Display`](std::fmt::Display) prints next to the label.
    ///
    /// Card 21 typed [`MembershipConflict::reason`] and card 22 puts it where a reader can see
    /// it. Membership mutation has **no MCP route** — it is an admin-plane path
    /// (`xtask member`, this repo), so there is no `structuredContent.reason` to carry it;
    /// `bins/gateway/src/mcp_application.rs::SUPPORTED_OPERATION_KEYS` names the whole wired
    /// surface and no membership key is on it. The audit row IS the observable surface of a
    /// refused membership mutation, so that is where the code belongs. `NotFound` is
    /// `ErrorCode::NotFound`, not a `CONFLICT`, and correctly has no reason code.
    pub fn conflict_reason(&self) -> Option<ConflictReason> {
        match self {
            Self::Conflict(conflict) => Some(conflict.reason()),
            Self::NotFound | Self::InvalidInput | Self::Db(_) => None,
        }
    }
}

impl From<sqlx::Error> for MembershipRepoError {
    fn from(error: sqlx::Error) -> Self {
        Self::Db(error)
    }
}

impl std::fmt::Display for MembershipRepoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conflict(conflict) => write!(
                f,
                "CONFLICT {} ({})",
                conflict.as_str(),
                conflict.reason().code()
            ),
            Self::NotFound => f.write_str("NOT_FOUND"),
            Self::InvalidInput => f.write_str(
                "INVALID_INPUT: actor, reason, ticket, trace_id and step_up_auth_context must \
                 all be non-empty (§77 Sensitive Admin Action)",
            ),
            Self::Db(error) => write!(f, "db: {error}"),
        }
    }
}

impl std::error::Error for MembershipRepoError {}

/// §77 `AuditEvent.action` spellings for this path, defined once next to their only writer
/// (§78.2). Prefix `MEMBERSHIP_` is what the e2e test counts.
const AUDIT_ACTION_INVITE: &str = "MEMBERSHIP_INVITE";
const AUDIT_ACTION_ACTIVATE: &str = "MEMBERSHIP_ACTIVATE";
const AUDIT_ACTION_SUSPEND: &str = "MEMBERSHIP_SUSPEND";
const AUDIT_ACTION_REMOVE: &str = "MEMBERSHIP_REMOVE";
const AUDIT_ACTION_CHANGE_ROLE: &str = "MEMBERSHIP_CHANGE_ROLE";
const AUDIT_ACTOR_TYPE: &str = "ADMIN";
const AUDIT_RESOURCE_TYPE: &str = "membership";
/// §77 `result`: the request was applied / the request was refused (same spellings as the
/// other audit writers and `ops.data_disclosures.outcome`).
const AUDIT_RESULT_SUCCESS: &str = "SUCCESS";
const AUDIT_RESULT_DENIED: &str = "DENIED";
const AUDIT_RISK_TAG: &str = "membership_lifecycle";
const AUDIT_RISK_TAG_EPOCH: &str = "security_epoch_bump";
/// 0041's sentinel for a `NOT NULL` §77 field with no value on this request: the admin path
/// has no HTTP user agent, and a refused / not-found request has no resource row.
const AUDIT_ABSENT: &str = "";

const fn audit_action(request: MembershipRequest) -> &'static str {
    match request {
        MembershipRequest::Invite(_) => AUDIT_ACTION_INVITE,
        MembershipRequest::Mutate(MembershipMutation::Activate) => AUDIT_ACTION_ACTIVATE,
        MembershipRequest::Mutate(MembershipMutation::Suspend) => AUDIT_ACTION_SUSPEND,
        MembershipRequest::Mutate(MembershipMutation::Remove) => AUDIT_ACTION_REMOVE,
        MembershipRequest::Mutate(MembershipMutation::ChangeRole(_)) => AUDIT_ACTION_CHANGE_ROLE,
    }
}

async fn set_tenant_local(txn: &mut Txn<'_>, tenant_id: TenantId) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT set_config('humaux.tenant_id', $1, true)")
        .bind(tenant_id.0.to_string())
        .execute(&mut **txn)
        .await?;
    Ok(())
}

/// `before`/`after` fingerprint shape: `STATE/ROLE`, human-readable in the audit row.
fn fingerprint(state: MembershipState, role: MembershipRole) -> String {
    format!("{}/{}", state.as_db_str(), role.as_db_str())
}

/// What steps 2–5 committed to the transaction (not yet audited).
struct Applied {
    membership_id: Uuid,
    before: Option<MembershipSnapshot>,
    after: MembershipTransition,
    user_security_epoch: Option<i64>,
}

/// Why steps 2–5 stopped, plus what the audit row can still say about the target row.
struct Stopped {
    error: MembershipRepoError,
    membership_id: Option<Uuid>,
    before: Option<MembershipSnapshot>,
}

impl From<sqlx::Error> for Stopped {
    fn from(error: sqlx::Error) -> Self {
        Self {
            error: MembershipRepoError::Db(error),
            membership_id: None,
            before: None,
        }
    }
}

impl From<MembershipRepoError> for Stopped {
    fn from(error: MembershipRepoError) -> Self {
        Self {
            error,
            membership_id: None,
            before: None,
        }
    }
}

/// Applies one request for `(tenant, user)` on behalf of `admin`.
pub async fn apply(
    pool: &MaintenanceDbPool,
    tenant_id: TenantId,
    user_id: UserId,
    request: MembershipRequest,
    admin: AdminAction<'_>,
) -> Result<MembershipOutcome, MembershipRepoError> {
    admin.validate()?;
    // dep: PostgreSQL(any) — opens a PostgreSQL transaction
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, tenant_id).await?;

    let applied = match decide_and_write(&mut txn, tenant_id, user_id, request).await {
        Ok(applied) => applied,
        Err(stopped) if stopped.error.is_refusal() => {
            // §77 "全部审计": the refusal is the event. Nothing else is in the transaction
            // (Conflict / NotFound are decided before any membership write), so committing
            // here commits exactly one DENIED row. An unknown tenant makes this insert trip
            // the control.tenants FK — NOT_FOUND via `error_code`, with no tenant to audit
            // into (the §77 system-tenant fallback is for unauthenticated security events,
            // not for an operator typo).
            audit_row(
                &mut txn,
                tenant_id,
                user_id,
                stopped.membership_id,
                request,
                stopped.before,
                Outcome::Refused(&stopped.error),
                admin,
            )
            .await?;
            txn.commit().await?;
            return Err(stopped.error);
        }
        Err(stopped) => return Err(stopped.error),
    };

    let audit_event_id = audit_row(
        &mut txn,
        tenant_id,
        user_id,
        Some(applied.membership_id),
        request,
        applied.before,
        Outcome::Applied {
            after: applied.after,
            user_security_epoch: applied.user_security_epoch,
        },
        admin,
    )
    .await?;

    txn.commit().await?;
    Ok(MembershipOutcome {
        membership_id: applied.membership_id,
        state: applied.after.state,
        role: applied.after.role,
        user_security_epoch: applied.user_security_epoch,
        audit_event_id,
    })
}

/// Steps 2–5 of the module doc. Every `Conflict` / `NotFound` returned here is decided
/// before any membership write; the two "cannot happen" post-write misses (a row locked
/// `FOR UPDATE` vanishing, a user row missing despite the FK) surface as `Db`, so a refusal
/// can never carry a half-applied transaction into its audit-and-commit.
async fn decide_and_write(
    txn: &mut Txn<'_>,
    tenant_id: TenantId,
    user_id: UserId,
    request: MembershipRequest,
) -> Result<Applied, Stopped> {
    let (membership_id, before, after) = match request {
        MembershipRequest::Invite(role) => {
            let membership_id = invite_row(txn, tenant_id, user_id, role).await?;
            (
                membership_id,
                None,
                MembershipTransition {
                    state: MembershipState::Invited,
                    role,
                    bumps_security_epoch: false,
                },
            )
        }
        MembershipRequest::Mutate(mutation) => {
            let (membership_id, snapshot) =
                lock_snapshot(txn, tenant_id, user_id, mutation).await?;
            let after = snapshot.apply(mutation).map_err(|conflict| Stopped {
                error: MembershipRepoError::Conflict(conflict),
                membership_id: Some(membership_id),
                before: Some(snapshot),
            })?;
            let updated = sqlx::query(
                "UPDATE control.memberships SET state = $3, role = $4, updated_at = now() \
                 WHERE membership_id = $1 AND tenant_id = $2",
            )
            .bind(membership_id)
            .bind(tenant_id.0)
            .bind(after.state.as_db_str())
            .bind(after.role.as_db_str())
            .execute(&mut **txn)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(sqlx::Error::RowNotFound.into());
            }
            (membership_id, Some(snapshot), after)
        }
    };

    let user_security_epoch = if after.bumps_security_epoch {
        Some(
            sqlx::query_scalar::<_, Option<i64>>("SELECT control.bump_user_security_epoch($1)")
                .bind(user_id.0)
                .fetch_one(&mut **txn)
                .await?
                .ok_or(sqlx::Error::RowNotFound)?,
        )
    } else {
        None
    };

    Ok(Applied {
        membership_id,
        before,
        after,
        user_security_epoch,
    })
}

async fn invite_row(
    txn: &mut Txn<'_>,
    tenant_id: TenantId,
    user_id: UserId,
    role: MembershipRole,
) -> Result<Uuid, Stopped> {
    let user_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM control.users WHERE user_id = $1)")
            .bind(user_id.0)
            .fetch_one(&mut **txn)
            .await?;
    if !user_exists {
        return Err(MembershipRepoError::NotFound.into());
    }
    // UNIQUE (tenant_id, user_id): a second invite (including one for a REMOVED row — REMOVED
    // is terminal, §6.3) is refused as already-in-state, not turned into a second row.
    let inserted: Option<Uuid> = sqlx::query_scalar(
        "INSERT INTO control.memberships (tenant_id, user_id, role, state) \
         VALUES ($1, $2, $3, $4) ON CONFLICT (tenant_id, user_id) DO NOTHING \
         RETURNING membership_id",
    )
    .bind(tenant_id.0)
    .bind(user_id.0)
    .bind(role.as_db_str())
    .bind(MembershipState::Invited.as_db_str())
    .fetch_optional(&mut **txn)
    .await?;
    if let Some(membership_id) = inserted {
        return Ok(membership_id);
    }
    // Refusal path only: name the existing row in the DENIED audit row.
    let existing: Option<Uuid> = sqlx::query_scalar(
        "SELECT membership_id FROM control.memberships WHERE tenant_id = $1 AND user_id = $2",
    )
    .bind(tenant_id.0)
    .bind(user_id.0)
    .fetch_optional(&mut **txn)
    .await?;
    Err(Stopped {
        error: MembershipRepoError::Conflict(MembershipConflict::AlreadyInState),
        membership_id: existing,
        before: None,
    })
}

/// Locks the target row and reads the snapshot the machine judges. The other-ACTIVE-OWNER
/// count is taken under `FOR UPDATE` on those rows too, so two concurrent "remove an owner"
/// requests serialize and the second one sees the first one's result.
async fn lock_snapshot(
    txn: &mut Txn<'_>,
    tenant_id: TenantId,
    user_id: UserId,
    mutation: MembershipMutation,
) -> Result<(Uuid, MembershipSnapshot), MembershipRepoError> {
    let row = sqlx::query(
        "SELECT membership_id, state, role FROM control.memberships \
         WHERE tenant_id = $1 AND user_id = $2 FOR UPDATE",
    )
    .bind(tenant_id.0)
    .bind(user_id.0)
    .fetch_optional(&mut **txn)
    .await?
    .ok_or(MembershipRepoError::NotFound)?;
    let membership_id: Uuid = row.try_get("membership_id")?;
    let state: String = row.try_get("state")?;
    let role: String = row.try_get("role")?;
    // A spelling outside the closed set cannot exist (CHECK constraints); fail closed anyway.
    let state = MembershipState::from_db_str(&state).ok_or(MembershipRepoError::NotFound)?;
    let role = MembershipRole::from_db_str(&role).ok_or(MembershipRepoError::NotFound)?;
    // Only a mutation that can take this row out of the ACTIVE-OWNER set needs the count —
    // activation never does, and locking every owner row for it would only add contention.
    let other_active_owners = if matches!(mutation, MembershipMutation::Activate) {
        0
    } else {
        let n: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM (SELECT 1 FROM control.memberships \
             WHERE tenant_id = $1 AND user_id <> $2 AND role = $3 AND state = $4 \
             FOR UPDATE) AS owners",
        )
        .bind(tenant_id.0)
        .bind(user_id.0)
        .bind(MembershipRole::Owner.as_db_str())
        .bind(MembershipState::Active.as_db_str())
        .fetch_one(&mut **txn)
        .await?;
        u32::try_from(n).unwrap_or(u32::MAX)
    };
    Ok((
        membership_id,
        MembershipSnapshot {
            state,
            role,
            other_active_owners,
        },
    ))
}

/// What the audit row records as the request's `result`.
enum Outcome<'a> {
    Applied {
        after: MembershipTransition,
        user_security_epoch: Option<i64>,
    },
    Refused(&'a MembershipRepoError),
}

#[allow(clippy::too_many_arguments)] // one audit row = these facts, in one place
async fn audit_row(
    txn: &mut Txn<'_>,
    tenant_id: TenantId,
    user_id: UserId,
    membership_id: Option<Uuid>,
    request: MembershipRequest,
    before: Option<MembershipSnapshot>,
    outcome: Outcome<'_>,
    admin: AdminAction<'_>,
) -> Result<Uuid, MembershipRepoError> {
    let mut risk_tags = vec![AUDIT_RISK_TAG.to_string()];
    let (result, after, user_security_epoch, refusal, refusal_code) = match outcome {
        Outcome::Applied {
            after,
            user_security_epoch,
        } => {
            if after.bumps_security_epoch {
                risk_tags.push(AUDIT_RISK_TAG_EPOCH.to_string());
            }
            (
                AUDIT_RESULT_SUCCESS,
                Some(after),
                user_security_epoch,
                None,
                None,
            )
        }
        // Card 22: the label AND the typed §52 D-B code. Both come from the one
        // `ConflictReason` table (§78.2) — `refusal_str` is already derived from
        // `reason().label()`, so the row can never carry a label and a code that disagree.
        Outcome::Refused(error) => (
            AUDIT_RESULT_DENIED,
            None,
            None,
            Some(error.refusal_str()),
            error.conflict_reason().map(ConflictReason::code),
        ),
    };
    let requested_role = match request {
        MembershipRequest::Invite(role)
        | MembershipRequest::Mutate(MembershipMutation::ChangeRole(role)) => Some(role.as_db_str()),
        MembershipRequest::Mutate(_) => None,
    };
    let metadata = json!({
        "user_id": user_id.0.to_string(),
        "from_state": before.map(|b| b.state.as_db_str()),
        "to_state": after.map(|a| a.state.as_db_str()),
        "from_role": before.map(|b| b.role.as_db_str()),
        "to_role": after.map(|a| a.role.as_db_str()),
        "requested_role": requested_role,
        "user_security_epoch": user_security_epoch,
        "refusal": refusal,
        "refusal_code": refusal_code,
        "reason": admin.reason,
        "ticket": admin.ticket,
        "step_up_auth_context": admin.step_up_auth_context,
    });
    let event_id: Uuid = sqlx::query_scalar(
        "SELECT control.audit_event_insert( \
           $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULL::inet, $12, $13, $14, $15, $16)",
    )
    .bind(Uuid::now_v7())
    .bind(OffsetDateTime::now_utc())
    .bind(tenant_id.0)
    .bind(AUDIT_ACTOR_TYPE)
    .bind(admin.actor)
    .bind(audit_action(request))
    .bind(AUDIT_RESOURCE_TYPE)
    .bind(membership_id.map_or_else(|| AUDIT_ABSENT.to_string(), |id| id.to_string()))
    .bind(result)
    .bind(admin.ticket)
    .bind(admin.trace_id)
    .bind(AUDIT_ABSENT)
    .bind(&risk_tags)
    .bind(before.map(|b| fingerprint(b.state, b.role)))
    .bind(after.map(|a| fingerprint(a.state, a.role)))
    .bind(metadata)
    .fetch_one(&mut **txn)
    .await?;
    Ok(event_id)
}
