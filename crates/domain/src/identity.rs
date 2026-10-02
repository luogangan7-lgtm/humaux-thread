//! `domain::identity` — `AuthorizationScope`, `VisibilityDescriptor`, and the sole visibility judge `can_read`
//!   (§6.1.1).
//! Depends-on: crates=[uuid]; services=[]; env=[]; modules=[domain::error, domain::ids]
//! Called-by: [adapters::affect_repo, adapters::confirm_token_repo, adapters::context_repo, adapters::continuity_read, adapters::continuity_repo, adapters::contribution_entry_repo, adapters::credential_repo, adapters::distill_repo, adapters::exact_census, adapters::mechanism_observation, adapters::membership_repo, adapters::memory_governance_repo, adapters::operation_receipt, adapters::private_projection_registry, adapters::projection_worker, adapters::provisioning, adapters::public_repo, adapters::qdrant, adapters::quota_repo, adapters::read_materialize, adapters::request_guard_repo, adapters::retrieval_query_source, adapters::retrieve, adapters::serving_repo, adapters::stream_repo, adapters::subject_repo, application::continuity, application::contribute, application::retrieval_embedding_port, gateway::auth, gateway::bootstrap, gateway::context, gateway::continuity, gateway::guard, gateway::mcp_application, gateway::memory, gateway::recall, gateway::remember, maintenance::main, projection::dense, projection::sparse, retrieval-worker::rpc, tests, xtask::member]
//! Invariants: []
//! Spec: Baseline §6; §6.1; §6.1.1; ADR-0033; ADR-0035
//!
//! §6.1.1 freezes: tenant isolation (PostgreSQL RLS, `SET LOCAL humaux.tenant_id/user_id`,
//! §6.1/§62) is not the end of multi-user isolation — inside one tenant, three visibility
//! classes exist (`USER_PRIVATE`/`WORKSPACE_SHARED`/`TENANT_SHARED`), and exactly one
//! function in the whole workspace decides whether a scope can see an object of a given
//! class: [`can_read`]. Every read-time adapter (PG RLS/SQL, Qdrant payload filter,
//! Object/Artifact authorization, Graph/Code association expansion) must route through the
//! same predicate — "业务 Adapter 不允许各自维护一套'差不多相同'的可见性条件" — so this
//! module is that predicate's one home, not a template each adapter re-derives.
//!
//! `can_read` alone is not a complete read-time filter: per §6.1.2 the real predicate is
//! `tenant filter AND authorized visibility disjunction AND query-specific narrower filters`,
//! and `can_read` implements only the middle term. Every caller must independently AND in its
//! own tenant-equality filter — see [`can_read`]'s doc for why.
//!
//! `AuthorizationScope`'s fields are private by design: the spec's own words are "请求得到
//! 一个不可由模型构造的" `AuthorizationScope` — the LLM/tool-call layer never builds one
//! directly, only the trusted authentication/authorization layer does, through [`AuthorizationScope::new`].
//! From there, an MCP tool argument may only reach [`AuthorizationScope::narrow`], which can
//! shrink the scope but is structurally unable to grow it (§6.1.1: "workspace_id、user_id
//! 若来自 MCP Tool 参数，只能进一步缩小 Auth Scope，绝不能扩大它").

use crate::error::{ConflictReason, ErrorCode};
use crate::ids::{TenantId, UserId, WorkspaceId};
use uuid::Uuid;

/// Principal id — identifies who/what is making the request (a `User` or a headless
/// `Agent`/service credential; §6/§83's `RequestContext.principal: Principal`).
///
/// Minted locally rather than folded into `domain::ids`'s frozen seven (§59): this task's
/// assigned file scope is `crates/domain/src/identity.rs` only, and `PrincipalId` is not
/// among that fixed list. Mint/parse shape hand-matches `ids::uuid_newtype!` (that macro is
/// private to `ids.rs`, and exporting it is a change outside this task's assigned files) —
/// same deviation already taken by `domain::authority`'s `MemoryId`/`EvidenceId` for the
/// identical reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PrincipalId(pub Uuid);

#[allow(clippy::new_without_default)] // see ids::uuid_newtype!'s identical note
impl PrincipalId {
    /// Mints a new id (UUIDv7, §49).
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Parses an existing UUID string; a malformed input is `INVALID_INPUT` (§52.1).
    pub fn parse(s: &str) -> Result<Self, ErrorCode> {
        Uuid::parse_str(s)
            .map(Self)
            .map_err(|_| ErrorCode::InvalidInput)
    }
}

/// A `WorkspaceId` collection capped at a fixed maximum, so one principal's workspace grant
/// list can never grow unbounded (§6.1.1's `AuthorizationScope.allowed_workspace_ids`).
///
/// ponytail: `MAX_LEN` is a fixed constant, not a per-tenant config knob — promote it to a
/// constructor parameter if a principal ever legitimately needs more concurrent workspace
/// grants than this.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedSet<T> {
    items: Vec<T>,
}

impl<T: PartialEq> BoundedSet<T> {
    /// Ceiling on distinct members (see the ponytail note on the type itself).
    pub const MAX_LEN: usize = 256;

    /// Builds a set from `items`, de-duplicating by `PartialEq`. `INVALID_INPUT` if the
    /// de-duplicated count exceeds [`Self::MAX_LEN`].
    pub fn new(items: impl IntoIterator<Item = T>) -> Result<Self, ErrorCode> {
        let mut out: Vec<T> = Vec::new();
        for item in items {
            if !out.contains(&item) {
                out.push(item);
            }
        }
        if out.len() > Self::MAX_LEN {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self { items: out })
    }

    /// A one-element set. Always within [`Self::MAX_LEN`] (`MAX_LEN` >= 1), so this is
    /// infallible — used by [`AuthorizationScope::narrow`] where a `Result` would only ever
    /// be `Ok`.
    fn singleton(item: T) -> Self {
        Self { items: vec![item] }
    }

    /// Whether `item` is a member.
    pub fn contains(&self, item: &T) -> bool {
        self.items.contains(item)
    }

    /// Number of distinct members.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether the set has no members.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Iterates members in insertion order (post de-dup). Added for T5.5's query adapters
    /// (`projection::dense`/`projection::sparse`, §17.1/§17.2): a static per-request Qdrant
    /// filter must enumerate every allowed workspace id to build a `WORKSPACE_SHARED`
    /// `MatchAny`/`IN` clause — `contains`/`len` alone cannot do that.
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.items.iter()
    }
}

/// The three intra-tenant visibility classes (§6.1.1), frozen closed set — no `Other`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisibilityClass {
    /// Visible only to one specific user.
    UserPrivate,
    /// Visible to members of one specific workspace.
    WorkspaceShared,
    /// Visible tenant-wide.
    TenantShared,
}

/// An object's visibility, as read off the row being checked (§6.1.1). `user_id`/`workspace_id`
/// are only meaningful for the matching `class` — [`can_read`] treats a class/id mismatch
/// (e.g. `UserPrivate` with `user_id: None`) as a malformed descriptor and fails closed
/// (`false`), it never panics on it. Row-level `CHECK` constraints (§48.0) are the place that
/// rejects such rows at write time; this type does not re-validate that invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisibilityDescriptor {
    pub class: VisibilityClass,
    pub user_id: Option<UserId>,
    pub workspace_id: Option<WorkspaceId>,
}

/// A request's authorization scope (§6.1.1) — tenant, the acting principal, the on-behalf-of
/// user if any, and the bounded set of workspaces the principal is currently authorized into.
/// Fields are private: only [`AuthorizationScope::new`] (the trusted authn/authz layer) and
/// [`AuthorizationScope::narrow`] (shrink-only, for MCP tool arguments) construct one — see
/// the module doc for why that matters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationScope {
    tenant_id: TenantId,
    principal: PrincipalId,
    user_id: Option<UserId>,
    allowed_workspace_ids: BoundedSet<WorkspaceId>,
}

impl AuthorizationScope {
    /// Constructs a scope. Callable only by the trusted authentication/authorization layer
    /// (§6.1.1) — never expose this to MCP tool-argument-driven code paths; those must go
    /// through [`Self::narrow`] on an already-constructed scope instead.
    pub fn new(
        tenant_id: TenantId,
        principal: PrincipalId,
        user_id: Option<UserId>,
        allowed_workspace_ids: BoundedSet<WorkspaceId>,
    ) -> Self {
        Self {
            tenant_id,
            principal,
            user_id,
            allowed_workspace_ids,
        }
    }

    /// The tenant this scope is bound to — the boundary `can_read`/`TENANT_SHARED` does not
    /// itself check (see [`can_read`]'s doc); callers use this to apply the separate tenant
    /// filter (§6.1.2).
    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    /// The acting principal (the authenticated user or headless agent/service credential).
    pub fn principal(&self) -> PrincipalId {
        self.principal
    }

    /// The on-behalf-of user, if any (`None` for a service/agent scope acting on no
    /// particular user).
    pub fn user_id(&self) -> Option<UserId> {
        self.user_id
    }

    /// The bounded set of workspaces this scope is currently authorized into. For a
    /// request scope this is built per request by the authentication layer (ADR-0035,
    /// §6.1.1): the on-behalf-of user's live ACTIVE WorkspaceMembership set
    /// (`control.workspace_memberships`, not tenant membership) intersected with the
    /// credential's optional bound workspace — never the credential's baked column alone,
    /// and never a union.
    pub fn allowed_workspace_ids(&self) -> &BoundedSet<WorkspaceId> {
        &self.allowed_workspace_ids
    }

    /// Narrows this scope to (at most) the single workspace `requested` (§6.1.1: an MCP tool
    /// argument may only shrink `AuthorizationScope`, never grow it). `Forbidden` if
    /// `requested` was not already inside the current scope — i.e. the caller tried to reach
    /// a workspace it was not already authorized into, which would be widening, not
    /// narrowing.
    ///
    /// §6.1.1 states the same shrink-only rule for `user_id` MCP-tool-argument narrowing; no
    /// `narrow_user` exists here because nothing in the current workspace constructs a scope
    /// from an MCP argument that targets `user_id` — `user_id` is only ever set by
    /// [`Self::new`]'s trusted authn/authz caller. Add a `narrow_user` with this same
    /// contains-then-shrink shape if/when a task needs an "act on behalf of user X, narrowed
    /// from an agent/service scope" MCP argument path.
    pub fn narrow(&self, requested: WorkspaceId) -> Result<Self, ErrorCode> {
        if !self.allowed_workspace_ids.contains(&requested) {
            return Err(ErrorCode::Forbidden);
        }
        Ok(Self {
            allowed_workspace_ids: BoundedSet::singleton(requested),
            ..self.clone()
        })
    }
}

/// The sole visibility judge for the whole workspace (§6.1.1) — every read-time adapter (PG
/// RLS/SQL, Qdrant payload filter, Object/Artifact authorization, Graph/Code association
/// expansion) must reduce its visibility check to this predicate, never re-derive one.
///
/// `TENANT_SHARED` reads as "同 tenant 且 principal 有 tenant read permission": this crate's
/// `AuthorizationScope` carries no separate permission bit, so tenant-read permission is
/// established by scope construction itself — [`AuthorizationScope::new`] is the trusted
/// authn/authz layer's job, and it must not hand out a scope for a tenant the principal
/// cannot at least tenant-read. Likewise `WORKSPACE_SHARED`'s "membership/role 允许" is baked
/// into which ids ended up in `allowed_workspace_ids` at construction time, not re-checked
/// here.
///
/// **`can_read` never checks tenant match.** `VisibilityDescriptor` carries no `tenant_id` at
/// all, so for `TENANT_SHARED` this function structurally cannot and does not verify "同
/// tenant" — it always returns `true` for that arm. This is by design, not an oversight:
/// §6.1.2 defines the real read-time predicate as three independently-`AND`ed terms —
/// `tenant filter AND authorized visibility disjunction AND query-specific narrower filters`
/// — and `can_read` implements only the middle term (the visibility disjunction). **Every
/// caller MUST additionally apply its own `object.tenant_id == scope.tenant_id()` filter
/// before/around this check** (e.g. a `WHERE tenant_id = ...` clause, a Qdrant tenant filter).
/// Passing a cross-tenant object straight to `can_read` for a `TENANT_SHARED` row will
/// incorrectly return `true`.
pub fn can_read(scope: &AuthorizationScope, object: &VisibilityDescriptor) -> bool {
    match object.class {
        VisibilityClass::TenantShared => true,
        VisibilityClass::UserPrivate => match object.user_id {
            Some(uid) => scope.user_id == Some(uid),
            None => false,
        },
        VisibilityClass::WorkspaceShared => match object.workspace_id {
            Some(wid) => scope.allowed_workspace_ids.contains(&wid),
            None => false,
        },
    }
}

// ============================================================================
// §6.3 MembershipState machine (ADR-0033, card 12)
// ============================================================================

/// §6.3 `MembershipState` — the closed four-state set `INVITED / ACTIVE / SUSPENDED /
/// REMOVED`, spelled once here (§78.2) in the exact form `control.memberships.state`'s
/// CHECK constraint (migration 0003) stores it. `REMOVED` is terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MembershipState {
    /// Row exists, the user has not accepted / been activated yet.
    Invited,
    /// Full member.
    Active,
    /// Access withheld (security/compliance/admin policy); data retained.
    Suspended,
    /// Terminal: the membership is gone; no transition leaves this state.
    Removed,
}

impl MembershipState {
    /// Every state, database spelling order.
    pub const ALL: [MembershipState; 4] =
        [Self::Invited, Self::Active, Self::Suspended, Self::Removed];

    /// The stored spelling (`control.memberships.state` CHECK, migration 0003).
    pub const fn as_db_str(self) -> &'static str {
        match self {
            Self::Invited => "INVITED",
            Self::Active => "ACTIVE",
            Self::Suspended => "SUSPENDED",
            Self::Removed => "REMOVED",
        }
    }

    /// Parses the stored spelling; anything outside the closed set is `None` (a row the
    /// CHECK constraint would never have admitted — the caller fails closed, §78.2).
    pub fn from_db_str(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|s| s.as_db_str() == value)
    }
}

/// §6.3 / migration 0160 `control.memberships.role` closed set `OWNER | ADMIN | MEMBER`.
/// `Owner` is the role the last-OWNER rule (§6.3 Ownership / Offboarding) protects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MembershipRole {
    /// Holds tenant-level authority; a tenant must keep at least one ACTIVE owner.
    Owner,
    /// Tenant administration without the ownership invariant.
    Admin,
    /// Plain member.
    Member,
}

impl MembershipRole {
    /// Every role, database spelling order.
    pub const ALL: [MembershipRole; 3] = [Self::Owner, Self::Admin, Self::Member];

    /// The stored spelling (`memberships_role_known` CHECK, migration 0160).
    pub const fn as_db_str(self) -> &'static str {
        match self {
            Self::Owner => "OWNER",
            Self::Admin => "ADMIN",
            Self::Member => "MEMBER",
        }
    }

    /// Parses the stored spelling (case-sensitive: 0160 canonicalizes on write, so the
    /// column only ever holds these three spellings); unknown ⇒ `None`.
    pub fn from_db_str(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|r| r.as_db_str() == value)
    }
}

/// One requested §6.3 membership mutation, as the admin path (`xtask member`) issues it.
/// `Invite` is not here: it creates the row (`INVITED`) rather than transitioning one, and
/// has no "from" state to judge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipMutation {
    /// `INVITED → ACTIVE` (accept) or `SUSPENDED → ACTIVE` (reinstate).
    Activate,
    /// `ACTIVE → SUSPENDED`.
    Suspend,
    /// `INVITED | ACTIVE | SUSPENDED → REMOVED` (terminal).
    Remove,
    /// Role change on an `ACTIVE` membership; state unchanged.
    ChangeRole(MembershipRole),
}

/// Why a [`MembershipMutation`] was refused by the type (never by the database — the CHECK
/// constraints only close the value sets, the machine lives here). All three surface as
/// `ErrorCode::Conflict` (§52: the object is in a state that refuses the request).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipConflict {
    /// The membership is already in the requested state / already holds the role.
    AlreadyInState,
    /// The machine has no such edge (e.g. anything out of `REMOVED`, `INVITED → SUSPENDED`).
    TransitionNotAllowed,
    /// §6.3 "last OWNER cannot silently leave": the mutation would leave the tenant with no
    /// `ACTIVE` `OWNER`. Transfer ownership first (promote another ACTIVE member to OWNER).
    LastOwner,
}

impl MembershipConflict {
    /// The wire error code every membership conflict maps to.
    pub const fn error_code(self) -> ErrorCode {
        ErrorCode::Conflict
    }

    /// The typed `CONFLICT` sub-reason (§52 D-B) this refusal is — the numeric code the
    /// gateway surfaces in `structuredContent {code:"CONFLICT", reason:<u16>}`.
    ///
    /// Card 21: before this, membership refusals carried their own SCREAMING_SNAKE strings and
    /// no code at all, while `ConflictReason` was the closed set that owns exactly that
    /// taxonomy — two registries for one wire contract, the second of which the client could
    /// not switch on. `AlreadyInState` reuses the generic `ALREADY_IN_STATE` (1201) rather than
    /// minting a membership-specific twin; the other two are §6.3's own edges and get 1203/1204.
    pub const fn reason(self) -> ConflictReason {
        match self {
            Self::AlreadyInState => ConflictReason::ALREADY_IN_STATE,
            Self::TransitionNotAllowed => ConflictReason::TRANSITION_NOT_ALLOWED,
            Self::LastOwner => ConflictReason::LAST_OWNER,
        }
    }

    /// SCREAMING_SNAKE spelling — what the §77 audit row of a refused membership mutation
    /// records as its `refusal`, and what `Display` prints.
    ///
    /// **Derived** from [`Self::reason`]'s label (§78.2: defined once). It used to be a second
    /// `match` with the same three strings typed again; the labels happened to agree, which is
    /// the only reason nobody noticed there were two tables. `expect` cannot fire: every arm of
    /// `reason` returns a `ConflictReason` this crate defines, and
    /// `conflict_reason_codes_and_labels_are_frozen_and_unique` proves every defined reason has
    /// a label.
    pub fn as_str(self) -> &'static str {
        self.reason()
            .label()
            .expect("every MembershipConflict maps to a defined ConflictReason")
    }
}

/// The current row the machine judges, plus the one fact outside the row the last-OWNER
/// rule needs: how many *other* memberships of the same tenant are `ACTIVE` `OWNER`s.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MembershipSnapshot {
    pub state: MembershipState,
    pub role: MembershipRole,
    /// `ACTIVE` owners of the tenant *excluding* this membership.
    pub other_active_owners: u32,
}

/// The row after a permitted mutation, plus whether §6.3 requires a security-epoch bump for
/// it ("membership removed/suspended" and "role/security-sensitive policy changed" ⇒ bump;
/// invite/activate do not — bumping on activation would also revoke the user's live
/// credentials in *other* tenants for no security reason).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MembershipTransition {
    pub state: MembershipState,
    pub role: MembershipRole,
    pub bumps_security_epoch: bool,
}

impl MembershipSnapshot {
    /// Whether this membership currently counts toward the tenant's ACTIVE-OWNER set.
    const fn is_active_owner(self) -> bool {
        matches!(self.state, MembershipState::Active) && matches!(self.role, MembershipRole::Owner)
    }

    /// §6.3 machine: judges `mutation` against this snapshot. Pure; the adapter applies the
    /// returned row inside the same transaction as the epoch bump and the audit row.
    pub fn apply(
        self,
        mutation: MembershipMutation,
    ) -> Result<MembershipTransition, MembershipConflict> {
        use MembershipConflict::{AlreadyInState, LastOwner, TransitionNotAllowed};
        use MembershipState::{Active, Invited, Removed, Suspended};
        let (state, role, bumps) = match (self.state, mutation) {
            (Removed, _) => return Err(TransitionNotAllowed),
            (Active, MembershipMutation::Activate) => return Err(AlreadyInState),
            (Invited | Suspended, MembershipMutation::Activate) => (Active, self.role, false),
            (Suspended, MembershipMutation::Suspend) => return Err(AlreadyInState),
            (Invited, MembershipMutation::Suspend) => return Err(TransitionNotAllowed),
            (Active, MembershipMutation::Suspend) => (Suspended, self.role, true),
            (Invited | Active | Suspended, MembershipMutation::Remove) => {
                (Removed, self.role, true)
            }
            (Invited | Suspended, MembershipMutation::ChangeRole(_)) => {
                return Err(TransitionNotAllowed);
            }
            (Active, MembershipMutation::ChangeRole(role)) if role == self.role => {
                return Err(AlreadyInState);
            }
            (Active, MembershipMutation::ChangeRole(role)) => (Active, role, true),
        };
        // Last-OWNER rule: leaving the ACTIVE-OWNER set is only allowed when someone else is
        // still in it. Only a membership that is in the set now can leave it.
        let leaves_owner_set =
            self.is_active_owner() && !(state == Active && role == MembershipRole::Owner);
        if leaves_owner_set && self.other_active_owners == 0 {
            return Err(LastOwner);
        }
        Ok(MembershipTransition {
            state,
            role,
            bumps_security_epoch: bumps,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Card 21 (folded card-12 debt): every §6.3 membership refusal carries a TYPED
    /// `ConflictReason`, and its audit/wire label is DERIVED from that reason rather than
    /// typed a second time. Two registries for one taxonomy is how a wire contract grows a
    /// second, un-switchable spelling.
    ///
    /// Fault injection: give `MembershipConflict::LastOwner` a `ConflictReason` this crate
    /// does not define and `as_str` panics here; change `ConflictReason::LAST_OWNER`'s label
    /// and the expected string below moves with it — which is the point, there is now one
    /// place to change.
    #[test]
    fn every_membership_conflict_has_a_distinct_typed_reason_and_a_derived_label() {
        const ALL: [MembershipConflict; 3] = [
            MembershipConflict::AlreadyInState,
            MembershipConflict::TransitionNotAllowed,
            MembershipConflict::LastOwner,
        ];
        let mut codes = std::collections::HashSet::new();
        for conflict in ALL {
            let reason = conflict.reason();
            assert!(
                crate::error::ConflictReason::ALL.contains(&reason),
                "{conflict:?} maps to an unregistered reason {reason:?}"
            );
            assert!(codes.insert(reason.code()), "{conflict:?} reuses a code");
            assert_eq!(
                conflict.as_str(),
                reason.label().expect("registered reason has a label"),
                "the audit label must be derived from the reason, not spelled twice"
            );
            assert_eq!(conflict.error_code(), ErrorCode::Conflict);
        }
        // The frozen codes themselves (§52 D-B: the wire contract, not an implementation note).
        assert_eq!(MembershipConflict::AlreadyInState.reason().code(), 1201);
        assert_eq!(
            MembershipConflict::TransitionNotAllowed.reason().code(),
            1203
        );
        assert_eq!(MembershipConflict::LastOwner.reason().code(), 1204);
        // …and the labels callers and §77 audit rows already depend on are unchanged.
        assert_eq!(MembershipConflict::LastOwner.as_str(), "LAST_OWNER");
        assert_eq!(
            MembershipConflict::TransitionNotAllowed.as_str(),
            "TRANSITION_NOT_ALLOWED"
        );
        assert_eq!(
            MembershipConflict::AlreadyInState.as_str(),
            "ALREADY_IN_STATE"
        );
    }

    fn scope(user_id: Option<UserId>, workspaces: &[WorkspaceId]) -> AuthorizationScope {
        AuthorizationScope::new(
            TenantId::new(),
            PrincipalId::new(),
            user_id,
            BoundedSet::new(workspaces.iter().copied()).unwrap(),
        )
    }

    // ---- can_read: TENANT_SHARED ----

    #[test]
    fn tenant_shared_always_readable() {
        let s = scope(None, &[]);
        let obj = VisibilityDescriptor {
            class: VisibilityClass::TenantShared,
            user_id: None,
            workspace_id: None,
        };
        assert!(can_read(&s, &obj));
    }

    // ---- can_read: USER_PRIVATE ----

    #[test]
    fn user_private_readable_by_matching_user() {
        let uid = UserId::new();
        let s = scope(Some(uid), &[]);
        let obj = VisibilityDescriptor {
            class: VisibilityClass::UserPrivate,
            user_id: Some(uid),
            workspace_id: None,
        };
        assert!(can_read(&s, &obj));
    }

    #[test]
    fn user_private_denied_for_different_user() {
        let s = scope(Some(UserId::new()), &[]);
        let obj = VisibilityDescriptor {
            class: VisibilityClass::UserPrivate,
            user_id: Some(UserId::new()), // a different user's row
            workspace_id: None,
        };
        assert!(!can_read(&s, &obj));
    }

    #[test]
    fn user_private_denied_when_scope_has_no_user() {
        let s = scope(None, &[]); // service/agent scope, no on-behalf-of user
        let obj = VisibilityDescriptor {
            class: VisibilityClass::UserPrivate,
            user_id: Some(UserId::new()),
            workspace_id: None,
        };
        assert!(!can_read(&s, &obj));
    }

    #[test]
    fn user_private_fails_closed_on_malformed_descriptor() {
        let uid = UserId::new();
        let s = scope(Some(uid), &[]);
        let obj = VisibilityDescriptor {
            class: VisibilityClass::UserPrivate,
            user_id: None, // malformed: USER_PRIVATE with no user_id
            workspace_id: None,
        };
        assert!(!can_read(&s, &obj));
    }

    // ---- can_read: WORKSPACE_SHARED ----

    #[test]
    fn workspace_shared_readable_when_workspace_allowed() {
        let wid = WorkspaceId::new();
        let s = scope(None, &[wid]);
        let obj = VisibilityDescriptor {
            class: VisibilityClass::WorkspaceShared,
            user_id: None,
            workspace_id: Some(wid),
        };
        assert!(can_read(&s, &obj));
    }

    #[test]
    fn workspace_shared_denied_when_workspace_not_allowed() {
        let s = scope(None, &[WorkspaceId::new()]);
        let obj = VisibilityDescriptor {
            class: VisibilityClass::WorkspaceShared,
            user_id: None,
            workspace_id: Some(WorkspaceId::new()), // not in scope's allowed set
        };
        assert!(!can_read(&s, &obj));
    }

    #[test]
    fn workspace_shared_fails_closed_on_malformed_descriptor() {
        let wid = WorkspaceId::new();
        let s = scope(None, &[wid]);
        let obj = VisibilityDescriptor {
            class: VisibilityClass::WorkspaceShared,
            user_id: None,
            workspace_id: None, // malformed: WORKSPACE_SHARED with no workspace_id
        };
        assert!(!can_read(&s, &obj));
    }

    // ---- narrow: shrink-only ----

    #[test]
    fn narrow_to_already_allowed_workspace_succeeds() {
        let wid = WorkspaceId::new();
        let other = WorkspaceId::new();
        let s = scope(None, &[wid, other]);

        let narrowed = s.narrow(wid).expect("wid is already in scope");
        assert_eq!(narrowed.allowed_workspace_ids().len(), 1);
        assert!(narrowed.allowed_workspace_ids().contains(&wid));
    }

    #[test]
    fn narrow_rejects_widening_to_an_unauthorized_workspace() {
        let allowed = WorkspaceId::new();
        let unauthorized = WorkspaceId::new();
        let s = scope(None, &[allowed]);

        let err = s
            .narrow(unauthorized)
            .expect_err("must not be able to widen into an unauthorized workspace");
        assert_eq!(err, ErrorCode::Forbidden);
    }

    #[test]
    fn narrow_actually_shrinks_what_can_be_read() {
        let wid_a = WorkspaceId::new();
        let wid_b = WorkspaceId::new();
        let s = scope(None, &[wid_a, wid_b]);
        let narrowed = s.narrow(wid_a).unwrap();

        let obj_b = VisibilityDescriptor {
            class: VisibilityClass::WorkspaceShared,
            user_id: None,
            workspace_id: Some(wid_b),
        };
        // Readable under the wider original scope...
        assert!(can_read(&s, &obj_b));
        // ...but not under the narrowed one.
        assert!(!can_read(&narrowed, &obj_b));
    }

    // ---- BoundedSet::iter ----

    #[test]
    fn bounded_set_iter_yields_every_member_in_insertion_order() {
        let a = WorkspaceId::new();
        let b = WorkspaceId::new();
        let s = BoundedSet::new([a, b]).unwrap();
        assert_eq!(s.iter().copied().collect::<Vec<_>>(), vec![a, b]);
    }

    // ---- BoundedSet cap ----

    #[test]
    fn bounded_set_rejects_over_cap() {
        let ids: Vec<WorkspaceId> = (0..=BoundedSet::<WorkspaceId>::MAX_LEN)
            .map(|_| WorkspaceId::new())
            .collect();
        assert_eq!(BoundedSet::new(ids).unwrap_err(), ErrorCode::InvalidInput);
    }

    // ---- §6.3 MembershipState machine (ADR-0033) ----

    fn snap(state: MembershipState, role: MembershipRole, others: u32) -> MembershipSnapshot {
        MembershipSnapshot {
            state,
            role,
            other_active_owners: others,
        }
    }

    #[test]
    fn membership_closed_sets_round_trip_their_db_spelling() {
        for s in MembershipState::ALL {
            assert_eq!(MembershipState::from_db_str(s.as_db_str()), Some(s));
        }
        for r in MembershipRole::ALL {
            assert_eq!(MembershipRole::from_db_str(r.as_db_str()), Some(r));
        }
        assert_eq!(MembershipState::from_db_str("active"), None);
        assert_eq!(MembershipRole::from_db_str("owner"), None);
    }

    #[test]
    fn membership_legal_edges_and_epoch_bumps() {
        use MembershipMutation::{Activate, ChangeRole, Remove, Suspend};
        use MembershipRole::{Admin, Member, Owner};
        use MembershipState::{Active, Invited, Removed, Suspended};
        let ok = |s, r, o, m| snap(s, r, o).apply(m).expect("legal edge");
        assert_eq!(
            ok(Invited, Member, 0, Activate),
            MembershipTransition {
                state: Active,
                role: Member,
                bumps_security_epoch: false
            }
        );
        assert_eq!(
            ok(Suspended, Member, 0, Activate),
            MembershipTransition {
                state: Active,
                role: Member,
                bumps_security_epoch: false
            }
        );
        assert_eq!(
            ok(Active, Member, 0, Suspend),
            MembershipTransition {
                state: Suspended,
                role: Member,
                bumps_security_epoch: true
            }
        );
        assert_eq!(ok(Invited, Member, 0, Remove).state, Removed);
        assert_eq!(
            ok(Active, Member, 0, Remove),
            MembershipTransition {
                state: Removed,
                role: Member,
                bumps_security_epoch: true
            }
        );
        assert_eq!(ok(Suspended, Owner, 0, Remove).state, Removed);
        assert_eq!(
            ok(Active, Member, 0, ChangeRole(Admin)),
            MembershipTransition {
                state: Active,
                role: Admin,
                bumps_security_epoch: true
            }
        );
        // An owner may leave the owner set when another ACTIVE owner remains.
        assert_eq!(ok(Active, Owner, 1, Remove).state, Removed);
        assert_eq!(ok(Active, Owner, 1, Suspend).state, Suspended);
        assert_eq!(ok(Active, Owner, 1, ChangeRole(Member)).role, Member);
    }

    #[test]
    fn membership_removed_is_terminal_and_illegal_edges_are_typed() {
        use MembershipConflict::{AlreadyInState, TransitionNotAllowed};
        use MembershipMutation::{Activate, ChangeRole, Remove, Suspend};
        use MembershipRole::{Admin, Member};
        use MembershipState::{Active, Invited, Removed, Suspended};
        for m in [Activate, Suspend, Remove, ChangeRole(Admin)] {
            assert_eq!(
                snap(Removed, Member, 5).apply(m),
                Err(TransitionNotAllowed),
                "{m:?}"
            );
        }
        assert_eq!(snap(Active, Member, 0).apply(Activate), Err(AlreadyInState));
        assert_eq!(
            snap(Suspended, Member, 0).apply(Suspend),
            Err(AlreadyInState)
        );
        assert_eq!(
            snap(Invited, Member, 0).apply(Suspend),
            Err(TransitionNotAllowed)
        );
        assert_eq!(
            snap(Invited, Member, 0).apply(ChangeRole(Admin)),
            Err(TransitionNotAllowed)
        );
        assert_eq!(
            snap(Suspended, Member, 0).apply(ChangeRole(Admin)),
            Err(TransitionNotAllowed)
        );
        assert_eq!(
            snap(Active, Member, 0).apply(ChangeRole(Member)),
            Err(AlreadyInState)
        );
        assert_eq!(AlreadyInState.error_code(), ErrorCode::Conflict);
    }

    #[test]
    fn membership_last_active_owner_cannot_leave_the_owner_set() {
        use MembershipConflict::LastOwner;
        use MembershipMutation::{Activate, ChangeRole, Remove, Suspend};
        use MembershipRole::{Member, Owner};
        use MembershipState::{Active, Invited, Suspended};
        let last = snap(Active, Owner, 0);
        assert_eq!(last.apply(Remove), Err(LastOwner));
        assert_eq!(last.apply(Suspend), Err(LastOwner));
        assert_eq!(last.apply(ChangeRole(Member)), Err(LastOwner));
        // Not in the ACTIVE-OWNER set ⇒ the rule does not apply (the tenant is already
        // ownerless or owned by someone else; removing this row changes nothing).
        assert_eq!(
            snap(Suspended, Owner, 0).apply(Remove).map(|t| t.state),
            Ok(MembershipState::Removed)
        );
        assert_eq!(
            snap(Invited, Owner, 0).apply(Remove).map(|t| t.state),
            Ok(MembershipState::Removed)
        );
        assert_eq!(
            snap(Suspended, Owner, 0).apply(Activate).map(|t| t.state),
            Ok(Active)
        );
    }
}
