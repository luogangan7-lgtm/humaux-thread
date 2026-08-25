//! `domain::identity` — `AuthorizationScope`, `VisibilityDescriptor`, and the sole
//! visibility judge `can_read` (§6.1.1).
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

use crate::error::ErrorCode;
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

    /// The bounded set of workspaces this scope is currently authorized into.
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

#[cfg(test)]
mod tests {
    use super::*;

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

    // ---- BoundedSet cap ----

    #[test]
    fn bounded_set_rejects_over_cap() {
        let ids: Vec<WorkspaceId> = (0..=BoundedSet::<WorkspaceId>::MAX_LEN)
            .map(|_| WorkspaceId::new())
            .collect();
        assert_eq!(BoundedSet::new(ids).unwrap_err(), ErrorCode::InvalidInput);
    }
}
