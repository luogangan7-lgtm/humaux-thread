//! `domain::ids` — seven UUIDv7 newtype ids and `Scope` (§59 core Rust types; id policy §49).
//! Depends-on: crates=[uuid]; services=[]; env=[]; modules=[domain::error]
//! Called-by: [adapters::byok, adapters::confirm_token_repo, adapters::consolidate_repo, adapters::context_repo, adapters::continuity_read, adapters::continuity_repo, adapters::contribution_entry_repo, adapters::contribution_reasoner, adapters::contribution_repo, adapters::distill_repo, adapters::exact_census, adapters::membership_repo, adapters::memory_governance_repo, adapters::operation_receipt, adapters::placement_repo, adapters::private_projection_registry, adapters::projection_worker, adapters::provider_budget, adapters::provisioning, adapters::public_provenance, adapters::public_repo, adapters::qdrant, adapters::quota_repo, adapters::read_materialize, adapters::remember, adapters::retrieval_query_source, adapters::retrieve, application::auth, application::confirm, application::continuity, application::correct, application::entitlement, application::notify, application::pin, application::referral, application::retrieval_embedding_port, domain::audit, domain::authority, domain::context, domain::egress, domain::identity, domain::policy, gateway::auth, gateway::context, gateway::continuity, gateway::guard, gateway::mcp_application, gateway::memory, gateway::recall, gateway::remember, maintenance::main, private-worker::distill, projection::card, projection::serving, projection::sparse, projection::stream, public-worker::main, retrieval-provider::adapters, retrieval-provider::admission, retrieval-provider::contract, retrieval-provider::router, retrieval-worker::main, retrieval-worker::rpc, tests, xtask::e2e_seed, xtask::member, xtask::projection_serve, xtask::soak, xtask::switch_visible]
//! Invariants: []
//! Spec: Baseline §49; §52; §52.1
//!
//! §49 freezes: all three layers (Evidence/Memory/Projection) mint new objects as UUIDv7
//! (native PostgreSQL 18) only; legacy ids do not participate in this module — they only
//! surface as `remember()`'s idempotency_key, landing in the `source.legacy_id` field
//! (queryable/reconcilable, never part of any join). This module therefore exposes exactly
//! two entry points, "mint a new id" and "parse an existing string" — no legacy uuid5/hash
//! formula compatibility (that formula retired with §49; no golden test may keep it alive).

use crate::error::ErrorCode;
use uuid::Uuid;

/// Generates the seven §59 UUID newtype ids: each wraps one `Uuid`, `new` goes through the
/// UUIDv7 minting entry point (§49: new objects are always UUIDv7), `parse` parses an
/// existing string (no legacy-formula compatibility).
macro_rules! uuid_newtype {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name(pub Uuid);

        // clippy suggests forwarding `Default` to `new()`; deliberately omitted:
        // `Default::default()` reads to callers as "deterministic/zero value", but here it
        // would silently mint a fresh random id — a bigger trap than one extra `::new()` call.
        #[allow(clippy::new_without_default)]
        impl $name {
            /// Mints a new id (UUIDv7, §49).
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            /// Parses an existing UUID string. Does not recognize the retired legacy
            /// uuid5/hash formula (§49). A malformed input is `INVALID_INPUT` by definition
            /// (§52.1) — the sole terminal error set, per §52's "only two error enums" rule.
            pub fn parse(s: &str) -> Result<Self, ErrorCode> {
                Uuid::parse_str(s)
                    .map(Self)
                    .map_err(|_| ErrorCode::InvalidInput)
            }
        }
    };
}

uuid_newtype!(
    TenantId,
    "Tenant id (§59 `Scope` top-level required field)."
);
uuid_newtype!(UserId, "User id (§59 `Scope`).");
uuid_newtype!(WorkspaceId, "Workspace id (§59 `Scope`).");
uuid_newtype!(RepositoryId, "Repository id (§59 `Scope`).");
uuid_newtype!(TaskId, "Task id (§59 `Scope`).");
uuid_newtype!(RunId, "Run id (§59 `Scope`).");
uuid_newtype!(AgentId, "Agent id (§59 `Scope`).");

/// Request scope (§59): `tenant_id` is required, the rest narrow progressively by call
/// layer. Which layer it narrows to is filled in by the caller (HTTP/MCP adapter) — Domain
/// itself never derives it.
#[derive(Debug, Clone)]
pub struct Scope {
    /// Required: all adjudication and retrieval happens within the tenant boundary
    /// (§59.1 I6 — cross-tenant Evidence must never have its authority raised).
    pub tenant_id: TenantId,
    /// User layer, if narrowed to it (§59).
    pub user_id: Option<UserId>,
    /// Workspace layer, if narrowed to it (§59).
    pub workspace_id: Option<WorkspaceId>,
    /// Repository layer, if narrowed to it (§59).
    pub repository_id: Option<RepositoryId>,
    /// Task layer, if narrowed to it (§59).
    pub task_id: Option<TaskId>,
    /// Run layer, if narrowed to it (§59).
    pub run_id: Option<RunId>,
    /// Agent layer, if narrowed to it (§59).
    pub agent_id: Option<AgentId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_ids_round_trip_through_parse() {
        let id = TenantId::new();
        let parsed = TenantId::parse(&id.0.to_string()).expect("valid uuid string parses");
        assert_eq!(id, parsed);
    }

    #[test]
    fn new_ids_are_v7() {
        // §49: new objects are always UUIDv7 — the version bits must read 7.
        assert_eq!(RunId::new().0.get_version_num(), 7);
    }

    #[test]
    fn parse_rejects_garbage() {
        assert!(AgentId::parse("not-a-uuid").is_err());
    }

    #[test]
    fn parse_error_is_invalid_input() {
        // §52.1: a malformed id string is INVALID_INPUT, not a leaked `uuid::Error` (§52).
        assert_eq!(
            AgentId::parse("not-a-uuid").unwrap_err(),
            crate::error::ErrorCode::InvalidInput
        );
    }
}
