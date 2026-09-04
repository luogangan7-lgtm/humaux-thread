//! `application::pin` — §36 `memory.pin` / `memory.unpin` use case, pure half (ADR-0019).
//!
//! The IO order (reserve BMO -> consume token -> visibility -> binding write -> COMMIT) lives
//! in `adapters::context_repo`; what is pure and unit-tested here:
//! - the claim rule: a presented confirmation must be *this* operation on *this* memory with
//!   no successor — a pin token never executes an unpin and vice versa (D-A);
//! - the idempotency rule (D-C): pin on an already-pinned memory returns the existing binding,
//!   unpin on a non-pinned memory is the existing `Conflict`;
//! - the binding shape: PINNED at the credential's bound workspace scope.

use humaux_domain::{
    authority::MemoryId,
    confirm::DestructiveOp,
    context::{BindingMode, BindingRequest, ScopeKind},
    error::ErrorCode,
    ids::WorkspaceId,
};
use uuid::Uuid;

/// D-A: the consumed claim must bind exactly (`expected`, `memory`, no successor). Anything
/// else is the existing `Conflict` (§52.1: no new variant, no existence oracle).
pub fn check_claim(
    expected: DestructiveOp,
    claim_op: DestructiveOp,
    claim_target: Uuid,
    claim_successor: Option<Uuid>,
    memory: MemoryId,
) -> Result<(), ErrorCode> {
    if !matches!(
        expected,
        DestructiveOp::MemoryPin | DestructiveOp::MemoryUnpin
    ) {
        return Err(ErrorCode::InvalidInput);
    }
    if claim_op != expected || claim_target != memory.0 || claim_successor.is_some() {
        return Err(ErrorCode::Conflict);
    }
    Ok(())
}

/// What a pin must do given the current active PINNED binding at that scope (D-C).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinAction {
    /// Already pinned: return this row, insert nothing.
    ReturnExisting(Uuid),
    /// Not pinned: authorize + insert one row.
    Insert,
}

#[must_use]
pub const fn pin_action(existing: Option<Uuid>) -> PinAction {
    match existing {
        Some(binding_id) => PinAction::ReturnExisting(binding_id),
        None => PinAction::Insert,
    }
}

/// D-C: unpin needs an active PINNED row to revoke; none is `Conflict`.
pub fn unpin_target(existing: Option<Uuid>) -> Result<Uuid, ErrorCode> {
    existing.ok_or(ErrorCode::Conflict)
}

/// The one binding shape `memory.pin` mints: PINNED, scoped to the bound workspace (the same
/// scope `context.assemble` reads through `scope_chain`).
#[must_use]
pub const fn pin_request(memory: MemoryId, workspace: WorkspaceId) -> BindingRequest {
    BindingRequest {
        mode: BindingMode::Pinned,
        scope_kind: ScopeKind::Workspace,
        scope_id: Some(workspace.0),
        memory_id: memory,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claim_must_be_this_op_this_memory_no_successor() {
        let memory = MemoryId::new();
        let other = MemoryId::new();
        for op in [DestructiveOp::MemoryPin, DestructiveOp::MemoryUnpin] {
            assert_eq!(check_claim(op, op, memory.0, None, memory), Ok(()));
            assert_eq!(
                check_claim(op, op, other.0, None, memory),
                Err(ErrorCode::Conflict),
                "token for X never executes on Y"
            );
            assert_eq!(
                check_claim(op, op, memory.0, Some(other.0), memory),
                Err(ErrorCode::Conflict),
                "a pin/unpin token carries no successor"
            );
            assert_eq!(
                check_claim(op, DestructiveOp::MemorySupersede, memory.0, None, memory),
                Err(ErrorCode::Conflict)
            );
        }
        assert_eq!(
            check_claim(
                DestructiveOp::MemoryPin,
                DestructiveOp::MemoryUnpin,
                memory.0,
                None,
                memory
            ),
            Err(ErrorCode::Conflict),
            "a pin confirmation never executes an unpin"
        );
        assert_eq!(
            check_claim(
                DestructiveOp::MemorySupersede,
                DestructiveOp::MemorySupersede,
                memory.0,
                None,
                memory
            ),
            Err(ErrorCode::InvalidInput),
            "this rule is only for the two binding ops"
        );
    }

    #[test]
    fn idempotency_rules() {
        let id = Uuid::now_v7();
        assert_eq!(pin_action(Some(id)), PinAction::ReturnExisting(id));
        assert_eq!(pin_action(None), PinAction::Insert);
        assert_eq!(unpin_target(Some(id)), Ok(id));
        assert_eq!(unpin_target(None), Err(ErrorCode::Conflict));
    }

    #[test]
    fn pin_request_is_pinned_at_workspace_scope() {
        let memory = MemoryId::new();
        let workspace = WorkspaceId(Uuid::now_v7());
        let request = pin_request(memory, workspace);
        assert_eq!(request.mode, BindingMode::Pinned);
        assert_eq!(request.scope_kind, ScopeKind::Workspace);
        assert_eq!(request.scope_id, Some(workspace.0));
        assert_eq!(request.memory_id, memory);
    }
}
