//! `application::archive` — §36 `memory.archive` / `memory.unarchive` use case, pure half (ADR-0024, Q3).
//! Depends-on: crates=[humaux-domain]; services=[];
//!   env=[]; modules=[domain::confirm, domain::error]
//! Called-by: [adapters::memory_governance_repo]
//! Invariants: []
//! Spec: §36; ADR-0024
//!
//! Archive is a visibility flag (`private.memory_records.archived_at`), not a fifth
//! AuthorityStatus — an archived Memory keeps its authority `status` and G59-4 pairing intact,
//! it is merely hidden from recall/context/enumerate. What is pure and unit-tested here:
//! - the claim rule: a presented confirmation must be *this* archive/unarchive op on *this*
//!   memory with no successor — an archive token never runs an unarchive and vice versa (D-B);
//! - the idempotency rule (D-B): archive of an already-archived Memory, and unarchive of a
//!   live one, are both `ALREADY_IN_STATE` (1201) — a success-shaped CONFLICT, never a second
//!   lifecycle event.
//!
//! The IO order (reserve BMO -> consume token -> visibility -> lifecycle event -> the
//! `archived_at` UPDATE -> COMMIT) lives in `adapters::memory_governance_repo`; the
//! `archived_at IS NULL` predicate on that UPDATE is the sole arbiter of the current state,
//! exactly as `status='active'` is for supersede.

use humaux_domain::{confirm::DestructiveOp, error::ConflictReason};

// The claim-binding check (token is *this* op on *this* target, no successor) lives inline in
// `adapters::memory_governance_repo::validate_archive` — the same place supersede/restore keep
// theirs. A second pure copy here would be a speculative helper nothing calls (ponytail: the
// binding is one `if` at the trust boundary, not a reusable abstraction).

/// D-B idempotency: given whether the Memory is already archived, decide whether the op is a
/// no-op (`ALREADY_IN_STATE`) or should proceed. `archive` of an archived row and `unarchive`
/// of a live row are both already-in-state; the productive direction returns `Ok(())`.
///
/// This is a pre-flight for a clean reason code; the adapter's `UPDATE ... WHERE archived_at
/// IS [NOT] NULL` remains the sole arbiter under the row lock (a concurrent racer that flips
/// the flag between this read and the UPDATE resolves as a 0-row UPDATE, same as supersede).
pub fn archive_allowed(op: DestructiveOp, currently_archived: bool) -> Result<(), ConflictReason> {
    let productive = match op {
        DestructiveOp::MemoryArchive => !currently_archived,
        DestructiveOp::MemoryUnarchive => currently_archived,
        _ => return Err(ConflictReason::ALREADY_IN_STATE),
    };
    if productive {
        Ok(())
    } else {
        Err(ConflictReason::ALREADY_IN_STATE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn already_in_state_is_symmetric() {
        // archive: productive only when currently live.
        assert_eq!(archive_allowed(DestructiveOp::MemoryArchive, false), Ok(()));
        assert_eq!(
            archive_allowed(DestructiveOp::MemoryArchive, true),
            Err(ConflictReason::ALREADY_IN_STATE)
        );
        // unarchive: productive only when currently archived.
        assert_eq!(
            archive_allowed(DestructiveOp::MemoryUnarchive, true),
            Ok(())
        );
        assert_eq!(
            archive_allowed(DestructiveOp::MemoryUnarchive, false),
            Err(ConflictReason::ALREADY_IN_STATE)
        );
        // A non-archive op never reaches this decision, but fails closed if it does.
        assert_eq!(
            archive_allowed(DestructiveOp::MemoryPin, false),
            Err(ConflictReason::ALREADY_IN_STATE)
        );
    }
}
