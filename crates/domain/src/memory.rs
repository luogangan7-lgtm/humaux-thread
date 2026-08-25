//! `domain::memory` — `MemoryType` (§8.5 / §59).
//!
//! §8.5 freezes 12 fixed types: `PROCEDURE` is retrievable procedural knowledge that does
//! not guarantee the Agent auto-executes it; `OUTCOME` holds Action -> Result -> Validation,
//! high-value knowledge for coding/agent continuity. This module lands only the type
//! skeleton — the `MemoryRecord` struct body is out of scope for T0.6.
//!
//! Note: the exhaustiveness test below pins variant *count and shape*, not the verbatim
//! wire name of each variant — it re-states the identifiers rather than checking them
//! against an external source of truth, so a coordinated rename would stay green. Verbatim
//! string pinning (the `error.rs` `as_str()` pattern) is deferred to the §78.2 DB↔Rust
//! contract-test card, not covered here.

/// Memory's fixed classification, a closed set of exactly 12 variants (§8.5, Rust shape §59).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryType {
    /// A stated fact (§8.5).
    Fact,
    /// A stated preference (§8.5).
    Preference,
    /// A recorded decision (§8.5).
    Decision,
    /// A rejected option or approach (§8.5).
    Rejection,
    /// Current state of something (§8.5).
    State,
    /// A known issue (§8.5).
    Issue,
    /// A lesson learned (§8.5).
    Lesson,
    /// A constraint that must hold (§8.5).
    Constraint,
    /// Retrievable procedural knowledge; does not guarantee the Agent auto-executes it (§8.5).
    Procedure,
    /// Action -> Result -> Validation; high-value knowledge for coding/agent continuity (§8.5).
    Outcome,
    /// A reference pointer to external material (§8.5).
    Reference,
    /// A free-form note (§8.5).
    Note,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_type_has_exactly_twelve_variants() {
        // §8.5 freezes this as a closed set of 12; this assertion does not rely on a derived
        // `EnumCount` — it enumerates by hand, so adding/removing a variant must update this
        // in lockstep, otherwise the compile-time match-exhaustiveness check fails first.
        let all = [
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
        ];
        assert_eq!(all.len(), 12);

        // Exhaustive match: missing or extra variant fails to compile — stronger than the
        // array count above.
        fn assert_exhaustive(t: MemoryType) {
            match t {
                MemoryType::Fact
                | MemoryType::Preference
                | MemoryType::Decision
                | MemoryType::Rejection
                | MemoryType::State
                | MemoryType::Issue
                | MemoryType::Lesson
                | MemoryType::Constraint
                | MemoryType::Procedure
                | MemoryType::Outcome
                | MemoryType::Reference
                | MemoryType::Note => {}
            }
        }
        for t in all {
            assert_exhaustive(t);
        }
    }
}
