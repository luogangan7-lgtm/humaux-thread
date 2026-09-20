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

impl MemoryType {
    /// Every variant, in §8.5's declaration order. Closed set of 12.
    pub const ALL: [Self; 12] = [
        Self::Fact,
        Self::Preference,
        Self::Decision,
        Self::Rejection,
        Self::State,
        Self::Issue,
        Self::Lesson,
        Self::Constraint,
        Self::Procedure,
        Self::Outcome,
        Self::Reference,
        Self::Note,
    ];

    /// The DB wire label (`private.memory_records.memory_type`, migration 0004's CHECK set).
    ///
    // ponytail: three adapter-side copies of this mapping predate it
    // (`adapters::qdrant`, `adapters::distill_repo`, `adapters::projection_worker`); they are
    // outside card 22b's allowed files. Collapse them onto this one when a card owns them —
    // the §78.2 contract test below now pins THIS list against the live CHECK, so a drift in
    // the domain copy is a red gate rather than a silent disagreement.
    #[must_use]
    pub const fn wire(self) -> &'static str {
        match self {
            Self::Fact => "FACT",
            Self::Preference => "PREFERENCE",
            Self::Decision => "DECISION",
            Self::Rejection => "REJECTION",
            Self::State => "STATE",
            Self::Issue => "ISSUE",
            Self::Lesson => "LESSON",
            Self::Constraint => "CONSTRAINT",
            Self::Procedure => "PROCEDURE",
            Self::Outcome => "OUTCOME",
            Self::Reference => "REFERENCE",
            Self::Note => "NOTE",
        }
    }

    /// Inverse of [`MemoryType::wire`]; `None` for a label outside the closed set.
    #[must_use]
    pub fn parse_wire(wire: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|t| t.wire() == wire)
    }
}

/// §25.4.A v1: the Mandatory-lane **type partition**. A closed set of four values.
///
/// This is NOT §25.2's display slots, NOT §24's CandidateVariant set, and NOT a claim that a
/// memory is current / authoritative / must-include. It answers exactly one question: which
/// protection group does `required_current_state_facets_v1` nominate this memory under?
/// Whether a nominated memory is admitted still runs the selector's frozen scope inheritance,
/// authority/origin floor, lifecycle, temporal validity and conflict rules (§25.4.A(4)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum MandatoryContextFacet {
    /// Current state of the world (§24 `state` reserve).
    State,
    /// Constraints that must hold (§24 `constraints` reserve).
    Constraints,
    /// Decisions still in force — choices AND explicit rejections (§24 `decisions` reserve).
    Decisions,
    /// Known issues (§24 `issues` reserve).
    Issues,
}

impl MandatoryContextFacet {
    /// Every facet, in §25.4.A(1)'s declaration order. Closed set of 4.
    pub const ALL: [Self; 4] = [
        Self::State,
        Self::Constraints,
        Self::Decisions,
        Self::Issues,
    ];

    /// The DB wire label (`private.memory_records.facet`). The migration's generated-column
    /// CASE mirrors these four strings and its CHECK pins the value domain; the §78.2 contract
    /// test in `adapters` reconciles the two, so neither side may drift alone.
    #[must_use]
    pub const fn wire(self) -> &'static str {
        match self {
            Self::State => "state",
            Self::Constraints => "constraints",
            Self::Decisions => "decisions",
            Self::Issues => "issues",
        }
    }

    /// Inverse of [`MandatoryContextFacet::wire`]; `None` for a label outside the closed set.
    #[must_use]
    pub fn parse_wire(wire: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|f| f.wire() == wire)
    }
}

/// §25.4.A(2): the **single** Rust-side facet mapping. `private.memory_records.facet` is a
/// `GENERATED ALWAYS ... STORED` projection of the same CASE (migration 0172) — there is no
/// independent write entry for the column, so this function is a *reconciliation partner*,
/// never a second source of truth. `None` means "not nominated by type"; it does NOT forbid
/// the memory from entering Mandatory through an explicit binding (§25.4.A(5)).
///
/// `REJECTION -> Decisions` is an explicit product contract adopted by card 22b (ADR-0045),
/// not a derivation from §24's reserve names: a route that was explicitly rejected must stay
/// deterministically reachable, not depend on similarity recall.
#[must_use]
pub const fn facet_for(memory_type: MemoryType) -> Option<MandatoryContextFacet> {
    match memory_type {
        MemoryType::State => Some(MandatoryContextFacet::State),
        MemoryType::Constraint => Some(MandatoryContextFacet::Constraints),
        MemoryType::Decision | MemoryType::Rejection => Some(MandatoryContextFacet::Decisions),
        MemoryType::Issue => Some(MandatoryContextFacet::Issues),
        // §25.4.A(3): a NEW MemoryType must be ruled on explicitly. This arm is exhaustive by
        // name on purpose — `_ => None` would let a 13th type be silently accepted as NULL.
        MemoryType::Fact
        | MemoryType::Preference
        | MemoryType::Lesson
        | MemoryType::Procedure
        | MemoryType::Outcome
        | MemoryType::Reference
        | MemoryType::Note => None,
    }
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

    #[test]
    fn memory_type_wire_round_trips() {
        assert_eq!(MemoryType::ALL.len(), 12);
        for t in MemoryType::ALL {
            assert_eq!(MemoryType::parse_wire(t.wire()), Some(t));
            assert!(t.wire().chars().all(|c| c.is_ascii_uppercase()));
        }
        assert_eq!(MemoryType::parse_wire("STATEFUL"), None);
    }

    #[test]
    fn mandatory_context_facet_wire_round_trips() {
        assert_eq!(MandatoryContextFacet::ALL.len(), 4);
        for f in MandatoryContextFacet::ALL {
            assert_eq!(MandatoryContextFacet::parse_wire(f.wire()), Some(f));
            assert!(f.wire().chars().all(|c| c.is_ascii_lowercase()));
        }
        assert_eq!(MandatoryContextFacet::parse_wire("STATE"), None);
    }

    /// §78.1 registered GOLDEN, card 22b (ruling §五.2). Deliberately written out by hand
    /// rather than computed from [`facet_for`]: a test that calls the production mapping
    /// cannot notice the production mapping changing. The `adapters` contract test reconciles
    /// the SAME 12 pairs against the live generated column.
    #[test]
    fn facet_golden_twelve_entries() {
        let golden: [(MemoryType, Option<MandatoryContextFacet>); 12] = [
            (MemoryType::Fact, None),
            (MemoryType::Preference, None),
            (MemoryType::Decision, Some(MandatoryContextFacet::Decisions)),
            (
                MemoryType::Rejection,
                Some(MandatoryContextFacet::Decisions),
            ),
            (MemoryType::State, Some(MandatoryContextFacet::State)),
            (MemoryType::Issue, Some(MandatoryContextFacet::Issues)),
            (MemoryType::Lesson, None),
            (
                MemoryType::Constraint,
                Some(MandatoryContextFacet::Constraints),
            ),
            (MemoryType::Procedure, None),
            (MemoryType::Outcome, None),
            (MemoryType::Reference, None),
            (MemoryType::Note, None),
        ];
        assert_eq!(golden.len(), MemoryType::ALL.len());
        for (t, expected) in golden {
            assert_eq!(facet_for(t), expected, "{}", t.wire());
        }
        // Every facet in the closed set is reachable from at least one MemoryType — a facet
        // nothing maps to would be an unreachable §24 reserve.
        for f in MandatoryContextFacet::ALL {
            assert!(
                MemoryType::ALL.into_iter().any(|t| facet_for(t) == Some(f)),
                "{} is unreachable",
                f.wire()
            );
        }
    }
}
