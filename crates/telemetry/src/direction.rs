//! `telemetry::direction` — §53.6 direction table: hand-written data checked for parity against its own generation
//!   source by `cargo xtask direction-table` (G80-9).
//! Depends-on: crates=[humaux-fail-closed-macro]; services=[]; env=[]; modules=[humaux-fail-closed-macro]
//! Called-by: []
//! Invariants: []
//! Spec: Baseline §53.2; §53.3; §53.4
//!
//! "由代码生成
//! 而非手写" (§53.6) means this const is the *reference* copy, not the source of truth —
//! `cargo xtask direction-table` regenerates the same shape from `degrade::DegradeCode`
//! (fail-open side) and every `#[fail_closed(threat = "...")]`-annotated function in the
//! workspace (fail-closed side) and diffs the two; a mismatch is red (G80-9, §80.1.1).
//!
//! Row shape is `subject + direction`, not the full `operation | failure | policy |
//! fallback | degrade_code | fault_test` schema §53.6 sketches as an example — this table's
//! only job is proving fail-open/fail-closed completeness, and §53.4 / §53.3 规则2 already
//! own the fault_test/degrade_code parity separately. Widening this table's row shape into
//! a second copy of that parity was already rejected for this table's fields (fault +
//! direction only, no fault-injection columns) — reuse the annotation + injection-test
//! mechanism for anything without a fault, don't extend the table.

use humaux_fail_closed_macro::fail_closed;

/// Which side of §53.6's fail-open/fail-closed split a [`DirectionRow`] belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Subject is a `DegradeCode` variant (§53.2) — `abstain()`s to a fallback value.
    FailOpen,
    /// Subject is a `#[fail_closed(threat = "...")]`-annotated function (§53.6) — refuses
    /// instead of falling back when its named threat applies.
    FailClosed,
}

/// One §53.6 direction table row. `subject` is either a `DegradeCode` variant name
/// (verbatim, PascalCase — §53.2) or a fail-closed function name; which one is determined
/// by `direction`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectionRow {
    pub subject: &'static str,
    pub direction: Direction,
}

/// §53.6 direction table. Fail-open side: all 11 `DegradeCode` variants (§53.2), each
/// `FailOpen` — this half never needs a manual edit beyond staying in sync with
/// `degrade::DegradeCode`, which `cargo xtask direction-table` verifies mechanically.
/// Fail-closed side: Phase 0 production code carries no `#[fail_closed]` annotations yet,
/// so [`example_fail_closed_check`] below is this table's sole fail-closed row — a real,
/// compiling generation source rather than an empty set on both sides (which would pass
/// vacuously and could never go red on either §80.1.1 G80-9 injection).
pub const DIRECTION_TABLE: &[DirectionRow] = &[
    DirectionRow {
        subject: "RerankModelMismatch",
        direction: Direction::FailOpen,
    },
    DirectionRow {
        subject: "RerankProviderTimeout",
        direction: Direction::FailOpen,
    },
    DirectionRow {
        subject: "EmbedProviderTimeout",
        direction: Direction::FailOpen,
    },
    DirectionRow {
        subject: "EgressDenied",
        direction: Direction::FailOpen,
    },
    DirectionRow {
        subject: "StatePinMissing",
        direction: Direction::FailOpen,
    },
    DirectionRow {
        subject: "StatePinAmbiguous",
        direction: Direction::FailOpen,
    },
    DirectionRow {
        subject: "CompletenessUnknown",
        direction: Direction::FailOpen,
    },
    DirectionRow {
        subject: "ProjectionLag",
        direction: Direction::FailOpen,
    },
    DirectionRow {
        subject: "GraphExpandCapped",
        direction: Direction::FailOpen,
    },
    DirectionRow {
        subject: "ProjectionInvisibleLoss",
        direction: Direction::FailOpen,
    },
    DirectionRow {
        subject: "LaneSubstituted",
        direction: Direction::FailOpen,
    },
    DirectionRow {
        subject: "example_fail_closed_check",
        direction: Direction::FailClosed,
    },
];

/// §53.6 fail-closed generation-source example: Phase 0's only annotated function, so
/// `cargo xtask direction-table` (G80-9) has a real fail-closed row to compare
/// [`DIRECTION_TABLE`] against. threat model: none — this function has no production
/// caller, it exists solely so the fail-closed side of this table has something to check
/// itself against before Phase 0 code actually needs the pattern.
#[fail_closed(threat = "demo threat model — no production caller yet")]
pub fn example_fail_closed_check() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direction_table_has_eleven_fail_open_rows_and_one_fail_closed_row() {
        let fail_open = DIRECTION_TABLE
            .iter()
            .filter(|r| r.direction == Direction::FailOpen)
            .count();
        let fail_closed = DIRECTION_TABLE
            .iter()
            .filter(|r| r.direction == Direction::FailClosed)
            .count();
        assert_eq!(fail_open, 11);
        assert_eq!(fail_closed, 1);
    }

    /// §53.2: the fail-open side's subjects must be exactly `DegradeCode::ALL`'s names —
    /// this is the same parity G80-9 checks from source text, pinned here as a compiled
    /// (not text-scanned) cross-check.
    #[test]
    fn fail_open_side_matches_degrade_code_all_by_name() {
        let table_subjects: std::collections::BTreeSet<&str> = DIRECTION_TABLE
            .iter()
            .filter(|r| r.direction == Direction::FailOpen)
            .map(|r| r.subject)
            .collect();
        let variant_subjects: std::collections::BTreeSet<&str> = crate::degrade::DegradeCode::ALL
            .iter()
            .map(|c| c.as_str())
            .collect();
        assert_eq!(table_subjects, variant_subjects);
    }

    #[test]
    fn example_fail_closed_check_is_callable() {
        assert!(example_fail_closed_check());
    }
}
