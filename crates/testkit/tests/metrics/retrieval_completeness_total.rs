// witness: family=retrieval_completeness_total labels=class,reason
//! Metric Witness for `retrieval_completeness_total{class,reason}` (§80.2 W-side; registry
//! row §41.2; §22.5 sole-constructor gate, §80.1 G80-6).
//!
//! Actively triggers `completeness::classify()`'s real degrade-direction table (§22.5) via
//! the crate-external witness door `classify_for_witness` (completeness.rs module doc
//! adjudication 3) and asserts a real observed counter delta per `{class,reason}` pair this
//! task's implementation can reach. `classify()` itself stays `pub(crate)` (§22.5's sole-
//! constructor privacy freeze) — this witness exercises the production emit path through the
//! one door built for exactly this purpose, not a test double.

use humaux_retrieval::completeness::{
    CensusResult, classify_for_witness, ledger, retrieval_completeness_total_count,
};
use humaux_retrieval::envelope::LaneStatus;
use humaux_retrieval::planner::{DirectGetLocator, PlannerDecision, QueryClass};

fn closed_ledger() -> ledger::LedgerReads {
    ledger::LedgerReads {
        expected: 10,
        done: 10,
        deleted: 0,
        skipped: 0,
        open_gaps: 0,
        pending: 0,
    }
}

fn broken_ledger() -> ledger::LedgerReads {
    ledger::LedgerReads {
        expected: 10,
        done: 5,
        deleted: 0,
        skipped: 0,
        open_gaps: 0,
        pending: 0, // 5 != 10
    }
}

/// Every `{class,reason}` pair `classify()`'s current implementation can produce must be
/// emittable and observable through the witness door: counter delta is exactly +1 per call.
#[test]
fn every_reachable_class_reason_pair_increments_by_exactly_one() {
    let cases: Vec<(&str, &str, PlannerDecision, LaneStatus, CensusResult, ledger::LedgerReads)> = vec![
        (
            "cannot_establish",
            "ledger_not_closed",
            PlannerDecision::Enumerate {
                predicate_id: "rejected_decisions_v1".to_string(),
            },
            LaneStatus::Ok,
            CensusResult { ok: true },
            broken_ledger(),
        ),
        (
            "cannot_establish",
            "census_failed",
            PlannerDecision::Class(QueryClass::Semantic),
            LaneStatus::Ok,
            CensusResult { ok: false },
            closed_ledger(),
        ),
        (
            "cannot_establish",
            "lane_failed",
            PlannerDecision::Class(QueryClass::Semantic),
            LaneStatus::Failed,
            CensusResult { ok: true },
            closed_ledger(),
        ),
        (
            "cannot_establish",
            "predicate_not_enumerable",
            PlannerDecision::CannotEstablish,
            LaneStatus::Ok,
            CensusResult { ok: true },
            closed_ledger(),
        ),
        (
            "exact",
            "none",
            PlannerDecision::Enumerate {
                predicate_id: "rejected_decisions_v1".to_string(),
            },
            LaneStatus::Ok,
            CensusResult { ok: true },
            closed_ledger(),
        ),
        (
            "exact",
            "none",
            PlannerDecision::DirectGet(DirectGetLocator::MemoryId(
                "00000000-0000-0000-0000-000000000000".to_string(),
            )),
            LaneStatus::Ok,
            CensusResult { ok: true },
            closed_ledger(),
        ),
        (
            "semantic_bounded",
            "none",
            PlannerDecision::Class(QueryClass::State),
            LaneStatus::Ok,
            CensusResult { ok: true },
            closed_ledger(),
        ),
    ];

    for (want_class, want_reason, planner_output, lane_status, census, reads) in cases {
        let before = retrieval_completeness_total_count(want_class, want_reason);
        let ledger = ledger::close(reads);
        let (class_label, reason_label) =
            classify_for_witness(&planner_output, lane_status, &census, &ledger);
        assert_eq!(
            (class_label, reason_label),
            (want_class, want_reason),
            "classify_for_witness produced an unexpected label pair for this fixture"
        );
        assert_eq!(
            retrieval_completeness_total_count(want_class, want_reason),
            before + 1,
            "retrieval_completeness_total{{class={want_class},reason={want_reason}}} must \
             increment exactly once per classify() call (§22.5)"
        );
    }
}

/// sample_count > 0 for the family as a whole (§80.2 D6 sentinel semantics): after the loop
/// above ran, at least one observable sample exists.
#[test]
fn family_has_nonzero_samples_after_positive_path() {
    let before = retrieval_completeness_total_count("exact", "none");
    let ledger = ledger::close(closed_ledger());
    classify_for_witness(
        &PlannerDecision::Enumerate {
            predicate_id: "rejected_decisions_v1".to_string(),
        },
        LaneStatus::Ok,
        &CensusResult { ok: true },
        &ledger,
    );
    assert!(
        retrieval_completeness_total_count("exact", "none") > before,
        "family must have samples > 0"
    );
}
