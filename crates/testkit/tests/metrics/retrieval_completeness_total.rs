// witness: family=retrieval_completeness_total labels=class,reason
//! `testkit::tests::metrics::retrieval_completeness_total` — Metric Witness for
//!   `retrieval_completeness_total{class,reason}` (§80.2 W-side; registry row §41.2; §22.5 sole-constructor gate,
//!   §80.1 G80-6).
//! Depends-on: crates=[]; services=[PostgreSQL(owner) r=[private.memory_records]]; env=[]; modules=[retrieval::completeness, retrieval::envelope, retrieval::planner, retrieval::predicate_registry, retrieval::request]
//! Called-by: []
//! Invariants: [reads private.memory_records to synthesize completeness fixtures; no fail-closed behaviour, test-only fixture]
//! Spec: none
//!
//! Each case builds a real typed request plus valid provenance, pipeline/A2, and no-overflow
//! context before calling `envelope_outcome_block`. The public `classify_for_witness` remains a
//! label-only component witness; it cannot increment final result metrics because it has none of
//! those final Envelope facts.

use std::collections::{BTreeMap, BTreeSet};

use humaux_retrieval::completeness::{
    CensusResult, ExactEnumeration, classify_for_witness, ledger,
    retrieval_completeness_total_count,
};
use humaux_retrieval::envelope::{
    CannotEstablishReasonWire, CompletenessClassWire, CompletenessInputs, CountScope,
    EvidenceBlock, KnowledgeBlock, LaneStatus, PipelineBlock, ProfileBlock, ProvenanceBlock,
    ProvenanceValue, build_projection_block, envelope_outcome_block,
};
use humaux_retrieval::planner::PlannerDecision;
use humaux_retrieval::predicate_registry::{PredicateRow, load_registry};
use humaux_retrieval::request::{
    RetrievalIntent, RetrievalRequest, build_request, resolve_registered_retrieval_profile,
};

const SCOPE: &str = "private.memory_records WHERE tenant_id = $1 AND visibility_workspace_id = $2";
const COLUMNS: [&str; 3] = ["memory_type", "superseded_at", "visibility_workspace_id"];

#[derive(Clone, Copy)]
enum RequestKind {
    Enumerate,
    DirectGet,
    Semantic,
    CannotEstablish,
}

fn predicate() -> humaux_retrieval::predicate_registry::PredicateEntry {
    load_registry(vec![PredicateRow {
        predicate_id: "rejected_decisions_v1".to_string(),
        sql_predicate: "memory_type='REJECTION' AND superseded_at IS NULL".to_string(),
        required_columns: COLUMNS.map(str::to_string).to_vec(),
        enumerable_scope: SCOPE.to_string(),
        surface_patterns: vec!["all rejected".to_string()],
        owner_module: "retrieval::planner".to_string(),
    }])
    .expect("validated witness predicate")
    .remove(0)
}

fn request(kind: RequestKind) -> RetrievalRequest {
    let profile =
        resolve_registered_retrieval_profile(&BTreeMap::new()).expect("registered default profile");
    let indexed: BTreeSet<String> = COLUMNS.into_iter().map(str::to_string).collect();
    let (query, registry, scopes) = match kind {
        RequestKind::Enumerate => (
            "all rejected".to_string(),
            vec![predicate()],
            BTreeSet::from([SCOPE.to_string()]),
        ),
        RequestKind::DirectGet => (
            "00000000-0000-0000-0000-000000000000".to_string(),
            vec![],
            BTreeSet::new(),
        ),
        RequestKind::Semantic => (
            "ordinary semantic query".to_string(),
            vec![],
            BTreeSet::new(),
        ),
        RequestKind::CannotEstablish => (
            "all rejected".to_string(),
            vec![predicate()],
            BTreeSet::new(),
        ),
    };
    build_request(
        RetrievalIntent::new(query, registry, indexed, scopes).expect("valid witness intent"),
        &profile,
    )
    .expect("typed request")
}

fn provenance(request: &RetrievalRequest) -> ProvenanceBlock {
    ProvenanceBlock {
        binary_build: "testkit-completeness-witness".to_string(),
        projection_version: ProvenanceValue::Used {
            id: "projection-test-v1".to_string(),
        },
        embedding_model_id: ProvenanceValue::Used {
            id: "embedding-test-v1".to_string(),
        },
        rerank_model_id: ProvenanceValue::Used {
            id: "rerank-test-v1".to_string(),
        },
        card_builder_version: ProvenanceValue::Used {
            id: "card-test-v1".to_string(),
        },
        profile_fingerprint: request.profile_fingerprint_identity().clone(),
        profile: ProfileBlock {
            top_k: request.top_k(),
            cand_k: request.cand_k(),
            cand_k_formula: "min(top_k*5, 200)".to_string(),
            lanes: vec!["literal".to_string()],
        },
    }
}

fn final_labels(
    kind: RequestKind,
    lane_status: LaneStatus,
    census: CensusResult,
    reads: ledger::LedgerReads,
) -> (&'static str, &'static str) {
    let request = request(kind);
    let ledger = ledger::close(reads);
    let counts = ledger.counts();
    let visible = counts
        .done()
        .checked_sub(counts.deleted() + counts.skipped())
        .expect("fixture A2 visibility cannot underflow");
    let pipeline = PipelineBlock {
        evidence: EvidenceBlock::no_batch(Some(counts.expected()), CountScope::StreamLedger),
        knowledge: KnowledgeBlock {
            eligible: Some(counts.expected()),
            processed: Some(counts.expected()),
            waiting_key: Some(0),
            failed: Some(0),
            count_scope: CountScope::StreamLedger,
        },
        projection: build_projection_block(&ledger, Some(visible)).value,
    };
    let outcome = envelope_outcome_block(
        &request,
        CompletenessInputs {
            lane_status: &lane_status,
            census: &census,
            ledger: &ledger,
            pipeline: &pipeline,
            provenance: &provenance(&request),
            visible: Some(visible),
            context: None,
            mandatory_missing: 0,
        },
        |outcome| Ok(outcome),
    )
    .expect("full final witness outcome")
    .finish();
    let class = match outcome.class {
        CompletenessClassWire::Exact => "exact",
        CompletenessClassWire::FacetComplete => "facet_complete",
        CompletenessClassWire::SemanticBounded => "semantic_bounded",
        CompletenessClassWire::CannotEstablish => "cannot_establish",
    };
    let reason = match outcome.reason {
        None => "none",
        Some(CannotEstablishReasonWire::LedgerNotClosed) => "ledger_not_closed",
        Some(CannotEstablishReasonWire::PredicateNotEnumerable) => "predicate_not_enumerable",
        Some(CannotEstablishReasonWire::CensusFailed) => "census_failed",
        Some(CannotEstablishReasonWire::LaneFailed) => "lane_failed",
        Some(CannotEstablishReasonWire::IndexCountUnavailable) => "index_count_unavailable",
        Some(CannotEstablishReasonWire::A2OvershootBeyondPending) => "a2_overshoot_beyond_pending",
        Some(CannotEstablishReasonWire::MandatoryContextOverflow) => "mandatory_context_overflow",
        Some(CannotEstablishReasonWire::CountUnknown) => "count_unknown",
        Some(CannotEstablishReasonWire::CountScopeMismatch) => "count_scope_mismatch",
        Some(CannotEstablishReasonWire::PipelineCountMismatch) => "pipeline_count_mismatch",
        Some(CannotEstablishReasonWire::MandatoryNotSatisfied) => "mandatory_not_satisfied",
        Some(CannotEstablishReasonWire::NoServingProjection) => "no_serving_projection",
    };
    (class, reason)
}

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
        pending: 0,
    }
}

/// Every class/reason pair that can pass final request provenance is emitted through the only
/// final Envelope outcome path and observed exactly once. The planner's
/// `CannotEstablish/PredicateNotEnumerable` result is separately checked as a component label:
/// `ProvenanceBlock::is_valid` deliberately rejects an unexecutable request before any metric.
#[test]
fn every_final_observable_class_reason_pair_increments_by_exactly_one() {
    let exact_census = CensusResult::enumerated(
        ExactEnumeration::new("rejected_decisions_v1", 1, 1, 0).expect("exact census"),
    );
    let cases = vec![
        (
            "cannot_establish",
            "ledger_not_closed",
            RequestKind::Enumerate,
            LaneStatus::Ok,
            exact_census.clone(),
            broken_ledger(),
        ),
        (
            "cannot_establish",
            "census_failed",
            RequestKind::Semantic,
            LaneStatus::Ok,
            CensusResult::failed(),
            closed_ledger(),
        ),
        (
            "cannot_establish",
            "lane_failed",
            RequestKind::Semantic,
            LaneStatus::Failed,
            CensusResult::ok_without_enumeration(),
            closed_ledger(),
        ),
        (
            "exact",
            "none",
            RequestKind::Enumerate,
            LaneStatus::Ok,
            exact_census.clone(),
            closed_ledger(),
        ),
        (
            "exact",
            "none",
            RequestKind::DirectGet,
            LaneStatus::Ok,
            exact_census,
            closed_ledger(),
        ),
        (
            "semantic_bounded",
            "none",
            RequestKind::Semantic,
            LaneStatus::Ok,
            CensusResult::ok_without_enumeration(),
            closed_ledger(),
        ),
    ];

    for (want_class, want_reason, kind, lane_status, census, reads) in cases {
        let before = retrieval_completeness_total_count(want_class, want_reason);
        assert_eq!(
            final_labels(kind, lane_status, census, reads),
            (want_class, want_reason),
            "final Envelope path produced unexpected labels"
        );
        assert_eq!(
            retrieval_completeness_total_count(want_class, want_reason),
            before + 1,
            "retrieval_completeness_total{{class={want_class},reason={want_reason}}} must increment exactly once per final outcome"
        );
    }
}

/// The public classifier remains a component-only label oracle; it is not a final-metric bypass.
#[test]
fn component_witness_labels_do_not_increment_final_metric() {
    let before = retrieval_completeness_total_count("exact", "none");
    let census = CensusResult::enumerated(
        ExactEnumeration::new("rejected_decisions_v1", 1, 1, 0).expect("exact census"),
    );
    let request = request(RequestKind::Enumerate);
    assert_eq!(
        classify_for_witness(
            request.planner_decision(),
            LaneStatus::Ok,
            &census,
            &ledger::close(closed_ledger()),
            0
        ),
        ("exact", "none")
    );
    assert_eq!(
        retrieval_completeness_total_count("exact", "none"),
        before,
        "component witness has no provenance/pipeline/context and must not emit a final metric"
    );
}

/// A request whose planner cannot establish its predicate must be visible to component testing,
/// but is not a final-metric route because no executable request-bound provenance exists.
#[test]
fn predicate_not_enumerable_is_component_visible_but_not_metric_emittable() {
    let before = retrieval_completeness_total_count("cannot_establish", "predicate_not_enumerable");
    let request = request(RequestKind::CannotEstablish);
    assert_eq!(
        classify_for_witness(
            request.planner_decision(),
            LaneStatus::Ok,
            &CensusResult::ok_without_enumeration(),
            &ledger::close(closed_ledger()),
            0
        ),
        ("cannot_establish", "predicate_not_enumerable")
    );
    assert_eq!(
        retrieval_completeness_total_count("cannot_establish", "predicate_not_enumerable"),
        before,
        "the unexecutable component result cannot bypass final provenance validation"
    );
}
