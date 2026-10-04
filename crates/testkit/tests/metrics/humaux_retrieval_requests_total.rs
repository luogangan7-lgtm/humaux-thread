// witness: family=humaux_retrieval_requests_total labels=intent,completeness_class
//! `testkit::tests::metrics::humaux_retrieval_requests_total` — Metric Witness for
//!   `humaux_retrieval_requests_total{intent,completeness_class}` (§80.2 W-side; registry row §41.2: §20 envelope
//!   returns; §53.5 INV-1 denominator).
//! Depends-on: crates=[humaux-domain]; services=[]; env=[]; modules=[domain::authority, retrieval::completeness, retrieval::envelope, retrieval::request]
//! Called-by: []
//! Invariants: []
//! Spec: Baseline §41.2; §53.5; §80.2; ADR-0061 D-C
//!
//! Builds one request per `RetrievalIntent` constructor, takes it through the only final path
//! (`envelope_outcome_block` then `PendingEnvelope::finish`) and reads the family back from
//! `completeness::render_metrics`, the exposition the gateway serves. One test function, so no
//! other test in this binary moves the process-wide counter between the before and after reads.
//! Compiled and run standalone by `cargo xtask metrics-registry` (`run_witness_probe`).

use std::collections::BTreeMap;
use std::time::Duration;

use humaux_domain::authority::MemoryId;
use humaux_retrieval::completeness::{CensusResult, ExactEnumeration, ledger, render_metrics};
use humaux_retrieval::envelope::{
    CompletenessInputs, CountScope, EvidenceBlock, KnowledgeBlock, LaneStatus, PipelineBlock,
    ProfileBlock, ProvenanceBlock, ProvenanceValue, build_projection_block, envelope_outcome_block,
};
use humaux_retrieval::request::{
    RetrievalIntent, RetrievalRequest, build_request, resolve_registered_retrieval_profile,
};

const FAMILY: &str = "humaux_retrieval_requests_total";
const LAG: Duration = Duration::from_secs(60);

/// `(label pairs, value)` of every sample of the family in one render.
fn samples() -> BTreeMap<Vec<(String, String)>, f64> {
    let mut out = String::new();
    render_metrics(&mut out);
    assert!(out.contains(&format!("# TYPE {FAMILY} counter\n")), "{out}");
    out.lines()
        .filter_map(|l| l.strip_prefix(&format!("{FAMILY}{{")))
        .map(|rest| {
            let (labels, value) = rest.split_once("} ").expect("sample line");
            let pairs = labels
                .split(',')
                .map(|kv| {
                    let (k, v) = kv.split_once('=').expect("k=v");
                    (k.to_string(), v.trim_matches('"').to_string())
                })
                .collect();
            (pairs, value.parse().expect("numeric sample"))
        })
        .collect()
}

fn key(intent: &str, class: &str) -> Vec<(String, String)> {
    vec![
        ("intent".to_string(), intent.to_string()),
        ("completeness_class".to_string(), class.to_string()),
    ]
}

fn provenance(request: &RetrievalRequest) -> ProvenanceBlock {
    let used = |id: &str| ProvenanceValue::Used { id: id.to_string() };
    ProvenanceBlock {
        binary_build: "testkit-requests-witness".to_string(),
        projection_version: used("projection-test-v1"),
        embedding_model_id: used("embedding-test-v1"),
        rerank_model_id: used("rerank-test-v1"),
        card_builder_version: used("card-test-v1"),
        profile_fingerprint: request.profile_fingerprint_identity().clone(),
        profile: ProfileBlock {
            top_k: request.top_k(),
            cand_k: request.cand_k(),
            cand_k_formula: request.cand_k_formula(),
            lanes: vec!["dense".to_string()],
        },
    }
}

/// Runs `request` through every final gate and returns the pending token unfinished.
fn pending(request: &RetrievalRequest) -> humaux_retrieval::envelope::PendingEnvelope<()> {
    let ledger = ledger::close(
        ledger::LedgerReads {
            expected: 1,
            done: 1,
            deleted: 0,
            skipped: 0,
            open_gaps: 0,
            pending: 0,
        },
        ledger::ProjectionReads {
            points_expected: 1,
            points_settled: 1,
            points_in_flight: 0,
            points_unsettled: 0,
            oldest_pending_age_secs: None,
        },
    );
    let pipeline = PipelineBlock {
        evidence: EvidenceBlock::no_batch(Some(1), CountScope::StreamLedger),
        knowledge: KnowledgeBlock {
            eligible: Some(1),
            processed: Some(1),
            waiting_key: Some(0),
            failed: Some(0),
            count_scope: CountScope::StreamLedger,
        },
        projection: build_projection_block(&ledger, Some(1), LAG).value,
    };
    let census = CensusResult::enumerated(
        ExactEnumeration::new("witness_enumeration_v1", 1, 1, 0).expect("exact census"),
    );
    envelope_outcome_block(
        request,
        CompletenessInputs {
            lane_status: &LaneStatus::Ok,
            census: &census,
            ledger: &ledger,
            pipeline: &pipeline,
            provenance: &provenance(request),
            visible: Some(1),
            context: None,
            mandatory_missing: 0,
            lag_threshold: LAG,
        },
        |_| Ok(()),
    )
    .expect("every final gate passes")
}

/// Each `RetrievalIntent` constructor moves exactly its own `{intent, completeness_class}` series
/// by one on `finish()`; a dropped pending moves nothing; all 16 seeded series keep the §41.2
/// label keys.
#[test]
fn finish_moves_exactly_the_series_of_its_intent_by_one() {
    let profile =
        resolve_registered_retrieval_profile(&BTreeMap::new()).expect("registered default profile");
    let text = RetrievalIntent::new(
        "ordinary semantic query".to_string(),
        vec![],
        Default::default(),
        Default::default(),
    )
    .expect("text intent");
    let cases = [
        (text, "text", "semantic_bounded"),
        (
            RetrievalIntent::trusted_context(),
            "context",
            "semantic_bounded",
        ),
        (
            RetrievalIntent::trusted_memory_get(MemoryId::new()),
            "direct_get",
            "exact",
        ),
        (
            RetrievalIntent::trusted_memory_enumerate(),
            "memory_enumerate",
            "exact",
        ),
    ];
    for (intent, want_intent, want_class) in cases {
        let request = build_request(intent, &profile).expect("typed request");
        let before = samples();
        assert_eq!(before.len(), 16, "4 intents x 4 classes seeded: {before:?}");

        drop(pending(&request));
        assert_eq!(
            samples(),
            before,
            "{want_intent}: a dropped pending recorded"
        );

        pending(&request).finish();
        let after = samples();
        let mut expected = before.clone();
        *expected
            .get_mut(&key(want_intent, want_class))
            .unwrap_or_else(|| panic!("{want_intent}/{want_class} not seeded")) += 1.0;
        assert_eq!(after, expected, "{want_intent}: exactly one series +1");
    }
}
