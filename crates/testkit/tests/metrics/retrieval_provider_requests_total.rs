// witness: family=retrieval_provider_requests_total labels=provider,purpose,region,result
//! `testkit::tests::metrics::retrieval_provider_requests_total` — Metric Witness for
//!   `retrieval_provider_requests_total{provider,purpose,region,result}` (§80.2 W-side; registry row §41.2; §19
//!   Provider Plane Observability).
//! Depends-on: crates=[]; services=[]; env=[]; modules=[retrieval-provider::admission, retrieval-provider::health,
//!   retrieval-provider::metrics]
//! Called-by: []
//! Invariants: [triggers the real record_provider_call and asserts a counter delta per
//!   {provider,purpose,region,result}, including the circuit_open value a completed call never produces]
//! Spec: Baseline §19
//!
//! Actively triggers the family's real production emit point,
//! `retrieval_provider::metrics::record_provider_call` — the sole `.inc(`/`.observe(` call
//! site for all four `retrieval_provider_*` families (§19 "每次外呼收尾 · 1") — and asserts a
//! real observed counter delta per `{provider,purpose,region,result}` combination this task's
//! implementation can reach, including the frozen `circuit_open` result value a completed
//! provider call never produces on its own (§19 Circuit Breaker).

use humaux_retrieval_provider::admission::RetrievalPurpose;
use humaux_retrieval_provider::health::{CIRCUIT_OPEN_RESULT_LABEL, ProviderCallOutcome};
use humaux_retrieval_provider::metrics::{
    Currency, Provider, Region, record_provider_call, retrieval_provider_requests_total_count,
};

/// Every `{provider,purpose,region,result}` combination this witness can reach must be
/// emittable and independently observable: counter delta is exactly +1 per call.
#[test]
fn every_reachable_label_combination_increments_by_exactly_one() {
    let cases: Vec<(Provider, RetrievalPurpose, Region, &'static str)> = vec![
        (
            Provider::DashScope,
            RetrievalPurpose::Embedding,
            Region::CnHangzhou,
            ProviderCallOutcome::Success.metric_result_label(),
        ),
        (
            Provider::DashScope,
            RetrievalPurpose::Rerank,
            Region::CnShanghai,
            ProviderCallOutcome::Http429.metric_result_label(),
        ),
        (
            Provider::Custom,
            RetrievalPurpose::Embedding,
            Region::CnBeijing,
            ProviderCallOutcome::Http5xx.metric_result_label(),
        ),
        (
            Provider::Custom,
            RetrievalPurpose::Rerank,
            Region::ApSoutheast1,
            CIRCUIT_OPEN_RESULT_LABEL,
        ),
    ];

    for (provider, purpose, region, result) in cases {
        let before = retrieval_provider_requests_total_count(provider, purpose, region, result);
        record_provider_call(provider, purpose, region, result, 10, 5, Currency::Usd, 0.1);
        assert_eq!(
            retrieval_provider_requests_total_count(provider, purpose, region, result),
            before + 1,
            "retrieval_provider_requests_total{{provider,purpose,region,result={result}}} must \
             increment exactly once per record_provider_call (§19)"
        );
    }
}

/// sample_count > 0 for the family as a whole (§80.2 D6 sentinel semantics).
#[test]
fn family_has_nonzero_samples_after_positive_path() {
    let provider = Provider::DashScope;
    let purpose = RetrievalPurpose::Embedding;
    let region = Region::CnHangzhou;
    let result = ProviderCallOutcome::Success.metric_result_label();
    let before = retrieval_provider_requests_total_count(provider, purpose, region, result);
    record_provider_call(provider, purpose, region, result, 1, 1, Currency::Usd, 0.01);
    assert!(
        retrieval_provider_requests_total_count(provider, purpose, region, result) > before,
        "family must have samples > 0"
    );
}
