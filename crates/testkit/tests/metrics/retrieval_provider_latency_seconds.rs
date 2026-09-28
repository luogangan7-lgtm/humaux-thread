// witness: family=retrieval_provider_latency_seconds labels=provider,purpose,region
//! `testkit::tests::metrics::retrieval_provider_latency_seconds` — Metric Witness for
//!   `retrieval_provider_latency_seconds{provider,purpose,region}` (§80.2 W-side; registry row §41.2; §19 Provider
//!   Health / Circuit Breaker's own latency input).
//! Depends-on: crates=[]; services=[]; env=[]; modules=[retrieval-provider::admission, retrieval-provider::health,
//!   retrieval-provider::metrics]
//! Called-by: []
//! Invariants: [triggers the real record_provider_call and asserts observed sample-count and sum deltas per
//!   {provider,purpose,region}]
//! Spec: none
//!
//! Actively triggers `retrieval_provider::metrics::record_provider_call` — the same sole
//! emit point [`retrieval_provider_requests_total`'s witness](../retrieval_provider_requests_total.rs)
//! exercises — and asserts a real observed sample-count and sum delta per
//! `{provider,purpose,region}` combination.

use humaux_retrieval_provider::admission::RetrievalPurpose;
use humaux_retrieval_provider::health::ProviderCallOutcome;
use humaux_retrieval_provider::metrics::{
    Currency, Provider, Region, record_provider_call, retrieval_provider_latency_seconds_sample_count,
    retrieval_provider_latency_seconds_sum,
};

/// Every `{provider,purpose,region}` combination this witness can reach must be emittable and
/// independently observable: sample count delta is exactly +1, sum delta exactly the recorded
/// latency, per call.
#[test]
fn every_reachable_label_combination_records_its_own_sample() {
    let result = ProviderCallOutcome::Success.metric_result_label();
    let cases: Vec<(Provider, RetrievalPurpose, Region, f64)> = vec![
        (Provider::DashScope, RetrievalPurpose::Embedding, Region::CnHangzhou, 0.12),
        (Provider::DashScope, RetrievalPurpose::Rerank, Region::CnShanghai, 0.34),
        (Provider::Custom, RetrievalPurpose::Embedding, Region::CnBeijing, 0.56),
        (Provider::Custom, RetrievalPurpose::Rerank, Region::ApSoutheast1, 0.78),
    ];

    for (provider, purpose, region, latency_seconds) in cases {
        let before_count = retrieval_provider_latency_seconds_sample_count(provider, purpose, region);
        let before_sum = retrieval_provider_latency_seconds_sum(provider, purpose, region);

        record_provider_call(provider, purpose, region, result, 1, 1, Currency::Usd, latency_seconds);

        assert_eq!(
            retrieval_provider_latency_seconds_sample_count(provider, purpose, region),
            before_count + 1,
            "retrieval_provider_latency_seconds{{provider,purpose,region}} sample count must \
             increment exactly once per record_provider_call (§19)"
        );
        let after_sum = retrieval_provider_latency_seconds_sum(provider, purpose, region);
        assert!(
            (after_sum - (before_sum + latency_seconds)).abs() < 1e-9,
            "sum must grow by exactly the recorded latency"
        );
    }
}

/// sample_count > 0 for the family as a whole (§80.2 D6 sentinel semantics).
#[test]
fn family_has_nonzero_samples_after_positive_path() {
    let provider = Provider::DashScope;
    let purpose = RetrievalPurpose::Embedding;
    let region = Region::CnHangzhou;
    let before = retrieval_provider_latency_seconds_sample_count(provider, purpose, region);
    record_provider_call(
        provider,
        purpose,
        region,
        ProviderCallOutcome::Success.metric_result_label(),
        1,
        1,
        Currency::Usd,
        0.01,
    );
    assert!(
        retrieval_provider_latency_seconds_sample_count(provider, purpose, region) > before,
        "family must have samples > 0"
    );
}
