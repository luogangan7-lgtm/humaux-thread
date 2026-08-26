// witness: family=retrieval_provider_cost_total labels=provider,purpose,currency
//! Metric Witness for `retrieval_provider_cost_total{provider,purpose,currency}` (§80.2
//! W-side; registry row §41.2; §19 "Retrieval 成本模型" cost-anomaly input).
//!
//! Actively triggers `retrieval_provider::metrics::record_provider_call` — the same sole
//! emit point the sibling `retrieval_provider_*` witnesses exercise — and asserts a real
//! observed counter delta, by the recorded minor-unit amount, per `{provider,purpose,currency}`
//! combination.

use humaux_retrieval_provider::admission::RetrievalPurpose;
use humaux_retrieval_provider::health::ProviderCallOutcome;
use humaux_retrieval_provider::metrics::{
    Currency, Provider, Region, record_provider_call, retrieval_provider_cost_total_count,
};

/// Every `{provider,purpose,currency}` combination this witness can reach must be emittable
/// and independently observable: counter delta equals the recorded `cost_minor_units` amount.
#[test]
fn every_reachable_label_combination_adds_its_own_cost_amount() {
    let result = ProviderCallOutcome::Success.metric_result_label();
    let cases: Vec<(Provider, RetrievalPurpose, Currency, u64)> = vec![
        (Provider::DashScope, RetrievalPurpose::Embedding, Currency::Usd, 42),
        (Provider::DashScope, RetrievalPurpose::Rerank, Currency::Cny, 300),
        (Provider::Custom, RetrievalPurpose::Embedding, Currency::Usd, 7),
        (Provider::Custom, RetrievalPurpose::Rerank, Currency::Cny, 99),
    ];

    for (provider, purpose, currency, cost_minor_units) in cases {
        let before = retrieval_provider_cost_total_count(provider, purpose, currency);
        record_provider_call(
            provider,
            purpose,
            Region::CnHangzhou,
            result,
            1,
            cost_minor_units,
            currency,
            0.01,
        );
        assert_eq!(
            retrieval_provider_cost_total_count(provider, purpose, currency),
            before + cost_minor_units,
            "retrieval_provider_cost_total{{provider,purpose,currency}} must grow by exactly \
             the recorded cost amount per record_provider_call (§19)"
        );
    }
}

/// sample_count > 0 for the family as a whole (§80.2 D6 sentinel semantics).
#[test]
fn family_has_nonzero_samples_after_positive_path() {
    let provider = Provider::DashScope;
    let purpose = RetrievalPurpose::Embedding;
    let currency = Currency::Usd;
    let before = retrieval_provider_cost_total_count(provider, purpose, currency);
    record_provider_call(
        provider,
        purpose,
        Region::CnHangzhou,
        ProviderCallOutcome::Success.metric_result_label(),
        1,
        1,
        currency,
        0.01,
    );
    assert!(
        retrieval_provider_cost_total_count(provider, purpose, currency) > before,
        "family must have samples > 0"
    );
}
