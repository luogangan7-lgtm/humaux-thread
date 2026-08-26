// witness: family=retrieval_provider_tokens_total labels=provider,purpose
//! Metric Witness for `retrieval_provider_tokens_total{provider,purpose}` (§80.2 W-side;
//! registry row §41.2; §19 Retrieval Provider Plane's token-denominated budget/quota input).
//!
//! Actively triggers `retrieval_provider::metrics::record_provider_call` — the same sole
//! emit point the sibling `retrieval_provider_*` witnesses exercise — and asserts a real
//! observed counter delta, by the recorded token amount (not a flat +1), per
//! `{provider,purpose}` combination.

use humaux_retrieval_provider::admission::RetrievalPurpose;
use humaux_retrieval_provider::health::ProviderCallOutcome;
use humaux_retrieval_provider::metrics::{
    Currency, Provider, Region, record_provider_call, retrieval_provider_tokens_total_count,
};

/// Every `{provider,purpose}` combination this witness can reach must be emittable and
/// independently observable: counter delta equals the recorded `input_tokens` amount.
#[test]
fn every_reachable_label_combination_adds_its_own_token_amount() {
    let result = ProviderCallOutcome::Success.metric_result_label();
    let cases: Vec<(Provider, RetrievalPurpose, u64)> = vec![
        (Provider::DashScope, RetrievalPurpose::Embedding, 128),
        (Provider::DashScope, RetrievalPurpose::Rerank, 4096),
        (Provider::Custom, RetrievalPurpose::Embedding, 256),
        (Provider::Custom, RetrievalPurpose::Rerank, 512),
    ];

    for (provider, purpose, tokens) in cases {
        let before = retrieval_provider_tokens_total_count(provider, purpose);
        record_provider_call(
            provider,
            purpose,
            Region::CnHangzhou,
            result,
            tokens,
            1,
            Currency::Usd,
            0.01,
        );
        assert_eq!(
            retrieval_provider_tokens_total_count(provider, purpose),
            before + tokens,
            "retrieval_provider_tokens_total{{provider,purpose}} must grow by exactly the \
             recorded token amount per record_provider_call (§19)"
        );
    }
}

/// sample_count > 0 for the family as a whole (§80.2 D6 sentinel semantics).
#[test]
fn family_has_nonzero_samples_after_positive_path() {
    let provider = Provider::DashScope;
    let purpose = RetrievalPurpose::Embedding;
    let before = retrieval_provider_tokens_total_count(provider, purpose);
    record_provider_call(
        provider,
        purpose,
        Region::CnHangzhou,
        ProviderCallOutcome::Success.metric_result_label(),
        1,
        1,
        Currency::Usd,
        0.01,
    );
    assert!(
        retrieval_provider_tokens_total_count(provider, purpose) > before,
        "family must have samples > 0"
    );
}
