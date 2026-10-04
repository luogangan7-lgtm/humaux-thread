// witness: family=private_reasoning_usage_total labels=-
//! `testkit::tests::metrics::private_reasoning_usage_total` — Metric Witness for `private_reasoning_usage_total`
//!   (§80.2 W-side; registry row §41.2 「§11 推理调用返回读 usage · 1」; consumer §35 quota).
//! Depends-on: crates=[]; services=[]; env=[]; modules=[adapters::model_call_ledger]
//! Called-by: []
//! Invariants: [drives the family's one emit (`model_call_ledger::count_private_reasoning_usage`, whose one production
//!   caller `finalize_private_call` metrics-registry D5 pins) and asserts the token delta, never mere presence]
//! Spec: Baseline §41.2; §35; §80.2; ADR-0061 D-C
//!
//! The family counts tokens, not calls: input + output as the provider reported them; an unreported or
//! negative count adds 0. The real path (a finalized private call counted once, after its commit) is proven
//! against PostgreSQL by `bins/private-worker/tests/distill_hop_e2e.rs` d5c. Compiled and run standalone by
//! `cargo xtask metrics-registry` (`run_witness_probe`).

use humaux_adapters::model_call_ledger::{
    FinalizeCall, count_private_reasoning_usage, private_reasoning_usage_total,
};

/// A finalized call adds input + output tokens; unknown usage adds 0 and a negative count is never subtracted.
#[test]
fn a_finalized_call_adds_its_reported_input_and_output_tokens() {
    let before = private_reasoning_usage_total();
    count_private_reasoning_usage(&FinalizeCall {
        input_tokens: Some(1200),
        output_tokens: Some(34),
        ..FinalizeCall::default()
    });
    assert_eq!(
        private_reasoning_usage_total(),
        before + 1234,
        "private_reasoning_usage_total must grow by input + output tokens (§41.2, §35)"
    );
    count_private_reasoning_usage(&FinalizeCall::default());
    count_private_reasoning_usage(&FinalizeCall {
        input_tokens: Some(-5),
        output_tokens: Some(6),
        ..FinalizeCall::default()
    });
    assert_eq!(private_reasoning_usage_total(), before + 1240);
}
