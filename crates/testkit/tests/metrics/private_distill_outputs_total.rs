// witness: family=private_distill_outputs_total labels=-
//! `testkit::tests::metrics::private_distill_outputs_total` — Metric Witness for `private_distill_outputs_total`
//!   (§80.2 W-side; registry row §41.2 「§11 每条产出 · 1」; consumers §39 stage liveness and the §42 no-output stage).
//! Depends-on: crates=[]; services=[]; env=[]; modules=[adapters::distill_repo]
//! Called-by: []
//! Invariants: [drives the family's one emit (`distill_repo::count_committed_distill`, whose one production caller
//!   `commit_distill_write` metrics-registry D5 pins) and asserts the exact delta, never mere presence]
//! Spec: Baseline §41.2; §42; §80.2; ADR-0061 D-C
//!
//! The encoder seeds a never-incremented counter at 0 (ADR-0061 D-A), so presence proves nothing: this
//! asserts one increment per committed memory record. The post-commit placement on the real path is proven against
//! PostgreSQL by `bins/private-worker/tests/distill_hop_e2e.rs` (d5 / d5c). Compiled and run standalone by
//! `cargo xtask metrics-registry` (`run_witness_probe`).

use humaux_adapters::distill_repo::{
    count_committed_distill, private_distill_outputs_total, private_distill_runs_total,
};

/// A committed write of three records adds exactly three outputs and one run (§42: outputs that stop rising while
/// runs rise is the no-output stage).
#[test]
fn each_committed_record_adds_exactly_one() {
    let (runs, outputs) = (private_distill_runs_total(), private_distill_outputs_total());
    count_committed_distill(1, 3);
    assert_eq!(
        private_distill_outputs_total(),
        outputs + 3,
        "private_distill_outputs_total must grow by one per committed memory record (§41.2)"
    );
    assert_eq!(private_distill_runs_total(), runs + 1);
}
