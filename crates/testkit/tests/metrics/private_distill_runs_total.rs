// witness: family=private_distill_runs_total labels=-
//! `testkit::tests::metrics::private_distill_runs_total` — Metric Witness for `private_distill_runs_total` (§80.2
//!   W-side; registry row §41.2 「§11 每次 run · 1」; consumers §39 stage liveness and the §42 no-output stage).
//! Depends-on: crates=[]; services=[]; env=[]; modules=[adapters::distill_repo]
//! Called-by: []
//! Invariants: [drives the family's one emit (`distill_repo::count_committed_distill`, whose one production caller
//!   `commit_distill_write` metrics-registry D5 pins) and asserts the exact delta, never mere presence]
//! Spec: Baseline §41.2; §42; §80.2; ADR-0061 D-C
//!
//! The encoder seeds a never-incremented counter at 0 (ADR-0061 D-A), so presence proves nothing: this
//! asserts the increment. The post-commit placement on the real path is proven against PostgreSQL by
//! `bins/private-worker/tests/distill_hop_e2e.rs` (d5 counts a committed write once, d5c counts a rolled-back
//! write not at all). Compiled and run standalone by `cargo xtask metrics-registry` (`run_witness_probe`).

use humaux_adapters::distill_repo::{
    count_committed_distill, private_distill_outputs_total, private_distill_runs_total,
};

/// Each committed write adds the runs it finished — 0 for an already finished run — and never its outputs.
#[test]
fn committed_writes_add_exactly_the_runs_finished() {
    let (runs, outputs) = (private_distill_runs_total(), private_distill_outputs_total());
    count_committed_distill(1, 0);
    count_committed_distill(1, 0);
    count_committed_distill(0, 0);
    assert_eq!(
        private_distill_runs_total(),
        runs + 2,
        "private_distill_runs_total must grow by one per finished run (§41.2, §42 no-output stage)"
    );
    assert_eq!(private_distill_outputs_total(), outputs);
}
