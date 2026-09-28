// witness: family=degrade_total labels=code
//! `testkit::tests::metrics::degrade_total` — Metric Witness for `degrade_total{code}` (§80.2 W-side; registry row
//!   §41.2).
//! Depends-on: crates=[humaux-telemetry]; services=[]; env=[]; modules=[telemetry::degrade]
//! Called-by: []
//! Invariants: []
//! Spec: Baseline §53.1
//!
//! Actively triggers the family's minimal positive path — one `abstain()` per
//! `DegradeCode` variant — and asserts a real observed delta per label value.
//! §53.1: `abstain()` is the sole increment point, so this witness exercises the
//! production emit path, not a test double. Compiled and run standalone by
//! `cargo xtask metrics-registry` (see `run_witness_probe`).

use humaux_telemetry::degrade::{DegradeCode, abstain, degrade_total_count};

/// Every label value (PascalCase variant name, §53.2) must be emittable and
/// observable: counter delta is exactly +1 per abstain, per code.
#[test]
fn every_code_label_value_increments_by_exactly_one() {
    for code in DegradeCode::ALL {
        let before = degrade_total_count(code);
        let out = abstain(code, ());
        assert_eq!(
            degrade_total_count(code),
            before + 1,
            "degrade_total{{code={}}} must increment exactly once per abstain (§53.1)",
            code.as_str()
        );
        assert!(
            out.degradations.iter().any(|&c| c == code),
            "abstain() must record the degradation on the Outcome (§53.2)"
        );
    }
}

/// sample_count > 0 for the family as a whole (§80.2 D6 sentinel semantics):
/// after the loop above ran, at least one observable sample exists per variant.
#[test]
fn family_has_nonzero_samples_after_positive_path() {
    // Trigger once more deterministically so this test does not depend on
    // execution order of the harness (§79: no hidden inter-test coupling).
    let code = DegradeCode::ALL[0];
    abstain(code, ());
    assert!(degrade_total_count(code) > 0, "family must have samples > 0");
}
