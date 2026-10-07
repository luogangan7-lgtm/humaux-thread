// witness: family=admission_rejected_total labels=class,reason
//! `testkit::tests::metrics::admission_rejected_total` — Metric Witness for `admission_rejected_total{class,reason}`
//!   (§80.2 W-side; registry row §41.2 「§67 admission control 返 503 处 · 1」; consumer §42 AdmissionRejected).
//! Depends-on: crates=[humaux-telemetry]; services=[]; env=[]; modules=[telemetry::admission]
//! Called-by: []
//! Invariants: [drives the family's one emit (`telemetry::admission::count_admission_rejected`, called once per 503 by
//!   `gateway::admission`) and asserts the exact per-reason delta and the rendered label set, never mere presence]
//! Spec: Baseline §41.2; §42; §67.2; §80.2; ADR-0061 D-A; ADR-0065 D-C
//!
//! The encoder seeds every reason at 0 (ADR-0061 D-A), so presence proves nothing: this asserts the increment. The
//! 503 path itself is proven by `gateway::admission::tests` (one rejection per refused request). Compiled and run
//! standalone by `cargo xtask metrics-registry` (`run_witness_probe`).

use humaux_telemetry::admission::{
    AdmissionRefusal, admission_rejected_total, count_admission_rejected, render,
};

/// Each refusal adds exactly 1 to its own reason and nothing to the others.
#[test]
fn each_refusal_adds_one_to_its_own_reason() {
    for reason in AdmissionRefusal::ALL {
        let before = AdmissionRefusal::ALL.map(admission_rejected_total);
        count_admission_rejected(reason);
        for (i, other) in AdmissionRefusal::ALL.into_iter().enumerate() {
            let want = before[i] + u64::from(other == reason);
            assert_eq!(
                admission_rejected_total(other),
                want,
                "admission_rejected_total{{reason={}}} after one {} refusal (§41.2: 1 per 503)",
                other.as_str(),
                reason.as_str()
            );
        }
    }
}

/// The exposition carries the §41.2 label keys `class`, `reason` with `class="gateway_inbound"` and a non-zero sample.
#[test]
fn family_renders_class_and_reason_with_samples() {
    count_admission_rejected(AdmissionRefusal::QueueFull);
    let mut out = String::new();
    render(&mut out);
    assert!(out.contains("# TYPE admission_rejected_total counter\n"), "{out}");
    let line = out
        .lines()
        .find(|l| l.starts_with("admission_rejected_total{class=\"gateway_inbound\",reason=\"queue_full\"} "))
        .unwrap_or_else(|| panic!("no queue_full series in {out}"));
    let value: f64 = line.rsplit(' ').next().and_then(|v| v.parse().ok()).expect("a sample value");
    assert!(value >= 1.0, "{line}");
}
