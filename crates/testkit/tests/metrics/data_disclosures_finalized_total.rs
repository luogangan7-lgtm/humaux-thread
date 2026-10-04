// witness: family=data_disclosures_finalized_total labels=outcome
//! `testkit::tests::metrics::data_disclosures_finalized_total` — Metric Witness for `data_disclosures_finalized_total{outcome}` (§80.2 W-side; registry row §41.2:
//!   §7.4 finalized_at written; §53.5 INV-2 denominator).
//! Depends-on: crates=[humaux-domain, humaux-telemetry]; services=[]; env=[]; modules=[domain::ticket_family, telemetry::health]
//! Called-by: []
//! Invariants: []
//! Spec: Baseline §41.2; §80.2; ADR-0061 D-D
//!
//! Publishes a fixture snapshot through `telemetry::health::publish` (the family's one emit) and
//! reads the family back from `telemetry::health::render`, the exposition `/metrics` serves.
//! Every field of the fixture carries a distinct value, so an emit wired to the wrong field is red.
//! Compiled and run standalone by `cargo xtask metrics-registry` (`run_witness_probe`).

use humaux_domain::ticket_family::TicketFamily;
use humaux_telemetry::health::{AgeBucket, DisclosureOutcome, HealthSnapshot, publish, render};

fn fixture() -> HealthSnapshot {
    HealthSnapshot {
        jobs_pending: 7,
        jobs_processing: 11,
        jobs_waiting_key: 13,
        jobs_dead: 17,
        oldest_pending_age_seconds: 19.5,
        projection_lag_events: 23,
        processing_gaps: vec![(TicketFamily::PrivateMemory, 29)],
        reserved_unfinalized: vec![
            (AgeBucket::Le10s, 2),
            (AgeBucket::Le60s, 3),
            (AgeBucket::Gt60s, 5),
        ],
        finalized_since_watermark: vec![
            (DisclosureOutcome::Success, 31),
            (DisclosureOutcome::Failed, 37),
            (DisclosureOutcome::Denied, 41),
        ],
    }
}

/// `(label keys, value)` of every sample of `family` in one render of the health exposition.
fn samples(family: &str) -> Vec<(Vec<String>, f64)> {
    let mut out = String::new();
    render(&mut out);
    out.lines()
        .filter(|l| !l.starts_with('#'))
        .filter(|l| l.split(['{', ' ']).next() == Some(family))
        .map(|l| {
            let (series, value) = l.rsplit_once(' ').expect("sample line");
            let keys = series
                .split_once('{')
                .map(|(_, rest)| {
                    rest.trim_end_matches('}')
                        .split(',')
                        .map(|kv| kv.split('=').next().unwrap_or("").to_string())
                        .collect()
                })
                .unwrap_or_default();
            (keys, value.parse().expect("numeric sample"))
        })
        .collect()
}

/// One sample per outcome, label key `outcome` only; two publishes add their deltas (a counter,
/// never a gauge).
#[test]
fn every_outcome_accumulates_its_finalized_delta() {
    publish(&fixture());
    publish(&fixture());
    let key = vec!["outcome".to_string()];
    assert_eq!(
        samples("data_disclosures_finalized_total"),
        vec![(key.clone(), 62.0), (key.clone(), 74.0), (key, 82.0)]
    );
    let mut out = String::new();
    render(&mut out);
    assert!(
        out.contains("# TYPE data_disclosures_finalized_total counter\n"),
        "{out}"
    );
    assert!(
        out.contains("data_disclosures_finalized_total{outcome=\"SUCCESS\"} 62\n"),
        "{out}"
    );
}
