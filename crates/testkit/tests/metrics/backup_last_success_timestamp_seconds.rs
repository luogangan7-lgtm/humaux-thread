// witness: family=backup_last_success_timestamp_seconds labels=target
//! `testkit::tests::metrics::backup_last_success_timestamp_seconds` — Metric Witness for `backup_last_success_timestamp_seconds` (§80.2 W-side; registry row §41.2:
//!   §44 DR_EVIDENCE run 收尾 · 1; consumers §42 BackupFailure).
//! Depends-on: crates=[humaux-telemetry]; services=[]; env=[]; modules=[telemetry::dr]
//! Called-by: []
//! Invariants: [drives the family's one `.set(` (`telemetry::dr::publish`, called by `maintenance::serve` after
//!   each successful DR_EVIDENCE run); `backup_last_success_timestamp_seconds` renders exactly one sample whose `target` is `local` and whose value is the reading's backup stop time]
//! Spec: Baseline §41.2; §42; §80.2; ADR-0064 D-K; ADR-0064 10.11 D
//!
//! The real run's values are asserted against PostgreSQL by `bins/maintenance/tests/serve.rs` (T-K1..T-K4). Compiled
//! and run standalone by `cargo xtask metrics-registry` (`run_witness_probe`).

use humaux_telemetry::dr::{DrReading, publish, render};

/// One publish of a fixture reading renders exactly these `backup_last_success_timestamp_seconds` samples (label `target` only ever `local`).
#[test]
fn backup_last_success_timestamp_seconds() {
    publish(&DrReading {
        backup_last_success: 1_788_224_400.0,
        restore_drill_last_success: 1_788_490_800.0,
        repo_bytes: 4_294_967_296,
        disk_free_bytes: [1_073_741_824, 8_053_063_679],
        budget_headroom_bytes: [-1_500, 2_147_483_648],
        wal_archive_failing: true,
    });
    let mut out = String::new();
    render(&mut out);
    let samples: Vec<&str> = out
        .lines()
        .filter(|l| !l.starts_with('#'))
        .filter(|l| l.split(['{', ' ']).next() == Some("backup_last_success_timestamp_seconds"))
        .collect();
    let expected: Vec<String> = [(
        "backup_last_success_timestamp_seconds{target=\"local\"}",
        "1788224400",
    )]
    .iter()
    .map(|(series, v)| format!("{series} {v}"))
    .collect();
    assert_eq!(samples, expected, "{out}");
}
