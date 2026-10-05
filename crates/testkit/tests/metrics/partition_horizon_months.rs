// witness: family=partition_horizon_months labels=table
//! `testkit::tests::metrics::partition_horizon_months` — Metric Witness for `partition_horizon_months{table}` (§80.2
//!   W-side; registry row §41.2 「PARTITIONS 任务每次成功运行收尾 · 1」; consumers §42 PartitionHorizonShort /
//!   PartitionHorizonExhausted / PartitionHorizonAbsent).
//! Depends-on: crates=[]; services=[]; env=[]; modules=[adapters::maintenance_repo]
//! Called-by: []
//! Invariants: [drives the family's one `.set()` (`maintenance_repo::set_partition_horizon_months`, called by
//!   `maintenance::serve` after each successful PARTITIONS run) and asserts the exact value of every closed `table`
//!   slot; drives the reset and asserts the family has no value at all afterwards, never the last one]
//! Spec: Baseline §41.2; §42; §80.2; ADR-0063 D-F; ADR-0063 D-K
//!
//! The real run's values are asserted against PostgreSQL by `bins/maintenance/tests/serve.rs` (T-K1, T-K3). Compiled
//! and run standalone by `cargo xtask metrics-registry` (`run_witness_probe`).

use humaux_adapters::maintenance_repo::{
    PartitionTable, partition_horizon_months, reset_partition_horizon_months,
    set_partition_horizon_months,
};

/// One set publishes every closed key's value in `PartitionTable::ALL` order; a reset removes them all (§42
/// PartitionHorizonAbsent reads absence, so a failed run must never leave the last value).
#[test]
fn set_publishes_every_table_and_reset_removes_the_family() {
    let months: [i64; PartitionTable::ALL.len()] = [3, 2, 1, 0, -1, 3];
    set_partition_horizon_months(months);
    assert_eq!(partition_horizon_months(), Some(months));
    assert_eq!(PartitionTable::ALL.map(PartitionTable::label).len(), months.len());
    reset_partition_horizon_months();
    assert_eq!(partition_horizon_months(), None, "a failed run renders no sample");
}
