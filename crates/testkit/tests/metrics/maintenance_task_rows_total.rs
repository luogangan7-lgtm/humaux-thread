// witness: family=maintenance_task_rows_total labels=task
//! `testkit::tests::metrics::maintenance_task_rows_total` — Metric Witness for `maintenance_task_rows_total{task}`
//!   (§80.2 W-side; registry row §41.2 「同一处按门返回的 affected 累加 · 1」; consumers §53 growth and the runbook's
//!   "Scheduled maintenance").
//! Depends-on: crates=[]; services=[]; env=[]; modules=[adapters::maintenance_repo]
//! Called-by: []
//! Invariants: [drives the family's one emit (`maintenance_repo::count_task_call`) and asserts the exact delta: the
//!   affected rows of a committed call, nothing for a failed one, never mere presence]
//! Spec: Baseline §41.2; §80.2; ADR-0061 D-C; ADR-0062 D-S
//!
//! Compiled and run standalone by `cargo xtask metrics-registry` (`run_witness_probe`); the real cycle's rows are
//! asserted against PostgreSQL by `bins/maintenance/tests/serve.rs::one_cycle_answers_200_and_counts_each_due_task`.

use humaux_adapters::maintenance_repo::{
    MaintenanceTask, count_task_call, maintenance_task_rows_total,
};

/// A committed call adds its affected rows to its own task; a failed call adds none.
#[test]
fn committed_calls_add_their_affected_rows_and_failed_calls_none() {
    for task in MaintenanceTask::ALL {
        let before = maintenance_task_rows_total(task);
        count_task_call(task, Some(7));
        count_task_call(task, Some(0));
        count_task_call(task, None);
        assert_eq!(
            maintenance_task_rows_total(task),
            before + 7,
            "{}: rows grow by the affected count of committed calls only (§41.2)",
            task.label()
        );
    }
}
