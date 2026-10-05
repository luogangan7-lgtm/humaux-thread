// witness: family=maintenance_task_runs_total labels=task,outcome
//! `testkit::tests::metrics::maintenance_task_runs_total` — Metric Witness for
//!   `maintenance_task_runs_total{task,outcome}` (§80.2 W-side; registry row §41.2 「§4.2 每次门调用收尾 · 1」;
//!   consumers §39 stage liveness and §42 MaintenanceTaskFailing).
//! Depends-on: crates=[]; services=[]; env=[]; modules=[adapters::maintenance_repo]
//! Called-by: []
//! Invariants: [drives the family's one emit (`maintenance_repo::count_task_call`, called once per tenant door call
//!   by `maintenance::serve`'s cycle) and asserts the exact delta of every closed `task` x `outcome` pair, never
//!   mere presence]
//! Spec: Baseline §41.2; §42; §80.2; ADR-0061 D-C; ADR-0062 D-S
//!
//! The encoder seeds a never-incremented counter at 0 (ADR-0061 D-A), so presence proves nothing: this asserts the
//! increment, per label pair, and that a failed call never counts as ok (the MaintenanceTaskFailing matcher reads
//! `outcome="failed"`). The real cycle's counts are asserted against PostgreSQL by
//! `bins/maintenance/tests/serve.rs::one_cycle_answers_200_and_counts_each_due_task`. Compiled and run standalone
//! by `cargo xtask metrics-registry` (`run_witness_probe`).

use humaux_adapters::maintenance_repo::{
    MaintenanceTask, TaskOutcome, count_task_call, maintenance_task_runs_total,
};

/// One call adds exactly one run to its own `{task,outcome}` pair and to no other.
#[test]
fn each_call_counts_one_run_under_its_task_and_outcome() {
    for task in MaintenanceTask::ALL {
        for (affected, outcome) in [(Some(3), TaskOutcome::Ok), (None, TaskOutcome::Failed)] {
            let before: Vec<u64> = MaintenanceTask::ALL
                .iter()
                .flat_map(|&t| TaskOutcome::ALL.map(|o| maintenance_task_runs_total(t, o)))
                .collect();
            count_task_call(task, affected);
            let after: Vec<u64> = MaintenanceTask::ALL
                .iter()
                .flat_map(|&t| TaskOutcome::ALL.map(|o| maintenance_task_runs_total(t, o)))
                .collect();
            let moved: Vec<usize> = (0..before.len()).filter(|&i| after[i] != before[i]).collect();
            let index = MaintenanceTask::ALL.iter().position(|&t| t == task).unwrap() * 2
                + TaskOutcome::ALL.iter().position(|&o| o == outcome).unwrap();
            assert_eq!(
                moved,
                vec![index],
                "{}/{}: one call moves exactly its own pair (§41.2, §42 MaintenanceTaskFailing)",
                task.label(),
                outcome.label()
            );
            assert_eq!(after[index], before[index] + 1, "{}: +1 run", task.label());
        }
    }
}
