//! Provider-free static guard for the B2 typed SQL binding surface.

#[test]
fn binds_only_the_eight_exposed_0131_functions() {
    let source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/contribution_execution_repo.rs"
    ))
    .expect("repository source");
    for name in [
        "enqueue_contribution_execution",
        "reserve_contribution_a",
        "reserve_contribution_b",
        "complete_contribution_a_exact",
        "complete_contribution_b_exact",
        "commit_contribution_candidate",
        "settle_contribution_terminal_job",
        "mark_contribution_reconciliation_required",
    ] {
        assert!(source.contains(name), "missing {name}");
    }
    for name in [
        "require_contribution_execution_lease",
        "reserve_contribution_execution_call",
        "settle_contribution_job_if_live",
    ] {
        assert!(
            !source.contains(&format!("private.{name}(")),
            "must not bind internal {name}"
        );
    }
    assert!(!source.contains("INSERT INTO private.contribution_executions"));
    assert!(!source.contains("UPDATE ops.model_call_ledger"));
}
