//! `domain::tests::egress_topology_ui` — §7.3 T4.1 compile-time sentinels: `EgressPermit` has no construction path
//!   outside `crates/domain/src/egress.rs` (`fail_*`), while the one sanctioned path (`authorize`) works fine from an
//!   external caller (`pass_*`) — same `trybuild` pattern `crates/adapters/tests/pool_typestate.rs` already uses for
//!   its own §6.2.3 topology.
//! Depends-on: crates=[trybuild]; services=[]; env=[]; modules=[]
//! Called-by: [cargo-test]
//! Invariants: []
//! Spec: Baseline §6.2.3; §7.3

#[test]
fn egress_permit_construction_topology() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass_authorize_mints_permit.rs");
    t.compile_fail("tests/ui/fail_egress_permit_struct_literal.rs");
    t.compile_fail("tests/ui/fail_egress_permit_issue_call.rs");
}
