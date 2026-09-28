//! `infra-cell::tests::intra_cell_topology_ui` — §83.4 判据1 compile-time proof: `IntraCellResource` has no
//!   construction path that accepts a raw URL/string — same `trybuild` pattern
//!   `crates/domain/tests/egress_topology_ui.rs` already uses for its own §7.3 `EgressPermit` topology.
//! Depends-on: crates=[trybuild]; services=[]; env=[]; modules=[]
//! Called-by: [cargo-test]
//! Invariants: []
//! Spec: none

#[test]
fn intra_cell_resource_has_no_raw_url_constructor() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass_qdrant_rest_variant.rs");
    t.compile_fail("tests/ui/fail_resource_from_raw_url_string.rs");
}
