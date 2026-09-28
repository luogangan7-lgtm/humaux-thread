//! `domain::tests::consolidate_typestate` — T4.6/T4.7 §11.8 compile-time sentinel: `AutoMutableMemoryId` must be
//!   reachable from an [`humaux_domain::consolidate::UnboundMemoryId`] (`pass_*`) and unreachable from a
//!   [`humaux_domain::consolidate::BoundMemoryId`] — i.e. from anything standing in for a Pinned/Mandatory memory
//!   (`fail_*`, compile-fail).
//! Depends-on: crates=[trybuild]; services=[]; env=[]; modules=[]
//! Called-by: [cargo-test]
//! Invariants: []
//! Spec: Baseline §11.8
//!
//! Same `trybuild` technique
//! `crates/adapters/tests/pool_typestate.rs` uses for the four typed DB pools.

#[test]
fn auto_mutable_memory_id_typestate_fixtures() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass_auto_mutable_from_unbound.rs");
    t.compile_fail("tests/ui/fail_bound_to_auto_mutable.rs");
}
