//! `adapters::tests::pool_typestate` — G6-DB1 compile-time sentinels (§6.2.3 assertions C/D): the seven typed pool
//!   wrappers must be usable through a correctly-typed port (`pass_*`) and must NOT be substitutable for each other
//!   or bypassable via `From`/`Deref`/raw field access (`fail_*`).
//! Depends-on: crates=[trybuild]; services=[]; env=[]; modules=[]
//! Called-by: [cargo-test]
//! Invariants: [compile-time only: no runtime services; failure = a wrapper became cross-substitutable or bypassable,
//!   or the count-only StreamCountFilter became convertible into a search filter (ADR-0057 D-C)]
//! Spec: Baseline §6.2.3; §17.1; ADR-0057
//!
//! `tests/ui/pass_*.rs` covers five wrappers currently used by compile-time ports;
//! role-match runtime coverage covers all seven roles.
//!
//! `tests/ui/fail_*.rs` covers both this task's four names (`fail_remember_with_batch_pool`,
//! `fail_pool_cross_from`, `fail_deref_inner`, `fail_raw_pool_field`) and, so assertion D's
//! own four-name list is fully satisfied too, the two spec names it doesn't share
//! (`fail_begin_batch_with_runtime_pool`, `fail_consolidation_with_private_pool`) — six
//! files total, no name collision between the two lists.

#[test]
fn pool_typestate_fixtures() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass_runtime_pool.rs");
    t.pass("tests/ui/pass_batch_pool.rs");
    t.pass("tests/ui/pass_consolidation_pool.rs");
    t.pass("tests/ui/pass_private_pool.rs");
    t.pass("tests/ui/pass_public_pool.rs");
    t.compile_fail("tests/ui/fail_remember_with_batch_pool.rs");
    t.compile_fail("tests/ui/fail_begin_batch_with_runtime_pool.rs");
    t.compile_fail("tests/ui/fail_consolidation_with_private_pool.rs");
    t.compile_fail("tests/ui/fail_pool_cross_from.rs");
    t.compile_fail("tests/ui/fail_deref_inner.rs");
    t.compile_fail("tests/ui/fail_raw_pool_field.rs");
    t.compile_fail("tests/ui/fail_contribution_cross_roles.rs");
    // ADR-0057 D-C: the ops stream count filter cannot become a dense search filter.
    t.compile_fail("tests/ui/fail_stream_count_filter_in_dense_search.rs");
}
