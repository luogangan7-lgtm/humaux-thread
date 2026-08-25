//! G6-DB1 compile-time sentinels (§6.2.3 assertions C/D): the four typed pool wrappers
//! must be usable through a correctly-typed port (`pass_*`) and must NOT be substitutable
//! for each other or bypassable via `From`/`Deref`/raw field access (`fail_*`).
//!
//! `tests/ui/pass_*.rs` covers §6.2.3 assertion C's four names (`pass_runtime_pool` /
//! `pass_batch_pool` / `pass_consolidation_pool` / `pass_private_pool`) — one per wrapper.
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
    t.compile_fail("tests/ui/fail_remember_with_batch_pool.rs");
    t.compile_fail("tests/ui/fail_begin_batch_with_runtime_pool.rs");
    t.compile_fail("tests/ui/fail_consolidation_with_private_pool.rs");
    t.compile_fail("tests/ui/fail_pool_cross_from.rs");
    t.compile_fail("tests/ui/fail_deref_inner.rs");
    t.compile_fail("tests/ui/fail_raw_pool_field.rs");
}
