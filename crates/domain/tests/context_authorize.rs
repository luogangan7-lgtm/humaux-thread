//! §25.4 × ADR-0019 compile-time sentinel: `ConfirmedUserActor` — the only thing
//! `authorize_pinned` accepts for a PINNED `BindingGrant` — has no construction path outside
//! `humaux_domain::context` (`fail_*`, compile-fail). Same `trybuild` technique as
//! `consolidate_typestate.rs`. The path name is the one architecture-check A3 already
//! whitelists as a legitimate `authorize_pinned(` caller.

#[test]
fn confirmed_user_actor_has_no_external_mint() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/fail_confirmed_user_actor_literal.rs");
}
