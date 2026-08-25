//! Cargo integration-test entry point for `tests/pair/` (G52-5).
//!
//! Same reason as `fault_main.rs`: cargo does not discover files in a
//! `tests/` subdirectory on its own, so each pair file is pulled in here as
//! a `#[path]` module. §52.2 registry currently has 2 rows ⇒ 4 files
//! (terminal + degraded per row).

#[path = "pair/cannot_establish_completeness_degraded.rs"]
mod cannot_establish_completeness_degraded;
#[path = "pair/cannot_establish_completeness_terminal.rs"]
mod cannot_establish_completeness_terminal;
#[path = "pair/projection_lag_degraded.rs"]
mod projection_lag_degraded;
#[path = "pair/projection_lag_terminal.rs"]
mod projection_lag_terminal;
