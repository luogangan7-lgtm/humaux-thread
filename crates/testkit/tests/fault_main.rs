//! Cargo integration-test entry point for `tests/fault/` (§53.4).
//!
//! Cargo only auto-discovers `*.rs` files directly under `tests/`; files in
//! a subdirectory (`tests/fault/*.rs`) are invisible to it unless pulled in
//! as modules from a file cargo *does* discover — that's what this file is
//! for. One `#[path = "fault/xxx.rs"] mod xxx;` line per `DegradeCode`
//! variant, so `tests/fault/README.md`'s file-count claim stays literally
//! true on disk while still compiling as one test binary.

#[path = "fault/completeness_unknown.rs"]
mod completeness_unknown;
#[path = "fault/egress_denied.rs"]
mod egress_denied;
#[path = "fault/embed_provider_timeout.rs"]
mod embed_provider_timeout;
#[path = "fault/graph_expand_capped.rs"]
mod graph_expand_capped;
#[path = "fault/projection_invisible_loss.rs"]
mod projection_invisible_loss;
#[path = "fault/projection_lag.rs"]
mod projection_lag;
#[path = "fault/rerank_model_mismatch.rs"]
mod rerank_model_mismatch;
#[path = "fault/rerank_provider_timeout.rs"]
mod rerank_provider_timeout;
#[path = "fault/state_pin_ambiguous.rs"]
mod state_pin_ambiguous;
#[path = "fault/state_pin_missing.rs"]
mod state_pin_missing;
