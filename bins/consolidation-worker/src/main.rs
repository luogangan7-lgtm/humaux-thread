//! `humaux-consolidation-worker` process entry (§4.2 minimal process set; §4.4 admin probe
//! contract; §11.7/§11.8 T4.6+T4.7). The actual orchestration ([`run_once`] and friends) lives
//! in `src/lib.rs` — see that module's doc comment for why: a binary-only crate has no target
//! `tests/*.rs` can link against, so splitting it out is what makes `tests/run_once_e2e.rs`
//! possible at all.
//!
//! [`run_once`]: humaux_consolidation_worker::run_once

use humaux_adapters::postgres::ConsolidationDbPool;

/// No config-loading infrastructure exists for any binary in this workspace yet (every other
/// `bins/*/src/main.rs` is still the Phase-0 "not wired yet" scaffold) — inventing one here,
/// for this binary alone, would be scope creep past T4.6/T4.7. `CONSOLIDATION_WORKER_PG_DSN`
/// is read directly so the process can at least prove [`ConsolidationDbPool::connect`]'s
/// §6.2.3 assertion E (`current_user == "role_consolidation_worker"`) against a real DSN when
/// one is supplied; `PrivateReasoningPort`'s real mTLS implementation and the claim/lease loop
/// that decides *when* to call `run_once` are later tasks' scope (the mTLS client doesn't
/// exist yet anywhere in this workspace).
#[tokio::main]
async fn main() {
    match std::env::var("CONSOLIDATION_WORKER_PG_DSN") {
        Err(_) => {
            println!(
                "humaux-consolidation-worker: CONSOLIDATION_WORKER_PG_DSN not set, not wired yet (Phase 4 scaffold)"
            );
        }
        Ok(dsn) => match ConsolidationDbPool::connect(&dsn).await {
            Ok(_pool) => {
                println!("humaux-consolidation-worker: connected as role_consolidation_worker")
            }
            Err(e) => eprintln!("humaux-consolidation-worker: {e}"),
        },
    }
}
