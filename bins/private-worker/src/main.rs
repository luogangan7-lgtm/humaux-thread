//! `humaux-private-worker` process entry (§4.2 minimal process set; §4.4 admin probe
//! contract; §11/§11.1 T4.4+T4.5).
//!
//! §11.1: "仅 humaux-private-worker 在最贴近 adapter 处解密" — this is the one process in the
//! workspace permitted to hold both DB write capability (`PrivateWorkerDbPool`,
//! `role_private_worker`) and BYOK decrypt capability at once (contrast
//! `humaux-consolidation-worker`, whose own Cargo.toml documents *not* having a path to
//! either `humaux_adapters::byok`'s decrypt trait or an OpenBao client, §11.8 — that binary
//! must go through this one over mTLS for any USER_REASONING call). This task's scope is the
//! process skeleton only: `humaux_adapters::openbao` (real OpenBao client) and this module's
//! own `OpenAiCompatTransport` real backend are still pending wiring (see `crates/adapters/
//! src/byok.rs`'s module doc) — wiring the actual claim/lease job loop and the concrete
//! `UserReasoningProvider` construction is a later task, same "not wired yet" boundary every
//! other `bins/*/src/main.rs` in this workspace currently documents for its own process.

use humaux_adapters::postgres::PrivateWorkerDbPool;

/// No config-loading infrastructure exists for any binary in this workspace yet (matches
/// `humaux-consolidation-worker/src/main.rs`'s own doc on the same point) —
/// `PRIVATE_WORKER_PG_DSN` is read directly so the process can at least prove
/// `PrivateWorkerDbPool::connect`'s §6.2.3 assertion E (`current_user ==
/// "role_private_worker"`) against a real DSN when one is supplied.
#[tokio::main]
async fn main() {
    match std::env::var("PRIVATE_WORKER_PG_DSN") {
        Err(_) => {
            println!(
                "humaux-private-worker: PRIVATE_WORKER_PG_DSN not set, not wired yet (Phase 4 scaffold)"
            );
        }
        Ok(dsn) => match PrivateWorkerDbPool::connect(&dsn).await {
            Ok(_pool) => println!("humaux-private-worker: connected as role_private_worker"),
            Err(e) => eprintln!("humaux-private-worker: {e}"),
        },
    }
}
