//! `adapters::tests::support::token_keys` — installs one per-process random consistency-token key set for test
//!   binaries that issue or verify tokens through the production entry points.
//! Depends-on: crates=[humaux-adapters]; services=[]; env=[]; modules=[adapters::retrieve]
//! Called-by: [adapters::tests::outbox_batch_remember,
//!   adapters::tests::private_projection_registry, adapters::tests::projection_claim, adapters::tests::projection_worker,
//!   adapters::tests::retrieve_read_your_writes, adapters::tests::support::governance_ops,
//!   adapters::tests::support::operation_receipt_fixture]
//! Invariants: [the key is generated at run time, never a literal, and never printed; the first install in a process
//!   wins and every later call is a no-op, so fixtures that each include this file share one key set]
//! Spec: Baseline §15.5; ADR-0059 D-G

#![allow(dead_code)]

use std::io::Read as _;

use humaux_adapters::retrieve::{TokenKeys, install_token_keys};

/// Installs a fresh random 32-byte current key (no previous) unless this process already has one.
pub fn install() {
    // Read from the OS CSPRNG directly: this file is also included by gateway test binaries,
    // which have no `rand` dependency.
    let mut key = [0u8; 32];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut urandom| urandom.read_exact(&mut key))
        .expect("read 32 bytes from /dev/urandom");
    let keys = TokenKeys::new(key.to_vec(), None).expect("a 32-byte random key is a valid key set");
    // The only Err is "a different set is already installed": the process already has keys,
    // which is all a fixture needs.
    let _ = install_token_keys(keys);
}
