//! T5.4+T5.6 — the two Qdrant-reachability-gated assertions the task brief asks for:
//!
//! 1. tenant keyword index effective (a cross-tenant filtered search never returns another
//!    tenant's points, §17.1);
//! 2. the §23.4 injection-2 fault against a real cluster: swap `verify_visible`'s real
//!    search-based confirmation for a bare `acknowledged` ack, and observe unflushed points
//!    get treated as visible.
//!
//! §79.2/§57.1 rule 3: an unreachable dependency must report `not_applicable` and name the
//! missing object, never silently pass. Both tests below name it — and it is **not**
//! "`crates/adapters/Cargo.toml` cannot be edited": that file belongs to this task and is
//! freely editable. The real blocker is §83.4 G80-3 (`xtask/src/architecture_check.rs:1354`),
//! which requires the reqwest/hyper-dependent manifest set to equal exactly
//! `{crates/infra-egress/Cargo.toml}` — adding `reqwest` here today would flip that gate from
//! pass to fail. Which of the three resolutions in `crates/adapters/src/qdrant.rs`'s module doc
//! applies is an architecture decision (§78.6 ADR) this task cannot make unilaterally. That is a
//! different, and honest, `not_applicable` reason from "Qdrant unreachable": this crate cannot
//! even attempt the TCP connection, so it never gets far enough to discover whether Qdrant
//! itself is up.
//!
//! `crates/adapters/src/qdrant.rs`'s own `tests/qdrant_contract.rs` file covers the pure logic
//! these two scenarios rest on (`condition_to_filter` shape, `verify_visible`'s contract, and
//! the same ack-only-vs-honest-search distinction reproduced against a fake `check_visible`) —
//! this file exists only to make the still-missing final wire-transport step visible in the
//! test run, rather than have it disappear silently.

#[test]
fn tenant_keyword_index_cross_tenant_search_isolation() {
    eprintln!(
        "NOT_APPLICABLE tenant_keyword_index_cross_tenant_search_isolation: missing object \
         `humaux-adapters`'s HTTP transport for Qdrant — undecided per §83.4 G80-3 (see \
         crates/adapters/src/qdrant.rs module doc for the three pending options; adding \
         `reqwest` directly to this crate's Cargo.toml would fail G80-3's manifest-set check, \
         not a file-ownership restriction). §17.1 requires an actual Qdrant PUT /collections + \
         POST /points/search round trip to prove tenant isolation; Qdrant at \
         http://127.0.0.1:6333 was never dialed because there is no transport to dial it with."
    );
}

#[test]
fn ack_only_verify_visible_fault_against_real_qdrant() {
    eprintln!(
        "NOT_APPLICABLE ack_only_verify_visible_fault_against_real_qdrant: missing object \
         `humaux-adapters`'s HTTP transport for Qdrant — undecided per §83.4 G80-3 (see \
         crates/adapters/src/qdrant.rs module doc). §23.4 注入 2's literal form needs a real \
         Qdrant upsert against an unoptimized segment so some points are genuinely not yet \
         search-visible. The pure-logic form of this same fault is covered and passing: \
         tests/qdrant_contract.rs::verify_visible_withholds_confirmation_when_some_ids_are_not_yet_visible."
    );
}
