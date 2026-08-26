//! `infra-network` — ADR-0003 / §83.4 Layer 0: the workspace's sole **protocol** choke point.
//!
//! §83.4 originally froze one raw-HTTP-transport choke point living inside
//! `crates/infra-egress/src/http.rs` (T4.1, G80-3). Reviewing that decision against ADR-0003's
//! Qdrant intra-cell wiring found it had silently fused two different concerns into one file:
//! "nobody but this file may *construct* a `reqwest::Client`" (a statement about the raw HTTP
//! protocol, true for any destination) and "nobody may *disclose data externally* without an
//! `EgressPermit`" (a statement about crossing the tenant/Cell trust boundary, true only for
//! *external* destinations). Qdrant is same-Cell infrastructure, not an external disclosure —
//! forcing it through the external-egress wrapper to reach the only legal client constructor
//! would have made every Qdrant call either write a spurious `ops.data_disclosures` row (§7.4)
//! it has no business writing, or bypass the choke point outright (a second `reqwest::Client`
//! constructor somewhere in `adapters`, exactly what G80-3 exists to make impossible).
//!
//! ADR-0003 splits the one file into two layers, this crate being the lower one:
//! - **Layer 0 (this crate)**: the sole `reqwest::Client` construction point,
//!   [`http::build_client`] — semantically neutral, no `OutboundPurpose`, no
//!   `EgressPermit`/`CellAccessPermit`, no disclosure-ledger awareness at all. It has no
//!   opinion on *where* a caller sends bytes, only on *how many places are allowed to build the
//!   thing that sends them*.
//! - **Layer 1A** (`humaux-infra-egress`): external egress, `OutboundPurpose` +
//!   `EgressPermit`, private-data purposes reserve/finalize `ops.data_disclosures` (§7.4).
//! - **Layer 1B** (`humaux-infra-cell`): same-Cell resource access, `IntraCellResource` +
//!   `CellAccessPermit` — never touches `ops.data_disclosures` (see that crate's module doc for
//!   the full "why Qdrant must never write the disclosure ledger" argument, `docs/adr/0003-…`).
//!
//! Both Layer 1 crates reach the `reqwest::Client`/`reqwest::Error` *types* only through this
//! crate's [`reqwest`] re-export (see this module's `pub use` below) — neither lists `reqwest`
//! in its own `Cargo.toml`, keeping `xtask architecture-check`'s G80-3 manifest-level check a
//! strict `{crates/infra-network/Cargo.toml}` equality rather than a growing set.

pub mod http;

/// Re-exported so Layer 1A/1B crates can name `reqwest::Client`/`reqwest::Error`/etc. without
/// their own `Cargo.toml` ever listing `reqwest` directly — see this module's doc.
pub use reqwest;
