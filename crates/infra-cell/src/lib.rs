//! `humaux-infra-cell` — ADR-0003 / §83.4 Layer 1B: same-Cell resource access.
//! Depends-on: crates=[]; services=[]; env=[]; modules=[]
//! Called-by: [crate(humaux-adapters), crate(humaux-admin), crate(humaux-gateway), crate(humaux-maintenance),
//!   crate(humaux-private-worker), crate(humaux-public-worker), crate(humaux-retrieval-worker), crate(xtask)]
//! Invariants: [crate root: reaches reqwest only through humaux-infra-network's re-export (G80-3); CellAccessPermit
//!   never leaves the Cell and never touches ops.data_disclosures]
//! Spec: Baseline §7.4; §83.4
//!
//! Companion crate to `humaux-infra-egress` (Layer 1A, external egress). Where Layer 1A's
//! `EgressPermit` crosses the tenant/Cell trust boundary and is accountable to `ops.
//! data_disclosures` (§7.4), this crate's [`permit::CellAccessPermit`] never leaves the Cell
//! and never touches that ledger — see `permit`'s module doc for the full argument (also
//! `docs/adr/0003-network-vs-egress-choke-point.md`).
//!
//! Both this crate and `humaux-infra-egress` reach `reqwest::Client`/`reqwest::Error` only
//! through `humaux-infra-network`'s re-export (`humaux_infra_network::reqwest`) — neither
//! lists `reqwest` in its own `Cargo.toml`, keeping `xtask architecture-check`'s G80-3 raw
//! HTTP client choke point a strict single-file/single-manifest equality (§83.4 Layer 0).
//!
//! §83.4's six intra-cell AND-judgment criteria and this crate's coverage of them:
//!
//! ```text
//! 1. registry membership   — resource.rs: IntraCellResource closed enum, no raw-URL path;
//!                            transport.rs: validate_path rejects a request-relative path
//!                            that would rewrite the URL authority (@, //, .. segment)
//! 2. same cell             — permit.rs:   authorize_cell_access WrongCell check, against
//!                            the registry's own baked-in local_cell_id (not a parameter)
//! 3. resolved address ∈ Cell CIDR — transport.rs: HttpIntraCellTransport::execute,
//!                            pre-connect; resource.rs: ResourceEntry::new rejects a
//!                            non-private/reserved CIDR at construction; redirects are
//!                            disabled at the client level (humaux-infra-network) and any
//!                            3xx status is surfaced as an explicit error, never followed
//! 4. no Internet/NAT route — deploy gate (README "Intra-cell network deploy gate"), not code
//! 5. destination identity (mTLS/SAN + API key) — TLS scheme (ResourceEntry::tls) and a
//!                            per-request headers path (IntraCellRequest::headers, for e.g.
//!                            Qdrant's `api-key`) are code-level; certificate provisioning
//!                            and the API key's actual value are still deploy gate (README)
//! 6. caller allowlist      — permit.rs:   authorize_cell_access UnknownCaller check, against
//!                            the registry's own baked-in local_caller_id (not a parameter)
//! ```

pub mod permit;
pub mod resource;
pub mod transport;

pub use permit::{CallerId, CellAccessPermit, CellId, PermitError, authorize_cell_access};
pub use resource::{
    CellAccessMode, CellCidr, IntraCellResource, IntraCellResourceRegistry, ResourceEntry,
    is_private_or_reserved_address,
};
pub use transport::{
    DEFAULT_MAX_RESPONSE_BYTES, DnsResolve, HttpIntraCellTransport, IntraCellError,
    IntraCellHttpTransport, IntraCellMethod, IntraCellRequest, IntraCellResponse, SystemDnsResolve,
    is_metadata_or_link_local,
};
