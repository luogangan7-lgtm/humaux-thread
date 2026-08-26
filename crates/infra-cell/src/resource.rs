//! `infra-cell::resource` — ADR-0003 / §83.4 `intra-cell-resource-registry`: the closed set of
//! same-Cell resources a caller may ever name, and the deploy-supplied table that resolves one
//! to an actual endpoint.
//!
//! §83.4 判据1 (registry membership): a caller can only ever hand
//! [`IntraCellHttpTransport::execute`](crate::transport::IntraCellHttpTransport::execute) an
//! [`IntraCellResource`] variant — there is no `IntraCellResource::Custom(String)` escape
//! hatch, so "give me this exact URL" is not an expressible request at the type level. The
//! actual host/port/CIDR/caller-allowlist for a variant is resolved by
//! [`IntraCellResourceRegistry`], populated once at deploy/bootstrap time from real deployment
//! topology (§78.1: not a literal in this crate) — see `tests/ui/fail_resource_from_raw_url_string.rs`
//! for the compile-fail proof that no path from this enum to a bare string exists (that proof
//! covers the direct-literal-coercion shape; `xtask architecture-check`'s G80-3 source scan
//! covers the general "no fn converts a string to this enum" property no fixed-name trybuild
//! fixture could pin — see that checker's own doc for why both are needed).

use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

use crate::permit::{CallerId, CellId};

/// §83.4's `intra-cell-resource-registry` fenced block, column 1 — closed set, first version
/// exactly one entry. SCREAMING_SNAKE variant idents (`#[allow(non_camel_case_types)]`) so
/// `xtask architecture-check`'s registry-consistency check needs no case-folding step, mirroring
/// `domain::egress::OutboundPurpose`'s identical convention against `external-egress-registry`.
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IntraCellResource {
    /// §17: the self-hosted Qdrant cluster's REST endpoint, same-Cell (§7.0/§7.2's own text:
    /// "Sparse/BM25 是本地 Retrieval lane" — Qdrant is Cell-internal infrastructure, not an
    /// external disclosure recipient; see `docs/adr/0003-…` for the full argument).
    QDRANT_REST,
}

impl IntraCellResource {
    /// Every variant — for a registry builder that wants to assert full coverage, and for
    /// `xtask architecture-check`'s registry-consistency scan.
    pub const ALL: [IntraCellResource; 1] = [Self::QDRANT_REST];

    /// ADR-0003 second-round correction (`domain::boundary`): every `IntraCellResource` is,
    /// by definition of belonging to this closed registry, the same legal entity operating
    /// this workspace processing with its own resources — never an
    /// `ExternalProcessor`/`ExternalIndependentRecipient`, regardless of protocol. This is
    /// *why* `crate::permit::CellAccessPermit` never reserves an `ops.data_disclosures` row
    /// (that module's doc) — the classification, not the network route, is the reason.
    pub fn recipient_class(self) -> humaux_domain::boundary::RecipientClass {
        humaux_domain::boundary::RecipientClass::SameEntityResource
    }

    /// This closed registry's one variant's own display name — for `bins/admin/cell_resources`'s
    /// `scanned_n`/missing-object reporting (§4.4 坑5), which must name what it covers/misses
    /// rather than only a bare count.
    pub fn name(self) -> &'static str {
        match self {
            Self::QDRANT_REST => "QDRANT_REST",
        }
    }
}

/// One CIDR block (IPv4 or IPv6). Structurally identical to `protocol::edge::Cidr` but
/// deliberately a separate, duplicated ~20-line type rather than a dependency on
/// `humaux-protocol`: that crate exists for the HTTP-edge trusted-proxy boundary, this one for
/// the intra-Cell egress boundary — two independent choke points that happen to need the same
/// primitive, not one shared concept that must never drift. Pulling in `humaux-protocol` here
/// would also add its `humaux-domain`/`hmac`/`sha2` dependency chain for four fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CellCidr {
    network: IpAddr,
    prefix_len: u8,
}

/// [`CellCidr::from_str`] failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellCidrParseError(pub String);

impl FromStr for CellCidr {
    type Err = CellCidrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (addr_s, len_s) = s
            .split_once('/')
            .ok_or_else(|| CellCidrParseError(s.to_string()))?;
        let network: IpAddr = addr_s
            .parse::<IpAddr>()
            .map_err(|_| CellCidrParseError(s.to_string()))?
            .to_canonical();
        let max_len: u8 = match network {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        let prefix_len: u8 = len_s
            .parse()
            .map_err(|_| CellCidrParseError(s.to_string()))?;
        if prefix_len > max_len {
            return Err(CellCidrParseError(s.to_string()));
        }
        Ok(CellCidr {
            network,
            prefix_len,
        })
    }
}

impl CellCidr {
    pub fn contains(&self, ip: IpAddr) -> bool {
        let ip = ip.to_canonical();
        match (self.network, ip) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                let mask = mask_u32(self.prefix_len);
                (u32::from(net) & mask) == (u32::from(ip) & mask)
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                let mask = mask_u128(self.prefix_len);
                (u128::from(net) & mask) == (u128::from(ip) & mask)
            }
            _ => false,
        }
    }

    /// Whether every address `self` names is also named by `other` — `self.prefix_len` must be
    /// at least as specific as `other`'s, and `self`'s network must fall inside `other`'s
    /// block. Used by [`is_private_or_reserved`] to reject a registered CIDR that is only
    /// *partially* private (e.g. `10.0.0.0/1`, whose network address happens to sit inside
    /// `10.0.0.0/8` but whose block covers half of all IPv4 space, public ranges included).
    fn is_subset_of(&self, other: &CellCidr) -> bool {
        self.prefix_len >= other.prefix_len && other.contains(self.network)
    }
}

const fn mask_u32(prefix_len: u8) -> u32 {
    if prefix_len == 0 {
        0
    } else {
        u32::MAX << (32 - prefix_len)
    }
}

const fn mask_u128(prefix_len: u8) -> u128 {
    if prefix_len == 0 {
        0
    } else {
        u128::MAX << (128 - prefix_len)
    }
}

/// §83.4 判据3's "public IP … 一律拒" half, checked once at registry-construction time rather
/// than left to whichever deploy config happens to be correct: every CIDR a [`ResourceEntry`]
/// registers must be a subset of one of these non-publicly-routable reference blocks (RFC1918
/// private space, RFC4193 unique-local IPv6, the CGNAT range, and loopback — loopback included
/// because `crates/adapters/tests/qdrant_live.rs`'s real-Qdrant smoke test legitimately targets
/// `127.0.0.1`, and it is exactly as non-public as the other four).
const PRIVATE_REFERENCE_CIDRS: [CellCidr; 6] = [
    // 10.0.0.0/8
    CellCidr {
        network: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)),
        prefix_len: 8,
    },
    // 172.16.0.0/12
    CellCidr {
        network: IpAddr::V4(Ipv4Addr::new(172, 16, 0, 0)),
        prefix_len: 12,
    },
    // 192.168.0.0/16
    CellCidr {
        network: IpAddr::V4(Ipv4Addr::new(192, 168, 0, 0)),
        prefix_len: 16,
    },
    // 100.64.0.0/10 (CGNAT, RFC6598)
    CellCidr {
        network: IpAddr::V4(Ipv4Addr::new(100, 64, 0, 0)),
        prefix_len: 10,
    },
    // 127.0.0.0/8 (loopback — local/dev Qdrant, see doc above)
    CellCidr {
        network: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 0)),
        prefix_len: 8,
    },
    // fc00::/7 (RFC4193 unique-local IPv6)
    CellCidr {
        network: IpAddr::V6(Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 0)),
        prefix_len: 7,
    },
];

/// §83.4 判据3: `cidr` names only non-publicly-routable addresses (see
/// [`PRIVATE_REFERENCE_CIDRS`]'s doc) — the discipline-not-topology gap §83.4 originally left
/// open (a registry entry could accept `0.0.0.0/0` and pass every other check).
fn is_private_or_reserved(cidr: &CellCidr) -> bool {
    PRIVATE_REFERENCE_CIDRS
        .iter()
        .any(|reference| cidr.is_subset_of(reference))
}

/// Single-address form of [`is_private_or_reserved`] — `bins/admin`'s `cell.resources` live
/// probe (§4.4) uses this to answer "is the address this hostname *actually* resolved to
/// right now private/reserved", independent of whichever CIDR the registry happens to declare
/// for the resource (registry declaration and live resolution are the two independent sources
/// that probe exists to cross-check — see its module doc).
pub fn is_private_or_reserved_address(ip: IpAddr) -> bool {
    let ip = ip.to_canonical();
    let prefix_len = match ip {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    };
    is_private_or_reserved(&CellCidr {
        network: ip,
        prefix_len,
    })
}

/// [`ResourceEntry::new`] failure: a registered CIDR is not entirely inside a private/reserved
/// range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceEntryError(pub String);

impl std::fmt::Display for ResourceEntryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ResourceEntryError {}

/// One [`IntraCellResource`]'s resolved deploy-topology facts — everything 判据2/3/5/6 need to
/// check at mint/call time. `host`/`port` are DNS-resolved at call time, never dereferenced to
/// a bare `IpAddr` here (that would reintroduce the exact "caller can name any address" hole
/// 判据1 exists to close).
#[derive(Debug, Clone)]
pub struct ResourceEntry {
    host: String,
    port: u16,
    cell_id: CellId,
    allowed_cidrs: Vec<CellCidr>,
    allowed_callers: BTreeSet<CallerId>,
    tls: bool,
}

impl ResourceEntry {
    /// Fails (rather than silently admitting) if any of `allowed_cidrs` is not entirely inside
    /// a private/reserved range (§83.4 判据3, [`is_private_or_reserved`]) — a public or
    /// all-encompassing CIDR is rejected at bootstrap instead of only being caught later by an
    /// alert reader auditing the deploy config.
    pub fn new(
        host: impl Into<String>,
        port: u16,
        cell_id: CellId,
        allowed_cidrs: Vec<CellCidr>,
        allowed_callers: BTreeSet<CallerId>,
        tls: bool,
    ) -> Result<Self, ResourceEntryError> {
        if let Some(bad) = allowed_cidrs.iter().find(|c| !is_private_or_reserved(c)) {
            return Err(ResourceEntryError(format!(
                "§83.4 判据3: CIDR {bad:?} is not entirely inside a private/reserved range \
                 (RFC1918 / RFC4193 / CGNAT / loopback) — public or overly-broad ranges must be \
                 rejected at registry construction, not discovered later"
            )));
        }
        Ok(Self {
            host: host.into(),
            port,
            cell_id,
            allowed_cidrs,
            allowed_callers,
            tls,
        })
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Not `pub`: [`crate::permit::authorize_cell_access`] is the only legitimate reader — a
    /// caller must never be able to read a resource's registered Cell back out and echo it as
    /// its own claimed identity (§83.4 判据2's self-attestation hole this closes, alongside
    /// dropping the caller-suppliable `source_cell_id` parameter from `authorize_cell_access`
    /// itself; see that fn's module doc).
    pub(crate) fn cell_id(&self) -> CellId {
        self.cell_id
    }

    /// §83.4 判据5: whether this resource's endpoint must be dialed over TLS. `execute`
    /// (`crate::transport`) selects `https`/`http` from this flag rather than hardcoding a
    /// scheme.
    pub fn tls(&self) -> bool {
        self.tls
    }

    /// §83.4 判据3: whether `ip` is inside this resource's Cell CIDR/Service IP set.
    pub fn address_in_cell(&self, ip: IpAddr) -> bool {
        self.allowed_cidrs.iter().any(|c| c.contains(ip))
    }

    /// §83.4 判据6: whether `caller` is on this resource's deploy-time allowlist.
    pub fn caller_allowed(&self, caller: &CallerId) -> bool {
        self.allowed_callers.contains(caller)
    }
}

/// §83.4's `intra-cell-resource-registry` — the deploy-supplied table
/// [`IntraCellResource`] variants resolve against. Built once at bootstrap from real deploy
/// topology (§78.1: no business config hardcoded here), not by this crate.
#[derive(Debug, Clone)]
pub struct IntraCellResourceRegistry {
    entries: BTreeMap<IntraCellResource, ResourceEntry>,
    local_cell_id: CellId,
    local_caller_id: CallerId,
}

impl IntraCellResourceRegistry {
    /// `local_cell_id`/`local_caller_id` are this Cell process's own deploy-time identity —
    /// sourced from deploy config/secret store at bootstrap, exactly once, never a
    /// call-site-suppliable value (§78.1). §83.4 判据2/6 (`crate::permit::authorize_cell_access`)
    /// compare a resource's registered `cell_id`/allowlist against these two fields instead of
    /// parameters a caller could spell arbitrarily — see that fn's module doc for the
    /// self-attestation hole this closes.
    pub fn new(
        entries: BTreeMap<IntraCellResource, ResourceEntry>,
        local_cell_id: CellId,
        local_caller_id: CallerId,
    ) -> Self {
        Self {
            entries,
            local_cell_id,
            local_caller_id,
        }
    }

    pub fn resolve(&self, resource: IntraCellResource) -> Option<&ResourceEntry> {
        self.entries.get(&resource)
    }

    /// Every registered entry — ADR-0003 second-round fix: `crate::transport`'s single DNS
    /// resolver needs a hostname → [`ResourceEntry`] lookup table at client-construction time,
    /// so the same CIDR/metadata judgment §83.4 判据3 runs both when validating a resolved
    /// address and when handing it to the HTTP connector (one lookup, not a check pass and a
    /// separate dial pass).
    pub(crate) fn entries(&self) -> impl Iterator<Item = &ResourceEntry> {
        self.entries.values()
    }

    pub(crate) fn local_cell_id(&self) -> CellId {
        self.local_cell_id
    }

    pub(crate) fn local_caller_id(&self) -> &CallerId {
        &self.local_caller_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ADR-0003 second-round correction: every registered `IntraCellResource` classifies as
    /// `SameEntityResource`, so `domain::boundary::requires_disclosure_record` must say `false`
    /// for it — the closed-registry membership itself, not any per-call route/IP check, is what
    /// makes this true.
    #[test]
    fn every_intra_cell_resource_skips_the_disclosure_ledger() {
        for resource in IntraCellResource::ALL {
            assert!(!humaux_domain::boundary::requires_disclosure_record(
                resource.recipient_class()
            ));
        }
    }

    #[test]
    fn cidr_contains_matches_v4_block() {
        let c: CellCidr = "10.0.0.0/8".parse().unwrap();
        assert!(c.contains("10.1.2.3".parse().unwrap()));
        assert!(!c.contains("11.0.0.1".parse().unwrap()));
    }

    #[test]
    fn cidr_rejects_metadata_style_address_outside_block() {
        let c: CellCidr = "10.0.0.0/8".parse().unwrap();
        assert!(!c.contains("169.254.169.254".parse().unwrap()));
    }

    #[test]
    fn registry_resolves_only_registered_resource() {
        let mut entries = BTreeMap::new();
        entries.insert(
            IntraCellResource::QDRANT_REST,
            ResourceEntry::new(
                "qdrant.cell-a.internal",
                6333,
                CellId(uuid::Uuid::now_v7()),
                vec!["10.0.0.0/8".parse().unwrap()],
                BTreeSet::new(),
                true,
            )
            .unwrap(),
        );
        let registry = IntraCellResourceRegistry::new(
            entries,
            CellId(uuid::Uuid::now_v7()),
            CallerId("retrieval-worker".to_string()),
        );
        assert!(registry.resolve(IntraCellResource::QDRANT_REST).is_some());
    }

    /// §83.4 判据3 regression: a public/all-encompassing CIDR must fail at construction, not
    /// merely be discouraged by discipline.
    #[test]
    fn public_cidr_is_rejected_at_construction() {
        let result = ResourceEntry::new(
            "qdrant.internal",
            6333,
            CellId(uuid::Uuid::now_v7()),
            vec!["0.0.0.0/0".parse().unwrap()],
            BTreeSet::new(),
            true,
        );
        assert!(result.is_err());
    }

    /// A CIDR whose network address happens to sit inside a private block, but whose prefix is
    /// wide enough to also cover public space (`10.0.0.0/1` is half of all IPv4 addresses, not
    /// a subset of `10.0.0.0/8`), must be rejected — network-address membership alone is not
    /// enough, the whole block must be contained.
    #[test]
    fn overly_broad_cidr_sharing_a_private_network_address_is_rejected() {
        let result = ResourceEntry::new(
            "qdrant.internal",
            6333,
            CellId(uuid::Uuid::now_v7()),
            vec!["10.0.0.0/1".parse().unwrap()],
            BTreeSet::new(),
            true,
        );
        assert!(result.is_err());
    }

    #[test]
    fn ordinary_private_cidr_is_accepted() {
        let result = ResourceEntry::new(
            "qdrant.internal",
            6333,
            CellId(uuid::Uuid::now_v7()),
            vec![
                "10.0.0.0/8".parse().unwrap(),
                "127.0.0.1/32".parse().unwrap(),
            ],
            BTreeSet::new(),
            true,
        );
        assert!(result.is_ok());
    }
}
