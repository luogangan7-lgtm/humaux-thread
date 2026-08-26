//! `humaux-admin q cell.resources` — §4.4 live probe: cross-checks §83.4's
//! `intra-cell-resource-registry` DECLARATION against what each [`IntraCellResource`]'s
//! registered hostname ACTUALLY, LIVE, resolves to and answers on.
//!
//! Humaux's own "不要拿声明校验声明" principle (旧系统坑5，全局适用): "the registry says this
//! resource lives in this Cell" is a deploy-time claim; "the resource's hostname currently
//! resolves inside this Cell's CIDR set and answers a real HTTP call" is a runtime fact. This
//! probe deliberately gets those two from two independent code paths — the registry's
//! [`ResourceEntry`] (deploy-supplied config, §78.1) versus a fresh [`SystemDnsResolve`] lookup
//! and a real [`HttpIntraCellTransport::execute`] call made *by this probe process itself* —
//! rather than asking the registry (or anything the registry configured) whether it thinks it
//! is healthy. A registry entry pointing at a deployment resource that no longer exists must
//! surface as this probe's own DNS-resolution failure (non-zero exit, no JSON envelope), never
//! as an ordinary `value = 0` "resource found, but unhealthy" reading (`value` counts healthy
//! resources — `run()`'s own doc comment is this probe's one authoritative source for what
//! `value` means; spec §4.4's table row freezes `scanned_n` only) — §4.4 坑5: "没有" ≠ "没扫到",
//! and neither is "无法判断" — collapsing the third case into the first is exactly the shape
//! §4.4 warns against for `scanned_n`, applied here to a probe that can't even reach its
//! object at all.
//!
//! **Known ceiling this guarantee cannot close purely at this layer**: it holds exactly when
//! the process's resolver actually returns NXDOMAIN for a name that does not exist —
//! `std::net::ToSocketAddrs`/`getaddrinfo` propagates that as `Err`, which the caller correctly
//! turns into this probe's own hard-fail (`registry_from_env`'s caller in `run()`). It cannot
//! hold when the network's own resolver rewrites NXDOMAIN into a synthetic A/AAAA record before
//! `getaddrinfo` ever sees a failure (ISP wildcard DNS, a corporate resolver, a VPN stub, or a
//! network-restricted sandbox that answers every external lookup with one fixed address) — at
//! that point this process has genuinely, correctly resolved the name to *something*, and no
//! amount of Rust-side logic run *after* `to_socket_addrs()` returns `Ok` can tell that answer
//! apart from a real (if misconfigured) DNS record; the two are the same shape by construction.
//! `run()`'s envelope adds an explicit `resolution_may_be_synthetic` warning for exactly this
//! shape (every resolved address is outside *both* the registered CIDR and every recognized
//! private/reserved range — see [`ResourceReport::to_json`]) rather than silently treating it
//! as an ordinary out-of-Cell reading, but it deliberately does not turn it into a hard,
//! non-zero-exit failure: that would misclassify a real misconfigured-to-a-public-address
//! resource (§4.4 坑5's own worked example one line up in the code) as this probe's own object
//! failure instead of as data.
//! // ponytail: OS-resolver trust is the ceiling; a raw DNS client that reads the wire RCODE
//! // directly (e.g. `hickory-resolver`) would let this probe distinguish "server said NXDOMAIN"
//! // from "server answered with something" instead of guessing from the answer's shape — add
//! // it if `resolution_may_be_synthetic` false positives/negatives become a real operational
//! // problem, not preemptively for one review finding.
//!
//! Phase-0 config wiring (no deploy-config loader exists yet in this workspace; §78.1: no
//! hardcoded business config instead): one resource, [`IntraCellResource::QDRANT_REST`], its
//! registry entry built from environment variables:
//!
//! | var | meaning |
//! |---|---|
//! | `HUMAUX_CELL_ID` | this Cell's UUID |
//! | `HUMAUX_CELL_CALLER_ID` | this probe process's own caller identity (§83.4 判据6) |
//! | `HUMAUX_QDRANT_HOST` | `QDRANT_REST`'s registered hostname |
//! | `HUMAUX_QDRANT_PORT` | its registered port |
//! | `HUMAUX_QDRANT_CIDR` | comma-separated CIDR list its resolved addresses must fall in |
//! | `HUMAUX_QDRANT_TLS` | `"true"` / `"false"` |
//!
//! Any missing/unparseable var is this probe's own "missing object" (same convention
//! `probe.rs`'s catalog-membership check already uses for an unwired backend) — non-zero exit,
//! no JSON envelope, never a `value = 0` standing in for "could not even build the registry".

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::time::Duration;

use humaux_infra_cell::{
    CallerId, CellCidr, CellId, DEFAULT_MAX_RESPONSE_BYTES, DnsResolve, HttpIntraCellTransport,
    IntraCellHttpTransport, IntraCellMethod, IntraCellRequest, IntraCellResource,
    IntraCellResourceRegistry, ResourceEntry, SystemDnsResolve, authorize_cell_access,
    is_metadata_or_link_local, is_private_or_reserved_address,
};

const PROBE_VERSION: &str = "cell.resources@1";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// One missing/unparseable required env var — this probe's own "missing object" (module doc).
struct MissingEnv(&'static str);

fn required_env(name: &'static str) -> Result<String, MissingEnv> {
    std::env::var(name).map_err(|_| MissingEnv(name))
}

/// Builds this Phase-0 registry from environment — see module doc's table. Returns `cell_id`
/// alongside the registry since [`ResourceEntry`]'s own copy is not `pub` outside this crate
/// (`crate::permit::authorize_cell_access` is deliberately the only legitimate reader — see
/// that fn's module doc in `humaux-infra-cell`), and [`scope_hash`] needs it independently.
fn registry_from_env() -> Result<(IntraCellResourceRegistry, CellId), MissingEnv> {
    let cell_id = CellId(
        uuid::Uuid::parse_str(&required_env("HUMAUX_CELL_ID")?)
            .map_err(|_| MissingEnv("HUMAUX_CELL_ID (not a UUID)"))?,
    );
    let caller_id = CallerId(required_env("HUMAUX_CELL_CALLER_ID")?);
    let host = required_env("HUMAUX_QDRANT_HOST")?;
    let port: u16 = required_env("HUMAUX_QDRANT_PORT")?
        .parse()
        .map_err(|_| MissingEnv("HUMAUX_QDRANT_PORT (not a u16)"))?;
    let cidrs: Vec<CellCidr> = required_env("HUMAUX_QDRANT_CIDR")?
        .split(',')
        .map(|s| s.trim().parse::<CellCidr>())
        .collect::<Result<_, _>>()
        .map_err(|_| MissingEnv("HUMAUX_QDRANT_CIDR (not a comma-separated CIDR list)"))?;
    let tls = match required_env("HUMAUX_QDRANT_TLS")?.as_str() {
        "true" => true,
        "false" => false,
        _ => return Err(MissingEnv("HUMAUX_QDRANT_TLS (must be \"true\"/\"false\")")),
    };

    // This probe process is itself the sole caller it registers — it needs to mint its own
    // `CellAccessPermit` to make the live `execute()` call below, not to model a real deploy's
    // full caller allowlist.
    let entry = ResourceEntry::new(
        host,
        port,
        cell_id,
        cidrs,
        BTreeSet::from([caller_id.clone()]),
        tls,
    )
    .map_err(|_| MissingEnv("HUMAUX_QDRANT_CIDR (rejected: not private/reserved, §83.4 判据3)"))?;

    let mut entries = BTreeMap::new();
    entries.insert(IntraCellResource::QDRANT_REST, entry);
    Ok((
        IntraCellResourceRegistry::new(entries, cell_id, caller_id),
        cell_id,
    ))
}

/// §57.1's three-state verdict ("所有闸三态 pass/fail/not_applicable；not_applicable 必须打印
/// 缺失对象名") applied to §83.4 判据5 (destination identity): a bare bool collapsing "nothing
/// was checked" into `true` is exactly the shape §57.1 forbids — this probe must never report
/// `identity_verified` as verified-true for a connection that carried no certificate at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdentityVerified {
    /// TLS resource, `execute()` returned a response (`reqwest`'s TLS backend fails the call
    /// on a cert/SNI mismatch, so a successful call already implies validation passed).
    Pass,
    /// TLS resource, `execute()` failed — could be a cert/identity failure or an unrelated
    /// connect failure; this probe cannot tell the two apart without inspecting the error's
    /// TLS-specific detail, so it reports `fail` rather than guessing which.
    Fail,
    /// Plain-HTTP resource: §83.4 判据5 has nothing to verify at this layer at all (identity
    /// pinning is deploy-gate, README "Intra-cell network deploy gate") — the missing object
    /// named per §57.1's rule.
    NotApplicable(&'static str),
    /// The dial was skipped entirely (`same_cell`/`private_route` already failed) — there is
    /// no connection whose identity could even be evaluated.
    NotApplicableDialSkipped,
}

impl IdentityVerified {
    fn to_json(self) -> serde_json::Value {
        match self {
            Self::Pass => serde_json::json!("pass"),
            Self::Fail => serde_json::json!("fail"),
            Self::NotApplicable(missing) => serde_json::json!({
                "status": "not_applicable",
                "missing_object": missing,
            }),
            Self::NotApplicableDialSkipped => serde_json::json!({
                "status": "not_applicable",
                "missing_object": "no connection attempted — resolved address failed same_cell/private_route",
            }),
        }
    }
}

/// One resource's row in the probe's output — §4.4's per-object breakdown, beyond the unified
/// `{value, scanned_n, ...}` scalar envelope.
struct ResourceReport {
    resource: &'static str,
    configured_host: String,
    configured_port: u16,
    resolved_ips: Vec<IpAddr>,
    same_cell: bool,
    private_route: bool,
    identity_verified: IdentityVerified,
    reachable: bool,
    /// Set only when the dial was skipped (major fix: a probe that has already judged an
    /// address out-of-Cell must not go on to perform the very transfer it judged out-of-Cell —
    /// see `probe_qdrant`'s doc) — `reachable: false` alone does not distinguish "we dialed and
    /// it failed" from "we refused to dial", and an operator reading this envelope needs that
    /// distinction to know whether `reachable: false` is a health problem or a policy refusal.
    dial_skipped_reason: Option<&'static str>,
}

impl ResourceReport {
    fn healthy(&self) -> bool {
        self.same_cell && self.private_route && self.reachable
    }

    /// Module doc's "known ceiling" caveat: every resolved address is outside *both* the
    /// registered CIDR and every recognized private/reserved range at all — the shape a
    /// wildcard/hijacking DNS resolver's synthetic answer would take, indistinguishable at
    /// this layer from a genuine (if badly misconfigured) public-address registry entry. Never
    /// changes `healthy()`/exit code — a warning for the reader, not a second judgment.
    fn resolution_may_be_synthetic(&self) -> bool {
        !self.resolved_ips.is_empty() && !self.same_cell && !self.private_route
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "resource": self.resource,
            "configured_host": self.configured_host,
            "configured_port": self.configured_port,
            "resolved_ips": self.resolved_ips.iter().map(IpAddr::to_string).collect::<Vec<_>>(),
            "same_cell": self.same_cell,
            "private_route": self.private_route,
            "identity_verified": self.identity_verified.to_json(),
            "reachable": self.reachable,
            "dial_skipped_reason": self.dial_skipped_reason,
            "resolution_may_be_synthetic": self.resolution_may_be_synthetic(),
        })
    }
}

/// Pure judgment, isolated so it is unit-testable without live DNS/network (see this module's
/// `#[cfg(test)]`): given what a live resolution of `entry.host()` returned, decide
/// `(same_cell, private_route)` — or `Err` if there is nothing to judge at all (zero resolved
/// addresses). §4.4/ADR-0003: a registry entry naming a deployment resource that no longer
/// exists must surface as *this* `Err` — this probe's own "missing object" — and never as a
/// silent `Ok((false, false))` a caller could round down to an ordinary `value = 0` health
/// reading (module doc's 坑5 call-out).
fn evaluate_resolved_ips(
    entry: &ResourceEntry,
    resolved_ips: &[IpAddr],
) -> Result<(bool, bool), String> {
    if resolved_ips.is_empty() {
        return Err(format!("{} resolved to zero addresses", entry.host()));
    }
    let same_cell = resolved_ips.iter().all(|ip| entry.address_in_cell(*ip));
    let private_route = resolved_ips
        .iter()
        .all(|ip| is_private_or_reserved_address(*ip) && !is_metadata_or_link_local(*ip));
    Ok((same_cell, private_route))
}

/// Runs `QDRANT_REST`'s live probe: a fresh DNS resolution (independent of the transport's own
/// — see module doc) decides `(same_cell, private_route)`, and only when *both* hold does this
/// process go on to make a real `execute()` call.
///
/// Major fix: this used to call `execute()` unconditionally, after already computing
/// `same_cell`/`private_route` — recording the judgment in the envelope but never gating the
/// dial on it. Combined with the transport blocker fix's own repro (the probe's host is
/// env-supplied and typically an IP literal), that made `humaux-admin q cell.resources` an
/// env-driven blind SSRF/port-reachability oracle: it would open a real socket to whatever
/// `HUMAUX_QDRANT_HOST` names, including a cloud-metadata address, and report hit/miss via
/// `reachable`. A probe whose stated purpose is cross-checking declaration against reality must
/// not perform the very transfer it has already judged out-of-Cell.
async fn probe_qdrant(
    registry: IntraCellResourceRegistry,
    entry: &ResourceEntry,
) -> Result<ResourceReport, String> {
    let resolved_ips = SystemDnsResolve
        .resolve(entry.host(), entry.port())
        .map_err(|e| format!("DNS resolution failed for {}: {e}", entry.host()))?;
    let (same_cell, private_route) = evaluate_resolved_ips(entry, &resolved_ips)?;

    if !same_cell || !private_route {
        return Ok(ResourceReport {
            resource: IntraCellResource::QDRANT_REST.name(),
            configured_host: entry.host().to_string(),
            configured_port: entry.port(),
            resolved_ips,
            same_cell,
            private_route,
            identity_verified: IdentityVerified::NotApplicableDialSkipped,
            reachable: false,
            dial_skipped_reason: Some(
                "resolved address is outside the registered Cell CIDR/service-IP set — refusing \
                 to dial an out-of-Cell address even to answer 'is it reachable'",
            ),
        });
    }

    // Borrow `registry` for the permit before moving it into the transport below — both need
    // it, but `authorize_cell_access` only needs a shared borrow.
    let permit = authorize_cell_access(&registry, IntraCellResource::QDRANT_REST, REQUEST_TIMEOUT)
        .map_err(|e| format!("minting this probe's own CellAccessPermit failed: {e:?}"))?;
    let transport =
        HttpIntraCellTransport::new(registry, REQUEST_TIMEOUT, DEFAULT_MAX_RESPONSE_BYTES)
            .map_err(|e| format!("building the intra-Cell HTTP client failed: {e}"))?;
    let reachable = transport
        .execute(
            &permit,
            IntraCellRequest {
                method: IntraCellMethod::Get,
                path: "/".to_string(),
                json_body: None,
                headers: Vec::new(),
            },
        )
        .await
        .map(|r| r.status < 500)
        .unwrap_or(false);

    // §57.1 三态 (major fix): §83.4 判据5 (destination identity) is a deploy-gate concern this
    // crate's code cannot fully re-derive (`infra_cell`'s own module doc). When the resource
    // requires TLS, a successful `execute()` already implies certificate/hostname validation
    // passed (`reqwest`'s TLS backend fails the call otherwise) — `Pass`/`Fail` accordingly. A
    // plain-HTTP resource has nothing for this layer to verify at all: `NotApplicable`, never a
    // bare `true` standing in for "verified" on a connection that carried no certificate.
    let identity_verified = match (entry.tls(), reachable) {
        (true, true) => IdentityVerified::Pass,
        (true, false) => IdentityVerified::Fail,
        (false, _) => {
            IdentityVerified::NotApplicable("resource is plain HTTP — §83.4 判据5 is deploy-gate")
        }
    };

    Ok(ResourceReport {
        resource: IntraCellResource::QDRANT_REST.name(),
        configured_host: entry.host().to_string(),
        configured_port: entry.port(),
        resolved_ips,
        same_cell,
        private_route,
        identity_verified,
        reachable,
        dial_skipped_reason: None,
    })
}

/// `sha256:`-prefixed digest of the scanned scope (§4.4: "两次结果只有 scope_hash 相同才可
///比") — `cell_id` + the sorted resource-name set this run actually scanned.
fn scope_hash(cell_id: CellId, resources: &[&str]) -> String {
    use sha2::{Digest, Sha256};
    let mut sorted = resources.to_vec();
    sorted.sort_unstable();
    let canonical = format!(
        "cell.resources|cell_id={}|resources={}",
        cell_id.0,
        sorted.join(",")
    );
    let digest: [u8; 32] = Sha256::digest(canonical.as_bytes()).into();
    format!(
        "sha256:{}",
        digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    )
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unavailable".to_string())
}

/// `q cell.resources` — returns the process exit code (module doc's fail-closed contract).
pub fn run() -> i32 {
    let (registry, cell_id) = match registry_from_env() {
        Ok(v) => v,
        Err(MissingEnv(name)) => {
            eprintln!("q cell.resources: fail — missing object: {name}");
            return 2;
        }
    };
    let Some(entry) = registry.resolve(IntraCellResource::QDRANT_REST).cloned() else {
        eprintln!("q cell.resources: fail — missing object: QDRANT_REST not in registry");
        return 2;
    };

    let Ok(rt) = tokio::runtime::Runtime::new() else {
        eprintln!("q cell.resources: fail — could not start async runtime");
        return 1;
    };

    let report = match rt.block_on(probe_qdrant(registry, &entry)) {
        Ok(r) => r,
        Err(reason) => {
            eprintln!("q cell.resources: fail — missing object: {reason}");
            return 2;
        }
    };

    // §4.4 坑5 minor fix: `scanned_n` must come from the same set this run actually probed
    // (the same slice `scope_hash` hashes), not from `IntraCellResource::ALL`'s declared
    // count — those two only happen to agree today because `registry_from_env` can only ever
    // build `QDRANT_REST` and `ALL` has exactly one variant. Fail closed (this probe's own
    // "missing object", not a `value = 0` reading) the day a second `IntraCellResource`
    // variant exists but this Phase-0 probe has not been extended to cover it.
    let probed_resources: Vec<&str> = vec![report.resource];
    if probed_resources.len() != IntraCellResource::ALL.len() {
        let unwired: Vec<&str> = IntraCellResource::ALL
            .iter()
            .map(|r| r.name())
            .filter(|name| !probed_resources.contains(name))
            .collect();
        eprintln!(
            "q cell.resources: fail — missing object: not wired into this probe: {}",
            unwired.join(", ")
        );
        return 2;
    }

    // `value` = count of resources this run found healthy (`ResourceReport::healthy`), out of
    // `scanned_n` — not a count of *unhealthy* resources. Spec §4.4's table row freezes only
    // `scanned_n`'s meaning; this doc is `value`'s one authoritative source for this probe.
    let value = i64::from(report.healthy());
    let envelope = serde_json::json!({
        "value": value,
        "scanned_n": probed_resources.len(),
        "scope_hash": scope_hash(cell_id, &probed_resources),
        "checked_at": now_rfc3339(),
        "probe_version": PROBE_VERSION,
        "resources": [report.to_json()],
    });
    println!("{}", serde_json::to_string_pretty(&envelope).unwrap());
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry_with_cidr(cidr: &str) -> ResourceEntry {
        ResourceEntry::new(
            "qdrant.internal",
            6333,
            CellId(uuid::Uuid::now_v7()),
            vec![cidr.parse().unwrap()],
            BTreeSet::new(),
            false,
        )
        .unwrap()
    }

    /// §4.4 fault-injection, task-required shape: "registry 加一行指向不存在的 deployment
    /// resource" — modeled here as the resolver returning zero addresses, exactly what
    /// `std::net::ToSocketAddrs` (`SystemDnsResolve`) returns for a genuinely non-existent
    /// hostname. This must be a hard `Err` (this probe's own "missing object", fail-closed) —
    /// never a value the caller could round down to an ordinary `Ok((false, false))` and print
    /// as a successful `value = 0` reading.
    #[test]
    fn empty_resolution_is_a_hard_error_not_a_health_reading() {
        let result = evaluate_resolved_ips(&entry_with_cidr("10.0.0.0/8"), &[]);
        assert!(result.is_err());
    }

    #[test]
    fn resolution_inside_the_registered_cidr_reads_healthy() {
        let (same_cell, private_route) = evaluate_resolved_ips(
            &entry_with_cidr("10.0.0.0/8"),
            &["10.1.2.3".parse().unwrap()],
        )
        .unwrap();
        assert!(same_cell);
        assert!(private_route);
    }

    /// A live resolution that succeeds but lands outside the registered CIDR is genuine data
    /// (§4.4: not "没扫到"), not a probe malfunction — `evaluate_resolved_ips` must return
    /// `Ok` here (the caller reports `same_cell: false` in the envelope), distinguishing this
    /// from the empty-resolution case above.
    #[test]
    fn resolution_outside_the_registered_cidr_is_data_not_an_error() {
        let (same_cell, _private_route) = evaluate_resolved_ips(
            &entry_with_cidr("10.0.0.0/8"),
            &["93.184.216.34".parse().unwrap()],
        )
        .unwrap();
        assert!(!same_cell);
    }
}
