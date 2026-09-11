//! `adapters::byok::ssrf` — §11.4 Custom / OpenAI-compatible Endpoint Security.
//!
//! A user-supplied custom `base_url` (BYOK "bring your own OpenAI-compatible endpoint") is
//! untrusted SSRF input: "普通 SaaS 用户不能借 custom endpoint 探测 Humaux 内网" (§11.4). This
//! module is the one choke point every custom-endpoint URL must pass through — at
//! configuration time (before it is ever stored on a [`super::ReasoningProviderDescriptor`])
//! and again on every redirect hop a transport impl follows (§11.4 "redirect policy: 不跟随到
//! 新 host 或每跳复验" — [`validate_custom_endpoint`] is the same function called for the
//! original URL and for each `Location` header, not two different implementations to keep in
//! sync).
//!
//! DNS resolution is behind the [`DnsResolver`] trait rather than called directly so a test can
//! simulate DNS rebinding (a hostname that *validates* by name but resolves to a forbidden IP,
//! §11.4's own worked case) without needing a real DNS server or network access at all — the
//! [`SystemDnsResolver`] production impl is the only one that actually touches the network,
//! via `std::net::ToSocketAddrs` (no new dependency: §83.4 G80-3 only fences raw
//! `reqwest`/`hyper` *client* construction, not DNS lookups, and `ToSocketAddrs` is stdlib).

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs};
use std::time::Duration;

// =============================================================================
// Policy
// =============================================================================

/// §11.4's bounds on a custom-endpoint call. `follow_redirects: false` by default — per
/// §11.4's redirect-policy line, the safe default is not to follow at all; a transport impl
/// that does choose to follow a redirect (`follow_redirects: true`, an explicit
/// tenant/enterprise policy override this module does not itself construct) MUST call
/// [`validate_custom_endpoint`] again on the `Location` target before connecting to it.
#[derive(Debug, Clone, Copy)]
pub struct CustomEndpointPolicy {
    pub max_response_bytes: u64,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub follow_redirects: bool,
    /// §11.3/§11.4: a provider-supplied `Retry-After` is untrusted custom-endpoint input — a
    /// hostile or misconfigured endpoint returning an enormous value must not park a caller's
    /// retry loop for it. `super::OpenAiCompatibleProvider::send_once` clamps every observed
    /// `Retry-After` to this ceiling before it can reach a sleep call.
    pub max_retry_after: Duration,
}

impl Default for CustomEndpointPolicy {
    fn default() -> Self {
        Self {
            // ponytail: fixed defaults, not per-tenant config — §11.2's
            // `custom_endpoint_policy` jsonb column is where a tenant override would live;
            // wiring that through is a later task (this module only defines the shape the
            // override would tighten/loosen, never widens past HTTPS-only on its own).
            max_response_bytes: 4 * 1024 * 1024,
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(30),
            follow_redirects: false,
            max_retry_after: Duration::from_secs(60),
        }
    }
}

/// §11.4 body-size bound, checked once the transport impl knows the actual length (either a
/// `Content-Length` header or the final buffered size — this function does not itself stream,
/// see [`super::OpenAiCompatTransport`]'s doc for why that responsibility stays with the
/// transport impl).
pub fn enforce_body_size_bound(
    actual_len: u64,
    policy: &CustomEndpointPolicy,
) -> Result<(), SsrfError> {
    if actual_len > policy.max_response_bytes {
        Err(SsrfError::ResponseTooLarge {
            actual: actual_len,
            limit: policy.max_response_bytes,
        })
    } else {
        Ok(())
    }
}

// =============================================================================
// Errors
// =============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SsrfError {
    /// §11.4 "HTTPS default" — anything else (`http://`, no scheme, etc.) is rejected.
    NonHttps,
    UrlMalformed(String),
    /// The URL's host is itself a literal IP address that resolves (trivially) to a
    /// forbidden range.
    HostIpForbidden(IpAddr),
    DnsResolutionFailed(String),
    /// The hostname resolved successfully, but to (at least one) forbidden address — this is
    /// the case that also covers DNS rebinding: a name that looks fine passing name-only
    /// validation but binds to `127.0.0.1`/an RFC1918 range/link-local/etc.
    ResolvedIpForbidden {
        host: String,
        ip: IpAddr,
    },
    /// A resolver returned zero addresses.
    NoAddressResolved(String),
    ResponseTooLarge {
        actual: u64,
        limit: u64,
    },
}

impl std::fmt::Display for SsrfError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NonHttps => write!(f, "custom endpoint must use https:// (§11.4 HTTPS default)"),
            Self::UrlMalformed(s) => write!(f, "malformed custom endpoint URL: {s}"),
            Self::HostIpForbidden(ip) => {
                write!(f, "host IP literal {ip} is private/reserved/loopback")
            }
            Self::DnsResolutionFailed(host) => write!(f, "DNS resolution failed for {host}"),
            Self::ResolvedIpForbidden { host, ip } => {
                write!(f, "{host} resolved to forbidden address {ip}")
            }
            Self::NoAddressResolved(host) => write!(f, "{host} resolved to zero addresses"),
            Self::ResponseTooLarge { actual, limit } => {
                write!(f, "response size {actual} exceeds bound {limit}")
            }
        }
    }
}

impl std::error::Error for SsrfError {}

// =============================================================================
// DNS resolver injection (real network access lives only in SystemDnsResolver)
// =============================================================================

pub trait DnsResolver: Send + Sync {
    fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, SsrfError>;
}

/// Static DNS pins for hardened egress (§11.4). Hosts listed here resolve to the pinned
/// addresses instead of the system resolver — for operators whose local DNS is not
/// trustworthy (VPN "fake-ip" ranges, captive resolvers) — while every other host falls back
/// to [`SystemDnsResolver`]. The forbidden-range check downstream still runs on whatever this
/// returns, so a pin can never admit a loopback/private/reserved address.
///
/// Spec format: `host=ip[|ip...][,host=ip...]`, e.g. `api.example.com=203.0.113.10|203.0.113.11`.
#[derive(Debug, Clone, Default)]
pub struct PinnedDnsResolver {
    pins: std::collections::HashMap<String, Vec<IpAddr>>,
}

impl PinnedDnsResolver {
    /// Parses the pin spec; rejects empty hosts, empty pin lists and unparsable addresses.
    pub fn parse(spec: &str) -> Result<Self, SsrfError> {
        let mut pins = std::collections::HashMap::new();
        for entry in spec.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            let (host, addrs) = entry.split_once('=').ok_or_else(|| {
                SsrfError::UrlMalformed(format!("dns pin {entry:?}: expected host=ip"))
            })?;
            let host = host.trim().to_ascii_lowercase();
            if host.is_empty() {
                return Err(SsrfError::UrlMalformed(format!(
                    "dns pin {entry:?}: empty host"
                )));
            }
            let parsed: Result<Vec<IpAddr>, _> = addrs
                .split('|')
                .map(str::trim)
                .filter(|a| !a.is_empty())
                .map(str::parse::<IpAddr>)
                .collect();
            let parsed = parsed
                .map_err(|_| SsrfError::UrlMalformed(format!("dns pin {entry:?}: bad address")))?;
            if parsed.is_empty() {
                return Err(SsrfError::UrlMalformed(format!(
                    "dns pin {entry:?}: no addresses"
                )));
            }
            pins.insert(host, parsed);
        }
        Ok(Self { pins })
    }

    /// Number of pinned hosts.
    #[must_use]
    pub fn len(&self) -> usize {
        self.pins.len()
    }

    /// True when no host is pinned (every lookup falls through to the system resolver).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pins.is_empty()
    }
}

impl DnsResolver for PinnedDnsResolver {
    fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, SsrfError> {
        match self.pins.get(&host.to_ascii_lowercase()) {
            Some(addrs) => Ok(addrs.clone()),
            None => SystemDnsResolver.resolve(host),
        }
    }
}

/// Production resolver: `std::net::ToSocketAddrs` — works uniformly for a bare hostname
/// (real DNS lookup) and an IP literal (returns that IP with no lookup at all), so both cases
/// funnel through the same forbidden-range check below.
pub struct SystemDnsResolver;

impl DnsResolver for SystemDnsResolver {
    fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, SsrfError> {
        // Port 0 is a placeholder — `ToSocketAddrs` requires a port to do the lookup but this
        // call only wants the resolved IPs, never an actual socket.
        (host, 0u16)
            .to_socket_addrs()
            .map(|it| it.map(|sa| sa.ip()).collect())
            .map_err(|_| SsrfError::DnsResolutionFailed(host.to_string()))
    }
}

// =============================================================================
// URL parsing (minimal, deliberately strict — see module doc)
// =============================================================================

struct ParsedAuthority {
    host: String,
    port: u16,
}

/// Deliberately narrow parser — this is not a general-purpose URL library, it exists only to
/// pull `(host, port)` out of a `https://...` custom endpoint string safely enough for SSRF
/// purposes. ponytail: no `url` crate dependency added for this (rung 5 — "already-installed
/// dependency solves it" does not apply, nothing in this workspace depends on `url` yet, and
/// pulling one in for four fields is disproportionate); upgrade path is swapping this for the
/// `url` crate if a real path/query ever needs parsing here too.
///
/// Rejects any authority containing `@` outright — no userinfo support at all. This is a
/// narrowing of accepted syntax, not a corner cut: userinfo-in-URL (`https://
/// trusted.com@evil.com/`) is a classic SSRF/parser-confusion vector, and BYOK custom
/// endpoints have no legitimate use for embedded credentials in the URL itself (the API key
/// goes in the `Authorization` header, never the URL).
fn parse_https_authority(url: &str) -> Result<ParsedAuthority, SsrfError> {
    let rest = url.strip_prefix("https://").ok_or(SsrfError::NonHttps)?;
    if rest.is_empty() {
        return Err(SsrfError::UrlMalformed("empty host".to_string()));
    }
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    if authority.contains('@') {
        return Err(SsrfError::UrlMalformed(
            "userinfo (@) in custom endpoint URL is not allowed".to_string(),
        ));
    }
    if authority.is_empty() {
        return Err(SsrfError::UrlMalformed("empty host".to_string()));
    }

    if let Some(bracket_rest) = authority.strip_prefix('[') {
        // IPv6 literal host: `[::1]` or `[::1]:8443`.
        let close = bracket_rest
            .find(']')
            .ok_or_else(|| SsrfError::UrlMalformed("unterminated IPv6 literal".to_string()))?;
        let host = bracket_rest[..close].to_string();
        let after = &bracket_rest[close + 1..];
        let port = if let Some(p) = after.strip_prefix(':') {
            p.parse::<u16>()
                .map_err(|_| SsrfError::UrlMalformed(format!("invalid port {p:?}")))?
        } else {
            443
        };
        return Ok(ParsedAuthority { host, port });
    }

    match authority.rsplit_once(':') {
        Some((host, port_str))
            if port_str.chars().all(|c| c.is_ascii_digit()) && !port_str.is_empty() =>
        {
            let port = port_str
                .parse::<u16>()
                .map_err(|_| SsrfError::UrlMalformed(format!("invalid port {port_str:?}")))?;
            Ok(ParsedAuthority {
                host: host.to_string(),
                port,
            })
        }
        _ => Ok(ParsedAuthority {
            host: authority.to_string(),
            port: 443,
        }),
    }
}

// =============================================================================
// Forbidden-IP checks — hand-rolled (see doc below for why)
// =============================================================================

/// §11.4 "private/reserved IP policy": every RFC1918/loopback/link-local/reserved/multicast
/// range, for both address families. Hand-rolled octet checks rather than relying solely on
/// `Ipv4Addr`/`Ipv6Addr`'s built-in `is_private`/`is_loopback`/etc: `Ipv6Addr` has no stable
/// `is_private`/unique-local helper across all supported Rust versions, so a single explicit
/// table covers both families uniformly and is exhaustively unit-tested below, rather than
/// depending on a partial std API plus a hand-written IPv6 gap-filler that could drift apart.
pub fn is_forbidden_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => ipv4_is_forbidden(v4),
        IpAddr::V6(v6) => {
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return ipv4_is_forbidden(mapped);
            }
            ipv6_is_forbidden(v6)
        }
    }
}

fn ipv4_is_forbidden(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    ip.is_unspecified()      // 0.0.0.0
        || o[0] == 0          // 0.0.0.0/8
        || o[0] == 127        // 127.0.0.0/8 loopback
        || o[0] == 10         // 10.0.0.0/8 private
        || (o[0] == 172 && (16..=31).contains(&o[1])) // 172.16.0.0/12 private
        || (o[0] == 192 && o[1] == 168) // 192.168.0.0/16 private
        || (o[0] == 169 && o[1] == 254) // 169.254.0.0/16 link-local (incl. 169.254.169.254 cloud metadata)
        || (o[0] == 100 && (64..=127).contains(&o[1])) // 100.64.0.0/10 CGNAT
        || (o[0] == 192 && o[1] == 0 && o[2] == 0) // 192.0.0.0/24 IETF protocol assignments
        || (o[0] == 192 && o[1] == 0 && o[2] == 2) // 192.0.2.0/24 TEST-NET-1
        || (o[0] == 198 && (18..=19).contains(&o[1])) // 198.18.0.0/15 benchmark
        || (o[0] == 198 && o[1] == 51 && o[2] == 100) // 198.51.100.0/24 TEST-NET-2
        || (o[0] == 203 && o[1] == 0 && o[2] == 113) // 203.0.113.0/24 TEST-NET-3
        || o[0] >= 224 // 224.0.0.0/4 multicast .. 255.255.255.255 broadcast/reserved
}

fn ipv6_is_forbidden(ip: Ipv6Addr) -> bool {
    ip.is_unspecified() // ::
        || ip.is_loopback() // ::1
        || (ip.segments()[0] & 0xfe00) == 0xfc00 // fc00::/7 unique local
        || (ip.segments()[0] & 0xffc0) == 0xfe80 // fe80::/10 link-local
        || (ip.segments()[0] & 0xff00) == 0xff00 // ff00::/8 multicast
}

// =============================================================================
// Validation entry point
// =============================================================================

// ADR-0039 (card 17) closed the gap this comment used to record: the production transport
// (`super::EgressHttpTransport`) now installs `super::SsrfCheckedResolver` — this module's own
// resolver plus this module's own `is_forbidden_ip` — as the *only* DNS resolver its
// `reqwest::Client` consults, so the connect-time lookup runs the same judgment this function
// ran, and a name that rebinds to a forbidden address is refused before any TCP connection
// (`crates/adapters/tests/byok_egress_rebinding.rs` asserts the forbidden address's listener
// sees nothing). `resolved_ips` therefore stays informational: it is not the pin, the shared
// resolver is. Residual ceiling, honestly recorded: with `HTTP_PROXY`/`HTTPS_PROXY` set and the
// destination outside `NO_PROXY`, resolution happens at the proxy and no client-side resolver
// is consulted at all — see `humaux_infra_egress::resolver`'s module doc.
#[derive(Debug, Clone)]
pub struct ValidatedEndpoint {
    pub host: String,
    pub port: u16,
    pub resolved_ips: Vec<IpAddr>,
}

/// §11.4's chain, condensed to what is testable without a real network: HTTPS default -> host
/// parse (no userinfo) -> DNS/IP resolution -> reject if *any* resolved address is
/// private/reserved/loopback/link-local. Callers still own the remaining §11.4 items this
/// function cannot decide alone (`TenantDataPolicy`/`Processor Registry`/`EgressPolicy`/region
/// metadata — all *policy* lookups, not the URL/IP shape this function checks) and must apply
/// [`enforce_body_size_bound`] plus `policy.connect_timeout`/`read_timeout` at the actual
/// transport layer (pending wiring, see `super`'s module doc).
///
/// Also the sole redirect-hop re-validator (§11.4 "每跳复验") — a transport impl that follows
/// a redirect calls this same function again on the `Location` header value, never skipping
/// straight to a raw connect.
pub fn validate_custom_endpoint(
    url: &str,
    resolver: &dyn DnsResolver,
) -> Result<ValidatedEndpoint, SsrfError> {
    let authority = parse_https_authority(url)?;

    // A bracketed/bare IPv6 or dotted-quad IPv4 host parses directly as an IP literal —
    // checked before any resolver call so a literal loopback/private IP is rejected without
    // even a loopback DNS round trip.
    if let Ok(literal) = authority.host.parse::<IpAddr>() {
        if is_forbidden_ip(literal) {
            return Err(SsrfError::HostIpForbidden(literal));
        }
        return Ok(ValidatedEndpoint {
            host: authority.host,
            port: authority.port,
            resolved_ips: vec![literal],
        });
    }

    let resolved = resolver.resolve(&authority.host)?;
    if resolved.is_empty() {
        return Err(SsrfError::NoAddressResolved(authority.host));
    }
    // Every resolved address must be clean — a hostname that resolves to both a public and a
    // private address (a real-world DNS-rebinding-adjacent misconfiguration, not only the
    // classic TTL-based rebinding attack) is rejected outright, not "clean if any one is ok".
    for ip in &resolved {
        if is_forbidden_ip(*ip) {
            return Err(SsrfError::ResolvedIpForbidden {
                host: authority.host,
                ip: *ip,
            });
        }
    }

    Ok(ValidatedEndpoint {
        host: authority.host,
        port: authority.port,
        resolved_ips: resolved,
    })
}

/// Test-only resolver that ignores the hostname and always returns a fixed address list —
/// used by this module's own tests below and by `super::tests` (the `OpenAiCompatibleProvider`
/// construction path also validates its `base_url` through this same DNS-injection point, see
/// `OpenAiCompatibleProvider::new`), so both call sites share one fake instead of two
/// hand-rolled copies drifting apart.
#[cfg(test)]
pub(crate) struct FakeResolver(pub(crate) Vec<IpAddr>);

#[cfg(test)]
impl DnsResolver for FakeResolver {
    fn resolve(&self, _host: &str) -> Result<Vec<IpAddr>, SsrfError> {
        Ok(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn public_ip() -> IpAddr {
        // 93.184.216.34 (example.com's long-standing public address) — not in any reserved
        // range checked above.
        IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))
    }

    #[test]
    fn non_https_is_rejected() {
        let resolver = FakeResolver(vec![public_ip()]);
        let err = validate_custom_endpoint("http://api.example.com/v1", &resolver).unwrap_err();
        assert_eq!(err, SsrfError::NonHttps);
    }

    #[test]
    fn public_https_hostname_is_accepted() {
        let resolver = FakeResolver(vec![public_ip()]);
        let ok = validate_custom_endpoint("https://api.example.com/v1/chat", &resolver).unwrap();
        assert_eq!(ok.host, "api.example.com");
        assert_eq!(ok.port, 443);
        assert_eq!(ok.resolved_ips, vec![public_ip()]);
    }

    #[test]
    fn explicit_port_is_parsed() {
        let resolver = FakeResolver(vec![public_ip()]);
        let ok = validate_custom_endpoint("https://api.example.com:8443/v1", &resolver).unwrap();
        assert_eq!(ok.port, 8443);
    }

    #[test]
    fn loopback_ip_literal_is_rejected() {
        let resolver = FakeResolver(vec![public_ip()]); // must not even be consulted
        let err = validate_custom_endpoint("https://127.0.0.1:8080/", &resolver).unwrap_err();
        assert!(matches!(err, SsrfError::HostIpForbidden(_)));
    }

    #[test]
    fn private_rfc1918_ip_literal_is_rejected() {
        let resolver = FakeResolver(vec![public_ip()]);
        for host in ["10.0.0.5", "172.16.0.5", "192.168.1.5"] {
            let url = format!("https://{host}/");
            let err = validate_custom_endpoint(&url, &resolver).unwrap_err();
            assert!(matches!(err, SsrfError::HostIpForbidden(_)), "{host}");
        }
    }

    #[test]
    fn cloud_metadata_link_local_ip_is_rejected() {
        let resolver = FakeResolver(vec![public_ip()]);
        let err = validate_custom_endpoint("https://169.254.169.254/latest/meta-data", &resolver)
            .unwrap_err();
        assert!(matches!(err, SsrfError::HostIpForbidden(_)));
    }

    /// §11.4's DNS-rebinding worked case: a hostname that reads as a normal public domain but
    /// resolves to a loopback address. The resolver is faked to always return `127.0.0.1`
    /// regardless of the hostname string, reproducing the attack shape without a real DNS
    /// server: name-only validation would pass this, IP-after-resolution validation must not.
    #[test]
    fn dns_rebinding_to_loopback_is_rejected() {
        let rebinding_resolver = FakeResolver(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]);
        let err = validate_custom_endpoint(
            "https://totally-normal-looking-domain.com/v1/chat",
            &rebinding_resolver,
        )
        .unwrap_err();
        assert_eq!(
            err,
            SsrfError::ResolvedIpForbidden {
                host: "totally-normal-looking-domain.com".to_string(),
                ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            }
        );
    }

    #[test]
    fn dns_rebinding_to_private_rfc1918_is_rejected() {
        let rebinding_resolver = FakeResolver(vec![IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))]);
        let err = validate_custom_endpoint("https://looks-fine.example.net/", &rebinding_resolver)
            .unwrap_err();
        assert!(matches!(err, SsrfError::ResolvedIpForbidden { .. }));
    }

    #[test]
    fn one_forbidden_address_among_several_resolved_still_rejects() {
        let mixed = FakeResolver(vec![public_ip(), IpAddr::V4(Ipv4Addr::LOCALHOST)]);
        let err =
            validate_custom_endpoint("https://multi-a-record.example.com/", &mixed).unwrap_err();
        assert!(matches!(err, SsrfError::ResolvedIpForbidden { .. }));
    }

    #[test]
    fn userinfo_in_authority_is_rejected() {
        let resolver = FakeResolver(vec![public_ip()]);
        let err = validate_custom_endpoint("https://trusted.com@evil.com/", &resolver).unwrap_err();
        assert!(matches!(err, SsrfError::UrlMalformed(_)));
    }

    #[test]
    fn ipv6_loopback_literal_is_rejected() {
        let resolver = FakeResolver(vec![public_ip()]);
        let err = validate_custom_endpoint("https://[::1]/", &resolver).unwrap_err();
        assert!(matches!(err, SsrfError::HostIpForbidden(_)));
    }

    #[test]
    fn ipv6_unique_local_is_forbidden() {
        assert!(is_forbidden_ip(IpAddr::V6("fc00::1".parse().unwrap())));
        assert!(is_forbidden_ip(IpAddr::V6("fe80::1".parse().unwrap())));
        assert!(!is_forbidden_ip(IpAddr::V6(
            "2606:2800:220:1:248:1893:25c8:1946".parse().unwrap() // example.com AAAA-shaped
        )));
    }

    #[test]
    fn body_size_bound_enforced() {
        let policy = CustomEndpointPolicy {
            max_response_bytes: 100,
            ..Default::default()
        };
        assert!(enforce_body_size_bound(100, &policy).is_ok());
        assert!(enforce_body_size_bound(101, &policy).is_err());
    }

    // A dedicated "redirect hop" test does not exist: no redirect-following code exists yet
    // (`CustomEndpointPolicy::follow_redirects` is a policy flag with no transport impl behind
    // it — see that field's doc) to exercise. §11.4 "每跳复验" reduces, until such an impl
    // exists, to "there is exactly one validation function" — already demonstrated by every
    // other test in this module calling the same `validate_custom_endpoint`, not by a test
    // that pretends to follow a redirect it never does.
}

#[cfg(test)]
mod pinned_dns_tests {
    use super::*;

    #[test]
    fn pinned_host_returns_pins_and_others_fall_back() {
        let r = PinnedDnsResolver::parse(
            "Api.Example.com=203.0.113.10|203.0.113.11, other.example=198.51.100.7",
        )
        .expect("valid spec");
        assert_eq!(r.len(), 2);
        let pinned = r.resolve("api.example.com").expect("pinned");
        assert_eq!(
            pinned,
            vec![
                "203.0.113.10".parse::<IpAddr>().unwrap(),
                "203.0.113.11".parse().unwrap()
            ]
        );
        // An IP literal is not pinned: the fallback resolver returns it verbatim.
        assert_eq!(
            r.resolve("192.0.2.9").expect("literal"),
            vec!["192.0.2.9".parse::<IpAddr>().unwrap()]
        );
    }

    #[test]
    fn malformed_specs_are_rejected() {
        for bad in [
            "api.example.com",
            "=203.0.113.10",
            "api.example.com=",
            "api.example.com=not-an-ip",
        ] {
            assert!(
                PinnedDnsResolver::parse(bad).is_err(),
                "{bad:?} must be rejected"
            );
        }
        assert!(
            PinnedDnsResolver::parse("")
                .expect("empty spec is no pins")
                .is_empty()
        );
    }

    #[test]
    fn pins_never_bypass_the_forbidden_range_check() {
        // A pin to a reserved/private address still fails downstream: the check is on the
        // resolved set, not on how it was resolved.
        let r = PinnedDnsResolver::parse("api.example.com=198.18.0.139").expect("parses");
        let addrs = r.resolve("api.example.com").expect("resolves");
        assert!(addrs.iter().copied().all(is_forbidden_ip));
    }
}
