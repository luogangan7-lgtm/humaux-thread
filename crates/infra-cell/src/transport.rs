//! `infra-cell::transport` — ADR-0003 / §83.4 Layer 1B: the same-Cell HTTP capability wrapper.
//! Depends-on: crates=[async-trait, humaux-infra-network, serde_json, tokio, url, uuid]; services=[Qdrant(*)];
//!   env=[]; modules=[humaux-infra-network, infra-cell::permit, infra-cell::resource, infra-network::http]
//! Called-by: [adapters::projection_worker, adapters::public_projection, adapters::qdrant, adapters::retrieve, admin::cell_resources, gateway::bootstrap, gateway::recall, public-worker::main, retrieval-worker::main, tests, xtask::e2e_seed, xtask::switch_visible]
//! Invariants: [execute takes a resource-relative path plus a CellAccessPermit, never a URL; paths are validated,
//!   resolved addresses must be in-Cell and not metadata/link-local, redirects are refused; every failure is a typed
//!   IntraCellError]
//! Spec: Baseline §83.4; ADR-0003
//!
//! [`IntraCellHttpTransport::execute`] takes an [`IntraCellRequest`] (a resource-relative
//! path + optional JSON body) plus a [`CellAccessPermit`], not a bare URL. [`validate_path`]
//! enforces that the relative-path contract actually holds — 判据1 is *not* "already closed by
//! the type signature" the way an earlier version of this doc claimed: `path` is a plain
//! `String` field, and `format!("http://{host}:{port}{path}")` string interpolation (not a URL
//! parser) is what turns it into the dialed address, so a path like `@evil.example.com/steal`
//! would otherwise rewrite the URL's authority entirely, landing the connection on a host that
//! was never DNS-resolved or CIDR-checked at all.
//!
//! **§83.4 判据3, single-resolver fix (ADR-0003 second round; OWASP SSRF Cheat Sheet: DNS
//! rebinding / check-then-use DNS pinning bypass) for a *hostname* authority.** An earlier
//! version of this file resolved `entry.host()` itself via `self.dns.resolve(...)`, validated
//! the result, and *then* handed the bare hostname string to `reqwest` — which resolves the
//! name a second time, inside its own connector, with no guarantee the second lookup returns
//! what the first one validated. [`ValidatingResolver`] closes that gap by construction rather
//! than by discipline: it implements `reqwest::dns::Resolve`
//! (`humaux_infra_network::http::build_client_with_resolver`, stable in reqwest 0.12.28) and
//! *is* the client's DNS resolver, so there is exactly one lookup per connection attempt *for a
//! name host*, and the CIDR/metadata judgment runs inside that single lookup, before `reqwest`
//! ever receives an address to dial.
//!
//! **This does not cover an IP-literal `entry.host()`, which is the common case in this tree's
//! own tests and every real registry construction site today.** `reqwest`/`hyper`'s connector
//! never calls a custom `dns_resolver` at all when the URL authority is (or WHATWG-normalizes
//! to) an IP literal — it parses and dials such an address directly, so [`ValidatingResolver`]
//! is never consulted for it (this was a real regression the second round introduced: the
//! first round's manual `self.dns.resolve(entry.host())` pre-check ran unconditionally, and
//! `ToSocketAddrs` happened to handle an IP-literal host too). [`HttpIntraCellTransport::execute`]
//! therefore re-derives the exact address `reqwest` is about to dial via `url::Url::parse` (the
//! same WHATWG host-parsing `reqwest` itself runs) *before* `send()`, and applies the identical
//! metadata/link-local-then-CIDR judgment there for a `Host::Ipv4`/`Host::Ipv6` result — a
//! `Host::Domain` result is left to [`ValidatingResolver`], run unconditionally when `send()`
//! actually resolves it. The unconditional hard block on the cloud-metadata address
//! `169.254.169.254` and any other link-local address runs first, before the registry's CIDR
//! allowlist, in both paths, so a misconfigured registry entry cannot accidentally widen that
//! one address back in. A rejected hostname resolution surfaces to the connector as a boxed
//! [`IntraCellError`]; `execute` walks the resulting `reqwest::Error`'s source chain
//! ([`downcast_intra_cell_error`]) to recover the original typed variant instead of flattening
//! it to a generic connect-failure string — the IP-literal pre-check instead returns its typed
//! error directly, having never called `send()` at all. TLS certificate validation still runs
//! against the original hostname (SNI/SAN) — this intercepts address *resolution* only, never
//! makes the client dial a bare IP with hostname verification skipped. A 3xx response is never
//! followed to a second, unchecked host either
//! (`humaux_infra_network::http::build_client_with_resolver` disables `reqwest`'s
//! redirect-following entirely; a 3xx status is surfaced here as
//! [`IntraCellError::UnexpectedRedirect`] instead of a followed connection).
//!
//! §83.4 判据4/5 (deploy-gate: no Internet/NAT route out of the Cell, mTLS/TLS-SAN identity
//! pinning) remain deploy-gate concerns (README "Intra-cell network deploy gate"), not
//! re-derived here — this crate's code-level judgment covers 判据1/2/3/6.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use humaux_infra_network::http::{ClientConfig, build_client_with_resolver};
use humaux_infra_network::reqwest;
use serde_json::Value;

use crate::permit::CellAccessPermit;
use crate::resource::{CellAccessMode, IntraCellResourceRegistry, ResourceEntry};

/// §83.4: response-buffering cap this transport enforces by default — same value and same
/// rationale `HttpEgressConfig::max_response_bytes` (Layer 1A) documents: comfortably covers a
/// real Qdrant REST response with margin, raise it if a real response is ever legitimately
/// observed near it.
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// HTTP method [`IntraCellRequest`] carries — a closed set (this crate's callers are all
/// Qdrant-REST-shaped today, §17); extend if a second [`crate::resource::IntraCellResource`]
/// needs a method these three don't cover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntraCellMethod {
    Get,
    Put,
    Post,
    Delete,
}

/// A resource-relative request — `path` is joined onto the registry-resolved
/// `host:port`, never a caller-supplied absolute URL (`execute`'s [`validate_path`] enforces
/// this at call time; see this module's doc for why the type shape alone was not enough).
/// `headers` (§83.4 判据5) carries per-call headers such as Qdrant's `api-key` — sourced by the
/// caller from the Cell's own secret store, never a literal (README "Intra-cell network deploy
/// gate").
#[derive(Debug, Clone)]
// dep: Qdrant(*) — qdrant wire call
pub struct IntraCellRequest {
    pub method: IntraCellMethod,
    /// Must start with `/`, and must not contain `@`, `//`, a `..` path segment, or
    /// whitespace/control characters — see [`validate_path`].
    pub path: String,
    pub json_body: Option<Value>,
    pub headers: Vec<(String, String)>,
}

/// §17.4-style raw response from an [`IntraCellHttpTransport::execute`] call — `status` and a
/// parsed JSON body when the response carried one.
#[derive(Debug, Clone, PartialEq)]
pub struct IntraCellResponse {
    pub status: u16,
    pub json_body: Option<Value>,
}

/// [`IntraCellHttpTransport::execute`] failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntraCellError {
    ExpiredPermit,
    /// §83.4 判据1: `request.path` does not satisfy [`validate_path`]'s contract.
    InvalidPath(String),
    /// 2026-08-30 read-only ruling: `permit`'s [`CellAccessMode::QdrantReadOnly`] does not admit
    /// this request's method/path — checked before DNS resolution or any connection attempt
    /// (see [`is_read_allowed`]'s doc for the exact allowlist).
    WriteDenied,
    /// The permit's resource has no registry entry — should not happen for a permit minted by
    /// [`crate::permit::authorize_cell_access`] against the same registry, but re-checked here
    /// (defense in depth, same posture `HttpExternalCall::call` takes re-checking `processor`).
    UnregisteredResource,
    DnsResolutionFailed(String),
    NoAddressResolved(String),
    /// §83.4 判据3, unconditional hard block: a resolved address is the cloud-metadata address
    /// or otherwise link-local — refused regardless of the resource's configured CIDR set.
    MetadataOrLinkLocalAddress(IpAddr),
    /// §83.4 判据3: a resolved address is outside the resource's registered Cell CIDR set.
    AddressNotInCell(IpAddr),
    /// §83.4 判据3: the destination responded with a 3xx — `build_client` disables redirect
    /// following, so this is the destination's own status, surfaced instead of silently
    /// followed to an unchecked second host.
    UnexpectedRedirect(u16),
    /// The response body exceeded the configured cap before being fully read.
    ResponseTooLarge {
        max: usize,
    },
    RequestFailed(String),
    ResponseBodyInvalid(String),
}

impl std::fmt::Display for IntraCellError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExpiredPermit => write!(f, "CellAccessPermit expired"),
            Self::InvalidPath(p) => write!(
                f,
                "invalid path {p:?}: must start with '/' and contain no '@', \"//\", \"..\" \
                 segment, or whitespace/control character"
            ),
            Self::WriteDenied => write!(
                f,
                "read-only permit: method/path is not on the Qdrant read allowlist"
            ),
            Self::UnregisteredResource => write!(f, "resource not in intra-cell registry"),
            Self::DnsResolutionFailed(h) => write!(f, "DNS resolution failed for {h}"),
            Self::NoAddressResolved(h) => write!(f, "{h} resolved to zero addresses"),
            Self::MetadataOrLinkLocalAddress(ip) => {
                write!(
                    f,
                    "resolved address {ip} is metadata/link-local, hard-blocked"
                )
            }
            Self::AddressNotInCell(ip) => {
                write!(
                    f,
                    "resolved address {ip} is outside the resource's Cell CIDR set"
                )
            }
            Self::UnexpectedRedirect(status) => {
                write!(
                    f,
                    "destination responded {status} (redirects are never followed)"
                )
            }
            Self::ResponseTooLarge { max } => {
                write!(f, "response body exceeded {max} byte cap")
            }
            Self::RequestFailed(e) => write!(f, "request failed: {e}"),
            Self::ResponseBodyInvalid(e) => write!(f, "response body invalid: {e}"),
        }
    }
}

impl std::error::Error for IntraCellError {}

/// §83.4 判据1: `path` must be safely joinable onto `http(s)://{host}:{port}` by plain string
/// concatenation — checked unconditionally, before any DNS/CIDR work or connection attempt
/// (same posture [`is_metadata_or_link_local`] takes). Rejects anything that could rewrite the
/// URL's authority (`@`, a `//` anywhere), escape the resource's own path space (a `..`
/// segment), or smuggle a header/line-injection payload (whitespace/control characters) —
/// proven exploitable against `reqwest`/the `url` crate's real parser: a path of
/// `@evil.example.com/steal` joined onto `http://qdrant.internal:6333` parses to
/// `host=evil.example.com`, with `qdrant.internal` demoted to discarded userinfo.
fn validate_path(path: &str) -> Result<(), IntraCellError> {
    let ok = path.starts_with('/')
        && !path.contains('@')
        && !path.contains("//")
        && !path.split('/').any(is_dot_dot_segment)
        && !path.chars().any(|c| c.is_control() || c.is_whitespace());
    if ok {
        Ok(())
    } else {
        Err(IntraCellError::InvalidPath(path.to_string()))
    }
}

/// Whether `segment` is a `..` path-traversal segment once `.`'s only percent-encoding
/// (`%2e`/`%2E` — `0x2e` written out in hex; the digit `2` has no case, only `e` does, so
/// these two spellings are exhaustive) is folded back to a literal `.`. A literal-string
/// comparison alone (`segment == ".."`) misses `%2e%2e`/`.%2e`/`%2e.`: the WHATWG URL parser
/// [`validate_path`]'s caller ultimately hands `path` to (`url::Url::parse`, `reqwest`'s own
/// parser) decodes and collapses these identically to a real `..` segment.
fn is_dot_dot_segment(segment: &str) -> bool {
    segment.replace("%2e", ".").replace("%2E", ".") == ".."
}

/// Qdrant REST path suffixes a [`CellAccessMode::QdrantReadOnly`] permit's `POST` may target —
/// 2026-08-30 ruling's exact allowlist: search/query/scroll/count, never a mutating endpoint
/// (`/points`, `/collections`, `/snapshots`, …). Matched by suffix, not exact equality, so a
/// per-collection path (`/collections/{name}/points/search`) still matches.
const QDRANT_READ_ONLY_POST_SUFFIXES: [&str; 5] = [
    "/points/search",
    "/points/search/batch",
    "/points/query",
    "/points/scroll",
    "/points/count",
];

/// Whether `method`/`path` is admitted under [`CellAccessMode::QdrantReadOnly`]: `GET` on any
/// path, `POST` only to [`QDRANT_READ_ONLY_POST_SUFFIXES`]; `PUT`/`DELETE` are refused
/// unconditionally regardless of path.
///
/// Matches against the path component only — everything from the first `?`/`#` onward is
/// stripped before the suffix check. Every real Qdrant read carries a query string (e.g.
/// `?consistency=quorum`), so matching the raw string would deny all of them; conversely a
/// mutating endpoint could otherwise smuggle an allowlisted suffix into its query string (e.g.
/// `/collections/c/points/delete?zz=/points/search`) and pass an `ends_with` check on the raw
/// path. Splitting first closes both holes.
fn is_read_allowed(method: IntraCellMethod, path: &str) -> bool {
    let path_only = path.split(['?', '#']).next().unwrap_or(path);
    match method {
        IntraCellMethod::Get => true,
        IntraCellMethod::Post => QDRANT_READ_ONLY_POST_SUFFIXES
            .iter()
            .any(|suffix| path_only.ends_with(suffix)),
        IntraCellMethod::Put | IntraCellMethod::Delete => false,
    }
}

/// §83.4 Layer 1B's capability wrapper: same-Cell resource access, never `ops.data_disclosures`
/// (see module doc / `crate::permit` doc for why).
#[async_trait::async_trait]
pub trait IntraCellHttpTransport: Send + Sync {
    async fn execute(
        &self,
        permit: &CellAccessPermit,
        request: IntraCellRequest,
    ) -> Result<IntraCellResponse, IntraCellError>;
}

/// DNS resolution injected so tests can simulate a hostname resolving outside the configured
/// Cell CIDR (same purpose `adapters::byok::ssrf::DnsResolver` serves for the external-egress
/// SSRF guard — not reused directly: that trait lives in `adapters`, which sits *above* this
/// crate in the dependency direction, so importing it here would invert the layering §3/§78.3
/// freezes).
pub trait DnsResolve: Send + Sync {
    fn resolve(&self, host: &str, port: u16) -> Result<Vec<IpAddr>, IntraCellError>;
}

/// Production resolver: `std::net::ToSocketAddrs`, same stdlib-only approach
/// `adapters::byok::ssrf::SystemDnsResolver` already establishes (§83.4 G80-3 only fences raw
/// `reqwest`/`hyper` *client* construction, not DNS lookups).
pub struct SystemDnsResolve;

impl DnsResolve for SystemDnsResolve {
    fn resolve(&self, host: &str, port: u16) -> Result<Vec<IpAddr>, IntraCellError> {
        (host, port)
            .to_socket_addrs()
            .map(|it| it.map(|sa: SocketAddr| sa.ip()).collect())
            .map_err(|_| IntraCellError::DnsResolutionFailed(host.to_string()))
    }
}

/// Whether `ip` is the cloud-metadata address or otherwise link-local — checked
/// unconditionally, before the resource's own configured CIDR allowlist, so a misconfigured
/// registry entry cannot accidentally re-admit it (§83.4 判据3's own explicit call-out). `pub`
/// (not just crate-private) so `bins/admin`'s `cell.resources` live probe (§4.4) can apply the
/// identical unconditional hard block when reporting a resolved address's live status.
pub fn is_metadata_or_link_local(ip: IpAddr) -> bool {
    match ip.to_canonical() {
        IpAddr::V4(v4) => v4.is_link_local(), // covers 169.254.0.0/16, incl. 169.254.169.254
        IpAddr::V6(v6) => (v6.segments()[0] & 0xffc0) == 0xfe80, // fe80::/10
    }
}

/// The single DNS resolver `HttpIntraCellTransport`'s `reqwest::Client` is built with (module
/// doc: "single-resolver fix") — implements `reqwest::dns::Resolve` so it is the *only* name
/// resolution this client ever performs, and enforces §83.4 判据3 (metadata/link-local hard
/// block, then Cell-CIDR membership) inside that one lookup rather than in a separate pre-check
/// pass a connector-level second lookup could disagree with.
struct ValidatingResolver {
    /// The injected, possibly test-doubled resolver — see `Cargo.toml`'s doc for why the
    /// blocking call happens on `tokio::task::spawn_blocking`.
    dns: Arc<dyn DnsResolve>,
    /// Snapshot of every registered resource, keyed by its registered hostname, lowercased
    /// (`with_resolver`'s doc) — taken once at transport construction (the registry does not
    /// change after bootstrap, §78.1). A host this transport's URLs never target (i.e. not in
    /// this map) resolves to [`IntraCellError::UnregisteredResource`]: this resolver only ever
    /// serves hostnames `execute` itself builds from the registry, so a miss here means
    /// something upstream already deviated from the registry it was handed.
    by_host: BTreeMap<String, ResourceEntry>,
}

impl reqwest::dns::Resolve for ValidatingResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        // `url::Url::parse` (both this crate's own URL build and `reqwest`'s internal one)
        // lowercases the authority before any resolver ever sees it — but `by_host`'s keys
        // come from the registry's `entry.host()` verbatim, so a registry entry with any
        // uppercase letter would otherwise miss this map on every call and fail closed with a
        // misleading `UnregisteredResource` (the resource *is* registered). Lowercasing here
        // too keeps the lookup correct regardless of the registry's own casing.
        let host = name.as_str().to_ascii_lowercase();
        let dns = self.dns.clone();
        let entry = self.by_host.get(&host).cloned();
        Box::pin(async move {
            let host_for_lookup = host.clone();
            let addrs = tokio::task::spawn_blocking(move || dns.resolve(&host_for_lookup, 0))
                .await
                .map_err(|e| {
                    Box::new(IntraCellError::DnsResolutionFailed(format!(
                        "resolver task did not complete: {e}"
                    ))) as Box<dyn std::error::Error + Send + Sync>
                })?
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            if addrs.is_empty() {
                return Err(Box::new(IntraCellError::NoAddressResolved(host))
                    as Box<dyn std::error::Error + Send + Sync>);
            }
            for ip in &addrs {
                if is_metadata_or_link_local(*ip) {
                    return Err(Box::new(IntraCellError::MetadataOrLinkLocalAddress(*ip))
                        as Box<dyn std::error::Error + Send + Sync>);
                }
            }
            let Some(entry) = entry else {
                return Err(Box::new(IntraCellError::UnregisteredResource)
                    as Box<dyn std::error::Error + Send + Sync>);
            };
            for ip in &addrs {
                if !entry.address_in_cell(*ip) {
                    return Err(Box::new(IntraCellError::AddressNotInCell(*ip))
                        as Box<dyn std::error::Error + Send + Sync>);
                }
            }
            // Port `0` here is intentionally discarded: `reqwest`'s `Resolve` docs guarantee an
            // explicit port in the URL (`execute` always builds one from `entry.port()`)
            // overrides whatever port a resolved `SocketAddr` carries.
            let resolved: reqwest::dns::Addrs =
                Box::new(addrs.into_iter().map(|ip| SocketAddr::new(ip, 0)));
            Ok(resolved)
        })
    }
}

/// Walks a `reqwest::Error`'s source chain looking for the [`IntraCellError`]
/// [`ValidatingResolver`] boxed — recovers the typed judgment (`MetadataOrLinkLocalAddress`,
/// `AddressNotInCell`, …) that the resolver rejected with, instead of flattening every
/// connect failure to [`IntraCellError::RequestFailed`]'s opaque string.
fn downcast_intra_cell_error(err: &reqwest::Error) -> Option<IntraCellError> {
    let mut source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(err);
    while let Some(e) = source {
        if let Some(ice) = e.downcast_ref::<IntraCellError>() {
            return Some(ice.clone());
        }
        source = e.source();
    }
    None
}

/// Real, network-backed [`IntraCellHttpTransport`].
pub struct HttpIntraCellTransport {
    registry: IntraCellResourceRegistry,
    client: reqwest::Client,
    max_response_bytes: usize,
}

impl HttpIntraCellTransport {
    pub fn new(
        registry: IntraCellResourceRegistry,
        request_timeout: Duration,
        max_response_bytes: usize,
    ) -> Result<Self, reqwest::Error> {
        Self::with_resolver(
            registry,
            request_timeout,
            max_response_bytes,
            Arc::new(SystemDnsResolve),
        )
    }

    /// Test/advanced-caller constructor with an injectable [`DnsResolve`] — wrapped in
    /// [`ValidatingResolver`] and installed as the client's *sole* DNS resolver (module doc:
    /// "single-resolver fix"), not merely consulted by a separate pre-check.
    pub fn with_resolver(
        registry: IntraCellResourceRegistry,
        request_timeout: Duration,
        max_response_bytes: usize,
        dns: Arc<dyn DnsResolve>,
    ) -> Result<Self, reqwest::Error> {
        // §83.4 判据3 minor fix: lowercased so a registry entry with any uppercase letter in
        // its hostname still matches `ValidatingResolver::resolve`'s lookup key, which is
        // itself lowercased (the same doc there).
        let by_host = registry
            .entries()
            .map(|e| (e.host().to_ascii_lowercase(), e.clone()))
            .collect();
        let resolver = Arc::new(ValidatingResolver { dns, by_host });
        // Layer 1B same-Cell resource access: never honor an env-configured HTTP(S) proxy — a
        // proxied request is resolved by the proxy, not by `resolver` above, which would
        // silently defeat the single-resolver DNS-rebinding fix this module exists for (see
        // `ClientConfig::trust_env_proxy`'s doc), and routing same-Cell traffic through an
        // external proxy is itself a §83.4 判据4 violation regardless.
        let client = build_client_with_resolver(
            ClientConfig {
                request_timeout,
                trust_env_proxy: false,
            },
            resolver,
        )?;
        Ok(Self {
            registry,
            client,
            max_response_bytes,
        })
    }
}

#[async_trait::async_trait]
impl IntraCellHttpTransport for HttpIntraCellTransport {
    async fn execute(
        &self,
        permit: &CellAccessPermit,
        request: IntraCellRequest,
    ) -> Result<IntraCellResponse, IntraCellError> {
        validate_path(&request.path)?;
        if permit.is_expired(std::time::Instant::now()) {
            return Err(IntraCellError::ExpiredPermit);
        }
        // 2026-08-30 ruling: enforced before any DNS resolution or dial, on the permit's own
        // mint-time access mode — a misconfigured registry entry cannot widen this back by the
        // time `execute` runs.
        if permit.access_mode() == CellAccessMode::QdrantReadOnly
            && !is_read_allowed(request.method, &request.path)
        {
            return Err(IntraCellError::WriteDenied);
        }
        let entry = self
            .registry
            .resolve(permit.resource())
            .ok_or(IntraCellError::UnregisteredResource)?;

        let scheme = if entry.tls() { "https" } else { "http" };
        let url = format!(
            "{scheme}://{}:{}{}",
            entry.host(),
            entry.port(),
            request.path
        );

        // §83.4 判据3 blocker fix: `ValidatingResolver` is a `reqwest::dns::Resolve` — it is
        // never consulted at all when the URL authority is (or WHATWG-normalizes to) an IP
        // literal, because `reqwest`/`hyper`'s connector parses and dials such an authority
        // directly. Every registry construction site in this tree today registers an IP
        // literal (`ResourceEntry::new("127.0.0.1", …)` in this module's own tests, this
        // crate's real callers), so this is not a corner case. `reqwest::Url::parse` runs the
        // identical WHATWG host-parsing/canonicalization `reqwest` itself applies before
        // dialing — matching on its `Host::Ipv4`/`Host::Ipv6` result catches every numeric
        // encoding uniformly (dotted-quad, dword, octal, hex, bracketed mapped-IPv6, …), not
        // just the plain-dotted-quad case a naive `str::parse::<IpAddr>()` pre-check would; a
        // `Host::Domain` here is the ordinary hostname path, left to `ValidatingResolver`
        // unconditionally when `send()` below actually resolves it.
        let parsed_url = url::Url::parse(&url)
            .map_err(|e| IntraCellError::RequestFailed(format!("invalid URL {url:?}: {e}")))?;
        if let Some(host) = parsed_url.host() {
            let literal_ip = match host {
                url::Host::Domain(_) => None,
                url::Host::Ipv4(v4) => Some(IpAddr::V4(v4)),
                url::Host::Ipv6(v6) => Some(IpAddr::V6(v6)),
            };
            if let Some(ip) = literal_ip {
                if is_metadata_or_link_local(ip) {
                    return Err(IntraCellError::MetadataOrLinkLocalAddress(ip));
                }
                if !entry.address_in_cell(ip) {
                    return Err(IntraCellError::AddressNotInCell(ip));
                }
            }
        }

        let mut builder = match request.method {
            IntraCellMethod::Get => self.client.get(&url),
            IntraCellMethod::Put => self.client.put(&url),
            IntraCellMethod::Post => self.client.post(&url),
            IntraCellMethod::Delete => self.client.delete(&url),
        };
        for (name, value) in &request.headers {
            builder = builder.header(name, value);
        }
        if let Some(body) = &request.json_body {
            builder = builder.json(body);
        }
        // §83.4 判据3 single-resolver fix: DNS resolution — and its metadata/link-local/CIDR
        // judgment — happens *inside* this `send()`, via `ValidatingResolver`, not before it.
        // dep: Qdrant(*) — outbound http call
        let mut response = match builder.send().await {
            Ok(r) => r,
            Err(e) => {
                return Err(downcast_intra_cell_error(&e)
                    .unwrap_or_else(|| IntraCellError::RequestFailed(e.to_string())));
            }
        };
        let status = response.status().as_u16();
        if (300..400).contains(&status) {
            return Err(IntraCellError::UnexpectedRedirect(status));
        }

        // Bounded read (§83.4, mirroring `HttpExternalCall::call`/Layer 1A): accumulate in
        // capped chunks instead of `response.bytes()`, which buffers the entire body regardless
        // of size before returning anything.
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| IntraCellError::RequestFailed(e.to_string()))?
        {
            if bytes.len() + chunk.len() > self.max_response_bytes {
                return Err(IntraCellError::ResponseTooLarge {
                    max: self.max_response_bytes,
                });
            }
            bytes.extend_from_slice(&chunk);
        }
        let json_body = if bytes.is_empty() {
            None
        } else {
            Some(
                serde_json::from_slice(&bytes)
                    .map_err(|e| IntraCellError::ResponseBodyInvalid(e.to_string()))?,
            )
        };
        Ok(IntraCellResponse { status, json_body })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permit::{CallerId, CellId, authorize_cell_access};
    use crate::resource::{IntraCellResource, ResourceEntry};
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::Ipv4Addr;
    use std::sync::Mutex;

    struct FixedResolve(Mutex<Vec<IpAddr>>);
    impl DnsResolve for FixedResolve {
        fn resolve(&self, _host: &str, _port: u16) -> Result<Vec<IpAddr>, IntraCellError> {
            Ok(self.0.lock().unwrap().clone())
        }
    }

    fn permit_for(registry: &IntraCellResourceRegistry) -> CellAccessPermit {
        authorize_cell_access(
            registry,
            IntraCellResource::QDRANT_REST,
            Duration::from_secs(30),
        )
        .unwrap()
    }

    // dep: Qdrant(*) — qdrant wire call
    fn plain_request(path: &str) -> IntraCellRequest {
        // dep: Qdrant(*) — qdrant wire call
        IntraCellRequest {
            method: IntraCellMethod::Get,
            path: path.to_string(),
            json_body: None,
            headers: Vec::new(),
        }
    }

    fn registry_allowing(cidr: &str, cell: CellId) -> IntraCellResourceRegistry {
        let mut entries = BTreeMap::new();
        entries.insert(
            IntraCellResource::QDRANT_REST,
            ResourceEntry::new(
                "qdrant.internal",
                6333,
                cell,
                vec![cidr.parse().unwrap()],
                BTreeSet::from([CallerId("retrieval-worker".to_string())]),
                true,
            )
            .unwrap(),
        );
        IntraCellResourceRegistry::new(entries, cell, CallerId("retrieval-worker".to_string()))
    }

    /// §83.4 判据3, unconditional hard block: even a registry entry whose CIDR set is a normal
    /// (non-public) private range must never let a resolved address of the cloud-metadata IP
    /// through — the block fires before the CIDR allowlist is even consulted, so it does not
    /// matter that the configured CIDR here does not itself contain the metadata address.
    #[tokio::test]
    async fn metadata_ip_is_rejected_even_with_a_normal_private_cidr_registered() {
        let cell = CellId(uuid::Uuid::now_v7());
        let registry = registry_allowing("10.0.0.0/8", cell);
        let permit = permit_for(&registry);
        let dns = FixedResolve(Mutex::new(vec![IpAddr::V4(Ipv4Addr::new(
            169, 254, 169, 254,
        ))]));
        let transport = HttpIntraCellTransport::with_resolver(
            registry,
            Duration::from_secs(1),
            DEFAULT_MAX_RESPONSE_BYTES,
            Arc::new(dns),
        )
        .unwrap();
        let result = transport
            .execute(&permit, plain_request("/collections"))
            .await;
        assert!(matches!(
            result,
            Err(IntraCellError::MetadataOrLinkLocalAddress(_))
        ));
    }

    /// §83.4 判据3: an address outside the resource's registered Cell CIDR (a different Cell,
    /// or the public internet) must be refused before any connection is attempted.
    #[tokio::test]
    async fn address_outside_cell_cidr_is_rejected() {
        let cell = CellId(uuid::Uuid::now_v7());
        let registry = registry_allowing("10.0.0.0/8", cell);
        let permit = permit_for(&registry);
        let dns = FixedResolve(Mutex::new(vec!["93.184.216.34".parse().unwrap()]));
        let transport = HttpIntraCellTransport::with_resolver(
            registry,
            Duration::from_secs(1),
            DEFAULT_MAX_RESPONSE_BYTES,
            Arc::new(dns),
        )
        .unwrap();
        let result = transport
            .execute(&permit, plain_request("/collections"))
            .await;
        assert!(matches!(result, Err(IntraCellError::AddressNotInCell(_))));
    }

    /// A resolver that panics if ever invoked — proves a check ran without the DNS resolver
    /// being consulted at all (shared by the two IP-literal-host tests below: an IP-literal
    /// authority must never reach [`ValidatingResolver`], `reqwest`'s connector dials it
    /// directly).
    struct PanicResolve;
    impl DnsResolve for PanicResolve {
        fn resolve(&self, _host: &str, _port: u16) -> Result<Vec<IpAddr>, IntraCellError> {
            panic!("an IP-literal host must never reach the DNS resolver");
        }
    }

    /// §83.4 判据3 blocker regression: a registry entry whose `host` is the cloud-metadata IP
    /// *literal* (not a hostname that resolves to it) must still be hard-blocked —
    /// `ValidatingResolver` is never consulted for an IP-literal authority (`reqwest`'s
    /// connector dials it directly), so this exercises `execute`'s own pre-`send()` check
    /// instead. [`PanicResolve`] proves the rejection does not come from the resolver path.
    #[tokio::test]
    async fn ip_literal_host_pointing_at_metadata_address_is_rejected_without_a_connection() {
        let cell = CellId(uuid::Uuid::now_v7());
        let mut entries = BTreeMap::new();
        entries.insert(
            IntraCellResource::QDRANT_REST,
            ResourceEntry::new(
                "169.254.169.254",
                80,
                cell,
                vec!["10.0.0.0/8".parse().unwrap()],
                BTreeSet::from([CallerId("retrieval-worker".to_string())]),
                false,
            )
            .unwrap(),
        );
        let registry =
            IntraCellResourceRegistry::new(entries, cell, CallerId("retrieval-worker".to_string()));
        let permit = permit_for(&registry);
        let transport = HttpIntraCellTransport::with_resolver(
            registry,
            Duration::from_secs(1),
            DEFAULT_MAX_RESPONSE_BYTES,
            Arc::new(PanicResolve),
        )
        .unwrap();
        let result = transport
            .execute(&permit, plain_request("/collections"))
            .await;
        assert!(matches!(
            result,
            Err(IntraCellError::MetadataOrLinkLocalAddress(_))
        ));
    }

    /// §83.4 判据3 blocker regression, decisive proof (reviewer repro): registry `host` is an
    /// IP literal (`127.0.0.1`-shaped) with a *real, live* listener behind it, registered CIDR
    /// deliberately does not cover that address, and the DNS resolver panics if ever called.
    /// Before this fix, `execute()` returned `Ok(200)` and the listener saw a real connection
    /// — `ValidatingResolver`'s CIDR judgment was never in the loop for an IP-literal host.
    #[tokio::test]
    async fn ip_literal_host_outside_registered_cidr_is_rejected_before_any_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_addr = listener.local_addr().unwrap();
        let saw_connection = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = saw_connection.clone();
        tokio::spawn(async move {
            if listener.accept().await.is_ok() {
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        });

        let cell = CellId(uuid::Uuid::now_v7());
        let mut entries = BTreeMap::new();
        entries.insert(
            IntraCellResource::QDRANT_REST,
            ResourceEntry::new(
                listener_addr.ip().to_string(),
                listener_addr.port(),
                cell,
                // Deliberately does not cover 127.0.0.1 — the point of this test.
                vec!["10.0.0.0/8".parse().unwrap()],
                BTreeSet::from([CallerId("retrieval-worker".to_string())]),
                false,
            )
            .unwrap(),
        );
        let registry =
            IntraCellResourceRegistry::new(entries, cell, CallerId("retrieval-worker".to_string()));
        let permit = permit_for(&registry);
        let transport = HttpIntraCellTransport::with_resolver(
            registry,
            Duration::from_secs(1),
            DEFAULT_MAX_RESPONSE_BYTES,
            Arc::new(PanicResolve),
        )
        .unwrap();

        let result = transport
            .execute(&permit, plain_request("/collections"))
            .await;
        assert!(matches!(result, Err(IntraCellError::AddressNotInCell(_))));

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !saw_connection.load(std::sync::atomic::Ordering::SeqCst),
            "the mismatched-CIDR IP-literal listener must see zero connections"
        );
    }

    #[tokio::test]
    async fn expired_permit_is_rejected_before_any_dns_lookup() {
        let cell = CellId(uuid::Uuid::now_v7());
        let registry = registry_allowing("10.0.0.0/8", cell);
        let permit = authorize_cell_access(
            &registry,
            IntraCellResource::QDRANT_REST,
            Duration::from_millis(0),
        )
        .unwrap();
        std::thread::sleep(Duration::from_millis(1));
        // A resolver that would panic if ever called — proves the expiry check runs first.
        struct PanicResolve;
        impl DnsResolve for PanicResolve {
            fn resolve(&self, _host: &str, _port: u16) -> Result<Vec<IpAddr>, IntraCellError> {
                panic!("must not be reached for an expired permit");
            }
        }
        let transport = HttpIntraCellTransport::with_resolver(
            registry,
            Duration::from_secs(1),
            DEFAULT_MAX_RESPONSE_BYTES,
            Arc::new(PanicResolve),
        )
        .unwrap();
        let result = transport
            .execute(&permit, plain_request("/collections"))
            .await;
        assert_eq!(result, Err(IntraCellError::ExpiredPermit));
    }

    #[test]
    fn validate_path_accepts_ordinary_relative_paths() {
        assert!(validate_path("/collections").is_ok());
        assert!(validate_path("/collections/foo/points?wait=true").is_ok());
    }

    #[test]
    fn validate_path_rejects_authority_rewrite_shapes() {
        assert!(validate_path("@evil.example.com/steal").is_err());
        assert!(validate_path("collections").is_err()); // no leading '/'
        assert!(validate_path("//evil.example.com/steal").is_err());
        assert!(validate_path("/../secret").is_err());
        assert!(validate_path("/collections/../../secret").is_err());
        assert!(validate_path("/collections\r\nHost: evil").is_err());
    }

    /// §83.4 判据1 minor regression: percent-encoded `..` segments (`%2e%2e`, `.%2e`, `%2e.`,
    /// mixed-case `%2E`) must be rejected exactly like a literal `..` — the URL parser
    /// `execute`'s `send()` ultimately hands `path` to collapses them identically.
    #[test]
    fn validate_path_rejects_percent_encoded_dot_dot_segments() {
        assert!(validate_path("/collections/%2e%2e/%2e%2e/admin").is_err());
        assert!(validate_path("/collections/.%2e/admin").is_err());
        assert!(validate_path("/collections/%2e./admin").is_err());
        assert!(validate_path("/collections/%2E%2E/admin").is_err());
    }

    /// §83.4 判据1 regression, decisive proof: two *real* `TcpListener`s — a `path` that
    /// rewrites the URL authority (`@{second listener's address}/pwned`) must land the
    /// connection on neither listener via the registered host (there is none reachable at
    /// `qdrant.internal`) nor succeed at all; specifically the declared/registered listener
    /// must see zero connections and `execute` must reject the request before ever building a
    /// URL that `reqwest` would dial.
    #[tokio::test]
    async fn path_that_rewrites_the_url_authority_is_rejected_before_any_connection() {
        let declared = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let declared_addr = declared.local_addr().unwrap();
        let declared_saw = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let declared_flag = declared_saw.clone();
        tokio::spawn(async move {
            if declared.accept().await.is_ok() {
                declared_flag.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        });

        let second = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_addr = second.local_addr().unwrap();
        let second_saw = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let second_flag = second_saw.clone();
        tokio::spawn(async move {
            if second.accept().await.is_ok() {
                second_flag.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        });

        let cell = CellId(uuid::Uuid::now_v7());
        let mut entries = BTreeMap::new();
        entries.insert(
            IntraCellResource::QDRANT_REST,
            ResourceEntry::new(
                declared_addr.ip().to_string(),
                declared_addr.port(),
                cell,
                vec!["127.0.0.0/8".parse().unwrap()],
                BTreeSet::from([CallerId("retrieval-worker".to_string())]),
                false,
            )
            .unwrap(),
        );
        let registry =
            IntraCellResourceRegistry::new(entries, cell, CallerId("retrieval-worker".to_string()));
        let permit = permit_for(&registry);
        let transport = HttpIntraCellTransport::new(
            registry,
            Duration::from_secs(1),
            DEFAULT_MAX_RESPONSE_BYTES,
        )
        .unwrap();

        let malicious_path = format!("@{second_addr}/pwned");
        let result = transport
            .execute(&permit, plain_request(&malicious_path))
            .await;
        assert!(matches!(result, Err(IntraCellError::InvalidPath(_))));

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !declared_saw.load(std::sync::atomic::Ordering::SeqCst),
            "the declared/registered listener must see zero connections"
        );
        assert!(
            !second_saw.load(std::sync::atomic::Ordering::SeqCst),
            "the path-injected second listener must see zero connections"
        );
    }

    /// §83.4 判据3: a 3xx from the (correctly DNS/CIDR-validated) destination must not be
    /// followed — `build_client`'s disabled redirect policy means `reqwest` never dials the
    /// `Location` target on its own; this asserts `execute` also surfaces it as an explicit
    /// error rather than returning the bare 3xx `IntraCellResponse` as if it were ordinary.
    #[tokio::test]
    async fn redirect_response_is_surfaced_as_an_explicit_error() {
        let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin_addr = origin.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut socket, _) = origin.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let response = "HTTP/1.1 302 Found\r\nLocation: http://elsewhere/x\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });

        let cell = CellId(uuid::Uuid::now_v7());
        let mut entries = BTreeMap::new();
        entries.insert(
            IntraCellResource::QDRANT_REST,
            ResourceEntry::new(
                origin_addr.ip().to_string(),
                origin_addr.port(),
                cell,
                vec!["127.0.0.0/8".parse().unwrap()],
                BTreeSet::from([CallerId("retrieval-worker".to_string())]),
                false,
            )
            .unwrap(),
        );
        let registry =
            IntraCellResourceRegistry::new(entries, cell, CallerId("retrieval-worker".to_string()));
        let permit = permit_for(&registry);
        let transport = HttpIntraCellTransport::new(
            registry,
            Duration::from_secs(1),
            DEFAULT_MAX_RESPONSE_BYTES,
        )
        .unwrap();

        let result = transport
            .execute(&permit, plain_request("/collections"))
            .await;
        assert_eq!(result, Err(IntraCellError::UnexpectedRedirect(302)));
    }

    /// A response larger than the configured cap must be rejected rather than buffered whole.
    #[tokio::test]
    async fn oversized_response_is_rejected_not_buffered() {
        let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin_addr = origin.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut socket, _) = origin.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let oversized_body = "x".repeat(64 * 1024);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                oversized_body.len(),
                oversized_body
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });

        let cell = CellId(uuid::Uuid::now_v7());
        let mut entries = BTreeMap::new();
        entries.insert(
            IntraCellResource::QDRANT_REST,
            ResourceEntry::new(
                origin_addr.ip().to_string(),
                origin_addr.port(),
                cell,
                vec!["127.0.0.0/8".parse().unwrap()],
                BTreeSet::from([CallerId("retrieval-worker".to_string())]),
                false,
            )
            .unwrap(),
        );
        let registry =
            IntraCellResourceRegistry::new(entries, cell, CallerId("retrieval-worker".to_string()));
        let permit = permit_for(&registry);
        let transport =
            HttpIntraCellTransport::new(registry, Duration::from_secs(1), 1024).unwrap();

        let result = transport
            .execute(&permit, plain_request("/collections"))
            .await;
        assert_eq!(result, Err(IntraCellError::ResponseTooLarge { max: 1024 }));
    }

    /// A resolver that hands out one queued response per call, panicking if exhausted — models
    /// a hostname whose resolution changes between successive lookups (DNS rebinding shape).
    struct SequencedResolve(Mutex<std::collections::VecDeque<Vec<IpAddr>>>);
    impl DnsResolve for SequencedResolve {
        fn resolve(&self, _host: &str, _port: u16) -> Result<Vec<IpAddr>, IntraCellError> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .pop_front()
                .expect("resolver exhausted — test called execute() more times than queued"))
        }
    }

    /// §83.4 判据3, single-resolver fix regression (ADR-0003 second round, OWASP SSRF Cheat
    /// Sheet's DNS rebinding shape): the *same* resolver instance backs every `execute()` call
    /// on this transport (it was installed once, at construction, as the client's sole DNS
    /// resolver) — there is no separate "validate now, trust later" pre-check whose result a
    /// later, differently-resolving lookup could bypass. A resolver that answers safely on its
    /// first call and with the cloud-metadata address on its second must let the first
    /// `execute()` succeed and must fail the second — proving every single connection attempt
    /// is independently re-validated by the one resolver in the loop, never cached as trusted.
    #[tokio::test]
    async fn rebinding_between_successive_calls_is_rejected_by_the_same_resolver() {
        let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin_addr = origin.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            loop {
                let Ok((mut socket, _)) = origin.accept().await else {
                    return;
                };
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });

        let cell = CellId(uuid::Uuid::now_v7());
        // The registry's own declared host is a name (not the literal listener IP) so both
        // queued resolver answers are genuinely "what this hostname resolved to this time",
        // matching a real rebinding shape rather than a registry that already pins an IP.
        let mut entries = BTreeMap::new();
        entries.insert(
            IntraCellResource::QDRANT_REST,
            ResourceEntry::new(
                "qdrant.internal",
                origin_addr.port(),
                cell,
                vec!["127.0.0.0/8".parse().unwrap()],
                BTreeSet::from([CallerId("retrieval-worker".to_string())]),
                false,
            )
            .unwrap(),
        );
        let registry =
            IntraCellResourceRegistry::new(entries, cell, CallerId("retrieval-worker".to_string()));
        let permit = permit_for(&registry);
        let dns = SequencedResolve(Mutex::new(std::collections::VecDeque::from([
            vec![origin_addr.ip()],
            vec![IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254))],
        ])));
        let transport = HttpIntraCellTransport::with_resolver(
            registry,
            Duration::from_secs(1),
            DEFAULT_MAX_RESPONSE_BYTES,
            Arc::new(dns),
        )
        .unwrap();

        let first = transport
            .execute(&permit, plain_request("/collections"))
            .await;
        assert_eq!(first.map(|r| r.status), Ok(200));

        let second = transport
            .execute(&permit, plain_request("/collections"))
            .await;
        assert!(matches!(
            second,
            Err(IntraCellError::MetadataOrLinkLocalAddress(_))
        ));
    }

    /// Positive control for the test above: a resolver that answers the same safe address on
    /// every call must let every call succeed — the fix does not turn re-resolution itself
    /// into a failure mode, only a *changed* answer that lands outside the Cell.
    #[tokio::test]
    async fn stable_resolution_to_a_cell_address_succeeds_on_every_call() {
        let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin_addr = origin.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            loop {
                let Ok((mut socket, _)) = origin.accept().await else {
                    return;
                };
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });

        let cell = CellId(uuid::Uuid::now_v7());
        let mut entries = BTreeMap::new();
        entries.insert(
            IntraCellResource::QDRANT_REST,
            ResourceEntry::new(
                "qdrant.internal",
                origin_addr.port(),
                cell,
                vec!["127.0.0.0/8".parse().unwrap()],
                BTreeSet::from([CallerId("retrieval-worker".to_string())]),
                false,
            )
            .unwrap(),
        );
        let registry =
            IntraCellResourceRegistry::new(entries, cell, CallerId("retrieval-worker".to_string()));
        let permit = permit_for(&registry);
        let dns = SequencedResolve(Mutex::new(std::collections::VecDeque::from([
            vec![origin_addr.ip()],
            vec![origin_addr.ip()],
        ])));
        let transport = HttpIntraCellTransport::with_resolver(
            registry,
            Duration::from_secs(1),
            DEFAULT_MAX_RESPONSE_BYTES,
            Arc::new(dns),
        )
        .unwrap();

        for _ in 0..2 {
            let result = transport
                .execute(&permit, plain_request("/collections"))
                .await;
            assert_eq!(result.map(|r| r.status), Ok(200));
        }
    }

    /// §83.4 判据3 minor regression: a registry entry whose hostname carries any uppercase
    /// letter must not silently fail every call with `UnregisteredResource` — `url::Url::parse`
    /// lowercases the authority before `ValidatingResolver` ever sees it, so `by_host`'s keys
    /// must be lowercased the same way at construction time.
    #[tokio::test]
    async fn registry_host_with_uppercase_letters_still_resolves() {
        let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin_addr = origin.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut socket, _) = origin.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });

        let cell = CellId(uuid::Uuid::now_v7());
        let mut entries = BTreeMap::new();
        entries.insert(
            IntraCellResource::QDRANT_REST,
            ResourceEntry::new(
                "Qdrant.Internal",
                origin_addr.port(),
                cell,
                vec!["127.0.0.0/8".parse().unwrap()],
                BTreeSet::from([CallerId("retrieval-worker".to_string())]),
                false,
            )
            .unwrap(),
        );
        let registry =
            IntraCellResourceRegistry::new(entries, cell, CallerId("retrieval-worker".to_string()));
        let permit = permit_for(&registry);
        let dns = FixedResolve(Mutex::new(vec![origin_addr.ip()]));
        let transport = HttpIntraCellTransport::with_resolver(
            registry,
            Duration::from_secs(1),
            DEFAULT_MAX_RESPONSE_BYTES,
            Arc::new(dns),
        )
        .unwrap();

        let result = transport
            .execute(&permit, plain_request("/collections"))
            .await;
        assert_eq!(result.map(|r| r.status), Ok(200));
    }

    /// A `QdrantReadOnly` registry entry, wired up like [`registry_allowing`] but with the
    /// stricter access mode a permit must carry through to `execute`'s allowlist check.
    fn registry_read_only(cidr: &str, cell: CellId) -> IntraCellResourceRegistry {
        let mut entries = BTreeMap::new();
        entries.insert(
            IntraCellResource::QDRANT_REST,
            ResourceEntry::new(
                "qdrant.internal",
                6333,
                cell,
                vec![cidr.parse().unwrap()],
                BTreeSet::from([CallerId("retrieval-worker".to_string())]),
                true,
            )
            .unwrap()
            .with_access_mode(CellAccessMode::QdrantReadOnly),
        );
        IntraCellResourceRegistry::new(entries, cell, CallerId("retrieval-worker".to_string()))
    }

    // dep: Qdrant(*) — qdrant wire call
    fn request(method: IntraCellMethod, path: &str) -> IntraCellRequest {
        // dep: Qdrant(*) — qdrant wire call
        IntraCellRequest {
            method,
            path: path.to_string(),
            json_body: None,
            headers: Vec::new(),
        }
    }

    /// 2026-08-30 ruling unit coverage: `GET` on any path is always admitted under
    /// `QdrantReadOnly`, checked before any DNS resolution happens (a resolver that always
    /// errors proves the allowlist decision is not what let this call proceed further than it
    /// should — a `WriteDenied` or a `DnsResolutionFailed` are the only two possible outcomes,
    /// and this asserts it is never the latter for `GET`).
    #[tokio::test]
    async fn read_only_permit_admits_get_on_any_path() {
        let cell = CellId(uuid::Uuid::now_v7());
        let registry = registry_read_only("10.0.0.0/8", cell);
        let permit = permit_for(&registry);
        struct AlwaysErr;
        impl DnsResolve for AlwaysErr {
            fn resolve(&self, host: &str, _port: u16) -> Result<Vec<IpAddr>, IntraCellError> {
                Err(IntraCellError::DnsResolutionFailed(host.to_string()))
            }
        }
        let transport = HttpIntraCellTransport::with_resolver(
            registry,
            Duration::from_secs(1),
            DEFAULT_MAX_RESPONSE_BYTES,
            Arc::new(AlwaysErr),
        )
        .unwrap();
        let result = transport
            .execute(&permit, request(IntraCellMethod::Get, "/collections/x"))
            .await;
        assert!(!matches!(result, Err(IntraCellError::WriteDenied)));
    }

    /// A `POST` to a read-shaped Qdrant endpoint (`/points/search`) is admitted under
    /// `QdrantReadOnly` — proven the same way as the `GET` case above (denial would surface as
    /// `WriteDenied`, never a DNS error, since the DNS resolver here always fails).
    #[tokio::test]
    async fn read_only_permit_admits_post_points_search() {
        let cell = CellId(uuid::Uuid::now_v7());
        let registry = registry_read_only("10.0.0.0/8", cell);
        let permit = permit_for(&registry);
        struct AlwaysErr;
        impl DnsResolve for AlwaysErr {
            fn resolve(&self, host: &str, _port: u16) -> Result<Vec<IpAddr>, IntraCellError> {
                Err(IntraCellError::DnsResolutionFailed(host.to_string()))
            }
        }
        let transport = HttpIntraCellTransport::with_resolver(
            registry,
            Duration::from_secs(1),
            DEFAULT_MAX_RESPONSE_BYTES,
            Arc::new(AlwaysErr),
        )
        .unwrap();
        let result = transport
            .execute(
                &permit,
                request(IntraCellMethod::Post, "/collections/x/points/search"),
            )
            .await;
        assert!(!matches!(result, Err(IntraCellError::WriteDenied)));
    }

    /// Every real Qdrant read `recall.rs` issues carries a query string (e.g.
    /// `ha_profile_for(QdrantOperation::ReadYourWriteStrict)` appends `?consistency=quorum`
    /// via `qdrant_path`) — a raw-string `ends_with` match against the allowlisted suffixes
    /// would deny 100% of production reads. This is the regression test for that hole.
    #[tokio::test]
    async fn read_only_permit_admits_post_points_query_with_consistency_param() {
        let cell = CellId(uuid::Uuid::now_v7());
        let registry = registry_read_only("10.0.0.0/8", cell);
        let permit = permit_for(&registry);
        struct AlwaysErr;
        impl DnsResolve for AlwaysErr {
            fn resolve(&self, host: &str, _port: u16) -> Result<Vec<IpAddr>, IntraCellError> {
                Err(IntraCellError::DnsResolutionFailed(host.to_string()))
            }
        }
        let transport = HttpIntraCellTransport::with_resolver(
            registry,
            Duration::from_secs(1),
            DEFAULT_MAX_RESPONSE_BYTES,
            Arc::new(AlwaysErr),
        )
        .unwrap();
        let result = transport
            .execute(
                &permit,
                request(
                    IntraCellMethod::Post,
                    "/collections/x/points/query?consistency=quorum",
                ),
            )
            .await;
        assert!(!matches!(result, Err(IntraCellError::WriteDenied)));
    }

    /// A mutating endpoint must not be able to smuggle an allowlisted suffix into its query
    /// string to bypass the `ends_with` check (e.g. `/points/delete?zz=/points/search`).
    #[tokio::test]
    async fn read_only_permit_denies_mutating_endpoint_with_allowlisted_suffix_in_query_string() {
        let cell = CellId(uuid::Uuid::now_v7());
        let registry = registry_read_only("10.0.0.0/8", cell);
        let permit = permit_for(&registry);
        let transport = HttpIntraCellTransport::new(
            registry,
            Duration::from_secs(1),
            DEFAULT_MAX_RESPONSE_BYTES,
        )
        .unwrap();
        let result = transport
            .execute(
                &permit,
                request(
                    IntraCellMethod::Post,
                    "/collections/x/points/delete?zz=/points/search",
                ),
            )
            .await;
        assert_eq!(result, Err(IntraCellError::WriteDenied));
    }

    #[tokio::test]
    async fn read_only_permit_denies_put_points() {
        let cell = CellId(uuid::Uuid::now_v7());
        let registry = registry_read_only("10.0.0.0/8", cell);
        let permit = permit_for(&registry);
        let transport = HttpIntraCellTransport::new(
            registry,
            Duration::from_secs(1),
            DEFAULT_MAX_RESPONSE_BYTES,
        )
        .unwrap();
        let result = transport
            .execute(
                &permit,
                request(IntraCellMethod::Put, "/collections/x/points"),
            )
            .await;
        assert_eq!(result, Err(IntraCellError::WriteDenied));
    }

    #[tokio::test]
    async fn read_only_permit_denies_delete() {
        let cell = CellId(uuid::Uuid::now_v7());
        let registry = registry_read_only("10.0.0.0/8", cell);
        let permit = permit_for(&registry);
        let transport = HttpIntraCellTransport::new(
            registry,
            Duration::from_secs(1),
            DEFAULT_MAX_RESPONSE_BYTES,
        )
        .unwrap();
        let result = transport
            .execute(&permit, request(IntraCellMethod::Delete, "/collections/x"))
            .await;
        assert_eq!(result, Err(IntraCellError::WriteDenied));
    }

    #[tokio::test]
    async fn read_only_permit_denies_post_to_a_mutating_endpoint() {
        let cell = CellId(uuid::Uuid::now_v7());
        let registry = registry_read_only("10.0.0.0/8", cell);
        let permit = permit_for(&registry);
        let transport = HttpIntraCellTransport::new(
            registry,
            Duration::from_secs(1),
            DEFAULT_MAX_RESPONSE_BYTES,
        )
        .unwrap();
        let result = transport
            .execute(
                &permit,
                request(IntraCellMethod::Post, "/collections/x/points"),
            )
            .await;
        assert_eq!(result, Err(IntraCellError::WriteDenied));
    }
}
