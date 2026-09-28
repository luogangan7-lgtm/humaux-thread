//! `protocol::edge` — H1: §73 Edge/Trusted-Proxy/IP Policy/ClientNetworkIdentity/API key/session/CSRF 中间件类型 （Phase 2
//!   wave 实现；判据出处见 spec 家章）。
//! Depends-on: crates=[hmac, sha2]; services=[]; env=[CARGO_MANIFEST_DIR]; modules=[]
//! Called-by: [gateway::auth, gateway::bootstrap, gateway::guard, tests, xtask::e2e_seed]
//! Invariants: [no runtime env or service read; CARGO_MANIFEST_DIR is build-time (`env!`) in the unit tests only, to embed migration 0037 for the contract checks]
//! Spec: §73; §73.2
//!
//! Every item here is a pure function or a plain value type: no HTTP framework, no DB, no
//! clock/RNG access baked in. Callers (the not-yet-wired `bins/gateway`) supply the raw
//! ingredients — TCP peer address, a raw header string, "now", freshly generated random
//! bytes — and get back a constructed value or a decision. Keeping I/O out of this module is
//! what makes the fault-injection tests below possible without a live socket or a clock mock.

use std::net::IpAddr;
use std::str::FromStr;
use std::time::SystemTime;

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

// ============================================================================
// §73.2 Trusted Proxy / Real IP — Cidr + ClientNetworkIdentity construction
// ============================================================================

/// One CIDR block, either address family (§73.2 `trusted_proxy_cidrs[]` / IP policy lists /
/// API credential `allowed_cidrs`— one shape serves all three, spec draws no distinction).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cidr {
    network: IpAddr,
    prefix_len: u8,
}

/// [`Cidr::from_str`] failure — malformed literal, not a runtime condition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CidrParseError(pub String);

impl std::fmt::Display for CidrParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid CIDR literal: {}", self.0)
    }
}
impl std::error::Error for CidrParseError {}

impl FromStr for Cidr {
    type Err = CidrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (addr_s, len_s) = s
            .split_once('/')
            .ok_or_else(|| CidrParseError(s.to_string()))?;
        // §73.2: canonicalize before it becomes a CIDR/rate-limit key — an IPv4-mapped IPv6
        // literal in config ("::ffff:10.0.0.0/104") collapses to the plain IPv4 form so it
        // compares correctly against a canonicalized peer address in `contains`.
        let network: IpAddr = addr_s
            .parse::<IpAddr>()
            .map_err(|_| CidrParseError(s.to_string()))?
            .to_canonical();
        let max_len: u8 = match network {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        let prefix_len: u8 = len_s.parse().map_err(|_| CidrParseError(s.to_string()))?;
        if prefix_len > max_len {
            return Err(CidrParseError(s.to_string()));
        }
        Ok(Cidr {
            network,
            prefix_len,
        })
    }
}

impl Cidr {
    /// Whether `ip` (canonicalized first, §73.2) falls inside this block. Cross-family
    /// comparisons (an IPv6-only address against a v4 block) never match — canonicalization
    /// already folded every IPv4-mapped v6 address into its v4 form, so a real mismatch here
    /// means the families are genuinely different.
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
}

/// `u32::MAX << n` panics at `n == 32`; the `/0` block ("match everything") is the one case
/// that needs the all-zero mask directly.
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

/// §73.2 `trusted_proxy_cidrs[]` / `max_forwarded_hops` — supplied by the caller's resolved
/// config (§78.1: no business threshold hardcoded in this module). Which header to read
/// (`X-Forwarded-For` vs `Forwarded`) is the caller's own extraction concern — nothing in
/// this crate is wired to a live header collection yet (`bins/gateway` is not built), so a
/// `trusted_ip_header` field here would be decorative; add it back when the gateway wiring
/// needs to pick the header itself rather than being handed an already-extracted value.
#[derive(Debug, Clone)]
pub struct TrustedProxyConfig {
    pub trusted_proxy_cidrs: Vec<Cidr>,
    pub max_forwarded_hops: usize,
}

/// §73.2 `ClientNetworkIdentity { peer_ip, client_ip, proxy_chain, asn?, country?, risk_tags[] }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientNetworkIdentity {
    pub peer_ip: IpAddr,
    pub client_ip: IpAddr,
    pub proxy_chain: Vec<IpAddr>,
    pub asn: Option<u32>,
    pub country: Option<String>,
    pub risk_tags: Vec<String>,
}

/// A forwarded-header value arrived from a TCP peer outside `trusted_proxy_cidrs` — §73.2's
/// forgery case. The header is discarded entirely, not partially trusted.
pub const RISK_TAG_FORWARDED_HEADER_IGNORED_UNTRUSTED_PEER: &str =
    "FORWARDED_HEADER_IGNORED_UNTRUSTED_PEER";
/// Forwarded-header hop count exceeded `max_forwarded_hops` — treated as suspect, chain
/// discarded, falls back to the peer address.
pub const RISK_TAG_FORWARDED_HOP_LIMIT_EXCEEDED: &str = "FORWARDED_HOP_LIMIT_EXCEEDED";
/// At least one comma-separated entry in the forwarded header did not parse as an IP address.
pub const RISK_TAG_FORWARDED_HEADER_UNPARSEABLE: &str = "FORWARDED_HEADER_UNPARSEABLE";

/// §73.2: "只有请求 TCP peer 属于可信代理 CIDR 时才解析 forwarded header" — this is the sole
/// constructor for [`ClientNetworkIdentity`]. `asn`/`country` are accepted as already-resolved
/// inputs (a GeoIP/ASN lookup is I/O, out of scope for a pure function) — pass `None` if the
/// caller has no such enrichment.
pub fn build_client_network_identity(
    peer_ip: IpAddr,
    forwarded_header_value: Option<&str>,
    config: &TrustedProxyConfig,
    asn: Option<u32>,
    country: Option<String>,
) -> ClientNetworkIdentity {
    let peer_ip = peer_ip.to_canonical();
    let peer_is_trusted = config
        .trusted_proxy_cidrs
        .iter()
        .any(|c| c.contains(peer_ip));

    let peer_only = |risk_tags: Vec<String>| ClientNetworkIdentity {
        peer_ip,
        client_ip: peer_ip,
        proxy_chain: Vec::new(),
        asn,
        country: country.clone(),
        risk_tags,
    };

    let Some(raw) = forwarded_header_value.filter(|s| !s.trim().is_empty()) else {
        return peer_only(Vec::new());
    };

    if !peer_is_trusted {
        return peer_only(vec![
            RISK_TAG_FORWARDED_HEADER_IGNORED_UNTRUSTED_PEER.to_string(),
        ]);
    }

    let mut risk_tags = Vec::new();
    let mut hops: Vec<IpAddr> = Vec::new();
    let mut saw_unparseable = false;
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        match part.parse::<IpAddr>() {
            Ok(ip) => hops.push(ip.to_canonical()),
            Err(_) => saw_unparseable = true,
        }
    }
    if saw_unparseable {
        risk_tags.push(RISK_TAG_FORWARDED_HEADER_UNPARSEABLE.to_string());
    }
    if hops.len() > config.max_forwarded_hops {
        // §73.3: tagged as suspect, but NOT discarded wholesale — a client can pad the
        // chain to force this branch on demand, and if the whole chain were dropped in
        // favor of `peer_ip` here, that client would be attributing every request to the
        // trusted proxy's own address (bypassing IP-policy denylists and collapsing every
        // such client onto one shared rate-limit key). The walk below still finds the real
        // client from the padded data.
        risk_tags.push(RISK_TAG_FORWARDED_HOP_LIMIT_EXCEEDED.to_string());
    }
    if hops.is_empty() {
        return peer_only(risk_tags);
    }
    // §73.2/§73.3: a real proxy APPENDS the address it received the request from — it does
    // not remove what the client sent. So the leftmost entry is exactly the part of the
    // chain the client itself controls. Walk right-to-left, skipping entries that are
    // themselves trusted-proxy addresses (chained trusted hops); the first non-trusted
    // entry from the right is the one no trusted proxy could have fabricated — that is
    // `client_ip`. Everything to its right is the (trusted) proxy chain.
    let split_at = hops
        .iter()
        .rposition(|ip| !config.trusted_proxy_cidrs.iter().any(|c| c.contains(*ip)));
    let Some(idx) = split_at else {
        // Every hop is itself a trusted-proxy address — none of them can be the real
        // client. Fall back to the TCP peer, which genuinely is a trusted proxy here.
        return peer_only(risk_tags);
    };
    ClientNetworkIdentity {
        peer_ip,
        client_ip: hops[idx],
        proxy_chain: hops[idx + 1..].to_vec(),
        asn,
        country,
        risk_tags,
    }
}

// ============================================================================
// §73.3 IP Policy — fixed priority chain
// ============================================================================

/// Which tier of the §73.3 chain produced a deny.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpPolicyDenyReason {
    GlobalDenylist,
    TenantExplicitDeny,
    TenantAllowlistRequired,
    CredentialCidrBinding,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IpPolicyDecision {
    Deny(IpPolicyDenyReason),
    /// §73.3: "IP 只能是风险/访问控制信号之一，不能替代用户身份认证" — an allow always
    /// carries risk tags rather than the risk tier itself ever denying.
    Allow {
        risk_tags: Vec<String>,
    },
}

/// One evaluation's inputs. Empty allowlist slices mean "no allowlist requirement configured"
/// (§73.3's "Tenant allowlist requirement" tier only fires when the tenant actually has one).
pub struct IpPolicyInput<'a> {
    pub ip: IpAddr,
    pub global_denylist: &'a [Cidr],
    pub global_emergency_allowlist: &'a [Cidr],
    pub tenant_denylist: &'a [Cidr],
    pub tenant_allowlist: &'a [Cidr],
    pub credential_allowed_cidrs: &'a [Cidr],
    /// Pre-computed by the caller (region/ASN lookup is I/O); tagged onto an `Allow`, never
    /// used to deny.
    pub region_asn_risk_tags: &'a [String],
}

/// §73.3 priority chain, spec verbatim: "Emergency deny > Administrative network policy >
/// Tenant explicit deny > Tenant allowlist requirement > Risk policy > allow". `global_denylist`
/// IS the "Emergency deny" tier; `global_emergency_allowlist` is "Administrative network
/// policy" — an ops override that beats a tenant's OWN policy below it but never beats the
/// global denylist above it (an IP on both lists stays denied). Admin API allowlist (§73.4) is
/// a separate hostname/route-scoped policy, evaluated by the Admin Plane's own layer, not
/// folded into this per-request chain.
pub fn evaluate_ip_policy(input: &IpPolicyInput<'_>) -> IpPolicyDecision {
    let ip = input.ip.to_canonical();
    let in_list = |list: &[Cidr]| list.iter().any(|c| c.contains(ip));

    if in_list(input.global_denylist) {
        return IpPolicyDecision::Deny(IpPolicyDenyReason::GlobalDenylist);
    }
    if in_list(input.global_emergency_allowlist) {
        return IpPolicyDecision::Allow {
            risk_tags: Vec::new(),
        };
    }
    if in_list(input.tenant_denylist) {
        return IpPolicyDecision::Deny(IpPolicyDenyReason::TenantExplicitDeny);
    }
    if !input.tenant_allowlist.is_empty() && !in_list(input.tenant_allowlist) {
        return IpPolicyDecision::Deny(IpPolicyDenyReason::TenantAllowlistRequired);
    }
    if !input.credential_allowed_cidrs.is_empty() && !in_list(input.credential_allowed_cidrs) {
        return IpPolicyDecision::Deny(IpPolicyDenyReason::CredentialCidrBinding);
    }
    IpPolicyDecision::Allow {
        risk_tags: input.region_asn_risk_tags.to_vec(),
    }
}

// ============================================================================
// §73.5 API Key / MCP Credential
// ============================================================================

/// §73.5 lifecycle: `CREATE -> ACTIVE -> ROTATING -> REVOKED/EXPIRED`. Wire form matches the
/// `control.api_keys.status` CHECK constraint (migration 0035) — [`ApiKeyStatus::as_db_str`]
/// / [`ApiKeyStatus::from_db_str`] are the only conversion points, kept next to the enum so
/// the two never drift independently. [`ApiKeyStatus::ALL`] + `contract_tests` below hold
/// this against the migration's literal CHECK list (§78.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKeyStatus {
    Create,
    Active,
    Rotating,
    Revoked,
    Expired,
}

impl ApiKeyStatus {
    /// Every variant, in the same order as `control.api_keys.status`'s CHECK list —
    /// order matters, [`contract_tests`] diffs this positionally against the migration.
    pub const ALL: [ApiKeyStatus; 5] = [
        ApiKeyStatus::Create,
        ApiKeyStatus::Active,
        ApiKeyStatus::Rotating,
        ApiKeyStatus::Revoked,
        ApiKeyStatus::Expired,
    ];

    pub const fn as_db_str(self) -> &'static str {
        match self {
            ApiKeyStatus::Create => "CREATE",
            ApiKeyStatus::Active => "ACTIVE",
            ApiKeyStatus::Rotating => "ROTATING",
            ApiKeyStatus::Revoked => "REVOKED",
            ApiKeyStatus::Expired => "EXPIRED",
        }
    }

    pub fn from_db_str(s: &str) -> Option<Self> {
        match s {
            "CREATE" => Some(ApiKeyStatus::Create),
            "ACTIVE" => Some(ApiKeyStatus::Active),
            "ROTATING" => Some(ApiKeyStatus::Rotating),
            "REVOKED" => Some(ApiKeyStatus::Revoked),
            "EXPIRED" => Some(ApiKeyStatus::Expired),
            _ => None,
        }
    }
}

/// §73.5 lifecycle edges. `Active` may skip `Rotating` straight to `Revoked`/`Expired`
/// (rotation is optional); `Rotating` only ever concludes the *old* key via `Revoked`/`Expired`
/// once its overlap window ends — it never goes back to `Active`.
pub fn is_legal_api_key_transition(from: ApiKeyStatus, to: ApiKeyStatus) -> bool {
    use ApiKeyStatus::*;
    matches!(
        (from, to),
        (Create, Active)
            | (Active, Rotating)
            | (Active, Revoked)
            | (Active, Expired)
            | (Rotating, Revoked)
            | (Rotating, Expired)
    )
}

/// §73.5 keyed hash: `HMAC-SHA256(pepper, raw_key)`. A random-entropy API key (unlike a
/// user-chosen password, §74.3) does not need Argon2's deliberately-slow KDF — a fast keyed
/// hash keyed by a server-side pepper is the standard shape for this credential class, and
/// `hmac::Mac::verify_slice` gives constant-time comparison for free (no hand-rolled
/// timing-safe compare here — that would be reinventing a primitive the crate already owns).
pub fn compute_api_key_hash(pepper: &[u8], raw_key: &str) -> Vec<u8> {
    let mut mac =
        <HmacSha256 as KeyInit>::new_from_slice(pepper).expect("HMAC accepts any key length");
    mac.update(raw_key.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

fn verify_api_key_hash(pepper: &[u8], raw_key: &str, stored_hash: &[u8]) -> bool {
    let mut mac =
        <HmacSha256 as KeyInit>::new_from_slice(pepper).expect("HMAC accepts any key length");
    mac.update(raw_key.as_bytes());
    mac.verify_slice(stored_hash).is_ok()
}

/// One `control.api_keys` row's fields relevant to validation — not the full DB row shape,
/// just what [`validate_api_key`] needs.
pub struct ApiKeyRecord {
    pub key_hash: Vec<u8>,
    pub status: ApiKeyStatus,
    pub allowed_cidrs: Vec<Cidr>,
    pub expires_at: Option<SystemTime>,
    pub revoked_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKeyRejection {
    /// Presented key does not verify against the stored keyed hash.
    HashMismatch,
    Revoked,
    Expired,
    /// `status` is not `Active`/`Rotating` (e.g. still `Create`, never activated).
    InactiveStatus,
    /// `allowed_cidrs` is non-empty and the request IP is not in it (§73.5 credential CIDR
    /// binding).
    CidrNotAllowed,
}

/// §73.5 validation: keyed-hash compare, then lifecycle status, then `expires_at`/`revoked_at`
/// timestamps (belt-and-suspenders with `status` — a row can be `Active` in the DB with a
/// stale `expires_at` between the row's actual expiry and the next sweep), then CIDR binding.
/// `now` is caller-supplied (§78.1: no clock read baked into a pure function).
pub fn validate_api_key(
    record: &ApiKeyRecord,
    presented_key: &str,
    pepper: &[u8],
    request_ip: IpAddr,
    now: SystemTime,
) -> Result<(), ApiKeyRejection> {
    if !verify_api_key_hash(pepper, presented_key, &record.key_hash) {
        return Err(ApiKeyRejection::HashMismatch);
    }
    match record.status {
        ApiKeyStatus::Revoked => return Err(ApiKeyRejection::Revoked),
        ApiKeyStatus::Expired => return Err(ApiKeyRejection::Expired),
        ApiKeyStatus::Create => return Err(ApiKeyRejection::InactiveStatus),
        ApiKeyStatus::Active | ApiKeyStatus::Rotating => {}
    }
    if record.revoked_at.is_some_and(|at| at <= now) {
        return Err(ApiKeyRejection::Revoked);
    }
    if record.expires_at.is_some_and(|at| at <= now) {
        return Err(ApiKeyRejection::Expired);
    }
    if !record.allowed_cidrs.is_empty() {
        let ip = request_ip.to_canonical();
        if !record.allowed_cidrs.iter().any(|c| c.contains(ip)) {
            return Err(ApiKeyRejection::CidrNotAllowed);
        }
    }
    Ok(())
}

/// §73.5 "日志只记录 fingerprint/prefix，不记录 secret". A short, deterministic,
/// non-reversible string safe for logs/audit metadata — never the raw key, never the full
/// keyed hash (itself a secret verifier). Four bytes of the hash is enough to disambiguate
/// two keys sharing a prefix without materially narrowing a brute-force search for the raw
/// key (a 4-byte HMAC prefix is not a usable oracle against the full 32-byte verifier).
pub fn api_key_log_fingerprint(prefix: &str, key_hash: &[u8]) -> String {
    let fp: String = key_hash
        .iter()
        .take(4)
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("{prefix}.{fp}")
}

// ============================================================================
// §73.6 Web Console Session + CSRF
// ============================================================================

/// §73.6 cookie name. The `__Host-` prefix is part of the name itself, not a separate flag —
/// browsers refuse to set a `__Host-`-prefixed cookie unless `Secure`, `Path=/`, and no
/// `Domain` attribute are all present, so the name alone constrains the attributes below.
pub const SESSION_COOKIE_NAME: &str = "__Host-humaux_session";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SameSitePolicy {
    Lax,
    Strict,
}

impl SameSitePolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            SameSitePolicy::Lax => "Lax",
            SameSitePolicy::Strict => "Strict",
        }
    }
}

/// §73.6 fixed session-cookie attribute string: `Secure`/`HttpOnly`/`Path=/` are frozen;
/// `SameSite` is the one attribute the spec leaves to caller judgement ("according to UX"), so
/// it is a parameter rather than baked into the constant.
pub fn session_cookie_attributes(same_site: SameSitePolicy) -> String {
    format!("Secure; HttpOnly; SameSite={}; Path=/", same_site.as_str())
}

/// Double-submit CSRF cookie name. Unlike [`SESSION_COOKIE_NAME`] this cookie is NOT
/// `HttpOnly` — the client script must read it to echo it back in [`CSRF_HEADER_NAME`] on
/// every state-changing request (§73.6: "SameSite 只是 defense in depth", this pair is the
/// actual enforcement). Carries the `__Host-` prefix for the same reason the session cookie
/// does: without it, any sibling subdomain (or anything that can write a cookie for the
/// parent domain — an unrelated subdomain takeover/XSS, or a plain-HTTP write since nothing
/// else forces this cookie `Secure`) can overwrite it in the victim's browser and forge the
/// matching pair — `verify_csrf_token` only compares cookie against header, nothing binds
/// either to the session. `__Host-` only requires `Secure; Path=/;` no `Domain` — the
/// cookie stays JS-readable, the double-submit echo still works.
pub const CSRF_COOKIE_NAME: &str = "__Host-humaux_csrf";
pub const CSRF_HEADER_NAME: &str = "x-humaux-csrf-token";

/// §73.6 fixed CSRF-cookie attribute string. Deliberately no `HttpOnly` (see
/// [`CSRF_COOKIE_NAME`]); `SameSite` is caller judgement same as
/// [`session_cookie_attributes`].
pub fn csrf_cookie_attributes(same_site: SameSitePolicy) -> String {
    format!("Secure; SameSite={}; Path=/", same_site.as_str())
}

/// Hex-encodes caller-supplied random bytes into a CSRF token. The RNG call itself stays with
/// the caller (I/O-adjacent, out of scope for a pure function); 32 bytes is the caller's
/// contract, not enforced here beyond the fixed-size input type.
pub fn generate_csrf_token(random_bytes: &[u8; 32]) -> String {
    random_bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Constant-time double-submit compare: the cookie value and the header value must be equal
/// and non-empty. Length is checked first (safe to leak — token length is fixed and public,
/// §73.6 is about the token *value*, not its length), the byte comparison itself never
/// short-circuits so its timing does not depend on how many leading bytes matched.
pub fn verify_csrf_token(cookie_value: &str, header_value: &str) -> bool {
    if cookie_value.is_empty() || cookie_value.len() != header_value.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in cookie_value.bytes().zip(header_value.bytes()) {
        diff |= a ^ b;
    }
    diff == 0
}

/// §78.2 "DB enum 与 Rust enum 走 contract test 对账": [`ApiKeyStatus::ALL`] must list
/// exactly `control.api_keys.status`'s CHECK constraint in `migrations/0037_edge_security.sql`,
/// both directions. Runs against the real migration file text (`include_str!`), not a live-DB
/// fixture, so it always executes and fails loud the moment either side drifts — same shape
/// as `crates/adapters/src/email/outbox.rs`'s `contract_tests`.
#[cfg(test)]
mod contract_tests {
    use super::*;

    const MIGRATION_SQL: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations/0037_edge_security.sql"
    ));

    fn check_values(column: &str) -> Vec<String> {
        let needle = format!("CHECK ({column} IN (");
        let start = MIGRATION_SQL
            .find(&needle)
            .unwrap_or_else(|| panic!("migration SQL has no `CHECK ({column} IN (...))` clause"))
            + needle.len();
        let end = MIGRATION_SQL[start..]
            .find(')')
            .expect("unterminated CHECK IN (...) clause")
            + start;
        MIGRATION_SQL[start..end]
            .split(',')
            .map(|s| s.trim().trim_matches('\'').to_string())
            .collect()
    }

    #[test]
    fn api_key_status_matches_check_constraint() {
        let db: Vec<String> = check_values("status");
        let rust: Vec<String> = ApiKeyStatus::ALL
            .iter()
            .map(|s| s.as_db_str().to_string())
            .collect();
        assert_eq!(
            db, rust,
            "ApiKeyStatus::ALL must list every status in DB order"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cidr(s: &str) -> Cidr {
        s.parse().unwrap()
    }

    // ---- §73.2 Cidr ----

    #[test]
    fn cidr_v4_contains() {
        let c = cidr("10.0.0.0/8");
        assert!(c.contains("10.1.2.3".parse().unwrap()));
        assert!(!c.contains("11.0.0.1".parse().unwrap()));
    }

    #[test]
    fn cidr_v6_contains() {
        let c = cidr("2001:db8::/32");
        assert!(c.contains("2001:db8::1".parse().unwrap()));
        assert!(!c.contains("2001:db9::1".parse().unwrap()));
    }

    #[test]
    fn cidr_canonicalizes_ipv4_mapped_ipv6() {
        // §73.2: IPv4/IPv6 must canonicalize before CIDR comparison.
        let c = cidr("10.0.0.0/8");
        let mapped: IpAddr = "::ffff:10.1.2.3".parse().unwrap();
        assert!(c.contains(mapped));
    }

    #[test]
    fn cidr_zero_prefix_matches_everything() {
        assert!(cidr("0.0.0.0/0").contains("203.0.113.9".parse().unwrap()));
        assert!(cidr("::/0").contains("2001:db8::1".parse().unwrap()));
    }

    #[test]
    fn cidr_rejects_malformed_literal() {
        assert!("not-a-cidr".parse::<Cidr>().is_err());
        assert!("10.0.0.0/33".parse::<Cidr>().is_err());
    }

    // ---- §73.2 ClientNetworkIdentity — the required red→green case ----

    fn trusted_proxy_config() -> TrustedProxyConfig {
        TrustedProxyConfig {
            trusted_proxy_cidrs: vec![cidr("10.0.0.0/8")],
            max_forwarded_hops: 3,
        }
    }

    /// 注错红转绿: a forged `X-Forwarded-For` arriving from a peer OUTSIDE
    /// `trusted_proxy_cidrs` must be ignored — `client_ip` falls back to the raw peer, not the
    /// attacker-supplied header.
    #[test]
    fn forged_xff_from_untrusted_peer_is_ignored() {
        let peer: IpAddr = "203.0.113.9".parse().unwrap(); // not in 10.0.0.0/8
        let id = build_client_network_identity(
            peer,
            Some("198.51.100.1"), // forged "real" client IP
            &trusted_proxy_config(),
            None,
            None,
        );
        assert_eq!(
            id.client_ip, peer,
            "forged header must not override peer_ip"
        );
        assert!(id.proxy_chain.is_empty());
        assert!(
            id.risk_tags
                .contains(&RISK_TAG_FORWARDED_HEADER_IGNORED_UNTRUSTED_PEER.to_string())
        );
    }

    #[test]
    fn xff_from_trusted_peer_is_honored() {
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        let id = build_client_network_identity(
            peer,
            Some(" 198.51.100.1 , 10.0.0.5 "),
            &trusted_proxy_config(),
            None,
            None,
        );
        assert_eq!(id.client_ip, "198.51.100.1".parse::<IpAddr>().unwrap());
        assert_eq!(id.proxy_chain, vec!["10.0.0.5".parse::<IpAddr>().unwrap()]);
        assert!(id.risk_tags.is_empty());
    }

    /// 注错红转绿 (§73.2/§73.3 blocker): the leftmost XFF entry is exactly the part of the
    /// chain the CLIENT controls — a real proxy appends what it received, it never removes
    /// what the client sent. An attacker behind the trusted CDN sends `X-Forwarded-For:
    /// 1.2.3.4`; the CDN appends the attacker's real address `203.0.113.66`. `client_ip`
    /// must resolve to the rightmost non-trusted hop (the CDN's own addition), never the
    /// leftmost attacker-supplied one — otherwise every §73.3 IP-policy consumer (denylist,
    /// allowlist, credential CIDR binding) and the rate-limit key evaluate a forged address.
    #[test]
    fn leftmost_forged_xff_entry_is_not_trusted_as_client_ip() {
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        let id = build_client_network_identity(
            peer,
            Some("1.2.3.4, 203.0.113.66"),
            &trusted_proxy_config(),
            None,
            None,
        );
        assert_eq!(
            id.client_ip,
            "203.0.113.66".parse::<IpAddr>().unwrap(),
            "client_ip must be the rightmost (proxy-appended) hop, not the attacker-supplied leftmost one"
        );
        assert!(id.proxy_chain.is_empty());
    }

    /// 注错红转绿 (§73.2/§73.3 major): hop-limit-exceeded must not collapse the request onto
    /// the trusted proxy's own address — that is a client-triggerable IP-policy bypass (pad
    /// the chain, get attributed to the CDN, dodge a denylisted real IP) and a shared
    /// rate-limit key for every client who does it. The chain is still walked for the real
    /// client even while the hop-limit risk tag is raised.
    #[test]
    fn xff_hop_limit_exceeded_still_resolves_real_client_not_peer() {
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        let id = build_client_network_identity(
            peer,
            Some("1.1.1.1,2.2.2.2,3.3.3.3,4.4.4.4"), // 4 hops > max_forwarded_hops=3
            &trusted_proxy_config(),
            None,
            None,
        );
        assert_eq!(
            id.client_ip,
            "4.4.4.4".parse::<IpAddr>().unwrap(),
            "must not fall back to peer_ip — that lets a client hide behind the trusted proxy"
        );
        assert!(
            id.risk_tags
                .contains(&RISK_TAG_FORWARDED_HOP_LIMIT_EXCEEDED.to_string())
        );
    }

    #[test]
    fn xff_all_hops_trusted_falls_back_to_peer() {
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        let id = build_client_network_identity(
            peer,
            Some("10.0.0.2, 10.0.0.3"), // every hop is itself a trusted-proxy address
            &trusted_proxy_config(),
            None,
            None,
        );
        assert_eq!(id.client_ip, peer);
    }

    // ---- §73.3 IP Policy — the required priority test ----

    /// 注错红转绿: emergency deny (global denylist) must override a tenant allowlist that
    /// would otherwise admit the same IP.
    #[test]
    fn global_denylist_overrides_tenant_allowlist() {
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        let denylist = vec![cidr("203.0.113.0/24")];
        let allowlist = vec![cidr("203.0.113.0/24")]; // same block, would otherwise allow
        let decision = evaluate_ip_policy(&IpPolicyInput {
            ip,
            global_denylist: &denylist,
            global_emergency_allowlist: &[],
            tenant_denylist: &[],
            tenant_allowlist: &allowlist,
            credential_allowed_cidrs: &[],
            region_asn_risk_tags: &[],
        });
        assert_eq!(
            decision,
            IpPolicyDecision::Deny(IpPolicyDenyReason::GlobalDenylist)
        );
    }

    #[test]
    fn emergency_allowlist_overrides_tenant_denylist_but_not_global_denylist() {
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        let emergency = vec![cidr("203.0.113.0/24")];
        let tenant_deny = vec![cidr("203.0.113.0/24")];
        let decision = evaluate_ip_policy(&IpPolicyInput {
            ip,
            global_denylist: &[],
            global_emergency_allowlist: &emergency,
            tenant_denylist: &tenant_deny,
            tenant_allowlist: &[],
            credential_allowed_cidrs: &[],
            region_asn_risk_tags: &[],
        });
        assert_eq!(decision, IpPolicyDecision::Allow { risk_tags: vec![] });

        // Same IP, but now also globally denylisted: emergency allow no longer wins.
        let decision2 = evaluate_ip_policy(&IpPolicyInput {
            ip,
            global_denylist: &emergency, // reuse same block as an explicit global deny
            global_emergency_allowlist: &emergency,
            tenant_denylist: &tenant_deny,
            tenant_allowlist: &[],
            credential_allowed_cidrs: &[],
            region_asn_risk_tags: &[],
        });
        assert_eq!(
            decision2,
            IpPolicyDecision::Deny(IpPolicyDenyReason::GlobalDenylist)
        );
    }

    #[test]
    fn risk_policy_never_denies_only_tags() {
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        let tags = vec!["HIGH_RISK_ASN".to_string()];
        let decision = evaluate_ip_policy(&IpPolicyInput {
            ip,
            global_denylist: &[],
            global_emergency_allowlist: &[],
            tenant_denylist: &[],
            tenant_allowlist: &[],
            credential_allowed_cidrs: &[],
            region_asn_risk_tags: &tags,
        });
        assert_eq!(decision, IpPolicyDecision::Allow { risk_tags: tags });
    }

    // ---- §73.5 API key ----

    fn active_record(hash: Vec<u8>) -> ApiKeyRecord {
        ApiKeyRecord {
            key_hash: hash,
            status: ApiKeyStatus::Active,
            allowed_cidrs: vec![],
            expires_at: None,
            revoked_at: None,
        }
    }

    #[test]
    fn valid_key_passes() {
        let pepper = b"server-side-pepper";
        let hash = compute_api_key_hash(pepper, "raw-key-value");
        let record = active_record(hash);
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        assert_eq!(
            validate_api_key(&record, "raw-key-value", pepper, ip, SystemTime::now()),
            Ok(())
        );
    }

    #[test]
    fn wrong_key_is_rejected() {
        let pepper = b"server-side-pepper";
        let hash = compute_api_key_hash(pepper, "raw-key-value");
        let record = active_record(hash);
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        assert_eq!(
            validate_api_key(&record, "wrong-key", pepper, ip, SystemTime::now()),
            Err(ApiKeyRejection::HashMismatch)
        );
    }

    /// 注错红转绿: REVOKED key must always be rejected, even with the right key material.
    #[test]
    fn revoked_key_is_rejected() {
        let pepper = b"server-side-pepper";
        let hash = compute_api_key_hash(pepper, "raw-key-value");
        let mut record = active_record(hash);
        record.status = ApiKeyStatus::Revoked;
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        assert_eq!(
            validate_api_key(&record, "raw-key-value", pepper, ip, SystemTime::now()),
            Err(ApiKeyRejection::Revoked)
        );
    }

    #[test]
    fn expired_by_timestamp_is_rejected_even_if_status_stale() {
        let pepper = b"server-side-pepper";
        let hash = compute_api_key_hash(pepper, "raw-key-value");
        let mut record = active_record(hash);
        record.expires_at = Some(SystemTime::UNIX_EPOCH);
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        assert_eq!(
            validate_api_key(&record, "raw-key-value", pepper, ip, SystemTime::now()),
            Err(ApiKeyRejection::Expired)
        );
    }

    #[test]
    fn cidr_binding_rejects_out_of_band_ip() {
        let pepper = b"server-side-pepper";
        let hash = compute_api_key_hash(pepper, "raw-key-value");
        let mut record = active_record(hash);
        record.allowed_cidrs = vec![cidr("10.0.0.0/8")];
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        assert_eq!(
            validate_api_key(&record, "raw-key-value", pepper, ip, SystemTime::now()),
            Err(ApiKeyRejection::CidrNotAllowed)
        );
        let bound_ip: IpAddr = "10.1.1.1".parse().unwrap();
        assert_eq!(
            validate_api_key(
                &record,
                "raw-key-value",
                pepper,
                bound_ip,
                SystemTime::now()
            ),
            Ok(())
        );
    }

    #[test]
    fn lifecycle_transitions() {
        use ApiKeyStatus::*;
        assert!(is_legal_api_key_transition(Create, Active));
        assert!(is_legal_api_key_transition(Active, Rotating));
        assert!(is_legal_api_key_transition(Rotating, Revoked));
        assert!(!is_legal_api_key_transition(Create, Revoked));
        assert!(!is_legal_api_key_transition(Revoked, Active));
        assert!(!is_legal_api_key_transition(Rotating, Active));
    }

    #[test]
    fn status_db_str_round_trips() {
        for s in [
            ApiKeyStatus::Create,
            ApiKeyStatus::Active,
            ApiKeyStatus::Rotating,
            ApiKeyStatus::Revoked,
            ApiKeyStatus::Expired,
        ] {
            assert_eq!(ApiKeyStatus::from_db_str(s.as_db_str()), Some(s));
        }
        assert_eq!(ApiKeyStatus::from_db_str("BOGUS"), None);
    }

    /// 注错红转绿: the log fingerprint helper must never leak the raw key or the full keyed
    /// hash — this is what a `grep` over log output is meant to catch (§73.5).
    #[test]
    fn log_fingerprint_never_contains_secret_material() {
        let pepper = b"server-side-pepper";
        let raw_key = "sk_live_super_secret_value_do_not_log";
        let hash = compute_api_key_hash(pepper, raw_key);
        let full_hash_hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();

        let fingerprint = api_key_log_fingerprint("sk_live_ab12", &hash);

        assert!(fingerprint.starts_with("sk_live_ab12."));
        assert!(!fingerprint.contains(raw_key));
        assert!(!fingerprint.contains(&full_hash_hex));
        // Fingerprint's hash segment is exactly 8 hex chars (4 bytes) — far shorter than the
        // 64-char full SHA-256 hex digest, so it cannot literally embed it.
        let hash_segment = fingerprint.rsplit('.').next().unwrap();
        assert_eq!(hash_segment.len(), 8);
    }

    // ---- §73.6 session cookie + CSRF ----

    #[test]
    fn session_cookie_name_has_host_prefix() {
        assert!(SESSION_COOKIE_NAME.starts_with("__Host-"));
    }

    #[test]
    fn session_cookie_attributes_are_fixed_plus_samesite() {
        let attrs = session_cookie_attributes(SameSitePolicy::Strict);
        assert!(attrs.contains("Secure"));
        assert!(attrs.contains("HttpOnly"));
        assert!(attrs.contains("Path=/"));
        assert!(attrs.contains("SameSite=Strict"));
    }

    /// 注错红转绿 (§73.6 major): an unprefixed CSRF cookie is forgeable by cookie injection
    /// (sibling subdomain, or a plain-HTTP write) since `verify_csrf_token` only checks
    /// cookie==header with no binding to the session — `__Host-` is what stops that.
    #[test]
    fn csrf_cookie_name_has_host_prefix() {
        assert!(CSRF_COOKIE_NAME.starts_with("__Host-"));
    }

    #[test]
    fn csrf_cookie_attributes_are_secure_not_httponly() {
        let attrs = csrf_cookie_attributes(SameSitePolicy::Strict);
        assert!(attrs.contains("Secure"));
        assert!(attrs.contains("Path=/"));
        assert!(attrs.contains("SameSite=Strict"));
        assert!(!attrs.contains("HttpOnly"));
    }

    #[test]
    fn csrf_token_round_trips() {
        let bytes = [7u8; 32];
        let token = generate_csrf_token(&bytes);
        assert!(verify_csrf_token(&token, &token));
    }

    /// 注错红转绿: a missing/empty CSRF token must fail closed, not vacuously succeed on an
    /// empty/empty comparison.
    #[test]
    fn empty_csrf_tokens_are_rejected() {
        assert!(!verify_csrf_token("", ""));
        assert!(!verify_csrf_token("abc", ""));
    }

    #[test]
    fn mismatched_csrf_tokens_are_rejected() {
        let a = generate_csrf_token(&[1u8; 32]);
        let b = generate_csrf_token(&[2u8; 32]);
        assert!(!verify_csrf_token(&a, &b));
    }
}
