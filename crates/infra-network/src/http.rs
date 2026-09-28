//! `infra-network::http` — ADR-0003 / §83.4 G80-3 判据1's real raw-client construction point.
//! Depends-on: crates=[reqwest, tokio]; services=[HTTP(loopback)]; env=[]; modules=[]
//! Called-by: [infra-cell::transport, infra-egress::http, infra-egress::raw, infra-egress::resolver]
//! Invariants: [egress HTTP failure surfaces as a transport error to the caller; no silent retry across trust boundaries]
//! Spec: none
//!
//! Every other `.rs` file in the workspace is forbidden from naming `reqwest::Client::new`/
//! `::builder` (or `hyper::Client::new`/`::builder`) directly, fully-qualified or via a bare
//! `use`-imported alias — `xtask architecture-check`'s G80-3 判据1 asserts the raw-client
//! construction-site set equals exactly `{crates/infra-network/src/http.rs}`. This is the
//! entire content of that choke point: one config type, one factory function. It carries no
//! `OutboundPurpose`/`EgressPermit`/`IntraCellResource`/`CellAccessPermit` — those are Layer 1
//! concerns (`humaux-infra-egress` / `humaux-infra-cell`), and this file must stay usable by
//! both without either becoming aware of the other.

use std::time::Duration;

/// §78.1: a named, overridable knob — not a literal buried inside `Client::builder()` — for
/// the whole-request timeout every client this factory builds carries. Every caller (Layer 1A
/// external egress, Layer 1B intra-Cell) supplies its own value; this type has no default,
/// deliberately — "how long is too long to wait" is a Layer 1 policy decision (§7.4's 60s
/// disclosure-ledger watchdog for Layer 1A, a Cell-internal SLO for Layer 1B), not something
/// this semantically-neutral layer should default on their behalf.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientConfig {
    /// Whole-request wall-clock timeout (connect + send + receive). `reqwest::Client` has no
    /// default timeout at all — without one, a hung peer holds a connection open forever.
    pub request_timeout: Duration,
    /// Whether to honor `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` from the process environment
    /// (`reqwest`'s default: `true`). Found necessary while wiring [`build_client_with_resolver`]
    /// for Layer 1B (`humaux-infra-cell`): a proxied request's destination is resolved by the
    /// *proxy*, not by this client's own `dns_resolver` — an environment-configured proxy would
    /// silently defeat the DNS-rebinding fix that function exists for, since the validating
    /// resolver never sees the real destination host at all. Layer 1A (`humaux-infra-egress`,
    /// external egress) legitimately wants an org's egress proxy honored; Layer 1B (same-Cell
    /// resource access) must never traverse one — same-Cell traffic routed through an external
    /// hop is itself a §83.4 判据4 violation (deploy-gate: no Internet/NAT route out of the
    /// Cell), whether or not the proxy is trustworthy. Each caller states its own answer
    /// explicitly (no default here, same reasoning as `request_timeout`'s doc).
    pub trust_env_proxy: bool,
}

/// Shared builder setup for [`build_client_with_resolver`] — kept as its own function so the
/// redirect policy and timeout live in one place independent of the resolver wiring.
fn client_builder(config: ClientConfig) -> reqwest::ClientBuilder {
    let builder = reqwest::Client::builder()
        .timeout(config.request_timeout)
        .redirect(reqwest::redirect::Policy::none());
    if config.trust_env_proxy {
        builder
    } else {
        builder.no_proxy()
    }
}

/// The workspace's one legal `reqwest::Client` construction site (ADR-0003, §83.4 G80-3) —
/// and, since ADR-0039, the only one that exists at all: a caller cannot build an outbound
/// client without naming the resolver it will dial through, because there is no overload that
/// omits it.
///
/// `resolver` is installed as the client's DNS resolver
/// (`reqwest::ClientBuilder::dns_resolver`, stable since 0.12.28) instead of `reqwest`'s
/// default `GaiResolver`.
///
/// ADR-0003 second-round correction (OWASP SSRF Cheat Sheet's DNS rebinding / "TOCTOU" pinning
/// bypass): a caller that resolves+validates a hostname itself and *then* hands the bare
/// hostname to an HTTP client leaves a gap — the client's own connector resolves the name a
/// second time, and nothing guarantees the second lookup returns the same addresses the first
/// one validated. This function exists so a Layer 1 caller can make its validating resolver the
/// *only* resolver this client ever consults: there is one lookup, not two, so "the addresses
/// that were validated" and "the addresses that get dialed" are structurally the same call, not
/// a discipline of remembering to re-check. TLS certificate validation still runs against the
/// original hostname (SNI/SAN) — only address *resolution* is intercepted, so this does not
/// degrade into dialing a bare IP and skipping hostname verification.
///
/// ADR-0039: the resolver-less sibling (`build_client`) that Layer 1A used to call is
/// **deleted**, not deprecated — while it existed, `infra-egress`'s two transports dialed with
/// the system resolver while §11.4's SSRF gate checked with an injected one, which is the
/// rebinding window this whole mechanism exists to close. `xtask architecture-check`'s
/// "outbound client dials only through the checked resolver" gate keeps it deleted and pins
/// this function's call sites to the two Layer 1 resolver modules.
///
/// §83.4 判据3: redirects are disabled at the client level (`Policy::none()`), not merely
/// checked at the first-hop hostname. `reqwest`'s default policy (`Policy::limited(10)`) would
/// otherwise follow a 3xx from a registry-resolved, CIDR-checked host to *any* address — a
/// compromised or misconfigured in-cell/external endpoint could redirect this client to
/// `169.254.169.254`, another Cell's CIDR, or the public internet, with none of that second
/// hop ever re-checked. Both Layer 1A (`humaux-infra-egress`) and Layer 1B (`humaux-infra-cell`)
/// build their client through this one function, so this closes the hole for both at once —
/// each layer's own transport maps the resulting 3xx status to an explicit error instead of a
/// followed connection.
///
/// Fails only if the TLS backend cannot initialize (`reqwest::Client::builder().build()`'s own
/// failure mode) — a process-startup-time configuration error, not a per-call one.
pub fn build_client_with_resolver<R>(
    config: ClientConfig,
    resolver: std::sync::Arc<R>,
) -> Result<reqwest::Client, reqwest::Error>
where
    R: reqwest::dns::Resolve + 'static,
{
    client_builder(config).dns_resolver(resolver).build()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StubResolve;
    impl reqwest::dns::Resolve for StubResolve {
        fn resolve(&self, _name: reqwest::dns::Name) -> reqwest::dns::Resolving {
            Box::pin(async {
                Ok(Box::new(std::iter::empty())
                    as Box<dyn Iterator<Item = std::net::SocketAddr> + Send>)
            })
        }
    }

    /// The one constructor builds successfully — its only failure mode is TLS-backend init.
    #[test]
    fn build_client_with_resolver_succeeds() {
        let client = build_client_with_resolver(
            ClientConfig {
                request_timeout: Duration::from_secs(5),
                trust_env_proxy: false,
            },
            std::sync::Arc::new(StubResolve),
        );
        assert!(client.is_ok());
    }

    /// §83.4 判据3 regression: a 3xx response from the destination this client dialed must
    /// never be followed to a second host. Two real listeners — the client must connect to
    /// only the first; the second (the redirect target) must see zero connections.
    #[tokio::test]
    async fn redirect_is_never_followed_to_a_second_host() {
        let redirect_target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = redirect_target.local_addr().unwrap();
        let target_saw_connection = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let target_flag = target_saw_connection.clone();
        tokio::spawn(async move {
            if redirect_target.accept().await.is_ok() {
                target_flag.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        });

        let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin_addr = origin.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut socket, _) = origin.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let response = format!(
                "HTTP/1.1 302 Found\r\nLocation: http://{target_addr}/pwned\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });

        // The URL authority below is an IP literal, which `reqwest`'s connector never hands to
        // a custom `dns_resolver` — `StubResolve` is therefore never consulted here, and this
        // test still exercises exactly the redirect policy it is about.
        let client = build_client_with_resolver(
            ClientConfig {
                request_timeout: Duration::from_secs(2),
                trust_env_proxy: false,
            },
            std::sync::Arc::new(StubResolve),
        )
        .unwrap();
        let response = client
            .get(format!("http://{origin_addr}/start"))
            // dep: HTTP(loopback) — outbound http call
            .send()
            .await
            .unwrap();
        // The redirect is surfaced as-is, not transparently followed.
        assert_eq!(response.status().as_u16(), 302);

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !target_saw_connection.load(std::sync::atomic::Ordering::SeqCst),
            "REDIRECT_FOLLOWED = true — the redirect target must never see a connection"
        );
    }
}
