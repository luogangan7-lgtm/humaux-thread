//! `infra-egress::http` — ADR-0003 / §83.4 Layer 1A: external-egress HTTP transport.
//! Depends-on: crates=[async-trait, humaux-contracts, humaux-domain, humaux-infra-network, tokio, uuid, zeroize];
//!   services=[HTTP(loopback), HTTP(provider)];
//!   env=[HUMAUX_TEST_INFRA_EGRESS_ENV_CREDENTIAL_SOURCE_FAILS_CLOSED_WHEN_UNSET,
//!   HUMAUX_TEST_INFRA_EGRESS_ENV_CREDENTIAL_SOURCE_READS_A_SET_VARIABLE]; modules=[contracts::config_registry,
//!   domain::egress, domain::error, humaux-infra-network, infra-egress::resolver, infra-network::http]
//! Called-by: [retrieval-provider::adapters]
//! Invariants: [the only holder of a reqwest client for external destinations (built via infra-network, never
//!   Client::new here); non-HTTPS endpoints are refused; provider HTTP statuses map to typed ErrorCodes (401
//!   Unauthorized, 429 RateLimited, 5xx Transient)]
//! Spec: Baseline §7.4; ADR-0003; §19; §7.3; §73.5
//!
//! Raw `reqwest::Client` construction lives one layer down, in `humaux-infra-network` (the
//! sole workspace-wide **protocol** choke point, `xtask architecture-check`'s G80-3 判据1) —
//! this module reaches the `reqwest::Client`/`reqwest::Error` *types* only through
//! `humaux_infra_network::reqwest`, and never calls `Client::new`/`::builder` itself.
//! [`HttpExternalCall`] is this module's one `humaux_domain::egress::ExternalCall`
//! implementation, and the only place in the workspace permitted to hold that client for
//! **external** (cross-trust-boundary, §7.4-disclosed) destinations — same-Cell destinations
//! (Qdrant) go through `humaux-infra-cell`'s `IntraCellHttpTransport` instead (ADR-0003), never
//! through this trait.
//!
//! ## Credential injection (`RetrievalCredentialSource`)
//!
//! [`HttpExternalCall::call`] sends every request with `Content-Type: application/json` (every
//! current/foreseen provider adapter in this workspace speaks JSON, §19 "Provider Plane" own
//! provider-shape survey) and an `Authorization: Bearer <credential>` header, resolved at call
//! time from the [`RetrievalCredentialSource`] this instance was constructed with — never a
//! bare `String` threaded in through a call argument, and never a field on
//! [`AuthorizedEgressPayload`] (§7.3: that type's digest is bound into the `EgressPermit`;
//! folding a credential into it would let a rotated/expired credential silently invalidate a
//! still-valid permit, and would put a secret inside the one struct §7.4's ledger writer reads
//! `payload_bytes`/`payload_sha256` straight off of). [`EgressSecret`] is the one sanctioned
//! carrier for the resolved value between [`RetrievalCredentialSource::credential`] and the
//! `Authorization` header this transport builds from it — see its own doc for why its `Debug`
//! never prints the value, its buffer zeroizes on drop, and (§73.5) why the header value built
//! from it is zeroized and marked sensitive too, not only the wrapper.
//!
//! §19 DOD-028 "任何 User BYOK MUST NOT 被 Retrieval Provider Plane 自动借用": [`call`](
//! HttpExternalCall::call) refuses to resolve a credential at all unless `permit.purpose()` is
//! [`PrivateDataPurpose::RetrievalEmbedding`] or [`PrivateDataPurpose::RetrievalRerank`] — the
//! platform retrieval credential this module injects can never be attached to a permit minted
//! for any other purpose, structurally, not by caller discipline.
//!
//! [`EnvCredentialSource`] is this module's only [`RetrievalCredentialSource`] impl today, and
//! is a **dev/OSS-default implementation, not the production credential path** — see its own
//! doc for the OpenBao upgrade this is standing in for (Baseline_2.9.md "Tenant Data
//! Encryption / Key Hierarchy").
//!
//! A non-2xx response status is classified into an [`ErrorCode`] by the caller-supplied
//! `status_classifier` (see [`HttpExternalCall::new`]) rather than one blanket code — this
//! crate is Layer 1A generic transport and does not itself know a specific provider's or
//! credential-domain's status semantics (e.g. `retrieval-provider::adapters::
//! map_dashscope_status`).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use humaux_domain::egress::{
    AuthorizedEgressPayload, EgressPermit, ExternalCall, PrivateDataPurpose, ProcessorId,
};
use humaux_domain::error::ErrorCode;
use humaux_infra_network::http::ClientConfig;
use humaux_infra_network::reqwest;

use crate::resolver::{DEFAULT_PIN_TTL, SystemCheckedResolve, build_pinned_client};

/// A resolved outbound credential value — e.g. the plaintext bearer token
/// [`HttpExternalCall::call`] injects as `Authorization: Bearer <value>`. Mirrors
/// `crates/adapters/src/byok.rs`'s `PlaintextApiKey`/`HeaderValue` pattern: not `Clone`/`Copy`
/// (no accidental second owner of a plaintext secret), and its `Debug` never prints the value
/// — §73.5 "日志只记录 fingerprint/prefix，不记录 secret" — so a stray `{:?}` on a value that
/// (incorrectly) captured one cannot leak it. [`EgressSecret::expose`] is the one sanctioned
/// reader, named like those types' own `expose` methods so a grep for "who reads a raw egress
/// credential" has exactly one hit per call site (today: [`HttpExternalCall::call`],
/// immediately before building the request). The backing buffer is a `zeroize::Zeroizing<
/// String>`, which overwrites it with zeros on drop rather than leaving the plaintext to sit in
/// freed heap memory for whatever happens to reuse that allocation next.
pub struct EgressSecret(zeroize::Zeroizing<String>);

impl EgressSecret {
    pub fn new(value: String) -> Self {
        Self(zeroize::Zeroizing::new(value))
    }

    /// The only way to read the raw credential — see struct doc.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for EgressSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EgressSecret(redacted)")
    }
}

/// Resolves this transport's outbound bearer credential — called once per [`HttpExternalCall::
/// call`], never cached on that struct, so a rotated credential is picked up on the very next
/// call rather than staying pinned to whatever was true at construction time. §7/"Key
/// Hierarchy": the production implementation belongs behind OpenBao Transit; [`
/// EnvCredentialSource`] below is this module's only implementation today, and is explicitly
/// not that production path — see its own doc.
#[async_trait::async_trait]
pub trait RetrievalCredentialSource: Send + Sync {
    async fn credential(&self) -> Result<EgressSecret, ErrorCode>;
}

/// Dev/OSS-default [`RetrievalCredentialSource`]: reads a plaintext credential from a named
/// process environment variable at call time, via `humaux_contracts::config_registry::
/// read_env_var` — this module's own §78 boundary-lint compliance: the workspace forbids a
/// direct `std::env::var` call anywhere outside `crates/contracts/`/`bins/*`/`xtask/`/`tests/`
/// (§50.1 "humaux-contracts owns the one legitimate config-read point"), so the actual raw read
/// lives there, not here.
///
/// **Not the production credential path.** Baseline_2.9.md's "Tenant Data Encryption / Key
/// Hierarchy" section names OpenBao Transit as the platform secret store this credential
/// should come from in production — a real deployment MUST supply an OpenBao-backed
/// `RetrievalCredentialSource` implementation (decrypting/fetching as close to
/// [`HttpExternalCall::call`]'s own call site as this trait already places the read) in place
/// of this type. `EnvCredentialSource` exists so `crates/retrieval-provider` has something real
/// to run against for local development and the OSS default config (Baseline_2.9.md "OSS 默认
/// 配置") before that OpenBao implementation lands.
pub struct EnvCredentialSource {
    var_name: String,
}

impl EnvCredentialSource {
    pub fn new(var_name: impl Into<String>) -> Self {
        Self {
            var_name: var_name.into(),
        }
    }
}

#[async_trait::async_trait]
impl RetrievalCredentialSource for EnvCredentialSource {
    /// [`ErrorCode::Internal`], not [`ErrorCode::WaitingKey`], when the named variable is
    /// unset or empty: this is the *platform's* retrieval credential (§19 "Provider Credential
    /// 边界" — independent of any `USER_REASONING`/`PLATFORM_PUBLIC` key, and no
    /// `CUSTOMER_RETRIEVAL_BYOK` trust domain exists yet for a tenant-facing "waiting on key"
    /// reading to apply to). A missing value here is an operator-side deployment defect, not a
    /// per-tenant or per-call condition — the same reasoning `map_dashscope_status`'s own doc
    /// (`retrieval-provider::adapters`) gives for classifying a real 401 as `Unauthorized`
    /// rather than `WaitingKey` under this same credential domain.
    async fn credential(&self) -> Result<EgressSecret, ErrorCode> {
        match humaux_contracts::config_registry::read_env_var(&self.var_name) {
            Some(value) => Ok(EgressSecret::new(value)),
            None => Err(ErrorCode::Internal),
        }
    }
}

/// [`HttpExternalCall::new`]'s failure mode — either the underlying `reqwest::Client` failed
/// to build (TLS-backend init only, per [`build_pinned_client`]'s own doc), or `endpoint` failed the
/// §7.4 scheme check (see [`endpoint_scheme_is_allowed`]). Kept as this crate's own small enum
/// rather than reusing `reqwest::Error` for the second case: `reqwest::Error` has no public
/// constructor this crate is allowed to use (this module never names `reqwest::Client::new`/
/// `::builder` itself, module doc), so a scheme rejection cannot be represented as one.
#[derive(Debug)]
pub enum HttpExternalCallError {
    Client(reqwest::Error),
    /// §7.4: a real platform egress credential must never be handed to a plaintext transport,
    /// and never to a proxy over one either (`ClientConfig::trust_env_proxy: true` above hands
    /// any `HTTP_PROXY`-configured host the same header) — see [`endpoint_scheme_is_allowed`].
    InsecureEndpoint,
}

impl fmt::Display for HttpExternalCallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(e) => write!(f, "client build failed: {e}"),
            Self::InsecureEndpoint => f.write_str(
                "endpoint must be https://, or http://127.0.0.1|localhost for local testing",
            ),
        }
    }
}

impl std::error::Error for HttpExternalCallError {}

impl From<reqwest::Error> for HttpExternalCallError {
    fn from(e: reqwest::Error) -> Self {
        Self::Client(e)
    }
}

/// §7.4: this transport injects a real platform bearer credential (module doc "Credential
/// injection") on every request — it must never be sent in cleartext, and never handed to
/// whatever host `HTTP_PROXY`/`HTTPS_PROXY` names either (`trust_env_proxy: true` below routes
/// *all* traffic, proxy included, through this same header). `https://` is always allowed;
/// plain `http://` is allowed only to loopback (`127.0.0.1`/`localhost`/`::1`), so this
/// crate's own `TcpListener`-backed tests keep working without opening a hole for any real
/// destination.
fn endpoint_scheme_is_allowed(endpoint: &str) -> bool {
    if let Some(rest) = endpoint.strip_prefix("https://") {
        return !rest.is_empty();
    }
    if let Some(rest) = endpoint.strip_prefix("http://") {
        let host = rest.split(['/', ':', '?']).next().unwrap_or("");
        return matches!(host, "127.0.0.1" | "localhost" | "::1" | "[::1]");
    }
    false
}

/// §78.1: named, overridable knobs — not literals buried inside `Client::builder()` — for the
/// per-request timeout and response-size cap this transport enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpEgressConfig {
    /// Whole-request wall-clock timeout (connect + send + receive). `reqwest::Client` has no
    /// default timeout at all — without one, a provider that accepts a connection and then
    /// never responds hangs this call forever, leaving any §7.4 disclosure row `reserve()`d
    /// around it open until the ledger's own 60s INV-3 watchdog notices from the outside.
    /// 20s is comfortably under that 60s window so a hung provider surfaces here first, as
    /// `ProviderTransient`, rather than only via the ledger alarm.
    pub request_timeout: Duration,
    /// Upper bound on response body bytes read into memory. Unbounded, a malicious or merely
    /// broken provider can force this adapter to buffer an arbitrarily large body per call.
    /// 8 MiB comfortably covers a real embedding/rerank/chat-completion JSON response with
    /// margin; raise it if a real provider response is ever legitimately observed near it.
    pub max_response_bytes: usize,
}

impl Default for HttpEgressConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(20),
            // ponytail: a fixed 8 MiB cap, not a per-purpose table — nothing in this task's
            // scope needs a different cap per `OutboundPurpose`; add one if a real provider's
            // legitimate response ever needs more.
            max_response_bytes: 8 * 1024 * 1024,
        }
    }
}

/// The sole `ExternalCall` (§7.3) implementation backed by a real `reqwest::Client`.
///
/// `endpoint` is a plain per-instance base URL, not a `ProcessorId`-keyed lookup: the
/// Processor Registry (`control.processors` et al., §7 末) that would resolve "which
/// `ProcessorId` goes to which URL" is a later task's deliverable (`humaux_domain::egress`'s
/// own module doc "out of scope" note) — a caller that already knows the target endpoint for
/// the permit it minted configures it here directly. Upgrade path: replace the single
/// `endpoint` field with a registry lookup keyed on `permit.processor()` once that table
/// lands, with no change to this impl's `ExternalCall` signature.
///
/// `processor` is the [`ProcessorId`] this instance actually sends to — [`call`](Self::call)
/// rejects any permit minted for a *different* processor (§7.4: the ledger's `processor_id`
/// column must name the real recipient, not merely some processor the caller once had a
/// permit for). Single-use consumption of a permit (§7.3 replay prevention across separate
/// calls) is deliberately not this type's job: `humaux_adapters::disclosure`'s §7.4
/// reserve/finalize ledger is the actual one-shot boundary — a second `call()` with the same
/// permit produces a second ledger row, and the ledger, not this transport, is what a §7.0
/// auditor reads to notice a permit reused past its intended single call.
pub struct HttpExternalCall {
    client: reqwest::Client,
    endpoint: String,
    processor: ProcessorId,
    config: HttpEgressConfig,
    /// See module doc "Credential injection" — resolved fresh on every [`Self::call`], never
    /// read at construction time.
    credential_source: Arc<dyn RetrievalCredentialSource>,
    /// §19.2 "429：有界退避；401：根据 provider/key domain 进入 invalid/waiting_key；5xx：
    /// transient retry" — a non-2xx status is classified by this caller-supplied function
    /// (e.g. `retrieval-provider::adapters::map_dashscope_status`) rather than one blanket
    /// [`ErrorCode::ProviderPermanent`]: this crate is Layer 1A generic transport and must not
    /// itself know a specific provider's/credential-domain's status semantics (module doc),
    /// but collapsing every status into one code made that classification dead code on the
    /// live path — a rejected/expired key was indistinguishable from a 500. A plain `fn`
    /// pointer, not `Arc<dyn Fn>`: every real classifier (`map_dashscope_status` included) is
    /// a pure function with no state to capture.
    status_classifier: fn(u16) -> ErrorCode,
}

impl HttpExternalCall {
    /// Builds the `reqwest::Client` this instance holds via Layer 0's
    /// [`build_pinned_client`] — ADR-0003: this crate itself has not named `Client::new`/
    /// `::builder` since the Layer 0/1A split (see module doc); ownership of the *decision* to
    /// build a client for this `(endpoint, processor)` pair still lives here.
    ///
    /// ADR-0039: the client dials only through [`SystemCheckedResolve`], pinned for
    /// [`DEFAULT_PIN_TTL`] — one lookup per host per TTL window, and the addresses that lookup
    /// returned are the only ones the connector may dial. Unlike the BYOK path
    /// (`crate::raw::RawHttpPost`, whose caller injects §11.4's private/reserved-range policy),
    /// this resolver carries no address policy: `endpoint` here is operator-configured
    /// (`retrieval-provider`'s DashScope wiring), not user-supplied SSRF input, and this crate's
    /// own loopback test fixtures are legitimate destinations. What ADR-0039 buys on this path
    /// is the pin itself — no second, unobserved name resolution between "the address this
    /// process resolved" and "the address it connected to".
    ///
    /// `status_classifier` — see the field's own doc. Pass `map_dashscope_status` (or the
    /// equivalent for a future provider); this crate has no sensible universal default because
    /// it deliberately does not know provider/credential-domain semantics.
    ///
    /// # Errors
    ///
    /// [`HttpExternalCallError::InsecureEndpoint`] if `endpoint` is neither `https://` nor a
    /// loopback `http://` (see [`endpoint_scheme_is_allowed`]) — checked before the network
    /// client is even built, so this never depends on a live connection.
    pub fn new(
        endpoint: impl Into<String>,
        processor: ProcessorId,
        config: HttpEgressConfig,
        credential_source: Arc<dyn RetrievalCredentialSource>,
        status_classifier: fn(u16) -> ErrorCode,
    ) -> Result<Self, HttpExternalCallError> {
        let endpoint = endpoint.into();
        if !endpoint_scheme_is_allowed(&endpoint) {
            return Err(HttpExternalCallError::InsecureEndpoint);
        }
        let client = build_pinned_client(
            ClientConfig {
                request_timeout: config.request_timeout,
                // Layer 1A external egress: preserve `reqwest`'s prior default (obey
                // `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY`) — an org's egress proxy is a legitimate
                // path for traffic that is, by definition, already leaving Humaux-operated
                // infrastructure. See `ClientConfig::trust_env_proxy`'s doc for why Layer 1B
                // (`humaux-infra-cell`) answers this differently, and `crate::resolver`'s module
                // doc for the ceiling this places on the pin.
                trust_env_proxy: true,
            },
            std::sync::Arc::new(SystemCheckedResolve),
            DEFAULT_PIN_TTL,
        )?;
        Ok(Self {
            client,
            endpoint,
            processor,
            config,
            credential_source,
            status_classifier,
        })
    }
}

#[async_trait::async_trait]
impl ExternalCall for HttpExternalCall {
    async fn call(
        &self,
        permit: &EgressPermit,
        payload: &AuthorizedEgressPayload,
    ) -> Result<Vec<u8>, ErrorCode> {
        // §19 "任何 User BYOK MUST NOT 被 Retrieval Provider Plane 自动借用" / DOD-028: this
        // transport's `credential_source` is always the *platform* retrieval credential (module
        // doc "Credential injection") — binding it to only the two retrieval purposes is what
        // makes that a structural fact rather than a caller discipline. A `UserReasoning`
        // permit (or any future non-retrieval purpose) must never reach the credential lookup
        // below at all.
        if !matches!(
            permit.purpose(),
            PrivateDataPurpose::RetrievalEmbedding | PrivateDataPurpose::RetrievalRerank
        ) {
            return Err(ErrorCode::Forbidden);
        }
        // §7.4: the ledger's `processor_id` must be the real recipient — a permit minted for
        // processor A must not be honorable by an instance wired to processor B's endpoint.
        if permit.processor() != self.processor {
            return Err(ErrorCode::Forbidden);
        }
        // §7.3: "adapter 必须验证 payload.sha256 == permit.payload_sha256；Permit 不能被拿去
        // 发送另一份正文" — checked before anything network-visible happens.
        if payload.sha256() != permit.payload_sha256() {
            return Err(ErrorCode::Forbidden);
        }
        // §7.3: a stale reservation must not be honored.
        if permit.is_expired(std::time::Instant::now()) {
            return Err(ErrorCode::Forbidden);
        }

        // Resolved last, right before the request is built (never earlier — module doc
        // "Credential injection"): a permit this call is about to reject never triggers a
        // credential lookup at all.
        let credential = self.credential_source.credential().await?;

        // §73.5: built into a `Zeroizing<String>` (not a plain `String` from `format!`) so the
        // one extra heap copy `format!` needs to prepend `"Bearer "` is itself zeroized on
        // drop, not only `credential`'s own buffer — then validated explicitly via
        // `HeaderValue::from_str` rather than handed to `.header()` as a bare string: a
        // malformed credential (e.g. a trailing newline from a file/secret mount) is rejected
        // right here as `ErrorCode::Internal`, instead of surfacing from `.send()` as a
        // `reqwest` builder error that the old blanket `ProviderTransient` mapping made look
        // like a retryable network fault. `set_sensitive(true)` keeps it out of HTTP/2 HPACK
        // indexing and out of any future `Debug` of the request.
        let bearer = zeroize::Zeroizing::new(format!("Bearer {}", credential.expose()));
        let mut auth_value =
            reqwest::header::HeaderValue::from_str(&bearer).map_err(|_| ErrorCode::Internal)?;
        auth_value.set_sensitive(true);

        let mut response = self
            .client
            .post(&self.endpoint)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::AUTHORIZATION, auth_value)
            .body(payload.bytes().to_vec())
            // dep: HTTP(provider) — outbound http call
            .send()
            .await
            .map_err(|e| {
                // A malformed request (e.g. a header `reqwest` itself rejected) is a config
                // defect, not a transient network condition — `.is_builder()` distinguishes
                // that class from an actual connect/send failure. The credential header itself
                // can no longer land here (validated above), but other builder failures should
                // not silently retry forever as `ProviderTransient` either.
                if e.is_builder() {
                    ErrorCode::Internal
                } else {
                    ErrorCode::ProviderTransient
                }
            })?;

        if !response.status().is_success() {
            return Err((self.status_classifier)(response.status().as_u16()));
        }

        // Bounded read (§83.4): accumulate in capped chunks instead of `response.bytes()`,
        // which buffers the entire body regardless of size before returning anything.
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| ErrorCode::ProviderTransient)?
        {
            if body.len() + chunk.len() > self.config.max_response_bytes {
                return Err(ErrorCode::ProviderPermanent);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use humaux_domain::dataclass::DataClass;
    use humaux_domain::egress::authorize;
    use humaux_domain::ids::TenantId;
    use std::time::Duration as StdDuration;
    use tokio::net::TcpListener;
    use uuid::Uuid;

    /// Fixed non-empty credential for every test in this module — none of them exercises
    /// [`RetrievalCredentialSource`] itself (that lives in `EnvCredentialSource`'s own doctest-
    /// free unit tests below), only [`HttpExternalCall::call`]'s permit/expiry/processor/
    /// timeout/size behavior, none of which depends on the credential's actual value.
    struct FixedCredential;

    #[async_trait::async_trait]
    impl RetrievalCredentialSource for FixedCredential {
        async fn credential(&self) -> Result<EgressSecret, ErrorCode> {
            Ok(EgressSecret::new("test-credential".to_string()))
        }
    }

    /// A distinct-per-status classifier for this module's own fault tests — proves the
    /// `status_classifier` plumbing carries a caller-supplied mapping through `call()`, without
    /// this crate needing to know a real provider's status semantics (`crate` module doc /
    /// `HttpExternalCall::status_classifier` field doc). Not `map_dashscope_status` itself:
    /// that function lives one crate up (`retrieval-provider`, not a dependency of this one).
    fn test_status_classifier(status: u16) -> ErrorCode {
        match status {
            401 => ErrorCode::Unauthorized,
            429 => ErrorCode::ProviderRateLimited,
            500..=599 => ErrorCode::ProviderTransient,
            _ => ErrorCode::ProviderPermanent,
        }
    }

    fn call_at(addr: std::net::SocketAddr, processor: ProcessorId) -> HttpExternalCall {
        HttpExternalCall::new(
            format!("http://{addr}/x"),
            processor,
            HttpEgressConfig::default(),
            std::sync::Arc::new(FixedCredential),
            test_status_classifier,
        )
        .expect("client builds")
    }

    /// §7.3 acceptance test, now actually observing the network rather than inferring it from
    /// an error-code side channel (a `ProviderTransient` from a bad URL and one from a real
    /// connection attempt were previously indistinguishable) — a real bound TCP listener that
    /// must see zero connection attempts while a digest-mismatched call is rejected.
    #[tokio::test]
    async fn mismatched_payload_makes_zero_network_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let processor = ProcessorId(Uuid::now_v7());

        let original = AuthorizedEgressPayload::new(b"authorized content".to_vec());
        let permit = authorize(
            TenantId::new(),
            processor,
            PrivateDataPurpose::RetrievalEmbedding,
            DataClass::Private,
            &original,
            StdDuration::from_secs(60),
        )
        .unwrap();
        let swapped = AuthorizedEgressPayload::new(b"a different payload entirely".to_vec());

        let call = call_at(addr, processor);
        // dep: HTTP(loopback) — outbound http call
        let result = call.call(&permit, &swapped).await;
        assert_eq!(result, Err(ErrorCode::Forbidden));

        let accepted = tokio::time::timeout(StdDuration::from_millis(200), listener.accept()).await;
        assert!(
            accepted.is_err(),
            "digest mismatch must be rejected before any connection is attempted"
        );
    }

    /// Inverse of the above: a matching payload against a real (if immediately-dropped)
    /// listener must produce exactly one connection attempt — proving the rejection above is
    /// the digest check actually firing, not every call unconditionally short-circuiting.
    #[tokio::test]
    async fn matching_payload_reaches_the_network_exactly_once() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let processor = ProcessorId(Uuid::now_v7());

        let payload = AuthorizedEgressPayload::new(b"authorized content".to_vec());
        let permit = authorize(
            TenantId::new(),
            processor,
            PrivateDataPurpose::RetrievalEmbedding,
            DataClass::Private,
            &payload,
            StdDuration::from_secs(60),
        )
        .unwrap();

        let call = call_at(addr, processor);
        // dep: HTTP(loopback) — outbound http call
        let call_task = tokio::spawn(async move { call.call(&permit, &payload).await });

        let (socket, _) = tokio::time::timeout(StdDuration::from_secs(2), listener.accept())
            .await
            .expect("exactly one connection must arrive")
            .expect("accept succeeds");
        drop(socket); // Reset before any response — the call errors out, that's not this test's concern.

        let _ = call_task.await;

        let second = tokio::time::timeout(StdDuration::from_millis(150), listener.accept()).await;
        assert!(
            second.is_err(),
            "expected exactly one connection attempt, saw a second"
        );
    }

    #[tokio::test]
    async fn expired_permit_is_rejected() {
        let processor = ProcessorId(Uuid::now_v7());
        let payload = AuthorizedEgressPayload::new(b"x".to_vec());
        let permit = authorize(
            TenantId::new(),
            processor,
            PrivateDataPurpose::RetrievalEmbedding,
            DataClass::Private,
            &payload,
            StdDuration::from_millis(0),
        )
        .unwrap();
        tokio::time::sleep(StdDuration::from_millis(5)).await;

        let call = call_at("127.0.0.1:1".parse().unwrap(), processor);
        // dep: HTTP(loopback) — outbound http call
        let result = call.call(&permit, &payload).await;
        assert_eq!(result, Err(ErrorCode::Forbidden));
    }

    /// §7.4: a permit minted for one processor must not be honorable by an `HttpExternalCall`
    /// wired to a different one, even though the payload digest and expiry are both fine.
    #[tokio::test]
    async fn permit_for_a_different_processor_is_rejected() {
        let minted_for = ProcessorId(Uuid::now_v7());
        let wired_to = ProcessorId(Uuid::now_v7());
        let payload = AuthorizedEgressPayload::new(b"x".to_vec());
        let permit = authorize(
            TenantId::new(),
            minted_for,
            PrivateDataPurpose::RetrievalEmbedding,
            DataClass::Private,
            &payload,
            StdDuration::from_secs(60),
        )
        .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let call = call_at(addr, wired_to);
        // dep: HTTP(loopback) — outbound http call
        let result = call.call(&permit, &payload).await;
        assert_eq!(result, Err(ErrorCode::Forbidden));

        let accepted = tokio::time::timeout(StdDuration::from_millis(200), listener.accept()).await;
        assert!(
            accepted.is_err(),
            "a processor mismatch must be rejected before any connection is attempted"
        );
    }

    /// §19 DOD-028 注错红转绿: before this guard existed, `call()` attached the platform
    /// retrieval credential regardless of `permit.purpose()` — a `UserReasoning` permit (any
    /// non-retrieval purpose) got the platform key on the wire. Same repro shape as the
    /// processor-mismatch test above: zero network connections, `Forbidden`.
    #[tokio::test]
    async fn permit_for_a_non_retrieval_purpose_is_rejected() {
        let processor = ProcessorId(Uuid::now_v7());
        let payload = AuthorizedEgressPayload::new(b"x".to_vec());
        let permit = authorize(
            TenantId::new(),
            processor,
            PrivateDataPurpose::UserReasoning,
            DataClass::Private,
            &payload,
            StdDuration::from_secs(60),
        )
        .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let call = call_at(addr, processor);
        // dep: HTTP(loopback) — outbound http call
        let result = call.call(&permit, &payload).await;
        assert_eq!(result, Err(ErrorCode::Forbidden));

        let accepted = tokio::time::timeout(StdDuration::from_millis(200), listener.accept()).await;
        assert!(
            accepted.is_err(),
            "a non-retrieval purpose must be rejected before any connection is attempted, \
             never silently borrow the platform retrieval credential"
        );
    }

    /// §7.4 scheme guard: a non-`https://`, non-loopback endpoint must be refused at
    /// construction time, before any credential is ever resolved or sent.
    #[test]
    fn insecure_non_loopback_endpoint_is_rejected_at_construction() {
        let result = HttpExternalCall::new(
            "http://attacker.example/x",
            ProcessorId(Uuid::now_v7()),
            HttpEgressConfig::default(),
            std::sync::Arc::new(FixedCredential),
            test_status_classifier,
        );
        assert!(matches!(
            result,
            Err(HttpExternalCallError::InsecureEndpoint)
        ));
    }

    #[test]
    fn https_endpoint_is_accepted() {
        let result = HttpExternalCall::new(
            "https://dashscope.aliyuncs.com/v1/embeddings",
            ProcessorId(Uuid::now_v7()),
            HttpEgressConfig::default(),
            std::sync::Arc::new(FixedCredential),
            test_status_classifier,
        );
        assert!(result.is_ok());
    }

    /// §19.2 / §42 blocker 注错红转绿: before `status_classifier` existed, every non-2xx
    /// status collapsed into one `ProviderPermanent`, making a rejected/expired key
    /// indistinguishable from a provider outage — `map_dashscope_status`
    /// (`retrieval-provider::adapters`) had zero live callers. A local listener replying
    /// 401/429/500 in turn must now surface three DISTINCT `ErrorCode`s, proving the raw
    /// status actually reaches the caller-supplied classifier rather than being discarded.
    #[tokio::test]
    async fn distinct_non_2xx_statuses_classify_to_distinct_error_codes() {
        async fn call_with_status(status: u16) -> Result<Vec<u8>, ErrorCode> {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let processor = ProcessorId(Uuid::now_v7());

            let server = tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                let response = format!(
                    "HTTP/1.1 {status} X\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            });

            let payload = AuthorizedEgressPayload::new(b"x".to_vec());
            let permit = authorize(
                TenantId::new(),
                processor,
                PrivateDataPurpose::RetrievalEmbedding,
                DataClass::Private,
                &payload,
                StdDuration::from_secs(60),
            )
            .unwrap();
            let call = call_at(addr, processor);
            // dep: HTTP(loopback) — outbound http call
            let result = call.call(&permit, &payload).await;
            server.abort();
            result
        }

        let unauthorized = call_with_status(401).await;
        let rate_limited = call_with_status(429).await;
        let server_error = call_with_status(500).await;

        assert_eq!(unauthorized, Err(ErrorCode::Unauthorized));
        assert_eq!(rate_limited, Err(ErrorCode::ProviderRateLimited));
        assert_eq!(server_error, Err(ErrorCode::ProviderTransient));
        assert_ne!(unauthorized, rate_limited);
        assert_ne!(rate_limited, server_error);
        assert_ne!(unauthorized, server_error);
    }

    /// §83.4 point (c): the client must carry a real request timeout — a provider that accepts
    /// the connection and then never sends a response must fail with `ProviderTransient`
    /// within roughly `request_timeout`, not hang indefinitely.
    #[tokio::test]
    async fn hung_provider_is_bounded_by_the_configured_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let processor = ProcessorId(Uuid::now_v7());

        // Accept the connection and then simply never write a response.
        tokio::spawn(async move {
            if let Ok((socket, _)) = listener.accept().await {
                std::mem::forget(socket); // keep the fd open, never respond
            }
        });

        let payload = AuthorizedEgressPayload::new(b"x".to_vec());
        let permit = authorize(
            TenantId::new(),
            processor,
            PrivateDataPurpose::RetrievalEmbedding,
            DataClass::Private,
            &payload,
            StdDuration::from_secs(60),
        )
        .unwrap();

        let config = HttpEgressConfig {
            request_timeout: StdDuration::from_millis(300),
            ..HttpEgressConfig::default()
        };
        let call = HttpExternalCall::new(
            format!("http://{addr}/x"),
            processor,
            config,
            std::sync::Arc::new(FixedCredential),
            test_status_classifier,
        )
        .unwrap();

        // dep: HTTP(loopback) — outbound http call
        let result = tokio::time::timeout(StdDuration::from_secs(5), call.call(&permit, &payload))
            .await
            .expect("the transport's own timeout must fire well before this outer bound");
        assert_eq!(result, Err(ErrorCode::ProviderTransient));
    }

    /// §83.4 point (b): a response body larger than `max_response_bytes` must be rejected
    /// rather than buffered in full.
    #[tokio::test]
    async fn oversized_response_is_rejected_not_buffered() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let processor = ProcessorId(Uuid::now_v7());

        let server = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await; // drain the request, ignore its content
            let oversized_body = "x".repeat(64 * 1024);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                oversized_body.len(),
                oversized_body
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });

        let payload = AuthorizedEgressPayload::new(b"x".to_vec());
        let permit = authorize(
            TenantId::new(),
            processor,
            PrivateDataPurpose::RetrievalEmbedding,
            DataClass::Private,
            &payload,
            StdDuration::from_secs(60),
        )
        .unwrap();

        let config = HttpEgressConfig {
            max_response_bytes: 1024, // far below the 64 KiB the fake server sends
            ..HttpEgressConfig::default()
        };
        let call = HttpExternalCall::new(
            format!("http://{addr}/x"),
            processor,
            config,
            std::sync::Arc::new(FixedCredential),
            test_status_classifier,
        )
        .unwrap();

        // dep: HTTP(loopback) — outbound http call
        let result = call.call(&permit, &payload).await;
        assert_eq!(result, Err(ErrorCode::ProviderPermanent));
        server.abort();
    }

    /// §73.5 "日志只记录 fingerprint/prefix，不记录 secret" — the structural half: `Debug`
    /// itself must never be able to print the value, independent of whether any call site
    /// remembers not to log it.
    #[test]
    fn egress_secret_debug_never_prints_the_value() {
        let secret = EgressSecret::new("do-not-print-me".to_string());
        let debug = format!("{secret:?}");
        assert!(!debug.contains("do-not-print-me"));
        assert_eq!(debug, "EgressSecret(redacted)");
    }

    /// §19.2 / §53 注错红转绿: a credential value `reqwest::header::HeaderValue::from_str`
    /// rejects (the normal shape for a key pasted with a trailing newline from a file/secret
    /// mount) used to surface as `ProviderTransient` from the old `.header(..., format!(...))
    /// + blanket `map_err` — a permanent operator misconfiguration reported, and retried
    /// forever, as if it were a network fault. It must instead be `Internal`, and — since the
    /// header is now validated before any request is built — cause zero network connections.
    #[tokio::test]
    async fn malformed_credential_is_a_config_error_not_a_transient_one() {
        struct NewlineCredential;
        #[async_trait::async_trait]
        impl RetrievalCredentialSource for NewlineCredential {
            async fn credential(&self) -> Result<EgressSecret, ErrorCode> {
                Ok(EgressSecret::new("bad-key\n".to_string()))
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let processor = ProcessorId(Uuid::now_v7());

        let payload = AuthorizedEgressPayload::new(b"x".to_vec());
        let permit = authorize(
            TenantId::new(),
            processor,
            PrivateDataPurpose::RetrievalEmbedding,
            DataClass::Private,
            &payload,
            StdDuration::from_secs(60),
        )
        .unwrap();

        let call = HttpExternalCall::new(
            format!("http://{addr}/x"),
            processor,
            HttpEgressConfig::default(),
            std::sync::Arc::new(NewlineCredential),
            test_status_classifier,
        )
        .unwrap();
        // dep: HTTP(loopback) — outbound http call
        let result = call.call(&permit, &payload).await;
        assert_eq!(result, Err(ErrorCode::Internal));

        let accepted = tokio::time::timeout(StdDuration::from_millis(200), listener.accept()).await;
        assert!(
            accepted.is_err(),
            "a malformed credential must be rejected before any connection is attempted"
        );
    }

    #[tokio::test]
    async fn env_credential_source_reads_a_set_variable() {
        let var = "HUMAUX_TEST_INFRA_EGRESS_ENV_CREDENTIAL_SOURCE_READS_A_SET_VARIABLE";
        // SAFETY: test-only, a process-unique variable name this test both sets and clears —
        // no other test in this crate names it (edition 2024 requires `unsafe` here because
        // `env::set_var` is not thread-safe against a concurrent *read* of the same variable
        // from another thread; no other thread in this test binary reads this name).
        unsafe { std::env::set_var(var, "shh") };
        let source = EnvCredentialSource::new(var);
        let secret = source.credential().await.expect("var is set and non-empty");
        assert_eq!(secret.expose(), "shh");
        unsafe { std::env::remove_var(var) };
    }

    #[tokio::test]
    async fn env_credential_source_fails_closed_when_variable_is_unset() {
        let var = "HUMAUX_TEST_INFRA_EGRESS_ENV_CREDENTIAL_SOURCE_FAILS_CLOSED_WHEN_UNSET";
        // SAFETY: see previous test.
        unsafe { std::env::remove_var(var) }; // ensure clean regardless of run order
        let source = EnvCredentialSource::new(var);
        let result = source.credential().await;
        assert_eq!(result.err(), Some(ErrorCode::Internal));
    }
}
