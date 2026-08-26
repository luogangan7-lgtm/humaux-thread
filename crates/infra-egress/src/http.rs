//! `infra-egress::http` — §83.4's sole raw HTTP transport construction point (G80-3).
//!
//! Every other file in the workspace is forbidden from naming `reqwest::Client::new`/
//! `::builder` (or `hyper::Client::new`/`::builder`) directly — `xtask architecture-check`'s
//! G80-3 判据1 asserts the raw-client construction-site set equals exactly
//! `{crates/infra-egress/src/http.rs}` (both the fully-qualified spelling and a bare
//! `use`-imported alias — `Client::new()` without the `reqwest::` prefix — are covered).
//! [`HttpExternalCall`] is this module's one `humaux_domain::egress::ExternalCall`
//! implementation, and the only place in the workspace permitted to hold that client.

use std::time::Duration;

use humaux_domain::egress::{AuthorizedEgressPayload, EgressPermit, ExternalCall, ProcessorId};
use humaux_domain::error::ErrorCode;

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
}

impl HttpExternalCall {
    /// The workspace's one legal `reqwest::Client` construction site (§83.4 G80-3).
    ///
    /// Fails only if the TLS backend cannot initialize (`reqwest::Client::builder().build()`'s
    /// own failure mode) — a process-startup-time configuration error, not a per-call one.
    pub fn new(
        endpoint: impl Into<String>,
        processor: ProcessorId,
        config: HttpEgressConfig,
    ) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()?;
        Ok(Self {
            client,
            endpoint: endpoint.into(),
            processor,
            config,
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

        let mut response = self
            .client
            .post(&self.endpoint)
            .body(payload.bytes().to_vec())
            .send()
            .await
            .map_err(|_| ErrorCode::ProviderTransient)?;

        if !response.status().is_success() {
            return Err(ErrorCode::ProviderPermanent);
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
    use humaux_domain::egress::{PrivateDataPurpose, authorize};
    use humaux_domain::ids::TenantId;
    use std::time::Duration as StdDuration;
    use tokio::net::TcpListener;
    use uuid::Uuid;

    fn call_at(addr: std::net::SocketAddr, processor: ProcessorId) -> HttpExternalCall {
        HttpExternalCall::new(
            format!("http://{addr}/x"),
            processor,
            HttpEgressConfig::default(),
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
            PrivateDataPurpose::UserReasoning,
            DataClass::Private,
            &original,
            StdDuration::from_secs(60),
        )
        .unwrap();
        let swapped = AuthorizedEgressPayload::new(b"a different payload entirely".to_vec());

        let call = call_at(addr, processor);
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
            PrivateDataPurpose::UserReasoning,
            DataClass::Private,
            &payload,
            StdDuration::from_secs(60),
        )
        .unwrap();

        let call = call_at(addr, processor);
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
            PrivateDataPurpose::UserReasoning,
            DataClass::Private,
            &payload,
            StdDuration::from_secs(60),
        )
        .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let call = call_at(addr, wired_to);
        let result = call.call(&permit, &payload).await;
        assert_eq!(result, Err(ErrorCode::Forbidden));

        let accepted = tokio::time::timeout(StdDuration::from_millis(200), listener.accept()).await;
        assert!(
            accepted.is_err(),
            "a processor mismatch must be rejected before any connection is attempted"
        );
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
            PrivateDataPurpose::UserReasoning,
            DataClass::Private,
            &payload,
            StdDuration::from_secs(60),
        )
        .unwrap();

        let config = HttpEgressConfig {
            request_timeout: StdDuration::from_millis(300),
            ..HttpEgressConfig::default()
        };
        let call = HttpExternalCall::new(format!("http://{addr}/x"), processor, config).unwrap();

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
            PrivateDataPurpose::UserReasoning,
            DataClass::Private,
            &payload,
            StdDuration::from_secs(60),
        )
        .unwrap();

        let config = HttpEgressConfig {
            max_response_bytes: 1024, // far below the 64 KiB the fake server sends
            ..HttpEgressConfig::default()
        };
        let call = HttpExternalCall::new(format!("http://{addr}/x"), processor, config).unwrap();

        let result = call.call(&permit, &payload).await;
        assert_eq!(result, Err(ErrorCode::ProviderPermanent));
        server.abort();
    }
}
