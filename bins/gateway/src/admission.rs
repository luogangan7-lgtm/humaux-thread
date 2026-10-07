//! `gateway::admission` — §67.2 admission control for the MCP router: the whole body is read first (bounded by B and
//!   the body-size key), then a per-credential slot K, the global concurrency C, the waiter cap Q and the wait W;
//!   overflow is HTTP 503 + `Retry-After` + the pre-parse `RATE_LIMITED` body.
//! Depends-on: crates=[axum, humaux-domain, humaux-telemetry, sha2, tokio]; services=[HTTP(loopback)]; env=[];
//!   modules=[domain::error, telemetry::admission]
//! Called-by: [gateway::bootstrap, gateway::main]
//! Invariants: [no admission state is touched before the whole body is read; the permit, the waiter count and the
//!   per-key count are released by Drop on every path (success, refusal, a client that disconnects while queued, a
//!   handler panic); an admitted request's permit and key slot live in the task that runs it until the handler
//!   returns, also when its client disconnects, so C bounds in-flight work and not just connected clients; exactly
//!   one counter increment per 503 and no log line; `/livez` and `/readyz` never pass this layer (`wrap` merges them
//!   after the layer)]
//! Spec: Baseline §67.2; §41.2; §52; ADR-0065 D-C
//!
//! [`Admission::enter`] is the core and returns a [`Refusal`]; the axum function is a thin translation of it, so tests
//! assert the variant directly. The layer sits outside `native_request_boundary`, whose own `to_bytes` returns at once
//! on the already-buffered body (ADR-0065 D-C step 0).

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use humaux_domain::error::ErrorCode;
use humaux_telemetry::admission::count_admission_rejected;
use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Why a request was refused (the closed `reason` label set of `admission_rejected_total`, ADR-0065 D-C).
pub use humaux_telemetry::admission::AdmissionRefusal as Refusal;

/// §67.2: `Retry-After: min(队列估算秒数, 30)`, with a 1 s floor (RFC 9110 §10.2.3 integer seconds; ruling W-8).
const RETRY_AFTER_MAX_SECS: u64 = 30;
// ponytail: EWMA weight 1/8 of the newest service time, fixed; a key only if a Retry-After estimate is ever measured
// to lag a real load shift by more than one refusal round.
const EWMA_SHIFT: u32 = 3;

/// The five admission bounds (ADR-0065 D-C), parsed and checked by `gateway::bootstrap`.
#[derive(Debug, Clone, Copy)]
pub struct AdmissionSettings {
    /// C, `HUMAUX_GATEWAY_ADMISSION_CONCURRENCY` (≥ 1).
    pub concurrency: u32,
    /// Q, `HUMAUX_GATEWAY_ADMISSION_QUEUE_DEPTH` (0 = no waiting).
    pub queue_depth: u32,
    /// W, `HUMAUX_GATEWAY_ADMISSION_MAX_WAIT_MS` (> 0).
    pub max_wait: Duration,
    /// K, `HUMAUX_GATEWAY_ADMISSION_PER_KEY_LIMIT` (1 ≤ K ≤ C + Q): requests one credential may have in flight plus
    /// waiting.
    pub per_key_limit: u32,
    /// B, `HUMAUX_GATEWAY_ADMISSION_BODY_READ_TIMEOUT_MS` (> 0, ≤ the handler timeout).
    pub body_read_timeout: Duration,
    /// `HUMAUX_GATEWAY_MAX_REQUEST_BODY_BYTES`, the same limit `native_request_boundary` applies.
    pub max_request_body_bytes: usize,
}

/// The admission state of one gateway process.
pub struct Admission {
    settings: AdmissionSettings,
    // ponytail: one process-wide semaphore; §67.2 is single node, so a second gateway replica needs shared
    // admission (or per-replica C = C / replicas).
    permits: Arc<Semaphore>,
    waiting: AtomicUsize,
    // ponytail: one Mutex<HashMap> for the per-key counts; at C + Q ≤ 80 entries the lock is uncontended, shard it
    // if C grows past a few hundred.
    keys: Mutex<HashMap<u64, u32>>,
    /// EWMA of admitted service time, microseconds (feeds `Retry-After` only).
    service_ewma_us: AtomicU64,
}

/// An admitted request: holds one permit and one per-key slot until dropped. Owned (no borrow of [`Admission`]), so
/// the task that runs the request can hold it to the end of the handler (ADR-0065 D-C).
pub struct Ticket {
    _permit: OwnedSemaphorePermit,
    _slot: KeySlot,
}

struct KeySlot {
    admission: Arc<Admission>,
    key: u64,
}

impl Drop for KeySlot {
    fn drop(&mut self) {
        let mut keys = self
            .admission
            .keys
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(n) = keys.get_mut(&self.key) {
            *n -= 1;
            if *n == 0 {
                keys.remove(&self.key);
            }
        }
    }
}

/// One counted waiter; the count drops with it, so a cancelled `enter` frees its queue slot.
struct WaitSlot<'a>(&'a AtomicUsize);

impl Drop for WaitSlot<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Admission {
    /// A fresh admission state with C permits.
    #[must_use]
    pub fn new(settings: AdmissionSettings) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(settings.concurrency as usize)),
            settings,
            waiting: AtomicUsize::new(0),
            keys: Mutex::new(HashMap::new()),
            service_ewma_us: AtomicU64::new(0),
        }
    }

    /// ADR-0065 D-C steps 1-4: the per-key slot, then a free permit, else a counted wait of at most W.
    ///
    /// # Errors
    /// [`Refusal::KeyLimit`], [`Refusal::QueueFull`] or [`Refusal::WaitTimeout`]; nothing is held after an error.
    pub async fn enter(self: &Arc<Self>, key: u64) -> Result<Ticket, Refusal> {
        let slot = self.take_key(key)?;
        // tokio hands a released permit to the first queued waiter, so this never jumps the queue.
        if let Ok(permit) = Arc::clone(&self.permits).try_acquire_owned() {
            return Ok(Ticket {
                _permit: permit,
                _slot: slot,
            });
        }
        let waiter = WaitSlot(&self.waiting);
        if self.waiting.fetch_add(1, Ordering::SeqCst) >= self.settings.queue_depth as usize {
            return Err(Refusal::QueueFull);
        }
        let permit = tokio::time::timeout(
            self.settings.max_wait,
            Arc::clone(&self.permits).acquire_owned(),
        )
        .await
        .ok()
        .and_then(Result::ok)
        .ok_or(Refusal::WaitTimeout)?;
        drop(waiter);
        Ok(Ticket {
            _permit: permit,
            _slot: slot,
        })
    }

    fn take_key(self: &Arc<Self>, key: u64) -> Result<KeySlot, Refusal> {
        let mut keys = self.keys.lock().unwrap_or_else(PoisonError::into_inner);
        let n = keys.entry(key).or_insert(0);
        if *n >= self.settings.per_key_limit {
            return Err(Refusal::KeyLimit);
        }
        *n += 1;
        Ok(KeySlot {
            admission: Arc::clone(self),
            key,
        })
    }

    fn observe(&self, service: Duration) {
        let sample = u64::try_from(service.as_micros()).unwrap_or(u64::MAX);
        // A lost update between two concurrent requests only skips one sample of a Retry-After hint.
        let old = self.service_ewma_us.load(Ordering::Relaxed);
        let next = if old == 0 {
            sample
        } else {
            old - (old >> EWMA_SHIFT) + (sample >> EWMA_SHIFT)
        };
        self.service_ewma_us.store(next, Ordering::Relaxed);
    }

    /// §67.2 queue estimate in whole seconds: waiting × mean service time / C, clamped to 1..=30.
    fn retry_after_secs(&self) -> u64 {
        let waiting = self.waiting.load(Ordering::SeqCst) as u64;
        let per_second = u64::from(self.settings.concurrency) * 1_000_000;
        waiting
            .saturating_mul(self.service_ewma_us.load(Ordering::Relaxed))
            .div_ceil(per_second)
            .clamp(1, RETRY_AFTER_MAX_SECS)
    }

    /// The overflow response: 503, `Retry-After`, and the pre-parse `RATE_LIMITED` text body (no JSON-RPC envelope;
    /// mcp.rs's preflight refusal shape). Counted once; deliberately not logged (ADR-0065 D-C rejected h).
    fn refuse(&self, reason: Refusal) -> Response {
        count_admission_rejected(reason);
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [
                (header::RETRY_AFTER, self.retry_after_secs().to_string()),
                (header::CONTENT_TYPE, "text/plain".to_owned()),
            ],
            ErrorCode::RateLimited.as_str(),
        )
            .into_response()
    }
}

/// The fairness key: the first 8 bytes of SHA-256 of the raw `Authorization` value; a missing header is key 0. It is
/// unverified on purpose — it decides fairness, never authorization (ADR-0065 D-C, L3) — and never logged.
fn fairness_key(authorization: Option<&HeaderValue>) -> u64 {
    authorization.map_or(0, |value| {
        let digest = Sha256::digest(value.as_bytes());
        let mut first = [0_u8; 8];
        first.copy_from_slice(&digest[..8]);
        u64::from_be_bytes(first)
    })
}

async fn admit(State(admission): State<Arc<Admission>>, request: Request, next: Next) -> Response {
    let (parts, body) = request.into_parts();
    // ADR-0065 D-C step 0: a slow body holds no permit, no queue slot and no key slot, and is cut off after B.
    // ponytail: the body is buffered before admission, bounded by MAX_REQUEST_BODY_BYTES and B per connection as
    // before card 38; a connection cap belongs at the listener if open connections ever become the limit.
    let settings = admission.settings;
    let bytes = match tokio::time::timeout(
        settings.body_read_timeout,
        to_bytes(body, settings.max_request_body_bytes),
    )
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(_)) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        Err(_) => {
            return (StatusCode::REQUEST_TIMEOUT, [(header::CONNECTION, "close")]).into_response();
        }
    };
    let key = fairness_key(parts.headers.get(header::AUTHORIZATION));
    let ticket = match admission.enter(key).await {
        Ok(ticket) => ticket,
        Err(reason) => return admission.refuse(reason),
    };
    let request = Request::from_parts(parts, Body::from(bytes));
    // ADR-0065 D-C (review P1): the admitted work runs in its own task, which owns the ticket. rmcp's stateless mode
    // spawns every handler detached and nothing reads its cancellation token, so a client that disconnects drops this
    // future but not the work (rate batch, quota reservation, tool body). Holding the ticket here would free the
    // permit and the key slot while that work still runs, and C would bound connected clients, not gateway / PG work.
    let work = tokio::spawn(async move {
        let started = Instant::now();
        // rmcp runs with `with_json_response(true)`: the response is complete when `next.run` returns (F10).
        let response = next.run(request).await;
        admission.observe(started.elapsed());
        drop(ticket);
        response
    });
    match work.await {
        Ok(response) => response,
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        // Only a runtime shutting down cancels the task.
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

/// The one wrap site (`main`): the admission layer on the MCP router only, then the supervision routes merged
/// outside it, so `/livez` and `/readyz` (the drain 503) never wait on admission.
pub fn wrap(mcp: Router, supervision: Router, admission: Arc<Admission>) -> Router {
    mcp.layer(middleware::from_fn_with_state(admission, admit))
        .merge(supervision)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::{get, post};
    use humaux_telemetry::admission::admission_rejected_total;
    use std::net::SocketAddr;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    /// The counter is process-wide, so every test that can refuse runs alone and asserts exact deltas.
    static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn settings(
        concurrency: u32,
        queue_depth: u32,
        max_wait_ms: u64,
        per_key: u32,
    ) -> AdmissionSettings {
        AdmissionSettings {
            concurrency,
            queue_depth,
            max_wait: Duration::from_millis(max_wait_ms),
            per_key_limit: per_key,
            body_read_timeout: Duration::from_secs(10),
            max_request_body_bytes: 65_536,
        }
    }

    /// A stub MCP route that holds every request until `release` gets permits, counting entries in `entered`.
    struct Stub {
        entered: Arc<AtomicUsize>,
        release: Arc<Semaphore>,
    }

    impl Stub {
        fn new() -> Self {
            Self {
                entered: Arc::new(AtomicUsize::new(0)),
                release: Arc::new(Semaphore::new(0)),
            }
        }

        fn router(&self) -> Router {
            let (entered, release) = (self.entered.clone(), self.release.clone());
            Router::new().route(
                "/mcp",
                post(move || async move {
                    entered.fetch_add(1, Ordering::SeqCst);
                    let _held = release.acquire().await;
                    "ok"
                }),
            )
        }

        async fn wait_entered(&self, n: usize) {
            let deadline = Instant::now() + Duration::from_secs(5);
            while self.entered.load(Ordering::SeqCst) < n {
                assert!(
                    Instant::now() < deadline,
                    "only {} of {n} requests entered",
                    self.entered.load(Ordering::SeqCst)
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }

        fn release_all(&self) {
            self.release.add_permits(Semaphore::MAX_PERMITS / 2);
        }
    }

    fn supervision() -> Router {
        Router::new()
            .route("/livez", get(async || "live\n"))
            .route("/readyz", get(async || "{\"status\":\"ready\"}\n"))
    }

    async fn serve(admission: Arc<Admission>, mcp: Router) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let app = wrap(mcp, supervision(), admission);
        tokio::spawn(async move { axum::serve(listener, app).await });
        addr
    }

    struct Reply {
        status: u16,
        headers: String,
        body: String,
    }

    /// One raw HTTP/1.1 exchange with `Connection: close`; `None` if nothing came back within `within`.
    async fn exchange(
        addr: SocketAddr,
        head: &str,
        body: &[u8],
        within: Duration,
    ) -> Option<Reply> {
        // dep: HTTP(loopback) — the test's own stub listener on 127.0.0.1:0
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream.write_all(head.as_bytes()).await.expect("head");
        stream.write_all(body).await.expect("body");
        let mut raw = Vec::new();
        tokio::time::timeout(within, stream.read_to_end(&mut raw))
            .await
            .ok()?
            .ok()?;
        let text = String::from_utf8_lossy(&raw).into_owned();
        let (head, body) = text.split_once("\r\n\r\n")?;
        let status = head.split(' ').nth(1)?.parse().ok()?;
        Some(Reply {
            status,
            headers: head.to_ascii_lowercase(),
            body: body.to_owned(),
        })
    }

    fn post_head(path: &str, authorization: Option<&str>, content_length: usize) -> String {
        let auth = authorization.map_or_else(String::new, |a| format!("Authorization: {a}\r\n"));
        format!(
            "POST {path} HTTP/1.1\r\nHost: t\r\n{auth}Content-Length: {content_length}\r\nConnection: close\r\n\r\n"
        )
    }

    async fn call(addr: SocketAddr, authorization: String, within: Duration) -> Option<Reply> {
        exchange(
            addr,
            &post_head("/mcp", Some(&authorization), 2),
            b"{}",
            within,
        )
        .await
    }

    fn deltas(before: [u64; 3]) -> [u64; 3] {
        let now = counts();
        [now[0] - before[0], now[1] - before[1], now[2] - before[2]]
    }

    fn counts() -> [u64; 3] {
        Refusal::ALL.map(admission_rejected_total)
    }

    /// §67.2 Q = 0 means no waiting: with 16 distinct credentials held at C = 16, the 17th distinct credential is
    /// refused at once as `queue_full`, never `key_limit`. Fault: the waiter-cap check removed ⇒ it waits W ⇒ red.
    #[tokio::test]
    async fn queue_zero_rejects_the_seventeenth_at_once() {
        let _serial = SERIAL.lock().await;
        let admission = Arc::new(Admission::new(settings(16, 0, 2000, 16)));
        let stub = Stub::new();
        let addr = serve(admission.clone(), stub.router()).await;
        let before = counts();
        let held: Vec<_> = (0..16)
            .map(|i| tokio::spawn(call(addr, format!("Bearer k{i}"), Duration::from_secs(10))))
            .collect();
        stub.wait_entered(16).await;
        let started = Instant::now();
        let reply = call(addr, "Bearer k16".into(), Duration::from_secs(5))
            .await
            .expect("17th reply");
        let took = started.elapsed();
        assert_eq!(reply.status, 503, "the 17th is refused");
        assert!(
            took < Duration::from_millis(100),
            "refused at once, took {took:?}"
        );
        assert!(
            matches!(admission.enter(17).await, Err(Refusal::QueueFull)),
            "the refusal is QueueFull"
        );
        assert_eq!(
            deltas(before),
            [1, 0, 0],
            "queue_full = 1, wait_timeout = 0, key_limit = 0"
        );
        stub.release_all();
        for h in held {
            assert_eq!(h.await.expect("join").expect("reply").status, 200);
        }
    }

    /// §67.2 overflow shape: C = 1, Q = 1, W = 200 ms — the waiter gets 503 with an integer `Retry-After` in 1..=30,
    /// `Content-Type: text/plain` and the bare body `RATE_LIMITED`. Fault: the header dropped ⇒ red.
    #[tokio::test]
    async fn a_waiter_past_max_wait_gets_503_retry_after_rate_limited() {
        let _serial = SERIAL.lock().await;
        let admission = Arc::new(Admission::new(settings(1, 1, 200, 2)));
        let stub = Stub::new();
        let addr = serve(admission, stub.router()).await;
        let before = counts();
        let held = tokio::spawn(call(addr, "Bearer a".into(), Duration::from_secs(10)));
        stub.wait_entered(1).await;
        let started = Instant::now();
        let reply = call(addr, "Bearer b".into(), Duration::from_secs(5))
            .await
            .expect("waiter reply");
        assert!(
            started.elapsed() >= Duration::from_millis(200),
            "it waited W first"
        );
        assert_eq!(reply.status, 503);
        let retry: u64 = reply
            .headers
            .lines()
            .find_map(|l| l.strip_prefix("retry-after: "))
            .expect("a Retry-After header")
            .trim()
            .parse()
            .expect("integer seconds");
        assert!((1..=30).contains(&retry), "Retry-After {retry}");
        assert!(
            reply.headers.contains("content-type: text/plain"),
            "{}",
            reply.headers
        );
        assert_eq!(reply.body, "RATE_LIMITED", "no JSON-RPC envelope");
        assert_eq!(deltas(before), [0, 1, 0], "wait_timeout = 1");
        stub.release_all();
        assert_eq!(held.await.expect("join").expect("reply").status, 200);
    }

    /// §42 AdmissionRejected's own test: admission capped at 1, two concurrent requests ⇒ the counter grows by exactly
    /// 1. Fault: the increment removed ⇒ red.
    #[tokio::test]
    async fn cap_one_two_requests_counts_one_rejection() {
        let _serial = SERIAL.lock().await;
        let admission = Arc::new(Admission::new(settings(1, 0, 200, 1)));
        let stub = Stub::new();
        let addr = serve(admission, stub.router()).await;
        let before = counts();
        let held = tokio::spawn(call(addr, "Bearer one".into(), Duration::from_secs(10)));
        stub.wait_entered(1).await;
        let reply = call(addr, "Bearer two".into(), Duration::from_secs(5))
            .await
            .expect("reply");
        assert_eq!(reply.status, 503);
        assert_eq!(
            deltas(before).iter().sum::<u64>(),
            1,
            "exactly one rejection counted"
        );
        stub.release_all();
        assert_eq!(held.await.expect("join").expect("reply").status, 200);
    }

    /// A client that goes away while queued frees its queue slot (the waiter count is a Drop guard). Fault: the guard
    /// replaced by a decrement after the await ⇒ the cancelled waiter keeps Q full ⇒ the next waiter is `QueueFull`.
    #[tokio::test]
    async fn a_dropped_waiter_frees_its_queue_slot() {
        let _serial = SERIAL.lock().await;
        let admission = Arc::new(Admission::new(settings(1, 1, 5000, 3)));
        let first = admission.enter(1).await.expect("the one permit");
        let queued = {
            let admission = admission.clone();
            tokio::spawn(async move { admission.enter(2).await.map(|_| ()) })
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while admission.waiting.load(Ordering::SeqCst) < 1 {
            assert!(Instant::now() < deadline, "the second request never queued");
            tokio::task::yield_now().await;
        }
        queued.abort();
        assert!(queued.await.expect_err("aborted").is_cancelled());
        assert_eq!(
            admission.waiting.load(Ordering::SeqCst),
            0,
            "the cancelled waiter left the queue"
        );
        let next = {
            let admission = admission.clone();
            tokio::spawn(async move { admission.enter(3).await.map(|_| ()) })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(first);
        assert_eq!(
            next.await.expect("join"),
            Ok(()),
            "the next waiter queued and was admitted"
        );
        assert!(
            admission.keys.lock().expect("keys").is_empty(),
            "every key slot released"
        );
    }

    /// ADR-0065 D-C (review P1): a client that sends its whole request and then disconnects keeps its permit until its
    /// handler ends, because the gateway's handlers outlive the request future (rmcp detaches them). C 1, Q 0: while
    /// the first handler still runs, a second credential is refused `queue_full`; once the handler ends the permit
    /// comes back. Fault: `next.run` awaited in the request future ⇒ the disconnect drops the ticket ⇒ the second
    /// request is admitted while the first handler still runs ⇒ red.
    #[tokio::test]
    async fn a_client_that_disconnects_keeps_its_permit_until_the_handler_ends() {
        let _serial = SERIAL.lock().await;
        let admission = Arc::new(Admission::new(settings(1, 0, 200, 1)));
        let stub = Stub::new();
        let addr = serve(admission.clone(), stub.router()).await;
        let before = counts();
        // dep: HTTP(loopback) — the test's own stub listener on 127.0.0.1:0
        let mut gone = TcpStream::connect(addr).await.expect("connect");
        gone.write_all(post_head("/mcp", Some("Bearer gone"), 2).as_bytes())
            .await
            .expect("head");
        gone.write_all(b"{}").await.expect("body");
        stub.wait_entered(1).await;
        drop(gone);
        // Long enough for the server's connection task to read the EOF and drop the request future.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let reply = call(addr, "Bearer next".into(), Duration::from_secs(5))
            .await
            .expect("a reply while the first handler still runs");
        assert_eq!(
            (reply.status, stub.entered.load(Ordering::SeqCst)),
            (503, 1),
            "the disconnected client's handler still holds the one permit"
        );
        assert_eq!(deltas(before), [1, 0, 0], "queue_full = 1");
        stub.release_all();
        let deadline = Instant::now() + Duration::from_secs(5);
        while admission.permits.available_permits() < 1 {
            assert!(
                Instant::now() < deadline,
                "the handler never gave its permit back"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let reply = call(addr, "Bearer next".into(), Duration::from_secs(5))
            .await
            .expect("reply");
        assert_eq!(reply.status, 200, "admitted once the handler ended");
        assert!(
            admission.keys.lock().expect("keys").is_empty(),
            "every key slot released"
        );
    }

    /// K bounds one credential while another is still admitted. Fault: the per-key check removed ⇒ red.
    #[tokio::test]
    async fn one_key_at_its_limit_is_rejected_while_another_key_is_admitted() {
        let _serial = SERIAL.lock().await;
        let admission = Arc::new(Admission::new(settings(4, 4, 200, 2)));
        let a = fairness_key(Some(&HeaderValue::from_static("Bearer a")));
        let b = fairness_key(Some(&HeaderValue::from_static("Bearer b")));
        assert_ne!(a, b);
        let held = [
            admission.enter(a).await.expect("a1"),
            admission.enter(a).await.expect("a2"),
        ];
        assert!(
            matches!(admission.enter(a).await, Err(Refusal::KeyLimit)),
            "a third request of key a"
        );
        let other = admission.enter(b).await;
        assert!(other.is_ok(), "key b is admitted while a is at K");
        drop((held, other));
        assert!(admission.enter(a).await.is_ok(), "a's slots came back");
    }

    /// ADR-0065 D-C step 0 (review R1): 16 connections that send headers and one body byte, then stall, hold no
    /// permit, so a 17th complete request is served at C = 16. Fault: the body read moved after the permit ⇒ the 17th
    /// waits W and gets 503 ⇒ red.
    #[tokio::test]
    async fn slow_body_clients_hold_no_permit() {
        let _serial = SERIAL.lock().await;
        let admission = Arc::new(Admission::new(settings(16, 64, 1000, 16)));
        let fast = Router::new().route("/mcp", post(async || "ok"));
        let addr = serve(admission, fast).await;
        let mut stalled = Vec::new();
        for i in 0..16 {
            // dep: HTTP(loopback) — the test's own stub listener on 127.0.0.1:0
            let mut s = TcpStream::connect(addr).await.expect("connect");
            s.write_all(post_head("/mcp", Some(&format!("Bearer s{i}")), 1000).as_bytes())
                .await
                .expect("head");
            s.write_all(b"{").await.expect("one byte");
            stalled.push(s);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        let started = Instant::now();
        let reply = call(addr, "Bearer fast".into(), Duration::from_secs(5))
            .await
            .expect("reply");
        assert_eq!(reply.status, 200, "served while 16 bodies stall");
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "took {:?}",
            started.elapsed()
        );
        drop(stalled);
    }

    /// B bounds the body read: a stalled body gets 408 + `Connection: close` after B = 200 ms. Fault: the timeout
    /// removed ⇒ no response within the test's own 2 s ⇒ red.
    #[tokio::test]
    async fn a_stalled_body_gets_408_after_the_body_read_timeout() {
        let _serial = SERIAL.lock().await;
        let mut s = settings(1, 0, 200, 1);
        s.body_read_timeout = Duration::from_millis(200);
        let fast = Router::new().route("/mcp", post(async || "ok"));
        let addr = serve(Arc::new(Admission::new(s)), fast).await;
        // dep: HTTP(loopback) — the test's own stub listener on 127.0.0.1:0
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream
            .write_all(post_head("/mcp", Some("Bearer slow"), 1000).as_bytes())
            .await
            .expect("head");
        stream.write_all(b"{").await.expect("one byte");
        let mut raw = Vec::new();
        let got = tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut raw)).await;
        assert!(got.is_ok(), "no response within 2 s");
        let text = String::from_utf8_lossy(&raw).to_ascii_lowercase();
        assert!(text.starts_with("http/1.1 408"), "{text}");
        assert!(text.contains("connection: close"), "{text}");
    }

    /// `/livez` and `/readyz` are merged outside the layer: with C = 1 held and Q = 0 both still answer 200. Fault:
    /// the layer applied after the merge ⇒ they get 503 ⇒ red.
    #[tokio::test]
    async fn supervision_routes_bypass_admission() {
        let _serial = SERIAL.lock().await;
        let admission = Arc::new(Admission::new(settings(1, 0, 200, 1)));
        let stub = Stub::new();
        let addr = serve(admission, stub.router()).await;
        let held = tokio::spawn(call(addr, "Bearer h".into(), Duration::from_secs(10)));
        stub.wait_entered(1).await;
        for path in ["/readyz", "/livez"] {
            let head = format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
            let reply = exchange(addr, &head, b"", Duration::from_secs(2))
                .await
                .expect("reply");
            assert_eq!(reply.status, 200, "{path} while admission is full");
        }
        stub.release_all();
        assert_eq!(held.await.expect("join").expect("reply").status, 200);
    }
}
