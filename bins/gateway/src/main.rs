//! `humaux-gateway` 进程入口（最小必要进程集见 §4.2；admin 探针契约见 §4.4）。
//!
//! ## Supervision surface (card 15, ADR-0037)
//!
//! Two unauthenticated GET routes a supervisor/load balancer can poll, merged onto the MCP
//! router:
//!
//! | route | 200 means | 503 means |
//! |---|---|---|
//! | `/livez` | this process is running and its accept loop is alive | — (a dead process refuses the connection) |
//! | `/readyz` | bootstrap completed **and** this process is still accepting new work | SIGTERM/Ctrl-C was received; the process is draining and must be taken out of rotation |
//!
//! The 503 is only a real answer if a supervisor can still *reach* it. A k8s
//! `readinessProbe.httpGet` opens a FRESH TCP connection on every poll, so flipping readiness to
//! false and closing the accept loop in the same instant would hand every such supervisor
//! ECONNREFUSED — indistinguishable from a crash, which is the one distinction this route
//! exists to make. [`DRAIN_ANNOUNCE_WINDOW`] is the gap between the two events, and it is the
//! reason the 503 branch is reachable at all.
//!
//! `/readyz` returning 200 is a real statement, not a placeholder: reaching this point means
//! [`GatewayBootstrap::build`] already connected the `role_gateway` pool and verified
//! `current_user` (§6.2.3 assertion E), resolved the effective config, and bound the listener.
//! A process that failed any of those never serves this route at all — the connection is
//! refused, which is the honest "not ready" (§4.4 坑5 in HTTP form: *unreachable* and
//! *reachable-but-unhealthy* must not be the same answer).
//!
//! **Why these two are outside `native_request_boundary`**: they carry no tenant data, no
//! config fingerprint, and no state mutation — a constant token and a status code — so the
//! boundary's Host/Origin allowlist (DNS-rebinding protection for the MCP surface) has nothing
//! to protect here, while a supervisor probing over the pod IP has no Host header the allowlist
//! could be configured to accept. Anything that would report *what* this process is (version,
//! fingerprint, tenant counts) belongs behind the boundary or in `humaux-admin q`, not here.

use std::{
    error::Error,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::{Router, http::StatusCode, routing::get};
use humaux_gateway::bootstrap::GatewayBootstrap;

/// How long the process keeps accepting connections AFTER readiness has gone false, so a
/// supervisor that dials a new connection per poll actually observes `503 draining` instead of
/// the ECONNREFUSED it cannot tell from a crash. Must exceed one readiness poll period; the
/// supervisor's SIGTERM→SIGKILL grace must exceed it plus the longest in-flight request
/// (docs/ops/supervision.md §3).
// ponytail: one constant, not a config key — every `HUMAUX_GATEWAY_*` key is declared in
// `bootstrap::registry()` and feeds the config fingerprint, and a drain window is not a
// fingerprint input. Promote it there if a deployment's readiness period ever exceeds 5s.
const DRAIN_ANNOUNCE_WINDOW: Duration = Duration::from_secs(5);

/// `/livez` + `/readyz`. `accepting` is flipped to `false` by the shutdown future the moment a
/// termination signal arrives, so a request in flight during the graceful drain window still
/// gets served while the next readiness poll already reports 503.
fn supervision_routes(accepting: Arc<AtomicBool>) -> Router {
    Router::new()
        .route("/livez", get(async || (StatusCode::OK, "live\n")))
        .route(
            "/readyz",
            get(async move || {
                if accepting.load(Ordering::SeqCst) {
                    (StatusCode::OK, "ready\n")
                } else {
                    (StatusCode::SERVICE_UNAVAILABLE, "draining\n")
                }
            }),
        )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let runtime = GatewayBootstrap::load_from_env()?.build().await?;
    let listener = tokio::net::TcpListener::bind(runtime.bind_addr()).await?;
    let accepting = Arc::new(AtomicBool::new(true));
    let app = runtime
        .adapter()
        .router()
        .merge(supervision_routes(accepting.clone()));

    // Both handlers are installed before `axum::serve` exists: `tokio::signal::ctrl_c()` only
    // registers SIGINT on the shutdown future's first poll (inside `serve`), so a Ctrl-C landing
    // before that poll would hit SIGINT's default disposition and kill the process undrained.
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let shutdown = async move {
        #[cfg(unix)]
        tokio::select! {
            _ = interrupt.recv() => {},
            _ = terminate.recv() => {},
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
        // Readiness goes false FIRST, and the accept loop stays open for `DRAIN_ANNOUNCE_WINDOW`
        // afterwards. Returning from this future is what stops axum accepting, so without the
        // wait the two events are simultaneous and a supervisor that opens a new connection per
        // poll (the k8s `readinessProbe.httpGet` this route is written for) could only ever get
        // ECONNREFUSED — never the 503 that says "draining, do not restart me".
        accepting.store(false, Ordering::SeqCst);
        eprintln!(
            "humaux-gateway: signal received, /readyz now 503, accepting for {}s more",
            DRAIN_ANNOUNCE_WINDOW.as_secs()
        );
        tokio::time::sleep(DRAIN_ANNOUNCE_WINDOW).await;
        eprintln!("humaux-gateway: drain window elapsed, closing the accept loop");
    };

    eprintln!(
        "humaux-gateway listening on {} config_fingerprint={}",
        listener.local_addr()?,
        runtime.config_fingerprint()
    );
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await?;
    Ok(())
}
