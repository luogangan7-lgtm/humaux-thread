//! `gateway::main` — `humaux-gateway` 进程入口（最小必要进程集见 §4.2；admin 探针契约见 §4.4）。
//! Depends-on: crates=[axum, humaux-telemetry, tokio]; services=[HTTP(loopback)]; env=[]; modules=[gateway::bootstrap,
//!   gateway::admission, gateway::guard, gateway::status, telemetry::metrics]
//! Called-by: [process(humaux-gateway)]
//! Invariants: [/readyz flips to 503 and the accept loop keeps draining for DRAIN_ANNOUNCE_WINDOW before closing, so a k8s readinessProbe never sees ECONNREFUSED confused with a crash; the ops listener stays up through the drain window and closes after it; `--metrics-families` reads no configuration]
//! Spec: Baseline §4.2; §4.4; §67.2; ADR-0037; ADR-0061 D-B; ADR-0061 D-F; ADR-0065 D-C
//!
//! ## Supervision surface (card 15, ADR-0037)
//!
//! Two unauthenticated GET routes a supervisor/load balancer can poll, merged onto the MCP
//! router:
//!
//! | route | 200 means | 503 means |
//! |---|---|---|
//! | `/livez` | this process is running and its accept loop is alive | — (a dead process refuses the connection) |
//! | `/readyz` | bootstrap completed, this process is still accepting new work, and the last readiness snapshot (PG, the retrieval RPC round trip, Qdrant; refreshed every `HUMAUX_GATEWAY_READINESS_REFRESH_SECONDS`) is all pass/not_applicable and ≤ 2 intervals old | body `{"status": "not_ready" \| "stale" \| "draining"}` — never a dependency name; the loopback `/status` names it (ADR-0061 D-F) |
//!
//! The 503 is only a real answer if a supervisor can still *reach* it. A k8s
//! `readinessProbe.httpGet` opens a FRESH TCP connection on every poll, so flipping readiness to
//! false and closing the accept loop in the same instant would hand every such supervisor
//! ECONNREFUSED — indistinguishable from a crash, which is the one distinction this route
//! exists to make. [`DRAIN_ANNOUNCE_WINDOW`] is the gap between the two events, and it is the
//! reason the 503 branch is reachable at all.
//!
//! `/readyz` returning 200 is a real statement: [`GatewayBootstrap::build`] connected the
//! `role_gateway` pool and verified `current_user` (§6.2.3 assertion E), and the cached snapshot
//! (`gateway::status`) says every hard dependency answered a real round trip within the last two
//! refresh intervals (ADR-0061 D-F, OPS-8: before card 34 it was 200 forever after boot). A
//! process that failed boot never serves this route at all — the connection is refused, which is
//! the honest "not ready" (§4.4 坑5 in HTTP form: *unreachable* and *reachable-but-unhealthy* must
//! not be the same answer). `/metrics` and `/status` are NOT on this listener: they live on the
//! loopback ops listener `HUMAUX_GATEWAY_METRICS_ADDR` (ADR-0061 D-B, E5), because this one sits
//! behind the reverse proxy.
//!
//! **Why these two are outside `native_request_boundary`**: they carry no tenant data, no
//! config fingerprint, and no state mutation — a constant token and a status code — so the
//! boundary's Host/Origin allowlist (DNS-rebinding protection for the MCP surface) has nothing
//! to protect here, while a supervisor probing over the pod IP has no Host header the allowlist
//! could be configured to accept. Anything that would report *what* this process is (version,
//! fingerprint, tenant counts) belongs behind the boundary or in `humaux-admin q`, not here.

use std::{error::Error, net::SocketAddr, sync::Arc, time::Duration};

use axum::{Router, http::StatusCode, routing::get};
use humaux_gateway::{
    admission,
    bootstrap::GatewayBootstrap,
    guard::GuardMetrics,
    status::{METRICS_ADDR_KEY, Readiness, Verdict, render_metrics},
};
use humaux_telemetry::metrics::{Routes, serve_loopback};

/// How long the process keeps accepting connections AFTER readiness has gone false, so a
/// supervisor that dials a new connection per poll actually observes `503 draining` instead of
/// the ECONNREFUSED it cannot tell from a crash. Must exceed one readiness poll period; the
/// supervisor's SIGTERM→SIGKILL grace must exceed it + the body-read bound B + the admission wait W + the handler and
/// finalize timeouts (ADR-0065 D-C; docs/ops/supervision.md §3).
// ponytail: one constant, not a config key — every `HUMAUX_GATEWAY_*` key is declared in
// `bootstrap::registry()` and feeds the config fingerprint, and a drain window is not a
// fingerprint input. Promote it there if a deployment's readiness period ever exceeds 5s.
const DRAIN_ANNOUNCE_WINDOW: Duration = Duration::from_secs(5);

/// `/livez` + `/readyz`. The readiness verdict is read from the cached snapshot (never a
/// dependency call per request), and the body is a status word only (ADR-0061 D-F).
fn supervision_routes(readiness: Arc<Readiness>) -> Router {
    Router::new()
        .route("/livez", get(async || (StatusCode::OK, "live\n")))
        .route(
            "/readyz",
            get(async move || {
                let verdict = readiness.verdict();
                let code = if verdict == Verdict::Ready {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                };
                (
                    code,
                    [(axum::http::header::CONTENT_TYPE, "application/json")],
                    format!("{{\"status\":\"{}\"}}\n", verdict.as_str()),
                )
            }),
        )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // ADR-0061 D-C: the zero-state exposition, before any configuration is read (D-H reads it).
    if std::env::args().nth(1).as_deref() == Some("--metrics-families") {
        print!("{}", render_metrics(&GuardMetrics::default()));
        return Ok(());
    }
    let started = std::time::SystemTime::now();
    let mut runtime = GatewayBootstrap::load_from_env()?.build().await?;
    let refresh = runtime.readiness_refresh();
    let probe = runtime
        .take_readiness_probe()
        .ok_or("readiness probe already taken")?;
    // ADR-0061 D-F: the first snapshot exists before anything accepts, so there is no "unknown".
    let readiness = Arc::new(Readiness::new(probe.check(refresh).await, refresh));
    readiness.spawn_refresh(probe);

    let (guard, for_status, config) = (
        runtime.guard(),
        Arc::clone(&readiness),
        runtime.effective_config().clone(),
    );
    // dep: HTTP(loopback) — the ops listener serving /metrics and /status (ADR-0061 D-B)
    let ops = serve_loopback(
        METRICS_ADDR_KEY,
        runtime.metrics_addr(),
        Routes {
            metrics: Box::new(move || Ok(render_metrics(guard.metrics()))),
            status: Box::new(move || Ok(for_status.status_json(started, &config))),
        },
    )?;
    let listener = tokio::net::TcpListener::bind(runtime.bind_addr()).await?;
    // ADR-0065 D-C: admission wraps the MCP router only; /livez and /readyz are merged outside it.
    let app = admission::wrap(
        runtime.adapter().router(),
        supervision_routes(readiness.clone()),
        runtime.admission(),
    );

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
        readiness.stop_accepting();
        eprintln!(
            "humaux-gateway: signal received, /readyz now 503, accepting for {}s more",
            DRAIN_ANNOUNCE_WINDOW.as_secs()
        );
        tokio::time::sleep(DRAIN_ANNOUNCE_WINDOW).await;
        // ADR-0061 D-B: metrics stay scrapable through the drain, so the drain itself is visible.
        drop(ops);
        eprintln!("humaux-gateway: drain window elapsed, closing the accept loop");
    };

    eprintln!(
        "humaux-gateway listening on {} ops={} config_fingerprint={}",
        listener.local_addr()?,
        runtime.metrics_addr(),
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
