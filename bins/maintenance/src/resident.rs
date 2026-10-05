//! `maintenance::resident` — what the two resident modes (`health serve`, `--serve`) share: the SIGTERM / SIGINT
//!   latch, the per-statement timeout on the pool DSN, the last-outcome record behind the 200/503 readiness verdict,
//!   the `/status` identity document and the loopback ops listener wiring (ADR-0061 D-B, D-D; ADR-0062 D-A).
//! Depends-on: crates=[humaux-telemetry, serde_json, tokio]; services=[HTTP(loopback)]; env=[CARGO_PKG_VERSION, HUMAUX_BUILD_GIT_SHA];
//!   modules=[telemetry::degrade, telemetry::metrics, maintenance::main]
//! Called-by: [maintenance::health_serve, maintenance::serve]
//! Invariants: [both signal handlers are installed before any work; a failed or stale last outcome answers 503 with
//!   the reason and the age, never the last good values; the ops listener binds a loopback address only; every
//!   resident mode's `/status` carries the full ADR-0061 D-B identity document, the 11 degrade codes included]
//! Spec: Baseline §41.2; §78.1; ADR-0037; ADR-0061 D-B; ADR-0061 D-D; ADR-0062 D-A

use std::net::SocketAddr;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use humaux_telemetry::degrade::{DegradeCode, degrade_last_fired_unix, degrade_total_count};
use humaux_telemetry::metrics::{OpsListener, Routes, parse_ops_addr, serve_loopback};
use serde_json::{Map, Value, json};
use tokio::signal::unix::{SignalKind, signal};
use tokio::task::JoinHandle;

use crate::{Failure, Result, env};

/// SIGTERM or SIGINT seen (supervision.md §3: Ctrl-C is handled exactly like SIGTERM).
pub(crate) struct Latch(JoinHandle<()>);

impl Latch {
    /// Installs both handlers now, before the caller does any work.
    pub(crate) fn install() -> Result<Self> {
        let handler = |kind| {
            signal(kind)
                .map_err(|e| Failure::Infra(format!("cannot install a signal handler: {e}")))
        };
        let (mut terminate, mut interrupt) = (
            handler(SignalKind::terminate())?,
            handler(SignalKind::interrupt())?,
        );
        Ok(Self(tokio::spawn(async move {
            tokio::select! {
                _ = terminate.recv() => {}
                _ = interrupt.recv() => {}
            }
        })))
    }

    /// Non-blocking: has a signal arrived? (ADR-0062 D-B: checked between per-tenant calls.)
    pub(crate) fn is_set(&self) -> bool {
        self.0.is_finished()
    }

    /// Resolves once a signal arrived. Await it at most once to completion (a `select!` arm that breaks).
    pub(crate) async fn wait(&mut self) {
        let _ = (&mut self.0).await;
    }
}

/// ADR-0061 D-D: the server cancels any statement at `limit` (`options[...]` is sqlx's startup-parameter form).
pub(crate) fn with_statement_timeout(dsn: &str, limit: Duration) -> String {
    let sep = if dsn.contains('?') { '&' } else { '?' };
    format!("{dsn}{sep}options[statement_timeout]={}", limit.as_millis())
}

/// What every scrape reads: when the last good outcome landed (`None`: none since start) and whether the latest
/// one failed.
pub(crate) struct Last {
    pub(crate) ok_at: Option<Instant>,
    pub(crate) error: Option<String>,
}

impl Last {
    /// A good outcome now.
    pub(crate) fn ok() -> Mutex<Self> {
        Mutex::new(Self {
            ok_at: Some(Instant::now()),
            error: None,
        })
    }

    /// No outcome yet: the listener is up before the first one finished, and readiness answers 503 until it does
    /// (ADR-0062 D-A: 200 means one clean cycle, never "booted").
    pub(crate) fn pending() -> Mutex<Self> {
        Mutex::new(Self {
            ok_at: None,
            error: None,
        })
    }
}

/// Readiness: `Err` (503) when the latest `what` failed or the last good one is older than `stale_after`
/// (`bound` names that limit for the operator); otherwise `Ok(body())`.
pub(crate) fn verdict(
    last: &Mutex<Last>,
    what: &str,
    stale_after: Duration,
    bound: &str,
    body: impl FnOnce() -> String,
) -> std::result::Result<String, String> {
    let last = last.lock().unwrap_or_else(PoisonError::into_inner);
    let good = last.ok_at.map_or_else(
        || format!("no good {what} since start"),
        |at| format!("last good {what} {} s ago", at.elapsed().as_secs()),
    );
    // ADR-0061 D-D: a stale gauge looks healthy, so a failed or old outcome is a 503, never the last values.
    if let Some(error) = &last.error {
        return Err(format!("{what} failed: {error} ({good})\n"));
    }
    match last.ok_at {
        None => Err(format!("{what} pending: {good}\n")),
        Some(at) if at.elapsed() > stale_after => {
            Err(format!("{what} stale: {good} (> {bound})\n"))
        }
        Some(_) => Ok(body()),
    }
}

/// The ops listener address from `key` (loopback, no default).
pub(crate) fn ops_addr(key: &str) -> Result<SocketAddr> {
    parse_ops_addr(&env(key)?).map_err(|reason| Failure::Usage(format!("{key}: {reason}")))
}

/// Binds the loopback ops listener; a non-loopback address is a usage error, any other bind failure infra.
pub(crate) fn listen(key: &str, addr: SocketAddr, routes: Routes) -> Result<OpsListener> {
    // dep: HTTP(loopback) — binds this mode's ops listener (/metrics, /status)
    serve_loopback(key, addr, routes).map_err(|e| match e.kind() {
        std::io::ErrorKind::InvalidInput => Failure::Usage(e.to_string()),
        _ => Failure::Infra(e.to_string()),
    })
}

/// ADR-0061 D-B: the identity document every process's `/status` starts with (`process`, `mode`, `crate_version`,
/// `git_sha`, `started_at`, `uptime_seconds`, `degrade` per code). `humaux-admin q degrade.counters` reads the
/// `degrade` block of every ops address, so a mode that leaves it out fails that probe; the caller adds its own
/// mode-specific keys.
pub(crate) fn status_document(mode: &str, started: SystemTime) -> Map<String, Value> {
    let degrade: Map<String, Value> = DegradeCode::ALL
        .iter()
        .map(|&code| {
            let entry = json!({
                "count": degrade_total_count(code),
                "last_fired_at": degrade_last_fired_unix(code),
            });
            (code.as_str().to_owned(), entry)
        })
        .collect();
    let mut doc = Map::new();
    doc.insert("process".into(), json!("humaux-maintenance"));
    doc.insert("mode".into(), json!(mode));
    doc.insert("crate_version".into(), json!(env!("CARGO_PKG_VERSION")));
    doc.insert("git_sha".into(), json!(option_env!("HUMAUX_BUILD_GIT_SHA")));
    doc.insert(
        "started_at".into(),
        json!(
            started
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_secs())
        ),
    );
    doc.insert(
        "uptime_seconds".into(),
        json!(started.elapsed().map_or(0, |d| d.as_secs())),
    );
    doc.insert("degrade".into(), Value::Object(degrade));
    doc
}
