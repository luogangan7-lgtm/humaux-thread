//! `gateway::status` — the gateway's truthful readiness and its ops surface: the cached dependency snapshot behind
//!   `/readyz`, the loopback `/status` document and the `/metrics` exposition (ADR-0061 D-B, D-C, D-F).
//! Depends-on: crates=[humaux-adapters, humaux-infra-cell, humaux-retrieval, humaux-telemetry, serde_json, tokio];
//!   services=[PostgreSQL(role_gateway), Qdrant(*), UDS(retrieval-worker)]; env=[CARGO_PKG_VERSION,
//!   HUMAUX_BUILD_GIT_SHA, HUMAUX_GATEWAY_METRICS_ADDR, HUMAUX_GATEWAY_READINESS_REFRESH_SECONDS,
//!   HUMAUX_GATEWAY_RETRIEVAL_RPC_SOCKET_PATH]; modules=[adapters::postgres, gateway::guard,
//!   gateway::retrieval_embedding_client, infra-cell::permit, infra-cell::resource, infra-cell::transport,
//!   retrieval::completeness, telemetry::degrade]
//! Called-by: [gateway::bootstrap, gateway::main]
//! Invariants: [a request never runs a dependency check, it reads the last snapshot; `/readyz` carries a status word
//!   only (ready | not_ready | stale | draining) and every dependency name, path, error and age goes to the loopback
//!   `/status` alone; PG, the retrieval RPC round trip and Qdrant are all hard; a snapshot older than 2 × the refresh
//!   interval on the monotonic clock is stale, never ready]
//! Spec: Baseline §4.4; §41.2; §57.1; ADR-0037; ADR-0061 D-B; ADR-0061 D-F
//!
//! The refresh task (`spawn_refresh`) is the only writer of the snapshot; `/readyz` and `/status` only read it. The
//! first snapshot is taken before the listener accepts, so readiness has no "unknown" window.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use humaux_adapters::postgres::RuntimeDbPool;
use humaux_infra_cell::{
    IntraCellHttpTransport, IntraCellMethod, IntraCellRequest, IntraCellResource,
    IntraCellResourceRegistry, authorize_cell_access,
};
use humaux_telemetry::degrade::{DegradeCode, degrade_last_fired_unix, degrade_total_count};
use serde_json::{Map, Value, json};

use crate::guard::GuardMetrics;
use crate::retrieval_embedding_client::GatewayRetrievalEmbeddingClient;

/// §78.1 / ADR-0061 D-F: seconds between readiness refreshes; also each check's deadline.
pub const READINESS_REFRESH_KEY: &str = "HUMAUX_GATEWAY_READINESS_REFRESH_SECONDS";
/// §78.1 / ADR-0061 D-B: the loopback ops listener serving `/metrics` and `/status`.
pub const METRICS_ADDR_KEY: &str = "HUMAUX_GATEWAY_METRICS_ADDR";
const SOCKET_KEY: &str = "HUMAUX_GATEWAY_RETRIEVAL_RPC_SOCKET_PATH";

/// One dependency's state (§57.1 three states).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DepState {
    /// The round trip succeeded.
    Pass,
    /// The round trip failed; `missing_object` names what.
    Fail,
    /// The dependency is not configured; `missing_object` names the key that turns it on (§57.1).
    NotApplicable,
}

impl DepState {
    /// The `/status` spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::NotApplicable => "not_applicable",
        }
    }
}

/// One dependency check's result.
#[derive(Clone, Debug)]
pub struct DepCheck {
    /// Pass, fail or not applicable.
    pub state: DepState,
    /// When the check finished.
    pub checked_at: SystemTime,
    /// What is missing; `None` exactly when `state == Pass`.
    pub missing_object: Option<String>,
}

impl DepCheck {
    fn new(state: DepState, missing_object: Option<String>) -> Self {
        Self {
            state,
            checked_at: SystemTime::now(),
            missing_object,
        }
    }

    fn of(result: Result<(), String>) -> Self {
        match result {
            Ok(()) => Self::new(DepState::Pass, None),
            Err(missing) => Self::new(DepState::Fail, Some(missing)),
        }
    }

    fn json(&self, now: SystemTime) -> Value {
        json!({
            "state": self.state.as_str(),
            "checked_at": unix(self.checked_at),
            "age_seconds": age(now, self.checked_at).as_secs(),
            "missing_object": self.missing_object,
        })
    }
}

/// The `/readyz` answer (ADR-0061 D-F): the only thing the public route says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Accepting, every dependency pass or not applicable, snapshot fresh.
    Ready,
    /// A dependency failed.
    NotReady,
    /// The snapshot is older than 2 × the refresh interval.
    Stale,
    /// A termination signal arrived.
    Draining,
}

impl Verdict {
    /// The `{"status": …}` word.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::NotReady => "not_ready",
            Self::Stale => "stale",
            Self::Draining => "draining",
        }
    }
}

/// The three hard dependencies (ADR-0061 D-F, E15) as of the last refresh.
#[derive(Clone, Debug)]
pub struct ReadinessSnapshot {
    /// `SELECT 1` as `role_gateway`.
    pub pg: DepCheck,
    /// `GET /internal/v1/retrieval/readyz` over the worker socket.
    pub retrieval_rpc: DepCheck,
    /// `GET /` on the Qdrant cell resource through the gateway's own permit and transport.
    pub qdrant: DepCheck,
    /// When the refresh that produced this snapshot finished (wall clock; reported on `/status` only).
    pub taken_at: SystemTime,
    /// The same moment on the monotonic clock: the only input of the staleness verdict, so a wall-clock step
    /// can neither age a fresh snapshot nor rejuvenate an old one (ADR-0061 review-fix 3, F7).
    pub taken: Instant,
}

impl ReadinessSnapshot {
    /// Ready, not ready or stale at `now` for a refresh interval `refresh` (draining is decided by
    /// the caller, which owns the accepting flag). Pure, so the staleness arm is unit-testable.
    #[must_use]
    pub fn verdict(&self, now: Instant, refresh: Duration) -> Verdict {
        if now.saturating_duration_since(self.taken) > 2 * refresh {
            return Verdict::Stale;
        }
        let failed = [&self.pg, &self.retrieval_rpc, &self.qdrant]
            .iter()
            .any(|d| d.state == DepState::Fail);
        if failed {
            Verdict::NotReady
        } else {
            Verdict::Ready
        }
    }

    fn json(&self, now: SystemTime, verdict: Verdict) -> Value {
        json!({
            "verdict": verdict.as_str(),
            "taken_at": unix(self.taken_at),
            "age_seconds": age(now, self.taken_at).as_secs(),
            "pg": self.pg.json(now),
            "retrieval_rpc": self.retrieval_rpc.json(now),
            "qdrant": self.qdrant.json(now),
        })
    }
}

/// The semantic-recall dependencies, present only when `HUMAUX_GATEWAY_RETRIEVAL_RPC_SOCKET_PATH`
/// is set (the same gate as the recall lane).
pub(crate) struct SemanticDeps {
    pub(crate) rpc: Arc<GatewayRetrievalEmbeddingClient>,
    pub(crate) socket_path: String,
    pub(crate) qdrant: Arc<dyn IntraCellHttpTransport>,
    pub(crate) registry: IntraCellResourceRegistry,
}

/// What one refresh checks; built once in bootstrap from the objects the request path uses.
pub struct ReadinessProbe {
    pub(crate) pool: Arc<RuntimeDbPool>,
    pub(crate) semantic: Option<SemanticDeps>,
}

impl ReadinessProbe {
    /// Runs the three checks concurrently, each bounded by `deadline`.
    pub async fn check(&self, deadline: Duration) -> ReadinessSnapshot {
        let bounded = |name: &'static str| {
            move |r: Result<Result<(), String>, tokio::time::error::Elapsed>| {
                r.unwrap_or_else(|_| {
                    Err(format!(
                        "{name} did not answer within {READINESS_REFRESH_KEY}"
                    ))
                })
            }
        };
        let pg = async {
            tokio::time::timeout(deadline, self.pool.ping())
                .await
                .map(|r| r.map_err(|e| format!("PostgreSQL as role_gateway ({e})")))
        };
        let (pg, retrieval_rpc, qdrant) = match &self.semantic {
            None => {
                let na = || {
                    DepCheck::new(
                        DepState::NotApplicable,
                        Some(format!("{SOCKET_KEY} (semantic recall disabled)")),
                    )
                };
                (DepCheck::of(bounded("PostgreSQL")(pg.await)), na(), na())
            }
            Some(deps) => {
                let rpc = async {
                    // dep: UDS(retrieval-worker) — the readiness round trip over the recall socket
                    deps.rpc
                        .ping(deadline)
                        .await
                        .map_err(|reason| format!("{} ({reason})", deps.socket_path))
                };
                let qdrant = tokio::time::timeout(deadline, qdrant_round_trip(deps, deadline));
                let (pg, rpc, qdrant) = tokio::join!(pg, rpc, qdrant);
                (
                    DepCheck::of(bounded("PostgreSQL")(pg)),
                    DepCheck::of(rpc),
                    DepCheck::of(bounded("the Qdrant cell resource")(qdrant)),
                )
            }
        };
        ReadinessSnapshot {
            pg,
            retrieval_rpc,
            qdrant,
            taken_at: SystemTime::now(),
            taken: Instant::now(),
        }
    }
}

/// The same `IntraCellRequest` path the retrieval worker's `--readyz` uses. `GET /` rather than
/// Qdrant's `/readyz`: the latter answers plain text, which the cell transport refuses to parse.
async fn qdrant_round_trip(deps: &SemanticDeps, ttl: Duration) -> Result<(), String> {
    let permit = authorize_cell_access(&deps.registry, IntraCellResource::QDRANT_REST, ttl)
        .map_err(|e| format!("the Qdrant cell permit ({e:?})"))?;
    let status = deps
        .qdrant
        .execute(
            &permit,
            // dep: Qdrant(*) — readiness round trip on the gateway's read-only cell resource
            IntraCellRequest {
                method: IntraCellMethod::Get,
                path: "/".to_owned(),
                json_body: None,
                headers: Vec::new(),
            },
        )
        .await
        .map_err(|e| format!("the Qdrant cell resource ({e:?})"))?
        .status;
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(format!("the Qdrant cell resource answered {status}"))
    }
}

/// The shared readiness state: the last snapshot, the refresh interval and the accepting flag.
pub struct Readiness {
    snapshot: RwLock<ReadinessSnapshot>,
    refresh: Duration,
    accepting: AtomicBool,
}

impl Readiness {
    /// Starts accepting with `first` as the snapshot.
    #[must_use]
    pub fn new(first: ReadinessSnapshot, refresh: Duration) -> Self {
        Self {
            snapshot: RwLock::new(first),
            refresh,
            accepting: AtomicBool::new(true),
        }
    }

    /// Called once by the shutdown future; `/readyz` answers `draining` from then on.
    pub fn stop_accepting(&self) {
        self.accepting.store(false, Ordering::SeqCst);
    }

    fn snapshot(&self) -> ReadinessSnapshot {
        self.snapshot
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The `/readyz` verdict now.
    #[must_use]
    pub fn verdict(&self) -> Verdict {
        if !self.accepting.load(Ordering::SeqCst) {
            return Verdict::Draining;
        }
        self.snapshot().verdict(Instant::now(), self.refresh)
    }

    /// Refreshes the snapshot every interval until the process exits (ADR-0061 D-F).
    pub fn spawn_refresh(self: &Arc<Self>, probe: ReadinessProbe) -> tokio::task::JoinHandle<()> {
        let readiness = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(readiness.refresh);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await; // immediate; the first snapshot was taken before accepting
            loop {
                ticker.tick().await;
                let next = probe.check(readiness.refresh).await;
                *readiness
                    .snapshot
                    .write()
                    .unwrap_or_else(PoisonError::into_inner) = next;
            }
        })
    }

    /// The loopback `/status` document (ADR-0061 D-B). `effective_config` carries no secret value.
    #[must_use]
    pub fn status_json(&self, started: SystemTime, effective_config: &Value) -> String {
        let now = SystemTime::now();
        let verdict = self.verdict();
        json!({
            "process": "humaux-gateway",
            "mode": "serve",
            "crate_version": env!("CARGO_PKG_VERSION"),
            "git_sha": option_env!("HUMAUX_BUILD_GIT_SHA"),
            "started_at": unix(started),
            "uptime_seconds": age(now, started).as_secs(),
            "degrade": degrade_json(),
            "accepting": self.accepting.load(Ordering::SeqCst),
            "readiness": self.snapshot().json(now, verdict),
            "effective_config": effective_config,
        })
        .to_string()
    }
}

/// `degrade_total` per code with its last fire time (§4.4 `degrade.counters`).
fn degrade_json() -> Value {
    let codes: Map<String, Value> = DegradeCode::ALL
        .iter()
        .map(|&code| {
            let entry = json!({
                "count": degrade_total_count(code),
                "last_fired_at": degrade_last_fired_unix(code),
            });
            (code.as_str().to_owned(), entry)
        })
        .collect();
    Value::Object(codes)
}

/// The gateway's nine §41.2 families (ADR-0061 D-C): `degrade_total`, the two retrieval families and
/// the six guard families. `--metrics-families` and `/metrics` both call this.
#[must_use]
pub fn render_metrics(guard: &GuardMetrics) -> String {
    let mut out = String::new();
    humaux_telemetry::degrade::render(&mut out);
    humaux_retrieval::completeness::render_metrics(&mut out);
    guard.render(&mut out);
    out
}

fn unix(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

fn age(now: SystemTime, then: SystemTime) -> Duration {
    now.duration_since(then).unwrap_or(Duration::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pass_at(t: SystemTime) -> DepCheck {
        DepCheck {
            state: DepState::Pass,
            checked_at: t,
            missing_object: None,
        }
    }

    /// A snapshot taken at monotonic `taken` whose wall clock read `wall`.
    fn snapshot_at(taken: Instant, wall: SystemTime) -> ReadinessSnapshot {
        ReadinessSnapshot {
            pg: pass_at(wall),
            retrieval_rpc: pass_at(wall),
            qdrant: pass_at(wall),
            taken_at: wall,
            taken,
        }
    }

    /// T-G4: an all-pass snapshot 3N old is stale, never ready. Fault: drop the age check ⇒ Ready.
    #[test]
    fn an_old_all_pass_snapshot_is_stale() {
        let refresh = Duration::from_secs(2);
        let (now, wall) = (Instant::now(), SystemTime::now());
        assert_eq!(snapshot_at(now, wall).verdict(now, refresh), Verdict::Ready);
        assert_eq!(
            snapshot_at(now - 3 * refresh, wall).verdict(now, refresh),
            Verdict::Stale
        );
        assert_eq!(
            snapshot_at(now - 2 * refresh, wall).verdict(now, refresh),
            Verdict::Ready,
            "exactly 2N is still fresh"
        );
    }

    /// ADR-0061 review-fix 3 (F7): staleness is monotonic. A wall clock stepped back an hour after an old snapshot
    /// does not make it fresh, and one stepped forward does not make a fresh snapshot stale. Fault: measure the age
    /// from `taken_at` (SystemTime) ⇒ red.
    #[test]
    fn staleness_ignores_wall_clock_steps() {
        let refresh = Duration::from_secs(2);
        let (now, wall) = (Instant::now(), SystemTime::now());
        let hour = Duration::from_secs(3600);
        assert_eq!(
            snapshot_at(now - 3 * refresh, wall + hour).verdict(now, refresh),
            Verdict::Stale,
            "an old snapshot whose wall time is in the future is still stale"
        );
        assert_eq!(
            snapshot_at(now, wall - hour).verdict(now, refresh),
            Verdict::Ready,
            "a fresh snapshot whose wall time is an hour old is still fresh"
        );
    }

    /// A failed dependency is not_ready; not_applicable is not a failure (§57.1).
    #[test]
    fn a_failed_dependency_is_not_ready_and_not_applicable_is_not() {
        let refresh = Duration::from_secs(2);
        let now = Instant::now();
        let mut s = snapshot_at(now, SystemTime::now());
        s.retrieval_rpc = DepCheck::new(DepState::NotApplicable, Some(SOCKET_KEY.into()));
        s.qdrant = DepCheck::new(DepState::NotApplicable, Some(SOCKET_KEY.into()));
        assert_eq!(s.verdict(now, refresh), Verdict::Ready);
        s.qdrant = DepCheck::new(DepState::Fail, Some("the Qdrant cell resource".into()));
        assert_eq!(s.verdict(now, refresh), Verdict::NotReady);
    }

    /// Draining wins over every snapshot.
    #[test]
    fn draining_wins() {
        let readiness = Readiness::new(
            snapshot_at(Instant::now(), SystemTime::now()),
            Duration::from_secs(2),
        );
        assert_eq!(readiness.verdict(), Verdict::Ready);
        readiness.stop_accepting();
        assert_eq!(readiness.verdict(), Verdict::Draining);
    }

    /// T-G1 (zero state): the nine gateway families, each with HELP, TYPE and ≥1 sample.
    #[test]
    fn render_metrics_carries_the_nine_gateway_families() {
        let out = render_metrics(&GuardMetrics::default());
        let families: Vec<&str> = out
            .lines()
            .filter_map(|l| l.strip_prefix("# TYPE "))
            .filter_map(|l| l.split(' ').next())
            .collect();
        assert_eq!(
            families,
            [
                "degrade_total",
                "humaux_retrieval_requests_total",
                "retrieval_completeness_total",
                "humaux_mcp_requests_total",
                "mcp_auth_attempts_total",
                "mcp_authz_denied_total",
                "mcp_quota_reservations_total",
                "mcp_bmo_consumed_total",
                "rate_limit_rejected_total",
                "admission_rejected_total",
            ]
        );
        for family in families {
            assert!(
                out.lines().any(|l| l.starts_with(&format!("{family}{{"))),
                "{family} has no sample"
            );
        }
    }
}
