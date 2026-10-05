//! `maintenance::health_serve` — `humaux-maintenance health serve`, the one resident owner of the §41.2 SQL-derived
//!   health gauges: a periodic `ops.health_snapshot` sampler plus the loopback `/metrics` + `/status` listener
//!   (ADR-0061 D-D process side, D-B).
//! Depends-on: crates=[humaux-adapters, humaux-telemetry, serde_json, time, tokio];
//!   services=[PostgreSQL(role_maintenance) x=[ops.health_snapshot]]; env=[
//!   HUMAUX_MAINTENANCE_HEALTH_SAMPLE_SECONDS, HUMAUX_MAINTENANCE_HEALTH_SERVE_METRICS_ADDR, HUMAUX_MAINTENANCE_PG_DSN]; modules=[adapters::disclosure, adapters::health, adapters::postgres,
//!   telemetry::health, telemetry::metrics, maintenance::main, maintenance::resident]
//! Called-by: [maintenance::main]
//! Invariants: [a scrape never runs SQL, it renders the last publish; a failed or stale (> 2 × interval) sample
//!   answers 503 with the error and the age, never the last good values; the first sample is taken before the
//!   listener binds; both keys are required with no code default; SIGTERM / SIGINT close the port and exit 0]
//! Spec: Baseline §41.2; §53.5; §42; §78.1; ADR-0061 D-B; ADR-0061 D-D
//!
//! Exactly one process samples (ADR-0061 D-D): one series per gauge, one EXECUTE grant. The finalized-disclosure
//! watermark lives here: it starts at process start and advances to each successful sample's `as_of`; a failed
//! sample leaves it where it was, so the next good sample counts the rows the failed one missed. It never purges:
//! the maintenance tasks are `--serve`'s (ADR-0062 D-A).

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use humaux_adapters::disclosure::DisclosureOutcome as DbOutcome;
use humaux_adapters::health::{HealthSample, read_health_snapshot};
use humaux_adapters::postgres::MaintenanceDbPool;
use humaux_telemetry::health::{self, AgeBucket, DisclosureOutcome, HealthSnapshot};
use humaux_telemetry::metrics::Routes;
use serde_json::{Value, json};
use time::OffsetDateTime;

use crate::resident::{self, Last, Latch, with_statement_timeout};
use crate::{Failure, Output, Result, env, env_parsed};

/// §78.1 / ADR-0061 D-B: this resident mode's own ops listener address (loopback only, no default).
const METRICS_ADDR: &str = "HUMAUX_MAINTENANCE_HEALTH_SERVE_METRICS_ADDR";
/// §78.1 / ADR-0061 D-D: seconds between samples; also each sample's statement timeout (no default).
const SAMPLE_SECONDS: &str = "HUMAUX_MAINTENANCE_HEALTH_SAMPLE_SECONDS";
const PG_DSN: &str = "HUMAUX_MAINTENANCE_PG_DSN";

/// `--metrics-families`: the zero-state exposition this process serves (ADR-0061 D-C, read by D-H).
pub(crate) fn metrics_families() -> String {
    let mut out = String::new();
    health::render(&mut out);
    out
}

fn verdict(last: &Mutex<Last>, interval: Duration) -> std::result::Result<String, String> {
    let bound = format!("2 x {SAMPLE_SECONDS}");
    resident::verdict(
        last,
        "health sample",
        2 * interval,
        &bound,
        metrics_families,
    )
}

fn status(started: SystemTime, last: &Mutex<Last>, interval: Duration) -> String {
    let mut doc = resident::status_document("health serve", started);
    let sample = verdict(last, interval).err();
    doc.insert(
        "health_sample".into(),
        sample.map_or_else(
            || json!({"state": "pass"}),
            |e| json!({"state": "fail", "error": e.trim_end()}),
        ),
    );
    Value::Object(doc).to_string()
}

/// The adapters row onto the telemetry snapshot; the outcome match is exhaustive, so a new DB outcome is a
/// compile error here rather than a silently dropped label (§78.2).
fn snapshot(s: &HealthSample) -> HealthSnapshot {
    let outcome = |o: &DbOutcome| match o {
        DbOutcome::Success => DisclosureOutcome::Success,
        DbOutcome::Failed => DisclosureOutcome::Failed,
        DbOutcome::Denied => DisclosureOutcome::Denied,
    };
    HealthSnapshot {
        jobs_pending: s.jobs_pending,
        jobs_processing: s.jobs_processing,
        jobs_waiting_key: s.jobs_waiting_key,
        jobs_dead: s.jobs_dead,
        oldest_pending_age_seconds: s.oldest_pending_age_seconds,
        projection_lag_events: s.projection_lag_events,
        processing_gaps: s.processing_gaps.clone(),
        reserved_unfinalized: vec![
            (AgeBucket::Le10s, s.reserved_le_10s),
            (AgeBucket::Le60s, s.reserved_le_60s),
            (AgeBucket::Gt60s, s.reserved_gt_60s),
        ],
        finalized_since_watermark: s
            .finalized_since
            .iter()
            .map(|(o, n)| (outcome(o), *n))
            .collect(),
    }
}

/// One bounded read; `Ok(as_of)` after it was published.
async fn sample(
    pool: &MaintenanceDbPool,
    since: OffsetDateTime,
    interval: Duration,
) -> std::result::Result<OffsetDateTime, String> {
    // The client-side bound covers a pool acquire against an unreachable server, which no statement_timeout sees.
    let read = tokio::time::timeout(interval, read_health_snapshot(pool, since))
        .await
        .map_err(|_| {
            format!("ops.health_snapshot(timestamptz) did not answer within {SAMPLE_SECONDS}")
        })?
        .map_err(|e| e.to_string())?;
    health::publish(&snapshot(&read));
    Ok(read.as_of)
}

/// `health serve`: samples every interval until SIGTERM / SIGINT, serving the last sample on the ops listener.
pub(crate) async fn serve() -> Result<Output> {
    let addr = resident::ops_addr(METRICS_ADDR)?;
    let seconds: u64 = env_parsed(SAMPLE_SECONDS)?;
    if seconds == 0 {
        return Err(Failure::Usage(format!("{SAMPLE_SECONDS} must be > 0")));
    }
    let interval = Duration::from_secs(seconds);
    let mut latch = Latch::install()?;
    let started = SystemTime::now();
    // dep: PostgreSQL(role_maintenance) — the sampler's pool; every statement bounded by the interval
    let pool = MaintenanceDbPool::connect(&with_statement_timeout(&env(PG_DSN)?, interval))
        .await
        .map_err(|e| Failure::Infra(format!("connect {PG_DSN}: {e}")))?;
    let mut watermark = sample(&pool, OffsetDateTime::now_utc(), interval)
        .await
        .map_err(|e| Failure::Infra(format!("first health sample: {e}")))?;
    let last = Arc::new(Last::ok());
    let (for_metrics, for_status) = (Arc::clone(&last), Arc::clone(&last));
    let routes = Routes {
        metrics: Box::new(move || verdict(&for_metrics, interval)),
        status: Box::new(move || Ok(status(started, &for_status, interval))),
    };
    let listener = resident::listen(METRICS_ADDR, addr, routes)?;
    eprintln!(
        "humaux-maintenance: health serve on {} every {seconds} s",
        listener.local_addr()
    );
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // the first tick is immediate; the first sample was taken above
    let mut samples: u64 = 1;
    loop {
        tokio::select! {
            _ = latch.wait() => break,
            _ = ticker.tick() => {}
        }
        let outcome = sample(&pool, watermark, interval).await;
        samples += 1;
        let mut guard = last.lock().unwrap_or_else(PoisonError::into_inner);
        match outcome {
            Ok(as_of) => {
                watermark = as_of;
                *guard = Last {
                    ok_at: Some(Instant::now()),
                    error: None,
                };
            }
            Err(e) => {
                eprintln!("humaux-maintenance: health sample failed: {e}");
                guard.error = Some(e);
            }
        }
    }
    drop(listener);
    Ok(Output::ok(json!({
        "command": "health serve",
        "outcome": "stopped",
        "samples": samples,
    })))
}
