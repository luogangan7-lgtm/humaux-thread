//! `maintenance::tests::health_serve` — the resident `humaux-maintenance health serve` process against a real
//!   throwaway PostgreSQL (ADR-0061 D-D process side, D-B): samples, the 503-on-failure rule, required keys,
//!   SIGTERM, `--metrics-families`.
//! Depends-on: crates=[serde_json]; services=[PostgreSQL(owner)
//!   w=[control.tenants, ops.jobs] x=[ops.health_snapshot], HTTP(loopback), subprocess(humaux-maintenance), subprocess(kill)];
//!   env=[HUMAUX_MAINTENANCE_HEALTH_SAMPLE_SECONDS, HUMAUX_MAINTENANCE_HEALTH_SERVE_METRICS_ADDR];
//!   modules=[maintenance::tests::support::throwaway]
//! Called-by: [cargo-test]
//! Invariants: [each DB test owns its throwaway database humaux_thread_c34_hs_<pid>_<n>, dropped by the fixture's
//!   Drop even on panic, and the spawned process is killed by its own Drop; the EXECUTE revoke of T-M2 happens
//!   only inside that throwaway database; every listener port is taken free at run time on 127.0.0.1]
//! Spec: Baseline §41.2; §78.1; §79.2; ADR-0061 D-B; ADR-0061 D-D

use std::io::Read;
use std::net::{SocketAddr, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[path = "support/throwaway.rs"]
#[allow(dead_code)]
mod throwaway;
use throwaway::{BIN, Db, MAINTENANCE_DSN, Serve, free_addr, get, poll, run};

const METRICS_ADDR: &str = "HUMAUX_MAINTENANCE_HEALTH_SERVE_METRICS_ADDR";
const SAMPLE_SECONDS: &str = "HUMAUX_MAINTENANCE_HEALTH_SAMPLE_SECONDS";
/// The test's sample interval; every "within 2 intervals" bound below is derived from it.
const INTERVAL: Duration = Duration::from_secs(1);
const FAMILIES: [&str; 9] = [
    "jobs_pending",
    "jobs_processing",
    "jobs_waiting_key",
    "jobs_dead",
    "oldest_pending_age_seconds",
    "projection_lag_events",
    "processing_gap_count",
    "data_disclosures_finalized_total",
    "data_disclosures_reserved_unfinalized",
];

fn value(metrics: &str, series: &str) -> Option<f64> {
    metrics
        .lines()
        .find_map(|l| l.strip_prefix(series)?.strip_prefix(' '))
        .and_then(|v| v.parse().ok())
}

fn db(test: &str) -> Option<Db> {
    throwaway::db(test, "c34_hs")
}

/// Spawns `health serve` on a free loopback port and waits for its first 200 (taken after the first sample).
fn spawn(db: &Db) -> (Serve, SocketAddr) {
    let addr = free_addr();
    // dep: subprocess(humaux-maintenance) — the resident `health serve` under test
    let child = Command::new(BIN)
        .args(["health", "serve"])
        .env_clear()
        .env(MAINTENANCE_DSN, &db.maintenance_dsn)
        .env(METRICS_ADDR, addr.to_string())
        .env(SAMPLE_SECONDS, INTERVAL.as_secs().to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn humaux-maintenance health serve");
    let serve = Serve(child);
    let first = poll(addr, "/metrics", 10 * INTERVAL, |(code, _)| *code == 200);
    assert_eq!(
        first.map(|(c, _)| c),
        Some(200),
        "health serve never answered /metrics 200"
    );
    (serve, addr)
}

/// T-M1: all nine families carry samples; a DEAD job inserted later shows within two intervals, so the process
/// samples continuously (sampling once only ⇒ `jobs_dead` stays 0, or the snapshot goes stale ⇒ 503 ⇒ red).
#[test]
fn metrics_carry_every_family_and_follow_a_new_dead_job() {
    let Some(mut db) = db("metrics_carry_every_family_and_follow_a_new_dead_job") else {
        return;
    };
    let (_serve, addr) = spawn(&db);
    let (_, body) = get(addr, "/metrics").expect("/metrics");
    for family in FAMILIES {
        assert!(
            body.contains(&format!("# TYPE {family} ")),
            "{family} TYPE\n{body}"
        );
        let samples = body
            .lines()
            .filter(|l| !l.starts_with('#'))
            .filter(|l| l.split(['{', ' ']).next() == Some(family))
            .count();
        assert!(samples >= 1, "{family} has no sample\n{body}");
    }
    assert_eq!(value(&body, "jobs_dead"), Some(0.0), "{body}");
    db.sql(
        "WITH t AS (INSERT INTO control.tenants (name) VALUES ('c34-hs') RETURNING tenant_id) \
         INSERT INTO ops.jobs (tenant_id, job_type, status, idempotency_key, payload) \
         SELECT tenant_id, 'C34_HEALTH_SERVE_TEST', 'DEAD', gen_random_uuid()::text, '{}'::jsonb FROM t",
    );
    let after = poll(
        addr,
        "/metrics",
        2 * INTERVAL + INTERVAL / 2,
        |(code, b)| *code == 200 && value(b, "jobs_dead") == Some(1.0),
    );
    let (code, body) = after.expect("/metrics after the insert");
    assert_eq!(
        (code, value(&body, "jobs_dead")),
        (200, Some(1.0)),
        "jobs_dead did not follow the inserted DEAD job within 2 intervals\n{body}"
    );
    let (code, status) = get(addr, "/status").expect("/status");
    assert_eq!(code, 200, "{status}");
    let status: serde_json::Value = serde_json::from_str(&status).expect("/status is JSON");
    assert_eq!(status["process"], "humaux-maintenance");
    assert_eq!(status["degrade"].as_object().map(|m| m.len()), Some(11));
}

/// T-M2 (the producer half of the HealthGaugesAbsent injection): EXECUTE revoked mid-run ⇒ `/metrics` answers
/// 503 naming the function within two intervals, never the last good values; re-granted ⇒ 200 again.
#[test]
fn a_failed_sample_answers_503_naming_the_function() {
    let Some(mut db) = db("a_failed_sample_answers_503_naming_the_function") else {
        return;
    };
    let (_serve, addr) = spawn(&db);
    db.sql("REVOKE EXECUTE ON FUNCTION ops.health_snapshot(timestamptz) FROM role_maintenance");
    let failed = poll(
        addr,
        "/metrics",
        2 * INTERVAL + INTERVAL / 2,
        |(code, _)| *code == 503,
    );
    let (code, body) = failed.expect("/metrics after the revoke");
    assert_eq!(code, 503, "a failed sample must not serve values\n{body}");
    assert!(
        body.contains("health_snapshot"),
        "503 names the function: {body}"
    );
    assert!(!body.contains("# TYPE"), "no partial exposition: {body}");
    db.sql("GRANT EXECUTE ON FUNCTION ops.health_snapshot(timestamptz) TO role_maintenance");
    let back = poll(
        addr,
        "/metrics",
        2 * INTERVAL + INTERVAL / 2,
        |(code, _)| *code == 200,
    );
    assert_eq!(
        back.map(|(c, _)| c),
        Some(200),
        "recovers after the re-grant"
    );
}

/// T-M3: each key is required by name, with no code default (§78.1); checked before any database work.
#[test]
fn a_missing_key_exits_non_zero_naming_it() {
    let out = run(&["health", "serve"], &[]);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "{stderr}");
    assert!(stderr.contains(METRICS_ADDR), "{stderr}");
    // ADR-0061 review-fix 3 (F9). Fault: accept port 0 in `telemetry::metrics::parse_ops_addr` ⇒ the next key is
    // named instead ⇒ red.
    let out = run(
        &["health", "serve"],
        &[(METRICS_ADDR, "127.0.0.1:0".to_owned())],
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "{stderr}");
    assert!(stderr.contains(METRICS_ADDR), "{stderr}");
    let out = run(
        &["health", "serve"],
        &[(METRICS_ADDR, free_addr().to_string())],
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "{stderr}");
    assert!(stderr.contains(SAMPLE_SECONDS), "{stderr}");
    let out = run(
        &["health", "serve"],
        &[
            (METRICS_ADDR, free_addr().to_string()),
            (SAMPLE_SECONDS, "0".to_owned()),
        ],
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success() && stderr.contains(SAMPLE_SECONDS),
        "{stderr}"
    );
}

/// T-M4: SIGTERM ⇒ exit 0 with its receipt, and the ops port is closed.
#[test]
fn sigterm_exits_zero_and_closes_the_port() {
    let Some(db) = db("sigterm_exits_zero_and_closes_the_port") else {
        return;
    };
    let (mut serve, addr) = spawn(&db);
    let pid = serve.0.id().to_string();
    // dep: subprocess(kill) — SIGTERM to the process this test spawned
    let sent = Command::new("kill").args(["-TERM", &pid]).status();
    assert!(sent.is_ok_and(|s| s.success()), "kill -TERM {pid}");
    let deadline = Instant::now() + 5 * INTERVAL;
    let status = loop {
        if let Some(status) = serve.0.try_wait().expect("try_wait") {
            break status;
        }
        assert!(Instant::now() < deadline, "no exit within 5 s of SIGTERM");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(status.success(), "exit {status}");
    let mut receipt = String::new();
    serve
        .0
        .stdout
        .take()
        .expect("stdout")
        .read_to_string(&mut receipt)
        .expect("read receipt");
    assert!(receipt.contains("\"stopped\""), "{receipt}");
    // dep: HTTP(loopback) — the closed ops port must refuse
    assert!(
        TcpStream::connect_timeout(&addr, Duration::from_secs(1)).is_err(),
        "the ops port is still open after exit"
    );
}

/// T-M5: `--metrics-families` needs no environment and prints the nine families at zero state.
#[test]
fn metrics_families_prints_nine_families_without_env() {
    let out = run(&["--metrics-families"], &[]);
    assert!(out.status.success(), "{out:?}");
    let text = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        text.lines().filter(|l| l.starts_with("# TYPE ")).count(),
        9,
        "{text}"
    );
    for family in FAMILIES {
        assert!(text.contains(&format!("# TYPE {family} ")), "{family}");
    }
}
