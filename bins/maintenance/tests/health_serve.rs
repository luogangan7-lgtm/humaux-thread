//! `maintenance::tests::health_serve` — the resident `humaux-maintenance health serve` process against a real
//!   throwaway PostgreSQL (ADR-0061 D-D process side, D-B): samples, the 503-on-failure rule, required keys,
//!   SIGTERM, `--metrics-families`.
//! Depends-on: crates=[humaux-testkit, postgres, serde_json]; services=[PostgreSQL(owner) r=[ops.schema_migrations]
//!   w=[control.tenants, ops.jobs] x=[ops.health_snapshot], HTTP(loopback), subprocess(humaux-maintenance), subprocess(kill)];
//!   env=[CARGO_BIN_EXE_humaux-maintenance, CARGO_MANIFEST_DIR, HUMAUX_MAINTENANCE_HEALTH_SAMPLE_SECONDS,
//!   HUMAUX_MAINTENANCE_HEALTH_SERVE_METRICS_ADDR, HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_TEST_PG_DSN];
//!   modules=[humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [each DB test owns its throwaway database humaux_thread_c34_hs_<pid>_<n>, dropped by the fixture's
//!   Drop even on panic, and the spawned process is killed by its own Drop; the EXECUTE revoke of T-M2 happens
//!   only inside that throwaway database; every listener port is taken free at run time on 127.0.0.1]
//! Spec: Baseline §41.2; §78.1; §79.2; ADR-0061 D-B; ADR-0061 D-D

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};

const BIN: &str = env!("CARGO_BIN_EXE_humaux-maintenance");
const OWNER_DSN: &str = "HUMAUX_TEST_PG_DSN";
const MAINTENANCE_DSN: &str = "HUMAUX_MAINTENANCE_PG_DSN";
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

static NEXT: AtomicUsize = AtomicUsize::new(0);

/// `postgres://user:pass@host:port/<db>?query` with the database replaced.
fn with_db(dsn: &str, db: &str) -> String {
    let (head, tail) = dsn.split_at(dsn.rfind('/').expect("dsn has a database path") + 1);
    let query = tail.find('?').map_or("", |i| &tail[i..]);
    format!("{head}{db}{query}")
}

/// A loopback port free at the moment of the call (the xtask e2e_onboard::free_port pattern).
fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("a free loopback port")
}

/// One HTTP/1.0 GET; `None` when nothing listens.
fn get(addr: SocketAddr, path: &str) -> Option<(u16, String)> {
    // dep: HTTP(loopback) — the process's ops listener
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(1)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(3))).ok()?;
    write!(stream, "GET {path} HTTP/1.0\r\nHost: {addr}\r\n\r\n").ok()?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw).ok()?;
    let code = raw.split_whitespace().nth(1)?.parse().ok()?;
    let body = raw.split_once("\r\n\r\n").map(|(_, b)| b.to_owned())?;
    Some((code, body))
}

/// Polls `GET path` until `done` holds or `within` elapses; returns the last answer.
fn poll(
    addr: SocketAddr,
    path: &str,
    within: Duration,
    done: impl Fn(&(u16, String)) -> bool,
) -> Option<(u16, String)> {
    let deadline = Instant::now() + within;
    loop {
        let answer = get(addr, path);
        if answer.as_ref().is_some_and(&done) || Instant::now() >= deadline {
            return answer;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn value(metrics: &str, series: &str) -> Option<f64> {
    metrics
        .lines()
        .find_map(|l| l.strip_prefix(series)?.strip_prefix(' '))
        .and_then(|v| v.parse().ok())
}

/// The spawned process, killed if the test did not stop it itself.
struct Serve(Child);

impl Drop for Serve {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Db {
    owner_dsn: String,
    maintenance_dsn: String,
    name: String,
    client: Option<Client>,
}

impl Drop for Db {
    fn drop(&mut self) {
        drop(self.client.take());
        // dep: PostgreSQL(owner) — drop this test's throwaway database
        match Client::connect(&with_db(&self.owner_dsn, "postgres"), NoTls) {
            Ok(mut admin) => {
                let drop_db = format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name);
                if let Err(e) = admin.batch_execute(&drop_db) {
                    eprintln!("health_serve cleanup: {drop_db} failed: {e}");
                }
            }
            Err(e) => eprintln!("health_serve cleanup: connect failed: {e}"),
        }
    }
}

/// Applies every migration file in order (bodies only; the manifests are the `xtask migrate` gate's job).
fn migrate(client: &mut Client) {
    // Role DDL is cluster-global (0201 ALTER ROLE, 0210 CREATE ROLE): one database at a time per process.
    static ONE_AT_A_TIME: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _serial = ONE_AT_A_TIME
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("migrations dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    files.sort();
    client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS ops; CREATE TABLE IF NOT EXISTS ops.schema_migrations \
             (migration_id text PRIMARY KEY, checksum text NOT NULL, \
              applied_at timestamptz NOT NULL DEFAULT now())",
        )
        .expect("migration ledger bootstrap");
    for file in files {
        let sql = std::fs::read_to_string(&file).expect("migration body");
        client
            .batch_execute(&sql)
            .unwrap_or_else(|e| panic!("apply {}: {e:?}", file.display()));
    }
}

fn db(test: &str) -> Option<Db> {
    let (Ok(owner_dsn), Ok(maintenance_dsn)) =
        (std::env::var(OWNER_DSN), std::env::var(MAINTENANCE_DSN))
    else {
        skip_or_fail(
            test,
            "missing object: HUMAUX_TEST_PG_DSN / HUMAUX_MAINTENANCE_PG_DSN",
            ExternalDep::Postgres,
        );
        return None;
    };
    let name = format!(
        "humaux_thread_c34_hs_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    );
    // dep: PostgreSQL(owner) — create this test's throwaway database
    let mut admin =
        Client::connect(&with_db(&owner_dsn, "postgres"), NoTls).expect("owner connect");
    admin
        .batch_execute(&format!("CREATE DATABASE {name}"))
        .expect("create throwaway db");
    drop(admin);
    let mut db = Db {
        maintenance_dsn: with_db(&maintenance_dsn, &name),
        owner_dsn: owner_dsn.clone(),
        name: name.clone(),
        client: None,
    };
    // dep: PostgreSQL(owner) — fixture connection to the throwaway database
    let mut client = Client::connect(&with_db(&owner_dsn, &name), NoTls).expect("connect test db");
    migrate(&mut client);
    db.client = Some(client);
    Some(db)
}

impl Db {
    fn sql(&mut self, statements: &str) {
        self.client
            .as_mut()
            .expect("client")
            .batch_execute(statements)
            .unwrap_or_else(|e| panic!("{statements}: {e:?}"));
    }

    /// Spawns `health serve` on a free loopback port and waits for its first 200 (taken after the first sample).
    fn serve(&self) -> (Serve, SocketAddr) {
        let addr = free_addr();
        // dep: subprocess(humaux-maintenance) — the resident `health serve` under test
        let child = Command::new(BIN)
            .args(["health", "serve"])
            .env_clear()
            .env(MAINTENANCE_DSN, &self.maintenance_dsn)
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
}

fn run(args: &[&str], env: &[(&str, String)]) -> Output {
    // dep: subprocess(humaux-maintenance) — one run with exactly `env`
    Command::new(BIN)
        .args(args)
        .env_clear()
        .envs(env.iter().map(|(k, v)| (k, v)))
        .output()
        .expect("run humaux-maintenance")
}

/// T-M1: all nine families carry samples; a DEAD job inserted later shows within two intervals, so the process
/// samples continuously (sampling once only ⇒ `jobs_dead` stays 0, or the snapshot goes stale ⇒ 503 ⇒ red).
#[test]
fn metrics_carry_every_family_and_follow_a_new_dead_job() {
    let Some(mut db) = db("metrics_carry_every_family_and_follow_a_new_dead_job") else {
        return;
    };
    let (_serve, addr) = db.serve();
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
    let (_serve, addr) = db.serve();
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
    let (mut serve, addr) = db.serve();
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
