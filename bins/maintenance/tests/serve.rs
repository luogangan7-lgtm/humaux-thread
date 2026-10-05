//! `maintenance::tests::serve` — the resident `humaux-maintenance --serve` daemon and its one-shot twin `sweep once`
//!   against real throwaway PostgreSQL databases (ADR-0062 D-A..D-D, D-L, D-Q; card 35 S1/S2): required keys, the
//!   lag relation, one counted cycle, 503 and recovery across a closed PG port, SIGTERM between calls, the tenant
//!   page rotation, the §77-gated one-page sweep with its per-task counts, the boot refusal of both resident
//!   modes when the migrator DSN is in their environment (ADR-0063 D-H, T-H3), and the PARTITIONS proposer with its
//!   `partition_horizon_months` family (ADR-0063 D-K, T-K1..T-K3).
//! Depends-on: crates=[serde_json]; services=[PostgreSQL(any), PostgreSQL(owner)
//!   r=[control.partition_registry, control.retention_policies, ops.maintenance_receipts, ops.stage_runs]
//!   w=[control.confirm_tokens, control.rate_buckets, control.tenants, control.users, control.workspaces, ops.jobs,
//!   ops.selection_snapshots, projection.stream_log]
//!   x=[control.maintenance_tenant_page, control.reap_quota_reservations], PostgreSQL(role_maintenance),
//!   HTTP(loopback), subprocess(humaux-maintenance)];
//!   env=[CARGO_TARGET_TMPDIR, HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS, HUMAUX_MAINTENANCE_HEALTH_SAMPLE_SECONDS,
//!   HUMAUX_MAINTENANCE_HEALTH_SERVE_METRICS_ADDR, HUMAUX_MAINTENANCE_SERVE_CONFIRM_TOKENS_CONSUMED_RETENTION_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_CYCLE_SECONDS, HUMAUX_MAINTENANCE_SERVE_JOBS_DEAD_RETENTION_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_JOBS_DONE_RETENTION_SECONDS, HUMAUX_MAINTENANCE_SERVE_LOST_AFTER_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_LOST_EVERY_SECONDS, HUMAUX_MAINTENANCE_SERVE_LOST_LIMIT,
//!   HUMAUX_MAINTENANCE_SERVE_METRICS_ADDR, HUMAUX_MAINTENANCE_SERVE_PARTITIONS_EVERY_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_RATE_BUCKETS_IDLE_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_REDRIVE_COOLDOWN_SECONDS, HUMAUX_MAINTENANCE_SERVE_REISSUE_COOLDOWN_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_TENANTS_PER_RUN,
//!   HUMAUX_MIGRATOR_PG_DSN, HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS];
//!   modules=[maintenance::tests::support::throwaway]
//! Called-by: [cargo-test]
//! Invariants: [every DB test owns its throwaway database humaux_thread_c35_serve_<pid>_<n>, dropped by the
//!   fixture's Drop even on panic, so no LOST transition ever touches the shared dev database; every spawned
//!   daemon is killed by its own Drop; the key tests point at a closed port and never reach a database]
//! Spec: Baseline §15.2; §77; §78.1; §79.2; ADR-0037; ADR-0057 D-F; ADR-0062 D-A; ADR-0062 D-B; ADR-0062 D-D;
//!   ADR-0062 D-Q; ADR-0063 D-H; ADR-0063 D-K

use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use serde_json::Value;

#[path = "support/throwaway.rs"]
#[allow(dead_code)]
mod throwaway;
use throwaway::{BIN, Db, MAINTENANCE_DSN, Serve, free_addr, get, poll, run};

const CYCLE_SECONDS: &str = "HUMAUX_MAINTENANCE_SERVE_CYCLE_SECONDS";
const LOST_AFTER_SECONDS: &str = "HUMAUX_MAINTENANCE_SERVE_LOST_AFTER_SECONDS";
const LAG_SECONDS: &str = "HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS";
/// A DSN nothing listens on: a config check that passes when it should not reaches the connect and names the DSN.
const CLOSED_DSN: &str = "postgres://role_maintenance:unused@127.0.0.1:1/none";
/// ADR-0063 D-K: the PARTITIONS cadence (seconds, >= CYCLE_SECONDS).
const PARTITIONS_EVERY: &str = "HUMAUX_MAINTENANCE_SERVE_PARTITIONS_EVERY_SECONDS";
/// The task keys the daemon requires, by D-C task name.
const TASKS: [&str; 9] = [
    "LOST",
    "QUOTA_RESERVATIONS",
    "PROVIDER_BUDGETS",
    "CONFIRM_TOKENS",
    "SNAPSHOTS",
    "RATE_BUCKETS",
    "JOBS",
    "REISSUE",
    "REDRIVE",
];
/// Their `/status` and receipt labels, in the same order.
const LABELS: [&str; 9] = [
    "lost",
    "quota_reservations",
    "provider_budgets",
    "confirm_tokens",
    "snapshots",
    "rate_buckets",
    "jobs",
    "reissue",
    "redrive",
];
/// The §77 fields every writing subcommand requires.
const ADMIN: [&str; 8] = [
    "--actor",
    "ops@example.test",
    "--reason",
    "card 35 T-Q1",
    "--ticket",
    "T-Q1",
    "--step-up-auth",
    "test-step-up",
];

/// Every key `--serve` requires, with values valid for `cycle` seconds (EVERY = CYCLE, LOST_AFTER > LAG).
fn keys(addr: SocketAddr, dsn: &str, cycle: u64, tenants_per_run: u32) -> Vec<(String, String)> {
    let mut keys = vec![
        (
            "HUMAUX_MAINTENANCE_SERVE_METRICS_ADDR".to_owned(),
            addr.to_string(),
        ),
        (CYCLE_SECONDS.to_owned(), cycle.to_string()),
        (
            "HUMAUX_MAINTENANCE_SERVE_TENANTS_PER_RUN".to_owned(),
            tenants_per_run.to_string(),
        ),
    ];
    for task in TASKS {
        keys.push((
            format!("HUMAUX_MAINTENANCE_SERVE_{task}_EVERY_SECONDS"),
            cycle.to_string(),
        ));
        keys.push((
            format!("HUMAUX_MAINTENANCE_SERVE_{task}_LIMIT"),
            "50".to_owned(),
        ));
    }
    // ADR-0063 D-K: the cluster-level PARTITIONS task has a cadence and no LIMIT.
    keys.push((PARTITIONS_EVERY.to_owned(), cycle.to_string()));
    keys.push((LOST_AFTER_SECONDS.to_owned(), "120".to_owned()));
    keys.push((LAG_SECONDS.to_owned(), "60".to_owned()));
    // ADR-0062 D-G..D-J ages: one hour each, so only rows seeded older than that are purgeable.
    for age in [
        "HUMAUX_MAINTENANCE_SERVE_CONFIRM_TOKENS_CONSUMED_RETENTION_SECONDS",
        "HUMAUX_MAINTENANCE_SERVE_RATE_BUCKETS_IDLE_SECONDS",
        "HUMAUX_MAINTENANCE_SERVE_JOBS_DONE_RETENTION_SECONDS",
        "HUMAUX_MAINTENANCE_SERVE_JOBS_DEAD_RETENTION_SECONDS",
        "HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS",
        "HUMAUX_MAINTENANCE_SERVE_REISSUE_COOLDOWN_SECONDS",
        "HUMAUX_MAINTENANCE_SERVE_REDRIVE_COOLDOWN_SECONDS",
    ] {
        keys.push((age.to_owned(), "3600".to_owned()));
    }
    keys.push((MAINTENANCE_DSN.to_owned(), dsn.to_owned()));
    keys
}

/// The keys `sweep once` reads: every `--serve` key but the listener and the cadences (ADR-0062 D-Q).
fn once_keys(dsn: &str) -> Vec<(String, String)> {
    keys(free_addr(), dsn, 5, 1000)
        .into_iter()
        .filter(|(k, _)| {
            k != "HUMAUX_MAINTENANCE_SERVE_METRICS_ADDR" && !k.ends_with("_EVERY_SECONDS")
        })
        .collect()
}

fn serve_with(env: &[(String, String)]) -> std::process::Output {
    let env: Vec<(&str, String)> = env.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
    run(&["--serve"], &env)
}

fn spawn(env: &[(String, String)]) -> Serve {
    // dep: subprocess(humaux-maintenance) — the resident `--serve` under test
    let child = Command::new(BIN)
        .arg("--serve")
        .env_clear()
        .envs(env.iter().map(|(k, v)| (k, v)))
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn humaux-maintenance --serve");
    Serve(child)
}

fn status(addr: SocketAddr) -> Value {
    let (code, body) = get(addr, "/status").expect("/status answers");
    assert_eq!(code, 200, "{body}");
    serde_json::from_str(&body).expect("/status is JSON")
}

/// Polls `/status` until at least `n` cycles finished.
fn after_cycles(addr: SocketAddr, n: u64, within: Duration) -> Value {
    let answer = poll(addr, "/status", within, |(code, body)| {
        *code == 200
            && serde_json::from_str::<Value>(body)
                .is_ok_and(|s| s["cycles"].as_u64().is_some_and(|c| c >= n))
    });
    let (_, body) = answer.expect("/status answers");
    let status: Value = serde_json::from_str(&body).expect("/status is JSON");
    assert!(
        status["cycles"].as_u64().is_some_and(|c| c >= n),
        "fewer than {n} cycles within {within:?}: {status}"
    );
    status
}

fn db(test: &str) -> Option<Db> {
    throwaway::db(test, "c35_serve")
}

/// One orphan ISSUED ticket (no job) issued two hours ago on a fresh tenant; returns the tenant.
fn seed_orphan(db: &mut Db, name: &str) -> String {
    let row = db
        .client()
        .query_one(
            "WITH t AS (INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id) \
             INSERT INTO projection.stream_log (tenant_id, scope_kind, scope_id, domain, projection_kind, \
               projection_version, stream_seq, commit_seq, state, issued_at) \
             SELECT tenant_id, 'workspace', gen_random_uuid(), 'code', 'retrieval_card', 'v1', 1, 1, 'ISSUED', \
               now() - interval '2 hours' FROM t RETURNING tenant_id::text",
            &[&name],
        )
        .expect("seed an orphan ticket");
    row.get(0)
}

fn ticket_state(db: &mut Db, tenant: &str) -> String {
    db.client()
        .query_one(
            "SELECT state FROM projection.stream_log WHERE tenant_id = $1::text::uuid",
            &[&tenant],
        )
        .expect("ticket row")
        .get(0)
}

fn task<'a>(status: &'a Value, name: &str) -> &'a Value {
    status["last_cycle"]
        .as_array()
        .and_then(|tasks| tasks.iter().find(|t| t["task"] == name))
        .unwrap_or_else(|| panic!("task {name} missing from last_cycle: {status}"))
}

/// T-B1: every key is required by name before any connection (§78.1). Fault: give CYCLE_SECONDS a default ⇒
/// removing it no longer names it (the run reaches the closed DSN instead) ⇒ red.
#[test]
fn serve_refuses_to_boot_without_each_key_naming_it() {
    let full = keys(free_addr(), CLOSED_DSN, 1, 10);
    for (i, (key, _)) in full.iter().enumerate() {
        let mut env = full.clone();
        env.remove(i);
        let out = serve_with(&env);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(2), "without {key}: {stderr}");
        assert!(stderr.contains(key.as_str()), "without {key}: {stderr}");
    }
    for (key, bad) in [
        (CYCLE_SECONDS, "0"),
        ("HUMAUX_MAINTENANCE_SERVE_TENANTS_PER_RUN", "0"),
        ("HUMAUX_MAINTENANCE_SERVE_LOST_LIMIT", "0"),
        // An EVERY below the cycle could never be honoured.
        ("HUMAUX_MAINTENANCE_SERVE_LOST_EVERY_SECONDS", "0"),
        // ADR-0062 D-N: a zero cool-down would reissue a ticket the moment it fails.
        ("HUMAUX_MAINTENANCE_SERVE_REISSUE_COOLDOWN_SECONDS", "0"),
        // ADR-0062 D-P: a zero cool-down would re-drive a death in the pass that killed it.
        ("HUMAUX_MAINTENANCE_SERVE_REDRIVE_COOLDOWN_SECONDS", "0"),
    ] {
        let env: Vec<(String, String)> = full
            .iter()
            .map(|(k, v)| (k.clone(), if k == key { bad.to_owned() } else { v.clone() }))
            .collect();
        let out = serve_with(&env);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(2), "{key}={bad}: {stderr}");
        assert!(stderr.contains(key), "{key}={bad}: {stderr}");
    }
}

/// T-B2 (ADR-0057 D-F): LOST_AFTER must exceed the gateway's lag key, or a stalled ticket turns LOST before it
/// ever reads as lag. Fault: drop the relation check ⇒ the run reaches the closed DSN ⇒ red.
#[test]
fn serve_refuses_lost_after_not_above_projection_lag() {
    for (lost_after, lag) in [("60", "60"), ("30", "60")] {
        let env: Vec<(String, String)> = keys(free_addr(), CLOSED_DSN, 1, 10)
            .into_iter()
            .map(|(k, v)| match k.as_str() {
                LOST_AFTER_SECONDS => (k, lost_after.to_owned()),
                LAG_SECONDS => (k, lag.to_owned()),
                _ => (k, v),
            })
            .collect();
        let out = serve_with(&env);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(2), "{lost_after}/{lag}: {stderr}");
        assert!(
            stderr.contains(LOST_AFTER_SECONDS) && stderr.contains(LAG_SECONDS),
            "names both keys: {stderr}"
        );
    }
}

/// T-A1: one cycle runs every due task over every tenant, sweeps the orphan LOST and answers 200; a failing door
/// makes the next cycles answer 503 naming the task, and the grant back makes it 200 again. Fault: the verdict
/// ignores failures ⇒ 200 after the revoke ⇒ red.
#[test]
fn one_cycle_answers_200_and_counts_each_due_task() {
    let Some(mut db) = db("one_cycle_answers_200_and_counts_each_due_task") else {
        return;
    };
    let orphan = seed_orphan(&mut db, "c35-serve-orphan");
    db.sql("INSERT INTO control.tenants (name) VALUES ('c35-serve-idle')");
    let tenants: i64 = db
        .client()
        .query_one("SELECT count(*) FROM control.tenants", &[])
        .expect("tenant count")
        .get(0);
    let addr = free_addr();
    let _serve = spawn(&keys(addr, &db.maintenance_dsn, 1, 1000));
    let status = after_cycles(addr, 1, Duration::from_secs(15));
    for name in LABELS {
        let run = task(&status, name);
        assert_eq!(run["ran"], true, "{name}: {status}");
        assert_eq!(run["tenants"].as_i64(), Some(tenants), "{name}: {status}");
        assert_eq!(run["failed"], 0, "{name}: {status}");
    }
    assert_eq!(task(&status, "lost")["affected"], 1, "{status}");
    // ADR-0061 D-B: the identity document every resident mode serves; `humaux-admin q degrade.counters` reads its
    // `degrade` block on every ops address (the card-35 rehearsal was red on its absence).
    assert_eq!(status["process"], "humaux-maintenance", "{status}");
    assert_eq!(status["mode"], "--serve", "{status}");
    assert!(
        status.get("git_sha").is_some() && status["started_at"].is_u64(),
        "{status}"
    );
    assert_eq!(
        status["degrade"].as_object().map(serde_json::Map::len),
        Some(11),
        "one entry per DegradeCode: {status}"
    );
    assert!(
        status["degrade"]["RerankModelMismatch"]["count"].is_u64(),
        "{status}"
    );
    assert_eq!(ticket_state(&mut db, &orphan), "LOST");
    let ready = poll(addr, "/metrics", Duration::from_secs(5), |(c, _)| *c == 200);
    let (code, body) = ready.expect("/metrics after a clean cycle");
    assert_eq!(code, 200, "ready after a clean cycle: {body}");
    // ADR-0062 D-S: one ok run per tenant door call, the swept orphan as one LOST row, every pair seeded.
    let lost_ok = sample(
        &body,
        r#"maintenance_task_runs_total{task="lost",outcome="ok"}"#,
    );
    assert!(lost_ok >= tenants, "a run per tenant call: {body}");
    assert_eq!(
        sample(&body, r#"maintenance_task_rows_total{task="lost"}"#),
        1,
        "{body}"
    );
    assert_eq!(
        sample(
            &body,
            r#"maintenance_task_runs_total{task="redrive",outcome="failed"}"#
        ),
        0,
        "{body}"
    );

    db.sql("REVOKE EXECUTE ON FUNCTION control.reap_quota_reservations(uuid,integer) FROM role_maintenance");
    let failed = poll(addr, "/metrics", Duration::from_secs(10), |(c, _)| {
        *c == 503
    });
    let (code, body) = failed.expect("/metrics after the revoke");
    assert_eq!(code, 503, "a failed call must not answer ready: {body}");
    assert!(
        body.contains("quota_reservations"),
        "503 names the task: {body}"
    );
    db.sql("GRANT EXECUTE ON FUNCTION control.reap_quota_reservations(uuid,integer) TO role_maintenance");
    let back = poll(addr, "/metrics", Duration::from_secs(10), |(c, _)| {
        *c == 200
    });
    let (code, body) = back.expect("/metrics after the re-grant");
    assert_eq!(code, 200, "recovers after the re-grant: {body}");
    assert!(
        sample(
            &body,
            r#"maintenance_task_runs_total{task="quota_reservations",outcome="failed"}"#
        ) > 0,
        "the revoked door's failed calls stay counted (§42 MaintenanceTaskFailing): {body}"
    );
}

/// The value of the exposition sample whose name and labels are exactly `series`.
fn sample(body: &str, series: &str) -> i64 {
    body.lines()
        .find_map(|l| l.strip_prefix(series)?.trim().parse::<f64>().ok())
        .map_or_else(
            || panic!("{series} missing from /metrics: {body}"),
            |v| v as i64,
        )
}

/// A loopback TCP proxy in front of PostgreSQL that the test can close (every live connection cut, port freed)
/// and reopen on the same port.
struct Proxy {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Proxy {
    fn open(listen: SocketAddr, upstream: String) -> Self {
        let listener = TcpListener::bind(listen).expect("bind the proxy port");
        listener.set_nonblocking(true).expect("nonblocking accept");
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            let live: Mutex<Vec<TcpStream>> = Mutex::new(Vec::new());
            while !flag.load(Ordering::Acquire) {
                let Ok((client, _)) = listener.accept() else {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                };
                // dep: PostgreSQL(any) — the real server behind the test's loopback proxy
                let Ok(server) = TcpStream::connect(&upstream) else {
                    continue;
                };
                client.set_nonblocking(false).expect("blocking client");
                let mut guard = live.lock().expect("live list");
                for (from, to) in [(&client, &server), (&server, &client)] {
                    let (mut from, mut to) = (
                        from.try_clone().expect("clone"),
                        to.try_clone().expect("clone"),
                    );
                    guard.push(from.try_clone().expect("clone"));
                    std::thread::spawn(move || {
                        let _ = std::io::copy(&mut from, &mut to);
                        let _ = to.shutdown(Shutdown::Both);
                    });
                }
            }
            for stream in live.lock().expect("live list").iter() {
                let _ = stream.shutdown(Shutdown::Both);
            }
        });
        Self {
            stop,
            thread: Some(thread),
        }
    }

    fn close(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.join().expect("proxy thread");
        }
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.close();
    }
}

/// `host:port` of a `postgres://user:pass@host:port/db?q` DSN, and the DSN with that authority replaced.
fn reroute(dsn: &str, to: SocketAddr) -> (String, String) {
    let at = dsn.rfind('@').expect("dsn has userinfo");
    let slash = at + dsn[at..].find('/').expect("dsn has a database path");
    let upstream = dsn[at + 1..slash].to_owned();
    (upstream, format!("{}@{to}{}", &dsn[..at], &dsn[slash..]))
}

/// T-A2: the daemon's database goes away (the proxy closes: live connections cut, port refused) ⇒ cycles fail,
/// `/metrics` answers 503 and the process stays alive; the port reopens ⇒ 200 without a restart. Fault: a cycle
/// failure returns `Err` ⇒ the process exits ⇒ red.
#[test]
fn a_closed_pg_port_fails_cycles_with_503_and_recovers_without_restart() {
    let Some(db) = db("a_closed_pg_port_fails_cycles_with_503_and_recovers_without_restart") else {
        return;
    };
    let port = free_addr();
    let (upstream, via_proxy) = reroute(&db.maintenance_dsn, port);
    let mut proxy = Proxy::open(port, upstream.clone());
    let addr = free_addr();
    let mut serve = spawn(&keys(addr, &via_proxy, 1, 100));
    let first = after_cycles(addr, 1, Duration::from_secs(15));
    assert_eq!(task(&first, "lost")["failed"], 0, "{first}");

    proxy.close();
    let failed = poll(addr, "/metrics", Duration::from_secs(15), |(c, _)| {
        *c == 503
    });
    let (code, body) = failed.expect("/metrics while PostgreSQL is unreachable");
    assert_eq!(code, 503, "{body}");
    assert!(
        serve.0.try_wait().expect("try_wait").is_none(),
        "the daemon exited on a failed cycle"
    );
    let cycles_down = status(addr)["cycles"].as_u64().expect("cycles");

    let _proxy = Proxy::open(port, upstream);
    let back = poll(addr, "/metrics", Duration::from_secs(20), |(c, _)| {
        *c == 200
    });
    assert_eq!(
        back.map(|(c, _)| c),
        Some(200),
        "recovers without a restart"
    );
    assert!(
        status(addr)["cycles"].as_u64().expect("cycles") > cycles_down,
        "cycles continue after the port reopens"
    );
    assert!(
        serve.0.try_wait().expect("try_wait").is_none(),
        "same process"
    );
}

/// T-A3 (ADR-0037 drain): SIGTERM while a long cycle is still issuing calls ⇒ exit 0 with the `stopped` receipt
/// within a quarter cycle, not at the end of the cycle. Fault: check the latch only between cycles ⇒ the exit
/// waits out the cycle budget ⇒ red.
#[test]
fn sigterm_between_calls_exits_0_with_a_stopped_receipt() {
    const CYCLE: u64 = 10;
    const TENANTS: u32 = 5000;
    let Some(mut db) = db("sigterm_between_calls_exits_0_with_a_stopped_receipt") else {
        return;
    };
    db.sql(&format!(
        "INSERT INTO control.tenants (name) SELECT 'c35-drain-' || g FROM generate_series(1, {TENANTS}) g"
    ));
    let addr = free_addr();
    let mut serve = spawn(&keys(addr, &db.maintenance_dsn, CYCLE, TENANTS));
    let running = poll(addr, "/status", Duration::from_secs(10), |(code, body)| {
        *code == 200 && serde_json::from_str::<Value>(body).is_ok_and(|s| s["in_cycle"] == true)
    });
    assert!(running.is_some(), "the first cycle never started");
    std::thread::sleep(Duration::from_secs(1));
    let mid = status(addr);
    assert_eq!(
        mid["in_cycle"], true,
        "the cycle must still be issuing calls: {mid}"
    );
    assert_eq!(mid["cycles"], 0, "{mid}");
    let (exit, took, receipt) = serve.terminate(Duration::from_secs(2 * CYCLE));
    println!("sigterm: exit={exit} took={took:?} receipt={receipt}");
    assert!(exit.success(), "exit {exit}");
    assert!(
        took < Duration::from_secs(CYCLE) / 4,
        "SIGTERM waited {took:?}: the latch must be checked between calls, not between cycles"
    );
    let receipt: Value = serde_json::from_str(receipt.trim()).expect("one JSON receipt");
    assert_eq!(receipt["outcome"], "stopped", "{receipt}");
    assert_eq!(receipt["command"], "--serve", "{receipt}");
    // dep: HTTP(loopback) — the closed ops port must refuse
    assert!(
        TcpStream::connect_timeout(&addr, Duration::from_secs(1)).is_err(),
        "the ops port is still open after exit"
    );
}

/// T-A4 (ADR-0062 D-A): the listener is up before the first cycle has finished, and until it has `/metrics` answers
/// 503 saying so — never 200 with zero counters; after the first clean cycle it answers 200. A long first cycle (5000
/// tenants, one page each, a 4 s budget) holds the window open. Fault: start the record as a good outcome
/// (`Last::ok()` in `serve`) ⇒ 200 mid-first-cycle ⇒ red.
#[test]
fn metrics_answers_503_until_the_first_cycle_finishes() {
    const CYCLE: u64 = 4;
    const TENANTS: u32 = 5000;
    let Some(mut db) = db("metrics_answers_503_until_the_first_cycle_finishes") else {
        return;
    };
    db.sql(&format!(
        "INSERT INTO control.tenants (name) SELECT 'c35-first-' || g FROM generate_series(1, {TENANTS}) g"
    ));
    let addr = free_addr();
    let _serve = spawn(&keys(addr, &db.maintenance_dsn, CYCLE, TENANTS));
    let running = poll(addr, "/status", Duration::from_secs(10), |(code, body)| {
        *code == 200
            && serde_json::from_str::<Value>(body)
                .is_ok_and(|s| s["in_cycle"] == true && s["cycles"] == 0)
    });
    assert!(running.is_some(), "the first cycle never started");
    let (code, body) = get(addr, "/metrics").expect("/metrics answers");
    let mid = status(addr);
    assert_eq!(
        mid["cycles"], 0,
        "the scrape must land inside the first cycle: {mid}"
    );
    println!("mid-first-cycle /metrics => {code}: {body}");
    assert_eq!(code, 503, "booted is not ready: {body}");
    assert!(body.contains("pending"), "503 names the state: {body}");
    after_cycles(addr, 1, Duration::from_secs(30));
    let ready = poll(addr, "/metrics", Duration::from_secs(10), |(c, _)| {
        *c == 200
    });
    assert_eq!(
        ready.as_ref().map(|(c, _)| *c),
        Some(200),
        "ready after the first clean cycle: {ready:?}"
    );
}

/// T-D1 (ADR-0062 D-D): walking `control.maintenance_tenant_page` as role_maintenance with a small page visits
/// every tenant exactly once and ends on a short page; `p_limit <= 0` is refused. Fault: `>=` for `>` in the
/// keyset predicate ⇒ the cursor tenant repeats ⇒ red.
#[test]
fn the_tenant_page_walks_every_tenant_once_per_rotation() {
    let Some(mut db) = db("the_tenant_page_walks_every_tenant_once_per_rotation") else {
        return;
    };
    db.sql(
        "INSERT INTO control.tenants (name) SELECT 'c35-page-' || g FROM generate_series(1, 7) g",
    );
    let client = db.client();
    let mut all: Vec<String> = client
        .query("SELECT tenant_id::text FROM control.tenants", &[])
        .expect("every tenant (owner)")
        .iter()
        .map(|r| r.get(0))
        .collect();
    all.sort();
    let mut txn = client.transaction().expect("txn");
    // dep: PostgreSQL(role_maintenance) — role switch: the page is walked as the daemon's role
    txn.batch_execute("SET LOCAL ROLE role_maintenance")
        .expect("act as role_maintenance");
    let mut seen: Vec<String> = Vec::new();
    let mut after: Option<String> = None;
    for _ in 0..=all.len() {
        let page: Vec<String> = txn
            .query(
                "SELECT t::text FROM control.maintenance_tenant_page($1::text::uuid, 3) AS t",
                &[&after],
            )
            .expect("tenant page as role_maintenance")
            .iter()
            .map(|r| r.get(0))
            .collect();
        seen.extend(page.iter().cloned());
        if page.len() < 3 {
            break;
        }
        after = page.last().cloned();
    }
    assert_eq!(
        seen, all,
        "one rotation = every tenant once, in tenant_id order"
    );
    let refused = txn
        .query("SELECT control.maintenance_tenant_page(NULL, 0)", &[])
        .expect_err("p_limit 0 is refused");
    assert_eq!(
        refused.code().map(|c| c.code()),
        Some("22023"),
        "{refused:?}"
    );
}

/// One purgeable row per purge door on `tenant`, each older than the one-hour ages of [`keys`].
fn seed_purgeable(db: &mut Db, tenant: &str) {
    db.sql(&format!(
        "WITH u AS (INSERT INTO control.users (user_id) VALUES (gen_random_uuid()) RETURNING user_id), \
              w AS (INSERT INTO control.workspaces (tenant_id, name) VALUES ('{tenant}', 'c35 once') \
                    RETURNING workspace_id) \
         INSERT INTO control.confirm_tokens (tenant_id, user_id, operation, target_id, nonce_sha256, issued_at, \
           expires_at, workspace_id) \
         SELECT '{tenant}', u.user_id, 'c35.once', gen_random_uuid(), \
                sha256(convert_to(gen_random_uuid()::text, 'UTF8')), now() - interval '1 day', \
                now() - interval '1 minute', w.workspace_id FROM u, w; \
         INSERT INTO ops.selection_snapshots (tenant_id, query_fingerprint, expires_at) \
           VALUES ('{tenant}', 'c35-once', now() - interval '1 minute'); \
         INSERT INTO control.rate_buckets (tenant_id, subject_kind, subject_id, operation, bucket_key, capacity, \
           tokens, refill_per_second, updated_at) \
           VALUES ('{tenant}', 'tenant', 'c35-once', 'mcp.read', 'default', 5, 5, 1, now() - interval '2 hours'); \
         INSERT INTO ops.jobs (tenant_id, job_type, status, idempotency_key, created_at) \
           VALUES ('{tenant}', 'c35.fixture', 'DONE', gen_random_uuid()::text, now() - interval '2 hours');"
    ));
}

/// T-Q1 (ADR-0062 D-Q): `sweep once` without the §77 fields exits 2 naming the first; with them it runs every task
/// for one page on its throwaway database, cadence keys absent, and prints one receipt with the per-task counts.
/// Fault: drop the §77 check ⇒ the fieldless run sweeps and exits 0 ⇒ red.
#[test]
fn sweep_once_requires_admin_fields_and_prints_per_task_counts() {
    let Some(mut db) = db("sweep_once_requires_admin_fields_and_prints_per_task_counts") else {
        return;
    };
    let orphan = seed_orphan(&mut db, "c35-once");
    seed_purgeable(&mut db, &orphan);
    let env: Vec<(String, String)> = once_keys(&db.maintenance_dsn);
    let env: Vec<(&str, String)> = env.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();

    let refused = run(&["sweep", "once"], &env);
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert_eq!(refused.status.code(), Some(2), "{stderr}");
    assert!(stderr.contains("--actor"), "{stderr}");
    assert_eq!(
        ticket_state(&mut db, &orphan),
        "ISSUED",
        "a refused run writes nothing"
    );

    let mut args = vec!["sweep", "once"];
    args.extend(ADMIN);
    let out = run(&args, &env);
    let stdout = String::from_utf8_lossy(&out.stdout);
    println!("sweep once => {stdout}");
    assert_eq!(
        out.status.code(),
        Some(0),
        "{stdout} {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let receipt: Value = serde_json::from_str(stdout.trim()).expect("one JSON receipt");
    assert_eq!(receipt["command"], "sweep once", "{receipt}");
    assert_eq!(receipt["outcome"], "swept", "{receipt}");
    for name in LABELS {
        let run = receipt["tasks"]
            .as_array()
            .and_then(|tasks| tasks.iter().find(|t| t["task"] == name))
            .unwrap_or_else(|| panic!("{name} missing: {receipt}"));
        assert_eq!(run["ran"], true, "{name}: {receipt}");
        assert_eq!(run["failed"], 0, "{name}: {receipt}");
        // The orphan has no outbox carrier, so it reaches no memory and the reissue door has nothing to drain; no
        // distill job died of its schema, so the re-drive door has nothing to re-arm.
        let expected = u64::from(!matches!(
            name,
            "quota_reservations" | "provider_budgets" | "reissue" | "redrive"
        ));
        assert_eq!(run["affected"], expected, "{name}: {receipt}");
    }
    assert_eq!(ticket_state(&mut db, &orphan), "LOST");
    let left: i64 = db
        .client()
        .query_one(
            "SELECT (SELECT count(*) FROM control.confirm_tokens) + (SELECT count(*) FROM ops.selection_snapshots) \
                  + (SELECT count(*) FROM control.rate_buckets) + (SELECT count(*) FROM ops.jobs)",
            &[],
        )
        .expect("leftovers")
        .get(0);
    assert_eq!(left, 0, "every seeded purgeable row went through its door");
    let receipts: i64 = db
        .client()
        .query_one("SELECT count(*) FROM ops.maintenance_receipts", &[])
        .expect("receipts")
        .get(0);
    assert_eq!(receipts, 4, "one receipt per purge door that removed a row");
}

/// Starts one resident mode with `env` (every key it needs plus the migrator key) and returns its exit status and
/// stderr; panics when it is still running after 5 s, i.e. when it booted.
fn refused_boot(args: &[&str], env: &[(String, String)]) -> (Option<i32>, String) {
    // dep: subprocess(humaux-maintenance) — a resident mode that must refuse to boot
    let child = Command::new(BIN)
        .args(args)
        .env_clear()
        .envs(env.iter().map(|(k, v)| (k, v)))
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn a resident mode");
    let mut serve = Serve(child);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = serve.0.try_wait().expect("try_wait") {
            break status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{args:?} booted with HUMAUX_MIGRATOR_PG_DSN in its environment"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    let mut stderr = String::new();
    std::io::Read::read_to_string(serve.0.stderr.as_mut().expect("piped stderr"), &mut stderr)
        .expect("read stderr");
    (status.code(), stderr)
}

/// T-H3 (ADR-0063 D-H): both resident modes refuse to boot, exit 2, when their environment defines the superuser
/// migrator DSN at all (even empty); stderr names the key and never its value, and nothing listens. Fault: delete
/// the `resident::refuse_owner_credentials()` call from either mode ⇒ that mode boots against the throwaway ⇒ red.
#[test]
fn resident_modes_refuse_to_boot_with_the_migrator_dsn_in_env() {
    const TEST: &str = "resident_modes_refuse_to_boot_with_the_migrator_dsn_in_env";
    const MIGRATOR: &str = "HUMAUX_MIGRATOR_PG_DSN";
    const DUMMY: &str = "postgres://refused@127.0.0.1:1/none";
    let Some(db) = throwaway::db(TEST, "c36_boot") else {
        return;
    };
    let serve_addr = free_addr();
    let health_addr = free_addr();
    let health_keys = |value: &str| {
        vec![
            (MAINTENANCE_DSN.to_owned(), db.maintenance_dsn.clone()),
            (
                "HUMAUX_MAINTENANCE_HEALTH_SERVE_METRICS_ADDR".to_owned(),
                health_addr.to_string(),
            ),
            (
                "HUMAUX_MAINTENANCE_HEALTH_SAMPLE_SECONDS".to_owned(),
                "1".to_owned(),
            ),
            (MIGRATOR.to_owned(), value.to_owned()),
        ]
    };
    let serve_keys = |value: &str| {
        let mut env = keys(serve_addr, &db.maintenance_dsn, 1, 100);
        env.push((MIGRATOR.to_owned(), value.to_owned()));
        env
    };
    let cases = [
        (vec!["--serve"], serve_keys(DUMMY), serve_addr),
        (vec!["--serve"], serve_keys(""), serve_addr),
        (vec!["health", "serve"], health_keys(DUMMY), health_addr),
        (vec!["health", "serve"], health_keys(""), health_addr),
    ];
    for (args, env, addr) in cases {
        let (code, stderr) = refused_boot(&args, &env);
        assert_eq!(code, Some(2), "{args:?}: {stderr}");
        assert!(
            stderr.contains(
                "boot refused: HUMAUX_MIGRATOR_PG_DSN is set in a resident mode's environment (ADR-0063 D-H)"
            ),
            "{args:?}: {stderr}"
        );
        assert!(
            !stderr.contains("refused@"),
            "{args:?} printed the value: {stderr}"
        );
        assert!(get(addr, "/metrics").is_none(), "{args:?} bound {addr}");
    }
}

/// The daemon's proposal state of every STAGE_RUNS registry row that has one: `(leaf, revision, proposed_at >=
/// effective_at of that revision)`, by leaf.
fn proposals(db: &mut Db) -> Vec<(String, i32, bool)> {
    db.client()
        .query(
            "SELECT r.leaf_name, r.proposed_policy_revision, r.proposed_at >= p.effective_at \
               FROM control.partition_registry r JOIN control.retention_policies p \
                 ON p.table_key = r.table_key AND p.policy_revision = r.proposed_policy_revision \
              WHERE r.table_key = 'STAGE_RUNS' AND r.proposed_at IS NOT NULL ORDER BY 1",
            &[],
        )
        .expect("proposals")
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect()
}

/// `true` when the exposition carries any `partition_horizon_months` sample.
fn has_horizon_sample(body: &str) -> bool {
    body.lines()
        .any(|l| l.starts_with("partition_horizon_months{"))
}

/// Asserts every closed table_key renders `months`.
fn assert_horizon(body: &str, months: i64) {
    for table in [
        "model_call_ledger",
        "stage_runs",
        "messages",
        "maintenance_receipts",
        "events",
        "audit_events",
    ] {
        assert_eq!(
            sample(
                body,
                &format!("partition_horizon_months{{table=\"{table}\"}}")
            ),
            months,
            "{table}: {body}"
        );
    }
}

/// T-K1 (ADR-0063 D-K): with revision 1 (1 month) superseded by revision 2 (2 months), the daemon proposes only the
/// leaf expired under revision 2, under revision 2; the newer expired month, the current month and the future months
/// are never proposed; every closed key renders its horizon (current month + 3 ⇒ 3) and the run counts once as ok.
/// Fault: propose under the first revision ⇒ the newer month is proposed (revision 1) ⇒ red.
#[test]
fn the_daemon_proposes_only_expired_months_of_the_latest_policy() {
    let Some(mut db) = throwaway::db(
        "the_daemon_proposes_only_expired_months_of_the_latest_policy",
        "c36_serve",
    ) else {
        return;
    };
    let c = db.client();
    let (_, oldest) = throwaway::past_leaf(c, "STAGE_RUNS", "ops.stage_runs", -3);
    throwaway::past_leaf(c, "STAGE_RUNS", "ops.stage_runs", -2);
    throwaway::effective_policy(c, "STAGE_RUNS", Some(1));
    throwaway::effective_policy(c, "STAGE_RUNS", Some(2));
    let addr = free_addr();
    let _serve = spawn(&keys(addr, &db.maintenance_dsn, 1, 1000));
    after_cycles(addr, 2, Duration::from_secs(15));
    assert_eq!(proposals(&mut db), vec![(oldest, 2, true)]);
    let ready = poll(addr, "/metrics", Duration::from_secs(5), |(c, _)| *c == 200);
    let (code, body) = ready.expect("/metrics");
    assert_eq!(code, 200, "{body}");
    assert_horizon(&body, 3);
    assert!(
        sample(
            &body,
            r#"maintenance_task_runs_total{task="partitions",outcome="ok"}"#
        ) >= 1,
        "{body}"
    );
}

/// T-K2 (ADR-0063 D-K, review finding 7): a policy approved with a future `effective_at` is not proposed by ticks
/// before it takes effect (an early forged proposal under the same revision stays as forged); after `effective_at`
/// the next tick proposes the expired month and re-proposes the forged one, both with `proposed_at >= effective_at`,
/// and `retention execute` drops each. Fault: drop the `effective_at <= clock_timestamp()` term from the proposer ⇒
/// an early tick proposes ⇒ red.
#[test]
fn a_future_effective_policy_is_proposed_after_it_takes_effect_and_executes() {
    let Some(mut db) = throwaway::db(
        "a_future_effective_policy_is_proposed_after_it_takes_effect_and_executes",
        "c36_serve",
    ) else {
        return;
    };
    let c = db.client();
    let (forged, forged_leaf) = throwaway::past_leaf(c, "STAGE_RUNS", "ops.stage_runs", -3);
    let (fresh, fresh_leaf) = throwaway::past_leaf(c, "STAGE_RUNS", "ops.stage_runs", -2);
    let effective: String = c
        .query_one(
            "SELECT to_char((clock_timestamp() + interval '8 seconds') AT TIME ZONE 'UTC', \
                            'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')",
            &[],
        )
        .expect("effective_at")
        .get(0);
    let owner = throwaway::with_db(&db.owner_dsn, &db.name);
    let retention = |args: &[&str]| {
        let mut all = vec!["retention"];
        all.extend(args);
        all.extend(ADMIN);
        run(&all, &[("HUMAUX_MIGRATOR_PG_DSN", owner.clone())])
    };
    let approved = retention(&[
        "approve",
        "--table",
        "STAGE_RUNS",
        "--months",
        "1",
        "--effective-at",
        &effective,
        "--lock-timeout-ms",
        "5000",
    ]);
    let stdout = String::from_utf8_lossy(&approved.stdout);
    assert_eq!(
        approved.status.code(),
        Some(0),
        "{stdout} {}",
        String::from_utf8_lossy(&approved.stderr)
    );
    let receipt: Value = serde_json::from_str(stdout.trim()).expect("approve receipt");
    let policy = receipt["policy_id"].as_str().expect("policy_id").to_owned();
    throwaway::forge_proposal(db.client(), &forged, 1, "clock_timestamp()");
    let addr = free_addr();
    let _serve = spawn(&keys(addr, &db.maintenance_dsn, 1, 1000));
    after_cycles(addr, 2, Duration::from_secs(6));
    let before: bool = db
        .client()
        .query_one(
            "SELECT clock_timestamp() < effective_at FROM control.retention_policies WHERE policy_id = $1::text::uuid",
            &[&policy],
        )
        .expect("clock")
        .get(0);
    assert!(
        before,
        "two ticks must finish before effective_at for this test to mean anything"
    );
    assert_eq!(
        proposals(&mut db),
        vec![(forged_leaf.clone(), 1, false)],
        "no tick before effective_at proposes; the early forge stays as it was"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let expected = vec![(forged_leaf, 1, true), (fresh_leaf, 1, true)];
    while proposals(&mut db) != expected {
        assert!(
            std::time::Instant::now() < deadline,
            "not (re-)proposed after effective_at: {:?}",
            proposals(&mut db)
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("c36_t_k2");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("export dir");
    let dir = dir.display().to_string();
    for registry in [&forged, &fresh] {
        let out = retention(&[
            "execute",
            "--policy",
            &policy,
            "--registry-id",
            registry,
            "--export-dir",
            &dir,
            "--lock-timeout-ms",
            "5000",
        ]);
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert_eq!(
            out.status.code(),
            Some(0),
            "{stdout} {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(stdout.contains(r#""outcome":"dropped""#), "{stdout}");
    }
}

/// T-K3 (ADR-0063 D-K, D-F, review finding 8): with PARTITIONS every 4 cycles, a run that fails (SELECT on the
/// registry revoked) counts `{task="partitions",outcome="failed"}` and removes every `partition_horizon_months`
/// sample, so the next clean cycle's 200 renders the family header with no sample (`PartitionHorizonAbsent`), never
/// the last value; after the re-grant the family is back with the right values. Fault: skip
/// `reset_partition_horizon_months()` ⇒ the stale samples render under 200 ⇒ red.
#[test]
fn a_failed_partitions_run_removes_the_horizon_family_and_counts_failed() {
    let Some(mut db) = throwaway::db(
        "a_failed_partitions_run_removes_the_horizon_family_and_counts_failed",
        "c36_serve",
    ) else {
        return;
    };
    let addr = free_addr();
    let env: Vec<(String, String)> = keys(addr, &db.maintenance_dsn, 1, 1000)
        .into_iter()
        .map(|(k, v)| {
            if k == PARTITIONS_EVERY {
                (k, "4".to_owned())
            } else {
                (k, v)
            }
        })
        .collect();
    let _serve = spawn(&env);
    let first = poll(addr, "/metrics", Duration::from_secs(15), |(c, b)| {
        *c == 200 && has_horizon_sample(b)
    });
    let (code, body) = first.expect("/metrics after the first run");
    assert_eq!(code, 200, "{body}");
    assert_horizon(&body, 3);

    db.sql("REVOKE SELECT ON control.partition_registry FROM role_maintenance");
    let failed_series = r#"maintenance_task_runs_total{task="partitions",outcome="failed"}"#;
    let failed = poll(addr, "/metrics", Duration::from_secs(20), |(c, b)| {
        *c == 200
            && b.lines()
                .find_map(|l| l.strip_prefix(failed_series)?.trim().parse::<f64>().ok())
                .is_some_and(|n| n >= 1.0)
    });
    let (code, body) = failed.expect("/metrics after a failed run");
    assert_eq!(code, 200, "a clean cycle after the failed run: {body}");
    assert!(sample(&body, failed_series) >= 1, "{body}");
    assert!(
        body.contains("# TYPE partition_horizon_months gauge"),
        "the header stays: {body}"
    );
    assert!(
        !has_horizon_sample(&body),
        "a failed run leaves no horizon sample: {body}"
    );

    db.sql("GRANT SELECT ON control.partition_registry TO role_maintenance");
    let back = poll(addr, "/metrics", Duration::from_secs(20), |(c, b)| {
        *c == 200 && has_horizon_sample(b)
    });
    let (code, body) = back.expect("/metrics after the re-grant");
    assert_eq!(code, 200, "{body}");
    assert_horizon(&body, 3);
}
