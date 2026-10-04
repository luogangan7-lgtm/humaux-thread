//! `consolidation-worker::tests::ops_listener` — card 34 / ADR-0061 D-B, E10 against the BINARY: `--serve` opens its
//!   own loopback ops listener from `HUMAUX_CONSOLIDATION_WORKER_SERVE_METRICS_ADDR` before any database work, it
//!   serves zero families and a `/status` naming the mode, `--run-once` opens none, and `--metrics-families` reads no
//!   configuration.
//! Depends-on: crates=[]; services=[HTTP(loopback), subprocess(humaux-consolidation-worker)];
//!   env=[CARGO_BIN_EXE_humaux-consolidation-worker, CONSOLIDATION_WORKER_PG_DSN, HUMAUX_CONSOLIDATION_WORKER_BATCH,
//!   HUMAUX_CONSOLIDATION_WORKER_CALL_TTL_SECS, HUMAUX_CONSOLIDATION_WORKER_DIAL_TIMEOUT_SECS,
//!   HUMAUX_CONSOLIDATION_WORKER_LEASE_SECS, HUMAUX_CONSOLIDATION_WORKER_MAX_ATTEMPTS,
//!   HUMAUX_CONSOLIDATION_WORKER_MAX_INPUTS, HUMAUX_CONSOLIDATION_WORKER_POLL_INTERVAL_SECS,
//!   HUMAUX_CONSOLIDATION_WORKER_RPC_SOCKET_PATH, HUMAUX_CONSOLIDATION_WORKER_SERVE_METRICS_ADDR]; modules=[]
//! Called-by: [cargo-test]
//! Invariants: [touches no shared state: PostgreSQL is a loopback port this test bound and never answers, so every
//!   mode is observed while it is still booting; every listener is a free loopback port taken at run time; the test
//!   kills only children it spawned]
//! Spec: Baseline §78.1; ADR-0061 D-B; ADR-0061 E10

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_humaux-consolidation-worker");
const SERVE_KEY: &str = "HUMAUX_CONSOLIDATION_WORKER_SERVE_METRICS_ADDR";

/// A loopback address that was bound and released.
fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("reserve a loopback port")
}

/// One `GET`; `None` when the port refuses.
fn get(addr: SocketAddr, path: &str) -> Option<(u16, String)> {
    // dep: HTTP(loopback) — the child's ops listener
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(1)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    write!(s, "GET {path} HTTP/1.1\r\nHost: {addr}\r\n\r\n").ok()?;
    let mut raw = String::new();
    s.read_to_string(&mut raw).ok()?;
    let code = raw.split_whitespace().nth(1)?.parse().ok()?;
    Some((code, raw.split_once("\r\n\r\n")?.1.to_owned()))
}

fn run(args: &[&str], env: &[(&str, String)]) -> Output {
    // dep: subprocess(humaux-consolidation-worker) — a mode that exits on its own
    Command::new(BIN)
        .args(args)
        .env_clear()
        .envs(env.iter().map(|(k, v)| (*k, v.as_str())))
        .output()
        .expect("run humaux-consolidation-worker")
}

fn spawn(args: &[&str], env: &[(&str, String)]) -> Child {
    // dep: subprocess(humaux-consolidation-worker) — a mode held in its database connect
    Command::new(BIN)
        .args(args)
        .env_clear()
        .envs(env.iter().map(|(k, v)| (*k, v.as_str())))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn humaux-consolidation-worker")
}

/// Every key `--serve` / `--run-once` read before connecting; PostgreSQL is `pg_port`.
fn boot_env(pg_port: u16, ops: SocketAddr) -> Vec<(&'static str, String)> {
    [
        (
            "CONSOLIDATION_WORKER_PG_DSN",
            format!("postgres://role_consolidation_worker@127.0.0.1:{pg_port}/humaux_thread_dev"),
        ),
        (
            "HUMAUX_CONSOLIDATION_WORKER_RPC_SOCKET_PATH",
            format!("/tmp/hc-c34ops-{}.sock", std::process::id()),
        ),
        ("HUMAUX_CONSOLIDATION_WORKER_CALL_TTL_SECS", "5".into()),
        ("HUMAUX_CONSOLIDATION_WORKER_DIAL_TIMEOUT_SECS", "5".into()),
        ("HUMAUX_CONSOLIDATION_WORKER_LEASE_SECS", "120".into()),
        ("HUMAUX_CONSOLIDATION_WORKER_BATCH", "16".into()),
        ("HUMAUX_CONSOLIDATION_WORKER_MAX_INPUTS", "1000".into()),
        ("HUMAUX_CONSOLIDATION_WORKER_MAX_ATTEMPTS", "5".into()),
        ("HUMAUX_CONSOLIDATION_WORKER_POLL_INTERVAL_SECS", "1".into()),
        (SERVE_KEY, ops.to_string()),
    ]
    .into()
}

/// T-W4 (consolidation half, E10): `--metrics-families` with an empty environment prints zero families, exit 0.
#[test]
fn metrics_families_prints_zero_families_without_env() {
    let out = run(&["--metrics-families"], &[]);
    assert!(out.status.success(), "{out:?}");
    assert!(out.stdout.is_empty(), "{out:?}");
}

/// T-W1: `--serve` refuses to boot without its key and names it; a non-loopback address or port 0 is refused
/// naming it. Fault: default the address, or accept port 0 in `telemetry::metrics::parse_ops_addr` ⇒ red.
#[test]
fn serve_needs_its_own_loopback_metrics_key() {
    for env in [
        Vec::new(),
        vec![(SERVE_KEY, format!("0.0.0.0:{}", free_addr().port()))],
        vec![(SERVE_KEY, "127.0.0.1:0".to_owned())],
    ] {
        let out = run(&["--serve"], &env);
        let text = String::from_utf8_lossy(&out.stderr);
        assert!(!out.status.success(), "--serve booted with {env:?}");
        assert!(text.contains(SERVE_KEY), "{text}");
    }
}

/// T-W2 / T-W3 (consolidation, E10): while `--serve` is held in its database connect it answers `/metrics` 200 with
/// zero families and `/status` naming the mode; `--run-once` with the key set opens no listener.
/// Fault: bind in `--run-once` ⇒ red.
#[test]
fn serve_opens_its_listener_before_the_database_and_run_once_none() {
    let stalling = TcpListener::bind("127.0.0.1:0").expect("bind a stalling PostgreSQL port");
    let pg_port = stalling.local_addr().expect("stalling port").port();

    let ops = free_addr();
    let mut child = spawn(&["--serve"], &boot_env(pg_port, ops));
    let deadline = Instant::now() + Duration::from_secs(30);
    let body = loop {
        if let Some((200, body)) = get(ops, "/metrics") {
            break body;
        }
        if let Some(status) = child.try_wait().expect("poll child") {
            panic!("--serve exited ({status}) before its ops listener answered");
        }
        assert!(
            Instant::now() < deadline,
            "/metrics never answered on {ops}"
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    let status = get(ops, "/status");
    let _ = child.kill();
    let _ = child.wait();
    assert!(!body.contains("# TYPE"), "{body}");
    let (code, status) = status.expect("/status answers");
    assert_eq!(code, 200, "{status}");
    assert!(
        status.contains("\"process\":\"humaux-consolidation-worker\"")
            && status.contains("\"mode\":\"serve\""),
        "{status}"
    );

    let ops = free_addr();
    let mut child = spawn(&["--run-once"], &boot_env(pg_port, ops));
    std::thread::sleep(Duration::from_secs(2));
    let alive = child.try_wait().expect("poll child").is_none();
    let open = get(ops, "/metrics").is_some();
    let _ = child.kill();
    let _ = child.wait();
    assert!(
        alive,
        "--run-once exited before the check; the stalling port did not hold it"
    );
    assert!(!open, "--run-once opened an ops listener");
    drop(stalling);
}
