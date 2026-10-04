//! `private-worker::tests::ops_listener` — card 34 / ADR-0061 D-B, E10 against the BINARY: `--serve-rpc` and
//!   `--distill-serve` each open their own loopback ops listener from their own key (before any database work), it
//!   serves zero families and a `/status` naming the mode, `--distill-once` opens none, and `--metrics-families`
//!   reads no configuration.
//! Depends-on: crates=[]; services=[HTTP(loopback), subprocess(humaux-private-worker)];
//!   env=[CARGO_BIN_EXE_humaux-private-worker, HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID,
//!   HUMAUX_PRIVATE_WORKER_CREDENTIALS, HUMAUX_PRIVATE_WORKER_DISTILL_POLL_INTERVAL_SECS,
//!   HUMAUX_PRIVATE_WORKER_DISTILL_SERVE_METRICS_ADDR, HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS,
//!   HUMAUX_PRIVATE_WORKER_HEALTH_RENEW_SECS, HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS,
//!   HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS, HUMAUX_PRIVATE_WORKER_REGIONS, HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH,
//!   HUMAUX_PRIVATE_WORKER_SERVE_RPC_METRICS_ADDR, PRIVATE_WORKER_PG_DSN]; modules=[]
//! Called-by: [cargo-test]
//! Invariants: [touches no shared state: PostgreSQL is a loopback port this test bound and never answers, so every
//!   mode is observed while it is still booting; every listener is a free loopback port taken at run time; the test
//!   kills only children it spawned]
//! Spec: Baseline §78.1; ADR-0061 D-B; ADR-0061 E10

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_humaux-private-worker");
const SERVE_RPC_KEY: &str = "HUMAUX_PRIVATE_WORKER_SERVE_RPC_METRICS_ADDR";
const DISTILL_SERVE_KEY: &str = "HUMAUX_PRIVATE_WORKER_DISTILL_SERVE_METRICS_ADDR";

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
    // dep: subprocess(humaux-private-worker) — a mode that exits on its own
    Command::new(BIN)
        .args(args)
        .env_clear()
        .envs(env.iter().map(|(k, v)| (*k, v.as_str())))
        .output()
        .expect("run humaux-private-worker")
}

fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// Every key the three distill / RPC modes read before connecting; PostgreSQL is `pg_port`.
fn boot_env(pg_port: u16) -> Vec<(&'static str, String)> {
    [
        (
            "PRIVATE_WORKER_PG_DSN",
            format!("postgres://role_private_worker@127.0.0.1:{pg_port}/humaux_thread_dev"),
        ),
        ("HUMAUX_PRIVATE_WORKER_HTTP_TIMEOUT_SECS", "5".into()),
        ("HUMAUX_PRIVATE_WORKER_HEALTH_RENEW_SECS", "1800".into()),
        ("HUMAUX_PRIVATE_WORKER_PERMIT_TTL_SECS", "30".into()),
        ("HUMAUX_PRIVATE_WORKER_CREDENTIALS", String::new()),
        ("HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS", String::new()),
        ("HUMAUX_PRIVATE_WORKER_REGIONS", String::new()),
        (
            "HUMAUX_PRIVATE_WORKER_DISTILL_POLL_INTERVAL_SECS",
            "1".into(),
        ),
        (
            "HUMAUX_PRIVATE_WORKER_RPC_SOCKET_PATH",
            format!("/tmp/hp-c34ops-{}.sock", std::process::id()),
        ),
        ("HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID", "0".into()),
        (SERVE_RPC_KEY, free_addr().to_string()),
        (DISTILL_SERVE_KEY, free_addr().to_string()),
    ]
    .into()
}

fn spawn(args: &[&str], env: &[(&str, String)]) -> Child {
    // dep: subprocess(humaux-private-worker) — a mode held in its database connect
    Command::new(BIN)
        .args(args)
        .env_clear()
        .envs(env.iter().map(|(k, v)| (*k, v.as_str())))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn humaux-private-worker")
}

fn addr_of(env: &[(&str, String)], key: &str) -> SocketAddr {
    env.iter()
        .find(|(k, _)| *k == key)
        .and_then(|(_, v)| v.parse().ok())
        .expect("ops address in env")
}

/// T-W4 (private half, E10): `--metrics-families` with an empty environment prints zero families and exits 0.
#[test]
fn metrics_families_prints_zero_families_without_env() {
    let out = run(&["--metrics-families"], &[]);
    assert!(out.status.success(), "{}", text(&out));
    assert_eq!(String::from_utf8_lossy(&out.stdout), "", "{}", text(&out));
}

/// T-W1: each resident mode refuses to boot without ITS OWN key and names it; with only the `--serve-rpc` key set,
/// `--distill-serve` still exits naming its own. Fault: default the address, or read one shared key ⇒ red.
#[test]
fn each_resident_mode_needs_its_own_metrics_key() {
    for (mode, key) in [
        ("--serve-rpc", SERVE_RPC_KEY),
        ("--distill-serve", DISTILL_SERVE_KEY),
    ] {
        let out = run(&[mode], &[]);
        assert!(!out.status.success(), "{mode} booted without {key}");
        assert!(text(&out).contains(key), "{mode}: {}", text(&out));
    }
    let out = run(
        &["--distill-serve"],
        &[(SERVE_RPC_KEY, free_addr().to_string())],
    );
    assert!(
        !out.status.success(),
        "--distill-serve booted on the --serve-rpc key"
    );
    assert!(text(&out).contains(DISTILL_SERVE_KEY), "{}", text(&out));
    // ADR-0061 review-fix 3 (F9): port 0 would bind a random port Prometheus cannot target. Fault: accept port 0
    // in `telemetry::metrics::parse_ops_addr` ⇒ the mode gets past its key ⇒ red.
    for (mode, key) in [
        ("--serve-rpc", SERVE_RPC_KEY),
        ("--distill-serve", DISTILL_SERVE_KEY),
    ] {
        let out = run(&[mode], &[(key, "127.0.0.1:0".to_owned())]);
        assert!(!out.status.success(), "{mode} booted on port 0");
        assert!(text(&out).contains(key), "{mode}: {}", text(&out));
    }
}

/// T-W2 / T-W3 (private, E10): while each mode is held in its database connect, `--serve-rpc` and
/// `--distill-serve` answer `/metrics` 200 with zero families and `/status` naming the mode; `--distill-once` with
/// both keys set opens no listener. Fault: bind in `--distill-once` ⇒ red; drop the listener handle early ⇒ red.
#[test]
fn resident_modes_open_their_listener_before_the_database_and_one_shots_none() {
    let stalling = TcpListener::bind("127.0.0.1:0").expect("bind a stalling PostgreSQL port");
    let pg_port = stalling.local_addr().expect("stalling port").port();
    for (mode, key, word) in [
        ("--serve-rpc", SERVE_RPC_KEY, "serve-rpc"),
        ("--distill-serve", DISTILL_SERVE_KEY, "distill-serve"),
    ] {
        let env = boot_env(pg_port);
        let addr = addr_of(&env, key);
        let mut child = spawn(&[mode], &env);
        let deadline = Instant::now() + Duration::from_secs(30);
        let body = loop {
            if let Some((200, body)) = get(addr, "/metrics") {
                break body;
            }
            if let Some(status) = child.try_wait().expect("poll child") {
                panic!("{mode} exited ({status}) before its ops listener answered");
            }
            assert!(
                Instant::now() < deadline,
                "{mode}: /metrics never answered on {addr}"
            );
            std::thread::sleep(Duration::from_millis(100));
        };
        let status = get(addr, "/status");
        let _ = child.kill();
        let _ = child.wait();
        assert!(!body.contains("# TYPE"), "{mode}: {body}");
        let (code, status) = status.expect("/status answers");
        assert_eq!(code, 200, "{status}");
        assert!(
            status.contains("\"process\":\"humaux-private-worker\"")
                && status.contains(&format!("\"mode\":\"{word}\"")),
            "{status}"
        );
    }
    let env = boot_env(pg_port);
    let mut child = spawn(&["--distill-once"], &env);
    std::thread::sleep(Duration::from_secs(2));
    let alive = child.try_wait().expect("poll child").is_none();
    let open = [SERVE_RPC_KEY, DISTILL_SERVE_KEY]
        .iter()
        .any(|key| get(addr_of(&env, key), "/metrics").is_some());
    let _ = child.kill();
    let _ = child.wait();
    assert!(
        alive,
        "--distill-once exited before the check; the stalling port did not hold it"
    );
    assert!(!open, "--distill-once opened an ops listener");
    drop(stalling);
}
