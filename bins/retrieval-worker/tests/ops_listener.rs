//! `retrieval-worker::tests::ops_listener` — card 34 / ADR-0061 D-B, D-C against the BINARY: each resident mode opens
//!   its own loopback ops listener from its own key, `/metrics` carries the four `retrieval_provider_*` families
//!   seeded over their closed label sets, the one-shot modes open none, `--metrics-families` reads no configuration.
//! Depends-on: crates=[humaux-testkit]; services=[HTTP(loopback), subprocess(humaux-retrieval-worker),
//!   subprocess(kill)]; env=[CARGO_BIN_EXE_humaux-retrieval-worker, HUMAUX_RETRIEVAL_WORKER_BACKOFF_BASE_SECS,
//!   HUMAUX_RETRIEVAL_WORKER_BACKOFF_MAX_SECS, HUMAUX_RETRIEVAL_WORKER_BATCH, HUMAUX_RETRIEVAL_WORKER_CALLER,
//!   HUMAUX_RETRIEVAL_WORKER_CELL_ID, HUMAUX_RETRIEVAL_WORKER_DIMENSION, HUMAUX_RETRIEVAL_WORKER_EGRESS_PROCESSOR_ID,
//!   HUMAUX_RETRIEVAL_WORKER_EMBEDDING_MODEL, HUMAUX_RETRIEVAL_WORKER_EMBEDDING_PROVIDER,
//!   HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION, HUMAUX_RETRIEVAL_WORKER_GATEWAY_UID,
//!   HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN, HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256,
//!   HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION, HUMAUX_RETRIEVAL_WORKER_LEASE_SECS,
//!   HUMAUX_RETRIEVAL_WORKER_MAX_ATTEMPTS, HUMAUX_RETRIEVAL_WORKER_MAX_INPUT_TOKENS,
//!   HUMAUX_RETRIEVAL_WORKER_MODEL_REVISION, HUMAUX_RETRIEVAL_WORKER_PER_TENANT_CAP, HUMAUX_RETRIEVAL_WORKER_PG_DSN,
//!   HUMAUX_RETRIEVAL_WORKER_POLL_INTERVAL_SECS, HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR,
//!   HUMAUX_RETRIEVAL_WORKER_QDRANT_HOST, HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT, HUMAUX_RETRIEVAL_WORKER_QDRANT_TLS,
//!   HUMAUX_RETRIEVAL_WORKER_REGION, HUMAUX_RETRIEVAL_WORKER_RPC_SOCKET_PATH,
//!   HUMAUX_RETRIEVAL_WORKER_SERVE_METRICS_ADDR, HUMAUX_RETRIEVAL_WORKER_SERVE_RPC_METRICS_ADDR,
//!   HUMAUX_TEST_GITLEAKS_BIN, HUMAUX_TEST_GITLEAKS_SHA256, HUMAUX_TEST_GITLEAKS_VERSION]; modules=[humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [every listener is a free loopback port taken at run time; `--serve` and the one-shot modes see
//!   PostgreSQL and Qdrant only on ports this test bound (closed or stalling); `--serve-rpc` only connects as
//!   role_retrieval_worker and is never sent a request; the test signals only children it spawned; a missing env is
//!   a failure under HUMAUX_REQUIRE_DB, a named SKIP otherwise (§79.2)]
//! Spec: Baseline §41.2; §78.1; §79.2; ADR-0061 D-B; ADR-0061 D-C

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use humaux_testkit::{ExternalDep, skip_or_fail};

const BIN: &str = env!("CARGO_BIN_EXE_humaux-retrieval-worker");
const SERVE_RPC_KEY: &str = "HUMAUX_RETRIEVAL_WORKER_SERVE_RPC_METRICS_ADDR";
const SERVE_KEY: &str = "HUMAUX_RETRIEVAL_WORKER_SERVE_METRICS_ADDR";
/// §41.2: the four families this process counts.
const FAMILIES: [&str; 4] = [
    "retrieval_provider_requests_total",
    "retrieval_provider_latency_seconds",
    "retrieval_provider_tokens_total",
    "retrieval_provider_cost_total",
];

/// A loopback address that was bound and released: free for the child, refused while nothing listens.
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

fn refuses(addr: SocketAddr) -> bool {
    // dep: HTTP(loopback) — probes that no listener is bound
    TcpStream::connect_timeout(&addr, Duration::from_secs(1)).is_err()
}

fn run(args: &[&str], env: &[(&str, String)]) -> Output {
    // dep: subprocess(humaux-retrieval-worker) — a mode that exits on its own
    Command::new(BIN)
        .args(args)
        .env_clear()
        .envs(env.iter().map(|(k, v)| (*k, v.as_str())))
        .output()
        .expect("run humaux-retrieval-worker")
}

fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// The pinned gitleaks triple as the worker's own keys, or `None` after a visible SKIP (a failure under
/// `HUMAUX_REQUIRE_DB=1`).
fn gitleaks(test: &str) -> Option<Vec<(&'static str, String)>> {
    let mut env = Vec::new();
    for (key, test_key) in [
        (
            "HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN",
            "HUMAUX_TEST_GITLEAKS_BIN",
        ),
        (
            "HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION",
            "HUMAUX_TEST_GITLEAKS_VERSION",
        ),
        (
            "HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256",
            "HUMAUX_TEST_GITLEAKS_SHA256",
        ),
    ] {
        let Some(value) = std::env::var(test_key).ok().filter(|v| !v.is_empty()) else {
            skip_or_fail(
                test,
                &format!("missing object: {test_key}"),
                ExternalDep::Postgres,
            );
            return None;
        };
        env.push((key, value));
    }
    Some(env)
}

/// Every key `--serve` / `--run-once` / `--readyz` need, PostgreSQL at `pg_port` and Qdrant on a closed port.
fn projection_env(
    pg_port: u16,
    gitleaks: &[(&'static str, String)],
) -> Vec<(&'static str, String)> {
    let mut env: Vec<(&'static str, String)> = [
        (
            "HUMAUX_RETRIEVAL_WORKER_PG_DSN",
            format!("postgres://role_retrieval_worker@127.0.0.1:{pg_port}/humaux_thread_dev"),
        ),
        (
            "HUMAUX_RETRIEVAL_WORKER_EMBEDDING_PROVIDER",
            "dashscope".into(),
        ),
        (
            "HUMAUX_RETRIEVAL_WORKER_EMBEDDING_MODEL",
            "ops-test-model".into(),
        ),
        ("HUMAUX_RETRIEVAL_WORKER_MODEL_REVISION", "r1".into()),
        ("HUMAUX_RETRIEVAL_WORKER_DIMENSION", "4".into()),
        (
            "HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION",
            "ops-test@r1".into(),
        ),
        ("HUMAUX_RETRIEVAL_WORKER_REGION", "cn-beijing".into()),
        ("HUMAUX_RETRIEVAL_WORKER_MAX_INPUT_TOKENS", "8192".into()),
        (
            "HUMAUX_RETRIEVAL_WORKER_EGRESS_PROCESSOR_ID",
            "0190c34a-0000-7000-8000-000000000034".into(),
        ),
        (
            "HUMAUX_RETRIEVAL_WORKER_CELL_ID",
            "00000000-0000-7000-8000-0000000c3402".into(),
        ),
        (
            "HUMAUX_RETRIEVAL_WORKER_CALLER",
            "humaux-retrieval-worker".into(),
        ),
        ("HUMAUX_RETRIEVAL_WORKER_QDRANT_HOST", "127.0.0.1".into()),
        (
            "HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT",
            free_addr().port().to_string(),
        ),
        ("HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR", "127.0.0.0/8".into()),
        ("HUMAUX_RETRIEVAL_WORKER_QDRANT_TLS", "false".into()),
        ("HUMAUX_RETRIEVAL_WORKER_BATCH", "4".into()),
        ("HUMAUX_RETRIEVAL_WORKER_POLL_INTERVAL_SECS", "1".into()),
        ("HUMAUX_RETRIEVAL_WORKER_LEASE_SECS", "60".into()),
        ("HUMAUX_RETRIEVAL_WORKER_PER_TENANT_CAP", "2".into()),
        ("HUMAUX_RETRIEVAL_WORKER_MAX_ATTEMPTS", "3".into()),
        ("HUMAUX_RETRIEVAL_WORKER_BACKOFF_BASE_SECS", "1".into()),
        ("HUMAUX_RETRIEVAL_WORKER_BACKOFF_MAX_SECS", "2".into()),
    ]
    .into();
    env.extend(gitleaks.iter().cloned());
    env
}

fn spawn(args: &[&str], env: &[(&str, String)]) -> Child {
    // dep: subprocess(humaux-retrieval-worker) — a resident mode under test
    Command::new(BIN)
        .args(args)
        .env_clear()
        .envs(env.iter().map(|(k, v)| (*k, v.as_str())))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn humaux-retrieval-worker")
}

/// Polls `/metrics` until 200, failing if the child exits first.
fn wait_metrics(child: &mut Child, addr: SocketAddr, what: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some((200, body)) = get(addr, "/metrics") {
            return body;
        }
        if let Some(status) = child.try_wait().expect("poll child") {
            panic!("{what} exited ({status}) before its ops listener answered");
        }
        assert!(
            Instant::now() < deadline,
            "{what}: /metrics never answered on {addr}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// SIGTERM, exit 0, then the port refuses.
fn terminate(mut child: Child, addr: SocketAddr, what: &str) {
    // dep: subprocess(kill) — SIGTERM to the child this test spawned (its own PID, never a pattern)
    let sent = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("run kill");
    assert!(sent.success());
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll child") {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("{what} did not exit after SIGTERM");
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(
        status.success(),
        "{what}: SIGTERM must exit 0, got {status}"
    );
    assert!(
        refuses(addr),
        "{what}: the ops port {addr} still accepts after exit"
    );
}

/// `# TYPE` family names and the number of sample lines of `family` (its `_bucket` lines for a histogram).
fn families_and_samples(body: &str, family: &str) -> (Vec<String>, usize) {
    let types = body
        .lines()
        .filter_map(|l| l.strip_prefix("# TYPE "))
        .map(|l| l.split(' ').next().unwrap_or("").to_owned())
        .collect();
    let samples = body
        .lines()
        .filter(|l| l.starts_with(&format!("{family}{{")))
        .count();
    (types, samples)
}

/// T-W4 (retrieval half): `--metrics-families` with an empty environment prints the four families, the requests
/// family seeded over provider × purpose × region × result (2 × 2 × 4 × 6), and exits 0.
#[test]
fn metrics_families_prints_the_four_provider_families_without_env() {
    let out = run(&["--metrics-families"], &[]);
    assert!(out.status.success(), "{}", text(&out));
    let body = String::from_utf8_lossy(&out.stdout);
    let (types, requests) = families_and_samples(&body, FAMILIES[0]);
    assert_eq!(types, FAMILIES, "{body}");
    assert_eq!(requests, 96, "{body}");
}

/// T-W1: each resident mode refuses to boot without ITS OWN key and names it; with only the `--serve-rpc` key set
/// (one shared key for both modes), `--serve` still exits naming `…_SERVE_METRICS_ADDR`, and `--serve-rpc` gets past
/// its key. A non-loopback address is refused naming the key. Fault: default the address ⇒ red.
#[test]
fn each_resident_mode_needs_its_own_metrics_key() {
    for (mode, key) in [("--serve", SERVE_KEY), ("--serve-rpc", SERVE_RPC_KEY)] {
        let out = run(&[mode], &[]);
        assert!(!out.status.success(), "{mode} booted without {key}");
        assert!(text(&out).contains(key), "{mode}: {}", text(&out));
    }
    let shared = [(SERVE_RPC_KEY, free_addr().to_string())];
    let out = run(&["--serve"], &shared);
    assert!(
        !out.status.success(),
        "--serve booted on the --serve-rpc key"
    );
    assert!(text(&out).contains(SERVE_KEY), "{}", text(&out));
    let out = run(&["--serve-rpc"], &shared);
    assert!(!text(&out).contains("METRICS_ADDR"), "{}", text(&out));

    let wide = [(SERVE_KEY, format!("0.0.0.0:{}", free_addr().port()))];
    let out = run(&["--serve"], &wide);
    assert!(
        !out.status.success(),
        "--serve bound a non-loopback ops address"
    );
    assert!(text(&out).contains(SERVE_KEY), "{}", text(&out));
    // ADR-0061 review-fix 3 (F9). Fault: accept port 0 in `telemetry::metrics::parse_ops_addr` ⇒ red.
    for (mode, key) in [("--serve", SERVE_KEY), ("--serve-rpc", SERVE_RPC_KEY)] {
        let out = run(&[mode], &[(key, "127.0.0.1:0".to_owned())]);
        assert!(!out.status.success(), "{mode} booted on port 0");
        assert!(text(&out).contains(key), "{mode}: {}", text(&out));
    }
}

/// T-W3: `--run-once` and `--readyz` open no listener even with both keys set. PostgreSQL is a port that accepts
/// and never answers, so each mode is checked while it is still running. Fault: bind in `--run-once` ⇒ red.
#[test]
fn one_shot_modes_open_no_listener() {
    let test = "one_shot_modes_open_no_listener";
    let Some(gitleaks) = gitleaks(test) else {
        return;
    };
    let stalling = TcpListener::bind("127.0.0.1:0").expect("bind a stalling PostgreSQL port");
    let pg_port = stalling.local_addr().expect("stalling port").port();
    for mode in ["--run-once", "--readyz"] {
        let (serve, rpc) = (free_addr(), free_addr());
        let mut env = projection_env(pg_port, &gitleaks);
        env.extend([
            (SERVE_KEY, serve.to_string()),
            (SERVE_RPC_KEY, rpc.to_string()),
        ]);
        let mut child = spawn(&[mode], &env);
        std::thread::sleep(Duration::from_secs(2));
        let alive = child.try_wait().expect("poll child").is_none();
        let open = !refuses(serve) || !refuses(rpc);
        let _ = child.kill();
        let _ = child.wait();
        assert!(
            alive,
            "{mode} exited before the check; the stalling port did not hold it"
        );
        assert!(!open, "{mode} opened an ops listener");
    }
    drop(stalling);
}

/// T-W2: both resident modes run at once from ONE environment carrying both keys, each on its own port.
/// `--serve-rpc` (connected as role_retrieval_worker) and `--serve` (PostgreSQL down) both answer `/metrics` with
/// the four provider families seeded over their closed sets and `/status` naming their own mode; SIGTERM closes
/// each port. Fault: render only recorded tuples ⇒ zero requests samples ⇒ red.
#[test]
fn both_modes_serve_the_four_provider_families_on_their_own_ports() {
    let test = "both_modes_serve_the_four_provider_families_on_their_own_ports";
    let Some(dsn) = std::env::var("HUMAUX_RETRIEVAL_WORKER_PG_DSN")
        .ok()
        .filter(|v| !v.is_empty())
    else {
        skip_or_fail(
            test,
            "missing object: HUMAUX_RETRIEVAL_WORKER_PG_DSN",
            ExternalDep::Postgres,
        );
        return;
    };
    let Some(gitleaks) = gitleaks(test) else {
        return;
    };
    let (serve, rpc) = (free_addr(), free_addr());
    let mut env = projection_env(free_addr().port(), &gitleaks);
    env.extend([
        (SERVE_KEY, serve.to_string()),
        (SERVE_RPC_KEY, rpc.to_string()),
    ]);
    let mut runner = spawn(&["--serve"], &env);

    // Short `/tmp` path: a macOS temp dir plus a name overflows `sockaddr_un`.
    let socket = format!("/tmp/hq-c34ops-{}.sock", std::process::id());
    env.retain(|(k, _)| *k != "HUMAUX_RETRIEVAL_WORKER_PG_DSN");
    env.extend([
        ("HUMAUX_RETRIEVAL_WORKER_PG_DSN", dsn),
        ("HUMAUX_RETRIEVAL_WORKER_RPC_SOCKET_PATH", socket.clone()),
        ("HUMAUX_RETRIEVAL_WORKER_GATEWAY_UID", "0".into()),
    ]);
    let mut listener = spawn(&["--serve-rpc"], &env);

    for (child, addr, mode) in [
        (&mut runner, serve, "serve"),
        (&mut listener, rpc, "serve-rpc"),
    ] {
        let body = wait_metrics(child, addr, mode);
        let (types, requests) = families_and_samples(&body, FAMILIES[0]);
        assert_eq!(types, FAMILIES, "{mode}: {body}");
        assert_eq!(requests, 96, "{mode}: {body}");
        let (code, status) = get(addr, "/status").expect("/status answers");
        assert_eq!(code, 200, "{status}");
        assert!(
            status.contains("\"process\":\"humaux-retrieval-worker\"")
                && status.contains(&format!("\"mode\":\"{mode}\"")),
            "{status}"
        );
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    while !std::path::Path::new(&socket).exists() {
        assert!(
            listener.try_wait().expect("poll --serve-rpc").is_none(),
            "--serve-rpc exited before binding its RPC socket"
        );
        assert!(
            Instant::now() < deadline,
            "--serve-rpc never bound {socket}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    terminate(runner, serve, "--serve");
    terminate(listener, rpc, "--serve-rpc");
    let _ = std::fs::remove_file(&socket);
}
