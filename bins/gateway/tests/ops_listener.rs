//! `gateway::tests::ops_listener` — the real `humaux-gateway` binary's ops surface (ADR-0061 D-B, D-C, D-F): the
//!   loopback `/metrics` and `/status`, the dependency-truthful status-word `/readyz`, the readiness flip on a removed
//!   retrieval socket and on a PostgreSQL that refuses connections, the drain window, and no secret in `/status`.
//! Depends-on: crates=[humaux-testkit, postgres, serde_json, uuid]; services=[PostgreSQL(owner),
//!   HTTP(loopback), UDS(serve), subprocess(humaux-gateway), subprocess(kill)];
//!   env=[CARGO_BIN_EXE_humaux-gateway, HUMAUX_GATEWAY_METRICS_ADDR, HUMAUX_GATEWAY_PG_DSN, HUMAUX_TEST_PG_DSN,
//!   HUMAUX_TEST_QDRANT_PORT]; modules=[gateway::bootstrap, humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [every listener port is taken free at run time on 127.0.0.1 and every socket is the test's own; the
//!   PG-down case revokes CONNECT only on its own throwaway database humaux_thread_c34_gw_<pid>_<n>, dropped by the
//!   fixture's Drop even on panic; each spawned gateway is killed by its own Drop; a missing dependency env is a
//!   failure under HUMAUX_REQUIRE_DB, a named SKIP otherwise (§79.2)]
//! Spec: Baseline §4.4; §41.2; §57.1; §79.2; ADR-0061 D-B; ADR-0061 D-C; ADR-0061 D-F

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::net::UnixListener;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use humaux_gateway::bootstrap::registry;
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use serde_json::Value;
use uuid::Uuid;

const BIN: &str = env!("CARGO_BIN_EXE_humaux-gateway");
const PREFIX: &str = "HUMAUX_GATEWAY_";
const METRICS_ADDR: &str = "HUMAUX_GATEWAY_METRICS_ADDR";
/// The test's refresh interval; every "within" bound below is derived from it.
const INTERVAL: Duration = Duration::from_secs(1);
const START: Duration = Duration::from_secs(15);
const GATEWAY_FAMILIES: [&str; 9] = [
    "degrade_total",
    "humaux_retrieval_requests_total",
    "retrieval_completeness_total",
    "humaux_mcp_requests_total",
    "mcp_auth_attempts_total",
    "mcp_authz_denied_total",
    "mcp_quota_reservations_total",
    "mcp_bmo_consumed_total",
    "rate_limit_rejected_total",
];

static NEXT: AtomicUsize = AtomicUsize::new(0);

fn env_or_skip(test: &str, name: &str, dep: ExternalDep) -> Option<String> {
    match std::env::var(name).ok().filter(|v| !v.is_empty()) {
        Some(v) => Some(v),
        None => {
            skip_or_fail(test, &format!("missing object: {name}"), dep);
            None
        }
    }
}

/// A loopback port free at the moment of the call (the xtask e2e_onboard::free_port pattern).
fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("a free loopback port")
}

/// `/tmp` directly: a macOS temp dir plus a name overflows `sockaddr_un`'s path limit.
fn socket_path() -> String {
    format!(
        "/tmp/hq-c34gw-{}.sock",
        &Uuid::now_v7().simple().to_string()[20..]
    )
}

/// One HTTP/1.0 GET; `None` when nothing answers.
fn get(addr: SocketAddr, path: &str) -> Option<(u16, String, String)> {
    // dep: HTTP(loopback) — the gateway's BIND_ADDR or ops listener
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(1)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    write!(stream, "GET {path} HTTP/1.0\r\nHost: {addr}\r\n\r\n").ok()?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw).ok()?;
    let (head, body) = raw.split_once("\r\n\r\n")?;
    let code = head.split_whitespace().nth(1)?.parse().ok()?;
    Some((code, head.to_owned(), body.to_owned()))
}

/// Polls `GET path` until `done` holds or `within` elapses; returns the last answer.
fn poll(
    addr: SocketAddr,
    path: &str,
    within: Duration,
    done: impl Fn(&(u16, String, String)) -> bool,
) -> Option<(u16, String, String)> {
    let deadline = Instant::now() + within;
    loop {
        let answer = get(addr, path);
        if answer.as_ref().is_some_and(&done) || Instant::now() >= deadline {
            return answer;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn status(ops: SocketAddr) -> Value {
    let (code, _, body) = get(ops, "/status").expect("/status answers");
    assert_eq!(code, 200, "{body}");
    serde_json::from_str(&body).expect("/status is JSON")
}

/// Every key the gateway requires, semantic recall disabled.
fn base_env(pg_dsn: &str, bind: SocketAddr, ops: SocketAddr) -> BTreeMap<String, String> {
    let mut env: BTreeMap<String, String> = [
        ("BIND_ADDR", bind.to_string()),
        ("ALLOWED_HOSTS", bind.to_string()),
        ("ALLOWED_ORIGINS", format!("http://{bind}")),
        ("MAX_REQUEST_BODY_BYTES", "65536".into()),
        ("PG_DSN", pg_dsn.into()),
        ("CREDENTIAL_PEPPER_HEX", random_hex("")),
        ("TOKEN_HMAC_KEY", random_hex("")),
        ("TRUSTED_PROXY_CIDRS", String::new()),
        ("MAX_FORWARDED_HOPS", "1".into()),
        ("GLOBAL_DENYLIST", String::new()),
        ("GLOBAL_EMERGENCY_ALLOWLIST", String::new()),
        ("RESERVATION_TTL_SECONDS", "30".into()),
        ("HANDLER_TIMEOUT_SECONDS", "5".into()),
        ("FINALIZE_TIMEOUT_SECONDS", "2".into()),
        ("REPLAY_TTL_SECONDS", "60".into()),
        ("CONFIRM_TOKEN_TTL_SECONDS", "300".into()),
        ("UNDO_WINDOW_SECONDS", "86400".into()),
        ("MOOD_HALF_LIFE_SECONDS", "21600".into()),
        ("PROJECTION_LAG_SECONDS", "60".into()),
        ("ENUMERATION_TTL_SECONDS", "900".into()),
        ("ENUMERATION_MANIFEST_CAP", "1000".into()),
        ("REMEMBER_SCOPE_KIND", "workspace".into()),
        ("REMEMBER_DOMAIN", "knowledge".into()),
        ("REMEMBER_PROJECTION_KIND", "ingest".into()),
        ("REMEMBER_PROJECTION_VERSION", "v1".into()),
        ("REMEMBER_REASONING_DOMAIN_ID", Uuid::now_v7().to_string()),
        ("REMEMBER_TOKEN_TTL_SECONDS", "60".into()),
        ("REMEMBER_DATA_CLASS", "INTERNAL".into()),
        ("REMEMBER_VISIBILITY_CLASS", "WORKSPACE_SHARED".into()),
        ("REMEMBER_EVENT_KIND", "USER_MESSAGE".into()),
        ("CONTEXT_TOTAL_TOKENS", "2048".into()),
        ("CONTEXT_MANDATORY_TOKENS", "1024".into()),
        ("METRICS_ADDR", ops.to_string()),
        ("READINESS_REFRESH_SECONDS", INTERVAL.as_secs().to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (format!("{PREFIX}{k}"), v))
    .collect();
    for name in ["PREAUTH_IP", "CREDENTIAL", "USER", "TENANT", "OPERATION"] {
        env.insert(format!("{PREFIX}RATE_{name}_CAPACITY"), "100".into());
        env.insert(
            format!("{PREFIX}RATE_{name}_REFILL_PER_SECOND"),
            "100".into(),
        );
    }
    env
}

/// 32 bytes of hex: `marker` (itself hex) then random v7 hex.
fn random_hex(marker: &str) -> String {
    let random = format!("{}{}", Uuid::now_v7().simple(), Uuid::now_v7().simple());
    format!("{marker}{}", &random[marker.len()..])
}

/// Turns semantic recall on: the retrieval socket at `socket`, Qdrant at the test port.
fn with_semantic(env: &mut BTreeMap<String, String>, socket: &str, qdrant_port: &str) {
    for (k, v) in [
        ("RETRIEVAL_RPC_SOCKET_PATH", socket.to_owned()),
        ("RETRIEVAL_RPC_PERMIT_TTL_SECONDS", "30".into()),
        ("EMBEDDING_DIMENSION", "4".into()),
        ("EMBEDDING_VERSION", "embed-v1".into()),
        ("QDRANT_HOST", "127.0.0.1".into()),
        ("QDRANT_PORT", qdrant_port.to_owned()),
        ("QDRANT_CIDR", "127.0.0.1/32".into()),
        ("QDRANT_TLS", "false".into()),
        ("CELL_ID", Uuid::now_v7().to_string()),
        ("CALLER_ID", "gateway".into()),
    ] {
        env.insert(format!("{PREFIX}{k}"), v);
    }
}

/// The spawned gateway, killed if the test did not stop it itself.
struct Gateway {
    child: Child,
}

impl Gateway {
    /// Spawns with exactly `env` as its `HUMAUX_GATEWAY_*` set and waits for `/livez`.
    fn start(env: &BTreeMap<String, String>) -> Self {
        let bind = env[&format!("{PREFIX}BIND_ADDR")]
            .parse()
            .expect("bind addr");
        // dep: subprocess(humaux-gateway) — the binary under test
        let mut command = Command::new(BIN);
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with(PREFIX) {
                command.env_remove(key);
            }
        }
        // dep: subprocess(humaux-gateway) — the binary under test
        let child = command
            .envs(env)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn humaux-gateway");
        let mut gateway = Self { child };
        let deadline = Instant::now() + START;
        loop {
            if get(bind, "/livez").is_some_and(|(code, _, _)| code == 200) {
                return gateway;
            }
            if let Some(status) = gateway.child.try_wait().expect("inspect gateway") {
                panic!("gateway exited during boot: {status}");
            }
            assert!(Instant::now() < deadline, "gateway did not answer /livez");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn sigterm(&self) {
        // dep: subprocess(kill) — SIGTERM to the spawned gateway
        let ok = Command::new("/bin/kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("run kill")
            .success();
        assert!(ok, "kill -TERM");
    }

    fn wait_exit(&mut self, within: Duration) -> std::process::ExitStatus {
        let deadline = Instant::now() + within;
        loop {
            if let Some(status) = self.child.try_wait().expect("inspect gateway") {
                return status;
            }
            assert!(Instant::now() < deadline, "gateway did not exit");
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A std `UnixListener` that answers every request with `answer` — the test's stand-in for the
/// worker's `GET /internal/v1/retrieval/readyz`. Dropping it stops the thread and removes the file.
struct FakeWorker {
    path: String,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl FakeWorker {
    fn bind(path: &str, answer: &'static str) -> Self {
        let _ = std::fs::remove_file(path);
        // dep: UDS(serve) — the test's own retrieval-worker stand-in
        let listener = UnixListener::bind(path).expect("bind the fake worker socket");
        listener.set_nonblocking(true).expect("nonblocking");
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut conn, _)) => {
                        let _ = conn.set_nonblocking(false);
                        let _ = conn.set_read_timeout(Some(Duration::from_secs(2)));
                        let mut head = Vec::new();
                        let mut chunk = [0u8; 1024];
                        while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                            match conn.read(&mut chunk) {
                                Ok(0) | Err(_) => break,
                                Ok(n) => head.extend_from_slice(&chunk[..n]),
                            }
                        }
                        let _ = conn.write_all(answer.as_bytes());
                    }
                    Err(_) => std::thread::sleep(Duration::from_millis(20)),
                }
            }
        });
        Self {
            path: path.to_owned(),
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for FakeWorker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let _ = std::fs::remove_file(&self.path);
    }
}

const WORKER_READY: &str =
    "HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nready\n";
const WORKER_PG_DOWN: &str = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 52\r\nConnection: close\r\n\r\nmissing object: PostgreSQL as role_retrieval_worker\n";

fn families_of(metrics: &str) -> Vec<&str> {
    metrics
        .lines()
        .filter_map(|l| l.strip_prefix("# TYPE "))
        .filter_map(|l| l.split(' ').next())
        .collect()
}

/// T-G1: the nine gateway families, every one seeded, on the loopback ops listener only — never on
/// BIND_ADDR (E5). Fault: drop the guard seeding ⇒ `mcp_authz_denied_total` has no sample ⇒ red.
#[test]
fn metrics_carry_the_nine_gateway_families_on_the_ops_listener_only() {
    let test = "metrics_carry_the_nine_gateway_families_on_the_ops_listener_only";
    let Some(dsn) = env_or_skip(test, "HUMAUX_GATEWAY_PG_DSN", ExternalDep::Postgres) else {
        return;
    };
    let (bind, ops) = (free_addr(), free_addr());
    let _gateway = Gateway::start(&base_env(&dsn, bind, ops));
    let (code, head, body) = get(ops, "/metrics").expect("/metrics answers");
    assert_eq!(code, 200, "{body}");
    assert!(
        head.contains("Content-Type: text/plain; version=0.0.4; charset=utf-8"),
        "{head}"
    );
    assert_eq!(families_of(&body), GATEWAY_FAMILIES);
    for family in GATEWAY_FAMILIES {
        assert!(
            body.lines().any(|l| l.starts_with(&format!("{family}{{"))),
            "{family} has no seeded sample"
        );
    }
    for path in ["/metrics", "/status"] {
        let (code, _, _) = get(bind, path).expect("BIND_ADDR answers");
        assert_ne!(
            code, 200,
            "{path} must not be served on BIND_ADDR (ADR-0061 E5)"
        );
    }
}

/// T-G2: the readiness flip on the retrieval socket. Faults: remove the refresh task ⇒ `/status`
/// keeps the boot-time pass ⇒ red; a dependency name in the `/readyz` body ⇒ red; a `connect()`-only
/// check ⇒ the worker answering 503 still reads ready ⇒ red.
#[test]
fn readyz_flips_with_the_retrieval_socket_and_names_it_only_in_status() {
    let test = "readyz_flips_with_the_retrieval_socket_and_names_it_only_in_status";
    let Some(dsn) = env_or_skip(test, "HUMAUX_GATEWAY_PG_DSN", ExternalDep::Postgres) else {
        return;
    };
    let Some(qdrant) = env_or_skip(test, "HUMAUX_TEST_QDRANT_PORT", ExternalDep::Qdrant) else {
        return;
    };
    let socket = socket_path();
    let worker = FakeWorker::bind(&socket, WORKER_READY);
    let (bind, ops) = (free_addr(), free_addr());
    let mut env = base_env(&dsn, bind, ops);
    with_semantic(&mut env, &socket, &qdrant);
    let _gateway = Gateway::start(&env);
    let flip = 2 * INTERVAL + Duration::from_millis(500);
    let ready = poll(bind, "/readyz", flip, |(c, _, _)| *c == 200).expect("/readyz");
    assert_eq!(
        (ready.0, ready.2.trim()),
        (200, r#"{"status":"ready"}"#),
        "{ready:?}"
    );

    drop(worker); // the listener and the socket file are gone
    let down = poll(bind, "/readyz", flip, |(c, _, _)| *c == 503).expect("/readyz");
    assert_eq!(
        (down.0, down.2.trim()),
        (503, r#"{"status":"not_ready"}"#),
        "{down:?}"
    );
    assert!(!down.2.contains(&socket) && !down.2.contains("retrieval_rpc"));
    let rpc = &status(ops)["readiness"]["retrieval_rpc"];
    assert_eq!(rpc["state"], "fail", "{rpc}");
    assert!(
        rpc["missing_object"]
            .as_str()
            .is_some_and(|m| m.contains(&socket)),
        "{rpc}"
    );

    let refusing = FakeWorker::bind(&socket, WORKER_PG_DOWN);
    let up = poll(bind, "/readyz", flip, |(c, _, _)| *c == 200);
    assert_eq!(
        up.as_ref().map(|a| a.0),
        Some(503),
        "a worker that answers 503 is not ready: {up:?}"
    );
    let rpc = &status(ops)["readiness"]["retrieval_rpc"];
    assert!(
        rpc["missing_object"]
            .as_str()
            .is_some_and(|m| m.contains("PostgreSQL as role_retrieval_worker")),
        "{rpc}"
    );
    drop(refusing);

    let _worker = FakeWorker::bind(&socket, WORKER_READY);
    let back = poll(bind, "/readyz", flip, |(c, _, _)| *c == 200).expect("/readyz");
    assert_eq!(back.0, 200, "{back:?}");
}

/// `postgres://user:pass@host:port/<db>?query` with the database replaced.
fn with_db(dsn: &str, db: &str) -> String {
    let (head, tail) = dsn.split_at(dsn.rfind('/').expect("dsn has a database path") + 1);
    let query = tail.find('?').map_or("", |i| &tail[i..]);
    format!("{head}{db}{query}")
}

/// A throwaway empty database, dropped WITH (FORCE) on every exit path.
struct ScratchDb {
    owner_dsn: String,
    name: String,
}

impl ScratchDb {
    fn create(owner_dsn: &str) -> Self {
        let name = format!(
            "humaux_thread_c34_gw_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        // dep: PostgreSQL(owner) — create this test's throwaway database
        let mut admin =
            Client::connect(&with_db(owner_dsn, "postgres"), NoTls).expect("owner connects");
        for sql in [
            format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"),
            format!("CREATE DATABASE {name}"),
        ] {
            admin
                .batch_execute(&sql)
                .expect("create the scratch database");
        }
        Self {
            owner_dsn: owner_dsn.to_owned(),
            name,
        }
    }

    fn admin(&self) -> Client {
        // dep: PostgreSQL(owner) — cluster-level statements on the scratch database
        Client::connect(&with_db(&self.owner_dsn, "postgres"), NoTls).expect("owner connects")
    }
}

impl Drop for ScratchDb {
    fn drop(&mut self) {
        let drop_db = format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name);
        // dep: PostgreSQL(owner) — drop this test's throwaway database
        match Client::connect(&with_db(&self.owner_dsn, "postgres"), NoTls) {
            Ok(mut admin) => {
                if let Err(e) = admin.batch_execute(&drop_db) {
                    eprintln!("ops_listener cleanup: {drop_db} failed: {e}");
                }
            }
            Err(e) => eprintln!("ops_listener cleanup: connect failed: {e}"),
        }
    }
}

/// T-G3: PostgreSQL refuses `role_gateway` after boot ⇒ 503 `not_ready`, and `/status` names `pg`.
/// Only the test's own throwaway database is touched. Fault: readiness reads only `accepting` ⇒ 200.
#[test]
fn readyz_is_not_ready_when_postgres_refuses_and_status_names_pg() {
    let test = "readyz_is_not_ready_when_postgres_refuses_and_status_names_pg";
    let Some(owner) = env_or_skip(test, "HUMAUX_TEST_PG_DSN", ExternalDep::Postgres) else {
        return;
    };
    let Some(gateway_dsn) = env_or_skip(test, "HUMAUX_GATEWAY_PG_DSN", ExternalDep::Postgres)
    else {
        return;
    };
    let db = ScratchDb::create(&owner);
    let (bind, ops) = (free_addr(), free_addr());
    let _gateway = Gateway::start(&base_env(&with_db(&gateway_dsn, &db.name), bind, ops));
    let flip = 2 * INTERVAL + Duration::from_millis(500);
    let ready = poll(bind, "/readyz", flip, |(c, _, _)| *c == 200).expect("/readyz");
    assert_eq!(ready.0, 200, "{ready:?}");
    assert_eq!(status(ops)["readiness"]["pg"]["state"], "pass");

    db.admin()
        .batch_execute(&format!(
            "REVOKE CONNECT ON DATABASE {0} FROM PUBLIC; REVOKE CONNECT ON DATABASE {0} FROM role_gateway;
             SELECT pg_terminate_backend(pid) FROM pg_stat_activity
              WHERE datname = '{0}' AND usename = 'role_gateway'",
            db.name
        ))
        .expect("refuse role_gateway on the scratch database");
    let down = poll(bind, "/readyz", flip, |(c, _, _)| *c == 503).expect("/readyz");
    assert_eq!(
        (down.0, down.2.trim()),
        (503, r#"{"status":"not_ready"}"#),
        "{down:?}"
    );
    let pg = &status(ops)["readiness"]["pg"];
    assert_eq!(pg["state"], "fail", "{pg}");
    assert!(
        pg["missing_object"]
            .as_str()
            .is_some_and(|m| m.contains("PostgreSQL as role_gateway")),
        "{pg}"
    );
}

/// T-G5: no secret reaches `/status`. The secret list is every `registry()` entry with
/// `secret == true` (both constructors), each given a distinct canary. Fault: serialize `value` for
/// the `entry_with_default` rows ⇒ the previous-pepper / previous-HMAC canary appears ⇒ red.
#[test]
fn status_never_carries_a_secret_value() {
    let test = "status_never_carries_a_secret_value";
    let Some(dsn) = env_or_skip(test, "HUMAUX_GATEWAY_PG_DSN", ExternalDep::Postgres) else {
        return;
    };
    let (bind, ops) = (free_addr(), free_addr());
    let mut env = base_env(&dsn, bind, ops);
    let secrets: Vec<String> = registry()
        .into_iter()
        .filter(|e| e.secret)
        .map(|e| e.name)
        .collect();
    assert!(secrets.len() >= 5, "{secrets:?}");
    let mut canaries = Vec::new();
    for (n, name) in secrets.iter().enumerate() {
        // Hex-valid so the hex keys still parse; the DSN carries it as its application_name.
        let canary = format!("c34ca{n:03x}");
        let value = if name.ends_with("_DSN") {
            let sep = if dsn.contains('?') { '&' } else { '?' };
            format!("{dsn}{sep}application_name={canary}")
        } else {
            random_hex(&canary)
        };
        env.insert(name.clone(), value);
        canaries.push(canary);
    }
    let _gateway = Gateway::start(&env);
    let (code, _, body) = get(ops, "/status").expect("/status answers");
    assert_eq!(code, 200, "{body}");
    for (name, canary) in secrets.iter().zip(&canaries) {
        assert!(
            !body.contains(canary.as_str()),
            "{name}'s canary {canary} is in /status"
        );
    }
    let doc: Value = serde_json::from_str(&body).expect("/status is JSON");
    let rows = doc["effective_config"]
        .as_array()
        .expect("effective_config");
    assert_eq!(rows.len(), registry().len());
    for name in &secrets {
        let row = rows
            .iter()
            .find(|r| r["name"] == name.as_str())
            .unwrap_or_else(|| panic!("{name} has no row"));
        assert_eq!(row["secret"], true, "{row}");
        assert!(row.get("value").is_none(), "{row}");
    }
    assert_eq!(doc["accepting"], true);
    for dep in ["pg", "retrieval_rpc", "qdrant"] {
        assert!(doc["readiness"][dep]["state"].is_string(), "{doc}");
    }
    assert_eq!(
        doc["degrade"].as_object().map(|o| o.len()),
        Some(11),
        "{doc}"
    );
    let (_, _, metrics) = get(ops, "/metrics").expect("ops answers");
    assert!(canaries.iter().all(|c| !metrics.contains(c.as_str())));
}

/// T-G6: a non-loopback ops address (and a missing one) is boot-fatal, naming the key. Needs no
/// database: the key is refused before any connection. Fault: delete the loopback refusal in
/// `bootstrap::parse_metrics_addr` ⇒ boot reaches PostgreSQL and names its DSN key instead ⇒ red.
/// ADR-0061 D-B: `serve_loopback`'s own refusal is unreachable from the gateway (the parser refuses
/// first); telemetry T-B2 and the worker T-W1 legs own that fault.
#[test]
fn a_non_loopback_or_missing_ops_address_exits_naming_the_key() {
    for value in [Some("0.0.0.0:19999"), None] {
        let mut env = base_env(
            "postgres://unused@127.0.0.1:1/none",
            free_addr(),
            free_addr(),
        );
        match value {
            Some(v) => env.insert(METRICS_ADDR.into(), v.into()),
            None => env.remove(METRICS_ADDR),
        };
        // dep: subprocess(humaux-gateway) — boot with a refused ops address
        let mut command = Command::new(BIN);
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with(PREFIX) {
                command.env_remove(key);
            }
        }
        // dep: subprocess(humaux-gateway) — boot with a refused ops address
        let out = command.envs(&env).output().expect("run humaux-gateway");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(!out.status.success(), "{value:?}: {stderr}");
        assert!(stderr.contains(METRICS_ADDR), "{value:?}: {stderr}");
    }
}

/// T-G7: SIGTERM ⇒ `/readyz` answers `draining` 503 while `/metrics` stays scrapable for the
/// window, then the ops port closes. Fault: drop the ops listener before the window ⇒ red.
#[test]
fn sigterm_drains_with_the_ops_listener_open_then_closes_it() {
    let test = "sigterm_drains_with_the_ops_listener_open_then_closes_it";
    let Some(dsn) = env_or_skip(test, "HUMAUX_GATEWAY_PG_DSN", ExternalDep::Postgres) else {
        return;
    };
    let (bind, ops) = (free_addr(), free_addr());
    let mut gateway = Gateway::start(&base_env(&dsn, bind, ops));
    gateway.sigterm();
    let draining = poll(bind, "/readyz", Duration::from_secs(4), |(c, _, _)| {
        *c == 503
    })
    .expect("/readyz answers while draining");
    assert_eq!(
        (draining.0, draining.2.trim()),
        (503, r#"{"status":"draining"}"#),
        "{draining:?}"
    );
    let metrics = get(ops, "/metrics").map(|a| a.0);
    assert_eq!(
        metrics,
        Some(200),
        "/metrics must stay up through the drain window"
    );
    let exit = gateway.wait_exit(Duration::from_secs(30));
    assert!(exit.success(), "{exit}");
    assert!(
        // dep: HTTP(loopback) — the closed ops port refuses
        TcpStream::connect_timeout(&ops, Duration::from_secs(1)).is_err(),
        "the ops port must be closed after exit"
    );
}

/// T-G8: `--metrics-families` reads no configuration: an empty environment prints the nine families.
#[test]
fn metrics_families_runs_with_an_empty_env() {
    // dep: subprocess(humaux-gateway) — the zero-state exposition
    let out = Command::new(BIN)
        .arg("--metrics-families")
        .env_clear()
        .output()
        .expect("run humaux-gateway --metrics-families");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert_eq!(families_of(&text), GATEWAY_FAMILIES);
}
