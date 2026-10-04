//! `retrieval-worker::tests::serve_drain` — Card 27 / ADR-0052 D-F + ADR-0037 — `humaux-retrieval-worker --serve`
//!   asserted against the BINARY: a database outage is a logged failed pass, not an exit, and SIGTERM ends it with 0.
//! Depends-on: crates=[]; services=[subprocess(humaux-retrieval-worker), subprocess(kill)];
//!   env=[CARGO_BIN_EXE_humaux-retrieval-worker, HUMAUX_REQUIRE_DB, HUMAUX_RETRIEVAL_WORKER_BACKOFF_BASE_SECS,
//!   HUMAUX_RETRIEVAL_WORKER_BACKOFF_MAX_SECS, HUMAUX_RETRIEVAL_WORKER_BATCH, HUMAUX_RETRIEVAL_WORKER_CALLER,
//!   HUMAUX_RETRIEVAL_WORKER_CELL_ID, HUMAUX_RETRIEVAL_WORKER_DIMENSION, HUMAUX_RETRIEVAL_WORKER_EGRESS_PROCESSOR_ID,
//!   HUMAUX_RETRIEVAL_WORKER_EMBEDDING_MODEL, HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION,
//!   HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN, HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256,
//!   HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION, HUMAUX_RETRIEVAL_WORKER_LEASE_SECS,
//!   HUMAUX_RETRIEVAL_WORKER_MAX_ATTEMPTS, HUMAUX_RETRIEVAL_WORKER_MODEL_REVISION,
//!   HUMAUX_RETRIEVAL_WORKER_PER_TENANT_CAP, HUMAUX_RETRIEVAL_WORKER_PG_DSN,
//!   HUMAUX_RETRIEVAL_WORKER_POLL_INTERVAL_SECS, HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR,
//!   HUMAUX_RETRIEVAL_WORKER_QDRANT_HOST, HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT, HUMAUX_RETRIEVAL_WORKER_QDRANT_TLS,
//!   HUMAUX_RETRIEVAL_WORKER_SERVE_METRICS_ADDR, HUMAUX_TEST_GITLEAKS_BIN, HUMAUX_TEST_GITLEAKS_SHA256, HUMAUX_TEST_GITLEAKS_VERSION]; modules=[]
//! Called-by: [cargo-test]
//! Invariants: [touches no shared state: PostgreSQL and Qdrant are closed loopback ports this test bound and released;
//!   it signals only the child it spawned; without the pinned gitleaks triple it prints a visible SKIP, and
//!   HUMAUX_REQUIRE_DB=1 turns that SKIP into a failure]
//! Spec: Baseline §79.2; ADR-0037; ADR-0052
//!
//! The resident projection runner must survive its database going away (the supervisor would
//! otherwise crash-loop it through every PG restart) and must still honour the ADR-0037 drain
//! contract while it cannot reach anything: SIGTERM is observed between passes and the process
//! exits 0. "Down" is a real absence — a port that was bound, read and released — never a stopped
//! shared container.

use std::io::Read;
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_humaux-retrieval-worker");

/// A port that was bound and released: connecting to it is refused rather than hanging.
fn dead_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve a loopback port");
    let port = listener.local_addr().expect("reserved port").port();
    drop(listener);
    port
}

/// `--serve` with every key it needs, PostgreSQL and Qdrant on closed loopback ports, stderr to
/// `stderr`. `gitleaks` = the pinned (binary, version, sha256) triple.
fn spawn_serve(gitleaks: [String; 3], stderr: std::fs::File) -> std::process::Child {
    let [bin, version, sha] = gitleaks;
    let pg = format!(
        "postgres://role_retrieval_worker@127.0.0.1:{}/humaux_thread_dev",
        dead_port()
    );
    // dep: subprocess(humaux-retrieval-worker) — the resident projection runner under test
    Command::new(BIN)
        .arg("--serve")
        .env_clear()
        .env("HUMAUX_RETRIEVAL_WORKER_PG_DSN", pg)
        .env(
            "HUMAUX_RETRIEVAL_WORKER_EMBEDDING_MODEL",
            "serve-drain-model",
        )
        .env("HUMAUX_RETRIEVAL_WORKER_MODEL_REVISION", "r1")
        .env("HUMAUX_RETRIEVAL_WORKER_DIMENSION", "4")
        .env(
            "HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION",
            "serve-drain@r1",
        )
        .env(
            "HUMAUX_RETRIEVAL_WORKER_EGRESS_PROCESSOR_ID",
            "00000000-0000-7000-8000-0000000c2701",
        )
        .env(
            "HUMAUX_RETRIEVAL_WORKER_CELL_ID",
            "00000000-0000-7000-8000-0000000c2702",
        )
        .env("HUMAUX_RETRIEVAL_WORKER_CALLER", "humaux-retrieval-worker")
        .env("HUMAUX_RETRIEVAL_WORKER_QDRANT_HOST", "127.0.0.1")
        .env(
            "HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT",
            dead_port().to_string(),
        )
        .env("HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR", "127.0.0.0/8")
        .env("HUMAUX_RETRIEVAL_WORKER_QDRANT_TLS", "false")
        .env("HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN", bin)
        .env("HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION", version)
        .env("HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256", sha)
        .env("HUMAUX_RETRIEVAL_WORKER_BATCH", "4")
        .env("HUMAUX_RETRIEVAL_WORKER_POLL_INTERVAL_SECS", "1")
        .env("HUMAUX_RETRIEVAL_WORKER_LEASE_SECS", "60")
        .env("HUMAUX_RETRIEVAL_WORKER_PER_TENANT_CAP", "2")
        .env("HUMAUX_RETRIEVAL_WORKER_MAX_ATTEMPTS", "3")
        .env("HUMAUX_RETRIEVAL_WORKER_BACKOFF_BASE_SECS", "1")
        .env("HUMAUX_RETRIEVAL_WORKER_BACKOFF_MAX_SECS", "2")
        // ADR-0061 D-B: the resident mode's own ops listener, a free loopback port.
        .env(
            "HUMAUX_RETRIEVAL_WORKER_SERVE_METRICS_ADDR",
            format!("127.0.0.1:{}", dead_port()),
        )
        .stdout(Stdio::null())
        .stderr(stderr)
        .spawn()
        .expect("spawn --serve")
}

/// ADR-0052 D-F / ADR-0037: `--serve` with PostgreSQL on a closed port logs
/// `projection pass failed`, keeps polling (still alive after the first failed pass), and exits 0
/// on SIGTERM with the between-passes line. Fault injection: make a failed connect return
/// `Err(Outcome::Failed)` in resident mode (the `--run-once` arm) ⇒ the process exits 2 before the
/// SIGTERM and this goes red.
#[test]
fn serve_exits_zero_on_sigterm_while_the_database_is_down() {
    let (Ok(bin), Ok(version), Ok(sha)) = (
        std::env::var("HUMAUX_TEST_GITLEAKS_BIN"),
        std::env::var("HUMAUX_TEST_GITLEAKS_VERSION"),
        std::env::var("HUMAUX_TEST_GITLEAKS_SHA256"),
    ) else {
        assert!(
            !std::env::var("HUMAUX_REQUIRE_DB").is_ok_and(|v| v == "1"),
            "HUMAUX_REQUIRE_DB=1 but the pinned gitleaks triple (HUMAUX_TEST_GITLEAKS_*) is unset"
        );
        eprintln!(
            "SKIP serve_exits_zero_on_sigterm_while_the_database_is_down: HUMAUX_TEST_GITLEAKS_* unset"
        );
        return;
    };
    let log = std::env::temp_dir().join(format!("serve_drain_{}.log", std::process::id()));
    let stderr = std::fs::File::create(&log).expect("stderr capture file");
    let mut child = spawn_serve([bin, version, sha], stderr);
    let read_log = || {
        let mut text = String::new();
        let _ = std::fs::File::open(&log).and_then(|mut f| f.read_to_string(&mut text));
        text
    };

    let deadline = Instant::now() + Duration::from_secs(120);
    while !read_log().contains("projection pass failed") {
        if let Some(status) = child.try_wait().expect("poll child") {
            panic!(
                "--serve exited ({status}) instead of logging a failed pass: {}",
                read_log()
            );
        }
        assert!(
            Instant::now() < deadline,
            "no failed pass logged: {}",
            read_log()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    std::thread::sleep(Duration::from_millis(1500));
    assert!(
        child.try_wait().expect("poll child").is_none(),
        "a resident runner must outlive a failed pass: {}",
        read_log()
    );

    // dep: subprocess(kill) — SIGTERM to the child this test spawned (its own PID, never a pattern)
    let sent = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("run kill");
    assert!(sent.success());
    let deadline = Instant::now() + Duration::from_secs(90);
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll child") {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("--serve did not exit after SIGTERM: {}", read_log());
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    let text = read_log();
    let _ = std::fs::remove_file(&log);
    assert!(
        status.success(),
        "SIGTERM must end --serve with 0: {status} {text}"
    );
    assert!(
        text.contains("signal received between passes"),
        "the drain line is missing: {text}"
    );
}
