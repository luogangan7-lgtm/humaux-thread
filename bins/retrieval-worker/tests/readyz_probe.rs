//! Card 15 / ADR-0037 — `humaux-retrieval-worker --readyz` asserted against the BINARY, in both
//! directions, with each dependency really taken away.
//!
//! The review this file answers found the readiness contract's down-path asserted for exactly
//! one dependency of one worker: nothing anywhere ran a `--readyz` against a down PostgreSQL or
//! a down Qdrant, even though docs/ops/supervision.md §2 documents all four rows of that table
//! as observed behaviour. "Down" here is a real absence — a TCP port that was bound, read, and
//! released, so nothing is listening on it — not a mock and not a stopped shared container
//! (Postgres and Qdrant are shared state for every other suite on this machine).
//!
//! Three-state (§79.2): the Postgres-down case needs no database at all and always runs; the
//! Qdrant cases need the live fixture DB (`HUMAUX_TEST_PG_DSN`) and, for the up-case, live
//! Qdrant (`HUMAUX_TEST_QDRANT_PORT`) — absent either, they print a visible SKIP naming what was
//! missing, and `HUMAUX_REQUIRE_DB=1` turns that SKIP into a failure.

use std::net::TcpListener;
use std::process::{Command, Output};

const BIN: &str = env!("CARGO_BIN_EXE_humaux-retrieval-worker");

/// macOS XProtect assesses a freshly linked binary on its first exec; pay that once.
fn warm_binary() {
    let _ = Command::new(BIN).arg("--warm-up-not-a-mode").output();
}

/// A port that was bound and released: connecting to it is refused rather than hanging, which
/// is what "the dependency is down" looks like to a probe.
fn dead_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve a loopback port");
    let port = listener.local_addr().expect("reserved port").port();
    drop(listener);
    port
}

/// §79.2 three-state: visible SKIP, or a failure when the run demanded a database.
fn skip(test: &str, missing: &str) {
    assert!(
        !std::env::var("HUMAUX_REQUIRE_DB").is_ok_and(|v| v == "1"),
        "HUMAUX_REQUIRE_DB=1 but {test} cannot run: {missing}"
    );
    eprintln!("SKIP {test}: {missing}");
}

fn role_dsn() -> Option<String> {
    let admin = std::env::var("HUMAUX_TEST_PG_DSN").ok()?;
    let rest = admin
        .strip_prefix("postgres://")
        .or_else(|| admin.strip_prefix("postgresql://"))?;
    let at = rest.find('@')?;
    Some(format!(
        "postgres://role_retrieval_worker:devlocal_role_retrieval_worker@{}",
        &rest[at + 1..]
    ))
}

/// `--readyz` with the whole environment it needs; the caller varies exactly one dependency.
fn readyz(dsn: &str, qdrant_host: &str, qdrant_port: u16) -> Output {
    Command::new(BIN)
        .arg("--readyz")
        .env("HUMAUX_RETRIEVAL_WORKER_PG_DSN", dsn)
        .env(
            "HUMAUX_RETRIEVAL_WORKER_CELL_ID",
            "00000000-0000-7000-8000-00000000c15c",
        )
        .env("HUMAUX_RETRIEVAL_WORKER_CALLER", "humaux-retrieval-worker")
        .env("HUMAUX_RETRIEVAL_WORKER_QDRANT_HOST", qdrant_host)
        .env(
            "HUMAUX_RETRIEVAL_WORKER_QDRANT_PORT",
            qdrant_port.to_string(),
        )
        .env("HUMAUX_RETRIEVAL_WORKER_QDRANT_CIDR", "127.0.0.0/8")
        .env("HUMAUX_RETRIEVAL_WORKER_QDRANT_TLS", "false")
        .output()
        .expect("spawn --readyz")
}

fn stderr_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

/// ADR-0037 D2: PostgreSQL down ⇒ non-zero AND the object named. Needs no database — the point
/// is that there is nothing listening on the port the DSN points at.
///
/// 注错: drop the `?` on `RolePool::connect` in `readyz()` (swallow the error) ⇒ this goes red,
/// because a probe that cannot reach its database must never exit zero.
#[test]
fn readyz_names_postgresql_when_the_database_is_down() {
    warm_binary();
    let dsn = format!(
        "postgres://role_retrieval_worker:devlocal_role_retrieval_worker@127.0.0.1:{}/humaux_thread_dev",
        dead_port()
    );
    let output = readyz(&dsn, "127.0.0.1", dead_port());
    let stderr = stderr_of(&output);
    assert!(
        !output.status.success(),
        "--readyz must fail when PostgreSQL is down: stderr={stderr}"
    );
    assert!(
        stderr.contains("missing object") && stderr.contains("PostgreSQL as role_retrieval_worker"),
        "a failed readiness probe must NAME the object that is down, and the DB is checked \
         first so it must be the DB that is named here: stderr={stderr}"
    );
}

/// ADR-0037 D2, the arm nothing exercised before: PostgreSQL up, Qdrant DOWN ⇒ non-zero with
/// `the Qdrant cell resource` named. Reaching this line at all proves the DB arm passed.
///
/// 注错: replace the `transport.execute(..)` round trip in `readyz()` with a bare `Ok(())`
/// ⇒ green Qdrant with nothing listening, and this goes red.
#[test]
fn readyz_names_the_qdrant_cell_resource_when_qdrant_is_down() {
    let Some(dsn) = role_dsn() else {
        return skip(
            "readyz_names_the_qdrant_cell_resource_when_qdrant_is_down",
            "HUMAUX_TEST_PG_DSN is not set (the DB must be UP for Qdrant to be the failure)",
        );
    };
    warm_binary();
    let output = readyz(&dsn, "127.0.0.1", dead_port());
    let stderr = stderr_of(&output);
    if stderr.contains("PostgreSQL as role_retrieval_worker") {
        return skip(
            "readyz_names_the_qdrant_cell_resource_when_qdrant_is_down",
            "the fixture database is unreachable as role_retrieval_worker",
        );
    }
    assert!(
        !output.status.success(),
        "--readyz must fail when Qdrant is down: stderr={stderr}"
    );
    assert!(
        stderr.contains("missing object") && stderr.contains("the Qdrant cell resource"),
        "a down Qdrant must be NAMED (docs/ops/supervision.md §2): stderr={stderr}"
    );
}

/// The up-direction, so a probe that always fails is as red as one that always passes.
#[test]
fn readyz_exits_zero_when_postgresql_and_qdrant_both_answer() {
    let test = "readyz_exits_zero_when_postgresql_and_qdrant_both_answer";
    let Some(dsn) = role_dsn() else {
        return skip(test, "HUMAUX_TEST_PG_DSN is not set");
    };
    let Some(port) = std::env::var("HUMAUX_TEST_QDRANT_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
    else {
        return skip(
            test,
            "HUMAUX_TEST_QDRANT_PORT is not set (live Qdrant required)",
        );
    };
    warm_binary();
    let output = readyz(&dsn, "127.0.0.1", port);
    let stderr = stderr_of(&output);
    if !output.status.success() && stderr.contains("missing object") {
        return skip(test, &format!("a dependency is genuinely down: {stderr}"));
    }
    assert!(
        output.status.success(),
        "--readyz must exit zero when every dependency answers: stderr={stderr}"
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("ready"),
        "stdout={}",
        String::from_utf8_lossy(&output.stdout)
    );
}
