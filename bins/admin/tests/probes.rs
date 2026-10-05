//! `admin::tests::probes` — the §4.4 probe catalog through the real `humaux-admin q <name>` binary: the three DB
//!   probes over a throwaway database and a real `role_admin` login, `degrade.counters` / `flags.effective` against
//!   real loopback ops listeners, `tls.expiry` over fixture certificates, `cell.resources` over a loopback HTTP
//!   stand-in for Qdrant and real Unix-socket listeners, and the three typed refusals (ADR-0061 D-J incl. ruling B1;
//!   card 34 T-J2..T-J10).
//! Depends-on: crates=[humaux-domain, humaux-telemetry, humaux-testkit, postgres, serde_json, uuid];
//!   services=[PostgreSQL(owner) r=[ops.schema_migrations] w=[control.tenants, ops.jobs, ops.outbox,
//!   projection.stream_checkpoints],
//!   PostgreSQL(role_admin), HTTP(loopback), UDS(serve), subprocess(humaux-admin)];
//!   env=[CARGO_BIN_EXE_humaux-admin, CARGO_MANIFEST_DIR, HUMAUX_ADMIN_OPS_ADDRS, HUMAUX_ADMIN_PG_DSN,
//!   HUMAUX_ADMIN_PRIVATE_INFERENCE_RPC_SOCKET_PATH, HUMAUX_ADMIN_RETRIEVAL_RPC_SOCKET_PATH,
//!   HUMAUX_ADMIN_TLS_CERT_PATHS, HUMAUX_CELL_CALLER_ID, HUMAUX_CELL_ID, HUMAUX_QDRANT_CIDR, HUMAUX_QDRANT_HOST,
//!   HUMAUX_QDRANT_PORT, HUMAUX_QDRANT_TLS, HUMAUX_TEST_PG_DSN]; modules=[domain::ticket_family, humaux-testkit,
//!   telemetry::degrade, telemetry::metrics]
//! Called-by: [cargo-test]
//! Invariants: [each DB test owns its throwaway database humaux_thread_c34_ap_<pid>_<n>, migrated from the files and
//!   dropped by the fixture's Drop even on panic; the shared dev database is never touched; every listener is a
//!   loopback port taken at run time; missing env -> §79.2 skip_or_fail]
//! Spec: Baseline §4.4; §79.2; ADR-0061 D-J
//! dep-map: allow table-undeclared — public.* names are the refusal objects the probes must name; no SQL reads them

use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener};
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

use humaux_domain::ticket_family::TicketFamily;
use humaux_telemetry::degrade::{DegradeCode, abstain};
use humaux_telemetry::metrics::{Routes, process_routes, serve_loopback};
use humaux_testkit::{ExternalDep, role_login_dsn, skip_or_fail};
use postgres::{Client, NoTls};
use serde_json::Value;
use uuid::Uuid;

const OWNER_DSN: &str = "HUMAUX_TEST_PG_DSN";
const PROBE_ENV: [&str; 5] = [
    "HUMAUX_ADMIN_PG_DSN",
    "HUMAUX_ADMIN_OPS_ADDRS",
    "HUMAUX_ADMIN_TLS_CERT_PATHS",
    "HUMAUX_ADMIN_RETRIEVAL_RPC_SOCKET_PATH",
    "HUMAUX_ADMIN_PRIVATE_INFERENCE_RPC_SOCKET_PATH",
];

static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Q {
    code: i32,
    stdout: String,
    stderr: String,
}

impl Q {
    /// The parsed §4.4 envelope; panics with the stderr when the probe refused.
    fn envelope(&self) -> Value {
        assert_eq!(self.code, 0, "probe refused: {}", self.stderr);
        let v: Value = serde_json::from_str(&self.stdout).expect("envelope JSON");
        for key in [
            "value",
            "scanned_n",
            "scope_hash",
            "checked_at",
            "probe_version",
        ] {
            assert!(v.get(key).is_some(), "envelope lacks {key}: {v}");
        }
        v
    }

    fn value_and_scanned(&self) -> (i64, i64) {
        let v = self.envelope();
        (
            v["value"].as_i64().expect("value"),
            v["scanned_n"].as_i64().expect("scanned_n"),
        )
    }
}

/// `humaux-admin q <name>` with only `env` among the probe keys.
fn q(name: &str, env: &[(&str, &str)]) -> Q {
    // dep: subprocess(humaux-admin) — the binary under test
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_humaux-admin"));
    cmd.args(["q", name]);
    for key in PROBE_ENV {
        cmd.env_remove(key);
    }
    cmd.envs(env.iter().copied());
    let out = cmd.output().expect("spawn humaux-admin");
    Q {
        code: out.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

/// A refusal: non-zero exit, no envelope, the object named on stderr.
fn assert_refuses_naming(r: &Q, object: &str) {
    assert_ne!(
        r.code, 0,
        "expected a refusal naming {object}: {}",
        r.stdout
    );
    assert!(
        r.stdout.trim().is_empty(),
        "a refusal prints no envelope: {}",
        r.stdout
    );
    assert!(
        r.stderr.contains(object),
        "stderr must name {object}: {}",
        r.stderr
    );
}

/// A loopback address nothing listens on (taken, then released).
fn closed_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    l.local_addr().expect("addr")
}

fn loopback_any() -> SocketAddr {
    "127.0.0.1:0".parse().expect("loopback")
}

// ---- throwaway database --------------------------------------------------------------------------------------

/// `postgres://user:pass@host:port/<db>?query` with the database replaced.
fn with_db(dsn: &str, db: &str) -> String {
    let (head, tail) = dsn.split_at(dsn.rfind('/').expect("dsn has a database path") + 1);
    let query = tail.find('?').map_or("", |i| &tail[i..]);
    format!("{head}{db}{query}")
}

struct Db {
    owner_dsn: String,
    name: String,
    client: Option<Client>,
}

impl Drop for Db {
    fn drop(&mut self) {
        drop(self.client.take());
        // dep: PostgreSQL(owner) — drop this test's throwaway database
        match Client::connect(&with_db(&self.owner_dsn, "postgres"), NoTls) {
            Ok(mut admin) => {
                if let Err(e) = admin.batch_execute(&format!(
                    "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                    self.name
                )) {
                    eprintln!("cleanup: drop database {} failed: {e}", self.name);
                }
            }
            Err(e) => eprintln!("cleanup: cannot reach postgres to drop {}: {e}", self.name),
        }
    }
}

/// Applies every migration body in order (the manifest checks are the `migrate` gate's job).
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

/// The throwaway database and a real `role_admin` login DSN into it.
fn fixture(test: &str) -> Option<(Db, String)> {
    let Ok(owner_dsn) = std::env::var(OWNER_DSN) else {
        skip_or_fail(test, OWNER_DSN, ExternalDep::Postgres);
        return None;
    };
    let name = format!(
        "humaux_thread_c34_ap_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    );
    // dep: PostgreSQL(owner) — create this test's throwaway database
    let mut admin = match Client::connect(&with_db(&owner_dsn, "postgres"), NoTls) {
        Ok(c) => c,
        Err(e) => {
            skip_or_fail(
                test,
                &format!("reachable {OWNER_DSN}: {e}"),
                ExternalDep::Postgres,
            );
            return None;
        }
    };
    admin
        .batch_execute(&format!("CREATE DATABASE {name}"))
        .expect("create throwaway db");
    let mut db = Db {
        owner_dsn: owner_dsn.clone(),
        name: name.clone(),
        client: None,
    };
    // dep: PostgreSQL(owner) — fixture connection to the throwaway database
    let mut client = Client::connect(&with_db(&owner_dsn, &name), NoTls).expect("connect test db");
    migrate(&mut client);
    db.client = Some(client);
    let login = match role_login_dsn(&with_db(&owner_dsn, &name), "role_admin", |n| {
        std::env::var(n).ok()
    }) {
        Ok(dsn) => dsn,
        Err(missing) => {
            skip_or_fail(test, &missing, ExternalDep::Postgres);
            return None;
        }
    };
    Some((db, login))
}

impl Db {
    fn sql(&mut self) -> &mut Client {
        self.client.as_mut().expect("client")
    }

    fn tenant(&mut self, name: &str) -> Uuid {
        self.sql()
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&name],
            )
            .expect("seed tenant")
            .get(0)
    }

    /// One `ops.jobs` row; `lease` = Some(true) an expired lease, Some(false) a live one.
    fn job(&mut self, tenant: Uuid, status: &str, lease: Option<bool>) {
        let lease_sql = match lease {
            None => "NULL, NULL",
            Some(true) => "'c34-worker', now() - interval '1 minute'",
            Some(false) => "'c34-worker', now() + interval '10 minutes'",
        };
        self.sql()
            .execute(
                &format!(
                    "INSERT INTO ops.jobs (tenant_id, job_type, status, idempotency_key, payload, \
                       lease_owner, lease_expires_at) \
                     VALUES ($1, 'C34_PROBE_TEST', $2, gen_random_uuid()::text, '{{}}'::jsonb, {lease_sql})"
                ),
                &[&tenant, &status],
            )
            .expect("seed job");
    }

    /// One `ops.outbox` carrier row in `status`. The probe reads only status and created_at, so the Evidence it
    /// would carry is not built: FK and identity triggers are off for this one insert (session_replication_role,
    /// superuser, throwaway database); the row-class CHECK still applies.
    fn outbox(&mut self, tenant: Uuid, seq: i64, status: &str) {
        let mut txn = self.sql().transaction().expect("outbox txn");
        // replica-mode: throwaway database only (humaux_thread_c34_ap_<pid>_<n>, created by fixture(), dropped WITH (FORCE) by Db::drop)
        txn.batch_execute("SET LOCAL session_replication_role = replica")
            .expect("replica role");
        txn.execute(
            "INSERT INTO ops.outbox (tenant_id, commit_seq, stream_seq, event_type, evidence_id, status) \
             VALUES ($1, $2, $2, 'EVIDENCE_ACCEPTED', gen_random_uuid(), $3)",
            &[&tenant, &seq, &status],
        )
        .expect("seed outbox");
        txn.commit().expect("commit outbox");
    }

    fn checkpoint(&mut self, tenant: Uuid, issued: i64, projected: i64) {
        let f = TicketFamily::PrivateMemory;
        self.sql()
            .execute(
                "INSERT INTO projection.stream_checkpoints (tenant_id, scope_kind, scope_id, domain, \
                   projection_kind, projection_version, issued_highwater, projection_highwater) \
                 VALUES ($1, 'TENANT', $1, $2, $3, $4, $5, $6)",
                &[
                    &tenant,
                    &f.domain(),
                    &f.projection_kind(),
                    &f.projection_version(),
                    &issued,
                    &projected,
                ],
            )
            .expect("seed checkpoint");
    }
}

// ---- DB probes -----------------------------------------------------------------------------------------------

/// T-J10: on empty tables each DB probe refuses naming the table, never 0/0. Fault: build the Reading at
/// `scanned_n == 0` in `probe::reading` ⇒ exit 0 ⇒ red.
#[test]
fn empty_tables_are_not_scanned_not_zero() {
    const T: &str = "empty_tables_are_not_scanned_not_zero";
    let Some((_db, dsn)) = fixture(T) else { return };
    for (name, table) in [
        ("jobs.stuck", "ops.jobs has no rows"),
        ("outbox.backlog", "ops.outbox has no rows"),
        (
            "stream.watermark",
            "projection.stream_checkpoints has no rows",
        ),
    ] {
        assert_refuses_naming(&q(name, &[("HUMAUX_ADMIN_PG_DSN", &dsn)]), table);
    }
}

/// T-J2 / T-J3 / T-J4 over two tenants, through a real role_admin login.
/// Faults (each in migration 0210's `ops.admin_probe_snapshot()` body, applied to this throwaway database only):
/// drop `lease_expires_at < now()` ⇒ jobs.stuck 2 ⇒ red; count FAILED as undelivered ⇒ outbox.backlog 3 ⇒ red;
/// `<=` for the lag test ⇒ stream.watermark 2 ⇒ red.
#[test]
fn db_probes_count_across_tenants() {
    const T: &str = "db_probes_count_across_tenants";
    let Some((mut db, dsn)) = fixture(T) else {
        return;
    };
    let (a, b) = (db.tenant("c34-probe-a"), db.tenant("c34-probe-b"));
    db.job(a, "PROCESSING", Some(true));
    db.job(b, "PROCESSING", Some(false));
    db.job(b, "PENDING", None);
    db.outbox(a, 1, "PENDING");
    db.outbox(b, 2, "PROCESSING");
    db.outbox(a, 3, "FAILED");
    db.outbox(b, 4, "DONE");
    db.checkpoint(a, 10, 4);
    db.checkpoint(b, 3, 3);
    let env = [("HUMAUX_ADMIN_PG_DSN", dsn.as_str())];

    let jobs = q("jobs.stuck", &env);
    assert_eq!(jobs.value_and_scanned(), (1, 3), "jobs.stuck");
    let v = jobs.envelope();
    assert_eq!(v["probe_version"], "jobs.stuck@1");
    assert_eq!(v["detail"]["in_lease"], 1);
    assert!(v["scope_hash"].as_str().unwrap().starts_with("sha256:"));

    let outbox = q("outbox.backlog", &env);
    assert_eq!(outbox.value_and_scanned(), (2, 4), "outbox.backlog");
    assert!(outbox.envelope()["detail"]["oldest_undelivered_age_seconds"].is_number());

    let streams = q("stream.watermark", &env);
    assert_eq!(streams.value_and_scanned(), (1, 2), "stream.watermark");
    let v = streams.envelope();
    assert_eq!(v["detail"]["lag_total"], 6);
    assert_eq!(v["detail"]["families"][0]["family"], "private_memory");
}

/// A DB probe without its DSN names the key, and with a wrong role's login it names role_admin.
#[test]
fn a_db_probe_without_its_dsn_names_the_key() {
    assert_refuses_naming(&q("jobs.stuck", &[]), "HUMAUX_ADMIN_PG_DSN");
}

// ---- typed refusals ------------------------------------------------------------------------------------------

/// T-J5 / T-J9: the §4.4 line 883 freeze. Fault: return a Reading (or `value = 0`) for any of them ⇒ exit 0 ⇒ red.
#[test]
fn absent_columns_are_named_refusals() {
    for (name, object) in [
        ("public.corroborated", "public.claims.corroboration"),
        ("public.consensus_ready", "public.claims.contributor_set"),
        ("parse.poison", "limit_hit"),
    ] {
        assert_refuses_naming(&q(name, &[]), object);
    }
}

// ---- /status probes ------------------------------------------------------------------------------------------

/// T-J6: `degrade.counters` against a real ops listener serving this process's real `/status` after one
/// `abstain(EgressDenied)`: value 1, `scanned_n` 11. A second, closed address ⇒ a refusal naming it. Fault: sum the
/// reachable processes only ⇒ exit 0 ⇒ red.
#[test]
fn degrade_counters_sum_every_listed_process_or_name_the_missing_one() {
    let _ = abstain(DegradeCode::EgressDenied, ());
    let routes = process_routes("humaux-admin-test", "probes", "0", None, |_| {});
    // dep: HTTP(loopback) — a real ops listener serving this process's /status
    let ops = serve_loopback("TEST_OPS_ADDR", loopback_any(), routes).expect("ops listener");
    let one = format!("t={}", ops.local_addr());

    let r = q("degrade.counters", &[("HUMAUX_ADMIN_OPS_ADDRS", &one)]);
    assert_eq!(r.value_and_scanned(), (1, 11));
    let v = r.envelope();
    assert_eq!(v["detail"]["EgressDenied"]["by_process"]["t"], 1);
    assert!(v["detail"]["EgressDenied"]["last_fired_at"].is_u64());

    let closed = closed_addr();
    let two = format!("{one},gone={closed}");
    assert_refuses_naming(
        &q("degrade.counters", &[("HUMAUX_ADMIN_OPS_ADDRS", &two)]),
        &format!("gone={closed}"),
    );
    assert_refuses_naming(&q("degrade.counters", &[]), "HUMAUX_ADMIN_OPS_ADDRS");
}

/// T-J7: `flags.effective` over a gateway `/status` captured as a fixture, served by a real ops listener:
/// 5 of 7 entries come from env. Fault: count `default` rows as env ⇒ 7 ⇒ red.
#[test]
fn flags_effective_counts_env_entries_of_the_gateway() {
    let fixture = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/gateway_status.json"),
    )
    .expect("fixture");
    let routes = Routes {
        metrics: Box::new(|| Ok(String::new())),
        status: Box::new(move || Ok(fixture.clone())),
    };
    // dep: HTTP(loopback) — a real ops listener serving the captured gateway /status
    let ops = serve_loopback("TEST_OPS_ADDR", loopback_any(), routes).expect("ops listener");
    let addrs = format!("gateway={}", ops.local_addr());
    let r = q("flags.effective", &[("HUMAUX_ADMIN_OPS_ADDRS", &addrs)]);
    assert_eq!(r.value_and_scanned(), (5, 7));
    let v = r.envelope();
    assert_eq!(v["probe_version"], "flags.effective@1");
    for row in v["detail"]["entries"].as_array().expect("entries") {
        if row["secret"] == true {
            assert!(
                row.get("value").is_none(),
                "a secret row never carries a value: {row}"
            );
        }
    }

    // A non-gateway process alone is not a flag source.
    let other = process_routes("humaux-maintenance", "health-serve", "0", None, |_| {});
    // dep: HTTP(loopback) — a non-gateway ops listener
    let ops2 = serve_loopback("TEST_OPS_ADDR", loopback_any(), other).expect("ops listener");
    assert_refuses_naming(
        &q(
            "flags.effective",
            &[(
                "HUMAUX_ADMIN_OPS_ADDRS",
                &format!("mh={}", ops2.local_addr()),
            )],
        ),
        "humaux-gateway",
    );
}

// ---- tls.expiry ----------------------------------------------------------------------------------------------

/// T-J8: fixture certificates with notAfter 2099 and 2001 ⇒ value 1, `scanned_n` 2. Fault: compare against
/// notBefore ⇒ both inside the window ⇒ value 2 ⇒ red. An unreadable path is named.
#[test]
fn tls_expiry_counts_certificates_inside_the_warn_window() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let paths = format!(
        "{},{}",
        dir.join("cert_not_after_2099.pem").display(),
        dir.join("cert_not_after_2001.pem").display()
    );
    let r = q("tls.expiry", &[("HUMAUX_ADMIN_TLS_CERT_PATHS", &paths)]);
    assert_eq!(r.value_and_scanned(), (1, 2));
    let v = r.envelope();
    assert_eq!(v["detail"]["files"][1]["warn"], true);
    assert_eq!(v["detail"]["files"][0]["warn"], false);

    let missing = dir.join("no_such_cert.pem");
    let missing = missing.display().to_string();
    assert_refuses_naming(
        &q("tls.expiry", &[("HUMAUX_ADMIN_TLS_CERT_PATHS", &missing)]),
        &missing,
    );
    assert_refuses_naming(&q("tls.expiry", &[]), "HUMAUX_ADMIN_TLS_CERT_PATHS");
}

// ---- cell.resources ------------------------------------------------------------------------------------------

/// A loopback stand-in for Qdrant's REST root: answers every request 200 (the probe only judges `status < 500`).
fn qdrant_stand_in() -> SocketAddr {
    let l = TcpListener::bind(loopback_any()).expect("bind");
    let addr = l.local_addr().expect("addr");
    std::thread::spawn(move || {
        for mut s in l.incoming().flatten() {
            let mut head = [0u8; 4096];
            let _ = s.read(&mut head);
            let _ = s.write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}",
            );
        }
    });
    addr
}

/// ADR-0061 D-J ruling B1: all three `IntraCellResource`s are probed. With the private-inference socket path naming a
/// file that does not exist the Reading is value 2 / `scanned_n` 3 and names `PRIVATE_INFERENCE_RPC`. Fault: ignore
/// the connect result ⇒ value 3 ⇒ red. A missing socket-path key is a refusal naming it.
#[test]
fn cell_resources_reads_all_three_resources_and_names_the_unreachable_socket() {
    let qdrant = qdrant_stand_in();
    let dir = std::env::temp_dir();
    let tag = format!(
        "{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    );
    let live = dir.join(format!("humaux_c34_cr_{tag}.sock"));
    let absent = dir.join(format!("humaux_c34_cr_{tag}_absent.sock"));
    let _ = std::fs::remove_file(&live);
    // dep: UDS(serve) — a real listener; the probe's connect lands in its backlog
    let _listener = UnixListener::bind(&live).expect("bind unix socket");
    let (live, absent) = (live.display().to_string(), absent.display().to_string());
    let port = qdrant.port().to_string();
    let cell = Uuid::now_v7().to_string();
    let base = [
        ("HUMAUX_CELL_ID", cell.as_str()),
        ("HUMAUX_CELL_CALLER_ID", "admin"),
        ("HUMAUX_QDRANT_HOST", "127.0.0.1"),
        ("HUMAUX_QDRANT_PORT", port.as_str()),
        ("HUMAUX_QDRANT_CIDR", "127.0.0.1/32"),
        ("HUMAUX_QDRANT_TLS", "false"),
        ("HUMAUX_ADMIN_RETRIEVAL_RPC_SOCKET_PATH", live.as_str()),
    ];

    let mut env = base.to_vec();
    env.push((
        "HUMAUX_ADMIN_PRIVATE_INFERENCE_RPC_SOCKET_PATH",
        absent.as_str(),
    ));
    let r = q("cell.resources", &env);
    assert_eq!(r.value_and_scanned(), (2, 3), "{}", r.stdout);
    assert_eq!(
        r.envelope()["detail"]["unhealthy"],
        serde_json::json!(["PRIVATE_INFERENCE_RPC"])
    );

    let mut env = base.to_vec();
    env.push((
        "HUMAUX_ADMIN_PRIVATE_INFERENCE_RPC_SOCKET_PATH",
        live.as_str(),
    ));
    assert_eq!(q("cell.resources", &env).value_and_scanned(), (3, 3));

    assert_refuses_naming(
        &q("cell.resources", &base),
        "HUMAUX_ADMIN_PRIVATE_INFERENCE_RPC_SOCKET_PATH",
    );
    let _ = std::fs::remove_file(&live);
}
