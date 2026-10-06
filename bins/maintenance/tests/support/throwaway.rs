//! `maintenance::tests::support::throwaway` — the resident-mode test kit: a throwaway database migrated from the
//!   files, the spawned process guard, free loopback ports and a one-shot HTTP GET (moved from
//!   `health_serve.rs`, card 35 S1), plus the §48.1 fixtures every retention test shares: an owner-made sealed leaf,
//!   an effective policy and a proposal forged through role_maintenance's column grant (ADR-0063 D-C, D-E, D-J),
//!   the full `--serve` key set and one production-shaped DR_EVIDENCE pass against any database (ADR-0064 D-K).
//! Depends-on: crates=[humaux-testkit, postgres]; services=[PostgreSQL(owner) r=[ops.schema_migrations]
//!   w=[control.retention_policies] x=[control.partition_adopt_leaf], PostgreSQL(role_maintenance)
//!   w=[control.partition_registry], HTTP(loopback), subprocess(humaux-maintenance), subprocess(kill)]; env=[CARGO_BIN_EXE_humaux-maintenance, CARGO_MANIFEST_DIR,
//!   HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS, HUMAUX_MAINTENANCE_DR_PGDATA_FS_PATH, HUMAUX_MAINTENANCE_DR_REPO_FS_PATH,
//!   HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_MAINTENANCE_SERVE_CONFIRM_TOKENS_CONSUMED_RETENTION_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_CYCLE_SECONDS, HUMAUX_MAINTENANCE_SERVE_DR_EVIDENCE_EVERY_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_JOBS_DEAD_RETENTION_SECONDS, HUMAUX_MAINTENANCE_SERVE_JOBS_DONE_RETENTION_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_LOST_AFTER_SECONDS, HUMAUX_MAINTENANCE_SERVE_METRICS_ADDR,
//!   HUMAUX_MAINTENANCE_SERVE_PARTITIONS_EVERY_SECONDS, HUMAUX_MAINTENANCE_SERVE_RATE_BUCKETS_IDLE_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_REDRIVE_COOLDOWN_SECONDS, HUMAUX_MAINTENANCE_SERVE_REISSUE_COOLDOWN_SECONDS,
//!   HUMAUX_MAINTENANCE_SERVE_TENANTS_PER_RUN, HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS, HUMAUX_TEST_PG_DSN];
//!   modules=[humaux-testkit, testkit::reaped]
//! Called-by: [maintenance::tests::backup, maintenance::tests::drill, maintenance::tests::health_serve,
//!   maintenance::tests::measure, maintenance::tests::partition_pg_facts, maintenance::tests::partitions,
//!   maintenance::tests::rebuild_cli, maintenance::tests::retention, maintenance::tests::serve,
//!   maintenance::tests::spike]
//! Invariants: [every database is humaux_thread_<prefix>_<pid>_<n>, created by the fixture and dropped WITH (FORCE)
//!   by its Drop even on panic, so the shared dev database never sees a fixture row; the spawned process is
//!   killed by its own Drop (the DR_EVIDENCE pass holds it as testkit::reaped::Reaped); missing env -> §79.2
//!   skip_or_fail]
//! Spec: Baseline §78.1; §79.2; ADR-0061 D-D; ADR-0062 D-A; ADR-0063 D-E; ADR-0063 D-J; ADR-0064 D-K

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use humaux_testkit::reaped::SpawnReaped;
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};

pub const BIN: &str = env!("CARGO_BIN_EXE_humaux-maintenance");
pub const OWNER_DSN: &str = "HUMAUX_TEST_PG_DSN";
pub const MAINTENANCE_DSN: &str = "HUMAUX_MAINTENANCE_PG_DSN";

static NEXT: AtomicUsize = AtomicUsize::new(0);

/// `postgres://user:pass@host:port/<db>?query` with the database replaced.
pub fn with_db(dsn: &str, db: &str) -> String {
    let (head, tail) = dsn.split_at(dsn.rfind('/').expect("dsn has a database path") + 1);
    let query = tail.find('?').map_or("", |i| &tail[i..]);
    format!("{head}{db}{query}")
}

/// A loopback port free at the moment of the call (the xtask e2e_onboard::free_port pattern).
pub fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("a free loopback port")
}

/// One HTTP/1.0 GET; `None` when nothing listens.
pub fn get(addr: SocketAddr, path: &str) -> Option<(u16, String)> {
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
pub fn poll(
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

/// The spawned process, killed if the test did not stop it itself.
pub struct Serve(pub Child);

impl Drop for Serve {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Serve {
    /// SIGTERM, then the exit status within `within` and everything it printed on stdout.
    pub fn terminate(&mut self, within: Duration) -> (std::process::ExitStatus, Duration, String) {
        let pid = self.0.id().to_string();
        let sent = Instant::now();
        // dep: subprocess(kill) — SIGTERM to the process this test spawned
        let ok = Command::new("kill").args(["-TERM", &pid]).status();
        assert!(ok.is_ok_and(|s| s.success()), "kill -TERM {pid}");
        let status = loop {
            if let Some(status) = self.0.try_wait().expect("try_wait") {
                break status;
            }
            assert!(
                sent.elapsed() < within,
                "no exit within {within:?} of SIGTERM"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        let took = sent.elapsed();
        let mut out = String::new();
        if let Some(mut stdout) = self.0.stdout.take() {
            stdout.read_to_string(&mut out).expect("read stdout");
        }
        (status, took, out)
    }
}

pub struct Db {
    pub owner_dsn: String,
    pub maintenance_dsn: String,
    pub name: String,
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
                    eprintln!("throwaway cleanup: {drop_db} failed: {e}");
                }
            }
            Err(e) => eprintln!("throwaway cleanup: connect failed: {e}"),
        }
    }
}

/// Applies every migration file in order (bodies only; the manifests are the `xtask migrate` gate's job), or only
/// those whose 4-digit stem is at or before `through`.
fn migrate(client: &mut Client, through: Option<&str>) {
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
        .filter(|p| {
            through.is_none_or(|t| {
                let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or_default();
                stem.get(..t.len()).is_some_and(|n| n <= t)
            })
        })
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

/// Every migration file applied to `client`'s database (a scratch cluster's, card 37 drill tests).
pub fn migrate_all(client: &mut Client) {
    migrate(client, None);
}

/// A fresh migrated database `humaux_thread_<prefix>_<pid>_<n>`, or `None` after a §79.2 skip.
pub fn db(test: &str, prefix: &str) -> Option<Db> {
    db_through(test, prefix, None)
}

/// Like [`db`], migrated only through the migration whose 4-digit stem is `through` (`Some("0000")`: no
/// migration, only the ledger bootstrap: a bare database for scratch-table facts).
pub fn db_through(test: &str, prefix: &str, through: Option<&str>) -> Option<Db> {
    let mut db = empty(test, prefix)?;
    migrate(db.client(), through);
    Some(db)
}

/// A fresh database `humaux_thread_<prefix>_<pid>_<n>` with nothing in it (not even the migration ledger): the target
/// of a `pg_restore`. `None` after a §79.2 skip.
pub fn empty(test: &str, prefix: &str) -> Option<Db> {
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
        "humaux_thread_{prefix}_{}_{}",
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
    db.client = Some(Client::connect(&with_db(&owner_dsn, &name), NoTls).expect("connect test db"));
    Some(db)
}

impl Db {
    /// The owner connection to this database.
    pub fn client(&mut self) -> &mut Client {
        self.client.as_mut().expect("client")
    }

    pub fn sql(&mut self, statements: &str) {
        self.client()
            .batch_execute(statements)
            .unwrap_or_else(|e| panic!("{statements}: {e:?}"));
    }
}

/// One run with exactly `env`.
pub fn run(args: &[&str], env: &[(&str, String)]) -> Output {
    // dep: subprocess(humaux-maintenance) — one run with exactly `env`
    Command::new(BIN)
        .args(args)
        .env_clear()
        .envs(env.iter().map(|(k, v)| (k, v)))
        .output()
        .expect("run humaux-maintenance")
}

/// UTC month start `months` after the current one, as SQL (ADR-0063 D-D step 5 arithmetic).
pub fn month_sql(months: i32) -> String {
    format!("date_add(date_trunc('month', now(), 'UTC'), make_interval(months => {months}), 'UTC')")
}

/// ADR-0063 D-E fixture: a leaf of `parent` for the UTC month `months` from now (negative = past), owned by
/// role_migration_owner like a creator-made one (the definer re-owns only what it owns), sealed and registered by `control.partition_adopt_leaf` exactly like a creator-made one; `(registry_id, leaf)`.
pub fn past_leaf(c: &mut Client, table_key: &str, parent: &str, months: i32) -> (String, String) {
    let m = month_sql(months);
    let row = c
        .query_one(
            &format!(
                "SELECT $1::text || '_p' || to_char({m} AT TIME ZONE 'UTC', 'YYYYMM'), \
                        to_char({m} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') || '+00', \
                        to_char(date_add({m}, interval '1 month', 'UTC') AT TIME ZONE 'UTC', \
                                'YYYY-MM-DD HH24:MI:SS') || '+00'"
            ),
            &[&parent],
        )
        .expect("leaf name and bounds");
    let (leaf, lo, hi): (String, String, String) = (row.get(0), row.get(1), row.get(2));
    c.batch_execute(&format!(
        "CREATE TABLE {leaf} PARTITION OF {parent} FOR VALUES FROM ('{lo}') TO ('{hi}'); \
         ALTER TABLE {leaf} OWNER TO role_migration_owner"
    ))
    .unwrap_or_else(|e| panic!("create {leaf}: {e:?}"));
    let registry_id: String = c
        .query_one(
            "SELECT control.partition_adopt_leaf($1, $2::text::regclass)::text",
            &[&table_key, &leaf],
        )
        .unwrap_or_else(|e| panic!("adopt {leaf}: {e:?}"))
        .get(0);
    (registry_id, leaf)
}

/// ADR-0063 D-C fixture: the next revision of `table_key`'s policy, approved and effective an hour ago (a direct owner
/// INSERT, so it is effective at once); `(policy_id, policy_revision)`.
pub fn effective_policy(c: &mut Client, table_key: &str, months: Option<i32>) -> (String, i32) {
    let row = c
        .query_one(
            "INSERT INTO control.retention_policies \
               (table_key, retention_months, policy_revision, approved_by, approved_at, effective_at) \
             SELECT $1, $2, coalesce(max(policy_revision), 0) + 1, 'c36 fixture', now() - interval '1 hour', \
                    now() - interval '1 hour' \
               FROM control.retention_policies WHERE table_key = $1 \
             RETURNING policy_id::text, policy_revision",
            &[&table_key, &months],
        )
        .expect("policy fixture");
    (row.get(0), row.get(1))
}

/// ADR-0063 D-J step 4: writes the proposal columns of one registry row as role_maintenance through its column
/// grant (the forge every test of the corroboration uses); `proposed_at` is `proposed_at_sql`.
pub fn forge_proposal(c: &mut Client, registry_id: &str, revision: i32, proposed_at_sql: &str) {
    let mut tx = c.transaction().expect("begin");
    // dep: PostgreSQL(role_maintenance) — role switch: the daemon's column UPDATE cells
    tx.batch_execute("SET LOCAL ROLE role_maintenance")
        .expect("set role");
    let n = tx
        .execute(
            &format!(
                "UPDATE control.partition_registry SET proposed_policy_revision = $2, proposed_at = {proposed_at_sql} \
                 WHERE registry_id = $1::text::uuid"
            ),
            &[&registry_id, &revision],
        )
        .expect("forge proposal");
    assert_eq!(n, 1, "one registry row");
    tx.commit().expect("commit");
}

/// The nine per-tenant D-C tasks by key stem (each has `_EVERY_SECONDS` and `_LIMIT`, ADR-0062 D-C).
pub const SERVE_TASKS: [&str; 9] = [
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

/// Every key `--serve` requires, valid for `cycle` seconds (EVERY = CYCLE, LIMIT 50, ages one hour, LOST_AFTER >
/// LAG); the cluster-level PARTITIONS and DR_EVIDENCE cadences, and the two DR `df` paths (`repo_fs`, `/`).
pub fn serve_keys(
    addr: SocketAddr,
    dsn: &str,
    cycle: u64,
    tenants_per_run: u32,
    repo_fs: &Path,
) -> Vec<(String, String)> {
    let mut keys = vec![
        (
            "HUMAUX_MAINTENANCE_SERVE_METRICS_ADDR".to_owned(),
            addr.to_string(),
        ),
        (
            "HUMAUX_MAINTENANCE_SERVE_CYCLE_SECONDS".to_owned(),
            cycle.to_string(),
        ),
        (
            "HUMAUX_MAINTENANCE_SERVE_TENANTS_PER_RUN".to_owned(),
            tenants_per_run.to_string(),
        ),
    ];
    for task in SERVE_TASKS {
        keys.push((
            format!("HUMAUX_MAINTENANCE_SERVE_{task}_EVERY_SECONDS"),
            cycle.to_string(),
        ));
        keys.push((
            format!("HUMAUX_MAINTENANCE_SERVE_{task}_LIMIT"),
            "50".to_owned(),
        ));
    }
    for (key, value) in [
        // ADR-0063 D-K / ADR-0064 D-K: the cluster-level tasks have a cadence and no LIMIT.
        (
            "HUMAUX_MAINTENANCE_SERVE_PARTITIONS_EVERY_SECONDS",
            cycle.to_string(),
        ),
        (
            "HUMAUX_MAINTENANCE_SERVE_DR_EVIDENCE_EVERY_SECONDS",
            cycle.to_string(),
        ),
        // ADR-0064 10.11 J: any existing directory on each filesystem.
        (
            "HUMAUX_MAINTENANCE_DR_REPO_FS_PATH",
            repo_fs.to_string_lossy().into_owned(),
        ),
        ("HUMAUX_MAINTENANCE_DR_PGDATA_FS_PATH", "/".to_owned()),
        (
            "HUMAUX_MAINTENANCE_SERVE_LOST_AFTER_SECONDS",
            "120".to_owned(),
        ),
        ("HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS", "60".to_owned()),
    ] {
        keys.push((key.to_owned(), value));
    }
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

/// ADR-0064 D-K (T-L1 step 5, T-J13): one DR_EVIDENCE pass the way production runs it — `humaux-maintenance
/// --serve` against `dsn` (role_maintenance) until its first cycle answers `/metrics` 200 (every task runs on the
/// first cycle), then killed. Returns that exposition.
pub fn dr_evidence_pass(dsn: &str, repo_fs: &Path) -> String {
    let addr = free_addr();
    let env = serve_keys(addr, dsn, 10, 1000, repo_fs);
    // dep: subprocess(humaux-maintenance) — one resident `--serve`, killed when this pass returns or unwinds
    let _serve = Command::new(BIN)
        .arg("--serve")
        .env_clear()
        .envs(env.iter().map(|(k, v)| (k, v)))
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn_reaped("spawn humaux-maintenance --serve");
    let answer = poll(addr, "/metrics", Duration::from_secs(60), |(c, _)| {
        *c == 200
    });
    let (code, body) = answer.expect("--serve answers /metrics");
    assert_eq!(code, 200, "the first DR_EVIDENCE cycle: {body}");
    body
}

/// The value of the exposition sample `series` (name and labels exactly), `None` when it is not rendered.
pub fn sample_value(body: &str, series: &str) -> Option<f64> {
    body.lines()
        .find_map(|l| l.strip_prefix(series)?.strip_prefix(' ')?.parse().ok())
}
