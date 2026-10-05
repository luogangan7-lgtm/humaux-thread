//! `adapters::tests::support::throwaway_db` — one migrated throwaway database per test (the
//!   `health_snapshot.rs` pattern, shared since card 35): tests that move ticket state (ISSUED -> LOST) or purge
//!   rows run here, never on the shared dev database.
//! Depends-on: crates=[humaux-testkit, postgres]; services=[PostgreSQL(owner) r=[ops.schema_migrations]];
//!   env=[CARGO_MANIFEST_DIR, HUMAUX_TEST_PG_DSN]; modules=[humaux-testkit]
//! Called-by: [adapters::tests::a2_point_identity, adapters::tests::confirm_token_retention,
//!   adapters::tests::enumerate_scale, adapters::tests::maintenance_doors, adapters::tests::projection_lag,
//!   adapters::tests::quota_and_rate, adapters::tests::stream_repo, private-worker::tests::derived_dispatch_e2e]
//! Invariants: [the database is humaux_thread_<prefix>_<pid>_<n>, created here and dropped WITH (FORCE) by Drop
//!   even on panic; migrations are applied one database at a time per process (role DDL is cluster-global)]
//! Spec: Baseline §79.2; ADR-0062 E8

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use humaux_testkit::DbFixtureSkipReason;
use postgres::{Client, NoTls};

static NEXT: AtomicUsize = AtomicUsize::new(0);

/// `postgres://user:pass@host:port/<db>?query` with the database replaced.
pub fn with_db(dsn: &str, db: &str) -> String {
    let (head, tail) = dsn.split_at(dsn.rfind('/').expect("dsn has a database path") + 1);
    let query = tail.find('?').map_or("", |i| &tail[i..]);
    format!("{head}{db}{query}")
}

/// A created and migrated database; dropping it drops the database.
pub struct ThrowawayDb {
    owner_dsn: String,
    /// The database name.
    pub name: String,
}

impl ThrowawayDb {
    /// The owner DSN pointed at this database.
    pub fn dsn(&self) -> String {
        with_db(&self.owner_dsn, &self.name)
    }
}

impl Drop for ThrowawayDb {
    fn drop(&mut self) {
        // dep: PostgreSQL(owner) — drop this test's throwaway database
        match Client::connect(&with_db(&self.owner_dsn, "postgres"), NoTls) {
            Ok(mut admin) => {
                let drop_db = format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name);
                if let Err(e) = admin.batch_execute(&drop_db) {
                    eprintln!("throwaway cleanup: {drop_db} failed: {e}");
                }
            }
            Err(e) => eprintln!(
                "throwaway cleanup: cannot reach postgres to drop {}: {e}",
                self.name
            ),
        }
    }
}

/// Creates `humaux_thread_<prefix>_<pid>_<n>` from `HUMAUX_TEST_PG_DSN` and applies every migration body in order.
pub fn create(prefix: &str) -> Result<ThrowawayDb, DbFixtureSkipReason> {
    let owner_dsn =
        std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
    let name = format!(
        "humaux_thread_{prefix}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    );
    // dep: PostgreSQL(owner) — create this test's throwaway database
    let mut admin = Client::connect(&with_db(&owner_dsn, "postgres"), NoTls)
        .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
    admin
        .batch_execute(&format!("CREATE DATABASE {name}"))
        .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
    let db = ThrowawayDb { owner_dsn, name };
    // dep: PostgreSQL(owner) — apply the migration bodies to the throwaway database
    let mut client = Client::connect(&db.dsn(), NoTls)
        .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
    migrate(&mut client).map_err(DbFixtureSkipReason::IsolationSetupFailed)?;
    Ok(db)
}

/// Every migration body in file order (the manifest checks are the `xtask migrate` gate's job).
fn migrate(client: &mut Client) -> Result<(), String> {
    static ONE_AT_A_TIME: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _serial = ONE_AT_A_TIME
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .map_err(|e| format!("{}: {e}", dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    files.sort();
    client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS ops; CREATE TABLE IF NOT EXISTS ops.schema_migrations \
             (migration_id text PRIMARY KEY, checksum text NOT NULL, \
              applied_at timestamptz NOT NULL DEFAULT now())",
        )
        .map_err(|e| format!("migration ledger bootstrap: {e}"))?;
    for file in files {
        let sql = std::fs::read_to_string(&file).map_err(|e| format!("{}: {e}", file.display()))?;
        client
            .batch_execute(&sql)
            .map_err(|e| format!("apply {}: {e:?}", file.display()))?;
    }
    Ok(())
}
