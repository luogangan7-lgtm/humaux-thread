//! xtask `migrate` — applies `migrations/*.sql` in filename order against a live PostgreSQL
//! instance (§46 migration safety). DSN comes from `--dsn <url>` or `HUMAUX_TEST_PG_DSN`;
//! neither present ⇒ `not_applicable` naming the missing object (§57.1 rule 2), never a
//! silent skip. Applied migrations are recorded in `ops.schema_migrations` (self-bootstrapped
//! here — no earlier migration can create the table this runner needs before it runs) so a
//! second run is idempotent (0 applied, not an error).
//!
//! Each `migrations/<stem>.sql` file is sent as one PostgreSQL simple-query message via
//! [`postgres::Client::batch_execute`]: per the wire protocol, multiple statements in a single
//! simple-query message run as one implicit transaction unless the file itself contains
//! `BEGIN`/`COMMIT` — so a file either applies completely or not at all, no partial DDL.

use postgres::{Client, NoTls};
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

const DSN_ENV: &str = "HUMAUX_TEST_PG_DSN";

/// Bootstrap table this runner owns; not a `migrations/*.sql` file itself because it must
/// exist *before* the first migration can be recorded (chicken-and-egg on migration 0001).
const BOOTSTRAP_SQL: &str = "\
    CREATE SCHEMA IF NOT EXISTS ops; \
    CREATE TABLE IF NOT EXISTS ops.schema_migrations ( \
      migration_id text PRIMARY KEY, \
      checksum     text NOT NULL, \
      applied_at   timestamptz NOT NULL DEFAULT now() \
    );";

/// FNV-1a: dependency-free, deterministic, sufficient for drift detection (this is not a
/// security boundary — sha256 is already the workspace's payload-identity function,
/// §48.0①, and reusing it here would suggest this check carries that same guarantee).
fn fnv1a_hex(bytes: &[u8]) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let mut out = String::with_capacity(16);
    let _ = write!(out, "{hash:016x}");
    out
}

fn dsn_from_args(args: &[String]) -> Option<String> {
    args.iter()
        .position(|a| a == "--dsn")
        .and_then(|i| args.get(i + 1))
        .cloned()
}

/// One `migrations/*.sql` file in filename order, paired with its already-loaded contents.
struct PendingMigration {
    migration_id: String,
    sql: String,
}

/// Enumerates `migrations_dir/*.sql` sorted by filename (the numeric prefix is the ordering
/// key, §46 "按序应用"). Manifest presence/shape is `migration-rehearsal`'s job, not this
/// runner's — a `.sql` file with no manifest still applies here.
fn collect_migrations(migrations_dir: &Path) -> Result<Vec<PendingMigration>, String> {
    let mut paths: Vec<_> = fs::read_dir(migrations_dir)
        .map_err(|e| format!("cannot read {}: {e}", migrations_dir.display()))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("sql"))
        .collect();
    paths.sort();

    paths
        .into_iter()
        .map(|path| {
            let migration_id = path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            let sql = fs::read_to_string(&path)
                .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
            Ok(PendingMigration { migration_id, sql })
        })
        .collect()
}

/// Applies every not-yet-recorded migration in order; stops at the first failure (a later
/// migration may depend on an earlier one's objects, so partial-then-continue would mask
/// the real error behind a cascade of unrelated ones). Returns `(applied, skipped)` counts.
fn apply_all(
    client: &mut Client,
    migrations: &[PendingMigration],
) -> Result<(usize, usize), String> {
    client
        .batch_execute(BOOTSTRAP_SQL)
        .map_err(|e| format!("bootstrap ops.schema_migrations: {e}"))?;

    let mut applied = 0usize;
    let mut skipped = 0usize;
    for m in migrations {
        // §46 drift detection: `ops.schema_migrations.checksum` is written on every apply
        // but was never read back — editing an already-applied file silently printed
        // "skip (already applied)" and returned `pass`, on the one axis the column exists
        // to catch. `Option` distinguishes "not yet applied" (no row) from "applied with
        // this checksum" so a changed file fails loudly instead of being skipped quietly.
        let recorded_checksum: Option<String> = client
            .query_opt(
                "SELECT checksum FROM ops.schema_migrations WHERE migration_id = $1",
                &[&m.migration_id],
            )
            .map_err(|e| {
                format!(
                    "{}: cannot check ops.schema_migrations: {e}",
                    m.migration_id
                )
            })?
            .map(|row| row.get(0));

        if let Some(recorded) = recorded_checksum {
            let current = fnv1a_hex(m.sql.as_bytes());
            if current != recorded {
                return Err(format!(
                    "{}: drift detected — file's checksum {current} does not match \
                     recorded {recorded} (already applied with different content; §46 \
                     migrations are immutable once applied, add a new migration instead \
                     of editing this one)",
                    m.migration_id
                ));
            }
            skipped += 1;
            eprintln!("migrate: skip  {} (already applied)", m.migration_id);
            continue;
        }

        client.batch_execute(&m.sql).map_err(|e| {
            // `Display` alone can collapse to a bare "db error" with no message; the
            // server's actual complaint lives in `DbError` (falls back to `{e:?}` for
            // non-server errors like a connection drop mid-batch).
            let detail = e
                .as_db_error()
                .map(|db| format!("{} — {}", db.message(), db.detail().unwrap_or("")))
                .unwrap_or_else(|| format!("{e:?}"));
            format!("{}: {detail}", m.migration_id)
        })?;

        let checksum = fnv1a_hex(m.sql.as_bytes());
        client
            .execute(
                "INSERT INTO ops.schema_migrations (migration_id, checksum) VALUES ($1, $2)",
                &[&m.migration_id, &checksum],
            )
            .map_err(|e| {
                format!(
                    "{}: cannot record in ops.schema_migrations: {e}",
                    m.migration_id
                )
            })?;

        applied += 1;
        eprintln!("migrate: apply {}", m.migration_id);
    }

    Ok((applied, skipped))
}

/// `cargo xtask migrate [--dsn <url>]` — three-state: `not_applicable` (no DSN configured,
/// names the missing env var), `fail` (any migration errors, names which one), `pass`
/// (every migration recorded, whether newly applied or already-applied/idempotent-skipped).
pub fn run(args: &[String]) -> i32 {
    let dsn = dsn_from_args(args).or_else(|| std::env::var(DSN_ENV).ok());
    let Some(dsn) = dsn else {
        eprintln!(
            "migrate: not_applicable (missing object: --dsn or ${DSN_ENV} env var — no target database configured)"
        );
        return 0;
    };

    let migrations_dir = Path::new("migrations");
    let migrations = match collect_migrations(migrations_dir) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("migrate: fail ({e})");
            return 1;
        }
    };

    let mut client = match Client::connect(&dsn, NoTls) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("migrate: fail (cannot connect to {DSN_ENV}: {e})");
            return 1;
        }
    };

    match apply_all(&mut client, &migrations) {
        Ok((applied, skipped)) => {
            eprintln!(
                "migrate: pass ({applied} applied, {skipped} already-applied, {} total)",
                migrations.len()
            );
            0
        }
        Err(e) => {
            eprintln!("migrate: fail ({e})");
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use postgres::Client;
    use std::fs;

    #[test]
    fn collect_migrations_sorts_by_filename() {
        let dir =
            std::env::temp_dir().join(format!("xtask_migrate_collect_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("0002_b.sql"), "-- second").unwrap();
        fs::write(dir.join("0001_a.sql"), "-- first").unwrap();
        // Manifest files must be ignored by the runner (that's migration-rehearsal's
        // domain, §46.1) — a stray .toml here must not appear in the applied set.
        fs::write(
            dir.join("0001_a.manifest.toml"),
            "migration_id = \"0001_a\"",
        )
        .unwrap();

        let migrations = collect_migrations(&dir).expect("dir reads cleanly");
        let ids: Vec<&str> = migrations.iter().map(|m| m.migration_id.as_str()).collect();
        assert_eq!(ids, vec!["0001_a", "0002_b"]);

        let _ = fs::remove_dir_all(&dir);
    }

    /// Idempotency + atomicity, live-DB gated (repo CLAUDE.md 硬规则③: skip with a
    /// printed reason, never a silent pass, when `HUMAUX_TEST_PG_DSN` is unset). Runs
    /// against an isolated schema so a failing assertion never pollutes the dev DB's
    /// main schemas (硬规则④).
    #[test]
    fn apply_all_is_idempotent_and_atomic_on_a_bad_file() {
        let Ok(dsn) = std::env::var(DSN_ENV) else {
            eprintln!("migrate test: not_applicable — {DSN_ENV} unset, skipping");
            return;
        };
        let Ok(mut client) = Client::connect(&dsn, NoTls) else {
            eprintln!(
                "migrate test: not_applicable — cannot reach Postgres at ${DSN_ENV}, skipping"
            );
            return;
        };

        // `ops.schema_migrations.migration_id` is a real (global, not schema-scoped)
        // primary key shared with the dev DB's actual migration history — reusing a
        // fixed id like "0001_ok" across test runs would find it "already applied" from
        // a *previous* run of this same test and silently break the first-run
        // assertion below. A run-unique suffix keeps this test's rows from colliding
        // with themselves, with real migrations, or with another agent's concurrent
        // run of this same suite against the same shared dev DB.
        let run_id = format!(
            "{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let schema = format!("xtask_migrate_fixture_{run_id}");
        client
            .batch_execute(&format!(
                "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema};"
            ))
            .expect("fixture schema setup");

        let id_ok1 = format!("test_{run_id}_0001_ok");
        let id_ok2 = format!("test_{run_id}_0002_ok");
        let id_bad = format!("test_{run_id}_0003_bad");
        let migrations = vec![
            PendingMigration {
                migration_id: id_ok1.clone(),
                sql: format!("CREATE TABLE {schema}.t (id int)"),
            },
            PendingMigration {
                migration_id: id_ok2.clone(),
                sql: format!("INSERT INTO {schema}.t VALUES (1)"),
            },
        ];

        let (applied, skipped) = apply_all(&mut client, &migrations).expect("clean apply");
        assert_eq!((applied, skipped), (2, 0), "first run applies both");

        let (applied, skipped) = apply_all(&mut client, &migrations).expect("idempotent re-apply");
        assert_eq!(
            (applied, skipped),
            (0, 2),
            "second run must apply nothing, skip both"
        );

        // Fault injection: a third migration that fails at RUNTIME (not parse time) must
        // roll back its whole batch-execute call and record nothing for it (simple-query
        // implicit transaction atomicity — the module doc's central claim). A syntax error
        // like a stray `THIS IS NOT SQL;` is rejected by the parser for the entire
        // multi-statement string before any statement executes — `to_regclass(u) IS NULL`
        // would then hold trivially because nothing ran, not because rollback undid it,
        // and this test would still pass against a server that wrapped nothing in a
        // transaction. `1/0` only fails once the engine actually executes it, after the
        // preceding CREATE TABLE has already run — only rollback can explain it vanishing.
        let bad = vec![PendingMigration {
            migration_id: id_bad.clone(),
            sql: format!("CREATE TABLE {schema}.u (id int); INSERT INTO {schema}.u VALUES (1/0);"),
        }];
        let err = apply_all(&mut client, &bad)
            .expect_err("a runtime-failing statement must fail, not silently pass");
        assert!(
            err.contains(&id_bad),
            "error must name the failing migration: {err}"
        );

        let recorded: bool = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM ops.schema_migrations WHERE migration_id = $1)",
                &[&id_bad],
            )
            .unwrap()
            .get(0);
        assert!(
            !recorded,
            "failed migration must not be recorded (atomicity)"
        );

        let table_exists: bool = client
            .query_one(
                &format!("SELECT to_regclass('{schema}.u') IS NOT NULL"),
                &[],
            )
            .unwrap()
            .get(0);
        assert!(
            !table_exists,
            "the CREATE TABLE half of the failed batch must have rolled back too"
        );

        client
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .expect("fixture cleanup");
        client
            .execute(
                "DELETE FROM ops.schema_migrations WHERE migration_id = ANY($1)",
                &[&vec![id_ok1, id_ok2, id_bad]],
            )
            .expect("fixture cleanup: unrecord this test's own migration ids");
    }
}
