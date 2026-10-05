//! `xtask::serial_lane` — card 23 serial isolated lane runner for #[ignore]d DB/Qdrant tests.
//! Depends-on: crates=[humaux-testkit, postgres]; services=[PostgreSQL(any) r=[ops.schema_migrations]
//!   w=[ops.schema_migrations], Qdrant(*), subprocess(cargo), subprocess(humaux-*)];
//!   env=[CARGO, CARGO_MANIFEST_DIR, CARGO_TARGET_DIR, HUMAUX_0132_GATE_MODE, HUMAUX_ADMIN_PG_DSN,
//!   HUMAUX_GATEWAY_PG_DSN, HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_MECHANISM_ADMIN_BIN, HUMAUX_MECHANISM_FIXTURE,
//!   HUMAUX_PUBLIC_PROVENANCE_FAULT_DB, HUMAUX_REQUIRE_DB, HUMAUX_RETRIEVAL_WORKER_PG_DSN,
//!   HUMAUX_SERIAL_LANE_WARM_SECS, HUMAUX_TEST_GITLEAKS_BIN, HUMAUX_TEST_PG_DSN, HUMAUX_TEST_QDRANT_PORT];
//!   modules=[xtask::migrate]
//! Called-by: [xtask::main]
//! Invariants: [every #[ignore] carries lane(a|b|c) in its reason and an ignored test without one reds the gate;
//!   lane(a) resources are provisioned before the run; a missing dependency is a failure under HUMAUX_REQUIRE_DB,
//!   never a SKIP; every database the lane created is dropped when its group ends (also on a red test or a panic),
//!   never humaux_thread_{dev,c31,ci,stable_observations}]
//! Spec: Baseline §79.2; ADR-0047
//!
//! `cargo xtask serial-lane` — card 23's serial isolated lane for the `#[ignore]`d tests.
//!
//! # Why this exists
//!
//! The deployment report counted ~108 tests that are `#[ignore]`d because they need a serial
//! run against an isolated database (and, for some, an isolated Qdrant). Nothing ran them, so
//! a whole tier of coverage was green only because nobody looked. This subcommand is the
//! place that looks.
//!
//! # The disposition rule (the part that cannot rot)
//!
//! Every `#[ignore = "..."]` in `crates/` and `bins/` must carry its disposition **in the
//! ignore reason itself** — `lane(<disposition>) <free text>`:
//!
//! - `lane(a:<resource>)` — needs a dedicated resource this lane provisions, then runs.
//! - `lane(b)` — timing-sensitive: runs here, serially, after the warm-up below.
//! - `lane(c)` — retired: it pins a path that no longer exists. Not run; the free text is the
//!   written reason, and an empty one is a red.
//!
//! The disposition lives next to the test rather than in a table inside this file on purpose.
//! A table here would be a second list of the same tests, and the only thing keeping two
//! lists in step is whoever remembers — which is exactly the failure this card exists to
//! stop. [`inventory`] walks the tree, so an ignored test added tomorrow without a disposition
//! reds this gate the first time it runs, wherever it was added.
//!
//! # Warm-up (the folded card-8/card-17 host-transient debts)
//!
//! macOS pays a Gatekeeper/XProtect provenance assessment the first time a freshly linked
//! executable is exec'd; during an assessment storm it has been measured past two minutes.
//! Tests with their own short deadlines (the scanner fixture's 2 s probe, the gateway's 5 s
//! process-start budget) then go red for a reason that has nothing to do with the code. The
//! lane therefore exec's the pinned scanner and every test binary once, **before** any timed
//! step, and reports that warm-up time separately from the lane's own wall clock — widening a
//! production timeout to hide a host effect is not on the table.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

/// A resource a `lane(a:…)` test needs before it can run. Closed set: an unknown resource name
/// is a red, not a silently skipped test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Resource {
    /// The repo-wide isolated test database named by `HUMAUX_TEST_PG_DSN` (§79.2), which must
    /// already be at head: the lane checks it and never migrates it (ADR-0063 D-M).
    SharedDb,
    /// A per-run `humaux_thread_request_guard_<stamp>` database, with every role DSN pointed at
    /// it — the "dedicated request-guard fixture" these tests' ignore reasons promise. It is a
    /// real database, not a synonym for [`Self::SharedDb`]: the fixtures seed fixed
    /// `TENANT_ID` / `USER_ID` constants and audit rows that a shared database carries between
    /// runs.
    RequestGuard,
    /// A disposable Qdrant reachable at `HUMAUX_TEST_QDRANT_PORT`, plus a per-run database so
    /// the registry oracle does not meet serving projections an earlier run left behind.
    Qdrant,
    /// The test creates and drops its own database; it needs a maintenance login that may do
    /// so, i.e. `HUMAUX_MAINTENANCE_PG_DSN`, and a per-run database to do it from — which is
    /// also the isolation the global-queue / global-profile oracles in this group need.
    Disposable,
    /// A throwaway database migrated only through 0131, plus `HUMAUX_0132_GATE_MODE=pre0132`.
    /// The test re-runs 0132's own SQL and requires its `55000` hard stop, so head is the wrong
    /// target: on head the objects already exist and the migration fails for another reason.
    Pre0132,
    /// The shared database (already past 0132) plus `HUMAUX_0132_GATE_MODE=post0132`.
    Post0132,
    /// `humaux_thread_stable_observations` plus `HUMAUX_MECHANISM_FIXTURE` naming it.
    MechanismFixture,
    /// The mechanism admin binary at `HUMAUX_MECHANISM_ADMIN_BIN`.
    MechanismAdminBin,
    /// A throwaway database the provenance fault test may damage
    /// (`HUMAUX_PUBLIC_PROVENANCE_FAULT_DB=1` is its own refuse-to-run guard).
    ProvenanceMutation,
}

impl Resource {
    fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "shared_db" => Self::SharedDb,
            "request_guard" => Self::RequestGuard,
            "qdrant" => Self::Qdrant,
            "disposable" => Self::Disposable,
            "pre_0132" => Self::Pre0132,
            "post_0132" => Self::Post0132,
            "mechanism_fixture" => Self::MechanismFixture,
            "mechanism_admin_bin" => Self::MechanismAdminBin,
            "provenance_mutation" => Self::ProvenanceMutation,
            _ => return None,
        })
    }

    const fn label(self) -> &'static str {
        match self {
            Self::SharedDb => "shared_db",
            Self::RequestGuard => "request_guard",
            Self::Qdrant => "qdrant",
            Self::Disposable => "disposable",
            Self::Pre0132 => "pre_0132",
            Self::Post0132 => "post_0132",
            Self::MechanismFixture => "mechanism_fixture",
            Self::MechanismAdminBin => "mechanism_admin_bin",
            Self::ProvenanceMutation => "provenance_mutation",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Disposition {
    /// (a) needs a provisioned resource.
    Provisioned(Resource),
    /// (b) timing-sensitive — serial, after warm-up.
    Timing,
    /// (c) retired with a written reason; deliberately not run.
    Retired,
}

impl Disposition {
    /// `lane(a:shared_db)` / `lane(b)` / `lane(c)`.
    fn parse(tag: &str) -> Option<Self> {
        match tag {
            "b" => Some(Self::Timing),
            "c" => Some(Self::Retired),
            other => other
                .strip_prefix("a:")
                .and_then(Resource::parse)
                .map(Self::Provisioned),
        }
    }
}

#[derive(Debug, Clone)]
struct Entry {
    file: PathBuf,
    line: usize,
    package: String,
    /// `Some(name)` for an integration test target, `None` for a `--lib` unit test.
    target: Option<String>,
    test: String,
    disposition: Disposition,
    note: String,
}

/// One `#[ignore = "…"]` that cannot be turned into an [`Entry`] — the audit's red list.
#[derive(Debug)]
struct Defect {
    file: PathBuf,
    line: usize,
    why: String,
}

// ---------------------------------------------------------------------------------------
// Inventory
// ---------------------------------------------------------------------------------------

fn rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n == "target") {
                    continue;
                }
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

/// Package name of the crate owning `file`: the nearest ancestor `Cargo.toml`'s `name`.
fn package_of(file: &Path) -> Option<String> {
    let mut dir = file.parent()?;
    loop {
        let manifest = dir.join("Cargo.toml");
        if manifest.is_file() {
            let text = std::fs::read_to_string(&manifest).ok()?;
            for line in text.lines() {
                if let Some(rest) = line.trim().strip_prefix("name")
                    && let Some(value) = rest.trim().strip_prefix('=')
                {
                    return Some(value.trim().trim_matches('"').to_string());
                }
            }
            return None;
        }
        dir = dir.parent()?;
    }
}

/// `crates/<c>/tests/<t>.rs` → integration target `<t>`; anything under `src/` → `--lib`.
fn target_of(file: &Path) -> Option<String> {
    let parent = file.parent()?;
    if parent.file_name().is_some_and(|n| n == "tests") {
        return Some(file.file_stem()?.to_string_lossy().into_owned());
    }
    // A `tests/support/*.rs` helper carries no `#[test]`; anything deeper under `tests/` that
    // somehow does would be reported as a defect by the caller rather than guessed at.
    if file.components().any(|c| c.as_os_str() == "src") {
        None
    } else {
        Some(file.file_stem()?.to_string_lossy().into_owned())
    }
}

/// Walk `crates/` and `bins/`, returning every disposed ignored test and every defect.
///
/// Only a line whose trimmed text *starts* with `#[ignore` counts: the same text inside a doc
/// comment (`crates/contracts/**` documents the G50-1 rule about `#[ignore]`, and
/// `bins/retrieval-worker/src/main.rs` explains why one test is `#[ignore]`-free) is prose,
/// not an attribute, and treating it as one would make the audit demand a disposition for a
/// sentence.
fn inventory(root: &Path) -> (Vec<Entry>, Vec<Defect>) {
    let mut entries = Vec::new();
    let mut defects = Vec::new();
    for dir in ["crates", "bins"] {
        for file in rust_sources(&root.join(dir)) {
            let Ok(text) = std::fs::read_to_string(&file) else {
                continue;
            };
            let lines: Vec<&str> = text.lines().collect();
            for (i, raw) in lines.iter().enumerate() {
                if !raw.trim_start().starts_with("#[ignore") {
                    continue;
                }
                let rel = file.strip_prefix(root).unwrap_or(&file).to_path_buf();
                let line = i + 1;
                match read_ignore(&file, &rel, &lines, line) {
                    Ok(entry) => entries.push(entry),
                    Err(why) => defects.push(Defect {
                        file: rel,
                        line,
                        why,
                    }),
                }
            }
        }
    }
    (entries, defects)
}

/// Read one `#[ignore]` attribute at 1-based `line` into an [`Entry`], or say what is wrong
/// with it. Split out of [`inventory`] so the error path is a returned `String` rather than a
/// closure holding a mutable borrow of the defect list across the success path.
fn read_ignore(file: &Path, rel: &Path, lines: &[&str], line: usize) -> Result<Entry, String> {
    let trimmed = lines[line - 1].trim();
    let reason = trimmed
        .strip_prefix("#[ignore = \"")
        .and_then(|r| r.strip_suffix("\"]"))
        .ok_or_else(|| {
            "bare `#[ignore]` (or a reason this lane cannot read on one line) — every ignore \
             must carry `lane(a:<resource>)`, `lane(b)` or `lane(c)`"
                .to_string()
        })?;
    let rest = reason.strip_prefix("lane(").ok_or_else(|| {
        format!(
            "no disposition: reason starts {:?} — prefix it with `lane(a:<resource>) `, \
             `lane(b) ` or `lane(c) `",
            &reason[..reason.len().min(48)]
        )
    })?;
    let (tag, note) = rest
        .split_once(')')
        .ok_or_else(|| "malformed disposition: `lane(` never closed".to_string())?;
    let disposition =
        Disposition::parse(tag).ok_or_else(|| format!("unknown disposition `lane({tag})`"))?;
    let note = note.trim().to_string();
    if note.is_empty() {
        return Err(format!(
            "`lane({tag})` carries no written reason — a disposition without one is the silent \
             ignore this gate exists to stop"
        ));
    }
    let test = lines[line..]
        .iter()
        .take(12)
        .find_map(|l| {
            let idx = l.find("fn ")?;
            let name: String = l[idx + 3..]
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            (!name.is_empty()).then_some(name)
        })
        .ok_or_else(|| "no `fn` follows this `#[ignore]`".to_string())?;
    let package =
        package_of(file).ok_or_else(|| "cannot resolve the owning cargo package".to_string())?;
    Ok(Entry {
        file: rel.to_path_buf(),
        line,
        package,
        target: target_of(file),
        test,
        disposition,
        note,
    })
}

// ---------------------------------------------------------------------------------------
// Provisioning
// ---------------------------------------------------------------------------------------

/// Swap the database component of a `postgres://user:pw@host:port/db` DSN.
///
/// String surgery rather than a URL crate: `xtask` has no `url` dependency, the DSNs this lane
/// handles are the four the card pins (no query string — the fixtures reject `?`/`#` outright),
/// and adding a dependency to rewrite one path segment is the shape ponytail exists to refuse.
fn dsn_with_database(dsn: &str, database: &str) -> Option<String> {
    let (scheme, rest) = dsn.split_once("://")?;
    let (authority, _) = rest.split_once('/')?;
    Some(format!("{scheme}://{authority}/{database}"))
}

/// Create `database` if it is absent, then migrate it (optionally stopping at `through`).
///
/// `postgres::Client` rather than shelling out to `psql`: xtask already depends on the driver
/// for `migrate` and `rls-check`, and `psql` is not on this node's PATH — a provisioner that
/// needs a client binary nobody installed is a provisioner that always reports "missing
/// object" and never provisions anything.
///
/// `CREATE DATABASE` cannot run inside a transaction and has no `IF NOT EXISTS`, so existence
/// is a `pg_database` read first, and the name cannot be a bind parameter. Additive only: an
/// existing database is migrated, never dropped — this lane must be safe to point at a
/// database somebody else is using.
fn ensure_database(admin_dsn: &str, database: &str) -> Result<(), String> {
    ensure_database_through(admin_dsn, database, None)
}

fn ensure_database_through(
    admin_dsn: &str,
    database: &str,
    through: Option<&str>,
) -> Result<(), String> {
    let postgres_dsn = dsn_with_database(admin_dsn, "postgres")
        .ok_or_else(|| "cannot rewrite the admin DSN onto the `postgres` database".to_string())?;
    // dep: PostgreSQL(any) — postgres_dsn, serial lane target database
    let mut client = postgres::Client::connect(&postgres_dsn, postgres::NoTls)
        .map_err(|e| format!("cannot connect to provision {database}: {e}"))?;
    create_if_absent(&mut client, database)?;
    let target = dsn_with_database(admin_dsn, database)
        .ok_or_else(|| "cannot rewrite the admin DSN onto the target database".to_string())?;
    let mut args = vec!["--dsn".to_string(), target];
    if let Some(through) = through {
        args.push("--through".to_string());
        args.push(through.to_string());
    }
    let code = crate::migrate::run(&args);
    if code == 0 {
        Ok(())
    } else {
        Err(format!("migrate exited {code} against {database}"))
    }
}

/// `CREATE DATABASE` unless `database` exists; one this process created is recorded for [`drop_provisioned`].
fn create_if_absent(client: &mut postgres::Client, database: &str) -> Result<(), String> {
    let exists = client
        .query_opt("SELECT 1 FROM pg_database WHERE datname = $1", &[&database])
        .map_err(|e| format!("cannot read pg_database: {e}"))?
        .is_some();
    if !exists {
        let quoted = database.replace('"', "\"\"");
        client
            .batch_execute(&format!("CREATE DATABASE \"{quoted}\""))
            .map_err(|e| format!("cannot create {database}: {e}"))?;
        eprintln!("serial-lane: provisioned database {database}");
        if let Ok(mut created) = PROVISIONED_THIS_RUN.lock() {
            created.push(database.to_string());
        }
    }
    Ok(())
}

/// Every database THIS process created (never one it found), in creation order, not yet dropped — the only set
/// [`drop_provisioned`] may touch. Each lane run used to leave ~10 `humaux_thread_{request_guard,qdrant,disposable,
/// pre0132,prov_fault}_<stamp>` databases (~200 MB) on the shared container unless `--drop-provisioned` was passed
/// (47 found on 2026-10-05); dropping is now the lane's own job (owner disk constraint, 2026-10-05).
static PROVISIONED_THIS_RUN: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

/// How many databases this run dropped so far (the end-of-lane line).
static DROPPED_THIS_RUN: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Never dropped, even when this process created them: the shared dev and CI databases and the fixed-name fixture
/// (`humaux_thread_stable_observations`) the next lane run expects.
const NEVER_DROPPED: [&str; 4] = [
    "humaux_thread_dev",
    "humaux_thread_c31",
    "humaux_thread_ci",
    "humaux_thread_stable_observations",
];

/// Drops (`WITH (FORCE)`) every database recorded in [`PROVISIONED_THIS_RUN`] and forgets it; called when the lane
/// group that provisioned them ends and once more when the lane ends (also by [`DropOnExit`] on a panic). Each drop
/// is reported by name; a failed drop is a visible line, not an exit code (the tests already have theirs). Returns
/// the number dropped.
fn drop_provisioned() -> usize {
    let created: Vec<String> = PROVISIONED_THIS_RUN
        .lock()
        .map(|mut c| std::mem::take(&mut *c))
        .unwrap_or_default();
    let created: Vec<String> = created
        .into_iter()
        .filter(|d| !NEVER_DROPPED.contains(&d.as_str()))
        .collect();
    if created.is_empty() {
        return 0;
    }
    let Some(postgres_dsn) = std::env::var("HUMAUX_TEST_PG_DSN")
        .ok()
        .and_then(|admin| dsn_with_database(&admin, "postgres"))
    else {
        eprintln!("serial-lane: cannot drop {created:?}: HUMAUX_TEST_PG_DSN unset or not a URL");
        return 0;
    };
    // dep: PostgreSQL(any) — postgres_dsn, serial lane target database
    let mut client = match postgres::Client::connect(&postgres_dsn, postgres::NoTls) {
        Ok(client) => client,
        Err(e) => {
            eprintln!("serial-lane: cannot connect to drop {created:?}: {e}");
            return 0;
        }
    };
    let mut dropped = 0;
    for database in created {
        let quoted = database.replace('"', "\"\"");
        // FORCE: a test that leaked a pooled connection must not keep its throwaway alive.
        match client.batch_execute(&format!(
            "DROP DATABASE IF EXISTS \"{quoted}\" WITH (FORCE)"
        )) {
            Ok(()) => {
                println!("serial-lane: dropped provisioned database {database}");
                dropped += 1;
            }
            Err(e) => eprintln!("serial-lane: could not drop {database}: {e}"),
        }
    }
    DROPPED_THIS_RUN.fetch_add(dropped, std::sync::atomic::Ordering::Relaxed);
    dropped
}

/// Drops what is still recorded and prints the run's one count line when the lane returns or unwinds. Ctrl-C
/// (SIGINT) kills the process without unwinding: the lane installs no signal handler.
// ponytail: no SIGINT handler (a killed lane leaves its last group's databases), add one if interrupted lanes recur.
struct DropOnExit;

impl Drop for DropOnExit {
    fn drop(&mut self) {
        drop_provisioned();
        println!(
            "serial-lane: dropped {} provisioned database(s) this run",
            DROPPED_THIS_RUN.load(std::sync::atomic::Ordering::Relaxed)
        );
    }
}

/// ADR-0063 D-M (card 36 review, 2026-10-05): the shared database moves only through the main line's dev-conversion
/// protocol (`c36_dev_convert.sh`: copy rehearsal EQUAL, backup, writers stopped, `MAINLINE_GO=1`). The lane used to
/// run `migrate` against `HUMAUX_TEST_PG_DSN` here, which applied any pending file (a forward fix such as 0232) to dev
/// outside that protocol. Now a shared database that lacks a migration file is an error naming the files (its
/// groups are NOT RUN), and nothing is applied.
fn shared_db_at_head(admin_dsn: &str) -> Result<(), String> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../migrations");
    let files: BTreeSet<String> = std::fs::read_dir(&dir)
        .map_err(|e| format!("cannot read {}: {e}", dir.display()))?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .filter_map(|p| p.file_stem().map(|s| s.to_string_lossy().into_owned()))
        .collect();
    // dep: PostgreSQL(any) — HUMAUX_TEST_PG_DSN, read-only ledger read of the shared database
    let mut client = postgres::Client::connect(admin_dsn, postgres::NoTls)
        .map_err(|e| format!("cannot connect to the shared database: {e}"))?;
    let applied: BTreeSet<String> = client
        .query("SELECT migration_id FROM ops.schema_migrations", &[])
        .map_err(|e| format!("cannot read ops.schema_migrations: {e}"))?
        .iter()
        .map(|row| row.get(0))
        .collect();
    let pending: Vec<&String> = files.difference(&applied).collect();
    if pending.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "the shared database lacks {pending:?}; the lane never migrates it (ADR-0063 D-M: the main \
             line applies them through the dev-conversion protocol)"
        ))
    }
}

/// Seconds since the epoch — names a throwaway database so two lanes never share one.
fn unix_stamp() -> Result<u64, String> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|e| e.to_string())
}

/// Every role DSN in the environment, rewritten to point at `database`.
fn role_dsns_pointed_at(database: &str) -> EnvOverrides {
    [
        "HUMAUX_TEST_PG_DSN",
        "HUMAUX_MAINTENANCE_PG_DSN",
        "HUMAUX_GATEWAY_PG_DSN",
        "HUMAUX_RETRIEVAL_WORKER_PG_DSN",
        "HUMAUX_ADMIN_PG_DSN",
    ]
    .into_iter()
    .filter_map(|var| {
        let dsn = std::env::var(var).ok()?;
        Some((var.to_string(), dsn_with_database(&dsn, database)?))
    })
    .collect()
}

/// Environment overrides a resource group runs under, on top of the caller's environment.
type EnvOverrides = Vec<(String, String)>;

/// `target/debug/humaux-admin`, built if it is not there yet.
///
/// `bins/admin` is a real binary of this workspace (`humaux-admin mechanism status` reads
/// `HUMAUX_ADMIN_PG_DSN`), so `HUMAUX_MECHANISM_ADMIN_BIN` names something the lane can produce.
/// The `dev` profile, because that is where `cargo test` puts the test binaries this group runs
/// beside it.
fn admin_binary() -> Result<PathBuf, String> {
    // `<root>/xtask` is this crate's manifest dir; the lane's own cwd is not assumed.
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or_else(|| "cannot locate the repo root from CARGO_MANIFEST_DIR".to_string())?;
    let target_dir =
        std::env::var("CARGO_TARGET_DIR").map_or_else(|_| root.join("target"), PathBuf::from);
    let path = target_dir.join("debug").join("humaux-admin");
    if !path.is_file() {
        // dep: subprocess(cargo) — cargo build/test invocation for the serial lane
        let built = Command::new(env!("CARGO"))
            .args(["build", "-p", "humaux-admin"])
            .current_dir(root)
            .status()
            .map_err(|e| format!("missing object: humaux-admin (cargo not runnable: {e})"))?;
        if !built.success() {
            return Err("missing object: humaux-admin (cargo build -p humaux-admin failed)".into());
        }
    }
    if path.is_file() {
        Ok(path)
    } else {
        Err(format!("missing object: {}", path.display()))
    }
}

/// Provision `resource`, returning the environment its tests run under, or the missing object.
///
/// Every arm either provisions or names what is missing — there is no arm that returns "run it
/// anyway and hope", because that is how an unprovisioned test becomes a silent skip again.
fn provision(resource: Resource) -> Result<EnvOverrides, String> {
    let admin = std::env::var("HUMAUX_TEST_PG_DSN")
        .map_err(|_| "missing object: HUMAUX_TEST_PG_DSN".to_string())?;
    match resource {
        // The shared isolated database is whatever HUMAUX_TEST_PG_DSN names (the shared dev
        // database): checked at head, never migrated here.
        Resource::SharedDb => shared_db_at_head(&admin).map(|()| Vec::new()),
        // The ignore reasons in `request_guard.rs` / `quota_and_rate.rs` / `service_credentials.rs`
        // promise a *dedicated* fixture. Before card 23's lane run 1 this arm was a synonym for
        // `SharedDb`, so the promise was words: the fixtures seed fixed TENANT_ID/USER_ID
        // constants and audit rows, and a shared database carries them between runs.
        Resource::RequestGuard => {
            let db = format!("humaux_thread_request_guard_{}", unix_stamp()?);
            ensure_database(&admin, &db)?;
            Ok(role_dsns_pointed_at(&db))
        }
        Resource::Qdrant => {
            let port = std::env::var("HUMAUX_TEST_QDRANT_PORT")
                .map_err(|_| "missing object: HUMAUX_TEST_QDRANT_PORT".to_string())?;
            // dep: Qdrant(*) — HUMAUX_TEST_QDRANT_PORT reachability probe
            std::net::TcpStream::connect_timeout(
                &format!("127.0.0.1:{port}")
                    .parse()
                    .map_err(|e| format!("bad HUMAUX_TEST_QDRANT_PORT: {e}"))?,
                Duration::from_secs(2),
            )
            .map_err(|e| format!("missing object: Qdrant on 127.0.0.1:{port} ({e})"))?;
            // "an isolated PostgreSQL fixture migrated through 0119" is what the ignore reason
            // asks for; the shared database carries serving projections from every earlier run,
            // so the registry oracle met `ServingProjectionChanged` instead of its own verdict.
            // Head covers 0119, and a per-run database is the isolation the reason claims.
            let db = format!("humaux_thread_qdrant_{}", unix_stamp()?);
            ensure_database(&admin, &db)?;
            Ok(role_dsns_pointed_at(&db))
        }
        Resource::Disposable => {
            std::env::var("HUMAUX_MAINTENANCE_PG_DSN")
                .map_err(|_| "missing object: HUMAUX_MAINTENANCE_PG_DSN".to_string())?;
            // Also the home of the oracles that read a *global* queue or a global profile
            // table: on the shared database they claimed a row a 2026-09-08 run left
            // PROCESSING, or walked a legacy profile created in September. A per-run database
            // is the only isolation such an oracle can have. The two tests that create and
            // drop their own throwaway database are unaffected — they need a connectable owner
            // DSN, not a particular one.
            let db = format!("humaux_thread_disposable_{}", unix_stamp()?);
            ensure_database(&admin, &db)?;
            Ok(role_dsns_pointed_at(&db))
        }
        Resource::Pre0132 => {
            let stamp = unix_stamp()?;
            let db = format!("humaux_thread_pre0132_{stamp}");
            ensure_database_through(&admin, &db, Some("0131"))?;
            let mut env = vec![("HUMAUX_0132_GATE_MODE".to_string(), "pre0132".to_string())];
            env.extend(role_dsns_pointed_at(&db));
            Ok(env)
        }
        Resource::Post0132 => {
            shared_db_at_head(&admin)?;
            Ok(vec![(
                "HUMAUX_0132_GATE_MODE".to_string(),
                "post0132".to_string(),
            )])
        }
        Resource::MechanismFixture => {
            const DB: &str = "humaux_thread_stable_observations";
            // §79.2 / §57.1 named NOT RUN. `mechanism_observation.rs::admin_pool` `expect`s
            // `HUMAUX_ADMIN_PG_DSN`, and the suite asserts `session_user = 'role_admin'` and
            // that `SET ROLE role_maintenance` is refused — so the object this group needs is a
            // real role_admin LOGIN, not the variable, and the repo's `?options=-c role=…` form
            // on the superuser DSN cannot stand in for it.
            //
            // The arm therefore *connects*. Migration 0110 creates `role_admin` with LOGIN and
            // no password on purpose ("Authentication is provisioned externally"), so a node can
            // have the variable set and still have nothing behind it; a presence check would
            // hand the group five panics where §79.2 wants one named missing object.
            let admin_login = std::env::var("HUMAUX_ADMIN_PG_DSN").map_err(|_| {
                "missing object: HUMAUX_ADMIN_PG_DSN (role_admin login)".to_string()
            })?;
            ensure_database(&admin, DB)?;
            let login_target = dsn_with_database(&admin_login, DB).ok_or_else(|| {
                "cannot rewrite HUMAUX_ADMIN_PG_DSN onto the fixture database".to_string()
            })?;
            // dep: PostgreSQL(any) — login_target, credential check
            postgres::Client::connect(&login_target, postgres::NoTls).map_err(|e| {
                format!("missing object: HUMAUX_ADMIN_PG_DSN (role_admin login): {e}")
            })?;
            let mut env = vec![("HUMAUX_MECHANISM_FIXTURE".to_string(), DB.to_string())];
            env.extend(role_dsns_pointed_at(DB));
            Ok(env)
        }
        // The same fixture database and role DSNs as `MechanismFixture` — the CLI test calls the
        // suite's own `db()`, which asserts `HUMAUX_MECHANISM_FIXTURE` — plus the real
        // `humaux-admin` binary, which this workspace builds, so the path is something the lane
        // produces rather than a named NOT RUN waiting for somebody to export it by hand.
        Resource::MechanismAdminBin => {
            let mut env = provision(Resource::MechanismFixture)?;
            env.push((
                "HUMAUX_MECHANISM_ADMIN_BIN".to_string(),
                admin_binary()?.to_string_lossy().into_owned(),
            ));
            Ok(env)
        }
        Resource::ProvenanceMutation => {
            // A throwaway target, because this test deliberately damages source guards. Named
            // per run so two lanes never share one, and never dropped by this lane — dropping
            // databases is a destructive operation and this gate does not own that decision.
            let db = format!("humaux_thread_prov_fault_{}", unix_stamp()?);
            ensure_database(&admin, &db)?;
            let mut env = vec![(
                "HUMAUX_PUBLIC_PROVENANCE_FAULT_DB".to_string(),
                "1".to_string(),
            )];
            env.extend(role_dsns_pointed_at(&db));
            Ok(env)
        }
    }
}

// ---------------------------------------------------------------------------------------
// Warm-up (folded card-8 / card-17 host-transient debts)
// ---------------------------------------------------------------------------------------

/// Spawn `binary` once with a scrubbed environment so macOS pays its first-exec provenance
/// assessment here instead of inside a test's own deadline. Returns the live child rather than
/// waiting: the assessment is measured in *minutes* per freshly linked binary during a storm
/// (2026-09-23, this node: one `--list` sat for 2m53s), so warming a few dozen binaries one at
/// a time would cost hours and make this lane unusable as a gate. The assessments overlap
/// happily, so the caller spawns them all and then waits.
///
/// A binary that cannot be warmed is not fatal: it still gets its real run, it just keeps the
/// host risk it had before.
fn warm(binary: &Path, args: &[&str]) -> Option<std::process::Child> {
    // dep: subprocess(humaux-*) — launches the watched process by its resolved binary path
    Command::new(binary)
        .args(args)
        .env_clear()
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()
}

/// Build (never run) the test targets the lane will use, then exec each produced binary once,
/// all at the same time.
///
/// `cargo test --no-run --message-format=json` is what names the freshly linked executables;
/// guessing paths under `target/debug/deps` would pick up stale binaries from earlier builds,
/// which are exactly the ones that do *not* need warming.
///
/// Only the binaries the lane is about to *run* are warmed. Warming a package's whole test
/// suite meant paying the assessment for targets with no ignored test in them at all — work
/// that buys nothing, on the slowest step in the lane.
fn warm_up(packages: &[String], targets: &BTreeSet<String>) -> Duration {
    let start = Instant::now();
    let mut children = Vec::new();
    if let Ok(scanner) = std::env::var("HUMAUX_TEST_GITLEAKS_BIN") {
        children.extend(warm(Path::new(&scanner), &["version"]));
    }
    for package in packages {
        // dep: subprocess(cargo) — cargo build/test invocation for the serial lane
        let out = Command::new(env!("CARGO"))
            .arg("test")
            .arg("-p")
            .arg(package)
            .args(["--tests", "--no-run", "--message-format=json"])
            .output();
        let Ok(out) = out else { continue };
        for line in String::from_utf8_lossy(&out.stdout).lines() {
            // One field is needed — `"executable":"<path>"` — and cargo emits one JSON object
            // per line, so a substring read is the whole parser. Deserializing the full
            // message schema would buy nothing this uses.
            let Some(rest) = line.split("\"executable\":\"").nth(1) else {
                continue;
            };
            let Some(path) = rest.split('"').next() else {
                continue;
            };
            if path == "null" || path.is_empty() {
                continue;
            }
            // `target/debug/deps/<target>-<hash>` — the lane only runs targets it has entries
            // for, so anything else is a binary nobody is about to execute.
            let stem = Path::new(path).file_name().and_then(|n| n.to_str());
            let is_wanted = stem.is_some_and(|stem| {
                stem.rsplit_once('-')
                    .is_some_and(|(name, _hash)| targets.contains(name))
            });
            if is_wanted {
                children.extend(warm(Path::new(path), &["--list"]));
            }
        }
    }
    // Bounded wait. The point of warming is to *start* the assessment outside the timed step,
    // not to guarantee it finished: on a saturated host a single `--list` has been observed
    // blocked in `_dyld_start` with 0:00.00 CPU for 32 minutes (2026-09-23, `sample` output in
    // ADR-0047), and syspolicyd appears to serialize, so an unbounded wait makes the lane
    // unable to terminate at all. Past the budget the lane proceeds and the stragglers finish
    // on their own; the tests that need them pay whatever is left rather than the whole thing.
    let spawned = children.len();
    let budget = Duration::from_secs(
        std::env::var("HUMAUX_SERIAL_LANE_WARM_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(180),
    );
    // The budget is measured from here, not from `start`: `start` also covers the
    // `cargo test --no-run` builds above, which on a cold tree run for tens of minutes and
    // would spend the whole budget before a single child had been spawned — a bounded wait
    // that never waits is the same bug as an unbounded one, pointed the other way.
    let wait_start = Instant::now();
    let mut finished = 0usize;
    for child in &mut children {
        loop {
            match child.try_wait() {
                Ok(Some(_)) => {
                    finished += 1;
                    break;
                }
                Ok(None) if wait_start.elapsed() < budget => {
                    std::thread::sleep(Duration::from_millis(200));
                }
                // Over budget, or the child cannot be waited on: leave it running.
                _ => break,
            }
        }
    }
    eprintln!(
        "serial-lane: warm-up spawned {spawned}, {finished} finished within {}s budget",
        budget.as_secs()
    );
    start.elapsed()
}

// ---------------------------------------------------------------------------------------
// Running
// ---------------------------------------------------------------------------------------

#[derive(Debug, Default)]
struct Tally {
    passed: usize,
    failed: Vec<String>,
    not_run: Vec<String>,
}

/// Run one (package, target, resource) group serially and fold its libtest result into `tally`.
fn run_group(
    package: &str,
    target: Option<&str>,
    env: &EnvOverrides,
    tests: &[&Entry],
    tally: &mut Tally,
) {
    // dep: subprocess(cargo) — cargo build/test invocation for the serial lane
    let mut cmd = Command::new(env!("CARGO"));
    cmd.args(["test", "-p", package]);
    match target {
        Some(t) => {
            cmd.args(["--test", t]);
        }
        None => {
            cmd.arg("--lib");
        }
    }
    cmd.arg("--");
    // `--ignored` runs only the ignored set; `--exact` makes every filter below a whole-name
    // match, so one group's names can never pull in a neighbour's.
    cmd.args(["--ignored", "--exact", "--test-threads=1"]);
    for entry in tests {
        cmd.arg(&entry.test);
    }
    for (key, value) in env {
        cmd.env(key, value);
    }
    // A declared dependency that is missing must fail, not skip (§79.2 / testkit::skip_or_fail).
    cmd.env("HUMAUX_REQUIRE_DB", "1");
    let label = target.unwrap_or("<lib>");
    let output = match cmd.output() {
        Ok(o) => o,
        Err(e) => {
            for entry in tests {
                tally
                    .not_run
                    .push(format!("{label}::{} (cargo not runnable: {e})", entry.test));
            }
            return;
        }
    };
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let verdicts = libtest_verdicts(
        &text,
        &tests.iter().map(|e| e.test.as_str()).collect::<Vec<_>>(),
    );
    let mut dump = !output.status.success();
    for entry in tests {
        match verdicts.get(entry.test.as_str()) {
            Some(LibtestVerdict::Ok) => {
                tally.passed += 1;
                println!("  PASS  {package} {label}::{}", entry.test);
            }
            Some(LibtestVerdict::Failed(detail)) => {
                dump = true;
                tally
                    .failed
                    .push(format!("{package} {label}::{} ({detail})", entry.test));
                println!("  FAIL  {package} {label}::{} ({detail})", entry.test);
            }
            Some(LibtestVerdict::NoVerdict) | None => {
                dump = true;
                tally.not_run.push(format!(
                    "{package} {label}::{} (no libtest verdict — the target did not build or \
                     the binary aborted; see the output below)",
                    entry.test
                ));
                println!("  ????  {package} {label}::{} (no verdict)", entry.test);
            }
        }
    }
    // The whole output, not a tail. A group can fail eight tests at once (card 23's lane run 1
    // did), and twelve trailing lines are then libtest's summary and nothing about why any of
    // them failed — every panic message scrolled past. A log that names failures it cannot
    // explain sends the next reader back to re-run the group by hand, which is the cost this
    // lane exists to remove. Dumped on any non-PASS verdict too (card 34: a FAIL that the exit
    // status did not confirm left no evidence at all).
    if dump {
        for line in text.lines() {
            eprintln!("    | {line}");
        }
    }
}

/// What libtest said about one test of a group.
#[derive(Debug, PartialEq, Eq)]
enum LibtestVerdict {
    Ok,
    /// The first lines of the test's `---- <name> stdout ----` section (its panic message), or
    /// `FAILED` when libtest printed no section.
    Failed(String),
    /// No `test <name> ... ` line at all: the binary did not reach the test.
    NoVerdict,
}

/// libtest verdicts for `tests`, read from the libtest contract rather than from the token
/// after `test <name> ... `.
///
/// Card 34 lane runs 1–2: `distill_fairness_live` spawns resident worker subprocesses that
/// inherit stdout, so their log lines land on the libtest line between `test <name> ... ` and
/// `ok` — the old parser took the first such line as the verdict and reported a FAIL for a test
/// that passed, while `cargo test` exited 0 and nothing was dumped. The contract libtest keeps
/// regardless of interleaving: a failed test is listed by name under the trailing `failures:`
/// heading (indented), and a test that ran has a `test <name> ... ` line. So: listed → Failed;
/// ran and not listed → Ok; no line → NoVerdict.
fn libtest_verdicts<'a>(text: &str, tests: &[&'a str]) -> BTreeMap<&'a str, LibtestVerdict> {
    let lines: Vec<&str> = text.lines().collect();
    // Names under the LAST `failures:` heading (libtest prints the heading twice: once before
    // the per-test stdout sections, once before the bare name list; the list is the last one).
    let mut failed: BTreeSet<&str> = BTreeSet::new();
    if let Some(start) = lines.iter().rposition(|l| l.trim_end() == "failures:") {
        for l in &lines[start + 1..] {
            if l.starts_with("test result:") {
                break;
            }
            let name = l.trim();
            if !name.is_empty() && !name.contains(' ') {
                failed.insert(name);
            }
        }
    }
    let mut out = BTreeMap::new();
    for &test in tests {
        let needle = format!("test {test} ... ");
        let ran = lines.iter().any(|l| l.trim_start().starts_with(&needle));
        let verdict = if failed.contains(test) {
            let header = format!("---- {test} stdout ----");
            let detail = lines
                .iter()
                .position(|l| l.trim() == header)
                .map(|i| {
                    lines[i + 1..]
                        .iter()
                        .take_while(|l| !l.starts_with("---- ") && l.trim_end() != "failures:")
                        .filter(|l| !l.trim().is_empty())
                        .take(12)
                        .map(|l| l.trim())
                        .collect::<Vec<_>>()
                        .join(" / ")
                })
                .filter(|d| !d.is_empty())
                .unwrap_or_else(|| "FAILED".to_owned());
            LibtestVerdict::Failed(detail)
        } else if ran {
            LibtestVerdict::Ok
        } else {
            LibtestVerdict::NoVerdict
        };
        out.insert(test, verdict);
    }
    out
}

// ---------------------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------------------

/// `cargo xtask serial-lane [--audit-only]`.
///
/// No arguments is the gate-chain invocation: audit, provision, warm up, run serially, report.
/// `--audit-only` is the cheap half — the disposition audit with no database or cargo work —
/// for a pre-flight that does not want to pay the full lane.
#[allow(clippy::too_many_lines)] // one linear lane: audit, provision, warm up, run, report
pub fn run(args: &[String]) -> i32 {
    let root = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let audit_only = args.iter().any(|a| a == "--audit-only");
    // `--drop-provisioned` is still accepted (older gate lines pass it); dropping is now unconditional.
    let (entries, defects) = inventory(&root);

    println!(
        "serial-lane: {} ignored tests inventoried",
        entries.len() + defects.len()
    );
    if !defects.is_empty() {
        eprintln!(
            "serial-lane: fail ({} ignored test(s) with no disposition)",
            defects.len()
        );
        for d in &defects {
            eprintln!("  {}:{} — {}", d.file.display(), d.line, d.why);
        }
        return 1;
    }

    let mut by_disposition: BTreeMap<Disposition, Vec<&Entry>> = BTreeMap::new();
    for entry in &entries {
        by_disposition
            .entry(entry.disposition)
            .or_default()
            .push(entry);
    }
    for (disposition, group) in &by_disposition {
        let label = match disposition {
            Disposition::Provisioned(r) => format!("a:{}", r.label()),
            Disposition::Timing => "b".to_string(),
            Disposition::Retired => "c".to_string(),
        };
        println!("  lane({label}): {}", group.len());
    }

    let retired = by_disposition
        .get(&Disposition::Retired)
        .map_or(0, Vec::len);
    if let Some(group) = by_disposition.get(&Disposition::Retired) {
        println!("serial-lane: retired (not run, reason recorded):");
        for entry in group {
            println!(
                "  {}:{} {} — {}",
                entry.file.display(),
                entry.line,
                entry.test,
                entry.note
            );
        }
    }

    if audit_only {
        println!("serial-lane: pass (audit only — every ignored test carries a disposition)");
        return 0;
    }

    // Everything the lane will actually run, grouped by the resource it needs.
    let mut runnable: BTreeMap<Option<Resource>, Vec<&Entry>> = BTreeMap::new();
    for entry in &entries {
        match entry.disposition {
            Disposition::Provisioned(r) => runnable.entry(Some(r)).or_default().push(entry),
            Disposition::Timing => runnable.entry(None).or_default().push(entry),
            Disposition::Retired => {}
        }
    }

    let mut packages: Vec<String> = runnable
        .values()
        .flatten()
        .map(|e| e.package.clone())
        .collect();
    packages.sort();
    packages.dedup();
    // libtest binaries are named `<target>-<hash>`; a `--lib` entry's binary is named after the
    // crate with `-` replaced by `_`, which is how cargo names it in `deps/`.
    let targets: BTreeSet<String> = runnable
        .values()
        .flatten()
        .map(|e| {
            e.target
                .clone()
                .unwrap_or_else(|| e.package.replace('-', "_"))
        })
        .collect();
    let warm = warm_up(&packages, &targets);
    println!(
        "serial-lane: warm-up {:.1}s (excluded from the lane clock below)",
        warm.as_secs_f64()
    );

    let _drop_on_exit = DropOnExit;
    let start = Instant::now();
    let mut tally = Tally::default();
    for (resource, group) in &runnable {
        let mut by_target: BTreeMap<(String, Option<String>), Vec<&Entry>> = BTreeMap::new();
        for entry in group.iter().copied() {
            by_target
                .entry((entry.package.clone(), entry.target.clone()))
                .or_default()
                .push(entry);
        }
        // `disposable` is provisioned once per *target*; every other resource once per group.
        // Its oracles are the ones that scan a global table — every unfinished anonymous
        // dispatch, every reasoning profile in the database — so one database shared with the
        // next target in the same group is the residue this disposition exists to escape:
        // `phase9_independence_attestation` deliberately strands dispatches, and
        // `reasoning_route_shadow_bootstrap` then counts every domain `public_runtime`'s
        // fixtures created. Per-resource isolation is not isolation for a global scan.
        let per_target = matches!(resource, Some(Resource::Disposable));
        let mut shared: Option<Result<EnvOverrides, String>> = None;
        for ((package, target), tests) in &by_target {
            let provisioned = match resource {
                // lane(b) needs no resource beyond the warm-up that just happened.
                None => Ok(Vec::new()),
                Some(r) if per_target => provision(*r),
                Some(r) => shared
                    .get_or_insert_with(|| provision(*r))
                    .as_ref()
                    .map(Clone::clone)
                    .map_err(Clone::clone),
            };
            let env = match provisioned {
                Ok(env) => env,
                Err(why) => {
                    let label = resource.map_or("<none>", Resource::label);
                    eprintln!("serial-lane: cannot provision {label}: {why}");
                    for entry in tests {
                        tally.not_run.push(format!(
                            "{}::{} ({why})",
                            entry.file.display(),
                            entry.test
                        ));
                    }
                    continue;
                }
            };
            run_group(package, target.as_deref(), &env, tests, &mut tally);
            if per_target {
                drop_provisioned();
            }
        }
        // The group's databases are done with once its last target ran (the next group provisions its own).
        drop_provisioned();
    }
    let elapsed = start.elapsed();

    let n = tally.passed + tally.failed.len() + tally.not_run.len();
    println!(
        "serial-lane: n={n} run-set, passed={}, failed={}, not_run={}, retired={retired} \
         (inventory={}), lane clock {:.1}s",
        tally.passed,
        tally.failed.len(),
        tally.not_run.len(),
        entries.len(),
        elapsed.as_secs_f64()
    );
    for line in &tally.failed {
        eprintln!("  FAIL     {line}");
    }
    for line in &tally.not_run {
        eprintln!("  NOT RUN  {line}");
    }
    i32::from(!tally.failed.is_empty() || !tally.not_run.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ADR-0063 D-M: a shared database whose ledger lacks the migration files is refused naming them, and nothing is
    /// applied to it (the throwaway keeps its one ledger table); with every file recorded it passes. Fault: run
    /// `crate::migrate::run` before the check (what the old SharedDb / Post0132 arms did) ⇒ 0001.. are applied to the
    /// throwaway ⇒ red.
    #[test]
    fn a_shared_db_missing_a_migration_is_refused_and_never_migrated() {
        let Some((_db, dsn)) = crate::migrate::tests::throwaway(
            "a_shared_db_missing_a_migration_is_refused_and_never_migrated",
            "lane_head",
        ) else {
            return;
        };
        // dep: PostgreSQL(any) — superuser on the throwaway: the real ledger shape, no migration body
        let mut c = postgres::Client::connect(&dsn, postgres::NoTls).expect("throwaway connect");
        c.batch_execute(
            "CREATE SCHEMA ops; CREATE TABLE ops.schema_migrations (migration_id text PRIMARY KEY, \
             checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())",
        )
        .expect("bare ledger");
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../migrations");
        let mut stems: Vec<String> = std::fs::read_dir(&dir)
            .expect("migrations dir")
            .map(|e| e.expect("entry").path())
            .filter(|p| p.extension().is_some_and(|x| x == "sql"))
            .map(|p| p.file_stem().expect("stem").to_string_lossy().into_owned())
            .collect();
        stems.sort();
        let refused = shared_db_at_head(&dsn).expect_err("pending files are refused");
        assert!(
            refused.contains(&stems[0])
                && refused.contains(&stems[stems.len() - 1])
                && refused.contains("never migrates"),
            "{refused}"
        );
        let tables: i64 = c
            .query_one(
                "SELECT count(*) FROM pg_class WHERE relkind IN ('r', 'p') AND relpersistence = 'p' \
                   AND relnamespace NOT IN ('pg_catalog'::regnamespace, 'information_schema'::regnamespace)",
                &[],
            )
            .expect("tables")
            .get(0);
        assert_eq!(
            tables, 1,
            "nothing was applied: only the ledger table exists"
        );
        for stem in &stems {
            c.execute(
                "INSERT INTO ops.schema_migrations (migration_id, checksum) VALUES ($1, 'x')",
                &[stem],
            )
            .expect("record");
        }
        shared_db_at_head(&dsn).expect("at head");
    }

    /// Owner disk constraint (2026-10-05): a database the lane created is gone once [`drop_provisioned`] ran for its
    /// group, and a protected name recorded by mistake is never dropped. Fault: return before the DROP loop (the old
    /// opt-in behaviour) ⇒ the probe database survives ⇒ red.
    #[test]
    fn a_provisioned_database_is_gone_after_its_group_and_dev_never_is() {
        let test = "a_provisioned_database_is_gone_after_its_group_and_dev_never_is";
        let Ok(admin) = std::env::var("HUMAUX_TEST_PG_DSN") else {
            humaux_testkit::skip_or_fail(
                test,
                "missing object: HUMAUX_TEST_PG_DSN",
                humaux_testkit::ExternalDep::Postgres,
            );
            return;
        };
        let postgres_dsn = dsn_with_database(&admin, "postgres").expect("URL DSN");
        // dep: PostgreSQL(any) — superuser on the `postgres` database: CREATE / pg_database reads
        let mut client =
            postgres::Client::connect(&postgres_dsn, postgres::NoTls).expect("admin connect");
        let probe = format!("humaux_thread_lane_drop_probe_{}", std::process::id());
        let exists = |client: &mut postgres::Client, db: &str| -> bool {
            client
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)",
                    &[&db],
                )
                .expect("pg_database")
                .get(0)
        };
        create_if_absent(&mut client, &probe).expect("provision the probe");
        assert!(exists(&mut client, &probe), "provisioned");
        let dev_present = exists(&mut client, "humaux_thread_dev");
        PROVISIONED_THIS_RUN
            .lock()
            .expect("list")
            .push("humaux_thread_dev".to_owned());
        assert_eq!(drop_provisioned(), 1, "the probe only");
        assert!(!exists(&mut client, &probe), "dropped when its group ended");
        assert_eq!(
            exists(&mut client, "humaux_thread_dev"),
            dev_present,
            "dev never dropped"
        );
        assert!(
            PROVISIONED_THIS_RUN.lock().expect("list").is_empty(),
            "forgotten once dropped"
        );
    }

    fn repo_root() -> PathBuf {
        // `xtask`'s manifest dir is `<root>/xtask`.
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xtask lives under the repo root")
            .to_path_buf()
    }

    /// The gate itself: every `#[ignore]` in the tree carries a disposition.
    ///
    /// Fault injection: strip the `lane(...)` prefix from any one ignore reason and this goes
    /// red naming that file and line — which is the whole point, since the disposition lives
    /// next to the test rather than in a table here.
    #[test]
    fn every_ignored_test_has_a_disposition() {
        let (entries, defects) = inventory(&repo_root());
        assert!(
            defects.is_empty(),
            "ignored tests without a disposition:\n{}",
            defects
                .iter()
                .map(|d| format!("  {}:{} — {}", d.file.display(), d.line, d.why))
                .collect::<Vec<_>>()
                .join("\n")
        );
        assert!(
            entries.len() >= 100,
            "the deployment report counted ~108 ignored tests; the walk found {} — a collapse \
             this large means the walk stopped seeing files, not that the tests were fixed",
            entries.len()
        );
    }

    /// A retired disposition must carry its written reason; an empty one is the silent ignore
    /// this gate exists to stop.
    #[test]
    fn retired_dispositions_carry_a_written_reason() {
        let (entries, _) = inventory(&repo_root());
        for entry in entries
            .iter()
            .filter(|e| e.disposition == Disposition::Retired)
        {
            assert!(
                entry.note.len() > 20,
                "{}:{} {} is retired with only {:?} — write why",
                entry.file.display(),
                entry.line,
                entry.test,
                entry.note
            );
        }
    }

    #[test]
    fn disposition_grammar_is_closed() {
        assert_eq!(
            Disposition::parse("a:shared_db"),
            Some(Disposition::Provisioned(Resource::SharedDb))
        );
        assert_eq!(Disposition::parse("b"), Some(Disposition::Timing));
        assert_eq!(Disposition::parse("c"), Some(Disposition::Retired));
        assert_eq!(Disposition::parse("a:no_such_resource"), None);
        assert_eq!(
            Disposition::parse("a:pre_0132"),
            Some(Disposition::Provisioned(Resource::Pre0132))
        );
        assert_eq!(Disposition::parse("d"), None);
        assert_eq!(Disposition::parse(""), None);
    }

    #[test]
    fn dsn_database_swap_keeps_the_authority() {
        assert_eq!(
            dsn_with_database("postgres://u:p@127.0.0.1:54329/humaux_thread_dev", "other")
                .as_deref(),
            Some("postgres://u:p@127.0.0.1:54329/other")
        );
        assert_eq!(dsn_with_database("not-a-dsn", "x"), None);
    }

    /// Doc-comment prose about `#[ignore]` is not an attribute. Three files in the tree say
    /// the words; none of them is an ignored test, and demanding a disposition for a sentence
    /// would make the audit unfixable.
    #[test]
    fn prose_about_ignore_is_not_an_attribute() {
        let (_, defects) = inventory(&repo_root());
        assert!(
            !defects
                .iter()
                .any(|d| d.file.ends_with("feature_registry_contract.rs")),
            "a doc comment was read as an `#[ignore]` attribute"
        );
    }

    /// Card 34 lane bug: a resident worker subprocess writes on the libtest line between
    /// `test <name> ... ` and `ok`. The verdict comes from the libtest contract, not from the
    /// token after the ellipsis. Fault: take the token after `... ` as the verdict → this test
    /// reports Failed("humaux-private-worker: …").
    #[test]
    fn interleaved_child_output_on_the_libtest_line_is_still_a_pass() {
        let text = "running 2 tests\n\
test distill_poison_live ... ok\n\
test distill_fairness_live ... humaux-private-worker: distill dispatch claimed=24 completed=24 \
failed=0 stopped=-\nok\n\n\
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 46 filtered out; finished in 1061.27s\n";
        let v = libtest_verdicts(
            text,
            &["distill_poison_live", "distill_fairness_live", "absent_one"],
        );
        assert_eq!(v["distill_poison_live"], LibtestVerdict::Ok);
        assert_eq!(v["distill_fairness_live"], LibtestVerdict::Ok);
        assert_eq!(v["absent_one"], LibtestVerdict::NoVerdict);
    }

    /// A real failure is read from the trailing `failures:` name list, with the panic line from
    /// the test's stdout section as detail. Fault: drop the `failures:` scan → Ok.
    #[test]
    fn a_listed_failure_is_failed_with_its_panic_line() {
        let text = "running 2 tests\n\
test a_pass ... ok\n\
test a_fail ... FAILED\n\n\
failures:\n\n\
---- a_fail stdout ----\n\n\
thread 'a_fail' panicked at bins/x/tests/y.rs:10:5:\n\
assertion `left == right` failed\n  left: 1\n right: 2\n\n\n\
failures:\n    a_fail\n\n\
test result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n";
        let v = libtest_verdicts(text, &["a_pass", "a_fail"]);
        assert_eq!(v["a_pass"], LibtestVerdict::Ok);
        match &v["a_fail"] {
            LibtestVerdict::Failed(d) => {
                assert!(d.contains("panicked at bins/x/tests/y.rs:10:5"), "{d}");
                assert!(d.contains("left: 1"), "{d}");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }
}
