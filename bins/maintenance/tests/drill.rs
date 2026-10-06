//! `maintenance::tests::drill` — the card-37 S5 restore drill and real restore (ADR-0064 D-L, D-M, D-N, D-O, D-U as
//!   amended by section 10.4 S5 and 10.11 B, H, I): the step-0 refusals (owner DSN, no verified set, a stale drill,
//!   a writable repository mount, the memory/disk floor), the label-scoped destroy, the receipt written after
//!   destroy, an evidence file without secrets, and the ignored live legs (end to end, a corrupt WAL segment, the
//!   injected catalog faults, `restore pitr`); T-L1's step 5 is one DR_EVIDENCE pass of the daemon (card 37 S6,
//!   ADR-0064 D-K). Each test names the fault that turns it red.
//! Depends-on: crates=[async-trait, humaux-adapters, humaux-infra-cell, humaux-local-secret-scan, humaux-projection,
//!   humaux-retrieval, humaux-retrieval-provider, postgres, serde_json, sqlx, uuid]; services=[subprocess(docker),
//!   subprocess(humaux-maintenance), PostgreSQL(owner) r=[ops.restore_drills, private.memory_records,
//!   projection.memory_vectors, projection.stream_log] w=[ops.backup_receipts, ops.backup_sets, ops.outbox,
//!   ops.schema_migrations, private.evidence_objects, projection.stream_checkpoints, projection.tenant_placements],
//!   PostgreSQL(role_private_worker), Qdrant(*), HTTP(loopback)]; env=[CARGO_BIN_EXE_humaux-maintenance, HOME, HUMAUX_C37_EVIDENCE,
//!   HUMAUX_DRILL_CPUS, HUMAUX_DRILL_PG_MEM_LIMIT, HUMAUX_DRILL_QDRANT_MEM_LIMIT,
//!   HUMAUX_MAINTENANCE_BACKUP_COMPOSE_FILE, HUMAUX_MAINTENANCE_BACKUP_PROJECT, HUMAUX_MAINTENANCE_DRILL_COMPOSE_FILE,
//!   HUMAUX_MAINTENANCE_DRILL_MIN_FREE_MEMORY_BYTES, HUMAUX_MAINTENANCE_DRILL_RESTORE_TIMEOUT_SECONDS,
//!   HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_MAINTENANCE_QDRANT_CIDR, HUMAUX_MAINTENANCE_QDRANT_HOST,
//!   HUMAUX_MAINTENANCE_QDRANT_PORT, HUMAUX_MAINTENANCE_RESTORE_MIN_FREE_DISK_BYTES, HUMAUX_MIGRATOR_PG_DSN,
//!   HUMAUX_PG_ARCHIVE_TIMEOUT_SECONDS, HUMAUX_PG_CONTAINER, HUMAUX_PG_IMAGE, HUMAUX_PG_LISTEN_ADDRESSES,
//!   HUMAUX_PG_PORT, HUMAUX_PG_REPO_DIR, HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN, HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256,
//!   HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION, HUMAUX_TEST_GITLEAKS_BIN, HUMAUX_TEST_GITLEAKS_SHA256,
//!   HUMAUX_TEST_GITLEAKS_VERSION, PATH];
//!   modules=[adapters::private_projection_registry, adapters::tests::support::a2_fixture,
//!   adapters::tests::support::governance_ops, adapters::tests::support::scratch_qdrant,
//!   humaux-infra-cell, humaux-local-secret-scan, humaux-projection, humaux-retrieval, humaux-retrieval-provider,
//!   maintenance::tests::support::containers, maintenance::tests::support::throwaway]
//! Called-by: [cargo-test]
//! Invariants: [the backed-up cluster is a scratch project humaux-c37-<purpose>-<pid>-<n> from deploy/compose/backup.yml
//!   (TCP on loopback, migrated from the files), receipts without a source go to a throwaway database
//!   humaux_thread_c37_dr_<pid>_<n> on the dev cluster; every drill project is humaux-drill-<uuid> and is destroyed by
//!   the drill itself; every container, volume and project a test creates is removed by its guard on every path;
//!   tests that create or observe `humaux.drill` resources hold DRILLS (one at a time per process); the shared
//!   humaux-thread-pg / humaux-thread-qdrant are never named; every secret is a throwaway value generated here]
//! Spec: Baseline §44; §79.2; ADR-0064 D-L; ADR-0064 D-M; ADR-0064 D-N; ADR-0064 D-O; ADR-0064 D-Q; ADR-0064 D-U

#[path = "support/throwaway.rs"]
#[allow(dead_code)]
mod throwaway;

#[path = "support/containers.rs"]
#[allow(dead_code)]
mod containers;

#[path = "../../../crates/adapters/tests/support/a2_fixture.rs"]
mod a2_fixture;
#[path = "../../../crates/adapters/tests/support/governance_ops.rs"]
#[allow(dead_code)]
mod governance_ops;
#[path = "../../../crates/adapters/tests/support/scratch_qdrant.rs"]
#[allow(dead_code)]
mod scratch_qdrant;

// The #[path]-included a2 fixture's crates: dep-map attributes an included file to the package it lives in
// (adapters), so this binary names them once here.
use async_trait as _;
use humaux_infra_cell as _;
use humaux_local_secret_scan as _;
use humaux_projection as _;
use humaux_retrieval as _;
use humaux_retrieval_provider as _;
use sqlx as _;

use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

use a2_fixture::{Handle, TENANT_SHARED};
use containers::{IMAGE, Source, both, docker, root};
use humaux_adapters::private_projection_registry::{
    bind_embedding_fingerprint, worker_fingerprint_inputs,
};
use serde_json::Value;
use uuid::Uuid;

/// Tests that create or observe `humaux.drill` resources run one at a time (step 0 refuses on ANY drill resource).
static DRILLS: Mutex<()> = Mutex::new(());

fn drills() -> MutexGuard<'static, ()> {
    DRILLS.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The arm's one JSON receipt (the last stdout line).
fn receipt(out: &Output) -> Value {
    let stdout = String::from_utf8_lossy(&out.stdout);
    let last = stdout.lines().last().unwrap_or("");
    serde_json::from_str(last).unwrap_or_else(|e| panic!("receipt json ({e}): {}", both(out)))
}

fn code(out: &Output) -> i32 {
    out.status.code().unwrap_or(-1)
}

fn random_hex() -> String {
    format!("{}", Uuid::new_v4().simple())
}

/// Every resource carrying the `humaux.drill` label (any value).
fn drill_resources() -> Vec<String> {
    // dep: subprocess(docker) — list labelled drill resources (read-only)
    let mut all = Vec::new();
    for args in [
        ["ps", "-a", "--format", "{{.Names}}"],
        ["volume", "ls", "--format", "{{.Name}}"],
        ["network", "ls", "--format", "{{.Name}}"],
    ] {
        let mut a: Vec<&str> = args.to_vec();
        a.extend_from_slice(&["--filter", "label=humaux.drill"]);
        all.extend(
            docker(&a)
                .unwrap_or_default()
                .lines()
                .filter(|l| !l.is_empty())
                .map(str::to_owned),
        );
    }
    all
}

/// One `humaux-maintenance` run with exactly `env` (+ PATH and HOME for the docker CLI).
fn maintenance(args: &[&str], env: &[(String, String)]) -> Output {
    let mut all: Vec<(&str, String)> = env.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
    for key in ["PATH", "HOME"] {
        if let Ok(v) = std::env::var(key) {
            all.push((key, v));
        }
    }
    // dep: subprocess(humaux-maintenance) — one restore arm run
    throwaway::run(args, &all)
}

/// The drill's environment: `dsn` is the source's role_maintenance DSN; `repo_dir` the repository mount point.
fn drill_env(
    dsn: &str,
    repo_dir: &Path,
    compose: &Path,
    source: Option<&Source>,
) -> Vec<(String, String)> {
    let mut env: Vec<(String, String)> = [
        ("HUMAUX_MAINTENANCE_PG_DSN", dsn.to_owned()),
        (
            "HUMAUX_MAINTENANCE_DRILL_COMPOSE_FILE",
            compose.to_string_lossy().into_owned(),
        ),
        (
            "HUMAUX_MAINTENANCE_DRILL_MIN_FREE_MEMORY_BYTES",
            "1".to_owned(),
        ),
        (
            "HUMAUX_MAINTENANCE_DRILL_RESTORE_TIMEOUT_SECONDS",
            "240".to_owned(),
        ),
        (
            "HUMAUX_MAINTENANCE_RESTORE_MIN_FREE_DISK_BYTES",
            (1_u64 << 20).to_string(),
        ),
        ("HUMAUX_PG_IMAGE", IMAGE.to_owned()),
        (
            "HUMAUX_PG_REPO_DIR",
            repo_dir.to_string_lossy().into_owned(),
        ),
        ("HUMAUX_DRILL_PG_MEM_LIMIT", "512m".to_owned()),
        ("HUMAUX_DRILL_QDRANT_MEM_LIMIT", "512m".to_owned()),
        ("HUMAUX_DRILL_CPUS", "1".to_owned()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v))
    .collect();
    for (from, to) in [
        (
            "HUMAUX_TEST_GITLEAKS_BIN",
            "HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN",
        ),
        (
            "HUMAUX_TEST_GITLEAKS_VERSION",
            "HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION",
        ),
        (
            "HUMAUX_TEST_GITLEAKS_SHA256",
            "HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256",
        ),
    ] {
        if let Ok(v) = std::env::var(from) {
            env.push((to.to_owned(), v));
        }
    }
    match source {
        Some(s) => {
            env.push((
                "HUMAUX_MAINTENANCE_BACKUP_COMPOSE_FILE".to_owned(),
                containers::backup_yml().to_string_lossy().into_owned(),
            ));
            env.push((
                "HUMAUX_MAINTENANCE_BACKUP_PROJECT".to_owned(),
                s.name.clone(),
            ));
            env.push((
                "PGBACKREST_REPO1_CIPHER_PASS".to_owned(),
                s.env_value("PGBACKREST_REPO1_CIPHER_PASS"),
            ));
        }
        None => env.push(("PGBACKREST_REPO1_CIPHER_PASS".to_owned(), random_hex())),
    }
    env
}

fn set_env(env: &mut Vec<(String, String)>, key: &str, value: String) {
    env.retain(|(k, _)| k != key);
    env.push((key.to_owned(), value));
}

/// `deploy/compose/drill.yml`, absolute (the shipped file).
fn drill_yml() -> PathBuf {
    root().join("deploy/compose/drill.yml")
}

/// A directory the test owns; removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "humaux-c37-{name}-{}-{}",
            std::process::id(),
            random_hex()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.0) {
            eprintln!("drill test cleanup: rm -rf {}: {e}", self.0.display());
        }
    }
}

/// A throwaway receipts database holding one forged VERIFIED set (the table derives VERIFIED from these columns;
/// the refusal tests need a verified set to get past step 0's second refusal, not a repository).
fn receipts_with_a_verified_set(test: &str) -> Option<throwaway::Db> {
    let mut db = throwaway::db(test, "c37_dr")?;
    db.sql(
        "INSERT INTO ops.backup_sets (backup_label, manifest_sha256) VALUES ('20261006-000000F', sha256('m'::bytea)); \
         INSERT INTO ops.backup_receipts (backup_label, backup_type, backup_started_at, backup_stopped_at, \
           manifest_sha256, verify_exit, verified_manifest_sha256, outcome) \
         VALUES ('20261006-000000F', 'full', now() - interval '1 hour', now() - interval '50 minutes', \
           sha256('m'::bytea), 0, sha256('m'::bytea), 'VERIFIED')",
    );
    Some(db)
}

/// Removes the named containers and volumes on drop (decoys, stale resources).
struct Leftovers {
    containers: Vec<String>,
    volumes: Vec<String>,
}

impl Drop for Leftovers {
    fn drop(&mut self) {
        for c in &self.containers {
            if let Err(e) = docker(&["rm", "-f", "-v", c])
                && !e.contains("No such container")
            {
                eprintln!("drill test cleanup: {e}");
            }
        }
        for v in &self.volumes {
            if let Err(e) = docker(&["volume", "rm", "-f", v]) {
                eprintln!("drill test cleanup: {e}");
            }
        }
    }
}

// ============================================================================
// No-source refusals
// ============================================================================

/// T-M4 (D-L step 0): `HUMAUX_MIGRATOR_PG_DSN` in the environment is refused (exit 3) before anything is read or
/// created. Fault: delete the check → the drill goes on to connect and to Docker → not exit 3 → red.
#[test]
fn drill_refuses_with_the_migrator_dsn_in_env() {
    let dir = TempDir::new("tm4");
    let mut env = drill_env(
        "postgres://role_maintenance:x@127.0.0.1:1/none",
        &dir.0,
        &drill_yml(),
        None,
    );
    set_env(
        &mut env,
        "HUMAUX_MIGRATOR_PG_DSN",
        "postgres://owner@127.0.0.1:1/none".to_owned(),
    );
    let evidence = dir.0.join("e.json");
    let out = maintenance(
        &[
            "restore",
            "drill",
            "--evidence",
            &evidence.to_string_lossy(),
        ],
        &env,
    );
    assert_eq!(code(&out), 3, "{}", both(&out));
    let r = receipt(&out);
    assert!(
        r["reason"]
            .as_str()
            .is_some_and(|s| s.starts_with("migrator_dsn_present")),
        "{r}"
    );
    assert_eq!(r["repo_class"], "local_only", "{r}");
    assert!(!evidence.exists(), "nothing written");
}

/// T-M4b (review finding: crontab evidence directory): `restore drill` and `restore pitr` refuse an `--evidence`
/// path whose directory does not exist (exit 2) before anything is read, created or recorded, because the evidence
/// is written after the receipt. Fault: drop the check → both go on to connect → not exit 2 → red.
#[test]
fn drill_and_pitr_refuse_an_evidence_path_whose_directory_is_missing() {
    let dir = TempDir::new("tm4b");
    let env = drill_env(
        "postgres://role_maintenance:x@127.0.0.1:1/none",
        &dir.0,
        &drill_yml(),
        None,
    );
    let missing = dir.0.join("missing");
    let evidence = missing.join("e.json");
    let evidence = evidence.to_string_lossy();
    let compose = drill_yml();
    let compose = compose.to_string_lossy();
    let named = format!("directory {} does not exist", missing.display());
    for args in [
        vec!["restore", "drill", "--evidence", &evidence],
        vec![
            "restore",
            "pitr",
            "--compose-file",
            &compose,
            "--project",
            "humaux-c37-tm4b",
            "--target",
            "end",
            "--evidence",
            &evidence,
        ],
    ] {
        let out = maintenance(&args, &env);
        assert_eq!(code(&out), 2, "{args:?}: {}", both(&out));
        assert!(both(&out).contains(&named), "{args:?}: {}", both(&out));
    }
    assert!(!missing.exists(), "nothing created");
}

/// T-M3 (D-L step 0/1): with no VERIFIED receipt the drill refuses `no_verified_backup` (exit 3) and creates
/// nothing, even when a FAILED or a later-unverified set exists. Fault: fall back to the newest unverified set →
/// the drill goes on → not exit 3 → red.
#[test]
fn drill_refuses_without_a_verified_backup() {
    let Some(mut db) = throwaway::db("drill_refuses_without_a_verified_backup", "c37_dr") else {
        return;
    };
    // A set verified once, then FAILED: its LATEST receipt is not VERIFIED (D-K's rule), plus a refusal row.
    db.sql(
        "INSERT INTO ops.backup_sets (backup_label, manifest_sha256) VALUES ('20261006-000000F', sha256('m'::bytea)); \
         INSERT INTO ops.backup_receipts (backup_label, backup_type, backup_started_at, backup_stopped_at, \
           manifest_sha256, verify_exit, verified_manifest_sha256, outcome, recorded_at) \
         VALUES ('20261006-000000F', 'full', now() - interval '2 hours', now() - interval '110 minutes', \
           sha256('m'::bytea), 0, sha256('m'::bytea), 'VERIFIED', now() - interval '100 minutes'); \
         INSERT INTO ops.backup_receipts (backup_label, backup_type, backup_started_at, backup_stopped_at, \
           manifest_sha256, verify_exit, outcome, failure) \
         VALUES ('20261006-000000F', 'full', now() - interval '2 hours', now() - interval '110 minutes', \
           sha256('m'::bytea), 0, 'FAILED', 'verify_invalid'); \
         INSERT INTO ops.backup_receipts (backup_type, outcome, failure) VALUES ('full', 'FAILED', 'budget_free_floor:1')",
    );
    let dir = TempDir::new("tm3");
    let env = drill_env(&db.maintenance_dsn, &dir.0, &drill_yml(), None);
    let evidence = dir.0.join("e.json");
    let out = maintenance(
        &[
            "restore",
            "drill",
            "--evidence",
            &evidence.to_string_lossy(),
        ],
        &env,
    );
    assert_eq!(code(&out), 3, "{}", both(&out));
    assert_eq!(receipt(&out)["reason"], "no_verified_backup");
    let drills: i64 = db
        .client()
        .query_one("SELECT count(*) FROM ops.restore_drills", &[])
        .expect("count")
        .get(0);
    assert_eq!(drills, 0, "no receipt for a refusal");
}

/// T-M1' (D-M, 10.4 S5): a repository bind mounted read-write (a compose file with `read_only: false`) is refused
/// `drill_repo_writable` (exit 3) before any project exists, and the probe leaves no file behind. Fault: remove the
/// probe → the drill goes on past step 0 → not exit 3 → red.
#[test]
fn drill_refuses_a_writable_repo_mount() {
    let test = "drill_refuses_a_writable_repo_mount";
    let Some(db) = receipts_with_a_verified_set(test) else {
        return;
    };
    if !containers::docker_ready(test) {
        return;
    }
    let _one = drills();
    let dir = TempDir::new("tm1");
    let repo_dir = dir.0.join("mnt");
    std::fs::create_dir_all(repo_dir.join("repo")).expect("repo dir");
    let shipped = std::fs::read_to_string(drill_yml()).expect("drill.yml");
    let conf = root().join("deploy/pgbackrest/pgbackrest.conf");
    let writable = shipped
        .replace(
            "source: ../pgbackrest/pgbackrest.conf",
            &format!("source: {}", conf.display()),
        )
        .replace(
            "        target: /var/lib/pgbackrest\n        read_only: true",
            "        target: /var/lib/pgbackrest\n        read_only: false",
        );
    assert_ne!(
        writable, shipped,
        "the override changed the repository bind"
    );
    let compose = dir.0.join("drill-rw.yml");
    std::fs::write(&compose, writable).expect("override");
    let env = drill_env(&db.maintenance_dsn, &repo_dir, &compose, None);
    let out = maintenance(
        &[
            "restore",
            "drill",
            "--evidence",
            &dir.0.join("e.json").to_string_lossy(),
        ],
        &env,
    );
    assert_eq!(code(&out), 3, "{}", both(&out));
    assert!(
        receipt(&out)["reason"]
            .as_str()
            .is_some_and(|r| r.starts_with("drill_repo_writable")),
        "{}",
        both(&out)
    );
    assert!(drill_resources().is_empty(), "no project exists");
    let left: Vec<_> = std::fs::read_dir(repo_dir.join("repo"))
        .expect("repo dir")
        .collect();
    assert!(left.is_empty(), "the probe file was removed");
}

/// T-M6 (D-L step 0, D-O): any `humaux.drill` resource blocks the next drill (exit 3, named, with the
/// `--destroy-stale` hint); `--destroy-stale` removes exactly the labelled resources, an unlabelled decoy survives,
/// and the repository's info files are byte-identical afterwards (10.11 I). Fault: step 0 checks only the drill's
/// own id → the stale volume is not seen → not exit 3 → red.
#[test]
fn a_leftover_drill_resource_blocks_the_next_drill() {
    let test = "a_leftover_drill_resource_blocks_the_next_drill";
    let Some(db) = receipts_with_a_verified_set(test) else {
        return;
    };
    if !containers::docker_ready(test) {
        return;
    }
    let _one = drills();
    let pid = std::process::id();
    let stale = format!("humaux-c37-stale-{pid}");
    let decoy = format!("humaux-c37-stale-decoy-{pid}");
    let _left = Leftovers {
        containers: Vec::new(),
        volumes: vec![stale.clone(), decoy.clone()],
    };
    docker(&[
        "volume",
        "create",
        "--label",
        &format!("humaux.drill=stale{pid}"),
        &stale,
    ])
    .expect("stale");
    docker(&[
        "volume",
        "create",
        "--label",
        &format!("humaux.c37={pid}"),
        &decoy,
    ])
    .expect("decoy");
    let dir = TempDir::new("tm6");
    let repo_dir = dir.0.join("mnt");
    for f in [
        "repo/backup/humaux/backup.info",
        "repo/archive/humaux/archive.info",
    ] {
        let p = repo_dir.join(f);
        std::fs::create_dir_all(p.parent().expect("parent")).expect("dirs");
        std::fs::write(&p, format!("{f} {}", random_hex())).expect("info file");
    }
    let info = |f: &str| std::fs::read(repo_dir.join(f)).expect("info");
    let before = (
        info("repo/backup/humaux/backup.info"),
        info("repo/archive/humaux/archive.info"),
    );
    let env = drill_env(&db.maintenance_dsn, &repo_dir, &drill_yml(), None);
    let out = maintenance(
        &[
            "restore",
            "drill",
            "--evidence",
            &dir.0.join("e.json").to_string_lossy(),
        ],
        &env,
    );
    assert_eq!(code(&out), 3, "{}", both(&out));
    let reason = receipt(&out)["reason"].as_str().unwrap_or("").to_owned();
    assert!(reason.starts_with("stale_drill"), "{reason}");
    assert!(reason.contains(&stale), "names the resource: {reason}");
    assert!(reason.contains("--destroy-stale"), "{reason}");

    let out = maintenance(&["restore", "drill", "--destroy-stale"], &env);
    assert_eq!(code(&out), 0, "{}", both(&out));
    assert!(
        docker(&["volume", "inspect", &stale]).is_err(),
        "the labelled volume is gone"
    );
    assert!(
        docker(&["volume", "inspect", &decoy]).is_ok(),
        "the unlabelled decoy survives"
    );
    assert!(drill_resources().is_empty());
    assert_eq!(
        before,
        (
            info("repo/backup/humaux/backup.info"),
            info("repo/archive/humaux/archive.info")
        ),
        "repo_intact after --destroy-stale"
    );
}

// ============================================================================
// A live scratch source (TCP, migrated) and the shared drill run
// ============================================================================

/// The scratch source as the drill sees it: migrated database `humaux_thread`, role_maintenance with a throwaway
/// password, the stanza created, one VERIFIED backup taken by the real binary.
struct Live {
    /// The repository (a host directory) and the cluster.
    source: Source,
    /// role_maintenance on the source (the drill's `HUMAUX_MAINTENANCE_PG_DSN`).
    dsn: String,
    /// The superuser DSN (fixtures and assertions).
    owner: String,
    /// Every throwaway secret this source uses (T-O3 greps the evidence for each).
    secrets: Vec<String>,
}

const LIVE_DB: &str = "humaux_thread";

fn live(test: &str, purpose: &str) -> Option<Live> {
    let mut source = Source::prepare(test, purpose)?;
    source.set_env("HUMAUX_PG_LISTEN_ADDRESSES", "*");
    source.restart("");
    source.psql(&format!("CREATE DATABASE {LIVE_DB}"));
    let owner = source.owner_dsn(LIVE_DB);
    // dep: PostgreSQL(owner) — migrate the scratch cluster's database (never the dev cluster)
    let mut client = postgres::Client::connect(&owner, postgres::NoTls).expect("scratch owner");
    throwaway::migrate_all(&mut client);
    let mut secrets = vec![
        source.env_value("POSTGRES_PASSWORD"),
        source.env_value("PGBACKREST_REPO1_CIPHER_PASS"),
    ];
    for role in [
        "role_maintenance",
        "role_gateway",
        "role_retrieval_worker",
        "role_private_worker",
    ] {
        let pw = random_hex();
        client
            .batch_execute(&format!("ALTER ROLE {role} PASSWORD '{pw}'"))
            .expect("role password");
        secrets.push(pw);
    }
    let dsn = format!(
        "postgres://role_maintenance:{}@127.0.0.1:{}/{LIVE_DB}",
        secrets[2],
        source.env_value("HUMAUX_PG_PORT")
    );
    source.stanza_create();
    Some(Live {
        source,
        dsn,
        owner,
        secrets,
    })
}

impl Live {
    /// `backup run` through the real binary; asserted VERIFIED.
    fn backup(&self) {
        let out = self.source.maintenance(&self.dsn, &["backup", "run"], &[]);
        assert_eq!(code(&out), 0, "{}", both(&out));
        assert_eq!(receipt(&out)["outcome"], "VERIFIED");
    }

    fn client(&self) -> postgres::Client {
        // dep: PostgreSQL(owner) — assertions on the scratch source
        postgres::Client::connect(&self.owner, postgres::NoTls).expect("scratch owner")
    }

    fn env(&self) -> Vec<(String, String)> {
        let mut env = drill_env(
            &self.dsn,
            &self.source.repo_dir,
            &drill_yml(),
            Some(&self.source),
        );
        secrets_out(&mut env);
        env
    }

    /// One drill; `(output, evidence json, evidence text)`.
    fn drill(
        &self,
        id: Uuid,
        env: &[(String, String)],
        evidence: &Path,
    ) -> (Output, Value, String) {
        let out = maintenance(
            &[
                "restore",
                "drill",
                "--evidence",
                &evidence.to_string_lossy(),
                "--drill-id",
                &id.to_string(),
            ],
            env,
        );
        let text = std::fs::read_to_string(evidence).unwrap_or_default();
        let json = serde_json::from_str(&text).unwrap_or(Value::Null);
        (out, json, text)
    }

    /// `(succeeded, residue, repo_intact, failure)` of drill `id`'s receipt in the source.
    fn drill_row(&self, id: Uuid) -> Option<DrillRow> {
        self.client()
            .query_opt(
                "SELECT succeeded, residue, repo_intact, failure FROM ops.restore_drills WHERE restore_drill_id = $1",
                &[&id],
            )
            .expect("receipt")
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
    }

    fn succeeded_drills(&self) -> i64 {
        self.client()
            .query_one(
                "SELECT count(*) FROM ops.restore_drills WHERE succeeded",
                &[],
            )
            .expect("count")
            .get(0)
    }
}

/// Nothing secret-looking from the test process leaks into the binary's environment beyond what the drill needs.
fn secrets_out(env: &mut Vec<(String, String)>) {
    env.retain(|(k, _)| k != "HUMAUX_MIGRATOR_PG_DSN");
}

/// `(succeeded, residue, repo_intact, failure)` of one `ops.restore_drills` row.
type DrillRow = (bool, Option<i32>, Option<bool>, Option<String>);

/// What the one shared drill run recorded (T-M2, T-M7, T-O2, T-O3 read it; the source is gone by then).
struct Shared {
    floor: Output,
    floor_resources: Vec<String>,
    drill: Output,
    id: Uuid,
    evidence: Value,
    evidence_text: String,
    evidence_mode: u32,
    row: Option<DrillRow>,
    decoys_survived: (bool, bool),
    resources_after: Vec<String>,
    secrets: Vec<String>,
}

/// One migrated source with one VERIFIED backup: a drill below the disk floor, then a full drill (no tenants, so
/// it fails `isolation_not_applicable` but runs every step through destroy and receipt) beside two unlabelled
/// decoys named after its own id. `None` after a §79.2 skip.
fn shared() -> Option<&'static Shared> {
    static SHARED: OnceLock<Option<Shared>> = OnceLock::new();
    SHARED
        .get_or_init(|| {
            let test = "drill shared run";
            let one = drills();
            let live = live(test, "drs")?;
            live.backup();
            let dir = TempDir::new("drs");

            let mut env = live.env();
            set_env(
                &mut env,
                "HUMAUX_MAINTENANCE_RESTORE_MIN_FREE_DISK_BYTES",
                u64::MAX.to_string(),
            );
            let floor = maintenance(
                &[
                    "restore",
                    "drill",
                    "--evidence",
                    &dir.0.join("floor.json").to_string_lossy(),
                ],
                &env,
            );
            let floor_resources = drill_resources();

            let id = Uuid::now_v7();
            let pid = std::process::id();
            let decoy_volume = format!("humaux-drill-{id}_pgdata_decoy");
            let decoy_container = format!("humaux-drill-{id}-decoy-pg-1");
            let _left = Leftovers {
                containers: vec![decoy_container.clone()],
                volumes: vec![decoy_volume.clone()],
            };
            docker(&[
                "volume",
                "create",
                "--label",
                &format!("humaux.c37={pid}"),
                &decoy_volume,
            ])
            .expect("decoy volume");
            docker(&[
                "create",
                "--name",
                &decoy_container,
                "--label",
                &format!("com.docker.compose.project=humaux-drill-{id}-decoy"),
                "--label",
                &format!("humaux.c37={pid}"),
                IMAGE,
                "true",
            ])
            .expect("decoy container");
            let evidence = dir.0.join("drill.json");
            let (drill, json, text) = live.drill(id, &live.env(), &evidence);
            use std::os::unix::fs::PermissionsExt;
            let evidence_mode = std::fs::metadata(&evidence)
                .map(|m| m.permissions().mode() & 0o777)
                .unwrap_or(0);
            let shared = Shared {
                floor,
                floor_resources,
                drill,
                id,
                evidence: json,
                evidence_text: text,
                evidence_mode,
                row: live.drill_row(id),
                decoys_survived: (
                    docker(&["volume", "inspect", &decoy_volume]).is_ok(),
                    docker(&["container", "inspect", &decoy_container]).is_ok(),
                ),
                resources_after: drill_resources(),
                secrets: {
                    let mut s = live.secrets.clone();
                    s.push(live.dsn.clone());
                    s
                },
            };
            drop(live);
            drop(one);
            Some(shared)
        })
        .as_ref()
}

/// T-M7 (D-L step 0, 10.11 H/I): a disk floor no host can meet refuses `drill_free_bytes:<need>/<have>` naming all
/// four terms (restore, vectors, repo_copy, floor), exit 3, no project. Fault: skip the resource precheck → the
/// drill proceeds → not exit 3 → red.
#[test]
fn drill_refuses_below_the_free_memory_or_disk_floor() {
    let Some(s) = shared() else { return };
    assert_eq!(code(&s.floor), 3, "{}", both(&s.floor));
    let reason = receipt(&s.floor)["reason"]
        .as_str()
        .unwrap_or("")
        .to_owned();
    assert!(reason.starts_with("drill_free_bytes:"), "{reason}");
    for term in [
        "restore=",
        "vectors=",
        "repo_copy=",
        "floor=18446744073709551615",
    ] {
        assert!(reason.contains(term), "names {term}: {reason}");
    }
    assert!(
        s.floor_resources.is_empty(),
        "no project: {:?}",
        s.floor_resources
    );
}

/// T-M2 (D-O): the drill destroys exactly its own project — a volume `humaux-drill-<id>_pgdata_decoy` and a compose
/// project `humaux-drill-<id>-decoy`, both without the `humaux.drill` label, survive; nothing of the drill is
/// left. Fault: destroy by name prefix → the decoys are removed → red.
#[test]
fn drill_destroys_only_its_own_project() {
    let Some(s) = shared() else { return };
    assert_eq!(s.decoys_survived, (true, true), "{}", both(&s.drill));
    assert!(s.resources_after.is_empty(), "{:?}", s.resources_after);
    assert_eq!(s.evidence["project"], format!("humaux-drill-{}", s.id));
}

/// T-O2 (D-O): the receipt is written after destroy and carries the residue count (0) and `repo_intact`; the
/// drill without two tenants fails `isolation_not_applicable` and the table records `succeeded = false`. Fault:
/// write the receipt before destroy → residue unknown (NULL) → red.
#[test]
fn the_drill_receipt_is_written_after_destroy_with_residue() {
    let Some(s) = shared() else { return };
    assert_eq!(code(&s.drill), 1, "{}", both(&s.drill));
    let (succeeded, residue, repo_intact, failure) = s.row.clone().expect("a receipt row");
    assert!(!succeeded);
    assert_eq!(residue, Some(0), "residue counted after destroy");
    assert_eq!(repo_intact, Some(true));
    assert!(
        failure
            .as_deref()
            .is_some_and(|f| f.starts_with("isolation_not_applicable")),
        "{failure:?}"
    );
    assert_eq!(s.evidence["residue"], 0);
    assert_eq!(s.evidence["witness_a_present"], true, "{}", s.evidence_text);
    assert_eq!(s.evidence["witness_b_absent"], true, "{}", s.evidence_text);
}

/// T-O3 (D-O): the evidence file is 0600 and holds no secret: no DSN, none of the source's passwords, no cipher
/// pass; it does carry the class and the notice. Fault: put the drill DSN in the evidence → `postgres://` → red.
#[test]
fn the_evidence_file_holds_no_secret() {
    let Some(s) = shared() else { return };
    assert!(!s.evidence_text.is_empty(), "{}", both(&s.drill));
    assert_eq!(s.evidence_mode, 0o600);
    assert!(
        !s.evidence_text.contains("postgres://"),
        "a DSN in the evidence"
    );
    for secret in &s.secrets {
        assert!(
            !s.evidence_text.contains(secret.as_str()),
            "a secret in the evidence"
        );
    }
    assert_eq!(s.evidence["repo_class"], "local_only");
    assert!(s.evidence_text.contains("NOT OFFSITE"));
}

// ============================================================================
// Ignored live legs
// ============================================================================

const A2_LABEL: &str = "embed-v1";

/// The tenant's placement row and the label binding of the worker's boot (the S3 `prepare`).
fn prepare(h: &mut Handle) {
    h.admin
        .execute(
            "INSERT INTO projection.tenant_placements (tenant_id, projection_family, collection_name, placement_class) \
             VALUES ($1, 'private_memory_v1', $2, 'SHARED_FALLBACK')",
            &[&h.tenant_id, &h.collection],
        )
        .expect("placement row");
    h.rt.block_on(bind_embedding_fingerprint(
        &h.retrieval,
        A2_LABEL,
        &worker_fingerprint_inputs("c37-test", "c37-model", "r1", 4, "v1"),
    ))
    .expect("label binding");
}

fn settle_distill(h: &mut Handle) {
    h.admin
        .execute(
            "UPDATE ops.outbox SET status = 'DONE' WHERE tenant_id = $1 \
               AND event_type = 'EVIDENCE_ACCEPTED' AND status IN ('PENDING', 'PROCESSING')",
            &[&h.tenant_id],
        )
        .expect("distill settled");
}

/// Two tenants with projected memories (vectors stored the production way), one ticket left unprojected (ISSUED,
/// its memory distilled) and one Evidence still being distilled (D-Q step 2). Returns the handles (their pools
/// must outlive the drill) and the source collection's point count.
fn seed(live: &Live, qdrant_port: u16) -> Vec<Handle> {
    let mut handles = Vec::new();
    for name in ["a", "b"] {
        let mut h = Handle::in_throwaway_at(live.owner.clone(), Box::new(()), qdrant_port)
            .unwrap_or_else(|e| panic!("a2 fixture: {e:?}"));
        prepare(&mut h);
        let ws = h.workspace();
        h.fan_out(ws, &format!("c37 drill {name} fan"), 2);
        let e = h.evidence(ws, &format!("c37 drill {name} single"));
        h.memory(e, &format!("c37 drill {name} single"), TENANT_SHARED);
        h.drain(ws);
        settle_distill(&mut h);
        h.admin
            .execute(
                "UPDATE projection.stream_checkpoints SET serving = true WHERE tenant_id = $1 AND scope_id = $2",
                &[&h.tenant_id, &ws],
            )
            .expect("serving");
        if name == "a" {
            let e = h.evidence(ws, "c37 drill unprojected");
            h.memory(e, "c37 drill unprojected", TENANT_SHARED);
            settle_distill(&mut h);
            h.evidence(ws, "c37 drill distill pending");
        }
        handles.push(h);
    }
    handles
}

/// `GET path` on the loopback Qdrant; the JSON body.
fn qdrant_get(port: u16, path: &str) -> Value {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    // dep: Qdrant(*) — read-only GET on the decoy (the source's scratch Qdrant)
    // dep: HTTP(loopback) — the scratch container's REST port
    let (status, body) = throwaway::get(addr, path).expect("qdrant answers");
    assert_eq!(status, 200, "{path}: {body}");
    serde_json::from_str(&body).expect("qdrant json")
}

/// Every collection of the decoy (the source's own Qdrant, configured as the maintenance Qdrant) with its points.
fn qdrant_state(port: u16) -> Vec<(String, Value)> {
    let collections = qdrant_get(port, "/collections");
    let mut out: Vec<(String, Value)> = collections["result"]["collections"]
        .as_array()
        .expect("collections")
        .iter()
        .filter_map(|c| c["name"].as_str().map(str::to_owned))
        .map(|name| {
            let points =
                qdrant_get(port, &format!("/collections/{name}"))["result"]["points_count"].clone();
            (name, points)
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Sorted `relative path` of every file under `dir` plus the bytes of `backup.info`.
fn repo_listing(repo: &Path) -> (Vec<String>, Vec<u8>) {
    fn walk(base: &Path, dir: &Path, out: &mut Vec<String>) {
        for e in std::fs::read_dir(dir).expect("read dir").flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(base, &p, out);
            } else {
                out.push(
                    p.strip_prefix(base)
                        .expect("prefix")
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    let mut files = Vec::new();
    walk(repo, repo, &mut files);
    files.sort();
    (
        files,
        std::fs::read(repo.join("backup/humaux/backup.info")).expect("backup.info"),
    )
}

/// T-L1 (D-Q acceptance, 10.4 S5): image built, scratch source from backup.yml, migrated, two tenants projected
/// with stored vectors, one unprojected ticket and one distill-pending Evidence; `backup check` (weak_subkeys=0),
/// three `backup run` with a write burst between them (all VERIFIED, two fulls left); then `restore drill` passes
/// every D-N check with `repo_intact`, provider calls 0 and the decoy Qdrant untouched; the receipt says succeeded;
/// one DR_EVIDENCE pass (S6) reads the third set's stop, the drill's finish, positive headroom, no WAL latch and no
/// `target="offsite"` series; teardown `down -v` leaves the repository's file list and `backup.info` unchanged (T-W6 folded in). The evidence
/// goes to `$HUMAUX_C37_EVIDENCE` when set. Faults: retention ≠ 2 (three fulls); the `repo_intact` term dropped;
/// `--archive-mode=off` dropped (archiver attempts > 0); the drill Qdrant built from env (the decoy changes or
/// `drill_qdrant_not_fresh`); the repository as a named volume (down -v removes it).
#[test]
#[ignore = "lane(c) live Docker drill on its own humaux-c37-* scratch containers, which no lane resource provisions; run by its named chain gate c37_drill_e2e_live: HUMAUX_REQUIRE_DOCKER=1 -- --ignored --exact drill_end_to_end_on_scratch_containers"]
#[allow(clippy::too_many_lines)] // the one acceptance scenario of D-Q, in its order
fn drill_end_to_end_on_scratch_containers() {
    let test = "drill_end_to_end_on_scratch_containers";
    let one = drills();
    let Some(live) = live(test, "l1") else { return };
    let qdrant =
        scratch_qdrant::ScratchQdrant::start("decoy").unwrap_or_else(|e| panic!("{test}: {e}"));
    let handles = seed(&live, qdrant.port);

    let check = live
        .source
        .maintenance(&live.dsn, &["backup", "check"], &[]);
    assert_eq!(code(&check), 0, "{}", both(&check));
    assert!(
        String::from_utf8_lossy(&check.stdout)
            .lines()
            .any(|l| l == "weak_subkeys=0")
    );
    // The `--archive-mode=off` fault needs archive settings INSIDE the backed-up data directory: backup.yml sets
    // them by `-c` flags (which override postgresql.auto.conf on the source), so without these lines the restored
    // drill cluster would never archive whether or not the drill passed `--archive-mode=off`.
    // ALTER SYSTEM refuses a transaction block, so one statement per call.
    let mut owner = live.client();
    for sql in [
        "ALTER SYSTEM SET archive_mode = 'on'",
        "ALTER SYSTEM SET archive_command = 'pgbackrest --stanza=humaux archive-push %p'",
    ] {
        owner
            .batch_execute(sql)
            .expect("archive settings in postgresql.auto.conf");
    }
    drop(owner);
    for i in 0..3 {
        if i > 0 {
            live.source.seed(&format!("burst{i}"), 2);
        }
        live.backup();
    }
    assert_eq!(
        live.source.labels().len(),
        2,
        "retention 2: exactly two fulls"
    );

    let issued_before: i64 = live
        .client()
        .query_one(
            "SELECT count(*) FROM projection.stream_log WHERE state = 'ISSUED'",
            &[],
        )
        .expect("issued")
        .get(0);
    let decoy_before = qdrant_state(qdrant.port);
    let mut env = live.env();
    set_env(
        &mut env,
        "HUMAUX_MAINTENANCE_QDRANT_HOST",
        "127.0.0.1".to_owned(),
    );
    set_env(
        &mut env,
        "HUMAUX_MAINTENANCE_QDRANT_PORT",
        qdrant.port.to_string(),
    );
    set_env(
        &mut env,
        "HUMAUX_MAINTENANCE_QDRANT_CIDR",
        "127.0.0.1/32".to_owned(),
    );
    let dir = TempDir::new("l1");
    let evidence = std::env::var("HUMAUX_C37_EVIDENCE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| dir.0.join("drill.json"));
    let id = Uuid::now_v7();
    let (out, ev, text) = live.drill(id, &env, &evidence);
    assert_eq!(code(&out), 0, "{}\n{text}", both(&out));
    for (k, v) in [
        ("succeeded", Value::Bool(true)),
        ("witness_a_present", Value::Bool(true)),
        ("witness_b_absent", Value::Bool(true)),
        ("server_version_matches", Value::Bool(true)),
        ("repo_intact", Value::Bool(true)),
        ("migrations_drift", Value::from(0)),
        ("rls_unforced", Value::from(0)),
        ("isolation_violations", Value::from(0)),
        ("isolation_pairs", Value::from(2)),
        ("payload_digest_mismatches", Value::from(0)),
        ("provider_calls", Value::from(0)),
        ("drill_archiver_attempts", Value::from(0)),
        ("legacy_points_without_vector", Value::from(0)),
        ("unprojected_at_target", Value::from(1)),
        ("residue", Value::from(0)),
        ("repo_class", Value::from("local_only")),
    ] {
        assert_eq!(ev[k], v, "{k}: {text}");
    }
    assert_eq!(ev["backup"]["manifest_matches"], true, "{text}");
    assert_eq!(ev["rebuild"]["equivalent"], true, "{text}");
    assert_eq!(ev["rebuild"]["distill_pending_inputs"], 1, "{text}");
    assert!(
        ev["rebuild"]["points"].as_i64().is_some_and(|p| p >= 6),
        "{text}"
    );
    assert!(
        ev["restored_in_flight"]["issued_g1"]
            .as_i64()
            .is_some_and(|n| n >= 1),
        "{text}"
    );
    assert!(text.contains("NOT OFFSITE"));
    let issued_after: i64 = live
        .client()
        .query_one(
            "SELECT count(*) FROM projection.stream_log WHERE state = 'ISSUED'",
            &[],
        )
        .expect("issued")
        .get(0);
    assert_eq!(
        issued_before, issued_after,
        "the source's in-flight tickets are untouched"
    );
    assert_eq!(
        decoy_before,
        qdrant_state(qdrant.port),
        "the configured Qdrant is untouched"
    );
    let (succeeded, residue, repo_intact, _) = live.drill_row(id).expect("receipt");
    assert!(succeeded, "the table derived success");
    assert_eq!((residue, repo_intact), (Some(0), Some(true)));
    let status = live
        .source
        .maintenance(&live.dsn, &["backup", "status"], &[]);
    assert_eq!(
        receipt(&status)["repo_class"],
        "local_only",
        "{}",
        both(&status)
    );
    assert!(drill_resources().is_empty());

    // Step 5 (card 37 S6, ADR-0064 D-K / 10.11 D): one DR_EVIDENCE pass of the real daemon against the source.
    let third_stop: f64 = live
        .client()
        .query_one(
            "SELECT extract(epoch FROM backup_stopped_at)::float8 FROM ops.backup_receipts \
             WHERE outcome = 'VERIFIED' ORDER BY recorded_at DESC LIMIT 1",
            &[],
        )
        .expect("the third set's stop")
        .get(0);
    let finished: f64 = live
        .client()
        .query_one(
            "SELECT extract(epoch FROM finished_at)::float8 FROM ops.restore_drills WHERE restore_drill_id = $1",
            &[&id],
        )
        .expect("the drill's finish")
        .get(0);
    let metrics = throwaway::dr_evidence_pass(&live.dsn, &live.source.repo_dir);
    let gauge = |series: &str| throwaway::sample_value(&metrics, series);
    assert_eq!(
        gauge(r#"backup_last_success_timestamp_seconds{target="local"}"#),
        Some(third_stop),
        "{metrics}"
    );
    assert_eq!(
        gauge(r#"restore_drill_last_success_timestamp_seconds{target="local"}"#),
        Some(finished),
        "{metrics}"
    );
    for limit in ["repo_max", "free_floor"] {
        let headroom = gauge(&format!(
            r#"backup_budget_headroom_bytes{{limit="{limit}"}}"#
        ));
        assert!(headroom.is_some_and(|h| h > 0.0), "{limit}: {metrics}");
    }
    assert_eq!(gauge("wal_archive_failing"), Some(0.0), "{metrics}");
    assert!(
        !metrics.contains(r#"target="offsite""#),
        "no offsite series: {metrics}"
    );

    // Teardown (T-W6, 10.11 I): down -v of the source project leaves the repository untouched.
    drop(handles);
    // A clean shutdown archives its last segment, so archive/ may only grow; backup/ and backup.info stay as they are.
    let (files, info) = repo_listing(&live.source.repo());
    live.source.down_volumes();
    let (after, info_after) = repo_listing(&live.source.repo());
    assert_eq!(info, info_after, "backup.info survives down -v");
    let backup = |f: &Vec<String>| {
        f.iter()
            .filter(|p| p.starts_with("backup/"))
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(
        backup(&files),
        backup(&after),
        "the backup/ tree survives down -v"
    );
    assert!(
        files.iter().all(|f| after.contains(f)),
        "no archived file disappeared"
    );
    drop(qdrant);
    drop(live);
    drop(one);
}

/// Corrupts (flips one byte of) the first archived WAL segment after `label`'s stop segment; returns its path.
fn corrupt_wal_after_set(live: &Live, label: &str) -> PathBuf {
    let info = live.source.info();
    let set = info["backup"]
        .as_array()
        .and_then(|s| s.iter().find(|s| s["label"] == label))
        .expect("set")
        .clone();
    let stop = set["archive"]["stop"]
        .as_str()
        .expect("archive stop")
        .to_owned();
    let mut segments = Vec::new();
    let archive = live.source.repo().join("archive/humaux");
    for dir in std::fs::read_dir(&archive).expect("archive").flatten() {
        if !dir.path().is_dir() {
            continue;
        }
        for timeline in std::fs::read_dir(dir.path())
            .expect("version dir")
            .flatten()
        {
            if !timeline.path().is_dir() {
                continue;
            }
            for f in std::fs::read_dir(timeline.path())
                .expect("segments")
                .flatten()
            {
                let name = f.file_name().to_string_lossy().into_owned();
                if name.len() > 24 && !name.contains(".backup") && name[..24] > *stop.as_str() {
                    segments.push(f.path());
                }
            }
        }
    }
    segments.sort();
    let first = segments.first().expect("a segment after the set").clone();
    let mut bytes = std::fs::read(&first).expect("segment");
    let at = bytes.len() / 2;
    bytes[at] ^= 0xff;
    std::fs::write(&first, &bytes).expect("flip one byte");
    first
}

/// Writes and archives `n` forced segments (one row each), waiting for the archiver.
fn archive_segments(live: &Live, n: usize) {
    for i in 0..n {
        live.source.psql(&format!(
            "CREATE TABLE IF NOT EXISTS c37_seg (i int); INSERT INTO c37_seg VALUES ({i}); SELECT pg_switch_wal()"
        ));
    }
    let drained = (0..120).any(|_| {
        std::thread::sleep(Duration::from_millis(250));
        live.source
            .psql("SELECT count(*) = 0 FROM pg_ls_archive_statusdir() WHERE name LIKE '%.ready'")
            == "t"
    });
    assert!(drained, "segments archived");
}

/// T-L2 (card fault): a byte flipped in a WAL segment archived after the set makes the drill fail
/// `restore_failed` (PG18 refuses to start when recovery ends before T), exit 1, `succeeded = false`, and no
/// succeeded drill exists for the drill gauge to read (its D-K leg is the newest succeeded `finished_at`). Fault: a
/// corrupt WAL that does not fail the drill, or a success claimed anyway → red.
#[test]
#[ignore = "lane(c) live Docker drill on its own humaux-c37-* scratch containers, which no lane resource provisions; run by its named chain gate c37_drill_corrupt_wal_live: HUMAUX_REQUIRE_DOCKER=1 -- --ignored --exact drill_with_a_corrupted_wal_segment_fails_and_no_gauge_advances"]
fn drill_with_a_corrupted_wal_segment_fails_and_no_gauge_advances() {
    let test = "drill_with_a_corrupted_wal_segment_fails_and_no_gauge_advances";
    let one = drills();
    let Some(live) = live(test, "l2") else { return };
    live.backup();
    let label = live.source.labels().pop().expect("one set");
    archive_segments(&live, 3);
    corrupt_wal_after_set(&live, &label);
    let dir = TempDir::new("l2");
    let id = Uuid::now_v7();
    let (out, ev, text) = live.drill(id, &live.env(), &dir.0.join("drill.json"));
    assert_eq!(code(&out), 1, "{}\n{text}", both(&out));
    let failure = ev["failure"].as_str().unwrap_or("");
    assert!(failure.starts_with("restore_failed"), "{failure}");
    let (succeeded, residue, _, _) = live.drill_row(id).expect("receipt");
    assert!(!succeeded);
    assert_eq!(residue, Some(0));
    assert_eq!(
        live.succeeded_drills(),
        0,
        "the drill gauge has nothing newer to read"
    );
    assert!(drill_resources().is_empty());
    drop(live);
    drop(one);
}

/// T-L4 (D-N faults, finding 4): four catalog faults injected into the scratch source after the backup and before
/// the drill reach the restored cluster through PITR and each is reported: NO FORCE RLS on a tenant table
/// (`rls_unforced`), RLS disabled on `private.memory_records` (`isolation_violations`), a `schema_migrations` row
/// applied after T (`migrations_drift`), an Evidence row created after T (`payload_digest_mismatches`); exit 1,
/// `succeeded = false`. Fault: any of (d), (e), (f), (h) hard-coded to 0 → red.
#[test]
#[ignore = "lane(c) live Docker drill on its own humaux-c37-* scratch containers, which no lane resource provisions; run by its named chain gate c37_drill_catalog_faults_live: HUMAUX_REQUIRE_DOCKER=1 -- --ignored --exact drill_reports_each_injected_catalog_fault"]
fn drill_reports_each_injected_catalog_fault() {
    let test = "drill_reports_each_injected_catalog_fault";
    let one = drills();
    let Some(live) = live(test, "l4") else { return };
    let qdrant =
        scratch_qdrant::ScratchQdrant::start("qdrant").unwrap_or_else(|e| panic!("{test}: {e}"));
    let handles = seed(&live, qdrant.port);
    live.backup();
    let tenant_a = handles[0].tenant_id;
    // replica-mode: throwaway database only — the scratch source cluster of this drill, never a shared database.
    live.client()
        .batch_execute(&format!(
            "ALTER TABLE projection.memory_vectors NO FORCE ROW LEVEL SECURITY; \
             ALTER TABLE private.memory_records DISABLE ROW LEVEL SECURITY; \
             INSERT INTO ops.schema_migrations (migration_id, checksum, applied_at) \
               VALUES ('9999_c37_fault', 'x', now() + interval '1 day'); \
             SET session_replication_role = replica; \
             UPDATE private.evidence_objects SET created_at = now() + interval '1 day' \
              WHERE evidence_id = (SELECT evidence_id FROM private.evidence_objects \
                                    WHERE tenant_id = '{tenant_a}' ORDER BY evidence_id LIMIT 1); \
             SET session_replication_role = origin"
        ))
        .expect("inject the four faults");
    let dir = TempDir::new("l4");
    let id = Uuid::now_v7();
    let (out, ev, text) = live.drill(id, &live.env(), &dir.0.join("drill.json"));
    assert_eq!(code(&out), 1, "{}\n{text}", both(&out));
    for k in [
        "rls_unforced",
        "isolation_violations",
        "migrations_drift",
        "payload_digest_mismatches",
    ] {
        assert!(ev[k].as_i64().is_some_and(|n| n >= 1), "{k} >= 1: {text}");
    }
    assert_eq!(ev["succeeded"], false);
    let (succeeded, residue, _, _) = live.drill_row(id).expect("receipt");
    assert!(!succeeded);
    assert_eq!(residue, Some(0));
    assert_eq!(live.succeeded_drills(), 0);
    drop(handles);
    drop(qdrant);
    drop(live);
    drop(one);
}

/// Removes a compose project's resources (by its project label) on drop: the project `restore pitr` creates.
struct ProjectGuard(String);

impl Drop for ProjectGuard {
    fn drop(&mut self) {
        let filter = format!("label=com.docker.compose.project={}", self.0);
        for (list, rm) in [
            (
                vec!["ps", "-aq", "--filter", &filter],
                vec!["rm", "-f", "-v"],
            ),
            (
                vec!["volume", "ls", "-q", "--filter", &filter],
                vec!["volume", "rm", "-f"],
            ),
            (
                vec!["network", "ls", "-q", "--filter", &filter],
                vec!["network", "rm"],
            ),
        ] {
            for id in docker(&list)
                .unwrap_or_default()
                .lines()
                .filter(|l| !l.is_empty())
            {
                let mut args = rm.clone();
                args.push(id);
                if let Err(e) = docker(&args) {
                    eprintln!("pitr test cleanup: {e}");
                }
            }
        }
    }
}

/// T-Q1 (D-U, 10.11 B/I): `restore pitr --target end` refuses (exit 3, nothing restored) (i) while the production
/// project runs (`production_pg_running`), (ii) below the disk floor (`restore_free_bytes:` with both numbers),
/// (iii) when the set does not verify (`repo_set_unverifiable`); with production stopped it restores into a new
/// project to the end of the archive (the last committed witness is present), quarantines every worker role
/// (`rolcanlogin = false`; a login as role_private_worker is refused) and reports the class, notice and repo dir.
/// Faults: skip either refusal; skip the pre-restore verify; skip NOLOGIN → red.
#[test]
#[ignore = "lane(c) live Docker drill on its own humaux-c37-* scratch containers, which no lane resource provisions; run by its named chain gate c37_restore_pitr_live: HUMAUX_REQUIRE_DOCKER=1 -- --ignored --exact restore_pitr_to_end_of_archive_quarantines_worker_roles"]
#[allow(clippy::too_many_lines)] // one scenario: three refusals, then the restore and its quarantine
fn restore_pitr_to_end_of_archive_quarantines_worker_roles() {
    let test = "restore_pitr_to_end_of_archive_quarantines_worker_roles";
    let one = drills();
    let Some(live) = live(test, "q1") else { return };
    live.backup();
    let label = live.source.labels().pop().expect("one set");
    live.client()
        .batch_execute(
            "CREATE TABLE c37_witness (w text); INSERT INTO c37_witness VALUES ('last committed')",
        )
        .expect("witness");
    archive_segments(&live, 1);

    let target = format!("humaux-c37-q1new-{}", std::process::id());
    let _guard = ProjectGuard(target.clone());
    let dir = TempDir::new("q1");
    let compose = dir.0.join("backup-capped.yml");
    let shipped = std::fs::read_to_string(containers::backup_yml()).expect("backup.yml");
    let conf = root().join("deploy/pgbackrest/pgbackrest.conf");
    let capped = shipped
        .replace(
            "source: ../pgbackrest/pgbackrest.conf",
            &format!("source: {}", conf.display()),
        )
        .replace(
            "    container_name: ${HUMAUX_PG_CONTAINER:?}\n",
            &format!(
                "    container_name: ${{HUMAUX_PG_CONTAINER:?}}\n    mem_limit: 768m\n    cpus: 1\n    labels:\n      humaux.c37: \"{}\"\n",
                std::process::id()
            ),
        );
    assert_ne!(capped, shipped);
    std::fs::write(&compose, capped).expect("capped compose");
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("port")
        .port();
    let mut env = live.env();
    for (k, v) in [
        ("HUMAUX_PG_CONTAINER", target.clone()),
        ("HUMAUX_PG_PORT", port.to_string()),
        ("HUMAUX_PG_ARCHIVE_TIMEOUT_SECONDS", "3600".to_owned()),
        ("HUMAUX_PG_LISTEN_ADDRESSES", "*".to_owned()),
        (
            "POSTGRES_PASSWORD",
            live.source.env_value("POSTGRES_PASSWORD"),
        ),
    ] {
        set_env(&mut env, k, v);
    }
    let evidence = dir.0.join("pitr.json");
    let args = [
        "restore",
        "pitr",
        "--compose-file",
        &compose.to_string_lossy(),
        "--project",
        &target,
        "--target",
        "end",
        "--evidence",
        &evidence.to_string_lossy(),
    ]
    .map(str::to_owned);
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let pitr = |env: &[(String, String)]| maintenance(&args, env);
    let reason = |out: &Output| receipt(out)["reason"].as_str().unwrap_or("").to_owned();

    // (i) production still runs.
    let out = pitr(&env);
    assert_eq!(code(&out), 3, "{}", both(&out));
    assert!(
        reason(&out).starts_with("production_pg_running:"),
        "{}",
        both(&out)
    );
    live.source.stop();
    // (ii) the disk floor.
    let mut floor = env.clone();
    set_env(
        &mut floor,
        "HUMAUX_MAINTENANCE_RESTORE_MIN_FREE_DISK_BYTES",
        u64::MAX.to_string(),
    );
    let out = pitr(&floor);
    assert_eq!(code(&out), 3, "{}", both(&out));
    let r = reason(&out);
    assert!(
        r.starts_with("restore_free_bytes:18446744073709551615/"),
        "{r}"
    );
    // (iii) a set that does not verify; the byte is restored afterwards.
    let bundle = live
        .source
        .repo()
        .join(format!("backup/humaux/{label}/bundle/1"));
    let original = std::fs::read(&bundle).expect("bundle 1");
    let mut flipped = original.clone();
    flipped[1000] ^= 0xff;
    std::fs::write(&bundle, &flipped).expect("flip");
    let out = pitr(&env);
    std::fs::write(&bundle, &original).expect("restore the byte");
    assert_eq!(code(&out), 3, "{}", both(&out));
    assert!(
        reason(&out).starts_with(&format!("repo_set_unverifiable:{label}")),
        "{}",
        both(&out)
    );
    assert!(!evidence.exists(), "nothing restored, nothing written");

    // The restore.
    let out = pitr(&env);
    assert_eq!(code(&out), 0, "{}", both(&out));
    let r = receipt(&out);
    assert_eq!(r["repo_class"], "local_only");
    assert!(
        r["notice"]
            .as_str()
            .is_some_and(|n| n.starts_with("NOT OFFSITE"))
    );
    assert_eq!(
        r["repo_dir"],
        live.source.repo_dir.to_string_lossy().as_ref()
    );
    let restored = format!(
        "postgres://postgres:{}@127.0.0.1:{port}/{LIVE_DB}",
        live.source.env_value("POSTGRES_PASSWORD")
    );
    let mut client = (0..60)
        .find_map(|_| {
            std::thread::sleep(Duration::from_millis(500));
            // dep: PostgreSQL(owner) — the restored cluster of the new project
            postgres::Client::connect(&restored, postgres::NoTls).ok()
        })
        .expect("the restored cluster accepts TCP");
    let w: String = client
        .query_one("SELECT w FROM c37_witness", &[])
        .expect("end of archive: the last witness")
        .get(0);
    assert_eq!(w, "last committed");
    for role in [
        "role_gateway",
        "role_private_worker",
        "role_consolidation_worker",
        "role_public_worker",
    ] {
        let can: bool = client
            .query_one(
                "SELECT rolcanlogin FROM pg_roles WHERE rolname = $1",
                &[&role],
            )
            .expect("role")
            .get(0);
        assert!(!can, "{role} quarantined");
    }
    let login = format!(
        "postgres://role_private_worker:{}@127.0.0.1:{port}/{LIVE_DB}",
        live.secrets[5]
    );
    // dep: PostgreSQL(role_private_worker) — the login the quarantine must refuse
    let refused = postgres::Client::connect(&login, postgres::NoTls)
        .err()
        .map(|e| format!("{e:?}"))
        .unwrap_or_default();
    assert!(refused.contains("not permitted to log in"), "{refused}");
    drop(client);
    drop(live);
    drop(one);
}
