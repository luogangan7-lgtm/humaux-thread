//! `maintenance::tests::measure` — the card-37 S7 measurements (ADR-0064 section 10.4 S7): M1 backup / verify /
//!   expire / PITR restore on a scratch source holding the card-36 dev dump, M3 WAL per day and the drill's archive
//!   wait, M5 the repository peak, and M2 a rebuild of 17,435 stored 1024-dim vectors across two tenants. Each test is
//!   ignored, run once by hand with `--nocapture`, and prints `MEASURE m<k> <key>=<median> <unit> n=<n> …` lines that
//!   are pasted into ADR-0064 (gate c37_measurements). Each asserts the outcome it measures (VERIFIED sets, retention
//!   2, a restored cluster, an equivalent rebuild with zero provider calls), so a number is never printed for a
//!   failed run.
//! Depends-on: crates=[async-trait, humaux-adapters, humaux-domain, humaux-infra-cell, humaux-local-secret-scan,
//!   humaux-projection, humaux-retrieval, humaux-retrieval-provider, postgres, serde_json, sqlx, tokio, uuid];
//!   services=[subprocess(docker), subprocess(humaux-maintenance), PostgreSQL(owner) r=[projection.memory_vectors,
//!   projection.private_memory_points, projection.stream_log] w=[ops.outbox, projection.stream_checkpoints,
//!   projection.tenant_placements], PostgreSQL(role_maintenance), PostgreSQL(role_retrieval_worker), Qdrant(*)];
//!   env=[HOME, HUMAUX_C37_MEASURE_DUMP, HUMAUX_MAINTENANCE_BACKUP_COMPOSE_FILE, HUMAUX_MAINTENANCE_BACKUP_PROJECT,
//!   HUMAUX_MAINTENANCE_DRILL_RESTORE_TIMEOUT_SECONDS, HUMAUX_MAINTENANCE_PG_DSN,
//!   HUMAUX_MAINTENANCE_RESTORE_MIN_FREE_DISK_BYTES, HUMAUX_PG_ARCHIVE_TIMEOUT_SECONDS, HUMAUX_PG_CONTAINER,
//!   HUMAUX_PG_IMAGE, HUMAUX_PG_LISTEN_ADDRESSES, HUMAUX_PG_PORT, HUMAUX_PG_REPO_DIR, PATH];
//!   modules=[adapters::postgres, adapters::private_projection_registry, adapters::projection_worker,
//!   adapters::provisioning, adapters::qdrant, adapters::rebuild, adapters::stream_repo,
//!   adapters::tests::support::a2_fixture, adapters::tests::support::governance_ops,
//!   adapters::tests::support::scratch_qdrant, domain::egress, domain::error, domain::ids, humaux-infra-cell,
//!   humaux-local-secret-scan, humaux-projection, humaux-retrieval, humaux-retrieval-provider, infra-cell::permit,
//!   infra-cell::resource, maintenance::tests::support::containers, maintenance::tests::support::throwaway]
//! Called-by: [cargo-test]
//! Invariants: [measurements only: ignored, never in the chain; the backed-up cluster is a scratch project
//!   humaux-c37-m1-<pid>-<n> and every restore a project humaux-c37-m1r-<pid>-<i>, each removed by its guard; the
//!   dump is READ as a file (the shared humaux-thread-pg is never named, ruling E7); M2's database is a throwaway
//!   humaux_thread_c37_m2_<pid>_<n> on the dev cluster and its Qdrant a scratch container; no secret is printed]
//! Spec: ADR-0064 D-Q; ADR-0064 D-V; ADR-0064 section 10.4 S7; ADR-0064 section 10.6

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

// The #[path]-included a2 fixture's crates (dep-map attributes an included file to the package it lives in).
use humaux_infra_cell as _;
use humaux_projection as _;
use humaux_retrieval as _;
use humaux_retrieval_provider as _;
use sqlx as _;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use a2_fixture::{Handle, TENANT_SHARED};
use containers::{Source, both, docker, root};
use humaux_adapters::postgres::RetrievalWorkerDbPool;
use humaux_adapters::private_projection_registry::{
    bind_embedding_fingerprint, worker_fingerprint_inputs,
};
use humaux_adapters::projection_worker::{
    CardEmbedder, PassConfig, run_claimed_pass_for_run, run_once,
};
use humaux_adapters::provisioning::{self, QdrantFace};
use humaux_adapters::qdrant::RetrievalFamily;
use humaux_adapters::rebuild::{self, ClosedDeps, RebuildDeps, Stream, Verdict};
use humaux_adapters::stream_repo::{Backoff, ClaimFamily, OnlyRun};
use humaux_domain::egress::ProcessorId;
use humaux_domain::error::ErrorCode;
use humaux_domain::ids::TenantId;
use humaux_local_secret_scan::SealedRetrievalCard;
use serde_json::Value;
use uuid::Uuid;

/// One `MEASURE` line: the median of `runs` with every run listed in the note.
fn measure(id: &str, key: &str, unit: &str, runs: &[f64], note: &str) -> f64 {
    let mut sorted = runs.to_vec();
    sorted.sort_by(f64::total_cmp);
    let median = sorted[sorted.len() / 2];
    let all: Vec<String> = runs.iter().map(|r| format!("{r:.3}")).collect();
    println!(
        "MEASURE {id} {key}={median:.3} {unit} n={} (runs={}; {note})",
        runs.len(),
        all.join(",")
    );
    median
}

fn secs(t: Instant) -> f64 {
    t.elapsed().as_secs_f64()
}

/// Sum of the file sizes under `dir` (0 when it does not exist yet).
fn tree_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| {
            let p = e.path();
            if p.is_dir() {
                tree_bytes(&p)
            } else {
                e.metadata().map(|m| m.len()).unwrap_or(0)
            }
        })
        .sum()
}

// ============================================================================
// M1 / M3 / M5 — backup, verify, expire, PITR restore, WAL, peak (Docker)
// ============================================================================

/// The capped `backup.yml` a restore project is created from (absolute conf path, the D-P caps), as T-Q1 builds it.
fn capped_compose(dir: &Path) -> PathBuf {
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
    assert_ne!(capped, shipped, "the capped override applied");
    let path = dir.join("backup-capped.yml");
    std::fs::write(&path, capped).expect("capped compose");
    path
}

/// Removes every container, volume and network of one compose project by its label (never a name prefix).
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
                    eprintln!("measure cleanup: {e}");
                }
            }
        }
    }
}

/// `pgbackrest <args>` in the source, timed and asserted to exit 0; `(seconds, stdout)`.
fn timed(source: &Source, args: &[&str]) -> (f64, String) {
    let t = Instant::now();
    let out = source.pgbackrest(args);
    let took = secs(t);
    assert!(out.status.success(), "pgbackrest {args:?}: {}", both(&out));
    (took, String::from_utf8_lossy(&out.stdout).into_owned())
}

/// M1 (×5), M3 (×2), M5 (×1) on one scratch source holding the card-36 dev dump (`HUMAUX_C37_MEASURE_DUMP`, read
/// as a file): three full backups, each verified and followed by an expire; three `pgbackrest check` waits; then,
/// with the source stopped, three `restore pitr --target end` runs into fresh capped projects.
#[test]
#[ignore = "lane(c) ADR-0064 measurement on a dev-sized scratch copy that no lane resource provisions, run once and pasted into the ADR as MEASURE lines; never in the chain or the lane: HUMAUX_REQUIRE_DOCKER=1 HUMAUX_C37_MEASURE_DUMP=<dump> -- --ignored --exact m1_m3_m5_backup_restore_and_wal_on_the_dev_dump --nocapture"]
#[allow(clippy::too_many_lines, clippy::cast_precision_loss)] // one measurement run, in its order
fn m1_m3_m5_backup_restore_and_wal_on_the_dev_dump() {
    let test = "m1_m3_m5_backup_restore_and_wal_on_the_dev_dump";
    let dump = PathBuf::from(
        std::env::var("HUMAUX_C37_MEASURE_DUMP")
            .expect("HUMAUX_C37_MEASURE_DUMP = the card-36 dev dump file (pg_dump -Fc)"),
    );
    let Some(mut source) = Source::prepare(test, "m1") else {
        return;
    };
    source.set_env("HUMAUX_PG_LISTEN_ADDRESSES", "*");
    source.restart("");
    let version = source.pgbackrest_version();
    // The roles the dump's policies name exist once the migrations ran (cluster-global); `humaux_thread` is also
    // the database `restore pitr` quarantines in.
    source.psql("CREATE DATABASE humaux_thread");
    // dep: PostgreSQL(owner) — migrate the scratch cluster (never the dev cluster)
    let mut owner = postgres::Client::connect(&source.owner_dsn("humaux_thread"), postgres::NoTls)
        .expect("scratch owner");
    throwaway::migrate_all(&mut owner);
    drop(owner);
    source.stanza_create();

    // The dev dump, with archiving on: its WAL is the M3 sample of real write WAL.
    let archived = |s: &Source| -> f64 {
        s.psql("SELECT archived_count FROM pg_stat_archiver")
            .parse()
            .expect("archived_count")
    };
    let archive_dir = source.repo().join("archive");
    let (count0, bytes0) = (archived(&source), tree_bytes(&archive_dir));
    source.psql("CREATE DATABASE humaux_devcopy");
    let t = Instant::now();
    // dep: subprocess(docker) — pg_restore of the dump file into the scratch container (stdin = the file)
    let restored = Command::new("docker")
        .args([
            "exec",
            "-i",
            "-u",
            "postgres",
            &source.name,
            "pg_restore",
            "--no-owner",
            "--no-acl",
            "-d",
            "humaux_devcopy",
        ])
        .stdin(Stdio::from(
            std::fs::File::open(&dump).expect("open the dump"),
        ))
        .output()
        .expect("pg_restore");
    let load_s = secs(t);
    let errors = String::from_utf8_lossy(&restored.stderr)
        .lines()
        .find(|l| l.contains("errors ignored on restore"))
        .unwrap_or("errors ignored on restore: 0")
        .to_owned();
    source.psql("SELECT pg_switch_wal()");
    timed(&source, &["check", "--log-level-console=warn"]);
    let (count1, bytes1) = (archived(&source), tree_bytes(&archive_dir));
    let devcopy: f64 = source
        .psql("SELECT pg_database_size('humaux_devcopy')")
        .parse()
        .expect("size");
    let cluster: f64 = source
        .psql("SELECT sum(pg_database_size(datname)) FROM pg_database")
        .parse()
        .expect("size");
    assert!(
        devcopy > 100.0 * 1024.0 * 1024.0,
        "the dump restored: {errors}"
    );

    // M1: three fulls, each verified (the arm's pass rule), then expire (retention 2).
    let (mut full, mut verify, mut expire, mut set) = (vec![], vec![], vec![], vec![]);
    for _ in 0..3 {
        full.push(
            timed(
                &source,
                &[
                    "backup",
                    "--type=full",
                    "--no-expire-auto",
                    "--log-level-console=warn",
                ],
            )
            .0,
        );
        let info = source.info();
        let newest = info["backup"]
            .as_array()
            .and_then(|a| a.last())
            .cloned()
            .unwrap_or(Value::Null);
        let label = newest["label"].as_str().expect("label").to_owned();
        set.push(
            newest["info"]["repository"]["size"]
                .as_f64()
                .expect("set size"),
        );
        let (took, out) = timed(
            &source,
            &[
                "verify",
                &format!("--set={label}"),
                "--output=text",
                "--verbose",
                "--log-level-console=warn",
            ],
        );
        assert!(
            out.contains(&format!("backup: {label}, status: valid,")),
            "{out}"
        );
        verify.push(took);
        expire.push(timed(&source, &["expire", "--log-level-console=warn"]).0);
    }
    assert_eq!(source.labels().len(), 2, "retention 2");

    // M3: the drill's step-2 wait (`pgbackrest check` in production pg), n = 3.
    let checks: Vec<f64> = (0..3)
        .map(|_| timed(&source, &["check", "--log-level-console=warn"]).0)
        .collect();

    // M1 pitr_restore_s: the source stopped (refusal 3), three restores to the end of the archive.
    source.stop();
    let dir = source.dir.clone();
    let compose = capped_compose(&dir);
    let mut restores = Vec::new();
    for i in 0..3 {
        let project = format!("humaux-c37-m1r-{}-{i}", std::process::id());
        let _guard = ProjectGuard(project.clone());
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .expect("port")
            .port();
        let evidence = dir.join(format!("pitr-{i}.json"));
        let mut env: Vec<(&str, String)> = vec![
            // Only the database name is read from it (the quarantine runs over the socket).
            (
                "HUMAUX_MAINTENANCE_PG_DSN",
                "postgres://role_maintenance@127.0.0.1/humaux_thread".to_owned(),
            ),
            (
                "HUMAUX_MAINTENANCE_BACKUP_COMPOSE_FILE",
                containers::backup_yml().to_string_lossy().into_owned(),
            ),
            ("HUMAUX_MAINTENANCE_BACKUP_PROJECT", source.name.clone()),
            (
                "HUMAUX_MAINTENANCE_DRILL_RESTORE_TIMEOUT_SECONDS",
                "900".to_owned(),
            ),
            (
                "HUMAUX_MAINTENANCE_RESTORE_MIN_FREE_DISK_BYTES",
                (1_u64 << 20).to_string(),
            ),
            ("HUMAUX_PG_IMAGE", containers::IMAGE.to_owned()),
            (
                "HUMAUX_PG_REPO_DIR",
                source.repo_dir.to_string_lossy().into_owned(),
            ),
            ("HUMAUX_PG_CONTAINER", project.clone()),
            ("HUMAUX_PG_PORT", port.to_string()),
            ("HUMAUX_PG_ARCHIVE_TIMEOUT_SECONDS", "3600".to_owned()),
            ("HUMAUX_PG_LISTEN_ADDRESSES", "*".to_owned()),
            ("POSTGRES_PASSWORD", source.env_value("POSTGRES_PASSWORD")),
            (
                "PGBACKREST_REPO1_CIPHER_PASS",
                source.env_value("PGBACKREST_REPO1_CIPHER_PASS"),
            ),
        ];
        for key in ["PATH", "HOME"] {
            if let Ok(v) = std::env::var(key) {
                env.push((key, v));
            }
        }
        // dep: subprocess(humaux-maintenance) — one real `restore pitr` into a fresh capped project
        let out = throwaway::run(
            &[
                "restore",
                "pitr",
                "--compose-file",
                &compose.to_string_lossy(),
                "--project",
                &project,
                "--target",
                "end",
                "--evidence",
                &evidence.to_string_lossy(),
            ],
            &env,
        );
        assert!(out.status.success(), "restore pitr {i}: {}", both(&out));
        let receipt: Value = serde_json::from_str(
            String::from_utf8_lossy(&out.stdout)
                .lines()
                .last()
                .unwrap_or(""),
        )
        .expect("receipt json");
        assert_eq!(receipt["outcome"], "restored", "{receipt}");
        restores.push(receipt["restore_s"].as_f64().expect("restore_s"));
    }

    let mib16 = 16.0 * 1024.0 * 1024.0;
    let segments = count1 - count0;
    let load_wal = (bytes1.saturating_sub(bytes0)) as f64;
    let note = format!(
        "pgbackrest={version}; scratch source 768 MiB / 1 CPU, repo on a host bind; cluster {cluster:.0} B incl. the \
         dev copy {devcopy:.0} B from {} ({errors})",
        dump.file_name()
            .map(|f| f.to_string_lossy())
            .unwrap_or_default()
    );
    measure("m1", "full_backup_s", "s", &full, &note);
    let f = measure(
        "m1",
        "set_repo_bytes",
        "bytes",
        &set,
        "F of D-V: info backup[].info.repository.size, zst + aes-256-cbc",
    );
    measure(
        "m1",
        "verify_full_s",
        "s",
        &verify,
        "verify --set, the arm's pass rule",
    );
    measure(
        "m1",
        "pitr_restore_s",
        "s",
        &restores,
        "restore pitr --target end receipt restore_s: restore + socket boot + quarantine + TCP boot; source stopped",
    );
    measure(
        "m1",
        "expire_s",
        "s",
        &expire,
        "expire after each verified full; run 3 removed the oldest set",
    );
    // W: an upper bound for one paying user = the archived bytes of rewriting the whole dev copy once per day plus
    // 24 forced switches (SP-9); no production cluster of this system exists yet (re-measure after go-live).
    let w = load_wal + 24.0 * 692.0;
    measure(
        "m3",
        "wal_bytes_per_day",
        "bytes",
        &[w],
        &format!(
            "upper bound: one full rewrite of the dev copy per day (pg_restore {load_s:.1} s, {segments:.0} segments \
             = {:.0} B raw archived as {load_wal:.0} B, ratio {:.3}) + 24 x SP-9 692 B",
            segments * mib16,
            load_wal / (segments * mib16).max(1.0)
        ),
    );
    measure(
        "m3",
        "drill_check_wait_s",
        "s",
        &checks,
        "pgbackrest check in production pg (the drill's archive_wait_seconds)",
    );
    let peak = 3.0 * f + 2.0 * w;
    let max = 8.0 * 1024.0 * 1024.0 * 1024.0;
    measure(
        "m5",
        "peak_bytes",
        "bytes",
        &[peak],
        &format!(
            "(N+1)F + N W with N=2, F=median set_repo_bytes, W=m3 bound; REPO_MAX_BYTES 8 GiB = {max:.0} B, \
             headroom {:.0} B",
            max - peak
        ),
    );
}

// ============================================================================
// M2 — rebuild of a dev-sized collection from stored vectors (PG throwaway + scratch Qdrant)
// ============================================================================

const POINTS: usize = 17_435;
const DIM: u32 = 1024;
const PER_EVIDENCE: usize = 50;
const LABEL: &str = "embed-v1";

/// A deterministic 1024-dim vector per memory id (xorshift): the seed's provider stand-in. The rebuild itself runs
/// with the closed no-provider deps, so this embedder is never reached after the seed.
struct Det;

#[async_trait::async_trait]
impl CardEmbedder for Det {
    async fn embed_cards(
        &self,
        _tenant_id: TenantId,
        dimension: u32,
        _cards: &[SealedRetrievalCard],
        memory_ids: &[Uuid],
    ) -> Result<Vec<Vec<f32>>, ErrorCode> {
        Ok(memory_ids
            .iter()
            .map(|id| {
                let mut s = id.as_u128() | 1;
                (0..dimension)
                    .map(|_| {
                        s ^= s << 13;
                        s ^= s >> 7;
                        s ^= s << 17;
                        #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
                        let x = (s as u32) as f32 / u32::MAX as f32 - 0.5;
                        x
                    })
                    .collect()
            })
            .collect())
    }
}

fn role_dsn(h: &Handle, role: &str) -> String {
    let sep = if h.dsn.contains('?') { '&' } else { '?' };
    format!("{}{sep}options=-c%20role%3D{role}", h.dsn)
}

/// One tenant with `n` TENANT_SHARED memories (50 per Evidence) projected the production way into a 1024-dim
/// collection, its stream serving. Returns the stream.
fn seed_tenant(h: &mut Handle, face: &QdrantFace, n: usize, tag: &str) -> Stream {
    let collection = format!("c37_m2_{}", Uuid::new_v4().simple());
    h.rt.block_on(provisioning::ensure_collection(face, &collection, DIM))
        .expect("1024-dim collection");
    h.admin
        .execute(
            "INSERT INTO projection.tenant_placements (tenant_id, projection_family, collection_name, placement_class) \
             VALUES ($1, 'private_memory_v1', $2, 'SHARED_FALLBACK')",
            &[&h.tenant_id, &collection],
        )
        .expect("placement row");
    h.rt.block_on(bind_embedding_fingerprint(
        &h.retrieval,
        LABEL,
        &worker_fingerprint_inputs("c37-measure", "c37-model", "r1", DIM, "v1"),
    ))
    .expect("label binding");
    let ws = h.workspace();
    let mut left = n;
    let mut k = 0;
    while left > 0 {
        let m = left.min(PER_EVIDENCE);
        let e = h.evidence(ws, &format!("m2 {tag} evidence {k}"));
        for j in 0..m {
            h.memory(e, &format!("m2 {tag} memory {k} {j}"), TENANT_SHARED);
        }
        left -= m;
        k += 1;
    }
    h.admin
        .execute(
            "UPDATE ops.outbox SET status = 'DONE' WHERE tenant_id = $1 \
               AND event_type = 'EVIDENCE_ACCEPTED' AND status IN ('PENDING', 'PROCESSING')",
            &[&h.tenant_id],
        )
        .expect("distill settled");
    // The fixture's deps hold a 300 s permit, so each pass gets fresh deps; a pass of 10 tickets stays far inside it.
    let deadline = Instant::now() + Duration::from_secs(3600);
    loop {
        let mut deps = h.deps(ws, h.transport.clone());
        deps.placement.collection_name = collection.clone();
        deps.dimension = DIM;
        deps.embedder = Arc::new(Det);
        // dep: Qdrant(*) — the seed's real projector upserts into the scratch collection
        let o = h.rt.block_on(run_once(&deps, 10)).expect("run_once");
        assert_eq!(o.failed, 0, "{o:?}");
        let issued: i64 = h
            .admin
            .query_one(
                "SELECT count(*) FROM projection.stream_log WHERE tenant_id = $1 AND state = 'ISSUED'",
                &[&h.tenant_id],
            )
            .expect("issued")
            .get(0);
        if issued == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the seed did not drain within an hour"
        );
        if o.done == 0 {
            std::thread::sleep(Duration::from_secs(1));
        }
    }
    h.admin
        .execute(
            "UPDATE projection.stream_checkpoints SET serving = true WHERE tenant_id = $1 AND scope_id = $2",
            &[&h.tenant_id, &ws],
        )
        .expect("serving");
    let live: i64 = h
        .admin
        .query_one(
            "SELECT count(*) FROM projection.private_memory_points p JOIN projection.memory_vectors v \
               ON v.tenant_id = p.tenant_id AND v.memory_id = p.memory_id AND v.fingerprint_sha256 = p.fingerprint_sha256 \
              AND v.input_sha256 = p.input_sha256 \
             WHERE p.tenant_id = $1 AND p.projection_live AND v.vector IS NOT NULL",
            &[&h.tenant_id],
        )
        .expect("stored vectors")
        .get(0);
    assert_eq!(
        usize::try_from(live).ok(),
        Some(n),
        "every point stored its vector"
    );
    Stream {
        key: governance_ops::stream(h.tenant_id, ws),
        collection,
    }
}

/// `(issue_s, project_s, verify_s)` of one rebuild of `stream`: open + issue, the in-process run-scoped projector
/// with the closed no-provider deps, verify (asserted equivalent, zero provider calls) and close.
fn rebuild_once(h: &Handle, face: &QdrantFace, stream: &Stream) -> (f64, f64, f64, i64) {
    // dep: PostgreSQL(role_retrieval_worker) — the verifier's reader and the pass's pool
    let reader =
        h.rt.block_on(RetrievalWorkerDbPool::connect(&role_dsn(
            h,
            "role_retrieval_worker",
        )))
        .expect("reader");
    let worker =
        h.rt.block_on(rebuild::worker_label(&h.maintenance, LABEL))
            .expect("label bound");
    let deps = RebuildDeps {
        maintenance: &h.maintenance,
        reader: &reader,
        qdrant: face,
        worker: &worker,
    };
    let registry = h.registry.clone();
    // dep: PostgreSQL(role_retrieval_worker) — the run-scoped pass's own pool
    let pass_pool =
        h.rt.block_on(RetrievalWorkerDbPool::connect(&role_dsn(
            h,
            "role_retrieval_worker",
        )))
        .expect("pass pool");
    let (shared, refusing) = rebuild::drill_projection_deps(ClosedDeps {
        pool: pass_pool,
        scanner: h.scanner.clone(),
        transport: h.transport.clone(),
        mint_permit: Arc::new(move || {
            humaux_infra_cell::authorize_cell_access(
                &registry,
                humaux_infra_cell::IntraCellResource::QDRANT_REST,
                Duration::from_secs(300),
            )
            .ok()
        }),
        embedding_version: LABEL.to_owned(),
        dimension: DIM,
        processor_id: ProcessorId(Uuid::from_u128(0x0c37_0007)),
        sleep: Arc::new(|period| Box::pin(tokio::time::sleep(period))),
    });
    let cfg = PassConfig {
        claim: ClaimFamily::of(RetrievalFamily::PrivateMemoryV1).expect("ticket family"),
        lease_owner: format!("c37-measure/{}", Uuid::now_v7()),
        lease_secs: 60.0,
        batch: 50,
        per_tenant_cap: 50,
        max_attempts: 3,
        backoff: Backoff {
            base_secs: 1.0,
            max_secs: 2.0,
        },
    };
    h.rt.block_on(async {
        let t = Instant::now();
        let precheck = rebuild::precheck(&deps, stream, None)
            .await
            .expect("precheck");
        assert_eq!(precheck, Ok(0), "no legacy point");
        let run = rebuild::open_run(&deps, stream).await.expect("open");
        provisioning::ensure_collection(face, &stream.collection, DIM)
            .await
            .expect("collection");
        let issued = rebuild::issue_tickets(&deps, stream, &run, 1000, false)
            .await
            .expect("issue");
        let issue_s = secs(t);
        let only = OnlyRun {
            tenant_id: stream.key.tenant_id.0,
            run_id: run.run_id,
        };
        let pump = || -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
            Box::pin(async {
                run_claimed_pass_for_run(&shared, &cfg, only)
                    .await
                    .expect("pass");
            })
        };
        let t = Instant::now();
        let settled =
            rebuild::wait_generation(&deps, stream, &run, Duration::from_secs(1800), &pump)
                .await
                .expect("wait");
        let project_s = secs(t);
        assert!(settled, "the generation settled");
        let t = Instant::now();
        let (report, _) = rebuild::verify_run(&deps, stream, &run, false)
            .await
            .expect("verify");
        let verify_s = secs(t);
        assert_eq!(report.verdict, Verdict::Equivalent, "{}", report.json);
        rebuild::close_run(&deps, stream, &run, &report)
            .await
            .expect("close");
        assert_eq!(refusing.attempts(), 0, "no provider call");
        (issue_s, project_s, verify_s, issued)
    })
}

/// M2 (×4): 17,435 synthetic 1024-dim points across two tenants (vectors stored the production way), then three
/// rebuilds of both streams from the stored vectors; each round's times are summed over the two streams.
#[test]
#[ignore = "lane(c) ADR-0064 measurement on a dev-sized scratch copy that no lane resource provisions, run once and pasted into the ADR as MEASURE lines; never in the chain or the lane: HUMAUX_REQUIRE_DB=1 HUMAUX_REQUIRE_DOCKER=1 -- --ignored --exact m2_rebuild_of_dev_sized_stored_vectors --nocapture"]
#[allow(clippy::cast_precision_loss)]
fn m2_rebuild_of_dev_sized_stored_vectors() {
    let test = "m2_rebuild_of_dev_sized_stored_vectors";
    // Drop order (reverse of declaration): the handles and their pools, then the database, then the container.
    let qdrant =
        scratch_qdrant::ScratchQdrant::start("m2").unwrap_or_else(|e| panic!("{test}: {e}"));
    let Some(db) = throwaway::db(test, "c37_m2") else {
        return;
    };
    let dsn = throwaway::with_db(&db.owner_dsn, &db.name);
    let face = QdrantFace::new("127.0.0.1", qdrant.port, "127.0.0.1/32").expect("face");
    let t = Instant::now();
    let mut tenants = Vec::new();
    for (tag, n) in [("a", POINTS - POINTS / 2), ("b", POINTS / 2)] {
        let mut h = Handle::in_throwaway_at(dsn.clone(), Box::new(()), qdrant.port)
            .unwrap_or_else(|e| panic!("a2 fixture: {e:?}"));
        let stream = seed_tenant(&mut h, &face, n, tag);
        tenants.push((h, stream));
    }
    let seed_s = secs(t);
    let (mut issue, mut project, mut verify, mut per_row) = (vec![], vec![], vec![], vec![]);
    let mut issued = 0;
    for _ in 0..3 {
        let (mut i, mut p, mut v) = (0.0, 0.0, 0.0);
        issued = 0;
        for (h, stream) in &tenants {
            let r = rebuild_once(h, &face, stream);
            i += r.0;
            p += r.1;
            v += r.2;
            issued += r.3;
        }
        issue.push(i);
        project.push(p);
        verify.push(v);
        // dep: PostgreSQL(owner) — the vector table's size per row (no tenant filter: both tenants)
        let mut c = postgres::Client::connect(&dsn, postgres::NoTls).expect("owner");
        per_row.push(
            c.query_one(
                "SELECT pg_total_relation_size('projection.memory_vectors')::float8 / count(*) \
                 FROM projection.memory_vectors",
                &[],
            )
            .expect("vector bytes")
            .get::<_, f64>(0),
        );
    }
    let note = format!(
        "{POINTS} points x {DIM} dims, two tenants, {issued} generation tickets per round (one per Evidence of \
         {PER_EVIDENCE}); throwaway DB on the dev cluster, scratch Qdrant 512 MiB / 1 CPU; seed {seed_s:.0} s"
    );
    measure("m2", "rebuild_issue_s", "s", &issue, &note);
    measure(
        "m2",
        "rebuild_project_s",
        "s",
        &project,
        "in-process run-scoped pass, stored vectors, NoProviderEmbedder attempts 0",
    );
    measure(
        "m2",
        "verify_s",
        "s",
        &verify,
        "E1-E5 incl. per-point vector compare, verdict equivalent",
    );
    measure(
        "m2",
        "vectors_bytes_per_row",
        "bytes",
        &per_row,
        "pg_total_relation_size(projection.memory_vectors) / rows, 1024 float4",
    );
    drop(tenants);
    drop(db);
    drop(qdrant);
}
