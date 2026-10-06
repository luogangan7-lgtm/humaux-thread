//! `maintenance::drill` — the one-shot arms `restore drill`, `restore drill --destroy-stale` and `restore pitr`
//!   (ADR-0064 D-L, D-M, D-N, D-O, D-U as amended by section 10.4 S5 and 10.11 B, H; ruling E17: the one local
//!   posix repository, class `local_only`). The drill restores the newest VERIFIED set to its own witnessed T in an
//!   isolated compose project `humaux-drill-<id>` from `deploy/compose/drill.yml`, neutralises the restored
//!   credentials, runs the D-N checks, rebuilds the drill Qdrant from the restored PostgreSQL with no provider,
//!   destroys only its own labelled resources and writes the receipt (source) and the evidence file. `restore pitr`
//!   restores into a given project (the real restore) and quarantines the worker roles.
//! Depends-on: crates=[humaux-adapters, humaux-domain, rand, serde_json, time, tokio, uuid];
//!   services=[PostgreSQL(role_maintenance), PostgreSQL(role_gateway), PostgreSQL(role_retrieval_worker),
//!   PostgreSQL(owner) r=[ops.jobs, ops.model_call_ledger, ops.schema_migrations, projection.stream_log], Qdrant(*),
//!   subprocess(docker)];
//!   env=[HUMAUX_DRILL_ID, HUMAUX_DRILL_PG_LISTEN_ADDRESSES, HUMAUX_MAINTENANCE_BACKUP_COMPOSE_FILE, HUMAUX_MAINTENANCE_BACKUP_PROJECT,
//!   HUMAUX_MAINTENANCE_DRILL_COMPOSE_FILE, HUMAUX_MAINTENANCE_DRILL_MIN_FREE_MEMORY_BYTES,
//!   HUMAUX_MAINTENANCE_DRILL_RESTORE_TIMEOUT_SECONDS, HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_MAINTENANCE_QDRANT_HOST,
//!   HUMAUX_MAINTENANCE_QDRANT_PORT, HUMAUX_MAINTENANCE_RESTORE_MIN_FREE_DISK_BYTES, HUMAUX_MIGRATOR_PG_DSN,
//!   HUMAUX_PG_LISTEN_ADDRESSES, HUMAUX_PG_REPO_DIR, HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN, HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256,
//!   HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION];
//!   modules=[adapters::maintenance_repo, adapters::postgres, adapters::projection_worker, adapters::provisioning,
//!   adapters::qdrant, adapters::rebuild, adapters::stream_repo, domain::egress, maintenance::backup,
//!   maintenance::main]
//! Called-by: [maintenance::main]
//! Invariants: [every refusal (exit 3) happens before any project exists; the drill mounts exactly one thing it did
//!   not create (the repository bind, read-only, never created on the host) and refuses `drill_repo_writable` unless
//!   a write probe through that mount gets EROFS; destroy is label-scoped (`com.docker.compose.project` =
//!   `humaux-drill-<id>` or `humaux.drill` = `<id>`), never a name prefix, never prune, and a drop guard runs it on
//!   every path after `up`; the receipt is written AFTER destroy and residue; success is derived by the table and
//!   claimed only when every check passed; the restored cluster listens on its socket only until every password is
//!   neutralised; the drill's projector is the closed no-provider deps and claims only its own run's tickets; no
//!   DSN, password or cipher pass is printed, logged or written to the evidence; `HUMAUX_MIGRATOR_PG_DSN` present =
//!   refused]
//! Spec: Baseline §44; §67.2; §78.1; ADR-0053 D-F; ADR-0064 D-L; ADR-0064 D-M; ADR-0064 D-N; ADR-0064 D-O;
//!   ADR-0064 D-U; ADR-0064 E17
//!
//! Exit codes (ADR-0053 D-F): `restore drill` 0 succeeded, 1 the drill ran and failed (a receipt with
//! `succeeded = false` is written) or infrastructure, 3 refused before step 3; `restore pitr` 0 restored, 1 failed,
//! 3 refused before anything was restored; 2 usage.
//!
//! Spike facts (ADR-0064 `MEASURE sp<n>`): restore and `archive-get` work from a read-only bind and a write probe
//! gets EROFS (SP-5, so no `cp -a` copy and `repo_copy = 0` in the floor); the image's `postgres` is 999:999 (SP-13).

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io::Write as _;
use std::pin::Pin;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use humaux_adapters::maintenance_repo::{self as repo, DrillReceipt, TenantFacts, VerifiedSet};
use humaux_adapters::postgres::{MaintenanceDbPool, RetrievalWorkerDbPool, RuntimeDbPool};
use humaux_adapters::projection_worker::{
    PassConfig, SharedProjectionDeps, run_claimed_pass_for_run,
};
use humaux_adapters::provisioning::{self, ProvisioningError, QdrantFace};
use humaux_adapters::qdrant::RetrievalFamily;
use humaux_adapters::rebuild::{self, RebuildDeps, ScannerPin, Stream, Verdict};
use humaux_adapters::stream_repo::{Backoff, ClaimFamily, OnlyRun};
use humaux_domain::egress::ProcessorId;
use serde_json::{Map, Value, json};
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};
use uuid::Uuid;

use crate::backup::{self, NOT_OFFSITE_NOTICE, Pg, REPO_CLASS, STANZA};
use crate::{Args, Failure, Output, Result, env, env_parsed, pool};

const COMPOSE_FILE: &str = "HUMAUX_MAINTENANCE_DRILL_COMPOSE_FILE";
const MIN_FREE_MEMORY: &str = "HUMAUX_MAINTENANCE_DRILL_MIN_FREE_MEMORY_BYTES";
const RESTORE_TIMEOUT: &str = "HUMAUX_MAINTENANCE_DRILL_RESTORE_TIMEOUT_SECONDS";
/// 10.11 B/H: one floor for the drill and `restore pitr` (the production disk keeps this much free).
const MIN_FREE_DISK: &str = "HUMAUX_MAINTENANCE_RESTORE_MIN_FREE_DISK_BYTES";
/// card 36 D-H: the drill never needs an owner credential; its presence in the environment is a refusal.
const MIGRATOR_DSN: &str = "HUMAUX_MIGRATOR_PG_DSN";
const BACKUP_COMPOSE_FILE: &str = "HUMAUX_MAINTENANCE_BACKUP_COMPOSE_FILE";
const BACKUP_PROJECT: &str = "HUMAUX_MAINTENANCE_BACKUP_PROJECT";

/// The label every drill container, volume and network carries (`deploy/compose/drill.yml`).
const DRILL_LABEL: &str = "humaux.drill";
/// The repository filesystem's mount point in every container (the compose bind target).
const REPO_MOUNT: &str = "/var/lib/pgbackrest";
/// `pg1-path` in the conf: the base image's PGDATA.
const PG1_PATH: &str = "/var/lib/postgresql/18/docker";
/// D-M: roles whose passwords the drill replaces before TCP opens; every other LOGIN role becomes NOLOGIN.
const DRILL_LOGIN_ROLES: [&str; 4] = [
    "postgres",
    "role_maintenance",
    "role_retrieval_worker",
    "role_gateway",
];
/// D-U: the roles that keep LOGIN after a real restore (the rebuild runs before traffic); every other LOGIN role
/// is quarantined NOLOGIN until the operator reconciles.
const PITR_LOGIN_ROLES: [&str; 3] = ["postgres", "role_maintenance", "role_retrieval_worker"];
/// D-M: the drill's egress identity. The closed no-provider deps never disclose anything, so no ledger row carries it.
const DRILL_PROCESSOR: Uuid = Uuid::from_u128(0x0c37_d211_0000_0000_0000_0000_0000_0001);
/// The drill's in-process pass: one claimer, its own run's tickets only (D-N(g)).
// ponytail: fixed knobs (issue/claim batch 50, lease 60 s, 3 attempts, 1–2 s backoff, 50 ms between passes) — the
// run-scoped pass has no competitor, so they only bound a stuck pass; read the worker's keys if a drill ever
// shares its tickets with a resident worker.
const PASS_BATCH: i32 = 50;
const PASS_LEASE_SECS: f64 = 60.0;
const PASS_MAX_ATTEMPTS: i32 = 3;
const PASS_BACKOFF_SECS: (f64, f64) = (1.0, 2.0);
const PASS_PAUSE: Duration = Duration::from_millis(50);
/// Poll step while waiting for promotion / readiness (inside the operator's restore timeout).
const POLL: Duration = Duration::from_millis(500);

/// A named failure of a drill step (`failure` of the receipt), or infrastructure.
type Step<T> = std::result::Result<T, String>;

fn infra<E: std::fmt::Display>(what: &str) -> impl FnOnce(E) -> String + '_ {
    move |e| format!("infra:{what}: {e}")
}

fn text(out: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// The last `n` lines of `s` (log tails in failure names; never a secret: passwords travel on stdin only).
fn tail(s: &str, n: usize) -> String {
    let lines: Vec<&str> = s.lines().collect();
    lines[lines.len().saturating_sub(n)..].join(" | ")
}

/// One docker CLI call; `Err` carries its output.
fn docker(args: &[&str]) -> Step<String> {
    // dep: subprocess(docker) — one docker CLI call
    let out = Command::new("docker")
        .args(args)
        .output()
        .map_err(infra("docker"))?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
    } else {
        Err(format!("docker {}: {}", args.join(" "), text(&out).trim()))
    }
}

/// A random password for one neutralised role: 32 hex characters (DSN-safe), held in memory only.
fn secret() -> String {
    format!("{:032x}", rand::random::<u128>())
}

/// `YYYY-MM-DD HH:MM:SS.ffffff+00`, the recovery target form pgBackRest and PostgreSQL both accept.
fn pg_time(t: OffsetDateTime) -> String {
    let t = t.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}+00",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second(),
        t.microsecond()
    )
}

fn unix(t: Option<OffsetDateTime>) -> Value {
    t.map_or(Value::Null, |t| json!(t.unix_timestamp()))
}

/// `(host:port, database)` of a `postgres://user:pass@host:port/db?query` DSN (no secret leaves this function).
fn dsn_target(dsn: &str) -> Option<(String, String)> {
    let rest = dsn.split_once("://")?.1;
    let rest = rest.rsplit_once('@').map_or(rest, |(_, r)| r);
    let (hostport, path) = rest.split_once('/')?;
    let db = path.split('?').next()?.to_owned();
    Some((hostport.to_owned(), db))
}

/// The fields every arm prints (ADR-0064 10.2), the drill and `restore pitr` included.
fn class_fields(arm: &str) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("arm".to_owned(), json!(arm));
    m.insert("repo_class".to_owned(), json!(REPO_CLASS));
    m.insert("notice".to_owned(), json!(NOT_OFFSITE_NOTICE));
    m
}

fn refused(arm: &str, id: Option<Uuid>, reason: String) -> Output {
    let mut m = class_fields(arm);
    m.insert("drill_id".to_owned(), json!(id));
    m.insert("outcome".to_owned(), json!("refused"));
    m.insert("reason".to_owned(), json!(reason));
    Output {
        receipt: Value::Object(m),
        refused: true,
        failed: false,
        once: Vec::new(),
    }
}

/// `--evidence <file>` whose directory exists, checked before any work: the evidence is written after the receipt,
/// so a missing directory found only then would leave a success receipt whose evidence is lost (runbook §11.1).
fn evidence_arg(args: &Args) -> Result<String> {
    let path = args.required("--evidence")?;
    let dir = match std::path::Path::new(&path).parent() {
        Some(d) if !d.as_os_str().is_empty() => d.to_path_buf(),
        _ => std::path::PathBuf::from("."),
    };
    if dir.is_dir() {
        Ok(path)
    } else {
        Err(Failure::Usage(format!(
            "--evidence {path}: directory {} does not exist",
            dir.display()
        )))
    }
}

/// Writes `value` pretty-printed to `path`, mode 0600 (the evidence of D-O; it carries no secret).
fn write_evidence(path: &str, value: &Value) -> Result<()> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let body = serde_json::to_string_pretty(value)
        .map_err(|e| Failure::Infra(format!("evidence json: {e}")))?;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| Failure::Infra(format!("evidence {path}: {e}")))?;
    f.set_permissions(std::fs::Permissions::from_mode(0o600))
        .and_then(|()| f.write_all(body.as_bytes()))
        .and_then(|()| f.write_all(b"\n"))
        .map_err(|e| Failure::Infra(format!("evidence {path}: {e}")))
}

// ============================================================================
// Compose projects and labelled one-shots
// ============================================================================

/// A compose project addressed by file and name; `env` adds the variables this process sets (the drill id and the
/// listen address), everything else is interpolated from the inherited environment (dr.env).
struct Project {
    file: String,
    name: String,
    env: Vec<(&'static str, String)>,
}

impl Project {
    fn run(&self, listen: &str, args: &[&str], stdin: Option<&str>) -> Step<std::process::Output> {
        // dep: subprocess(docker) — compose against this project only
        let mut cmd = Command::new("docker");
        cmd.args(["compose", "-f", &self.file, "-p", &self.name])
            .args(args)
            .envs(self.env.iter().map(|(k, v)| (*k, v)))
            .env(self.listen_key(), listen);
        let out = match stdin {
            None => cmd.output().map_err(infra("docker compose"))?,
            Some(input) => {
                let mut child = cmd
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .map_err(infra("docker compose"))?;
                if let Some(mut pipe) = child.stdin.take() {
                    pipe.write_all(input.as_bytes())
                        .map_err(infra("docker compose stdin"))?;
                }
                child.wait_with_output().map_err(infra("docker compose"))?
            }
        };
        Ok(out)
    }

    /// The listen-address variable of the file (`drill.yml`: the drill's; `backup.yml`: production's).
    fn listen_key(&self) -> &'static str {
        if self.env.iter().any(|(k, _)| *k == "HUMAUX_DRILL_ID") {
            "HUMAUX_DRILL_PG_LISTEN_ADDRESSES"
        } else {
            "HUMAUX_PG_LISTEN_ADDRESSES"
        }
    }

    fn ok(&self, listen: &str, args: &[&str]) -> Step<String> {
        let out = self.run(listen, args, None)?;
        if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
        } else {
            Err(format!(
                "docker compose {}: {}",
                args.first().unwrap_or(&""),
                tail(&text(&out), 8)
            ))
        }
    }

    /// `psql` as the image superuser over the restored cluster's socket (local trust), SQL on stdin.
    fn psql(&self, listen: &str, sql: &str) -> Step<String> {
        self.psql_in(listen, "postgres", sql)
    }

    /// [`Project::psql`] in database `db`.
    fn psql_in(&self, listen: &str, db: &str, sql: &str) -> Step<String> {
        let out = self.run(
            listen,
            &[
                "exec",
                "-T",
                "--user",
                "postgres",
                "pg",
                "psql",
                "-qAt",
                "-v",
                "ON_ERROR_STOP=1",
                "-d",
                db,
            ],
            Some(sql),
        )?;
        if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
        } else {
            Err(format!(
                "psql: {}",
                tail(&String::from_utf8_lossy(&out.stderr), 4)
            ))
        }
    }

    /// The `pg` image and the repository bind (source, read-only) as this file declares them
    /// (`docker compose config`, which creates nothing).
    fn mounts(&self, listen: &str) -> Step<Mounts> {
        let raw = self.ok(listen, &["config", "--format", "json"])?;
        let config: Value = serde_json::from_str(&raw).map_err(infra("compose config json"))?;
        let pg = &config["services"]["pg"];
        let image = pg["image"]
            .as_str()
            .ok_or_else(|| "infra:compose config: services.pg.image".to_owned())?
            .to_owned();
        let volume = |target: &str| {
            pg["volumes"]
                .as_array()
                .and_then(|v| v.iter().find(|m| m["target"] == target))
                .cloned()
        };
        let repo = volume(REPO_MOUNT)
            .ok_or_else(|| format!("infra:compose config: no bind at {REPO_MOUNT}"))?;
        let source = |m: &Value| m["source"].as_str().map(str::to_owned);
        Ok(Mounts {
            image,
            repo: source(&repo).ok_or_else(|| "infra:compose config: repo source".to_owned())?,
            repo_read_only: repo["read_only"].as_bool().unwrap_or(false),
        })
    }

    /// `docker compose port <service> <port>` → the published loopback port.
    fn port(&self, listen: &str, service: &str, port: &str) -> Step<u16> {
        let out = self.ok(listen, &["port", service, port])?;
        out.rsplit(':')
            .next()
            .and_then(|p| p.trim().parse().ok())
            .ok_or_else(|| format!("infra:compose port {service}: {out}"))
    }
}

impl Project {
    /// Waits until the restored cluster has promoted (`pg_is_in_recovery() = false`) within `timeout`. A `pg` that
    /// exits (PG18 refuses to start when recovery ends before its target, e.g. a corrupt or missing segment) fails
    /// `restore_failed`.
    async fn wait_promoted(&self, listen: &str, timeout: Duration) -> Step<()> {
        let started = Instant::now();
        loop {
            let running = self.ok(listen, &["ps", "-q", "--status", "running", "pg"])?;
            if running.is_empty() {
                let logs = self
                    .ok(listen, &["logs", "--no-color", "--tail", "12", "pg"])
                    .unwrap_or_default();
                return Err(format!("restore_failed: pg exited: {}", tail(&logs, 6)));
            }
            if let Ok(answer) = self.psql(listen, "SELECT pg_is_in_recovery()")
                && answer == "f"
            {
                return Ok(());
            }
            if started.elapsed() >= timeout {
                return Err("restore_timeout".to_owned());
            }
            tokio::time::sleep(POLL).await;
        }
    }
}

/// What a project mounts into `pg`.
struct Mounts {
    image: String,
    repo: String,
    repo_read_only: bool,
}

impl Mounts {
    /// One labelled `docker run --rm` of the pg image as `postgres`, the repository mounted exactly as the compose
    /// file declares it (D-M: the one-shot sees what the drill would). Its output on success.
    fn oneshot(&self, label: &str, script: &str) -> Step<String> {
        let repo = format!(
            "type=bind,source={},target={REPO_MOUNT}{}",
            self.repo,
            if self.repo_read_only { ",readonly" } else { "" }
        );
        docker(&[
            "run",
            "--rm",
            "--label",
            label,
            "--memory",
            "256m",
            "--cpus",
            "1",
            "--user",
            "postgres",
            "--mount",
            &repo,
            "--entrypoint",
            "bash",
            &self.image,
            "-c",
            script,
        ])
    }

    /// 10.11 H `repo_intact` digest: sha256 of the sorted `backup/` tree listing (names and sizes) plus both info
    /// files' sums. `Err` when either info file is missing or unreadable.
    fn repo_digest(&self, label: &str) -> Step<String> {
        self.oneshot(
            label,
            &format!(
                "set -o pipefail; cd {REPO_MOUNT} && {{ find repo/backup -type f -printf '%P %s\\n' | LC_ALL=C sort; \
                 sha256sum repo/backup/{STANZA}/backup.info repo/archive/{STANZA}/archive.info; }} | sha256sum | cut -c1-64"
            ),
        )
    }
}

// ============================================================================
// Destroy (D-O): label-scoped, never a prefix, never prune
// ============================================================================

/// Every resource of drill `id`: compose-project label OR drill label, by exact value.
fn drill_filters(id: &str) -> [String; 2] {
    [
        format!("label=com.docker.compose.project=humaux-drill-{id}"),
        format!("label={DRILL_LABEL}={id}"),
    ]
}

/// `(kind, name)` of every container, volume and network matching `filter`.
fn listed(filter: &str) -> Step<Vec<(&'static str, String)>> {
    let mut found = Vec::new();
    for (kind, args) in [
        ("container", vec!["ps", "-a", "--format", "{{.Names}}"]),
        ("volume", vec!["volume", "ls", "--format", "{{.Name}}"]),
        ("network", vec!["network", "ls", "--format", "{{.Name}}"]),
    ] {
        let mut all = args.clone();
        all.extend_from_slice(&["--filter", filter]);
        for name in docker(&all)?.lines().filter(|l| !l.trim().is_empty()) {
            found.push((kind, name.trim().to_owned()));
        }
    }
    Ok(found)
}

/// The resources of drill `id`, deduplicated.
fn drill_resources(id: &str) -> Step<BTreeSet<(&'static str, String)>> {
    let mut all = BTreeSet::new();
    for filter in drill_filters(id) {
        all.extend(listed(&filter)?);
    }
    Ok(all)
}

/// ADR-0064 D-O destroy: containers (with their anonymous volumes), then volumes, then networks of drill `id`
/// only. Returns what it could not remove.
fn destroy(id: &str) -> Vec<String> {
    let mut errors = Vec::new();
    let found = match drill_resources(id) {
        Ok(found) => found,
        Err(e) => return vec![e],
    };
    for kind in ["container", "volume", "network"] {
        for (_, name) in found.iter().filter(|(k, _)| *k == kind) {
            let args: Vec<&str> = match kind {
                "container" => vec!["rm", "-f", "-v", name],
                "volume" => vec!["volume", "rm", "-f", name],
                _ => vec!["network", "rm", name],
            };
            if let Err(e) = docker(&args) {
                errors.push(e);
            }
        }
    }
    errors
}

/// Removes drill `id`'s resources if dropped armed (every error and panic path after `up`).
struct DestroyGuard {
    id: String,
    armed: bool,
}

impl Drop for DestroyGuard {
    fn drop(&mut self) {
        if self.armed {
            for e in destroy(&self.id) {
                eprintln!("humaux-maintenance restore drill: destroy: {e}");
            }
        }
    }
}

/// `restore drill --destroy-stale`: D-O's destroy for every `humaux.drill` label value found; nothing else.
fn destroy_stale() -> Result<Output> {
    let mut ids = BTreeSet::new();
    for (kind, args) in [
        ("container", vec!["ps", "-a"]),
        ("volume", vec!["volume", "ls"]),
        ("network", vec!["network", "ls"]),
    ] {
        let mut all = args.clone();
        let format = format!("{{{{.Label \"{DRILL_LABEL}\"}}}}");
        let filter = format!("label={DRILL_LABEL}");
        all.extend_from_slice(&["--filter", &filter, "--format", &format]);
        let out = docker(&all).map_err(|e| Failure::Infra(format!("{kind} list: {e}")))?;
        ids.extend(
            out.lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(str::to_owned),
        );
    }
    let mut m = class_fields("restore drill --destroy-stale");
    let mut errors = Vec::new();
    for id in &ids {
        errors.extend(destroy(id));
    }
    m.insert("destroyed_drill_ids".to_owned(), json!(ids));
    m.insert("errors".to_owned(), json!(errors));
    m.insert(
        "outcome".to_owned(),
        json!(if errors.is_empty() {
            "destroyed"
        } else {
            "failed"
        }),
    );
    Ok(Output {
        failed: !errors.is_empty(),
        receipt: Value::Object(m),
        refused: false,
        once: Vec::new(),
    })
}

// ============================================================================
// restore drill
// ============================================================================

/// `restore drill --evidence <file> [--drill-id <uuid>]` | `restore drill --destroy-stale` (ADR-0064 D-L).
#[allow(clippy::too_many_lines)] // one ordered list of step-0 refusals (D-L): the order is the contract
pub(crate) async fn drill(args: &Args) -> Result<Output> {
    if args.0.iter().any(|a| a == "--destroy-stale") {
        return destroy_stale();
    }
    let evidence = evidence_arg(args)?;
    let id: Uuid = match args.get("--drill-id") {
        Some(_) => args.parsed("--drill-id")?,
        None => Uuid::now_v7(),
    };
    const ARM: &str = "restore drill";

    // Step 0: refusals before the project (exit 3).
    if std::env::var_os(MIGRATOR_DSN).is_some() {
        return Ok(refused(
            ARM,
            Some(id),
            format!(
                "migrator_dsn_present: {MIGRATOR_DSN} is set; the drill never needs an owner credential"
            ),
        ));
    }
    let source = pool().await?;
    let Some(set) = repo::newest_verified_set(&source)
        .await
        .map_err(|e| Failure::Infra(format!("backup receipts: {e}")))?
    else {
        return Ok(refused(ARM, Some(id), "no_verified_backup".to_owned()));
    };
    let stale = listed(&format!("label={DRILL_LABEL}")).map_err(Failure::Infra)?;
    if !stale.is_empty() {
        let names: Vec<String> = stale.iter().map(|(k, n)| format!("{k} {n}")).collect();
        return Ok(refused(
            ARM,
            Some(id),
            format!(
                "stale_drill: {}; run humaux-maintenance restore drill --destroy-stale",
                names.join(", ")
            ),
        ));
    }
    let project = Project {
        file: env(COMPOSE_FILE)?,
        name: format!("humaux-drill-{id}"),
        env: vec![("HUMAUX_DRILL_ID", id.to_string())],
    };
    let min_memory: u64 = env_parsed(MIN_FREE_MEMORY)?;
    let floor: u64 = env_parsed(MIN_FREE_DISK)?;
    let timeout = Duration::from_secs(env_parsed(RESTORE_TIMEOUT)?);
    let label = format!("{DRILL_LABEL}={id}");
    let mounts = project.mounts("").map_err(Failure::Infra)?;
    // D-M + 10.4 S5: the repository must be read-only to the drill; anything but EROFS on a write probe refuses.
    let probe = mounts
        .oneshot(
            &label,
            &format!(
                "P={REPO_MOUNT}/repo/.drill-probe-{id}; if touch $P 2>/tmp/e; then rm -f $P; echo probe=written; \
                 else cat /tmp/e; fi; awk '/^MemAvailable:/ {{print \"mem_kb=\" $2}}' /proc/meminfo; \
                 df -Pk / | awk 'NR==2 {{print \"disk_kb=\" $4}}'"
            ),
        )
        .map_err(Failure::Infra)?;
    if !probe.contains("Read-only file system") {
        return Ok(refused(
            ARM,
            Some(id),
            format!("drill_repo_writable: a write probe under {REPO_MOUNT}/repo did not get EROFS"),
        ));
    }
    let (memory, disk) = (
        probe_bytes(&probe, "mem_kb="),
        probe_bytes(&probe, "disk_kb="),
    );
    if memory < min_memory {
        return Ok(refused(
            ARM,
            Some(id),
            format!("drill_free_memory:{min_memory}/{memory}"),
        ));
    }
    // 10.11 H: need = restore size + vectors + repo copy (0: SP-5 restores from the ro bind) + the floor.
    let pg = Pg::from_env()?;
    let (restore, vectors) = restore_bytes(&pg, &source, &set).await?;
    let need = restore.saturating_add(vectors).saturating_add(floor);
    if need > disk {
        return Ok(refused(
            ARM,
            Some(id),
            format!(
                "drill_free_bytes:{need}/{disk} (restore={restore} vectors={vectors} repo_copy=0 floor={floor})"
            ),
        ));
    }

    let mut d = Drill {
        id,
        project,
        mounts,
        label,
        set,
        pg,
        source,
        db: dsn_target(&env("HUMAUX_MAINTENANCE_PG_DSN")?)
            .map(|(_, db)| db)
            .ok_or_else(|| Failure::Usage("HUMAUX_MAINTENANCE_PG_DSN: no database".to_owned()))?,
        timeout,
        r: DrillReceipt {
            drill_id: id,
            started_at: Some(OffsetDateTime::now_utc()),
            ..DrillReceipt::default()
        },
        ev: class_fields(ARM),
        phases: Map::new(),
    };
    d.r.backup_label = Some(d.set.label.clone());
    d.r.backup_manifest_sha256 = Some(d.set.manifest_sha256.clone());
    d.run(&evidence).await
}

/// `<key><kB>` of the step-0 one-shot's output, in bytes (0 when absent: a missing reading refuses).
fn probe_bytes(probe: &str, key: &str) -> u64 {
    probe
        .lines()
        .find_map(|l| l.strip_prefix(key))
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map_or(0, |k| k.saturating_mul(1024))
}

/// 10.11 H floor terms: the set's restore size from `info` and the live registry rows × (4 × dimension + 2,048).
async fn restore_bytes(
    pg: &Pg,
    source: &MaintenanceDbPool,
    set: &VerifiedSet,
) -> Result<(u64, u64)> {
    let info = pg.info()?;
    let restore = backup::set_of(&info, &set.label)
        .and_then(|s| s["info"]["size"].as_u64())
        .ok_or_else(|| Failure::Infra(format!("pgbackrest info: no size for {}", set.label)))?;
    let mut vectors: u64 = 0;
    for tenant in rebuild::all_tenants(source).await? {
        let facts = repo::tenant_facts(source, tenant, None)
            .await
            .map_err(|e| Failure::Infra(format!("tenant facts: {e}")))?;
        vectors = vectors.saturating_add(u64::try_from(facts.vector_bytes).unwrap_or(0));
    }
    Ok((restore, vectors))
}

/// One drill in progress: what it restores, where, and what it found so far.
struct Drill {
    id: Uuid,
    project: Project,
    mounts: Mounts,
    label: String,
    set: VerifiedSet,
    pg: Pg,
    source: MaintenanceDbPool,
    /// The source DSN's database (the restored cluster has the same).
    db: String,
    timeout: Duration,
    r: DrillReceipt,
    ev: Map<String, Value>,
    phases: Map<String, Value>,
}

/// The pools of the restored cluster (DSNs built from `docker compose port` and the neutralised passwords).
struct Restored {
    maintenance: MaintenanceDbPool,
    gateway: RuntimeDbPool,
    reader: RetrievalWorkerDbPool,
    /// The run-scoped pass's own pool (moved into the closed deps once).
    pass: Option<RetrievalWorkerDbPool>,
    qdrant_port: u16,
}

impl Drill {
    /// Records the first named failure (a later one never overwrites it).
    fn fail(&mut self, failure: impl Into<String>) {
        if self.r.failure.is_none() {
            self.r.failure = Some(failure.into());
        }
    }

    fn phase(&mut self, name: &str, since: Instant) {
        self.phases
            .insert(name.to_owned(), json!(since.elapsed().as_secs_f64()));
    }

    /// Steps 1–9: the receipt is written after destroy, always (once step 0 passed).
    async fn run(mut self, evidence: &str) -> Result<Output> {
        let mut guard = DestroyGuard {
            id: self.id.to_string(),
            armed: false,
        };
        let before = self.mounts.repo_digest(&self.label);
        if let Err(e) = self.pipeline(&mut guard).await {
            self.fail(e);
        }
        // Step 8: destroy (only this drill's labelled resources), then the repository digest, then residue.
        let t = Instant::now();
        let errors = destroy(&self.id.to_string());
        guard.armed = false;
        let after = self.mounts.repo_digest(&self.label);
        if let Err(e) = &before {
            eprintln!("humaux-maintenance restore drill: repo digest before: {e}");
        }
        if let Err(e) = &after {
            eprintln!("humaux-maintenance restore drill: repo digest after: {e}");
        }
        self.r.repo_intact = Some(matches!((&before, &after), (Ok(a), Ok(b)) if a == b));
        let residue = drill_resources(&self.id.to_string())
            .map(|r| r.len())
            .map_err(Failure::Infra)?;
        self.r.residue = Some(i32::try_from(residue).unwrap_or(i32::MAX));
        if !errors.is_empty() {
            self.fail(format!("destroy_failed: {}", errors.join("; ")));
        }
        self.phase("destroy_s", t);
        self.r.finished_at = Some(OffsetDateTime::now_utc());
        self.derive_failure();
        self.r.phase_seconds = Some(Value::Object(self.phases.clone()));

        // Step 9: the receipt in the source (success derived by the table), then the evidence file.
        let succeeded = repo::insert_drill_receipt(&self.source, &self.r)
            .await
            .map_err(|e| Failure::Infra(format!("restore_drills receipt: {e}")))?;
        let ev = self.evidence(succeeded);
        write_evidence(evidence, &ev)?;
        let mut out = class_fields("restore drill");
        out.insert("drill_id".to_owned(), json!(self.id));
        out.insert(
            "outcome".to_owned(),
            json!(if succeeded { "succeeded" } else { "failed" }),
        );
        out.insert("failure".to_owned(), json!(self.r.failure));
        out.insert("residue".to_owned(), json!(self.r.residue));
        out.insert("repo_intact".to_owned(), json!(self.r.repo_intact));
        out.insert("evidence".to_owned(), json!(evidence));
        Ok(Output {
            failed: !succeeded,
            receipt: Value::Object(out),
            refused: false,
            once: Vec::new(),
        })
    }

    /// A drill claims success only when every check the table derives success from passed (D-O): any check that
    /// failed or was not reached becomes the named failure, so the receipt never claims what the CHECK refuses.
    fn derive_failure(&mut self) {
        let r = &self.r;
        let checks = [
            ("manifest_matches", r.manifest_matches == Some(true)),
            ("witness_a_present", r.witness_a_present == Some(true)),
            ("witness_b_absent", r.witness_b_absent == Some(true)),
            (
                "server_version_matches",
                r.server_version_matches == Some(true),
            ),
            ("rebuild_equivalent", r.rebuild_equivalent == Some(true)),
            ("repo_intact", r.repo_intact == Some(true)),
            ("migrations_drift", r.migrations_drift == Some(0)),
            ("rls_unforced", r.rls_unforced == Some(0)),
            ("isolation_violations", r.isolation_violations == Some(0)),
            ("isolation_pairs", r.isolation_pairs == Some(2)),
            (
                "payload_digest_mismatches",
                r.payload_digest_mismatches == Some(0),
            ),
            ("provider_calls", r.provider_calls == Some(0)),
            (
                "drill_archiver_attempts",
                r.drill_archiver_attempts == Some(0),
            ),
            (
                "legacy_points_without_vector",
                r.legacy_points_without_vector == Some(0),
            ),
            ("residue", r.residue == Some(0)),
            ("rebuild_points", r.rebuild_points.is_some()),
        ];
        let failed: Vec<&str> = checks
            .iter()
            .filter(|(_, ok)| !ok)
            .map(|(name, _)| *name)
            .collect();
        if !failed.is_empty() {
            self.fail(format!("checks_failed:{}", failed.join(",")));
        }
    }

    /// Steps 2–7. An `Err` is the step's named failure; the caller destroys and writes the receipt either way.
    #[allow(clippy::too_many_lines)] // steps 2-7 in their D-L order
    async fn pipeline(&mut self, guard: &mut DestroyGuard) -> Step<()> {
        // Step 2: witnesses in the source; T between them; `pgbackrest check` forces the switch and waits for the
        // segment holding B (bounded by pgBackRest's archive-timeout, 10.11 H).
        let t = Instant::now();
        let source = &self.source;
        let (a_at, _) = repo::write_witness(source, self.id, "A")
            .await
            .map_err(infra("witness A"))?;
        let target = repo::clock(source).await.map_err(infra("clock"))?;
        let (b_at, b_wal) = repo::write_witness(source, self.id, "B")
            .await
            .map_err(infra("witness B"))?;
        self.r.target_time = Some(target);
        let wait = Instant::now();
        let check = self.pg.pgbackrest(&["check"]).map_err(|e| match e {
            Failure::Infra(m) | Failure::Usage(m) => format!("infra:pgbackrest check: {m}"),
            Failure::Provisioning(p) => format!("infra:pgbackrest check: {p}"),
        })?;
        let archive_wait = wait.elapsed().as_secs_f64();
        self.ev.insert(
            "witnesses".to_owned(),
            json!({
                "a_written_at": a_at.unix_timestamp(),
                "target_time": pg_time(target),
                "b_written_at": b_at.unix_timestamp(),
                "b_walfile": b_wal,
                "archive_wait_seconds": archive_wait,
            }),
        );
        if check.code != 0 {
            return Err(format!("archive_check_failed:exit {}", check.code));
        }
        self.phase("witnesses_s", t);

        // Steps 3–4: the isolated project and the restore to T (no --delta: a non-empty target is refused).
        if !backup::valid_label(&self.set.label) {
            return Err("infra:set label is not a pgBackRest label".to_owned());
        }
        let restore_started = Instant::now();
        guard.armed = true;
        self.project.ok("", &["up", "-d", "qdrant"])?;
        let restore = format!(
            "mkdir -p {PG1_PATH} && chmod 700 {PG1_PATH} && pgbackrest --stanza={STANZA} --set={} --type=time \
             '--target={}' --target-action=promote --archive-mode=off --log-level-console=warn restore",
            self.set.label,
            pg_time(target)
        );
        let out = self.project.run(
            "",
            &[
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "--user",
                "postgres",
                "--entrypoint",
                "bash",
                "pg",
                "-c",
                &restore,
            ],
            None,
        )?;
        if !out.status.success() {
            return Err(format!("restore_failed: {}", tail(&text(&out), 6)));
        }
        self.project.ok("", &["up", "-d", "pg"])?;
        self.project.wait_promoted("", self.timeout).await?;
        self.phase("restore_s", restore_started);

        // Step 5: credential neutralisation over the socket, then TCP (D-M).
        let passwords: BTreeMap<&str, String> =
            DRILL_LOGIN_ROLES.iter().map(|r| (*r, secret())).collect();
        self.project
            .psql("", &neutralise_sql(&passwords, &DRILL_LOGIN_ROLES))
            .map_err(|e| format!("neutralise_failed: {e}"))?;
        self.project
            .ok("*", &["up", "-d", "--force-recreate", "pg"])?;
        let mut restored = self.connect(&passwords).await?;

        // Step 6: the checks (b) first, on the pool every later step uses.
        let t = Instant::now();
        let kinds = repo::witness_kinds(&restored.maintenance, self.id)
            .await
            .map_err(infra("witness kinds"))?;
        self.r.witness_a_present = Some(kinds.iter().any(|k| k == "A"));
        self.r.witness_b_absent = Some(!kinds.iter().any(|k| k == "B"));
        if kinds.iter().any(|k| k == "B") {
            return Err("drill_pool_is_not_the_restore".to_owned());
        }
        self.check_manifest();
        self.check_cluster(&restored, target).await;
        let tenants = rebuild::all_tenants(&restored.maintenance)
            .await
            .map_err(infra("drill tenants"))?;
        let mut facts = BTreeMap::new();
        for tenant in &tenants {
            match repo::tenant_facts(&restored.maintenance, *tenant, None).await {
                Ok(f) => {
                    facts.insert(*tenant, f);
                }
                Err(e) => self.fail(format!("probe_failed:tenant_facts: {e}")),
            }
        }
        self.restored_in_flight(&facts);
        self.check_isolation(&restored, &facts).await;
        self.check_digests(&facts, target).await;
        self.phase("checks_s", t);

        // Step 7: rebuild into the empty drill Qdrant with the closed no-provider deps (D-N(g)).
        let t = Instant::now();
        self.rebuild(&mut restored, &tenants, &facts).await?;
        self.phase("rebuild_s", t);
        self.r.rto_seconds = Some(restore_started.elapsed().as_secs_f64());
        Ok(())
    }

    /// The restored cluster's pools over TCP. D-N(g) / finding 6: the targets come from `docker compose port` only
    /// and are refused when equal to the source DSN's or the configured Qdrant's address.
    async fn connect(&self, passwords: &BTreeMap<&str, String>) -> Step<Restored> {
        let pg_port = self.project.port("*", "pg", "5432")?;
        let qdrant_port = self.project.port("*", "qdrant", "6333")?;
        let source_dsn =
            env("HUMAUX_MAINTENANCE_PG_DSN").map_err(|_| "infra:source dsn".to_owned())?;
        let (source_addr, db) =
            dsn_target(&source_dsn).ok_or_else(|| "infra:source dsn shape".to_owned())?;
        let pg_addr = format!("127.0.0.1:{pg_port}");
        let qdrant_addr = format!("127.0.0.1:{qdrant_port}");
        let configured_qdrant = match (
            std::env::var("HUMAUX_MAINTENANCE_QDRANT_HOST"),
            std::env::var("HUMAUX_MAINTENANCE_QDRANT_PORT"),
        ) {
            (Ok(h), Ok(p)) => Some(format!("{h}:{p}")),
            _ => None,
        };
        let localhost = |a: &str| a.replace("localhost:", "127.0.0.1:");
        if localhost(&source_addr) == pg_addr
            || configured_qdrant.as_deref().map(localhost) == Some(qdrant_addr)
        {
            return Err("drill_target_is_live".to_owned());
        }
        let dsn = |role: &str| format!("postgres://{role}:{}@{pg_addr}/{db}", passwords[role]);
        let started = Instant::now();
        loop {
            // dep: PostgreSQL(role_maintenance) — the restored cluster (neutralised password)
            match MaintenanceDbPool::connect(&dsn("role_maintenance")).await {
                Ok(maintenance) => {
                    // dep: PostgreSQL(role_gateway) — the restored cluster, isolation probes only
                    let gateway = RuntimeDbPool::connect(&dsn("role_gateway"))
                        .await
                        .map_err(infra("drill role_gateway"))?;
                    // dep: PostgreSQL(role_retrieval_worker) — the restored cluster, verifier reads
                    let reader = RetrievalWorkerDbPool::connect(&dsn("role_retrieval_worker"))
                        .await
                        .map_err(infra("drill role_retrieval_worker"))?;
                    // dep: PostgreSQL(role_retrieval_worker) — the restored cluster, the run-scoped pass
                    let pass = RetrievalWorkerDbPool::connect(&dsn("role_retrieval_worker"))
                        .await
                        .map_err(infra("drill role_retrieval_worker"))?;
                    return Ok(Restored {
                        maintenance,
                        gateway,
                        reader,
                        pass: Some(pass),
                        qdrant_port,
                    });
                }
                Err(e) if started.elapsed() >= self.timeout => {
                    return Err(format!("infra:drill role_maintenance: {e}"));
                }
                Err(_) => tokio::time::sleep(POLL).await,
            }
        }
    }

    /// D-N (a): the restored set's manifest pulled back now equals the label's identity.
    fn check_manifest(&mut self) {
        let script = format!(
            "set -o pipefail; pgbackrest --stanza={STANZA} --log-level-console=warn repo-get \
             backup/{STANZA}/{}/backup.manifest | sha256sum | cut -c1-64",
            self.set.label
        );
        match self.project.run(
            "*",
            &[
                "exec", "-T", "--user", "postgres", "pg", "bash", "-c", &script,
            ],
            None,
        ) {
            Ok(out) if out.status.success() => {
                let got = String::from_utf8_lossy(&out.stdout).trim().to_owned();
                self.r.manifest_matches = Some(got == backup::hex(&self.set.manifest_sha256));
            }
            Ok(out) => self.fail(format!("probe_failed:manifest: {}", tail(&text(&out), 3))),
            Err(e) => self.fail(format!("probe_failed:manifest: {e}")),
        }
    }

    /// D-N (c), (d), (e), (j): version, migrations at T, RLS FORCE, archiver attempts.
    async fn check_cluster(&mut self, restored: &Restored, target: OffsetDateTime) {
        let drill = repo::cluster_facts(&restored.maintenance).await;
        let source = repo::cluster_facts(&self.source).await;
        match (drill, source) {
            (Ok(d), Ok(s)) => {
                self.r.server_version_matches = Some(d.server_version_num == s.server_version_num);
                self.r.rls_unforced = Some(i32::try_from(d.rls_unforced).unwrap_or(i32::MAX));
                self.r.drill_archiver_attempts = Some(d.archiver_attempts);
                self.ev.insert(
                    "source".to_owned(),
                    json!({ "server_version_num": s.server_version_num }),
                );
            }
            (Err(e), _) | (_, Err(e)) => self.fail(format!("probe_failed:cluster: {e}")),
        }
        // D-N (d): `ops.schema_migrations` is owner-only (0201 D-C), so each side is read READ ONLY as the image
        // superuser over its container's socket (the drill already holds the docker CLI, root-equivalent).
        let sql = |at: &str| {
            format!(
                "SELECT migration_id || ' ' || checksum FROM ops.schema_migrations{at} ORDER BY 1"
            )
        };
        let drill = self.project.psql_in("*", &self.db, &sql(""));
        let source = self.pg.exec(&[
            "psql",
            "-qAt",
            "-v",
            "ON_ERROR_STOP=1",
            "-d",
            &self.db,
            "-c",
            &sql(&format!(" WHERE applied_at <= '{}'", pg_time(target))),
        ]);
        match (drill, source) {
            (Ok(d), Ok(s)) if s.code == 0 => {
                let rows = |t: &str| t.lines().map(str::to_owned).collect::<BTreeSet<String>>();
                let drift = rows(&d).symmetric_difference(&rows(&s.stdout)).count();
                self.r.migrations_drift = Some(i32::try_from(drift).unwrap_or(i32::MAX));
            }
            (Ok(_), Ok(s)) => self.fail(format!("probe_failed:migrations: {}", tail(&s.stderr, 3))),
            (Err(e), _) => self.fail(format!("probe_failed:migrations: {e}")),
            (_, Err(_)) => self.fail("probe_failed:migrations: source exec"),
        }
    }

    /// R-37 quarantine: what the restored cluster still had in flight at T, left untouched (it is destroyed).
    fn restored_in_flight(&mut self, facts: &BTreeMap<Uuid, TenantFacts>) {
        let sum = |f: fn(&TenantFacts) -> i64| facts.values().map(f).sum::<i64>();
        self.r.restored_in_flight = Some(json!({
            "issued_g1": sum(|f| f.issued_g1),
            "open_jobs": sum(|f| f.open_jobs),
            "reserved_calls": sum(|f| f.reserved_calls),
        }));
    }

    /// D-N (f): the two tenants with the most memories, each probed as role_gateway (memories, Evidence, registry)
    /// and role_retrieval_worker (vectors); every other-tenant row seen is a violation, and each tenant must see
    /// its own memories (non-vacuous).
    async fn check_isolation(&mut self, restored: &Restored, facts: &BTreeMap<Uuid, TenantFacts>) {
        let mut ranked: Vec<(Uuid, i64)> = facts
            .iter()
            .filter(|(_, f)| f.memories > 0)
            .map(|(t, f)| (*t, f.memories))
            .collect();
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        ranked.truncate(2);
        if ranked.len() < 2 {
            self.fail("isolation_not_applicable: fewer than two tenants with memory records");
        }
        let mut violations = 0_i64;
        let mut pairs = 0;
        for (tenant, _) in &ranked {
            let gateway = repo::gateway_isolation(&restored.gateway, *tenant).await;
            let vectors = repo::vector_isolation(&restored.reader, *tenant).await;
            match (gateway, vectors) {
                (Ok((others, own)), Ok(vector_others)) => {
                    violations += others + vector_others;
                    pairs += 1;
                    if own == 0 {
                        self.fail(format!("isolation_vacuous:{tenant}"));
                    }
                }
                (Err(e), _) | (_, Err(e)) => {
                    self.fail(format!("probe_failed:isolation: {e}"));
                    return;
                }
            }
        }
        self.r.isolation_violations = Some(i32::try_from(violations).unwrap_or(i32::MAX));
        self.r.isolation_pairs = Some(pairs);
    }

    /// D-N (h): per tenant, the Evidence digest of the source at T against the restored cluster's (all rows).
    async fn check_digests(&mut self, facts: &BTreeMap<Uuid, TenantFacts>, target: OffsetDateTime) {
        let tenants = match rebuild::all_tenants(&self.source).await {
            Ok(t) => t,
            Err(e) => {
                self.fail(format!("probe_failed:digests: {e}"));
                return;
            }
        };
        let mut mismatches = 0;
        let mut seen: BTreeSet<Uuid> = BTreeSet::new();
        for tenant in tenants {
            seen.insert(tenant);
            match repo::tenant_facts(&self.source, tenant, Some(target)).await {
                Ok(s) => {
                    if facts.get(&tenant).map(|d| &d.evidence_digest) != Some(&s.evidence_digest) {
                        mismatches += 1;
                    }
                }
                Err(e) => {
                    self.fail(format!("probe_failed:digests: {e}"));
                    return;
                }
            }
        }
        mismatches += facts.keys().filter(|t| !seen.contains(t)).count();
        self.r.payload_digest_mismatches = Some(i32::try_from(mismatches).unwrap_or(i32::MAX));
    }

    /// D-N (g): the D-E sequence for every stream of every restored tenant, `require_stored_vector`, projected by
    /// an in-process pass that claims only the run's tickets through the closed no-provider deps.
    async fn rebuild(
        &mut self,
        restored: &mut Restored,
        tenants: &[Uuid],
        facts: &BTreeMap<Uuid, TenantFacts>,
    ) -> Step<()> {
        let labels: BTreeSet<&String> = facts.values().flat_map(|f| f.labels.iter()).collect();
        if labels.len() > 1 {
            return Err(format!("drill_labels_ambiguous:{labels:?}"));
        }
        let mut totals = Totals {
            equivalent: true,
            ..Totals::default()
        };
        if let Some(label) = labels.first() {
            let worker = rebuild::worker_label(&restored.maintenance, label)
                .await
                .map_err(infra("worker label"))?;
            // dep: Qdrant(*) — the drill's own Qdrant only (`docker compose port`), never the configured one
            let face = QdrantFace::new("127.0.0.1", restored.qdrant_port, "127.0.0.1/32")
                .map_err(infra("drill qdrant face"))?;
            if !rebuild::collection_names(&face)
                .await
                .map_err(infra("drill qdrant"))?
                .is_empty()
            {
                return Err("drill_qdrant_not_fresh".to_owned());
            }
            let closed = rebuild::drill_closed_deps(
                restored
                    .pass
                    .take()
                    .ok_or_else(|| "infra:the pass pool is used once".to_owned())?,
                restored.qdrant_port,
                &scanner_pin()?,
                label,
                worker.dimension,
                ProcessorId(DRILL_PROCESSOR),
                Arc::new(|period| Box::pin(tokio::time::sleep(period))),
            )
            .map_err(infra("drill deps"))?;
            let (shared, no_provider) = rebuild::drill_projection_deps(closed);
            let cfg = pass_config(self.id)?;
            let deps = RebuildDeps {
                maintenance: &restored.maintenance,
                reader: &restored.reader,
                qdrant: &face,
                worker: &worker,
            };
            for tenant in tenants {
                for stream in rebuild::streams(&restored.maintenance, *tenant, None)
                    .await
                    .map_err(infra("streams"))?
                {
                    let (verdict, report) =
                        rebuild_one(&deps, &stream, &shared, &cfg, self.timeout)
                            .await
                            .map_err(infra("rebuild"))?;
                    totals.add(verdict, report);
                }
            }
            totals.provider_calls = no_provider.attempts();
        }
        self.r.rebuild_equivalent = Some(totals.equivalent);
        self.r.rebuild_points = Some(totals.points);
        self.r.legacy_points_without_vector = Some(totals.legacy);
        self.r.unprojected_at_target = Some(totals.unprojected);
        self.r.provider_calls = Some(i64::try_from(totals.provider_calls).unwrap_or(i64::MAX));
        self.ev.insert(
            "rebuild".to_owned(),
            json!({
                "streams": totals.streams,
                "equivalent": totals.equivalent,
                "points": totals.points,
                "orphans_deleted": totals.orphans,
                "distill_pending_inputs": totals.pending,
                "unprojected_at_target": totals.unprojected,
                "provider_calls": totals.provider_calls,
            }),
        );
        Ok(())
    }

    /// D-O evidence: every check, the timings, RPO/RTO readings; no DSN, password or key.
    fn evidence(&self, succeeded: bool) -> Value {
        let r = &self.r;
        let mut ev = self.ev.clone();
        ev.insert("drill_id".to_owned(), json!(self.id));
        ev.insert("project".to_owned(), json!(self.project.name));
        ev.insert(
            "backup".to_owned(),
            json!({
                "label": self.set.label,
                "type": "full",
                "stopped_at": unix(self.set.stopped_at),
                "manifest_sha256": backup::hex(&self.set.manifest_sha256),
                "manifest_matches": r.manifest_matches,
            }),
        );
        for (k, v) in [
            ("witness_a_present", json!(r.witness_a_present)),
            ("witness_b_absent", json!(r.witness_b_absent)),
            ("server_version_matches", json!(r.server_version_matches)),
            ("migrations_drift", json!(r.migrations_drift)),
            ("rls_unforced", json!(r.rls_unforced)),
            ("isolation_violations", json!(r.isolation_violations)),
            ("isolation_pairs", json!(r.isolation_pairs)),
            (
                "payload_digest_mismatches",
                json!(r.payload_digest_mismatches),
            ),
            ("provider_calls", json!(r.provider_calls)),
            ("drill_archiver_attempts", json!(r.drill_archiver_attempts)),
            ("rebuild_points", json!(r.rebuild_points)),
            (
                "legacy_points_without_vector",
                json!(r.legacy_points_without_vector),
            ),
            ("unprojected_at_target", json!(r.unprojected_at_target)),
            ("restored_in_flight", json!(r.restored_in_flight)),
            ("repo_intact", json!(r.repo_intact)),
            ("residue", json!(r.residue)),
            ("timings", json!(r.phase_seconds)),
            ("rto_seconds", json!(r.rto_seconds)),
            (
                "rpo",
                json!({
                    "wal_archive_wait_s": ev.get("witnesses").map(|w| w["archive_wait_seconds"].clone()),
                    "backup_age_s": self.set.stopped_at.map(|s| (OffsetDateTime::now_utc() - s).whole_seconds()),
                }),
            ),
            ("succeeded", json!(succeeded)),
            ("failure", json!(r.failure)),
        ] {
            ev.insert(k.to_owned(), v);
        }
        Value::Object(ev)
    }
}

/// The rebuild's sums over every stream report (D-N(g) evidence and receipt columns).
#[derive(Default)]
struct Totals {
    streams: Vec<Value>,
    equivalent: bool,
    points: i64,
    legacy: i64,
    orphans: i64,
    unprojected: i64,
    pending: i64,
    provider_calls: u64,
}

impl Totals {
    fn add(&mut self, verdict: Verdict, report: Value) {
        let n = |k: &str| report[k].as_i64().unwrap_or(0);
        self.equivalent &= verdict == Verdict::Equivalent;
        self.points += n("points");
        self.legacy += n("legacy_points_without_vector");
        self.orphans += n("orphans_deleted");
        self.unprojected += report["excluded"]["without_stored_vector"]
            .as_i64()
            .unwrap_or(0);
        self.pending += report["excluded"]["distill_pending"].as_i64().unwrap_or(0);
        self.streams.push(report);
    }
}

/// The retrieval worker's pinned scanner, read under the worker's own key names (the card-35 peer-key precedent).
fn scanner_pin() -> Step<ScannerPin> {
    let key = |name: &str| env(name).map_err(|_| format!("infra:missing {name}"));
    Ok(ScannerPin {
        executable: key("HUMAUX_RETRIEVAL_WORKER_GITLEAKS_BIN")?.into(),
        version: key("HUMAUX_RETRIEVAL_WORKER_GITLEAKS_VERSION")?,
        sha256: key("HUMAUX_RETRIEVAL_WORKER_GITLEAKS_SHA256")?,
    })
}

/// The drill's in-process pass (D-N(g)): the private-memory claim (`TicketFamily::ALL` has one member) under a
/// lease owner naming the drill.
fn pass_config(id: Uuid) -> Step<PassConfig> {
    Ok(PassConfig {
        claim: ClaimFamily::of(RetrievalFamily::PrivateMemoryV1)
            .ok_or_else(|| "infra:claim family".to_owned())?,
        lease_owner: format!("humaux-maintenance-drill/{id}"),
        lease_secs: PASS_LEASE_SECS,
        batch: i64::from(PASS_BATCH),
        per_tenant_cap: i64::from(PASS_BATCH),
        max_attempts: PASS_MAX_ATTEMPTS,
        backoff: Backoff {
            base_secs: PASS_BACKOFF_SECS.0,
            max_secs: PASS_BACKOFF_SECS.1,
        },
    })
}

/// SQL that sets a fresh password on every role of `keep` (values never leave this process but by stdin) and turns
/// every other LOGIN role NOLOGIN (D-M, D-U).
fn neutralise_sql(passwords: &BTreeMap<&str, String>, keep: &[&str]) -> String {
    let mut sql = String::from("BEGIN;\n");
    for (role, password) in passwords {
        sql.push_str(&format!("ALTER ROLE {role} PASSWORD '{password}';\n"));
    }
    let keep: Vec<String> = keep.iter().map(|r| format!("'{r}'")).collect();
    sql.push_str(&format!(
        "DO $$ DECLARE r record; BEGIN FOR r IN SELECT rolname FROM pg_roles WHERE rolcanlogin \
         AND rolname NOT IN ({}) LOOP EXECUTE format('ALTER ROLE %I NOLOGIN', r.rolname); END LOOP; END $$;\nCOMMIT;\n",
        keep.join(", ")
    ));
    sql
}

type PumpFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// One stream through D-E steps 1–7 with a pump that claims only this run's tickets (D-N(g)); `(verdict, report)`.
async fn rebuild_one(
    deps: &RebuildDeps<'_>,
    stream: &Stream,
    shared: &SharedProjectionDeps,
    cfg: &PassConfig,
    wait: Duration,
) -> std::result::Result<(Verdict, Value), ProvisioningError> {
    if let Err(receipt) = rebuild::precheck(deps, stream, None).await? {
        return Ok((Verdict::ReEmbedRequired, receipt));
    }
    let run = rebuild::open_run(deps, stream).await?;
    provisioning::ensure_collection(deps.qdrant, &stream.collection, deps.worker.dimension).await?;
    let issued = rebuild::issue_tickets(deps, stream, &run, PASS_BATCH, true).await?;
    let only = OnlyRun {
        tenant_id: stream.key.tenant_id.0,
        run_id: run.run_id,
    };
    let pump = move || -> PumpFuture<'_> {
        Box::pin(async move {
            if let Err(e) = run_claimed_pass_for_run(shared, cfg, only).await {
                eprintln!("humaux-maintenance restore drill: pass: {e:?}");
            }
            tokio::time::sleep(PASS_PAUSE).await;
        })
    };
    let settled = rebuild::wait_generation(deps, stream, &run, wait, &pump).await?;
    let (mut report, orphans) = if settled {
        rebuild::verify_run(deps, stream, &run, true).await?
    } else {
        let mut report = rebuild::verify_stream(deps, stream, Some(run.run_id), true).await?;
        report.verdict = Verdict::CannotEstablish;
        report.json["verdict"] = json!(Verdict::CannotEstablish.as_db_str());
        (report, 0)
    };
    report.json["run_id"] = json!(run.run_id);
    report.json["generation"] = json!(run.generation);
    report.json["issued"] = json!(issued);
    report.json["orphans_deleted"] = json!(orphans);
    if report.verdict == Verdict::Equivalent {
        match rebuild::close_run(deps, stream, &run, &report).await {
            Ok(()) => report.json["closed"] = json!(true),
            Err(ProvisioningError::Refused(reason)) => {
                report.verdict = Verdict::CannotEstablish;
                report.json["verdict"] = json!(Verdict::CannotEstablish.as_db_str());
                report.json["close_refused"] = json!(reason);
            }
            Err(other) => return Err(other),
        }
    }
    Ok((report.verdict, report.json))
}

// ============================================================================
// restore pitr (D-U as amended by 10.11 B)
// ============================================================================

/// `restore pitr --compose-file <backup.yml> --project <p> (--target end | --target-time <rfc3339>) --evidence <file>`:
/// the real restore into project `<p>`. Refusals before anything is restored (exit 3), in this order: the project's
/// `pgdata` volume is non-empty; `HUMAUX_MIGRATOR_PG_DSN` is set; a container of the production backup project is
/// running (`production_pg_running`: only one cluster may ever archive into the stanza, so a promoted timeline can
/// never fork beside a running one); the disk floor; `verify --set` of the set the restore will use. Then the
/// restore, a socket-only boot, the NOLOGIN quarantine of every LOGIN role but the superuser, role_maintenance and
/// role_retrieval_worker, and the boot with the operator's listen address. The restored cluster archives on its new
/// timeline: it is the new production.
#[allow(clippy::too_many_lines)] // one ordered list of D-U refusals (10.11 B): the order is the contract
pub(crate) async fn pitr(args: &Args) -> Result<Output> {
    const ARM: &str = "restore pitr";
    let file = args.required("--compose-file")?;
    let name = args.required("--project")?;
    let evidence = evidence_arg(args)?;
    let target: Option<OffsetDateTime> = match (args.get("--target"), args.get("--target-time")) {
        (Some(t), None) if t == "end" => None,
        (None, Some(_)) => Some(
            OffsetDateTime::parse(&args.required("--target-time")?, &Rfc3339)
                .map_err(|_| Failure::Usage("--target-time: not an RFC 3339 time".to_owned()))?,
        ),
        _ => {
            return Err(Failure::Usage(
                "exactly one of --target end or --target-time <rfc3339>".to_owned(),
            ));
        }
    };
    let listen = env("HUMAUX_PG_LISTEN_ADDRESSES").unwrap_or_default();
    let timeout = Duration::from_secs(env_parsed(RESTORE_TIMEOUT)?);
    let floor: u64 = env_parsed(MIN_FREE_DISK)?;
    let project = Project {
        file,
        name: name.clone(),
        env: Vec::new(),
    };
    let mounts = project.mounts("").map_err(Failure::Infra)?;

    // Refusal 1: a non-empty pgdata volume (pgBackRest without --delta refuses too; this names it first).
    let volume = format!("{name}_pgdata");
    if docker(&["volume", "inspect", "--format", "{{.Name}}", &volume]).is_ok() {
        let listing = docker(&[
            "run",
            "--rm",
            "--label",
            &format!("humaux.pitr={name}"),
            "--user",
            "postgres",
            "-v",
            &format!("{volume}:/var/lib/postgresql"),
            "--entrypoint",
            "sh",
            &mounts.image,
            "-c",
            &format!("ls -A {PG1_PATH} 2>/dev/null | head -1"),
        ])
        .map_err(Failure::Infra)?;
        if !listing.is_empty() {
            return Ok(refused(ARM, None, format!("pgdata_not_empty:{volume}")));
        }
    }
    // Refusal 2: no owner credential.
    if std::env::var_os(MIGRATOR_DSN).is_some() {
        return Ok(refused(
            ARM,
            None,
            format!("migrator_dsn_present: {MIGRATOR_DSN} is set"),
        ));
    }
    // Refusal 3: the production cluster still runs (it would keep archiving into the stanza beside the restore).
    let production = env(BACKUP_PROJECT)?;
    let _ = env(BACKUP_COMPOSE_FILE)?;
    let running = docker(&[
        "ps",
        "--filter",
        &format!("label=com.docker.compose.project={production}"),
        "--filter",
        "status=running",
        "--format",
        "{{.Names}}",
    ])
    .map_err(Failure::Infra)?;
    if let Some(container) = running.lines().find(|l| !l.trim().is_empty()) {
        return Ok(refused(
            ARM,
            None,
            format!("production_pg_running:{}", container.trim()),
        ));
    }
    let (label, restore) = pitr_set(&project, target)?;
    // Refusal 4: the disk floor (10.11 B).
    let df = mounts
        .oneshot(
            &format!("humaux.pitr={name}"),
            "df -Pk / | awk 'NR==2 {print $4}'",
        )
        .map_err(Failure::Infra)?;
    let have = df.trim().parse::<u64>().unwrap_or(0).saturating_mul(1024);
    let need = restore.saturating_add(floor);
    if need > have {
        return Ok(refused(
            ARM,
            None,
            format!("restore_free_bytes:{need}/{have} (restore={restore} floor={floor})"),
        ));
    }
    // Refusal 5: the set must verify now (finding 7 part 2).
    let verify = project
        .run(
            "",
            &[
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "--user",
                "postgres",
                "--entrypoint",
                "pgbackrest",
                "pg",
                "--stanza=humaux",
                "verify",
                &format!("--set={label}"),
                "--output=text",
                "--verbose",
                "--log-level-console=warn",
            ],
            None,
        )
        .map_err(Failure::Infra)?;
    let code = verify.status.code().unwrap_or(-1);
    if let Some(why) =
        backup::verify_failure(code, &String::from_utf8_lossy(&verify.stdout), &label)
    {
        return Ok(refused(
            ARM,
            None,
            format!("repo_set_unverifiable:{label} ({why})"),
        ));
    }

    let db = dsn_target(&env("HUMAUX_MAINTENANCE_PG_DSN")?)
        .map(|(_, db)| db)
        .ok_or_else(|| Failure::Usage("HUMAUX_MAINTENANCE_PG_DSN: no database".to_owned()))?;
    pitr_restore(&project, &label, &db, target, timeout, &listen, &evidence).await
}

/// The set `restore pitr` will use: the newest in the repository that stopped at or before the target (any for
/// `end`), with its restore size from `info`.
fn pitr_set(project: &Project, target: Option<OffsetDateTime>) -> Result<(String, u64)> {
    let info_out = project
        .run(
            "",
            &[
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "--user",
                "postgres",
                "--entrypoint",
                "pgbackrest",
                "pg",
                "--stanza=humaux",
                "--log-level-console=warn",
                "info",
                "--output=json",
            ],
            None,
        )
        .map_err(Failure::Infra)?;
    if !info_out.status.success() {
        return Err(Failure::Infra(format!(
            "pgbackrest info: {}",
            tail(&text(&info_out), 4)
        )));
    }
    let info: Value = serde_json::from_slice(&info_out.stdout)
        .map_err(|e| Failure::Infra(format!("pgbackrest info json: {e}")))?;
    let set = info[0]["backup"]
        .as_array()
        .and_then(|sets| {
            sets.iter().rev().find(|s| {
                target.is_none_or(|t| {
                    s["timestamp"]["stop"]
                        .as_i64()
                        .is_some_and(|stop| stop <= t.unix_timestamp())
                })
            })
        })
        .cloned()
        .ok_or_else(|| Failure::Infra("pgbackrest info: no set before the target".to_owned()))?;
    let label = set["label"]
        .as_str()
        .filter(|l| backup::valid_label(l))
        .ok_or_else(|| Failure::Infra("pgbackrest info: set label".to_owned()))?
        .to_owned();
    let restore = set["info"]["size"].as_u64().unwrap_or(0);
    Ok((label, restore))
}

/// D-U after every refusal passed: restore, socket-only boot, the NOLOGIN quarantine, the operator's boot, the
/// evidence.
async fn pitr_restore(
    project: &Project,
    label: &str,
    db: &str,
    target: Option<OffsetDateTime>,
    timeout: Duration,
    listen: &str,
    evidence: &str,
) -> Result<Output> {
    const ARM: &str = "restore pitr";
    // The restore: end of archive by default (--type=default), a time only for logical corruption.
    let started = Instant::now();
    let kind = match target {
        None => "--type=default".to_owned(),
        Some(t) => format!(
            "--type=time '--target={}' --target-action=promote",
            pg_time(t)
        ),
    };
    let restore_cmd = format!(
        "mkdir -p {PG1_PATH} && chmod 700 {PG1_PATH} && pgbackrest --stanza={STANZA} --set={label} {kind}          --log-level-console=warn restore"
    );
    let mut out = class_fields(ARM);
    out.insert("project".to_owned(), json!(project.name));
    out.insert("backup_label".to_owned(), json!(label));
    out.insert(
        "target".to_owned(),
        json!(target.map_or_else(|| "end".to_owned(), pg_time)),
    );
    out.insert(
        "repo_dir".to_owned(),
        json!(std::env::var("HUMAUX_PG_REPO_DIR").ok()),
    );
    let result: Step<Value> = async {
        let ran = project.run(
            "",
            &[
                "run", "--rm", "--no-deps", "-T", "--user", "postgres", "--entrypoint", "bash", "pg", "-c",
                &restore_cmd,
            ],
            None,
        )?;
        if !ran.status.success() {
            return Err(format!("restore_failed: {}", tail(&text(&ran), 6)));
        }
        project.ok("", &["up", "-d", "pg"])?;
        project.wait_promoted("", timeout).await?;
        // The quarantine as a database fact, before TCP opens.
        let keep: BTreeMap<&str, String> = BTreeMap::new();
        project
            .psql("", &neutralise_sql(&keep, &PITR_LOGIN_ROLES))
            .map_err(|e| format!("quarantine_failed: {e}"))?;
        let counts = project.psql_in(
            "",
            db,
            "SELECT (SELECT count(*) FROM ops.jobs WHERE status NOT IN ('DONE', 'FAILED', 'DEAD')) || ' ' || \
                    (SELECT count(*) FROM ops.model_call_ledger WHERE status = 'RESERVED') || ' ' || \
                    (SELECT count(*) FROM projection.stream_log WHERE state = 'ISSUED'); \
             SELECT string_agg(rolname, ',' ORDER BY rolname) FROM pg_roles \
              WHERE NOT rolcanlogin AND rolname LIKE 'role\\_%'",
        )?;
        let mut lines = counts.lines();
        let nums: Vec<i64> = lines
            .next()
            .unwrap_or("")
            .split_whitespace()
            .filter_map(|n| n.parse().ok())
            .collect();
        let locked = lines.next().unwrap_or("").to_owned();
        project.ok(listen, &["up", "-d", "--force-recreate", "pg"])?;
        Ok(json!({
            "restored_in_flight": {
                "open_jobs": nums.first(),
                "reserved_calls": nums.get(1),
                "issued_tickets": nums.get(2),
            },
            "nologin_roles": locked.split(',').filter(|r| !r.is_empty()).collect::<Vec<_>>(),
            "lost_window": target.map_or_else(
                || "none: replayed to the end of the archive".to_owned(),
                |t| format!("every write after {}", pg_time(t))),
        }))
    }
    .await;
    out.insert(
        "restore_s".to_owned(),
        json!(started.elapsed().as_secs_f64()),
    );
    let failed = match result {
        Ok(Value::Object(m)) => {
            out.extend(m);
            out.insert("outcome".to_owned(), json!("restored"));
            false
        }
        Ok(_) => false,
        Err(e) => {
            out.insert("outcome".to_owned(), json!("failed"));
            out.insert("failure".to_owned(), json!(e));
            true
        }
    };
    let receipt = Value::Object(out);
    write_evidence(evidence, &receipt)?;
    Ok(Output {
        failed,
        receipt,
        refused: false,
        once: Vec::new(),
    })
}
