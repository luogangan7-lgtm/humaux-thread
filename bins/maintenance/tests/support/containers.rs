//! `maintenance::tests::support::containers` — the scratch backup source of the card-37 Docker suites: the pinned
//!   PG image built from `deploy/images/postgres`, and one compose project `humaux-c37-<purpose>-<pid>-<n>` started
//!   from the real `deploy/compose/backup.yml` (plus a test-owned override: caps, labels, and the per-test layout)
//!   with its repository filesystem on a per-test host directory (ADR-0064 D-P, spike SP-14).
//! Depends-on: crates=[humaux-testkit, serde_json, uuid]; services=[subprocess(docker), subprocess(humaux-maintenance),
//!   subprocess(chmod)]; env=[CARGO_BIN_EXE_humaux-maintenance, CARGO_MANIFEST_DIR,
//!   HUMAUX_MAINTENANCE_BACKUP_COMPOSE_FILE, HUMAUX_MAINTENANCE_BACKUP_MIN_FREE_BYTES, HUMAUX_MAINTENANCE_BACKUP_PROJECT,
//!   HUMAUX_MAINTENANCE_BACKUP_REPO_MAX_BYTES, HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_PG_ARCHIVE_TIMEOUT_SECONDS,
//!   HUMAUX_PG_CONTAINER, HUMAUX_PG_IMAGE, HUMAUX_PG_LISTEN_ADDRESSES, HUMAUX_PG_PORT, HUMAUX_PG_REPO_DIR];
//!   modules=[humaux-testkit]
//! Called-by: [maintenance::tests::backup, maintenance::tests::drill, maintenance::tests::measure,
//!   maintenance::tests::spike]
//! Invariants: [one scratch source at a time per test process (the 8 GiB Docker VM is shared, D-P); nothing starts
//!   below 1 GiB of free VM memory besides the 768 MiB cap and the reason is `blocked: free=<n> MiB`; the guard exists
//!   before `up`, so its Drop runs `down -v` for the project, `rm -f -v` for every one-shot it named and removes the
//!   host directory on every path, panic included, printing any failed removal; the shared humaux-thread-pg /
//!   humaux-thread-qdrant are never named; the cipher pass and the superuser password are throwaway values
//!   generated here and passed to docker by NAME (environment), never on argv]
//! Spec: Baseline §79.2; ADR-0063 ("Chain stall"); ADR-0064 D-P; ADR-0064 E7

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use humaux_testkit::{ExternalDep, skip_or_fail};

/// The test image tag; rebuilt (layer cache) once per test process, so a Dockerfile edit is always exercised.
pub const IMAGE: &str = "humaux-c37-pg:test";
/// D-P caps: PG 768 MiB, one CPU; 1 GiB must stay free besides.
const MEM_LIMIT_MIB: u64 = 768;
const FLOOR_MIB: u64 = 1024;

static NEXT: AtomicUsize = AtomicUsize::new(0);
static ONE_SOURCE: Mutex<()> = Mutex::new(());

/// The repository root of the checkout.
pub fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// `deploy/compose/backup.yml`, absolute.
pub fn backup_yml() -> PathBuf {
    root().join("deploy/compose/backup.yml")
}

fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// One docker CLI call; `Err` carries its stderr.
pub fn docker(args: &[&str]) -> Result<String, String> {
    // dep: subprocess(docker) — one docker CLI call
    let out = Command::new("docker")
        .args(args)
        .output()
        .map_err(|e| format!("docker {}: {e}", args.join(" ")))?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
    } else {
        Err(format!("docker {}: {}", args.join(" "), text(&out).trim()))
    }
}

/// §79.2: Docker reachable, else a SKIP (or a red under `HUMAUX_REQUIRE_DOCKER=1`); the image is built once.
pub fn docker_ready(test: &str) -> bool {
    if let Err(e) = docker(&["info", "--format", "{{.ServerVersion}}"]) {
        skip_or_fail(
            test,
            &format!("missing object: docker engine ({e})"),
            ExternalDep::Docker,
        );
        return false;
    }
    static BUILT: OnceLock<Result<(), String>> = OnceLock::new();
    let built = BUILT.get_or_init(|| {
        let dir = root().join("deploy/images/postgres");
        docker(&[
            "build",
            "-q",
            "--platform",
            "linux/arm64",
            "-t",
            IMAGE,
            &dir.to_string_lossy(),
        ])
        .map(|_| ())
    });
    if let Err(e) = built {
        panic!("{test}: building {IMAGE} failed: {e}");
    }
    true
}

/// `"123.4MiB / 7.7GiB"` → MiB of the first figure.
fn mib(usage: &str) -> u64 {
    let figure = usage.split('/').next().unwrap_or("").trim();
    let split = figure
        .find(|c: char| c.is_ascii_alphabetic())
        .unwrap_or(figure.len());
    let (number, unit) = figure.split_at(split);
    let n: f64 = number.trim().parse().unwrap_or(0.0);
    let scale = match unit {
        "GiB" | "GB" => 1024.0,
        "KiB" | "kB" => 1.0 / 1024.0,
        "B" => 1.0 / (1024.0 * 1024.0),
        _ => 1.0,
    };
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let mib = (n * scale) as u64;
    mib
}

/// The Docker VM's memory minus what its running containers use, in MiB.
fn vm_free_mib() -> u64 {
    let total: u64 = docker(&["info", "--format", "{{.MemTotal}}"])
        .ok()
        .and_then(|t| t.parse().ok())
        .unwrap_or(0);
    let used: u64 = docker(&["stats", "--no-stream", "--format", "{{.MemUsage}}"])
        .unwrap_or_default()
        .lines()
        .map(mib)
        .sum();
    (total / (1024 * 1024)).saturating_sub(used)
}

/// A throwaway secret (two v4 uuids, hex): role passwords and the cipher pass of a scratch source.
pub fn random_hex() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// A running scratch source; dropping it removes the project, its volumes, its one-shots and its host directory.
pub struct Source {
    /// Compose project = container name = `humaux-c37-<purpose>-<pid>-<n>`.
    pub name: String,
    /// The per-test directory: override files and the repository mount point.
    pub dir: PathBuf,
    /// `HUMAUX_PG_REPO_DIR`: the repository filesystem's mount point (`repo/` lives under it).
    pub repo_dir: PathBuf,
    env: Vec<(String, String)>,
    oneshots: Vec<String>,
    _one: MutexGuard<'static, ()>,
}

impl Drop for Source {
    fn drop(&mut self) {
        for name in &self.oneshots {
            if let Err(e) = docker(&["rm", "-f", "-v", name])
                && !e.contains("No such container")
            {
                eprintln!("containers cleanup: {e}");
            }
        }
        // dep: subprocess(docker) — remove this test's project, its named volumes and its network
        match self.compose(&[], &["down", "-v", "--remove-orphans", "--timeout", "5"]) {
            Ok(out) if out.status.success() => {}
            Ok(out) => eprintln!("containers cleanup: down -v {}: {}", self.name, text(&out)),
            Err(e) => eprintln!("containers cleanup: down -v {}: {e}", self.name),
        }
        // Files pgBackRest created through the bind keep 0640/0750 modes; make them removable first.
        // dep: subprocess(chmod) — this test's own directory only
        let _ = Command::new("chmod")
            .arg("-R")
            .arg("u+rwx")
            .arg(&self.dir)
            .status();
        if let Err(e) = std::fs::remove_dir_all(&self.dir) {
            eprintln!("containers cleanup: rm -rf {}: {e}", self.dir.display());
        }
    }
}

impl Source {
    /// Starts the scratch source with `extra` (a compose override document, may be empty) on top of
    /// `backup.yml` + the cap/label override, and waits (≤ 90 s) for the first-boot initdb to finish. `None` after a
    /// §79.2 skip.
    pub fn start(test: &str, purpose: &str, extra: &str) -> Option<Self> {
        let source = Self::prepare(test, purpose)?;
        source.restart(extra);
        Some(source)
    }

    /// Like [`Source::start`] without starting anything: the test writes files into `dir` (a bind source such as
    /// a shim) and then calls [`Source::restart`]. The guard already owns the project and the directory.
    pub fn prepare(test: &str, purpose: &str) -> Option<Self> {
        if !docker_ready(test) {
            return None;
        }
        let one = ONE_SOURCE.lock().unwrap_or_else(PoisonError::into_inner);
        let free = vm_free_mib();
        assert!(
            free >= FLOOR_MIB + MEM_LIMIT_MIB,
            "{test}: blocked: free={free} MiB"
        );
        let name = format!(
            "humaux-c37-{purpose}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        );
        let dir = std::env::temp_dir().join(&name);
        let repo_dir = dir.join("mnt");
        std::fs::create_dir_all(&repo_dir).expect("scratch directory");
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .expect("a free loopback port")
            .port();
        let env = vec![
            ("HUMAUX_PG_IMAGE".to_owned(), IMAGE.to_owned()),
            ("HUMAUX_PG_CONTAINER".to_owned(), name.clone()),
            ("HUMAUX_PG_PORT".to_owned(), port.to_string()),
            (
                "HUMAUX_PG_ARCHIVE_TIMEOUT_SECONDS".to_owned(),
                "3600".to_owned(),
            ),
            ("HUMAUX_PG_LISTEN_ADDRESSES".to_owned(), String::new()),
            (
                "HUMAUX_PG_REPO_DIR".to_owned(),
                repo_dir.to_string_lossy().into_owned(),
            ),
            ("POSTGRES_PASSWORD".to_owned(), random_hex()),
            ("PGBACKREST_REPO1_CIPHER_PASS".to_owned(), random_hex()),
        ];
        let pid = std::process::id();
        std::fs::write(
            dir.join("caps.yml"),
            format!(
                "services:\n  pg:\n    mem_limit: {MEM_LIMIT_MIB}m\n    cpus: 1\n    labels:\n      humaux.c37: \"{pid}\"\n\
                 volumes:\n  pgdata:\n    labels:\n      humaux.c37: \"{pid}\"\n"
            ),
        )
        .expect("caps override");
        let source = Self {
            name,
            dir,
            repo_dir,
            env,
            oneshots: Vec::new(),
            _one: one,
        };
        Some(source)
    }

    /// `docker compose -f backup.yml -f caps.yml [-f <files>] -p <name> <args>` with this source's environment.
    fn compose(&self, files: &[&Path], args: &[&str]) -> Result<Output, String> {
        // dep: subprocess(docker) — compose against this test's project only
        let mut cmd = Command::new("docker");
        cmd.arg("compose").arg("-f").arg(backup_yml());
        cmd.arg("-f").arg(self.dir.join("caps.yml"));
        for f in files {
            cmd.arg("-f").arg(f);
        }
        cmd.args(["-p", &self.name]).args(args);
        cmd.envs(self.env.iter().map(|(k, v)| (k, v)));
        cmd.output().map_err(|e| format!("docker compose: {e}"))
    }

    /// (Re)creates `pg` with `extra` as the last override (empty = backup.yml as shipped); the volume is kept.
    pub fn up(&self, extra: &str) {
        let extra_file = self.dir.join("extra.yml");
        let files: Vec<&Path> = if extra.is_empty() {
            Vec::new()
        } else {
            std::fs::write(&extra_file, extra).expect("extra override");
            vec![extra_file.as_path()]
        };
        let out = self
            .compose(&files, &["up", "-d", "--force-recreate"])
            .expect("compose up");
        assert!(
            out.status.success(),
            "compose up {}: {}",
            self.name,
            text(&out)
        );
    }

    /// Waits for the entrypoint's first-boot initdb (if any) and then for the final server.
    fn wait_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let logs = self.logs();
            // A first boot runs the entrypoint's temporary server before the real one (two "ready" lines); a
            // restart on an initialised volume prints "Skipping initialization" and starts once.
            let need = if logs.contains("Skipping initialization") {
                1
            } else {
                2
            };
            let booted = logs
                .matches("database system is ready to accept connections")
                .count()
                >= need;
            if booted && self.exec(&["pg_isready", "-q"]).status.success() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{} not ready within 90 s",
                self.name
            );
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    /// Everything the `pg` container logged so far (both streams).
    pub fn logs(&self) -> String {
        // dep: subprocess(docker) — this test's container log
        Command::new("docker")
            .args(["logs", &self.name])
            .output()
            .map(|o| text(&o))
            .unwrap_or_default()
    }

    /// Restarts `pg` with `extra` and waits for it.
    pub fn restart(&self, extra: &str) {
        self.up(extra);
        self.wait_ready();
    }

    /// `argv` as `postgres` inside `pg` (the arms' own exec path).
    pub fn exec(&self, argv: &[&str]) -> Output {
        let mut args = vec!["exec", "-T", "--user", "postgres", "pg"];
        args.extend_from_slice(argv);
        self.compose(&[], &args).expect("compose exec")
    }

    /// `sh -c script` as `postgres`, asserted to succeed; its stdout.
    pub fn sh(&self, script: &str) -> String {
        let out = self.exec(&["sh", "-c", script]);
        assert!(out.status.success(), "{script}: {}", text(&out));
        String::from_utf8_lossy(&out.stdout).trim().to_owned()
    }

    /// One psql statement list over the socket (local trust), asserted to succeed; tuples-only output.
    pub fn psql(&self, sql: &str) -> String {
        let out = self.exec(&["psql", "-qAt", "-v", "ON_ERROR_STOP=1", "-c", sql]);
        assert!(out.status.success(), "{sql}: {}", text(&out));
        String::from_utf8_lossy(&out.stdout).trim().to_owned()
    }

    /// Gives the repository mount point to `postgres` (0750) as runbook go-live step 1 does on the server; a tmpfs
    /// mount point is created root-owned.
    pub fn own_repo_mount(&self) {
        let out = self
            .compose(
                &[],
                &[
                    "exec",
                    "-T",
                    "--user",
                    "root",
                    "pg",
                    "sh",
                    "-c",
                    "chown postgres:postgres /var/lib/pgbackrest && chmod 0750 /var/lib/pgbackrest",
                ],
            )
            .expect("compose exec");
        assert!(
            out.status.success(),
            "chown the repository mount: {}",
            text(&out)
        );
    }

    /// `pgbackrest --stanza=humaux <args>`; the raw output.
    pub fn pgbackrest(&self, args: &[&str]) -> Output {
        let mut argv = vec!["pgbackrest", "--stanza=humaux"];
        argv.extend_from_slice(args);
        self.exec(&argv)
    }

    /// `stanza-create`, asserted.
    pub fn stanza_create(&self) {
        let out = self.pgbackrest(&["stanza-create", "--log-level-console=warn"]);
        assert!(out.status.success(), "stanza-create: {}", text(&out));
    }

    /// A seeded table of about `mib` MiB of poorly compressible rows (md5 text).
    pub fn seed(&self, table: &str, mib: u32) {
        let rows = mib * 1024 * 1024 / 1100;
        self.psql(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (i int, t text); \
             INSERT INTO {table} SELECT g, (SELECT string_agg(md5(random()::text || g || k), '') \
               FROM generate_series(1, 32) k) FROM generate_series(1, {rows}) g"
        ));
    }

    /// The labels of the sets `info` lists, oldest first.
    pub fn labels(&self) -> Vec<String> {
        let out = self.pgbackrest(&["info", "--output=json"]);
        assert!(out.status.success(), "info: {}", text(&out));
        let info: serde_json::Value = serde_json::from_slice(&out.stdout).expect("info json");
        info[0]["backup"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|s| s["label"].as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// `info --output=json`'s stanza object.
    pub fn info(&self) -> serde_json::Value {
        let out = self.pgbackrest(&["info", "--output=json"]);
        assert!(out.status.success(), "info: {}", text(&out));
        let info: serde_json::Value = serde_json::from_slice(&out.stdout).expect("info json");
        info[0].clone()
    }

    /// The pgBackRest repository on the host side (`<mount>/repo`).
    pub fn repo(&self) -> PathBuf {
        self.repo_dir.join("repo")
    }

    /// One `docker run --rm -d` one-shot of the test image named `<name>-<suffix>`, labelled like the source and
    /// removed by this guard; returns the container name.
    pub fn oneshot(&mut self, suffix: &str, args: &[&str]) -> String {
        let name = format!("{}-{suffix}", self.name);
        self.oneshots.push(name.clone());
        let label = format!("humaux.c37={}", std::process::id());
        // dep: subprocess(docker) — a labelled one-shot of the test image
        let mut cmd = Command::new("docker");
        cmd.args([
            "run", "-d", "--name", &name, "--label", &label, "--memory", "512m", "--cpus", "1",
        ]);
        cmd.args(["-e", "PGBACKREST_REPO1_CIPHER_PASS"]);
        cmd.args(args);
        cmd.envs(self.env.iter().map(|(k, v)| (k, v)));
        let out = cmd.output().expect("docker run");
        assert!(out.status.success(), "docker run {name}: {}", text(&out));
        name
    }

    /// `humaux-maintenance <args>` against this source and the receipts database `dsn`, with exactly the backup
    /// arms' environment (plus `extra`), PATH and HOME (docker's own config).
    pub fn maintenance(&self, dsn: &str, args: &[&str], extra: &[(&str, String)]) -> Output {
        // dep: subprocess(humaux-maintenance) — one backup arm run
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_humaux-maintenance"));
        cmd.args(args).env_clear();
        for key in ["PATH", "HOME"] {
            if let Ok(v) = std::env::var(key) {
                cmd.env(key, v);
            }
        }
        cmd.env("HUMAUX_MAINTENANCE_PG_DSN", dsn)
            .env("HUMAUX_MAINTENANCE_BACKUP_COMPOSE_FILE", backup_yml())
            .env("HUMAUX_MAINTENANCE_BACKUP_PROJECT", &self.name)
            .env(
                "HUMAUX_MAINTENANCE_BACKUP_REPO_MAX_BYTES",
                (8_u64 << 30).to_string(),
            )
            .env(
                "HUMAUX_MAINTENANCE_BACKUP_MIN_FREE_BYTES",
                (1_u64 << 20).to_string(),
            );
        cmd.envs(extra.iter().map(|(k, v)| (*k, v)));
        cmd.output().expect("run humaux-maintenance")
    }

    /// `docker compose stop` of `pg` (the volume and the repository stay): the "production is down" state a real
    /// restore requires (ADR-0064 10.11 B).
    pub fn stop(&self) {
        let out = self
            .compose(&[], &["stop", "--timeout", "30"])
            .expect("compose stop");
        assert!(
            out.status.success(),
            "compose stop {}: {}",
            self.name,
            text(&out)
        );
    }

    /// `docker compose down -v` of this project now (T-L1's teardown asserts the repository survives it, 10.11 I).
    pub fn down_volumes(&self) {
        let out = self
            .compose(&[], &["down", "-v", "--remove-orphans", "--timeout", "5"])
            .expect("compose down -v");
        assert!(
            out.status.success(),
            "compose down -v {}: {}",
            self.name,
            text(&out)
        );
    }

    /// Sets one of this source's compose variables (e.g. `HUMAUX_PG_LISTEN_ADDRESSES` = `*` for a TCP source);
    /// takes effect at the next [`Source::restart`].
    pub fn set_env(&mut self, key: &str, value: &str) {
        self.env.retain(|(k, _)| k != key);
        self.env.push((key.to_owned(), value.to_owned()));
    }

    /// One of this source's compose variables (throwaway values only; never printed by the support module).
    pub fn env_value(&self, key: &str) -> String {
        self.env
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    }

    /// The superuser DSN of database `db` on this source's published loopback port (TCP sources only).
    pub fn owner_dsn(&self, db: &str) -> String {
        format!(
            "postgres://postgres:{}@127.0.0.1:{}/{db}",
            self.env_value("POSTGRES_PASSWORD"),
            self.env_value("HUMAUX_PG_PORT")
        )
    }

    /// The pgBackRest version string the image reports (`pgBackRest 2.59.3` → `2.59.3`).
    pub fn pgbackrest_version(&self) -> String {
        let out = self.exec(&["pgbackrest", "version"]);
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .trim_start_matches("pgBackRest ")
            .to_owned()
    }
}

/// stdout + stderr of a finished process, for assertion messages.
pub fn both(out: &Output) -> String {
    text(out)
}
