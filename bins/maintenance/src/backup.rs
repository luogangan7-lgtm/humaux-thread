//! `maintenance::backup` — the one-shot arms `backup check | run | verify | status` (ADR-0064 D-J as amended by
//!   section 10 and 10.11 A, C, E, G; ruling E17: one local posix repository, class `local_only`). Every pgBackRest
//!   command runs inside the production `pg` container through `docker compose -f <file> -p <project> exec -T --user
//!   postgres pg …`; every receipt goes to `ops.backup_receipts` through `adapters::maintenance_repo`. No arm touches a
//!   metric: the gauges are the daemon's, read from these receipts (D-K, card 37 S6).
//! Depends-on: crates=[humaux-adapters, serde_json, time]; services=[PostgreSQL(role_maintenance), subprocess(docker)];
//!   env=[HUMAUX_MAINTENANCE_BACKUP_COMPOSE_FILE, HUMAUX_MAINTENANCE_BACKUP_MIN_FREE_BYTES,
//!   HUMAUX_MAINTENANCE_BACKUP_PROJECT, HUMAUX_MAINTENANCE_BACKUP_REPO_MAX_BYTES, HUMAUX_MAINTENANCE_PG_DSN];
//!   modules=[adapters::maintenance_repo, adapters::postgres, maintenance::main]
//! Called-by: [maintenance::drill, maintenance::main]
//! Invariants: [every arm prints `repo_class` = `local_only` and the NOT OFFSITE notice, both consts here, no config
//!   key, flag or env changes them; `run` keeps the fixed order FS identity + budget precheck on live du/df ->
//!   backup --type=full --no-expire-auto -> verify sequence -> expire only if VERIFIED -> measure -> receipt; a
//!   refusal (exit 3) writes a FAILED receipt and nothing to the repository; VERIFIED is claimed only after
//!   `repo-get` + label identity + `verify --set` all passed and the table derives it; `verify` and `status` never
//!   expire; `status` writes nothing; a cipher subkey never leaves the container, only zero-byte counts do]
//! Spec: Baseline §44; §78.1; ADR-0053 D-F; ADR-0064 D-H; ADR-0064 D-J; ADR-0064 D-V; ADR-0064 E17
//!
//! Exit codes (ADR-0053 D-F): `run` 0 receipt VERIFIED, 1 backup or verify failed (FAILED receipt, nothing
//! expired), 3 refused before writing (`repo_shares_pgdata_fs`, `budget_repo_max:<bytes>`,
//! `budget_free_floor:<bytes>`; FAILED receipt, repository untouched); `check` 0 / 1; `verify` 0 VERIFIED / 1;
//! `status` 0; 2 usage; 1 infrastructure.
//!
//! Spike facts this module rests on (ADR-0064 `MEASURE sp<n>` lines): `verify --set` exits 0 even when a file of
//! the set is invalid and reports it only as `status: error` in its text output (SP-1), so a verification passes
//! only on exit 0 AND `status: ok` AND `backup: <label>, status: valid`; `info --output=json` carries the set size
//! as `backup[].info.repository.size` and the cipher as `repo[].cipher` (SP-11).

use std::process::Command;

use humaux_adapters::maintenance_repo::{self as repo, BackupReceipt, BackupStatusFacts};
use serde_json::{Value, json};
use time::OffsetDateTime;

use crate::{Args, Failure, Output, Result, env, env_parsed, pool};

/// The pgBackRest stanza; pinned to `deploy/pgbackrest/pgbackrest.conf`'s `[humaux]` section by T-H6.
pub const STANZA: &str = "humaux";
/// ADR-0064 E17 / 10.2: the repository class card 37 can have.
// ponytail: one value on purpose (E17); card 37d adds the second value and its derivation
pub const REPO_CLASS: &str = "local_only";
/// ADR-0064 10.2: printed by every arm; host loss = total loss until card 37d's NAS copy exists.
pub const NOT_OFFSITE_NOTICE: &str = "NOT OFFSITE: every backup copy lives on this database host; host loss = total loss / 非异地：所有备份副本都在本数据库主机上，主机丢失 = 全部丢失 (ADR-0064)";

/// The repository filesystem's mount point inside `pg` (the compose bind target, 10.11 A).
const REPO_MOUNT: &str = "/var/lib/pgbackrest";
/// `repo1-path` in the conf (T-H6 pins the conf).
const REPO_PATH: &str = "/var/lib/pgbackrest/repo";
/// `pg1-path` in the conf: the base image's PGDATA.
const PG1_PATH: &str = "/var/lib/postgresql/18/docker";
/// ADR-0064 D-H / 10.11 G: the only cipher `check` accepts for repo1.
const CIPHER: &str = "aes-256-cbc";
/// pgBackRest NEWS (weak subkeys): a subkey with six or more zero bytes is weak.
const WEAK_ZERO_BYTES: u64 = 6;

const COMPOSE_FILE: &str = "HUMAUX_MAINTENANCE_BACKUP_COMPOSE_FILE";
const PROJECT: &str = "HUMAUX_MAINTENANCE_BACKUP_PROJECT";
const REPO_MAX: &str = "HUMAUX_MAINTENANCE_BACKUP_REPO_MAX_BYTES";
const MIN_FREE: &str = "HUMAUX_MAINTENANCE_BACKUP_MIN_FREE_BYTES";

/// One finished command inside `pg`.
pub(crate) struct Ran {
    pub(crate) code: i32,
    pub(crate) stdout: String,
    pub(crate) stderr: String,
}

/// The production `pg` service, addressed by compose file and project (no runner service, 9.3 D-H).
pub(crate) struct Pg {
    file: String,
    project: String,
}

impl Pg {
    pub(crate) fn from_env() -> Result<Self> {
        Ok(Self {
            file: env(COMPOSE_FILE)?,
            project: env(PROJECT)?,
        })
    }

    /// Runs `argv` as `postgres` inside `pg`. Its output is returned, never printed here: a caller forwards what
    /// it may print (a `repo-get` of an info file carries a subkey).
    pub(crate) fn exec(&self, argv: &[&str]) -> Result<Ran> {
        // dep: subprocess(docker) — `docker compose exec` into the production pg container (ADR-0064 9.3 D-H)
        let out = Command::new("docker")
            .args(["compose", "-f", &self.file, "-p", &self.project])
            .args(["exec", "-T", "--user", "postgres", "pg"])
            .args(argv)
            .output()
            .map_err(|e| Failure::Infra(format!("docker compose exec: {e}")))?;
        Ok(Ran {
            code: out.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        })
    }

    pub(crate) fn sh(&self, script: &str) -> Result<Ran> {
        self.exec(&["sh", "-c", script])
    }

    /// `pgbackrest --stanza=humaux <args>`, its log forwarded to stderr (stdout carries only the receipt).
    pub(crate) fn pgbackrest(&self, args: &[&str]) -> Result<Ran> {
        let stanza = format!("--stanza={STANZA}");
        let argv: Vec<&str> = ["pgbackrest", stanza.as_str()]
            .into_iter()
            .chain(args.iter().copied())
            .collect();
        let ran = self.exec(&argv)?;
        eprint!("{}{}", ran.stdout, ran.stderr);
        Ok(ran)
    }

    /// `info --output=json`'s stanza object.
    pub(crate) fn info(&self) -> Result<Value> {
        let ran = self.exec(&[
            "pgbackrest",
            &format!("--stanza={STANZA}"),
            "info",
            "--output=json",
        ])?;
        if ran.code != 0 {
            return Err(Failure::Infra(format!(
                "pgbackrest info exit {}: {}",
                ran.code,
                ran.stderr.trim()
            )));
        }
        let all: Value = serde_json::from_str(&ran.stdout)
            .map_err(|e| Failure::Infra(format!("pgbackrest info json: {e}")))?;
        all.as_array()
            .and_then(|a| a.iter().find(|s| s["name"] == STANZA))
            .cloned()
            .ok_or_else(|| Failure::Infra(format!("pgbackrest info: no stanza {STANZA}")))
    }

    /// 10.11 A: the repository filesystem must not be PGDATA's. `stat` of the mount point (the bind target), so a
    /// repository on the pgdata volume is caught before anything, `repo/` included, exists there.
    fn shares_pgdata_fs(&self) -> Result<bool> {
        let ran = self.exec(&["stat", "-c", "%d", REPO_MOUNT, PG1_PATH])?;
        let devs: Vec<&str> = ran.stdout.split_whitespace().collect();
        match (ran.code, devs.as_slice()) {
            (0, [repo, pgdata]) => Ok(repo == pgdata),
            _ => Err(Failure::Infra(format!(
                "stat of {REPO_MOUNT} / {PG1_PATH} failed: {}",
                ran.stderr.trim()
            ))),
        }
    }

    /// Live `(repo_bytes, repo_free_bytes)`: `du -sk` of the repository, `df -Pk` of its filesystem (10.11 A).
    fn measure(&self) -> Result<(i64, i64)> {
        let ran = self.sh(&format!(
            "du -sk {REPO_PATH} 2>/dev/null | cut -f1 || echo 0; df -Pk {REPO_MOUNT} | tail -1"
        ))?;
        let mut lines = ran.stdout.lines();
        let used = lines.next().and_then(|l| l.trim().parse::<i64>().ok());
        let free = lines
            .next()
            .and_then(|l| l.split_whitespace().nth(3))
            .and_then(|f| f.parse::<i64>().ok());
        match (used, free) {
            (Some(u), Some(f)) => Ok((u * 1024, f * 1024)),
            _ => Err(Failure::Infra(format!(
                "du/df of the repository failed: {}",
                ran.stderr.trim()
            ))),
        }
    }

    /// The first backup's estimate (10.11 A): `du -sk <pg1-path>/base`, an uncompressed upper bound, no grant needed.
    fn base_bytes(&self) -> Result<i64> {
        let ran = self.exec(&["du", "-sk", &format!("{PG1_PATH}/base")])?;
        ran.stdout
            .split_whitespace()
            .next()
            .and_then(|k| k.parse::<i64>().ok())
            .map(|k| k * 1024)
            .ok_or_else(|| Failure::Infra(format!("du of {PG1_PATH}/base: {}", ran.stderr.trim())))
    }
}

/// A pgBackRest set label (`YYYYMMDD-HHMMSSF` and its diff/incr forms): the only text interpolated into a shell line.
pub(crate) fn valid_label(label: &str) -> bool {
    !label.is_empty()
        && label.len() <= 64
        && label
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn labels(info: &Value) -> Vec<String> {
    info["backup"]
        .as_array()
        .map(|sets| {
            sets.iter()
                .filter_map(|s| s["label"].as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn set_of<'a>(info: &'a Value, label: &str) -> Option<&'a Value> {
    info["backup"]
        .as_array()?
        .iter()
        .find(|s| s["label"] == label)
}

fn epoch(value: &Value) -> Option<OffsetDateTime> {
    value
        .as_i64()
        .and_then(|s| OffsetDateTime::from_unix_timestamp(s).ok())
}

fn unix(t: Option<OffsetDateTime>) -> Value {
    t.map_or(Value::Null, |t| json!(t.unix_timestamp()))
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(text: &str) -> Option<Vec<u8>> {
    (text.len() == 64)
        .then(|| {
            (0..64)
                .step_by(2)
                .map(|i| u8::from_str_radix(&text[i..i + 2], 16).ok())
                .collect::<Option<Vec<u8>>>()
        })
        .flatten()
}

/// The pass rule of `verify --set` (SP-1: an invalid file still exits 0): exit 0 AND the stanza `status: ok` AND
/// the set `status: valid`; otherwise the named failure. Shared with `restore pitr`'s pre-restore verify (D-U).
pub(crate) fn verify_failure(code: i32, stdout: &str, label: &str) -> Option<String> {
    let ok = stdout.lines().any(|l| l.trim() == "status: ok")
        && stdout.contains(&format!("backup: {label}, status: valid,"));
    if code != 0 {
        Some(format!("verify_exit:{code}"))
    } else if !ok {
        Some("verify_invalid".to_owned())
    } else {
        None
    }
}

/// What the verify sequence (D-J steps 1–4) established for one set.
#[derive(Default)]
struct Verified {
    manifest: Option<Vec<u8>>,
    verify_exit: Option<i32>,
    verified: Option<Vec<u8>>,
    failure: Option<String>,
}

/// ADR-0064 D-J verify sequence: (1) sha256 of the set's `backup.manifest` pulled back with `repo-get` (decrypted
/// inside `pg`, only the digest leaves); (2) label identity against `ops.backup_sets` BEFORE pgBackRest's verify, so
/// a manifest overwritten after its first verification fails `manifest_changed` (T-J5); (3) `verify --set` must exit
/// 0 AND report the stanza `status: ok` and the set `status: valid` (SP-1: an invalid file still exits 0); (4) the
/// first clean verification binds the label's identity.
async fn verify_set(
    pg: &Pg,
    pool: &humaux_adapters::postgres::MaintenanceDbPool,
    label: &str,
) -> Result<Verified> {
    let mut v = Verified::default();
    let pulled = pg.sh(&format!(
        "{{ pgbackrest --stanza={STANZA} --log-level-console=warn repo-get backup/{STANZA}/{label}/backup.manifest; \
         echo \"repo_get_exit=$?\" >&2; }} | sha256sum | cut -c1-64"
    ))?;
    let manifest = (pulled.stderr.contains("repo_get_exit=0"))
        .then(|| unhex(pulled.stdout.trim()))
        .flatten();
    let Some(manifest) = manifest else {
        eprint!("{}", pulled.stderr);
        v.failure = Some("manifest_unreadable".to_owned());
        return Ok(v);
    };
    v.manifest = Some(manifest.clone());
    let first = repo::first_verified_manifest(pool, label)
        .await
        .map_err(|e| Failure::Infra(format!("backup set identity: {e}")))?;
    if first.as_ref().is_some_and(|f| *f != manifest) {
        v.failure = Some("manifest_changed".to_owned());
        return Ok(v);
    }
    let ran = pg.pgbackrest(&[
        "verify",
        &format!("--set={label}"),
        "--output=text",
        "--verbose",
        "--log-level-console=warn",
    ])?;
    v.verify_exit = Some(ran.code);
    v.failure = verify_failure(ran.code, &ran.stdout, label);
    if v.failure.is_none() {
        repo::bind_backup_set(pool, label, &manifest)
            .await
            .map_err(|e| Failure::Infra(format!("backup set identity: {e}")))?;
        v.verified = Some(manifest);
    }
    Ok(v)
}

/// The fields every arm prints (ADR-0064 10.2).
fn class_fields(arm: &str) -> serde_json::Map<String, Value> {
    let mut m = serde_json::Map::new();
    m.insert("arm".to_owned(), json!(arm));
    m.insert("repo_class".to_owned(), json!(REPO_CLASS));
    m.insert("notice".to_owned(), json!(NOT_OFFSITE_NOTICE));
    m
}

async fn record(
    pool: &humaux_adapters::postgres::MaintenanceDbPool,
    receipt: &BackupReceipt,
) -> Result<String> {
    repo::insert_backup_receipt(pool, receipt)
        .await
        .map(|id| id.to_string())
        .map_err(|e| Failure::Infra(format!("backup receipts: {e}")))
}

/// `backup check` (10.11 G): FS identity, `pgbackrest check`, repo1's cipher, the NEWS weak-subkey count.
pub async fn check(_args: &Args) -> Result<Output> {
    let pg = Pg::from_env()?;
    let mut receipt = class_fields("backup check");
    let mut once = Vec::new();
    let failure = check_steps(&pg, &mut receipt, &mut once)?;
    receipt.insert(
        "outcome".to_owned(),
        json!(if failure.is_none() { "ok" } else { "failed" }),
    );
    receipt.insert("failure".to_owned(), json!(failure));
    Ok(Output {
        failed: failure.is_some(),
        receipt: Value::Object(receipt),
        refused: false,
        once,
    })
}

fn check_steps(
    pg: &Pg,
    receipt: &mut serde_json::Map<String, Value>,
    once: &mut Vec<String>,
) -> Result<Option<String>> {
    if pg.shares_pgdata_fs()? {
        return Ok(Some("repo_shares_pgdata_fs".to_owned()));
    }
    let ran = pg.pgbackrest(&["check"])?;
    if ran.code != 0 {
        return Ok(Some(format!("check_exit:{}", ran.code)));
    }
    let info = pg.info()?;
    let cipher = info["repo"]
        .as_array()
        .and_then(|r| r.iter().find(|r| r["key"] == 1))
        .and_then(|r| r["cipher"].as_str())
        .unwrap_or("none")
        .to_owned();
    once.push(format!("repo_cipher={cipher}"));
    receipt.insert("repo_cipher".to_owned(), json!(cipher));
    if cipher != CIPHER {
        return Ok(Some("repo_not_encrypted".to_owned()));
    }
    // pgBackRest NEWS (weak subkeys): the subkey of each info file is decoded and its zero bytes counted INSIDE pg;
    // only `<bytes> <zeros>` per file leaves the container, never the subkey (T-J12).
    let ran = pg.sh(&format!(
        "for f in backup/{STANZA}/backup.info archive/{STANZA}/archive.info; do \
           pgbackrest --stanza={STANZA} --log-level-console=warn repo-get \"$f\" \
           | sed -n 's/^cipher-pass=\"\\{{0,1\\}}\\([^\"]*\\)\"\\{{0,1\\}}$/\\1/p' | base64 -d \
           | od -An -v -tu1 | tr -s ' ' '\\n' | awk 'NF {{ n++; if ($1 == 0) z++ }} END {{ print n + 0, z + 0 }}'; \
         done"
    ))?;
    let counts: Vec<(u64, u64)> = ran
        .stdout
        .lines()
        .filter_map(|l| {
            let mut it = l.split_whitespace().map(|x| x.parse::<u64>().ok());
            Some((it.next()??, it.next()??))
        })
        .collect();
    // A subkey that could not be read decodes to 0 bytes: that is a failed check, never `weak_subkeys=0`.
    if counts.len() != 2 || counts.iter().any(|(bytes, _)| *bytes == 0) {
        return Ok(Some("subkey_unreadable".to_owned()));
    }
    let weak = counts.iter().filter(|(_, z)| *z >= WEAK_ZERO_BYTES).count();
    once.push(format!("weak_subkeys={weak}"));
    receipt.insert("weak_subkeys".to_owned(), json!(weak));
    Ok((weak > 0).then(|| format!("weak_subkeys:{weak}")))
}

/// `backup run` (D-J steps 1–6 as 10.11 A orders them). Exit 0 iff the receipt is VERIFIED.
pub async fn run(_args: &Args) -> Result<Output> {
    let pg = Pg::from_env()?;
    let repo_max: i64 = env_parsed(REPO_MAX)?;
    let min_free: i64 = env_parsed(MIN_FREE)?;
    let pool = pool().await?;
    let mut receipt = BackupReceipt {
        repo_max_bytes: Some(repo_max),
        min_free_bytes: Some(min_free),
        ..BackupReceipt::default()
    };
    let mut out = class_fields("backup run");

    if let Some(reason) = precheck(&pg, &pool, &mut receipt, (repo_max, min_free)).await? {
        receipt.failure = Some(reason.clone());
        out.insert(
            "receipt_id".to_owned(),
            json!(record(&pool, &receipt).await?),
        );
        out.insert("outcome".to_owned(), json!("refused"));
        out.insert("reason".to_owned(), json!(reason));
        budget_fields(&mut out, &receipt);
        return Ok(Output {
            receipt: Value::Object(out),
            refused: true,
            failed: false,
            once: Vec::new(),
        });
    }

    // Step 2: one full set; expire-auto is off on the command line too, so a conf edit cannot expire early.
    let before = labels(&pg.info()?);
    let ran = pg.pgbackrest(&["backup", "--type=full", "--no-expire-auto"])?;
    let info = pg.info()?;
    let new: Vec<String> = labels(&info)
        .into_iter()
        .filter(|l| !before.contains(l))
        .collect();
    let label = match (ran.code, new.as_slice()) {
        (0, [label]) if valid_label(label) => Some(label.clone()),
        (0, _) => {
            receipt.failure = Some("backup_label_unknown".to_owned());
            None
        }
        (code, _) => {
            receipt.failure = Some(format!("backup_exit:{code}"));
            new.first().filter(|l| valid_label(l)).cloned()
        }
    };
    if let Some(label) = &label {
        let set = set_of(&info, label);
        receipt.backup_label = Some(label.clone());
        receipt.backup_started_at = set.and_then(|s| epoch(&s["timestamp"]["start"]));
        receipt.backup_stopped_at = set.and_then(|s| epoch(&s["timestamp"]["stop"]));
        receipt.set_repo_bytes = set.and_then(|s| s["info"]["repository"]["size"].as_i64());
    }

    // Step 3: the verify sequence, only for a set the backup completed.
    if let (Some(label), None) = (&label, &receipt.failure) {
        let v = verify_set(&pg, &pool, label).await?;
        receipt.manifest_sha256 = v.manifest;
        receipt.verify_exit = v.verify_exit;
        receipt.verified_manifest_sha256 = v.verified;
        receipt.failure = v.failure;
    }

    // Step 4: expire ONLY after a VERIFIED set (E16 invariant 5: a failed verification expires nothing, T-J7).
    let mut expire_failed = None;
    if receipt.failure.is_none() {
        let ran = pg.pgbackrest(&["expire"])?;
        if ran.code != 0 {
            expire_failed = Some(format!("expire_exit:{}", ran.code));
        }
    }

    // Step 5: measure after the run; step 6: the receipt (outcome derived by the table).
    let (repo_bytes, repo_free) = pg.measure()?;
    receipt.repo_bytes = Some(repo_bytes);
    receipt.repo_free_bytes = Some(repo_free);
    out.insert(
        "receipt_id".to_owned(),
        json!(record(&pool, &receipt).await?),
    );
    let verified = receipt.failure.is_none();
    out.insert(
        "outcome".to_owned(),
        json!(if verified { "VERIFIED" } else { "FAILED" }),
    );
    out.insert("expire_failed".to_owned(), json!(expire_failed));
    set_fields(&mut out, &receipt);
    Ok(Output {
        receipt: Value::Object(out),
        refused: false,
        failed: !verified || expire_failed.is_some(),
        once: Vec::new(),
    })
}

/// Step 1 of `run` (10.11 A): FS identity, then the budget precheck on live numbers measured in `pg` now. Fills the
/// receipt's live numbers and estimate; `Some(reason)` is a refusal, and nothing has been written to the repository.
async fn precheck(
    pg: &Pg,
    pool: &humaux_adapters::postgres::MaintenanceDbPool,
    receipt: &mut BackupReceipt,
    (repo_max, min_free): (i64, i64),
) -> Result<Option<String>> {
    if pg.shares_pgdata_fs()? {
        return Ok(Some("repo_shares_pgdata_fs".to_owned()));
    }
    let (repo_bytes, repo_free) = pg.measure()?;
    let estimate = estimate(pg, pool).await?;
    receipt.repo_bytes = Some(repo_bytes);
    receipt.repo_free_bytes = Some(repo_free);
    receipt.estimate_bytes = Some(estimate);
    Ok(if repo_bytes + estimate > repo_max {
        Some(format!("budget_repo_max:{}", repo_bytes + estimate))
    } else if repo_free - estimate < min_free {
        Some(format!("budget_free_floor:{}", repo_free - estimate))
    } else {
        None
    })
}

/// ADR-0064 D-V: newest VERIFIED set bytes × 1.25, or for the first backup `du -sk <pg1-path>/base` (10.11 A).
async fn estimate(pg: &Pg, pool: &humaux_adapters::postgres::MaintenanceDbPool) -> Result<i64> {
    match repo::newest_verified_set_repo_bytes(pool)
        .await
        .map_err(|e| Failure::Infra(format!("backup receipts: {e}")))?
    {
        Some(set) => Ok(repo::estimate_from_set_bytes(set)),
        None => pg.base_bytes(),
    }
}

/// The set and budget fields of a `run` receipt as printed.
fn set_fields(out: &mut serde_json::Map<String, Value>, r: &BackupReceipt) {
    out.insert("failure".to_owned(), json!(r.failure));
    out.insert("backup_label".to_owned(), json!(r.backup_label));
    out.insert("backup_started_at".to_owned(), unix(r.backup_started_at));
    out.insert("backup_stopped_at".to_owned(), unix(r.backup_stopped_at));
    out.insert(
        "manifest_sha256".to_owned(),
        json!(r.manifest_sha256.as_deref().map(hex)),
    );
    out.insert("set_repo_bytes".to_owned(), json!(r.set_repo_bytes));
    budget_fields(out, r);
}

fn budget_fields(out: &mut serde_json::Map<String, Value>, r: &BackupReceipt) {
    out.insert("repo_bytes".to_owned(), json!(r.repo_bytes));
    out.insert("repo_free_bytes".to_owned(), json!(r.repo_free_bytes));
    out.insert("repo_max_bytes".to_owned(), json!(r.repo_max_bytes));
    out.insert("min_free_bytes".to_owned(), json!(r.min_free_bytes));
    out.insert("estimate_bytes".to_owned(), json!(r.estimate_bytes));
}

/// `backup verify [--label <set>]`: the verify sequence for one set (default: the newest in `info`), one receipt.
/// Never expires and never measures the repository (the budget numbers are `run`'s, D-K).
pub async fn verify(args: &Args) -> Result<Output> {
    let pg = Pg::from_env()?;
    let wanted = args.get("--label");
    if wanted.as_deref().is_some_and(|l| !valid_label(l)) {
        return Err(Failure::Usage(
            "--label: not a pgBackRest set label".to_owned(),
        ));
    }
    let pool = pool().await?;
    let info = pg.info()?;
    let label = match wanted {
        Some(l) => l,
        None => labels(&info)
            .pop()
            .ok_or_else(|| Failure::Infra("pgbackrest info lists no backup set".to_owned()))?,
    };
    let set = set_of(&info, &label);
    let mut receipt = BackupReceipt {
        backup_label: Some(label.clone()),
        backup_started_at: set.and_then(|s| epoch(&s["timestamp"]["start"])),
        backup_stopped_at: set.and_then(|s| epoch(&s["timestamp"]["stop"])),
        ..BackupReceipt::default()
    };
    if set.is_none() {
        receipt.failure = Some("set_not_in_repository".to_owned());
    } else {
        let v = verify_set(&pg, &pool, &label).await?;
        receipt.manifest_sha256 = v.manifest;
        receipt.verify_exit = v.verify_exit;
        receipt.verified_manifest_sha256 = v.verified;
        receipt.failure = v.failure;
    }
    let mut out = class_fields("backup verify");
    out.insert(
        "receipt_id".to_owned(),
        json!(record(&pool, &receipt).await?),
    );
    out.insert("backup_label".to_owned(), json!(label));
    let verified = receipt.failure.is_none();
    out.insert(
        "outcome".to_owned(),
        json!(if verified { "VERIFIED" } else { "FAILED" }),
    );
    out.insert("failure".to_owned(), json!(receipt.failure));
    Ok(Output {
        receipt: Value::Object(out),
        refused: false,
        failed: !verified,
        once: Vec::new(),
    })
}

/// `backup status`, read-only (T-J4): class and notice, the newest VERIFIED set, the live budget numbers and both
/// headrooms, `wal_archive_failing`, and the PITR window (10.11 E).
pub async fn status(_args: &Args) -> Result<Output> {
    let pg = Pg::from_env()?;
    let repo_max: i64 = env_parsed(REPO_MAX)?;
    let min_free: i64 = env_parsed(MIN_FREE)?;
    let pool = pool().await?;
    let facts = repo::backup_status_facts(&pool)
        .await
        .map_err(|e| Failure::Infra(format!("backup status facts: {e}")))?;
    let info = pg.info()?;
    let (repo_bytes, repo_free) = pg.measure()?;
    let estimate = estimate(&pg, &pool).await?;
    let now = OffsetDateTime::now_utc();
    let newest = facts
        .sets
        .iter()
        .filter(|s| s.verified)
        .max_by_key(|s| s.stopped_at);
    let mut out = class_fields("backup status");
    out.insert(
        "newest_verified".to_owned(),
        newest.map_or(Value::Null, |s| {
            json!({
                "backup_label": s.label,
                "backup_stopped_at": unix(s.stopped_at),
                "age_seconds": s.stopped_at.map(|t| (now - t).whole_seconds()),
            })
        }),
    );
    out.insert("repo_bytes".to_owned(), json!(repo_bytes));
    out.insert("repo_max_bytes".to_owned(), json!(repo_max));
    out.insert("repo_free_bytes".to_owned(), json!(repo_free));
    out.insert("min_free_bytes".to_owned(), json!(min_free));
    out.insert("estimate_bytes".to_owned(), json!(estimate));
    out.insert(
        "headroom_bytes".to_owned(),
        json!({ "repo_max": repo_max - repo_bytes - estimate,
                "free_floor": repo_free - estimate - min_free }),
    );
    let failing = facts.latched_since.or(facts.failing_now_since);
    out.insert(
        "wal_archive_failing".to_owned(),
        json!(u8::from(failing.is_some())),
    );
    out.insert(
        "pitr_window_unbroken_since".to_owned(),
        pitr_window(&facts, &info, failing),
    );
    Ok(Output::ok(Value::Object(out)))
}

/// 10.11 E: the start of the oldest full present in `info` whose latest receipt is VERIFIED and that started after
/// every recorded WAL-archive failure; `broken since …` while the latch holds; `none` when no set qualifies.
fn pitr_window(facts: &BackupStatusFacts, info: &Value, failing: Option<OffsetDateTime>) -> Value {
    if let Some(since) = failing {
        return json!(format!(
            "broken since {}: run humaux-dr.sh backup run",
            since.unix_timestamp()
        ));
    }
    let present = labels(info);
    facts
        .sets
        .iter()
        .filter(|s| s.verified && present.contains(&s.label))
        .filter_map(|s| set_of(info, &s.label).and_then(|i| epoch(&i["timestamp"]["start"])))
        .filter(|start| facts.newest_failure.is_none_or(|f| *start > f))
        .min()
        .map_or(json!("none"), |t| json!(t.unix_timestamp()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The only text a shell line interpolates is a set label; anything else is refused before a container runs.
    #[test]
    fn only_set_labels_reach_a_shell_line() {
        assert!(valid_label("20261005-215916F"));
        assert!(valid_label("20261005-215916F_20261006-001500D"));
        for bad in ["", "x;rm -rf /", "../x", "a b", "$(id)"] {
            assert!(!valid_label(bad), "{bad:?}");
        }
    }

    /// `unhex` accepts exactly a 64-hex sha256 line, so an empty or truncated `sha256sum` is never a digest.
    #[test]
    fn a_manifest_digest_is_exactly_64_hex() {
        let d = "e02028a503c30577d76377387d9c122fe94336ed884f15d053defc6ba9394851";
        assert_eq!(unhex(d).map(|b| hex(&b)), Some(d.to_owned()));
        assert_eq!(unhex(&d[..63]), None);
        assert_eq!(unhex("zz"), None);
    }

    /// T-J3b: each half of the pass rule fails alone. A non-zero exit with a fully valid report is `verify_exit:<n>`
    /// (the corrupt-file fixture of T-J3 exits 0 per SP-1, so it never reaches this branch); exit 0 with an invalid
    /// report is `verify_invalid`; exit 0 with both statuses passes. Fault: `code != 0` → `false` → red.
    #[test]
    fn verify_needs_exit_zero_and_a_valid_report() {
        let label = "20261006-003000F";
        let valid = format!(
            "stanza: humaux\n    status: ok\n    backup: {label}, status: valid, total-files: 9\n"
        );
        assert_eq!(
            verify_failure(1, &valid, label).as_deref(),
            Some("verify_exit:1")
        );
        assert_eq!(
            verify_failure(0, &valid.replace("valid,", "invalid,"), label).as_deref(),
            Some("verify_invalid")
        );
        assert_eq!(verify_failure(0, &valid, label), None);
    }
}
