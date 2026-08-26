//! xtask `migration-rehearsal` — G46-1 / G80-37 (§46.1 Migration Rehearsal Contract).
//!
//! Static side (this module, always runs): enumerate `migrations/*.sql`, assert each
//! has exactly one same-stem `<stem>.manifest.toml`, and validate the manifest's
//! required fields (`migration_id`/`class`/`precheck`/`postcheck`/
//! `rollback_or_forward_fix`/`backup_restore_requirement`). Missing manifest, orphan
//! manifest, or an illegal field ⇒ `fail`, each error names its file (§57.1 rule 2).
//!
//! Execution side (spin up Postgres, run `up -> down -> up`, diff
//! `schema_digest_before/after`) needs live DB infrastructure Phase 0 does not have —
//! per §57.1 rule 3 it reports `not_applicable` naming the missing object, never a
//! silent skip.

use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

/// §46.1: the three migration classes a manifest must declare, closed enum.
///
/// - `Reversible`: `up -> down -> up`; the two `up` runs must leave an identical schema digest.
/// - `ExpandContract`: expand/migrate/switch/contract; `contract` only after the observation window.
/// - `ForwardOnly`: no faked `down`; requires a pre-migration restore point + forward-fix rehearsal.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MigrationClass {
    Reversible,
    ExpandContract,
    ForwardOnly,
}

impl MigrationClass {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "REVERSIBLE" => Some(Self::Reversible),
            "EXPAND_CONTRACT" => Some(Self::ExpandContract),
            "FORWARD_ONLY" => Some(Self::ForwardOnly),
            _ => None,
        }
    }
}

/// §46.1: required manifest fields for one `migrations/*.sql` file.
///
/// `schema_digest_before/after` are computed at rehearsal-execution time, not authored
/// statically, so they are not part of this struct.
// ponytail: precheck/postcheck/rollback_or_forward_fix/backup_restore_requirement are
// validated-present but only read by the rehearsal *execution* runner (not built this
// phase, §57.1 rule 3) — allow(dead_code) instead of dropping the fields now and
// re-adding them later.
#[derive(Debug)]
#[allow(dead_code)]
pub struct Manifest {
    pub migration_id: String,
    pub class: MigrationClass,
    pub precheck: String,
    pub postcheck: String,
    pub rollback_or_forward_fix: String,
    pub backup_restore_requirement: String,
}

/// One static-check failure. Always names the offending file (§57.1 rule 2: a
/// `not_applicable`/`fail` that cannot point at the object is "扫不到当通过").
#[derive(Debug)]
pub struct CheckError {
    pub file: String,
    pub reason: String,
}

impl fmt::Display for CheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.file, self.reason)
    }
}

/// §46.1: the manifest's frozen required-field set (besides `class`, checked separately
/// against the closed enum).
const REQUIRED_STRING_FIELDS: &[&str] = &[
    "migration_id",
    "precheck",
    "postcheck",
    "rollback_or_forward_fix",
    "backup_restore_requirement",
];

/// §46.1: manifest is the sibling `<stem>.manifest.toml` next to `<stem>.sql`.
fn manifest_path_for(sql_path: &Path) -> PathBuf {
    sql_path.with_extension("manifest.toml")
}

/// Inverse of [`manifest_path_for`]: which `<stem>.sql` a manifest file claims to describe.
fn expected_sql_for(manifest_path: &Path) -> PathBuf {
    let name = manifest_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy();
    let stem = name.strip_suffix(".manifest.toml").unwrap_or(&name);
    manifest_path.with_file_name(format!("{stem}.sql"))
}

/// Parse and validate one manifest's required fields (§46.1).
///
/// `file` is used only for error attribution, not read from disk here.
pub fn parse_manifest(file: &str, contents: &str) -> Result<Manifest, CheckError> {
    let err = |reason: String| CheckError {
        file: file.to_string(),
        reason,
    };

    let value: toml::Value =
        toml::from_str(contents).map_err(|e| err(format!("invalid TOML: {e}")))?;
    let table = value
        .as_table()
        .ok_or_else(|| err("manifest root must be a TOML table".to_string()))?;

    let mut fields = HashMap::with_capacity(REQUIRED_STRING_FIELDS.len());
    for key in REQUIRED_STRING_FIELDS {
        let v = table
            .get(*key)
            .and_then(|v| v.as_str())
            .ok_or_else(|| err(format!("missing required field `{key}`")))?;
        fields.insert(*key, v.to_string());
    }

    let class_raw = table
        .get("class")
        .and_then(|v| v.as_str())
        .ok_or_else(|| err("missing required field `class`".to_string()))?;
    let class = MigrationClass::parse(class_raw).ok_or_else(|| {
        err(format!(
            "class `{class_raw}` not in {{REVERSIBLE, EXPAND_CONTRACT, FORWARD_ONLY}}"
        ))
    })?;

    Ok(Manifest {
        migration_id: fields.remove("migration_id").expect("checked above"),
        class,
        precheck: fields.remove("precheck").expect("checked above"),
        postcheck: fields.remove("postcheck").expect("checked above"),
        rollback_or_forward_fix: fields
            .remove("rollback_or_forward_fix")
            .expect("checked above"),
        backup_restore_requirement: fields
            .remove("backup_restore_requirement")
            .expect("checked above"),
    })
}

/// Static side of G46-1 / G80-37: enumerate `migrations_dir/*.sql`, assert each has
/// exactly one same-stem manifest, and validate that manifest's required fields.
/// Also flags orphan `*.manifest.toml` files with no matching `.sql` (§46.1 "恰好一个").
///
/// Returns every failure found (not just the first, so one CI run surfaces the whole
/// backlog) plus every successfully parsed [`Manifest`] as pass evidence — `errors`
/// empty = `pass`.
pub fn check_static(migrations_dir: &Path) -> (Vec<Manifest>, Vec<CheckError>) {
    let mut manifests = Vec::new();
    let mut errors = Vec::new();
    let entries = match fs::read_dir(migrations_dir) {
        Ok(entries) => entries,
        Err(e) => {
            errors.push(CheckError {
                file: migrations_dir.display().to_string(),
                reason: format!("cannot read migrations dir: {e}"),
            });
            return (manifests, errors);
        }
    };

    let mut sql_files = Vec::new();
    let mut manifest_files = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        if name.ends_with(".manifest.toml") {
            manifest_files.push(path);
        } else if path.extension().and_then(|e| e.to_str()) == Some("sql") {
            sql_files.push(path);
        }
    }

    for sql in &sql_files {
        let sql_name = sql
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let manifest_path = manifest_path_for(sql);
        if !manifest_path.is_file() {
            errors.push(CheckError {
                file: sql_name,
                reason: format!(
                    "missing manifest {} (§46.1: every migrations/*.sql needs exactly one)",
                    manifest_path
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                ),
            });
            continue;
        }
        let manifest_name = manifest_path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        match fs::read_to_string(&manifest_path) {
            Ok(contents) => match parse_manifest(&manifest_name, &contents) {
                Ok(m) => manifests.push(m),
                Err(e) => errors.push(e),
            },
            Err(e) => errors.push(CheckError {
                file: manifest_name,
                reason: format!("cannot read manifest: {e}"),
            }),
        }
    }

    for manifest in &manifest_files {
        if !expected_sql_for(manifest).is_file() {
            errors.push(CheckError {
                file: manifest
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string(),
                reason: "orphan manifest: no matching migrations/*.sql (§46.1 恰好一个)"
                    .to_string(),
            });
        }
    }

    (manifests, errors)
}

/// `cargo xtask migration-rehearsal` — G46-1 / G80-37 (§46.1).
///
/// Static manifest contract always runs and can `fail`. Rehearsal execution
/// （`up -> down -> up` + 逐 class 比对 schema digest）目前仍是 `not_applicable`——
/// 但**缺的是执行器，不是库**，见 [`rehearsal_execution_verdict`]。
pub fn run(_args: &[String]) -> i32 {
    let migrations_dir = Path::new("migrations");
    let (manifests, errors) = check_static(migrations_dir);
    if !errors.is_empty() {
        eprintln!(
            "migration-rehearsal: fail ({} static error(s))",
            errors.len()
        );
        for e in &errors {
            eprintln!("  {e}");
        }
        return 1;
    }
    eprintln!(
        "migration-rehearsal: static pass ({} manifest(s), G46-1 contract satisfied)",
        manifests.len()
    );
    for m in &manifests {
        eprintln!("  {} class={:?}", m.migration_id, m.class);
    }
    eprintln!("{}", rehearsal_execution_verdict(database_dsn().is_some()));
    0
}

/// 本次运行有没有可用的库 DSN。§78 的 env 扫描把 `xtask/` 整个豁免（它是 CI 闸工具本身），
/// 所以这里直读环境变量是允许的。
fn database_dsn() -> Option<String> {
    ["HUMAUX_TEST_PG_DSN", "DATABASE_URL"]
        .iter()
        .find_map(|k| std::env::var(k).ok().filter(|v| !v.is_empty()))
}

/// Rehearsal 执行段的三态措辞。**纯函数，便于直接对两种世界断言**。
///
/// 先前这里是一句**写死的**常量：「missing object: live PostgreSQL instance … Phase 0 has
/// no runtime DB infrastructure」。它点错了对象，而且那个错会**永远为真**——无论环境怎么
/// 变，那个字符串都说"没有库"。ADR-0005 给 CI 配上 Postgres service 之后，它声称缺失的
/// 东西根本不缺了，可它一个字都不会变。
///
/// §57.1 第2条要求 NA「打印缺失对象名」，而缺失对象得是**探测出来的**，不是作者当年写下
/// 的判断（ADR-0006：NA 的主语选错，闸就会在情况变化后继续沉默）。所以改成据实分辨：
/// - 没有 DSN ⇒ 缺的是库；
/// - 有 DSN ⇒ 库在，缺的是**执行器本身**（DOD-089，phase=16 尚未交付）。
///
/// 两种情形都仍是 `not_applicable`（执行器确实没交付），但说的是实话，而且执行器落地那天
/// 这句话会自己变——不需要有人记得回来改一句注释。
fn rehearsal_execution_verdict(has_dsn: bool) -> String {
    if has_dsn {
        "migration-rehearsal: rehearsal execution not_applicable (missing object: \
         rehearsal executor itself — up/down/up + schema_digest_before/after 比对尚未实现，\
         DOD-089 phase=16。**库不是瓶颈**：本次运行已有可用 DSN)"
            .to_string()
    } else {
        "migration-rehearsal: rehearsal execution not_applicable (missing object: \
         HUMAUX_TEST_PG_DSN / DATABASE_URL — 没有库可以 rehearse。执行器本身也尚未实现，\
         DOD-089 phase=16)"
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 两种世界必须说不同的话。先前那句写死的常量对两种世界说同一句，而且那句在有库时
    /// 是**假的**——ADR-0005 给 CI 配上 Postgres service 之后它依然宣称「没有库」。
    #[test]
    fn rehearsal_verdict_names_the_object_that_is_actually_missing() {
        let without = rehearsal_execution_verdict(false);
        let with = rehearsal_execution_verdict(true);

        assert!(without.contains("HUMAUX_TEST_PG_DSN"), "{without}");
        assert!(
            with.contains("rehearsal executor itself"),
            "有库时缺的是执行器，不该再说缺库: {with}"
        );
        assert!(
            !with.contains("HUMAUX_TEST_PG_DSN /"),
            "有库时不得把库列为缺失对象: {with}"
        );
        assert_ne!(without, with, "两种世界说同一句话 = 这条判定其实没在看环境");
        // §57.1 第2条：两种情形都必须打印缺失对象名。
        for v in [&without, &with] {
            assert!(v.contains("missing object"), "{v}");
            assert!(v.contains("not_applicable"), "{v}");
        }
    }

    fn fixture(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/migration_rehearsal")
            .join(name)
    }

    #[test]
    fn missing_manifest_is_red() {
        let (_, errors) = check_static(&fixture("missing_manifest"));
        assert!(!errors.is_empty(), "sql without manifest must fail G46-1");
        assert!(errors.iter().any(|e| e.file == "0002_add_col.sql"));
    }

    #[test]
    fn present_valid_manifest_is_green() {
        let (manifests, errors) = check_static(&fixture("ok"));
        assert!(
            errors.is_empty(),
            "valid manifest set must pass: {errors:?}"
        );
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].migration_id, "0001_init");
        assert_eq!(manifests[0].class, MigrationClass::Reversible);
    }

    /// §80.1: a gate without a red→green fault-injection record does not count as
    /// existing. Exercised here directly (not via the fixture dirs above) so the same
    /// migration goes from missing manifest (red) to valid manifest (green).
    #[test]
    fn red_to_green_after_adding_manifest() {
        let dir =
            std::env::temp_dir().join(format!("xtask_migr_rehearsal_r2g_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("0001_x.sql"), "-- noop").unwrap();

        let (_, red) = check_static(&dir);
        assert!(!red.is_empty(), "no manifest yet must be red");

        fs::write(
            dir.join("0001_x.manifest.toml"),
            r#"
migration_id = "0001_x"
class = "REVERSIBLE"
precheck = "select 1"
postcheck = "select 1"
rollback_or_forward_fix = "down migration reverts the added column"
backup_restore_requirement = "pg_dump before apply"
"#,
        )
        .unwrap();

        let (_, green) = check_static(&dir);
        assert!(green.is_empty(), "manifest added must be green: {green:?}");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn illegal_class_is_red() {
        let dir =
            std::env::temp_dir().join(format!("xtask_migr_rehearsal_class_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("0001_y.sql"), "-- noop").unwrap();
        fs::write(
            dir.join("0001_y.manifest.toml"),
            r#"
migration_id = "0001_y"
class = "SOMETHING_ELSE"
precheck = "select 1"
postcheck = "select 1"
rollback_or_forward_fix = "n/a"
backup_restore_requirement = "n/a"
"#,
        )
        .unwrap();

        let (_, errors) = check_static(&dir);
        assert!(
            !errors.is_empty(),
            "class outside the closed enum must fail"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn orphan_manifest_is_red() {
        let dir = std::env::temp_dir().join(format!(
            "xtask_migr_rehearsal_orphan_{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("0003_ghost.manifest.toml"),
            r#"
migration_id = "0003_ghost"
class = "FORWARD_ONLY"
precheck = "select 1"
postcheck = "select 1"
rollback_or_forward_fix = "forward-fix only"
backup_restore_requirement = "restore point before apply"
"#,
        )
        .unwrap();

        let (_, errors) = check_static(&dir);
        assert!(
            !errors.is_empty(),
            "manifest with no matching .sql must fail"
        );

        let _ = fs::remove_dir_all(&dir);
    }
}
