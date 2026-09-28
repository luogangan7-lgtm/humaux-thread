//! `xtask::migration_rehearsal` — G46-1 / G80-37 migration rehearsal contract: static manifest checks.
//! Depends-on: crates=[humaux-testkit, postgres, toml]; services=[PostgreSQL(owner)]; env=[CARGO_MANIFEST_DIR, HUMAUX_TEST_PG_DSN]; modules=[]
//! Called-by: [tests, xtask::main, xtask::migrate]
//! Invariants: [missing manifest, orphan manifest, or an illegal required field ⇒ fail, each error names its file (§57.1 rule 2)]
//! Spec: Baseline §46.1
//!
//! xtask `migration-rehearsal` — G46-1 / G80-37 (§46.1 Migration Rehearsal Contract).
//!
//! Static side (this module, always runs): enumerate `migrations/*.sql`, assert each
//! has exactly one same-stem `<stem>.manifest.toml`, and validate the manifest's
//! required fields (`migration_id`/`class`/`precheck`/`postcheck`/
//! `rollback_or_forward_fix`/`backup_restore_requirement`). Missing manifest, orphan
//! manifest, or an illegal field ⇒ `fail`, each error names its file (§57.1 rule 2).
//!
//! Check-SQL side (ADR-0050 D-E, runs when `HUMAUX_TEST_PG_DSN`/`DATABASE_URL` is set): one
//! `BEGIN READ ONLY` transaction; every manifest's `precheck`/`postcheck` is `EXPLAIN`ed
//! (syntax + semantic analysis, no execution) and `prepare`d (result shape: exactly one `bool`
//! column), then ROLLBACK. Any error or shape mismatch is a `fail` naming
//! `<manifest> <field>: <message>`. A DSN that is set but unreachable is a `fail`. This proves
//! syntax and shape against the HEAD schema only; `migrate`'s throwaway 0001→head test proves
//! each precheck true in its pre-state.
//!
//! Execution side (spin up Postgres, run `up -> down -> up`, diff
//! `schema_digest_before/after`) is still not built — per §57.1 rule 3 it reports
//! `not_applicable` naming the missing object, never a silent skip.
//!
//! depends-on: `migrations/*.sql` + `*.manifest.toml`; optionally Postgres (read-only).
//! called-by: `cargo xtask migration-rehearsal` (chain extra gate `migration_rehearsal`);
//! `migrate::collect_migrations` reuses [`parse_manifest`].

use postgres::types::Type;
use postgres::{Client, NoTls};
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
///
/// `precheck`/`postcheck` are executed by `xtask migrate` (ADR-0050 D-D) and syntax/shape
/// checked by [`check_sql`] here (D-E).
#[derive(Debug, Clone)]
pub struct Manifest {
    pub migration_id: String,
    pub class: MigrationClass,
    pub precheck: String,
    pub postcheck: String,
    // Validated-present only: read by the rehearsal *execution* runner (DOD-089, not built).
    #[allow(dead_code)]
    pub rollback_or_forward_fix: String,
    #[allow(dead_code)]
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

    for key in ["precheck", "postcheck"] {
        if let Some(at) = statement_separator(&fields[key]) {
            return Err(err(format!(
                "`{key}` has a `;` statement separator at byte {at}: a check is exactly one \
                 SQL statement (ADR-0051 D-L)"
            )));
        }
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

/// Byte offset of the first `;` in `sql` that is outside `--` / nested `/* */` comments,
/// `'…'` literals (`''` escape), `"…"` identifiers and `$tag$…$tag$` bodies — i.e. a real
/// statement separator. Checks are single statements by contract; a second statement would
/// otherwise ride along with `EXPLAIN` over the simple-query protocol.
fn statement_separator(sql: &str) -> Option<usize> {
    let b = sql.as_bytes();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b';' => return Some(i),
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                let mut depth = 0usize;
                while i < b.len() {
                    if b[i..].starts_with(b"/*") {
                        depth += 1;
                        i += 2;
                    } else if b[i..].starts_with(b"*/") {
                        depth -= 1;
                        i += 2;
                        if depth == 0 {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
                continue;
            }
            q @ (b'\'' | b'"') => {
                i += 1;
                while i < b.len() {
                    if b[i] == q {
                        if b.get(i + 1) == Some(&q) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'$' => {
                let end = b[i + 1..]
                    .iter()
                    .position(|c| !(c.is_ascii_alphanumeric() || *c == b'_'))
                    .map(|n| i + 1 + n)
                    .filter(|e| b[*e] == b'$');
                if let Some(end) = end {
                    let tag = &b[i..=end];
                    let body = end + 1;
                    i = b[body..]
                        .windows(tag.len())
                        .position(|w| w == tag)
                        .map_or(b.len(), |n| body + n + tag.len());
                    continue;
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
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
    let dsn = database_dsn();
    let code = match &dsn {
        None => {
            eprintln!(
                "migration-rehearsal: check-sql not_applicable (missing object: \
                 HUMAUX_TEST_PG_DSN / DATABASE_URL)"
            );
            0
        }
        // dep: PostgreSQL(any) — read-only EXPLAIN/prepare of every manifest check (ADR-0050 D-E).
        Some(dsn) => match Client::connect(dsn, NoTls) {
            Err(e) => {
                eprintln!(
                    "migration-rehearsal: check-sql fail (a DSN is set but the database is \
                     unreachable: {e})"
                );
                1
            }
            Ok(mut client) => match check_sql(&mut client, &manifests) {
                Ok((checked, invalid)) if invalid.is_empty() => {
                    eprintln!("migration-rehearsal: check-sql pass ({checked} checks, 0 invalid)");
                    0
                }
                Ok((checked, invalid)) => {
                    eprintln!(
                        "migration-rehearsal: check-sql fail ({checked} checks, {} invalid)",
                        invalid.len()
                    );
                    for line in &invalid {
                        eprintln!("  {line}");
                    }
                    1
                }
                Err(e) => {
                    eprintln!("migration-rehearsal: check-sql fail ({e})");
                    1
                }
            },
        },
    };
    eprintln!("{}", rehearsal_execution_verdict(dsn.is_some()));
    code
}

/// ADR-0050 D-E: `EXPLAIN` + `prepare` every manifest check inside one read-only
/// transaction that is always rolled back. Each check runs under its own savepoint so one
/// invalid check does not abort the rest. Returns `(checks run, invalid lines)` where each
/// line is `<migration_id>.manifest.toml <field>: <message>`; `Err` only for a transaction
/// that cannot be opened at all.
fn check_sql(client: &mut Client, manifests: &[Manifest]) -> Result<(usize, Vec<String>), String> {
    // dep: PostgreSQL(owner) — READ ONLY transaction, rolled back at the end.
    let mut tx = client
        .build_transaction()
        .read_only(true)
        .start()
        .map_err(|e| format!("cannot open a read-only transaction: {e}"))?;
    let mut checked = 0usize;
    let mut invalid = Vec::new();
    for m in manifests {
        for (field, sql) in [("precheck", &m.precheck), ("postcheck", &m.postcheck)] {
            checked += 1;
            let verdict = (|| -> Result<(), String> {
                let mut sp = tx.transaction().map_err(|e| e.to_string())?;
                let text = |e: postgres::Error| {
                    e.as_db_error()
                        .map_or_else(|| e.to_string(), |db| db.message().to_string())
                };
                // Extended protocol: the server rejects a second command outright instead of
                // executing it after the EXPLAIN (ADR-0051 D-L; parse_manifest refuses `;` early).
                sp.query(&format!("EXPLAIN {sql}"), &[]).map_err(text)?;
                let stmt = sp.prepare(sql).map_err(text)?;
                match stmt.columns() {
                    [col] if *col.type_() == Type::BOOL => Ok(()),
                    [col] => Err(format!("returns {}, expected one bool column", col.type_())),
                    cols => Err(format!(
                        "returns {} columns, expected one bool column",
                        cols.len()
                    )),
                }
                // `sp` drops here → ROLLBACK TO SAVEPOINT, so an error never poisons `tx`.
            })();
            if let Err(msg) = verdict {
                invalid.push(format!("{}.manifest.toml {field}: {msg}", m.migration_id));
            }
        }
    }
    tx.rollback().map_err(|e| format!("rollback: {e}"))?;
    Ok((checked, invalid))
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

    fn manifest_with_precheck(precheck: &str) -> String {
        format!(
            "migration_id = \"0001_x\"\nclass = \"REVERSIBLE\"\nprecheck = '''{precheck}'''\n\
             postcheck = \"select true\"\nrollback_or_forward_fix = \"n/a\"\n\
             backup_restore_requirement = \"n/a\"\n"
        )
    }

    #[test]
    fn manifest_check_with_semicolon_outside_comments_is_refused() {
        let e = parse_manifest(
            "0001_x.manifest.toml",
            &manifest_with_precheck("select true; drop table x"),
        )
        .expect_err("two statements must be refused");
        assert!(
            e.reason.contains("`precheck`") && e.reason.contains(';'),
            "{e}"
        );
        assert_eq!(statement_separator("select 1;"), Some(8));
    }

    #[test]
    fn manifest_check_semicolon_inside_comment_or_literal_is_allowed() {
        for sql in [
            "select 1 -- a; b\n",
            "select 1 /* a; /* nested; */ b; */",
            "select 'it''s; fine' = 'x'",
            "select \"a;b\" from t",
            "select $f$ a; b $f$ = $$c;$$",
        ] {
            assert_eq!(statement_separator(sql), None, "{sql}");
            parse_manifest("0001_x.manifest.toml", &manifest_with_precheck(sql)).expect(sql);
        }
        // The real manifests whose `;` sits in `--` comments (0155 0161 0164 0165 0167 0170,
        // seven checks) still parse.
        let real = Path::new(env!("CARGO_MANIFEST_DIR")).join("../migrations");
        let (manifests, errors) = check_static(&real);
        assert!(errors.is_empty(), "{errors:?}");
        let with_semicolon = manifests
            .iter()
            .flat_map(|m| [&m.precheck, &m.postcheck])
            .filter(|sql| sql.contains(';'))
            .count();
        assert_eq!(with_semicolon, 7);
    }

    /// The EXPLAIN runs over the extended protocol: a two-statement check errors instead of
    /// executing its second statement.
    #[test]
    fn explain_uses_extended_protocol_multi_statement_rejected() {
        const TEST: &str = "explain_uses_extended_protocol_multi_statement_rejected";
        let Some(dsn) = database_dsn() else {
            humaux_testkit::skip_or_fail(
                TEST,
                "missing object: HUMAUX_TEST_PG_DSN",
                humaux_testkit::ExternalDep::Postgres,
            );
            return;
        };
        // dep: PostgreSQL(owner) — read-only EXPLAIN of a two-statement check.
        let mut client = Client::connect(&dsn, NoTls).expect("connect (DSN is set)");
        let m = Manifest {
            migration_id: "0001_two".to_string(),
            class: MigrationClass::ForwardOnly,
            precheck: "select true; select 1/0 = 1".to_string(),
            postcheck: "select true".to_string(),
            rollback_or_forward_fix: String::new(),
            backup_restore_requirement: String::new(),
        };
        let (checked, invalid) = check_sql(&mut client, &[m]).expect("read-only txn");
        assert_eq!(checked, 2);
        assert_eq!(invalid.len(), 1, "{invalid:?}");
        assert!(
            invalid[0].starts_with("0001_two.manifest.toml precheck:")
                && !invalid[0].contains("division by zero"),
            "second statement must not execute: {invalid:?}"
        );
    }

    /// ADR-0050 D-E: a valid check passes; an invalid-SQL check and a non-boolean check are
    /// each listed by manifest and field. Read-only against `HUMAUX_TEST_PG_DSN`.
    #[test]
    fn check_sql_pass_flags_invalid_and_non_boolean_manifests() {
        const TEST: &str = "check_sql_pass_flags_invalid_and_non_boolean_manifests";
        let Some(dsn) = database_dsn() else {
            humaux_testkit::skip_or_fail(
                TEST,
                "missing object: HUMAUX_TEST_PG_DSN",
                humaux_testkit::ExternalDep::Postgres,
            );
            return;
        };
        // dep: PostgreSQL(any) — HUMAUX_TEST_PG_DSN — read-only EXPLAIN of scratch manifest checks.
        let mut client = Client::connect(&dsn, NoTls).expect("connect (DSN is set)");
        let manifest = |id: &str, pre: &str, post: &str| Manifest {
            migration_id: id.to_string(),
            class: MigrationClass::ForwardOnly,
            precheck: pre.to_string(),
            postcheck: post.to_string(),
            rollback_or_forward_fix: String::new(),
            backup_restore_requirement: String::new(),
        };
        let manifests = [
            manifest(
                "0001_ok",
                "select true",
                "select to_regclass('pg_class') IS NOT NULL",
            ),
            manifest("0002_bad", "selec true", "select true"),
            manifest("0003_int", "select true", "select 1"),
        ];
        let (checked, invalid) = check_sql(&mut client, &manifests).expect("read-only txn");
        assert_eq!(checked, 6);
        assert_eq!(invalid.len(), 2, "{invalid:?}");
        assert!(
            invalid[0].starts_with("0002_bad.manifest.toml precheck:")
                && invalid[0].contains("syntax error"),
            "{invalid:?}"
        );
        assert!(
            invalid[1].starts_with("0003_int.manifest.toml postcheck:")
                && invalid[1].contains("int4"),
            "{invalid:?}"
        );
    }
}
