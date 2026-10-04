//! `admin::probe` — `humaux-admin q <name>`: the §4.4 instant probe catalog (8 Readings, 3 typed refusals).
//! Depends-on: crates=[humaux-adapters, humaux-domain, serde_json, sha2, time, tokio]; services=[PostgreSQL(role_admin)
//!   x=[ops.admin_probe_snapshot]]; env=[CARGO_PKG_NAME, CARGO_PKG_VERSION, HUMAUX_ADMIN_PG_DSN,
//!   HUMAUX_BUILD_GIT_SHA, HUMAUX_BUILD_TIME]; modules=[adapters::health, adapters::postgres, admin::cell_resources,
//!   admin::ops_status, admin::tls_expiry]
//! Called-by: [admin::cell_resources, admin::main, admin::ops_status, admin::tls_expiry]
//! Invariants: [the probe is read-only: every DB probe is one call of the read-only aggregate definer
//!   ops.admin_probe_snapshot(); a Reading always has scanned_n > 0, an empty denominator is a MissingObject; a DB
//!   probe's scope_hash covers the deployed definition of that function, read with the sample]
//! Spec: Baseline §4.4; §1.14; ADR-0037; ADR-0061 D-J
//! dep-map: allow table-undeclared — the table names here are probe names, refusal text and scan-scope labels; the
//!   one SQL read is ops.admin_probe_snapshot() through adapters::health
//!
//! Unified output contract (§4.4): `{value, scanned_n, scope_hash, checked_at, probe_version}`.
//!
//! **The one discipline of this module (§4.4 坑5).** A probe either **reaches its object** and gives a reading,
//! or it **cannot** — and then it exits non-zero **naming the missing object**. It never collapses that into
//! `value = 0` (a false "there is none") or `scanned_n = 0` (which §1.14 G4 reads as "mechanism stale"). The
//! two-valued [`ProbeOutcome`] carries this in the type: `MissingObject` has no `value`, and every new Reading
//! goes through [`reading`], which turns an empty denominator into a `MissingObject` naming it.
//!
//! The 11 probes after ADR-0061 D-J:
//!
//! * Readings: `stream.watermark`, `outbox.backlog`, `jobs.stuck` (one `ops.admin_probe_snapshot()` call as
//!   `role_admin`, cross-tenant aggregates, no tenant id); `degrade.counters`, `flags.effective` (the loopback
//!   `/status` of each `HUMAUX_ADMIN_OPS_ADDRS` entry, [`crate::ops_status`]); `tls.expiry`
//!   ([`crate::tls_expiry`]); `deploy.binary` (compile-time facts, [`deploy_binary`]); `cell.resources`
//!   ([`crate::cell_resources`]).
//! * Typed refusals (§4.4 line 883 freeze, ADR-0061 E2): `public.corroborated`, `public.consensus_ready` and
//!   `parse.poison` name the column or state that does not exist in the schema ([`missing_object`]).

use humaux_adapters::health::{self, AdminProbeSample};
use humaux_adapters::postgres::AdminDbPool;
use serde_json::json;

/// §4.4 冻结的探针名闭集（新增走 PR）。`mechanism.registry --render` 不在此列
/// ——它是 `render` 子命令而非 `q`，见 main.rs 模块文档。ADR-0003 第二轮调研新增
/// `cell.resources`（§83.4 Layer 1B live probe：registry 声明与运行时事实必须来自两处，见
/// `crate::cell_resources` 模块文档）——`humaux-admin q` 的子命令集合须与 spec §4.4
/// 探针目录表逐名相等，本次改动同批同步了那张表，但相等性目前是人工维护，不是闸强制的。
// ponytail: 与 §4.4 spec 表的相等性无自动闸（`xtask::mechanism_registry` 不解析这张表，
// `INTRA_CELL_RESOURCE_REGISTRY`/`EXTERNAL_EGRESS_REGISTRY` 同款手抄副本问题是仓库既有的、
// 跨多处的系统性缺口，不是本轮改动引入的，也不是这一处能局部补齐的——补齐需要在
// xtask 里新增「解析 Baseline_2.9.md 里 `§4.4`/`intra-cell-resource-registry` 围栏并跟
// Rust 端逐名 assert set 相等」这一类通用能力，规模超出本轮 minor 发现的范围。升级路径：
// 在 architecture-check 里加一条这样的解析+比对，一次性覆盖 KNOWN_PROBES 和两个
// registry 三处手抄副本，而不是每处各修一次。
pub(crate) const KNOWN_PROBES: [&str; 11] = [
    "public.corroborated",
    "public.consensus_ready",
    "stream.watermark",
    "outbox.backlog",
    "jobs.stuck",
    "degrade.counters",
    "flags.effective",
    "deploy.binary",
    "tls.expiry",
    "parse.poison",
    "cell.resources",
];

/// The probes that reach their object (a subset of `KNOWN_PROBES`). Every other one must answer
/// [`ProbeOutcome::MissingObject`], which `every_unwired_probe_names_its_missing_object` checks name by name:
/// turn one of the three refusals into a Reading (even `value = 0`) without listing it here ⇒ that test is red.
///
/// Test-only ledger: the wiring itself is the match in [`outcome`]; drift between the two reds a test, never
/// production behaviour.
#[cfg(test)]
const WIRED_PROBES: [&str; 8] = [
    "stream.watermark",
    "outbox.backlog",
    "jobs.stuck",
    "degrade.counters",
    "flags.effective",
    "deploy.binary",
    "tls.expiry",
    "cell.resources",
];

/// `stream.watermark` scan predicate description (ADR-0061 D-J). The scope hashed is this plus the deployed definition
/// of `ops.admin_probe_snapshot()` ([`db_scope`]). Editing it without bumping `@n` and `PINNED` is red.
const STREAM_SCOPE: &str =
    "ops.admin_probe_snapshot()#stream_lagging|projection_highwater < issued_highwater|tenants=*";
/// `outbox.backlog` scan predicate: undelivered = PENDING or PROCESSING over every `ops.outbox` row (E2c).
const OUTBOX_SCOPE: &str =
    "ops.admin_probe_snapshot()#outbox_undelivered|status IN ('PENDING','PROCESSING')|tenants=*";
/// `jobs.stuck` scan predicate: an expired lease on a PROCESSING job over every `ops.jobs` row (E2c).
const JOBS_SCOPE: &str = "ops.admin_probe_snapshot()#jobs_stuck|status = 'PROCESSING' AND lease_expires_at < now()|tenants=*";

/// §4.4 scope of a DB probe: its predicate description plus the statement it ran, as `pg_get_functiondef` read it
/// with the sample — a forward migration that changes a predicate changes the hash even when nobody edits the
/// description (ADR-0061 review-fix 3, F3).
fn db_scope(description: &str, sample: &AdminProbeSample) -> String {
    format!("{description}|{}", sample.definition)
}

/// 一次探针的结果。**二值，没有第三种**：够到了对象（[`Self::Reading`]），或够不到并点名
/// （[`Self::MissingObject`]）。§4.4 坑5 的类型化落点——`MissingObject` 分支不带 `value`，
/// 所以「够不到却报 0」在本模块里连写都写不出来。
pub(crate) enum ProbeOutcome {
    Reading {
        /// 语义由每条探针自己的文档定义；§4.4 的表只冻结 `scanned_n` 的分母。
        value: i64,
        /// 实际扫过的行数/对象数。恒 `> 0`：这里的 `0` 只可能表示「没扫到」，而「没扫到」
        /// 在本模块里只有 `MissingObject` 一种表达。
        scanned_n: i64,
        /// 规范化的扫描域描述，[`scope_hash`] 的输入（§4.4：两次结果只有 `scope_hash`
        /// 相同才可比）。
        scope: String,
        /// §4.4 `probe_version` = 名 + 版本；改谓词必须升版本。
        version: &'static str,
        /// 该探针的逐对象明细，超出标量 envelope 的部分。
        detail: serde_json::Value,
    },
    /// 够不到对象——点名缺的那一个。非零退出，不产出任何 envelope。
    MissingObject(String),
}

/// `sha256:` 前缀的扫描域摘要（§4.4）。`canonical` 由调用方拼成稳定字符串。
pub(crate) fn scope_hash(canonical: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest: [u8; 32] = Sha256::digest(canonical.as_bytes()).into();
    let mut out = String::with_capacity(7 + 64);
    out.push_str("sha256:");
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// §4.4 `checked_at`（RFC3339，UTC）。
pub(crate) fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unavailable".to_string())
}

/// §4.4 `deploy.binary`：运行中二进制的 git sha / build time / crate 版本，分母恒为 1。
///
/// sha 与 build time 是**编译期**烧进来的（`HUMAUX_BUILD_GIT_SHA` / `HUMAUX_BUILD_TIME`），
/// 不是运行期读环境——运行期读会让「换个环境变量就自称是另一个版本」成立，那正是坑4
/// 想抓的「名与实对不上」本身。烧进来的东西缺席时本探针拒绝作答（`MissingObject`），
/// 因为一条印着 `"unknown"` 的 `deploy.binary` 比没有这条探针更坏：验活会全绿。
/// `build.rs` burns the sha in from `git rev-parse HEAD` when the environment did not already
/// carry one, so an ordinary `cargo build` produces a binary that CAN name its revision — before
/// that script existed this probe was documented as live and answered `missing object` in every
/// build the workspace produced.
fn deploy_binary() -> ProbeOutcome {
    deploy_binary_from(
        option_env!("HUMAUX_BUILD_GIT_SHA"),
        option_env!("HUMAUX_BUILD_TIME"),
    )
}

/// The compile-time facts as data, so BOTH arms are reachable from a test without rebuilding the
/// crate under a different environment. `deploy.binary` is in `WIRED_PROBES`, which makes the two
/// catalog-wide tests skip it entirely — this seam is what actually covers its `Reading` arm.
fn deploy_binary_from(git_sha: Option<&str>, build_time: Option<&str>) -> ProbeOutcome {
    let Some(git_sha) = git_sha.filter(|s| !s.trim().is_empty()) else {
        return ProbeOutcome::MissingObject(
            "the running binary's git sha — HUMAUX_BUILD_GIT_SHA was not set when this binary \
             was compiled and `build.rs` found no git checkout to read it from, so it cannot \
             name the revision it was built from (§4.4 坑4). Release builds must set it; see \
             docs/ops/supervision.md."
                .to_owned(),
        );
    };
    let build_time = build_time.filter(|s| !s.trim().is_empty());
    ProbeOutcome::Reading {
        // 一个运行中的二进制被完整识别 = 1，分母也是 1（§4.4 表：`deploy.binary` 的分母为 1）。
        value: 1,
        scanned_n: 1,
        scope: format!(
            "deploy.binary|crate={}|version={}",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION")
        ),
        version: "deploy.binary@1",
        detail: json!({
            "crate_name": env!("CARGO_PKG_NAME"),
            "crate_version": env!("CARGO_PKG_VERSION"),
            "git_sha": git_sha,
            "build_time": build_time,
        }),
    }
}

/// A Reading, or — when the denominator is empty — the `MissingObject` §4.4 requires: 0/0 means "not scanned",
/// never "none" (ADR-0061 D-J). Every Reading built in this crate after ADR-0061 goes through here.
pub(crate) fn reading(
    value: i64,
    scanned_n: usize,
    scope: String,
    version: &'static str,
    detail: serde_json::Value,
    denominator: &str,
) -> ProbeOutcome {
    match i64::try_from(scanned_n) {
        Ok(scanned_n) if scanned_n > 0 => ProbeOutcome::Reading {
            value,
            scanned_n,
            scope,
            version,
            detail,
        },
        _ => ProbeOutcome::MissingObject(format!("{denominator} has no rows — nothing scanned")),
    }
}

/// The three probes whose object does not exist in the schema (§4.4 line 883 freeze; ADR-0061 E2).
fn missing_object(name: &str) -> String {
    match name {
        "public.corroborated" => "public.claims.corroboration — §7.6 freezes that column as GA-建 and it does \
             not exist in the schema yet (§4.4: while the column is absent the probe must exit non-zero naming it)"
            .to_owned(),
        "public.consensus_ready" => "public.claims.contributor_set — §7.6 freezes that column as GA-建 and it \
             does not exist in the schema yet (§4.4)"
            .to_owned(),
        "parse.poison" => "private.artifacts POISON state / limit_hit column — no parse-poison state or \
             limit_hit column exists anywhere in the schema (ADR-0061 E2)"
            .to_owned(),
        other => format!("{other} is in the catalog but has no outcome arm"),
    }
}

/// One `ops.admin_probe_snapshot()` read as `role_admin` (`HUMAUX_ADMIN_PG_DSN`).
fn admin_sample() -> Result<AdminProbeSample, String> {
    let dsn = std::env::var("HUMAUX_ADMIN_PG_DSN").map_err(|_| {
        "HUMAUX_ADMIN_PG_DSN (a role_admin login; required for this probe)".to_owned()
    })?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("a tokio runtime: {e}"))?;
    runtime.block_on(async {
        // dep: PostgreSQL(role_admin) — opens the read-only role_admin pool
        let pool = AdminDbPool::connect(&dsn)
            .await
            .map_err(|e| format!("PostgreSQL as role_admin (HUMAUX_ADMIN_PG_DSN): {e}"))?;
        // dep: PostgreSQL(role_admin) — ops.admin_probe_snapshot(), the 0210 aggregate definer, via adapters::health
        health::read_admin_probe_snapshot(&pool)
            .await
            .map_err(|e| format!("ops.admin_probe_snapshot() as role_admin: {e}"))
    })
}

/// The three DB probes over one aggregate row; pure so each arm is testable without a database.
fn from_admin_sample(name: &str, s: &AdminProbeSample) -> ProbeOutcome {
    let n = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);
    let size = |v: u64| usize::try_from(v).unwrap_or(usize::MAX);
    match name {
        "stream.watermark" => {
            let families: Vec<_> = s
                .streams
                .iter()
                .map(|f| {
                    json!({
                        "family": f.family.domain(),
                        "streams": f.streams,
                        "lagging": f.lagging,
                        "lag_total": f.lag_total,
                        "lag_max": f.lag_max,
                    })
                })
                .collect();
            reading(
                n(s.streams.iter().map(|f| f.lagging).sum()),
                size(s.streams.iter().map(|f| f.streams).sum()),
                db_scope(STREAM_SCOPE, s),
                "stream.watermark@1",
                json!({
                    "lag_total": s.streams.iter().map(|f| f.lag_total).sum::<u64>(),
                    "lag_max": s.streams.iter().map(|f| f.lag_max).max(),
                    "families": families,
                }),
                "projection.stream_checkpoints",
            )
        }
        "outbox.backlog" => reading(
            n(s.outbox_undelivered),
            size(s.outbox_total),
            db_scope(OUTBOX_SCOPE, s),
            "outbox.backlog@1",
            json!({ "oldest_undelivered_age_seconds": s.outbox_oldest_undelivered_age_seconds }),
            "ops.outbox",
        ),
        "jobs.stuck" => reading(
            n(s.jobs_stuck),
            size(s.jobs_total),
            db_scope(JOBS_SCOPE, s),
            "jobs.stuck@1",
            json!({ "in_lease": s.jobs_in_lease }),
            "ops.jobs",
        ),
        other => ProbeOutcome::MissingObject(missing_object(other)),
    }
}

/// One probe's result. `cell.resources` renders its own envelope and is dispatched in [`run`].
pub(crate) fn outcome(name: &str) -> ProbeOutcome {
    match name {
        "deploy.binary" => deploy_binary(),
        "stream.watermark" | "outbox.backlog" | "jobs.stuck" => match admin_sample() {
            Ok(sample) => from_admin_sample(name, &sample),
            Err(missing) => ProbeOutcome::MissingObject(missing),
        },
        "degrade.counters" => crate::ops_status::degrade_counters(),
        "flags.effective" => crate::ops_status::flags_effective(),
        "tls.expiry" => crate::tls_expiry::run(),
        other => ProbeOutcome::MissingObject(missing_object(other)),
    }
}

/// 把一次结果落成 stdout/stderr + 退出码。Envelope 只在 [`ProbeOutcome::Reading`] 一侧存在。
fn render(name: &str, outcome: ProbeOutcome) -> i32 {
    match outcome {
        ProbeOutcome::Reading {
            value,
            scanned_n,
            scope,
            version,
            detail,
        } => {
            let envelope = json!({
                "value": value,
                "scanned_n": scanned_n,
                "scope_hash": scope_hash(&scope),
                "checked_at": now_rfc3339(),
                "probe_version": version,
                "detail": detail,
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&envelope).unwrap_or_else(|_| envelope.to_string())
            );
            0
        }
        ProbeOutcome::MissingObject(reason) => {
            eprintln!("q {name}: fail — missing object: {reason}");
            1
        }
    }
}

/// Runs one probe; returns the process exit code (0 Reading, 1 MissingObject, 2 usage).
pub fn run(name: &str, args: &[String]) -> i32 {
    // ADR-0061 D-J: the `--arg k=v` slot takes no key in this catalog version, so any argument is a usage error.
    if let Some(arg) = args.first() {
        eprintln!(
            "q {name}: usage — unknown argument {arg:?}: no probe in this catalog takes an argument"
        );
        return 2;
    }
    if !KNOWN_PROBES.contains(&name) {
        eprintln!(
            "q {name}: fail — missing object: not in §4.4 probe catalog (known: {})",
            KNOWN_PROBES.join(", ")
        );
        return 2;
    }
    // `cell.resources` 自带 live 网络往返与它自己的逐资源 envelope，见该模块文档；其余探针
    // 走本模块的纯 `outcome` 座位。
    if name == "cell.resources" {
        return crate::cell_resources::run();
    }
    render(name, outcome(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// §4.4 坑5: an empty denominator is never a Reading. Fault: let [`reading`] build a Reading at
    /// `scanned_n == 0` ⇒ red.
    #[test]
    fn no_probe_reports_a_reading_it_did_not_scan() {
        let ProbeOutcome::MissingObject(m) =
            reading(0, 0, "s".into(), "x@1", json!({}), "ops.jobs")
        else {
            panic!("0/0 is \"not scanned\", never a reading (§4.4 坑5)");
        };
        assert_eq!(m, "ops.jobs has no rows — nothing scanned");
        assert!(matches!(
            reading(0, 3, "s".into(), "x@1", json!({}), "ops.jobs"),
            ProbeOutcome::Reading { scanned_n: 3, .. }
        ));
    }

    fn sample() -> AdminProbeSample {
        use humaux_adapters::health::StreamFamilyLag;
        use humaux_domain::ticket_family::TicketFamily;
        AdminProbeSample {
            as_of: time::OffsetDateTime::UNIX_EPOCH,
            streams: vec![StreamFamilyLag {
                family: TicketFamily::PrivateMemory,
                streams: 2,
                lagging: 1,
                lag_total: 6,
                lag_max: 6,
            }],
            outbox_total: 4,
            outbox_undelivered: 2,
            outbox_oldest_undelivered_age_seconds: Some(9.0),
            jobs_total: 3,
            jobs_stuck: 1,
            jobs_in_lease: 1,
            definition: "CREATE OR REPLACE FUNCTION ops.admin_probe_snapshot() … j.lease_expires_at < now() …"
                .to_owned(),
        }
    }

    /// The three DB arms map the aggregate row onto (value, `scanned_n`) as ADR-0061 D-J's table says. Fault: use
    /// `outbox_total` as the jobs denominator ⇒ red.
    #[test]
    fn db_probes_map_value_and_denominator() {
        for (name, want) in [
            ("stream.watermark", (1, 2)),
            ("outbox.backlog", (2, 4)),
            ("jobs.stuck", (1, 3)),
        ] {
            let ProbeOutcome::Reading {
                value, scanned_n, ..
            } = from_admin_sample(name, &sample())
            else {
                panic!("{name}: a non-empty sample is a reading");
            };
            assert_eq!((value, scanned_n), want, "{name}");
        }
        let empty = AdminProbeSample {
            streams: vec![],
            jobs_total: 0,
            ..sample()
        };
        for (name, object) in [
            ("stream.watermark", "projection.stream_checkpoints"),
            ("jobs.stuck", "ops.jobs"),
        ] {
            let ProbeOutcome::MissingObject(m) = from_admin_sample(name, &empty) else {
                panic!("{name}: an empty table is not scanned");
            };
            assert!(m.starts_with(object), "{m}");
        }
    }

    /// ADR-0061 review-fix 3 (F3): the three DB probes hash the deployed statement, not only their description: a
    /// redefinition of `ops.admin_probe_snapshot()` changes each `scope_hash`, an identical one keeps it. Fault: hash
    /// the description only ⇒ red.
    #[test]
    fn db_scope_hashes_follow_the_deployed_definition() {
        let changed = AdminProbeSample {
            definition: sample()
                .definition
                .replace("j.lease_expires_at < now()", "j.lease_expires_at <= now()"),
            ..sample()
        };
        for name in ["stream.watermark", "outbox.backlog", "jobs.stuck"] {
            let hash = |s: &AdminProbeSample| match from_admin_sample(name, s) {
                ProbeOutcome::Reading { scope, .. } => scope_hash(&scope),
                ProbeOutcome::MissingObject(m) => panic!("{name}: {m}"),
            };
            assert_eq!(hash(&sample()), hash(&sample()), "{name}");
            assert_ne!(
                hash(&sample()),
                hash(&changed),
                "{name}: a redefined statement kept its scope_hash"
            );
        }
    }

    /// T-J11: `probe_version` and the scope hash of every pinned predicate description. Fault: edit a predicate
    /// constant without bumping `@n` and this pin ⇒ red.
    #[test]
    fn pinned_probe_versions_and_scope_hashes() {
        const PINNED: [(&str, &str, &str); 6] = [
            (
                "stream.watermark",
                "stream.watermark@1",
                "b902b0bf0bc5d0411ff3fb34e58e63855bb67cab2304431abadcf5789f62ab8d",
            ),
            (
                "outbox.backlog",
                "outbox.backlog@1",
                "a560d25100719588cbd05ef5159e4017d3e1fcb201c0e5a2be6d01e3ec5a0e7c",
            ),
            (
                "jobs.stuck",
                "jobs.stuck@1",
                "51616b4ecc70c4541230d8d52ef4eee6b59b019ca059c8fd69181c8738bf3d86",
            ),
            (
                "degrade.counters",
                "degrade.counters@1",
                "d750fb19123972c15abb4b177fee802a4e6fdff7ad2a438c724bcd9f784cc1c7",
            ),
            (
                "flags.effective",
                "flags.effective@1",
                "5c8009750e249f7cc1288adbe02cb8e7b84cb5d1b9df3591cfaa7161032b6d3a",
            ),
            (
                "tls.expiry",
                "tls.expiry@1",
                "17e475dd788d75001e99e964f8eabf84ad18aca1b800838e5d3558215f61f0bc",
            ),
        ];
        let live = [
            (
                STREAM_SCOPE,
                version_of(from_admin_sample("stream.watermark", &sample())),
            ),
            (
                OUTBOX_SCOPE,
                version_of(from_admin_sample("outbox.backlog", &sample())),
            ),
            (
                JOBS_SCOPE,
                version_of(from_admin_sample("jobs.stuck", &sample())),
            ),
            (crate::ops_status::DEGRADE_SCOPE, "degrade.counters@1"),
            (crate::ops_status::FLAGS_SCOPE, "flags.effective@1"),
            (crate::tls_expiry::SCOPE, "tls.expiry@1"),
        ];
        for ((name, version, hash), (scope, live_version)) in PINNED.into_iter().zip(live) {
            assert_eq!(live_version, version, "{name}");
            assert_eq!(
                scope_hash(scope),
                format!("sha256:{hash}"),
                "{name}: predicate changed without a version bump"
            );
        }
    }

    fn version_of(o: ProbeOutcome) -> &'static str {
        match o {
            ProbeOutcome::Reading { version, .. } => version,
            ProbeOutcome::MissingObject(m) => panic!("{m}"),
        }
    }

    /// T-J12: the `q` set is exactly the 11 §4.4 names, each either wired or one of the three typed refusals.
    #[test]
    fn the_catalog_is_eleven_names_eight_wired_three_refused() {
        let mut names = KNOWN_PROBES.to_vec();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), 11);
        let refused: Vec<_> = KNOWN_PROBES
            .into_iter()
            .filter(|n| !WIRED_PROBES.contains(n))
            .collect();
        assert_eq!(
            refused,
            [
                "public.corroborated",
                "public.consensus_ready",
                "parse.poison"
            ]
        );
    }

    /// The `--arg` slot takes no key yet: any argument is a usage error (exit 2), not a silently ignored flag.
    #[test]
    fn an_argument_is_a_usage_error() {
        assert_eq!(run("deploy.binary", &["--arg".into(), "k=v".into()]), 2);
    }

    /// placeholder-detection：未接线的探针**必须**点名它缺的对象，不许悄悄变成一个读数。
    ///
    /// 注错：把 `outcome` 里任意一条未接线探针改成返回 `Reading`（不同步 `WIRED_PROBES`）
    /// ⇒ 本测试红并点名那一条。
    #[test]
    fn every_unwired_probe_names_its_missing_object() {
        for name in KNOWN_PROBES {
            if WIRED_PROBES.contains(&name) {
                continue;
            }
            let ProbeOutcome::MissingObject(reason) = outcome(name) else {
                panic!(
                    "{name} 未在 WIRED_PROBES 中却给出了读数——够不到的对象不许压成读数（§4.4 坑5）"
                );
            };
            assert!(
                !reason.trim().is_empty() && !reason.contains("has no outcome arm"),
                "{name}: 缺失对象必须被点名，不能是笼统理由：{reason}"
            );
        }
    }

    /// 目录闭集与已接线子集不许漂移：`WIRED_PROBES` 里出现一个不在 `KNOWN_PROBES` 的名字
    /// ⇒ 红（否则上一条测试会静默跳过一条根本不存在的探针）。
    #[test]
    fn wired_probes_are_a_subset_of_the_frozen_catalog() {
        for name in WIRED_PROBES {
            assert!(KNOWN_PROBES.contains(&name), "{name} 不在 §4.4 冻结目录里");
        }
    }

    /// 目录外的名字是「不在目录里」这一种失败，不是读数。
    #[test]
    fn a_name_outside_the_catalog_never_renders_an_envelope() {
        assert_ne!(run("not.a.probe", &[]), 0);
    }

    /// `deploy.binary` 的 `Reading` 臂——`WIRED_PROBES` 让两条目录级测试跳过它，此前它一行
    /// 覆盖都没有，而 `HUMAUX_BUILD_GIT_SHA` 从来没有任何构建设过，所以它在每次 gate 里都走
    /// `MissingObject`。两个臂都在这里定死：有 sha ⇒ 冻结 envelope（`value=1, scanned_n=1`
    /// 加 §4.4 的 `probe_version`）；没有 ⇒ 点名缺的正是那个 sha，且**不带 value**。
    ///
    /// 注错：把缺 sha 的分支改成 `Reading { value: 0, .. }`（坑5 的形状）⇒ 第二段红。
    #[test]
    fn deploy_binary_reads_the_burned_in_sha_and_refuses_without_one() {
        let ProbeOutcome::Reading {
            value,
            scanned_n,
            scope,
            version,
            detail,
        } = deploy_binary_from(Some("0123456789abcdef"), Some("2026-09-09T00:00:00Z"))
        else {
            panic!("a burned-in sha must produce a reading, not a missing object");
        };
        assert_eq!(
            (value, scanned_n),
            (1, 1),
            "§4.4: deploy.binary 的分母恒为 1"
        );
        assert_eq!(version, "deploy.binary@1");
        assert_eq!(detail["git_sha"], "0123456789abcdef");
        assert_eq!(detail["build_time"], "2026-09-09T00:00:00Z");
        assert_eq!(detail["crate_name"], env!("CARGO_PKG_NAME"));
        assert!(scope_hash(&scope).starts_with("sha256:"));

        for absent in [None, Some(""), Some("   ")] {
            let ProbeOutcome::MissingObject(reason) = deploy_binary_from(absent, None) else {
                panic!("{absent:?} is not a revision — the probe must refuse, not invent one");
            };
            assert!(reason.contains("git sha"), "缺失对象必须被点名：{reason}");
        }
    }

    /// `build.rs` 与探针的接线是否真的通了——**这条测试就是「文档说 live、实际每次都 missing」
    /// 那个缺口的闸**：编译期若拿到了 sha，`outcome("deploy.binary")` 必须是读数。
    ///
    /// 注错：删掉 `bins/admin/build.rs`（且不在环境里设 `HUMAUX_BUILD_GIT_SHA`）⇒ 在 git
    /// 检出里跑这条测试立刻红。非 git 检出（vendored tarball）里它自动降级为「拒绝作答」的
    /// 断言，因为那时候拒绝作答才是对的。
    #[test]
    fn this_build_burned_in_a_sha_when_it_had_a_checkout_to_read() {
        match (
            option_env!("HUMAUX_BUILD_GIT_SHA"),
            outcome("deploy.binary"),
        ) {
            (Some(sha), ProbeOutcome::Reading { detail, .. }) if !sha.trim().is_empty() => {
                assert_eq!(detail["git_sha"], sha);
            }
            (Some(sha), _) if !sha.trim().is_empty() => {
                panic!("the build burned in {sha} but the probe refused to report it")
            }
            (_, ProbeOutcome::MissingObject(_)) => {
                // No checkout at build time; refusing to answer is the correct outcome.
            }
            (_, ProbeOutcome::Reading { .. }) => {
                panic!("no sha was burned in, yet the probe produced a reading (§4.4 坑4)")
            }
        }
    }

    /// `scope_hash` 是同输入同输出、异输入异输出的规范摘要（§4.4 可比性前提）。
    #[test]
    fn scope_hash_is_stable_and_discriminating() {
        assert_eq!(scope_hash("a|b"), scope_hash("a|b"));
        assert_ne!(scope_hash("a|b"), scope_hash("a|c"));
        assert!(scope_hash("a|b").starts_with("sha256:"));
        assert_eq!(scope_hash("a|b").len(), "sha256:".len() + 64);
    }
}
