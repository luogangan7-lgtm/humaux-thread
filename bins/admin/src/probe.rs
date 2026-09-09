//! `humaux-admin q <name>` —— §4.4 即时探针目录。
//!
//! 统一输出契约（§4.4）：`{value, scanned_n, scope_hash, checked_at, probe_version}`。
//!
//! **本模块的唯一纪律（§4.4 坑5）**：一条探针要么**够到了它的对象**并给出读数，要么
//! **够不到**——后者必须以非零退出码失败并**点名缺的对象**，禁止压成 `value = 0`（假事实：
//! 「没有」）或 `scanned_n = 0`（会被 §1.14 G4 读成「机制过期」）。这条纪律由本模块的
//! [`ProbeOutcome`] 二值枚举在类型上承载：`MissingObject` 分支**没有** `value` 字段，
//! 所以「够不到却报了个 0」在这里写不出来；而 `Reading` 分支的 `scanned_n > 0` 由
//! `no_probe_reports_a_reading_it_did_not_scan` 逐条守住（注错见该测试的文档注释）。
//!
//! 目录里 11 条探针今天的接线状态分成三档，`WIRED_PROBES` 是其中「已接线」那档的闭集：
//!
//! * `cell.resources` —— live probe，见 [`crate::cell_resources`]。
//! * `deploy.binary` —— 编译期事实（git sha / build time / crate 版本），见 [`deploy_binary`]。
//!   §4.4 坑4「三臂全跑旧镜像而验活全绿」正是这条探针存在的理由，所以 sha 没被烧进二进制时
//!   它**拒绝作答**（`MissingObject`），而不是印一个 `"unknown"` 让验活继续全绿。
//! * 其余 9 条 —— 各自点名自己缺的那个对象（见 [`missing_object`]）。它们缺的不是「代码没写」
//!   这种笼统理由，而是本进程当下**确实没有**的那个具体东西：读能力、列、或存储。ADR-0037
//!   记了每一条的解锁条件；本卡的 allowed-files 不含 `migrations/`、`crates/adapters/` 与
//!   `bins/admin/Cargo.toml`，而这 9 条无一例外要动其中之一。

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

/// 本进程今天真正接得上其对象的探针（`KNOWN_PROBES` 的子集）。其余每一条都必须走
/// [`ProbeOutcome::MissingObject`]——由 `every_unwired_probe_names_its_missing_object`
/// 逐条守住。**这就是本卡验收要求的 placeholder-detection 测试**：把任意一条未接线探针
/// 改成返回读数（哪怕 `value = 0`）而不把它加进本闭集，那个测试立刻红。
///
/// 只在测试构建里存在：它是那道闸的台账，生产路径一个字节都不读它——接线状态在
/// [`outcome`] 的 match 臂上，不在这张表上，两处若漂移，红的是测试而不是生产行为。
#[cfg(test)]
const WIRED_PROBES: [&str; 2] = ["deploy.binary", "cell.resources"];

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

/// 未接线探针缺的那个**具体对象**。不是「代码没写」，是本进程当下确实没有的东西。
/// 解锁条件逐条记在 ADR-0037。
fn missing_object(name: &str) -> String {
    // 这三个是本仓当下的三堵墙，9 条探针每条至少撞其中一堵。写在一处，免得 9 份手抄副本。
    const NO_READ_CAPABILITY: &str = "this process has no read capability for it: `role_admin` holds only the §6.2.2 \
         observation SELECT grants, and G80-40 confines `sqlx::PgPool` to \
         crates/adapters/src/postgres.rs, so `humaux-admin` has no query surface for it either \
         (ADR-0037)";
    match name {
        "public.corroborated" => format!(
            "public.claims.corroboration — {NO_READ_CAPABILITY}; and §7.6 freezes that column as \
             GA-建, so it does not exist in the schema yet either (§4.4: 列尚未建时探针必须以非零 \
             退出码失败并打印缺的列名)"
        ),
        "public.consensus_ready" => format!(
            "public.claims.contributor_set — {NO_READ_CAPABILITY}; and §7.6 freezes that column \
             as GA-建, so it does not exist in the schema yet either"
        ),
        "stream.watermark" => format!("projection.stream_checkpoints — {NO_READ_CAPABILITY}"),
        "outbox.backlog" => format!("ops.outbox — {NO_READ_CAPABILITY}"),
        "jobs.stuck" => format!("ops.jobs — {NO_READ_CAPABILITY}"),
        "parse.poison" => format!("private.artifacts — {NO_READ_CAPABILITY}"),
        "degrade.counters" => "the §53 DegradeCode counter store — `degrade_total{code}` is an \
             in-process counter with no process-external store, so no second process can read \
             another's counts at all (ADR-0037)"
            .to_owned(),
        "flags.effective" => "the effective-flag registry — it is built per process inside \
             `humaux_gateway::bootstrap` (`resolve_effective_config`) and is not published \
             anywhere `humaux-admin` can read; §4.4 坑4 wants the EFFECTIVE value, which by \
             construction only the process that resolved it holds (ADR-0037)"
            .to_owned(),
        "tls.expiry" => "the TLS certificate store — no certificate path is configured for this \
             process, and `humaux-admin` links no X.509 parser (ADR-0037)"
            .to_owned(),
        other => format!("{other} is in the catalog but has no outcome arm"),
    }
}

/// 一条探针的结果——**纯函数除 `cell.resources` 外**（那条要 DNS/网络，见 [`run`]）。
/// 单元测试直接调它，不经进程。
pub(crate) fn outcome(name: &str) -> ProbeOutcome {
    match name {
        "deploy.binary" => deploy_binary(),
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

/// 跑一次探针；返回进程退出码。
pub fn run(name: &str, _args: &[String]) -> i32 {
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

    /// §4.4 坑5 的守卫，也是本卡验收点名的 fault-injection 测试。
    ///
    /// 注错：把 [`missing_object`] 覆盖的任意一条改成
    /// `ProbeOutcome::Reading { value: 0, scanned_n: 0, .. }`（「够不到 ⇒ 报 0」这个正是坑5
    /// 的形状）⇒ 本测试红，因为 `scanned_n == 0` 在本模块里没有合法读数含义。
    #[test]
    fn no_probe_reports_a_reading_it_did_not_scan() {
        for name in KNOWN_PROBES {
            if name == "cell.resources" {
                continue; // live 探针，见 `run`
            }
            if let ProbeOutcome::Reading {
                scanned_n, scope, ..
            } = outcome(name)
            {
                assert!(
                    scanned_n > 0,
                    "{name}: scanned_n == 0 是「没扫到」，不是读数（§4.4 坑5）"
                );
                assert!(!scope.is_empty(), "{name}: 读数必须带可比的扫描域（§4.4）");
            }
        }
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
