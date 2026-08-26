//! `humaux-admin q <name>` —— §4.4 即时探针目录占位。
//!
//! 统一输出契约（§4.4）：`{value, scanned_n, scope_hash, checked_at, probe_version}`。
//! T0.9 只交付 CLI 骨架，探针后端（DB 连接 / metrics 查询）尚未接线；未接线时必须
//! **非零退出并打印缺失对象名**，禁止压成 `value = 0` 冒充「探到了 0」（§4.4：「列尚未建时
//! 探针必须以非零退出码失败并打印缺的列名，禁止压成 value = 0（假事实）」——同一纪律套用到
//! 「连接都未接线」这个更早的阶段）。因此本占位不产出任何 JSON envelope，只报失败原因。

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
const KNOWN_PROBES: [&str; 11] = [
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

/// 跑一次探针；返回进程退出码。
pub fn run(name: &str, _args: &[String]) -> i32 {
    if !KNOWN_PROBES.contains(&name) {
        eprintln!(
            "q {name}: fail — missing object: not in §4.4 probe catalog (known: {})",
            KNOWN_PROBES.join(", ")
        );
        return 2;
    }
    // `cell.resources` is the one probe wired past the Phase 0 scaffold — see
    // `crate::cell_resources`'s module doc.
    if name == "cell.resources" {
        return crate::cell_resources::run();
    }
    eprintln!(
        "q {name}: fail — missing object: probe backend not wired yet (Phase 0 scaffold, §4.4)"
    );
    1
}
