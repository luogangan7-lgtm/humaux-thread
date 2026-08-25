//! `humaux-admin q <name>` —— §4.4 即时探针目录占位。
//!
//! 统一输出契约（§4.4）：`{value, scanned_n, scope_hash, checked_at, probe_version}`。
//! T0.9 只交付 CLI 骨架，探针后端（DB 连接 / metrics 查询）尚未接线；未接线时必须
//! **非零退出并打印缺失对象名**，禁止压成 `value = 0` 冒充「探到了 0」（§4.4：「列尚未建时
//! 探针必须以非零退出码失败并打印缺的列名，禁止压成 value = 0（假事实）」——同一纪律套用到
//! 「连接都未接线」这个更早的阶段）。因此本占位不产出任何 JSON envelope，只报失败原因。

/// §4.4 冻结的 10 条探针名闭集（新增走 PR）。`mechanism.registry --render` 不在此列
/// ——它是 `render` 子命令而非 `q`，见 main.rs 模块文档。
const KNOWN_PROBES: [&str; 10] = [
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
    eprintln!(
        "q {name}: fail — missing object: probe backend not wired yet (Phase 0 scaffold, §4.4)"
    );
    1
}
