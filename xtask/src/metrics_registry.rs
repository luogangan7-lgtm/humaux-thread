//! `xtask::metrics_registry` — G80-6 metrics-registry-check: Registry/Code/Witness three-way falsification, plus the exported (D7) and rule (D8) legs.
//! Depends-on: crates=[]; services=[subprocess(cargo)]; env=[CARGO_MANIFEST_DIR]; modules=[]
//! Called-by: [xtask::main]
//! Invariants: [each Witness file is really compiled and run in a throwaway probe package; missing/failing/timed-out witnesses count as 0 passed, never substituted by comment; an unknown argument exits 2 naming it; "exported" is read from each process's real `--metrics-families` output, never from a hand list; a rule-referenced family that no process exports fails unless NOT_YET_PRODUCED names its producer card]
//! Spec: Baseline §80.2; §41.2; §42; ADR-0061 D-H
//!
//! Modes (ADR-0061 D-H; the strict parser replaces the old `any(--strict)` scan that let
//! `--check` run the default mode silently, E6): no mode flag = D1–D6; `--check` = D1–D8;
//! `--exposition <process>=<file>` (repeatable) = the D7 parser over live scrapes (EX);
//! `--strict` also counts not_applicable. D7 runs `cargo run -p humaux-<process> -- --metrics-families`
//! for the six process keys in [`PROCESSES`] (`maintenance-serve` adds `--serve`, ADR-0062 D-S); D8 scans `deploy/prometheus/*.rules.yml`.
//!
//! xtask `metrics-registry` — G80-6 `metrics-registry-check`：Registry(R) / Code(C) /
//! Witness(W) 三方证伪（§80.2 全文）。R 解析 §41.2 注册表；C 静态扫 `crates/**/src/**/*.rs`
//! 的发射点；W 枚举 `crates/testkit/tests/metrics/<family>.rs`（文件名从 family 确定性派生，
//! §80.2「不建第二张表」冻结）**并真实执行它**：每个 witness 文件被复制进一个一次性 probe
//! package（path 依赖 `crates_root` 下除 `testkit` 外的每个生产 crate），`cargo test` 真编译
//! 真运行，`test result: … passed …` 是唯一证据来源（见 [`run_witness_probe`]）——不存在、
//! 编译失败、超时、断言失败都诚实地折算成 0 passed，不会被一行手写注释顶替。D1–D6 逐条实现
//! 并逐条打印（§80.2）。
//!
//! **生效期**：G80-6 从 Phase 14 起必过（§57.1 表 Phase 14 行）。默认模式下 D 检查照算照打印，
//! 但一个 family 若在代码里和 witness 目录里都毫无踪迹（`c_count==0 && !witness_exists`），
//! 该 check 的三态状态直接落为 `NotApplicable` 并打印缺失对象名（§57.1 第2条），不计入退出码；
//! `--strict` 旗标关闭这条豁免，把 `NotApplicable` 也计入退出码，按真三方判定红绿（Phase 14
//! 起 CI 切 `--strict`，见任务卡）。这条豁免只看"这个 family 有没有任何交付痕迹"，不看它是不是
//! 本轮其他组的产物——已经落地一半（有 C 无 W，如当前的 `degrade_total`）不豁免，那是没写完，
//! 不是没轮到（repo CLAUDE.md「拒绝伪修复」）。R 侧解析失败或 §41.2 表不存在 ⇒ 直接 fail，
//! 不是 `not_applicable`（§80.2 D6 正哨兵语义：`|R|>0` 必须成立，这是前提不是判定项）。

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

const SPEC_PATH: &str = "docs/architecture/Baseline_2.9.md";
const CRATES_DIR: &str = "crates";
const REGISTRY_HEADING: &str = "## 41.2 注册表（全集）";
const WITNESS_SUBDIR: &str = "testkit/tests/metrics";
const RULES_DIR: &str = "deploy/prometheus";
const RULES_SUFFIX: &str = ".rules.yml";
const USAGE: &str =
    "usage: cargo xtask metrics-registry [--check] [--strict] [--exposition <process>=<file>]...";

/// The processes that serve `/metrics` (ADR-0061 D-B). The cargo package and bin of each is
/// `humaux-<process>`; the key is also the `--exposition` process name. `maintenance-serve` is
/// `humaux-maintenance --serve` (ADR-0062 D-S): a second resident mode of that binary whose families
/// differ from `health serve`'s, read with `--serve --metrics-families`.
const PROCESSES: [&str; 6] = [
    "gateway",
    "retrieval-worker",
    "private-worker",
    "consolidation-worker",
    "maintenance",
    "maintenance-serve",
];

/// ADR-0061 D-H D8(c): rule-referenced families no process exports yet, each with the card
/// that adds its producer. An entry whose family IS exported fails as a stale entry, so the
/// producer card has to delete its row (E10 closed: card 34b's private worker exports the two
/// distill counters).
const NOT_YET_PRODUCED: &[(&str, &str)] = &[("backup_last_success_timestamp_seconds", "card 37")];

/// §41.2 R4 (ADR-0061 addendum, D-M): families whose one `.inc(` sits in a `pub` helper so a DB-free
/// witness can drive it. The helper's production call sites are the real emit sites, so D5 also
/// requires exactly one per helper — deleting or duplicating the call is red here.
const EMIT_HELPERS: &[(&str, &str)] = &[
    ("private_distill_runs_total", "count_committed_distill"),
    ("private_distill_outputs_total", "count_committed_distill"),
    (
        "private_reasoning_usage_total",
        "count_private_reasoning_usage",
    ),
];

/// §42 ①: third-party exporter families a rule may name although §41.2 does not register them.
const THIRD_PARTY_PREFIXES: &[&str] = &["node_filesystem_"];

/// PromQL words that are neither a family nor followed by `(` as a function call is.
const PROMQL_KEYWORDS: &[&str] = &[
    "by",
    "without",
    "ignoring",
    "on",
    "group_left",
    "group_right",
    "and",
    "or",
    "unless",
    "offset",
    "bool",
    "atan2",
    "inf",
    "nan",
];

/// Aggregation operators: `sum by (code) (…)` puts a clause between the name and its `(`.
const PROMQL_AGGREGATIONS: &[&str] = &[
    "sum",
    "min",
    "max",
    "avg",
    "group",
    "stddev",
    "stdvar",
    "count",
    "count_values",
    "bottomk",
    "topk",
    "quantile",
    "limitk",
    "limit_ratio",
];

/// 一行 §41.2 注册表拆出的单个 family（同一单元格 `/` 分隔的多个 family 各产生一条，
/// §80.2「parser 拆成多个 family，各自产生一条派生路径」）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryEntry {
    pub family: String,
    pub labels: BTreeSet<String>,
    pub declared_emit_count: u32,
    /// §41.2「量纲」列的第一段（`counter`/`gauge`/`histogram`）。当前 D-checks 不消费它——
    /// 真正需要它的是 D4 备注的 histogram bucket 后缀剥离，而本闸尚无真实 scrape 数据源
    /// （见模块文档）；解析它只是让 R 的产出形状与任务卡①一致，供未来接线。
    pub metric_kind: String,
}

/// 单个 family 在 witness 侧的真实执行证据。`labels` 来自文件里一行
/// `// witness: family=<f> labels=<a,b,c|->` 声明（纯身份/预期 label 元数据，从不携带
/// 计数——见 [`parse_witness_meta`]）；`tests_passed`/`tests_total` 来自 [`run_witness_probe`]
/// 真编译真运行该文件后解析 cargo 自己打印的 `test result: … passed …`，不存在/编译失败/
/// 超时/断言失败一律诚实落到 `(0, 0)`，谁都改不了这两个数字，除非真的让断言通过。
#[derive(Debug, Clone, Default)]
struct WitnessInfo {
    labels: Option<BTreeSet<String>>,
    tests_passed: u64,
    tests_total: u64,
}

/// 闸的三态判定（§57.1：所有闸三态 pass/fail/not_applicable；`not_applicable` 必须打印
/// 缺失对象名）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateStatus {
    Pass,
    Fail,
    NotApplicable,
}

/// 一条 D1–D6 判定，三态直接落在 `status` 上（§57.1）：违反集为空 -> `Pass`；违反集里
/// 至少一个 family 已有交付痕迹 -> `Fail`；违反集非空但每个 family 都毫无交付痕迹
/// （`is_unimplemented`）-> `NotApplicable`，`na_families` 记下具体名字供打印
/// （§57.1「必须打印缺失对象名」）。`--strict` 是否把 `NotApplicable` 也计入退出码由
/// `report()` 决定，不改变这里算出来的原始三态。
#[derive(Debug, Clone)]
pub struct DCheck {
    pub id: &'static str,
    pub status: GateStatus,
    pub detail: String,
    na_families: BTreeSet<String>,
}

/// 违反集按「是否毫无交付痕迹」拆分后，直接给出这条 check 该落的三态（见 `DCheck` 文档）。
fn status_for<'a>(
    violating: impl Iterator<Item = &'a str>,
    code_counts: &BTreeMap<String, usize>,
    witness_exists: &BTreeSet<String>,
) -> (GateStatus, BTreeSet<String>) {
    let mut real = false;
    let mut na = BTreeSet::new();
    let mut any = false;
    for f in violating {
        any = true;
        if is_unimplemented(f, code_counts, witness_exists) {
            na.insert(f.to_string());
        } else {
            real = true;
        }
    }
    let status = if !any {
        GateStatus::Pass
    } else if real {
        GateStatus::Fail
    } else {
        GateStatus::NotApplicable
    };
    (status, na)
}

// ---------------------------------------------------------------------------
// R: §41.2 registry parsing
// ---------------------------------------------------------------------------

/// 截出 `## 41.2` 到下一个 `## ` 标题之间的正文（不含两侧标题行）。§41.2 是唯一数据源，
/// 解析目标是 canonical md 本身，不引入第二个数据文件。
fn extract_registry_section(spec: &str) -> Option<&str> {
    let start = spec.find(REGISTRY_HEADING)? + REGISTRY_HEADING.len();
    let rest = &spec[start..];
    let end = rest.find("\n## ").unwrap_or(rest.len());
    Some(&rest[..end])
}

/// 表格数据行的判据：markdown 表格行去掉首尾 `|` 后按 `|` 拆列。表头/分隔行/正文段落
/// 均不以 `` | ` `` 开头（§41.2 每条指标名单元格都用反引号包裹），天然被过滤掉。
fn is_table_row(line: &str) -> bool {
    line.trim_start().starts_with("| `")
}

fn split_row(line: &str) -> Vec<&str> {
    line.trim()
        .trim_matches('|')
        .split('|')
        .map(str::trim)
        .collect()
}

/// 反引号包裹的片段（§41.2 单元格里每个 family 名都独立包一对反引号，`/` 分隔多个）。
fn backtick_contents(cell: &str) -> Vec<&str> {
    cell.split('`')
        .enumerate()
        .filter_map(|(i, s)| (i % 2 == 1).then_some(s))
        .collect()
}

/// `name{label,label}` -> (name, {label,label})；无花括号 -> (name, {})。
fn parse_family_labels(content: &str) -> (String, BTreeSet<String>) {
    match content.find('{') {
        Some(brace) => {
            let name = content[..brace].trim().to_string();
            let end = content.find('}').unwrap_or(content.len());
            let labels = content[brace + 1..end]
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
            (name, labels)
        }
        None => (content.trim().to_string(), BTreeSet::new()),
    }
}

/// 取数点列「处数」：找 `·`（可能带前置 `各`）后紧跟的整数。逐个 `·` 出现位置尝试，
/// 第一个能解出数字的即为答案——`tombstoned_unpurged_over_sla` 那行 `· 1；扫 ... 12 列闸`
/// 里的 `12` 不紧跟在任何 `·` 后，天然不会被误取。
fn extract_emit_count(cell: &str) -> Option<u32> {
    for (i, c) in cell.char_indices() {
        if c != '·' {
            continue;
        }
        let rest = cell[i + c.len_utf8()..].trim_start();
        let rest = rest.strip_prefix('各').map(str::trim_start).unwrap_or(rest);
        let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
        if let Ok(n) = digits.parse() {
            return Some(n);
        }
    }
    None
}

/// 量纲列第一段（`counter`/`gauge`/`histogram`），`·` 前的部分。
fn extract_kind(cell: &str) -> String {
    cell.split('·').next().unwrap_or("").trim().to_string()
}

/// 解析 §41.2 全表 -> `Vec<RegistryEntry>`（任务卡①）。单行解析失败（拿不到处数）的行
/// 整行跳过而非 panic——真实 spec 是否每行都能解析，由 `real_spec_parses_known_rows` 兜底。
pub fn parse_registry(spec: &str) -> Vec<RegistryEntry> {
    let Some(section) = extract_registry_section(spec) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for line in section.lines().filter(|l| is_table_row(l)) {
        let cols = split_row(line);
        let (Some(family_cell), Some(emit_cell), Some(kind_cell)) =
            (cols.first(), cols.get(1), cols.get(2))
        else {
            continue;
        };
        let Some(declared_emit_count) = extract_emit_count(emit_cell) else {
            continue;
        };
        let metric_kind = extract_kind(kind_cell);
        for content in backtick_contents(family_cell) {
            let (family, labels) = parse_family_labels(content);
            if family.is_empty() {
                continue;
            }
            out.push(RegistryEntry {
                family,
                labels,
                declared_emit_count,
                metric_kind: metric_kind.clone(),
            });
        }
    }
    out
}

// ---------------------------------------------------------------------------
// C: static scan of crates/**/src/**/*.rs emit call sites
// ---------------------------------------------------------------------------

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if matches!(name, "target" | ".git" | "node_modules") {
                continue;
            }
            collect_rs_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs")
            && path.components().any(|c| c.as_os_str() == "src")
        {
            // 只扫生产代码（路径含 `src/` 段）——排除 `tests/`，避免 C 把测试自己的断言
            // 调用当成发射点，也避免和 W（`testkit/tests/metrics/`）的扫描域重叠。
            out.push(path);
        }
    }
}

/// 按 rustfmt 惯例整块跳过顶层 `#[cfg(test)] mod tests { ... }`（`}` 独占一行、无缩进即
/// 判定为块尾）。不做花括号深度计数——字符串字面量里的 `{`/`}` 会让深度计数出错，而本仓库
/// 测试模块清一色是 rustfmt 输出的顶层 `mod tests`，行首 `}` 已经是可靠边界。
// ponytail: 假设 rustfmt 格式（顶层 mod、`}` 顶格收尾）；非顶层或手工重排缩进的
// `#[cfg(test)]` 块会被误跳过或不跳过。升级路径：字符串/注释感知的花括号计数器。
fn strip_cfg_test_blocks(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    let mut skipping = false;
    for line in src.lines() {
        if !skipping && line.trim() == "#[cfg(test)]" {
            skipping = true;
            continue;
        }
        if skipping {
            if line == "}" {
                skipping = false;
            }
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

fn is_screaming_char(b: u8) -> bool {
    b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// 一行里所有 `SCREAMING_IDENT.inc(`/`.observe(`/`.set(` 调用，取出 ident 并转小写
/// （R2/R4 的落地：family 名与它的 static 名字只差大小写，是本仓库当前唯一的真实约定，
/// 见 `crates/telemetry/src/degrade.rs` 的 `DEGRADE_TOTAL`）。任务卡「按注册名匹配调用点
/// 计数」在这里体现为：先通用抽取调用点名字，再按名字分组计数/比对 R——不是反过来只对
/// R 里已登记的名字做定向搜索，否则 D3（私加未登记名字）永远抓不到。
fn find_screaming_calls_in_line(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    if line.trim_start().starts_with("//") {
        // whole-line comment (注错1: `// DEGRADE_TOTAL.inc();`) carries no real emit.
        return out;
    }
    for pat in [".inc(", ".observe(", ".set("] {
        let bytes = line.as_bytes();
        let mut start = 0;
        while let Some(pos) = line[start..].find(pat) {
            let abs = start + pos;
            let mut i = abs;
            while i > 0 && is_screaming_char(bytes[i - 1]) {
                i -= 1;
            }
            let run = &line[i..abs];
            let boundary_ok = i == 0 || !is_ident_byte(bytes[i - 1]);
            if boundary_ok && !run.is_empty() && run.bytes().any(|b| b.is_ascii_uppercase()) {
                out.push(run.to_ascii_lowercase());
            }
            start = abs + pat.len();
        }
    }
    out
}

/// `// labels: a,b,c`（`-` 或空 = 空集）。本仓库尚无任何真实 label 向量指标实现
/// （见模块文档），这是纯静态占位约定，只在紧邻发射调用的同行/上一行生效。
fn parse_labels_comment(text: &str) -> Option<BTreeSet<String>> {
    let rest = text.split("// labels:").nth(1)?.trim();
    if rest.is_empty() || rest == "-" {
        return Some(BTreeSet::new());
    }
    Some(
        rest.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect(),
    )
}

/// Production call sites of `helper` in one line: `helper(` at an identifier boundary, not its own
/// `fn helper(` definition, not in a whole-line comment.
fn count_helper_calls_in_line(line: &str, helper: &str) -> usize {
    if line.trim_start().starts_with("//") {
        return 0;
    }
    let pat = format!("{helper}(");
    line.match_indices(&pat)
        .filter(|(at, _)| {
            let before = &line[..*at];
            !before.ends_with("fn ") && before.as_bytes().last().is_none_or(|b| !is_ident_byte(*b))
        })
        .count()
}

/// [`EMIT_HELPERS`]' production call sites under `crates_root` (cfg(test) blocks stripped), per helper.
fn scan_helper_calls(crates_root: &Path) -> BTreeMap<&'static str, usize> {
    let mut files = Vec::new();
    collect_rs_files(crates_root, &mut files);
    let mut out: BTreeMap<&'static str, usize> =
        EMIT_HELPERS.iter().map(|(_, h)| (*h, 0)).collect();
    for path in files {
        let Ok(raw) = fs::read_to_string(&path) else {
            continue;
        };
        for line in strip_cfg_test_blocks(&raw).lines() {
            for (helper, n) in out.iter_mut() {
                *n += count_helper_calls_in_line(line, helper);
            }
        }
    }
    out
}

/// 扫 `crates_root` 下所有生产代码文件，返回 (family -> 调用点计数, family -> 观测到的 label 集合并集)。
fn scan_code(crates_root: &Path) -> (BTreeMap<String, usize>, BTreeMap<String, BTreeSet<String>>) {
    let mut files = Vec::new();
    collect_rs_files(crates_root, &mut files);
    let mut counts = BTreeMap::new();
    let mut labels: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for path in files {
        let Ok(raw) = fs::read_to_string(&path) else {
            continue;
        };
        let stripped = strip_cfg_test_blocks(&raw);
        let lines: Vec<&str> = stripped.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            for family in find_screaming_calls_in_line(line) {
                *counts.entry(family.clone()).or_insert(0) += 1;
                let lbl = parse_labels_comment(line).or_else(|| {
                    (i > 0)
                        .then(|| parse_labels_comment(lines[i - 1]))
                        .flatten()
                });
                if let Some(lset) = lbl {
                    labels.entry(family).or_default().extend(lset);
                }
            }
        }
    }
    (counts, labels)
}

// ---------------------------------------------------------------------------
// W: crates/testkit/tests/metrics/<family>.rs enumeration
// ---------------------------------------------------------------------------

/// `// witness: family=<f> labels=<a,b,c|->` — identity + expected label set **only**.
/// Never carries pass/fail counts (blocker fix: those used to be hand-typed integers
/// that D2/D6 trusted verbatim without ever running anything — see module doc and
/// [`run_witness_probe`], which is now the only source of pass/fail evidence).
/// `family=` mismatching the file's own stem is treated as a damaged marker (no label
/// evidence), never silently trusted off the filename.
fn parse_witness_meta(content: &str, expected_family: &str) -> Option<BTreeSet<String>> {
    let marker = content
        .lines()
        .find_map(|l| l.trim().strip_prefix("// witness:"))?;
    let mut family = None;
    let mut labels = None;
    for tok in marker.split_whitespace() {
        let Some((k, v)) = tok.split_once('=') else {
            continue;
        };
        match k {
            "family" => family = Some(v),
            "labels" => {
                labels = Some(if v == "-" {
                    BTreeSet::new()
                } else {
                    v.split(',').map(String::from).collect()
                })
            }
            _ => {}
        }
    }
    (family == Some(expected_family)).then(|| labels.unwrap_or_default())
}

/// This workspace's library crates as `(package name, absolute crate dir)`, read
/// straight off each `crates/*/Cargo.toml`'s `name = "..."` line — no `toml` crate
/// pulled in for one field, matching how every other parser in this file reads its
/// one source of truth by hand. `testkit` is excluded: witness files live inside it
/// but must call *production* crates, not depend back on the harness that hosts them.
fn discover_dependency_crates(crates_root: &Path) -> Vec<(String, PathBuf)> {
    let mut out = Vec::new();
    let Ok(entries) = fs::read_dir(crates_root) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() || path.file_name().and_then(|n| n.to_str()) == Some("testkit") {
            continue;
        }
        let Ok(manifest) = fs::read_to_string(path.join("Cargo.toml")) else {
            continue;
        };
        let Some(name) = manifest.lines().find_map(|l| {
            l.trim()
                .strip_prefix("name = \"")
                .and_then(|rest| rest.strip_suffix('"'))
        }) else {
            continue;
        };
        out.push((name.to_string(), path.canonicalize().unwrap_or(path)));
    }
    out
}

/// `edition = "..."` under `[workspace.package]` in the root manifest, so the probe
/// package (see [`run_witness_probe`]) never drifts from the real workspace's edition.
fn workspace_edition(workspace_root: &Path) -> String {
    fs::read_to_string(workspace_root.join("Cargo.toml"))
        .ok()
        .and_then(|t| {
            t.lines().find_map(|l| {
                l.trim()
                    .strip_prefix("edition = \"")
                    .and_then(|rest| rest.strip_suffix('"'))
                    .map(String::from)
            })
        })
        .unwrap_or_else(|| "2021".to_string())
}

/// `test result: ok. 3 passed; 0 failed; ...` -> `(3, 3)`. `FAILED. 1 passed; 2 failed`
/// -> `(1, 3)`. No such line anywhere in `text` (compile error, timeout, panic before
/// the harness prints its summary) -> `(0, 0)` — the only shape that ever counts as
/// "no evidence", matching every other empty-evidence path in this module.
fn parse_test_result_line(text: &str) -> (u64, u64) {
    for line in text.lines() {
        let Some(rest) = line.trim().strip_prefix("test result: ") else {
            continue;
        };
        let mut passed = 0u64;
        let mut failed = 0u64;
        for part in rest.split(';') {
            let part = part.trim();
            if let Some(n) = part.strip_suffix(" passed") {
                passed = n
                    .split_whitespace()
                    .next_back()
                    .unwrap_or("")
                    .parse()
                    .unwrap_or(0);
            } else if let Some(n) = part.strip_suffix(" failed") {
                failed = n
                    .split_whitespace()
                    .next_back()
                    .unwrap_or("")
                    .parse()
                    .unwrap_or(0);
            }
        }
        return (passed, passed + failed);
    }
    (0, 0)
}

/// Compiles and **runs** exactly one witness file as a real `cargo test`, against a
/// throwaway probe package that path-depends on every production crate under
/// `crates_root` (via [`discover_dependency_crates`]) — so a witness can call a real
/// public entry point (`degrade::degrade_total_count()` and friends) and assert on a
/// real delta, per testkit README rule 2. Shares `<workspace_root>/target/…` as its
/// `--target-dir` so only the tiny probe binary itself needs compiling per call;
/// dependency crates stay cached across families and across repeated xtask runs.
///
/// This closes the blocker this function replaces: a witness used to need only a
/// `// witness: ... sample_count=<n>` comment — not even syntactically valid Rust —
/// to make D2/D6 green. Now the only evidence is cargo's own
/// `test result: … passed …` line, parsed by [`parse_test_result_line`] from a run
/// that actually happened. A compile error, a missing `#[test]`, a real assertion
/// failure, a process that never finishes (120s budget below) — all of them are
/// indistinguishable from "no witness" to D2/D6, exactly as they should be.
///
// ponytail: poll-based timeout via `Child::try_wait` instead of pulling in a
// `wait-timeout`-shaped crate — one coarse 120s/family budget is all this gate needs;
// upgrade to a real async/signal-based kill if that ever proves too slow.
fn run_witness_probe(
    workspace_root: &Path,
    crates_root: &Path,
    family: &str,
    witness_src: &Path,
) -> (u64, u64) {
    let probe_dir = workspace_root.join("target").join("metrics-witness-probe");
    let tests_dir = probe_dir.join("tests");
    if fs::create_dir_all(&tests_dir).is_err() {
        return (0, 0);
    }
    // Cargo auto-discovers every `tests/*.rs` file as its own test target — a stale
    // file from a previous family in this same probe dir must not leak into this run.
    if let Ok(entries) = fs::read_dir(&tests_dir) {
        for e in entries.flatten() {
            let _ = fs::remove_file(e.path());
        }
    }
    let Ok(src) = fs::read_to_string(witness_src) else {
        return (0, 0);
    };
    if fs::write(tests_dir.join(format!("{family}.rs")), src).is_err() {
        return (0, 0);
    }
    let mut manifest = format!(
        "[package]\nname = \"metrics-witness-probe\"\nversion = \"0.0.0\"\nedition = \"{}\"\npublish = false\n\n[workspace]\n\n[dependencies]\n",
        workspace_edition(workspace_root)
    );
    for (name, path) in discover_dependency_crates(crates_root) {
        manifest.push_str(&format!("{name} = {{ path = {path:?} }}\n"));
    }
    if fs::write(probe_dir.join("Cargo.toml"), manifest).is_err() {
        return (0, 0);
    }

    // dep: subprocess(cargo) — compiles and runs the witness probe package
    let mut child = match std::process::Command::new("cargo")
        .arg("test")
        .arg("--manifest-path")
        .arg(probe_dir.join("Cargo.toml"))
        .arg("--target-dir")
        .arg(
            workspace_root
                .join("target")
                .join("metrics-witness-probe-out"),
        )
        .arg("--offline")
        .args(["--", "--test-threads=1"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(_) => return (0, 0),
    };

    let start = std::time::Instant::now();
    let output = loop {
        match child.try_wait() {
            Ok(Some(_)) => break child.wait_with_output().ok(),
            Ok(None) if start.elapsed() > std::time::Duration::from_secs(120) => {
                let _ = child.kill();
                break None;
            }
            Ok(None) => std::thread::sleep(std::time::Duration::from_millis(100)),
            Err(_) => break None,
        }
    };
    match output {
        Some(o) => parse_test_result_line(&String::from_utf8_lossy(&o.stdout)),
        None => (0, 0),
    }
}

/// `crates_root/testkit/tests/metrics/<family>.rs` 枚举（任务卡③：文件名即 family，
/// 不建第二张「family -> 文件名」表）。每个找到的文件都真实跑一次 [`run_witness_probe`]——
/// 文件存在只决定 D1/D3 的结构性判据，pass/fail 证据永远来自真实执行。返回 (存在证据的
/// family 集合, family -> 真实执行结果)。
fn scan_witness(crates_root: &Path) -> (BTreeSet<String>, BTreeMap<String, WitnessInfo>) {
    let dir = crates_root.join(WITNESS_SUBDIR);
    let mut exists = BTreeSet::new();
    let mut info = BTreeMap::new();
    let Ok(entries) = fs::read_dir(&dir) else {
        return (exists, info);
    };
    let workspace_root = crates_root.parent().unwrap_or(Path::new("."));
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let stem = stem.to_string();
        exists.insert(stem.clone());
        let labels = fs::read_to_string(&path)
            .ok()
            .and_then(|content| parse_witness_meta(&content, &stem));
        let (tests_passed, tests_total) =
            run_witness_probe(workspace_root, crates_root, &stem, &path);
        info.insert(
            stem,
            WitnessInfo {
                labels,
                tests_passed,
                tests_total,
            },
        );
    }
    (exists, info)
}

// ---------------------------------------------------------------------------
// D1-D6
// ---------------------------------------------------------------------------

/// 一个 family 在当前仓库里是否毫无交付痕迹：代码零调用点 **且** witness 文件不存在。
/// 只有这种"两边都是零"的 family 才有资格被默认模式豁免——半成品（比如现在的
/// `degrade_total`：有 C 无 W）不豁免，那是没写完（repo CLAUDE.md「拒绝伪修复」）。
fn is_unimplemented(
    family: &str,
    code_counts: &BTreeMap<String, usize>,
    witness_exists: &BTreeSet<String>,
) -> bool {
    code_counts.get(family).copied().unwrap_or(0) == 0 && !witness_exists.contains(family)
}

/// D1: `families(R) \ families(C) != ∅` -> 红「表有代码无」。
fn check_d1(
    registry: &[RegistryEntry],
    code_counts: &BTreeMap<String, usize>,
    witness_exists: &BTreeSet<String>,
) -> DCheck {
    let missing: Vec<&str> = registry
        .iter()
        .map(|r| r.family.as_str())
        .filter(|f| code_counts.get(*f).copied().unwrap_or(0) == 0)
        .collect();
    let (status, na_families) = status_for(missing.iter().copied(), code_counts, witness_exists);
    let detail = if missing.is_empty() {
        format!("{} families 均有 >=1 代码发射点", registry.len())
    } else {
        format!("表有代码无 families(R)\\families(C) = {missing:?}")
    };
    DCheck {
        id: "D1",
        status,
        detail,
        na_families,
    }
}

/// D2: `∀f: witness 文件存在 && cargo test 真跑该文件 && 全部 #[test] 真通过`
/// （见 [`run_witness_probe`]——不再信任任何手写数字，见模块文档）。
fn check_d2(
    registry: &[RegistryEntry],
    code_counts: &BTreeMap<String, usize>,
    witness_exists: &BTreeSet<String>,
    witness_info: &BTreeMap<String, WitnessInfo>,
) -> DCheck {
    let bad: Vec<&str> = registry
        .iter()
        .map(|r| r.family.as_str())
        .filter(|f| {
            let ok = witness_exists.contains(*f)
                && witness_info
                    .get(*f)
                    .is_some_and(|w| w.tests_total > 0 && w.tests_passed == w.tests_total);
            !ok
        })
        .collect();
    let (status, na_families) = status_for(bad.iter().copied(), code_counts, witness_exists);
    let detail = if bad.is_empty() {
        format!(
            "{} families 均恰 1 个 witness 且 cargo test 真实全部通过",
            registry.len()
        )
    } else {
        format!("witness 缺失/未真实通过: {bad:?}")
    };
    DCheck {
        id: "D2",
        status,
        detail,
        na_families,
    }
}

/// D3: `(families(C) ∪ families(W)) \ families(R) != ∅` -> 红「实现有表无」。
/// 从不豁免——多出来的名字本身就是"已经私加"，不存在"还没到这期"的说法。
fn check_d3(
    registry: &[RegistryEntry],
    code_counts: &BTreeMap<String, usize>,
    witness_exists: &BTreeSet<String>,
) -> DCheck {
    let r_families: BTreeSet<&str> = registry.iter().map(|r| r.family.as_str()).collect();
    let rogue: BTreeSet<&str> = code_counts
        .keys()
        .map(String::as_str)
        .chain(witness_exists.iter().map(String::as_str))
        .filter(|f| !r_families.contains(*f))
        .collect();
    if rogue.is_empty() {
        DCheck {
            id: "D3",
            status: GateStatus::Pass,
            detail: "代码/witness 未出现表外 family".to_string(),
            na_families: BTreeSet::new(),
        }
    } else {
        DCheck {
            id: "D3",
            status: GateStatus::Fail,
            detail: format!("实现有表无 (families(C)∪families(W))\\families(R) = {rogue:?}"),
            na_families: BTreeSet::new(),
        }
    }
}

/// D4: `∀f: labels(R,f)==labels(C,f)` 且（有 witness 证据时）`==labels(W,f)`。
/// C 侧比对独立于 witness 是否存在——witness 缺失是 D2 的职责，但「代码里的 label 集
/// 是否偷偷偏离 registry」是 D4 自己的职责，不能因为还没写 witness 就完全不设防
/// （§80.2 D4：否则往一个尚无 witness 的 family 生产 emit 点私加 label，六条 D-check
/// 没有一条会抓到）。一旦有任何一侧（C 或 W）给出证据，比对结果都算 real——label
/// 集偏离从来不是"没交付"，是"交付错了"。
fn check_d4(
    registry: &[RegistryEntry],
    code_labels: &BTreeMap<String, BTreeSet<String>>,
    witness_info: &BTreeMap<String, WitnessInfo>,
) -> DCheck {
    let mut bad = Vec::new();
    for r in registry {
        let c_mismatch = code_labels
            .get(&r.family)
            .is_some_and(|c_labels| c_labels != &r.labels);
        let w_mismatch = witness_info
            .get(&r.family)
            .is_some_and(|w| w.labels.clone().unwrap_or_default() != r.labels);
        if c_mismatch || w_mismatch {
            bad.push(r.family.as_str());
        }
    }
    if bad.is_empty() {
        DCheck {
            id: "D4",
            status: GateStatus::Pass,
            detail: "有证据的 family（C 和/或 W）的 label 集与 registry 一致".to_string(),
            na_families: BTreeSet::new(),
        }
    } else {
        DCheck {
            id: "D4",
            status: GateStatus::Fail,
            detail: format!("label 集不一致: {bad:?}"),
            na_families: BTreeSet::new(),
        }
    }
}

/// D5: `∀f: actual_emit_callsite_count(C,f) == declared_emit_count(R,f)`; for a registered family
/// in [`EMIT_HELPERS`] its helper's production callers must also equal the declared count.
fn check_d5(
    registry: &[RegistryEntry],
    code_counts: &BTreeMap<String, usize>,
    witness_exists: &BTreeSet<String>,
    helper_calls: &BTreeMap<&'static str, usize>,
) -> DCheck {
    // (family, site kind, declared, actual): `.inc(` sites, then each EMIT_HELPERS helper's callers.
    let bad: Vec<(&str, String, u32, usize)> = registry
        .iter()
        .flat_map(|r| {
            let sites = code_counts.get(&r.family).copied().unwrap_or(0);
            std::iter::once((String::new(), sites))
                .chain(
                    EMIT_HELPERS
                        .iter()
                        .filter(|(f, _)| *f == r.family)
                        .map(|(_, h)| {
                            (
                                format!("<-{h}()"),
                                helper_calls.get(h).copied().unwrap_or(0),
                            )
                        }),
                )
                .filter(|(_, actual)| *actual as u32 != r.declared_emit_count)
                .map(|(kind, actual)| (r.family.as_str(), kind, r.declared_emit_count, actual))
                .collect::<Vec<_>>()
        })
        .collect();
    let (status, na_families) = status_for(
        bad.iter().map(|(f, _, _, _)| *f),
        code_counts,
        witness_exists,
    );
    let detail = if bad.is_empty() {
        format!("{} families 发射点计数与声明一致", registry.len())
    } else {
        format!(
            "处数不等(family,declared,actual): {:?}",
            bad.iter()
                .map(|(f, k, d, a)| format!("{f}{k}={d}/{a}"))
                .collect::<Vec<_>>()
        )
    };
    DCheck {
        id: "D5",
        status,
        detail,
        na_families,
    }
}

/// D6 正哨兵：`|R|>0 && |W|==|R| && sum(tests_total)>0 && sum(tests_passed)==sum(tests_total)`。
/// coverage 不满（还有 family 没 witness）算未交付，整体豁免进 not_applicable；
/// coverage 满但 [`run_witness_probe`] 真跑出来的通过数不等于总数（witness 都在、但真实
/// 断言没有全过——对应 §80.2 注错6「scrape target matcher 改成不存在的 job」的等价物：
/// 真实执行观察到的信号是坏的）算真的坏了，不豁免。
fn check_d6(
    registry: &[RegistryEntry],
    code_counts: &BTreeMap<String, usize>,
    witness_exists: &BTreeSet<String>,
    witness_info: &BTreeMap<String, WitnessInfo>,
) -> DCheck {
    let total = registry.len();
    let missing: Vec<&str> = registry
        .iter()
        .map(|r| r.family.as_str())
        .filter(|f| !witness_exists.contains(*f))
        .collect();
    let present_count = total - missing.len();
    let sum_passed: u64 = registry
        .iter()
        .filter_map(|r| witness_info.get(&r.family))
        .map(|w| w.tests_passed)
        .sum();
    let sum_total: u64 = registry
        .iter()
        .filter_map(|r| witness_info.get(&r.family))
        .map(|w| w.tests_total)
        .sum();
    let pass = total > 0 && present_count == total && sum_total > 0 && sum_passed == sum_total;
    let detail = format!(
        "|R|={total} |W|={present_count} tests_passed={sum_passed} tests_total={sum_total}"
    );
    if pass {
        return DCheck {
            id: "D6",
            status: GateStatus::Pass,
            detail,
            na_families: BTreeSet::new(),
        };
    }
    if !missing.is_empty() {
        // 覆盖率不满：missing 里有代码的 family（比如现在的 degrade_total）是半成品，
        // 不能靠"整体还没到 Phase 14"这把大伞盖过去——用同一条 is_unimplemented 判据
        // 逐个拆开，避免它同时被 D2 判 real、又被 D6 塞进 not_applicable 名单自相矛盾。
        let (status, na_families) = status_for(missing.into_iter(), code_counts, witness_exists);
        DCheck {
            id: "D6",
            status,
            detail,
            na_families,
        }
    } else {
        // coverage 满但真实通过数不等于总数：witness 都"存在"却真的没全过，真坏。
        DCheck {
            id: "D6",
            status: GateStatus::Fail,
            detail,
            na_families: BTreeSet::new(),
        }
    }
}

/// 对给定 (spec 全文, crates 根目录) 跑 R/C/W 采集 + D1–D6。`None` 表示 R 侧解析失败/
/// 表不存在——调用方必须直接 fail，不能当 `not_applicable`（§80.2 D6 正哨兵语义）。
pub fn check_all(spec: &str, crates_root: &Path) -> Option<Vec<DCheck>> {
    let registry = parse_registry(spec);
    if registry.is_empty() {
        return None;
    }
    let (code_counts, code_labels) = scan_code(crates_root);
    let (witness_exists, witness_info) = scan_witness(crates_root);
    Some(vec![
        check_d1(&registry, &code_counts, &witness_exists),
        check_d2(&registry, &code_counts, &witness_exists, &witness_info),
        check_d3(&registry, &code_counts, &witness_exists),
        check_d4(&registry, &code_labels, &witness_info),
        check_d5(
            &registry,
            &code_counts,
            &witness_exists,
            &scan_helper_calls(crates_root),
        ),
        check_d6(&registry, &code_counts, &witness_exists, &witness_info),
    ])
}

// ---------------------------------------------------------------------------
// D7: exported families, read from each process's real `--metrics-families`
// ---------------------------------------------------------------------------

/// One `# TYPE`d family in a text exposition: its declared kind and the label-key set of
/// every sample (histogram samples are folded onto the base name with `le` dropped).
#[derive(Debug, Default)]
struct ExpFamily {
    kind: String,
    samples: Vec<BTreeSet<String>>,
}

/// A parsed text exposition (format 0.0.4). `problems` holds lines the parser could not
/// attribute to a `# TYPE`d family; they are D7/EX failures, never skipped.
#[derive(Debug, Default)]
struct Exposition {
    families: BTreeMap<String, ExpFamily>,
    problems: Vec<String>,
}

/// Label keys of one sample's `{k="v",…}` block; values are skipped with `\`-escapes honoured.
/// `rest` is the sample line after its name; no `{` means no labels.
fn sample_label_keys(rest: &str) -> Result<BTreeSet<String>, String> {
    let Some(body) = rest.strip_prefix('{') else {
        return Ok(BTreeSet::new());
    };
    let mut keys = BTreeSet::new();
    let mut it = body.chars().peekable();
    loop {
        while matches!(it.peek(), Some(' ' | ',')) {
            it.next();
        }
        match it.peek() {
            Some('}') => return Ok(keys),
            None => return Err("unterminated label set".into()),
            _ => {}
        }
        let mut key = String::new();
        while let Some(&c) = it.peek() {
            if c == '=' {
                break;
            }
            key.push(c);
            it.next();
        }
        if it.next() != Some('=') || it.next() != Some('"') {
            return Err(format!("label `{key}` lacks =\"value\""));
        }
        let mut escaped = false;
        loop {
            match it.next() {
                None => return Err(format!("label `{key}` value unterminated")),
                Some('\\') if !escaped => escaped = true,
                Some('"') if !escaped => break,
                Some(_) => escaped = false,
            }
        }
        keys.insert(key.trim().to_string());
    }
}

fn parse_exposition(text: &str) -> Exposition {
    let mut e = Exposition::default();
    for line in text.lines().map(str::trim).filter(|l| !l.is_empty()) {
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let mut parts = rest.split_whitespace();
            if let (Some(name), Some(kind)) = (parts.next(), parts.next()) {
                e.families.entry(name.to_string()).or_default().kind = kind.to_string();
            }
            continue;
        }
        if line.starts_with('#') {
            continue;
        }
        let name_end = line
            .find(|c: char| c == '{' || c.is_whitespace())
            .unwrap_or(line.len());
        let name = &line[..name_end];
        let mut keys = match sample_label_keys(&line[name_end..]) {
            Ok(k) => k,
            Err(err) => {
                e.problems.push(format!("sample `{name}`: {err}"));
                continue;
            }
        };
        let family = if e.families.contains_key(name) {
            name.to_string()
        } else {
            // A histogram family has no sample under its own name, only these three suffixes.
            let base = ["_bucket", "_sum", "_count"].iter().find_map(|s| {
                name.strip_suffix(s)
                    .filter(|b| e.families.get(*b).is_some_and(|f| f.kind == "histogram"))
                    .map(|b| (b.to_string(), *s))
            });
            match base {
                Some((b, suffix)) => {
                    if suffix == "_bucket" {
                        keys.remove("le");
                    }
                    b
                }
                None => {
                    e.problems
                        .push(format!("sample `{name}` has no preceding `# TYPE` line"));
                    continue;
                }
            }
        };
        if let Some(f) = e.families.get_mut(&family) {
            f.samples.push(keys);
        }
    }
    e
}

/// The R / R6 / kind leg shared by D7 and EX: every family is registered, its kind is the
/// §41.2 kind, and every sample carries exactly the §41.2 label keys.
fn registry_problems(
    process: &str,
    exp: &Exposition,
    registry: &BTreeMap<&str, &RegistryEntry>,
) -> Vec<String> {
    let mut out: Vec<String> = exp
        .problems
        .iter()
        .map(|p| format!("{process}: {p}"))
        .collect();
    for (name, fam) in &exp.families {
        let Some(r) = registry.get(name.as_str()) else {
            out.push(format!(
                "{process}: `{name}` exported, not registered in §41.2"
            ));
            continue;
        };
        if fam.kind != r.metric_kind {
            out.push(format!(
                "{process}: `{name}` is a {} but §41.2 registers a {}",
                fam.kind, r.metric_kind
            ));
        }
        if let Some(keys) = fam.samples.iter().find(|k| **k != r.labels) {
            out.push(format!(
                "{process}: `{name}` sample label keys {keys:?} != §41.2 {:?} (R6)",
                r.labels
            ));
        }
    }
    out
}

fn registry_index(registry: &[RegistryEntry]) -> BTreeMap<&str, &RegistryEntry> {
    registry.iter().map(|r| (r.family.as_str(), r)).collect()
}

/// D7: the union of what the six process keys export is registered, kind- and label-exact.
/// `outputs` is `(process, its --metrics-families stdout or the run error)`. A process that
/// cannot be built or run is a fail naming it: it is in-tree, so there is no not_applicable.
/// Returns the check and the exported family union (D8(c) input): a family counts as exported
/// only when its render carries at least one sample line, since a `# TYPE` line alone gives a
/// rule nothing to read (ADR-0061 review-fix 3, F10).
fn check_d7(
    registry: &[RegistryEntry],
    outputs: &[(String, Result<String, String>)],
) -> (DCheck, BTreeSet<String>) {
    let index = registry_index(registry);
    let mut problems = Vec::new();
    let mut exported = BTreeSet::new();
    let mut per_process = Vec::new();
    for (process, out) in outputs {
        match out {
            Ok(text) => {
                let exp = parse_exposition(text);
                problems.extend(registry_problems(process, &exp, &index));
                let sampled: Vec<String> = exp
                    .families
                    .into_iter()
                    .filter(|(_, f)| !f.samples.is_empty())
                    .map(|(name, _)| name)
                    .collect();
                per_process.push(format!("{process}={}", sampled.len()));
                exported.extend(sampled);
            }
            Err(err) => problems.push(format!("humaux-{process} --metrics-families: {err}")),
        }
    }
    let check = if problems.is_empty() {
        DCheck {
            id: "D7",
            status: GateStatus::Pass,
            detail: format!(
                "{} exported families registered, kind- and label-exact ({})",
                exported.len(),
                per_process.join(" ")
            ),
            na_families: BTreeSet::new(),
        }
    } else {
        DCheck {
            id: "D7",
            status: GateStatus::Fail,
            detail: problems.join("; "),
            na_families: BTreeSet::new(),
        }
    };
    (check, exported)
}

/// Runs `cargo run -q -p humaux-<process> --bin humaux-<process> -- [--serve] --metrics-families`
/// (ADR-0061 D-C): cargo rebuilds from the current tree, so neither a stale binary nor a
/// hand list can disagree with what `/metrics` serves.
fn metrics_families_output(process: &str) -> Result<String, String> {
    // ADR-0062 D-S: the one process key that names a mode, not a binary.
    let (bin, mode): (&str, &[&str]) = match process {
        "maintenance-serve" => ("maintenance", &["--serve"]),
        other => (other, &[]),
    };
    let pkg = format!("humaux-{bin}");
    // dep: subprocess(cargo) — builds and runs the process's own zero-state render
    let out = std::process::Command::new("cargo")
        .args(["run", "-q", "-p", &pkg, "--bin", &pkg, "--"])
        .args(mode)
        .arg("--metrics-families")
        .output()
        .map_err(|e| format!("cannot spawn cargo: {e}"))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        let tail: Vec<&str> = stderr.lines().rev().take(3).collect();
        return Err(format!(
            "exited {} ({})",
            out.status,
            tail.into_iter().rev().collect::<Vec<_>>().join(" | ")
        ));
    }
    String::from_utf8(out.stdout).map_err(|e| format!("stdout is not UTF-8: {e}"))
}

// ---------------------------------------------------------------------------
// D8: families and label keys referenced by the loaded rule files
// ---------------------------------------------------------------------------

/// Every `expr:` value in a rule file: inline (quotes stripped) or a `|`/`>` block, plus any
/// continuation line indented deeper than the `expr:` key.
// ponytail: a line scanner, not a YAML parser; flow mappings (`{expr: …}`) are not read.
// Upgrade to a YAML crate if a rule file ever uses them.
fn rule_exprs(yaml: &str) -> Vec<String> {
    let lines: Vec<&str> = yaml.lines().collect();
    let mut out = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        let Some(rest) = trimmed.strip_prefix("expr:") else {
            continue;
        };
        let indent = line.len() - trimmed.len();
        let mut text = rest.trim().to_string();
        if text.starts_with('|') || text.starts_with('>') {
            text.clear();
        }
        for cont in &lines[i + 1..] {
            if cont.trim().is_empty() {
                continue;
            }
            if cont.len() - cont.trim_start().len() <= indent {
                break;
            }
            text.push(' ');
            text.push_str(cont.trim());
        }
        let t = text.trim();
        let t = ['\'', '"']
            .iter()
            .find_map(|q| t.strip_prefix(*q).and_then(|s| s.strip_suffix(*q)))
            .unwrap_or(t);
        out.push(t.to_string());
    }
    out
}

/// Index one past the quote that closes the string opening at `i`.
fn skip_quoted(b: &[u8], i: usize) -> usize {
    let q = b[i];
    let mut j = i + 1;
    while j < b.len() {
        if b[j] == b'\\' && q != b'`' {
            j += 2;
            continue;
        }
        if b[j] == q {
            return j + 1;
        }
        j += 1;
    }
    b.len()
}

/// Matcher keys of the `{…}` block opening at `i`, and the index one past its `}`.
fn matcher_keys(b: &[u8], i: usize) -> (BTreeSet<String>, usize) {
    let mut keys = BTreeSet::new();
    let mut j = i + 1;
    while j < b.len() && b[j] != b'}' {
        let c = b[j];
        if c == b'"' || c == b'\'' || c == b'`' {
            j = skip_quoted(b, j);
        } else if c.is_ascii_alphabetic() || c == b'_' {
            let start = j;
            while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
                j += 1;
            }
            keys.insert(String::from_utf8_lossy(&b[start..j]).into_owned());
        } else {
            j += 1;
        }
    }
    (keys, (j + 1).min(b.len()))
}

fn skip_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

/// The metric identifiers of one PromQL expression, each with the keys of its own `{…}`
/// matcher, plus the keys of every `by/without/on/ignoring/group_left/group_right (…)` list.
/// Functions and aggregations (a name followed by `(` or a grouping clause), keywords,
/// strings, `[…]` ranges and numbers are skipped.
fn expr_refs(expr: &str) -> (Vec<(String, BTreeSet<String>)>, BTreeSet<String>) {
    let b = expr.as_bytes();
    let mut idents = Vec::new();
    let mut grouping = BTreeSet::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'"' || c == b'\'' || c == b'`' {
            i = skip_quoted(b, i);
        } else if c == b'[' {
            i = b[i..]
                .iter()
                .position(|&x| x == b']')
                .map_or(b.len(), |p| i + p + 1);
        } else if c == b'{' {
            // a bare selector (`{__name__=…}`): its keys belong to no identifier here
            i = matcher_keys(b, i).1;
        } else if c.is_ascii_digit() || c == b'.' {
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'.' || b[i] == b'_') {
                i += 1;
            }
        } else if c.is_ascii_alphabetic() || c == b'_' || c == b':' {
            let start = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b':') {
                i += 1;
            }
            let word = &expr[start..i];
            let lower = word.to_ascii_lowercase();
            let next = skip_ws(b, i);
            if PROMQL_KEYWORDS.contains(&lower.as_str()) {
                let lists = matches!(
                    word,
                    "by" | "without" | "on" | "ignoring" | "group_left" | "group_right"
                );
                if lists && b.get(next) == Some(&b'(') {
                    let close = b[next..]
                        .iter()
                        .position(|&x| x == b')')
                        .map_or(b.len(), |p| next + p);
                    grouping.extend(
                        expr[next + 1..close]
                            .split(',')
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(String::from),
                    );
                    i = (close + 1).min(b.len());
                }
            } else if PROMQL_AGGREGATIONS.contains(&word) || b.get(next) == Some(&b'(') {
                // function or aggregation call
            } else if b.get(next) == Some(&b'{') {
                let (keys, end) = matcher_keys(b, next);
                idents.push((word.to_string(), keys));
                i = end;
            } else {
                idents.push((word.to_string(), BTreeSet::new()));
            }
        } else {
            i += 1;
        }
    }
    (idents, grouping)
}

/// The §41.2 family an identifier names, and its label keys: a registered name, or a
/// histogram's `_bucket` (adds `le`), `_sum` or `_count` series.
fn resolve_family<'a>(
    ident: &str,
    index: &BTreeMap<&str, &'a RegistryEntry>,
) -> Option<(&'a RegistryEntry, bool)> {
    if let Some(r) = index.get(ident) {
        return Some((r, false));
    }
    ["_bucket", "_sum", "_count"].iter().find_map(|s| {
        let base = ident.strip_suffix(s)?;
        let r = index.get(base).filter(|r| r.metric_kind == "histogram")?;
        Some((*r, *s == "_bucket"))
    })
}

/// D8 over `rules` = `(file name, content)` of every `*.rules.yml`:
/// (a) every identifier is a §41.2 family or a §42 ① third-party family;
/// (b) every matcher key is a label of its family, and every grouping key a label of some
///     family in the same expression (§42 ④);
/// (c) every referenced family is exported (D7), unless [`NOT_YET_PRODUCED`] names its
///     producer card (not_applicable); a listed family that is exported fails as stale;
/// (d) no rule file or no `expr:` is a fail (sentinel).
fn check_d8(
    registry: &[RegistryEntry],
    rules: &[(String, String)],
    exported: &BTreeSet<String>,
) -> DCheck {
    let index = registry_index(registry);
    let mut problems = Vec::new();
    let mut referenced: BTreeMap<String, String> = BTreeMap::new();
    let mut exprs = 0usize;
    for (file, content) in rules {
        for expr in rule_exprs(content) {
            exprs += 1;
            let (idents, grouping) = expr_refs(&expr);
            let mut expr_labels = BTreeSet::new();
            for (ident, keys) in &idents {
                if THIRD_PARTY_PREFIXES.iter().any(|p| ident.starts_with(p)) {
                    continue;
                }
                let Some((r, bucket)) = resolve_family(ident, &index) else {
                    problems.push(format!("{file}: `{ident}` is not a §41.2 family (§41.4②)"));
                    continue;
                };
                let mut allowed = r.labels.clone();
                if bucket {
                    allowed.insert("le".to_string());
                }
                for k in keys.difference(&allowed) {
                    problems.push(format!(
                        "{file}: `{ident}{{{k}…}}` — `{k}` is not a §41.2 label of `{}` (§42 ④)",
                        r.family
                    ));
                }
                expr_labels.extend(allowed);
                referenced
                    .entry(r.family.clone())
                    .or_insert_with(|| file.clone());
            }
            for k in grouping.difference(&expr_labels) {
                problems.push(format!(
                    "{file}: grouping key `{k}` is not a §41.2 label of any family in `{expr}` (§42 ④)"
                ));
            }
        }
    }
    if rules.is_empty() || exprs == 0 {
        problems.push(format!(
            "no `expr:` in any {RULES_DIR}/*{RULES_SUFFIX} (sentinel: the rules leg reads nothing)"
        ));
    }
    let mut na = BTreeSet::new();
    let mut producers = Vec::new();
    for (family, file) in &referenced {
        if exported.contains(family) {
            continue;
        }
        match NOT_YET_PRODUCED.iter().find(|(f, _)| f == family) {
            Some((_, card)) => {
                na.insert(family.clone());
                producers.push(format!("{family} (producer: {card})"));
            }
            None => problems.push(format!(
                "{file}: `{family}` is referenced by a rule but no process exports it"
            )),
        }
    }
    for (family, card) in NOT_YET_PRODUCED {
        if exported.contains(*family) {
            problems.push(format!(
                "stale allowlist entry: `{family}` ({card}) is exported now; delete it from NOT_YET_PRODUCED"
            ));
        }
    }
    let status = if !problems.is_empty() {
        GateStatus::Fail
    } else if !na.is_empty() {
        GateStatus::NotApplicable
    } else {
        GateStatus::Pass
    };
    let mut detail = format!(
        "{} rule file(s), {exprs} expr(s), {} referenced families",
        rules.len(),
        referenced.len()
    );
    if !problems.is_empty() {
        detail.push_str(&format!("; {}", problems.join("; ")));
    }
    if !producers.is_empty() {
        detail.push_str(&format!(
            "; rule-referenced, not yet exported: {}",
            producers.join(", ")
        ));
    }
    DCheck {
        id: "D8",
        status,
        detail,
        na_families: na,
    }
}

/// `(file name, content)` of every `*.rules.yml` directly under `dir`, sorted by name.
fn read_rule_files(dir: &Path) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            if !name.ends_with(RULES_SUFFIX) {
                return None;
            }
            fs::read_to_string(e.path()).ok().map(|c| (name, c))
        })
        .collect();
    out.sort();
    out
}

// ---------------------------------------------------------------------------
// EX: live scrapes (`--exposition`) against the same parser
// ---------------------------------------------------------------------------

/// EX: each scraped file `(process, path, content or read error)` carries exactly its
/// process's `--metrics-families` family set, every family has ≥1 sample, and the D7
/// R / R6 / kind leg holds.
fn check_expositions(
    registry: &[RegistryEntry],
    files: &[(String, String, Result<String, String>)],
    families_of: &BTreeMap<String, Result<String, String>>,
) -> DCheck {
    let index = registry_index(registry);
    let mut problems = Vec::new();
    for (process, path, content) in files {
        let text = match content {
            Ok(t) => t,
            Err(e) => {
                problems.push(format!("{path}: {e}"));
                continue;
            }
        };
        let exp = parse_exposition(text);
        problems.extend(registry_problems(path, &exp, &index));
        for (name, fam) in &exp.families {
            if fam.samples.is_empty() {
                problems.push(format!("{path}: `{name}` has zero samples"));
            }
        }
        match families_of.get(process) {
            Some(Ok(expected)) => {
                let want: BTreeSet<String> =
                    parse_exposition(expected).families.into_keys().collect();
                let got: BTreeSet<String> = exp.families.keys().cloned().collect();
                let missing: Vec<&String> = want.difference(&got).collect();
                let extra: Vec<&String> = got.difference(&want).collect();
                if !missing.is_empty() || !extra.is_empty() {
                    problems.push(format!(
                        "{path}: family set != humaux-{process} --metrics-families (missing {missing:?}, extra {extra:?})"
                    ));
                }
            }
            Some(Err(e)) => problems.push(format!("humaux-{process} --metrics-families: {e}")),
            None => problems.push(format!("humaux-{process} --metrics-families: not run")),
        }
    }
    let status = if problems.is_empty() {
        GateStatus::Pass
    } else {
        GateStatus::Fail
    };
    let detail = if problems.is_empty() {
        format!(
            "{} scrape(s) match their process's --metrics-families, every family sampled, R/R6 hold",
            files.len()
        )
    } else {
        problems.join("; ")
    };
    DCheck {
        id: "EX",
        status,
        detail,
        na_families: BTreeSet::new(),
    }
}

/// Parsed command line (ADR-0061 D-H strict parser).
#[derive(Debug, Default, PartialEq, Eq)]
struct Opts {
    check: bool,
    strict: bool,
    expositions: Vec<(String, PathBuf)>,
}

/// Any argument outside `--check`, `--strict`, `--exposition <process>=<file>` is an error
/// naming it (E6: the old parser ignored it and ran the default mode).
fn parse_args(args: &[String]) -> Result<Opts, String> {
    let mut o = Opts::default();
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--check" => o.check = true,
            "--strict" => o.strict = true,
            "--exposition" => {
                let v = it.next().ok_or("`--exposition` needs <process>=<file>")?;
                let (p, f) = v
                    .split_once('=')
                    .ok_or_else(|| format!("`--exposition {v}`: expected <process>=<file>"))?;
                if !PROCESSES.contains(&p) {
                    return Err(format!(
                        "`--exposition {v}`: unknown process `{p}` (known: {PROCESSES:?})"
                    ));
                }
                o.expositions.push((p.to_string(), PathBuf::from(f)));
            }
            other => return Err(format!("unknown argument `{other}`")),
        }
    }
    Ok(o)
}

/// 打印每条 D-check 自己算出的三态（§57.1：pass/fail/not_applicable，`not_applicable`
/// 打印缺失对象名，就地打印，不是脱节的汇总行）。`--strict` 决定 `NotApplicable` 是否也
/// 计入退出码：默认模式下不计入（G80-6 未到 Phase 14 生效期的过渡豁免），`--strict` 下
/// 计入（真三方判定红绿）。`Fail` 永远计入退出码，与 `strict` 无关。
fn report(checks: &[DCheck], strict: bool) -> i32 {
    let mut failed = false;
    for c in checks {
        match c.status {
            GateStatus::Pass => {
                eprintln!("metrics-registry {}: pass — {}", c.id, c.detail);
            }
            GateStatus::Fail => {
                eprintln!("metrics-registry {}: fail — {}", c.id, c.detail);
                failed = true;
            }
            GateStatus::NotApplicable => {
                eprintln!(
                    "metrics-registry {}: not_applicable — missing object(s) {:?}: {}",
                    c.id, c.na_families, c.detail
                );
                if strict {
                    failed = true;
                }
            }
        }
    }
    i32::from(failed)
}

/// Entry point: exit 2 on a usage error, otherwise 1 iff a check fails (or, with `--strict`,
/// is not_applicable).
pub fn run(args: &[String]) -> i32 {
    let opts = match parse_args(args) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("metrics-registry: {e}\n{USAGE}");
            return 2;
        }
    };
    let spec = match fs::read_to_string(SPEC_PATH) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("metrics-registry: fail — cannot read {SPEC_PATH}: {e}");
            return 1;
        }
    };
    let registry = parse_registry(&spec);
    let mut checks = Vec::new();
    let d1_to_d6 = opts.check || opts.expositions.is_empty();
    let rows = if d1_to_d6 {
        check_all(&spec, Path::new(CRATES_DIR))
    } else {
        (!registry.is_empty()).then(Vec::new)
    };
    let Some(rows) = rows else {
        eprintln!(
            "metrics-registry: fail — §41.2 registry unreadable or empty at {SPEC_PATH} \
             (§80.2 D6 正哨兵语义: |R|>0 必须成立，这是 fail 不是 not_applicable)"
        );
        return 1;
    };
    checks.extend(rows);

    let needed: BTreeSet<&str> = if opts.check {
        PROCESSES.into_iter().collect()
    } else {
        opts.expositions.iter().map(|(p, _)| p.as_str()).collect()
    };
    let families_of: BTreeMap<String, Result<String, String>> = needed
        .into_iter()
        .map(|p| (p.to_string(), metrics_families_output(p)))
        .collect();
    if opts.check {
        let outputs: Vec<(String, Result<String, String>)> = families_of
            .iter()
            .map(|(p, o)| (p.clone(), o.clone()))
            .collect();
        let (d7, exported) = check_d7(&registry, &outputs);
        checks.push(d7);
        checks.push(check_d8(
            &registry,
            &read_rule_files(Path::new(RULES_DIR)),
            &exported,
        ));
    }
    if !opts.expositions.is_empty() {
        let files: Vec<(String, String, Result<String, String>)> = opts
            .expositions
            .iter()
            .map(|(p, path)| {
                let shown = path.display().to_string();
                let content = fs::read_to_string(path).map_err(|e| format!("cannot read: {e}"));
                (p.clone(), shown, content)
            })
            .collect();
        checks.push(check_expositions(&registry, &files, &families_of));
    }
    report(&checks, opts.strict)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn spec_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/architecture/Baseline_2.9.md")
    }

    // -- R parser against the real, frozen spec ----------------------------

    #[test]
    fn real_spec_registry_nonempty_and_known_rows_parse() {
        let text = fs::read_to_string(spec_path()).expect("spec must be readable");
        let registry = parse_registry(&text);
        assert!(!registry.is_empty(), "R sentinel: |R| must be > 0");

        let get = |name: &str| {
            registry
                .iter()
                .find(|r| r.family == name)
                .unwrap_or_else(|| {
                    panic!("family {name} not found in parsed registry: {registry:?}")
                })
        };

        let degrade = get("degrade_total");
        assert_eq!(degrade.declared_emit_count, 1);
        assert_eq!(degrade.metric_kind, "counter");
        assert_eq!(degrade.labels, BTreeSet::from(["code".to_string()]));

        let egress = get("egress_chars_total");
        assert_eq!(egress.declared_emit_count, 2);
        assert_eq!(egress.labels, BTreeSet::from(["domain".to_string()]));

        let mcp = get("humaux_mcp_requests_total");
        assert_eq!(
            mcp.labels,
            BTreeSet::from([
                "tool".to_string(),
                "result".to_string(),
                "plan_class".to_string()
            ])
        );

        // multi-family cell split by `/`, each gets the row's shared declared count.
        for f in ["evidence_highwater", "knowledge_highwater"] {
            let e = get(f);
            assert_eq!(e.declared_emit_count, 1);
            assert_eq!(e.metric_kind, "gauge");
        }
        for f in [
            "jobs_pending",
            "jobs_processing",
            "jobs_waiting_key",
            "jobs_dead",
        ] {
            assert_eq!(get(f).declared_emit_count, 1);
        }

        // no-label rows (⊕ new names, R6: 空 label 集也是全集).
        let tomb = get("tombstoned_unpurged_over_sla");
        assert!(tomb.labels.is_empty());
    }

    #[test]
    fn real_repo_r_sentinel_and_d3_pass_others_not_applicable_default_mode() {
        // 真实仓库当前只有 degrade_total 有代码、没有任何 family 有 witness——
        // 断言这条已知现状：R 非空、D3 干净（没有私加名字），退出码在默认模式下
        // 仍非 0（degrade_total 有代码无 witness，属半成品，D2 算 real，不豁免）。
        let spec_text = fs::read_to_string(spec_path()).expect("spec must be readable");
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let crates_root = workspace_root.join(CRATES_DIR);
        let checks = check_all(&spec_text, &crates_root).expect("R must parse");
        let d3 = checks.iter().find(|c| c.id == "D3").unwrap();
        assert_eq!(
            d3.status,
            GateStatus::Pass,
            "unexpected rogue family: {}",
            d3.detail
        );
        let d2 = checks.iter().find(|c| c.id == "D2").unwrap();
        assert!(d2.na_families.contains("humaux_mcp_requests_total"));
    }

    // -- shared fixture for the §80.2 six-injection red/green tests --------

    const FIXTURE_SPEC: &str = "\
## 41.2 注册表（全集）

| 指标名 | 取数点（章 · 动作 · 处数） | 量纲 | 消费方 |
|---|---|---|---|
| `degrade_total{code}` | §53.1 `abstain()` 内唯一 `.inc()` · 1 | counter·次 | test |
| `egress_chars_total{domain}` | §7.5 入口 B/C · 2 | counter·字符 | test |
| `humaux_retrieval_requests_total{intent,completeness_class}` | §20 planner · 1 | counter·次 | test |
| `humaux_mcp_requests_total{tool,result,plan_class}` | §33 gateway · 1 | counter·次 | test |

## 41.3 next
placeholder
";

    const FIXTURE_CODE: &str = "\
//! fixture production code.
pub fn abstain() {
    // labels: code
    DEGRADE_TOTAL.inc();
}

pub fn seal_card() {
    // labels: domain
    EGRESS_CHARS_TOTAL.inc();
}

pub fn seal_query() {
    // labels: domain
    EGRESS_CHARS_TOTAL.inc();
}

pub fn build_request() {
    // labels: intent,completeness_class
    HUMAUX_RETRIEVAL_REQUESTS_TOTAL.inc();
}

pub fn gateway_respond() {
    // labels: tool,result,plan_class
    HUMAUX_MCP_REQUESTS_TOTAL.inc();
}
";

    /// A **real**, self-contained `#[test]` (no production-crate dependency needed —
    /// the fixture proves `run_witness_probe` really compiles/runs/observes, not that
    /// it integrates with real telemetry) that bumps a local atomic and asserts on the
    /// real delta. `pass` picks the expected delta: `true` -> `1` (matches the real
    /// `fetch_add(1)`, so the assertion genuinely holds); `false` -> `999` (the
    /// assertion genuinely fails) — this is the real-execution equivalent of what used
    /// to be a hand-edited `sample_count=0` comment (§80.2 注错6).
    fn witness_file(family: &str, labels: &str, pass: bool) -> String {
        let expected = if pass { 1 } else { 999 };
        format!(
            "// witness: family={family} labels={labels}\n\
             #[test]\n\
             fn witness() {{\n\
                 static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);\n\
                 let before = COUNTER.load(std::sync::atomic::Ordering::Relaxed);\n\
                 COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);\n\
                 let after = COUNTER.load(std::sync::atomic::Ordering::Relaxed);\n\
                 assert_eq!(after, before + {expected}, \"fixture witness must observe a real counter delta\");\n\
             }}\n"
        )
    }

    static FIXTURE_SEQ: AtomicU64 = AtomicU64::new(0);

    const FIXTURE_WITNESSES: [(&str, &str); 4] = [
        ("degrade_total", "code"),
        ("egress_chars_total", "domain"),
        (
            "humaux_retrieval_requests_total",
            "intent,completeness_class",
        ),
        ("humaux_mcp_requests_total", "tool,result,plan_class"),
    ];

    /// 写一份可变的 fixture（spec md 副本 + `crates/fixture/src/lib.rs` + 4 个 witness 文件）
    /// 到系统临时目录（硬规则③：注错测试用 fixture 副本，禁止改坏仓库真文件）。
    fn write_fixture() -> (PathBuf, PathBuf) {
        let seq = FIXTURE_SEQ.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "metrics_registry_fixture_{}_{seq}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let crates_root = root.join("crates");
        fs::create_dir_all(crates_root.join("fixture/src")).unwrap();
        fs::create_dir_all(crates_root.join(WITNESS_SUBDIR)).unwrap();
        fs::write(crates_root.join("fixture/src/lib.rs"), FIXTURE_CODE).unwrap();
        for (family, labels) in FIXTURE_WITNESSES {
            fs::write(
                crates_root
                    .join(WITNESS_SUBDIR)
                    .join(format!("{family}.rs")),
                witness_file(family, labels, true),
            )
            .unwrap();
        }
        let spec_path = root.join("spec.md");
        fs::write(&spec_path, FIXTURE_SPEC).unwrap();
        (spec_path, crates_root)
    }

    fn run_fixture(spec_path: &Path, crates_root: &Path) -> Vec<DCheck> {
        let spec = fs::read_to_string(spec_path).unwrap();
        check_all(&spec, crates_root).expect("fixture R must parse")
    }

    fn status(checks: &[DCheck], id: &str) -> GateStatus {
        checks.iter().find(|c| c.id == id).unwrap().status
    }

    #[test]
    fn fixture_baseline_all_pass() {
        let (spec_path, crates_root) = write_fixture();
        let checks = run_fixture(&spec_path, &crates_root);
        for id in ["D1", "D2", "D3", "D4", "D5", "D6"] {
            assert_eq!(
                status(&checks, id),
                GateStatus::Pass,
                "{id}: {:?}",
                checks.iter().find(|c| c.id == id)
            );
        }
        fs::remove_dir_all(spec_path.parent().unwrap()).ok();
    }

    /// 注错1: 注释掉 `degrade_total` 唯一生产 emit 点 -> D1/D5 至少一条红（§80.2）。
    #[test]
    fn injection1_comment_out_degrade_total_emit_then_green() {
        let (spec_path, crates_root) = write_fixture();
        let code_path = crates_root.join("fixture/src/lib.rs");
        let mutated = FIXTURE_CODE.replace("DEGRADE_TOTAL.inc();", "// DEGRADE_TOTAL.inc();");
        assert_ne!(mutated, FIXTURE_CODE);
        fs::write(&code_path, &mutated).unwrap();
        let red = run_fixture(&spec_path, &crates_root);
        // witness exists (only the code emit is commented out), so both must be real
        // `Fail`, not swept into `NotApplicable`.
        assert_eq!(status(&red, "D1"), GateStatus::Fail);
        assert_eq!(status(&red, "D5"), GateStatus::Fail);

        fs::write(&code_path, FIXTURE_CODE).unwrap();
        let green = run_fixture(&spec_path, &crates_root);
        assert_eq!(status(&green, "D1"), GateStatus::Pass);
        assert_eq!(status(&green, "D5"), GateStatus::Pass);
        fs::remove_dir_all(spec_path.parent().unwrap()).ok();
    }

    /// 注错2: 私加 `retrieval_pool_total.inc()` -> D3 红。
    #[test]
    fn injection2_private_extra_emit_then_green() {
        let (spec_path, crates_root) = write_fixture();
        let code_path = crates_root.join("fixture/src/lib.rs");
        let mutated = format!(
            "{FIXTURE_CODE}\npub fn candidate_builder() {{\n    RETRIEVAL_POOL_TOTAL.inc();\n}}\n"
        );
        fs::write(&code_path, &mutated).unwrap();
        let red = run_fixture(&spec_path, &crates_root);
        assert_eq!(status(&red, "D3"), GateStatus::Fail);
        assert!(
            red.iter()
                .find(|c| c.id == "D3")
                .unwrap()
                .detail
                .contains("retrieval_pool_total")
        );

        fs::write(&code_path, FIXTURE_CODE).unwrap();
        let green = run_fixture(&spec_path, &crates_root);
        assert_eq!(status(&green, "D3"), GateStatus::Pass);
        fs::remove_dir_all(spec_path.parent().unwrap()).ok();
    }

    /// 注错3: 给 `humaux_retrieval_requests_total` 私加 `degraded` label -> D4 红。
    #[test]
    fn injection3_extra_label_then_green() {
        let (spec_path, crates_root) = write_fixture();
        let code_path = crates_root.join("fixture/src/lib.rs");
        let mutated = FIXTURE_CODE.replace(
            "// labels: intent,completeness_class",
            "// labels: intent,completeness_class,degraded",
        );
        assert_ne!(mutated, FIXTURE_CODE);
        fs::write(&code_path, &mutated).unwrap();
        let red = run_fixture(&spec_path, &crates_root);
        assert_eq!(status(&red, "D4"), GateStatus::Fail);

        fs::write(&code_path, FIXTURE_CODE).unwrap();
        let green = run_fixture(&spec_path, &crates_root);
        assert_eq!(status(&green, "D4"), GateStatus::Pass);
        fs::remove_dir_all(spec_path.parent().unwrap()).ok();
    }

    /// 注错4: 删掉 `egress_chars_total` 在 `seal_query()` 的一处 emit（2->1） -> D5 红。
    #[test]
    fn injection4_drop_one_of_two_emits_then_green() {
        let (spec_path, crates_root) = write_fixture();
        let code_path = crates_root.join("fixture/src/lib.rs");
        let mutated = FIXTURE_CODE.replace(
            "pub fn seal_query() {\n    // labels: domain\n    EGRESS_CHARS_TOTAL.inc();\n}\n\n",
            "",
        );
        assert_ne!(mutated, FIXTURE_CODE);
        fs::write(&code_path, &mutated).unwrap();
        let red = run_fixture(&spec_path, &crates_root);
        assert_eq!(status(&red, "D5"), GateStatus::Fail);
        assert!(
            red.iter()
                .find(|c| c.id == "D5")
                .unwrap()
                .detail
                .contains("egress_chars_total=2/1")
        );

        fs::write(&code_path, FIXTURE_CODE).unwrap();
        let green = run_fixture(&spec_path, &crates_root);
        assert_eq!(status(&green, "D5"), GateStatus::Pass);
        fs::remove_dir_all(spec_path.parent().unwrap()).ok();
    }

    /// 注错5: 删除 `crates/testkit/tests/metrics/humaux_mcp_requests_total.rs`
    /// -> witness_count 1->0 -> D2 红。
    #[test]
    fn injection5_delete_witness_file_then_green() {
        let (spec_path, crates_root) = write_fixture();
        let witness_path = crates_root
            .join(WITNESS_SUBDIR)
            .join("humaux_mcp_requests_total.rs");
        let backup = fs::read_to_string(&witness_path).unwrap();
        fs::remove_file(&witness_path).unwrap();
        let red = run_fixture(&spec_path, &crates_root);
        // code still emits it — must be real `Fail`, not swept into `NotApplicable`.
        assert_eq!(status(&red, "D2"), GateStatus::Fail);
        let d2 = red.iter().find(|c| c.id == "D2").unwrap();
        assert!(d2.detail.contains("humaux_mcp_requests_total"));

        fs::write(&witness_path, &backup).unwrap();
        let green = run_fixture(&spec_path, &crates_root);
        assert_eq!(status(&green, "D2"), GateStatus::Pass);
        fs::remove_dir_all(spec_path.parent().unwrap()).ok();
    }

    /// 注错6: 让全部 4 个 witness 的真实断言真的失败（§80.2 注错6「scrape target matcher
    /// 改成不存在的 job」的等价物——真实执行观察到的信号是坏的，不是靠编辑两个数字模拟）
    /// -> cargo test 真的报 0 passed -> D2/D6 红。
    #[test]
    fn injection6_broken_scrape_matcher_then_green() {
        let (spec_path, crates_root) = write_fixture();
        let dir = crates_root.join(WITNESS_SUBDIR);
        for (family, labels) in FIXTURE_WITNESSES {
            fs::write(
                dir.join(format!("{family}.rs")),
                witness_file(family, labels, false),
            )
            .unwrap();
        }
        let red = run_fixture(&spec_path, &crates_root);
        assert_eq!(status(&red, "D2"), GateStatus::Fail);
        assert_eq!(status(&red, "D6"), GateStatus::Fail);
        let d6 = red.iter().find(|c| c.id == "D6").unwrap();
        assert!(d6.detail.contains("tests_passed=0"));

        for (family, labels) in FIXTURE_WITNESSES {
            fs::write(
                dir.join(format!("{family}.rs")),
                witness_file(family, labels, true),
            )
            .unwrap();
        }
        let green = run_fixture(&spec_path, &crates_root);
        assert_eq!(status(&green, "D2"), GateStatus::Pass);
        assert_eq!(status(&green, "D6"), GateStatus::Pass);
        fs::remove_dir_all(spec_path.parent().unwrap()).ok();
    }

    // -- D7 / D8 / EX / flags (ADR-0061 D-H) ---------------------------------

    fn real_registry() -> Vec<RegistryEntry> {
        parse_registry(&fs::read_to_string(spec_path()).expect("spec must be readable"))
    }

    /// The frozen INV-1..4 file only (§42 freeze ③: byte-identical), so later rule files do
    /// not move these assertions.
    fn real_rules() -> Vec<(String, String)> {
        read_rule_files(
            &Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .join(RULES_DIR),
        )
        .into_iter()
        .filter(|(name, _)| name == "invariants.rules.yml")
        .collect()
    }

    fn gateway_exposition_without(drop: &str) -> String {
        let mut out = String::new();
        for (family, kind, labels) in [
            ("degrade_total", "counter", r#"code="ProjectionLag""#),
            (
                "humaux_retrieval_requests_total",
                "counter",
                r#"intent="text",completeness_class="complete""#,
            ),
            (
                "humaux_mcp_requests_total",
                "counter",
                r#"tool="recall",result="ok",plan_class="unclassified""#,
            ),
            (
                "data_disclosures_finalized_total",
                "counter",
                r#"outcome="SUCCESS""#,
            ),
            (
                "data_disclosures_reserved_unfinalized",
                "gauge",
                r#"age_bucket="gt_60s""#,
            ),
        ] {
            if family == drop {
                continue;
            }
            out.push_str(&format!(
                "# HELP {family} h\n# TYPE {family} {kind}\n{family}{{{labels}}} 0\n"
            ));
        }
        out
    }

    fn d7_of(text: &str) -> DCheck {
        check_d7(
            &real_registry(),
            &[("gateway".into(), Ok(text.to_string()))],
        )
        .0
    }

    #[test]
    fn exposition_parser_folds_histograms_and_honours_escapes() {
        let e = parse_exposition(
            "# HELP retrieval_provider_latency_seconds h\n\
             # TYPE retrieval_provider_latency_seconds histogram\n\
             retrieval_provider_latency_seconds_bucket{provider=\"p\",purpose=\"embedding\",region=\"r\",le=\"+Inf\"} 0\n\
             retrieval_provider_latency_seconds_sum{provider=\"p\",purpose=\"embedding\",region=\"r\"} 0\n\
             retrieval_provider_latency_seconds_count{provider=\"p\",purpose=\"embedding\",region=\"r\"} 0\n\
             # TYPE degrade_total counter\n\
             degrade_total{code=\"a\\\"b,c=\\\\\"} 1\n",
        );
        assert!(e.problems.is_empty(), "{:?}", e.problems);
        let h = &e.families["retrieval_provider_latency_seconds"];
        assert_eq!(h.samples.len(), 3);
        let want: BTreeSet<String> = ["provider", "purpose", "region"].map(String::from).into();
        assert!(h.samples.iter().all(|k| *k == want), "{:?}", h.samples);
        assert_eq!(
            e.families["degrade_total"].samples,
            vec![BTreeSet::from(["code".to_string()])]
        );
        let d7 = check_d7(
            &real_registry(),
            &[(
                "retrieval-worker".into(),
                Ok("# TYPE x_total counter\nx_total 1\nstray 1\n".into()),
            )],
        )
        .0;
        assert!(
            d7.detail.contains("`stray` has no preceding `# TYPE` line"),
            "{}",
            d7.detail
        );
    }

    /// T-H1: an exported family that §41.2 does not register is a D7 fail naming it.
    #[test]
    fn t_h1_d7_unregistered_family_fails_naming_it() {
        let green = d7_of(&gateway_exposition_without(""));
        assert_eq!(green.status, GateStatus::Pass, "{}", green.detail);
        let red = d7_of(&gateway_exposition_without("").replace("degrade_total", "degrade_totals"));
        assert_eq!(red.status, GateStatus::Fail);
        assert!(
            red.detail
                .contains("`degrade_totals` exported, not registered"),
            "{}",
            red.detail
        );
        let failed = check_d7(
            &real_registry(),
            &[("maintenance".into(), Err("exited 101".into()))],
        )
        .0;
        assert_eq!(failed.status, GateStatus::Fail);
        assert!(
            failed
                .detail
                .contains("humaux-maintenance --metrics-families: exited 101")
        );
    }

    /// ADR-0061 review-fix 3 (F10): a family whose render is HELP/TYPE only is not exported, so D8(c) cannot read a
    /// rule family as present when no series exists. Fault: count every `# TYPE` line ⇒ red.
    #[test]
    fn d7_a_help_type_only_family_is_not_exported() {
        let (_, exported) = check_d7(
            &real_registry(),
            &[(
                "maintenance".into(),
                Ok("# HELP jobs_dead h\n# TYPE jobs_dead gauge\n# HELP jobs_pending h\n# TYPE jobs_pending gauge\njobs_pending 0\n".into()),
            )],
        );
        assert_eq!(exported, BTreeSet::from(["jobs_pending".to_string()]));
    }

    /// T-H2: an exported kind that differs from §41.2 fails.
    #[test]
    fn t_h2_d7_wrong_kind_fails() {
        let red = d7_of(
            &gateway_exposition_without("")
                .replace("# TYPE degrade_total counter", "# TYPE degrade_total gauge"),
        );
        assert_eq!(red.status, GateStatus::Fail);
        assert!(
            red.detail
                .contains("`degrade_total` is a gauge but §41.2 registers a counter")
        );
    }

    /// T-H3: a sample label key outside §41.2 fails (R6).
    #[test]
    fn t_h3_d7_extra_label_key_fails() {
        let red = d7_of(&gateway_exposition_without("").replace(
            r#"degrade_total{code="ProjectionLag"}"#,
            r#"degrade_total{code="ProjectionLag",tenant="t1"}"#,
        ));
        assert_eq!(red.status, GateStatus::Fail);
        assert!(
            red.detail.contains("`degrade_total` sample label keys"),
            "{}",
            red.detail
        );
        assert!(red.detail.contains("(R6)"));
    }

    fn all_exported() -> BTreeSet<String> {
        parse_exposition(&gateway_exposition_without(""))
            .families
            .into_keys()
            .collect()
    }

    fn rule(expr: &str) -> Vec<(String, String)> {
        vec![(
            "t.rules.yml".to_string(),
            format!(
                "groups:\n  - name: g\n    rules:\n      - alert: A\n        expr: {expr}\n        labels:\n          severity: critical\n"
            ),
        )]
    }

    #[test]
    fn d8_passes_on_the_real_rule_files_when_their_families_are_exported() {
        let rules = real_rules();
        assert!(
            !rules.is_empty(),
            "deploy/prometheus/*.rules.yml must exist"
        );
        let d8 = check_d8(&real_registry(), &rules, &all_exported());
        assert_eq!(d8.status, GateStatus::Pass, "{}", d8.detail);
        // INV-1/2/4 are `|` blocks, INV-3 is inline: all four are read.
        assert!(
            d8.detail.contains("4 expr(s), 4 referenced families"),
            "{}",
            d8.detail
        );
    }

    #[test]
    fn expr_refs_reads_matchers_grouping_and_skips_functions() {
        let (idents, grouping) = expr_refs(
            "sum by (code) (rate(degrade_total{code=~\"Egress.*\"}[24h] offset 5m)) / ignoring(code) group_left sum(rate(degrade_total[24h])) > 0.4 and absent(vector(1))",
        );
        assert_eq!(
            idents,
            vec![
                (
                    "degrade_total".to_string(),
                    BTreeSet::from(["code".to_string()])
                ),
                ("degrade_total".to_string(), BTreeSet::new()),
            ]
        );
        assert_eq!(grouping, BTreeSet::from(["code".to_string()]));
    }

    /// T-H4: a rule naming a family outside §41.2 (`queries_total`, §41.4②) fails.
    #[test]
    fn t_h4_d8_rule_naming_unregistered_family_fails() {
        let d8 = check_d8(
            &real_registry(),
            &rule("sum(rate(queries_total[5m])) == 0"),
            &all_exported(),
        );
        assert_eq!(d8.status, GateStatus::Fail);
        assert!(
            d8.detail.contains("`queries_total` is not a §41.2 family"),
            "{}",
            d8.detail
        );
    }

    /// T-H5: `by (stream)` on the unlabeled `projection_lag_events` fails (§42 ④).
    #[test]
    fn t_h5_d8_by_stream_on_projection_lag_fails() {
        let mut exported = all_exported();
        exported.insert("projection_lag_events".into());
        let red = check_d8(
            &real_registry(),
            &rule("max by (stream) (projection_lag_events) > 100"),
            &exported,
        );
        assert_eq!(red.status, GateStatus::Fail);
        assert!(
            red.detail.contains("grouping key `stream`"),
            "{}",
            red.detail
        );
        let green = check_d8(
            &real_registry(),
            &rule("max(projection_lag_events) > 100"),
            &exported,
        );
        assert_eq!(green.status, GateStatus::Pass, "{}", green.detail);
    }

    /// T-H6: an allowlisted unexported family is not_applicable naming its producer card
    /// (exit 0, 1 with `--strict`); a non-allowlisted unexported one fails; an allowlisted one
    /// that is exported fails as stale.
    #[test]
    fn t_h6_d8_allowlist_is_the_only_not_applicable_path() {
        let registry = real_registry();
        let backup = rule("time() - backup_last_success_timestamp_seconds > 93600");
        let na = check_d8(&registry, &backup, &all_exported());
        assert_eq!(na.status, GateStatus::NotApplicable, "{}", na.detail);
        assert!(
            na.na_families
                .contains("backup_last_success_timestamp_seconds")
        );
        assert!(na.detail.contains("(producer: card 37)"), "{}", na.detail);
        assert_eq!(report(std::slice::from_ref(&na), false), 0);
        assert_eq!(report(std::slice::from_ref(&na), true), 1);

        let unlisted = check_d8(
            &registry,
            &rule("delta(jobs_dead[15m]) > 0"),
            &all_exported(),
        );
        assert_eq!(unlisted.status, GateStatus::Fail, "{}", unlisted.detail);
        assert!(
            unlisted
                .detail
                .contains("`jobs_dead` is referenced by a rule but no process exports it")
        );
        assert_eq!(report(std::slice::from_ref(&unlisted), false), 1);

        let mut exported = all_exported();
        exported.insert("backup_last_success_timestamp_seconds".into());
        let stale = check_d8(&registry, &real_rules(), &exported);
        assert_eq!(stale.status, GateStatus::Fail);
        assert!(
            stale.detail.contains(
                "stale allowlist entry: `backup_last_success_timestamp_seconds` (card 37)"
            ),
            "{}",
            stale.detail
        );
    }

    /// Card 34b (§41.2 R4): a helper-emitted family's real emit sites are its helper's
    /// production callers — none (the call deleted) or two (a second caller) is a D5 fail, and
    /// the definition, a comment and a longer identifier never count.
    #[test]
    fn d5_counts_emit_helper_callers_exactly() {
        let h = "count_committed_distill";
        assert_eq!(
            count_helper_calls_in_line("pub fn count_committed_distill(runs: u64) {", h),
            0
        );
        assert_eq!(
            count_helper_calls_in_line("    // count_committed_distill(1, 0);", h),
            0
        );
        assert_eq!(
            count_helper_calls_in_line("    recount_committed_distill(1, 0);", h),
            0
        );
        assert_eq!(
            count_helper_calls_in_line("    count_committed_distill(runs, outputs);", h),
            1
        );
        assert_eq!(
            count_helper_calls_in_line("    x::count_committed_distill(a, b);", h),
            1
        );

        let registry = vec![RegistryEntry {
            family: "private_distill_runs_total".into(),
            labels: BTreeSet::new(),
            declared_emit_count: 1,
            metric_kind: "counter".into(),
        }];
        let code_counts = BTreeMap::from([("private_distill_runs_total".to_string(), 1)]);
        let witnesses = BTreeSet::from(["private_distill_runs_total".to_string()]);
        let d5 = |callers: usize| {
            check_d5(
                &registry,
                &code_counts,
                &witnesses,
                &BTreeMap::from([(h, callers)]),
            )
        };
        assert_eq!(d5(1).status, GateStatus::Pass, "{}", d5(1).detail);
        for callers in [0, 2] {
            let red = d5(callers);
            assert_eq!(red.status, GateStatus::Fail, "{}", red.detail);
            assert!(
                red.detail.contains(&format!(
                    "private_distill_runs_total<-count_committed_distill()=1/{callers}"
                )),
                "{}",
                red.detail
            );
        }
    }

    /// T-H10: CoreMetricAbsent names `humaux_mcp_requests_total`; a gateway exposition
    /// without it is a D8(c) fail naming it (the review's P0: never a silent not_applicable).
    #[test]
    fn t_h10_core_metric_absent_family_unexported_fails() {
        let text = gateway_exposition_without("humaux_mcp_requests_total");
        let (d7, exported) = check_d7(&real_registry(), &[("gateway".into(), Ok(text))]);
        assert_eq!(d7.status, GateStatus::Pass, "{}", d7.detail);
        let d8 = check_d8(
            &real_registry(),
            &rule(
                "absent(humaux_retrieval_requests_total) or absent(degrade_total) or absent(humaux_mcp_requests_total)",
            ),
            &exported,
        );
        assert_eq!(d8.status, GateStatus::Fail);
        assert!(
            d8.detail.contains(
                "`humaux_mcp_requests_total` is referenced by a rule but no process exports it"
            ),
            "{}",
            d8.detail
        );
    }

    /// T-H7: no rule file, or rule files without `expr:`, is a fail (sentinel).
    #[test]
    fn t_h7_d8_empty_rules_dir_fails() {
        let empty = std::env::temp_dir().join(format!("mr_empty_rules_{}", std::process::id()));
        fs::create_dir_all(&empty).unwrap();
        let none = check_d8(&real_registry(), &read_rule_files(&empty), &all_exported());
        fs::remove_dir_all(&empty).ok();
        assert_eq!(none.status, GateStatus::Fail);
        assert!(none.detail.contains("no `expr:`"), "{}", none.detail);
        let no_expr = check_d8(
            &real_registry(),
            &[("x.rules.yml".into(), "groups: []\n".into())],
            &all_exported(),
        );
        assert_eq!(no_expr.status, GateStatus::Fail);
    }

    /// T-H8: an unknown flag exits 2 naming it, before any file is read.
    #[test]
    fn t_h8_unknown_flag_exits_2() {
        assert_eq!(run(&["--chek".to_string()]), 2);
        assert_eq!(
            parse_args(&["--chek".to_string()]),
            Err("unknown argument `--chek`".to_string())
        );
        assert!(
            parse_args(&["--exposition".into(), "gw=x.prom".into()])
                .unwrap_err()
                .contains("unknown process `gw`")
        );
        assert!(parse_args(&["--exposition".into()]).is_err());
        assert_eq!(
            parse_args(&[
                "--check".into(),
                "--strict".into(),
                "--exposition".into(),
                "maintenance=m.prom".into()
            ]),
            Ok(Opts {
                check: true,
                strict: true,
                expositions: vec![("maintenance".into(), PathBuf::from("m.prom"))],
            })
        );
    }

    /// T-H9: a scraped family with zero samples, or a family set that differs from the
    /// process's `--metrics-families`, fails EX.
    #[test]
    fn t_h9_exposition_zero_sample_family_fails() {
        let registry = real_registry();
        let full = gateway_exposition_without("");
        let families_of = BTreeMap::from([("gateway".to_string(), Ok::<_, String>(full.clone()))]);
        let file = |text: String| vec![("gateway".to_string(), "gw.prom".to_string(), Ok(text))];
        let green = check_expositions(&registry, &file(full.clone()), &families_of);
        assert_eq!(green.status, GateStatus::Pass, "{}", green.detail);

        let zero = full.replace("degrade_total{code=\"ProjectionLag\"} 0\n", "");
        let red = check_expositions(&registry, &file(zero), &families_of);
        assert_eq!(red.status, GateStatus::Fail);
        assert!(
            red.detail.contains("`degrade_total` has zero samples"),
            "{}",
            red.detail
        );

        let short = gateway_exposition_without("jobs_dead");
        let short = short.replace("# HELP degrade_total h\n# TYPE degrade_total counter\ndegrade_total{code=\"ProjectionLag\"} 0\n", "");
        let red = check_expositions(&registry, &file(short), &families_of);
        assert!(
            red.detail.contains("missing [\"degrade_total\"]"),
            "{}",
            red.detail
        );
    }

    // -- unit-level parser tests --------------------------------------------

    #[test]
    fn extract_emit_count_handles_ge_prefix_and_stray_digits() {
        assert_eq!(extract_emit_count("§7.5 入口 B/C · 2"), Some(2));
        assert_eq!(
            extract_emit_count("§31 周期采样，四个 `.set()` · 各 1"),
            Some(1)
        );
        assert_eq!(
            extract_emit_count(
                "· 1；扫 ... 的行数（现算，不落列 —— 落列即违反 §37.2 那道 12 列闸）"
            ),
            Some(1)
        );
    }

    #[test]
    fn backtick_contents_splits_multi_family_cell() {
        let cell = "`evidence_highwater` / `knowledge_highwater`";
        assert_eq!(
            backtick_contents(cell),
            vec!["evidence_highwater", "knowledge_highwater"]
        );
    }

    #[test]
    fn strict_mode_turns_na_into_real_failure() {
        let (spec_path, crates_root) = write_fixture();
        let witness_path = crates_root
            .join(WITNESS_SUBDIR)
            .join("humaux_mcp_requests_total.rs");
        fs::remove_file(&witness_path).unwrap();
        let checks = run_fixture(&spec_path, &crates_root);
        // code still emits this family, so this is already a real `Fail` with or
        // without `--strict` — `report()`'s strict-vs-default branch on
        // `GateStatus::NotApplicable` is covered separately by
        // `real_repo_r_sentinel_and_d3_pass_others_not_applicable_default_mode` below.
        assert!(checks.iter().any(|c| c.status == GateStatus::Fail));
        let exit_strict = report(&checks, true);
        assert_eq!(exit_strict, 1);
        fs::remove_dir_all(spec_path.parent().unwrap()).ok();
    }
}
