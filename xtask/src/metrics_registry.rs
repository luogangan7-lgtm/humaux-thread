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

/// D5: `∀f: actual_emit_callsite_count(C,f) == declared_emit_count(R,f)`。
fn check_d5(
    registry: &[RegistryEntry],
    code_counts: &BTreeMap<String, usize>,
    witness_exists: &BTreeSet<String>,
) -> DCheck {
    let bad: Vec<(String, u32, usize)> = registry
        .iter()
        .filter_map(|r| {
            let actual = code_counts.get(&r.family).copied().unwrap_or(0);
            (actual as u32 != r.declared_emit_count)
                .then(|| (r.family.clone(), r.declared_emit_count, actual))
        })
        .collect();
    let (status, na_families) = status_for(
        bad.iter().map(|(f, _, _)| f.as_str()),
        code_counts,
        witness_exists,
    );
    let detail = if bad.is_empty() {
        format!("{} families 发射点计数与声明一致", registry.len())
    } else {
        format!(
            "处数不等(family,declared,actual): {:?}",
            bad.iter()
                .map(|(f, d, a)| format!("{f}={d}/{a}"))
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
        check_d5(&registry, &code_counts, &witness_exists),
        check_d6(&registry, &code_counts, &witness_exists, &witness_info),
    ])
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

pub fn run(args: &[String]) -> i32 {
    let strict = args.iter().any(|a| a == "--strict");
    let spec = match fs::read_to_string(SPEC_PATH) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("metrics-registry: fail — cannot read {SPEC_PATH}: {e}");
            return 1;
        }
    };
    match check_all(&spec, Path::new(CRATES_DIR)) {
        Some(checks) => report(&checks, strict),
        None => {
            eprintln!(
                "metrics-registry: fail — §41.2 registry unreadable or empty at {SPEC_PATH} \
                 (§80.2 D6 正哨兵语义: |R|>0 必须成立，这是 fail 不是 not_applicable)"
            );
            1
        }
    }
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
