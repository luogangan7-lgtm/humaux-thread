//! xtask `contract-impact` — G80-42 `contract-impact-check` (§80.3).
//!
//! 不重新定义各业务判据；只保证「承重契约改动后，对应 checker 被一起执行」（§80.3 开篇：
//! 「修一处不能再长十三处」）。canonical block id 集合与 block -> mandatory checker 的
//! 唯一依赖映射都从活体 spec 的 §80.3 两个围栏解析（不在本文件另开硬编码真源）——家章明天
//! 改了映射，这里下一次跑就自动跟着变，不会静默过期（§80.1 冻结的核心诉求）。
//!
//! Phase 0 must-pass（line ~9838：`G80-42` 在 Phase 0 起必过列表内），spec/ci.yml 读不到是
//! 环境/checkout 故障 => `fail`，不是 `not_applicable`（同 config_check.rs 对 G80-41 的判例）。

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::process::Command;

/// spec 唯一真源；cwd 假定为 repo root（`cargo xtask` 惯例，同 config_check.rs），
/// 这样才能与 `git diff --name-only` 输出的仓库相对路径直接比较。
const SPEC_PATH: &str = "docs/architecture/Baseline_2.8.md";
const CI_WORKFLOW_PATH: &str = ".github/workflows/ci.yml";
const FEATURES_TOML_PATH: &str = "config/features.toml";
const ROOT_CARGO_TOML_PATH: &str = "Cargo.toml";

/// §80.3「机器识别以下 7 个承重块」正文前的锚点，定位其后紧跟的 ```text 围栏。
const BLOCK_LIST_ANCHOR: &str = "机器识别以下 7 个承重块：";
const GENERIC_FENCE_OPEN: &str = "```text";
const IMPACT_MAP_FENCE_OPEN: &str = "```contract-impact-map";
const FENCE_CLOSE_LINE: &str = "\n```";

/// 正哨兵的判定只有 pass/fail（被测对象——spec 自身两个围栏——恒存在，§57.1 第2条的
/// `not_applicable` 例外不适用；三态语义的 `not_applicable` 分支在下面 [`CheckerStatus`] 里）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GateStatus {
    Pass,
    Fail,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GateResult {
    label: String,
    status: GateStatus,
    detail: String,
}

/// §80.3「Canonical Contract Blocks」围栏一行：block id + 其 §引用（引用只作展示，不参与判定）。
#[derive(Debug, Clone, PartialEq, Eq)]
struct CanonicalBlock {
    id: String,
}

/// §80.3 `contract-impact-map` 围栏一行：block id -> 该 block 变更时的 mandatory checker 集合。
#[derive(Debug, Clone, PartialEq, Eq)]
struct ImpactMapRow {
    block_id: String,
    checkers: Vec<String>,
}

/// 从 `from_byte` 起在 `text` 中找 `fence_open` 这一行，返回围栏正文（不含围栏标记行）。
/// `mechanism_registry.rs::extract_fences` 只扫全文单一围栏名；本函数多一个起始偏移量，
/// 供「先定位锚点文字、再取其后第一个围栏」的用法（§80.3 的 block 列表围栏没有专属围栏名，
/// 只能靠前面那句锚点文字定位到具体是哪一个 ```text）。
fn extract_fence_from<'a>(
    text: &'a str,
    from_byte: usize,
    fence_open: &str,
) -> Option<Vec<&'a str>> {
    let slice = text.get(from_byte..)?;
    let open_at = slice.find(fence_open)?;
    let after_open = &slice[open_at + fence_open.len()..];
    let line_end = after_open.find('\n')?;
    let body_start = &after_open[line_end + 1..];
    let close_at = body_start.find(FENCE_CLOSE_LINE)?;
    Some(body_start[..close_at].lines().collect())
}

/// 解析 §80.3「Canonical Contract Blocks」围栏 -> 实际扫到的 block id 集合
/// （正哨兵左手边：`actual_canonical_block_ids`）。锚点/围栏找不到 => 空集，
/// 这正是 G80-42 注错 D（matcher 写坏）要红的情形，不必再写一条 special-case。
fn parse_canonical_blocks(text: &str) -> Vec<CanonicalBlock> {
    let Some(anchor_pos) = text.find(BLOCK_LIST_ANCHOR) else {
        return Vec::new();
    };
    let Some(body) = extract_fence_from(text, anchor_pos, GENERIC_FENCE_OPEN) else {
        return Vec::new();
    };
    body.iter()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .filter_map(|line| {
            Some(CanonicalBlock {
                id: line.split_whitespace().next()?.to_string(),
            })
        })
        .collect()
}

/// 解析 §80.3 `contract-impact-map` 围栏 -> 每个 block 的 mandatory checker 集合
/// （正哨兵右手边：`impact-map 第一列`，同时也是 §3 步骤「计算 mandatory checker set」的数据源）。
fn parse_impact_map(text: &str) -> Vec<ImpactMapRow> {
    let Some(body) = extract_fence_from(text, 0, IMPACT_MAP_FENCE_OPEN) else {
        return Vec::new();
    };
    body.iter()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .filter_map(|line| {
            let (id, checkers) = line.split_once('|')?;
            let checkers = checkers
                .split(',')
                .map(str::trim)
                .filter(|c| !c.is_empty())
                .map(str::to_string)
                .collect();
            Some(ImpactMapRow {
                block_id: id.trim().to_string(),
                checkers,
            })
        })
        .collect()
}

/// §80.3「映射本身不能偷偷漏新 block」正哨兵：`actual_canonical_block_ids == impact-map 第一列`，
/// 且恰好 7 个（注错 C 多一个、注错 D 变空集，两者都在这里被拦）。
fn check_positive_sentinel(blocks: &[CanonicalBlock], map: &[ImpactMapRow]) -> GateResult {
    let actual: BTreeSet<&str> = blocks.iter().map(|b| b.id.as_str()).collect();
    let mapped: BTreeSet<&str> = map.iter().map(|r| r.block_id.as_str()).collect();
    let label = "block-sentinel".to_string();
    if actual.len() != 7 {
        return GateResult {
            label,
            status: GateStatus::Fail,
            detail: format!(
                "actual canonical block ids = {} 个（期望恰 7 个）: {actual:?}",
                actual.len()
            ),
        };
    }
    if actual != mapped {
        let extra: Vec<&&str> = actual.difference(&mapped).collect();
        let missing: Vec<&&str> = mapped.difference(&actual).collect();
        return GateResult {
            label,
            status: GateStatus::Fail,
            detail: format!(
                "actual block ids 与 impact-map 第一列不等；多出={extra:?}；缺失={missing:?}"
            ),
        };
    }
    GateResult {
        label,
        status: GateStatus::Pass,
        detail: format!("{} 个 canonical block 与 impact-map 一一对应", actual.len()),
    }
}

/// 一个 canonical block 在仓库/spec 中的定位方式（§80.3 declaring row 的具体落点，任务卡
/// 逐条列明：MECHANISM_SPEC=§1.14 围栏 / DB_ROLE_MATRIX=§6.2 / MCP_TOOL_CATALOG=§33.1 /
/// METRIC_REGISTRY=§41.2 / FEATURE_REGISTRY=config/features.toml / WORKSPACE_LAYOUT=§58 树 +
/// 根 Cargo.toml members / GATE_REGISTRY=§80.1 表）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Location {
    /// spec 文件内 1-indexed 闭区间行号（每次运行按当前 spec 文本重新定位标题/围栏，
    /// 不写死行号——行号会随 spec 编辑漂移，标题文字更稳）。
    SpecRange(usize, usize),
    /// 整个外部文件即该 block 本体（block 变更 == 该文件出现在 git diff 里）。
    WholeFile(&'static str),
}

/// 定位 `heading` 这一行开始、到下一条满足 `is_boundary` 的行之前结束的 1-indexed 闭区间。
fn section_range(
    text: &str,
    heading: &str,
    is_boundary: impl Fn(&str) -> bool,
) -> Option<(usize, usize)> {
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.iter().position(|l| *l == heading)?;
    let end = lines[start + 1..]
        .iter()
        .position(|l| is_boundary(l))
        .map_or(lines.len(), |i| start + 1 + i);
    Some((start + 1, end))
}

/// 定位以 `fence_open` 开头、到下一条 ``` 之前结束的围栏，含围栏标记行本身（1-indexed 闭区间）。
fn fence_line_range(text: &str, fence_open: &str) -> Option<(usize, usize)> {
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.iter().position(|l| l.trim() == fence_open)?;
    let end = lines[start + 1..].iter().position(|l| l.trim() == "```")?;
    Some((start + 1, start + 1 + end + 1))
}

/// 对当前 spec 文本重新计算全部 7 个 block 的定位（§80.3 declaring row，任务卡列明）。
/// 找不到锚点的 block 定位为空 Vec —— 在正哨兵之外再补一层：无法定位就永远判不出「变了」，
/// 是比误报更安全的默认值；真正的漂移由 `real_spec_all_blocks_resolve` 测试兜底。
fn resolve_block_locations(spec_text: &str) -> BTreeMap<&'static str, Vec<Location>> {
    let mut m: BTreeMap<&'static str, Vec<Location>> = BTreeMap::new();
    m.insert(
        "MECHANISM_SPEC",
        fence_line_range(spec_text, "```mechanism-registry")
            .map_or(vec![], |(s, e)| vec![Location::SpecRange(s, e)]),
    );
    m.insert(
        "DB_ROLE_MATRIX",
        section_range(spec_text, "## 6.2 Database Role 隔离", |l| {
            l.starts_with("## ")
        })
        .map_or(vec![], |(s, e)| vec![Location::SpecRange(s, e)]),
    );
    m.insert(
        "MCP_TOOL_CATALOG",
        section_range(spec_text, "## 33.1 Canonical Tool Contract", |l| {
            l.starts_with("## ")
        })
        .map_or(vec![], |(s, e)| vec![Location::SpecRange(s, e)]),
    );
    m.insert(
        "METRIC_REGISTRY",
        section_range(spec_text, "## 41.2 注册表（全集）", |l| {
            l.starts_with("## ")
        })
        .map_or(vec![], |(s, e)| vec![Location::SpecRange(s, e)]),
    );
    m.insert(
        "FEATURE_REGISTRY",
        vec![Location::WholeFile(FEATURES_TOML_PATH)],
    );
    {
        let mut v = vec![Location::WholeFile(ROOT_CARGO_TOML_PATH)];
        if let Some((s, e)) = section_range(spec_text, "# 58. 推荐 Rust Workspace", |l| {
            l.starts_with("# ")
        }) {
            v.push(Location::SpecRange(s, e));
        }
        m.insert("WORKSPACE_LAYOUT", v);
    }
    m.insert(
        "GATE_REGISTRY",
        section_range(
            spec_text,
            "## 80.1 架构闸登记表（本章冻结）",
            |l| l.starts_with("## "),
        )
        .map_or(vec![], |(s, e)| vec![Location::SpecRange(s, e)]),
    );
    m
}

/// 一个 block 是否被本次 diff 触碰：WholeFile 看文件名是否在改动文件列表里；
/// SpecRange 看改动行号是否落进该 block 当前的行区间（§80.3 步骤 1「识别哪些 canonical
/// block changed」的行域判定）。
fn block_changed(
    locations: &[Location],
    changed_files: &BTreeSet<String>,
    changed_spec_lines: &BTreeSet<usize>,
) -> bool {
    locations.iter().any(|loc| match loc {
        Location::WholeFile(path) => changed_files.contains(*path),
        Location::SpecRange(s, e) => changed_spec_lines.range(*s..=*e).next().is_some(),
    })
}

/// checker 当前的实现状态：已实现 -> 必须在 CI 里被跑到；未实现 -> 结构性 `not_applicable`，
/// 不算这次 PR 的责任（§80.3 步骤 4 的「checker 自己未执行」不适用于本来就不存在的 checker）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CheckerKind {
    /// 载荷是真正跑判据的 `cargo xtask <subcommand>` 名字。
    Implemented(&'static str),
    NotImplemented,
}

/// §80.3 impact-map 里出现的 checker id -> 是否已是真正跑判据的 xtask 子命令（不是永远
/// `not_applicable` 的 Phase 0 占位）。G80-10（mechanism-registry）/ G80-41（config-check）各
/// 自有独立子命令；**G80-3 没有独立子命令，判据本体活在 `architecture-check` 内部**
/// （`g80_3_outbound_choke_point`，§83.4）——之前这里按 `NotImplemented` 处理，导致
/// contract-impact 对 WORKSPACE_LAYOUT 块的闭合永远报「checker G80-3 not implemented yet」，
/// 即便 G80-3 本身早已实现且每次 CI 都真的跑了（ci.yml 的 `cargo xtask architecture-check`
/// 步骤，见 [`executed_xtask_subcommands`]）。映射到 `architecture-check` 这个真实存在、真的
/// 无条件跑 G80-3 的子命令，比新增一个 `architecture-check --only <id>` 子选项更省——后者要求
/// architecture-check 自己先长出按 checker id 过滤的能力，而 contract-impact 现在只需要知道
/// 「G80-3 跑了没有」，不需要单独跑它。其余（含任务卡点名的 mcp-contract-lock，Phase 15 交付）
/// 仍按 `NotImplemented` 处理——各自任务卡交付时在此加一行即可，不改调用方逻辑。
fn default_checker_kind(id: &str) -> CheckerKind {
    match id {
        "G80-3" => CheckerKind::Implemented("architecture-check"),
        "G80-10" => CheckerKind::Implemented("mechanism-registry"),
        "G80-41" => CheckerKind::Implemented("config-check"),
        _ => CheckerKind::NotImplemented,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CheckerStatus {
    Pass,
    Fail(String),
    NotApplicable(String),
}

/// §80.3 步骤 3/4：changed block 的 mandatory checker 若已实现却没在本次 CI 清单里执行 -> fail；
/// 若本体尚未实现 -> not_applicable 并点名（checker id 本身就是缺失对象名）。
fn evaluate_checker(
    id: &str,
    executed: &BTreeSet<String>,
    kind_fn: impl Fn(&str) -> CheckerKind,
) -> CheckerStatus {
    match kind_fn(id) {
        CheckerKind::Implemented(sub) => {
            if executed.contains(sub) {
                CheckerStatus::Pass
            } else {
                CheckerStatus::Fail(format!(
                    "mandatory checker {id}（`cargo xtask {sub}`）missing from CI job manifest（{CI_WORKFLOW_PATH}）"
                ))
            }
        }
        CheckerKind::NotImplemented => CheckerStatus::NotApplicable(format!(
            "missing object: checker {id} not implemented yet（no xtask subcommand delivered）"
        )),
    }
}

/// `git diff --name-only <base>` -> 改动文件的仓库相对路径集合。
fn git_changed_files(base: &str) -> Result<BTreeSet<String>, String> {
    let out = Command::new("git")
        .args(["diff", "--name-only", base])
        .output()
        .map_err(|e| format!("git diff --name-only 启动失败: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "git diff --name-only 失败: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect())
}

/// `git diff --unified=0 <base> -- <file>` -> 该文件在「新版」坐标系下被改动触及的行号集合。
/// hunk 头 `@@ -a,b +c,d @@` 取 `+c,d`：`d==0`（纯删除）记 `c`（插入点附近），否则记 `c..c+d-1`。
fn git_changed_lines(base: &str, file: &str) -> Result<BTreeSet<usize>, String> {
    let out = Command::new("git")
        .args(["diff", "--unified=0", base, "--", file])
        .output()
        .map_err(|e| format!("git diff -U0 启动失败: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "git diff -U0 失败: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(parse_unified_hunks(&String::from_utf8_lossy(&out.stdout)))
}

fn parse_unified_hunks(diff_text: &str) -> BTreeSet<usize> {
    let mut lines_touched = BTreeSet::new();
    for line in diff_text.lines() {
        let Some(rest) = line.strip_prefix("@@ ") else {
            continue;
        };
        let Some(plus_tok) = rest.split_whitespace().find(|t| t.starts_with('+')) else {
            continue;
        };
        let spec = &plus_tok[1..];
        let mut parts = spec.splitn(2, ',');
        let Some(Ok(start)) = parts.next().map(|s| s.parse::<usize>()) else {
            continue;
        };
        let len: usize = parts.next().and_then(|s| s.parse().ok()).unwrap_or(1);
        if len == 0 {
            lines_touched.insert(start.max(1));
        } else {
            lines_touched.extend(start..start + len);
        }
    }
    lines_touched
}

/// 扫 ci.yml 全文里每处 `cargo xtask <subcommand>` 出现，取出 subcommand 词——不解析 YAML
/// 结构（stdlib 够用，见 CLAUDE.md ladder rung 3），因为本闸只关心「这个子命令有没有被跑」，
/// 不关心它挂在哪个 job/step 名下。
fn executed_xtask_subcommands(ci_text: &str) -> BTreeSet<String> {
    const MARKER: &str = "cargo xtask ";
    let mut out = BTreeSet::new();
    let mut rest = ci_text;
    while let Some(idx) = rest.find(MARKER) {
        let after = &rest[idx + MARKER.len()..];
        let tok: String = after.chars().take_while(|c| !c.is_whitespace()).collect();
        rest = &after[tok.len()..];
        if !tok.is_empty() {
            out.insert(tok);
        }
    }
    out
}

/// §80.3 步骤 1-4 的核心：对每个 impact-map 行判断 block 是否变了，变了就逐 checker 求状态。
/// 纯函数，供 [`run`] 与单测共用（单测直接构造 changed_files/changed_spec_lines，不碰真 git）。
fn evaluate_closure(
    map: &[ImpactMapRow],
    locations: &BTreeMap<&'static str, Vec<Location>>,
    changed_files: &BTreeSet<String>,
    changed_spec_lines: &BTreeSet<usize>,
    executed: &BTreeSet<String>,
    kind_fn: impl Fn(&str) -> CheckerKind,
) -> Vec<(String, String, CheckerStatus)> {
    let mut out = Vec::new();
    for row in map {
        let locs = locations
            .get(row.block_id.as_str())
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if !block_changed(locs, changed_files, changed_spec_lines) {
            continue;
        }
        for checker in &row.checkers {
            out.push((
                row.block_id.clone(),
                checker.clone(),
                evaluate_checker(checker, executed, &kind_fn),
            ));
        }
    }
    out
}

/// §80.1 前言 / §57.1 第2条: verdict for "--base not provided", as a pure function of
/// whether this is an actual CI run. Extracted (rather than inlined at the `std::env::var`
/// call site) so the branching is unit-testable without mutating real process env vars —
/// `run()` supplies the live `CI`/`GITHUB_ACTIONS` lookup, tests inject the bool directly.
///
/// In CI the merge-base is always computable (the checked-out ref has a base commit), so a
/// missing `--base` there is a wiring gap between this checker and its caller, not a
/// legitimate "被测对象尚未交付" not_applicable — silently skipping the closure would leave
/// only the 7-block structural sentinel running in CI, exactly the "静默过期" shape §80.1
/// warns about. Outside CI (a developer running the sentinel-only mode ad hoc) the object
/// really is absent by choice, so not_applicable still applies there.
fn missing_base_verdict(in_ci: bool) -> (bool, String) {
    if in_ci {
        (
            true,
            "contract-impact closure: fail — missing object: --base <merge-base> not provided \
             while running in CI (CI/GITHUB_ACTIONS env detected); merge-base is computable \
             here, so this is a caller wiring gap, not a legitimate not_applicable — pass \
             --base <merge-base sha> from the CI job"
                .to_string(),
        )
    } else {
        (
            false,
            "contract-impact closure: not_applicable — missing object: --base <merge-base> \
             not provided, change-closure skipped"
                .to_string(),
        )
    }
}

fn parse_base_arg(args: &[String]) -> Option<String> {
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if let Some(v) = a.strip_prefix("--base=") {
            return Some(v.to_string());
        }
        if a == "--base" {
            return it.next().cloned();
        }
    }
    None
}

pub fn run(args: &[String]) -> i32 {
    let spec_text = match fs::read_to_string(SPEC_PATH) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("contract-impact: fail — cannot read {SPEC_PATH}: {e}");
            return 1;
        }
    };

    let blocks = parse_canonical_blocks(&spec_text);
    let map = parse_impact_map(&spec_text);

    let sentinel = check_positive_sentinel(&blocks, &map);
    let mut failed = sentinel.status == GateStatus::Fail;
    let tag = |s: GateStatus| match s {
        GateStatus::Pass => "pass",
        GateStatus::Fail => "fail",
    };
    eprintln!(
        "contract-impact {}: {} — {}",
        sentinel.label,
        tag(sentinel.status),
        sentinel.detail
    );

    match parse_base_arg(args) {
        None => {
            let in_ci = std::env::var("CI").is_ok() || std::env::var("GITHUB_ACTIONS").is_ok();
            let (must_fail, msg) = missing_base_verdict(in_ci);
            eprintln!("{msg}");
            failed = failed || must_fail;
        }
        Some(base) => {
            let changed_files = match git_changed_files(&base) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("contract-impact closure: fail — {e}");
                    return 1;
                }
            };
            let changed_spec_lines = if changed_files.contains(SPEC_PATH) {
                match git_changed_lines(&base, SPEC_PATH) {
                    Ok(l) => l,
                    Err(e) => {
                        eprintln!("contract-impact closure: fail — {e}");
                        return 1;
                    }
                }
            } else {
                BTreeSet::new()
            };
            let ci_text = match fs::read_to_string(CI_WORKFLOW_PATH) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!(
                        "contract-impact closure: fail — cannot read {CI_WORKFLOW_PATH}: {e}"
                    );
                    return 1;
                }
            };
            let executed = executed_xtask_subcommands(&ci_text);
            let locations = resolve_block_locations(&spec_text);
            let statuses = evaluate_closure(
                &map,
                &locations,
                &changed_files,
                &changed_spec_lines,
                &executed,
                default_checker_kind,
            );

            if statuses.is_empty() {
                eprintln!(
                    "contract-impact closure: pass — no canonical block changed against {base}"
                );
            }
            for (block_id, checker_id, status) in &statuses {
                match status {
                    CheckerStatus::Pass => {
                        eprintln!("contract-impact closure {block_id}/{checker_id}: pass")
                    }
                    CheckerStatus::Fail(d) => {
                        failed = true;
                        eprintln!("contract-impact closure {block_id}/{checker_id}: fail — {d}");
                    }
                    CheckerStatus::NotApplicable(d) => {
                        eprintln!(
                            "contract-impact closure {block_id}/{checker_id}: not_applicable — {d}"
                        )
                    }
                }
            }
        }
    }

    i32::from(failed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn real_spec() -> String {
        let path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/architecture/Baseline_2.8.md");
        fs::read_to_string(path).expect("spec must be readable in test env")
    }

    fn real_ci_yml() -> String {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../.github/workflows/ci.yml");
        fs::read_to_string(path).expect("ci.yml must be readable in test env")
    }

    /// 最小合规 fixture：镜像真实 §80.3 两个围栏的形状，7 行对 7 行，独立于 15074 行真 spec
    /// （§1.14 冻结的「唯一副本」约束的是生产解析目标，不约束测试 fixture，同
    /// mechanism_registry.rs 的先例）。
    const VALID_FIXTURE: &str = "\
## 1.14 机制的分母元数据契约（新增，P0）

```mechanism-registry
1 | - | NO_MECHANISM | - | - | - | - | -
```

## 6.2 Database Role 隔离

role universe body

## 6.3 Tenant boundary

## 33.1 Canonical Tool Contract

tool list body

## 33.2 boundary

## 41.2 注册表（全集）

metric table body

## 41.3 boundary

## 80.3 contract-impact-check

机器识别以下 7 个承重块：

```text
MECHANISM_SPEC      §1.14  mechanism-registry fence
DB_ROLE_MATRIX      §6.2   role universe
MCP_TOOL_CATALOG    §33.1  canonical 8 tools
METRIC_REGISTRY     §41.2  metric table
FEATURE_REGISTRY    §50.1  config/features.toml contract
WORKSPACE_LAYOUT    §58    workspace tree
GATE_REGISTRY       §80.1  G80 registry
```

```contract-impact-map
MECHANISM_SPEC   | G80-10,G80-17,G80-41
DB_ROLE_MATRIX   | G80-26,G80-40
MCP_TOOL_CATALOG | mcp-contract-lock,mcp-compat-matrix
METRIC_REGISTRY  | G80-6,G80-18
FEATURE_REGISTRY | G80-41
WORKSPACE_LAYOUT | workspace-member-check,G80-3,G80-40,G80-41
GATE_REGISTRY    | G80-23,G80-24,gate-phase-coverage
```
";

    // ---- 解析 + 正哨兵 ----

    #[test]
    fn valid_fixture_sentinel_passes() {
        let blocks = parse_canonical_blocks(VALID_FIXTURE);
        let map = parse_impact_map(VALID_FIXTURE);
        assert_eq!(blocks.len(), 7);
        assert_eq!(map.len(), 7);
        assert_eq!(
            check_positive_sentinel(&blocks, &map).status,
            GateStatus::Pass
        );
    }

    #[test]
    fn real_spec_sentinel_passes_and_all_blocks_resolve() {
        let text = real_spec();
        let blocks = parse_canonical_blocks(&text);
        let map = parse_impact_map(&text);
        let sentinel = check_positive_sentinel(&blocks, &map);
        assert_eq!(sentinel.status, GateStatus::Pass, "{}", sentinel.detail);

        // 兜底：resolve_block_locations 对真 spec 的每个 block 都能定位到至少一处
        // （标题文字漂移会让这里先红，而不是让 block_changed 静默永远判 false）。
        let locations = resolve_block_locations(&text);
        for row in &map {
            let locs = locations.get(row.block_id.as_str());
            assert!(
                locs.is_some_and(|l| !l.is_empty()),
                "block {} 未能在真 spec 中定位",
                row.block_id
            );
        }
    }

    // ---- G80-42 注错 A：修改 MECHANISM_SPEC，CI manifest 删除 G80-10 -> mandatory checker 缺项 ----

    #[test]
    fn fault_a_ci_missing_mechanism_registry_step_is_red_then_green() {
        let map = parse_impact_map(VALID_FIXTURE);
        let locations = resolve_block_locations(VALID_FIXTURE);
        let changed_files: BTreeSet<String> = [SPEC_PATH.to_string()].into();
        // MECHANISM_SPEC 的 fence 行区间内随便一行，模拟「改了 MECHANISM_SPEC」。
        let mechanism_range = match locations["MECHANISM_SPEC"][0] {
            Location::SpecRange(s, _) => s,
            _ => unreachable!(),
        };
        let changed_lines: BTreeSet<usize> = [mechanism_range].into();

        let ci_missing_step =
            "name: CI\njobs:\n  ci:\n    steps:\n      - run: cargo xtask config-check\n";
        let red = evaluate_closure(
            &map,
            &locations,
            &changed_files,
            &changed_lines,
            &executed_xtask_subcommands(ci_missing_step),
            default_checker_kind,
        );
        let g80_10 = red
            .iter()
            .find(|(b, c, _)| b == "MECHANISM_SPEC" && c == "G80-10")
            .unwrap();
        assert!(matches!(&g80_10.2, CheckerStatus::Fail(d) if d.contains("G80-10")));

        let ci_with_step =
            "name: CI\njobs:\n  ci:\n    steps:\n      - run: cargo xtask mechanism-registry\n";
        let green = evaluate_closure(
            &map,
            &locations,
            &changed_files,
            &changed_lines,
            &executed_xtask_subcommands(ci_with_step),
            default_checker_kind,
        );
        let g80_10_green = green
            .iter()
            .find(|(b, c, _)| b == "MECHANISM_SPEC" && c == "G80-10")
            .unwrap();
        assert_eq!(g80_10_green.2, CheckerStatus::Pass);
    }

    // ---- G80-42 注错 B：DB_ROLE_MATRIX 变更触发 G80-26，故意不跑 G80-40 -> 红 ----

    #[test]
    fn fault_b_db_role_matrix_partial_checker_run_is_red_then_green() {
        let map = parse_impact_map(VALID_FIXTURE);
        let locations = resolve_block_locations(VALID_FIXTURE);
        let changed_files: BTreeSet<String> = [SPEC_PATH.to_string()].into();
        let db_range = match locations["DB_ROLE_MATRIX"][0] {
            Location::SpecRange(s, _) => s,
            _ => unreachable!(),
        };
        let changed_lines: BTreeSet<usize> = [db_range].into();

        // 场景内假设 G80-26/G80-40 都已实现（不同于当前仓库真实状态），专测「跑了一个漏一个」。
        let kind_fn = |id: &str| match id {
            "G80-26" => CheckerKind::Implemented("role-grant-check"),
            "G80-40" => CheckerKind::Implemented("db-pool-topology-check"),
            other => default_checker_kind(other),
        };
        let ci_only_g80_26 = "run: cargo xtask role-grant-check\n";
        let red = evaluate_closure(
            &map,
            &locations,
            &changed_files,
            &changed_lines,
            &executed_xtask_subcommands(ci_only_g80_26),
            kind_fn,
        );
        assert!(matches!(
            red.iter()
                .find(|(b, c, _)| b == "DB_ROLE_MATRIX" && c == "G80-40")
                .unwrap()
                .2,
            CheckerStatus::Fail(_)
        ));
        assert_eq!(
            red.iter()
                .find(|(b, c, _)| b == "DB_ROLE_MATRIX" && c == "G80-26")
                .unwrap()
                .2,
            CheckerStatus::Pass
        );

        let ci_both =
            "run: cargo xtask role-grant-check\nrun: cargo xtask db-pool-topology-check\n";
        let green = evaluate_closure(
            &map,
            &locations,
            &changed_files,
            &changed_lines,
            &executed_xtask_subcommands(ci_both),
            kind_fn,
        );
        assert!(green.iter().all(|(_, _, s)| *s == CheckerStatus::Pass));
    }

    // ---- G80-42 注错 C：加第 8 个 block，不加 impact-map 行 -> 正哨兵红 ----

    #[test]
    fn fault_c_eighth_block_without_map_row_is_red_then_green() {
        let green_before = check_positive_sentinel(
            &parse_canonical_blocks(VALID_FIXTURE),
            &parse_impact_map(VALID_FIXTURE),
        );
        assert_eq!(green_before.status, GateStatus::Pass);

        let mutated = VALID_FIXTURE.replace(
            "FEATURE_REGISTRY    §50.1  config/features.toml contract\n",
            "FEATURE_REGISTRY    §50.1  config/features.toml contract\nFEATURE_REGISTRY_V2 §50.1  second registry\n",
        );
        assert_ne!(mutated, VALID_FIXTURE);
        let red = check_positive_sentinel(
            &parse_canonical_blocks(&mutated),
            &parse_impact_map(&mutated),
        );
        assert_eq!(red.status, GateStatus::Fail);
        assert!(red.detail.contains("FEATURE_REGISTRY_V2"));

        let green_after = check_positive_sentinel(
            &parse_canonical_blocks(VALID_FIXTURE),
            &parse_impact_map(VALID_FIXTURE),
        );
        assert_eq!(green_after.status, GateStatus::Pass);
    }

    // ---- G80-42 注错 D：impact-map matcher 写坏，actual block ids = ∅ -> 正哨兵要求恰 7 个 -> 红 ----

    #[test]
    fn fault_d_broken_block_list_anchor_yields_empty_set_is_red_then_green() {
        let mutated =
            VALID_FIXTURE.replace(BLOCK_LIST_ANCHOR, "该锚点文字被写坏，matcher 再也找不到");
        assert_ne!(mutated, VALID_FIXTURE);
        let blocks = parse_canonical_blocks(&mutated);
        assert!(blocks.is_empty());
        let red = check_positive_sentinel(&blocks, &parse_impact_map(&mutated));
        assert_eq!(red.status, GateStatus::Fail);
        assert!(red.detail.contains("0 个"));

        let green = check_positive_sentinel(
            &parse_canonical_blocks(VALID_FIXTURE),
            &parse_impact_map(VALID_FIXTURE),
        );
        assert_eq!(green.status, GateStatus::Pass);
    }

    // ---- 支撑逻辑的单元覆盖 ----

    #[test]
    fn checker_not_implemented_row_still_reported_as_not_applicable_not_dropped() {
        let map = parse_impact_map(VALID_FIXTURE);
        let locations = resolve_block_locations(VALID_FIXTURE);
        let changed_files: BTreeSet<String> = [SPEC_PATH.to_string()].into();
        let mcp_range = match locations["MCP_TOOL_CATALOG"][0] {
            Location::SpecRange(s, _) => s,
            _ => unreachable!(),
        };
        let statuses = evaluate_closure(
            &map,
            &locations,
            &changed_files,
            &[mcp_range].into(),
            &BTreeSet::new(),
            default_checker_kind,
        );
        let mcp_lock = statuses
            .iter()
            .find(|(b, c, _)| b == "MCP_TOOL_CATALOG" && c == "mcp-contract-lock")
            .unwrap();
        assert!(
            matches!(&mcp_lock.2, CheckerStatus::NotApplicable(d) if d.contains("mcp-contract-lock"))
        );
    }

    #[test]
    fn whole_file_block_changed_by_filename_membership() {
        let locations = resolve_block_locations(VALID_FIXTURE);
        assert!(block_changed(
            &locations["FEATURE_REGISTRY"],
            &[FEATURES_TOML_PATH.to_string()].into(),
            &BTreeSet::new()
        ));
        assert!(!block_changed(
            &locations["FEATURE_REGISTRY"],
            &BTreeSet::new(),
            &BTreeSet::new()
        ));
    }

    #[test]
    fn parse_unified_hunks_reads_plus_side_range() {
        let diff = "@@ -10,2 +12,3 @@ context\n+a\n+b\n+c\n";
        assert_eq!(parse_unified_hunks(diff), [12usize, 13, 14].into());
    }

    #[test]
    fn parse_unified_hunks_pure_deletion_marks_insertion_point() {
        let diff = "@@ -5,2 +4,0 @@ context\n-a\n-b\n";
        assert_eq!(parse_unified_hunks(diff), [4usize].into());
    }

    #[test]
    fn executed_xtask_subcommands_scans_regardless_of_yaml_shape() {
        let text = "steps:\n  - name: x\n    run: cargo xtask mechanism-registry\n  - run: |\n      cargo xtask config-check\n";
        let found = executed_xtask_subcommands(text);
        assert!(found.contains("mechanism-registry"));
        assert!(found.contains("config-check"));
    }

    #[test]
    fn real_ci_yml_executes_currently_implemented_checkers() {
        // §80.1: G80-10 = mechanism-registry-check, G80-41 = config-check（都已实现）——
        // real ci.yml 里必须真的执行这两个子命令，否则本闸自己在真仓库上就该红。
        let executed = executed_xtask_subcommands(&real_ci_yml());
        assert!(executed.contains("mechanism-registry"));
        assert!(executed.contains("config-check"));
    }

    // ---- blocker fix: missing --base in an actual CI run must fail, not silently skip ----

    #[test]
    fn missing_base_in_ci_is_fail_not_silent_skip() {
        let (must_fail, msg) = missing_base_verdict(true);
        assert!(must_fail, "CI run without --base must fail the gate");
        assert!(msg.starts_with("contract-impact closure: fail"));
        assert!(msg.contains("wiring gap"));
    }

    #[test]
    fn missing_base_outside_ci_stays_not_applicable() {
        let (must_fail, msg) = missing_base_verdict(false);
        assert!(!must_fail, "local ad-hoc sentinel-only run must not fail");
        assert!(msg.starts_with("contract-impact closure: not_applicable"));
    }

    /// Positive control, inverse of the fixed wiring gap: real `ci.yml` now invokes
    /// `cargo xtask contract-impact` with `--base ${{ github.event.pull_request.base.sha }}`
    /// (see `.github/workflows/ci.yml`'s `cargo xtask contract-impact` step) — the closure
    /// this module computes therefore actually runs in CI instead of silently reporting
    /// `not_applicable` on every PR (the exact gap `missing_base_verdict(true)` above pins the
    /// failing shape of).
    #[test]
    fn real_ci_yml_contract_impact_step_has_base_flag() {
        let ci_text = real_ci_yml();
        // The step's `name:` line also contains the literal `cargo xtask contract-impact`
        // text (as the step's display name) — split on the `run:` line specifically so this
        // does not match that unrelated first occurrence.
        let step = ci_text
            .split("run: cargo xtask contract-impact")
            .nth(1)
            .map(|rest| rest.lines().next().unwrap_or(""))
            .unwrap_or("");
        assert!(
            step.contains("--base"),
            "ci.yml's contract-impact step lost its --base flag — the closure this module \
             computes would go back to never running in CI"
        );
    }

    #[test]
    fn base_arg_parsing_supports_space_and_equals_forms() {
        assert_eq!(
            parse_base_arg(&["--base".into(), "abc123".into()]),
            Some("abc123".into())
        );
        assert_eq!(
            parse_base_arg(&["--base=abc123".into()]),
            Some("abc123".into())
        );
        assert_eq!(parse_base_arg(&["--other".into()]), None);
    }
}
