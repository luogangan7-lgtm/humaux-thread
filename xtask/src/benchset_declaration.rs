//! xtask `benchset-declaration` — G80-16 benchset-declaration-check.
//! 判据以 §69「Benchmark 集合分母声明」节为唯一真源：表恰 10 行、
//! `set_id` 与 §55 集合清单逐名对齐、「判定」格禁现 legacy `NO_DOD_ITEM`、§55.3 七字段
//! （spec 行 9279–9291：`set_id`/`fixed_denominator`/`decision_depth`/`resolution`/
//! `spread_tol`/`measured_at`/`frozen_by`）缺任一 ⇒ `NOT_DECLARED`、owning phase 到期仍
//! `NOT_DECLARED` ⇒ 红。本文件不复述判据散文，只引用 § 号（仓库硬边界）。

use std::fs;
use std::path::Path;

const SPEC_PATH: &str = "docs/architecture/Baseline_2.9.md";
/// §69 声明表标题；用它动态定位表格起点而不是硬编码行号，spec 改版漂移时自动跟随。
const TABLE_HEADING: &str = "## Benchmark 集合分母声明";
/// §55 顶层章节标题；其后第一个 ```text 围栏是集合清单（spec 行 9231–9241）。
const CH55_HEADING_PREFIX: &str = "# 55.";
const REQUIRED_ROW_COUNT: usize = 10;
const LEGACY_MARKER: &str = "NO_DOD_ITEM";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation(pub String);

// ---------- §55 集合清单 ----------

fn normalize_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// 抽取 §55 顶层章节标题后第一个 ```text 围栏的 10 行集合清单，逐行折叠内部空白后返回，
/// 供与声明表「§55 名称」列比对（判据：`set_id` 与 §55 集合清单逐名对齐）。
pub fn parse_ch55_set_list(spec_md: &str) -> Vec<String> {
    let mut in_ch55 = false;
    let mut in_fence = false;
    let mut out = Vec::new();
    for line in spec_md.lines() {
        if line.starts_with(CH55_HEADING_PREFIX) {
            in_ch55 = true;
            continue;
        }
        if in_ch55 && !in_fence && line.starts_with("# ") {
            break; // 下一个顶层章节，§55 结束
        }
        if in_ch55 && !in_fence && line.trim() == "```text" {
            in_fence = true;
            continue;
        }
        if in_fence {
            if line.trim() == "```" {
                break;
            }
            out.push(normalize_ws(line));
        }
    }
    out
}

// ---------- §69 声明表 ----------

fn extract_table_section(spec_md: &str) -> Option<&str> {
    let start = spec_md.find(TABLE_HEADING)?;
    let rest = &spec_md[start..];
    let end = rest[TABLE_HEADING.len()..]
        .find("\n## ")
        .map(|i| i + TABLE_HEADING.len())
        .unwrap_or(rest.len());
    Some(&rest[..end])
}

fn strip_backticks(s: &str) -> String {
    s.trim_matches('`').to_string()
}

/// 声明表数据行：每行 7 列（`set_id` / §55 名称 / `fixed_denominator` / `decision_depth` /
/// `resolution`+`spread_tol` 合并列 / `measured_at`+`frozen_by` 合并列 / 判定）。数据行判据：
/// 首格是反引号包裹的 `set_id`（表头/分隔行都不是，天然被过滤——与 metrics_registry.rs 同一手法）。
pub fn parse_declaration_table(spec_md: &str) -> Vec<Vec<String>> {
    let Some(section) = extract_table_section(spec_md) else {
        return Vec::new();
    };
    section
        .lines()
        .filter(|l| l.trim_start().starts_with("| `"))
        .map(|l| {
            l.trim()
                .trim_matches('|')
                .split('|')
                .map(|c| c.trim().to_string())
                .collect()
        })
        .collect()
}

/// 比较用规范键：去掉全部空白。§55 清单用连续空格做列对齐排版，声明表单元格里同一处
/// 全角括号前完全不留空格——两处原文的空白宽度不是语义的一部分，逐名对齐比较必须忽略它，
/// 否则会把纯排版差异误判成名字不对齐。
fn canonical_key(s: &str) -> String {
    s.chars().filter(|c| !c.is_whitespace()).collect()
}

/// 判据：`set_id` 与 §55 集合清单逐名对齐（比较表「§55 名称」列 vs §55 清单，忽略空白后的
/// 无序集合相等）。
pub fn check_name_alignment(
    table_names: &[String],
    ch55_names: &[String],
) -> Result<(), Violation> {
    let mut a: Vec<String> = table_names.iter().map(|s| canonical_key(s)).collect();
    let mut b: Vec<String> = ch55_names.iter().map(|s| canonical_key(s)).collect();
    a.sort();
    b.sort();
    if a == b {
        return Ok(());
    }
    let extra: Vec<&String> = table_names
        .iter()
        .filter(|n| !b.contains(&canonical_key(n)))
        .collect();
    let missing: Vec<&String> = ch55_names
        .iter()
        .filter(|n| !a.contains(&canonical_key(n)))
        .collect();
    if extra.is_empty() && missing.is_empty() {
        // 成员集合相同但排序后的多重集不等 ⇒ 计数不同，即存在重复名称——`contains` 只判成员
        // 资格，点不出这种差异，必须单独点名，否则报了红却指不出对象（§57.1 精神同理）。
        return Err(Violation(format!(
            "set_id 与 §55 集合清单逐名不对齐：两边名称集合相同但存在重复名称（计数不匹配）；\
             表中名称计数={a:?}，§55 清单名称计数={b:?}"
        )));
    }
    Err(Violation(format!(
        "set_id 与 §55 集合清单逐名不对齐：表中多出 {extra:?}，§55 清单缺失 {missing:?}"
    )))
}

/// §55.3 七字段中除 `set_id` 外，其余 6 个在本表的取值位置。
struct FieldCells<'a> {
    fixed_denominator: &'a str,
    decision_depth: &'a str,
    /// `resolution` 与 `spread_tol` 在表里合并成一列（spec 行 11158 表头），取数法不同但
    /// 单元格位置相同，缺失判定共用同一条「含 `未实测`」规则（§69 行 11036 冻结：两者不许
    /// 互相顶替，但表格布局本身就是合并列，本 checker 遵照表结构如实解析）。
    resolution_spread_tol: &'a str,
    measured_at_frozen_by: &'a str,
}

fn field_cells(row: &[String]) -> Option<FieldCells<'_>> {
    Some(FieldCells {
        fixed_denominator: row.get(2)?,
        decision_depth: row.get(3)?,
        resolution_spread_tol: row.get(4)?,
        measured_at_frozen_by: row.get(5)?,
    })
}

/// `fixed_denominator=<N>=<层1 n1 + 层2 n2>`（spec 9279–9291）：必须能解析出一个整数分母，
/// 不是随便一个非空占位符（否则把 `未声明` 换成任意非空垃圾即可判 declared）。
fn valid_fixed_denominator(v: &str) -> bool {
    v.bytes().any(|b| b.is_ascii_digit())
}

/// `decision_depth`：`top_k=<N>` 或字面「精确相等」（spec 9285：`planner_predicate` 用后者）。
fn valid_decision_depth(v: &str) -> bool {
    if v.contains("精确相等") {
        return true;
    }
    match v.find("top_k=") {
        Some(idx) => v[idx + "top_k=".len()..]
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_digit()),
        None => false,
    }
}

/// 一处子字段自陈「未实测/未通过合格取数」的标记词。含任一即视为该字段**未声明**——哪怕
/// 同段还写着一个「X 题」的读数。这堵掉「拿一个全集/占位读数糊弄，却在同段自陈分层未实测」
/// 的假绿（对抗审查 finding 9：continuation 用全集 4 题填 state 层 resolution，同段却写
/// 「分层均未实测」，旧判据只看有没有「题」+数字就放行）。
const UNMEASURED_MARKERS: &[&str] = &["未实测", "未分层", "未通过", "未采信", "待重取", "待填"];

/// 合并列里某一子字段是否带独立的、**未被未实测标记否定的**「题」量纲实测读数。
fn has_dimensioned_reading(s: &str) -> bool {
    if UNMEASURED_MARKERS.iter().any(|m| s.contains(m)) {
        return false;
    }
    s.contains('题') && s.bytes().any(|b| b.is_ascii_digit())
}

/// §69 冻结（spec ~11155）：`resolution` 与 `spread_tol` 合并列必须**分别标明**、各自带独立
/// 实测读数，禁止拿一个数含糊过两个字段。按字面出现的 `spread_tol` 关键字切成两半分别校验；
/// 找不到关键字（如整格只写「未实测」）即两者都算缺失。任一段带未实测自陈（§69:11214
/// 「全集读数未分层，本身即 NOT_DECLARED 一项」）即判该字段缺失，即使段内另有读数。
fn missing_resolution_spread_tol(cell: &str) -> Vec<&'static str> {
    let mut missing = Vec::new();
    match cell.find("spread_tol") {
        Some(idx) => {
            let (resolution_part, spread_part) = cell.split_at(idx);
            if !has_dimensioned_reading(resolution_part) {
                missing.push("resolution");
            }
            if !has_dimensioned_reading(spread_part) {
                missing.push("spread_tol");
            }
        }
        None => {
            missing.push("resolution");
            missing.push("spread_tol");
        }
    }
    missing
}

/// `measured_at=<YYYY-MM-DD>`。
fn valid_date(s: &str) -> bool {
    let b = s.trim().as_bytes();
    b.len() == 10
        && b[0..4].iter().all(u8::is_ascii_digit)
        && b[4] == b'-'
        && b[5..7].iter().all(u8::is_ascii_digit)
        && b[7] == b'-'
        && b[8..10].iter().all(u8::is_ascii_digit)
}

/// `frozen_by=<commit sha>`：hex，短 sha 起步长度 7。
fn valid_sha(s: &str) -> bool {
    let s = s.trim();
    s.len() >= 7 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// 拆 `measured_at / frozen_by` 合并列，各自按其自身形态校验。没有 `/` 分隔（如单独一个 `—`）
/// 时无法把同一段文本同时解成日期和 sha，两个字段一律判未取数——不能像旧逻辑那样把任意
/// 非空占位符广播成「两个字段都存在」。
fn split_measured_frozen(cell: &str) -> (bool, bool) {
    match cell.split_once('/') {
        Some((a, b)) => (valid_date(a), valid_sha(b)),
        None => (false, false),
    }
}

/// §55.3 七字段判据：缺任一 ⇒ 该集合 `NOT_DECLARED`，返回 (是否全部声明, 缺失字段名列表)。
/// 每个字段按 §55.3 自身的模板形态校验（不是黑名单占位符比较），闭合「填个非空垃圾就算已
/// 声明」的漏洞。
pub fn row_declared(row: &[String]) -> (bool, Vec<&'static str>) {
    let mut missing = Vec::new();
    let set_id_ok = row.first().is_some_and(|s| !strip_backticks(s).is_empty());
    if !set_id_ok {
        missing.push("set_id");
    }
    let Some(cells) = field_cells(row) else {
        missing
            .push("fixed_denominator/decision_depth/resolution/spread_tol/measured_at/frozen_by");
        return (false, missing);
    };
    if !valid_fixed_denominator(cells.fixed_denominator) {
        missing.push("fixed_denominator");
    }
    if !valid_decision_depth(cells.decision_depth) {
        missing.push("decision_depth");
    }
    missing.extend(missing_resolution_spread_tol(cells.resolution_spread_tol));
    let (measured_at_ok, frozen_by_ok) = split_measured_frozen(cells.measured_at_frozen_by);
    if !measured_at_ok {
        missing.push("measured_at");
    }
    if !frozen_by_ok {
        missing.push("frozen_by");
    }
    (missing.is_empty(), missing)
}

/// 判定格里 `owner=...（Phase N[+|/M]）` 标注中的最小 phase 数字。
fn parse_owner_phase_from_verdict(cell: &str) -> Option<u32> {
    let idx = cell.find("Phase ")?;
    let rest = &cell[idx + "Phase ".len()..];
    let mut nums = Vec::new();
    let mut cur = String::new();
    for ch in rest.chars() {
        if ch.is_ascii_digit() {
            cur.push(ch);
            continue;
        }
        if !cur.is_empty() {
            nums.push(std::mem::take(&mut cur));
        }
        if ch == '）' || ch == ')' {
            break;
        }
    }
    if !cur.is_empty() {
        nums.push(cur);
    }
    nums.iter().filter_map(|s| s.parse::<u32>().ok()).min()
}

fn parse_dod_phase_tag(line: &str) -> Option<u32> {
    let idx = line.find("[phase=")?;
    let rest = &line[idx + "[phase=".len()..];
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// 判定格没有 `owner=Phase` 标注时的通用回退：从全文 DoD checklist 中找引用该 `set_id` 的
/// 条目，取其中最小的 `[phase=N]`。按 § 号 11173「每行必须声明 owning phase」判定格本该带
/// 标注——这不是只给 `continuation_198_v2` 开的特例，是任何一行缺标注时都生效的通用规则；
/// 目前 spec 现状里唯一命中这条回退的行是 `continuation_198_v2`（对应 DOD-015，spec 行 10882）。
fn dod_owning_phase(spec_md: &str, set_id: &str) -> Option<u32> {
    spec_md
        .lines()
        .filter(|l| l.contains(set_id) && l.contains("[phase="))
        .filter_map(parse_dod_phase_tag)
        .min()
}

fn owning_phase(spec_md: &str, set_id: &str, verdict_cell: &str) -> Option<u32> {
    parse_owner_phase_from_verdict(verdict_cell).or_else(|| dod_owning_phase(spec_md, set_id))
}

fn parse_phase_arg(args: &[String]) -> u32 {
    // 见 dod_check::parse_phase_flag 的同款注释：真源在 `phase::current_phase`，
    // 顺带修掉两处解析器各写各的（这里原先不认 `--phase=N` 形式）。
    crate::phase::current_phase(args)
}

/// 一次完整判定的结果：`violations` 决定退出码，`na_list` 只是尚未到 owning phase 的附注。
pub struct EvalResult {
    pub violations: Vec<Violation>,
    pub na_list: Vec<String>,
}

/// 对已解析的声明表跑 §69 全部判据（恰 9 行 / 逐名对齐 / legacy marker / 七字段 / owning
/// phase）。从 `run()` 抽出以便测试直接驱动 owning-phase 到期分支（无需解析 stdout/stderr）。
/// §55.3.2（ADR-0007）分段声明的 BLOCKED_ON_SUT 面。规则只绑在 **declared** 行上（未声明
/// 行已由 owning-phase 到期红覆盖，双报只是噪声）：
/// - `BLOCKED_ON_SUT` 标注必须带 `unblock_phase=<N>`（缺 ⇒ 红：挡箭牌没有到期日）；
/// - `current_phase >= unblock_phase` ⇒ 红（§55.3.2「自动转 DUE_UNDECLARED」的 CI 面；
///   注错：把 unblock_phase 改小于当前 phase ⇒ 红）；
/// - declared + BLOCKED_ON_SUT ⇒ `evals/<set_id>/blocked_stage_inventory.toml` 必须存在
///   （§55.3.2 防挑软样本条款：被挡段的攻击清单与已声明段语料同一提交冻结；注错：删
///   inventory 而行仍 DECLARED ⇒ 红）。
fn blocked_on_sut_violations(
    set_id: &str,
    verdict_cell: &str,
    declared: bool,
    current_phase: u32,
    manifest_root: Option<&Path>,
) -> Vec<Violation> {
    let mut out = Vec::new();
    if !verdict_cell.contains("BLOCKED_ON_SUT") || !declared {
        return out;
    }
    match int_after(verdict_cell, "unblock_phase=") {
        None => out.push(Violation(format!(
            "{set_id}: BLOCKED_ON_SUT 段未标 unblock_phase=<N>（§55.3.2：挡箭牌必须带到期日）"
        ))),
        Some(unblock) => {
            if i64::from(current_phase) >= unblock {
                out.push(Violation(format!(
                    "{set_id}: BLOCKED_ON_SUT 段的 unblock_phase={unblock} 已到期（当前 phase \
                     {current_phase}）——该段按 §55.3.2 转 DUE_UNDECLARED，行不得再以 scoped \
                     DECLARED 通过"
                )));
            }
        }
    }
    if let Some(root) = manifest_root {
        let inventory = root
            .join("evals")
            .join(set_id)
            .join("blocked_stage_inventory.toml");
        if !inventory.exists() {
            out.push(Violation(format!(
                "{set_id}: scoped DECLARED 但被挡段的冻结清单不存在（{}）——§55.3.2 防挑软\
                 样本条款要求它与已声明段语料同一提交冻结",
                inventory.display()
            )));
        }
    }
    out
}

/// §55.3.1 manifest gate for one `DECLARED` row: the BenchmarkManifest file must exist and
/// carry the frozen field set — "`BenchmarkManifest` 缺 source/version/license/hash 时同样
/// 视为 NOT_DECLARED"，注错「删除 manifest ⇒ 红」以本函数落地（此前 checker 只读 spec 表
/// 行，manifest 整个消失也无感——这正是删注错第一次跑就抓出来的缺口）。
fn manifest_violation(manifest_root: &Path, set_id: &str) -> Option<Violation> {
    let path = manifest_root
        .join("evals")
        .join(set_id)
        .join("manifest.toml");
    let text = match fs::read_to_string(&path) {
        Ok(t) => t,
        Err(_) => {
            return Some(Violation(format!(
                "{set_id}: DECLARED 但 BenchmarkManifest 不存在（{}）——§55.3.1 视为 NOT_DECLARED",
                path.display()
            )));
        }
    };
    const REQUIRED_KEYS: &[&str] = &[
        "set_id",
        "source_kind",
        "source_ref",
        "source_version",
        "license",
        "fixture_sha256",
        "fixed_denominator",
        "decision_depth",
        "measured_at",
        "frozen_by",
    ];
    let missing: Vec<&str> = REQUIRED_KEYS
        .iter()
        .filter(|k| {
            !text.lines().any(|l| {
                let l = l.trim();
                l.starts_with(**k) && l[k.len()..].trim_start().starts_with('=')
            })
        })
        .copied()
        .collect();
    if missing.is_empty() {
        None
    } else {
        Some(Violation(format!(
            "{set_id}: BenchmarkManifest 缺字段 {}（§55.3.1）——视为 NOT_DECLARED",
            missing.join(",")
        )))
    }
}

/// 抽 `key` 标签**同一行内**、标签之后的第一个十进制整数。找不到标签、或标签所在行标签之后
/// 无数字 ⇒ `None`。
///
/// 「同一行」是硬约束（不是注释愿望）：先把 `key` 之后的文本切到该行行尾，再在这一行里找数字。
/// 早先版本用 `rest.find(digit)` 在标签之后**整段**找，标签同行无数字时会静默抓到下一行的
/// 数字（如 md5 / 日期）——正是可达校验被绕过的通道（对抗审查 finding 6）。
fn int_after(haystack: &str, key: &str) -> Option<i64> {
    let idx = haystack.find(key)? + key.len();
    let after = &haystack[idx..];
    let line = &after[..after.find('\n').unwrap_or(after.len())];
    let start = line.find(|c: char| c.is_ascii_digit())?;
    let digits: String = line[start..]
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    digits.parse().ok()
}

/// 定位 §69 Continuation Gate 的 ```text 块（以 `gate_id:      continuation_198_v2` 那行为锚，
/// 向前找最近的 ```text 围栏、向后找闭合 ``` ）。块不存在 ⇒ `None`（本 checker 域外，NA）。
fn extract_continuation_gate_block(spec_md: &str) -> Option<&str> {
    let anchor = spec_md.find("gate_id:")?;
    let fence_open = spec_md[..anchor].rfind("```text")? + "```text".len();
    let fence_close = spec_md[fence_open..].find("```")? + fence_open;
    Some(&spec_md[fence_open..fence_close])
}

/// [`continuation_reachability_status`] 的三态：抓不到合格实测数 / 三数齐备且①②都过 /
/// 三数齐备但某条破。分成三态（而非 `Option<Violation>`）是为了让调用方能区分「gate 块没填」
/// 与「填了且算式过」——这两者在旧的 `Option` 形态里都是 `None`，于是「声明表行判 DECLARED
/// 却把 gate 块改坏成无数字」这条假绿通道无从拦（对抗审查 finding 6）。
#[derive(Debug, Clone, PartialEq, Eq)]
enum ReachabilityStatus {
    /// gate 块不存在，或 `baseline_min` / `spread_tol` 抓不到（仍是「未实测」散文）——
    /// gate 运行时 `cannot_establish`，本身不是 CI 红（除非声明表行同时判 DECLARED，见
    /// [`evaluate`] 的绑定）。
    NotMeasured,
    /// 三数齐备且①②都成立。
    Holds,
    /// 三数齐备但①或②破。
    Violated(String),
}

/// spec:11213/11215 可达条件①②：Continuation Gate 块声明 `baseline_min` / `spread_tol` / `Δ`
/// 后独立重算——不读块里写的「✓」结论（那是作者自陈，正是要防的假绿通道），只抓原始声明
/// 数值重算。resolution 用 §69 冻结的全集上界 4 题。
/// - ① `Δ > max(resolution, spread_tol)`（步长大过噪声）
/// - ② `baseline_min + Δ <= state_denominator` 且 `baseline_min >= Δ`（步长够得着分母两侧）
fn continuation_reachability_status(spec_md: &str) -> ReachabilityStatus {
    const RESOLUTION_UPPER: i64 = 4; // §69 全集上界（分层未实测）
    let Some(block) = extract_continuation_gate_block(spec_md) else {
        return ReachabilityStatus::NotMeasured;
    };
    // 实测锚：baseline_min 行 `实测 baseline_min state = N`、spread_tol 行 `实测 spread_tol = N`。
    // 未填时（散文「首次取数…未采信」/「未通过合格取数」）这两个锚不存在 ⇒ NotMeasured。
    let (Some(delta), Some(state_denom), Some(baseline_min), Some(spread_tol)) = (
        int_after(block, "判定步长"),
        int_after(block, "state "), // denominator 行 `198 = state 178 + ...`
        int_after(block, "实测 baseline_min state ="),
        int_after(block, "实测 spread_tol ="),
    ) else {
        return ReachabilityStatus::NotMeasured;
    };

    if delta <= RESOLUTION_UPPER.max(spread_tol) {
        return ReachabilityStatus::Violated(format!(
            "可达条件① 破——Δ={delta} 未大过 max(resolution={RESOLUTION_UPPER}, \
             spread_tol={spread_tol})；步长被噪声淹没 ⇒ §69 应输出 cannot_establish"
        ));
    }
    if baseline_min + delta > state_denom {
        return ReachabilityStatus::Violated(format!(
            "可达条件② 破——baseline_min({baseline_min}) + Δ({delta}) = {} > state 分母 \
             {state_denom}；PASS 侧越过分母上界，不可达",
            baseline_min + delta
        ));
    }
    if baseline_min < delta {
        return ReachabilityStatus::Violated(format!(
            "可达条件② 破——baseline_min({baseline_min}) < Δ({delta})；FAIL 侧够不着，不可达"
        ));
    }
    ReachabilityStatus::Holds
}

pub fn evaluate(
    spec_md: &str,
    table: &[Vec<String>],
    current_phase: u32,
    manifest_root: Option<&Path>,
) -> EvalResult {
    let mut violations = Vec::new();

    if table.len() != REQUIRED_ROW_COUNT {
        violations.push(Violation(format!(
            "声明表应恰好 {REQUIRED_ROW_COUNT} 行，实际 {} 行",
            table.len()
        )));
    }

    let ch55_names = parse_ch55_set_list(spec_md);
    let table_names: Vec<String> = table
        .iter()
        .filter_map(|r| r.get(1))
        .map(|s| normalize_ws(s))
        .collect();
    if let Err(v) = check_name_alignment(&table_names, &ch55_names) {
        violations.push(v);
    }

    // spec:11215：可达条件①② 与 frozen_by 同处校验（Continuation Gate 块声明数值后独立重算）。
    // Violated（三数齐备但算式破）无条件红；NotMeasured 是否红取决于声明表行是否判 DECLARED，
    // 在行循环里绑定（对抗审查 finding 6：两处独立文本必须绑，否则 DECLARED 行 + 坏块无红）。
    let reachability = continuation_reachability_status(spec_md);
    if let ReachabilityStatus::Violated(msg) = &reachability {
        violations.push(Violation(format!("continuation_198_v2: {msg}")));
    }

    let mut na_list: Vec<String> = Vec::new();
    for row in table {
        let set_id = row.first().map(|s| strip_backticks(s)).unwrap_or_default();
        let verdict_cell = row.get(6).map(String::as_str).unwrap_or("");

        if verdict_cell.contains(LEGACY_MARKER) {
            violations.push(Violation(format!(
                "{set_id}: 判定格出现禁用标记 {LEGACY_MARKER}"
            )));
        }

        // §55.3.2（ADR-0007）：declared 行上的 BLOCKED_ON_SUT 标注三项校验。
        violations.extend(blocked_on_sut_violations(
            &set_id,
            verdict_cell,
            row_declared(row).0,
            current_phase,
            manifest_root,
        ));

        // 绑定两处文本：continuation_198_v2 声明表行判 DECLARED，但 Continuation Gate 块抓不到
        // 合格实测数（NotMeasured）⇒ 红。DECLARED 意味着 baseline_min/spread_tol 已冻结落库，
        // gate 块必须有可被可达校验独立重算的数字；块被改坏/未填而表行仍 DECLARED 正是
        // finding 6 的假绿通道。
        if set_id == "continuation_198_v2"
            && row_declared(row).0
            && reachability == ReachabilityStatus::NotMeasured
        {
            violations.push(Violation(
                "continuation_198_v2: 声明表行判 DECLARED，但 Continuation Gate 块抓不到合格的 \
                 baseline_min/spread_tol 实测数——DECLARED 要求 gate 块带可独立重算可达①②的数值 \
                 (§69)"
                    .to_string(),
            ));
        }

        // owner/conditional-owner 标注检查对**所有**行无条件跑（spec 11173「每行必须声明」是
        // 无条件的）：declared 与否只决定是否再叠加 owning-phase 到期红，不能反过来让已声明的
        // 行跳过标注检查——否则从一个七字段齐全的行删掉 owner 标注本闸完全无感。
        let phase = owning_phase(spec_md, &set_id, verdict_cell);
        let (declared, missing) = row_declared(row);

        match phase {
            None => {
                violations.push(Violation(format!(
                    "{set_id}: 无法确定 owning phase（判定格缺 `owner=Phase` 标注，\
                     DoD checklist 亦无可反解的 `[phase=N]`）"
                )));
            }
            Some(_) if declared => {
                // 七字段齐全，不受 owning phase 到期约束——但 §55.3.1 的 manifest 面还要过。
                if let Some(root) = manifest_root
                    && let Some(v) = manifest_violation(root, &set_id)
                {
                    violations.push(v);
                }
            }
            Some(phase) if current_phase >= phase => {
                violations.push(Violation(format!(
                    "{set_id}: owning phase {phase} 已到期（当前 phase {current_phase}）但仍 \
                     NOT_DECLARED（缺：{}）",
                    missing.join(",")
                )));
            }
            Some(phase) => {
                na_list.push(format!("{set_id} (owner phase={phase})"));
            }
        }
    }

    EvalResult {
        violations,
        na_list,
    }
}

pub fn run(args: &[String]) -> i32 {
    let current_phase = parse_phase_arg(args);
    let spec_md = match fs::read_to_string(SPEC_PATH) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("benchset-declaration: fail — cannot read {SPEC_PATH}: {e}");
            return 1;
        }
    };

    let table = parse_declaration_table(&spec_md);
    if table.is_empty() {
        eprintln!(
            "benchset-declaration: not_applicable (missing object: heading `{TABLE_HEADING}` not found in {SPEC_PATH})"
        );
        return 0;
    }

    let EvalResult {
        violations,
        na_list,
    } = evaluate(&spec_md, &table, current_phase, Some(Path::new(".")));

    // 三态单一状态行（§57.1）：有 violation ⇒ 只 fail（na_list 作附注打印，不当第二个状态）；
    // 无 violation 且 na_list 非空 ⇒ 只 not_applicable 并点名缺失对象；两者皆无 ⇒ pass。
    if !violations.is_empty() {
        for v in &violations {
            eprintln!("benchset-declaration: fail — {}", v.0);
        }
        if !na_list.is_empty() {
            eprintln!(
                "benchset-declaration: note — {} 集合未到 owning phase（当前 phase {current_phase}），本次判定不计入：{}",
                na_list.len(),
                na_list.join(", ")
            );
        }
        1
    } else if !na_list.is_empty() {
        println!(
            "benchset-declaration: not_applicable (missing object: {} 集合未到 owning phase，当前 phase {current_phase}: {})",
            na_list.len(),
            na_list.join(", ")
        );
        0
    } else {
        println!("benchset-declaration: pass");
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn repo_root() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
    }

    fn real_spec_md() -> String {
        let path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/architecture/Baseline_2.9.md");
        fs::read_to_string(path).expect("spec must be readable in test env")
    }

    /// tempdir fixture 副本：真实仓库文件全程不被触碰（硬规则③）。
    fn fixture_copy(content: &str) -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "xtask-benchset-declaration-fixture-{}-{n}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("tempdir create");
        let path = dir.join("Baseline_2.9.md");
        fs::write(&path, content).expect("write fixture");
        fs::read_to_string(&path).expect("read fixture back")
    }

    #[test]
    fn parse_ch55_set_list_has_10_entries() {
        let spec = real_spec_md();
        let names = parse_ch55_set_list(&spec);
        assert_eq!(names.len(), 10, "§55 集合清单应恰好 10 行: {names:?}");
        assert!(names.contains(&"code retrieval set".to_string()));
    }

    #[test]
    fn parse_declaration_table_real_has_10_rows_in_known_order() {
        let spec = real_spec_md();
        let table = parse_declaration_table(&spec);
        assert_eq!(table.len(), REQUIRED_ROW_COUNT);
        let set_ids: Vec<String> = table.iter().map(|r| strip_backticks(&r[0])).collect();
        assert_eq!(
            set_ids,
            vec![
                "continuation_198_v2",
                "longmemeval_style",
                "agent_workflow_outcome",
                "code_retrieval",
                "exact_completeness",
                "project_continuity",
                "public_provenance_revocation",
                "planner_predicate",
                "memory_security_lifecycle",
                "grounding_evolution",
            ]
        );
    }

    /// 真实 spec 现状：9 个集合全部 `NOT_DECLARED`（§55.3 七字段没有任何一行齐全）。
    #[test]
    fn baseline_real_spec_all_9_rows_not_declared() {
        // `planner_predicate` (T6.1, §20.3) is exempt: its row now carries a real 21-item
        // eval-set measurement (evals/planner_predicate/dataset.tsv, crates/retrieval/tests/
        // planner_predicate_eval.rs) and is genuinely `DECLARED` — same "pin expires when the
        // underlying gap is actually closed" pattern the ADR-0001 comment below documents for
        // `memory_security_lifecycle`'s name-alignment pin.
        // `exact_completeness` joined 2026-08-27: 11-item eval set
        // (evals/exact_completeness/dataset.tsv, crates/adapters/tests/
        // exact_completeness_eval.rs) with measured resolution (1) and spread (0) — same
        // expiring-pin pattern as `planner_predicate` below.
        // `continuation_198_v2` was rolled back to NOT_DECLARED (2026-08-27): its state-layer
        // resolution is unmeasured (§69:11214 «全集读数未分层，本身即 NOT_DECLARED 一项») and
        // its baseline never passed a degradation screen — so it is NOT exempt, it must parse
        // as NOT_DECLARED like the other undeclared rows below.
        // `memory_security_lifecycle` joined 2026-08-27 as scoped-DECLARED (ADR-0007
        // §55.3.2): Write/Recall/Repair measured, Action=BLOCKED_ON_SUT(phase=14).
        const DECLARED_EXEMPT: &[&str] = &[
            "planner_predicate",
            "exact_completeness",
            "memory_security_lifecycle",
        ];
        let spec = real_spec_md();
        let table = parse_declaration_table(&spec);
        for row in &table {
            let set_id = strip_backticks(&row[0]);
            let (declared, missing) = row_declared(row);
            if DECLARED_EXEMPT.contains(&set_id.as_str()) {
                // `continue` alone (the pre-fix shape) drops coverage of this row entirely
                // rather than flipping it — the pin then says nothing about the row's actual
                // state, and a regression back to NOT_DECLARED would pass silently. Assert the
                // exempt row is still genuinely `DECLARED` before skipping the below.
                assert!(
                    declared,
                    "{set_id} is DECLARED_EXEMPT but row_declared() now says NOT_DECLARED \
                     (missing={missing:?}) — the pin is stale, update the exemption or fix the \
                     spec row"
                );
                continue;
            }
            assert!(
                !declared,
                "{set_id} 在真实 spec 上应为 NOT_DECLARED，实际判 declared，missing={missing:?}"
            );
        }
    }

    /// 基线钉（ADR-0001 后）：`memory_security_lifecycle` 行的名称失配已在 spec 侧修复
    /// （「§55 名称」列改回 §55 清单原名），逐名对齐在真实 spec 上必须干净。此前这里钉的
    /// 是修复前的红态——spec 一修，钉过期即翻绿断言（历史见 ADR-0001）。
    #[test]
    fn baseline_real_spec_name_alignment_is_clean_after_adr_0001() {
        let spec = real_spec_md();
        let table = parse_declaration_table(&spec);
        let ch55_names = parse_ch55_set_list(&spec);
        let table_names: Vec<String> = table.iter().map(|r| normalize_ws(&r[1])).collect();
        let result = check_name_alignment(&table_names, &ch55_names);
        assert!(result.is_ok(), "unexpected mismatch: {:?}", result.err());
    }

    #[test]
    fn baseline_real_spec_no_legacy_no_dod_item_marker() {
        let spec = real_spec_md();
        let table = parse_declaration_table(&spec);
        for row in &table {
            assert!(!row[6].contains(LEGACY_MARKER));
        }
    }

    /// 当前 Phase 0：所有 owner phase（含 continuation_198_v2 的 DOD-015 phase=7）均 >= 4，
    /// 因此 owning-phase 判据本身不产生任何 fail（不代表整闸 pass ——判据②的逐名对齐仍会红）。
    #[test]
    fn owning_phase_at_phase_0_never_fails_for_any_row() {
        let spec = real_spec_md();
        let table = parse_declaration_table(&spec);
        for row in &table {
            let set_id = strip_backticks(&row[0]);
            let verdict_cell = &row[6];
            let (declared, _) = row_declared(row);
            if declared {
                continue;
            }
            let phase = owning_phase(&spec, &set_id, verdict_cell);
            assert!(
                phase.is_some_and(|p| p >= 4),
                "{set_id} owning phase should resolve to >=4, got {phase:?}"
            );
        }
    }

    #[test]
    fn continuation_owning_phase_resolves_to_dod_015_phase_7() {
        let spec = real_spec_md();
        assert_eq!(dod_owning_phase(&spec, "continuation_198_v2"), Some(7));
    }

    /// 合成的**完整**（已实测）Continuation Gate 块——四个可达锚齐全，供可达校验注错。用合成
    /// 块而非真实 spec：真实 spec 的 continuation 已回退成 NOT_DECLARED、gate 块无实测锚，注错
    /// 无从落在真实文本上。`base` 是一份①②都过的健康块（Δ=5, denom=178, baseline_min=134,
    /// spread_tol=1），各注错在它之上改一处。
    fn healthy_gate_block() -> String {
        "```text\n\
         gate_id:      continuation_198_v2\n\
         denominator:  198 = state 178 + fact 20\n\
         Δ:            判定步长 5 题\n\
         spread_tol:   实测 spread_tol = 1 题\n\
         baseline_min: 实测 baseline_min state = 134 题\n\
         ```"
        .to_string()
    }

    /// 健康块（134/1/Δ=5）：可达①② 均过 ⇒ Holds。
    #[test]
    fn healthy_gate_block_reachability_holds() {
        assert_eq!(
            continuation_reachability_status(&healthy_gate_block()),
            ReachabilityStatus::Holds
        );
    }

    /// 注错（spec:11215）：`Δ` 改成 178 ⇒ 可达条件②破（134+178 > 178）⇒ Violated。
    #[test]
    fn fault_delta_178_breaks_reachability_two() {
        let broken = healthy_gate_block().replace("判定步长 5 题", "判定步长 178 题");
        match continuation_reachability_status(&broken) {
            ReachabilityStatus::Violated(m) => assert!(m.contains("可达条件②"), "got: {m}"),
            other => panic!("Δ=178 must be Violated②, got {other:?}"),
        }
    }

    /// 注错（spec:11215）：`Δ` 改成 1 ⇒ 可达条件①破（1 > max(4,1)=4 假）⇒ Violated。
    #[test]
    fn fault_delta_1_breaks_reachability_one() {
        let broken = healthy_gate_block().replace("判定步长 5 题", "判定步长 1 题");
        match continuation_reachability_status(&broken) {
            ReachabilityStatus::Violated(m) => assert!(m.contains("可达条件①"), "got: {m}"),
            other => panic!("Δ=1 must be Violated①, got {other:?}"),
        }
    }

    /// 注错（对抗审查 finding 7）：②的 FAIL 侧子条件 `baseline_min < Δ` 独立注错——把
    /// baseline_min 降到 3（< Δ=5），同时仍过①(5>max(4,1)=4)与②-PASS(3+5≤178)，只破 FAIL 侧。
    /// 此前只有 Δ=178（②-PASS 侧）与 Δ=1（①）注错，L429 的 `baseline_min < delta` 分支零覆盖，
    /// 删掉它全部单测仍绿——违反 §80.1「没注错红转绿不算存在」。
    #[test]
    fn fault_baseline_below_delta_breaks_reachability_two_fail_side() {
        let broken =
            healthy_gate_block().replace("baseline_min state = 134", "baseline_min state = 3");
        match continuation_reachability_status(&broken) {
            ReachabilityStatus::Violated(m) => {
                assert!(m.contains("可达条件②") && m.contains("FAIL 侧"), "got: {m}");
            }
            other => panic!("baseline_min=3 < Δ=5 must be Violated②-FAIL, got {other:?}"),
        }
    }

    /// baseline 未填（gate 块仍是「未实测」散文，抓不到实测锚）⇒ NotMeasured：gate 运行时
    /// cannot_establish，本身不是 CI 红（除非声明表行同时 DECLARED，见下一测试）。
    #[test]
    fn unmeasured_baseline_is_not_measured() {
        let synthetic = "```text\ngate_id:      continuation_198_v2\n\
             denominator:  198 = state 178 + fact 20\n\
             Δ:            判定步长 5 题\n\
             spread_tol:   至今未实测 ⇒ 块内不写常数\n\
             baseline_min: 取数一次后写死，记 frozen_by=<commit>\n```";
        assert_eq!(
            continuation_reachability_status(synthetic),
            ReachabilityStatus::NotMeasured
        );
    }

    /// 注错（对抗审查 finding 9）：resolution 段同时有「4 题」读数和「分层未实测」自陈 ⇒ 判
    /// resolution 缺失。旧判据只看有没有「题」+数字，会被全集读数糊弄放行（continuation 用全集
    /// 4 题填 state 层 resolution 却同段自陈分层未实测正是此形）。反向：干净分层实测不误伤。
    #[test]
    fn unmeasured_marker_negates_a_dimensioned_reading() {
        let hoodwink = "`resolution`：全集 4 题（实测）· state/fact 分层均未实测；\
                        `spread_tol`：state 1 题、fact 0 题";
        assert!(
            missing_resolution_spread_tol(hoodwink).contains(&"resolution"),
            "全集读数 + 分层未实测自陈必须判 resolution 缺失"
        );
        let clean = "`resolution`：state 层 3 题、fact 层 2 题（实测）；\
                     `spread_tol`：state 1 题、fact 0 题";
        assert!(
            !missing_resolution_spread_tol(clean).contains(&"resolution"),
            "干净分层实测不得误伤"
        );
        assert!(!missing_resolution_spread_tol(clean).contains(&"spread_tol"));
    }

    /// 真实 spec：continuation 已回退成 NOT_DECLARED、gate 块无实测锚 ⇒ NotMeasured，且声明表
    /// 行非 DECLARED ⇒ evaluate 不产生任何 continuation 可达违规。
    #[test]
    fn real_spec_continuation_is_not_measured_and_clean() {
        let spec = real_spec_md();
        assert_eq!(
            continuation_reachability_status(&spec),
            ReachabilityStatus::NotMeasured
        );
        let table = parse_declaration_table(&spec);
        let result = evaluate(&spec, &table, 8, Some(&repo_root()));
        assert!(
            !result
                .violations
                .iter()
                .any(|v| v.0.contains("可达条件") || v.0.contains("Continuation Gate 块抓不到")),
            "NOT_DECLARED + unmeasured block must not red: {:?}",
            result.violations
        );
    }

    /// 注错（对抗审查 finding 6 兜底）：把声明表行改成 DECLARED（七字段填满形态）但 gate 块
    /// 仍无实测锚（NotMeasured）⇒ 两处独立文本被绑定，evaluate 必须红。防「表行 DECLARED +
    /// 坏块」这条假绿通道。
    #[test]
    fn declared_row_with_unmeasured_gate_block_is_red() {
        let spec = real_spec_md();
        let mut table = parse_declaration_table(&spec);
        // 换掉真实 continuation 行为一个七字段齐全（=DECLARED）的形态；gate 块保持真实 spec 的
        // 未实测散文（NotMeasured）。
        for row in &mut table {
            if strip_backticks(&row[0]) == "continuation_198_v2" {
                *row = vec![
                    "`continuation_198_v2`".to_string(),
                    "Humaux 真实 continuation set".to_string(),
                    "198 = state 178 + fact 20".to_string(),
                    "top_k=5".to_string(),
                    "resolution：4 题；spread_tol：1 题".to_string(),
                    "2026-08-27 / e33913d".to_string(),
                    "`DECLARED`（owning phase 由 DoD 反解 = 7）".to_string(),
                ];
            }
        }
        let result = evaluate(&spec, &table, 8, Some(&repo_root()));
        assert!(
            result
                .violations
                .iter()
                .any(|v| v.0.contains("Continuation Gate 块抓不到")),
            "DECLARED row + unmeasured block must red: {:?}",
            result.violations
        );
    }

    /// §55.3.2（ADR-0007）BLOCKED_ON_SUT 三项注错。合成一个 scoped-DECLARED 形态的
    /// memory_security_lifecycle 行（七字段齐全 + 判定格带 BLOCKED_ON_SUT 标注），逐项驱动。
    fn scoped_declared_msl_row(verdict: &str) -> Vec<String> {
        vec![
            "`memory_security_lifecycle`".to_string(),
            "memory security lifecycle set（Write -> Recall -> Action -> Repair）".to_string(),
            "30 = write 12 + recall 10 + repair 8".to_string(),
            "精确相等".to_string(),
            "resolution：1 题；spread_tol：0 题".to_string(),
            "2026-08-27 / abcdef0".to_string(),
            verdict.to_string(),
        ]
    }

    /// 注错①：BLOCKED_ON_SUT 无 unblock_phase ⇒ 红（挡箭牌必须带到期日）。
    #[test]
    fn fault_blocked_on_sut_without_unblock_phase_is_red() {
        let row = scoped_declared_msl_row(
            "`DECLARED`（scoped）· owner=Security/Private Memory（Phase 4+）· Action=BLOCKED_ON_SUT",
        );
        assert!(row_declared(&row).0, "fixture row must parse declared");
        let v = blocked_on_sut_violations("memory_security_lifecycle", &row[6], true, 8, None);
        assert!(
            v.iter().any(|x| x.0.contains("unblock_phase")),
            "missing unblock_phase must red: {v:?}"
        );
    }

    /// 注错②：unblock_phase 已到期 ⇒ 红（把 14 改成 7，当前 phase 8）。反向：14 未到期不红。
    #[test]
    fn fault_expired_unblock_phase_is_red_and_future_is_not() {
        let expired = scoped_declared_msl_row(
            "`DECLARED`（scoped）· Action=BLOCKED_ON_SUT unblock_phase=7 · owner=X（Phase 4+）",
        );
        let v = blocked_on_sut_violations("memory_security_lifecycle", &expired[6], true, 8, None);
        assert!(
            v.iter().any(|x| x.0.contains("已到期")),
            "expired unblock_phase must red: {v:?}"
        );
        let future = scoped_declared_msl_row(
            "`DECLARED`（scoped）· Action=BLOCKED_ON_SUT unblock_phase=14 · owner=X（Phase 4+）",
        );
        let v = blocked_on_sut_violations("memory_security_lifecycle", &future[6], true, 8, None);
        assert!(
            !v.iter().any(|x| x.0.contains("已到期")),
            "future unblock_phase must not red: {v:?}"
        );
    }

    /// 注错③（防挑软样本）：scoped-DECLARED 行但被挡段冻结清单不存在 ⇒ 红。
    /// 未声明行不触发本组校验（owning-phase 到期红已覆盖，双报是噪声）。
    #[test]
    fn fault_missing_blocked_stage_inventory_is_red_only_when_declared() {
        let verdict =
            "`DECLARED`（scoped）· Action=BLOCKED_ON_SUT unblock_phase=14 · owner=X（Phase 4+）";
        let row = scoped_declared_msl_row(verdict);
        // 用一个不存在 inventory 的合成 set_id 驱动「缺失红」——真实 memory_security_lifecycle
        // 的 inventory 已随本 wave 落地，不再是缺失态。
        let v = blocked_on_sut_violations(
            "synthetic_blocked_set_no_inventory",
            &row[6],
            true,
            8,
            Some(&repo_root()),
        );
        assert!(
            v.iter().any(|x| x.0.contains("blocked_stage_inventory")),
            "missing inventory must red on declared row: {v:?}"
        );
        let none = blocked_on_sut_violations(
            "synthetic_blocked_set_no_inventory",
            &row[6],
            false,
            8,
            Some(&repo_root()),
        );
        assert!(
            none.is_empty(),
            "undeclared row must not double-report: {none:?}"
        );
    }

    /// 窄度正对照：真实 spec 的 memory_security_lifecycle 行（scoped-DECLARED，inventory 已落地、
    /// unblock_phase=14 未到期）经 evaluate 不得产生任何 BLOCKED_ON_SUT 违规——闸只红在该红的
    /// 对象上（§57.1 精神）。
    #[test]
    fn real_msl_row_passes_blocked_on_sut_gate() {
        let spec = real_spec_md();
        let table = parse_declaration_table(&spec);
        let result = evaluate(&spec, &table, 8, Some(&repo_root()));
        assert!(
            !result
                .violations
                .iter()
                .any(|v| v.0.contains("memory_security_lifecycle")
                    && (v.0.contains("BLOCKED_ON_SUT")
                        || v.0.contains("unblock_phase")
                        || v.0.contains("blocked_stage_inventory"))),
            "real msl row must pass the scoped/BLOCKED_ON_SUT gate: {:?}",
            result.violations
        );
    }

    /// 注错：Phase 11 后 `code_retrieval`（owner=Code Phase 11+，仍 NOT_DECLARED）必须红——
    /// 此前 owning-phase 到期分支零测试覆盖，默认档位 (phase 0) 下 11 个单测全部惰性通过。
    #[test]
    fn owning_phase_expires_red_at_phase_11_for_code_retrieval() {
        let spec = real_spec_md();
        let table = parse_declaration_table(&spec);
        let result = evaluate(&spec, &table, 11, Some(&repo_root()));
        assert!(
            result
                .violations
                .iter()
                .any(|v| v.0.contains("code_retrieval") && v.0.contains("owning phase")),
            "expected code_retrieval owning-phase-expired violation at phase 11: {:?}",
            result.violations
        );
    }

    /// 反向：Phase 10（未到期）同一行必须落 not_applicable（na_list），不产生 violation。
    #[test]
    fn owning_phase_not_yet_due_at_phase_10_for_code_retrieval() {
        let spec = real_spec_md();
        let table = parse_declaration_table(&spec);
        let result = evaluate(&spec, &table, 10, Some(&repo_root()));
        assert!(
            !result
                .violations
                .iter()
                .any(|v| v.0.contains("code_retrieval")),
            "code_retrieval must not fail before its owning phase: {:?}",
            result.violations
        );
        assert!(
            result.na_list.iter().any(|s| s.contains("code_retrieval")),
            "code_retrieval should be listed as not-yet-due: {:?}",
            result.na_list
        );
    }

    /// 回归（owner 标注检查只跑在未声明行）：declared 行删掉 owner 标注也必须被点名——
    /// 用 fixture 把 continuation_198_v2 那行的 `owner=Phase` 判定格清空、同时把 DoD-015 的
    /// `[phase=` 标签也抹掉（切断通用回退），并把该行七字段填成declared 形态，验证仍然产生
    /// 「无法确定 owning phase」违规而不是被 `declared { continue }` 放过。
    #[test]
    fn declared_row_without_any_owning_phase_signal_still_fails() {
        let spec = real_spec_md();
        let table = parse_declaration_table(&spec);
        // 构造一个已经七字段齐全、且没有任何可解析 owning phase 信号的行。
        let declared_row = vec![
            "`synthetic_declared_row`".to_string(),
            "synthetic set".to_string(),
            "10 = a 5 + b 5".to_string(),
            "top_k=5".to_string(),
            "resolution：4 题；spread_tol：2 题".to_string(),
            "2026-01-01 / abcdef0".to_string(),
            "`DECLARED`".to_string(), // 判定格里没有 owner=Phase 标注
        ];
        let (declared, _) = row_declared(&declared_row);
        assert!(declared, "fixture row must itself be §55.3-declared");
        let mut synthetic_table = table.clone();
        synthetic_table.push(declared_row);
        let result = evaluate(&spec, &synthetic_table, 0, Some(&repo_root()));
        assert!(
            result
                .violations
                .iter()
                .any(|v| v.0.contains("synthetic_declared_row")
                    && v.0.contains("无法确定 owning phase")),
            "declared row with no owner=Phase annotation and no DoD fallback must still fail: {:?}",
            result.violations
        );
    }

    /// 注错（§55.3.1 manifest 面）：declared 行的 manifest 文件不存在 ⇒ 必须红。第一次跑
    /// 删-manifest 注错时 checker 只读 spec 表行、对 manifest 消失完全无感——本测试钉住修复。
    #[test]
    fn declared_row_with_no_manifest_file_is_red() {
        let spec = real_spec_md();
        let mut table = parse_declaration_table(&spec);
        table.push(vec![
            "`synthetic_declared_row`".to_string(),
            "synthetic set".to_string(),
            "10 = a 5 + b 5".to_string(),
            "top_k=5".to_string(),
            "resolution：4 题；spread_tol：2 题".to_string(),
            "2026-01-01 / abcdef0".to_string(),
            "`DECLARED` · owner=Retrieval（Phase 6+）".to_string(),
        ]);
        let result = evaluate(&spec, &table, 0, Some(&repo_root()));
        assert!(
            result
                .violations
                .iter()
                .any(|v| v.0.contains("synthetic_declared_row")
                    && v.0.contains("BenchmarkManifest 不存在")),
            "declared row without a manifest file must be red: {:?}",
            result.violations
        );
    }

    /// 窄度对照：真实的两个 DECLARED 行（planner_predicate / exact_completeness）manifest
    /// 齐全，本闸不得对它们产生任何 manifest 违规——闸只红在该红的对象上（§57.1 精神）。
    #[test]
    fn real_declared_rows_pass_the_manifest_gate() {
        let spec = real_spec_md();
        let table = parse_declaration_table(&spec);
        let result = evaluate(&spec, &table, 0, Some(&repo_root()));
        assert!(
            !result
                .violations
                .iter()
                .any(|v| v.0.contains("BenchmarkManifest")),
            "real declared rows carry complete manifests; manifest violations here mean the \
             gate is over-firing: {:?}",
            result.violations
        );
    }

    /// 注错 A：fixture 表删一行（删 `code_retrieval` 那一行）⇒ 9 ≠ 恰好 9 ⇒ 红。
    #[test]
    fn fault_row_deleted_breaks_exact_9_count() {
        let raw = real_spec_md();
        let row_marker = "| `code_retrieval` | code retrieval set |";
        let line_start = raw.find(row_marker).expect("row must exist");
        let line_end = raw[line_start..]
            .find('\n')
            .map(|i| line_start + i + 1)
            .unwrap_or(raw.len());
        let mut bad = String::new();
        bad.push_str(&raw[..line_start]);
        bad.push_str(&raw[line_end..]);
        assert_ne!(bad, raw);
        let fixture = fixture_copy(&bad);
        let table = parse_declaration_table(&fixture);
        assert_eq!(table.len(), REQUIRED_ROW_COUNT - 1);
        assert_ne!(table.len(), REQUIRED_ROW_COUNT);
    }

    /// 注错 B：把任一行「判定」格写回 legacy `NO_DOD_ITEM` ⇒ 立即红。
    #[test]
    fn fault_verdict_rewritten_to_legacy_marker_is_red() {
        let raw = real_spec_md();
        let bad = raw.replacen(
            "`NOT_DECLARED` · owner=Private Memory（Phase 4+）",
            "NO_DOD_ITEM",
            1,
        );
        assert_ne!(bad, raw);
        let fixture = fixture_copy(&bad);
        let table = parse_declaration_table(&fixture);
        let hit = table
            .iter()
            .any(|r| r.get(6).is_some_and(|c| c.contains(LEGACY_MARKER)));
        assert!(hit, "rewritten NO_DOD_ITEM row must be detected");
    }

    #[test]
    fn not_applicable_when_table_heading_missing() {
        let fixture = fixture_copy("# 1. only chapter\nnothing else here\n");
        assert!(parse_declaration_table(&fixture).is_empty());
    }

    /// `--phase` 覆盖仍然生效；**不传参数时读仓库根的 PHASE 真源，不再默认 0**。
    /// 旧断言「默认 0」测的正是被审计判定为假绿根因的行为（默认 0 豁免一切 phase>0 条目），
    /// 因此这条改成钉新契约：无参数 ⇒ 至少是已交付的相位 7。
    #[test]
    fn parse_phase_arg_reads_the_truth_source_and_honors_the_flag() {
        assert_eq!(
            parse_phase_arg(&["--phase".to_string(), "3".to_string()]),
            3
        );
        assert_eq!(parse_phase_arg(&["--phase=11".to_string()]), 11);
        assert!(
            parse_phase_arg(&[]) >= 7,
            "无参数必须读 PHASE 真源（当前 7），不得退回 0"
        );
    }
}
