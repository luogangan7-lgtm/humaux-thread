//! xtask `benchset-declaration` — G80-16 benchset-declaration-check.
//! 判据以 §69「Benchmark 集合分母声明」节为唯一真源：表恰 10 行、
//! `set_id` 与 §55 集合清单逐名对齐、「判定」格禁现 legacy `NO_DOD_ITEM`、§55.3 七字段
//! （spec 行 9279–9291：`set_id`/`fixed_denominator`/`decision_depth`/`resolution`/
//! `spread_tol`/`measured_at`/`frozen_by`）缺任一 ⇒ `NOT_DECLARED`、owning phase 到期仍
//! `NOT_DECLARED` ⇒ 红。本文件不复述判据散文，只引用 § 号（仓库硬边界）。

use std::fs;

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

/// 合并列里某一子字段是否带独立的「题」量纲实测读数（不是裸占位符、不是只提了维度没给数）。
fn has_dimensioned_reading(s: &str) -> bool {
    s.contains('题') && s.bytes().any(|b| b.is_ascii_digit())
}

/// §69 冻结（spec ~11155）：`resolution` 与 `spread_tol` 合并列必须**分别标明**、各自带独立
/// 实测读数，禁止拿一个数含糊过两个字段。按字面出现的 `spread_tol` 关键字切成两半分别校验；
/// 找不到关键字（如整格只写「未实测」）即两者都算缺失。
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
    args.iter()
        .position(|a| a == "--phase")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0)
}

/// 一次完整判定的结果：`violations` 决定退出码，`na_list` 只是尚未到 owning phase 的附注。
pub struct EvalResult {
    pub violations: Vec<Violation>,
    pub na_list: Vec<String>,
}

/// 对已解析的声明表跑 §69 全部判据（恰 9 行 / 逐名对齐 / legacy marker / 七字段 / owning
/// phase）。从 `run()` 抽出以便测试直接驱动 owning-phase 到期分支（无需解析 stdout/stderr）。
pub fn evaluate(spec_md: &str, table: &[Vec<String>], current_phase: u32) -> EvalResult {
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

    let mut na_list: Vec<String> = Vec::new();
    for row in table {
        let set_id = row.first().map(|s| strip_backticks(s)).unwrap_or_default();
        let verdict_cell = row.get(6).map(String::as_str).unwrap_or("");

        if verdict_cell.contains(LEGACY_MARKER) {
            violations.push(Violation(format!(
                "{set_id}: 判定格出现禁用标记 {LEGACY_MARKER}"
            )));
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
                // 七字段齐全，不受 owning phase 到期约束。
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
    } = evaluate(&spec_md, &table, current_phase);

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
        const DECLARED_EXEMPT: &[&str] = &["planner_predicate"];
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

    /// 注错：Phase 11 后 `code_retrieval`（owner=Code Phase 11+，仍 NOT_DECLARED）必须红——
    /// 此前 owning-phase 到期分支零测试覆盖，默认档位 (phase 0) 下 11 个单测全部惰性通过。
    #[test]
    fn owning_phase_expires_red_at_phase_11_for_code_retrieval() {
        let spec = real_spec_md();
        let table = parse_declaration_table(&spec);
        let result = evaluate(&spec, &table, 11);
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
        let result = evaluate(&spec, &table, 10);
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
        let result = evaluate(&spec, &synthetic_table, 0);
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

    #[test]
    fn parse_phase_arg_defaults_to_0_and_parses_flag() {
        assert_eq!(parse_phase_arg(&[]), 0);
        assert_eq!(
            parse_phase_arg(&["--phase".to_string(), "7".to_string()]),
            7
        );
    }
}
