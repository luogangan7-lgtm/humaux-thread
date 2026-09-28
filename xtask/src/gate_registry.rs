//! `xtask::gate_registry` — G80-23 gate-anchor-check + G80-24 gate-registry-coverage.
//! Depends-on: crates=[]; services=[PostgreSQL(any) r=[ops.column_vitality]]; env=[CARGO_MANIFEST_DIR]; modules=[]
//! Called-by: [xtask::main]
//! Invariants: [G80-23 catches a registry cell whose anchor no longer resolves; G80-24 catches an id present in a home chapter but never registered]
//! Spec: Baseline §80.1.2; §57.1
//!
//! xtask `gate-registry` — G80-23 `gate-anchor-check` + G80-24 `gate-registry-coverage`
//! (§80.1.2 is the home chapter for both; judgement prose lives there and in §57.1 for
//! G80-24③ — this module cites § numbers, it does not restate the rules).
//!
//! G80-23 catches the home-chapter-side gap: a §80.1 registry cell that no longer resolves
//! (dead anchor, drifted id, missing fault record). G80-24 catches the opposite gap: an id
//! that exists in a home chapter but was never registered in §80.1. The two are deliberately
//! not merged (§80.1.2: "两个方向缺一个都有半边看不见").
//!
//! Parsing targets the canonical spec markdown itself (`docs/architecture/Baseline_2.9.md`),
//! never a second copy — every table/section is re-located by heading text each run, so a
//! spec edit cannot silently desync this checker (same discipline as `mechanism_registry.rs`
//! / `contract_impact.rs`).
//!
//! ponytail: the anchor/family grammar is a small closed set (§80.1.2), so this hand-rolls a
//! handful of char-scanning matchers instead of pulling in the `regex` crate — no new xtask
//! dependency needed for five fixed shapes. Upgrade only if the grammar itself grows more
//! than a couple more forms.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

const SPEC_PATH: &str = "docs/architecture/Baseline_2.9.md";

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

/// Verdict shared by every sub-check. §57.1 第2条 makes the project-wide gate contract
/// three-state (`pass`/`fail`/`not_applicable`, the latter always naming the missing object),
/// but that third state's only legitimate reason is "被测对象在当前 Phase 尚未交付" — every
/// object G80-23/G80-24 check (§80.1, §80.1.2, §57.1's own tables) is a frozen chapter that's
/// mandatory from Phase 0 onward, so none of them is ever legitimately "not yet delivered".
/// This module's Verdict is Pass/Fail only for that reason, not because it opts out of the
/// project convention: a missing table here means broken, not deferred, and reporting it
/// `not_applicable` would fail-open (`report` treats `not_applicable` as non-fatal).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Fail(Vec<String>),
}

// ============================================================================
// markdown heading index — shared by every section/table lookup below
// ============================================================================

/// One `#`/`##`/`###` (up to 6, none deeper appear in this spec) heading line. `number` is
/// `None` for unnumbered headings (e.g. `## Privacy Disclosure Ledger …`) — those still count
/// as section *boundaries*, they're just never an anchor *target*.
struct Heading {
    level: u8,
    number: Option<String>,
    line_idx: usize,
}

/// Parses one line as a heading: `level` = leading `#` run length; `number` = the numeric
/// token (dot-separated digit groups, e.g. `80.1.1`) immediately following the `#`s and a
/// mandatory space, with a single trailing `.` stripped so both `# 42. Alerting` (chapter
/// style) and `## 11.9 Consolidation …` (section style) parse to `"42"` / `"11.9"`.
fn parse_heading(line: &str) -> Option<(u8, Option<String>)> {
    let hashes = line.chars().take_while(|&c| c == '#').count();
    if hashes == 0 || hashes > 6 {
        return None;
    }
    let rest = &line[hashes..];
    if !rest.starts_with(' ') {
        return None;
    }
    let title = rest.trim_start();
    let bytes = title.as_bytes();
    let mut end = 0usize;
    while end < bytes.len() && (bytes[end].is_ascii_digit() || bytes[end] == b'.') {
        end += 1;
    }
    let token = title[..end].trim_end_matches('.');
    let number =
        (!token.is_empty() && token.as_bytes()[0].is_ascii_digit()).then(|| token.to_string());
    Some((hashes as u8, number))
}

fn build_headings(lines: &[&str]) -> Vec<Heading> {
    lines
        .iter()
        .enumerate()
        .filter_map(|(i, l)| {
            parse_heading(l).map(|(level, number)| Heading {
                level,
                number,
                line_idx: i,
            })
        })
        .collect()
}

/// `[start, end)` 0-based line range of the body owned by `headings[idx]` — everything up to
/// (not including) the next heading whose level is `<=` this one's (a deeper subsection stays
/// part of the body; §1.14's `### 1.14.1` remains inside `§1.14`'s range this way).
fn section_body_range(headings: &[Heading], idx: usize, total_lines: usize) -> (usize, usize) {
    let h = &headings[idx];
    let start = h.line_idx + 1;
    let end = headings[idx + 1..]
        .iter()
        .find(|nh| nh.level <= h.level)
        .map(|nh| nh.line_idx)
        .unwrap_or(total_lines);
    (start, end)
}

fn section_lines<'a>(lines: &[&'a str], headings: &[Heading], num: &str) -> Option<Vec<&'a str>> {
    let idx = headings
        .iter()
        .position(|h| h.number.as_deref() == Some(num))?;
    let (s, e) = section_body_range(headings, idx, lines.len());
    Some(lines[s..e].to_vec())
}

fn find_section_body(lines: &[&str], headings: &[Heading], num: &str) -> Option<String> {
    section_lines(lines, headings, num).map(|ls| ls.join("\n"))
}

// ============================================================================
// generic markdown-table row extraction
// ============================================================================

/// Rows of the `| a | b | ... |` table whose header is `lines[header_idx]` (separator row is
/// `header_idx + 1`), stopping at the first following line that isn't `|`-fenced.
fn parse_table_at(lines: &[&str], header_idx: usize) -> Vec<(usize, Vec<String>)> {
    let mut rows = Vec::new();
    let mut i = header_idx + 2;
    while i < lines.len() {
        let line = lines[i].trim();
        if !line.starts_with('|') {
            break;
        }
        let cells: Vec<String> = line
            .trim_matches('|')
            .split('|')
            .map(|c| c.trim().to_string())
            .collect();
        rows.push((i, cells));
        i += 1;
    }
    rows
}

/// Every backtick-quoted token in `s`, in order (`` `X` text `Y` `` → `["X", "Y"]`).
fn backtick_tokens(s: &str) -> Vec<String> {
    s.split('`')
        .skip(1)
        .step_by(2)
        .map(str::to_string)
        .collect()
}

/// Every maximal `[A-Za-z0-9-]` run in `text`, as a set — used where the id shape isn't known
/// in advance (§80.1.2②'s "id 在正文内逐字出现" is a whole-token test, not a substring test:
/// a cell anchoring `G12` must not be satisfied by a body that only contains `G120`).
fn text_id_tokens(text: &str) -> BTreeSet<&str> {
    let mut out = BTreeSet::new();
    let mut start: Option<usize> = None;
    for (i, c) in text.char_indices() {
        let is_tok = c.is_ascii_alphanumeric() || c == '-';
        match (is_tok, start) {
            (true, None) => start = Some(i),
            (false, Some(s)) => {
                out.insert(&text[s..i]);
                start = None;
            }
            _ => {}
        }
    }
    if let Some(s) = start {
        out.insert(&text[s..]);
    }
    out
}

// ============================================================================
// §80.1.2 anchor grammar (closed set): §X.Y#ID · §X.Y · §X · 同左 · —
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
enum AnchorUnit {
    Section { num: String, id: Option<String> },
    SameAsLeft,
    Dash,
}

/// Gate-id syntax used at `#ID` anchor positions: starts with an uppercase ASCII letter,
/// body is ASCII alnum/`-` only (no leading/trailing/doubled `-`), and contains at least one
/// digit. §80.1.2's anchor grammar names a narrower closed set (`G<数字>` ·
/// `G<数字>-<数字><可选小写字母>` · `INV-<数字>` · `D<数字>`), but the real, frozen §80.1
/// table also anchors `§6.2.3#G6-DB1`/`#G6-DB2` (G80-40) — a shape that grammar's literal
/// four forms don't cover, and §80.1.2's own id-family binding table registers `G6-DB\d` as a
/// real family for that section. This validator accepts the observed superset rather than the
/// narrower stated closed set, since a real, frozen-green anchor is stronger evidence of what
/// counts as "a gate id" here than the prose's four enumerated shapes; it still rejects what
/// ①/② exist to catch — prose, punctuation, Chinese text, or a wrong-shaped id leaking into a
/// cell (§80.1.2① 注错 b).
fn is_valid_gate_id(s: &str) -> bool {
    if s.is_empty() || !s.as_bytes()[0].is_ascii_uppercase() {
        return false;
    }
    let mut has_digit = false;
    let mut prev_dash = false;
    for c in s.chars() {
        if c == '-' {
            if prev_dash {
                return false;
            }
            prev_dash = true;
        } else if c.is_ascii_alphanumeric() {
            has_digit |= c.is_ascii_digit();
            prev_dash = false;
        } else {
            return false;
        }
    }
    has_digit && !prev_dash
}

/// One `§X.Y#ID` / `§X.Y` / `§X` unit — every character of `seg` must be consumed by the
/// grammar (no stray prose, parens, or punctuation), so a cell that has drifted from a pure
/// anchor into descriptive text fails here (§80.1.2① 注错 b).
fn parse_section_anchor(seg: &str) -> Option<AnchorUnit> {
    let seg = seg.trim();
    let rest = seg.strip_prefix('§')?;
    let (num_part, id_part) = match rest.find('#') {
        Some(i) => (&rest[..i], Some(&rest[i + 1..])),
        None => (rest, None),
    };
    if num_part.is_empty() || !num_part.as_bytes()[0].is_ascii_digit() {
        return None;
    }
    let mut prev_dot = false;
    for c in num_part.chars() {
        if c.is_ascii_digit() {
            prev_dot = false;
        } else if c == '.' && !prev_dot {
            prev_dot = true;
        } else {
            return None;
        }
    }
    if prev_dot {
        return None; // trailing '.' inside a section number (not the chapter-style suffix)
    }
    let id = match id_part {
        Some(idp) if !idp.is_empty() && is_valid_gate_id(idp) => Some(idp.to_string()),
        Some(_) => return None,
        None => None,
    };
    Some(AnchorUnit::Section {
        num: num_part.to_string(),
        id,
    })
}

/// Whole-cell parse. `同左` is only valid where `allow_same_left` (注错出处 column only, §80.1.2
/// 锚文法). `—` is valid in either column. Anything mixing `同左`/`—` with other units, or using
/// a divider other than exactly `" · "`, fails to parse (① 红).
fn parse_anchor_cell(raw: &str, allow_same_left: bool) -> Option<Vec<AnchorUnit>> {
    let cell = raw.trim();
    if cell == "同左" {
        return allow_same_left.then_some(vec![AnchorUnit::SameAsLeft]);
    }
    if cell == "—" {
        return Some(vec![AnchorUnit::Dash]);
    }
    let mut out = Vec::new();
    for seg in cell.split(" · ") {
        out.push(parse_section_anchor(seg)?);
    }
    (!out.is_empty()).then_some(out)
}

// ============================================================================
// §80.1 registry table
// ============================================================================

struct GateRow {
    id: String,
    cross_phase: bool,
    criterion_raw: String,
    error_raw: String,
}

fn parse_gate_table(text: &str) -> Result<Vec<GateRow>, String> {
    let lines: Vec<&str> = text.lines().collect();
    let headings = build_headings(&lines);
    let idx = headings
        .iter()
        .position(|h| h.number.as_deref() == Some("80.1"))
        .ok_or_else(|| "§80.1 heading (架构闸登记表) not found".to_string())?;
    let (s, e) = section_body_range(&headings, idx, lines.len());
    let header_rel = lines[s..e]
        .iter()
        .position(|l| l.trim().starts_with("| 闸"))
        .ok_or_else(|| "§80.1 registry table header row (| 闸 | ...) not found".to_string())?;
    let header_idx = s + header_rel;
    let mut out = Vec::new();
    for (_, cells) in parse_table_at(&lines, header_idx) {
        if cells.len() < 4 {
            continue;
        }
        let first_cell = cells[0].trim();
        let id = first_cell
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_string();
        if !id.starts_with("G80-") {
            continue;
        }
        out.push(GateRow {
            id,
            cross_phase: first_cell.contains("(cross-phase)"),
            criterion_raw: cells[2].clone(),
            error_raw: cells[3].clone(),
        });
    }
    Ok(out)
}

/// Resolves one row's (判据出处, 注错出处) into parsed unit lists, with `同左` expanded to the
/// criterion column's own units. `None` means one of the two cells failed grammar parsing —
/// callers skip such rows for ②/③/④/G80-24, since ① already reports the grammar failure
/// itself and re-deriving downstream facts from an unparseable cell would be meaningless.
fn resolved_units_for_row(row: &GateRow) -> Option<(Vec<AnchorUnit>, Vec<AnchorUnit>)> {
    let crit = parse_anchor_cell(&row.criterion_raw, false)?;
    let err_cell = parse_anchor_cell(&row.error_raw, true)?;
    let err = if err_cell.len() == 1 && err_cell[0] == AnchorUnit::SameAsLeft {
        crit.clone()
    } else {
        err_cell
    };
    Some((crit, err))
}

// ============================================================================
// §80.1.2 id-family binding table
// ============================================================================

struct FamilyRow {
    section_num: String,
    pattern_raw: String,
    retired: Vec<String>,
}

fn parse_family_table(text: &str) -> Result<Vec<FamilyRow>, String> {
    let lines: Vec<&str> = text.lines().collect();
    let headings = build_headings(&lines);
    let idx = headings
        .iter()
        .position(|h| h.number.as_deref() == Some("80.1.2"))
        .ok_or_else(|| "§80.1.2 heading (登记表自身的两道闸) not found".to_string())?;
    let (s, e) = section_body_range(&headings, idx, lines.len());
    let header_rel = lines[s..e]
        .iter()
        .position(|l| l.trim().starts_with("| 家章小节"))
        .ok_or_else(|| "§80.1.2 id-family binding table header row not found".to_string())?;
    let header_idx = s + header_rel;
    let mut out = Vec::new();
    for (_, cells) in parse_table_at(&lines, header_idx) {
        if cells.len() < 3 {
            continue;
        }
        let Some(num) = cells[0].trim().strip_prefix('§') else {
            continue;
        };
        let pattern_raw = cells[1].trim().trim_matches('`').to_string();
        let retired = backtick_tokens(&cells[2])
            .into_iter()
            .filter(|t| is_valid_gate_id(t))
            .collect();
        out.push(FamilyRow {
            section_num: num.to_string(),
            pattern_raw,
            retired,
        });
    }
    Ok(out)
}

/// A compiled id-family regex from the binding table's second column — the only shapes that
/// occur are a literal prefix optionally followed by `\d`/`\d+` (one-or-more digits) and an
/// optional trailing `[a-z]?` (single lowercase letter); `has_digits = false` means the whole
/// pattern is a literal exact id (§80.3 row: `G80-42`, no digit placeholder).
struct CompiledPattern {
    prefix: String,
    has_digits: bool,
    allow_letter: bool,
}

fn compile_family_pattern(raw: &str) -> Option<CompiledPattern> {
    let mut p = raw.trim();
    let allow_letter = if let Some(s) = p.strip_suffix("[a-z]?") {
        p = s;
        true
    } else {
        false
    };
    let has_digits = if let Some(s) = p.strip_suffix("\\d+") {
        p = s;
        true
    } else if let Some(s) = p.strip_suffix("\\d") {
        p = s;
        true
    } else {
        false
    };
    if p.is_empty() && !has_digits {
        return None;
    }
    Some(CompiledPattern {
        prefix: p.to_string(),
        has_digits,
        allow_letter,
    })
}

/// Whether `token` (a whole identifier-shaped run, not a substring) fully matches `cp`.
fn pattern_matches(cp: &CompiledPattern, token: &str) -> bool {
    if !cp.has_digits {
        return token == cp.prefix;
    }
    let Some(rest) = token.strip_prefix(cp.prefix.as_str()) else {
        return false;
    };
    let digit_len = rest.bytes().take_while(|b| b.is_ascii_digit()).count();
    if digit_len == 0 {
        return false;
    }
    let tail = &rest[digit_len..];
    if tail.is_empty() {
        return true;
    }
    cp.allow_letter
        && tail.chars().count() == 1
        && tail.chars().next().unwrap().is_ascii_lowercase()
}

/// Every maximal `[A-Za-z0-9-]` run in `text` that fully matches `cp`, deduplicated — the
/// family regex's "去重 id 集合" (§80.1.2①). Non-ASCII (Chinese prose) and any other
/// punctuation naturally bounds tokens, so this needs no word-boundary special-casing.
fn extract_ids_matching(text: &str, cp: &CompiledPattern) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut cur = String::new();
    for c in text.chars().chain(std::iter::once(' ')) {
        if c.is_ascii_alphanumeric() || c == '-' {
            cur.push(c);
        } else if !cur.is_empty() {
            if pattern_matches(cp, &cur) {
                out.insert(cur.clone());
            }
            cur.clear();
        }
    }
    out
}

// ============================================================================
// §57.1 Phase table
// ============================================================================

struct PhaseRow {
    must_pass_raw: String,
}

fn parse_phase_table(text: &str) -> Result<Vec<PhaseRow>, String> {
    let lines: Vec<&str> = text.lines().collect();
    let headings = build_headings(&lines);
    let idx = headings
        .iter()
        .position(|h| h.number.as_deref() == Some("57.1"))
        .ok_or_else(|| "§57.1 heading (Phase 出场判据与闸的生效期) not found".to_string())?;
    let (s, e) = section_body_range(&headings, idx, lines.len());
    let header_rel = lines[s..e]
        .iter()
        .position(|l| l.trim().starts_with("| Phase"))
        .ok_or_else(|| "§57.1 Phase table header row not found".to_string())?;
    let header_idx = s + header_rel;
    let mut out = Vec::new();
    for (_, cells) in parse_table_at(&lines, header_idx) {
        if cells.len() < 3 {
            continue;
        }
        out.push(PhaseRow {
            must_pass_raw: cells[2].clone(),
        });
    }
    Ok(out)
}

/// Gates that are phased by sub-item rather than as a whole (§57.1: "按子项分期的 family 必须
/// 列全子项且并集等于该 family"; today only `G80-17`/D2·D3). Detected generically by scanning
/// the whole spec for the `` `G80-N` 按子项分期 `` sentence shape and pulling every backtick
/// id-grammar token that follows it on the same line — not hardcoded, so a future second
/// sub-itemed gate is picked up without touching this file.
fn subitem_families(text: &str) -> BTreeMap<String, BTreeSet<String>> {
    let mut out = BTreeMap::new();
    for line in text.lines() {
        let Some(pos) = line.find("按子项分期") else {
            continue;
        };
        let (prefix, suffix) = line.split_at(pos);
        let Some(family_id) = backtick_tokens(prefix)
            .into_iter()
            .rev()
            .find(|t| is_valid_gate_id(t))
        else {
            continue;
        };
        let subitems: BTreeSet<String> = backtick_tokens(suffix)
            .into_iter()
            .filter(|t| is_valid_gate_id(t) && t != &family_id)
            .collect();
        if !subitems.is_empty() {
            out.insert(family_id, subitems);
        }
    }
    out
}

/// Extracts the ` ```text ` block immediately following the "Repository 第一天启用：" line in
/// §46 — the 17-line list §57.1 覆盖校验② requires each line of to appear exactly once in the
/// §57.1 Phase table's "本期起必过" column.
fn repo_day_one_items(text: &str) -> Result<Vec<String>, String> {
    let lines: Vec<&str> = text.lines().collect();
    let headings = build_headings(&lines);
    let body = section_lines(&lines, &headings, "46")
        .ok_or_else(|| "§46 heading (CI/CD) not found".to_string())?;
    let start_rel = body
        .iter()
        .position(|l| l.contains("Repository 第一天启用"))
        .ok_or_else(|| "§46 \"Repository 第一天启用\" line not found".to_string())?;
    let fence_rel = body[start_rel..]
        .iter()
        .position(|l| l.trim() == "```text")
        .map(|i| start_rel + i)
        .ok_or_else(|| "§46 ```text block after \"Repository 第一天启用\" not found".to_string())?;
    for (i, l) in body[fence_rel + 1..].iter().enumerate() {
        if l.trim() == "```" {
            return Ok(body[fence_rel + 1..fence_rel + 1 + i]
                .iter()
                .map(|l| l.to_string())
                .collect());
        }
    }
    Err("§46 ```text block after \"Repository 第一天启用\" never closes".to_string())
}

// ============================================================================
// G80-23 gate-anchor-check (§80.1.2)
// ============================================================================

/// ① 表两列每格整体匹配锚文法闭集.
fn g80_23_check1(rows: &[GateRow]) -> Verdict {
    let mut bad = Vec::new();
    for r in rows {
        if parse_anchor_cell(&r.criterion_raw, false).is_none() {
            bad.push(format!(
                "{}: 判据出处单元格不匹配锚文法: {:?}",
                r.id, r.criterion_raw
            ));
        }
        if parse_anchor_cell(&r.error_raw, true).is_none() {
            bad.push(format!(
                "{}: 注错出处单元格不匹配锚文法: {:?}",
                r.id, r.error_raw
            ));
        }
    }
    if bad.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(bad)
    }
}

/// ② 每个 §…#ID 锚：小节标题行存在，且 id 在该小节正文内逐字出现 ≥ 1 次.
fn g80_23_check2(text: &str, rows: &[GateRow]) -> Verdict {
    let lines: Vec<&str> = text.lines().collect();
    let headings = build_headings(&lines);
    let mut bad = Vec::new();
    for r in rows {
        let Some((crit, err)) = resolved_units_for_row(r) else {
            continue;
        };
        // §80.1.2 anchor grammar's `同左` makes 判据/注错 resolve to the *same* unit list —
        // dedup by (num, id) so that shared case doesn't print the identical finding twice.
        let units: BTreeSet<(&str, &str)> = crit
            .iter()
            .chain(err.iter())
            .filter_map(|u| match u {
                AnchorUnit::Section { num, id: Some(id) } => Some((num.as_str(), id.as_str())),
                _ => None,
            })
            .collect();
        for (num, id) in units {
            match find_section_body(&lines, &headings, num) {
                None => bad.push(format!("{}: §{num} 标题行不存在（锚 §{num}#{id}）", r.id)),
                Some(body) if !text_id_tokens(&body).contains(id) => bad.push(format!(
                    "{}: §{num} 正文未逐字出现 id {id}（锚 §{num}#{id}）",
                    r.id
                )),
                Some(_) => {}
            }
        }
    }
    if bad.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(bad)
    }
}

/// ③ 每个不带 #ID 的锚：对应的标题行必须存在.
fn g80_23_check3(text: &str, rows: &[GateRow]) -> Verdict {
    let lines: Vec<&str> = text.lines().collect();
    let headings = build_headings(&lines);
    let mut bad = Vec::new();
    for r in rows {
        let Some((crit, err)) = resolved_units_for_row(r) else {
            continue;
        };
        // Dedup the same way check2 does (`同左` makes 判据/注错 resolve to identical units).
        let nums: BTreeSet<&str> = crit
            .iter()
            .chain(err.iter())
            .filter_map(|u| match u {
                AnchorUnit::Section { num, id: None } => Some(num.as_str()),
                _ => None,
            })
            .collect();
        for num in nums {
            if find_section_body(&lines, &headings, num).is_none() {
                bad.push(format!("{}: §{num} 标题行不存在", r.id));
            }
        }
    }
    if bad.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(bad)
    }
}

/// ④ 注错出处所指范围正文必须逐字含「注错」或「注入」；锚为 §80.1.1 时另加一条：§80.1.1
/// 内必须有一行以该行的闸 id 打头。列值为 — 的行（NOT_ADMITTED）不参与本条。
fn g80_23_check4(text: &str, rows: &[GateRow]) -> Verdict {
    let lines: Vec<&str> = text.lines().collect();
    let headings = build_headings(&lines);
    let mut bad = Vec::new();
    for r in rows {
        let Some(err_cell) = parse_anchor_cell(&r.error_raw, true) else {
            continue;
        };
        if err_cell.len() == 1 && err_cell[0] == AnchorUnit::Dash {
            continue; // NOT_ADMITTED — excluded from ④ per §80.1.2
        }
        let Some(crit) = parse_anchor_cell(&r.criterion_raw, false) else {
            continue;
        };
        let resolved = if err_cell.len() == 1 && err_cell[0] == AnchorUnit::SameAsLeft {
            crit
        } else {
            err_cell
        };
        for unit in &resolved {
            let AnchorUnit::Section { num, .. } = unit else {
                continue;
            };
            match section_lines(&lines, &headings, num) {
                None => bad.push(format!("{}: 注错出处 §{num} 标题行不存在", r.id)),
                Some(ls) => {
                    let body = ls.join("\n");
                    if !(body.contains("注错") || body.contains("注入")) {
                        bad.push(format!(
                            "{}: 注错出处 §{num} 正文不含「注错」或「注入」",
                            r.id
                        ));
                    }
                    if num == "80.1.1" {
                        let has_line = ls.iter().any(|l| {
                            l.starts_with(r.id.as_str())
                                && l[r.id.len()..]
                                    .chars()
                                    .next()
                                    .is_none_or(|c| !c.is_ascii_alphanumeric())
                        });
                        if !has_line {
                            bad.push(format!("{}: §80.1.1 内无以 {} 打头的行", r.id, r.id));
                        }
                    }
                }
            }
        }
    }
    if bad.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(bad)
    }
}

/// ⑤ NOT_ADMITTED（注错出处 == — 或解析失败）行数必须为 0. §80.1 准入条件：「『注错出处』
/// 列解析成功且不是 `—` ⇒ ADMITTED；否则 NOT_ADMITTED」 — a cell that fails to parse is
/// "否则", not silently ADMITTED (① already reports the parse failure itself; this still has
/// to count it here too, since ① and ⑤ answer different questions on the same cell).
fn g80_23_check5(rows: &[GateRow]) -> Verdict {
    let not_admitted: Vec<&str> = rows
        .iter()
        .filter(|r| {
            parse_anchor_cell(&r.error_raw, true)
                .is_none_or(|c| c.len() == 1 && c[0] == AnchorUnit::Dash)
        })
        .map(|r| r.id.as_str())
        .collect();
    if not_admitted.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(vec![format!(
            "NOT_ADMITTED 行数 = {}（{:?}）",
            not_admitted.len(),
            not_admitted
        )])
    }
}

// ============================================================================
// G80-24 gate-registry-coverage (§80.1.2, ③ 判据在 §57.1)
// ============================================================================

/// ① 逐条 id 族绑定表行：A（该小节正文按族正则去重集合）\ (B（§80.1 引用到该小节的 id 集合）
/// ∪ Rt（退役集）) 必须为空.
fn g80_24_check1(text: &str, families: &[FamilyRow], gate_rows: &[GateRow]) -> Verdict {
    let lines: Vec<&str> = text.lines().collect();
    let headings = build_headings(&lines);

    let mut b_by_section: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for r in gate_rows {
        let Some((crit, err)) = resolved_units_for_row(r) else {
            continue;
        };
        for unit in crit.into_iter().chain(err) {
            if let AnchorUnit::Section { num, id: Some(id) } = unit {
                b_by_section.entry(num).or_default().insert(id);
            }
        }
    }

    let mut bad = Vec::new();
    for fr in families {
        let Some(cp) = compile_family_pattern(&fr.pattern_raw) else {
            bad.push(format!(
                "§{}: 无法解析族正则 {:?}",
                fr.section_num, fr.pattern_raw
            ));
            continue;
        };
        // §80.1.2①: 扫描域按小节自身边界（section_body_range）取正文，天然排除 §80.1 /
        // §80.1.1 / §80.1.2 —— 三节自身从不是任何绑定表行的「家章小节」。
        let Some(body) = find_section_body(&lines, &headings, &fr.section_num) else {
            bad.push(format!("§{}: 家章小节标题行不存在", fr.section_num));
            continue;
        };
        let a = extract_ids_matching(&body, &cp);
        let empty = BTreeSet::new();
        let b = b_by_section.get(&fr.section_num).unwrap_or(&empty);
        let rt: BTreeSet<&String> = fr.retired.iter().collect();
        let diff: Vec<&String> = a
            .iter()
            .filter(|id| !b.contains(id.as_str()) && !rt.contains(id))
            .collect();
        if !diff.is_empty() {
            bad.push(format!("§{}: A\\(B∪Rt) 非空 = {:?}", fr.section_num, diff));
        }
    }
    if bad.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(bad)
    }
}

/// ② §80.1 表里每个 §…#ID 锚，其 (小节, id族) 必须在绑定表里有行.
fn g80_24_check2(families: &[FamilyRow], gate_rows: &[GateRow]) -> Verdict {
    let compiled: Vec<(String, String, Option<CompiledPattern>)> = families
        .iter()
        .map(|f| {
            (
                f.section_num.clone(),
                f.pattern_raw.clone(),
                compile_family_pattern(&f.pattern_raw),
            )
        })
        .collect();
    let mut bad = Vec::new();
    for r in gate_rows {
        let Some((crit, err)) = resolved_units_for_row(r) else {
            continue;
        };
        for unit in crit.into_iter().chain(err) {
            let AnchorUnit::Section { num, id: Some(id) } = unit else {
                continue;
            };
            match compiled.iter().find(|(n, _, _)| n == &num) {
                None => bad.push(format!("{}: §{num}#{id} 在绑定表无该小节行", r.id)),
                Some((_, _, None)) => {
                    bad.push(format!("{}: §{num}#{id} 绑定行的族正则无法解析", r.id))
                }
                Some((_, pat_raw, Some(cp))) => {
                    if !pattern_matches(cp, &id) {
                        bad.push(format!(
                            "{}: §{num}#{id} 不匹配该小节绑定的族正则 {pat_raw:?}",
                            r.id
                        ));
                    }
                }
            }
        }
    }
    if bad.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(bad)
    }
}

/// §57.1 覆盖校验 ①②③（归入 G80-24③，判据 §57.1）：① 每个 G80-* 在「本期起必过」列恰好
/// 出现一次（按子项分期的 family 见 [`subitem_families`]）；② §46「Repository 第一天启用」
/// 代码块里每一行同样恰好出现一次；③ 本表引用的每个 G80-* 必须在 §80.1 有行.
///
/// §57.1 第2条: `not_applicable` 的唯一合法理由是「被测对象在当前 Phase 尚未交付」——
/// §57.1 与 §80.1 都是冻结章，本表本身不存在只可能是被删/被改坏，不是这种对象，因此这里
/// 缺表判 Fail 而不是 NotApplicable（否则 fail-open：缺表 ⇒ 单条 NotApplicable ⇒
/// `report` 对 NotApplicable 返回 false ⇒ 进程 EXIT=0）.
fn g80_24_check3(text: &str, gate_rows: &[GateRow]) -> Verdict {
    let phase_rows = match parse_phase_table(text) {
        Ok(r) => r,
        Err(e) => return Verdict::Fail(vec![e]),
    };
    let subitems = subitem_families(text);
    let declared: BTreeSet<String> = gate_rows.iter().map(|r| r.id.clone()).collect();
    let mut bad = Vec::new();
    for row in gate_rows {
        let id = &row.id;
        // A registry row marked cross-phase remains registered and CI-enforced, but is not
        // required to appear in a single Phase exit column.
        if row.cross_phase {
            continue;
        }
        if let Some(expected) = subitems.get(id) {
            let mut counts: BTreeMap<String, u32> = BTreeMap::new();
            let prefix = format!("{id}.");
            for pr in &phase_rows {
                for tok in backtick_tokens(&pr.must_pass_raw) {
                    if let Some(sub) = tok.strip_prefix(prefix.as_str()) {
                        *counts.entry(sub.to_string()).or_default() += 1;
                    }
                }
            }
            for sub in expected {
                match counts.get(sub).copied().unwrap_or(0) {
                    1 => {}
                    0 => bad.push(format!("{id}.{sub}: 未定生效期（0 次）")),
                    n => bad.push(format!("{id}.{sub}: 出现 {n} 次（应恰 1 次）")),
                }
            }
            for sub in counts.keys() {
                if !expected.contains(sub) {
                    bad.push(format!(
                        "{id}.{sub}: 子项不在该 family 全部子项集合内（{expected:?}）"
                    ));
                }
            }
        } else {
            let count = phase_rows
                .iter()
                .flat_map(|pr| backtick_tokens(&pr.must_pass_raw))
                .filter(|tok| tok == id)
                .count();
            match count {
                1 => {}
                0 => bad.push(format!("{id}: 未定生效期（0 次）")),
                n => bad.push(format!("{id}: 出现 {n} 次（应恰 1 次）")),
            }
        }
    }

    // ② §46「Repository 第一天启用」代码块里每一行同样恰好出现一次.
    match repo_day_one_items(text) {
        Ok(items) => {
            let mut counts: BTreeMap<String, u32> = BTreeMap::new();
            for pr in &phase_rows {
                for tok in backtick_tokens(&pr.must_pass_raw) {
                    *counts.entry(tok).or_default() += 1;
                }
            }
            for item in &items {
                match counts.get(item).copied().unwrap_or(0) {
                    1 => {}
                    0 => bad.push(format!("§46 `{item}`: 未在本期起必过列出现（0 次）")),
                    n => bad.push(format!("§46 `{item}`: 出现 {n} 次（应恰 1 次）")),
                }
            }
        }
        Err(e) => bad.push(format!("§46: {e}")),
    }

    // ③ 本表引用的每个 G80-* 必须在 §80.1 有行；引用不存在的 gate_id ⇒ 红. 按子项分期的
    // token（如 `G80-17.D2`）先剥掉子项后缀，恢复成 family id 再比对.
    let mut referenced: BTreeSet<String> = BTreeSet::new();
    for pr in &phase_rows {
        for tok in backtick_tokens(&pr.must_pass_raw) {
            if is_valid_gate_id(&tok) {
                referenced.insert(tok);
            } else if let Some((base, _sub)) = tok.split_once('.')
                && is_valid_gate_id(base)
            {
                referenced.insert(base.to_string());
            }
        }
    }
    for id in &referenced {
        if !declared.contains(id) {
            bad.push(format!("{id}: §57.1 引用但 §80.1 无此登记"));
        }
    }

    if bad.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(bad)
    }
}

// ============================================================================
// entry point
// ============================================================================

/// Runs all 8 sub-checks (G80-23①–⑤, G80-24①–③) against `text`. If the §80.1 registry table
/// or §80.1.2 binding table itself cannot be located, that is reported as a single `Fail`
/// (missing object named) rather than 8 confusing duplicate failures — every sub-check below
/// depends on one or both of those tables existing.
///
/// §57.1 第2条: `not_applicable` 的唯一合法理由是「被测对象在当前 Phase 尚未交付」——
/// §80.1/§80.1.2 是冻结章且这两道闸 Phase 0 起必过，表本身不存在只可能是被删/被改坏，用
/// NotApplicable 会 fail-open（`report` 对 NotApplicable 返回 false ⇒ 进程 EXIT=0）.
pub fn run_all(text: &str) -> Vec<(&'static str, Verdict)> {
    let gate_rows = match parse_gate_table(text) {
        Ok(r) => r,
        Err(missing) => {
            return vec![(
                "gate-registry (§80.1 registry table)",
                Verdict::Fail(vec![missing]),
            )];
        }
    };
    // 坑5「零不等于没扫到」: parse_gate_table's row filter (`id.starts_with("G80-")`) has no
    // lower bound of its own — a harmless-looking table-header/id-format drift in §80.1 (e.g.
    // a stray prefix on every id) silently shrinks it to 0 rows, and 0 rows means every
    // sub-check below trivially reports Pass over an empty set (vacuous truth) instead of
    // catching that the table itself stopped being readable.
    if gate_rows.is_empty() {
        return vec![(
            "gate-registry (§80.1 registry table)",
            Verdict::Fail(vec![
                "§80.1 registry table matched 0 rows — parse_gate_table's row filter no longer \
                 recognizes the table (坑5: 零不等于没扫到)"
                    .to_string(),
            ]),
        )];
    }
    let family_rows = match parse_family_table(text) {
        Ok(r) => r,
        Err(missing) => {
            return vec![(
                "gate-registry (§80.1.2 id-family binding table)",
                Verdict::Fail(vec![missing]),
            )];
        }
    };
    vec![
        (
            "G80-23① 单元格整体匹配锚文法（§80.1.2）",
            g80_23_check1(&gate_rows),
        ),
        (
            "G80-23② #ID 锚：标题存在且 id 逐字出现（§80.1.2）",
            g80_23_check2(text, &gate_rows),
        ),
        (
            "G80-23③ 无ID锚：标题行存在（§80.1.2）",
            g80_23_check3(text, &gate_rows),
        ),
        (
            "G80-23④ 注错出处含「注错/注入」（§80.1.2）",
            g80_23_check4(text, &gate_rows),
        ),
        (
            "G80-23⑤ NOT_ADMITTED 行数（§80.1.2）",
            g80_23_check5(&gate_rows),
        ),
        (
            "G80-24① id族绑定表 A\\(B∪Rt)（§80.1.2）",
            g80_24_check1(text, &family_rows, &gate_rows),
        ),
        (
            "G80-24② §…#ID 锚的(小节,id族)有绑定行（§80.1.2）",
            g80_24_check2(&family_rows, &gate_rows),
        ),
        (
            "G80-24③ §57.1 本期起必过恰一次（§57.1）",
            g80_24_check3(text, &gate_rows),
        ),
    ]
}

fn report(name: &str, verdict: &Verdict) -> bool {
    match verdict {
        Verdict::Pass => {
            println!("gate-registry: pass — {name}");
            false
        }
        Verdict::Fail(details) => {
            eprintln!("gate-registry: fail — {name}");
            for d in details {
                eprintln!("  {d}");
            }
            true
        }
    }
}

pub fn run(_args: &[String]) -> i32 {
    let spec_path = workspace_root().join(SPEC_PATH);
    let text = match fs::read_to_string(&spec_path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!(
                "gate-registry: fail — cannot read {}: {e}",
                spec_path.display()
            );
            return 1;
        }
    };
    let mut had_fail = false;
    for (name, verdict) in &run_all(&text) {
        if report(name, verdict) {
            had_fail = true;
        }
    }
    if had_fail { 1 } else { 0 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn real_spec_path() -> PathBuf {
        workspace_root().join(SPEC_PATH)
    }

    fn real_spec_text() -> String {
        fs::read_to_string(real_spec_path()).expect("spec must be readable")
    }

    /// Process-wide counter appended to every tempdir name below: `cargo test` runs these in
    /// parallel threads, and nanosecond `SystemTime` resolution alone is not always fine
    /// enough on every platform to keep two concurrent tempdir names from colliding.
    static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

    fn tmp_spec_copy(tag: &str) -> PathBuf {
        let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "gate-registry-{tag}-{}-{}-{seq}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let dest = dir.join("spec.md");
        fs::copy(real_spec_path(), &dest).unwrap();
        dest
    }

    /// Applies one exact string replacement to a tempdir copy of the real spec and returns
    /// the mutated text — the §80.1.2 硬规则③ pattern: never mutate the repo's own file.
    fn mutate(needle: &str, replacement: &str) -> String {
        let path = tmp_spec_copy("fault");
        let original = fs::read_to_string(&path).unwrap();
        assert!(
            original.contains(needle),
            "fixture assumption broken: needle {needle:?} not found in real spec — the spec \
             text this test targets has moved, update the mutation"
        );
        let mutated = original.replacen(needle, replacement, 1);
        fs::remove_dir_all(path.parent().unwrap()).ok();
        mutated
    }

    fn fails(results: &[(&str, Verdict)], label_substr: &str) -> bool {
        results
            .iter()
            .any(|(name, v)| name.contains(label_substr) && matches!(v, Verdict::Fail(_)))
    }

    // -- id / anchor grammar unit tests -----------------------------------------------------

    #[test]
    fn is_valid_gate_id_accepts_real_spec_shapes() {
        // G6-DB1/G6-DB2 (G80-40's real §6.2.3 anchors) are the reason this isn't the narrower
        // 4-shape closed set literally spelled out in §80.1.2 — see is_valid_gate_id's doc.
        for ok in [
            "G0", "G80", "G80-23", "G23-1a", "INV-1", "D2", "G6-DB1", "G6-DB2",
        ] {
            assert!(is_valid_gate_id(ok), "{ok}");
        }
        for bad in ["INV", "D", "g0", "G80-", "G--1", "G1-", "GX"] {
            assert!(!is_valid_gate_id(bad), "{bad}");
        }
    }

    #[test]
    fn parse_anchor_cell_rejects_prose_appended_to_an_anchor() {
        assert!(parse_anchor_cell("§55.1", false).is_some());
        assert!(parse_anchor_cell("§55.1（全 workspace 计数 == 1）", false).is_none());
    }

    #[test]
    fn parse_anchor_cell_rejects_wrong_divider() {
        assert!(parse_anchor_cell("§1.12 · §1.3", false).is_some());
        assert!(parse_anchor_cell("§1.12·§1.3", false).is_none());
        assert!(parse_anchor_cell("§1.12, §1.3", false).is_none());
    }

    #[test]
    fn parse_anchor_cell_same_left_only_allowed_where_flagged() {
        assert!(parse_anchor_cell("同左", true).is_some());
        assert!(parse_anchor_cell("同左", false).is_none());
    }

    #[test]
    fn compiled_pattern_matches_family_shapes() {
        let dash_letter = compile_family_pattern("G23-\\d+[a-z]?").unwrap();
        assert!(pattern_matches(&dash_letter, "G23-1"));
        assert!(pattern_matches(&dash_letter, "G23-1a"));
        assert!(!pattern_matches(&dash_letter, "G23-1ab"));
        assert!(!pattern_matches(&dash_letter, "G24-1"));

        let bare_g = compile_family_pattern("G\\d").unwrap();
        assert!(pattern_matches(&bare_g, "G0"));
        assert!(!pattern_matches(&bare_g, "G6-DB1"));

        let literal = compile_family_pattern("G80-42").unwrap();
        assert!(pattern_matches(&literal, "G80-42"));
        assert!(!pattern_matches(&literal, "G80-4"));
    }

    // -- real repo baseline: both gates must be green on the frozen spec --------------------

    /// §80.1.2 states "这两道闸在冻结时就是绿的" — this asserts exactly that (all 8
    /// sub-checks Pass), not a softened "6 Pass + 2 known Fail" baseline. A prior version of
    /// this test baked two live spec-content gaps in as accepted exceptions; that is the same
    /// shape §80.1.2 itself criticizes about the retired G80-21 ("一道闸落地即红、只能靠调松
    /// 定义活下去") — it makes `cargo test` report green while `cargo run -p xtask --
    /// gate-registry` (the real merge gate) reports EXIT=1 on the same content, and a green
    /// test suite sitting on top of a red gate is a false all-clear signal.
    ///
    /// **This test is therefore expected to fail red right now** — that failure is correct,
    /// not a regression in the checker. The two live gaps are genuine spec-content bugs, not
    /// parser bugs:
    ///
    /// - G80-23②: §80.1's G80-14 row anchors `§59.1#G59-6`, but `### G59-6 / Private Authority
    ///   Boundary` is defined under `# 10. Authority Contract` (`docs/architecture/
    ///   Baseline_2.9.md:1904`), not inside §59.1 (whose body is G59-1..G59-5 only,
    ///   `:10121`-`:10230`). Fix: move the `### G59-6` block into §59.1, or repoint the G80-14
    ///   anchor at `:13756`.
    /// - G80-24①: §20's body declares `### G20-1 Snapshot Selection` (`:4441`, inside §20.4),
    ///   but no §80.1 row anchors `§20#G20-1` — G80-32 (`:13773`) anchors bare `§20.4` (no
    ///   `#ID`), which doesn't register as covering `G20-1` for the §20 family. Fix: change
    ///   that cell to `§20#G20-1 · §20.4` (not `§20.4#G20-1` — the binding table only has a
    ///   `§20` row, so that shape would newly trip G80-24②).
    ///
    /// Both fixes are edits to `docs/architecture/Baseline_2.9.md`, which is out of scope for
    /// this file (`xtask/src/gate_registry.rs` only) — **blocked pending that spec edit**, not
    /// fixable from this module without loosening a check to paper over live content bugs
    /// (which this module must not do). Once the spec is corrected upstream this test starts
    /// passing with no further code change; do not re-add a per-finding exception list here to
    /// force it green early.
    #[test]
    fn real_spec_green_on_frozen_spec() {
        let text = real_spec_text();
        let results = run_all(&text);
        assert_eq!(results.len(), 8, "{results:?}");
        for (name, verdict) in &results {
            assert_eq!(*verdict, Verdict::Pass, "{name} was not Pass: {verdict:?}");
        }
    }

    // -- G80-23 注错 a-f (§80.1.2) ------------------------------------------------------------

    /// 注错 a: G80-5 的 §23.4#G23-1a 改成 §23.4#G23-1z ⇒ §23.4 内无此 id ⇒ ② 红.
    #[test]
    fn fault_a_g23_1a_renamed_to_g23_1z_is_check2_red() {
        let text = mutate("§23.4#G23-1a", "§23.4#G23-1z");
        assert!(fails(&run_all(&text), "G80-23②"));
    }

    /// 注错 b: G80-2 判据出处格里补一句散文 ⇒ 单元格不再整体匹配锚文法 ⇒ ① 红.
    #[test]
    fn fault_b_prose_appended_to_criterion_cell_is_check1_red() {
        let text = mutate(
            "| G80-2 唯一构造点 | PR | §55.1 | §80.1.1 |",
            "| G80-2 唯一构造点 | PR | §55.1（全 workspace 计数 == 1） | §80.1.1 |",
        );
        assert!(fails(&run_all(&text), "G80-23①"));
    }

    /// 注错 c: §53.6 标题改成 §53.7 ⇒ G80-9 的 §53.6 锚定位不到标题行 ⇒ ③ 红.
    #[test]
    fn fault_c_heading_renamed_is_check3_red() {
        let text = mutate(
            "## 53.6 fail-closed 侧与 direction table 的完备性",
            "## 53.7 fail-closed 侧与 direction table 的完备性",
        );
        assert!(fails(&run_all(&text), "G80-23③"));
    }

    /// 注错 d: §37.2 里的注错句整句删掉，判据留着 ⇒ 正文不再含「注错」「注入」⇒ ④ 红.
    #[test]
    fn fault_d_removing_fault_sentence_is_check4_red() {
        // The needle stops before the arithmetic on purpose: the column count in that
        // sentence moves every time §15.1's DDL gains a column (0167 took it 12 -> 14), and a
        // fixture that quotes the numbers goes red on a spec edit that did nothing wrong.
        // Dropping the 注错 token alone is what check ④ actually observes.
        let text = mutate("注错：加一列 `deleted_count bigint`", "");
        assert!(fails(&run_all(&text), "G80-23④"));
    }

    /// 注错 e: G80-7 的注错出处改成 — ⇒ NOT_ADMITTED 行数 0 → 1 ⇒ ⑤ 红.
    #[test]
    fn fault_e_error_cell_dash_is_check5_red() {
        let text = mutate(
            "| G80-7 `ops.column_vitality` | Nightly | §9.1 | §80.1.1 |",
            "| G80-7 `ops.column_vitality` | Nightly | §9.1 | — |",
        );
        assert!(fails(&run_all(&text), "G80-23⑤"));
    }

    /// 注错 f: G80-31 的注错出处从「同左」改回旧锚 §25.4 ⇒ §25.4 正文不含「注错/注入」⇒ ④ 红.
    #[test]
    fn fault_f_stale_error_anchor_is_check4_red() {
        let text = mutate(
            "| G80-31 Mandatory Context non-eviction | PR · e2e | §25.5#G25-1 | 同左 |",
            "| G80-31 Mandatory Context non-eviction | PR · e2e | §25.5#G25-1 | §25.4 |",
        );
        assert!(fails(&run_all(&text), "G80-23④"));
    }

    // -- G80-24 注错 f-i (§80.1.2) -------------------------------------------------------------
    //
    // f/g/h below use small synthetic spec-shaped text + hand-built rows instead of mutating
    // a real-spec tempdir copy: the fault each demonstrates needs to be isolated from every
    // *other* real §23.4/§59.1 text this repo happens to also contain (§80.1.2 家章's own
    // 注错-g narrative explicitly notes real §59.1 prose repeats "G59-2" outside its 验收闸
    // block, which would mask the fault if injected into the real file) — a purpose-built
    // fixture proves the exact same check function transitions red without that noise. This
    // still respects 硬规则③ (never mutate the tracked repo file): these fixtures are pure
    // in-memory strings, no file touched at all, real or temporary.

    /// 注错 f: §23.4 新增 G23-7 而不改 §80.1 ⇒ A 由 9 变 10、B 仍 8、Rt 仍 1 ⇒ ① 差集
    /// {G23-7} ⇒ 红. 正对照：去掉那句新增文本的同款 fixture 先证明是绿的.
    #[test]
    fn fault_g24_f_undeclared_new_id_is_check1_red() {
        let gate_rows = vec![GateRow {
            id: "G80-5".to_string(),
            cross_phase: false,
            criterion_raw: "§23.4#G23-1a".to_string(),
            error_raw: "同左".to_string(),
        }];
        let families = vec![FamilyRow {
            section_num: "23.4".to_string(),
            pattern_raw: "G23-\\d+[a-z]?".to_string(),
            retired: vec!["G23-1".to_string()],
        }];

        let clean = "## 23.4 可判定 gate\n\nG23-1a 分母外生。\n\n## 23.5 next\n";
        assert_eq!(g80_24_check1(clean, &families, &gate_rows), Verdict::Pass);

        let text = "## 23.4 可判定 gate\n\nG23-1a 分母外生。G23-7 是本测试新增、未在 §80.1 登记的 id。\n\n## 23.5 next\n";
        assert!(matches!(
            g80_24_check1(text, &families, &gate_rows),
            Verdict::Fail(_)
        ));
    }

    /// 注错 g: §59.1 家章正文里的 G59-2 整条删掉，但 §80.1 仍锚定 §59.1#G59-2 ⇒ **G80-23②
    /// 红**（§80.1.2 原文强调这条示范的正是 G80-23②，不是 G80-24 系checks — "这条同时是
    /// 两道闸不可合并的证据"）. 正对照：G59-2 还在正文时先证明是绿的.
    #[test]
    fn fault_g24_g_removing_family_member_still_referenced_is_g80_23_check2_red() {
        let rows = vec![GateRow {
            id: "G80-14".to_string(),
            cross_phase: false,
            criterion_raw: "§59.1#G59-1 · §59.1#G59-2".to_string(),
            error_raw: "同左".to_string(),
        }];

        let clean = "## 59.1 Authority 冻结契约\n\nG59-1 全序无平局。G59-2 Public 不覆盖。\n\n## 59.2 next\n";
        assert_eq!(g80_23_check2(clean, &rows), Verdict::Pass);

        let text = "## 59.1 Authority 冻结契约\n\nG59-1 全序无平局。\n\n## 59.2 next\n";
        assert!(matches!(g80_23_check2(text, &rows), Verdict::Fail(_)));
    }

    /// 注错 h: 在 §65 写一道带 id 的闸 G65-1 并在 §80.1 登记锚 §65#G65-1，绑定表不加 §65 行
    /// ⇒ G80-24② 红. 正对照：绑定表有 §65 行时先证明是绿的.
    #[test]
    fn fault_g24_h_anchor_section_missing_from_binding_table_is_check2_red() {
        let rows = vec![GateRow {
            id: "G80-98".to_string(),
            cross_phase: false,
            criterion_raw: "§65#G65-1".to_string(),
            error_raw: "同左".to_string(),
        }];

        let with_binding = vec![FamilyRow {
            section_num: "65".to_string(),
            pattern_raw: "G65-\\d".to_string(),
            retired: vec![],
        }];
        assert_eq!(g80_24_check2(&with_binding, &rows), Verdict::Pass);

        let families: Vec<FamilyRow> = vec![]; // 绑定表完全没有 §65 这一行
        assert!(matches!(g80_24_check2(&families, &rows), Verdict::Fail(_)));
    }

    /// 注错 i: 从 §57.1 Phase 8 行删掉 G80-31 ⇒ ③ 计数 1 → 0 ⇒ 红.
    #[test]
    fn fault_g24_i_removing_phase_entry_is_check3_red() {
        let text = mutate("`G80-31`", "");
        assert!(fails(&run_all(&text), "G80-24③"));
    }

    /// 正对照 for 注错 i: doubling the Phase-8 entry (0→1→2) must also be red, proving the
    /// check watches both directions, not just absence. Pure in-memory string mutation, no
    /// tempdir file involved (never touches the real spec file on disk either way).
    #[test]
    fn fault_g24_i_duplicating_phase_entry_is_check3_red() {
        let real = real_spec_text();
        let doubled = real.replacen("| `G80-31` `G80-46` |", "| `G80-31` `G80-31` `G80-46` |", 1);
        assert_ne!(
            doubled, real,
            "fixture assumption broken: Phase 8 row text has moved"
        );
        assert!(fails(&run_all(&doubled), "G80-24③"));
    }

    #[test]
    fn cross_phase_marker_allows_registered_gate_without_phase_exit_entry() {
        let real = real_spec_text();
        let rows = parse_gate_table(&real).expect("real registry parses");
        assert_eq!(g80_24_check3(&real, &rows), Verdict::Pass);
    }

    #[test]
    fn removing_cross_phase_marker_requires_phase_exit_entry() {
        let real = real_spec_text();
        let rows = parse_gate_table(&real.replace(
            "G80-44 R4 fault manifest closure (cross-phase)",
            "G80-44 R4 fault manifest closure",
        ))
        .expect("mutated registry parses");
        assert!(fails(
            &run_all(&real.replace(
                "G80-44 R4 fault manifest closure (cross-phase)",
                "G80-44 R4 fault manifest closure",
            )),
            "G80-24③"
        ));
        assert!(matches!(g80_24_check3(&real, &rows), Verdict::Fail(_)));
    }

    #[test]
    fn arbitrary_cross_phase_marked_id_is_not_hardcoded_to_g80_44() {
        let real = real_spec_text().replace(
            "G80-44 R4 fault manifest closure (cross-phase)",
            "G80-98 arbitrary closure (cross-phase)",
        );
        let rows = parse_gate_table(&real).expect("mutated registry parses");
        assert_eq!(g80_24_check3(&real, &rows), Verdict::Pass);
    }
}
