//! `xtask::direction_table` — G80-9: §53.6 direction table generated-vs-handwritten parity.
//! Depends-on: crates=[]; services=[]; env=[CARGO_MANIFEST_DIR]; modules=[xtask::architecture_check]
//! Called-by: [xtask::main]
//! Invariants: [generated table (DegradeCode variants + #[fail_closed] scan) must set-equal telemetry::direction::DIRECTION_TABLE]
//! Spec: Baseline §53.6; §53.2
//!
//! xtask `direction-table` — G80-9: §53.6 direction table generated-vs-handwritten parity.
//!
//! Generation source: `DegradeCode`'s variant list (§53.2, fail-open side) + every
//! `#[fail_closed(threat = "...")]`-annotated function found anywhere in the workspace
//! (fail-closed side, §53.6). Compared against the handwritten
//! `telemetry::direction::DIRECTION_TABLE` (crates/telemetry/src/direction.rs) — a mismatch
//! is red. Text-level source scanning, same style as every other xtask gate in this repo
//! (see `xtask/src/architecture_check.rs`); judgment stays in the family chapter, this file
//! only cites § numbers.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use crate::architecture_check::parse_degrade_variant_names;

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

/// This gate's three-state verdict (§57.1: all gates pass/fail/not_applicable).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Fail(Vec<String>),
    NotApplicable(String),
}

/// This checker's own path, relative to workspace root — excluded from the fail-closed
/// workspace scan below for the same reason `architecture_check.rs::SELF_FILE` excludes
/// itself there: this file's own `#[cfg(test)]` fixtures necessarily contain the literal
/// `#[fail_closed(` text the scan hunts for.
const SELF_FILE: &str = "xtask/src/direction_table.rs";

/// The macro crate's own directory — excluded from the same scan for the identical reason:
/// its `tests/compile_fail.rs` builds fixture source containing the literal attribute text
/// as a Rust *string literal* (to feed a throwaway `cargo build`), which this text-only
/// scanner cannot tell apart from a real annotation the way a token-aware parser could.
const FAIL_CLOSED_MACRO_DIR: &str = "crates/fail-closed-macro/";

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Whether the line containing byte offset `idx` in `source` is a `//` comment line —
/// same guard `architecture_check.rs::line_is_comment_at` uses, duplicated here (private
/// to that module) so a spec-quoting doc comment never counts as a real annotation hit.
fn line_is_comment_at(source: &str, idx: usize) -> bool {
    let line_start = source[..idx].rfind('\n').map(|i| i + 1).unwrap_or(0);
    source[line_start..]
        .lines()
        .next()
        .unwrap_or("")
        .trim_start()
        .starts_with("//")
}

fn display(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}

/// Recursively collects every `.rs` file under `dir`, skipping `target`/`.git`/`node_modules`
/// (same exclusion list as `architecture_check.rs::walk_files`, duplicated because that
/// helper is private to its own module).
fn walk_rs_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk_rs_files_rec(dir, &mut out);
    out
}

fn walk_rs_files_rec(dir: &Path, out: &mut Vec<PathBuf>) {
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
            walk_rs_files_rec(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

// ============================================================================
// fail-open side: DegradeCode variants (§53.2)
// ============================================================================

fn fail_open_rows(root: &Path) -> Result<Vec<(String, String)>, String> {
    let path = root.join("crates/telemetry/src/degrade.rs");
    let source =
        fs::read_to_string(&path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let variants = parse_degrade_variant_names(&source);
    if variants.is_empty() {
        return Err(format!(
            "no DegradeCode variants parsed from {}",
            path.display()
        ));
    }
    Ok(variants
        .into_iter()
        .map(|v| (v, "FailOpen".to_string()))
        .collect())
}

// ============================================================================
// fail-closed side: #[fail_closed(threat = "...")] scan (§53.6)
// ============================================================================

/// `text` after word-bounded `key`, trimmed of a following `sep` (`=` for attribute args
/// like `threat = "..."`, `:` for struct-literal fields like `subject: "..."`) and up to
/// the closing `"` — or `None` if `key` isn't present or isn't followed by `sep "..."`.
fn extract_quoted(text: &str, key: &str, sep: char) -> Option<String> {
    let bytes = text.as_bytes();
    let mut start = 0usize;
    while let Some(rel) = text[start..].find(key) {
        let idx = start + rel;
        let before_ok = idx == 0 || !is_ident_byte(bytes[idx - 1]);
        let after_idx = idx + key.len();
        let after_ok = after_idx >= bytes.len() || !is_ident_byte(bytes[after_idx]);
        if before_ok && after_ok {
            let after = text[after_idx..].trim_start();
            if let Some(after_sep) = after.strip_prefix(sep) {
                let after_sep = after_sep.trim_start();
                if let Some(rest) = after_sep.strip_prefix('"')
                    && let Some(end) = rest.find('"')
                {
                    return Some(rest[..end].to_string());
                }
            }
        }
        start = idx + key.len();
    }
    None
}

/// The name of the first `fn` in `rest` (the text immediately following an attribute's
/// closing `)]`), skipping visibility modifiers, further attributes, and `//` comment lines
/// in between — the shape this codebase actually writes is `#[fail_closed(...)]` directly
/// above the `fn` it annotates.
///
/// ponytail: not a Rust parser — an intervening non-attribute, non-comment, non-visibility
/// line between the attribute and its `fn` (none exists in this codebase today) would
/// defeat it; upgrade to token-level scanning (`proc-macro2`/`syn`) if that ever happens.
fn next_fn_name(rest: &str) -> Option<String> {
    let mut s = rest;
    loop {
        s = s.trim_start();
        if let Some(stripped) = s.strip_prefix("pub(crate)") {
            s = stripped;
        } else if let Some(stripped) = s.strip_prefix("pub") {
            s = stripped;
        } else if s.starts_with("//") {
            s = s.split_once('\n').map(|(_, r)| r).unwrap_or("");
        } else if s.starts_with("#[") {
            let end = s.find(']')?;
            s = &s[end + 1..];
        } else {
            break;
        }
    }
    let s = s.strip_prefix("fn ")?.trim_start();
    let name: String = s.chars().take_while(|c| is_ident_byte(*c as u8)).collect();
    if name.is_empty() { None } else { Some(name) }
}

/// Every `(function name, threat)` pair from `#[fail_closed(threat = "...")]` annotations
/// in `source`. A `#[fail_closed(` with no parseable `threat` string or no following `fn`
/// is silently skipped — the macro itself (`humaux-fail-closed-macro`) is what refuses to
/// *compile* that shape (§53.6), so by the time this scan runs against a real build the
/// shape is guaranteed well-formed; this scan only needs to not crash on a hand-edited
/// fixture that violates that guarantee (the §80.1.1 G80-9 injections do exactly that).
pub fn parse_fail_closed_annotations(source: &str) -> Vec<(String, String)> {
    let needle = "#[fail_closed(";
    let mut out = Vec::new();
    let mut start = 0usize;
    while let Some(rel) = source[start..].find(needle) {
        let idx = start + rel;
        if line_is_comment_at(source, idx) {
            start = idx + needle.len();
            continue;
        }
        let after = idx + needle.len();
        let Some(close_rel) = source[after..].find(")]") else {
            start = after;
            continue;
        };
        let attr_body = &source[after..after + close_rel];
        let rest = &source[after + close_rel + 2..];
        if let (Some(threat), Some(name)) =
            (extract_quoted(attr_body, "threat", '='), next_fn_name(rest))
        {
            out.push((name, threat));
        }
        start = after + close_rel + 2;
    }
    out
}

fn fail_closed_rows(root: &Path) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for path in walk_rs_files(root) {
        let disp = display(root, &path);
        if disp == SELF_FILE || disp.starts_with(FAIL_CLOSED_MACRO_DIR) {
            continue;
        }
        let Ok(source) = fs::read_to_string(&path) else {
            continue;
        };
        for (name, _threat) in parse_fail_closed_annotations(&source) {
            out.push((name, "FailClosed".to_string()));
        }
    }
    out
}

// ============================================================================
// handwritten table parse (crates/telemetry/src/direction.rs)
// ============================================================================

/// Extracts every `(subject, direction)` pair from `direction.rs`'s `DIRECTION_TABLE` array
/// literal — scoped to the array's own byte range (`= &[` .. the next `];`) so nothing
/// outside it (doc comments, unrelated code) is mistaken for a row. Safe because the array
/// holds only `DirectionRow { .. }` literals whose fields (a `&'static str`, an enum path)
/// never themselves contain `[`/`]`.
pub fn parse_handwritten_direction_table(source: &str) -> Option<Vec<(String, String)>> {
    let anchor = "DIRECTION_TABLE: &[DirectionRow] = &[";
    let arr_start = source.find(anchor)? + anchor.len();
    let arr_end = arr_start + source[arr_start..].find("];")?;
    let block = &source[arr_start..arr_end];
    let mut rows = Vec::new();
    let mut search = 0usize;
    while let Some(rel) = block[search..].find("DirectionRow {") {
        let idx = search + rel;
        let close_rel = block[idx..].find('}')?;
        let row_text = &block[idx..idx + close_rel];
        let subject = extract_quoted(row_text, "subject", ':')?;
        let dir_marker = "Direction::";
        let dir_idx = row_text.find(dir_marker)? + dir_marker.len();
        let direction: String = row_text[dir_idx..]
            .chars()
            .take_while(|c| is_ident_byte(*c as u8))
            .collect();
        rows.push((subject, direction));
        search = idx + close_rel;
    }
    Some(rows)
}

// ============================================================================
// comparator
// ============================================================================

/// Pure §53.6/G80-9 comparator: `generated` (fail-open rows + fail-closed rows, freshly
/// scanned from source) must equal `handwritten` (`DIRECTION_TABLE`'s parsed rows) as sets.
/// Reports both directions of mismatch — a row only the generated side has (an annotation
/// or variant the handwritten table forgot) and a row only the handwritten side has (a
/// deleted/renamed variant, or a stale row for a removed annotation) — same "don't collapse
/// to a bare count" shape as `architecture_check.rs::rule2_check`.
pub fn compare(generated: &[(String, String)], handwritten: &[(String, String)]) -> Verdict {
    let gen_set: BTreeSet<(String, String)> = generated.iter().cloned().collect();
    let hand_set: BTreeSet<(String, String)> = handwritten.iter().cloned().collect();
    if gen_set == hand_set && generated.len() == handwritten.len() {
        return Verdict::Pass;
    }
    let missing_in_handwritten: Vec<String> = gen_set
        .difference(&hand_set)
        .map(|(s, d)| format!("{s} ({d})"))
        .collect();
    let missing_in_generated: Vec<String> = hand_set
        .difference(&gen_set)
        .map(|(s, d)| format!("{s} ({d})"))
        .collect();
    let mut details = vec![format!(
        "generated table ({} rows) != handwritten DIRECTION_TABLE ({} rows)",
        generated.len(),
        handwritten.len()
    )];
    if !missing_in_handwritten.is_empty() {
        details.push(format!(
            "row(s) generated from source but missing in handwritten table: {}",
            missing_in_handwritten.join(", ")
        ));
    }
    if !missing_in_generated.is_empty() {
        details.push(format!(
            "row(s) in handwritten table with no matching generation source: {}",
            missing_in_generated.join(", ")
        ));
    }
    Verdict::Fail(details)
}

// ============================================================================
// entry point
// ============================================================================

fn run_check(root: &Path) -> Verdict {
    let generated = match fail_open_rows(root) {
        Ok(mut rows) => {
            rows.extend(fail_closed_rows(root));
            rows
        }
        Err(e) => return Verdict::NotApplicable(format!("degrade.rs::DegradeCode ({e})")),
    };
    let direction_rs = root.join("crates/telemetry/src/direction.rs");
    let Ok(direction_src) = fs::read_to_string(&direction_rs) else {
        return Verdict::NotApplicable(format!(
            "telemetry::direction::DIRECTION_TABLE ({})",
            direction_rs.display()
        ));
    };
    let Some(handwritten) = parse_handwritten_direction_table(&direction_src) else {
        return Verdict::NotApplicable(
            "telemetry::direction::DIRECTION_TABLE array literal (unparseable shape)".to_string(),
        );
    };
    compare(&generated, &handwritten)
}

pub fn run(_args: &[String]) -> i32 {
    let root = workspace_root();
    match run_check(&root) {
        Verdict::Pass => {
            println!(
                "direction-table: pass — §53.6 generated table == handwritten DIRECTION_TABLE"
            );
            0
        }
        Verdict::Fail(details) => {
            eprintln!(
                "direction-table: fail — §53.6 generated table != handwritten DIRECTION_TABLE"
            );
            for d in details {
                eprintln!("  {d}");
            }
            1
        }
        Verdict::NotApplicable(missing) => {
            println!("direction-table: not_applicable (missing object: {missing})");
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn real_root() -> PathBuf {
        workspace_root()
    }

    // -- pure-fixture unit tests -----------------------------------------------------------

    #[test]
    fn parse_fail_closed_annotations_extracts_name_and_threat() {
        let src = "#[fail_closed(threat = \"egress\")]\npub fn guard() -> bool { true }\n";
        let hits = parse_fail_closed_annotations(src);
        assert_eq!(hits, vec![("guard".to_string(), "egress".to_string())]);
    }

    #[test]
    fn parse_fail_closed_annotations_skips_comment_mentions() {
        let src = "// example: #[fail_closed(threat = \"x\")]\nfn real() {}\n";
        assert!(parse_fail_closed_annotations(src).is_empty());
    }

    #[test]
    fn parse_fail_closed_annotations_skips_missing_threat() {
        // Mirrors a hand-edited fixture that violates the macro's own compile-time
        // guarantee (§80.1.1 G80-9 注错 b works by deleting the whole attribute, not just
        // `threat`, but this still must not panic on the degenerate shape).
        let src = "#[fail_closed()]\nfn guard() {}\n";
        assert!(parse_fail_closed_annotations(src).is_empty());
    }

    #[test]
    fn parse_handwritten_direction_table_parses_fixture() {
        let src = "pub const DIRECTION_TABLE: &[DirectionRow] = &[\n\
             DirectionRow { subject: \"A\", direction: Direction::FailOpen },\n\
             DirectionRow { subject: \"guard\", direction: Direction::FailClosed },\n\
             ];\n";
        let rows = parse_handwritten_direction_table(src).unwrap();
        assert_eq!(
            rows,
            vec![
                ("A".to_string(), "FailOpen".to_string()),
                ("guard".to_string(), "FailClosed".to_string()),
            ]
        );
    }

    #[test]
    fn compare_pass_when_equal_as_sets_regardless_of_order() {
        let a = vec![
            ("A".to_string(), "FailOpen".to_string()),
            ("B".to_string(), "FailOpen".to_string()),
        ];
        let b = vec![
            ("B".to_string(), "FailOpen".to_string()),
            ("A".to_string(), "FailOpen".to_string()),
        ];
        assert_eq!(compare(&a, &b), Verdict::Pass);
    }

    #[test]
    fn compare_reports_row_missing_in_handwritten() {
        let generated = vec![
            ("A".to_string(), "FailOpen".to_string()),
            ("B".to_string(), "FailOpen".to_string()),
        ];
        let handwritten = vec![("A".to_string(), "FailOpen".to_string())];
        let v = compare(&generated, &handwritten);
        assert!(
            matches!(&v, Verdict::Fail(d) if d.iter().any(|l| l.contains('B'))),
            "{v:?}"
        );
    }

    #[test]
    fn compare_reports_row_missing_in_generated() {
        let generated = vec![("A".to_string(), "FailOpen".to_string())];
        let handwritten = vec![
            ("A".to_string(), "FailOpen".to_string()),
            ("B".to_string(), "FailOpen".to_string()),
        ];
        let v = compare(&generated, &handwritten);
        assert!(
            matches!(&v, Verdict::Fail(d) if d.iter().any(|l| l.contains('B'))),
            "{v:?}"
        );
    }

    // -- real repo -------------------------------------------------------------------------

    /// §80.1 准入条件的另一半：真实代码今天必须是绿的，不是靠样本硬凑出来的绿。
    #[test]
    fn real_repo_direction_table_is_green() {
        let verdict = run_check(&real_root());
        assert_eq!(verdict, Verdict::Pass, "{verdict:?}");
    }

    // -- tempdir fixtures for the two §80.1.1 G80-9 injections ------------------------------
    //
    // Task rule 3: injection tests use a tempdir copy, never the real repo files. Only
    // `crates/telemetry/src/{degrade,direction}.rs` are copied — `fail_open_rows` reads
    // only the first, and `fail_closed_rows`'s workspace walk is scoped to whatever `root`
    // is passed in, so a tempdir containing just this subtree is a complete, self-contained
    // scan domain for `run_check`.

    fn tempdir_copy(name: &str) -> PathBuf {
        let tmp = std::env::temp_dir().join(format!(
            "direction-table-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let src_dir = real_root().join("crates/telemetry/src");
        let dst_dir = tmp.join("crates/telemetry/src");
        fs::create_dir_all(&dst_dir).unwrap();
        for f in ["degrade.rs", "direction.rs"] {
            fs::copy(src_dir.join(f), dst_dir.join(f)).unwrap();
        }
        tmp
    }

    /// 正向对照：an unmodified tempdir copy of the real subtree must still be green — proves
    /// the two injections below go red because of the mutation, not because tempdir copying
    /// itself broke something (same pairing as
    /// `architecture_check.rs::rule3_fixture_copy_at_correct_path_is_green`).
    #[test]
    fn tempdir_copy_unmodified_is_green() {
        let tmp = tempdir_copy("green");
        let verdict = run_check(&tmp);
        assert_eq!(verdict, Verdict::Pass, "{verdict:?}");
        fs::remove_dir_all(&tmp).ok();
    }

    /// Removes the `DirectionRow { .. }` literal whose text contains `subject_needle`
    /// (including its trailing comma), format-independent — locates the row the same way
    /// [`parse_handwritten_direction_table`] does, rather than matching an exact rustfmt
    /// output string.
    fn remove_row_containing(source: &str, subject_needle: &str) -> Option<String> {
        let mut search = 0usize;
        while let Some(rel) = source[search..].find("DirectionRow {") {
            let idx = search + rel;
            let close_rel = source[idx..].find('}')?;
            let end = idx + close_rel + 1;
            if source[idx..end].contains(subject_needle) {
                let mut real_end = end;
                if let Some(comma_rel) = source[end..].find(',')
                    && source[end..end + comma_rel].trim().is_empty()
                {
                    real_end = end + comma_rel + 1;
                }
                return Some(format!("{}{}", &source[..idx], &source[real_end..]));
            }
            search = end;
        }
        None
    }

    /// §80.1.1 G80-9 注错 a: "在手写 direction table 里删一行 ⇒ 生成表与手写表不等 ⇒ 红".
    #[test]
    fn g80_9_fault_a_handwritten_row_deleted_is_red() {
        let tmp = tempdir_copy("fault-a");
        let direction_path = tmp.join("crates/telemetry/src/direction.rs");
        let src = fs::read_to_string(&direction_path).unwrap();
        let mutated = remove_row_containing(&src, "EgressDenied")
            .expect("fixture row not found — direction.rs shape drifted");
        assert_ne!(mutated, src);
        fs::write(&direction_path, mutated).unwrap();

        let verdict = run_check(&tmp);
        assert!(matches!(verdict, Verdict::Fail(_)), "{verdict:?}");
        fs::remove_dir_all(&tmp).ok();
    }

    /// Removes the `#[fail_closed(...)]` attribute immediately preceding `fn_name`,
    /// format-independent (locates it the same way [`parse_fail_closed_annotations`] does).
    fn remove_fail_closed_attr(source: &str, fn_name: &str) -> Option<String> {
        let needle = "#[fail_closed(";
        let mut start = 0usize;
        while let Some(rel) = source[start..].find(needle) {
            let idx = start + rel;
            let after = idx + needle.len();
            let close_rel = source[after..].find(")]")?;
            let attr_end = after + close_rel + 2;
            if next_fn_name(&source[attr_end..]).as_deref() == Some(fn_name) {
                let mut real_end = attr_end;
                if source[attr_end..].starts_with('\n') {
                    real_end += 1;
                }
                return Some(format!("{}{}", &source[..idx], &source[real_end..]));
            }
            start = attr_end;
        }
        None
    }

    /// §80.1.1 G80-9 注错 b: "给一个 fail-closed 函数去掉 #[fail_closed(threat=…)] ⇒
    /// 生成侧少一行 ⇒ 红".
    #[test]
    fn g80_9_fault_b_fail_closed_annotation_removed_is_red() {
        let tmp = tempdir_copy("fault-b");
        let direction_path = tmp.join("crates/telemetry/src/direction.rs");
        let src = fs::read_to_string(&direction_path).unwrap();
        let mutated = remove_fail_closed_attr(&src, "example_fail_closed_check")
            .expect("fixture annotation not found — direction.rs shape drifted");
        assert_ne!(mutated, src);
        fs::write(&direction_path, mutated).unwrap();

        let verdict = run_check(&tmp);
        assert!(matches!(verdict, Verdict::Fail(_)), "{verdict:?}");
        fs::remove_dir_all(&tmp).ok();
    }
}
