//! xtask `architecture-check` — the CI gate family for §53.3 (Outcome fallback rule +
//! positive sentinels), §55.1 / §48.0① / §59.1 (unique construction points), §78.3
//! (Workspace Dependency Rule), the §78 Architecture Boundary Lints, and §52.4 G52-1.
//!
//! Every sub-check below is independent and prints its own three-state verdict
//! (`pass` / `fail` / `not_applicable`); `not_applicable` always names the missing object
//! (§57.1 第2条). The process exit code is 1 iff at least one sub-check `fail`ed —
//! `not_applicable` never fails the build, it is a legitimate state for an object whose
//! delivering Phase has not landed yet (§80.1.1).
//!
//! §80.1.1 registers the injected-fault records for the rules that don't yet have a home
//! in their own spec section: G80-1 (§53.3 规则3), G80-2 (§55.1), G80-4 (§16.2, out of this
//! task's scope), G80-20 (§67.4, out of this task's scope). G80-22 lives at §48.0①. G59-3
//! lives at §59.1. Each rule below cites its own §.
//!
//! G52-1 / G59-3 / G80-2 / G80-22 scan the **whole workspace root** (`crates/` + `bins/` +
//! `evals/` + `xtask/` + `migrations/`), not just `crates/`: §80.1.1 / §48.0① / §59.1 / §52.4
//! all name `bins/*` / `evals/*` as required injection/scan territory. [`walk_workspace_rs`]
//! is the shared scan domain for those four — it excludes this file itself ([`SELF_FILE`]) and
//! strips every file's own `#[cfg(test)]` module ([`strip_cfg_test_module`]), both documented
//! at their definitions below.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Absolute path to the workspace root, derived from `xtask`'s own manifest dir rather than
/// the process CWD — tests need this to be stable regardless of where `cargo test` is
/// invoked from; `run()` uses it too so both paths exercise identical logic.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

/// One sub-check's outcome. `Fail`/`NotApplicable` carry human-readable detail lines;
/// `NotApplicable`'s single `String` is always the missing object name (§57.1 第2条).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Fail(Vec<String>),
    NotApplicable(String),
}

// ============================================================================
// shared fs helpers
// ============================================================================

/// Recursively collects every file under `dir` whose extension is in `exts`, skipping
/// `target`/`.git`/`node_modules`. Read errors on a subtree (permissions, races) are
/// swallowed the same way `config_check.rs::walk` does — a directory this can't read
/// contributes no files, it does not abort the whole scan.
fn walk_files(dir: &Path, exts: &[&str]) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk_files_rec(dir, exts, &mut out);
    out
}

fn walk_files_rec(dir: &Path, exts: &[&str], out: &mut Vec<PathBuf>) {
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
            walk_files_rec(&path, exts, out);
        } else if let Some(ext) = path.extension().and_then(|e| e.to_str())
            && exts.contains(&ext)
        {
            out.push(path);
        }
    }
}

/// This checker's own path, relative to workspace root. Excluded from the workspace-wide
/// construction/type-uniqueness scans below (§59.1 G59-3, §55.1 G80-2, §48.0① G80-22, §52.4
/// G52-1) once their scan domain widens past `crates/` to the whole workspace (§80.1.1 /
/// §48.0①/§52.4 all name `bins/*` / `evals/*` / xtask as required scan territory): this file's
/// own source necessarily contains the literal needle text those scans hunt for — both as the
/// needle arguments passed to [`count_construction_calls`] / [`find_construction_braces`]
/// themselves, and as this file's own `mod tests` fixture strings — so scanning it can only
/// ever produce false hits about itself, never a real violation in production code elsewhere.
/// This is not a loosened judgment: it excludes the auditor from auditing its own necessarily
/// self-quoting source, the same way a linter's own test fixtures are excluded from its own
/// lint run.
const SELF_FILE: &str = "xtask/src/architecture_check.rs";

/// Blanks out the first top-level `#[cfg(test)] mod ... { .. }` block in `source`, replacing
/// its non-newline bytes with spaces (char-by-char, so a multi-byte UTF-8 character is
/// replaced by exactly one space rather than corrupting the string) so every other line's
/// number is unaffected. Applied by [`walk_workspace_rs`] below: this codebase's xtask helper
/// crate legitimately writes fixture *strings* for its own unit tests that reuse real spec
/// vocabulary — e.g. `metrics_registry.rs`'s own `mod tests` defines a `pub fn build_request()`
/// fixture for its unrelated §80.2 metrics-registry tests, which textually collides with G80-2's
/// `RetrievalRequest`/`build_request` object-existence probe once the scan domain widens to
/// include xtask/. Without this, one checker's test fixtures would flip a *different* checker's
/// verdict. This does not hide a real violation: every four-check target in this codebase (see
/// `authority.rs`'s own `mod tests`, which only ever calls `Authority::new`) already routes its
/// own tests through the public constructor, never the raw struct literal — the rule these
/// checks enforce holds *inside* test code too, so stripping test-only text cannot turn a real
/// violation invisible, it only removes unrelated fixture noise.
///
/// ponytail: single top-level `mod tests` per file, matching this codebase's own rustfmt-output
/// convention (`metrics_registry.rs`'s own `#[cfg(test)]`-skip logic documents the identical
/// assumption for its spec-fixture line scan) — a file with two would only have its first
/// block stripped. Upgrade to a loop if that convention ever changes.
fn strip_cfg_test_module(source: &str) -> String {
    // Skip past `#[cfg(test)]` occurrences that are themselves inside a `//` comment (e.g. a
    // doc comment quoting the attribute as example text, not applying it) — same false-match
    // shape every other scan in this module guards against with `line_is_comment_at`.
    let mut search = 0usize;
    let attr_rel = loop {
        let Some(rel) = source[search..].find("#[cfg(test)]") else {
            return source.to_string();
        };
        let idx = search + rel;
        if !line_is_comment_at(source, idx) {
            break idx;
        }
        search = idx + "#[cfg(test)]".len();
    };
    let Some(mod_rel) = source[attr_rel..].find("mod ") else {
        return source.to_string();
    };
    let mod_kw = attr_rel + mod_rel;
    let Some(brace_rel) = source[mod_kw..].find('{') else {
        return source.to_string();
    };
    let open = mod_kw + brace_rel;
    let Some(close) = matching_brace_end(source, open) else {
        return source.to_string();
    };
    source
        .char_indices()
        .map(|(i, c)| {
            if i >= open && i < close && c != '\n' {
                ' '
            } else {
                c
            }
        })
        .collect()
}

/// Every `.rs` file in the whole workspace (skipping target/.git/node_modules like
/// [`walk_files`]) with its `#[cfg(test)]` module stripped ([`strip_cfg_test_module`]) and
/// [`SELF_FILE`] excluded outright (its production code, not just its tests, quotes these
/// checks' own needle strings — see [`SELF_FILE`]'s doc) — the shared scan domain for the four
/// workspace-wide construction/type-uniqueness checks.
fn walk_workspace_rs(root: &Path) -> Vec<(PathBuf, String)> {
    read_files(&walk_files(root, &["rs"]))
        .into_iter()
        .filter(|(p, _)| display(root, p) != SELF_FILE)
        .map(|(p, s)| {
            let stripped = strip_cfg_test_module(&s);
            (p, stripped)
        })
        .collect()
}

fn read_files(paths: &[PathBuf]) -> Vec<(PathBuf, String)> {
    paths
        .iter()
        .filter_map(|p| fs::read_to_string(p).ok().map(|s| (p.clone(), s)))
        .collect()
}

fn display(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}

/// Whether the line containing byte offset `idx` in `source` is a `//` comment line
/// (trimmed prefix starts with `//`). Cheap guard against grepping doc-comment prose that
/// merely *mentions* a pattern instead of containing real code (§53.3 / §59.1 G59-3 scans
/// below both need this — a spec-quoting comment must never count as a hit).
fn line_is_comment_at(source: &str, idx: usize) -> bool {
    let line_start = source[..idx].rfind('\n').map(|i| i + 1).unwrap_or(0);
    source[line_start..]
        .lines()
        .next()
        .unwrap_or("")
        .trim_start()
        .starts_with("//")
}

/// Byte offset one past the `}` that closes the `{` at `open_brace_idx` (which must itself
/// be `{`), found by naive depth counting over raw bytes.
///
/// ponytail: this is not a Rust parser — it does not know about strings, chars, or comments,
/// so a literal `{`/`}` inside a string constant would desync the count. None of this
/// module's targets (fn bodies, `degrade_code! {}`, `impl` blocks) contain brace characters
/// inside string literals today; upgrade to `syn` if that ever changes.
fn matching_brace_end(source: &str, open_brace_idx: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut depth = 0i32;
    let mut i = open_brace_idx;
    while i < bytes.len() {
        match bytes[i] {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i + 1);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

// ============================================================================
// §53.3 规则1 + 规则3: Outcome<_> fallback scan, shared by real code and sentinels
// ============================================================================

/// Scans `source` for `fn`s whose signature contains the literal text `Outcome<` and, within
/// each such fn's body (nested closures included — §53.3 规则1 says "函数体内", not "顶层
/// 语句"), flags `unwrap_or_default()` / `unwrap_or(` / `Ok(Vec::new())` / `Ok(None)` unless
/// the same line also calls `abstain(` (i.e. is itself the abstain call chain).
///
/// ponytail: literal-text matching on `Outcome<`, not type resolution — a fn returning the
/// `Response<T>` alias (`Result<Outcome<T>, ErrorCode>`) is invisible to this scan because
/// its signature text says `Response<T>`, not `Outcome<`. §53.3 registers this as a grep-
/// style check ("forbidden import grep with positive sentinels", §78.3); upgrade to alias-
/// aware matching if `Response<T>`-returning functions start actually using bare fallbacks.
pub fn scan_outcome_fallback_violations(display_path: &str, source: &str) -> Vec<String> {
    let mut violations = Vec::new();
    let mut search_from = 0usize;
    while let Some(rel) = source[search_from..].find("fn ") {
        let fn_kw = search_from + rel;
        let after = &source[fn_kw..];
        let brace_opt = after.find('{');
        let semi_opt = after.find(';');
        // A `;` reached before the next `{` means this is a body-less declaration (a trait
        // method signature, `fn a(&self) -> Outcome<i32>;`) — its own "signature" text must
        // not be treated as ending at some later, unrelated fn's opening brace, and that
        // later fn's real body must not be misattributed to this bodyless one.
        let is_bodyless = match (semi_opt, brace_opt) {
            (Some(s), Some(b)) => s < b,
            (Some(_), None) => true,
            _ => false,
        };
        if is_bodyless {
            search_from = fn_kw + semi_opt.expect("is_bodyless implies semi_opt is Some") + 1;
            continue;
        }
        let Some(brace_rel) = brace_opt else {
            break;
        };
        let open_brace = fn_kw + brace_rel;
        let signature = &source[fn_kw..open_brace];
        // A stray '}' before the next '{' means we scanned across some other block (e.g.
        // "fn" appearing inside a comment/string) rather than a real signature — skip it.
        if !signature.contains('}')
            && signature.contains("Outcome<")
            && !line_is_comment_at(source, fn_kw)
            && let Some(close) = matching_brace_end(source, open_brace)
        {
            let body = &source[open_brace..close];
            let body_start_line = source[..open_brace].matches('\n').count() + 1;
            for (i, line) in body.lines().enumerate() {
                // §53.3 规则1 targets real fallback calls, not prose that merely mentions
                // them (e.g. a comment citing this very rule inside the fn body it documents).
                if line.trim_start().starts_with("//") {
                    continue;
                }
                let has_fallback = line.contains("unwrap_or_default()")
                    || line.contains("unwrap_or(")
                    || line.contains("Ok(Vec::new())")
                    || line.contains("Ok(None)");
                if has_fallback && !line.contains("abstain(") {
                    violations.push(format!("{display_path}:{}", body_start_line + i));
                }
            }
            search_from = close;
            continue;
        }
        search_from = open_brace + 1;
    }
    violations
}

/// §53.3 规则1: no `crates/**/*.rs` function returning `Outcome<_>` may fall back to a
/// swallowed-error value outside `abstain()`'s call chain.
pub fn rule1_forbidden_fallback(root: &Path) -> Verdict {
    let crates_dir = root.join("crates");
    let files = read_files(&walk_files(&crates_dir, &["rs"]));
    let mut violations = Vec::new();
    for (path, source) in &files {
        violations.extend(scan_outcome_fallback_violations(
            &display(root, path),
            source,
        ));
    }
    if violations.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(violations)
    }
}

/// §53.3 规则3: exactly 3 `.sample` files must live under `crates/testkit/sentinels/`, and
/// [`scan_outcome_fallback_violations`] must hit each of them **exactly** once — not "3 hits
/// somewhere in the directory total". Aggregating only the total would let one sentinel file
/// go missing while another one over-hits and still read as 3; the spec's "3 个【本该命中】的
/// 样本文件…恰好命中 3 次" names the file dimension, not just the sum, so this checks both.
pub fn rule3_positive_sentinels(root: &Path) -> Verdict {
    let sentinels_dir = root.join("crates/testkit/sentinels");
    let files = read_files(&walk_files(&sentinels_dir, &["sample"]));
    if files.len() != 3 {
        return Verdict::Fail(vec![format!(
            "expected exactly 3 `.sample` files under crates/testkit/sentinels/, found {}: {:?}",
            files.len(),
            files
                .iter()
                .map(|(p, _)| display(root, p))
                .collect::<Vec<_>>()
        )]);
    }
    let mut bad = Vec::new();
    for (path, source) in &files {
        let disp = display(root, path);
        let hits = scan_outcome_fallback_violations(&disp, source);
        if hits.len() != 1 {
            bad.push(format!(
                "{disp}: expected exactly 1 hit, found {}: {:?}",
                hits.len(),
                hits
            ));
        }
    }
    if bad.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(bad)
    }
}

// ============================================================================
// §53.3 规则2: |DegradeCode variants| == |testkit/tests/fault/*.rs| by name
// ============================================================================

/// PascalCase → snake_case (`RerankModelMismatch` → `rerank_model_mismatch`), the §53.4
/// file-naming rule (`lower(fold(变体名))`, distinct from `degrade::fold`'s SCREAMING_SNAKE
/// output — same insertion rule, different case target).
pub fn pascal_to_snake(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    for (i, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i != 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// Extracts the variant identifier list from a `degrade_code! { ... }` macro invocation
/// (as defined in `crates/telemetry/src/degrade.rs`) — doc-comment lines (`///`) are
/// skipped, each remaining non-empty trimmed line (minus its trailing comma) is one variant.
pub fn parse_degrade_variant_names(source: &str) -> Vec<String> {
    let Some(kw_rel) = source.find("degrade_code! {") else {
        return Vec::new();
    };
    let open_brace = kw_rel + "degrade_code! {".len() - 1;
    let Some(close) = matching_brace_end(source, open_brace) else {
        return Vec::new();
    };
    let block = &source[open_brace + 1..close - 1];
    let mut out = Vec::new();
    for line in block.lines() {
        let t = line.trim();
        if t.is_empty() || t.starts_with("///") {
            continue;
        }
        let t = t.trim_end_matches(',');
        if t.chars().next().is_some_and(|c| c.is_ascii_uppercase())
            && t.chars().all(|c| c.is_ascii_alphanumeric())
        {
            out.push(t.to_string());
        }
    }
    out
}

/// §53.3 规则2 pure comparator — covers 2 of the rule's 3 named counts (`|DegradeCode variants|`
/// vs. `|testkit/tests/fault/*.rs|`). The third count (live `degrade_total{code}` label
/// cardinality) is a runtime Prometheus fact this source-only xtask check cannot observe; its
/// static proxy — `fold` must be injective over the live variant set, so the label cardinality
/// can never silently fall below the variant count via an `as_str()`-for-`fold()` swap — lives
/// as `fold_is_injective_over_all_variants` in `crates/telemetry/src/degrade.rs`, not here. Do
/// not read this sub-check's name as promising all three counts.
/// `variant_names` vs. the set of `.rs` file stems present under `testkit/tests/fault/`.
/// Reports both directions of mismatch (missing variant files *and* orphaned fault files)
/// rather than only a bare count, so a name-level typo (equal counts, wrong names) is still
/// caught, not just a count drift.
pub fn rule2_check(variant_names: &[String], fault_stems: &BTreeSet<String>) -> Verdict {
    let expected: BTreeSet<String> = variant_names.iter().map(|v| pascal_to_snake(v)).collect();
    let missing: Vec<&String> = expected.difference(fault_stems).collect();
    let extra: Vec<&String> = fault_stems.difference(&expected).collect();
    if missing.is_empty() && extra.is_empty() && variant_names.len() == fault_stems.len() {
        return Verdict::Pass;
    }
    let mut details = vec![format!(
        "|DegradeCode variants|={} != |testkit/tests/fault/*.rs|={}",
        variant_names.len(),
        fault_stems.len()
    )];
    if !missing.is_empty() {
        details.push(format!(
            "missing object: fault test file(s) for variant(s) {}",
            missing
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !extra.is_empty() {
        details.push(format!(
            "unexpected fault file(s) with no matching DegradeCode variant: {}",
            extra
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Verdict::Fail(details)
}

fn rule2_degrade_fault_parity(root: &Path) -> Verdict {
    let degrade_path = root.join("crates/telemetry/src/degrade.rs");
    let Ok(degrade_src) = fs::read_to_string(&degrade_path) else {
        return Verdict::Fail(vec![format!("cannot read {}", degrade_path.display())]);
    };
    let variants = parse_degrade_variant_names(&degrade_src);
    let fault_dir = root.join("crates/testkit/tests/fault");
    let stems: BTreeSet<String> = walk_files(&fault_dir, &["rs"])
        .iter()
        .filter_map(|p| p.file_stem().and_then(|s| s.to_str()).map(String::from))
        .collect();
    rule2_check(&variants, &stems)
}

// ============================================================================
// §52.4 G52-1: only ErrorCode may serialize at an `error_code` position
// ============================================================================

/// Scans `source` for `error_code\s*:\s*<Type>` occurrences (struct field decls, struct
/// literals, and fn params alike — text-level, not scope-aware) and returns every PascalCase
/// type name found there. Lowercase captures (`error_code: code`, a value not a type) are
/// filtered out by construction: Rust type/enum names are PascalCase by convention, so this
/// alone tells a type occupying the position apart from a variable merely named `code`.
///
/// ponytail: this targets the flat `error_code: ErrorCode` field convention this codebase
/// actually uses today, not a nested `error: { code: ... }` JSON shape — no serde-tagged
/// response envelope exists yet to grep for. Revisit when §23's envelope type lands.
pub fn find_error_code_field_types(source: &str) -> Vec<String> {
    let needle = "error_code";
    let mut out = Vec::new();
    let mut start = 0usize;
    while let Some(rel) = source[start..].find(needle) {
        let idx = start + rel;
        if line_is_comment_at(source, idx) {
            start = idx + needle.len();
            continue;
        }
        let after = &source[idx + needle.len()..];
        if let Some(rest) = after.trim_start().strip_prefix(':') {
            let rest = rest.trim_start();
            let ty: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if ty.chars().next().is_some_and(|c| c.is_ascii_uppercase()) {
                out.push(ty);
            }
        }
        start = idx + needle.len();
    }
    out
}

/// G52-1 pure comparator: the set of types found at an `error_code` position must be exactly
/// `{"ErrorCode"}` — anything else is a second enum sharing the slot (the violation the spec
/// injection names); an empty set means the positive control itself didn't fire, which is
/// indistinguishable from "matcher is broken" (§59.1 G59-3 同款「写成 <= 1 时又是一个恒真
/// 闸」), so that must fail too, not silently pass.
pub fn g52_1_check(types_found: &BTreeSet<String>) -> Verdict {
    let mut expected = BTreeSet::new();
    expected.insert("ErrorCode".to_string());
    if types_found == &expected {
        Verdict::Pass
    } else if types_found.is_empty() {
        Verdict::Fail(vec![
            "positive control missing: no `error_code: ErrorCode` occurrence found anywhere \
             in the workspace — matcher is broken, not the codebase"
                .to_string(),
        ])
    } else {
        let extra: Vec<&String> = types_found.difference(&expected).collect();
        Verdict::Fail(vec![format!(
            "type(s) other than ErrorCode found at an `error_code` position: {:?}",
            extra
        )])
    }
}

fn g52_1_error_code_uniqueness(root: &Path) -> Verdict {
    // §52.4 G52-1 names "扫全 workspace" explicitly, not just crates/.
    let files = walk_workspace_rs(root);
    let mut types_found = BTreeSet::new();
    for (_, source) in &files {
        types_found.extend(find_error_code_field_types(source));
    }
    g52_1_check(&types_found)
}

// ============================================================================
// §59.1 G59-3: exactly one `Authority { .. }` struct literal, inside authority.rs::new()
// ============================================================================

/// Word-bounded "`word` immediately (after whitespace) followed by `{`" scan over
/// `[search_start, search_end)` of `source`, skipping declaration-shaped and comment-line
/// occurrences. `is_declaration` receives the trimmed text immediately preceding the match.
/// Shared by the `Authority {` and (within `impl Authority` scope only) `Self {` scans below —
/// same shape, different word and declaration predicate.
fn find_construction_braces(
    source: &str,
    word: &str,
    search_start: usize,
    search_end: usize,
    is_declaration: impl Fn(&str) -> bool,
) -> Vec<usize> {
    let bytes = source.as_bytes();
    let mut out = Vec::new();
    let mut start = search_start;
    while start < search_end {
        let Some(rel) = source[start..search_end].find(word) else {
            break;
        };
        let idx = start + rel;
        let before_ok = idx == 0 || !is_ident_byte(bytes[idx - 1]);
        let after_idx = idx + word.len();
        let after_ok = after_idx >= bytes.len() || !is_ident_byte(bytes[after_idx]);
        if before_ok
            && after_ok
            && !is_declaration(source[..idx].trim_end())
            && source[after_idx..].trim_start().starts_with('{')
            && !line_is_comment_at(source, idx)
        {
            out.push(idx);
        }
        start = idx + word.len();
    }
    out
}

/// Every `impl Authority { .. }` block's `(open_brace, close_brace)` byte range in `source`
/// (word-bounded, so `impl AuthorityPolicy` is not mistaken for it — same convention as
/// [`authority_new_body_range`] below). Trait impls (`impl PartialOrd for Authority`) are not
/// matched: the token immediately after `impl` there is the trait name, not `Authority`.
fn impl_authority_block_ranges(source: &str) -> Vec<(usize, usize)> {
    let bytes = source.as_bytes();
    let needle = "impl Authority";
    let mut out = Vec::new();
    let mut search = 0usize;
    while let Some(rel) = source[search..].find(needle) {
        let idx = search + rel;
        let after_idx = idx + needle.len();
        let after_ok = after_idx >= bytes.len() || !is_ident_byte(bytes[after_idx]);
        if after_ok && let Some(brace_rel) = source[after_idx..].find('{') {
            let open = after_idx + brace_rel;
            if let Some(close) = matching_brace_end(source, open) {
                out.push((open, close));
            }
        }
        search = idx + needle.len();
    }
    out
}

/// Finds every `Authority {` struct-literal site in `source`, plus every `Self {` site that
/// falls inside an `impl Authority { .. }` block (`Self` only resolves to `Authority` there —
/// §59.1 G59-3's real construction point, `authority.rs::Authority::new`, is free to spell its
/// return as `Ok(Self { .. })`, the idiomatic form `clippy::use_self` prefers, and a matcher
/// that only recognizes the bare `Authority { .. }` spelling silently reads that as "0 hits",
/// which prints as the unrelated, misleading "not yet landed" reason instead of a real bug
/// report). `AuthorityClass {` / `AuthorityStatus {` / `AuthorityPolicy {` don't match (word
/// bounds); comment lines are skipped both ways.
pub fn find_authority_struct_literals(source: &str) -> Vec<usize> {
    let mut hits = find_construction_braces(source, "Authority", 0, source.len(), |prefix| {
        // `struct Authority {` / `impl Authority {` are declarations, not construction sites;
        // `-> Authority {` is a fn signature whose own body brace happens to follow the
        // return-type name, same false-positive shape as `count_construction_calls` guards.
        prefix.ends_with("struct") || prefix.ends_with("impl") || prefix.ends_with("->")
    });
    for (open, close) in impl_authority_block_ranges(source) {
        hits.extend(find_construction_braces(
            source,
            "Self",
            open,
            close,
            |prefix| {
                // `-> Self {` is `Authority::new`'s own fn signature ending in its body brace, not
                // a `Self { .. }` struct literal — the brace belongs to the fn body that follows.
                prefix.ends_with("->")
            },
        ));
    }
    hits.sort_unstable();
    hits
}

/// Byte range of `Authority::new`'s fn body within `source`, if `source` contains both an
/// `impl Authority { .. }` block (word-bounded, so `impl AuthorityPolicy` is not mistaken
/// for it) and a `fn new(` inside it.
fn authority_new_body_range(source: &str) -> Option<(usize, usize)> {
    let bytes = source.as_bytes();
    let needle = "impl Authority";
    let mut search = 0usize;
    while let Some(rel) = source[search..].find(needle) {
        let idx = search + rel;
        let after_idx = idx + needle.len();
        let after_ok = after_idx >= bytes.len() || !is_ident_byte(bytes[after_idx]);
        if after_ok {
            let impl_brace_rel = source[after_idx..].find('{')?;
            let impl_open = after_idx + impl_brace_rel;
            let impl_close = matching_brace_end(source, impl_open)?;
            let impl_body = &source[impl_open..impl_close];
            if let Some(fn_rel) = impl_body.find("fn new(") {
                let fn_kw = impl_open + fn_rel;
                if let Some(brace_rel) = source[fn_kw..impl_close].find('{') {
                    let fn_open = fn_kw + brace_rel;
                    if let Some(fn_close) = matching_brace_end(source, fn_open) {
                        return Some((fn_open, fn_close));
                    }
                }
            }
            return None;
        }
        search = idx + needle.len();
    }
    None
}

/// G59-3 pure comparator: exactly one `Authority {`/`Self {}` construction site in the whole
/// workspace, and it must sit inside `authority.rs`'s `Authority::new` body — the § assertion
/// is written `== 1`, never `<= 1` (a matcher that hits 0 due to a typo would pass `<= 1`
/// silently, the exact "恒真闸" shape §59.1 calls out).
pub fn g59_3_check(
    hits: &[(String, usize)],
    authority_rs_new_range: Option<(usize, usize)>,
) -> Verdict {
    match hits.len() {
        // 0 hits is ambiguous by construction: it means either "Authority::new isn't landed
        // yet" or "it landed, but the matcher failed to recognize the shape it was written
        // in" — the message must not assert either cause as fact (§53.3/§59.1 同款「恒真闸」
        // 反例：把「matcher 坏了」误报成「预期状态」本身就是一次假报告).
        0 => Verdict::Fail(vec![
            "missing object: no `Authority {{ .. }}`/`Self {{ .. }}` (inside `impl Authority`) \
             construction site found anywhere in the scanned workspace — either \
             Authority::new is not yet landed, or it landed in a shape this matcher does not \
             recognize; both must be treated as red, never assumed"
                .to_string(),
        ]),
        1 => {
            let (file, offset) = &hits[0];
            let in_new = file.ends_with("authority.rs")
                && authority_rs_new_range.is_some_and(|(s, e)| (s..e).contains(offset));
            if in_new {
                Verdict::Pass
            } else {
                Verdict::Fail(vec![format!(
                    "the sole `Authority {{ .. }}` literal is at {file}, not inside \
                     authority.rs::Authority::new()"
                )])
            }
        }
        n => Verdict::Fail(vec![format!(
            "expected exactly 1 `Authority {{ .. }}` construction site, found {n}"
        )]),
    }
}

fn g59_3_authority_construction_point(root: &Path) -> Verdict {
    // Whole workspace, not just crates/domain: §59.1 G59-3 注错a names `evals/` as the
    // injection point for the second construction site, which lives outside crates/ entirely.
    let files = walk_workspace_rs(root);
    let mut hits = Vec::new();
    let mut new_range = None;
    for (path, source) in &files {
        let disp = display(root, path);
        for offset in find_authority_struct_literals(source) {
            hits.push((disp.clone(), offset));
        }
        if path.file_name().and_then(|n| n.to_str()) == Some("authority.rs") {
            new_range = authority_new_body_range(source);
        }
    }
    g59_3_check(&hits, new_range)
}

// ============================================================================
// §78.3 Workspace Dependency Rule
// ============================================================================

/// crates that `humaux-domain` may never depend on: §78.3's verbatim seven-item list (axum,
/// sqlx, qdrant client, reqwest, dashscope, stripe, openbao client) plus the separate
/// Architecture Boundary Lints line "Domain crate cannot depend on adapters/protocol/runtime"
/// (`humaux-adapters` / `humaux-protocol` / `humaux-runtime` — the last does not exist as a
/// workspace member yet, forbidding it now is still correct: domain trivially depends on
/// nothing named that), plus a substring catch-all for "任何 SDK", since provider SDKs
/// multiply faster than this list can be hand-maintained. `tokio-postgres` / `aws-sdk-s3` are
/// not in §78.3's literal text but are strictly redundant with `sqlx` / the `sdk` catch-all —
/// kept as extra, non-conflicting protection, not removed.
///
/// ponytail: the substring check is `name.to_lowercase().contains("sdk")` — cheap and
/// catches `aws-sdk-s3` / future `*-sdk` crates without enumerating them, at the cost of a
/// false positive on some hypothetical crate that merely has "sdk" in its name without being
/// one. No such crate exists in this dependency tree today.
const DOMAIN_FORBIDDEN_DEPS: &[&str] = &[
    "sqlx",
    "axum",
    "reqwest",
    "qdrant-client",
    "tokio-postgres",
    "aws-sdk-s3",
    "stripe",
    "openbao",
    "dashscope",
    "humaux-adapters",
    "humaux-protocol",
    "humaux-runtime",
];

/// Pure §78.3 comparator over a parsed `cargo metadata --no-deps` document — takes the JSON
/// text directly so tests can inject a fixture without shelling out to `cargo`.
///
/// `humaux-application` is deliberately not checked here: no spec line or ADR grounds a
/// forbidden-dependency rule for it (§78's only frozen line names `domain`, not
/// `application`) — inventing one and shipping it as if §78.3 required it would be exactly the
/// kind of ungrounded judgment this checker exists to catch elsewhere. Add it back only once a
/// spec section or `docs/adr/` entry actually says so.
pub fn dependency_rule_from_metadata_json(metadata_json: &str) -> Verdict {
    let parsed: serde_json::Value = match serde_json::from_str(metadata_json) {
        Ok(v) => v,
        Err(e) => return Verdict::Fail(vec![format!("cargo metadata output not valid JSON: {e}")]),
    };
    let Some(packages) = parsed.get("packages").and_then(|p| p.as_array()) else {
        return Verdict::Fail(vec![
            "cargo metadata output has no `packages` array".to_string(),
        ]);
    };

    let mut violations = Vec::new();
    check_package_deps(
        packages,
        "humaux-domain",
        DOMAIN_FORBIDDEN_DEPS,
        |name| name.to_lowercase().contains("sdk"),
        &mut violations,
    );

    if violations.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(violations)
    }
}

fn check_package_deps(
    packages: &[serde_json::Value],
    package_name: &str,
    forbidden_exact: &[&str],
    forbidden_pred: impl Fn(&str) -> bool,
    violations: &mut Vec<String>,
) {
    let Some(pkg) = packages
        .iter()
        .find(|p| p.get("name").and_then(|n| n.as_str()) == Some(package_name))
    else {
        // Package not present in this metadata document — the matcher's target went missing
        // (renamed, dropped from the workspace, or a wrong `--no-deps` fixture was handed in).
        // §59.1 G59-3 同款「恒真闸」: silently returning here would make this half of the gate
        // permanently, invisibly green — that is a red, not an inapplicable check, because
        // `humaux-domain` is a frozen workspace member name (§58), not an optional object.
        violations.push(format!(
            "package `{package_name}` not found in cargo metadata output — matcher target \
             missing, not a clean dependency graph"
        ));
        return;
    };
    let deps = pkg
        .get("dependencies")
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();
    for dep in &deps {
        let Some(dep_name) = dep.get("name").and_then(|n| n.as_str()) else {
            continue;
        };
        if forbidden_exact.contains(&dep_name) || forbidden_pred(dep_name) {
            violations.push(format!(
                "{package_name} depends on forbidden crate {dep_name}"
            ));
        }
    }
}

fn dependency_rule_check(root: &Path) -> Verdict {
    let output = Command::new("cargo")
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(root)
        .output();
    match output {
        Ok(o) if o.status.success() => {
            dependency_rule_from_metadata_json(&String::from_utf8_lossy(&o.stdout))
        }
        Ok(o) => Verdict::Fail(vec![format!(
            "cargo metadata exited {}: {}",
            o.status,
            String::from_utf8_lossy(&o.stderr)
        )]),
        Err(e) => Verdict::Fail(vec![format!("failed to run cargo metadata: {e}")]),
    }
}

// ============================================================================
// §78 Boundary Lints: No env::var outside config/bootstrap
// ============================================================================

/// §78 boundary lint pure scanner: any real call to `std::env::var` is a violation, under
/// either spelling the codebase actually uses —
///
/// - fully qualified: `std::env::var("X")`,
/// - short form after `use std::env;`: `env::var("X")` (a substring of the qualified spelling,
///   so one `env::var` needle search catches both — the character before `env` in the
///   qualified form is `:`, never an identifier char, so the word-boundary check does not
///   need to special-case it),
/// - re-exported short form after `use std::env::var;`: bare `var("X")` calls in that file.
///
/// Comment lines are skipped for both. Callers filter which `(path, source)` pairs get passed
/// in — that's where the `contracts` config-read-point and `bins/*`/`xtask/` bootstrap
/// exemptions live (§78: "No env::var outside config/bootstrap").
pub fn scan_env_var_violations(display_path: &str, source: &str) -> Vec<String> {
    let bytes = source.as_bytes();
    let mut hit_lines: BTreeSet<usize> = BTreeSet::new();
    let mut record = |idx: usize| {
        if !line_is_comment_at(source, idx) {
            hit_lines.insert(source[..idx].matches('\n').count() + 1);
        }
    };

    let qualified_or_short = "env::var";
    let mut start = 0usize;
    while let Some(rel) = source[start..].find(qualified_or_short) {
        let idx = start + rel;
        // `use std::env::var;` is an import declaration, not a call — only the bare `var(`
        // call it enables (scanned separately below) is the real access site.
        let line_start = source[..idx].rfind('\n').map(|i| i + 1).unwrap_or(0);
        let line_is_use = source[line_start..]
            .lines()
            .next()
            .unwrap_or("")
            .trim_start()
            .starts_with("use ");
        if (idx == 0 || !is_ident_byte(bytes[idx - 1])) && !line_is_use {
            record(idx);
        }
        start = idx + qualified_or_short.len();
    }

    // ponytail: `use std::env::var` gates a bare-`var(`-call scan of the *whole file*, not a
    // scope-resolved one — a file that imports `std::env::var` this way and also happens to
    // define/call an unrelated fn literally named `var` would over-flag it too. No such name
    // collision exists in this codebase today; tighten to real scope resolution if one shows
    // up.
    if source.contains("use std::env::var") {
        let mut start = 0usize;
        while let Some(rel) = source[start..].find("var(") {
            let idx = start + rel;
            let before_ok = idx == 0 || !is_ident_byte(bytes[idx - 1]);
            let already_env_qualified =
                idx >= "env::".len() && &source[idx - "env::".len()..idx] == "env::";
            if before_ok && !already_env_qualified {
                record(idx);
            }
            start = idx + "var(".len();
        }
    }

    hit_lines
        .into_iter()
        .map(|line| format!("{display_path}:{line}"))
        .collect()
}

/// Byte offset of the file's trailing `#[cfg(test)]`-mod region, if the next
/// non-empty line after the attribute begins a `mod` — see the caller's ponytail note.
fn cfg_test_mod_offset(source: &str) -> Option<usize> {
    let mut search_from = 0usize;
    while let Some(rel) = source[search_from..].find("#[cfg(test)]") {
        let at = search_from + rel;
        let after = &source[at..];
        let mut lines = after.lines();
        lines.next(); // the attribute line itself
        for line in lines {
            let trimmed = line.trim_start();
            if trimmed.is_empty() {
                continue;
            }
            if trimmed.starts_with("mod ") || trimmed.starts_with("pub mod ") {
                return Some(at);
            }
            break;
        }
        search_from = at + "#[cfg(test)]".len();
    }
    None
}

fn env_var_scan(root: &Path) -> Verdict {
    // Whole workspace, not just crates/: bins/* and xtask are real exemptible bootstrap
    // territory (see below), but a violation planted in evals/ (outside both) must still be
    // observable — it was invisible to the old crates/-only domain.
    let files = read_files(&walk_files(root, &["rs"]));
    let mut violations = Vec::new();
    for (path, source) in &files {
        // exemptions: humaux-contracts owns the one legitimate config-read point (§50.1);
        // bins/* is real process bootstrap; xtask is the CI-gate tooling itself (this file's
        // own module doc: "the CI gate family") — both categories the spec line names
        // ("config/bootstrap"), and both now genuinely reachable now that the scan domain is
        // the whole workspace instead of only crates/.
        let disp = display(root, path);
        if disp.contains("crates/contracts/")
            || disp.starts_with("bins/")
            || disp.starts_with("xtask/")
            || disp.contains("/tests/")
        {
            // `/tests/` integration dirs: reading HUMAUX_TEST_PG_DSN there is the
            // sanctioned three-state DB-fixture pattern, not production config
            // (§79.2 / repo CLAUDE.md 测试规范). Production reads stay covered.
            continue;
        }
        // Same sanction for a file's trailing `#[cfg(test)] mod` region: this repo's
        // convention keeps unit tests at file end; env reads after that attr are test
        // fixture, not runtime config.
        // ponytail: prefix-scope heuristic (everything after the first `#[cfg(test)]`
        // line followed by `mod` is exempt); upgrade to real span parsing if a
        // production `#[cfg(test)]`-adjacent violation ever needs catching.
        let scan_source = match cfg_test_mod_offset(source) {
            Some(off) => &source[..off],
            None => source.as_str(),
        };
        violations.extend(scan_env_var_violations(&disp, scan_source));
    }
    if violations.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(violations)
    }
}

// ============================================================================
// §55.1 G80-2 / §48.0① G80-22: unique construction points not yet delivered
// ============================================================================

/// Counts non-definition occurrences of `needle` (e.g. `"Foo {"` for a struct literal, or
/// `"Foo("` for a tuple-struct/fn-style call) in `source`. Excludes shapes that share the same
/// trailing token but are not construction: the type's own declaration or an `impl`/`trait`
/// block header for it (`struct`/`enum`/`impl`/`trait`/`for` immediately precedes the type
/// name — the `for` case covers blanket/trait impls, `impl<T> From<T> for Foo {`), and a
/// function signature whose return type is immediately followed by its own body-opening brace
/// (`-> Foo {`, prefix trimmed ends with `->` — that `{` opens the fn body, it is not a
/// struct-literal brace). Checking the immediate prefix rather than "does the whole line start
/// with `struct`/`fn`" matters: a construction call can share a line with an unrelated `fn`
/// keyword (e.g. `fn shortcut() { let _ = Foo { .. }; }`), and a whole-line check would
/// wrongly exempt it. Without the `impl` case, `impl Foo { .. }` landing anywhere in a scanned
/// file makes this permanently over-count by one — a false red the day the type's first method
/// is added.
fn count_construction_calls(source: &str, needle: &str) -> usize {
    let mut count = 0usize;
    let mut start = 0usize;
    while let Some(rel) = source[start..].find(needle) {
        let idx = start + rel;
        let prefix = source[..idx].trim_end();
        let is_declaration = prefix.ends_with("struct")
            || prefix.ends_with("enum")
            || prefix.ends_with("impl")
            || prefix.ends_with("trait")
            || prefix.ends_with("for")
            || prefix.ends_with("->");
        if !is_declaration && !line_is_comment_at(source, idx) {
            count += 1;
        }
        start = idx + needle.len();
    }
    count
}

fn g80_2_build_request_unique(root: &Path) -> Verdict {
    // Whole workspace, not just crates/: §55.1 G80-2's own 注错 names `evals/` as the second
    // construction site, which lives outside crates/ entirely.
    let files = walk_workspace_rs(root);
    let object_exists = files
        .iter()
        .any(|(_, s)| s.contains("struct RetrievalRequest") || s.contains("fn build_request("));
    if !object_exists {
        return Verdict::NotApplicable(
            "retrieval::build_request / RetrievalRequest (§55.1, Phase 1 尚未交付)".to_string(),
        );
    }
    let mut count = 0usize;
    let mut sites = Vec::new();
    for (path, source) in &files {
        let n = count_construction_calls(source, "RetrievalRequest {");
        if n > 0 {
            sites.push(format!("{}: {n}", display(root, path)));
        }
        count += n;
    }
    if count == 1 {
        Verdict::Pass
    } else {
        Verdict::Fail(vec![format!(
            "expected exactly 1 `RetrievalRequest {{ .. }}` construction site, found {count}: {sites:?}"
        )])
    }
}

fn g80_22_payload_sha256_unique(root: &Path) -> Verdict {
    // Whole workspace, not just crates/: §48.0① names `bins/*` / `evals/*` / migration tooling
    // as required scan territory for the second construction site.
    let files = walk_workspace_rs(root);
    let object_exists = files.iter().any(|(_, s)| {
        s.contains("struct EvidencePayloadSha256") || s.contains("fn payload_sha256(")
    });
    if !object_exists {
        return Verdict::NotApplicable(
            "evidence::payload_sha256 / EvidencePayloadSha256 (§48.0① G80-22, Phase 6 尚未交付)"
                .to_string(),
        );
    }
    let mut count = 0usize;
    let mut sites = Vec::new();
    for (path, source) in &files {
        let n = count_construction_calls(source, "EvidencePayloadSha256(");
        if n > 0 {
            sites.push(format!("{}: {n}", display(root, path)));
        }
        count += n;
    }
    if count == 1 {
        Verdict::Pass
    } else {
        Verdict::Fail(vec![format!(
            "expected exactly 1 `EvidencePayloadSha256( .. )` construction site, found {count}: {sites:?}"
        )])
    }
}

/// §11.8 / CLAUDE.md "唯一构造点模式": `classify()` (`crates/domain/src/consolidate.rs`) is the
/// sole entry point that may produce a [`ClassifiedMemoryId`] — the fixer-review that landed
/// this check found the rule named in CLAUDE.md's own list but never wired into this file
/// (grep for "classify" here returned zero hits before this function existed). Counts
/// *definitions* of `fn classify(` workspace-wide, not call sites — `adapters::byok::
/// classify_http_status` is a different function name and does not match this needle.
fn g11_8_classify_sole_construction_point(root: &Path) -> Verdict {
    let files = walk_workspace_rs(root);
    let object_exists = files.iter().any(|(_, s)| s.contains("fn classify("));
    if !object_exists {
        return Verdict::NotApplicable(
            "domain::consolidate::classify (§11.8, T4.6/T4.7 尚未交付)".to_string(),
        );
    }
    let mut count = 0usize;
    let mut sites = Vec::new();
    for (path, source) in &files {
        let n = source.matches("fn classify(").count();
        if n > 0 {
            sites.push(format!("{}: {n}", display(root, path)));
        }
        count += n;
    }
    if count == 1 {
        Verdict::Pass
    } else {
        Verdict::Fail(vec![format!(
            "expected exactly 1 `fn classify(` definition, found {count}: {sites:?}"
        )])
    }
}

// ============================================================================
// §83.4 G80-3: Outbound Network Choke Point — real RHS
// ============================================================================

/// §83.4's `external-egress-registry` fenced block, column 1, verbatim order — the exact
/// variant-name set `OutboundPurpose` (`crates/domain/src/egress.rs`, §7.3/§83.4) must equal.
const EXTERNAL_EGRESS_REGISTRY: &[&str] = &[
    "USER_REASONING",
    "RETRIEVAL_EMBEDDING",
    "RETRIEVAL_RERANK",
    "PUBLIC_REASONING",
    "BILLING",
    "TRANSACTIONAL_EMAIL",
    "GIT_PROVIDER",
    "OAUTH_METADATA",
    "PUBLIC_SOURCE_FETCH",
    "DEADMAN_HEALTHCHECK",
];

const INFRA_EGRESS_HTTP_RS: &str = "crates/infra-egress/src/http.rs";

/// §83.4 判据1, 注错 a: `reqwest`/`hyper`'s client type reached through a `use`-imported bare
/// alias (`use reqwest::Client; Client::new()`) is exactly as much a raw-client construction
/// as the fully-qualified spelling — the fully-qualified needle set below cannot see it at
/// all, which is the exact loophole the 注错 a fixture in this module's tests demonstrates.
/// Matched with a word-boundary guard ([`is_bare_needle_word_start`]) so an unrelated type that
/// merely ends in `Client` (`MyClient::new(`) is not a false hit.
const BARE_HTTP_CLIENT_NEEDLES: [&str; 4] = [
    "Client::new(",
    "Client::builder(",
    "Client::default(",
    "ClientBuilder::new(",
];

/// §83.4 raw HTTP transport construction needles: `reqwest::Client::new`/`::builder` and
/// `hyper::Client::new`/`::builder` (fully-qualified spellings, kept alongside the bare-alias
/// set above so both spellings are named explicitly in one place rather than relying on the
/// bare set's substring overlap to carry the qualified case), comment-line-filtered like every
/// other scan in this module. The needle already includes the call-opening `(`, so — unlike
/// [`count_construction_calls`]'s struct-literal targets — there is no legal
/// `struct`/`impl`/`->`-prefixed spelling of `reqwest::Client::new(` that isn't a real call,
/// so no declaration-shape guard is needed for the qualified forms.
fn find_raw_http_client_calls(source: &str) -> Vec<usize> {
    let qualified_needles = [
        "reqwest::Client::new(",
        "reqwest::Client::builder(",
        "hyper::Client::new(",
        "hyper::Client::builder(",
    ];
    let mut hits = Vec::new();
    for needle in qualified_needles {
        let mut start = 0usize;
        while let Some(rel) = source[start..].find(needle) {
            let idx = start + rel;
            if !line_is_comment_at(source, idx) {
                hits.push(idx);
            }
            start = idx + needle.len();
        }
    }
    for needle in BARE_HTTP_CLIENT_NEEDLES {
        let mut start = 0usize;
        while let Some(rel) = source[start..].find(needle) {
            let idx = start + rel;
            if !line_is_comment_at(source, idx) && is_bare_needle_word_start(source, idx) {
                hits.push(idx);
            }
            start = idx + needle.len();
        }
    }
    hits
}

/// Word-boundary guard for [`BARE_HTTP_CLIENT_NEEDLES`]: true unless the byte immediately
/// before `idx` continues an identifier (letter/digit/`_`) — rejects `MyClient::new(` (a
/// same-shaped but unrelated local type) while accepting both a bare `Client::new(` and the
/// fully-qualified `reqwest::Client::new(` (preceded by `:`, not an identifier char).
fn is_bare_needle_word_start(source: &str, idx: usize) -> bool {
    match source[..idx].chars().next_back() {
        None => true,
        Some(c) => !(c.is_alphanumeric() || c == '_'),
    }
}

/// Extracts each variant identifier from a `enum <enum_name> { .. }` block in `source` — same
/// shape as [`parse_degrade_variant_names`] but for a plain `enum` (not the `degrade_code!`
/// macro) and tolerant of data-carrying variants (`Foo(Bar)` counts as `Foo`, matching
/// `OutboundPurpose`'s three private-data variants that each carry an `EgressPermit`).
/// Doc-comment (`///`) and attribute (`#[..]`) lines are skipped.
fn parse_enum_variant_names(source: &str, enum_name: &str) -> Vec<String> {
    let needle = format!("enum {enum_name}");
    let Some(rel) = source.find(&needle) else {
        return Vec::new();
    };
    let after = rel + needle.len();
    let Some(brace_rel) = source[after..].find('{') else {
        return Vec::new();
    };
    let open = after + brace_rel;
    let Some(close) = matching_brace_end(source, open) else {
        return Vec::new();
    };
    let block = &source[open + 1..close - 1];
    let mut out = Vec::new();
    for line in block.lines() {
        let t = line.trim();
        if t.is_empty() || t.starts_with("//") || t.starts_with('#') {
            continue;
        }
        let ident: String = t
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if ident.chars().next().is_some_and(|c| c.is_ascii_uppercase()) {
            out.push(ident);
        }
    }
    out
}

/// §83.4 判据3: same `enum <enum_name> { .. }` block [`parse_enum_variant_names`] walks, but
/// keeps each variant's tuple payload text verbatim instead of discarding it — the payload
/// shape is exactly what 判据3 ("标为 private-data 的 purpose 在类型上只能由 EgressPermit
/// 构造") needs to check. One line per variant is assumed (this codebase's own style for
/// `OutboundPurpose`, matched by every fixture in this module's tests): a variant's payload,
/// if any, must open and close its parens on the same line.
fn parse_enum_variant_payloads(source: &str, enum_name: &str) -> Vec<(String, Option<String>)> {
    let needle = format!("enum {enum_name}");
    let Some(rel) = source.find(&needle) else {
        return Vec::new();
    };
    let after = rel + needle.len();
    let Some(brace_rel) = source[after..].find('{') else {
        return Vec::new();
    };
    let open = after + brace_rel;
    let Some(close) = matching_brace_end(source, open) else {
        return Vec::new();
    };
    let block = &source[open + 1..close - 1];
    let mut out = Vec::new();
    for line in block.lines() {
        let t = line.trim();
        if t.is_empty() || t.starts_with("//") || t.starts_with('#') {
            continue;
        }
        let ident: String = t
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if !ident.chars().next().is_some_and(|c| c.is_ascii_uppercase()) {
            continue;
        }
        let rest = t[ident.len()..].trim_start();
        let payload = if let Some(inner) = rest.strip_prefix('(') {
            inner.find(')').map(|end| inner[..end].trim().to_string())
        } else {
            None
        };
        out.push((ident, payload));
    }
    out
}

/// §83.4 判据0 (workspace 正哨兵): `crates/infra-egress` must be a real, live workspace
/// member with its transport file present — any one of the three missing is red before
/// 判据1/2 are even evaluated. Without this, deleting the whole crate would make the
/// raw-client scan below vacuously report "0 hits outside the expected file" (there is no
/// file left to hit) — the exact 恒真闸 shape §59.1/§80.1 call out elsewhere in this module.
/// `metadata_json` is injectable so tests do not need to shell out to a real `cargo metadata`
/// against a fixture directory that has no real workspace `Cargo.toml`.
fn g80_3_workspace_sentinel(root: &Path, metadata_json: Option<&str>) -> Vec<String> {
    let mut problems = Vec::new();
    if !root.join("crates/infra-egress/Cargo.toml").is_file() {
        problems.push("正哨兵缺失: crates/infra-egress/Cargo.toml 不存在".to_string());
    }
    if !root.join(INFRA_EGRESS_HTTP_RS).is_file() {
        problems.push(format!("正哨兵缺失: {INFRA_EGRESS_HTTP_RS} 不存在"));
    }
    // Exact match against `packages[].name`, not a substring scan over the raw JSON text: a
    // substring match on `"humaux-infra-egress"` stays true even after the crate is dropped
    // from the workspace `members` list, as long as *some* other member still path-depends on
    // it by name (that dependency edge's own JSON also contains the literal string) — the
    // 注错 0b fixture below pins this against regressing back to `.contains`.
    let member_present = metadata_json
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
        .and_then(|v| v.get("packages").and_then(|p| p.as_array()).cloned())
        .is_some_and(|packages| {
            packages
                .iter()
                .any(|p| p.get("name").and_then(|n| n.as_str()) == Some("humaux-infra-egress"))
        });
    if !member_present {
        problems.push("正哨兵缺失: cargo metadata members 不含 humaux-infra-egress".to_string());
    }
    problems
}

/// §83.4 判据1 补充 (manifest-level positive check, 注错 a 的第二重防线): the *set* of
/// workspace-member manifests that declare a `reqwest`/`hyper` dependency must equal exactly
/// `{crates/infra-egress/Cargo.toml}`. A raw client call needs a matching `[dependencies]`
/// entry to even compile, so this catches a second crate reaching for `reqwest`/`hyper` at the
/// manifest level — before any source-text needle (bare-alias or otherwise) even has a call
/// site to find. Uses the same `cargo metadata` document [`g80_3_workspace_sentinel`] already
/// requires, parsed the same way §78.3's `dependency_rule_from_metadata_json` parses it.
fn g80_3_manifest_dependency_check(metadata_json: &str, root: &Path) -> Vec<String> {
    let mut problems = Vec::new();
    let parsed: serde_json::Value = match serde_json::from_str(metadata_json) {
        Ok(v) => v,
        Err(e) => {
            problems.push(format!("cargo metadata output not valid JSON: {e}"));
            return problems;
        }
    };
    let Some(packages) = parsed.get("packages").and_then(|p| p.as_array()) else {
        problems.push("cargo metadata output has no `packages` array".to_string());
        return problems;
    };

    // `cargo metadata`'s `manifest_path` is always an absolute, canonicalized path — `root`
    // here is typically `CARGO_MANIFEST_DIR/..` (a literal, un-canonicalized `..` component),
    // so a plain `display()` (component-wise `strip_prefix`) never matches it. Canonicalizing
    // `root` once fixes the real-repo case; fixture tests pass a nonexistent root (e.g.
    // `/fixture-root`) where `canonicalize` fails, so this falls back to comparing against
    // `root` as given — exactly [`display`]'s existing behavior for those fixtures.
    let root_for_display = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());

    let mut actual: BTreeSet<String> = BTreeSet::new();
    for pkg in packages {
        let deps = pkg
            .get("dependencies")
            .and_then(|d| d.as_array())
            .cloned()
            .unwrap_or_default();
        let depends_on_http_client = deps.iter().any(|d| {
            matches!(
                d.get("name").and_then(|n| n.as_str()),
                Some("reqwest") | Some("hyper")
            )
        });
        if !depends_on_http_client {
            continue;
        }
        let Some(manifest_path) = pkg.get("manifest_path").and_then(|m| m.as_str()) else {
            continue;
        };
        actual.insert(display(&root_for_display, Path::new(manifest_path)));
    }

    let mut expected = BTreeSet::new();
    expected.insert("crates/infra-egress/Cargo.toml".to_string());
    if actual != expected {
        problems.push(format!(
            "reqwest/hyper 依赖声明的 manifest 集合 != {{crates/infra-egress/Cargo.toml}}，实际: \
             {actual:?}"
        ));
    }
    problems
}

/// §83.4 判据1/2 pure comparator over an already-scanned `.rs` file set — separated from
/// [`g80_3_outbound_choke_point`] so tests can inject a small fixture file list instead of
/// scanning (or faking) the whole workspace.
fn g80_3_transport_and_registry_check(files: &[(PathBuf, String)], root: &Path) -> Vec<String> {
    let mut problems = Vec::new();

    // 判据1: raw client construction path set == {infra-egress/src/http.rs}
    let mut raw_sites: BTreeSet<String> = BTreeSet::new();
    for (path, source) in files {
        if !find_raw_http_client_calls(source).is_empty() {
            raw_sites.insert(display(root, path));
        }
    }
    let mut expected_raw = BTreeSet::new();
    expected_raw.insert(INFRA_EGRESS_HTTP_RS.to_string());
    if raw_sites != expected_raw {
        problems.push(format!(
            "raw HTTP client (reqwest::Client::new/builder, hyper::Client::new/builder) 构造 \
             点集合 != {{{INFRA_EGRESS_HTTP_RS}}}，实际: {raw_sites:?}"
        ));
    }

    // 判据2: OutboundPurpose 变体集合 == external-egress-registry 第一列，逐字相等
    match files
        .iter()
        .find(|(p, _)| display(root, p) == "crates/domain/src/egress.rs")
    {
        None => problems.push(
            "missing object: crates/domain/src/egress.rs::OutboundPurpose (§83.4 判据2 尚未\
             交付)"
                .to_string(),
        ),
        Some((_, source)) => {
            let actual: BTreeSet<String> = parse_enum_variant_names(source, "OutboundPurpose")
                .into_iter()
                .collect();
            let expected: BTreeSet<String> = EXTERNAL_EGRESS_REGISTRY
                .iter()
                .map(|s| s.to_string())
                .collect();
            if actual.is_empty() {
                problems.push(
                    "positive control missing: OutboundPurpose 变体集合为空 —— matcher 坏了或\
                     枚举未落地，两者都必须报红，不得默认放行"
                        .to_string(),
                );
            } else if actual != expected {
                problems.push(format!(
                    "OutboundPurpose 变体集合 != external-egress-registry 第一列，实际: \
                     {actual:?}，期望: {expected:?}"
                ));
            }

            // 判据3: 每个 private-data 变体的 tuple payload 必须逐字是 `EgressPermit` ——
            // "标为 private-data 的 purpose 在类型上只能由 EgressPermit 构造" is a claim
            // about *this enum's definition text*, not about `authorize`'s behavior, so it
            // belongs here alongside 判据2 rather than in a trybuild fixture that can only
            // ever exercise `EgressPermit`'s own construction path (§7.3), never this enum's
            // payload shape. `EXTERNAL_EGRESS_REGISTRY`'s first three entries are exactly the
            // private-data rows (registry column order, pinned by 判据2's own comparison).
            let payloads = parse_enum_variant_payloads(source, "OutboundPurpose");
            for private_variant in &EXTERNAL_EGRESS_REGISTRY[..3] {
                match payloads.iter().find(|(name, _)| name == private_variant) {
                    None => {} // absence is already reported by 判据2 above.
                    Some((_, None)) => problems.push(format!(
                        "判据3 违反: OutboundPurpose::{private_variant} 是裸变体，未携带 \
                         EgressPermit —— private-data purpose 必须类型化携带 permit"
                    )),
                    Some((_, Some(payload))) if payload != "EgressPermit" => {
                        problems.push(format!(
                            "判据3 违反: OutboundPurpose::{private_variant} 携带的是 \
                             `{payload}`，不是 `EgressPermit`"
                        ))
                    }
                    Some((_, Some(_))) => {}
                }
            }
        }
    }

    problems
}

/// §83.4 G80-3 full check: 判据0/1/2/3, folding in real `cargo metadata` for the workspace
/// positive sentinel.
fn g80_3_outbound_choke_point(root: &Path) -> Verdict {
    // No early `NotApplicable` return for a missing `crates/infra-egress` directory: T4.1 has
    // already delivered this crate, so its absence from here on is a regression (someone
    // deleted it), not "not yet built" — 注错 0a's whole point. Falling straight into the
    // workspace sentinel below reports that as `Fail` with the three named-missing-object
    // lines it already produces, instead of the vacuous "nothing to scan, so nothing is wrong"
    // `NotApplicable` a directory-existence early-return would produce.
    let metadata_json = Command::new("cargo")
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(root)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).into_owned());

    let mut problems = g80_3_workspace_sentinel(root, metadata_json.as_deref());
    if !problems.is_empty() {
        // 判据0 任一为假 -> 先红：前提不成立时继续判 1/2/3 只会制造噪声，不会制造信息。
        return Verdict::Fail(problems);
    }

    // 判据0 通过意味着 `metadata_json` 一定是 `Some`（sentinel 的 member_present 检查在
    // `None` 时必产生 problem，已在上面提前返回）——`expect` 而非静默跳过，manifest 依赖检查
    // 不是可选项。
    let metadata_json =
        metadata_json.expect("g80_3_workspace_sentinel passed with no metadata_json");
    problems.extend(g80_3_manifest_dependency_check(&metadata_json, root));

    let files = walk_workspace_rs(root);
    problems.extend(g80_3_transport_and_registry_check(&files, root));

    if problems.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(problems)
    }
}

// ============================================================================
// entry point
// ============================================================================

fn report(name: &str, verdict: &Verdict) -> bool {
    match verdict {
        Verdict::Pass => {
            println!("architecture-check: pass — {name}");
            false
        }
        Verdict::Fail(details) => {
            eprintln!("architecture-check: fail — {name}");
            for d in details {
                eprintln!("  {d}");
            }
            true
        }
        Verdict::NotApplicable(missing) => {
            println!("architecture-check: not_applicable — {name} (missing object: {missing})");
            false
        }
    }
}

// ============================================================================
// §6.2.3 G80-40 static side — typed DB pool topology
// ============================================================================

/// Counts non-comment occurrences of the `PgPool` *type* in `src`: matches the word
/// `PgPool` whether written bare (after `use sqlx::PgPool`) or qualified (`sqlx::PgPool`),
/// but not `PgPoolOptions` (the builder is not the pool handle) and not comment mentions.
/// This is the raw-pool-naming signal for G80-40 — the encapsulation check counts it in
/// `postgres.rs` (must be >0, live sentinel) and everywhere else (must be 0).
fn count_pgpool_type(src: &str) -> usize {
    let mut n = 0usize;
    for line in src.lines() {
        if line.trim_start().starts_with("//") {
            continue;
        }
        let bytes = line.as_bytes();
        let mut from = 0usize;
        while let Some(rel) = line[from..].find("PgPool") {
            let at = from + rel;
            let after = at + "PgPool".len();
            // exclude PgPoolOptions and any PgPool<ident> continuation
            let next_is_ident = bytes
                .get(after)
                .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_');
            if !next_is_ident {
                n += 1;
            }
            from = after;
        }
    }
    n
}

/// G80-40 static side (§6.2.3): raw `sqlx::PgPool` may be *named* only inside the single
/// encapsulation point `crates/adapters/src/postgres.rs`; the four wrapper types must each
/// be declared exactly once there. The deliberate compile-fail fixtures under
/// `crates/adapters/tests/ui/` are this scan's positive sentinel — they contain the raw
/// type on purpose, so zero hits there means the scanner went blind (§53.3 规则3 同款).
/// The runtime half of G80-40 (SELECT current_user + SQL 双向夹具) lives in
/// `crates/adapters/tests/` and is judged by `cargo test`, not here.
pub fn g6_db_pool_topology(root: &Path) -> Verdict {
    let postgres_rs = root.join("crates/adapters/src/postgres.rs");
    let Ok(pool_src) = fs::read_to_string(&postgres_rs) else {
        return Verdict::NotApplicable(
            "crates/adapters/src/postgres.rs (§6.2.3 唯一封装点尚未交付)".to_string(),
        );
    };

    let mut problems = Vec::new();

    // Four wrappers, each declared exactly once, all in the encapsulation point.
    for wrapper in [
        "RuntimeDbPool",
        "BatchIssuerDbPool",
        "ConsolidationDbPool",
        "PrivateWorkerDbPool",
    ] {
        let decl = format!("pub struct {wrapper}");
        let n = pool_src.matches(&decl).count();
        if n != 1 {
            problems.push(format!(
                "§6.2.3 wrapper `{wrapper}` 声明数 {n} != 1（唯一封装点 postgres.rs）"
            ));
        }
    }

    // Positive control (scanner-is-alive): the encapsulation point itself must name the
    // raw type at least once — the four wrappers each hold a `sqlx::PgPool` inner field.
    // If this drops to 0 the scanner (or postgres.rs) is broken; a dead scanner would
    // otherwise report "no violations" forever. (The ui/ compile-fail fixtures test field
    // *privacy* via `.0`, not raw-type naming, so they are not a naming sentinel.)
    let home_hits = count_pgpool_type(&pool_src);
    if home_hits == 0 {
        problems.push(
            "positive sentinel dead: postgres.rs names sqlx::PgPool 0 times (§53.3 规则3 \
             同款——扫描器失明或封装点被掏空)"
                .to_string(),
        );
    }

    // Raw `sqlx::PgPool` naming outside the encapsulation point.
    for file in walk_files(&root.join("crates"), &["rs"]) {
        let disp = file
            .strip_prefix(root)
            .unwrap_or(&file)
            .to_string_lossy()
            .replace('\\', "/");
        let Ok(src) = fs::read_to_string(&file) else {
            continue;
        };
        let hits = count_pgpool_type(&src);
        if hits == 0 {
            continue;
        }
        if disp.ends_with("crates/adapters/src/postgres.rs") {
            continue; // the one legal home
        }
        problems.push(format!(
            "raw PgPool type named outside encapsulation point: {disp} ({hits} hit(s))"
        ));
    }

    if problems.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(problems)
    }
}

pub fn run(_args: &[String]) -> i32 {
    let root = workspace_root();
    let checks: Vec<(&str, Verdict)> = vec![
        (
            "§53.3 规则1 (Outcome<_> fallback outside abstain)",
            rule1_forbidden_fallback(&root),
        ),
        (
            "§53.3 规则2（前两项：DegradeCode ↔ fault test 文件名一一对应；标签基数由 \
             degrade.rs::fold_is_injective_over_all_variants 静态代理覆盖）",
            rule2_degrade_fault_parity(&root),
        ),
        (
            "§53.3 规则3 (sentinels positive control)",
            rule3_positive_sentinels(&root),
        ),
        (
            "§52.4 G52-1 (ErrorCode sole at error_code position)",
            g52_1_error_code_uniqueness(&root),
        ),
        (
            "§59.1 G59-3 (Authority::new sole construction point)",
            g59_3_authority_construction_point(&root),
        ),
        (
            "§78.3 Workspace Dependency Rule",
            dependency_rule_check(&root),
        ),
        (
            "§78 boundary lint (no env::var outside config/bootstrap)",
            env_var_scan(&root),
        ),
        (
            "§55.1 G80-2 (build_request sole construction point)",
            g80_2_build_request_unique(&root),
        ),
        (
            "§48.0① G80-22 (payload_sha256 sole construction point)",
            g80_22_payload_sha256_unique(&root),
        ),
        (
            "§6.2.3 G80-40 static (typed DB pool topology)",
            g6_db_pool_topology(&root),
        ),
        (
            "§83.4 G80-3 (outbound network choke point real RHS)",
            g80_3_outbound_choke_point(&root),
        ),
        (
            "§11.8 (classify sole construction point)",
            g11_8_classify_sole_construction_point(&root),
        ),
    ];

    let mut had_fail = false;
    for (name, verdict) in &checks {
        if report(name, verdict) {
            had_fail = true;
        }
    }
    if had_fail { 1 } else { 0 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn real_root() -> PathBuf {
        workspace_root()
    }

    // -- §53.3 规则1/规则3: scan_outcome_fallback_violations ------------------------------

    #[test]
    fn rule1_flags_unwrap_or_default_outside_abstain() {
        let src = "fn pick(pool: &[i32]) -> Outcome<i32> {\n    let c = pool.first().copied().unwrap_or_default();\n    Outcome::Decided(c)\n}\n";
        let hits = scan_outcome_fallback_violations("f.rs", src);
        assert_eq!(hits.len(), 1, "{hits:?}");
    }

    #[test]
    fn rule1_flags_unwrap_or_paren() {
        let src = "fn r(x: Option<i32>) -> Outcome<i32> {\n    let v = x.unwrap_or(0);\n    Outcome::Decided(v)\n}\n";
        assert_eq!(scan_outcome_fallback_violations("f.rs", src).len(), 1);
    }

    #[test]
    fn rule1_flags_ok_none() {
        let src = "fn l() -> Outcome<Result<Option<i32>, E>> {\n    return Outcome::Decided(Ok(None));\n}\n";
        assert_eq!(scan_outcome_fallback_violations("f.rs", src).len(), 1);
    }

    #[test]
    fn rule1_flags_ok_vec_new() {
        let src = "fn l() -> Outcome<Result<Vec<i32>, E>> {\n    return Outcome::Decided(Ok(Vec::new()));\n}\n";
        assert_eq!(scan_outcome_fallback_violations("f.rs", src).len(), 1);
    }

    #[test]
    fn rule1_allows_fallback_on_the_abstain_call_chain() {
        let src = "fn r(x: Option<i32>) -> Outcome<i32> {\n    x.map(Outcome::Decided).unwrap_or_else(|| abstain(DegradeCode::X, 0))\n}\n";
        assert!(scan_outcome_fallback_violations("f.rs", src).is_empty());
    }

    #[test]
    fn rule1_ignores_functions_not_returning_outcome() {
        let src = "fn plain(x: Option<i32>) -> i32 {\n    x.unwrap_or_default()\n}\n";
        assert!(scan_outcome_fallback_violations("f.rs", src).is_empty());
    }

    #[test]
    fn rule1_ignores_comment_mentioning_the_pattern() {
        let src = "// example: unwrap_or_default() inside Outcome<T> is forbidden\nfn real() -> Outcome<i32> {\n    Outcome::Decided(1)\n}\n";
        assert!(scan_outcome_fallback_violations("f.rs", src).is_empty());
    }

    #[test]
    fn rule1_real_repo_source_is_clean() {
        // §80.1 准入条件的另一半：真实代码今天必须是绿的，不是靠样本硬凑出来的绿。
        let root = real_root();
        assert_eq!(rule1_forbidden_fallback(&root), Verdict::Pass);
    }

    // -- §53.3 规则3: real sentinels hit exactly 3 -----------------------------------------

    #[test]
    fn rule3_real_sentinels_hit_exactly_three() {
        let root = real_root();
        assert_eq!(rule3_positive_sentinels(&root), Verdict::Pass);
    }

    /// 注错 (G80-1, §80.1.1): rename `sentinels/` away → 0 hits ≠ 3 → red. Uses a tempdir
    /// copy of the real fixtures, never the repo's own sentinels/ directory.
    #[test]
    fn rule3_fault_renamed_sentinels_dir_is_red() {
        let tmp = std::env::temp_dir().join(format!(
            "arch-check-rule3-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(tmp.join("crates/testkit/sentinels_renamed")).unwrap();
        // deliberately NOT named "sentinels" — simulates the injected fault.
        let verdict = rule3_positive_sentinels(&tmp);
        assert!(matches!(verdict, Verdict::Fail(_)), "{verdict:?}");
        fs::remove_dir_all(&tmp).ok();
    }

    /// 正向对照：把真实 3 个正哨兵样本复制进 tempdir 的正确路径 → 必须绿，证明红转绿真的
    /// 是「目录名」这一个变量在起作用，不是 fixture 本身写死了红。
    #[test]
    fn rule3_fixture_copy_at_correct_path_is_green() {
        let root = real_root();
        let real_sentinels = root.join("crates/testkit/sentinels");
        let tmp = std::env::temp_dir().join(format!(
            "arch-check-rule3-green-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let dest = tmp.join("crates/testkit/sentinels");
        fs::create_dir_all(&dest).unwrap();
        for entry in fs::read_dir(&real_sentinels).unwrap().flatten() {
            let p = entry.path();
            fs::copy(&p, dest.join(p.file_name().unwrap())).unwrap();
        }
        assert_eq!(rule3_positive_sentinels(&tmp), Verdict::Pass);
        fs::remove_dir_all(&tmp).ok();
    }

    // -- §53.3 规则2 ---------------------------------------------------------------------

    #[test]
    fn pascal_to_snake_matches_spec_examples() {
        assert_eq!(
            pascal_to_snake("ProjectionInvisibleLoss"),
            "projection_invisible_loss"
        );
        assert_eq!(
            pascal_to_snake("RerankProviderTimeout"),
            "rerank_provider_timeout"
        );
    }

    #[test]
    fn parse_degrade_variant_names_matches_real_degrade_rs() {
        let root = real_root();
        let src = fs::read_to_string(root.join("crates/telemetry/src/degrade.rs")).unwrap();
        let variants = parse_degrade_variant_names(&src);
        assert_eq!(variants.len(), 10, "{variants:?}");
        assert!(variants.contains(&"ProjectionInvisibleLoss".to_string()));
        assert!(variants.contains(&"RerankProviderTimeout".to_string()));
    }

    #[test]
    fn rule2_check_passes_when_names_match_exactly() {
        let variants = vec!["FooBar".to_string(), "Baz".to_string()];
        let mut stems = BTreeSet::new();
        stems.insert("foo_bar".to_string());
        stems.insert("baz".to_string());
        assert_eq!(rule2_check(&variants, &stems), Verdict::Pass);
    }

    /// T0.4 (§53.4 fault test files) lands in the same wave as this checker, in parallel —
    /// an oracle test rather than a hardcoded verdict: independently re-derives the expected
    /// verdict from the same two real inputs (`degrade.rs`'s variant list, the live
    /// `tests/fault/` directory listing) via [`rule2_check`], and asserts the wired-up
    /// [`rule2_degrade_fault_parity`] agrees — true whether T0.4 has landed yet or not, so it
    /// can't go stale/racy against the other group's concurrent progress (task hard rule ⑤:
    /// never weaken the judgment to chase "currently green").
    #[test]
    fn rule2_real_repo_matches_independently_recomputed_expectation() {
        let root = real_root();
        let degrade_src = fs::read_to_string(root.join("crates/telemetry/src/degrade.rs")).unwrap();
        let variants = parse_degrade_variant_names(&degrade_src);
        let fault_dir = root.join("crates/testkit/tests/fault");
        let stems: BTreeSet<String> = fs::read_dir(&fault_dir)
            .unwrap()
            .flatten()
            .filter_map(|e| {
                let p = e.path();
                if p.extension().and_then(|x| x.to_str()) == Some("rs") {
                    p.file_stem().and_then(|s| s.to_str()).map(String::from)
                } else {
                    None
                }
            })
            .collect();
        let expected = rule2_check(&variants, &stems);
        assert_eq!(rule2_degrade_fault_parity(&root), expected);
    }

    /// 注错: fault file present for a variant that no longer exists in DegradeCode → 红
    /// (orphaned fault file, extra != []).
    #[test]
    fn rule2_fault_orphaned_fault_file_is_red() {
        let variants = vec!["FooBar".to_string()];
        let mut stems = BTreeSet::new();
        stems.insert("foo_bar".to_string());
        stems.insert("stale_variant".to_string());
        assert!(matches!(rule2_check(&variants, &stems), Verdict::Fail(_)));
    }

    // -- §52.4 G52-1 ---------------------------------------------------------------------

    #[test]
    fn find_error_code_field_types_captures_pascal_case_type() {
        let src = "struct M {\n    pub error_code: ErrorCode,\n}\n";
        assert_eq!(
            find_error_code_field_types(src),
            vec!["ErrorCode".to_string()]
        );
    }

    #[test]
    fn find_error_code_field_types_ignores_lowercase_value_construction() {
        let src = "M { error_code: code, other: 1 }";
        assert!(find_error_code_field_types(src).is_empty());
    }

    #[test]
    fn find_error_code_field_types_ignores_str_field() {
        let src = "struct P {\n    pub error_code: &'static str,\n}\n";
        assert!(find_error_code_field_types(src).is_empty());
    }

    #[test]
    fn find_error_code_field_types_ignores_error_codes_plural() {
        let src = "let error_codes: std::collections::HashSet<&str> = HashSet::new();";
        assert!(find_error_code_field_types(src).is_empty());
    }

    #[test]
    fn g52_1_check_passes_on_exactly_error_code() {
        let mut set = BTreeSet::new();
        set.insert("ErrorCode".to_string());
        assert_eq!(g52_1_check(&set), Verdict::Pass);
    }

    #[test]
    fn g52_1_check_fails_on_empty_set_positive_control_missing() {
        assert!(matches!(g52_1_check(&BTreeSet::new()), Verdict::Fail(_)));
    }

    /// 注错: a second enum (e.g. `crates/billing::BillingError`) shows up at an
    /// `error_code` position → red (§52.4 G52-1's own named injection).
    #[test]
    fn g52_1_check_fails_when_second_enum_present() {
        let mut set = BTreeSet::new();
        set.insert("ErrorCode".to_string());
        set.insert("BillingError".to_string());
        assert!(matches!(g52_1_check(&set), Verdict::Fail(_)));
    }

    #[test]
    fn g52_1_real_repo_passes() {
        let root = real_root();
        assert_eq!(g52_1_error_code_uniqueness(&root), Verdict::Pass);
    }

    // -- §59.1 G59-3 ---------------------------------------------------------------------

    #[test]
    fn find_authority_struct_literals_matches_bare_authority_brace() {
        let src = "let a = Authority { class, confidence };";
        assert_eq!(find_authority_struct_literals(src).len(), 1);
    }

    #[test]
    fn find_authority_struct_literals_ignores_authority_class() {
        let src = "let c = AuthorityClass::PublicKnowledge; let s = AuthorityStatus::Active; let p = AuthorityPolicy {};";
        assert!(find_authority_struct_literals(src).is_empty());
    }

    #[test]
    fn find_authority_struct_literals_ignores_comment() {
        let src = "// Authority { .. } must be built only by Authority::new\nlet x = 1;";
        assert!(find_authority_struct_literals(src).is_empty());
    }

    #[test]
    fn g59_3_check_fails_on_zero_hits() {
        assert!(matches!(g59_3_check(&[], None), Verdict::Fail(_)));
    }

    /// 注错 a (§59.1 G59-3): a second `Authority {` literal appears in `evals/` → count 1→2 → 红.
    #[test]
    fn g59_3_check_fails_on_two_hits() {
        let hits = vec![
            ("crates/domain/src/authority.rs".to_string(), 100),
            ("evals/probe.rs".to_string(), 5),
        ];
        assert!(matches!(
            g59_3_check(&hits, Some((0, 200))),
            Verdict::Fail(_)
        ));
    }

    #[test]
    fn g59_3_check_fails_when_sole_hit_outside_new() {
        let hits = vec![("crates/domain/src/authority.rs".to_string(), 500)];
        // range (0,200) does not contain 500 -> outside new()
        assert!(matches!(
            g59_3_check(&hits, Some((0, 200))),
            Verdict::Fail(_)
        ));
    }

    #[test]
    fn g59_3_check_passes_when_sole_hit_inside_new_in_authority_rs() {
        let hits = vec![("crates/domain/src/authority.rs".to_string(), 150)];
        assert_eq!(g59_3_check(&hits, Some((100, 200))), Verdict::Pass);
    }

    #[test]
    fn authority_new_body_range_finds_real_shape() {
        let src = "impl Authority {\n    pub fn new(x: i32) -> Self {\n        Authority { x }\n    }\n}\n";
        let range = authority_new_body_range(src);
        assert!(range.is_some());
        let (s, e) = range.unwrap();
        let hit = find_authority_struct_literals(src)[0];
        assert!((s..e).contains(&hit), "hit {hit} not in range {s}..{e}");
    }

    /// T0.7 has landed `Authority::new` as the sole real construction point (`crates/domain/src
    /// /authority.rs`) — this must be a hard `Pass`, not a Fail/Pass-both-accepted assertion.
    /// A tautological version of this test (accepting either verdict) would keep passing even
    /// while the matcher itself is broken, exactly the "恒真闸" shape §59.1 exists to rule out.
    #[test]
    fn g59_3_real_repo_hits_exactly_once_inside_authority_new() {
        let root = real_root();
        assert_eq!(g59_3_authority_construction_point(&root), Verdict::Pass);
    }

    // -- §78.3 Workspace Dependency Rule ---------------------------------------------------

    fn metadata_fixture(domain_deps: &[&str]) -> String {
        let dom: Vec<String> = domain_deps
            .iter()
            .map(|d| format!(r#"{{"name":"{d}"}}"#))
            .collect();
        format!(
            r#"{{"packages":[{{"name":"humaux-domain","dependencies":[{}]}}]}}"#,
            dom.join(",")
        )
    }

    #[test]
    fn dependency_rule_passes_on_clean_fixture() {
        let json = metadata_fixture(&["uuid"]);
        assert_eq!(dependency_rule_from_metadata_json(&json), Verdict::Pass);
    }

    /// 注错 (§78.3): domain gains an sqlx dependency → red.
    #[test]
    fn dependency_rule_fails_when_domain_depends_on_sqlx() {
        let json = metadata_fixture(&["uuid", "sqlx"]);
        assert!(matches!(
            dependency_rule_from_metadata_json(&json),
            Verdict::Fail(_)
        ));
    }

    #[test]
    fn dependency_rule_fails_on_sdk_substring_catch_all() {
        let json = metadata_fixture(&["aws-sdk-dynamodb"]);
        assert!(matches!(
            dependency_rule_from_metadata_json(&json),
            Verdict::Fail(_)
        ));
    }

    /// 注错 (§78.3): domain gains a dashscope dependency → red (the item the old list was
    /// missing entirely — §78.3's verbatim seven-item list names it).
    #[test]
    fn dependency_rule_fails_when_domain_depends_on_dashscope() {
        let json = metadata_fixture(&["dashscope"]);
        assert!(matches!(
            dependency_rule_from_metadata_json(&json),
            Verdict::Fail(_)
        ));
    }

    /// 注错 (Architecture Boundary Lints 第1行 "adapters/protocol/runtime"): domain gains a
    /// humaux-protocol dependency → red.
    #[test]
    fn dependency_rule_fails_when_domain_depends_on_protocol() {
        let json = metadata_fixture(&["humaux-protocol"]);
        assert!(matches!(
            dependency_rule_from_metadata_json(&json),
            Verdict::Fail(_)
        ));
    }

    /// §59.1 G59-3 同款「恒真闸」: `humaux-domain` absent from the metadata document must be a
    /// red, not a silent pass — a matcher whose target package went missing must never read as
    /// "nothing to report".
    #[test]
    fn dependency_rule_fails_when_domain_package_missing_from_metadata() {
        let json = r#"{"packages":[{"name":"humaux-application","dependencies":[]}]}"#;
        assert!(matches!(
            dependency_rule_from_metadata_json(json),
            Verdict::Fail(_)
        ));
    }

    #[test]
    fn dependency_rule_real_repo_passes() {
        let root = real_root();
        assert_eq!(dependency_rule_check(&root), Verdict::Pass);
    }

    // -- §78 env::var boundary lint --------------------------------------------------------

    #[test]
    fn env_var_scan_flags_std_env_var() {
        let src = "fn f() {\n    let x = std::env::var(\"X\");\n}\n";
        assert_eq!(scan_env_var_violations("f.rs", src).len(), 1);
    }

    #[test]
    fn env_var_scan_ignores_comment() {
        let src = "// std::env::var(\"X\") is forbidden here\nfn f() {}\n";
        assert!(scan_env_var_violations("f.rs", src).is_empty());
    }

    #[test]
    fn env_var_scan_real_repo_passes() {
        let root = real_root();
        assert_eq!(env_var_scan(&root), Verdict::Pass);
    }

    /// 注错 (§78 boundary lints): inject `std::env::var` into a fixture file outside the
    /// contracts/bins exemption → red. Uses a tempdir copy, never the real crates tree.
    #[test]
    fn env_var_scan_fault_injected_call_outside_exemption_is_red() {
        let tmp = std::env::temp_dir().join(format!(
            "arch-check-envvar-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let crate_dir = tmp.join("crates/some_adapter/src");
        fs::create_dir_all(&crate_dir).unwrap();
        fs::write(
            crate_dir.join("lib.rs"),
            "pub fn f() { let _ = std::env::var(\"NOT_ALLOWED\"); }\n",
        )
        .unwrap();
        assert!(matches!(env_var_scan(&tmp), Verdict::Fail(_)));
        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn env_var_scan_exempts_contracts_dir() {
        let tmp = std::env::temp_dir().join(format!(
            "arch-check-envvar-exempt-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let crate_dir = tmp.join("crates/contracts/src");
        fs::create_dir_all(&crate_dir).unwrap();
        fs::write(
            crate_dir.join("lib.rs"),
            "pub fn f() { let _ = std::env::var(\"OK_HERE\"); }\n",
        )
        .unwrap();
        assert_eq!(env_var_scan(&tmp), Verdict::Pass);
        fs::remove_dir_all(&tmp).ok();
    }

    // -- G80-2 / G80-22 --------------------------------------------------------------------

    #[test]
    fn g80_2_real_repo_is_not_applicable() {
        let root = real_root();
        assert!(matches!(
            g80_2_build_request_unique(&root),
            Verdict::NotApplicable(_)
        ));
    }

    /// T1.6 delivers `evidence::payload_sha256` / `EvidencePayloadSha256` (§48.0① G80-22): the
    /// real-repo check flips from `NotApplicable` (Phase 6 undelivered) to `Pass` (exactly one
    /// construction site, in `crates/domain/src/evidence.rs`).
    #[test]
    fn g80_22_real_repo_passes() {
        let root = real_root();
        assert_eq!(g80_22_payload_sha256_unique(&root), Verdict::Pass);
    }

    fn fresh_tmp(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "arch-check-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn g80_2_passes_with_exactly_one_construction_site() {
        let tmp = fresh_tmp("g80-2-green");
        let dir = tmp.join("crates/retrieval/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("lib.rs"),
            "struct RetrievalRequest { limit: usize }\npub fn build_request() -> RetrievalRequest {\n    RetrievalRequest { limit: 5 }\n}\n",
        )
        .unwrap();
        assert_eq!(g80_2_build_request_unique(&tmp), Verdict::Pass);
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 (G80-2, §80.1.1): a second construction site in the real top-level `evals/` — a
    /// sibling of `crates/`, **not** nested under it — → count 1→2 → 红. Placing it as a true
    /// sibling (rather than `crates/evals_stand_in/`) is the point: the old crates/-only scan
    /// domain would have missed this fault entirely (evals/ sits outside crates/), so this
    /// test only passes because the scan domain is now the whole workspace root.
    #[test]
    fn g80_2_fails_with_second_construction_site_in_real_evals_dir() {
        let tmp = fresh_tmp("g80-2-red-evals");
        let dir = tmp.join("crates/retrieval/src");
        let evals_dir = tmp.join("evals");
        fs::create_dir_all(&dir).unwrap();
        fs::create_dir_all(&evals_dir).unwrap();
        fs::write(
            dir.join("lib.rs"),
            "struct RetrievalRequest { limit: usize }\npub fn build_request() -> RetrievalRequest {\n    RetrievalRequest { limit: 5 }\n}\n",
        )
        .unwrap();
        fs::write(
            evals_dir.join("probe.rs"),
            "fn shortcut() { let _ = RetrievalRequest { limit: 60 }; }\n",
        )
        .unwrap();
        assert!(matches!(g80_2_build_request_unique(&tmp), Verdict::Fail(_)));
        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn g80_22_passes_with_exactly_one_construction_site() {
        let tmp = fresh_tmp("g80-22-green");
        let dir = tmp.join("crates/domain/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("evidence.rs"),
            "struct EvidencePayloadSha256([u8; 32]);\npub fn payload_sha256(bytes: &[u8]) -> EvidencePayloadSha256 {\n    EvidencePayloadSha256([0; 32])\n}\n",
        )
        .unwrap();
        assert_eq!(g80_22_payload_sha256_unique(&tmp), Verdict::Pass);
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 (G80-22, §48.0①): matcher's type name typo'd to a nonexistent name → 0 hits → 红
    /// (same shape as §59.1 G59-3 注错 b — this is exercised at the pure-fn level directly).
    #[test]
    fn g80_22_fails_when_object_exists_but_zero_construction_sites_found() {
        let tmp = fresh_tmp("g80-22-red-zero");
        let dir = tmp.join("crates/domain/src");
        fs::create_dir_all(&dir).unwrap();
        // object exists (fn payload_sha256 defined) but never actually constructs the newtype
        // under its real name — simulates the matcher-typo failure mode.
        fs::write(
            dir.join("evidence.rs"),
            "struct EvidencePayloadSha256([u8; 32]);\npub fn payload_sha256(bytes: &[u8]) -> EvidencePayloadSha256 {\n    todo!()\n}\n",
        )
        .unwrap();
        assert!(matches!(
            g80_22_payload_sha256_unique(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 (G80-22, §48.0①): same shape as `g80_2_fails_with_second_construction_site_in_real_evals_dir`
    /// — the second site lives in a real top-level `evals/`, outside the old crates/-only scan
    /// domain, proving the widened workspace-root walk (not a crates/-nested stand-in) is what
    /// catches it.
    #[test]
    fn g80_22_fails_with_second_construction_site_in_real_evals_dir() {
        let tmp = fresh_tmp("g80-22-red-evals");
        let dir = tmp.join("crates/domain/src");
        let evals_dir = tmp.join("evals");
        fs::create_dir_all(&dir).unwrap();
        fs::create_dir_all(&evals_dir).unwrap();
        fs::write(
            dir.join("evidence.rs"),
            "struct EvidencePayloadSha256([u8; 32]);\npub fn payload_sha256(bytes: &[u8]) -> EvidencePayloadSha256 {\n    EvidencePayloadSha256([0; 32])\n}\n",
        )
        .unwrap();
        fs::write(
            evals_dir.join("probe.rs"),
            "fn shortcut() { let _ = EvidencePayloadSha256([1; 32]); }\n",
        )
        .unwrap();
        assert!(matches!(
            g80_22_payload_sha256_unique(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    // -- widened scan domain: bins/ / evals/ now genuinely inside the four workspace-wide
    // -- checks' territory, not just crates/ (§80.1.1 / §48.0① / §59.1 / §52.4) -------------

    /// 注错 (§52.4 G52-1 "扫全 workspace"): a second error enum at an `error_code` position in
    /// `bins/`, previously outside the old crates/-only scan domain.
    #[test]
    fn g52_1_fails_on_second_enum_found_in_bins_dir() {
        let tmp = fresh_tmp("g52-1-red-bins");
        let dir = tmp.join("bins/worker/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("main.rs"),
            "struct M {\n    pub error_code: BillingError,\n}\n",
        )
        .unwrap();
        assert!(matches!(
            g52_1_error_code_uniqueness(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 a (§59.1 G59-3, integration level): a second `Authority {` literal in the real
    /// top-level `evals/` — the exact site named by the spec's own G59-3 注错a — was invisible
    /// to the old crates/domain-only scan domain; the widened workspace-root walk must catch
    /// it via the wired-up [`g59_3_authority_construction_point`], not just the pure comparator.
    #[test]
    fn g59_3_fails_on_second_hit_in_real_evals_dir() {
        let tmp = fresh_tmp("g59-3-red-evals");
        let dir = tmp.join("crates/domain/src");
        let evals_dir = tmp.join("evals");
        fs::create_dir_all(&dir).unwrap();
        fs::create_dir_all(&evals_dir).unwrap();
        fs::write(
            dir.join("authority.rs"),
            "impl Authority {\n    pub fn new() -> Self {\n        Authority { x: 1 }\n    }\n}\n",
        )
        .unwrap();
        fs::write(
            evals_dir.join("probe.rs"),
            "fn shortcut() { let _ = Authority { x: 2 }; }\n",
        )
        .unwrap();
        assert!(matches!(
            g59_3_authority_construction_point(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 正向对照 for the same fixture: with the second site removed, the sole real hit is inside
    /// `authority.rs::Authority::new`'s body → green — proves the red above is really about the
    /// second site, not some other defect in the fixture.
    #[test]
    fn g59_3_passes_with_only_the_real_evals_dir_present_and_empty() {
        let tmp = fresh_tmp("g59-3-green-evals");
        let dir = tmp.join("crates/domain/src");
        let evals_dir = tmp.join("evals");
        fs::create_dir_all(&dir).unwrap();
        fs::create_dir_all(&evals_dir).unwrap();
        fs::write(
            dir.join("authority.rs"),
            "impl Authority {\n    pub fn new() -> Self {\n        Authority { x: 1 }\n    }\n}\n",
        )
        .unwrap();
        assert_eq!(g59_3_authority_construction_point(&tmp), Verdict::Pass);
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 (§78 boundary lints, widened domain): `std::env::var` planted in the real top-level
    /// `evals/` — not `bins/`, not `xtask/`, so no exemption applies — was invisible to the old
    /// crates/-only scan domain.
    #[test]
    fn env_var_scan_fails_on_call_in_real_evals_dir() {
        let tmp = fresh_tmp("envvar-red-evals");
        let dir = tmp.join("evals");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("probe.rs"),
            "pub fn f() { let _ = std::env::var(\"X\"); }\n",
        )
        .unwrap();
        assert!(matches!(env_var_scan(&tmp), Verdict::Fail(_)));
        fs::remove_dir_all(&tmp).ok();
    }

    /// §78 boundary lint: `xtask/` is bootstrap/CI-gate tooling, exempt like `bins/*` — a real
    /// call there (mirroring `contract_impact.rs`'s own `std::env::var("CI")`) must stay green.
    #[test]
    fn env_var_scan_exempts_xtask_dir() {
        let tmp = fresh_tmp("envvar-exempt-xtask");
        let dir = tmp.join("xtask/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("some_tool.rs"),
            "pub fn f() { let _ = std::env::var(\"CI\"); }\n",
        )
        .unwrap();
        assert_eq!(env_var_scan(&tmp), Verdict::Pass);
        fs::remove_dir_all(&tmp).ok();
    }

    /// §78 short-form spelling: `use std::env;` then bare `env::var(` must be caught, not just
    /// the fully-qualified `std::env::var`.
    #[test]
    fn env_var_scan_flags_short_form_after_use_std_env() {
        let src = "use std::env;\nfn f() {\n    let x = env::var(\"X\");\n}\n";
        assert_eq!(scan_env_var_violations("f.rs", src).len(), 1);
    }

    /// §78 re-exported short form: `use std::env::var;` then a bare `var(` call must be caught.
    #[test]
    fn env_var_scan_flags_bare_var_after_use_std_env_var() {
        let src = "use std::env::var;\nfn f() {\n    let x = var(\"X\");\n}\n";
        assert_eq!(scan_env_var_violations("f.rs", src).len(), 1);
    }

    /// A bare `var(` call with no `use std::env::var` import anywhere in the file is not an
    /// env-var access at all (some unrelated local fn named `var`) and must not be flagged.
    #[test]
    fn env_var_scan_ignores_unrelated_bare_var_call_without_the_import() {
        let src = "fn var(x: i32) -> i32 { x }\nfn f() -> i32 { var(1) }\n";
        assert!(scan_env_var_violations("f.rs", src).is_empty());
    }

    // -- §59.1 G59-3: `Self {}` construction inside `impl Authority` scope ------------------

    /// The real spelling `Authority::new` is free to use: `Ok(Self { .. })`, the idiomatic
    /// form `clippy::use_self` prefers — must be recognized as a construction site exactly
    /// like the bare `Authority { .. }` spelling.
    #[test]
    fn find_authority_struct_literals_matches_self_inside_impl_authority() {
        let src = "impl Authority {\n    pub fn new(x: i32) -> Result<Self, E> {\n        Ok(Self { x })\n    }\n}\n";
        assert_eq!(find_authority_struct_literals(src).len(), 1);
    }

    /// `Self { .. }` outside any `impl Authority` block (a different type's own constructor)
    /// must not be mistaken for an Authority construction site.
    #[test]
    fn find_authority_struct_literals_ignores_self_outside_impl_authority() {
        let src = "impl Other {\n    fn new() -> Self {\n        Self { x: 1 }\n    }\n}\n";
        assert!(find_authority_struct_literals(src).is_empty());
    }

    /// `-> Self {` is `Authority::new`'s own fn-signature body brace, not a `Self { .. }`
    /// struct literal — must not double-count alongside the real `Authority { .. }` literal in
    /// the same body.
    #[test]
    fn find_authority_struct_literals_does_not_miscount_arrow_self_as_a_literal() {
        let src = "impl Authority {\n    pub fn new(x: i32) -> Self {\n        Authority { x }\n    }\n}\n";
        assert_eq!(find_authority_struct_literals(src).len(), 1);
    }
    /// G80-40 static: real repo must pass (four wrappers once each, no raw pool outside,
    /// ui fixtures keep the sentinel alive).
    #[test]
    fn g80_40_real_repo_is_green() {
        assert_eq!(g6_db_pool_topology(&real_root()), Verdict::Pass);
    }

    /// 注错 a：把 raw `sqlx::PgPool` 写进封装点之外的生产文件 ⇒ 红并点名文件。
    #[test]
    fn g80_40_fault_raw_pool_outside_encapsulation_is_red() {
        let tmp = fresh_tmp("g80-40-raw-pool");
        copy_adapters_fixture(&tmp);
        fs::write(
            tmp.join("crates/other/src/lib.rs"),
            "pub struct Svc { pool: sqlx::PgPool }\n",
        )
        .unwrap();
        match g6_db_pool_topology(&tmp) {
            Verdict::Fail(lines) => {
                assert!(lines.iter().any(|l| l.contains("crates/other/src/lib.rs")))
            }
            other => panic!("expected Fail, got {other:?}"),
        }
    }

    /// 注错 b：掏空封装点里 `sqlx::PgPool` 的实际命名（只留注释提及） ⇒ 红——
    /// 扫描器失明（永远匹配 0）必须可观察。
    #[test]
    fn g80_40_fault_dead_sentinel_is_red() {
        let tmp = fresh_tmp("g80-40-sentinel");
        copy_adapters_fixture(&tmp);
        // wrappers still declared (so the four-count check passes) but their inner field
        // no longer *names* the raw type — only a comment mentions it. A live scanner
        // must notice the naming sentinel went to 0.
        fs::write(
            tmp.join("crates/adapters/src/postgres.rs"),
            "// inner is a sqlx::PgPool, morally\n\
             pub struct RuntimeDbPool(());\n\
             pub struct BatchIssuerDbPool(());\n\
             pub struct ConsolidationDbPool(());\n\
             pub struct PrivateWorkerDbPool(());\n",
        )
        .unwrap();
        match g6_db_pool_topology(&tmp) {
            Verdict::Fail(lines) => assert!(lines.iter().any(|l| l.contains("sentinel"))),
            other => panic!("expected Fail, got {other:?}"),
        }
    }

    /// fixture: minimal adapters crate shape for the two injections above.
    fn copy_adapters_fixture(tmp: &Path) {
        for d in [
            "crates/adapters/src",
            "crates/adapters/tests/ui",
            "crates/other/src",
        ] {
            fs::create_dir_all(tmp.join(d)).unwrap();
        }
        fs::write(
            tmp.join("crates/adapters/src/postgres.rs"),
            "pub struct RuntimeDbPool { inner: sqlx::PgPool }\n\
             pub struct BatchIssuerDbPool { inner: sqlx::PgPool }\n\
             pub struct ConsolidationDbPool { inner: sqlx::PgPool }\n\
             pub struct PrivateWorkerDbPool { inner: sqlx::PgPool }\n",
        )
        .unwrap();
        fs::write(
            tmp.join("crates/adapters/tests/ui/fail_raw_pool_field.rs"),
            "struct Bad { pool: sqlx::PgPool }\nfn main() {}\n",
        )
        .unwrap();
        fs::write(tmp.join("crates/other/src/lib.rs"), "\n").unwrap();
    }

    // -- §83.4 G80-3: Outbound Network Choke Point ------------------------------------------

    fn outbound_purpose_fixture() -> String {
        "pub enum OutboundPurpose {\n    \
             USER_REASONING(EgressPermit),\n    \
             RETRIEVAL_EMBEDDING(EgressPermit),\n    \
             RETRIEVAL_RERANK(EgressPermit),\n    \
             PUBLIC_REASONING,\n    \
             BILLING,\n    \
             TRANSACTIONAL_EMAIL,\n    \
             GIT_PROVIDER,\n    \
             OAUTH_METADATA,\n    \
             PUBLIC_SOURCE_FETCH,\n    \
             DEADMAN_HEALTHCHECK,\n\
         }\n"
        .to_string()
    }

    /// §80.1 准入条件: the real repo, once T4.1 lands, must be green end to end (includes a
    /// real `cargo metadata` shell-out — this is the one test in this section that is not a
    /// pure-fixture unit test).
    #[test]
    fn g80_3_real_repo_is_green() {
        assert_eq!(g80_3_outbound_choke_point(&real_root()), Verdict::Pass);
    }

    /// Positive control: the exact fixture shape above, scanned in isolation, must itself be
    /// clean — proves a later red is the injected fault, not a matcher that is broken by
    /// construction.
    #[test]
    fn g80_3_clean_fixture_has_no_problems() {
        let root = PathBuf::from("/fixture-root");
        let files = vec![
            (
                root.join("crates/infra-egress/src/http.rs"),
                "pub fn f() -> reqwest::Client { reqwest::Client::new() }\n".to_string(),
            ),
            (
                root.join("crates/domain/src/egress.rs"),
                outbound_purpose_fixture(),
            ),
        ];
        assert!(g80_3_transport_and_registry_check(&files, &root).is_empty());
    }

    /// 注错 a (§83.4): a raw `reqwest::Client::new` written in some other crate must turn
    /// 判据1 red and name the offending file, alongside the legitimate one.
    #[test]
    fn g80_3_fault_raw_client_outside_http_rs_is_red_and_named() {
        let root = PathBuf::from("/fixture-root");
        let files = vec![
            (
                root.join("crates/infra-egress/src/http.rs"),
                "pub fn f() -> reqwest::Client { reqwest::Client::new() }\n".to_string(),
            ),
            (
                root.join("crates/domain/src/egress.rs"),
                outbound_purpose_fixture(),
            ),
            (
                root.join("crates/adapters/src/retrieval.rs"),
                "pub fn g() -> reqwest::Client { reqwest::Client::new() }\n".to_string(),
            ),
        ];
        let problems = g80_3_transport_and_registry_check(&files, &root);
        assert!(!problems.is_empty());
        assert!(
            problems
                .iter()
                .any(|p| p.contains("crates/adapters/src/retrieval.rs"))
        );
    }

    /// 注错 a, bare-alias variant (§83.4): the exact loophole a real reviewer found —
    /// `use reqwest::Client; Client::new()` (also `Client::default()` and
    /// `ClientBuilder::new()`) — must be caught even though it never spells `reqwest::` at the
    /// call site. Before [`BARE_HTTP_CLIENT_NEEDLES`] this whole file scanned clean.
    #[test]
    fn g80_3_fault_bare_alias_raw_client_is_red_and_named() {
        let root = PathBuf::from("/fixture-root");
        let files = vec![
            (
                root.join("crates/infra-egress/src/http.rs"),
                "pub fn f() -> reqwest::Client { reqwest::Client::new() }\n".to_string(),
            ),
            (
                root.join("crates/domain/src/egress.rs"),
                outbound_purpose_fixture(),
            ),
            (
                root.join("crates/adapters/src/_tmp_review_probe.rs"),
                "use reqwest::Client;\n\
                 pub fn a() -> Client { Client::new() }\n\
                 pub fn b() -> Client { Client::default() }\n\
                 pub fn c() -> reqwest::ClientBuilder { ClientBuilder::new() }\n"
                    .to_string(),
            ),
        ];
        let problems = g80_3_transport_and_registry_check(&files, &root);
        assert!(
            problems
                .iter()
                .any(|p| p.contains("crates/adapters/src/_tmp_review_probe.rs")),
            "bare `Client::new()`/`Client::default()`/`ClientBuilder::new()` must be caught: \
             {problems:?}"
        );
    }

    /// Negative control for [`is_bare_needle_word_start`]: a same-shaped but unrelated local
    /// type (`MyClient::new(`) must not be mistaken for `reqwest`/`hyper`'s `Client`.
    #[test]
    fn g80_3_bare_needle_does_not_match_unrelated_client_suffix_type() {
        let root = PathBuf::from("/fixture-root");
        let files = vec![
            (
                root.join("crates/infra-egress/src/http.rs"),
                "pub fn f() -> reqwest::Client { reqwest::Client::new() }\n".to_string(),
            ),
            (
                root.join("crates/domain/src/egress.rs"),
                outbound_purpose_fixture(),
            ),
            (
                root.join("crates/adapters/src/redis.rs"),
                "struct MyClient;\nimpl MyClient { fn f() -> Self { MyClient::new() } }\n"
                    .to_string(),
            ),
        ];
        assert!(g80_3_transport_and_registry_check(&files, &root).is_empty());
    }

    /// 注错 c (§83.4 判据3): a private-data variant that stops carrying `EgressPermit` (the
    /// spec's own "改成 generic NetworkPermit" wording, reproduced here as a bare variant —
    /// same red either way, since 判据3 checks the payload's *name*) must turn 判据3 red. Before
    /// this check existed, the module doc merely *claimed* trybuild fixtures covered this —
    /// they never did (none of `egress_topology_ui.rs`'s three fixtures touch `OutboundPurpose`
    /// at all), so this fault ran clean end to end.
    #[test]
    fn g80_3_fault_private_variant_without_permit_is_red() {
        let root = PathBuf::from("/fixture-root");
        let egress_src = outbound_purpose_fixture()
            .replace("RETRIEVAL_RERANK(EgressPermit),\n", "RETRIEVAL_RERANK,\n");
        let files = vec![
            (
                root.join("crates/infra-egress/src/http.rs"),
                "pub fn f() -> reqwest::Client { reqwest::Client::new() }\n".to_string(),
            ),
            (root.join("crates/domain/src/egress.rs"), egress_src),
        ];
        let problems = g80_3_transport_and_registry_check(&files, &root);
        assert!(
            problems
                .iter()
                .any(|p| p.contains("RETRIEVAL_RERANK") && p.contains("判据3")),
            "{problems:?}"
        );
    }

    /// 判据3 must also catch the payload being swapped for a *different* named type (the
    /// spec's literal "改成 generic NetworkPermit" wording), not just a bare variant.
    #[test]
    fn g80_3_fault_private_variant_wrong_payload_type_is_red() {
        let root = PathBuf::from("/fixture-root");
        let egress_src = outbound_purpose_fixture().replace(
            "USER_REASONING(EgressPermit),\n",
            "USER_REASONING(NetworkPermit),\n",
        );
        let files = vec![
            (
                root.join("crates/infra-egress/src/http.rs"),
                "pub fn f() -> reqwest::Client { reqwest::Client::new() }\n".to_string(),
            ),
            (root.join("crates/domain/src/egress.rs"), egress_src),
        ];
        let problems = g80_3_transport_and_registry_check(&files, &root);
        assert!(
            problems
                .iter()
                .any(|p| p.contains("USER_REASONING") && p.contains("NetworkPermit")),
            "{problems:?}"
        );
    }

    /// 注错 b (§83.4): `OutboundPurpose` growing an unregistered variant must turn 判据2 red.
    #[test]
    fn g80_3_fault_unregistered_purpose_variant_is_red() {
        let root = PathBuf::from("/fixture-root");
        let mut egress_src = outbound_purpose_fixture();
        egress_src = egress_src.replace(
            "DEADMAN_HEALTHCHECK,\n",
            "DEADMAN_HEALTHCHECK,\n    MYSTERY_PROVIDER,\n",
        );
        let files = vec![
            (
                root.join("crates/infra-egress/src/http.rs"),
                "pub fn f() -> reqwest::Client { reqwest::Client::new() }\n".to_string(),
            ),
            (root.join("crates/domain/src/egress.rs"), egress_src),
        ];
        let problems = g80_3_transport_and_registry_check(&files, &root);
        assert!(problems.iter().any(|p| p.contains("MYSTERY_PROVIDER")));
    }

    /// 注错 0a (§83.4): deleting `infra-egress/src/http.rs` must flip the positive sentinel
    /// red, even when `cargo metadata` still (on paper) lists the crate as a member.
    #[test]
    fn g80_3_sentinel_flags_deleted_http_rs() {
        let tmp = fresh_tmp("g80-3-deleted-http-rs");
        fs::create_dir_all(tmp.join("crates/infra-egress/src")).unwrap();
        fs::write(tmp.join("crates/infra-egress/Cargo.toml"), "").unwrap();
        // http.rs deliberately not written.
        let fake_metadata = r#"{"packages":[{"name":"humaux-infra-egress"}]}"#;
        let problems = g80_3_workspace_sentinel(&tmp, Some(fake_metadata));
        assert!(problems.iter().any(|p| p.contains(INFRA_EGRESS_HTTP_RS)));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 0b (§83.4): removing the crate from `cargo metadata`'s member list (e.g. dropped
    /// from the workspace `Cargo.toml`) must flip the positive sentinel red even though the
    /// files themselves are still on disk.
    #[test]
    fn g80_3_sentinel_flags_missing_metadata_member() {
        let tmp = fresh_tmp("g80-3-missing-member");
        fs::create_dir_all(tmp.join("crates/infra-egress/src")).unwrap();
        fs::write(tmp.join("crates/infra-egress/Cargo.toml"), "").unwrap();
        fs::write(tmp.join(INFRA_EGRESS_HTTP_RS), "").unwrap();
        let fake_metadata = r#"{"packages":[{"name":"humaux-domain"}]}"#;
        let problems = g80_3_workspace_sentinel(&tmp, Some(fake_metadata));
        assert!(problems.iter().any(|p| p.contains("cargo metadata")));
        fs::remove_dir_all(&tmp).ok();
    }

    /// Regression pin for the `member_present` fix: `"humaux-infra-egress"` appearing only as
    /// a *substring* of some other package's dependency listing (a plausible shape once a
    /// second crate path-depends on it by name) must NOT read as the crate itself being a
    /// workspace member — the bug this test would have caught: `.contains("humaux-infra-egress")`
    /// over the raw JSON text stays true here even though no `packages[].name` equals it.
    #[test]
    fn g80_3_sentinel_member_present_requires_exact_name_not_substring() {
        let tmp = fresh_tmp("g80-3-substring-not-member");
        fs::create_dir_all(tmp.join("crates/infra-egress/src")).unwrap();
        fs::write(tmp.join("crates/infra-egress/Cargo.toml"), "").unwrap();
        fs::write(tmp.join(INFRA_EGRESS_HTTP_RS), "").unwrap();
        // No package here is literally named `humaux-infra-egress` — it only appears inside
        // another package's dependency list (e.g. after the crate was dropped from `members`
        // but a stale path-dependency edge to it still resolves in the lockfile/graph).
        let fake_metadata = r#"{"packages":[{"name":"humaux-domain","dependencies":[]},
            {"name":"humaux-other","dependencies":[{"name":"humaux-infra-egress"}]}]}"#;
        let problems = g80_3_workspace_sentinel(&tmp, Some(fake_metadata));
        assert!(
            problems.iter().any(|p| p.contains("cargo metadata")),
            "a dependency edge naming the crate must not be mistaken for workspace membership: \
             {problems:?}"
        );
        fs::remove_dir_all(&tmp).ok();
    }

    /// §83.4 判据0, full-path 注错 0a: with `crates/infra-egress` already delivered, deleting
    /// the whole directory must flip [`g80_3_outbound_choke_point`] itself to `Fail`, not
    /// `NotApplicable` — the early-return this test replaces made deletion vacuously
    /// undetectable (判据1's raw-client scan finds nothing to complain about when there is
    /// nothing left to scan).
    #[test]
    fn g80_3_outbound_choke_point_fails_when_infra_egress_directory_is_deleted() {
        let tmp = fresh_tmp("g80-3-deleted-crate-full-path");
        fs::create_dir_all(&tmp).unwrap();
        // No `crates/infra-egress` at all, and no workspace `Cargo.toml` for `cargo metadata`
        // to succeed against either — both read as absence, which is exactly the point: once
        // delivered, absence is a fault, never a legitimate "nothing to check" state.
        assert_eq!(
            g80_3_outbound_choke_point(&tmp),
            Verdict::Fail(g80_3_workspace_sentinel(&tmp, None))
        );
        fs::remove_dir_all(&tmp).ok();
    }

    /// Positive control for [`g80_3_manifest_dependency_check`]: the real repo's own `cargo
    /// metadata` document must show exactly one `reqwest`/`hyper`-dependent manifest.
    #[test]
    fn g80_3_manifest_dependency_check_real_repo_is_clean() {
        let root = real_root();
        let output = Command::new("cargo")
            .args(["metadata", "--no-deps", "--format-version", "1"])
            .current_dir(&root)
            .output()
            .expect("cargo metadata");
        assert!(output.status.success());
        let metadata_json = String::from_utf8_lossy(&output.stdout);
        assert!(g80_3_manifest_dependency_check(&metadata_json, &root).is_empty());
    }

    /// 注错 a, manifest-level (§83.4 判据1 补充): a second workspace member declaring a
    /// `reqwest` dependency must be reported even before any source line calls it.
    #[test]
    fn g80_3_manifest_dependency_check_fault_second_crate_depends_on_reqwest() {
        let root = PathBuf::from("/fixture-root");
        let metadata_json = r#"{"packages":[
            {"name":"humaux-infra-egress",
             "manifest_path":"/fixture-root/crates/infra-egress/Cargo.toml",
             "dependencies":[{"name":"reqwest"}]},
            {"name":"humaux-adapters",
             "manifest_path":"/fixture-root/crates/adapters/Cargo.toml",
             "dependencies":[{"name":"reqwest"}]}
        ]}"#;
        let problems = g80_3_manifest_dependency_check(metadata_json, &root);
        assert!(
            problems
                .iter()
                .any(|p| p.contains("crates/adapters/Cargo.toml")),
            "{problems:?}"
        );
    }
}
