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
//! in their own spec section: G80-1 (§53.3 规则3), G80-2 (§55.1), G80-4 (§16.2 — registered
//! below; `not_applicable` only while the `serving_version` definition is absent),
//! G80-20 (§67.4, out of this task's scope). G80-22 lives at §48.0①. G80-11's source_hash leg
//! lives at §1.3/§48.0 (T5.1) — same single-crate-convergence family as G80-22, G80-11's other
//! two legs (§1.12 tool schema, §7.5 classifier) are out of this task's scope. G59-3 lives at
//! §59.1. Each rule below cites its own §.
//!
//! G52-1 / G59-3 / G80-2 / G80-4 / G80-11 / G80-22 scan the **whole workspace root**
//! (`crates/` + `bins/` + `evals/` + `xtask/` + `migrations/`), not just `crates/`: §80.1.1 /
//! §48.0① / §59.1 / §52.4 all name `bins/*` / `evals/*` as required injection/scan territory.
//! [`walk_workspace_rs`] is the shared scan domain for those five — it excludes this file
//! itself ([`SELF_FILE`]) and strips every file's own `#[cfg(test)]` module
//! ([`strip_cfg_test_module`]), both documented
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
/// 测试控制量的名字前缀。读这些**不是**本闸要管的事。
///
/// 本闸的判据是「不许有第二个**生产配置**读取点」（§78.1/§50.1，唯一读取点在
/// `humaux-contracts`）。`/tests/` 目录的豁免早就写明了这层意思——「读
/// `HUMAUX_TEST_PG_DSN` 属于三态 DB fixture 模式，不是生产配置」——但那条豁免是按
/// **目录**给的，于是同一类读取只要挪出 `/tests/` 就会被误报。
///
/// 实际撞上了：`crates/testkit/src/lib.rs::skip_or_fail` 是「跳过还是失败」的唯一判定点
/// （ADR-0005），它读 `HUMAUX_REQUIRE_DB` —— 与 `/tests/` 里那些读取同一类别，却因为住在
/// `src/` 被判违规。把豁免从「哪个目录」改成「哪一类变量」，判据一点没松：任何名字不带
/// 这些前缀的读取，无论在哪个文件，照旧算第二个生产配置读取点。
const TEST_CONTROL_ENV_PREFIXES: [&str; 2] = ["HUMAUX_TEST_", "HUMAUX_REQUIRE_"];

/// 命中处所在行读的是不是测试控制量。
///
/// 只看命中行本身：这些读取在本仓一律写成 `env::var("NAME")` 的单行形式（变量名与调用同行）。
/// 名字由参数传入、跨行拼接的写法**不会**被豁免——那正确，因为那种写法下静态扫描无从判断
/// 读的到底是什么，宁可误报也不放过。
fn line_reads_test_control_var(source: &str, idx: usize) -> bool {
    let line_start = source[..idx].rfind('\n').map(|i| i + 1).unwrap_or(0);
    let line = source[line_start..].lines().next().unwrap_or("");
    TEST_CONTROL_ENV_PREFIXES
        .iter()
        .any(|prefix| line.contains(prefix))
}

pub fn scan_env_var_violations(display_path: &str, source: &str) -> Vec<String> {
    let bytes = source.as_bytes();
    let mut hit_lines: BTreeSet<usize> = BTreeSet::new();
    let mut record = |idx: usize| {
        if !line_is_comment_at(source, idx) && !line_reads_test_control_var(source, idx) {
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
// §55.1 G80-2 / §48.0① G80-22: unique construction points
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

/// §16.2 G80-4: "检索侧禁止把 `projection_version` 当常量读，只能经
/// `serving_version(stream_family)` 取" — the enforcement mechanism is a workspace-wide
/// consumer call-site count. `serving_version`'s own definition (`adapters::serving_repo`) and
/// its direct definition-testing file (`adapters/tests/serving_repo.rs`, which calls it purely
/// to pin the function's own read behavior, not as a retrieval consumer) are excluded from the
/// count — same shape as `count_construction_calls` excluding a type's own declaration.
///
/// The pool-owned wrapper and its caller-owned transaction form share one SQL authority
/// in `serving_repo`. Count both forms together: moving a retrieval read into its final RR
/// transaction changes the API spelling, not the exactly-one consumer constraint. A missing
/// consumer is a failure; only a missing definition can be `not_applicable` (ADR-0006).
fn g80_4_serving_version_sole_entry_point(root: &Path) -> Verdict {
    /// 定义点及其直属测试。**按精确路径排除，不用 `disp.contains("serving_repo")`**：
    /// 子串匹配会连带排除任何未来路径含该子串的文件（`serving_repo_client.rs`、
    /// `tests/serving_repo_wiring.rs`……），真调用点藏进那种文件里就永远数不到——
    /// 与 G80-22.5 踩过的「改名绕过」同型。
    const DEFINITION_FILES: [&str; 2] = [
        "crates/adapters/src/serving_repo.rs",
        "crates/adapters/tests/serving_repo.rs",
    ];

    let files = walk_workspace_rs(root);

    // §57.1 第2条：not_applicable 的唯一合法理由是**被测对象尚未交付**，且必须打印缺失
    // 对象名。这里的被测对象是 `serving_version` 这个函数本身，按**内容**探针判（同
    // `g80_2` 的 `fn build_request(` 手法），不按调用点数量。
    //
    // 先前判 NA 的条件是「检索侧一个调用点都没有」——而那恰恰就是 §16.2 被违反的状态本身，
    // 于是这道闸在违规最严重的时候最安静，NA 与违规不可区分（ADR-0006）。§57.1 允许 NA 是
    // 因为「没有被测对象就没什么可判的」，不是因为「判了会红」。
    if !files
        .iter()
        .any(|(_, src)| src.contains("pub async fn serving_version("))
    {
        return Verdict::NotApplicable(
            "missing object: §16.2 读路由入口 `pub async fn serving_version(` 尚未交付".to_string(),
        );
    }

    let mut count = 0usize;
    let mut sites = Vec::new();
    for (path, source) in &files {
        let disp = display(root, path);
        if disp.ends_with(SELF_FILE) || DEFINITION_FILES.iter().any(|d| disp.ends_with(d)) {
            continue;
        }
        // The two closed spellings enter the same authoritative SQL; together they must
        // still have exactly one consumer. Doc links lack `(` and comments are excluded.
        let n = source
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .map(|l| {
                l.matches("serving_version(").count() + l.matches("serving_version_in_txn(").count()
            })
            .sum::<usize>();
        if n > 0 {
            sites.push(format!("{disp}: {n}"));
            count += n;
        }
    }

    match count {
        1 => Verdict::Pass,
        0 => Verdict::Fail(vec![format!(
            "§16.2「检索侧只能经 serving_version(stream_family) 取」——消费侧调用点为 0，\
             也就是读路径根本没有经过读路由。这**不是** not_applicable：被测对象
             `serving_version` 就在 {}，缺的是调用它。",
            DEFINITION_FILES[0]
        )]),
        n => Verdict::Fail(vec![format!(
            "expected exactly 1 consumer-side `serving_version`/`serving_version_in_txn` call site outside the \
             definition files, found {n}: {sites:?}"
        )]),
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

/// §1.3/§48.0 G80-11 (source_hash leg): `humaux_projection::fingerprint::source_hash` is the
/// sole construction point of `SourceHash` (§16.1/§16.1.1, T5.1) — same single-crate
/// convergence family as `EvidencePayloadSha256` (G80-22, above) and the §1.12 canonical tool
/// schema. A second `SourceHash(` construction site anywhere in the workspace (a second crate
/// re-deriving the processing-input-fingerprint encoding instead of calling `source_hash`)
/// would let two different encodings both claim the name "the fingerprint", exactly the drift
/// §1.3's "缺任意一项...回放/模型对比/精准重建...全部无法回答" warns about.
fn g80_11_source_hash_unique(root: &Path) -> Verdict {
    let files = walk_workspace_rs(root);
    let object_exists = files
        .iter()
        .any(|(_, s)| s.contains("struct SourceHash") || s.contains("fn source_hash("));
    if !object_exists {
        return Verdict::NotApplicable(
            "projection::fingerprint::source_hash / SourceHash (§1.3/§48.0 G80-11, T5.1 尚未交付)"
                .to_string(),
        );
    }
    let mut count = 0usize;
    let mut sites = Vec::new();
    for (path, source) in &files {
        let n = count_construction_calls(source, "SourceHash(");
        if n > 0 {
            sites.push(format!("{}: {n}", display(root, path)));
        }
        count += n;
    }
    if count == 1 {
        Verdict::Pass
    } else {
        Verdict::Fail(vec![format!(
            "expected exactly 1 `SourceHash( .. )` construction site, found {count}: {sites:?}"
        )])
    }
}

// ============================================================================
// §23.1② / §22.5: `LedgerCounts` field set is exactly 6 — same shape as §37.2's "12 列"
// check. Guards the frozen invariant that the `visible` numerator can never be folded back
// into the ledger struct (adding a 7th `visible: u64` field would let G23-2's two injections
// stop being observable, §23.1②'s own text names this exact regression).
// ============================================================================

const LEDGER_COUNTS_FIELDS: &[&str] = &[
    "expected",
    "done",
    "deleted",
    "skipped",
    "open_gaps",
    "pending",
];

fn g23_1_ledger_counts_exactly_six_fields(root: &Path) -> Verdict {
    let files = walk_workspace_rs(root);
    let hit = files
        .iter()
        .find(|(_, s)| s.contains("struct LedgerCounts {"));
    let Some((path, source)) = hit else {
        return Verdict::NotApplicable(
            "retrieval::completeness::LedgerCounts (§22.5, ledger::close 尚未交付)".to_string(),
        );
    };

    let Some(start) = source.find("struct LedgerCounts {") else {
        return Verdict::NotApplicable("LedgerCounts struct body".to_string());
    };
    let Some(rel_end) = source[start..].find('}') else {
        return Verdict::Fail(vec![format!(
            "{}: `struct LedgerCounts {{` has no matching `}}` on the same scan window",
            display(root, path)
        )]);
    };
    let body = &source[start..start + rel_end];

    // One field name per non-empty line inside the braces, `name: type,` shape — the struct's
    // own definition is exactly this shape (see completeness.rs), so a plain per-line field
    // name extraction is sufficient without a full Rust parser.
    let mut fields: Vec<&str> = Vec::new();
    for line in body.lines().skip(1) {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some((name, _)) = line.split_once(':') {
            fields.push(name.trim());
        }
    }

    if fields.len() != 6 {
        return Verdict::Fail(vec![format!(
            "{}: LedgerCounts has {} field(s) {:?}, expected exactly 6 {:?} — a 7th field \
             (e.g. `visible: u64`) would move the numerator back inside the ledger struct, \
             defeating G23-2's fault injections (§23.1②)",
            display(root, path),
            fields.len(),
            fields,
            LEDGER_COUNTS_FIELDS
        )]);
    }

    let expected: std::collections::BTreeSet<&str> = LEDGER_COUNTS_FIELDS.iter().copied().collect();
    let actual: std::collections::BTreeSet<&str> = fields.iter().copied().collect();
    if expected != actual {
        return Verdict::Fail(vec![format!(
            "{}: LedgerCounts field set is {:?}, expected exactly {:?}",
            display(root, path),
            fields,
            LEDGER_COUNTS_FIELDS
        )]);
    }

    Verdict::Pass
}

/// §11.8 / §22.5 / CLAUDE.md "唯一构造点模式": two independent concerns each name their sole
/// constructor `classify()` — `ClassifiedMemoryId` (`domain::consolidate`, §11.8) and
/// `CompletenessClass` (`retrieval::completeness`, §22.5). `FreshnessClass`'s equivalent
/// (§21.5) is named `classify_age` in `retrieval::signals`, not `classify` — it does not share
/// this needle and is out of this check's scope.
///
/// A single workspace-wide `fn classify(` count conflates the two concerns and can never pass
/// once both land (T6.4 pushed the count from 1 to 2 — the fixer-review finding that named
/// this exact collision) — so each concern's own file is checked independently: exactly 1
/// `fn classify(` in that file, never `<= 1` (a missing constructor is its own
/// `NotApplicable`, not a silent pass). The workspace-wide total is still cross-checked
/// against the number of landed known sites, so a stray third `fn classify(` anywhere else is
/// still red — it now fails at the site list instead of at a single global count.
const CLASSIFY_SOLE_CONSTRUCTION_SITES: &[(&str, &str)] = &[
    (
        "crates/domain/src/consolidate.rs",
        "ClassifiedMemoryId (§11.8)",
    ),
    (
        "crates/retrieval/src/completeness.rs",
        "CompletenessClass (§22.5)",
    ),
];

fn g11_8_classify_sole_construction_point(root: &Path) -> Verdict {
    let files = walk_workspace_rs(root);
    let total: usize = files
        .iter()
        .map(|(_, s)| s.matches("fn classify(").count())
        .sum();

    let mut applicable = 0usize;
    let mut failures = Vec::new();
    for (rel_path, label) in CLASSIFY_SOLE_CONSTRUCTION_SITES {
        let full = root.join(rel_path);
        let Some((_, source)) = files.iter().find(|(p, _)| *p == full) else {
            continue;
        };
        applicable += 1;
        let n = source.matches("fn classify(").count();
        if n != 1 {
            failures.push(format!(
                "{rel_path} ({label}): expected exactly 1 `fn classify(`, found {n}"
            ));
        }
    }

    if applicable == 0 {
        return Verdict::NotApplicable(
            "domain::consolidate::classify (§11.8) / retrieval::completeness::classify \
             (§22.5) — neither has landed yet"
                .to_string(),
        );
    }

    if total != applicable {
        failures.push(format!(
            "workspace-wide `fn classify(` count is {total}, expected {applicable} (one per \
             landed sole-construction site {CLASSIFY_SOLE_CONSTRUCTION_SITES:?}) — a stray \
             definition exists outside the known sites"
        ));
    }

    if failures.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(failures)
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

const INFRA_NETWORK_HTTP_RS: &str = "crates/infra-network/src/http.rs";

/// ADR-0003 / §83.4's `intra-cell-resource-registry` fenced block, column 1 — the exact
/// variant-name set `IntraCellResource` (`crates/infra-cell/src/resource.rs`) must equal.
/// Sibling of [`EXTERNAL_EGRESS_REGISTRY`]; the two are checked for disjointness by
/// [`intra_cell_registry_disjoint_from_external`] — a resource must never be nameable through
/// both registries at once.
const INTRA_CELL_RESOURCE_REGISTRY: &[&str] = &[
    "QDRANT_REST",
    "RETRIEVAL_EMBEDDING_RPC",
    "PRIVATE_INFERENCE_RPC",
];

const INFRA_CELL_RESOURCE_RS: &str = "crates/infra-cell/src/resource.rs";

/// ADR-0003 Layer 1A's own sentinel file (external-egress HTTP transport, now built on top of
/// Layer 0 instead of constructing its own raw client — [`g80_3_layer1_crates_sentinel`]).
const INFRA_EGRESS_HTTP_RS_LAYER1A: &str = "crates/infra-egress/src/http.rs";

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

/// §83.4 判据1, decisive proof this closes: `humaux_infra_network::reqwest`'s re-export lets a
/// second crate reach `reqwest::Client` without ever listing `reqwest` in its own manifest —
/// [`g80_3_manifest_dependency_check`] alone cannot see that (the second crate depends on
/// `humaux-infra-network`, not `reqwest`, so its manifest set stays clean), and
/// [`is_bare_needle_word_start`]'s own word-boundary guard *rejects* an attacker-chosen alias
/// that happens to end in `Client` (`use ... reqwest::Client as HttpClient; HttpClient::new()`
/// — the byte before `Client::new(` inside `HttpClient::new(` is `p`, an identifier char) by
/// design, since that guard exists to reject an *unrelated* type merely named `MyClient`. This
/// needle instead matches the import line itself — `reqwest::Client as`/`reqwest::ClientBuilder
/// as`, any qualification prefix, any chosen alias name — so the alias's own name can never
/// evade it the way the call-site needle above can be evaded.
const REQWEST_CLIENT_ALIAS_IMPORT_NEEDLES: [&str; 2] =
    ["reqwest::Client as ", "reqwest::ClientBuilder as "];

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
    for needle in REQWEST_CLIENT_ALIAS_IMPORT_NEEDLES {
        let mut start = 0usize;
        while let Some(rel) = source[start..].find(needle) {
            let idx = start + rel;
            if !line_is_comment_at(source, idx) {
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

/// §83.4 判据0 (workspace 正哨兵): `crates/infra-network` must be a real, live workspace
/// member with its transport file present — any one of the three missing is red before
/// 判据1/2 are even evaluated. Without this, deleting the whole crate would make the
/// raw-client scan below vacuously report "0 hits outside the expected file" (there is no
/// file left to hit) — the exact 恒真闸 shape §59.1/§80.1 call out elsewhere in this module.
/// `metadata_json` is injectable so tests do not need to shell out to a real `cargo metadata`
/// against a fixture directory that has no real workspace `Cargo.toml`.
fn g80_3_workspace_sentinel(root: &Path, metadata_json: Option<&str>) -> Vec<String> {
    let mut problems = Vec::new();
    if !root.join("crates/infra-network/Cargo.toml").is_file() {
        problems.push("正哨兵缺失: crates/infra-network/Cargo.toml 不存在".to_string());
    }
    if !root.join(INFRA_NETWORK_HTTP_RS).is_file() {
        problems.push(format!("正哨兵缺失: {INFRA_NETWORK_HTTP_RS} 不存在"));
    }
    // Exact match against `packages[].name`, not a substring scan over the raw JSON text: a
    // substring match on `"humaux-infra-network"` stays true even after the crate is dropped
    // from the workspace `members` list, as long as *some* other member still path-depends on
    // it by name (that dependency edge's own JSON also contains the literal string) — the
    // 注错 0b fixture below pins this against regressing back to `.contains`.
    let member_present = metadata_json
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
        .and_then(|v| v.get("packages").and_then(|p| p.as_array()).cloned())
        .is_some_and(|packages| {
            packages
                .iter()
                .any(|p| p.get("name").and_then(|n| n.as_str()) == Some("humaux-infra-network"))
        });
    if !member_present {
        problems.push("正哨兵缺失: cargo metadata members 不含 humaux-infra-network".to_string());
    }
    problems
}

/// ADR-0003 / §83.4 判据0 补充: Layer 1's two capability-wrapper crates
/// (`humaux-infra-egress` external egress, `humaux-infra-cell` same-Cell access) must both be
/// real, live workspace members — same three-part shape [`g80_3_workspace_sentinel`] already
/// checks for Layer 0, one crate at a time so a missing member names exactly which crate/file
/// is gone rather than one merged, ambiguous line.
fn g80_3_layer1_crates_sentinel(root: &Path, metadata_json: Option<&str>) -> Vec<String> {
    let mut problems = Vec::new();
    let member_names: std::collections::HashSet<String> = metadata_json
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
        .and_then(|v| v.get("packages").and_then(|p| p.as_array()).cloned())
        .map(|packages| {
            packages
                .iter()
                .filter_map(|p| p.get("name").and_then(|n| n.as_str()))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();

    for (crate_dir, manifest_name, sentinel_file) in [
        (
            "crates/infra-egress",
            "humaux-infra-egress",
            INFRA_EGRESS_HTTP_RS_LAYER1A,
        ),
        (
            "crates/infra-cell",
            "humaux-infra-cell",
            INFRA_CELL_RESOURCE_RS,
        ),
    ] {
        if !root.join(format!("{crate_dir}/Cargo.toml")).is_file() {
            problems.push(format!("正哨兵缺失: {crate_dir}/Cargo.toml 不存在"));
        }
        if !root.join(sentinel_file).is_file() {
            problems.push(format!("正哨兵缺失: {sentinel_file} 不存在"));
        }
        if !member_names.contains(manifest_name) {
            problems.push(format!(
                "正哨兵缺失: cargo metadata members 不含 {manifest_name}"
            ));
        }
    }
    problems
}

/// §83.4 判据1 补充 (manifest-level positive check, 注错 a 的第二重防线): the *set* of
/// workspace-member manifests that declare a `reqwest`/`hyper` dependency must equal exactly
/// `{crates/infra-network/Cargo.toml}`. A raw client call needs a matching `[dependencies]`
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
    expected.insert("crates/infra-network/Cargo.toml".to_string());
    if actual != expected {
        problems.push(format!(
            "reqwest/hyper 依赖声明的 manifest 集合 != {{crates/infra-network/Cargo.toml}}，实际: \
             {actual:?}"
        ));
    }
    problems
}

/// §83.4 判据1 补充, second backstop: `pub use reqwest;` (`crates/infra-network/src/lib.rs`)
/// lets any crate reach `reqwest::Client`/`reqwest::Error` *types* through
/// `humaux_infra_network::reqwest` without ever listing `reqwest` in its own manifest — which
/// means [`g80_3_manifest_dependency_check`] alone cannot see a new crate that starts depending
/// on `humaux-infra-network` for exactly that purpose. This asserts the *dependents* of
/// `humaux-infra-network` are exactly the two Layer 1 crates ADR-0003 names — a third crate
/// (e.g. `humaux-adapters`) adding `humaux-infra-network = { path = ... }` to reach the
/// re-export is red here even if [`find_raw_http_client_calls`]'s needles somehow miss the
/// resulting call site.
fn g80_3_infra_network_dependents_check(metadata_json: &str, root: &Path) -> Vec<String> {
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
    let root_for_display = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());

    let mut actual: BTreeSet<String> = BTreeSet::new();
    for pkg in packages {
        let deps = pkg
            .get("dependencies")
            .and_then(|d| d.as_array())
            .cloned()
            .unwrap_or_default();
        let depends_on_infra_network = deps
            .iter()
            .any(|d| d.get("name").and_then(|n| n.as_str()) == Some("humaux-infra-network"));
        if !depends_on_infra_network {
            continue;
        }
        let Some(manifest_path) = pkg.get("manifest_path").and_then(|m| m.as_str()) else {
            continue;
        };
        actual.insert(display(&root_for_display, Path::new(manifest_path)));
    }

    let expected: BTreeSet<String> = [
        "crates/infra-egress/Cargo.toml",
        "crates/infra-cell/Cargo.toml",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    if actual != expected {
        problems.push(format!(
            "humaux-infra-network 的依赖方 manifest 集合 != \
             {{crates/infra-egress/Cargo.toml, crates/infra-cell/Cargo.toml}}，实际: {actual:?}"
        ));
    }
    problems
}

/// §83.4 判据1/2 pure comparator over an already-scanned `.rs` file set — separated from
/// [`g80_3_outbound_choke_point`] so tests can inject a small fixture file list instead of
/// scanning (or faking) the whole workspace.
fn g80_3_transport_and_registry_check(files: &[(PathBuf, String)], root: &Path) -> Vec<String> {
    let mut problems = Vec::new();

    // 判据1: raw client construction path set == {infra-network/src/http.rs}
    let mut raw_sites: BTreeSet<String> = BTreeSet::new();
    for (path, source) in files {
        if !find_raw_http_client_calls(source).is_empty() {
            raw_sites.insert(display(root, path));
        }
    }
    let mut expected_raw = BTreeSet::new();
    expected_raw.insert(INFRA_NETWORK_HTTP_RS.to_string());
    if raw_sites != expected_raw {
        problems.push(format!(
            "raw HTTP client (reqwest::Client::new/builder, hyper::Client::new/builder) 构造 \
             点集合 != {{{INFRA_NETWORK_HTTP_RS}}}，实际: {raw_sites:?}"
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

/// ADR-0003 / §83.4 Layer 1B's own registry-consistency judgment — the sibling of 判据2 above,
/// scanning `crates/infra-cell/src/resource.rs`'s `IntraCellResource` enum instead of
/// `domain::egress::OutboundPurpose`. Pure comparator over an already-scanned file set, same
/// shape [`g80_3_transport_and_registry_check`] uses, so tests can inject a small fixture
/// instead of scanning the whole workspace.
fn g80_3_intra_cell_registry_check(files: &[(PathBuf, String)], root: &Path) -> Vec<String> {
    let mut problems = Vec::new();
    match files
        .iter()
        .find(|(p, _)| display(root, p) == INFRA_CELL_RESOURCE_RS)
    {
        None => problems.push(format!(
            "missing object: {INFRA_CELL_RESOURCE_RS}::IntraCellResource (ADR-0003 判据1 尚未\
             交付)"
        )),
        Some((_, source)) => {
            let actual: BTreeSet<String> = parse_enum_variant_names(source, "IntraCellResource")
                .into_iter()
                .collect();
            let expected: BTreeSet<String> = INTRA_CELL_RESOURCE_REGISTRY
                .iter()
                .map(|s| s.to_string())
                .collect();
            if actual.is_empty() {
                problems.push(
                    "positive control missing: IntraCellResource 变体集合为空 —— matcher 坏了\
                     或枚举未落地，两者都必须报红，不得默认放行"
                        .to_string(),
                );
            } else if actual != expected {
                problems.push(format!(
                    "IntraCellResource 变体集合 != intra-cell-resource-registry，实际: \
                     {actual:?}，期望: {expected:?}"
                ));
            }
            problems.extend(g80_3_no_string_to_resource_constructor_fn(source));
        }
    }
    problems
}

/// Byte offset of the `)` matching the `(` at `s`'s own start (`s.as_bytes()[0]` must be `(`).
/// Depth-counting sibling of [`matching_brace_end`] for parens instead of braces.
fn find_matching_paren(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    if bytes.first() != Some(&b'(') {
        return None;
    }
    let mut depth = 0i32;
    for (i, b) in bytes.iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Whether `ident` appears in `header` as a whole identifier, not merely a substring —
/// `"IntraCellResourceRegistry"` must not match a search for `"IntraCellResource"` (both the
/// byte before the match and the byte after it must not continue an identifier). Word-boundary
/// sibling of [`is_bare_needle_word_start`], checked on both ends since `header` here is a
/// short, already-located slice rather than a whole-file scan.
fn header_names_ident_exactly(header: &str, ident: &str) -> bool {
    let mut start = 0usize;
    while let Some(rel) = header[start..].find(ident) {
        let idx = start + rel;
        let end = idx + ident.len();
        let before_ok = match header[..idx].chars().next_back() {
            None => true,
            Some(c) => !(c.is_alphanumeric() || c == '_'),
        };
        let after_ok = match header[end..].chars().next() {
            None => true,
            Some(c) => !(c.is_alphanumeric() || c == '_'),
        };
        if before_ok && after_ok {
            return true;
        }
        start = end;
    }
    false
}

/// §83.4 判据1, 注错 f's real mechanism: no `fn` inside `IntraCellResource`'s own `impl`
/// surface (`impl IntraCellResource { .. }` or `impl <Trait> for IntraCellResource { .. }`)
/// may accept a `&str`/`String` parameter and return `Self` — the general "no string
/// constructs this enum" property, scoped to this enum's own impls specifically so an
/// unrelated type's legitimate `&str -> Result<Self, _>` (e.g. `CellCidr::from_str`, in the
/// same file) is never a false hit.
///
/// A single fixed-name trybuild fixture (`tests/ui/fail_resource_from_raw_url_string.rs`)
/// cannot pin this property on its own: it only ever exercises a *direct literal* coerced to
/// `IntraCellResource`, which stays compile-fail regardless of whether some differently-named
/// associated fn (`from_url`, `parse_legacy`, …) exists that does the equivalent conversion
/// through a normal function call instead of a literal — `IntraCellResource::from_url("x")`
/// compiles fine even though the trybuild fixture's own literal-coercion line still doesn't.
/// This scan is the check that actually observes the property 判据1 claims: decisive proof is
/// that adding `pub fn from_url(_u: &str) -> Self { Self::QDRANT_REST }` inside `impl
/// IntraCellResource` left both the trybuild suite and `architecture-check` green before this
/// function existed.
fn g80_3_no_string_to_resource_constructor_fn(source: &str) -> Vec<String> {
    let mut problems = Vec::new();
    let mut search_from = 0usize;
    while let Some(rel) = source[search_from..].find("impl ") {
        let idx = search_from + rel;
        if line_is_comment_at(source, idx) {
            search_from = idx + 5;
            continue;
        }
        let Some(brace_rel) = source[idx..].find('{') else {
            break;
        };
        let open = idx + brace_rel;
        let header = &source[idx..open];
        search_from = open + 1;
        if !header_names_ident_exactly(header, "IntraCellResource") {
            continue;
        }
        let Some(close) = matching_brace_end(source, open) else {
            continue;
        };
        let body = &source[open + 1..close - 1];
        let body_start_abs = open + 1;
        for (fn_rel, _) in body.match_indices("fn ") {
            if line_is_comment_at(source, body_start_abs + fn_rel) {
                continue;
            }
            let name_start = fn_rel + 3;
            let Some(paren_rel) = body[name_start..].find('(') else {
                continue;
            };
            let paren_abs_in_body = name_start + paren_rel;
            let Some(close_paren_rel) = find_matching_paren(&body[paren_abs_in_body..]) else {
                continue;
            };
            let params = &body[paren_abs_in_body + 1..paren_abs_in_body + close_paren_rel];
            let after_params = &body[paren_abs_in_body + close_paren_rel + 1..];
            let sig_end = after_params.find('{').unwrap_or(after_params.len());
            let return_part = after_params[..sig_end].trim_start();
            let takes_string = params.contains("&str") || params.contains("String");
            let returns_self = return_part.starts_with("->") && return_part.contains("Self");
            if takes_string && returns_self {
                let name: String = body[name_start..]
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                    .collect();
                problems.push(format!(
                    "判据1 违反: {header_trimmed} 内存在字符串构造函数 fn {name}(...) -> Self \
                     形状 —— 任意命名的 fn 均不得从 &str/String 构造 Self（不止 From/FromStr \
                     两个固定名字）",
                    header_trimmed = header.trim()
                ));
            }
        }
    }
    problems
}

/// ADR-0003 reverse sentinel: `EXTERNAL_EGRESS_REGISTRY` and `INTRA_CELL_RESOURCE_REGISTRY`
/// must name disjoint sets — a resource nameable through *both* registries at once is exactly
/// the "Qdrant snuck into external-egress-registry" back door ADR-0003 exists to close (the
/// two enums' own 判据2/判据1-sibling checks each independently compare against *one* of these
/// two registries and would both report PASS if someone edited `OutboundPurpose` and
/// `EXTERNAL_EGRESS_REGISTRY` together to add a variant that is also still a live
/// `IntraCellResource` entry — this check is the only one that looks at both at once). Pure
/// comparator over injectable registry slices so the fault test below does not have to mutate
/// the real consts.
fn intra_cell_registry_disjoint_from_external(
    external: &[&str],
    intra_cell: &[&str],
) -> Vec<String> {
    let external_set: BTreeSet<&str> = external.iter().copied().collect();
    intra_cell
        .iter()
        .filter(|r| external_set.contains(*r))
        .map(|r| {
            format!(
                "ADR-0003 反向哨兵违反: {r:?} 同时出现在 external-egress-registry 与 \
                 intra-cell-resource-registry —— 一个资源不得同时可经两条 registry 命名"
            )
        })
        .collect()
}

/// §83.4 G80-3 full check: Layer 0/1A/1B 判据0/1/2/3, folding in real `cargo metadata` for the
/// workspace positive sentinel.
fn g80_3_outbound_choke_point(root: &Path) -> Verdict {
    // No early `NotApplicable` return for a missing `crates/infra-network` directory: T4.1 has
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
    problems.extend(g80_3_layer1_crates_sentinel(root, metadata_json.as_deref()));
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
    problems.extend(g80_3_infra_network_dependents_check(&metadata_json, root));

    let files = walk_workspace_rs(root);
    problems.extend(g80_3_transport_and_registry_check(&files, root));
    problems.extend(g80_3_intra_cell_registry_check(&files, root));
    problems.extend(intra_cell_registry_disjoint_from_external(
        EXTERNAL_EGRESS_REGISTRY,
        INTRA_CELL_RESOURCE_REGISTRY,
    ));

    if problems.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(problems)
    }
}

// ============================================================================
// ADR-0012 §决定1/2 — the RPC transport must stay a Unix domain socket + kernel
// peer-credential identity, never TCP.
// ============================================================================

const RETRIEVAL_WORKER_SRC_DIR: &str = "bins/retrieval-worker/src";
const GATEWAY_RETRIEVAL_CLIENT_FILE: &str = "bins/gateway/src/retrieval_embedding_client.rs";

/// ADR-0012 is binding on the *transport*, not just on which symbols may cross the gateway/
/// worker boundary — swapping `UnixListener`/`UnixStream` for `TcpListener`/`TcpStream` in
/// either the worker's RPC listener or the gateway's dial would silently drop the
/// `peer_cred()`-based identity check (§决定2) that the ADR calls "读 body 之前" enforcement,
/// while every other gate here (which only greps for worker-only *symbols*) would keep saying
/// pass. This gate fails on any literal `TcpListener`/`TcpStream` in the two RPC-specific
/// files, and requires the corresponding Unix type to actually appear — so a same-host TCP
/// swap (or the two files simply losing their transport code entirely) both go red.
fn adr_0012_uds_transport_gate(root: &Path) -> Verdict {
    let worker_dir = root.join(RETRIEVAL_WORKER_SRC_DIR);
    let gateway_file = root.join(GATEWAY_RETRIEVAL_CLIENT_FILE);
    if !worker_dir.is_dir() || !gateway_file.is_file() {
        return Verdict::NotApplicable(format!(
            "missing object: {RETRIEVAL_WORKER_SRC_DIR} or {GATEWAY_RETRIEVAL_CLIENT_FILE}"
        ));
    }
    let mut files = read_files(&walk_files(&worker_dir, &["rs"]));
    let Ok(gateway_source) = std::fs::read_to_string(&gateway_file) else {
        return Verdict::NotApplicable(format!("unreadable: {GATEWAY_RETRIEVAL_CLIENT_FILE}"));
    };
    files.push((gateway_file.clone(), gateway_source));

    const FORBIDDEN: [&str; 2] = ["TcpListener", "TcpStream"];
    let mut problems = Vec::new();
    let mut has_unix_listener = false;
    let mut has_unix_stream = false;
    for (path, source) in &files {
        for needle in FORBIDDEN {
            let mut search_from = 0;
            while let Some(offset) = source[search_from..].find(needle) {
                let idx = search_from + offset;
                if !line_is_comment_at(source, idx) {
                    problems.push(format!(
                        "ADR-0012 决定1/2 违反: {} 使用了 TCP 传输 {needle:?}（必须是 UDS + peer_cred）",
                        display(root, path)
                    ));
                }
                search_from = idx + needle.len();
            }
        }
        if source.contains("UnixListener") {
            has_unix_listener = true;
        }
        if source.contains("UnixStream") {
            has_unix_stream = true;
        }
    }
    if !has_unix_listener {
        problems.push(format!(
            "ADR-0012 决定1 违反: {RETRIEVAL_WORKER_SRC_DIR} 未见 UnixListener — RPC 监听器传输丢失或被替换"
        ));
    }
    if !has_unix_stream {
        problems.push(format!(
            "ADR-0012 决定1 违反: {GATEWAY_RETRIEVAL_CLIENT_FILE} 未见 UnixStream — 网关拨号传输丢失或被替换"
        ));
    }
    if problems.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(problems)
    }
}

// ============================================================================
// ADR-0012 — gateway↔retrieval-worker query-embedding RPC boundary
// ============================================================================

const GATEWAY_SRC_DIR: &str = "bins/gateway/src";

/// §4.2 / ADR-0012 决定3: `bins/gateway` may authorize
/// [`IntraCellResource::RETRIEVAL_EMBEDDING_RPC`](crate) via the shared registry/permit gate,
/// but the DashScope provider credential, `role_retrieval_worker` pool, and the platform
/// retrieval credential env var stay `humaux-retrieval-worker`-only (ADR-0012's own
/// consequence: "gateway 与 retrieval-worker 以不同 OS 用户运行" presumes gateway never even
/// links the code that would need that credential). A literal hit for any of the three needles
/// outside a comment line is a regression of that boundary, not a style nit.
fn adr_0012_gateway_boundary_gate(root: &Path) -> Verdict {
    let dir = root.join(GATEWAY_SRC_DIR);
    if !dir.is_dir() {
        return Verdict::NotApplicable(format!("missing object: {GATEWAY_SRC_DIR}"));
    }
    let files = read_files(&walk_files(&dir, &["rs"]));
    if files.is_empty() {
        return Verdict::NotApplicable(format!("missing object: {GATEWAY_SRC_DIR}/*.rs"));
    }
    const FORBIDDEN: [&str; 3] = [
        "DashscopeEmbeddingProvider",
        "RetrievalWorkerDbPool",
        "DASHSCOPE_API_KEY",
    ];
    let mut problems = Vec::new();
    for (path, source) in &files {
        for needle in FORBIDDEN {
            let mut search_from = 0;
            while let Some(offset) = source[search_from..].find(needle) {
                let idx = search_from + offset;
                if !line_is_comment_at(source, idx) {
                    problems.push(format!(
                        "ADR-0012 决定3 违反: {} 引用了 worker-only 符号 {needle:?}",
                        display(root, path)
                    ));
                }
                search_from = idx + needle.len();
            }
        }
    }
    if problems.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(problems)
    }
}

/// ADR-0014: gateway's own `IntraCellResource::QDRANT_REST` registry entry must always carry
/// `CellAccessMode::QdrantReadOnly` — a static source-scan companion to
/// `crates/infra-cell`'s runtime enforcement (`HttpIntraCellTransport::execute`'s method/path
/// allowlist), catching a `bootstrap.rs` regression that drops
/// `.with_access_mode(CellAccessMode::QdrantReadOnly)` or explicitly reaches for
/// `CellAccessMode::ReadWrite` before it ever reaches a running process.
fn adr_0014_gateway_qdrant_read_only_gate(root: &Path) -> Verdict {
    let dir = root.join(GATEWAY_SRC_DIR);
    if !dir.is_dir() {
        return Verdict::NotApplicable(format!("missing object: {GATEWAY_SRC_DIR}"));
    }
    let files = read_files(&walk_files(&dir, &["rs"]));
    if files.is_empty() {
        return Verdict::NotApplicable(format!("missing object: {GATEWAY_SRC_DIR}/*.rs"));
    }
    let mut found_qdrant_entry = false;
    let mut problems = Vec::new();
    for (path, source) in &files {
        let needle = "IntraCellResource::QDRANT_REST";
        let mut search_from = 0;
        while let Some(offset) = source[search_from..].find(needle) {
            let idx = search_from + offset;
            if !line_is_comment_at(source, idx) {
                // Only a registry-entry *construction* site is in scope — e.g.
                // `entries.insert(IntraCellResource::QDRANT_REST, ResourceEntry::new(...)
                // .with_access_mode(...))`. A plain reference to the variant (minting a permit,
                // `authorize_cell_access(&registry, IntraCellResource::QDRANT_REST, ttl)`,
                // rustdoc prose) is not — this scan would otherwise demand
                // `CellAccessMode::QdrantReadOnly` appear near every read-only permit request
                // too, which has nothing to configure.
                let window_end = (idx + 800).min(source.len());
                let window = &source[idx..window_end];
                if window.contains("ResourceEntry::new(") {
                    found_qdrant_entry = true;
                    if !window.contains("CellAccessMode::QdrantReadOnly") {
                        problems.push(format!(
                            "ADR-0014 违反: {} 注册 QDRANT_REST 时未见 CellAccessMode::QdrantReadOnly（附近 800 字符窗口内）",
                            display(root, path)
                        ));
                    }
                }
            }
            search_from = idx + needle.len();
        }
        let mut search_from = 0;
        while let Some(offset) = source[search_from..].find("CellAccessMode::ReadWrite") {
            let idx = search_from + offset;
            if !line_is_comment_at(source, idx) {
                problems.push(format!(
                    "ADR-0014 违反: {} 显式构造了 CellAccessMode::ReadWrite（gateway 的 Qdrant 访问必须只读）",
                    display(root, path)
                ));
            }
            search_from = idx + "CellAccessMode::ReadWrite".len();
        }
    }
    if !found_qdrant_entry {
        return Verdict::NotApplicable(
            "gateway 尚未注册 IntraCellResource::QDRANT_REST resource entry".to_owned(),
        );
    }
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
/// encapsulation point `crates/adapters/src/postgres.rs`; the seven wrapper types must each
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

    // Eight wrappers, each declared exactly once, all in the encapsulation point.
    for wrapper in [
        "RuntimeDbPool",
        "BatchIssuerDbPool",
        "ConsolidationDbPool",
        "PrivateWorkerDbPool",
        "RetrievalWorkerDbPool",
        "MaintenanceDbPool",
        "PublicWorkerDbPool",
        "AdminDbPool",
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
    // raw type at least once — the seven wrappers each hold a `sqlx::PgPool` inner field.
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

// ============================================================================
// §20#G20-2 / G80-39 — online recall/context/continuity: zero static dependency on any
// reasoning provider (T6.2, §20.0)
// ============================================================================

/// The online-lane file set §20.0 names: `application::retrieve` (T6.2's own orchestration
/// entry point), its sibling online lanes `application::continuity` and `domain::context`
/// (§25.4 Mandatory Context Lane), `adapters::retrieve` — the one DB-backed online recall
/// path already shipped (T3.8's `recall_with_overlay`; see that module's own doc comment for
/// why real I/O lives there and not in `application::retrieve`) — plus the §20 Planner
/// (`crates/retrieval/src/*.rs`) and its process entry point (`bins/retrieval-worker/src/
/// main.rs`): a generative query rewrite inserted in the Planner is exactly as much a §20.0
/// violation as one inserted in `application::retrieve`, and prior to this fix the Planner
/// wasn't scanned at all. A listed file that does not exist yet contributes 0 hits, not a
/// violation — §20.0's rule is "MUST NOT call", and an absent file trivially satisfies that;
/// see [`g20_2_no_hidden_generative_recall`] for how a *placeholder* file (present but
/// content-free) is treated instead of silently counting toward a green Pass.
const ONLINE_LANE_FILES: &[&str] = &[
    "crates/application/src/retrieve.rs",
    "crates/application/src/continuity.rs",
    "crates/domain/src/context.rs",
    "crates/adapters/src/retrieve.rs",
    "crates/retrieval/src/lib.rs",
    "crates/retrieval/src/planner.rs",
    "crates/retrieval/src/candidate.rs",
    "crates/retrieval/src/fusion.rs",
    "crates/retrieval/src/rerank.rs",
    "crates/retrieval/src/compiler.rs",
    "crates/retrieval/src/completeness.rs",
    "crates/retrieval/src/envelope.rs",
    "crates/retrieval/src/predicate_eval.rs",
    "crates/retrieval/src/predicate_registry.rs",
    "crates/retrieval/src/signals.rs",
    "bins/retrieval-worker/src/main.rs",
];

/// The needles §20.0/G20-2 forbids anywhere in the online call graph, each paired with
/// whether a hit additionally requires a non-identifier byte on the *right* (see
/// [`count_identifier_hits`]):
///
/// - the two provider *type* names (naming either — bare, `dyn`-bound, or as a generic
///   bound — is how Rust code reaches either provider's methods at all) — `true`: a full
///   identifier match, so `UserReasoningProviderFactory` (a different type) does not count;
/// - the literal method name from the spec's own 注错 text ("在 recall 前插一个
///   complete_structured() query rewrite") — `false`, a left-bounded *prefix* match: the
///   workspace's one real generative call site is
///   `complete_structured_with_bounded_repair` (`crates/adapters/src/byok.rs`), whose name
///   starts with `complete_structured` but continues with an identifier byte (`_`) — a
///   right-boundary requirement here silently exempted the exact call the rule exists to
///   catch (confirmed by injecting a caller of it into `application::retrieve` under the old
///   matcher: the check stayed green);
/// - `byok`, the adapter module that owns both provider traits and every generative call
///   into them — belt-and-suspenders against a call site that reaches a provider through a
///   local re-export/alias and so names neither the trait nor `complete_structured*`
///   directly (e.g. `use humaux_adapters::byok::complete_structured_with_bounded_repair as
///   rewrite;`). Confirmed zero false positives against the current online lane: the only
///   pre-existing occurrence of the substring `byok` in the scan domain is this file's own
///   `//!` doc line naming it in prose, which the comment-skip rule already excludes.
const FORBIDDEN_GENERATIVE_NEEDLES: &[(&str, bool)] = &[
    ("UserReasoningProvider", true),
    ("PublicReasoningProvider", true),
    ("complete_structured", false),
    ("byok", true),
];

/// `application::retrieve`'s own placeholder convention (T0.x scaffold modules across this
/// workspace share the exact `占位模块` marker in their sole doc comment, e.g.
/// `crates/application/src/continuity.rs`) — a file matching this is present but carries no
/// real call graph, so it must not count as scanned coverage the way a real online-lane file
/// does (see [`g20_2_no_hidden_generative_recall`]).
fn is_online_lane_placeholder(src: &str) -> bool {
    src.contains("占位模块")
}

/// Word-boundary, comment-aware occurrence count of `needle` in `src` — same matching
/// discipline as [`count_pgpool_type`] (prev byte not an identifier byte, `//`-comment lines
/// excluded), generalized to an arbitrary needle instead of one hardcoded type name.
/// `require_right_boundary` additionally requires the byte after the match not be an
/// identifier byte either; pass `false` for a needle whose real-world violations extend past
/// the needle itself (see [`FORBIDDEN_GENERATIVE_NEEDLES`]'s doc for why `complete_structured`
/// needs this).
fn count_identifier_hits(src: &str, needle: &str, require_right_boundary: bool) -> usize {
    let mut n = 0usize;
    for line in src.lines() {
        if line.trim_start().starts_with("//") {
            continue;
        }
        let bytes = line.as_bytes();
        let mut from = 0usize;
        while let Some(rel) = line[from..].find(needle) {
            let at = from + rel;
            let after = at + needle.len();
            let prev_is_ident = at > 0 && is_ident_byte(bytes[at - 1]);
            let next_is_ident = bytes.get(after).is_some_and(|b| is_ident_byte(*b));
            if !prev_is_ident && (!require_right_boundary || !next_is_ident) {
                n += 1;
            }
            from = after;
        }
    }
    n
}

/// §20#G20-2 / G80-39: `application::retrieve` and its online-lane siblings (now including
/// the §20 Planner, see [`ONLINE_LANE_FILES`]) must have zero static dependency on either
/// reasoning provider — recall/context/continuity MUST NOT implicitly call USER_REASONING or
/// PLATFORM_PUBLIC (§20.0). Pure text scan, same class as [`g6_db_pool_topology`] —
/// deliberately not a `cargo tree`/dependency-graph query: the ban is about the *call graph*
/// (or a re-export of it, hence the `byok` needle) naming either provider at all inside the
/// online lane's own source text, not about a crate-level `Cargo.toml` dependency edge — e.g.
/// `adapters::retrieve` legitimately lives in the same crate as `byok.rs` today, which is
/// fine as long as `adapters/retrieve.rs`'s own source never spells out either provider or
/// `byok`/`complete_structured*`.
///
/// There is deliberately no permanent positive-occurrence sentinel *inside* this scan domain
/// (unlike [`g6_db_pool_topology`]'s `home_hits`): the rule here is "must be 0 everywhere in
/// this domain, forever" — a legitimate non-zero home inside the domain would itself be the
/// violation §20.0 forbids, so there is no legal place to put one. Scanner-aliveness is
/// instead proven the way a rule with no legal positive home has to prove it (§53.3 规则3's
/// own reasoning, applied without a permanent fixture): the `g20_2_fault_*` mutation tests
/// below flip a synthetic copy of this same file set from 0 hits to >0 and assert the check
/// goes red, plus a standalone `count_identifier_hits` unit test proves the matcher itself
/// recognizes the needle text independent of any file.
///
/// A `Pass` additionally requires at least one scanned file to carry real content: a file
/// that is either absent or a bare T0.x `占位模块` placeholder (see
/// [`is_online_lane_placeholder`]) is tracked separately and, if *every* listed file is in
/// that state, the check reports `NotApplicable` instead of a vacuous `Pass` — there being no
/// real online call graph yet to have scanned in the first place.
fn g20_2_no_hidden_generative_recall(root: &Path) -> Verdict {
    let mut problems = Vec::new();
    let mut not_yet_real = Vec::new();
    for rel in ONLINE_LANE_FILES {
        let path = root.join(rel);
        let src = match fs::read_to_string(&path) {
            Ok(src) => src,
            Err(_) => {
                not_yet_real.push(format!("{rel} (absent)"));
                continue; // absent file ⇒ 0 hits, not a violation (see ONLINE_LANE_FILES doc)
            }
        };
        if is_online_lane_placeholder(&src) {
            not_yet_real.push(format!("{rel} (T0.x placeholder)"));
        }
        for (needle, require_right_boundary) in FORBIDDEN_GENERATIVE_NEEDLES {
            let hits = count_identifier_hits(&src, needle, *require_right_boundary);
            if hits > 0 {
                problems.push(format!(
                    "{rel}: 静态依赖 {needle} 命中 {hits} 次（§20.0 online recall/context/\
                     continuity 禁止隐式调用生成式 provider）"
                ));
            }
        }
    }
    if !problems.is_empty() {
        return Verdict::Fail(problems);
    }
    if not_yet_real.len() == ONLINE_LANE_FILES.len() {
        return Verdict::NotApplicable(not_yet_real.join(", "));
    }
    Verdict::Pass
}

/// §22.5 / §23.1②: the A1 identity (`done + open_gaps + pending == expected`) must have
/// **exactly one** implementation workspace-wide. §22.5 freezes it verbatim: 「A1 的算式全库
/// 只此一处，不会两边各写一遍再漂移」—— `retrieval::completeness::ledger::close`（三次独立
/// 取数）与 `projection::stream::advance_prefix`（watermark 推进）都必须调用同一个判定，
/// 不得各写一份表达式。
///
/// 扫描的是**算式形态**而不是函数名：一个复制回来的 `done + open_gaps + pending` 即使换了
/// 变量名也会被 `open_gaps + pending` / `pending + open_gaps` 这两个片段抓到。允许出现的地方
/// 只有算式的家（`crates/domain/src/ledger.rs`）与本检查器自己。
pub fn g22_5_a1_sole_implementation(root: &Path) -> Verdict {
    const HOME: &str = "crates/domain/src/ledger.rs";
    // 算式的两种书写顺序；任一出现在 HOME 之外即为第二份实现。
    const ARITHMETIC_SHAPES: [&str; 2] = ["open_gaps + pending", "pending + open_gaps"];

    let mut strays = Vec::new();
    let home_hits = fs::read_to_string(root.join(HOME))
        // 带左括号做词边界：否则 `a1_holds_renamed` 含子串 `a1_holds`，改名不会被抓到
        // （本闸自己第一版就踩了这个，变异测试当场抓出）。
        .map(|s| s.matches("pub fn a1_holds(").count())
        .unwrap_or(0);
    for (path, source) in read_files(&walk_files(root, &["rs"])) {
        let disp = display(root, &path);
        if disp.ends_with(SELF_FILE) {
            continue; // 本文件逐字包含这些片段作为判据
        }
        let hits: usize = source
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .map(|l| {
                ARITHMETIC_SHAPES
                    .iter()
                    .map(|shape| l.matches(shape).count())
                    .sum::<usize>()
            })
            .sum();
        if hits == 0 {
            continue;
        }
        if disp.ends_with(HOME) {
            continue; // 家里怎么写算式由它自己决定（当前是 saturating_add，防回绕）
        } else {
            strays.push(format!(
                "{disp}: A1 arithmetic written here ({hits} site(s))"
            ));
        }
    }

    // 活哨兵（§53.3 规则3 同款）：家里必须**恰好**定义一次 `a1_holds`。算式的书写形式由家
    // 自己决定（当前是 `saturating_add`，防回绕成假闭合），所以哨兵钉在定义上而不是字面算式
    // 上——否则家一改写法，整道闸就会静默失明。
    if home_hits != 1 {
        strays.push(format!(
            "positive sentinel: {HOME} defines `pub fn a1_holds` {home_hits} time(s), expected \
             exactly 1 (§53.3 规则3 同款——扫描器失明或算式的家被掏空)"
        ));
    }

    if strays.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(strays)
    }
}

// ============================================================================
// §19 "Provider Plane Architecture Gate" — seven CI checks, spec text verbatim (module doc's
// own §80.1.1 registry note): this section carries **no** G80-*/INV-*/D*-style id ("本段无
// G80-*/INV-* 形式编号" — the p7 task digest's own instruction is "勿发明编号"), so each check
// below is named descriptively, the same way [`dependency_rule_check`]/[`env_var_scan`]
// already are for rules with no spec-assigned number.
// ============================================================================

/// DashScope SDK-specific identifier hits in `src`, line by line, skipping `//`-comment lines
/// (same discipline as [`count_identifier_hits`]). Deliberately **not** a bare
/// case-insensitive `"dashscope"` substring match: the bare lowercase word `"dashscope"` is
/// this crate's own `ProviderId`/business-data value (a routing/health/pricing lookup key
/// threaded through `router.rs`/`admission.rs`/`health.rs`/`metrics.rs`/test fixtures
/// entirely legitimately) — matching on it would flag that data, not "importing the SDK".
/// The needles here instead target the SDK-*shape* surface a real DashScope adapter carries
/// and nothing else legitimately would: `Dashscope`-prefixed PascalCase type names
/// (`DashscopeEmbeddingProvider`, `DashscopeEmbeddingRequest`, …), `dashscope_`-embedded
/// snake_case names (`map_dashscope_status`), the literal API host, and a `dashscope::` path
/// segment (`use dashscope::Client;`, `dashscope::embeddings::create(...)`) — the most likely
/// real shape of the violation these gates name ("Domain does not import DashScope SDK"),
/// added after a review found the original three-needle set scored zero hits on exactly that
/// shape. The `::` suffix cannot collide with the bare lowercase `"dashscope"` `ProviderId`
/// business-data string this crate's routing/health/pricing code legitimately carries (no
/// `::` follows a string literal's contents). Confirmed against the real repo
/// (`crates/retrieval-provider/src/adapters.rs` and its own `tests/`) to hit only there — see
/// this function's own unit tests for the positive/negative pair that pins the distinction.
fn provider_plane_dashscope_sdk_hits(src: &str) -> usize {
    const NEEDLES: [&str; 4] = [
        "Dashscope",
        "dashscope_",
        "dashscope.aliyuncs.com",
        "dashscope::",
    ];
    src.lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .map(|l| NEEDLES.iter().map(|n| l.matches(n).count()).sum::<usize>())
        .sum()
}

/// Provider Plane Architecture Gate check 1/7: "Domain does not import DashScope SDK". Scans
/// `crates/domain/src/**/*.rs` only — a real, fully-populated crate (not a T0.x placeholder),
/// so `Pass` on zero hits is a genuine finding, not a vacuous one (contrast
/// [`g20_2_no_hidden_generative_recall`]'s `NotApplicable`-when-all-placeholder branch, which
/// does not apply here because `crates/domain` has never been a placeholder).
fn provider_plane_gate1_domain_no_dashscope_sdk(root: &Path) -> Verdict {
    let mut problems = Vec::new();
    for (path, source) in read_files(&walk_files(&root.join("crates/domain/src"), &["rs"])) {
        let hits = provider_plane_dashscope_sdk_hits(&source);
        if hits > 0 {
            problems.push(format!(
                "{}: DashScope SDK identifier hits {hits} (§19 Provider Plane Architecture \
                 Gate: \"Domain does not import DashScope SDK\")",
                display(root, &path)
            ));
        }
    }
    if problems.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(problems)
    }
}

/// Provider Plane Architecture Gate check 2/7: "Application does not call DashScope directly".
/// Same shape as check 1/7 against `crates/application/src/**/*.rs`.
fn provider_plane_gate2_application_no_dashscope_direct(root: &Path) -> Verdict {
    let mut problems = Vec::new();
    for (path, source) in read_files(&walk_files(&root.join("crates/application/src"), &["rs"])) {
        let hits = provider_plane_dashscope_sdk_hits(&source);
        if hits > 0 {
            problems.push(format!(
                "{}: DashScope SDK identifier hits {hits} (§19 Provider Plane Architecture \
                 Gate: \"Application does not call DashScope directly\")",
                display(root, &path)
            ));
        }
    }
    if problems.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(problems)
    }
}

/// Provider Plane Architecture Gate check 3/7 home: the one module/tests pair allowed to name
/// the DashScope SDK surface at all — `retrieval-provider::adapters` itself, plus its own
/// `tests/` (a test file importing the adapter's public types to exercise it is not "a second
/// importer of the provider client", it is the adapter's own test surface — same allowance
/// [`SELF_FILE`] grants this checker over itself).
fn provider_plane_gate3_home(disp: &str) -> bool {
    disp == "crates/retrieval-provider/src/adapters.rs"
        || disp.starts_with("crates/retrieval-provider/tests/")
}

/// Provider Plane Architecture Gate check 3/7: "Only retrieval-provider/adapters may import
/// provider client". Whole-workspace scan ([`walk_workspace_rs`], same domain
/// §20#G20-2/G80-39 uses) — any DashScope SDK identifier hit outside
/// [`provider_plane_gate3_home`] is a second importer.
fn provider_plane_gate3_only_adapters_import_provider_client(root: &Path) -> Verdict {
    let mut problems = Vec::new();
    for (path, source) in walk_workspace_rs(root) {
        let disp = display(root, &path);
        if provider_plane_gate3_home(&disp) {
            continue;
        }
        let hits = provider_plane_dashscope_sdk_hits(&source);
        if hits > 0 {
            problems.push(format!(
                "{disp}: DashScope SDK identifier hits {hits} outside retrieval-provider/\
                 adapters (§19 Provider Plane Architecture Gate: \"Only retrieval-provider/\
                 adapters may import provider client\")"
            ));
        }
    }
    if problems.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(problems)
    }
}

/// Provider Plane Architecture Gate checks 4-6/7 share one scan domain and one "is this
/// feature even built yet" precondition: every source file in
/// `crates/retrieval-provider/src/`, not just `adapters.rs` alone (§19 review: the original
/// single-file scan meant a second external-call-carrying file — an eventual rerank or
/// custom-endpoint adapter — would go unscanned entirely, so "every external retrieval call"
/// was enforced against one file, not "every"). A directory that is absent, or whose every
/// file is still a bare T0.x `占位模块` placeholder (see [`is_online_lane_placeholder`],
/// reused here), has no real call graph to judge — `NotApplicable`, not a vacuous `Pass`, same
/// precondition split [`g20_2_no_hidden_generative_recall`] applies to its own online-lane
/// file set.
const PROVIDER_PLANE_SRC_DIR: &str = "crates/retrieval-provider/src";

/// External-call-site count for one file: `.call(`/`::call(` occurrences (non-comment lines),
/// counted only in a file that also names the `ExternalCall` trait or its `HttpExternalCall`
/// implementor (both contain the substring `"ExternalCall"`) — scoping the scan to files that
/// actually deal with that trait, rather than the field name (`self.transport`) today's sole
/// adapter happens to use. §19 review: the old needle (`".transport.call("`) failed open on a
/// renamed field or an aliased/`.await`-chained call shape; driving detection off the trait's
/// own method name instead survives both.
fn provider_plane_external_call_sites(source: &str) -> usize {
    if !source.contains("ExternalCall") {
        return 0;
    }
    count_code_line_hits(source, ".call(") + count_code_line_hits(source, "::call(")
}

/// Blanks every `/* ... */` block-comment span in `source` (bytes replaced with spaces,
/// newlines preserved so line numbers are unaffected — same discipline
/// [`strip_cfg_test_module`] uses for its own brace-matched span). An unterminated `/*` blanks
/// to the end of the string rather than panicking or looping.
fn strip_block_comments(source: &str) -> String {
    let mut out = String::with_capacity(source.len());
    let mut rest = source;
    while let Some(start) = rest.find("/*") {
        out.push_str(&rest[..start]);
        let tail = &rest[start..];
        let end = tail.find("*/").map(|e| e + 2).unwrap_or(tail.len());
        out.extend(
            tail[..end]
                .chars()
                .map(|c| if c == '\n' { '\n' } else { ' ' }),
        );
        rest = &tail[end..];
    }
    out.push_str(rest);
    out
}

/// The prefix of `line` up to (excluding) its first `//` that is not inside a `"..."` string
/// literal — a trailing comment (`let n = 1; // needle`) must not count as real code (§19
/// review: `count_code_line_hits` previously only skipped a line whose *leading* trimmed text
/// was `//`, so a trailing comment on an otherwise-real code line still counted). No escape
/// handling — this codebase's own needle strings never place an escaped `"` before a same-line
/// `//` (see this function's own tests for the shapes it does handle). A leading full-line
/// comment reduces to `""` here too, so this alone replaces the old leading-only filter.
fn code_before_line_comment(line: &str) -> &str {
    let bytes = line.as_bytes();
    let mut in_string = false;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => in_string = !in_string,
            b'/' if !in_string && bytes.get(i + 1) == Some(&b'/') => return &line[..i],
            _ => {}
        }
        i += 1;
    }
    line
}

/// Non-comment-line occurrence count of `needle` in `src` (plain substring, no identifier
/// boundary — the companion needles this feeds are multi-word/qualified paths like
/// `"admission::decide("`, not bare identifiers [`count_identifier_hits`] is built for).
/// Strips `/* */` block comments over the whole source first, then a trailing `//` comment (if
/// any, outside a string literal) from each remaining line before matching.
fn count_code_line_hits(src: &str, needle: &str) -> usize {
    strip_block_comments(src)
        .lines()
        .map(|l| code_before_line_comment(l).matches(needle).count())
        .sum()
}

/// Shared body for Architecture Gate checks 4-6/7: `companion_needles` is the evidence a real
/// external-call site must also carry (egress permit minting / ModelCallLedger write /
/// admission participation); `rule_name` and `not_yet_wired_note` are this check's own §19
/// quote and the missing-object explanation for the "call site exists, companion integration
/// does not yet" state.
///
/// Three-way split on the companion evidence, not two: real code-line hits ⇒ `Pass`; **zero**
/// code-line hits but the companion is named somewhere in a `//`-comment ⇒ `Fail` — a doc
/// comment claiming the integration exists without a line of code that does it is exactly the
/// 伪修复 shape repo CLAUDE.md's 交付准则 names ("拒绝伪修复：改表层没动根因"); zero hits
/// anywhere at all ⇒ `NotApplicable`, the honest "not yet wired, a separate task's
/// deliverable" state — not a silent `Pass`.
fn provider_plane_call_site_companion_check(
    root: &Path,
    companion_needles: &[&str],
    rule_name: &str,
    not_yet_wired_note: &str,
) -> Verdict {
    let files = read_files(&walk_files(&root.join(PROVIDER_PLANE_SRC_DIR), &["rs"]));
    if files.is_empty() {
        return Verdict::NotApplicable(format!(
            "{PROVIDER_PLANE_SRC_DIR}/ (absent) — §19 Provider Plane Architecture Gate: \
             \"{rule_name}\""
        ));
    }
    if files.iter().all(|(_, s)| is_online_lane_placeholder(s)) {
        return Verdict::NotApplicable(format!(
            "{PROVIDER_PLANE_SRC_DIR}/ (T0.x placeholder) — §19 Provider Plane Architecture \
             Gate: \"{rule_name}\""
        ));
    }

    let mut call_sites = 0usize;
    let mut real_hits = 0usize;
    let mut comment_only_files: Vec<String> = Vec::new();
    for (path, source) in &files {
        let sites = provider_plane_external_call_sites(source);
        if sites == 0 {
            continue;
        }
        call_sites += sites;
        let file_real: usize = companion_needles
            .iter()
            .map(|n| count_code_line_hits(source, n))
            .sum();
        if file_real > 0 {
            real_hits += file_real;
            continue;
        }
        let file_comment: usize = companion_needles
            .iter()
            .map(|n| source.matches(n).count()) // includes comment lines, unlike count_code_line_hits
            .sum();
        if file_comment > 0 {
            comment_only_files.push(display(root, path));
        }
    }

    if call_sites == 0 {
        // §19 review: this used to be a vacuous `Pass` ("no call site found, no violation
        // possible") — indistinguishable from the needle simply missing a real call site (a
        // renamed field, an aliased call). `NotApplicable` names the gap instead of silently
        // going green either way when needle drift, not a genuinely-empty call graph, is the
        // cause.
        return Verdict::NotApplicable(format!(
            "{PROVIDER_PLANE_SRC_DIR}/*.rs: no external call site matched (`.call(`/`::call(` \
             in a file naming `ExternalCall`) — §19 Provider Plane Architecture Gate: \
             \"{rule_name}\""
        ));
    }
    if real_hits > 0 {
        return Verdict::Pass;
    }
    if !comment_only_files.is_empty() {
        return Verdict::Fail(vec![format!(
            "{call_sites} external call site(s) across {PROVIDER_PLANE_SRC_DIR}/ \
             ({comment_only_files:?}), and {not_yet_wired_note} appears only in a comment, \
             never in real code — §19 Provider Plane Architecture Gate: \"{rule_name}\""
        )]);
    }
    Verdict::NotApplicable(format!(
        "{PROVIDER_PLANE_SRC_DIR}/*.rs: {call_sites} external call site(s) found, but no \
         {not_yet_wired_note} — §19 Provider Plane Architecture Gate: \"{rule_name}\" (a \
         separate task's deliverable not yet wired into the real call path)"
    ))
}

/// Provider Plane Architecture Gate check 4/7: "Every external retrieval call passes
/// EgressPolicy". Real positive finding today: `DashscopeEmbeddingProvider::embed` mints an
/// `EgressPermit` via `egress::authorize(...)` before its one `.transport.call(` — this is the
/// one of the seven checks whose real target already exists end to end (T4.1's `EgressPermit`
/// topology + T7.1's adapter wiring), so this check is a real `Pass`, not `NotApplicable`.
fn provider_plane_gate4_egress_policy_before_external_call(root: &Path) -> Verdict {
    provider_plane_call_site_companion_check(
        root,
        &["authorize("],
        "Every external retrieval call passes EgressPolicy",
        "EgressPermit-minting `authorize(` call",
    )
}

/// Provider Plane Architecture Gate check 5/7: "Every external retrieval call creates
/// ModelCallLedger entry". `adapters::model_call_ledger::reserve_call`/`finalize` (§19.1) are
/// real (T7.4) but not yet called from `DashscopeEmbeddingProvider::embed` — the digest's own
/// task 4 ("ModelCallLedger + Pricing Registry") wires the *table*; wiring it into *this* call
/// site is out of this task's file ownership (`adapters.rs` belongs to T7.1). `NotApplicable`
/// is the honest state, not a silently-passing `Pass`.
fn provider_plane_gate5_ledger_entry_per_external_call(root: &Path) -> Verdict {
    provider_plane_call_site_companion_check(
        root,
        &["model_call_ledger", "ModelCallLedger"],
        "Every external retrieval call creates ModelCallLedger entry",
        "ModelCallLedger reserve/finalize reference",
    )
}

/// Provider Plane Architecture Gate check 6/7: "Every call participates in provider admission
/// control". `admission::decide`/`AdmissionRequest` (§19 Admission Controller) are real (T7.3)
/// but not yet called from `DashscopeEmbeddingProvider::embed` — same "table built, call site
/// not yet wired to it, out of this task's file ownership" state as check 5/7.
fn provider_plane_gate6_admission_participation(root: &Path) -> Verdict {
    provider_plane_call_site_companion_check(
        root,
        &["admission::decide(", "AdmissionRequest"],
        "Every call participates in provider admission control",
        "admission::decide/AdmissionRequest reference",
    )
}

/// The tail of `source` starting at `insert_idx` (the byte offset of an `INSERT INTO
/// private.processing_runs` match), up to the closing `"` of the enclosing Rust string literal
/// — i.e. the SQL statement text itself, not the whole file. No escape handling: this
/// codebase's own INSERT-statement string literals never place an escaped `"` inside the SQL
/// text (see this function's own tests). Falls back to the rest of `source` if no closing
/// quote is found (malformed/truncated fixture — better to over-scan than panic).
fn insert_statement_text(source: &str, insert_idx: usize) -> &str {
    let bytes = source.as_bytes();
    let mut i = insert_idx;
    while i < bytes.len() {
        if bytes[i] == b'"' && bytes.get(i.wrapping_sub(1)) != Some(&b'\\') {
            return &source[insert_idx..i];
        }
        i += 1;
    }
    &source[insert_idx..]
}

/// Provider Plane Architecture Gate check 7/7: "Embedding projection write contains
/// provider/model/version metadata". `private.processing_runs.model_provider`/`model_id`/
/// `model_revision` are already `NOT NULL` at the DB layer (`migrations/0064_processing_runs_
/// axes_and_retrieval_cards_model_fix.sql`); whole-workspace scan of every `INSERT INTO
/// private.processing_runs` call site.
///
/// `**/tests/**` is excluded from the evidence set (§19 review): a test fixture proves the
/// matcher works, not that a real production write path exists —
/// `crates/adapters/tests/processing_runs_fingerprint_rerun.rs`'s own fixture is the *only*
/// hit in the workspace today, so counting it would green this gate on a test double, exactly
/// the false-assurance shape §57.1's three-state rule exists to prevent. `NotApplicable` until
/// a real (non-`tests/`) call site lands.
///
/// The three metadata-field checks are scoped to the matched INSERT statement's own text
/// ([`insert_statement_text`]), not the whole file — a file whose INSERT omits a column but
/// happens to mention its name elsewhere (a struct field, an unrelated `SELECT`) must not
/// count as carrying it.
fn provider_plane_gate7_projection_write_has_model_metadata(root: &Path) -> Verdict {
    const INSERT_NEEDLE: &str = "INSERT INTO private.processing_runs";
    const METADATA_FIELDS: [&str; 3] = ["model_provider", "model_id", "model_revision"];

    let mut insert_sites: Vec<String> = Vec::new();
    let mut incomplete: Vec<String> = Vec::new();
    for (path, source) in walk_workspace_rs(root) {
        let disp = display(root, &path);
        if disp.contains("/tests/") {
            continue;
        }
        let mut search = 0usize;
        while let Some(rel) = source[search..].find(INSERT_NEEDLE) {
            let idx = search + rel;
            insert_sites.push(disp.clone());
            let stmt = insert_statement_text(&source, idx);
            let missing: Vec<&str> = METADATA_FIELDS
                .iter()
                .copied()
                .filter(|f| !stmt.contains(f))
                .collect();
            if !missing.is_empty() {
                incomplete.push(format!(
                    "{disp}: INSERT INTO private.processing_runs missing {missing:?} (§19 \
                     Provider Plane Architecture Gate: \"Embedding projection write contains \
                     provider/model/version metadata\")"
                ));
            }
            search = idx + INSERT_NEEDLE.len();
        }
    }
    if !incomplete.is_empty() {
        return Verdict::Fail(incomplete);
    }
    if insert_sites.is_empty() {
        return Verdict::NotApplicable(
            "no `INSERT INTO private.processing_runs` call site outside tests/ yet (§19 \
             embedding-projection write path, a later Phase's deliverable — only a test \
             fixture exists today) — §19 Provider Plane Architecture Gate: \"Embedding \
             projection write contains provider/model/version metadata\""
                .to_string(),
        );
    }
    Verdict::Pass
}

// ============================================================================
// §19 Embedding Provider Failover 硬规则 — sole decision point. Added per code review
// (blocker): `failover::decide_embedding_failover`'s own rustdoc claims to be "the only place
// in the workspace that may declare two ProjectionContracts failover-compatible", but
// `router.rs::projection_compatible` independently hand-rolled the identical
// provider_id/model_id/dimension comparison (via its own `ProjectionRef` type) without ever
// calling through it — and, per its own module doc, without `normalization`/`projection_version`
// at all.
//
// Fixed by extracting the actual 5-axis comparison out of `decide_embedding_failover` into
// `failover::projection_contracts_compatible` — a permit-free pure function
// `decide_embedding_failover` itself now calls after its own permit checks. `router.rs` cannot
// call `decide_embedding_failover` directly: that function requires a live `EgressPermit`, and
// `router::resolve` runs *before* a route (and therefore a `ProcessorId` a permit could be
// minted for) has been chosen (`router::projection_compatible`'s own doc). Calling either
// function is still routing through the one real comparison — this gate accepts both, so a
// second *independently hand-rolled* comparison remains exactly as impossible as it was when
// only `decide_embedding_failover` existed.
// ============================================================================

/// Pure comparator: `hits` is every file (outside `failover.rs`) that names the `ProjectionRef`
/// type — router.rs's own embedding-projection-compatibility reference type — without calling
/// through either `failover::decide_embedding_failover` (the permit-gated real egress call
/// site) or `failover::projection_contracts_compatible` (the permit-free pure comparator that
/// function itself delegates to, and the one a pre-permit caller like `router.rs` uses
/// instead — see this section's own module comment). Empty ⇒ the sole decision point holds;
/// any hit ⇒ a second, independently-maintained comparison exists that can silently diverge
/// from the 5-axis rule.
pub fn embedding_failover_sole_decision_point_check(hits: &[String]) -> Verdict {
    if hits.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(
            hits.iter()
                .map(|f| {
                    format!(
                        "{f}: defines/uses `ProjectionRef` without calling \
                         `failover::decide_embedding_failover` or \
                         `failover::projection_contracts_compatible` — §19 Embedding Provider \
                         Failover 硬规则 names those the sole places that may declare two \
                         projection contracts failover-compatible"
                    )
                })
                .collect(),
        )
    }
}

const EMBEDDING_FAILOVER_HOME: &str = "crates/retrieval-provider/src/failover.rs";

fn provider_plane_embedding_failover_sole_decision_point(root: &Path) -> Verdict {
    let hits: Vec<String> = walk_workspace_rs(root)
        .into_iter()
        .filter_map(|(path, source)| {
            let disp = display(root, &path);
            if disp == EMBEDDING_FAILOVER_HOME {
                return None;
            }
            let has_projection_ref = source.contains("ProjectionRef");
            let delegates = source.contains("decide_embedding_failover(")
                || source.contains("projection_contracts_compatible(");
            (has_projection_ref && !delegates).then_some(disp)
        })
        .collect();
    embedding_failover_sole_decision_point_check(&hits)
}

// ============================================================================
// §19 DOD-028 Retrieval Credential Boundary — added per code review (minor): the credential
// boundary this task introduced (`HttpExternalCall::call`'s purpose guard binding the platform
// retrieval credential to `RetrievalEmbedding`/`RetrievalRerank`, and `EgressSecret::expose`'s
// own doc claim of "exactly one hit per call site") shipped with no CI gate asserting either
// stayed true. Two checks, same `== 1`/set-equality shape the file's other sole-construction-
// point checks (`payload_sha256`, `Authority::new`) already use.
// ============================================================================

const EGRESS_SECRET_HOME: &str = "crates/infra-egress/src/http.rs";

/// (a) `EgressSecret::expose` call-site set == `{crates/infra-egress/src/http.rs}`. A file
/// counts as a call site only if it also names `EgressSecret` itself — a bare `.expose(` text
/// match alone would also hit `crates/adapters/src/byok.rs`'s unrelated
/// `PlaintextApiKey::expose`, a different credential type for a different trust domain
/// (USER_REASONING BYOK, not the platform retrieval credential this check is about).
/// `walk_workspace_rs` already strips `#[cfg(test)]` bodies, so this file's own test-only
/// `secret.expose()` assertion does not count as a second site.
fn g_egress_secret_expose_sole_call_site(root: &Path) -> Verdict {
    let hits: Vec<String> = walk_workspace_rs(root)
        .into_iter()
        .map(|(p, s)| (display(root, &p), s))
        .filter(|(disp, s)| {
            disp != EGRESS_SECRET_HOME && s.contains("EgressSecret") && s.contains(".expose(")
        })
        .map(|(disp, _)| {
            format!(
                "{disp}: reads an `EgressSecret` via `.expose()` outside the one sanctioned \
                 call site {EGRESS_SECRET_HOME} (that module's own doc, \"Credential \
                 injection\": \"a grep for who reads a raw egress credential has exactly one \
                 hit per call site\")"
            )
        })
        .collect();
    if hits.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(hits)
    }
}

/// (b) the §19 DOD-028 purpose guard from the blocker/major review finding actually exists:
/// `HttpExternalCall::call` must bind the platform retrieval credential to
/// `RetrievalEmbedding`/`RetrievalRerank` before it ever resolves that credential — "任何 User
/// BYOK MUST NOT 被 Retrieval Provider Plane 自动借用". Textual, like this file's other checks:
/// looks for the exact guard shape rather than parsing control flow, so a rewrite that keeps
/// the same restriction under different wording would need this needle updated too — same
/// trade-off every other check in this file already accepts.
fn g_retrieval_credential_purpose_guard_exists(root: &Path) -> Verdict {
    let files = walk_workspace_rs(root);
    let Some((_, source)) = files
        .iter()
        .find(|(p, _)| display(root, p) == EGRESS_SECRET_HOME)
    else {
        return Verdict::NotApplicable(format!("{EGRESS_SECRET_HOME} not found"));
    };
    let has_guard = source
        .contains("PrivateDataPurpose::RetrievalEmbedding | PrivateDataPurpose::RetrievalRerank");
    if has_guard {
        Verdict::Pass
    } else {
        Verdict::Fail(vec![format!(
            "{EGRESS_SECRET_HOME}: `HttpExternalCall::call` has no guard binding the platform \
             retrieval credential to RetrievalEmbedding/RetrievalRerank permits — §19 DOD-028"
        )])
    }
}

/// The seven §19 Provider Plane Architecture Gate checks, factored out of [`run`] so that
/// function stays under clippy's `too_many_lines` threshold — same reason this file already
/// splits other check families into their own functions.
fn provider_plane_architecture_gate_checks(root: &Path) -> Vec<(&'static str, Verdict)> {
    vec![
        (
            "§19 Provider Plane Architecture Gate 1/7 (Domain does not import DashScope SDK)",
            provider_plane_gate1_domain_no_dashscope_sdk(root),
        ),
        (
            "§19 Provider Plane Architecture Gate 2/7 (Application does not call DashScope \
             directly)",
            provider_plane_gate2_application_no_dashscope_direct(root),
        ),
        (
            "§19 Provider Plane Architecture Gate 3/7 (Only retrieval-provider/adapters may \
             import provider client)",
            provider_plane_gate3_only_adapters_import_provider_client(root),
        ),
        (
            "§19 Provider Plane Architecture Gate 4/7 (Every external retrieval call passes \
             EgressPolicy)",
            provider_plane_gate4_egress_policy_before_external_call(root),
        ),
        (
            "§19 Provider Plane Architecture Gate 5/7 (Every external retrieval call creates \
             ModelCallLedger entry)",
            provider_plane_gate5_ledger_entry_per_external_call(root),
        ),
        (
            "§19 Provider Plane Architecture Gate 6/7 (Every call participates in provider \
             admission control)",
            provider_plane_gate6_admission_participation(root),
        ),
        (
            "§19 Embedding Provider Failover 硬规则 (decide_embedding_failover / \
             projection_contracts_compatible sole decision point)",
            provider_plane_embedding_failover_sole_decision_point(root),
        ),
        (
            "§19 Provider Plane Architecture Gate 7/7 (Embedding projection write contains \
             provider/model/version metadata)",
            provider_plane_gate7_projection_write_has_model_metadata(root),
        ),
        (
            "§19 DOD-028 (a) EgressSecret::expose sole call site",
            g_egress_secret_expose_sole_call_site(root),
        ),
        (
            "§19 DOD-028 (b) retrieval credential purpose guard exists",
            g_retrieval_credential_purpose_guard_exists(root),
        ),
        (
            "§11.10#G11-2 / G80-43 (Grounding validity / recheck debt)",
            g80_43_grounding_validity(root),
        ),
        (
            "§25.4 A1 (Candidate 唯一构造点)",
            g25_4_candidate_sole_construction_point(root),
        ),
        (
            "§25.4 A2 (context_bindings 唯一写入点)",
            g25_4_binding_insert_sole_site(root),
        ),
        (
            "§25.4 A3 (authorize_mandatory/pinned 唯一调用点)",
            g25_4_authorize_sole_caller(root),
        ),
    ]
}

/// 统计 `needle` 在**非注释行**上的出现次数，跳过本文件自身（它逐字包含这些片段作为判据）。
/// 返回 `(总数, 逐文件明细)`。
fn count_outside_allowed(root: &Path, needle: &str, allowed: &[&str]) -> (usize, Vec<String>) {
    let mut total = 0usize;
    let mut sites = Vec::new();
    for (path, source) in read_files(&walk_files(root, &["rs"])) {
        let disp = display(root, &path);
        // 精确路径排除，**不按子串**：子串匹配会连带排除任何路径含该片段的文件
        // （`candidate_helpers.rs` 之类），真违规藏进去就永远数不到（ADR-0006 决定 2）。
        if disp.ends_with(SELF_FILE) || allowed.iter().any(|a| disp.ends_with(a)) {
            continue;
        }
        // `crates/<pkg>/tests/*.rs` 是 cargo 的集成测试目录——**按路径结构判定**
        // （第三段恰好是 `tests`），不是 `contains("/tests/")` 那种子串匹配：
        // 结构判定不会把一个碰巧叫 tests 的业务目录也放行。夹具造候选/写 binding 不是
        // 本闸要抓的违规，生产代码那么做才是。
        let segments: Vec<&str> = disp.split('/').collect();
        if segments.first() == Some(&"crates") && segments.get(2) == Some(&"tests") {
            continue;
        }
        // 同理剥掉 `src/*.rs` 尾部的 `#[cfg(test)] mod` 区。
        // ponytail: 沿用 `env_var_scan` 的同一前缀启发式（第一个 `#[cfg(test)] mod` 之后全免），
        // 天花板一致——生产代码若排在 cfg(test) 之后会被误免；真需要时一起换成 span 解析。
        let source = match cfg_test_mod_offset(&source) {
            Some(off) => &source[..off],
            None => source.as_str(),
        };
        let n = source
            .lines()
            .filter(|l| {
                let t = l.trim_start();
                !t.starts_with("//") && !t.starts_with("///")
            })
            .map(|l| l.matches(needle).count())
            .sum::<usize>();
        if n > 0 {
            sites.push(format!("{disp}: {n}"));
            total += n;
        }
    }
    (total, sites)
}

/// §25.4 A1：`Candidate` 只能在它自己的模块里造。
///
/// 机制①（Mandatory 不可被 rerank 淘汰）的一条腿。`Candidate` 的字段已经是私有的，
/// 所以「手搓一个字面量」由编译器挡住；本闸挡的是**另一种失效**：有人把字段改回 `pub`，
/// 或在别处加一个 `Candidate::new(...)` 把 Mandatory 行包装成普通候选送进排序。
/// 前者编译器不会说话，后者编译得过——两种都只有静态扫描看得见。
fn g25_4_candidate_sole_construction_point(root: &Path) -> Verdict {
    const HOME: [&str; 1] = ["crates/retrieval/src/candidate.rs"];

    let files = read_files(&walk_files(root, &["rs"]));
    // 三态：被测对象是 `Candidate` 这个类型本身。
    if !files
        .iter()
        .any(|(_, s)| s.contains("pub struct Candidate {"))
    {
        return Verdict::NotApplicable(
            "missing object: §24 `pub struct Candidate` 尚未交付".to_string(),
        );
    }

    let mut strays = Vec::new();

    // ① 别处不得调用构造函数。
    let (calls, call_sites) = count_outside_allowed(root, "Candidate::new(", &HOME);
    if calls > 0 {
        strays.push(format!(
            "§24 `Candidate::new(` 只允许出现在 {}，实得 {calls} 处: {call_sites:?}",
            HOME[0]
        ));
    }

    // ② 字段必须仍是私有的。字段改回 `pub` 时编译器一声不吭，而「手搓一个
    //    `Candidate { fusion_score: 0.0, .. }` 把 Mandatory 伪装成普通候选」就又成立了
    //    ——这正是本闸存在的原因（实测过：收口前全仓字段全 pub）。
    if let Some((_, home)) = files
        .iter()
        .find(|(p, _)| display(root, p).ends_with(HOME[0]))
    {
        for field in [
            "pub id: String",
            "pub facet: Facet",
            "pub fusion_score: f32",
            "pub estimated_rerank_tokens: u32",
        ] {
            if home.contains(field) {
                strays.push(format!(
                    "{}: `Candidate` 的字段 `{field}` 又变回 pub —— 字段公开之后\
                     「Mandatory 不可被淘汰」只能是纪律，不再是拓扑",
                    HOME[0]
                ));
            }
        }
    }

    if strays.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(strays)
    }
}

/// §25.4 A2：`private.context_bindings` 的写入点唯一。
///
/// 绕过 `domain::context` 的三个 `authorize_*` 直接写 binding，就是 §25.4 那条攻击路径
/// （把低 origin 内容永久钉进每次 Context）的落地形态。
fn g25_4_binding_insert_sole_site(root: &Path) -> Verdict {
    const INSERT_HOME: [&str; 1] = ["crates/adapters/src/context_repo.rs"];
    const GRANT_HOME: [&str; 1] = ["crates/domain/src/context.rs"];

    let files = read_files(&walk_files(root, &["rs"]));
    if !files
        .iter()
        .any(|(_, s)| s.contains("INSERT INTO private.context_bindings"))
    {
        return Verdict::NotApplicable(
            "missing object: §25.4 `INSERT INTO private.context_bindings` 写入路径尚未交付"
                .to_string(),
        );
    }

    let mut strays = Vec::new();
    let (inserts, sites) =
        count_outside_allowed(root, "INSERT INTO private.context_bindings", &INSERT_HOME);
    if inserts > 0 {
        strays.push(format!(
            "§25.4 binding 写入点只允许在 {}，实得 {inserts} 处: {sites:?}",
            INSERT_HOME[0]
        ));
    }
    // `BindingGrant` 的字面量同理：它是写入口唯一接受的类型，能在别处造出来就等于
    // 绕过了三个 authorize_*。
    let (grants, gsites) = count_outside_allowed(root, "BindingGrant {", &GRANT_HOME);
    if grants > 0 {
        strays.push(format!(
            "§25.4 `BindingGrant {{` 字面量只允许在 {}，实得 {grants} 处: {gsites:?}",
            GRANT_HOME[0]
        ));
    }

    if strays.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(strays)
    }
}

/// §25.4 A3：`authorize_mandatory` / `authorize_pinned` 的调用点收敛，连同 ADR-0019 D-A 的
/// actor 铸造点 `ConfirmedUserActor::from_consumed_confirmation`——它是 pub fn，可见性挡不住
/// consolidation / retention / private-worker 代码调它，这条 needle 才是把「只在 consume 之后
/// 铸造」钉死的东西。
///
/// **零调用点在这条闸里是合法的 Pass，与 G80-4 相反**——两者的规则方向不同：
/// G80-4 说的是「检索侧**必须**经过 serving_version」，零调用点就是违规本身；
/// 本闸说的是「**只有** context_repo 可以调 authorize_*」，零调用点意味着没人违规。
/// 同样是计数，NA/Pass 的语义相反，照着 G80-4 的形状改这里会改错（ADR-0006）。
fn g25_4_authorize_sole_caller(root: &Path) -> Verdict {
    // 定义点 + 允许的调用方 + 测试目录（夹具自己要调它们）。测试目录按**精确路径**列出，
    // 不用 `contains("/tests/")`：那会把任何路径含 tests 的生产文件也放行。
    const ALLOWED: [&str; 4] = [
        "crates/domain/src/context.rs",
        "crates/adapters/src/context_repo.rs",
        "crates/adapters/tests/mandatory_context_lane.rs",
        "crates/domain/tests/context_authorize.rs",
    ];

    let files = read_files(&walk_files(root, &["rs"]));
    if !files
        .iter()
        .any(|(_, s)| s.contains("pub fn authorize_mandatory("))
    {
        return Verdict::NotApplicable(
            "missing object: §25.4 `pub fn authorize_mandatory(` 尚未交付".to_string(),
        );
    }

    let mut strays = Vec::new();
    for needle in [
        "authorize_mandatory(",
        "authorize_pinned(",
        "from_consumed_confirmation(",
    ] {
        let (n, sites) = count_outside_allowed(root, needle, &ALLOWED);
        if n > 0 {
            strays.push(format!(
                "§25.4 `{needle}` 的调用点越界（只允许 {ALLOWED:?}），实得 {n} 处: {sites:?}"
            ));
        }
    }

    if strays.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(strays)
    }
}
/// §80.1 `G80-43` Grounding validity / recheck debt —— 判据 §11.10#G11-2。
///
/// **这道闸证明结构，不证明语义。** 说清楚边界，免得有人把它当成 §11.10#G11-2 的全部：
/// 「version 变了要判 RECHECK_REQUIRED」「resolver error 不能伪装成 Missing」这类**语义**，
/// 由 `crates/domain/src/grounding.rs` 的夹具 A–H 在运行时证明（spec §11.10 点名的三条注错
/// 已逐条实测红转绿）。本闸负责的是**那些夹具还在不在、派生点还是不是唯一一处、有没有人
/// 另开一个可手改的 stale 真源**——静态可扫、运行时测不到的那一面。
///
/// 因此 DOD-092 的 verifier 不能只写这道闸：它得同时点名那组夹具。**本闸绿 ≠ 语义成立。**
///
/// 四条断言，各自钉一个**不同**的失效面（缺一都会让判据变成看起来在跑的空壳）：
///
/// 1. **八条固定夹具 A–H 逐名在场**（§11.10#G11-2 逐条列了它们）。夹具被删掉时判据会静默
///    变弱——测试数量下降没人看得见，而 `cargo test` 照样全绿。
/// 2. **`derive_grounding_state` 恰好定义一次**（活哨兵，§53.3 规则3 同款）。`GroundingState`
///    的字段是私有的，本模块外造不出来；哨兵防的是家本身被掏空或改名后扫描器失明。
/// 3. **没有可手改的 stale 真源**（DOD-092 原话「不存在可手改 `memory.stale=true` 真源」）：
///    全 workspace 不允许出现给 memory 赋 stale 布尔的写法。
///
/// 三态：`crates/domain/src/grounding.rs` 不存在时返回 `not_applicable` 并点名（§57.1 第2条），
/// 不静默 pass——Phase 4 之前这道闸本来就没有被测对象。
pub fn g80_43_grounding_validity(root: &Path) -> Verdict {
    const HOME: &str = "crates/domain/src/grounding.rs";
    /// §11.10#G11-2 逐条列出的八条夹具，按 A–H 的函数名前缀。判据正文在 spec，这里只钉
    /// 「它们在场」——不复制判据内容（CLAUDE.md：判据正文不得复制到第二处）。
    const FIXTURE_PREFIXES: [&str; 8] = [
        "fn fixture_a_",
        "fn fixture_b_",
        "fn fixture_c_",
        "fn fixture_d_",
        "fn fixture_e_",
        "fn fixture_f_",
        "fn fixture_g_",
        "fn fixture_h_",
    ];
    /// 「可手改的 stale 真源」的书写形态。
    ///
    /// **`stale: bool` 排在第一位不是凑数**：DOD-092 禁的是「真源」，而真源是**字段本身**——
    /// 先有可写的 `stale` 字段，才谈得上给它赋值。只钉赋值语法的话，`*stale_ref = true`
    /// 这类间接写法就绕过去了；钉在字段声明上是堵上游，赋值那几条只是补网。
    ///
    /// 反过来，**读** stale 做判断不是 DOD-092 禁的事（`fn is_stale(&self) -> bool` 不含
    /// `stale: bool`，天然不命中），所以这里不会误报合法读路径——见
    /// `g80_43_reading_a_stale_field_is_not_a_violation`。
    /// 派生点被掏空的形态。**这条是被对抗审查打出来的**：先前三条断言只看
    /// `pub fn derive_grounding_state(` 在不在，于是函数体写成 `{ todo!() }` 照样判 Pass
    /// ——闸绿着，而 spec §11.10 点名的注错（删掉 version compare）它一点都感觉不到。
    /// 这里不去匹配「比较逻辑长什么样」（那是脆的，改个写法就瞎），只钉一件无歧义的事：
    /// **唯一派生点不能是桩**。
    const STUB_BODIES: [&str; 2] = ["todo!()", "unimplemented!()"];
    const HAND_SET_STALE_SHAPES: [&str; 5] = [
        "stale: bool",
        "stale = true",
        "stale: true",
        "set_stale(",
        "stale=true",
    ];

    let Ok(home_src) = fs::read_to_string(root.join(HOME)) else {
        return Verdict::NotApplicable(format!(
            "missing object: {HOME} (§8.8 Grounding Validity 派生点，Phase 4 交付物)"
        ));
    };

    let mut strays = Vec::new();

    // 1. 八条夹具逐名在场。
    for prefix in FIXTURE_PREFIXES {
        if !home_src.contains(prefix) {
            strays.push(format!(
                "§11.10#G11-2 夹具缺失：{HOME} 中找不到 `{prefix}…`（八条 A–H 缺一即判据变弱）"
            ));
        }
    }

    // 2. 活哨兵：派生点恰好一处。带左括号做词边界——`derive_grounding_state_v2` 含子串，
    //    不带括号的话改名不会被抓到（G80-22.5 的同款教训，见 g22_5_a1_sole_implementation）。
    let home_hits = home_src.matches("pub fn derive_grounding_state(").count();
    if home_hits != 1 {
        strays.push(format!(
            "positive sentinel: {HOME} defines `pub fn derive_grounding_state` {home_hits} \
             time(s), expected exactly 1（§53.3 规则3 同款——扫描器失明或派生点被掏空）"
        ));
    }

    // 3. 派生点不是桩（见 STUB_BODIES 的 doc：这是补上来的第四个失效面）。
    if let Some(at) = home_src.find("pub fn derive_grounding_state(") {
        // 只看这个函数往后的一小段，免得把文件别处的 todo!() 算到它头上。
        let body = &home_src[at..(at + 600).min(home_src.len())];
        for stub in STUB_BODIES {
            if body.contains(stub) {
                strays.push(format!(
                    "{HOME}: 唯一派生点 `derive_grounding_state` 的函数体是桩（含 `{stub}`）\
                     ——结构在场但语义空缺，§11.10#G11-2 的四态派生无从谈起"
                ));
            }
        }
    }

    // 4. DOD-092：全 workspace 不得出现手改 stale 的写法。
    for (path, source) in read_files(&walk_files(root, &["rs"])) {
        let disp = display(root, &path);
        if disp.ends_with(SELF_FILE) {
            continue; // 本文件逐字包含这些形态作为判据
        }
        let hits: usize = source
            .lines()
            .filter(|l| !l.trim_start().starts_with("//") && !l.trim_start().starts_with("///"))
            .map(|l| {
                HAND_SET_STALE_SHAPES
                    .iter()
                    .map(|shape| l.matches(shape).count())
                    .sum::<usize>()
            })
            .sum();
        if hits > 0 {
            strays.push(format!(
                "{disp}: DOD-092 禁止的可手改 stale 真源（{hits} 处）——GroundingState 只能由 \
                 `derive_grounding_state` 推导"
            ));
        }
    }

    if strays.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(strays)
    }
}

/// R3 is intentionally admission-time only.  This parser is small but structural: it scopes
/// every resolver assertion to the actual 0130 function body, so a comment or a second helper
/// cannot satisfy a missing decision predicate.  The live migration is the contract; tests
/// inject mutations into a self-contained fixture below.
fn r3_health_migration_contract(source: &str) -> Verdict {
    let sql = source
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut failures = Vec::new();
    r3_health_table_contract(&sql, &mut failures);

    let resolver_start = "CREATE FUNCTION control.resolve_user_reasoning_admission(";
    let Some(start) = sql.find(resolver_start) else {
        failures.push("missing exact R3 admission resolver signature".to_string());
        return Verdict::Fail(failures);
    };
    let resolver = &sql[start..];
    let Some(body_start) = resolver.find("AS $$") else {
        failures.push("resolver is missing a SQL body".to_string());
        return Verdict::Fail(failures);
    };
    let body_end = resolver[body_start..]
        .find("$$;")
        .map(|end| body_start + end + 3)
        .unwrap_or(resolver.len());
    let resolver = &resolver[..body_end];
    r3_health_resolver_contract(&sql, resolver, &mut failures);

    if failures.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(failures)
    }
}

fn r3_health_table_contract(sql: &str, failures: &mut Vec<String>) {
    for table in [
        "ops.reasoning_provider_health_observations",
        "ops.reasoning_account_health_observations",
    ] {
        let create = format!("CREATE TABLE {table}");
        if !sql.contains(&create) {
            failures.push(format!("missing append-only health authority {table}"));
        }
        if !sql.contains(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"))
            || !sql.contains(&format!("ALTER TABLE {table} FORCE ROW LEVEL SECURITY"))
        {
            failures.push(format!("{table}: ENABLE + FORCE RLS are both required"));
        }
    }
    let provider_start = sql.find("CREATE TABLE ops.reasoning_provider_health_observations");
    let account_start = sql.find("CREATE TABLE ops.reasoning_account_health_observations");
    let account_end = sql.find("CREATE INDEX reasoning_account_health_latest");
    for (name, block) in [
        (
            "provider",
            provider_start.zip(account_start).map(|(a, b)| &sql[a..b]),
        ),
        (
            "account",
            account_start.zip(account_end).map(|(a, b)| &sql[a..b]),
        ),
    ] {
        let Some(block) = block else {
            continue;
        };
        if !block.contains("source_kind text NOT NULL CHECK (btrim(source_kind) <> '')")
            || !block.contains(
                "reason_code text CHECK (reason_code IS NULL OR btrim(reason_code) <> '')",
            )
        {
            failures.push(format!(
                "{name} health observation needs audit-only source_kind/reason_code shape"
            ));
        }
    }
    if sql.contains("_health_current") {
        failures.push("R3 forbids mutable *_health_current projection tables".to_string());
    }
    if !sql.contains("ops.reasoning_health_observation_reject_mutation()")
        || !sql.contains("CREATE TRIGGER reasoning_provider_health_append_only")
        || !sql.contains("CREATE TRIGGER reasoning_account_health_append_only")
        || !sql.contains("CREATE TRIGGER reasoning_provider_health_reject_truncate")
        || !sql.contains("CREATE TRIGGER reasoning_account_health_reject_truncate")
    {
        failures.push("both R3 health histories need explicit append-only guards".to_string());
    }
}

fn r3_health_resolver_contract(sql: &str, resolver: &str, failures: &mut Vec<String>) {
    if !resolver.contains("SECURITY DEFINER") || !resolver.contains("SET search_path = pg_catalog")
    {
        failures.push("resolver must be SECURITY DEFINER with search_path=pg_catalog".to_string());
    }
    if resolver.matches("clock_timestamp()").count() != 1 {
        failures.push("resolver must capture clock_timestamp() exactly once".to_string());
    }
    if resolver
        .matches("ORDER BY observation.observed_at DESC, observation.observation_id DESC")
        .count()
        != 2
        || resolver.matches("LIMIT 1").count() < 2
    {
        failures.push(
            "resolver must select one latest provider and account row before verdict".to_string(),
        );
    }
    for predicate in [
        "observed_at <= route.admitted_at",
        "route.admitted_at <",
        "lifecycle_state = 'SERVING'",
        "strategy = 'PINNED'",
        "fallback_class = 'NONE'",
        "1 = (\n       SELECT count(*)",
        "b.effective_to IS NULL",
    ] {
        if !resolver.contains(predicate) {
            failures.push(format!(
                "resolver missing R3 decision predicate {predicate:?}"
            ));
        }
    }
    if resolver.contains("COALESCE(provider_health") || resolver.contains("COALESCE(account_health")
    {
        failures.push(
            "resolver may not fallback from the deterministically latest health row".to_string(),
        );
    }
    let grant = "GRANT EXECUTE ON FUNCTION control.resolve_user_reasoning_admission(uuid, bigint, uuid, text)";
    let revoke = sql[..sql.find(grant).unwrap_or(0)]
        .rfind("REVOKE ALL ON FUNCTION")
        .map(|start| &sql[start..sql.find(grant).unwrap_or(start)]);
    if !sql.contains(grant)
        || !sql.contains("TO role_private_worker")
        || !revoke.is_some_and(|block| {
            block.contains("control.resolve_user_reasoning_admission(uuid, bigint, uuid, text)")
                && block.contains("FROM PUBLIC")
        })
    {
        failures.push(
            "resolver must be private-worker-only; PUBLIC EXECUTE must be revoked".to_string(),
        );
    }
}

fn r3_health_migration_gate(root: &Path) -> Verdict {
    let path = root.join("migrations/0130_reasoning_route_health_admission.sql");
    match fs::read_to_string(&path) {
        Ok(source) => r3_health_migration_contract(&source),
        Err(_) => Verdict::NotApplicable(
            "migrations/0130_reasoning_route_health_admission.sql is not present yet; R3 health activation is not integrated"
                .to_string(),
        ),
    }
}

/// The R3 call ledger is not an optional receipt beside the real provider call.  The migration
/// owns the coupled shape (ledger snapshot, disclosure link, candidate reference); the runtime
/// owns the transaction order.  Keep the two static halves separate so a comment or an
/// unrelated retrieval ledger cannot green the contribution path.
fn r3_contribution_ledger_migration_contract(source: &str) -> Verdict {
    let sql = source
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut failures = Vec::new();
    let ledger = sql
        .find("ALTER TABLE ops.model_call_ledger")
        .map(|start| &sql[start..]);
    let Some(ledger) = ledger else {
        return Verdict::Fail(vec![
            "R3 requires an ops.model_call_ledger expansion in 0130, not a second receipt table"
                .to_string(),
        ]);
    };
    for column in [
        "call_kind",
        "intent_sha256",
        "reasoning_domain_id",
        "binding_id",
        "binding_version",
        "route_policy_id",
        "route_policy_version",
        "profile_id",
        "profile_version",
        "provider_account_id",
        "provider_endpoint_id",
        "credential_ref",
        "billing_account_id",
        "billing_instrument_id",
        "provider_health_observation_id",
        "account_health_observation_id",
        "billing_responsibility",
        "admitted_at",
        "egress_processor_id",
    ] {
        if !ledger.contains(column) {
            failures.push(format!("model_call_ledger R3 snapshot missing {column}"));
        }
    }
    for invariant in [
        "CONTRIBUTION_DEIDENTIFY",
        "model_call_ledger_reasoning_snapshot_shape",
        "ops.reasoning_model_call_validate()",
        "reasoning_model_call_validate",
        "model_call_ledger_reasoning_recipient_unique",
        "model_call_ledger_reasoning_binding_unique",
        "model_call_ledger_reasoning_endpoint_fk",
        "data_disclosures_tenant_model_call_unique",
        "data_disclosures_reasoning_model_call_fk",
        "data_disclosure_reasoning_model_call_validate",
        "contribution_candidates_reasoning_model_call_exact_fk",
        "contribution_candidate_reasoning_route_validate",
        "provider_endpoints_tenant_endpoint_egress_unique",
        "endpoint.egress_processor_id",
        "endpoint.egress_processor_id IS NOT NULL",
        "route.egress_processor_id",
    ] {
        if !sql.contains(invariant) {
            failures.push(format!("R3 ledger linkage/guard missing {invariant}"));
        }
    }
    if !ledger.contains("estimated_cost IS NULL") || !ledger.contains("actual_cost IS NULL") {
        failures.push(
            "CONTRIBUTION_DEIDENTIFY must keep estimated_cost and actual_cost NULL".to_string(),
        );
    }
    for idempotency_invariant in [
        "model_call_ledger_reasoning_call_kind_known",
        "call_kind IN ('COVERAGE_PROBE', 'TYPED_ASSESSMENT')",
        "octet_length(intent_sha256) = 32",
        "OLD.call_kind IS DISTINCT FROM NEW.call_kind",
        "OLD.intent_sha256 IS DISTINCT FROM NEW.intent_sha256",
    ] {
        if !ledger.contains(idempotency_invariant) {
            failures.push(format!(
                "R3 idempotency requires immutable call kind and intent fingerprint: missing {idempotency_invariant}"
            ));
        }
    }
    if !ledger.contains("request_id <> '00000000-0000-0000-0000-000000000000'::uuid") {
        failures.push(
            "R3 ledger shape must reject the nil UUID request_id for CONTRIBUTION_DEIDENTIFY"
                .to_string(),
        );
    }
    if failures.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(failures)
    }
}

fn r3_function_body<'a>(source: &'a str, signature: &str) -> Option<&'a str> {
    let start = source.find(signature)?;
    let open = start + source[start..].find('{')?;
    let close = matching_brace_end(source, open)?;
    Some(&source[open..close])
}

fn r3_ordered(body: &str, markers: &[&str]) -> bool {
    let mut cursor = 0;
    for marker in markers {
        let Some(relative) = body[cursor..].find(marker) else {
            return false;
        };
        cursor += relative + marker.len();
    }
    true
}

/// The `let permit = authorize(` window the R3 reserve leg is judged on: inline in the
/// reserve helper, or one level down in `authorize_structured_egress` when the reserve leg
/// delegates to it with the resolver-derived `&admission` (never a config-derived processor).
fn r3_authorize_window<'a>(contribution: &'a str, reserve: &'a str) -> Option<&'a str> {
    if let Some(start) = reserve.find("let permit = authorize(") {
        return Some(&reserve[start..reserve.len().min(start + 512)]);
    }
    let call = reserve.find("authorize_structured_egress(")?;
    let call_window = &reserve[call..reserve.len().min(call + 512)];
    if !call_window.contains("&admission") {
        return None;
    }
    let helper = r3_function_body(contribution, "fn authorize_structured_egress")?;
    let start = helper.find("let permit = authorize(")?;
    Some(&helper[start..helper.len().min(start + 512)])
}

/// The provider-call marker the R3 call leg is ordered on. `complete_structured_timed(` is
/// accepted only when that shared helper is itself a single `.complete_structured(` with no
/// retry loop — otherwise the literal direct call is required, as before.
fn r3_provider_call_marker(contribution: &str, call: &str) -> &'static str {
    if call.contains("complete_structured_timed(")
        && let Some(helper) = r3_function_body(contribution, "async fn complete_structured_timed")
    {
        let single_call = helper.matches(".complete_structured(").count() == 1;
        let no_retry = !helper.contains("loop {") && !helper.contains("retry");
        if single_call && no_retry {
            return "complete_structured_timed(";
        }
    }
    "complete_structured("
}

fn r3_contribution_runtime_contract(
    contribution: &str,
    ledger: &str,
    disclosure: &str,
    byok: &str,
) -> Verdict {
    let required = [
        "resolve_and_reserve_reasoning_call",
        "complete_structured(",
        "finalize_reasoning_call",
    ];
    let mut missing: Vec<String> = required
        .iter()
        .filter(|needle| !contribution.contains(**needle))
        .map(|needle| format!("missing contribution runtime marker {needle}"))
        .collect();
    for (source, marker) in [
        (ledger, "reserve_reasoning_call_in_txn"),
        (ledger, "finalize_reasoning_call_in_txn"),
        (ledger, "egress_processor_id"),
        (disclosure, "reserve_reasoning_in_txn"),
        (disclosure, "finalize_reasoning_in_txn"),
        (disclosure, "model_call_id"),
        (byok, "endpoint_ref"),
    ] {
        if !source.contains(marker) {
            missing.push(format!("missing R3 runtime marker {marker}"));
        }
    }
    if !missing.is_empty() {
        return Verdict::Fail(std::mem::take(&mut missing));
    }
    let reserve = r3_function_body(contribution, "async fn resolve_and_reserve_reasoning_call");
    let call = r3_function_body(contribution, "async fn call_structured");
    let finalize = r3_function_body(contribution, "async fn finalize_reasoning_call");
    let ledger_reserve = r3_function_body(ledger, "async fn reserve_reasoning_call_in_txn");
    let disclosure_reserve = r3_function_body(disclosure, "async fn reserve_reasoning_in_txn");
    let Some((reserve, call, finalize)) =
        reserve.zip(call).zip(finalize).map(|((a, b), c)| (a, b, c))
    else {
        return Verdict::Fail(vec![
            "R3 contribution helper body is missing or malformed".to_string(),
        ]);
    };
    let Some((ledger_reserve, disclosure_reserve)) = ledger_reserve.zip(disclosure_reserve) else {
        return Verdict::Fail(vec![
            "R3 in-transaction reserve helper body is missing or malformed".to_string(),
        ]);
    };
    // §11.2.5 R3 + ADR-0015 D5: the authorize leg may live in the shared `pub(crate)`
    // helper `authorize_structured_egress` (one provider path for Contribution and
    // Consolidate). The gate follows exactly one level of indirection and still demands
    // the resolver-derived processor at the actual `authorize(` call.
    let authorize_window = r3_authorize_window(contribution, reserve);
    if !r3_ordered(
        reserve,
        &[
            "load_with_admission",
            "reserve_reasoning_call_in_txn",
            "reserve_reasoning_in_txn",
            "txn.commit()",
        ],
    ) || !authorize_window.is_some_and(|window| window.contains("admission.egress_processor_id"))
    {
        return Verdict::Fail(vec![
            "R3 reserve helper must use resolver-derived processor and atomically resolve -> ledger reserve -> disclosure reserve -> commit"
                .to_string(),
        ]);
    }
    if !ledger_reserve.contains("locator.egress_processor_id")
        || !disclosure_reserve.contains("model_call_id")
        || !disclosure_reserve.contains("reserve_in_txn")
    {
        return Verdict::Fail(vec![
            "R3 ledger/disclosure reserve helpers must carry resolver processor and exact model_call_id"
                .to_string(),
        ]);
    }
    let provider_marker = r3_provider_call_marker(contribution, call);
    if !r3_ordered(
        call,
        &[
            "resolve_and_reserve_reasoning_call",
            provider_marker,
            "self.finalize_reasoning_call",
        ],
    ) || !r3_ordered(
        finalize,
        &[
            "finalize_reasoning_in_txn",
            "finalize_reasoning_call_in_txn",
            "txn.commit()",
        ],
    ) {
        return Verdict::Fail(vec![
            "R3 actual call must be reserve -> provider -> dual finalize, with no provider retry path"
                .to_string(),
        ]);
    }
    Verdict::Pass
}

/// A committed RESERVED row is the idempotency authority. This structural gate keeps its lookup
/// ahead of a fresh resolver call, so current health cannot turn a crashed attempt into a retry.
fn r3_contribution_idempotency_contract(
    contribution: &str,
    ledger: &str,
    application: &str,
    contribute: &str,
    pg_tests: &str,
) -> Verdict {
    let mut failures = Vec::new();
    if contribution.contains("Uuid::now_v7") {
        failures.push("ContributionReasoner must not mint a retry request id".to_string());
    }
    r3_application_idempotency_contract(application, contribution, contribute, &mut failures);
    let reserve = r3_function_body(contribution, "async fn resolve_and_reserve_reasoning_call");
    let lookup = r3_function_body(ledger, "async fn lookup_reasoning_call_in_txn");
    if !reserve.is_some_and(|body| {
        r3_ordered(
            body,
            &[
                "lookup_reasoning_call_in_txn",
                "load_with_admission",
                "reserve_reasoning_call_in_txn",
                "reserve_reasoning_in_txn",
                "txn.commit()",
            ],
        ) && body.contains("ReasoningCallLookup::ExistingReserved")
            && body.contains("existing_reservation")
    }) {
        failures.push(
            "R3 retry must lookup ExistingReserved before current admission and return before provider dispatch"
                .to_string(),
        );
    }
    r3_ledger_idempotency_contract(ledger, reserve, lookup, pg_tests, &mut failures);
    if failures.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(failures)
    }
}

fn r3_application_idempotency_contract(
    application: &str,
    contribution: &str,
    contribute: &str,
    failures: &mut Vec<String>,
) {
    for marker in [
        "ContributionReasoningCallKind",
        "CoverageProbe",
        "TypedAssessment",
        "LogicalReasoningCallId",
        "ReasoningIntentSha256",
        "contribution_attempt",
        "with_contribution_attempt",
        "contribution_attempt_is_canonical",
    ] {
        if !application.contains(marker) {
            failures.push(format!(
                "caller-carried R3 idempotency contract missing {marker}"
            ));
        }
    }
    let canonical_attempt =
        r3_function_body(application, "pub fn contribution_attempt_is_canonical");
    if !canonical_attempt.is_some_and(|body| {
        body.contains("(ContributionReasoningCallKind::CoverageProbe, None)")
            && body.contains("(ContributionReasoningCallKind::TypedAssessment, Some(_))")
    }) || !contribution.contains("contribution_attempt_is_canonical(coverage_digest)")
        || !contribute.contains("ContributionReasoningCallKind::CoverageProbe,\n                None")
        || !contribute.contains("ContributionReasoningCallKind::TypedAssessment,\n                Some(public_coverage.binding().digest_sha256())")
    {
        failures.push(
            "R3 coverage shape requires probe=None and typed assessment=exact canonical coverage digest"
                .to_string(),
        );
    }
    let intent_builder = r3_function_body(application, "pub fn with_contribution_attempt");
    let intent_encoder = r3_function_body(application, "fn contribution_reasoning_intent_sha256");
    if !intent_builder.is_some_and(|body| {
        body.contains("intent_schema_version: CONTRIBUTION_REASONING_INTENT_SCHEMA_VERSION")
            && body.contains("contribution_reasoning_intent_sha256(")
            && body.contains("CONTRIBUTION_REASONING_INTENT_SCHEMA_VERSION,")
    }) || !intent_encoder.is_some_and(|body| {
        body.contains("b\"intent_schema_version\\0\"")
            && body.contains("intent_schema_version.to_be_bytes()")
    }) {
        failures.push(
            "R3 canonical intent must explicitly version intent_schema_version before hashing"
                .to_string(),
        );
    }
}

fn r3_ledger_idempotency_contract(
    ledger: &str,
    reserve: Option<&str>,
    lookup: Option<&str>,
    pg_tests: &str,
    failures: &mut Vec<String>,
) {
    let tenant_scoped_lock = ledger.contains(
        "const REASONING_REQUEST_ADVISORY_LOCK_SQL: &str = \"SELECT pg_advisory_xact_lock(hashtextextended('model-call-request:' || $1::uuid::text || ':' || $2::uuid::text,0))\"",
    ) && lookup.is_some_and(|body| {
        body.contains("sqlx::query(REASONING_REQUEST_ADVISORY_LOCK_SQL)")
            && r3_ordered(body, &[".bind(tenant_id)", ".bind(logical_call_id.0)"])
    });
    if !lookup.is_some_and(|body| {
        tenant_scoped_lock
            && body.contains("WHERE call.tenant_id=$1 AND call.request_id=$2")
            && body.contains("FOR UPDATE OF call")
            && body.contains("status")
            && body.contains("call_kind")
            && body.contains("intent_sha256")
            && body.contains("ReasoningReservationConflict")
    }) {
        failures.push(
            "R3 retry lookup must serialize the tenant+request key and reject terminal or intent-mismatched rows"
                .to_string(),
        );
    }
    if !reserve.is_some_and(|body| body.contains("attempt.logical_call_id.0.is_nil()")) {
        failures.push(
            "R3 Reasoner must fail closed on a nil caller-carried logical_call_id".to_string(),
        );
    }
    if !ledger.contains("input.logical_call_id.0")
        || !ledger.contains("input.call_kind.as_db_str()")
        || !ledger.contains("input.intent_sha256.0.as_slice()")
    {
        failures.push(
            "R3 ledger insert must persist caller logical id plus independent kind and intent hash"
                .to_string(),
        );
    }
    let test_name = "fn reasoning_attempt_ledger_disclosure_candidate_and_atomicity()";
    if !pg_tests.contains(test_name)
        || !pg_tests.contains("existing_reserved_after_health_change")
        || !pg_tests.contains("same tenant/request key serializes before authority query")
        || !pg_tests.contains("same request UUID across tenants does not serialize")
        || !pg_tests.contains("model-call-request:' || $1::uuid::text || ':' || $2::uuid::text")
        || !pg_tests.contains("ledger_intent_mutation")
    {
        failures.push(
            "R3 PostgreSQL idempotency fault test must cover tenant-scoped locking, health drift, and intent mutation"
                .to_string(),
        );
    }
}

fn r3_contribution_ledger_gate(root: &Path) -> Verdict {
    let migration =
        match fs::read_to_string(root.join("migrations/0130_reasoning_route_health_admission.sql"))
        {
            Ok(source) => source,
            Err(e) => return Verdict::Fail(vec![format!("cannot read 0130 R3 migration: {e}")]),
        };
    let contribution =
        match fs::read_to_string(root.join("crates/adapters/src/contribution_reasoner.rs")) {
            Ok(source) => source,
            Err(e) => return Verdict::Fail(vec![format!("cannot read ContributionReasoner: {e}")]),
        };
    let ledger = fs::read_to_string(root.join("crates/adapters/src/model_call_ledger.rs"));
    let disclosure = fs::read_to_string(root.join("crates/adapters/src/disclosure.rs"));
    let byok = fs::read_to_string(root.join("crates/adapters/src/byok.rs"));
    let application = fs::read_to_string(root.join("crates/application/src/consolidate.rs"));
    let contribute = fs::read_to_string(root.join("crates/application/src/contribute.rs"));
    let pg_tests =
        fs::read_to_string(root.join("crates/adapters/tests/reasoning_route_health_admission.rs"));
    let mut failures = match r3_contribution_ledger_migration_contract(&migration) {
        Verdict::Fail(items) => items,
        Verdict::Pass => Vec::new(),
        Verdict::NotApplicable(detail) => vec![detail],
    };
    let (Ok(ledger), Ok(disclosure), Ok(byok), Ok(application), Ok(contribute), Ok(pg_tests)) =
        (ledger, disclosure, byok, application, contribute, pg_tests)
    else {
        return Verdict::Fail(vec![
            "cannot read one or more R3 contribution runtime/idempotency modules".to_string(),
        ]);
    };
    match r3_contribution_runtime_contract(&contribution, &ledger, &disclosure, &byok) {
        Verdict::Fail(items) => failures.extend(items),
        Verdict::Pass => {}
        Verdict::NotApplicable(detail) => failures.push(detail),
    }
    match r3_contribution_idempotency_contract(
        &contribution,
        &ledger,
        &application,
        &contribute,
        &pg_tests,
    ) {
        Verdict::Fail(items) => failures.extend(items),
        Verdict::Pass => {}
        Verdict::NotApplicable(detail) => failures.push(detail),
    }
    if failures.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(failures)
    }
}

/// W1's storage writer is intentionally one SQL-owned protocol. This companion check keeps
/// the marker a function-local scope switch and prevents a second Rust/application CAS.
#[allow(clippy::too_many_lines)]
fn w1_continuity_contract_from(
    migration: &str,
    domain: &str,
    adapter: &str,
    marker_sets_outside_publish: usize,
) -> Verdict {
    let mut failures = Vec::new();
    for needle in [
        "CREATE TABLE private.continuity_projects",
        "CREATE TABLE private.continuity_facet_versions",
        "CREATE TABLE private.continuity_facet_memory_links",
        "CREATE TABLE private.continuity_facet_evidence_links",
        "CREATE TABLE private.continuity_facet_slots",
        "ALTER TABLE private.continuity_projects FORCE ROW LEVEL SECURITY;",
        "ALTER TABLE private.continuity_facet_versions FORCE ROW LEVEL SECURITY;",
        "ALTER TABLE private.continuity_facet_memory_links FORCE ROW LEVEL SECURITY;",
        "ALTER TABLE private.continuity_facet_evidence_links FORCE ROW LEVEL SECURITY;",
        "ALTER TABLE private.continuity_facet_slots FORCE ROW LEVEL SECURITY;",
        "CONSTRAINT continuity_facet_versions_kind_closed CHECK",
        "CONSTRAINT continuity_facet_slots_kind_closed CHECK",
        "CONSTRAINT continuity_facet_slots_current_fkey FOREIGN KEY",
        "DEFERRABLE INITIALLY DEFERRED",
        "CREATE POLICY continuity_evidence_owner_exact_allow",
        "AS PERMISSIVE FOR SELECT TO role_migration_owner",
        "current_setting('humaux.continuity_publish',true)='1' AND CASE",
        "CREATE POLICY continuity_evidence_owner_exact_guard",
        "AS RESTRICTIVE FOR SELECT TO role_migration_owner",
        "CREATE POLICY continuity_evidence_owner_lock_allow",
        "AS PERMISSIVE FOR UPDATE TO role_migration_owner",
        "END) WITH CHECK (false);",
        "CREATE POLICY continuity_evidence_owner_lock_guard",
        "AS RESTRICTIVE FOR UPDATE TO role_migration_owner",
        "END) WITH CHECK (\n      current_setting('humaux.continuity_publish',true) IS DISTINCT FROM '1');",
        "current_setting('humaux.continuity_publish',true) IS DISTINCT FROM '1' OR CASE",
        "pg_catalog.pg_input_is_valid(current_setting('humaux.tenant_id',true),'uuid')",
        "pg_catalog.pg_input_is_valid(current_setting('humaux.workspace_id',true),'uuid')",
        "pg_catalog.pg_input_is_valid(current_setting('humaux.user_id',true),'uuid')",
        "SET humaux.continuity_publish TO '1'",
        "AND slot.project_id=p_project_id AND slot.facet_kind=p_facet_kind\n    FOR UPDATE;",
        "locked_version<>p_expected_slot_version",
        "slot.slot_version=p_expected_slot_version",
        "GET DIAGNOSTICS affected=ROW_COUNT",
        "IF affected<>1",
        "ORDER BY memory.memory_id FOR SHARE OF memory",
        "ORDER BY evidence.evidence_id FOR SHARE OF evidence",
        "SELECT count(*) INTO locked_source_count FROM (",
        ") locked_memory;",
        ") locked_evidence;",
    ] {
        if !migration.contains(needle) {
            failures.push(format!("0136 continuity contract missing `{needle}`"));
        }
    }
    for forbidden in [
        "ON CONFLICT",
        "MERGE INTO",
        "MAX(facet_version)",
        "Scope::Project",
        "compute_contribution_source_backing_closure_v1",
        "set_config('humaux.continuity_publish'",
        "GET DIAGNOSTICS locked_source_count=ROW_COUNT",
    ] {
        if migration.contains(forbidden) {
            failures.push(format!(
                "0136 continuity contract contains forbidden `{forbidden}`"
            ));
        }
    }
    if migration
        .matches("SET humaux.continuity_publish TO '1'")
        .count()
        != 1
        || marker_sets_outside_publish != 0
    {
        failures
            .push("continuity marker must be set only by publish function proconfig".to_string());
    }
    for derived in ["'HANDOFF'", "'COVERAGE'"] {
        if migration.contains(derived) {
            failures.push(format!(
                "derived facet has writable storage literal {derived}"
            ));
        }
    }
    for needle in [
        "continuity_uuid_v7!(ProjectId)",
        "continuity_uuid_v7!(ContinuityFacetVersionId)",
        "pub const ALL: [Self; 15]",
    ] {
        if !domain.contains(needle) {
            failures.push(format!("domain continuity surface missing `{needle}`"));
        }
    }
    for needle in [
        "pool: &RuntimeDbPool",
        "authorization: &AuthorizationScope",
        "authorization.narrow(command.workspace_id)",
        "private.register_continuity_project",
        "private.publish_continuity_facet",
    ] {
        if !adapter.contains(needle) {
            failures.push(format!("adapter continuity surface missing `{needle}`"));
        }
    }
    if adapter.contains("slot_version + 1") || adapter.contains("expected_slot_version + 1") {
        failures.push("Rust adapter independently computes continuity CAS successor".to_string());
    }
    if failures.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(failures)
    }
}

fn w1_continuity_gate(root: &Path) -> Verdict {
    let paths = [
        "migrations/0136_project_continuity.sql",
        "crates/domain/src/continuity.rs",
        "crates/adapters/src/continuity_repo.rs",
    ];
    let mut sources = Vec::new();
    for path in paths {
        match fs::read_to_string(root.join(path)) {
            Ok(source) => sources.push(source),
            Err(_) => return Verdict::NotApplicable(format!("missing object: {path}")),
        }
    }
    let sql_marker_sets = walk_files(&root.join("migrations"), &["sql"])
        .into_iter()
        .filter(|path| !path.ends_with("0136_project_continuity.sql"))
        .filter_map(|path| fs::read_to_string(path).ok())
        .map(|source| {
            source.matches("SET humaux.continuity_publish").count()
                + source
                    .matches("set_config('humaux.continuity_publish'")
                    .count()
        })
        .sum::<usize>();
    let rust_marker_sets = walk_workspace_rs(root)
        .into_iter()
        .filter(|(path, _)| {
            path.strip_prefix(root).is_ok_and(|relative| {
                let mut components = relative.components();
                components
                    .next()
                    .is_some_and(|part| part.as_os_str() == "crates")
                    && components.any(|part| part.as_os_str() == "src")
            })
        })
        .map(|(_, source)| {
            source.matches("SET humaux.continuity_publish").count()
                + source
                    .matches("set_config('humaux.continuity_publish'")
                    .count()
        })
        .sum::<usize>();
    w1_continuity_contract_from(
        &sources[0],
        &sources[1],
        &sources[2],
        sql_marker_sets + rust_marker_sets,
    )
}

/// §25.3.1 W2: one native Continuity read route backed by one Gateway-only
/// SECURITY DEFINER reader and one application-owned RR READ ONLY transaction.
#[allow(clippy::too_many_lines)]
fn w2_continuity_contract_from(
    migration: &str,
    manifest: &str,
    application: &str,
    adapter: &str,
    gateway: &str,
    guard: &str,
    dispatch: &str,
) -> Verdict {
    let mut failures = Vec::new();
    let manifest: toml::Value = match toml::from_str(manifest) {
        Ok(value) => value,
        Err(error) => {
            return Verdict::Fail(vec![format!(
                "0137 continuity manifest is invalid TOML: {error}"
            )]);
        }
    };
    if manifest.get("migration_id").and_then(toml::Value::as_str)
        != Some("0137_project_continuity_read")
    {
        failures.push("0137 continuity manifest must bind its exact migration_id".to_string());
    }
    if manifest.get("class").and_then(toml::Value::as_str) != Some("FORWARD_ONLY") {
        failures.push("0137 continuity manifest must remain FORWARD_ONLY".to_string());
    }
    for (field, needle) in [
        (
            "precheck",
            "to_regprocedure('private.read_continuity_project_storage_v1(uuid,uuid,uuid,uuid,uuid,uuid[])') is null",
        ),
        (
            "precheck",
            "to_regclass('private.continuity_projects') is not null",
        ),
        ("precheck", "private.publish_continuity_facet("),
        (
            "postcheck",
            "select count(*)=1 from reader where prosecdef and provolatile='s'",
        ),
        ("postcheck", "array['search_path=pg_catalog']::text[]"),
        (
            "postcheck",
            "pg_get_userbyid(proowner)='role_migration_owner'",
        ),
        ("postcheck", "not has_function_privilege('public'"),
        ("postcheck", "role.name='role_gateway'"),
        (
            "postcheck",
            "SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER",
        ),
        ("postcheck", "strpos(definition,'INSERT ') = 0"),
        ("postcheck", "strpos(definition,'UPDATE ') = 0"),
        ("postcheck", "strpos(definition,'DELETE ') = 0"),
    ] {
        if !manifest
            .get(field)
            .and_then(toml::Value::as_str)
            .is_some_and(|value| value.contains(needle))
        {
            failures.push(format!(
                "0137 continuity manifest {field} missing `{needle}`"
            ));
        }
    }
    for needle in [
        "CREATE FUNCTION private.read_continuity_project_storage_v1(",
        "LANGUAGE plpgsql STABLE SECURITY DEFINER",
        "SET search_path TO pg_catalog",
        "ALTER FUNCTION private.read_continuity_project_storage_v1(",
        ") OWNER TO role_migration_owner;",
        "project.workspace_id=ANY(p_authorized_workspace_ids)",
        "project.lifecycle_state='ACTIVE'",
        "p_requested_workspace_id IS NULL",
        "pg_catalog.pg_input_is_valid(tenant_setting,'uuid')",
        "memory_links_exact boolean",
        "evidence_links_exact boolean",
        "stored_body_sha256 bytea",
        "ORDER BY slot.facet_kind NULLS FIRST",
        "REVOKE ALL ON FUNCTION private.read_continuity_project_storage_v1(",
        ") FROM PUBLIC;",
        "GRANT EXECUTE ON FUNCTION private.read_continuity_project_storage_v1(",
        ") TO role_gateway;",
        "array_agg(link.memory_id ORDER BY link.memory_id)",
        "array_agg(link.memory_sha256 ORDER BY link.memory_id)",
        "array_agg(link.evidence_id ORDER BY link.evidence_id)",
        "array_agg(link.evidence_sha256 ORDER BY link.evidence_id)",
    ] {
        if !migration.contains(needle) {
            failures.push(format!("0137 continuity reader missing `{needle}`"));
        }
    }
    for forbidden in [
        "CREATE TABLE",
        "INSERT INTO",
        "UPDATE private.",
        "DELETE FROM",
    ] {
        if migration.contains(forbidden) {
            failures.push(format!(
                "0137 continuity reader contains forbidden `{forbidden}`"
            ));
        }
    }
    for needle in [
        "pub const W2_FORCED_UNAVAILABLE: [ContinuityFacetKind; 6]",
        "VerifiedCurrentFacet::from_authoritative_jsonb_text",
        "humaux.continuity.result.v1\\0",
        "ContinuityFacetKind::ALL",
        "if accounted != 17",
        "if counts[0] == 17 && !closed.handoff.counts.overflow",
    ] {
        if !application.contains(needle) {
            failures.push(format!("application continuity surface missing `{needle}`"));
        }
    }
    for needle in [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
        "private.read_continuity_project_storage_v1",
        "install_lookup_context(&mut tx, authorization).await?;",
        "install_workspace_context(&mut tx, workspace).await?;",
        "let frozen = fetch_frozen_in_txn",
        "final_memory_ids_in_txn",
        "DiagnosticCode::VisibilityRevalidationFailed",
        "TOMBSTONED",
        "count(stream.commit_seq) AS association_count",
        "let body_sha256 = digest(raw.stored_body_sha256)?;",
    ] {
        if !adapter.contains(needle) {
            failures.push(format!("adapter continuity reader missing `{needle}`"));
        }
    }
    if adapter.contains("fetch_frozen(") || adapter.contains("assemble_materialized(") {
        failures.push("W2 reader opens a forbidden second materialization path".to_string());
    }
    for needle in ["read_project_continuity", "PostgresContinuityReadPort"] {
        if !gateway.contains(needle) {
            failures.push(format!("Gateway continuity route missing `{needle}`"));
        }
    }
    for needle in ["WorkspaceAdmission::PreserveContinuityFilter => None"] {
        if !guard.contains(needle) {
            failures.push(format!("Gateway continuity guard missing `{needle}`"));
        }
    }
    if guard
        .matches("operation.operation_key() != \"continuity.get\"")
        .count()
        != 2
    {
        failures.push(
            "Gateway continuity guard must preserve both Continuity scope branches".to_string(),
        );
    }
    for needle in [
        "SUPPORTED_OPERATION_KEYS: [&str; 14]",
        "\"continuity.get\" =>",
    ] {
        if !dispatch.contains(needle) {
            failures.push(format!("Gateway continuity dispatch missing `{needle}`"));
        }
    }
    if dispatch
        .matches("catalog.validate_output(ToolName::Continuity, &value)")
        .count()
        != 2
    {
        failures.push(
            "Gateway continuity dispatch must validate both Continuity result paths".to_string(),
        );
    }
    for source in [application, adapter, gateway, guard, dispatch] {
        for forbidden in ["Scope::Project", "embedding", "rerank", "USER_REASONING"] {
            if source.contains(forbidden) {
                failures.push(format!(
                    "W2 production path contains forbidden `{forbidden}`"
                ));
            }
        }
    }
    if failures.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail(failures)
    }
}

fn w2_continuity_gate(root: &Path) -> Verdict {
    let paths = [
        "migrations/0137_project_continuity_read.sql",
        "migrations/0137_project_continuity_read.manifest.toml",
        "crates/application/src/continuity.rs",
        "crates/adapters/src/continuity_read.rs",
        "bins/gateway/src/continuity.rs",
        "bins/gateway/src/guard.rs",
        "bins/gateway/src/mcp_application.rs",
    ];
    let mut sources = Vec::new();
    for path in paths {
        match fs::read_to_string(root.join(path)) {
            Ok(source) => sources.push(source),
            Err(_) => return Verdict::NotApplicable(format!("missing object: {path}")),
        }
    }
    w2_continuity_contract_from(
        &sources[0],
        &sources[1],
        &sources[2],
        &sources[3],
        &sources[4],
        &sources[5],
        &sources[6],
    )
}

#[allow(clippy::too_many_lines)] // §57.1: one flat, closed registry of every gate in declaration order — splitting it would hide the list this file exists to make visible.
pub fn run(_args: &[String]) -> i32 {
    let root = workspace_root();
    let mut checks: Vec<(&str, Verdict)> = vec![
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
            "§16.2 G80-4 (serving_version sole retrieval entry point)",
            g80_4_serving_version_sole_entry_point(&root),
        ),
        (
            "§48.0① G80-22 (payload_sha256 sole construction point)",
            g80_22_payload_sha256_unique(&root),
        ),
        (
            "§1.3/§48.0 G80-11 (source_hash sole construction point)",
            g80_11_source_hash_unique(&root),
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
        (
            "§20#G20-2 / G80-39 (online recall 无隐藏生成调用)",
            g20_2_no_hidden_generative_recall(&root),
        ),
        (
            "§23.1② (LedgerCounts 字段集恰为 6)",
            g23_1_ledger_counts_exactly_six_fields(&root),
        ),
        (
            "§22.5 (A1 算式全库只此一处)",
            g22_5_a1_sole_implementation(&root),
        ),
        (
            "§11.2.4–11.2.5 R3 (append-only exact-health admission contract)",
            r3_health_migration_gate(&root),
        ),
        (
            "§11.2.5 R3 (actual contribution call ledger/disclosure transaction)",
            r3_contribution_ledger_gate(&root),
        ),
        (
            "§25.3.1 W1 (Project Continuity storage/CAS/marker boundary)",
            w1_continuity_gate(&root),
        ),
        (
            "§25.3.1 W2 (Project Continuity native RR read boundary)",
            w2_continuity_gate(&root),
        ),
        (
            "ADR-0012 (gateway↔retrieval-worker boundary, worker-only symbols)",
            adr_0012_gateway_boundary_gate(&root),
        ),
        (
            "ADR-0012 (RPC transport stays UDS + peer_cred, never TCP)",
            adr_0012_uds_transport_gate(&root),
        ),
        (
            "ADR-0014 (gateway Qdrant access is QdrantReadOnly, never ReadWrite)",
            adr_0014_gateway_qdrant_read_only_gate(&root),
        ),
    ];
    checks.extend(provider_plane_architecture_gate_checks(&root));

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

    fn w1_real_sources() -> (String, String, String) {
        let root = real_root();
        (
            fs::read_to_string(root.join("migrations/0136_project_continuity.sql")).unwrap(),
            fs::read_to_string(root.join("crates/domain/src/continuity.rs")).unwrap(),
            fs::read_to_string(root.join("crates/adapters/src/continuity_repo.rs")).unwrap(),
        )
    }

    #[test]
    fn w1_continuity_real_repository_gate_is_green() {
        assert_eq!(w1_continuity_gate(&real_root()), Verdict::Pass);
    }

    #[test]
    fn w1_continuity_contract_mutations_are_red() {
        let (migration, domain, adapter) = w1_real_sources();
        for needle in [
            "ALTER TABLE private.continuity_projects FORCE ROW LEVEL SECURITY;",
            "CONSTRAINT continuity_facet_slots_kind_closed CHECK",
            "CONSTRAINT continuity_facet_slots_current_fkey FOREIGN KEY",
            "ORDER BY memory.memory_id FOR SHARE OF memory",
            "ORDER BY evidence.evidence_id FOR SHARE OF evidence",
            "AND slot.project_id=p_project_id AND slot.facet_kind=p_facet_kind\n    FOR UPDATE;",
            "slot.slot_version=p_expected_slot_version",
            "GET DIAGNOSTICS affected=ROW_COUNT",
            "IF affected<>1",
            "SET humaux.continuity_publish TO '1'",
            "CREATE POLICY continuity_evidence_owner_lock_allow",
            "END) WITH CHECK (false);",
            "CREATE POLICY continuity_evidence_owner_lock_guard",
            "END) WITH CHECK (\n      current_setting('humaux.continuity_publish',true) IS DISTINCT FROM '1');",
        ] {
            let broken = migration.replacen(needle, "", 1);
            assert!(
                matches!(
                    w1_continuity_contract_from(&broken, &domain, &adapter, 0),
                    Verdict::Fail(_)
                ),
                "mutation removing `{needle}` must fail the actual W1 gate"
            );
        }
        assert!(matches!(
            w1_continuity_contract_from(
                &format!("{migration}\n-- 'HANDOFF'"),
                &domain,
                &adapter,
                0
            ),
            Verdict::Fail(_)
        ));
        assert!(matches!(
            w1_continuity_contract_from(&migration, &domain, &adapter, 1),
            Verdict::Fail(_)
        ));
        assert!(matches!(
            w1_continuity_contract_from(
                &migration,
                &domain.replacen("continuity_uuid_v7!(ProjectId)", "", 1),
                &adapter,
                0,
            ),
            Verdict::Fail(_)
        ));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn w2_continuity_real_repository_and_boundary_mutations() {
        let root = real_root();
        let paths = [
            "migrations/0137_project_continuity_read.sql",
            "migrations/0137_project_continuity_read.manifest.toml",
            "crates/application/src/continuity.rs",
            "crates/adapters/src/continuity_read.rs",
            "bins/gateway/src/continuity.rs",
            "bins/gateway/src/guard.rs",
            "bins/gateway/src/mcp_application.rs",
        ];
        let sources: Vec<_> = paths
            .iter()
            .map(|path| fs::read_to_string(root.join(path)).unwrap())
            .collect();
        assert_eq!(
            w2_continuity_contract_from(
                &sources[0],
                &sources[1],
                &sources[2],
                &sources[3],
                &sources[4],
                &sources[5],
                &sources[6]
            ),
            Verdict::Pass
        );
        for (index, needle) in [
            (0, "LANGUAGE plpgsql STABLE SECURITY DEFINER"),
            (
                0,
                "ALTER FUNCTION private.read_continuity_project_storage_v1(",
            ),
            (0, ") OWNER TO role_migration_owner;"),
            (
                0,
                "REVOKE ALL ON FUNCTION private.read_continuity_project_storage_v1(",
            ),
            (0, ") FROM PUBLIC;"),
            (0, "project.workspace_id=ANY(p_authorized_workspace_ids)"),
            (0, "p_requested_workspace_id IS NULL"),
            (0, "pg_catalog.pg_input_is_valid(tenant_setting,'uuid')"),
            (0, "memory_links_exact boolean"),
            (0, "evidence_links_exact boolean"),
            (0, "stored_body_sha256 bytea"),
            (0, "array_agg(link.memory_id ORDER BY link.memory_id)"),
            (0, "array_agg(link.memory_sha256 ORDER BY link.memory_id)"),
            (0, "array_agg(link.evidence_id ORDER BY link.evidence_id)"),
            (
                0,
                "array_agg(link.evidence_sha256 ORDER BY link.evidence_id)",
            ),
            (1, "migration_id = \"0137_project_continuity_read\""),
            (
                1,
                "to_regprocedure('private.read_continuity_project_storage_v1(uuid,uuid,uuid,uuid,uuid,uuid[])') is null",
            ),
            (1, "to_regclass('private.continuity_projects') is not null"),
            (1, "private.publish_continuity_facet("),
            (
                1,
                "select count(*)=1 from reader where prosecdef and provolatile='s'",
            ),
            (1, "array['search_path=pg_catalog']::text[]"),
            (1, "pg_get_userbyid(proowner)='role_migration_owner'"),
            (1, "not has_function_privilege('public'"),
            (1, "role.name='role_gateway'"),
            (1, "SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER"),
            (1, "strpos(definition,'INSERT ') = 0"),
            (
                2,
                "pub const W2_FORCED_UNAVAILABLE: [ContinuityFacetKind; 6]",
            ),
            (2, "humaux.continuity.result.v1\\0"),
            (2, "if accounted != 17"),
            (2, "if counts[0] == 17 && !closed.handoff.counts.overflow"),
            (
                3,
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
            ),
            (3, "let frozen = fetch_frozen_in_txn"),
            (3, "TOMBSTONED"),
            (3, "install_lookup_context(&mut tx, authorization).await?;"),
            (3, "install_workspace_context(&mut tx, workspace).await?;"),
            (3, "count(stream.commit_seq) AS association_count"),
            (3, "let body_sha256 = digest(raw.stored_body_sha256)?;"),
            (5, "WorkspaceAdmission::PreserveContinuityFilter => None"),
            (5, "operation.operation_key() != \"continuity.get\""),
            (6, "catalog.validate_output(ToolName::Continuity, &value)"),
            (6, "SUPPORTED_OPERATION_KEYS: [&str; 14]"),
            (6, "\"continuity.get\" =>"),
        ] {
            let mut broken = sources.clone();
            broken[index] = broken[index].replacen(needle, "", 1);
            assert!(
                matches!(
                    w2_continuity_contract_from(
                        &broken[0], &broken[1], &broken[2], &broken[3], &broken[4], &broken[5],
                        &broken[6]
                    ),
                    Verdict::Fail(_)
                ),
                "mutation removing `{needle}` must fail the actual W2 gate"
            );
        }
    }

    #[test]
    fn r3_health_contract_real_migration_is_green() {
        let source = fs::read_to_string(
            real_root().join("migrations/0130_reasoning_route_health_admission.sql"),
        )
        .unwrap();
        assert_eq!(r3_health_migration_contract(&source), Verdict::Pass);
    }

    #[test]
    fn r3_health_contract_faults_current_projection_latest_order_and_fallback() {
        let source = fs::read_to_string(
            real_root().join("migrations/0130_reasoning_route_health_admission.sql"),
        )
        .unwrap();
        for (label, broken) in [
            (
                "current projection",
                format!("{source}\nCREATE TABLE ops.provider_health_current ();"),
            ),
            (
                "latest order",
                source.replacen(
                    "ORDER BY observation.observed_at DESC, observation.observation_id DESC",
                    "ORDER BY observation.observation_id DESC",
                    1,
                ),
            ),
            (
                "fallback candidate",
                source.replacen(
                    "candidate.fallback_class = 'NONE'",
                    "candidate.fallback_class = 'ANY'",
                    1,
                ),
            ),
            (
                "health audit receipt",
                source.replacen(
                    "source_kind text NOT NULL CHECK (btrim(source_kind) <> ''),",
                    "source_kind text,",
                    1,
                ),
            ),
        ] {
            assert!(
                matches!(r3_health_migration_contract(&broken), Verdict::Fail(_)),
                "{label} fault escaped"
            );
        }
    }

    fn r3_contribution_runtime_fixture() -> (&'static str, &'static str, &'static str, &'static str)
    {
        (
            "async fn resolve_and_reserve_reasoning_call() { load_with_admission(); let permit = authorize(admission.egress_processor_id); reserve_reasoning_call_in_txn(); reserve_reasoning_in_txn(); txn.commit(); }\nasync fn finalize_reasoning_call() { finalize_reasoning_in_txn(); finalize_reasoning_call_in_txn(); txn.commit(); }\nasync fn call_structured() { resolve_and_reserve_reasoning_call(); complete_structured(); self.finalize_reasoning_call(); }",
            "async fn reserve_reasoning_call_in_txn() { locator.egress_processor_id; }\nasync fn finalize_reasoning_call_in_txn() {}\negress_processor_id",
            "async fn reserve_reasoning_in_txn(model_call_id: Uuid) { model_call_id; reserve_in_txn(); }\nasync fn finalize_reasoning_in_txn() {}\nmodel_call_id",
            "fn endpoint_ref() {}",
        )
    }

    #[test]
    fn r3_contribution_runtime_contract_accepts_atomic_reserve_and_finalize_order() {
        let (contribution, ledger, disclosure, byok) = r3_contribution_runtime_fixture();
        assert_eq!(
            r3_contribution_runtime_contract(contribution, ledger, disclosure, byok),
            Verdict::Pass
        );
    }

    #[test]
    fn r3_contribution_runtime_contract_faults_reversed_reserves_and_config_authority() {
        let (contribution, ledger, disclosure, byok) = r3_contribution_runtime_fixture();
        let provider_before_reserve = contribution.replacen(
            "reserve_reasoning_call_in_txn(); reserve_reasoning_in_txn();",
            "reserve_reasoning_in_txn(); reserve_reasoning_call_in_txn();",
            1,
        );
        assert!(matches!(
            r3_contribution_runtime_contract(&provider_before_reserve, ledger, disclosure, byok),
            Verdict::Fail(_)
        ));
        let config_authority = contribution.replacen(
            "authorize(admission.egress_processor_id)",
            "authorize(self.config.allowed_egress_processor_id)",
            1,
        );
        assert!(matches!(
            r3_contribution_runtime_contract(&config_authority, ledger, disclosure, byok),
            Verdict::Fail(_)
        ));
    }

    fn r3_contribution_runtime_shared_helper_fixture() -> String {
        concat!(
            "async fn resolve_and_reserve_reasoning_call() { load_with_admission(); ",
            "let (wire_payload, permit) = authorize_structured_egress(tenant_id, &admission, &descriptor); ",
            "reserve_reasoning_call_in_txn(); reserve_reasoning_in_txn(); txn.commit() }\n",
            "async fn call_structured() { self.resolve_and_reserve_reasoning_call(); ",
            "complete_structured_timed(self.provider); self.finalize_reasoning_call() }\n",
            "async fn finalize_reasoning_call() { finalize_reasoning_in_txn(); ",
            "finalize_reasoning_call_in_txn(); txn.commit() }\n",
            "pub(crate) fn authorize_structured_egress() { let permit = authorize(admission.egress_processor_id); }\n",
            "pub(crate) async fn complete_structured_timed() { provider.complete_structured(context, request).await }\n",
        )
        .to_string()
    }

    #[test]
    fn r3_contribution_runtime_contract_follows_shared_helpers_one_level() {
        let (_, ledger, disclosure, byok) = r3_contribution_runtime_fixture();
        let contribution = r3_contribution_runtime_shared_helper_fixture();
        assert_eq!(
            r3_contribution_runtime_contract(&contribution, ledger, disclosure, byok),
            Verdict::Pass
        );
        for (label, broken) in [
            (
                "helper authorizes with config processor",
                contribution.replacen(
                    "authorize(admission.egress_processor_id)",
                    "authorize(self.config.allowed_egress_processor_id)",
                    1,
                ),
            ),
            (
                "reserve leg passes a non-resolver admission",
                contribution.replacen("&admission, &descriptor", "&self.config, &descriptor", 1),
            ),
            (
                "timed helper grows a retry loop",
                contribution.replacen(
                    "provider.complete_structured(context, request).await",
                    "loop { provider.complete_structured(context, request).await }",
                    1,
                ),
            ),
        ] {
            assert!(
                matches!(
                    r3_contribution_runtime_contract(&broken, ledger, disclosure, byok),
                    Verdict::Fail(_)
                ),
                "{label} fault escaped"
            );
        }
    }

    #[test]
    fn r3_idempotency_contract_real_sources_are_green() {
        let root = real_root();
        let contribution =
            fs::read_to_string(root.join("crates/adapters/src/contribution_reasoner.rs")).unwrap();
        let ledger =
            fs::read_to_string(root.join("crates/adapters/src/model_call_ledger.rs")).unwrap();
        let application =
            fs::read_to_string(root.join("crates/application/src/consolidate.rs")).unwrap();
        let contribute =
            fs::read_to_string(root.join("crates/application/src/contribute.rs")).unwrap();
        let pg_tests = fs::read_to_string(
            root.join("crates/adapters/tests/reasoning_route_health_admission.rs"),
        )
        .unwrap();
        assert_eq!(
            r3_contribution_idempotency_contract(
                &contribution,
                &ledger,
                &application,
                &contribute,
                &pg_tests,
            ),
            Verdict::Pass
        );
    }

    #[test]
    fn r3_idempotency_contract_faults_reasoner_minted_key_and_late_lookup() {
        let root = real_root();
        let contribution =
            fs::read_to_string(root.join("crates/adapters/src/contribution_reasoner.rs")).unwrap();
        let ledger =
            fs::read_to_string(root.join("crates/adapters/src/model_call_ledger.rs")).unwrap();
        let application =
            fs::read_to_string(root.join("crates/application/src/consolidate.rs")).unwrap();
        let contribute =
            fs::read_to_string(root.join("crates/application/src/contribute.rs")).unwrap();
        let pg_tests = fs::read_to_string(
            root.join("crates/adapters/tests/reasoning_route_health_admission.rs"),
        )
        .unwrap();
        for broken in [
            format!("Uuid::now_v7();\n{contribution}"),
            contribution.replacen("lookup_reasoning_call_in_txn", "lookup_removed", 1),
        ] {
            assert!(matches!(
                r3_contribution_idempotency_contract(
                    &broken,
                    &ledger,
                    &application,
                    &contribute,
                    &pg_tests,
                ),
                Verdict::Fail(_)
            ));
        }
        let typed_without_coverage = application.replacen(
            "ContributionReasoningCallKind::TypedAssessment, Some(_)",
            "ContributionReasoningCallKind::TypedAssessment, None",
            1,
        );
        assert!(matches!(
            r3_contribution_idempotency_contract(
                &contribution,
                &ledger,
                &typed_without_coverage,
                &contribute,
                &pg_tests,
            ),
            Verdict::Fail(_)
        ));
        let unversioned_intent =
            application.replacen("b\"intent_schema_version\\0\"", "b\"version\\0\"", 1);
        assert!(matches!(
            r3_contribution_idempotency_contract(
                &contribution,
                &ledger,
                &unversioned_intent,
                &contribute,
                &pg_tests,
            ),
            Verdict::Fail(_)
        ));
        let request_only_lock = ledger.replacen(
            "'model-call-request:' || $1::uuid::text || ':' || $2::uuid::text",
            "'model-call-request:' || $2::uuid::text",
            1,
        );
        assert!(matches!(
            r3_contribution_idempotency_contract(
                &contribution,
                &request_only_lock,
                &application,
                &contribute,
                &pg_tests,
            ),
            Verdict::Fail(_)
        ));
        let nil_allowed = contribution.replacen("attempt.logical_call_id.0.is_nil()", "false", 1);
        assert!(matches!(
            r3_contribution_idempotency_contract(
                &nil_allowed,
                &ledger,
                &application,
                &contribute,
                &pg_tests,
            ),
            Verdict::Fail(_)
        ));
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
    fn g80_2_real_repo_passes() {
        let root = real_root();
        assert_eq!(g80_2_build_request_unique(&root), Verdict::Pass);
    }

    /// T1.6 delivers `evidence::payload_sha256` / `EvidencePayloadSha256` (§48.0① G80-22): the
    /// real-repo check flips from `NotApplicable` (Phase 6 undelivered) to `Pass` (exactly one
    /// construction site, in `crates/domain/src/evidence.rs`).
    #[test]
    fn g80_22_real_repo_passes() {
        let root = real_root();
        assert_eq!(g80_22_payload_sha256_unique(&root), Verdict::Pass);
    }

    // ---- G80-43 (§11.10#G11-2) 注错族：fixture 树，不在真仓上变异 ----

    /// 最小合规 fixture：一个 `grounding.rs`，八条夹具齐、派生点恰一处、无手改 stale。
    /// `stub = true` 时派生点写成 `todo!()`——那是 G80-43 第 3 条断言要抓的形态，
    /// 不是合规 fixture 的默认长相（先前默认就是 todo!()，导致「闸对桩无感」这个洞
    /// 被自己的绿色测试盖住了，由对抗审查打出来）。
    fn grounding_fixture_inner(dir: &Path, fixtures: &[&str], derive_sites: usize, stub: bool) {
        fs::create_dir_all(dir).unwrap();
        let body = if stub {
            "{ todo!() }"
        } else {
            // 最小但**不是桩**的真函数体：形状够真，判据只看「不是 todo!()/unimplemented!()」。
            "{ let _ = i; GroundingState(GroundingStateKind::Current) }"
        };
        let mut src = String::new();
        for _ in 0..derive_sites {
            src.push_str(&format!(
                "pub fn derive_grounding_state(i: GroundingInputs<'_>) -> GroundingState {body}\n"
            ));
        }
        for f in fixtures {
            src.push_str(&format!("#[test]\nfn {f}() {{}}\n"));
        }
        fs::write(dir.join("grounding.rs"), src).unwrap();
    }

    /// 合规 fixture（非桩）。
    fn grounding_fixture(dir: &Path, fixtures: &[&str], derive_sites: usize) {
        grounding_fixture_inner(dir, fixtures, derive_sites, false);
    }

    const ALL_EIGHT: [&str; 8] = [
        "fixture_a_same_version_is_current",
        "fixture_b_changed_version_is_recheck_required",
        "fixture_c_missing_resource_is_unresolved",
        "fixture_d_resolver_error_is_cannot_establish_not_missing",
        "fixture_e_relocation_with_same_token_stays_current",
        "fixture_f_source_changed_mid_revalidation_is_stale_input",
        "fixture_g_recheck_required_must_not_enter_mandatory_context",
        "fixture_h_after_confirm_rebinding_returns_to_current",
    ];

    #[test]
    fn g80_43_green_on_compliant_fixture() {
        let tmp = fresh_tmp("g80-43-green");
        grounding_fixture(&tmp.join("crates/domain/src"), &ALL_EIGHT, 1);
        assert_eq!(g80_43_grounding_validity(&tmp), Verdict::Pass);
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 ①：删掉夹具 D（「resolver error 不能伪装 Missing」那条）→ 红并点名。
    /// 这正是判据静默变弱的形态：`cargo test` 少跑一条，全绿如故。
    #[test]
    fn g80_43_red_when_a_fixture_is_deleted() {
        let tmp = fresh_tmp("g80-43-red-fixture");
        let seven: Vec<&str> = ALL_EIGHT
            .iter()
            .copied()
            .filter(|f| !f.starts_with("fixture_d_"))
            .collect();
        grounding_fixture(&tmp.join("crates/domain/src"), &seven, 1);
        match g80_43_grounding_validity(&tmp) {
            Verdict::Fail(v) => assert!(
                v.iter().any(|m| m.contains("fn fixture_d_")),
                "必须点名缺失的那条夹具，而不是只说「有东西不对」: {v:?}"
            ),
            other => panic!("删掉夹具 D 必须红，实得 {other:?}"),
        }
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 ②：派生点被掏空（0 处）→ 活哨兵红。没有这条的话，`grounding.rs` 被清空成一个
    /// 只剩八条空测试的壳，前一条断言照样绿。
    #[test]
    fn g80_43_red_when_derivation_point_is_gone() {
        let tmp = fresh_tmp("g80-43-red-nohome");
        grounding_fixture(&tmp.join("crates/domain/src"), &ALL_EIGHT, 0);
        match g80_43_grounding_validity(&tmp) {
            Verdict::Fail(v) => assert!(v.iter().any(|m| m.contains("positive sentinel")), "{v:?}"),
            other => panic!("派生点消失必须红，实得 {other:?}"),
        }
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 ③：出现第二个派生点 → 活哨兵同样红（`!= 1` 不是 `< 1`）。
    #[test]
    fn g80_43_red_on_second_derivation_point() {
        let tmp = fresh_tmp("g80-43-red-two");
        grounding_fixture(&tmp.join("crates/domain/src"), &ALL_EIGHT, 2);
        assert!(
            matches!(g80_43_grounding_validity(&tmp), Verdict::Fail(_)),
            "两个派生点必须红"
        );
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 ④：派生点**在场但是桩** → 红。
    ///
    /// 这条是对抗审查的产物：先前三条断言只看 `pub fn derive_grounding_state(` 在不在，
    /// 于是 `{ todo!() }` 也判 Pass——闸绿着，而 spec §11.10 点名的注错（删掉 version
    /// compare）它一点感觉都没有。审查员正是拿本 fixture 当时的绿色证明了这个洞。
    #[test]
    fn g80_43_red_when_derivation_point_is_a_stub() {
        let tmp = fresh_tmp("g80-43-red-stub");
        grounding_fixture_inner(&tmp.join("crates/domain/src"), &ALL_EIGHT, 1, true);
        match g80_43_grounding_validity(&tmp) {
            Verdict::Fail(v) => assert!(
                v.iter()
                    .any(|m| m.contains("是桩") && m.contains("derive_grounding_state")),
                "必须点名「派生点是桩」，而不是含糊报错: {v:?}"
            ),
            other => panic!("桩化的派生点必须红，实得 {other:?}"),
        }
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 ⑤：DOD-092 —— 别处冒出一个可手改的 stale 真源 → 红并点名文件。
    #[test]
    fn g80_43_red_on_hand_settable_stale_flag() {
        let tmp = fresh_tmp("g80-43-red-stale");
        grounding_fixture(&tmp.join("crates/domain/src"), &ALL_EIGHT, 1);
        let other = tmp.join("crates/adapters/src");
        fs::create_dir_all(&other).unwrap();
        fs::write(
            other.join("repo.rs"),
            "fn mark(m: &mut Memory) {\n    m.stale = true;\n}\n",
        )
        .unwrap();
        match g80_43_grounding_validity(&tmp) {
            Verdict::Fail(v) => assert!(
                v.iter()
                    .any(|m| m.contains("repo.rs") && m.contains("DOD-092")),
                "{v:?}"
            ),
            other => panic!("手改 stale 必须红，实得 {other:?}"),
        }
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 ⑤：连赋值都没有，只是**声明**了一个可写的 `stale: bool` 字段 -> 仍然红。
    /// 这条钉的是 DOD-092 的「真源」二字：字段一旦存在，`*r = true` 之类的间接赋值就有了
    /// 落点，而那些写法逃得过赋值形态的匹配。
    #[test]
    fn g80_43_red_on_stale_field_declaration_even_without_assignment() {
        let tmp = fresh_tmp("g80-43-red-field");
        grounding_fixture(&tmp.join("crates/domain/src"), &ALL_EIGHT, 1);
        let other = tmp.join("crates/domain/src");
        fs::write(
            other.join("memory.rs"),
            "pub struct MemoryRow {\n    pub stale: bool,\n}\n",
        )
        .unwrap();
        match g80_43_grounding_validity(&tmp) {
            Verdict::Fail(v) => assert!(
                v.iter()
                    .any(|m| m.contains("memory.rs") && m.contains("DOD-092")),
                "{v:?}"
            ),
            other => panic!("可写 stale 字段的声明必须红，实得 {other:?}"),
        }
        fs::remove_dir_all(&tmp).ok();
    }

    /// 反向对照：**读** stale 不是 DOD-092 禁的事，只有赋值才是。没有这条的话，判据可能被
    /// 写成粗暴匹配 `stale`，把合法的读判定也误报成违规（G80-3 上线首日就是这类误报）。
    #[test]
    fn g80_43_reading_a_stale_field_is_not_a_violation() {
        let tmp = fresh_tmp("g80-43-green-read");
        grounding_fixture(&tmp.join("crates/domain/src"), &ALL_EIGHT, 1);
        let other = tmp.join("crates/adapters/src");
        fs::create_dir_all(&other).unwrap();
        fs::write(
            other.join("repo.rs"),
            "fn is_stale(m: &Memory) -> bool {\n    m.stale\n}\n",
        )
        .unwrap();
        assert_eq!(g80_43_grounding_validity(&tmp), Verdict::Pass);
        fs::remove_dir_all(&tmp).ok();
    }

    /// 三态：被测对象整个不存在（Phase 4 之前）→ `not_applicable` 并**点名缺失对象**，
    /// 不是静默 pass（§57.1 第2条）。
    #[test]
    fn g80_43_not_applicable_without_the_grounding_module() {
        let tmp = fresh_tmp("g80-43-na");
        fs::create_dir_all(tmp.join("crates/domain/src")).unwrap();
        match g80_43_grounding_validity(&tmp) {
            Verdict::NotApplicable(m) => {
                assert!(m.contains("missing object"), "{m}");
                assert!(m.contains("grounding.rs"), "{m}");
            }
            other => panic!("缺被测对象时必须 not_applicable 并点名，实得 {other:?}"),
        }
        fs::remove_dir_all(&tmp).ok();
    }

    /// 真仓现状：Phase 4 的 grounding 已交付 → pass。
    #[test]
    fn g80_43_real_repo_passes() {
        assert_eq!(g80_43_grounding_validity(&real_root()), Verdict::Pass);
    }

    // ---- G80-4 (§16.2 换代期读路由) 注错族 ----

    /// 造一棵最小树：`serving_repo.rs` 里有定义，`consumers` 里各放一个调用点。
    fn serving_version_fixture(root: &Path, consumers: &[(&str, usize)]) {
        let def = root.join("crates/adapters/src");
        fs::create_dir_all(&def).unwrap();
        fs::write(
            def.join("serving_repo.rs"),
            "pub async fn serving_version(pool: &P, f: &F) -> R { todo!() }\n",
        )
        .unwrap();
        for (rel, n) in consumers {
            let path = root.join(rel);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            let body: String = (0..*n)
                .map(|_| "    let _ = serving_version(pool, &f).await;\n".to_string())
                .collect();
            fs::write(&path, format!("pub async fn consume() {{\n{body}}}\n")).unwrap();
        }
    }

    #[test]
    fn g80_4_passes_with_exactly_one_consumer_call_site() {
        let tmp = fresh_tmp("g80-4-green");
        serving_version_fixture(&tmp, &[("crates/adapters/src/retrieve.rs", 1)]);
        assert_eq!(g80_4_serving_version_sole_entry_point(&tmp), Verdict::Pass);
        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn g80_4_transaction_form_preserves_the_single_consumer_constraint() {
        let tmp = fresh_tmp("g80-4-transaction");
        let consumer = "crates/adapters/src/retrieve.rs";
        serving_version_fixture(&tmp, &[(consumer, 1)]);
        let path = tmp.join(consumer);
        let original = fs::read_to_string(&path).unwrap();
        fs::write(
            &path,
            original.replace("serving_version(", "serving_version_in_txn("),
        )
        .unwrap();
        assert_eq!(g80_4_serving_version_sole_entry_point(&tmp), Verdict::Pass);

        // One caller of each spelling is still two consumers, not two independent passes.
        fs::write(tmp.join("crates/adapters/src/second.rs"), original).unwrap();
        assert!(matches!(
            g80_4_serving_version_sole_entry_point(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    /// **本族最重要的一条。** 零调用点 = §16.2 被违反的状态本身，必须判 `Fail` 而**不是**
    /// `NotApplicable`。先前那版正是在这里判 NA，于是闸在违规最严重时最安静（ADR-0006）。
    #[test]
    fn g80_4_zero_call_sites_is_fail_not_not_applicable() {
        let tmp = fresh_tmp("g80-4-zero");
        serving_version_fixture(&tmp, &[]);
        match g80_4_serving_version_sole_entry_point(&tmp) {
            Verdict::Fail(v) => assert!(
                v.iter().any(|m| m.contains("消费侧调用点为 0")),
                "必须点名「一个调用点都没有」而不是含糊报错: {v:?}"
            ),
            other => panic!(
                "零调用点必须红——那正是违规状态本身，判 not_applicable 等于把违规当成\
                 「没什么可判的」。实得 {other:?}"
            ),
        }
        fs::remove_dir_all(&tmp).ok();
    }

    /// 三态：被测对象（`serving_version` 函数本身）真的不存在时才 NA，且必须点名。
    #[test]
    fn g80_4_not_applicable_only_when_the_function_itself_is_absent() {
        let tmp = fresh_tmp("g80-4-na");
        fs::create_dir_all(tmp.join("crates/adapters/src")).unwrap();
        fs::write(
            tmp.join("crates/adapters/src/retrieve.rs"),
            "pub fn nothing() {}\n",
        )
        .unwrap();
        match g80_4_serving_version_sole_entry_point(&tmp) {
            Verdict::NotApplicable(m) => {
                assert!(m.contains("missing object"), "{m}");
                assert!(m.contains("serving_version"), "{m}");
            }
            other => panic!("函数本身不存在时才该 NA，实得 {other:?}"),
        }
        fs::remove_dir_all(&tmp).ok();
    }

    /// 第二个调用点 → 红（`== 1` 不是 `>= 1`）。
    #[test]
    fn g80_4_red_on_a_second_consumer_call_site() {
        let tmp = fresh_tmp("g80-4-two");
        serving_version_fixture(
            &tmp,
            &[
                ("crates/adapters/src/retrieve.rs", 1),
                ("crates/retrieval/src/planner.rs", 1),
            ],
        );
        assert!(
            matches!(
                g80_4_serving_version_sole_entry_point(&tmp),
                Verdict::Fail(_)
            ),
            "两个消费侧调用点必须红"
        );
        fs::remove_dir_all(&tmp).ok();
    }

    /// 排除按**精确路径**而非子串：调用点藏进一个路径含 `serving_repo` 子串的文件里，
    /// 照样要被数到。先前那版 `disp.contains("serving_repo")` 会把它整个排除 = 假绿。
    #[test]
    fn g80_4_substring_named_file_does_not_escape_the_count() {
        let tmp = fresh_tmp("g80-4-substr");
        serving_version_fixture(
            &tmp,
            &[
                ("crates/adapters/src/retrieve.rs", 1),
                ("crates/adapters/src/serving_repo_client.rs", 1),
            ],
        );
        match g80_4_serving_version_sole_entry_point(&tmp) {
            Verdict::Fail(v) => assert!(
                v.iter().any(|m| m.contains("serving_repo_client.rs")),
                "藏在子串同名文件里的调用点必须被点名: {v:?}"
            ),
            other => panic!("子串同名文件不得逃过计数，实得 {other:?}"),
        }
        fs::remove_dir_all(&tmp).ok();
    }

    /// 真仓现状：读路径已接入 ⇒ 恰好一处 ⇒ pass。
    #[test]
    fn g80_4_real_repo_passes() {
        assert_eq!(
            g80_4_serving_version_sole_entry_point(&real_root()),
            Verdict::Pass
        );
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

    // -- §1.3/§48.0 G80-11 (source_hash leg, T5.1) -------------------------------------------

    /// T5.1 delivers `projection::fingerprint::source_hash` / `SourceHash`: the real-repo
    /// check flips from `NotApplicable` to `Pass` (exactly one construction site, in
    /// `crates/projection/src/fingerprint.rs`) the same way G80-22 did for T1.6.
    #[test]
    fn g80_11_real_repo_passes() {
        let root = real_root();
        assert_eq!(g80_11_source_hash_unique(&root), Verdict::Pass);
    }

    #[test]
    fn g80_11_passes_with_exactly_one_construction_site() {
        let tmp = fresh_tmp("g80-11-green");
        let dir = tmp.join("crates/projection/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("fingerprint.rs"),
            "struct SourceHash([u8; 32]);\npub fn source_hash(x: u64) -> SourceHash {\n    SourceHash([0; 32])\n}\n",
        )
        .unwrap();
        assert_eq!(g80_11_source_hash_unique(&tmp), Verdict::Pass);
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 (G80-11): matcher's type name typo'd to a nonexistent name → 0 hits → 红 (same
    /// shape as G80-22's zero-construction-sites 注错).
    #[test]
    fn g80_11_fails_when_object_exists_but_zero_construction_sites_found() {
        let tmp = fresh_tmp("g80-11-red-zero");
        let dir = tmp.join("crates/projection/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("fingerprint.rs"),
            "struct SourceHash([u8; 32]);\npub fn source_hash(x: u64) -> SourceHash {\n    todo!()\n}\n",
        )
        .unwrap();
        assert!(matches!(g80_11_source_hash_unique(&tmp), Verdict::Fail(_)));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 (G80-11): a second construction site in the real top-level `evals/` — same shape
    /// as G80-22's evals/ 注错, proving the workspace-root scan (not a crates/-only one)
    /// catches a second crate re-deriving the fingerprint encoding.
    #[test]
    fn g80_11_fails_with_second_construction_site_in_real_evals_dir() {
        let tmp = fresh_tmp("g80-11-red-evals");
        let dir = tmp.join("crates/projection/src");
        let evals_dir = tmp.join("evals");
        fs::create_dir_all(&dir).unwrap();
        fs::create_dir_all(&evals_dir).unwrap();
        fs::write(
            dir.join("fingerprint.rs"),
            "struct SourceHash([u8; 32]);\npub fn source_hash(x: u64) -> SourceHash {\n    SourceHash([0; 32])\n}\n",
        )
        .unwrap();
        fs::write(
            evals_dir.join("probe.rs"),
            "fn shortcut() { let _ = SourceHash([1; 32]); }\n",
        )
        .unwrap();
        assert!(matches!(g80_11_source_hash_unique(&tmp), Verdict::Fail(_)));
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
    /// 豁免必须是**窄**的：同一个文件里读一个不带测试前缀的变量，照旧算违规。
    /// 没有这条，`TEST_CONTROL_ENV_PREFIXES` 就等于把 env 扫描整体关掉了。
    #[test]
    fn env_var_scan_test_control_exemption_does_not_cover_production_config() {
        let src = "pub fn a() { let _ = std::env::var(\"HUMAUX_REQUIRE_DB\"); }\n\
                   pub fn b() { let _ = std::env::var(\"DATABASE_URL\"); }\n";
        let hits = scan_env_var_violations("crates/testkit/src/lib.rs", src);
        assert_eq!(
            hits.len(),
            1,
            "只有第 2 行（DATABASE_URL，生产配置）该被抓；第 1 行是测试控制量: {hits:?}"
        );
        assert!(hits[0].ends_with(":2"), "抓错行了: {hits:?}");
    }

    /// 反向：名字由变量传入、静态看不出读的是什么 —— 不豁免（宁可误报也不放过）。
    #[test]
    fn env_var_scan_does_not_exempt_a_dynamically_named_read() {
        let src = "pub fn a(name: &str) { let _ = std::env::var(name); }\n";
        assert_eq!(
            scan_env_var_violations("crates/testkit/src/lib.rs", src).len(),
            1,
            "名字看不见时不得豁免"
        );
    }

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
    /// G80-40 static: real repo must pass (seven wrappers once each, no raw pool outside,
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
        // wrappers still declared (so the seven-count check passes) but their inner field
        // no longer *names* the raw type — only a comment mentions it. A live scanner
        // must notice the naming sentinel went to 0.
        fs::write(
            tmp.join("crates/adapters/src/postgres.rs"),
            "// inner is a sqlx::PgPool, morally\n\
             pub struct RuntimeDbPool(());\n\
             pub struct BatchIssuerDbPool(());\n\
             pub struct ConsolidationDbPool(());\n\
             pub struct PrivateWorkerDbPool(());\n\
             pub struct RetrievalWorkerDbPool(());\n\
             pub struct MaintenanceDbPool(());\n\
             pub struct PublicWorkerDbPool(());\n\
             pub struct AdminDbPool(());\n",
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
             pub struct PrivateWorkerDbPool { inner: sqlx::PgPool }\n\
             pub struct RetrievalWorkerDbPool { inner: sqlx::PgPool }\n\
             pub struct MaintenanceDbPool { inner: sqlx::PgPool }\n\
             pub struct PublicWorkerDbPool { inner: sqlx::PgPool }\n\
             pub struct AdminDbPool { inner: sqlx::PgPool }\n",
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
                root.join("crates/infra-network/src/http.rs"),
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
                root.join("crates/infra-network/src/http.rs"),
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
                root.join("crates/infra-network/src/http.rs"),
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
                root.join("crates/infra-network/src/http.rs"),
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

    // -- §83.4 判据1, 注错 f: IntraCellResource string-constructor scan ---------------------

    /// Baseline: `IntraCellResource`'s real `impl` (closed enum, an `ALL` const, no fn
    /// converting a string to `Self`) must scan clean.
    #[test]
    fn g80_3_no_string_to_resource_constructor_fn_clean_impl_is_empty() {
        let src = "pub enum IntraCellResource {\n    QDRANT_REST,\n}\n\
                   impl IntraCellResource {\n    \
                       pub const ALL: [IntraCellResource; 1] = [Self::QDRANT_REST];\n\
                   }\n";
        assert!(g80_3_no_string_to_resource_constructor_fn(src).is_empty());
    }

    /// Decisive blindness proof (the exact injection the review found): an arbitrarily-named
    /// associated fn — not `From::from`, not `FromStr::parse` — taking `&str` and returning
    /// `Self` inside `impl IntraCellResource` is exactly the raw-URL construction path 判据1
    /// forbids, and no fixed-name trybuild fixture would ever exercise it.
    #[test]
    fn g80_3_no_string_to_resource_constructor_fn_flags_arbitrarily_named_fn() {
        let src = "pub enum IntraCellResource {\n    QDRANT_REST,\n}\n\
                   impl IntraCellResource {\n    \
                       pub fn from_url(_u: &str) -> Self {\n        Self::QDRANT_REST\n    }\n\
                   }\n";
        let problems = g80_3_no_string_to_resource_constructor_fn(src);
        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("from_url"));
    }

    /// The standard-trait spellings (`From<&str>`, `FromStr::parse` returning `Self`) must
    /// also be caught — this scan is meant to subsume, not merely supplement, what a
    /// `From<&str>`-shaped trybuild fixture would catch.
    #[test]
    fn g80_3_no_string_to_resource_constructor_fn_flags_from_str_impl() {
        let src = "pub enum IntraCellResource {\n    QDRANT_REST,\n}\n\
                   impl From<&str> for IntraCellResource {\n    \
                       fn from(_s: &str) -> Self {\n        Self::QDRANT_REST\n    }\n\
                   }\n";
        let problems = g80_3_no_string_to_resource_constructor_fn(src);
        assert_eq!(problems.len(), 1);
    }

    /// Negative control for [`header_names_ident_exactly`]: an unrelated type whose name
    /// merely *contains* `IntraCellResource` as a substring (`IntraCellResourceRegistry`) must
    /// not be scanned as if it were `IntraCellResource`'s own impl — a real `&str -> Self`
    /// fn on that unrelated type is not a 判据1 violation.
    #[test]
    fn g80_3_no_string_to_resource_constructor_fn_ignores_registry_type_name_collision() {
        let src = "impl IntraCellResourceRegistry {\n    \
                       pub fn from_str(_s: &str) -> Self {\n        todo!()\n    }\n\
                   }\n";
        assert!(g80_3_no_string_to_resource_constructor_fn(src).is_empty());
    }

    /// Negative control: an unrelated type in the *same file* with a legitimate
    /// `&str -> Result<Self, _>` (`CellCidr::from_str`, real production shape) must never be a
    /// false hit — the scan is scoped to `IntraCellResource`'s own impl blocks only.
    #[test]
    fn g80_3_no_string_to_resource_constructor_fn_ignores_unrelated_type_from_str() {
        let src = "impl FromStr for CellCidr {\n    \
                       type Err = CellCidrParseError;\n    \
                       fn from_str(s: &str) -> Result<Self, Self::Err> {\n        todo!()\n    }\n\
                   }\n\
                   impl IntraCellResource {\n    \
                       pub const ALL: [IntraCellResource; 1] = [Self::QDRANT_REST];\n\
                   }\n";
        assert!(g80_3_no_string_to_resource_constructor_fn(src).is_empty());
    }

    /// The real `crates/infra-cell/src/resource.rs` must scan clean end to end.
    #[test]
    fn g80_3_no_string_to_resource_constructor_fn_real_file_is_clean() {
        let source = fs::read_to_string(real_root().join(INFRA_CELL_RESOURCE_RS)).unwrap();
        assert!(g80_3_no_string_to_resource_constructor_fn(&source).is_empty());
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
                root.join("crates/infra-network/src/http.rs"),
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
                root.join("crates/infra-network/src/http.rs"),
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
                root.join("crates/infra-network/src/http.rs"),
                "pub fn f() -> reqwest::Client { reqwest::Client::new() }\n".to_string(),
            ),
            (root.join("crates/domain/src/egress.rs"), egress_src),
        ];
        let problems = g80_3_transport_and_registry_check(&files, &root);
        assert!(problems.iter().any(|p| p.contains("MYSTERY_PROVIDER")));
    }

    /// 注错 0a (§83.4): deleting `infra-network/src/http.rs` must flip the positive sentinel
    /// red, even when `cargo metadata` still (on paper) lists the crate as a member.
    #[test]
    fn g80_3_sentinel_flags_deleted_http_rs() {
        let tmp = fresh_tmp("g80-3-deleted-http-rs");
        fs::create_dir_all(tmp.join("crates/infra-network/src")).unwrap();
        fs::write(tmp.join("crates/infra-network/Cargo.toml"), "").unwrap();
        // http.rs deliberately not written.
        let fake_metadata = r#"{"packages":[{"name":"humaux-infra-network"}]}"#;
        let problems = g80_3_workspace_sentinel(&tmp, Some(fake_metadata));
        assert!(problems.iter().any(|p| p.contains(INFRA_NETWORK_HTTP_RS)));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 0b (§83.4): removing the crate from `cargo metadata`'s member list (e.g. dropped
    /// from the workspace `Cargo.toml`) must flip the positive sentinel red even though the
    /// files themselves are still on disk.
    #[test]
    fn g80_3_sentinel_flags_missing_metadata_member() {
        let tmp = fresh_tmp("g80-3-missing-member");
        fs::create_dir_all(tmp.join("crates/infra-network/src")).unwrap();
        fs::write(tmp.join("crates/infra-network/Cargo.toml"), "").unwrap();
        fs::write(tmp.join(INFRA_NETWORK_HTTP_RS), "").unwrap();
        let fake_metadata = r#"{"packages":[{"name":"humaux-domain"}]}"#;
        let problems = g80_3_workspace_sentinel(&tmp, Some(fake_metadata));
        assert!(problems.iter().any(|p| p.contains("cargo metadata")));
        fs::remove_dir_all(&tmp).ok();
    }

    /// Regression pin for the `member_present` fix: `"humaux-infra-network"` appearing only as
    /// a *substring* of some other package's dependency listing (a plausible shape once a
    /// second crate path-depends on it by name) must NOT read as the crate itself being a
    /// workspace member — the bug this test would have caught: `.contains("humaux-infra-network")`
    /// over the raw JSON text stays true here even though no `packages[].name` equals it.
    #[test]
    fn g80_3_sentinel_member_present_requires_exact_name_not_substring() {
        let tmp = fresh_tmp("g80-3-substring-not-member");
        fs::create_dir_all(tmp.join("crates/infra-network/src")).unwrap();
        fs::write(tmp.join("crates/infra-network/Cargo.toml"), "").unwrap();
        fs::write(tmp.join(INFRA_NETWORK_HTTP_RS), "").unwrap();
        // No package here is literally named `humaux-infra-network` — it only appears inside
        // another package's dependency list (e.g. after the crate was dropped from `members`
        // but a stale path-dependency edge to it still resolves in the lockfile/graph).
        let fake_metadata = r#"{"packages":[{"name":"humaux-domain","dependencies":[]},
            {"name":"humaux-other","dependencies":[{"name":"humaux-infra-network"}]}]}"#;
        let problems = g80_3_workspace_sentinel(&tmp, Some(fake_metadata));
        assert!(
            problems.iter().any(|p| p.contains("cargo metadata")),
            "a dependency edge naming the crate must not be mistaken for workspace membership: \
             {problems:?}"
        );
        fs::remove_dir_all(&tmp).ok();
    }

    /// §83.4 判据0, full-path 注错 0a: with `crates/infra-network` already delivered, deleting
    /// the whole directory must flip [`g80_3_outbound_choke_point`] itself to `Fail`, not
    /// `NotApplicable` — the early-return this test replaces made deletion vacuously
    /// undetectable (判据1's raw-client scan finds nothing to complain about when there is
    /// nothing left to scan).
    #[test]
    fn g80_3_outbound_choke_point_fails_when_infra_egress_directory_is_deleted() {
        let tmp = fresh_tmp("g80-3-deleted-crate-full-path");
        fs::create_dir_all(&tmp).unwrap();
        // No `crates/infra-network` at all, and no workspace `Cargo.toml` for `cargo metadata`
        // to succeed against either — both read as absence, which is exactly the point: once
        // delivered, absence is a fault, never a legitimate "nothing to check" state.
        let mut expected = g80_3_workspace_sentinel(&tmp, None);
        expected.extend(g80_3_layer1_crates_sentinel(&tmp, None));
        assert_eq!(g80_3_outbound_choke_point(&tmp), Verdict::Fail(expected));
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
            {"name":"humaux-infra-network",
             "manifest_path":"/fixture-root/crates/infra-network/Cargo.toml",
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

    /// Positive control for [`g80_3_infra_network_dependents_check`]: the real repo's own
    /// `cargo metadata` document must show exactly the two Layer 1 crates depending on
    /// `humaux-infra-network`.
    #[test]
    fn g80_3_infra_network_dependents_check_real_repo_is_clean() {
        let root = real_root();
        let output = Command::new("cargo")
            .args(["metadata", "--no-deps", "--format-version", "1"])
            .current_dir(&root)
            .output()
            .expect("cargo metadata");
        assert!(output.status.success());
        let metadata_json = String::from_utf8_lossy(&output.stdout);
        assert!(g80_3_infra_network_dependents_check(&metadata_json, &root).is_empty());
    }

    /// Decisive proof, reproducing the exact exploit the review found: a third crate
    /// (`humaux-adapters`) adding a path dependency on `humaux-infra-network` — to reach the
    /// `reqwest` re-export without ever listing `reqwest` itself — must be reported by this
    /// dependents-set check even though [`g80_3_manifest_dependency_check`] alone stays clean
    /// for it (it never lists `reqwest`/`hyper` directly).
    #[test]
    fn g80_3_infra_network_dependents_check_fault_third_crate_depends_on_infra_network() {
        let root = PathBuf::from("/fixture-root");
        let metadata_json = r#"{"packages":[
            {"name":"humaux-infra-egress",
             "manifest_path":"/fixture-root/crates/infra-egress/Cargo.toml",
             "dependencies":[{"name":"humaux-infra-network"}]},
            {"name":"humaux-infra-cell",
             "manifest_path":"/fixture-root/crates/infra-cell/Cargo.toml",
             "dependencies":[{"name":"humaux-infra-network"}]},
            {"name":"humaux-adapters",
             "manifest_path":"/fixture-root/crates/adapters/Cargo.toml",
             "dependencies":[{"name":"humaux-infra-network"}]}
        ]}"#;
        let problems = g80_3_infra_network_dependents_check(metadata_json, &root);
        assert!(
            problems
                .iter()
                .any(|p| p.contains("crates/adapters/Cargo.toml")),
            "{problems:?}"
        );
    }

    /// §83.4 判据1, decisive proof (the exact injected-alias exploit the review found): `use
    /// humaux_infra_network::reqwest::Client as HttpClient; HttpClient::new()` must be caught —
    /// [`BARE_HTTP_CLIENT_NEEDLES`]'s word-boundary guard rejects the call site itself (the
    /// byte before `Client::new(` inside `HttpClient::new(` is `p`), but
    /// [`REQWEST_CLIENT_ALIAS_IMPORT_NEEDLES`] catches the import line regardless of the
    /// alias's chosen name.
    #[test]
    fn find_raw_http_client_calls_catches_renamed_alias_import_regardless_of_call_site() {
        let src = "use humaux_infra_network::reqwest::Client as HttpClient;\n\
                   pub fn injected_alias() -> HttpClient { HttpClient::new() }\n";
        assert_eq!(find_raw_http_client_calls(src).len(), 1);
    }

    // ============================================================================
    // ADR-0003 / §83.4 Layer 1B: intra-cell-resource-registry checks + the two注错 tests the
    // task explicitly names — (a) adapters constructing `reqwest::Client` directly, (b) Qdrant
    // sneaking into `external-egress-registry`. (c) ("`IntraCellResource` accepts a raw URL")
    // is a compile-time property, not a source-scan one — proved by
    // `crates/infra-cell/tests/intra_cell_topology_ui.rs`'s `trybuild` fixture instead.
    // ============================================================================

    fn intra_cell_resource_fixture() -> String {
        "pub enum IntraCellResource {\n    QDRANT_REST,\n}\n".to_string()
    }

    #[test]
    fn g80_3_intra_cell_registry_clean_fixture_has_no_problems() {
        let root = PathBuf::from("/fixture-root");
        let files = vec![(
            root.join(INFRA_CELL_RESOURCE_RS),
            intra_cell_resource_fixture(),
        )];
        assert!(g80_3_intra_cell_registry_check(&files, &root).is_empty());
    }

    /// ADR-0003 判据1 sibling of 注错 b: `IntraCellResource` growing an unregistered variant
    /// must turn this check red, same shape [`g80_3_fault_unregistered_purpose_variant_is_red`]
    /// already proves for `OutboundPurpose`.
    #[test]
    fn g80_3_intra_cell_registry_fault_unregistered_resource_is_red() {
        let root = PathBuf::from("/fixture-root");
        let src = intra_cell_resource_fixture()
            .replace("QDRANT_REST,\n", "QDRANT_REST,\n    MYSTERY_RESOURCE,\n");
        let files = vec![(root.join(INFRA_CELL_RESOURCE_RS), src)];
        let problems = g80_3_intra_cell_registry_check(&files, &root);
        assert!(problems.iter().any(|p| p.contains("MYSTERY_RESOURCE")));
    }

    /// 注错 (a) (task item, source-level): a raw `reqwest::Client::new` written directly inside
    /// `adapters/src/qdrant.rs` — the exact file ADR-0003 wires to `IntraCellHttpTransport`
    /// instead of a raw client — must turn 判据1 red and name that file. Same mechanism
    /// [`g80_3_fault_raw_client_outside_http_rs_is_red_and_named`] already proves generically,
    /// pinned here against the specific file/crate the task calls out by name.
    #[test]
    fn g80_3_fault_adapters_qdrant_constructs_raw_reqwest_client_is_red() {
        let root = PathBuf::from("/fixture-root");
        let files = vec![
            (
                root.join(INFRA_NETWORK_HTTP_RS),
                "pub fn f() -> reqwest::Client { reqwest::Client::new() }\n".to_string(),
            ),
            (
                root.join("crates/domain/src/egress.rs"),
                outbound_purpose_fixture(),
            ),
            (
                root.join("crates/adapters/src/qdrant.rs"),
                "pub fn sneaky() -> reqwest::Client { reqwest::Client::new() }\n".to_string(),
            ),
        ];
        let problems = g80_3_transport_and_registry_check(&files, &root);
        assert!(
            problems
                .iter()
                .any(|p| p.contains("crates/adapters/src/qdrant.rs")),
            "{problems:?}"
        );
    }

    /// 注错 (b) (task item): Qdrant sneaking into `external-egress-registry` while still
    /// present in `intra-cell-resource-registry` — the coordinated double-registration
    /// [`intra_cell_registry_disjoint_from_external`] exists to catch, which neither registry's
    /// own single-sided set-equality check (判据2 / this module's intra-cell sibling) can see on
    /// its own (both would independently read PASS if `OutboundPurpose`/`EXTERNAL_EGRESS_
    /// REGISTRY` were edited together to add the same name `IntraCellResource` already has).
    #[test]
    fn g80_3_fault_qdrant_added_to_external_egress_registry_is_red() {
        let external_with_qdrant: Vec<&str> = EXTERNAL_EGRESS_REGISTRY
            .iter()
            .copied()
            .chain(std::iter::once("QDRANT_REST"))
            .collect();
        let problems = intra_cell_registry_disjoint_from_external(
            &external_with_qdrant,
            INTRA_CELL_RESOURCE_REGISTRY,
        );
        assert!(
            problems.iter().any(|p| p.contains("QDRANT_REST")),
            "{problems:?}"
        );
    }

    /// Positive control: the real workspace's two registries must in fact be disjoint today.
    #[test]
    fn g80_3_real_registries_are_disjoint() {
        assert!(
            intra_cell_registry_disjoint_from_external(
                EXTERNAL_EGRESS_REGISTRY,
                INTRA_CELL_RESOURCE_REGISTRY,
            )
            .is_empty()
        );
    }

    // -- §20#G20-2 / G80-39: online recall/context/continuity — zero static dependency on
    // any reasoning provider ----------------------------------------------------------------

    /// Standalone proof the matcher itself recognizes the needle text — independent of any
    /// file — since this rule has no legal permanent positive-hit fixture inside its own scan
    /// domain (see [`g20_2_no_hidden_generative_recall`]'s doc for why not).
    #[test]
    fn count_identifier_hits_word_boundary_and_comment_rules() {
        assert_eq!(
            count_identifier_hits(
                "let x = UserReasoningProvider::default();",
                "UserReasoningProvider",
                true
            ),
            1
        );
        assert_eq!(
            count_identifier_hits(
                "// UserReasoningProvider mentioned only in prose",
                "UserReasoningProvider",
                true
            ),
            0
        );
        // `UserReasoningProviderFactory` shares the needle as a prefix but is a different
        // identifier — next byte is still an ident byte, so with `require_right_boundary:
        // true` it must not count as a hit.
        assert_eq!(
            count_identifier_hits(
                "struct UserReasoningProviderFactory;",
                "UserReasoningProvider",
                true
            ),
            0
        );
    }

    /// Decisive: with `require_right_boundary: false`, `complete_structured` must count a
    /// call to `complete_structured_with_bounded_repair` — this is the exact real call site
    /// (`crates/adapters/src/byok.rs`) the old always-full-identifier matcher silently missed
    /// (0 hits under the pre-fix matcher; see [`FORBIDDEN_GENERATIVE_NEEDLES`]'s doc).
    #[test]
    fn count_identifier_hits_prefix_mode_catches_bounded_repair_call() {
        assert_eq!(
            count_identifier_hits(
                "rewrite::complete_structured_with_bounded_repair(req).await",
                "complete_structured",
                false
            ),
            1
        );
        // Same source, `require_right_boundary: true` (the old behavior) — 0 hits, proving
        // this is a genuine regression the prefix mode fixes, not a redundant belt.
        assert_eq!(
            count_identifier_hits(
                "rewrite::complete_structured_with_bounded_repair(req).await",
                "complete_structured",
                true
            ),
            0
        );
    }

    /// The `byok` module-path needle: catches an aliased re-export that names neither
    /// provider trait nor `complete_structured*` directly.
    #[test]
    fn count_identifier_hits_byok_needle() {
        assert_eq!(
            count_identifier_hits(
                "use humaux_adapters::byok::complete_structured_with_bounded_repair as rewrite;",
                "byok",
                true
            ),
            1
        );
        // The current repo's only pre-existing mention is prose in a `//!` doc comment —
        // must not count.
        assert_eq!(
            count_identifier_hits(
                "//! names either type and never imports `humaux_adapters::byok` or anything",
                "byok",
                true
            ),
            0
        );
    }

    /// Minimal, realistic fixture for the four §20.0 online-lane files; `retrieve_extra` is
    /// spliced into `application::retrieve`'s body to carry the injected fault in the tests
    /// below (empty string ⇒ the clean/green baseline).
    fn write_online_lane_fixture(tmp: &Path, retrieve_extra: &str) {
        for d in [
            "crates/application/src",
            "crates/domain/src",
            "crates/adapters/src",
        ] {
            fs::create_dir_all(tmp.join(d)).unwrap();
        }
        fs::write(
            tmp.join("crates/application/src/retrieve.rs"),
            format!(
                "//! application::retrieve — pure orchestration entry point (§20.0).\n\
                 pub enum QueryTransform {{ Deterministic }}\n\
                 pub fn resolve_profile() -> QueryTransform {{ QueryTransform::Deterministic }}\n\
                 {retrieve_extra}"
            ),
        )
        .unwrap();
        fs::write(
            tmp.join("crates/application/src/continuity.rs"),
            "//! application::continuity — placeholder.\n",
        )
        .unwrap();
        fs::write(
            tmp.join("crates/domain/src/context.rs"),
            "//! domain::context — placeholder (§25.4 Mandatory Context Lane).\n",
        )
        .unwrap();
        fs::write(
            tmp.join("crates/adapters/src/retrieve.rs"),
            "//! adapters::retrieve — recall_with_overlay (§15.5) lives here.\n\
             pub fn recall_with_overlay() {}\n",
        )
        .unwrap();
    }

    /// Positive control: the real workspace's online-lane files are clean today.
    #[test]
    fn g20_2_real_repo_is_green() {
        assert_eq!(
            g20_2_no_hidden_generative_recall(&real_root()),
            Verdict::Pass
        );
    }

    #[test]
    fn g20_2_clean_fixture_is_green() {
        let tmp = fresh_tmp("g20-2-green");
        write_online_lane_fixture(&tmp, "");
        assert_eq!(g20_2_no_hidden_generative_recall(&tmp), Verdict::Pass);
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 a: a bare `dyn UserReasoningProvider` reference sneaks into the orchestration
    /// entry point ⇒ static dependency 0→1 ⇒ red, and the offending file is named.
    #[test]
    fn g20_2_fault_user_reasoning_provider_reference_is_red() {
        let tmp = fresh_tmp("g20-2-user");
        write_online_lane_fixture(
            &tmp,
            "fn sneaky(p: &dyn UserReasoningProvider) { let _ = p; }\n",
        );
        match g20_2_no_hidden_generative_recall(&tmp) {
            Verdict::Fail(lines) => assert!(
                lines
                    .iter()
                    .any(|l| l.contains("UserReasoningProvider") && l.contains("retrieve.rs")),
                "{lines:?}"
            ),
            other => panic!("expected Fail, got {other:?}"),
        }
        fs::remove_dir_all(&tmp).ok();
    }

    /// Same shape, `PublicReasoningProvider` half — proves both needles are live, not just
    /// whichever one happens to have a real trait definition in the workspace today.
    #[test]
    fn g20_2_fault_public_reasoning_provider_reference_is_red() {
        let tmp = fresh_tmp("g20-2-public");
        write_online_lane_fixture(
            &tmp,
            "fn sneaky(p: &dyn PublicReasoningProvider) { let _ = p; }\n",
        );
        match g20_2_no_hidden_generative_recall(&tmp) {
            Verdict::Fail(lines) => assert!(
                lines.iter().any(|l| l.contains("PublicReasoningProvider")),
                "{lines:?}"
            ),
            other => panic!("expected Fail, got {other:?}"),
        }
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 (task item 3, decisive acceptance): insert one `complete_structured()` generative
    /// query rewrite call ahead of recall ⇒ static dependency 0→1 on both the trait name and
    /// the method name ⇒ red. This is the exact injection the spec's own G20-2/G80-39 text
    /// names ("在 recall 前插一个 complete_structured() query rewrite").
    #[test]
    fn g20_2_fault_complete_structured_call_before_recall_is_red() {
        let tmp = fresh_tmp("g20-2-rewrite");
        write_online_lane_fixture(
            &tmp,
            "async fn recall(provider: &dyn UserReasoningProvider) {\n    \
             let _rewritten = provider.complete_structured().await;\n    \
             // then the real deterministic recall would run\n\
             }\n",
        );
        match g20_2_no_hidden_generative_recall(&tmp) {
            Verdict::Fail(lines) => {
                assert!(
                    lines.iter().any(|l| l.contains("complete_structured")),
                    "{lines:?}"
                );
                assert!(
                    lines.iter().any(|l| l.contains("UserReasoningProvider")),
                    "{lines:?}"
                );
            }
            other => panic!("expected Fail, got {other:?}"),
        }
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 (real-world repro that exposed the blocker this fix closes): the aliased-import
    /// shape from the spec's own `byok.rs` — `complete_structured_with_bounded_repair` reached
    /// through a `use ... as rewrite;` re-export, called from a `pub` fn that never spells out
    /// `UserReasoningProvider` at the call site at all. Under the pre-fix matcher (full-
    /// identifier `complete_structured` + no `byok` needle) this fixture stayed
    /// `Verdict::Pass`; it must now be red on both needles.
    #[test]
    fn g20_2_fault_bounded_repair_via_byok_alias_is_red() {
        let tmp = fresh_tmp("g20-2-byok-alias");
        write_online_lane_fixture(
            &tmp,
            "use humaux_adapters::byok::complete_structured_with_bounded_repair as rewrite;\n\
             pub async fn sneaky_rewrite() {\n    \
             let _ = rewrite(Default::default()).await;\n\
             }\n",
        );
        match g20_2_no_hidden_generative_recall(&tmp) {
            Verdict::Fail(lines) => {
                assert!(
                    lines.iter().any(|l| l.contains("complete_structured")),
                    "{lines:?}"
                );
                assert!(lines.iter().any(|l| l.contains("byok")), "{lines:?}");
            }
            other => panic!("expected Fail, got {other:?}"),
        }
        fs::remove_dir_all(&tmp).ok();
    }

    /// A scan domain where every listed file is either absent or a bare T0.x placeholder must
    /// report `NotApplicable`, not a vacuous `Pass` — there is no real call graph to have
    /// cleared the rule against yet (see [`g20_2_no_hidden_generative_recall`]'s doc).
    #[test]
    fn g20_2_all_placeholder_scan_domain_is_not_applicable() {
        let tmp = fresh_tmp("g20-2-na");
        for d in [
            "crates/application/src",
            "crates/domain/src",
            "crates/adapters/src",
        ] {
            fs::create_dir_all(tmp.join(d)).unwrap();
        }
        for rel in [
            "crates/application/src/retrieve.rs",
            "crates/application/src/continuity.rs",
            "crates/domain/src/context.rs",
            "crates/adapters/src/retrieve.rs",
        ] {
            fs::write(tmp.join(rel), "//! 占位模块（T0.x 任务填充）。\n").unwrap();
        }
        match g20_2_no_hidden_generative_recall(&tmp) {
            Verdict::NotApplicable(missing) => {
                assert!(missing.contains("retrieve.rs"), "{missing}");
            }
            other => panic!("expected NotApplicable, got {other:?}"),
        }
        fs::remove_dir_all(&tmp).ok();
    }

    // -- §11.8 / §22.5: classify() sole-construction-point sites, path-qualified ----------

    /// Positive control against the real repo: both known sites have landed
    /// (`domain::consolidate::classify` from an earlier wave, `retrieval::completeness::
    /// classify` from this task) and the workspace-wide total matches — no stray third
    /// `fn classify(` exists anywhere else.
    #[test]
    fn classify_sole_construction_passes_on_real_repo() {
        assert_eq!(
            g11_8_classify_sole_construction_point(&real_root()),
            Verdict::Pass
        );
    }

    fn write_classify_sites_fixture(
        tmp: &Path,
        consolidate_count: usize,
        completeness_count: usize,
    ) {
        let domain_dir = tmp.join("crates/domain/src");
        let retrieval_dir = tmp.join("crates/retrieval/src");
        fs::create_dir_all(&domain_dir).unwrap();
        fs::create_dir_all(&retrieval_dir).unwrap();
        let mut consolidate_src = String::new();
        for i in 0..consolidate_count {
            consolidate_src.push_str(&format!("pub fn classify(id: u64) -> u64 {{ id + {i} }}\n"));
        }
        let mut completeness_src = String::new();
        for i in 0..completeness_count {
            completeness_src.push_str(&format!(
                "pub(crate) fn classify(x: u64) -> u64 {{ x + {i} }}\n"
            ));
        }
        fs::write(domain_dir.join("consolidate.rs"), consolidate_src).unwrap();
        fs::write(retrieval_dir.join("completeness.rs"), completeness_src).unwrap();
    }

    #[test]
    fn classify_sole_construction_passes_with_exactly_one_each() {
        let tmp = fresh_tmp("g11-8-green");
        write_classify_sites_fixture(&tmp, 1, 1);
        assert_eq!(g11_8_classify_sole_construction_point(&tmp), Verdict::Pass);
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 (this task's own finding, decisive acceptance): a second `fn classify(` landing
    /// in `retrieval::completeness.rs` alongside the first (e.g. a copy-paste duplicate) ⇒
    /// that file's own count is 2 != 1 ⇒ red. This is exactly the regression a bare
    /// workspace-wide `== 1` could no longer detect once two legitimate sites coexist — the
    /// per-path assertion is what makes it observable again.
    #[test]
    fn classify_sole_construction_fault_duplicate_in_completeness_is_red() {
        let tmp = fresh_tmp("g11-8-red-duplicate");
        write_classify_sites_fixture(&tmp, 1, 2);
        match g11_8_classify_sole_construction_point(&tmp) {
            Verdict::Fail(lines) => assert!(
                lines
                    .iter()
                    .any(|l| l.contains("completeness.rs") && l.contains("found 2")),
                "{lines:?}"
            ),
            other => panic!("expected Fail, got {other:?}"),
        }
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 (stray-site regression): a third, unlisted `fn classify(` appearing anywhere else
    /// in the workspace must still be red even though both known sites individually hold
    /// exactly 1 — this is the cross-check the workspace-wide total/`applicable` comparison
    /// exists for.
    #[test]
    fn classify_sole_construction_fault_stray_third_site_is_red() {
        let tmp = fresh_tmp("g11-8-red-stray");
        write_classify_sites_fixture(&tmp, 1, 1);
        let stray_dir = tmp.join("crates/retrieval/src");
        fs::write(
            stray_dir.join("stray.rs"),
            "pub fn classify(x: u64) -> u64 { x }\n",
        )
        .unwrap();
        match g11_8_classify_sole_construction_point(&tmp) {
            Verdict::Fail(lines) => assert!(
                lines.iter().any(|l| l.contains("workspace-wide")),
                "{lines:?}"
            ),
            other => panic!("expected Fail, got {other:?}"),
        }
        fs::remove_dir_all(&tmp).ok();
    }

    // -- §23.1② G23-1: LedgerCounts field set is exactly 6 --------------------------------

    /// Positive control against the real repo: `ledger::close` has landed (this task), and
    /// `LedgerCounts`'s six fields must match §23.1②'s frozen set exactly.
    #[test]
    fn ledger_counts_six_fields_passes_on_real_repo() {
        assert_eq!(
            g23_1_ledger_counts_exactly_six_fields(&real_root()),
            Verdict::Pass
        );
    }

    fn write_ledger_counts_fixture(tmp: &Path, body: &str) {
        let dir = tmp.join("crates/retrieval/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("completeness.rs"),
            format!("pub struct LedgerCounts {{\n{body}}}\n"),
        )
        .unwrap();
    }

    #[test]
    fn ledger_counts_six_fields_passes_with_exactly_six() {
        let tmp = fresh_tmp("g23-1-green");
        write_ledger_counts_fixture(
            &tmp,
            "    expected: u64,\n    done: u64,\n    deleted: u64,\n    skipped: u64,\n    \
             open_gaps: u64,\n    pending: u64,\n",
        );
        assert_eq!(g23_1_ledger_counts_exactly_six_fields(&tmp), Verdict::Pass);
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 (decisive acceptance, §23.1②'s own worked example): adding a 7th `visible: u64`
    /// field ⇒ `7 != 6` ⇒ red. This is the exact regression §23.1② names as the one that
    /// would let G23-2's two injections stop being observable (numerator folded back into the
    /// ledger struct, all counts taken from PostgreSQL only).
    #[test]
    fn ledger_counts_fault_seven_fields_with_visible_is_red() {
        let tmp = fresh_tmp("g23-1-red");
        write_ledger_counts_fixture(
            &tmp,
            "    expected: u64,\n    done: u64,\n    deleted: u64,\n    skipped: u64,\n    \
             open_gaps: u64,\n    pending: u64,\n    visible: u64,\n",
        );
        match g23_1_ledger_counts_exactly_six_fields(&tmp) {
            Verdict::Fail(lines) => {
                assert!(lines.iter().any(|l| l.contains('7')), "{lines:?}");
                assert!(lines.iter().any(|l| l.contains("visible")), "{lines:?}");
            }
            other => panic!("expected Fail, got {other:?}"),
        }
        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn ledger_counts_not_applicable_when_type_absent() {
        let tmp = fresh_tmp("g23-1-na");
        fs::create_dir_all(&tmp).unwrap();
        assert_eq!(
            g23_1_ledger_counts_exactly_six_fields(&tmp),
            Verdict::NotApplicable(
                "retrieval::completeness::LedgerCounts (§22.5, ledger::close 尚未交付)".to_string()
            )
        );
        fs::remove_dir_all(&tmp).ok();
    }
    /// §22.5 A1 闸：真仓库必须绿（算式只在 domain::ledger 一处）。
    #[test]
    fn g22_5_a1_real_repo_is_green() {
        assert_eq!(g22_5_a1_sole_implementation(&real_root()), Verdict::Pass);
    }

    /// 注错 a：把算式抄回第二个 crate ⇒ 红并点名文件。
    #[test]
    fn g22_5_a1_fault_second_copy_elsewhere_is_red() {
        let tmp = fresh_tmp("a1-second-copy");
        write_a1_fixture(&tmp, "pub fn a1_holds(e: u64) -> bool { e == e }");
        fs::create_dir_all(tmp.join("crates/projection/src")).unwrap();
        fs::write(
            tmp.join("crates/projection/src/stream.rs"),
            "fn advance() -> bool { expected == done + open_gaps + pending }\n",
        )
        .unwrap();
        match g22_5_a1_sole_implementation(&tmp) {
            Verdict::Fail(lines) => {
                assert!(
                    lines
                        .iter()
                        .any(|l| l.contains("crates/projection/src/stream.rs"))
                )
            }
            other => panic!("expected Fail, got {other:?}"),
        }
    }

    /// 注错 b：算式的家被掏空（定义没了）⇒ 哨兵必须红，否则整道闸静默失明。
    #[test]
    fn g22_5_a1_fault_dead_home_sentinel_is_red() {
        let tmp = fresh_tmp("a1-dead-home");
        write_a1_fixture(&tmp, "// the judgment used to live here\n");
        match g22_5_a1_sole_implementation(&tmp) {
            Verdict::Fail(lines) => assert!(lines.iter().any(|l| l.contains("sentinel"))),
            other => panic!("expected Fail, got {other:?}"),
        }
    }

    /// 注错 c：家里改名（`a1_holds_renamed`）——子串仍含 `a1_holds`，
    /// 没有词边界的匹配会漏掉它。本闸第一版正是这么漏的。
    #[test]
    fn g22_5_a1_fault_renamed_home_fn_is_red() {
        let tmp = fresh_tmp("a1-renamed-home");
        write_a1_fixture(&tmp, "pub fn a1_holds_renamed(e: u64) -> bool { e == e }");
        match g22_5_a1_sole_implementation(&tmp) {
            Verdict::Fail(lines) => assert!(lines.iter().any(|l| l.contains("sentinel"))),
            other => panic!("expected Fail (word-boundary), got {other:?}"),
        }
    }

    fn write_a1_fixture(tmp: &Path, home_body: &str) {
        fs::create_dir_all(tmp.join("crates/domain/src")).unwrap();
        fs::write(tmp.join("crates/domain/src/ledger.rs"), home_body).unwrap();
    }

    // ========================================================================================
    // §19 Provider Plane Architecture Gate — 7 checks
    // ========================================================================================

    /// Positive/negative pin for [`provider_plane_dashscope_sdk_hits`]'s own core claim: the
    /// bare business-data string `"dashscope"` (a `ProviderId` value, legitimate everywhere)
    /// must NOT count, while the real SDK-shape needles (PascalCase type name, snake_case
    /// name fragment, literal API host) must.
    #[test]
    fn provider_plane_dashscope_sdk_hits_distinguishes_data_from_sdk_surface() {
        assert_eq!(
            provider_plane_dashscope_sdk_hits("let p = ProviderId(\"dashscope\".into());"),
            0,
            "the bare provider_id string value must not count as SDK-surface"
        );
        assert_eq!(
            provider_plane_dashscope_sdk_hits("pub struct DashscopeEmbeddingProvider {}"),
            1
        );
        assert_eq!(
            provider_plane_dashscope_sdk_hits("pub fn map_dashscope_status(s: u16) {}"),
            1
        );
        assert_eq!(
            provider_plane_dashscope_sdk_hits(
                "const URL: &str = \"https://dashscope.aliyuncs.com/v1\";"
            ),
            1
        );
        assert_eq!(
            provider_plane_dashscope_sdk_hits("// mentions Dashscope only in a comment"),
            0,
            "comment lines are skipped, same discipline as count_identifier_hits"
        );
        // §19 review: the original three-needle set scored 0 on exactly this shape — a Rust
        // path import of the SDK crate — despite it being the rule's own named example
        // ("Domain does not import DashScope SDK").
        assert_eq!(
            provider_plane_dashscope_sdk_hits("use dashscope::Client;"),
            1,
            "a bare `use dashscope::...` import must count as an SDK hit"
        );
        assert_eq!(
            provider_plane_dashscope_sdk_hits("dashscope::embeddings::create(&req).await?;"),
            1
        );
        assert_eq!(
            provider_plane_dashscope_sdk_hits("let p = ProviderId(\"dashscope\".into());"),
            0,
            "the `dashscope::` needle must still not collide with the bare business-data \
             string — no `::` follows a string literal's contents"
        );
    }

    // -- Gate 1/7: Domain does not import DashScope SDK -------------------------------------

    #[test]
    fn provider_plane_gate1_real_repo_is_clean() {
        assert_eq!(
            provider_plane_gate1_domain_no_dashscope_sdk(&real_root()),
            Verdict::Pass
        );
    }

    /// 注错: inject a `Dashscope`-named type into `crates/domain/src/` → red.
    #[test]
    fn provider_plane_gate1_fault_dashscope_type_in_domain_is_red() {
        let tmp = fresh_tmp("pp-gate1-fault");
        let dir = tmp.join("crates/domain/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("leak.rs"),
            "pub struct DashscopeEmbeddingProvider;\n",
        )
        .unwrap();
        assert!(matches!(
            provider_plane_gate1_domain_no_dashscope_sdk(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 (§19 review): a bare `use dashscope::...` import — the rule's own named example,
    /// and the shape the original three-needle set missed entirely (0 hits) — must also be
    /// caught.
    #[test]
    fn provider_plane_gate1_fault_dashscope_path_import_in_domain_is_red() {
        let tmp = fresh_tmp("pp-gate1-fault-import");
        let dir = tmp.join("crates/domain/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("leak.rs"), "use dashscope::Client;\n").unwrap();
        assert!(matches!(
            provider_plane_gate1_domain_no_dashscope_sdk(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 正向对照: the same domain dir with only the legitimate business-data string is green —
    /// proves the fault above is the SDK-shape needle, not any mention of the word.
    #[test]
    fn provider_plane_gate1_business_data_string_alone_is_green() {
        let tmp = fresh_tmp("pp-gate1-green");
        let dir = tmp.join("crates/domain/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("ok.rs"), "let provider = \"dashscope\";\n").unwrap();
        assert_eq!(
            provider_plane_gate1_domain_no_dashscope_sdk(&tmp),
            Verdict::Pass
        );
        fs::remove_dir_all(&tmp).ok();
    }

    // -- Gate 2/7: Application does not call DashScope directly -----------------------------

    #[test]
    fn provider_plane_gate2_real_repo_is_clean() {
        assert_eq!(
            provider_plane_gate2_application_no_dashscope_direct(&real_root()),
            Verdict::Pass
        );
    }

    /// 注错: inject a direct DashScope call into `crates/application/src/` → red.
    #[test]
    fn provider_plane_gate2_fault_dashscope_call_in_application_is_red() {
        let tmp = fresh_tmp("pp-gate2-fault");
        let dir = tmp.join("crates/application/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("leak.rs"),
            "fn go() { let _ = map_dashscope_status(200); }\n",
        )
        .unwrap();
        assert!(matches!(
            provider_plane_gate2_application_no_dashscope_direct(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    // -- Gate 3/7: Only retrieval-provider/adapters may import provider client --------------

    #[test]
    fn provider_plane_gate3_real_repo_is_clean() {
        assert_eq!(
            provider_plane_gate3_only_adapters_import_provider_client(&real_root()),
            Verdict::Pass
        );
    }

    /// 注错: a second importer of the DashScope SDK surface outside retrieval-provider/
    /// adapters (here: `crates/retrieval-provider/src/router.rs`, a sibling module in the
    /// *same* crate — proves the home is the `adapters.rs` file, not the whole crate) → red.
    #[test]
    fn provider_plane_gate3_fault_second_importer_outside_adapters_is_red() {
        let tmp = fresh_tmp("pp-gate3-fault");
        let dir = tmp.join("crates/retrieval-provider/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("router.rs"),
            "pub struct DashscopeEmbeddingProvider;\n",
        )
        .unwrap();
        assert!(matches!(
            provider_plane_gate3_only_adapters_import_provider_client(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 正向对照: the identical SDK-shape identifier inside the real home
    /// (`crates/retrieval-provider/src/adapters.rs`) is green — proves the fault above fires
    /// on *location*, not on the identifier's mere presence anywhere.
    #[test]
    fn provider_plane_gate3_same_identifier_inside_home_is_green() {
        let tmp = fresh_tmp("pp-gate3-green");
        let dir = tmp.join("crates/retrieval-provider/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("adapters.rs"),
            "pub struct DashscopeEmbeddingProvider;\n",
        )
        .unwrap();
        assert_eq!(
            provider_plane_gate3_only_adapters_import_provider_client(&tmp),
            Verdict::Pass
        );
        fs::remove_dir_all(&tmp).ok();
    }

    /// 正向对照: the adapter's own `tests/` directory is exempt too (§19's "adapters" reading
    /// includes the module's own test surface, module doc).
    #[test]
    fn provider_plane_gate3_own_tests_dir_is_exempt() {
        let tmp = fresh_tmp("pp-gate3-tests-green");
        let dir = tmp.join("crates/retrieval-provider/tests");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("dashscope_live_smoke.rs"),
            "pub struct DashscopeEmbeddingProvider;\n",
        )
        .unwrap();
        assert_eq!(
            provider_plane_gate3_only_adapters_import_provider_client(&tmp),
            Verdict::Pass
        );
        fs::remove_dir_all(&tmp).ok();
    }

    // -- Gates 4-6/7: shared companion-check helper ------------------------------------------

    /// `body` is prefixed with an `ExternalCall` mention — matching the real adapter's own
    /// `use humaux_domain::egress::{ExternalCall, ...}` — since [`provider_plane_external_call_sites`]
    /// now scopes its `.call(`/`::call(` scan to files that name the trait (§19 review: no
    /// longer keyed on the one field name `self.transport`).
    fn write_adapter_home(tmp: &Path, body: &str) {
        let dir = tmp.join("crates/retrieval-provider/src");
        fs::create_dir_all(&dir).unwrap();
        let full = format!("use humaux_domain::egress::ExternalCall;\n{body}");
        fs::write(dir.join("adapters.rs"), full).unwrap();
    }

    #[test]
    fn provider_plane_gate4_real_repo_egress_is_wired() {
        // Unlike 5/7 and 6/7, this one has a real, already-wired target (T7.1's adapter) —
        // pin it as a real `Pass`, not `NotApplicable`, so a regression that un-wires
        // `egress::authorize` from the real call site is caught, not swallowed.
        assert_eq!(
            provider_plane_gate4_egress_policy_before_external_call(&real_root()),
            Verdict::Pass
        );
    }

    /// §19 review: a real, non-placeholder home with the `ExternalCall` trait named but zero
    /// `.call(`/`::call(` hits ⇒ `NotApplicable` (needle found nothing to require), never the
    /// old vacuous `Pass` a caller could not distinguish from "needle drifted and missed a
    /// real call site".
    #[test]
    fn provider_plane_gate456_no_call_site_is_not_applicable() {
        let tmp = fresh_tmp("pp-gate456-no-call-site");
        write_adapter_home(&tmp, "pub struct Real;\nfn placeholder_only() {}\n");
        for check in [
            provider_plane_gate4_egress_policy_before_external_call,
            provider_plane_gate5_ledger_entry_per_external_call,
            provider_plane_gate6_admission_participation,
        ] {
            assert!(matches!(check(&tmp), Verdict::NotApplicable(_)));
        }
        fs::remove_dir_all(&tmp).ok();
    }

    /// §19 review: renaming the field the call is made through (`self.transport` →
    /// `self.http_client`) must not fail the call-site scan open — the old needle
    /// (`".transport.call("`) depended on that exact field name.
    #[test]
    fn provider_plane_gate4_call_site_detected_through_a_renamed_field() {
        let tmp = fresh_tmp("pp-gate4-renamed-field");
        write_adapter_home(
            &tmp,
            "fn embed() {\n    let permit = egress::authorize(x, y, z)?;\n    self.http_client.call(&permit, &payload);\n}\n",
        );
        assert_eq!(
            provider_plane_gate4_egress_policy_before_external_call(&tmp),
            Verdict::Pass
        );
        fs::remove_dir_all(&tmp).ok();
    }

    /// §19 review: a second file in the crate (not `adapters.rs`) carrying its own external
    /// call site must be scanned too, not silently skipped.
    #[test]
    fn provider_plane_gate4_call_site_in_a_second_crate_file_is_scanned() {
        let tmp = fresh_tmp("pp-gate4-second-file");
        let dir = tmp.join("crates/retrieval-provider/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("rerank_adapter.rs"),
            "use humaux_domain::egress::ExternalCall;\nfn rerank() {\n    let permit = egress::authorize(x, y, z)?;\n    self.transport.call(&permit, &payload);\n}\n",
        )
        .unwrap();
        assert_eq!(
            provider_plane_gate4_egress_policy_before_external_call(&tmp),
            Verdict::Pass
        );
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错: absent adapters.rs ⇒ `NotApplicable`, not a vacuous `Pass`.
    #[test]
    fn provider_plane_gate456_absent_home_is_not_applicable() {
        let tmp = fresh_tmp("pp-gate456-absent");
        fs::create_dir_all(&tmp).unwrap();
        for check in [
            provider_plane_gate4_egress_policy_before_external_call,
            provider_plane_gate5_ledger_entry_per_external_call,
            provider_plane_gate6_admission_participation,
        ] {
            assert!(matches!(check(&tmp), Verdict::NotApplicable(_)));
        }
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错: a T0.x placeholder home ⇒ `NotApplicable`, not `Pass` (no real call graph to
    /// judge yet — same precondition [`g20_2_no_hidden_generative_recall`] applies).
    #[test]
    fn provider_plane_gate456_placeholder_home_is_not_applicable() {
        let tmp = fresh_tmp("pp-gate456-placeholder");
        write_adapter_home(&tmp, "//! retrieval-provider::adapters — 占位模块\n");
        for check in [
            provider_plane_gate4_egress_policy_before_external_call,
            provider_plane_gate5_ledger_entry_per_external_call,
            provider_plane_gate6_admission_participation,
        ] {
            assert!(matches!(check(&tmp), Verdict::NotApplicable(_)));
        }
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错: a real (non-placeholder) home with a call site but zero companion evidence at
    /// all ⇒ `NotApplicable` (honest "not yet wired"), never a silent `Pass`.
    #[test]
    fn provider_plane_gate4_fault_call_site_without_authorize_is_not_applicable() {
        let tmp = fresh_tmp("pp-gate4-fault");
        write_adapter_home(
            &tmp,
            "pub struct Real;\nfn embed() { self.transport.call(&permit, &payload); }\n",
        );
        assert!(matches!(
            provider_plane_gate4_egress_policy_before_external_call(&tmp),
            Verdict::NotApplicable(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 正向对照 (红转绿): the identical fixture, with a real (non-comment) `authorize(` call
    /// added, goes green.
    #[test]
    fn provider_plane_gate4_fault_fixed_by_adding_authorize_call() {
        let tmp = fresh_tmp("pp-gate4-fixed");
        write_adapter_home(
            &tmp,
            "pub struct Real;\nfn embed() {\n    let permit = egress::authorize(x, y, z)?;\n    self.transport.call(&permit, &payload);\n}\n",
        );
        assert_eq!(
            provider_plane_gate4_egress_policy_before_external_call(&tmp),
            Verdict::Pass
        );
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 (伪修复 shape): the companion is named only in a comment, never in real code ⇒
    /// `Fail`, not `NotApplicable` and not `Pass` — this is the reachable red state 4/5/6
    /// each need (§80.1 "一道闸没有注错红转绿记录就不算存在").
    #[test]
    fn provider_plane_gate5_fault_ledger_mentioned_only_in_comment_is_red() {
        let tmp = fresh_tmp("pp-gate5-fault-comment");
        write_adapter_home(
            &tmp,
            "// TODO: write a ModelCallLedger entry here eventually\nfn embed() { self.transport.call(&permit, &payload); }\n",
        );
        assert!(matches!(
            provider_plane_gate5_ledger_entry_per_external_call(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 正向对照 (红转绿): the identical fixture with a real (non-comment) ModelCallLedger
    /// reference goes green.
    #[test]
    fn provider_plane_gate5_fixed_by_real_ledger_reference() {
        let tmp = fresh_tmp("pp-gate5-fixed");
        write_adapter_home(
            &tmp,
            "fn embed() {\n    model_call_ledger::reserve_call(&pool, &req)?;\n    self.transport.call(&permit, &payload);\n}\n",
        );
        assert_eq!(
            provider_plane_gate5_ledger_entry_per_external_call(&tmp),
            Verdict::Pass
        );
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 (§19 review): a trailing `//` comment on a real code line was previously counted
    /// as a real hit (`count_code_line_hits` only skipped *leading* `//` lines) — must not be.
    #[test]
    fn provider_plane_gate5_fault_ledger_mentioned_only_in_trailing_comment_is_red() {
        let tmp = fresh_tmp("pp-gate5-fault-trailing-comment");
        write_adapter_home(
            &tmp,
            "fn embed() { self.transport.call(&permit, &payload); } // model_call_ledger later\n",
        );
        assert!(matches!(
            provider_plane_gate5_ledger_entry_per_external_call(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 (§19 review): same shape, inside a `/* */` block comment.
    #[test]
    fn provider_plane_gate5_fault_ledger_mentioned_only_in_block_comment_is_red() {
        let tmp = fresh_tmp("pp-gate5-fault-block-comment");
        write_adapter_home(
            &tmp,
            "/* model_call_ledger wiring TODO */\nfn embed() { self.transport.call(&permit, &payload); }\n",
        );
        assert!(matches!(
            provider_plane_gate5_ledger_entry_per_external_call(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn provider_plane_gate5_real_repo_is_wired() {
        // `adapters.rs::embed` now calls `model_call_ledger::reserve_call`/`finalize_call`
        // around its external call — pin flipped from `NotApplicable` to `Pass` (this test's
        // own doc comment did exactly what it said: it started failing when the wiring
        // landed, telling this agent to update the pin instead of the gate silently staying
        // green either way).
        assert_eq!(
            provider_plane_gate5_ledger_entry_per_external_call(&real_root()),
            Verdict::Pass
        );
    }

    /// Same 伪修复 shape as gate 5/7, for admission control.
    #[test]
    fn provider_plane_gate6_fault_admission_mentioned_only_in_comment_is_red() {
        let tmp = fresh_tmp("pp-gate6-fault-comment");
        write_adapter_home(
            &tmp,
            "// should call admission::decide( before sending\nfn embed() { self.transport.call(&permit, &payload); }\n",
        );
        assert!(matches!(
            provider_plane_gate6_admission_participation(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 正向对照 (红转绿): a real (non-comment) `admission::decide(` call goes green.
    #[test]
    fn provider_plane_gate6_fixed_by_real_admission_call() {
        let tmp = fresh_tmp("pp-gate6-fixed");
        write_adapter_home(
            &tmp,
            "fn embed() {\n    let d = admission::decide(&req, &budgets);\n    self.transport.call(&permit, &payload);\n}\n",
        );
        assert_eq!(
            provider_plane_gate6_admission_participation(&tmp),
            Verdict::Pass
        );
        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn provider_plane_gate6_real_repo_is_wired() {
        // `adapters.rs::admission_gate` now calls `admission::decide`/builds
        // `admission::AdmissionRequest` around the external call — pin flipped from
        // `NotApplicable` to `Pass`, same reasoning as gate 5/7's pin above.
        assert_eq!(
            provider_plane_gate6_admission_participation(&real_root()),
            Verdict::Pass
        );
    }

    // -- §19 Embedding Provider Failover 硬规则: decide_embedding_failover sole decision point -

    #[test]
    fn embedding_failover_sole_decision_point_check_passes_on_empty() {
        assert_eq!(
            embedding_failover_sole_decision_point_check(&[]),
            Verdict::Pass
        );
    }

    #[test]
    fn embedding_failover_sole_decision_point_check_fails_on_a_hit() {
        let hits = vec!["crates/retrieval-provider/src/router.rs".to_string()];
        assert!(matches!(
            embedding_failover_sole_decision_point_check(&hits),
            Verdict::Fail(_)
        ));
    }

    /// 注错 (红转绿): a fixture defining `ProjectionRef` without delegating to
    /// `decide_embedding_failover` is red.
    #[test]
    fn provider_plane_embedding_failover_fault_hand_rolled_comparison_is_red() {
        let tmp = fresh_tmp("pp-failover-fault");
        let dir = tmp.join("crates/retrieval-provider/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("router.rs"),
            "pub struct ProjectionRef { pub provider_id: String }\nfn projection_compatible() {}\n",
        )
        .unwrap();
        assert!(matches!(
            provider_plane_embedding_failover_sole_decision_point(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 正向对照 (红转绿): the identical fixture, with a real call to
    /// `decide_embedding_failover(` added, goes green.
    #[test]
    fn provider_plane_embedding_failover_fixed_by_delegating() {
        let tmp = fresh_tmp("pp-failover-fixed");
        let dir = tmp.join("crates/retrieval-provider/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("router.rs"),
            "pub struct ProjectionRef { pub provider_id: String }\nfn projection_compatible() {\n    failover::decide_embedding_failover(a, b, c, d)\n}\n",
        )
        .unwrap();
        assert_eq!(
            provider_plane_embedding_failover_sole_decision_point(&tmp),
            Verdict::Pass
        );
        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn provider_plane_embedding_failover_real_repo_is_green() {
        // Was an honest red pin (repo CLAUDE.md 交付准则「拒绝伪修复」): `router.rs::
        // ProjectionRef`/`projection_compatible` used to hand-roll the identical
        // provider_id/model_id/dimension comparison inline instead of calling through
        // `failover`. Fixed by extracting `failover::projection_contracts_compatible` (the
        // permit-free half of `decide_embedding_failover`'s own comparison) and having
        // `router::projection_compatible` call it — see this section's own module comment for
        // why `router.rs` cannot instead call the permit-gated `decide_embedding_failover`
        // directly. Both call names are the same one decision point; this pin now proves the
        // real repo actually routes through one of them, not documents a known gap.
        assert_eq!(
            provider_plane_embedding_failover_sole_decision_point(&real_root()),
            Verdict::Pass
        );
    }

    // -- §19 DOD-028 (a): EgressSecret::expose sole call site --------------------------------

    #[test]
    fn g_egress_secret_expose_sole_call_site_passes_on_real_repo() {
        assert_eq!(
            g_egress_secret_expose_sole_call_site(&real_root()),
            Verdict::Pass
        );
    }

    /// 注错 (红转绿): a second file that both names `EgressSecret` and calls `.expose()` on one
    /// — the exact "credential read outside the one sanctioned call site" shape this check
    /// exists to catch — is red.
    #[test]
    fn g_egress_secret_expose_fault_second_call_site_is_red() {
        let tmp = fresh_tmp("egress-secret-fault");
        let dir = tmp.join("crates/some-other-crate/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("lib.rs"),
            "use humaux_infra_egress::http::EgressSecret;\nfn leak(s: &EgressSecret) -> &str { s.expose() }\n",
        )
        .unwrap();
        assert!(matches!(
            g_egress_secret_expose_sole_call_site(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 正向对照: a file that calls some *other* type's `.expose()` (never naming
    /// `EgressSecret`) must not be flagged — this is exactly `byok.rs`'s unrelated
    /// `PlaintextApiKey::expose` shape.
    #[test]
    fn g_egress_secret_expose_ignores_unrelated_expose_methods() {
        let tmp = fresh_tmp("egress-secret-unrelated");
        let dir = tmp.join("crates/some-other-crate/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("lib.rs"),
            "struct PlaintextApiKey(String);\nimpl PlaintextApiKey {\n    fn expose(&self) -> &str { &self.0 }\n}\n",
        )
        .unwrap();
        assert_eq!(g_egress_secret_expose_sole_call_site(&tmp), Verdict::Pass);
        fs::remove_dir_all(&tmp).ok();
    }

    // -- §19 DOD-028 (b): retrieval credential purpose guard exists --------------------------

    #[test]
    fn g_retrieval_credential_purpose_guard_exists_passes_on_real_repo() {
        assert_eq!(
            g_retrieval_credential_purpose_guard_exists(&real_root()),
            Verdict::Pass
        );
    }

    /// 注错 (红转绿): a fixture standing in for `http.rs` with the purpose guard's needle
    /// text absent — proving this check would have caught finding 1 before it was fixed.
    #[test]
    fn g_retrieval_credential_purpose_guard_fault_missing_guard_is_red() {
        let tmp = fresh_tmp("purpose-guard-fault");
        let dir = tmp.join("crates/infra-egress/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("http.rs"),
            "async fn call() {\n    // no purpose guard here\n}\n",
        )
        .unwrap();
        assert!(matches!(
            g_retrieval_credential_purpose_guard_exists(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 正向对照: the identical fixture with the guard's needle text present goes green.
    #[test]
    fn g_retrieval_credential_purpose_guard_fixed_by_adding_guard() {
        let tmp = fresh_tmp("purpose-guard-fixed");
        let dir = tmp.join("crates/infra-egress/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("http.rs"),
            "async fn call() {\n    if !matches!(permit.purpose(), PrivateDataPurpose::RetrievalEmbedding | PrivateDataPurpose::RetrievalRerank) {\n        return Err(ErrorCode::Forbidden);\n    }\n}\n",
        )
        .unwrap();
        assert_eq!(
            g_retrieval_credential_purpose_guard_exists(&tmp),
            Verdict::Pass
        );
        fs::remove_dir_all(&tmp).ok();
    }

    // -- Gate 7/7: Embedding projection write contains provider/model/version metadata -------

    #[test]
    fn provider_plane_gate7_real_repo_is_honestly_not_applicable() {
        // §19 review: the only `INSERT INTO private.processing_runs` in the workspace today is
        // `crates/adapters/tests/processing_runs_fingerprint_rerun.rs` — a test fixture, now
        // excluded from evidence (module doc above). Pinned so a future real (non-`tests/`)
        // write path landing is *noticed* (this test starts failing, telling the next agent to
        // flip the pin to `Pass`) instead of the gate silently staying green on a test double.
        assert!(matches!(
            provider_plane_gate7_projection_write_has_model_metadata(&real_root()),
            Verdict::NotApplicable(_)
        ));
    }

    #[test]
    fn insert_statement_text_stops_at_the_string_literals_closing_quote() {
        let source = "let q = \"INSERT INTO private.processing_runs (model_id) VALUES ($1)\"; let unrelated = \"model_provider model_revision\";";
        let idx = source.find("INSERT INTO").unwrap();
        let stmt = insert_statement_text(source, idx);
        assert!(stmt.contains("model_id"));
        assert!(
            !stmt.contains("model_provider"),
            "the metadata check must not see text after the statement's own closing quote"
        );
    }

    /// 注错: a real `INSERT INTO private.processing_runs` call site missing the metadata
    /// columns ⇒ red.
    #[test]
    fn provider_plane_gate7_fault_insert_missing_model_metadata_is_red() {
        let tmp = fresh_tmp("pp-gate7-fault");
        let dir = tmp.join("crates/adapters/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("write.rs"),
            "let q = \"INSERT INTO private.processing_runs (id) VALUES ($1)\";\n",
        )
        .unwrap();
        assert!(matches!(
            provider_plane_gate7_projection_write_has_model_metadata(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 正向对照 (红转绿): the identical insert site with all three metadata columns present
    /// goes green.
    #[test]
    fn provider_plane_gate7_fixed_by_including_all_three_metadata_columns() {
        let tmp = fresh_tmp("pp-gate7-fixed");
        let dir = tmp.join("crates/adapters/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("write.rs"),
            "let q = \"INSERT INTO private.processing_runs (model_provider, model_id, model_revision) VALUES ($1, $2, $3)\";\n",
        )
        .unwrap();
        assert_eq!(
            provider_plane_gate7_projection_write_has_model_metadata(&tmp),
            Verdict::Pass
        );
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 (§19 review, secondary weakness): the INSERT statement itself omits
    /// `model_provider`, but the same file mentions it elsewhere (an unrelated struct field) —
    /// must still be `Fail`, not swallowed into a false `Pass` by a whole-file `.contains`.
    #[test]
    fn provider_plane_gate7_fault_metadata_mentioned_only_outside_the_statement_is_red() {
        let tmp = fresh_tmp("pp-gate7-fault-outside-statement");
        let dir = tmp.join("crates/adapters/src");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("write.rs"),
            "struct Row { model_provider: String }\nlet q = \"INSERT INTO private.processing_runs (model_id, model_revision) VALUES ($1, $2)\";\n",
        )
        .unwrap();
        assert!(matches!(
            provider_plane_gate7_projection_write_has_model_metadata(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 (§19 review): the only real `INSERT INTO private.processing_runs` in a `tests/`
    /// directory must not green this gate — a test fixture proves the matcher works, not that
    /// a production write path exists.
    #[test]
    fn provider_plane_gate7_test_fixture_only_is_not_applicable() {
        let tmp = fresh_tmp("pp-gate7-tests-only");
        let dir = tmp.join("crates/adapters/tests");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("fixture.rs"),
            "let q = \"INSERT INTO private.processing_runs (model_provider, model_id, model_revision) VALUES ($1, $2, $3)\";\n",
        )
        .unwrap();
        assert!(matches!(
            provider_plane_gate7_projection_write_has_model_metadata(&tmp),
            Verdict::NotApplicable(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn adr_0014_real_gateway_bootstrap_is_green() {
        assert_eq!(
            adr_0014_gateway_qdrant_read_only_gate(&real_root()),
            Verdict::Pass
        );
    }

    fn adr_0014_tmp_root(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "arch-check-adr0014-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn adr_0014_write_bootstrap(tmp: &Path, body: &str) {
        let dir = tmp.join(GATEWAY_SRC_DIR);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("bootstrap.rs"), body).unwrap();
    }

    /// 注错正向对照：`.with_access_mode(CellAccessMode::QdrantReadOnly)` present → green.
    #[test]
    fn adr_0014_qdrant_read_only_entry_is_green() {
        let tmp = adr_0014_tmp_root("green");
        adr_0014_write_bootstrap(
            &tmp,
            "entries.insert(IntraCellResource::QDRANT_REST, ResourceEntry::new(host, port, cell, cidrs, callers, tls).unwrap().with_access_mode(CellAccessMode::QdrantReadOnly));",
        );
        assert_eq!(adr_0014_gateway_qdrant_read_only_gate(&tmp), Verdict::Pass);
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 (红): 建了 QDRANT_REST entry 却没有 `.with_access_mode(QdrantReadOnly)` → red.
    #[test]
    fn adr_0014_qdrant_entry_missing_read_only_mode_is_red() {
        let tmp = adr_0014_tmp_root("missing-mode");
        adr_0014_write_bootstrap(
            &tmp,
            "entries.insert(IntraCellResource::QDRANT_REST, ResourceEntry::new(host, port, cell, cidrs, callers, tls).unwrap());",
        );
        assert!(matches!(
            adr_0014_gateway_qdrant_read_only_gate(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 注错 (红): 显式构造 `CellAccessMode::ReadWrite` → red, 即便同时也标了 QdrantReadOnly。
    #[test]
    fn adr_0014_explicit_read_write_mode_is_red() {
        let tmp = adr_0014_tmp_root("read-write");
        adr_0014_write_bootstrap(
            &tmp,
            "entries.insert(IntraCellResource::QDRANT_REST, ResourceEntry::new(host, port, cell, cidrs, callers, tls).unwrap().with_access_mode(CellAccessMode::QdrantReadOnly));\nlet oops = CellAccessMode::ReadWrite;",
        );
        assert!(matches!(
            adr_0014_gateway_qdrant_read_only_gate(&tmp),
            Verdict::Fail(_)
        ));
        fs::remove_dir_all(&tmp).ok();
    }

    /// 正向对照：只引用 `IntraCellResource::QDRANT_REST`（铸造 permit）而不构造 `ResourceEntry`
    /// 的文件不应被误判——这条扫描只关心注册点，不关心每一次读用途的引用。
    #[test]
    fn adr_0014_permit_reference_without_construction_is_not_flagged() {
        let tmp = adr_0014_tmp_root("permit-only");
        adr_0014_write_bootstrap(
            &tmp,
            "authorize_cell_access(&self.cell_registry, IntraCellResource::QDRANT_REST, Duration::from_secs(30))",
        );
        assert_eq!(
            adr_0014_gateway_qdrant_read_only_gate(&tmp),
            Verdict::NotApplicable(
                "gateway 尚未注册 IntraCellResource::QDRANT_REST resource entry".to_owned()
            )
        );
        fs::remove_dir_all(&tmp).ok();
    }
}
