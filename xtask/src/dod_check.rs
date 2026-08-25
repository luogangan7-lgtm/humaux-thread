//! xtask `dod-check` — G80-33 DoD verifier closure + G80-17.D2 BootstrapDeferredSpec key-set
//! equality (§69 DoD Verifier Contract, §69 Bootstrap Deferred Manifest / D1–D3, §1.14 /
//! §1.14.1). Judgment criteria live only in those spec sections (repo CLAUDE.md hard
//! boundary: "判据正文不得复制到任何第二处"); this file cites § numbers and does not
//! restate their prose.
//!
//! Reads two files as data, never as compiled dependencies (xtask does not, and per repo
//! convention must not, depend on `humaux-testkit`):
//! - the canonical spec (`SPEC_PATH`) — §69 checkbox list + Verifier Contract + Bootstrap
//!   Deferred Manifest, and §1.14's `mechanism-registry` fence for G80-17.D1/D2;
//! - `crates/testkit/src/dod.rs` (`TESTKIT_DOD_PATH`) — the `DodVerifier` registry, scanned
//!   as source text for its `DodVerifier { ... }` literals (see that file's own rustdoc for
//!   the exact layout this parser expects).
//!
//! `--phase N` (default 0) is the "current phase" both G80-33 rules 4/5 and G80-17.D3 gate
//! on.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const SPEC_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../docs/architecture/Baseline_2.8.md"
);
const TESTKIT_DOD_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../crates/testkit/src/dod.rs");

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

/// §57.1: all gates are three-state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateStatus {
    Pass,
    Fail,
    NotApplicable,
}

/// Uniform three-state outcome record (§57.1) emitted by every check in this file.
#[derive(Debug, Clone)]
pub struct GateResult {
    /// Gate/check identifier this result belongs to.
    pub gate: String,
    /// §57.1 three-state status.
    pub status: GateStatus,
    /// Human-readable explanation; required when `status` is `NotApplicable` (§57.1: must
    /// name the missing object).
    pub detail: String,
}

fn pass(gate: impl Into<String>, detail: impl Into<String>) -> GateResult {
    GateResult {
        gate: gate.into(),
        status: GateStatus::Pass,
        detail: detail.into(),
    }
}
fn fail(gate: impl Into<String>, detail: impl Into<String>) -> GateResult {
    GateResult {
        gate: gate.into(),
        status: GateStatus::Fail,
        detail: detail.into(),
    }
}
fn not_applicable(gate: impl Into<String>, detail: impl Into<String>) -> GateResult {
    GateResult {
        gate: gate.into(),
        status: GateStatus::NotApplicable,
        detail: detail.into(),
    }
}

// ============================================================================
// §69 section slicing + checkbox parsing
// ============================================================================

/// Slices the `# 69. Definition of Done` chapter out of the full spec text, up to (not
/// including) the next top-level `# N.` heading. Covers the DoD checklist, the Verifier
/// Contract, and the Bootstrap Deferred Manifest / D1-D3 table — all of §69 (spec lines
/// 10854-11189 in Baseline_2.8.md at the time of writing).
fn section_69(text: &str) -> &str {
    let start = match text.find("# 69. Definition of Done") {
        Some(i) => i,
        None => return "",
    };
    let rest = &text[start..];
    let end = rest
        .match_indices("\n# ")
        .find(|(i, _)| {
            rest[*i + 1..]
                .strip_prefix("# ")
                .and_then(|r| r.split('.').next())
                .is_some_and(|n| n.trim().parse::<u32>().is_ok())
        })
        .map(|(i, _)| i + 1)
        .unwrap_or(rest.len());
    &rest[..end]
}

/// G80-33 rule 1: every `- [ ]` line in §69 must carry a `[DOD-xxx]` tag. Returns the
/// offending lines verbatim (empty ⇒ pass). Deliberately does not match `- [deferred]`
/// (§69's DOD-054 sibling line) — that is not an unchecked DoD checkbox.
fn orphan_checkbox_lines(section: &str) -> Vec<&str> {
    section
        .lines()
        .filter(|l| l.trim_start().starts_with("- [ ]"))
        .filter(|l| !l.contains("[DOD-"))
        .collect()
}

fn check_rule1(section: &str) -> GateResult {
    let orphans = orphan_checkbox_lines(section);
    if orphans.is_empty() {
        pass(
            "G80-33 rule1",
            "no unchecked §69 line lacks a [DOD-xxx] tag",
        )
    } else {
        fail(
            "G80-33 rule1",
            format!(
                "{} line(s) with no DOD id: {}",
                orphans.len(),
                orphans.join(" | ")
            ),
        )
    }
}

/// One `- [ ] [DOD-xxx][phase=N]` entry parsed out of §69.
#[derive(Debug, Clone)]
struct CheckboxEntry {
    id: String,
    num: u32,
    phase: u32,
}

/// Extracts `id`/`phase` from a `[DOD-xxx][phase=N]` tag starting at byte offset of `[DOD-`
/// within `line`. Returns `None` if the tag is malformed (missing digits or phase suffix).
fn parse_dod_phase_tag(line: &str) -> Option<(String, u32)> {
    let dod_start = line.find("[DOD-")? + "[DOD-".len();
    let dod_close = dod_start + line[dod_start..].find(']')?;
    let num_str = &line[dod_start..dod_close];
    let num: u32 = num_str.parse().ok()?;
    let after = &line[dod_close..];
    let phase_start = after.find("[phase=")? + "[phase=".len();
    let phase_close = phase_start + after[phase_start..].find(']')?;
    let phase: u32 = after[phase_start..phase_close].trim().parse().ok()?;
    Some((format!("DOD-{num:03}"), phase))
}

fn parse_checkbox_entries(section: &str) -> Vec<CheckboxEntry> {
    section
        .lines()
        .filter(|l| l.trim_start().starts_with("- [ ]"))
        .filter_map(|l| {
            let (id, phase) = parse_dod_phase_tag(l)?;
            let num: u32 = id.trim_start_matches("DOD-").parse().ok()?;
            Some(CheckboxEntry { id, num, phase })
        })
        .collect()
}

/// Reads the frozen `当前 DoD IDs: DOD-001 .. DOD-091` line (§69 DoD Verifier Contract) and
/// returns `(min, max)`. This is the single source for "how many ids should exist" — never
/// hardcoded, so a future re-freeze (adding/removing DoD items) doesn't require touching
/// this checker.
fn frozen_id_bounds(section: &str) -> Option<(u32, u32)> {
    let line = section.lines().find(|l| l.contains("当前 DoD IDs:"))?;
    let mut nums = line.match_indices("DOD-").filter_map(|(i, _)| {
        let s = &line[i + "DOD-".len()..];
        let end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
        s[..end].parse::<u32>().ok()
    });
    let min = nums.next()?;
    let max = nums.next()?;
    Some((min, max))
}

/// G80-33 rule 2: ids unique, contiguous `DOD-001..DOD-0{max}` per the frozen bounds line,
/// and every `phase` in `0..=17`.
fn check_rule2(entries: &[CheckboxEntry], bounds: Option<(u32, u32)>) -> GateResult {
    let Some((min, max)) = bounds else {
        return fail(
            "G80-33 rule2",
            "missing object: §69 frozen '当前 DoD IDs: DOD-001 .. DOD-0NN' line",
        );
    };

    let mut seen: BTreeSet<u32> = BTreeSet::new();
    let mut dups = Vec::new();
    for e in entries {
        if !seen.insert(e.num) {
            dups.push(e.id.clone());
        }
    }
    let expected: BTreeSet<u32> = (min..=max).collect();
    let missing: Vec<u32> = expected.difference(&seen).copied().collect();
    let extra: Vec<u32> = seen.difference(&expected).copied().collect();
    let out_of_range: Vec<String> = entries
        .iter()
        .filter(|e| !(0..=17).contains(&e.phase))
        .map(|e| format!("{}(phase={})", e.id, e.phase))
        .collect();

    let mut problems = Vec::new();
    if !dups.is_empty() {
        problems.push(format!("duplicate ids: {}", dups.join(",")));
    }
    if !missing.is_empty() {
        problems.push(format!("missing ids: {missing:?}"));
    }
    if !extra.is_empty() {
        problems.push(format!("ids outside DOD-{min:03}..DOD-{max:03}: {extra:?}"));
    }
    if !out_of_range.is_empty() {
        problems.push(format!("phase outside 0..=17: {}", out_of_range.join(",")));
    }

    if problems.is_empty() {
        pass(
            "G80-33 rule2",
            format!("DOD-{min:03}..DOD-{max:03} unique/contiguous, all phases in 0..=17"),
        )
    } else {
        fail("G80-33 rule2", problems.join("; "))
    }
}

// ============================================================================
// testkit/src/dod.rs registry — read as text (xtask has no dependency on humaux-testkit)
// ============================================================================

#[derive(Debug, Clone)]
struct RegistryEntry {
    id: String,
    phase: u32,
    fault: String,
    #[allow(dead_code)] // parsed for completeness; kind classification not yet enforced
    kind: String,
    verifier_ref: String,
}

fn unquote(s: &str) -> String {
    s.trim().trim_matches('"').to_string()
}

/// Scans `crates/testkit/src/dod.rs` source text for `DodVerifier { ... }` struct literals.
/// Relies on that file's documented one-field-per-line layout (see its own rustdoc); a block
/// missing any of the five fields is silently dropped — such a block cannot represent a
/// registered verifier for [`check_dod_ids`], which will then correctly report the
/// corresponding DoD id as having zero verifiers.
fn parse_registry(text: &str) -> Vec<RegistryEntry> {
    const OPEN: &str = "DodVerifier {";
    // Anchor past the struct definition (and its leading rustdoc, which names `DodVerifier {`
    // in prose) before hunting for real struct-literal entries: `pub const ...: &[DodVerifier]`
    // marks the start of the array literals this scan is actually meant to read. Absent that
    // marker there are no registered entries to find, so scan nothing rather than risk matching
    // the struct definition itself.
    let mut out = Vec::new();
    let mut rest = match text.find("pub const") {
        Some(array_start) => &text[array_start..],
        None => "",
    };
    while let Some(start) = rest.find(OPEN) {
        let after = &rest[start + OPEN.len()..];
        let end = after.find("},").unwrap_or(after.len());
        let block = &after[..end];

        let mut id = None;
        let mut phase = None;
        let mut fault = None;
        let mut kind = None;
        let mut verifier_ref = None;
        for raw in block.lines() {
            let line = raw.trim().trim_end_matches(',');
            if let Some(v) = line.strip_prefix("id:") {
                id = Some(unquote(v));
            } else if let Some(v) = line.strip_prefix("phase:") {
                phase = v.trim().parse::<u32>().ok();
            } else if let Some(v) = line.strip_prefix("fault:") {
                fault = Some(unquote(v));
            } else if let Some(v) = line.strip_prefix("kind:") {
                kind = Some(unquote(v));
            } else if let Some(v) = line.strip_prefix("verifier_ref:") {
                verifier_ref = Some(unquote(v));
            }
        }
        if let (Some(id), Some(phase), Some(fault), Some(kind), Some(verifier_ref)) =
            (id, phase, fault, kind, verifier_ref)
        {
            out.push(RegistryEntry {
                id,
                phase,
                fault,
                kind,
                verifier_ref,
            });
        }
        rest = &after[end..];
    }
    out
}

// ============================================================================
// architecture-check subprocess bridge (G80-33 rule 5 execution body)
// ============================================================================

/// Runs `cargo xtask architecture-check` once and returns its combined stdout+stderr, so
/// every `architecture-check::<name>` verifier this run needs can be resolved from a single
/// invocation rather than one subprocess per DoD id.
fn run_architecture_check() -> Result<String, String> {
    let output = Command::new("cargo")
        .args(["run", "--quiet", "-p", "xtask", "--", "architecture-check"])
        .current_dir(workspace_root())
        .output()
        .map_err(|e| format!("failed to spawn `cargo run -p xtask -- architecture-check`: {e}"))?;
    let mut combined = String::from_utf8_lossy(&output.stdout).into_owned();
    combined.push('\n');
    combined.push_str(&String::from_utf8_lossy(&output.stderr));
    Ok(combined)
}

/// Finds architecture-check's report line for sub-check `name` (its `report()` prints
/// `"architecture-check: <tag> — <name>..."`) and returns `<tag>`.
fn lookup_check_tag<'a>(output: &'a str, name: &str) -> Option<&'a str> {
    output
        .lines()
        .find(|l| l.contains(name))
        .and_then(|l| l.strip_prefix("architecture-check: "))
        .and_then(|rest| rest.split(" — ").next())
        .map(str::trim)
}

/// Resolves one `verifier_ref` to a [`GateStatus`] by actually running its named execution
/// body (G80-33 rule 5). `arch_cache` memoizes the one `architecture-check` subprocess call
/// across every `architecture-check::*` verifier in a single `run()`.
fn resolve_verifier(
    verifier_ref: &str,
    arch_cache: &mut Option<Result<String, String>>,
) -> Result<GateStatus, String> {
    if let Some(name) = verifier_ref.strip_prefix("architecture-check::") {
        let output = arch_cache
            .get_or_insert_with(run_architecture_check)
            .as_ref()
            .map_err(|e| e.clone())?;
        return match lookup_check_tag(output, name) {
            Some("pass") => Ok(GateStatus::Pass),
            Some("fail") => Ok(GateStatus::Fail),
            Some("not_applicable") => Ok(GateStatus::NotApplicable),
            Some(other) => Err(format!("unrecognized architecture-check tag {other:?}")),
            None => Err(format!(
                "architecture-check produced no report line for sub-check {name:?}"
            )),
        };
    }
    // `test::<package>::<filter>` — runs `cargo test -p <package> <filter>` and judges by
    // cargo's own `test result:` line (§69 verifier kind = "test"). A filter matching zero
    // tests is Fail, not Pass — a vacuous verifier must not satisfy G80-33 rule 5.
    if let Some(rest) = verifier_ref.strip_prefix("test::") {
        let (package, filter) = rest
            .split_once("::")
            .ok_or_else(|| format!("test:: verifier needs <package>::<filter>, got {rest:?}"))?;
        let out = std::process::Command::new("cargo")
            .args(["test", "-p", package, filter, "--", "--test-threads=1"])
            .output()
            .map_err(|e| format!("cargo test spawn failed: {e}"))?;
        let text = String::from_utf8_lossy(&out.stdout);
        let (mut passed, mut failed) = (0u64, 0u64);
        for line in text.lines() {
            if let Some(rest) = line.trim().strip_prefix("test result: ") {
                for part in rest.split(';') {
                    let part = part.trim();
                    if let Some(n) = part.strip_suffix(" passed") {
                        passed += n
                            .split_whitespace()
                            .next_back()
                            .unwrap_or("0")
                            .parse::<u64>()
                            .unwrap_or(0);
                    } else if let Some(n) = part.strip_suffix(" failed") {
                        failed += n
                            .split_whitespace()
                            .next_back()
                            .unwrap_or("0")
                            .parse::<u64>()
                            .unwrap_or(0);
                    }
                }
            }
        }
        return Ok(if failed > 0 || passed == 0 {
            GateStatus::Fail
        } else {
            GateStatus::Pass
        });
    }
    Err(format!(
        "no execution binding registered in dod_check.rs for verifier_ref {verifier_ref:?}"
    ))
}

/// G80-33 rules 3/4/5/6, evaluated per §69 checkbox id against the testkit registry.
/// rule 4: `cb` isn't due yet (`current_phase < cb.phase`) — not_applicable, but must still
/// name the missing/registered object either way (§57.1 第2条).
fn not_due_result(
    cb: &CheckboxEntry,
    matches: &[&RegistryEntry],
    current_phase: u32,
) -> GateResult {
    let detail = match matches {
        [] => format!(
            "owner phase {} > current phase {current_phase}; missing object: crates/testkit/src/dod.rs has no DodVerifier entry for {} yet",
            cb.phase, cb.id
        ),
        [entry] => format!(
            "owner phase {} > current phase {current_phase}; verifier {} registered but not required yet",
            cb.phase, entry.verifier_ref
        ),
        _ => format!(
            "owner phase {} > current phase {current_phase}; {} duplicate verifiers already registered for {}",
            cb.phase,
            matches.len(),
            cb.id
        ),
    };
    not_applicable(cb.id.clone(), detail)
}

/// `cb` is due (`current_phase >= cb.phase`) and has exactly one registry `entry` (rule 3's
/// forward direction already holds) — rules 3(reverse)/5/6 plus the registry-vs-§69 phase
/// agreement check.
fn due_single_verifier_result(
    cb: &CheckboxEntry,
    entry: &RegistryEntry,
    registry: &[RegistryEntry],
    arch_cache: &mut Option<Result<String, String>>,
) -> GateResult {
    // Registry metadata must agree with §69's own canonical [phase=N] tag — otherwise the
    // registry could silently drift from the spec it claims to verify.
    if entry.phase != cb.phase {
        return fail(
            cb.id.clone(),
            format!(
                "registry phase={} disagrees with §69 [phase={}]",
                entry.phase, cb.phase
            ),
        );
    }
    // rule 6: fault must be non-empty.
    if entry.fault.trim().is_empty() {
        return fail(cb.id.clone(), "fault=\"\" — NOT_ADMITTED");
    }
    // rule 3 (reverse direction): verifier_ref must map back to exactly one id.
    let ref_owners = registry
        .iter()
        .filter(|e| e.verifier_ref == entry.verifier_ref)
        .count();
    if ref_owners != 1 {
        return fail(
            cb.id.clone(),
            format!(
                "verifier_ref {:?} is shared by {ref_owners} registry entries — reverse lookup ambiguous",
                entry.verifier_ref
            ),
        );
    }
    // rule 5: verifier must pass.
    match resolve_verifier(&entry.verifier_ref, arch_cache) {
        Ok(GateStatus::Pass) => pass(
            cb.id.clone(),
            format!(
                "verifier {} pass (fault={})",
                entry.verifier_ref, entry.fault
            ),
        ),
        Ok(other) => fail(
            cb.id.clone(),
            format!("verifier {} did not pass: {other:?}", entry.verifier_ref),
        ),
        Err(e) => fail(
            cb.id.clone(),
            format!("verifier {} execution error: {e}", entry.verifier_ref),
        ),
    }
}

fn check_dod_ids(
    checkboxes: &[CheckboxEntry],
    registry: &[RegistryEntry],
    current_phase: u32,
) -> Vec<GateResult> {
    let mut results = Vec::new();
    let checkbox_ids: BTreeSet<&str> = checkboxes.iter().map(|e| e.id.as_str()).collect();
    let mut arch_cache: Option<Result<String, String>> = None;

    for cb in checkboxes {
        let matches: Vec<&RegistryEntry> = registry.iter().filter(|e| e.id == cb.id).collect();

        if current_phase < cb.phase {
            results.push(not_due_result(cb, &matches, current_phase));
            continue;
        }

        // rule 3: exactly one verifier.
        match matches.as_slice() {
            [] => {
                results.push(fail(
                    cb.id.clone(),
                    format!(
                        "no verifier registered (missing object: crates/testkit/src/dod.rs DodVerifier entry with id=\"{}\")",
                        cb.id
                    ),
                ));
            }
            [entry] => {
                results.push(due_single_verifier_result(
                    cb,
                    entry,
                    registry,
                    &mut arch_cache,
                ));
            }
            many => {
                results.push(fail(
                    cb.id.clone(),
                    format!(
                        "{} verifiers registered for {}, expected exactly 1: {:?}",
                        many.len(),
                        cb.id,
                        many.iter().map(|e| &e.verifier_ref).collect::<Vec<_>>()
                    ),
                ));
            }
        }
    }

    // rule 3 reverse: a registry entry whose id isn't even a real §69 checkbox.
    for entry in registry {
        if !checkbox_ids.contains(entry.id.as_str()) {
            results.push(fail(
                entry.id.clone(),
                format!(
                    "verifier_ref {:?} points to {}, which is not a §69 checkbox id",
                    entry.verifier_ref, entry.id
                ),
            ));
        }
    }

    results
}

// ============================================================================
// G80-17.D1/D2/D3 — §1.14 BootstrapDeferredSpec vs §69 Bootstrap Deferred Manifest
// ============================================================================

/// One §1.14 `mechanism-registry` fence row's fields relevant to §1.14.1's
/// `BootstrapDeferredSpec` formula.
#[derive(Debug, Clone)]
struct MechRow {
    ch: u32,
    mechanism: String,
    activation_kind: String,
    min_denominator: Option<i64>,
    bootstrap_value: Option<i64>,
}

/// Parses the single `mechanism-registry` fence's 8-column rows (§1.14 line 419 column
/// order). Column-count / activation_kind-enum / fence-count validity is `mechanism-registry`
/// G0's job (xtask/src/mechanism_registry.rs), not this checker's — a malformed row here
/// simply fails to parse and is dropped, which starves it out of `BootstrapDeferredSpec`
/// (visible downstream as a D1/D2 diff, not a silent pass).
fn extract_mechanism_registry_rows(text: &str) -> Vec<MechRow> {
    const OPEN: &str = "```mechanism-registry";
    let Some(start) = text.find(OPEN) else {
        return Vec::new();
    };
    let body_start = start + OPEN.len();
    let Some(rel_end) = text[body_start..].find("\n```") else {
        return Vec::new();
    };
    text[body_start..body_start + rel_end]
        .lines()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split('|').map(str::trim).collect();
            if fields.len() != 8 {
                return None;
            }
            Some(MechRow {
                ch: fields[0].parse().ok()?,
                mechanism: fields[1].to_string(),
                activation_kind: fields[2].to_string(),
                min_denominator: fields[3].parse::<i64>().ok(),
                bootstrap_value: fields[5].parse::<i64>().ok(),
            })
        })
        .collect()
}

/// §1.14.1 `BootstrapDeferredSpec`: rows that are `DENOMINATOR_GATED`, have integer
/// `bootstrap_value`/`min_denominator`, and `bootstrap_value < min_denominator`. Key set is
/// `(ch, mechanism)`, exact string match (§69 Bootstrap Deferred Manifest "配对键").
fn bootstrap_deferred_spec_keys(rows: &[MechRow]) -> BTreeSet<(u32, String)> {
    rows.iter()
        .filter(|r| {
            r.activation_kind == "DENOMINATOR_GATED"
                && matches!((r.bootstrap_value, r.min_denominator), (Some(b), Some(m)) if b < m)
        })
        .map(|r| (r.ch, r.mechanism.clone()))
        .collect()
}

/// One row of §69's "Phase 0 Migration Bootstrap Deferred Manifest" table.
#[derive(Debug, Clone)]
struct ManifestRow {
    ch: u32,
    mechanism: String,
    syn_test: String,
}

/// Content of the first `` `...` `` span in `s` — the manifest's syn test column carries
/// trailing prose after the closing backtick for one row (row 8: `` `syn_two_nodes`（kind
/// 双节点） ``), so a plain `trim_matches('`')` would keep that prose.
fn first_backtick_token(s: &str) -> Option<String> {
    let start = s.find('`')? + 1;
    let len = s[start..].find('`')?;
    Some(s[start..start + len].to_string())
}

/// Parses the 5-column `| # | mechanism | §ch | ... | `syn_*` |` manifest table out of §69.
/// Header/separator rows fall out naturally: their first column doesn't parse as a bare
/// number.
fn parse_manifest_table(section_69: &str) -> Vec<ManifestRow> {
    section_69
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if !line.starts_with('|') {
                return None;
            }
            let cols: Vec<&str> = line.trim_matches('|').split('|').map(str::trim).collect();
            if cols.len() != 5 {
                return None;
            }
            cols[0].parse::<u32>().ok()?; // the leading "#" column; discarded, just a filter
            let ch: u32 = cols[2].trim_start_matches('§').parse().ok()?;
            Some(ManifestRow {
                ch,
                mechanism: cols[1].to_string(),
                syn_test: first_backtick_token(cols[4]).unwrap_or_default(),
            })
        })
        .collect()
}

/// G80-17.D2: `keys(manifest) == keys(BootstrapDeferredSpec)`, computed purely from spec
/// text (§1.14 static columns) — never from runtime Observation state (§69 "D2 不读取
/// `NOT_APPLICABLE_YET Observation` 行数、runtime `derived_status` 或 markdown `status` 列").
fn check_d2(mech_rows: &[MechRow], manifest_rows: &[ManifestRow]) -> GateResult {
    let spec_keys = bootstrap_deferred_spec_keys(mech_rows);
    let manifest_keys: BTreeSet<(u32, String)> = manifest_rows
        .iter()
        .map(|r| (r.ch, r.mechanism.clone()))
        .collect();

    if spec_keys == manifest_keys {
        pass(
            "G80-17.D2",
            format!("{} keys match exactly", spec_keys.len()),
        )
    } else {
        let manifest_only: Vec<_> = manifest_keys.difference(&spec_keys).collect();
        let spec_only: Vec<_> = spec_keys.difference(&manifest_keys).collect();
        fail(
            "G80-17.D2",
            format!(
                "manifest={} vs spec={}; manifest-only={manifest_only:?}; spec-only={spec_only:?}",
                manifest_keys.len(),
                spec_keys.len()
            ),
        )
    }
}

/// G80-17.D1: every manifest row's `(ch,mechanism)` must independently satisfy §1.14's
/// `DENOMINATOR_GATED && bootstrap_value < min_denominator` eligibility — a distinct check
/// from D2's set-equality so a single row losing eligibility (e.g. its `bootstrap_value`
/// edited above `min_denominator`) is reported with the specific numbers, not just a set
/// diff.
fn check_d1(mech_rows: &[MechRow], manifest_rows: &[ManifestRow]) -> GateResult {
    let mut violations = Vec::new();
    for m in manifest_rows {
        match mech_rows.iter().find(|r| r.ch == m.ch) {
            None => violations.push(format!("ch={} not found in §1.14 registry", m.ch)),
            Some(row) => {
                let eligible = row.activation_kind == "DENOMINATOR_GATED"
                    && matches!((row.bootstrap_value, row.min_denominator), (Some(b), Some(mn)) if b < mn);
                if !eligible {
                    violations.push(format!(
                        "ch={} mechanism={:?}: activation_kind={} bootstrap_value={:?} min_denominator={:?} — not eligible",
                        m.ch, m.mechanism, row.activation_kind, row.bootstrap_value, row.min_denominator
                    ));
                }
            }
        }
    }
    if violations.is_empty() {
        pass(
            "G80-17.D1",
            format!("{} manifest row(s) all eligible", manifest_rows.len()),
        )
    } else {
        fail("G80-17.D1", violations.join("; "))
    }
}

/// G80-17.D3: each manifest row's `syn_*` synthetic-denominator test must exist, not be
/// `#[ignore]`d, and be green. Before Phase 14 this whole gate is `not_applicable` and must
/// name which fixtures are still missing (task scoping of §69 D3 for this wave — the
/// fixtures this checks for don't exist until a later wave builds them).
///
/// ponytail: "green" is approximated here as "found and not `#[ignore]`d" via a source grep,
/// not an actual `cargo test <name>` run — upgrade to invoking the real test once the Phase 14
/// wave adds these fixtures and their pass/fail becomes the interesting signal.
fn check_d3(manifest_rows: &[ManifestRow], current_phase: u32) -> GateResult {
    let root = workspace_root();
    let haystack = walk_rs_sources(&root.join("crates")).join("\n");

    let mut missing = Vec::new();
    let mut ignored = Vec::new();
    for m in manifest_rows {
        let needle = format!("fn {}", m.syn_test);
        match haystack.find(&needle) {
            None => missing.push(m.syn_test.clone()),
            Some(pos) => {
                let preceding = &haystack[..pos];
                let last_lines: String = preceding
                    .lines()
                    .rev()
                    .take(3)
                    .collect::<Vec<_>>()
                    .join("\n");
                if last_lines.contains("#[ignore") {
                    ignored.push(m.syn_test.clone());
                }
            }
        }
    }

    if current_phase < 14 {
        return not_applicable(
            "G80-17.D3",
            format!(
                "not required before phase 14; missing fixtures: {:?}{}",
                missing,
                if ignored.is_empty() {
                    String::new()
                } else {
                    format!("; ignored fixtures: {ignored:?}")
                }
            ),
        );
    }
    if missing.is_empty() && ignored.is_empty() {
        pass(
            "G80-17.D3",
            format!(
                "{} syn_* fixtures present and not #[ignore]d",
                manifest_rows.len()
            ),
        )
    } else {
        fail(
            "G80-17.D3",
            format!("missing={missing:?}; ignored={ignored:?}"),
        )
    }
}

/// Concatenates every `.rs` file under `dir` (skipping `target`/`.git`), for D3's plain-text
/// `fn <name>` existence grep. Small, unindexed scan is deliberate — D3 runs at most once per
/// `dod-check` invocation and only 8 needles exist (§69 Bootstrap Deferred Manifest, fixed at
/// 8 rows).
fn walk_rs_sources(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if matches!(name, "target" | ".git") {
                continue;
            }
            out.extend(walk_rs_sources(&path));
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs")
            && let Ok(src) = fs::read_to_string(&path)
        {
            out.push(src);
        }
    }
    out
}

// ============================================================================
// CLI entry point
// ============================================================================

fn parse_phase_flag(args: &[String]) -> u32 {
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if a == "--phase" {
            if let Some(v) = it.next()
                && let Ok(n) = v.parse()
            {
                return n;
            }
        } else if let Some(v) = a.strip_prefix("--phase=")
            && let Ok(n) = v.parse()
        {
            return n;
        }
    }
    0
}

fn report(results: &[GateResult]) -> i32 {
    let mut failed = false;
    for r in results {
        let tag = match r.status {
            GateStatus::Pass => "pass",
            GateStatus::Fail => {
                failed = true;
                "fail"
            }
            GateStatus::NotApplicable => "not_applicable",
        };
        eprintln!("dod-check {}: {tag} — {}", r.gate, r.detail);
    }
    i32::from(failed)
}

/// Runs the full §69/§1.14 DoD verifier closure check (G80-33 + G80-17.D1/D2/D3).
pub fn run(args: &[String]) -> i32 {
    let phase = parse_phase_flag(args);

    let text = match fs::read_to_string(SPEC_PATH) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("dod-check: fail — cannot read spec at {SPEC_PATH}: {e}");
            return 1;
        }
    };
    let registry_text = match fs::read_to_string(TESTKIT_DOD_PATH) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("dod-check: fail — cannot read registry at {TESTKIT_DOD_PATH}: {e}");
            return 1;
        }
    };

    let mut results = check_all(&text, &registry_text, phase);
    results.sort_by(|a, b| a.gate.cmp(&b.gate));
    report(&results)
}

/// Pure function form of [`run`] (spec text + registry text + phase in, results out) — the
/// shape every fixture test below exercises directly, without touching the real filesystem
/// paths.
fn check_all(spec_text: &str, registry_text: &str, phase: u32) -> Vec<GateResult> {
    let section = section_69(spec_text);
    let mut results = vec![check_rule1(section)];

    let checkboxes = parse_checkbox_entries(section);
    let bounds = frozen_id_bounds(section);
    results.push(check_rule2(&checkboxes, bounds));

    let registry = parse_registry(registry_text);
    results.extend(check_dod_ids(&checkboxes, &registry, phase));

    let mech_rows = extract_mechanism_registry_rows(spec_text);
    let manifest_rows = parse_manifest_table(section);
    results.push(check_d2(&mech_rows, &manifest_rows));
    results.push(check_d1(&mech_rows, &manifest_rows));
    results.push(check_d3(&manifest_rows, phase));

    results
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate<'a>(results: &'a [GateResult], name: &str) -> &'a GateResult {
        results
            .iter()
            .find(|r| r.gate == name)
            .unwrap_or_else(|| panic!("no gate result named {name:?} in {results:#?}"))
    }

    fn real_spec() -> String {
        fs::read_to_string(SPEC_PATH).expect("spec must be readable")
    }
    fn real_registry() -> String {
        fs::read_to_string(TESTKIT_DOD_PATH).expect("registry must be readable")
    }

    // -- section_69 / rule1 --------------------------------------------------------------

    #[test]
    fn section_69_isolates_only_that_chapter() {
        let text = "# 68. Prior\nnoise\n# 69. Definition of Done\nbody\n- [ ] [DOD-001][phase=0] x\n# 70. Next\nnoise2\n";
        let s = section_69(text);
        assert!(s.contains("DOD-001"));
        assert!(!s.contains("noise2"));
        assert!(!s.contains("68. Prior"));
    }

    #[test]
    fn rule1_passes_on_tagged_checkbox_and_ignores_deferred_bullet() {
        let section = "- [ ] [DOD-001][phase=0] ok\n- [deferred] not a dod checkbox\n";
        assert_eq!(check_rule1(section).status, GateStatus::Pass);
    }

    /// 注错: add a checkbox with no DOD id (§69 G80-33 injected fault #4: "给 §69 新增
    /// checkbox 不加 DOD id -> 红").
    #[test]
    fn rule1_red_on_orphan_checkbox_then_green() {
        let red_section = "- [ ] [DOD-001][phase=0] ok\n- [ ] a new requirement with no id\n";
        let red = check_rule1(red_section);
        assert_eq!(red.status, GateStatus::Fail);
        assert!(red.detail.contains("a new requirement with no id"));

        let green_section = "- [ ] [DOD-001][phase=0] ok\n";
        assert_eq!(check_rule1(green_section).status, GateStatus::Pass);
    }

    #[test]
    fn real_spec_rule1_passes() {
        let text = real_spec();
        assert_eq!(check_rule1(section_69(&text)).status, GateStatus::Pass);
    }

    // -- rule2 -----------------------------------------------------------------------------

    #[test]
    fn frozen_id_bounds_parses_min_max() {
        let section = "text\n```text\n当前 DoD IDs: DOD-001 .. DOD-091\n```\n";
        assert_eq!(frozen_id_bounds(section), Some((1, 91)));
    }

    fn fixture_section(ids_and_phases: &[(u32, u32)], max: u32) -> String {
        let mut s = String::from("```text\n当前 DoD IDs: DOD-001 .. DOD-");
        s.push_str(&format!("{max:03}\n```\n"));
        for (n, p) in ids_and_phases {
            s.push_str(&format!("- [ ] [DOD-{n:03}][phase={p}] item\n"));
        }
        s
    }

    #[test]
    fn rule2_passes_on_unique_contiguous_ids_and_valid_phases() {
        let section = fixture_section(&[(1, 0), (2, 3), (3, 17)], 3);
        let entries = parse_checkbox_entries(&section);
        let bounds = frozen_id_bounds(&section);
        assert_eq!(check_rule2(&entries, bounds).status, GateStatus::Pass);
    }

    #[test]
    fn rule2_red_on_duplicate_id() {
        let section = fixture_section(&[(1, 0), (1, 0), (2, 0)], 2);
        let entries = parse_checkbox_entries(&section);
        let bounds = frozen_id_bounds(&section);
        let r = check_rule2(&entries, bounds);
        assert_eq!(r.status, GateStatus::Fail);
        assert!(r.detail.contains("duplicate"));
    }

    #[test]
    fn rule2_red_on_phase_out_of_range() {
        let section = fixture_section(&[(1, 0), (2, 18)], 2);
        let entries = parse_checkbox_entries(&section);
        let bounds = frozen_id_bounds(&section);
        let r = check_rule2(&entries, bounds);
        assert_eq!(r.status, GateStatus::Fail);
        assert!(r.detail.contains("phase outside"));
    }

    #[test]
    fn real_spec_rule2_passes() {
        let text = real_spec();
        let section = section_69(&text);
        let entries = parse_checkbox_entries(section);
        let bounds = frozen_id_bounds(section);
        let r = check_rule2(&entries, bounds);
        assert_eq!(r.status, GateStatus::Pass, "{}", r.detail);
        assert_eq!(entries.len(), 91, "expected all 91 DoD ids to parse");
    }

    // -- registry parsing / rule3 / rule6 ---------------------------------------------------

    #[test]
    fn parse_registry_reads_real_phase0_file() {
        let entries = parse_registry(&real_registry());
        assert_eq!(entries.len(), 5, "{entries:#?}");
        for id in ["DOD-001", "DOD-002", "DOD-003", "DOD-004", "DOD-005"] {
            assert!(entries.iter().any(|e| e.id == id), "missing {id}");
        }
    }

    fn one_checkbox(id: &str, phase: u32) -> Vec<CheckboxEntry> {
        vec![CheckboxEntry {
            id: id.to_string(),
            num: id.trim_start_matches("DOD-").parse().unwrap(),
            phase,
        }]
    }

    /// 注错: delete a verifier from the registry — 1 -> 0 -> red (§69 G80-33 injected fault
    /// #1). Uses a real DOD-003-shaped checkbox against an empty registry, i.e. simulates
    /// "registry entry removed" without touching the real testkit/dod.rs file.
    #[test]
    fn rule3_red_when_verifier_missing_for_due_phase() {
        let checkboxes = one_checkbox("DOD-003", 0);
        let results = check_dod_ids(&checkboxes, &[], 0);
        let r = gate(&results, "DOD-003");
        assert_eq!(r.status, GateStatus::Fail);
        assert!(r.detail.contains("no verifier registered"));
    }

    #[test]
    fn rule3_red_when_two_verifiers_registered_for_same_id() {
        let checkboxes = one_checkbox("DOD-003", 0);
        let registry = vec![
            RegistryEntry {
                id: "DOD-003".into(),
                phase: 0,
                fault: "f1".into(),
                kind: "gate".into(),
                verifier_ref: "architecture-check::A".into(),
            },
            RegistryEntry {
                id: "DOD-003".into(),
                phase: 0,
                fault: "f2".into(),
                kind: "gate".into(),
                verifier_ref: "architecture-check::B".into(),
            },
        ];
        let results = check_dod_ids(&checkboxes, &registry, 0);
        let r = gate(&results, "DOD-003");
        assert_eq!(r.status, GateStatus::Fail);
        assert!(r.detail.contains("expected exactly 1"));
    }

    /// 注错: `fault = ""` -> NOT_ADMITTED -> red (§69 G80-33 injected fault #3).
    #[test]
    fn rule6_red_when_fault_empty() {
        let checkboxes = one_checkbox("DOD-003", 0);
        let registry = vec![RegistryEntry {
            id: "DOD-003".into(),
            phase: 0,
            fault: String::new(),
            kind: "gate".into(),
            verifier_ref: "architecture-check::X".into(),
        }];
        let results = check_dod_ids(&checkboxes, &registry, 0);
        let r = gate(&results, "DOD-003");
        assert_eq!(r.status, GateStatus::Fail);
        assert!(r.detail.contains("NOT_ADMITTED"));
    }

    #[test]
    fn rule4_not_applicable_when_phase_not_yet_due_and_prints_missing_object() {
        let checkboxes = one_checkbox("DOD-999", 6);
        let results = check_dod_ids(&checkboxes, &[], 0);
        let r = gate(&results, "DOD-999");
        assert_eq!(r.status, GateStatus::NotApplicable);
        assert!(r.detail.contains("missing object"));
        assert!(r.detail.contains("DOD-999"));
    }

    /// Real registry entries for DOD-001..005: due at `--phase 0`, exactly one verifier
    /// each, non-empty fault, and — this is the only assertion in this file that spawns the
    /// real `architecture-check` subprocess — each verifier's *actual* pass/fail readout
    /// today. DOD-002 is a genuine, expected fail: its verifier is `§55.1 G80-2`
    /// (`build_request`/`RetrievalRequest`), and that object is real, current
    /// architecture-check state is `not_applicable` ("Phase 1 尚未交付") — this dod-check
    /// gate correctly reports that as DOD-002 not yet being satisfied, not as a bug in this
    /// test or the registry (see the `NB` comment on that entry in
    /// `crates/testkit/src/dod.rs`).
    #[test]
    fn real_phase0_dod_ids_readout_matches_current_repo_state() {
        let text = real_spec();
        let section = section_69(&text);
        let checkboxes: Vec<CheckboxEntry> = parse_checkbox_entries(section)
            .into_iter()
            .filter(|c| {
                ["DOD-001", "DOD-002", "DOD-003", "DOD-004", "DOD-005"].contains(&c.id.as_str())
            })
            .collect();
        assert_eq!(checkboxes.len(), 5);
        let registry = parse_registry(&real_registry());
        let results = check_dod_ids(&checkboxes, &registry, 0);
        // DOD-002 的 verifier 已改配 test::humaux-domain::ids（ADR-0001 同批更正——
        // 原 G80-2 映射是量错对象），五条 Phase 0 DoD 在真仓库上全 Pass。
        for id in ["DOD-001", "DOD-002", "DOD-003", "DOD-004", "DOD-005"] {
            let r = gate(&results, id);
            assert_eq!(r.status, GateStatus::Pass, "{id}: {}", r.detail);
        }
    }

    // -- G80-17.D1/D2/D3 ---------------------------------------------------------------------

    fn mech_row(
        ch: u32,
        mechanism: &str,
        kind: &str,
        min: Option<i64>,
        boot: Option<i64>,
    ) -> MechRow {
        MechRow {
            ch,
            mechanism: mechanism.to_string(),
            activation_kind: kind.to_string(),
            min_denominator: min,
            bootstrap_value: boot,
        }
    }
    fn manifest_row(ch: u32, mechanism: &str, syn: &str) -> ManifestRow {
        ManifestRow {
            ch,
            mechanism: mechanism.to_string(),
            syn_test: syn.to_string(),
        }
    }

    #[test]
    fn d2_passes_on_matching_key_sets() {
        let mech = vec![
            mech_row(12, "consensus 判定", "DENOMINATOR_GATED", Some(1), Some(0)),
            mech_row(15, "stream_log", "ALWAYS", None, None),
        ];
        let manifest = vec![manifest_row(12, "consensus 判定", "syn_consensus_4contrib")];
        assert_eq!(check_d2(&mech, &manifest).status, GateStatus::Pass);
    }

    /// 注错: delete manifest row 8 -> 8 vs 7 -> red (§69 D2 injected fault).
    #[test]
    fn d2_red_on_manifest_row_deleted() {
        let mech = vec![
            mech_row(12, "consensus 判定", "DENOMINATOR_GATED", Some(1), Some(0)),
            mech_row(
                21,
                "corroboration 加权",
                "DENOMINATOR_GATED",
                Some(1),
                Some(0),
            ),
        ];
        let manifest_full = vec![
            manifest_row(12, "consensus 判定", "syn_consensus_4contrib"),
            manifest_row(21, "corroboration 加权", "syn_corroboration_2src"),
        ];
        let manifest_missing_one =
            vec![manifest_row(12, "consensus 判定", "syn_consensus_4contrib")];

        assert_eq!(check_d2(&mech, &manifest_full).status, GateStatus::Pass);
        let red = check_d2(&mech, &manifest_missing_one);
        assert_eq!(red.status, GateStatus::Fail);
        assert!(red.detail.contains("manifest=1"));
        assert!(red.detail.contains("spec=2"));
    }

    /// 注错: rename the mechanism text by one character -> set diff -> red (§69 D2 injected
    /// fault).
    #[test]
    fn d2_red_on_mechanism_name_off_by_one_char() {
        let mech = vec![mech_row(
            43,
            "K8s HPA / PDB / anti-affinity（§67 同源）",
            "DENOMINATOR_GATED",
            Some(2),
            Some(0),
        )];
        let manifest = vec![manifest_row(
            43,
            "K8s HPA / PDB / anti-affinityX（§67 同源）",
            "syn_two_nodes",
        )];
        let r = check_d2(&mech, &manifest);
        assert_eq!(r.status, GateStatus::Fail);
        assert!(r.detail.contains("manifest-only"));
        assert!(r.detail.contains("spec-only"));
    }

    #[test]
    fn d1_passes_when_row_eligible() {
        let mech = vec![mech_row(
            21,
            "corroboration 加权",
            "DENOMINATOR_GATED",
            Some(1),
            Some(0),
        )];
        let manifest = vec![manifest_row(
            21,
            "corroboration 加权",
            "syn_corroboration_2src",
        )];
        assert_eq!(check_d1(&mech, &manifest).status, GateStatus::Pass);
    }

    /// 注错: bump `bootstrap_value` to/above `min_denominator` while the row stays in the
    /// manifest (§69 D1 injected fault: "把 ch=21 bootstrap_value 改成 2（min=1）而仍留本表 ⇒
    /// 红").
    #[test]
    fn d1_red_when_row_no_longer_eligible() {
        let mech = vec![mech_row(
            21,
            "corroboration 加权",
            "DENOMINATOR_GATED",
            Some(1),
            Some(2),
        )];
        let manifest = vec![manifest_row(
            21,
            "corroboration 加权",
            "syn_corroboration_2src",
        )];
        let r = check_d1(&mech, &manifest);
        assert_eq!(r.status, GateStatus::Fail);
        assert!(r.detail.contains("not eligible"));
    }

    #[test]
    fn d3_not_applicable_before_phase14_and_names_missing_fixtures() {
        let manifest = vec![manifest_row(
            12,
            "consensus 判定",
            "syn_does_not_exist_anywhere",
        )];
        let r = check_d3(&manifest, 0);
        assert_eq!(r.status, GateStatus::NotApplicable);
        assert!(r.detail.contains("syn_does_not_exist_anywhere"));
    }

    #[test]
    fn real_spec_d1_d2_pass_and_d3_is_not_applicable_at_phase_zero() {
        let text = real_spec();
        let section = section_69(&text);
        let mech_rows = extract_mechanism_registry_rows(&text);
        let manifest_rows = parse_manifest_table(section);
        assert_eq!(manifest_rows.len(), 8, "{manifest_rows:#?}");

        let d2 = check_d2(&mech_rows, &manifest_rows);
        assert_eq!(d2.status, GateStatus::Pass, "{}", d2.detail);
        let d1 = check_d1(&mech_rows, &manifest_rows);
        assert_eq!(d1.status, GateStatus::Pass, "{}", d1.detail);
        let d3 = check_d3(&manifest_rows, 0);
        assert_eq!(d3.status, GateStatus::NotApplicable, "{}", d3.detail);
    }

    // -- end-to-end over the real files ------------------------------------------------------

    #[test]
    fn real_files_check_all_at_phase_zero_has_exactly_the_expected_fail() {
        let results = check_all(&real_spec(), &real_registry(), 0);
        let fail_gates: Vec<&str> = results
            .iter()
            .filter(|r| r.status == GateStatus::Fail)
            .map(|r| r.gate.as_str())
            .collect();
        // Phase 0 五条 DoD 的 verifier 全部真实通过（DOD-002 已改配 test::humaux-domain::ids，
        // ADR-0001）；DOD-006.. 为 not_applicable（owner phase > 0），永不 fail。
        assert_eq!(fail_gates, Vec::<&str>::new(), "{results:#?}");
    }
}
