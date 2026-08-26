//! xtask `card-version` — §18.4 CI 闸: RetrievalCard's single version axis
//! (`card_builder_version`) must pair with exactly one `card_template_hash` workspace-wide,
//! and `card_template_hash` must actually match the template it claims to fingerprint.
//!
//! §18.4 frozen ruling: "同 `card_builder_version` 必同 `card_template_hash`，CI 按
//! `(card_builder_version, card_template_hash)` 去重，出现一对多即红" — `card_template_hash`
//! is not a second version axis, it is the *content fingerprint of that one builder version's
//! template* (`projection::card` module doc); two different hashes under one builder version
//! means the template changed without the version bumping, exactly the drift this gate exists
//! to catch.
//!
//! Three independent checks, combined in [`run`] (a workspace with a genuine second producer
//! failing any one of them fails the build):
//!
//! 1. [`dedupe_check`] — the textual pairing scan described above. On a workspace with only
//!    one producer of `CARD_BUILDER_VERSION`/`CARD_TEMPLATE_HASH` (this repo, today) this can
//!    only ever find one pairing and trivially passes; it exists for the day a second producer
//!    appears, and for a workspace-wide sweep of `card_template_version` name resurrection
//!    (see 3 below, which reuses this check's own file corpus).
//! 2. [`hash_derivation_check`] — calls the *real* linked `projection::card::assemble` (via
//!    `projection::card::template_fingerprint`) and asserts `CARD_TEMPLATE_HASH` still matches
//!    its output. This is the check that actually observes `assemble`'s body: 1 alone cannot —
//!    a single-file self-inconsistency (template edited, hash left stale) has nothing to pair
//!    against, so a from-scratch fault injection on `assemble`'s separator used to leave every
//!    check in this file green (T5.8 review finding).
//! 3. [`retired_name_check`] — §18.4: "旧名 `card_template_version` 全文作废". Scans the same
//!    file corpus `dedupe_check` walks for the retired name (either casing), excluding this
//!    checker's own file and the spec's own §18.4 prose (the one place the retired name is
//!    legitimately spelled out, to say it's retired).
//!
//! Scan domain for 1 and 3: every `.rs` file under the workspace (crates/ + xtask/, this file's
//! own source excluded — see [`SELF_FILE`]) and every `migrations/*.sql` file, plus the spec's
//! own `docs/architecture/Baseline_2.9.md` ([`SPEC_FILE`]). Same text-scan style as every other
//! xtask gate in this repo (`xtask/src/direction_table.rs`'s own doc makes the same point) —
//! judgment stays in §18.4, this file only cites it.

use std::fs;
use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

/// This gate's three-state verdict (§57.1: all gates pass/fail/not_applicable).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Pass(Vec<String>),
    Fail(Vec<String>),
    NotApplicable(String),
}

/// This checker's own path, relative to workspace root — excluded from the scan for the same
/// reason `architecture_check.rs::SELF_FILE` / `direction_table.rs::SELF_FILE` exclude
/// themselves: this file's own doc comment and `#[cfg(test)]` fixtures necessarily contain the
/// literal `CARD_BUILDER_VERSION`/`CARD_TEMPLATE_HASH` text the scan hunts for, in shapes (bare
/// prose mentions, tempdir-fixture source strings) this text-only scanner cannot tell apart
/// from a real `pub const` definition.
const SELF_FILE: &str = "xtask/src/card_version.rs";

/// The spec's own file, workspace-root-relative — §18.4's prose legitimately spells out both
/// `CARD_BUILDER_VERSION`/`CARD_TEMPLATE_HASH` and the retired `card_template_version` name (to
/// say the latter is retired), so it is excluded from [`retired_name_check`] the same way
/// [`SELF_FILE`] is excluded from the pairing scan.
const SPEC_FILE: &str = "docs/architecture/Baseline_2.9.md";

/// §18.4: "旧名 `card_template_version` 全文作废" — both casings, since Rust source would spell
/// a resurrected const `CARD_TEMPLATE_VERSION` while the spec's own prose uses snake_case.
const RETIRED_VERSION_NAMES: &[&str] = &["card_template_version", "CARD_TEMPLATE_VERSION"];

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn display(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}

/// Recursively collects every `.rs`/`.sql` file under `dir`, skipping `target`/`.git`/
/// `node_modules` — same exclusion list `direction_table.rs::walk_rs_files` uses, duplicated
/// here (private to this module, same rationale that module's own doc gives for not sharing
/// `architecture_check.rs`'s copy: each gate owns its own scan so one gate's fixture needs
/// never leak into another's).
fn walk_files(dir: &Path, out: &mut Vec<PathBuf>) {
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
            walk_files(&path, out);
        } else {
            let ext = path.extension().and_then(|e| e.to_str());
            if matches!(ext, Some("rs") | Some("sql")) {
                out.push(path);
            }
        }
    }
}

/// Every occurrence of `const_name = "VALUE";` (in either order relative to a type
/// annotation — `NAME: &str = "V"` or `NAME: &'static str = "V"`) in `source`, in appearance
/// order. Word-bounded so e.g. `MY_CARD_BUILDER_VERSION` never matches a search for
/// `CARD_BUILDER_VERSION`.
fn extract_const_str_values(source: &str, const_name: &str) -> Vec<String> {
    let bytes = source.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    while let Some(rel) = source[start..].find(const_name) {
        let idx = start + rel;
        let before_ok = idx == 0 || !is_ident_byte(bytes[idx - 1]);
        let after_idx = idx + const_name.len();
        let after_ok = after_idx >= bytes.len() || !is_ident_byte(bytes[after_idx]);
        if before_ok && after_ok {
            // From here, tolerate `: &str`, `: &'static str`, whitespace, before the `=`, then
            // a double-quoted literal (no escape handling needed — every real definition in
            // this workspace uses a plain literal, and a fixture that doesn't just fails to
            // match, which is a safe direction for this gate to fail in).
            let rest = &source[after_idx..];
            if let Some(eq_rel) = rest.find('=') {
                let between = &rest[..eq_rel];
                let between_ok = between.chars().all(|c| {
                    c.is_whitespace()
                        || c == ':'
                        || c == '&'
                        || c == '\''
                        || c.is_alphanumeric()
                        || c == '_'
                });
                if between_ok {
                    let after_eq = rest[eq_rel + 1..].trim_start();
                    if let Some(quoted) = after_eq.strip_prefix('"')
                        && let Some(end) = quoted.find('"')
                    {
                        out.push(quoted[..end].to_string());
                    }
                }
            }
        }
        start = idx + const_name.len();
    }
    out
}

/// One `(card_builder_version, card_template_hash)` pairing site, with its provenance file for
/// error messages.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Pairing {
    builder_version: String,
    template_hash: String,
    file: String,
}

/// Scans `files` for `CARD_BUILDER_VERSION`/`CARD_TEMPLATE_HASH` definitions. A file that
/// defines exactly one of each contributes one [`Pairing`]. A file whose counts don't match
/// 1:1 (and at least one side is non-zero) cannot be safely paired at all — reported back as an
/// `ambiguous` detail line rather than silently guessed at, same "don't collapse to a bare
/// count" discipline `direction_table.rs::compare` uses.
fn collect_pairings(root: &Path, files: &[PathBuf]) -> (Vec<Pairing>, Vec<String>) {
    let mut pairings = Vec::new();
    let mut ambiguous = Vec::new();
    for path in files {
        let disp = display(root, path);
        if disp == SELF_FILE {
            continue;
        }
        let Ok(source) = fs::read_to_string(path) else {
            continue;
        };
        let builders = extract_const_str_values(&source, "CARD_BUILDER_VERSION");
        let hashes = extract_const_str_values(&source, "CARD_TEMPLATE_HASH");
        match (builders.len(), hashes.len()) {
            (0, 0) => {}
            (1, 1) => pairings.push(Pairing {
                builder_version: builders.into_iter().next().unwrap(),
                template_hash: hashes.into_iter().next().unwrap(),
                file: disp,
            }),
            (b, h) => ambiguous.push(format!(
                "{disp}: {b} CARD_BUILDER_VERSION definition(s), {h} CARD_TEMPLATE_HASH \
                 definition(s) — cannot pair 1:1, fix so each file defines at most one of each"
            )),
        }
    }
    (pairings, ambiguous)
}

/// Pure §18.4 comparator: every `card_builder_version` value must map to exactly one
/// `card_template_hash` value across all of `pairings`. `ambiguous` (from
/// [`collect_pairings`]) is folded straight into a `Fail` if non-empty — an unparseable
/// pairing is not a pass, it is a shape this gate cannot vouch for.
fn dedupe_check(pairings: &[Pairing], ambiguous: &[String]) -> Verdict {
    if pairings.is_empty() && ambiguous.is_empty() {
        return Verdict::NotApplicable(
            "CARD_BUILDER_VERSION/CARD_TEMPLATE_HASH constants (not found anywhere in the \
             scanned workspace)"
                .to_string(),
        );
    }
    if !ambiguous.is_empty() {
        return Verdict::Fail(ambiguous.to_vec());
    }

    use std::collections::BTreeMap;
    let mut by_builder: BTreeMap<&str, Vec<&Pairing>> = BTreeMap::new();
    for p in pairings {
        by_builder
            .entry(p.builder_version.as_str())
            .or_default()
            .push(p);
    }

    let mut details = Vec::new();
    for (builder, ps) in &by_builder {
        let mut hashes: Vec<&str> = ps.iter().map(|p| p.template_hash.as_str()).collect();
        hashes.sort_unstable();
        hashes.dedup();
        if hashes.len() > 1 {
            let sites: Vec<String> = ps
                .iter()
                .map(|p| format!("{} (hash {:?}, {})", builder, p.template_hash, p.file))
                .collect();
            details.push(format!(
                "card_builder_version {builder:?} has {} distinct card_template_hash values: {}",
                hashes.len(),
                sites.join("; ")
            ));
        }
    }

    if details.is_empty() {
        let summary: Vec<String> = by_builder
            .iter()
            .map(|(builder, ps)| {
                format!(
                    "{builder:?} -> {:?} ({} site(s))",
                    ps[0].template_hash,
                    ps.len()
                )
            })
            .collect();
        Verdict::Pass(summary)
    } else {
        Verdict::Fail(details)
    }
}

/// The shared corpus for [`run_check`]'s pairing scan and [`retired_name_check`]: every
/// `.rs`/`.sql` file under `root` plus [`SPEC_FILE`] (a single named file, not part of the
/// recursive walk since it's `.md`).
fn scan_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    walk_files(root, &mut files);
    let spec = root.join(SPEC_FILE);
    if spec.is_file() {
        files.push(spec);
    }
    files
}

fn run_check(root: &Path) -> Verdict {
    // §18.4 "扫代码/迁移/spec" — even though §18.4's own prose is expected to contribute zero
    // pairs (see module doc).
    let files = scan_files(root);
    let (pairings, ambiguous) = collect_pairings(root, &files);
    dedupe_check(&pairings, &ambiguous)
}

/// Pure comparator: `computed` is what [`hash_derivation_check`] recomputes from `assemble`'s
/// real current output, `declared` is the literal [`humaux_projection::card::CARD_TEMPLATE_HASH`]
/// value. Split out so the drift case is testable without needing a second, differently-broken
/// build of `projection::card` linked in (see the tempdir-fixture-style tests below).
fn compare_template_hash(computed: &str, declared: &str) -> Verdict {
    if computed == declared {
        Verdict::Pass(vec![format!(
            "CARD_TEMPLATE_HASH {declared:?} matches template_fingerprint()'s recomputation of \
             assemble()'s current output"
        )])
    } else {
        Verdict::Fail(vec![format!(
            "CARD_TEMPLATE_HASH is {declared:?} but assemble()'s current golden output hashes \
             to {computed:?} — the template changed (or CARD_TEMPLATE_HASH was hand-edited) \
             without CARD_TEMPLATE_HASH being re-derived and pasted (§18.4)"
        )])
    }
}

/// §18.4 blocker (T5.8 review): `dedupe_check` alone can never observe `assemble`'s body — a
/// single-file drift (template edited, hash left stale) has no second producer to pair
/// against. This calls the *real* `humaux_projection::card::template_fingerprint()` (which
/// calls the real `assemble`) and compares it against the real `CARD_TEMPLATE_HASH` constant,
/// so a fault-injected `assemble` edit fails the build here even with only one producer in the
/// workspace.
fn hash_derivation_check() -> Verdict {
    compare_template_hash(
        &humaux_projection::card::template_fingerprint(),
        humaux_projection::card::CARD_TEMPLATE_HASH,
    )
}

/// §18.4: "旧名 `card_template_version` 全文作废". Scans `files` (the same corpus
/// [`run_check`] walks) for [`RETIRED_VERSION_NAMES`], skipping this checker's own file and
/// [`SPEC_FILE`]'s legitimate retired-name prose.
fn retired_name_check(root: &Path, files: &[PathBuf]) -> Verdict {
    let mut hits = Vec::new();
    for path in files {
        let disp = display(root, path);
        if disp == SELF_FILE || disp == SPEC_FILE {
            continue;
        }
        let Ok(source) = fs::read_to_string(path) else {
            continue;
        };
        for name in RETIRED_VERSION_NAMES {
            if source.contains(name) {
                hits.push(format!(
                    "{disp}: contains retired name {name:?} (§18.4 作废)"
                ));
            }
        }
    }
    if hits.is_empty() {
        Verdict::Pass(vec![
            "retired name card_template_version/CARD_TEMPLATE_VERSION not found outside §18.4 \
             spec prose"
                .to_string(),
        ])
    } else {
        Verdict::Fail(hits)
    }
}

pub fn run(_args: &[String]) -> i32 {
    let root = workspace_root();
    let files = scan_files(&root);

    let checks: [(&str, Verdict); 3] = [
        ("dedupe", run_check(&root)),
        ("hash-derivation", hash_derivation_check()),
        ("retired-name", retired_name_check(&root, &files)),
    ];

    let mut any_fail = false;
    for (name, verdict) in &checks {
        match verdict {
            Verdict::Pass(summary) => {
                println!("card-version[{name}]: pass");
                for line in summary {
                    println!("  {line}");
                }
            }
            Verdict::Fail(details) => {
                any_fail = true;
                eprintln!("card-version[{name}]: fail");
                for d in details {
                    eprintln!("  {d}");
                }
            }
            Verdict::NotApplicable(missing) => {
                println!("card-version[{name}]: not_applicable (missing object: {missing})");
            }
        }
    }

    if any_fail {
        eprintln!(
            "card-version: fail — §18.4 (card_builder_version, card_template_hash) invariant \
             violated"
        );
        1
    } else {
        println!("card-version: pass — §18.4 version axis invariant holds");
        0
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
    fn extract_const_str_values_finds_ampersand_str_definition() {
        let src = "pub const CARD_BUILDER_VERSION: &str = \"v1\";\n";
        assert_eq!(
            extract_const_str_values(src, "CARD_BUILDER_VERSION"),
            vec!["v1".to_string()]
        );
    }

    #[test]
    fn extract_const_str_values_finds_static_str_definition() {
        let src = "pub const CARD_TEMPLATE_HASH: &'static str = \"abc\";\n";
        assert_eq!(
            extract_const_str_values(src, "CARD_TEMPLATE_HASH"),
            vec!["abc".to_string()]
        );
    }

    #[test]
    fn extract_const_str_values_is_word_bounded() {
        let src = "pub const MY_CARD_BUILDER_VERSION: &str = \"v1\";\n";
        assert!(extract_const_str_values(src, "CARD_BUILDER_VERSION").is_empty());
    }

    #[test]
    fn extract_const_str_values_ignores_non_string_assignment() {
        let src = "let CARD_BUILDER_VERSION_COUNT: u32 = 3;\n";
        assert!(extract_const_str_values(src, "CARD_BUILDER_VERSION").is_empty());
    }

    #[test]
    fn dedupe_check_not_applicable_when_nothing_found() {
        assert_eq!(
            dedupe_check(&[], &[]),
            Verdict::NotApplicable(
                "CARD_BUILDER_VERSION/CARD_TEMPLATE_HASH constants (not found anywhere in the \
                 scanned workspace)"
                    .to_string()
            )
        );
    }

    #[test]
    fn dedupe_check_pass_on_single_pairing() {
        let pairings = vec![Pairing {
            builder_version: "v1".to_string(),
            template_hash: "h1".to_string(),
            file: "a.rs".to_string(),
        }];
        assert!(matches!(dedupe_check(&pairings, &[]), Verdict::Pass(_)));
    }

    #[test]
    fn dedupe_check_pass_when_same_builder_same_hash_from_multiple_sites() {
        let pairings = vec![
            Pairing {
                builder_version: "v1".into(),
                template_hash: "h1".into(),
                file: "a.rs".into(),
            },
            Pairing {
                builder_version: "v1".into(),
                template_hash: "h1".into(),
                file: "b.sql".into(),
            },
        ];
        assert!(matches!(dedupe_check(&pairings, &[]), Verdict::Pass(_)));
    }

    #[test]
    fn dedupe_check_fail_on_one_to_many() {
        let pairings = vec![
            Pairing {
                builder_version: "v1".into(),
                template_hash: "h1".into(),
                file: "a.rs".into(),
            },
            Pairing {
                builder_version: "v1".into(),
                template_hash: "h2".into(),
                file: "b.rs".into(),
            },
        ];
        let v = dedupe_check(&pairings, &[]);
        assert!(
            matches!(&v, Verdict::Fail(d) if d.iter().any(|l| l.contains("v1") && l.contains("2 distinct"))),
            "{v:?}"
        );
    }

    #[test]
    fn dedupe_check_fail_on_ambiguous_pairing_takes_priority() {
        let pairings = vec![Pairing {
            builder_version: "v1".into(),
            template_hash: "h1".into(),
            file: "a.rs".into(),
        }];
        let ambiguous = vec!["b.rs: 2 CARD_BUILDER_VERSION definition(s), 1 ...".to_string()];
        assert!(matches!(
            dedupe_check(&pairings, &ambiguous),
            Verdict::Fail(_)
        ));
    }

    #[test]
    fn dedupe_check_distinct_builder_versions_each_with_one_hash_is_pass() {
        let pairings = vec![
            Pairing {
                builder_version: "v1".into(),
                template_hash: "h1".into(),
                file: "a.rs".into(),
            },
            Pairing {
                builder_version: "v2".into(),
                template_hash: "h2".into(),
                file: "b.rs".into(),
            },
        ];
        assert!(matches!(dedupe_check(&pairings, &[]), Verdict::Pass(_)));
    }

    // -- real repo -------------------------------------------------------------------------

    /// §80.1 准入条件: the real repo must be green today, not only on a hand-built fixture —
    /// `projection::card`'s `CARD_BUILDER_VERSION`/`CARD_TEMPLATE_HASH` (T5.7) are the one real
    /// pairing site this should find.
    #[test]
    fn real_repo_card_version_is_green() {
        let verdict = run_check(&real_root());
        assert!(matches!(verdict, Verdict::Pass(_)), "{verdict:?}");
    }

    #[test]
    fn real_repo_finds_the_projection_card_pairing() {
        let mut files = Vec::new();
        walk_files(&real_root(), &mut files);
        let (pairings, ambiguous) = collect_pairings(&real_root(), &files);
        assert!(ambiguous.is_empty(), "{ambiguous:?}");
        assert!(
            pairings
                .iter()
                .any(|p| p.file.ends_with("crates/projection/src/card.rs")),
            "expected a pairing from crates/projection/src/card.rs, got {pairings:?}"
        );
    }

    /// §18.4 blocker (T5.8 review): the real `CARD_TEMPLATE_HASH` must match `assemble`'s
    /// real current output today. This is the check `real_repo_card_version_is_green` cannot
    /// exercise — `dedupe_check` never reads `assemble`'s body at all.
    #[test]
    fn real_repo_hash_derivation_is_green() {
        let verdict = hash_derivation_check();
        assert!(matches!(verdict, Verdict::Pass(_)), "{verdict:?}");
    }

    #[test]
    fn real_repo_has_no_retired_version_name() {
        let root = real_root();
        let files = scan_files(&root);
        let verdict = retired_name_check(&root, &files);
        assert!(matches!(verdict, Verdict::Pass(_)), "{verdict:?}");
    }

    // -- hash_derivation_check: pure comparator fault injection ------------------------------

    #[test]
    fn compare_template_hash_pass_when_equal() {
        assert!(matches!(
            compare_template_hash("v1-abc", "v1-abc"),
            Verdict::Pass(_)
        ));
    }

    /// The exact fault §18.4 names: template edited (hash recomputes differently) without
    /// `CARD_TEMPLATE_HASH` being bumped to match ⇒ red.
    #[test]
    fn compare_template_hash_fail_when_computed_diverges_from_declared() {
        let v = compare_template_hash("v1-freshly-computed", "v1-stale-declared");
        assert!(
            matches!(&v, Verdict::Fail(d) if d.iter().any(|l| l.contains("stale-declared") && l.contains("freshly-computed"))),
            "{v:?}"
        );
    }

    // -- retired_name_check: tempdir fixtures -------------------------------------------------

    #[test]
    fn retired_name_check_clean_tree_is_pass() {
        let tmp = tempdir("retired-clean");
        fs::write(
            tmp.join("a.rs"),
            "pub const CARD_BUILDER_VERSION: &str = \"v1\";\n",
        )
        .unwrap();
        let mut files = Vec::new();
        walk_files(&tmp, &mut files);
        let verdict = retired_name_check(&tmp, &files);
        assert!(matches!(verdict, Verdict::Pass(_)), "{verdict:?}");
        fs::remove_dir_all(&tmp).ok();
    }

    /// §18.4's resurrection fault: the retired name reappears somewhere ⇒ red, even though
    /// `dedupe_check` (which never looks for this name) would stay green on the same tree.
    #[test]
    fn retired_name_check_resurrected_name_is_red() {
        let tmp = tempdir("retired-resurrected");
        fs::write(
            tmp.join("a.rs"),
            "pub const CARD_TEMPLATE_VERSION: &str = \"v2\";\n",
        )
        .unwrap();
        let mut files = Vec::new();
        walk_files(&tmp, &mut files);
        let verdict = retired_name_check(&tmp, &files);
        assert!(matches!(&verdict, Verdict::Fail(_)), "{verdict:?}");
        fs::remove_dir_all(&tmp).ok();
    }

    /// snake_case spelling (as the spec's own prose uses it) is caught too, not just the
    /// SCREAMING_SNAKE Rust-const spelling.
    #[test]
    fn retired_name_check_snake_case_spelling_is_also_red() {
        let tmp = tempdir("retired-snake-case");
        fs::write(
            tmp.join("a.sql"),
            "-- refers to card_template_version here\n",
        )
        .unwrap();
        let mut files = Vec::new();
        walk_files(&tmp, &mut files);
        let verdict = retired_name_check(&tmp, &files);
        assert!(matches!(verdict, Verdict::Fail(_)), "{verdict:?}");
        fs::remove_dir_all(&tmp).ok();
    }

    // -- tempdir fixtures: 造一对多 ⇒ 红; 单对 ⇒ 绿 -----------------------------------------

    fn tempdir(name: &str) -> PathBuf {
        let tmp = std::env::temp_dir().join(format!(
            "card-version-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&tmp).unwrap();
        tmp
    }

    /// Single pairing site ⇒ green (the "单对 ⇒ 绿" half of the task's own acceptance line).
    #[test]
    fn tempdir_fixture_single_pair_is_green() {
        let tmp = tempdir("single-pair");
        fs::write(
            tmp.join("card.rs"),
            "pub const CARD_BUILDER_VERSION: &str = \"v1\";\n\
             pub const CARD_TEMPLATE_HASH: &str = \"deadbeef\";\n",
        )
        .unwrap();

        let verdict = run_check(&tmp);
        assert!(matches!(verdict, Verdict::Pass(_)), "{verdict:?}");
        fs::remove_dir_all(&tmp).ok();
    }

    /// Two files pairing the *same* `card_builder_version` with two *different*
    /// `card_template_hash` values ⇒ red (the "造一对多 ⇒ 红" half). This is the shape §18.4
    /// itself describes as the violation: "同 `card_builder_version` 必同 `card_template_hash`
    /// ... 出现一对多即红".
    #[test]
    fn tempdir_fixture_one_builder_version_two_hashes_is_red() {
        let tmp = tempdir("one-to-many");
        fs::write(
            tmp.join("a.rs"),
            "pub const CARD_BUILDER_VERSION: &str = \"v1\";\n\
             pub const CARD_TEMPLATE_HASH: &str = \"hash-a\";\n",
        )
        .unwrap();
        fs::write(
            tmp.join("b.rs"),
            "pub const CARD_BUILDER_VERSION: &str = \"v1\";\n\
             pub const CARD_TEMPLATE_HASH: &str = \"hash-b\";\n",
        )
        .unwrap();

        let verdict = run_check(&tmp);
        assert!(
            matches!(&verdict, Verdict::Fail(d) if d.iter().any(|l| l.contains("v1"))),
            "{verdict:?}"
        );
        fs::remove_dir_all(&tmp).ok();
    }

    /// Two *different* builder versions, each internally consistent, is not the violation
    /// §18.4 names — must stay green (proves the gate compares hashes *within* one builder
    /// version, not across all versions ever seen).
    #[test]
    fn tempdir_fixture_two_distinct_builder_versions_each_consistent_is_green() {
        let tmp = tempdir("two-versions");
        fs::write(
            tmp.join("a.rs"),
            "pub const CARD_BUILDER_VERSION: &str = \"v1\";\n\
             pub const CARD_TEMPLATE_HASH: &str = \"hash-a\";\n",
        )
        .unwrap();
        fs::write(
            tmp.join("b.rs"),
            "pub const CARD_BUILDER_VERSION: &str = \"v2\";\n\
             pub const CARD_TEMPLATE_HASH: &str = \"hash-b\";\n",
        )
        .unwrap();

        let verdict = run_check(&tmp);
        assert!(matches!(verdict, Verdict::Pass(_)), "{verdict:?}");
        fs::remove_dir_all(&tmp).ok();
    }

    /// Empty tempdir (no definitions at all) ⇒ `not_applicable`, never a silent pass/fail —
    /// §57.1 rule 2.
    #[test]
    fn tempdir_fixture_empty_is_not_applicable() {
        let tmp = tempdir("empty");
        let verdict = run_check(&tmp);
        assert!(matches!(verdict, Verdict::NotApplicable(_)), "{verdict:?}");
        fs::remove_dir_all(&tmp).ok();
    }
}
