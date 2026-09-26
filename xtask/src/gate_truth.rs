//! xtask `gate-truth <chain.log>` — a green chain means the DB tests ran (ADR-0050 D-C,
//! Baseline 2.9 §79.2 「跳过不等于通过」, audit P1-12 / TH-1).
//!
//! Reads a gate-chain log, pairs every `Running tests/<stem>.rs (…)` line with the libtest
//! `test result: … N passed; … finished in X s` line that follows it (unit-test and doc-test
//! results, and result lines with no preceding `Running tests/` line, are ignored), and flags
//! every DB-bound binary that reports `N > 0` passed in under [`FLOOR_SECS`] — the shape of a
//! whole binary that skipped because its fixture could not reach a database.
//!
//! The DB-bound set is derived, never hand-listed: an integration target (`{crates,bins}/*/
//! tests/*.rs`, `xtask/tests/*.rs`) is DB-bound when its source, or any file it pulls in with
//! `#[path = "…"] mod`, contains one of [`DB_MARKERS`]. The same stem in two packages is
//! DB-bound if either is. Vacuous guards: zero parsed results or an empty DB-bound set exit 1.
//!
//! depends-on: the chain log file (argument) and the repo's test sources (read-only); no DB.
//! called-by: `gates_card.sh` as its LAST gate (`gate_truth`, reads the chain's own `$LOG`).
//! invariants: only whole-binary skips are visible here; partial skips are caught by the
//! chain's `HUMAUX_REQUIRE_*` declarations (ADR-0050 D-A). Upgrade path if a real DB binary
//! ever runs under the floor: a skip ledger written by `skip_or_fail`, not a higher floor.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

/// A DB-bound binary that passed tests faster than this did not touch a database. The
/// fastest genuine DB binary on card 24 was `facet_contract` at 0.06 s.
const FLOOR_SECS: f64 = 0.05;

/// Source markers of a `humaux_testkit` DB/Qdrant fixture.
const DB_MARKERS: [&str; 4] = [
    "run_db_fixture",
    "DbIntegrationFixture",
    "ExternalDep::Postgres",
    "ExternalDep::Qdrant",
];

/// One integration-test binary result: `(stem, passed, secs)`.
type BinResult = (String, u64, f64);

pub fn run(args: &[String]) -> i32 {
    let Some(log) = args.first() else {
        eprintln!("usage: cargo xtask gate-truth <chain.log>");
        return 2;
    };
    let text = match fs::read_to_string(log) {
        Ok(text) => text,
        Err(e) => {
            eprintln!("gate-truth: fail (cannot read {log}: {e})");
            return 1;
        }
    };
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let results = parse_results(&text);
    let db_bound = db_bound_stems(&root);
    if results.is_empty() {
        eprintln!("gate-truth: fail (vacuous: no integration-test result in {log})");
        return 1;
    }
    if db_bound.is_empty() {
        eprintln!("gate-truth: fail (vacuous: derived an empty DB-bound set)");
        return 1;
    }
    let bad = offenders(&results, &db_bound);
    if !bad.is_empty() {
        for (stem, passed, secs) in &bad {
            eprintln!(
                "gate-truth: offender {stem}: {passed} passed in {secs:.2}s — a DB-bound binary \
                 that cannot have reached its database"
            );
        }
        eprintln!("gate-truth: fail ({} offenders)", bad.len());
        return 1;
    }
    let seen: Vec<&BinResult> = results
        .iter()
        .filter(|(stem, _, _)| db_bound.contains(stem))
        .collect();
    eprintln!(
        "gate-truth: pass ({} binaries, {} DB-bound, 0 offenders)",
        results.len(),
        seen.len()
    );
    for (stem, passed, secs) in seen {
        eprintln!("  {stem}: {passed} passed in {secs:.2}s");
    }
    0
}

/// Every `Running tests/<stem>.rs` paired with its next `test result:` line.
fn parse_results(text: &str) -> Vec<BinResult> {
    let mut out = Vec::new();
    let mut current: Option<String> = None;
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("Running ") {
            current = rest
                .strip_prefix("tests/")
                .and_then(|p| p.split_whitespace().next())
                .and_then(|p| Path::new(p).file_stem())
                .map(|s| s.to_string_lossy().into_owned());
        } else if line.starts_with("Doc-tests ") {
            current = None;
        } else if line.starts_with("test result:") {
            let Some(stem) = current.take() else { continue };
            let passed = line
                .split(';')
                .find_map(|part| part.trim().rsplit_once(' ').filter(|(_, w)| *w == "passed"))
                .and_then(|(head, _)| head.rsplit(' ').next()?.parse().ok());
            let secs = line
                .rsplit_once("finished in ")
                .and_then(|(_, t)| t.trim().trim_end_matches('s').parse().ok());
            if let (Some(passed), Some(secs)) = (passed, secs) {
                out.push((stem, passed, secs));
            }
        }
    }
    out
}

/// Stems of every integration-test target whose source (or a `#[path]` module it includes)
/// contains a [`DB_MARKERS`] entry.
fn db_bound_stems(root: &Path) -> BTreeSet<String> {
    let mut dirs: Vec<PathBuf> = ["crates", "bins"]
        .iter()
        .filter_map(|group| fs::read_dir(root.join(group)).ok())
        .flat_map(|entries| entries.flatten().map(|e| e.path().join("tests")))
        .collect();
    dirs.push(root.join("xtask/tests"));
    let mut out = BTreeSet::new();
    for dir in dirs {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for path in entries.flatten().map(|e| e.path()) {
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            if is_db_bound(&path, &mut BTreeSet::new())
                && let Some(stem) = path.file_stem()
            {
                out.insert(stem.to_string_lossy().into_owned());
            }
        }
    }
    out
}

/// `file` or any `#[path = "…"]` module it names (recursively) contains a marker.
fn is_db_bound(file: &Path, visited: &mut BTreeSet<PathBuf>) -> bool {
    if !visited.insert(file.to_path_buf()) {
        return false;
    }
    let Ok(src) = fs::read_to_string(file) else {
        return false;
    };
    if DB_MARKERS.iter().any(|m| src.contains(m)) {
        return true;
    }
    let dir = file.parent().unwrap_or(Path::new("."));
    src.lines()
        .filter_map(|l| l.trim().strip_prefix("#[path = \""))
        .filter_map(|rest| rest.split_once('"').map(|(p, _)| dir.join(p)))
        .any(|module| is_db_bound(&module, visited))
}

fn offenders(results: &[BinResult], db_bound: &BTreeSet<String>) -> Vec<BinResult> {
    results
        .iter()
        .filter(|(stem, passed, secs)| db_bound.contains(stem) && *passed > 0 && *secs < FLOOR_SECS)
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(stems: &[&str]) -> BTreeSet<String> {
        stems.iter().map(|s| s.to_string()).collect()
    }

    fn block(stem: &str, passed: u64, secs: &str) -> String {
        format!(
            "     Running tests/{stem}.rs (/t/debug/deps/{stem}-0123456789abcdef)\n\n\
             running {passed} tests\n\
             test result: ok. {passed} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; \
             finished in {secs}s\n\n"
        )
    }

    fn repo_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
    }

    #[test]
    fn gate_truth_flags_db_bound_binary_passing_under_50ms() {
        let results = parse_results(&block("provider_budget", 15, "0.00"));
        assert_eq!(results, vec![("provider_budget".to_string(), 15, 0.0)]);
        let bad = offenders(&results, &set(&["provider_budget"]));
        assert_eq!(bad.len(), 1);
    }

    #[test]
    fn gate_truth_ignores_fast_non_db_bound_binary() {
        let results = parse_results(&block("qdrant_contract", 35, "0.00"));
        assert!(offenders(&results, &set(&["provider_budget"])).is_empty());
    }

    #[test]
    fn gate_truth_ignores_db_bound_binary_with_zero_passed_or_failed_result() {
        let log = format!(
            "{}     Running tests/g80_31_handoff.rs (/t/g80-0123456789abcdef)\n\
             test result: FAILED. 0 passed; 11 failed; 0 ignored; 0 measured; 0 filtered out; \
             finished in 0.00s\n",
            block("provider_budget", 0, "0.00")
        );
        let results = parse_results(&log);
        assert_eq!(results.len(), 2, "{results:?}");
        assert!(offenders(&results, &set(&["provider_budget", "g80_31_handoff"])).is_empty());
    }

    #[test]
    fn gate_truth_fails_on_log_with_no_integration_results() {
        // Unit-test and doc-test results, and a result line with no `Running tests/` before
        // it (the serial lane's nested output), are not integration results.
        let log = "     Running unittests src/lib.rs (/t/humaux_domain-0123456789abcdef)\n\
                   test result: ok. 222 passed; 0 failed; finished in 0.01s\n\
                   Doc-tests humaux_domain\n\
                   test result: ok. 3 passed; 0 failed; finished in 0.00s\n\
                   test result: ok. 9 passed; 0 failed; finished in 0.00s\n";
        assert!(parse_results(log).is_empty());
        let dir = std::env::temp_dir().join(format!("gate_truth_empty_{}", std::process::id()));
        fs::write(&dir, log).unwrap();
        assert_eq!(
            run(&[dir.display().to_string()]),
            1,
            "vacuous log must be red"
        );
        let _ = fs::remove_file(&dir);
    }

    #[test]
    fn gate_truth_derives_db_bound_set_from_sources_and_path_modules() {
        let stems = db_bound_stems(&repo_root());
        for fixture in [
            "g80_31_handoff",
            "provider_budget",
            "mandatory_context_lane",
            "retrieval_query_sources",
        ] {
            assert!(stems.contains(fixture), "{fixture} must be DB-bound");
        }
        assert!(
            !stems.contains("qdrant_contract"),
            "a pure contract test is not DB-bound"
        );
        assert!(!stems.contains("contribution_scan"));
        assert!(
            !stems.iter().any(|s| s.ends_with("_fixture")),
            "tests/support/** modules are never targets: {stems:?}"
        );
        // `#[path]` following: operation_receipts' fixture lives in support/.
        assert!(stems.contains("operation_receipts"));
    }

    #[test]
    fn gate_truth_same_stem_in_two_packages_is_db_bound_if_either_is() {
        let root = std::env::temp_dir().join(format!("gate_truth_stems_{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        for (pkg, body) in [
            (
                "crates/a",
                "fn t() { humaux_testkit::run_db_fixture::<F, _>(\"x\", |_| ()); }",
            ),
            ("bins/b", "fn t() {}"),
        ] {
            fs::create_dir_all(root.join(pkg).join("tests/support")).unwrap();
            fs::write(root.join(pkg).join("tests/shared_stem.rs"), body).unwrap();
        }
        // A marker reached only through `#[path]` counts; the support file is not a target.
        fs::write(
            root.join("bins/b/tests/support/fx.rs"),
            "impl DbIntegrationFixture for F {}",
        )
        .unwrap();
        fs::write(
            root.join("bins/b/tests/via_path.rs"),
            "#[path = \"support/fx.rs\"]\nmod fx;\n",
        )
        .unwrap();
        let stems = db_bound_stems(&root);
        assert_eq!(stems, set(&["shared_stem", "via_path"]));
        let _ = fs::remove_dir_all(&root);
    }

    /// Replay of card 24's final chain (`gates_card24_final3.log`, excerpt): the four
    /// 61719-pinned fixtures are flagged, and nothing else — including the fastest genuine DB
    /// binary (`facet_contract`, 0.06 s) and fast non-DB binaries.
    #[test]
    fn gate_truth_card24_log_excerpt_flags_exactly_the_four_fixtures() {
        let log = [
            block("facet_contract", 3, "0.06"),
            block("g80_31_handoff", 11, "0.00"),
            block("mandatory_context_lane", 6, "0.00"),
            block("provider_budget", 15, "0.00"),
            block("qdrant_contract", 35, "0.00"),
            block("retrieval_query_sources", 3, "0.00"),
            block("contribution_scan", 1, "0.00"),
            block("operation_receipts", 9, "4.37"),
            block("fault_main", 10, "0.00"),
        ]
        .concat();
        let bad: Vec<String> = offenders(&parse_results(&log), &db_bound_stems(&repo_root()))
            .into_iter()
            .map(|(stem, _, _)| stem)
            .collect();
        assert_eq!(
            bad,
            vec![
                "g80_31_handoff",
                "mandatory_context_lane",
                "provider_budget",
                "retrieval_query_sources"
            ]
        );
    }
}
