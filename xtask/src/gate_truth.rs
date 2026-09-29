//! `xtask::gate_truth` — `cargo xtask gate-truth <chain.log>`: a green chain means the DB tests ran (ADR-0050 D-C).
//! Depends-on: crates=[]; services=[fs(chain log)]; env=[CARGO_MANIFEST_DIR]; modules=[xtask::dep_map]
//! Called-by: [xtask::main]
//! Invariants: [read-only; an unreadable log, zero parsed results or an empty DB-bound set exit 1 (vacuous is
//!   red, never green); only whole-binary skips are visible here — partial skips are caught by the chain's
//!   `HUMAUX_REQUIRE_*` declarations (ADR-0050 D-A)]
//! Spec: Baseline §79.2; ADR-0050; ADR-0051
//!
//! Baseline 2.9 §79.2 「跳过不等于通过」, audit P1-12 / TH-1.
//!
//! Reads a gate-chain log, pairs every `Running tests/<stem>.rs (…)` line with the libtest
//! `test result: … N passed; … finished in X s` line that follows it (unit-test and doc-test
//! results, and result lines with no preceding `Running tests/` line, are ignored), and flags
//! every DB-bound binary that reports `N > 0` passed in under [`FLOOR_SECS`] — the shape of a
//! whole binary that skipped because its fixture could not reach a database.
//!
//! The DB-bound set is derived, never hand-listed, from the same computed facts that render
//! `dependency_map.md` §Test binaries (ADR-0051 D-K): an integration target (`{crates,bins}/*/
//! tests/*.rs`, `xtask/tests/*.rs`) is DB-bound when [`dep_map::test_target_services`] finds that
//! its **default run** (the chain's `cargo test`, no `--ignored`) reaches PostgreSQL or Qdrant —
//! a call-site pattern (`Client::connect(`, `DbPool::connect(`, `.begin()`, `IntraCellRequest {`,
//! …), a `HUMAUX_TEST_PG_DSN` / `HUMAUX_TEST_QDRANT_PORT` literal or a testkit fixture marker
//! inside a non-`#[ignore]` test or anything it names (local items, `#[path]` modules). So a
//! raw-DSN binary with no testkit marker is covered, and a binary whose only DB tests are lane
//! tests is not DB-bound for this run. The `ignored` count excuses nothing. The same stem in two
//! packages is DB-bound if either is. Vacuous guards: zero parsed results or an empty DB-bound
//! set exit 1.
//!
//! Upgrade path if a real DB binary ever runs under the floor: a skip ledger written by
//! `skip_or_fail`, not a higher floor.
//!
//! Card 27 (2026-09-29): the fixed 0.05 s floor flagged `facet_contract` (4 real DB tests in
//! 0.04 s) on a warm run, exactly the flake ADR-0050 predicted. The floor is now the SMALLER
//! of the fixed floor and [`PER_TEST_FLOOR_SECS`] × passed: a skipped test costs microseconds,
//! a real one at least one PostgreSQL round trip, so a binary whose passed tests average under
//! 3 ms each did not reach its database. The fixed floor still bounds large binaries. A real
//! single-test binary that finishes under 3 ms would still be flagged — the ledger stays the
//! upgrade path for that case.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use crate::dep_map::{self, Service};

/// A DB-bound binary that passed tests faster than this did not touch a database. The
/// fastest genuine DB binary on card 24 was `facet_contract` at 0.06 s.
const FLOOR_SECS: f64 = 0.05;

/// Per-passed-test floor (card 27): one PostgreSQL round trip is ≥ ~3 ms on the loopback dev
/// cluster; a skipped test costs microseconds. The effective floor is
/// `min(FLOOR_SECS, PER_TEST_FLOOR_SECS * passed)`.
const PER_TEST_FLOOR_SECS: f64 = 0.003;

/// The floor a binary with `passed` tests must exceed to count as having reached its database.
fn floor_for(passed: u64) -> f64 {
    FLOOR_SECS.min(PER_TEST_FLOOR_SECS * passed as f64)
}

/// One integration-test binary result: `(stem, passed, ignored, secs)`.
type BinResult = (String, u64, u64, f64);

pub fn run(args: &[String]) -> i32 {
    let Some(log) = args.first() else {
        eprintln!("usage: cargo xtask gate-truth <chain.log>");
        return 2;
    };
    // dep: fs(chain log) — reads the gate-chain log named on the command line
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
        for (stem, passed, _, secs) in &bad {
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
        .filter(|(stem, ..)| db_bound.contains(stem))
        .collect();
    eprintln!(
        "gate-truth: pass ({} binaries, {} DB-bound, 0 offenders)",
        results.len(),
        seen.len()
    );
    for (stem, passed, _, secs) in seen {
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
            let ignored = line
                .split(';')
                .find_map(|part| part.trim().strip_suffix(" ignored"))
                .and_then(|n| n.rsplit(' ').next()?.parse().ok())
                .unwrap_or(0);
            let secs = line
                .rsplit_once("finished in ")
                .and_then(|(_, t)| t.trim().trim_end_matches('s').parse().ok());
            if let (Some(passed), Some(secs)) = (passed, secs) {
                out.push((stem, passed, ignored, secs));
            }
        }
    }
    out
}

/// Stems of every integration-test target that reaches PostgreSQL or Qdrant
/// ([`dep_map::test_target_services`]).
fn db_bound_stems(root: &Path) -> BTreeSet<String> {
    dep_map::test_target_services(root)
        .into_iter()
        .filter(|(_, services)| {
            services.contains(&Service::PostgreSQL) || services.contains(&Service::Qdrant)
        })
        .map(|(stem, _)| stem)
        .collect()
}

fn is_fast_db_pass((stem, passed, _, secs): &BinResult, db_bound: &BTreeSet<String>) -> bool {
    db_bound.contains(stem) && *passed > 0 && *secs < floor_for(*passed)
}

/// Fast passes of binaries whose default run is DB-bound — the whole-binary skip shape. The
/// `ignored` count excuses nothing: a binary whose passing tests are pure is not DB-bound in
/// the first place ([`dep_map::test_target_services`] judges the non-`#[ignore]` tests only).
fn offenders(results: &[BinResult], db_bound: &BTreeSet<String>) -> Vec<BinResult> {
    results
        .iter()
        .filter(|r| is_fast_db_pass(r, db_bound))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

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
        assert_eq!(results, vec![("provider_budget".to_string(), 15, 0, 0.0)]);
        let bad = offenders(&results, &set(&["provider_budget"]));
        assert_eq!(bad.len(), 1);
    }

    /// Card 27: `facet_contract` really ran 4 DB tests in 0.04 s on a warm run (under the fixed
    /// 0.05 s floor) — a per-test floor keeps it green while a 4-test binary at 0.00 s stays red.
    #[test]
    fn gate_truth_per_test_floor_keeps_a_fast_genuine_small_binary_green() {
        let real = parse_results(&block("facet_contract", 4, "0.04"));
        assert!(offenders(&real, &set(&["facet_contract"])).is_empty());
        let skipped = parse_results(&block("facet_contract", 4, "0.00"));
        assert_eq!(offenders(&skipped, &set(&["facet_contract"])).len(), 1);
        // The fixed floor still bounds large binaries: 40 tests in 0.04 s is 1 ms per test.
        let big = parse_results(&block("g80_31_handoff", 40, "0.04"));
        assert_eq!(offenders(&big, &set(&["g80_31_handoff"])).len(), 1);
        assert!((floor_for(4) - 0.012).abs() < 1e-9);
        assert!((floor_for(40) - 0.05).abs() < 1e-9);
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

    /// ADR-0051 D-K: a test that reads `HUMAUX_TEST_PG_DSN` itself and connects with a raw
    /// `Client::connect(` — no testkit marker anywhere — is DB-bound through the dep-map facts;
    /// the old four-marker scan missed exactly this shape. A mention in a comment is not a fact.
    #[test]
    fn db_bound_set_includes_raw_dsn_target_via_dep_map() {
        let root = std::env::temp_dir().join(format!("gate_truth_raw_dsn_{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("crates/a/tests")).unwrap();
        fs::write(
            root.join("crates/a/tests/raw_dsn.rs"),
            "#[test]\nfn t() {\n    let dsn = std::env::var(\"HUMAUX_TEST_PG_DSN\").unwrap();\n    \
             let _c = postgres::Client::connect(&dsn, postgres::NoTls);\n}\n",
        )
        .unwrap();
        fs::write(
            root.join("crates/a/tests/comment_only.rs"),
            "// run_db_fixture and HUMAUX_TEST_PG_DSN are only mentioned here\nfn t() {}\n",
        )
        .unwrap();
        let stems = db_bound_stems(&root);
        assert_eq!(stems, set(&["raw_dsn"]));
        let _ = fs::remove_dir_all(&root);
    }

    /// Review P1 (card 26): the `ignored` count excuses nothing. Card 25's `public_trust` shape
    /// (1 passed, 5 ignored, 0.00 s) is an offender when its default run is DB-bound; it passes
    /// only because [`db_bound_stems`] finds its one non-ignored test pure (next test).
    #[test]
    fn gate_truth_ignored_count_does_not_excuse_a_fast_db_bound_binary() {
        let results = parse_results(
            "     Running tests/public_trust.rs (/t/public_trust-0123456789abcdef)\n\
             test result: ok. 1 passed; 0 failed; 5 ignored; 0 measured; 0 filtered out; \
             finished in 0.00s\n",
        );
        assert_eq!(results, vec![("public_trust".to_string(), 1, 5, 0.0)]);
        assert_eq!(offenders(&results, &set(&["public_trust"])).len(), 1);
    }

    /// ADR-0051 D-K: the DB-bound set judges the default (non-`--ignored`) run. A binary whose
    /// only DB code sits in `#[ignore]` lane tests is not DB-bound; one whose non-ignored test
    /// reaches a DB helper (directly, through a local fn, or a `#[path]` module) is — even with
    /// lane tests beside it, which is the "one lane test + one silently skipping DB test" shape.
    #[test]
    fn gate_truth_db_bound_set_is_the_default_run_closure() {
        let root = std::env::temp_dir().join(format!("gate_truth_default_{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("crates/a/tests/support")).unwrap();
        let lane = "#[test]\n#[ignore = \"lane(a:shared_db) x\"]\nfn lane() { connect(); }\n\
                    fn connect() { let _ = postgres::Client::connect(\"x\", postgres::NoTls); }\n";
        let files = [
            (
                "pure_plus_lane.rs",
                format!("{lane}#[test]\nfn pure() {{ assert_eq!(1, 1); }}\n"),
            ),
            (
                "helper_plus_lane.rs",
                format!("{lane}#[test]\nfn db() {{ connect(); }}\n"),
            ),
            (
                "module_plus_lane.rs",
                format!(
                    "#[path = \"support/fx.rs\"]\nmod fx;\nuse fx::seed;\n{lane}\
                     #[test]\nfn db() {{ seed(); }}\n"
                ),
            ),
            (
                "env_plus_lane.rs",
                format!(
                    "{lane}#[test]\nfn skips() {{ if std::env::var(\"HUMAUX_TEST_PG_DSN\").is_err() \
                     {{ return; }} }}\n"
                ),
            ),
        ];
        for (name, body) in &files {
            fs::write(root.join("crates/a/tests").join(name), body).unwrap();
        }
        fs::write(
            root.join("crates/a/tests/support/fx.rs"),
            "pub fn seed() { humaux_testkit::run_db_fixture::<F, _>(\"x\", |_| ()); }\n",
        )
        .unwrap();
        let stems = db_bound_stems(&root);
        assert_eq!(
            stems,
            set(&["env_plus_lane", "helper_plus_lane", "module_plus_lane"])
        );
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
            .map(|(stem, ..)| stem)
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
