//! `xtask::r4_fault_manifest` — parses and validates contracts/r4_fault_manifest.toml.
//! Depends-on: crates=[toml]; services=[]; env=[CARGO_MANIFEST_DIR]; modules=[]
//! Called-by: [xtask::main]
//! Invariants: [every fault_case entry must carry all ROOT_KEYS; a missing key names the file and the key]
//! Spec: Baseline §11.2.5.1; ADR-0008
//!
use std::{collections::BTreeSet, fs, path::PathBuf};

const MANIFEST_PATH: &str = "contracts/r4_fault_manifest.toml";
const ROOT_KEYS: &[&str] = &[
    "canon_ref",
    "fault_case",
    "gate",
    "phase",
    "provider_mode",
    "schema_version",
];
const CASE_KEYS: &[&str] = &[
    "case_id",
    "dispatch_expectation",
    "durable_observation",
    "fault_injection",
    "gate_id",
    "probe_axes",
    "requires_fresh_connection",
    "runner_case_id",
    "sql_case_id",
    "variant",
];
const PROBE_AXES: &[&str] = &["candidate", "disclosure", "execution", "job", "ledger"];

pub fn run(args: &[String]) -> i32 {
    if !args.is_empty() {
        eprintln!("usage: cargo xtask r4-fault-manifest");
        return 2;
    }

    let path = workspace_root().join(MANIFEST_PATH);
    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(error) => {
            eprintln!("G-R4-FAULT-MANIFEST FAIL: {}: {error}", path.display());
            return 1;
        }
    };

    match validate(&raw) {
        Ok(()) => {
            println!("G-R4-FAULT-MANIFEST PASS: keyed contract and evidence mappings are exact");
            0
        }
        Err(errors) => {
            for error in errors {
                eprintln!("G-R4-FAULT-MANIFEST FAIL: {error}");
            }
            1
        }
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask must be a workspace member")
        .to_path_buf()
}

#[allow(clippy::too_many_lines)] // One pass keeps every closed manifest mapping in one diagnostic context.
fn validate(raw: &str) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();
    let compact = raw
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase();
    if compact.contains("phase10") {
        errors.push("Phase10 is outside the R4 manifest boundary".to_owned());
    }
    if compact.contains("liveprovider") {
        errors.push("live-provider evidence is outside the R4 manifest boundary".to_owned());
    }

    let value = match toml::from_str::<toml::Value>(raw) {
        Ok(value) => value,
        Err(error) => return Err(vec![format!("invalid TOML: {error}")]),
    };
    let Some(root) = value.as_table() else {
        return Err(vec!["manifest root must be a TOML table".to_owned()]);
    };

    validate_keys("manifest", root, ROOT_KEYS, &mut errors);
    validate_integer(root, "schema_version", 1, &mut errors);
    validate_integer(root, "phase", 9, &mut errors);
    validate_string(root, "gate", "G-R4-FAULT-MANIFEST", &mut errors);
    validate_string(root, "provider_mode", "recording", &mut errors);
    validate_string(
        root,
        "canon_ref",
        "docs/architecture/Baseline_2.9.md §11.2.5.1",
        &mut errors,
    );

    let expected_pairs = expected_pairs();
    let mut actual_pairs = BTreeSet::new();
    let mut case_ids = BTreeSet::new();
    let mut sql_case_ids = BTreeSet::new();
    let cases = match root.get("fault_case").and_then(toml::Value::as_array) {
        Some(cases) => cases,
        None => {
            errors.push("fault_case must be an array of tables".to_owned());
            return Err(errors);
        }
    };

    for (index, value) in cases.iter().enumerate() {
        let label = format!("fault_case[{index}]");
        let Some(case) = value.as_table() else {
            errors.push(format!("{label} must be a table"));
            continue;
        };
        validate_keys(&label, case, CASE_KEYS, &mut errors);

        let Some(gate_id) = required_string(case, "gate_id", &label, &mut errors) else {
            continue;
        };
        let Some(variant) = required_string(case, "variant", &label, &mut errors) else {
            continue;
        };
        let pair = (gate_id.to_owned(), variant.to_owned());
        if !actual_pairs.insert(pair.clone()) {
            errors.push(format!("duplicate acceptance key ({gate_id}, {variant})"));
        }

        let expected_case_id = case_id(gate_id, variant);
        validate_string(case, "case_id", &expected_case_id, &mut errors);
        if let Some(actual_case_id) = case.get("case_id").and_then(toml::Value::as_str)
            && !case_ids.insert(actual_case_id.to_owned())
        {
            errors.push(format!("duplicate case_id {actual_case_id}"));
        }

        let expected_sql_case_id = if pair == ("R4-FG-05".to_owned(), "BASE".to_owned()) {
            "NONE".to_owned()
        } else {
            format!("sql_{expected_case_id}")
        };
        validate_string(case, "sql_case_id", &expected_sql_case_id, &mut errors);
        if let Some(actual_sql_case_id) = case.get("sql_case_id").and_then(toml::Value::as_str)
            && actual_sql_case_id != "NONE"
            && !sql_case_ids.insert(actual_sql_case_id.to_owned())
        {
            errors.push(format!("duplicate sql_case_id {actual_sql_case_id}"));
        }

        let runner_case_id = expected_runner_case_id(gate_id, variant);
        validate_string(case, "runner_case_id", runner_case_id, &mut errors);
        validate_string(
            case,
            "dispatch_expectation",
            expected_dispatch(runner_case_id),
            &mut errors,
        );
        validate_probe_axes(case, &label, &mut errors);
        validate_bool(case, "requires_fresh_connection", true, &mut errors);
        validate_nonempty(case, "fault_injection", &label, &mut errors);
        validate_nonempty(case, "durable_observation", &label, &mut errors);
    }

    for missing in expected_pairs.difference(&actual_pairs) {
        errors.push(format!(
            "missing acceptance key ({}, {})",
            missing.0, missing.1
        ));
    }
    for extra in actual_pairs.difference(&expected_pairs) {
        errors.push(format!("extra acceptance key ({}, {})", extra.0, extra.1));
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn expected_pairs() -> BTreeSet<(String, String)> {
    let mut pairs = (1..=17)
        .map(|number| (format!("R4-FG-{number:02}"), "BASE".to_owned()))
        .collect::<BTreeSet<_>>();
    for variant in [
        "B_GATE_FAIL",
        "SCAN_REJECT",
        "PROVIDER_DEFINITE_FAILURE",
        "TIMEOUT",
    ] {
        pairs.insert(("R4-FG-18".to_owned(), variant.to_owned()));
    }
    pairs
}

fn case_id(gate_id: &str, variant: &str) -> String {
    let gate = gate_id
        .strip_prefix("R4-FG-")
        .unwrap_or(gate_id)
        .to_ascii_lowercase();
    format!("r4_fg_{gate}_{}", variant.to_ascii_lowercase())
}

fn expected_runner_case_id(gate_id: &str, variant: &str) -> &'static str {
    match (gate_id, variant) {
        ("R4-FG-04", "BASE") => "runner_a_reserve_crash_zero_dispatch",
        ("R4-FG-05", "BASE") => "runner_a_response_before_commit_zero_redispatch",
        ("R4-FG-08", "BASE") | ("R4-FG-18", "TIMEOUT") => {
            "runner_b_reserve_timeout_zero_redispatch"
        }
        ("R4-FG-09", "BASE") => "runner_b_scanner_unknown_zero_redispatch",
        ("R4-FG-15", "BASE") => "runner_reserved_successor_zero_redispatch",
        ("R4-FG-13" | "R4-FG-17", "BASE")
        | ("R4-FG-18", "B_GATE_FAIL" | "SCAN_REJECT" | "PROVIDER_DEFINITE_FAILURE") => {
            "runner_live_late_and_outcomes"
        }
        _ => "NONE",
    }
}

fn expected_dispatch(runner_case_id: &str) -> &'static str {
    match runner_case_id {
        "NONE" => "NOT_APPLICABLE",
        "runner_a_reserve_crash_zero_dispatch" => "ZERO_AFTER_RESERVE",
        "runner_reserved_successor_zero_redispatch" => "ZERO_ON_RESERVED_SUCCESSOR",
        _ => "ONE_INITIAL_ZERO_REDISPATCH",
    }
}

fn validate_keys(label: &str, table: &toml::Table, expected: &[&str], errors: &mut Vec<String>) {
    let actual = table.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    for missing in expected.difference(&actual) {
        errors.push(format!("{label} missing field {missing}"));
    }
    for extra in actual.difference(&expected) {
        errors.push(format!("{label} has unknown field {extra}"));
    }
}

fn required_string<'a>(
    table: &'a toml::Table,
    key: &str,
    label: &str,
    errors: &mut Vec<String>,
) -> Option<&'a str> {
    match table.get(key).and_then(toml::Value::as_str) {
        Some(value) => Some(value),
        None => {
            errors.push(format!("{label}.{key} must be a string"));
            None
        }
    }
}

fn validate_string(table: &toml::Table, key: &str, expected: &str, errors: &mut Vec<String>) {
    match table.get(key).and_then(toml::Value::as_str) {
        Some(actual) if actual == expected => {}
        Some(actual) => errors.push(format!(
            "{key} mapping mismatch: expected {expected}, found {actual}"
        )),
        None => errors.push(format!("{key} must be the string {expected}")),
    }
}

fn validate_integer(table: &toml::Table, key: &str, expected: i64, errors: &mut Vec<String>) {
    match table.get(key).and_then(toml::Value::as_integer) {
        Some(actual) if actual == expected => {}
        Some(actual) => errors.push(format!("{key} must be {expected}, found {actual}")),
        None => errors.push(format!("{key} must be the integer {expected}")),
    }
}

fn validate_bool(table: &toml::Table, key: &str, expected: bool, errors: &mut Vec<String>) {
    match table.get(key).and_then(toml::Value::as_bool) {
        Some(actual) if actual == expected => {}
        Some(actual) => errors.push(format!("{key} must be {expected}, found {actual}")),
        None => errors.push(format!("{key} must be a boolean")),
    }
}

fn validate_nonempty(table: &toml::Table, key: &str, label: &str, errors: &mut Vec<String>) {
    match table.get(key).and_then(toml::Value::as_str) {
        Some(value) if !value.trim().is_empty() => {}
        _ => errors.push(format!("{label}.{key} must be a non-empty string")),
    }
}

fn validate_probe_axes(table: &toml::Table, label: &str, errors: &mut Vec<String>) {
    let Some(values) = table.get("probe_axes").and_then(toml::Value::as_array) else {
        errors.push(format!("{label}.probe_axes must be an array"));
        return;
    };
    let axes = values
        .iter()
        .filter_map(toml::Value::as_str)
        .collect::<BTreeSet<_>>();
    let expected = PROBE_AXES.iter().copied().collect::<BTreeSet<_>>();
    if values.len() != axes.len() || axes != expected {
        errors.push(format!(
            "{label}.probe_axes must be the exact execution/job/ledger/disclosure/candidate set"
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::validate;

    const VALID: &str = include_str!("../../contracts/r4_fault_manifest.toml");

    fn case_block(raw: &str, case_id: &str) -> String {
        raw.split("[[fault_case]]")
            .skip(1)
            .find(|block| block.contains(&format!("case_id = \"{case_id}\"")))
            .map(|block| format!("[[fault_case]]{block}"))
            .expect("case fixture")
    }

    fn without_case(raw: &str, case_id: &str) -> String {
        raw.replacen(&case_block(raw, case_id), "", 1)
    }

    #[test]
    fn canonical_manifest_passes() {
        assert_eq!(validate(VALID), Ok(()));
    }

    #[test]
    fn contract_mutations_fail_closed() {
        let mutations = [
            ("missing", without_case(VALID, "r4_fg_05_base")),
            (
                "duplicate",
                format!("{VALID}\n{}", case_block(VALID, "r4_fg_01_base")),
            ),
            (
                "extra",
                VALID.replacen("gate_id = \"R4-FG-01\"", "gate_id = \"R4-FG-19\"", 1),
            ),
            (
                "wrong variant",
                VALID.replacen("variant = \"BASE\"", "variant = \"WRONG\"", 1),
            ),
            (
                "unknown SQL mapping",
                VALID.replacen(
                    "sql_case_id = \"sql_r4_fg_01_base\"",
                    "sql_case_id = \"sql_unknown\"",
                    1,
                ),
            ),
            (
                "unknown runner mapping",
                VALID.replacen(
                    "runner_case_id = \"runner_a_reserve_crash_zero_dispatch\"",
                    "runner_case_id = \"runner_unknown\"",
                    1,
                ),
            ),
            (
                "wrong probe axes",
                VALID.replacen(
                    "probe_axes = [\"execution\", \"job\", \"ledger\", \"disclosure\", \"candidate\"]",
                    "probe_axes = [\"execution\", \"job\"]",
                    1,
                ),
            ),
            ("Phase10", VALID.replacen("phase = 9", "phase = 10", 1)),
            (
                "live provider",
                VALID.replacen(
                    "provider_mode = \"recording\"",
                    "provider_mode = \"live_provider\"",
                    1,
                ),
            ),
            (
                "numeric tally",
                VALID.replacen("phase = 9", "phase = 9\nexpected_case_count = 21", 1),
            ),
        ];

        for (label, mutation) in mutations {
            assert!(validate(&mutation).is_err(), "mutation accepted: {label}");
        }
    }
}
