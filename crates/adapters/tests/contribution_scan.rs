//! Real binary tests for the contribution scanner's fail-closed boundaries.
//!
//! The runner supplies the pinned binary fixture through environment variables. Production
//! receives the same fields from typed configuration; it never reads these variables.

use humaux_adapters::contribution_scan::{ContributionScanner, ContributionScannerConfig};
use humaux_domain::error::ErrorCode;
use std::{path::PathBuf, time::Duration};

fn fixture_config() -> ContributionScannerConfig {
    ContributionScannerConfig {
        executable: PathBuf::from(
            std::env::var("HUMAUX_TEST_GITLEAKS_BIN")
                .expect("ignored fixture test requires HUMAUX_TEST_GITLEAKS_BIN"),
        ),
        expected_version: std::env::var("HUMAUX_TEST_GITLEAKS_VERSION")
            .expect("ignored fixture test requires HUMAUX_TEST_GITLEAKS_VERSION"),
        expected_executable_sha256: std::env::var("HUMAUX_TEST_GITLEAKS_SHA256")
            .expect("ignored fixture test requires HUMAUX_TEST_GITLEAKS_SHA256"),
        timeout: Duration::from_secs(5),
        max_payload_bytes: 64 * 1024,
        finding_exit_code: 1,
    }
}

#[test]
#[ignore = "lane(b) requires pinned real Gitleaks fixture"]
fn real_gitleaks_allows_clean_fixture_and_blocks_known_secret_fixture() {
    let config = fixture_config();
    let expected_version = config.expected_version.clone();
    let expected_binary_sha256 = config.expected_executable_sha256.clone();
    let scanner = ContributionScanner::new(config).expect("verified scanner fixture");
    let receipt = scanner
        .scan(b"Generalized statement with no personal identifiers.")
        .expect("clean fixture");
    assert_eq!(receipt.gitleaks_version(), expected_version);
    assert_eq!(receipt.gitleaks_binary_sha256(), expected_binary_sha256);

    // Synthetic GitHub-shaped fixture, not a credential. No email or digit run: only the
    // actual Gitleaks stage can reject this, rather than the earlier privacy check.
    assert_eq!(
        scanner.scan(b"ghp_RkqFzVpLwNyHtBvDgXsWuCePjMoTnAiSyEkl\n"),
        Err(ErrorCode::Forbidden)
    );
}

#[test]
#[ignore = "lane(b) requires pinned real Gitleaks fixture"]
fn real_gitleaks_decodes_base64_secret_before_matching() {
    let scanner = ContributionScanner::new(fixture_config()).expect("verified scanner fixture");
    // Base64 of the synthetic GitHub-shaped value used above. The scanner owns
    // `--max-decode-depth=5`; this proves the pinned binary, not a local regex,
    // sees the decoded secret.
    assert_eq!(
        scanner.scan(b"Z2hwX1JrcUZ6VnBMd055SHRCdkRnWHNXdUNlUGpNb1RuQWlTeUVrbA=="),
        Err(ErrorCode::Forbidden)
    );
}

#[test]
#[ignore = "lane(b) requires pinned real Gitleaks fixture"]
fn deterministic_privacy_rules_block_synthetic_email_and_phone() {
    let scanner = ContributionScanner::new(fixture_config()).expect("verified scanner fixture");
    assert_eq!(
        scanner.scan(b"contact a@example.test"),
        Err(ErrorCode::Forbidden)
    );
    assert_eq!(
        scanner.scan(b"call +1 (415) 555-0123"),
        Err(ErrorCode::Forbidden)
    );
    for input in [
        br#"{"email":"a@example.test"}"#.as_slice(),
        br#"{"email":"a\u0040example.test"}"#.as_slice(),
        br#"{"a\u0040example.test":true}"#.as_slice(),
    ] {
        assert_eq!(scanner.scan(input), Err(ErrorCode::Forbidden));
    }
}

#[test]
#[ignore = "lane(b) requires pinned real Gitleaks fixture"]
fn host_config_and_inline_allow_do_not_disable_scanning() {
    const TEST_NAME: &str = "host_config_and_inline_allow_do_not_disable_scanning";
    if std::env::var_os("HUMAUX_TEST_SCAN_CONFIG_CHILD").is_some() {
        let scanner = ContributionScanner::new(fixture_config()).unwrap();
        assert_eq!(
            scanner.scan(b"ghp_RkqFzVpLwNyHtBvDgXsWuCePjMoTnAiSyEkl //gitleaks:allow"),
            Err(ErrorCode::Forbidden)
        );
        return;
    }
    let directory = std::env::temp_dir().join(format!(
        "humaux-scanner-config-test-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir(&directory).unwrap();
    let config = "title = 'synthetic hostile override'\n[extend]\nuseDefault = true\n[allowlist]\nregexes = ['.*']\n";
    let config_path = directory.join(".gitleaks.toml");
    std::fs::write(&config_path, config).unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST_NAME, "--ignored", "--nocapture"])
        .env("HUMAUX_TEST_SCAN_CONFIG_CHILD", "1")
        .env("GITLEAKS_CONFIG", &config_path)
        .env("GITLEAKS_CONFIG_TOML", config)
        .current_dir(&directory)
        .output()
        .unwrap();
    std::fs::remove_file(config_path).unwrap();
    std::fs::remove_dir(directory).unwrap();
    assert!(
        output.status.success(),
        "isolated child failed: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
#[ignore = "lane(b) requires pinned real Gitleaks fixture"]
fn binary_replaced_after_construction_fails_closed() {
    let mut config = fixture_config();
    let copy = std::env::temp_dir().join(format!(
        "humaux-scanner-binary-test-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::copy(&config.executable, &copy).unwrap();
    config.executable = copy.clone();
    let scanner = ContributionScanner::new(config).unwrap();
    std::fs::write(&copy, b"changed executable bytes").unwrap();
    let result = scanner.scan(b"otherwise clean contribution");
    std::fs::remove_file(copy).unwrap();
    assert_eq!(result, Err(ErrorCode::DependencyUnavailable));
}

#[test]
fn missing_binary_fails_closed_without_runtime_fixture() {
    let missing = ContributionScannerConfig {
        executable: PathBuf::from("/definitely/not/a/gitleaks-binary"),
        expected_version: "unused".to_owned(),
        expected_executable_sha256: "0".repeat(64),
        timeout: Duration::from_secs(1),
        max_payload_bytes: 1,
        finding_exit_code: 1,
    };
    assert!(matches!(
        ContributionScanner::new(missing),
        Err(ErrorCode::DependencyUnavailable)
    ));
}

#[test]
#[ignore = "lane(b) requires pinned real Gitleaks fixture"]
fn real_binary_version_mismatch_fails_closed() {
    let mut mismatch = fixture_config();
    mismatch.expected_version.push_str("-mismatch");
    assert!(matches!(
        ContributionScanner::new(mismatch),
        Err(ErrorCode::Conflict)
    ));
}

#[test]
#[ignore = "lane(b) requires pinned real Gitleaks fixture"]
fn real_binary_hash_mismatch_fails_closed() {
    let mut mismatch = fixture_config();
    mismatch.expected_executable_sha256 = "0".repeat(64);
    assert!(matches!(
        ContributionScanner::new(mismatch),
        Err(ErrorCode::Conflict)
    ));
}

#[test]
#[ignore = "lane(b) requires pinned real Gitleaks fixture"]
fn real_binary_version_probe_honors_timeout() {
    let mut timed_out = fixture_config();
    timed_out.timeout = Duration::from_nanos(1);
    assert!(matches!(
        ContributionScanner::new(timed_out),
        Err(ErrorCode::DependencyUnavailable)
    ));
}
