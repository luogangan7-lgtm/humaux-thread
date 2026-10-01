//! `local-secret-scan::tests::seal_rules` — Retrieval-seal rule set and pinned-binary re-verification against real
//!   and fake scanner binaries.
//! Depends-on: crates=[humaux-domain, humaux-projection, humaux-retrieval, sha2, uuid]; services=[];
//!   env=[HUMAUX_TEST_GITLEAKS_BIN, HUMAUX_TEST_GITLEAKS_SHA256, HUMAUX_TEST_GITLEAKS_VERSION];
//!   modules=[domain::authority, domain::dataclass, domain::error, domain::memory, humaux-local-secret-scan,
//!   projection::card, retrieval::request]
//! Called-by: [cargo-test]
//! Invariants: [the pinned gitleaks comes from HUMAUX_TEST_GITLEAKS_* (production reads typed config); the swap
//!   tests run two same-length fake scanners in a private temp dir and never the real binary]
//! Spec: Baseline §7.5; §12.1.1; ADR-0056
//!
//! Credential vectors are the fake GitHub-shaped value already used by
//! `crates/adapters/tests/contribution_scan.rs`, never a live key.

use humaux_domain::{dataclass::DataClass, error::ErrorCode};
use humaux_local_secret_scan::{
    LocalSecretScanOutcome, LocalSecretScanRejectionStage, LocalSecretScanner,
    LocalSecretScannerConfig,
};
use humaux_projection::card::{
    CardBudget, CardBuildOutcome, CardInput, EgressDisposition, RetrievalCard, build_card,
};
use humaux_retrieval::request::{
    RetrievalIntent, RetrievalRequest, build_request, resolve_registered_retrieval_profile,
};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, time::Duration};

/// A date, an e-mail address, a phone number, a 12-digit order number and a random UUID: each
/// one is refused by the contribution rules and must be sealable for private retrieval.
const IDENTIFIERS: [&str; 5] = [
    "the billing cutover is on 2026-10-15",
    "write to a@example.test about the invoice",
    "call +1 (415) 555-0123 for the on-call rota",
    "order 123456789012 shipped late",
    "incident 3f0c9a4e-1b7d-4c55-9e21-79869668a78d is closed",
];

/// Synthetic GitHub-shaped fixture (contribution_scan.rs), not a credential.
const FAKE_GITHUB_TOKEN: &str = "ghp_RkqFzVpLwNyHtBvDgXsWuCePjMoTnAiSyEkl";

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("ignored fixture test requires {name}"))
}

fn config(executable: PathBuf, version: &str, sha256: String) -> LocalSecretScannerConfig {
    LocalSecretScannerConfig {
        executable,
        expected_version: version.to_owned(),
        expected_executable_sha256: sha256,
        timeout: Duration::from_secs(5),
        max_payload_bytes: 64 * 1024,
        finding_exit_code: 1,
    }
}

fn real_scanner() -> LocalSecretScanner {
    LocalSecretScanner::new(config(
        env("HUMAUX_TEST_GITLEAKS_BIN").into(),
        &env("HUMAUX_TEST_GITLEAKS_VERSION"),
        env("HUMAUX_TEST_GITLEAKS_SHA256"),
    ))
    .expect("verified scanner fixture")
}

fn request(text: &str) -> RetrievalRequest {
    let intent = RetrievalIntent::new(
        text.to_owned(),
        Vec::new(),
        Default::default(),
        Default::default(),
    )
    .expect("intent");
    let profile = resolve_registered_retrieval_profile(&Default::default()).expect("profile");
    build_request(intent, &profile).expect("request")
}

fn card(text: &str) -> RetrievalCard {
    let CardBuildOutcome::Card(card) = build_card(
        CardInput {
            memory_id: humaux_domain::authority::MemoryId(uuid::Uuid::now_v7()),
            memory_type: humaux_domain::memory::MemoryType::Fact,
            data_class: DataClass::Private,
            egress_disposition: EgressDisposition::Allowed,
            workspace_id: None,
            topic: None,
            effective_from: std::time::SystemTime::now(),
            title: text.to_owned(),
            key_claim: Some(text.to_owned()),
            entities: Vec::new(),
            evidence_excerpt: Some(text.to_owned()),
        },
        CardBudget::default(),
    ) else {
        panic!("fixture card builds");
    };
    *card
}

fn seal_query(scanner: &LocalSecretScanner, text: &str) -> Result<String, ErrorCode> {
    let request = request(text);
    let query = request.trusted_query().expect("text query");
    scanner
        .seal_query(&query)
        .map(|sealed| sealed.classifier_revision().to_owned())
}

/// The attestation fingerprint of `lib.rs` (`attestation_fingerprint`), recomputed from the
/// public receipt fields for a given rules version.
fn fingerprint(rules_version: &str, digest: &str, version: &str, binary: &str) -> String {
    let mut canonical = b"humaux-local-secret-scan-attestation-v1".to_vec();
    for field in [rules_version, digest, version, binary] {
        canonical.extend_from_slice(&(field.len() as u64).to_be_bytes());
        canonical.extend_from_slice(field.as_bytes());
    }
    let hex: String = Sha256::digest(&canonical)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("scanner-attestation-sha256:{hex}")
}

/// ADR-0056 D-A/D-B: the five identifier shapes seal as query and as card, and the sealed
/// query's classifier names the seal rule set, not the contribution one. Fault: privacy rules
/// back on the seal path ⇒ `Forbidden` ⇒ red.
#[test]
#[ignore = "lane(b) requires pinned real Gitleaks fixture"]
fn seal_query_and_seal_card_accept_date_email_phone_long_number_and_uuid() {
    let scanner = real_scanner();
    let clean = scanner
        .scan(b"Generalized statement with no personal identifiers.")
        .expect("clean contribution fixture");
    let seal_identity = fingerprint(
        "retrieval-seal-secrets-v1",
        clean.privacy_rules_digest(),
        clean.gitleaks_version(),
        clean.gitleaks_binary_sha256(),
    );
    let contribution_identity = fingerprint(
        clean.privacy_rules_version(),
        clean.privacy_rules_digest(),
        clean.gitleaks_version(),
        clean.gitleaks_binary_sha256(),
    );
    assert_ne!(seal_identity, contribution_identity);
    for text in IDENTIFIERS {
        assert_eq!(
            seal_query(&scanner, text).as_deref(),
            Ok(seal_identity.as_str()),
            "seal_query({text:?})"
        );
        assert!(
            scanner.seal_card(&card(text)).is_ok(),
            "seal_card({text:?})"
        );
    }
}

/// A gitleaks finding is still `Forbidden` on both seals. Fault: bypass `run_checked` on the
/// seal path ⇒ `Ok` ⇒ red.
#[test]
#[ignore = "lane(b) requires pinned real Gitleaks fixture"]
fn seal_query_and_seal_card_refuse_a_gitleaks_finding() {
    let scanner = real_scanner();
    let text = format!("the CI bot uses {FAKE_GITHUB_TOKEN} for releases");
    assert_eq!(seal_query(&scanner, &text), Err(ErrorCode::Forbidden));
    assert_eq!(
        scanner.seal_card(&card(&text)).map(|_| ()),
        Err(ErrorCode::Forbidden)
    );
}

/// The §12 contribution path keeps its deterministic privacy stage.
#[test]
#[ignore = "lane(b) requires pinned real Gitleaks fixture"]
fn scan_outcome_still_rejects_email_and_phone_as_deterministic_privacy() {
    let scanner = real_scanner();
    for payload in [
        b"contact a@example.test".as_slice(),
        b"call +1 (415) 555-0123".as_slice(),
    ] {
        let Ok(LocalSecretScanOutcome::Reject(receipt)) = scanner.scan_outcome(payload) else {
            panic!("privacy identifier must reject");
        };
        assert_eq!(
            receipt.rejection_stage(),
            LocalSecretScanRejectionStage::DeterministicPrivacy
        );
        assert_eq!(receipt.privacy_rules_version(), "contribution-privacy-v2");
    }
}

/// Two fake scanners of equal length in a private temp dir. `A` answers `version` and passes
/// every scan; `B` would also pass but leaves a marker file, so "B ran" is observable.
#[cfg(unix)]
struct FakeScanners {
    dir: PathBuf,
    a: PathBuf,
    b_bytes: Vec<u8>,
    marker: PathBuf,
}

#[cfg(unix)]
impl FakeScanners {
    fn new() -> Self {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("c30b-fake-scan-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&dir).expect("temp dir");
        let marker = dir.join("b-ran");
        let a_bytes = b"#!/bin/sh\n[ \"$1\" = version ] && { echo fake-1; exit 0; }\ncat >/dev/null\nexit 0\n".to_vec();
        let mut b_bytes = format!(
            "#!/bin/sh\n[ \"$1\" = version ] && {{ echo fake-1; exit 0; }}\ntouch {}\ncat >/dev/null\nexit 0\n",
            marker.display()
        )
        .into_bytes();
        // Same length: a stat tuple that differs only in ctime/ino is the case under test.
        b_bytes.resize(a_bytes.len().max(b_bytes.len()), b'#');
        let mut a_padded = a_bytes.clone();
        a_padded.resize(b_bytes.len(), b'#');
        a_padded.push(b'\n');
        b_bytes.push(b'\n');
        let a = dir.join("scanner");
        std::fs::write(&a, &a_padded).expect("write A");
        std::fs::set_permissions(&a, std::fs::Permissions::from_mode(0o755)).expect("chmod A");
        Self {
            dir,
            a,
            b_bytes,
            marker,
        }
    }

    fn scanner(&self) -> LocalSecretScanner {
        let bytes = std::fs::read(&self.a).expect("read A");
        let sha: String = Sha256::digest(&bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        LocalSecretScanner::new(config(self.a.clone(), "fake-1", sha)).expect("fake A verifies")
    }

    fn b_ran(&self) -> bool {
        self.marker.exists()
    }
}

#[cfg(unix)]
impl Drop for FakeScanners {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// ADR-0056 D-D: B's bytes written into A's inode with A's mtime restored — only `ctime`
/// betrays it, and the re-hash refuses before B is spawned. Fault: skip the re-hash on a stamp
/// change ⇒ B runs and passes ⇒ `Ok` ⇒ red.
#[cfg(unix)]
#[test]
fn swapped_binary_by_content_is_detected() {
    let fakes = FakeScanners::new();
    let scanner = fakes.scanner();
    assert!(seal_query(&scanner, "clean before the swap").is_ok());
    let mtime = std::fs::metadata(&fakes.a)
        .and_then(|m| m.modified())
        .expect("A mtime");
    std::fs::write(&fakes.a, &fakes.b_bytes).expect("overwrite A in place");
    std::fs::File::options()
        .write(true)
        .open(&fakes.a)
        .and_then(|f| f.set_modified(mtime))
        .expect("restore mtime");
    assert_eq!(
        seal_query(&scanner, "clean after the swap"),
        Err(ErrorCode::DependencyUnavailable)
    );
    assert_eq!(
        seal_query(&scanner, "clean again"),
        Err(ErrorCode::DependencyUnavailable),
        "a failed re-hash is never cached as good"
    );
    assert!(!fakes.b_ran(), "the swapped binary must never execute");
}

/// A rename over A's path gives a new inode ⇒ re-hash ⇒ refused.
#[cfg(unix)]
#[test]
fn swapped_binary_by_path_is_detected() {
    use std::os::unix::fs::PermissionsExt;
    let fakes = FakeScanners::new();
    let scanner = fakes.scanner();
    let b = fakes.dir.join("scanner-b");
    std::fs::write(&b, &fakes.b_bytes).expect("write B");
    std::fs::set_permissions(&b, std::fs::Permissions::from_mode(0o755)).expect("chmod B");
    std::fs::rename(&b, &fakes.a).expect("rename B over A");
    assert_eq!(
        seal_query(&scanner, "clean after the rename"),
        Err(ErrorCode::DependencyUnavailable)
    );
    assert!(!fakes.b_ran(), "the swapped binary must never execute");
}

/// Identical bytes with a new stamp re-hash, match, and keep scanning.
#[cfg(unix)]
#[test]
fn touched_binary_with_identical_bytes_still_scans() {
    let fakes = FakeScanners::new();
    let scanner = fakes.scanner();
    let bytes = std::fs::read(&fakes.a).expect("read A");
    std::fs::write(&fakes.a, &bytes).expect("rewrite identical bytes");
    assert!(seal_query(&scanner, "clean after a touch").is_ok());
    assert!(seal_query(&scanner, "clean on the cached stamp").is_ok());
}
