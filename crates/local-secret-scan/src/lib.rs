//! Shared local, fail-closed scanner for bytes before external retrieval egress or contribution disclosure.
//!
//! Configuration is supplied explicitly by trusted wiring. The scanner never reads an
//! environment variable, never invokes a shell, and never returns scanner stdout/stderr or input
//! bytes through a debug/error surface.  A successful [`LocalSecretScanReceipt`] is opaque to
//! callers and carries the immutable scanner configuration identities that the contribution
//! repository persists with the candidate and release.

use humaux_domain::{dataclass::DataClass, error::ErrorCode, evidence::EvidencePayloadSha256};
use humaux_projection::card::RetrievalCard;
use humaux_retrieval::request::{ProfileFingerprint, TrustedRetrievalQuery};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

/// Explicit, pinned configuration for the local contribution scanner.
#[derive(Debug, Clone)]
pub struct LocalSecretScannerConfig {
    /// Absolute path to the vetted gitleaks binary; never resolved from `PATH`.
    pub executable: PathBuf,
    /// Exact trimmed output expected from `gitleaks version`.
    pub expected_version: String,
    /// Lowercase SHA-256 of the vetted executable bytes.  The pinned 8.30.1 binary embeds the
    /// rules, so this binds both executable behavior and its rule set without inventing a second
    /// configuration artifact.
    pub expected_executable_sha256: String,
    /// Upper bound for all binary invocations, including version probing.
    pub timeout: Duration,
    /// Maximum disclosed payload size.  Oversize input fails; it is never truncated.
    pub max_payload_bytes: usize,
    /// Exit status used by this pinned gitleaks configuration when it finds a secret.
    pub finding_exit_code: i32,
}

/// Closed disposition carried by a local scanner attestation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalSecretScanDisposition {
    Pass,
    Reject,
}

impl LocalSecretScanDisposition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Reject => "REJECT",
        }
    }
}

/// Closed stage that rejected the disclosed payload, when the disposition is `REJECT`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalSecretScanRejectionStage {
    None,
    DeterministicPrivacy,
    Gitleaks,
}

impl LocalSecretScanRejectionStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::DeterministicPrivacy => "DETERMINISTIC_PRIVACY",
            Self::Gitleaks => "GITLEAKS",
        }
    }
}

/// Opaque typed result of a local scanner execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalSecretScanOutcome {
    Pass(LocalSecretScanReceipt),
    Reject(LocalSecretScanReceipt),
}

impl LocalSecretScanOutcome {
    pub fn receipt(&self) -> &LocalSecretScanReceipt {
        match self {
            Self::Pass(receipt) | Self::Reject(receipt) => receipt,
        }
    }
}

/// Opaque attestation emitted for either scanner disposition.
///
/// Its fields remain private so no caller can construct a self-declared passed result.  The
/// paired contribution repository reads the narrow accessor set to persist attestation metadata.
#[derive(Clone, PartialEq, Eq)]
pub struct LocalSecretScanReceipt {
    payload_sha256: EvidencePayloadSha256,
    privacy_rules_version: &'static str,
    privacy_rules_digest: String,
    gitleaks_version: String,
    gitleaks_binary_sha256: String,
    disposition: LocalSecretScanDisposition,
    rejection_stage: LocalSecretScanRejectionStage,
}

impl std::fmt::Debug for LocalSecretScanReceipt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalSecretScanReceipt")
            .field("payload_sha256", &self.payload_sha256)
            .field("privacy_rules_version", &self.privacy_rules_version)
            .field("privacy_rules_digest", &self.privacy_rules_digest)
            .field("gitleaks_version", &self.gitleaks_version)
            .field("gitleaks_binary_sha256", &self.gitleaks_binary_sha256)
            .field("disposition", &self.disposition)
            .field("rejection_stage", &self.rejection_stage)
            .finish()
    }
}

impl LocalSecretScanReceipt {
    /// Canonical digest of precisely the bytes all scanner stages inspected.
    pub fn payload_sha256(&self) -> EvidencePayloadSha256 {
        self.payload_sha256
    }

    /// Version of the built-in deterministic privacy rules.
    pub fn privacy_rules_version(&self) -> &'static str {
        self.privacy_rules_version
    }

    /// Digest of the compiled scan implementation, including rules and invocation flags.
    pub fn privacy_rules_digest(&self) -> &str {
        &self.privacy_rules_digest
    }

    /// Exact vetted gitleaks version observed before this scan.
    pub fn gitleaks_version(&self) -> &str {
        &self.gitleaks_version
    }

    /// SHA-256 of the vetted gitleaks binary, including its embedded rule set.
    pub fn gitleaks_binary_sha256(&self) -> &str {
        &self.gitleaks_binary_sha256
    }

    pub const fn disposition(&self) -> LocalSecretScanDisposition {
        self.disposition
    }

    pub const fn rejection_stage(&self) -> LocalSecretScanRejectionStage {
        self.rejection_stage
    }

    /// Fixed receipt payload for persistence. It is intentionally closed and contains neither
    /// scanned bytes nor finding text or process output.
    pub fn receipt_json(&self) -> serde_json::Value {
        serde_json::json!({
            "receipt_schema_version": "contribution-scan-receipt-v1",
            "payload_sha256": self.payload_sha256.to_hex(),
            "privacy_rules_version": self.privacy_rules_version,
            "privacy_rules_digest": self.privacy_rules_digest,
            "gitleaks_version": self.gitleaks_version,
            "gitleaks_binary_sha256": self.gitleaks_binary_sha256,
            "scan_disposition": self.disposition.as_str(),
            "rejection_stage": self.rejection_stage.as_str(),
        })
    }

    /// Stable identity of the complete scanner attestation that approved these bytes. Length
    /// prefixes make the canonical tuple unambiguous; the domain separator permits a deliberate
    /// future format change without silently colliding with this revision.
    fn attestation_fingerprint(&self) -> String {
        let mut canonical = b"humaux-local-secret-scan-attestation-v1".to_vec();
        for field in [
            self.privacy_rules_version.as_bytes(),
            self.privacy_rules_digest.as_bytes(),
            self.gitleaks_version.as_bytes(),
            self.gitleaks_binary_sha256.as_bytes(),
        ] {
            canonical.extend_from_slice(&(field.len() as u64).to_be_bytes());
            canonical.extend_from_slice(field);
        }
        format!("scanner-attestation-sha256:{}", sha256_hex(&canonical))
    }
}

/// Concrete scanner adapter.  Constructing it verifies the pinned rules digest and binary
/// version and binary digest, so a missing, changed, or mismatched scanner fails before any
/// candidate can pass.
pub struct LocalSecretScanner {
    config: LocalSecretScannerConfig,
}

impl LocalSecretScanner {
    /// Validates the explicit configuration against the local pinned artifacts.
    pub fn new(config: LocalSecretScannerConfig) -> Result<Self, ErrorCode> {
        if !config.executable.is_absolute()
            || config.expected_version.trim().is_empty()
            || config.expected_executable_sha256.len() != 64
            || config.timeout.is_zero()
            || config.max_payload_bytes == 0
            || !(1..=255).contains(&config.finding_exit_code)
        {
            return Err(ErrorCode::InvalidInput);
        }
        let executable =
            fs::read(&config.executable).map_err(|_| ErrorCode::DependencyUnavailable)?;
        if sha256_hex(&executable) != config.expected_executable_sha256 {
            return Err(ErrorCode::Conflict);
        }
        let version = run_version(&config)?;
        if version != config.expected_version {
            return Err(ErrorCode::Conflict);
        }
        Ok(Self { config })
    }

    /// Runs deterministic privacy checks and the pinned gitleaks stdin scan.
    pub fn scan_outcome(&self, bytes: &[u8]) -> Result<LocalSecretScanOutcome, ErrorCode> {
        if bytes.is_empty() || bytes.len() > self.config.max_payload_bytes {
            return Err(ErrorCode::InvalidInput);
        }
        if privacy_rejection(bytes)? {
            return Ok(LocalSecretScanOutcome::Reject(self.receipt(
                bytes,
                LocalSecretScanDisposition::Reject,
                LocalSecretScanRejectionStage::DeterministicPrivacy,
            )));
        }
        verify_executable(&self.config)?;
        let result = run_gitleaks(&self.config, bytes)?;
        verify_executable(&self.config)?;
        Ok(self.outcome_for_exit(bytes, result))
    }

    /// Legacy compatibility entrypoint: callers that only accept approved bytes continue to see
    /// `Forbidden` for either deterministic or gitleaks findings.
    pub fn scan(&self, bytes: &[u8]) -> Result<LocalSecretScanReceipt, ErrorCode> {
        match self.scan_outcome(bytes)? {
            LocalSecretScanOutcome::Pass(receipt) => Ok(receipt),
            LocalSecretScanOutcome::Reject(_) => Err(ErrorCode::Forbidden),
        }
    }

    fn outcome_for_exit(&self, bytes: &[u8], exit: ScanExit) -> LocalSecretScanOutcome {
        match exit {
            ScanExit::Clean => LocalSecretScanOutcome::Pass(self.receipt(
                bytes,
                LocalSecretScanDisposition::Pass,
                LocalSecretScanRejectionStage::None,
            )),
            ScanExit::Finding => LocalSecretScanOutcome::Reject(self.receipt(
                bytes,
                LocalSecretScanDisposition::Reject,
                LocalSecretScanRejectionStage::Gitleaks,
            )),
        }
    }

    fn receipt(
        &self,
        bytes: &[u8],
        disposition: LocalSecretScanDisposition,
        rejection_stage: LocalSecretScanRejectionStage,
    ) -> LocalSecretScanReceipt {
        LocalSecretScanReceipt {
            payload_sha256: humaux_domain::evidence::payload_sha256(bytes),
            privacy_rules_version: LOCAL_SECRET_RULES_VERSION,
            privacy_rules_digest: sha256_hex(include_bytes!("lib.rs")),
            gitleaks_version: self.config.expected_version.clone(),
            gitleaks_binary_sha256: self.config.expected_executable_sha256.clone(),
            disposition,
            rejection_stage,
        }
    }
}

/// Opaque, scan-attested query bytes eligible for a retrieval provider call.
#[derive(Clone, PartialEq, Eq)]
pub struct SealedRetrievalQuery {
    text: String,
    data_class: DataClass,
    receipt: LocalSecretScanReceipt,
    profile_fingerprint: ProfileFingerprint,
    scanner_attestation_fingerprint: String,
}

impl std::fmt::Debug for SealedRetrievalQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealedRetrievalQuery")
            .field("data_class", &self.data_class)
            .field("payload_sha256", &self.receipt.payload_sha256())
            .field("profile_fingerprint", &self.profile_fingerprint)
            .finish()
    }
}

impl SealedRetrievalQuery {
    pub fn as_str(&self) -> &str {
        &self.text
    }
    pub const fn data_class(&self) -> DataClass {
        self.data_class
    }
    pub fn payload_sha256(&self) -> EvidencePayloadSha256 {
        self.receipt.payload_sha256()
    }

    /// Exact SHA-256 bytes of the sealed UTF-8 query. This is derived only from the immutable
    /// sealed content; callers cannot provide or replace a digest independently.
    pub fn payload_sha256_bytes(&self) -> [u8; 32] {
        Sha256::digest(self.text.as_bytes()).into()
    }

    /// Exact byte length of the sealed UTF-8 query.
    pub fn payload_bytes(&self) -> usize {
        self.text.len()
    }

    /// Effective registered retrieval-profile identity carried from the sole request builder.
    pub fn profile_fingerprint_identity(&self) -> &ProfileFingerprint {
        &self.profile_fingerprint
    }

    /// Server-owned identity of the full successful scanner attestation: built-in rules version
    /// and digest plus the observed Gitleaks version and vetted binary digest.
    pub fn classifier_revision(&self) -> &str {
        &self.scanner_attestation_fingerprint
    }
}

/// Opaque, scan-attested retrieval-card bytes eligible for a dense write or rerank call.
#[derive(Clone, PartialEq, Eq)]
pub struct SealedRetrievalCard {
    text: String,
    data_class: DataClass,
    receipt: LocalSecretScanReceipt,
}

impl std::fmt::Debug for SealedRetrievalCard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealedRetrievalCard")
            .field("data_class", &self.data_class)
            .field("payload_sha256", &self.receipt.payload_sha256())
            .finish()
    }
}

impl SealedRetrievalCard {
    pub fn as_str(&self) -> &str {
        &self.text
    }
    pub const fn data_class(&self) -> DataClass {
        self.data_class
    }
    pub fn payload_sha256(&self) -> EvidencePayloadSha256 {
        self.receipt.payload_sha256()
    }
}

impl LocalSecretScanner {
    /// Seals only a text query minted by retrieval's sole `build_request` path. Query input is
    /// tenant-private by policy, never a caller-selected grade. This local scan does not replace
    /// authorization, EgressPermit issuance, disclosure reservation/finalization, or send gates.
    pub fn seal_query(
        &self,
        query: &TrustedRetrievalQuery<'_>,
    ) -> Result<SealedRetrievalQuery, ErrorCode> {
        let text = query.text();
        if text.chars().count() > RETRIEVAL_QUERY_MAX_CHARS
            || text.len() > SEALED_RETRIEVAL_MAX_BYTES
        {
            return Err(ErrorCode::InvalidInput);
        }
        let receipt = self.scan(text.as_bytes())?;
        let scanner_attestation_fingerprint = receipt.attestation_fingerprint();
        Ok(SealedRetrievalQuery {
            text: text.to_owned(),
            data_class: DataClass::Private,
            receipt,
            profile_fingerprint: query.profile_fingerprint_identity().clone(),
            scanner_attestation_fingerprint,
        })
    }

    /// Seals only the actual `build_card` result, retaining its actual data class.
    pub fn seal_card(&self, card: &RetrievalCard) -> Result<SealedRetrievalCard, ErrorCode> {
        if card.data_class == DataClass::SecretMaterial
            || card.card_text.len() > SEALED_RETRIEVAL_MAX_BYTES
        {
            return Err(ErrorCode::InvalidInput);
        }
        let receipt = self.scan(card.card_text.as_bytes())?;
        Ok(SealedRetrievalCard {
            text: card.card_text.clone(),
            data_class: card.data_class,
            receipt,
        })
    }
}

/// Query schema ceiling.
pub const RETRIEVAL_QUERY_MAX_CHARS: usize = 4_096;
/// Explicit byte ceiling for bytes inspected before external eligibility.
pub const SEALED_RETRIEVAL_MAX_BYTES: usize = 16 * 1024;

const LOCAL_SECRET_RULES_VERSION: &str = "contribution-privacy-v2";

fn verify_executable(config: &LocalSecretScannerConfig) -> Result<(), ErrorCode> {
    let bytes = fs::read(&config.executable).map_err(|_| ErrorCode::DependencyUnavailable)?;
    if sha256_hex(&bytes) != config.expected_executable_sha256 {
        return Err(ErrorCode::DependencyUnavailable);
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn privacy_rejection(bytes: &[u8]) -> Result<bool, ErrorCode> {
    let text = std::str::from_utf8(bytes).map_err(|_| ErrorCode::InvalidInput)?;
    if contains_email_like(text) || contains_phone_like(text) {
        return Ok(true);
    }
    // Structured provider output may escape identifiers. Inspect decoded strings and keys as
    // well as exact wire bytes; this never changes the payload that receives the receipt.
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(text) {
        let mut pending = vec![&value];
        while let Some(value) = pending.pop() {
            match value {
                serde_json::Value::String(s)
                    if contains_email_like(s) || contains_phone_like(s) =>
                {
                    return Ok(true);
                }
                serde_json::Value::Array(values) => pending.extend(values),
                serde_json::Value::Object(values) => {
                    if values
                        .keys()
                        .any(|key| contains_email_like(key) || contains_phone_like(key))
                    {
                        return Ok(true);
                    }
                    pending.extend(values.values());
                }
                _ => {}
            }
        }
    }
    Ok(false)
}

fn contains_email_like(text: &str) -> bool {
    text.split(|c: char| {
        !c.is_ascii_alphanumeric() && !matches!(c, '@' | '.' | '_' | '%' | '+' | '-')
    })
    .any(|token| {
        let Some((local, domain)) = token.rsplit_once('@') else {
            return false;
        };
        !local.is_empty()
            && domain.contains('.')
            && domain
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
    })
}

fn contains_phone_like(text: &str) -> bool {
    let mut digits = 0_u8;
    for byte in text.bytes() {
        if byte.is_ascii_digit() {
            digits = digits.saturating_add(1);
            if digits >= 7 {
                return true;
            }
        } else if !matches!(byte, b' ' | b'-' | b'(' | b')' | b'.' | b'+') {
            digits = 0;
        }
    }
    false
}

enum ScanExit {
    Clean,
    Finding,
}

fn run_version(config: &LocalSecretScannerConfig) -> Result<String, ErrorCode> {
    let mut child = Command::new(&config.executable)
        .arg("version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or(ErrorCode::DependencyUnavailable)?;
    let reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut stdout, &mut bytes).map(|_| bytes)
    });
    let waited = wait_until_exit(&mut child, config.timeout);
    if waited.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let bytes = reader
        .join()
        .map_err(|_| ErrorCode::DependencyUnavailable)?
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    if !waited?.success() || bytes.len() > 256 {
        return Err(ErrorCode::DependencyUnavailable);
    }
    String::from_utf8(bytes)
        .map(|value| value.trim().to_owned())
        .map_err(|_| ErrorCode::DependencyUnavailable)
}

fn run_gitleaks(config: &LocalSecretScannerConfig, bytes: &[u8]) -> Result<ScanExit, ErrorCode> {
    // A private empty working directory excludes repository allowlists/config. Explicitly
    // discard configuration from the parent environment and disable inline allow comments.
    let directory = ScanDirectory::new()?;
    let mut child = Command::new(&config.executable)
        .arg("stdin")
        .arg("--ignore-gitleaks-allow")
        .arg("--max-decode-depth=5")
        .arg("--exit-code")
        .arg(config.finding_exit_code.to_string())
        .env_remove("GITLEAKS_CONFIG")
        .env_remove("GITLEAKS_CONFIG_TOML")
        .current_dir(&directory.0)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    let mut stdin = child.stdin.take().ok_or(ErrorCode::DependencyUnavailable)?;
    let input = bytes.to_vec();
    let writer = thread::spawn(move || stdin.write_all(&input));
    // Always join the writer before propagating the wait result.  If the timeout path killed
    // the child, closing stdin releases a blocked writer instead of leaving a test/worker thread.
    let waited = wait_until_exit(&mut child, config.timeout);
    if waited.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let writer_result = writer
        .join()
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    let status = waited?;
    writer_result.map_err(|_| ErrorCode::DependencyUnavailable)?;
    if status.success() {
        Ok(ScanExit::Clean)
    } else if status.code() == Some(config.finding_exit_code) {
        Ok(ScanExit::Finding)
    } else {
        Err(ErrorCode::DependencyUnavailable)
    }
}

struct ScanDirectory(PathBuf);
impl ScanDirectory {
    fn new() -> Result<Self, ErrorCode> {
        let path =
            std::env::temp_dir().join(format!("humaux-local-secret-scan-{}", uuid::Uuid::now_v7()));
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder
            .create(&path)
            .map_err(|_| ErrorCode::DependencyUnavailable)?;
        Ok(Self(path))
    }
}
impl Drop for ScanDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir(&self.0);
    }
}

fn wait_until_exit(
    child: &mut std::process::Child,
    timeout: Duration,
) -> Result<std::process::ExitStatus, ErrorCode> {
    let start = Instant::now();
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|_| ErrorCode::DependencyUnavailable)?
        {
            return Ok(status);
        }
        if start.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            return Err(ErrorCode::DependencyUnavailable);
        }
        thread::sleep(Duration::from_millis(5));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt(binary_sha256: &str) -> LocalSecretScanReceipt {
        LocalSecretScanReceipt {
            payload_sha256: humaux_domain::evidence::payload_sha256(b"same query"),
            privacy_rules_version: "same-rules-version",
            privacy_rules_digest: "same-rules-digest".to_owned(),
            gitleaks_version: "same-gitleaks-version".to_owned(),
            gitleaks_binary_sha256: binary_sha256.to_owned(),
            disposition: LocalSecretScanDisposition::Pass,
            rejection_stage: LocalSecretScanRejectionStage::None,
        }
    }

    fn scanner_without_runtime_fixture() -> LocalSecretScanner {
        LocalSecretScanner {
            config: LocalSecretScannerConfig {
                executable: PathBuf::from("/intentionally/not/executed/by-these-unit-tests"),
                expected_version: "gitleaks-test-version".to_owned(),
                expected_executable_sha256: "a".repeat(64),
                timeout: Duration::from_secs(1),
                max_payload_bytes: 1024,
                finding_exit_code: 1,
            },
        }
    }

    #[test]
    fn scanner_attestation_fingerprint_covers_more_than_rules_version() {
        let first = receipt(&"a".repeat(64));
        let second = receipt(&"b".repeat(64));

        assert_eq!(
            first.privacy_rules_version(),
            second.privacy_rules_version()
        );
        assert_ne!(
            first.attestation_fingerprint(),
            second.attestation_fingerprint(),
            "a different vetted scanner binary must have a different classifier identity"
        );
        assert!(first.attestation_fingerprint().len() <= 128);
    }

    #[test]
    fn clean_exit_is_a_typed_pass_with_opaque_attestation() {
        let outcome = scanner_without_runtime_fixture().outcome_for_exit(b"clean", ScanExit::Clean);
        let LocalSecretScanOutcome::Pass(receipt) = outcome else {
            panic!("clean scanner exit must pass");
        };
        assert_eq!(receipt.disposition(), LocalSecretScanDisposition::Pass);
        assert_eq!(
            receipt.rejection_stage(),
            LocalSecretScanRejectionStage::None
        );
        assert_eq!(
            receipt.payload_sha256(),
            humaux_domain::evidence::payload_sha256(b"clean")
        );
    }

    #[test]
    fn deterministic_privacy_rejection_is_typed_and_legacy_scan_is_forbidden() {
        let scanner = scanner_without_runtime_fixture();
        let payload = b"contact a@example.test";
        let outcome = scanner
            .scan_outcome(payload)
            .expect("deterministic check runs first");
        let LocalSecretScanOutcome::Reject(receipt) = outcome else {
            panic!("privacy identifier must reject");
        };
        assert_eq!(receipt.disposition(), LocalSecretScanDisposition::Reject);
        assert_eq!(
            receipt.rejection_stage(),
            LocalSecretScanRejectionStage::DeterministicPrivacy
        );
        assert_eq!(scanner.scan(payload), Err(ErrorCode::Forbidden));
    }

    #[test]
    fn gitleaks_exit_is_a_typed_reject_without_fixture() {
        let outcome = scanner_without_runtime_fixture()
            .outcome_for_exit(b"synthetic-secret", ScanExit::Finding);
        let LocalSecretScanOutcome::Reject(receipt) = outcome else {
            panic!("gitleaks finding must reject");
        };
        assert_eq!(
            receipt.rejection_stage(),
            LocalSecretScanRejectionStage::Gitleaks
        );
    }

    #[test]
    fn receipt_json_has_exact_closed_shape_and_no_sensitive_content() {
        let payload = b"contact a@example.test";
        let outcome = scanner_without_runtime_fixture()
            .scan_outcome(payload)
            .expect("deterministic check runs first");
        let json = outcome.receipt().receipt_json();
        assert_eq!(
            json,
            serde_json::json!({
                "receipt_schema_version": "contribution-scan-receipt-v1",
                "payload_sha256": humaux_domain::evidence::payload_sha256(payload).to_hex(),
                "privacy_rules_version": LOCAL_SECRET_RULES_VERSION,
                "privacy_rules_digest": sha256_hex(include_bytes!("lib.rs")),
                "gitleaks_version": "gitleaks-test-version",
                "gitleaks_binary_sha256": "a".repeat(64),
                "scan_disposition": "REJECT",
                "rejection_stage": "DETERMINISTIC_PRIVACY",
            })
        );
        let rendered = json.to_string();
        assert!(!rendered.contains("a@example.test"));
        assert!(!rendered.contains("stdout"));
        assert!(!rendered.contains("stderr"));
        assert!(!format!("{:?}", outcome.receipt()).contains("a@example.test"));
    }
}
