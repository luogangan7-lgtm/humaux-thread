//! Contribution scanner adapter over the shared, pinned local secret scanner.
//!
//! The concrete scanner lives in `humaux-retrieval::secret_scan`: both contribution release
//! and retrieval egress consume it, while this adapter alone implements the application port.
//! That keeps IO out of Domain and avoids the invalid `retrieval -> adapters` dependency edge.

use async_trait::async_trait;
use humaux_application::contribute::ContributionScannerPort;
use humaux_domain::error::ErrorCode;
pub use humaux_local_secret_scan::{
    LocalSecretScanOutcome as ContributionScanOutcome,
    LocalSecretScanReceipt as ContributionScanReceipt,
    LocalSecretScannerConfig as ContributionScannerConfig,
};

/// Contribution-specific application-port adapter around the shared concrete scanner.
pub struct ContributionScanner(humaux_local_secret_scan::LocalSecretScanner);

impl ContributionScanner {
    /// Validates the absolute pinned executable, version, and executable SHA-256.
    pub fn new(config: ContributionScannerConfig) -> Result<Self, ErrorCode> {
        humaux_local_secret_scan::LocalSecretScanner::new(config).map(Self)
    }

    /// Scans the exact candidate bytes; receipt remains opaque and byte-bound.
    pub fn scan(&self, bytes: &[u8]) -> Result<ContributionScanReceipt, ErrorCode> {
        self.0.scan(bytes)
    }

    /// Typed scanner outcome with the fixed, versioned receipt JSON available from its receipt.
    pub fn scan_outcome(&self, bytes: &[u8]) -> Result<ContributionScanOutcome, ErrorCode> {
        self.0.scan_outcome(bytes)
    }

    /// Fixed, versioned JSON projection for persistence of a scanner attestation.
    pub fn receipt_json(receipt: &ContributionScanReceipt) -> serde_json::Value {
        receipt.receipt_json()
    }
}

#[async_trait]
impl ContributionScannerPort for ContributionScanner {
    type Receipt = ContributionScanReceipt;

    async fn scan_disclosed_bytes(&self, bytes: &[u8]) -> Result<Self::Receipt, ErrorCode> {
        self.scan(bytes)
    }
}
