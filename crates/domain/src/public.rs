//! `domain::public` — the type surface of §12's Public Contribution Pipeline: "用户知识进入
//! 公共域必须是一个明确 Release 行为" (spec:2659). This module carries the closed sets and the
//! sole `ContributionRelease` construction point (DOD-051's type half, spec:11125); the
//! matching DB surface (staging.*/public.* CHECK constraints) lives in migrations, with
//! `as_db_str` here as the single Rust-side wire form for each closed set.
//!
//! Pure types + construction-time validation — no IO (§3 / §78.3 domain import rule).

use crate::authority::{EvidenceId, MemoryId, NonEmptyVec};
use std::fmt;

/// §12.1 Contribution Policy closed set (spec:2687-2691): `DISABLED` / `MANUAL` /
/// `AUTO_AFTER_USER_DISTILLATION`. A release must snapshot the policy it was made under
/// (spec:2693 "必须保存 policy snapshot + consent/grant version") — hence the field on
/// [`ContributionRelease`], not a lookup at read time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ContributionPolicy {
    /// Contribution is off; no release may be constructed under it (enforced by
    /// [`ContributionRelease::release`]).
    Disabled,
    /// Each release requires an explicit user action.
    Manual,
    /// Release happens automatically, but only after user-facing distillation.
    AutoAfterUserDistillation,
}

impl ContributionPolicy {
    /// All variants, declaration order — the §78.2 DB↔Rust contract test iterates this
    /// (`adapters/tests/public_contribution_contract.rs`); a new variant must be added here
    /// AND to the exhaustive `as_db_str` match, so the two cannot drift silently.
    pub const ALL: [ContributionPolicy; 3] = [
        ContributionPolicy::Disabled,
        ContributionPolicy::Manual,
        ContributionPolicy::AutoAfterUserDistillation,
    ];

    /// SCREAMING_SNAKE wire form, verbatim the spec:2687-2691 token list (and the migration
    /// CHECK constraint literal for the policy-snapshot column).
    pub const fn as_db_str(self) -> &'static str {
        match self {
            ContributionPolicy::Disabled => "DISABLED",
            ContributionPolicy::Manual => "MANUAL",
            ContributionPolicy::AutoAfterUserDistillation => "AUTO_AFTER_USER_DISTILLATION",
        }
    }
}

/// §13 release lifecycle closed set (spec:2874-): `ACTIVE -> REVOKED`, nothing else — revocation
/// is a state flip on the release row, never a delete (the provenance DAG rooted in it must stay
/// auditable).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReleaseState {
    /// The release is in force; public claims may cite it.
    Active,
    /// §13: the release was withdrawn. Terminal.
    Revoked,
}

impl ReleaseState {
    /// All variants, declaration order (§78.2 contract-test iteration source).
    pub const ALL: [ReleaseState; 2] = [ReleaseState::Active, ReleaseState::Revoked];

    /// SCREAMING_SNAKE wire form (migration CHECK literal).
    pub const fn as_db_str(self) -> &'static str {
        match self {
            ReleaseState::Active => "ACTIVE",
            ReleaseState::Revoked => "REVOKED",
        }
    }
}

/// §12.4 `PublicSource.source_type` closed set of 5 (spec:2751-2759). Public knowledge
/// completion only ever adds Evidence through one of these source types — "不能由企业 LLM
/// 凭空生成事实" (spec:2741).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PublicSourceType {
    /// A user's explicit [`ContributionRelease`].
    UserContribution,
    /// An official document import.
    OfficialDocument,
    /// Public web content.
    PublicWeb,
    /// An open-source document.
    OpenSourceDocument,
    /// An administrator-driven import.
    AdminImport,
}

impl PublicSourceType {
    /// All variants, declaration order (§78.2 contract-test iteration source).
    pub const ALL: [PublicSourceType; 5] = [
        PublicSourceType::UserContribution,
        PublicSourceType::OfficialDocument,
        PublicSourceType::PublicWeb,
        PublicSourceType::OpenSourceDocument,
        PublicSourceType::AdminImport,
    ];

    /// SCREAMING_SNAKE wire form, verbatim the spec:2751-2759 token list (the
    /// `public.sources.source_type` column value).
    pub const fn as_db_str(self) -> &'static str {
        match self {
            PublicSourceType::UserContribution => "USER_CONTRIBUTION",
            PublicSourceType::OfficialDocument => "OFFICIAL_DOCUMENT",
            PublicSourceType::PublicWeb => "PUBLIC_WEB",
            PublicSourceType::OpenSourceDocument => "OPEN_SOURCE_DOCUMENT",
            PublicSourceType::AdminImport => "ADMIN_IMPORT",
        }
    }
}

/// Public-claim moderation closed set, derived from the §12 Quarantine/Promotion flow
/// (spec:2846-2852): `PUBLIC_STAGING -> trust evaluation -> ... -> supported claim ->
/// public searchable`, with "高风险/新 contributor 可以进入审核" as the review branch,
/// quarantine as the poisoning-check failure branch, and §13 revocation as the terminal
/// withdrawal. The spec freezes the flow, not a token list — this enum is that flow's
/// state set, one variant per distinguishable stage (DOD-053: "poisoning/quarantine
/// 状态可见").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ModerationState {
    /// Entry state of every contribution (spec:2846 flow head).
    PublicStaging,
    /// Review branch for high-risk/new contributors (spec:2851-2852).
    UnderReview,
    /// Passed trust evaluation + poisoning/anomaly checks; publicly searchable.
    Supported,
    /// Failed poisoning/anomaly checks; held out of public retrieval.
    Quarantined,
    /// §13: the underlying release was revoked.
    Revoked,
}

impl ModerationState {
    /// All variants, declaration order (§78.2 contract-test iteration source).
    pub const ALL: [ModerationState; 5] = [
        ModerationState::PublicStaging,
        ModerationState::UnderReview,
        ModerationState::Supported,
        ModerationState::Quarantined,
        ModerationState::Revoked,
    ];

    /// SCREAMING_SNAKE wire form (migration CHECK literal for `moderation_state`).
    pub const fn as_db_str(self) -> &'static str {
        match self {
            ModerationState::PublicStaging => "PUBLIC_STAGING",
            ModerationState::UnderReview => "UNDER_REVIEW",
            ModerationState::Supported => "SUPPORTED",
            ModerationState::Quarantined => "QUARANTINED",
            ModerationState::Revoked => "REVOKED",
        }
    }
}

/// Outcome closed set of the §12 De-identification / Secret Scan step (the pipeline stage
/// between Contribution Candidate and ContributionRelease in the spec:2661-2678 flow):
/// binary by design — a scan either passed or blocked the release, there is no
/// "passed with warnings" that could leak through.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScanOutcome {
    /// The scan found nothing that blocks release.
    Passed,
    /// The scan blocks release ([`ContributionRelease::release`] rejects it).
    Blocked,
}

impl ScanOutcome {
    /// All variants, declaration order (§78.2 contract-test iteration source).
    pub const ALL: [ScanOutcome; 2] = [ScanOutcome::Passed, ScanOutcome::Blocked];

    /// SCREAMING_SNAKE wire form (migration CHECK literal for the scan-outcome columns).
    pub const fn as_db_str(self) -> &'static str {
        match self {
            ScanOutcome::Passed => "PASSED",
            ScanOutcome::Blocked => "BLOCKED",
        }
    }
}

/// §50 fail-loud construction-time error for this module — a local struct, not a third
/// runtime error enum beside `ErrorCode`/`DegradeCode` (same carve-out
/// `retrieval::predicate_registry::PredicateRegistryError` documents).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContributionReleaseError(pub String);

impl fmt::Display for ContributionReleaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ContributionReleaseError {}

/// §12.5 Rights Provenance (spec:2773-2779): the rights half of a contribution/public
/// import's provenance, kept alongside the technical half so "撤销与审计" can trace a
/// synthesis back to its rights basis.
///
/// Fields are private; [`RightsProvenance::new`] is the sole construction point (the
/// `policy::PredicateEntry` pattern) — a held value always has a non-blank `rights_basis`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RightsProvenance {
    rights_basis: String,
    source_license: Option<String>,
    publisher: Option<String>,
    contributor_attestation: Option<String>,
    redistribution_policy: Option<String>,
}

impl RightsProvenance {
    /// Sole construction point. Rejects a blank (empty or whitespace-only) `rights_basis` —
    /// spec:2773-2779 lists it first and unconditionally; a release with no stated rights
    /// basis cannot be audited or revoked-with-cause. The four remaining fields are optional
    /// in the spec list and stay `Option` here.
    pub fn new(
        rights_basis: String,
        source_license: Option<String>,
        publisher: Option<String>,
        contributor_attestation: Option<String>,
        redistribution_policy: Option<String>,
    ) -> Result<Self, ContributionReleaseError> {
        if rights_basis.trim().is_empty() {
            return Err(ContributionReleaseError(
                "rights_basis must be non-blank (§12.5)".to_owned(),
            ));
        }
        Ok(Self {
            rights_basis,
            source_license,
            publisher,
            contributor_attestation,
            redistribution_policy,
        })
    }

    /// The mandatory rights basis (§12.5). Never blank.
    pub fn rights_basis(&self) -> &str {
        &self.rights_basis
    }

    /// Optional source license (§12.5).
    pub fn source_license(&self) -> Option<&str> {
        self.source_license.as_deref()
    }

    /// Optional publisher (§12.5).
    pub fn publisher(&self) -> Option<&str> {
        self.publisher.as_deref()
    }

    /// Optional contributor attestation (§12.5).
    pub fn contributor_attestation(&self) -> Option<&str> {
        self.contributor_attestation.as_deref()
    }

    /// Optional redistribution policy (§12.5).
    pub fn redistribution_policy(&self) -> Option<&str> {
        self.redistribution_policy.as_deref()
    }
}

/// One row of §12.1's `staging.contribution_release_sources` (spec:2697-2703), as a type:
/// the table's CHECK "`evidence_id` / `memory_id` 恰好一个非 NULL" is unrepresentable-invalid
/// here — a variant carries exactly one id, there is no two-`Option` shape to mis-populate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReleaseSource {
    /// The row's `evidence_id` half (FK to the authority Evidence table).
    Evidence(EvidenceId),
    /// The row's `memory_id` half (FK to the private memory table).
    Memory(MemoryId),
}

/// §12 ContributionRelease — the explicit Release act itself. DOD-051's type half
/// (spec:11125 "ContributionRelease 有 privacy + rights provenance"): a value of this type
/// cannot exist without a policy snapshot, a consent version, a validated
/// [`RightsProvenance`], passed privacy/secret scans, and at least one source.
///
/// Fields are private; [`ContributionRelease::release`] is the sole construction point —
/// already-validated semantics, a held value needs no re-checking downstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContributionRelease {
    policy: ContributionPolicy,
    consent_version: String,
    rights: RightsProvenance,
    privacy_scan: ScanOutcome,
    secret_scan: ScanOutcome,
    sources: NonEmptyVec<ReleaseSource>,
}

impl ContributionRelease {
    /// Sole construction point. Rejects, each fail-loud (§50):
    ///
    /// - `policy == Disabled` — a release under a disabled policy is a contradiction of
    ///   §12.1's own gate (spec:2687);
    /// - blank `consent_version` — spec:2693 requires the consent/grant version be saved;
    ///   a blank snapshot saves nothing;
    /// - `privacy` or `secret` scan `Blocked` — the §12 flow (spec:2661-2678) puts the
    ///   De-identification / Secret Scan step strictly before ContributionRelease;
    /// - empty `sources` — spec:2694-2703's link table is the origin of the public
    ///   provenance DAG ("Public provenance DAG 从这张表开始闭包"); a release with zero
    ///   sources would be an unrooted DAG node.
    pub fn release(
        policy: ContributionPolicy,
        consent_version: String,
        rights: RightsProvenance,
        privacy: ScanOutcome,
        secret: ScanOutcome,
        sources: Vec<ReleaseSource>,
    ) -> Result<Self, ContributionReleaseError> {
        if policy == ContributionPolicy::Disabled {
            return Err(ContributionReleaseError(
                "cannot release under a DISABLED contribution policy (§12.1)".to_owned(),
            ));
        }
        if consent_version.trim().is_empty() {
            return Err(ContributionReleaseError(
                "consent_version must be non-blank (§12.1 policy snapshot + consent version)"
                    .to_owned(),
            ));
        }
        if privacy == ScanOutcome::Blocked {
            return Err(ContributionReleaseError(
                "privacy scan blocked the release (§12 pipeline order)".to_owned(),
            ));
        }
        if secret == ScanOutcome::Blocked {
            return Err(ContributionReleaseError(
                "secret scan blocked the release (§12 pipeline order)".to_owned(),
            ));
        }
        let sources = NonEmptyVec::new(sources).map_err(|_| {
            ContributionReleaseError(
                "a release must carry at least one source (§12.1 provenance DAG root)".to_owned(),
            )
        })?;
        Ok(Self {
            policy,
            consent_version,
            rights,
            privacy_scan: privacy,
            secret_scan: secret,
            sources,
        })
    }

    /// Policy snapshot the release was made under (spec:2693). Never `Disabled`.
    pub fn policy(&self) -> ContributionPolicy {
        self.policy
    }

    /// Consent/grant version snapshot (spec:2693). Never blank.
    pub fn consent_version(&self) -> &str {
        &self.consent_version
    }

    /// §12.5 rights provenance (DOD-051).
    pub fn rights(&self) -> &RightsProvenance {
        &self.rights
    }

    /// Privacy-scan outcome. Always `Passed` on a held value.
    pub fn privacy_scan(&self) -> ScanOutcome {
        self.privacy_scan
    }

    /// Secret-scan outcome. Always `Passed` on a held value.
    pub fn secret_scan(&self) -> ScanOutcome {
        self.secret_scan
    }

    /// The release's sources (§12.1 link-table rows). Never empty.
    pub fn sources(&self) -> &[ReleaseSource] {
        self.sources.as_slice()
    }
}

/// §12.2's closed capability set for the Public LLM (spec:2709-2719): it "只能" normalize /
/// classify / merge / summarize / detect contradiction / propose relation / produce synthesis
/// from supported claims. DOD-050 ("Public LLM 不能创建无 Evidence 的事实") at the vocabulary
/// level: there is no `CreateFact` variant — the absence is itself the type proof that no
/// code path can even name that capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PublicLlmCapability {
    /// spec:2711 normalize.
    Normalize,
    /// spec:2712 classify.
    Classify,
    /// spec:2713 merge.
    Merge,
    /// spec:2714 summarize.
    Summarize,
    /// spec:2715 detect contradiction.
    DetectContradiction,
    /// spec:2716 propose relation.
    ProposeRelation,
    /// spec:2717 produce synthesis from supported claims (must trace back to a
    /// [`ContributionRelease`], spec:2721).
    ProduceSynthesis,
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- as_db_str exhaustive pins: the production `match` already refuses to compile on a
    // new variant; these additionally pin the literal wire values (policy.rs precedent).

    #[test]
    fn contribution_policy_db_str_matches_spec() {
        use ContributionPolicy::*;
        for p in [Disabled, Manual, AutoAfterUserDistillation] {
            let expected = match p {
                Disabled => "DISABLED",
                Manual => "MANUAL",
                AutoAfterUserDistillation => "AUTO_AFTER_USER_DISTILLATION",
            };
            assert_eq!(p.as_db_str(), expected);
        }
    }

    #[test]
    fn release_state_db_str_matches_spec() {
        use ReleaseState::*;
        for s in [Active, Revoked] {
            let expected = match s {
                Active => "ACTIVE",
                Revoked => "REVOKED",
            };
            assert_eq!(s.as_db_str(), expected);
        }
    }

    #[test]
    fn public_source_type_db_str_matches_spec() {
        use PublicSourceType::*;
        for t in [
            UserContribution,
            OfficialDocument,
            PublicWeb,
            OpenSourceDocument,
            AdminImport,
        ] {
            let expected = match t {
                UserContribution => "USER_CONTRIBUTION",
                OfficialDocument => "OFFICIAL_DOCUMENT",
                PublicWeb => "PUBLIC_WEB",
                OpenSourceDocument => "OPEN_SOURCE_DOCUMENT",
                AdminImport => "ADMIN_IMPORT",
            };
            assert_eq!(t.as_db_str(), expected);
        }
    }

    #[test]
    fn moderation_state_db_str_matches_spec() {
        use ModerationState::*;
        for m in [PublicStaging, UnderReview, Supported, Quarantined, Revoked] {
            let expected = match m {
                PublicStaging => "PUBLIC_STAGING",
                UnderReview => "UNDER_REVIEW",
                Supported => "SUPPORTED",
                Quarantined => "QUARANTINED",
                Revoked => "REVOKED",
            };
            assert_eq!(m.as_db_str(), expected);
        }
    }

    #[test]
    fn scan_outcome_db_str_matches_spec() {
        use ScanOutcome::*;
        for o in [Passed, Blocked] {
            let expected = match o {
                Passed => "PASSED",
                Blocked => "BLOCKED",
            };
            assert_eq!(o.as_db_str(), expected);
        }
    }

    // --- RightsProvenance construction point.

    #[test]
    fn rights_provenance_rejects_blank_rights_basis() {
        for blank in ["", "   ", "\t\n"] {
            let err = RightsProvenance::new(blank.to_owned(), None, None, None, None)
                .expect_err("blank rights_basis must be rejected");
            assert!(
                err.0.contains("rights_basis"),
                "error names the field: {err}"
            );
        }
    }

    #[test]
    fn rights_provenance_valid_construction_exposes_accessors() {
        let rights = RightsProvenance::new(
            "user consent v3".to_owned(),
            Some("CC-BY-4.0".to_owned()),
            Some("Example Press".to_owned()),
            None,
            Some("redistribution allowed".to_owned()),
        )
        .expect("non-blank rights_basis constructs");
        assert_eq!(rights.rights_basis(), "user consent v3");
        assert_eq!(rights.source_license(), Some("CC-BY-4.0"));
        assert_eq!(rights.publisher(), Some("Example Press"));
        assert_eq!(rights.contributor_attestation(), None);
        assert_eq!(
            rights.redistribution_policy(),
            Some("redistribution allowed")
        );
    }

    // --- ContributionRelease sole construction point (DOD-051 type half).

    fn valid_rights() -> RightsProvenance {
        RightsProvenance::new("user consent v3".to_owned(), None, None, None, None)
            .expect("valid rights")
    }

    fn one_source() -> Vec<ReleaseSource> {
        vec![ReleaseSource::Evidence(EvidenceId::new())]
    }

    #[test]
    fn release_rejects_disabled_policy() {
        let err = ContributionRelease::release(
            ContributionPolicy::Disabled,
            "consent-v1".to_owned(),
            valid_rights(),
            ScanOutcome::Passed,
            ScanOutcome::Passed,
            one_source(),
        )
        .expect_err("DISABLED policy must not release");
        assert!(err.0.contains("DISABLED"), "error names the cause: {err}");
    }

    #[test]
    fn release_rejects_blank_consent_version() {
        let err = ContributionRelease::release(
            ContributionPolicy::Manual,
            "  ".to_owned(),
            valid_rights(),
            ScanOutcome::Passed,
            ScanOutcome::Passed,
            one_source(),
        )
        .expect_err("blank consent_version must be rejected");
        assert!(
            err.0.contains("consent_version"),
            "error names the field: {err}"
        );
    }

    #[test]
    fn release_rejects_blocked_scans() {
        for (privacy, secret, which) in [
            (ScanOutcome::Blocked, ScanOutcome::Passed, "privacy"),
            (ScanOutcome::Passed, ScanOutcome::Blocked, "secret"),
        ] {
            let err = ContributionRelease::release(
                ContributionPolicy::Manual,
                "consent-v1".to_owned(),
                valid_rights(),
                privacy,
                secret,
                one_source(),
            )
            .expect_err("a Blocked scan must not release");
            assert!(err.0.contains(which), "error names the failing scan: {err}");
        }
    }

    #[test]
    fn release_rejects_empty_sources() {
        let err = ContributionRelease::release(
            ContributionPolicy::Manual,
            "consent-v1".to_owned(),
            valid_rights(),
            ScanOutcome::Passed,
            ScanOutcome::Passed,
            vec![],
        )
        .expect_err("empty sources must be rejected");
        assert!(err.0.contains("source"), "error names the cause: {err}");
    }

    /// Positive counterpart, exercising both `ReleaseSource` variants in one release.
    #[test]
    fn release_valid_construction_exposes_accessors() {
        let evidence = EvidenceId::new();
        let memory = MemoryId::new();
        let release = ContributionRelease::release(
            ContributionPolicy::AutoAfterUserDistillation,
            "consent-v2".to_owned(),
            valid_rights(),
            ScanOutcome::Passed,
            ScanOutcome::Passed,
            vec![
                ReleaseSource::Evidence(evidence),
                ReleaseSource::Memory(memory),
            ],
        )
        .expect("all gates passed");
        assert_eq!(
            release.policy(),
            ContributionPolicy::AutoAfterUserDistillation
        );
        assert_eq!(release.consent_version(), "consent-v2");
        assert_eq!(release.rights().rights_basis(), "user consent v3");
        assert_eq!(release.privacy_scan(), ScanOutcome::Passed);
        assert_eq!(release.secret_scan(), ScanOutcome::Passed);
        assert_eq!(
            release.sources(),
            &[
                ReleaseSource::Evidence(evidence),
                ReleaseSource::Memory(memory),
            ]
        );
    }

    /// §12.2 / DOD-050: the capability vocabulary is exactly the 7 spec verbs. The exhaustive
    /// `match` forces this test to be revisited if a variant is ever added — and `CreateFact`
    /// is provably absent because the enum cannot name it.
    #[test]
    fn public_llm_capability_closed_set_is_the_seven_spec_verbs() {
        use PublicLlmCapability::*;
        let all = [
            Normalize,
            Classify,
            Merge,
            Summarize,
            DetectContradiction,
            ProposeRelation,
            ProduceSynthesis,
        ];
        for c in all {
            // Exhaustive: a new variant fails to compile here until it is classified.
            match c {
                Normalize | Classify | Merge | Summarize | DetectContradiction
                | ProposeRelation | ProduceSynthesis => {}
            }
        }
        assert_eq!(
            all.len(),
            7,
            "spec:2709-2719 lists exactly seven capabilities"
        );
    }
}
