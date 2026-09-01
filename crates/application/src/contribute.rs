//! `application::contribute` — explicit private-to-public contribution entry workflow.
//!
//! Preparation derives candidate bytes through sealed `USER_REASONING`, hashes those exact bytes
//! at the canonical domain construction point, scans them with an injected adapter, and stores an
//! immutable candidate.  Finalization carries only an authenticated confirmation bound to that
//! digest.  The repository alone rechecks policy, authorization, source hashes, and confirmation
//! before creating a release, its source links, and its outbox record in one transaction.

use async_trait::async_trait;
use humaux_domain::{
    error::ErrorCode,
    evidence::{EvidencePayloadSha256, payload_sha256},
    identity::AuthorizationScope,
    public::{ContributionPolicy, ReleaseSource, RightsProvenance},
};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use uuid::Uuid;

use crate::consolidate::{
    ContentSha256, ContributionReasoningCallKind, LogicalReasoningCallId, PrivateReasoningDomainId,
    PrivateReasoningPort, PrivateReasoningPurpose, PrivateReasoningResult, ProviderTraceRef,
    ReasoningRouteBindingId, ReasoningRouteBindingVersion, SealedPrivateReasoningRequest,
    UserReasoningProfileVersion,
};

/// Authenticated request to prepare one contribution candidate.
///
/// `requested_sources` is only an intent.  The durable port reads every source under the supplied
/// scope and returns the trusted current hashes; callers never supply a source-manifest hash,
/// rights record, policy snapshot, or scan verdict.
#[derive(Clone)]
pub struct PrepareContribution {
    /// Authenticated actor and tenant.  This flow requires an on-behalf-of user.
    pub authorization: AuthorizationScope,
    /// Candidate source identities which the port validates under the authenticated scope.
    pub requested_sources: Vec<ReleaseSource>,
    /// Existing private-worker reasoning domain to use for de-identification.
    pub reasoning_domain_id: PrivateReasoningDomainId,
    /// Exact immutable reasoning route Binding selected by the authenticated caller.
    pub binding_id: ReasoningRouteBindingId,
    /// Exact immutable version of [`Self::binding_id`].
    pub binding_version: ReasoningRouteBindingVersion,
    /// Durable logical identity for the first USER_REASONING call.
    pub coverage_probe_call_id: LogicalReasoningCallId,
    /// Durable logical identity for the second USER_REASONING call.
    pub assessment_call_id: LogicalReasoningCallId,
}

/// ID-free preparation request for the R4 contribution execution enqueue path.
///
/// The repository that enqueues an execution owns minting the execution, job, A-call, B-call, and
/// candidate identifiers.  This request therefore carries only caller-owned admission and routing
/// inputs.
#[derive(Clone)]
pub struct ContributionPreparationInput {
    /// Authenticated actor and tenant. This flow requires an on-behalf-of user.
    pub authorization: AuthorizationScope,
    /// Candidate source identities which the repository validates under the authenticated scope.
    pub requested_sources: Vec<ReleaseSource>,
    /// Existing private-worker reasoning domain to use for de-identification.
    pub reasoning_domain_id: PrivateReasoningDomainId,
    /// Exact immutable reasoning route Binding selected by the authenticated caller.
    pub binding_id: ReasoningRouteBindingId,
    /// Exact immutable version of [`Self::binding_id`].
    pub binding_version: ReasoningRouteBindingVersion,
}

impl From<&PrepareContribution> for ContributionPreparationInput {
    fn from(request: &PrepareContribution) -> Self {
        Self {
            authorization: request.authorization.clone(),
            requested_sources: request.requested_sources.clone(),
            reasoning_domain_id: request.reasoning_domain_id,
            binding_id: request.binding_id,
            binding_version: request.binding_version,
        }
    }
}

/// Candidate identity returned after immutable disclosed bytes have been stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContributionCandidateId(pub Uuid);

/// Immutable release identity returned after finalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContributionReleaseId(pub Uuid);

/// Maximum number of untrusted public-coverage summaries that may enter one USER_REASONING call.
///
/// The Phase 9 caller is intentionally limited to this bounded digest: public content may inform
/// a user's own novelty decision, but it never gives a platform model a Phase 9 decision role.
pub const MAX_PUBLIC_COVERAGE_ITEMS: usize = 32;

/// Maximum UTF-8 bytes in one public-coverage summary.
pub const MAX_PUBLIC_COVERAGE_ITEM_BYTES: usize = 1_024;

/// Maximum bytes allowed in the de-identified probe sent to the public-coverage selector.
pub const MAX_CONTRIBUTION_COVERAGE_PROBE_BYTES: usize = 4_096;

/// Stable identity of the exact bounded public-coverage snapshot supplied to USER_REASONING.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicCoverageDigestBinding {
    /// Public-service snapshot identity; carries no contributor identity.
    digest_id: Uuid,
    /// Version of the canonical digest representation.
    coverage_version: u32,
    /// Canonical hash of that public-service snapshot.
    digest_sha256: ContentSha256,
}

impl PublicCoverageDigestBinding {
    /// Opaque public snapshot identity; it carries no tenant or contributor identity.
    pub fn digest_id(self) -> Uuid {
        self.digest_id
    }

    /// Version of the canonical public-coverage representation.
    pub fn coverage_version(self) -> u32 {
        self.coverage_version
    }

    /// Canonical digest over the versioned public-safe summaries.
    pub fn digest_sha256(self) -> ContentSha256 {
        self.digest_sha256
    }
}

/// Bounded, untrusted public coverage supplied only as context for a user's reasoning model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicCoverageDigest {
    binding: PublicCoverageDigestBinding,
    summaries: Vec<String>,
}

impl PublicCoverageDigest {
    /// Constructs bounded public context without allowing an unbounded public prompt to enter
    /// Phase 9. Empty summaries are rejected so the digest cannot hide malformed coverage input.
    pub fn new(
        digest_id: Uuid,
        coverage_version: u32,
        mut summaries: Vec<String>,
    ) -> Result<Self, ErrorCode> {
        if digest_id.is_nil()
            || coverage_version == 0
            || summaries.len() > MAX_PUBLIC_COVERAGE_ITEMS
            || summaries
                .iter()
                .any(|summary| summary.is_empty() || summary.len() > MAX_PUBLIC_COVERAGE_ITEM_BYTES)
        {
            return Err(ErrorCode::InvalidInput);
        }
        // The coverage SQL returns a bounded set. Its outer SELECT has no ordering contract, so
        // digest the exact set in a canonical order before it reaches USER_REASONING or CAS.
        summaries.sort_unstable();
        Ok(Self {
            binding: PublicCoverageDigestBinding {
                digest_id,
                coverage_version,
                digest_sha256: ContentSha256(
                    Sha256::digest(canonical_coverage_bytes(
                        digest_id,
                        coverage_version,
                        &summaries,
                    ))
                    .into(),
                ),
            },
            summaries,
        })
    }

    /// Reconstructs the exact canonical R4 representation persisted after stage A.
    ///
    /// The decoder rejects alternate encodings, unsorted summaries, trailing bytes, and every
    /// bound violation accepted by neither [`Self::new`] nor the original canonical encoder.
    /// This lets a resumed stage B consume the durable snapshot without querying current public
    /// knowledge or inventing another serialization.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, ErrorCode> {
        const PREFIX: &[u8] = b"humaux.phase9.public-coverage\0";
        let mut cursor = PREFIX.len();
        if !bytes.starts_with(PREFIX) {
            return Err(ErrorCode::InvalidInput);
        }

        let digest_id = Uuid::from_slice(take_canonical(bytes, &mut cursor, 16)?)
            .map_err(|_| ErrorCode::InvalidInput)?;
        let coverage_version = u32::from_be_bytes(
            take_canonical(bytes, &mut cursor, 4)?
                .try_into()
                .map_err(|_| ErrorCode::InvalidInput)?,
        );
        let item_count = u32::from_be_bytes(
            take_canonical(bytes, &mut cursor, 4)?
                .try_into()
                .map_err(|_| ErrorCode::InvalidInput)?,
        ) as usize;
        if item_count > MAX_PUBLIC_COVERAGE_ITEMS {
            return Err(ErrorCode::InvalidInput);
        }

        let mut summaries = Vec::with_capacity(item_count);
        for _ in 0..item_count {
            let length = u32::from_be_bytes(
                take_canonical(bytes, &mut cursor, 4)?
                    .try_into()
                    .map_err(|_| ErrorCode::InvalidInput)?,
            ) as usize;
            if length == 0 || length > MAX_PUBLIC_COVERAGE_ITEM_BYTES {
                return Err(ErrorCode::InvalidInput);
            }
            summaries.push(
                std::str::from_utf8(take_canonical(bytes, &mut cursor, length)?)
                    .map_err(|_| ErrorCode::InvalidInput)?
                    .to_owned(),
            );
        }
        if cursor != bytes.len() {
            return Err(ErrorCode::InvalidInput);
        }

        let decoded = Self::new(digest_id, coverage_version, summaries)?;
        if decoded.canonical_bytes() != bytes {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(decoded)
    }

    /// Identity the USER_REASONING response must echo exactly before its candidate can be stored.
    pub fn binding(&self) -> PublicCoverageDigestBinding {
        self.binding
    }

    /// Bounded summaries for the USER_REASONING port. They remain untrusted context, not policy.
    pub fn summaries(&self) -> &[String] {
        &self.summaries
    }

    /// Exact canonical bytes persisted by R4; callers must not invent a second serialization.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        canonical_coverage_bytes(
            self.binding.digest_id,
            self.binding.coverage_version,
            &self.summaries,
        )
    }
}

fn take_canonical<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    length: usize,
) -> Result<&'a [u8], ErrorCode> {
    let end = cursor
        .checked_add(length)
        .filter(|end| *end <= bytes.len())
        .ok_or(ErrorCode::InvalidInput)?;
    let value = &bytes[*cursor..end];
    *cursor = end;
    Ok(value)
}

fn canonical_coverage_bytes(
    digest_id: Uuid,
    coverage_version: u32,
    summaries: &[String],
) -> Vec<u8> {
    let mut bytes = b"humaux.phase9.public-coverage\0".to_vec();
    bytes.extend_from_slice(digest_id.as_bytes());
    bytes.extend_from_slice(&coverage_version.to_be_bytes());
    bytes.extend_from_slice(&(summaries.len() as u32).to_be_bytes());
    for summary in summaries {
        bytes.extend_from_slice(&(summary.len() as u32).to_be_bytes());
        bytes.extend_from_slice(summary.as_bytes());
    }
    bytes
}

/// A closed, explicit Phase 9 judgement. There is no default or permissive fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContributionGate {
    Pass,
    Fail,
}

/// Private-input-bound, public-safe semantic probe produced before public coverage selection.
///
/// The public-coverage port can read only public_safe_bytes. The source-manifest hash and provider
/// trace stay private to this application contract and are carried to persistence.
#[derive(Clone)]
pub struct ContributionCoverageProbe {
    source_manifest_hash: ContentSha256,
    output_bytes: Vec<u8>,
    output_sha256: EvidencePayloadSha256,
    provider_trace: ProviderTraceRef,
}

impl ContributionCoverageProbe {
    pub fn new(
        source_manifest_hash: ContentSha256,
        output_bytes: Vec<u8>,
        output_sha256: EvidencePayloadSha256,
        provider_trace: ProviderTraceRef,
    ) -> Result<Self, ErrorCode> {
        if output_bytes.is_empty()
            || output_bytes.len() > MAX_CONTRIBUTION_COVERAGE_PROBE_BYTES
            || payload_sha256(&output_bytes) != output_sha256
            || provider_trace.0.trim().is_empty()
        {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            source_manifest_hash,
            output_bytes,
            output_sha256,
            provider_trace,
        })
    }

    /// The only probe payload visible to PublicCoveragePort.
    pub fn public_safe_bytes(&self) -> &[u8] {
        &self.output_bytes
    }

    /// Protected binding retained with the candidate; never handed to `PublicCoveragePort`.
    pub fn source_manifest_hash(&self) -> ContentSha256 {
        self.source_manifest_hash
    }

    /// Exact hash of the public-safe probe bytes.
    pub fn output_sha256(&self) -> EvidencePayloadSha256 {
        self.output_sha256
    }

    /// USER_REASONING trace for the first private call.
    pub fn provider_trace(&self) -> &ProviderTraceRef {
        &self.provider_trace
    }

    fn matches_preparation(&self, preparation: &PreparationSnapshot) -> bool {
        self.source_manifest_hash == preparation.source_manifest_hash
            && self.output_sha256 == payload_sha256(&self.output_bytes)
            && !self.provider_trace.0.trim().is_empty()
    }
}

/// The first sealed USER_REASONING call: derive a de-identified coverage probe.
pub struct ContributionCoverageProbeRequest {
    pub reasoning: SealedPrivateReasoningRequest,
}

/// Request sent only to the user's BYOK reasoning boundary for gap-aware contribution assessment.
pub struct ContributionAssessmentRequest<'a> {
    /// Repository-sealed private source manifest; no public worker receives this capability.
    pub reasoning: SealedPrivateReasoningRequest,
    /// The exact public-safe probe used to select the returned coverage digest.
    pub coverage_probe: &'a ContributionCoverageProbe,
    /// Bounded, untrusted public coverage used solely for novelty/gap comparison.
    pub public_coverage: &'a PublicCoverageDigest,
}

/// USER_REASONING's complete Phase 9 response, including the de-identified candidate.
///
/// The response must bind to the exact [`PublicCoverageDigestBinding`] sent in the request. The
/// four gates are deliberately independent so a quality pass cannot mask a grounding failure.
#[derive(Clone)]
pub struct ContributionAssessment {
    pub coverage_probe_sha256: EvidencePayloadSha256,
    pub public_coverage_binding: PublicCoverageDigestBinding,
    pub novelty: ContributionGate,
    pub quality: ContributionGate,
    pub generality: ContributionGate,
    pub grounding: ContributionGate,
    pub deidentified_candidate: PrivateReasoningResult,
}

impl ContributionAssessment {
    fn accepts(&self, probe: &ContributionCoverageProbe, coverage: &PublicCoverageDigest) -> bool {
        self.coverage_probe_sha256 == probe.output_sha256
            && self.public_coverage_binding == coverage.binding()
            && self.novelty == ContributionGate::Pass
            && self.quality == ContributionGate::Pass
            && self.generality == ContributionGate::Pass
            && self.grounding == ContributionGate::Pass
            && !self.deidentified_candidate.output_bytes.is_empty()
            && payload_sha256(&self.deidentified_candidate.output_bytes).to_hex()
                == hex::encode(self.deidentified_candidate.output_sha256.0)
            && !self
                .deidentified_candidate
                .provider_trace
                .0
                .trim()
                .is_empty()
    }
}

/// USER_REASONING-only Phase 9 assessment boundary.
///
/// Implementations may use the authenticated user's BYOK model over private material and the
/// bounded digest. PLATFORM_PUBLIC models must not implement or participate in this port.
#[async_trait]
pub trait UserContributionAssessmentPort: Send + Sync {
    async fn derive_coverage_probe(
        &self,
        request: ContributionCoverageProbeRequest,
    ) -> Result<ContributionCoverageProbe, ErrorCode>;

    async fn assess(
        &self,
        request: ContributionAssessmentRequest<'_>,
    ) -> Result<ContributionAssessment, ErrorCode>;
}

/// Trusted application boundary for selecting one bounded, public-safe coverage snapshot.
///
/// Its summaries remain untrusted context for USER_REASONING, but the caller cannot choose the
/// snapshot or forge its binding. The production adapter invokes the narrow public coverage
/// function; it never receives a general public-table read capability.
#[async_trait]
pub trait PublicCoveragePort: Send + Sync {
    async fn load_public_coverage(
        &self,
        probe: &ContributionCoverageProbe,
    ) -> Result<PublicCoverageDigest, ErrorCode>;
}

/// A verified source identity and its current content hash at preparation time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContributionSourceSnapshot {
    /// Source visible to the authenticated user in the current tenant.
    pub source: ReleaseSource,
    /// Current raw-content hash, rechecked by storage and finalization to close TOCTOU.
    pub current_hash: ContentSha256,
}

/// Repository-derived, trusted state bound to a prepared candidate.
///
/// This type has public fields because the paired persistence adapter must write it, but it has no
/// deserialization surface and is only obtained from [`ContributionCandidatePort::load_preparation`].
/// In particular, policy, rights, authorization, source hashes, and sealed request are not caller
/// input.
#[derive(Clone)]
pub struct PreparationSnapshot {
    /// Authenticated scope captured while source visibility was checked.
    pub authorization: AuthorizationScope,
    /// Non-empty validated source identities and current hashes.
    pub sources: Vec<ContributionSourceSnapshot>,
    /// Hash over the repository-constructed source manifest.
    pub source_manifest_hash: ContentSha256,
    /// Durable contribution policy identity and immutable version.
    pub policy_id: Uuid,
    /// Durable contribution policy version.
    pub policy_version: i64,
    /// Active policy snapshot; disabled policy cannot yield a candidate.
    pub policy: ContributionPolicy,
    /// Trusted typed rights snapshot, never supplied by the external caller.
    pub rights: RightsProvenance,
    /// Exact existing private-worker request, including the source-manifest binding.
    pub reasoning: SealedPrivateReasoningRequest,
    /// Derived only from the exact Binding admission locator; never copied from caller input.
    pub resolved_profile_version: UserReasoningProfileVersion,
}

/// An authenticated confirmation of precisely one prepared candidate.
///
/// There is deliberately no boolean `approved` or caller-supplied scan field.  The repository
/// must match this receipt's user, tenant, candidate, digest, and policy version in its final
/// transaction before it creates a release.
#[derive(Clone)]
pub struct ConfirmContribution {
    /// Authenticated actor and tenant at finalization time.
    pub authorization: AuthorizationScope,
    /// Candidate the user previewed through the private-worker service.
    pub candidate_id: ContributionCandidateId,
    /// Exact bytes the user confirmed, represented by the canonical payload hash.
    pub candidate_payload_sha256: EvidencePayloadSha256,
    /// Immutable gateway-written confirmation receipt identifier.
    pub confirmation_id: Uuid,
}

/// The private candidate record passed directly from orchestration to durable storage.
///
/// `ScanReceipt` is an associated opaque type: an external caller cannot deserialize or
/// construct a `PASSED` verdict.  The concrete scanner creates it only after every required
/// stage succeeds, while its paired repository reads its persistence accessors.
pub struct StoredCandidate<'a, ScanReceipt> {
    /// Repository-derived state that binds policy, rights, authenticated user, and sources.
    pub preparation: &'a PreparationSnapshot,
    /// Immutable disclosed output bytes.  Never logged or sent to a public dispatcher.
    pub disclosed_bytes: &'a [u8],
    /// Sole canonical hash over `disclosed_bytes`.
    pub payload_sha256: EvidencePayloadSha256,
    /// Provider trace from the successful sealed private-worker response.
    pub provider_trace: &'a ProviderTraceRef,
    /// Exact successful ModelCallLedger receipt for the disclosed bytes.
    pub model_call_id: Uuid,
    /// Opaque scan attestation from the injected scanner pipeline.
    pub scan_receipt: &'a ScanReceipt,
}

/// Candidate record accepted by the additive Phase 9 gap-quality contract.
///
/// Its dedicated port keeps the decision and its public-coverage binding available for durable
/// storage when an adapter adopts this seam; the existing legacy port remains untouched.
pub struct AssessedStoredCandidate<'a, ScanReceipt> {
    candidate: StoredCandidate<'a, ScanReceipt>,
    /// First USER_REASONING response, bound to the trusted source manifest and probe scan.
    coverage_probe: &'a ContributionCoverageProbe,
    /// Canonically bound public coverage selected from the public-safe probe.
    public_coverage: &'a PublicCoverageDigest,
    /// Receipt proving the exact probe bytes passed deterministic scanning.
    probe_scan_receipt: &'a ScanReceipt,
    /// Second USER_REASONING response bound to the probe and coverage digest.
    assessment: &'a ContributionAssessment,
}

impl<'a, ScanReceipt> AssessedStoredCandidate<'a, ScanReceipt> {
    /// Validates and binds the only value that may cross the assessed persistence seam.
    ///
    /// Keeping these fields private prevents callers from pairing a scanned candidate with an
    /// assessment for another probe or coverage snapshot. The concrete repository still verifies
    /// its opaque scan receipts before durable storage.
    pub fn try_bind(
        candidate: StoredCandidate<'a, ScanReceipt>,
        coverage_probe: &'a ContributionCoverageProbe,
        public_coverage: &'a PublicCoverageDigest,
        probe_scan_receipt: &'a ScanReceipt,
        assessment: &'a ContributionAssessment,
    ) -> Result<Self, ErrorCode> {
        if !coverage_probe.matches_preparation(candidate.preparation)
            || !assessment.accepts(coverage_probe, public_coverage)
            || candidate.disclosed_bytes != assessment.deidentified_candidate.output_bytes
            || candidate.payload_sha256 != payload_sha256(candidate.disclosed_bytes)
            || candidate.payload_sha256.to_hex()
                != hex::encode(assessment.deidentified_candidate.output_sha256.0)
            || candidate.provider_trace != &assessment.deidentified_candidate.provider_trace
            || candidate.model_call_id != assessment.deidentified_candidate.model_call_id
            || assessment.deidentified_candidate.binding_id
                != candidate.preparation.reasoning.binding_id
            || assessment.deidentified_candidate.binding_version
                != candidate.preparation.reasoning.binding_version
        {
            return Err(ErrorCode::Conflict);
        }
        Ok(Self {
            candidate,
            coverage_probe,
            public_coverage,
            probe_scan_receipt,
            assessment,
        })
    }

    pub fn candidate(&self) -> &StoredCandidate<'a, ScanReceipt> {
        &self.candidate
    }

    pub fn coverage_probe(&self) -> &'a ContributionCoverageProbe {
        self.coverage_probe
    }

    pub fn public_coverage(&self) -> &'a PublicCoverageDigest {
        self.public_coverage
    }

    pub fn probe_scan_receipt(&self) -> &'a ScanReceipt {
        self.probe_scan_receipt
    }

    pub fn assessment(&self) -> &'a ContributionAssessment {
        self.assessment
    }

    pub fn into_parts(
        self,
    ) -> (
        StoredCandidate<'a, ScanReceipt>,
        &'a ContributionCoverageProbe,
        &'a PublicCoverageDigest,
        &'a ScanReceipt,
        &'a ContributionAssessment,
    ) {
        (
            self.candidate,
            self.coverage_probe,
            self.public_coverage,
            self.probe_scan_receipt,
            self.assessment,
        )
    }
}

/// Scanner boundary for disclosed candidate bytes.
#[async_trait]
pub trait ContributionScannerPort: Send + Sync {
    /// Scanner-specific durable attestation type, created only on successful scanning.
    type Receipt: Send + Sync;

    /// Runs all mandatory privacy and secret-scanner stages over the exact disclosed bytes.
    async fn scan_disclosed_bytes(&self, bytes: &[u8]) -> Result<Self::Receipt, ErrorCode>;
}

/// Durable candidate/finalization boundary.
///
/// The implementation owns private repository I/O.  It must construct the preparation snapshot
/// under real authorization and recheck it before persistence; finalization must atomically check
/// the gateway confirmation and current policy/source state before writing release, sources, and
/// outbox.  It returns no payload, so public dispatch can accept only a release identifier.
#[async_trait]
pub trait ContributionCandidatePort: Send + Sync {
    /// Must be the opaque receipt produced by the paired [`ContributionScannerPort`].
    type ScanReceipt: Send + Sync;

    /// Loads current, trusted preparation state without requiring caller-minted execution IDs.
    async fn load_execution_preparation(
        &self,
        request: &ContributionPreparationInput,
    ) -> Result<PreparationSnapshot, ErrorCode>;

    /// Legacy preparation wrapper. Logical call IDs remain local to legacy reasoning orchestration.
    async fn load_preparation(
        &self,
        request: &PrepareContribution,
    ) -> Result<PreparationSnapshot, ErrorCode> {
        self.load_execution_preparation(&ContributionPreparationInput::from(request))
            .await
    }

    /// Persists one immutable candidate and all repository-owned durable bindings.
    async fn store_prepared(
        &self,
        candidate: StoredCandidate<'_, Self::ScanReceipt>,
    ) -> Result<ContributionCandidateId, ErrorCode>;

    /// Consumes a durable authenticated confirmation and creates one release transactionally.
    async fn finalize_confirmed(
        &self,
        confirmation: ConfirmContribution,
    ) -> Result<ContributionReleaseId, ErrorCode>;
}

/// Additive persistence seam for candidates that passed the USER_REASONING gap-quality contract.
///
/// The production adapter persists the assessment binding and immutable candidate in one
/// transaction; any additional adapter must provide the same atomicity.
#[async_trait]
pub trait AssessedContributionCandidatePort: ContributionCandidatePort {
    async fn store_assessed_prepared(
        &self,
        candidate: AssessedStoredCandidate<'_, Self::ScanReceipt>,
    ) -> Result<ContributionCandidateId, ErrorCode>;
}

fn preparation_matches(
    request: &ContributionPreparationInput,
    preparation: &PreparationSnapshot,
) -> bool {
    let snapshot_sources = preparation
        .sources
        .iter()
        .map(|source| source.source)
        .collect::<HashSet<_>>();
    preparation.authorization == request.authorization
        && preparation.sources.len() == snapshot_sources.len()
        && snapshot_sources.len() == request.requested_sources.len()
        && request
            .requested_sources
            .iter()
            .all(|source| snapshot_sources.contains(source))
        && preparation.policy != ContributionPolicy::Disabled
        && preparation.reasoning.purpose == PrivateReasoningPurpose::ContributionDeidentify
        && preparation.reasoning.reasoning_domain_id == request.reasoning_domain_id
        && preparation.reasoning.binding_id == request.binding_id
        && preparation.reasoning.binding_version == request.binding_version
        && preparation.resolved_profile_version.0 > 0
        && preparation.reasoning.input_manifest_hash == preparation.source_manifest_hash
        && preparation.reasoning.contribution_attempt.is_none()
}

fn contribution_reasoning_call(
    base: SealedPrivateReasoningRequest,
    logical_call_id: LogicalReasoningCallId,
    kind: ContributionReasoningCallKind,
    coverage_digest: Option<ContentSha256>,
) -> SealedPrivateReasoningRequest {
    base.with_contribution_attempt(logical_call_id, kind, coverage_digest)
}

/// Already-authorized, repository-derived preparation for the R4 enqueue command.
///
/// Fields stay private so the typed value can only be produced by [`prepare_assessed_input`].
#[derive(Clone)]
pub struct PreparedAssessedContribution {
    request: ContributionPreparationInput,
    preparation: PreparationSnapshot,
}

impl PreparedAssessedContribution {
    /// Binds a repository-loaded current snapshot to its exact ID-free request.
    ///
    /// Adapters use this constructor when preparation and durable enqueue share one database
    /// transaction.  The caller still cannot supply execution identities, policy, rights, source
    /// hashes, or a fingerprint; those values must already be present in the repository snapshot.
    pub fn try_from_current(
        request: ContributionPreparationInput,
        preparation: PreparationSnapshot,
    ) -> Result<Self, ErrorCode> {
        if request.authorization.user_id().is_none() {
            return Err(ErrorCode::Unauthorized);
        }
        if request.requested_sources.is_empty()
            || request
                .requested_sources
                .iter()
                .collect::<HashSet<_>>()
                .len()
                != request.requested_sources.len()
        {
            return Err(ErrorCode::InvalidInput);
        }
        if !preparation_matches(&request, &preparation) {
            return Err(ErrorCode::Conflict);
        }
        Ok(Self {
            request,
            preparation,
        })
    }

    pub const fn request(&self) -> &ContributionPreparationInput {
        &self.request
    }

    pub const fn preparation(&self) -> &PreparationSnapshot {
        &self.preparation
    }

    pub fn into_preparation(self) -> PreparationSnapshot {
        self.preparation
    }
}

/// R4 preparation seam: loads and validates trusted state without caller-minted execution IDs.
/// It performs no reasoning call, scan, candidate storage, provider dispatch, or ID minting.
pub async fn prepare_assessed_input<C>(
    request: ContributionPreparationInput,
    candidates: &C,
) -> Result<PreparedAssessedContribution, ErrorCode>
where
    C: ContributionCandidatePort + ?Sized,
{
    let preparation = candidates.load_execution_preparation(&request).await?;
    PreparedAssessedContribution::try_from_current(request, preparation)
}

/// Derives, scans, and stores an immutable contribution candidate.
///
/// Legacy-only compatibility prepare path. It must never feed anonymous admission because it
/// lacks the Phase 9 USER gap-quality bindings; new Phase 9 callers use [`prepare_assessed`].
/// No release or outbox event is created on inference, integrity, scan, or storage failure.
#[deprecated(note = "legacy-only; Phase 9 anonymous admission requires prepare_assessed")]
pub async fn prepare<R, S, C>(
    request: PrepareContribution,
    reasoning_port: &R,
    scanner: &S,
    candidates: &C,
) -> Result<ContributionCandidateId, ErrorCode>
where
    R: PrivateReasoningPort + ?Sized,
    S: ContributionScannerPort + ?Sized,
    C: ContributionCandidatePort<ScanReceipt = S::Receipt> + ?Sized,
{
    if request.authorization.user_id().is_none() {
        return Err(ErrorCode::Unauthorized);
    }
    if request.requested_sources.is_empty()
        || request
            .requested_sources
            .iter()
            .collect::<HashSet<_>>()
            .len()
            != request.requested_sources.len()
    {
        return Err(ErrorCode::InvalidInput);
    }

    let preparation = candidates.load_preparation(&request).await?;
    if !preparation_matches(&ContributionPreparationInput::from(&request), &preparation) {
        return Err(ErrorCode::Conflict);
    }

    let sealed = contribution_reasoning_call(
        preparation.reasoning,
        request.assessment_call_id,
        ContributionReasoningCallKind::TypedAssessment,
        None,
    );
    let PrivateReasoningResult {
        output_bytes,
        output_sha256,
        provider_trace,
        model_call_id,
        binding_id,
        binding_version,
    } = reasoning_port
        .infer(sealed)
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    if binding_id != sealed.binding_id || binding_version != sealed.binding_version {
        return Err(ErrorCode::Conflict);
    }
    let candidate_hash = payload_sha256(&output_bytes);
    if candidate_hash.to_hex() != hex::encode(output_sha256.0) {
        return Err(ErrorCode::Conflict);
    }
    let scan_receipt = scanner.scan_disclosed_bytes(&output_bytes).await?;

    candidates
        .store_prepared(StoredCandidate {
            preparation: &preparation,
            disclosed_bytes: &output_bytes,
            payload_sha256: candidate_hash,
            provider_trace: &provider_trace,
            model_call_id,
            scan_receipt: &scan_receipt,
        })
        .await
}

/// Runs the Phase 9 USER_REASONING gap-quality contract and stores only a fully accepted result.
///
/// This additive API intentionally has no PLATFORM_PUBLIC reasoning dependency. It can be
/// integrated by a persistence adapter later; until then its typed port and tests are an
/// application-layer contract, not evidence of production adapter adoption.
pub async fn prepare_assessed<A, P, S, C>(
    request: PrepareContribution,
    assessment_port: &A,
    public_coverage_port: &P,
    scanner: &S,
    candidates: &C,
) -> Result<ContributionCandidateId, ErrorCode>
where
    A: UserContributionAssessmentPort + ?Sized,
    P: PublicCoveragePort + ?Sized,
    S: ContributionScannerPort + ?Sized,
    C: AssessedContributionCandidatePort<ScanReceipt = S::Receipt> + ?Sized,
{
    if request.authorization.user_id().is_none() {
        return Err(ErrorCode::Unauthorized);
    }
    if request.requested_sources.is_empty()
        || request.coverage_probe_call_id.0.is_nil()
        || request.assessment_call_id.0.is_nil()
        || request.coverage_probe_call_id == request.assessment_call_id
        || request
            .requested_sources
            .iter()
            .collect::<HashSet<_>>()
            .len()
            != request.requested_sources.len()
    {
        return Err(ErrorCode::InvalidInput);
    }

    let coverage_probe_call_id = request.coverage_probe_call_id;
    let assessment_call_id = request.assessment_call_id;
    let prepared =
        prepare_assessed_input(ContributionPreparationInput::from(&request), candidates).await?;
    let preparation = prepared.preparation();
    let coverage_probe = assessment_port
        .derive_coverage_probe(ContributionCoverageProbeRequest {
            reasoning: contribution_reasoning_call(
                preparation.reasoning,
                coverage_probe_call_id,
                ContributionReasoningCallKind::CoverageProbe,
                None,
            ),
        })
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    if !coverage_probe.matches_preparation(preparation) {
        return Err(ErrorCode::Conflict);
    }
    let probe_scan_receipt = scanner
        .scan_disclosed_bytes(coverage_probe.public_safe_bytes())
        .await?;
    let public_coverage = public_coverage_port
        .load_public_coverage(&coverage_probe)
        .await?;

    let assessment = assessment_port
        .assess(ContributionAssessmentRequest {
            reasoning: contribution_reasoning_call(
                preparation.reasoning,
                assessment_call_id,
                ContributionReasoningCallKind::TypedAssessment,
                Some(public_coverage.binding().digest_sha256()),
            ),
            coverage_probe: &coverage_probe,
            public_coverage: &public_coverage,
        })
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    if !assessment.accepts(&coverage_probe, &public_coverage) {
        return Err(ErrorCode::Conflict);
    }

    let disclosed_bytes = &assessment.deidentified_candidate.output_bytes;
    let scan_receipt = scanner.scan_disclosed_bytes(disclosed_bytes).await?;
    candidates
        .store_assessed_prepared(AssessedStoredCandidate::try_bind(
            StoredCandidate {
                preparation,
                disclosed_bytes,
                payload_sha256: payload_sha256(disclosed_bytes),
                provider_trace: &assessment.deidentified_candidate.provider_trace,
                model_call_id: assessment.deidentified_candidate.model_call_id,
                scan_receipt: &scan_receipt,
            },
            &coverage_probe,
            &public_coverage,
            &probe_scan_receipt,
            &assessment,
        )?)
        .await
}

/// Finalizes an authenticated confirmation without exposing candidate bytes to public callers.
pub async fn finalize<C>(
    confirmation: ConfirmContribution,
    candidates: &C,
) -> Result<ContributionReleaseId, ErrorCode>
where
    C: ContributionCandidatePort + ?Sized,
{
    if confirmation.authorization.user_id().is_none() {
        return Err(ErrorCode::Unauthorized);
    }
    candidates.finalize_confirmed(confirmation).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use humaux_domain::{
        authority::EvidenceId,
        identity::{BoundedSet, PrincipalId},
        ids::{TenantId, UserId, WorkspaceId},
    };
    use sha2::{Digest, Sha256};
    use std::sync::Mutex;

    #[derive(Clone, Copy)]
    struct Receipt;

    struct Scanner(Result<Receipt, ErrorCode>);

    #[async_trait]
    impl ContributionScannerPort for Scanner {
        type Receipt = Receipt;

        async fn scan_disclosed_bytes(&self, _bytes: &[u8]) -> Result<Self::Receipt, ErrorCode> {
            self.0
        }
    }

    #[derive(Default)]
    struct Candidates {
        stored: Mutex<Vec<(Vec<u8>, EvidencePayloadSha256)>>,
        finalized: Mutex<usize>,
    }

    #[async_trait]
    impl ContributionCandidatePort for Candidates {
        type ScanReceipt = Receipt;

        async fn load_execution_preparation(
            &self,
            request: &ContributionPreparationInput,
        ) -> Result<PreparationSnapshot, ErrorCode> {
            fixture_preparation(request)
        }

        async fn store_prepared(
            &self,
            candidate: StoredCandidate<'_, Self::ScanReceipt>,
        ) -> Result<ContributionCandidateId, ErrorCode> {
            self.stored
                .lock()
                .expect("test mutex")
                .push((candidate.disclosed_bytes.to_vec(), candidate.payload_sha256));
            Ok(ContributionCandidateId(Uuid::nil()))
        }

        async fn finalize_confirmed(
            &self,
            _confirmation: ConfirmContribution,
        ) -> Result<ContributionReleaseId, ErrorCode> {
            *self.finalized.lock().expect("test mutex") += 1;
            Ok(ContributionReleaseId(Uuid::nil()))
        }
    }

    #[async_trait]
    impl AssessedContributionCandidatePort for Candidates {
        async fn store_assessed_prepared(
            &self,
            candidate: AssessedStoredCandidate<'_, Self::ScanReceipt>,
        ) -> Result<ContributionCandidateId, ErrorCode> {
            self.stored.lock().expect("test mutex").push((
                candidate.candidate.disclosed_bytes.to_vec(),
                candidate.candidate.payload_sha256,
            ));
            Ok(ContributionCandidateId(Uuid::nil()))
        }
    }

    struct Reasoning(PrivateReasoningResult);

    #[async_trait]
    impl PrivateReasoningPort for Reasoning {
        async fn infer(
            &self,
            _request: SealedPrivateReasoningRequest,
        ) -> Result<PrivateReasoningResult, crate::consolidate::PrivateReasoningError> {
            Ok(self.0.clone())
        }
    }

    struct Assessment {
        probe: ContributionCoverageProbe,
        assessment: ContributionAssessment,
    }

    #[async_trait]
    impl UserContributionAssessmentPort for Assessment {
        async fn derive_coverage_probe(
            &self,
            _request: ContributionCoverageProbeRequest,
        ) -> Result<ContributionCoverageProbe, ErrorCode> {
            Ok(self.probe.clone())
        }

        async fn assess(
            &self,
            _request: ContributionAssessmentRequest<'_>,
        ) -> Result<ContributionAssessment, ErrorCode> {
            Ok(self.assessment.clone())
        }
    }

    struct Coverage {
        digest: PublicCoverageDigest,
        calls: Mutex<usize>,
    }

    impl Coverage {
        fn new(digest: PublicCoverageDigest) -> Self {
            Self {
                digest,
                calls: Mutex::new(0),
            }
        }
    }

    #[async_trait]
    impl PublicCoveragePort for Coverage {
        async fn load_public_coverage(
            &self,
            _probe: &ContributionCoverageProbe,
        ) -> Result<PublicCoverageDigest, ErrorCode> {
            *self.calls.lock().expect("test mutex") += 1;
            Ok(self.digest.clone())
        }
    }

    struct RecordingScanner {
        calls: Mutex<Vec<Vec<u8>>>,
        fail_on_call: Option<usize>,
    }

    impl RecordingScanner {
        fn passing() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                fail_on_call: None,
            }
        }

        fn failing_first() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                fail_on_call: Some(1),
            }
        }
    }

    #[async_trait]
    impl ContributionScannerPort for RecordingScanner {
        type Receipt = Receipt;

        async fn scan_disclosed_bytes(&self, _bytes: &[u8]) -> Result<Self::Receipt, ErrorCode> {
            let call = {
                let mut calls = self.calls.lock().expect("test mutex");
                calls.push(_bytes.to_vec());
                calls.len()
            };
            if self.fail_on_call == Some(call) {
                return Err(ErrorCode::DependencyUnavailable);
            }
            Ok(Receipt)
        }
    }

    fn scope(user_id: Option<UserId>) -> AuthorizationScope {
        AuthorizationScope::new(
            TenantId::new(),
            PrincipalId::new(),
            user_id,
            BoundedSet::new([WorkspaceId::new()]).expect("one workspace"),
        )
    }

    fn fixture_preparation(
        request: &ContributionPreparationInput,
    ) -> Result<PreparationSnapshot, ErrorCode> {
        let source = *request
            .requested_sources
            .first()
            .ok_or(ErrorCode::InvalidInput)?;
        Ok(PreparationSnapshot {
            authorization: request.authorization.clone(),
            sources: vec![ContributionSourceSnapshot {
                source,
                current_hash: ContentSha256([3; 32]),
            }],
            source_manifest_hash: ContentSha256([4; 32]),
            policy_id: Uuid::nil(),
            policy_version: 1,
            policy: ContributionPolicy::Manual,
            rights: RightsProvenance::new("fixture-rights".to_owned(), None, None, None, None)
                .expect("valid fixture rights"),
            reasoning: SealedPrivateReasoningRequest {
                reasoning_domain_id: request.reasoning_domain_id,
                binding_id: request.binding_id,
                binding_version: request.binding_version,
                input_manifest_hash: ContentSha256([4; 32]),
                purpose: PrivateReasoningPurpose::ContributionDeidentify,
                contribution_attempt: None,
            },
            resolved_profile_version: UserReasoningProfileVersion(1),
        })
    }

    fn request(scope: AuthorizationScope) -> PrepareContribution {
        PrepareContribution {
            authorization: scope,
            requested_sources: vec![ReleaseSource::Evidence(EvidenceId(Uuid::nil()))],
            reasoning_domain_id: PrivateReasoningDomainId(Uuid::nil()),
            binding_id: ReasoningRouteBindingId(Uuid::from_u128(5)),
            binding_version: ReasoningRouteBindingVersion(1),
            coverage_probe_call_id: LogicalReasoningCallId(Uuid::from_u128(6)),
            assessment_call_id: LogicalReasoningCallId(Uuid::from_u128(7)),
        }
    }

    fn reasoning(bytes: &[u8]) -> Reasoning {
        Reasoning(PrivateReasoningResult {
            output_bytes: bytes.to_vec(),
            output_sha256: ContentSha256(Sha256::digest(bytes).into()),
            provider_trace: ProviderTraceRef("fixture".to_owned()),
            model_call_id: Uuid::from_u128(9),
            binding_id: ReasoningRouteBindingId(Uuid::from_u128(5)),
            binding_version: ReasoningRouteBindingVersion(1),
        })
    }

    fn coverage(seed: u8) -> PublicCoverageDigest {
        PublicCoverageDigest::new(
            Uuid::from_u128(u128::from(seed) + 1),
            1,
            vec!["untrusted public coverage summary".to_owned()],
        )
        .expect("bounded fixture coverage")
    }

    fn probe(manifest: ContentSha256) -> ContributionCoverageProbe {
        let bytes = b"deidentified coverage probe".to_vec();
        ContributionCoverageProbe::new(
            manifest,
            bytes.clone(),
            payload_sha256(&bytes),
            ProviderTraceRef("probe fixture".to_owned()),
        )
        .expect("valid fixture probe")
    }

    fn accepted_assessment(
        probe: ContributionCoverageProbe,
        binding: PublicCoverageDigestBinding,
    ) -> Assessment {
        Assessment {
            assessment: ContributionAssessment {
                coverage_probe_sha256: probe.output_sha256,
                public_coverage_binding: binding,
                novelty: ContributionGate::Pass,
                quality: ContributionGate::Pass,
                generality: ContributionGate::Pass,
                grounding: ContributionGate::Pass,
                deidentified_candidate: reasoning(b"deidentified candidate").0,
            },
            probe,
        }
    }

    #[tokio::test]
    async fn contribution_execution_preparation_seam_accepts_no_caller_minted_ids() {
        let request = ContributionPreparationInput {
            authorization: scope(Some(UserId::new())),
            requested_sources: vec![ReleaseSource::Evidence(EvidenceId(Uuid::nil()))],
            reasoning_domain_id: PrivateReasoningDomainId(Uuid::nil()),
            binding_id: ReasoningRouteBindingId(Uuid::from_u128(5)),
            binding_version: ReasoningRouteBindingVersion(1),
        };
        let prepared = prepare_assessed_input(request, &Candidates::default())
            .await
            .expect("valid R4 preparation");

        assert_eq!(prepared.request().binding_version.0, 1);
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn prepare_hashes_exact_bytes_scans_then_stores() {
        let candidates = Candidates::default();
        let id = prepare(
            request(scope(Some(UserId::new()))),
            &reasoning(b"candidate\r\n"),
            &Scanner(Ok(Receipt)),
            &candidates,
        )
        .await
        .expect("prepared candidate");

        assert_eq!(id, ContributionCandidateId(Uuid::nil()));
        let stored = candidates.stored.lock().expect("test mutex");
        assert_eq!(stored[0].0, b"candidate\r\n");
        assert_eq!(stored[0].1, payload_sha256(b"candidate\r\n"));
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn missing_user_or_scan_failure_never_stores_a_candidate() {
        let candidates = Candidates::default();
        let missing_user = prepare(
            request(scope(None)),
            &reasoning(b"candidate"),
            &Scanner(Ok(Receipt)),
            &candidates,
        )
        .await
        .expect_err("unbound user must fail");
        assert_eq!(missing_user, ErrorCode::Unauthorized);

        let scan_failed = prepare(
            request(scope(Some(UserId::new()))),
            &reasoning(b"candidate"),
            &Scanner(Err(ErrorCode::DependencyUnavailable)),
            &candidates,
        )
        .await
        .expect_err("scanner must fail closed");
        assert_eq!(scan_failed, ErrorCode::DependencyUnavailable);
        assert!(candidates.stored.lock().expect("test mutex").is_empty());
    }

    #[tokio::test]
    async fn assessed_prepare_rejects_every_failed_gap_quality_gate_before_storage() {
        for failed_gate in 0..4 {
            let coverage = coverage(failed_gate);
            let probe = probe(ContentSha256([4; 32]));
            let mut assessment = accepted_assessment(probe.clone(), coverage.binding());
            match failed_gate {
                0 => assessment.assessment.novelty = ContributionGate::Fail,
                1 => assessment.assessment.quality = ContributionGate::Fail,
                2 => assessment.assessment.generality = ContributionGate::Fail,
                3 => assessment.assessment.grounding = ContributionGate::Fail,
                _ => unreachable!("four Phase 9 gates"),
            }
            let candidates = Candidates::default();
            let scanner = RecordingScanner::passing();
            let coverage_port = Coverage::new(coverage);
            let error = prepare_assessed(
                request(scope(Some(UserId::new()))),
                &assessment,
                &coverage_port,
                &scanner,
                &candidates,
            )
            .await
            .expect_err("failed USER_REASONING gate must fail closed");
            assert_eq!(error, ErrorCode::Conflict);
            assert!(candidates.stored.lock().expect("test mutex").is_empty());
            let calls = scanner.calls.lock().expect("test mutex");
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0], probe.public_safe_bytes());
            assert_eq!(*coverage_port.calls.lock().expect("test mutex"), 1);
        }
    }

    #[tokio::test]
    async fn assessed_prepare_rejects_public_coverage_response_binding_mismatch() {
        let requested_coverage = coverage(7);
        let mismatched_coverage = PublicCoverageDigest::new(
            Uuid::from_u128(7),
            1,
            vec!["different untrusted public coverage summary".to_owned()],
        )
        .expect("canonical fixture coverage");
        let probe = probe(ContentSha256([4; 32]));
        let candidates = Candidates::default();
        let error = prepare_assessed(
            request(scope(Some(UserId::new()))),
            &accepted_assessment(probe, mismatched_coverage.binding()),
            &Coverage::new(requested_coverage),
            &Scanner(Ok(Receipt)),
            &candidates,
        )
        .await
        .expect_err("assessment for a different coverage snapshot must fail closed");

        assert_eq!(error, ErrorCode::Conflict);
        assert!(candidates.stored.lock().expect("test mutex").is_empty());
    }

    #[tokio::test]
    async fn assessed_storage_value_rejects_direct_cross_binding_or_candidate_mutation() {
        let candidates = Candidates::default();
        let request = request(scope(Some(UserId::new())));
        let preparation = candidates
            .load_preparation(&request)
            .await
            .expect("fixture preparation");
        let probe = probe(preparation.source_manifest_hash);
        let accepted_coverage = coverage(13);
        let different_coverage = coverage(14);
        let assessment = accepted_assessment(probe.clone(), accepted_coverage.binding()).assessment;
        let candidate_bytes = assessment.deidentified_candidate.output_bytes.clone();
        let receipt = Receipt;

        let cross_binding = AssessedStoredCandidate::try_bind(
            StoredCandidate {
                preparation: &preparation,
                disclosed_bytes: &candidate_bytes,
                payload_sha256: payload_sha256(&candidate_bytes),
                provider_trace: &assessment.deidentified_candidate.provider_trace,
                model_call_id: assessment.deidentified_candidate.model_call_id,
                scan_receipt: &receipt,
            },
            &probe,
            &different_coverage,
            &receipt,
            &assessment,
        );
        assert_eq!(cross_binding.err(), Some(ErrorCode::Conflict));

        let substituted_bytes = b"different candidate";
        let substituted_candidate = AssessedStoredCandidate::try_bind(
            StoredCandidate {
                preparation: &preparation,
                disclosed_bytes: substituted_bytes,
                payload_sha256: payload_sha256(substituted_bytes),
                provider_trace: &assessment.deidentified_candidate.provider_trace,
                model_call_id: assessment.deidentified_candidate.model_call_id,
                scan_receipt: &receipt,
            },
            &probe,
            &accepted_coverage,
            &receipt,
            &assessment,
        );
        assert_eq!(substituted_candidate.err(), Some(ErrorCode::Conflict));
    }

    #[tokio::test]
    async fn assessed_prepare_rejects_probe_manifest_or_hash_mismatch_before_scanning() {
        for mismatch in 0..2 {
            let coverage = coverage(11);
            let mut probe = probe(ContentSha256([4; 32]));
            if mismatch == 0 {
                probe.source_manifest_hash = ContentSha256([9; 32]);
            } else {
                probe.output_sha256 = payload_sha256(b"different probe bytes");
            }
            let candidates = Candidates::default();
            let scanner = RecordingScanner::passing();
            let coverage_port = Coverage::new(coverage.clone());
            let error = prepare_assessed(
                request(scope(Some(UserId::new()))),
                &accepted_assessment(probe, coverage.binding()),
                &coverage_port,
                &scanner,
                &candidates,
            )
            .await
            .expect_err("unbound coverage probe must fail closed");

            assert_eq!(error, ErrorCode::Conflict);
            assert!(scanner.calls.lock().expect("test mutex").is_empty());
            assert_eq!(*coverage_port.calls.lock().expect("test mutex"), 0);
            assert!(candidates.stored.lock().expect("test mutex").is_empty());
        }
    }

    #[tokio::test]
    async fn probe_scan_failure_never_calls_public_coverage() {
        let coverage = coverage(12);
        let candidates = Candidates::default();
        let scanner = RecordingScanner::failing_first();
        let coverage_port = Coverage::new(coverage.clone());
        let error = prepare_assessed(
            request(scope(Some(UserId::new()))),
            &accepted_assessment(probe(ContentSha256([4; 32])), coverage.binding()),
            &coverage_port,
            &scanner,
            &candidates,
        )
        .await
        .expect_err("probe scan must fail closed");

        assert_eq!(error, ErrorCode::DependencyUnavailable);
        assert_eq!(scanner.calls.lock().expect("test mutex").len(), 1);
        assert_eq!(*coverage_port.calls.lock().expect("test mutex"), 0);
        assert!(candidates.stored.lock().expect("test mutex").is_empty());
    }

    #[test]
    fn canonical_coverage_binding_commits_to_version_and_summaries() {
        let id = Uuid::from_u128(42);
        let base = PublicCoverageDigest::new(id, 1, vec!["summary".to_owned()])
            .expect("canonical fixture coverage");
        let changed_summary = PublicCoverageDigest::new(id, 1, vec!["other summary".to_owned()])
            .expect("canonical fixture coverage");
        let changed_version = PublicCoverageDigest::new(id, 2, vec!["summary".to_owned()])
            .expect("canonical fixture coverage");

        assert_ne!(base.binding(), changed_summary.binding());
        assert_ne!(base.binding(), changed_version.binding());
    }

    #[test]
    fn coverage_digest_canonicalizes_unordered_public_rows() {
        let id = Uuid::from_u128(43);
        let ordered = PublicCoverageDigest::new(id, 1, vec!["a".to_owned(), "b".to_owned()])
            .expect("canonical fixture coverage");
        let unordered = PublicCoverageDigest::new(id, 1, vec!["b".to_owned(), "a".to_owned()])
            .expect("canonical fixture coverage");

        assert_eq!(ordered.binding(), unordered.binding());
        assert_eq!(unordered.summaries(), ["a", "b"]);
    }

    #[test]
    fn coverage_digest_strictly_reconstructs_persisted_canonical_bytes() {
        let id = Uuid::from_u128(44);
        let original =
            PublicCoverageDigest::new(id, 3, vec!["second".to_owned(), "first".to_owned()])
                .expect("canonical fixture coverage");
        let canonical = original.canonical_bytes();
        let restored = PublicCoverageDigest::from_canonical_bytes(&canonical)
            .expect("canonical coverage must round trip");

        assert_eq!(restored, original);

        let mut trailing = canonical.clone();
        trailing.push(0);
        assert_eq!(
            PublicCoverageDigest::from_canonical_bytes(&trailing).unwrap_err(),
            ErrorCode::InvalidInput
        );
        assert_eq!(
            PublicCoverageDigest::from_canonical_bytes(&canonical[..canonical.len() - 1])
                .unwrap_err(),
            ErrorCode::InvalidInput
        );

        let noncanonical =
            canonical_coverage_bytes(id, 3, &["second".to_owned(), "first".to_owned()]);
        assert_eq!(
            PublicCoverageDigest::from_canonical_bytes(&noncanonical).unwrap_err(),
            ErrorCode::InvalidInput
        );
    }

    #[test]
    fn coverage_digest_rejects_empty_binding_identity() {
        assert!(PublicCoverageDigest::new(Uuid::nil(), 1, vec![]).is_err());
        assert!(PublicCoverageDigest::new(Uuid::from_u128(1), 0, vec![]).is_err());
    }

    #[tokio::test]
    async fn assessed_prepare_loads_trusted_coverage_then_scans_and_stores() {
        let coverage = coverage(9);
        let candidates = Candidates::default();
        let scanner = RecordingScanner::passing();
        let probe = probe(ContentSha256([4; 32]));
        let id = prepare_assessed(
            request(scope(Some(UserId::new()))),
            &accepted_assessment(probe.clone(), coverage.binding()),
            &Coverage::new(coverage),
            &scanner,
            &candidates,
        )
        .await
        .expect("accepted assessment stores one candidate");

        assert_eq!(id, ContributionCandidateId(Uuid::nil()));
        let calls = scanner.calls.lock().expect("test mutex");
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], probe.public_safe_bytes());
        assert_eq!(calls[1], b"deidentified candidate");
        assert_eq!(candidates.stored.lock().expect("test mutex").len(), 1);
    }

    #[tokio::test]
    async fn finalize_forwards_only_authenticated_confirmation_binding() {
        let candidates = Candidates::default();
        let digest = payload_sha256(b"candidate");
        let denied = finalize(
            ConfirmContribution {
                authorization: scope(None),
                candidate_id: ContributionCandidateId(Uuid::nil()),
                candidate_payload_sha256: digest,
                confirmation_id: Uuid::nil(),
            },
            &candidates,
        )
        .await
        .expect_err("unbound user must fail");
        assert_eq!(denied, ErrorCode::Unauthorized);
        assert_eq!(*candidates.finalized.lock().expect("test mutex"), 0);

        finalize(
            ConfirmContribution {
                authorization: scope(Some(UserId::new())),
                candidate_id: ContributionCandidateId(Uuid::nil()),
                candidate_payload_sha256: digest,
                confirmation_id: Uuid::nil(),
            },
            &candidates,
        )
        .await
        .expect("repository owns confirmation/current-state comparison");
        assert_eq!(*candidates.finalized.lock().expect("test mutex"), 1);
    }
}
