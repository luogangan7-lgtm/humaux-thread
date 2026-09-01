//! Phase 9 R4 application contract for one durable two-stage contribution execution.
//!
//! This module is deliberately pure. PostgreSQL owns the state machine and coupled writes; the
//! application layer carries caller-supplied identities and exact completion evidence without
//! minting ids, selecting routes, or dispatching a provider.

use humaux_domain::{error::ErrorCode, evidence::EvidencePayloadSha256};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    consolidate::{ContentSha256, LogicalReasoningCallId, ProviderTraceRef, ReasoningIntentSha256},
    contribute::{
        ContributionGate, PreparationSnapshot, PreparedAssessedContribution, PublicCoverageDigest,
    },
};

/// Stable workflow identity. It can only wrap a non-nil caller/repository supplied UUID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContributionExecutionId(Uuid);

impl ContributionExecutionId {
    pub fn try_from_uuid(value: Uuid) -> Result<Self, ErrorCode> {
        if value.is_nil() {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self(value))
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// The exact closed state set owned by migration 0131.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContributionExecutionState {
    ReadyA,
    AReserved,
    ReadyB,
    BReserved,
    ReadyCandidate,
    Done,
    NotContributable,
    RejectedSafety,
    FailedTerminal,
}

impl ContributionExecutionState {
    pub const fn as_db_str(self) -> &'static str {
        match self {
            Self::ReadyA => "READY_A",
            Self::AReserved => "A_RESERVED",
            Self::ReadyB => "READY_B",
            Self::BReserved => "B_RESERVED",
            Self::ReadyCandidate => "READY_CANDIDATE",
            Self::Done => "DONE",
            Self::NotContributable => "NOT_CONTRIBUTABLE",
            Self::RejectedSafety => "REJECTED_SAFETY",
            Self::FailedTerminal => "FAILED_TERMINAL",
        }
    }

    pub fn try_from_db(value: &str) -> Result<Self, ErrorCode> {
        match value {
            "READY_A" => Ok(Self::ReadyA),
            "A_RESERVED" => Ok(Self::AReserved),
            "READY_B" => Ok(Self::ReadyB),
            "B_RESERVED" => Ok(Self::BReserved),
            "READY_CANDIDATE" => Ok(Self::ReadyCandidate),
            "DONE" => Ok(Self::Done),
            "NOT_CONTRIBUTABLE" => Ok(Self::NotContributable),
            "REJECTED_SAFETY" => Ok(Self::RejectedSafety),
            "FAILED_TERMINAL" => Ok(Self::FailedTerminal),
            _ => Err(ErrorCode::InvalidInput),
        }
    }
}

/// The two immutable execution stages. There is no generic/stringly call kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContributionExecutionStage {
    Coverage,
    Assessment,
}

/// Closed completion outcomes accepted by the 0131 A command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoverageCompletionOutcome {
    Usable,
    RejectedSafety,
    FailedTerminal,
}

impl CoverageCompletionOutcome {
    pub const fn as_db_str(self) -> &'static str {
        match self {
            Self::Usable => "USABLE",
            Self::RejectedSafety => "REJECTED_SAFETY",
            Self::FailedTerminal => "FAILED_TERMINAL",
        }
    }
}

/// Closed completion outcomes accepted by the 0131 B command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssessmentCompletionOutcome {
    ReadyCandidate,
    NotContributable,
    RejectedSafety,
    FailedTerminal,
}

impl AssessmentCompletionOutcome {
    pub const fn as_db_str(self) -> &'static str {
        match self {
            Self::ReadyCandidate => "READY_CANDIDATE",
            Self::NotContributable => "NOT_CONTRIBUTABLE",
            Self::RejectedSafety => "REJECTED_SAFETY",
            Self::FailedTerminal => "FAILED_TERMINAL",
        }
    }
}

/// Caller-supplied logical A/B identities. Neither this type nor its users mint ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContributionLogicalCallIds {
    coverage: LogicalReasoningCallId,
    assessment: LogicalReasoningCallId,
}

impl ContributionLogicalCallIds {
    pub fn try_new(
        coverage: LogicalReasoningCallId,
        assessment: LogicalReasoningCallId,
    ) -> Result<Self, ErrorCode> {
        if coverage.0.is_nil() || assessment.0.is_nil() || coverage == assessment {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            coverage,
            assessment,
        })
    }

    pub const fn coverage(self) -> LogicalReasoningCallId {
        self.coverage
    }

    pub const fn assessment(self) -> LogicalReasoningCallId {
        self.assessment
    }
}

/// Versioned immutable prompt contract persisted at enqueue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContributionPromptContract {
    version: i64,
    sha256: ContentSha256,
}

impl ContributionPromptContract {
    pub fn try_new(version: i64, sha256: ContentSha256) -> Result<Self, ErrorCode> {
        if version <= 0 {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self { version, sha256 })
    }

    pub const fn version(self) -> i64 {
        self.version
    }

    pub const fn sha256(self) -> ContentSha256 {
        self.sha256
    }
}

/// Immutable application value consumed by the authoritative enqueue SQL command.
pub struct ContributionExecutionEnqueueInput {
    idempotency_key: String,
    coverage_contract: ContributionPromptContract,
    assessment_contract: ContributionPromptContract,
    prepared: PreparedAssessedContribution,
}

impl ContributionExecutionEnqueueInput {
    pub fn try_new(
        idempotency_key: String,
        coverage_contract: ContributionPromptContract,
        assessment_contract: ContributionPromptContract,
        prepared: PreparedAssessedContribution,
    ) -> Result<Self, ErrorCode> {
        let preparation = prepared.preparation();
        if idempotency_key.trim().is_empty()
            || preparation.authorization.user_id().is_none()
            || preparation.sources.is_empty()
            || preparation.policy_version <= 0
            || preparation.reasoning.binding_version.0 <= 0
            || preparation.reasoning.input_manifest_hash != preparation.source_manifest_hash
            || preparation.reasoning.contribution_attempt.is_some()
        {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            idempotency_key,
            coverage_contract,
            assessment_contract,
            prepared,
        })
    }

    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    pub const fn coverage_contract(&self) -> ContributionPromptContract {
        self.coverage_contract
    }

    pub const fn assessment_contract(&self) -> ContributionPromptContract {
        self.assessment_contract
    }

    pub const fn preparation(&self) -> &PreparationSnapshot {
        self.prepared.preparation()
    }

    pub const fn prepared(&self) -> &PreparedAssessedContribution {
        &self.prepared
    }
}

/// The six immutable axes which authorize one exact late completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExactContributionCallBinding {
    execution_id: ContributionExecutionId,
    stage: ContributionExecutionStage,
    model_call_id: Uuid,
    request_id: LogicalReasoningCallId,
    intent_sha256: ReasoningIntentSha256,
    disclosure_id: Uuid,
}

impl ExactContributionCallBinding {
    pub fn try_new(
        execution_id: ContributionExecutionId,
        stage: ContributionExecutionStage,
        model_call_id: Uuid,
        request_id: LogicalReasoningCallId,
        intent_sha256: ReasoningIntentSha256,
        disclosure_id: Uuid,
    ) -> Result<Self, ErrorCode> {
        if model_call_id.is_nil() || request_id.0.is_nil() || disclosure_id.is_nil() {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            execution_id,
            stage,
            model_call_id,
            request_id,
            intent_sha256,
            disclosure_id,
        })
    }

    pub const fn execution_id(self) -> ContributionExecutionId {
        self.execution_id
    }

    pub const fn stage(self) -> ContributionExecutionStage {
        self.stage
    }

    pub const fn model_call_id(self) -> Uuid {
        self.model_call_id
    }

    pub const fn request_id(self) -> LogicalReasoningCallId {
        self.request_id
    }

    pub const fn intent_sha256(self) -> ReasoningIntentSha256 {
        self.intent_sha256
    }

    pub const fn disclosure_id(self) -> Uuid {
        self.disclosure_id
    }

    /// Fails closed on the first independently named mismatch axis.
    pub fn validate_completion(self, completion: Self) -> Result<(), ExactCompletionMismatch> {
        if self.execution_id != completion.execution_id {
            return Err(ExactCompletionMismatch::Execution);
        }
        if self.stage != completion.stage {
            return Err(ExactCompletionMismatch::Stage);
        }
        if self.model_call_id != completion.model_call_id {
            return Err(ExactCompletionMismatch::ModelCall);
        }
        if self.request_id != completion.request_id {
            return Err(ExactCompletionMismatch::Request);
        }
        if self.intent_sha256 != completion.intent_sha256 {
            return Err(ExactCompletionMismatch::Intent);
        }
        if self.disclosure_id != completion.disclosure_id {
            return Err(ExactCompletionMismatch::Disclosure);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExactCompletionMismatch {
    Execution,
    Stage,
    ModelCall,
    Request,
    Intent,
    Disclosure,
}

/// Trusted repository classification of a reservation lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistedReservationStatus {
    NewlyReserved,
    ExistingReserved,
}

/// Non-cloneable authority to perform the one provider dispatch created by a new reservation.
#[derive(Debug, PartialEq, Eq)]
pub struct ProviderDispatchPermit {
    reservation: ReservedContributionCall,
}

impl ProviderDispatchPermit {
    pub const fn binding(&self) -> ExactContributionCallBinding {
        self.reservation.binding
    }

    pub const fn reservation(&self) -> &ReservedContributionCall {
        &self.reservation
    }
}

/// Exact nine-column reserve result with the boolean translated into [`ReservationDecision`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservedContributionCall {
    binding: ExactContributionCallBinding,
    processor_id: Option<String>,
    provider_model_id: Option<String>,
    model_revision: Option<String>,
    egress_processor_id: Option<Uuid>,
    credential_ref: Option<Uuid>,
}

impl ReservedContributionCall {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        binding: ExactContributionCallBinding,
        processor_id: Option<String>,
        provider_model_id: Option<String>,
        model_revision: Option<String>,
        egress_processor_id: Option<Uuid>,
        credential_ref: Option<Uuid>,
    ) -> Self {
        Self {
            binding,
            processor_id,
            provider_model_id,
            model_revision,
            egress_processor_id,
            credential_ref,
        }
    }

    pub const fn binding(&self) -> ExactContributionCallBinding {
        self.binding
    }

    pub fn processor_id(&self) -> Option<&str> {
        self.processor_id.as_deref()
    }

    pub fn provider_model_id(&self) -> Option<&str> {
        self.provider_model_id.as_deref()
    }

    pub fn model_revision(&self) -> Option<&str> {
        self.model_revision.as_deref()
    }

    pub const fn egress_processor_id(&self) -> Option<Uuid> {
        self.egress_processor_id
    }

    pub const fn credential_ref(&self) -> Option<Uuid> {
        self.credential_ref
    }
}

/// Closed reservation result. An existing observation carries no dispatch authority.
#[derive(Debug, PartialEq, Eq)]
pub enum ReservationDecision {
    NewlyReserved(ProviderDispatchPermit),
    ExistingReserved(ReservedContributionCall),
}

impl ReservationDecision {
    /// Trusted conversion seam for the repository's closed persisted status.
    pub fn from_persisted(
        reservation: ReservedContributionCall,
        status: PersistedReservationStatus,
    ) -> Self {
        match status {
            PersistedReservationStatus::NewlyReserved => {
                Self::NewlyReserved(ProviderDispatchPermit { reservation })
            }
            PersistedReservationStatus::ExistingReserved => Self::ExistingReserved(reservation),
        }
    }

    pub fn dispatch_permit(self) -> Option<ProviderDispatchPermit> {
        match self {
            Self::NewlyReserved(permit) => Some(permit),
            Self::ExistingReserved(_) => None,
        }
    }

    pub const fn binding(&self) -> ExactContributionCallBinding {
        match self {
            Self::NewlyReserved(permit) => permit.reservation.binding,
            Self::ExistingReserved(reservation) => reservation.binding,
        }
    }
}

/// Exact scanner value carried to a coupled completion command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionScanReceipt {
    scan_receipt: serde_json::Value,
    scan_receipt_sha256: ContentSha256,
}

impl CompletionScanReceipt {
    pub fn try_new(
        scan_receipt: serde_json::Value,
        scan_receipt_sha256: ContentSha256,
    ) -> Result<Self, ErrorCode> {
        if !scan_receipt.is_object() {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            scan_receipt,
            scan_receipt_sha256,
        })
    }

    pub const fn value(&self) -> &serde_json::Value {
        &self.scan_receipt
    }

    pub const fn sha256(&self) -> ContentSha256 {
        self.scan_receipt_sha256
    }
}

/// Successful A provider/scan receipt; B carries provider and candidate scan facts separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuccessfulCoverageReceipt {
    scan_receipt: CompletionScanReceipt,
    provider_trace: ProviderTraceRef,
    provider_request_id: Option<String>,
}

impl SuccessfulCoverageReceipt {
    pub fn try_new(
        scan_receipt: CompletionScanReceipt,
        provider_trace: ProviderTraceRef,
        provider_request_id: Option<String>,
    ) -> Result<Self, ErrorCode> {
        if provider_trace.0.trim().is_empty()
            || provider_request_id.as_deref().is_some_and(str::is_empty)
        {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            scan_receipt,
            provider_trace,
            provider_request_id,
        })
    }

    pub const fn scan_receipt(&self) -> &CompletionScanReceipt {
        &self.scan_receipt
    }

    pub const fn provider_trace(&self) -> &ProviderTraceRef {
        &self.provider_trace
    }

    pub fn provider_request_id(&self) -> Option<&str> {
        self.provider_request_id.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefiniteProviderFailure {
    error_class: String,
    provider_request_id: Option<String>,
}

impl DefiniteProviderFailure {
    pub fn try_new(
        error_class: String,
        provider_request_id: Option<String>,
    ) -> Result<Self, ErrorCode> {
        if error_class.trim().is_empty()
            || provider_request_id.as_deref().is_some_and(str::is_empty)
        {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            error_class,
            provider_request_id,
        })
    }

    pub fn error_class(&self) -> &str {
        &self.error_class
    }

    pub fn provider_request_id(&self) -> Option<&str> {
        self.provider_request_id.as_deref()
    }
}

/// Exact A completion payload; its variant fixes the SQL outcome string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoverageCompletionValue {
    Usable {
        coverage_probe_sha256: EvidencePayloadSha256,
        coverage: PublicCoverageDigest,
        receipt: SuccessfulCoverageReceipt,
    },
    RejectedSafety(SuccessfulCoverageReceipt),
    FailedTerminal(DefiniteProviderFailure),
}

impl CoverageCompletionValue {
    pub const fn outcome(&self) -> CoverageCompletionOutcome {
        match self {
            Self::Usable { .. } => CoverageCompletionOutcome::Usable,
            Self::RejectedSafety(_) => CoverageCompletionOutcome::RejectedSafety,
            Self::FailedTerminal(_) => CoverageCompletionOutcome::FailedTerminal,
        }
    }
}

/// Immutable exact A completion command value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageCompletion {
    binding: ExactContributionCallBinding,
    value: CoverageCompletionValue,
}

impl CoverageCompletion {
    pub fn try_new(
        reserved: ExactContributionCallBinding,
        completion: ExactContributionCallBinding,
        value: CoverageCompletionValue,
    ) -> Result<Self, ExactCompletionMismatch> {
        reserved.validate_completion(completion)?;
        if completion.stage != ContributionExecutionStage::Coverage {
            return Err(ExactCompletionMismatch::Stage);
        }
        Ok(Self {
            binding: completion,
            value,
        })
    }

    pub const fn binding(&self) -> ExactContributionCallBinding {
        self.binding
    }

    pub const fn value(&self) -> &CoverageCompletionValue {
        &self.value
    }
}

/// Exact non-terminal B response, including its canonical bytes and raw four-gate result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssessmentCompletionEvidence {
    output_canonical: Vec<u8>,
    output_sha256: ContentSha256,
    novelty: ContributionGate,
    quality: ContributionGate,
    generality: ContributionGate,
    grounding: ContributionGate,
    provider_trace: ProviderTraceRef,
    provider_request_id: Option<String>,
}

impl AssessmentCompletionEvidence {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        output_canonical: Vec<u8>,
        output_sha256: ContentSha256,
        novelty: ContributionGate,
        quality: ContributionGate,
        generality: ContributionGate,
        grounding: ContributionGate,
        provider_trace: ProviderTraceRef,
        provider_request_id: Option<String>,
    ) -> Result<Self, ErrorCode> {
        if output_canonical.is_empty()
            || ContentSha256(Sha256::digest(&output_canonical).into()) != output_sha256
            || provider_trace.0.trim().is_empty()
            || provider_request_id.as_deref().is_some_and(str::is_empty)
        {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            output_canonical,
            output_sha256,
            novelty,
            quality,
            generality,
            grounding,
            provider_trace,
            provider_request_id,
        })
    }

    pub fn output_canonical(&self) -> &[u8] {
        &self.output_canonical
    }

    pub const fn output_sha256(&self) -> ContentSha256 {
        self.output_sha256
    }

    pub const fn gates(&self) -> [ContributionGate; 4] {
        [self.novelty, self.quality, self.generality, self.grounding]
    }

    pub const fn provider_trace(&self) -> &ProviderTraceRef {
        &self.provider_trace
    }

    pub fn provider_request_id(&self) -> Option<&str> {
        self.provider_request_id.as_deref()
    }
}

/// Candidate/scan values present only for READY_CANDIDATE or REJECTED_SAFETY.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateCompletionEvidence {
    body: Vec<u8>,
    sha256: EvidencePayloadSha256,
    scan_receipt: CompletionScanReceipt,
}

impl CandidateCompletionEvidence {
    pub fn try_new(
        body: Vec<u8>,
        sha256: EvidencePayloadSha256,
        scan_receipt: CompletionScanReceipt,
    ) -> Result<Self, ErrorCode> {
        if body.is_empty() || humaux_domain::evidence::payload_sha256(&body) != sha256 {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            body,
            sha256,
            scan_receipt,
        })
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub const fn sha256(&self) -> EvidencePayloadSha256 {
        self.sha256
    }

    pub const fn scan_receipt(&self) -> &CompletionScanReceipt {
        &self.scan_receipt
    }
}

/// Exact B completion payload; variants prevent impossible outcome/receipt combinations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssessmentCompletionValue {
    ReadyCandidate {
        assessment: AssessmentCompletionEvidence,
        candidate: CandidateCompletionEvidence,
    },
    NotContributable(AssessmentCompletionEvidence),
    RejectedSafety {
        assessment: AssessmentCompletionEvidence,
        candidate: CandidateCompletionEvidence,
    },
    FailedTerminal(DefiniteProviderFailure),
}

impl AssessmentCompletionValue {
    pub fn try_ready_candidate(
        assessment: AssessmentCompletionEvidence,
        candidate: CandidateCompletionEvidence,
    ) -> Result<Self, ErrorCode> {
        if assessment
            .gates()
            .iter()
            .any(|gate| *gate != ContributionGate::Pass)
        {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self::ReadyCandidate {
            assessment,
            candidate,
        })
    }

    pub fn try_not_contributable(
        assessment: AssessmentCompletionEvidence,
    ) -> Result<Self, ErrorCode> {
        if assessment
            .gates()
            .iter()
            .all(|gate| *gate == ContributionGate::Pass)
        {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self::NotContributable(assessment))
    }

    pub const fn outcome(&self) -> AssessmentCompletionOutcome {
        match self {
            Self::ReadyCandidate { .. } => AssessmentCompletionOutcome::ReadyCandidate,
            Self::NotContributable(_) => AssessmentCompletionOutcome::NotContributable,
            Self::RejectedSafety { .. } => AssessmentCompletionOutcome::RejectedSafety,
            Self::FailedTerminal(_) => AssessmentCompletionOutcome::FailedTerminal,
        }
    }
}

/// Immutable exact B completion command value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssessmentCompletion {
    binding: ExactContributionCallBinding,
    value: AssessmentCompletionValue,
}

impl AssessmentCompletion {
    pub fn try_new(
        reserved: ExactContributionCallBinding,
        completion: ExactContributionCallBinding,
        value: AssessmentCompletionValue,
    ) -> Result<Self, ExactCompletionMismatch> {
        reserved.validate_completion(completion)?;
        if completion.stage != ContributionExecutionStage::Assessment {
            return Err(ExactCompletionMismatch::Stage);
        }
        Ok(Self {
            binding: completion,
            value,
        })
    }

    pub const fn binding(&self) -> ExactContributionCallBinding {
        self.binding
    }

    pub const fn value(&self) -> &AssessmentCompletionValue {
        &self.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(byte: u8) -> Uuid {
        Uuid::from_bytes([byte; 16])
    }

    fn binding() -> ExactContributionCallBinding {
        ExactContributionCallBinding::try_new(
            ContributionExecutionId::try_from_uuid(id(1)).expect("execution"),
            ContributionExecutionStage::Coverage,
            id(2),
            LogicalReasoningCallId(id(3)),
            ReasoningIntentSha256([4; 32]),
            id(5),
        )
        .expect("binding")
    }

    fn reservation() -> ReservedContributionCall {
        ReservedContributionCall::new(binding(), None, None, None, None, None)
    }

    #[test]
    fn contribution_execution_requires_distinct_non_nil_logical_ids() {
        assert!(
            ContributionLogicalCallIds::try_new(
                LogicalReasoningCallId(Uuid::nil()),
                LogicalReasoningCallId(id(1)),
            )
            .is_err()
        );
        assert!(
            ContributionLogicalCallIds::try_new(
                LogicalReasoningCallId(id(1)),
                LogicalReasoningCallId(Uuid::nil()),
            )
            .is_err()
        );
        assert!(
            ContributionLogicalCallIds::try_new(
                LogicalReasoningCallId(id(1)),
                LogicalReasoningCallId(id(1)),
            )
            .is_err()
        );
        assert!(
            ContributionLogicalCallIds::try_new(
                LogicalReasoningCallId(id(1)),
                LogicalReasoningCallId(id(2)),
            )
            .is_ok()
        );
    }

    #[test]
    fn contribution_execution_permit_exists_only_for_new_reservation() {
        assert!(
            ReservationDecision::from_persisted(
                reservation(),
                PersistedReservationStatus::NewlyReserved
            )
            .dispatch_permit()
            .is_some()
        );
        assert!(
            ReservationDecision::from_persisted(
                reservation(),
                PersistedReservationStatus::ExistingReserved
            )
            .dispatch_permit()
            .is_none()
        );
    }

    #[test]
    fn contribution_execution_rejects_each_exact_completion_mismatch_axis() {
        let expected = binding();
        let cases = [
            (
                ExactContributionCallBinding::try_new(
                    ContributionExecutionId::try_from_uuid(id(9)).expect("execution"),
                    expected.stage,
                    expected.model_call_id,
                    expected.request_id,
                    expected.intent_sha256,
                    expected.disclosure_id,
                )
                .expect("binding"),
                ExactCompletionMismatch::Execution,
            ),
            (
                ExactContributionCallBinding::try_new(
                    expected.execution_id,
                    ContributionExecutionStage::Assessment,
                    expected.model_call_id,
                    expected.request_id,
                    expected.intent_sha256,
                    expected.disclosure_id,
                )
                .expect("binding"),
                ExactCompletionMismatch::Stage,
            ),
            (
                ExactContributionCallBinding::try_new(
                    expected.execution_id,
                    expected.stage,
                    id(9),
                    expected.request_id,
                    expected.intent_sha256,
                    expected.disclosure_id,
                )
                .expect("binding"),
                ExactCompletionMismatch::ModelCall,
            ),
            (
                ExactContributionCallBinding::try_new(
                    expected.execution_id,
                    expected.stage,
                    expected.model_call_id,
                    LogicalReasoningCallId(id(9)),
                    expected.intent_sha256,
                    expected.disclosure_id,
                )
                .expect("binding"),
                ExactCompletionMismatch::Request,
            ),
            (
                ExactContributionCallBinding::try_new(
                    expected.execution_id,
                    expected.stage,
                    expected.model_call_id,
                    expected.request_id,
                    ReasoningIntentSha256([9; 32]),
                    expected.disclosure_id,
                )
                .expect("binding"),
                ExactCompletionMismatch::Intent,
            ),
            (
                ExactContributionCallBinding::try_new(
                    expected.execution_id,
                    expected.stage,
                    expected.model_call_id,
                    expected.request_id,
                    expected.intent_sha256,
                    id(9),
                )
                .expect("binding"),
                ExactCompletionMismatch::Disclosure,
            ),
        ];

        for (actual, mismatch) in cases {
            assert_eq!(expected.validate_completion(actual), Err(mismatch));
        }
        assert_eq!(expected.validate_completion(expected), Ok(()));
    }
}
