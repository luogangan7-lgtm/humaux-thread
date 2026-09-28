//! `humaux-private-worker` — Bounded Phase 9 private contribution worker orchestration.
//! Depends-on: crates=[humaux-adapters, humaux-application, humaux-domain, serde_json, sha2, uuid]; services=[]; env=[]; modules=[adapters::contribution_execution_ingress, adapters::contribution_execution_repo, adapters::contribution_reasoner, adapters::contribution_scan, adapters::jobs, adapters::postgres, application::consolidate, application::contribute, application::contribution_execution, domain::error, domain::evidence]
//! Called-by: [crate(humaux-consolidation-worker), tests]
//! Invariants: []
//! Spec: ADR-0012; ADR-0016
//!
//! `pub mod inference_rpc` exists on this lib target (not only inside `src/main.rs`) for the
//! same reason `bins/retrieval-worker/src/lib.rs` exports its own ADR-0012 `rpc` module:
//! `bins/consolidation-worker/tests/consolidation_hop_e2e.rs` spawns the real handler
//! in-process against a temp UDS path rather than reimplementing (and drifting from) it.
//! `pub mod distill` (ADR-0016) is the Evidence → MemoryRecord hop the binary's
//! `--distill-once`/`--distill-serve` modes drive and `tests/distill_hop_e2e.rs` proves.
//!
//! This library deliberately exposes one `run_once` operation rather than a resident loop. The
//! binary's provider/config bootstrap remains deployment-owned; this module owns only the frozen
//! R4 claim, reserve, single-dispatch, scan, completion, and reconciliation state machine.

pub mod distill;
pub mod inference_rpc;

use humaux_adapters::{
    contribution_execution_ingress::{
        ContributionExecutionIngress, ContributionExecutionIngressError,
    },
    contribution_execution_repo::{
        ContributionExecutionRead, ContributionExecutionRepo, ContributionExecutionRepoError,
        ContributionJobLease, EnqueuedContributionExecution,
    },
    contribution_reasoner::{
        ContributionDispatchOutcome, ContributionReasoner, DispatchedContributionCall,
        ParsedContributionAssessment, assessment_prompt_contract, coverage_prompt_contract,
        parse_assessment, parse_coverage_probe,
    },
    contribution_scan::{ContributionScanOutcome, ContributionScanner},
    jobs::{self, ClaimedJob, JobStatus, JobsError},
    postgres::PrivateWorkerDbPool,
};
use humaux_application::{
    consolidate::ContentSha256,
    contribute::{ContributionCoverageProbe, ContributionPreparationInput, PublicCoveragePort},
    contribution_execution::{
        AssessmentCompletion, AssessmentCompletionEvidence, AssessmentCompletionValue,
        CandidateCompletionEvidence, ContributionExecutionId, ContributionExecutionState,
        CoverageCompletion, CoverageCompletionValue, DefiniteProviderFailure, ReservationDecision,
        SuccessfulCoverageReceipt,
    },
};
use humaux_domain::{error::ErrorCode, evidence::payload_sha256};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

const CONTRIBUTION_JOB_TYPE: &str = "CONTRIBUTION_EXECUTE";
const CONTRIBUTION_JOB_SCHEMA_VERSION: u64 = 1;
const DEFINITE_PROVIDER_ERROR_CLASS: &str = "PROVIDER_PERMANENT";

/// Production-core MANUAL contribution command owned by the private-worker process.
///
/// Phase 15 may place an authenticated mTLS transport in front of this command. The command
/// itself accepts only the existing verified ID-free preparation input and a caller idempotency
/// key; it cannot receive rights, policy, source hashes, durable identities, or a fingerprint.
pub struct StartManualContributionCommand<'a> {
    ingress: ContributionExecutionIngress<'a>,
}

impl<'a> StartManualContributionCommand<'a> {
    pub const fn new(pool: &'a PrivateWorkerDbPool) -> Self {
        Self {
            ingress: ContributionExecutionIngress::new(pool),
        }
    }

    pub async fn execute(
        &self,
        request: ContributionPreparationInput,
        idempotency_key: String,
    ) -> Result<EnqueuedContributionExecution, ContributionExecutionIngressError> {
        self.ingress
            .start_manual(
                request,
                idempotency_key,
                coverage_prompt_contract(),
                assessment_prompt_contract(),
            )
            .await
    }
}

/// Deployment-owned lease settings for one bounded claim.
#[derive(Debug, Clone, PartialEq)]
pub struct ContributionExecutionRunnerConfig {
    pub lease_owner: String,
    pub lease_seconds: f64,
}

impl ContributionExecutionRunnerConfig {
    pub fn validate(&self) -> Result<(), ErrorCode> {
        if self.lease_owner.trim().is_empty()
            || !self.lease_seconds.is_finite()
            || self.lease_seconds <= 0.0
        {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(())
    }
}

/// Durable result of one bounded claim attempt. Provider-call counts are intentionally omitted:
/// the non-cloneable dispatch permit and recording-provider tests are the authority for that gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContributionRunOnceReport {
    Idle,
    Completed {
        execution_id: ContributionExecutionId,
        state: ContributionExecutionState,
    },
    LateCompleted {
        execution_id: ContributionExecutionId,
        state: ContributionExecutionState,
    },
    ReconciliationRequired {
        execution_id: ContributionExecutionId,
        state: ContributionExecutionState,
    },
}

#[derive(Debug)]
pub enum ContributionExecutionRunnerError {
    Jobs(JobsError),
    Repository(ContributionExecutionRepoError),
    Domain(ErrorCode),
    InvalidClaim(&'static str),
    Invariant(&'static str),
}

impl std::fmt::Display for ContributionExecutionRunnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Jobs(error) => write!(f, "contribution claim failed: {error}"),
            Self::Repository(error) => write!(f, "contribution repository failed: {error}"),
            Self::Domain(error) => write!(f, "contribution contract failed: {error}"),
            Self::InvalidClaim(message) => write!(f, "invalid contribution claim: {message}"),
            Self::Invariant(message) => write!(f, "contribution invariant failed: {message}"),
        }
    }
}

impl std::error::Error for ContributionExecutionRunnerError {}

impl From<JobsError> for ContributionExecutionRunnerError {
    fn from(value: JobsError) -> Self {
        Self::Jobs(value)
    }
}

impl From<ContributionExecutionRepoError> for ContributionExecutionRunnerError {
    fn from(value: ContributionExecutionRepoError) -> Self {
        Self::Repository(value)
    }
}

impl From<ErrorCode> for ContributionExecutionRunnerError {
    fn from(value: ErrorCode) -> Self {
        Self::Domain(value)
    }
}

/// Concrete R4 orchestrator. It accepts the production reasoner and scanner directly; no
/// worker-local provider abstraction or generic routing layer is introduced.
pub struct ContributionExecutionRunner<'a, 'provider> {
    pool: &'a PrivateWorkerDbPool,
    reasoner: &'a ContributionReasoner<'provider>,
    scanner: &'a ContributionScanner,
    config: ContributionExecutionRunnerConfig,
}

impl<'a, 'provider> ContributionExecutionRunner<'a, 'provider> {
    pub fn new(
        pool: &'a PrivateWorkerDbPool,
        reasoner: &'a ContributionReasoner<'provider>,
        scanner: &'a ContributionScanner,
        config: ContributionExecutionRunnerConfig,
    ) -> Result<Self, ErrorCode> {
        config.validate()?;
        Ok(Self {
            pool,
            reasoner,
            scanner,
            config,
        })
    }

    /// Claims at most one exact contribution job and runs only the state already persisted by
    /// migration 0131. `private_claim` is the sole claim surface used by this worker.
    pub async fn run_once(
        &self,
        tenant_id: Uuid,
    ) -> Result<ContributionRunOnceReport, ContributionExecutionRunnerError> {
        if tenant_id.is_nil() {
            return Err(ErrorCode::InvalidInput.into());
        }
        let mut claimed = jobs::private_claim(
            self.pool,
            tenant_id,
            &self.config.lease_owner,
            self.config.lease_seconds,
            1,
        )
        .await?;
        let Some(job) = claimed.pop() else {
            return Ok(ContributionRunOnceReport::Idle);
        };
        if !claimed.is_empty() {
            return Err(ContributionExecutionRunnerError::Invariant(
                "bounded claim returned more than one job",
            ));
        }
        self.run_claimed(tenant_id, job).await
    }

    async fn run_claimed(
        &self,
        tenant_id: Uuid,
        job: ClaimedJob,
    ) -> Result<ContributionRunOnceReport, ContributionExecutionRunnerError> {
        validate_claim(tenant_id, &self.config.lease_owner, &job)?;
        let execution_id = parse_execution_id(&job.payload)?;
        let lease = ContributionJobLease::try_new(
            job.job_id,
            job.lease_owner
                .ok_or(ContributionExecutionRunnerError::InvalidClaim(
                    "processing claim has no lease owner",
                ))?,
            job.attempt,
        )?;
        let repo = ContributionExecutionRepo::new(self.pool);
        let execution = repo.load(tenant_id, execution_id).await?.ok_or(
            ContributionExecutionRunnerError::InvalidClaim("execution does not exist"),
        )?;
        if execution.tenant_id != tenant_id || execution.execution_id != execution_id {
            return Err(ContributionExecutionRunnerError::InvalidClaim(
                "job and execution identity mismatch",
            ));
        }
        self.run_execution(&repo, execution, &lease).await
    }

    async fn run_execution(
        &self,
        repo: &ContributionExecutionRepo<'_>,
        execution: ContributionExecutionRead,
        lease: &ContributionJobLease,
    ) -> Result<ContributionRunOnceReport, ContributionExecutionRunnerError> {
        match execution.state {
            ContributionExecutionState::ReadyA => self.run_a(repo, execution, lease).await,
            ContributionExecutionState::AReserved | ContributionExecutionState::BReserved => {
                self.reconcile(repo, &execution, lease).await
            }
            ContributionExecutionState::ReadyB => self.run_b(repo, execution, lease).await,
            ContributionExecutionState::ReadyCandidate => {
                repo.commit_candidate(execution.tenant_id, execution.execution_id, lease)
                    .await?;
                Ok(ContributionRunOnceReport::Completed {
                    execution_id: execution.execution_id,
                    state: ContributionExecutionState::Done,
                })
            }
            state @ (ContributionExecutionState::NotContributable
            | ContributionExecutionState::RejectedSafety
            | ContributionExecutionState::FailedTerminal) => {
                repo.settle_terminal_job(execution.tenant_id, execution.execution_id, lease)
                    .await?;
                Ok(ContributionRunOnceReport::Completed {
                    execution_id: execution.execution_id,
                    state,
                })
            }
            ContributionExecutionState::Done => Err(ContributionExecutionRunnerError::Invariant(
                "a PROCESSING claim cannot settle an already DONE execution",
            )),
        }
    }

    async fn run_a(
        &self,
        repo: &ContributionExecutionRepo<'_>,
        execution: ContributionExecutionRead,
        lease: &ContributionJobLease,
    ) -> Result<ContributionRunOnceReport, ContributionExecutionRunnerError> {
        let prepared = self.reasoner.prepare_a(&execution).await?;
        let plan = prepared.reserve_plan()?;
        let reservation = repo
            .reserve_a_new(execution.tenant_id, execution.execution_id, lease, &plan)
            .await?;
        let permit = match reservation {
            ReservationDecision::NewlyReserved(permit) => permit,
            ReservationDecision::ExistingReserved(_) => {
                return self.reconciliation_report(&execution);
            }
        };
        let reserved = permit.binding();
        let dispatched = match self.reasoner.dispatch_prepared(prepared, permit).await {
            Ok(outcome) => outcome,
            Err(_) => return self.reconcile(repo, &execution, lease).await,
        };
        let (completion_binding, value) = match dispatched {
            ContributionDispatchOutcome::Response(response) => {
                let binding = response.binding();
                match self.coverage_value(repo, &execution, &response).await {
                    Ok(value) => (binding, value),
                    Err(_) => return self.reconcile(repo, &execution, lease).await,
                }
            }
            ContributionDispatchOutcome::DefiniteTerminal { binding, .. } => {
                let failure = DefiniteProviderFailure::try_new(
                    DEFINITE_PROVIDER_ERROR_CLASS.to_owned(),
                    None,
                )?;
                let completion = CoverageCompletion::try_new(
                    reserved,
                    binding,
                    CoverageCompletionValue::FailedTerminal(failure),
                )
                .map_err(|_| ErrorCode::Conflict)?;
                return self.complete_a(repo, &execution, lease, &completion).await;
            }
            ContributionDispatchOutcome::NonTerminal { .. } => {
                return self.reconcile(repo, &execution, lease).await;
            }
        };
        let completion = CoverageCompletion::try_new(reserved, completion_binding, value)
            .map_err(|_| ErrorCode::Conflict)?;
        self.complete_a(repo, &execution, lease, &completion).await
    }

    async fn coverage_value(
        &self,
        repo: &ContributionExecutionRepo<'_>,
        execution: &ContributionExecutionRead,
        response: &DispatchedContributionCall,
    ) -> Result<CoverageCompletionValue, ContributionExecutionRunnerError> {
        let parsed = parse_coverage_probe(response.output_bytes())?;
        let scan = self.scanner.scan_outcome(parsed.bytes())?;
        let scan_receipt = repo
            .bind_completion_scan_receipt(
                execution.tenant_id,
                ContributionScanner::receipt_json(scan.receipt()),
            )
            .await?;
        let receipt = SuccessfulCoverageReceipt::try_new(
            scan_receipt,
            response.provider_trace().clone(),
            None,
        )?;
        match scan {
            ContributionScanOutcome::Reject(_) => {
                Ok(CoverageCompletionValue::RejectedSafety(receipt))
            }
            ContributionScanOutcome::Pass(_) => {
                let probe = ContributionCoverageProbe::new(
                    execution.input_manifest_hash,
                    parsed.bytes().to_vec(),
                    payload_sha256(parsed.bytes()),
                    response.provider_trace().clone(),
                )?;
                let coverage = self.reasoner.load_public_coverage(&probe).await?;
                Ok(CoverageCompletionValue::Usable {
                    coverage_probe_sha256: probe.output_sha256(),
                    coverage,
                    receipt,
                })
            }
        }
    }

    async fn complete_a(
        &self,
        repo: &ContributionExecutionRepo<'_>,
        execution: &ContributionExecutionRead,
        lease: &ContributionJobLease,
        completion: &CoverageCompletion,
    ) -> Result<ContributionRunOnceReport, ContributionExecutionRunnerError> {
        let (state, late) =
            complete_a_live_or_late(repo, execution.tenant_id, completion, lease).await?;
        if late {
            return Ok(ContributionRunOnceReport::LateCompleted {
                execution_id: execution.execution_id,
                state,
            });
        }
        match state {
            ContributionExecutionState::ReadyB => {
                let next = repo
                    .load(execution.tenant_id, execution.execution_id)
                    .await?
                    .ok_or(ContributionExecutionRunnerError::Invariant(
                        "A completion lost its execution",
                    ))?;
                self.run_b(repo, next, lease).await
            }
            ContributionExecutionState::RejectedSafety
            | ContributionExecutionState::FailedTerminal => {
                Ok(ContributionRunOnceReport::Completed {
                    execution_id: execution.execution_id,
                    state,
                })
            }
            _ => Err(ContributionExecutionRunnerError::Invariant(
                "A completion returned an impossible state",
            )),
        }
    }

    async fn run_b(
        &self,
        repo: &ContributionExecutionRepo<'_>,
        execution: ContributionExecutionRead,
        lease: &ContributionJobLease,
    ) -> Result<ContributionRunOnceReport, ContributionExecutionRunnerError> {
        let prepared = self.reasoner.prepare_b(&execution).await?;
        let plan = prepared.reserve_plan()?;
        let reservation = repo
            .reserve_b_new(execution.tenant_id, execution.execution_id, lease, &plan)
            .await?;
        let permit = match reservation {
            ReservationDecision::NewlyReserved(permit) => permit,
            ReservationDecision::ExistingReserved(_) => {
                return self.reconciliation_report(&execution);
            }
        };
        let reserved = permit.binding();
        let dispatched = match self.reasoner.dispatch_prepared(prepared, permit).await {
            Ok(outcome) => outcome,
            Err(_) => return self.reconcile(repo, &execution, lease).await,
        };
        let (completion_binding, value) = match dispatched {
            ContributionDispatchOutcome::Response(response) => {
                let binding = response.binding();
                match self.assessment_value(repo, &execution, &response).await {
                    Ok(value) => (binding, value),
                    Err(_) => return self.reconcile(repo, &execution, lease).await,
                }
            }
            ContributionDispatchOutcome::DefiniteTerminal { binding, .. } => {
                let failure = DefiniteProviderFailure::try_new(
                    DEFINITE_PROVIDER_ERROR_CLASS.to_owned(),
                    None,
                )?;
                let completion = AssessmentCompletion::try_new(
                    reserved,
                    binding,
                    AssessmentCompletionValue::FailedTerminal(failure),
                )
                .map_err(|_| ErrorCode::Conflict)?;
                return self.complete_b(repo, &execution, lease, &completion).await;
            }
            ContributionDispatchOutcome::NonTerminal { .. } => {
                return self.reconcile(repo, &execution, lease).await;
            }
        };
        let completion = AssessmentCompletion::try_new(reserved, completion_binding, value)
            .map_err(|_| ErrorCode::Conflict)?;
        self.complete_b(repo, &execution, lease, &completion).await
    }

    async fn assessment_value(
        &self,
        repo: &ContributionExecutionRepo<'_>,
        execution: &ContributionExecutionRead,
        response: &DispatchedContributionCall,
    ) -> Result<AssessmentCompletionValue, ContributionExecutionRunnerError> {
        let parsed = parse_assessment(response.output_bytes())?;
        let gates = parsed.gates();
        let canonical = parsed.canonical().to_vec();
        let assessment = AssessmentCompletionEvidence::try_new(
            canonical.clone(),
            ContentSha256(Sha256::digest(&canonical).into()),
            gates[0],
            gates[1],
            gates[2],
            gates[3],
            response.provider_trace().clone(),
            None,
        )?;
        match parsed {
            ParsedContributionAssessment::NotContributable { .. } => Ok(
                AssessmentCompletionValue::try_not_contributable(assessment)?,
            ),
            ParsedContributionAssessment::Candidate { candidate, .. } => {
                let scan = self.scanner.scan_outcome(&candidate)?;
                let scan_receipt = repo
                    .bind_completion_scan_receipt(
                        execution.tenant_id,
                        ContributionScanner::receipt_json(scan.receipt()),
                    )
                    .await?;
                let candidate = CandidateCompletionEvidence::try_new(
                    candidate.clone(),
                    payload_sha256(&candidate),
                    scan_receipt,
                )?;
                match scan {
                    ContributionScanOutcome::Pass(_) => Ok(
                        AssessmentCompletionValue::try_ready_candidate(assessment, candidate)?,
                    ),
                    ContributionScanOutcome::Reject(_) => {
                        Ok(AssessmentCompletionValue::RejectedSafety {
                            assessment,
                            candidate,
                        })
                    }
                }
            }
        }
    }

    async fn complete_b(
        &self,
        repo: &ContributionExecutionRepo<'_>,
        execution: &ContributionExecutionRead,
        lease: &ContributionJobLease,
        completion: &AssessmentCompletion,
    ) -> Result<ContributionRunOnceReport, ContributionExecutionRunnerError> {
        let (state, late) =
            complete_b_live_or_late(repo, execution.tenant_id, completion, lease).await?;
        if late {
            return Ok(ContributionRunOnceReport::LateCompleted {
                execution_id: execution.execution_id,
                state,
            });
        }
        match state {
            ContributionExecutionState::ReadyCandidate => {
                repo.commit_candidate(execution.tenant_id, execution.execution_id, lease)
                    .await?;
                Ok(ContributionRunOnceReport::Completed {
                    execution_id: execution.execution_id,
                    state: ContributionExecutionState::Done,
                })
            }
            ContributionExecutionState::NotContributable
            | ContributionExecutionState::RejectedSafety
            | ContributionExecutionState::FailedTerminal => {
                Ok(ContributionRunOnceReport::Completed {
                    execution_id: execution.execution_id,
                    state,
                })
            }
            _ => Err(ContributionExecutionRunnerError::Invariant(
                "B completion returned an impossible state",
            )),
        }
    }

    async fn reconcile(
        &self,
        repo: &ContributionExecutionRepo<'_>,
        execution: &ContributionExecutionRead,
        lease: &ContributionJobLease,
    ) -> Result<ContributionRunOnceReport, ContributionExecutionRunnerError> {
        if !repo
            .mark_reconciliation_required(execution.tenant_id, execution.execution_id, lease)
            .await?
        {
            return Err(ContributionExecutionRunnerError::Invariant(
                "reserved execution was not marked for reconciliation",
            ));
        }
        self.reconciliation_report(execution)
    }

    fn reconciliation_report(
        &self,
        execution: &ContributionExecutionRead,
    ) -> Result<ContributionRunOnceReport, ContributionExecutionRunnerError> {
        if !matches!(
            execution.state,
            ContributionExecutionState::AReserved | ContributionExecutionState::BReserved
        ) {
            // The in-memory read preceded reservation; only those two predecessor states are
            // legal here. Report the durable stage reached by that reservation explicitly.
            let state = match execution.state {
                ContributionExecutionState::ReadyA => ContributionExecutionState::AReserved,
                ContributionExecutionState::ReadyB => ContributionExecutionState::BReserved,
                _ => {
                    return Err(ContributionExecutionRunnerError::Invariant(
                        "reconciliation requested outside a reservable state",
                    ));
                }
            };
            return Ok(ContributionRunOnceReport::ReconciliationRequired {
                execution_id: execution.execution_id,
                state,
            });
        }
        Ok(ContributionRunOnceReport::ReconciliationRequired {
            execution_id: execution.execution_id,
            state: execution.state,
        })
    }
}

async fn complete_a_live_or_late(
    repo: &ContributionExecutionRepo<'_>,
    tenant_id: Uuid,
    completion: &CoverageCompletion,
    lease: &ContributionJobLease,
) -> Result<(ContributionExecutionState, bool), ContributionExecutionRepoError> {
    match repo
        .complete_a_exact(tenant_id, completion, Some(lease))
        .await
    {
        Ok(state) => Ok((state, false)),
        Err(error) if error.is_fresh_lease_required() => repo
            .complete_a_exact(tenant_id, completion, None)
            .await
            .map(|state| (state, true)),
        Err(error) => Err(error),
    }
}

async fn complete_b_live_or_late(
    repo: &ContributionExecutionRepo<'_>,
    tenant_id: Uuid,
    completion: &AssessmentCompletion,
    lease: &ContributionJobLease,
) -> Result<(ContributionExecutionState, bool), ContributionExecutionRepoError> {
    match repo
        .complete_b_exact(tenant_id, completion, Some(lease))
        .await
    {
        Ok(state) => Ok((state, false)),
        Err(error) if error.is_fresh_lease_required() => repo
            .complete_b_exact(tenant_id, completion, None)
            .await
            .map(|state| (state, true)),
        Err(error) => Err(error),
    }
}

fn validate_claim(
    tenant_id: Uuid,
    lease_owner: &str,
    job: &ClaimedJob,
) -> Result<(), ContributionExecutionRunnerError> {
    if job.tenant_id != tenant_id
        || job.job_type != CONTRIBUTION_JOB_TYPE
        || job.status != JobStatus::Processing
        || job.attempt <= 0
        || job.lease_owner.as_deref() != Some(lease_owner)
        || job.lease_expires_at.is_none()
        || job.stream_key.is_some()
        || job.stream_seq.is_some()
    {
        return Err(ContributionExecutionRunnerError::InvalidClaim(
            "claim fields do not match the private contribution contract",
        ));
    }
    Ok(())
}

fn parse_execution_id(
    payload: &Value,
) -> Result<ContributionExecutionId, ContributionExecutionRunnerError> {
    let object = payload
        .as_object()
        .filter(|object| object.len() == 2)
        .ok_or(ContributionExecutionRunnerError::InvalidClaim(
            "payload is not the exact contribution schema",
        ))?;
    if object.get("schema_version").and_then(Value::as_u64) != Some(CONTRIBUTION_JOB_SCHEMA_VERSION)
    {
        return Err(ContributionExecutionRunnerError::InvalidClaim(
            "payload schema version is invalid",
        ));
    }
    let raw = object.get("execution_id").and_then(Value::as_str).ok_or(
        ContributionExecutionRunnerError::InvalidClaim("payload execution id is invalid"),
    )?;
    let id = Uuid::parse_str(raw).map_err(|_| {
        ContributionExecutionRunnerError::InvalidClaim("payload execution id is invalid")
    })?;
    ContributionExecutionId::try_from_uuid(id).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_and_payload_are_closed() {
        assert!(
            ContributionExecutionRunnerConfig {
                lease_owner: "worker-a".into(),
                lease_seconds: 30.0,
            }
            .validate()
            .is_ok()
        );
        assert!(
            ContributionExecutionRunnerConfig {
                lease_owner: " ".into(),
                lease_seconds: 30.0,
            }
            .validate()
            .is_err()
        );

        let id = Uuid::from_u128(1);
        let valid = serde_json::json!({"schema_version": 1, "execution_id": id.to_string()});
        assert_eq!(
            parse_execution_id(&valid).expect("exact payload").as_uuid(),
            id
        );
        for invalid in [
            serde_json::json!({"schema_version": 1, "execution_id": id.to_string(), "extra": true}),
            serde_json::json!({"schema_version": "1", "execution_id": id.to_string()}),
            serde_json::json!({"schema_version": 1, "execution_id": Uuid::nil().to_string()}),
        ] {
            assert!(parse_execution_id(&invalid).is_err());
        }
    }
}
