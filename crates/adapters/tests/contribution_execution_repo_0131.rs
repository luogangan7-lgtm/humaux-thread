//! Provider-free PostgreSQL 18 coverage for migration 0131's eight typed repository commands.
//!
//! Admin SQL is limited to fixture setup, lease/fault simulation, and durable observations.
//! Every coupled contribution mutation goes through `ContributionExecutionRepo`.

use std::time::Duration;

use async_trait::async_trait;
#[allow(deprecated)] // Shared fixture still compiles one explicitly legacy compatibility helper.
#[path = "support/contribution_fixture.rs"]
mod contribution_fixture;

use contribution_fixture::ContributionFixture;
use humaux_adapters::{
    byok::{
        PrivateInferenceContext, ReasoningCapability, ReasoningProviderDescriptor,
        ReasoningProviderError, StructuredReasoningRequest, StructuredReasoningResponse,
        UserReasoningProvider, VisionReasoningRequest, VisionReasoningResponse,
    },
    contribution_entry_repo::ContributionEntryRepo,
    contribution_execution_repo::{
        ContributionExecutionRepo, ContributionExecutionRepoError, ContributionJobLease,
        ContributionReservePlan,
    },
    contribution_reasoner::{
        ContributionReasoner, ContributionReasonerConfig, assessment_prompt_contract,
        coverage_prompt_contract,
    },
    disclosure::DeletionCapability,
};
use humaux_application::{
    consolidate::{ContentSha256, ProviderTraceRef, ReasoningIntentSha256},
    contribute::{ContributionGate, PublicCoverageDigest, prepare_assessed_input},
    contribution_execution::{
        AssessmentCompletion, AssessmentCompletionEvidence, AssessmentCompletionValue,
        CandidateCompletionEvidence, CompletionScanReceipt, ContributionExecutionEnqueueInput,
        ContributionExecutionState, ContributionPromptContract, CoverageCompletion,
        CoverageCompletionValue, ExactContributionCallBinding, SuccessfulCoverageReceipt,
    },
};
use humaux_domain::{
    egress::ProcessorId,
    evidence::payload_sha256,
    identity::{AuthorizationScope, PrincipalId},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

fn require_db() -> bool {
    match std::env::var("HUMAUX_TEST_PG_DSN") {
        Ok(_) => true,
        Err(_) if std::env::var("HUMAUX_REQUIRE_DB").as_deref() == Ok("1") => {
            panic!("HUMAUX_REQUIRE_DB=1 requires HUMAUX_TEST_PG_DSN")
        }
        Err(_) => false,
    }
}

fn direct_request(
    fixture: &ContributionFixture,
) -> humaux_application::contribute::PrepareContribution {
    let mut request = fixture.request();
    let user = fixture.auth.user_id().expect("fixture user");
    request.authorization = AuthorizationScope::new(
        fixture.auth.tenant_id(),
        PrincipalId(user.0),
        Some(user),
        fixture.auth.allowed_workspace_ids().clone(),
    );
    request
}

fn input(fixture: &ContributionFixture, label: &str) -> ContributionExecutionEnqueueInput {
    let prepared = fixture
        .rt
        .block_on(prepare_assessed_input(
            (&direct_request(fixture)).into(),
            &ContributionEntryRepo::new(&fixture.private),
        ))
        .expect("trusted ID-free preparation");
    ContributionExecutionEnqueueInput::try_new(
        format!("repo-0131-{label}-{}", Uuid::new_v4()),
        coverage_prompt_contract(),
        assessment_prompt_contract(),
        prepared,
    )
    .expect("typed enqueue input")
}

fn claim(fixture: &mut ContributionFixture, job: Uuid, owner: &str) -> ContributionJobLease {
    let attempt = fixture
        .admin
        .query_one(
            "UPDATE ops.jobs SET status='PROCESSING',attempt=attempt+1,lease_owner=$2,\
             lease_expires_at=clock_timestamp()+interval '10 minutes' \
             WHERE job_id=$1 RETURNING attempt",
            &[&job, &owner],
        )
        .expect("fresh fixture lease")
        .get(0);
    ContributionJobLease::try_new(job, owner.to_owned(), attempt).expect("typed lease")
}

struct NoCallProvider {
    descriptor: ReasoningProviderDescriptor,
}

impl NoCallProvider {
    fn new() -> Self {
        Self {
            descriptor: ReasoningProviderDescriptor {
                provider_id: "offline-fixture".into(),
                model_id: "offline-fixture".into(),
                model_revision: None,
                capabilities: vec![
                    ReasoningCapability::Text,
                    ReasoningCapability::StructuredOutput,
                ],
                custom_endpoint: None,
            },
        }
    }
}

#[async_trait]
impl UserReasoningProvider for NoCallProvider {
    fn descriptor(&self) -> &ReasoningProviderDescriptor {
        &self.descriptor
    }

    fn endpoint_ref(&self) -> &str {
        "https://reasoning.invalid/v1/chat/completions"
    }

    fn model_revision(&self) -> Option<&str> {
        None
    }

    async fn complete_structured(
        &self,
        _: &PrivateInferenceContext,
        _: StructuredReasoningRequest,
    ) -> Result<StructuredReasoningResponse, ReasoningProviderError> {
        panic!("prepare must not call a provider")
    }

    async fn analyze_vision(
        &self,
        _: &PrivateInferenceContext,
        _: VisionReasoningRequest,
    ) -> Result<VisionReasoningResponse, ReasoningProviderError> {
        panic!("prepare must not call a provider")
    }
}

fn prepared_plan(
    fixture: &ContributionFixture,
    execution_id: humaux_application::contribution_execution::ContributionExecutionId,
    stage: humaux_application::contribution_execution::ContributionExecutionStage,
) -> ContributionReservePlan {
    let execution = fixture
        .rt
        .block_on(
            ContributionExecutionRepo::new(&fixture.private)
                .load(fixture.auth.tenant_id().0, execution_id),
        )
        .expect("load execution for prepare")
        .expect("execution exists");
    let provider = NoCallProvider::new();
    let reasoner = ContributionReasoner::new_for_execution(
        &fixture.private,
        &provider,
        ContributionReasonerConfig {
            allowed_egress_processor_id: ProcessorId(fixture.egress_processor),
            region: "test-region".into(),
            permit_ttl: Duration::from_secs(30),
            deletion_capability: DeletionCapability::Unknown,
            system_prompt: "Remove personal identifiers and return JSON.".into(),
            json_schema: r#"{"type":"object"}"#.into(),
            max_output_tokens: 128,
        },
    )
    .expect("execution reasoner");
    fixture
        .rt
        .block_on(async {
            match stage {
                humaux_application::contribution_execution::ContributionExecutionStage::Coverage => {
                    reasoner.prepare_a(&execution).await
                }
                humaux_application::contribution_execution::ContributionExecutionStage::Assessment => {
                    reasoner.prepare_b(&execution).await
                }
            }
        })
        .expect("provider-free preparation")
        .reserve_plan()
        .expect("sealed prepared route witness")
}

fn receipt(fixture: &mut ContributionFixture, label: &str) -> CompletionScanReceipt {
    let value = json!({
        "privacy_rules_version": format!("r4-b2-{label}"),
        "privacy_rules_digest": "r4-b2-live",
        "gitleaks_version": "fixture",
        "gitleaks_binary_sha256": "a".repeat(64),
    });
    let digest: Vec<u8> = fixture
        .admin
        .query_one(
            "SELECT sha256(convert_to($1::jsonb::text,'UTF8'))",
            &[&value],
        )
        .expect("canonical receipt hash")
        .get(0);
    CompletionScanReceipt::try_new(
        value,
        ContentSha256(digest.try_into().expect("32-byte receipt hash")),
    )
    .expect("typed receipt")
}

fn coverage(fixture: &mut ContributionFixture, label: &str) -> CoverageCompletionValue {
    let probe = format!("provider-free coverage {label}").into_bytes();
    CoverageCompletionValue::Usable {
        coverage_probe_sha256: payload_sha256(&probe),
        coverage: PublicCoverageDigest::new(
            Uuid::new_v4(),
            1,
            vec![format!("bounded public summary {label}")],
        )
        .expect("bounded coverage"),
        receipt: SuccessfulCoverageReceipt::try_new(
            receipt(fixture, label),
            ProviderTraceRef(format!("coverage-trace-{label}")),
            Some(format!("coverage-request-{label}")),
        )
        .expect("coverage receipt"),
    }
}

fn ready_candidate(fixture: &mut ContributionFixture, label: &str) -> AssessmentCompletionValue {
    let output = format!("canonical assessment {label}").into_bytes();
    let assessment = AssessmentCompletionEvidence::try_new(
        output.clone(),
        ContentSha256(Sha256::digest(&output).into()),
        ContributionGate::Pass,
        ContributionGate::Pass,
        ContributionGate::Pass,
        ContributionGate::Pass,
        ProviderTraceRef(format!("assessment-trace-{label}")),
        Some(format!("assessment-request-{label}")),
    )
    .expect("assessment evidence");
    let body = format!("deidentified candidate {label}").into_bytes();
    let candidate = CandidateCompletionEvidence::try_new(
        body.clone(),
        payload_sha256(&body),
        receipt(fixture, label),
    )
    .expect("candidate evidence");
    AssessmentCompletionValue::try_ready_candidate(assessment, candidate)
        .expect("all-pass candidate")
}

fn not_contributable(label: &str) -> AssessmentCompletionValue {
    let output = format!("canonical non-contributable assessment {label}").into_bytes();
    let assessment = AssessmentCompletionEvidence::try_new(
        output.clone(),
        ContentSha256(Sha256::digest(&output).into()),
        ContributionGate::Fail,
        ContributionGate::Pass,
        ContributionGate::Pass,
        ContributionGate::Pass,
        ProviderTraceRef(format!("assessment-trace-{label}")),
        Some(format!("assessment-request-{label}")),
    )
    .expect("assessment evidence");
    AssessmentCompletionValue::try_not_contributable(assessment).expect("failed novelty gate")
}

fn wrong(binding: ExactContributionCallBinding) -> ExactContributionCallBinding {
    ExactContributionCallBinding::try_new(
        binding.execution_id(),
        binding.stage(),
        binding.model_call_id(),
        binding.request_id(),
        ReasoningIntentSha256([0xff; 32]),
        binding.disclosure_id(),
    )
    .expect("well-formed wrong binding")
}

fn db_error<T>(result: Result<T, ContributionExecutionRepoError>, label: &str) {
    assert!(
        matches!(result, Err(ContributionExecutionRepoError::Db(_))),
        "{label} must fail in authoritative SQL"
    );
}

#[test]
#[ignore = "requires disposable PostgreSQL 18 migrated through 0131"]
#[allow(clippy::too_many_lines)]
fn typed_repo_all_eight_commands_are_durable_and_provider_free() {
    if !require_db() {
        eprintln!("SKIP: HUMAUX_TEST_PG_DSN is not set");
        return;
    }

    let mut fixture = ContributionFixture::new();
    let tenant = fixture.auth.tenant_id().0;

    // 1. enqueue: create, exact retry, and conflict with no extra durable rows.
    let enqueue_input = input(&fixture, "candidate");
    let first = fixture
        .rt
        .block_on(ContributionExecutionRepo::new(&fixture.private).enqueue(&enqueue_input))
        .expect("enqueue");
    assert!(first.created);
    let initial_read = fixture
        .rt
        .block_on(ContributionExecutionRepo::new(&fixture.private).load(tenant, first.execution_id))
        .expect("typed execution read")
        .expect("enqueued execution exists");
    assert_eq!(initial_read.execution_id, first.execution_id);
    assert_eq!(initial_read.tenant_id, tenant);
    assert_eq!(initial_read.state, ContributionExecutionState::ReadyA);
    assert_eq!(initial_read.coverage_contract_version, 1);
    assert_eq!(initial_read.assessment_contract_version, 1);
    assert_eq!(
        initial_read.coverage_prompt_contract_sha256,
        coverage_prompt_contract().sha256()
    );
    assert_eq!(
        initial_read.assessment_prompt_contract_sha256,
        assessment_prompt_contract().sha256()
    );
    assert_eq!(initial_read.sources.len(), 1);
    assert_eq!(initial_read.sources[0].ordinal, 0);
    assert!(initial_read.coverage.is_none());
    let scan_receipt_json = serde_json::json!({
        "receipt_schema_version": "contribution-scan-receipt-v1",
        "scan_disposition": "PASS",
    });
    let bound_scan_receipt = fixture
        .rt
        .block_on(
            ContributionExecutionRepo::new(&fixture.private)
                .bind_completion_scan_receipt(tenant, scan_receipt_json.clone()),
        )
        .expect("bind scan receipt with PostgreSQL jsonb text digest");
    let expected_scan_digest: Vec<u8> = fixture
        .admin
        .query_one(
            "SELECT sha256(convert_to($1::jsonb::text, 'UTF8'))",
            &[&scan_receipt_json],
        )
        .expect("calculate authoritative scan digest")
        .get(0);
    assert_eq!(
        bound_scan_receipt.sha256().0.as_slice(),
        expected_scan_digest.as_slice()
    );
    fixture
        .admin
        .execute(
            "UPDATE ops.jobs SET payload='{}'::jsonb WHERE job_id=$1",
            &[&first.job_id],
        )
        .expect("payload fault injection");
    let payload_independent_read = fixture
        .rt
        .block_on(ContributionExecutionRepo::new(&fixture.private).load(tenant, first.execution_id))
        .expect("execution read ignores job payload")
        .expect("execution still exists");
    assert_eq!(
        payload_independent_read.state,
        ContributionExecutionState::ReadyA
    );
    let retry = fixture
        .rt
        .block_on(ContributionExecutionRepo::new(&fixture.private).enqueue(&enqueue_input))
        .expect("retry");
    assert!(!retry.created);
    assert_eq!(retry.execution_id, first.execution_id);
    assert_eq!(retry.job_id, first.job_id);
    assert_eq!(retry.candidate_id, first.candidate_id);
    assert_eq!(retry.logical_call_ids, first.logical_call_ids);
    let root_counts = fixture
        .admin
        .query_one(
            "SELECT (SELECT count(*) FROM ops.jobs WHERE job_id=$1),\
             (SELECT count(*) FROM private.contribution_executions WHERE execution_id=$2),\
             (SELECT count(*) FROM private.contribution_execution_sources WHERE execution_id=$2),\
             (SELECT count(*) FROM ops.contribution_execution_job_links WHERE execution_id=$2)",
            &[&first.job_id, &first.execution_id.as_uuid()],
        )
        .expect("root counts");
    let before = (
        root_counts.get::<_, i64>(0),
        root_counts.get::<_, i64>(1),
        root_counts.get::<_, i64>(2),
        root_counts.get::<_, i64>(3),
    );
    assert_eq!(before, (1, 1, 1, 1));
    let prepared = fixture
        .rt
        .block_on(prepare_assessed_input(
            (&direct_request(&fixture)).into(),
            &ContributionEntryRepo::new(&fixture.private),
        ))
        .expect("second preparation");
    let conflict = ContributionExecutionEnqueueInput::try_new(
        enqueue_input.idempotency_key().to_owned(),
        ContributionPromptContract::try_new(9, ContentSha256([9; 32])).expect("changed contract"),
        assessment_prompt_contract(),
        prepared,
    )
    .expect("conflicting input");
    db_error(
        fixture
            .rt
            .block_on(ContributionExecutionRepo::new(&fixture.private).enqueue(&conflict)),
        "enqueue conflict",
    );
    let after = fixture
        .admin
        .query_one(
            "SELECT (SELECT count(*) FROM ops.jobs WHERE job_id=$1),\
             (SELECT count(*) FROM private.contribution_executions WHERE execution_id=$2),\
             (SELECT count(*) FROM private.contribution_execution_sources WHERE execution_id=$2),\
             (SELECT count(*) FROM ops.contribution_execution_job_links WHERE execution_id=$2)",
            &[&first.job_id, &first.execution_id.as_uuid()],
        )
        .expect("post-conflict counts");
    assert_eq!(
        (
            after.get::<_, i64>(0),
            after.get::<_, i64>(1),
            after.get::<_, i64>(2),
            after.get::<_, i64>(3)
        ),
        before
    );

    // 2. reserve A: first call grants the only permit; re-observation returns no permit,
    // preserves the single ledger/disclosure pair, and records reconciliation on the job.
    let lease_a1 = claim(&mut fixture, first.job_id, "typed-a-1");
    let reserve_a_plan = prepared_plan(
        &fixture,
        first.execution_id,
        humaux_application::contribution_execution::ContributionExecutionStage::Coverage,
    );
    let a_new = fixture
        .rt
        .block_on(
            ContributionExecutionRepo::new(&fixture.private).reserve_a_new(
                tenant,
                first.execution_id,
                &lease_a1,
                &reserve_a_plan,
            ),
        )
        .expect("reserve A");
    let a_binding = a_new.binding();
    assert!(a_new.dispatch_permit().is_some());
    let a_existing = fixture
        .rt
        .block_on(
            ContributionExecutionRepo::new(&fixture.private).reserve_a_new(
                tenant,
                first.execution_id,
                &lease_a1,
                &reserve_a_plan,
            ),
        )
        .expect("existing A");
    assert_eq!(a_existing.binding(), a_binding);
    assert!(a_existing.dispatch_permit().is_none());
    let observed_a = fixture
        .admin
        .query_one(
            "SELECT e.state::text,j.status,j.last_error_class,\
             (SELECT count(*) FROM ops.model_call_ledger l WHERE l.model_call_id=e.coverage_model_call_id),\
             (SELECT count(*) FROM ops.data_disclosures d WHERE d.disclosure_id=e.coverage_disclosure_id) \
             FROM private.contribution_executions e \
             JOIN ops.contribution_execution_job_links x USING(execution_id) \
             JOIN ops.jobs j USING(job_id) WHERE e.execution_id=$1",
            &[&first.execution_id.as_uuid()],
        )
        .expect("A observation");
    assert_eq!(observed_a.get::<_, String>(0), "A_RESERVED");
    assert_eq!(observed_a.get::<_, String>(1), "FAILED");
    assert_eq!(
        observed_a.get::<_, Option<String>>(2).as_deref(),
        Some("RECONCILIATION_REQUIRED")
    );
    assert_eq!(observed_a.get::<_, i64>(3), 1);
    assert_eq!(observed_a.get::<_, i64>(4), 1);

    // 3. complete A exact: representative binding mismatch rolls back; exact receipt advances.
    let lease_a2 = claim(&mut fixture, first.job_id, "typed-a-2");
    let wrong_a = CoverageCompletion::try_new(
        wrong(a_binding),
        wrong(a_binding),
        coverage(&mut fixture, "wrong-a"),
    )
    .expect("application-valid wrong A");
    db_error(
        fixture.rt.block_on(
            ContributionExecutionRepo::new(&fixture.private).complete_a_exact(
                tenant,
                &wrong_a,
                Some(&lease_a2),
            ),
        ),
        "A exact mismatch",
    );
    let unchanged_a = fixture
        .admin
        .query_one(
            "SELECT e.state::text,l.status,d.outcome FROM private.contribution_executions e \
             JOIN ops.model_call_ledger l ON l.model_call_id=e.coverage_model_call_id \
             JOIN ops.data_disclosures d ON d.disclosure_id=e.coverage_disclosure_id \
             WHERE e.execution_id=$1",
            &[&first.execution_id.as_uuid()],
        )
        .expect("A rollback");
    assert_eq!(unchanged_a.get::<_, String>(0), "A_RESERVED");
    assert_eq!(unchanged_a.get::<_, String>(1), "RESERVED");
    assert_eq!(unchanged_a.get::<_, Option<String>>(2), None);
    let complete_a =
        CoverageCompletion::try_new(a_binding, a_binding, coverage(&mut fixture, "candidate-a"))
            .expect("exact A");
    assert_eq!(
        fixture
            .rt
            .block_on(
                ContributionExecutionRepo::new(&fixture.private).complete_a_exact(
                    tenant,
                    &complete_a,
                    Some(&lease_a2),
                )
            )
            .expect("complete A"),
        ContributionExecutionState::ReadyB
    );
    let ready_b_read = fixture
        .rt
        .block_on(ContributionExecutionRepo::new(&fixture.private).load(tenant, first.execution_id))
        .expect("typed READY_B read")
        .expect("READY_B execution exists");
    assert_eq!(ready_b_read.state, ContributionExecutionState::ReadyB);
    let coverage_snapshot = ready_b_read.coverage.expect("READY_B coverage snapshot");
    assert!(coverage_snapshot.snapshot_id != Uuid::nil());
    assert!(coverage_snapshot.version > 0);
    assert!(!coverage_snapshot.canonical_summaries.is_empty());
    let coverage_digest: [u8; 32] = Sha256::digest(&coverage_snapshot.canonical_summaries).into();
    assert_eq!(coverage_snapshot.digest_sha256.0, coverage_digest);

    // 4. reserve B: same permit/no-permit and reconciliation contract.
    let reserve_b_plan = prepared_plan(
        &fixture,
        first.execution_id,
        humaux_application::contribution_execution::ContributionExecutionStage::Assessment,
    );
    let b_new = fixture
        .rt
        .block_on(
            ContributionExecutionRepo::new(&fixture.private).reserve_b_new(
                tenant,
                first.execution_id,
                &lease_a2,
                &reserve_b_plan,
            ),
        )
        .expect("reserve B");
    let b_binding = b_new.binding();
    assert!(b_new.dispatch_permit().is_some());
    let b_existing = fixture
        .rt
        .block_on(
            ContributionExecutionRepo::new(&fixture.private).reserve_b_new(
                tenant,
                first.execution_id,
                &lease_a2,
                &reserve_b_plan,
            ),
        )
        .expect("existing B");
    assert_eq!(b_existing.binding(), b_binding);
    assert!(b_existing.dispatch_permit().is_none());

    // 5. complete B exact: representative mismatch rolls back, then READY_CANDIDATE persists.
    let lease_b2 = claim(&mut fixture, first.job_id, "typed-b-2");
    let wrong_b = AssessmentCompletion::try_new(
        wrong(b_binding),
        wrong(b_binding),
        ready_candidate(&mut fixture, "wrong-b"),
    )
    .expect("application-valid wrong B");
    db_error(
        fixture.rt.block_on(
            ContributionExecutionRepo::new(&fixture.private).complete_b_exact(
                tenant,
                &wrong_b,
                Some(&lease_b2),
            ),
        ),
        "B exact mismatch",
    );
    let unchanged_b = fixture
        .admin
        .query_one(
            "SELECT e.state::text,l.status,d.outcome,\
             (SELECT count(*) FROM staging.contribution_candidates c WHERE c.contribution_execution_id=e.execution_id) \
             FROM private.contribution_executions e \
             JOIN ops.model_call_ledger l ON l.model_call_id=e.assessment_model_call_id \
             JOIN ops.data_disclosures d ON d.disclosure_id=e.assessment_disclosure_id \
             WHERE e.execution_id=$1",
            &[&first.execution_id.as_uuid()],
        )
        .expect("B rollback");
    assert_eq!(unchanged_b.get::<_, String>(0), "B_RESERVED");
    assert_eq!(unchanged_b.get::<_, String>(1), "RESERVED");
    assert_eq!(unchanged_b.get::<_, Option<String>>(2), None);
    assert_eq!(unchanged_b.get::<_, i64>(3), 0);
    let complete_b = AssessmentCompletion::try_new(
        b_binding,
        b_binding,
        ready_candidate(&mut fixture, "candidate-b"),
    )
    .expect("exact B");
    assert_eq!(
        fixture
            .rt
            .block_on(
                ContributionExecutionRepo::new(&fixture.private).complete_b_exact(
                    tenant,
                    &complete_b,
                    Some(&lease_b2),
                )
            )
            .expect("complete B"),
        ContributionExecutionState::ReadyCandidate
    );

    // 6. commit candidate: all coupled candidate rows and both terminal states commit together.
    let committed_candidate = fixture
        .rt
        .block_on(
            ContributionExecutionRepo::new(&fixture.private).commit_candidate(
                tenant,
                first.execution_id,
                &lease_b2,
            ),
        )
        .expect("commit candidate");
    assert_eq!(committed_candidate, first.candidate_id);
    let candidate = fixture
        .admin
        .query_one(
            "SELECT e.state::text,j.status,\
             (SELECT count(*) FROM staging.contribution_candidates c WHERE c.contribution_execution_id=e.execution_id),\
             (SELECT count(*) FROM staging.contribution_candidate_sources s WHERE s.candidate_id=e.candidate_id),\
             (SELECT count(*) FROM staging.contribution_candidate_phase9_assessments a WHERE a.candidate_id=e.candidate_id) \
             FROM private.contribution_executions e \
             JOIN ops.contribution_execution_job_links x USING(execution_id) \
             JOIN ops.jobs j USING(job_id) WHERE e.execution_id=$1",
            &[&first.execution_id.as_uuid()],
        )
        .expect("candidate observation");
    assert_eq!(candidate.get::<_, String>(0), "DONE");
    assert_eq!(candidate.get::<_, String>(1), "DONE");
    assert_eq!(candidate.get::<_, i64>(2), 1);
    assert_eq!(candidate.get::<_, i64>(3), 1);
    assert_eq!(candidate.get::<_, i64>(4), 1);

    // 7. settle terminal: a lease-free exact late receipt leaves the expired job for a fresh
    // claimant, which settles through the typed command without replaying provider work.
    let terminal_input = input(&fixture, "late-terminal");
    let terminal = fixture
        .rt
        .block_on(ContributionExecutionRepo::new(&fixture.private).enqueue(&terminal_input))
        .expect("terminal enqueue");
    let terminal_lease = claim(&mut fixture, terminal.job_id, "terminal");
    let terminal_a_plan = prepared_plan(
        &fixture,
        terminal.execution_id,
        humaux_application::contribution_execution::ContributionExecutionStage::Coverage,
    );
    let terminal_a_binding = fixture
        .rt
        .block_on(
            ContributionExecutionRepo::new(&fixture.private).reserve_a_new(
                tenant,
                terminal.execution_id,
                &terminal_lease,
                &terminal_a_plan,
            ),
        )
        .expect("terminal reserve A")
        .binding();
    let terminal_a = CoverageCompletion::try_new(
        terminal_a_binding,
        terminal_a_binding,
        coverage(&mut fixture, "terminal-a"),
    )
    .expect("terminal A");
    fixture
        .rt
        .block_on(
            ContributionExecutionRepo::new(&fixture.private).complete_a_exact(
                tenant,
                &terminal_a,
                Some(&terminal_lease),
            ),
        )
        .expect("terminal complete A");
    let terminal_b_plan = prepared_plan(
        &fixture,
        terminal.execution_id,
        humaux_application::contribution_execution::ContributionExecutionStage::Assessment,
    );
    let terminal_b_binding = fixture
        .rt
        .block_on(
            ContributionExecutionRepo::new(&fixture.private).reserve_b_new(
                tenant,
                terminal.execution_id,
                &terminal_lease,
                &terminal_b_plan,
            ),
        )
        .expect("terminal reserve B")
        .binding();
    fixture
        .admin
        .execute(
            "UPDATE ops.jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE job_id=$1",
            &[&terminal.job_id],
        )
        .expect("expire claimant");
    let terminal_b = AssessmentCompletion::try_new(
        terminal_b_binding,
        terminal_b_binding,
        not_contributable("late-terminal"),
    )
    .expect("late terminal B");
    assert_eq!(
        fixture
            .rt
            .block_on(
                ContributionExecutionRepo::new(&fixture.private).complete_b_exact(
                    tenant,
                    &terminal_b,
                    None,
                )
            )
            .expect("lease-free late completion"),
        ContributionExecutionState::NotContributable
    );
    let late = fixture
        .admin
        .query_one(
            "SELECT e.state::text,l.status,j.status,j.last_error_class \
             FROM private.contribution_executions e \
             JOIN ops.model_call_ledger l ON l.model_call_id=e.assessment_model_call_id \
             JOIN ops.contribution_execution_job_links x USING(execution_id) \
             JOIN ops.jobs j USING(job_id) WHERE e.execution_id=$1",
            &[&terminal.execution_id.as_uuid()],
        )
        .expect("late observation");
    assert_eq!(late.get::<_, String>(0), "NOT_CONTRIBUTABLE");
    assert_eq!(late.get::<_, String>(1), "SUCCEEDED");
    assert_eq!(late.get::<_, String>(2), "PROCESSING");
    assert_eq!(late.get::<_, Option<String>>(3), None);
    let settle_lease = claim(&mut fixture, terminal.job_id, "terminal-settle");
    let settled = fixture
        .rt
        .block_on(
            ContributionExecutionRepo::new(&fixture.private).settle_terminal_job(
                tenant,
                terminal.execution_id,
                &settle_lease,
            ),
        )
        .expect("settle terminal");
    assert_eq!(settled, "DONE");

    // 8. reconciliation: preserve the exact reservation and fail only the claimed job.
    let reconciliation_input = input(&fixture, "reconciliation");
    let reconciliation = fixture
        .rt
        .block_on(ContributionExecutionRepo::new(&fixture.private).enqueue(&reconciliation_input))
        .expect("reconciliation enqueue");
    let reconciliation_lease = claim(&mut fixture, reconciliation.job_id, "reconciliation");
    let reconciliation_plan = prepared_plan(
        &fixture,
        reconciliation.execution_id,
        humaux_application::contribution_execution::ContributionExecutionStage::Coverage,
    );
    assert!(
        fixture
            .rt
            .block_on(
                ContributionExecutionRepo::new(&fixture.private).reserve_a_new(
                    tenant,
                    reconciliation.execution_id,
                    &reconciliation_lease,
                    &reconciliation_plan,
                )
            )
            .expect("reconciliation reserve")
            .dispatch_permit()
            .is_some()
    );
    let reconciliation_marked = fixture
        .rt
        .block_on(
            ContributionExecutionRepo::new(&fixture.private).mark_reconciliation_required(
                tenant,
                reconciliation.execution_id,
                &reconciliation_lease,
            ),
        )
        .expect("mark reconciliation");
    assert!(reconciliation_marked);
    let reconciled = fixture
        .admin
        .query_one(
            "SELECT e.state::text,l.status,j.status,j.last_error_class \
             FROM private.contribution_executions e \
             JOIN ops.model_call_ledger l ON l.model_call_id=e.coverage_model_call_id \
             JOIN ops.contribution_execution_job_links x USING(execution_id) \
             JOIN ops.jobs j USING(job_id) WHERE e.execution_id=$1",
            &[&reconciliation.execution_id.as_uuid()],
        )
        .expect("reconciliation observation");
    assert_eq!(reconciled.get::<_, String>(0), "A_RESERVED");
    assert_eq!(reconciled.get::<_, String>(1), "RESERVED");
    assert_eq!(reconciled.get::<_, String>(2), "FAILED");
    assert_eq!(
        reconciled.get::<_, Option<String>>(3).as_deref(),
        Some("RECONCILIATION_REQUIRED")
    );
    eprintln!(
        "R4-B2 live observations: enqueue_bundle={before:?}; \
         A_mismatch=A_RESERVED/RESERVED/open; B_mismatch=B_RESERVED/RESERVED/open/candidates=0; \
         candidate_id={committed_candidate}; candidate_bundle=DONE/DONE/1/1/1; \
         late_completion=NOT_CONTRIBUTABLE/SUCCEEDED/PROCESSING; settle={settled}; \
         reconciliation_marked={reconciliation_marked}/A_RESERVED/RESERVED/FAILED"
    );
}
