//! `private-worker::tests::contribution_execution_runner` — Real-PostgreSQL R4-C worker coverage with an in-process
//!   recording provider.
//! Depends-on: crates=[async-trait, humaux-adapters, humaux-application, humaux-domain, humaux-testkit, postgres, serde_json, sha2, uuid]; services=[PostgreSQL(owner) r=[ops.contribution_execution_job_links, private.contribution_executions, staging.contribution_candidates] w=[ops.jobs]]; env=[HUMAUX_TEST_PG_DSN]; modules=[adapters::byok, adapters::contribution_entry_repo, adapters::contribution_execution_repo, adapters::contribution_reasoner, adapters::contribution_scan, adapters::disclosure, adapters::jobs, adapters::tests::support::contribution_fixture, application::contribute, application::contribution_execution, domain::egress, domain::error, humaux-private-worker, humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [no external model or production credential; admin SQL only seeds, injects faults and observes,
//!   mutations go through the typed runner; without a DB the test SKIPs unless HUMAUX_REQUIRE_DB, then panics]
//! Spec: none
//!
//! No external model or production credential is used. Admin SQL is limited to fixture setup,
//! fault injection, and durable observation; contribution mutations go through the typed runner.
#![allow(deprecated)] // Shared fixture retains one explicitly legacy compatibility helper.

#[path = "../../../crates/adapters/tests/support/contribution_fixture.rs"]
mod contribution_fixture;

use std::{
    collections::{BTreeSet, VecDeque},
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use contribution_fixture::ContributionFixture;
use humaux_adapters::{
    byok::{
        PrivateInferenceContext, ReasoningCapability, ReasoningProviderDescriptor,
        ReasoningProviderError, StructuredReasoningRequest, StructuredReasoningResponse,
        TokenUsage, UserReasoningProvider, VisionReasoningRequest, VisionReasoningResponse,
        structured_request_body,
    },
    contribution_entry_repo::ContributionEntryRepo,
    contribution_execution_repo::{ContributionExecutionRepo, ContributionJobLease},
    contribution_reasoner::{
        ContributionReasoner, ContributionReasonerConfig, assessment_prompt_contract,
        coverage_prompt_contract,
    },
    contribution_scan::{ContributionScanner, ContributionScannerConfig},
    disclosure::DeletionCapability,
    jobs,
};
use humaux_application::{
    contribute::prepare_assessed_input,
    contribution_execution::{ContributionExecutionEnqueueInput, ContributionExecutionState},
};
use humaux_domain::{egress::ProcessorId, error::ErrorCode};
use humaux_private_worker::{
    ContributionExecutionRunner, ContributionExecutionRunnerConfig,
    ContributionExecutionRunnerError, ContributionRunOnceReport,
};
use postgres::{Client, NoTls};
use sha2::{Digest, Sha256};
use uuid::Uuid;

static SERIAL: Mutex<()> = Mutex::new(());
static SCANNER_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

/// Env probe only; the skip goes through `humaux_testkit::skip_or_fail`, which turns a
/// missing DSN into a failure under `HUMAUX_REQUIRE_DB=1` (§79.2, ADR-0051 D-K).
fn require_db() -> bool {
    std::env::var("HUMAUX_TEST_PG_DSN").is_ok()
}

enum ProviderStep {
    Json(String),
    RemoveThenJson(PathBuf, String),
    SleepThenJson(Duration, String),
    Permanent,
    MayHaveReachedTimeout,
}

struct RecordingProvider {
    descriptor: ReasoningProviderDescriptor,
    endpoint_ref: String,
    steps: Mutex<VecDeque<ProviderStep>>,
    calls: AtomicUsize,
}

impl RecordingProvider {
    fn new(steps: impl IntoIterator<Item = ProviderStep>) -> Self {
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
            endpoint_ref: "https://reasoning.invalid/v1/chat/completions".into(),
            steps: Mutex::new(steps.into_iter().collect()),
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl UserReasoningProvider for RecordingProvider {
    fn descriptor(&self) -> &ReasoningProviderDescriptor {
        &self.descriptor
    }

    fn endpoint_ref(&self) -> &str {
        &self.endpoint_ref
    }

    fn model_revision(&self) -> Option<&str> {
        self.descriptor.model_revision.as_deref()
    }

    async fn complete_structured(
        &self,
        context: &PrivateInferenceContext,
        request: StructuredReasoningRequest,
    ) -> Result<StructuredReasoningResponse, ReasoningProviderError> {
        let wire = structured_request_body(&self.descriptor, &request);
        let wire_sha256: [u8; 32] = Sha256::digest(&wire).into();
        assert_eq!(context.egress_permit().payload_sha256(), wire_sha256);
        self.calls.fetch_add(1, Ordering::SeqCst);
        let step = self
            .steps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pop_front()
            .expect("recording provider has an exact response for every authorized call");
        match step {
            ProviderStep::Json(json) => Ok(StructuredReasoningResponse {
                json,
                usage: TokenUsage::default(),
            }),
            ProviderStep::RemoveThenJson(path, json) => {
                fs::remove_file(path)
                    .expect("inject scanner disappearance after provider response");
                Ok(StructuredReasoningResponse {
                    json,
                    usage: TokenUsage::default(),
                })
            }
            ProviderStep::SleepThenJson(duration, json) => {
                std::thread::sleep(duration);
                Ok(StructuredReasoningResponse {
                    json,
                    usage: TokenUsage::default(),
                })
            }
            ProviderStep::Permanent => Err(ReasoningProviderError::ProviderPermanent {
                message: "fixture policy denial".into(),
            }),
            ProviderStep::MayHaveReachedTimeout => {
                Err(ReasoningProviderError::RetryWait { retry_after: None })
            }
        }
    }

    async fn analyze_vision(
        &self,
        _context: &PrivateInferenceContext,
        _request: VisionReasoningRequest,
    ) -> Result<VisionReasoningResponse, ReasoningProviderError> {
        Err(ReasoningProviderError::UnsupportedCapability(
            ReasoningCapability::Vision,
        ))
    }
}

struct TestScannerExecutable(PathBuf);

impl Drop for TestScannerExecutable {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn test_scanner() -> (TestScannerExecutable, ContributionScanner) {
    let sequence = SCANNER_SEQUENCE.fetch_add(1, Ordering::SeqCst);
    let path = std::env::temp_dir().join(format!(
        "humaux-r4c-scanner-{}-{sequence}",
        std::process::id()
    ));
    let body = b"#!/bin/sh\ncase \"$1\" in\n  version) printf '%s\\n' 'r4-c-test-scanner' ;;\n  stdin) cat >/dev/null; exit 0 ;;\n  *) exit 2 ;;\nesac\n";
    fs::write(&path, body).expect("write isolated scanner executable");
    let mut permissions = fs::metadata(&path).expect("scanner metadata").permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(&path, permissions).expect("scanner permissions");
    let digest = Sha256::digest(body)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let scanner = ContributionScanner::new(ContributionScannerConfig {
        executable: path.clone(),
        expected_version: "r4-c-test-scanner".into(),
        expected_executable_sha256: digest,
        timeout: Duration::from_secs(10),
        max_payload_bytes: 65_536,
        finding_exit_code: 1,
    })
    .expect("construct pinned isolated scanner");
    (TestScannerExecutable(path), scanner)
}

fn reasoner_config(fixture: &ContributionFixture) -> ContributionReasonerConfig {
    ContributionReasonerConfig {
        allowed_egress_processor_id: ProcessorId(fixture.egress_processor),
        region: "test-region".into(),
        permit_ttl: Duration::from_secs(30),
        deletion_capability: DeletionCapability::Unknown,
        system_prompt: "validated static contribution prompt".into(),
        json_schema: r#"{"type":"object"}"#.into(),
        max_output_tokens: 128,
    }
}

fn enqueue(
    fixture: &ContributionFixture,
    label: &str,
) -> humaux_adapters::contribution_execution_repo::EnqueuedContributionExecution {
    let prepared = fixture
        .rt
        .block_on(prepare_assessed_input(
            (&fixture.request()).into(),
            &ContributionEntryRepo::new(&fixture.private),
        ))
        .expect("trusted runner preparation");
    let input = ContributionExecutionEnqueueInput::try_new(
        format!("r4-c-runner-{label}-{}", Uuid::new_v4()),
        coverage_prompt_contract(),
        assessment_prompt_contract(),
        prepared,
    )
    .expect("typed runner enqueue input");
    fixture
        .rt
        .block_on(ContributionExecutionRepo::new(&fixture.private).enqueue(&input))
        .expect("enqueue runner execution")
}

fn run(
    fixture: &ContributionFixture,
    provider: &RecordingProvider,
    scanner: &ContributionScanner,
    lease_seconds: f64,
) -> Result<ContributionRunOnceReport, ContributionExecutionRunnerError> {
    let reasoner = ContributionReasoner::new_for_execution(
        &fixture.private,
        provider,
        reasoner_config(fixture),
    )
    .expect("execution reasoner");
    let runner = ContributionExecutionRunner::new(
        &fixture.private,
        &reasoner,
        scanner,
        ContributionExecutionRunnerConfig {
            lease_owner: format!("r4-c-runner-{}", Uuid::new_v4()),
            lease_seconds,
        },
    )
    .expect("bounded runner");
    fixture
        .rt
        .block_on(runner.run_once(fixture.auth.tenant_id().0))
}

fn requeue_for_reconciliation(fixture: &mut ContributionFixture, job_id: Uuid) {
    fixture
        .admin
        .execute(
            "UPDATE ops.jobs SET status='PENDING',next_retry_at=clock_timestamp(),lease_owner=NULL,lease_expires_at=NULL,last_error_class=NULL WHERE job_id=$1",
            &[&job_id],
        )
        .expect("simulate authorized reconciliation requeue");
}

fn reserve_a_without_dispatch(
    fixture: &ContributionFixture,
    provider: &RecordingProvider,
    execution_id: humaux_application::contribution_execution::ContributionExecutionId,
    owner: &str,
) {
    let claimed = fixture
        .rt
        .block_on(jobs::private_claim(
            &fixture.private,
            fixture.auth.tenant_id().0,
            owner,
            60.0,
            1,
        ))
        .expect("private claim before A reserve");
    assert_eq!(claimed.len(), 1);
    let lease =
        ContributionJobLease::try_new(claimed[0].job_id, owner.to_owned(), claimed[0].attempt)
            .expect("typed contribution lease");
    let repo = ContributionExecutionRepo::new(&fixture.private);
    let execution = fixture
        .rt
        .block_on(repo.load(fixture.auth.tenant_id().0, execution_id))
        .expect("load execution before A reserve")
        .expect("execution exists before A reserve");
    let reasoner = ContributionReasoner::new_for_execution(
        &fixture.private,
        provider,
        reasoner_config(fixture),
    )
    .expect("execution reasoner");
    let prepared = fixture
        .rt
        .block_on(reasoner.prepare_a(&execution))
        .expect("prepare A without dispatch");
    let plan = prepared.reserve_plan().expect("A reserve plan");
    let permit = fixture
        .rt
        .block_on(repo.reserve_a_new(execution.tenant_id, execution.execution_id, &lease, &plan))
        .expect("durable A reserve")
        .dispatch_permit()
        .expect("new reserve grants the sole permit");
    drop(permit); // fault injection: committed reservation, process dies before HTTP.
}

fn reconnected_admin() -> Client {
    // dep: PostgreSQL(owner) — role-scoped pool call
    Client::connect(
        &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PostgreSQL 18 DSN"),
        NoTls,
    )
    .expect("fresh PostgreSQL observation connection")
}

fn state_and_job(execution_id: Uuid, job_id: Uuid) -> (String, String, Option<String>) {
    let row = reconnected_admin()
        .query_one(
            "SELECT e.state::text,j.status,j.last_error_class FROM private.contribution_executions e JOIN ops.contribution_execution_job_links l USING(tenant_id,execution_id) JOIN ops.jobs j USING(tenant_id,job_id) WHERE e.execution_id=$1 AND j.job_id=$2",
            &[&execution_id, &job_id],
        )
        .expect("durable runner state");
    (row.get(0), row.get(1), row.get(2))
}

fn candidate_count(execution_id: Uuid) -> i64 {
    reconnected_admin()
        .query_one(
            "SELECT count(*) FROM staging.contribution_candidates WHERE contribution_execution_id=$1",
            &[&execution_id],
        )
        .expect("reconnected candidate count")
        .get(0)
}

fn record_fault_evidence(
    observed: &mut BTreeSet<String>,
    gate_id: &str,
    variant: &str,
    case_id: &str,
    dispatch_before: usize,
    dispatch_after_first: usize,
    dispatch_after_successor: usize,
) {
    let key = format!("{gate_id}/{variant}");
    assert!(
        observed.insert(key.clone()),
        "duplicate fault evidence key {key}"
    );
    let manifest_sha256: String =
        Sha256::digest(include_bytes!("../../../contracts/r4_fault_manifest.toml"))
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
    println!(
        "R4_FAULT_EVIDENCE {}",
        serde_json::json!({
            "gate_id": gate_id,
            "variant": variant,
            "case_id": case_id,
            "manifest_sha256": manifest_sha256,
            "durable_observation_connection": "reconnected",
            "provider": "recording",
            "external_provider_calls": 0,
            "phase10": false,
            "dispatch_before": dispatch_before,
            "dispatch_after_first": dispatch_after_first,
            "dispatch_after_successor": dispatch_after_successor,
            "dispatch_successor_delta": dispatch_after_successor - dispatch_after_first,
        })
    );
}

#[test]
#[ignore = "lane(a:shared_db) requires disposable PostgreSQL 18 migrated through 0131"]
#[allow(clippy::too_many_lines)]
fn runner_closes_dispatch_replay_scan_terminal_and_late_completion_paths() {
    if !require_db() {
        humaux_testkit::skip_or_fail(
            "runner_closes_dispatch_replay_scan_terminal_and_late_completion_paths",
            "HUMAUX_TEST_PG_DSN",
            humaux_testkit::ExternalDep::Postgres,
        );
        return;
    }
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut observed_fault_keys = BTreeSet::new();

    // Fresh A -> B -> candidate commit: exactly two authorized calls and one durable candidate.
    {
        let fixture = ContributionFixture::new();
        let enqueued = enqueue(&fixture, "happy");
        let provider = RecordingProvider::new([
            ProviderStep::Json(r#"{"probe":"general knowledge"}"#.into()),
            ProviderStep::Json(r#"{"novelty":"PASS","quality":"PASS","generality":"PASS","grounding":"PASS","candidate":"generalized candidate"}"#.into()),
        ]);
        let (_scanner_file, scanner) = test_scanner();
        assert_eq!(
            run(&fixture, &provider, &scanner, 60.0).expect("happy runner"),
            ContributionRunOnceReport::Completed {
                execution_id: enqueued.execution_id,
                state: ContributionExecutionState::Done,
            }
        );
        assert_eq!(provider.calls(), 2);
        assert_eq!(
            state_and_job(enqueued.execution_id.as_uuid(), enqueued.job_id),
            ("DONE".into(), "DONE".into(), None)
        );
        assert_eq!(candidate_count(enqueued.execution_id.as_uuid()), 1);
    }

    // R4-FG-04/BASE: reservation commits, the process dies before provider I/O, and a
    // successor observes A_RESERVED without receiving another dispatch permit. The same
    // successor observation is R4-FG-15/BASE's zero-redispatch reserved-stage gate.
    {
        let mut fixture = ContributionFixture::new();
        let enqueued = enqueue(&fixture, "a-reserve-crash");
        let provider = RecordingProvider::new([]);
        reserve_a_without_dispatch(
            &fixture,
            &provider,
            enqueued.execution_id,
            "r4-a-reserve-crash",
        );
        assert_eq!(provider.calls(), 0);
        assert_eq!(
            state_and_job(enqueued.execution_id.as_uuid(), enqueued.job_id),
            ("A_RESERVED".into(), "PROCESSING".into(), None)
        );
        requeue_for_reconciliation(&mut fixture, enqueued.job_id);
        let (_scanner_file, scanner) = test_scanner();
        assert_eq!(
            run(&fixture, &provider, &scanner, 60.0).expect("reserved A successor"),
            ContributionRunOnceReport::ReconciliationRequired {
                execution_id: enqueued.execution_id,
                state: ContributionExecutionState::AReserved,
            }
        );
        assert_eq!(
            provider.calls(),
            0,
            "reserved A successor must not dispatch"
        );
        assert_eq!(
            state_and_job(enqueued.execution_id.as_uuid(), enqueued.job_id),
            (
                "A_RESERVED".into(),
                "FAILED".into(),
                Some("RECONCILIATION_REQUIRED".into())
            )
        );
        record_fault_evidence(
            &mut observed_fault_keys,
            "R4-FG-04",
            "BASE",
            "runner_a_reserve_crash_zero_dispatch",
            0,
            0,
            0,
        );
        record_fault_evidence(
            &mut observed_fault_keys,
            "R4-FG-15",
            "BASE",
            "runner_reserved_successor_zero_redispatch",
            0,
            0,
            0,
        );
    }

    // R4-FG-08/BASE + R4-FG-18/TIMEOUT: A completes, B reserve commits, then the provider
    // reports the timeout-shaped nonterminal outcome. A successor observes B_RESERVED and
    // cannot redispatch B.
    {
        let mut fixture = ContributionFixture::new();
        let enqueued = enqueue(&fixture, "b-timeout");
        let provider = RecordingProvider::new([
            ProviderStep::Json(r#"{"probe":"general knowledge"}"#.into()),
            ProviderStep::MayHaveReachedTimeout,
        ]);
        let (_scanner_file, scanner) = test_scanner();
        assert_eq!(
            run(&fixture, &provider, &scanner, 60.0).expect("B timeout reconciliation"),
            ContributionRunOnceReport::ReconciliationRequired {
                execution_id: enqueued.execution_id,
                state: ContributionExecutionState::BReserved,
            }
        );
        assert_eq!(provider.calls(), 2);
        assert_eq!(
            state_and_job(enqueued.execution_id.as_uuid(), enqueued.job_id),
            (
                "B_RESERVED".into(),
                "FAILED".into(),
                Some("RECONCILIATION_REQUIRED".into())
            )
        );
        requeue_for_reconciliation(&mut fixture, enqueued.job_id);
        assert_eq!(
            run(&fixture, &provider, &scanner, 60.0).expect("reserved successor"),
            ContributionRunOnceReport::ReconciliationRequired {
                execution_id: enqueued.execution_id,
                state: ContributionExecutionState::BReserved,
            }
        );
        assert_eq!(
            provider.calls(),
            2,
            "reserved successor must not redispatch"
        );
        record_fault_evidence(
            &mut observed_fault_keys,
            "R4-FG-08",
            "BASE",
            "runner_b_reserve_timeout_zero_redispatch",
            0,
            2,
            2,
        );
        record_fault_evidence(
            &mut observed_fault_keys,
            "R4-FG-18",
            "TIMEOUT",
            "runner_b_reserve_timeout_zero_redispatch",
            0,
            2,
            2,
        );
    }

    // R4-FG-18/B_GATE_FAIL: the provider call succeeds and the typed raw FAIL gate produces
    // NOT_CONTRIBUTABLE. It is a normal business result with no durable candidate.
    {
        let fixture = ContributionFixture::new();
        let enqueued = enqueue(&fixture, "b-gate-fail");
        let provider = RecordingProvider::new([
            ProviderStep::Json(r#"{"probe":"general knowledge"}"#.into()),
            ProviderStep::Json(r#"{"novelty":"PASS","quality":"FAIL","generality":"PASS","grounding":"PASS","candidate":"unused generalized candidate"}"#.into()),
        ]);
        let (_scanner_file, scanner) = test_scanner();
        assert_eq!(
            run(&fixture, &provider, &scanner, 60.0).expect("B gate fail"),
            ContributionRunOnceReport::Completed {
                execution_id: enqueued.execution_id,
                state: ContributionExecutionState::NotContributable,
            }
        );
        assert_eq!(provider.calls(), 2);
        assert_eq!(
            state_and_job(enqueued.execution_id.as_uuid(), enqueued.job_id),
            ("NOT_CONTRIBUTABLE".into(), "DONE".into(), None)
        );
        assert_eq!(candidate_count(enqueued.execution_id.as_uuid()), 0);
        record_fault_evidence(
            &mut observed_fault_keys,
            "R4-FG-18",
            "B_GATE_FAIL",
            "runner_live_late_and_outcomes",
            0,
            2,
            2,
        );
    }

    // R4-FG-18/SCAN_REJECT: a clean A reaches B; the deidentified candidate scanner rejects
    // deterministically. Provider receipts succeed, no candidate is committed, and this is
    // not classified as provider failure.
    {
        let fixture = ContributionFixture::new();
        let enqueued = enqueue(&fixture, "b-scan-reject");
        let provider = RecordingProvider::new([
            ProviderStep::Json(r#"{"probe":"general knowledge"}"#.into()),
            ProviderStep::Json(r#"{"novelty":"PASS","quality":"PASS","generality":"PASS","grounding":"PASS","candidate":"contact person@example.test"}"#.into()),
        ]);
        let (_scanner_file, scanner) = test_scanner();
        assert_eq!(
            run(&fixture, &provider, &scanner, 60.0).expect("scanner rejection"),
            ContributionRunOnceReport::Completed {
                execution_id: enqueued.execution_id,
                state: ContributionExecutionState::RejectedSafety,
            }
        );
        assert_eq!(provider.calls(), 2);
        assert_eq!(
            state_and_job(enqueued.execution_id.as_uuid(), enqueued.job_id),
            ("REJECTED_SAFETY".into(), "DONE".into(), None)
        );
        assert_eq!(candidate_count(enqueued.execution_id.as_uuid()), 0);
        record_fault_evidence(
            &mut observed_fault_keys,
            "R4-FG-18",
            "SCAN_REJECT",
            "runner_live_late_and_outcomes",
            0,
            2,
            2,
        );
    }

    // A definite provider denial is terminal and cannot be retried by the runner.
    {
        let fixture = ContributionFixture::new();
        let enqueued = enqueue(&fixture, "permanent");
        let provider = RecordingProvider::new([ProviderStep::Permanent]);
        let (_scanner_file, scanner) = test_scanner();
        assert_eq!(
            run(&fixture, &provider, &scanner, 60.0).expect("terminal provider failure"),
            ContributionRunOnceReport::Completed {
                execution_id: enqueued.execution_id,
                state: ContributionExecutionState::FailedTerminal,
            }
        );
        assert_eq!(provider.calls(), 1);
        assert_eq!(
            state_and_job(enqueued.execution_id.as_uuid(), enqueued.job_id),
            (
                "FAILED_TERMINAL".into(),
                "FAILED".into(),
                Some("PROVIDER_PERMANENT".into())
            )
        );
        assert_eq!(candidate_count(enqueued.execution_id.as_uuid()), 0);
        record_fault_evidence(
            &mut observed_fault_keys,
            "R4-FG-18",
            "PROVIDER_DEFINITE_FAILURE",
            "runner_live_late_and_outcomes",
            0,
            1,
            1,
        );
    }

    // Expiring the lease inside the sole provider call exercises the exact 55000/message late-A
    // retry. It records the response, stops before B, and never settles the expired job.
    {
        let fixture = ContributionFixture::new();
        let enqueued = enqueue(&fixture, "late-a");
        let provider = RecordingProvider::new([ProviderStep::SleepThenJson(
            Duration::from_millis(2_500),
            r#"{"probe":"general knowledge"}"#.into(),
        )]);
        let (_scanner_file, scanner) = test_scanner();
        assert_eq!(
            run(&fixture, &provider, &scanner, 2.0).expect("exact late A completion"),
            ContributionRunOnceReport::LateCompleted {
                execution_id: enqueued.execution_id,
                state: ContributionExecutionState::ReadyB,
            }
        );
        assert_eq!(provider.calls(), 1, "late A must stop before B");
        assert_eq!(
            state_and_job(enqueued.execution_id.as_uuid(), enqueued.job_id),
            ("READY_B".into(), "PROCESSING".into(), None)
        );
        record_fault_evidence(
            &mut observed_fault_keys,
            "R4-FG-13",
            "BASE",
            "runner_live_late_and_outcomes",
            0,
            1,
            1,
        );
    }

    // R4-FG-05/BASE: A provider response exists, then scanner infrastructure disappears
    // before the A completion transaction. The durable reservation remains and a successor
    // cannot replay the provider call.
    {
        let mut fixture = ContributionFixture::new();
        let enqueued = enqueue(&fixture, "a-response-before-commit");
        let provider = RecordingProvider::new([ProviderStep::Json(
            r#"{"probe":"general knowledge"}"#.into(),
        )]);
        let (scanner_file, scanner) = test_scanner();
        fs::remove_file(&scanner_file.0).expect("inject scanner disappearance");
        assert_eq!(
            run(&fixture, &provider, &scanner, 60.0).expect("scanner infrastructure disposition"),
            ContributionRunOnceReport::ReconciliationRequired {
                execution_id: enqueued.execution_id,
                state: ContributionExecutionState::AReserved,
            }
        );
        assert_eq!(provider.calls(), 1);
        assert_eq!(
            state_and_job(enqueued.execution_id.as_uuid(), enqueued.job_id),
            (
                "A_RESERVED".into(),
                "FAILED".into(),
                Some("RECONCILIATION_REQUIRED".into())
            )
        );
        requeue_for_reconciliation(&mut fixture, enqueued.job_id);
        assert_eq!(
            run(&fixture, &provider, &scanner, 60.0).expect("A reserved successor"),
            ContributionRunOnceReport::ReconciliationRequired {
                execution_id: enqueued.execution_id,
                state: ContributionExecutionState::AReserved,
            }
        );
        assert_eq!(provider.calls(), 1, "A response must never be replayed");
        record_fault_evidence(
            &mut observed_fault_keys,
            "R4-FG-05",
            "BASE",
            "runner_a_response_before_commit_zero_redispatch",
            0,
            1,
            1,
        );
    }

    // R4-FG-09/BASE: A completes; after the B provider response, scanner infrastructure
    // becomes unknown. B stays durably reserved and the successor cannot redispatch it.
    {
        let mut fixture = ContributionFixture::new();
        let enqueued = enqueue(&fixture, "b-scanner-unknown");
        let (scanner_file, scanner) = test_scanner();
        let provider = RecordingProvider::new([
            ProviderStep::Json(r#"{"probe":"general knowledge"}"#.into()),
            ProviderStep::RemoveThenJson(
                scanner_file.0.clone(),
                r#"{"novelty":"PASS","quality":"PASS","generality":"PASS","grounding":"PASS","candidate":"generalized candidate"}"#.into(),
            ),
        ]);
        assert_eq!(
            run(&fixture, &provider, &scanner, 60.0).expect("B scanner unknown"),
            ContributionRunOnceReport::ReconciliationRequired {
                execution_id: enqueued.execution_id,
                state: ContributionExecutionState::BReserved,
            }
        );
        assert_eq!(provider.calls(), 2);
        assert_eq!(
            state_and_job(enqueued.execution_id.as_uuid(), enqueued.job_id),
            (
                "B_RESERVED".into(),
                "FAILED".into(),
                Some("RECONCILIATION_REQUIRED".into())
            )
        );
        requeue_for_reconciliation(&mut fixture, enqueued.job_id);
        assert_eq!(
            run(&fixture, &provider, &scanner, 60.0).expect("B reserved successor"),
            ContributionRunOnceReport::ReconciliationRequired {
                execution_id: enqueued.execution_id,
                state: ContributionExecutionState::BReserved,
            }
        );
        assert_eq!(
            provider.calls(),
            2,
            "B scanner unknown must never redispatch"
        );
        record_fault_evidence(
            &mut observed_fault_keys,
            "R4-FG-09",
            "BASE",
            "runner_b_scanner_unknown_zero_redispatch",
            0,
            2,
            2,
        );
    }

    // A closed payload parser rejects unknown fields before reasoner preparation or dispatch.
    {
        let mut fixture = ContributionFixture::new();
        let enqueued = enqueue(&fixture, "malformed-payload");
        fixture
            .admin
            .execute(
                "UPDATE ops.jobs SET payload=payload||'{\"unexpected\":true}'::jsonb WHERE job_id=$1",
                &[&enqueued.job_id],
            )
            .expect("inject unknown payload field");
        let provider = RecordingProvider::new([]);
        let (_scanner_file, scanner) = test_scanner();
        assert!(matches!(
            run(&fixture, &provider, &scanner, 60.0),
            Err(ContributionExecutionRunnerError::InvalidClaim(_))
        ));
        assert_eq!(provider.calls(), 0);
    }

    // The preceding live-DONE, definite-failure, and exact-late branches jointly exercise
    // R4-FG-17's runner half; SQL supplies the atomic row-set proof.
    record_fault_evidence(
        &mut observed_fault_keys,
        "R4-FG-17",
        "BASE",
        "runner_live_late_and_outcomes",
        0,
        2,
        2,
    );
    let expected_fault_keys = [
        "R4-FG-04/BASE",
        "R4-FG-05/BASE",
        "R4-FG-08/BASE",
        "R4-FG-09/BASE",
        "R4-FG-13/BASE",
        "R4-FG-15/BASE",
        "R4-FG-17/BASE",
        "R4-FG-18/B_GATE_FAIL",
        "R4-FG-18/SCAN_REJECT",
        "R4-FG-18/PROVIDER_DEFINITE_FAILURE",
        "R4-FG-18/TIMEOUT",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    assert_eq!(observed_fault_keys, expected_fault_keys);
}

#[test]
fn runner_config_fails_closed_without_database_or_provider() {
    assert_eq!(
        ContributionExecutionRunnerConfig {
            lease_owner: String::new(),
            lease_seconds: 30.0,
        }
        .validate(),
        Err(ErrorCode::InvalidInput)
    );
}
