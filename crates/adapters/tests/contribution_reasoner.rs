//! §12.1.1 contribution reasoner boundary. The PG cases use a recording provider: no user key
//! or external network is involved, but the real private pool and disclosure ledger are used.

#[path = "support/contribution_fixture.rs"]
mod contribution_fixture;

use std::{sync::Mutex, time::Duration};

use async_trait::async_trait;
use contribution_fixture::ContributionFixture as Fixture;
use humaux_adapters::{
    byok::{
        PrivateInferenceContext, ReasoningCapability, ReasoningProviderDescriptor,
        ReasoningProviderError, StructuredReasoningRequest, StructuredReasoningResponse,
        TokenUsage, UserReasoningProvider, VisionReasoningRequest, VisionReasoningResponse,
        structured_request_body,
    },
    contribution_entry_repo::ContributionEntryRepo,
    contribution_reasoner::{ContributionReasoner, ContributionReasonerConfig},
    disclosure::DeletionCapability,
};
use humaux_application::{
    consolidate::{
        ContributionReasoningCallKind, LogicalReasoningCallId, PrivateReasoningPort,
        PrivateReasoningPurpose, SealedPrivateReasoningRequest,
    },
    contribute::ContributionCandidatePort,
};
use humaux_domain::{egress::ProcessorId, error::ErrorCode};
use sha2::{Digest, Sha256};
use uuid::Uuid;

static SERIAL: Mutex<()> = Mutex::new(());

fn config(allowed_egress_processor_id: Uuid) -> ContributionReasonerConfig {
    ContributionReasonerConfig {
        allowed_egress_processor_id: ProcessorId(allowed_egress_processor_id),
        region: "test-region".into(),
        permit_ttl: Duration::from_secs(30),
        deletion_capability: DeletionCapability::Unknown,
        system_prompt: "Remove personal identifiers and return JSON.".into(),
        json_schema: r#"{"type":"object"}"#.into(),
        max_output_tokens: 128,
    }
}

fn fresh_request(fixture: &Fixture) -> humaux_application::contribute::PrepareContribution {
    let mut request = fixture.request();
    request.coverage_probe_call_id = LogicalReasoningCallId(Uuid::new_v4());
    request.assessment_call_id = LogicalReasoningCallId(Uuid::new_v4());
    request
}

fn coverage_probe_call(
    base: SealedPrivateReasoningRequest,
    request: &humaux_application::contribute::PrepareContribution,
) -> SealedPrivateReasoningRequest {
    base.with_contribution_attempt(
        request.coverage_probe_call_id,
        ContributionReasoningCallKind::CoverageProbe,
        None,
    )
}

fn reasoning_side_effect_counts(fixture: &mut Fixture) -> (i64, i64) {
    let row = fixture.admin.query_one(
        "SELECT (SELECT count(*) FROM ops.model_call_ledger WHERE tenant_id=$1 AND purpose='CONTRIBUTION_DEIDENTIFY'), \
                (SELECT count(*) FROM ops.data_disclosures WHERE tenant_id=$1 AND purpose='USER_REASONING')",
        &[&fixture.auth.tenant_id().0],
    ).expect("reasoning side-effect counts");
    (row.get(0), row.get(1))
}

fn reserve_without_dispatch(fixture: &mut Fixture, sealed: SealedPrivateReasoningRequest) -> Uuid {
    let attempt = sealed
        .contribution_attempt
        .expect("sealed contribution attempt");
    fixture
        .admin
        .query_one(
            "SELECT set_config('humaux.tenant_id',$1,false)",
            &[&fixture.auth.tenant_id().0.to_string()],
        )
        .expect("tenant GUC");
    let model_call_id: Uuid = fixture.admin.query_one(
        "INSERT INTO ops.model_call_ledger(request_id,tenant_id,purpose,provider,model,model_revision,call_kind,intent_sha256,reasoning_domain_id,binding_id,binding_version,route_policy_id,route_policy_version,profile_id,profile_version,provider_account_id,provider_endpoint_id,egress_processor_id,credential_ref,billing_account_id,billing_instrument_id,provider_health_observation_id,account_health_observation_id,billing_responsibility,admitted_at) \
         SELECT $3,admission.tenant_id,admission.purpose,admission.processor_id,admission.provider_model_id,admission.model_revision,$4,$5,admission.reasoning_domain_id,admission.binding_id,admission.binding_version,admission.route_policy_id,admission.route_policy_version,admission.profile_id,admission.profile_version,admission.provider_account_id,admission.provider_endpoint_id,admission.egress_processor_id,admission.credential_ref,admission.billing_account_id,admission.billing_instrument_id,admission.provider_health_observation_id,admission.account_health_observation_id,'USER',admission.admitted_at \
         FROM control.resolve_user_reasoning_admission($1,1,$2,'CONTRIBUTION_DEIDENTIFY') admission RETURNING model_call_id",
        &[&fixture.binding, &fixture.domain, &attempt.logical_call_id.0, &attempt.call_kind.as_db_str(), &attempt.intent_sha256.0.to_vec()],
    ).expect("crash-window reasoning reservation").get(0);
    fixture.admin.execute(
        "INSERT INTO ops.data_disclosures(grant_id,tenant_id,processor_id,region,data_class,purpose,payload_sha256,payload_bytes,model_call_id) VALUES($1,$2,$3,'test-region','PRIVATE','USER_REASONING',$4,0,$5)",
        &[&Uuid::new_v4(), &fixture.auth.tenant_id().0, &fixture.egress_processor, &vec![4_u8; 32], &model_call_id],
    ).expect("crash-window disclosure reservation");
    model_call_id
}

fn record_unavailable_health(fixture: &mut Fixture) {
    fixture
        .admin
        .query_one(
            "SELECT set_config('humaux.tenant_id',$1,false)",
            &[&fixture.auth.tenant_id().0.to_string()],
        )
        .expect("tenant GUC");
    fixture.admin.execute(
        "INSERT INTO ops.reasoning_provider_health_observations(tenant_id,processor_id,processor_model_id,provider_model_id,model_revision,provider_endpoint_id,endpoint_ref,region,service_tier,source_kind,reason_code,verdict,observed_at,valid_until) \
         SELECT admission.tenant_id,admission.processor_id,admission.processor_model_id,admission.provider_model_id,admission.model_revision,admission.provider_endpoint_id,admission.endpoint_ref,admission.region,admission.service_tier,'TEST','FORCED_UNAVAILABLE','UNAVAILABLE',clock_timestamp(),clock_timestamp()+interval '1 hour' \
         FROM control.resolve_user_reasoning_admission($1,1,$2,'CONTRIBUTION_DEIDENTIFY') admission",
        &[&fixture.binding, &fixture.domain],
    ).expect("new unavailable health observation");
}

struct RecordingProvider {
    descriptor: ReasoningProviderDescriptor,
    endpoint_ref: String,
    calls: Mutex<Vec<RecordedCall>>,
    fail: bool,
    finalize_disclosure_early: bool,
}

struct RecordedCall {
    trace_id: String,
    domain_id: Uuid,
    payload_sha256: [u8; 32],
    body_sha256: [u8; 32],
}

impl RecordingProvider {
    fn new(provider_id: &str, model_id: &str) -> Self {
        Self {
            descriptor: ReasoningProviderDescriptor {
                provider_id: provider_id.into(),
                model_id: model_id.into(),
                model_revision: None,
                capabilities: vec![
                    ReasoningCapability::Text,
                    ReasoningCapability::StructuredOutput,
                ],
                custom_endpoint: None,
            },
            endpoint_ref: "https://reasoning.invalid/v1/chat/completions".into(),
            calls: Mutex::new(Vec::new()),
            fail: false,
            finalize_disclosure_early: false,
        }
    }

    fn failing(provider_id: &str, model_id: &str) -> Self {
        Self {
            fail: true,
            ..Self::new(provider_id, model_id)
        }
    }

    fn with_finalize_fault(provider_id: &str, model_id: &str) -> Self {
        Self {
            finalize_disclosure_early: true,
            ..Self::new(provider_id, model_id)
        }
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
        let body = structured_request_body(&self.descriptor, &request);
        let body_sha256: [u8; 32] = Sha256::digest(&body).into();
        let payload_sha256 = context.egress_permit().payload_sha256();
        assert_eq!(
            payload_sha256, body_sha256,
            "permit must bind the exact provider wire body"
        );
        self.calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(RecordedCall {
                trace_id: context.trace_id().to_owned(),
                domain_id: context.reasoning_domain_id().0,
                payload_sha256,
                body_sha256,
            });
        if self.finalize_disclosure_early {
            let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG fixture");
            let tenant_id = context.tenant_id().0;
            let model_call_id = Uuid::parse_str(context.trace_id()).expect("model call trace");
            tokio::task::spawn_blocking(move || {
                let mut admin =
                    postgres::Client::connect(&dsn, postgres::NoTls).expect("fault-injection admin");
                admin
                    .query_one(
                        "SELECT set_config('humaux.tenant_id',$1,false)",
                        &[&tenant_id.to_string()],
                    )
                    .expect("fault tenant GUC");
                admin
                    .execute(
                        "UPDATE ops.data_disclosures SET finalized_at=clock_timestamp(),outcome='SUCCESS' WHERE tenant_id=$1 AND model_call_id=$2 AND finalized_at IS NULL",
                        &[&tenant_id, &model_call_id],
                    )
                    .expect("inject disclosure finalize race");
            })
            .await
            .expect("fault injection task");
        }
        if self.fail {
            return Err(ReasoningProviderError::ProviderPermanent {
                message: "recorded failure".into(),
            });
        }
        Ok(StructuredReasoningResponse {
            json: r#"{"text":"generalized"}"#.into(),
            usage: TokenUsage::default(),
        })
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

#[test]
fn incomplete_configuration_fails_before_private_read_or_egress() {
    let mut missing = config(Uuid::new_v4());
    missing.region.clear();
    assert_eq!(missing.validate(), Err(ErrorCode::InvalidInput));
    let mut zero_timeout = config(Uuid::new_v4());
    zero_timeout.permit_ttl = Duration::ZERO;
    assert_eq!(zero_timeout.validate(), Err(ErrorCode::InvalidInput));
}

#[test]
#[ignore = "requires isolated PostgreSQL"]
fn legacy_infer_without_canonical_coverage_is_rejected_without_side_effects() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = Fixture::new();
    let request = fresh_request(&fixture);
    let snapshot = fixture
        .rt
        .block_on(ContributionEntryRepo::new(&fixture.private).load_preparation(&request))
        .expect("fresh preparation");
    let before = reasoning_side_effect_counts(&mut fixture);
    let provider = RecordingProvider::new("offline-fixture", "offline-fixture");
    let reasoner = ContributionReasoner::new(
        &fixture.private,
        request,
        &provider,
        config(fixture.egress_processor),
    )
    .expect("reasoner");
    assert!(
        fixture
            .rt
            .block_on(reasoner.infer(snapshot.reasoning))
            .is_err()
    );
    assert_eq!(
        provider
            .calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len(),
        0
    );
    assert_eq!(reasoning_side_effect_counts(&mut fixture), before);
}

#[test]
#[ignore = "requires isolated PostgreSQL"]
fn nil_logical_call_is_rejected_without_side_effects() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = Fixture::new();
    let request = fresh_request(&fixture);
    let snapshot = fixture
        .rt
        .block_on(ContributionEntryRepo::new(&fixture.private).load_preparation(&request))
        .expect("fresh preparation");
    let sealed = snapshot.reasoning.with_contribution_attempt(
        LogicalReasoningCallId(Uuid::nil()),
        ContributionReasoningCallKind::CoverageProbe,
        None,
    );
    let before = reasoning_side_effect_counts(&mut fixture);
    let provider = RecordingProvider::new("offline-fixture", "offline-fixture");
    let reasoner = ContributionReasoner::new(
        &fixture.private,
        request,
        &provider,
        config(fixture.egress_processor),
    )
    .expect("reasoner");
    assert!(
        fixture
            .rt
            .block_on(reasoner.call_coverage_probe(sealed))
            .is_err()
    );
    assert_eq!(
        provider
            .calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len(),
        0
    );
    assert_eq!(reasoning_side_effect_counts(&mut fixture), before);
}

#[test]
#[ignore = "requires isolated PostgreSQL"]
fn same_logical_call_uuid_in_two_tenants_uses_distinct_advisory_locks() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut first = Fixture::new();
    let mut second = Fixture::new();
    let first_tenant = first.auth.tenant_id().0;
    let second_tenant = second.auth.tenant_id().0;
    let logical_call_id = Uuid::new_v4();
    let mut first_txn = first.admin.transaction().expect("first lock transaction");
    first_txn
        .query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended('model-call-request:' || $1::uuid::text || ':' || $2::uuid::text,0))",
            &[&first_tenant, &logical_call_id],
        )
        .expect("first tenant lock");
    let distinct_tenant_acquired: bool = second
        .admin
        .query_one(
            "SELECT pg_try_advisory_xact_lock(hashtextextended('model-call-request:' || $1::uuid::text || ':' || $2::uuid::text,0))",
            &[&second_tenant, &logical_call_id],
        )
        .expect("second tenant lock probe")
        .get(0);
    let same_tenant_acquired: bool = second
        .admin
        .query_one(
            "SELECT pg_try_advisory_xact_lock(hashtextextended('model-call-request:' || $1::uuid::text || ':' || $2::uuid::text,0))",
            &[&first_tenant, &logical_call_id],
        )
        .expect("same tenant lock probe")
        .get(0);
    assert!(distinct_tenant_acquired);
    assert!(!same_tenant_acquired);
    first_txn.rollback().expect("release first lock");
}

#[test]
#[ignore = "requires isolated PostgreSQL"]
fn endpoint_revision_descriptor_or_allowlist_mismatch_has_zero_side_effects() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = Fixture::new();
    let before = reasoning_side_effect_counts(&mut fixture);

    let mut endpoint_provider = RecordingProvider::new("offline-fixture", "offline-fixture");
    endpoint_provider.endpoint_ref.push('/');
    let request = fresh_request(&fixture);
    let snapshot = fixture
        .rt
        .block_on(ContributionEntryRepo::new(&fixture.private).load_preparation(&request))
        .expect("fresh preparation");
    let sealed = coverage_probe_call(snapshot.reasoning, &request);
    let reasoner = ContributionReasoner::new(
        &fixture.private,
        request,
        &endpoint_provider,
        config(fixture.egress_processor),
    )
    .expect("endpoint mismatch reasoner");
    assert!(
        fixture
            .rt
            .block_on(reasoner.call_coverage_probe(sealed))
            .is_err()
    );
    assert_eq!(
        endpoint_provider
            .calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len(),
        0
    );
    assert_eq!(reasoning_side_effect_counts(&mut fixture), before);

    let mut revision_provider = RecordingProvider::new("offline-fixture", "offline-fixture");
    revision_provider.descriptor.model_revision = Some("unexpected-revision".into());
    let request = fresh_request(&fixture);
    let snapshot = fixture
        .rt
        .block_on(ContributionEntryRepo::new(&fixture.private).load_preparation(&request))
        .expect("fresh preparation");
    let sealed = coverage_probe_call(snapshot.reasoning, &request);
    let reasoner = ContributionReasoner::new(
        &fixture.private,
        request,
        &revision_provider,
        config(fixture.egress_processor),
    )
    .expect("revision mismatch reasoner");
    assert!(
        fixture
            .rt
            .block_on(reasoner.call_coverage_probe(sealed))
            .is_err()
    );
    assert_eq!(
        revision_provider
            .calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len(),
        0
    );
    assert_eq!(reasoning_side_effect_counts(&mut fixture), before);

    let provider = RecordingProvider::new("offline-fixture", "offline-fixture");
    let request = fresh_request(&fixture);
    let snapshot = fixture
        .rt
        .block_on(ContributionEntryRepo::new(&fixture.private).load_preparation(&request))
        .expect("fresh preparation");
    let sealed = coverage_probe_call(snapshot.reasoning, &request);
    let reasoner =
        ContributionReasoner::new(&fixture.private, request, &provider, config(Uuid::new_v4()))
            .expect("allowlist mismatch reasoner");
    assert!(
        fixture
            .rt
            .block_on(reasoner.call_coverage_probe(sealed))
            .is_err()
    );
    assert_eq!(
        provider
            .calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len(),
        0
    );
    assert_eq!(reasoning_side_effect_counts(&mut fixture), before);
}

#[test]
#[ignore = "requires isolated PostgreSQL"]
fn wrong_binding_is_denied_before_ledger_disclosure_or_provider() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = Fixture::new();
    let before = reasoning_side_effect_counts(&mut fixture);
    let mut request = fresh_request(&fixture);
    request.binding_id = humaux_application::consolidate::ReasoningRouteBindingId(Uuid::new_v4());
    let result = fixture
        .rt
        .block_on(ContributionEntryRepo::new(&fixture.private).load_preparation(&request));
    assert!(result.is_err());
    assert_eq!(reasoning_side_effect_counts(&mut fixture), before);
}

#[test]
#[ignore = "requires isolated PostgreSQL"]
fn matching_reserved_retry_returns_original_model_call_without_dispatch_or_readmission() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = Fixture::new();
    let request = fresh_request(&fixture);
    let snapshot = fixture
        .rt
        .block_on(ContributionEntryRepo::new(&fixture.private).load_preparation(&request))
        .expect("fresh preparation");
    let sealed = coverage_probe_call(snapshot.reasoning, &request);
    let model_call_id = reserve_without_dispatch(&mut fixture, sealed);
    let before = reasoning_side_effect_counts(&mut fixture);
    record_unavailable_health(&mut fixture);
    let provider = RecordingProvider::new("offline-fixture", "offline-fixture");
    let reasoner = ContributionReasoner::new(
        &fixture.private,
        request.clone(),
        &provider,
        config(fixture.egress_processor),
    )
    .expect("reasoner");
    let retry = fixture
        .rt
        .block_on(reasoner.call_coverage_probe(sealed))
        .expect_err("open reservation is not dispatched again");
    assert_eq!(retry.existing_model_call_id(), Some(model_call_id));
    assert_eq!(
        provider
            .calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len(),
        0
    );
    assert_eq!(reasoning_side_effect_counts(&mut fixture), before);

    let mut new_request = request;
    new_request.coverage_probe_call_id = LogicalReasoningCallId(Uuid::new_v4());
    new_request.assessment_call_id = LogicalReasoningCallId(Uuid::new_v4());
    let new_sealed = coverage_probe_call(snapshot.reasoning, &new_request);
    let next = ContributionReasoner::new(
        &fixture.private,
        new_request,
        &provider,
        config(fixture.egress_processor),
    )
    .expect("new logical call");
    assert!(
        fixture
            .rt
            .block_on(next.call_coverage_probe(new_sealed))
            .is_err()
    );
    assert_eq!(
        provider
            .calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len(),
        0
    );
    assert_eq!(reasoning_side_effect_counts(&mut fixture), before);
}

#[test]
#[ignore = "requires isolated PostgreSQL"]
fn changed_intent_or_terminal_logical_call_conflicts_without_repeat_dispatch() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = Fixture::new();
    let request = fresh_request(&fixture);
    let snapshot = fixture
        .rt
        .block_on(ContributionEntryRepo::new(&fixture.private).load_preparation(&request))
        .expect("fresh preparation");
    let sealed = coverage_probe_call(snapshot.reasoning, &request);
    reserve_without_dispatch(&mut fixture, sealed);
    let mut changed_base = snapshot.reasoning;
    changed_base.input_manifest_hash.0[0] ^= 1;
    let changed = coverage_probe_call(changed_base, &request);
    let provider = RecordingProvider::new("offline-fixture", "offline-fixture");
    let reasoner = ContributionReasoner::new(
        &fixture.private,
        request,
        &provider,
        config(fixture.egress_processor),
    )
    .expect("reasoner");
    let conflict = fixture
        .rt
        .block_on(reasoner.call_coverage_probe(changed))
        .expect_err("same key with another intent conflicts");
    assert_eq!(conflict.existing_model_call_id(), None);
    assert_eq!(
        provider
            .calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len(),
        0
    );

    let terminal_request = fresh_request(&fixture);
    let terminal_sealed = coverage_probe_call(snapshot.reasoning, &terminal_request);
    let terminal_reasoner = ContributionReasoner::new(
        &fixture.private,
        terminal_request,
        &provider,
        config(fixture.egress_processor),
    )
    .expect("terminal reasoner");
    fixture
        .rt
        .block_on(terminal_reasoner.call_coverage_probe(terminal_sealed))
        .expect("first dispatch succeeds");
    assert_eq!(
        provider
            .calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len(),
        1
    );
    let terminal_retry = fixture
        .rt
        .block_on(terminal_reasoner.call_coverage_probe(terminal_sealed))
        .expect_err("terminal call never reopens");
    assert_eq!(terminal_retry.existing_model_call_id(), None);
    assert_eq!(
        provider
            .calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len(),
        1
    );
}

#[test]
#[ignore = "requires isolated PostgreSQL"]
fn concurrent_same_logical_call_serializes_to_one_reservation_and_dispatch() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let fixture = Fixture::new();
    let request = fresh_request(&fixture);
    let snapshot = fixture
        .rt
        .block_on(ContributionEntryRepo::new(&fixture.private).load_preparation(&request))
        .expect("fresh preparation");
    let sealed = coverage_probe_call(snapshot.reasoning, &request);
    let provider = RecordingProvider::new("offline-fixture", "offline-fixture");
    let first = ContributionReasoner::new(
        &fixture.private,
        request.clone(),
        &provider,
        config(fixture.egress_processor),
    )
    .expect("first reasoner");
    let second = ContributionReasoner::new(
        &fixture.private,
        request,
        &provider,
        config(fixture.egress_processor),
    )
    .expect("second reasoner");
    let (left, right) = fixture.rt.block_on(async {
        tokio::join!(
            first.call_coverage_probe(sealed),
            second.call_coverage_probe(sealed)
        )
    });
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    assert_eq!(
        provider
            .calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len(),
        1
    );
}

#[test]
#[ignore = "requires isolated PostgreSQL"]
fn records_exact_wire_hash_and_finalizes_the_reserved_disclosure() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = Fixture::new();
    let request = fresh_request(&fixture);
    let snapshot = fixture
        .rt
        .block_on(ContributionEntryRepo::new(&fixture.private).load_preparation(&request))
        .expect("fresh sealed contribution input");
    assert_eq!(
        snapshot.reasoning.purpose,
        PrivateReasoningPurpose::ContributionDeidentify
    );
    let sealed = coverage_probe_call(snapshot.reasoning, &request);
    let provider = RecordingProvider::new("offline-fixture", "offline-fixture");
    let reasoner = ContributionReasoner::new(
        &fixture.private,
        request,
        &provider,
        config(fixture.egress_processor),
    )
    .expect("complete bridge config");
    let result = fixture
        .rt
        .block_on(reasoner.call_coverage_probe(sealed))
        .expect("recorded inference");
    assert_eq!(result.output_bytes, br#"{"text":"generalized"}"#);
    assert!(!result.provider_trace.0.is_empty());
    let calls = provider.calls.lock().unwrap_or_else(|e| e.into_inner());
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].domain_id, fixture.domain);
    assert!(!calls[0].trace_id.is_empty());
    assert_eq!(calls[0].payload_sha256, calls[0].body_sha256);
    let row = fixture.admin.query_one(
        "SELECT d.payload_sha256,d.finalized_at IS NOT NULL,d.outcome,\
                EXISTS(SELECT 1 FROM ops.data_disclosure_sources s WHERE s.disclosure_id=d.disclosure_id AND s.source_kind='MEMORY' AND s.memory_id=$2) \
         FROM ops.data_disclosures d WHERE d.tenant_id=$1 ORDER BY d.reserved_at DESC LIMIT 1",
        &[&fixture.auth.tenant_id().0, &fixture.memory],
    ).expect("ledger readback");
    let ledger_hash: Vec<u8> = row.get(0);
    assert_eq!(ledger_hash.as_slice(), calls[0].body_sha256);
    assert!(
        row.get::<_, bool>(1),
        "provider return is followed by finalization"
    );
    assert_eq!(row.get::<_, String>(2), "SUCCESS");
    assert!(
        row.get::<_, bool>(3),
        "reservation binds the trusted memory source"
    );
}

#[test]
#[ignore = "requires isolated PostgreSQL"]
fn legacy_profile_change_cannot_replace_exact_binding_admission() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = Fixture::new();
    let credential: Uuid = fixture.admin.query_one(
        "INSERT INTO control.credentials(tenant_id,purpose,openbao_ref) VALUES($1,'USER_REASONING','other-fixture-ref') RETURNING credential_id",
        &[&fixture.auth.tenant_id().0],
    ).unwrap().get(0);
    let other_profile: Uuid = fixture.admin.query_one(
        "INSERT INTO control.user_reasoning_profiles(tenant_id,user_id,provider_id,model_id,credential_ref,capabilities,profile_version) \
         VALUES($1,$2,'domain-bound-provider','domain-bound-model',$3,ARRAY['TEXT','STRUCTURED_OUTPUT'],1) RETURNING profile_id",
        &[&fixture.auth.tenant_id().0, &fixture.auth.user_id().expect("fixture user").0, &credential],
    ).unwrap().get(0);
    fixture.admin.execute(
        "UPDATE control.private_reasoning_domains SET user_reasoning_profile_id=$2 WHERE reasoning_domain_id=$1",
        &[&fixture.domain, &other_profile],
    ).unwrap();
    let request = fresh_request(&fixture);
    let snapshot = fixture
        .rt
        .block_on(ContributionEntryRepo::new(&fixture.private).load_preparation(&request))
        .unwrap();
    let sealed = coverage_probe_call(snapshot.reasoning, &request);
    let provider = RecordingProvider::new("offline-fixture", "offline-fixture");
    let reasoner = ContributionReasoner::new(
        &fixture.private,
        request,
        &provider,
        config(fixture.egress_processor),
    )
    .unwrap();
    fixture
        .rt
        .block_on(reasoner.call_coverage_probe(sealed))
        .expect("legacy profile cannot replace the exact Binding admission");
    assert_eq!(
        provider
            .calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len(),
        1
    );
}

#[test]
#[ignore = "requires isolated PostgreSQL"]
fn provider_failure_finalizes_the_already_reserved_disclosure_as_failed() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = Fixture::new();
    let request = fresh_request(&fixture);
    let snapshot = fixture
        .rt
        .block_on(ContributionEntryRepo::new(&fixture.private).load_preparation(&request))
        .unwrap();
    let sealed = coverage_probe_call(snapshot.reasoning, &request);
    let provider = RecordingProvider::failing("offline-fixture", "offline-fixture");
    let reasoner = ContributionReasoner::new(
        &fixture.private,
        request,
        &provider,
        config(fixture.egress_processor),
    )
    .unwrap();
    assert!(
        fixture
            .rt
            .block_on(reasoner.call_coverage_probe(sealed))
            .is_err()
    );
    let row = fixture.admin.query_one(
        "SELECT finalized_at IS NOT NULL,outcome FROM ops.data_disclosures WHERE tenant_id=$1 ORDER BY reserved_at DESC LIMIT 1",
        &[&fixture.auth.tenant_id().0],
    ).unwrap();
    assert!(row.get::<_, bool>(0));
    assert_eq!(row.get::<_, String>(1), "FAILED");
}

#[test]
#[ignore = "requires isolated PostgreSQL"]
fn finalize_failure_never_retries_provider_and_leaves_reserved_call_for_reconciliation() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = Fixture::new();
    let request = fresh_request(&fixture);
    let snapshot = fixture
        .rt
        .block_on(ContributionEntryRepo::new(&fixture.private).load_preparation(&request))
        .expect("fresh preparation");
    let sealed = coverage_probe_call(snapshot.reasoning, &request);
    let provider = RecordingProvider::with_finalize_fault("offline-fixture", "offline-fixture");
    let reasoner = ContributionReasoner::new(
        &fixture.private,
        request,
        &provider,
        config(fixture.egress_processor),
    )
    .expect("reasoner");
    assert!(
        fixture
            .rt
            .block_on(reasoner.call_coverage_probe(sealed))
            .is_err()
    );
    assert_eq!(
        provider
            .calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len(),
        1
    );
    assert!(
        fixture
            .rt
            .block_on(reasoner.call_coverage_probe(sealed))
            .is_err()
    );
    assert_eq!(
        provider
            .calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len(),
        1
    );
    let status: String = fixture
        .admin
        .query_one(
            "SELECT status FROM ops.model_call_ledger WHERE tenant_id=$1 AND request_id=$2",
            &[
                &fixture.auth.tenant_id().0,
                &sealed
                    .contribution_attempt
                    .expect("attempt")
                    .logical_call_id
                    .0,
            ],
        )
        .expect("reserved call readback")
        .get(0);
    assert_eq!(status, "RESERVED");
}
