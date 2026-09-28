//! `adapters::tests::phase9_exact_assessed_storage_binding` — Real-PostgreSQL acceptance for migration 0126's exact
//!   assessed storage chain.
//! Depends-on: crates=[async-trait, humaux-adapters, humaux-application, humaux-domain, postgres, sha2, uuid];
//!   services=[PostgreSQL(role_private_worker) r=[ops.commit_seq_seq] w=[control.anonymous_source_lineage,
//!   ops.outbox, ops.public_anonymous_dispatches, staging.contribution_candidate_phase9_assessments,
//!   staging.contribution_candidates, staging.contribution_releases, staging.sanitized_public_candidates]
//!   x=[public.phase9_public_coverage_for_probe, staging.assert_phase9_exact_assessed_release]]; env=[];
//!   modules=[adapters::contribution_entry_repo, adapters::tests::support::contribution_fixture,
//!   application::consolidate, application::contribute, domain::error, domain::evidence]
//! Called-by: [cargo-test]
//! Invariants: [omissions, mismatches and rebinding of the assessed storage chain are rejected; R3 authenticates a
//!   field-identical chain separately; the tests are #[ignore] lane tests]
//! Spec: Baseline §12; §79.2
//!
//! It rejects omissions, mismatches and rebinding; R3 separately authenticates a field-identical
//! chain against the frozen reasoning route and ModelCallLedger receipt.

#[path = "support/contribution_fixture.rs"]
mod contribution_fixture;

use std::sync::Mutex;

use async_trait::async_trait;
use contribution_fixture::ContributionFixture;
use humaux_adapters::contribution_entry_repo::ContributionEntryRepo;
use humaux_application::{
    consolidate::{ContentSha256, PrivateReasoningResult, ProviderTraceRef},
    contribute::{
        self, ContributionAssessment, ContributionAssessmentRequest, ContributionCoverageProbe,
        ContributionCoverageProbeRequest, ContributionGate, PublicCoverageDigest,
        PublicCoveragePort, UserContributionAssessmentPort,
    },
};
use humaux_domain::{error::ErrorCode, evidence::payload_sha256};
use postgres::{Transaction, error::SqlState};
use sha2::{Digest, Sha256};
use uuid::Uuid;

static SERIAL: Mutex<()> = Mutex::new(());

#[derive(Clone)]
struct AssessmentReasoner(Vec<u8>);

#[async_trait]
impl UserContributionAssessmentPort for AssessmentReasoner {
    async fn derive_coverage_probe(
        &self,
        request: ContributionCoverageProbeRequest,
    ) -> Result<ContributionCoverageProbe, ErrorCode> {
        let probe = b"phase9 exact assessed storage binding".to_vec();
        ContributionCoverageProbe::new(
            request.reasoning.input_manifest_hash,
            probe.clone(),
            payload_sha256(&probe),
            ProviderTraceRef("offline-exact-binding-probe".into()),
        )
    }

    async fn assess(
        &self,
        request: ContributionAssessmentRequest<'_>,
    ) -> Result<ContributionAssessment, ErrorCode> {
        let model_call_id = request
            .reasoning
            .contribution_attempt
            .expect("assessed request carries its logical receipt")
            .logical_call_id
            .0;
        Ok(ContributionAssessment {
            coverage_probe_sha256: request.coverage_probe.output_sha256(),
            public_coverage_binding: request.public_coverage.binding(),
            novelty: ContributionGate::Pass,
            quality: ContributionGate::Pass,
            generality: ContributionGate::Pass,
            grounding: ContributionGate::Pass,
            deidentified_candidate: PrivateReasoningResult {
                output_sha256: ContentSha256(Sha256::digest(&self.0).into()),
                output_bytes: self.0.clone(),
                provider_trace: ProviderTraceRef(model_call_id.to_string()),
                model_call_id,
                binding_id: request.reasoning.binding_id,
                binding_version: request.reasoning.binding_version,
            },
        })
    }
}

struct Coverage(PublicCoverageDigest);

#[async_trait]
impl PublicCoveragePort for Coverage {
    async fn load_public_coverage(
        &self,
        _probe: &ContributionCoverageProbe,
    ) -> Result<PublicCoverageDigest, ErrorCode> {
        Ok(self.0.clone())
    }
}

#[derive(Clone)]
struct Binding {
    release_id: Uuid,
    candidate_id: Uuid,
    source_id: Uuid,
    envelope: Vec<u8>,
    outbox_id: Uuid,
}

fn coverage(fixture: &mut ContributionFixture) -> PublicCoverageDigest {
    let rows = fixture
        .admin
        .query(
            "SELECT snapshot_id,coverage_version,summary \
             FROM public.phase9_public_coverage_for_probe($1,32)",
            &[&b"phase9 exact assessed storage binding".as_slice()],
        )
        .expect("bounded coverage");
    let first = rows.first().expect("coverage binding row");
    PublicCoverageDigest::new(
        first.get("snapshot_id"),
        u32::try_from(first.get::<_, i32>("coverage_version")).expect("positive version"),
        rows.iter()
            .filter_map(|row| row.get::<_, Option<String>>("summary"))
            .collect(),
    )
    .expect("coverage digest")
}

fn assessed_release(fixture: &mut ContributionFixture, text: &str) -> Binding {
    let current_coverage = coverage(fixture);
    let candidate = fixture
        .rt
        .block_on(contribute::prepare_assessed(
            fixture.request(),
            &AssessmentReasoner(text.as_bytes().to_vec()),
            &Coverage(current_coverage),
            &fixture.scanner(),
            &ContributionEntryRepo::new(&fixture.private),
        ))
        .expect("assessed candidate");
    let confirmation = fixture.confirm(candidate);
    let release_id = fixture
        .rt
        .block_on(contribute::finalize(
            confirmation,
            &ContributionEntryRepo::new(&fixture.private),
        ))
        .expect("exact assessed finalize")
        .0;
    let row = fixture
        .admin
        .query_one(
            "SELECT r.candidate_id,l.anonymous_source_id,s.envelope_sha256,o.outbox_id \
             FROM staging.contribution_releases r \
             JOIN control.anonymous_source_lineage l \
               ON l.tenant_id=r.tenant_id AND l.contribution_release_id=r.contribution_release_id \
             JOIN staging.sanitized_public_candidates s \
               ON s.tenant_id=l.tenant_id AND s.anonymous_source_id=l.anonymous_source_id \
             JOIN ops.outbox o ON o.tenant_id=l.tenant_id \
               AND o.anonymous_source_id=l.anonymous_source_id \
               AND o.candidate_envelope_sha256=s.envelope_sha256 \
               AND o.event_type='PUBLIC_ANONYMOUS_RELEASE' AND o.anonymous_source_revision=1 \
             WHERE r.contribution_release_id=$1",
            &[&release_id],
        )
        .expect("complete exact storage chain");
    Binding {
        release_id,
        candidate_id: row.get(0),
        source_id: row.get(1),
        envelope: row.get(2),
        outbox_id: row.get(3),
    }
}

fn expect_commit_check_failure(transaction: Transaction<'_>, context: &str) {
    let error = transaction.commit().expect_err(context);
    assert_eq!(
        error.code(),
        Some(&SqlState::CHECK_VIOLATION),
        "{context}: {error}"
    );
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL migrated through 0126 and pinned Gitleaks"]
fn legal_assessed_finalize_and_legacy_expand_both_commit_with_acl_boundary() {
    let _serial = SERIAL.lock().expect("serial fixture");
    let mut fixture = ContributionFixture::new();
    let binding = assessed_release(&mut fixture, "Public safe exact assessed payload.");
    let legacy_release = fixture.finalize_release();

    let row = fixture
        .admin
        .query_one(
            "SELECT r.policy_snapshot->>'principal_id', \
              (SELECT count(*) FROM ops.outbox WHERE contribution_release_id=$2 \
                AND event_type='PUBLIC_RELEASE'), \
              has_function_privilege('role_private_worker', \
                'staging.assert_phase9_exact_assessed_release(uuid)','EXECUTE'), \
              has_function_privilege('role_public_worker', \
                'staging.assert_phase9_exact_assessed_release(uuid)','EXECUTE'), \
              has_table_privilege('role_public_worker','control.anonymous_source_lineage','SELECT'), \
              has_table_privilege('role_public_worker','staging.contribution_releases','SELECT'), \
              has_table_privilege('role_public_worker','ops.outbox','SELECT') \
             FROM staging.contribution_releases r WHERE r.contribution_release_id=$1",
            &[&binding.release_id, &legacy_release],
        )
        .expect("positive and ACL gates");
    assert_eq!(
        row.get::<_, String>(0),
        fixture.auth.principal().0.to_string(),
        "0124 must continue reading principal only from the protected policy snapshot"
    );
    assert_eq!(row.get::<_, i64>(1), 1, "legacy expand path remains live");
    for index in 2..7 {
        assert!(!row.get::<_, bool>(index), "ACL negative gate {index}");
    }
    assert_eq!(binding.envelope.len(), 32);
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL migrated through 0126 and pinned Gitleaks"]
#[allow(clippy::too_many_lines)] // Each mutation is one required binding-negative case.
fn every_cross_row_mismatch_is_rejected() {
    let _serial = SERIAL.lock().expect("serial fixture");
    let mut fixture = ContributionFixture::new();

    let legacy_candidate = fixture.prepare();
    let mut transaction = fixture.admin.transaction().expect("manual assessment tx");
    transaction
        .query_one(
            "SELECT set_config('humaux.tenant_id',$1,true),set_config('humaux.user_id',$2,true)",
            &[
                &fixture.auth.tenant_id().0.to_string(),
                &fixture.auth.user_id().expect("fixture user").0.to_string(),
            ],
        )
        .expect("private tenant and user scope");
    // dep: PostgreSQL(role_private_worker) — role switch before the scoped statements for `every_cross_row_mismatch_is_rejected`
    transaction
        .batch_execute("SET LOCAL ROLE role_private_worker")
        .expect("private worker hand-assembly role");
    transaction
        .execute(
            "WITH candidate AS ( \
               SELECT *,convert_to('manual probe','UTF8') AS probe,uuidv4() AS coverage_id, \
                 sha256(convert_to('manual coverage','UTF8')) AS coverage_sha \
               FROM staging.contribution_candidates WHERE candidate_id=$1 \
             ), receipt AS ( \
               SELECT candidate.*,jsonb_build_object( \
                 'probe_sha256',encode(sha256(probe),'hex'), \
                 'coverage_digest_id',coverage_id::text,'coverage_version','1', \
                 'coverage_digest_sha256',encode(coverage_sha,'hex'), \
                 'candidate_payload_sha256',encode(disclosed_payload_sha256,'hex'), \
                 'novelty','PASS','quality','PASS','generality','PASS','grounding','PASS') AS body \
               FROM candidate \
             ) \
             INSERT INTO staging.contribution_candidate_phase9_assessments( \
               candidate_id,tenant_id,probe_bytes,probe_sha256,probe_source_manifest_hash, \
               probe_provider_trace,probe_scan_receipt,coverage_digest_id,coverage_version, \
               coverage_digest_sha256,candidate_payload_sha256,assessment_digest, \
               assessment_provider_trace,assessment_receipt) \
             SELECT candidate_id,tenant_id,probe,sha256(probe),source_manifest_hash, \
               'manual-probe','{}'::jsonb,coverage_id,1,coverage_sha,disclosed_payload_sha256, \
               sha256(convert_to(body::text,'UTF8')),'manual-forged-output',body FROM receipt",
            &[&legacy_candidate.0],
        )
        .expect("locally valid hand-assembled PASSED assessment");
    let error = transaction
        .commit()
        .expect_err("private worker cannot mismatch a hand-assembled assessment provider");
    assert_eq!(error.code(), Some(&SqlState::FOREIGN_KEY_VIOLATION));

    let first = assessed_release(&mut fixture, "First exact assessed payload.");

    let mut transaction = fixture.admin.transaction().expect("candidate mismatch tx");
    transaction
        .execute(
            "UPDATE staging.contribution_candidates \
             SET policy_snapshot=jsonb_set(policy_snapshot,'{principal_id}',to_jsonb($2::text)) \
             WHERE candidate_id=$1",
            &[&first.candidate_id, &Uuid::new_v4().to_string()],
        )
        .expect("locally valid candidate policy snapshot");
    expect_commit_check_failure(
        transaction,
        "candidate-to-release policy snapshot mismatch must fail at commit",
    );

    let mut transaction = fixture.admin.transaction().expect("assessment mismatch tx");
    transaction
        .execute(
            "UPDATE staging.contribution_candidate_phase9_assessments \
             SET assessment_provider_trace='forged-provider-trace' WHERE candidate_id=$1",
            &[&first.candidate_id],
        )
        .expect("deferred assessment mismatch");
    let error = transaction
        .commit()
        .expect_err("assessment-to-candidate provider trace mismatch must fail at commit");
    assert_eq!(error.code(), Some(&SqlState::FOREIGN_KEY_VIOLATION));

    let mut transaction = fixture.admin.transaction().expect("release mismatch tx");
    let error = transaction
        .execute(
            "UPDATE staging.contribution_releases \
             SET disclosed_payload=convert_to('forged release bytes','UTF8'), \
                 disclosed_payload_sha256=sha256(convert_to('forged release bytes','UTF8')) \
             WHERE contribution_release_id=$1",
            &[&first.release_id],
        )
        .expect_err("release identity and snapshot must be immutable");
    assert_eq!(error.code(), Some(&SqlState::CHECK_VIOLATION));
    drop(transaction);

    let legacy_release = fixture.finalize_release();
    let mut transaction = fixture.admin.transaction().expect("lineage mismatch tx");
    transaction
        .execute(
            "UPDATE control.anonymous_source_lineage SET contribution_release_id=$2 \
             WHERE contribution_release_id=$1",
            &[&first.release_id, &legacy_release],
        )
        .expect("existing FKs permit the cross-release rebind until the exact gate");
    expect_commit_check_failure(
        transaction,
        "release-to-lineage mismatch must fail at commit",
    );

    let mut transaction = fixture.admin.transaction().expect("envelope mismatch tx");
    transaction
        .execute(
            "UPDATE staging.sanitized_public_candidates \
             SET assessment_digest=decode(repeat('ab',32),'hex'), \
                 envelope_sha256=sha256(content_sha256||policy_digest|| \
                   decode(repeat('ab',32),'hex')||convert_to(policy_version||':PASSED','UTF8')) \
             WHERE anonymous_source_id=$1 AND envelope_sha256=$2",
            &[&first.source_id, &first.envelope],
        )
        .expect("locally self-consistent forged envelope");
    expect_commit_check_failure(
        transaction,
        "sealed-envelope-to-assessment mismatch must fail at commit",
    );

    let second = assessed_release(&mut fixture, "Second exact assessed payload.");
    let mut transaction = fixture.admin.transaction().expect("outbox swap tx");
    transaction
        .execute(
            "DELETE FROM ops.public_anonymous_dispatches WHERE outbox_event_id IN ($1,$2)",
            &[&first.outbox_id, &second.outbox_id],
        )
        .expect("remove derived dispatch rows inside rejected transaction");
    transaction
        .execute(
            "DELETE FROM ops.outbox WHERE outbox_id IN ($1,$2)",
            &[&first.outbox_id, &second.outbox_id],
        )
        .expect("remove original authorities inside rejected transaction");
    transaction
        .execute(
            "INSERT INTO ops.outbox(tenant_id,commit_seq,event_type,anonymous_source_id, \
               candidate_envelope_sha256,anonymous_source_revision) \
             SELECT tenant_id,nextval('ops.commit_seq_seq'),'PUBLIC_ANONYMOUS_RELEASE',$2,$3,1 \
             FROM staging.contribution_releases WHERE contribution_release_id=$1",
            &[&first.release_id, &second.source_id, &second.envelope],
        )
        .expect("a valid opaque pair cannot stand in for a different release");
    expect_commit_check_failure(
        transaction,
        "anonymous outbox must bind its exact release, source, envelope and revision",
    );
}
