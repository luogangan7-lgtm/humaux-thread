//! `adapters::tests::support::public_anonymous_seam` — Shared helpers for the Phase 9 **anonymous** public seam:
//!   assessed prepare -> confirm -> finalize -> `run_anonymous_once` admission -> `control.anonymous_source_lineage`
//!   read-back -> `evaluate_anonymous_claim`.
//! Depends-on: crates=[async-trait, humaux-adapters, humaux-application, humaux-domain, postgres, sha2, uuid];
//!   services=[PostgreSQL(owner) r=[control.anonymous_source_lineage, public.claims, public.provenance_edges,
//!   public.sources] w=[control.public_moderator_grants, ops.jobs] x=[public.phase9_public_coverage_for_probe]];
//!   env=[HUMAUX_TEST_PG_DSN]; modules=[adapters::contribution_entry_repo, adapters::postgres, adapters::public_repo,
//!   adapters::tests::support::contribution_fixture, application::consolidate, application::contribute,
//!   domain::error, domain::evidence, domain::public]
//! Called-by: [adapters::tests::public_runtime, adapters::tests::public_runtime_qdrant]
//! Invariants: [the only live public-tier seam since 0124 revoked role_public_worker's read of
//!   staging.contribution_releases; admit_release must never be reopened for that role (ADR-0047); callers declare
//!   the fixture module themselves]
//! Spec: Baseline §13; ADR-0047
//!
//! Migration 0124 (`0124_phase9_independence_attestation:233`) REVOKEd
//! `SELECT ON staging.contribution_releases` from `role_public_worker` on purpose: the anonymous
//! role must never link a claim to a release or contributor. Every oracle that used to reach the
//! public tier through `public_repo::admit_release` is therefore standing on the wrong side of a
//! deliberate fence, and the only live seam is the one in this module. Re-opening `admit_release`
//! for `role_public_worker` is forbidden — the 0165 attempt that returned `candidate_id` /
//! `confirmation_id` / `disclosed_payload` to that role was reverted as a lineage leak
//! (ADR-0047).
//!
//! Callers must also declare the fixture module at their test-crate root:
//! `#[path = "support/contribution_fixture.rs"] mod contribution_fixture;`.
#![allow(dead_code)]

use async_trait::async_trait;
use humaux_adapters::{postgres::PublicWorkerDbPool, public_repo};
use humaux_application::{
    consolidate::ProviderTraceRef,
    contribute::{
        self, ContributionAssessment, ContributionAssessmentRequest, ContributionCoverageProbe,
        ContributionCoverageProbeRequest, ContributionGate, PublicCoverageDigest,
        PublicCoveragePort, UserContributionAssessmentPort,
    },
};
use humaux_domain::evidence::payload_sha256;
use humaux_domain::public::ModerationState;
use postgres::{Client, NoTls};
use sha2::Digest;
use uuid::Uuid;

use crate::contribution_fixture::ContributionFixture;

/// Appends a `SET role` to a DSN so a test can hold a pool with exactly one role's rights.
pub fn dsn_as_role(dsn: &str, role: &str) -> String {
    let sep = if dsn.contains('?') { '&' } else { '?' };
    format!("{dsn}{sep}options=-c%20role%3D{role}")
}

/// Drives the dispatch state machine without projecting anywhere.
///
/// `run_anonymous_once` advances admission, revoke and projection dispatches in one call; a test
/// that only needs the PostgreSQL half (or needs to drain another fixture's residue out of the
/// global queue) passes this so no external projection is written.
pub struct NoopProjector;

#[async_trait]
impl public_repo::PublicProjectionPort for NoopProjector {
    async fn project_live(
        &self,
        _: &public_repo::EligibleObject,
    ) -> Result<public_repo::ProjectionWriteOutcome, humaux_domain::error::ErrorCode> {
        Ok(public_repo::ProjectionWriteOutcome::Applied)
    }

    async fn retire(
        &self,
        _: &public_repo::ProjectionIdentity,
    ) -> Result<(), humaux_domain::error::ErrorCode> {
        Ok(())
    }
}

struct OfflineAssessmentReasoner;

#[async_trait]
impl UserContributionAssessmentPort for OfflineAssessmentReasoner {
    async fn derive_coverage_probe(
        &self,
        request: ContributionCoverageProbeRequest,
    ) -> Result<ContributionCoverageProbe, humaux_domain::error::ErrorCode> {
        let probe = b"assessed public".to_vec();
        ContributionCoverageProbe::new(
            request.reasoning.input_manifest_hash,
            probe.clone(),
            payload_sha256(&probe),
            ProviderTraceRef("offline-assessed-probe".into()),
        )
    }

    async fn assess(
        &self,
        request: ContributionAssessmentRequest<'_>,
    ) -> Result<ContributionAssessment, humaux_domain::error::ErrorCode> {
        let candidate = b"Assessed public knowledge without identifying details.".to_vec();
        let model_call_id = request
            .reasoning
            .contribution_attempt
            .expect("sealed assessment attempt")
            .logical_call_id
            .0;
        Ok(ContributionAssessment {
            coverage_probe_sha256: request.coverage_probe.output_sha256(),
            public_coverage_binding: request.public_coverage.binding(),
            novelty: ContributionGate::Pass,
            quality: ContributionGate::Pass,
            generality: ContributionGate::Pass,
            grounding: ContributionGate::Pass,
            deidentified_candidate: humaux_application::consolidate::PrivateReasoningResult {
                output_sha256: humaux_application::consolidate::ContentSha256(
                    sha2::Sha256::digest(&candidate).into(),
                ),
                output_bytes: candidate,
                provider_trace: ProviderTraceRef(model_call_id.to_string()),
                model_call_id,
                binding_id: request.reasoning.binding_id,
                binding_version: request.reasoning.binding_version,
            },
        })
    }
}

struct OfflineCoveragePort(PublicCoverageDigest);

#[async_trait]
impl PublicCoveragePort for OfflineCoveragePort {
    async fn load_public_coverage(
        &self,
        _probe: &ContributionCoverageProbe,
    ) -> Result<PublicCoverageDigest, humaux_domain::error::ErrorCode> {
        Ok(self.0.clone())
    }
}

/// Reads the canonical current public coverage for a probe straight from the database, so the
/// offline port returns the same digest `contribute::finalize` will re-check.
pub fn coverage_for_probe(probe: &[u8]) -> PublicCoverageDigest {
    // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
    let mut reader = Client::connect(
        &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG"),
        NoTls,
    )
    .expect("isolated coverage reader");
    let rows = reader
        .query(
            "SELECT snapshot_id,coverage_version,summary \
             FROM public.phase9_public_coverage_for_probe($1,32)",
            &[&probe],
        )
        .expect("private coverage contract");
    let first = rows.first().expect("stable coverage row");
    let snapshot_id: Uuid = first.get("snapshot_id");
    let version: i32 = first.get("coverage_version");
    let summaries = rows
        .iter()
        .filter_map(|row| row.get::<_, Option<String>>("summary"))
        .collect();
    PublicCoverageDigest::new(
        snapshot_id,
        u32::try_from(version).expect("positive version"),
        summaries,
    )
    .expect("canonical current coverage")
}

pub fn prepare_assessed_candidate(
    fixture: &ContributionFixture,
) -> humaux_application::contribute::ContributionCandidateId {
    fixture
        .rt
        .block_on(contribute::prepare_assessed(
            fixture.request(),
            &OfflineAssessmentReasoner,
            &OfflineCoveragePort(coverage_for_probe(b"assessed public")),
            &fixture.scanner(),
            &humaux_adapters::contribution_entry_repo::ContributionEntryRepo::new(&fixture.private),
        ))
        .expect("assessed prepare")
}

/// Runs the authenticated assessed entry flow to a stored release id.
pub fn finalize_assessed_release(fixture: &ContributionFixture) -> Uuid {
    let candidate = prepare_assessed_candidate(fixture);
    let confirmation = fixture.confirm(candidate);
    fixture
        .rt
        .block_on(contribute::finalize(
            confirmation,
            &humaux_adapters::contribution_entry_repo::ContributionEntryRepo::new(&fixture.private),
        ))
        .expect("assessed finalize")
        .0
}

/// Empties the **global** anonymous dispatch queue.
///
/// `ops.public_anonymous_dispatches` is deliberately tenant-free (§13), so an earlier fixture in
/// the same database leaves work that would otherwise be leased by the next test's call and
/// counted as its own.
pub fn drain_anonymous_queue(
    fixture: &ContributionFixture,
    public: &PublicWorkerDbPool,
    lease_owner: &str,
) {
    loop {
        let drained = fixture
            .rt
            .block_on(public_repo::run_anonymous_once(
                public,
                lease_owner,
                64,
                &NoopProjector,
            ))
            .expect("drain prior global anonymous work");
        if drained == 0 {
            break;
        }
    }
}

/// The live admission seam: finalize a release, let the anonymous worker admit it, then read the
/// admitted identity back **through** `control.anonymous_source_lineage` — the only table that
/// still relates a release to its anonymous source, and one no public-tier role can read.
pub fn admit_assessed_release(
    fixture: &mut ContributionFixture,
    public: &PublicWorkerDbPool,
    lease_owner: &str,
) -> public_repo::AdmittedRelease {
    drain_anonymous_queue(fixture, public, lease_owner);
    let release_id = finalize_assessed_release(fixture);
    assert!(
        fixture
            .rt
            .block_on(public_repo::run_anonymous_once(
                public,
                lease_owner,
                64,
                &NoopProjector,
            ))
            .expect("anonymous assessed admission")
            >= 1
    );
    let row = fixture
        .admin
        .query_one(
            "SELECT source.source_id,claim.claim_id,claim.object_revision \
             FROM public.sources source \
             JOIN public.provenance_edges edge ON edge.source_id=source.source_id \
             JOIN public.claims claim ON claim.claim_id=edge.claim_id \
             JOIN control.anonymous_source_lineage lineage \
               ON lineage.anonymous_source_id=source.source_id \
             WHERE lineage.contribution_release_id=$1",
            &[&release_id],
        )
        .expect("anonymous admitted identity");
    public_repo::AdmittedRelease {
        release_id,
        source_id: row.get(0),
        claim_id: row.get(1),
        object_revision: row.get(2),
    }
}

pub fn grant_moderator(fixture: &mut ContributionFixture) {
    fixture
        .admin
        .execute(
            "INSERT INTO control.public_moderator_grants(user_id,grant_version,enabled) \
             VALUES($1,1,true) ON CONFLICT(user_id) DO UPDATE \
             SET grant_version=EXCLUDED.grant_version,enabled=EXCLUDED.enabled",
            &[&fixture.auth.user_id().expect("fixture user").0],
        )
        .expect("active global moderator grant");
}

/// The only supported-evaluation path for an anonymous root. Migration 0134
/// (`public.guard_trust_evaluation`) keeps the legacy `public_repo::evaluate_claim` writer for
/// legacy roots only: a claim whose root is `lineage_mode='ANONYMOUS_RELEASE'` raises 42501
/// ("anonymous-root claims require the protected anonymous evaluator"). Anything admitted through
/// [`admit_assessed_release`] has an anonymous root.
pub fn evaluate_anonymous_supported(
    fixture: &mut ContributionFixture,
    claim_id: Uuid,
    expected_revision: i64,
) -> public_repo::EvaluationResult {
    grant_moderator(fixture);
    let body_sha256: Vec<u8> = fixture
        .admin
        .query_one(
            "SELECT sha256(convert_to(content::text,'UTF8')) FROM public.claims WHERE claim_id=$1",
            &[&claim_id],
        )
        .expect("current anonymous body hash")
        .get(0);
    fixture
        .rt
        .block_on(public_repo::evaluate_anonymous_claim(
            &fixture.private,
            &fixture.auth,
            &public_repo::EvaluateClaim {
                claim_id,
                expected_revision,
                expected_body_sha256: &body_sha256,
                policy_version: "phase9-anonymous-trust-v1",
                rationale: "protected fixture moderator rationale",
                target_state: ModerationState::Supported,
            },
        ))
        .expect("supported anonymous evaluation")
}

/// Enqueues the tenant-scoped `PUBLIC_PROJECT` job the runtime uses to project a currently
/// eligible object. Admission and evaluation are anonymous (0124); *projection* is not — it reads
/// only `public.eligible_objects`, which carries no contributor or release link.
pub fn seed_project_job(
    fixture: &mut ContributionFixture,
    claim_id: Uuid,
    revision: i64,
    idempotency_key: &str,
) -> Uuid {
    let claim_id_text = claim_id.to_string();
    fixture
        .admin
        .query_one(
            "INSERT INTO ops.jobs(tenant_id,job_type,idempotency_key,next_retry_at,payload) \
             VALUES($1,'PUBLIC_PROJECT',$2,clock_timestamp(), \
               jsonb_build_object('object_id',$3::text,'object_kind','CLAIM','object_revision',$4::bigint)) \
             RETURNING job_id",
            &[
                &fixture.auth.tenant_id().0,
                &idempotency_key,
                &claim_id_text,
                &revision,
            ],
        )
        .expect("project job")
        .get(0)
}

pub fn job_status(fixture: &mut ContributionFixture, job_id: Uuid) -> String {
    fixture
        .admin
        .query_one("SELECT status FROM ops.jobs WHERE job_id=$1", &[&job_id])
        .expect("job status")
        .get(0)
}
