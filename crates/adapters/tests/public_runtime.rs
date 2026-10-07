//! `adapters::tests::public_runtime` — Phase 9 public-runtime real PostgreSQL tests.
//! Depends-on: crates=[async-trait, humaux-adapters, humaux-application, humaux-domain, postgres, sqlx, tokio, uuid];
//!   services=[PostgreSQL(any) r=[control.anonymous_claim_trust_authorities, control.anonymous_source_lineage,
//!   ops.outbox, public.anonymous_source_lifecycle_events, public.claim_independence_attestations,
//!   public.claim_trust_evaluation_sources, public.claim_trust_evaluations, public.claims,
//!   public.current_anonymous_source_objects, public.eligible_objects, public.poisoning_signals,
//!   public.provenance_edges, public.sources, staging.contribution_candidate_phase9_assessments] w=[ops.jobs,
//!   ops.public_anonymous_dispatches, public.anonymous_claim_trust_receipts]
//!   x=[ops.claim_global_anonymous_public_dispatches, ops.enqueue_public_projection_from_anonymous_dispatch,
//!   public.evaluate_anonymous_claim, public.revoke_anonymous_dispatch], PostgreSQL(role_gateway),
//!   PostgreSQL(role_public_worker)]; env=[HUMAUX_TEST_PG_DSN]; modules=[adapters::contribution_entry_repo,
//!   adapters::contribution_repo, adapters::postgres, adapters::public_repo,
//!   adapters::tests::support::contribution_fixture, adapters::tests::support::public_anonymous_seam,
//!   application::contribute, domain::error, domain::ids, domain::public]
//! Called-by: [cargo-test]
//! Invariants: [public admission is exercised only after the authenticated prepare -> confirm -> finalize entry flow;
//!   forbidden or duplicate admissions are Forbidden/Conflict; the tests are #[ignore] lane tests]
//! Spec: Baseline §12.6; §79.2
//!
//! Public admission is exercised only after the
//! authenticated contribution prepare -> confirmation -> finalize entry flow.

#[path = "support/contribution_fixture.rs"]
mod contribution_fixture;
#[path = "support/public_anonymous_seam.rs"]
mod public_anonymous_seam;

use std::{
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};

use async_trait::async_trait;

use contribution_fixture::ContributionFixture;
use humaux_adapters::{
    contribution_repo,
    postgres::{PublicWorkerDbPool, RuntimeDbPool},
    public_repo,
};
use humaux_application::contribute;
use humaux_domain::ids::TenantId;
use humaux_domain::public::ModerationState;
use postgres::{Client, NoTls};
use public_anonymous_seam::{
    NoopProjector, admit_assessed_release, coverage_for_probe, drain_anonymous_queue, dsn_as_role,
    evaluate_anonymous_supported, finalize_assessed_release, grant_moderator, job_status,
    prepare_assessed_candidate, seed_project_job,
};
use sqlx::Row;
use uuid::Uuid;

struct RecordingProjector {
    live: Mutex<Vec<public_repo::ProjectionIdentity>>,
    retired: Mutex<Vec<public_repo::ProjectionIdentity>>,
    outcome: Mutex<public_repo::ProjectionWriteOutcome>,
}

impl Default for RecordingProjector {
    fn default() -> Self {
        Self {
            live: Mutex::new(Vec::new()),
            retired: Mutex::new(Vec::new()),
            outcome: Mutex::new(public_repo::ProjectionWriteOutcome::Applied),
        }
    }
}

impl RecordingProjector {
    fn with_outcome(outcome: public_repo::ProjectionWriteOutcome) -> Self {
        Self {
            outcome: Mutex::new(outcome),
            ..Self::default()
        }
    }
}

#[async_trait]
impl public_repo::PublicProjectionPort for RecordingProjector {
    async fn project_live(
        &self,
        object: &public_repo::EligibleObject,
    ) -> Result<public_repo::ProjectionWriteOutcome, humaux_domain::error::ErrorCode> {
        self.live
            .lock()
            .expect("live records")
            .push(object.identity());
        Ok(*self.outcome.lock().expect("configured outcome"))
    }

    async fn retire(
        &self,
        identity: &public_repo::ProjectionIdentity,
    ) -> Result<(), humaux_domain::error::ErrorCode> {
        self.retired
            .lock()
            .expect("retired records")
            .push(identity.clone());
        Ok(())
    }
}

struct BarrierProjector {
    entered: mpsc::SyncSender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

#[async_trait]
impl public_repo::PublicProjectionPort for BarrierProjector {
    async fn project_live(
        &self,
        _: &public_repo::EligibleObject,
    ) -> Result<public_repo::ProjectionWriteOutcome, humaux_domain::error::ErrorCode> {
        self.entered
            .send(())
            .map_err(|_| humaux_domain::error::ErrorCode::DependencyUnavailable)?;
        self.release
            .lock()
            .expect("projector release channel")
            .recv()
            .map_err(|_| humaux_domain::error::ErrorCode::DependencyUnavailable)?;
        Ok(public_repo::ProjectionWriteOutcome::Applied)
    }

    async fn retire(
        &self,
        _: &public_repo::ProjectionIdentity,
    ) -> Result<(), humaux_domain::error::ErrorCode> {
        Ok(())
    }
}

static SERIAL: Mutex<()> = Mutex::new(());

/// Protected lineage removed the public worker's direct staging-table capability in 0124. A
/// legacy release may remain stored during the expand window, but it cannot bypass the anonymous
/// assessed admission seam.
#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL migrated through latest schema and pinned Gitleaks"]
fn legacy_release_admission_is_fenced_from_protected_rows() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = ContributionFixture::new();
    let public = fixture
        .rt
        // dep: PostgreSQL(role_public_worker) — open a role-scoped PG connection/pool for this test
        .block_on(PublicWorkerDbPool::connect(&dsn_as_role(
            &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG"),
            "role_public_worker",
        )))
        .expect("public role pool");
    let release = fixture.finalize_release();
    let admission = fixture.rt.block_on(public_repo::admit_release(
        &public,
        fixture.auth.tenant_id(),
        release,
    ));
    assert_eq!(admission, Err(humaux_domain::error::ErrorCode::Forbidden));
    let claims: i64 = fixture
        .admin
        .query_one(
            "SELECT count(*) FROM public.claims WHERE intake_release_id=$1",
            &[&release],
        )
        .expect("no legacy public claim")
        .get(0);
    assert_eq!(claims, 0);
}

/// Release/outbox dispatch keeps the event identity and consumer unique and does not let a
/// generic worker claim the resulting public job. The event comes from the authenticated entry
/// flow, so dispatch cannot be confused with a caller-seeded passed release.
#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL migrated through latest schema and pinned Gitleaks"]
fn drain_release_outbox_enqueues_one_typed_public_job() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = ContributionFixture::new();
    let public = fixture
        .rt
        // dep: PostgreSQL(role_public_worker) — open a role-scoped PG connection/pool for this test
        .block_on(PublicWorkerDbPool::connect(&dsn_as_role(
            &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG"),
            "role_public_worker",
        )))
        .expect("public role pool");
    fixture.finalize_release();
    let n = fixture
        .rt
        .block_on(public_repo::drain_outbox(
            &public,
            TenantId(fixture.auth.tenant_id().0),
            8,
        ))
        .expect("drain");
    assert_eq!(n, 1);
    let row = fixture
        .admin
        .query_one(
            "SELECT job_type,outbox_event_id,consumer FROM ops.jobs WHERE tenant_id=$1",
            &[&fixture.auth.tenant_id().0],
        )
        .expect("job");
    let kind: String = row.get(0);
    let event: Option<Uuid> = row.get(1);
    let consumer: Option<String> = row.get(2);
    assert_eq!(kind, "PUBLIC_RELEASE_APPLY");
    assert!(event.is_some());
    assert_eq!(consumer.as_deref(), Some("phase9-public-runtime"));
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL migrated through latest schema and pinned Gitleaks"]
#[allow(clippy::too_many_lines)] // Replay, exact identity, and one-row lifecycle checks form one real-PG oracle.
fn assessed_release_admits_once_into_anonymous_under_review_claim() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = ContributionFixture::new();
    let public = fixture
        .rt
        // dep: PostgreSQL(role_public_worker) — open a role-scoped PG connection/pool for this test
        .block_on(PublicWorkerDbPool::connect(&dsn_as_role(
            &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG"),
            "role_public_worker",
        )))
        .expect("public role pool");
    drain_anonymous_queue(
        &fixture,
        &public,
        "public-runtime-anonymous-admission-preflight",
    );
    let release = finalize_assessed_release(&fixture);
    let jobs: i64 = fixture
        .admin
        .query_one(
            "SELECT count(*) FROM ops.public_anonymous_dispatches dispatch \
             JOIN ops.outbox event ON event.outbox_id=dispatch.outbox_event_id \
             JOIN control.anonymous_source_lineage lineage \
               ON lineage.anonymous_source_id=event.anonymous_source_id \
             WHERE dispatch.job_type='PUBLIC_ANONYMOUS_RELEASE_APPLY' \
               AND lineage.contribution_release_id=$1",
            &[&release],
        )
        .expect("anonymous job")
        .get(0);
    assert_eq!(jobs, 1);
    assert_eq!(
        fixture
            .rt
            .block_on(public_repo::run_anonymous_once(
                &public,
                "public-runtime-anonymous-admission-fixture",
                8,
                &NoopProjector,
            ))
            .expect("anonymous assessed admission"),
        1
    );
    let admitted = fixture
        .admin
        .query_one(
            "SELECT s.source_id,c.claim_id,c.object_revision \
             FROM public.sources s JOIN public.provenance_edges p ON p.source_id=s.source_id \
             JOIN public.claims c ON c.claim_id=p.claim_id \
             JOIN control.anonymous_source_lineage l ON l.anonymous_source_id=s.source_id \
             WHERE l.contribution_release_id=$1",
            &[&release],
        )
        .expect("anonymous admitted identity");
    let source_id: Uuid = admitted.get(0);
    let claim_id: Uuid = admitted.get(1);
    let _object_revision: i64 = admitted.get(2);
    let row = fixture
        .admin
        .query_one(
            "SELECT s.lineage_mode,s.contribution_release_id IS NULL, \
                    s.publisher IS NULL,s.source_url IS NULL, \
                    s.anonymous_envelope_sha256 IS NOT NULL, \
                    c.moderation_state,c.object_revision, \
                    EXISTS(SELECT 1 FROM public.anonymous_source_lifecycle_events e \
                      WHERE e.anonymous_source_id=s.source_id AND e.claim_id=c.claim_id AND e.event_type='ADMIT') \
             FROM public.sources s JOIN public.claims c ON c.claim_id=$2 \
             WHERE s.source_id=$1",
            &[&source_id, &claim_id],
        )
        .expect("anonymous admitted rows");
    assert_eq!(row.get::<_, String>(0), "ANONYMOUS_RELEASE");
    assert!(row.get::<_, bool>(1));
    assert!(row.get::<_, bool>(2));
    assert!(row.get::<_, bool>(3));
    assert!(row.get::<_, bool>(4));
    assert_eq!(row.get::<_, String>(5), "UNDER_REVIEW");
    assert_eq!(row.get::<_, i64>(6), 1);
    assert!(row.get::<_, bool>(7));
    drain_anonymous_queue(
        &fixture,
        &public,
        "public-runtime-anonymous-admission-project",
    );
    let anonymous_job: Uuid = fixture
        .admin
        .query_one(
            "SELECT dispatch.dispatch_id FROM ops.public_anonymous_dispatches dispatch \
             JOIN ops.outbox event ON event.outbox_id=dispatch.outbox_event_id \
             JOIN control.anonymous_source_lineage lineage \
               ON lineage.anonymous_source_id=event.anonymous_source_id \
             WHERE dispatch.job_type='PUBLIC_ANONYMOUS_RELEASE_APPLY' \
               AND lineage.contribution_release_id=$1",
            &[&release],
        )
        .expect("anonymous job identity")
        .get(0);
    fixture
        .admin
        .execute(
            "UPDATE ops.public_anonymous_dispatches \
             SET status='RETRY_WAIT',next_retry_at=clock_timestamp(),lease_owner=NULL,lease_expires_at=NULL \
             WHERE dispatch_id=$1",
            &[&anonymous_job],
        )
        .expect("requeue exact anonymous event for replay");
    assert_eq!(
        fixture
            .rt
            .block_on(public_repo::run_anonymous_once(
                &public,
                "public-runtime-anonymous-admission-replay",
                1,
                &NoopProjector,
            ))
            .expect("idempotent anonymous replay"),
        1
    );
    let admitted_rows: i64 = fixture
        .admin
        .query_one(
            "SELECT count(*) FROM public.anonymous_source_lifecycle_events WHERE anonymous_source_id=$1 AND event_type='ADMIT'",
            &[&source_id],
        )
        .expect("anonymous admission event count")
        .get(0);
    assert_eq!(
        admitted_rows, 1,
        "exact replay does not duplicate the claim"
    );
}

#[test]
#[ignore = "lane(a:disposable) needs a per-run database: its oracle counts the GLOBAL unfinished rows of ops.public_anonymous_dispatches, and a shared database carries an earlier run's PROCESSING row into that count"]
#[allow(clippy::too_many_lines)] // Admission, evaluation, dispatch authority, and revoke share one lifecycle oracle.
fn assessed_anonymous_lifecycle_tracks_supported_revision_and_revocation_fails_closed() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = ContributionFixture::new();
    let public = fixture
        .rt
        // dep: PostgreSQL(role_public_worker) — open a role-scoped PG connection/pool for this test
        .block_on(PublicWorkerDbPool::connect(&dsn_as_role(
            &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG"),
            "role_public_worker",
        )))
        .expect("public role pool");
    drain_anonymous_queue(&fixture, &public, "phase9-lifecycle-preflight");
    let release = finalize_assessed_release(&fixture);
    assert!(
        fixture
            .rt
            .block_on(public_repo::run_anonymous_once(
                &public,
                "phase9-lifecycle-admit",
                64,
                &NoopProjector,
            ))
            .expect("anonymous admission")
            >= 1
    );
    // A global queue can contain projections from earlier serial fixtures. Drain that bounded
    // snapshot and its admission projections before leasing this test's revoke authority.
    let _ = fixture
        .rt
        .block_on(public_repo::run_anonymous_once(
            &public,
            "phase9-lifecycle-admit-project",
            64,
            &NoopProjector,
        ))
        .expect("anonymous admission projection");
    let pending: i64 = fixture
        .admin
        .query_one(
            "SELECT count(*) FROM ops.public_anonymous_dispatches \
             WHERE status IN ('PENDING','RETRY_WAIT','PROCESSING')",
            &[],
        )
        .expect("global queue readback")
        .get(0);
    assert_eq!(
        pending, 0,
        "global queue drained before revoke authority test"
    );
    let admitted = fixture
        .admin
        .query_one(
            "SELECT source.source_id,claim.claim_id,claim.object_revision \
             FROM public.sources source \
             JOIN public.provenance_edges edge ON edge.source_id=source.source_id \
             JOIN public.claims claim ON claim.claim_id=edge.claim_id \
             JOIN control.anonymous_source_lineage lineage ON lineage.anonymous_source_id=source.source_id \
             WHERE lineage.contribution_release_id=$1",
            &[&release],
        )
        .expect("anonymous admitted claim");
    let source_id: Uuid = admitted.get(0);
    let claim_id: Uuid = admitted.get(1);
    let initial_revision: i64 = admitted.get(2);
    grant_moderator(&mut fixture);
    let initial_body_sha256: Vec<u8> = fixture
        .admin
        .query_one(
            "SELECT sha256(convert_to(content::text,'UTF8')) FROM public.claims WHERE claim_id=$1",
            &[&claim_id],
        )
        .expect("initial anonymous body hash")
        .get(0);
    assert_eq!(
        fixture.rt.block_on(public_repo::evaluate_claim(
            &public,
            &fixture.auth,
            &public_repo::EvaluateClaim {
                claim_id,
                expected_revision: initial_revision,
                expected_body_sha256: &initial_body_sha256,
                policy_version: "legacy-anonymous-must-fail",
                rationale: "legacy identity-bearing path is forbidden",
                target_state: ModerationState::Supported,
            },
        )),
        Err(humaux_domain::error::ErrorCode::Forbidden),
        "legacy evaluator cannot write an identity-bearing anonymous receipt"
    );
    let mut spoof_scope = ContributionFixture::new();
    grant_moderator(&mut spoof_scope);
    let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG");
    // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
    let mut raw_private = Client::connect(&dsn_as_role(&dsn, "role_private_worker"), NoTls)
        .expect("raw private worker for scope-spoof fault");
    let mut spoof_transaction = raw_private.transaction().expect("scope-spoof transaction");
    spoof_transaction
        .batch_execute(&format!(
            "SET LOCAL humaux.tenant_id='{}'; SET LOCAL humaux.user_id='{}'",
            fixture.auth.tenant_id().0,
            fixture.auth.user_id().expect("fixture user").0
        ))
        .expect("real caller scope");
    let spoof_error = spoof_transaction
        .query(
            "SELECT * FROM public.evaluate_anonymous_claim($1,$2,$3,$4,$5,$6,$7,$8)",
            &[
                &spoof_scope.auth.tenant_id().0,
                &spoof_scope.auth.user_id().expect("other valid moderator").0,
                &claim_id,
                &initial_revision,
                &initial_body_sha256,
                &"scope-spoof-v1",
                &"valid other moderator cannot replace caller scope",
                &"SUPPORTED",
            ],
        )
        .expect_err("valid moderator parameters cannot spoof caller transaction scope");
    assert_eq!(
        spoof_error.code().map(postgres::error::SqlState::code),
        Some("42501")
    );
    drop(spoof_transaction);
    let supported = evaluate_anonymous_supported(&mut fixture, claim_id, initial_revision);
    let boundary = fixture
        .admin
        .query_one(
            "SELECT claim.current_evaluation_id IS NULL, \
                    claim.current_anonymous_trust_receipt_id=$2, \
                    EXISTS(SELECT 1 FROM control.anonymous_claim_trust_authorities authority \
                      WHERE authority.public_receipt_id=$2 AND authority.claim_id=$1), \
                    EXISTS(SELECT 1 FROM public.anonymous_claim_trust_receipts receipt \
                      JOIN public.claim_independence_attestations attestation \
                        ON attestation.attestation_id=receipt.independence_attestation_id \
                      WHERE receipt.anonymous_trust_receipt_id=$2 \
                        AND receipt.root_set_sha256=attestation.root_set_sha \
                        AND receipt.support_count=attestation.support_count \
                        AND receipt.independent_support_count=attestation.independent_count \
                        AND receipt.sybil_risk=attestation.sybil_risk \
                        AND receipt.checks_complete=attestation.checks_complete) \
             FROM public.claims claim WHERE claim.claim_id=$1",
            &[&claim_id, &supported.evaluation_id],
        )
        .expect("anonymous trust boundary readback");
    assert!(boundary.get::<_, bool>(0));
    assert!(boundary.get::<_, bool>(1));
    assert!(boundary.get::<_, bool>(2));
    assert!(boundary.get::<_, bool>(3));

    let forbidden_receipt_columns: i64 = fixture
        .admin
        .query_one(
            "SELECT count(*) FROM pg_attribute attribute \
             CROSS JOIN unnest(ARRAY['tenant','release','contributor','publisher','evaluator', \
               'grant','rationale','anonymous_trust_authority_id','protected']) forbidden(token) \
             WHERE attribute.attrelid='public.anonymous_claim_trust_receipts'::regclass \
               AND attribute.attnum>0 AND NOT attribute.attisdropped \
               AND attribute.attname ILIKE '%'||forbidden.token||'%'",
            &[],
        )
        .expect("identity-free receipt catalog")
        .get(0);
    assert_eq!(forbidden_receipt_columns, 0);

    let function_boundary = fixture
        .admin
        .query_one(
            "SELECT pg_get_userbyid(proc.proowner)='role_migration_owner',proc.prosecdef, \
                    proc.provolatile='v',proc.proconfig=ARRAY['search_path=pg_catalog'], \
                    has_function_privilege('role_private_worker',proc.oid,'EXECUTE'), \
                    NOT has_function_privilege('role_gateway',proc.oid,'EXECUTE'), \
                    NOT has_function_privilege('role_public_worker',proc.oid,'EXECUTE'), \
                    NOT has_function_privilege('role_retrieval_worker',proc.oid,'EXECUTE'), \
                    NOT EXISTS(SELECT 1 FROM aclexplode(coalesce(proc.proacl, \
                      acldefault('f',proc.proowner))) acl \
                      WHERE acl.grantee=0 AND acl.privilege_type='EXECUTE') \
             FROM pg_proc proc \
             WHERE proc.oid='public.evaluate_anonymous_claim(uuid,uuid,uuid,bigint,bytea,text,text,text)'::regprocedure",
            &[],
        )
        .expect("anonymous evaluator catalog boundary");
    for index in 0..9 {
        assert!(function_boundary.get::<_, bool>(index));
    }

    for role in [
        "role_public_worker",
        "role_gateway",
        "role_retrieval_worker",
    ] {
        // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
        let mut denied = Client::connect(&dsn_as_role(&dsn, role), NoTls)
            .unwrap_or_else(|error| panic!("{role} connection: {error}"));
        for relation in [
            "public.claim_trust_evaluations",
            "public.claim_trust_evaluation_sources",
            "public.poisoning_signals",
            "control.anonymous_claim_trust_authorities",
        ] {
            let error = denied
                .query(&format!("SELECT 1 FROM {relation} LIMIT 1"), &[])
                .unwrap_err();
            assert_eq!(
                error.code().map(postgres::error::SqlState::code),
                Some("42501")
            );
        }
    }

    let gateway = fixture
        .rt
        // dep: PostgreSQL(role_gateway) — open a role-scoped PG connection/pool for this test
        .block_on(RuntimeDbPool::connect(&dsn_as_role(&dsn, "role_gateway")))
        .expect("gateway pool");
    let body_sha256: [u8; 32] = initial_body_sha256
        .as_slice()
        .try_into()
        .expect("body sha length");
    let identity = public_repo::ProjectionIdentity {
        object_id: claim_id,
        object_kind: "CLAIM".to_owned(),
        object_revision: supported.object_revision,
        evaluation_id: supported.evaluation_id,
        body_sha256,
    };
    assert!(
        fixture
            .rt
            .block_on(public_repo::hydrate_gateway(&gateway, &identity))
            .expect("safe anonymous hydrate")
            .is_some()
    );

    let mut missing_authority = fixture.admin.transaction().expect("mutation transaction");
    missing_authority
        .execute(
            "INSERT INTO public.anonymous_claim_trust_receipts( \
               anonymous_trust_receipt_id,claim_id,object_revision,body_sha256,policy_version, \
               moderation_state,independence_attestation_id,root_set_sha256,support_count, \
               independent_support_count,sybil_risk,checks_complete,review_commitment) \
             SELECT uuidv7(),claim_id,object_revision+1000,body_sha256,policy_version, \
               moderation_state,independence_attestation_id,root_set_sha256,support_count, \
               independent_support_count,sybil_risk,checks_complete,sha256(review_commitment) \
             FROM public.anonymous_claim_trust_receipts \
             WHERE anonymous_trust_receipt_id=$1",
            &[&supported.evaluation_id],
        )
        .expect("stage receipt without authority");
    let mutation_error = missing_authority
        .batch_execute("SET CONSTRAINTS ALL IMMEDIATE")
        .expect_err("receipt without protected authority must fail at transaction boundary");
    assert_eq!(
        mutation_error.code().map(postgres::error::SqlState::code),
        Some("23514")
    );
    drop(missing_authority);
    let receipt = fixture
        .admin
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM public.current_anonymous_source_objects current_object \
               WHERE current_object.anonymous_source_id=$1 AND current_object.claim_id=$2 \
                 AND current_object.object_revision=$3), \
                    EXISTS(SELECT 1 FROM public.eligible_objects eligible \
               WHERE eligible.object_id=$2 AND eligible.object_kind='CLAIM' \
                 AND eligible.object_revision=$3)",
            &[&source_id, &claim_id, &supported.object_revision],
        )
        .expect("exact current lifecycle and eligibility");
    assert!(receipt.get::<_, bool>(0));
    assert!(receipt.get::<_, bool>(1));
    let body_sha256: Vec<u8> = fixture
        .admin
        .query_one(
            "SELECT sha256(convert_to(content::text,'UTF8')) FROM public.claims WHERE claim_id=$1",
            &[&claim_id],
        )
        .expect("current body hash")
        .get(0);
    let stale = fixture.rt.block_on(public_repo::evaluate_claim(
        &public,
        &fixture.auth,
        &public_repo::EvaluateClaim {
            claim_id,
            expected_revision: initial_revision,
            expected_body_sha256: &body_sha256,
            policy_version: "public-runtime-test-v1",
            rationale: "stale lifecycle evaluation must fail closed",
            target_state: ModerationState::Supported,
        },
    ));
    assert_eq!(stale, Err(humaux_domain::error::ErrorCode::Conflict));
    assert!(
        fixture
            .rt
            .block_on(contribution_repo::revoke_release(
                &fixture.private,
                fixture.auth.tenant_id(),
                release,
            ))
            .expect("private assessed revoke")
    );
    let raw_public = fixture
        .rt
        .block_on(
            // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
            // ADR-0065 D-A: a deliberately raw pool spelled through `Pool<Postgres>` (the G80-40 precedent of
            // outbox_batch_remember.rs), so the pool builder keeps its one site in postgres.rs.
            sqlx::Pool::<sqlx::Postgres>::connect(&dsn_as_role(
                &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG"),
                "role_public_worker",
            )),
        )
        .expect("raw public-role pool for dispatch authority negative control");

    let leased = fixture.rt.block_on(async {
        sqlx::query(
            "SELECT dispatch_id,attempt FROM ops.claim_global_anonymous_public_dispatches($1,$2,$3)",
        )
        .bind("phase9-lifecycle-wrong-revision")
        .bind(60.0)
        .bind(1_i64)
        .fetch_one(&raw_public)
        .await
    })
    .expect("claim exact revoke dispatch");
    let dispatch_id: Uuid = leased.try_get("dispatch_id").expect("dispatch id");
    let attempt: i32 = leased.try_get("attempt").expect("dispatch attempt");
    let wrong_revision = fixture.rt.block_on(async {
        sqlx::query(
            "SELECT ops.enqueue_public_projection_from_anonymous_dispatch($1,$2,$3,$4,NULL,$5)",
        )
        .bind(dispatch_id)
        .bind("phase9-lifecycle-wrong-revision")
        .bind(attempt)
        .bind(claim_id)
        .bind(supported.object_revision + 1)
        .execute(&raw_public)
        .await
    });
    let wrong_revision = wrong_revision.expect_err("wrong object revision is rejected");
    assert_eq!(
        wrong_revision
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("42501")
    );
    fixture
        .admin
        .execute(
            "UPDATE ops.public_anonymous_dispatches \
             SET status='PENDING',next_retry_at=clock_timestamp(),lease_owner=NULL,lease_expires_at=NULL \
             WHERE dispatch_id=$1",
            &[&dispatch_id],
        )
        .expect("requeue revoke after negative control");
    assert_eq!(
        fixture
            .rt
            .block_on(public_repo::run_anonymous_once(
                &public,
                "phase9-lifecycle-revoke",
                1,
                &NoopProjector,
            ))
            .expect("anonymous revoke"),
        1
    );
    let revoked = fixture
        .admin
        .query_one(
            "SELECT moderation_state,object_revision,current_evaluation_id IS NULL, \
                    current_anonymous_trust_receipt_id IS NULL, \
                    NOT EXISTS(SELECT 1 FROM public.eligible_objects eligible \
                      WHERE eligible.object_id=$1 AND eligible.object_kind='CLAIM'), \
                    NOT EXISTS(SELECT 1 FROM public.current_anonymous_source_objects current_object \
                      WHERE current_object.anonymous_source_id=$2 AND current_object.claim_id=$1 \
                        AND current_object.object_revision=$3) \
             FROM public.claims WHERE claim_id=$1",
            &[&claim_id, &source_id, &supported.object_revision],
        )
        .expect("revoked lifecycle state");
    assert_eq!(revoked.get::<_, String>(0), "REVOKED");
    assert_eq!(revoked.get::<_, i64>(1), supported.object_revision + 1);
    assert!(revoked.get::<_, bool>(2));
    assert!(revoked.get::<_, bool>(3));
    assert!(revoked.get::<_, bool>(4));
    assert!(revoked.get::<_, bool>(5));
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL migrated through latest schema and pinned Gitleaks"]
fn public_coverage_change_after_assessed_prepare_conflicts_on_finalize() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut stale = ContributionFixture::new();
    let candidate = prepare_assessed_candidate(&stale);
    let prepared_snapshot: Uuid = stale
        .admin
        .query_one(
            "SELECT coverage_digest_id FROM staging.contribution_candidate_phase9_assessments WHERE candidate_id=$1",
            &[&candidate.0],
        )
        .expect("stored assessed binding")
        .get(0);

    // A distinct assessed contribution becomes a supported public object matching the same
    // probe, so it fills the gap after `stale` was prepared.
    let mut mutator = ContributionFixture::new();
    let public = mutator
        .rt
        // dep: PostgreSQL(role_public_worker) — open a role-scoped PG connection/pool for this test
        .block_on(PublicWorkerDbPool::connect(&dsn_as_role(
            &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG"),
            "role_public_worker",
        )))
        .expect("public role pool");
    let release = finalize_assessed_release(&mutator);
    mutator
        .rt
        .block_on(public_repo::run_anonymous_once(
            &public,
            "phase9-coverage-stale-fixture",
            8,
            &NoopProjector,
        ))
        .expect("admit assessed mutation");
    let admitted = mutator
        .admin
        .query_one(
            "SELECT c.claim_id,c.object_revision FROM public.claims c \
             JOIN public.provenance_edges p USING(claim_id) \
             JOIN public.sources s USING(source_id) \
             JOIN control.anonymous_source_lineage l ON l.anonymous_source_id=s.source_id \
             WHERE l.contribution_release_id=$1",
            &[&release],
        )
        .expect("admitted anonymous claim");
    let claim_id: Uuid = admitted.get(0);
    let revision: i64 = admitted.get(1);
    evaluate_anonymous_supported(&mut mutator, claim_id, revision);

    let current_snapshot = coverage_for_probe(b"assessed public").binding().digest_id();
    assert_ne!(
        prepared_snapshot, current_snapshot,
        "public coverage changed"
    );
    let confirmation = stale.confirm(candidate);
    let result = stale.rt.block_on(contribute::finalize(
        confirmation,
        &humaux_adapters::contribution_entry_repo::ContributionEntryRepo::new(&stale.private),
    ));
    assert_eq!(result, Err(humaux_domain::error::ErrorCode::Conflict));
    assert_eq!(stale.counts(), (1, 0, 0));
}

/// Revocation is fenced in the private transaction before an anonymous release job can run.
/// The public worker must consume both opaque events without creating a source or claim.
#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL migrated through latest schema and pinned Gitleaks"]
fn assessed_revoke_before_admit_never_creates_anonymous_claim() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = ContributionFixture::new();
    let public = fixture
        .rt
        // dep: PostgreSQL(role_public_worker) — open a role-scoped PG connection/pool for this test
        .block_on(PublicWorkerDbPool::connect(&dsn_as_role(
            &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG"),
            "role_public_worker",
        )))
        .expect("public role pool");
    drain_anonymous_queue(
        &fixture,
        &public,
        "public-runtime-revoke-before-admit-preflight",
    );
    let release = finalize_assessed_release(&fixture);
    assert!(
        fixture
            .rt
            .block_on(contribution_repo::revoke_release(
                &fixture.private,
                fixture.auth.tenant_id(),
                release,
            ))
            .expect("private anonymous revoke")
    );
    assert_eq!(
        fixture
            .rt
            .block_on(public_repo::run_anonymous_once(
                &public,
                "public-runtime-anonymous-revoke-before-admit",
                8,
                &NoopProjector,
            ))
            .expect("consume fenced anonymous events"),
        2
    );
    let row = fixture
        .admin
        .query_one(
            "SELECT count(*) FROM public.sources s \
             JOIN control.anonymous_source_lineage l ON l.anonymous_source_id=s.source_id \
             WHERE l.contribution_release_id=$1",
            &[&release],
        )
        .expect("anonymous source count")
        .get::<_, i64>(0);
    assert_eq!(row, 0, "revocation fence blocks source creation");
    let claims: i64 = fixture
        .admin
        .query_one(
            "SELECT count(*) FROM public.claims c \
             JOIN public.provenance_edges p ON p.claim_id=c.claim_id \
             JOIN control.anonymous_source_lineage l ON l.anonymous_source_id=p.source_id \
             WHERE l.contribution_release_id=$1",
            &[&release],
        )
        .expect("anonymous claim count")
        .get(0);
    assert_eq!(
        claims, 0,
        "revocation before admission cannot resurrect a claim"
    );
}

/// A revoke reaches an initial under-review claim through its current source closure even before
/// that claim has any trust receipt. A repeated private revoke emits no second event, so the
/// public consumer cannot advance the object revision twice.
#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL migrated through latest schema and pinned Gitleaks"]
fn revoke_reaches_under_review_claim_via_source_closure_once() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = ContributionFixture::new();
    let public = fixture
        .rt
        // dep: PostgreSQL(role_public_worker) — open a role-scoped PG connection/pool for this test
        .block_on(PublicWorkerDbPool::connect(&dsn_as_role(
            &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG"),
            "role_public_worker",
        )))
        .expect("public role pool");
    let admitted = admit_assessed_release(
        &mut fixture,
        &public,
        "public-runtime-source-closure-admission",
    );
    let release = admitted.release_id;
    assert!(
        fixture
            .rt
            .block_on(contribution_repo::revoke_release(
                &fixture.private,
                fixture.auth.tenant_id(),
                release,
            ))
            .expect("first private revoke")
    );
    assert!(
        !fixture
            .rt
            .block_on(contribution_repo::revoke_release(
                &fixture.private,
                fixture.auth.tenant_id(),
                release,
            ))
            .expect("idempotent private revoke")
    );
    assert_eq!(
        fixture
            .rt
            .block_on(public_repo::run_anonymous_once(
                &public,
                "public-runtime-source-closure-revoke",
                8,
                &NoopProjector,
            ))
            .expect("apply projection and protected-mapping revoke"),
        2,
        "the queued admission projection and protected-mapping revoke each complete once"
    );
    let row = fixture
        .admin
        .query_one(
            "SELECT moderation_state,object_revision,current_evaluation_id IS NULL \
             FROM public.claims WHERE claim_id=$1",
            &[&admitted.claim_id],
        )
        .expect("revoked claim");
    assert_eq!(row.get::<_, String>(0), "REVOKED");
    assert_eq!(row.get::<_, i64>(1), 2);
    assert!(row.get::<_, bool>(2));
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL migrated through latest schema and pinned Gitleaks"]
#[allow(clippy::too_many_lines)] // Negative dispatch authority and the exact protected-mapping revoke are one oracle.
fn assessed_revoke_reaches_anonymous_under_review_claim_once() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = ContributionFixture::new();
    let public = fixture
        .rt
        // dep: PostgreSQL(role_public_worker) — open a role-scoped PG connection/pool for this test
        .block_on(PublicWorkerDbPool::connect(&dsn_as_role(
            &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG"),
            "role_public_worker",
        )))
        .expect("public role pool");
    drain_anonymous_queue(
        &fixture,
        &public,
        "public-runtime-anonymous-revoke-preflight",
    );
    let release = finalize_assessed_release(&fixture);
    assert_eq!(
        fixture
            .rt
            .block_on(public_repo::run_anonymous_once(
                &public,
                "public-runtime-anonymous-admission-before-revoke",
                8,
                &NoopProjector,
            ))
            .expect("anonymous public admission"),
        1
    );
    let admitted_claim_id: Uuid = fixture
        .admin
        .query_one(
            "SELECT c.claim_id FROM public.claims c \
             JOIN public.provenance_edges p ON p.claim_id=c.claim_id \
             JOIN control.anonymous_source_lineage l ON l.anonymous_source_id=p.source_id \
             WHERE l.contribution_release_id=$1",
            &[&release],
        )
        .expect("anonymous claim")
        .get(0);
    assert!(
        fixture
            .rt
            .block_on(contribution_repo::revoke_release(
                &fixture.private,
                fixture.auth.tenant_id(),
                release,
            ))
            .expect("first private revoke")
    );
    assert!(
        !fixture
            .rt
            .block_on(contribution_repo::revoke_release(
                &fixture.private,
                fixture.auth.tenant_id(),
                release,
            ))
            .expect("idempotent private revoke")
    );
    assert_eq!(
        fixture
            .rt
            .block_on(public_repo::run_anonymous_once(
                &public,
                "public-runtime-anonymous-drain-admission-projection",
                1,
                &NoopProjector,
            ))
            .expect("drain admission projection before revoke authority check"),
        1
    );
    let raw_public = fixture
        .rt
        .block_on(
            // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
            // ADR-0065 D-A: a deliberately raw pool spelled through `Pool<Postgres>` (the G80-40 precedent of
            // outbox_batch_remember.rs), so the pool builder keeps its one site in postgres.rs.
            sqlx::Pool::<sqlx::Postgres>::connect(&dsn_as_role(
                &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG"),
                "role_public_worker",
            )),
        )
        .expect("raw public-role pool for revoke authority checks");
    let leased = fixture
        .rt
        .block_on(async {
            sqlx::query(
                "SELECT dispatch_id,job_type,attempt \
                 FROM ops.claim_global_anonymous_public_dispatches($1,$2,$3)",
            )
            .bind("public-runtime-anonymous-revoke-authority")
            .bind(60.0)
            .bind(1_i64)
            .fetch_one(&raw_public)
            .await
        })
        .expect("claim exact revoke dispatch");
    let dispatch_id: Uuid = leased.try_get("dispatch_id").expect("dispatch id");
    let job_type: String = leased.try_get("job_type").expect("dispatch job type");
    let attempt: i32 = leased.try_get("attempt").expect("dispatch attempt");
    assert_eq!(job_type, "PUBLIC_ANONYMOUS_REVOKE_APPLY");
    let wrong_dispatch = fixture.rt.block_on(async {
        sqlx::query("SELECT * FROM public.revoke_anonymous_dispatch($1,$2,$3)")
            .bind(Uuid::new_v4())
            .bind("public-runtime-anonymous-revoke-authority")
            .bind(attempt)
            .fetch_all(&raw_public)
            .await
    });
    assert_eq!(
        wrong_dispatch
            .expect_err("a different dispatch id cannot authorize revocation")
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("42501")
    );
    fixture
        .admin
        .execute(
            "UPDATE ops.public_anonymous_dispatches \
             SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE dispatch_id=$1",
            &[&dispatch_id],
        )
        .expect("expire only the leased revoke authority");
    let expired_dispatch = fixture.rt.block_on(async {
        sqlx::query("SELECT * FROM public.revoke_anonymous_dispatch($1,$2,$3)")
            .bind(dispatch_id)
            .bind("public-runtime-anonymous-revoke-authority")
            .bind(attempt)
            .fetch_all(&raw_public)
            .await
    });
    assert_eq!(
        expired_dispatch
            .expect_err("an expired dispatch cannot authorize revocation")
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("42501")
    );
    fixture
        .admin
        .execute(
            "UPDATE ops.public_anonymous_dispatches \
             SET status='PENDING',next_retry_at=clock_timestamp(),lease_owner=NULL,lease_expires_at=NULL \
             WHERE dispatch_id=$1",
            &[&dispatch_id],
        )
        .expect("requeue exact revoke authority after negative checks");
    assert_eq!(
        fixture
            .rt
            .block_on(public_repo::run_anonymous_once(
                &public,
                "public-runtime-anonymous-revoke-fixture",
                8,
                &NoopProjector,
            ))
            .expect("apply anonymous revoke"),
        1
    );
    let row = fixture
        .admin
        .query_one(
            "SELECT moderation_state,object_revision,current_evaluation_id IS NULL, \
                    EXISTS(SELECT 1 FROM public.anonymous_source_lifecycle_events e \
                      WHERE e.claim_id=$1 AND e.event_type='REVOKE') \
             FROM public.claims WHERE claim_id=$1",
            &[&admitted_claim_id],
        )
        .expect("anonymous revoked claim");
    assert_eq!(row.get::<_, String>(0), "REVOKED");
    assert_eq!(row.get::<_, i64>(1), 2);
    assert!(row.get::<_, bool>(2));
    assert!(row.get::<_, bool>(3));
}

/// A later eligible revision must leave its own point live while permanently retiring each prior
/// receipt identity. The controlled projector is only the remote seam; admission and both
/// moderator evaluations use the authenticated real-PG path.
#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL migrated through latest schema and pinned Gitleaks"]
fn current_eligible_projection_retires_only_older_receipts() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = ContributionFixture::new();
    let public = fixture
        .rt
        // dep: PostgreSQL(role_public_worker) — open a role-scoped PG connection/pool for this test
        .block_on(PublicWorkerDbPool::connect(&dsn_as_role(
            &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG"),
            "role_public_worker",
        )))
        .expect("public role pool");
    let admitted = admit_assessed_release(&mut fixture, &public, "public-runtime-retire-admission");
    let first =
        evaluate_anonymous_supported(&mut fixture, admitted.claim_id, admitted.object_revision);
    let second =
        evaluate_anonymous_supported(&mut fixture, admitted.claim_id, first.object_revision);
    let idempotency_key = format!("public-runtime-retire-earlier-receipt-{}", Uuid::now_v7());
    let job = seed_project_job(
        &mut fixture,
        admitted.claim_id,
        second.object_revision,
        &idempotency_key,
    );
    let projector = RecordingProjector::default();
    assert_eq!(
        fixture
            .rt
            .block_on(public_repo::run_once(
                &public,
                fixture.auth.tenant_id(),
                "public-runtime-retire-earlier-receipt",
                1,
                &projector,
            ))
            .expect("project current receipt"),
        1
    );
    let live = projector.live.lock().expect("live records").clone();
    let retired = projector.retired.lock().expect("retired records").clone();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].object_revision, second.object_revision);
    assert_eq!(live[0].evaluation_id, second.evaluation_id);
    assert_eq!(retired.len(), 1);
    assert_eq!(retired[0].object_revision, first.object_revision);
    assert_eq!(retired[0].evaluation_id, first.evaluation_id);
    assert_ne!(retired[0], live[0]);
    assert_eq!(job_status(&mut fixture, job), "DONE");
}

/// A remote tombstone report for the exact current eligible identity is not terminal: a fresh
/// strict hydration still finds it eligible, so the attempt is fenced out of DONE and replayed.
#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL migrated through latest schema and pinned Gitleaks"]
fn superseded_current_projection_is_retryable_then_replay_converges() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = ContributionFixture::new();
    let public = fixture
        .rt
        // dep: PostgreSQL(role_public_worker) — open a role-scoped PG connection/pool for this test
        .block_on(PublicWorkerDbPool::connect(&dsn_as_role(
            &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG"),
            "role_public_worker",
        )))
        .expect("public role pool");
    let admitted =
        admit_assessed_release(&mut fixture, &public, "public-runtime-superseded-admission");
    let current =
        evaluate_anonymous_supported(&mut fixture, admitted.claim_id, admitted.object_revision);
    let idempotency_key = format!("public-runtime-superseded-current-{}", Uuid::now_v7());
    let job = seed_project_job(
        &mut fixture,
        admitted.claim_id,
        current.object_revision,
        &idempotency_key,
    );
    let superseded =
        RecordingProjector::with_outcome(public_repo::ProjectionWriteOutcome::Superseded);
    assert_eq!(
        fixture
            .rt
            .block_on(public_repo::run_once(
                &public,
                fixture.auth.tenant_id(),
                "public-runtime-superseded-current",
                1,
                &superseded,
            ))
            .expect("retryable superseded outcome is contained"),
        0
    );
    assert_eq!(job_status(&mut fixture, job), "RETRY_WAIT");
    fixture
        .admin
        .execute(
            "UPDATE ops.jobs SET next_retry_at=clock_timestamp() WHERE job_id=$1",
            &[&job],
        )
        .expect("make fenced replay immediately claimable");
    let replay = RecordingProjector::default();
    assert_eq!(
        fixture
            .rt
            .block_on(public_repo::run_once(
                &public,
                fixture.auth.tenant_id(),
                "public-runtime-superseded-current",
                1,
                &replay,
            ))
            .expect("fresh attempt converges"),
        1
    );
    assert_eq!(job_status(&mut fixture, job), "DONE");
    let live = replay.live.lock().expect("replay live record").clone();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].evaluation_id, current.evaluation_id);
}

/// The remote callback is deliberately held until the exact live lease has elapsed. It may have
/// completed outside PostgreSQL, but the old attempt cannot mark DONE; a requeued new attempt
/// increments its fencing token and converges.
#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL migrated through latest schema and pinned Gitleaks"]
#[allow(clippy::too_many_lines)] // The exact lease expiry, blocked remote callback, and fenced replay share one causal real-PG oracle.
fn lease_expiry_during_projection_fences_old_done_then_replays() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = ContributionFixture::new();
    let dsn = dsn_as_role(
        &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG"),
        "role_public_worker",
    );
    let public = fixture
        .rt
        // dep: PostgreSQL(role_public_worker) — open a role-scoped PG connection/pool for this test
        .block_on(PublicWorkerDbPool::connect(&dsn))
        .expect("public role pool");
    let admitted = admit_assessed_release(&mut fixture, &public, "public-runtime-lease-admission");
    let current =
        evaluate_anonymous_supported(&mut fixture, admitted.claim_id, admitted.object_revision);
    let idempotency_key = format!("public-runtime-lease-expiry-{}", Uuid::now_v7());
    let job = seed_project_job(
        &mut fixture,
        admitted.claim_id,
        current.object_revision,
        &idempotency_key,
    );
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let projector = Arc::new(BarrierProjector {
        entered: entered_tx,
        release: Mutex::new(release_rx),
    });
    let tenant_id = fixture.auth.tenant_id();
    let old_projector = Arc::clone(&projector);
    let old_dsn = dsn.clone();
    let old_owner = format!("public-runtime-lease-old-{}", Uuid::now_v7());
    let old_owner_for_thread = old_owner.clone();
    let old = thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("old worker runtime");
        let pool = rt
            // dep: PostgreSQL(role_public_worker) — open a role-scoped PG connection/pool for this test
            .block_on(PublicWorkerDbPool::connect(&old_dsn))
            .expect("old public worker pool");
        rt.block_on(public_repo::run_once(
            &pool,
            tenant_id,
            &old_owner_for_thread,
            1,
            old_projector.as_ref(),
        ))
    });
    entered_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("remote projector entered under claimed lease");
    let deadline = Instant::now() + Duration::from_secs(75);
    loop {
        let expired: bool = fixture
            .admin
            .query_one(
                "SELECT lease_expires_at < clock_timestamp() FROM ops.jobs WHERE job_id=$1",
                &[&job],
            )
            .expect("lease observation")
            .get(0);
        if expired {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "claimed lease did not expire in time"
        );
        thread::sleep(Duration::from_millis(250));
    }
    release_tx.send(()).expect("release remote callback");
    assert_eq!(
        old.join()
            .expect("old worker joins")
            .expect("old worker contains expired attempt"),
        0,
        "expired attempt may finish remote I/O but cannot mark DONE"
    );
    let row = fixture
        .admin
        .query_one(
            "SELECT status,attempt,lease_owner=$2 FROM ops.jobs WHERE job_id=$1",
            &[&job, &old_owner],
        )
        .expect("old attempt state");
    assert_eq!(row.get::<_, String>(0), "PROCESSING");
    assert_eq!(row.get::<_, i32>(1), 1);
    assert!(row.get::<_, bool>(2));
    fixture
        .admin
        .execute(
            "UPDATE ops.jobs SET status='RETRY_WAIT',next_retry_at=clock_timestamp(), \
             lease_owner=NULL,lease_expires_at=NULL WHERE job_id=$1 AND attempt=1",
            &[&job],
        )
        .expect("test reaper requeues only the expired exact attempt");
    let replay = RecordingProjector::default();
    assert_eq!(
        fixture
            .rt
            .block_on(public_repo::run_once(
                &public,
                fixture.auth.tenant_id(),
                "public-runtime-lease-replay",
                1,
                &replay,
            ))
            .expect("new fenced attempt converges"),
        1
    );
    let row = fixture
        .admin
        .query_one(
            "SELECT status,attempt FROM ops.jobs WHERE job_id=$1",
            &[&job],
        )
        .expect("replayed job state");
    assert_eq!(row.get::<_, String>(0), "DONE");
    assert_eq!(row.get::<_, i32>(1), 2);
    assert_eq!(replay.live.lock().expect("replay remote call").len(), 1);
}
