//! Phase 9 real PostgreSQL acceptance for public trust decisions and typed serving hydration.

#[path = "support/contribution_fixture.rs"]
mod contribution_fixture;

use std::sync::Mutex;

use contribution_fixture::ContributionFixture;
use humaux_adapters::{
    contribution_repo,
    postgres::{PublicWorkerDbPool, RetrievalWorkerDbPool, RuntimeDbPool},
    public_repo::{self, EvaluateClaim, ProjectionIdentity},
};
use humaux_domain::{
    authority::MemoryId,
    error::ErrorCode,
    public::{
        ContributionPolicy, ContributionRelease, ModerationState, ReleaseSource, RightsProvenance,
        ScanOutcome,
    },
};
use postgres::{Client, NoTls};
use uuid::Uuid;

static SERIAL: Mutex<()> = Mutex::new(());

fn dsn_as_role(dsn: &str, role: &str) -> String {
    let sep = if dsn.contains('?') { '&' } else { '?' };
    format!("{dsn}{sep}options=-c%20role%3D{role}")
}

fn public_pool(fixture: &ContributionFixture) -> PublicWorkerDbPool {
    fixture
        .rt
        .block_on(PublicWorkerDbPool::connect(&dsn_as_role(
            &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG"),
            "role_public_worker",
        )))
        .expect("public role pool")
}

fn admit(
    fixture: &mut ContributionFixture,
    public: &PublicWorkerDbPool,
) -> public_repo::AdmittedRelease {
    let release = ContributionRelease::release(
        ContributionPolicy::Manual,
        "legacy-compatibility-enclave-v1".to_owned(),
        RightsProvenance::new("fixture rights".to_owned(), None, None, None, None)
            .expect("legacy fixture rights"),
        ScanOutcome::Passed,
        ScanOutcome::Passed,
        vec![ReleaseSource::Memory(MemoryId(fixture.memory))],
    )
    .expect("legacy compatibility release shape");
    let release_id = fixture
        .rt
        .block_on(contribution_repo::create_release(
            &fixture.private,
            fixture.auth.tenant_id(),
            &release,
        ))
        .expect("legacy expand-window release");

    // The current public worker must not resolve any release, even an expand-window legacy
    // release.  The enclave below represents an already-admitted historical legacy root.
    assert_eq!(
        fixture.rt.block_on(public_repo::admit_release(
            public,
            fixture.auth.tenant_id(),
            release_id,
        )),
        Err(ErrorCode::Forbidden),
        "the public worker cannot bypass the protected release boundary"
    );

    let mut tx = fixture
        .admin
        .transaction()
        .expect("legacy enclave transaction");
    tx.batch_execute("SET LOCAL ROLE role_migration_owner")
        .expect("legacy enclave owner");
    let source_id: Uuid = tx
        .query_one(
            "INSERT INTO public.sources( \
               source_type,content_hash,rights_basis,trust_class,contribution_release_id) \
             VALUES('USER_CONTRIBUTION',$1,'fixture rights','UNDER_REVIEW',$2) \
             RETURNING source_id",
            &[&"legacy-compatibility-enclave-content-v1", &release_id],
        )
        .expect("legacy compatibility source")
        .get(0);
    let claim_id: Uuid = tx
        .query_one(
            "INSERT INTO public.claims(content,moderation_state,intake_release_id) \
             VALUES('{\"text\":\"legacy compatibility enclave\"}'::jsonb,'UNDER_REVIEW',$1) \
             RETURNING claim_id",
            &[&release_id],
        )
        .expect("legacy compatibility claim")
        .get(0);
    tx.execute(
        "INSERT INTO public.provenance_edges(claim_id,source_id) VALUES($1,$2)",
        &[&claim_id, &source_id],
    )
    .expect("legacy direct provenance");
    tx.execute(
        "INSERT INTO public.source_closure(claim_id,root_source_id,depth,is_current) \
         VALUES($1,$2,1,true)",
        &[&claim_id, &source_id],
    )
    .expect("legacy root closure");
    tx.commit().expect("legacy compatibility enclave commit");

    let anonymous_bindings: i64 = fixture
        .admin
        .query_one(
            "SELECT count(*) FROM control.anonymous_source_lineage \
             WHERE contribution_release_id=$1",
            &[&release_id],
        )
        .expect("legacy enclave is distinct from anonymous topology")
        .get(0);
    assert_eq!(anonymous_bindings, 0);
    public_repo::AdmittedRelease {
        release_id,
        source_id,
        claim_id,
        object_revision: 1,
    }
}

fn body_hash(fixture: &mut ContributionFixture, claim_id: Uuid) -> Vec<u8> {
    fixture
        .admin
        .query_one(
            "SELECT sha256(convert_to(content::text,'UTF8')) FROM public.claims WHERE claim_id=$1",
            &[&claim_id],
        )
        .expect("claim body")
        .get(0)
}

fn review(
    fixture: &mut ContributionFixture,
    public: &PublicWorkerDbPool,
    claim_id: Uuid,
    expected_revision: i64,
    expected_body_sha256: &[u8],
    target_state: ModerationState,
) -> Result<public_repo::EvaluationResult, ErrorCode> {
    fixture.rt.block_on(public_repo::evaluate_claim(
        public,
        &fixture.auth,
        &EvaluateClaim {
            claim_id,
            expected_revision,
            expected_body_sha256,
            policy_version: "public-trust-acceptance-v1",
            rationale: "real PostgreSQL public-trust acceptance",
            target_state,
        },
    ))
}

fn grant_moderator(fixture: &mut ContributionFixture) {
    fixture
        .admin
        .execute(
            "INSERT INTO control.public_moderator_grants(user_id,grant_version,enabled) \
             VALUES($1,1,true) ON CONFLICT(user_id) DO UPDATE \
             SET grant_version=EXCLUDED.grant_version,enabled=EXCLUDED.enabled",
            &[&fixture.auth.user_id().expect("fixture user").0],
        )
        .expect("admin global moderator grant");
}

fn counts(fixture: &mut ContributionFixture, claim_id: Uuid) -> (i64, i64, i64, i64) {
    let row = fixture
        .admin
        .query_one(
            "SELECT \
              (SELECT count(*) FROM public.claim_trust_evaluations WHERE claim_id=$1), \
              (SELECT count(*) FROM public.claim_trust_evaluation_sources r \
                JOIN public.claim_trust_evaluations e USING(evaluation_id) WHERE e.claim_id=$1), \
              (SELECT count(*) FROM public.poisoning_signals p \
                JOIN public.claim_trust_evaluations e USING(evaluation_id) WHERE e.claim_id=$1), \
              (SELECT count(*) FROM ops.outbox WHERE public_claim_id=$1)",
            &[&claim_id],
        )
        .expect("trust counts");
    (row.get(0), row.get(1), row.get(2), row.get(3))
}

fn identity(fixture: &mut ContributionFixture, claim_id: Uuid) -> ProjectionIdentity {
    let row = fixture
        .admin
        .query_one(
            "SELECT claim_id,object_revision,current_evaluation_id,sha256(convert_to(content::text,'UTF8')) \
             FROM public.claims WHERE claim_id=$1",
            &[&claim_id],
        )
        .expect("supported identity");
    ProjectionIdentity {
        object_id: row.get(0),
        object_kind: "CLAIM".into(),
        object_revision: row.get(1),
        evaluation_id: row.get(2),
        body_sha256: row.get::<_, Vec<u8>>(3).try_into().expect("sha256 length"),
    }
}

fn legacy_receipt_source_gate(candidate: &str, canonical_conditions: &[(&str, usize)]) -> bool {
    canonical_conditions
        .iter()
        .all(|(needle, count)| candidate.matches(needle).count() == *count)
        && candidate
            .matches("WITH (security_barrier=true,security_invoker=false)")
            .count()
            == 2
        && candidate
            .matches("REVOKE ALL ON public._legacy_receipt_match_basis")
            .count()
            == 1
        && candidate
            .matches("RETURNS boolean LANGUAGE sql STABLE SECURITY INVOKER")
            .count()
            == 1
        && candidate
            .matches("REVOKE ALL ON FUNCTION public.public_receipt_matches")
            .count()
            == 1
        && candidate
            .split("CREATE OR REPLACE FUNCTION public.public_receipt_matches")
            .nth(1)
            .and_then(|matcher| {
                matcher
                    .split("ALTER FUNCTION public.public_receipt_matches")
                    .next()
            })
            .is_some_and(|matcher| {
                !matcher.contains("claim_trust_evaluations")
                    && !matcher.contains("claim_trust_evaluation_sources")
                    && !matcher.contains("poisoning_signals")
            })
        && candidate
            .split("CREATE OR REPLACE VIEW public.eligible_objects")
            .nth(1)
            .and_then(|eligible| eligible.split("ALTER VIEW public.eligible_objects").next())
            .is_some_and(|eligible| !eligible.contains("public.public_receipt_matches"))
}

#[test]
fn legacy_receipt_basis_source_mutations_are_red() {
    let migration = include_str!("../../../migrations/0135_phase9_legacy_trust_trigger_acl.sql");
    let canonical_conditions = [
        (
            "WHERE EXISTS(\n      SELECT 1 FROM public.claim_trust_evaluation_sources",
            1,
        ),
        ("AND evaluation.support_count=(", 1),
        (
            "FROM public.current_public_roots(evaluation.claim_id,evaluation.synthesis_id)",
            2,
        ),
        ("\n      EXCEPT\n", 4),
        (
            "closure_root.claim_id IS NOT DISTINCT FROM evaluation.claim_id",
            2,
        ),
        (
            "receipt_root.source_content_hash IS DISTINCT FROM source.content_hash",
            1,
        ),
        (
            "receipt_root.contribution_release_id IS DISTINCT FROM source.contribution_release_id",
            1,
        ),
        ("JOIN ops.public_release_revocations revocation", 1),
        ("JOIN public._legacy_receipt_match_basis receipt_basis", 2),
    ];
    assert!(legacy_receipt_source_gate(migration, &canonical_conditions));
    for (condition, _) in canonical_conditions {
        let mutated = migration.replacen(condition, "/* predicate condition removed */", 1);
        assert!(
            !legacy_receipt_source_gate(&mutated, &canonical_conditions),
            "removing canonical condition must turn the source gate red: {condition}"
        );
    }
    for mutated in [
        migration.replacen(
            "WITH (security_barrier=true,security_invoker=false)",
            "WITH (security_barrier=true,security_invoker=true)",
            1,
        ),
        migration.replacen(
            "REVOKE ALL ON public._legacy_receipt_match_basis",
            "GRANT SELECT ON public._legacy_receipt_match_basis",
            1,
        ),
        migration.replacen(
            "RETURNS boolean LANGUAGE sql STABLE SECURITY INVOKER",
            "RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER",
            1,
        ),
        migration.replacen(
            "REVOKE ALL ON FUNCTION public.public_receipt_matches",
            "GRANT EXECUTE ON FUNCTION public.public_receipt_matches",
            1,
        ),
        migration.replacen(
            "ON receipt_basis.claim_id=claim.claim_id",
            "ON public.public_receipt_matches(claim.claim_id,NULL,claim.current_evaluation_id,claim.object_revision,claim.content)\n+   AND receipt_basis.claim_id=claim.claim_id",
            1,
        ),
    ] {
        assert!(!legacy_receipt_source_gate(&mutated, &canonical_conditions));
    }
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL and pinned real Gitleaks"]
fn non_moderator_cannot_change_any_manual_review_state() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for state in [
        ModerationState::UnderReview,
        ModerationState::Quarantined,
        ModerationState::Supported,
    ] {
        let mut fixture = ContributionFixture::new();
        let public = public_pool(&fixture);
        let admitted = admit(&mut fixture, &public);
        let before = counts(&mut fixture, admitted.claim_id);
        let hash = body_hash(&mut fixture, admitted.claim_id);
        assert_eq!(
            review(
                &mut fixture,
                &public,
                admitted.claim_id,
                admitted.object_revision,
                &hash,
                state,
            ),
            Err(ErrorCode::Forbidden),
            "{state:?} requires a global moderator"
        );
        assert_eq!(counts(&mut fixture, admitted.claim_id), before);
    }
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL and pinned real Gitleaks"]
fn active_global_moderator_persists_complete_immutable_supported_receipt() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = ContributionFixture::new();
    let public = public_pool(&fixture);
    let admitted = admit(&mut fixture, &public);
    grant_moderator(&mut fixture);
    let hash = body_hash(&mut fixture, admitted.claim_id);
    let result = review(
        &mut fixture,
        &public,
        admitted.claim_id,
        admitted.object_revision,
        &hash,
        ModerationState::Supported,
    )
    .expect("active moderator support");
    assert_eq!(result.moderation_state, "SUPPORTED");
    let row = fixture.admin.query_one(
        "SELECT e.support_count, \
           (SELECT count(*) FROM public.claim_trust_evaluation_sources r WHERE r.evaluation_id=e.evaluation_id), \
           (SELECT count(*) FROM public.poisoning_signals p WHERE p.evaluation_id=e.evaluation_id), \
           e.evaluator_grant_version \
         FROM public.claim_trust_evaluations e WHERE e.evaluation_id=$1",
        &[&result.evaluation_id],
    ).expect("sealed receipt");
    assert_eq!(row.get::<_, i32>(0) as i64, row.get::<_, i64>(1));
    assert_eq!(row.get::<_, i64>(2), 2, "both poisoning signals persist");
    assert_eq!(row.get::<_, i64>(3), 1);
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL and pinned real Gitleaks"]
fn strict_gateway_and_retrieval_hydration_reject_each_wrong_identity_field() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = ContributionFixture::new();
    let public = public_pool(&fixture);
    let admitted = admit(&mut fixture, &public);
    grant_moderator(&mut fixture);
    let hash = body_hash(&mut fixture, admitted.claim_id);
    review(
        &mut fixture,
        &public,
        admitted.claim_id,
        admitted.object_revision,
        &hash,
        ModerationState::Supported,
    )
    .unwrap();
    let expected = identity(&mut fixture, admitted.claim_id);
    let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG");
    let gateway = fixture
        .rt
        .block_on(RuntimeDbPool::connect(&dsn_as_role(&dsn, "role_gateway")))
        .unwrap();
    let retrieval = fixture
        .rt
        .block_on(RetrievalWorkerDbPool::connect(&dsn_as_role(
            &dsn,
            "role_retrieval_worker",
        )))
        .unwrap();
    let mut public_runtime =
        Client::connect(&dsn_as_role(&dsn, "role_public_worker"), NoTls).unwrap();
    assert!(
        public_runtime
            .query_opt(
                "SELECT object_id FROM public.eligible_objects \
                 WHERE object_id=$1 AND object_kind=$2 AND object_revision=$3 \
                   AND current_evaluation_id=$4 AND sha256(convert_to(content::text,'UTF8'))=$5",
                &[
                    &expected.object_id,
                    &expected.object_kind,
                    &expected.object_revision,
                    &expected.evaluation_id,
                    &&expected.body_sha256[..],
                ],
            )
            .unwrap()
            .is_some(),
        "public worker hydrates only through the owner-rights eligible view"
    );
    assert!(
        fixture
            .rt
            .block_on(public_repo::hydrate_gateway(&gateway, &expected))
            .unwrap()
            .is_some()
    );
    assert!(
        fixture
            .rt
            .block_on(public_repo::hydrate_retrieval(&retrieval, &expected))
            .unwrap()
            .is_some()
    );
    let mut wrong = vec![expected.clone(); 4];
    wrong[0].object_revision += 1;
    wrong[1].evaluation_id = Uuid::now_v7();
    wrong[2].body_sha256[0] ^= 1;
    wrong[3].object_kind = "SYNTHESIS".into();
    for bad in wrong {
        assert!(
            fixture
                .rt
                .block_on(public_repo::hydrate_gateway(&gateway, &bad))
                .unwrap()
                .is_none()
        );
        assert!(
            fixture
                .rt
                .block_on(public_repo::hydrate_retrieval(&retrieval, &bad))
                .unwrap()
                .is_none()
        );
    }
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL and pinned real Gitleaks"]
fn quarantine_is_durable_but_not_retrievable_and_revoked_or_disabled_moderator_cannot_mutate() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for change in ["disabled", "membership"] {
        let mut fixture = ContributionFixture::new();
        let public = public_pool(&fixture);
        let admitted = admit(&mut fixture, &public);
        grant_moderator(&mut fixture);
        if change == "disabled" {
            fixture
                .admin
                .execute(
                    "UPDATE control.public_moderator_grants SET enabled=false WHERE user_id=$1",
                    &[&fixture.auth.user_id().unwrap().0],
                )
                .unwrap();
        } else {
            fixture.admin.execute("UPDATE control.memberships SET state='SUSPENDED' WHERE tenant_id=$1 AND user_id=$2", &[&fixture.auth.tenant_id().0, &fixture.auth.user_id().unwrap().0]).unwrap();
        }
        let hash = body_hash(&mut fixture, admitted.claim_id);
        assert_eq!(
            review(
                &mut fixture,
                &public,
                admitted.claim_id,
                admitted.object_revision,
                &hash,
                ModerationState::Supported
            ),
            Err(ErrorCode::Forbidden)
        );
    }
    let mut fixture = ContributionFixture::new();
    let public = public_pool(&fixture);
    let admitted = admit(&mut fixture, &public);
    grant_moderator(&mut fixture);
    let hash = body_hash(&mut fixture, admitted.claim_id);
    let result = review(
        &mut fixture,
        &public,
        admitted.claim_id,
        admitted.object_revision,
        &hash,
        ModerationState::Quarantined,
    )
    .unwrap();
    assert_eq!(result.moderation_state, "QUARANTINED");
    let expected = identity(&mut fixture, admitted.claim_id);
    let dsn = std::env::var("HUMAUX_TEST_PG_DSN").unwrap();
    let gateway = fixture
        .rt
        .block_on(RuntimeDbPool::connect(&dsn_as_role(&dsn, "role_gateway")))
        .unwrap();
    assert!(
        fixture
            .rt
            .block_on(public_repo::hydrate_gateway(&gateway, &expected))
            .unwrap()
            .is_none()
    );
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL and pinned real Gitleaks"]
fn sealed_receipts_reject_runtime_rewrite_or_root_append_and_stale_requests_leave_no_trace() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut fixture = ContributionFixture::new();
    let public = public_pool(&fixture);
    let admitted = admit(&mut fixture, &public);
    grant_moderator(&mut fixture);
    let hash = body_hash(&mut fixture, admitted.claim_id);
    let supported = review(
        &mut fixture,
        &public,
        admitted.claim_id,
        admitted.object_revision,
        &hash,
        ModerationState::Supported,
    )
    .unwrap();
    let sealed = counts(&mut fixture, admitted.claim_id);
    let dsn = std::env::var("HUMAUX_TEST_PG_DSN").unwrap();
    let mut runtime = Client::connect(&dsn_as_role(&dsn, "role_public_worker"), NoTls).unwrap();
    assert_runtime_receipt_acl(&mut fixture, &mut runtime, &admitted, &supported);
    assert_runtime_receipt_relations(&dsn, &admitted);
    assert_mismatched_receipt_leaves_no_trace(
        &mut fixture,
        &mut runtime,
        &admitted,
        &supported,
        &hash,
    );
    assert!(runtime
        .execute(
            "UPDATE public.claim_trust_evaluations SET rationale='rewrite' WHERE evaluation_id=$1",
            &[&supported.evaluation_id]
        )
        .is_err());
    let root: Uuid = fixture.admin.query_one("SELECT root_source_id FROM public.claim_trust_evaluation_sources WHERE evaluation_id=$1", &[&supported.evaluation_id]).unwrap().get(0);
    let mut append = runtime.transaction().unwrap();
    append.execute("INSERT INTO public.claim_trust_evaluation_sources(evaluation_id,root_source_id,source_content_hash) VALUES($1,$2,'append')", &[&supported.evaluation_id, &root]).expect_err("sealed receipt rejects duplicate root");
    append.rollback().unwrap();
    assert_eq!(
        review(
            &mut fixture,
            &public,
            admitted.claim_id,
            supported.object_revision - 1,
            &hash,
            ModerationState::Quarantined
        ),
        Err(ErrorCode::Conflict)
    );
    let stale_hash = [0u8; 32];
    assert_eq!(
        review(
            &mut fixture,
            &public,
            admitted.claim_id,
            supported.object_revision,
            &stale_hash,
            ModerationState::Quarantined
        ),
        Err(ErrorCode::Conflict)
    );
    assert_eq!(counts(&mut fixture, admitted.claim_id), sealed);
}

fn assert_runtime_receipt_acl(
    fixture: &mut ContributionFixture,
    runtime: &mut Client,
    admitted: &public_repo::AdmittedRelease,
    supported: &public_repo::EvaluationResult,
) {
    let acl = runtime
        .query_one(
            "SELECT NOT has_function_privilege(current_user, \
               'public.require_trust_root_seal()','EXECUTE'), \
                    NOT has_function_privilege(current_user, \
               'public.guard_evaluated_source_identity()','EXECUTE'), \
                    NOT has_function_privilege(current_user, \
               'public.public_receipt_matches(uuid,uuid,uuid,bigint,jsonb)','EXECUTE')",
            &[],
        )
        .unwrap();
    for index in 0..3 {
        assert!(acl.get::<_, bool>(index));
    }
    for call in [
        "SELECT public.require_trust_root_seal()",
        "SELECT public.guard_evaluated_source_identity()",
    ] {
        assert!(
            runtime.query(call, &[]).is_err(),
            "runtime direct call: {call}"
        );
    }
    let predicate_error = runtime
        .query(
            "SELECT public.public_receipt_matches(NULL,NULL,NULL,1,'{}'::jsonb)",
            &[],
        )
        .expect_err("direct legacy receipt predicate must not return a boolean");
    assert_eq!(
        predicate_error.code().map(postgres::error::SqlState::code),
        Some("42501")
    );
    let predicate = fixture
        .admin
        .query_one(
            "SELECT NOT proc.prosecdef,proc.provolatile='s' \
             FROM pg_proc proc WHERE proc.oid= \
               'public.public_receipt_matches(uuid,uuid,uuid,bigint,jsonb)'::regprocedure",
            &[],
        )
        .unwrap();
    assert!(predicate.get::<_, bool>(0));
    assert!(predicate.get::<_, bool>(1));
    let mut owner_context = fixture.admin.transaction().unwrap();
    owner_context
        .batch_execute("SET LOCAL ROLE role_migration_owner")
        .unwrap();
    let owner_match: bool = owner_context
        .query_one(
            "SELECT public.public_receipt_matches($1,NULL,$2,$3,content) \
             FROM public.claims WHERE claim_id=$1",
            &[
                &admitted.claim_id,
                &supported.evaluation_id,
                &supported.object_revision,
            ],
        )
        .expect("trusted owner-context matcher remains usable")
        .get(0);
    assert!(owner_match);
    owner_context.commit().unwrap();
}

fn assert_runtime_receipt_relations(dsn: &str, admitted: &public_repo::AdmittedRelease) {
    for role in [
        "role_gateway",
        "role_public_worker",
        "role_retrieval_worker",
    ] {
        let mut denied = Client::connect(&dsn_as_role(dsn, role), NoTls).unwrap();
        for relation in [
            "public.claim_trust_evaluations",
            "public.claim_trust_evaluation_sources",
            "public.poisoning_signals",
            "public._legacy_receipt_match_basis",
        ] {
            let error = denied
                .query(&format!("SELECT 1 FROM {relation} LIMIT 1"), &[])
                .unwrap_err();
            assert_eq!(
                error.code().map(postgres::error::SqlState::code),
                Some("42501")
            );
        }
        let roots: i64 = denied
            .query_one(
                "SELECT count(*) FROM public.current_public_roots($1,NULL)",
                &[&admitted.claim_id],
            )
            .expect("current_public_roots remains INVOKER with a working runtime ACL path")
            .get(0);
        assert_eq!(roots, 1);
    }
}

fn assert_mismatched_receipt_leaves_no_trace(
    fixture: &mut ContributionFixture,
    runtime: &mut Client,
    admitted: &public_repo::AdmittedRelease,
    supported: &public_repo::EvaluationResult,
    hash: &[u8],
) {
    let mismatch_id = Uuid::now_v7();
    let mut mismatch = runtime.transaction().unwrap();
    mismatch
        .execute(
            "INSERT INTO public.claim_trust_evaluations( \
               evaluation_id,claim_id,object_revision,body_sha256,policy_version,moderation_state, \
               rationale,support_count,independent_support_count,trusted_source_count, \
               identity_incomplete,contradiction_count,checks_complete) \
             VALUES($1,$2,$3,$4,'public-trust-acl-v1','UNDER_REVIEW', \
                    'deferred seal mismatch',1,1,1,false,0,false)",
            &[
                &mismatch_id,
                &admitted.claim_id,
                &(supported.object_revision + 1),
                &hash,
            ],
        )
        .expect("stage mismatched legacy receipt");
    let mismatch_error = mismatch
        .commit()
        .expect_err("deferred seal mismatch must fail at commit");
    assert_eq!(
        mismatch_error.code().map(postgres::error::SqlState::code),
        Some("23514"),
        "the invariant, not a SELECT ACL, rejects the mismatch"
    );
    let partial: i64 = fixture
        .admin
        .query_one(
            "SELECT count(*) FROM public.claim_trust_evaluations WHERE evaluation_id=$1",
            &[&mismatch_id],
        )
        .unwrap()
        .get(0);
    assert_eq!(
        partial, 0,
        "failed deferred transaction leaves no partial receipt"
    );
}
