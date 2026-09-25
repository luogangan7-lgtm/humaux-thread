//! Real-PostgreSQL acceptance for the protected Phase 9 anti-Sybil aggregate.
//! The fixture uses the real assessed prepare -> confirm -> finalize -> anonymous admit path.

#[path = "support/contribution_fixture.rs"]
mod contribution_fixture;

use std::sync::{Arc, Barrier, Mutex};

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
use postgres::{Client, NoTls};
use sha2::{Digest, Sha256};
use uuid::Uuid;

static SERIAL: Mutex<()> = Mutex::new(());

#[derive(Clone)]
struct AssessmentReasoner {
    candidate: Vec<u8>,
}

#[async_trait]
impl UserContributionAssessmentPort for AssessmentReasoner {
    async fn derive_coverage_probe(
        &self,
        request: ContributionCoverageProbeRequest,
    ) -> Result<ContributionCoverageProbe, ErrorCode> {
        let probe = b"phase9 independence acceptance".to_vec();
        ContributionCoverageProbe::new(
            request.reasoning.input_manifest_hash,
            probe.clone(),
            payload_sha256(&probe),
            ProviderTraceRef("offline-independence-probe".into()),
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
                output_sha256: ContentSha256(Sha256::digest(&self.candidate).into()),
                output_bytes: self.candidate.clone(),
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
struct Root {
    source_id: Uuid,
    envelope: Vec<u8>,
    claim_id: Uuid,
    release_id: Uuid,
}

#[derive(Debug)]
struct Attestation {
    id: Uuid,
    support: i32,
    independent: i32,
    risk: String,
}

fn dsn_as_role(dsn: &str, role: &str) -> String {
    format!(
        "{dsn}{}options=-c%20role%3D{role}",
        if dsn.contains('?') { '&' } else { '?' }
    )
}

fn coverage(fixture: &mut ContributionFixture) -> PublicCoverageDigest {
    let rows = fixture
        .admin
        .query(
            "SELECT snapshot_id,coverage_version,summary \
             FROM public.phase9_public_coverage_for_probe($1,32)",
            &[&b"phase9 independence acceptance".as_slice()],
        )
        .expect("bounded public coverage");
    let first = rows
        .first()
        .expect("coverage always returns its binding row");
    PublicCoverageDigest::new(
        first.get("snapshot_id"),
        u32::try_from(first.get::<_, i32>("coverage_version")).expect("positive version"),
        rows.iter()
            .filter_map(|row| row.get::<_, Option<String>>("summary"))
            .collect(),
    )
    .expect("current coverage binding")
}

fn publish(fixture: &mut ContributionFixture, candidate: &str) -> Root {
    let current_coverage = coverage(fixture);
    let assessed = fixture
        .rt
        .block_on(contribute::prepare_assessed(
            fixture.request(),
            &AssessmentReasoner {
                candidate: candidate.as_bytes().to_vec(),
            },
            &Coverage(current_coverage),
            &fixture.scanner(),
            &ContributionEntryRepo::new(&fixture.private),
        ))
        .expect("assessed candidate");
    let confirmation = fixture.confirm(assessed);
    let release_id = fixture
        .rt
        .block_on(contribute::finalize(
            confirmation,
            &ContributionEntryRepo::new(&fixture.private),
        ))
        .expect("active assessed release")
        .0;
    let binding = fixture
        .admin
        .query_one(
            "SELECT lineage.anonymous_source_id,sealed.envelope_sha256 \
             FROM control.anonymous_source_lineage lineage \
             JOIN staging.sanitized_public_candidates sealed \
               ON sealed.tenant_id=lineage.tenant_id \
              AND sealed.anonymous_source_id=lineage.anonymous_source_id \
             WHERE lineage.contribution_release_id=$1",
            &[&release_id],
        )
        .expect("protected anonymous binding");
    let source_id: Uuid = binding.get(0);
    let envelope: Vec<u8> = binding.get(1);
    let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PostgreSQL");
    let mut public = Client::connect(&dsn_as_role(&dsn, "role_public_worker"), NoTls)
        .expect("public-worker setup connection");
    let lease_owner = "phase9-independence-attestation";
    let dispatch = public
        .query_one(
            "SELECT dispatch_id,attempt \
             FROM ops.claim_global_anonymous_public_dispatches($1,60.0,1)",
            &[&lease_owner],
        )
        .expect("claim anonymous release dispatch");
    let dispatch_id: Uuid = dispatch.get(0);
    let attempt: i32 = dispatch.get(1);
    let claim_id = public
        .query_one(
            "SELECT claim_id FROM public.admit_anonymous_dispatch($1,$2,$3)",
            &[&dispatch_id, &lease_owner, &attempt],
        )
        .expect("authorized anonymous admission through dispatch")
        .get(0);
    let identity = fixture
        .admin
        .query_one(
            "SELECT contribution_release_id IS NULL,publisher IS NULL,source_url IS NULL, \
                    verified_organization IS NULL,canonical_url IS NULL, \
                    document_fingerprint IS NULL,upstream_root IS NULL, \
                    identity_policy_version IS NULL,trusted_by_policy IS NULL \
             FROM public.sources WHERE source_id=$1",
            &[&source_id],
        )
        .expect("anonymous public root");
    for index in 0..9 {
        assert!(
            identity.get::<_, bool>(index),
            "anonymous public identity column {index} must stay NULL"
        );
    }
    Root {
        source_id,
        envelope,
        claim_id,
        release_id,
    }
}

fn merge_roots(admin: &mut Client, target: Uuid, roots: &[&Root]) {
    let revision: i64 = admin
        .query_one(
            "SELECT object_revision FROM public.claims WHERE claim_id=$1",
            &[&target],
        )
        .expect("target claim")
        .get(0);
    let mut transaction = admin.transaction().expect("root merge transaction");
    transaction
        .batch_execute("SET LOCAL ROLE role_migration_owner")
        .expect("migration-owner fixture path");
    for root in roots {
        transaction
            .execute(
                "INSERT INTO public.provenance_edges(claim_id,source_id) VALUES($1,$2) \
                 ON CONFLICT DO NOTHING",
                &[&target, &root.source_id],
            )
            .expect("direct provenance root");
        transaction
            .execute(
                "INSERT INTO public.source_closure(claim_id,root_source_id) VALUES($1,$2) \
                 ON CONFLICT DO NOTHING",
                &[&target, &root.source_id],
            )
            .expect("current root closure");
        if root.claim_id != target {
            transaction
                .execute(
                    "INSERT INTO public.anonymous_source_lifecycle_events( \
                       anonymous_source_id,candidate_envelope_sha256,claim_id,object_revision,event_type) \
                     VALUES($1,$2,$3,$4,'ADMIT') ON CONFLICT DO NOTHING",
                    &[&root.source_id, &root.envelope, &target, &revision],
                )
                .expect("exact current lifecycle");
        }
    }
    transaction.commit().expect("commit root merge");
}

fn claim_binding(admin: &mut Client, claim_id: Uuid) -> (i64, Vec<u8>) {
    let row = admin
        .query_one(
            "SELECT object_revision,sha256(convert_to(content::text,'UTF8')) \
             FROM public.claims WHERE claim_id=$1",
            &[&claim_id],
        )
        .expect("claim binding");
    (row.get(0), row.get(1))
}

fn attest(
    public: &mut Client,
    claim_id: Uuid,
    revision: i64,
    body_sha: &[u8],
) -> Result<Attestation, postgres::Error> {
    public
        .query_one(
            "SELECT attestation_id,support_count,independent_count,sybil_risk \
             FROM public.attest_current_claim_independence($1,$2,$3)",
            &[&claim_id, &revision, &body_sha],
        )
        .map(|row| Attestation {
            id: row.get(0),
            support: row.get(1),
            independent: row.get(2),
            risk: row.get(3),
        })
}

#[test]
#[ignore = "lane(a:disposable) needs a per-run database migrated through 0124: it claims from the GLOBAL ops.public_anonymous_dispatches queue, so a shared database hands it a dispatch an earlier run left PROCESSING"]
#[allow(
    clippy::too_many_lines,
    reason = "one acceptance fixture covers aggregate independence and exactly-once behavior"
)]
fn protected_independence_attestation_is_fail_closed_aggregate_and_exactly_once() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PostgreSQL through 0124");
    let role_dsn = dsn_as_role(&dsn, "role_public_worker");
    let mut public = Client::connect(&role_dsn, NoTls).expect("public-worker connection");

    let mut principal_a = ContributionFixture::new();
    let a1 = publish(&mut principal_a, "independent fixture alpha one");
    let a2 = publish(&mut principal_a, "independent fixture alpha two");
    let a3 = publish(&mut principal_a, "independent fixture alpha three");
    let mut principal_b = ContributionFixture::new();
    let b1 = publish(&mut principal_b, "independent fixture beta one");
    let b_same = publish(&mut principal_b, "independent fixture alpha one");
    let mut principal_c = ContributionFixture::new();
    let c1 = publish(&mut principal_c, "independent fixture gamma one");
    let mut principal_d = ContributionFixture::new();
    let d1 = publish(&mut principal_d, "independent fixture delta one");

    // Three roots controlled by one protected principal plus one other principal: four supports,
    // two transitive components, and no protected identity in the returned/public row.
    merge_roots(&mut principal_a.admin, a1.claim_id, &[&a1, &a2, &a3, &b1]);
    let (a_revision, a_body) = claim_binding(&mut principal_a.admin, a1.claim_id);
    let concentrated = attest(&mut public, a1.claim_id, a_revision, &a_body).expect("aggregate");
    assert_eq!((concentrated.support, concentrated.independent), (4, 2));
    assert_eq!(concentrated.risk, "CONCENTRATED");
    let replay = attest(&mut public, a1.claim_id, a_revision, &a_body).expect("exact replay");
    assert_eq!(replay.id, concentrated.id);
    let receipt_rows: i64 = principal_a
        .admin
        .query_one(
            "SELECT count(*) FROM public.claim_independence_attestations WHERE claim_id=$1",
            &[&a1.claim_id],
        )
        .unwrap()
        .get(0);
    assert_eq!(receipt_rows, 1);

    assert!(attest(&mut public, a1.claim_id, a_revision + 1, &a_body).is_err());
    let mut wrong_body = a_body.clone();
    wrong_body[0] ^= 1;
    assert!(attest(&mut public, a1.claim_id, a_revision, &wrong_body).is_err());

    // Different protected principals still collapse when their public-safe content hash is the
    // same. Content equality is an edge, not a post-hoc count adjustment.
    merge_roots(&mut principal_b.admin, b_same.claim_id, &[&b_same, &a1]);
    let (same_revision, same_body) = claim_binding(&mut principal_b.admin, b_same.claim_id);
    let duplicate = attest(&mut public, b_same.claim_id, same_revision, &same_body).unwrap();
    assert_eq!((duplicate.support, duplicate.independent), (2, 1));
    assert_eq!(duplicate.risk, "CONCENTRATED");

    // Four distinct principal/content roots stay four components. Two simultaneous public-worker
    // calls serialize to one immutable receipt and both receive the same id.
    merge_roots(&mut principal_c.admin, c1.claim_id, &[&c1, &a1, &b1, &d1]);
    let (distinct_revision, distinct_body) = claim_binding(&mut principal_c.admin, c1.claim_id);
    let distinct_claim_id = c1.claim_id;
    let barrier = Arc::new(Barrier::new(2));
    let calls = (0..2)
        .map(|_| {
            let thread_dsn = role_dsn.clone();
            let thread_barrier = Arc::clone(&barrier);
            let thread_body = distinct_body.clone();
            std::thread::spawn(move || {
                let mut connection = Client::connect(&thread_dsn, NoTls).unwrap();
                thread_barrier.wait();
                attest(
                    &mut connection,
                    distinct_claim_id,
                    distinct_revision,
                    &thread_body,
                )
                .unwrap()
            })
        })
        .collect::<Vec<_>>();
    let results = calls
        .into_iter()
        .map(|call| call.join().expect("concurrent attestation"))
        .collect::<Vec<_>>();
    assert_eq!(results[0].id, results[1].id);
    assert_eq!((results[0].support, results[0].independent), (4, 4));
    assert_eq!(results[0].risk, "NO_CONCENTRATION_OBSERVED");
    assert_eq!(
        principal_c
            .admin
            .query_one(
                "SELECT count(*) FROM public.claim_independence_attestations WHERE claim_id=$1",
                &[&c1.claim_id],
            )
            .unwrap()
            .get::<_, i64>(0),
        1
    );

    // A fully valid new root after sealing makes the old root digest stale; no second receipt is
    // created for the same object revision.
    merge_roots(&mut principal_a.admin, a1.claim_id, &[&c1]);
    assert!(attest(&mut public, a1.claim_id, a_revision, &a_body).is_err());
    assert_eq!(
        principal_a
            .admin
            .query_one(
                "SELECT count(*) FROM public.claim_independence_attestations WHERE claim_id=$1",
                &[&a1.claim_id],
            )
            .unwrap()
            .get::<_, i64>(0),
        1
    );

    // A public anonymous root with no protected lineage cannot be counted as independent.
    let missing_source = Uuid::new_v4();
    let missing_envelope = vec![71_u8; 32];
    let mut missing = principal_b.admin.transaction().unwrap();
    missing
        .batch_execute("SET LOCAL ROLE role_migration_owner")
        .unwrap();
    missing.execute(
        "INSERT INTO public.sources(source_id,source_type,content_hash,rights_basis,trust_class,lineage_mode,anonymous_envelope_sha256) \
         VALUES($1,'ANONYMOUS_USER_CONTRIBUTION',$2,'fixture','fixture','ANONYMOUS_RELEASE',$3)",
        &[&missing_source, &"missing-lineage", &missing_envelope],
    ).unwrap();
    missing
        .execute(
            "INSERT INTO public.provenance_edges(claim_id,source_id) VALUES($1,$2)",
            &[&b1.claim_id, &missing_source],
        )
        .unwrap();
    missing
        .execute(
            "INSERT INTO public.source_closure(claim_id,root_source_id) VALUES($1,$2)",
            &[&b1.claim_id, &missing_source],
        )
        .unwrap();
    missing.commit().unwrap();
    let (missing_revision, missing_body) = claim_binding(&mut principal_b.admin, b1.claim_id);
    assert!(attest(&mut public, b1.claim_id, missing_revision, &missing_body).is_err());

    // Latest protected authority REVOKE is a hard fence even if public provenance still exists.
    principal_d.admin.execute(
        "INSERT INTO public.anonymous_source_authority_events(anonymous_source_id,candidate_envelope_sha256,source_revision,event_type) \
         VALUES($1,$2,2,'REVOKE')",
        &[&d1.source_id, &d1.envelope],
    ).unwrap();
    let (revoked_revision, revoked_body) = claim_binding(&mut principal_d.admin, d1.claim_id);
    assert!(attest(&mut public, d1.claim_id, revoked_revision, &revoked_body).is_err());

    // A receipt sealed under a different algorithm policy is stale, not silently replaced.
    let (policy_revision, policy_body) = claim_binding(&mut principal_a.admin, a2.claim_id);
    let root_sha: Vec<u8> = principal_a.admin.query_one(
        "SELECT sha256(convert_to(string_agg(root_source_id::text,'|' ORDER BY root_source_id),'UTF8')) \
         FROM public.current_public_roots($1,NULL)",
        &[&a2.claim_id],
    ).unwrap().get(0);
    principal_a.admin.execute(
        "INSERT INTO public.claim_independence_attestations( \
           claim_id,object_revision,body_sha,root_set_sha,support_count,independent_count,sybil_risk, \
           policy_version,policy_digest,checks_complete) \
         VALUES($1,$2,$3,$4,1,1,'NO_CONCENTRATION_OBSERVED','stale-policy',$5,true)",
        &[&a2.claim_id, &policy_revision, &policy_body, &root_sha, &vec![99_u8; 32]],
    ).unwrap();
    assert!(attest(&mut public, a2.claim_id, policy_revision, &policy_body).is_err());

    assert!(
        public
            .query(
                "SELECT contribution_release_id FROM control.anonymous_source_lineage LIMIT 1",
                &[]
            )
            .is_err()
    );
    assert!(
        public
            .query(
                "SELECT policy_snapshot FROM staging.contribution_releases LIMIT 1",
                &[]
            )
            .is_err()
    );
    assert!(public.execute(
        "INSERT INTO public.claim_independence_attestations( \
           claim_id,object_revision,body_sha,root_set_sha,support_count,independent_count,sybil_risk, \
           policy_version,policy_digest,checks_complete) \
         VALUES($1,$2,$3,$4,1,1,'NO_CONCENTRATION_OBSERVED','forged',$5,true)",
        &[&a3.claim_id, &1_i64, &vec![1_u8; 32], &vec![2_u8; 32], &vec![3_u8; 32]],
    ).is_err());

    // Keep the release ids live in the test contract: each root above was created by a distinct
    // real assessed release rather than by inserting protected fixture shortcuts.
    assert_eq!(
        [
            a1.release_id,
            a2.release_id,
            a3.release_id,
            b1.release_id,
            b_same.release_id,
            c1.release_id,
            d1.release_id,
        ]
        .into_iter()
        .collect::<std::collections::HashSet<_>>()
        .len(),
        7
    );
}
