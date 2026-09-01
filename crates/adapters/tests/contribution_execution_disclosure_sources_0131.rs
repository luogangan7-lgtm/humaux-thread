//! Live proof that each R4-A/B reservation atomically records its exact source disclosure.

#[path = "support/contribution_fixture.rs"]
mod contribution_fixture;

use contribution_fixture::ContributionFixture;
use postgres::{Client, Row};
use serde_json::json;
use uuid::Uuid;

struct Execution {
    tenant: Uuid,
    user: Uuid,
    domain: Uuid,
    binding: Uuid,
    policy: Uuid,
    evidence: Uuid,
    memory: Uuid,
    execution: Uuid,
    job: Uuid,
    coverage_request: Uuid,
    assessment_request: Uuid,
    candidate: Uuid,
    key: String,
}

fn source_manifest(db: &mut Client, evidence: Uuid, memory: Uuid) -> (Vec<u8>, Vec<Vec<u8>>) {
    let row = db
        .query_one(
            "WITH source(kind,source_id,source_hash) AS (\
             SELECT 'e',evidence_id,payload_sha256 FROM private.evidence_objects WHERE evidence_id=$1 \
             UNION ALL \
             SELECT 'm',memory_id,sha256(convert_to(content::text,'UTF8')) \
             FROM private.memory_records WHERE memory_id=$2) \
             SELECT sha256(convert_to(string_agg(kind||':'||source_id::text||':'||encode(source_hash,'hex'),\
             '|' ORDER BY kind,source_id),'UTF8')),array_agg(source_hash ORDER BY kind,source_id) \
             FROM source",
            &[&evidence, &memory],
        )
        .expect("canonical current evidence/memory manifest");
    (row.get(0), row.get(1))
}

fn enqueue(
    db: &mut Client,
    execution: &Execution,
    manifest_hash: &[u8],
    source_hashes: &[Vec<u8>],
) -> Row {
    let policy_snapshot = json!({
        "policy": "MANUAL",
        "principal_id": execution.user.to_string(),
        "allowed_workspace_ids": []
    });
    db.query_one(
        "SELECT * FROM private.enqueue_contribution_execution(\
         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,1,$13,$14,NULL,NULL,NULL,NULL,\
         $15,1,1,1,$16,$17,$18,$19,$20)",
        &[
            &execution.tenant,
            &execution.execution,
            &execution.job,
            &execution.coverage_request,
            &execution.assessment_request,
            &execution.candidate,
            &execution.key,
            &vec![0x91_u8; 32],
            &execution.user,
            &execution.domain,
            &manifest_hash,
            &execution.policy,
            &policy_snapshot,
            &"USER_CONSENT",
            &execution.binding,
            &vec![0x71_u8; 32],
            &vec![0x72_u8; 32],
            &vec!["EVIDENCE".to_owned(), "MEMORY".to_owned()],
            &vec![execution.evidence, execution.memory],
            &source_hashes,
        ],
    )
    .expect("enqueue exact two-source execution")
}

fn claim(db: &mut Client, execution: &Execution, owner: &str) -> i32 {
    db.query_one(
        "UPDATE ops.jobs SET status='PROCESSING',attempt=attempt+1,lease_owner=$2,\
         lease_expires_at=clock_timestamp()+interval '10 minutes'\
         WHERE job_id=$1 RETURNING attempt",
        &[&execution.job, &owner],
    )
    .expect("fresh lease")
    .get(0)
}

fn prepared_route(db: &mut Client, execution: &Execution) -> serde_json::Value {
    db.query_one(
        "SELECT jsonb_build_object(\
         'schema_version',1,'tenant_id',admission.tenant_id,'binding_id',admission.binding_id,\
         'binding_version',admission.binding_version,'reasoning_domain_id',admission.reasoning_domain_id,\
         'purpose',admission.purpose,'route_policy_id',admission.route_policy_id,\
         'route_policy_version',admission.route_policy_version,'profile_id',admission.profile_id,\
         'profile_version',admission.profile_version,'provider_account_id',admission.provider_account_id,\
         'processor_id',admission.processor_id,'processor_model_id',admission.processor_model_id,\
         'provider_model_id',admission.provider_model_id,'model_revision',admission.model_revision,\
         'provider_endpoint_id',admission.provider_endpoint_id,'egress_processor_id',admission.egress_processor_id,\
         'endpoint_ref',admission.endpoint_ref,'region',admission.region,'service_tier',admission.service_tier,\
         'credential_ref',admission.credential_ref,'billing_account_id',admission.billing_account_id,\
         'billing_instrument_id',admission.billing_instrument_id) \
         FROM control.resolve_user_reasoning_admission($1,1,$2,'CONTRIBUTION_DEIDENTIFY') admission",
        &[&execution.binding, &execution.domain],
    )
    .expect("current route expectation")
    .get(0)
}

fn reserve(
    db: &mut Client,
    function: &str,
    execution: &Execution,
    owner: &str,
    attempt: i32,
    intent: &[u8],
) -> (Uuid, Uuid, bool) {
    let route = prepared_route(db, execution);
    let sql = format!(
        "SELECT * FROM private.{function}(\
         $1,$2,$3,$4,$5,$6,$7,$8,$9,NULL,NULL,'PRIVATE',$10,17,$11)"
    );
    let row = db
        .query_one(
            &sql,
            &[
                &execution.tenant,
                &execution.execution,
                &execution.job,
                &owner,
                &attempt,
                &Uuid::new_v4(),
                &Uuid::new_v4(),
                &intent,
                &Uuid::new_v4(),
                &vec![0xc1_u8; 32],
                &route,
            ],
        )
        .expect("typed reserve");
    (row.get(0), row.get(1), row.get(8))
}

fn assert_exact_sources(db: &mut Client, disclosure: Uuid, evidence: Uuid, memory: Uuid) {
    let row = db
        .query_one(
            "SELECT count(*),array_agg(source_kind||':'||coalesce(evidence_id,memory_id)::text ORDER BY ordinal),\
             array_agg(ordinal ORDER BY ordinal) FROM ops.data_disclosure_sources WHERE disclosure_id=$1",
            &[&disclosure],
        )
        .expect("durable disclosure sources");
    assert_eq!(row.get::<_, i64>(0), 2, "nonempty exact source count");
    assert_eq!(
        row.get::<_, Vec<String>>(1),
        vec![format!("EVIDENCE:{evidence}"), format!("MEMORY:{memory}")]
    );
    assert_eq!(
        row.get::<_, Vec<i32>>(2),
        vec![0, 1],
        "frozen source ordering"
    );
}

#[test]
#[ignore = "requires isolated PostgreSQL 18 migrated through 0131"]
#[allow(
    clippy::too_many_lines,
    reason = "single integration scenario covers atomic ordered disclosure"
)]
fn r4_a4_reservations_atomically_disclose_exact_ordered_sources_without_duplicates() {
    let mut fixture = ContributionFixture::new();
    let tenant: Uuid = fixture
        .admin
        .query_one(
            "SELECT tenant_id FROM private.memory_records WHERE memory_id=$1",
            &[&fixture.memory],
        )
        .expect("fixture tenant")
        .get(0);
    let user: Uuid = fixture
        .admin
        .query_one(
            "SELECT owner_user_id FROM control.private_reasoning_domains WHERE reasoning_domain_id=$1",
            &[&fixture.domain],
        )
        .expect("fixture user")
        .get(0);
    let policy: Uuid = fixture
        .admin
        .query_one(
            "SELECT policy_id FROM control.contribution_policies WHERE tenant_id=$1",
            &[&tenant],
        )
        .expect("fixture policy")
        .get(0);
    let evidence: Uuid = fixture
        .admin
        .query_one(
            "SELECT evidence_id FROM private.memory_evidence WHERE memory_id=$1",
            &[&fixture.memory],
        )
        .expect("fixture evidence")
        .get(0);
    let execution = Execution {
        tenant,
        user,
        domain: fixture.domain,
        binding: fixture.binding,
        policy,
        evidence,
        memory: fixture.memory,
        execution: Uuid::new_v4(),
        job: Uuid::new_v4(),
        coverage_request: Uuid::new_v4(),
        assessment_request: Uuid::new_v4(),
        candidate: Uuid::new_v4(),
        key: format!("r4-a4-disclosure-sources-{}", Uuid::new_v4()),
    };
    let (manifest_hash, source_hashes) =
        source_manifest(&mut fixture.admin, evidence, fixture.memory);
    assert!(
        enqueue(
            &mut fixture.admin,
            &execution,
            &manifest_hash,
            &source_hashes
        )
        .get::<_, bool>(5)
    );

    let a_attempt = claim(&mut fixture.admin, &execution, "r4-a4-a");
    let a_intent = vec![0xd1_u8; 32];
    let (a_call, a_disclosure, a_new) = reserve(
        &mut fixture.admin,
        "reserve_contribution_a",
        &execution,
        "r4-a4-a",
        a_attempt,
        &a_intent,
    );
    assert!(a_new, "A grants its one dispatch permit");
    assert_exact_sources(&mut fixture.admin, a_disclosure, evidence, fixture.memory);
    let (_, duplicate_a_disclosure, duplicate_a_new) = reserve(
        &mut fixture.admin,
        "reserve_contribution_a",
        &execution,
        "r4-a4-a",
        a_attempt,
        &a_intent,
    );
    assert_eq!(duplicate_a_disclosure, a_disclosure);
    assert!(
        !duplicate_a_new,
        "existing A does not grant a second permit"
    );
    assert_exact_sources(&mut fixture.admin, a_disclosure, evidence, fixture.memory);

    let b_attempt = claim(&mut fixture.admin, &execution, "r4-a4-b");
    let a_receipt = json!({"receipt":"a"});
    let a_receipt_hash: Vec<u8> = fixture
        .admin
        .query_one(
            "SELECT sha256(convert_to($1::jsonb::text,'UTF8'))",
            &[&a_receipt],
        )
        .expect("receipt hash")
        .get(0);
    fixture
        .admin
        .query_one(
            "SELECT private.complete_contribution_a_exact(\
             $1,$2,$3,$4,$5,$6,'USABLE',$7,$8,1,$9,$10,$11,$12,'a-trace','a-request',NULL,$13,$14,$15)",
            &[
                &execution.tenant,
                &execution.execution,
                &a_call,
                &execution.coverage_request,
                &a_intent,
                &a_disclosure,
                &vec![0xd2_u8; 32],
                &Uuid::new_v4(),
                &b"coverage".to_vec(),
                &vec![0xd3_u8; 32],
                &a_receipt,
                &a_receipt_hash,
                &execution.job,
                &"r4-a4-b",
                &b_attempt,
            ],
        )
        .expect("A completion before B reserve");
    let b_intent = vec![0xe1_u8; 32];
    let (_, b_disclosure, b_new) = reserve(
        &mut fixture.admin,
        "reserve_contribution_b",
        &execution,
        "r4-a4-b",
        b_attempt,
        &b_intent,
    );
    assert!(b_new, "B grants its one dispatch permit");
    assert_exact_sources(&mut fixture.admin, b_disclosure, evidence, fixture.memory);
    let (_, duplicate_b_disclosure, duplicate_b_new) = reserve(
        &mut fixture.admin,
        "reserve_contribution_b",
        &execution,
        "r4-a4-b",
        b_attempt,
        &b_intent,
    );
    assert_eq!(duplicate_b_disclosure, b_disclosure);
    assert!(
        !duplicate_b_new,
        "existing B does not grant a second permit"
    );
    assert_exact_sources(&mut fixture.admin, b_disclosure, evidence, fixture.memory);
}
