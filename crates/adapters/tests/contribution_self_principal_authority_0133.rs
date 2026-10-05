//! `adapters::tests::contribution_self_principal_authority_0133` — Phase 9 migration 0133: final self-principal
//!   reservation authority gate.
//! Depends-on: crates=[postgres, serde_json, uuid]; services=[PostgreSQL(any) r=[control.contribution_policies,
//!   ops.data_disclosures, ops.model_call_ledger] w=[control.private_reasoning_domains, control.tenants,
//!   control.workspace_memberships, control.workspaces, ops.jobs, ops.reasoning_provider_health_observations,
//!   private.contribution_execution_sources, private.contribution_executions, private.events,
//!   private.evidence_objects, private.memory_evidence, private.memory_records]
//!   x=[control.resolve_user_reasoning_admission, ops.guard_contribution_input_change, ops.lock_contribution_inputs,
//!   private.assert_contribution_prepared_route_shape, private.enqueue_contribution_execution,
//!   private.reserve_contribution_a]]; env=[CARGO_MANIFEST_DIR, HUMAUX_TEST_PG_DSN];
//!   modules=[adapters::tests::support::contribution_fixture]
//! Called-by: [cargo-test]
//! Invariants: [the 0133 contract is pinned statically; authority changes and deletes must wait for the reservation
//!   and then fail closed; the PG tests are #[ignore] lane tests needing PostgreSQL 18 migrated through 0133]
//! Spec: none

#[path = "support/contribution_fixture.rs"]
mod contribution_fixture;

use contribution_fixture::ContributionFixture;
use postgres::{Client, GenericClient, NoTls, Row};
use serde_json::{Value, json};
use std::{
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

#[derive(Clone)]
struct Execution {
    tenant: Uuid,
    user: Uuid,
    domain: Uuid,
    binding: Uuid,
    policy: Uuid,
    source_kind: &'static str,
    source: Uuid,
    source_hash: Vec<u8>,
    manifest_hash: Vec<u8>,
    execution: Uuid,
    job: Uuid,
    coverage_request: Uuid,
    assessment_request: Uuid,
    candidate: Uuid,
    key: String,
}

fn prepare(fixture: &mut ContributionFixture, label: &str) -> Execution {
    let tenant = fixture
        .admin
        .query_one(
            "SELECT tenant_id FROM private.memory_records WHERE memory_id=$1",
            &[&fixture.memory],
        )
        .expect("tenant")
        .get(0);
    let user = fixture.admin.query_one(
        "SELECT owner_user_id FROM control.private_reasoning_domains WHERE reasoning_domain_id=$1",
        &[&fixture.domain],
    ).expect("user").get(0);
    let policy = fixture.admin.query_one(
        "SELECT policy_id FROM control.contribution_policies WHERE tenant_id=$1 AND effective_to IS NULL",
        &[&tenant],
    ).expect("open policy").get(0);
    let source_hash: Vec<u8> = fixture.admin.query_one(
        "SELECT sha256(convert_to(content::text,'UTF8')) FROM private.memory_records WHERE memory_id=$1",
        &[&fixture.memory],
    ).expect("current source hash").get(0);
    let manifest_hash = fixture
        .admin
        .query_one(
            "SELECT sha256(convert_to('m:'||$1::uuid::text||':'||encode($2::bytea,'hex'),'UTF8'))",
            &[&fixture.memory, &source_hash],
        )
        .expect("canonical manifest")
        .get(0);
    Execution {
        tenant,
        user,
        domain: fixture.domain,
        binding: fixture.binding,
        policy,
        source_kind: "MEMORY",
        source: fixture.memory,
        source_hash,
        manifest_hash,
        execution: Uuid::new_v4(),
        job: Uuid::new_v4(),
        coverage_request: Uuid::new_v4(),
        assessment_request: Uuid::new_v4(),
        candidate: Uuid::new_v4(),
        key: format!("r4-0133-{label}-{}", Uuid::new_v4()),
    }
}

fn enqueue(db: &mut Client, e: &Execution) -> Row {
    let snapshot =
        json!({"policy":"MANUAL","principal_id":e.user.to_string(),"allowed_workspace_ids":[]});
    db.query_one(
        "SELECT * FROM private.enqueue_contribution_execution(\
         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,1,$13,$14,NULL,NULL,NULL,NULL,\
         $15,1,1,1,$16,$17,$18,$19,$20)",
        &[
            &e.tenant,
            &e.execution,
            &e.job,
            &e.coverage_request,
            &e.assessment_request,
            &e.candidate,
            &e.key,
            &vec![0x61_u8; 32],
            &e.user,
            &e.domain,
            &e.manifest_hash,
            &e.policy,
            &snapshot,
            &"USER_CONSENT",
            &e.binding,
            &vec![0x71_u8; 32],
            &vec![0x72_u8; 32],
            &vec![e.source_kind.to_owned()],
            &vec![e.source],
            &vec![e.source_hash.clone()],
        ],
    )
    .expect("enqueue")
}

fn claim(db: &mut Client, e: &Execution) -> i32 {
    db.query_one(
        "UPDATE ops.jobs SET status='PROCESSING',attempt=attempt+1,lease_owner='p9-0133',\
         lease_expires_at=clock_timestamp()+interval '10 minutes' WHERE job_id=$1 RETURNING attempt",
        &[&e.job],
    ).expect("claim").get(0)
}

fn route(db: &mut Client, e: &Execution) -> Value {
    db.query_one(
        "SELECT set_config('humaux.tenant_id',$1,false)",
        &[&e.tenant.to_string()],
    )
    .expect("tenant GUC");
    db.query_one(
        "SELECT jsonb_build_object(\
         'schema_version',1,'tenant_id',a.tenant_id,'binding_id',a.binding_id,\
         'binding_version',a.binding_version,'reasoning_domain_id',a.reasoning_domain_id,\
         'purpose',a.purpose,'route_policy_id',a.route_policy_id,\
         'route_policy_version',a.route_policy_version,'profile_id',a.profile_id,\
         'profile_version',a.profile_version,'provider_account_id',a.provider_account_id,\
         'processor_id',a.processor_id,'processor_model_id',a.processor_model_id,\
         'provider_model_id',a.provider_model_id,'model_revision',a.model_revision,\
         'provider_endpoint_id',a.provider_endpoint_id,'egress_processor_id',a.egress_processor_id,\
         'endpoint_ref',a.endpoint_ref,'region',a.region,'service_tier',a.service_tier,\
         'credential_ref',a.credential_ref,'billing_account_id',a.billing_account_id,\
         'billing_instrument_id',a.billing_instrument_id)\
         FROM control.resolve_user_reasoning_admission($1,1,$2,'CONTRIBUTION_DEIDENTIFY') a",
        &[&e.binding, &e.domain],
    )
    .expect("current route")
    .get(0)
}

fn reserve(
    db: &mut impl GenericClient,
    e: &Execution,
    attempt: i32,
    expected: &Value,
    intent: &[u8],
) -> Result<Row, postgres::Error> {
    db.query_one(
        "SELECT * FROM private.reserve_contribution_a(\
         $1,$2,$3,'p9-0133',$4,$5,$6,$7,$8,NULL,NULL,'PRIVATE',$9,17,$10)",
        &[
            &e.tenant,
            &e.execution,
            &e.job,
            &attempt,
            &Uuid::new_v4(),
            &Uuid::new_v4(),
            &intent,
            &Uuid::new_v4(),
            &vec![0x81_u8; 32],
            expected,
        ],
    )
}

fn seed_ready(fixture: &mut ContributionFixture, label: &str) -> (Execution, i32, Value) {
    let e = prepare(fixture, label);
    assert!(enqueue(&mut fixture.admin, &e).get::<_, bool>(5));
    let attempt = claim(&mut fixture.admin, &e);
    let expected = route(&mut fixture.admin, &e);
    (e, attempt, expected)
}

fn seal(db: &mut Client, e: &Execution) -> (i16, i32, Vec<u8>) {
    let row = db.query_one(
        "SELECT source_backing_closure_version,backing_link_count,source_backing_closure_sha256 \
         FROM private.contribution_executions WHERE execution_id=$1",
        &[&e.execution],
    ).expect("root closure seal");
    (row.get(0), row.get(1), row.get(2))
}

fn assert_closure_denied(
    db: &mut Client,
    e: &Execution,
    attempt: i32,
    expected: &Value,
    label: &str,
) {
    assert!(
        reserve(db, e, attempt, expected, &[0xa7; 32]).is_err(),
        "{label}: changed closure must not reserve"
    );
    let row = db
        .query_one(
            "SELECT \
             (SELECT count(*) FROM ops.model_call_ledger WHERE request_id=$1),\
             (SELECT count(*) FROM ops.data_disclosures disclosure JOIN ops.model_call_ledger call USING(model_call_id) WHERE call.request_id=$1),\
             state::text,coverage_model_call_id IS NULL AND coverage_disclosure_id IS NULL AND coverage_intent_sha256 IS NULL \
             FROM private.contribution_executions WHERE execution_id=$2",
            &[&e.coverage_request, &e.execution],
        )
        .expect("failed reservation leaves no durable effect");
    assert_eq!(row.get::<_, i64>(0), 0, "{label}: no model-call ledger row");
    assert_eq!(row.get::<_, i64>(1), 0, "{label}: no disclosure row");
    assert_eq!(
        row.get::<_, String>(2),
        "READY_A",
        "{label}: root remains ready"
    );
    assert!(
        row.get::<_, bool>(3),
        "{label}: root reservation fields remain null"
    );
}

fn direct_evidence_execution(fixture: &mut ContributionFixture, label: &str) -> Execution {
    let mut e = prepare(fixture, label);
    let evidence: Uuid = fixture
        .admin
        .query_one(
            "SELECT evidence_id FROM private.memory_evidence WHERE memory_id=$1 ORDER BY evidence_id LIMIT 1",
            &[&fixture.memory],
        )
        .expect("fixture backing evidence")
        .get(0);
    let source_hash: Vec<u8> = fixture
        .admin
        .query_one(
            "SELECT payload_sha256 FROM private.evidence_objects WHERE evidence_id=$1",
            &[&evidence],
        )
        .expect("evidence hash")
        .get(0);
    e.source = evidence;
    e.source_kind = "EVIDENCE";
    e.source_hash = source_hash.clone();
    e.manifest_hash = fixture
        .admin
        .query_one(
            "SELECT sha256(convert_to('e:'||$1::uuid::text||':'||encode($2::bytea,'hex'),'UTF8'))",
            &[&evidence, &source_hash],
        )
        .expect("evidence manifest")
        .get(0);
    e
}

#[test]
fn migration_0133_contract_is_closed_and_serialized() {
    let sql = include_str!(
        "../../../migrations/0133_contribution_self_principal_reservation_authority.sql"
    );
    let manifest = include_str!(
        "../../../migrations/0133_contribution_self_principal_reservation_authority.manifest.toml"
    );
    let ingress = include_str!("../src/contribution_entry_repo.rs");
    let hard_stop = sql.find("DO $$").expect("hard stop");
    let cutover_lock = sql
        .find("PERFORM ops.lock_contribution_inputs();")
        .expect("cutover lock");
    let nonterminal = sql
        .find("WHERE state IN ('READY_A'")
        .expect("nonterminal precheck");
    assert!(hard_stop < cutover_lock && cutover_lock < nonterminal);
    assert!(ingress.contains("SELECT ops.lock_contribution_inputs()"));
    assert!(sql.contains(
        "BEFORE UPDATE OR DELETE ON control.workspaces\n  FOR EACH ROW EXECUTE FUNCTION ops.guard_contribution_input_change()"
    ));
    assert!(manifest.contains("tgrelid='control.workspaces'::regclass"));
    assert!(manifest.contains("tgtype=27"));
    assert!(sql.contains("IF root_match_count = 0 THEN"));
    assert!(sql.contains("IF root_match_count <> 1 THEN"));
    assert!(manifest.contains("root_match_count <> 1"));
    assert_eq!(
        sql.matches("'processor_model_id', admission.processor_model_id")
            .count(),
        1
    );
    assert!(!sql.contains("'provider_health_observation_id', admission"));
    assert!(!sql.contains("'account_health_observation_id', admission"));
    assert!(!sql.contains("'admitted_at', admission"));
}

fn workspace_shared_fixture() -> (ContributionFixture, Uuid, Uuid) {
    let mut fixture = ContributionFixture::new();
    let tenant: Uuid = fixture
        .admin
        .query_one(
            "SELECT tenant_id FROM private.memory_records WHERE memory_id=$1",
            &[&fixture.memory],
        )
        .expect("fixture tenant")
        .get(0);
    let workspace: Uuid = fixture
        .admin
        .query_one(
            "INSERT INTO control.workspaces(tenant_id,name) VALUES($1,$2) RETURNING workspace_id",
            &[&tenant, &format!("closure-workspace-{}", Uuid::new_v4())],
        )
        .expect("workspace")
        .get(0);
    // §6.1.1 / migration 0163: the WORKSPACE_SHARED arm requires an ACTIVE WorkspaceMembership
    // for THAT workspace — an ACTIVE tenant membership is no longer enough (ADR-0035). Without
    // this row the fixture's own owner cannot read what it just shared, so both oracles below
    // fail before reaching the reservation behaviour they measure. Written here as the
    // superuser owner; the runtime write path is control.set_workspace_membership.
    fixture
        .admin
        .execute(
            "INSERT INTO control.workspace_memberships(tenant_id,workspace_id,user_id,role,state) \
             SELECT $1,$2,domain.owner_user_id,'MEMBER','ACTIVE' \
             FROM control.private_reasoning_domains domain \
             WHERE domain.reasoning_domain_id=$3",
            &[&tenant, &workspace, &fixture.domain],
        )
        .expect("workspace membership for the reasoning domain owner");
    let other_tenant: Uuid = fixture
        .admin
        .query_one(
            "INSERT INTO control.tenants(name,state) VALUES($1,'ACTIVE') RETURNING tenant_id",
            &[&format!("closure-other-tenant-{}", Uuid::new_v4())],
        )
        .expect("other tenant")
        .get(0);
    fixture
        .admin
        .execute(
            "UPDATE private.memory_records SET visibility_class='WORKSPACE_SHARED',\
             visibility_user_id=NULL,visibility_workspace_id=$2 WHERE memory_id=$1",
            &[&fixture.memory, &workspace],
        )
        .expect("workspace-share memory");
    fixture
        .admin
        .execute(
            "UPDATE private.evidence_objects evidence SET visibility_class='WORKSPACE_SHARED',\
             visibility_user_id=NULL,visibility_workspace_id=$2 FROM private.memory_evidence backing \
             WHERE backing.memory_id=$1 AND backing.evidence_id=evidence.evidence_id",
            &[&fixture.memory, &workspace],
        )
        .expect("workspace-share backing evidence");
    (fixture, workspace, other_tenant)
}

fn wait_for_advisory<T>(db: &mut Client, application_name: &str, worker: &thread::JoinHandle<T>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let waiting = db
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity \
                 WHERE application_name=$1 AND wait_event_type='Lock' AND wait_event='advisory')",
                &[&application_name],
            )
            .expect("workspace wait probe")
            .get::<_, bool>(0);
        if waiting {
            return;
        }
        if worker.is_finished() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("workspace mutation did not wait for the reservation transaction's input lock");
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL 18 migrated through 0133"]
fn workspace_authority_change_waits_for_reservation_and_then_fails_closed() {
    let (mut fixture, workspace, other_tenant) = workspace_shared_fixture();
    let (reserved, reserved_attempt, reserved_route) =
        seed_ready(&mut fixture, "workspace-lock-holder");
    let (stale, stale_attempt, stale_route) = seed_ready(&mut fixture, "workspace-stale");
    let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PostgreSQL fixture");
    // dep: PostgreSQL(any) — opens the role-scoped connection for `workspace_authority_change_waits_for_reservation_and_then_fails_closed`
    let mut reservation_client = Client::connect(&dsn, NoTls).expect("reservation connection");
    let mut reservation_tx = reservation_client
        .transaction()
        .expect("reservation transaction");
    let row = reserve(
        &mut reservation_tx,
        &reserved,
        reserved_attempt,
        &reserved_route,
        &[0xd1; 32],
    )
    .expect("reservation while workspace authority is current");
    assert!(row.get::<_, bool>(8));

    let application_name = format!("workspace-authority-drift-{}", Uuid::new_v4().simple());
    let (started_tx, started_rx) = mpsc::channel();
    let mutation_dsn = dsn.clone();
    let mutation_application = application_name.clone();
    let mutation = thread::spawn(move || {
        // dep: PostgreSQL(any) — opens the role-scoped connection for `workspace_authority_change_waits_for_reservation_and_then_fails_closed`
        let mut client = Client::connect(&mutation_dsn, NoTls).expect("workspace mutation client");
        client
            .query_one(
                "SELECT set_config('application_name',$1,false)",
                &[&mutation_application],
            )
            .expect("tag workspace mutation");
        started_tx.send(()).expect("announce workspace mutation");
        // §6.1.1 / 0162: a WorkspaceMembership is tenant-scoped, and its (tenant_id,
        // workspace_id) FK is `ON DELETE CASCADE` with no `ON UPDATE` action — so a workspace
        // that leaves its tenant leaves its memberships behind. Dropping them is part of the
        // drift this test simulates; without it the FK refuses the UPDATE outright and the
        // oracle below never gets to measure anything.
        client
            .execute(
                "DELETE FROM control.workspace_memberships WHERE workspace_id=$1",
                &[&workspace],
            )
            .expect("the drifting workspace leaves its tenant-scoped memberships");
        client.execute(
            "UPDATE control.workspaces SET tenant_id=$2 WHERE workspace_id=$1",
            &[&workspace, &other_tenant],
        )
    });
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("workspace mutation started");
    wait_for_advisory(&mut fixture.admin, &application_name, &mutation);
    reservation_tx
        .commit()
        .expect("commit reservation and release input lock");
    assert_eq!(
        mutation
            .join()
            .expect("workspace mutation thread")
            .expect("workspace mutation after lock release"),
        1
    );
    assert_closure_denied(
        &mut fixture.admin,
        &stale,
        stale_attempt,
        &stale_route,
        "workspace tenant drift",
    );
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL 18 migrated through 0133"]
fn workspace_delete_waits_for_reservation_and_cannot_remove_referenced_authority() {
    let (mut fixture, workspace, _) = workspace_shared_fixture();
    let (reserved, attempt, route) = seed_ready(&mut fixture, "workspace-delete-lock-holder");
    let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PostgreSQL fixture");
    // dep: PostgreSQL(any) — opens the role-scoped connection for `workspace_delete_waits_for_reservation_and_cannot_remove_referenced_authority`
    let mut reservation_client = Client::connect(&dsn, NoTls).expect("reservation connection");
    let mut reservation_tx = reservation_client
        .transaction()
        .expect("reservation transaction");
    assert!(
        reserve(&mut reservation_tx, &reserved, attempt, &route, &[0xd2; 32],)
            .expect("reservation while workspace exists")
            .get::<_, bool>(8)
    );

    let application_name = format!("workspace-authority-delete-{}", Uuid::new_v4().simple());
    let (started_tx, started_rx) = mpsc::channel();
    let mutation_application = application_name.clone();
    let mutation = thread::spawn(move || {
        // dep: PostgreSQL(any) — opens the role-scoped connection for `workspace_delete_waits_for_reservation_and_cannot_remove_referenced_authority`
        let mut client = Client::connect(&dsn, NoTls).expect("workspace delete client");
        client
            .query_one(
                "SELECT set_config('application_name',$1,false)",
                &[&mutation_application],
            )
            .expect("tag workspace delete");
        started_tx.send(()).expect("announce workspace delete");
        client.execute(
            "DELETE FROM control.workspaces WHERE workspace_id=$1",
            &[&workspace],
        )
    });
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("workspace delete started");
    wait_for_advisory(&mut fixture.admin, &application_name, &mutation);
    reservation_tx
        .commit()
        .expect("commit reservation and release input lock");
    assert!(
        mutation.join().expect("workspace delete thread").is_err(),
        "referenced authority workspace must remain after the lock releases"
    );
    let tenant_after: Uuid = fixture
        .admin
        .query_one(
            "SELECT tenant_id FROM control.workspaces WHERE workspace_id=$1",
            &[&workspace],
        )
        .expect("workspace remains")
        .get(0);
    assert_eq!(tenant_after, reserved.tenant);
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL 18 migrated through 0133"]
#[allow(clippy::too_many_lines)] // One fixture proves the coupled route and no-mutation axes.
fn reservation_authority_route_replay_and_exact_root_axes() {
    let mut fixture = ContributionFixture::new();

    let (green, attempt, expected) = seed_ready(&mut fixture, "green");
    let intent = vec![0xa1_u8; 32];
    let first =
        reserve(&mut fixture.admin, &green, attempt, &expected, &intent).expect("green reserve");
    assert!(first.get::<_, bool>(8));
    let replay = reserve(&mut fixture.admin, &green, attempt, &expected, &intent).expect("replay");
    assert!(!replay.get::<_, bool>(8));

    let (malformed, malformed_attempt, mut malformed_route) = seed_ready(&mut fixture, "malformed");
    malformed_route
        .as_object_mut()
        .expect("object")
        .remove("processor_model_id");
    assert!(
        reserve(
            &mut fixture.admin,
            &malformed,
            malformed_attempt,
            &malformed_route,
            &[0xb1; 32]
        )
        .is_err()
    );
    let mut unknown = expected.clone();
    unknown["unknown_axis"] = json!("forbidden");
    let mut wrong_type = expected.clone();
    wrong_type["binding_version"] = json!("1");
    let mut wrong_version = expected.clone();
    wrong_version["schema_version"] = json!(2);
    let mut wrong_null = expected.clone();
    wrong_null["provider_model_id"] = Value::Null;
    for invalid in [&unknown, &wrong_type, &wrong_version, &wrong_null] {
        assert!(
            fixture
                .admin
                .query_one(
                    "SELECT private.assert_contribution_prepared_route_shape($1)",
                    &[invalid],
                )
                .is_err()
        );
    }

    let (processor_drift, processor_attempt, mut processor_route) =
        seed_ready(&mut fixture, "processor-drift");
    let external_model = processor_route["provider_model_id"].clone();
    processor_route["processor_model_id"] = json!(Uuid::new_v4().to_string());
    assert_eq!(processor_route["provider_model_id"], external_model);
    assert!(
        reserve(
            &mut fixture.admin,
            &processor_drift,
            processor_attempt,
            &processor_route,
            &[0xc1; 32]
        )
        .is_err()
    );

    let (provider_drift, provider_attempt, mut provider_route) =
        seed_ready(&mut fixture, "provider-drift");
    provider_route["provider_model_id"] = json!("different-external-model");
    assert!(
        reserve(
            &mut fixture.admin,
            &provider_drift,
            provider_attempt,
            &provider_route,
            &[0xd1; 32]
        )
        .is_err()
    );

    let (observation, observation_attempt, observation_route) =
        seed_ready(&mut fixture, "observation");
    fixture.admin.execute(
        "INSERT INTO ops.reasoning_provider_health_observations(tenant_id,processor_id,processor_model_id,provider_model_id,model_revision,provider_endpoint_id,endpoint_ref,region,service_tier,source_kind,verdict,observed_at,valid_until)\
         SELECT tenant_id,processor_id,processor_model_id,provider_model_id,model_revision,provider_endpoint_id,endpoint_ref,region,service_tier,'TEST','HEALTHY',clock_timestamp(),clock_timestamp()+interval '1 hour'\
         FROM ops.reasoning_provider_health_observations WHERE tenant_id=$1 ORDER BY observation_id DESC LIMIT 1",
        &[&observation.tenant],
    ).expect("insert provider observation");
    assert!(
        reserve(
            &mut fixture.admin,
            &observation,
            observation_attempt,
            &observation_route,
            &[0xe1; 32]
        )
        .is_ok()
    );

    let left = prepare(&mut fixture, "collision-left");
    let mut right = prepare(&mut fixture, "collision-right");
    right.assessment_request = left.coverage_request;
    assert!(enqueue(&mut fixture.admin, &left).get::<_, bool>(5));
    assert!(enqueue(&mut fixture.admin, &right).get::<_, bool>(5));
    let collision_attempt = claim(&mut fixture.admin, &left);
    let collision_route = route(&mut fixture.admin, &left);
    assert!(
        reserve(
            &mut fixture.admin,
            &left,
            collision_attempt,
            &collision_route,
            &[0xf1; 32]
        )
        .is_err()
    );
    let counts = fixture.admin.query_one(
        "SELECT (SELECT count(*) FROM ops.model_call_ledger WHERE request_id=$1),\
                (SELECT count(*) FROM ops.data_disclosures d JOIN ops.model_call_ledger l USING(model_call_id) WHERE l.request_id=$1)",
        &[&left.coverage_request],
    ).expect("collision durable counts");
    assert_eq!(counts.get::<_, i64>(0), 0);
    assert_eq!(counts.get::<_, i64>(1), 0);
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL 18 migrated through 0133"]
#[allow(clippy::too_many_lines)] // One root-only PG regression keeps every V1 closure axis visible.
fn root_closure_seal_rejects_backing_and_direct_source_drift_without_reservation() {
    let mut fixture = ContributionFixture::new();
    let seal_root = prepare(&mut fixture, "closure-seal-replay");
    let multi_role_evidence: Uuid = fixture
        .admin
        .query_one(
            "SELECT evidence_id FROM private.memory_evidence WHERE memory_id=$1 ORDER BY evidence_id LIMIT 1",
            &[&fixture.memory],
        )
        .expect("multi-role backing evidence")
        .get(0);
    fixture
        .admin
        .execute(
            "INSERT INTO private.memory_evidence(memory_id,evidence_id,role,ordinal) \
             VALUES($1,$2,'PRIMARY',1)",
            &[&fixture.memory, &multi_role_evidence],
        )
        .expect("same Evidence may carry a second backing role");
    assert!(enqueue(&mut fixture.admin, &seal_root).get::<_, bool>(5));
    let original_seal = seal(&mut fixture.admin, &seal_root);
    assert_eq!(original_seal.0, 1, "V1 closure seal version");
    assert_eq!(
        original_seal.1, 2,
        "same Evidence has two legal backing roles"
    );
    assert_eq!(original_seal.2.len(), 32, "V1 closure seal hash width");
    assert!(
        fixture
            .admin
            .execute(
                "UPDATE private.contribution_executions \
                 SET source_backing_closure_version=1,source_backing_closure_sha256=NULL,backing_link_count=0 \
                 WHERE execution_id=$1",
                &[&seal_root.execution],
            )
            .is_err(),
        "partial V1 closure seal must fail the table shape check"
    );
    assert_eq!(seal(&mut fixture.admin, &seal_root), original_seal);
    let partial_counts = fixture
        .admin
        .query_one(
            "SELECT \
             (SELECT count(*) FROM ops.model_call_ledger WHERE request_id=$1),\
             (SELECT count(*) FROM ops.data_disclosures disclosure JOIN ops.model_call_ledger call USING(model_call_id) WHERE call.request_id=$1),\
             state::text,coverage_model_call_id IS NULL AND coverage_disclosure_id IS NULL \
             FROM private.contribution_executions WHERE execution_id=$2",
            &[&seal_root.coverage_request, &seal_root.execution],
        )
        .expect("partial seal rejection leaves root untouched");
    assert_eq!(partial_counts.get::<_, i64>(0), 0);
    assert_eq!(partial_counts.get::<_, i64>(1), 0);
    assert_eq!(partial_counts.get::<_, String>(2), "READY_A");
    assert!(partial_counts.get::<_, bool>(3));
    assert!(
        fixture
            .admin
            .execute(
                "UPDATE private.contribution_executions \
                 SET source_backing_closure_sha256=decode(repeat('ff',32),'hex') \
                 WHERE execution_id=$1",
                &[&seal_root.execution],
            )
            .is_err(),
        "a complete but different closure seal must fail the immutability trigger"
    );
    assert_eq!(seal(&mut fixture.admin, &seal_root), original_seal);
    assert!(
        !enqueue(&mut fixture.admin, &seal_root).get::<_, bool>(5),
        "identical enqueue is an idempotent replay"
    );
    assert_eq!(seal(&mut fixture.admin, &seal_root), original_seal);
    fixture
        .admin
        .execute(
            "DELETE FROM private.memory_evidence \
             WHERE memory_id=$1 AND evidence_id=$2 AND role='PRIMARY'",
            &[&fixture.memory, &multi_role_evidence],
        )
        .expect("restore single-role backing fixture");

    let (root, attempt, expected) = seed_ready(&mut fixture, "closure-drift");
    assert!(
        fixture
            .admin
            .execute(
                "UPDATE private.contribution_execution_sources SET ordinal=1 \
                 WHERE execution_id=$1 AND ordinal=0",
                &[&root.execution],
            )
            .is_err(),
        "direct source ordinal is protected by the append-only relation trigger"
    );
    assert_eq!(
        fixture
            .admin
            .query_one(
                "SELECT ordinal FROM private.contribution_execution_sources \
                 WHERE execution_id=$1",
                &[&root.execution],
            )
            .expect("source ordinal remains sealed")
            .get::<_, i32>(0),
        0
    );
    let backing: Uuid = fixture
        .admin
        .query_one(
            "SELECT evidence_id FROM private.memory_evidence WHERE memory_id=$1 ORDER BY evidence_id LIMIT 1",
            &[&fixture.memory],
        )
        .expect("fixture backing")
        .get(0);
    let temporary_backing: Uuid = fixture
        .admin
        .query_one(
            "INSERT INTO private.evidence_objects(tenant_id,evidence_kind,payload_sha256,data_class,origin_class,visibility_class,reasoning_domain_id) \
             VALUES($1,'EVENT',sha256(convert_to('{\"temporary\":true}','UTF8')),'INTERNAL','DirectUserInput','TENANT_SHARED',$2) RETURNING evidence_id",
            &[&root.tenant, &root.domain],
        )
        .expect("temporary backing evidence")
        .get(0);
    fixture
        .admin
        .execute(
            "INSERT INTO private.events(event_id,event_kind,payload) VALUES($1,'USER_MESSAGE','{}')",
            &[&temporary_backing],
        )
        .expect("temporary backing event");

    fixture
        .admin
        .execute(
            "INSERT INTO private.memory_evidence(memory_id,evidence_id,role,ordinal) VALUES($1,$2,'SUPPORTING',9)",
            &[&fixture.memory, &temporary_backing],
        )
        .expect("add backing after root seal");
    assert_closure_denied(
        &mut fixture.admin,
        &root,
        attempt,
        &expected,
        "backing addition",
    );
    fixture
        .admin
        .execute(
            "DELETE FROM private.memory_evidence WHERE memory_id=$1 AND evidence_id=$2 AND role='SUPPORTING'",
            &[&fixture.memory, &temporary_backing],
        )
        .expect("restore added backing");

    fixture
        .admin
        .query_one(
            "SELECT set_config('humaux.closure_memory',$1,false)",
            &[&fixture.memory.to_string()],
        )
        .expect("temporary backing parameter");
    fixture
        .admin
        .batch_execute(
            "CREATE TEMP TABLE closure_removed_backing AS \
             SELECT memory_id,evidence_id,role,ordinal,created_at FROM private.memory_evidence \
             WHERE memory_id=current_setting('humaux.closure_memory')::uuid",
        )
        .expect("capture backing for exact restoration");
    fixture
        .admin
        .execute(
            "INSERT INTO private.memory_evidence(memory_id,evidence_id,role,ordinal) VALUES($1,$2,'SUPPORTING',10)",
            &[&fixture.memory, &temporary_backing],
        )
        .expect("replacement backing before removal");
    fixture
        .admin
        .execute(
            "DELETE FROM private.memory_evidence WHERE memory_id=$1 AND evidence_id=$2",
            &[&fixture.memory, &backing],
        )
        .expect("remove sealed backing");
    assert_closure_denied(
        &mut fixture.admin,
        &root,
        attempt,
        &expected,
        "backing removal",
    );
    fixture
        .admin
        .batch_execute(
            "INSERT INTO private.memory_evidence(memory_id,evidence_id,role,ordinal,created_at) \
             SELECT memory_id,evidence_id,role,ordinal,created_at FROM closure_removed_backing; \
             DROP TABLE closure_removed_backing",
        )
        .expect("restore removed backing exactly");
    fixture
        .admin
        .execute(
            "DELETE FROM private.memory_evidence WHERE memory_id=$1 AND evidence_id=$2",
            &[&fixture.memory, &temporary_backing],
        )
        .expect("remove replacement backing");
    fixture
        .admin
        .execute(
            "DELETE FROM private.events WHERE event_id=$1",
            &[&temporary_backing],
        )
        .expect("remove temporary event");
    fixture
        .admin
        .execute(
            "DELETE FROM private.evidence_objects WHERE evidence_id=$1",
            &[&temporary_backing],
        )
        .expect("remove temporary evidence");

    fixture
        .admin
        .execute(
            "UPDATE private.memory_evidence SET role='PRIMARY' WHERE memory_id=$1 AND evidence_id=$2 AND role='SUPPORTING'",
            &[&fixture.memory, &backing],
        )
        .expect("change backing role");
    assert_closure_denied(
        &mut fixture.admin,
        &root,
        attempt,
        &expected,
        "backing role",
    );
    fixture
        .admin
        .execute(
            "UPDATE private.memory_evidence SET role='SUPPORTING' WHERE memory_id=$1 AND evidence_id=$2 AND role='PRIMARY'",
            &[&fixture.memory, &backing],
        )
        .expect("restore backing role");
    fixture
        .admin
        .execute(
            "UPDATE private.memory_evidence SET ordinal=1 WHERE memory_id=$1 AND evidence_id=$2 AND role='SUPPORTING'",
            &[&fixture.memory, &backing],
        )
        .expect("change backing ordinal");
    assert_closure_denied(
        &mut fixture.admin,
        &root,
        attempt,
        &expected,
        "backing ordinal",
    );
    fixture
        .admin
        .execute(
            "UPDATE private.memory_evidence SET ordinal=0 WHERE memory_id=$1 AND evidence_id=$2 AND role='SUPPORTING'",
            &[&fixture.memory, &backing],
        )
        .expect("restore backing ordinal");

    fixture
        .admin
        .execute(
            "UPDATE private.evidence_objects SET payload_sha256=decode(repeat('ab',32),'hex') WHERE evidence_id=$1",
            &[&backing],
        )
        .expect("change backing payload hash");
    assert_closure_denied(
        &mut fixture.admin,
        &root,
        attempt,
        &expected,
        "backing payload hash",
    );
    fixture
        .admin
        .execute(
            "UPDATE private.evidence_objects SET payload_sha256=sha256(convert_to('{}','UTF8')) WHERE evidence_id=$1",
            &[&backing],
        )
        .expect("restore backing payload hash");
    fixture
        .admin
        .execute(
            "UPDATE private.evidence_objects SET data_class='PRIVATE' WHERE evidence_id=$1",
            &[&backing],
        )
        .expect("change backing data class");
    assert_closure_denied(
        &mut fixture.admin,
        &root,
        attempt,
        &expected,
        "backing data class",
    );
    fixture
        .admin
        .execute(
            "UPDATE private.evidence_objects SET data_class='INTERNAL' WHERE evidence_id=$1",
            &[&backing],
        )
        .expect("restore backing data class");
    let other_domain: Uuid = fixture
        .admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains(tenant_id,name,owner_user_id,status) VALUES($1,$2,$3,'ACTIVE') RETURNING reasoning_domain_id",
            &[&root.tenant, &format!("closure-other-{}", Uuid::new_v4()), &root.user],
        )
        .expect("other active domain")
        .get(0);
    fixture
        .admin
        .execute(
            "UPDATE private.evidence_objects SET reasoning_domain_id=$2 WHERE evidence_id=$1",
            &[&backing, &other_domain],
        )
        .expect("change backing domain");
    assert_closure_denied(
        &mut fixture.admin,
        &root,
        attempt,
        &expected,
        "backing domain",
    );
    fixture
        .admin
        .execute(
            "UPDATE private.evidence_objects SET reasoning_domain_id=$2 WHERE evidence_id=$1",
            &[&backing, &root.domain],
        )
        .expect("restore backing domain");
    fixture
        .admin
        .execute(
            "UPDATE private.evidence_objects SET visibility_class='USER_PRIVATE',visibility_user_id=$2,visibility_workspace_id=NULL WHERE evidence_id=$1",
            &[&backing, &root.user],
        )
        .expect("change backing visibility");
    assert_closure_denied(
        &mut fixture.admin,
        &root,
        attempt,
        &expected,
        "backing visibility",
    );
    fixture
        .admin
        .execute(
            "UPDATE private.evidence_objects SET visibility_class='TENANT_SHARED',visibility_user_id=NULL,visibility_workspace_id=NULL WHERE evidence_id=$1",
            &[&backing],
        )
        .expect("restore backing visibility");

    fixture
        .admin
        .execute(
            "UPDATE private.memory_records SET content='{\"text\":\"closure changed\"}' WHERE memory_id=$1",
            &[&fixture.memory],
        )
        .expect("change direct memory content");
    assert_closure_denied(
        &mut fixture.admin,
        &root,
        attempt,
        &expected,
        "direct memory content",
    );
    fixture
        .admin
        .execute(
            "UPDATE private.memory_records SET content='{\"text\":\"private fixture detail\"}' WHERE memory_id=$1",
            &[&fixture.memory],
        )
        .expect("restore direct memory content");
    fixture
        .admin
        .execute(
            "UPDATE private.memory_records SET status='revoked' WHERE memory_id=$1",
            &[&fixture.memory],
        )
        .expect("change direct memory status");
    assert_closure_denied(
        &mut fixture.admin,
        &root,
        attempt,
        &expected,
        "direct memory status",
    );
    fixture
        .admin
        .execute(
            "UPDATE private.memory_records SET status='active' WHERE memory_id=$1",
            &[&fixture.memory],
        )
        .expect("restore direct memory status");
    fixture
        .admin
        .execute(
            "UPDATE private.memory_records SET visibility_class='USER_PRIVATE',visibility_user_id=$2,visibility_workspace_id=NULL WHERE memory_id=$1",
            &[&fixture.memory, &root.user],
        )
        .expect("change direct memory visibility");
    assert_closure_denied(
        &mut fixture.admin,
        &root,
        attempt,
        &expected,
        "direct memory visibility",
    );
    fixture
        .admin
        .execute(
            "UPDATE private.memory_records SET visibility_class='TENANT_SHARED',visibility_user_id=NULL,visibility_workspace_id=NULL WHERE memory_id=$1",
            &[&fixture.memory],
        )
        .expect("restore direct memory visibility");

    let direct = direct_evidence_execution(&mut fixture, "direct-evidence-drift");
    assert!(enqueue(&mut fixture.admin, &direct).get::<_, bool>(5));
    let direct_attempt = claim(&mut fixture.admin, &direct);
    let direct_expected = route(&mut fixture.admin, &direct);
    fixture
        .admin
        .execute(
            "UPDATE private.evidence_objects SET payload_sha256=decode(repeat('cd',32),'hex') WHERE evidence_id=$1",
            &[&direct.source],
        )
        .expect("change direct evidence payload hash");
    assert_closure_denied(
        &mut fixture.admin,
        &direct,
        direct_attempt,
        &direct_expected,
        "direct evidence payload hash",
    );
    fixture
        .admin
        .execute(
            "UPDATE private.evidence_objects SET payload_sha256=sha256(convert_to('{}','UTF8')) WHERE evidence_id=$1",
            &[&direct.source],
        )
        .expect("restore direct evidence payload hash");
    fixture
        .admin
        .execute(
            "UPDATE private.evidence_objects SET data_class='PRIVATE' WHERE evidence_id=$1",
            &[&direct.source],
        )
        .expect("change direct evidence data class");
    assert_closure_denied(
        &mut fixture.admin,
        &direct,
        direct_attempt,
        &direct_expected,
        "direct evidence data class",
    );
    fixture
        .admin
        .execute(
            "UPDATE private.evidence_objects SET data_class='INTERNAL' WHERE evidence_id=$1",
            &[&direct.source],
        )
        .expect("restore direct evidence data class");
    fixture
        .admin
        .execute(
            "UPDATE private.evidence_objects SET reasoning_domain_id=$2 WHERE evidence_id=$1",
            &[&direct.source, &other_domain],
        )
        .expect("change direct evidence domain");
    assert_closure_denied(
        &mut fixture.admin,
        &direct,
        direct_attempt,
        &direct_expected,
        "direct evidence domain",
    );
    fixture
        .admin
        .execute(
            "UPDATE private.evidence_objects SET reasoning_domain_id=$2 WHERE evidence_id=$1",
            &[&direct.source, &direct.domain],
        )
        .expect("restore direct evidence domain");
    fixture
        .admin
        .execute(
            "UPDATE private.evidence_objects SET visibility_class='USER_PRIVATE',\
             visibility_user_id=$2,visibility_workspace_id=NULL WHERE evidence_id=$1",
            &[&direct.source, &direct.user],
        )
        .expect("change direct evidence visibility");
    assert_closure_denied(
        &mut fixture.admin,
        &direct,
        direct_attempt,
        &direct_expected,
        "direct evidence visibility",
    );
    fixture
        .admin
        .execute(
            "UPDATE private.evidence_objects SET visibility_class='TENANT_SHARED',\
             visibility_user_id=NULL,visibility_workspace_id=NULL WHERE evidence_id=$1",
            &[&direct.source],
        )
        .expect("restore direct evidence visibility");

    fixture
        .admin
        .batch_execute(
            "CREATE TEMP TABLE closure_audit_time AS \
             SELECT memory_id,evidence_id,role,created_at FROM private.memory_evidence \
             WHERE memory_id=current_setting('humaux.closure_memory')::uuid AND evidence_id IS NOT NULL",
        )
        .expect("capture non-closure audit time");
    fixture
        .admin
        .execute(
            "UPDATE private.memory_evidence SET created_at=clock_timestamp() WHERE memory_id=$1 AND evidence_id=$2",
            &[&fixture.memory, &backing],
        )
        .expect("change non-closure backing audit time");
    let audit_attempt = claim(&mut fixture.admin, &root);
    assert!(
        reserve(
            &mut fixture.admin,
            &root,
            audit_attempt,
            &expected,
            &[0xa8; 32]
        )
        .is_ok(),
        "created_at is not part of the closure seal"
    );
    fixture
        .admin
        .batch_execute(
            "UPDATE private.memory_evidence backing SET created_at=stored.created_at \
             FROM closure_audit_time stored \
             WHERE (backing.memory_id,backing.evidence_id,backing.role)=(stored.memory_id,stored.evidence_id,stored.role); \
             DROP TABLE closure_audit_time",
        )
        .expect("restore non-closure audit time exactly");
}

fn database_dsn(base: &str, database: &str) -> String {
    let (without_query, query) = base
        .split_once('?')
        .map_or((base, None), |(head, tail)| (head, Some(tail)));
    let slash = without_query.rfind('/').expect("database path in test DSN");
    format!(
        "{}{database}{}",
        &without_query[..=slash],
        query.map_or(String::new(), |tail| format!("?{tail}"))
    )
}

#[test]
#[ignore = "lane(a:disposable) creates and removes a disposable PostgreSQL 18 database"]
fn cutover_nonterminal_precheck_is_atomic() {
    let base_dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PostgreSQL 18 DSN");
    let database = format!("humaux_0133_cutover_{}", Uuid::new_v4().simple());
    let test_dsn = database_dsn(&base_dsn, &database);
    // dep: PostgreSQL(any) — opens the role-scoped connection for `cutover_nonterminal_precheck_is_atomic`
    let mut admin = Client::connect(&base_dsn, NoTls).expect("admin DB");
    admin
        .batch_execute(&format!("CREATE DATABASE {database}"))
        .expect("create disposable cutover DB");

    {
        // dep: PostgreSQL(any) — opens the role-scoped connection for `cutover_nonterminal_precheck_is_atomic`
        let mut db = Client::connect(&test_dsn, NoTls).expect("cutover DB");
        let migration_dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut paths = std::fs::read_dir(&migration_dir)
            .expect("migration directory")
            .map(|entry| entry.expect("migration entry").path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "sql"))
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| name.get(..4))
                    .and_then(|prefix| prefix.parse::<u16>().ok())
                    .is_some_and(|number| number <= 132)
            })
            .collect::<Vec<_>>();
        paths.sort();
        for path in paths {
            db.batch_execute(&std::fs::read_to_string(&path).expect("read migration"))
                .unwrap_or_else(|error| panic!("apply {}: {error}", path.display()));
        }

        let ids = (0..9).map(|_| Uuid::new_v4()).collect::<Vec<_>>();
        // replica-mode: throwaway database only (humaux_0133_cutover_<uuid>, created and dropped WITH (FORCE) by this test)
        db.batch_execute("SET session_replication_role=replica")
            .expect("fixture trigger bypass");
        db.execute(
            "INSERT INTO private.contribution_executions(\
             execution_id,tenant_id,user_id,enqueue_idempotency_key,enqueue_fingerprint,\
             reasoning_domain_id,input_manifest_hash,source_count,policy_id,policy_version,\
             policy_snapshot,rights_basis,coverage_request_id,assessment_request_id,candidate_id,\
             coverage_contract_version,assessment_contract_version,coverage_prompt_contract_sha256,\
             assessment_prompt_contract_sha256,binding_id,binding_version)\
             VALUES($1,$2,$3,'cutover',decode(repeat('11',32),'hex'),$4,\
             decode(repeat('22',32),'hex'),1,$5,1,'{\"policy\":\"MANUAL\"}','fixture',\
             $6,$7,$8,1,1,decode(repeat('33',32),'hex'),decode(repeat('44',32),'hex'),$9,1)",
            &[
                &ids[0], &ids[1], &ids[2], &ids[3], &ids[4], &ids[5], &ids[6], &ids[7], &ids[8],
            ],
        )
        .expect("nonterminal pre-0133 root");
        db.batch_execute("SET session_replication_role=origin")
            .expect("restore triggers");

        let migration = std::fs::read_to_string(
            migration_dir.join("0133_contribution_self_principal_reservation_authority.sql"),
        )
        .expect("0133 migration");
        let mut transaction = db.transaction().expect("cutover transaction");
        let error = transaction
            .batch_execute(&migration)
            .expect_err("hard stop");
        assert_eq!(
            error.code(),
            Some(&postgres::error::SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
        );
        transaction.rollback().expect("rollback failed cutover");

        let row = db.query_one(
            "SELECT count(*)=1,\
             to_regprocedure('private.assert_contribution_prepared_route_shape(jsonb)') IS NULL,\
             has_function_privilege('role_private_worker',\
               'private.reserve_contribution_a(uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint)',\
               'EXECUTE')\
             FROM private.contribution_executions WHERE execution_id=$1 AND state='READY_A'",
            &[&ids[0]],
        ).expect("post-failure atomic state");
        assert!(row.get::<_, bool>(0));
        assert!(row.get::<_, bool>(1));
        assert!(row.get::<_, bool>(2));
    }

    admin
        .batch_execute(&format!("DROP DATABASE {database} WITH (FORCE)"))
        .expect("drop disposable cutover DB");
}
