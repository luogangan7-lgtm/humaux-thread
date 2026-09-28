//! `adapters::tests::reasoning_route_foundation` — Real-PG18 negative gates for the inert USER_REASONING route
//!   foundation.
//! Depends-on: crates=[postgres, uuid]; services=[PostgreSQL(owner) w=[control.credentials, control.memberships,
//!   control.private_reasoning_domains, control.processor_models, control.provider_accounts,
//!   control.provider_billing_accounts, control.provider_billing_instruments, control.provider_endpoints,
//!   control.reasoning_credential_bindings, control.reasoning_profiles, control.reasoning_route_bindings,
//!   control.reasoning_route_candidates, control.reasoning_route_policies, control.tenants, control.users]];
//!   env=[HUMAUX_TEST_PG_DSN]; modules=[]
//! Called-by: [cargo-test]
//! Invariants: [R1 proves ownership, trust, payer and ACL constraints on the route tables without activating a
//!   router; the tests are #[ignore] lane tests on an isolated database]
//! Spec: none
//!
//! R1 proves ownership, trust, payer, and ACL constraints without activating a router.

use postgres::{Client, GenericClient, NoTls, error::SqlState};
use std::{
    sync::mpsc,
    time::{Duration, Instant},
};
use uuid::Uuid;

fn seed_identity<C: GenericClient>(client: &mut C, name: &str) -> (Uuid, Uuid, Uuid) {
    let tenant_id: Uuid = client
        .query_one(
            "INSERT INTO control.tenants(name) VALUES($1) RETURNING tenant_id",
            &[&name],
        )
        .expect("tenant")
        .get(0);
    let user_id: Uuid = client
        .query_one(
            "INSERT INTO control.users(state) VALUES('ACTIVE') RETURNING user_id",
            &[],
        )
        .expect("user")
        .get(0);
    client
        .execute(
            "INSERT INTO control.memberships(tenant_id,user_id,role,state) VALUES($1,$2,'OWNER','ACTIVE')",
            &[&tenant_id, &user_id],
        )
        .expect("membership");
    let credential_id: Uuid = client
        .query_one(
            "INSERT INTO control.credentials(tenant_id,purpose,openbao_ref) VALUES($1,'USER_REASONING',$2) RETURNING credential_id",
            &[&tenant_id, &format!("openbao://fixture/{}", Uuid::new_v4())],
        )
        .expect("credential reference")
        .get(0);
    (tenant_id, user_id, credential_id)
}

fn assert_read_committed<C: GenericClient>(client: &mut C) {
    assert_eq!(
        client
            .query_one("SHOW transaction_isolation", &[])
            .expect("transaction isolation")
            .get::<_, String>(0),
        "read committed"
    );
}

#[derive(Clone)]
struct RouteLane {
    tenant: Uuid,
    user: Uuid,
    credential: Uuid,
    account: Uuid,
    endpoint: Uuid,
    model: Uuid,
    profile: Uuid,
    domain: Uuid,
}

fn seed_route_lane<C: GenericClient>(client: &mut C, label: &str) -> RouteLane {
    let (tenant, user, credential) = seed_identity(client, label);
    let suffix = Uuid::new_v4();
    let processor = format!("race-processor-{suffix}");
    let provider_model = format!("race-model-{suffix}");
    let mut account_hash = Vec::with_capacity(32);
    account_hash.extend_from_slice(suffix.as_bytes());
    account_hash.extend_from_slice(suffix.as_bytes());
    let model: Uuid = client.query_one(
        "INSERT INTO control.processor_models(processor_id,provider_model_id,capabilities,status,catalog_observed_at) VALUES($1,$2,ARRAY['TEXT'],'ACTIVE',clock_timestamp()) RETURNING processor_model_id",
        &[&processor, &provider_model],
    ).expect("race model").get(0);
    let account: Uuid = client.query_one(
        "INSERT INTO control.provider_accounts(tenant_id,owner_user_id,processor_id,external_account_ref_hash) VALUES($1,$2,$3,$4) RETURNING provider_account_id",
        &[&tenant, &user, &processor, &account_hash],
    ).expect("race account").get(0);
    client.execute(
        "INSERT INTO control.reasoning_credential_bindings(credential_ref,tenant_id,owner_user_id,provider_account_id,processor_id) VALUES($1,$2,$3,$4,$5)",
        &[&credential, &tenant, &user, &account, &processor],
    ).expect("race credential binding");
    let endpoint: Uuid = client.query_one(
        "INSERT INTO control.provider_endpoints(tenant_id,provider_account_id,region,service_tier,endpoint_ref) VALUES($1,$2,'us-east-1','standard','race') RETURNING endpoint_id",
        &[&tenant, &account],
    ).expect("race endpoint").get(0);
    let profile: Uuid = client.query_one(
        "INSERT INTO control.reasoning_profiles(tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,capabilities) VALUES($1,$2,$3,$4,$5,$6,ARRAY['TEXT']) RETURNING profile_id",
        &[&tenant, &user, &account, &endpoint, &model, &credential],
    ).expect("race profile").get(0);
    let domain: Uuid = client.query_one(
        "INSERT INTO control.private_reasoning_domains(tenant_id,name,owner_user_id) VALUES($1,$2,$3) RETURNING reasoning_domain_id",
        &[&tenant, &format!("{label} domain"), &user],
    ).expect("race domain").get(0);
    RouteLane {
        tenant,
        user,
        credential,
        account,
        endpoint,
        model,
        profile,
        domain,
    }
}

fn spawn_transaction_statement(
    dsn: &str,
    statement: String,
) -> (i32, std::thread::JoinHandle<Result<(), String>>) {
    let (pid_tx, pid_rx) = mpsc::channel();
    let dsn = dsn.to_owned();
    let actor = std::thread::spawn(move || {
        // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
        let mut client = Client::connect(&dsn, NoTls).expect("race PostgreSQL connection");
        client
            .batch_execute(
                "BEGIN ISOLATION LEVEL READ COMMITTED; SET LOCAL statement_timeout='10s'",
            )
            .expect("race begin");
        assert_read_committed(&mut client);
        let pid: i32 = client
            .query_one("SELECT pg_backend_pid()", &[])
            .expect("race backend pid")
            .get(0);
        pid_tx.send(pid).expect("publish race backend pid");
        match client.execute(&statement, &[]) {
            Ok(_) => {
                client.batch_execute("COMMIT").expect("race commit");
                Ok(())
            }
            Err(error) => {
                client.batch_execute("ROLLBACK").expect("race rollback");
                Err(error
                    .code()
                    .map(|code| code.code().to_owned())
                    .unwrap_or_else(|| error.to_string()))
            }
        }
    });
    (
        pid_rx.recv().expect("race backend pid before statement"),
        actor,
    )
}

fn wait_for_ungranted_lock(observer: &mut Client, pid: i32, advisory_only: bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let waiting: bool = observer
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid=$1 AND NOT granted AND (NOT $2 OR locktype='advisory'))",
                &[&pid, &advisory_only],
            )
            .expect("observe race lock wait")
            .get(0);
        if waiting {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "backend {pid} never exposed the expected ungranted lock in pg_locks"
        );
        std::thread::yield_now();
    }
}

fn assert_cross_spine_successor_waits_then_rejects(
    dsn: &str,
    first_version: String,
    cross_spine_successor: String,
) {
    // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
    let mut first = Client::connect(dsn, NoTls).expect("first spine connection");
    first
        .batch_execute("BEGIN ISOLATION LEVEL READ COMMITTED")
        .expect("first spine begin");
    assert_read_committed(&mut first);
    first
        .execute(&first_version, &[])
        .expect("uncommitted first spine version");
    let (successor_pid, successor) = spawn_transaction_statement(dsn, cross_spine_successor);
    // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
    let mut observer = Client::connect(dsn, NoTls).expect("spine lock observer");
    wait_for_ungranted_lock(&mut observer, successor_pid, true);
    first.batch_execute("COMMIT").expect("publish first spine");
    assert_eq!(
        successor.join().expect("successor thread"),
        Err("23514".to_owned())
    );
}

fn seed_shadow_policy<C: GenericClient>(client: &mut C, lane: &RouteLane) -> Uuid {
    let policy: Uuid = client.query_one(
        "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose) VALUES($1,$2,'PRIVATE_DISTILL_TEXT') RETURNING route_policy_id",
        &[&lane.tenant, &lane.user],
    ).expect("race policy").get(0);
    client.execute(
        "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,1,$3,1,0)",
        &[&lane.tenant, &policy, &lane.profile],
    ).expect("race policy candidate");
    client.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='SHADOW' WHERE route_policy_id=$1 AND policy_version=1",
        &[&policy],
    ).expect("race policy SHADOW");
    policy
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL 18 migrated through 0128"]
#[allow(
    clippy::too_many_lines,
    reason = "one integration scenario verifies the complete reasoning-route ownership and payer boundary"
)]
fn user_reasoning_foundation_rejects_cross_owner_trust_and_payer_edges() {
    let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PostgreSQL 18 DSN");
    // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
    let mut client = Client::connect(&dsn, NoTls).expect("isolated PostgreSQL 18");
    let version: i32 = client
        .query_one("SHOW server_version_num", &[])
        .expect("server version")
        .get::<_, String>(0)
        .parse()
        .expect("numeric server version");
    assert!(version >= 180_000, "requires PostgreSQL 18, got {version}");
    assert!(
        client
            .query_one(
                "SELECT to_regclass('control.reasoning_route_bindings') IS NOT NULL",
                &[]
            )
            .expect("migration probe")
            .get::<_, bool>(0),
        "migration 0128 is not applied"
    );

    let mut txn = client.transaction().expect("fixture transaction");
    assert_read_committed(&mut txn);
    let (tenant_a, user_a, credential_a) = seed_identity(&mut txn, "reasoning route A");
    let (tenant_b, user_b, credential_b) = seed_identity(&mut txn, "reasoning route B");
    let model_a: Uuid = txn
        .query_one(
            "INSERT INTO control.processor_models(processor_id,provider_model_id,capabilities,status,catalog_observed_at) VALUES('processor-a','model-a',ARRAY['TEXT'],'ACTIVE',clock_timestamp()) RETURNING processor_model_id",
            &[],
        )
        .expect("model")
        .get(0);
    let account_a: Uuid = txn.query_one(
        "INSERT INTO control.provider_accounts(tenant_id,owner_user_id,processor_id,external_account_ref_hash) VALUES($1,$2,'processor-a',decode(repeat('01',32),'hex')) RETURNING provider_account_id",
        &[&tenant_a, &user_a],
    ).expect("account").get(0);
    txn.execute(
        "INSERT INTO control.reasoning_credential_bindings(credential_ref,tenant_id,owner_user_id,provider_account_id,processor_id) VALUES($1,$2,$3,$4,'processor-a')",
        &[&credential_a, &tenant_a, &user_a, &account_a],
    ).expect("typed credential binding");
    let endpoint_a: Uuid = txn.query_one(
        "INSERT INTO control.provider_endpoints(tenant_id,provider_account_id,region,service_tier,endpoint_ref) VALUES($1,$2,'us-east-1','standard','default') RETURNING endpoint_id",
        &[&tenant_a, &account_a],
    ).expect("endpoint").get(0);
    let billing_a: Uuid = txn.query_one(
        "INSERT INTO control.provider_billing_accounts(tenant_id,owner_user_id,provider_account_id,account_ref) VALUES($1,$2,$3,'account-ref') RETURNING billing_account_id",
        &[&tenant_a, &user_a, &account_a],
    ).expect("billing account").get(0);
    let instrument_a: Uuid = txn.query_one(
        "INSERT INTO control.provider_billing_instruments(tenant_id,billing_account_id,owner_user_id,payer_user_id,instrument_kind,invocation_eligibility,currency,coverage_processor_id,coverage_provider_model_id,coverage_region,coverage_service_tier,valid_from,overage_policy) VALUES($1,$2,$3,$3,'PAYG','API_CALLABLE','USD','processor-a','model-a','us-east-1','standard',clock_timestamp()-interval '1 second','DENY') RETURNING billing_instrument_id",
        &[&tenant_a, &billing_a, &user_a],
    ).expect("billing instrument").get(0);
    let profile_a: Uuid = txn.query_one(
        "INSERT INTO control.reasoning_profiles(tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,billing_account_id,default_billing_instrument_id,capabilities) VALUES($1,$2,$3,$4,$5,$6,$7,$8,ARRAY['TEXT']) RETURNING profile_id",
        &[&tenant_a, &user_a, &account_a, &endpoint_a, &model_a, &credential_a, &billing_a, &instrument_a],
    ).expect("profile").get(0);
    let policy_a: Uuid = txn.query_one(
        "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose) VALUES($1,$2,'PRIVATE_DISTILL_TEXT') RETURNING route_policy_id",
        &[&tenant_a, &user_a],
    ).expect("policy").get(0);
    txn.execute(
        "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,1,$3,1,0)",
        &[&tenant_a, &policy_a, &profile_a],
    ).expect("candidate");
    let domain_a: Uuid = txn.query_one(
        "INSERT INTO control.private_reasoning_domains(tenant_id,name,owner_user_id) VALUES($1,'route foundation domain',$2) RETURNING reasoning_domain_id",
        &[&tenant_a, &user_a],
    ).expect("domain").get(0);
    txn.batch_execute("SAVEPOINT draft_binding")
        .expect("savepoint");
    let draft_binding = txn.execute(
        "INSERT INTO control.reasoning_route_bindings(tenant_id,reasoning_domain_id,purpose,route_policy_id,route_policy_version) VALUES($1,$2,'PRIVATE_DISTILL_TEXT',$3,1)",
        &[&tenant_a, &domain_a, &policy_a],
    ).expect_err("binding must not reference DRAFT");
    assert_eq!(draft_binding.code(), Some(&SqlState::CHECK_VIOLATION));
    txn.batch_execute("ROLLBACK TO SAVEPOINT draft_binding")
        .expect("rollback");
    txn.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='SHADOW' WHERE route_policy_id=$1 AND policy_version=1",
        &[&policy_a],
    ).expect("DRAFT to SHADOW with one priority-0 candidate");
    txn.batch_execute("SAVEPOINT frozen_candidate")
        .expect("savepoint");
    let frozen_candidate = txn.execute(
        "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,1,$3,1,1)",
        &[&tenant_a, &policy_a, &profile_a],
    ).expect_err("SHADOW candidate set is frozen");
    assert_eq!(frozen_candidate.code(), Some(&SqlState::CHECK_VIOLATION));
    txn.batch_execute("ROLLBACK TO SAVEPOINT frozen_candidate")
        .expect("rollback");
    let binding_a: Uuid = txn
        .query_one(
            "INSERT INTO control.reasoning_route_bindings(tenant_id,reasoning_domain_id,purpose,route_policy_id,route_policy_version) VALUES($1,$2,'PRIVATE_DISTILL_TEXT',$3,1) RETURNING binding_id",
            &[&tenant_a, &domain_a, &policy_a],
        )
        .expect("binding")
        .get(0);
    txn.batch_execute("SAVEPOINT shadow_backtrack")
        .expect("savepoint");
    let shadow_backtrack = txn.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='DRAFT' WHERE route_policy_id=$1 AND policy_version=1",
        &[&policy_a],
    ).expect_err("SHADOW must not backtrack to DRAFT");
    assert_eq!(
        shadow_backtrack.code(),
        Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT shadow_backtrack")
        .expect("rollback");
    txn.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='SERVING' WHERE route_policy_id=$1 AND policy_version=1",
        &[&policy_a],
    ).expect("SHADOW to SERVING");
    for target in ["SHADOW", "DRAFT"] {
        txn.batch_execute("SAVEPOINT serving_backtrack")
            .expect("savepoint");
        let serving_backtrack = txn.execute(
            "UPDATE control.reasoning_route_policies SET lifecycle_state=$1 WHERE route_policy_id=$2 AND policy_version=1",
            &[&target, &policy_a],
        ).expect_err("SERVING must not backtrack");
        assert_eq!(
            serving_backtrack.code(),
            Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
        );
        txn.batch_execute("ROLLBACK TO SAVEPOINT serving_backtrack")
            .expect("rollback");
    }

    // A successor snapshot may reuse a logical id at a new version, while v1 stays exact.
    txn.execute(
        "INSERT INTO control.reasoning_profiles(profile_id,tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,billing_account_id,default_billing_instrument_id,capabilities,profile_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,ARRAY['TEXT'],2)",
        &[&profile_a, &tenant_a, &user_a, &account_a, &endpoint_a, &model_a, &credential_a, &billing_a, &instrument_a],
    ).expect("profile v2");
    txn.execute(
        "INSERT INTO control.reasoning_route_policies(route_policy_id,tenant_id,policy_owner_user_id,purpose,policy_version) VALUES($1,$2,$3,'PRIVATE_DISTILL_TEXT',2)",
        &[&policy_a, &tenant_a, &user_a],
    ).expect("policy v2");
    txn.execute(
        "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,2,$3,2,0)",
        &[&tenant_a, &policy_a, &profile_a],
    ).expect("candidate v2");
    txn.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='SHADOW' WHERE route_policy_id=$1 AND policy_version=2",
        &[&policy_a],
    ).expect("policy v2 enters SHADOW after its candidate is frozen");
    assert_eq!(
        txn.query_one("SELECT profile_version FROM control.reasoning_profiles WHERE profile_id=$1 AND profile_version=1", &[&profile_a]).expect("v1 exact read").get::<_, i64>(0),
        1
    );
    assert_eq!(
        txn.query_one("SELECT policy_version FROM control.reasoning_route_policies WHERE route_policy_id=$1 AND policy_version=1", &[&policy_a]).expect("policy v1 exact read").get::<_, i64>(0),
        1
    );
    assert_eq!(
        txn.query_one("SELECT profile_version FROM control.reasoning_route_candidates WHERE route_policy_id=$1 AND route_policy_version=1 AND profile_id=$2", &[&policy_a, &profile_a]).expect("candidate v1 exact read").get::<_, i64>(0),
        1
    );

    txn.batch_execute("SAVEPOINT direct_shadow_insert")
        .expect("savepoint");
    let direct_shadow_insert = txn.execute(
        "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose,lifecycle_state) VALUES($1,$2,'PRIVATE_DISTILL_TEXT','SHADOW')",
        &[&tenant_a, &user_a],
    ).expect_err("every policy version must start in DRAFT");
    assert_eq!(
        direct_shadow_insert.code(),
        Some(&SqlState::CHECK_VIOLATION)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT direct_shadow_insert")
        .expect("rollback");

    let zero_policy: Uuid = txn.query_one(
        "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose) VALUES($1,$2,'PRIVATE_DISTILL_TEXT') RETURNING route_policy_id",
        &[&tenant_a, &user_a],
    ).expect("zero-candidate policy").get(0);
    txn.batch_execute("SAVEPOINT zero_candidates")
        .expect("savepoint");
    let zero_candidates = txn.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='SHADOW' WHERE route_policy_id=$1 AND policy_version=1",
        &[&zero_policy],
    ).expect_err("PINNED policy cannot shadow with zero candidates");
    assert_eq!(
        zero_candidates.code(),
        Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT zero_candidates")
        .expect("rollback");

    let priority_policy: Uuid = txn.query_one(
        "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose) VALUES($1,$2,'PRIVATE_DISTILL_TEXT') RETURNING route_policy_id",
        &[&tenant_a, &user_a],
    ).expect("non-zero priority policy").get(0);
    txn.execute(
        "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,1,$3,1,1)",
        &[&tenant_a, &priority_policy, &profile_a],
    ).expect("DRAFT accepts candidate before freeze");
    txn.batch_execute("SAVEPOINT nonzero_priority")
        .expect("savepoint");
    let nonzero_priority = txn.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='SHADOW' WHERE route_policy_id=$1 AND policy_version=1",
        &[&priority_policy],
    ).expect_err("PINNED policy requires priority zero");
    assert_eq!(
        nonzero_priority.code(),
        Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT nonzero_priority")
        .expect("rollback");

    let multi_policy: Uuid = txn.query_one(
        "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose) VALUES($1,$2,'PRIVATE_DISTILL_TEXT') RETURNING route_policy_id",
        &[&tenant_a, &user_a],
    ).expect("multi-candidate policy").get(0);
    let second_profile_a: Uuid = txn.query_one(
        "INSERT INTO control.reasoning_profiles(tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,billing_account_id,default_billing_instrument_id,capabilities) VALUES($1,$2,$3,$4,$5,$6,$7,$8,ARRAY['TEXT']) RETURNING profile_id",
        &[&tenant_a, &user_a, &account_a, &endpoint_a, &model_a, &credential_a, &billing_a, &instrument_a],
    ).expect("second same-owner profile").get(0);
    for (profile_id, priority) in [(profile_a, 0_i32), (second_profile_a, 1_i32)] {
        txn.execute(
            "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,1,$3,1,$4)",
            &[&tenant_a, &multi_policy, &profile_id, &priority],
        ).expect("DRAFT may assemble candidate set");
    }
    txn.batch_execute("SAVEPOINT multiple_candidates")
        .expect("savepoint");
    let multiple_candidates = txn.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='SHADOW' WHERE route_policy_id=$1 AND policy_version=1",
        &[&multi_policy],
    ).expect_err("PINNED policy cannot shadow with multiple candidates");
    assert_eq!(
        multiple_candidates.code(),
        Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT multiple_candidates")
        .expect("rollback");

    let skip_policy: Uuid = txn.query_one(
        "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose) VALUES($1,$2,'PRIVATE_DISTILL_TEXT') RETURNING route_policy_id",
        &[&tenant_a, &user_a],
    ).expect("skip policy").get(0);
    txn.execute(
        "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,1,$3,1,0)",
        &[&tenant_a, &skip_policy, &profile_a],
    ).expect("skip policy candidate");
    txn.batch_execute("SAVEPOINT lifecycle_skip")
        .expect("savepoint");
    let lifecycle_skip = txn.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='SERVING' WHERE route_policy_id=$1 AND policy_version=1",
        &[&skip_policy],
    ).expect_err("DRAFT must not skip SHADOW");
    assert_eq!(
        lifecycle_skip.code(),
        Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT lifecycle_skip")
        .expect("rollback");

    let disabled_policy: Uuid = txn.query_one(
        "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose) VALUES($1,$2,'PRIVATE_DISTILL_TEXT') RETURNING route_policy_id",
        &[&tenant_a, &user_a],
    ).expect("disabled-candidate policy").get(0);
    txn.execute(
        "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority,enabled) VALUES($1,$2,1,$3,1,0,false)",
        &[&tenant_a, &disabled_policy, &profile_a],
    ).expect("disabled DRAFT candidate");
    txn.batch_execute("SAVEPOINT disabled_candidate")
        .expect("savepoint");
    let disabled_candidate = txn.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='SHADOW' WHERE route_policy_id=$1 AND policy_version=1",
        &[&disabled_policy],
    ).expect_err("disabled-only candidate cannot freeze a PINNED policy");
    assert_eq!(
        disabled_candidate.code(),
        Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT disabled_candidate")
        .expect("rollback");

    let closed_draft_policy: Uuid = txn.query_one(
        "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose,effective_from) VALUES($1,$2,'PRIVATE_DISTILL_TEXT',clock_timestamp()-interval '1 second') RETURNING route_policy_id",
        &[&tenant_a, &user_a],
    ).expect("closed DRAFT policy").get(0);
    txn.execute(
        "UPDATE control.reasoning_route_policies SET effective_to=clock_timestamp() WHERE route_policy_id=$1 AND policy_version=1",
        &[&closed_draft_policy],
    ).expect("close DRAFT policy");
    txn.batch_execute("SAVEPOINT closed_draft_candidate")
        .expect("savepoint");
    let closed_draft_candidate = txn.execute(
        "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,1,$3,1,0)",
        &[&tenant_a, &closed_draft_policy, &profile_a],
    ).expect_err("closed DRAFT cannot accept candidates");
    assert_eq!(
        closed_draft_candidate.code(),
        Some(&SqlState::CHECK_VIOLATION)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT closed_draft_candidate")
        .expect("rollback");
    txn.batch_execute("SAVEPOINT closed_draft_shadow")
        .expect("savepoint");
    let closed_draft_shadow = txn.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='SHADOW' WHERE route_policy_id=$1 AND policy_version=1",
        &[&closed_draft_policy],
    ).expect_err("closed DRAFT cannot advance");
    assert_eq!(
        closed_draft_shadow.code(),
        Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT closed_draft_shadow")
        .expect("rollback");

    let closed_shadow_policy: Uuid = txn.query_one(
        "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose,effective_from) VALUES($1,$2,'PRIVATE_DISTILL_TEXT',clock_timestamp()-interval '1 second') RETURNING route_policy_id",
        &[&tenant_a, &user_a],
    ).expect("closed SHADOW policy").get(0);
    txn.execute(
        "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,1,$3,1,0)",
        &[&tenant_a, &closed_shadow_policy, &profile_a],
    ).expect("closed SHADOW policy candidate");
    txn.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='SHADOW' WHERE route_policy_id=$1 AND policy_version=1",
        &[&closed_shadow_policy],
    ).expect("policy enters SHADOW before close");
    txn.execute(
        "UPDATE control.reasoning_route_policies SET effective_to=clock_timestamp() WHERE route_policy_id=$1 AND policy_version=1",
        &[&closed_shadow_policy],
    ).expect("close SHADOW policy");
    txn.batch_execute("SAVEPOINT closed_shadow_serving")
        .expect("savepoint");
    let closed_shadow_serving = txn.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='SERVING' WHERE route_policy_id=$1 AND policy_version=1",
        &[&closed_shadow_policy],
    ).expect_err("closed SHADOW cannot advance");
    assert_eq!(
        closed_shadow_serving.code(),
        Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT closed_shadow_serving")
        .expect("rollback");
    let closed_domain: Uuid = txn.query_one(
        "INSERT INTO control.private_reasoning_domains(tenant_id,name,owner_user_id) VALUES($1,'closed policy domain',$2) RETURNING reasoning_domain_id",
        &[&tenant_a, &user_a],
    ).expect("closed policy domain").get(0);
    txn.batch_execute("SAVEPOINT closed_policy_binding")
        .expect("savepoint");
    let closed_policy_binding = txn.execute(
        "INSERT INTO control.reasoning_route_bindings(tenant_id,reasoning_domain_id,purpose,route_policy_id,route_policy_version) VALUES($1,$2,'PRIVATE_DISTILL_TEXT',$3,1)",
        &[&tenant_a, &closed_domain, &closed_shadow_policy],
    ).expect_err("binding cannot reference a closed policy");
    assert_eq!(
        closed_policy_binding.code(),
        Some(&SqlState::CHECK_VIOLATION)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT closed_policy_binding")
        .expect("rollback");

    for (name, statement) in [
        (
            "profile_update",
            "UPDATE control.reasoning_profiles SET processing_region='x' WHERE profile_id=$1 AND profile_version=1",
        ),
        (
            "profile_delete",
            "DELETE FROM control.reasoning_profiles WHERE profile_id=$1 AND profile_version=1",
        ),
        (
            "policy_update",
            "UPDATE control.reasoning_route_policies SET billing_responsibility='PLATFORM' WHERE route_policy_id=$1 AND policy_version=1",
        ),
        (
            "policy_delete",
            "DELETE FROM control.reasoning_route_policies WHERE route_policy_id=$1 AND policy_version=1",
        ),
        (
            "candidate_update",
            "UPDATE control.reasoning_route_candidates SET priority=9 WHERE route_policy_id=$1 AND route_policy_version=1 AND profile_id=$2 AND profile_version=1",
        ),
        (
            "candidate_delete",
            "DELETE FROM control.reasoning_route_candidates WHERE route_policy_id=$1 AND route_policy_version=1 AND profile_id=$2 AND profile_version=1",
        ),
    ] {
        txn.batch_execute(&format!("SAVEPOINT {name}"))
            .expect("savepoint");
        let params: Vec<&(dyn postgres::types::ToSql + Sync)> = if name.starts_with("candidate") {
            vec![&policy_a, &profile_a]
        } else if name.starts_with("policy") {
            vec![&policy_a]
        } else {
            vec![&profile_a]
        };
        let failure = txn
            .execute(statement, &params)
            .expect_err("semantic snapshot mutation must fail");
        assert_eq!(
            failure.code(),
            Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE),
            "{name}"
        );
        txn.batch_execute(&format!("ROLLBACK TO SAVEPOINT {name}"))
            .expect("rollback");
    }

    for (name, statement) in [
        (
            "wrong_profile_version",
            "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,2,$3,99,4)",
        ),
        (
            "wrong_policy_version",
            "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,99,$3,2,4)",
        ),
    ] {
        txn.batch_execute(&format!("SAVEPOINT {name}"))
            .expect("savepoint");
        let failure = txn
            .execute(statement, &[&tenant_a, &policy_a, &profile_a])
            .expect_err("candidate version must be exact");
        assert_eq!(failure.code(), Some(&SqlState::CHECK_VIOLATION), "{name}");
        txn.batch_execute(&format!("ROLLBACK TO SAVEPOINT {name}"))
            .expect("rollback");
    }

    txn.batch_execute("SAVEPOINT second_current")
        .expect("savepoint");
    let second_current = txn.execute(
        "INSERT INTO control.reasoning_route_bindings(tenant_id,reasoning_domain_id,purpose,route_policy_id,route_policy_version) VALUES($1,$2,'PRIVATE_DISTILL_TEXT',$3,2)",
        &[&tenant_a, &domain_a, &policy_a],
    ).expect_err("only one current binding is permitted");
    assert_eq!(second_current.code(), Some(&SqlState::UNIQUE_VIOLATION));
    txn.batch_execute("ROLLBACK TO SAVEPOINT second_current")
        .expect("rollback");
    txn.execute(
        "UPDATE control.reasoning_route_bindings SET effective_to=clock_timestamp() WHERE binding_id=$1 AND binding_version=1",
        &[&binding_a],
    ).expect("close current binding");
    txn.execute(
        "INSERT INTO control.reasoning_route_bindings(binding_id,binding_version,tenant_id,reasoning_domain_id,purpose,route_policy_id,route_policy_version) VALUES($1,2,$2,$3,'PRIVATE_DISTILL_TEXT',$4,2)",
        &[&binding_a, &tenant_a, &domain_a, &policy_a],
    ).expect("successor binding");
    assert_eq!(txn.query_one("SELECT count(*) FROM control.reasoning_route_bindings WHERE tenant_id=$1 AND reasoning_domain_id=$2 AND purpose='PRIVATE_DISTILL_TEXT' AND effective_to IS NULL", &[&tenant_a, &domain_a]).expect("one current").get::<_, i64>(0), 1);
    let other_user_a: Uuid = txn
        .query_one(
            "INSERT INTO control.users(state) VALUES('ACTIVE') RETURNING user_id",
            &[],
        )
        .expect("other tenant A user")
        .get(0);
    txn.execute(
        "INSERT INTO control.memberships(tenant_id,user_id,role,state) VALUES($1,$2,'MEMBER','ACTIVE')",
        &[&tenant_a, &other_user_a],
    )
    .expect("other tenant A membership");
    let other_policy_a: Uuid = txn
        .query_one(
            "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose) VALUES($1,$2,'PRIVATE_DISTILL_TEXT') RETURNING route_policy_id",
            &[&tenant_a, &other_user_a],
        )
        .expect("other owner policy")
        .get(0);

    let account_b: Uuid = txn.query_one(
        "INSERT INTO control.provider_accounts(tenant_id,owner_user_id,processor_id,external_account_ref_hash) VALUES($1,$2,'processor-b',decode(repeat('02',32),'hex')) RETURNING provider_account_id",
        &[&tenant_b, &user_b],
    ).expect("other account").get(0);
    let endpoint_b: Uuid = txn.query_one(
        "INSERT INTO control.provider_endpoints(tenant_id,provider_account_id,region,service_tier,endpoint_ref) VALUES($1,$2,'us-east-1','standard','default') RETURNING endpoint_id",
        &[&tenant_b, &account_b],
    ).expect("other endpoint").get(0);
    let model_b: Uuid = txn.query_one(
        "INSERT INTO control.processor_models(processor_id,provider_model_id,capabilities,status,catalog_observed_at) VALUES('processor-b','model-b',ARRAY['TEXT'],'ACTIVE',clock_timestamp()) RETURNING processor_model_id",
        &[],
    ).expect("other model").get(0);
    txn.execute(
        "INSERT INTO control.reasoning_credential_bindings(credential_ref,tenant_id,owner_user_id,provider_account_id,processor_id) VALUES($1,$2,$3,$4,'processor-b')",
        &[&credential_b, &tenant_b, &user_b, &account_b],
    ).expect("other typed credential binding");
    let profile_b: Uuid = txn.query_one(
        "INSERT INTO control.reasoning_profiles(tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,capabilities) VALUES($1,$2,$3,$4,$5,$6,ARRAY['TEXT']) RETURNING profile_id",
        &[&tenant_b, &user_b, &account_b, &endpoint_b, &model_b, &credential_b],
    ).expect("other profile").get(0);

    txn.batch_execute("SAVEPOINT profile_spine")
        .expect("savepoint");
    let profile_spine = txn.execute(
        "INSERT INTO control.reasoning_profiles(profile_id,profile_version,tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,capabilities) VALUES($1,3,$2,$3,$4,$5,$6,$7,ARRAY['TEXT'])",
        &[&profile_a, &tenant_b, &user_b, &account_b, &endpoint_b, &model_b, &credential_b],
    ).expect_err("profile successor cannot change identity spine");
    assert_eq!(profile_spine.code(), Some(&SqlState::CHECK_VIOLATION));
    txn.batch_execute("ROLLBACK TO SAVEPOINT profile_spine")
        .expect("rollback");

    txn.batch_execute("SAVEPOINT policy_spine")
        .expect("savepoint");
    let policy_spine = txn.execute(
        "INSERT INTO control.reasoning_route_policies(route_policy_id,policy_version,tenant_id,policy_owner_user_id,purpose) VALUES($1,3,$2,$3,'PRIVATE_DISTILL_TEXT')",
        &[&policy_a, &tenant_b, &user_b],
    ).expect_err("policy successor cannot change identity spine");
    assert_eq!(policy_spine.code(), Some(&SqlState::CHECK_VIOLATION));
    txn.batch_execute("ROLLBACK TO SAVEPOINT policy_spine")
        .expect("rollback");

    let policy_b: Uuid = txn.query_one(
        "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose) VALUES($1,$2,'PRIVATE_DISTILL_TEXT') RETURNING route_policy_id",
        &[&tenant_b, &user_b],
    ).expect("tenant B policy").get(0);
    txn.execute(
        "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,1,$3,1,0)",
        &[&tenant_b, &policy_b, &profile_b],
    ).expect("tenant B candidate");
    txn.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='SHADOW' WHERE route_policy_id=$1 AND policy_version=1",
        &[&policy_b],
    ).expect("tenant B policy SHADOW");
    let domain_b: Uuid = txn.query_one(
        "INSERT INTO control.private_reasoning_domains(tenant_id,name,owner_user_id) VALUES($1,'route foundation domain B',$2) RETURNING reasoning_domain_id",
        &[&tenant_b, &user_b],
    ).expect("tenant B domain").get(0);
    txn.batch_execute("SAVEPOINT binding_spine")
        .expect("savepoint");
    let binding_spine = txn.execute(
        "INSERT INTO control.reasoning_route_bindings(binding_id,binding_version,tenant_id,reasoning_domain_id,purpose,route_policy_id,route_policy_version) VALUES($1,3,$2,$3,'PRIVATE_DISTILL_TEXT',$4,1)",
        &[&binding_a, &tenant_b, &domain_b, &policy_b],
    ).expect_err("binding successor cannot change identity spine");
    assert_eq!(binding_spine.code(), Some(&SqlState::CHECK_VIOLATION));
    txn.batch_execute("ROLLBACK TO SAVEPOINT binding_spine")
        .expect("rollback");

    txn.batch_execute("SAVEPOINT cross_owner_candidate")
        .expect("savepoint");
    let cross_owner = txn.execute(
        "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,1,$3,1,1)",
        &[&tenant_a, &other_policy_a, &profile_b],
    ).expect_err("cross-owner candidate must fail before routing");
    assert_eq!(cross_owner.code(), Some(&SqlState::CHECK_VIOLATION));
    txn.batch_execute("ROLLBACK TO SAVEPOINT cross_owner_candidate")
        .expect("rollback");

    txn.batch_execute("SAVEPOINT profile_credential_mismatch")
        .expect("savepoint");
    let credential_mismatch = txn.execute(
        "INSERT INTO control.reasoning_profiles(tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,capabilities) VALUES($1,$2,$3,$4,$5,$6,ARRAY['TEXT'])",
        &[&tenant_a, &user_a, &account_a, &endpoint_a, &model_a, &credential_b],
    ).expect_err("profile credential reference must remain in its tenant");
    assert_eq!(credential_mismatch.code(), Some(&SqlState::CHECK_VIOLATION));
    txn.batch_execute("ROLLBACK TO SAVEPOINT profile_credential_mismatch")
        .expect("rollback");

    let account_a_other: Uuid = txn.query_one(
        "INSERT INTO control.provider_accounts(tenant_id,owner_user_id,processor_id,external_account_ref_hash) VALUES($1,$2,'processor-a',decode(repeat('04',32),'hex')) RETURNING provider_account_id",
        &[&tenant_a, &user_a],
    ).expect("same-owner second account").get(0);
    let endpoint_a_other: Uuid = txn.query_one(
        "INSERT INTO control.provider_endpoints(tenant_id,provider_account_id,region,service_tier,endpoint_ref) VALUES($1,$2,'us-east-1','standard','other') RETURNING endpoint_id",
        &[&tenant_a, &account_a_other],
    ).expect("same-owner second endpoint").get(0);
    txn.batch_execute("SAVEPOINT wrong_credential_account")
        .expect("savepoint");
    let wrong_credential_account = txn.execute(
        "INSERT INTO control.reasoning_profiles(tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,capabilities) VALUES($1,$2,$3,$4,$5,$6,ARRAY['TEXT'])",
        &[&tenant_a, &user_a, &account_a_other, &endpoint_a_other, &model_a, &credential_a],
    ).expect_err("credential authority cannot move to another same-owner account");
    assert_eq!(
        wrong_credential_account.code(),
        Some(&SqlState::CHECK_VIOLATION)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT wrong_credential_account")
        .expect("rollback");

    let wrong_purpose_credential: Uuid = txn.query_one(
        "INSERT INTO control.credentials(tenant_id,purpose,openbao_ref) VALUES($1,'OTHER',$2) RETURNING credential_id",
        &[&tenant_a, &format!("openbao://fixture/{}", Uuid::new_v4())],
    ).expect("wrong purpose credential").get(0);
    txn.batch_execute("SAVEPOINT wrong_credential_purpose")
        .expect("savepoint");
    let wrong_credential_purpose = txn.execute(
        "INSERT INTO control.reasoning_credential_bindings(credential_ref,tenant_id,owner_user_id,provider_account_id,processor_id) VALUES($1,$2,$3,$4,'processor-a')",
        &[&wrong_purpose_credential, &tenant_a, &user_a, &account_a],
    ).expect_err("binding cannot relabel a non-reasoning base credential");
    assert_eq!(
        wrong_credential_purpose.code(),
        Some(&SqlState::CHECK_VIOLATION)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT wrong_credential_purpose")
        .expect("rollback");
    txn.batch_execute("SAVEPOINT interactive_only")
        .expect("savepoint");
    let interactive_only = txn.execute(
        "INSERT INTO control.reasoning_credential_bindings(credential_ref,tenant_id,owner_user_id,provider_account_id,processor_id,invocation_eligibility) VALUES($1,$2,$3,$4,'processor-a','INTERACTIVE_ONLY')",
        &[&credential_a, &tenant_a, &user_a, &account_a],
    ).expect_err("interactive-only credential must never authorize API routing");
    assert_eq!(interactive_only.code(), Some(&SqlState::CHECK_VIOLATION));
    txn.batch_execute("ROLLBACK TO SAVEPOINT interactive_only")
        .expect("rollback");
    txn.batch_execute("SAVEPOINT interactive_instrument")
        .expect("savepoint");
    let interactive_instrument: Uuid = txn.query_one(
        "INSERT INTO control.provider_billing_instruments(tenant_id,billing_account_id,owner_user_id,payer_user_id,instrument_kind,invocation_eligibility,currency,coverage_processor_id,coverage_provider_model_id,coverage_region,coverage_service_tier,valid_from,overage_policy) VALUES($1,$2,$3,$3,'PAYG','INTERACTIVE_ONLY','USD','processor-a','model-a','us-east-1','standard',clock_timestamp(),'DENY') RETURNING billing_instrument_id",
        &[&tenant_a, &billing_a, &user_a],
    ).expect("interactive-only instrument is a typed record").get(0);
    let interactive_instrument_rejected = txn.execute(
        "INSERT INTO control.reasoning_profiles(tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,billing_account_id,default_billing_instrument_id,capabilities) VALUES($1,$2,$3,$4,$5,$6,$7,$8,ARRAY['TEXT'])",
        &[&tenant_a, &user_a, &account_a, &endpoint_a, &model_a, &credential_a, &billing_a, &interactive_instrument],
    ).expect_err("interactive-only billing instrument must never authorize API routing");
    assert_eq!(
        interactive_instrument_rejected.code(),
        Some(&SqlState::CHECK_VIOLATION)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT interactive_instrument")
        .expect("rollback");

    txn.batch_execute("SAVEPOINT coverage_mismatch")
        .expect("savepoint");
    let premium_endpoint: Uuid = txn.query_one(
        "INSERT INTO control.provider_endpoints(tenant_id,provider_account_id,region,service_tier,endpoint_ref) VALUES($1,$2,'us-east-1','premium','premium') RETURNING endpoint_id",
        &[&tenant_a, &account_a],
    ).expect("premium endpoint").get(0);
    let coverage_mismatch = txn.execute(
        "INSERT INTO control.reasoning_profiles(tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,billing_account_id,default_billing_instrument_id,capabilities) VALUES($1,$2,$3,$4,$5,$6,$7,$8,ARRAY['TEXT'])",
        &[&tenant_a, &user_a, &account_a, &premium_endpoint, &model_a, &credential_a, &billing_a, &instrument_a],
    ).expect_err("instrument coverage must be exact for processor/model/region/tier");
    assert_eq!(coverage_mismatch.code(), Some(&SqlState::CHECK_VIOLATION));
    txn.batch_execute("ROLLBACK TO SAVEPOINT coverage_mismatch")
        .expect("rollback");
    txn.batch_execute("SAVEPOINT parent_drift")
        .expect("savepoint");
    let parent_drift = txn.execute("UPDATE control.provider_accounts SET processor_id='processor-b' WHERE provider_account_id=$1", &[&account_a])
        .expect_err("immutable profile parent identity must not drift");
    assert_eq!(
        parent_drift.code(),
        Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT parent_drift")
        .expect("rollback");
    txn.execute(
        "UPDATE control.reasoning_profiles SET enabled=false WHERE profile_id=$1 AND profile_version=2",
        &[&profile_a],
    )
    .expect("profile administrative disable is allowed");
    txn.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='SERVING' WHERE route_policy_id=$1 AND policy_version=2",
        &[&policy_a],
    )
    .expect("policy lifecycle transition is allowed");

    txn.execute(
        "UPDATE control.provider_endpoints SET enabled=false WHERE endpoint_id=$1",
        &[&endpoint_a],
    )
    .expect("disable endpoint before profile authority gates");
    txn.batch_execute("SAVEPOINT disabled_endpoint_profile")
        .expect("savepoint");
    let disabled_endpoint_profile = txn.execute(
        "INSERT INTO control.reasoning_profiles(tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,capabilities) VALUES($1,$2,$3,$4,$5,$6,ARRAY['TEXT'])",
        &[&tenant_a, &user_a, &account_a, &endpoint_a, &model_a, &credential_a],
    ).expect_err("disabled endpoint cannot authorize a new profile");
    assert_eq!(
        disabled_endpoint_profile.code(),
        Some(&SqlState::CHECK_VIOLATION)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT disabled_endpoint_profile")
        .expect("rollback");
    txn.execute(
        "UPDATE control.reasoning_profiles SET enabled=false WHERE profile_id=$1 AND profile_version=1",
        &[&profile_a],
    ).expect("profile disable succeeds with disabled endpoint");
    txn.batch_execute("SAVEPOINT disabled_endpoint_reenable")
        .expect("savepoint");
    let disabled_endpoint_reenable = txn.execute(
        "UPDATE control.reasoning_profiles SET enabled=true WHERE profile_id=$1 AND profile_version=1",
        &[&profile_a],
    ).expect_err("disabled endpoint must block profile re-enable");
    assert_eq!(
        disabled_endpoint_reenable.code(),
        Some(&SqlState::CHECK_VIOLATION)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT disabled_endpoint_reenable")
        .expect("rollback");
    txn.execute(
        "UPDATE control.provider_endpoints SET enabled=true WHERE endpoint_id=$1",
        &[&endpoint_a],
    )
    .expect("restore endpoint while account authority is active");
    txn.execute(
        "UPDATE control.reasoning_profiles SET enabled=true WHERE profile_id=$1 AND profile_version=1",
        &[&profile_a],
    ).expect("restore profile after endpoint authority returns");

    txn.execute(
        "UPDATE control.provider_billing_accounts SET enabled=false WHERE billing_account_id=$1",
        &[&billing_a],
    )
    .expect("disable billing account before profile authority gates");
    txn.batch_execute("SAVEPOINT disabled_billing_profile")
        .expect("savepoint");
    let disabled_billing_profile = txn.execute(
        "INSERT INTO control.reasoning_profiles(tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,billing_account_id,default_billing_instrument_id,capabilities) VALUES($1,$2,$3,$4,$5,$6,$7,$8,ARRAY['TEXT'])",
        &[&tenant_a, &user_a, &account_a, &endpoint_a, &model_a, &credential_a, &billing_a, &instrument_a],
    ).expect_err("disabled billing account cannot authorize a new profile");
    assert_eq!(
        disabled_billing_profile.code(),
        Some(&SqlState::CHECK_VIOLATION)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT disabled_billing_profile")
        .expect("rollback");
    txn.execute(
        "UPDATE control.reasoning_profiles SET enabled=false WHERE profile_id=$1 AND profile_version=1",
        &[&profile_a],
    ).expect("profile disable succeeds with disabled billing account");
    txn.batch_execute("SAVEPOINT disabled_billing_reenable")
        .expect("savepoint");
    let disabled_billing_reenable = txn.execute(
        "UPDATE control.reasoning_profiles SET enabled=true WHERE profile_id=$1 AND profile_version=1",
        &[&profile_a],
    ).expect_err("disabled billing account must block profile re-enable");
    assert_eq!(
        disabled_billing_reenable.code(),
        Some(&SqlState::CHECK_VIOLATION)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT disabled_billing_reenable")
        .expect("rollback");
    txn.execute(
        "UPDATE control.provider_billing_accounts SET enabled=true WHERE billing_account_id=$1",
        &[&billing_a],
    )
    .expect("restore billing account while provider authority is active");
    txn.execute(
        "UPDATE control.reasoning_profiles SET enabled=true WHERE profile_id=$1 AND profile_version=1",
        &[&profile_a],
    ).expect("restore profile after billing authority returns");

    txn.execute(
        "UPDATE control.processor_models SET status='RETIRED' WHERE processor_model_id=$1",
        &[&model_a],
    )
    .expect("model administrative status transition is allowed");
    txn.batch_execute("SAVEPOINT model_catalog_drift")
        .expect("savepoint");
    let model_catalog_drift = txn.execute(
        "UPDATE control.processor_models SET catalog_observed_at=clock_timestamp() WHERE processor_model_id=$1",
        &[&model_a],
    ).expect_err("model catalog evidence is immutable when attached to a profile");
    assert_eq!(
        model_catalog_drift.code(),
        Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT model_catalog_drift")
        .expect("rollback");
    txn.batch_execute("SAVEPOINT model_request_drift")
        .expect("savepoint");
    let model_request_drift = txn
        .execute(
            "UPDATE control.processor_models SET provider_request_id='rewritten' WHERE processor_model_id=$1",
            &[&model_a],
        )
        .expect_err("model request evidence is immutable when attached to a profile");
    assert_eq!(
        model_request_drift.code(),
        Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT model_request_drift")
        .expect("rollback");
    txn.batch_execute("SAVEPOINT credential_binding_mutation")
        .expect("savepoint");
    let credential_binding_mutation = txn.execute(
        "UPDATE control.reasoning_credential_bindings SET processor_id='processor-b' WHERE credential_ref=$1 AND provider_account_id=$2",
        &[&credential_a, &account_a],
    ).expect_err("credential authority binding is append-only");
    assert_eq!(
        credential_binding_mutation.code(),
        Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT credential_binding_mutation")
        .expect("rollback");
    txn.batch_execute("SAVEPOINT credential_binding_delete")
        .expect("savepoint");
    let credential_binding_delete = txn.execute(
        "DELETE FROM control.reasoning_credential_bindings WHERE credential_ref=$1 AND provider_account_id=$2",
        &[&credential_a, &account_a],
    ).expect_err("credential authority binding cannot be deleted");
    assert_eq!(
        credential_binding_delete.code(),
        Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT credential_binding_delete")
        .expect("rollback");

    txn.batch_execute("SAVEPOINT payer_escalation")
        .expect("savepoint");
    let payer_escalation = txn.execute(
        "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose,billing_responsibility) VALUES($1,$2,'PRIVATE_DISTILL_TEXT','PLATFORM')",
        &[&tenant_a, &user_a],
    ).expect_err("R1 must reject platform payer on a user route");
    assert_eq!(payer_escalation.code(), Some(&SqlState::CHECK_VIOLATION));
    txn.batch_execute("ROLLBACK TO SAVEPOINT payer_escalation")
        .expect("rollback");

    txn.batch_execute("SAVEPOINT public_trust")
        .expect("savepoint");
    let public_trust = txn.execute(
        "INSERT INTO control.provider_accounts(tenant_id,owner_user_id,processor_id,external_account_ref_hash,trust_domain) VALUES($1,$2,'processor-a',decode(repeat('03',32),'hex'),'PLATFORM_PUBLIC')",
        &[&tenant_a, &user_a],
    ).expect_err("R1 private foundation must reject public trust domain");
    assert_eq!(public_trust.code(), Some(&SqlState::CHECK_VIOLATION));
    txn.batch_execute("ROLLBACK TO SAVEPOINT public_trust")
        .expect("rollback");

    txn.batch_execute("SAVEPOINT binding_owner_mismatch")
        .expect("savepoint");
    let binding_owner_mismatch = txn.execute(
        "INSERT INTO control.reasoning_route_bindings(tenant_id,reasoning_domain_id,purpose,route_policy_id,route_policy_version,effective_from) VALUES($1,$2,'PRIVATE_DISTILL_TEXT',$3,1,clock_timestamp()+interval '1 second')",
        &[&tenant_a, &domain_a, &other_policy_a],
    ).expect_err("binding must reject a policy owned by a different user");
    assert_eq!(
        binding_owner_mismatch.code(),
        Some(&SqlState::CHECK_VIOLATION)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT binding_owner_mismatch")
        .expect("rollback");

    let close_after_revocation: Uuid = txn.query_one(
        "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose,effective_from) VALUES($1,$2,'PRIVATE_DISTILL_TEXT',clock_timestamp()-interval '1 second') RETURNING route_policy_id",
        &[&tenant_a, &user_a],
    ).expect("policy to close after membership revocation").get(0);
    let advance_after_revocation: Uuid = txn.query_one(
        "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose) VALUES($1,$2,'PRIVATE_DISTILL_TEXT') RETURNING route_policy_id",
        &[&tenant_a, &user_a],
    ).expect("policy to advance after membership revocation").get(0);
    txn.execute(
        "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,1,$3,1,0)",
        &[&tenant_a, &advance_after_revocation, &profile_a],
    ).expect("candidate before parent revocation");
    let binding_close_policy: Uuid = txn.query_one(
        "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose) VALUES($1,$2,'PRIVATE_DISTILL_TEXT') RETURNING route_policy_id",
        &[&tenant_a, &user_a],
    ).expect("policy for post-revocation binding close").get(0);
    txn.execute(
        "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,1,$3,1,0)",
        &[&tenant_a, &binding_close_policy, &profile_a],
    ).expect("candidate for post-revocation binding close");
    txn.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='SHADOW' WHERE route_policy_id=$1 AND policy_version=1",
        &[&binding_close_policy],
    ).expect("SHADOW policy for post-revocation binding close");
    let binding_close_domain: Uuid = txn.query_one(
        "INSERT INTO control.private_reasoning_domains(tenant_id,name,owner_user_id) VALUES($1,'post-revocation binding close',$2) RETURNING reasoning_domain_id",
        &[&tenant_a, &user_a],
    ).expect("domain for post-revocation binding close").get(0);
    let binding_close_id: Uuid = txn.query_one(
        "INSERT INTO control.reasoning_route_bindings(tenant_id,reasoning_domain_id,purpose,route_policy_id,route_policy_version) VALUES($1,$2,'PRIVATE_DISTILL_TEXT',$3,1) RETURNING binding_id",
        &[&tenant_a, &binding_close_domain, &binding_close_policy],
    ).expect("current binding before membership revocation").get(0);

    txn.execute(
        "UPDATE control.provider_accounts SET enabled=false WHERE provider_account_id=$1",
        &[&account_a],
    )
    .expect("account kill switch remains available");
    txn.execute(
        "UPDATE control.provider_endpoints SET enabled=false WHERE endpoint_id=$1",
        &[&endpoint_a],
    )
    .expect("endpoint kill switch remains available after account disable");
    txn.execute(
        "UPDATE control.provider_billing_accounts SET enabled=false WHERE billing_account_id=$1",
        &[&billing_a],
    )
    .expect("billing account kill switch remains available after account disable");
    txn.execute(
        "UPDATE control.provider_billing_instruments SET enabled=false WHERE billing_instrument_id=$1",
        &[&instrument_a],
    ).expect("billing instrument kill switch remains available after parent disable");
    txn.execute(
        "UPDATE control.reasoning_profiles SET enabled=false WHERE profile_id=$1 AND profile_version=1",
        &[&profile_a],
    ).expect("profile kill switch remains available after parent disable");

    for (savepoint, statement, id) in [
        (
            "endpoint_reenable",
            "UPDATE control.provider_endpoints SET enabled=true WHERE endpoint_id=$1",
            endpoint_a,
        ),
        (
            "billing_reenable",
            "UPDATE control.provider_billing_accounts SET enabled=true WHERE billing_account_id=$1",
            billing_a,
        ),
        (
            "instrument_reenable",
            "UPDATE control.provider_billing_instruments SET enabled=true WHERE billing_instrument_id=$1",
            instrument_a,
        ),
        (
            "profile_reenable",
            "UPDATE control.reasoning_profiles SET enabled=true WHERE profile_id=$1 AND profile_version=1",
            profile_a,
        ),
    ] {
        txn.batch_execute(&format!("SAVEPOINT {savepoint}"))
            .expect("re-enable savepoint");
        let rejected = txn
            .execute(statement, &[&id])
            .expect_err("re-enable must revalidate current parent authority");
        assert_eq!(rejected.code(), Some(&SqlState::CHECK_VIOLATION));
        txn.batch_execute(&format!("ROLLBACK TO SAVEPOINT {savepoint}"))
            .expect("re-enable rollback");
    }

    txn.execute(
        "UPDATE control.processor_models SET status='RETIRED' WHERE processor_model_id=$1",
        &[&model_b],
    )
    .expect("retire second model");
    txn.execute(
        "UPDATE control.reasoning_profiles SET enabled=false WHERE profile_id=$1 AND profile_version=1",
        &[&profile_b],
    ).expect("profile disable remains available after model retirement");
    txn.batch_execute("SAVEPOINT retired_model_reenable")
        .expect("savepoint");
    let retired_model_reenable = txn.execute(
        "UPDATE control.reasoning_profiles SET enabled=true WHERE profile_id=$1 AND profile_version=1",
        &[&profile_b],
    ).expect_err("retired model must block profile re-enable");
    assert_eq!(
        retired_model_reenable.code(),
        Some(&SqlState::CHECK_VIOLATION)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT retired_model_reenable")
        .expect("rollback");

    txn.execute(
        "UPDATE control.memberships SET state='SUSPENDED' WHERE tenant_id=$1 AND user_id=$2",
        &[&tenant_a, &user_a],
    )
    .expect("revoke owner membership authority");
    txn.execute(
        "UPDATE control.provider_accounts SET updated_at=clock_timestamp() WHERE provider_account_id=$1",
        &[&account_a],
    ).expect("updated_at-only maintenance does not require live parent authority");
    txn.execute(
        "UPDATE control.reasoning_route_policies SET effective_to=clock_timestamp() WHERE route_policy_id=$1 AND policy_version=1",
        &[&close_after_revocation],
    ).expect("policy close remains available after membership revocation");
    txn.execute(
        "UPDATE control.reasoning_route_bindings SET effective_to=clock_timestamp() WHERE binding_id=$1 AND binding_version=1",
        &[&binding_close_id],
    ).expect("binding close remains available after membership revocation");
    assert!(
        txn.query_one(
            "SELECT effective_to IS NOT NULL FROM control.reasoning_route_bindings WHERE binding_id=$1 AND binding_version=1",
            &[&binding_close_id],
        ).expect("closed binding readback").get::<_, bool>(0)
    );
    txn.batch_execute("SAVEPOINT binding_second_close")
        .expect("savepoint");
    let binding_second_close = txn.execute(
        "UPDATE control.reasoning_route_bindings SET effective_to=clock_timestamp() WHERE binding_id=$1 AND binding_version=1",
        &[&binding_close_id],
    ).expect_err("binding cannot close twice");
    assert_eq!(
        binding_second_close.code(),
        Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT binding_second_close")
        .expect("rollback");
    txn.batch_execute("SAVEPOINT binding_close_semantic_drift")
        .expect("savepoint");
    let binding_close_semantic_drift = txn.execute(
        "UPDATE control.reasoning_route_bindings SET route_policy_id=$1 WHERE binding_id=$2 AND binding_version=1",
        &[&policy_a, &binding_close_id],
    ).expect_err("closed binding cannot carry semantic drift");
    assert_eq!(
        binding_close_semantic_drift.code(),
        Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT binding_close_semantic_drift")
        .expect("rollback");

    txn.batch_execute("SAVEPOINT account_reenable_after_revocation")
        .expect("savepoint");
    let account_reenable = txn
        .execute(
            "UPDATE control.provider_accounts SET enabled=true WHERE provider_account_id=$1",
            &[&account_a],
        )
        .expect_err("revoked membership must block account re-enable");
    assert_eq!(account_reenable.code(), Some(&SqlState::CHECK_VIOLATION));
    txn.batch_execute("ROLLBACK TO SAVEPOINT account_reenable_after_revocation")
        .expect("rollback");

    txn.batch_execute("SAVEPOINT policy_advance_after_revocation")
        .expect("savepoint");
    let policy_advance = txn.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='SHADOW' WHERE route_policy_id=$1 AND policy_version=1",
        &[&advance_after_revocation],
    ).expect_err("revoked membership must block lifecycle advance");
    assert_eq!(policy_advance.code(), Some(&SqlState::CHECK_VIOLATION));
    txn.batch_execute("ROLLBACK TO SAVEPOINT policy_advance_after_revocation")
        .expect("rollback");
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL 18 migrated through 0128"]
#[allow(
    clippy::too_many_lines,
    reason = "one concurrent integration scenario verifies version-spine and policy-candidate race safety"
)]
fn version_spines_and_policy_candidate_freeze_are_race_safe() {
    let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PostgreSQL 18 DSN");
    // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
    let mut seed = Client::connect(&dsn, NoTls).expect("isolated PostgreSQL 18");
    let version: i32 = seed
        .query_one("SHOW server_version_num", &[])
        .expect("server version")
        .get::<_, String>(0)
        .parse()
        .expect("numeric server version");
    assert!(version >= 180_000, "requires PostgreSQL 18, got {version}");
    let mut txn = seed.transaction().expect("race seed transaction");
    assert_read_committed(&mut txn);
    let lane_a = seed_route_lane(&mut txn, &format!("reasoning race A {}", Uuid::new_v4()));
    let lane_b = seed_route_lane(&mut txn, &format!("reasoning race B {}", Uuid::new_v4()));
    let binding_policy_a = seed_shadow_policy(&mut txn, &lane_a);
    let binding_policy_b = seed_shadow_policy(&mut txn, &lane_b);
    let freeze_policy: Uuid = txn.query_one(
        "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose) VALUES($1,$2,'PRIVATE_DISTILL_TEXT') RETURNING route_policy_id",
        &[&lane_a.tenant, &lane_a.user],
    ).expect("freeze policy").get(0);
    txn.execute(
        "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,1,$3,1,0)",
        &[&lane_a.tenant, &freeze_policy, &lane_a.profile],
    ).expect("freeze policy initial candidate");
    txn.execute(
        "INSERT INTO control.reasoning_profiles(profile_id,profile_version,tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,capabilities) VALUES($1,2,$2,$3,$4,$5,$6,$7,ARRAY['TEXT'])",
        &[&lane_a.profile, &lane_a.tenant, &lane_a.user, &lane_a.account, &lane_a.endpoint, &lane_a.model, &lane_a.credential],
    ).expect("same-spine profile v2");
    txn.commit().expect("commit race fixtures");

    let counts_before: (i64, i64, i64) = {
        let row = seed.query_one(
            "SELECT (SELECT count(*) FROM control.reasoning_profiles), (SELECT count(*) FROM control.reasoning_route_policies), (SELECT count(*) FROM control.reasoning_route_bindings)",
            &[],
        ).expect("spine counts before non-RC gates");
        (row.get(0), row.get(1), row.get(2))
    };
    for (isolation_sql, isolation_name, prefix) in [
        ("REPEATABLE READ", "repeatable read", "rr"),
        ("SERIALIZABLE", "serializable", "serial"),
    ] {
        // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
        let mut isolated = Client::connect(&dsn, NoTls).expect("non-RC gate connection");
        isolated
            .batch_execute(&format!("BEGIN ISOLATION LEVEL {isolation_sql}"))
            .expect("non-RC begin");
        assert_eq!(
            isolated
                .query_one("SHOW transaction_isolation", &[])
                .expect("non-RC isolation")
                .get::<_, String>(0),
            isolation_name
        );
        for (suffix, statement) in [
            (
                "profile",
                format!(
                    "INSERT INTO control.reasoning_profiles(profile_id,profile_version,tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,capabilities) VALUES('{}',1,'{}','{}','{}','{}','{}','{}',ARRAY['TEXT'])",
                    Uuid::new_v4(),
                    lane_a.tenant,
                    lane_a.user,
                    lane_a.account,
                    lane_a.endpoint,
                    lane_a.model,
                    lane_a.credential
                ),
            ),
            (
                "policy",
                format!(
                    "INSERT INTO control.reasoning_route_policies(route_policy_id,policy_version,tenant_id,policy_owner_user_id,purpose) VALUES('{}',1,'{}','{}','PRIVATE_DISTILL_TEXT')",
                    Uuid::new_v4(),
                    lane_a.tenant,
                    lane_a.user
                ),
            ),
            (
                "binding",
                format!(
                    "INSERT INTO control.reasoning_route_bindings(binding_id,binding_version,tenant_id,reasoning_domain_id,purpose,route_policy_id,route_policy_version) VALUES('{}',1,'{}','{}','PRIVATE_DISTILL_TEXT','{binding_policy_a}',1)",
                    Uuid::new_v4(),
                    lane_a.tenant,
                    lane_a.domain
                ),
            ),
        ] {
            let savepoint = format!("{prefix}_{suffix}");
            isolated
                .batch_execute(&format!("SAVEPOINT {savepoint}"))
                .expect("non-RC savepoint");
            let rejected = isolated
                .execute(&statement, &[])
                .expect_err("spine insert must fail closed outside READ COMMITTED");
            assert_eq!(
                rejected.code(),
                Some(&SqlState::T_R_SERIALIZATION_FAILURE),
                "{savepoint}"
            );
            isolated
                .batch_execute(&format!("ROLLBACK TO SAVEPOINT {savepoint}"))
                .expect("non-RC rollback");
        }
        isolated
            .batch_execute("ROLLBACK")
            .expect("non-RC gate rollback");
    }
    let counts_after: (i64, i64, i64) = {
        let row = seed.query_one(
            "SELECT (SELECT count(*) FROM control.reasoning_profiles), (SELECT count(*) FROM control.reasoning_route_policies), (SELECT count(*) FROM control.reasoning_route_bindings)",
            &[],
        ).expect("spine counts after non-RC gates");
        (row.get(0), row.get(1), row.get(2))
    };
    assert_eq!(
        counts_after, counts_before,
        "non-RC rejection must not write rows"
    );

    let profile_id = Uuid::new_v4();
    assert_cross_spine_successor_waits_then_rejects(
        &dsn,
        format!(
            "INSERT INTO control.reasoning_profiles(profile_id,profile_version,tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,capabilities) VALUES('{profile_id}',1,'{}','{}','{}','{}','{}','{}',ARRAY['TEXT'])",
            lane_a.tenant,
            lane_a.user,
            lane_a.account,
            lane_a.endpoint,
            lane_a.model,
            lane_a.credential
        ),
        format!(
            "INSERT INTO control.reasoning_profiles(profile_id,profile_version,tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,capabilities) VALUES('{profile_id}',2,'{}','{}','{}','{}','{}','{}',ARRAY['TEXT'])",
            lane_b.tenant,
            lane_b.user,
            lane_b.account,
            lane_b.endpoint,
            lane_b.model,
            lane_b.credential
        ),
    );

    let policy_id = Uuid::new_v4();
    assert_cross_spine_successor_waits_then_rejects(
        &dsn,
        format!(
            "INSERT INTO control.reasoning_route_policies(route_policy_id,policy_version,tenant_id,policy_owner_user_id,purpose) VALUES('{policy_id}',1,'{}','{}','PRIVATE_DISTILL_TEXT')",
            lane_a.tenant, lane_a.user
        ),
        format!(
            "INSERT INTO control.reasoning_route_policies(route_policy_id,policy_version,tenant_id,policy_owner_user_id,purpose) VALUES('{policy_id}',2,'{}','{}','PRIVATE_DISTILL_TEXT')",
            lane_b.tenant, lane_b.user
        ),
    );

    let binding_id = Uuid::new_v4();
    assert_cross_spine_successor_waits_then_rejects(
        &dsn,
        format!(
            "INSERT INTO control.reasoning_route_bindings(binding_id,binding_version,tenant_id,reasoning_domain_id,purpose,route_policy_id,route_policy_version) VALUES('{binding_id}',1,'{}','{}','PRIVATE_DISTILL_TEXT','{binding_policy_a}',1)",
            lane_a.tenant, lane_a.domain
        ),
        format!(
            "INSERT INTO control.reasoning_route_bindings(binding_id,binding_version,tenant_id,reasoning_domain_id,purpose,route_policy_id,route_policy_version) VALUES('{binding_id}',2,'{}','{}','PRIVATE_DISTILL_TEXT','{binding_policy_b}',1)",
            lane_b.tenant, lane_b.domain
        ),
    );

    // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
    let mut transition = Client::connect(&dsn, NoTls).expect("transition race connection");
    transition
        .batch_execute("BEGIN ISOLATION LEVEL READ COMMITTED")
        .expect("transition race begin");
    assert_read_committed(&mut transition);
    transition
        .execute(
            "UPDATE control.reasoning_route_policies SET lifecycle_state='SHADOW' WHERE route_policy_id=$1 AND policy_version=1",
            &[&freeze_policy],
        )
        .expect("uncommitted DRAFT to SHADOW transition");
    let (candidate_pid, candidate) = spawn_transaction_statement(
        &dsn,
        format!(
            "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES('{}','{freeze_policy}',1,'{}',2,1)",
            lane_a.tenant, lane_a.profile
        ),
    );
    // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
    let mut observer = Client::connect(&dsn, NoTls).expect("candidate lock observer");
    wait_for_ungranted_lock(&mut observer, candidate_pid, false);
    transition
        .batch_execute("COMMIT")
        .expect("publish SHADOW transition");
    assert_eq!(
        candidate.join().expect("candidate thread"),
        Err("23514".to_owned())
    );
    let row = seed.query_one(
        "SELECT policy.lifecycle_state, count(candidate.*) FROM control.reasoning_route_policies policy JOIN control.reasoning_route_candidates candidate ON candidate.tenant_id=policy.tenant_id AND candidate.route_policy_id=policy.route_policy_id AND candidate.route_policy_version=policy.policy_version WHERE policy.route_policy_id=$1 AND policy.policy_version=1 GROUP BY policy.lifecycle_state",
        &[&freeze_policy],
    ).expect("freeze race result");
    let lifecycle: String = row.get(0);
    let candidates: i64 = row.get(1);
    assert_eq!((lifecycle.as_str(), candidates), ("SHADOW", 1));
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL 18 migrated through 0128"]
fn route_foundation_tables_are_acl_protected_until_r2_activation() {
    let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PostgreSQL 18 DSN");
    // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
    let mut client = Client::connect(&dsn, NoTls).expect("isolated PostgreSQL 18");
    let mut txn = client.transaction().expect("fixture transaction");
    for role in [
        "role_private_worker",
        "role_public_worker",
        "role_retrieval_worker",
    ] {
        for table in [
            "reasoning_profiles",
            "reasoning_route_policies",
            "reasoning_route_candidates",
            "reasoning_route_bindings",
            "reasoning_credential_bindings",
        ] {
            txn.batch_execute("SAVEPOINT acl").expect("savepoint");
            // dep: PostgreSQL(owner) — test switches PG role to exercise RLS
            txn.batch_execute(&format!("SET LOCAL ROLE {role}"))
                .expect("known runtime role");
            let denied = txn
                .query(&format!("SELECT * FROM control.{table} LIMIT 1"), &[])
                .expect_err("R1 foundation tables must be unavailable before activation");
            assert_eq!(
                denied.code(),
                Some(&SqlState::INSUFFICIENT_PRIVILEGE),
                "{role} {table}"
            );
            txn.batch_execute("ROLLBACK TO SAVEPOINT acl")
                .expect("rollback");
        }
    }
}
