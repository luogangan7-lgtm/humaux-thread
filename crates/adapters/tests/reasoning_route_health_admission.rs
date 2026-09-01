//! Focused PostgreSQL 18 gates for Phase 9 R3 exact Binding-only admission.

use postgres::{Client, GenericClient, NoTls};
use uuid::Uuid;

#[derive(Clone)]
struct Lane {
    tenant: Uuid,
    user: Uuid,
    credential: Uuid,
    account: Uuid,
    endpoint: Uuid,
    egress_processor: Option<Uuid>,
    endpoint_ref: String,
    processor: String,
    model: Uuid,
    provider_model: String,
    revision: Option<String>,
    region: String,
    tier: String,
    profile: Uuid,
    policy: Uuid,
    domain: Uuid,
    binding: Uuid,
    billing_account: Option<Uuid>,
    billing_instrument: Option<Uuid>,
}

fn dsn() -> String {
    std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PostgreSQL 18 DSN")
}

#[allow(
    clippy::too_many_lines,
    reason = "fixture seeds the complete route health dependency graph in one transaction"
)]
fn seed_lane(
    db: &mut impl GenericClient,
    label: &str,
    billing: bool,
    serving: bool,
    provision_egress: bool,
) -> Lane {
    let suffix = Uuid::new_v4();
    let tenant: Uuid = db
        .query_one(
            "INSERT INTO control.tenants(name) VALUES($1) RETURNING tenant_id",
            &[&format!("r3-{label}-{suffix}")],
        )
        .expect("tenant")
        .get(0);
    let user: Uuid = db
        .query_one(
            "INSERT INTO control.users(state) VALUES('ACTIVE') RETURNING user_id",
            &[],
        )
        .expect("user")
        .get(0);
    db.execute(
        "INSERT INTO control.memberships(tenant_id,user_id,role,state) VALUES($1,$2,'OWNER','ACTIVE')",
        &[&tenant, &user],
    )
    .expect("membership");
    let credential: Uuid = db
        .query_one(
            "INSERT INTO control.credentials(tenant_id,purpose,openbao_ref) VALUES($1,'USER_REASONING',$2) RETURNING credential_id",
            &[&tenant, &format!("openbao://r3/{suffix}")],
        )
        .expect("credential locator")
        .get(0);
    let processor = format!("processor-{suffix}");
    let provider_model = format!("model-{suffix}");
    let revision = Some(format!("revision-{suffix}"));
    let model: Uuid = db
        .query_one(
            "INSERT INTO control.processor_models(processor_id,provider_model_id,model_revision,capabilities,status,catalog_observed_at) VALUES($1,$2,$3,ARRAY['TEXT'],'ACTIVE',clock_timestamp()) RETURNING processor_model_id",
            &[&processor, &provider_model, &revision],
        )
        .expect("processor model")
        .get(0);
    let account_hash = suffix.as_bytes().repeat(2);
    let account: Uuid = db
        .query_one(
            "INSERT INTO control.provider_accounts(tenant_id,owner_user_id,processor_id,external_account_ref_hash) VALUES($1,$2,$3,$4) RETURNING provider_account_id",
            &[&tenant, &user, &processor, &account_hash],
        )
        .expect("provider account")
        .get(0);
    db.execute(
        "INSERT INTO control.reasoning_credential_bindings(credential_ref,tenant_id,owner_user_id,provider_account_id,processor_id) VALUES($1,$2,$3,$4,$5)",
        &[&credential, &tenant, &user, &account, &processor],
    )
    .expect("credential binding");
    let endpoint_ref = format!("endpoint-{suffix}");
    let region = "region-r3".to_owned();
    let tier = "tier-r3".to_owned();
    let egress_processor = provision_egress.then(Uuid::new_v4);
    let endpoint: Uuid = db
        .query_one(
            "INSERT INTO control.provider_endpoints(tenant_id,provider_account_id,region,service_tier,endpoint_ref,egress_processor_id) VALUES($1,$2,$3,$4,$5,$6) RETURNING endpoint_id",
            &[&tenant, &account, &region, &tier, &endpoint_ref, &egress_processor],
        )
        .expect("provider endpoint")
        .get(0);
    let (billing_account, billing_instrument) = if billing {
        let billing_account: Uuid = db
            .query_one(
                "INSERT INTO control.provider_billing_accounts(tenant_id,owner_user_id,provider_account_id,account_ref) VALUES($1,$2,$3,$4) RETURNING billing_account_id",
                &[&tenant, &user, &account, &format!("billing-{suffix}")],
            )
            .expect("billing account")
            .get(0);
        let billing_instrument: Uuid = db
            .query_one(
                "INSERT INTO control.provider_billing_instruments(tenant_id,billing_account_id,owner_user_id,payer_user_id,instrument_kind,invocation_eligibility,currency,coverage_processor_id,coverage_provider_model_id,coverage_model_revision,coverage_region,coverage_service_tier,valid_from,overage_policy) VALUES($1,$2,$3,$3,'PAYG','API_CALLABLE','USD',$4,$5,$6,$7,$8,clock_timestamp()-interval '1 hour','PAYG') RETURNING billing_instrument_id",
                &[&tenant, &billing_account, &user, &processor, &provider_model, &revision, &region, &tier],
            )
            .expect("billing instrument")
            .get(0);
        (Some(billing_account), Some(billing_instrument))
    } else {
        (None, None)
    };
    let profile: Uuid = db
        .query_one(
            "INSERT INTO control.reasoning_profiles(tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,billing_account_id,default_billing_instrument_id,capabilities,processing_region) VALUES($1,$2,$3,$4,$5,$6,$7,$8,ARRAY['TEXT'],$9) RETURNING profile_id",
            &[&tenant, &user, &account, &endpoint, &model, &credential, &billing_account, &billing_instrument, &region],
        )
        .expect("reasoning profile")
        .get(0);
    let domain: Uuid = db
        .query_one(
            "INSERT INTO control.private_reasoning_domains(tenant_id,name,owner_user_id,status) VALUES($1,$2,$3,'ACTIVE') RETURNING reasoning_domain_id",
            &[&tenant, &format!("domain-{suffix}"), &user],
        )
        .expect("reasoning domain")
        .get(0);
    let policy: Uuid = db
        .query_one(
            "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose) VALUES($1,$2,'CONTRIBUTION_DEIDENTIFY') RETURNING route_policy_id",
            &[&tenant, &user],
        )
        .expect("route policy")
        .get(0);
    db.execute(
        "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,1,$3,1,0)",
        &[&tenant, &policy, &profile],
    )
    .expect("pinned candidate");
    db.execute(
        "UPDATE control.reasoning_route_policies SET lifecycle_state='SHADOW' WHERE route_policy_id=$1 AND policy_version=1",
        &[&policy],
    )
    .expect("shadow policy");
    if serving {
        db.execute(
            "UPDATE control.reasoning_route_policies SET lifecycle_state='SERVING' WHERE route_policy_id=$1 AND policy_version=1",
            &[&policy],
        )
        .expect("serving policy");
    }
    let binding: Uuid = db
        .query_one(
            "INSERT INTO control.reasoning_route_bindings(tenant_id,reasoning_domain_id,purpose,route_policy_id,route_policy_version) VALUES($1,$2,'CONTRIBUTION_DEIDENTIFY',$3,1) RETURNING binding_id",
            &[&tenant, &domain, &policy],
        )
        .expect("route binding")
        .get(0);
    Lane {
        tenant,
        user,
        credential,
        account,
        endpoint,
        egress_processor,
        endpoint_ref,
        processor,
        model,
        provider_model,
        revision,
        region,
        tier,
        profile,
        policy,
        domain,
        binding,
        billing_account,
        billing_instrument,
    }
}

fn set_tenant(db: &mut impl GenericClient, tenant: Uuid) {
    db.query_one(
        "SELECT set_config('humaux.tenant_id',$1,false)",
        &[&tenant.to_string()],
    )
    .expect("tenant GUC");
}

fn resolve_count(db: &mut impl GenericClient, lane: &Lane) -> i64 {
    db.query_one(
        "SELECT count(*) FROM control.resolve_user_reasoning_admission($1,1,$2,'CONTRIBUTION_DEIDENTIFY')",
        &[&lane.binding, &lane.domain],
    )
    .expect("resolve admission")
    .get(0)
}

fn insert_provider(
    db: &mut impl GenericClient,
    lane: &Lane,
    verdict: &str,
    observed_offset_seconds: i64,
    valid_offset_seconds: i64,
) -> i64 {
    db.query_one(
        "INSERT INTO ops.reasoning_provider_health_observations(tenant_id,processor_id,processor_model_id,provider_model_id,model_revision,provider_endpoint_id,endpoint_ref,region,service_tier,source_kind,reason_code,verdict,observed_at,valid_until) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'TEST',NULL,$10,clock_timestamp()+$11::bigint*interval '1 second',clock_timestamp()+$12::bigint*interval '1 second') RETURNING observation_id",
        &[&lane.tenant, &lane.processor, &lane.model, &lane.provider_model, &lane.revision, &lane.endpoint, &lane.endpoint_ref, &lane.region, &lane.tier, &verdict, &observed_offset_seconds, &valid_offset_seconds],
    )
    .expect("provider observation")
    .get(0)
}

#[allow(
    clippy::too_many_arguments,
    reason = "fixture mirrors the account health observation columns explicitly"
)]
fn insert_account(
    db: &mut impl GenericClient,
    lane: &Lane,
    account_verdict: &str,
    credential_verdict: &str,
    billing_account_verdict: Option<&str>,
    billing_instrument_verdict: Option<&str>,
    observed_offset_seconds: i64,
    valid_offset_seconds: i64,
) -> i64 {
    db.query_one(
        "INSERT INTO ops.reasoning_account_health_observations(tenant_id,provider_account_id,credential_ref,billing_account_id,billing_instrument_id,source_kind,reason_code,account_verdict,credential_verdict,billing_account_verdict,billing_instrument_verdict,observed_at,valid_until) VALUES($1,$2,$3,$4,$5,'TEST',NULL,$6,$7,$8,$9,clock_timestamp()+$10::bigint*interval '1 second',clock_timestamp()+$11::bigint*interval '1 second') RETURNING observation_id",
        &[&lane.tenant, &lane.account, &lane.credential, &lane.billing_account, &lane.billing_instrument, &account_verdict, &credential_verdict, &billing_account_verdict, &billing_instrument_verdict, &observed_offset_seconds, &valid_offset_seconds],
    )
    .expect("account observation")
    .get(0)
}

fn insert_fresh_pair(db: &mut impl GenericClient, lane: &Lane) -> (i64, i64) {
    let provider = insert_provider(db, lane, "HEALTHY", -1, 300);
    let account = insert_account(
        db,
        lane,
        "HEALTHY",
        "VALID",
        lane.billing_account.map(|_| "ENABLED"),
        lane.billing_instrument.map(|_| "ENABLED"),
        -1,
        300,
    );
    (provider, account)
}

fn reserve_reasoning_call(
    db: &mut impl GenericClient,
    lane: &Lane,
    request_id: Uuid,
    call_kind: &str,
    intent_byte: u8,
) -> Uuid {
    let intent_sha256 = vec![intent_byte; 32];
    db.query_one(
        "INSERT INTO ops.model_call_ledger(request_id,tenant_id,purpose,call_kind,intent_sha256,provider,model,model_revision,reasoning_domain_id,binding_id,binding_version,route_policy_id,route_policy_version,profile_id,profile_version,provider_account_id,provider_endpoint_id,egress_processor_id,credential_ref,billing_account_id,billing_instrument_id,provider_health_observation_id,account_health_observation_id,billing_responsibility,admitted_at) SELECT $3,admission.tenant_id,admission.purpose,$4,$5,admission.processor_id,admission.provider_model_id,admission.model_revision,admission.reasoning_domain_id,admission.binding_id,admission.binding_version,admission.route_policy_id,admission.route_policy_version,admission.profile_id,admission.profile_version,admission.provider_account_id,admission.provider_endpoint_id,admission.egress_processor_id,admission.credential_ref,admission.billing_account_id,admission.billing_instrument_id,admission.provider_health_observation_id,admission.account_health_observation_id,'USER',admission.admitted_at FROM control.resolve_user_reasoning_admission($1,1,$2,'CONTRIBUTION_DEIDENTIFY') admission RETURNING model_call_id",
        &[&lane.binding, &lane.domain, &request_id, &call_kind, &intent_sha256],
    )
    .expect("reserve exact reasoning model call")
    .get(0)
}

fn reserve_reasoning_disclosure(
    db: &mut impl GenericClient,
    lane: &Lane,
    model_call_id: Uuid,
    processor_id: Uuid,
) -> Uuid {
    let digest = vec![7_u8; 32];
    db.query_one(
        "INSERT INTO ops.data_disclosures(grant_id,tenant_id,processor_id,region,data_class,purpose,payload_sha256,payload_bytes,model_call_id) VALUES($1,$2,$3,$4,'PRIVATE','USER_REASONING',$5,17,$6) RETURNING disclosure_id",
        &[&Uuid::new_v4(), &lane.tenant, &processor_id, &lane.region, &digest, &model_call_id],
    )
    .expect("reserve exact reasoning disclosure")
    .get(0)
}

fn insert_reasoning_candidate(
    db: &mut impl GenericClient,
    lane: &Lane,
    contribution_policy: Uuid,
    model_call_id: Uuid,
    profile_version: i64,
) -> Result<u64, postgres::Error> {
    let manifest_hash = vec![3_u8; 32];
    let payload = b"deidentified candidate".to_vec();
    db.execute(
        "INSERT INTO staging.contribution_candidates(tenant_id,user_id,policy_id,policy_version,policy_snapshot,reasoning_domain_id,profile_version,source_manifest_hash,source_count,disclosed_payload,disclosed_payload_sha256,provider_trace,scan_receipt,rights_basis,binding_id,binding_version,model_call_id) VALUES($1,$2,$3,1,jsonb_build_object('policy','MANUAL','principal_id',$2::uuid::text,'allowed_workspace_ids','[]'::jsonb),$4,$5,$6,1,$7,sha256($7),'r3-provider-trace',jsonb_build_object('privacy_rules_version','r3','privacy_rules_digest','r3','gitleaks_version','r3','gitleaks_binary_sha256',repeat('a',64)),'USER_CONSENT',$8,1,$9)",
        &[&lane.tenant, &lane.user, &contribution_policy, &lane.domain, &profile_version, &manifest_hash, &payload, &lane.binding, &model_call_id],
    )
}

fn begin_case(db: &mut Client, name: &str) {
    db.batch_execute(&format!("BEGIN; SAVEPOINT {name}"))
        .expect("begin negative case");
}

fn end_case(db: &mut Client, name: &str) {
    db.batch_execute(&format!("ROLLBACK TO SAVEPOINT {name}; ROLLBACK"))
        .expect("rollback negative case");
}

#[test]
#[ignore = "requires isolated PostgreSQL 18 migrated through 0130"]
#[allow(
    clippy::too_many_lines,
    reason = "matrix acceptance test enumerates the complete admission decision surface"
)]
fn reasoning_route_health_admission_matrix() {
    let mut db = Client::connect(&dsn(), NoTls).expect("PostgreSQL 18");
    let lane = seed_lane(&mut db, "matrix", true, true, true);
    set_tenant(&mut db, lane.tenant);

    assert_eq!(resolve_count(&mut db, &lane), 0, "missing health denies");

    let other_credential: Uuid = db
        .query_one(
            "INSERT INTO control.credentials(tenant_id,purpose,openbao_ref) VALUES($1,'USER_REASONING',$2) RETURNING credential_id",
            &[&lane.tenant, &format!("openbao://r3/mismatch-{}", Uuid::new_v4())],
        )
        .expect("mismatch credential")
        .get(0);
    db.execute(
        "INSERT INTO ops.reasoning_provider_health_observations(tenant_id,processor_id,processor_model_id,provider_model_id,model_revision,provider_endpoint_id,endpoint_ref,region,service_tier,source_kind,reason_code,verdict,observed_at,valid_until) VALUES($1,$2,$3,$4,'wrong-revision',$5,$6,$7,$8,'TEST','FAULT_IDENTITY','HEALTHY',clock_timestamp()-interval '1 second',clock_timestamp()+interval '5 minutes')",
        &[&lane.tenant, &lane.processor, &lane.model, &lane.provider_model, &lane.endpoint, &lane.endpoint_ref, &lane.region, &lane.tier],
    ).expect("revision mismatch observation");
    db.execute(
        "INSERT INTO ops.reasoning_provider_health_observations(tenant_id,processor_id,processor_model_id,provider_model_id,model_revision,provider_endpoint_id,endpoint_ref,region,service_tier,source_kind,reason_code,verdict,observed_at,valid_until) VALUES($1,$2,$3,$4,$5,$6,'wrong-endpoint',$7,$8,'TEST','FAULT_IDENTITY','HEALTHY',clock_timestamp()-interval '1 second',clock_timestamp()+interval '5 minutes')",
        &[&lane.tenant, &lane.processor, &lane.model, &lane.provider_model, &lane.revision, &lane.endpoint, &lane.region, &lane.tier],
    ).expect("endpoint mismatch observation");
    db.execute(
        "INSERT INTO ops.reasoning_account_health_observations(tenant_id,provider_account_id,credential_ref,billing_account_id,billing_instrument_id,source_kind,reason_code,account_verdict,credential_verdict,billing_account_verdict,billing_instrument_verdict,observed_at,valid_until) VALUES($1,$2,$3,$4,$5,'TEST','FAULT_IDENTITY','HEALTHY','VALID','ENABLED','ENABLED',clock_timestamp()-interval '1 second',clock_timestamp()+interval '5 minutes')",
        &[&lane.tenant, &lane.account, &other_credential, &lane.billing_account, &lane.billing_instrument],
    ).expect("account tuple mismatch observation");
    assert_eq!(
        resolve_count(&mut db, &lane),
        0,
        "identity mismatch is missing, never substitution"
    );

    insert_provider(&mut db, &lane, "HEALTHY", -120, -60);
    insert_account(
        &mut db,
        &lane,
        "HEALTHY",
        "VALID",
        Some("ENABLED"),
        Some("ENABLED"),
        -120,
        -60,
    );
    assert_eq!(resolve_count(&mut db, &lane), 0, "stale latest denies");

    let (provider_observation, account_observation) = insert_fresh_pair(&mut db, &lane);
    let row = db
        .query_one(
            "SELECT * FROM control.resolve_user_reasoning_admission($1,1,$2,'CONTRIBUTION_DEIDENTIFY')",
            &[&lane.binding, &lane.domain],
        )
        .expect("fresh exact health admits");
    assert_eq!(row.get::<_, Uuid>(0), lane.tenant);
    assert_eq!(row.get::<_, Uuid>(7), lane.profile);
    assert_eq!(
        row.get::<_, i64>("provider_health_observation_id"),
        provider_observation
    );
    assert_eq!(
        row.get::<_, i64>("account_health_observation_id"),
        account_observation
    );
    assert_eq!(
        row.get::<_, Uuid>("egress_processor_id"),
        lane.egress_processor.expect("egress")
    );
    assert_eq!(row.columns().len(), 25, "frozen locator shape");

    for verdict in ["DEGRADED", "UNAVAILABLE", "UNKNOWN"] {
        begin_case(&mut db, "provider_verdict");
        insert_provider(&mut db, &lane, verdict, 0, 300);
        assert_eq!(
            resolve_count(&mut db, &lane),
            0,
            "provider {verdict} denies"
        );
        end_case(&mut db, "provider_verdict");
    }
    let account_negatives = [
        ("UNHEALTHY", "VALID", Some("ENABLED"), Some("ENABLED")),
        ("UNKNOWN", "VALID", Some("ENABLED"), Some("ENABLED")),
        ("HEALTHY", "INVALID", Some("ENABLED"), Some("ENABLED")),
        ("HEALTHY", "UNKNOWN", Some("ENABLED"), Some("ENABLED")),
        ("HEALTHY", "VALID", Some("DISABLED"), Some("ENABLED")),
        ("HEALTHY", "VALID", Some("UNKNOWN"), Some("ENABLED")),
        ("HEALTHY", "VALID", Some("ENABLED"), Some("DISABLED")),
        ("HEALTHY", "VALID", Some("ENABLED"), Some("UNKNOWN")),
    ];
    for (account, credential, billing, instrument) in account_negatives {
        begin_case(&mut db, "account_verdict");
        insert_account(
            &mut db, &lane, account, credential, billing, instrument, 0, 300,
        );
        assert_eq!(
            resolve_count(&mut db, &lane),
            0,
            "negative account component denies"
        );
        end_case(&mut db, "account_verdict");
    }

    begin_case(&mut db, "provider_future");
    insert_provider(&mut db, &lane, "HEALTHY", 60, 120);
    assert_eq!(
        resolve_count(&mut db, &lane),
        0,
        "future provider observation denies"
    );
    end_case(&mut db, "provider_future");
    begin_case(&mut db, "account_future");
    insert_account(
        &mut db,
        &lane,
        "HEALTHY",
        "VALID",
        Some("ENABLED"),
        Some("ENABLED"),
        60,
        120,
    );
    assert_eq!(
        resolve_count(&mut db, &lane),
        0,
        "future account observation denies"
    );
    end_case(&mut db, "account_future");

    begin_case(&mut db, "same_timestamp_tie");
    db.execute(
        "WITH observed AS MATERIALIZED (SELECT clock_timestamp() AS at) INSERT INTO ops.reasoning_provider_health_observations(tenant_id,processor_id,processor_model_id,provider_model_id,model_revision,provider_endpoint_id,endpoint_ref,region,service_tier,source_kind,reason_code,verdict,observed_at,valid_until) SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,'TEST','FAULT_TIE',verdict,observed.at,observed.at+interval '5 minutes' FROM observed CROSS JOIN (VALUES ('HEALTHY',1),('UNAVAILABLE',2)) verdicts(verdict,ordinal) ORDER BY ordinal",
        &[&lane.tenant, &lane.processor, &lane.model, &lane.provider_model, &lane.revision, &lane.endpoint, &lane.endpoint_ref, &lane.region, &lane.tier],
    ).expect("same timestamp observations");
    assert_eq!(
        resolve_count(&mut db, &lane),
        0,
        "larger observation id wins exact tie"
    );
    end_case(&mut db, "same_timestamp_tie");

    begin_case(&mut db, "latest_negative");
    insert_provider(&mut db, &lane, "UNAVAILABLE", 0, 300);
    db.execute(
        "INSERT INTO ops.reasoning_provider_health_observations(tenant_id,processor_id,processor_model_id,provider_model_id,model_revision,provider_endpoint_id,endpoint_ref,region,service_tier,source_kind,reason_code,verdict,observed_at,valid_until) VALUES($1,'other-processor',$2,$3,$4,$5,$6,$7,$8,'TEST','FAULT_IDENTITY','HEALTHY',clock_timestamp()+interval '1 second',clock_timestamp()+interval '5 minutes')",
        &[&lane.tenant, &lane.model, &lane.provider_model, &lane.revision, &lane.endpoint, &lane.endpoint_ref, &lane.region, &lane.tier],
    ).expect("non-exact healthy alternative");
    assert_eq!(
        resolve_count(&mut db, &lane),
        0,
        "latest exact negative never falls back"
    );
    end_case(&mut db, "latest_negative");

    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM control.resolve_user_reasoning_admission($1,1,$2,'PRIVATE_DISTILL_TEXT')",
            &[&lane.binding, &lane.domain],
        )
        .expect("purpose mismatch")
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM control.resolve_user_reasoning_admission($1,1,$2,'CONTRIBUTION_DEIDENTIFY')",
            &[&lane.binding, &Uuid::new_v4()],
        )
        .expect("domain mismatch")
        .get::<_, i64>(0),
        0
    );

    for (name, update) in [
        (
            "account_disabled",
            "UPDATE control.provider_accounts SET enabled=false,status='DISABLED',updated_at=clock_timestamp() WHERE provider_account_id=$1",
        ),
        (
            "endpoint_disabled",
            "UPDATE control.provider_endpoints SET enabled=false,updated_at=clock_timestamp() WHERE endpoint_id=$1",
        ),
        (
            "profile_disabled",
            "UPDATE control.reasoning_profiles SET enabled=false,updated_at=clock_timestamp() WHERE profile_id=$1 AND profile_version=1",
        ),
        (
            "billing_disabled",
            "UPDATE control.provider_billing_accounts SET enabled=false,updated_at=clock_timestamp() WHERE billing_account_id=$1",
        ),
        (
            "instrument_disabled",
            "UPDATE control.provider_billing_instruments SET enabled=false,updated_at=clock_timestamp() WHERE billing_instrument_id=$1",
        ),
    ] {
        begin_case(&mut db, name);
        let id = match name {
            "account_disabled" => lane.account,
            "endpoint_disabled" => lane.endpoint,
            "profile_disabled" => lane.profile,
            "billing_disabled" => lane.billing_account.expect("billing account"),
            _ => lane.billing_instrument.expect("billing instrument"),
        };
        db.execute(update, &[&id])
            .expect("disable current admin row");
        assert_eq!(
            resolve_count(&mut db, &lane),
            0,
            "current admin disable denies"
        );
        end_case(&mut db, name);
    }

    begin_case(&mut db, "credential_missing");
    db.batch_execute("SET LOCAL session_replication_role=replica")
        .expect("fault injection mode");
    db.execute(
        "DELETE FROM control.reasoning_credential_bindings WHERE credential_ref=$1 AND provider_account_id=$2",
        &[&lane.credential, &lane.account],
    )
    .expect("remove current credential authority fixture");
    db.batch_execute("SET LOCAL session_replication_role=origin")
        .expect("restore triggers");
    assert_eq!(
        resolve_count(&mut db, &lane),
        0,
        "missing credential authority denies"
    );
    end_case(&mut db, "credential_missing");

    begin_case(&mut db, "old_binding");
    db.execute(
        "UPDATE control.reasoning_route_bindings SET effective_to=clock_timestamp() WHERE binding_id=$1 AND binding_version=1",
        &[&lane.binding],
    )
    .expect("close old binding");
    db.execute(
        "INSERT INTO control.reasoning_route_bindings(binding_id,binding_version,tenant_id,reasoning_domain_id,purpose,route_policy_id,route_policy_version) VALUES($1,2,$2,$3,'CONTRIBUTION_DEIDENTIFY',$4,1)",
        &[&lane.binding, &lane.tenant, &lane.domain, &lane.policy],
    )
    .expect("binding successor");
    assert_eq!(
        resolve_count(&mut db, &lane),
        0,
        "closed historical binding denies"
    );
    let current: i64 = db.query_one(
        "SELECT count(*) FROM control.resolve_user_reasoning_admission($1,2,$2,'CONTRIBUTION_DEIDENTIFY')",
        &[&lane.binding, &lane.domain],
    ).expect("current binding successor").get(0);
    assert_eq!(current, 1, "exact current binding version admits");
    end_case(&mut db, "old_binding");

    begin_case(&mut db, "zero_candidate");
    db.batch_execute("SET LOCAL session_replication_role=replica")
        .expect("fault injection mode");
    db.execute(
        "DELETE FROM control.reasoning_route_candidates WHERE route_policy_id=$1 AND route_policy_version=1",
        &[&lane.policy],
    )
    .expect("zero candidate fixture");
    db.batch_execute("SET LOCAL session_replication_role=origin")
        .expect("restore triggers");
    assert_eq!(resolve_count(&mut db, &lane), 0, "zero candidate denies");
    end_case(&mut db, "zero_candidate");

    begin_case(&mut db, "two_candidates");
    let second_profile: Uuid = db.query_one(
        "INSERT INTO control.reasoning_profiles(tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,billing_account_id,default_billing_instrument_id,capabilities,processing_region) VALUES($1,$2,$3,$4,$5,$6,$7,$8,ARRAY['TEXT'],$9) RETURNING profile_id",
        &[&lane.tenant, &lane.user, &lane.account, &lane.endpoint, &lane.model, &lane.credential, &lane.billing_account, &lane.billing_instrument, &lane.region],
    ).expect("second profile fixture").get(0);
    db.batch_execute("SET LOCAL session_replication_role=replica")
        .expect("fault injection mode");
    db.execute(
        "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,1,$3,1,1)",
        &[&lane.tenant, &lane.policy, &second_profile],
    )
    .expect("second candidate fault fixture");
    db.batch_execute("SET LOCAL session_replication_role=origin")
        .expect("restore triggers");
    assert_eq!(
        resolve_count(&mut db, &lane),
        0,
        "two candidates deny without substitution"
    );
    end_case(&mut db, "two_candidates");

    let shadow = seed_lane(&mut db, "shadow", false, false, true);
    insert_fresh_pair(&mut db, &shadow);
    set_tenant(&mut db, shadow.tenant);
    assert_eq!(resolve_count(&mut db, &shadow), 0, "SHADOW never admits");

    let legacy_null_egress = seed_lane(&mut db, "legacy-null-egress", false, true, false);
    insert_fresh_pair(&mut db, &legacy_null_egress);
    set_tenant(&mut db, legacy_null_egress.tenant);
    assert_eq!(
        resolve_count(&mut db, &legacy_null_egress),
        0,
        "SERVING endpoint without recipient authority denies"
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM ops.model_call_ledger WHERE tenant_id=$1 AND purpose='CONTRIBUTION_DEIDENTIFY'",
            &[&legacy_null_egress.tenant],
        )
        .expect("denial side effects")
        .get::<_, i64>(0),
        0,
        "resolver denials never reserve a provider attempt"
    );
}

#[test]
#[ignore = "requires isolated PostgreSQL 18 migrated through 0130"]
#[allow(
    clippy::too_many_lines,
    reason = "ACL acceptance test covers append-only and nullable shape cases together"
)]
fn reasoning_route_health_acl_append_only_and_null_shape() {
    let dsn = dsn();
    let mut owner = Client::connect(&dsn, NoTls).expect("PostgreSQL 18");
    let lane = seed_lane(&mut owner, "acl", false, true, true);
    set_tenant(&mut owner, lane.tenant);
    insert_fresh_pair(&mut owner, &lane);
    assert_eq!(
        resolve_count(&mut owner, &lane),
        1,
        "NULL billing shape admits with NULL components"
    );

    for statement in [
        "UPDATE ops.reasoning_provider_health_observations SET verdict='UNKNOWN'",
        "DELETE FROM ops.reasoning_provider_health_observations",
        "UPDATE ops.reasoning_account_health_observations SET account_verdict='UNKNOWN'",
        "DELETE FROM ops.reasoning_account_health_observations",
        "TRUNCATE ops.reasoning_provider_health_observations",
        "TRUNCATE ops.reasoning_account_health_observations",
    ] {
        owner
            .batch_execute("BEGIN; SAVEPOINT append_only")
            .expect("savepoint");
        assert!(
            owner.batch_execute(statement).is_err(),
            "{statement} must fail closed"
        );
        owner
            .batch_execute("ROLLBACK TO SAVEPOINT append_only; ROLLBACK")
            .expect("rollback");
    }

    for statement in [
        format!(
            "INSERT INTO ops.reasoning_account_health_observations(tenant_id,provider_account_id,credential_ref,source_kind,reason_code,account_verdict,credential_verdict,billing_account_verdict,observed_at,valid_until) VALUES('{}','{}','{}','TEST','FAULT_NULL_SHAPE','HEALTHY','VALID','ENABLED',clock_timestamp(),clock_timestamp()+interval '1 minute')",
            lane.tenant, lane.account, lane.credential
        ),
        format!(
            "INSERT INTO ops.reasoning_account_health_observations(tenant_id,provider_account_id,credential_ref,billing_account_id,source_kind,reason_code,account_verdict,credential_verdict,observed_at,valid_until) VALUES('{}','{}','{}','{}','TEST','FAULT_NULL_SHAPE','HEALTHY','VALID',clock_timestamp(),clock_timestamp()+interval '1 minute')",
            lane.tenant,
            lane.account,
            lane.credential,
            Uuid::new_v4()
        ),
    ] {
        owner
            .batch_execute("BEGIN; SAVEPOINT null_shape")
            .expect("savepoint");
        assert!(
            owner.batch_execute(&statement).is_err(),
            "invalid billing null shape must fail"
        );
        owner
            .batch_execute("ROLLBACK TO SAVEPOINT null_shape; ROLLBACK")
            .expect("rollback");
    }

    owner
        .batch_execute("BEGIN; SAVEPOINT nil_egress")
        .expect("nil egress case");
    assert!(owner.execute(
        "INSERT INTO control.provider_endpoints(tenant_id,provider_account_id,region,service_tier,endpoint_ref,egress_processor_id) VALUES($1,$2,$3,$4,$5,'00000000-0000-0000-0000-000000000000')",
        &[&lane.tenant, &lane.account, &lane.region, &lane.tier, &format!("nil-{}", Uuid::new_v4())],
    ).is_err(), "nil recipient authority cannot become an endpoint route");
    owner
        .batch_execute("ROLLBACK TO SAVEPOINT nil_egress; ROLLBACK")
        .expect("nil rollback");

    let metadata = owner.query_one(
        "SELECT a.attnotnull, b.attnotnull, a.atthasdef, b.atthasdef, EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='staging.contribution_candidates'::regclass AND conname='contribution_candidates_reasoning_binding_exact_fk' AND contype='f' AND convalidated) FROM pg_attribute a JOIN pg_attribute b ON b.attrelid=a.attrelid WHERE a.attrelid='staging.contribution_candidates'::regclass AND a.attname='binding_id' AND b.attname='binding_version'",
        &[],
    ).expect("legacy-safe binding metadata");
    assert!(!metadata.get::<_, bool>(0) && !metadata.get::<_, bool>(1));
    assert!(!metadata.get::<_, bool>(2) && !metadata.get::<_, bool>(3));
    assert!(metadata.get::<_, bool>(4), "exact binding FK is validated");

    let definition: String = owner.query_one(
        "SELECT pg_get_functiondef('control.resolve_user_reasoning_admission(uuid,bigint,uuid,text)'::regprocedure)",
        &[],
    ).expect("resolver definition").get(0);
    let lower = definition.to_ascii_lowercase();
    for forbidden in [
        "openbao_ref",
        "external_account_ref_hash",
        "secret",
        "http_",
        "dblink",
        "fallback_same_payer",
    ] {
        assert!(
            !lower.contains(forbidden),
            "resolver leaks or performs forbidden action: {forbidden}"
        );
    }
    assert_eq!(
        definition.matches("clock_timestamp()").count(),
        1,
        "one captured wall clock"
    );

    let mut worker = Client::connect(&dsn, NoTls).expect("worker connection");
    worker
        .batch_execute("BEGIN; SET LOCAL ROLE role_private_worker")
        .expect("worker role");
    worker
        .query_one(
            "SELECT set_config('humaux.tenant_id',$1,true)",
            &[&lane.tenant.to_string()],
        )
        .expect("worker tenant GUC");
    let count: i64 = worker.query_one(
        "SELECT count(*) FROM control.resolve_user_reasoning_admission($1,1,$2,'CONTRIBUTION_DEIDENTIFY')",
        &[&lane.binding, &lane.domain],
    ).expect("worker EXECUTE only resolver").get(0);
    assert_eq!(count, 1);
    worker
        .batch_execute("SAVEPOINT direct_table")
        .expect("savepoint");
    assert!(
        worker
            .query(
                "SELECT * FROM ops.reasoning_provider_health_observations",
                &[]
            )
            .is_err()
    );
    worker
        .batch_execute("ROLLBACK TO SAVEPOINT direct_table")
        .expect("rollback denial");
    assert!(
        worker
            .query(
                "SELECT * FROM ops.reasoning_account_health_observations",
                &[]
            )
            .is_err()
    );
    worker.batch_execute("ROLLBACK").expect("worker rollback");

    assert!(!owner.query_one(
        "SELECT has_function_privilege(0,'control.resolve_user_reasoning_admission(uuid,bigint,uuid,text)','EXECUTE')",
        &[],
    ).expect("PUBLIC ACL").get::<_, bool>(0));
    assert!(!owner.query_one(
        "SELECT has_function_privilege('role_gateway','control.resolve_user_reasoning_admission(uuid,bigint,uuid,text)','EXECUTE')",
        &[],
    ).expect("gateway ACL").get::<_, bool>(0));
}

#[test]
#[ignore = "requires isolated PostgreSQL 18 migrated through 0130"]
#[allow(
    clippy::too_many_lines,
    reason = "ledger acceptance test verifies disclosure, candidate and atomicity invariants together"
)]
fn reasoning_attempt_ledger_disclosure_candidate_and_atomicity() {
    let mut db = Client::connect(&dsn(), NoTls).expect("PostgreSQL 18");
    let lane = seed_lane(&mut db, "ledger", true, true, true);
    let egress = lane.egress_processor.expect("provisioned recipient");
    set_tenant(&mut db, lane.tenant);
    insert_fresh_pair(&mut db, &lane);

    let mut worker = Client::connect(&dsn(), NoTls).expect("private worker connection");
    worker
        .batch_execute("BEGIN; SET LOCAL ROLE role_private_worker")
        .expect("private worker reserve transaction");
    worker
        .query_one(
            "SELECT set_config('humaux.tenant_id',$1,true)",
            &[&lane.tenant.to_string()],
        )
        .expect("worker tenant GUC");
    let worker_call =
        reserve_reasoning_call(&mut worker, &lane, Uuid::new_v4(), "COVERAGE_PROBE", 4);
    reserve_reasoning_disclosure(&mut worker, &lane, worker_call, egress);
    worker
        .batch_execute("ROLLBACK")
        .expect("worker reserve rollback");

    db.execute(
        "INSERT INTO ops.model_call_ledger(tenant_id,provider,purpose) VALUES($1,'legacy-retrieval','embedding')",
        &[&lane.tenant],
    )
    .expect("legacy retrieval row keeps nullable reasoning shape");
    let legacy_nonnull: i32 = db
        .query_one(
            "SELECT num_nonnulls(call_kind,intent_sha256,binding_id,binding_version,egress_processor_id,admitted_at) FROM ops.model_call_ledger WHERE tenant_id=$1 AND provider='legacy-retrieval'",
            &[&lane.tenant],
        )
        .expect("legacy reasoning shape")
        .get(0);
    assert_eq!(legacy_nonnull, 0);

    let nil_intent_sha256 = vec![8_u8; 32];
    assert!(
        db.query_one(
            "INSERT INTO ops.model_call_ledger(request_id,tenant_id,purpose,call_kind,intent_sha256,provider,model,model_revision,reasoning_domain_id,binding_id,binding_version,route_policy_id,route_policy_version,profile_id,profile_version,provider_account_id,provider_endpoint_id,egress_processor_id,credential_ref,billing_account_id,billing_instrument_id,provider_health_observation_id,account_health_observation_id,billing_responsibility,admitted_at) SELECT $3,admission.tenant_id,admission.purpose,'COVERAGE_PROBE',$4,admission.processor_id,admission.provider_model_id,admission.model_revision,admission.reasoning_domain_id,admission.binding_id,admission.binding_version,admission.route_policy_id,admission.route_policy_version,admission.profile_id,admission.profile_version,admission.provider_account_id,admission.provider_endpoint_id,admission.egress_processor_id,admission.credential_ref,admission.billing_account_id,admission.billing_instrument_id,admission.provider_health_observation_id,admission.account_health_observation_id,'USER',admission.admitted_at FROM control.resolve_user_reasoning_admission($1,1,$2,'CONTRIBUTION_DEIDENTIFY') admission RETURNING model_call_id",
            &[&lane.binding, &lane.domain, &Uuid::nil(), &nil_intent_sha256],
        )
        .is_err(),
        "CONTRIBUTION_DEIDENTIFY must reject nil request_id"
    );

    let request_id = Uuid::new_v4();

    let mut contender = Client::connect(&dsn(), NoTls).expect("idempotency contender");
    db.batch_execute("BEGIN").expect("hold request namespace");
    db.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended('model-call-request:' || $1::uuid::text || ':' || $2::uuid::text,0))",
        &[&lane.tenant, &request_id],
    )
    .expect("first same-key lock");
    contender
        .batch_execute("SET statement_timeout='200ms'")
        .expect("bounded lock wait");
    assert!(contender.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended('model-call-request:' || $1::uuid::text || ':' || $2::uuid::text,0))",
        &[&lane.tenant, &request_id],
    ).is_err(), "same tenant/request key serializes before authority query");
    contender
        .batch_execute("SET statement_timeout=0")
        .expect("reset timeout");
    contender.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended('model-call-request:' || $1::uuid::text || ':' || $2::uuid::text,0))",
        &[&Uuid::new_v4(), &request_id],
    ).expect("same request UUID across tenants does not serialize");
    db.batch_execute("ROLLBACK")
        .expect("release request namespace");

    let model_call = reserve_reasoning_call(&mut db, &lane, request_id, "TYPED_ASSESSMENT", 5);
    let disclosure = reserve_reasoning_disclosure(&mut db, &lane, model_call, egress);
    let reserved = db
        .query_one(
            "SELECT status,input_tokens,billable_tokens,estimated_cost,actual_cost,egress_processor_id FROM ops.model_call_ledger WHERE model_call_id=$1",
            &[&model_call],
        )
        .expect("reserved immutable snapshot");
    assert_eq!(reserved.get::<_, String>(0), "RESERVED");
    assert!(reserved.get::<_, Option<i64>>(1).is_none());
    assert!(reserved.get::<_, Option<i64>>(2).is_none());
    assert!(reserved.get::<_, Option<f64>>(3).is_none());
    assert!(reserved.get::<_, Option<f64>>(4).is_none());
    assert_eq!(reserved.get::<_, Uuid>(5), egress);

    let original_admitted_at: std::time::SystemTime = db
        .query_one(
            "SELECT admitted_at FROM ops.model_call_ledger WHERE model_call_id=$1",
            &[&model_call],
        )
        .expect("original admission clock")
        .get(0);
    begin_case(&mut db, "existing_reserved_after_health_change");
    insert_provider(&mut db, &lane, "UNAVAILABLE", 0, 300);
    assert_eq!(
        resolve_count(&mut db, &lane),
        0,
        "current resolver denies a new attempt"
    );
    let same_intent = vec![5_u8; 32];
    let existing = db.query_one(
        "SELECT model_call_id,admitted_at FROM ops.model_call_ledger call WHERE tenant_id=$1 AND request_id=$2 AND status='RESERVED' AND call_kind='TYPED_ASSESSMENT' AND intent_sha256=$3 AND EXISTS(SELECT 1 FROM ops.data_disclosures disclosure WHERE disclosure.tenant_id=call.tenant_id AND disclosure.model_call_id=call.model_call_id AND disclosure.processor_id=call.egress_processor_id AND disclosure.finalized_at IS NULL)",
        &[&lane.tenant, &request_id, &same_intent],
    ).expect("same intent returns existing RESERVED before current resolver");
    assert_eq!(existing.get::<_, Uuid>(0), model_call);
    assert_eq!(
        existing.get::<_, std::time::SystemTime>(1),
        original_admitted_at
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM ops.model_call_ledger WHERE tenant_id=$1 AND request_id=$2 AND intent_sha256=$3",
            &[&lane.tenant, &request_id, &vec![6_u8; 32]],
        )
        .expect("different intent lookup")
        .get::<_, i64>(0),
        0,
        "same id with different intent is conflict, not an existing reservation"
    );
    end_case(&mut db, "existing_reserved_after_health_change");

    for (name, statement) in [
        (
            "ledger_recipient_mutation",
            format!(
                "UPDATE ops.model_call_ledger SET egress_processor_id='{}' WHERE model_call_id='{}'",
                Uuid::new_v4(),
                model_call
            ),
        ),
        (
            "ledger_estimated_platform_cost",
            format!(
                "UPDATE ops.model_call_ledger SET estimated_cost=1 WHERE model_call_id='{}'",
                model_call
            ),
        ),
        (
            "ledger_actual_platform_cost",
            format!(
                "UPDATE ops.model_call_ledger SET actual_cost=1 WHERE model_call_id='{}'",
                model_call
            ),
        ),
        (
            "ledger_call_kind_mutation",
            format!(
                "UPDATE ops.model_call_ledger SET call_kind='COVERAGE_PROBE' WHERE model_call_id='{}'",
                model_call
            ),
        ),
        (
            "ledger_intent_mutation",
            format!(
                "UPDATE ops.model_call_ledger SET intent_sha256=decode(repeat('ab',32),'hex') WHERE model_call_id='{}'",
                model_call
            ),
        ),
        (
            "disclosure_model_call_mutation",
            format!(
                "UPDATE ops.data_disclosures SET model_call_id='{}' WHERE disclosure_id='{}'",
                Uuid::new_v4(),
                disclosure
            ),
        ),
        (
            "disclosure_recipient_mutation",
            format!(
                "UPDATE ops.data_disclosures SET processor_id='{}' WHERE disclosure_id='{}'",
                Uuid::new_v4(),
                disclosure
            ),
        ),
    ] {
        begin_case(&mut db, name);
        assert!(
            db.batch_execute(&statement).is_err(),
            "{name} must fail closed"
        );
        end_case(&mut db, name);
    }

    let duplicate = db.execute(
        "INSERT INTO ops.model_call_ledger(request_id,tenant_id,provider,purpose) VALUES($1,$2,'different-snapshot','embedding')",
        &[&request_id, &lane.tenant],
    );
    assert!(
        duplicate.is_err(),
        "one tenant/request attempt key cannot fork"
    );

    let rollback_request = Uuid::new_v4();
    db.batch_execute("BEGIN")
        .expect("atomic reserve transaction");
    let rolled_back_call =
        reserve_reasoning_call(&mut db, &lane, rollback_request, "COVERAGE_PROBE", 8);
    let digest = vec![9_u8; 32];
    assert!(db.execute(
        "INSERT INTO ops.data_disclosures(grant_id,tenant_id,processor_id,region,data_class,purpose,payload_sha256,payload_bytes,model_call_id) VALUES($1,$2,$3,$4,'PRIVATE','USER_REASONING',$5,17,$6)",
        &[&Uuid::new_v4(), &lane.tenant, &Uuid::new_v4(), &lane.region, &digest, &rolled_back_call],
    ).is_err(), "recipient mismatch aborts the reserve transaction");
    db.batch_execute("ROLLBACK").expect("reserve rollback");
    let rolled_back: i64 = db
        .query_one(
            "SELECT count(*) FROM ops.model_call_ledger WHERE tenant_id=$1 AND request_id=$2",
            &[&lane.tenant, &rollback_request],
        )
        .expect("atomic rollback evidence")
        .get(0);
    assert_eq!(rolled_back, 0);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM ops.data_disclosures WHERE tenant_id=$1 AND model_call_id=$2",
            &[&lane.tenant, &rolled_back_call],
        )
        .expect("disclosure rollback evidence")
        .get::<_, i64>(0),
        0
    );

    db.execute(
        "UPDATE ops.model_call_ledger SET status='SUCCEEDED' WHERE model_call_id=$1",
        &[&model_call],
    )
    .expect("finalize call once without fabricated usage/cost");
    assert_eq!(
        db.query_one(
            "SELECT status FROM ops.model_call_ledger WHERE tenant_id=$1 AND request_id=$2",
            &[&lane.tenant, &request_id],
        )
        .expect("terminal retry conflict")
        .get::<_, String>(0),
        "SUCCEEDED"
    );
    db.execute(
        "UPDATE ops.data_disclosures SET finalized_at=clock_timestamp(),outcome='SUCCESS' WHERE disclosure_id=$1",
        &[&disclosure],
    )
    .expect("finalize disclosure once");

    let probe_call = reserve_reasoning_call(&mut db, &lane, Uuid::new_v4(), "COVERAGE_PROBE", 9);
    let probe_disclosure = reserve_reasoning_disclosure(&mut db, &lane, probe_call, egress);
    db.execute(
        "UPDATE ops.model_call_ledger SET status='SUCCEEDED' WHERE model_call_id=$1",
        &[&probe_call],
    )
    .expect("finalize coverage probe call");
    db.execute(
        "UPDATE ops.data_disclosures SET finalized_at=clock_timestamp(),outcome='SUCCESS' WHERE disclosure_id=$1",
        &[&probe_disclosure],
    )
    .expect("finalize coverage probe disclosure");

    let contribution_policy: Uuid = db
        .query_one(
            "INSERT INTO control.contribution_policies(tenant_id,allow_public_contribution) VALUES($1,true) RETURNING policy_id",
            &[&lane.tenant],
        )
        .expect("contribution policy")
        .get(0);

    begin_case(&mut db, "probe_is_not_assessment");
    assert!(
        insert_reasoning_candidate(&mut db, &lane, contribution_policy, probe_call, 1).is_err(),
        "COVERAGE_PROBE can never authorize a contribution candidate"
    );
    end_case(&mut db, "probe_is_not_assessment");

    db.batch_execute("BEGIN; SAVEPOINT historical_candidate")
        .expect("historical attempt case");
    insert_provider(&mut db, &lane, "UNAVAILABLE", 0, 300);
    db.execute(
        "UPDATE control.provider_endpoints SET enabled=false,updated_at=clock_timestamp() WHERE endpoint_id=$1",
        &[&lane.endpoint],
    )
    .expect("disable endpoint after completed attempt");
    assert_eq!(
        resolve_count(&mut db, &lane),
        0,
        "next attempt uses current denial"
    );
    assert_eq!(
        insert_reasoning_candidate(&mut db, &lane, contribution_policy, model_call, 1)
            .expect("historical successful attempt remains candidate authority"),
        1
    );
    assert!(
        insert_reasoning_candidate(&mut db, &lane, contribution_policy, model_call, 2).is_err(),
        "candidate profile mirror cannot diverge from the immutable call"
    );
    db.batch_execute("ROLLBACK TO SAVEPOINT historical_candidate; ROLLBACK")
        .expect("historical candidate rollback");

    let guard: String = db
        .query_one(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='ops.model_call_ledger'::regclass AND conname='model_call_ledger_reasoning_snapshot_shape'",
            &[],
        )
        .expect("reasoning cost shape")
        .get(0);
    assert!(guard.contains("estimated_cost IS NULL") && guard.contains("actual_cost IS NULL"));
}

#[test]
#[ignore = "requires isolated PostgreSQL 18 migrated through 0130"]
fn reasoning_route_health_statement_snapshot_advances_on_next_call() {
    let dsn = dsn();
    let mut first = Client::connect(&dsn, NoTls).expect("first connection");
    let lane = seed_lane(&mut first, "snapshot", false, true, true);
    set_tenant(&mut first, lane.tenant);
    insert_fresh_pair(&mut first, &lane);
    first
        .batch_execute("BEGIN ISOLATION LEVEL READ COMMITTED")
        .expect("read committed");
    assert_eq!(
        resolve_count(&mut first, &lane),
        1,
        "first statement sees fresh HEALTHY"
    );

    let mut second = Client::connect(&dsn, NoTls).expect("second connection");
    insert_provider(&mut second, &lane, "UNAVAILABLE", 0, 300);
    assert_eq!(
        resolve_count(&mut first, &lane),
        0,
        "next statement snapshot sees concurrently committed latest negative"
    );
    first.batch_execute("ROLLBACK").expect("snapshot rollback");
}
