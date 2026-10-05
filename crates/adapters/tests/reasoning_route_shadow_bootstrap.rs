//! `adapters::tests::reasoning_route_shadow_bootstrap` — Lane test for control.bootstrap_contribution_deidentify_shadow on a disposable database migrated through 0129.
//! Depends-on: crates=[postgres, uuid]; services=[PostgreSQL(owner)
//!   r=[control.bootstrap_contribution_deidentify_shadow, control.reasoning_profiles] w=[control.credentials,
//!   control.memberships, control.private_reasoning_domains, control.processor_models, control.provider_accounts,
//!   control.provider_billing_accounts, control.provider_billing_instruments, control.provider_endpoints,
//!   control.reasoning_credential_bindings, control.reasoning_route_bindings, control.reasoning_route_candidates,
//!   control.reasoning_route_domain_receipts, control.reasoning_route_policies,
//!   control.reasoning_route_profile_receipts, control.tenants, control.user_reasoning_profiles, control.users]
//!   x=[control.assert_reasoning_route_shadow_receipts, control.bootstrap_contribution_deidentify_shadow,
//!   control.reasoning_route_profile_receipt_fingerprint, control.resolve_contribution_deidentify_shadow]];
//!   env=[HUMAUX_TEST_PG_DSN]; modules=[]
//! Called-by: [cargo-test]
//! Invariants: [the contribution de-identify shadow bootstrap must be exact, replayable and fail closed; it walks
//!   every user reasoning domain, so it runs only as a lane(a:disposable) #[ignore] test]
//! Spec: none
//!
use postgres::{Client, GenericClient, NoTls};
use uuid::Uuid;

fn dsn() -> Option<String> {
    std::env::var("HUMAUX_TEST_PG_DSN").ok()
}

fn artifact_census(db: &mut impl GenericClient, tenant: &Uuid) -> (i64, i64, i64, i64, i64, i64) {
    let row = db
        .query_one(
            "SELECT \
               (SELECT count(*) FROM control.reasoning_profiles WHERE tenant_id=$1), \
               (SELECT count(*) FROM control.reasoning_route_policies WHERE tenant_id=$1), \
               (SELECT count(*) FROM control.reasoning_route_candidates WHERE tenant_id=$1), \
               (SELECT count(*) FROM control.reasoning_route_bindings WHERE tenant_id=$1), \
               (SELECT count(*) FROM control.reasoning_route_profile_receipts WHERE tenant_id=$1), \
               (SELECT count(*) FROM control.reasoning_route_domain_receipts WHERE tenant_id=$1)",
            &[tenant],
        )
        .expect("R2 artifact census");
    (
        row.get(0),
        row.get(1),
        row.get(2),
        row.get(3),
        row.get(4),
        row.get(5),
    )
}

fn assert_shadow_green(db: &mut impl GenericClient, domain: &Uuid) {
    db.query_one(
        "SELECT control.assert_reasoning_route_shadow_receipts()",
        &[],
    )
    .expect("R2 receipt assertion is green");
    db.query_one(
        "SELECT profile_receipts, domain_receipts FROM control.bootstrap_contribution_deidentify_shadow()",
        &[],
    )
    .expect("R2 bootstrap is green");
    let state: String = db
        .query_one(
            "SELECT compatibility_state FROM control.resolve_contribution_deidentify_shadow($1)",
            &[domain],
        )
        .expect("R2 resolver is green")
        .get(0);
    assert_eq!(state, "EQUIVALENT");
}

fn assert_shadow_red(db: &mut impl GenericClient, domain: &Uuid) {
    let state: String = db
        .query_one(
            "SELECT compatibility_state FROM control.resolve_contribution_deidentify_shadow($1)",
            &[domain],
        )
        .expect("R2 resolver is callable during corruption")
        .get(0);
    assert_ne!(state, "EQUIVALENT", "corrupt route must fail closed");
    assert!(
        db.query(
            "SELECT * FROM control.bootstrap_contribution_deidentify_shadow()",
            &[]
        )
        .is_err(),
        "bootstrap assertion must reject the corrupt route"
    );
}

/// The R2 test is intentionally SQL-level: the migration owner's explicit procedure is
/// the only bootstrap writer and the resolver must remain deterministic/no-egress.
#[test]
#[ignore = "lane(a:disposable) needs a per-run database migrated through 0129: control.bootstrap_contribution_deidentify_shadow walks EVERY control.user_reasoning_profiles row, so one legacy profile in a shared database is a 23514"]
#[allow(
    clippy::too_many_lines,
    reason = "single integration scenario covers the complete shadow bootstrap contract"
)]
fn contribution_deidentify_shadow_bootstrap_is_exact_replayable_and_fail_closed() {
    let Some(dsn) = dsn() else {
        eprintln!("not_applicable: HUMAUX_TEST_PG_DSN unset");
        return;
    };
    // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
    let mut db = Client::connect(&dsn, NoTls).expect("isolated PostgreSQL 18");
    let mut tx = db.transaction().expect("fixture transaction");
    let tenant = Uuid::new_v4();
    let user = Uuid::new_v4();
    let credential = Uuid::new_v4();
    let legacy_profile = Uuid::new_v4();
    let domain_a = Uuid::new_v4();
    let domain_b = Uuid::new_v4();
    let domain_null = Uuid::new_v4();
    let domain_null_two = Uuid::new_v4();
    let account = Uuid::new_v4();
    let endpoint = Uuid::new_v4();
    let model = Uuid::new_v4();
    let billing = Uuid::new_v4();
    let instrument = Uuid::new_v4();

    tx.execute(
        "INSERT INTO control.tenants(tenant_id, name) VALUES($1, 'r2')",
        &[&tenant],
    )
    .expect("tenant");
    tx.execute(
        "INSERT INTO control.users(user_id, state) VALUES($1, 'ACTIVE')",
        &[&user],
    )
    .expect("user");
    tx.execute(
        "INSERT INTO control.memberships(tenant_id, user_id, role, state) VALUES($1, $2, 'OWNER', 'ACTIVE')",
        &[&tenant, &user],
    )
    .expect("membership");
    tx.execute("INSERT INTO control.credentials(credential_id, tenant_id, purpose, openbao_ref) VALUES($1, $2, 'USER_REASONING', 'openbao://r2')", &[&credential, &tenant])
        .expect("credential");
    tx.execute("INSERT INTO control.user_reasoning_profiles(profile_id, tenant_id, user_id, provider_id, model_id, credential_ref, capabilities, processing_region) VALUES($1,$2,$3,'processor-r2','model-r2',$4,ARRAY['TEXT'],'region-r2')", &[&legacy_profile, &tenant, &user, &credential])
        .expect("legacy profile");
    for (domain, profile) in [
        (domain_a, Some(legacy_profile)),
        (domain_b, Some(legacy_profile)),
        (domain_null, None),
        (domain_null_two, None),
    ] {
        tx.execute("INSERT INTO control.private_reasoning_domains(reasoning_domain_id, tenant_id, name, owner_user_id, user_reasoning_profile_id, status) VALUES($1,$2,'r2',$3,$4,'ACTIVE')", &[&domain, &tenant, &user, &profile])
            .expect("domain");
    }
    tx.execute("INSERT INTO control.processor_models(processor_model_id, processor_id, provider_model_id, model_revision, capabilities, status, catalog_observed_at) VALUES($1,'processor-r2','model-r2',NULL,ARRAY['TEXT'],'ACTIVE',clock_timestamp())", &[&model])
        .expect("model");
    tx.execute("INSERT INTO control.provider_accounts(provider_account_id, tenant_id, owner_user_id, processor_id, external_account_ref_hash) VALUES($1,$2,$3,'processor-r2',decode(repeat('aa',32),'hex'))", &[&account, &tenant, &user])
        .expect("account");
    tx.execute("INSERT INTO control.provider_endpoints(endpoint_id, tenant_id, provider_account_id, region, service_tier, endpoint_ref) VALUES($1,$2,$3,'region-r2','tier-r2','endpoint-r2')", &[&endpoint, &tenant, &account])
        .expect("endpoint");
    tx.execute("INSERT INTO control.reasoning_credential_bindings(credential_ref, tenant_id, owner_user_id, provider_account_id, processor_id) VALUES($1,$2,$3,$4,'processor-r2')", &[&credential, &tenant, &user, &account])
        .expect("credential binding");
    tx.execute("INSERT INTO control.provider_billing_accounts(billing_account_id, tenant_id, owner_user_id, provider_account_id, account_ref) VALUES($1,$2,$3,$4,'billing-r2')", &[&billing, &tenant, &user, &account])
        .expect("billing account");
    tx.execute("INSERT INTO control.provider_billing_instruments(billing_instrument_id, tenant_id, billing_account_id, owner_user_id, payer_user_id, instrument_kind, invocation_eligibility, currency, coverage_processor_id, coverage_provider_model_id, coverage_model_revision, coverage_region, coverage_service_tier, valid_from, overage_policy) VALUES($1,$2,$3,$4,$4,'PAYG','API_CALLABLE','USD','processor-r2','model-r2',NULL,'region-r2','tier-r2',clock_timestamp()-interval '1 hour','PAYG')", &[&instrument, &tenant, &billing, &user])
        .expect("billing instrument");
    tx.commit().expect("commit fixtures");

    // Both error classes must roll back every R2 artifact; recovery must remain usable.
    let mut tx = db.transaction().expect("zero-match transaction");
    tx.batch_execute("SAVEPOINT zero_match").expect("savepoint");
    let missing_profile = Uuid::new_v4();
    let missing_domain = Uuid::new_v4();
    tx.execute("INSERT INTO control.user_reasoning_profiles(profile_id, tenant_id, user_id, provider_id, model_id, credential_ref, capabilities) VALUES($1,$2,$3,'missing-r2','model-r2',$4,ARRAY['TEXT'])", &[&missing_profile, &tenant, &user, &credential]).expect("zero-match legacy profile");
    tx.execute("INSERT INTO control.private_reasoning_domains(reasoning_domain_id, tenant_id, name, owner_user_id, user_reasoning_profile_id, status) VALUES($1,$2,'missing-r2',$3,$4,'ACTIVE')", &[&missing_domain, &tenant, &user, &missing_profile]).expect("zero-match domain");
    assert!(
        tx.query(
            "SELECT * FROM control.bootstrap_contribution_deidentify_shadow()",
            &[]
        )
        .is_err(),
        "zero typed identity must fail atomically"
    );
    tx.batch_execute("ROLLBACK TO SAVEPOINT zero_match")
        .expect("zero-match rollback");
    assert_eq!(
        artifact_census(&mut tx, &tenant),
        (0, 0, 0, 0, 0, 0),
        "zero-match failure leaves no new profile, policy, candidate, binding, or receipt"
    );
    assert_shadow_green(&mut tx, &domain_a);
    let baseline = artifact_census(&mut tx, &tenant);
    tx.batch_execute("SAVEPOINT multiple_match")
        .expect("savepoint");
    let ambiguous_profile = Uuid::new_v4();
    let ambiguous_domain = Uuid::new_v4();
    tx.execute("INSERT INTO control.provider_endpoints(tenant_id, provider_account_id, region, service_tier, endpoint_ref) VALUES($1,$2,'region-r2','tier-r2','endpoint-r2-duplicate')", &[&tenant, &account]).expect("second typed endpoint");
    tx.execute("INSERT INTO control.user_reasoning_profiles(profile_id, tenant_id, user_id, provider_id, model_id, credential_ref, capabilities, processing_region) VALUES($1,$2,$3,'processor-r2','model-r2',$4,ARRAY['TEXT'],'region-r2')", &[&ambiguous_profile, &tenant, &user, &credential]).expect("ambiguous legacy profile");
    tx.execute("INSERT INTO control.private_reasoning_domains(reasoning_domain_id, tenant_id, name, owner_user_id, user_reasoning_profile_id, status) VALUES($1,$2,'ambiguous-r2',$3,$4,'ACTIVE')", &[&ambiguous_domain, &tenant, &user, &ambiguous_profile]).expect("ambiguous domain");
    assert!(
        tx.query(
            "SELECT * FROM control.bootstrap_contribution_deidentify_shadow()",
            &[]
        )
        .is_err(),
        "multiple typed identities must fail atomically"
    );
    tx.batch_execute("ROLLBACK TO SAVEPOINT multiple_match")
        .expect("multiple-match rollback");
    assert_eq!(
        artifact_census(&mut tx, &tenant),
        baseline,
        "multiple-match failure leaves no new profile, policy, candidate, binding, or receipt"
    );
    assert_shadow_green(&mut tx, &domain_a);
    tx.commit().expect("commit rollback proofs");

    let replay = db.query_one("SELECT profile_receipts, domain_receipts FROM control.bootstrap_contribution_deidentify_shadow()", &[]).expect("replay");
    assert_eq!(
        (replay.get::<_, i64>(0), replay.get::<_, i64>(1)),
        (1, 4),
        "replay reports the same stable total after recovery bootstrap"
    );
    let equivalent: i64 = db.query_one("WITH eligible AS (SELECT reasoning_domain_id FROM control.private_reasoning_domains WHERE tenant_id=$1 AND status='ACTIVE'), resolved AS (SELECT e.reasoning_domain_id, r.compatibility_state FROM eligible e CROSS JOIN LATERAL control.resolve_contribution_deidentify_shadow(e.reasoning_domain_id) r) SELECT count(*) FROM ((SELECT reasoning_domain_id, 'EQUIVALENT'::text AS compatibility_state FROM eligible EXCEPT SELECT reasoning_domain_id, compatibility_state FROM resolved) UNION ALL (SELECT reasoning_domain_id, compatibility_state FROM resolved EXCEPT SELECT reasoning_domain_id, 'EQUIVALENT'::text FROM eligible)) mismatch", &[&tenant]).expect("bidirectional shadow comparison").get(0);
    assert_eq!(
        equivalent, 0,
        "shadow comparison includes canonical output fingerprint"
    );
    let null_class: String = db
        .query_one(
            "SELECT compatibility_state FROM control.resolve_contribution_deidentify_shadow($1)",
            &[&domain_null],
        )
        .expect("null receipt")
        .get(0);
    assert_eq!(null_class, "EQUIVALENT");
    let null_receipt: (String, i64) = {
        let row = db.query_one("SELECT disposition, count(binding.binding_id) FROM control.reasoning_route_domain_receipts receipt LEFT JOIN control.reasoning_route_bindings binding ON (binding.tenant_id, binding.binding_id, binding.binding_version)=(receipt.tenant_id, receipt.binding_id, receipt.binding_version) WHERE receipt.tenant_id=$1 AND receipt.reasoning_domain_id=$2 GROUP BY receipt.disposition", &[&tenant, &domain_null]).expect("null receipt shape");
        (row.get(0), row.get(1))
    };
    assert_eq!(null_receipt, ("NO_LEGACY_PROFILE".to_owned(), 0));
    let counts: (i64, i64, i64, i64) = {
        let row = db.query_one("SELECT (SELECT count(*) FROM control.reasoning_route_profile_receipts WHERE tenant_id=$1), (SELECT count(*) FROM control.reasoning_route_domain_receipts WHERE tenant_id=$1), (SELECT count(*) FROM (SELECT 1 FROM control.reasoning_route_candidates WHERE tenant_id=$1 GROUP BY tenant_id, route_policy_id, route_policy_version HAVING count(*) <> 1) mismatch), (SELECT count(*) FROM (SELECT 1 FROM control.reasoning_route_bindings WHERE tenant_id=$1 GROUP BY tenant_id, reasoning_domain_id, purpose HAVING count(*) FILTER (WHERE effective_to IS NULL) <> 1) mismatch)", &[&tenant]).expect("cardinality");
        (row.get(0), row.get(1), row.get(2), row.get(3))
    };
    assert_eq!(counts, (1, 4, 0, 0));

    for statement in [
        "UPDATE control.reasoning_route_profile_receipts SET execution_fingerprint='mutated' WHERE tenant_id=$1",
        "DELETE FROM control.reasoning_route_profile_receipts WHERE tenant_id=$1",
        "UPDATE control.reasoning_route_domain_receipts SET execution_fingerprint='mutated' WHERE tenant_id=$1",
        "DELETE FROM control.reasoning_route_domain_receipts WHERE tenant_id=$1",
    ] {
        assert!(
            db.execute(statement, &[&tenant]).is_err(),
            "both R2 receipt tables reject UPDATE and DELETE"
        );
    }

    let timezone_fingerprints: (String, String) = {
        let mut tx = db.transaction().expect("timezone fingerprint transaction");
        tx.batch_execute("SET LOCAL TIME ZONE 'UTC'")
            .expect("UTC timezone");
        let utc: String = tx
            .query_one(
                "SELECT control.reasoning_route_profile_receipt_fingerprint($1, profile_id, profile_version) FROM control.reasoning_route_profile_receipts WHERE tenant_id=$2",
                &[&legacy_profile, &tenant],
            )
            .expect("UTC fingerprint")
            .get(0);
        tx.batch_execute("SET LOCAL TIME ZONE 'Asia/Shanghai'")
            .expect("Shanghai timezone");
        let shanghai: String = tx
            .query_one(
                "SELECT control.reasoning_route_profile_receipt_fingerprint($1, profile_id, profile_version) FROM control.reasoning_route_profile_receipts WHERE tenant_id=$2",
                &[&legacy_profile, &tenant],
            )
            .expect("Shanghai fingerprint")
            .get(0);
        tx.commit().expect("timezone fingerprint transaction");
        (utc, shanghai)
    };
    assert_eq!(
        timezone_fingerprints.0, timezone_fingerprints.1,
        "canonical receipt fingerprint is session-TimeZone invariant"
    );

    let mut tx = db
        .transaction()
        .expect("fingerprint sensitivity transaction");
    for (savepoint, update) in [
        (
            "instrument_kind",
            "UPDATE control.provider_billing_instruments SET instrument_kind='TOKEN_PLAN' WHERE billing_instrument_id=$1",
        ),
        (
            "currency",
            "UPDATE control.provider_billing_instruments SET currency='EUR' WHERE billing_instrument_id=$1",
        ),
    ] {
        tx.batch_execute(&format!("SAVEPOINT {savepoint}"))
            .expect("fingerprint savepoint");
        let before: String = tx
            .query_one(
                "SELECT control.reasoning_route_profile_receipt_fingerprint($1, profile_id, profile_version) FROM control.reasoning_route_profile_receipts WHERE tenant_id=$2",
                &[&legacy_profile, &tenant],
            )
            .expect("fingerprint before mutation")
            .get(0);
        // replica-mode: throwaway database only (humaux_thread_disposable_<stamp>, migrated to head by `cargo xtask serial-lane` for lane a:disposable, dropped by its --drop-provisioned)
        tx.batch_execute("SET LOCAL session_replication_role = replica")
            .expect("disable immutable-parent trigger for fingerprint fault");
        tx.execute(update, &[&instrument])
            .expect("instrument fingerprint fault injection");
        tx.batch_execute("SET LOCAL session_replication_role = origin")
            .expect("restore immutable-parent trigger");
        let after: String = tx
            .query_one(
                "SELECT control.reasoning_route_profile_receipt_fingerprint($1, profile_id, profile_version) FROM control.reasoning_route_profile_receipts WHERE tenant_id=$2",
                &[&legacy_profile, &tenant],
            )
            .expect("fingerprint after mutation")
            .get(0);
        assert_ne!(
            before, after,
            "{savepoint} participates in the receipt fingerprint"
        );
        tx.batch_execute(&format!("ROLLBACK TO SAVEPOINT {savepoint}"))
            .expect("rollback fingerprint fault");
        assert_shadow_green(&mut tx, &domain_a);
    }
    tx.commit().expect("fingerprint sensitivity transaction");

    let mut tx = db.transaction().expect("corruption transaction");
    tx.batch_execute("SAVEPOINT closed_binding")
        .expect("savepoint");
    tx.execute(
        "UPDATE control.reasoning_route_bindings SET effective_to=clock_timestamp() WHERE tenant_id=$1 AND reasoning_domain_id=$2 AND purpose='CONTRIBUTION_DEIDENTIFY'",
        &[&tenant, &domain_a],
    )
    .expect("close exact binding");
    assert_shadow_red(&mut tx, &domain_a);
    tx.batch_execute("ROLLBACK TO SAVEPOINT closed_binding")
        .expect("rollback closed binding");
    assert_shadow_green(&mut tx, &domain_a);

    tx.batch_execute("SAVEPOINT missing_binding")
        .expect("savepoint");
    // replica-mode: throwaway database only (humaux_thread_disposable_<stamp>, migrated to head by `cargo xtask serial-lane` for lane a:disposable, dropped by its --drop-provisioned)
    tx.batch_execute(
        "SET LOCAL session_replication_role = replica; \
         DELETE FROM control.reasoning_route_bindings; \
         SET LOCAL session_replication_role = origin",
    )
    .expect("missing binding corruption fault injection");
    assert_shadow_red(&mut tx, &domain_a);
    tx.batch_execute("ROLLBACK TO SAVEPOINT missing_binding")
        .expect("rollback missing binding");
    assert_shadow_green(&mut tx, &domain_a);

    tx.batch_execute("SAVEPOINT missing_policy")
        .expect("savepoint");
    // replica-mode: throwaway database only (humaux_thread_disposable_<stamp>, migrated to head by `cargo xtask serial-lane` for lane a:disposable, dropped by its --drop-provisioned)
    tx.batch_execute(
        "SET LOCAL session_replication_role = replica; \
         DELETE FROM control.reasoning_route_policies; \
         SET LOCAL session_replication_role = origin",
    )
    .expect("missing policy corruption fault injection");
    assert_shadow_red(&mut tx, &domain_a);
    tx.batch_execute("ROLLBACK TO SAVEPOINT missing_policy")
        .expect("rollback missing policy");
    assert_shadow_green(&mut tx, &domain_a);

    tx.batch_execute("SAVEPOINT missing_candidate")
        .expect("savepoint");
    // replica-mode: throwaway database only (humaux_thread_disposable_<stamp>, migrated to head by `cargo xtask serial-lane` for lane a:disposable, dropped by its --drop-provisioned)
    tx.batch_execute(
        "SET LOCAL session_replication_role = replica; \
         DELETE FROM control.reasoning_route_candidates; \
         SET LOCAL session_replication_role = origin",
    )
    .expect("candidate corruption fault injection");
    assert_shadow_red(&mut tx, &domain_a);
    tx.batch_execute("ROLLBACK TO SAVEPOINT missing_candidate")
        .expect("rollback candidate corruption");
    assert_shadow_green(&mut tx, &domain_a);

    tx.batch_execute("SAVEPOINT profile_receipt_fingerprint_drift")
        .expect("savepoint");
    // replica-mode: throwaway database only (humaux_thread_disposable_<stamp>, migrated to head by `cargo xtask serial-lane` for lane a:disposable, dropped by its --drop-provisioned)
    tx.batch_execute("SET LOCAL session_replication_role = replica")
        .expect("disable receipt trigger for profile receipt corruption injection");
    tx.execute(
        "UPDATE control.reasoning_route_profile_receipts SET execution_fingerprint='profile-receipt-drift' WHERE tenant_id=$1 AND legacy_profile_id=$2",
        &[&tenant, &legacy_profile],
    )
    .expect("profile receipt fingerprint fault injection");
    tx.batch_execute("SET LOCAL session_replication_role = origin")
        .expect("restore receipt trigger");
    assert_shadow_red(&mut tx, &domain_a);
    tx.batch_execute("ROLLBACK TO SAVEPOINT profile_receipt_fingerprint_drift")
        .expect("rollback profile receipt fingerprint drift");
    assert_shadow_green(&mut tx, &domain_a);

    tx.batch_execute("SAVEPOINT fingerprint_drift")
        .expect("savepoint");
    // replica-mode: throwaway database only (humaux_thread_disposable_<stamp>, migrated to head by `cargo xtask serial-lane` for lane a:disposable, dropped by its --drop-provisioned)
    tx.batch_execute("SET LOCAL session_replication_role = replica")
        .expect("disable receipt trigger for corruption injection");
    tx.execute(
        "UPDATE control.reasoning_route_domain_receipts SET execution_fingerprint='receipt-drift' WHERE tenant_id=$1 AND reasoning_domain_id=$2",
        &[&tenant, &domain_a],
    )
    .expect("domain receipt fingerprint fault injection");
    tx.batch_execute("SET LOCAL session_replication_role = origin")
        .expect("restore receipt trigger");
    assert_shadow_red(&mut tx, &domain_a);
    tx.batch_execute("ROLLBACK TO SAVEPOINT fingerprint_drift")
        .expect("rollback receipt fingerprint drift");
    assert_shadow_green(&mut tx, &domain_a);

    tx.batch_execute("SAVEPOINT null_receipt_fingerprint_drift")
        .expect("savepoint");
    // replica-mode: throwaway database only (humaux_thread_disposable_<stamp>, migrated to head by `cargo xtask serial-lane` for lane a:disposable, dropped by its --drop-provisioned)
    tx.batch_execute("SET LOCAL session_replication_role = replica")
        .expect("disable receipt trigger for null receipt corruption injection");
    tx.execute(
        "UPDATE control.reasoning_route_domain_receipts SET execution_fingerprint='null-receipt-drift' WHERE tenant_id=$1 AND reasoning_domain_id=$2",
        &[&tenant, &domain_null],
    )
    .expect("null receipt fingerprint fault injection");
    tx.batch_execute("SET LOCAL session_replication_role = origin")
        .expect("restore receipt trigger");
    assert_shadow_red(&mut tx, &domain_null);
    tx.batch_execute("ROLLBACK TO SAVEPOINT null_receipt_fingerprint_drift")
        .expect("rollback null receipt fingerprint drift");
    assert_shadow_green(&mut tx, &domain_a);

    tx.batch_execute("SAVEPOINT cross_tenant_receipt")
        .expect("savepoint");
    let other_tenant = Uuid::new_v4();
    tx.execute(
        "INSERT INTO control.tenants(tenant_id, name) VALUES($1, 'r2-isolation')",
        &[&other_tenant],
    )
    .expect("cross-tenant fixture tenant");
    tx.execute(
        "INSERT INTO control.reasoning_route_domain_receipts(tenant_id, reasoning_domain_id, disposition, execution_fingerprint) VALUES($1,$2,'NO_LEGACY_PROFILE','cross-tenant')",
        &[&other_tenant, &domain_a],
    )
    .expect("cross-tenant receipt fixture");
    let state: String = tx
        .query_one(
            "SELECT compatibility_state FROM control.resolve_contribution_deidentify_shadow($1)",
            &[&domain_a],
        )
        .expect("tenant-isolated resolver")
        .get(0);
    assert_eq!(
        state, "EQUIVALENT",
        "foreign-tenant receipt cannot alter this domain"
    );
    assert!(
        tx.query(
            "SELECT * FROM control.bootstrap_contribution_deidentify_shadow()",
            &[]
        )
        .is_err(),
        "bootstrap assertion rejects a cross-tenant receipt"
    );
    tx.batch_execute("ROLLBACK TO SAVEPOINT cross_tenant_receipt")
        .expect("rollback cross-tenant receipt");
    assert_shadow_green(&mut tx, &domain_a);
    tx.rollback().expect("rollback fixture corruption");
}
