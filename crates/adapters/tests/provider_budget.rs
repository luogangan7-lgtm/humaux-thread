//! Real-role acceptance for migration 0117's persistent sliding provider budget.

use std::{str::FromStr, thread, time::Duration};

use humaux_adapters::{
    model_call_ledger::{self, FinalizeCall, ModelCallOutcome, ReserveCall},
    postgres::{MaintenanceDbPool, RetrievalWorkerDbPool},
    provider_budget::{
        ProviderBudgetRequest, ProviderBudgetReservationStatus,
        finalize_and_settle_provider_budget, mark_provider_budget_dispatched,
        reap_expired_provider_budgets, reserve_provider_budget, settle_provider_budget,
    },
};
use humaux_domain::{egress::PrivateDataPurpose, error::ErrorCode, ids::TenantId};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls, error::SqlState};
use sqlx::{postgres::PgConnectOptions, types::Uuid};

const FIXTURE_DB: &str = "humaux_thread_request_guard_20260828";

struct Handle {
    rt: tokio::runtime::Runtime,
    retrieval: RetrievalWorkerDbPool,
    maintenance: MaintenanceDbPool,
    admin: Client,
    tenant_id: Uuid,
    provider_id: String,
    region: String,
    retrieval_dsn: String,
    maintenance_dsn: String,
    gateway_dsn: String,
}

struct Fixture;

fn setup_error(stage: &str) -> DbFixtureSkipReason {
    DbFixtureSkipReason::IsolationSetupFailed(format!(
        "provider budget fixture setup failed at {stage}"
    ))
}

fn checked_dsn(name: &str, expected_role: Option<&str>) -> Result<String, DbFixtureSkipReason> {
    let dsn = std::env::var(name).map_err(|_| setup_error("required DSN"))?;
    let options = PgConnectOptions::from_str(&dsn).map_err(|_| setup_error("DSN parse"))?;
    if expected_role.is_some_and(|role| options.get_username() != role)
        || options.get_host() != "127.0.0.1"
        || options.get_port() != 61719
        || options.get_database() != Some(FIXTURE_DB)
        || dsn.contains(['?', '#'])
    {
        return Err(setup_error("DSN boundary validation"));
    }
    Ok(dsn)
}

fn role_login_ok(dsn: &str, role: &str) -> Result<(), DbFixtureSkipReason> {
    let mut client = Client::connect(dsn, NoTls).map_err(|_| setup_error("actual role login"))?;
    let ok: bool = client
        .query_one(
            "SELECT current_user=$1 AND session_user=$1 \
             AND NOT (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user)",
            &[&role],
        )
        .map_err(|_| setup_error("actual role identity probe"))?
        .get(0);
    ok.then_some(())
        .ok_or_else(|| setup_error("actual role identity mismatch"))
}

impl DbIntegrationFixture for Fixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let owner_dsn = checked_dsn("HUMAUX_TEST_PG_DSN", None)?;
        let retrieval_dsn = checked_dsn(
            "HUMAUX_RETRIEVAL_WORKER_PG_DSN",
            Some("role_retrieval_worker"),
        )?;
        let maintenance_dsn = checked_dsn("HUMAUX_MAINTENANCE_PG_DSN", Some("role_maintenance"))?;
        let gateway_dsn = checked_dsn("HUMAUX_GATEWAY_PG_DSN", Some("role_gateway"))?;
        role_login_ok(&retrieval_dsn, "role_retrieval_worker")?;
        role_login_ok(&maintenance_dsn, "role_maintenance")?;
        role_login_ok(&gateway_dsn, "role_gateway")?;
        let mut admin =
            Client::connect(&owner_dsn, NoTls).map_err(|_| setup_error("owner login"))?;
        let ready: bool = admin
            .query_one(
                "SELECT to_regprocedure('ops.reserve_retrieval_provider_budget(uuid,uuid,text,text,text,text,bigint,bigint)') IS NOT NULL",
                &[],
            )
            .map_err(|_| setup_error("migration readiness probe"))?
            .get(0);
        if !ready {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "migration 0117 is not applied".into(),
            ));
        }
        let tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants(name) VALUES('provider-budget fixture') RETURNING tenant_id",
                &[],
            )
            .map_err(|_| setup_error("seed tenant"))?
            .get(0);
        let provider_id = format!("provider-budget-{}", Uuid::now_v7());
        let region = "provider-budget-region".to_string();
        seed_four_limits(&mut admin, tenant_id, &provider_id, &region, 10, 10)?;
        let rt = tokio::runtime::Runtime::new().map_err(|_| setup_error("runtime"))?;
        let retrieval = rt
            .block_on(RetrievalWorkerDbPool::connect(&retrieval_dsn))
            .map_err(|_| setup_error("retrieval worker pool"))?;
        let maintenance = rt
            .block_on(MaintenanceDbPool::connect(&maintenance_dsn))
            .map_err(|_| setup_error("maintenance pool"))?;
        Ok(Handle {
            rt,
            retrieval,
            maintenance,
            admin,
            tenant_id,
            provider_id,
            region,
            retrieval_dsn,
            maintenance_dsn,
            gateway_dsn,
        })
    }
}

fn seed_four_limits(
    admin: &mut Client,
    tenant_id: Uuid,
    provider_id: &str,
    region: &str,
    tpm: i64,
    rpm: i64,
) -> Result<(), DbFixtureSkipReason> {
    for (row_tenant, row_region, purpose) in [
        (None, None, None),
        (None, Some(region), None),
        (Some(tenant_id), None, None),
        (Some(tenant_id), None, Some("RETRIEVAL_EMBEDDING")),
    ] {
        admin
            .execute(
                "INSERT INTO control.retrieval_provider_admission_limits \
                 (tenant_id,provider_id,region,purpose,tpm_limit,rpm_limit,effective_from) \
                 VALUES($1,$2,$3,$4,$5,$6,clock_timestamp()-interval '1 second')",
                &[&row_tenant, &provider_id, &row_region, &purpose, &tpm, &rpm],
            )
            .map_err(|_| setup_error("seed canonical limit"))?;
    }
    Ok(())
}

fn seed_tenant_limits(
    admin: &mut Client,
    tenant_id: Uuid,
    provider_id: &str,
) -> Result<(), DbFixtureSkipReason> {
    for purpose in [None, Some("RETRIEVAL_EMBEDDING")] {
        admin
            .execute(
                "INSERT INTO control.retrieval_provider_admission_limits \
                 (tenant_id,provider_id,purpose,tpm_limit,rpm_limit,effective_from) \
                 VALUES($1,$2,$3,1000,1000,clock_timestamp()-interval '1 second')",
                &[&tenant_id, &provider_id, &purpose],
            )
            .map_err(|_| setup_error("seed tenant canonical limit"))?;
    }
    Ok(())
}

fn ledger(handle: &Handle) -> model_call_ledger::ReserveCall {
    ledger_for(handle, handle.tenant_id)
}

fn ledger_for(handle: &Handle, tenant_id: Uuid) -> model_call_ledger::ReserveCall {
    ReserveCall {
        request_id: None,
        tenant_id,
        workspace_id: None,
        purpose: Some("embedding".to_string()),
        provider: handle.provider_id.clone(),
        model: Some("provider-budget-model".to_string()),
        model_revision: None,
        estimated_cost: None,
    }
}

fn request<'a>(
    handle: &'a Handle,
    model_call_id: Uuid,
    ttl: Duration,
) -> ProviderBudgetRequest<'a> {
    request_for(
        handle,
        handle.tenant_id,
        &handle.region,
        model_call_id,
        ttl,
        7,
    )
}

fn request_for<'a>(
    handle: &'a Handle,
    tenant_id: Uuid,
    region: &'a str,
    model_call_id: Uuid,
    ttl: Duration,
    estimated_tokens: u64,
) -> ProviderBudgetRequest<'a> {
    ProviderBudgetRequest {
        tenant_id: TenantId(tenant_id),
        model_call_id,
        provider_id: &handle.provider_id,
        model_id: "provider-budget-model",
        region,
        purpose: PrivateDataPurpose::RetrievalEmbedding,
        estimated_tokens,
        ttl,
    }
}

fn release_undispatched(
    tenant_id: Uuid,
    handle: &mut Handle,
    model_call_id: Uuid,
    reservation_id: Uuid,
) {
    handle
        .rt
        .block_on(model_call_ledger::finalize_call(
            &handle.retrieval,
            tenant_id,
            model_call_id,
            ModelCallOutcome::Failed,
            &FinalizeCall::default(),
        ))
        .expect("finalize undispatched fixture call");
    assert_eq!(
        handle.rt.block_on(settle_provider_budget(
            &handle.retrieval,
            TenantId(tenant_id),
            reservation_id,
        )),
        Ok(ProviderBudgetReservationStatus::Released),
        "only an undispatched failed call may release its budget"
    );
}

fn reserve_test_budget(
    handle: &Handle,
    tenant_id: Uuid,
    region: &str,
    estimated_tokens: u64,
) -> (
    Uuid,
    humaux_adapters::provider_budget::ProviderBudgetReservation,
) {
    let model_call_id = reserve_test_ledger(handle, tenant_id);
    let reservation = handle
        .rt
        .block_on(reserve_provider_budget(
            &handle.retrieval,
            &request_for(
                handle,
                tenant_id,
                region,
                model_call_id,
                Duration::from_secs(30),
                estimated_tokens,
            ),
        ))
        .expect("fixture budget reserve");
    (model_call_id, reservation)
}

fn reserve_test_ledger(handle: &Handle, tenant_id: Uuid) -> Uuid {
    let call = handle
        .rt
        .block_on(model_call_ledger::reserve_call(
            &handle.retrieval,
            &ledger_for(handle, tenant_id),
        ))
        .expect("fixture ledger");
    call.model_call_id
}

fn allocation_totals(handle: &mut Handle) -> (i64, i64) {
    handle
        .admin
        .query_one(
            "SELECT count(*), coalesce(sum(tokens),0)::bigint FROM ops.retrieval_provider_budget_allocations \
             WHERE tenant_id=$1",
            &[&handle.tenant_id],
        )
        .map(|row| (row.get(0), row.get(1)))
        .expect("allocation totals")
}

fn open_reservations(handle: &mut Handle) -> i64 {
    handle
        .admin
        .query_one(
            "SELECT count(*) FROM ops.retrieval_provider_budget_reservations \
             WHERE tenant_id=$1 AND status='RESERVED'",
            &[&handle.tenant_id],
        )
        .expect("open reservation count")
        .get(0)
}

fn reservation_status(handle: &mut Handle, reservation_id: Uuid) -> String {
    handle
        .admin
        .query_one(
            "SELECT status FROM ops.retrieval_provider_budget_reservations \
             WHERE tenant_id=$1 AND reservation_id=$2",
            &[&handle.tenant_id, &reservation_id],
        )
        .expect("provider budget reservation status")
        .get(0)
}

fn reservation_dispatched(handle: &mut Handle, reservation_id: Uuid) -> bool {
    handle
        .admin
        .query_one(
            "SELECT dispatched_at IS NOT NULL \
             FROM ops.retrieval_provider_budget_reservations \
             WHERE tenant_id=$1 AND reservation_id=$2",
            &[&handle.tenant_id, &reservation_id],
        )
        .expect("provider budget dispatch status")
        .get(0)
}

fn ledger_status(handle: &mut Handle, model_call_id: Uuid) -> String {
    handle
        .admin
        .query_one(
            "SELECT status FROM ops.model_call_ledger \
             WHERE tenant_id=$1 AND model_call_id=$2",
            &[&handle.tenant_id, &model_call_id],
        )
        .expect("model-call ledger status")
        .get(0)
}

fn stranded_reservation(
    handle: &mut Handle,
    dispatched: bool,
    ledger_outcome: Option<ModelCallOutcome>,
    ttl: Duration,
) -> Uuid {
    let model_call_id = reserve_test_ledger(handle, handle.tenant_id);
    let reservation = handle
        .rt
        .block_on(reserve_provider_budget(
            &handle.retrieval,
            &request(handle, model_call_id, ttl),
        ))
        .expect("recovery fixture budget reserve");
    if dispatched {
        handle
            .rt
            .block_on(mark_provider_budget_dispatched(
                &handle.retrieval,
                TenantId(handle.tenant_id),
                reservation.reservation_id,
            ))
            .expect("recovery fixture dispatch mark");
    }
    if let Some(outcome) = ledger_outcome {
        handle
            .rt
            .block_on(model_call_ledger::finalize_call(
                &handle.retrieval,
                handle.tenant_id,
                model_call_id,
                outcome,
                &FinalizeCall::default(),
            ))
            .expect("recovery fixture ledger terminal");
    }
    reservation.reservation_id
}

fn allocation_limit_id(
    tenant_id: Uuid,
    handle: &mut Handle,
    reservation_id: Uuid,
    tier: &str,
) -> Uuid {
    handle
        .admin
        .query_one(
            "SELECT limit_id FROM ops.retrieval_provider_budget_allocations \
             WHERE tenant_id=$1 AND reservation_id=$2 AND tier=$3",
            &[&tenant_id, &reservation_id, &tier],
        )
        .expect("canonical allocation limit")
        .get(0)
}

/// Owner-only fixture seeding for the strict sliding-window boundary.  Production rows remain
/// immutable; this creates an otherwise-valid historic reservation after reserving its ledger.
fn seed_historic_counted_reservation(handle: &mut Handle, age_seconds: i64) {
    let call = handle
        .rt
        .block_on(model_call_ledger::reserve_call(
            &handle.retrieval,
            &ledger(handle),
        ))
        .expect("historic ledger");
    let reservation_id = Uuid::now_v7();
    handle.admin.execute(
        "INSERT INTO ops.retrieval_provider_budget_reservations \
         (reservation_id,tenant_id,model_call_id,provider_id,model_id,region,purpose,requested_tokens,ttl_micros,reserved_at,expires_at,status) \
         VALUES($1,$2,$3,$4,'provider-budget-model',$5,'embedding',10,60000000,clock_timestamp()-($6::bigint * interval '1 second'),clock_timestamp()+interval '1 hour','RESERVED')",
        &[&reservation_id,&handle.tenant_id,&call.model_call_id,&handle.provider_id,&handle.region,&age_seconds],
    ).expect("historic reservation");
    handle.admin.execute(
        "INSERT INTO ops.retrieval_provider_budget_allocations \
         (tenant_id,reservation_id,limit_id,tier,limit_provider_id,limit_tenant_id,limit_region,limit_purpose,limit_tpm_limit,limit_rpm_limit,tokens,requests,reserved_at) \
         SELECT $1,$2,limit_id, CASE WHEN tenant_id IS NULL AND region IS NULL THEN 'GLOBAL' WHEN tenant_id IS NULL THEN 'REGION' WHEN purpose IS NULL THEN 'TENANT' ELSE 'PURPOSE' END, provider_id,tenant_id,region,purpose,tpm_limit,rpm_limit,10,1,(SELECT reserved_at FROM ops.retrieval_provider_budget_reservations WHERE tenant_id=$1 AND reservation_id=$2) \
         FROM control.retrieval_provider_admission_limits WHERE provider_id=$3 AND (tenant_id IS NULL OR tenant_id=$1)",
        &[&handle.tenant_id,&reservation_id,&handle.provider_id],
    ).expect("historic four allocations");
}

#[test]
fn concurrent_reserve_cannot_exceed_any_shared_tier() {
    run_db_fixture::<Fixture, _>("provider_budget_concurrent", |mut handle| {
        let first = handle
            .rt
            .block_on(model_call_ledger::reserve_call(
                &handle.retrieval,
                &ledger(&handle),
            ))
            .expect("first ledger");
        let second = handle
            .rt
            .block_on(model_call_ledger::reserve_call(
                &handle.retrieval,
                &ledger(&handle),
            ))
            .expect("second ledger");
        let first_request = request(&handle, first.model_call_id, Duration::from_secs(30));
        let second_request = request(&handle, second.model_call_id, Duration::from_secs(30));
        let (a, b) = handle.rt.block_on(async {
            tokio::join!(
                reserve_provider_budget(&handle.retrieval, &first_request),
                reserve_provider_budget(&handle.retrieval, &second_request),
            )
        });
        assert_eq!(
            usize::from(a.is_ok()) + usize::from(b.is_ok()),
            1,
            "only one 7-token reservation fits hard limit 10"
        );
        assert!(matches!(
            a.err().or_else(|| b.err()),
            Some(ErrorCode::CostBudgetExceeded)
        ));
        assert_eq!(
            allocation_totals(&mut handle),
            (4, 28),
            "one call allocates once to each canonical tier"
        );
    });
}

#[test]
fn each_canonical_tier_independently_denies_and_a_missing_tier_fails_closed() {
    for (name, predicate) in [
        (
            "global",
            "tenant_id IS NULL AND region IS NULL AND purpose IS NULL",
        ),
        (
            "region",
            "tenant_id IS NULL AND region IS NOT NULL AND purpose IS NULL",
        ),
        (
            "tenant",
            "tenant_id IS NOT NULL AND region IS NULL AND purpose IS NULL",
        ),
        (
            "purpose",
            "tenant_id IS NOT NULL AND region IS NULL AND purpose IS NOT NULL",
        ),
    ] {
        run_tier_case(name, predicate);
    }
}

fn run_tier_case(name: &str, predicate: &str) {
    run_db_fixture::<Fixture, _>(&format!("provider_budget_tier_{name}"), |mut handle| {
        handle
            .admin
            .execute(
                "UPDATE control.retrieval_provider_admission_limits \
                 SET tpm_limit=1000,rpm_limit=1000 WHERE provider_id=$1",
                &[&handle.provider_id],
            )
            .expect("make non-target tiers non-binding");
        handle
            .admin
            .execute(
                &format!(
                    "UPDATE control.retrieval_provider_admission_limits \
                     SET tpm_limit=7 WHERE provider_id=$1 AND {predicate}"
                ),
                &[&handle.provider_id],
            )
            .expect("narrow exactly one canonical tier");
        let tenant_id = handle.tenant_id;
        let region = handle.region.clone();
        let (first_call_id, first) = reserve_test_budget(&handle, tenant_id, &region, 7);
        let denied_call = handle
            .rt
            .block_on(model_call_ledger::reserve_call(
                &handle.retrieval,
                &ledger(&handle),
            ))
            .expect("denied tier ledger");
        assert_eq!(
            handle.rt.block_on(reserve_provider_budget(
                &handle.retrieval,
                &request_for(
                    &handle,
                    handle.tenant_id,
                    &handle.region,
                    denied_call.model_call_id,
                    Duration::from_secs(30),
                    1,
                ),
            )),
            Err(ErrorCode::CostBudgetExceeded),
            "{name} is independently enforced while every other tier has ample capacity"
        );
        release_undispatched(tenant_id, &mut handle, first_call_id, first.reservation_id);
        handle
            .admin
            .execute(
                &format!(
                    "UPDATE control.retrieval_provider_admission_limits \
                         SET effective_to=clock_timestamp() WHERE provider_id=$1 AND {predicate}"
                ),
                &[&handle.provider_id],
            )
            .expect("remove target canonical tier");
        let missing_call = handle
            .rt
            .block_on(model_call_ledger::reserve_call(
                &handle.retrieval,
                &ledger(&handle),
            ))
            .expect("missing-tier ledger");
        assert_eq!(
            handle.rt.block_on(reserve_provider_budget(
                &handle.retrieval,
                &request(&handle, missing_call.model_call_id, Duration::from_secs(30)),
            )),
            Err(ErrorCode::Conflict),
            "removing {name} must reject admission instead of silently skipping its gate"
        );
    });
}

#[test]
fn cross_tenant_and_region_scope_matches_the_canonical_limit_keys() {
    run_db_fixture::<Fixture, _>("provider_budget_cross_tenant_scope", |mut handle| {
        let tenant_b: Uuid = handle
            .admin
            .query_one(
                "INSERT INTO control.tenants(name) VALUES('provider-budget foreign tenant') RETURNING tenant_id",
                &[],
        )
        .expect("seed foreign tenant")
        .get(0);
        seed_tenant_limits(&mut handle.admin, tenant_b, &handle.provider_id)
            .expect("seed foreign tenant's own tiers");
        handle
            .admin
            .execute(
                "UPDATE control.retrieval_provider_admission_limits \
                 SET tpm_limit=1000,rpm_limit=1000 WHERE provider_id=$1",
                &[&handle.provider_id],
            )
            .expect("start with non-binding limits");
        same_region_scope(&mut handle, tenant_b);
        global_scope(&mut handle, tenant_b);
        local_identity_scope(&mut handle, tenant_b);
        cross_region_scope(&mut handle, tenant_b);
    });
}

fn same_region_scope(handle: &mut Handle, tenant_b: Uuid) {
    handle.admin.execute("UPDATE control.retrieval_provider_admission_limits SET tpm_limit=7 WHERE provider_id=$1 AND tenant_id IS NULL AND region=$2 AND purpose IS NULL", &[&handle.provider_id, &handle.region]).expect("narrow same-region limit");
    let tenant_a = handle.tenant_id;
    let region = handle.region.clone();
    let (call_a, reservation_a) = reserve_test_budget(handle, tenant_a, &region, 7);
    let call_b = reserve_test_ledger(handle, tenant_b);
    assert_eq!(
        handle.rt.block_on(reserve_provider_budget(
            &handle.retrieval,
            &request_for(
                handle,
                tenant_b,
                &region,
                call_b,
                Duration::from_secs(30),
                1
            )
        )),
        Err(ErrorCode::CostBudgetExceeded),
        "same-region tenant B is counted by tenant A's Region allocation"
    );
    release_undispatched(tenant_a, handle, call_a, reservation_a.reservation_id);
}

fn global_scope(handle: &mut Handle, tenant_b: Uuid) {
    handle.admin.execute("UPDATE control.retrieval_provider_admission_limits SET tpm_limit=1000 WHERE provider_id=$1 AND tenant_id IS NULL AND region IS NOT NULL", &[&handle.provider_id]).expect("widen regions");
    handle.admin.execute("UPDATE control.retrieval_provider_admission_limits SET tpm_limit=7 WHERE provider_id=$1 AND tenant_id IS NULL AND region IS NULL AND purpose IS NULL", &[&handle.provider_id]).expect("narrow global limit");
    let tenant_a = handle.tenant_id;
    let region = handle.region.clone();
    let (call_a, reservation_a) = reserve_test_budget(handle, tenant_a, &region, 7);
    let call_b = reserve_test_ledger(handle, tenant_b);
    assert_eq!(
        handle.rt.block_on(reserve_provider_budget(
            &handle.retrieval,
            &request_for(
                handle,
                tenant_b,
                &region,
                call_b,
                Duration::from_secs(30),
                1
            )
        )),
        Err(ErrorCode::CostBudgetExceeded),
        "same-provider Global allocation spans tenant identities"
    );
    release_undispatched(tenant_a, handle, call_a, reservation_a.reservation_id);
}

fn local_identity_scope(handle: &mut Handle, tenant_b: Uuid) {
    handle.admin.execute("UPDATE control.retrieval_provider_admission_limits SET tpm_limit=1000 WHERE provider_id=$1 AND tenant_id IS NULL", &[&handle.provider_id]).expect("widen shared rows");
    let tenant_a = handle.tenant_id;
    let region = handle.region.clone();
    let (call_a, reservation_a) = reserve_test_budget(handle, tenant_a, &region, 7);
    let (call_b, reservation_b) = reserve_test_budget(handle, tenant_b, &region, 7);
    for tier in ["GLOBAL", "REGION"] {
        assert_eq!(
            allocation_limit_id(tenant_a, handle, reservation_a.reservation_id, tier),
            allocation_limit_id(tenant_b, handle, reservation_b.reservation_id, tier),
            "{tier} is shared"
        );
    }
    for tier in ["TENANT", "PURPOSE"] {
        assert_ne!(
            allocation_limit_id(tenant_a, handle, reservation_a.reservation_id, tier),
            allocation_limit_id(tenant_b, handle, reservation_b.reservation_id, tier),
            "{tier} is tenant-scoped"
        );
    }
    release_undispatched(tenant_a, handle, call_a, reservation_a.reservation_id);
    release_undispatched(tenant_b, handle, call_b, reservation_b.reservation_id);
}

fn cross_region_scope(handle: &mut Handle, tenant_b: Uuid) {
    let other_region = "provider-budget-other-region";
    handle.admin.execute("INSERT INTO control.retrieval_provider_admission_limits (provider_id,region,tpm_limit,rpm_limit,effective_from) VALUES($1,$2,7,1000,clock_timestamp()-interval '1 second')", &[&handle.provider_id, &other_region]).expect("seed independent other-region row");
    different_regions_do_not_share_region(handle, tenant_b, other_region);
    different_regions_share_global(handle, tenant_b, other_region);
}

fn different_regions_do_not_share_region(handle: &mut Handle, tenant_b: Uuid, other_region: &str) {
    handle.admin.execute("UPDATE control.retrieval_provider_admission_limits SET tpm_limit=7 WHERE provider_id=$1 AND tenant_id IS NULL AND region=$2", &[&handle.provider_id, &handle.region]).expect("narrow first region");
    let tenant_a = handle.tenant_id;
    let region = handle.region.clone();
    let (call_a, reservation_a) = reserve_test_budget(handle, tenant_a, &region, 7);
    let (call_b, reservation_b) = reserve_test_budget(handle, tenant_b, other_region, 7);
    release_undispatched(tenant_a, handle, call_a, reservation_a.reservation_id);
    release_undispatched(tenant_b, handle, call_b, reservation_b.reservation_id);
}

fn different_regions_share_global(handle: &mut Handle, tenant_b: Uuid, other_region: &str) {
    handle.admin.execute("UPDATE control.retrieval_provider_admission_limits SET tpm_limit=1000 WHERE provider_id=$1 AND tenant_id IS NULL AND region IS NOT NULL", &[&handle.provider_id]).expect("widen region rows for global-only check");
    handle.admin.execute("UPDATE control.retrieval_provider_admission_limits SET tpm_limit=7 WHERE provider_id=$1 AND tenant_id IS NULL AND region IS NULL AND purpose IS NULL", &[&handle.provider_id]).expect("narrow Global row for cross-region check");
    let tenant_a = handle.tenant_id;
    let region = handle.region.clone();
    let (call_a, reservation_a) = reserve_test_budget(handle, tenant_a, &region, 7);
    let call_b = reserve_test_ledger(handle, tenant_b);
    assert_eq!(
        handle.rt.block_on(reserve_provider_budget(
            &handle.retrieval,
            &request_for(
                handle,
                tenant_b,
                other_region,
                call_b,
                Duration::from_secs(30),
                1
            )
        )),
        Err(ErrorCode::CostBudgetExceeded),
        "different regions share Global but no Region ceiling"
    );
    release_undispatched(tenant_a, handle, call_a, reservation_a.reservation_id);
}

#[test]
fn strict_sixty_second_sliding_boundary_counts_59_and_excludes_61() {
    run_db_fixture::<Fixture, _>("provider_budget_sliding_window", |mut handle| {
        seed_historic_counted_reservation(&mut handle, 59);
        let blocked = handle
            .rt
            .block_on(model_call_ledger::reserve_call(
                &handle.retrieval,
                &ledger(&handle),
            ))
            .expect("blocked ledger");
        assert_eq!(
            handle.rt.block_on(reserve_provider_budget(
                &handle.retrieval,
                &ProviderBudgetRequest {
                    estimated_tokens: 1,
                    ..request(&handle, blocked.model_call_id, Duration::from_secs(30))
                }
            )),
            Err(ErrorCode::CostBudgetExceeded)
        );
        handle.admin.execute("UPDATE ops.retrieval_provider_budget_reservations SET status='EXPIRED', settled_at=clock_timestamp() WHERE tenant_id=$1 AND status='RESERVED'", &[&handle.tenant_id]).expect("retire 59-second fixture row");
        seed_historic_counted_reservation(&mut handle, 61);
        let allowed = handle
            .rt
            .block_on(model_call_ledger::reserve_call(
                &handle.retrieval,
                &ledger(&handle),
            ))
            .expect("allowed ledger");
        assert!(
            handle
                .rt
                .block_on(reserve_provider_budget(
                    &handle.retrieval,
                    &ProviderBudgetRequest {
                        estimated_tokens: 1,
                        ..request(&handle, allowed.model_call_id, Duration::from_secs(30))
                    }
                ))
                .is_ok()
        );
    });
}

#[test]
fn concurrent_same_model_call_replays_one_reservation() {
    run_db_fixture::<Fixture, _>("provider_budget_idempotent_concurrent", |mut handle| {
        let call = handle
            .rt
            .block_on(model_call_ledger::reserve_call(
                &handle.retrieval,
                &ledger(&handle),
            ))
            .expect("ledger");
        let first_request = request(&handle, call.model_call_id, Duration::from_secs(30));
        let second_request = request(&handle, call.model_call_id, Duration::from_secs(30));
        let (first, second) = handle.rt.block_on(async {
            tokio::join!(
                reserve_provider_budget(&handle.retrieval, &first_request),
                reserve_provider_budget(&handle.retrieval, &second_request),
            )
        });
        let first = first.expect("first identical reserve must succeed");
        let second = second.expect("second identical reserve must replay");
        assert_eq!(first.reservation_id, second.reservation_id);
        assert_eq!(allocation_totals(&mut handle), (4, 28));
    });
}

#[test]
fn failed_attempt_consumes_the_independent_rpm_budget() {
    run_db_fixture::<Fixture, _>("provider_budget_rpm", |mut handle| {
        handle
            .admin
            .execute(
                "UPDATE control.retrieval_provider_admission_limits SET rpm_limit=1 \
                 WHERE provider_id=$1",
                &[&handle.provider_id],
            )
            .expect("narrow RPM only");
        let attempted_call = handle
            .rt
            .block_on(model_call_ledger::reserve_call(
                &handle.retrieval,
                &ledger(&handle),
            ))
            .expect("attempted ledger");
        let attempted_request = ProviderBudgetRequest {
            estimated_tokens: 1,
            ..request(
                &handle,
                attempted_call.model_call_id,
                Duration::from_secs(30),
            )
        };
        let attempted = handle
            .rt
            .block_on(reserve_provider_budget(
                &handle.retrieval,
                &attempted_request,
            ))
            .expect("first RPM admission");
        handle
            .rt
            .block_on(mark_provider_budget_dispatched(
                &handle.retrieval,
                TenantId(handle.tenant_id),
                attempted.reservation_id,
            ))
            .expect("persist dispatch before attempted call");
        handle
            .rt
            .block_on(model_call_ledger::finalize_call(
                &handle.retrieval,
                handle.tenant_id,
                attempted_call.model_call_id,
                ModelCallOutcome::Failed,
                &FinalizeCall::default(),
            ))
            .expect("failed provider ledger");
        assert_eq!(
            handle.rt.block_on(settle_provider_budget(
                &handle.retrieval,
                TenantId(handle.tenant_id),
                attempted.reservation_id,
            )),
            Ok(ProviderBudgetReservationStatus::Consumed),
            "an attempted failed provider call remains in the RPM rolling aggregate"
        );
        let second_call = handle
            .rt
            .block_on(model_call_ledger::reserve_call(
                &handle.retrieval,
                &ledger(&handle),
            ))
            .expect("second ledger");
        let second_request = ProviderBudgetRequest {
            estimated_tokens: 1,
            ..request(&handle, second_call.model_call_id, Duration::from_secs(30))
        };
        assert_eq!(
            handle
                .rt
                .block_on(reserve_provider_budget(&handle.retrieval, &second_request)),
            Err(ErrorCode::CostBudgetExceeded),
            "TPM has room, so this denial is the independent RPM ceiling"
        );
    });
}

#[test]
fn finalized_pre_send_failure_cannot_be_marked_dispatched_and_releases() {
    run_db_fixture::<Fixture, _>("provider_budget_pre_send_failure", |mut handle| {
        let call = handle
            .rt
            .block_on(model_call_ledger::reserve_call(
                &handle.retrieval,
                &ledger(&handle),
            ))
            .expect("pre-send failure ledger");
        let reservation = handle
            .rt
            .block_on(reserve_provider_budget(
                &handle.retrieval,
                &request(&handle, call.model_call_id, Duration::from_secs(30)),
            ))
            .expect("pre-send failure budget reserve");
        handle
            .rt
            .block_on(model_call_ledger::finalize_call(
                &handle.retrieval,
                handle.tenant_id,
                call.model_call_id,
                ModelCallOutcome::Failed,
                &FinalizeCall::default(),
            ))
            .expect("finalize proved pre-send failure");
        assert_eq!(
            handle.rt.block_on(mark_provider_budget_dispatched(
                &handle.retrieval,
                TenantId(handle.tenant_id),
                reservation.reservation_id,
            )),
            Err(ErrorCode::Conflict),
            "a terminal FAILED ledger cannot acquire a durable dispatch fact"
        );
        let dispatched_at_is_null: bool = handle
            .admin
            .query_one(
                "SELECT dispatched_at IS NULL FROM ops.retrieval_provider_budget_reservations \
                 WHERE tenant_id=$1 AND reservation_id=$2",
                &[&handle.tenant_id, &reservation.reservation_id],
            )
            .expect("read dispatch fact after rejected mark")
            .get(0);
        assert!(
            dispatched_at_is_null,
            "a rejected post-failure dispatch mark must leave the durable fact unset"
        );
        assert_eq!(
            handle.rt.block_on(settle_provider_budget(
                &handle.retrieval,
                TenantId(handle.tenant_id),
                reservation.reservation_id,
            )),
            Ok(ProviderBudgetReservationStatus::Released),
            "a proven pre-send failure derives RELEASED from the absent dispatch fact"
        );
    });
}

#[test]
fn success_consumes_failure_releases_and_expiry_reclaims_idempotently() {
    run_db_fixture::<Fixture, _>("provider_budget_lifecycle", |mut handle| {
        let failure_call = handle
            .rt
            .block_on(model_call_ledger::reserve_call(
                &handle.retrieval,
                &ledger(&handle),
            ))
            .expect("failure ledger");
        let failure = handle
            .rt
            .block_on(reserve_provider_budget(
                &handle.retrieval,
                &request(&handle, failure_call.model_call_id, Duration::from_secs(30)),
            ))
            .expect("budget reserve");
        let snapshot_before =
            assert_allocation_snapshot_is_immutable(&mut handle, failure.reservation_id);
        handle
            .rt
            .block_on(model_call_ledger::finalize_call(
                &handle.retrieval,
                handle.tenant_id,
                failure_call.model_call_id,
                ModelCallOutcome::Failed,
                &FinalizeCall::default(),
            ))
            .expect("ledger failure after control mutation");
        assert_eq!(
            handle.rt.block_on(settle_provider_budget(
                &handle.retrieval,
                TenantId(handle.tenant_id),
                failure.reservation_id
            )),
            Ok(ProviderBudgetReservationStatus::Released),
            "terminal guard uses the immutable allocation tier snapshot"
        );
        assert_four_tiers_and_restore_snapshot(
            &mut handle,
            failure.reservation_id,
            &snapshot_before,
        );

        assert_expiry_reclaims_idempotently(&mut handle);
        assert_dispatched_success_consumes(&mut handle);
        assert_eq!(
            open_reservations(&mut handle),
            0,
            "terminal/reaped calls leave no open budget reservation"
        );
    });
}

fn assert_atomic_finalize_and_settle(handle: &mut Handle) {
    for (dispatched, outcome, expected) in [
        (
            false,
            ModelCallOutcome::Failed,
            ProviderBudgetReservationStatus::Released,
        ),
        (
            true,
            ModelCallOutcome::Succeeded,
            ProviderBudgetReservationStatus::Consumed,
        ),
        (
            true,
            ModelCallOutcome::Failed,
            ProviderBudgetReservationStatus::Consumed,
        ),
    ] {
        let model_call_id = reserve_test_ledger(handle, handle.tenant_id);
        let reservation = handle
            .rt
            .block_on(reserve_provider_budget(
                &handle.retrieval,
                &request(handle, model_call_id, Duration::from_secs(30)),
            ))
            .expect("atomic completion budget reserve");
        if dispatched {
            handle
                .rt
                .block_on(mark_provider_budget_dispatched(
                    &handle.retrieval,
                    TenantId(handle.tenant_id),
                    reservation.reservation_id,
                ))
                .expect("atomic completion dispatch mark");
        }
        assert_eq!(
            handle.rt.block_on(finalize_and_settle_provider_budget(
                &handle.retrieval,
                TenantId(handle.tenant_id),
                reservation.reservation_id,
                model_call_id,
                outcome,
                &FinalizeCall::default(),
            )),
            Ok(expected)
        );
    }
}

#[test]
fn atomic_completion_and_maintenance_recovery_close_every_crash_residue() {
    run_db_fixture::<Fixture, _>("provider_budget_recovery", |mut handle| {
        handle
            .admin
            .execute(
                "UPDATE control.retrieval_provider_admission_limits \
                 SET tpm_limit=1000,rpm_limit=1000 WHERE provider_id=$1",
                &[&handle.provider_id],
            )
            .expect("widen recovery fixture limits");
        assert_atomic_finalize_and_settle(&mut handle);

        let pre_send_terminal = stranded_reservation(
            &mut handle,
            false,
            Some(ModelCallOutcome::Failed),
            Duration::from_secs(30),
        );
        let dispatched_terminal = stranded_reservation(
            &mut handle,
            true,
            Some(ModelCallOutcome::Failed),
            Duration::from_secs(30),
        );
        let dispatched_unknown =
            stranded_reservation(&mut handle, true, None, Duration::from_secs(1));
        thread::sleep(Duration::from_millis(1_100));

        assert_eq!(
            handle.rt.block_on(reap_expired_provider_budgets(
                &handle.maintenance,
                TenantId(handle.tenant_id),
                8,
            )),
            Ok(3)
        );
        assert_eq!(
            reservation_status(&mut handle, pre_send_terminal),
            "RELEASED"
        );
        assert_eq!(
            reservation_status(&mut handle, dispatched_terminal),
            "CONSUMED"
        );
        assert_eq!(
            reservation_status(&mut handle, dispatched_unknown),
            "CONSUMED",
            "an expired MAY_HAVE_REACHED call is never released"
        );
        assert_eq!(open_reservations(&mut handle), 0);
        assert_eq!(
            handle.rt.block_on(reap_expired_provider_budgets(
                &handle.maintenance,
                TenantId(handle.tenant_id),
                8,
            )),
            Ok(0),
            "maintenance recovery is idempotent"
        );
    });
}

fn assert_cross_pair_rejected_and_matching_pair_commits(handle: &mut Handle) {
    let model_call_a = reserve_test_ledger(handle, handle.tenant_id);
    let reservation_a = handle
        .rt
        .block_on(reserve_provider_budget(
            &handle.retrieval,
            &request(handle, model_call_a, Duration::from_secs(30)),
        ))
        .expect("reserve identity pair A");
    let model_call_b = reserve_test_ledger(handle, handle.tenant_id);
    let reservation_b = handle
        .rt
        .block_on(reserve_provider_budget(
            &handle.retrieval,
            &request(handle, model_call_b, Duration::from_secs(30)),
        ))
        .expect("reserve identity pair B");
    handle
        .rt
        .block_on(model_call_ledger::finalize_call(
            &handle.retrieval,
            handle.tenant_id,
            model_call_b,
            ModelCallOutcome::Failed,
            &FinalizeCall::default(),
        ))
        .expect("create terminal-without-settle pair B");

    assert_eq!(
        handle.rt.block_on(finalize_and_settle_provider_budget(
            &handle.retrieval,
            TenantId(handle.tenant_id),
            reservation_b.reservation_id,
            model_call_a,
            ModelCallOutcome::Failed,
            &FinalizeCall::default(),
        )),
        Err(ErrorCode::Conflict)
    );
    assert_eq!(ledger_status(handle, model_call_a), "RESERVED");
    assert_eq!(
        reservation_status(handle, reservation_a.reservation_id),
        "RESERVED"
    );
    assert_eq!(ledger_status(handle, model_call_b), "FAILED");
    assert_eq!(
        reservation_status(handle, reservation_b.reservation_id),
        "RESERVED"
    );

    for (reservation_id, model_call_id) in [
        (reservation_a.reservation_id, model_call_a),
        (reservation_b.reservation_id, model_call_b),
    ] {
        assert_eq!(
            handle.rt.block_on(finalize_and_settle_provider_budget(
                &handle.retrieval,
                TenantId(handle.tenant_id),
                reservation_id,
                model_call_id,
                ModelCallOutcome::Failed,
                &FinalizeCall::default(),
            )),
            Ok(ProviderBudgetReservationStatus::Released)
        );
    }
}

fn assert_post_identity_constraint_failure_rolls_back(handle: &mut Handle) {
    let model_call_id = reserve_test_ledger(handle, handle.tenant_id);
    let reservation = handle
        .rt
        .block_on(reserve_provider_budget(
            &handle.retrieval,
            &request(handle, model_call_id, Duration::from_secs(30)),
        ))
        .expect("reserve rollback pair");
    assert_eq!(
        handle.rt.block_on(finalize_and_settle_provider_budget(
            &handle.retrieval,
            TenantId(handle.tenant_id),
            reservation.reservation_id,
            model_call_id,
            ModelCallOutcome::Failed,
            &FinalizeCall {
                input_tokens: Some(-1),
                ..Default::default()
            },
        )),
        Err(ErrorCode::Internal)
    );
    assert_eq!(ledger_status(handle, model_call_id), "RESERVED");
    assert_eq!(
        reservation_status(handle, reservation.reservation_id),
        "RESERVED"
    );
}

#[test]
fn atomic_completion_binds_identity_and_rolls_back_both_rows() {
    run_db_fixture::<Fixture, _>("provider_budget_atomic_identity", |mut handle| {
        handle
            .admin
            .execute(
                "UPDATE control.retrieval_provider_admission_limits \
                 SET tpm_limit=1000,rpm_limit=1000 WHERE provider_id=$1",
                &[&handle.provider_id],
            )
            .expect("widen identity fixture limits");
        assert_cross_pair_rejected_and_matching_pair_commits(&mut handle);
        assert_post_identity_constraint_failure_rolls_back(&mut handle);
    });
}

#[test]
fn concurrent_dispatch_and_atomic_completion_share_one_lock_order() {
    run_db_fixture::<Fixture, _>("provider_budget_atomic_lock_order", |mut handle| {
        let model_call_id = reserve_test_ledger(&handle, handle.tenant_id);
        let reservation = handle
            .rt
            .block_on(reserve_provider_budget(
                &handle.retrieval,
                &request(&handle, model_call_id, Duration::from_secs(30)),
            ))
            .expect("reserve lock-order pair");
        let finalize = FinalizeCall::default();
        let (marked, completed) = handle
            .rt
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(5), async {
                    tokio::join!(
                        mark_provider_budget_dispatched(
                            &handle.retrieval,
                            TenantId(handle.tenant_id),
                            reservation.reservation_id,
                        ),
                        finalize_and_settle_provider_budget(
                            &handle.retrieval,
                            TenantId(handle.tenant_id),
                            reservation.reservation_id,
                            model_call_id,
                            ModelCallOutcome::Failed,
                            &finalize,
                        )
                    )
                })
                .await
            })
            .expect("shared lock order prevents deadlock");

        match (marked, completed) {
            (Ok(_), Ok(ProviderBudgetReservationStatus::Consumed)) => {
                assert!(reservation_dispatched(
                    &mut handle,
                    reservation.reservation_id
                ));
            }
            (Err(ErrorCode::Conflict), Ok(ProviderBudgetReservationStatus::Released)) => {
                assert!(!reservation_dispatched(
                    &mut handle,
                    reservation.reservation_id
                ));
            }
            other => panic!("unexpected dispatch/finalize race result: {other:?}"),
        }
        assert_ne!(
            reservation_status(&mut handle, reservation.reservation_id),
            "RESERVED"
        );
    });
}

fn assert_allocation_snapshot_is_immutable(
    handle: &mut Handle,
    reservation_id: Uuid,
) -> (String, i64, i64) {
    let snapshot_before: (String, i64, i64) = handle.admin.query_one("SELECT limit_provider_id, limit_tpm_limit, limit_rpm_limit FROM ops.retrieval_provider_budget_allocations WHERE tenant_id=$1 AND reservation_id=$2 AND tier='GLOBAL'", &[&handle.tenant_id, &reservation_id]).map(|row| (row.get(0), row.get(1), row.get(2))).expect("allocation configuration snapshot");
    handle.admin.execute("UPDATE control.retrieval_provider_admission_limits SET provider_id='rewritten-provider', tpm_limit=99, rpm_limit=98 WHERE limit_id=(SELECT limit_id FROM ops.retrieval_provider_budget_allocations WHERE tenant_id=$1 AND reservation_id=$2 AND tier='GLOBAL')", &[&handle.tenant_id, &reservation_id]).expect("mutate active config after allocation");
    let snapshot_after: (String, i64, i64) = handle.admin.query_one("SELECT limit_provider_id, limit_tpm_limit, limit_rpm_limit FROM ops.retrieval_provider_budget_allocations WHERE tenant_id=$1 AND reservation_id=$2 AND tier='GLOBAL'", &[&handle.tenant_id, &reservation_id]).map(|row| (row.get(0), row.get(1), row.get(2))).expect("immutable allocation configuration snapshot");
    assert_eq!(snapshot_before, snapshot_after);
    snapshot_before
}

fn assert_four_tiers_and_restore_snapshot(
    handle: &mut Handle,
    reservation_id: Uuid,
    snapshot: &(String, i64, i64),
) {
    let tier_counts: (i64, i64, i64, i64) = handle.admin.query_one("SELECT count(*) FILTER (WHERE tier='GLOBAL'), count(*) FILTER (WHERE tier='REGION'), count(*) FILTER (WHERE tier='TENANT'), count(*) FILTER (WHERE tier='PURPOSE') FROM ops.retrieval_provider_budget_allocations WHERE tenant_id=$1 AND reservation_id=$2", &[&handle.tenant_id, &reservation_id]).map(|row| (row.get(0), row.get(1), row.get(2), row.get(3))).expect("four canonical allocation tiers");
    assert_eq!(tier_counts, (1, 1, 1, 1));
    handle.admin.execute("UPDATE control.retrieval_provider_admission_limits SET provider_id=$3, tpm_limit=$4, rpm_limit=$5 WHERE limit_id=(SELECT limit_id FROM ops.retrieval_provider_budget_allocations WHERE tenant_id=$1 AND reservation_id=$2 AND tier='GLOBAL')", &[&handle.tenant_id, &reservation_id, &snapshot.0, &snapshot.1, &snapshot.2]).expect("restore fixture config after snapshot assertion");
}

fn assert_expiry_reclaims_idempotently(handle: &mut Handle) {
    let expiry_call = reserve_test_ledger(handle, handle.tenant_id);
    let expiry = handle
        .rt
        .block_on(reserve_provider_budget(
            &handle.retrieval,
            &request(handle, expiry_call, Duration::from_millis(1)),
        ))
        .expect("budget reserve");
    thread::sleep(Duration::from_millis(5));
    assert_eq!(
        handle.rt.block_on(reap_expired_provider_budgets(
            &handle.maintenance,
            TenantId(handle.tenant_id),
            8
        )),
        Ok(1)
    );
    assert_eq!(
        handle.rt.block_on(reap_expired_provider_budgets(
            &handle.maintenance,
            TenantId(handle.tenant_id),
            8
        )),
        Ok(0)
    );
    assert_eq!(
        handle.rt.block_on(settle_provider_budget(
            &handle.retrieval,
            TenantId(handle.tenant_id),
            expiry.reservation_id
        )),
        Err(ErrorCode::Conflict)
    );
}

fn assert_dispatched_success_consumes(handle: &mut Handle) {
    let success_call = reserve_test_ledger(handle, handle.tenant_id);
    let success = handle
        .rt
        .block_on(reserve_provider_budget(
            &handle.retrieval,
            &request(handle, success_call, Duration::from_secs(30)),
        ))
        .expect("budget reserve");
    handle
        .rt
        .block_on(mark_provider_budget_dispatched(
            &handle.retrieval,
            TenantId(handle.tenant_id),
            success.reservation_id,
        ))
        .expect("persist dispatch before success");
    handle
        .rt
        .block_on(model_call_ledger::finalize_call(
            &handle.retrieval,
            handle.tenant_id,
            success_call,
            ModelCallOutcome::Succeeded,
            &FinalizeCall::default(),
        ))
        .expect("ledger success");
    assert_eq!(
        handle.rt.block_on(settle_provider_budget(
            &handle.retrieval,
            TenantId(handle.tenant_id),
            success.reservation_id
        )),
        Ok(ProviderBudgetReservationStatus::Consumed)
    );
}

#[test]
fn replay_and_identity_mismatches_do_not_create_budget_leaks() {
    run_db_fixture::<Fixture, _>("provider_budget_identity", |mut handle| {
        let call = handle
            .rt
            .block_on(model_call_ledger::reserve_call(
                &handle.retrieval,
                &ledger(&handle),
            ))
            .expect("ledger");
        let original = request(&handle, call.model_call_id, Duration::from_secs(30));
        let reservation = handle
            .rt
            .block_on(reserve_provider_budget(&handle.retrieval, &original))
            .expect("initial reserve");
        assert_same_request_replays(&handle, &original, reservation.reservation_id);
        let changed_tokens = ProviderBudgetRequest {
            estimated_tokens: 8,
            ..original
        };
        assert_eq!(
            handle
                .rt
                .block_on(reserve_provider_budget(&handle.retrieval, &changed_tokens)),
            Err(ErrorCode::Conflict)
        );
        let wrong_provider = ProviderBudgetRequest {
            provider_id: "wrong-provider",
            ..original
        };
        assert_eq!(
            handle
                .rt
                .block_on(reserve_provider_budget(&handle.retrieval, &wrong_provider)),
            Err(ErrorCode::Conflict)
        );
        for changed in [
            ProviderBudgetRequest {
                model_id: "wrong-model",
                ..original
            },
            ProviderBudgetRequest {
                region: "wrong-region",
                ..original
            },
            ProviderBudgetRequest {
                purpose: PrivateDataPurpose::RetrievalRerank,
                ..original
            },
            ProviderBudgetRequest {
                tenant_id: TenantId(Uuid::now_v7()),
                ..original
            },
        ] {
            assert_eq!(
                handle
                    .rt
                    .block_on(reserve_provider_budget(&handle.retrieval, &changed)),
                Err(ErrorCode::Conflict)
            );
        }
        let finalized_call = handle
            .rt
            .block_on(model_call_ledger::reserve_call(
                &handle.retrieval,
                &ledger(&handle),
            ))
            .expect("ledger to finalize before budget reserve");
        handle
            .rt
            .block_on(model_call_ledger::finalize_call(
                &handle.retrieval,
                handle.tenant_id,
                finalized_call.model_call_id,
                ModelCallOutcome::Failed,
                &FinalizeCall::default(),
            ))
            .expect("terminal ledger");
        assert_eq!(
            handle.rt.block_on(reserve_provider_budget(
                &handle.retrieval,
                &request(
                    &handle,
                    finalized_call.model_call_id,
                    Duration::from_secs(30)
                ),
            )),
            Err(ErrorCode::Conflict),
            "budget reserve requires a still-RESERVED ledger"
        );
        let rows: i64 = handle.admin.query_one("SELECT count(*) FROM ops.retrieval_provider_budget_reservations WHERE tenant_id=$1", &[&handle.tenant_id]).expect("reservation count").get(0);
        assert_eq!(
            rows, 1,
            "rejected replays must not leak an extra reservation or allocation ledger"
        );
    });
}

fn assert_same_request_replays(
    handle: &Handle,
    request: &ProviderBudgetRequest<'_>,
    reservation_id: Uuid,
) {
    assert_eq!(
        handle
            .rt
            .block_on(reserve_provider_budget(&handle.retrieval, request))
            .expect("same replay")
            .reservation_id,
        reservation_id
    );
}

#[test]
fn budget_tables_are_function_only_for_actual_runtime_roles() {
    run_db_fixture::<Fixture, _>("provider_budget_acl", |handle| {
        let mut retrieval = Client::connect(&handle.retrieval_dsn, NoTls).expect("retrieval login");
        let mut maintenance =
            Client::connect(&handle.maintenance_dsn, NoTls).expect("maintenance login");
        let mut gateway = Client::connect(&handle.gateway_dsn, NoTls).expect("gateway login");
        for client in [&mut retrieval, &mut maintenance, &mut gateway] {
            for statement in [
                "INSERT INTO ops.retrieval_provider_budget_reservations DEFAULT VALUES",
                "INSERT INTO ops.retrieval_provider_budget_allocations DEFAULT VALUES",
            ] {
                let error = client
                    .execute(statement, &[])
                    .expect_err("runtime role must not have direct budget-table DML");
                assert_eq!(error.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE));
            }
        }
        let gateway_error = gateway
            .query_one(
                "SELECT * FROM ops.reserve_retrieval_provider_budget($1,$2,$3,$4,$5,$6,$7,$8)",
                &[
                    &handle.tenant_id,
                    &Uuid::now_v7(),
                    &"x",
                    &"x",
                    &"x",
                    &"embedding",
                    &1_i64,
                    &1_i64,
                ],
            )
            .expect_err("gateway cannot execute reserve function");
        assert_eq!(
            gateway_error.code(),
            Some(&SqlState::INSUFFICIENT_PRIVILEGE)
        );
        let retrieval_error = retrieval
            .query_one(
                "SELECT ops.reap_expired_retrieval_provider_budget($1,$2)",
                &[&handle.tenant_id, &1_i32],
            )
            .expect_err("retrieval worker cannot execute maintenance reaper");
        assert_eq!(
            retrieval_error.code(),
            Some(&SqlState::INSUFFICIENT_PRIVILEGE)
        );
        let maintenance_error = maintenance
            .query_one(
                "SELECT * FROM ops.reserve_retrieval_provider_budget($1,$2,$3,$4,$5,$6,$7,$8)",
                &[
                    &handle.tenant_id,
                    &Uuid::now_v7(),
                    &"x",
                    &"x",
                    &"x",
                    &"embedding",
                    &1_i64,
                    &1_i64,
                ],
            )
            .expect_err("maintenance cannot execute reserve function");
        assert_eq!(
            maintenance_error.code(),
            Some(&SqlState::INSUFFICIENT_PRIVILEGE)
        );
    });
}

#[test]
fn actual_roles_reject_null_sql_arguments_without_mutating_budget_state() {
    run_db_fixture::<Fixture, _>("provider_budget_null_arguments", |mut handle| {
        let call = handle
            .rt
            .block_on(model_call_ledger::reserve_call(
                &handle.retrieval,
                &ledger(&handle),
            ))
            .expect("ledger for direct SQL null probes");
        let reservation = handle
            .rt
            .block_on(reserve_provider_budget(
                &handle.retrieval,
                &request(&handle, call.model_call_id, Duration::from_secs(30)),
            ))
            .expect("reservation for direct SQL null probes");
        let before: (i64, i64, String) = handle
            .admin
            .query_one(
                "SELECT count(*),coalesce(sum(a.tokens),0)::bigint,max(r.status) \
                 FROM ops.retrieval_provider_budget_reservations r \
                 LEFT JOIN ops.retrieval_provider_budget_allocations a \
                   ON a.tenant_id=r.tenant_id AND a.reservation_id=r.reservation_id \
                 WHERE r.tenant_id=$1",
                &[&handle.tenant_id],
            )
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .expect("budget state before null probes");
        assert_actual_role_null_calls(&handle, reservation.reservation_id, call.model_call_id);

        let after: (i64, i64, String) = handle
            .admin
            .query_one(
                "SELECT count(*),coalesce(sum(a.tokens),0)::bigint,max(r.status) \
                 FROM ops.retrieval_provider_budget_reservations r \
                 LEFT JOIN ops.retrieval_provider_budget_allocations a \
                   ON a.tenant_id=r.tenant_id AND a.reservation_id=r.reservation_id \
                 WHERE r.tenant_id=$1",
                &[&handle.tenant_id],
            )
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .expect("budget state after null probes");
        assert_eq!(
            after, before,
            "NULL direct SQL calls leave every row unchanged"
        );
    });
}

fn assert_actual_role_null_calls(handle: &Handle, reservation_id: Uuid, model_call_id: Uuid) {
    let null_uuid: Option<Uuid> = None;
    let null_text: Option<&str> = None;
    let null_i32: Option<i32> = None;
    let mut retrieval = Client::connect(&handle.retrieval_dsn, NoTls)
        .expect("actual retrieval-worker login for null probes");
    retrieval
        .execute(
            "SELECT set_config('humaux.tenant_id',$1,false)",
            &[&handle.tenant_id.to_string()],
        )
        .expect("scope retrieval-worker direct SQL session");
    let reserve_null = retrieval
        .query_one(
            "SELECT * FROM ops.reserve_retrieval_provider_budget($1,$2,$3,$4,$5,$6,$7,$8)",
            &[
                &handle.tenant_id,
                &Uuid::now_v7(),
                &handle.provider_id,
                &"provider-budget-model",
                &handle.region,
                &null_text,
                &1_i64,
                &1_i64,
            ],
        )
        .expect_err("NULL reserve purpose must fail before admission");
    assert_eq!(reserve_null.code().map(SqlState::code), Some("P0003"));
    let mark_null = retrieval
        .query_one(
            "SELECT ops.mark_retrieval_provider_budget_dispatched($1,$2)",
            &[&handle.tenant_id, &null_uuid],
        )
        .expect_err("NULL dispatch reservation id must fail closed");
    assert_eq!(mark_null.code().map(SqlState::code), Some("P0003"));
    let settle_null = retrieval
        .query_one(
            "SELECT ops.settle_retrieval_provider_budget($1,$2)",
            &[&handle.tenant_id, &null_uuid],
        )
        .expect_err("NULL settle reservation id must fail closed");
    assert_eq!(settle_null.code().map(SqlState::code), Some("P0003"));
    assert_atomic_finalize_null_calls(
        &mut retrieval,
        handle.tenant_id,
        reservation_id,
        model_call_id,
    );
    let mut maintenance = Client::connect(&handle.maintenance_dsn, NoTls)
        .expect("actual maintenance login for null probes");
    maintenance
        .execute(
            "SELECT set_config('humaux.tenant_id',$1,false)",
            &[&handle.tenant_id.to_string()],
        )
        .expect("scope maintenance direct SQL session");
    let reap_null = maintenance
        .query_one(
            "SELECT ops.reap_expired_retrieval_provider_budget($1,$2)",
            &[&handle.tenant_id, &null_i32],
        )
        .expect_err("NULL reaper limit must not become an unbounded batch");
    assert_eq!(reap_null.code().map(SqlState::code), Some("P0003"));
}

fn assert_atomic_finalize_null_calls(
    retrieval: &mut Client,
    tenant_id: Uuid,
    reservation_id: Uuid,
    model_call_id: Uuid,
) {
    let null_uuid: Option<Uuid> = None;
    let null_text: Option<&str> = None;
    let null_i32: Option<i32> = None;
    let null_i64: Option<i64> = None;
    let null_bool: Option<bool> = None;
    let null_f64: Option<f64> = None;
    let finalize_null = retrieval
        .query_one(
            "SELECT ops.finalize_retrieval_provider_budget(\
               $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
            &[
                &tenant_id,
                &null_uuid,
                &Uuid::now_v7(),
                &"FAILED",
                &null_i64,
                &null_i64,
                &null_i32,
                &null_i64,
                &null_bool,
                &null_i32,
                &null_f64,
                &null_text,
                &null_text,
            ],
        )
        .expect_err("NULL atomic-finalize reservation id must fail closed");
    assert_eq!(finalize_null.code().map(SqlState::code), Some("P0003"));
    let finalize_outcome_null = retrieval
        .query_one(
            "SELECT ops.finalize_retrieval_provider_budget(\
               $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
            &[
                &tenant_id,
                &reservation_id,
                &model_call_id,
                &null_text,
                &null_i64,
                &null_i64,
                &null_i32,
                &null_i64,
                &null_bool,
                &null_i32,
                &null_f64,
                &null_text,
                &null_text,
            ],
        )
        .expect_err("NULL atomic-finalize outcome must fail at the SQL boundary");
    assert_eq!(
        finalize_outcome_null.code().map(SqlState::code),
        Some("P0003")
    );
}

#[test]
fn overlapping_finite_limit_cannot_replace_a_missing_canonical_tier() {
    run_db_fixture::<Fixture, _>("provider_budget_overlapping_limit", |mut handle| {
        // 0093 forbids two open-ended rows, but a finite row can still overlap the current
        // Global tier.  A bare "four rows" lookup would accept two Globals plus no Purpose;
        // reserve must classify and reject that malformed active set.
        handle
            .admin
            .execute(
                "INSERT INTO control.retrieval_provider_admission_limits \
                 (provider_id, tpm_limit, rpm_limit, effective_from, effective_to) \
                 VALUES($1,10,10,clock_timestamp()-interval '1 second',clock_timestamp()+interval '1 minute')",
                &[&handle.provider_id],
            )
            .expect("seed finite overlapping global limit");
        handle
            .admin
            .execute(
                "DELETE FROM control.retrieval_provider_admission_limits \
                 WHERE tenant_id=$1 AND provider_id=$2 AND region IS NULL \
                   AND purpose='RETRIEVAL_EMBEDDING'",
                &[&handle.tenant_id, &handle.provider_id],
            )
            .expect("remove purpose tier to prove classification, not count, is enforced");
        let call = handle
            .rt
            .block_on(model_call_ledger::reserve_call(
                &handle.retrieval,
                &ledger(&handle),
            ))
            .expect("ledger");
        assert_eq!(
            handle.rt.block_on(reserve_provider_budget(
                &handle.retrieval,
                &request(&handle, call.model_call_id, Duration::from_secs(30)),
            )),
            Err(ErrorCode::Conflict)
        );
        assert_eq!(allocation_totals(&mut handle), (0, 0));
    });
}
