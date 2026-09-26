//! §72.2.1 acceptance candidates for quota reservations and independent rate buckets.
//! These tests intentionally require the dedicated RequestGuard fixture after migration 0113.

use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use humaux_adapters::{
    postgres::{MaintenanceDbPool, RuntimeDbPool},
    quota_repo::{self, RatePolicy, RateSubject, ReservationStatus, ReserveResult},
};
use humaux_domain::{
    error::ErrorCode,
    identity::{AuthorizationScope, BoundedSet, PrincipalId},
    ids::{TenantId, UserId},
};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sqlx::postgres::PgConnectOptions;
use sqlx::types::Uuid;
use tokio::sync::Barrier;

const FIXTURE_DB: &str = "humaux_thread_request_guard_20260828";
const TENANT_ID: Uuid = Uuid::from_u128(0x0bad_cafe_0000_4000_8000_0000_0000_0101);
const USER_ID: Uuid = Uuid::from_u128(0x0bad_cafe_0000_4000_8000_0000_0000_0102);
const PRINCIPAL_ID: Uuid = Uuid::from_u128(0x0bad_cafe_0000_4000_8000_0000_0000_0103);
const TENANT2_ID: Uuid = Uuid::from_u128(0x0bad_cafe_0000_4000_8000_0000_0000_0111);

struct Handle {
    rt: tokio::runtime::Runtime,
    runtime: RuntimeDbPool,
    maintenance: MaintenanceDbPool,
    admin: Client,
    gateway: Client,
    auth: AuthorizationScope,
    auth2: AuthorizationScope,
}

struct Fixture;

fn setup_failed<T>(_: T) -> DbFixtureSkipReason {
    DbFixtureSkipReason::IsolationSetupFailed("quota fixture setup failed".into())
}

/// ADR-0047 D-D: the dedicated request-guard fixture is whatever `HUMAUX_TEST_PG_DSN` names —
/// under `cargo xtask serial-lane` a per-run `humaux_thread_request_guard_<stamp>` database the
/// lane provisions and migrates — and every role DSN must name that same database. The
/// machine-local `61719 / FIXTURE_DB` pair stays an accepted legacy target.
///
/// Pinning *only* that pair made every test in this file report `IsolationSetupFailed` on any
/// standard node: a printed SKIP, or a fail under `HUMAUX_REQUIRE_DB=1`. The identical rule
/// already landed in `support/operation_receipt_fixture.rs` and `g80_31_handoff.rs`; this is a
/// read of that rule, not a second one.
fn same_target(role: &PgConnectOptions, owner: &PgConnectOptions) -> bool {
    (role.get_port() == 61719 && role.get_database() == Some(FIXTURE_DB))
        || (role.get_port() == owner.get_port() && role.get_database() == owner.get_database())
}

impl DbIntegrationFixture for Fixture {
    type Handle = Handle;

    #[allow(clippy::too_many_lines)] // One isolated fixture validates the complete role and schema preflight before tests run.
    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let owner_dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let options = PgConnectOptions::from_str(&owner_dsn).map_err(setup_failed)?;
        if options.get_host() != "127.0.0.1" || owner_dsn.contains(['?', '#']) {
            return Err(setup_failed(()));
        }
        let mut admin = Client::connect(&owner_dsn, NoTls).map_err(setup_failed)?;
        let ready: bool = admin
            .query_one(
                "SELECT to_regclass('control.usage_reservations') IS NOT NULL
                 AND to_regclass('control.rate_buckets') IS NOT NULL",
                &[],
            )
            .map_err(setup_failed)?
            .get(0);
        if !ready {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "migration 0113 is not applied".into(),
            ));
        }
        admin
            .execute(
                "INSERT INTO control.tenants(tenant_id,name,state) VALUES($1,'quota fixture','ACTIVE'),($2,'quota fixture two','ACTIVE') ON CONFLICT DO NOTHING",
                &[&TENANT_ID, &TENANT2_ID],
            )
            .map_err(setup_failed)?;
        admin
            .execute(
                "INSERT INTO control.users(user_id,state) VALUES($1,'ACTIVE') ON CONFLICT DO NOTHING",
                &[&USER_ID],
            )
            .map_err(setup_failed)?;
        admin
            .execute(
                "INSERT INTO control.memberships(tenant_id,user_id,role,state) VALUES($1,$2,'member','ACTIVE') ON CONFLICT DO NOTHING",
                &[&TENANT_ID, &USER_ID],
            )
            .map_err(setup_failed)?;
        admin
            .execute(
                "INSERT INTO control.memberships(tenant_id,user_id,role,state) VALUES($1,$2,'member','ACTIVE') ON CONFLICT DO NOTHING",
                &[&TENANT2_ID, &USER_ID],
            )
            .map_err(setup_failed)?;
        let gateway_dsn = std::env::var("HUMAUX_GATEWAY_PG_DSN").map_err(setup_failed)?;
        let maintenance_dsn = std::env::var("HUMAUX_MAINTENANCE_PG_DSN").map_err(setup_failed)?;
        let gateway_options = PgConnectOptions::from_str(&gateway_dsn).map_err(setup_failed)?;
        let maintenance_options =
            PgConnectOptions::from_str(&maintenance_dsn).map_err(setup_failed)?;
        if gateway_options.get_username() != "role_gateway"
            || maintenance_options.get_username() != "role_maintenance"
            || gateway_options.get_host() != "127.0.0.1"
            || maintenance_options.get_host() != "127.0.0.1"
            || !same_target(&gateway_options, &options)
            || !same_target(&maintenance_options, &options)
            || gateway_dsn.contains(['?', '#'])
            || maintenance_dsn.contains(['?', '#'])
        {
            return Err(setup_failed(()));
        }
        let mut gateway = Client::connect(&gateway_dsn, NoTls).map_err(setup_failed)?;
        let role_ok: bool = gateway
            .query_one(
                "SELECT current_user='role_gateway' AND session_user='role_gateway'
                 AND NOT (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user)",
                &[],
            )
            .map_err(setup_failed)?
            .get(0);
        if !role_ok {
            return Err(setup_failed(()));
        }
        let rt = tokio::runtime::Runtime::new().map_err(setup_failed)?;
        let runtime = rt
            .block_on(RuntimeDbPool::connect(&gateway_dsn))
            .map_err(setup_failed)?;
        let maintenance = rt
            .block_on(MaintenanceDbPool::connect(&maintenance_dsn))
            .map_err(setup_failed)?;
        let auth = AuthorizationScope::new(
            TenantId(TENANT_ID),
            PrincipalId(PRINCIPAL_ID),
            Some(UserId(USER_ID)),
            BoundedSet::new([]).map_err(setup_failed)?,
        );
        let auth2 = AuthorizationScope::new(
            TenantId(TENANT2_ID),
            PrincipalId(PRINCIPAL_ID),
            Some(UserId(USER_ID)),
            BoundedSet::new([]).map_err(setup_failed)?,
        );
        Ok(Handle {
            rt,
            runtime,
            maintenance,
            admin,
            gateway,
            auth,
            auth2,
        })
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM control.rate_buckets WHERE tenant_id IN ('{TENANT_ID}','{TENANT2_ID}');
             DELETE FROM control.usage_reservations WHERE tenant_id IN ('{TENANT_ID}','{TENANT2_ID}');
             DELETE FROM control.quota_windows WHERE tenant_id IN ('{TENANT_ID}','{TENANT2_ID}');
             DELETE FROM control.entitlement_snapshots WHERE tenant_id IN ('{TENANT_ID}','{TENANT2_ID}');
             DELETE FROM control.memberships WHERE tenant_id IN ('{TENANT_ID}','{TENANT2_ID}');
             DELETE FROM control.users WHERE user_id='{USER_ID}';
             DELETE FROM control.tenants WHERE tenant_id IN ('{TENANT_ID}','{TENANT2_ID}');"
        ));
    }
}

fn snapshot(handle: &mut Handle, tenant: Uuid, limit: i64) {
    let effective = format!(
        "jsonb_build_object('mcp.billable_operations.per_period', jsonb_build_object('limit',{limit},'period','subscription_period','charge_policy','success_only','period_start',to_char((clock_timestamp()-interval '1 second') AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),'period_end',to_char((clock_timestamp()+interval '1 hour') AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')))"
    );
    handle
        .admin
        .execute(
            &format!(
                "INSERT INTO control.entitlement_snapshots(tenant_id,effective,source_grant_ids,computed_at)
                 VALUES($1,{effective},ARRAY[$2]::uuid[],clock_timestamp())
                 ON CONFLICT (tenant_id) DO UPDATE SET effective=EXCLUDED.effective,source_grant_ids=EXCLUDED.source_grant_ids,computed_at=EXCLUDED.computed_at"
            ),
            &[&tenant, &Uuid::now_v7()],
        )
        .expect("admin seeds projected snapshot");
}

fn issue_for(handle: &mut Handle, tenant: Uuid, limit: i64) {
    snapshot(handle, tenant, limit);
    handle
        .rt
        .block_on(quota_repo::issue_window(
            &handle.maintenance,
            TenantId(tenant),
        ))
        .expect("maintenance issues projected quota window");
}

fn issue(handle: &mut Handle, limit: i64) {
    issue_for(handle, TENANT_ID, limit);
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("bounded concurrent fixture operation")
}

async fn issue_at_barrier(
    pool: &MaintenanceDbPool,
    barrier: Arc<Barrier>,
    tenant: TenantId,
) -> Result<quota_repo::QuotaWindow, ErrorCode> {
    barrier.wait().await;
    quota_repo::issue_window(pool, tenant).await
}

async fn reserve_at_barrier<'a>(
    pool: &'a RuntimeDbPool,
    auth: &'a AuthorizationScope,
    barrier: Arc<Barrier>,
    request_id: Uuid,
    fingerprint: &'a str,
) -> Result<ReserveResult, ErrorCode> {
    barrier.wait().await;
    quota_repo::reserve_bmo(
        pool,
        auth,
        request_id,
        "mcp.read",
        fingerprint,
        Duration::from_secs(60),
    )
    .await
}

fn change_limit_preserving_period(handle: &mut Handle, limit: i64) {
    handle
        .admin
        .execute(
            "UPDATE control.entitlement_snapshots
             SET effective=jsonb_set(effective,'{mcp.billable_operations.per_period,limit}',to_jsonb($2::bigint))
             WHERE tenant_id=$1",
            &[&TENANT_ID, &limit],
        )
        .expect("admin changes only projected limit");
}

fn change_period_end_preserving_start(handle: &mut Handle) {
    handle
        .admin
        .execute(
            "UPDATE control.entitlement_snapshots
             SET effective=jsonb_set(effective,'{mcp.billable_operations.per_period,period_end}',
                 to_jsonb(to_char((q.window_end + interval '1 hour') AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')))
             FROM (SELECT window_end FROM control.quota_windows WHERE tenant_id=$1 AND entitlement_key=$2 LIMIT 1) q
             WHERE tenant_id=$1",
            &[&TENANT_ID, &quota_repo::BMO_ENTITLEMENT],
        )
        .expect("admin changes only projected period end");
}

#[test]
#[ignore = "lane(a:request_guard) requires migration 0113 and the dedicated request-guard PostgreSQL fixture"]
fn concurrent_issuer_is_single_window_and_retry_does_not_rewrite_limit() {
    run_db_fixture::<Fixture, _>("quota_concurrent_issuer", |mut h| {
        snapshot(&mut h, TENANT_ID, 3);
        let barrier = Arc::new(Barrier::new(4));
        let (a, b, c, d) = h.rt.block_on(bounded(async {
            tokio::join!(
                issue_at_barrier(&h.maintenance, barrier.clone(), TenantId(TENANT_ID)),
                issue_at_barrier(&h.maintenance, barrier.clone(), TenantId(TENANT_ID)),
                issue_at_barrier(&h.maintenance, barrier.clone(), TenantId(TENANT_ID)),
                issue_at_barrier(&h.maintenance, barrier, TenantId(TENANT_ID)),
            )
        }));
        assert_eq!(
            [a.is_ok(), b.is_ok(), c.is_ok(), d.is_ok()]
                .into_iter()
                .filter(|v| *v)
                .count(),
            4
        );
        assert_eq!(
            h.admin
                .query_one(
                    "SELECT count(*) FROM control.quota_windows WHERE tenant_id=$1",
                    &[&TENANT_ID]
                )
                .unwrap()
                .get::<_, i64>(0),
            1
        );
        change_limit_preserving_period(&mut h, 99);
        assert!(
            h.rt.block_on(quota_repo::issue_window(
                &h.maintenance,
                TenantId(TENANT_ID)
            ))
            .is_ok()
        );
        assert_eq!(h.admin.query_one("SELECT hard_limit FROM control.quota_windows WHERE tenant_id=$1 AND entitlement_key=$2", &[&TENANT_ID, &quota_repo::BMO_ENTITLEMENT]).unwrap().get::<_, i64>(0), 3);
        change_period_end_preserving_start(&mut h);
        assert!(
            h.rt.block_on(quota_repo::issue_window(
                &h.maintenance,
                TenantId(TENANT_ID)
            ))
            .is_err()
        );
    });
}

#[test]
#[ignore = "lane(a:request_guard) requires migration 0113 and the dedicated request-guard PostgreSQL fixture"]
fn concurrent_capacity_one_creates_one_and_same_request_is_idempotent() {
    run_db_fixture::<Fixture, _>("quota_concurrent_reservations", |mut h| {
        issue(&mut h, 1);
        let fp = "e".repeat(64);
        let barrier = Arc::new(Barrier::new(4));
        let requests = [
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
        ];
        let (a, b, c, d) = h.rt.block_on(bounded(async {
            tokio::join!(
                reserve_at_barrier(&h.runtime, &h.auth, barrier.clone(), requests[0], &fp),
                reserve_at_barrier(&h.runtime, &h.auth, barrier.clone(), requests[1], &fp),
                reserve_at_barrier(&h.runtime, &h.auth, barrier.clone(), requests[2], &fp),
                reserve_at_barrier(&h.runtime, &h.auth, barrier, requests[3], &fp),
            )
        }));
        let results = [a, b, c, d];
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, Ok(ReserveResult::Created(_))))
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, Err(ErrorCode::QuotaExhausted)))
                .count(),
            3
        );
        let counters = h.admin.query_one("SELECT reserved, consumed FROM control.quota_windows WHERE tenant_id=$1 AND entitlement_key=$2", &[&TENANT_ID, &quota_repo::BMO_ENTITLEMENT]).unwrap();
        assert_eq!(
            (counters.get::<_, i64>(0), counters.get::<_, i64>(1)),
            (1, 0)
        );

        issue_for(&mut h, TENANT2_ID, 4);
        let same = Uuid::now_v7();
        let same_fp = "f".repeat(64);
        let barrier = Arc::new(Barrier::new(4));
        let (a, b, c, d) = h.rt.block_on(bounded(async {
            tokio::join!(
                reserve_at_barrier(&h.runtime, &h.auth2, barrier.clone(), same, &same_fp),
                reserve_at_barrier(&h.runtime, &h.auth2, barrier.clone(), same, &same_fp),
                reserve_at_barrier(&h.runtime, &h.auth2, barrier.clone(), same, &same_fp),
                reserve_at_barrier(&h.runtime, &h.auth2, barrier, same, &same_fp),
            )
        }));
        let results = [a, b, c, d];
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, Ok(ReserveResult::Created(_))))
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, Ok(ReserveResult::Existing(_))))
                .count(),
            3
        );
        assert_eq!(h.admin.query_one("SELECT count(*) FROM control.usage_reservations WHERE tenant_id=$1 AND request_id=$2", &[&TENANT2_ID, &same]).unwrap().get::<_, i64>(0), 1);
    });
}

#[test]
#[ignore = "lane(a:request_guard) requires migration 0113 and the dedicated request-guard PostgreSQL fixture"]
fn issuer_is_projection_only_and_runtime_cannot_issue_or_insert_window() {
    run_db_fixture::<Fixture, _>("quota_issuer_projection_and_acl", |mut h| {
        assert!(
            h.rt.block_on(quota_repo::issue_window(
                &h.maintenance,
                TenantId(TENANT_ID)
            ))
            .is_err()
        );
        issue(&mut h, 2);
        let no_guc = h.gateway.query_one(
            "SELECT control.issue_quota_window($1,$2)",
            &[&TENANT_ID, &quota_repo::BMO_ENTITLEMENT],
        );
        assert!(no_guc.is_err(), "gateway cannot issue without tenant GUC");
        let direct_insert = h.gateway.execute(
            "INSERT INTO control.quota_windows(tenant_id,entitlement_key,window_start,window_end,hard_limit)
             VALUES($1,$2,clock_timestamp(),clock_timestamp()+interval '1 hour',1)",
            &[&TENANT_ID, &quota_repo::BMO_ENTITLEMENT],
        );
        assert!(
            direct_insert.is_err(),
            "gateway cannot insert quota windows"
        );
        let denied = h.admin.query_one(
            "SELECT has_table_privilege('role_gateway','control.quota_windows','INSERT')
             OR has_function_privilege('role_gateway','control.issue_quota_window(uuid,text)','EXECUTE')",
            &[],
        ).expect("admin checks ACL").get::<_, bool>(0);
        assert!(!denied);
    });
}

#[test]
#[ignore = "lane(a:request_guard) requires migration 0113 and the dedicated request-guard PostgreSQL fixture"]
fn reservation_capacity_replay_consumption_release_and_reap_are_terminal() {
    run_db_fixture::<Fixture, _>("quota_reservation_lifecycle", |mut h| {
        issue(&mut h, 1);
        issue_for(&mut h, TENANT2_ID, 1);
        let request = Uuid::now_v7();
        let other =
            h.rt.block_on(quota_repo::reserve_bmo(
                &h.runtime,
                &h.auth2,
                request,
                "mcp.read",
                &"a".repeat(64),
                Duration::from_secs(60),
            ))
            .expect("same request id is independent across tenants");
        assert!(matches!(other, ReserveResult::Created(_)));
        let fp = "a".repeat(64);
        let first =
            h.rt.block_on(quota_repo::reserve_bmo(
                &h.runtime,
                &h.auth,
                request,
                "mcp.read",
                &fp,
                Duration::from_secs(60),
            ))
            .expect("first reservation");
        let reservation = match first {
            ReserveResult::Created(r) => r,
            ReserveResult::Existing(_) => panic!("fresh request"),
        };
        assert!(matches!(
            h.rt.block_on(quota_repo::reserve_bmo(
                &h.runtime,
                &h.auth,
                request,
                "mcp.read",
                &fp,
                Duration::from_secs(60)
            )),
            Ok(ReserveResult::Existing(ReservationStatus::Reserved))
        ));
        assert_eq!(
            h.rt.block_on(quota_repo::finish_reservation(
                &h.runtime,
                &h.auth,
                &reservation,
                true
            )),
            Ok(ReservationStatus::Consumed)
        );
        assert_eq!(
            h.rt.block_on(quota_repo::finish_reservation(
                &h.runtime,
                &h.auth,
                &reservation,
                true
            )),
            Ok(ReservationStatus::Consumed)
        );
        assert_eq!(
            h.rt.block_on(quota_repo::finish_reservation(
                &h.runtime,
                &h.auth,
                &reservation,
                false
            )),
            Err(ErrorCode::Conflict)
        );
        assert!(matches!(
            h.rt.block_on(quota_repo::reserve_bmo(
                &h.runtime,
                &h.auth,
                Uuid::now_v7(),
                "mcp.read",
                &fp,
                Duration::from_micros(1)
            )),
            Err(ErrorCode::QuotaExhausted)
        ));
    });
}

#[test]
#[ignore = "lane(a:request_guard) requires migration 0113 and the dedicated request-guard PostgreSQL fixture"]
fn mismatched_replay_and_invalid_inputs_fail_closed() {
    run_db_fixture::<Fixture, _>("quota_argument_binding", |mut h| {
        issue(&mut h, 2);
        let request = Uuid::now_v7();
        let fp = "b".repeat(64);
        let r = match h
            .rt
            .block_on(quota_repo::reserve_bmo(
                &h.runtime,
                &h.auth,
                request,
                "mcp.write",
                &fp,
                Duration::from_secs(60),
            ))
            .unwrap()
        {
            ReserveResult::Created(r) => r,
            ReserveResult::Existing(_) => panic!("fresh request"),
        };
        assert!(matches!(
            h.rt.block_on(quota_repo::reserve_bmo(
                &h.runtime,
                &h.auth,
                request,
                "mcp.other",
                &fp,
                Duration::from_secs(60)
            )),
            Err(ErrorCode::Conflict)
        ));
        assert_eq!(
            h.rt.block_on(quota_repo::finish_reservation(
                &h.runtime, &h.auth, &r, false
            )),
            Ok(ReservationStatus::Released)
        );
        assert!(matches!(
            RatePolicy::new(0, 1),
            Err(ErrorCode::InvalidInput)
        ));
        assert_eq!(
            h.rt.block_on(quota_repo::consume_rate(
                &h.runtime,
                RateSubject::User(&h.auth),
                "MCP",
                "bucket",
                RatePolicy::new(1, 1).unwrap()
            )),
            Err(ErrorCode::InvalidInput)
        );
    });
}

#[test]
#[ignore = "lane(a:request_guard) requires migration 0113 and the dedicated request-guard PostgreSQL fixture"]
fn rate_buckets_are_subject_scoped_and_quota_failure_does_not_refund_rate() {
    run_db_fixture::<Fixture, _>("rate_bucket_subjects", |mut h| {
        issue(&mut h, 0);
        let policy = RatePolicy::new(1, 1).unwrap();
        assert_eq!(
            h.rt.block_on(quota_repo::consume_rate(
                &h.runtime,
                RateSubject::User(&h.auth),
                "mcp.read",
                "default",
                policy
            )),
            Ok(())
        );
        assert_eq!(
            h.rt.block_on(quota_repo::consume_rate(
                &h.runtime,
                RateSubject::User(&h.auth),
                "mcp.read",
                "default",
                policy
            )),
            Err(ErrorCode::RateLimited)
        );
        assert!(matches!(
            h.rt.block_on(quota_repo::reserve_bmo(
                &h.runtime,
                &h.auth,
                Uuid::now_v7(),
                "mcp.read",
                &"c".repeat(64),
                Duration::from_secs(60)
            )),
            Err(ErrorCode::QuotaExhausted)
        ));
        assert_eq!(
            h.rt.block_on(quota_repo::consume_rate(
                &h.runtime,
                RateSubject::Tenant(&h.auth),
                "mcp.read",
                "default",
                policy
            )),
            Ok(())
        );
    });
}

#[test]
#[ignore = "lane(a:request_guard) requires migration 0113 and the dedicated request-guard PostgreSQL fixture"]
fn expired_reservation_is_reaped_without_charging() {
    run_db_fixture::<Fixture, _>("quota_expiry_reap", |mut h| {
        issue(&mut h, 1);
        let result =
            h.rt.block_on(quota_repo::reserve_bmo(
                &h.runtime,
                &h.auth,
                Uuid::now_v7(),
                "mcp.read",
                &"d".repeat(64),
                Duration::from_secs(2),
            ))
            .unwrap();
        let reservation = match result {
            ReserveResult::Created(r) => r,
            ReserveResult::Existing(_) => panic!("fresh request"),
        };
        let mut expired = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            expired = h
                .admin
                .query_one(
                    "SELECT clock_timestamp() >= expires_at FROM control.usage_reservations WHERE reservation_id=$1",
                    &[&reservation.id()],
                )
                .expect("read DB clock against reservation lease")
                .get(0);
            if expired {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        assert!(expired, "database clock must prove lease expiry");
        assert_eq!(
            h.rt.block_on(quota_repo::reap_expired(
                &h.maintenance,
                TenantId(TENANT_ID),
                10
            ))
            .unwrap(),
            1
        );
        let row = h
            .admin
            .query_one(
                "SELECT status, finished_at IS NOT NULL FROM control.usage_reservations WHERE reservation_id=$1",
                &[&reservation.id()],
            )
            .expect("read reaped reservation");
        assert_eq!(row.get::<_, String>(0), "RELEASED");
        assert!(row.get::<_, bool>(1));
        let counters = h
            .admin
            .query_one(
                "SELECT reserved, consumed FROM control.quota_windows WHERE tenant_id=$1 AND entitlement_key=$2",
                &[&TENANT_ID, &quota_repo::BMO_ENTITLEMENT],
            )
            .expect("read released counters");
        assert_eq!(counters.get::<_, i64>(0), 0);
        assert_eq!(counters.get::<_, i64>(1), 0);
        assert_eq!(
            h.rt.block_on(quota_repo::reap_expired(
                &h.maintenance,
                TenantId(TENANT_ID),
                10
            ))
            .unwrap(),
            0
        );
        assert_eq!(
            h.rt.block_on(quota_repo::finish_reservation(
                &h.runtime,
                &h.auth,
                &reservation,
                true
            )),
            Err(ErrorCode::Conflict)
        );
        let after = h
            .admin
            .query_one(
                "SELECT reserved, consumed FROM control.quota_windows WHERE tenant_id=$1 AND entitlement_key=$2",
                &[&TENANT_ID, &quota_repo::BMO_ENTITLEMENT],
            )
            .expect("read terminal counters");
        assert_eq!((after.get::<_, i64>(0), after.get::<_, i64>(1)), (0, 0));
    });
}

/// Card 24 soak (2026-09-26): both lanes share the pre-auth `ip` bucket, and the request that
/// lost `pg_try_advisory_xact_lock` was answered `RATE_LIMITED` while the bucket held 99 of
/// 100 tokens — §72.3's code for "秒/分钟级速率超限" reported for a lock race any two clients
/// behind one NAT would hit. A contended bucket now WAITS for its holder. Fault control: put
/// the try-lock back and this reads `Err(RateLimited)` after ~0 ms.
#[test]
#[ignore = "lane(a:request_guard) requires migration 0113 and the dedicated request-guard PostgreSQL fixture"]
fn a_contended_rate_bucket_waits_for_its_holder_instead_of_answering_rate_limited() {
    run_db_fixture::<Fixture, _>("rate_bucket_contention", |mut h| {
        issue(&mut h, 0);
        let policy = RatePolicy::new(100, 100).unwrap();
        let key = format!(
            "rate:{}:user:{}:mcp.read:default",
            h.auth.tenant_id().0,
            h.auth.user_id().expect("fixture auth carries a user").0
        );
        // Another session holds the bucket's lock for 500 ms — the shape of a concurrent
        // request on the same bucket. Its own connection, so the wait is a real cross-session
        // lock wait and not the pool handing the consumer the holder's connection.
        let dsn = std::env::var("HUMAUX_GATEWAY_PG_DSN").expect("fixture env");
        let (held_tx, held_rx) = std::sync::mpsc::channel::<()>();
        let holder = std::thread::spawn(move || {
            let mut client = Client::connect(&dsn, NoTls).expect("holder connects");
            let mut txn = client.transaction().expect("holder txn");
            txn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                &[&key],
            )
            .expect("hold the bucket lock");
            held_tx.send(()).expect("signal the lock is held");
            std::thread::sleep(Duration::from_millis(500));
            txn.commit().expect("release the bucket lock");
        });
        held_rx.recv().expect("holder took the lock");
        let started = Instant::now();
        let result = h.rt.block_on(quota_repo::consume_rate(
            &h.runtime,
            RateSubject::User(&h.auth),
            "mcp.read",
            "default",
            policy,
        ));
        let held_for = started.elapsed();
        holder.join().expect("holder thread");
        assert_eq!(result, Ok(()), "contention is a wait, not a rate verdict");
        assert!(
            held_for >= Duration::from_millis(400),
            "the consumer waited for the holder rather than refusing: {held_for:?}"
        );
    });
}
