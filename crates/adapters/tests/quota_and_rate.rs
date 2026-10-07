//! `adapters::tests::quota_and_rate` — §72.2.1 acceptance candidates for quota reservations and independent rate
//!   buckets.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-testkit, postgres, sqlx, tokio];
//!   services=[PostgreSQL(any) w=[control.entitlement_snapshots, control.memberships, control.quota_windows,
//!   control.rate_buckets, control.tenants, control.usage_reservations, control.users]
//!   x=[control.issue_quota_window], PostgreSQL(role_gateway), PostgreSQL(role_maintenance)];
//!   env=[HUMAUX_GATEWAY_PG_DSN, HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_TEST_PG_DSN]; modules=[adapters::postgres,
//!   adapters::quota_repo, adapters::tests::support::throwaway_db, domain::error, domain::identity, domain::ids,
//!   humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [needs the dedicated RequestGuard fixture after migration 0113 (gateway + maintenance DSNs); exhausted
//!   windows are QuotaExhausted/RateLimited, never an allow; the fixture tests are #[ignore] lane tests; the SEC-6
//!   pre-auth IP tests are not ignored and own a throwaway database each, because pre-auth buckets live under the
//!   system tenant, which no fixture Drop on the shared database may clean; the c38 batch tests (lock order under
//!   contention, parity, lock_timeout, statement_timeout, deadlock victim) likewise own a humaux_thread_c38_rate_*
//!   database each, so their holder sessions and deadlock experiments never touch the shared one]
//! Spec: Baseline §72.2.1; §73.2; §79.2; ADR-0062 E5; ADR-0065 D-D
//!
//! These tests intentionally require the dedicated RequestGuard fixture after migration 0113.

use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use humaux_adapters::{
    postgres::{MaintenanceDbPool, PoolSettings, RuntimeDbPool},
    quota_repo::{self, RateCharge, RatePolicy, RateSubject, ReservationStatus, ReserveResult},
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

#[path = "support/throwaway_db.rs"]
#[allow(dead_code)]
mod throwaway_db;

const FIXTURE_DB: &str = "humaux_thread_request_guard_20260828";
const TENANT_ID: Uuid = Uuid::from_u128(0x0bad_cafe_0000_4000_8000_0000_0000_0101);
const USER_ID: Uuid = Uuid::from_u128(0x0bad_cafe_0000_4000_8000_0000_0000_0102);
const PRINCIPAL_ID: Uuid = Uuid::from_u128(0x0bad_cafe_0000_4000_8000_0000_0000_0103);
const TENANT2_ID: Uuid = Uuid::from_u128(0x0bad_cafe_0000_4000_8000_0000_0000_0111);
/// ADR-0065 D-D: the former `SET LOCAL lock_timeout` literal, now the registered key's fixture value.
const LOCK_TIMEOUT: Duration = Duration::from_secs(2);

struct Handle {
    rt: tokio::runtime::Runtime,
    runtime: RuntimeDbPool,
    maintenance: MaintenanceDbPool,
    admin: Client,
    gateway: Client,
    auth: AuthorizationScope,
    auth2: AuthorizationScope,
    /// Last field: released only after `Drop` has deleted the fixture rows.
    _serial: std::sync::MutexGuard<'static, ()>,
}

/// Every `Fixture` test seeds and (in `Drop`) deletes the same TENANT_ID / TENANT2_ID rows, so two of them in one
/// process must not overlap: under the default parallel harness one test's Drop deleted the tenant another was using
/// (`TenantBoundary`, card 38 S3 run of `--include-ignored`). The serial lane ran them one at a time anyway.
static SHARED_FIXTURE: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
        let serial = SHARED_FIXTURE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let owner_dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let options = PgConnectOptions::from_str(&owner_dsn).map_err(setup_failed)?;
        if options.get_host() != "127.0.0.1" || owner_dsn.contains(['?', '#']) {
            return Err(setup_failed(()));
        }
        // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
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
        // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
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
            // dep: PostgreSQL(role_gateway) — open a role-scoped PG connection/pool for this test
            .block_on(RuntimeDbPool::connect(&gateway_dsn))
            .map_err(setup_failed)?;
        let maintenance = rt
            // dep: PostgreSQL(role_maintenance) — open a role-scoped PG connection/pool for this test
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
            _serial: serial,
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
                RatePolicy::new(1, 1).unwrap(),
                LOCK_TIMEOUT
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
                policy,
                LOCK_TIMEOUT
            )),
            Ok(())
        );
        assert_eq!(
            h.rt.block_on(quota_repo::consume_rate(
                &h.runtime,
                RateSubject::User(&h.auth),
                "mcp.read",
                "default",
                policy,
                LOCK_TIMEOUT
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
                policy,
                LOCK_TIMEOUT
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
            // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
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
            LOCK_TIMEOUT,
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

/// Fields drop in order: the pool and the owner connection close before `_db` drops the database.
struct PreauthHandle {
    rt: tokio::runtime::Runtime,
    runtime: RuntimeDbPool,
    admin: Client,
    _db: throwaway_db::ThrowawayDb,
}

struct PreauthFixture;

impl DbIntegrationFixture for PreauthFixture {
    type Handle = PreauthHandle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let setup = |e: String| DbFixtureSkipReason::IsolationSetupFailed(e);
        let db = throwaway_db::create("c35_sec6")?;
        let dsn = db.dsn();
        // dep: PostgreSQL(any) — owner connection reading the bucket keys of this test's throwaway database
        let admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
        let rt = tokio::runtime::Runtime::new().map_err(|e| setup(e.to_string()))?;
        let sep = if dsn.contains('?') { '&' } else { '?' };
        // dep: PostgreSQL(role_gateway) — the pre-auth rate consumer
        let runtime = rt
            .block_on(RuntimeDbPool::connect(&format!(
                "{dsn}{sep}options=-c%20role%3Drole_gateway"
            )))
            .map_err(|e| setup(e.to_string()))?;
        Ok(PreauthHandle {
            rt,
            runtime,
            admin,
            _db: db,
        })
    }
}

impl PreauthHandle {
    /// One pre-auth call from `ip` against a capacity-1 bucket.
    fn consume(&self, ip: &str) -> Result<(), ErrorCode> {
        self.rt.block_on(quota_repo::consume_rate(
            &self.runtime,
            RateSubject::PreauthIp(ip.parse().expect("ip literal"), 64),
            "mcp.read",
            "default",
            RatePolicy::new(1, 1).expect("policy"),
            LOCK_TIMEOUT,
        ))
    }

    fn ip_buckets(&mut self) -> Vec<String> {
        self.admin
            .query(
                "SELECT subject_id FROM control.rate_buckets WHERE subject_kind = 'ip' ORDER BY subject_id",
                &[],
            )
            .expect("ip buckets")
            .iter()
            .map(|r| r.get(0))
            .collect()
    }
}

/// T-S1 (SEC-6, §73.2): two addresses of one IPv6 /64 are one client and drain one pre-auth bucket. Fault: key by
/// `ip.to_string()` ⇒ the second address gets its own full bucket ⇒ red.
#[test]
fn two_ipv6_addresses_in_one_64_share_a_preauth_bucket() {
    run_db_fixture::<PreauthFixture, _>(
        "two_ipv6_addresses_in_one_64_share_a_preauth_bucket",
        |mut h| {
            assert_eq!(h.consume("2001:db8:1:2::1"), Ok(()));
            assert_eq!(
                h.consume("2001:db8:1:2:ffff:ffff:ffff:ffff"),
                Err(ErrorCode::RateLimited)
            );
            assert_eq!(h.ip_buckets(), ["2001:db8:1:2::/64"]);
        },
    );
}

/// T-S2 (SEC-6): neighbouring /64s inside one /48 are different clients. Fault: mask /48 ⇒ they share ⇒ red.
#[test]
fn ipv6_addresses_in_different_64s_do_not_share() {
    run_db_fixture::<PreauthFixture, _>("ipv6_addresses_in_different_64s_do_not_share", |mut h| {
        assert_eq!(h.consume("2001:db8:1:2::1"), Ok(()));
        assert_eq!(h.consume("2001:db8:1:3::1"), Ok(()));
        assert_eq!(h.ip_buckets(), ["2001:db8:1:2::/64", "2001:db8:1:3::/64"]);
    });
}

/// T-S3 (SEC-6, §73.2 canonicalisation): IPv4 keys by its full address, and an IPv4-mapped IPv6 address is that
/// IPv4 client. Fault: mask v4 to /24 ⇒ `.10` shares `.9`'s bucket ⇒ red.
#[test]
fn ipv4_keeps_its_full_address_key() {
    run_db_fixture::<PreauthFixture, _>("ipv4_keeps_its_full_address_key", |mut h| {
        assert_eq!(h.consume("203.0.113.9"), Ok(()));
        assert_eq!(h.consume("203.0.113.10"), Ok(()));
        assert_eq!(h.consume("::ffff:203.0.113.9"), Err(ErrorCode::RateLimited));
        assert_eq!(h.ip_buckets(), ["203.0.113.10", "203.0.113.9"]);
    });
}

/// Fields drop in order: the pool and the owner connection close before `_db` drops the database.
struct BatchHandle {
    rt: tokio::runtime::Runtime,
    runtime: Arc<RuntimeDbPool>,
    admin: Client,
    owner_dsn: String,
    auth: AuthorizationScope,
    auth2: AuthorizationScope,
    _db: throwaway_db::ThrowawayDb,
}

/// ADR-0065 D-D: one throwaway `humaux_thread_c38_rate_*` database per test with the two fixture tenants — the
/// lock-wait and deadlock experiments never touch the shared request-guard database.
struct BatchFixture;

impl DbIntegrationFixture for BatchFixture {
    type Handle = BatchHandle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let setup = |e: String| DbFixtureSkipReason::IsolationSetupFailed(e);
        let db = throwaway_db::create("c38_rate")?;
        let owner_dsn = db.dsn();
        // dep: PostgreSQL(any) — owner connection: tenant rows, bucket reads and pg_locks of this throwaway database
        let mut admin = Client::connect(&owner_dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
        admin
            .execute(
                "INSERT INTO control.tenants(tenant_id,name,state) VALUES($1,'c38 batch','ACTIVE'),($2,'c38 batch two','ACTIVE')",
                &[&TENANT_ID, &TENANT2_ID],
            )
            .map_err(|e| setup(e.to_string()))?;
        let rt = tokio::runtime::Runtime::new().map_err(|e| setup(e.to_string()))?;
        let sep = if owner_dsn.contains('?') { '&' } else { '?' };
        // dep: PostgreSQL(role_gateway) — the batch rate consumer
        let runtime = rt
            .block_on(RuntimeDbPool::connect(&format!(
                "{owner_dsn}{sep}options=-c%20role%3Drole_gateway"
            )))
            .map_err(|e| setup(e.to_string()))?;
        let scope = |tenant| {
            BoundedSet::new([]).map(|set| {
                AuthorizationScope::new(
                    TenantId(tenant),
                    PrincipalId(PRINCIPAL_ID),
                    Some(UserId(USER_ID)),
                    set,
                )
            })
        };
        Ok(BatchHandle {
            rt,
            runtime: Arc::new(runtime),
            admin,
            owner_dsn,
            auth: scope(TENANT_ID).map_err(|e| setup(format!("{e:?}")))?,
            auth2: scope(TENANT2_ID).map_err(|e| setup(format!("{e:?}")))?,
            _db: db,
        })
    }
}

/// The guard's post-auth shape: credential/shared, user, tenant, credential/operation (`mcp.read`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Charge {
    Credential,
    User,
    Tenant,
    Operation,
}

fn charge(auth: &AuthorizationScope, which: Charge, policy: RatePolicy) -> RateCharge<'_> {
    let credential = RateSubject::Credential {
        auth,
        credential_id: auth.principal().0,
    };
    let shared = quota_repo::SHARED_RATE_OPERATION;
    let (subject, operation, bucket_key) = match which {
        Charge::Credential => (credential, shared, "credential"),
        Charge::User => (RateSubject::User(auth), shared, "user"),
        Charge::Tenant => (RateSubject::Tenant(auth), shared, "tenant"),
        Charge::Operation => (credential, "mcp.read", "operation"),
    };
    RateCharge {
        subject,
        operation,
        bucket_key,
        policy,
    }
}

/// The advisory-lock key `consume_rate_batch` takes for a tenant-scope or user-scope shared bucket.
fn lock_key(auth: &AuthorizationScope, which: Charge) -> String {
    let tenant = auth.tenant_id().0;
    let user = auth.user_id().expect("fixture auth carries a user").0;
    match which {
        Charge::Tenant => format!("rate:{tenant}:tenant:{tenant}:mcp:tenant"),
        Charge::User => format!("rate:{tenant}:user:{user}:mcp:user"),
        other => unreachable!("no holder for {other:?}"),
    }
}

impl BatchHandle {
    /// A separate session holding `key`'s advisory lock until `release` fires (or `hold` elapses).
    fn hold(
        &self,
        key: String,
        release: std::sync::mpsc::Receiver<()>,
        hold: Duration,
    ) -> std::thread::JoinHandle<()> {
        let dsn = self.owner_dsn.clone();
        let (held_tx, held_rx) = std::sync::mpsc::channel::<()>();
        let holder = std::thread::spawn(move || {
            // dep: PostgreSQL(any) — the holder session of this throwaway database
            let mut client = Client::connect(&dsn, NoTls).expect("holder connects");
            let mut txn = client.transaction().expect("holder txn");
            txn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                &[&key],
            )
            .expect("hold the bucket lock");
            held_tx.send(()).expect("signal the lock is held");
            let _ = release.recv_timeout(hold);
            txn.commit().expect("release the bucket lock");
        });
        held_rx.recv().expect("holder took the lock");
        holder
    }

    /// Advisory-lock requests of this database still waiting.
    fn waiting(&mut self) -> i64 {
        self.admin
            .query_one(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted \
                 AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
                &[],
            )
            .expect("read pg_locks")
            .get(0)
    }

    fn await_waiting(&mut self, n: i64) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.waiting() < n {
            assert!(Instant::now() < deadline, "{n} lock waiters never queued");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn batch(
        &self,
        auth: &AuthorizationScope,
        input: &[Charge],
        policy: RatePolicy,
        lock_timeout: Duration,
    ) -> tokio::task::JoinHandle<Result<(), (ErrorCode, usize)>> {
        let (pool, auth, input) = (Arc::clone(&self.runtime), auth.clone(), input.to_vec());
        self.rt.spawn(async move {
            let charges: Vec<_> = input.iter().map(|&c| charge(&auth, c, policy)).collect();
            quota_repo::consume_rate_batch(&pool, &charges, lock_timeout).await
        })
    }

    /// Tokens spent per non-empty `(kind, operation, bucket_key)` of `tenant`; an absent row spent nothing.
    fn spent(&mut self, tenant: Uuid) -> Vec<(String, String, String, i64)> {
        self.admin
            .query(
                "SELECT subject_kind, operation, bucket_key, round(capacity - tokens)::bigint FROM control.rate_buckets \
                 WHERE tenant_id = $1 AND round(capacity - tokens) > 0 ORDER BY 1, 2, 3",
                &[&tenant],
            )
            .expect("read buckets")
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
            .collect()
    }
}

/// ADR-0065 D-D (W-5): two batches with opposite input orders and a third session queued ahead of them. Holder H
/// takes the user lock; B (user, tenant) queues; A (tenant, user) queues; H commits; both batches finish Ok inside
/// 3 s. Fault: the sort in `lock_order` removed ⇒ B holds user and waits tenant while A holds tenant and waits
/// user ⇒ 40P01 after deadlock_timeout (or 55P03) ⇒ DependencyUnavailable ⇒ red.
#[test]
fn c38_reversed_batches_with_a_queued_waiter_do_not_deadlock() {
    run_db_fixture::<BatchFixture, _>(
        "c38_reversed_batches_with_a_queued_waiter_do_not_deadlock",
        |mut h| {
            let policy = RatePolicy::new(100, 100).expect("policy");
            let (release, released) = std::sync::mpsc::channel::<()>();
            let holder = h.hold(
                lock_key(&h.auth, Charge::User),
                released,
                Duration::from_secs(10),
            );
            let b = h.batch(
                &h.auth,
                &[Charge::User, Charge::Tenant],
                policy,
                Duration::from_secs(2),
            );
            h.await_waiting(1);
            let a = h.batch(
                &h.auth,
                &[Charge::Tenant, Charge::User],
                policy,
                Duration::from_secs(2),
            );
            h.await_waiting(2);
            release.send(()).expect("release the holder");
            holder.join().expect("holder thread");
            let results = h.rt.block_on(async {
                tokio::time::timeout(Duration::from_secs(3), async {
                    (
                        b.await.expect("batch B task"),
                        a.await.expect("batch A task"),
                    )
                })
                .await
                .expect("both batches finish within 3 s")
            });
            assert_eq!(
                results,
                (Ok(()), Ok(())),
                "opposite input orders never deadlock"
            );
        },
    );
}

/// ADR-0065 D-D parity: a denial at input index 2 (the tenant bucket) spends exactly what four sequential
/// transactions in input order spent — credential and user taken, tenant refused, operation untouched. Fault: the
/// batch evaluates in tier order (tenant first) ⇒ credential and user are not spent ⇒ red.
#[test]
fn c38_batch_denial_spends_exactly_what_the_sequential_path_spent() {
    run_db_fixture::<BatchFixture, _>(
        "c38_batch_denial_spends_exactly_what_the_sequential_path_spent",
        |mut h| {
            let roomy = RatePolicy::new(5, 1).expect("policy");
            let single = RatePolicy::new(1, 1).expect("policy");
            let policy_of = |c| if c == Charge::Tenant { single } else { roomy };
            let input = [
                Charge::Credential,
                Charge::User,
                Charge::Tenant,
                Charge::Operation,
            ];
            for auth in [&h.auth, &h.auth2] {
                let drain = charge(auth, Charge::Tenant, single);
                assert_eq!(
                    h.rt.block_on(quota_repo::consume_rate_batch(
                        &h.runtime,
                        &[drain],
                        LOCK_TIMEOUT
                    )),
                    Ok(()),
                    "drain the tenant bucket"
                );
            }
            // The sequential path: one transaction per bucket, stopping at the first denial.
            let mut sequential = Ok(());
            for (i, &c) in input.iter().enumerate() {
                let one = charge(&h.auth, c, policy_of(c));
                let r = h.rt.block_on(quota_repo::consume_rate(
                    &h.runtime,
                    one.subject,
                    one.operation,
                    one.bucket_key,
                    one.policy,
                    LOCK_TIMEOUT,
                ));
                if let Err(code) = r {
                    sequential = Err((code, i));
                    break;
                }
            }
            let charges: Vec<_> = input
                .iter()
                .map(|&c| charge(&h.auth2, c, policy_of(c)))
                .collect();
            let batch = h.rt.block_on(quota_repo::consume_rate_batch(
                &h.runtime,
                &charges,
                LOCK_TIMEOUT,
            ));
            assert_eq!(sequential, Err((ErrorCode::RateLimited, 2)));
            assert_eq!(batch, sequential, "the same verdict at the same index");
            let (seq, bat) = (h.spent(TENANT_ID), h.spent(TENANT2_ID));
            assert_eq!(
                bat, seq,
                "the batch spends exactly what the sequential path spent"
            );
            assert_eq!(
                seq.iter().map(|r| (r.0.as_str(), r.3)).collect::<Vec<_>>(),
                [("credential", 1), ("tenant", 1), ("user", 1)]
            );
        },
    );
}

/// ADR-0065 D-D (W-4): a rate-lock wait past the registered lock_timeout is 55P03, answered DependencyUnavailable
/// (503) — never RATE_LIMITED, never Conflict — and well before the holder lets go. Fault: the 55P03 arm of
/// `rate_db_error` removed ⇒ `db_error` ⇒ Conflict ⇒ red.
#[test]
fn c38_a_rate_lock_wait_past_lock_timeout_is_dependency_unavailable() {
    run_db_fixture::<BatchFixture, _>(
        "c38_a_rate_lock_wait_past_lock_timeout_is_dependency_unavailable",
        |h| {
            let policy = RatePolicy::new(100, 100).expect("policy");
            let (_release, released) = std::sync::mpsc::channel::<()>();
            let holder = h.hold(
                lock_key(&h.auth, Charge::Tenant),
                released,
                Duration::from_millis(400),
            );
            let started = Instant::now();
            let result = h.rt.block_on(h.batch(
                &h.auth,
                &[Charge::Credential, Charge::User, Charge::Tenant],
                policy,
                Duration::from_millis(100),
            ));
            let waited = started.elapsed();
            holder.join().expect("holder thread");
            assert_eq!(
                result.expect("batch task"),
                Err((ErrorCode::DependencyUnavailable, 2)),
                "a lock_timeout is the store being unavailable"
            );
            assert!(
                waited < Duration::from_millis(400),
                "lock_timeout ended the wait before the holder did: {waited:?}"
            );
        },
    );
}

/// ADR-0065 D-B / D-D: a rate-lock wait ended by the session's statement_timeout is SQLSTATE 57014, answered
/// DependencyUnavailable (503) — never Internal. The pool is the gateway's production shape (`connect_with`,
/// statement_timeout 150 ms as a startup option) and lock_timeout (5 s) is far above it, so the cancel is the
/// statement timeout's. Fault: 57014 dropped from `rate_db_error`'s arm ⇒ `db_error` ⇒ Internal ⇒ red.
#[test]
fn c38_a_rate_statement_timeout_is_dependency_unavailable() {
    run_db_fixture::<BatchFixture, _>(
        "c38_a_rate_statement_timeout_is_dependency_unavailable",
        |h| {
            let policy = RatePolicy::new(100, 100).expect("policy");
            let settings = PoolSettings {
                max_connections: 2,
                min_connections: 0,
                acquire_timeout: Duration::from_secs(5),
                idle_timeout: Duration::from_secs(60),
                max_lifetime: Duration::from_secs(600),
                statement_timeout: Duration::from_millis(150),
                idle_in_transaction_timeout: Duration::from_secs(5),
                application_name: "c38_rate_57014".to_owned(),
            };
            let sep = if h.owner_dsn.contains('?') { '&' } else { '?' };
            // dep: PostgreSQL(role_gateway) — the gateway's sized pool shape on this throwaway database
            let pool =
                h.rt.block_on(RuntimeDbPool::connect_with(
                    &format!("{}{sep}options=-c%20role%3Drole_gateway", h.owner_dsn),
                    &settings,
                ))
                .expect("sized role_gateway pool");
            let (_release, released) = std::sync::mpsc::channel::<()>();
            let holder = h.hold(
                lock_key(&h.auth, Charge::Tenant),
                released,
                Duration::from_secs(2),
            );
            let charges = [
                charge(&h.auth, Charge::Credential, policy),
                charge(&h.auth, Charge::Tenant, policy),
            ];
            let started = Instant::now();
            let result = h.rt.block_on(quota_repo::consume_rate_batch(
                &pool,
                &charges,
                Duration::from_secs(5),
            ));
            let waited = started.elapsed();
            drop(pool);
            holder.join().expect("holder thread");
            assert_eq!(
                result,
                Err((ErrorCode::DependencyUnavailable, 1)),
                "a statement_timeout is the store being unavailable"
            );
            assert!(
                waited < Duration::from_secs(2),
                "statement_timeout ended the wait before the holder did: {waited:?}"
            );
        },
    );
}

/// ADR-0065 D-D (W-5): a wait cycle the server's deadlock detector breaks is SQLSTATE 40P01, answered
/// DependencyUnavailable (503) — never Conflict. Holder H takes the user lock; the batch (user, tenant) takes the
/// tenant lock and queues on the user lock; 200 ms later H asks for the tenant lock, closing the cycle. The batch
/// waited first, so its deadlock_timeout fires first and it is the victim; H then gets the tenant lock. Fault: 40P01
/// dropped from `rate_db_error`'s arm ⇒ `db_error` ⇒ Conflict ⇒ red.
#[test]
fn c38_a_rate_deadlock_victim_is_dependency_unavailable() {
    run_db_fixture::<BatchFixture, _>(
        "c38_a_rate_deadlock_victim_is_dependency_unavailable",
        |mut h| {
            let policy = RatePolicy::new(100, 100).expect("policy");
            let dsn = h.owner_dsn.clone();
            let (user_key, tenant_key) = (
                lock_key(&h.auth, Charge::User),
                lock_key(&h.auth, Charge::Tenant),
            );
            let (held_tx, held_rx) = std::sync::mpsc::channel::<()>();
            let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
            let holder = std::thread::spawn(move || -> Result<(), String> {
                // dep: PostgreSQL(any) — the holder session of this throwaway database
                let mut client = Client::connect(&dsn, NoTls).map_err(|e| e.to_string())?;
                let mut txn = client.transaction().map_err(|e| e.to_string())?;
                let lock = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))";
                txn.execute(lock, &[&user_key]).map_err(|e| e.to_string())?;
                held_tx.send(()).map_err(|e| e.to_string())?;
                go_rx
                    .recv_timeout(Duration::from_secs(10))
                    .map_err(|e| e.to_string())?;
                txn.execute(lock, &[&tenant_key])
                    .map_err(|e| format!("the holder lost the cycle: {e}"))?;
                txn.commit().map_err(|e| e.to_string())
            });
            held_rx.recv().expect("holder took the user lock");
            let batch = h.batch(
                &h.auth,
                &[Charge::User, Charge::Tenant],
                policy,
                Duration::from_secs(5),
            );
            h.await_waiting(1);
            std::thread::sleep(Duration::from_millis(200));
            go_tx.send(()).expect("close the cycle");
            let result = h.rt.block_on(async {
                tokio::time::timeout(Duration::from_secs(5), batch)
                    .await
                    .expect("the detector ends the cycle within 5 s")
                    .expect("batch task")
            });
            let holder = holder.join().expect("holder thread");
            assert_eq!(
                result,
                Err((ErrorCode::DependencyUnavailable, 0)),
                "a deadlock victim is the store being unavailable"
            );
            assert_eq!(
                holder,
                Ok(()),
                "the holder got the tenant lock after the abort"
            );
        },
    );
}
