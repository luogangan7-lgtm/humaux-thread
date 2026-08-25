//! T1.5 integration test — §6.1/§62 RLS defense in depth: a request transaction sets
//! `SET LOCAL humaux.tenant_id`/`humaux.user_id`, and PostgreSQL Row Level Security enforces
//! tenant isolation independently of the application-layer `can_read` predicate
//! (`humaux_domain::identity::can_read`, unit-tested in that crate).
//!
//! Self-contained fixture: builds its own scratch schema, table, non-superuser test role,
//! and the exact §62 policy shape, instead of depending on canonical DDL from the sibling
//! T1.1 task (which may not have landed yet in a parallel wave) — this test proves the RLS
//! *mechanism* frozen by §6.1/§62, not T1.1's specific table layout. Everything it creates
//! lives under `test_auth_scope_rls`/`test_auth_scope_rls_role`, dropped up front and again
//! on teardown — the dev DB's real `private` schema is never touched (repo `CLAUDE.md` hard
//! rule ④).
//!
//! Three-state skip contract (`humaux_testkit::DbFixtureSkipReason`, §79.2): no
//! `HUMAUX_TEST_PG_DSN` set, DSN set but unreachable, or fixture DDL failed (e.g. the
//! connecting role lacks `CREATEROLE`/`CREATE SCHEMA`) — each prints a visible `SKIP` with
//! its reason and the test returns, it never silently passes.
//!
//! The RLS check requires a *non-superuser* session: PostgreSQL superusers and
//! `BYPASSRLS` roles bypass row security unconditionally, `FORCE ROW LEVEL SECURITY`
//! included, so a raw superuser connection could not prove anything here. The fixture
//! connects with whatever role `HUMAUX_TEST_PG_DSN` names (dev default: `postgres`,
//! superuser) to create objects, then every assertion runs after `SET LOCAL ROLE` into a
//! freshly created non-superuser/non-`BYPASSRLS` role — `SET LOCAL ROLE` changes
//! `current_user` for RLS purposes without a second login.

use humaux_domain::ids::TenantId;
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};

const SCHEMA: &str = "test_auth_scope_rls";
const ROLE: &str = "test_auth_scope_rls_role";

struct RlsHandle {
    client: Client,
}

impl Drop for RlsHandle {
    fn drop(&mut self) {
        // Best-effort teardown so a passing run leaves the dev DB clean; a failed run's
        // leftovers are removed by the next run's `DROP ... IF EXISTS` in `isolate()`
        // regardless, so an error here is not itself a test failure.
        let _ = self.client.batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {SCHEMA} CASCADE; DROP ROLE IF EXISTS {ROLE};"
        ));
    }
}

struct RlsFixture;

impl DbIntegrationFixture for RlsFixture {
    type Handle = RlsHandle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut client = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        // §62 policy shape verbatim: ENABLE + FORCE ROW LEVEL SECURITY, USING/WITH CHECK
        // both keyed on `current_setting('humaux.tenant_id', true)::uuid`. `NOLOGIN` is
        // sufficient for the test role — assertions reach it via `SET LOCAL ROLE`, never a
        // second network login.
        client
            .batch_execute(&format!(
                "DROP SCHEMA IF EXISTS {SCHEMA} CASCADE;
                 DROP ROLE IF EXISTS {ROLE};
                 CREATE ROLE {ROLE} NOLOGIN NOBYPASSRLS;
                 CREATE SCHEMA {SCHEMA};
                 CREATE TABLE {SCHEMA}.rows (
                     id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                     tenant_id uuid NOT NULL,
                     body text NOT NULL
                 );
                 ALTER TABLE {SCHEMA}.rows ENABLE ROW LEVEL SECURITY;
                 ALTER TABLE {SCHEMA}.rows FORCE ROW LEVEL SECURITY;
                 CREATE POLICY tenant_isolation ON {SCHEMA}.rows
                 USING (tenant_id = nullif(current_setting('humaux.tenant_id', true), '')::uuid)
                 WITH CHECK (tenant_id = nullif(current_setting('humaux.tenant_id', true), '')::uuid);
                 GRANT USAGE ON SCHEMA {SCHEMA} TO {ROLE};
                 GRANT SELECT, INSERT ON {SCHEMA}.rows TO {ROLE};"
            ))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        Ok(RlsHandle { client })
    }
}

#[test]
fn rls_tenant_context_via_set_local() {
    run_db_fixture::<RlsFixture, _>("rls_tenant_context_via_set_local", |mut handle| {
        // `.0`: `postgres::ToSql`/text interpolation need the inner `uuid::Uuid` — this
        // test's own concern, not something `TenantId` itself should grow (`ids.rs` is
        // outside this task's assigned files, see identity.rs module doc).
        let tenant_a = TenantId::new().0;
        let tenant_b = TenantId::new().0;

        // Seed one row for tenant A, as the non-superuser role, under its own tenant
        // context — proves "正确 tenant 可读写自己的行" also covers the write side.
        {
            let mut txn = handle.client.transaction().expect("begin seed transaction");
            txn.batch_execute(&format!(
                "SET LOCAL ROLE {ROLE};
                 SET LOCAL humaux.tenant_id = '{tenant_a}';
                 SET LOCAL humaux.user_id = '{tenant_a}';"
            ))
            .expect("set local tenant/user context for seed insert");
            let inserted = txn
                .execute(
                    &format!("INSERT INTO {SCHEMA}.rows (tenant_id, body) VALUES ($1, 'a-row')"),
                    &[&tenant_a],
                )
                .expect("insert own-tenant row must succeed under matching SET LOCAL context");
            assert_eq!(inserted, 1, "exactly one row inserted for tenant A");
            txn.commit().expect("commit seed transaction");
        }

        // Cross-tenant SELECT: SET LOCAL to tenant B, must see 0 of tenant A's rows.
        {
            let mut txn = handle
                .client
                .transaction()
                .expect("begin cross-tenant transaction");
            txn.batch_execute(&format!(
                "SET LOCAL ROLE {ROLE};
                 SET LOCAL humaux.tenant_id = '{tenant_b}';"
            ))
            .expect("set local tenant context for cross-tenant read");
            let row = txn
                .query_one(&format!("SELECT count(*) FROM {SCHEMA}.rows"), &[])
                .expect("count query");
            let count: i64 = row.get(0);
            assert_eq!(
                count, 0,
                "§6.1.1: tenant B's session must see 0 rows of tenant A's data"
            );
            txn.rollback().expect("rollback read-only transaction");
        }

        // No SET LOCAL at all — deliberately run *after* two transactions that already used
        // `SET LOCAL humaux.tenant_id` on this same pooled connection. PostgreSQL's
        // placeholder GUCs (an undeclared `namespace.name` like `humaux.tenant_id`, §6.1)
        // reset to `''` — not NULL — once a session-local `SET LOCAL` has ever targeted them
        // and the transaction ends; only a session that *never* touched the GUC sees NULL
        // from `current_setting(..., true)`. A real gateway reuses pooled connections across
        // requests (§6.2.3), so "forgot to SET LOCAL" on request N+1 hits the `''` case, not
        // the pristine-session NULL case — verified by running this scenario third, not
        // first. Plain `current_setting(...)::uuid` (§62's literal example) would then throw
        // `invalid input syntax for type uuid: ""` instead of failing closed to 0 rows: this
        // fixture's policy guards with `nullif(current_setting(...), '')::uuid` so both the
        // pristine-NULL and post-reset-`''` cases collapse to the same NULL comparison, which
        // is NULL (not true) for every row — excluded, 0 rows either way. The canonical §62
        // policy T1.1/T1.3 install on `private.*` must carry the same guard, or the
        // "forgot to SET LOCAL" failure mode is a 500 on a warm connection, not the 0-row
        // fail-closed read this task requires.
        {
            let mut txn = handle
                .client
                .transaction()
                .expect("begin no-context transaction");
            txn.batch_execute(&format!("SET LOCAL ROLE {ROLE};"))
                .expect("set local role only, no tenant context");
            let row = txn
                .query_one(&format!("SELECT count(*) FROM {SCHEMA}.rows"), &[])
                .expect("count query");
            let count: i64 = row.get(0);
            assert_eq!(
                count, 0,
                "§6.1: a transaction that never set humaux.tenant_id must read 0 rows, not everything"
            );
            txn.rollback().expect("rollback read-only transaction");
        }

        // Correct tenant reads its own row back.
        {
            let mut txn = handle
                .client
                .transaction()
                .expect("begin own-tenant read transaction");
            txn.batch_execute(&format!(
                "SET LOCAL ROLE {ROLE};
                 SET LOCAL humaux.tenant_id = '{tenant_a}';"
            ))
            .expect("set local tenant context for own-tenant read");
            let row = txn
                .query_one(
                    &format!("SELECT count(*), max(body) FROM {SCHEMA}.rows"),
                    &[],
                )
                .expect("count query");
            let count: i64 = row.get(0);
            let body: Option<String> = row.get(1);
            assert_eq!(count, 1, "tenant A must read exactly its own seeded row");
            assert_eq!(body.as_deref(), Some("a-row"));
            txn.rollback().expect("rollback read-only transaction");
        }
    });
}
