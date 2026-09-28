//! `retrieval-provider::tests::router_rls_integration` — T7.2 DB integration test — proves
//!   `migrations/0088_retrieval_provider_routes.sql`'s nullable-`tenant_id` RLS design against a *real*
//!   `control.retrieval_provider_routes` table (not a synthetic scratch schema, unlike
//!   `crates/adapters/tests/auth_scope_rls.rs`'s own fixture — this task's design decision is specifically about this
//!   table's real policy and FK-driven column set, so the test exercises the table itself).
//! Depends-on: crates=[humaux-testkit, postgres, uuid]; services=[PostgreSQL(owner)
//!   w=[control.retrieval_provider_routes, control.tenants], PostgreSQL(role_gateway)]; env=[HUMAUX_TEST_PG_DSN];
//!   modules=[humaux-testkit, retrieval-provider::router]
//! Called-by: [cargo-test]
//! Invariants: [a tenant route is visible only to its tenant, a NULL-tenant default to every tenant, and role_gateway
//!   can SELECT but never INSERT; seeding uses the owner connection only]
//! Spec: §6.2.0; §6.2.1; §79.2
//!
//! - A tenant-specific row is visible only to its own tenant, never a different one
//!   (cross-tenant isolation — the migration's stated leak concern).
//! - A platform-wide default row (`tenant_id IS NULL`) is visible to *every* tenant
//!   (migration header: "策略允许 NULL 表示全局默认").
//! - `role_gateway` — a real, frozen (§6.2.0) runtime role, not a synthetic test role — can
//!   SELECT under `control`'s §6.2.1 domain default, but has no INSERT grant at all: seeding
//!   is done via the raw superuser connection (mirrors how a migration/admin path would seed
//!   platform config in production, per the migration header's "只读" reasoning), and this
//!   test additionally asserts `role_gateway` really cannot INSERT.
//!
//! Three-state skip contract (`humaux_testkit::DbFixtureSkipReason`, §79.2): no
//! `HUMAUX_TEST_PG_DSN` ⇒ visible SKIP, never a silent pass.
//!
//! Teardown deletes only the specific rows this test inserts (by their own freshly-minted
//! ids) — the real `control.tenants`/`control.retrieval_provider_routes` tables are never
//! truncated or otherwise bulk-modified (CLAUDE.md 硬边界④/repo `CLAUDE.md`).

use humaux_retrieval_provider::router::RoutePurpose;
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use uuid::Uuid;

struct RlsHandle {
    client: Client,
    tenant_a: Uuid,
    tenant_b: Uuid,
    route_tenant_a: Uuid,
    route_tenant_b: Uuid,
    route_global: Uuid,
}

impl Drop for RlsHandle {
    fn drop(&mut self) {
        // Best-effort, id-scoped cleanup — never a schema-wide DELETE/TRUNCATE against the
        // real `control` schema.
        // dep: PostgreSQL(owner) — admin fixture insert for the RLS integration test.
        let _ = self.client.execute(
            "DELETE FROM control.retrieval_provider_routes WHERE route_id = ANY($1)",
            &[&vec![
                self.route_tenant_a,
                self.route_tenant_b,
                self.route_global,
            ]],
        );
        // dep: PostgreSQL(owner) — admin fixture insert for the RLS integration test.
        let _ = self.client.execute(
            "DELETE FROM control.tenants WHERE tenant_id = ANY($1)",
            &[&vec![self.tenant_a, self.tenant_b]],
        );
    }
}

struct RlsFixture;

impl DbIntegrationFixture for RlsFixture {
    type Handle = RlsHandle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        // dep: PostgreSQL(owner) — connects with the exact HUMAUX_TEST_PG_DSN guard this fixture requires.
        let mut client = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        let tenant_row = |client: &mut Client, name: &str| -> Result<Uuid, DbFixtureSkipReason> {
            client
                .query_one(
                    "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                    &[&name],
                )
                .map(|r| r.get(0))
                .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))
        };
        let tenant_a = tenant_row(&mut client, "T7.2 rls test tenant A")?;
        let tenant_b = tenant_row(&mut client, "T7.2 rls test tenant B")?;

        let insert_route = |client: &mut Client,
                            tenant_id: Option<Uuid>,
                            model: &str|
         -> Result<Uuid, DbFixtureSkipReason> {
            client
                .query_one(
                    "INSERT INTO control.retrieval_provider_routes
                       (tenant_id, purpose, embedding_provider_id, embedding_model_id, \
                        embedding_dimension, priority, enabled)
                     VALUES ($1, 'RETRIEVAL_EMBEDDING', 'dashscope', $2, 1024, 0, true)
                     RETURNING route_id",
                    &[&tenant_id, &model],
                )
                .map(|r| r.get(0))
                .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))
        };
        let route_tenant_a = insert_route(&mut client, Some(tenant_a), "model-for-a")?;
        let route_tenant_b = insert_route(&mut client, Some(tenant_b), "model-for-b")?;
        let route_global = insert_route(&mut client, None, "model-global-default")?;

        Ok(RlsHandle {
            client,
            tenant_a,
            tenant_b,
            route_tenant_a,
            route_tenant_b,
            route_global,
        })
    }
}

/// `route_id`s visible to `tenant_id` under `role_gateway` — the real runtime role, real
/// `control` schema §6.2.1 domain-default SELECT grant, real RLS policy from migration 0088.
fn visible_route_ids(client: &mut Client, tenant_id: Uuid) -> Vec<Uuid> {
    let mut txn = client.transaction().expect("begin read transaction");
    // dep: PostgreSQL(role_gateway) — role switch before the scoped statements for `visible_route_ids`
    txn.batch_execute(&format!(
        "SET LOCAL ROLE role_gateway;
         SET LOCAL humaux.tenant_id = '{tenant_id}';"
    ))
    .expect("set local role/tenant context");
    // No route_id filter — RLS itself is what's under test, not an application-side WHERE.
    let rows = txn
        .query(
            "SELECT route_id FROM control.retrieval_provider_routes",
            &[],
        )
        .expect("select under role_gateway + tenant context");
    let ids = rows.iter().map(|r| r.get(0)).collect();
    txn.rollback().expect("rollback read-only transaction");
    ids
}

#[test]
fn tenant_specific_route_is_isolated_and_global_default_is_shared() {
    run_db_fixture::<RlsFixture, _>(
        "tenant_specific_route_is_isolated_and_global_default_is_shared",
        |mut handle| {
            let seen_by_a = visible_route_ids(&mut handle.client, handle.tenant_a);
            assert!(
                seen_by_a.contains(&handle.route_tenant_a),
                "tenant A must see its own tenant-specific route"
            );
            assert!(
                !seen_by_a.contains(&handle.route_tenant_b),
                "§6.1.1/migrations 0088: tenant A must NOT see tenant B's route — cross-tenant leak"
            );
            assert!(
                seen_by_a.contains(&handle.route_global),
                "migrations 0088 header: a NULL-tenant row is a platform-wide default, visible \
                 to every tenant"
            );

            let seen_by_b = visible_route_ids(&mut handle.client, handle.tenant_b);
            assert!(seen_by_b.contains(&handle.route_tenant_b));
            assert!(
                !seen_by_b.contains(&handle.route_tenant_a),
                "§6.1.1/migrations 0088: tenant B must NOT see tenant A's route — cross-tenant leak"
            );
            assert!(seen_by_b.contains(&handle.route_global));
        },
    );
}

#[test]
fn role_gateway_has_no_write_grant_on_control_retrieval_provider_routes() {
    run_db_fixture::<RlsFixture, _>(
        "role_gateway_has_no_write_grant_on_control_retrieval_provider_routes",
        |mut handle| {
            let mut txn = handle.client.transaction().expect("begin transaction");
            // dep: PostgreSQL(role_gateway) — role switch before the scoped statements for `role_gateway_has_no_write_grant_on_control_retrieval_provider_routes`
            txn.batch_execute(&format!(
                "SET LOCAL ROLE role_gateway;
                 SET LOCAL humaux.tenant_id = '{}';",
                handle.tenant_a
            ))
            .expect("set local role/tenant context");
            // dep: PostgreSQL(owner) — real control.retrieval_provider_routes write inside the open transaction, proving the migration's RLS policy.
            let result = txn.execute(
                "INSERT INTO control.retrieval_provider_routes
                   (tenant_id, purpose, embedding_provider_id, embedding_model_id)
                 VALUES ($1, 'RETRIEVAL_EMBEDDING', 'dashscope', 'attempted-write')",
                &[&handle.tenant_a],
            );
            assert!(
                result.is_err(),
                "migrations 0088 header: §6.2.1 control-schema domain default grants no \
                 runtime role INSERT — this table is migration/admin-managed only"
            );
            txn.rollback().expect("rollback failed insert attempt");
        },
    );
}

/// Code-review finding fix: `router.rs::RoutePurpose::as_db_str()` claims to match
/// migrations/0088's `purpose` CHECK constraint, but nothing reconciled the two — mirrors
/// `crates/adapters/tests/disclosure_ledger.rs::wire_strings_match_live_db_check_constraints`,
/// which pulls the live CHECK text via `pg_get_constraintdef` instead of trusting the Rust
/// enum's doc comment. A future migration that respells the CHECK now fails this test instead
/// of silently desyncing from `RoutePurpose`.
#[test]
fn route_purpose_wire_strings_match_live_db_check_constraint() {
    run_db_fixture::<RlsFixture, _>(
        "route_purpose_wire_strings_match_live_db_check_constraint",
        |mut handle| {
            let mut expected = vec![
                RoutePurpose::Embedding.as_db_str(),
                RoutePurpose::Rerank.as_db_str(),
            ];
            expected.sort_unstable();

            let def: String = handle
                .client
                .query_one(
                    "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
                     WHERE conrelid = to_regclass('control.retrieval_provider_routes') \
                       AND conname = 'retrieval_provider_routes_purpose_check'",
                    &[],
                )
                .expect("retrieval_provider_routes_purpose_check must exist")
                .get(0);
            // Quoted literals sit at the odd positions of a split on `'` (the definition
            // always opens with non-literal SQL text before the first literal) — same
            // extraction disclosure_ledger.rs's own contract test uses.
            let mut actual: Vec<&str> = def.split('\'').skip(1).step_by(2).collect();
            actual.sort_unstable();
            assert_eq!(
                actual, expected,
                "retrieval_provider_routes_purpose_check literals drifted from \
                 RoutePurpose::as_db_str() — db def: {def}"
            );
        },
    );
}
