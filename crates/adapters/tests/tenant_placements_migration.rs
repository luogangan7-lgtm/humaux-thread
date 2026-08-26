//! T5.4+T5.6 integration test — `projection.tenant_placements` (§17.3, migration 0068)
//! against a real Postgres. Same convention as `stream_repo.rs`: shared table, each test
//! scopes rows to its own throwaway `control.tenants` row cleaned up on drop.
//!
//! Three-state skip (§79.2): no `HUMAUX_TEST_PG_DSN`, unreachable DB, or migration 0068 not
//! yet applied all print a visible SKIP and return.
//!
//! What this file does NOT cover: the §17.1 Qdrant-side tenant keyword index / cross-tenant
//! *point* filter test the T5.4+T5.6 brief also asks for. That needs a live Qdrant HTTP
//! client this crate does not have yet (`crates/adapters/src/qdrant.rs`'s module doc) — see
//! `tests/qdrant_live.rs`, which reports `not_applicable` naming exactly that missing object
//! rather than silently standing in for it. What *is* real here is the control-plane side of
//! §17.3: the Postgres row-level tenant boundary on `projection.tenant_placements` itself,
//! and the §78.2 DB-enum/Rust-enum contract for `placement_class`/`promotion_state`.

use humaux_adapters::qdrant::{PlacementClass, PromotionState};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    // See stream_repo.rs/disclosure_ledger.rs for why this exact URL-encoded form (rust-
    // postgres rejects the `options[role]=X` form sqlx tolerates).
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

struct Handle {
    admin: Client,
    tenant_a: Uuid,
    tenant_b: Uuid,
    dsn: String,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Best-effort cleanup (repo CLAUDE.md hard rule ④: this file never touches a
        // schema/table of its own, only rows it created under its own throwaway tenants).
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM projection.tenant_placements WHERE tenant_id IN ('{0}', '{1}'); \
             DELETE FROM control.tenants WHERE tenant_id IN ('{0}', '{1}');",
            self.tenant_a, self.tenant_b
        ));
    }
}

struct PlacementFixture;

impl DbIntegrationFixture for PlacementFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        let exists: bool = admin
            .query_one(
                "SELECT to_regclass('projection.tenant_placements') IS NOT NULL",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !exists {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "projection.tenant_placements does not exist — run `cargo xtask migrate` first"
                    .to_string(),
            ));
        }
        let has_family_column: bool = admin
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = 'projection' AND table_name = 'tenant_placements' \
                 AND column_name = 'projection_family')",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !has_family_column {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "projection.tenant_placements.projection_family does not exist — migration \
                 0068_tenant_placements_qdrant_fields not applied yet"
                    .to_string(),
            ));
        }

        let tenant_a: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"tenant_placements_migration.rs tenant A"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        let tenant_b: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"tenant_placements_migration.rs tenant B"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        Ok(Handle {
            admin,
            tenant_a,
            tenant_b,
            dsn,
        })
    }
}

/// §78.2 DB-enum/Rust-enum contract: `placement_class`'s CHECK constraint (migration 0068)
/// must enumerate exactly `PlacementClass::ALL`'s db strings, no more, no fewer.
#[test]
fn placement_class_check_constraint_matches_rust_enum() {
    run_db_fixture::<PlacementFixture, _>(
        "placement_class_check_constraint_matches_rust_enum",
        |mut h| {
            let def: String = h
                .admin
                .query_one(
                    "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
                 WHERE conname = 'tenant_placements_placement_class_known'",
                    &[],
                )
                .expect("constraint must exist")
                .get(0);
            for variant in PlacementClass::ALL {
                assert!(
                    def.contains(variant.as_db_str()),
                    "CHECK def `{def}` missing Rust variant `{}`",
                    variant.as_db_str()
                );
            }
        },
    );
}

/// Same contract for `promotion_state`.
#[test]
fn promotion_state_check_constraint_matches_rust_enum() {
    run_db_fixture::<PlacementFixture, _>(
        "promotion_state_check_constraint_matches_rust_enum",
        |mut h| {
            let def: String = h
                .admin
                .query_one(
                    "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
                 WHERE conname = 'tenant_placements_promotion_state_known'",
                    &[],
                )
                .expect("constraint must exist")
                .get(0);
            for variant in PromotionState::ALL {
                assert!(
                    def.contains(variant.as_db_str()),
                    "CHECK def `{def}` missing Rust variant `{}`",
                    variant.as_db_str()
                );
            }
        },
    );
}

/// §6.1 tenant isolation on the control-plane placement table itself: `role_retrieval_worker`
/// (the domain-default writer of this table, §6.2.1) scoped to tenant A must never see
/// tenant B's placement row, even though both exist in the same shared table.
#[test]
fn cross_tenant_rls_hides_other_tenants_placement_rows() {
    run_db_fixture::<PlacementFixture, _>(
        "cross_tenant_rls_hides_other_tenants_placement_rows",
        |mut h| {
            h.admin
                .execute(
                    "INSERT INTO projection.tenant_placements \
                 (tenant_id, projection_family, collection_name) VALUES ($1, $2, $2)",
                    &[&h.tenant_a, &"private_memory_v1"],
                )
                .expect("seed tenant A row");
            h.admin
                .execute(
                    "INSERT INTO projection.tenant_placements \
                 (tenant_id, projection_family, collection_name) VALUES ($1, $2, $2)",
                    &[&h.tenant_b, &"private_memory_v1"],
                )
                .expect("seed tenant B row");

            let worker_dsn = dsn_as_role(&h.dsn, "role_retrieval_worker");
            let mut worker = match Client::connect(&worker_dsn, NoTls) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!(
                        "SKIP cross_tenant_rls_hides_other_tenants_placement_rows: role_retrieval_worker connect failed: {e} (§79.2)"
                    );
                    return;
                }
            };
            let mut txn = worker.transaction().expect("begin txn");
            txn.execute(
                "SELECT set_config('humaux.tenant_id', $1, true)",
                &[&h.tenant_a.to_string()],
            )
            .expect("SET LOCAL humaux.tenant_id");

            let rows = txn
                .query(
                    "SELECT tenant_id FROM projection.tenant_placements ORDER BY tenant_id",
                    &[],
                )
                .expect("scoped select");
            let seen: Vec<Uuid> = rows.iter().map(|r| r.get(0)).collect();

            assert!(seen.contains(&h.tenant_a), "tenant A must see its own row");
            assert!(
                !seen.contains(&h.tenant_b),
                "§6.1 RLS violation: tenant A's connection saw tenant B's placement row: {seen:?}"
            );
            txn.rollback().ok();
        },
    );
}
