//! §33.10 rule 9 / card 21 (card 1 review P2, folded) — `control.sweep_confirm_tokens`
//! against a real Postgres, through the one caller-side door
//! (`confirm_token_repo::sweep_expired`) and the maintenance role that owns it.
//!
//! What this pins that the migration manifests cannot: 0169/0170's manifests assert the sweep
//! *exists* with the right owner, `search_path` and EXECUTE set. They never assert what it
//! deletes. The retention predicate — the whole point of the migration — was verified by one
//! hand-run and then by nothing, on a table whose defect was unbounded growth. Four rows in the
//! caller's tenant cover the predicate's whole truth table, and a fifth in a second tenant
//! covers the RLS scoping that keeps a sweep from becoming a cross-tenant erase.
//!
//! Three-state skip (§79.2): no DSN, unreachable DB, or 0169 not applied all print a visible
//! SKIP and return.

use std::time::{Duration, SystemTime};

use humaux_adapters::confirm_token_repo;
use humaux_adapters::postgres::MaintenanceDbPool;
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sha2::{Digest, Sha256};
use sqlx::types::Uuid;

/// The audit-retention window this file sweeps with: a token consumed inside it survives, one
/// consumed before it does not.
const RETENTION: Duration = Duration::from_secs(3600);

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

struct Handle {
    rt: tokio::runtime::Runtime,
    maintenance: MaintenanceDbPool,
    admin: Client,
    tenant_id: Uuid,
    other_tenant_id: Uuid,
    user_id: Uuid,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // ON DELETE CASCADE from control.tenants/users takes the token rows with it; the
        // DELETEs are spelled anyway so a failure to cascade is not silently tolerated.
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM control.confirm_tokens WHERE tenant_id IN ('{0}','{1}'); \
             DELETE FROM control.users WHERE user_id = '{2}'; \
             DELETE FROM control.tenants WHERE tenant_id IN ('{0}','{1}');",
            self.tenant_id, self.other_tenant_id, self.user_id
        ));
    }
}

struct ConfirmTokenFixture;

impl DbIntegrationFixture for ConfirmTokenFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        let swept: bool = admin
            .query_one(
                "SELECT to_regprocedure('control.sweep_confirm_tokens(interval)') IS NOT NULL",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !swept {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "control.sweep_confirm_tokens(interval) does not exist — run \
                 `cargo xtask migrate` (migrations/0169_confirm_token_retention.sql) first"
                    .to_string(),
            ));
        }

        let tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"confirm_token_retention.rs throwaway tenant"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        let other_tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"confirm_token_retention.rs bystander tenant"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        let user_id = Uuid::new_v4();
        admin
            .execute(
                "INSERT INTO control.users (user_id) VALUES ($1)",
                &[&user_id],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let maintenance = rt
            .block_on(MaintenanceDbPool::connect(&dsn_as_role(
                &dsn,
                "role_maintenance",
            )))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        Ok(Handle {
            rt,
            maintenance,
            admin,
            tenant_id,
            other_tenant_id,
            user_id,
        })
    }
}

/// Seeds one token with the exact `(issued_at, expires_at, consumed_at)` the case under test
/// needs. `label` is both the `operation` key and the nonce seed, so every row is identifiable
/// after the sweep without reading any secret material.
fn seed_token(
    handle: &mut Handle,
    tenant_id: Uuid,
    label: &str,
    expires_in: i64,
    consumed_secs_ago: Option<i64>,
) {
    let now = SystemTime::now();
    let issued_at = now - Duration::from_secs(24 * 3600);
    let expires_at = if expires_in >= 0 {
        now + Duration::from_secs(expires_in as u64)
    } else {
        now - Duration::from_secs((-expires_in) as u64)
    };
    let consumed_at = consumed_secs_ago.map(|s| now - Duration::from_secs(s as u64));
    let nonce = Sha256::digest(label.as_bytes()).to_vec();
    let user_id = handle.user_id;
    handle
        .admin
        .execute(
            "INSERT INTO control.confirm_tokens \
               (tenant_id, user_id, operation, target_id, nonce_sha256, issued_at, expires_at, \
                consumed_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            &[
                &tenant_id,
                &user_id,
                &label,
                &Uuid::new_v4(),
                &nonce,
                &issued_at,
                &expires_at,
                &consumed_at,
            ],
        )
        .expect("seed confirm token");
}

/// The `operation` labels still present for one tenant, sorted.
fn surviving(handle: &mut Handle, tenant_id: Uuid) -> Vec<String> {
    handle
        .admin
        .query(
            "SELECT operation FROM control.confirm_tokens WHERE tenant_id = $1 ORDER BY operation",
            &[&tenant_id],
        )
        .expect("read surviving tokens")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect()
}

/// The whole retention predicate in one pass: expired-and-never-consumed goes, expired-and-
/// consumed-long-ago goes, expired-but-consumed-recently STAYS (the §9 audit answer "which
/// confirm token authorized this destructive call" must outlive the call), unexpired STAYS
/// (it can still gate a call), and another tenant's expired row STAYS (FORCE RLS scopes the
/// definer's own DELETE — a sweep is never a cross-tenant erase).
///
/// Fault injection: drop `expires_at < now()` from 0169's predicate and `still.unexpired` goes
/// with it; drop the `consumed_at IS NULL OR ...` clause and the recently-consumed audit row is
/// erased; call the sweep without a tenant context and the count is 0 instead of 2.
#[test]
fn the_sweep_deletes_only_expired_tokens_no_longer_wanted_as_audit() {
    run_db_fixture::<ConfirmTokenFixture, _>(
        "the_sweep_deletes_only_expired_tokens_no_longer_wanted_as_audit",
        |mut handle| {
            let tenant_id = handle.tenant_id;
            let other = handle.other_tenant_id;
            // deletable: expired, never consumed — nothing happened, no audit event to keep.
            seed_token(&mut handle, tenant_id, "gone.unconsumed", -60, None);
            // deletable: expired AND consumed longer ago than the retention window.
            seed_token(&mut handle, tenant_id, "gone.old_consumed", -60, Some(7200));
            // kept: expired but consumed inside the retention window (still audit).
            seed_token(
                &mut handle,
                tenant_id,
                "still.fresh_consumed",
                -60,
                Some(60),
            );
            // kept: not expired yet — it can still gate a call.
            seed_token(&mut handle, tenant_id, "still.unexpired", 600, None);
            // kept: another tenant's expired, unconsumed row.
            seed_token(&mut handle, other, "still.other_tenant", -60, None);

            let deleted = handle
                .rt
                .block_on(confirm_token_repo::sweep_expired(
                    &handle.maintenance,
                    tenant_id,
                    RETENTION,
                ))
                .expect("sweep runs under role_maintenance");
            assert_eq!(
                deleted, 2,
                "exactly the two rows that can no longer gate anything and are no longer audit"
            );
            assert_eq!(
                surviving(&mut handle, tenant_id),
                vec![
                    "still.fresh_consumed".to_owned(),
                    "still.unexpired".to_owned()
                ],
                "a recently-consumed token and an unexpired one must both survive"
            );
            assert_eq!(
                surviving(&mut handle, other),
                vec!["still.other_tenant".to_owned()],
                "RLS scopes the definer's DELETE: another tenant's rows are untouchable"
            );

            // Idempotent: a second sweep of the same tenant finds nothing left to delete.
            let again = handle
                .rt
                .block_on(confirm_token_repo::sweep_expired(
                    &handle.maintenance,
                    tenant_id,
                    RETENTION,
                ))
                .expect("second sweep");
            assert_eq!(again, 0, "the sweep must converge, not re-delete");

            // A zero retention makes the recently-consumed row deletable too — proof the
            // interval is the CALLER's policy and not a literal frozen inside the function.
            let zero = handle
                .rt
                .block_on(confirm_token_repo::sweep_expired(
                    &handle.maintenance,
                    tenant_id,
                    Duration::ZERO,
                ))
                .expect("zero-retention sweep");
            assert_eq!(zero, 1, "the consumed-recently row is retention-gated");
            assert_eq!(
                surviving(&mut handle, tenant_id),
                vec!["still.unexpired".to_owned()],
                "an unexpired token is never deletable, at any retention"
            );
        },
    );
}

/// §6.2.1: the sweep is the ONLY door. `role_maintenance` holds no table-level DELETE on
/// `control.confirm_tokens` (0169 granted one, `xtask rls-check` caught it, 0170 revoked it) —
/// so an operator who skips the function and types the predicate by hand is refused by the
/// database, not by convention. This is the assertion that keeps the retraction honest.
#[test]
fn the_maintenance_role_cannot_delete_a_confirm_token_directly() {
    run_db_fixture::<ConfirmTokenFixture, _>(
        "the_maintenance_role_cannot_delete_a_confirm_token_directly",
        |mut handle| {
            let tenant_id = handle.tenant_id;
            seed_token(&mut handle, tenant_id, "gone.by_hand", -60, None);

            let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("checked by the fixture");
            let mut maintenance = Client::connect(&dsn_as_role(&dsn, "role_maintenance"), NoTls)
                .expect("role_maintenance connects");
            maintenance
                .batch_execute(&format!("SET humaux.tenant_id = '{tenant_id}'"))
                .expect("tenant context");
            let refused = maintenance.execute(
                "DELETE FROM control.confirm_tokens WHERE expires_at < now()",
                &[],
            );
            let error = refused.expect_err("a direct DELETE must be refused");
            assert_eq!(
                error.code().map(|c| c.code()),
                Some("42501"),
                "insufficient_privilege, not some other failure: {error}"
            );
            assert_eq!(
                surviving(&mut handle, tenant_id),
                vec!["gone.by_hand".to_owned()],
                "the row must still be there after the refused DELETE"
            );
        },
    );
}
