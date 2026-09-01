//! §73.5.1 real PostgreSQL acceptance. Only a dedicated loopback fixture is writable.

use std::net::{IpAddr, Ipv4Addr};
use std::str::FromStr;
use std::time::SystemTime;

use humaux_adapters::postgres::RuntimeDbPool;
use humaux_domain::error::ErrorCode;
use humaux_domain::ids::{TenantId, UserId, WorkspaceId};
use humaux_gateway::auth::{
    AuthenticatedServiceCredential, CredentialScope, authenticate_service_credential,
};
use humaux_protocol::edge::compute_api_key_hash;
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sqlx::postgres::PgConnectOptions;
use sqlx::types::Uuid;

// Synthetic fixture material, never a real issued credential or deployment pepper.
const TEST_PEPPER: &[u8] = b"synthetic-service-credential-fixture-only";
const FIXTURE_DB: &str = "humaux_thread_service_credentials_20260828";

struct Key {
    id: Uuid,
    wire: String,
}

struct Handle {
    rt: tokio::runtime::Runtime,
    runtime: RuntimeDbPool,
    admin: Client,
    gateway: Client,
    tenant: Uuid,
    other_tenant: Uuid,
    user: Uuid,
    workspace: Uuid,
    other_workspace: Uuid,
}

fn setup_failed<T>(_: T) -> DbFixtureSkipReason {
    DbFixtureSkipReason::IsolationSetupFailed("service credential fixture setup failed".into())
}

fn db_ok<T>(result: Result<T, postgres::Error>) -> T {
    result.unwrap_or_else(|error| {
        panic!(
            "fixture SQL failed: {}",
            error.code().map_or("non-SQL", |code| code.code())
        )
    })
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Delete only this test's rows, in FK order; no shared schema/table resets.
        for table in ["api_keys", "memberships", "workspaces", "tenants"] {
            let _ = self.admin.execute(
                &format!("DELETE FROM control.{table} WHERE tenant_id IN ($1, $2)"),
                &[&self.tenant, &self.other_tenant],
            );
        }
        let _ = self.admin.execute(
            "DELETE FROM control.users WHERE user_id = $1",
            &[&self.user],
        );
    }
}

struct CredentialFixture;

impl DbIntegrationFixture for CredentialFixture {
    type Handle = Handle;

    fn isolate() -> Result<Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let options = PgConnectOptions::from_str(&dsn).map_err(setup_failed)?;
        if options.get_host() != "127.0.0.1"
            || options.get_port() != 61719
            || options.get_database() != Some(FIXTURE_DB)
            || dsn.contains(['?', '#'])
        {
            return Err(setup_failed(()));
        }
        let mut admin = Client::connect(&dsn, NoTls).map_err(setup_failed)?;
        let actual_db: String = admin
            .query_one("SELECT current_database()", &[])
            .map_err(setup_failed)?
            .get(0);
        if actual_db != FIXTURE_DB {
            return Err(setup_failed(()));
        }
        let gateway_dsn = std::env::var("HUMAUX_GATEWAY_PG_DSN").map_err(setup_failed)?;
        let gateway_options = PgConnectOptions::from_str(&gateway_dsn).map_err(setup_failed)?;
        if gateway_options.get_username() != "role_gateway"
            || gateway_options.get_host() != "127.0.0.1"
            || gateway_options.get_port() != 61719
            || gateway_options.get_database() != Some(FIXTURE_DB)
            || gateway_dsn.contains(['?', '#'])
        {
            return Err(setup_failed(()));
        }
        let mut gateway = Client::connect(&gateway_dsn, NoTls).map_err(setup_failed)?;
        let identity = gateway
            .query_one(
                "SELECT current_user = 'role_gateway', session_user = 'role_gateway', \
             (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user)",
                &[],
            )
            .map_err(setup_failed)?;
        if !identity.get::<_, bool>(0) || !identity.get::<_, bool>(1) || identity.get::<_, bool>(2)
        {
            return Err(setup_failed(()));
        }
        let rt = tokio::runtime::Runtime::new().map_err(setup_failed)?;
        let runtime = rt
            .block_on(RuntimeDbPool::connect(&gateway_dsn))
            .map_err(setup_failed)?;
        let tenant = Uuid::now_v7();
        let other_tenant = Uuid::now_v7();
        let user = Uuid::now_v7();
        let workspace = Uuid::now_v7();
        let other_workspace = Uuid::now_v7();
        let mut seed = admin.transaction().map_err(setup_failed)?;
        seed.execute(
            "INSERT INTO control.tenants (tenant_id, name, state) VALUES \
             ($1, 'credential fixture', 'ACTIVE'), ($2, 'other credential fixture', 'ACTIVE')",
            &[&tenant, &other_tenant],
        )
        .map_err(setup_failed)?;
        seed.execute(
            "INSERT INTO control.users (user_id, state) VALUES ($1, 'ACTIVE')",
            &[&user],
        )
        .map_err(setup_failed)?;
        seed.execute(
            "INSERT INTO control.memberships (tenant_id, user_id, role, state) \
             VALUES ($1, $2, 'member', 'ACTIVE')",
            &[&tenant, &user],
        )
        .map_err(setup_failed)?;
        seed.execute(
            "INSERT INTO control.workspaces (workspace_id, tenant_id, name) VALUES \
             ($1, $2, 'bound fixture'), ($3, $4, 'foreign fixture')",
            &[&workspace, &tenant, &other_workspace, &other_tenant],
        )
        .map_err(setup_failed)?;
        seed.commit().map_err(setup_failed)?;
        Ok(Handle {
            rt,
            runtime,
            admin,
            gateway,
            tenant,
            other_tenant,
            user,
            workspace,
            other_workspace,
        })
    }
}

impl Handle {
    fn key(&mut self, user: Option<Uuid>, workspace: Option<Uuid>, version: Option<i16>) -> Key {
        let id = Uuid::now_v7();
        let prefix = format!("fixture_{}", id.simple());
        let wire = format!(
            "{prefix}.synthetic_test_material_{}",
            Uuid::now_v7().simple()
        );
        let hash = compute_api_key_hash(TEST_PEPPER, &wire);
        let tenant_epoch = version.map(|_| 0_i64);
        let user_epoch = user.map(|_| 0_i64);
        db_ok(self.admin.execute(
            "INSERT INTO control.api_keys \
             (api_key_id, tenant_id, prefix, key_hash, status, scopes, authorization_version, \
              user_id, workspace_id, tenant_security_epoch, user_security_epoch) \
             VALUES ($1, $2, $3, $4, 'ACTIVE', ARRAY['context:read'], $5, $6, $7, $8, $9)",
            &[
                &id,
                &self.tenant,
                &prefix,
                &hash,
                &version,
                &user,
                &workspace,
                &tenant_epoch,
                &user_epoch,
            ],
        ));
        Key { id, wire }
    }

    fn authenticate(&self, key: &Key) -> Result<AuthenticatedServiceCredential, ErrorCode> {
        self.rt.block_on(authenticate_service_credential(
            &self.runtime,
            &format!("Bearer {}", key.wire),
            TEST_PEPPER,
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            SystemTime::now(),
        ))
    }

    fn used(&mut self, key: &Key) -> bool {
        db_ok(self.admin.query_one(
            "SELECT last_used_at IS NOT NULL FROM control.api_keys WHERE api_key_id = $1",
            &[&key.id],
        ))
        .get(0)
    }

    fn set_key(&mut self, key: &Key, assignments: &str) {
        // All assignments below are fixed test literals, never external SQL input.
        db_ok(self.admin.execute(
            &format!("UPDATE control.api_keys SET {assignments} WHERE api_key_id = $1"),
            &[&key.id],
        ));
    }
}

#[test]
#[ignore = "requires isolated PostgreSQL fixture with migration 0112"]
fn legacy_and_wrong_hmac_credentials_are_inert_without_usage_writes() {
    run_db_fixture::<CredentialFixture, _>("legacy and HMAC rejection", |mut f| {
        let legacy = f.key(None, None, None);
        assert_eq!(f.authenticate(&legacy).err(), Some(ErrorCode::Unauthorized));
        assert!(!f.used(&legacy));
        let valid = f.key(None, None, Some(1));
        let prefix = valid.wire.split_once('.').unwrap().0;
        let wrong = Key {
            id: valid.id,
            wire: format!("{prefix}.{}", "x".repeat(40)),
        };
        assert_eq!(f.authenticate(&wrong).err(), Some(ErrorCode::Unauthorized));
        assert!(!f.used(&valid));
        assert!(f.authenticate(&valid).is_ok());
        assert!(f.used(&valid));
    });
}

#[test]
#[ignore = "requires isolated PostgreSQL fixture with migration 0112"]
fn valid_machine_and_pat_scopes_come_only_from_database_bindings() {
    run_db_fixture::<CredentialFixture, _>("machine and PAT bindings", |mut f| {
        let machine = f.key(None, Some(f.workspace), Some(1));
        let credential = f
            .authenticate(&machine)
            .expect("machine credential must authenticate");
        let auth = credential
            .authorize(CredentialScope::ContextRead, None)
            .unwrap();
        assert_eq!(auth.tenant_id(), TenantId(f.tenant));
        assert_eq!(auth.principal().0, machine.id);
        assert_eq!(auth.user_id(), None);
        assert_eq!(
            credential.bound_workspace_id(),
            Some(WorkspaceId(f.workspace))
        );
        assert_eq!(
            auth.allowed_workspace_ids()
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            [WorkspaceId(f.workspace)]
        );
        let pat = f.key(Some(f.user), None, Some(1));
        let credential = f.authenticate(&pat).expect("PAT must authenticate");
        let auth = credential
            .authorize(CredentialScope::ContextRead, None)
            .unwrap();
        assert_eq!(auth.user_id(), Some(UserId(f.user)));
        assert_eq!(auth.principal().0, pat.id);
        assert!(auth.allowed_workspace_ids().is_empty());
        assert_eq!(
            credential.authorize(CredentialScope::ContextRead, Some(WorkspaceId(f.workspace))),
            Err(ErrorCode::Forbidden)
        );
        assert!(f.used(&machine) && f.used(&pat));
    });
}

#[test]
#[ignore = "requires isolated PostgreSQL fixture with migration 0112"]
fn missing_unknown_scopes_and_workspace_expansion_fail_closed() {
    run_db_fixture::<CredentialFixture, _>("scope and workspace rejection", |mut f| {
        let key = f.key(None, Some(f.workspace), Some(1));
        let credential = f.authenticate(&key).unwrap();
        assert_eq!(
            credential.authorize(CredentialScope::MemoryWrite, None),
            Err(ErrorCode::Forbidden)
        );
        assert_eq!(
            credential.authorize(
                CredentialScope::ContextRead,
                Some(WorkspaceId(f.other_workspace))
            ),
            Err(ErrorCode::Forbidden)
        );
        assert_eq!(
            credential.authorize(CredentialScope::ContextRead, Some(WorkspaceId::new())),
            Err(ErrorCode::Forbidden)
        );
        f.set_key(&key, "scopes = ARRAY['context:*']");
        assert_eq!(f.authenticate(&key).err(), Some(ErrorCode::Unauthorized));
        f.set_key(&key, "scopes = ARRAY[]::text[]");
        assert_eq!(
            f.authenticate(&key)
                .unwrap()
                .authorize(CredentialScope::ContextRead, None),
            Err(ErrorCode::Forbidden)
        );
    });
}

#[test]
#[ignore = "requires isolated PostgreSQL fixture with migration 0112"]
fn each_request_checks_key_lifecycle_expiry_revocation_and_cidr() {
    run_db_fixture::<CredentialFixture, _>("live key lifecycle", |mut f| {
        let key = f.key(None, None, Some(1));
        assert!(f.authenticate(&key).is_ok());
        f.set_key(&key, "status = 'ROTATING'");
        assert!(
            f.authenticate(&key).is_ok(),
            "valid rotation overlap remains allowed"
        );
        for (invalid, reset) in [
            ("status = 'CREATE'", "status = 'ACTIVE'"),
            ("status = 'REVOKED'", "status = 'ACTIVE'"),
            ("status = 'EXPIRED'", "status = 'ACTIVE'"),
            (
                "expires_at = now() - interval '1 second'",
                "expires_at = NULL",
            ),
            (
                "revoked_at = now() - interval '1 second'",
                "revoked_at = NULL",
            ),
            (
                "allowed_cidrs = ARRAY['203.0.113.0/24']::cidr[]",
                "allowed_cidrs = ARRAY['127.0.0.0/8']::cidr[]",
            ),
        ] {
            f.set_key(&key, invalid);
            assert_eq!(
                f.authenticate(&key).err(),
                Some(ErrorCode::Unauthorized),
                "{invalid}"
            );
            f.set_key(&key, reset);
            assert!(f.authenticate(&key).is_ok(), "{reset}");
        }
    });
}

#[test]
#[ignore = "requires isolated PostgreSQL fixture with migration 0112"]
fn each_request_rechecks_live_accounts_membership_and_security_epochs() {
    run_db_fixture::<CredentialFixture, _>("live grant revalidation", |mut f| {
        let key = f.key(Some(f.user), Some(f.workspace), Some(1));
        assert!(f.authenticate(&key).is_ok());
        for (invalid, reset, id) in [
            (
                "UPDATE control.tenants SET state = 'SUSPENDED' WHERE tenant_id = $1",
                "UPDATE control.tenants SET state = 'ACTIVE' WHERE tenant_id = $1",
                f.tenant,
            ),
            (
                "UPDATE control.tenants SET security_epoch = 1 WHERE tenant_id = $1",
                "UPDATE control.tenants SET security_epoch = 0 WHERE tenant_id = $1",
                f.tenant,
            ),
            (
                "UPDATE control.users SET state = 'SUSPENDED' WHERE user_id = $1",
                "UPDATE control.users SET state = 'ACTIVE' WHERE user_id = $1",
                f.user,
            ),
            (
                "UPDATE control.users SET security_epoch = 1 WHERE user_id = $1",
                "UPDATE control.users SET security_epoch = 0 WHERE user_id = $1",
                f.user,
            ),
            (
                "UPDATE control.memberships SET state = 'REMOVED' WHERE user_id = $1",
                "UPDATE control.memberships SET state = 'ACTIVE' WHERE user_id = $1",
                f.user,
            ),
            (
                "UPDATE control.memberships SET state = 'SUSPENDED' WHERE user_id = $1",
                "UPDATE control.memberships SET state = 'ACTIVE' WHERE user_id = $1",
                f.user,
            ),
        ] {
            db_ok(f.admin.execute(invalid, &[&id]));
            assert_eq!(
                f.authenticate(&key).err(),
                Some(ErrorCode::Unauthorized),
                "{invalid}"
            );
            db_ok(f.admin.execute(reset, &[&id]));
            assert!(f.authenticate(&key).is_ok(), "{reset}");
        }
        db_ok(f.admin.execute(
            "DELETE FROM control.memberships WHERE tenant_id = $1 AND user_id = $2",
            &[&f.tenant, &f.user],
        ));
        db_ok(f.admin.execute("INSERT INTO control.memberships (tenant_id, user_id, role, state) VALUES ($1, $2, 'member', 'ACTIVE')", &[&f.other_tenant, &f.user]));
        assert_eq!(
            f.authenticate(&key).err(),
            Some(ErrorCode::Unauthorized),
            "another tenant's membership is not this tenant's grant"
        );
    });
}

#[test]
#[ignore = "requires isolated PostgreSQL fixture with migration 0112"]
fn database_rejects_null_version_partial_bindings_and_cross_tenant_workspace() {
    run_db_fixture::<CredentialFixture, _>("binding constraints", |mut f| {
        let key = f.key(None, None, Some(1));
        for invalid in [
            "authorization_version = NULL",
            "authorization_version = 2",
            "tenant_security_epoch = NULL",
            "tenant_security_epoch = -1",
            "user_security_epoch = 0",
        ] {
            let result = f.admin.execute(
                &format!("UPDATE control.api_keys SET {invalid} WHERE api_key_id = $1"),
                &[&key.id],
            );
            let error = result.expect_err("malformed binding must fail at the database");
            assert_eq!(error.code().map(|code| code.code()), Some("23514"));
        }
        let error = f
            .admin
            .execute(
                "UPDATE control.api_keys SET workspace_id = $1 WHERE api_key_id = $2",
                &[&f.other_workspace, &key.id],
            )
            .expect_err("cross-tenant workspace binding must fail");
        assert_eq!(error.code().map(|code| code.code()), Some("23503"));
        let legacy = f.key(None, None, None);
        let error = f
            .admin
            .execute(
                "UPDATE control.api_keys SET workspace_id = $1 WHERE api_key_id = $2",
                &[&f.workspace, &legacy.id],
            )
            .expect_err("partial legacy binding must fail");
        assert_eq!(error.code().map(|code| code.code()), Some("23514"));
    });
}

#[test]
#[ignore = "requires isolated PostgreSQL fixture with migration 0112"]
fn credential_lookup_is_gateway_only_and_direct_table_reads_stay_rls_scoped() {
    run_db_fixture::<CredentialFixture, _>("credential role boundary", |mut f| {
        let key = f.key(None, None, Some(1));
        let prefix = key.wire.split_once('.').unwrap().0;
        let before = guc_state(&mut f.gateway);
        let direct: i64 = db_ok(
            f.gateway
                .query_one("SELECT count(*) FROM control.api_keys", &[]),
        )
        .get(0);
        assert_eq!(
            direct, 0,
            "no pre-auth tenant GUC means no direct row access"
        );
        let lookup: i64 = db_ok(f.gateway.query_one(
            "SELECT count(*) FROM control.api_key_lookup($1)",
            &[&prefix],
        ))
        .get(0);
        assert_eq!(
            lookup, 1,
            "the sole pre-auth definer returns the requested prefix"
        );
        assert_eq!(guc_state(&mut f.gateway), before);
        let identity: bool = db_ok(f.gateway.query_one(
            "SELECT current_user = 'role_gateway' AND session_user = 'role_gateway'",
            &[],
        ))
        .get(0);
        assert!(
            identity,
            "definer invocation restores the real gateway caller"
        );
        let error = f
            .gateway
            .batch_execute("SET ROLE role_migration_owner")
            .expect_err("gateway login cannot become the trusted owner");
        assert_eq!(error.code().map(|code| code.code()), Some("42501"));
        let mut txn = db_ok(f.admin.transaction());
        db_ok(txn.batch_execute("SET LOCAL ROLE role_public_worker"));
        let error = txn
            .query(
                "SELECT count(*) FROM control.api_key_lookup($1)",
                &[&prefix],
            )
            .expect_err("public worker must not read private credential verifiers");
        assert_eq!(error.code().map(|code| code.code()), Some("42501"));
        db_ok(txn.rollback());
    });
}

fn guc_state(client: &mut Client) -> (Option<String>, Option<String>) {
    let row = db_ok(client.query_one(
        "SELECT current_setting('humaux.tenant_id', true), current_setting('humaux.user_id', true)",
        &[],
    ));
    (row.get(0), row.get(1))
}

#[test]
#[ignore = "requires isolated PostgreSQL fixture with migration 0112"]
fn bootstrap_ignores_forged_gucs_and_touch_changes_only_one_row_timestamp() {
    run_db_fixture::<CredentialFixture, _>("bootstrap GUC and touch boundary", |mut f| {
        let target = f.key(Some(f.user), Some(f.workspace), Some(1));
        let other = f.key(None, None, Some(1));
        let target_before: String = db_ok(f.admin.query_one(
            "SELECT (to_jsonb(k) - 'last_used_at')::text FROM control.api_keys k WHERE api_key_id = $1",
            &[&target.id],
        )).get(0);
        let other_before: String = db_ok(f.admin.query_one(
            "SELECT to_jsonb(k)::text FROM control.api_keys k WHERE api_key_id = $1",
            &[&other.id],
        ))
        .get(0);
        db_ok(f.gateway.query_one(
            "SELECT set_config('humaux.tenant_id', $1, false), set_config('humaux.user_id', $2, false)",
            &[&f.other_tenant.to_string(), &Uuid::now_v7().to_string()],
        ));
        let before = guc_state(&mut f.gateway);
        let prefix = target.wire.split_once('.').unwrap().0;
        let actual: Uuid = db_ok(f.gateway.query_one(
            "SELECT api_key_id FROM control.api_key_lookup($1)",
            &[&prefix],
        ))
        .get(0);
        assert_eq!(actual, target.id);
        assert!(!f.used(&target));
        db_ok(
            f.gateway
                .query_one("SELECT control.api_key_touch_last_used($1)", &[&target.id]),
        );
        assert!(f.used(&target));
        assert!(!f.used(&other));
        assert_eq!(guc_state(&mut f.gateway), before);
        let target_after: String = db_ok(f.admin.query_one(
            "SELECT (to_jsonb(k) - 'last_used_at')::text FROM control.api_keys k WHERE api_key_id = $1",
            &[&target.id],
        )).get(0);
        let other_after: String = db_ok(f.admin.query_one(
            "SELECT to_jsonb(k)::text FROM control.api_keys k WHERE api_key_id = $1",
            &[&other.id],
        ))
        .get(0);
        // Boolean comparisons keep verifier material out of a failed assertion's output.
        assert!(
            target_before == target_after,
            "touch modified a non-timestamp column"
        );
        assert!(
            other_before == other_after,
            "touch modified another credential"
        );
    });
}
