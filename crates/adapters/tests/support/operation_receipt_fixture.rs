//! `adapters::tests::support::operation_receipt_fixture` — Shared, isolated §34.0.1 PostgreSQL fixture for adapter
//!   and gateway integration tests.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-testkit, postgres, serde_json, sqlx, tokio];
//!   services=[PostgreSQL(owner) w=[control.api_keys, control.audit_events, control.entitlement_snapshots,
//!   control.memberships, control.operation_receipts, control.private_reasoning_domains, control.quota_windows,
//!   control.rate_buckets, control.tenants, control.usage_reservations, control.users, control.workspace_memberships,
//!   control.workspaces, coord.tasks, ops.jobs, ops.outbox, ops.selection_snapshot_items, ops.selection_snapshots,
//!   private.context_bindings, private.continuity_facet_evidence_links, private.continuity_facet_memory_links,
//!   private.continuity_facet_slots, private.continuity_facet_versions, private.continuity_projects, private.events,
//!   private.evidence_objects, private.memory_evidence, private.memory_records, projection.private_memory_points,
//!   projection.stream_checkpoints, projection.stream_log, projection.tenant_placements]
//!   x=[control.check_operation_receipt], PostgreSQL(role_gateway), PostgreSQL(role_maintenance)];
//!   env=[HUMAUX_GATEWAY_PG_DSN, HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_TEST_PG_DSN]; modules=[adapters::postgres,
//!   adapters::quota_repo, adapters::tests::support::token_keys, domain::identity, domain::ids, humaux-testkit]
//! Called-by: [adapters::tests::auth_scope_rls, adapters::tests::membership_lifecycle, adapters::tests::operation_receipts, gateway::mcp_application, gateway::tests::continuity_get, gateway::tests::mcp_gateway, gateway::tests::read_decoupling, gateway::tests::semantic_recall_wiring]
//! Invariants: [test-only, included by #[path]; runtime writes are real role_gateway logins and the owner connection
//!   only seeds/cleans the fixture tenant; an isolation setup failure is a fixture error]
//! Spec: Baseline §25.4; §34.0.1; §79.2; ADR-0035
//!
//! This module is test-only and included by relative path; it is not an adapters export or a
//! replacement for `humaux_testkit`. Runtime writes remain actual `role_gateway` logins.

#[path = "token_keys.rs"]
mod token_keys;
use std::str::FromStr;

use humaux_adapters::{
    postgres::{MaintenanceDbPool, RuntimeDbPool},
    quota_repo,
};
use humaux_domain::{
    identity::{AuthorizationScope, BoundedSet, PrincipalId},
    ids::{TenantId, UserId, WorkspaceId},
};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture};
use postgres::{Client, NoTls};
use sqlx::postgres::PgConnectOptions;
use sqlx::types::Uuid;

pub const FIXTURE_DB: &str = "humaux_thread_request_guard_20260828";
const PHASE9_OWNER_FIXTURE_DB: &str = "humaux_thread_p9_owner_accept";
pub const OPERATION: &str = "remember.put";
pub const DOMAIN: &str = "knowledge";
pub const PROJECTION_KIND: &str = "ingest";
pub const PROJECTION_VERSION: &str = "v1";
/// Test-only HMAC pepper for synthetic credentials. It is neither a deployment secret nor an
/// environment value; gateway tests use it to derive the row hash they pass back here.
pub const SYNTHETIC_CREDENTIAL_PEPPER: &[u8] = b"operation-receipt-fixture-only";

/// Synthetic bearer material for an isolated API-key row.
pub struct SyntheticServiceCredential {
    pub api_key_id: Uuid,
    pub bearer: String,
}

/// Closed scope sets permitted for synthetic fixture credentials.
#[derive(Clone, Copy)]
pub enum SyntheticCredentialScopes {
    RememberWrite,
    ContextRead,
    RememberWriteAndContextRead,
}

impl SyntheticCredentialScopes {
    const fn sql_array(self) -> &'static str {
        match self {
            Self::RememberWrite => "ARRAY['memory:write']",
            Self::ContextRead => "ARRAY['context:read']",
            Self::RememberWriteAndContextRead => "ARRAY['memory:write','context:read']",
        }
    }
}

/// IDs for one owner-seeded workspace-visible ContextReadAdapter row.
pub struct ScopedContextRecord {
    pub memory_id: Uuid,
    pub evidence_id: Uuid,
    pub binding_id: Uuid,
}

/// Every consumer of this fixture creates and tears down a real tenant, and the teardown
/// toggles triggers on shared tables (`control.audit_events`, the continuity append-only
/// guards). Two handles alive at once in one test binary therefore interfere — a fact that
/// stayed hidden while the fixture skipped on every standard environment. Handles hold this
/// guard for their whole lifetime so consumers serialize without `--test-threads=1`.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
thread_local! {
    // A test that builds a second handle on the same thread must not deadlock on itself.
    static SERIAL_HELD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub struct Handle {
    _serial: Option<std::sync::MutexGuard<'static, ()>>,
    pub rt: tokio::runtime::Runtime,
    pub runtime: RuntimeDbPool,
    pub maintenance: MaintenanceDbPool,
    admin_dsn: String,
    gateway_dsn: String,
    pub admin: Client,
    pub tenant_id: Uuid,
    pub user_id: Uuid,
    pub principal_id: Uuid,
    pub workspace_id: Uuid,
    pub reasoning_domain_id: Uuid,
    pub auth: AuthorizationScope,
    extra_user_ids: Vec<Uuid>,
    extra_workspace_ids: Vec<Uuid>,
    system_preauth_subject_ids: Vec<String>,
}

pub struct Fixture;

fn setup_failed<T>(_: T) -> DbFixtureSkipReason {
    DbFixtureSkipReason::IsolationSetupFailed("operation receipt fixture setup failed".into())
}

/// The disposable target every role DSN must point at. By contract that is whatever
/// `HUMAUX_TEST_PG_DSN` names (the repo-wide isolated test database, §79.2) — pinning two
/// machine-local ports here made every consumer of this fixture skip (or spin up a side
/// container) on any standard environment, i.e. a false green. The two legacy literal targets
/// stay accepted for the environments that still use them.
struct FixtureTarget {
    port: u16,
    database: String,
}

fn expected_dsn(
    options: &PgConnectOptions,
    dsn: &str,
    role: Option<&str>,
    target: &FixtureTarget,
) -> bool {
    let fixture_target = (options.get_port() == target.port
        && options.get_database() == Some(target.database.as_str()))
        || (options.get_port() == 61719 && options.get_database() == Some(FIXTURE_DB))
        || (options.get_port() == 50324 && options.get_database() == Some(PHASE9_OWNER_FIXTURE_DB));
    role.is_none_or(|expected| options.get_username() == expected)
        && options.get_host() == "127.0.0.1"
        && fixture_target
        && !dsn.contains(['?', '#'])
}

fn fixture_dsns() -> Result<(String, String, String), DbFixtureSkipReason> {
    let admin_dsn =
        std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
    let gateway_dsn = std::env::var("HUMAUX_GATEWAY_PG_DSN").map_err(setup_failed)?;
    let maintenance_dsn = std::env::var("HUMAUX_MAINTENANCE_PG_DSN").map_err(setup_failed)?;
    let admin_options = PgConnectOptions::from_str(&admin_dsn).map_err(setup_failed)?;
    let gateway_options = PgConnectOptions::from_str(&gateway_dsn).map_err(setup_failed)?;
    let maintenance_options = PgConnectOptions::from_str(&maintenance_dsn).map_err(setup_failed)?;
    let target = FixtureTarget {
        port: admin_options.get_port(),
        database: admin_options
            .get_database()
            .ok_or_else(|| setup_failed(()))?
            .to_owned(),
    };
    if !expected_dsn(&admin_options, &admin_dsn, None, &target)
        || !expected_dsn(
            &gateway_options,
            &gateway_dsn,
            Some("role_gateway"),
            &target,
        )
        || !expected_dsn(
            &maintenance_options,
            &maintenance_dsn,
            Some("role_maintenance"),
            &target,
        )
    {
        return Err(setup_failed(()));
    }
    Ok((admin_dsn, gateway_dsn, maintenance_dsn))
}

impl DbIntegrationFixture for Fixture {
    type Handle = Handle;

    #[allow(clippy::too_many_lines)] // one linear owner-role seed of the whole fixture tenant tree
    fn isolate() -> Result<Handle, DbFixtureSkipReason> {
        token_keys::install();
        let serial = if SERIAL_HELD.with(std::cell::Cell::get) {
            None
        } else {
            let guard = SERIAL
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            SERIAL_HELD.with(|held| held.set(true));
            Some(guard)
        };
        let (admin_dsn, gateway_dsn, maintenance_dsn) = fixture_dsns()?;

        // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
        let mut admin = Client::connect(&admin_dsn, NoTls).map_err(setup_failed)?;
        let required: bool = admin
            .query_one(
                "SELECT to_regclass('control.operation_receipts') IS NOT NULL \
                   AND to_regclass('control.usage_reservations') IS NOT NULL \
                   AND to_regprocedure('control.check_operation_receipt()') IS NOT NULL",
                &[],
            )
            .map_err(setup_failed)?
            .get(0);
        if !required {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "guard82 is required: migration 0114 operation receipts is not applied".into(),
            ));
        }

        // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
        let mut gateway = Client::connect(&gateway_dsn, NoTls).map_err(setup_failed)?;
        let gateway_is_real_login: bool = gateway
            .query_one(
                "SELECT current_user='role_gateway' AND session_user='role_gateway' \
                 AND NOT (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user)",
                &[],
            )
            .map_err(setup_failed)?
            .get(0);
        if !gateway_is_real_login {
            return Err(setup_failed(()));
        }

        let tenant_id = Uuid::new_v4();
        let user_id = Uuid::new_v4();
        let principal_id = user_id;
        let workspace_id = Uuid::new_v4();
        let reasoning_domain_id = Uuid::new_v4();
        let rt = tokio::runtime::Runtime::new().map_err(setup_failed)?;
        let runtime = rt
            // dep: PostgreSQL(role_gateway) — test opens a direct PG connection for setup/verification
            .block_on(RuntimeDbPool::connect(&gateway_dsn))
            .map_err(setup_failed)?;
        let maintenance = rt
            // dep: PostgreSQL(role_maintenance) — test opens a direct PG connection for setup/verification
            .block_on(MaintenanceDbPool::connect(&maintenance_dsn))
            .map_err(setup_failed)?;
        let auth = AuthorizationScope::new(
            TenantId(tenant_id),
            PrincipalId(principal_id),
            Some(UserId(user_id)),
            BoundedSet::new([WorkspaceId(workspace_id)]).map_err(setup_failed)?,
        );

        let mut seed = admin.transaction().map_err(setup_failed)?;
        seed.execute(
            "INSERT INTO control.tenants(tenant_id,name,state) VALUES($1,$2,'ACTIVE')",
            &[&tenant_id, &format!("operation-receipts-{tenant_id}")],
        )
        .map_err(setup_failed)?;
        seed.execute(
            "INSERT INTO control.users(user_id,state) VALUES($1,'ACTIVE')",
            &[&user_id],
        )
        .map_err(setup_failed)?;
        seed.execute(
            "INSERT INTO control.memberships(tenant_id,user_id,role,state) VALUES($1,$2,'member','ACTIVE')",
            &[&tenant_id, &user_id],
        )
        .map_err(setup_failed)?;
        seed.execute(
            "INSERT INTO control.workspaces(workspace_id,tenant_id,name) VALUES($1,$2,'operation receipt fixture')",
            &[&workspace_id, &tenant_id],
        )
        .map_err(setup_failed)?;
        // ADR-0035 (card 13): a user-bound PAT's `allowed_workspace_ids` is now derived from live
        // `control.workspace_memberships` (0162), not tenant membership. The fixture tenant is
        // created after 0162's backfill, so the fixture user is made an ACTIVE MEMBER of its own
        // workspace here — the test-fixture analogue of that backfill — or the default user-bound
        // credential authenticates to an empty workspace set. Extra workspaces created by
        // `seed_workspace` grant no membership unless the test seeds one (mirrors the freeze rule).
        seed.execute(
            "INSERT INTO control.workspace_memberships(tenant_id,workspace_id,user_id,role,state) \
             VALUES($1,$2,$3,'MEMBER','ACTIVE')",
            &[&tenant_id, &workspace_id, &user_id],
        )
        .map_err(setup_failed)?;
        seed.execute(
            "INSERT INTO control.private_reasoning_domains(reasoning_domain_id,tenant_id,name) VALUES($1,$2,'default')",
            &[&reasoning_domain_id, &tenant_id],
        )
        .map_err(setup_failed)?;
        seed.commit().map_err(setup_failed)?;

        Ok(Handle {
            _serial: serial,
            rt,
            runtime,
            maintenance,
            admin_dsn,
            gateway_dsn,
            admin,
            tenant_id,
            user_id,
            principal_id,
            workspace_id,
            reasoning_domain_id,
            auth,
            extra_user_ids: Vec::new(),
            extra_workspace_ids: Vec::new(),
            system_preauth_subject_ids: Vec::new(),
        })
    }
}

impl Drop for Handle {
    #[allow(clippy::too_many_lines)]
    fn drop(&mut self) {
        if self._serial.is_some() {
            SERIAL_HELD.with(|held| held.set(false));
        }
        let mut user_ids = self.extra_user_ids.clone();
        user_ids.push(self.user_id);
        let mut workspace_ids = self.extra_workspace_ids.clone();
        workspace_ids.push(self.workspace_id);
        // Card-31 pattern (card 33 leak fix): remember's EVIDENCE_ACCEPTED rows and every PRIMARY
        // link a governance op materializes enqueue DERIVED_* jobs (0164 triggers). They are deleted
        // in their own autocommit statement first, so a failure anywhere in the all-or-nothing
        // teardown below (which ends in the tenant delete) cannot roll the job delete back.
        if let Err(error) = self.admin.execute(
            "DELETE FROM ops.jobs WHERE tenant_id=$1",
            &[&self.tenant_id],
        ) {
            eprintln!(
                "operation receipt fixture job cleanup failed for tenant {}: {error}",
                self.tenant_id
            );
        }
        // Strict guard82-only owner teardown. The trigger change is transactional: any cleanup
        // error rolls it back rather than leaving audit_events mutable.
        let cleanup = (|| -> Result<(), postgres::Error> {
            let mut txn = self.admin.transaction()?;
            let continuity_schema_ready: bool = txn
                .query_one(
                    "SELECT to_regclass('private.continuity_projects') IS NOT NULL \
                       AND to_regclass('private.continuity_facet_versions') IS NOT NULL \
                       AND to_regclass('private.continuity_facet_memory_links') IS NOT NULL \
                       AND to_regclass('private.continuity_facet_evidence_links') IS NOT NULL \
                       AND to_regclass('private.continuity_facet_slots') IS NOT NULL",
                    &[],
                )?
                .get(0);
            txn.batch_execute(
                "ALTER TABLE control.audit_events DISABLE TRIGGER audit_events_reject_mutation",
            )?;
            if continuity_schema_ready {
                let has_continuity_project: bool = txn
                    .query_one(
                        "SELECT EXISTS(SELECT 1 FROM private.continuity_projects WHERE tenant_id=$1)",
                        &[&self.tenant_id],
                    )?
                    .get(0);
                if has_continuity_project {
                    txn.query_one(
                        "SELECT pg_advisory_xact_lock(\
                           hashtextextended('operation_receipt_fixture_continuity_cleanup',0))",
                        &[],
                    )?;
                    txn.batch_execute(
                        "ALTER TABLE private.continuity_facet_memory_links \
                           DISABLE TRIGGER continuity_facet_memory_links_append_only; \
                         ALTER TABLE private.continuity_facet_evidence_links \
                           DISABLE TRIGGER continuity_facet_evidence_links_append_only; \
                         ALTER TABLE private.continuity_facet_versions \
                           DISABLE TRIGGER continuity_facet_versions_append_only; \
                         ALTER TABLE private.continuity_projects \
                           DISABLE TRIGGER continuity_project_mutation_guard",
                    )?;
                    for statement in [
                        "DELETE FROM private.continuity_facet_memory_links WHERE tenant_id=$1",
                        "DELETE FROM private.continuity_facet_evidence_links WHERE tenant_id=$1",
                        "DELETE FROM private.continuity_facet_slots WHERE tenant_id=$1",
                        "DELETE FROM private.continuity_facet_versions WHERE tenant_id=$1",
                        "DELETE FROM private.continuity_projects WHERE tenant_id=$1",
                    ] {
                        txn.execute(statement, &[&self.tenant_id])?;
                    }
                    txn.batch_execute("SET CONSTRAINTS ALL IMMEDIATE")?;
                    txn.batch_execute(
                        "ALTER TABLE private.continuity_facet_memory_links \
                           ENABLE TRIGGER continuity_facet_memory_links_append_only; \
                         ALTER TABLE private.continuity_facet_evidence_links \
                           ENABLE TRIGGER continuity_facet_evidence_links_append_only; \
                         ALTER TABLE private.continuity_facet_versions \
                           ENABLE TRIGGER continuity_facet_versions_append_only; \
                         ALTER TABLE private.continuity_projects \
                           ENABLE TRIGGER continuity_project_mutation_guard",
                    )?;
                }
            }
            for statement in [
                "DELETE FROM control.operation_receipts WHERE tenant_id=$1",
                "DELETE FROM ops.outbox WHERE tenant_id=$1",
                "DELETE FROM projection.private_memory_points WHERE tenant_id=$1",
                "DELETE FROM projection.tenant_placements WHERE tenant_id=$1",
                "DELETE FROM projection.stream_log WHERE tenant_id=$1",
                "DELETE FROM projection.stream_checkpoints WHERE tenant_id=$1",
                "DELETE FROM private.context_bindings WHERE tenant_id=$1",
                "DELETE FROM ops.selection_snapshot_items WHERE tenant_id=$1",
                "DELETE FROM ops.selection_snapshots WHERE tenant_id=$1",
            ] {
                txn.execute(statement, &[&self.tenant_id])?;
            }
            txn.execute(
                "DELETE FROM control.rate_buckets \
                 WHERE tenant_id='00000000-0000-0000-0000-000000000000' \
                   AND subject_kind='ip' AND subject_id=ANY($1) \
                   AND operation='mcp' AND bucket_key='preauth'",
                &[&self.system_preauth_subject_ids],
            )?;
            txn.execute(
                "DELETE FROM private.memory_evidence WHERE memory_id IN \
                 (SELECT memory_id FROM private.memory_records WHERE tenant_id=$1)",
                &[&self.tenant_id],
            )?;
            txn.execute(
                "DELETE FROM private.memory_records WHERE tenant_id=$1",
                &[&self.tenant_id],
            )?;
            txn.execute(
                "DELETE FROM private.events WHERE event_id IN \
                 (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id=$1)",
                &[&self.tenant_id],
            )?;
            txn.execute(
                "DELETE FROM private.evidence_objects WHERE tenant_id=$1",
                &[&self.tenant_id],
            )?;
            for statement in [
                "DELETE FROM control.audit_events WHERE tenant_id=$1",
                "DELETE FROM control.usage_reservations WHERE tenant_id=$1",
                "DELETE FROM control.rate_buckets WHERE tenant_id=$1",
                "DELETE FROM control.quota_windows WHERE tenant_id=$1",
                "DELETE FROM control.entitlement_snapshots WHERE tenant_id=$1",
                "DELETE FROM control.api_keys WHERE tenant_id=$1",
                "DELETE FROM control.private_reasoning_domains WHERE tenant_id=$1",
                "DELETE FROM control.memberships WHERE tenant_id=$1",
                // card 22b review fix: §25.4.A(7)'s TaskId is resolved against `coord.tasks`, so
                // a fixture that binds anything to a task now owns rows in this table too, and
                // its `tenant_id` FK blocks the tenant delete below.
                "DELETE FROM coord.tasks WHERE tenant_id=$1",
            ] {
                txn.execute(statement, &[&self.tenant_id])?;
            }
            txn.execute(
                "DELETE FROM control.workspaces WHERE tenant_id=$1 AND workspace_id=ANY($2)",
                &[&self.tenant_id, &workspace_ids],
            )?;
            txn.execute(
                "DELETE FROM control.users WHERE user_id=ANY($1)",
                &[&user_ids],
            )?;
            txn.execute(
                "DELETE FROM control.tenants WHERE tenant_id=$1",
                &[&self.tenant_id],
            )?;
            txn.batch_execute(
                "ALTER TABLE control.audit_events ENABLE TRIGGER audit_events_reject_mutation",
            )?;
            txn.commit()
        })();
        if let Err(error) = cleanup {
            if std::thread::panicking() {
                eprintln!("operation receipt fixture cleanup failed: {error:?}");
            } else {
                panic!("operation receipt fixture cleanup failed: {error:?}");
            }
        }
    }
}

impl Handle {
    pub fn seed_legacy_system_preauth_bucket(&mut self, subject_id: String) {
        self.admin.execute(
            "INSERT INTO control.rate_buckets(tenant_id,subject_kind,subject_id,operation,bucket_key,capacity,tokens,refill_per_second,updated_at) \
             VALUES('00000000-0000-0000-0000-000000000000','ip',$1,'mcp','preauth',100,0,100,clock_timestamp()-interval '2 seconds') \
             ON CONFLICT (tenant_id,subject_kind,subject_id,operation,bucket_key) DO UPDATE \
             SET capacity=EXCLUDED.capacity,tokens=EXCLUDED.tokens,refill_per_second=EXCLUDED.refill_per_second,updated_at=EXCLUDED.updated_at",
            &[&subject_id],
        ).unwrap();
        self.system_preauth_subject_ids.push(subject_id);
    }
    /// Tracks only fixture-owned identities so teardown also works after an assertion panic.
    pub fn seed_peer_user(&mut self) -> Uuid {
        let user_id = Uuid::new_v4();
        self.extra_user_ids.push(user_id);
        let mut seed = self.admin.transaction().expect("begin peer user seed");
        seed.execute(
            "INSERT INTO control.users(user_id,state) VALUES($1,'ACTIVE')",
            &[&user_id],
        )
        .expect("owner seeds fixture peer user");
        seed.execute(
            "INSERT INTO control.memberships(tenant_id,user_id,role,state) \
             VALUES($1,$2,'member','ACTIVE')",
            &[&self.tenant_id, &user_id],
        )
        .expect("owner seeds fixture peer membership");
        // ADR-0035 (card 13): a tenant membership no longer grants any workspace. Give the peer an
        // ACTIVE WorkspaceMembership on the fixture default workspace so peer-bound credentials
        // authenticate to it (the common case: a peer acting in the fixture workspace). Tests that
        // need a peer with a different/precise workspace set seed those rows themselves.
        seed.execute(
            "INSERT INTO control.workspace_memberships(tenant_id,workspace_id,user_id,role,state) \
             VALUES($1,$2,$3,'MEMBER','ACTIVE')",
            &[&self.tenant_id, &self.workspace_id, &user_id],
        )
        .expect("owner seeds fixture peer workspace membership");
        seed.commit().expect("commit fixture peer user");
        user_id
    }

    pub fn seed_workspace(&mut self) -> Uuid {
        self.seed_workspace_with_id(Uuid::new_v4())
    }

    /// Seeds a fixture-owned workspace with a caller-selected identity.
    pub fn seed_workspace_with_id(&mut self, workspace_id: Uuid) -> Uuid {
        assert!(
            !workspace_id.is_nil(),
            "fixture workspace id must be non-nil"
        );
        self.extra_workspace_ids.push(workspace_id);
        self.admin
            .execute(
                "INSERT INTO control.workspaces(workspace_id,tenant_id,name) \
                 VALUES($1,$2,'operation receipt extra workspace')",
                &[&workspace_id, &self.tenant_id],
            )
            .expect("owner seeds fixture workspace");
        workspace_id
    }

    /// Seeds the current entitlement projection then issues one real maintenance quota window.
    pub fn seed_current_entitlement_and_window(&mut self, limit: i64) {
        let effective = format!(
            "jsonb_build_object('mcp.billable_operations.per_period', jsonb_build_object(\
         'limit',{limit},'period','subscription_period','charge_policy','success_only',\
         'period_start',to_char((clock_timestamp()-interval '1 second') AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),\
         'period_end',to_char((clock_timestamp()+interval '1 hour') AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')))",
        );
        self.admin
        .execute(
            &format!(
                "INSERT INTO control.entitlement_snapshots(tenant_id,effective,source_grant_ids,computed_at) \
                 VALUES($1,{effective},ARRAY[$2]::uuid[],clock_timestamp()) \
                 ON CONFLICT (tenant_id) DO UPDATE SET effective=EXCLUDED.effective, \
                 source_grant_ids=EXCLUDED.source_grant_ids,computed_at=EXCLUDED.computed_at"
            ),
            &[&self.tenant_id, &Uuid::new_v4()],
        )
        .expect("owner seeds projected quota facts");
        self.rt
            .block_on(quota_repo::issue_window(
                &self.maintenance,
                TenantId(self.tenant_id),
            ))
            .expect("maintenance issues quota window");
    }

    /// Seeds one v1 synthetic service credential for this fixture and a current quota window.
    /// The caller supplies only an already-derived test hash; no secret or DSN leaves this fixture.
    pub fn seed_synthetic_service_credential_and_window(
        &mut self,
        scopes: SyntheticCredentialScopes,
        prefix: &str,
        wire: &str,
        key_hash: &[u8],
        limit: i64,
    ) -> SyntheticServiceCredential {
        let credential = self.seed_synthetic_service_credential(scopes, prefix, wire, key_hash);
        self.seed_current_entitlement_and_window(limit);
        credential
    }

    /// Adds an identity without changing the tenant's already-issued quota period.
    pub fn seed_synthetic_service_credential(
        &mut self,
        scopes: SyntheticCredentialScopes,
        prefix: &str,
        wire: &str,
        key_hash: &[u8],
    ) -> SyntheticServiceCredential {
        let api_key_id = Uuid::new_v4();
        let scopes = scopes.sql_array();
        self.admin
            .execute(
                &format!(
                    "INSERT INTO control.api_keys \
                 (api_key_id,tenant_id,prefix,key_hash,status,scopes,authorization_version, \
                  user_id,workspace_id,tenant_security_epoch,user_security_epoch) \
                 VALUES($1,$2,$3,$4,'ACTIVE',{scopes},1,$5,$6,0,0)"
                ),
                &[
                    &api_key_id,
                    &self.tenant_id,
                    &prefix,
                    &key_hash,
                    &self.user_id,
                    &self.workspace_id,
                ],
            )
            .expect("owner seeds isolated synthetic service credential");
        // ADR-0035 (card 13): a PAT bound to (user_id, workspace_id) needs the matching ACTIVE
        // WorkspaceMembership to authenticate (a tenant membership no longer grants any workspace).
        // Ensure it for whichever workspace this credential binds to — `with_workspace` may have
        // pointed self.workspace_id at a freshly seeded workspace. Idempotent for the default one.
        self.admin
            .execute(
                "INSERT INTO control.workspace_memberships(tenant_id,workspace_id,user_id,role,state) \
                 VALUES($1,$2,$3,'MEMBER','ACTIVE') ON CONFLICT DO NOTHING",
                &[&self.tenant_id, &self.workspace_id, &self.user_id],
            )
            .expect("owner seeds credential workspace membership");
        SyntheticServiceCredential {
            api_key_id,
            bearer: format!("Bearer {wire}"),
        }
    }

    /// Marks a fixture-created credential revoked. Runtime code retains no UPDATE/DELETE grant.
    pub fn revoke_service_credential(&mut self, credential: &SyntheticServiceCredential) {
        self.admin
            .execute(
                "UPDATE control.api_keys SET status='REVOKED',revoked_at=clock_timestamp() WHERE api_key_id=$1",
                &[&credential.api_key_id],
            )
            .expect("owner revokes isolated synthetic credential");
    }

    /// Seeds a `WORKSPACE_SHARED` memory and matching workspace-scoped mandatory binding.
    /// This is owner-only fixture setup; requests still use the gateway runtime identity.
    pub fn seed_workspace_visible_context_record(&mut self) -> ScopedContextRecord {
        let mut txn = self
            .admin
            .transaction()
            .expect("begin scoped context fixture seed");
        let evidence_id: Uuid = txn
            .query_one(
                r#"INSERT INTO private.evidence_objects
                   (tenant_id,evidence_kind,payload_sha256,data_class,origin_class,visibility_class,visibility_workspace_id,reasoning_domain_id)
                 VALUES($1,'EVENT',$2,'INTERNAL','DirectUserInput','WORKSPACE_SHARED',$3,$4)
                 RETURNING evidence_id"#,
                &[
                    &self.tenant_id,
                    &vec![3_u8; 32],
                    &self.workspace_id,
                    &self.reasoning_domain_id,
                ],
            )
            .expect("owner seeds workspace evidence")
            .get(0);
        let confidence: f32 = 0.9;
        let memory_id: Uuid = txn
            .query_one(
                r#"INSERT INTO private.memory_records
                   (tenant_id,memory_type,content,visibility_class,visibility_workspace_id,authority_class,confidence,status,asserted_at)
                 VALUES($1,'NOTE',$2,'WORKSPACE_SHARED',$3,'ProjectConstraint',$4,'active',clock_timestamp())
                 RETURNING memory_id"#,
                &[
                    &self.tenant_id,
                    &serde_json::json!({"fixture": "operation receipt scoped context"}),
                    &self.workspace_id,
                    &confidence,
                ],
            )
            .expect("owner seeds workspace-visible memory")
            .get(0);
        txn.execute(
            r#"INSERT INTO private.memory_evidence(memory_id,evidence_id,role,grounding_mode)
             VALUES($1,$2,'PRIMARY','SNAPSHOT')"#,
            &[&memory_id, &evidence_id],
        )
        .expect("owner links context memory to evidence");
        let binding_id: Uuid = txn
            .query_one(
                r#"INSERT INTO private.context_bindings
                   (tenant_id,memory_id,mode,scope_kind,scope_id,created_by)
                 VALUES($1,$2,'MANDATORY','WORKSPACE',$3,$4) RETURNING context_binding_id"#,
                &[
                    &self.tenant_id,
                    &memory_id,
                    &self.workspace_id,
                    &self.user_id,
                ],
            )
            .expect("owner seeds workspace-scoped mandatory binding")
            .get(0);
        txn.commit().expect("commit scoped context fixture seed");
        ScopedContextRecord {
            memory_id,
            evidence_id,
            binding_id,
        }
    }

    /// Reasserts that any additional lock-holder connection is an actual unprivileged gateway
    /// LOGIN, never `SET ROLE` on the owner DSN.
    pub fn assert_gateway_login(&self) {
        let mut gateway = self.gateway_client().expect("connect actual gateway login");
        let valid: bool = gateway
            .query_one(
                "SELECT current_user='role_gateway' AND session_user='role_gateway' \
                 AND NOT (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user)",
                &[],
            )
            .expect("inspect actual gateway role")
            .get(0);
        assert!(
            valid,
            "fixture requires actual unprivileged role_gateway LOGIN"
        );
    }

    /// Opens a second real gateway-login client for a lock-holder test; it never derives a role
    /// or password from the owner connection.
    pub(crate) fn gateway_client(&self) -> Result<Client, postgres::Error> {
        // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
        Client::connect(&self.gateway_dsn, NoTls)
    }

    /// Returns the validated gateway login only to an isolated child-process test.
    /// The process builder passes it directly to `Command` and never logs it.
    pub(crate) fn gateway_dsn_for_process(&self) -> &str {
        &self.gateway_dsn
    }

    /// Builds a test-only actor DSN from the already-validated gateway credential, adding only
    /// an application name so PostgreSQL lock diagnostics can identify the actor.
    pub(crate) fn gateway_application_dsn(&self, application_name: &str) -> String {
        format!("{}?application_name={application_name}", self.gateway_dsn)
    }

    /// Connects the already-validated gateway credential through a test-local TCP proxy.
    /// `RuntimeDbPool::connect` repeats the literal role_gateway check after startup; this helper
    /// never exposes or logs the credential URI.
    pub(crate) async fn runtime_via_loopback_proxy(
        &self,
        proxy_port: u16,
    ) -> Result<RuntimeDbPool, String> {
        if proxy_port == 0 {
            return Err("loopback proxy port must be nonzero".into());
        }
        // Split at the (already validated, host == 127.0.0.1) authority, whatever its port.
        let (credential, rest) = self
            .gateway_dsn
            .rsplit_once('@')
            .ok_or_else(|| "validated gateway DSN lost approved authority".to_owned())?;
        let database = rest
            .split_once('/')
            .map(|(_, db)| db)
            .ok_or_else(|| "validated gateway DSN lost its database".to_owned())?;
        let proxied = format!("{credential}@127.0.0.1:{proxy_port}/{database}?sslmode=disable");
        // dep: PostgreSQL(role_gateway) — test opens a direct PG connection for setup/verification
        RuntimeDbPool::connect(&proxied)
            .await
            .map_err(|_| "loopback proxy gateway login rejected".to_owned())
    }

    /// Opens another checked role_gateway pool without exposing fixture credentials to a test.
    pub(crate) async fn fresh_runtime(&self) -> Result<RuntimeDbPool, String> {
        // dep: PostgreSQL(role_gateway) — test opens a direct PG connection for setup/verification
        RuntimeDbPool::connect(&self.gateway_dsn)
            .await
            .map_err(|_| "fresh gateway login rejected".to_owned())
    }

    /// Opens a second owner client for relation-lock tests; this is fixture teardown identity,
    /// never a request-path identity.
    pub(crate) fn owner_client(&self) -> Result<Client, postgres::Error> {
        // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
        Client::connect(&self.admin_dsn, NoTls)
    }
}
