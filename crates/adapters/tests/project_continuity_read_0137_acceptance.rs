//! `adapters::tests::project_continuity_read_0137_acceptance` — Hostile-state acceptance for 0137 continuity reads: ACL, malformed authority context and storage damage all fail closed.
//! Depends-on: crates=[humaux-domain, postgres, serde_json, uuid]; services=[PostgreSQL(any) r=[control.tenants,
//!   ops.outbox, private.continuity_projects] w=[private.continuity_facet_memory_links,
//!   private.continuity_facet_slots, private.continuity_facet_versions, private.evidence_objects,
//!   private.memory_evidence, private.memory_records] x=[private.read_continuity_project_storage_v1]];
//!   env=[HUMAUX_CONTINUITY_RESTART_PHASE, HUMAUX_CONTINUITY_RESTART_STATE];
//!   modules=[adapters::tests::support::continuity_0137_fixture, domain::error]
//! Called-by: [cargo-test]
//! Invariants: [the live role ACL is exact and read-only attempts fail; malformed authority context, structural
//!   damage and array mispairing all fail closed as CannotEstablishCompleteness, never current/complete]
//! Spec: Baseline §25.3.1; §79.2
//!
#[path = "support/continuity_0137_fixture.rs"]
mod continuity_0137_fixture;

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::Path,
    thread,
    time::{Duration, Instant},
};

use continuity_0137_fixture::{
    CleanupOwner, Fixture, assert_diagnostic_pure_cases, preflight_test, required_dsn, set_context,
};
use humaux_domain::error::ErrorCode;
use postgres::{Client, Error, NoTls, types::ToSql};
use serde_json::{Value, json};
use uuid::Uuid;

const READ_FUNCTION: &str =
    "private.read_continuity_project_storage_v1(uuid,uuid,uuid,uuid,uuid,uuid[])";
const POLICY_DDL_LOCK_KEY: i64 = 13_720_260_831;

fn sql_suffix(fixture: &Fixture) -> String {
    fixture.project.simple().to_string()
}

fn lock_policy_ddl(admin: &mut Client) {
    admin
        .query_one("SELECT pg_advisory_lock($1)", &[&POLICY_DDL_LOCK_KEY])
        .unwrap();
}

fn unlock_policy_ddl(admin: &mut Client) {
    assert!(
        admin
            .query_one("SELECT pg_advisory_unlock($1)", &[&POLICY_DDL_LOCK_KEY])
            .unwrap()
            .get::<_, bool>(0)
    );
}

fn state(error: &Error) -> Option<&str> {
    error.code().map(postgres::error::SqlState::code)
}

fn goal(value: &Value) -> &Value {
    value["facets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|facet| facet["kind"] == "GOAL")
        .expect("GOAL facet")
}

fn assert_goal(value: &Value, status: &str, diagnostic: Option<&str>) {
    let facet = goal(value);
    assert_eq!(facet["status"], status);
    if let Some(expected) = diagnostic {
        assert_eq!(facet["diagnostic_code"], expected);
        assert!(facet.get("current").is_none(), "non-current payload leaked");
    }
}

fn protected_trigger_identity(admin: &mut Client) -> Vec<(String, String)> {
    admin
        .query(
            "SELECT n.nspname || '.' || c.relname || '.' || t.tgname, t.tgenabled::text \
             FROM pg_trigger t \
             JOIN pg_class c ON c.oid=t.tgrelid \
             JOIN pg_namespace n ON n.oid=c.relnamespace \
             WHERE NOT t.tgisinternal AND n.nspname='private' AND t.tgname IN ( \
                 'continuity_project_mutation_guard', \
                 'continuity_facet_versions_append_only', \
                 'continuity_facet_memory_links_append_only', \
                 'continuity_facet_evidence_links_append_only', \
                 'continuity_facet_version_source_required') \
             ORDER BY 1",
            &[],
        )
        .unwrap()
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

fn expect_completeness_error(fixture: &Fixture) {
    assert_eq!(
        fixture.read().unwrap_err(),
        ErrorCode::CannotEstablishCompleteness
    );
}

fn error_in_savepoint(client: &mut Client, sql: &str, params: &[&(dyn ToSql + Sync)]) -> String {
    client.batch_execute("SAVEPOINT acceptance_fault").unwrap();
    let error = client.execute(sql, params).unwrap_err();
    let code = state(&error).unwrap_or("non-database-error").to_owned();
    client
        .batch_execute("ROLLBACK TO SAVEPOINT acceptance_fault; RELEASE SAVEPOINT acceptance_fault")
        .unwrap();
    code
}

struct BarrierGuard {
    fixture: Fixture,
    object_id: Uuid,
    table: String,
    function: String,
    memory_policy: String,
    evidence_policy: String,
    key: i64,
}

impl BarrierGuard {
    fn install(fixture: &Fixture, object_id: Uuid) -> Self {
        let suffix = sql_suffix(fixture);
        let guard = Self {
            fixture: fixture.clone(),
            object_id,
            table: format!("continuity_w2_barrier_{suffix}"),
            function: format!("continuity_w2_wait_{suffix}"),
            memory_policy: format!("continuity_w2_memory_wait_{suffix}"),
            evidence_policy: format!("continuity_w2_evidence_wait_{suffix}"),
            key: fixture.project.as_u128() as i64,
        };
        let mut admin = fixture.admin();
        lock_policy_ddl(&mut admin);
        let objects = format!(
            "CREATE TABLE private.{table}( \
               tenant_id uuid NOT NULL,object_id uuid NOT NULL,lock_key bigint NOT NULL, \
               PRIMARY KEY(tenant_id,object_id)); \
             CREATE FUNCTION private.{function}(p_tenant_id uuid,p_object_id uuid) \
             RETURNS boolean LANGUAGE plpgsql VOLATILE SECURITY DEFINER \
             SET search_path TO pg_catalog AS $$ \
             DECLARE v_key bigint; \
             BEGIN \
               SELECT barrier.lock_key INTO v_key FROM private.{table} barrier \
                WHERE barrier.tenant_id=p_tenant_id AND barrier.object_id=p_object_id; \
               IF v_key IS NOT NULL THEN PERFORM pg_advisory_xact_lock(v_key); END IF; \
               RETURN true; \
             END $$; \
             REVOKE ALL ON FUNCTION private.{function}(uuid,uuid) FROM PUBLIC; \
             GRANT EXECUTE ON FUNCTION private.{function}(uuid,uuid) TO role_gateway;",
            table = guard.table,
            function = guard.function,
        );
        let identities = guard.object_identities();
        fixture
            .execute_ddl_batch(&mut admin, "barrier.install.objects", &identities, &objects)
            .unwrap();
        let memory_policy = format!(
            "CREATE POLICY {} ON private.memory_records \
             AS RESTRICTIVE FOR SELECT TO role_gateway \
             USING (private.{}(tenant_id,memory_id));",
            guard.memory_policy, guard.function,
        );
        fixture
            .execute_ddl_batch(
                &mut admin,
                "barrier.install.memory_policy",
                &identities,
                &memory_policy,
            )
            .unwrap();
        let evidence_policy = format!(
            "CREATE POLICY {} ON private.evidence_objects \
             AS RESTRICTIVE FOR SELECT TO role_gateway \
             USING (private.{}(tenant_id,evidence_id));",
            guard.evidence_policy, guard.function,
        );
        fixture
            .execute_ddl_batch(
                &mut admin,
                "barrier.install.evidence_policy",
                &identities,
                &evidence_policy,
            )
            .unwrap();
        admin
            .execute(
                &format!(
                    "INSERT INTO private.{}(tenant_id,object_id,lock_key) VALUES($1,$2,$3)",
                    guard.table
                ),
                &[&fixture.tenant, &object_id, &guard.key],
            )
            .unwrap();
        unlock_policy_ddl(&mut admin);
        guard
    }

    fn object_identities(&self) -> Value {
        json!({
            "tenant_id": self.fixture.tenant.to_string(),
            "project_id": self.fixture.project.to_string(),
            "barrier_object_id": self.object_id.to_string(),
            "advisory_lock_key": self.key,
            "table": format!("private.{}", self.table),
            "function": format!("private.{}(uuid,uuid)", self.function),
            "memory_policy": format!("private.memory_records/{}", self.memory_policy),
            "evidence_policy": format!("private.evidence_objects/{}", self.evidence_policy),
        })
    }

    fn drop_objects(&self, admin: &mut Client) {
        let identities = self.object_identities();
        let memory_policy = format!(
            "DROP POLICY IF EXISTS {} ON private.memory_records;",
            self.memory_policy,
        );
        self.fixture
            .execute_ddl_batch(
                admin,
                "barrier.drop.memory_policy",
                &identities,
                &memory_policy,
            )
            .unwrap();
        let evidence_policy = format!(
            "DROP POLICY IF EXISTS {} ON private.evidence_objects;",
            self.evidence_policy,
        );
        self.fixture
            .execute_ddl_batch(
                admin,
                "barrier.drop.evidence_policy",
                &identities,
                &evidence_policy,
            )
            .unwrap();
        let objects = format!(
            "DROP FUNCTION IF EXISTS private.{}(uuid,uuid); \
             DROP TABLE IF EXISTS private.{};",
            self.function, self.table,
        );
        self.fixture
            .execute_ddl_batch(admin, "barrier.drop.objects", &identities, &objects)
            .unwrap();
    }

    fn lock(&self, admin: &mut Client) {
        admin
            .query_one("SELECT pg_advisory_lock($1)", &[&self.key])
            .unwrap();
    }

    fn unlock(&self, admin: &mut Client) {
        assert!(
            admin
                .query_one("SELECT pg_advisory_unlock($1)", &[&self.key])
                .unwrap()
                .get::<_, bool>(0)
        );
    }
}

impl Drop for BarrierGuard {
    fn drop(&mut self) {
        if let Ok(mut admin) = self.fixture.try_admin() {
            lock_policy_ddl(&mut admin);
            self.drop_objects(&mut admin);
            unlock_policy_ddl(&mut admin);
        }
    }
}

struct FaultGuard {
    fixture: Fixture,
    evidence_id: Uuid,
    function: String,
    policy: String,
}

impl FaultGuard {
    fn install_evidence_association(fixture: &Fixture, evidence_id: Uuid) -> Self {
        let suffix = sql_suffix(fixture);
        let guard = Self {
            fixture: fixture.clone(),
            evidence_id,
            function: format!("continuity_w2_fault_{suffix}"),
            policy: format!("continuity_w2_outbox_fault_{suffix}"),
        };
        let function = format!(
            "CREATE FUNCTION private.{function}(p_tenant_id uuid,p_evidence_id uuid) \
             RETURNS boolean LANGUAGE plpgsql VOLATILE SECURITY DEFINER \
             SET search_path TO pg_catalog AS $$ \
             BEGIN \
               IF p_tenant_id IS NOT DISTINCT FROM '{tenant}'::uuid \
                  AND p_evidence_id IS NOT DISTINCT FROM '{evidence}'::uuid THEN \
                 RAISE EXCEPTION 'directed continuity W2 evidence fault' USING ERRCODE='P0001'; \
               END IF; \
               RETURN true; \
             END $$; \
             REVOKE ALL ON FUNCTION private.{function}(uuid,uuid) FROM PUBLIC; \
             GRANT EXECUTE ON FUNCTION private.{function}(uuid,uuid) TO role_gateway;",
            function = guard.function,
            tenant = fixture.tenant,
            evidence = evidence_id,
        );
        let mut admin = fixture.admin();
        lock_policy_ddl(&mut admin);
        let identities = guard.object_identities();
        fixture
            .execute_ddl_batch(&mut admin, "fault.install.function", &identities, &function)
            .unwrap();
        let policy = format!(
            "CREATE POLICY {} ON ops.outbox AS RESTRICTIVE FOR SELECT TO role_gateway \
             USING (private.{}(tenant_id,evidence_id));",
            guard.policy, guard.function,
        );
        fixture
            .execute_ddl_batch(&mut admin, "fault.install.policy", &identities, &policy)
            .unwrap();
        unlock_policy_ddl(&mut admin);
        guard
    }

    fn object_identities(&self) -> Value {
        json!({
            "tenant_id": self.fixture.tenant.to_string(),
            "evidence_id": self.evidence_id.to_string(),
            "function": format!("private.{}(uuid,uuid)", self.function),
            "policy": format!("ops.outbox/{}", self.policy),
        })
    }

    fn drop_objects(&self, admin: &mut Client) {
        let identities = self.object_identities();
        let policy = format!("DROP POLICY IF EXISTS {} ON ops.outbox;", self.policy,);
        self.fixture
            .execute_ddl_batch(admin, "fault.drop.policy", &identities, &policy)
            .unwrap();
        let function = format!(
            "DROP FUNCTION IF EXISTS private.{}(uuid,uuid);",
            self.function,
        );
        self.fixture
            .execute_ddl_batch(admin, "fault.drop.function", &identities, &function)
            .unwrap();
    }
}

impl Drop for FaultGuard {
    fn drop(&mut self) {
        if let Ok(mut admin) = self.fixture.try_admin() {
            lock_policy_ddl(&mut admin);
            self.drop_objects(&mut admin);
            unlock_policy_ddl(&mut admin);
        }
    }
}

fn waiter_pid(fixture: &Fixture, admin: &mut Client, application_name: &str) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline {
        if let Some(row) = admin
            .query_opt(
                "SELECT pid FROM pg_stat_activity \
                 WHERE application_name=$1 AND wait_event_type='Lock' AND wait_event='advisory' \
                 ORDER BY query_start LIMIT 1",
                &[&application_name],
            )
            .unwrap()
        {
            let pid = row.get(0);
            fixture.record_backend("gw", application_name, pid);
            return pid;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("adapter did not reach source-revalidation advisory barrier");
}

fn assert_read_only_attempt(fixture: &Fixture, admin: &mut Client) {
    let table = format!("continuity_w2_readonly_probe_{}", sql_suffix(fixture));
    let identities = json!({
        "tenant_id": fixture.tenant.to_string(),
        "project_id": fixture.project.to_string(),
        "table": format!("public.{table}"),
    });
    let create = format!(
        "CREATE TABLE public.{table}(value integer NOT NULL); \
         INSERT INTO public.{table} VALUES(1); \
         GRANT SELECT,UPDATE ON public.{table} TO role_gateway;"
    );
    fixture
        .execute_ddl_batch(
            admin,
            "read_only_attempt.create_insert_grant",
            &identities,
            &create,
        )
        .unwrap();
    let mut gateway = fixture.gateway("continuity_w2_readonly_attempt");
    gateway
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .unwrap();
    let error = gateway
        .execute(&format!("UPDATE public.{table} SET value=2"), &[])
        .unwrap_err();
    assert_eq!(state(&error), Some("25006"));
    let _ = gateway.batch_execute("ROLLBACK");
    let drop_table = format!("DROP TABLE public.{table}");
    fixture
        .execute_ddl_batch(
            admin,
            "read_only_attempt.drop_table",
            &identities,
            &drop_table,
        )
        .unwrap();
}

fn continuity_counts(fixture: &Fixture, admin: &mut Client) -> (i64, i64, i64) {
    let row = admin
        .query_one(
            "SELECT \
               (SELECT count(*) FROM private.continuity_facet_versions WHERE project_id=$1), \
               (SELECT count(*) FROM private.continuity_facet_slots WHERE project_id=$1), \
               (SELECT count(*) FROM ops.outbox WHERE tenant_id=$2)",
            &[&fixture.project, &fixture.tenant],
        )
        .unwrap();
    (row.get(0), row.get(1), row.get(2))
}

fn direct_reader_error(
    fixture: &Fixture,
    context: [String; 4],
    tenant: Uuid,
    principal: Uuid,
    user: Option<Uuid>,
    authorized: Option<Vec<Uuid>>,
) -> String {
    let mut gateway = fixture.gateway("continuity_w2_malformed_authority");
    gateway.batch_execute("BEGIN").unwrap();
    gateway
        .query_one(
            "SELECT set_config('humaux.tenant_id',$1,true), \
                    set_config('humaux.workspace_id',$2,true), \
                    set_config('humaux.principal_id',$3,true), \
                    set_config('humaux.user_id',$4,true)",
            &[&context[0], &context[1], &context[2], &context[3]],
        )
        .unwrap();
    let error = gateway
        .query(
            "SELECT * FROM private.read_continuity_project_storage_v1($1,$2,NULL,$3,$4,$5)",
            &[&tenant, &fixture.project, &principal, &user, &authorized],
        )
        .unwrap_err();
    let code = state(&error).unwrap_or("non-database-error").to_owned();
    gateway.batch_execute("ROLLBACK").unwrap();
    code
}

#[test]
fn live_role_acl_and_read_only_attempt_are_exact() {
    preflight_test("live_role_acl_and_read_only_attempt_are_exact");
    let Some(dsn) = required_dsn() else { return };
    let fixture = Fixture::new(dsn.clone(), "live_role_acl_and_read_only_attempt_are_exact");
    let roles = [
        "role_gateway",
        "role_private_worker",
        "role_consolidation_worker",
        "role_public_worker",
        "role_retrieval_worker",
        "role_batch_issuer",
        "role_maintenance",
    ];
    let mut admin = fixture.admin();
    for role in roles {
        let password = format!("devlocal_{role}");
        let mut client = fixture
            .try_gateway_as(role, &password, "continuity_w2_live_acl")
            .unwrap_or_else(|error| panic!("{role} must be a live LOGIN role: {error}"));
        let actual: String = client.query_one("SELECT current_user", &[]).unwrap().get(0);
        assert_eq!(actual, role);
        let direct = client
            .query_one("SELECT count(*) FROM private.continuity_projects", &[])
            .unwrap_err();
        assert_eq!(state(&direct), Some("42501"), "direct read {role}");
        if role == "role_gateway" {
            client.batch_execute("BEGIN").unwrap();
            set_context(
                &mut client,
                fixture.tenant,
                Uuid::nil(),
                fixture.principal,
                Some(fixture.user),
            );
            let count = client
                .query(
                    "SELECT * FROM private.read_continuity_project_storage_v1($1,$2,NULL,$3,$4,$5)",
                    &[
                        &fixture.tenant,
                        &fixture.project,
                        &fixture.principal,
                        &Some(fixture.user),
                        &vec![fixture.workspace],
                    ],
                )
                .unwrap()
                .len();
            assert_eq!(count, 15);
            client.batch_execute("ROLLBACK").unwrap();
        } else {
            let denied = client
                .query(
                    "SELECT * FROM private.read_continuity_project_storage_v1($1,$2,NULL,$3,$4,$5)",
                    &[
                        &fixture.tenant,
                        &fixture.project,
                        &fixture.principal,
                        &Some(fixture.user),
                        &vec![fixture.workspace],
                    ],
                )
                .unwrap_err();
            assert_eq!(state(&denied), Some("42501"), "function execute {role}");
        }
    }
    let gateway_exec: bool = admin
        .query_one(
            "SELECT has_function_privilege('role_gateway',$1,'EXECUTE')",
            &[&READ_FUNCTION],
        )
        .unwrap()
        .get(0);
    assert!(gateway_exec);
    for role in &roles[1..] {
        let allowed: bool = admin
            .query_one(
                "SELECT has_function_privilege($1,$2,'EXECUTE')",
                &[role, &READ_FUNCTION],
            )
            .unwrap()
            .get(0);
        assert!(!allowed, "unexpected EXECUTE for {role}");
    }

    assert_read_only_attempt(&fixture, &mut admin);
}

#[test]
fn malformed_authority_context_and_workspace_arrays_fail_closed() {
    preflight_test("malformed_authority_context_and_workspace_arrays_fail_closed");
    let Some(dsn) = required_dsn() else { return };
    let fixture = Fixture::new(
        dsn,
        "malformed_authority_context_and_workspace_arrays_fail_closed",
    );
    let mut admin = fixture.admin();
    let before = continuity_counts(&fixture, &mut admin);
    let correct = || {
        [
            fixture.tenant.to_string(),
            Uuid::nil().to_string(),
            fixture.principal.to_string(),
            fixture.user.to_string(),
        ]
    };
    let valid = Some(vec![fixture.workspace]);

    let mut context_cases = Vec::new();
    for (index, value) in [
        (0, "not-a-tenant-uuid"),
        (1, "not-a-workspace-uuid"),
        (2, "not-a-principal-uuid"),
        (3, "not-a-user-uuid"),
    ] {
        let mut context = correct();
        context[index] = value.to_owned();
        context_cases.push(context);
    }
    let mut tenant_mismatch = correct();
    tenant_mismatch[0] = fixture.other_workspace.to_string();
    context_cases.push(tenant_mismatch);
    let mut workspace_not_nil = correct();
    workspace_not_nil[1] = fixture.workspace.to_string();
    context_cases.push(workspace_not_nil);
    let mut principal_mismatch = correct();
    principal_mismatch[2] = fixture.other_workspace.to_string();
    context_cases.push(principal_mismatch);
    let mut user_mismatch = correct();
    user_mismatch[3] = fixture.other_user.to_string();
    context_cases.push(user_mismatch);

    for context in context_cases {
        assert_eq!(
            direct_reader_error(
                &fixture,
                context,
                fixture.tenant,
                fixture.principal,
                Some(fixture.user),
                valid.clone(),
            ),
            "42501"
        );
    }

    let mut descending = vec![fixture.workspace, fixture.other_workspace];
    descending.sort_unstable_by(|left, right| right.cmp(left));
    for authorized in [
        None,
        Some(vec![Uuid::nil()]),
        Some(vec![fixture.workspace, fixture.workspace]),
        Some(descending),
    ] {
        assert_eq!(
            direct_reader_error(
                &fixture,
                correct(),
                fixture.tenant,
                fixture.principal,
                Some(fixture.user),
                authorized,
            ),
            "42501"
        );
    }

    assert_eq!(continuity_counts(&fixture, &mut admin), before);
}

#[test]
fn structural_damage_and_unrepresentable_corruption_fail_closed() {
    preflight_test("structural_damage_and_unrepresentable_corruption_fail_closed");
    let Some(dsn) = required_dsn() else { return };

    let missing = Fixture::new(
        dsn.clone(),
        "structural_damage_and_unrepresentable_corruption_fail_closed",
    );
    missing
        .admin()
        .execute(
            "DELETE FROM private.continuity_facet_slots \
             WHERE tenant_id=$1 AND project_id=$2 AND facet_kind='TESTS'",
            &[&missing.tenant, &missing.project],
        )
        .unwrap();
    expect_completeness_error(&missing);

    let rollback = Fixture::new(
        dsn.clone(),
        "structural_damage_and_unrepresentable_corruption_fail_closed",
    );
    rollback.publish_memory_successor();
    rollback
        .admin()
        .execute(
            "UPDATE private.continuity_facet_slots SET slot_version=1,current_version_id=$3 \
             WHERE tenant_id=$1 AND project_id=$2 AND facet_kind='GOAL'",
            &[&rollback.tenant, &rollback.project, &rollback.goal_v1],
        )
        .unwrap();
    expect_completeness_error(&rollback);

    let orphan = Fixture::new(
        dsn.clone(),
        "structural_damage_and_unrepresentable_corruption_fail_closed",
    );
    orphan
        .admin()
        .execute(
            "UPDATE private.continuity_facet_slots SET slot_version=0,current_version_id=NULL,slot_state=NULL \
             WHERE tenant_id=$1 AND project_id=$2 AND facet_kind='GOAL'",
            &[&orphan.tenant, &orphan.project],
        )
        .unwrap();
    expect_completeness_error(&orphan);

    let guarded = Fixture::new(
        dsn,
        "structural_damage_and_unrepresentable_corruption_fail_closed",
    );
    let mut admin = guarded.admin();
    admin.batch_execute("BEGIN").unwrap();
    let cases = [
        (
            "duplicate slot",
            "INSERT INTO private.continuity_facet_slots(tenant_id,workspace_id,project_id,facet_kind) \
             VALUES($1,$2,$3,'GOAL')",
            "23505",
        ),
        (
            "unknown slot",
            "INSERT INTO private.continuity_facet_slots(tenant_id,workspace_id,project_id,facet_kind) \
             VALUES($1,$2,$3,'UNKNOWN')",
            "23514",
        ),
        (
            "derived slot",
            "INSERT INTO private.continuity_facet_slots(tenant_id,workspace_id,project_id,facet_kind) \
             VALUES($1,$2,$3,'HANDOFF')",
            "23514",
        ),
    ];
    for (name, sql, expected) in cases {
        assert_eq!(
            error_in_savepoint(
                &mut admin,
                sql,
                &[&guarded.tenant, &guarded.workspace, &guarded.project],
            ),
            expected,
            "{name}"
        );
    }
    assert_eq!(
        error_in_savepoint(
            &mut admin,
            "UPDATE private.continuity_facet_versions SET body='{}'::jsonb \
             WHERE facet_version_id=$1",
            &[&guarded.goal_v1],
        ),
        "23514",
        "append-only body/hash closure"
    );
    assert_eq!(
        error_in_savepoint(
            &mut admin,
            "UPDATE private.continuity_facet_memory_links SET memory_sha256=$2 \
             WHERE facet_version_id=$1",
            &[&guarded.goal_v1, &vec![0_u8; 31]],
        ),
        "23514",
        "append-only link/array source closure"
    );
    admin.batch_execute("ROLLBACK").unwrap();
}

#[test]
fn raw_parallel_array_mispairing_never_yields_current_or_complete() {
    preflight_test("raw_parallel_array_mispairing_never_yields_current_or_complete");
    let Some(dsn) = required_dsn() else { return };
    let fixture = Fixture::new(
        dsn,
        "raw_parallel_array_mispairing_never_yields_current_or_complete",
    );
    let second = fixture.create_memory(serde_json::json!({"w2":"second source"}));
    let version = fixture.publish_goal_memories(1, &[fixture.memory, second]);
    let mut admin = fixture.admin();
    let rows = admin
        .query(
            "SELECT memory_id,memory_sha256 \
             FROM private.continuity_facet_memory_links \
             WHERE facet_version_id=$1 ORDER BY memory_id",
            &[&version],
        )
        .unwrap();
    assert_eq!(rows.len(), 2);
    let first_id: Uuid = rows[0].get(0);
    let first_hash: Vec<u8> = rows[0].get(1);
    let second_id: Uuid = rows[1].get(0);
    let second_hash: Vec<u8> = rows[1].get(1);
    assert_ne!(first_hash, second_hash);

    admin
        .batch_execute("SET session_replication_role=replica")
        .unwrap();
    let changed = admin
        .execute(
            "UPDATE private.continuity_facet_memory_links \
             SET memory_sha256=CASE WHEN memory_id=$2 THEN $5 \
                                    WHEN memory_id=$3 THEN $4 ELSE memory_sha256 END \
             WHERE facet_version_id=$1 AND memory_id IN($2,$3)",
            &[&version, &first_id, &second_id, &first_hash, &second_hash],
        )
        .unwrap();
    admin
        .batch_execute("SET session_replication_role=origin")
        .unwrap();
    assert_eq!(changed, 2);

    let value = fixture
        .read()
        .expect("positional corruption is typed stale");
    assert_goal(&value, "STALE", Some("SOURCE_REVALIDATION_FAILED"));
}

#[derive(Clone, Copy, Debug)]
enum MemoryCase {
    ImmutableCurrent,
    SnapshotCurrent,
    Revoked,
    Expired,
    Superseded,
    HashDrift,
    UserVisible,
    UserHidden,
    WorkspaceVisible,
    WorkspaceHidden,
    SecretBacking,
    TombstonedBacking,
    LiveRecheck,
    LiveNotJudged,
}

fn set_memory_visibility(
    fixture: &Fixture,
    admin: &mut Client,
    class: &str,
    user: Option<Uuid>,
    workspace: Option<Uuid>,
) {
    admin
        .execute(
            "UPDATE private.memory_records SET visibility_class=$3, \
             visibility_user_id=$4,visibility_workspace_id=$5 \
             WHERE tenant_id=$1 AND memory_id=$2",
            &[&fixture.tenant, &fixture.memory, &class, &user, &workspace],
        )
        .unwrap();
}

fn make_memory_backing_secret(fixture: &Fixture) {
    fixture
        .admin()
        .execute(
            "UPDATE private.evidence_objects SET data_class='SECRET_MATERIAL' \
             WHERE tenant_id=$1 AND evidence_id=$2",
            &[&fixture.tenant, &fixture.backing_evidence],
        )
        .unwrap();
}

fn apply_memory_case(fixture: &Fixture, case: MemoryCase) -> bool {
    let mut admin = fixture.admin();
    match case {
        MemoryCase::ImmutableCurrent => true,
        MemoryCase::SnapshotCurrent => {
            admin
                .execute(
                    "UPDATE private.memory_evidence SET grounding_mode='SNAPSHOT' \
                     WHERE memory_id=$1",
                    &[&fixture.memory],
                )
                .unwrap();
            true
        }
        MemoryCase::Revoked | MemoryCase::Expired => {
            let status = if matches!(case, MemoryCase::Revoked) {
                "revoked"
            } else {
                "expired"
            };
            admin
                .execute(
                    "UPDATE private.memory_records SET status=$3 \
                     WHERE tenant_id=$1 AND memory_id=$2",
                    &[&fixture.tenant, &fixture.memory, &status],
                )
                .unwrap();
            false
        }
        MemoryCase::Superseded => {
            drop(admin);
            fixture.supersede_memory();
            false
        }
        MemoryCase::HashDrift => {
            admin
                .execute(
                    "UPDATE private.memory_records SET content=$3 \
                     WHERE tenant_id=$1 AND memory_id=$2",
                    &[
                        &fixture.tenant,
                        &fixture.memory,
                        &serde_json::json!({"drift":true}),
                    ],
                )
                .unwrap();
            false
        }
        MemoryCase::UserVisible | MemoryCase::UserHidden => {
            let visible = matches!(case, MemoryCase::UserVisible);
            let user = if visible {
                fixture.user
            } else {
                fixture.other_user
            };
            set_memory_visibility(fixture, &mut admin, "USER_PRIVATE", Some(user), None);
            visible
        }
        MemoryCase::WorkspaceVisible | MemoryCase::WorkspaceHidden => {
            let visible = matches!(case, MemoryCase::WorkspaceVisible);
            let workspace = if visible {
                fixture.workspace
            } else {
                fixture.other_workspace
            };
            set_memory_visibility(
                fixture,
                &mut admin,
                "WORKSPACE_SHARED",
                None,
                Some(workspace),
            );
            visible
        }
        MemoryCase::SecretBacking => {
            drop(admin);
            make_memory_backing_secret(fixture);
            false
        }
        MemoryCase::TombstonedBacking => {
            drop(admin);
            fixture.associate(fixture.backing_evidence, fixture.tenant, "TOMBSTONED");
            false
        }
        MemoryCase::LiveRecheck | MemoryCase::LiveNotJudged => {
            let recorded: Option<&str> = if matches!(case, MemoryCase::LiveNotJudged) {
                Some("resolver-version")
            } else {
                None
            };
            admin
                .execute(
                    "UPDATE private.memory_evidence SET grounding_mode='LIVE',recorded_version=$2 \
                     WHERE memory_id=$1",
                    &[&fixture.memory, &recorded],
                )
                .unwrap();
            false
        }
    }
}

#[test]
fn memory_eligibility_visibility_grounding_and_lifecycle_matrix() {
    preflight_test("memory_eligibility_visibility_grounding_and_lifecycle_matrix");
    let Some(dsn) = required_dsn() else { return };
    let cases = [
        MemoryCase::ImmutableCurrent,
        MemoryCase::SnapshotCurrent,
        MemoryCase::Revoked,
        MemoryCase::Expired,
        MemoryCase::Superseded,
        MemoryCase::HashDrift,
        MemoryCase::UserVisible,
        MemoryCase::UserHidden,
        MemoryCase::WorkspaceVisible,
        MemoryCase::WorkspaceHidden,
        MemoryCase::SecretBacking,
        MemoryCase::TombstonedBacking,
        MemoryCase::LiveRecheck,
        MemoryCase::LiveNotJudged,
    ];
    for case in cases {
        let fixture = Fixture::new(
            dsn.clone(),
            "memory_eligibility_visibility_grounding_and_lifecycle_matrix",
        );
        let current = apply_memory_case(&fixture, case);
        let value = fixture
            .read()
            .unwrap_or_else(|error| panic!("{case:?}: {error:?}"));
        if current {
            assert_goal(&value, "CURRENT", None);
        } else {
            assert_goal(&value, "STALE", Some("SOURCE_REVALIDATION_FAILED"));
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum EvidenceCase {
    AllLive,
    VisibilityHidden,
    Secret,
    HashDrift,
    ZeroAssociation,
    MixedTombstone,
    WrongTenantOnly,
    WrongTenantDecoyIgnored,
}

#[test]
fn direct_evidence_truth_table_is_fail_closed() {
    preflight_test("direct_evidence_truth_table_is_fail_closed");
    let Some(dsn) = required_dsn() else { return };
    let cases = [
        EvidenceCase::AllLive,
        EvidenceCase::VisibilityHidden,
        EvidenceCase::Secret,
        EvidenceCase::HashDrift,
        EvidenceCase::ZeroAssociation,
        EvidenceCase::MixedTombstone,
        EvidenceCase::WrongTenantOnly,
        EvidenceCase::WrongTenantDecoyIgnored,
    ];
    for case in cases {
        let fixture = Fixture::new(dsn.clone(), "direct_evidence_truth_table_is_fail_closed");
        let associated = !matches!(
            case,
            EvidenceCase::ZeroAssociation | EvidenceCase::WrongTenantOnly
        );
        let evidence = fixture.publish_evidence_successor(associated);
        let mut admin = fixture.admin();
        let expected = match case {
            EvidenceCase::AllLive => ("CURRENT", None),
            EvidenceCase::VisibilityHidden => {
                admin
                    .execute(
                        "UPDATE private.evidence_objects SET visibility_class='WORKSPACE_SHARED', \
                         visibility_user_id=NULL,visibility_workspace_id=$3 \
                         WHERE tenant_id=$1 AND evidence_id=$2",
                        &[&fixture.tenant, &evidence, &fixture.other_workspace],
                    )
                    .unwrap();
                ("STALE", Some("VISIBILITY_REVALIDATION_FAILED"))
            }
            EvidenceCase::Secret => {
                admin
                    .execute(
                        "UPDATE private.evidence_objects SET data_class='SECRET_MATERIAL' \
                         WHERE tenant_id=$1 AND evidence_id=$2",
                        &[&fixture.tenant, &evidence],
                    )
                    .unwrap();
                ("STALE", Some("VISIBILITY_REVALIDATION_FAILED"))
            }
            EvidenceCase::HashDrift => {
                admin
                    .execute(
                        "UPDATE private.evidence_objects SET payload_sha256=$3 \
                         WHERE tenant_id=$1 AND evidence_id=$2",
                        &[&fixture.tenant, &evidence, &vec![0x72_u8; 32]],
                    )
                    .unwrap();
                ("STALE", Some("SOURCE_REVALIDATION_FAILED"))
            }
            EvidenceCase::ZeroAssociation => ("STALE", Some("SOURCE_REVALIDATION_FAILED")),
            EvidenceCase::MixedTombstone => {
                drop(admin);
                fixture.associate(evidence, fixture.tenant, "TOMBSTONED");
                ("STALE", Some("SOURCE_REVALIDATION_FAILED"))
            }
            EvidenceCase::WrongTenantOnly => {
                let decoy = fixture.create_decoy_tenant();
                fixture.associate(evidence, decoy, "DONE");
                ("STALE", Some("SOURCE_REVALIDATION_FAILED"))
            }
            EvidenceCase::WrongTenantDecoyIgnored => {
                let decoy = fixture.create_decoy_tenant();
                fixture.associate(evidence, decoy, "TOMBSTONED");
                ("CURRENT", None)
            }
        };
        let value = fixture
            .read()
            .unwrap_or_else(|error| panic!("{case:?}: {error:?}"));
        assert_goal(&value, expected.0, expected.1);
    }
}

#[test]
fn evidence_association_query_failure_is_dependency_unavailable() {
    preflight_test("evidence_association_query_failure_is_dependency_unavailable");
    let Some(dsn) = required_dsn() else { return };
    let fixture = Fixture::new(
        dsn.clone(),
        "evidence_association_query_failure_is_dependency_unavailable",
    );
    let evidence = fixture.publish_evidence_successor(true);
    let guard = FaultGuard::install_evidence_association(&fixture, evidence);
    assert_eq!(
        fixture.read().unwrap_err(),
        ErrorCode::DependencyUnavailable
    );

    let unrelated = Fixture::new(
        dsn,
        "evidence_association_query_failure_is_dependency_unavailable",
    );
    assert_goal(&unrelated.read().unwrap(), "CURRENT", None);
    drop(guard);
    assert_goal(&fixture.read().unwrap(), "CURRENT", None);
}

#[test]
#[allow(clippy::too_many_lines)]
fn directed_fault_guard_panic_cleanup_preserves_global_acl() {
    preflight_test("directed_fault_guard_panic_cleanup_preserves_global_acl");
    let Some(dsn) = required_dsn() else { return };
    let partial_tenant = Uuid::now_v7();
    let partial_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe({
        let dsn = dsn.clone();
        move || Fixture::panic_after_partial_seed(dsn, partial_tenant)
    }));
    assert!(partial_panic.is_err());
    // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
    let mut partial_admin = Client::connect(&dsn, NoTls).expect("partial-seed census connect");
    let partial_remaining: i64 = partial_admin
        .query_one(
            "SELECT count(*) FROM control.tenants WHERE tenant_id=$1",
            &[&partial_tenant],
        )
        .unwrap()
        .get(0);
    assert_eq!(partial_remaining, 0, "partial constructor seed leaked");
    drop(partial_admin);
    let fixture = Fixture::new(
        dsn,
        "directed_fault_guard_panic_cleanup_preserves_global_acl",
    );
    let evidence = fixture.publish_evidence_successor(true);
    let decoy_tenant = fixture.create_decoy_tenant();
    let target_ledger = fixture.restart_state()["cleanup_ledger"].clone();
    let target_tenant = fixture.tenant;
    let target_admin_dsn = fixture.admin_dsn.clone();
    let unrelated = Fixture::new(
        fixture.admin_dsn.clone(),
        "directed_fault_guard_panic_cleanup_preserves_global_acl",
    );
    let mut admin = fixture.admin();
    let acl_before: Option<String> = admin
        .query_one(
            "SELECT relacl::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace \
             WHERE n.nspname='ops' AND c.relname='outbox'",
            &[],
        )
        .unwrap()
        .get(0);
    let trigger_before = protected_trigger_identity(&mut admin);
    let guard = FaultGuard::install_evidence_association(&fixture, evidence);
    let function = guard.function.clone();
    let policy = guard.policy.clone();
    let fixture_clone = fixture.clone();
    drop(fixture_clone);
    let clone_surviving_rows: i64 = admin
        .query_one(
            "SELECT count(*) FROM control.tenants WHERE tenant_id=$1",
            &[&fixture.tenant],
        )
        .unwrap()
        .get(0);
    assert_eq!(
        clone_surviving_rows, 1,
        "non-final Fixture clone cleaned early"
    );
    let target_fixture = fixture;
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _fixture = target_fixture;
        let _guard = guard;
        panic!("forced RAII cleanup probe");
    }));
    assert!(panic.is_err());

    drop(admin);
    let mut admin =
        // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
        Client::connect(&target_admin_dsn, NoTls).expect("unwind cleanup census connect");
    let residues: (i64, i64) = {
        let row = admin
            .query_one(
                "SELECT \
                   (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace \
                     WHERE n.nspname='private' AND p.proname=$1), \
                   (SELECT count(*) FROM pg_policies WHERE schemaname='ops' \
                     AND tablename='outbox' AND policyname=$2)",
                &[&function, &policy],
            )
            .unwrap();
        (row.get(0), row.get(1))
    };
    let acl_after: Option<String> = admin
        .query_one(
            "SELECT relacl::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace \
             WHERE n.nspname='ops' AND c.relname='outbox'",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(residues, (0, 0));
    let census_owner = CleanupOwner::from_state(target_admin_dsn.clone(), &target_ledger)
        .expect("exact cleanup ledger census");
    let counts = census_owner.counts().expect("fixture cleanup census");
    assert_eq!(counts.len(), 15);
    assert!(
        counts.iter().all(|count| *count == 0),
        "residual fixture rows: {counts:?}"
    );
    assert_eq!(acl_after, acl_before);
    let trigger_after = protected_trigger_identity(&mut admin);
    assert_eq!(trigger_before.len(), 5);
    assert!(trigger_before.iter().all(|(_, enabled)| enabled == "O"));
    assert_eq!(trigger_after, trigger_before);
    assert!(
        admin
            .query_one(
                "SELECT has_table_privilege('role_gateway','ops.outbox','SELECT')",
                &[],
            )
            .unwrap()
            .get::<_, bool>(0)
    );
    assert_goal(&unrelated.read().unwrap(), "CURRENT", None);
    assert_ne!(decoy_tenant, unrelated.tenant);
    let target_rows_after_final_drop: i64 = admin
        .query_one(
            "SELECT count(*) FROM control.tenants WHERE tenant_id=$1",
            &[&target_tenant],
        )
        .unwrap()
        .get(0);
    assert_eq!(target_rows_after_final_drop, 0);
}

#[test]
fn two_connection_rr_revoke_and_tombstone_are_coherent() {
    preflight_test("two_connection_rr_revoke_and_tombstone_are_coherent");
    let Some(dsn) = required_dsn() else { return };

    let memory = Fixture::new(
        dsn.clone(),
        "two_connection_rr_revoke_and_tombstone_are_coherent",
    );
    let guard = BarrierGuard::install(&memory, memory.memory);
    let mut lock = memory.admin();
    guard.lock(&mut lock);
    let app = memory.application_name.clone();
    let worker_fixture = memory.clone();
    let worker = thread::spawn(move || worker_fixture.read());
    let mut observer = memory.admin();
    let _pid = waiter_pid(&memory, &mut observer, &app);
    observer
        .execute(
            "UPDATE private.memory_records SET status='revoked' \
             WHERE tenant_id=$1 AND memory_id=$2",
            &[&memory.tenant, &memory.memory],
        )
        .unwrap();
    guard.unlock(&mut lock);
    let old = worker.join().unwrap().expect("old RR remains coherent");
    assert_goal(&old, "CURRENT", None);
    let next = memory.read().expect("new RR returns a typed stale facet");
    assert_goal(&next, "STALE", Some("SOURCE_REVALIDATION_FAILED"));
    drop(guard);

    let evidence = Fixture::new(dsn, "two_connection_rr_revoke_and_tombstone_are_coherent");
    let evidence_id = evidence.publish_evidence_successor(true);
    let guard = BarrierGuard::install(&evidence, evidence_id);
    let mut lock = evidence.admin();
    guard.lock(&mut lock);
    let app = evidence.application_name.clone();
    let worker_fixture = evidence.clone();
    let worker = thread::spawn(move || worker_fixture.read());
    let mut observer = evidence.admin();
    let _pid = waiter_pid(&evidence, &mut observer, &app);
    evidence.associate(evidence_id, evidence.tenant, "TOMBSTONED");
    guard.unlock(&mut lock);
    let old = worker
        .join()
        .unwrap()
        .expect("old RR ignores later tombstone");
    assert_goal(&old, "CURRENT", None);
    let next = evidence.read().expect("new RR sees tombstone");
    assert_goal(&next, "STALE", Some("SOURCE_REVALIDATION_FAILED"));
}

#[test]
fn terminated_source_revalidation_is_bounded_and_has_no_residue() {
    preflight_test("terminated_source_revalidation_is_bounded_and_has_no_residue");
    let Some(dsn) = required_dsn() else { return };
    let fixture = Fixture::new(
        dsn,
        "terminated_source_revalidation_is_bounded_and_has_no_residue",
    );
    let guard = BarrierGuard::install(&fixture, fixture.memory);
    let mut admin = fixture.admin();
    let before: (i64, i64, i64) = {
        let row = admin
            .query_one(
                "SELECT \
                   (SELECT count(*) FROM private.continuity_facet_versions WHERE project_id=$1), \
                   (SELECT count(*) FROM private.continuity_facet_slots WHERE project_id=$1), \
                   (SELECT count(*) FROM ops.outbox WHERE tenant_id=$2)",
                &[&fixture.project, &fixture.tenant],
            )
            .unwrap();
        (row.get(0), row.get(1), row.get(2))
    };
    guard.lock(&mut admin);
    let started = Instant::now();
    let app = fixture.application_name.clone();
    let worker_fixture = fixture.clone();
    let worker = thread::spawn(move || worker_fixture.read());
    let mut killer = fixture.admin();
    let pid = waiter_pid(&fixture, &mut killer, &app);
    assert!(
        killer
            .query_one("SELECT pg_terminate_backend($1)", &[&pid])
            .unwrap()
            .get::<_, bool>(0)
    );
    guard.unlock(&mut admin);
    assert_eq!(
        worker.join().unwrap().unwrap_err(),
        ErrorCode::DependencyUnavailable
    );
    assert!(started.elapsed() < Duration::from_secs(8));
    let row = killer
        .query_one(
            "SELECT \
               (SELECT count(*) FROM private.continuity_facet_versions WHERE project_id=$1), \
               (SELECT count(*) FROM private.continuity_facet_slots WHERE project_id=$1), \
               (SELECT count(*) FROM ops.outbox WHERE tenant_id=$2)",
            &[&fixture.project, &fixture.tenant],
        )
        .unwrap();
    let after = (row.get(0), row.get(1), row.get(2));
    assert_eq!(after, before);
}

#[test]
fn statement_timeout_is_bounded_rollback_no_partial_no_retry() {
    preflight_test("statement_timeout_is_bounded_rollback_no_partial_no_retry");
    let Some(dsn) = required_dsn() else { return };
    let fixture = Fixture::new(
        dsn,
        "statement_timeout_is_bounded_rollback_no_partial_no_retry",
    );
    let guard = BarrierGuard::install(&fixture, fixture.memory);
    let mut admin = fixture.admin();
    let before = continuity_counts(&fixture, &mut admin);
    guard.lock(&mut admin);

    let mut timed = fixture.clone();
    timed
        .gateway_dsn
        .push_str("&options=-c%20statement_timeout%3D250ms");
    let app = timed.application_name.clone();
    let started = Instant::now();
    let worker = thread::spawn(move || timed.read());
    let mut observer = fixture.admin();
    let pid = waiter_pid(&fixture, &mut observer, &app);
    let active: i64 = observer
        .query_one(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE application_name=$1 AND state='active'",
            &[&app],
        )
        .unwrap()
        .get(0);
    assert_eq!(active, 1, "exactly one request/backend reaches the barrier");
    let result = worker.join().unwrap();
    guard.unlock(&mut admin);

    assert_eq!(result.unwrap_err(), ErrorCode::DependencyUnavailable);
    assert!(started.elapsed() >= Duration::from_millis(150));
    assert!(started.elapsed() < Duration::from_secs(3));
    let remaining: i64 = observer
        .query_one(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE application_name=$1 AND pid<>$2 AND state='active'",
            &[&app, &pid],
        )
        .unwrap()
        .get(0);
    assert_eq!(remaining, 0, "no invisible retry/backend appeared");
    assert_eq!(continuity_counts(&fixture, &mut observer), before);
}

fn semantic_projection(mut value: Value) -> Value {
    value.as_object_mut().unwrap().remove("snapshot");
    let handoff = value["handoff"].as_object_mut().unwrap();
    handoff.remove("context_snapshot_seq");
    handoff.remove("snapshot_token_sha256");
    value
}

#[test]
fn restart_semantic_projection_probe() {
    preflight_test("restart_semantic_projection_probe");
    let Ok(phase) = std::env::var("HUMAUX_CONTINUITY_RESTART_PHASE") else {
        return;
    };
    let Some(dsn) = required_dsn() else { return };
    let state_path = std::env::var("HUMAUX_CONTINUITY_RESTART_STATE")
        .expect("restart probe requires HUMAUX_CONTINUITY_RESTART_STATE");
    match phase.as_str() {
        "seed" => {
            let fixture = Fixture::new(dsn, "restart_semantic_projection_probe");
            let mut state = fixture.restart_state();
            state["expected"] = semantic_projection(fixture.read().unwrap());
            let temp_path = format!("{state_path}.tmp-{}", std::process::id());
            let bytes = serde_json::to_vec_pretty(&state).unwrap();
            let mut temp = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)
                .expect("create restart state temp");
            temp.write_all(&bytes).expect("write restart state temp");
            temp.sync_all().expect("sync restart state temp");
            drop(temp);
            fs::rename(&temp_path, &state_path).expect("publish restart state atomically");
            let parent = Path::new(&state_path)
                .parent()
                .unwrap_or_else(|| Path::new("."));
            File::open(parent)
                .expect("open restart state parent")
                .sync_all()
                .expect("sync restart state parent");
            fixture
                .disarm_cleanup()
                .expect("restart cleanup transfer must have sole Arc owner");
        }
        "verify" => {
            let state: Value = serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
            let fixture = Fixture::from_state(dsn, &state, "restart_semantic_projection_probe");
            fixture.rearm_cleanup();
            assert_eq!(
                semantic_projection(fixture.read().unwrap()),
                state["expected"]
            );
            fixture.cleanup_now().expect("restart verify cleanup");
        }
        other => panic!("unknown restart phase {other}"),
    }
}

#[test]
fn adapter_static_external_and_write_plane_is_zero() {
    preflight_test("adapter_static_external_and_write_plane_is_zero");
    assert!(
        CleanupOwner::from_state(
            "postgres://invalid".to_owned(),
            &json!({
                "tenants": ["not-a-uuid"]
            })
        )
        .is_err()
    );
    assert_diagnostic_pure_cases();
    let source = include_str!("../src/continuity_read.rs");
    for forbidden in ["provider", "cache", "embedding", "rerank", "llm"] {
        assert!(
            !source.to_ascii_lowercase().contains(forbidden),
            "{forbidden}"
        );
    }
    assert_eq!(source.matches(".begin()").count(), 1);
    assert!(source.contains("REPEATABLE READ, READ ONLY"));
    assert!(
        !source.contains("INSERT INTO")
            && !source.contains("UPDATE ")
            && !source.contains("DELETE FROM")
    );
}
