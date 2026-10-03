//! `adapters::tests::project_continuity_0136` — Real-PostgreSQL acceptance for migration 0136's project continuity registration and facet publication.
//! Depends-on: crates=[humaux-testkit, postgres, serde_json, uuid]; services=[PostgreSQL(any)
//!   r=[control.contribution_policies,
//!   private.continuity_] w=[control.memberships, control.private_reasoning_domains, control.tenants, control.users,
//!   control.workspace_memberships, control.workspaces, private.continuity_facet_memory_links,
//!   private.continuity_facet_slots, private.continuity_facet_versions, private.continuity_projects,
//!   private.evidence_objects, private.memory_evidence, private.memory_records]
//!   x=[private.compute_contribution_source_backing_closure_v1, private.enqueue_contribution_execution,
//!   private.publish_continuity_facet, private.register_continuity_project], PostgreSQL(role_gateway),
//!   PostgreSQL(role_migration_owner)]; env=[HUMAUX_REQUIRE_DB, HUMAUX_TEST_PG_DSN];
//!   modules=[adapters::tests::support::contribution_fixture, humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [registration is eager and idempotent; null/unknown facets, ACL violations and a terminated pre-commit
//!   backend leave no residue; one winner per expected version; a missing DB fails when HUMAUX_REQUIRE_DB=1]
//! Spec: Baseline §25.3.1; §79.2; ADR-0035
//!
use postgres::{Client, Error, GenericClient, NoTls, Row, types::ToSql};
use serde_json::{Value, json};
use std::{
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

#[path = "support/contribution_fixture.rs"]
mod contribution_fixture;
use contribution_fixture::ContributionFixture;

const NIL: Uuid = Uuid::nil();
const FACETS: [&str; 15] = [
    "GOAL",
    "CURRENT_STATE",
    "DECISIONS",
    "REJECTIONS",
    "CONSTRAINTS",
    "KNOWN_ISSUES",
    "NEXT_ACTIONS",
    "ACTIVE_TASKS",
    "RECENT_CHANGES",
    "CODE",
    "TESTS",
    "CONFIG",
    "MIGRATIONS",
    "PROCEDURES",
    "OUTCOMES",
];

fn required_dsn() -> Option<String> {
    match std::env::var("HUMAUX_TEST_PG_DSN") {
        Ok(dsn) => Some(dsn),
        Err(_) if std::env::var("HUMAUX_REQUIRE_DB").as_deref() == Ok("1") => {
            panic!("HUMAUX_REQUIRE_DB=1 requires isolated HUMAUX_TEST_PG_DSN")
        }
        Err(_) => None,
    }
}

fn role_dsn(base: &str, role: &str, app: &str) -> String {
    // ADR-0059 D-D: a real login as `role`, its password from HUMAUX_ROLE_PASSWORD_<SUFFIX>.
    let dsn = humaux_testkit::role_login_dsn(base, role, |name| std::env::var(name).ok())
        .unwrap_or_else(|missing| panic!("missing object: {missing} (ADR-0059 D-D)"));
    let separator = if dsn.contains('?') { '&' } else { '?' };
    format!("{dsn}{separator}application_name={app}")
}

struct Fixture {
    admin_dsn: String,
    gateway_dsn: String,
    tenant: Uuid,
    user: Uuid,
    principal: Uuid,
    workspace: Uuid,
    other_workspace: Uuid,
    domain: Uuid,
    memory: Uuid,
    memory_hash: Vec<u8>,
    evidence: Uuid,
    tenant_evidence: Uuid,
    user_evidence: Uuid,
    cross_workspace_evidence: Uuid,
    evidence_hash: Vec<u8>,
}

impl Fixture {
    #[allow(clippy::too_many_lines)]
    fn new(admin_dsn: String) -> Self {
        let gateway_dsn = role_dsn(&admin_dsn, "role_gateway", "continuity_gateway");
        let tenant = Uuid::now_v7();
        let user = Uuid::now_v7();
        let principal = Uuid::now_v7();
        let workspace = Uuid::now_v7();
        let other_workspace = Uuid::now_v7();
        let domain = Uuid::now_v7();
        let memory = Uuid::now_v7();
        let evidence = Uuid::now_v7();
        let tenant_evidence = Uuid::now_v7();
        let user_evidence = Uuid::now_v7();
        let cross_workspace_evidence = Uuid::now_v7();
        let evidence_hash = vec![0x5a; 32];
        let content = json!({"continuity":"source"});
        // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
        let mut admin = Client::connect(&admin_dsn, NoTls).expect("admin connect");
        admin
            .execute(
                "INSERT INTO control.tenants(tenant_id,name,state) VALUES($1,'w1','ACTIVE')",
                &[&tenant],
            )
            .unwrap();
        admin
            .execute(
                "INSERT INTO control.users(user_id,state) VALUES($1,'ACTIVE')",
                &[&user],
            )
            .unwrap();
        admin
            .execute(
                "INSERT INTO control.memberships(tenant_id,user_id,role,state) \
             VALUES($1,$2,'OWNER','ACTIVE')",
                &[&tenant, &user],
            )
            .unwrap();
        for id in [workspace, other_workspace] {
            admin.execute(
                "INSERT INTO control.workspaces(workspace_id,tenant_id,name) VALUES($1,$2,'w1')",
                &[&id, &tenant],
            ).unwrap();
        }
        // ADR-0035 (card 13): 0163 re-points the WORKSPACE_SHARED visibility arm from
        // control.memberships (tenant membership) to an ACTIVE control.workspace_memberships row
        // for the row's OWN workspace. `f.user` is made a member of BOTH `workspace` and
        // `other_workspace`: this is only the base-RLS visibility gate, and
        // `evidence_owner_marker_matrix_is_exact_and_real_updates_are_closed`'s "legacy" (no
        // `humaux.continuity_publish` marker) block does a raw `role_migration_owner` read/lock/
        // update of `cross_workspace_evidence` and expects it visible — matching pre-0163
        // behaviour, since `role_migration_owner` carries no bypass arm on `evidence_objects`.
        // `headless_visibility_hash_successor_and_acl_are_closed`'s rejection of the same
        // Evidence during an actual `publish_continuity_facet()` call is unaffected by this: that
        // rejection comes from the function's own explicit `visibility_workspace_id=p_workspace_id`
        // match (via the 0136 `continuity_evidence_owner_exact_*` policies gated on
        // `humaux.continuity_publish='1'`), never from workspace_memberships breadth.
        for id in [workspace, other_workspace] {
            admin
                .execute(
                    "INSERT INTO control.workspace_memberships(tenant_id,workspace_id,user_id,role,state) \
                 VALUES($1,$2,$3,'MEMBER','ACTIVE')",
                    &[&tenant, &id, &user],
                )
                .unwrap();
        }
        admin
            .execute(
                "INSERT INTO control.private_reasoning_domains(reasoning_domain_id,tenant_id,name) \
             VALUES($1,$2,'w1')",
                &[&domain, &tenant],
            )
            .unwrap();
        admin.batch_execute("BEGIN").unwrap();
        admin
            .execute(
                "INSERT INTO private.memory_records( \
              memory_id,tenant_id,memory_type,content,visibility_class,authority_class, \
              confidence,status,asserted_at) \
             VALUES($1,$2,'FACT',$3,'TENANT_SHARED','ProjectDecision',1,'active',now())",
                &[&memory, &tenant, &content],
            )
            .unwrap();
        let memory_hash: Vec<u8> = admin
            .query_one(
                "SELECT sha256(convert_to(content::text,'UTF8')) \
             FROM private.memory_records WHERE memory_id=$1",
                &[&memory],
            )
            .unwrap()
            .get(0);
        admin
            .execute(
                "INSERT INTO private.evidence_objects( \
              evidence_id,tenant_id,evidence_kind,payload_sha256,data_class,origin_class, \
              visibility_class,visibility_workspace_id,reasoning_domain_id) \
             VALUES($1,$2,'EVENT',$3,'PRIVATE','AuthenticatedAgent', \
              'WORKSPACE_SHARED',$4,$5)",
                &[&evidence, &tenant, &evidence_hash, &workspace, &domain],
            )
            .unwrap();
        admin
            .execute(
                "INSERT INTO private.evidence_objects( \
              evidence_id,tenant_id,evidence_kind,payload_sha256,data_class,origin_class, \
              visibility_class,reasoning_domain_id) \
             VALUES($1,$2,'EVENT',$3,'PRIVATE','AuthenticatedAgent','TENANT_SHARED',$4)",
                &[&tenant_evidence, &tenant, &evidence_hash, &domain],
            )
            .unwrap();
        admin
            .execute(
                "INSERT INTO private.evidence_objects( \
              evidence_id,tenant_id,evidence_kind,payload_sha256,data_class,origin_class, \
              visibility_class,visibility_user_id,reasoning_domain_id) \
             VALUES($1,$2,'EVENT',$3,'PRIVATE','AuthenticatedAgent','USER_PRIVATE',$4,$5)",
                &[&user_evidence, &tenant, &evidence_hash, &user, &domain],
            )
            .unwrap();
        admin
            .execute(
                "INSERT INTO private.evidence_objects( \
              evidence_id,tenant_id,evidence_kind,payload_sha256,data_class,origin_class, \
              visibility_class,visibility_workspace_id,reasoning_domain_id) \
             VALUES($1,$2,'EVENT',$3,'PRIVATE','AuthenticatedAgent','WORKSPACE_SHARED',$4,$5)",
                &[
                    &cross_workspace_evidence,
                    &tenant,
                    &evidence_hash,
                    &other_workspace,
                    &domain,
                ],
            )
            .unwrap();
        admin
            .execute(
                "INSERT INTO private.memory_evidence(memory_id,evidence_id,role) \
                 VALUES($1,$2,'SUPPORTING')",
                &[&memory, &evidence],
            )
            .unwrap();
        admin.batch_execute("COMMIT").unwrap();
        Self {
            admin_dsn,
            gateway_dsn,
            tenant,
            user,
            principal,
            workspace,
            other_workspace,
            domain,
            memory,
            memory_hash,
            evidence,
            tenant_evidence,
            user_evidence,
            cross_workspace_evidence,
            evidence_hash,
        }
    }

    fn gateway(&self, app: &str) -> Client {
        let dsn = self.gateway_dsn.replace("continuity_gateway", app);
        // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
        Client::connect(&dsn, NoTls).expect("gateway connect")
    }

    fn admin(&self) -> Client {
        // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
        Client::connect(&self.admin_dsn, NoTls).expect("admin connect")
    }

    fn project(&self, title: &str) -> Uuid {
        let project = Uuid::now_v7();
        let mut gateway = self.gateway("continuity_register");
        register(
            &mut gateway,
            self.tenant,
            self.workspace,
            project,
            self.principal,
            None,
            title,
        )
        .unwrap();
        project
    }
}

fn set_context(
    client: &mut impl GenericClient,
    tenant: Uuid,
    workspace: Uuid,
    principal: Uuid,
    user: Option<Uuid>,
) {
    client
        .query_one(
            "SELECT set_config('humaux.tenant_id',$1,true), \
                set_config('humaux.workspace_id',$2,true), \
                set_config('humaux.principal_id',$3,true), \
                set_config('humaux.user_id',$4,true)",
            &[
                &tenant.to_string(),
                &workspace.to_string(),
                &principal.to_string(),
                &user.unwrap_or(NIL).to_string(),
            ],
        )
        .unwrap();
}

fn register(
    client: &mut Client,
    tenant: Uuid,
    workspace: Uuid,
    project: Uuid,
    principal: Uuid,
    user: Option<Uuid>,
    title: &str,
) -> Result<Uuid, Error> {
    let mut tx = client.transaction()?;
    set_context(&mut tx, tenant, workspace, principal, user);
    let row = tx.query_one(
        "SELECT private.register_continuity_project($1,$2,$3,$4,$5,$6)",
        &[&tenant, &workspace, &project, &principal, &user, &title],
    )?;
    let id = row.get(0);
    tx.commit()?;
    Ok(id)
}

#[allow(clippy::too_many_arguments)]
fn publish_in_open_tx(
    client: &mut Client,
    tenant: Uuid,
    workspace: Uuid,
    project: Uuid,
    principal: Uuid,
    user: Option<Uuid>,
    facet: &str,
    expected: i64,
    body: &Value,
    memories: &[Uuid],
    memory_hashes: &[Vec<u8>],
    evidence: &[Uuid],
    evidence_hashes: &[Vec<u8>],
) -> Result<Row, Error> {
    set_context(client, tenant, workspace, principal, user);
    client.query_one(
        "SELECT facet_version_id,facet_version,slot_version,body_sha256 \
         FROM private.publish_continuity_facet( \
          $1,$2,$3,$4,$5,$6,$7,'CURRENT',$8,$9,$10,$11,$12)",
        &[
            &tenant,
            &workspace,
            &project,
            &principal,
            &user,
            &facet,
            &expected,
            body,
            &memories,
            &memory_hashes,
            &evidence,
            &evidence_hashes,
        ],
    )
}

#[allow(clippy::too_many_arguments)]
fn publish(
    client: &mut Client,
    tenant: Uuid,
    workspace: Uuid,
    project: Uuid,
    principal: Uuid,
    user: Option<Uuid>,
    facet: &str,
    expected: i64,
    body: &Value,
    memories: &[Uuid],
    memory_hashes: &[Vec<u8>],
    evidence: &[Uuid],
    evidence_hashes: &[Vec<u8>],
) -> Result<Row, Error> {
    client.batch_execute("BEGIN")?;
    let result = publish_in_open_tx(
        client,
        tenant,
        workspace,
        project,
        principal,
        user,
        facet,
        expected,
        body,
        memories,
        memory_hashes,
        evidence,
        evidence_hashes,
    );
    match result {
        Ok(row) => {
            client.batch_execute("COMMIT")?;
            Ok(row)
        }
        Err(error) => {
            let _ = client.batch_execute("ROLLBACK");
            Err(error)
        }
    }
}

fn state(error: &Error) -> Option<&str> {
    error.as_db_error().map(|db| db.code().code())
}

fn execute_error_in_savepoint(
    client: &mut Client,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> String {
    client
        .batch_execute("SAVEPOINT continuity_negative")
        .unwrap();
    let error = client.execute(sql, params).unwrap_err();
    let code = state(&error).unwrap().to_owned();
    client
        .batch_execute(
            "ROLLBACK TO SAVEPOINT continuity_negative; RELEASE SAVEPOINT continuity_negative",
        )
        .unwrap();
    code
}

#[test]
fn migration_static_contract_witnesses() {
    let sql = include_str!("../../../migrations/0136_project_continuity.sql");
    let manifest = include_str!("../../../migrations/0136_project_continuity.manifest.toml");
    assert_eq!(sql.matches("CREATE TABLE private.continuity_").count(), 5);
    assert_eq!(
        sql.matches("SET humaux.continuity_publish TO '1'").count(),
        1
    );
    assert!(!sql.contains("set_config('humaux.continuity_publish'"));
    assert!(!sql.contains("'HANDOFF'") && !sql.contains("'COVERAGE'"));
    for witness in [
        "FOR UPDATE",
        "slot.slot_version=p_expected_slot_version",
        "GET DIAGNOSTICS affected=ROW_COUNT",
        "FOR SHARE OF memory",
        "FOR SHARE OF evidence",
        "FORCE ROW LEVEL SECURITY",
        "continuity_evidence_owner_exact_allow",
        "continuity_evidence_owner_exact_guard",
        "continuity_evidence_owner_lock_allow",
        "continuity_evidence_owner_lock_guard",
        "END) WITH CHECK (false)",
    ] {
        assert!(sql.contains(witness), "missing witness {witness}");
    }
    assert!(manifest.contains("slot.slot_version=p_expected_slot_version"));
    assert!(manifest.contains("humaux.continuity_publish=1"));
}

#[test]
fn registration_is_eager_exact_and_idempotent() {
    let Some(dsn) = required_dsn() else { return };
    let f = Fixture::new(dsn);
    let project = Uuid::now_v7();
    let mut gateway = f.gateway("continuity_register_exact");
    assert_eq!(
        register(
            &mut gateway,
            f.tenant,
            f.workspace,
            project,
            f.principal,
            None,
            "exact"
        )
        .unwrap(),
        project
    );
    assert_eq!(
        register(
            &mut gateway,
            f.tenant,
            f.workspace,
            project,
            f.principal,
            None,
            "exact"
        )
        .unwrap(),
        project
    );
    let mut admin = f.admin();
    let rows = admin
        .query(
            "SELECT facet_kind,slot_version,current_version_id,slot_state \
         FROM private.continuity_facet_slots WHERE project_id=$1 ORDER BY facet_kind",
            &[&project],
        )
        .unwrap();
    assert_eq!(rows.len(), 15);
    let mut actual: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
    actual.sort();
    let mut expected: Vec<String> = FACETS.iter().map(ToString::to_string).collect();
    expected.sort();
    assert_eq!(actual, expected);
    assert!(rows.iter().all(|row| row.get::<_, i64>(1) == 0
        && row.get::<_, Option<Uuid>>(2).is_none()
        && row.get::<_, Option<String>>(3).is_none()));
    let v4 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let error = register(
        &mut gateway,
        f.tenant,
        f.workspace,
        v4,
        f.principal,
        None,
        "bad",
    )
    .unwrap_err();
    assert_eq!(state(&error), Some("22023"));

    let error = register(
        &mut gateway,
        f.tenant,
        f.other_workspace,
        project,
        f.principal,
        None,
        "exact",
    )
    .unwrap_err();
    assert_eq!(state(&error), Some("23514"));

    admin
        .execute(
            "DELETE FROM private.continuity_facet_slots \
             WHERE project_id=$1 AND facet_kind='GOAL'",
            &[&project],
        )
        .unwrap();
    let error = register(
        &mut gateway,
        f.tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "exact",
    )
    .unwrap_err();
    assert_eq!(state(&error), Some("23514"));
    let remaining: i64 = admin
        .query_one(
            "SELECT count(*) FROM private.continuity_facet_slots WHERE project_id=$1",
            &[&project],
        )
        .unwrap()
        .get(0);
    assert_eq!(
        remaining, 14,
        "registration retry must not repair partial state"
    );
}

#[test]
fn null_scalar_and_unknown_facet_fail_closed_without_residue() {
    let Some(dsn) = required_dsn() else { return };
    let f = Fixture::new(dsn);
    let mut gateway = f.gateway("continuity_null_scalars");
    let null_principal_project = Uuid::now_v7();
    gateway.batch_execute("BEGIN").unwrap();
    set_context(&mut gateway, f.tenant, f.workspace, f.principal, None);
    let error = gateway
        .query_one(
            "SELECT private.register_continuity_project($1,$2,$3,NULL,NULL,'null-principal')",
            &[&f.tenant, &f.workspace, &null_principal_project],
        )
        .unwrap_err();
    assert_eq!(state(&error), Some("42501"));
    gateway.batch_execute("ROLLBACK").unwrap();

    gateway.batch_execute("BEGIN").unwrap();
    set_context(&mut gateway, f.tenant, f.workspace, f.principal, None);
    let error = gateway
        .query_one(
            "SELECT private.register_continuity_project($1,$2,NULL,$3,NULL,'null-project')",
            &[&f.tenant, &f.workspace, &f.principal],
        )
        .unwrap_err();
    assert_eq!(state(&error), Some("22023"));
    gateway.batch_execute("ROLLBACK").unwrap();

    let project = f.project("scalar-shape");
    for (sql, expected) in [
        (
            "SELECT * FROM private.publish_continuity_facet($1,$2,$3,$4,NULL,NULL,0,'CURRENT',$5,$6,$7,$8,$9)",
            "22023",
        ),
        (
            "SELECT * FROM private.publish_continuity_facet($1,$2,$3,$4,NULL,'UNKNOWN',0,'CURRENT',$5,$6,$7,$8,$9)",
            "22023",
        ),
        (
            "SELECT * FROM private.publish_continuity_facet($1,$2,$3,$4,NULL,'GOAL',NULL,'CURRENT',$5,$6,$7,$8,$9)",
            "22023",
        ),
        (
            "SELECT * FROM private.publish_continuity_facet($1,$2,$3,$4,NULL,'GOAL',0,NULL,$5,$6,$7,$8,$9)",
            "22023",
        ),
    ] {
        gateway.batch_execute("BEGIN").unwrap();
        set_context(&mut gateway, f.tenant, f.workspace, f.principal, None);
        let error = gateway
            .query_one(
                sql,
                &[
                    &f.tenant,
                    &f.workspace,
                    &project,
                    &f.principal,
                    &json!({"shape":true}),
                    &vec![f.memory],
                    &vec![f.memory_hash.clone()],
                    &Vec::<Uuid>::new(),
                    &Vec::<Vec<u8>>::new(),
                ],
            )
            .unwrap_err();
        assert_eq!(state(&error), Some(expected));
        gateway.batch_execute("ROLLBACK").unwrap();
    }
    let other_tenant = Uuid::now_v7();
    let error = publish(
        &mut gateway,
        other_tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "GOAL",
        0,
        &json!({"cross_tenant":true}),
        &[f.memory],
        std::slice::from_ref(&f.memory_hash),
        &[],
        &[],
    )
    .unwrap_err();
    assert_eq!(state(&error), Some("42501"));
    let mut admin = f.admin();
    let residue: i64 = admin
        .query_one(
            "SELECT count(*) FROM private.continuity_facet_versions WHERE project_id=$1",
            &[&project],
        )
        .unwrap()
        .get(0);
    assert_eq!(residue, 0);
    let null_project_count: i64 = admin
        .query_one(
            "SELECT count(*) FROM private.continuity_projects WHERE project_id=$1",
            &[&null_principal_project],
        )
        .unwrap()
        .get(0);
    assert_eq!(null_project_count, 0);
}

#[test]
#[allow(clippy::too_many_lines)]
fn headless_visibility_hash_successor_and_acl_are_closed() {
    let Some(dsn) = required_dsn() else { return };
    let f = Fixture::new(dsn.clone());
    let project = f.project("publish");
    let mut gateway = f.gateway("continuity_publish_visibility");
    let body1 = json!({"v":1});
    let row = publish(
        &mut gateway,
        f.tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "GOAL",
        0,
        &body1,
        &[f.memory],
        std::slice::from_ref(&f.memory_hash),
        &[f.evidence],
        std::slice::from_ref(&f.evidence_hash),
    )
    .unwrap();
    assert_eq!(row.get::<_, i64>(1), 1);
    assert_eq!(
        register(
            &mut gateway,
            f.tenant,
            f.workspace,
            project,
            f.principal,
            None,
            "publish",
        )
        .unwrap(),
        project,
        "registration retry remains idempotent after a facet publication"
    );
    let body2 = json!({"v":2});
    let row = publish(
        &mut gateway,
        f.tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "GOAL",
        1,
        &body2,
        &[f.memory],
        std::slice::from_ref(&f.memory_hash),
        &[],
        &[],
    )
    .unwrap();
    assert_eq!(row.get::<_, i64>(1), 2);
    let bad_hash = vec![9; 32];
    let error = publish(
        &mut gateway,
        f.tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "DECISIONS",
        0,
        &body1,
        &[f.memory],
        &[bad_hash],
        &[],
        &[],
    )
    .unwrap_err();
    assert_eq!(state(&error), Some("23514"));
    publish(
        &mut gateway,
        f.tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "DECISIONS",
        0,
        &body1,
        &[],
        &[],
        &[f.tenant_evidence],
        std::slice::from_ref(&f.evidence_hash),
    )
    .expect("headless TENANT_SHARED Evidence is visible");
    let error = publish(
        &mut gateway,
        f.tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "REJECTIONS",
        0,
        &body1,
        &[],
        &[],
        &[f.user_evidence],
        std::slice::from_ref(&f.evidence_hash),
    )
    .unwrap_err();
    assert_eq!(state(&error), Some("23514"));
    let error = publish(
        &mut gateway,
        f.tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "CONSTRAINTS",
        0,
        &body1,
        &[],
        &[],
        &[f.cross_workspace_evidence],
        std::slice::from_ref(&f.evidence_hash),
    )
    .unwrap_err();
    assert_eq!(state(&error), Some("23514"));
    let mut admin = f.admin();
    let residue: i64 = admin
        .query_one(
            "SELECT count(*) FROM private.continuity_facet_versions \
         WHERE project_id=$1 AND facet_kind IN ('REJECTIONS','CONSTRAINTS')",
            &[&project],
        )
        .unwrap()
        .get(0);
    assert_eq!(residue, 0);
    let private_dsn = role_dsn(&dsn, "role_private_worker", "continuity_private_denied");
    // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
    let mut private = Client::connect(&private_dsn, NoTls).unwrap();
    private.batch_execute("BEGIN").unwrap();
    set_context(&mut private, f.tenant, f.workspace, f.principal, None);
    let denied = private
        .query_one(
            "SELECT private.register_continuity_project($1,$2,$3,$4,$5,$6)",
            &[
                &f.tenant,
                &f.workspace,
                &Uuid::now_v7(),
                &f.principal,
                &Option::<Uuid>::None,
                &"denied",
            ],
        )
        .unwrap_err();
    assert_eq!(state(&denied), Some("42501"));
    private.batch_execute("ROLLBACK").unwrap();
}

#[test]
#[allow(clippy::too_many_lines)]
fn owner_mutation_append_only_deferred_source_and_runtime_acl_are_enforced() {
    let Some(dsn) = required_dsn() else { return };
    let f = Fixture::new(dsn);
    let project = f.project("mutation-guards");
    let mut gateway = f.gateway("continuity_guard_seed");
    let goal = publish(
        &mut gateway,
        f.tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "GOAL",
        0,
        &json!({"guard":"goal"}),
        &[f.memory],
        std::slice::from_ref(&f.memory_hash),
        &[],
        &[],
    )
    .unwrap();
    let goal_version: Uuid = goal.get(0);
    let decisions = publish(
        &mut gateway,
        f.tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "DECISIONS",
        0,
        &json!({"guard":"decisions"}),
        &[f.memory],
        std::slice::from_ref(&f.memory_hash),
        &[],
        &[],
    )
    .unwrap();
    let decisions_version: Uuid = decisions.get(0);

    let mut direct_gateway = f.gateway("continuity_direct_acl");
    direct_gateway.batch_execute("BEGIN").unwrap();
    set_context(
        &mut direct_gateway,
        f.tenant,
        f.workspace,
        f.principal,
        None,
    );
    let direct_error = direct_gateway
        .execute(
            "UPDATE private.continuity_projects SET title='forbidden' WHERE project_id=$1",
            &[&project],
        )
        .unwrap_err();
    assert_eq!(state(&direct_error), Some("42501"));
    let _ = direct_gateway.batch_execute("ROLLBACK");

    let mut admin = f.admin();
    let acl_violations: i64 = admin
        .query_one(
            "WITH roles(name) AS (VALUES \
               ('role_admin'),('role_private_worker'),('role_consolidation_worker'), \
               ('role_public_worker'),('role_retrieval_worker'),('role_batch_issuer'), \
               ('role_maintenance'),('public')), functions(signature) AS (VALUES \
               ('private.register_continuity_project(uuid,uuid,uuid,uuid,uuid,text)'), \
               ('private.publish_continuity_facet(uuid,uuid,uuid,uuid,uuid,text,bigint,text,jsonb,uuid[],bytea[],uuid[],bytea[])')) \
             SELECT count(*) FROM roles CROSS JOIN functions \
             WHERE has_function_privilege(roles.name,functions.signature,'EXECUTE')",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(acl_violations, 0);
    let gateway_execute_count: i64 = admin
        .query_one(
            "SELECT count(*) FROM unnest(ARRAY[ \
              'private.register_continuity_project(uuid,uuid,uuid,uuid,uuid,text)', \
              'private.publish_continuity_facet(uuid,uuid,uuid,uuid,uuid,text,bigint,text,jsonb,uuid[],bytea[],uuid[],bytea[])']) signature \
             WHERE has_function_privilege('role_gateway',signature,'EXECUTE')",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(gateway_execute_count, 2);

    // dep: PostgreSQL(role_migration_owner) — role switch before the scoped statements for `owner_mutation_append_only_deferred_source_and_runtime_acl_are_enforced`
    admin
        .batch_execute("BEGIN; SET LOCAL ROLE role_migration_owner")
        .unwrap();
    admin
        .execute(
            "UPDATE private.continuity_projects SET title='legal title',updated_at=now() \
             WHERE project_id=$1",
            &[&project],
        )
        .unwrap();
    assert_eq!(
        execute_error_in_savepoint(
            &mut admin,
            "UPDATE private.continuity_projects SET workspace_id=$2 WHERE project_id=$1",
            &[&project, &f.other_workspace],
        ),
        "23514"
    );
    assert_eq!(
        execute_error_in_savepoint(
            &mut admin,
            "DELETE FROM private.continuity_projects WHERE project_id=$1",
            &[&project],
        ),
        "23514"
    );
    assert_eq!(
        execute_error_in_savepoint(
            &mut admin,
            "UPDATE private.continuity_facet_versions SET body='{}' WHERE facet_version_id=$1",
            &[&goal_version],
        ),
        "23514"
    );
    assert_eq!(
        execute_error_in_savepoint(
            &mut admin,
            "DELETE FROM private.continuity_facet_memory_links WHERE facet_version_id=$1",
            &[&goal_version],
        ),
        "23514"
    );
    assert_eq!(
        execute_error_in_savepoint(
            &mut admin,
            "INSERT INTO private.continuity_facet_slots(tenant_id,workspace_id,project_id,facet_kind) \
             VALUES($1,$2,$3,'HANDOFF')",
            &[&f.tenant, &f.workspace, &project],
        ),
        "23514"
    );
    admin.batch_execute("COMMIT").unwrap();

    let orphan_version = Uuid::now_v7();
    // dep: PostgreSQL(role_migration_owner) — role switch before the scoped statements for `owner_mutation_append_only_deferred_source_and_runtime_acl_are_enforced`
    admin
        .batch_execute("BEGIN; SET LOCAL ROLE role_migration_owner")
        .unwrap();
    admin
        .execute(
            "INSERT INTO private.continuity_facet_versions( \
               tenant_id,workspace_id,project_id,facet_kind,facet_version_id,facet_version, \
               body,body_sha256,authored_by_principal_id) \
             VALUES($1,$2,$3,'CURRENT_STATE',$4,1,$5,sha256(convert_to($5::jsonb::text,'UTF8')),$6)",
            &[
                &f.tenant,
                &f.workspace,
                &project,
                &orphan_version,
                &json!({"orphan":true}),
                &f.principal,
            ],
        )
        .unwrap();
    let commit_error = admin.batch_execute("COMMIT").unwrap_err();
    assert_eq!(state(&commit_error), Some("23514"));
    let _ = admin.batch_execute("ROLLBACK");
    let orphan_count: i64 = admin
        .query_one(
            "SELECT count(*) FROM private.continuity_facet_versions WHERE facet_version_id=$1",
            &[&orphan_version],
        )
        .unwrap()
        .get(0);
    assert_eq!(orphan_count, 0);

    // dep: PostgreSQL(role_migration_owner) — role switch before the scoped statements for `owner_mutation_append_only_deferred_source_and_runtime_acl_are_enforced`
    admin
        .batch_execute("BEGIN; SET LOCAL ROLE role_migration_owner")
        .unwrap();
    admin
        .execute(
            "UPDATE private.continuity_facet_slots SET current_version_id=$2 \
             WHERE project_id=$1 AND facet_kind='GOAL'",
            &[&project, &decisions_version],
        )
        .unwrap();
    let commit_error = admin.batch_execute("COMMIT").unwrap_err();
    assert_eq!(state(&commit_error), Some("23503"));
    let _ = admin.batch_execute("ROLLBACK");
    let restored: Uuid = admin
        .query_one(
            "SELECT current_version_id FROM private.continuity_facet_slots \
             WHERE project_id=$1 AND facet_kind='GOAL'",
            &[&project],
        )
        .unwrap()
        .get(0);
    assert_eq!(restored, goal_version);

    let trigger_count: i64 = admin
        .query_one(
            "SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgname=ANY(ARRAY[ \
              'continuity_project_mutation_guard','continuity_facet_versions_append_only', \
              'continuity_facet_memory_links_append_only','continuity_facet_evidence_links_append_only', \
              'continuity_facet_version_source_required'])",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(trigger_count, 5);
}

#[test]
fn same_expected_has_one_winner_one_p9c01_and_real_slot_wait() {
    let Some(dsn) = required_dsn() else { return };
    let f = Fixture::new(dsn);
    let project = f.project("race");
    let mut first = f.gateway("continuity_race_first");
    first.batch_execute("BEGIN").unwrap();
    publish_in_open_tx(
        &mut first,
        f.tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "GOAL",
        0,
        &json!({"writer":1}),
        &[f.memory],
        std::slice::from_ref(&f.memory_hash),
        &[],
        &[],
    )
    .unwrap();
    let second_dsn = f
        .gateway_dsn
        .replace("continuity_gateway", "continuity_race_second");
    let (sent, received) = mpsc::channel();
    let memory_hash = f.memory_hash.clone();
    let ids = (f.tenant, f.workspace, project, f.principal, f.memory);
    let worker = thread::spawn(move || {
        // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
        let mut second = Client::connect(&second_dsn, NoTls).unwrap();
        let result = publish(
            &mut second,
            ids.0,
            ids.1,
            ids.2,
            ids.3,
            None,
            "GOAL",
            0,
            &json!({"writer":2}),
            &[ids.4],
            &[memory_hash],
            &[],
            &[],
        );
        sent.send(
            result
                .map(|_| ())
                .map_err(|e| state(&e).map(ToString::to_string)),
        )
        .unwrap();
    });
    let mut admin = f.admin();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_block = false;
    while Instant::now() < deadline {
        saw_block = admin
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity activity \
             WHERE activity.application_name='continuity_race_second' \
               AND cardinality(pg_blocking_pids(activity.pid))>0)",
                &[],
            )
            .unwrap()
            .get(0);
        if saw_block {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(saw_block, "second writer never blocked on exact slot");
    first.batch_execute("COMMIT").unwrap();
    let loser = received
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap_err();
    assert_eq!(loser.as_deref(), Some("P9C01"));
    worker.join().unwrap();
    let counts = admin
        .query_one(
            "SELECT (SELECT count(*) FROM private.continuity_facet_versions WHERE project_id=$1), \
                (SELECT count(*) FROM private.continuity_facet_memory_links WHERE project_id=$1)",
            &[&project],
        )
        .unwrap();
    assert_eq!((counts.get::<_, i64>(0), counts.get::<_, i64>(1)), (1, 1));
}

#[test]
fn different_facets_do_not_wait_on_a_project_lock() {
    let Some(dsn) = required_dsn() else { return };
    let f = Fixture::new(dsn);
    let project = f.project("different-facets");
    let mut first = f.gateway("continuity_different_first");
    first.batch_execute("BEGIN").unwrap();
    publish_in_open_tx(
        &mut first,
        f.tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "GOAL",
        0,
        &json!({"facet":"goal"}),
        &[f.memory],
        std::slice::from_ref(&f.memory_hash),
        &[],
        &[],
    )
    .unwrap();
    let second_dsn = f
        .gateway_dsn
        .replace("continuity_gateway", "continuity_different_second");
    let ids = (f.tenant, f.workspace, project, f.principal, f.memory);
    let hash = f.memory_hash.clone();
    let (sent, received) = mpsc::channel();
    let worker = thread::spawn(move || {
        // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
        let mut second = Client::connect(&second_dsn, NoTls).unwrap();
        sent.send(
            publish(
                &mut second,
                ids.0,
                ids.1,
                ids.2,
                ids.3,
                None,
                "DECISIONS",
                0,
                &json!({"facet":"decisions"}),
                &[ids.4],
                &[hash],
                &[],
                &[],
            )
            .map(|_| ()),
        )
        .unwrap();
    });
    received
        .recv_timeout(Duration::from_secs(5))
        .expect("different facet writer waited on a project lock")
        .expect("different facet publish failed");
    first.batch_execute("COMMIT").unwrap();
    worker.join().unwrap();
}

#[test]
fn terminated_precommit_backend_leaves_no_publication_residue() {
    let Some(dsn) = required_dsn() else { return };
    let f = Fixture::new(dsn);
    let project = f.project("terminate");
    let mut gateway = f.gateway("continuity_terminate_writer");
    let pid: i32 = gateway
        .query_one("SELECT pg_backend_pid()", &[])
        .unwrap()
        .get(0);
    gateway.batch_execute("BEGIN").unwrap();
    publish_in_open_tx(
        &mut gateway,
        f.tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "GOAL",
        0,
        &json!({"uncommitted":true}),
        &[f.memory],
        std::slice::from_ref(&f.memory_hash),
        &[],
        &[],
    )
    .unwrap();
    let mut admin = f.admin();
    assert!(
        admin
            .query_one("SELECT pg_terminate_backend($1)", &[&pid])
            .unwrap()
            .get::<_, bool>(0)
    );
    let row = admin
        .query_one(
            "SELECT (SELECT count(*) FROM private.continuity_facet_versions WHERE project_id=$1), \
                (SELECT slot_version FROM private.continuity_facet_slots \
                 WHERE project_id=$1 AND facet_kind='GOAL')",
            &[&project],
        )
        .unwrap();
    assert_eq!((row.get::<_, i64>(0), row.get::<_, i64>(1)), (0, 0));
}

#[test]
#[allow(clippy::too_many_lines)]
fn source_for_share_blocks_change_and_marker_restores_after_error() {
    let Some(dsn) = required_dsn() else { return };
    let f = Fixture::new(dsn);
    let project = f.project("source-lock");
    let mut gateway = f.gateway("continuity_source_lock");
    gateway.batch_execute("BEGIN").unwrap();
    publish_in_open_tx(
        &mut gateway,
        f.tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "GOAL",
        0,
        &json!({"lock":true}),
        &[f.memory],
        std::slice::from_ref(&f.memory_hash),
        &[],
        &[],
    )
    .unwrap();
    let marker: Option<String> = gateway
        .query_one(
            "SELECT nullif(current_setting('humaux.continuity_publish',true),'')",
            &[],
        )
        .unwrap()
        .get(0);
    assert!(
        marker.is_none(),
        "function-level marker leaked after normal return"
    );
    let admin_dsn = f.admin_dsn.clone();
    let memory = f.memory;
    let updater = thread::spawn(move || {
        // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
        let mut client = Client::connect(&admin_dsn, NoTls).unwrap();
        client
            .execute(
                "UPDATE private.memory_records SET status='revoked' WHERE memory_id=$1",
                &[&memory],
            )
            .unwrap();
    });
    let mut admin = f.admin();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut blocked = false;
    while Instant::now() < deadline {
        blocked=admin.query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity a WHERE a.query LIKE \
             'UPDATE private.memory_records SET status=%' AND cardinality(pg_blocking_pids(a.pid))>0)", &[]).unwrap().get(0);
        if blocked {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(blocked, "source update did not wait on FOR SHARE");
    gateway.batch_execute("COMMIT").unwrap();
    updater.join().unwrap();
    let error = publish(
        &mut gateway,
        f.tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "DECISIONS",
        0,
        &json!({"inactive":true}),
        &[f.memory],
        std::slice::from_ref(&f.memory_hash),
        &[],
        &[],
    )
    .unwrap_err();
    assert_eq!(state(&error), Some("23514"));
    let absent = Uuid::now_v7();
    let error = publish(
        &mut gateway,
        f.tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "REJECTIONS",
        0,
        &json!({"absent":true}),
        &[absent],
        std::slice::from_ref(&f.memory_hash),
        &[],
        &[],
    )
    .unwrap_err();
    assert_eq!(state(&error), Some("23514"));
    gateway.batch_execute("BEGIN").unwrap();
    set_context(&mut gateway, f.tenant, f.workspace, f.principal, None);
    gateway.batch_execute("SAVEPOINT before_failure").unwrap();
    let error = publish_in_open_tx(
        &mut gateway,
        f.tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "GOAL",
        0,
        &json!({"stale":true}),
        &[f.memory],
        std::slice::from_ref(&f.memory_hash),
        &[],
        &[],
    )
    .unwrap_err();
    assert_eq!(state(&error), Some("P9C01"));
    gateway
        .batch_execute("ROLLBACK TO SAVEPOINT before_failure")
        .unwrap();
    let marker: Option<String> = gateway
        .query_one(
            "SELECT nullif(current_setting('humaux.continuity_publish',true),'')",
            &[],
        )
        .unwrap()
        .get(0);
    assert!(
        marker.is_none(),
        "function-level marker leaked after exception"
    );
    gateway.batch_execute("ROLLBACK").unwrap();
}

#[test]
fn publish_marker_restores_before_same_transaction_0133_legacy_reader() {
    let Some(dsn) = required_dsn() else { return };
    let f = Fixture::new(dsn);
    let project = f.project("legacy-0133");
    let mut admin = f.admin();
    // dep: PostgreSQL(role_gateway) — role switch before the scoped statements for `publish_marker_restores_before_same_transaction_0133_legacy_reader`
    admin
        .batch_execute("BEGIN; SET LOCAL ROLE role_gateway")
        .unwrap();
    publish_in_open_tx(
        &mut admin,
        f.tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "GOAL",
        0,
        &json!({"legacy":true}),
        &[],
        &[],
        &[f.evidence],
        std::slice::from_ref(&f.evidence_hash),
    )
    .unwrap();
    admin.batch_execute("RESET ROLE").unwrap();
    admin
        .query_one("SELECT set_config('humaux.workspace_id','',true)", &[])
        .unwrap();
    // dep: PostgreSQL(role_migration_owner) — role switch before the scoped statements for `publish_marker_restores_before_same_transaction_0133_legacy_reader`
    admin
        .batch_execute("SET LOCAL ROLE role_migration_owner")
        .unwrap();
    let row = admin
        .query_one(
            "SELECT direct_count,backing_link_count,closure_sha256 \
         FROM private.compute_contribution_source_backing_closure_v1( \
           $1,$2,$3,$4,$5,$6)",
            &[
                &f.tenant,
                &f.user,
                &f.domain,
                &vec!["EVIDENCE"],
                &vec![f.evidence],
                &vec![f.evidence_hash.clone()],
            ],
        )
        .expect("0133 legacy WORKSPACE_SHARED reader remains unchanged after publish");
    assert_eq!(row.get::<_, i64>(0), 1);
    admin.batch_execute("ROLLBACK").unwrap();
}

#[test]
fn role_private_worker_enqueue_preserves_0133_legacy_workspace_shared_route() {
    let Some(dsn) = required_dsn() else { return };
    let mut fixture = ContributionFixture::new();
    let tenant: Uuid = fixture
        .admin
        .query_one(
            "SELECT tenant_id FROM private.memory_records WHERE memory_id=$1",
            &[&fixture.memory],
        )
        .unwrap()
        .get(0);
    let user: Uuid = fixture.admin.query_one(
        "SELECT owner_user_id FROM control.private_reasoning_domains WHERE reasoning_domain_id=$1",
        &[&fixture.domain],
    ).unwrap().get(0);
    let workspace: Uuid = fixture.admin.query_one(
        "INSERT INTO control.workspaces(tenant_id,name) VALUES($1,'legacy-wsc') RETURNING workspace_id",
        &[&tenant],
    ).unwrap().get(0);
    // ADR-0035 (card 13): 0163 re-points the WORKSPACE_SHARED visibility arm from
    // control.memberships (tenant membership) to an ACTIVE control.workspace_memberships row
    // for the row's OWN workspace. `user` (the reasoning domain owner) is made a member of this
    // freshly-created `workspace` so the base RLS policy still surfaces the row to
    // `private.compute_contribution_source_backing_closure_v1`'s own (pre-0163) WORKSPACE_SHARED
    // re-check, which never reads workspace_memberships itself.
    fixture
        .admin
        .execute(
            "INSERT INTO control.workspace_memberships(tenant_id,workspace_id,user_id,role,state) \
             VALUES($1,$2,$3,'MEMBER','ACTIVE')",
            &[&tenant, &workspace, &user],
        )
        .unwrap();
    fixture
        .admin
        .execute(
            "UPDATE private.memory_records SET visibility_class='WORKSPACE_SHARED', \
         visibility_user_id=NULL,visibility_workspace_id=$2 WHERE memory_id=$1",
            &[&fixture.memory, &workspace],
        )
        .unwrap();
    fixture
        .admin
        .execute(
            "UPDATE private.evidence_objects evidence SET visibility_class='WORKSPACE_SHARED', \
         visibility_user_id=NULL,visibility_workspace_id=$2 FROM private.memory_evidence backing \
         WHERE backing.memory_id=$1 AND backing.evidence_id=evidence.evidence_id",
            &[&fixture.memory, &workspace],
        )
        .unwrap();
    let source_hash: Vec<u8> = fixture.admin.query_one(
        "SELECT sha256(convert_to(content::text,'UTF8')) FROM private.memory_records WHERE memory_id=$1",
        &[&fixture.memory],
    ).unwrap().get(0);
    let manifest_hash: Vec<u8> = fixture
        .admin
        .query_one(
            "SELECT sha256(convert_to('m:'||$1::uuid::text||':'||encode($2::bytea,'hex'),'UTF8'))",
            &[&fixture.memory, &source_hash],
        )
        .unwrap()
        .get(0);
    let policy: Uuid = fixture.admin.query_one(
        "SELECT policy_id FROM control.contribution_policies WHERE tenant_id=$1 AND effective_to IS NULL",
        &[&tenant],
    ).unwrap().get(0);
    let private_dsn = role_dsn(&dsn, "role_private_worker", "continuity_legacy_enqueue");
    // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
    let mut private = Client::connect(&private_dsn, NoTls).unwrap();
    let snapshot =
        json!({"policy":"MANUAL","principal_id":user.to_string(),"allowed_workspace_ids":[]});
    let row = private
        .query_one(
            "SELECT * FROM private.enqueue_contribution_execution( \
         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,1,$13,$14,NULL,NULL,NULL,NULL, \
         $15,1,1,1,$16,$17,$18,$19,$20)",
            &[
                &tenant,
                &Uuid::new_v4(),
                &Uuid::new_v4(),
                &Uuid::new_v4(),
                &Uuid::new_v4(),
                &Uuid::new_v4(),
                &format!("continuity-legacy-{}", Uuid::new_v4()),
                &vec![0x61_u8; 32],
                &user,
                &fixture.domain,
                &manifest_hash,
                &policy,
                &snapshot,
                &"USER_CONSENT",
                &fixture.binding,
                &vec![0x71_u8; 32],
                &vec![0x72_u8; 32],
                &vec!["MEMORY".to_owned()],
                &vec![fixture.memory],
                &vec![source_hash],
            ],
        )
        .expect("real role_private_worker enqueue/COMMIT keeps legacy WSC source visible");
    assert!(row.get::<_, bool>(5));
}

#[test]
#[allow(clippy::too_many_lines)]
fn evidence_owner_marker_matrix_is_exact_and_real_updates_are_closed() {
    let Some(dsn) = required_dsn() else { return };
    let f = Fixture::new(dsn);
    let mut admin = f.admin();

    admin.batch_execute("BEGIN").unwrap();
    set_context(&mut admin, f.tenant, f.workspace, f.principal, Some(f.user));
    // dep: PostgreSQL(role_migration_owner) — role switch before the scoped statements for `evidence_owner_marker_matrix_is_exact_and_real_updates_are_closed`
    admin
        .batch_execute("SET LOCAL ROLE role_migration_owner")
        .unwrap();
    let legacy_plain: i64 = admin
        .query_one(
            "SELECT count(*) FROM private.evidence_objects WHERE evidence_id=$1",
            &[&f.cross_workspace_evidence],
        )
        .unwrap()
        .get(0);
    let legacy_locked: i64 = admin
        .query_one(
            "SELECT count(*) FROM (SELECT evidence_id FROM private.evidence_objects \
             WHERE evidence_id=$1 FOR SHARE) locked",
            &[&f.cross_workspace_evidence],
        )
        .unwrap()
        .get(0);
    let legacy_updated = admin
        .execute(
            "UPDATE private.evidence_objects SET payload_sha256=payload_sha256 WHERE evidence_id=$1",
            &[&f.cross_workspace_evidence],
        )
        .unwrap();
    assert_eq!((legacy_plain, legacy_locked, legacy_updated), (1, 1, 1));
    admin.batch_execute("ROLLBACK").unwrap();

    admin.batch_execute("BEGIN").unwrap();
    set_context(&mut admin, f.tenant, f.workspace, f.principal, None);
    // dep: PostgreSQL(role_migration_owner) — role switch before the scoped statements for `evidence_owner_marker_matrix_is_exact_and_real_updates_are_closed`
    admin
        .batch_execute(
            "SET LOCAL humaux.continuity_publish='1'; SET LOCAL ROLE role_migration_owner",
        )
        .unwrap();
    let exact_plain: i64 = admin
        .query_one(
            "SELECT count(*) FROM private.evidence_objects WHERE evidence_id=$1",
            &[&f.evidence],
        )
        .unwrap()
        .get(0);
    let exact_locked: i64 = admin
        .query_one(
            "SELECT count(*) FROM (SELECT evidence_id FROM private.evidence_objects \
             WHERE evidence_id=$1 FOR SHARE) locked",
            &[&f.evidence],
        )
        .unwrap()
        .get(0);
    assert_eq!((exact_plain, exact_locked), (1, 1));
    let update_error = admin
        .execute(
            "UPDATE private.evidence_objects SET payload_sha256=payload_sha256 WHERE evidence_id=$1",
            &[&f.evidence],
        )
        .unwrap_err();
    assert_eq!(state(&update_error), Some("42501"));
    admin.batch_execute("ROLLBACK").unwrap();

    admin.batch_execute("BEGIN").unwrap();
    set_context(&mut admin, f.tenant, f.workspace, f.principal, None);
    // dep: PostgreSQL(role_migration_owner) — role switch before the scoped statements for `evidence_owner_marker_matrix_is_exact_and_real_updates_are_closed`
    admin
        .batch_execute("SET LOCAL ROLE role_migration_owner")
        .unwrap();
    let legacy_cannot_see_exact: i64 = admin
        .query_one(
            "SELECT count(*) FROM private.evidence_objects WHERE evidence_id=$1",
            &[&f.evidence],
        )
        .unwrap()
        .get(0);
    assert_eq!(legacy_cannot_see_exact, 0);
    admin.batch_execute("ROLLBACK").unwrap();

    admin.batch_execute("BEGIN").unwrap();
    set_context(&mut admin, f.tenant, f.workspace, f.principal, Some(f.user));
    // dep: PostgreSQL(role_migration_owner) — role switch before the scoped statements for `evidence_owner_marker_matrix_is_exact_and_real_updates_are_closed`
    admin
        .batch_execute(
            "SET LOCAL humaux.continuity_publish='1'; SET LOCAL ROLE role_migration_owner",
        )
        .unwrap();
    let exact_cannot_see_legacy: i64 = admin
        .query_one(
            "SELECT count(*) FROM private.evidence_objects WHERE evidence_id=$1",
            &[&f.cross_workspace_evidence],
        )
        .unwrap()
        .get(0);
    let exact_cannot_lock_legacy: i64 = admin
        .query_one(
            "SELECT count(*) FROM (SELECT evidence_id FROM private.evidence_objects \
             WHERE evidence_id=$1 FOR SHARE) locked",
            &[&f.cross_workspace_evidence],
        )
        .unwrap()
        .get(0);
    let exact_cannot_update_legacy = admin
        .execute(
            "UPDATE private.evidence_objects SET payload_sha256=payload_sha256 WHERE evidence_id=$1",
            &[&f.cross_workspace_evidence],
        )
        .unwrap();
    assert_eq!(
        (
            exact_cannot_see_legacy,
            exact_cannot_lock_legacy,
            exact_cannot_update_legacy,
        ),
        (0, 0, 0)
    );
    admin.batch_execute("ROLLBACK").unwrap();
}

#[test]
fn evidence_for_share_blocks_hash_change_until_publish_commit() {
    let Some(dsn) = required_dsn() else { return };
    let f = Fixture::new(dsn);
    let project = f.project("evidence-lock");
    let mut gateway = f.gateway("continuity_evidence_lock");
    gateway.batch_execute("BEGIN").unwrap();
    publish_in_open_tx(
        &mut gateway,
        f.tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "GOAL",
        0,
        &json!({"evidence_lock":true}),
        &[],
        &[],
        &[f.evidence],
        std::slice::from_ref(&f.evidence_hash),
    )
    .unwrap();
    let admin_dsn = f.admin_dsn.clone();
    let evidence = f.evidence;
    let worker = thread::spawn(move || {
        // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
        let mut admin = Client::connect(&admin_dsn, NoTls).unwrap();
        admin
            .execute(
                "UPDATE private.evidence_objects SET payload_sha256=$2 WHERE evidence_id=$1",
                &[&evidence, &vec![0x44_u8; 32]],
            )
            .unwrap();
    });
    let mut admin = f.admin();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut blocked = false;
    while Instant::now() < deadline {
        blocked = admin
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity a WHERE a.query LIKE \
             'UPDATE private.evidence_objects SET payload_sha256=%' \
             AND cardinality(pg_blocking_pids(a.pid))>0)",
                &[],
            )
            .unwrap()
            .get(0);
        if blocked {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(blocked, "Evidence update did not wait on FOR SHARE");
    gateway.batch_execute("COMMIT").unwrap();
    worker.join().unwrap();
    let error = publish(
        &mut gateway,
        f.tenant,
        f.workspace,
        project,
        f.principal,
        None,
        "DECISIONS",
        0,
        &json!({"changed_hash":true}),
        &[],
        &[],
        &[f.evidence],
        std::slice::from_ref(&f.evidence_hash),
    )
    .unwrap_err();
    assert_eq!(state(&error), Some("23514"));
    let residue: i64 = admin
        .query_one(
            "SELECT count(*) FROM private.continuity_facet_versions \
             WHERE project_id=$1 AND facet_kind='DECISIONS'",
            &[&project],
        )
        .unwrap()
        .get(0);
    assert_eq!(residue, 0);
}
