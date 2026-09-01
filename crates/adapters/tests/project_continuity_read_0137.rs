use humaux_adapters::{continuity_read::PostgresContinuityReadPort, postgres::RuntimeDbPool};
use humaux_application::continuity::read_project_continuity;
use humaux_domain::{
    context::ContextBudget,
    continuity::ProjectId,
    error::ErrorCode,
    identity::{AuthorizationScope, BoundedSet, PrincipalId},
    ids::{TenantId, UserId, WorkspaceId},
};
use postgres::{Client, GenericClient, NoTls};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

#[path = "support/continuity_0137_cleanup.rs"]
mod continuity_0137_cleanup;
use continuity_0137_cleanup::CleanupOwner;

const NIL: Uuid = Uuid::nil();

#[allow(clippy::too_many_arguments)]
fn register_fixture_ids(
    cleanup: &CleanupOwner,
    tenant: Uuid,
    user: Uuid,
    workspace: Uuid,
    other_workspace: Uuid,
    domain: Uuid,
    project: Uuid,
    memory: Uuid,
    evidence: Uuid,
) {
    cleanup.register_tenant(tenant);
    cleanup.register_user(user);
    cleanup.register_workspace(workspace);
    cleanup.register_workspace(other_workspace);
    cleanup.register_domain(domain);
    cleanup.register_project(project);
    cleanup.register_memory(memory);
    cleanup.register_evidence(evidence);
}

fn required_dsn() -> Option<String> {
    match std::env::var("HUMAUX_TEST_PG_DSN") {
        Ok(dsn) => Some(dsn),
        Err(_) if std::env::var("HUMAUX_REQUIRE_DB").as_deref() == Ok("1") => {
            panic!("HUMAUX_REQUIRE_DB=1 requires isolated HUMAUX_TEST_PG_DSN")
        }
        Err(_) => None,
    }
}

fn role_dsn(base: &str, role: &str, password: &str, app: &str) -> String {
    // Swap the credential pair of whatever admin DSN the caller supplied — the host/port/db
    // suffix is what we need, the admin password is not fixed by contract (local dev, CI and
    // the compose file each use a different one, so a hardcoded `postgres:postgres@` prefix
    // makes this fixture non-portable and panics everywhere but one machine).
    // Same shape as `dsn_as_role` in tests/mandatory_context_lane.rs.
    let suffix = base
        .strip_prefix("postgres://")
        .or_else(|| base.strip_prefix("postgresql://"))
        .and_then(|rest| rest.split_once('@').map(|(_creds, host)| host))
        .expect("HUMAUX_TEST_PG_DSN must be postgres://<user>:<password>@<host>/<db>");
    let separator = if suffix.contains('?') { '&' } else { '?' };
    format!("postgres://{role}:{password}@{suffix}{separator}application_name={app}")
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
            "SELECT set_config('humaux.tenant_id',$1,true),\
                    set_config('humaux.workspace_id',$2,true),\
                    set_config('humaux.principal_id',$3,true),\
                    set_config('humaux.user_id',$4,true)",
            &[
                &tenant.to_string(),
                &workspace.to_string(),
                &principal.to_string(),
                &user.unwrap_or(NIL).to_string(),
            ],
        )
        .expect("set W1 write context");
}

struct Fixture {
    admin_dsn: String,
    gateway_dsn: String,
    tenant: Uuid,
    user: Uuid,
    principal: Uuid,
    workspace: Uuid,
    other_workspace: Uuid,
    project: Uuid,
    memory: Uuid,
    _cleanup: Arc<CleanupOwner>,
}

impl Fixture {
    #[allow(clippy::too_many_lines)]
    fn new(admin_dsn: String) -> Self {
        let cleanup = Arc::new(CleanupOwner::new(admin_dsn.clone()));
        let gateway_dsn = role_dsn(
            &admin_dsn,
            "role_gateway",
            "devlocal_role_gateway",
            "continuity_w2",
        );
        let tenant = Uuid::now_v7();
        let user = Uuid::now_v7();
        let principal = Uuid::now_v7();
        let workspace = Uuid::now_v7();
        let other_workspace = Uuid::now_v7();
        let project = Uuid::now_v7();
        let domain = Uuid::now_v7();
        let memory = Uuid::now_v7();
        let evidence = Uuid::now_v7();
        let evidence_hash = vec![0x44_u8; 32];
        register_fixture_ids(
            &cleanup,
            tenant,
            user,
            workspace,
            other_workspace,
            domain,
            project,
            memory,
            evidence,
        );
        let content = json!({"w2":"authoritative memory"});
        let mut admin = Client::connect(&admin_dsn, NoTls).expect("admin connect");
        admin
            .execute(
                "INSERT INTO control.tenants(tenant_id,name,state) VALUES($1,'w2','ACTIVE')",
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
                "INSERT INTO control.memberships(tenant_id,user_id,role,state)\
                 VALUES($1,$2,'OWNER','ACTIVE')",
                &[&tenant, &user],
            )
            .unwrap();
        for workspace_id in [workspace, other_workspace] {
            admin
                .execute(
                    "INSERT INTO control.workspaces(workspace_id,tenant_id,name)\
                     VALUES($1,$2,'w2')",
                    &[&workspace_id, &tenant],
                )
                .unwrap();
        }
        admin
            .execute(
                "INSERT INTO control.private_reasoning_domains(\
                   reasoning_domain_id,tenant_id,name) VALUES($1,$2,'w2')",
                &[&domain, &tenant],
            )
            .unwrap();
        admin.batch_execute("BEGIN").unwrap();
        admin
            .execute(
                "INSERT INTO private.evidence_objects(\
                   evidence_id,tenant_id,evidence_kind,payload_sha256,data_class,origin_class,\
                   visibility_class,reasoning_domain_id)\
                 VALUES($1,$2,'EVENT',$3,'PRIVATE','AuthenticatedAgent','TENANT_SHARED',$4)",
                &[&evidence, &tenant, &evidence_hash, &domain],
            )
            .unwrap();
        admin
            .execute(
                "INSERT INTO private.memory_records(\
                   memory_id,tenant_id,memory_type,content,visibility_class,authority_class,\
                   confidence,status,asserted_at)\
                 VALUES($1,$2,'FACT',$3,'TENANT_SHARED','ProjectDecision',1,'active',now())",
                &[&memory, &tenant, &content],
            )
            .unwrap();
        admin
            .execute(
                "INSERT INTO private.memory_evidence(\
                   memory_id,evidence_id,role,grounding_mode,recorded_version)\
                 VALUES($1,$2,'SUPPORTING','IMMUTABLE',NULL)",
                &[&memory, &evidence],
            )
            .unwrap();
        admin.batch_execute("COMMIT").unwrap();
        let memory_hash: Vec<u8> = admin
            .query_one(
                "SELECT sha256(convert_to(content::text,'UTF8'))\
                 FROM private.memory_records WHERE tenant_id=$1 AND memory_id=$2",
                &[&tenant, &memory],
            )
            .unwrap()
            .get(0);
        let mut gateway = Client::connect(&gateway_dsn, NoTls).expect("gateway connect");
        gateway.batch_execute("BEGIN").unwrap();
        set_context(&mut gateway, tenant, workspace, principal, Some(user));
        gateway
            .query_one(
                "SELECT private.register_continuity_project($1,$2,$3,$4,$5,'w2')",
                &[&tenant, &workspace, &project, &principal, &Some(user)],
            )
            .unwrap();
        gateway.batch_execute("COMMIT; BEGIN").unwrap();
        set_context(&mut gateway, tenant, workspace, principal, Some(user));
        gateway
            .query_one(
                "SELECT facet_version_id FROM private.publish_continuity_facet(\
                   $1,$2,$3,$4,$5,'GOAL',0,'CURRENT',$6,$7,$8,$9,$10)",
                &[
                    &tenant,
                    &workspace,
                    &project,
                    &principal,
                    &Some(user),
                    &json!({"goal":"ship W2"}),
                    &vec![memory],
                    &vec![memory_hash],
                    &Vec::<Uuid>::new(),
                    &Vec::<Vec<u8>>::new(),
                ],
            )
            .unwrap();
        gateway.batch_execute("COMMIT").unwrap();
        Self {
            admin_dsn,
            gateway_dsn,
            tenant,
            user,
            principal,
            workspace,
            other_workspace,
            project,
            memory,
            _cleanup: cleanup,
        }
    }

    fn authorization(&self, workspaces: &[Uuid]) -> AuthorizationScope {
        AuthorizationScope::new(
            TenantId(self.tenant),
            PrincipalId(self.principal),
            Some(UserId(self.user)),
            BoundedSet::new(workspaces.iter().copied().map(WorkspaceId)).unwrap(),
        )
    }

    fn publish_successor(&self) {
        let mut admin = Client::connect(&self.admin_dsn, NoTls).unwrap();
        let memory_hash: Vec<u8> = admin
            .query_one(
                "SELECT sha256(convert_to(content::text,'UTF8')) FROM private.memory_records \
                 WHERE tenant_id=$1 AND memory_id=$2",
                &[&self.tenant, &self.memory],
            )
            .unwrap()
            .get(0);
        let mut gateway = Client::connect(&self.gateway_dsn, NoTls).unwrap();
        gateway.batch_execute("BEGIN").unwrap();
        set_context(
            &mut gateway,
            self.tenant,
            self.workspace,
            self.principal,
            Some(self.user),
        );
        gateway
            .query_one(
                "SELECT facet_version_id FROM private.publish_continuity_facet(\
                 $1,$2,$3,$4,$5,'GOAL',1,'CURRENT',$6,$7,$8,$9,$10)",
                &[
                    &self.tenant,
                    &self.workspace,
                    &self.project,
                    &self.principal,
                    &Some(self.user),
                    &json!({"goal":"successor"}),
                    &vec![self.memory],
                    &vec![memory_hash],
                    &Vec::<Uuid>::new(),
                    &Vec::<Vec<u8>>::new(),
                ],
            )
            .unwrap();
        gateway.batch_execute("COMMIT").unwrap();
    }
}

#[test]
fn migration_and_one_rr_static_contract_are_closed() {
    let sql = include_str!("../../../migrations/0137_project_continuity_read.sql");
    let manifest = include_str!("../../../migrations/0137_project_continuity_read.manifest.toml");
    let adapter = include_str!("../src/continuity_read.rs");
    assert_eq!(sql.matches("CREATE FUNCTION").count(), 1);
    assert!(!sql.contains("CREATE TABLE") && !sql.contains("CREATE POLICY"));
    for witness in [
        "SECURITY DEFINER",
        "SET search_path TO pg_catalog",
        "project.workspace_id=ANY(p_authorized_workspace_ids)",
        "project.lifecycle_state='ACTIVE'",
        "version.body::text",
        "selected_is_latest",
        "max(candidate.facet_version)",
        "continuity_facet_versions orphan",
        "memory_links_exact",
        "evidence_links_exact",
        "damaged.tenant_id=version.tenant_id",
        "ORDER BY link.memory_id",
        "ORDER BY link.evidence_id",
        "pg_current_snapshot()::text",
    ] {
        assert!(sql.contains(witness), "missing SQL witness {witness}");
    }
    for witness in [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
        "install_lookup_context",
        "install_workspace_context",
        "fetch_frozen_in_txn",
        "final_memory_ids_in_txn",
        "association_count",
        "has_tombstone",
        "CannotEstablishCompleteness",
    ] {
        assert!(
            adapter.contains(witness),
            "missing adapter witness {witness}"
        );
    }
    assert!(manifest.contains("role_gateway"));
    assert!(!adapter.contains("provider") && !adapter.contains("cache"));
}

#[test]
fn authorized_read_is_current_then_revoked_source_is_non_leaking_stale() {
    let Some(dsn) = required_dsn() else { return };
    let fixture = Fixture::new(dsn);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let pool = Arc::new(
        runtime
            .block_on(RuntimeDbPool::connect(&fixture.gateway_dsn))
            .expect("checked Gateway pool"),
    );
    let adapter = PostgresContinuityReadPort::new(pool);
    let budget = ContextBudget::new(10_000, 10_000).unwrap();
    let authorization = fixture.authorization(&[fixture.other_workspace, fixture.workspace]);
    let current = runtime
        .block_on(read_project_continuity(
            &adapter,
            &authorization,
            ProjectId(fixture.project),
            None,
            budget,
        ))
        .expect("authorized parent and immutable source");
    let current = serde_json::to_value(current).unwrap();
    assert_eq!(current["facets"].as_array().unwrap().len(), 17);
    assert_eq!(current["facets"][0]["kind"], "GOAL");
    assert_eq!(current["facets"][0]["status"], "CURRENT");
    assert_eq!(current["facets"][0]["current"]["body"]["goal"], "ship W2");
    assert_eq!(current["facets"][13]["kind"], "HANDOFF");
    assert_eq!(current["facets"][16]["kind"], "COVERAGE");
    assert_eq!(current["coverage"]["required"], 17);
    assert_eq!(current["coverage"]["unavailable"], 6);

    let empty = fixture.authorization(&[]);
    assert_eq!(
        runtime
            .block_on(read_project_continuity(
                &adapter,
                &empty,
                ProjectId(fixture.project),
                None,
                budget,
            ))
            .unwrap_err(),
        ErrorCode::NotFound
    );
    assert_eq!(
        runtime
            .block_on(read_project_continuity(
                &adapter,
                &authorization,
                ProjectId(fixture.project),
                Some(WorkspaceId(fixture.other_workspace)),
                budget,
            ))
            .unwrap_err(),
        ErrorCode::NotFound
    );

    let mut admin = Client::connect(&fixture.admin_dsn, NoTls).unwrap();
    admin
        .execute(
            "UPDATE private.memory_records SET status='revoked'\
                 WHERE tenant_id=$1 AND memory_id=$2",
            &[&fixture.tenant, &fixture.memory],
        )
        .unwrap();
    let stale = runtime
        .block_on(read_project_continuity(
            &adapter,
            &authorization,
            ProjectId(fixture.project),
            Some(WorkspaceId(fixture.workspace)),
            budget,
        ))
        .expect("source failure is a facet state");
    let stale = serde_json::to_value(stale).unwrap();
    assert_eq!(stale["facets"][0]["status"], "STALE");
    assert_eq!(
        stale["facets"][0]["diagnostic_code"],
        "SOURCE_REVALIDATION_FAILED"
    );
    assert!(stale["facets"][0].get("current").is_none());
}

#[test]
fn owner_pointer_rollback_and_orphan_version_cannot_become_current_or_missing() {
    let Some(dsn) = required_dsn() else { return };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for fault in ["rollback", "orphan"] {
        let fixture = Fixture::new(dsn.clone());
        if fault == "rollback" {
            fixture.publish_successor();
        }
        let mut admin = Client::connect(&fixture.admin_dsn, NoTls).unwrap();
        if fault == "rollback" {
            let v1: Uuid = admin
                .query_one(
                    "SELECT facet_version_id FROM private.continuity_facet_versions \
                     WHERE tenant_id=$1 AND project_id=$2 AND facet_kind='GOAL' \
                       AND facet_version=1",
                    &[&fixture.tenant, &fixture.project],
                )
                .unwrap()
                .get(0);
            admin
                .execute(
                    "UPDATE private.continuity_facet_slots SET slot_version=1, \
                     current_version_id=$3,slot_state='CURRENT' \
                     WHERE tenant_id=$1 AND project_id=$2 AND facet_kind='GOAL'",
                    &[&fixture.tenant, &fixture.project, &v1],
                )
                .unwrap();
        } else {
            admin
                .execute(
                    "UPDATE private.continuity_facet_slots SET slot_version=0, \
                     current_version_id=NULL,slot_state=NULL \
                     WHERE tenant_id=$1 AND project_id=$2 AND facet_kind='GOAL'",
                    &[&fixture.tenant, &fixture.project],
                )
                .unwrap();
        }
        runtime.block_on(async {
            let pool = Arc::new(RuntimeDbPool::connect(&fixture.gateway_dsn).await.unwrap());
            let adapter = PostgresContinuityReadPort::new(pool);
            let result = read_project_continuity(
                &adapter,
                &fixture.authorization(&[fixture.workspace]),
                ProjectId(fixture.project),
                None,
                ContextBudget::new(10_000, 10_000).unwrap(),
            )
            .await;
            assert_eq!(
                result.unwrap_err(),
                ErrorCode::CannotEstablishCompleteness,
                "{fault}"
            );
        });
    }
}
