//! §25.4/§25.5 Mandatory Context Lane 的 DB 判据（DOD-020，phase=7 欠账）。
//!
//! 本文件打三件事：
//! 1. **probe 的缺失对象是探测出来的，而且两个方向都打**——`REGISTRY` 自己声明的依赖逐条
//!    查目录，满足契约的列**不得**被报成缺失（`probe_reports_the_columns_that_are_actually_missing`），
//!    而一条真的不满足的依赖**必须**被报成缺失（`an_unsatisfiable_requirement_is_reported_missing`
//!    ：把生成列 `private.memory_records.facet` 临时换成同名可写列，种出不满足的那一侧）。
//!    只打前一半的话 `probe_required_columns` 整个改成 `Ok(None)` 也照样绿——今天目录里
//!    五个 selector 的依赖全部满足（ADR-0006；card 22b 之后 task 维度住在
//!    `private.context_bindings`，facet 是生成列）。
//! 2. **selector 是机械规则，不是相似度**——种一条 ProjectConstraint，它必须被选出来；
//!    种一条同样文本但 authority 不够的，必须选不出来。
//! 3. **binding 的撤销立即生效**——§25.5 的正对照，最容易漏的那条。
//!
//! 三态（§79.2）由 `testkit::skip_or_fail` 统一判定；CI 声明 `HUMAUX_REQUIRE_DB=1` 时
//! 跳过即失败（ADR-0005）。

use humaux_adapters::context_repo;
use humaux_adapters::postgres::RuntimeDbPool;
use humaux_domain::context::SelectorId;
use humaux_domain::error::ErrorCode;
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId};
use humaux_domain::ids::{Scope, TenantId, UserId, WorkspaceId};
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use sqlx::postgres::PgConnectOptions;
use std::str::FromStr;
use uuid::Uuid;

const NAME: &str = "mandatory_context_lane";
const REQUEST_GUARD_DB: &str = "humaux_thread_request_guard_20260828";

/// Uses the isolated fixture's real gateway LOGIN. This fixture deliberately refuses a
/// privileged connection plus `SET ROLE`, which cannot prove the request-pool boundary.
fn gateway_dsn() -> Option<String> {
    let Ok(dsn) = std::env::var("HUMAUX_GATEWAY_PG_DSN") else {
        skip_or_fail(
            NAME,
            "missing object: role_gateway PostgreSQL DSN",
            ExternalDep::Postgres,
        );
        return None;
    };
    let Ok(options) = PgConnectOptions::from_str(&dsn) else {
        skip_or_fail(
            NAME,
            "invalid object: role_gateway PostgreSQL DSN",
            ExternalDep::Postgres,
        );
        return None;
    };
    if options.get_username() != "role_gateway"
        || options.get_host() != "127.0.0.1"
        || options.get_port() != 61719
        || options.get_database() != Some(REQUEST_GUARD_DB)
        || dsn.contains(['?', '#'])
    {
        skip_or_fail(
            NAME,
            "invalid object: isolated role_gateway fixture DSN",
            ExternalDep::Postgres,
        );
        return None;
    }
    let Ok(mut gateway) = Client::connect(&dsn, NoTls) else {
        skip_or_fail(
            NAME,
            "missing object: role_gateway PostgreSQL login",
            ExternalDep::Postgres,
        );
        return None;
    };
    let Ok(role_ok) = gateway.query_one(
        "SELECT current_user='role_gateway' AND session_user='role_gateway' \
         AND NOT (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user)",
        &[],
    ) else {
        skip_or_fail(
            NAME,
            "missing object: role_gateway session identity probe",
            ExternalDep::Postgres,
        );
        return None;
    };
    if !role_ok.get::<_, bool>(0) {
        skip_or_fail(
            NAME,
            "invalid object: role_gateway must be LOGIN, non-superuser, non-bypassrls",
            ExternalDep::Postgres,
        );
        return None;
    }
    Some(dsn)
}

struct Fixture {
    admin: Client,
    tenant_id: Uuid,
    evidence_id: Uuid,
    user_ids: Vec<Uuid>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM private.context_bindings WHERE tenant_id = '{0}'; \
             DELETE FROM private.memory_evidence WHERE memory_id IN \
               (SELECT memory_id FROM private.memory_records WHERE tenant_id = '{0}'); \
             DELETE FROM private.memory_records WHERE tenant_id = '{0}'; \
             DELETE FROM private.events WHERE event_id IN \
               (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id = '{0}'); \
             DELETE FROM private.evidence_objects WHERE tenant_id = '{0}'; \
             DELETE FROM control.private_reasoning_domains WHERE tenant_id = '{0}'; \
             DELETE FROM control.memberships WHERE tenant_id = '{0}'; \
             DELETE FROM control.workspaces WHERE tenant_id = '{0}'; \
             DELETE FROM control.tenants WHERE tenant_id = '{0}';",
            self.tenant_id
        ));
        for user_id in &self.user_ids {
            let _ = self
                .admin
                .execute("DELETE FROM control.users WHERE user_id = $1", &[user_id]);
        }
    }
}

/// 建 fixture。返回 `None` 时已经打印过 SKIP（或在声明模式下 panic 过）。
fn setup() -> Option<(Fixture, String)> {
    let Ok(dsn) = std::env::var("HUMAUX_TEST_PG_DSN") else {
        skip_or_fail(NAME, "missing object: Postgres DSN", ExternalDep::Postgres);
        return None;
    };
    let Ok(options) = PgConnectOptions::from_str(&dsn) else {
        skip_or_fail(
            NAME,
            "invalid object: owner PostgreSQL DSN",
            ExternalDep::Postgres,
        );
        return None;
    };
    if options.get_host() != "127.0.0.1"
        || options.get_port() != 61719
        || options.get_database() != Some(REQUEST_GUARD_DB)
        || dsn.contains(['?', '#'])
    {
        skip_or_fail(
            NAME,
            "invalid object: isolated owner fixture DSN",
            ExternalDep::Postgres,
        );
        return None;
    }
    let Ok(mut admin) = Client::connect(&dsn, NoTls) else {
        skip_or_fail(NAME, "missing object: live Postgres", ExternalDep::Postgres);
        return None;
    };
    let gateway_dsn = gateway_dsn()?;
    // 迁移未应用这条腿点名 0102 的产物。注意：**触发器/列在不在是断言不是 skip 条件**
    // 的那条规矩（ADR-0006）针对的是「被测判据本身」；这里 `mode` 列是**被测对象所在的
    // 表结构**，它不在就等于整个 lane 无从谈起，属于合法的 NA。
    let migrated: bool = admin
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
             WHERE table_schema='private' AND table_name='context_bindings' \
               AND column_name='mode')",
            &[],
        )
        .ok()?
        .get(0);
    if !migrated {
        skip_or_fail(
            NAME,
            "missing object: private.context_bindings.mode — run `cargo xtask migrate` \
             (migrations/0102_context_bindings_mode_scope.sql)",
            ExternalDep::Postgres,
        );
        return None;
    }

    let tenant_id: Uuid = admin
        .query_one(
            "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
            &[&"mandatory_context_lane throwaway tenant"],
        )
        .ok()?
        .get(0);
    let reasoning_domain_id: Uuid = admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
             VALUES ($1, 'mandatory_context_lane domain') RETURNING reasoning_domain_id",
            &[&tenant_id],
        )
        .ok()?
        .get(0);
    let evidence_id: Uuid = admin
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                visibility_class, reasoning_domain_id) \
             VALUES ($1, 'EVENT', $2, 'INTERNAL', 'DirectUserInput', 'TENANT_SHARED', $3) \
             RETURNING evidence_id",
            &[&tenant_id, &vec![0u8; 32], &reasoning_domain_id],
        )
        .ok()?
        .get(0);
    admin
        .execute(
            "INSERT INTO private.events (event_id, event_kind, payload) \
             VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)",
            &[&evidence_id],
        )
        .ok()?;

    Some((
        Fixture {
            admin,
            tenant_id,
            evidence_id,
            user_ids: Vec::new(),
        },
        gateway_dsn,
    ))
}

/// 播一条 memory（含 §8.6 要求的 evidence 链），返回 memory_id。
///
/// `grounding_mode` 决定这条 memory 在装配快照里的 grounding 结论（migration 0100 语义）：
/// `SNAPSHOT` ⇒ 无 LIVE edge ⇒ CURRENT ⇒ 进 lane；`LIVE`（且不给 recorded_version）⇒
/// RECHECK_REQUIRED ⇒ 被铸造门分流进 needs_verification（DOD-093）。**0100 的默认回填
/// 正是 LIVE+NULL**——所以本 fixture 必须显式选档，否则种出来的行全都进不了 lane
/// （这不是 bug，是门在工作；本文件第一次跑红正是这样发现的）。
fn seed_memory(f: &mut Fixture, authority: &str, grounding_mode: &str) -> Uuid {
    let confidence: f32 = 0.9;
    let mut txn = f.admin.transaction().expect("begin");
    let memory_id: Uuid = txn
        .query_one(
            "INSERT INTO private.memory_records \
               (tenant_id, memory_type, content, visibility_class, \
                authority_class, confidence, status, asserted_at) \
             VALUES ($1, 'NOTE', $2, 'TENANT_SHARED', $3, $4, 'active', now()) \
             RETURNING memory_id",
            &[
                &f.tenant_id,
                &serde_json::json!({"fixture": "mandatory_context_lane"}),
                &authority,
                &confidence,
            ],
        )
        .expect("insert memory")
        .get(0);
    txn.execute(
        "INSERT INTO private.memory_evidence (memory_id, evidence_id, role, grounding_mode) \
         VALUES ($1, $2, 'PRIMARY', $3)",
        &[&memory_id, &f.evidence_id, &grounding_mode],
    )
    .expect("link evidence (§8.6)");
    txn.commit().expect("commit");
    memory_id
}

fn scope_for(tenant_id: Uuid) -> Scope {
    Scope {
        tenant_id: TenantId(tenant_id),
        user_id: None,
        workspace_id: None,
        repository_id: None,
        task_id: None,
        run_id: None,
        agent_id: None,
    }
}

fn authorization_for(tenant_id: Uuid) -> AuthorizationScope {
    AuthorizationScope::new(
        TenantId(tenant_id),
        PrincipalId::new(),
        None,
        BoundedSet::new([]).expect("empty workspace grant set"),
    )
}

fn seed_user(f: &mut Fixture) -> Uuid {
    let user_id: Uuid = f
        .admin
        .query_one(
            "INSERT INTO control.users(state) VALUES ('ACTIVE') RETURNING user_id",
            &[],
        )
        .expect("insert active user")
        .get(0);
    f.admin
        .execute(
            "INSERT INTO control.memberships(tenant_id, user_id, role, state) \
             VALUES ($1, $2, 'member', 'ACTIVE')",
            &[&f.tenant_id, &user_id],
        )
        .expect("insert active membership");
    f.user_ids.push(user_id);
    user_id
}

fn seed_workspace(f: &mut Fixture, name: &str) -> Uuid {
    f.admin
        .query_one(
            "INSERT INTO control.workspaces(tenant_id, name) VALUES ($1, $2) \
             RETURNING workspace_id",
            &[&f.tenant_id, &name],
        )
        .expect("insert workspace")
        .get(0)
}

fn authorization_for_user(
    tenant_id: Uuid,
    user_id: Uuid,
    workspaces: impl IntoIterator<Item = Uuid>,
) -> AuthorizationScope {
    AuthorizationScope::new(
        TenantId(tenant_id),
        PrincipalId::new(),
        Some(UserId(user_id)),
        BoundedSet::new(workspaces.into_iter().map(WorkspaceId))
            .expect("bounded workspace grant set"),
    )
}

fn scoped(tenant_id: Uuid, user_id: Uuid, workspace_id: Option<Uuid>) -> Scope {
    Scope {
        tenant_id: TenantId(tenant_id),
        user_id: Some(UserId(user_id)),
        workspace_id: workspace_id.map(WorkspaceId),
        repository_id: None,
        task_id: None,
        run_id: None,
        agent_id: None,
    }
}

fn seed_evidence(
    f: &mut Fixture,
    visibility_class: &str,
    visibility_user_id: Option<Uuid>,
    visibility_workspace_id: Option<Uuid>,
) -> Uuid {
    f.admin
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                visibility_class, visibility_user_id, visibility_workspace_id, reasoning_domain_id) \
             VALUES ($1, 'EVENT', $2, 'INTERNAL', 'DirectUserInput', $3, $4, $5, \
               (SELECT reasoning_domain_id FROM control.private_reasoning_domains \
                WHERE tenant_id = $1 LIMIT 1)) RETURNING evidence_id",
            &[
                &f.tenant_id,
                &vec![1_u8; 32],
                &visibility_class,
                &visibility_user_id,
                &visibility_workspace_id,
            ],
        )
        .expect("insert visible evidence")
        .get(0)
}

fn seed_memory_with_visibility(
    f: &mut Fixture,
    visibility_class: &str,
    visibility_user_id: Option<Uuid>,
    visibility_workspace_id: Option<Uuid>,
    evidence_id: Uuid,
) -> Uuid {
    let confidence: f32 = 0.9;
    let mut txn = f.admin.transaction().expect("begin");
    let memory_id: Uuid = txn
        .query_one(
            "INSERT INTO private.memory_records \
               (tenant_id, memory_type, content, visibility_class, visibility_user_id, \
                visibility_workspace_id, authority_class, confidence, status, asserted_at) \
             VALUES ($1, 'NOTE', $2, $3, $4, $5, 'ProjectConstraint', $6, 'active', now()) \
             RETURNING memory_id",
            &[
                &f.tenant_id,
                &serde_json::json!({"fixture": "context authorization"}),
                &visibility_class,
                &visibility_user_id,
                &visibility_workspace_id,
                &confidence,
            ],
        )
        .expect("insert memory")
        .get(0);
    txn.execute(
        "INSERT INTO private.memory_evidence(memory_id, evidence_id, role, grounding_mode) \
         VALUES ($1, $2, 'PRIMARY', 'SNAPSHOT')",
        &[&memory_id, &evidence_id],
    )
    .expect("link evidence");
    txn.commit().expect("commit");
    memory_id
}

/// `pg_attribute.attgenerated` —— 列不在时 `None`，在且非生成列时 `Some("")`。
///
/// probe 自己查的就是这一列（`context_repo::probe_required_columns`）；两条判据都从这里
/// 取「目录实况」，免得判据和被判对象各写一份 SQL 再慢慢漂开。
fn attgenerated(admin: &mut Client, schema: &str, table: &str, column: &str) -> Option<String> {
    admin
        .query_opt(
            "SELECT a.attgenerated::text FROM pg_attribute a \
               JOIN pg_class c ON c.oid = a.attrelid \
               JOIN pg_namespace n ON n.oid = c.relnamespace \
              WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3 \
                AND a.attnum > 0 AND NOT a.attisdropped",
            &[&schema, &table, &column],
        )
        .expect("probe truth")
        .map(|row| row.get(0))
}

/// 目录里 facet 的生成列契约被临时换成一个**同名可写列**，`Drop` 时换回去。
///
/// 改名与补列在同一条 simple query 里（= 一个隐式事务，ACCESS EXCLUSIVE 锁全程持有），
/// 所以别的会话永远看不到一个**没有 `facet` 列**的 `memory_records`——变的只是契约
/// （可写 vs `GENERATED ALWAYS ... STORED`），引用 `m.facet` 的 selector SQL 照常编译。
/// 还原写在 `Drop` 里而不是测试末尾：中间任何一条断言红了都不能把影子留在夹具库里。
///
/// ponytail: 影子窗口是进程外可见的。`cargo test` 逐个跑 test 二进制，所以同库的
/// `facet_contract.rs` 不会撞上；换成并行 runner（nextest）要给这两个二进制加库级串行。
struct FacetShadow {
    admin: Client,
}

impl FacetShadow {
    /// `dsn` 必须是 [`setup`] 已经校验过的 owner 夹具 DSN——这里做 DDL，认错库就是改生产。
    fn arm(dsn: &str) -> Self {
        let mut admin = Client::connect(dsn, NoTls).expect("owner login for the facet shadow");
        admin
            .batch_execute(
                "ALTER TABLE private.memory_records \
                   RENAME COLUMN facet TO facet__probe_shadow; \
                 ALTER TABLE private.memory_records ADD COLUMN facet text;",
            )
            .expect("shadow private.memory_records.facet");
        Self { admin }
    }
}

impl Drop for FacetShadow {
    fn drop(&mut self) {
        // 展开中再 panic 会 abort 掉整个测试进程，所以这里只喊，不 panic。
        if let Err(err) = self.admin.batch_execute(
            "ALTER TABLE private.memory_records DROP COLUMN facet; \
             ALTER TABLE private.memory_records \
               RENAME COLUMN facet__probe_shadow TO facet;",
        ) {
            eprintln!(
                "FATAL: private.memory_records.facet 的影子没能还原（{err}）——\
                 夹具库 {REQUEST_GUARD_DB} 现在带着一个可写的 facet 列"
            );
        }
    }
}

/// probe 报的缺失对象必须与目录的实况一致（**满足 ⇒ 不报**这一半）。
///
/// 写死一份「哪两列不存在」的清单今天也会绿——card 22b 之前这里正是那样写的
/// （`memory_records.task_id` / `.facet` 硬编码），而那份清单在 registry 改口的同一天就
/// 失效了。现在改成**从 `REGISTRY` 自己声明的依赖出发**逐条查目录：列一落地 probe 必须
/// 自动改口，registry 换一张表 probe 也必须跟着换，都不需要有人回来改一行代码（ADR-0006）。
///
/// 第四项 `stored_generated`（§25.4.A(11)）一并核：存在性不是 facet 的契约，
/// `attgenerated='s'` 才是；一个可写的同名列必须被判成缺失。
#[test]
fn probe_reports_the_columns_that_are_actually_missing() {
    let Some((mut f, dsn)) = setup() else { return };

    let mut truth = Vec::new();
    for spec in &humaux_domain::context::REGISTRY {
        for (schema, table, column, stored_generated) in spec.required_columns {
            let attgenerated = attgenerated(&mut f.admin, schema, table, column);
            let satisfied = match &attgenerated {
                None => false,
                Some(kind) => !*stored_generated || kind == "s",
            };
            truth.push((format!("{schema}.{table}.{column}"), satisfied));
        }
    }
    assert!(!truth.is_empty(), "REGISTRY declares no dependency at all");

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let pool = rt
        .block_on(RuntimeDbPool::connect(&dsn))
        .expect("gateway pool");
    let availability = rt
        .block_on(context_repo::probe_selectors(&pool))
        .expect("probe");

    for (name, satisfied) in truth {
        // The probe reports the first unsatisfied column of a selector, and suffixes the
        // generated-column case, so compare on the prefix rather than on equality.
        let reported = availability.iter().any(|a| {
            a.missing_object
                .as_deref()
                .is_some_and(|missing| missing.starts_with(name.as_str()))
        });
        assert!(
            !(satisfied && reported),
            "probe 报 {name} 缺失，但目录里它是满足契约的"
        );
    }
    // §25.4.A(11): the task selector must never probe `memory_records.task_id` again.
    assert!(
        !availability.iter().any(|a| a
            .missing_object
            .as_deref()
            .is_some_and(|missing| missing.contains("memory_records.task_id"))),
        "§25.4.A(11) forbids probing private.memory_records.task_id"
    );
}

/// 反向判据：**一条真的不满足的依赖必须被报成缺失**（ADR-0006 的另一半）。
///
/// 上面那条只证「满足 ⇒ 不报」。目录里五个 selector 的依赖今天全部满足，所以它的另一半
/// 是空跑的——把 `probe_required_columns` 整个改成 `Ok(None)` 也照样绿。这条把不满足的
/// 那一侧**种出来**再看 probe 认不认。
///
/// 种的正是 §25.4.A(11) 那条契约：facet 的判据不是「列在」，是 `attgenerated='s'`。
/// 把生成列改名让位给一个同名**可写**列，probe 必须报
/// `private.memory_records.facet (not stored-generated)`；还原之后必须不报——后半句才让
/// 前半句说明问题（否则一个逢列必报的 probe 也能过）。
///
/// 注错：probe 只查存在性（去掉 `attgenerated='s'` 那一支）⇒ 影子期这条红。
/// 注错：probe 恒报 `Ok(None)` ⇒ 影子期这条红（上面那条依旧绿）。
#[test]
fn an_unsatisfiable_requirement_is_reported_missing() {
    let Some((mut f, dsn)) = setup() else { return };
    const FACET: &str = "private.memory_records.facet";

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let pool = rt
        .block_on(RuntimeDbPool::connect(&dsn))
        .expect("gateway pool");
    let reported_facet = |pool: &RuntimeDbPool| -> Option<String> {
        rt.block_on(context_repo::probe_selectors(pool))
            .expect("probe")
            .iter()
            .find_map(|a| {
                a.missing_object
                    .as_deref()
                    .filter(|missing| missing.starts_with(FACET))
                    .map(str::to_owned)
            })
    };

    // 夹具库还没跑 0172 时 facet 本来就不满足——那就直接用它，不必再种一个。
    let generated = attgenerated(&mut f.admin, "private", "memory_records", "facet");
    let shadow = (generated.as_deref() == Some("s")).then(|| {
        // setup() 已经校验过这个 DSN 指向 127.0.0.1:61719 的隔离夹具库；DDL 只许打在那里。
        FacetShadow::arm(&std::env::var("HUMAUX_TEST_PG_DSN").expect("owner DSN"))
    });

    let reported = reported_facet(&pool);
    assert!(
        reported.is_some(),
        "{FACET} 不满足 REGISTRY 的依赖（catalog: {generated:?}，shadow: {}），\
         probe 必须把它报成缺失——一个只会报 None 的 probe 让 NA 变成静默可用",
        shadow.is_some()
    );
    if shadow.is_some() {
        assert_eq!(
            reported.as_deref(),
            Some("private.memory_records.facet (not stored-generated)"),
            "§25.4.A(11)：存在性不是 facet 的契约，一个可写的同名列必须被判成缺失，\
             而且缺失对象要说清是哪一条契约破了"
        );
    }

    drop(shadow);
    if generated.as_deref() == Some("s") {
        assert_eq!(
            reported_facet(&pool),
            None,
            "还原之后 probe 必须改口——否则上面那一报证明不了任何事（ADR-0006：\
             缺失对象是探测出来的，不是写死的）"
        );
    }
}

/// §25.4：ProjectConstraint 被机械规则选出来；authority 不够的同形 memory 选不出来。
///
/// 注错：把 `PROJECT_CONSTRAINTS_WHERE` 的 `authority_class = 'ProjectConstraint'` 去掉
/// ⇒ 第二条断言红（低 authority 的那条会被选进来）。
#[test]
fn project_constraints_are_selected_by_authority_not_similarity() {
    let Some((mut f, dsn)) = setup() else { return };

    let constraint = seed_memory(&mut f, "ProjectConstraint", "SNAPSHOT");
    // 内容逐字相同、只有 authority 不同——相似度选法会把两条都选进来，机械规则只选一条。
    let _note = seed_memory(&mut f, "PrivateKnowledge", "SNAPSHOT");

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let pool = rt
        .block_on(RuntimeDbPool::connect(&dsn))
        .expect("gateway pool");
    let frozen = rt
        .block_on(context_repo::fetch_frozen(
            &pool,
            &authorization_for(f.tenant_id),
            &scope_for(f.tenant_id),
        ))
        .expect("fetch frozen");

    let picked: Vec<Uuid> = frozen
        .mandatory
        .rows()
        .iter()
        .filter(|r| r.selector() == SelectorId::ProjectActiveConstraintsV1)
        .map(|r| r.memory_id().0)
        .collect();

    assert!(
        picked.contains(&constraint),
        "ProjectConstraint 必须被选进 Mandatory lane"
    );
    assert_eq!(
        picked.len(),
        1,
        "内容相同但 authority 不够的那条不得被选进来——选法是机械 authority 规则，\
         不是文本相似度: {picked:?}"
    );
}

/// 判据 (d)（DOD-093 / §11.10 注错四的落点）：LIVE 且未记版本的 ProjectConstraint
/// **不进** mandatory rows，且在 `needs_verification[]` **具名**（RECHECK_REQUIRED）。
///
/// 本文件第一次改造后跑红的正是这个行为——0100 的默认回填（LIVE+NULL）让所有夹具行
/// 被分流。那次红不是回归，是门在工作；本条把它钉成正式判据。
/// 注错：`from_selector` 恒喂 Judged(CURRENT)（「忽略 GroundingState」唯一可写出的形态）
/// ⇒ 行进了 rows、needs_verification 空 ⇒ 两条断言都红。
#[test]
fn a_live_unversioned_constraint_is_diverted_and_named_not_consumed() {
    let Some((mut f, dsn)) = setup() else { return };
    let diverted = seed_memory(&mut f, "ProjectConstraint", "LIVE");
    let admitted = seed_memory(&mut f, "ProjectConstraint", "SNAPSHOT");

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let pool = rt
        .block_on(RuntimeDbPool::connect(&dsn))
        .expect("gateway pool");
    let frozen = rt
        .block_on(context_repo::fetch_frozen(
            &pool,
            &authorization_for(f.tenant_id),
            &scope_for(f.tenant_id),
        ))
        .expect("fetch frozen");

    let row_ids: Vec<Uuid> = frozen
        .mandatory
        .rows()
        .iter()
        .map(|r| r.memory_id().0)
        .collect();
    assert!(
        row_ids.contains(&admitted),
        "SNAPSHOT 档（CURRENT）的 constraint 必须进 lane"
    );
    assert!(
        !row_ids.contains(&diverted),
        "LIVE+未记版本（RECHECK_REQUIRED）的 constraint 不得进 lane——静默消费即 DOD-093 违规"
    );
    assert!(
        frozen
            .mandatory
            .needs_verification()
            .iter()
            .any(|nv| nv.memory_id.0 == diverted),
        "被分流的行必须在 needs_verification[] 具名，不许消失: {:?}",
        frozen.mandatory.needs_verification()
    );
}

/// §25.5：仅由 binding 选中的行在撤销后消失，项目约束仍由独立 selector 保留。
///
/// 注错：把 `EXPLICIT_BINDINGS_WHERE` 的 `cb.revoked_at IS NULL` 去掉 ⇒ 本条红。
#[test]
fn revoking_a_binding_removes_it_from_the_lane() {
    let Some((mut f, dsn)) = setup() else { return };
    // card 22c (ADR-0046, §25.4.B(6)): this row used to be seeded at `ExplicitTaskContext` so
    // that ONLY the explicit-binding selector could deliver it. A stored 6 is now refused by
    // `memory_records_stored_authority_v2_check` (I-STORE), and `ProjectConstraint` — the only
    // remaining class at or above the explicit selector's floor — is unconditionally claimed by
    // `project_active_constraints_v1` as well. So the judgment is asserted where it is still
    // observable: on the PER-SELECTOR outcomes, before the lane merges and dedups them.
    let memory_id = seed_memory(&mut f, "ProjectConstraint", "SNAPSHOT");
    let constraint_id = seed_memory(&mut f, "ProjectConstraint", "SNAPSHOT");
    let low_authority_id = seed_memory(&mut f, "ProjectDecision", "SNAPSHOT");
    let binding_ids: Vec<Uuid> = [memory_id, constraint_id, low_authority_id]
        .into_iter()
        .map(|id| {
            f.admin
                .query_one(
                    "INSERT INTO private.context_bindings \
                       (tenant_id, memory_id, mode, scope_kind, created_by) \
                     VALUES ($1, $2, 'MANDATORY', 'TENANT', $1) RETURNING context_binding_id",
                    &[&f.tenant_id, &id],
                )
                .expect("insert binding")
                .get(0)
        })
        .collect();

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let pool = rt
        .block_on(RuntimeDbPool::connect(&dsn))
        .expect("gateway pool");

    let snapshot = || {
        rt.block_on(context_repo::fetch_frozen(
            &pool,
            &authorization_for(f.tenant_id),
            &scope_for(f.tenant_id),
        ))
        .expect("fetch frozen")
    };
    // Per-selector readback, un-merged: `selector_outcomes` is the same in-transaction path the
    // lane uses, one `SelectorOutcome` per selector.
    let selector_rows = |id: SelectorId| -> Vec<Uuid> {
        let outcomes = rt
            .block_on(context_repo::selector_outcomes(
                &pool,
                &authorization_for(f.tenant_id),
                &scope_for(f.tenant_id),
            ))
            .expect("per-selector readback");
        let mut ids: Vec<Uuid> = outcomes
            .iter()
            .filter_map(|outcome| match outcome {
                humaux_domain::context::SelectorOutcome::Ran {
                    id: outcome_id,
                    rows,
                    ..
                } if *outcome_id == id => Some(rows.iter().map(|row| row.memory_id().0)),
                _ => None,
            })
            .flatten()
            .collect();
        ids.sort();
        ids
    };

    let before = snapshot();
    assert_eq!(before.mandatory.expected(), 2);
    let mut both = vec![memory_id, constraint_id];
    both.sort();
    assert_eq!(
        selector_rows(SelectorId::ExplicitMandatoryBindingsV1),
        both,
        "两条达到下限的 MANDATORY 绑定必须被显式绑定 selector 选中；低 authority 那条不得进入"
    );
    assert!(
        !selector_rows(SelectorId::ExplicitMandatoryBindingsV1).contains(&low_authority_id),
        "低于下限的绑定目标不得被显式绑定 selector 选中"
    );
    for binding_id in &binding_ids[..2] {
        let revoked = rt
            .block_on(context_repo::revoke_binding(
                &pool,
                f.tenant_id,
                *binding_id,
            ))
            .expect("revoke");
        assert!(revoked, "第一次撤销必须真的改了一行");
    }
    let after = snapshot();
    // The bindings are gone, so the binding selector delivers nothing — that is the §25.5
    // positive control this test exists for. The same two rows are still delivered by the
    // INDEPENDENT project-constraints selector, which is the other half: revoking a binding
    // must not delete a row that qualifies on its own.
    assert_eq!(
        selector_rows(SelectorId::ExplicitMandatoryBindingsV1),
        Vec::<Uuid>::new(),
        "binding 撤销后显式绑定 selector 必须立即不再选中它们"
    );
    assert_eq!(
        selector_rows(SelectorId::ProjectActiveConstraintsV1),
        both,
        "仍满足项目约束的行不可被一起删掉"
    );
    assert_eq!(after.mandatory.expected(), 2);

    // 幂等：再撤一次不改行。
    let again = rt
        .block_on(context_repo::revoke_binding(
            &pool,
            f.tenant_id,
            binding_ids[0],
        ))
        .expect("revoke again");
    assert!(!again, "重复撤销不该再改行");
}

struct VisibilityCases {
    current_user: Uuid,
    other_user: Uuid,
    allowed_workspace: Uuid,
    other_workspace: Uuid,
    current_private: Uuid,
    other_private: Uuid,
    allowed_shared: Uuid,
    other_shared: Uuid,
    hidden_source: Uuid,
    revoked: Uuid,
    explicit_good: Uuid,
    explicit_wrong_scope: Uuid,
}

fn seed_visibility_cases(f: &mut Fixture) -> VisibilityCases {
    let current_user = seed_user(f);
    let other_user = seed_user(f);
    let allowed_workspace = seed_workspace(f, "allowed");
    let other_workspace = seed_workspace(f, "other");
    let current_private_evidence = seed_evidence(f, "USER_PRIVATE", Some(current_user), None);
    let current_private = seed_memory_with_visibility(
        f,
        "USER_PRIVATE",
        Some(current_user),
        None,
        current_private_evidence,
    );
    let other_private_evidence = seed_evidence(f, "USER_PRIVATE", Some(other_user), None);
    let other_private = seed_memory_with_visibility(
        f,
        "USER_PRIVATE",
        Some(other_user),
        None,
        other_private_evidence,
    );
    let allowed_shared_evidence =
        seed_evidence(f, "WORKSPACE_SHARED", None, Some(allowed_workspace));
    let allowed_shared = seed_memory_with_visibility(
        f,
        "WORKSPACE_SHARED",
        None,
        Some(allowed_workspace),
        allowed_shared_evidence,
    );
    let other_shared_evidence = seed_evidence(f, "WORKSPACE_SHARED", None, Some(other_workspace));
    let other_shared = seed_memory_with_visibility(
        f,
        "WORKSPACE_SHARED",
        None,
        Some(other_workspace),
        other_shared_evidence,
    );
    let hidden_source_evidence = seed_evidence(f, "USER_PRIVATE", Some(other_user), None);
    let hidden_source =
        seed_memory_with_visibility(f, "TENANT_SHARED", None, None, hidden_source_evidence);
    let revoked = seed_memory_with_visibility(f, "TENANT_SHARED", None, None, f.evidence_id);
    f.admin
        .execute(
            "UPDATE private.memory_records SET status = 'revoked' WHERE memory_id = $1",
            &[&revoked],
        )
        .expect("revoke memory");
    let explicit_good = seed_memory_with_visibility(f, "TENANT_SHARED", None, None, f.evidence_id);
    let explicit_wrong_scope =
        seed_memory_with_visibility(f, "TENANT_SHARED", None, None, f.evidence_id);
    // card 22c (ADR-0046, §25.4.B(6)): these two used to be UPDATEd to `ExplicitTaskContext` so
    // that only the explicit-binding selector could reach them. A stored 6 is now refused
    // outright (I-STORE), and there is no class above the explicit selector's floor that the
    // project selector does not also claim. They stay at the seeder's `ProjectConstraint`, and
    // the scope judgment below is asserted on the explicit selector's OWN outcome instead of on
    // the merged lane (where the project selector would be the representative selector).
    for (memory_id, mode, scope_kind, scope_id) in [
        (explicit_good, "MANDATORY", "USER", Some(current_user)),
        (
            explicit_wrong_scope,
            "MANDATORY",
            "WORKSPACE",
            Some(other_workspace),
        ),
        (current_private, "PINNED", "USER", Some(current_user)),
        (other_private, "PINNED", "USER", Some(other_user)),
    ] {
        f.admin
            .execute(
                "INSERT INTO private.context_bindings \
                   (tenant_id, memory_id, mode, scope_kind, scope_id, created_by) \
                 VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &f.tenant_id,
                    &memory_id,
                    &mode,
                    &scope_kind,
                    &scope_id,
                    &current_user,
                ],
            )
            .expect("insert scoped binding");
    }
    VisibilityCases {
        current_user,
        other_user,
        allowed_workspace,
        other_workspace,
        current_private,
        other_private,
        allowed_shared,
        other_shared,
        hidden_source,
        revoked,
        explicit_good,
        explicit_wrong_scope,
    }
}

/// Real-PG authorization counterexamples: the adapter must use the authenticated scope and
/// the real Memory/Evidence descriptors, while a binding is relevant only on `scope_chain`.
#[test]
// card 22c added the per-selector readback (the merged lane can no longer attribute a row to
// the explicit-binding selector, §25.4.B(6)). One fixture, one set of counterexamples.
#[allow(clippy::too_many_lines)]
fn context_lanes_filter_visibility_lifecycle_and_binding_scope() {
    let Some((mut f, dsn)) = setup() else { return };
    let cases = seed_visibility_cases(&mut f);
    let authorization =
        authorization_for_user(f.tenant_id, cases.current_user, [cases.allowed_workspace]);
    let scope = scoped(
        f.tenant_id,
        cases.current_user,
        Some(cases.allowed_workspace),
    );
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let pool = rt
        .block_on(RuntimeDbPool::connect(&dsn))
        .expect("gateway pool");
    let frozen = rt
        .block_on(context_repo::fetch_frozen(&pool, &authorization, &scope))
        .expect("authorized fetch");
    let mandatory: Vec<Uuid> = frozen
        .mandatory
        .rows()
        .iter()
        .map(|row| row.memory_id().0)
        .collect();
    assert!(mandatory.contains(&cases.current_private));
    assert!(mandatory.contains(&cases.allowed_shared));
    assert!(!mandatory.contains(&cases.other_shared));
    assert!(!mandatory.contains(&cases.hidden_source));
    assert!(!mandatory.contains(&cases.revoked));
    let outcomes = rt
        .block_on(context_repo::selector_outcomes(
            &pool,
            &authorization,
            &scope,
        ))
        .expect("per-selector readback");
    let explicit: Vec<Uuid> = outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            humaux_domain::context::SelectorOutcome::Ran { id, rows, .. }
                if *id == SelectorId::ExplicitMandatoryBindingsV1 =>
            {
                Some(rows.iter().map(|row| row.memory_id().0))
            }
            _ => None,
        })
        .flatten()
        .collect();
    assert!(explicit.contains(&cases.explicit_good));
    assert!(!explicit.contains(&cases.explicit_wrong_scope));
    assert!(
        frozen
            .pinned
            .rows()
            .iter()
            .any(|row| row.memory_id().0 == cases.current_private),
        "current user's USER_PRIVATE pin must remain visible"
    );
    assert!(
        !frozen
            .pinned
            .rows()
            .iter()
            .any(|row| row.memory_id().0 == cases.other_private),
        "same-tenant other user's private pin must not enter the pinned lane"
    );
    assert!(matches!(
        rt.block_on(context_repo::fetch_frozen(
            &pool,
            &authorization,
            &scoped(f.tenant_id, cases.current_user, Some(cases.other_workspace))
        )),
        Err(ErrorCode::Forbidden)
    ));
    assert!(matches!(
        rt.block_on(context_repo::fetch_frozen(
            &pool,
            &authorization,
            &scoped(f.tenant_id, cases.other_user, Some(cases.allowed_workspace))
        )),
        Err(ErrorCode::Forbidden)
    ));

    let other_tenant: Uuid = f
        .admin
        .query_one(
            "INSERT INTO control.tenants(name) VALUES('context authorization other tenant') \
             RETURNING tenant_id",
            &[],
        )
        .expect("insert other tenant")
        .get(0);
    let mut cross_tenant = scope.clone();
    cross_tenant.tenant_id = TenantId(other_tenant);
    assert!(matches!(
        rt.block_on(context_repo::fetch_frozen(
            &pool,
            &authorization,
            &cross_tenant
        )),
        Err(ErrorCode::Forbidden)
    ));
    f.admin
        .execute(
            "DELETE FROM control.tenants WHERE tenant_id = $1",
            &[&other_tenant],
        )
        .expect("cleanup other tenant");
}
