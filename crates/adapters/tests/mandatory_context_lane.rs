//! §25.4/§25.5 Mandatory Context Lane 的 DB 判据（DOD-020，phase=7 欠账）。
//!
//! 本文件打三件事：
//! 1. **probe 的缺失对象是探测出来的**——`task_explicit_context_v1` /
//!    `required_current_state_facets_v1` 今天报的缺失列（`task_id` / `facet`）必须与
//!    `information_schema` 的实况一致，而不是写死的判断（ADR-0006）。
//! 2. **selector 是机械规则，不是相似度**——种一条 ProjectConstraint，它必须被选出来；
//!    种一条同样文本但 authority 不够的，必须选不出来。
//! 3. **binding 的撤销立即生效**——§25.5 的正对照，最容易漏的那条。
//!
//! 三态（§79.2）由 `testkit::skip_or_fail` 统一判定；CI 声明 `HUMAUX_REQUIRE_DB=1` 时
//! 跳过即失败（ADR-0005）。

use humaux_adapters::context_repo;
use humaux_adapters::postgres::RuntimeDbPool;
use humaux_domain::context::SelectorId;
use humaux_domain::ids::{Scope, TenantId};
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use uuid::Uuid;

const NAME: &str = "mandatory_context_lane";

fn dsn_as_role(dsn: &str, role: &str) -> String {
    // 与 tests/serving_repo.rs 同形：把 DSN 的用户名换成指定角色。
    let Some(rest) = dsn.strip_prefix("postgres://") else {
        return dsn.to_string();
    };
    let Some(at) = rest.find('@') else {
        return dsn.to_string();
    };
    format!("postgres://{role}:devlocal_{role}@{}", &rest[at + 1..])
}

struct Fixture {
    admin: Client,
    tenant_id: Uuid,
    evidence_id: Uuid,
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
             DELETE FROM control.tenants WHERE tenant_id = '{0}';",
            self.tenant_id
        ));
    }
}

/// 建 fixture。返回 `None` 时已经打印过 SKIP（或在声明模式下 panic 过）。
fn setup() -> Option<(Fixture, String)> {
    let Ok(dsn) = std::env::var("HUMAUX_TEST_PG_DSN") else {
        skip_or_fail(NAME, "missing object: Postgres DSN", ExternalDep::Postgres);
        return None;
    };
    let Ok(mut admin) = Client::connect(&dsn, NoTls) else {
        skip_or_fail(NAME, "missing object: live Postgres", ExternalDep::Postgres);
        return None;
    };
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
        },
        dsn,
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

/// probe 报的缺失对象必须与 `information_schema` 的实况一致。
///
/// 写死一个「task_id 不存在」的判断今天也会绿——所以本条**先自己去查一遍**再比对。
/// 列一落地，probe 必须自动改口，不需要有人回来改代码（ADR-0006）。
#[test]
fn probe_reports_the_columns_that_are_actually_missing() {
    let Some((mut f, dsn)) = setup() else { return };

    let mut truth = Vec::new();
    for (table, column) in [("memory_records", "task_id"), ("memory_records", "facet")] {
        let exists: bool = f
            .admin
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
                 WHERE table_schema='private' AND table_name=$1 AND column_name=$2)",
                &[&table, &column],
            )
            .expect("probe truth")
            .get(0);
        truth.push((format!("private.{table}.{column}"), exists));
    }

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let pool = rt
        .block_on(RuntimeDbPool::connect(&dsn_as_role(&dsn, "role_gateway")))
        .expect("gateway pool");
    let availability = rt
        .block_on(context_repo::probe_selectors(&pool))
        .expect("probe");

    for (name, exists) in truth {
        let reported = availability
            .iter()
            .any(|a| a.missing_object.as_deref() == Some(name.as_str()));
        assert_eq!(
            reported, !exists,
            "probe 对 {name} 的判断与 information_schema 不一致：\
             列存在={exists} 而 probe 报缺失={reported}"
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
        .block_on(RuntimeDbPool::connect(&dsn_as_role(&dsn, "role_gateway")))
        .expect("gateway pool");
    let frozen = rt
        .block_on(context_repo::fetch_frozen(&pool, &scope_for(f.tenant_id)))
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
        .block_on(RuntimeDbPool::connect(&dsn_as_role(&dsn, "role_gateway")))
        .expect("gateway pool");
    let frozen = rt
        .block_on(context_repo::fetch_frozen(&pool, &scope_for(f.tenant_id)))
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

/// §25.5 正对照：binding 撤销后必须**立刻**从 lane 里消失。最容易漏的一条。
///
/// 注错：把 `EXPLICIT_BINDINGS_WHERE` 的 `cb.revoked_at IS NULL` 去掉 ⇒ 本条红。
#[test]
fn revoking_a_binding_removes_it_from_the_lane() {
    let Some((mut f, dsn)) = setup() else { return };
    let memory_id = seed_memory(&mut f, "ProjectConstraint", "SNAPSHOT");

    let binding_id: Uuid = f
        .admin
        .query_one(
            "INSERT INTO private.context_bindings \
               (tenant_id, memory_id, mode, scope_kind, created_by) \
             VALUES ($1, $2, 'MANDATORY', 'TENANT', $1) RETURNING context_binding_id",
            &[&f.tenant_id, &memory_id],
        )
        .expect("insert binding")
        .get(0);

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let pool = rt
        .block_on(RuntimeDbPool::connect(&dsn_as_role(&dsn, "role_gateway")))
        .expect("gateway pool");

    let bound = |pool: &RuntimeDbPool| -> bool {
        let frozen = rt
            .block_on(context_repo::fetch_frozen(pool, &scope_for(f.tenant_id)))
            .expect("fetch frozen");
        frozen.mandatory.rows().iter().any(|r| {
            r.selector() == SelectorId::ExplicitMandatoryBindingsV1 && r.memory_id().0 == memory_id
        })
    };

    assert!(bound(&pool), "未撤销的 MANDATORY binding 必须在 lane 里");

    let revoked = rt
        .block_on(context_repo::revoke_binding(&pool, f.tenant_id, binding_id))
        .expect("revoke");
    assert!(revoked, "第一次撤销必须真的改了一行");

    assert!(
        !bound(&pool),
        "已撤销的 binding 必须立刻从 lane 里消失（§25.5 正对照）"
    );

    // 幂等：再撤一次不改行。
    let again = rt
        .block_on(context_repo::revoke_binding(&pool, f.tenant_id, binding_id))
        .expect("revoke again");
    assert!(!again, "重复撤销不该再改行");
}
