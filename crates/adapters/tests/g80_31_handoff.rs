//! G80-31（§57.1 Phase 8 出场判据）端到端：**同一快照两次装配 handoff 逐字节相同**，
//! 且 Mandatory 的选取是机械规则、撤销立即生效、pinned 的「钉 3 带 2」可观测。
//!
//! 快照身份语义（`FrozenReads` doc）：两次装配各开各的 REPEATABLE READ 事务——静默
//! fixture 库（本 harness 独占本租户的写权）里两个事务看到同一世界。**前置显式断言
//! snapshot token 相等**（不是假设）：token 不等说明有并发写者，此时字节不同不是回归，
//! 是前置没满足——判据与环境噪声分开。
//!
//! 三态照 `testkit::skip_or_fail`（CI 声明 HUMAUX_REQUIRE_DB=1 时跳过即失败）。
//! NA 的唯一主语是 probe 探测的具名缺失对象；字节不同 / mandatory 被淘汰 /
//! needs_verification 空 **永远是红**，与 NA 零重合（ADR-0006）。

use humaux_adapters::context_repo::ContextReadAdapter;
use humaux_adapters::postgres::RuntimeDbPool;
use humaux_application::continuity::assemble_handoff;
use humaux_domain::context::ContextBudget;
use humaux_domain::ids::{Scope, TenantId};
use humaux_retrieval::handoff::Handoff;
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use uuid::Uuid;

const NAME: &str = "g80_31_handoff";

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    let Some(rest) = admin_dsn.strip_prefix("postgres://") else {
        return admin_dsn.to_string();
    };
    let Some(at) = rest.find('@') else {
        return admin_dsn.to_string();
    };
    format!("postgres://{role}:devlocal_{role}@{}", &rest[at + 1..])
}

struct Fixture {
    admin: Client,
    tenant_id: Uuid,
    evidence_id: Uuid,
    dsn: String,
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

fn setup() -> Option<Fixture> {
    let Ok(dsn) = std::env::var("HUMAUX_TEST_PG_DSN") else {
        skip_or_fail(NAME, "missing object: Postgres DSN", ExternalDep::Postgres);
        return None;
    };
    let Ok(mut admin) = Client::connect(&dsn, NoTls) else {
        skip_or_fail(NAME, "missing object: live Postgres", ExternalDep::Postgres);
        return None;
    };
    let tenant_id: Uuid = admin
        .query_one(
            "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
            &[&"g80_31_handoff throwaway tenant"],
        )
        .ok()?
        .get(0);
    let reasoning_domain_id: Uuid = admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
             VALUES ($1, 'g80_31 domain') RETURNING reasoning_domain_id",
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
    Some(Fixture {
        admin,
        tenant_id,
        evidence_id,
        dsn,
    })
}

/// 批量播 memory（SNAPSHOT 档 evidence，进得了 lane）。返回 id。
fn seed_many(f: &mut Fixture, authority: &str, n: usize) -> Vec<Uuid> {
    let confidence: f32 = 0.9;
    let mut out = Vec::with_capacity(n);
    let mut txn = f.admin.transaction().expect("begin");
    for _ in 0..n {
        let id: Uuid = txn
            .query_one(
                "INSERT INTO private.memory_records \
                   (tenant_id, memory_type, content, visibility_class, \
                    authority_class, confidence, status, asserted_at) \
                 VALUES ($1, 'NOTE', $2, 'TENANT_SHARED', $3, $4, 'active', now()) \
                 RETURNING memory_id",
                &[
                    &f.tenant_id,
                    &serde_json::json!({"fixture": NAME}),
                    &authority,
                    &confidence,
                ],
            )
            .expect("insert memory")
            .get(0);
        txn.execute(
            "INSERT INTO private.memory_evidence (memory_id, evidence_id, role, grounding_mode) \
             VALUES ($1, $2, 'PRIMARY', 'SNAPSHOT')",
            &[&id, &f.evidence_id],
        )
        .expect("link evidence");
        out.push(id);
    }
    txn.commit().expect("commit");
    out
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

fn budget() -> ContextBudget {
    // 200 条噪声不进 mandatory（authority 不够），预算给足 constraint。
    ContextBudget::new(1_000_000, 500_000).expect("budget")
}

/// 每次装配自己连一个 pool：typed pool 刻意无 `Clone`（§6.2.3 闭集），测试迁就它
/// 而不是撬开它——两次装配本来就该是两个独立事务。
async fn one_handoff(dsn: &str, tenant: Uuid) -> Handoff {
    let pool = RuntimeDbPool::connect(&dsn_as_role(dsn, "role_gateway"))
        .await
        .expect("pool");
    let adapter = ContextReadAdapter::new(pool);
    assemble_handoff(&adapter, &scope_for(tenant), budget())
        .await
        .expect("assemble handoff")
}

/// 判据 (a)+(b)：1 条 ProjectConstraint + 200 条噪声，两次独立装配（两个事务）——
/// token 相等（前置）∧ 逐字节相同 ∧ constraint 在 mandatory ∧ 噪声不进。
#[test]
fn same_snapshot_assembles_byte_identical_handoffs_with_the_constraint_present() {
    let Some(mut f) = setup() else { return };
    let constraint = seed_many(&mut f, "ProjectConstraint", 1)[0];
    let noise = seed_many(&mut f, "PrivateKnowledge", 200);

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let (h1, h2) = rt.block_on(async {
        let h1 = one_handoff(&f.dsn, f.tenant_id).await;
        let h2 = one_handoff(&f.dsn, f.tenant_id).await;
        (h1, h2)
    });

    // 前置：同一世界。token 不等 ⇒ 有并发写者 ⇒ 前置没满足（显式可观测，不是假设）。
    assert_eq!(
        h1.snapshot_token_sha256, h2.snapshot_token_sha256,
        "前置不满足：两次装配读到了不同快照（fixture 库应当静默）"
    );

    // §57.1 Phase 8 前半：逐字节相同。
    assert_eq!(
        h1.canonical_bytes(),
        h2.canonical_bytes(),
        "同一快照两次装配必须逐字节相同（G80-31）"
    );
    assert_eq!(h1.sha256(), h2.sha256());

    // 后半（G25-1 的 e2e 面）：constraint 在，200 噪声一条都不进。
    let ids: Vec<String> = h1.mandatory.iter().map(|i| i.memory_id.clone()).collect();
    assert!(
        ids.contains(&constraint.to_string()),
        "ProjectConstraint 必须在 mandatory 里"
    );
    for n in &noise {
        assert!(
            !ids.contains(&n.to_string()),
            "PrivateKnowledge 噪声不得进 mandatory（选取是机械 authority 规则）"
        );
    }
    assert_eq!(h1.counts.mandatory_returned, 1);
    assert!(!h1.counts.overflow);
}

/// 判据 (c)：supersede 之后重装配必须消失——「不可淘汰」的反向对照，防「永不丢」过拟合。
#[test]
fn a_superseded_constraint_disappears_on_reassembly() {
    let Some(mut f) = setup() else { return };
    let constraint = seed_many(&mut f, "ProjectConstraint", 1)[0];

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let before = rt.block_on(one_handoff(&f.dsn, f.tenant_id));
    // supersede：同步 Client 在 block_on **之外**（runtime 套 runtime 的教训），
    // 也不需要撬 typed pool 的私有 `pool()`。
    f.admin
        .execute(
            // G59-4 CHECK：superseded_by 非 NULL ⇔ status='superseded'，两列一起改
            // （第一次只改一列被 CHECK 顶回来——闸在工作）。
            "UPDATE private.memory_records \
             SET superseded_by = memory_id, status = 'superseded' WHERE memory_id = $1",
            &[&constraint],
        )
        .expect("supersede");
    let after = rt.block_on(one_handoff(&f.dsn, f.tenant_id));

    assert!(
        before
            .mandatory
            .iter()
            .any(|i| i.memory_id == constraint.to_string()),
        "supersede 前必须在"
    );
    assert!(
        !after
            .mandatory
            .iter()
            .any(|i| i.memory_id == constraint.to_string()),
        "supersede 后必须消失——否则「不可淘汰」是过拟合出来的「永不丢」"
    );
    assert_ne!(
        before.canonical_bytes(),
        after.canonical_bytes(),
        "世界变了，字节必须变"
    );
}

/// 判据 (e)：钉 3 条、1 条低 authority——pinned.expected==3、returned==2、excluded 具名。
/// **oracle 是测试播种的外部真值**，不是内部守恒式（守恒式在 expected 内生时恒真）。
#[test]
fn pinned_three_with_one_low_authority_reports_expected_three_returned_two() {
    let Some(mut f) = setup() else { return };
    let high = seed_many(&mut f, "ProjectConstraint", 2);
    let low = seed_many(&mut f, "PrivateKnowledge", 1)[0];

    for id in high.iter().chain(std::iter::once(&low)) {
        f.admin
            .execute(
                "INSERT INTO private.context_bindings \
                   (tenant_id, memory_id, mode, scope_kind, created_by) \
                 VALUES ($1, $2, 'PINNED', 'TENANT', $1)",
                &[&f.tenant_id, id],
            )
            .expect("pin");
    }

    let rt = tokio::runtime::Runtime::new().expect("rt");
    let h = rt.block_on(one_handoff(&f.dsn, f.tenant_id));

    assert_eq!(h.counts.pinned_expected, 3, "外部真值：钉了 3 条");
    assert_eq!(h.counts.pinned_returned, 2, "低 authority 那条不进 lane");
    assert_eq!(
        h.counts.pinned_excluded, 1,
        "被排除的必须计数——「钉 3 带 2」不可观测就是静默截断"
    );
}
