//! `adapters::tests::offset_pagination_positive_control` — §20.4 禁掉的「活集合上的跨事务 OFFSET 分页」——**可执行的坏变体**（DOD-012 的具名注错）。
//! Depends-on: crates=[humaux-testkit, postgres, serde_json, uuid]; services=[PostgreSQL(any) w=[control.private_reasoning_domains, control.tenants, private.events, private.evidence_objects, private.memory_evidence, private.memory_records]];
//!   env=[HUMAUX_TEST_PG_DSN]; modules=[humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [pins the forbidden cross-transaction OFFSET/LIMIT variant as an executable positive control that must
//!   go wrong when rows land between pages; a missing DB goes through skip_or_fail]
//! Spec: Baseline §20.4; §80.1; §79.2
//!
//! `consolidate_snapshot.rs` 与 `selection_snapshot.rs` 各自的模块注释都声称做过红转绿：
//! 「本地把实现换成朴素 OFFSET/LIMIT 跨事务分页，测试红了，换回来绿了，坏变体刻意不提交」。
//! 按 §80.1 与本仓自己的裁定，**只活在注释里的注错无人能复跑**——那段话是历史见证，
//! 不是判据。本文件把坏变体钉成可执行物：main 仍然全绿（断言的是坏变体**必然出错**），
//! 而「§20.4 禁的那个形态到底错在哪」从散文变成每次 CI 都跑的事实。
//!
//! 比原做法更强的一点：跨事务分页的每一页是独立的 autocommit 查询，所以本测试可以在
//! 两页**之间**确定性地插行——不需要并发、不需要竞态、不 flaky。原注释里的红转绿依赖
//! 60 个并发插入恰好落在分页中间；这里的插入就是顺序代码。
//!
//! 三态（§79.2）：由 `testkit::skip_or_fail` 统一判定（CI 声明 HUMAUX_REQUIRE_DB=1 时
//! 跳过即失败）。

use std::collections::BTreeSet;

use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use uuid::Uuid;

const NAME: &str = "offset_pagination_positive_control";
const PAGE: i64 = 40;

struct Fixture {
    admin: Client,
    tenant_id: Uuid,
    evidence_id: Uuid,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM private.memory_evidence WHERE memory_id IN \
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
    // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
    let Ok(mut admin) = Client::connect(&dsn, NoTls) else {
        skip_or_fail(NAME, "missing object: live Postgres", ExternalDep::Postgres);
        return None;
    };
    let tenant_id: Uuid = admin
        .query_one(
            "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
            &[&"offset_pagination_positive_control throwaway tenant"],
        )
        .ok()?
        .get(0);
    let reasoning_domain_id: Uuid = admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
             VALUES ($1, 'offset_pagination domain') RETURNING reasoning_domain_id",
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
    })
}

/// 播 `n` 条 memory，返回它们的 id。uuidv7 单调递增 ⇒ 后插的行在
/// `ORDER BY memory_id DESC` 下排最前——这正是坏变体窗口被推移的机理。
fn seed_batch(f: &mut Fixture, n: usize) -> Vec<Uuid> {
    let confidence: f32 = 0.9;
    let mut out = Vec::with_capacity(n);
    let mut txn = f.admin.transaction().expect("begin");
    for _ in 0..n {
        let id: Uuid = txn
            .query_one(
                "INSERT INTO private.memory_records \
                   (tenant_id, memory_type, content, visibility_class, \
                    authority_class, confidence, status, asserted_at) \
                 VALUES ($1, 'NOTE', $2, 'TENANT_SHARED', 'PrivateKnowledge', $3, 'active', now()) \
                 RETURNING memory_id",
                &[
                    &f.tenant_id,
                    &serde_json::json!({"fixture": NAME}),
                    &confidence,
                ],
            )
            .expect("insert memory")
            .get(0);
        // dep: PostgreSQL(any) — pool/txn query execution
        txn.execute(
            "INSERT INTO private.memory_evidence (memory_id, evidence_id, role) \
             VALUES ($1, $2, 'PRIMARY')",
            &[&id, &f.evidence_id],
        )
        .expect("link evidence (§8.6)");
        out.push(id);
    }
    txn.commit().expect("commit seed");
    out
}

/// **坏变体本体**：§20.4 禁的形态，逐字——每页一个独立 autocommit 查询、
/// `ORDER BY memory_id DESC LIMIT … OFFSET …`、页数按**开始前**的 COUNT 预算、
/// 无共享 snapshot、无 manifest。
fn paginate_via_cross_txn_offset(
    f: &mut Fixture,
    mutate_between_pages: &mut dyn FnMut(&mut Fixture),
) -> Vec<Uuid> {
    let total: i64 = f
        .admin
        .query_one(
            "SELECT count(*) FROM private.memory_records WHERE tenant_id = $1",
            &[&f.tenant_id],
        )
        .expect("count")
        .get(0);
    let pages = (total + PAGE - 1) / PAGE;

    let mut seen = Vec::new();
    for page in 0..pages {
        let rows = f
            .admin
            .query(
                "SELECT memory_id FROM private.memory_records WHERE tenant_id = $1 \
                 ORDER BY memory_id DESC LIMIT $2 OFFSET $3",
                &[&f.tenant_id, &PAGE, &(page * PAGE)],
            )
            .expect("page query");
        seen.extend(rows.iter().map(|r| r.get::<_, Uuid>(0)));
        // 页与页之间世界继续动——跨事务分页对此毫无防御，这正是被禁的原因。
        mutate_between_pages(f);
    }
    seen
}

/// 正控制：坏变体在「页间有写入」的世界里**必然**同时产生重复与遗漏。
///
/// 这就是 DOD-012「snapshot-bound selection 无漏/重」的具名注错：真实现
/// （`select_and_materialize_inputs` 的单事务 snapshot / `selection_repo` 的 manifest 分页）
/// 对同样的写入模式免疫——那两条既有测试各自断言。本条则钉住反面：**放弃 snapshot 的
/// 那个形态确实会漏会重**，两边合起来才是完整判据；只有正面那半时，「snapshot 有没有
/// 在起作用」无从区分（也许世界根本没动）。
#[test]
fn banned_cross_txn_offset_pagination_exhibits_both_duplicates_and_misses() {
    let Some(mut f) = setup() else { return };

    // 100 条原始行；页间插 60 条更高排序的行（uuidv7 更大 ⇒ DESC 排最前 ⇒ 整个窗口后移）。
    let original: BTreeSet<Uuid> = seed_batch(&mut f, 100).into_iter().collect();
    let mut inserted_between = false;
    let seen = paginate_via_cross_txn_offset(&mut f, &mut |fx| {
        if !inserted_between {
            inserted_between = true;
            let _ = seed_batch(fx, 60);
        }
    });

    // 重复：同一行出现在多页（窗口后移把已读过的行又推回后一页的 OFFSET 区间）。
    let mut uniq = BTreeSet::new();
    let duplicates = seen.iter().filter(|id| !uniq.insert(**id)).count();

    // 遗漏：原始行被推出预算好的页数之外。
    let seen_set: BTreeSet<Uuid> = seen.iter().copied().collect();
    let missed = original.difference(&seen_set).count();

    assert!(
        duplicates > 0,
        "坏变体必须产生重复——没有重复说明本正控制没有制造出窗口推移，判据失去区分能力"
    );
    assert!(
        missed > 0,
        "坏变体必须产生遗漏——页数按开始前的 COUNT 预算，60 条新行必须把尾部原始行推出窗口"
    );
    eprintln!("{NAME}: duplicates={duplicates} missed={missed}（坏变体如预期出错）");
}
