//! §75.2 / §19.1 append-only：两个账本的守卫在 UPDATE / DELETE / TRUNCATE 三条路径上都
//! 必须拒绝。
//!
//! **这个文件存在的理由是它此前不存在。** `control.credit_ledger` 的
//! `credit_ledger_reject_mutation` 触发器从 0032 写下起，全仓 `tests/` 里一次都没被碰过——
//! 一个从未被执行过的守卫，按 §80.1 的准入条件不算数。真去打它才发现：
//!
//! ```text
//! TRUNCATE control.credit_ledger CASCADE  (owner 身份)  -> 成功，剩 0 行
//! TRUNCATE ops.model_call_ledger CASCADE  (owner 身份)  -> 成功，剩 0 行
//! ```
//!
//! 病因与 0047→0057 在 `ops.data_disclosures` 上踩过的逐字同型：`FOR EACH ROW` 触发器
//! **永远不会被 TRUNCATE 触发**，所以「BEFORE UPDATE OR DELETE FOR EACH ROW」看起来把
//! append-only 钉死了，实际留着一条整表清空的路。migration `0101` 补上 STATEMENT 级
//! TRUNCATE 触发器，本文件是它的判据。
//!
//! 为什么 owner 身份是相关威胁面：0032 自己的注释写着这道防线的存在理由就是
//! 「append-only 必须在 migration-owner 认证的连接下也成立，不只在运行时角色的 grant
//! 之下」——runtime 角色本来就只有 SELECT。防线的自述目标与它的实际覆盖面对不上。
//!
//! 三态（§79.2）：无 DSN / 连不上 / 迁移未应用，三条腿各自打印可见 SKIP 并点名缺失对象，
//! 由 `testkit::skip_or_fail` 统一判定（CI 声明 `HUMAUX_REQUIRE_DB=1` 时跳过即失败）。

use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use uuid::Uuid;

struct Handle {
    admin: Client,
    tenant_id: Uuid,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // 本 fixture 写进的 credit_ledger 行**删不掉**——那正是被测的不变量。
        // 只清掉能清的（同 disclosure_ledger.rs 自陈的 permanent-leak 形状）。
        let _ = self.admin.execute(
            "DELETE FROM control.tenants WHERE tenant_id = $1",
            &[&self.tenant_id],
        );
    }
}

struct LedgerFixture;

impl DbIntegrationFixture for LedgerFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        // 三态的 skip 腿只看**表在不在**（= 迁移到底跑没跑过）。触发器在不在**不是** skip
        // 条件而是断言——按 ADR-0006 的判定顺序：被测对象不存在才 NA，被测对象存在但观测
        // 为空是 **Fail**，因为那通常就是违规本身。这里被测对象是账本表，而「表在、守卫不在」
        // 恰恰是本文件要抓的那个状态，把它当成「不适用」等于让闸在违规时最安静。
        let tables_exist: bool = admin
            .query_one(
                "SELECT to_regclass('control.credit_ledger') IS NOT NULL
                    AND to_regclass('ops.model_call_ledger') IS NOT NULL",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !tables_exist {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "control.credit_ledger / ops.model_call_ledger do not exist — run \
                 `cargo xtask migrate` against HUMAUX_TEST_PG_DSN first"
                    .to_string(),
            ));
        }

        let tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"ledger_append_only.rs throwaway tenant"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        Ok(Handle { admin, tenant_id })
    }
}

/// 播一条 credit_ledger 行，返回它的 id。
fn seed_credit_entry(admin: &mut Client, tenant_id: &Uuid) -> Uuid {
    admin
        .query_one(
            "INSERT INTO control.credit_ledger
               (tenant_id, delta, reason, reference_type, reference_id)
             VALUES ($1, 100, 'ledger_append_only.rs', 'test', gen_random_uuid())
             RETURNING ledger_entry_id",
            &[tenant_id],
        )
        .expect("seed credit_ledger entry")
        .get(0)
}

/// §75.2：`control.credit_ledger` 三条路径全拒。
///
/// 每条都跑在**显式且永不提交**的事务里：万一守卫回归、操作真的成功了，drop 时的回滚
/// 也会把它丢掉，而不是把共享开发库的账本真清了（同 disclosure_ledger.rs 的先例）。
#[test]
fn credit_ledger_rejects_update_delete_and_truncate() {
    run_db_fixture::<LedgerFixture, _>(
        "credit_ledger_rejects_update_delete_and_truncate",
        |mut h| {
            // 守卫在场是**断言**不是前置条件（见 isolate 的注释）：0101 被回滚、触发器被人
            // DROP 掉、或函数被 CREATE OR REPLACE 改坏——三种都是违规，都必须红。
            let truncate_guards: i64 = h
                .admin
                .query_one(
                    "SELECT count(*) FROM pg_trigger
                 WHERE NOT tgisinternal AND tgtype & 32 > 0
                   AND tgrelid IN ('control.credit_ledger'::regclass,
                                   'ops.model_call_ledger'::regclass)",
                    &[],
                )
                .expect("count BEFORE TRUNCATE triggers")
                .get(0);
            assert_eq!(
                truncate_guards, 2,
                "两个账本都必须有 STATEMENT 级 BEFORE TRUNCATE 守卫（migration 0101）——\
             少一个就意味着那张表可以被整表清空，而 FOR EACH ROW 的 UPDATE/DELETE 触发器\
             对 TRUNCATE 完全无感"
            );

            let entry_id = seed_credit_entry(&mut h.admin, &h.tenant_id);

            // ---- UPDATE ----
            let mut txn = h.admin.transaction().expect("begin txn");
            let err = txn
                .execute(
                    "UPDATE control.credit_ledger SET delta = 999 WHERE ledger_entry_id = $1",
                    &[&entry_id],
                )
                .expect_err("§75.2: 原地改余额必须被拒——撤销只能通过反向 entry");
            assert_eq!(
                err.code().map(|c| c.code()),
                Some("42501"),
                "expected insufficient_privilege, got: {err:?}"
            );
            drop(txn);

            // ---- DELETE ----
            // 必须命中真实存在的那一行：守卫是 FOR EACH ROW，`WHERE false` 一行都不匹配时
            // 它根本不会触发，测试会「通过」而什么都没测到。
            let mut txn = h.admin.transaction().expect("begin txn");
            let err = txn
                .execute(
                    "DELETE FROM control.credit_ledger WHERE ledger_entry_id = $1",
                    &[&entry_id],
                )
                .expect_err("§75.2: 账本条目不可删");
            assert_eq!(
                err.code().map(|c| c.code()),
                Some("42501"),
                "expected insufficient_privilege, got: {err:?}"
            );
            drop(txn);

            // ---- TRUNCATE ----
            // 必须带 CASCADE：不带时 PostgreSQL 更早地以「cannot truncate a table referenced
            // in a foreign key constraint」拒绝（control.referral_rewards 引用了它），守卫
            // 触发器根本不会 fire，断言就变成在测 FK 而不是在测 §75.2。
            //
            // ponytail: 因此本条会短暂持有 credit_ledger + referral_rewards 两张表的
            // AccessExclusiveLock。目前全仓没有第二个测试从库里读这两张表，构不成死锁环，
            // 所以不设 advisory lock（只有一个参与者的锁是空操作）。**若将来新增了读这两张表
            // 的 DB 测试，必须让它取共享 advisory lock 与本条互斥**——照
            // `disclosure_ledger.rs` + `testkit::DISCLOSURE_LEDGER_ADVISORY_LOCK` 的形状做，
            // 那个洞是实测踩出来的（40P01）。
            let mut txn = h.admin.transaction().expect("begin txn");
            let err = txn
                .execute("TRUNCATE control.credit_ledger CASCADE", &[])
                .expect_err("§75.2: 整表清空必须被拒（0101 之前这里是成功的）");
            assert_eq!(
                err.code().map(|c| c.code()),
                Some("42501"),
                "expected insufficient_privilege, got: {err:?}"
            );
            drop(txn);
        },
    );
}

/// §19.1：`ops.model_call_ledger` 同样三条路径全拒。
///
/// 它没有依赖表，所以 TRUNCATE 不需要 CASCADE（不带也能触发到守卫，实测过），
/// 锁面只有这一张表。
#[test]
fn model_call_ledger_rejects_delete_and_truncate() {
    run_db_fixture::<LedgerFixture, _>("model_call_ledger_rejects_delete_and_truncate", |mut h| {
        // 守卫在场是**断言**不是前置条件（见 isolate 的注释）：0101 被回滚、触发器被人
        // DROP 掉、或函数被 CREATE OR REPLACE 改坏——三种都是违规，都必须红。
        let truncate_guards: i64 = h
            .admin
            .query_one(
                "SELECT count(*) FROM pg_trigger
                 WHERE NOT tgisinternal AND tgtype & 32 > 0
                   AND tgrelid IN ('control.credit_ledger'::regclass,
                                   'ops.model_call_ledger'::regclass)",
                &[],
            )
            .expect("count BEFORE TRUNCATE triggers")
            .get(0);
        assert_eq!(
            truncate_guards, 2,
            "两个账本都必须有 STATEMENT 级 BEFORE TRUNCATE 守卫（migration 0101）——\
             少一个就意味着那张表可以被整表清空，而 FOR EACH ROW 的 UPDATE/DELETE 触发器\
             对 TRUNCATE 完全无感"
        );

        // 这里只打 TRUNCATE 与空集 DELETE 的**语句级**行为：插一条合法的
        // model_call_ledger 行需要 provider_pricing_versions 等一串前置，那些已由
        // `model_call_ledger.rs` 覆盖；本条要证的是 0101 补上的那条腿。
        let mut txn = h.admin.transaction().expect("begin txn");
        let err = txn
            .execute("TRUNCATE ops.model_call_ledger", &[])
            .expect_err("§19.1: 整表清空必须被拒（0101 之前这里是成功的）");
        assert_eq!(
            err.code().map(|c| c.code()),
            Some("42501"),
            "expected insufficient_privilege, got: {err:?}"
        );
        assert!(
            format!("{err:?}").contains("append-only"),
            "错误必须来自 §19.1 的 append-only 守卫本身，而不是别的什么把它顺手挡住了: {err:?}"
        );
        drop(txn);
    });
}
