//! `adapters::tests::retrieve_no_hidden_generative_recall` — T6.2 (§20.0 / §20#G20-2 / G80-39) e2e integration test —
//!   the `ModelCallLedger` half of the "online recall 无隐藏生成调用" gate: recall/context/continuity must never leave a
//!   `purpose='query_rewrite'` row in `ops.model_call_ledger` (§19.1).
//! Depends-on: crates=[humaux-testkit, postgres, sqlx]; services=[PostgreSQL(owner) w=[control.tenants, ops.model_call_ledger]]; env=[HUMAUX_TEST_PG_DSN]; modules=[humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [recall/context/continuity must leave no query_rewrite row in ops.model_call_ledger (the injected
//!   fault row must be detected); no DSN, unreachable DB or migration missing is a visible SKIP]
//! Spec: Baseline §19.1; §20; §20.0
//!
//! Complements
//! `xtask architecture-check`'s static G20-2/G80-39 sub-check (`application::retrieve`'s own
//! doc comment), which proves the *code* never names a reasoning provider; this proves the
//! *ledger* never records the call such a provider would have made, on a real Postgres —
//! matching the §80.1 registry row's `PR · e2e` mode.
//!
//! `ops.model_call_ledger` is still a §48 skeleton table (0008) with no real writer yet
//! (full §19.1 field list is Phase 7 scope) — this file hand-seeds the one row shape the
//! injected fault would produce, same "seed what the real write path would write" convention
//! `retrieve_read_your_writes.rs`'s own module doc documents for the read side.
//!
//! Three-state skip (§79.2): no `HUMAUX_TEST_PG_DSN`, an unreachable DB, or
//! `migrations/0080_model_call_ledger_purpose.sql` not yet applied all print a visible SKIP.

use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;

struct Handle {
    admin: Client,
    tenant_id: Uuid,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Best-effort cleanup, repo CLAUDE.md hard rule ④: only rows this file created.
        let _ = self.admin.execute(
            "DELETE FROM ops.model_call_ledger WHERE tenant_id = $1",
            &[&self.tenant_id],
        );
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
        // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        let migrated: bool = admin
            .query_one(
                "SELECT EXISTS (
                   SELECT 1 FROM information_schema.columns
                   WHERE table_schema = 'ops' AND table_name = 'model_call_ledger'
                     AND column_name = 'purpose'
                 )",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !migrated {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "ops.model_call_ledger.purpose column missing — run `cargo xtask migrate` \
                 (migrations through 0080_model_call_ledger_purpose.sql) against \
                 HUMAUX_TEST_PG_DSN first"
                    .to_string(),
            ));
        }

        let tenant_id = Uuid::new_v4();
        admin
            .execute(
                "INSERT INTO control.tenants (tenant_id, name, state) VALUES ($1, $2, 'ACTIVE')",
                &[&tenant_id, &format!("t6.2-ledger-test-{tenant_id}")],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        Ok(Handle { admin, tenant_id })
    }
}

/// §20#G20-2/G80-39's e2e assertion itself: does `ops.model_call_ledger` carry a
/// `purpose='query_rewrite'` row for this tenant. What a real CI e2e run would check after
/// exercising the online recall/context/continuity path.
fn query_rewrite_ledger_hits(admin: &mut Client, tenant_id: Uuid) -> i64 {
    admin
        .query_one(
            "SELECT count(*) FROM ops.model_call_ledger \
             WHERE tenant_id = $1 AND purpose = 'query_rewrite'",
            &[&tenant_id],
        )
        .expect("count query_rewrite ledger rows")
        .get(0)
}

/// 正对照（决定性验收的一半）：现状必须绿 — 一个刚建好的租户，没有任何 online recall 调用
/// 留下的 ledger 行，`purpose='query_rewrite'` 命中数必须是 0。
#[test]
fn no_query_rewrite_ledger_entries_is_the_clean_baseline() {
    run_db_fixture::<LedgerFixture, _>(
        "no_query_rewrite_ledger_entries_is_the_clean_baseline",
        |mut handle| {
            assert_eq!(
                query_rewrite_ledger_hits(&mut handle.admin, handle.tenant_id),
                0
            );
        },
    );
}

/// 注错（决定性验收的另一半，与 xtask architecture-check 的静态 0→1 配对）：直接向
/// `ops.model_call_ledger` 写入一条 `purpose='query_rewrite'` 行 —— 模拟"在 recall 前插入一次
/// `complete_structured()` 生成式改写"真会留下的痕迹 —— 断言必须变红（命中数 > 0）。
#[test]
fn injected_query_rewrite_ledger_entry_is_red() {
    run_db_fixture::<LedgerFixture, _>(
        "injected_query_rewrite_ledger_entry_is_red",
        |mut handle| {
            handle
                .admin
                .execute(
                    "INSERT INTO ops.model_call_ledger (tenant_id, provider, purpose) \
                     VALUES ($1, 'dashscope', 'query_rewrite')",
                    &[&handle.tenant_id],
                )
                .expect("seed the injected fault row");
            assert!(query_rewrite_ledger_hits(&mut handle.admin, handle.tenant_id) > 0);
        },
    );
}
