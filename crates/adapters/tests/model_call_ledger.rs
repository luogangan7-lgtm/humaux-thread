//! T7.4 integration test — `model_call_ledger` (§19.1) against a real Postgres, on
//! `migrations/0094_model_call_ledger_fields.sql` / `0095_provider_pricing_versions.sql` /
//! `0096_tenant_cost_events.sql`. Same convention as `disclosure_ledger.rs`: shared tables,
//! each test scopes rows to its own throwaway `control.tenants` row.
//!
//! Three-state skip (§79.2): no DSN, unreachable DB, or the migrations not yet applied all
//! print a visible SKIP and return.

use humaux_adapters::model_call_ledger::{
    self, FinalizeCall, ModelCallLedgerError, ModelCallOutcome, ReserveCall,
};
use humaux_adapters::postgres::{PrivateWorkerDbPool, RetrievalWorkerDbPool};
use humaux_domain::ledger::ModelCallPurpose;
use humaux_retrieval_provider::cost::{UsageSnapshot, compute_cost};
use humaux_retrieval_provider::pricing::{self, PricingVersion};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::error::SqlState;
use postgres::{Client, NoTls};
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    // Same libpq `options=-c role=X` form `disclosure_ledger.rs` documents (rust-postgres
    // rejects the older `options[role]=X` shape outright).
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

/// The pricing row `a_later_pricing_update_does_not_change_a_historical_calls_cost` inserts is
/// tagged with a source_ref unique to that test's fixture (its tenant id). Every fixture's Drop
/// cleans pricing rows by source_ref, and the binary's tests run in parallel: with one shared
/// literal, a sibling test's teardown deleted this test's row between the INSERT and the read
/// (observed in the card-20 gate chain as "the update must actually have landed a new row").
fn pricing_source_ref(tenant_id: uuid::Uuid) -> String {
    format!("model_call_ledger.rs test {tenant_id}")
}

struct Handle {
    rt: tokio::runtime::Runtime,
    retrieval_worker: RetrievalWorkerDbPool,
    admin: Client,
    tenant_id: Uuid,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Best-effort cleanup (repo CLAUDE.md hard rule ④). ops.model_call_ledger is
        // append-only (migrations/0094's guard trigger rejects DELETE unconditionally, even
        // as `postgres` superuser — BYPASSRLS bypasses row security, not triggers), and
        // control.tenants is FK-referenced by those rows, so neither is deleted here — same
        // permanent-across-runs shape `disclosure_ledger.rs`'s own Drop impl documents.
        // ops.tenant_cost_events carries no such guard, so it is cleaned up.
        let _ = self.admin.execute(
            "DELETE FROM ops.tenant_cost_events WHERE tenant_id = $1",
            &[&self.tenant_id],
        );
        // control.provider_pricing_versions also carries no delete-guard (migrations/0095 —
        // an admin-only table, not append-only by design) — clean up the one row
        // `a_later_pricing_update_does_not_change_a_historical_calls_cost` inserts, tagged by
        // its own `source_ref`. That test also closes the row it superseded (migrations/0097's
        // `provider_pricing_versions_one_open_window`, code-review finding #2, requires it —
        // §19's real bootstrap seed row for this test's key gets its `effective_to` set), so
        // cleanup must also reopen that row (`effective_to = NULL`) — otherwise this test
        // would permanently mutate the real bootstrap seed data on every run, which the old
        // comment here claimed never happened.
        //
        // Order matters: DELETE the marker row *first*, then reopen the old row. Reopening
        // before deleting would momentarily leave both rows open at once, which
        // provider_pricing_versions_one_open_window itself rejects — confirmed live (the
        // opposite order raised exactly that unique-violation). Matched by
        // `old.effective_to = <the marker row's own effective_from>` (the exact close/open
        // boundary this test creates), captured before the DELETE removes it, so this never
        // touches an unrelated row.
        let superseded_at: Option<i64> = self
            .admin
            .query_opt(
                "SELECT extract(epoch FROM effective_from)::bigint \
                 FROM control.provider_pricing_versions WHERE source_ref = $1",
                &[&pricing_source_ref(self.tenant_id)],
            )
            .ok()
            .flatten()
            .map(|row| row.get(0));
        let _ = self.admin.execute(
            "DELETE FROM control.provider_pricing_versions WHERE source_ref = $1",
            &[&pricing_source_ref(self.tenant_id)],
        );
        if let Some(superseded_at) = superseded_at {
            let _ = self.admin.execute(
                "UPDATE control.provider_pricing_versions \
                   SET effective_to = NULL \
                 WHERE provider_id = 'dashscope' AND model_id = 'text-embedding-v4' \
                   AND region = 'cn-beijing' \
                   AND effective_to = to_timestamp($1)",
                &[&(superseded_at as f64)],
            );
        }
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

        let migrated: bool = admin
            .query_one(
                "SELECT EXISTS (
                   SELECT 1 FROM information_schema.columns
                   WHERE table_schema = 'ops' AND table_name = 'model_call_ledger'
                     AND column_name = 'request_id'
                 )",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !migrated {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "ops.model_call_ledger.request_id column missing — run `cargo xtask migrate` \
                 (migrations through 0096_tenant_cost_events.sql) against HUMAUX_TEST_PG_DSN \
                 first"
                    .to_string(),
            ));
        }

        let tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"model_call_ledger.rs throwaway tenant"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        let retrieval_dsn = dsn_as_role(&dsn, "role_retrieval_worker");
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let retrieval_worker = rt
            .block_on(RetrievalWorkerDbPool::connect(&retrieval_dsn))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        Ok(Handle {
            rt,
            retrieval_worker,
            admin,
            tenant_id,
        })
    }
}

fn ledger_row_count(handle: &mut Handle, tenant_id: Uuid) -> i64 {
    handle
        .admin
        .query_one(
            "SELECT count(*) FROM ops.model_call_ledger WHERE tenant_id = $1",
            &[&tenant_id],
        )
        .expect("count query")
        .get(0)
}

fn status_of(handle: &mut Handle, model_call_id: Uuid) -> String {
    handle
        .admin
        .query_one(
            "SELECT status FROM ops.model_call_ledger WHERE model_call_id = $1",
            &[&model_call_id],
        )
        .expect("row must exist")
        .get(0)
}

/// §19.1 reserve()->finalize() happy path: exactly one row, estimated and actual usage/cost
/// both persisted and independently correct, status transitions RESERVED -> SUCCEEDED.
#[test]
fn reserve_then_finalize_records_estimated_and_actual_cost() {
    run_db_fixture::<LedgerFixture, _>(
        "reserve_then_finalize_records_estimated_and_actual_cost",
        |mut handle| {
            let price = PricingVersion {
                input_token_price: 0.5,
                output_token_price: None,
                request_price: None,
                batch_discount: None,
                effective_from: 0,
                effective_to: None,
            };
            let estimated_usage = UsageSnapshot {
                billable_tokens: 400_000,
                output_tokens: 0,
                batch: false,
            };
            let actual_usage = UsageSnapshot {
                billable_tokens: 550_000,
                output_tokens: 0,
                batch: false,
            };
            let estimated_cost = compute_cost(&estimated_usage, &price);
            let actual_cost = compute_cost(&actual_usage, &price);
            assert_ne!(
                estimated_cost, actual_cost,
                "fixture sanity: usage must differ"
            );

            let reserved = handle
                .rt
                .block_on(model_call_ledger::reserve_call(
                    &handle.retrieval_worker,
                    &ReserveCall {
                        request_id: None,
                        tenant_id: handle.tenant_id,
                        workspace_id: None,
                        purpose: Some(ModelCallPurpose::Embedding),
                        provider: "dashscope".to_string(),
                        model: Some("text-embedding-v4".to_string()),
                        model_revision: None,
                        estimated_cost: Some(estimated_cost),
                    },
                ))
                .expect("reserve succeeds");
            assert!(!reserved.already_reserved);
            assert_eq!(status_of(&mut handle, reserved.model_call_id), "RESERVED");

            let changed = handle
                .rt
                .block_on(model_call_ledger::finalize_call(
                    &handle.retrieval_worker,
                    handle.tenant_id,
                    reserved.model_call_id,
                    ModelCallOutcome::Succeeded,
                    &FinalizeCall {
                        input_tokens: Some(550_000),
                        billable_tokens: Some(actual_usage.billable_tokens as i64),
                        actual_cost: Some(actual_cost),
                        ..Default::default()
                    },
                ))
                .expect("finalize succeeds");
            assert!(changed);
            assert_eq!(status_of(&mut handle, reserved.model_call_id), "SUCCEEDED");

            let row = handle
                .admin
                .query_one(
                    "SELECT estimated_cost, actual_cost, billable_tokens \
                     FROM ops.model_call_ledger WHERE model_call_id = $1",
                    &[&reserved.model_call_id],
                )
                .expect("row exists");
            let stored_estimated: f64 = row.get(0);
            let stored_actual: f64 = row.get(1);
            let stored_tokens: i64 = row.get(2);
            assert_eq!(stored_estimated, estimated_cost);
            assert_eq!(stored_actual, actual_cost);
            assert_eq!(stored_tokens, 550_000);
            assert_ne!(stored_estimated, stored_actual);
        },
    );
}

/// §19.1 "reservation-finalize" idempotency: a retry that reuses the same `request_id` returns
/// the original reservation instead of a second row — exactly one `ops.model_call_ledger` row
/// per logical call, even across a retried reserve().
#[test]
fn reserve_with_the_same_request_id_is_idempotent() {
    run_db_fixture::<LedgerFixture, _>(
        "reserve_with_the_same_request_id_is_idempotent",
        |mut handle| {
            let request_id = Uuid::now_v7();
            let input = ReserveCall {
                request_id: Some(request_id),
                tenant_id: handle.tenant_id,
                workspace_id: None,
                purpose: Some(ModelCallPurpose::Rerank),
                provider: "dashscope".to_string(),
                model: Some("qwen3-rerank".to_string()),
                model_revision: None,
                estimated_cost: Some(0.01),
            };

            let first = handle
                .rt
                .block_on(model_call_ledger::reserve_call(
                    &handle.retrieval_worker,
                    &input,
                ))
                .expect("first reserve succeeds");
            assert!(!first.already_reserved);

            let second = handle
                .rt
                .block_on(model_call_ledger::reserve_call(
                    &handle.retrieval_worker,
                    &input,
                ))
                .expect("retried reserve succeeds");
            assert!(second.already_reserved);
            assert_eq!(first.model_call_id, second.model_call_id);

            let rows: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM ops.model_call_ledger WHERE tenant_id = $1 AND request_id = $2",
                    &[&handle.tenant_id, &request_id],
                )
                .expect("count query")
                .get(0);
            assert_eq!(
                rows, 1,
                "a retried reserve() must never produce a second row"
            );
        },
    );
}

/// §19.1 append-only + one-shot finalize: a second finalize() attempt on an already-finalized
/// row must not silently succeed or silently no-op with a changed value — the guard trigger in
/// migrations/0094 raises a hard DB error, and DELETE is rejected outright. This is the
/// mutation this task's "更新价格后重算历史账单必须不变" acceptance rests on structurally: if
/// a row could be re-finalized, a stale in-flight retry could overwrite actual_cost with a
/// number computed against a since-updated PricingVersion.
#[test]
fn a_finalized_row_rejects_a_second_finalize_and_any_delete() {
    run_db_fixture::<LedgerFixture, _>(
        "a_finalized_row_rejects_a_second_finalize_and_any_delete",
        |mut handle| {
            let reserved = handle
                .rt
                .block_on(model_call_ledger::reserve_call(
                    &handle.retrieval_worker,
                    &ReserveCall {
                        request_id: None,
                        tenant_id: handle.tenant_id,
                        workspace_id: None,
                        purpose: Some(ModelCallPurpose::Embedding),
                        provider: "dashscope".to_string(),
                        model: Some("text-embedding-v4".to_string()),
                        model_revision: None,
                        estimated_cost: Some(0.1),
                    },
                ))
                .expect("reserve succeeds");

            let changed = handle
                .rt
                .block_on(model_call_ledger::finalize_call(
                    &handle.retrieval_worker,
                    handle.tenant_id,
                    reserved.model_call_id,
                    ModelCallOutcome::Succeeded,
                    &FinalizeCall {
                        actual_cost: Some(0.11),
                        ..Default::default()
                    },
                ))
                .expect("first finalize succeeds");
            assert!(changed);

            // finalize_call's own WHERE ... AND status = 'RESERVED' means a second call is a
            // no-op UPDATE (0 rows matched) rather than hitting the guard trigger at all —
            // both are acceptable "rejected" outcomes; assert the row-count-affected path.
            let second = handle
                .rt
                .block_on(model_call_ledger::finalize_call(
                    &handle.retrieval_worker,
                    handle.tenant_id,
                    reserved.model_call_id,
                    ModelCallOutcome::Failed,
                    &FinalizeCall {
                        actual_cost: Some(999.0),
                        ..Default::default()
                    },
                ))
                .expect("second finalize call does not error");
            assert!(
                !second,
                "a second finalize() must report no row changed, not silently overwrite"
            );
            assert_eq!(status_of(&mut handle, reserved.model_call_id), "SUCCEEDED");
            let actual_cost: f64 = handle
                .admin
                .query_one(
                    "SELECT actual_cost FROM ops.model_call_ledger WHERE model_call_id = $1",
                    &[&reserved.model_call_id],
                )
                .expect("row exists")
                .get(0);
            assert_eq!(
                actual_cost, 0.11,
                "the first finalize's value must survive untouched"
            );

            // The guard trigger's own DELETE rejection, exercised directly (bypassing this
            // crate's own repo functions, which never attempt DELETE at all) — proves the DB
            // itself refuses, not just that this crate's API surface omits the call.
            let err = handle
                .admin
                .execute(
                    "DELETE FROM ops.model_call_ledger WHERE model_call_id = $1",
                    &[&reserved.model_call_id],
                )
                .expect_err("append-only table must reject DELETE even from an admin connection");
            assert_eq!(
                err.code(),
                Some(&SqlState::INSUFFICIENT_PRIVILEGE),
                "unexpected error class for the append-only guard: {err}"
            );
        },
    );
}

/// §19 core acceptance: "更新价格后重算历史账单必须不变" — a `ModelCallLedger` row's cost,
/// once computed, is never affected by a later `control.provider_pricing_versions` insert.
/// This exercises the full DB round trip `pricing::resolve`'s own unit test only proves in
/// memory: load candidate rows for a call's `(provider, model, region)` at the call's
/// `called_at`, resolve, compute cost, insert a NEW pricing row that supersedes the old one
/// going forward, then re-load and re-resolve against the SAME historical `called_at` — the
/// resolved row and the computed cost must be bit-identical to before the update.
#[test]
fn a_later_pricing_update_does_not_change_a_historical_calls_cost() {
    run_db_fixture::<LedgerFixture, _>(
        "a_later_pricing_update_does_not_change_a_historical_calls_cost",
        |mut handle| {
            // The §19 bootstrap seed (migrations/0095) for this exact (provider, model,
            // region) — used as-is rather than seeding a throwaway row, so this test also
            // proves the real bootstrap data resolves correctly.
            let provider_id = "dashscope";
            let model_id = "text-embedding-v4";
            let region = "cn-beijing";

            let historical_called_at = OffsetDateTime::from_unix_timestamp(
                pricing_bootstrap_effective_from(&mut handle) + 3600,
            )
            .unwrap()
            .unix_timestamp();

            let versions_before = handle
                .rt
                .block_on(model_call_ledger::load_pricing_versions(
                    &handle.retrieval_worker,
                    provider_id,
                    model_id,
                    region,
                ))
                .expect("load pricing versions");
            let as_pure = |rows: &[model_call_ledger::PricingVersionRow]| -> Vec<PricingVersion> {
                rows.iter()
                    .map(|r| PricingVersion {
                        input_token_price: r.input_token_price,
                        output_token_price: r.output_token_price,
                        request_price: r.request_price,
                        batch_discount: r.batch_discount,
                        effective_from: r.effective_from,
                        effective_to: r.effective_to,
                    })
                    .collect()
            };
            let pure_before = as_pure(&versions_before);
            let resolved_before = pricing::resolve(&pure_before, historical_called_at)
                .expect("bootstrap row covers this timestamp");
            let usage = UsageSnapshot {
                billable_tokens: 2_000_000,
                output_tokens: 0,
                batch: true, // §19 bootstrap: text-embedding-v4 has a batch_discount
            };
            let cost_before = compute_cost(&usage, resolved_before);
            assert_eq!(
                cost_before, 0.5,
                "sanity: 2M tokens * (0.5 * (1-0.5)) == 0.5"
            );

            // Simulate a price update: close the currently-open row and insert a NEW,
            // later-effective row for the same (provider, model, region) — admin connection,
            // matching migrations/0095's own "writes are admin/migration-time only" design
            // (role_retrieval_worker has control.* = R only). §19 "允许价格更新而不重写历史
            // 账单": a real update always closes the prior open row's `effective_to` in the
            // same breath as opening the new one (code-review finding #2) — this both matches
            // the real discipline and satisfies migrations/0097's
            // `provider_pricing_versions_one_open_window` unique index, which now rejects the
            // old two-open-rows shape this test used to leave behind.
            let future_from = historical_called_at + 86_400 * 365; // one year later
            handle
                .admin
                .execute(
                    "UPDATE control.provider_pricing_versions \
                       SET effective_to = to_timestamp($4) \
                     WHERE provider_id = $1 AND model_id = $2 AND region = $3 \
                       AND effective_to IS NULL",
                    &[&provider_id, &model_id, &region, &(future_from as f64)],
                )
                .expect("close the prior open pricing row");
            handle
                .admin
                .execute(
                    "INSERT INTO control.provider_pricing_versions \
                       (provider_id, model_id, region, pricing_version, currency, \
                        input_token_price, batch_discount, effective_from, source_ref, verified_at) \
                     VALUES ($1, $2, $3, 'test-price-update', 'CNY', 0.9, 0.5, to_timestamp($4), \
                             $5, now())",
                    &[&provider_id, &model_id, &region, &(future_from as f64), &pricing_source_ref(handle.tenant_id)],
                )
                .expect("insert the updated pricing row");

            let versions_after = handle
                .rt
                .block_on(model_call_ledger::load_pricing_versions(
                    &handle.retrieval_worker,
                    provider_id,
                    model_id,
                    region,
                ))
                .expect("load pricing versions after update");
            assert!(
                versions_after.len() > versions_before.len(),
                "the update must actually have landed a new row"
            );
            let pure_after = as_pure(&versions_after);
            let resolved_after = pricing::resolve(&pure_after, historical_called_at)
                .expect("the historical timestamp must still resolve to a row");
            let cost_after = compute_cost(&usage, resolved_after);

            assert_eq!(
                resolved_after.input_token_price, resolved_before.input_token_price,
                "a later pricing row must not change which row a historical timestamp resolves to"
            );
            assert_eq!(
                cost_after, cost_before,
                "§19: 历史调用永远按当时 pricing snapshot 归因 — a price update must not change \
                 what this historical call recomputes to"
            );

            // And the *new*, later timestamp resolves to the new price — proving the pin above
            // isn't just "resolve() is broken and always returns the first row".
            let future_at = future_from + 3600;
            let resolved_future = pricing::resolve(&pure_after, future_at)
                .expect("a timestamp after the update must resolve to the new row");
            assert_eq!(resolved_future.input_token_price, 0.9);
        },
    );
}

/// Earliest `effective_from` for this `(provider, model, region)` — the §19 bootstrap seed's
/// own row (migrations/0095), robust to this same test file having previously left later
/// "price update" rows behind from an earlier run (`MIN`, not a bare `query_one`, because a
/// second run of this test would otherwise see two-or-more matching rows and
/// `query_one`/`RowCount`-error out — this table has no per-test cleanup, matching
/// `control.provider_pricing_versions`' own "writes are admin/migration-time, append mostly"
/// design, not this test's throwaway-tenant scoping).
fn pricing_bootstrap_effective_from(handle: &mut Handle) -> i64 {
    let row = handle
        .admin
        .query_one(
            "SELECT extract(epoch FROM min(effective_from))::bigint \
             FROM control.provider_pricing_versions \
             WHERE provider_id = 'dashscope' AND model_id = 'text-embedding-v4' \
               AND region = 'cn-beijing'",
            &[],
        )
        .expect("§19 bootstrap seed row (migrations/0095) must exist");
    row.get::<_, i64>(0)
}

/// §19.1 "每次外部调用必须产生 ModelCallLedger 行": a call that never goes through
/// [`model_call_ledger::reserve_call`] leaves zero rows — the enforcement is structural (no
/// second write path exists to produce a row), not a check that runs after the fact. Paired
/// with the wrapped path in the same test so the contrast is the actual assertion, not two
/// separate tests that could each pass by accident.
#[test]
fn only_the_reserve_call_wrapper_ever_produces_a_ledger_row() {
    run_db_fixture::<LedgerFixture, _>(
        "only_the_reserve_call_wrapper_ever_produces_a_ledger_row",
        |mut handle| {
            let tenant_id = handle.tenant_id;
            let before = ledger_row_count(&mut handle, tenant_id);

            // A stand-in for a real EmbeddingProvider/RerankProvider adapter call that
            // (incorrectly) never brackets itself with reserve_call/finalize_call — no SQL
            // against ops.model_call_ledger happens here at all.
            let _simulated_external_call_result = 200; // pretend HTTP status, unused otherwise

            assert_eq!(
                ledger_row_count(&mut handle, tenant_id),
                before,
                "a call that bypasses reserve_call must leave the ledger exactly as it found it"
            );

            let reserved = handle
                .rt
                .block_on(model_call_ledger::reserve_call(
                    &handle.retrieval_worker,
                    &ReserveCall {
                        request_id: None,
                        tenant_id: handle.tenant_id,
                        workspace_id: None,
                        purpose: Some(ModelCallPurpose::Embedding),
                        provider: "dashscope".to_string(),
                        model: Some("text-embedding-v4".to_string()),
                        model_revision: None,
                        estimated_cost: Some(0.01),
                    },
                ))
                .expect("reserve succeeds");
            handle
                .rt
                .block_on(model_call_ledger::finalize_call(
                    &handle.retrieval_worker,
                    handle.tenant_id,
                    reserved.model_call_id,
                    ModelCallOutcome::Succeeded,
                    &FinalizeCall::default(),
                ))
                .expect("finalize succeeds");

            assert_eq!(
                ledger_row_count(&mut handle, tenant_id),
                before + 1,
                "the wrapped call must have produced exactly one ledger row"
            );
        },
    );
}

/// §19 Tenant Full Cost Ledger bridge: a finalized external-model-token call feeds one
/// `ops.tenant_cost_events` row via [`model_call_ledger::record_external_model_cost_event`].
#[test]
fn finalized_call_can_feed_a_tenant_cost_event() {
    run_db_fixture::<LedgerFixture, _>(
        "finalized_call_can_feed_a_tenant_cost_event",
        |mut handle| {
            let reserved = handle
                .rt
                .block_on(model_call_ledger::reserve_call(
                    &handle.retrieval_worker,
                    &ReserveCall {
                        request_id: None,
                        tenant_id: handle.tenant_id,
                        workspace_id: None,
                        purpose: Some(ModelCallPurpose::Embedding),
                        provider: "dashscope".to_string(),
                        model: Some("text-embedding-v4".to_string()),
                        model_revision: None,
                        estimated_cost: Some(0.2),
                    },
                ))
                .expect("reserve succeeds");
            handle
                .rt
                .block_on(model_call_ledger::finalize_call(
                    &handle.retrieval_worker,
                    handle.tenant_id,
                    reserved.model_call_id,
                    ModelCallOutcome::Succeeded,
                    &FinalizeCall {
                        billable_tokens: Some(400_000),
                        actual_cost: Some(0.2),
                        ..Default::default()
                    },
                ))
                .expect("finalize succeeds");

            handle
                .rt
                .block_on(model_call_ledger::record_external_model_cost_event(
                    &handle.retrieval_worker,
                    handle.tenant_id,
                    reserved.request_id,
                    400_000,
                    Some(0.5),
                    0.2,
                    // A fixed instant that is itself a month start (2026-08-01), required by
                    // migrations/0099's `tenant_cost_events_period_is_month_start` CHECK
                    // (code-review finding #8: `period` must equal `date_trunc('month',
                    // period)`) — decomposed to a plain `Date` without needing `time::Month`
                    // (not re-exported via `sqlx::types::time`).
                    OffsetDateTime::from_unix_timestamp(1_785_542_400)
                        .unwrap()
                        .date(),
                ))
                .expect("cost event insert succeeds");

            let row = handle
                .admin
                .query_one(
                    "SELECT cost_type, quantity, unit, estimated_cost, source \
                 FROM ops.tenant_cost_events WHERE tenant_id = $1",
                    &[&handle.tenant_id],
                )
                .expect("cost event row exists");
            let cost_type: String = row.get(0);
            let quantity: f64 = row.get(1);
            let unit: String = row.get(2);
            let estimated_cost: f64 = row.get(3);
            let source: String = row.get(4);
            assert_eq!(cost_type, "external_model_tokens");
            assert_eq!(quantity, 400_000.0);
            assert_eq!(unit, "tokens");
            assert_eq!(estimated_cost, 0.2);
            assert_eq!(source, reserved.request_id.to_string());
        },
    );
}

/// A finalize() against a `model_call_id` that was never reserved (or belongs to a different
/// tenant) must report no row changed — never silently insert or adopt another tenant's row.
#[test]
fn finalize_of_an_unknown_model_call_id_reports_no_change() {
    run_db_fixture::<LedgerFixture, _>(
        "finalize_of_an_unknown_model_call_id_reports_no_change",
        |handle| {
            let unknown = Uuid::now_v7();
            let changed = handle
                .rt
                .block_on(model_call_ledger::finalize_call(
                    &handle.retrieval_worker,
                    handle.tenant_id,
                    unknown,
                    ModelCallOutcome::Failed,
                    &FinalizeCall {
                        error_class: Some("PROVIDER_TRANSIENT".to_string()),
                        ..Default::default()
                    },
                ))
                .expect("finalize call itself does not error");
            assert!(!changed);
        },
    );
}

// Keep the ModelCallLedgerError import exercised (Display/Error impls) — a plain instantiation
// via a real error would need a real query failure; this proves the impls compile end to end
// under this crate's own edition rather than only inside src/ unit tests.
#[allow(dead_code)]
fn _assert_error_impls_are_object_safe(e: ModelCallLedgerError) {
    let _: &dyn std::error::Error = &e;
}

// ============================================================================
// Card 20 (ADR-0042): the private reasoning purposes and the DB<->Rust closed-set contract.
// ============================================================================

/// Every single-quoted literal in a deparsed CHECK. PostgreSQL renders
/// `CHECK (col IN ('A','B'))` as `(col = ANY (ARRAY['A'::text, 'B'::text]))`, so a test that
/// grepped for `IN (` would pass on the authored text and fail on the catalog — this reads the
/// literals themselves and is indifferent to which form the deparser chose.
fn quoted_literals(constraint_def: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = constraint_def;
    while let Some(open) = rest.find('\'') {
        let after = &rest[open + 1..];
        let Some(close) = after.find('\'') else { break };
        out.push(after[..close].to_string());
        rest = &after[close + 1..];
    }
    out
}

fn purpose_constraint_def(handle: &mut Handle) -> String {
    handle
        .admin
        .query_one(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
             WHERE conrelid = 'ops.model_call_ledger'::regclass \
               AND conname = 'model_call_ledger_purpose_known'",
            &[],
        )
        .expect("model_call_ledger_purpose_known must exist")
        .get(0)
}

/// §78.2 DB enum <-> Rust enum contract. The mirror is checked in BOTH directions, so the
/// card-20 fault injection ("emit a purpose value not present in the Rust enum and the mirror
/// test must go red") is red whichever side drifts: a value added to
/// `migrations/0166_model_call_ledger_private_purposes.sql` without a
/// `humaux_domain::ledger::ModelCallPurpose` variant fails the first assert, and a variant
/// added without widening the CHECK fails the second.
#[test]
fn db_purpose_check_mirrors_the_rust_closed_set() {
    run_db_fixture::<LedgerFixture, _>(
        "db_purpose_check_mirrors_the_rust_closed_set",
        |mut handle| {
            let def = purpose_constraint_def(&mut handle);
            // Card note (e): accept both deparse forms rather than pinning one.
            assert!(
                def.contains("IN (") || def.contains("= ANY (ARRAY["),
                "purpose CHECK must still be a closed set, got: {def}"
            );

            let mut in_db = quoted_literals(&def);
            in_db.sort_unstable();
            in_db.dedup();

            let mut in_rust: Vec<String> = humaux_domain::ledger::ModelCallPurpose::ALL
                .into_iter()
                .map(|purpose| purpose.as_db_str().to_string())
                .collect();
            in_rust.sort_unstable();

            assert_eq!(
                in_db, in_rust,
                "ops.model_call_ledger.purpose CHECK and ModelCallPurpose::ALL have drifted — \
             widen/narrow both in one migration (§78.2)"
            );
        },
    );
}

/// The DB is the enforcing side, not just the documenting one: a purpose string that has no
/// `ModelCallPurpose` variant is rejected by the CHECK even for the owner role. Without this,
/// the mirror test above would still pass against a CHECK that had been dropped entirely.
#[test]
fn purpose_outside_the_closed_set_is_rejected_by_the_database() {
    run_db_fixture::<LedgerFixture, _>(
        "purpose_outside_the_closed_set_is_rejected_by_the_database",
        |mut handle| {
            let tenant_id = handle.tenant_id;
            let error = handle
                .admin
                .execute(
                    "INSERT INTO ops.model_call_ledger (request_id, tenant_id, provider, purpose) \
                     VALUES (gen_random_uuid(), $1, 'dashscope', 'PRIVATE_SOMETHING_ELSE')",
                    &[&tenant_id],
                )
                .expect_err("an unknown purpose must not be insertable");
            assert_eq!(
                error.code(),
                Some(&SqlState::CHECK_VIOLATION),
                "expected model_call_ledger_purpose_known to reject it, got: {error}"
            );
        },
    );
}

/// §11.6/§11.7: the private hops' reserve->finalize round trip on the role that actually runs
/// them (`role_private_worker`), through the same registration point the retrieval hops use.
/// One row per call, purpose/model/tenant persisted, token usage landed at finalize.
#[test]
fn private_purposes_reserve_and_finalize_on_the_private_worker_pool() {
    run_db_fixture::<LedgerFixture, _>(
        "private_purposes_reserve_and_finalize_on_the_private_worker_pool",
        |mut handle| {
            let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("fixture already required it");
            let private_worker = handle
                .rt
                .block_on(PrivateWorkerDbPool::connect(&dsn_as_role(
                    &dsn,
                    "role_private_worker",
                )))
                .expect("role_private_worker pool");
            let tenant_id = handle.tenant_id;
            let before = ledger_row_count(&mut handle, tenant_id);

            for (purpose, model) in [
                (ModelCallPurpose::PrivateDistillText, "MiniMax-Text-01"),
                (ModelCallPurpose::PrivateDistillVision, "MiniMax-VL-01"),
                (ModelCallPurpose::PrivateConsolidate, "MiniMax-Text-01"),
            ] {
                let reserved = handle
                    .rt
                    .block_on(model_call_ledger::reserve_private_call(
                        &private_worker,
                        &ReserveCall {
                            request_id: None,
                            tenant_id: handle.tenant_id,
                            workspace_id: None,
                            purpose: Some(purpose),
                            provider: "minimax".to_string(),
                            model: Some(model.to_string()),
                            model_revision: None,
                            estimated_cost: None,
                        },
                    ))
                    .expect("private purposes are admitted by the 0166 CHECK");
                assert!(!reserved.already_reserved);
                assert_eq!(status_of(&mut handle, reserved.model_call_id), "RESERVED");

                let changed = handle
                    .rt
                    .block_on(model_call_ledger::finalize_private_call(
                        &private_worker,
                        handle.tenant_id,
                        reserved.model_call_id,
                        ModelCallOutcome::Succeeded,
                        &FinalizeCall {
                            input_tokens: Some(1234),
                            billable_tokens: Some(1234),
                            output_tokens: Some(5678),
                            latency_ms: Some(42),
                            ..Default::default()
                        },
                    ))
                    .expect("finalize succeeds");
                assert!(changed);

                let row = handle
                    .admin
                    .query_one(
                        "SELECT purpose, model, tenant_id, status, input_tokens, \
                                billable_tokens, output_tokens, latency_ms, \
                                reasoning_domain_id IS NULL AND binding_id IS NULL \
                                AND billing_responsibility IS NULL AS route_columns_null \
                         FROM ops.model_call_ledger WHERE model_call_id = $1",
                        &[&reserved.model_call_id],
                    )
                    .expect("row must exist");
                assert_eq!(row.get::<_, String>("purpose"), purpose.as_db_str());
                assert_eq!(row.get::<_, String>("model"), model);
                assert_eq!(row.get::<_, Uuid>("tenant_id"), handle.tenant_id);
                assert_eq!(row.get::<_, String>("status"), "SUCCEEDED");
                assert_eq!(row.get::<_, i64>("input_tokens"), 1234);
                // §19.1 both priced dimensions (0168): a generative call bills the prompt at
                // `input_token_price` and the completion at `output_token_price`. Persisting
                // only the first is what left these rows unpriceable.
                assert_eq!(row.get::<_, i64>("billable_tokens"), 1234);
                assert_eq!(row.get::<_, i64>("output_tokens"), 5678);
                assert_eq!(row.get::<_, i32>("latency_ms"), 42);
                // 0130's USER-paid arm stays untouched: a platform-paid private row carries no
                // route/billing snapshot (migrations/0166 header).
                assert!(row.get::<_, bool>("route_columns_null"));
            }

            assert_eq!(
                ledger_row_count(&mut handle, tenant_id) - before,
                3,
                "exactly one ledger row per private call — no second receipt mechanism"
            );
        },
    );
}

/// Card 20 acceptance: "a failed provider call still produces a ledger row recording the
/// failure rather than nothing." The FAILED shape has no token numbers at all and an
/// `error_class` — and it is still one row, not zero.
#[test]
fn a_failed_private_call_is_ledgered_as_failed_not_absent() {
    run_db_fixture::<LedgerFixture, _>(
        "a_failed_private_call_is_ledgered_as_failed_not_absent",
        |mut handle| {
            let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("fixture already required it");
            let private_worker = handle
                .rt
                .block_on(PrivateWorkerDbPool::connect(&dsn_as_role(
                    &dsn,
                    "role_private_worker",
                )))
                .expect("role_private_worker pool");

            let reserved = handle
                .rt
                .block_on(model_call_ledger::reserve_private_call(
                    &private_worker,
                    &ReserveCall {
                        request_id: None,
                        tenant_id: handle.tenant_id,
                        workspace_id: None,
                        purpose: Some(ModelCallPurpose::PrivateConsolidate),
                        provider: "minimax".to_string(),
                        model: Some("MiniMax-Text-01".to_string()),
                        model_revision: None,
                        estimated_cost: None,
                    },
                ))
                .expect("reserve succeeds");

            let changed = handle
                .rt
                .block_on(model_call_ledger::finalize_private_call(
                    &private_worker,
                    handle.tenant_id,
                    reserved.model_call_id,
                    ModelCallOutcome::Failed,
                    &FinalizeCall {
                        latency_ms: Some(7),
                        error_class: Some("PROVIDER_ERROR".to_string()),
                        ..Default::default()
                    },
                ))
                .expect("finalize succeeds");
            assert!(changed);

            let row = handle
                .admin
                .query_one(
                    "SELECT status, error_class, input_tokens IS NULL AS no_tokens \
                     FROM ops.model_call_ledger WHERE model_call_id = $1",
                    &[&reserved.model_call_id],
                )
                .expect("a failed call still leaves its row");
            assert_eq!(row.get::<_, String>("status"), "FAILED");
            assert_eq!(row.get::<_, String>("error_class"), "PROVIDER_ERROR");
            assert!(row.get::<_, bool>("no_tokens"));
        },
    );
}
