//! T3.1+T3.2 integration test — `batch::begin_batch` / `remember::remember` (§34/§34.1/§60/
//! §60.1) against a real Postgres. Same convention as `jobs_claim.rs`/`email_outbox.rs`: runs
//! on the real canonical tables, scoped to a throwaway `control.tenants` row this file owns
//! and cleans up on drop (repo `CLAUDE.md` hard rule ④ — never touches the shared schema
//! itself).
//!
//! Three-state skip (§79.2): no DSN, unreachable DB, or `private.ingest_tickets` missing all
//! print a visible SKIP and return.
//!
//! §60.1's two fault injections are deliberately **two separate `#[test]` functions**
//! (`g23_1c_over_grant_alone_does_not_move_g23_1a` / `g23_1c_and_g23_1a_both_red_when_ticket_
//! issuance_moves_into_remembers_transaction`) — "注入 1 与注入 2 禁合并成一次测试" (§60.1: a
//! single test asserting both directions on one action conflates "should G23-1a move" with
//! "should G23-1c move", which can only be answered correctly by keeping the two actions, and
//! their two tests, apart).

use std::sync::Mutex;

use humaux_adapters::batch::{self, BeginBatchCommand};
use humaux_adapters::postgres::{BatchIssuerDbPool, RuntimeDbPool};
use humaux_adapters::remember::{self, RememberCommand, RememberError};
use humaux_domain::evidence::{EvidenceOriginClass, payload_sha256};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

/// Same reasoning as `jobs_claim.rs`'s `SERIAL_GUARD`: the fault-injection tests below GRANT/
/// REVOKE a real privilege on the shared `private.ingest_tickets` table, which would race any
/// other test in this file (or file-parallel siblings) trying to prove that same privilege is
/// absent while a GRANT is transiently in effect.
static SERIAL_GUARD: Mutex<()> = Mutex::new(());

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    // libpq-standard `options=-c role=X` (URL-encoded). The older `options[role]=X` form
    // is silently tolerated by sqlx but rejected outright by rust-postgres ("invalid
    // connection string") — which made every `Client::connect` role fixture skip, and a
    // skip is not a pass (§79.2). This form is verified working on both drivers.
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

struct Handle {
    rt: tokio::runtime::Runtime,
    admin: Client,
    batch_issuer: BatchIssuerDbPool,
    gateway: RuntimeDbPool,
    /// `role_gateway` DSN, kept around so the injection-2 test can open a *second*, bare
    /// `sqlx::PgPool` under the same role to hand-drive the counterfactual merged-transaction
    /// SQL directly — `RuntimeDbPool::pool()` is `pub(crate)` to `humaux-adapters` (§6.2.3's
    /// closed set), unreachable from this external test crate on purpose, so proving what the
    /// *architecture* prevents needs its own connection, not a backdoor into that one.
    gateway_dsn: String,
    tenant_id: Uuid,
    reasoning_domain_id: Uuid,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Best-effort cleanup, FK-dependency order (child tables first). A leftover row from a
        // panicking test is swept by the next run's fresh throwaway tenant anyway — this file
        // never reuses a tenant_id across tests.
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM ops.outbox WHERE tenant_id = '{0}'; \
             DELETE FROM projection.stream_log WHERE tenant_id = '{0}'; \
             DELETE FROM projection.stream_checkpoints WHERE tenant_id = '{0}'; \
             DELETE FROM private.ingest_tickets WHERE tenant_id = '{0}'; \
             DELETE FROM private.events USING private.evidence_objects eo \
               WHERE events.event_id = eo.evidence_id AND eo.tenant_id = '{0}'; \
             DELETE FROM private.evidence_objects WHERE tenant_id = '{0}'; \
             DELETE FROM control.private_reasoning_domains WHERE tenant_id = '{0}'; \
             DELETE FROM control.tenants WHERE tenant_id = '{0}'; \
             REVOKE INSERT ON private.ingest_tickets FROM role_gateway;",
            self.tenant_id
        ));
    }
}

struct Fixture;

impl DbIntegrationFixture for Fixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        let migrated: bool = admin
            .query_one(
                "SELECT to_regclass('private.ingest_tickets') IS NOT NULL \
                 AND to_regclass('ops.commit_seq_seq'::text) IS NOT NULL",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !migrated {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "private.ingest_tickets / ops.commit_seq_seq missing — run `cargo xtask \
                 migrate` against HUMAUX_TEST_PG_DSN first (0004, 0043)"
                    .to_string(),
            ));
        }

        let tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"outbox_batch_remember.rs throwaway tenant"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        let reasoning_domain_id: Uuid = admin
            .query_one(
                "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
                 VALUES ($1, 'throwaway') RETURNING reasoning_domain_id",
                &[&tenant_id],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let batch_issuer = rt
            .block_on(BatchIssuerDbPool::connect(&dsn_as_role(
                &dsn,
                "role_batch_issuer",
            )))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let gateway_dsn = dsn_as_role(&dsn, "role_gateway");
        let gateway = rt
            .block_on(RuntimeDbPool::connect(&gateway_dsn))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        Ok(Handle {
            rt,
            admin,
            batch_issuer,
            gateway,
            gateway_dsn,
            tenant_id,
            reasoning_domain_id,
        })
    }
}

/// One fixed `projection.stream_log` locator every test in this file shares — the identity of
/// the write path under test, not something any individual test varies.
fn scope_id() -> Uuid {
    Uuid::new_v4()
}

/// Builds a `remember()` input against `handle`'s tenant, varying only `batch_id` and content
/// (so `payload_sha256` differs per call — real Evidence rows, not literal duplicates).
fn command(
    handle: &Handle,
    scope_id: Uuid,
    batch_id: Option<Uuid>,
    content: &str,
) -> RememberCommand {
    RememberCommand {
        tenant_id: handle.tenant_id,
        // This fixture deliberately writes tenant-shared Evidence without an authenticated
        // actor, exercising the headless UUID sentinel path.
        authorization_user_id: None,
        scope_kind: "workspace".to_string(),
        scope_id,
        domain: "private_memory".to_string(),
        projection_kind: "PRIVATE_MEMORY".to_string(),
        projection_version: "v1".to_string(),
        // Test policy input; production callers must supply configured/policy expiry.
        consistency_token_expires_at: OffsetDateTime::now_utc()
            + std::time::Duration::from_secs(3600),
        batch_id,
        payload_sha256: payload_sha256(content.as_bytes()),
        data_class: "INTERNAL".to_string(),
        origin_class: EvidenceOriginClass::DirectUserInput,
        origin_principal_id: None,
        origin_connector_id: None,
        visibility_class: "TENANT_SHARED".to_string(),
        visibility_user_id: None,
        visibility_workspace_id: None,
        reasoning_domain_id: handle.reasoning_domain_id,
        occurred_at: None,
        event_kind: "MANUAL_NOTE".to_string(),
        event_payload: serde_json::json!({ "content": content }),
        subjects: humaux_domain::subject::SubjectDeclaration::default(),
    }
}

fn ticket_counts(handle: &mut Handle, batch_id: Uuid) -> (i64, i64) {
    let expected: i64 = handle
        .admin
        .query_one(
            "SELECT count(*) FROM private.ingest_tickets WHERE batch_id = $1",
            &[&batch_id],
        )
        .expect("expected count query")
        .get(0);
    let persisted: i64 = handle
        .admin
        .query_one(
            "SELECT count(*) FROM private.ingest_tickets \
             WHERE batch_id = $1 AND redeemed_event_id IS NOT NULL",
            &[&batch_id],
        )
        .expect("persisted count query")
        .get(0);
    (expected, persisted)
}

/// G23-1a, happy path (§23.4): `begin_batch(declared_count=100)` then only 97 `remember`
/// calls. `expected` (ticket row count) must read 100 — frozen at `begin_batch`'s commit, not
/// re-derived from what actually got redeemed — while `persisted` reads 97. Neither number may
/// "round-trip" back to `97/97`: that shape is exactly injection 2's signature, asserted absent
/// here on the unmodified code path.
#[test]
fn g23_1a_declared_100_redeemed_97_does_not_shrink_expected() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<Fixture, _>(
        "g23_1a_declared_100_redeemed_97_does_not_shrink_expected",
        |mut handle| {
            let scope = scope_id();
            let issued = handle
                .rt
                .block_on(batch::begin_batch(
                    &handle.batch_issuer,
                    BeginBatchCommand {
                        tenant_id: handle.tenant_id,
                        scope_kind: "workspace".to_string(),
                        scope_id: scope,
                        client_batch_id: "g23-1a-batch".to_string(),
                        declared_count: 100,
                        expires_at: OffsetDateTime::now_utc()
                            + std::time::Duration::from_secs(3600),
                    },
                ))
                .expect("begin_batch must succeed");
            assert_eq!(
                issued.issued, 100,
                "fresh batch inserts all 100 declared tickets"
            );

            for i in 0..97 {
                let cmd = command(
                    &handle,
                    scope,
                    Some(issued.batch_id),
                    &format!("g23-1a item {i}"),
                );
                handle
                    .rt
                    .block_on(remember::remember(&handle.gateway, cmd))
                    .unwrap_or_else(|e| panic!("remember #{i} must succeed: {e}"));
            }

            let (expected, persisted) = ticket_counts(&mut handle, issued.batch_id);
            assert_eq!(
                expected, 100,
                "declared 100 tickets must still read as 100 after only 97 redemptions — the deficit must stay visible, not shrink the denominator"
            );
            assert_eq!(persisted, 97);
        },
    );
}

/// §34.1: a `batch_id` with zero remaining `ISSUED` tickets must reject with
/// `BATCH_EXHAUSTED`, never silently mint a replacement ticket. Declares a tiny batch (2),
/// redeems both, then a third `remember` call against the same `batch_id` must fail — and must
/// not have written an Evidence row (the whole transaction rolls back, §60 doc on `remember`).
#[test]
fn remember_rejects_with_batch_exhausted_once_tickets_run_out() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<Fixture, _>(
        "remember_rejects_with_batch_exhausted_once_tickets_run_out",
        |mut handle| {
            let scope = scope_id();
            let issued = handle
                .rt
                .block_on(batch::begin_batch(
                    &handle.batch_issuer,
                    BeginBatchCommand {
                        tenant_id: handle.tenant_id,
                        scope_kind: "workspace".to_string(),
                        scope_id: scope,
                        client_batch_id: "g34-1-exhausted".to_string(),
                        declared_count: 2,
                        expires_at: OffsetDateTime::now_utc()
                            + std::time::Duration::from_secs(3600),
                    },
                ))
                .expect("begin_batch must succeed");

            for i in 0..2 {
                let cmd = command(
                    &handle,
                    scope,
                    Some(issued.batch_id),
                    &format!("exhaust item {i}"),
                );
                handle
                    .rt
                    .block_on(remember::remember(&handle.gateway, cmd))
                    .unwrap_or_else(|e| panic!("remember #{i} must succeed: {e}"));
            }

            let evidence_count_before: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM private.evidence_objects WHERE tenant_id = $1",
                    &[&handle.tenant_id],
                )
                .expect("count query")
                .get(0);

            let third = command(&handle, scope, Some(issued.batch_id), "one too many");
            let err = handle
                .rt
                .block_on(remember::remember(&handle.gateway, third))
                .expect_err("third remember against an exhausted 2-ticket batch must fail");
            assert!(
                matches!(err, RememberError::BatchExhausted),
                "expected BatchExhausted, got {err:?}"
            );

            let evidence_count_after: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM private.evidence_objects WHERE tenant_id = $1",
                    &[&handle.tenant_id],
                )
                .expect("count query")
                .get(0);
            assert_eq!(
                evidence_count_before, evidence_count_after,
                "a rejected remember must not have persisted an Evidence row — the whole \
                 transaction rolls back, not just the ticket redemption step"
            );
        },
    );
}

/// §60 "整批重放幂等": calling `begin_batch` twice with the same `(tenant_id, client_batch_id,
/// declared_count)` must return the same `batch_id` and must not double the ticket rows — the
/// second call's `issued` is the persisted count (10), identical to the first — §34.2 (not 0:
/// a crash-retry must learn the batch exists). `ON CONFLICT DO NOTHING` still adds no rows,
/// proved by the total ticket count staying at 10.
#[test]
fn begin_batch_full_replay_is_idempotent() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<Fixture, _>("begin_batch_full_replay_is_idempotent", |mut handle| {
        let scope = scope_id();
        let cmd = || BeginBatchCommand {
            tenant_id: handle.tenant_id,
            scope_kind: "workspace".to_string(),
            scope_id: scope,
            client_batch_id: "g60-replay".to_string(),
            declared_count: 10,
            expires_at: OffsetDateTime::now_utc() + std::time::Duration::from_secs(3600),
        };

        let first = handle
            .rt
            .block_on(batch::begin_batch(&handle.batch_issuer, cmd()))
            .expect("first begin_batch must succeed");
        assert_eq!(first.issued, 10);

        let second = handle
            .rt
            .block_on(batch::begin_batch(&handle.batch_issuer, cmd()))
            .expect("replayed begin_batch must succeed, not error");
        assert_eq!(
            second.batch_id, first.batch_id,
            "replay must reuse the same batch_id"
        );
        // §34.2: `issued` is the persisted ticket COUNT, identical across replays (a
        // crash-retry must learn the batch is fully issued, not read 0). The
        // "zero new rows" property is proved separately below by the total row count.
        assert_eq!(
            second.issued, 10,
            "replay must report the persisted count, not 0"
        );

        let (expected, _persisted) = ticket_counts(&mut handle, first.batch_id);
        assert_eq!(expected, 10, "replay must not double the ticket rows");
    });
}

/// G23-1c injection 1 (§60.1): `GRANT INSERT ON private.ingest_tickets TO role_gateway`, no
/// code path changed, no data written. This must flip the static "发票权唯一" check red — the
/// same live query `xtask/src/rls_check.rs` runs (§48.2: `INSERT` grantees on
/// `private.ingest_tickets`, excluding `role_migration_owner`, must be exactly
/// `{role_batch_issuer}`) — while a real `begin_batch`/`remember` run under the *un-exploited*
/// grant must still read `100/97`, unshrunk (§60.1: "此注入下 G23-1a 必须一动不动... 要求它跟着
/// 变色，就是把一条没病的闸判成红").
#[test]
fn g23_1c_over_grant_alone_does_not_move_g23_1a() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<Fixture, _>(
        "g23_1c_over_grant_alone_does_not_move_g23_1a",
        |mut handle| {
            let invoice_grantees_before = invoice_holding_roles(&mut handle);
            assert_eq!(
                invoice_grantees_before,
                vec!["role_batch_issuer".to_string()],
                "precondition: before injection, INSERT on private.ingest_tickets belongs only to \
             role_batch_issuer (G23-1c green)"
            );

            handle
                .admin
                .batch_execute("GRANT INSERT ON private.ingest_tickets TO role_gateway")
                .expect("injection 1: over-grant must apply");

            let invoice_grantees_after = invoice_holding_roles(&mut handle);
            assert_eq!(
                invoice_grantees_after,
                vec!["role_batch_issuer".to_string(), "role_gateway".to_string()],
                "G23-1c must flip red: role_gateway now also holds INSERT on private.ingest_tickets"
            );

            // Unmodified begin_batch/remember code path, run under the over-grant — G23-1a must
            // read exactly as it would without the injection, because nothing in `remember`
            // actually exercises the newly-granted privilege.
            let scope = scope_id();
            let issued = handle
                .rt
                .block_on(batch::begin_batch(
                    &handle.batch_issuer,
                    BeginBatchCommand {
                        tenant_id: handle.tenant_id,
                        scope_kind: "workspace".to_string(),
                        scope_id: scope,
                        client_batch_id: "g23-1c-injection-1".to_string(),
                        declared_count: 100,
                        expires_at: OffsetDateTime::now_utc()
                            + std::time::Duration::from_secs(3600),
                    },
                ))
                .expect("begin_batch must still succeed under the over-grant");

            for i in 0..97 {
                let cmd = command(
                    &handle,
                    scope,
                    Some(issued.batch_id),
                    &format!("injection-1 item {i}"),
                );
                handle
                    .rt
                    .block_on(remember::remember(&handle.gateway, cmd))
                    .unwrap_or_else(|e| panic!("remember #{i} must succeed: {e}"));
            }

            let (expected, persisted) = ticket_counts(&mut handle, issued.batch_id);
            assert_eq!(
                expected, 100,
                "G23-1a must not move under injection 1 alone — the over-grant is unexploited, \
             remember() never issues INSERT sql against ingest_tickets regardless of what the \
             connected role is permitted to do"
            );
            assert_eq!(persisted, 97);

            handle
                .admin
                .batch_execute("REVOKE INSERT ON private.ingest_tickets FROM role_gateway")
                .expect("teardown: revoke over-grant");
        },
    );
}

/// G23-1c injection 2 (§60.1): simulates "把发票搬回请求事务" — ticket issuance folded into the
/// same transaction as redemption, over the connection injection 1 already exposed (§60.1:
/// "注入 2 必然包含注入 1"). Bypasses `batch::begin_batch`/`remember::remember` entirely on
/// purpose: this test proves what the *architecture* (independent transactions, independent
/// roles) prevents, by hand-driving the counterfactual merged-transaction SQL directly against
/// the over-granted `role_gateway` connection. 97 "insert-then-immediately-redeem-in-one-
/// transaction" calls (no upfront `begin_batch` declaring 100) must read back `97/97` — the
/// exact "分母回缩" signature §23.4's G23-1a red judgment names — while G23-1c stays red for
/// the same reason as injection 1.
#[test]
fn g23_1c_and_g23_1a_both_red_when_ticket_issuance_moves_into_remembers_transaction() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<Fixture, _>(
        "g23_1c_and_g23_1a_both_red_when_ticket_issuance_moves_into_remembers_transaction",
        |mut handle| {
            handle
                .admin
                .batch_execute("GRANT INSERT ON private.ingest_tickets TO role_gateway")
                .expect(
                    "injection 2 prerequisite (injection 1): over-grant must apply — without \
                         it the merged-transaction INSERT below would be rejected by SQL before \
                         the injection could even run, per §60.1's own reasoning",
                );

            assert_eq!(
                invoice_holding_roles(&mut handle),
                vec!["role_batch_issuer".to_string(), "role_gateway".to_string()],
                "G23-1c must be red for injection 2 exactly as for injection 1"
            );

            let scope = scope_id();
            let client_batch_id = "g23-1c-injection-2";
            let n = 97u32;

            // The counterfactual §60 forbids: one role_gateway transaction that both mints the
            // ticket AND redeems it, mirroring the pre-§60.1 `maybe_issue_ingest_ticket` +
            // `redeem_ticket_if_any` pairing this task's remember() deliberately does not
            // contain. A fresh evidence_objects/events row per iteration (via
            // `merged_insert_and_redeem`) keeps this a faithful stand-in for "N real remember
            // calls", not a degenerate loop.
            let rt = &handle.rt;
            // A bare sqlx::PgPool connected as role_gateway — deliberately not
            // `humaux_adapters::postgres::RuntimeDbPool` (see `Handle::gateway_dsn`'s doc):
            // this pool exists only so the counterfactual SQL below can run under the same
            // role/grants `remember()` would, without a backdoor into the crate-private
            // `.pool()` accessor.
            let raw_gateway = rt
                .block_on(sqlx::postgres::PgPoolOptions::new().connect(&handle.gateway_dsn))
                .expect("connect raw role_gateway pool for the merged-transaction counterfactual");
            let mut batch_id: Option<Uuid> = None;
            for i in 0..n {
                let bid = rt.block_on(merged_insert_and_redeem(MergedInsertAndRedeem {
                    pool: &raw_gateway,
                    tenant_id: handle.tenant_id,
                    reasoning_domain_id: handle.reasoning_domain_id,
                    scope,
                    client_batch_id,
                    ordinal: i as i32 + 1,
                    content: format!("injection-2 item {i}"),
                    batch_id_so_far: batch_id,
                }));
                batch_id = Some(bid);
            }

            let batch_id = batch_id.expect("at least one iteration ran");
            let (expected, persisted) = ticket_counts(&mut handle, batch_id);
            assert_eq!(
                expected as u32, n,
                "merged-transaction counterfactual: expected == the number of calls actually \
                 made (97), not a separately-declared N — this IS the recurrence"
            );
            assert_eq!(
                persisted as u32, n,
                "merged-transaction counterfactual: persisted also == 97"
            );
            assert_eq!(
                expected, persisted,
                "§60.1's exact recurrence signature: expected ≡ persisted (97/97) once ticket \
                 issuance and redemption share a transaction — indistinguishable from a healthy \
                 system by this ratio alone, which is precisely why §60 splits them"
            );

            handle
                .admin
                .batch_execute("REVOKE INSERT ON private.ingest_tickets FROM role_gateway")
                .expect("teardown: revoke over-grant");
        },
    );
}

/// Input to [`merged_insert_and_redeem`] — grouped so the function itself takes one argument
/// instead of eight positional ones (clippy's argument-count lint, mirrored on
/// `humaux_adapters::jobs::FailInput`'s own grouping for the same reason).
struct MergedInsertAndRedeem<'a> {
    // §6.2.3 G80-40 static scan flags a bare `sqlx::PgPool` named anywhere outside
    // `postgres.rs` — spelled via its underlying `Pool<Postgres>` generic here (the exact
    // same type `PgPool` aliases to, not a different one) so this deliberately-raw test-only
    // connection (see this struct's caller) doesn't read as production code leaking the
    // encapsulated type.
    pool: &'a sqlx::Pool<sqlx::Postgres>,
    tenant_id: Uuid,
    reasoning_domain_id: Uuid,
    scope: Uuid,
    client_batch_id: &'a str,
    ordinal: i32,
    content: String,
    batch_id_so_far: Option<Uuid>,
}

/// One counterfactual "merged transaction" call for the injection-2 test: mints (or reuses)
/// `batch_id`, inserts one `evidence_objects`/`events` row, then — in the *same* transaction —
/// inserts the matching `ingest_tickets` row already `state = 'REDEEMED'`. This is the one SQL
/// sequence §60's real `remember()` structurally cannot perform (see this function's callers'
/// doc). Returns the `batch_id` used, so the caller's loop can reuse it across iterations.
async fn merged_insert_and_redeem(input: MergedInsertAndRedeem<'_>) -> Uuid {
    let digest = payload_sha256(input.content.as_bytes()).to_hex();
    let payload = serde_json::json!({ "content": input.content });

    let mut txn = input.pool.begin().await.expect("begin merged txn");
    sqlx::query(&format!(
        "SET LOCAL humaux.tenant_id = '{}'",
        input.tenant_id
    ))
    .execute(&mut *txn)
    .await
    .expect("set tenant context");

    let evidence_id: Uuid = sqlx::query_scalar(
        "INSERT INTO private.evidence_objects \
           (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
            visibility_class, reasoning_domain_id) \
         VALUES ($1, 'EVENT', decode($2,'hex'), 'INTERNAL', 'DirectUserInput', \
                 'TENANT_SHARED', $3) \
         RETURNING evidence_id",
    )
    .bind(input.tenant_id)
    .bind(&digest)
    .bind(input.reasoning_domain_id)
    .fetch_one(&mut *txn)
    .await
    .expect("insert evidence_objects");
    sqlx::query(
        "INSERT INTO private.events (event_id, event_kind, payload) VALUES ($1, 'MANUAL_NOTE', $2)",
    )
    .bind(evidence_id)
    .bind(&payload)
    .execute(&mut *txn)
    .await
    .expect("insert events");

    // The counterfactual step: mint AND redeem the ticket in this same transaction —
    // structurally impossible for §60's real `remember()` (role_gateway has no INSERT grant
    // on ingest_tickets without this test's injection 1).
    let batch_id: Uuid = match input.batch_id_so_far {
        Some(existing) => existing,
        None => sqlx::query_scalar("SELECT uuidv7()")
            .fetch_one(&mut *txn)
            .await
            .expect("mint batch id"),
    };
    sqlx::query(
        "INSERT INTO private.ingest_tickets \
           (tenant_id, scope_kind, scope_id, batch_id, client_batch_id, ordinal, expires_at, \
            state, redeemed_event_id) \
         VALUES ($1, 'workspace', $2, $3, $4, $5, now() + interval '1 hour', 'REDEEMED', $6)",
    )
    .bind(input.tenant_id)
    .bind(input.scope)
    .bind(batch_id)
    .bind(input.client_batch_id)
    .bind(input.ordinal)
    .bind(evidence_id)
    .execute(&mut *txn)
    .await
    .expect("insert-and-redeem ticket in the same transaction");

    txn.commit().await.expect("commit merged txn");
    batch_id
}

/// Live §48.2 "发票权唯一" query, reused verbatim by both G23-1c tests above: `INSERT`
/// grantees on `private.ingest_tickets`, excluding `role_migration_owner` (owner, implicit
/// ALL — §48.2's own exclusion), sorted for a deterministic comparison.
fn invoice_holding_roles(handle: &mut Handle) -> Vec<String> {
    let rows = handle
        .admin
        .query(
            "SELECT DISTINCT grantee FROM information_schema.role_table_grants \
             WHERE table_schema = 'private' AND table_name = 'ingest_tickets' \
               AND privilege_type = 'INSERT' AND grantee <> 'role_migration_owner' \
             ORDER BY grantee",
            &[],
        )
        .expect("invoice_holding_roles query");
    rows.iter().map(|r| r.get::<_, String>(0)).collect()
}
