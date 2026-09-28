//! `adapters::tests::email_outbox` — H3 (§74.6) integration test — `email::outbox` against a real Postgres,
//!   exercising the three literal task-brief acceptance behaviors: enqueue never blocks on SMTP, a suppressed address
//!   never gets enqueued (same outcome type either way — unified safety semantics), and a provider failure lands the
//!   row in `FAILED` with a matching delivery event.
//! Depends-on: crates=[humaux-adapters, humaux-testkit, postgres, serde_json, sqlx, tokio]; services=[PostgreSQL(any)
//!   w=[control.users, ops.email_delivery_events, ops.email_outbox, ops.email_suppressions],
//!   PostgreSQL(role_gateway), PostgreSQL(role_private_worker)]; env=[HUMAUX_TEST_PG_DSN]; modules=[adapters::email,
//!   adapters::email::outbox, adapters::email::test_double, adapters::postgres, humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [runs on 0038's real tables and claims only its own user's rows; a suppressed address is never
//!   enqueued and a provider rejection is recorded as a delivery event; no DSN/DB/migration is a visible SKIP]
//! Spec: Baseline §79.2
//!
//! Runs against `migrations/0038_email_deliverability.sql`'s real tables (not a self-built
//! scratch schema like T1.5's `auth_scope_rls.rs`) — `email::outbox`'s SQL names
//! `ops.email_outbox`/`ops.email_suppressions`/`ops.email_delivery_events` directly, so
//! proving the actual code path requires the actual migration applied. Three-state skip
//! (§79.2): no DSN, unreachable DB, or the migration not yet applied all print a visible SKIP
//! and return rather than fail — `cargo xtask migrate` must have run first for these tests to
//! execute for real.
//!
//! [`run_once`](outbox::run_once) claims by `state = 'QUEUED'` queue-wide in production
//! (`claim_user_id: None` — a real worker drains every tenant's rows, not one caller's). This
//! file passes `Some(handle.user_id)` instead, so a test's claim never touches a real queued
//! row left behind by an unrelated concurrent test binary or wave sharing the same dev DB.
//! `SERIAL_GUARD` below is a second, belt-and-suspenders layer: it still serializes every test
//! in *this* file against each other (two tests calling `run_once` concurrently could
//! otherwise race even within their own scoped claims if they shared a `user_id`, which they
//! don't today, but the fixture makes no promise they never will), rather than depending on
//! callers remembering `--test-threads=1`.

use std::sync::Mutex;

use humaux_adapters::email::test_double::{
    EmailErrorKind, RecordingEmailProvider, ScriptedOutcome,
};
use humaux_adapters::email::{EmailStream, outbox};
use humaux_adapters::postgres::{PrivateWorkerDbPool, RuntimeDbPool};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;

static SERIAL_GUARD: Mutex<()> = Mutex::new(());

/// `options[role]=...` on the admin DSN performs a post-connect `SET ROLE` (same technique
/// `postgres.rs`'s own `#[cfg(test)]` fixtures use, reproduced here since that helper is
/// private to its defining module) — no separate per-role credentials needed in dev.
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
    gateway: RuntimeDbPool,
    worker: PrivateWorkerDbPool,
    admin: Client,
    /// Throwaway `control.users` row this fixture owns, FK target for every
    /// `ops.email_outbox.user_id` this test file inserts.
    user_id: Uuid,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Best-effort: removes this run's throwaway user and everything that FK-references
        // it, so a passing run leaves the shared dev DB as it found it (repo CLAUDE.md hard
        // rule ④ — this file never touches a schema/table of its own, only rows it created).
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM ops.email_delivery_events \
               WHERE outbox_id IN (SELECT outbox_id FROM ops.email_outbox WHERE user_id = '{0}'); \
             DELETE FROM ops.email_outbox WHERE user_id = '{0}'; \
             DELETE FROM control.users WHERE user_id = '{0}';",
            self.user_id
        ));
    }
}

struct OutboxFixture;

impl DbIntegrationFixture for OutboxFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        // dep: PostgreSQL(any) — opens the role-scoped connection for `isolate`
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        let migrated: bool = admin
            .query_one("SELECT to_regclass('ops.email_outbox') IS NOT NULL", &[])
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !migrated {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "ops.email_outbox does not exist — run `cargo xtask migrate` \
                 (migrations/0038_email_deliverability.sql) against HUMAUX_TEST_PG_DSN first"
                    .to_string(),
            ));
        }

        let user_id = Uuid::new_v4();
        admin
            .execute(
                "INSERT INTO control.users (user_id) VALUES ($1)",
                &[&user_id],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let (gateway, worker) = rt
            .block_on(async {
                // dep: PostgreSQL(role_gateway) — opens the role-scoped connection for `isolate`
                let gw = RuntimeDbPool::connect(&dsn_as_role(&dsn, "role_gateway")).await?;
                // dep: PostgreSQL(role_private_worker) — opens the role-scoped connection for `isolate`
                let wk =
                    PrivateWorkerDbPool::connect(&dsn_as_role(&dsn, "role_private_worker")).await?;
                Ok::<_, humaux_adapters::postgres::PoolInitError>((gw, wk))
            })
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        Ok(Handle {
            rt,
            gateway,
            worker,
            admin,
            user_id,
        })
    }
}

fn unique_email(tag: &str) -> String {
    format!("email-outbox-test-{tag}-{}@example.invalid", Uuid::new_v4())
}

fn req(handle: &Handle, to: &str, stream: EmailStream) -> outbox::EnqueueRequest {
    outbox::EnqueueRequest {
        user_id: handle.user_id,
        to_email: to.to_string(),
        from_email: "no-reply@humaux.example".to_string(),
        stream,
        template_id: "EMAIL_VERIFICATION".to_string(),
        subject: "Verify your email".to_string(),
        payload: serde_json::json!({}),
    }
}

/// §74.6 "验证码 HTTP handler 不直接阻塞 SMTP" / "入队即返回": `enqueue` does one DB round
/// trip and returns `Queued` with a real row — well under any plausible SMTP round-trip
/// latency, and structurally provable too: `enqueue`'s signature takes no `EmailProvider` at
/// all, so there is nothing in this call that *could* reach the network.
#[test]
fn enqueue_returns_immediately_and_writes_queued_row() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<OutboxFixture, _>(
        "enqueue_returns_immediately_and_writes_queued_row",
        |mut handle| {
            let to = unique_email("enqueue");
            let started = std::time::Instant::now();
            let outcome = handle
                .rt
                .block_on(outbox::enqueue(
                    &handle.gateway,
                    &req(&handle, &to, EmailStream::Transactional),
                ))
                .expect("enqueue must not error for a non-suppressed address");
            let elapsed = started.elapsed();

            assert_eq!(outcome, outbox::EnqueueOutcome::Queued);
            // Generous upper bound — this is not a network call, one local DB round trip should
            // never approach the seconds an SMTP handshake can take.
            assert!(
                elapsed < std::time::Duration::from_secs(2),
                "enqueue took {elapsed:?} — too slow for a DB-only call, suggests it blocked on something else"
            );

            let state: String = handle
                .admin
                .query_one(
                    "SELECT state FROM ops.email_outbox WHERE user_id = $1 AND to_email = $2",
                    &[&handle.user_id, &to],
                )
                .expect("queued row must exist")
                .get(0);
            assert_eq!(state, "QUEUED");
        },
    );
}

/// §74.6 Suppression: "对已 suppression 邮箱需返回统一安全语义, 不泄露账户存在状态" —
/// suppressed and non-suppressed enqueue calls return the *same outcome shape*
/// (`EnqueueOutcome`), the caller is the one that must not branch its HTTP response on it;
/// this test's own obligation is narrower and concrete: the suppressed call must not write a
/// row at all ("不入队").
#[test]
fn suppressed_address_is_not_enqueued() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<OutboxFixture, _>("suppressed_address_is_not_enqueued", |mut handle| {
        let to = unique_email("suppressed");
        handle
            .admin
            .execute(
                "INSERT INTO ops.email_suppressions (email, reason, scope) VALUES ($1, 'HARD_BOUNCE', 'ALL')",
                &[&to],
            )
            .expect("seed suppression row");

        let outcome = handle
            .rt
            .block_on(outbox::enqueue(
                &handle.gateway,
                &req(&handle, &to, EmailStream::Transactional),
            ))
            .expect("enqueue against a suppressed address must not itself error");
        assert_eq!(outcome, outbox::EnqueueOutcome::Suppressed);

        let count: i64 = handle
            .admin
            .query_one(
                "SELECT count(*) FROM ops.email_outbox WHERE to_email = $1",
                &[&to],
            )
            .expect("count query")
            .get(0);
        assert_eq!(
            count, 0,
            "a suppressed address must not produce an ops.email_outbox row"
        );

        handle
            .admin
            .execute(
                "DELETE FROM ops.email_suppressions WHERE email = $1",
                &[&to],
            )
            .expect("cleanup suppression row");
    });
}

/// A marketing-scoped suppression must not block the transactional stream (§74.6 frozen:
/// "不能让营销投诉率打坏验证码/找回密码通道") — the negative case for the same mechanism the
/// previous test proves the positive case for.
#[test]
fn marketing_suppression_does_not_block_transactional_stream() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<OutboxFixture, _>(
        "marketing_suppression_does_not_block_transactional_stream",
        |mut handle| {
            let to = unique_email("marketing-only");
            handle
                .admin
                .execute(
                    "INSERT INTO ops.email_suppressions (email, reason, scope) \
                     VALUES ($1, 'UNSUBSCRIBE', 'MARKETING')",
                    &[&to],
                )
                .expect("seed marketing-scoped suppression row");

            let outcome = handle
                .rt
                .block_on(outbox::enqueue(
                    &handle.gateway,
                    &req(&handle, &to, EmailStream::Transactional),
                ))
                .expect("enqueue must not error");
            assert_eq!(
                outcome,
                outbox::EnqueueOutcome::Queued,
                "a MARKETING-scoped suppression must not block a TRANSACTIONAL send"
            );

            handle
                .admin
                .execute(
                    "DELETE FROM ops.email_outbox WHERE user_id = $1 AND to_email = $2",
                    &[&handle.user_id, &to],
                )
                .expect("cleanup outbox row");
            handle
                .admin
                .execute(
                    "DELETE FROM ops.email_suppressions WHERE email = $1",
                    &[&to],
                )
                .expect("cleanup suppression row");
        },
    );
}

/// Task brief: "provider 失败转 FAILED+事件记录". Enqueues a real row, then runs the worker
/// against a [`RecordingEmailProvider`] scripted to fail — asserts both the outbox row's
/// terminal state and the recorded delivery event.
#[test]
fn provider_failure_marks_row_failed_and_records_event() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<OutboxFixture, _>(
        "provider_failure_marks_row_failed_and_records_event",
        |mut handle| {
            let to = unique_email("provider-fail");
            handle
                .rt
                .block_on(outbox::enqueue(
                    &handle.gateway,
                    &req(&handle, &to, EmailStream::Transactional),
                ))
                .expect("seed a QUEUED row to claim");

            let outbox_id: Uuid = handle
                .admin
                .query_one(
                    "SELECT outbox_id FROM ops.email_outbox WHERE to_email = $1",
                    &[&to],
                )
                .expect("seeded row must exist")
                .get(0);

            let provider = RecordingEmailProvider::new();
            provider.push_outcome(ScriptedOutcome::Err(
                EmailErrorKind::Rejected,
                "550 mailbox unavailable".to_string(),
            ));

            let claimed = handle
                .rt
                .block_on(outbox::run_once(
                    &handle.worker,
                    &provider,
                    10,
                    Some(handle.user_id),
                ))
                .expect("run_once must not itself error on a provider failure");
            assert_eq!(
                claimed, 1,
                "worker must claim exactly the one row this test seeded, scoped by user_id"
            );

            let (state, provider_col): (String, Option<String>) = handle
                .admin
                .query_one(
                    "SELECT state, provider FROM ops.email_outbox WHERE outbox_id = $1",
                    &[&outbox_id],
                )
                .map(|row| (row.get(0), row.get(1)))
                .expect("row must still exist");
            assert_eq!(state, "FAILED");
            assert_eq!(
                provider_col.as_deref(),
                Some("recording-test-double"),
                "outbox.provider must be filled in with the dispatching adapter's identity"
            );

            let event_count: i64 = handle
            .admin
            .query_one(
                "SELECT count(*) FROM ops.email_delivery_events \
                 WHERE outbox_id = $1 AND event_type = 'FAILED' AND provider = 'recording-test-double'",
                &[&outbox_id],
            )
            .expect("count query")
            .get(0);
            assert_eq!(
                event_count, 1,
                "exactly one FAILED delivery event must be recorded, with provider set"
            );
        },
    );
}

/// Success-path counterpart: a provider that returns `Ok` drives the row to `SENT` with a
/// matching `SENT` delivery event carrying the provider's message id.
#[test]
fn provider_success_marks_row_sent_and_records_event() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<OutboxFixture, _>(
        "provider_success_marks_row_sent_and_records_event",
        |mut handle| {
            let to = unique_email("provider-ok");
            handle
                .rt
                .block_on(outbox::enqueue(
                    &handle.gateway,
                    &req(&handle, &to, EmailStream::Transactional),
                ))
                .expect("seed a QUEUED row to claim");

            let outbox_id: Uuid = handle
                .admin
                .query_one(
                    "SELECT outbox_id FROM ops.email_outbox WHERE to_email = $1",
                    &[&to],
                )
                .expect("seeded row must exist")
                .get(0);

            let provider = RecordingEmailProvider::new();
            let claimed = handle
                .rt
                .block_on(outbox::run_once(
                    &handle.worker,
                    &provider,
                    10,
                    Some(handle.user_id),
                ))
                .expect("run_once must succeed");
            assert_eq!(
                claimed, 1,
                "worker must claim exactly the one row this test seeded, scoped by user_id"
            );

            let (state, provider_message_id, provider_col): (String, Option<String>, Option<String>) = handle
                .admin
                .query_one(
                    "SELECT state, provider_message_id, provider FROM ops.email_outbox WHERE outbox_id = $1",
                    &[&outbox_id],
                )
                .map(|row| (row.get(0), row.get(1), row.get(2)))
                .expect("row must still exist");
            assert_eq!(state, "SENT");
            assert!(provider_message_id.is_some());
            assert_eq!(
                provider_col.as_deref(),
                Some("recording-test-double"),
                "outbox.provider must be filled in with the dispatching adapter's identity"
            );

            let event_count: i64 = handle
            .admin
            .query_one(
                "SELECT count(*) FROM ops.email_delivery_events WHERE outbox_id = $1 AND event_type = 'SENT'",
                &[&outbox_id],
            )
            .expect("count query")
            .get(0);
            assert_eq!(event_count, 1);
        },
    );
}
