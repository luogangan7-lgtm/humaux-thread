//! `adapters::tests::jobs_claim` — T3.5 integration test — `jobs` (§31/§61) against a real Postgres.
//! Depends-on: crates=[humaux-adapters, humaux-testkit, postgres, sqlx, tokio]; services=[PostgreSQL(any)
//!   r=[ops.claim_derived_work] w=[control.tenants, ops.jobs], PostgreSQL(role_consolidation_worker),
//!   PostgreSQL(role_gateway), PostgreSQL(role_private_worker)]; env=[HUMAUX_TEST_PG_DSN]; modules=[adapters::jobs, adapters::postgres,
//!   humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [runs on the real ops.jobs with rows scoped to a throwaway tenant; SKIP LOCKED claims never hand one
//!   job to two claimers; no DSN, unreachable DB or ops.jobs missing is a visible SKIP]
//! Spec: Baseline §31; §61; §79.2
//!
//! Runs on
//! `migrations/0008_ops_core.sql`'s real `ops.jobs` table (same convention as
//! `email_outbox.rs`: the table is shared, not a scratch schema, so every test scopes rows to
//! a throwaway `control.tenants` row this file owns and cleans up on drop).
//!
//! Three-state skip (§79.2): no DSN, unreachable DB, or `ops.jobs` missing all print a visible
//! SKIP and return.

use std::sync::Mutex;

use humaux_adapters::jobs::{self, FailInput, JobStatus};
use humaux_adapters::postgres::{ConsolidationDbPool, PrivateWorkerDbPool, RuntimeDbPool};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;

/// Serializes every test in this file. Each test seeds its own throwaway tenant so rows never
/// overlap between tests, but `RuntimeDbPool` is a real connection pool and the "no duplicate
/// claim across concurrent workers" test deliberately drives concurrent claims *within* itself
/// — running two such tests in true parallel would not corrupt correctness (tenants are
/// disjoint) but would make timing assertions (e.g. the empty-claim non-blocking check) noisy.
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
    gateway: RuntimeDbPool,
    private: PrivateWorkerDbPool,
    /// ADR-0058 D-L: the v1 derived claim/heartbeat/settle serve `role_consolidation_worker` only.
    consolidation: ConsolidationDbPool,
    admin: Client,
    tenant_id: Uuid,
    /// `role_gateway` DSN, kept around so the concurrency test below can open one independent
    /// `RuntimeDbPool` per simulated worker thread — `RuntimeDbPool` is deliberately not
    /// `Clone` (§6.2.3 closed set), and a real deployment's concurrent workers are separate
    /// processes with their own pools anyway, which this mirrors more faithfully than sharing
    /// one pool across `tokio::join!`-style concurrent futures would.
    gateway_dsn: String,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Best-effort cleanup (repo CLAUDE.md hard rule ④: this file never touches a
        // schema/table of its own, only rows it created under its own throwaway tenant).
        // Card-31 pattern (card 33 leak fix): this file seeds claimable jobs directly. They go in
        // their own batch whose failure is printed; the tenant row goes in a separate best-effort
        // batch, so a refused tenant delete can no longer roll the job delete back.
        if let Err(error) = self.admin.batch_execute(&format!(
            "DELETE FROM ops.jobs WHERE tenant_id = '{0}';",
            self.tenant_id,
        )) {
            eprintln!(
                "jobs_claim cleanup failed for tenant {}: {error}",
                self.tenant_id
            );
        }
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM control.tenants WHERE tenant_id = '{0}';",
            self.tenant_id,
        ));
    }
}

struct JobsFixture;

impl DbIntegrationFixture for JobsFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        let migrated: bool = admin
            .query_one("SELECT to_regclass('ops.jobs') IS NOT NULL", &[])
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !migrated {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "ops.jobs does not exist — run `cargo xtask migrate` \
                 (migrations/0008_ops_core.sql) against HUMAUX_TEST_PG_DSN first"
                    .to_string(),
            ));
        }

        let tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"jobs_claim.rs throwaway tenant"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        let gateway_dsn = dsn_as_role(&dsn, "role_gateway");
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let gateway = rt
            // dep: PostgreSQL(role_gateway) — open a role-scoped PG connection/pool for this test
            .block_on(RuntimeDbPool::connect(&gateway_dsn))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let private = rt
            // dep: PostgreSQL(role_private_worker) — open a role-scoped PG connection/pool for this test
            .block_on(PrivateWorkerDbPool::connect(&dsn_as_role(
                &dsn,
                "role_private_worker",
            )))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let consolidation = rt
            // dep: PostgreSQL(role_consolidation_worker) — open a role-scoped PG connection/pool for this test
            .block_on(ConsolidationDbPool::connect(&dsn_as_role(
                &dsn,
                "role_consolidation_worker",
            )))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        Ok(Handle {
            rt,
            gateway,
            private,
            consolidation,
            admin,
            tenant_id,
            gateway_dsn,
        })
    }
}

/// Seeds one `PENDING` `ops.jobs` row, claimable immediately (`next_retry_at = now()` — see
/// `jobs.rs`'s module doc on why this must be set explicitly). Returns its `job_id`.
fn seed_pending_with_type(handle: &mut Handle, job_type: &str, idempotency_key: &str) -> Uuid {
    handle
        .admin
        .query_one(
            "INSERT INTO ops.jobs (tenant_id, job_type, idempotency_key, next_retry_at) \
             VALUES ($1, $2, $3, now()) RETURNING job_id",
            &[&handle.tenant_id, &job_type, &idempotency_key],
        )
        .expect("seed PENDING job")
        .get(0)
}

fn seed_pending(handle: &mut Handle, idempotency_key: &str) -> Uuid {
    seed_pending_with_type(handle, "test.noop", idempotency_key)
}

fn job_status(handle: &mut Handle, job_id: Uuid) -> String {
    handle
        .admin
        .query_one("SELECT status FROM ops.jobs WHERE job_id = $1", &[&job_id])
        .expect("job must exist")
        .get(0)
}

fn job_attempt(handle: &mut Handle, job_id: Uuid) -> i32 {
    handle
        .admin
        .query_one("SELECT attempt FROM ops.jobs WHERE job_id = $1", &[&job_id])
        .expect("job must exist")
        .get(0)
}

/// §61's own claim SQL is a single atomic statement (the row lock and the `PROCESSING` flip
/// happen together), so no-duplicate-claim holds even under sequential calls — but the
/// property this test exists to prove is specifically that it holds under *real* concurrency,
/// not just atomicity read on paper. Seeds 6 claimable jobs and fires 4 concurrent `claim`
/// calls (limit 2 each, 8 requested seats for 6 rows) with distinct `lease_owner`s; asserts
/// every claimed `job_id` is unique across all four results (no job claimed twice) and that
/// exactly 6 rows total were claimed (every claimable row picked up exactly once).
#[test]
fn claim_has_no_duplicate_across_concurrent_workers() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<JobsFixture, _>(
        "claim_has_no_duplicate_across_concurrent_workers",
        |mut handle| {
            let seeded: Vec<Uuid> = (0..6)
                .map(|i| seed_pending(&mut handle, &format!("dup-check-{i}-{}", Uuid::new_v4())))
                .collect();

            let tenant_id = handle.tenant_id;
            let owners = ["worker-a", "worker-b", "worker-c", "worker-d"];
            // Real OS threads, each opening its own `RuntimeDbPool` off the same DSN — mirrors
            // independent worker processes (see `Handle::gateway_dsn`'s doc) and guarantees actual
            // concurrent transactions hit `ops.jobs` at once, not just sequential-but-atomic calls.
            let results: Vec<Vec<Uuid>> = std::thread::scope(|scope| {
                let handles: Vec<_> = owners
                    .iter()
                    .map(|owner| {
                        let dsn = handle.gateway_dsn.clone();
                        scope.spawn(move || {
                            let rt =
                                tokio::runtime::Runtime::new().expect("tokio runtime per thread");
                            rt.block_on(async {
                                // dep: PostgreSQL(role_gateway) — open a role-scoped PG connection/pool for this test
                                let pool = RuntimeDbPool::connect(&dsn).await.unwrap_or_else(|e| {
                                    panic!("connect for {owner} must succeed: {e}")
                                });
                                jobs::claim(&pool, tenant_id, owner, 60.0, 2)
                                    .await
                                    .unwrap_or_else(|e| {
                                        panic!("claim by {owner} must not error: {e}")
                                    })
                                    .into_iter()
                                    .map(|j| j.job_id)
                                    .collect::<Vec<_>>()
                            })
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().expect("worker thread must not panic"))
                    .collect()
            });

            let mut all_claimed: Vec<Uuid> = results.into_iter().flatten().collect();
            let total_claimed = all_claimed.len();
            assert_eq!(
                total_claimed, 6,
                "6 claimable rows across 4 workers requesting 2 each (8 seats) must yield exactly \
             6 claims total, not fewer (would mean a row got skipped) or more (would mean a \
             duplicate claim)"
            );

            all_claimed.sort();
            all_claimed.dedup();
            assert_eq!(
                all_claimed.len(),
                total_claimed,
                "every claimed job_id must be unique — a duplicate here means two workers both \
             claimed the same row"
            );

            let mut seeded_sorted = seeded.clone();
            seeded_sorted.sort();
            assert_eq!(
                all_claimed, seeded_sorted,
                "the claimed set must be exactly the seeded set"
            );
        },
    );
}

/// §31 "WAITING_KEY 不消耗 retry": claim (attempt 0 -> 1), mark_waiting_key, then
/// resume_from_waiting_key — none of the WAITING_KEY-path transitions touch `attempt`, so it
/// must read back as 1 (the value [`jobs::claim`] left it at) at every step of the round trip,
/// and the job must land back in `PENDING`, claimable again.
#[test]
fn waiting_key_round_trip_does_not_increment_attempt() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<JobsFixture, _>(
        "waiting_key_round_trip_does_not_increment_attempt",
        |mut handle| {
            let job_id = seed_pending(&mut handle, &format!("waiting-key-{}", Uuid::new_v4()));
            let tenant_id = handle.tenant_id;

            let claimed = handle
                .rt
                .block_on(jobs::claim(
                    &handle.gateway,
                    tenant_id,
                    "wk-worker",
                    60.0,
                    1,
                ))
                .expect("claim must succeed");
            assert_eq!(claimed.len(), 1);
            assert_eq!(
                claimed[0].attempt, 1,
                "first claim must bump attempt 0 -> 1"
            );
            assert_eq!(job_attempt(&mut handle, job_id), 1);

            let marked = handle
                .rt
                .block_on(jobs::mark_waiting_key(
                    &handle.gateway,
                    tenant_id,
                    job_id,
                    "wk-worker",
                    claimed[0].attempt,
                ))
                .expect("mark_waiting_key must not error");
            assert!(
                marked,
                "mark_waiting_key must apply while PROCESSING under the claiming lease"
            );
            assert_eq!(job_status(&mut handle, job_id), "WAITING_KEY");
            assert_eq!(
                job_attempt(&mut handle, job_id),
                1,
                "mark_waiting_key must not touch attempt"
            );

            let resumed = handle
                .rt
                .block_on(jobs::resume_from_waiting_key(
                    &handle.gateway,
                    tenant_id,
                    job_id,
                ))
                .expect("resume_from_waiting_key must not error");
            assert!(resumed);
            assert_eq!(job_status(&mut handle, job_id), "PENDING");
            assert_eq!(
                job_attempt(&mut handle, job_id),
                1,
                "the full WAITING_KEY round trip must leave attempt exactly where claim left it \
             (§31 WAITING_KEY 不消耗 retry)"
            );
        },
    );
}

/// §31 DEAD terminal state must be reachable: claim once (attempt -> 1), then `fail` with
/// `max_attempts = 1` — `attempt (1) >= max_attempts (1)` must land on `DEAD`, not
/// `RETRY_WAIT`.
#[test]
fn dead_is_reachable_once_retry_budget_is_exhausted() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<JobsFixture, _>(
        "dead_is_reachable_once_retry_budget_is_exhausted",
        |mut handle| {
            let job_id = seed_pending(&mut handle, &format!("dead-{}", Uuid::new_v4()));
            let tenant_id = handle.tenant_id;

            let claimed = handle
                .rt
                .block_on(jobs::claim(
                    &handle.gateway,
                    tenant_id,
                    "dead-worker",
                    60.0,
                    1,
                ))
                .expect("claim must succeed");
            let attempt = claimed[0].attempt;

            let outcome = handle
                .rt
                .block_on(jobs::fail(
                    &handle.gateway,
                    tenant_id,
                    FailInput {
                        job_id,
                        lease_owner: "dead-worker",
                        attempt,
                        error_class: "boom",
                        retryable: true,
                        max_attempts: 1,
                        retry_after_seconds: 1.0,
                    },
                ))
                .expect("fail must not error");
            assert_eq!(outcome, Some(JobStatus::Dead));
            assert_eq!(job_status(&mut handle, job_id), "DEAD");
        },
    );
}

/// `fail` with `retryable = false` must go straight to `FAILED` regardless of remaining
/// budget — the permanent-error path, distinct from `DEAD` (budget exhaustion).
#[test]
fn non_retryable_failure_reaches_failed_not_dead() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<JobsFixture, _>(
        "non_retryable_failure_reaches_failed_not_dead",
        |mut handle| {
            let job_id = seed_pending(&mut handle, &format!("failed-{}", Uuid::new_v4()));
            let tenant_id = handle.tenant_id;

            let claimed = handle
                .rt
                .block_on(jobs::claim(
                    &handle.gateway,
                    tenant_id,
                    "failed-worker",
                    60.0,
                    1,
                ))
                .expect("claim must succeed");
            let attempt = claimed[0].attempt;

            let outcome = handle
                .rt
                .block_on(jobs::fail(
                    &handle.gateway,
                    tenant_id,
                    FailInput {
                        job_id,
                        lease_owner: "failed-worker",
                        attempt,
                        error_class: "permanent_schema_violation",
                        retryable: false,
                        max_attempts: 100,
                        retry_after_seconds: 1.0,
                    },
                ))
                .expect("fail must not error");
            assert_eq!(outcome, Some(JobStatus::Failed));
            assert_eq!(job_status(&mut handle, job_id), "FAILED");
        },
    );
}

/// `complete` drives a claimed job to `DONE`.
#[test]
fn complete_transitions_processing_to_done() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<JobsFixture, _>("complete_transitions_processing_to_done", |mut handle| {
        let job_id = seed_pending(&mut handle, &format!("done-{}", Uuid::new_v4()));
        let tenant_id = handle.tenant_id;

        let claimed = handle
            .rt
            .block_on(jobs::claim(
                &handle.gateway,
                tenant_id,
                "done-worker",
                60.0,
                1,
            ))
            .expect("claim must succeed");

        let completed = handle
            .rt
            .block_on(jobs::complete(
                &handle.gateway,
                tenant_id,
                job_id,
                "done-worker",
                claimed[0].attempt,
            ))
            .expect("complete must not error");
        assert!(completed);
        assert_eq!(job_status(&mut handle, job_id), "DONE");
    });
}

/// `heartbeat` extends `lease_expires_at` for a still-owned `PROCESSING` lease.
#[test]
fn heartbeat_extends_lease_expiry() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<JobsFixture, _>("heartbeat_extends_lease_expiry", |mut handle| {
        let job_id = seed_pending(&mut handle, &format!("heartbeat-{}", Uuid::new_v4()));
        let tenant_id = handle.tenant_id;

        let claimed = handle
            .rt
            .block_on(jobs::claim(&handle.gateway, tenant_id, "hb-worker", 5.0, 1))
            .expect("claim must succeed");

        let before: std::time::SystemTime = handle
            .admin
            .query_one(
                "SELECT lease_expires_at FROM ops.jobs WHERE job_id = $1",
                &[&job_id],
            )
            .expect("row must exist")
            .get(0);

        let extended = handle
            .rt
            .block_on(jobs::heartbeat(
                &handle.gateway,
                tenant_id,
                job_id,
                "hb-worker",
                claimed[0].attempt,
                600.0,
            ))
            .expect("heartbeat must not error");
        assert!(extended);

        let after: std::time::SystemTime = handle
            .admin
            .query_one(
                "SELECT lease_expires_at FROM ops.jobs WHERE job_id = $1",
                &[&job_id],
            )
            .expect("row must exist")
            .get(0);
        assert!(
            after > before,
            "heartbeat with a longer lease_seconds must push lease_expires_at further out \
             ({after:?} was not after {before:?})"
        );
    });
}

/// A late heartbeat from a worker whose lease was already reclaimed (simulated here by
/// `complete`-ing the job under a *different* owner first) must observably fail rather than
/// resurrect a lease it no longer holds.
#[test]
fn heartbeat_after_lease_lost_is_a_no_op() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<JobsFixture, _>("heartbeat_after_lease_lost_is_a_no_op", |mut handle| {
        let job_id = seed_pending(&mut handle, &format!("lost-lease-{}", Uuid::new_v4()));
        let tenant_id = handle.tenant_id;

        let claimed = handle
            .rt
            .block_on(jobs::claim(
                &handle.gateway,
                tenant_id,
                "original-owner",
                60.0,
                1,
            ))
            .expect("claim must succeed");

        // Simulate a reaper handing the lease to a new owner by directly overwriting it —
        // this file has no reaper implementation to call (out of this task's scope), only the
        // observable end state a reaper would produce.
        handle
            .admin
            .execute(
                "UPDATE ops.jobs SET lease_owner = 'new-owner' WHERE job_id = $1",
                &[&job_id],
            )
            .expect("simulate lease reassignment");

        let result = handle
            .rt
            .block_on(jobs::heartbeat(
                &handle.gateway,
                tenant_id,
                job_id,
                "original-owner",
                claimed[0].attempt,
                600.0,
            ))
            .expect("heartbeat must not itself error, just report no-op");
        assert!(
            !result,
            "a heartbeat from the original (now-superseded) owner must be a no-op, not silently \
             extend a lease it no longer holds"
        );
    });
}

/// `FOR UPDATE SKIP LOCKED` never blocks on an empty or fully-claimed table (unlike plain
/// `FOR UPDATE`) — asserts an empty-table claim returns `Ok(vec![])` well under a timeout that
/// would trip if it were blocking.
#[test]
fn claim_on_empty_table_returns_empty_without_blocking() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<JobsFixture, _>(
        "claim_on_empty_table_returns_empty_without_blocking",
        |handle| {
            let tenant_id = handle.tenant_id;
            let result = handle.rt.block_on(async {
                tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    jobs::claim(&handle.gateway, tenant_id, "empty-worker", 60.0, 5),
                )
                .await
            });
            let claimed = result
                .expect("claim on an empty table must not block/hang")
                .expect("claim must not error");
            assert!(
                claimed.is_empty(),
                "no PENDING/RETRY_WAIT rows exist for this tenant"
            );
        },
    );
}

/// `attempt` is the sole monotonic fencing token: even the same owner cannot finish a job with
/// its first claim after a reaper has made it claimable and it has been claimed again.
#[test]
fn same_owner_reclaim_rejects_old_attempt() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<JobsFixture, _>("same_owner_reclaim_rejects_old_attempt", |mut handle| {
        let job_id = seed_pending(&mut handle, &format!("fence-reclaim-{}", Uuid::new_v4()));
        let tenant_id = handle.tenant_id;
        let first = handle
            .rt
            .block_on(jobs::claim(
                &handle.gateway,
                tenant_id,
                "same-owner",
                60.0,
                1,
            ))
            .expect("first claim")
            .remove(0);
        handle
            .admin
            .execute(
                "UPDATE ops.jobs SET status = 'PENDING', lease_owner = NULL, \
                 lease_expires_at = NULL, next_retry_at = clock_timestamp() WHERE job_id = $1",
                &[&job_id],
            )
            .expect("simulate reaper requeue");
        let second = handle
            .rt
            .block_on(jobs::claim(
                &handle.gateway,
                tenant_id,
                "same-owner",
                60.0,
                1,
            ))
            .expect("second claim")
            .remove(0);
        assert_eq!(second.attempt, first.attempt + 1);
        assert!(
            !handle
                .rt
                .block_on(jobs::complete(
                    &handle.gateway,
                    tenant_id,
                    job_id,
                    "same-owner",
                    first.attempt,
                ))
                .expect("stale completion query"),
            "same owner with an old attempt must not complete the reclaimed lease"
        );
        assert!(
            handle
                .rt
                .block_on(jobs::complete(
                    &handle.gateway,
                    tenant_id,
                    job_id,
                    "same-owner",
                    second.attempt,
                ))
                .expect("current completion query")
        );
    });
}

/// Every lease-sensitive transition rejects an expired lease, even when owner and attempt still
/// match. This uses PostgreSQL's clock, so no timing sleep manufactures the expiry ordering.
#[test]
fn expired_lease_rejects_sensitive_transitions() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<JobsFixture, _>(
        "expired_lease_rejects_sensitive_transitions",
        |mut handle| {
            let job_id = seed_pending(&mut handle, &format!("expired-{}", Uuid::new_v4()));
            let tenant_id = handle.tenant_id;
            let claimed = handle
                .rt
                .block_on(jobs::claim(
                    &handle.gateway,
                    tenant_id,
                    "expired-owner",
                    60.0,
                    1,
                ))
                .expect("claim")
                .remove(0);
            handle
            .admin
            .execute(
                "UPDATE ops.jobs SET lease_expires_at = clock_timestamp() - interval '1 second' \
                 WHERE job_id = $1",
                &[&job_id],
            )
            .expect("expire lease");
            assert!(
                !handle
                    .rt
                    .block_on(jobs::heartbeat(
                        &handle.gateway,
                        tenant_id,
                        job_id,
                        "expired-owner",
                        claimed.attempt,
                        60.0,
                    ))
                    .expect("expired heartbeat query")
            );
            assert_eq!(
                handle
                    .rt
                    .block_on(jobs::fail(
                        &handle.gateway,
                        tenant_id,
                        FailInput {
                            job_id,
                            lease_owner: "expired-owner",
                            attempt: claimed.attempt,
                            error_class: "expired",
                            retryable: false,
                            max_attempts: 1,
                            retry_after_seconds: 1.0,
                        },
                    ))
                    .expect("expired fail query"),
                None,
            );
            assert!(
                !handle
                    .rt
                    .block_on(jobs::mark_waiting_key(
                        &handle.gateway,
                        tenant_id,
                        job_id,
                        "expired-owner",
                        claimed.attempt,
                    ))
                    .expect("expired wait query")
            );
            assert!(
                !handle
                    .rt
                    .block_on(jobs::complete(
                        &handle.gateway,
                        tenant_id,
                        job_id,
                        "expired-owner",
                        claimed.attempt,
                    ))
                    .expect("expired complete query")
            );
            assert_eq!(job_status(&mut handle, job_id), "PROCESSING");
        },
    );
}

/// A stale token cannot heartbeat, fail, or complete a still-live lease.
#[test]
fn stale_attempt_rejects_heartbeat_fail_and_complete() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<JobsFixture, _>(
        "stale_attempt_rejects_heartbeat_fail_and_complete",
        |mut handle| {
            let job_id = seed_pending(&mut handle, &format!("stale-{}", Uuid::new_v4()));
            let tenant_id = handle.tenant_id;
            let claimed = handle
                .rt
                .block_on(jobs::claim(
                    &handle.gateway,
                    tenant_id,
                    "stale-owner",
                    60.0,
                    1,
                ))
                .expect("claim")
                .remove(0);
            let stale_attempt = claimed.attempt - 1;
            assert!(
                !handle
                    .rt
                    .block_on(jobs::heartbeat(
                        &handle.gateway,
                        tenant_id,
                        job_id,
                        "stale-owner",
                        stale_attempt,
                        60.0,
                    ))
                    .expect("stale heartbeat query")
            );
            assert_eq!(
                handle
                    .rt
                    .block_on(jobs::fail(
                        &handle.gateway,
                        tenant_id,
                        FailInput {
                            job_id,
                            lease_owner: "stale-owner",
                            attempt: stale_attempt,
                            error_class: "stale",
                            retryable: false,
                            max_attempts: 1,
                            retry_after_seconds: 1.0,
                        },
                    ))
                    .expect("stale fail query"),
                None,
            );
            assert!(
                !handle
                    .rt
                    .block_on(jobs::complete(
                        &handle.gateway,
                        tenant_id,
                        job_id,
                        "stale-owner",
                        stale_attempt,
                    ))
                    .expect("stale complete query")
            );
            assert!(
                handle
                    .rt
                    .block_on(jobs::complete(
                        &handle.gateway,
                        tenant_id,
                        job_id,
                        "stale-owner",
                        claimed.attempt,
                    ))
                    .expect("current complete query")
            );
        },
    );
}

/// Generic workers cannot steal any `PUBLIC_` job, and the private worker claims only the
/// contribution execution kind. Public dispatch ownership stays in `public_repo`, whose
/// SECURITY DEFINER entry points are tested by the public-runtime suite.
#[test]
fn generic_and_private_claims_preserve_public_boundary() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<JobsFixture, _>(
        "generic_and_private_claims_preserve_public_boundary",
        |mut handle| {
            let tenant_id = handle.tenant_id;
            let generic = seed_pending(&mut handle, &format!("generic-{}", Uuid::new_v4()));
            let contribution = seed_pending_with_type(
                &mut handle,
                "CONTRIBUTION_EXECUTE",
                &format!("contribution-{}", Uuid::new_v4()),
            );
            let mut public_ids = Vec::new();
            // The tenant-bearing queue has exactly these three public kinds. Anonymous
            // release/revoke work lives in ops.public_anonymous_dispatches and is exercised by
            // public_runtime; seeding it into ops.jobs would blur the 0123 boundary this test
            // protects.
            for kind in [
                "PUBLIC_RELEASE_APPLY",
                "PUBLIC_REVOKE_APPLY",
                "PUBLIC_PROJECT",
            ] {
                public_ids.push(seed_pending_with_type(
                    &mut handle,
                    kind,
                    &format!("{kind}-{}", Uuid::new_v4()),
                ));
            }
            let deferred = seed_pending_with_type(
                &mut handle,
                "PUBLIC_SYNTHESIS_REBUILD",
                &format!("deferred-{}", Uuid::new_v4()),
            );
            let generic_claimed = handle
                .rt
                .block_on(jobs::claim(&handle.gateway, tenant_id, "generic", 60.0, 8))
                .expect("generic claim");
            assert_eq!(
                generic_claimed.iter().map(|j| j.job_id).collect::<Vec<_>>(),
                vec![generic]
            );
            assert_eq!(job_status(&mut handle, contribution), "PENDING");
            // Seed after the generic claim: this row is a negative control for the exact
            // contribution-only private claim, not a claim about the existing generic scope.
            let private_other = seed_pending_with_type(
                &mut handle,
                "PRIVATE_OTHER",
                &format!("private-other-{}", Uuid::new_v4()),
            );
            let contribution_claimed = handle
                .rt
                .block_on(jobs::private_claim(
                    &handle.private,
                    tenant_id,
                    "private",
                    60.0,
                    8,
                ))
                .expect("private contribution claim");
            assert_eq!(
                contribution_claimed
                    .iter()
                    .map(|j| j.job_id)
                    .collect::<Vec<_>>(),
                vec![contribution]
            );
            assert_eq!(contribution_claimed[0].job_type, "CONTRIBUTION_EXECUTE");
            assert_eq!(contribution_claimed[0].attempt, 1);
            assert!(contribution_claimed[0].lease_expires_at.is_some());
            assert_eq!(job_status(&mut handle, private_other), "PENDING");
            for public_id in public_ids {
                assert_eq!(job_status(&mut handle, public_id), "PENDING");
            }
            assert_eq!(job_status(&mut handle, deferred), "PENDING");
        },
    );
}

// ----------------------------------------------------------------------------
// Card 21 (folded card-16 debt): a derived-work pass that outlasts its own lease.
//
// 2026-09-10 over-capacity soak: `lost_lease=6`. One `dispatch_pass` claimed 8 jobs / 77
// evidence rows and worked them SERIALLY, each with a real provider round trip, while the lease
// was renewed exactly once — before the first row. Lease = `LEASE_SECS` (120 s); work =
// `rows × provider_latency`. Those two numbers were never related to each other, so the lease
// expired mid-pass, another dispatcher re-claimed the job (`ops.claim_derived_work`'s
// `status='PROCESSING' AND lease_expires_at < clock_timestamp()` arm), and the provider budget
// was spent twice. `distill::run_once` then renewed per row; since ADR-0058 distill runs on the
// v2 slots (its own heartbeat per job, `distill_dispatch_v2.rs` T6) and these two tests pin the v1
// heartbeat/settle shape the consolidation worker still uses (`DERIVED_CONSOLIDATE`).
//
// Scoped entirely to this file's throwaway tenant: the competing claim is SIMULATED with an
// admin UPDATE on this job (the same technique `heartbeat_after_lease_lost_is_a_no_op` uses)
// rather than by calling the real cross-tenant `ops.claim_derived_work`, which would bump
// `attempt` on unrelated tenants' rows in the shared dev database.
// ----------------------------------------------------------------------------

/// Seeds one `DERIVED_CONSOLIDATE` job already `PROCESSING` under `owner`, with `lease_seconds`
/// left to run, and returns the lease a claim would have handed back.
fn seed_leased_derived_job(handle: &mut Handle, owner: &str, lease_seconds: f64) -> Uuid {
    handle
        .admin
        .query_one(
            "INSERT INTO ops.jobs \
               (tenant_id, job_type, idempotency_key, next_retry_at, status, attempt, \
                lease_owner, lease_expires_at) \
             VALUES ($1, $2, $3, now(), 'PROCESSING', 1, $4, \
                     clock_timestamp() + make_interval(secs => $5)) \
             RETURNING job_id",
            &[
                &handle.tenant_id,
                &jobs::DerivedJobType::Consolidate.as_db_str(),
                &format!("card21-{owner}-{}", Uuid::new_v4()),
                &owner,
                &lease_seconds,
            ],
        )
        .expect("seed leased DERIVED_CONSOLIDATE job")
        .get(0)
}

/// Exactly `ops.claim_derived_work`'s re-claim eligibility arm for a `PROCESSING` row, evaluated
/// in the database rather than restated in Rust — so this test cannot drift away from the
/// predicate that actually decides whether another dispatcher steals the job.
fn reclaimable_by_another_dispatcher(handle: &mut Handle, job_id: Uuid) -> bool {
    handle
        .admin
        .query_one(
            "SELECT status = 'PROCESSING' AND lease_expires_at < clock_timestamp() \
             FROM ops.jobs WHERE job_id = $1",
            &[&job_id],
        )
        .expect("job must exist")
        .get(0)
}

/// The fix. Four "rows" of 400 ms each — 1.6 s of work against a 1 s lease — with the lease
/// renewed before each row. The job is never eligible for re-claim, and the settle at the end
/// still owns it (`lost_lease` stays 0).
#[test]
fn a_derived_pass_longer_than_its_lease_heartbeats_per_row_and_settles() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<JobsFixture, _>(
        "a_derived_pass_longer_than_its_lease_heartbeats_per_row_and_settles",
        |mut handle| {
            let owner = format!("card21-hb-{}", Uuid::new_v4());
            let job_id = seed_leased_derived_job(&mut handle, &owner, 1.0);
            let lease = jobs::DerivedLease {
                tenant_id: handle.tenant_id,
                job_id,
                lease_owner: &owner,
                attempt: 1,
            };
            for row in 0..4 {
                let alive = handle
                    .rt
                    .block_on(jobs::heartbeat_derived_consolidation(
                        &handle.consolidation,
                        &lease,
                        1.0,
                    ))
                    .expect("heartbeat query must not error");
                assert!(alive, "lease must still be held at row {row}");
                std::thread::sleep(std::time::Duration::from_millis(400));
            }
            assert!(
                !reclaimable_by_another_dispatcher(&mut handle, job_id),
                "after 1.6 s of work on a 1 s lease, a per-row heartbeat must keep the job out \
                 of ops.claim_derived_work's re-claim arm"
            );
            let settled = handle
                .rt
                .block_on(jobs::settle_derived_consolidation(
                    &handle.consolidation,
                    &lease,
                    jobs::DerivedWorkOutcome::Done,
                    1.0,
                ))
                .expect("settle query must not error");
            assert!(settled, "lost_lease must be 0 for this pass");
            assert_eq!(job_status(&mut handle, job_id), "DONE");
        },
    );
}

/// The negative control — the card-16 shape. Same work, one heartbeat before the first row (what
/// `dispatch_pass` did on its own), then the lease expires, another dispatcher re-claims, and the
/// original worker's settle writes nothing: that `false` is the `report.lost_lease += 1` branch.
///
/// Without this test the one above proves nothing — it would pass on a 10 s lease too.
#[test]
fn the_same_derived_pass_without_per_row_heartbeats_loses_its_lease() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<JobsFixture, _>(
        "the_same_derived_pass_without_per_row_heartbeats_loses_its_lease",
        |mut handle| {
            let owner = format!("card21-nohb-{}", Uuid::new_v4());
            let job_id = seed_leased_derived_job(&mut handle, &owner, 1.0);
            let lease = jobs::DerivedLease {
                tenant_id: handle.tenant_id,
                job_id,
                lease_owner: &owner,
                attempt: 1,
            };
            // One heartbeat before the first row, then 1.6 s of serial work with none.
            assert!(
                handle
                    .rt
                    .block_on(jobs::heartbeat_derived_consolidation(
                        &handle.consolidation,
                        &lease,
                        1.0
                    ))
                    .expect("heartbeat query must not error")
            );
            std::thread::sleep(std::time::Duration::from_millis(1600));
            assert!(
                reclaimable_by_another_dispatcher(&mut handle, job_id),
                "the lease must be expired — this is the condition the soak hit"
            );
            // The competing dispatcher, simulated on this tenant's own row only.
            handle
                .admin
                .execute(
                    "UPDATE ops.jobs SET lease_owner = $2, attempt = attempt + 1, \
                            lease_expires_at = clock_timestamp() + interval '120 seconds' \
                     WHERE job_id = $1",
                    &[&job_id, &"card21-other-dispatcher"],
                )
                .expect("simulate the re-claim");
            let settled = handle
                .rt
                .block_on(jobs::settle_derived_consolidation(
                    &handle.consolidation,
                    &lease,
                    jobs::DerivedWorkOutcome::Done,
                    1.0,
                ))
                .expect("settle query must not error");
            assert!(
                !settled,
                "a re-claimed job must settle nothing for the original worker — this is \
                 lost_lease=6"
            );
            assert_eq!(
                job_status(&mut handle, job_id),
                "PROCESSING",
                "the job belongs to the new dispatcher now; the provider spend is redone"
            );
        },
    );
}
