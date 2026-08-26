//! T3.5 integration test — `jobs` (§31/§61) against a real Postgres. Runs on
//! `migrations/0008_ops_core.sql`'s real `ops.jobs` table (same convention as
//! `email_outbox.rs`: the table is shared, not a scratch schema, so every test scopes rows to
//! a throwaway `control.tenants` row this file owns and cleans up on drop).
//!
//! Three-state skip (§79.2): no DSN, unreachable DB, or `ops.jobs` missing all print a visible
//! SKIP and return.

use std::sync::Mutex;

use humaux_adapters::jobs::{self, FailInput, JobStatus};
use humaux_adapters::postgres::RuntimeDbPool;
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
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM ops.jobs WHERE tenant_id = '{0}'; \
             DELETE FROM control.tenants WHERE tenant_id = '{0}';",
            self.tenant_id
        ));
    }
}

struct JobsFixture;

impl DbIntegrationFixture for JobsFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
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
            .block_on(RuntimeDbPool::connect(&gateway_dsn))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        Ok(Handle {
            rt,
            gateway,
            admin,
            tenant_id,
            gateway_dsn,
        })
    }
}

/// Seeds one `PENDING` `ops.jobs` row, claimable immediately (`next_retry_at = now()` — see
/// `jobs.rs`'s module doc on why this must be set explicitly). Returns its `job_id`.
fn seed_pending(handle: &mut Handle, idempotency_key: &str) -> Uuid {
    handle
        .admin
        .query_one(
            "INSERT INTO ops.jobs (tenant_id, job_type, idempotency_key, next_retry_at) \
             VALUES ($1, 'test.noop', $2, now()) RETURNING job_id",
            &[&handle.tenant_id, &idempotency_key],
        )
        .expect("seed PENDING job")
        .get(0)
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

            handle
                .rt
                .block_on(jobs::claim(
                    &handle.gateway,
                    tenant_id,
                    "dead-worker",
                    60.0,
                    1,
                ))
                .expect("claim must succeed");

            let outcome = handle
                .rt
                .block_on(jobs::fail(
                    &handle.gateway,
                    tenant_id,
                    FailInput {
                        job_id,
                        lease_owner: "dead-worker",
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

            handle
                .rt
                .block_on(jobs::claim(
                    &handle.gateway,
                    tenant_id,
                    "failed-worker",
                    60.0,
                    1,
                ))
                .expect("claim must succeed");

            let outcome = handle
                .rt
                .block_on(jobs::fail(
                    &handle.gateway,
                    tenant_id,
                    FailInput {
                        job_id,
                        lease_owner: "failed-worker",
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

        handle
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

        handle
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

        handle
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
