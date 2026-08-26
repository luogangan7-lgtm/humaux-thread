//! T3.6+T3.7 integration test — §32.1 Scheduler Singleton/Failover Gate, G32-1/G80-38
//! Scheduler Exactly-once Enqueue. **This file is G80-38's execution body**: §80.1 registers
//! G80-38 against `§32.1#G32-1` and the task brief is explicit that this test *is* that gate,
//! not a second independent one.
//!
//! Two fixtures, two concerns:
//!
//! - [`SchedulerFixture`] drives `humaux_adapters::scheduler::claim_and_enqueue` (the real
//!   production function) against the real `ops.scheduler_leases` / `ops.jobs`
//!   (`migrations/0008_ops_core.sql` + `0044_scheduler_jobs_idempotency_key.sql`), scoped by a
//!   throwaway `control.tenants` row and a randomly-suffixed `schedule_id` prefix — same
//!   convention as the sibling T3.5 `jobs_claim.rs` ("the table is shared, not a scratch
//!   schema"). Proves the literal G32-1 steps 1-5: 3 concurrent replicas racing the same due
//!   tick still enqueue exactly one job, and after the winning replica is killed the next tick
//!   still enqueues exactly one job.
//! - [`FaultFixture`] builds its own scratch schema (repo CLAUDE.md hard rule ④: fault
//!   injection that drops a constraint must never touch the shared `ops` schema other
//!   parallel agents' tests are also writing to) mirroring only the one constraint under test,
//!   and proves the two named fault injections turn the mechanism red: removing the
//!   `UNIQUE(idempotency_key)` constraint, and computing the key from `schedule_id` alone.
//!
//! Three-state skip (§79.2): no DSN, unreachable DB, `ops.jobs`/`ops.scheduler_leases`
//! missing, or the `ops_jobs_idempotency_key_key` index (0044) missing all print a visible
//! SKIP and return.

use std::sync::{Barrier, Mutex};

use humaux_adapters::postgres::RuntimeDbPool;
use humaux_adapters::scheduler::{self, DueSchedule, EnqueueOutcome};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

/// Serializes every test in this file — mirrors `jobs_claim.rs`'s `SERIAL_GUARD`. Beyond that
/// file's reason (noisy timing), [`FaultFixture`] additionally `DROP`/`CREATE SCHEMA`s a fixed
/// name per test; running two tests in this file truly concurrently would race each other's
/// schema teardown/setup, not just be slow.
static SERIAL_GUARD: Mutex<()> = Mutex::new(());

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    // libpq-standard `options=-c role=X` (URL-encoded). The older `options[role]=X` form
    // is silently tolerated by sqlx but rejected outright by rust-postgres ("invalid
    // connection string") — which made every `Client::connect` role fixture skip, and a
    // skip is not a pass (§79.2). This form is verified working on both drivers.
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

// ---------------------------------------------------------------------------------------
// SchedulerFixture — real ops.scheduler_leases / ops.jobs, production claim_and_enqueue.
// ---------------------------------------------------------------------------------------

struct SchedulerHandle {
    admin: Client,
    tenant_id: Uuid,
    gateway_dsn: String,
    /// Every `schedule_id` this test minted (for Drop cleanup) — a fresh UUID-suffixed value
    /// per test run so concurrent runs of this same test (or other agents' unrelated tests
    /// against the same shared dev DB) can never collide on `(schedule_id, planned_at)`.
    schedule_id_prefix: String,
}

impl Drop for SchedulerHandle {
    fn drop(&mut self) {
        // Best-effort cleanup (repo CLAUDE.md hard rule ④): only rows this file created,
        // scoped by its own throwaway tenant and its own schedule_id prefix — the real `ops`
        // schema itself is never altered here.
        let _ = self.admin.execute(
            "DELETE FROM ops.jobs WHERE tenant_id = $1",
            &[&self.tenant_id],
        );
        let _ = self.admin.execute(
            "DELETE FROM ops.scheduler_leases WHERE schedule_id LIKE $1",
            &[&format!("{}%", self.schedule_id_prefix)],
        );
        let _ = self.admin.execute(
            "DELETE FROM control.tenants WHERE tenant_id = $1",
            &[&self.tenant_id],
        );
    }
}

struct SchedulerFixture;

impl DbIntegrationFixture for SchedulerFixture {
    type Handle = SchedulerHandle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        let jobs_ready: bool = admin
            .query_one(
                "SELECT to_regclass('ops.jobs') IS NOT NULL \
                     AND to_regclass('ops.scheduler_leases') IS NOT NULL \
                     AND to_regclass('ops.ops_jobs_idempotency_key_key') IS NOT NULL",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !jobs_ready {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "ops.jobs / ops.scheduler_leases / ops_jobs_idempotency_key_key missing — run \
                 `cargo xtask migrate` (0008_ops_core.sql, 0044_scheduler_jobs_idempotency_key.sql) \
                 against HUMAUX_TEST_PG_DSN first"
                    .to_string(),
            ));
        }

        let tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"scheduler_exactly_once.rs throwaway tenant"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        let gateway_dsn = dsn_as_role(&dsn, "role_gateway");
        let schedule_id_prefix = format!("t3_6_g32_1_{}_", Uuid::new_v4());

        Ok(SchedulerHandle {
            admin,
            tenant_id,
            gateway_dsn,
            schedule_id_prefix,
        })
    }
}

fn jobs_count_for_key(admin: &mut Client, key: &str) -> i64 {
    admin
        .query_one(
            "SELECT count(*) FROM ops.jobs WHERE idempotency_key = $1",
            &[&key],
        )
        .expect("count query must not error")
        .get(0)
}

/// Races `claim_and_enqueue` for the same `due` tick from every owner in `owners`,
/// synchronized on a [`Barrier`] so all racers reach their `INSERT`s at once (memory lesson:
/// a `sleep`-based race window is flaky — a negative control that never actually manifests the
/// race passes even without the fix under test; `Barrier` makes "everyone is ready" a
/// deterministic precondition instead of a timing guess). Each owner opens its own
/// `RuntimeDbPool` — mirrors independent scheduler replica processes, same rationale as
/// `jobs_claim.rs`'s `gateway_dsn` field doc.
fn race_claim_and_enqueue(
    gateway_dsn: &str,
    owners: &[&str],
    due: &DueSchedule,
) -> Vec<Option<EnqueueOutcome>> {
    let barrier = Barrier::new(owners.len());
    std::thread::scope(|scope| {
        let handles: Vec<_> = owners
            .iter()
            .map(|&owner| {
                let dsn = gateway_dsn.to_string();
                let due = due.clone();
                let barrier = &barrier;
                scope.spawn(move || {
                    let rt = tokio::runtime::Runtime::new().expect("tokio runtime per thread");
                    rt.block_on(async {
                        let pool = RuntimeDbPool::connect(&dsn)
                            .await
                            .unwrap_or_else(|e| panic!("connect for {owner} must succeed: {e}"));
                        barrier.wait();
                        scheduler::claim_and_enqueue(&pool, owner, 30, &due)
                            .await
                            .unwrap_or_else(|e| {
                                panic!("claim_and_enqueue by {owner} must not error: {e}")
                            })
                    })
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("racer thread must not panic"))
            .collect()
    })
}

/// G32-1 steps 1-5 verbatim: 3 concurrent scheduler replicas racing the same due tick still
/// produce exactly one `ops.jobs` row for `(schedule_id, planned_at)`; after the replica that
/// won round 1 is killed (simply excluded from round 2 — leadership here is claimed per-tick,
/// not held across ticks, see `adapters::scheduler`'s module doc), the next tick — raced by
/// only the two survivors — still produces exactly one row.
#[test]
fn g32_1_exactly_once_enqueue_survives_concurrent_replicas_and_leader_failover() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<SchedulerFixture, _>(
        "g32_1_exactly_once_enqueue_survives_concurrent_replicas_and_leader_failover",
        |mut handle| {
            let schedule_id = format!("{}sched", handle.schedule_id_prefix);
            let t1 = OffsetDateTime::now_utc();
            let due1 = DueSchedule {
                schedule_id: schedule_id.clone(),
                planned_at: t1,
                tenant_id: handle.tenant_id,
                job_type: "scheduler.g32_1_probe".to_string(),
                payload: serde_json::json!({"tick": "t1"}),
            };

            let owners = ["replica-a", "replica-b", "replica-c"];
            let round1 = race_claim_and_enqueue(&handle.gateway_dsn, &owners, &due1);
            let round1_enqueued = round1
                .iter()
                .filter(|o| matches!(o, Some(EnqueueOutcome::Enqueued { .. })))
                .count();
            assert_eq!(
                round1_enqueued, 1,
                "exactly one of 3 concurrent replicas must have both won the lease and \
                 inserted the job for the same due tick; got outcomes {round1:?}"
            );

            let key1 = scheduler::idempotency_key(&schedule_id, t1);
            assert_eq!(
                jobs_count_for_key(&mut handle.admin, &key1),
                1,
                "§32.1 step 3: (schedule_id, planned_at) must map to exactly 1 ops.jobs row"
            );

            // "Kill the current leader": find who won round 1's lease and simply never call
            // claim_and_enqueue from that owner again — no process to actually terminate,
            // since leadership is claimed fresh per tick (module doc), a dead replica just
            // stops participating in future rounds.
            let winner: String = handle
                .admin
                .query_one(
                    "SELECT leader_owner FROM ops.scheduler_leases WHERE schedule_id = $1",
                    &[&schedule_id],
                )
                .expect("exactly one scheduler_leases row must exist for this schedule_id")
                .get(0);
            let survivors: Vec<&str> = owners.iter().copied().filter(|o| *o != winner).collect();
            assert_eq!(
                survivors.len(),
                2,
                "exactly one of the three named owners must have won round 1's lease claim"
            );

            let t2 = OffsetDateTime::from_unix_timestamp(t1.unix_timestamp() + 3600)
                .expect("t1 + 1h is a valid unix timestamp");
            let due2 = DueSchedule {
                schedule_id: schedule_id.clone(),
                planned_at: t2,
                tenant_id: handle.tenant_id,
                job_type: due1.job_type.clone(),
                payload: serde_json::json!({"tick": "t2"}),
            };

            let round2 = race_claim_and_enqueue(&handle.gateway_dsn, &survivors, &due2);
            let round2_enqueued = round2
                .iter()
                .filter(|o| matches!(o, Some(EnqueueOutcome::Enqueued { .. })))
                .count();
            assert_eq!(
                round2_enqueued, 1,
                "§32.1 step 5: after the round-1 leader is gone, the next due tick raced by \
                 the 2 survivors must still enqueue exactly once; got outcomes {round2:?}"
            );

            let key2 = scheduler::idempotency_key(&schedule_id, t2);
            assert_eq!(
                jobs_count_for_key(&mut handle.admin, &key2),
                1,
                "§32.1 step 5: (schedule_id, t2) must map to exactly 1 ops.jobs row even \
                 though the winner of t1's round is no longer participating"
            );
        },
    );
}

// ---------------------------------------------------------------------------------------
// FaultFixture — scratch schema, safe to mutilate. Proves the two named G32-1 fault
// injections (§32.1: "移除 UNIQUE 或改 idempotency key 不含 planned_at/schedule_id ⇒
// 第一或第二周期红") actually turn the mechanism red, so the green result above is not a
// tautology.
// ---------------------------------------------------------------------------------------

const FAULT_SCHEMA: &str = "test_scheduler_g32_1_fault_injection";

struct FaultHandle {
    client: Client,
}

impl Drop for FaultHandle {
    fn drop(&mut self) {
        let _ = self
            .client
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {FAULT_SCHEMA} CASCADE;"));
    }
}

struct FaultFixture;

impl DbIntegrationFixture for FaultFixture {
    type Handle = FaultHandle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut client = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        // Minimal mirror of ops.jobs' single column that matters here: idempotency_key, with
        // the *named* UNIQUE constraint the "remove UNIQUE" fault injection later drops.
        // Nothing else about ops.jobs' real shape (tenant FK, RLS, status CHECK, ...) is
        // relevant to the property under test.
        client
            .batch_execute(&format!(
                "DROP SCHEMA IF EXISTS {FAULT_SCHEMA} CASCADE;
                 CREATE SCHEMA {FAULT_SCHEMA};
                 CREATE TABLE {FAULT_SCHEMA}.jobs (
                     job_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                     idempotency_key text NOT NULL,
                     CONSTRAINT jobs_idempotency_key_key UNIQUE (idempotency_key)
                 );"
            ))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        Ok(FaultHandle { client })
    }
}

/// One `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING` attempt — the exact statement
/// shape `adapters::scheduler::insert_job_idempotent` runs against `ops.jobs`, run here
/// against the scratch table so the constraint can be safely dropped mid-test. Returns
/// `Ok(true)` if a row was inserted, `Ok(false)` if the conflict fired (suppressed, not an
/// error), `Err` for any real DB error (e.g. "no unique constraint matching" once the fault
/// below drops it).
fn try_insert(client: &mut Client, key: &str) -> Result<bool, postgres::Error> {
    let row = client.query_opt(
        &format!(
            "INSERT INTO {FAULT_SCHEMA}.jobs (idempotency_key) VALUES ($1) \
             ON CONFLICT (idempotency_key) DO NOTHING RETURNING job_id"
        ),
        &[&key],
    )?;
    Ok(row.is_some())
}

/// Fault injection 1 (§32.1 "移除 UNIQUE ... ⇒ ... 红"): with the constraint present, `N`
/// concurrent racers inserting the *same* `idempotency_key` (bypassing any lease claim
/// entirely — simulating a leader-election bug where more than one replica believes it won)
/// still produce exactly 1 row, because `ops_jobs_idempotency_key_key`
/// (`migrations/0044_scheduler_jobs_idempotency_key.sql`) is the final arbiter, not the lease.
/// Dropping that exact constraint and re-running the identical `INSERT ... ON CONFLICT
/// (idempotency_key)` statement does not silently let duplicates through — Postgres requires
/// an existing unique constraint/index matching the conflict target, so every racer's
/// statement fails outright with a real runtime error (`SQLSTATE 42P10`, not a parse error:
/// the statement parses and plans identically, it is the *catalog* that changed) once the
/// constraint is gone. That hard failure — not a duplicate-row count — is this test's red
/// signal, and it is a stronger proof than counting duplicates would be: it holds even for a
/// single racer, not only under a race window that got lucky.
#[test]
fn g32_1_fault_injection_unique_constraint_is_the_final_arbiter() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<FaultFixture, _>(
        "g32_1_fault_injection_unique_constraint_is_the_final_arbiter",
        |mut handle| {
            let dsn = std::env::var("HUMAUX_TEST_PG_DSN")
                .expect("isolate() already required this to be set");

            // Green: constraint present, 5 racers same key, barrier-synchronized.
            let barrier = Barrier::new(5);
            let key = "dup-key-green";
            let results: Vec<Result<bool, String>> = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..5)
                    .map(|_| {
                        let dsn = dsn.clone();
                        let barrier = &barrier;
                        scope.spawn(move || {
                            let mut c = Client::connect(&dsn, NoTls).expect("racer connect");
                            barrier.wait();
                            try_insert(&mut c, key).map_err(|e| e.to_string())
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().expect("racer thread must not panic"))
                    .collect()
            });
            let inserted = results.iter().filter(|r| matches!(r, Ok(true))).count();
            assert_eq!(
                inserted, 1,
                "with the UNIQUE constraint present, exactly 1 of 5 concurrent same-key \
                 inserts must succeed; got {results:?}"
            );
            for r in &results {
                assert!(
                    r.is_ok(),
                    "with the constraint present every racer must either insert or be \
                     cleanly suppressed by ON CONFLICT, never error: {r:?}"
                );
            }

            // Red: drop the exact constraint the code path above relies on, re-run.
            handle
                .client
                .batch_execute(&format!(
                    "ALTER TABLE {FAULT_SCHEMA}.jobs DROP CONSTRAINT jobs_idempotency_key_key;"
                ))
                .expect("drop constraint (fault injection) must succeed");

            let mut post_fault = Client::connect(&dsn, NoTls).expect("post-fault connect");
            let err = try_insert(&mut post_fault, "dup-key-red").expect_err(
                "ON CONFLICT (idempotency_key) must fail once no unique constraint matches it",
            );
            // SQLSTATE, not string-matching `Display` — `postgres::Error`'s top-level
            // `Display` collapses to a generic "db error" (verified directly against this DB:
            // the human-readable detail only lives on the nested `DbError`), and SQLSTATE is
            // locale/wording-independent besides.
            let db_err = err
                .as_db_error()
                .expect("expected a Postgres DbError (not a connection-level error)");
            assert_eq!(
                *db_err.code(),
                postgres::error::SqlState::INVALID_COLUMN_REFERENCE,
                "expected SQLSTATE 42P10 (no unique/exclusion constraint matching ON CONFLICT) \
                 after dropping the constraint, got {:?}: {}",
                db_err.code(),
                db_err.message()
            );
        },
    );
}

/// Fault injection 2 (§32.1 "idempotency key 不含 planned_at/schedule_id ⇒ 第二周期红"): a
/// key formula that ignores `planned_at` makes two genuinely different due ticks of the same
/// schedule collide. The constraint stays intact throughout — the bug is in what gets hashed,
/// not in the DB — so `ON CONFLICT (idempotency_key) DO NOTHING` does exactly what it is
/// supposed to do and *wrongly* suppresses the second tick's legitimate enqueue (0 rows for
/// tick 2, not 1: an under-enqueue, the "第二周期红" the spec names). The real
/// `scheduler::idempotency_key` (hashes both `schedule_id` and `planned_at`, verified at
/// nanosecond precision by the unit tests in `adapters::scheduler`) does not have this bug:
/// both ticks enqueue independently.
#[test]
fn g32_1_fault_injection_idempotency_key_must_include_planned_at() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<FaultFixture, _>(
        "g32_1_fault_injection_idempotency_key_must_include_planned_at",
        |mut handle| {
            let schedule_id = "daily-digest";
            let t1 = OffsetDateTime::now_utc();
            let t2 = OffsetDateTime::from_unix_timestamp(t1.unix_timestamp() + 3600)
                .expect("t1 + 1h is a valid unix timestamp");

            // Red: broken formula only hashes schedule_id, so tick1 and tick2 collide.
            fn broken_key_omits_planned_at(schedule_id: &str) -> String {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut h = DefaultHasher::new();
                schedule_id.hash(&mut h);
                format!("{:016x}", h.finish())
            }
            let broken_key = broken_key_omits_planned_at(schedule_id);
            let tick1_inserted =
                try_insert(&mut handle.client, &broken_key).expect("tick1 insert must not error");
            let tick2_inserted =
                try_insert(&mut handle.client, &broken_key).expect("tick2 insert must not error");
            assert!(tick1_inserted, "tick1's own enqueue must succeed");
            assert!(
                !tick2_inserted,
                "§32.1 red case: a key formula omitting planned_at makes tick2 collide with \
                 tick1's row and get wrongly suppressed by ON CONFLICT DO NOTHING — this \
                 assertion demonstrates that under-enqueue, not the fix"
            );

            // Green: the real formula includes planned_at, both ticks enqueue independently.
            let key1 = scheduler::idempotency_key(schedule_id, t1);
            let key2 = scheduler::idempotency_key(schedule_id, t2);
            assert_ne!(
                key1, key2,
                "sanity: the real formula must not collide for these inputs"
            );
            let real_tick1 =
                try_insert(&mut handle.client, &key1).expect("real-formula tick1 must not error");
            let real_tick2 =
                try_insert(&mut handle.client, &key2).expect("real-formula tick2 must not error");
            assert!(
                real_tick1 && real_tick2,
                "with the real formula both ticks must enqueue independently"
            );
        },
    );
}
