//! `adapters::tests::distill_dispatch_v2` — ADR-0058 T1–T23: the four distill dispatch definers (0190), the v1 claim's
//!   distill refusal and the cutover block (0193), the tenant budget (0196) and the capability CHECKs (0195) against
//!   the real PostgreSQL, driven through `adapters::jobs` as role_private_worker.
//! Depends-on: crates=[humaux-adapters, humaux-testkit, postgres, sqlx, tokio];
//!   services=[PostgreSQL(owner) r=[control.processor_models, control.reasoning_profiles,
//!   control.user_reasoning_profiles, ops.claim_derived_work_v2, ops.commit_seq_seq, ops.distill_calls,
//!   ops.distill_tenant_scheduler, ops.provider_arbiters] w=[control.reasoning_route_bindings, control.tenants,
//!   ops.distill_calls, ops.distill_tenant_scheduler, ops.jobs, ops.outbox, ops.provider_slots,
//!   private.evidence_objects] x=[ops.admit_distill_budget, ops.begin_call, ops.claim_derived_work,
//!   ops.claim_derived_work_v2, ops.finish_derived_work_v2],
//!   PostgreSQL(role_private_worker), PostgreSQL(role_consolidation_worker), PostgreSQL(role_gateway)];
//!   env=[CARGO_MANIFEST_DIR, HUMAUX_TEST_PG_DSN]; modules=[adapters::byok, adapters::jobs, adapters::postgres, humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [every scenario ends with I-SLOT (each PROCESSING distill job with a dispatch_state holds exactly one
//!   slot bound to its own generation); scheduler rows of tenants this file did not create are row-locked for the
//!   whole test, so the cross-tenant claim only ever serves this file's tenants; a fixture deletes its jobs (and
//!   unbinds their slots) in one printed batch and its tenants in a separate best-effort batch; a missing DB is a
//!   fixture error under HUMAUX_REQUIRE_DB=1, never a silent pass]
//! Spec: Baseline §31; §61; §67.2; §11; §79.2; ADR-0058
//!
//! Each test names, in its doc, the fault that turns it red (ADR-0058 records the red→green runs).

use std::sync::{Barrier, Mutex};

use humaux_adapters::byok::ReasoningCapability;
use humaux_adapters::jobs::{
    self, CallAdmission, DispatchState, DistillCallBudget, DistillClaim, DistillFinish,
    DistillLease, JobsError,
};
use humaux_adapters::postgres::PrivateWorkerDbPool;
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;

/// Every test claims from the ONE global slot set, so tests in this file never overlap.
static SERIAL_GUARD: Mutex<()> = Mutex::new(());

const LEASE: f64 = 30.0;
const HARD: f64 = 300.0;
const MIN_REMAINING: f64 = 10.0;
const PARK: f64 = 60.0;
/// A §72.3 budget the T1–T20 scenarios never reach (T21 sets its own).
const BUDGET: DistillCallBudget = DistillCallBudget {
    window_seconds: 60.0,
    max_calls: 10_000,
};

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

struct Handle {
    rt: tokio::runtime::Runtime,
    admin: Client,
    /// Holds `FOR UPDATE` on every scheduler row that existed before this test (foreign tenants):
    /// the claim takes tenants `FOR UPDATE OF t SKIP LOCKED`, so they are invisible to it.
    fence: Client,
    private: PrivateWorkerDbPool,
    dsn: String,
    tenants: Vec<Uuid>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        let _ = self.fence.batch_execute("ROLLBACK");
        let ids = self
            .tenants
            .iter()
            .map(|t| format!("'{t}'"))
            .collect::<Vec<_>>()
            .join(",");
        if ids.is_empty() {
            return;
        }
        // Card-31 lesson: jobs (and the slots they hold) go in ONE batch whose failure is printed;
        // the tenant row (referenced by append-only audit rows) goes in a separate best-effort batch.
        if let Err(e) = self.admin.batch_execute(&format!(
            "UPDATE ops.provider_slots SET job_id = NULL, claim_generation = NULL, bound_until = NULL \
               WHERE job_id IN (SELECT job_id FROM ops.jobs WHERE tenant_id IN ({ids})); \
             DELETE FROM ops.jobs WHERE tenant_id IN ({ids});"
        )) {
            eprintln!("distill_dispatch_v2 cleanup: jobs/slots batch failed: {e}");
        }
        if let Err(e) = self.admin.batch_execute(&format!(
            "DELETE FROM control.tenants WHERE tenant_id IN ({ids});"
        )) {
            eprintln!("distill_dispatch_v2 cleanup: tenant batch failed (best effort): {e}");
        }
    }
}

struct Fixture;

impl DbIntegrationFixture for Fixture {
    type Handle = Handle;

    fn isolate() -> Result<Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let setup = |e: postgres::Error| DbFixtureSkipReason::IsolationSetupFailed(e.to_string());
        // dep: PostgreSQL(owner) — admin connection for seeding, inspection and cleanup
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
        let migrated: bool = admin
            .query_one(
                "SELECT to_regprocedure('ops.claim_derived_work_v2(text,double precision,double precision)') IS NOT NULL",
                &[],
            )
            .map_err(setup)?
            .get(0);
        if !migrated {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "ops.claim_derived_work_v2 is missing — run `cargo xtask migrate` (0190)".into(),
            ));
        }
        // A slot bound to a job that is gone or no longer PROCESSING under that generation is a
        // leftover of an aborted run (T9 would heal it after bound_until); free it now. A slot bound
        // to a live claim means a v2 worker is running against this DB: refuse.
        admin
            .batch_execute(
                "UPDATE ops.provider_slots s SET job_id = NULL, claim_generation = NULL, bound_until = NULL \
                 WHERE s.job_id IS NOT NULL AND NOT EXISTS ( \
                   SELECT 1 FROM ops.jobs j WHERE j.job_id = s.job_id AND j.status = 'PROCESSING' \
                     AND j.claim_generation = s.claim_generation)",
            )
            .map_err(setup)?;
        let bound: i64 = admin
            .query_one(
                "SELECT count(*) FROM ops.provider_slots WHERE job_id IS NOT NULL",
                &[],
            )
            .map_err(setup)?
            .get(0);
        if bound != 0 {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(format!(
                "{bound} provider slot(s) are bound to live claims — stop every v2 private-worker first"
            )));
        }
        // dep: PostgreSQL(owner) — fence connection holding foreign scheduler rows FOR UPDATE
        let mut fence = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
        fence
            .batch_execute("BEGIN; SELECT tenant_id FROM ops.distill_tenant_scheduler FOR UPDATE;")
            .map_err(setup)?;
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let private = rt
            // dep: PostgreSQL(role_private_worker) — role-scoped pool call
            .block_on(PrivateWorkerDbPool::connect(&dsn_as_role(
                &dsn,
                "role_private_worker",
            )))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        Ok(Handle {
            rt,
            admin,
            fence,
            private,
            dsn,
            tenants: Vec::new(),
        })
    }
}

impl Handle {
    fn tenant(&mut self) -> Uuid {
        let id: Uuid = self
            .admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ('distill_dispatch_v2.rs throwaway tenant') \
                 RETURNING tenant_id",
                &[],
            )
            .expect("seed tenant")
            .get(0);
        self.tenants.push(id);
        id
    }

    /// One READY `DERIVED_DISTILL` job created `age_secs` ago (FIFO order inside a tenant).
    fn seed(&mut self, tenant: Uuid, age_secs: f64) -> Uuid {
        self.admin
            .query_one(
                "INSERT INTO ops.jobs (tenant_id, job_type, status, next_retry_at, idempotency_key, payload, created_at) \
                 VALUES ($1, 'DERIVED_DISTILL', 'PENDING', clock_timestamp() - interval '1 second', \
                         'c32-dispatch-v2:' || gen_random_uuid()::text, '{}'::jsonb, \
                         clock_timestamp() - make_interval(secs => $2)) \
                 RETURNING job_id",
                &[&tenant, &age_secs],
            )
            .expect("seed distill job")
            .get(0)
    }

    fn claim(&self, owner: &str) -> Option<DistillClaim> {
        self.rt
            .block_on(jobs::claim_distill(&self.private, owner, LEASE, HARD))
            .unwrap_or_else(|e| panic!("claim by {owner} must not error: {e}"))
    }

    fn begin(&self, lease: &DistillLease<'_>, model_call_id: Uuid) -> Option<i32> {
        match self.begin_within(lease, model_call_id, BUDGET) {
            CallAdmission::Admitted(attempt) => Some(attempt),
            CallAdmission::Refused => None,
            CallAdmission::OverBudget => panic!("the T1–T20 budget is never reached"),
        }
    }

    fn begin_within(
        &self,
        lease: &DistillLease<'_>,
        model_call_id: Uuid,
        budget: DistillCallBudget,
    ) -> CallAdmission {
        self.rt
            .block_on(jobs::begin_distill_call(
                &self.private,
                lease,
                model_call_id,
                MIN_REMAINING,
                budget,
            ))
            .expect("begin_call must not error")
    }

    fn renew(&self, lease: &DistillLease<'_>) -> Option<sqlx::types::time::OffsetDateTime> {
        self.rt
            .block_on(jobs::renew_distill_lease(&self.private, lease, LEASE))
            .expect("renew must not error")
    }

    fn finish(
        &self,
        lease: &DistillLease<'_>,
        outcome: DistillFinish,
        class: Option<&str>,
    ) -> bool {
        self.try_finish(lease, outcome, class)
            .expect("finish must not error")
    }

    fn try_finish(
        &self,
        lease: &DistillLease<'_>,
        outcome: DistillFinish,
        class: Option<&str>,
    ) -> Result<bool, JobsError> {
        self.rt.block_on(jobs::finish_distill(
            &self.private,
            lease,
            outcome,
            class,
            0.0,
            PARK,
        ))
    }

    /// (status, dispatch_state, attempt, claim_generation, abandoned_claims, last_error_class)
    fn job(&mut self, job: Uuid) -> (String, Option<String>, i32, i32, i32, Option<String>) {
        let r = self
            .admin
            .query_one(
                "SELECT status, dispatch_state, attempt, claim_generation, abandoned_claims, last_error_class \
                 FROM ops.jobs WHERE job_id = $1",
                &[&job],
            )
            .expect("job row");
        (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5))
    }

    /// The (job, generation) a slot holds, or None when `job` holds no slot.
    fn slot_gen_of(&mut self, job: Uuid) -> Option<i32> {
        self.admin
            .query_opt(
                "SELECT claim_generation FROM ops.provider_slots WHERE job_id = $1",
                &[&job],
            )
            .expect("slot probe")
            .map(|r| r.get(0))
    }

    fn bound_slots(&mut self) -> i64 {
        self.admin
            .query_one(
                "SELECT count(*) FROM ops.provider_slots WHERE job_id IS NOT NULL",
                &[],
            )
            .expect("bound slots")
            .get(0)
    }

    fn calls_of(&mut self, job: Uuid) -> i64 {
        self.admin
            .query_one(
                "SELECT count(*) FROM ops.distill_calls WHERE job_id = $1",
                &[&job],
            )
            .expect("distill_calls count")
            .get(0)
    }

    fn sql(&mut self, sql: &str, job: Uuid) {
        self.admin.execute(sql, &[&job]).expect("admin statement");
    }

    /// ADR-0058 §2.1 I-SLOT over the whole table.
    fn assert_i_slot(&mut self) {
        let broken: i64 = self
            .admin
            .query_one(
                "SELECT count(*) FROM ops.jobs j \
                 WHERE j.job_type = 'DERIVED_DISTILL' AND j.status = 'PROCESSING' \
                   AND j.dispatch_state IS NOT NULL \
                   AND (SELECT count(*) FROM ops.provider_slots s \
                        WHERE s.job_id = j.job_id AND s.claim_generation = j.claim_generation) <> 1",
                &[],
            )
            .expect("I-SLOT probe")
            .get(0);
        assert_eq!(
            broken, 0,
            "I-SLOT: every dispatched distill job holds exactly its own slot"
        );
    }
}

fn run(name: &str, body: impl FnOnce(Handle)) {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<Fixture, _>(name, body);
}

/// T1 — fault: `attempt = j.attempt + 1` in the claim UPDATE.
#[test]
fn claim_takes_one_job_binds_one_slot_and_leaves_attempt_alone() {
    run(
        "claim_takes_one_job_binds_one_slot_and_leaves_attempt_alone",
        |mut h| {
            let a = h.tenant();
            let older = h.seed(a, 20.0);
            let newer = h.seed(a, 10.0);
            let c = h
                .claim("c32-t1")
                .expect("one READY job and four free slots");
            assert_eq!(c.job_id, older, "tenant FIFO: the oldest READY job first");
            assert_eq!(
                (c.attempt, c.claim_generation, c.abandoned_claims),
                (0, 1, 0)
            );
            assert!(c.lease_expires_at <= c.hard_deadline);
            let (status, state, attempt, generation, _, _) = h.job(older);
            assert_eq!(status, "PROCESSING");
            assert_eq!(state.as_deref(), Some(DispatchState::Claimed.as_db_str()));
            assert_eq!((attempt, generation), (0, 1), "no attempt at claim");
            assert_eq!(h.slot_gen_of(older), Some(1));
            assert_eq!(h.bound_slots(), 1, "one claim binds exactly one slot");
            let bound_at_deadline: bool = h
                .admin
                .query_one(
                    "SELECT s.bound_until = j.hard_deadline FROM ops.provider_slots s \
                 JOIN ops.jobs j ON j.job_id = s.job_id WHERE s.job_id = $1",
                    &[&older],
                )
                .unwrap()
                .get(0);
            assert!(
                bound_at_deadline,
                "slot bound_until is the claim's hard_deadline"
            );
            assert_eq!(h.job(newer).0, "PENDING", "one job per claim");
            h.assert_i_slot();
        },
    );
}

/// T2 — fault: replace the tenant pick by `ORDER BY j.created_at` over all tenants.
#[test]
fn claims_rotate_across_tenants_not_global_fifo() {
    run("claims_rotate_across_tenants_not_global_fifo", |mut h| {
        let (a, b, c) = (h.tenant(), h.tenant(), h.tenant());
        for i in 0..5 {
            h.seed(a, 100.0 - f64::from(i));
        }
        for i in 0..2 {
            h.seed(b, 50.0 - f64::from(i));
            h.seed(c, 40.0 - f64::from(i));
        }
        // A was served longest ago, then B, then C.
        for (t, turn) in [(a, -3i64), (b, -2), (c, -1)] {
            h.admin
                .execute(
                    "UPDATE ops.distill_tenant_scheduler SET last_served_turn = $2 WHERE tenant_id = $1",
                    &[&t, &turn],
                )
                .unwrap();
        }
        let mut served = Vec::new();
        for _ in 0..6 {
            let claim = h.claim("c32-t2").expect("READY work remains");
            served.push(claim.tenant_id);
            assert!(h.finish(
                &DistillLease::of(&claim, "c32-t2"),
                DistillFinish::Done,
                None
            ));
        }
        assert_eq!(
            served,
            vec![a, b, c, a, b, c],
            "least-recently-served tenant first"
        );
        h.assert_i_slot();
    });
}

/// T3 — fault: drop `FOR UPDATE SKIP LOCKED` on the slot pick and the `job_id IS NULL` recheck on
/// bind (with the arbiter lock also dropped: the arbiter serializes claims on its own, ADR-0058 E2).
#[test]
fn at_most_four_slots_are_bound_under_twelve_concurrent_claimers() {
    run(
        "at_most_four_slots_are_bound_under_twelve_concurrent_claimers",
        |mut h| {
            let mut seeded = Vec::new();
            for _ in 0..3 {
                let t = h.tenant();
                for i in 0..4 {
                    seeded.push(h.seed(t, 30.0 - f64::from(i)));
                }
            }
            let dsn = dsn_as_role(&h.dsn, "role_private_worker");
            let barrier = Barrier::new(12);
            let claimed: Vec<Uuid> = std::thread::scope(|scope| {
                let workers: Vec<_> = (0..12)
                    .map(|i| {
                        let (dsn, barrier) = (dsn.clone(), &barrier);
                        scope.spawn(move || {
                            let rt = tokio::runtime::Runtime::new().expect("runtime");
                            rt.block_on(async {
                                // dep: PostgreSQL(role_private_worker) — role-scoped pool call
                                let pool =
                                    PrivateWorkerDbPool::connect(&dsn).await.expect("connect");
                                barrier.wait();
                                jobs::claim_distill(&pool, &format!("c32-t3-{i}"), LEASE, HARD)
                                    .await
                                    .expect("claim must not error")
                                    .map(|c| c.job_id)
                            })
                        })
                    })
                    .collect();
                workers
                    .into_iter()
                    .filter_map(|w| w.join().expect("worker"))
                    .collect()
            });
            let distinct: std::collections::BTreeSet<_> = claimed.iter().collect();
            assert_eq!(distinct.len(), claimed.len(), "no job claimed twice");
            assert_eq!(claimed.len(), 4, "exactly four claims succeed");
            assert_eq!(h.bound_slots(), 4);
            let processing: i64 = h
            .admin
            .query_one(
                "SELECT count(*) FROM ops.jobs WHERE job_id = ANY($1) AND status = 'PROCESSING'",
                &[&seeded],
            )
            .unwrap()
            .get(0);
            assert_eq!(
                processing, 4,
                "PROCESSING distill jobs never exceed the four slots"
            );
            h.assert_i_slot();
        },
    );
}

/// T4 — fault: drop `FOR UPDATE` on the arbiter row.
#[test]
fn each_successful_claim_advances_the_arbiter_turn_exactly_once() {
    run(
        "each_successful_claim_advances_the_arbiter_turn_exactly_once",
        |mut h| {
            for _ in 0..3 {
                let t = h.tenant();
                for i in 0..8 {
                    h.seed(t, 30.0 - f64::from(i));
                }
            }
            let turn = |h: &mut Handle| -> i64 {
                h.admin
                    .query_one("SELECT next_turn FROM ops.provider_arbiters", &[])
                    .unwrap()
                    .get(0)
            };
            let before = turn(&mut h);
            let dsn = dsn_as_role(&h.dsn, "role_private_worker");
            let barrier = Barrier::new(12);
            let successes: usize = std::thread::scope(|scope| {
                let workers: Vec<_> = (0..12)
                    .map(|i| {
                        let (dsn, barrier) = (dsn.clone(), &barrier);
                        scope.spawn(move || {
                            let rt = tokio::runtime::Runtime::new().expect("runtime");
                            rt.block_on(async {
                                // dep: PostgreSQL(role_private_worker) — role-scoped pool call
                                let pool =
                                    PrivateWorkerDbPool::connect(&dsn).await.expect("connect");
                                let owner = format!("c32-t4-{i}");
                                barrier.wait();
                                let mut n = 0;
                                for _ in 0..3 {
                                    let Some(c) = jobs::claim_distill(&pool, &owner, LEASE, HARD)
                                        .await
                                        .expect("claim must not error")
                                    else {
                                        continue;
                                    };
                                    let lease = DistillLease::of(&c, &owner);
                                    assert!(
                                        jobs::finish_distill(
                                            &pool,
                                            &lease,
                                            DistillFinish::Done,
                                            None,
                                            0.0,
                                            PARK
                                        )
                                        .await
                                        .expect("finish")
                                    );
                                    n += 1;
                                }
                                n
                            })
                        })
                    })
                    .collect();
                workers.into_iter().map(|w| w.join().expect("worker")).sum()
            });
            let after = turn(&mut h);
            assert!(successes > 0);
            assert_eq!(
                usize::try_from(after - before).unwrap(),
                successes,
                "next_turn advances exactly once per successful claim"
            );
            h.assert_i_slot();
        },
    );
}

/// T5 — fault: drop `claim_generation = p_claim_generation` from begin_call (masked by the slot
/// EXISTS, which is generation-bound too: red needs both dropped; T16 gates the EXISTS alone).
#[test]
fn begin_call_counts_one_attempt_writes_one_call_row_and_refuses_a_stale_generation() {
    run(
        "begin_call_counts_one_attempt_writes_one_call_row_and_refuses_a_stale_generation",
        |mut h| {
            let a = h.tenant();
            let j = h.seed(a, 5.0);
            let c1 = h.claim("c32-t5").expect("claim gen 1");
            let m1 = Uuid::new_v4();
            assert_eq!(h.begin(&DistillLease::of(&c1, "c32-t5"), m1), Some(1));
            let (_, state, attempt, _, _, _) = h.job(j);
            assert_eq!(
                state.as_deref(),
                Some(DispatchState::DispatchIntent.as_db_str())
            );
            assert_eq!(attempt, 1);
            let row: (Uuid, i32, i32) = {
                let r = h
                    .admin
                    .query_one(
                        "SELECT job_id, claim_generation, attempt FROM ops.distill_calls WHERE model_call_id = $1",
                        &[&m1],
                    )
                    .expect("one distill_calls row");
                (r.get(0), r.get(1), r.get(2))
            };
            assert_eq!(row, (j, 1, 1));
            let dispatched: Option<Uuid> = h
                .admin
                .query_one(
                    "SELECT dispatch_model_call_id FROM ops.jobs WHERE job_id = $1",
                    &[&j],
                )
                .unwrap()
                .get(0);
            assert_eq!(dispatched, Some(m1));

            // Supersede gen 1 (T6 reconcile, then a re-claim by the SAME owner string): gen 1's
            // late begin_call must be refused although owner, status and lease all match.
            h.sql(
                "UPDATE ops.jobs SET hard_deadline = clock_timestamp() - interval '1 second', \
                   lease_expires_at = clock_timestamp() - interval '1 second' WHERE job_id = $1",
                j,
            );
            h.sql(
                "UPDATE ops.provider_slots SET bound_until = clock_timestamp() - interval '1 second' WHERE job_id = $1",
                j,
            );
            assert!(
                h.claim("c32-t5").is_none(),
                "T6 backs the job off by one lease"
            );
            h.sql(
                "UPDATE ops.jobs SET next_retry_at = clock_timestamp() WHERE job_id = $1",
                j,
            );
            let c2 = h.claim("c32-t5").expect("claim gen 2");
            assert_eq!(c2.claim_generation, 2);
            assert_eq!(
                h.begin(&DistillLease::of(&c1, "c32-t5"), Uuid::new_v4()),
                None
            );
            assert_eq!(h.job(j).2, 1, "a refused begin_call counts nothing");
            assert_eq!(h.calls_of(j), 1, "and writes no distill_calls row");
            let mut wrong_owner = DistillLease::of(&c2, "c32-t5");
            wrong_owner.lease_owner = "someone-else";
            assert_eq!(h.begin(&wrong_owner, Uuid::new_v4()), None);
            assert_eq!(
                h.begin(&DistillLease::of(&c2, "c32-t5"), Uuid::new_v4()),
                Some(2)
            );
            h.assert_i_slot();
        },
    );
}

/// T6 — fault: replace `LEAST(…, hard_deadline)` by the plain lease.
#[test]
fn renew_lease_is_fenced_and_never_passes_hard_deadline() {
    run(
        "renew_lease_is_fenced_and_never_passes_hard_deadline",
        |mut h| {
            let a = h.tenant();
            let j = h.seed(a, 5.0);
            let c = h.claim("c32-t6").expect("claim");
            let lease = DistillLease::of(&c, "c32-t6");
            h.sql(
            "UPDATE ops.jobs SET hard_deadline = clock_timestamp() + interval '5 seconds' WHERE job_id = $1",
            j,
        );
            let until = h.renew(&lease).expect("live lease renews");
            let ahead = until - sqlx::types::time::OffsetDateTime::now_utc();
            assert!(
                ahead.whole_seconds() < 6,
                "the renewed lease never passes hard_deadline (5 s away), got {ahead}"
            );
            let stored_capped: bool = h
                .admin
                .query_one(
                    "SELECT lease_expires_at <= hard_deadline FROM ops.jobs WHERE job_id = $1",
                    &[&j],
                )
                .unwrap()
                .get(0);
            assert!(stored_capped);
            let mut stale = lease;
            stale.claim_generation -= 1;
            assert!(
                h.renew(&stale).is_none(),
                "a superseded generation renews nothing"
            );
            let mut other = lease;
            other.lease_owner = "someone-else";
            assert!(h.renew(&other).is_none(), "another owner renews nothing");
            h.sql(
            "UPDATE ops.jobs SET hard_deadline = clock_timestamp() - interval '1 second' WHERE job_id = $1",
            j,
        );
            assert!(
                h.renew(&lease).is_none(),
                "past hard_deadline the lease is lost"
            );
            h.assert_i_slot();
        },
    );
}

/// T7 — fault: free the slot in the T5 branch (lease-expiry-frees-slot).
#[test]
fn an_expired_dispatch_intent_becomes_uncertain_and_keeps_its_slot() {
    run(
        "an_expired_dispatch_intent_becomes_uncertain_and_keeps_its_slot",
        |mut h| {
            let a = h.tenant();
            let j = h.seed(a, 5.0);
            let c = h.claim("c32-t7").expect("claim");
            let lease = DistillLease::of(&c, "c32-t7");
            assert_eq!(h.begin(&lease, Uuid::new_v4()), Some(1));
            h.sql(
            "UPDATE ops.jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE job_id = $1",
            j,
        );
            assert!(h.claim("c32-t7-sweeper").is_none(), "nothing else is READY");
            let (status, state, attempt, generation, _, _) = h.job(j);
            assert_eq!(status, "PROCESSING");
            assert_eq!(
                state.as_deref(),
                Some(DispatchState::ExecutionUncertain.as_db_str())
            );
            assert_eq!((attempt, generation), (1, 1));
            assert_eq!(
                h.slot_gen_of(j),
                Some(1),
                "lease expiry is not the end of execution"
            );
            assert!(
                h.renew(&lease).is_none(),
                "an uncertain call's heartbeat is lost"
            );
            assert!(
                h.finish(&lease, DistillFinish::Done, None),
                "a same-generation late result is still accepted (ADR-0036 D4)"
            );
            assert_eq!(h.slot_gen_of(j), None);
            h.assert_i_slot();
        },
    );
}

/// T8 — fault: test `lease_expires_at` instead of `hard_deadline` in the T6 branch.
#[test]
fn uncertain_reconciles_only_after_hard_deadline_with_its_class() {
    run(
        "uncertain_reconciles_only_after_hard_deadline_with_its_class",
        |mut h| {
            let a = h.tenant();
            let j = h.seed(a, 5.0);
            let c = h.claim("c32-t8").expect("claim");
            let m1 = Uuid::new_v4();
            assert_eq!(h.begin(&DistillLease::of(&c, "c32-t8"), m1), Some(1));
            h.sql(
            "UPDATE ops.jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE job_id = $1",
            j,
        );
            assert!(h.claim("c32-t8-sweeper").is_none());
            assert!(
                h.claim("c32-t8-sweeper").is_none(),
                "a second sweep before the deadline changes nothing"
            );
            let (status, state, ..) = h.job(j);
            assert_eq!(
                (status.as_str(), state.as_deref()),
                (
                    "PROCESSING",
                    Some(DispatchState::ExecutionUncertain.as_db_str())
                ),
                "never reconciled before hard_deadline"
            );
            assert_eq!(h.slot_gen_of(j), Some(1));
            h.sql(
            "UPDATE ops.jobs SET hard_deadline = clock_timestamp() - interval '1 second' WHERE job_id = $1",
            j,
        );
            h.sql(
            "UPDATE ops.provider_slots SET bound_until = clock_timestamp() - interval '1 second' WHERE job_id = $1",
            j,
        );
            assert!(
                h.claim("c32-t8-sweeper").is_none(),
                "the reconciled job backs off one lease"
            );
            let (status, state, attempt, _, _, class) = h.job(j);
            assert_eq!(status, "PENDING");
            assert_eq!(state, None);
            assert_eq!(attempt, 1, "the uncertain call stays counted");
            assert_eq!(class.as_deref(), Some("EXECUTION_UNCERTAIN"));
            assert_eq!(
                h.slot_gen_of(j),
                None,
                "only hard_deadline frees a dispatched slot"
            );
            let r = h
            .admin
            .query_one(
                "SELECT dispatch_model_call_id, next_retry_at > clock_timestamp() FROM ops.jobs WHERE job_id = $1",
                &[&j],
            )
            .unwrap();
            assert_eq!(
                r.get::<_, Option<Uuid>>(0),
                Some(m1),
                "the uncertain ledger row stays named"
            );
            assert!(r.get::<_, bool>(1));
            h.assert_i_slot();
        },
    );
}

/// T9 — fault: drop `abandoned_claims + 1` in the T4 branch.
#[test]
fn an_expired_claimed_job_is_ready_again_and_counts_an_abandoned_claim() {
    run(
        "an_expired_claimed_job_is_ready_again_and_counts_an_abandoned_claim",
        |mut h| {
            let a = h.tenant();
            let j = h.seed(a, 5.0);
            h.claim("c32-t9-dead").expect("claim gen 1");
            h.sql(
            "UPDATE ops.jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE job_id = $1",
            j,
        );
            let c = h
                .claim("c32-t9-live")
                .expect("the sweep re-opens the job and this claim takes it");
            assert_eq!(c.job_id, j);
            assert_eq!(
                (c.claim_generation, c.attempt, c.abandoned_claims),
                (2, 0, 1)
            );
            assert_eq!(h.slot_gen_of(j), Some(2));
            assert_eq!(h.bound_slots(), 1, "the abandoned claim's slot was freed");
            h.assert_i_slot();
        },
    );
}

/// T10 — fault: drop the generation predicate in finish.
#[test]
fn finish_frees_the_slot_and_rejects_a_stale_generation() {
    run(
        "finish_frees_the_slot_and_rejects_a_stale_generation",
        |mut h| {
            let a = h.tenant();
            let j = h.seed(a, 5.0);
            let c1 = h.claim("c32-t10").expect("claim gen 1");
            h.sql(
            "UPDATE ops.jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE job_id = $1",
            j,
        );
            let c2 = h.claim("c32-t10").expect("gen 2, same owner string");
            assert_eq!(c2.claim_generation, 2);
            assert!(
                !h.finish(&DistillLease::of(&c1, "c32-t10"), DistillFinish::Done, None),
                "gen 1 settles nothing"
            );
            let (status, state, ..) = h.job(j);
            assert_eq!(
                (status.as_str(), state.as_deref()),
                ("PROCESSING", Some("CLAIMED"))
            );
            assert_eq!(h.slot_gen_of(j), Some(2));
            assert!(h.finish(&DistillLease::of(&c2, "c32-t10"), DistillFinish::Done, None));
            assert_eq!(h.job(j).0, "DONE");
            assert_eq!(h.slot_gen_of(j), None, "finish frees its slot");
            assert_eq!(h.bound_slots(), 0);
            h.assert_i_slot();
        },
    );
}

/// T11 — fault: remove the park branch (NOT_READY always PENDING).
#[test]
fn not_ready_past_the_park_age_parks_waiting_key_and_is_rechecked() {
    run(
        "not_ready_past_the_park_age_parks_waiting_key_and_is_rechecked",
        |mut h| {
            let a = h.tenant();
            let j = h.seed(a, 5.0);
            let c1 = h.claim("c32-t11").expect("claim 1");
            assert!(h.finish(
                &DistillLease::of(&c1, "c32-t11"),
                DistillFinish::NotReady,
                Some("NO_BINDING")
            ));
            let (status, _, attempt, _, _, class) = h.job(j);
            assert_eq!(
                (status.as_str(), attempt, class.as_deref()),
                ("PENDING", 0, Some("NO_BINDING"))
            );
            let c2 = h.claim("c32-t11").expect("backoff 0: READY again");
            assert!(h.finish(
                &DistillLease::of(&c2, "c32-t11"),
                DistillFinish::NotReady,
                Some("NO_BINDING")
            ));
            assert_eq!(h.job(j).0, "PENDING", "younger than the park age");
            h.sql(
            "UPDATE ops.jobs SET not_ready_since = clock_timestamp() - interval '1 hour' WHERE job_id = $1",
            j,
        );
            let c3 = h.claim("c32-t11").expect("claim 3");
            assert!(h.finish(
                &DistillLease::of(&c3, "c32-t11"),
                DistillFinish::NotReady,
                Some("NO_BINDING")
            ));
            let (status, _, attempt, _, _, class) = h.job(j);
            assert_eq!(
                (status.as_str(), attempt, class.as_deref()),
                ("WAITING_KEY", 0, Some("NO_BINDING")),
                "parked visibly with its class, no attempt spent"
            );
            assert!(h.claim("c32-t11").is_none(), "parked for the park interval");
            h.sql(
                "UPDATE ops.jobs SET next_retry_at = clock_timestamp() WHERE job_id = $1",
                j,
            );
            let c4 = h
                .claim("c32-t11")
                .expect("a parked job is re-checked once its interval passed");
            assert_eq!(c4.job_id, j);
            assert!(h.finish(&DistillLease::of(&c4, "c32-t11"), DistillFinish::Done, None));
            h.assert_i_slot();
        },
    );
}

/// The values inside `<column> IN (...)` or `<column> = ANY (ARRAY[...])` of a CHECK / function
/// text, quotes and `::text` casts stripped.
fn closed_set(text: &str, column: &str) -> Vec<String> {
    let (start, close) = if let Some(i) = text.find(&format!("{column} = ANY (ARRAY[")) {
        (i + format!("{column} = ANY (ARRAY[").len(), ']')
    } else if let Some(i) = text.find(&format!("{column} IN (")) {
        (i + format!("{column} IN (").len(), ')')
    } else {
        panic!("no closed set for {column} in {text}");
    };
    let end = start + text[start..].find(close).expect("unterminated closed set");
    text[start..end]
        .split(',')
        .map(|v| {
            v.trim()
                .trim_end_matches("::text")
                .trim_matches('\'')
                .to_string()
        })
        .collect()
}

/// T12 — fault: add a Rust variant not in SQL.
#[test]
fn finish_outcomes_and_dispatch_states_match_the_rust_closed_sets() {
    run(
        "finish_outcomes_and_dispatch_states_match_the_rust_closed_sets",
        |mut h| {
            let check: String = h
                .admin
                .query_one(
                    "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
                 WHERE conrelid = 'ops.jobs'::regclass AND conname = 'jobs_dispatch_state_check'",
                    &[],
                )
                .unwrap()
                .get(0);
            let rust_states: Vec<String> = DispatchState::ALL
                .iter()
                .map(|s| s.as_db_str().to_string())
                .collect();
            assert_eq!(closed_set(&check, "dispatch_state"), rust_states);

            let body: String = h
            .admin
            .query_one(
                "SELECT pg_get_functiondef('ops.finish_derived_work_v2(uuid,uuid,text,integer,text,text,double precision,double precision)'::regprocedure)",
                &[],
            )
            .unwrap()
            .get(0);
            let rust_outcomes: Vec<String> = DistillFinish::ALL
                .iter()
                .map(|o| o.as_db_str().to_string())
                .collect();
            assert_eq!(closed_set(&body, "p_outcome"), rust_outcomes);

            let a = h.tenant();
            for outcome in DistillFinish::ALL {
                h.seed(a, 5.0);
                let c = h.claim("c32-t12").expect("claim");
                let settled = h
                    .try_finish(&DistillLease::of(&c, "c32-t12"), outcome, Some("TEST"))
                    .unwrap_or_else(|e| panic!("{} must be accepted: {e}", outcome.as_db_str()));
                assert!(settled, "{} settles a live claim", outcome.as_db_str());
            }
            let err = h
            .admin
            .query_one(
                "SELECT ops.finish_derived_work_v2(gen_random_uuid(), gen_random_uuid(), 'x', 1, 'BOGUS', NULL, 0, 1)",
                &[],
            )
            .expect_err("an unknown outcome is refused");
            assert_eq!(err.code().map(|c| c.code()), Some("23514"));
            h.assert_i_slot();
        },
    );
}

/// T13 — fault: drop the `derived_distill_scheduler_admit` trigger.
#[test]
fn the_enqueue_admits_one_scheduler_row_per_tenant() {
    run(
        "the_enqueue_admits_one_scheduler_row_per_tenant",
        |mut h| {
            let (a, other) = (h.tenant(), h.tenant());
            let rows = |h: &mut Handle, t: Uuid| -> (i64, Option<i64>) {
                let r = h
                .admin
                .query_one(
                    "SELECT count(*), max(last_served_turn) FROM ops.distill_tenant_scheduler WHERE tenant_id = $1",
                    &[&t],
                )
                .unwrap();
                (r.get(0), r.get(1))
            };
            assert_eq!(rows(&mut h, a).0, 0);
            for i in 0..3 {
                h.seed(a, f64::from(i));
            }
            assert_eq!(
                rows(&mut h, a),
                (1, Some(0)),
                "one row per tenant, initial turn 0"
            );
            h.admin
                .execute(
                    "INSERT INTO ops.jobs (tenant_id, job_type, idempotency_key, next_retry_at) \
                 VALUES ($1, 'test.noop', 'c32-dispatch-v2:' || gen_random_uuid()::text, now())",
                    &[&other],
                )
                .unwrap();
            assert_eq!(
                rows(&mut h, other).0,
                0,
                "only DERIVED_DISTILL work admits a tenant"
            );
            h.assert_i_slot();
        },
    );
}

/// T14 — fault: `GRANT SELECT ON ops.provider_slots TO role_private_worker`.
#[test]
fn runtime_roles_cannot_read_or_write_the_scheduling_tables() {
    run(
        "runtime_roles_cannot_read_or_write_the_scheduling_tables",
        |mut h| {
            for role in ["role_private_worker", "role_gateway"] {
                // dep: PostgreSQL(role_private_worker) — role-scoped connection (also role_gateway)
                let mut client =
                    Client::connect(&dsn_as_role(&h.dsn, role), NoTls).expect("role connect");
                for table in [
                    "ops.provider_slots",
                    "ops.provider_arbiters",
                    "ops.distill_tenant_scheduler",
                    "ops.distill_calls",
                ] {
                    for stmt in [
                        format!("SELECT 1 FROM {table} LIMIT 1"),
                        format!("DELETE FROM {table}"),
                    ] {
                        let err = client
                            .batch_execute(&stmt)
                            .expect_err("no runtime privilege");
                        assert_eq!(
                            err.code().map(|c| c.code()),
                            Some("42501"),
                            "{role}: {stmt} must be refused"
                        );
                    }
                }
            }
            h.assert_i_slot();
        },
    );
}

/// T15 — fault: heal on SKIP LOCKED NOT FOUND (decide "gone" from the probe).
#[test]
fn a_row_locked_in_flight_job_keeps_its_slot_through_a_claim() {
    run(
        "a_row_locked_in_flight_job_keeps_its_slot_through_a_claim",
        |mut h| {
            let a = h.tenant();
            for i in 0..5 {
                h.seed(a, 50.0 - f64::from(i));
            }
            let mut claims = Vec::new();
            for i in 0..4 {
                let owner = format!("c32-t15-{i}");
                let c = h.claim(&owner).expect("four slots");
                assert_eq!(
                    h.begin(&DistillLease::of(&c, &owner), Uuid::new_v4()),
                    Some(1)
                );
                claims.push(c);
            }
            let j1 = claims[0].job_id;
            h.sql(
            "UPDATE ops.jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE job_id = $1",
            j1,
        );
            // dep: PostgreSQL(owner) — connection X holding J1's row lock (as renew / finish would)
            let mut x = Client::connect(&h.dsn, NoTls).expect("connection X");
            x.batch_execute("BEGIN").unwrap();
            x.query(
                "SELECT 1 FROM ops.jobs WHERE job_id = $1 FOR UPDATE",
                &[&j1],
            )
            .unwrap();
            assert!(
                h.claim("c32-t15-y").is_none(),
                "no free slot: J1's slot stays bound"
            );
            assert_eq!(h.slot_gen_of(j1), Some(1));
            assert_eq!(
                h.job(j1).1.as_deref(),
                Some(DispatchState::DispatchIntent.as_db_str()),
                "a row the sweep cannot lock is left as it is"
            );
            x.batch_execute("ROLLBACK").unwrap();
            h.sql(
            "UPDATE ops.provider_slots SET bound_until = clock_timestamp() - interval '1 second' WHERE job_id = $1",
            j1,
        );
            h.sql("DELETE FROM ops.jobs WHERE job_id = $1", j1);
            let fifth = h
                .claim("c32-t15-y")
                .expect("the orphan slot is healed and reused");
            assert!(claims.iter().all(|c| c.job_id != fifth.job_id));
            assert_eq!(h.slot_gen_of(j1), None);
            assert_eq!(h.bound_slots(), 4);
            h.assert_i_slot();
        },
    );
}

/// T16 — fault: drop the `ops.provider_slots` EXISTS from begin_call.
#[test]
fn begin_call_refuses_when_the_slot_is_not_bound_to_its_generation() {
    run(
        "begin_call_refuses_when_the_slot_is_not_bound_to_its_generation",
        |mut h| {
            let a = h.tenant();
            let j = h.seed(a, 5.0);
            let c = h.claim("c32-t16").expect("claim");
            let lease = DistillLease::of(&c, "c32-t16");
            h.sql(
            "UPDATE ops.provider_slots SET job_id = NULL, claim_generation = NULL, bound_until = NULL WHERE job_id = $1",
            j,
        );
            assert_eq!(h.begin(&lease, Uuid::new_v4()), None, "no slot, no request");
            assert_eq!(h.job(j).2, 0, "attempt unchanged");
            assert_eq!(h.calls_of(j), 0, "no distill_calls row");
            assert!(h.finish(&lease, DistillFinish::NotReady, Some("DISPATCH_REFUSED")));
            h.assert_i_slot();
        },
    );
}

/// T18 — fault: drop the WAITING_KEY attempt revert in finish.
#[test]
fn a_waiting_key_finish_reverts_its_attempt() {
    run("a_waiting_key_finish_reverts_its_attempt", |mut h| {
        let a = h.tenant();
        let j = h.seed(a, 5.0);
        let c = h.claim("c32-t18").expect("claim");
        let lease = DistillLease::of(&c, "c32-t18");
        assert_eq!(h.begin(&lease, Uuid::new_v4()), Some(1));
        assert!(h.finish(&lease, DistillFinish::WaitingKey, Some("WAITING_KEY")));
        let (status, state, attempt, _, _, class) = h.job(j);
        assert_eq!(
            (status.as_str(), state, attempt, class.as_deref()),
            ("WAITING_KEY", None, 0, Some("WAITING_KEY")),
            "§11: a 401 spends no retry"
        );
        assert_eq!(h.calls_of(j), 1, "the physical call stays recorded");
        let parked: bool = h
            .admin
            .query_one(
                "SELECT next_retry_at > clock_timestamp() FROM ops.jobs WHERE job_id = $1",
                &[&j],
            )
            .unwrap()
            .get(0);
        assert!(parked);
        h.assert_i_slot();
    });
}

/// T17 (slice 2) — fault: keep 0164's body in 0193 (no DERIVED_DISTILL refusal).
#[test]
fn v1_claim_refuses_distill_for_every_role() {
    run("v1_claim_refuses_distill_for_every_role", |mut h| {
        let a = h.tenant();
        let j = h.seed(a, 5.0);
        let c = h.claim("c32-t17").expect("claim");
        assert_eq!(
            h.begin(&DistillLease::of(&c, "c32-t17"), Uuid::new_v4()),
            Some(1)
        );
        h.sql(
            "UPDATE ops.jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE job_id = $1",
            j,
        );
        assert!(h.claim("c32-t17-sweeper").is_none());
        assert_eq!(
            h.job(j).1.as_deref(),
            Some(DispatchState::ExecutionUncertain.as_db_str()),
            "an EXECUTION_UNCERTAIN distill job with an expired lease is present"
        );
        // dep: PostgreSQL(role_consolidation_worker) — the v1 claim's remaining executor
        let mut consolidation =
            Client::connect(&dsn_as_role(&h.dsn, "role_consolidation_worker"), NoTls)
                .expect("consolidation connection");
        // Rolled back: the consolidate claim is cross-tenant and must not touch foreign rows.
        let mut tx = consolidation.transaction().expect("begin");
        let refused = tx
            .query(
                "SELECT job_id FROM ops.claim_derived_work(ARRAY['DERIVED_DISTILL'], 'c32-t17-v1', 30, 8)",
                &[],
            )
            .expect_err("v1 must refuse DERIVED_DISTILL");
        assert_eq!(refused.code().map(|c| c.code()), Some("23514"), "{refused}");
        tx.rollback().expect("rollback");
        let mut tx = consolidation.transaction().expect("begin");
        let distill_claimed: i64 = tx
            .query_one(
                "SELECT count(*) FROM ops.claim_derived_work(ARRAY['DERIVED_CONSOLIDATE'], 'c32-t17-v1', 30, 8) \
                 WHERE job_type = 'DERIVED_DISTILL'",
                &[],
            )
            .expect("consolidate claim")
            .get(0);
        tx.rollback().expect("rollback");
        assert_eq!(distill_claimed, 0, "no arm of v1 can reach a distill row");
        // dep: PostgreSQL(role_private_worker) — the private worker lost EXECUTE on v1 (0193)
        let mut private = Client::connect(&dsn_as_role(&h.dsn, "role_private_worker"), NoTls)
            .expect("private connection");
        let denied = private
            .query(
                "SELECT job_id FROM ops.claim_derived_work(ARRAY['DERIVED_CONSOLIDATE'], 'c32-t17-v1', 30, 8)",
                &[],
            )
            .expect_err("role_private_worker has no EXECUTE on the v1 claim");
        assert_eq!(denied.code().map(|c| c.code()), Some("42501"), "{denied}");
        let (status, state, ..) = h.job(j);
        assert_eq!(
            (status.as_str(), state.as_deref()),
            (
                "PROCESSING",
                Some(DispatchState::ExecutionUncertain.as_db_str())
            )
        );
        h.assert_i_slot();
    });
}

const CUTOVER_SQL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../migrations/0193_private_worker_off_claim_v1.sql"
));

/// 0193's `adr0058-cutover` block, verbatim.
fn cutover_block() -> &'static str {
    let start = CUTOVER_SQL
        .find("-- BEGIN adr0058-cutover")
        .expect("0193 marks its cutover block");
    let end = CUTOVER_SQL
        .find("-- END adr0058-cutover")
        .expect("0193 closes its cutover block");
    &CUTOVER_SQL[start..end]
}

/// T19 (slice 2) — faults: drop the backfill (the job-less row has no job); drop the counter
/// reset (the attempt-12 job's first counted call is attempt 13 ⇒ DEAD at max 3).
#[test]
#[allow(clippy::too_many_lines)]
fn the_cutover_block_rearms_stranded_evidence_with_a_full_budget() {
    run(
        "the_cutover_block_rearms_stranded_evidence_with_a_full_budget",
        |mut h| {
            const MAX_ATTEMPTS: i32 = 3;
            let mut tx = h.admin.transaction().expect("begin");
            // v1 shapes, seeded with FK triggers off (the block itself runs with them on).
            let seeded = tx
                .query_one(
                    "WITH a AS (INSERT INTO control.tenants (name) VALUES ('distill_dispatch_v2.rs t19 unbound') RETURNING tenant_id), \
                          b AS (INSERT INTO control.tenants (name) VALUES ('distill_dispatch_v2.rs t19 bound') RETURNING tenant_id) \
                     SELECT a.tenant_id, b.tenant_id, gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), gen_random_uuid() FROM a, b",
                    &[],
                )
                .expect("seed tenants");
            let (unbound, bound): (Uuid, Uuid) = (seeded.get(0), seeded.get(1));
            let (domain_a, domain_b, ev_dead, ev_jobless): (Uuid, Uuid, Uuid, Uuid) =
                (seeded.get(2), seeded.get(3), seeded.get(4), seeded.get(5));
            tx.batch_execute("SET LOCAL session_replication_role = replica")
                .expect("replica");
            tx.execute(
                "INSERT INTO control.reasoning_route_bindings \
                   (tenant_id, reasoning_domain_id, purpose, route_policy_id, route_policy_version) \
                 VALUES ($1, $2, 'PRIVATE_DISTILL_TEXT', gen_random_uuid(), 1)",
                &[&bound, &domain_b],
            )
            .expect("binding of the bound tenant");
            for (tenant, domain, evidence) in
                [(bound, domain_b, ev_dead), (unbound, domain_a, ev_jobless)]
            {
                tx.execute(
                    "INSERT INTO private.evidence_objects \
                       (evidence_id, tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                        visibility_class, reasoning_domain_id) \
                     VALUES ($3::uuid, $1, 'EVENT', sha256(convert_to($3::uuid::text, 'UTF8')), 'PRIVATE', 'DirectUserInput', \
                             'TENANT_SHARED', $2)",
                    &[&tenant, &domain, &evidence],
                )
                .expect("evidence");
                tx.execute(
                    "INSERT INTO ops.outbox (tenant_id, commit_seq, stream_seq, event_type, evidence_id) \
                     VALUES ($1, nextval('ops.commit_seq_seq'), 1, 'EVIDENCE_ACCEPTED', $2)",
                    &[&tenant, &evidence],
                )
                .expect("open outbox row");
            }
            let pending12: Uuid = tx
                .query_one(
                    "INSERT INTO ops.jobs (tenant_id, job_type, status, attempt, next_retry_at, idempotency_key, payload) \
                     VALUES ($1, 'DERIVED_DISTILL', 'PENDING', 12, clock_timestamp(), 'c32-t19:' || gen_random_uuid()::text, '{}'::jsonb) \
                     RETURNING job_id",
                    &[&bound],
                )
                .expect("v1 PENDING job, attempt 12")
                .get(0);
            let dead31: Uuid = tx
                .query_one(
                    "INSERT INTO ops.jobs (tenant_id, job_type, status, attempt, next_retry_at, idempotency_key, payload, last_error_class) \
                     VALUES ($1, 'DERIVED_DISTILL', 'DEAD', 31, clock_timestamp(), 'derived-work:DERIVED_DISTILL:' || $2::uuid::text, \
                             jsonb_build_object('schema_version', 1, 'reasoning_domain_id', $3::uuid::text, 'evidence_id', $2::uuid::text), 'v1') \
                     RETURNING job_id",
                    &[&bound, &ev_dead, &domain_b],
                )
                .expect("v1 DEAD job, attempt 31, open outbox")
                .get(0);
            tx.batch_execute("SET LOCAL session_replication_role = DEFAULT")
                .expect("origin");

            tx.batch_execute(cutover_block()).expect("cutover block");

            let job =
                |tx: &mut postgres::Transaction<'_>, id: Uuid| -> (String, i32, Option<String>) {
                    let r = tx
                    .query_one(
                        "SELECT status, attempt, last_error_class FROM ops.jobs WHERE job_id = $1",
                        &[&id],
                    )
                    .expect("job");
                    (r.get(0), r.get(1), r.get(2))
                };
            assert_eq!(
                job(&mut tx, pending12),
                ("PENDING".into(), 0, None),
                "reset"
            );
            assert_eq!(
                job(&mut tx, dead31),
                ("PENDING".into(), 0, None),
                "re-armed with a full budget (bound tenant)"
            );
            let backfilled = tx
                .query_opt(
                    "SELECT status, attempt, payload ->> 'reasoning_domain_id' FROM ops.jobs \
                     WHERE idempotency_key = 'derived-work:DERIVED_DISTILL:' || $1::uuid::text",
                    &[&ev_jobless],
                )
                .expect("backfill probe")
                .expect("the job-less open outbox row got a job");
            assert_eq!(
                (
                    backfilled.get::<_, String>(0),
                    backfilled.get::<_, i32>(1),
                    backfilled.get::<_, Option<String>>(2)
                ),
                ("WAITING_KEY".into(), 0, Some(domain_a.to_string())),
                "§11: a tenant without a binding is known blocked"
            );
            let admitted: i64 = tx
                .query_one(
                    "SELECT count(*) FROM ops.distill_tenant_scheduler WHERE tenant_id = ANY($1)",
                    &[&vec![unbound, bound]],
                )
                .expect("scheduler")
                .get(0);
            assert_eq!(admitted, 2, "both tenants are in the rotation");

            // One counted failed call with max 3 must be a RETRY, not DEAD: the first admitted
            // call of the reset job is attempt 1.
            let mut claim = None;
            for _ in 0..3 {
                let row = tx
                    .query_opt(
                        "SELECT job_id, claim_generation FROM ops.claim_derived_work_v2('c32-t19', 30, 300)",
                        &[],
                    )
                    .expect("claim inside the txn");
                if let Some(row) = row
                    && row.get::<_, Uuid>(0) == pending12
                {
                    claim = Some(row.get::<_, i32>(1));
                    break;
                }
            }
            let generation = claim.expect("the reset job is claimed");
            let attempt: Option<i32> = tx
                .query_one(
                    "SELECT ops.begin_call($1, $2, 'c32-t19', $3, gen_random_uuid(), 0)",
                    &[&pending12, &bound, &generation],
                )
                .expect("begin_call")
                .get(0);
            assert_eq!(attempt, Some(1), "a full budget of real calls");
            assert!(
                attempt.unwrap() < MAX_ATTEMPTS,
                "the worker settles RETRY, not DEAD"
            );
            let settled: bool = tx
                .query_one(
                    "SELECT ops.finish_derived_work_v2($1, $2, 'c32-t19', $3, 'RETRY', 'RETRY_WAIT', 30, 60)",
                    &[&pending12, &bound, &generation],
                )
                .expect("finish")
                .get(0);
            assert!(settled);
            assert_eq!(
                job(&mut tx, pending12),
                ("PENDING".into(), 1, Some("RETRY_WAIT".into()))
            );
            tx.rollback().expect("rollback the seeded shapes");
            h.assert_i_slot();
        },
    );
}

/// T20 (slice 2, ruling E1 guard b) — fault: 0190's T6 backoff of one lease (no schedule, no
/// jitter). With attempt 3 and a 30 s lease the schedule is 120 s ± 25 %.
#[test]
fn t6_requeue_backs_off_on_the_retry_schedule() {
    run("t6_requeue_backs_off_on_the_retry_schedule", |mut h| {
        let a = h.tenant();
        let j = h.seed(a, 5.0);
        let c = h.claim("c32-t20").expect("claim");
        assert_eq!(
            h.begin(&DistillLease::of(&c, "c32-t20"), Uuid::new_v4()),
            Some(1)
        );
        h.sql(
            "UPDATE ops.jobs SET attempt = 3, lease_expires_at = clock_timestamp() - interval '2 seconds', \
                    hard_deadline = clock_timestamp() - interval '1 second' WHERE job_id = $1",
            j,
        );
        h.sql(
            "UPDATE ops.provider_slots SET bound_until = clock_timestamp() - interval '1 second' WHERE job_id = $1",
            j,
        );
        assert!(h.claim("c32-t20-sweeper").is_none());
        let backoff: f64 = h
            .admin
            .query_one(
                "SELECT extract(epoch FROM next_retry_at - clock_timestamp())::float8 FROM ops.jobs \
                 WHERE job_id = $1 AND status = 'PENDING' AND last_error_class = 'EXECUTION_UNCERTAIN'",
                &[&j],
            )
            .expect("T6 re-queued the job")
            .get(0);
        assert!(
            (88.0..=150.0).contains(&backoff),
            "T6 must use min(lease * 2^(attempt-1), cap) with jitter, got {backoff:.1}s"
        );
        h.assert_i_slot();
    });
}

/// T21 — ADR-0058 D-T, main-line ruling E1 guard (d): the T6 resend after EXECUTION_UNCERTAIN is a
/// new claim and a new admission, and the admission passes the tenant's §72.3 budget in the same
/// transaction as `ops.begin_call`. Fault: skip `ops.admit_distill_budget` in
/// `jobs::begin_distill_call` → the resend is admitted (attempt 2, a second distill_calls row).
#[test]
fn the_t6_resend_is_admitted_only_within_the_tenant_budget() {
    run(
        "the_t6_resend_is_admitted_only_within_the_tenant_budget",
        |mut h| {
            let one_per_hour = DistillCallBudget {
                window_seconds: 3600.0,
                max_calls: 1,
            };
            let a = h.tenant();
            let j = h.seed(a, 5.0);
            let c1 = h.claim("c32-t21").expect("claim gen 1");
            assert_eq!(
                h.begin_within(
                    &DistillLease::of(&c1, "c32-t21"),
                    Uuid::new_v4(),
                    one_per_hour
                ),
                CallAdmission::Admitted(1)
            );
            // The call's outcome is lost: T5, then T6 after hard_deadline.
            h.sql(
                "UPDATE ops.jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE job_id = $1",
                j,
            );
            assert!(h.claim("c32-t21-sweeper").is_none());
            h.sql(
                "UPDATE ops.jobs SET hard_deadline = clock_timestamp() - interval '1 second' WHERE job_id = $1",
                j,
            );
            h.sql(
                "UPDATE ops.provider_slots SET bound_until = clock_timestamp() - interval '1 second' WHERE job_id = $1",
                j,
            );
            assert!(h.claim("c32-t21-sweeper").is_none(), "T6 backs the job off");
            h.sql(
                "UPDATE ops.jobs SET next_retry_at = clock_timestamp() WHERE job_id = $1",
                j,
            );
            let c2 = h.claim("c32-t21").expect("gen 2: the resend's claim");
            assert_eq!(c2.last_error_class.as_deref(), Some("EXECUTION_UNCERTAIN"));
            assert_eq!(
                h.begin_within(
                    &DistillLease::of(&c2, "c32-t21"),
                    Uuid::new_v4(),
                    one_per_hour
                ),
                CallAdmission::OverBudget,
                "the automatic resend is refused while the tenant budget is spent"
            );
            let (status, state, attempt, ..) = h.job(j);
            assert_eq!(
                (status.as_str(), state.as_deref(), attempt),
                ("PROCESSING", Some(DispatchState::Claimed.as_db_str()), 1),
                "an over-budget admission counts nothing and dispatches nothing"
            );
            assert_eq!(h.calls_of(j), 1);

            // Per tenant: another tenant's first call is not charged to A.
            let b = h.tenant();
            let jb = h.seed(b, 5.0);
            let cb = h.claim("c32-t21-b").expect("tenant B claim");
            assert_eq!(cb.job_id, jb);
            assert_eq!(
                h.begin_within(
                    &DistillLease::of(&cb, "c32-t21-b"),
                    Uuid::new_v4(),
                    one_per_hour
                ),
                CallAdmission::Admitted(1)
            );

            // The window slides: once A's call is older than the window, the resend is admitted.
            h.sql(
                "UPDATE ops.distill_calls SET begun_at = begun_at - interval '2 hours' WHERE job_id = $1",
                j,
            );
            assert_eq!(
                h.begin_within(
                    &DistillLease::of(&c2, "c32-t21"),
                    Uuid::new_v4(),
                    one_per_hour
                ),
                CallAdmission::Admitted(2)
            );
            h.assert_i_slot();
        },
    );
}

/// T21b — ADR-0058 D-T: two admissions of one tenant cannot both read "one call left". Connection
/// X holds an uncommitted admission (budget check + begin_call) of job 1; job 2's admission must
/// wait for it and then see X's call. Fault: drop the tenant advisory lock from
/// `ops.admit_distill_budget` → job 2 counts 0 committed calls and is admitted too.
#[test]
fn a_concurrent_admission_of_one_tenant_sees_the_other_ones_call() {
    run(
        "a_concurrent_admission_of_one_tenant_sees_the_other_ones_call",
        |mut h| {
            let one_per_hour = DistillCallBudget {
                window_seconds: 3600.0,
                max_calls: 1,
            };
            let a = h.tenant();
            let j1 = h.seed(a, 6.0);
            let j2 = h.seed(a, 5.0);
            let c1 = h.claim("c32-t21b-1").expect("claim job 1");
            let c2 = h.claim("c32-t21b-2").expect("claim job 2");
            assert_eq!((c1.job_id, c2.job_id), (j1, j2));
            // dep: PostgreSQL(owner) — connection X holding an open admission of job 1
            let mut x = Client::connect(&h.dsn, NoTls).expect("connection X");
            x.batch_execute("BEGIN").unwrap();
            let within: bool = x
                .query_one("SELECT ops.admit_distill_budget($1, 3600, 1)", &[&a])
                .unwrap()
                .get(0);
            assert!(within);
            let attempt: Option<i32> = x
                .query_one(
                    "SELECT ops.begin_call($1, $2, 'c32-t21b-1', $3, $4, $5)",
                    &[
                        &j1,
                        &a,
                        &c1.claim_generation,
                        &Uuid::new_v4(),
                        &MIN_REMAINING,
                    ],
                )
                .unwrap()
                .get(0);
            assert_eq!(attempt, Some(1));
            let second = std::thread::scope(|scope| {
                let waiter = scope.spawn(|| {
                    h.rt.block_on(jobs::begin_distill_call(
                        &h.private,
                        &DistillLease::of(&c2, "c32-t21b-2"),
                        Uuid::new_v4(),
                        MIN_REMAINING,
                        one_per_hour,
                    ))
                });
                std::thread::sleep(std::time::Duration::from_millis(1500));
                assert!(
                    !waiter.is_finished(),
                    "job 2's admission waits for the tenant's open admission"
                );
                x.batch_execute("COMMIT").unwrap();
                waiter.join().expect("waiter")
            })
            .expect("begin_call must not error");
            assert_eq!(second, CallAdmission::OverBudget);
            assert_eq!(h.calls_of(j2), 0);
            assert_eq!(h.job(j2).2, 0);
            h.assert_i_slot();
        },
    );
}

/// T22 — ADR-0058 D-B, ruling E2 (uncaught fault M2 of the slice-1 review): the free-slot pick
/// never waits on, and never binds, a slot row another transaction holds. Connection X locks the
/// lowest free slot (as a heal or an operator repair would); the claim must return at once on
/// another slot. Fault: the pick without `FOR UPDATE SKIP LOCKED` (M2) → the claim blocks on X's
/// row (and, with the `job_id IS NULL` recheck also dropped, M2+M3, binds it after X ends).
#[test]
fn a_claim_skips_a_slot_row_another_transaction_holds() {
    run(
        "a_claim_skips_a_slot_row_another_transaction_holds",
        |mut h| {
            let a = h.tenant();
            let j = h.seed(a, 5.0);
            let held: i16 = h
                .admin
                .query_one(
                    "SELECT min(slot_no) FROM ops.provider_slots WHERE job_id IS NULL",
                    &[],
                )
                .unwrap()
                .get(0);
            // dep: PostgreSQL(owner) — connection X holding one free slot row
            let mut x = Client::connect(&h.dsn, NoTls).expect("connection X");
            x.batch_execute("BEGIN").unwrap();
            x.query(
                "SELECT 1 FROM ops.provider_slots WHERE slot_no = $1 FOR UPDATE",
                &[&held],
            )
            .unwrap();
            let (claim, waited) = std::thread::scope(|scope| {
                let claimer = scope.spawn(|| {
                    h.rt.block_on(jobs::claim_distill(&h.private, "c32-t22", LEASE, HARD))
                });
                let started = std::time::Instant::now();
                while !claimer.is_finished() && started.elapsed().as_secs_f64() < 3.0 {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                let waited = !claimer.is_finished();
                x.batch_execute("ROLLBACK").unwrap();
                (claimer.join().expect("claimer"), waited)
            });
            assert!(
                !waited,
                "the claim waited on a slot row another transaction holds"
            );
            let claim = claim
                .expect("claim must not error")
                .expect("a free slot is left");
            assert_eq!(claim.job_id, j);
            let bound: i16 = h
                .admin
                .query_one(
                    "SELECT slot_no FROM ops.provider_slots WHERE job_id = $1",
                    &[&j],
                )
                .unwrap()
                .get(0);
            assert_ne!(bound, held, "the held slot is skipped, not bound");
            h.assert_i_slot();
        },
    );
}

/// T23 — §78.2 / ADR-0058 D-M (main-line ruling 2026-10-02 10:35, test 4): the three LIVE
/// capability CHECKs (0048, 0128 x2, widened by 0195) hold exactly `ReasoningCapability::ALL`.
/// Fault: skip 0195 (run against a database where it is not applied) → the CHECKs lack TOOL_CALLS
/// and REASONING_SPLIT.
#[test]
fn reasoning_capability_checks_match_the_rust_closed_set() {
    run(
        "reasoning_capability_checks_match_the_rust_closed_set",
        |mut h| {
            let rust: Vec<String> = ReasoningCapability::ALL
                .iter()
                .map(|c| c.as_str().to_owned())
                .collect();
            for (table, constraint) in [
                (
                    "control.user_reasoning_profiles",
                    "user_reasoning_profiles_capabilities_known",
                ),
                (
                    "control.processor_models",
                    "processor_models_capabilities_check",
                ),
                (
                    "control.reasoning_profiles",
                    "reasoning_profiles_capabilities_check",
                ),
            ] {
                let def: String = h
                    .admin
                    .query_one(
                        "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
                         WHERE conrelid = $1::text::regclass AND conname = $2",
                        &[&table, &constraint],
                    )
                    .unwrap_or_else(|e| panic!("{table}.{constraint}: {e}"))
                    .get(0);
                // Every quoted literal of the CHECK is a capability (both the `<@ ARRAY[` and the
                // `= ANY (ARRAY[` / `IN (` forms quote each value once).
                let db: Vec<String> = def
                    .split('\'')
                    .skip(1)
                    .step_by(2)
                    .map(str::to_owned)
                    .collect();
                assert_eq!(db, rust, "{table}.{constraint}: {def}");
            }
        },
    );
}
