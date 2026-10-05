//! `private-worker::tests::support::double_spend` — the ADR-0058 §7 M3 double-spend report over the attempt ledger
//!   (`ops.distill_calls`) joined to `ops.model_call_ledger`, shared by E8, E10 and the live gate.
//! Depends-on: crates=[humaux-testkit, postgres, uuid]; services=[PostgreSQL(owner) r=[ops.distill_calls,
//!   ops.model_call_ledger] w=[ops.distill_calls, ops.jobs, ops.model_call_ledger]]; env=[HUMAUX_TEST_PG_DSN];
//!   modules=[humaux-testkit]
//! Called-by: [private-worker::tests::derived_dispatch_e2e]
//! Invariants: [one query per figure, scoped to the caller's tenants; a RESERVED or SUCCEEDED call followed by a
//!   later generation of the same job is a duplicate, two generations' call intervals that overlap are an overlap (a
//!   RESERVED call's interval stays open until its claim's hard_deadline, ADR-0058 R6), a SUCCEEDED ledger row with no
//!   distill_calls row is a call outside begin_call]
//! Spec: ADR-0058; §19.1; §67.2

use postgres::GenericClient;
use postgres::types::ToSql;
use uuid::Uuid;

/// ADR-0058 §7 M3 figures for one set of tenants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoubleSpend {
    /// Calls of a job made in a later generation than a call of the same job whose answer was
    /// paid for or unknown (`SUCCEEDED` / `RESERVED`): the Evidence was dispatched again.
    pub duplicates: i64,
    /// Pairs of calls of one job, different generations, whose intervals overlap: answered calls
    /// span `[called_at, called_at + latency_ms]`, a `RESERVED` one (outcome unknown — a killed or
    /// cut worker) stays open until its claim's hard deadline (ADR-0058 R6).
    pub overlaps: i64,
    /// `(job, generation)` groups with more admitted calls than one claim may spend.
    pub per_gen_over_budget: i64,
    /// `PRIVATE_DISTILL_TEXT` ledger rows SUCCEEDED with no `ops.distill_calls` row.
    pub unattributed_succeeded: i64,
    /// Every unattributed ledger row by `(status, error_class)` (refused dispatches, L13 orphans).
    pub unattributed: Vec<(String, Option<String>, i64)>,
}

impl DoubleSpend {
    /// The one line M3/E8/E10 print.
    pub fn line(&self) -> String {
        format!(
            "double_spend duplicates={} overlaps={} per_gen_over_budget={} unattributed_succeeded={} unattributed={:?}",
            self.duplicates,
            self.overlaps,
            self.per_gen_over_budget,
            self.unattributed_succeeded,
            self.unattributed
        )
    }
}

fn count(admin: &mut impl GenericClient, sql: &str, params: &[&(dyn ToSql + Sync)]) -> i64 {
    admin
        .query_one(sql, params)
        .unwrap_or_else(|e| panic!("double_spend query failed: {e}\n{sql}"))
        .get(0)
}

/// `per_gen_budget` = 1 + malformed re-ask budget + empty-retry budget; `hard_deadline_seconds` =
/// the dispatchers' `HUMAUX_PRIVATE_WORKER_DISTILL_HARD_DEADLINE_SECS`.
pub fn report(
    admin: &mut impl GenericClient,
    tenants: &[Uuid],
    per_gen_budget: i64,
    hard_deadline_seconds: f64,
) -> DoubleSpend {
    let tenants = tenants.to_vec();
    let duplicates = count(
        admin,
        "SELECT count(*) FROM ops.distill_calls c2 \
         WHERE c2.tenant_id = ANY($1) AND EXISTS ( \
           SELECT 1 FROM ops.distill_calls c1 \
           JOIN ops.model_call_ledger l1 ON l1.model_call_id = c1.model_call_id \
           WHERE c1.job_id = c2.job_id AND c1.claim_generation < c2.claim_generation \
             AND l1.status IN ('SUCCEEDED', 'RESERVED'))",
        &[&tenants],
    );
    // ADR-0058 R6: a RESERVED call may still be in flight until its claim's hard_deadline.
    // ponytail: the claim's hard_deadline is not kept per generation, so `begun_at + hard_deadline`
    // bounds it from above by the read leg (claim → begin_call, far under the T6 backoff); upgrade:
    // an ops.distill_calls.hard_deadline column if a false overlap ever shows.
    let overlaps = count(
        admin,
        "WITH calls AS ( \
           SELECT c.job_id, c.claim_generation, l.called_at AS starts, \
                  CASE WHEN l.status = 'RESERVED' THEN c.begun_at + make_interval(secs => $2) \
                       ELSE l.called_at + make_interval(secs => coalesce(l.latency_ms, 0) / 1000.0) \
                  END AS ends \
           FROM ops.distill_calls c JOIN ops.model_call_ledger l ON l.model_call_id = c.model_call_id \
           WHERE c.tenant_id = ANY($1)) \
         SELECT count(*) FROM calls c1 JOIN calls c2 ON c2.job_id = c1.job_id \
              AND c2.claim_generation > c1.claim_generation \
         WHERE c1.starts < c2.ends AND c2.starts < c1.ends",
        &[&tenants, &hard_deadline_seconds],
    );
    let per_gen_over_budget = count(
        admin,
        "SELECT count(*) FROM (SELECT job_id, claim_generation FROM ops.distill_calls \
           WHERE tenant_id = ANY($1) GROUP BY 1, 2 HAVING count(*) > $2) g",
        &[&tenants, &per_gen_budget],
    );
    let unattributed: Vec<(String, Option<String>, i64)> = admin
        .query(
            "SELECT l.status, l.error_class, count(*) FROM ops.model_call_ledger l \
             WHERE l.tenant_id = ANY($1) AND l.purpose = 'PRIVATE_DISTILL_TEXT' \
               AND NOT EXISTS (SELECT 1 FROM ops.distill_calls c WHERE c.model_call_id = l.model_call_id) \
             GROUP BY 1, 2 ORDER BY 1, 2",
            &[&tenants],
        )
        .expect("unattributed query")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    let unattributed_succeeded = unattributed
        .iter()
        .filter(|(status, _, _)| status == "SUCCEEDED")
        .map(|(_, _, n)| n)
        .sum();
    DoubleSpend {
        duplicates,
        overlaps,
        per_gen_over_budget,
        unattributed_succeeded,
        unattributed,
    }
}

#[cfg(test)]
mod tests {
    use super::report;
    use postgres::{Client, NoTls};
    use uuid::Uuid;

    /// ADR-0058 R6 — fault: a `RESERVED` row's interval ends at `called_at` (zero length) → a later
    /// generation's call inside the killed claim's window is not an overlap and this is red.
    #[test]
    fn a_reserved_call_overlaps_a_later_generation_until_the_claims_hard_deadline() {
        let Ok(dsn) = std::env::var("HUMAUX_TEST_PG_DSN") else {
            humaux_testkit::skip_or_fail(
                "a_reserved_call_overlaps_a_later_generation_until_the_claims_hard_deadline",
                "HUMAUX_TEST_PG_DSN",
                humaux_testkit::ExternalDep::Postgres,
            );
            return;
        };
        // dep: PostgreSQL(owner) — seeds the two ledgers in one rolled-back transaction
        let mut admin = Client::connect(&dsn, NoTls).expect("owner connection");
        let mut txn = admin.transaction().expect("txn");
        // The report reads two tables only: FK targets and the ledger's insert validation are not
        // what is under test, and every row goes with the rollback.
        // replica-mode: fault setup, rolled back (no tenant row is planted; every row goes with `txn.rollback()`)
        txn.batch_execute("SET LOCAL session_replication_role = replica")
            .expect("replica role");
        let tenant = Uuid::now_v7();
        // Generation 1 of each job was killed mid-call (RESERVED); generation 2 answered 30 s
        // (inside a 120 s hard deadline) or 130 s (past it) after generation 1 began.
        for (job, gen2_after) in [(Uuid::now_v7(), 30.0_f64), (Uuid::now_v7(), 130.0)] {
            let (killed, answered) = (Uuid::now_v7(), Uuid::now_v7());
            txn.execute(
                "INSERT INTO ops.jobs (job_id, tenant_id, job_type, status, next_retry_at, \
                   idempotency_key, payload) \
                 VALUES ($1, $2, 'DERIVED_DISTILL', 'DONE', now(), $1::uuid::text, '{}')",
                &[&job, &tenant],
            )
            .expect("job");
            txn.execute(
                "INSERT INTO ops.distill_calls \
                   (model_call_id, tenant_id, job_id, claim_generation, attempt, begun_at) \
                 VALUES ($1, $3, $4, 1, 1, now() - interval '200 seconds'), \
                        ($2, $3, $4, 2, 2, now() - interval '200 seconds' \
                                            + make_interval(secs => $5))",
                &[&killed, &answered, &tenant, &job, &gen2_after],
            )
            .expect("distill calls");
            txn.execute(
                "INSERT INTO ops.model_call_ledger \
                   (model_call_id, tenant_id, provider, purpose, status, called_at, latency_ms) \
                 VALUES ($1, $3, 'test', 'PRIVATE_DISTILL_TEXT', 'RESERVED', \
                         now() - interval '200 seconds', NULL), \
                        ($2, $3, 'test', 'PRIVATE_DISTILL_TEXT', 'SUCCEEDED', \
                         now() - interval '200 seconds' + make_interval(secs => $4), 1000)",
                &[&killed, &answered, &tenant, &gen2_after],
            )
            .expect("ledger rows");
        }
        let figures = report(&mut txn, &[tenant], 3, 120.0);
        txn.rollback().expect("rollback");
        assert_eq!(figures.overlaps, 1, "{}", figures.line());
        assert_eq!(figures.duplicates, 2, "{}", figures.line());
    }
}
