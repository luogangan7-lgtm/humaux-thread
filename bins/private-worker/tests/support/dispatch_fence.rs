//! `private-worker::tests::support::dispatch_fence` — isolation for tests that drive the cross-tenant ADR-0058 distill
//!   claim on the shared dev database: foreign tenants are made invisible to the claim, leftover slots are healed.
//! Depends-on: crates=[postgres]; services=[PostgreSQL(owner) r=[ops.distill_tenant_scheduler, ops.jobs]
//!   w=[ops.provider_slots]]; env=[]; modules=[]
//! Called-by: [private-worker::tests::derived_dispatch_e2e, private-worker::tests::distill_hop_e2e]
//! Invariants: [the fence holds FOR UPDATE on every scheduler row that existed before the test, and the claim takes
//!   tenants FOR UPDATE OF t SKIP LOCKED, so a test only ever serves the tenants it creates; a slot bound to a live
//!   claim refuses the test instead of stealing it]
//! Spec: ADR-0058; §79.2

use postgres::{Client, NoTls};

/// Frees slots left bound by an aborted run (their job is gone or no longer `PROCESSING` under
/// that generation — T9 would heal them after `bound_until`), refuses when a slot is bound to a
/// live claim (a v2 worker is running against this database), then opens the fence connection.
/// Dropping the returned client (or `ROLLBACK` on it) releases the fence.
pub fn open(admin: &mut Client, dsn: &str) -> Result<Client, String> {
    admin
        .batch_execute(
            "UPDATE ops.provider_slots s SET job_id = NULL, claim_generation = NULL, bound_until = NULL \
             WHERE s.job_id IS NOT NULL AND NOT EXISTS ( \
               SELECT 1 FROM ops.jobs j WHERE j.job_id = s.job_id AND j.status = 'PROCESSING' \
                 AND j.claim_generation = s.claim_generation)",
        )
        .map_err(|e| format!("slot heal failed: {e}"))?;
    let bound: i64 = admin
        .query_one(
            "SELECT count(*) FROM ops.provider_slots WHERE job_id IS NOT NULL",
            &[],
        )
        .map_err(|e| format!("slot probe failed: {e}"))?
        .get(0);
    if bound != 0 {
        return Err(format!(
            "{bound} provider slot(s) are bound to live claims — stop every v2 private-worker first"
        ));
    }
    // dep: PostgreSQL(owner) — fence connection holding foreign scheduler rows FOR UPDATE
    let mut fence = Client::connect(dsn, NoTls).map_err(|e| format!("fence connect: {e}"))?;
    fence
        .batch_execute("BEGIN; SELECT tenant_id FROM ops.distill_tenant_scheduler FOR UPDATE;")
        .map_err(|e| format!("fence lock: {e}"))?;
    Ok(fence)
}
