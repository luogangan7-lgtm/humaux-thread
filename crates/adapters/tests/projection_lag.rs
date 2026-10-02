//! `adapters::tests::projection_lag` — ADR-0057 D-D/D-E: the projection-lag reading (age of the stream's oldest
//!   ISSUED/PROCESSING/RETRY_WAIT ticket, DB clock, same snapshot as the ledger) against a real PostgreSQL ledger and
//!   the real projection worker, judged by the production classifier and block builder.
//! Depends-on: crates=[humaux-adapters, humaux-retrieval, humaux-telemetry, humaux-testkit, sqlx];
//!   services=[PostgreSQL(owner) w=[projection.stream_log], PostgreSQL(role_private_worker) w=[projection.stream_log]];
//!   env=[];
//!   modules=[adapters::stream_repo, adapters::tests::support::a2_fixture, adapters::tests::support::governance_ops,
//!   humaux-testkit, retrieval::completeness, retrieval::envelope, retrieval::planner, telemetry::degrade]
//! Called-by: [cargo-test]
//! Invariants: [the age is read by stream_repo::fetch_ledger_closure (the production read path); a ticket is made old
//!   only by backdating its issued_at as the owner (no state change, so the 0167 guard is not involved); the
//!   WAITING_KEY ticket reaches that state through role_private_worker's legal edges; a missing PG / Qdrant /
//!   gitleaks is a fixture error, never a silent pass]
//! Spec: Baseline §22.4; §52.2; ADR-0057

use std::time::Duration;

use humaux_adapters::stream_repo;
use humaux_retrieval::completeness::{CensusResult, LedgerClosure, classify_for_witness};
use humaux_retrieval::envelope::{LaneStatus, build_projection_block};
use humaux_retrieval::planner::{PlannerDecision, QueryClass};
use humaux_telemetry::degrade::DegradeCode;
use humaux_testkit::run_db_fixture;
use sqlx::types::Uuid;

#[path = "support/a2_fixture.rs"]
mod a2_fixture;
#[path = "support/governance_ops.rs"]
#[allow(dead_code)]
mod governance_ops;
use a2_fixture::{Fixture, Handle};
use governance_ops::stream;

/// The threshold every test judges with (the gateway reads it from `PROJECTION_LAG_SECONDS`).
const THRESHOLD: Duration = Duration::from_secs(60);
/// How far a ticket is pushed into the past — far beyond the threshold.
const BACKDATE_SECS: i64 = 3_600;

/// One fresh workspace with one remembered Evidence (one ISSUED ticket) and one memory of it.
fn one_pending_ticket(h: &mut Handle) -> Uuid {
    let ws = h.workspace();
    h.fan_out(ws, "lag", 1);
    ws
}

/// Pushes every ticket of `ws`'s stream `BACKDATE_SECS` into the past. Owner write of
/// `issued_at` only: the state is unchanged, so the 0167 transition guard returns early.
fn backdate(h: &mut Handle, ws: Uuid) {
    let n = h
        .admin
        .execute(
            "UPDATE projection.stream_log \
                SET issued_at = issued_at - make_interval(secs => $3) \
              WHERE tenant_id = $1 AND scope_id = $2",
            &[&h.tenant_id, &ws, &(BACKDATE_SECS as f64)],
        )
        .expect("backdate issued_at");
    assert!(n >= 1, "the fixture must have a ticket to backdate");
}

fn closure(h: &Handle, ws: Uuid) -> LedgerClosure {
    let reader = h.scope(ws);
    h.rt.block_on(stream_repo::fetch_ledger_closure(
        &h.retrieval,
        &stream(h.tenant_id, ws),
        &reader,
    ))
    .expect("ledger closure through the production read path")
}

fn class_of(ledger: &LedgerClosure) -> (&'static str, &'static str) {
    classify_for_witness(
        &PlannerDecision::Class(QueryClass::Semantic),
        LaneStatus::Ok,
        &CensusResult::ok_without_enumeration(),
        ledger,
        0,
        THRESHOLD,
    )
}

/// Test 23: a pending ticket older than the threshold lags — reading, class and degradation.
/// Fault: remove the `oldest_pending_age_secs` column (None ⇒ never lags).
#[test]
fn a_pending_ticket_older_than_the_threshold_lags() {
    run_db_fixture::<Fixture, _>("a_pending_ticket_older_than_the_threshold_lags", |mut h| {
        let ws = one_pending_ticket(&mut h);
        let fresh = closure(&h, ws);
        let fresh_age = fresh.points().oldest_pending_age_secs;
        println!("lag fresh: oldest_pending_age_secs={fresh_age:?}");
        assert!(
            fresh_age.is_some_and(|age| age < THRESHOLD.as_secs()),
            "a just-issued ticket is pending and young: {fresh_age:?}"
        );
        assert!(!fresh.lagging(THRESHOLD));

        backdate(&mut h, ws);
        let ledger = closure(&h, ws);
        let age = ledger.points().oldest_pending_age_secs;
        println!("lag backdated: oldest_pending_age_secs={age:?}");
        assert!(
            age.is_some_and(|age| age >= BACKDATE_SECS as u64),
            "the DB-clock age must cover the backdate: {age:?}"
        );
        assert!(
            ledger.is_closed(),
            "A1 holds: the ticket is pending, not lost"
        );
        assert!(ledger.lagging(THRESHOLD));
        assert_eq!(class_of(&ledger), ("cannot_establish", "projection_lag"));
        let block = build_projection_block(&ledger, Some(0), THRESHOLD);
        assert!(
            block.degradations.contains(&DegradeCode::ProjectionLag),
            "{:?}",
            block.degradations
        );
    });
}

/// Test 24: a WAITING_KEY ticket, however old, is not lag (DOD-014 names it as its own signal).
/// Fault: add WAITING_KEY to the age filter.
#[test]
fn a_waiting_key_ticket_never_lags() {
    run_db_fixture::<Fixture, _>("a_waiting_key_ticket_never_lags", |mut h| {
        let ws = one_pending_ticket(&mut h);
        backdate(&mut h, ws);
        // ISSUED -> PROCESSING -> WAITING_KEY: the two legal edges of role_private_worker (0167
        // guard), under the tenant GUC its FORCE-RLS view needs.
        let tenant = h.tenant_id.to_string();
        let mut txn = h.admin.transaction().expect("owner txn");
        // dep: PostgreSQL(role_private_worker) — role switch for the two legal state edges
        txn.batch_execute("SET LOCAL ROLE role_private_worker")
            .expect("act as role_private_worker");
        txn.execute(
            "SELECT set_config('humaux.tenant_id', $1, true)",
            &[&tenant],
        )
        .expect("tenant GUC");
        for state in ["PROCESSING", "WAITING_KEY"] {
            let n = txn
                .execute(
                    "UPDATE projection.stream_log SET state = $3 \
                      WHERE tenant_id = $1 AND scope_id = $2",
                    &[&h.tenant_id, &ws, &state],
                )
                .expect("legal private-worker transition");
            assert_eq!(n, 1, "one ticket moves to {state}");
        }
        txn.commit().expect("commit transitions");

        let ledger = closure(&h, ws);
        println!(
            "lag waiting_key: oldest_pending_age_secs={:?} pending={}",
            ledger.points().oldest_pending_age_secs,
            ledger.counts().pending()
        );
        assert_eq!(
            ledger.counts().pending(),
            1,
            "WAITING_KEY is still pending for A1"
        );
        assert_eq!(ledger.points().oldest_pending_age_secs, None);
        assert!(!ledger.lagging(THRESHOLD));
        assert_ne!(class_of(&ledger).1, "projection_lag");
        let block = build_projection_block(&ledger, Some(0), THRESHOLD);
        assert!(
            !block.degradations.contains(&DegradeCode::ProjectionLag),
            "{:?}",
            block.degradations
        );
    });
}

/// Test 25: the real worker settles the backdated ticket and the lag clears — the settled row
/// keeps its old `issued_at` and must not count. Fault: age over every state (`min(issued_at)`
/// without the FILTER).
#[test]
fn settling_the_ticket_clears_lag() {
    run_db_fixture::<Fixture, _>("settling_the_ticket_clears_lag", |mut h| {
        let ws = one_pending_ticket(&mut h);
        backdate(&mut h, ws);
        assert!(closure(&h, ws).lagging(THRESHOLD), "precondition: lagging");

        h.drain(ws);
        let ledger = closure(&h, ws);
        println!(
            "lag settled: oldest_pending_age_secs={:?} done={}",
            ledger.points().oldest_pending_age_secs,
            ledger.counts().done()
        );
        assert_eq!(ledger.points().oldest_pending_age_secs, None);
        assert!(!ledger.lagging(THRESHOLD));
        assert_eq!(class_of(&ledger), ("semantic_bounded", "none"));
        let reader = h.scope(ws);
        let settled = h.assert_closed("after_settle", ws, &reader);
        assert_eq!(settled.done, 1);
    });
}
