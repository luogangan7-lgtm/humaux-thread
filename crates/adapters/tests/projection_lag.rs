//! `adapters::tests::projection_lag` — ADR-0057 D-D/D-E: the projection-lag reading (age of the stream's oldest
//!   ISSUED/PROCESSING/RETRY_WAIT ticket, DB clock, same snapshot as the ledger) against a real PostgreSQL ledger and
//!   the real projection worker, judged by the production classifier and block builder.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-projection, humaux-retrieval, humaux-telemetry,
//!   humaux-testkit, postgres, sqlx, tokio];
//!   services=[PostgreSQL(owner) w=[control.private_reasoning_domains, control.tenants, ops.outbox, private.events,
//!   private.evidence_objects, private.memory_evidence, private.memory_records, projection.stream_checkpoints,
//!   projection.stream_log] r=[ops.commit_seq_seq], PostgreSQL(role_maintenance),
//!   PostgreSQL(role_private_worker) w=[projection.stream_log],
//!   PostgreSQL(role_retrieval_worker)];
//!   env=[];
//!   modules=[adapters::maintenance_repo, adapters::postgres, adapters::stream_repo, adapters::tests::support::a2_fixture,
//!   adapters::tests::support::governance_ops, adapters::tests::support::throwaway_db, domain::identity, domain::ids,
//!   humaux-testkit, projection::stream, retrieval::completeness, retrieval::envelope, retrieval::planner,
//!   telemetry::degrade]
//! Called-by: [cargo-test]
//! Invariants: [the age is read by stream_repo::fetch_ledger_closure (the production read path); a ticket is made old
//!   only by backdating its issued_at as the owner (no state change, so the 0167 guard is not involved); the
//!   WAITING_KEY ticket reaches that state through role_private_worker's legal edges; a missing PG / Qdrant /
//!   gitleaks is a fixture error, never a silent pass; the sweep_lost and reissue readings (ADR-0062 D-L/D-N) run in
//!   their own throwaway database, never on the shared dev database]
//! Spec: Baseline §15.2; §22.4; §52.2; ADR-0057; ADR-0062 D-L; ADR-0062 D-N

use std::time::Duration;

use humaux_adapters::postgres::{MaintenanceDbPool, RetrievalWorkerDbPool};
use humaux_adapters::{maintenance_repo, stream_repo};
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId};
use humaux_domain::ids::{TenantId, WorkspaceId};
use humaux_projection::stream::StreamKey;
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
#[path = "support/throwaway_db.rs"]
#[allow(dead_code)]
mod throwaway_db;
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

/// libpq `options=-c role=X` on the owner DSN (the `stream_repo.rs` fixture form).
fn as_role(dsn: &str, role: &str) -> String {
    let sep = if dsn.contains('?') { '&' } else { '?' };
    format!("{dsn}{sep}options=-c%20role%3D{role}")
}

/// T-L4 (ADR-0062 D-L, ruling E13): an orphan ISSUED ticket older than the threshold reads as
/// `PROJECTION_LAG`; after `sweep_lost` it is LOST, the lag clears (ADR-0057 D-D counts only pending
/// tickets) and card 31's recall shows the loss as unsettled slack and a ratio below 1, judged by the
/// production classifier and block builder. Fault: the sweep leaves the ticket ISSUED ⇒ the lag stays
/// and `points_unsettled` reads 0 ⇒ red.
#[test]
fn a_swept_orphan_trades_lag_for_unsettled_slack_and_a_ratio_below_one() {
    run_db_fixture::<LostFixture, _>(
        "a_swept_orphan_trades_lag_for_unsettled_slack_and_a_ratio_below_one",
        |mut h| {
            let read = |h: &LostHandle| {
                h.rt.block_on(stream_repo::fetch_ledger_closure(
                    &h.retrieval,
                    &h.key,
                    &h.reader,
                ))
                .expect("ledger closure through the production read path")
            };
            let before = read(&h);
            assert!(
                before.lagging(THRESHOLD),
                "an orphan past the threshold lags"
            );
            assert_eq!(class_of(&before), ("cannot_establish", "projection_lag"));

            let swept =
                h.rt.block_on(stream_repo::sweep_lost(
                    &h.maintenance,
                    h.tenant,
                    THRESHOLD,
                    10,
                ))
                .expect("sweep as role_maintenance");
            assert_eq!(swept, 1, "the orphan is swept");
            let state: String = h
                .admin
                .query_one(
                    "SELECT state FROM projection.stream_log WHERE tenant_id = $1",
                    &[&h.tenant],
                )
                .expect("ticket")
                .get(0);
            assert_eq!(state, "LOST");

            let after = read(&h);
            let block = build_projection_block(&after, Some(0), THRESHOLD);
            println!(
                "lost reading: lagging={} unsettled={} ratio={:?} degradations={:?}",
                after.lagging(THRESHOLD),
                block.value.points_unsettled,
                block.value.completeness_ratio,
                block.degradations
            );
            assert!(
                !after.lagging(THRESHOLD),
                "LOST is not pending, so it is not lag"
            );
            assert!(
                !block.degradations.contains(&DegradeCode::ProjectionLag),
                "{:?}",
                block.degradations
            );
            assert_eq!(
                block.value.points_unsettled, 1,
                "the lost point is unsettled slack"
            );
            assert!(
                block.value.completeness_ratio.is_some_and(|r| r < 1.0),
                "the missing point keeps the ratio below 1: {:?}",
                block.value.completeness_ratio
            );
        },
    );
}

/// T-L3 (ADR-0062 D-L/D-N, the DB half): the orphan reads as lag, then as LOST (no lag, one unsettled point), then —
/// after the reissue door — as one point in flight with nothing unsettled: the drain puts a fresh ticket on the
/// orphan's own stream. Fault: the sweep leaves the ticket ISSUED ⇒ it is still in flight, the door has nothing to
/// drain ⇒ red.
#[test]
fn an_orphan_reads_as_lag_then_lost_then_in_flight_after_reissue() {
    run_db_fixture::<LostFixture, _>(
        "an_orphan_reads_as_lag_then_lost_then_in_flight_after_reissue",
        |mut h| {
            let block = |h: &LostHandle| {
                let closure =
                    h.rt.block_on(stream_repo::fetch_ledger_closure(
                        &h.retrieval,
                        &h.key,
                        &h.reader,
                    ))
                    .expect("ledger closure through the production read path");
                let lagging = closure.lagging(THRESHOLD);
                let b = build_projection_block(&closure, Some(0), THRESHOLD).value;
                println!(
                    "orphan reading: lagging={lagging} F={} Q={}",
                    b.points_in_flight, b.points_unsettled
                );
                (lagging, b.points_in_flight, b.points_unsettled)
            };
            assert_eq!(block(&h), (true, 1, 0), "an orphan past the threshold lags");
            let swept =
                h.rt.block_on(stream_repo::sweep_lost(
                    &h.maintenance,
                    h.tenant,
                    THRESHOLD,
                    10,
                ))
                .expect("sweep as role_maintenance");
            assert_eq!(swept, 1, "the orphan is swept");
            assert_eq!(
                block(&h),
                (false, 0, 1),
                "LOST: no lag, one unsettled point"
            );
            // The fixture wrote commit_seq 1 by hand; the door draws from the sequence.
            h.admin
                .batch_execute("SELECT setval('ops.commit_seq_seq', 100)")
                .expect("move the commit sequence past the hand-written seq");
            let reissue = |h: &LostHandle| {
                h.rt.block_on(maintenance_repo::reissue_unsettled_tickets(
                    &h.maintenance,
                    h.tenant,
                    THRESHOLD,
                    10,
                ))
                .expect("reissue door as role_maintenance")
            };
            // ADR-0062 D-N (0223): the cool-down counts from the sweep's `lost_at`, so nothing is issued yet; the
            // owner then moves that clock past the cool-down, as the wall clock would.
            assert_eq!(reissue(&h), 0, "swept just now: inside the cool-down");
            h.admin
                .execute(
                    "UPDATE projection.stream_log SET lost_at = lost_at - make_interval(secs => $3) \
                      WHERE tenant_id = $1 AND scope_id = $2 AND state = 'LOST'",
                    &[
                        &h.tenant,
                        &h.key.scope_id,
                        &(THRESHOLD.as_secs_f64() + 1.0),
                    ],
                )
                .expect("age the LOST transition past the cool-down");
            assert_eq!(reissue(&h), 1, "one fresh ticket for the lost point");
            assert_eq!(
                block(&h),
                (false, 1, 0),
                "after the reissue the point is in flight and nothing is unsettled"
            );
            let on_stream: i64 = h
                .admin
                .query_one(
                    "SELECT count(*) FROM projection.stream_log WHERE tenant_id = $1 AND scope_id = $2 \
                       AND state = 'ISSUED'",
                    &[&h.tenant, &h.key.scope_id],
                )
                .expect("fresh ticket")
                .get(0);
            assert_eq!(
                on_stream, 1,
                "the fresh ticket sits on the orphan's own stream"
            );
        },
    );
}

/// One throwaway database holding one tenant, one workspace stream with one orphan ISSUED ticket
/// issued `BACKDATE_SECS` ago, and the TENANT_SHARED memory its Evidence carries. The Evidence's
/// `EVIDENCE_ACCEPTED` carrier is DONE: a PENDING one is still being distilled, which the claim holds
/// back on purpose and `sweep_lost` leaves ISSUED (ADR-0062 D-L).
struct LostHandle {
    rt: tokio::runtime::Runtime,
    retrieval: RetrievalWorkerDbPool,
    maintenance: MaintenanceDbPool,
    admin: postgres::Client,
    tenant: Uuid,
    key: StreamKey,
    reader: AuthorizationScope,
    _db: throwaway_db::ThrowawayDb,
}

struct LostFixture;

impl humaux_testkit::DbIntegrationFixture for LostFixture {
    type Handle = LostHandle;

    fn isolate() -> Result<Self::Handle, humaux_testkit::DbFixtureSkipReason> {
        use humaux_testkit::DbFixtureSkipReason as Skip;
        let setup = |e: &dyn std::fmt::Display| Skip::IsolationSetupFailed(e.to_string());
        let db = throwaway_db::create("c35_lag")?;
        let dsn = db.dsn();
        // dep: PostgreSQL(owner) — seeds the throwaway database
        let mut admin = postgres::Client::connect(&dsn, postgres::NoTls).map_err(|e| setup(&e))?;
        let tenant: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ('c35 lost reading') RETURNING tenant_id",
                &[],
            )
            .map_err(|e| setup(&e))?
            .get(0);
        let key = StreamKey::new(
            TenantId(tenant),
            "workspace",
            Uuid::new_v4(),
            "code",
            "retrieval_card",
            "v1",
        );
        let k = [
            &key.scope_kind,
            &key.domain,
            &key.projection_kind,
            &key.projection_version,
        ];
        // §8.6: the memory and its evidence link commit together (deferred orphan check).
        let mut txn = admin.transaction().map_err(|e| setup(&e))?;
        txn.execute(
            "INSERT INTO projection.stream_checkpoints (tenant_id, scope_kind, scope_id, domain, \
               projection_kind, projection_version, issued_highwater) VALUES ($1,$2,$3,$4,$5,$6,1)",
            &[&tenant, k[0], &key.scope_id, k[1], k[2], k[3]],
        )
        .map_err(|e| setup(&e))?;
        txn.execute(
            "INSERT INTO projection.stream_log (tenant_id, scope_kind, scope_id, domain, projection_kind, \
               projection_version, stream_seq, commit_seq, state, issued_at) \
             VALUES ($1,$2,$3,$4,$5,$6,1,1,'ISSUED', now() - make_interval(secs => $7))",
            &[&tenant, k[0], &key.scope_id, k[1], k[2], k[3], &(BACKDATE_SECS as f64)],
        )
        .map_err(|e| setup(&e))?;
        txn.batch_execute(&format!(
            "WITH d AS (INSERT INTO control.private_reasoning_domains (tenant_id, name) \
                          VALUES ('{tenant}', 'c35 lost') RETURNING reasoning_domain_id), \
                  e AS (INSERT INTO private.evidence_objects (tenant_id, evidence_kind, payload_sha256, \
                          data_class, origin_class, visibility_class, reasoning_domain_id) \
                        SELECT '{tenant}', 'EVENT', sha256(convert_to('c35-lost', 'UTF8')), 'INTERNAL', \
                          'DirectUserInput', 'TENANT_SHARED', reasoning_domain_id FROM d RETURNING evidence_id), \
                  ev AS (INSERT INTO private.events (event_id, event_kind, payload) \
                         SELECT evidence_id, 'USER_MESSAGE', '{{}}' FROM e), \
                  o AS (INSERT INTO ops.outbox (tenant_id, commit_seq, stream_seq, event_type, evidence_id, \
                          status) SELECT '{tenant}', 1, 1, 'EVIDENCE_ACCEPTED', evidence_id, 'DONE' FROM e), \
                  m AS (INSERT INTO private.memory_records (tenant_id, memory_type, content, visibility_class, \
                          authority_class, confidence, status, asserted_at) \
                        VALUES ('{tenant}', 'NOTE', '{{\"title\":\"t\"}}', 'TENANT_SHARED', 'PrivateKnowledge', \
                          0.9, 'active', now()) RETURNING memory_id) \
             INSERT INTO private.memory_evidence (memory_id, evidence_id, role, ordinal) \
             SELECT m.memory_id, e.evidence_id, 'PRIMARY', 0 FROM m, e"
        ))
        .map_err(|e| setup(&e))?;
        txn.commit().map_err(|e| setup(&e))?;
        let rt = tokio::runtime::Runtime::new().map_err(|e| setup(&e))?;
        let retrieval = rt
            // dep: PostgreSQL(role_retrieval_worker) — the production ledger read
            .block_on(RetrievalWorkerDbPool::connect(&as_role(
                &dsn,
                "role_retrieval_worker",
            )))
            .map_err(|e| setup(&e))?;
        let maintenance = rt
            // dep: PostgreSQL(role_maintenance) — the sweep
            .block_on(MaintenanceDbPool::connect(&as_role(
                &dsn,
                "role_maintenance",
            )))
            .map_err(|e| setup(&e))?;
        let reader = AuthorizationScope::new(
            TenantId(tenant),
            PrincipalId::new(),
            None,
            BoundedSet::new(Vec::<WorkspaceId>::new()).map_err(|e| setup(&format!("{e:?}")))?,
        );
        Ok(LostHandle {
            rt,
            retrieval,
            maintenance,
            admin,
            tenant,
            key,
            reader,
            _db: db,
        })
    }
}
