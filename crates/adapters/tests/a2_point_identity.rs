//! `adapters::tests::a2_point_identity` — ADR-0057 D-A/D-B/D-L/D-M: §23.1② A2 in the point unit, against a real
//!   PostgreSQL ledger, the real `projection.stream_point_ledger` definer, the real projection worker and a real Qdrant;
//!   and the Q drain (ADR-0062 D-N): the reissue door on throwaway databases.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-telemetry, humaux-testkit, postgres, serde_json, sqlx];
//!   services=[PostgreSQL(owner) r=[ops.commit_seq_seq, projection.ticket_reissues] w=[control.users,
//!   control.workspaces, ops.outbox, private.evidence_objects, private.memory_evidence,
//!   projection.stream_checkpoints, projection.stream_log] x=[private.memory_subject_visibility_ok],
//!   PostgreSQL(role_gateway) r=[private.memory_records] x=[projection.stream_point_ledger],
//!   PostgreSQL(role_maintenance), PostgreSQL(role_retrieval_worker) w=[projection.stream_log], Qdrant(*)]; env=[];
//!   modules=[adapters::affect_repo, adapters::maintenance_repo, adapters::retrieve, adapters::stream_repo,
//!   adapters::tests::support::a2_fixture, adapters::tests::support::governance_ops,
//!   adapters::tests::support::throwaway_db, domain::affect, domain::authority, domain::confirm, domain::ids,
//!   humaux-testkit, telemetry::degrade]
//! Called-by: [cargo-test]
//! Invariants: [both sides of A2 are the production producers: stream_repo::fetch_ledger_closure (the 0189 definer)
//!   and retrieve::visible_count_of_version (the caller-scoped Qdrant count); governance ops run through the real
//!   memory_governance_repo ops; throwaway tenant + collection cleaned up on Drop; a missing PG / Qdrant / gitleaks is
//!   a fixture error, never a silent pass; every reissue runs in its own throwaway database, never on the shared dev
//!   database (ADR-0062 E8)]
//! Spec: Baseline §23.1②; §22.4; §79.2; ADR-0049; ADR-0057; ADR-0057 D-H; ADR-0062 D-M; ADR-0062 D-N
//!
//! Every test prints `a2 <label>: visible L F Q U done` so a red run names the reading that moved.

use std::time::Duration;

use humaux_adapters::affect_repo;
use humaux_adapters::maintenance_repo;
use humaux_adapters::retrieve::{IndexFace, stream_count_of_version};
use humaux_domain::affect::MoodHalfLife;
use humaux_domain::authority::MemoryId;
use humaux_domain::confirm::DestructiveOp;
use humaux_domain::ids::WorkspaceId;
use humaux_telemetry::degrade::DegradeCode;
use humaux_testkit::run_db_fixture;
use sqlx::types::Uuid;

#[path = "support/a2_fixture.rs"]
mod a2_fixture;
#[path = "support/governance_ops.rs"]
mod governance_ops;
#[path = "support/throwaway_db.rs"]
mod throwaway_db;
use a2_fixture::{Fixture, Handle, TENANT_SHARED};
use governance_ops::stream;

/// Two TENANT_SHARED memories (M1 on E1, M2 on E2) projected on a fresh workspace.
fn two_memories(h: &mut Handle) -> (Uuid, Uuid, Uuid) {
    let ws = h.workspace();
    let e1 = h.evidence(ws, "a2 m1");
    let m1 = h.memory(e1, "a2 m1", TENANT_SHARED);
    let e2 = h.evidence(ws, "a2 m2");
    let m2 = h.memory(e2, "a2 m2", TENANT_SHARED);
    h.drain(ws);
    let reader = h.scope(ws);
    let b = h.assert_closed("seeded", ws, &reader);
    assert_eq!((b.visible, b.points_settled), (Some(2), 2));
    (ws, m1, m2)
}

// ---- test 16: the identity after each governance op (real ops + real worker) ----
// Fault for all five: `build_projection_block` judges on ticket `done` again — every one reads
// loss or overshoot (lifecycle tickets add rows that add 0 / +1 / -1 points).

#[test]
fn identity_holds_after_supersede() {
    run_db_fixture::<Fixture, _>("identity_holds_after_supersede", |mut h| {
        let (ws, m1, m2) = two_memories(&mut h);
        let reader = h.scope(ws);
        governance_ops::supersede(&h.rt, &h.gateway, &reader, &stream(h.tenant_id, ws), m1, m2)
            .expect("supersede");
        h.drain(ws);
        let b = h.assert_closed("after_supersede", ws, &reader);
        assert_eq!((b.visible, b.points_settled, b.done), (Some(1), 1, 3));
    });
}

#[test]
fn identity_holds_after_restore() {
    run_db_fixture::<Fixture, _>("identity_holds_after_restore", |mut h| {
        let (ws, m1, m2) = two_memories(&mut h);
        let reader = h.scope(ws);
        let key = stream(h.tenant_id, ws);
        governance_ops::supersede(&h.rt, &h.gateway, &reader, &key, m1, m2).expect("supersede");
        h.drain(ws);
        governance_ops::restore(&h.rt, &h.gateway, &reader, &key, m1).expect("restore");
        h.drain(ws);
        let b = h.assert_closed("after_restore", ws, &reader);
        assert_eq!((b.visible, b.points_settled, b.done), (Some(2), 2, 4));
    });
}

#[test]
fn identity_holds_after_archive() {
    run_db_fixture::<Fixture, _>("identity_holds_after_archive", |mut h| {
        let (ws, m1, _) = two_memories(&mut h);
        let reader = h.scope(ws);
        let key = stream(h.tenant_id, ws);
        governance_ops::archive(
            &h.rt,
            &h.gateway,
            &reader,
            &key,
            m1,
            DestructiveOp::MemoryArchive,
        )
        .expect("archive");
        h.drain(ws);
        let b = h.assert_closed("after_archive", ws, &reader);
        assert_eq!((b.visible, b.points_settled, b.done), (Some(2), 2, 3));
    });
}

#[test]
fn identity_holds_after_unarchive() {
    run_db_fixture::<Fixture, _>("identity_holds_after_unarchive", |mut h| {
        let (ws, m1, _) = two_memories(&mut h);
        let reader = h.scope(ws);
        let key = stream(h.tenant_id, ws);
        governance_ops::archive(
            &h.rt,
            &h.gateway,
            &reader,
            &key,
            m1,
            DestructiveOp::MemoryArchive,
        )
        .expect("archive");
        h.drain(ws);
        governance_ops::archive(
            &h.rt,
            &h.gateway,
            &reader,
            &key,
            m1,
            DestructiveOp::MemoryUnarchive,
        )
        .expect("unarchive");
        h.drain(ws);
        let b = h.assert_closed("after_unarchive", ws, &reader);
        assert_eq!((b.visible, b.points_settled, b.done), (Some(2), 2, 4));
    });
}

#[test]
fn identity_holds_after_correct() {
    run_db_fixture::<Fixture, _>("identity_holds_after_correct", |mut h| {
        let (ws, m1, _) = two_memories(&mut h);
        let reader = h.scope(ws);
        governance_ops::correct(
            &h.rt,
            &h.gateway,
            &reader,
            &stream(h.tenant_id, ws),
            m1,
            "fixed",
        )
        .expect("correct");
        h.drain(ws);
        // Two tickets per correct (ADR-0057 D-B): E2 projects M2, E1 retires M1.
        let b = h.assert_closed("after_correct", ws, &reader);
        assert_eq!((b.visible, b.points_settled, b.done), (Some(2), 2, 4));
    });
}

/// test 17 — one Evidence, three memories: one ticket, three points. Fault: ticket-unit judge
/// (`visible 3 > done 1 + pending 0` ⇒ Inconsistent).
#[test]
fn one_evidence_three_memories_is_closed() {
    run_db_fixture::<Fixture, _>("one_evidence_three_memories_is_closed", |mut h| {
        let ws = h.workspace();
        h.fan_out(ws, "a2 fan-out", 3);
        h.drain(ws);
        let reader = h.scope(ws);
        let b = h.assert_closed("fan_out_1_to_3", ws, &reader);
        assert_eq!((b.visible, b.points_settled, b.done), (Some(3), 3, 1));
        assert_eq!(b.completeness_ratio, Some(1.0));
    });
}

/// test 18 — after `correct`, M1's point is retired (ADR-0057 D-B through the ADR-0049 path).
/// Fault: remove the E1 `issue_lifecycle_ticket` call in `correct_atomically`.
#[test]
fn correct_retires_the_corrected_versions_point() {
    run_db_fixture::<Fixture, _>("correct_retires_the_corrected_versions_point", |mut h| {
        let (ws, m1, _) = two_memories(&mut h);
        assert_eq!(h.points_of(m1), 1, "precondition: M1 is projected");
        let reader = h.scope(ws);
        governance_ops::correct(
            &h.rt,
            &h.gateway,
            &reader,
            &stream(h.tenant_id, ws),
            m1,
            "fixed",
        )
        .expect("correct");
        h.drain(ws);
        assert_eq!(h.points_of(m1), 0, "M1's point is deleted from Qdrant");
        assert!(!h.registry_live(m1), "M1's registry row is retired");
    });
}

/// test 18b — undo of a correction: M2's deactivation carries its own ticket (ADR-0057 D-B).
/// Fault: remove the restore-branch `issue_lifecycle_ticket(…, successor)` ⇒ M2's point stays,
/// `visible = L+1` (Inconsistent).
#[test]
fn identity_holds_after_correct_then_restore() {
    run_db_fixture::<Fixture, _>("identity_holds_after_correct_then_restore", |mut h| {
        let (ws, m1, _) = two_memories(&mut h);
        let reader = h.scope(ws);
        let key = stream(h.tenant_id, ws);
        let corrected = governance_ops::correct(&h.rt, &h.gateway, &reader, &key, m1, "fixed")
            .expect("correct");
        h.drain(ws);
        governance_ops::restore(&h.rt, &h.gateway, &reader, &key, m1).expect("undo the correction");
        h.drain(ws);
        let m2 = corrected.new_memory_id.0;
        h.assert_closed("after_correct_undo", ws, &reader);
        assert_eq!(h.points_of(m2), 0, "the correction's M2 point is retired");
        assert!(!h.registry_live(m2));
        assert_eq!(h.points_of(m1), 1, "M1 is projected again");
    });
}

/// test 19 — a pending ticket over a projected 1→3 fan-out: every memory of its Evidence may hold
/// its point (F = 3), so `visible = 3 > L = 0` is in flight, not inconsistent; the worker then
/// closes it. Fault: the definer drops the pending filter (F = 0 ⇒ Inconsistent, ratio null).
#[test]
fn mid_flight_ticket_reads_in_flight_then_closed() {
    run_db_fixture::<Fixture, _>("mid_flight_ticket_reads_in_flight_then_closed", |mut h| {
        let ws = h.workspace();
        let memories = h.fan_out(ws, "a2 in flight", 3);
        h.drain(ws);
        let reader = h.scope(ws);
        governance_ops::archive(
            &h.rt,
            &h.gateway,
            &reader,
            &stream(h.tenant_id, ws),
            memories[0],
            DestructiveOp::MemoryArchive,
        )
        .expect("archive issues a ticket on the shared evidence");
        let in_flight = h.reading("in_flight", ws, &reader);
        assert_eq!(
            (
                in_flight.value.points_settled,
                in_flight.value.points_in_flight
            ),
            (0, 3)
        );
        assert!(in_flight.degradations.is_empty());
        assert!(
            in_flight.value.completeness_ratio.is_some(),
            "in flight keeps a ratio"
        );
        assert!(!in_flight.value.current);
        h.drain(ws);
        h.assert_closed("in_flight_drained", ws, &reader);
    });
}

/// test 20 — the card-30 debt: W1 holds 6 points, W2 23; a W1 read counts W1's stream only.
/// Fault: drop the workspace term from `family_probe` (visible 29 ⇒ overshoot).
#[test]
fn two_workspaces_count_only_their_own_stream() {
    run_db_fixture::<Fixture, _>("two_workspaces_count_only_their_own_stream", |mut h| {
        let w1 = h.workspace();
        let w2 = h.workspace();
        h.fan_out(w1, "a2 w1", 6);
        h.fan_out(w2, "a2 w2", 23);
        h.drain(w1);
        h.drain(w2);
        let b1 = h.assert_closed("w1", w1, &h.scope(w1));
        assert_eq!((b1.visible, b1.points_settled), (Some(6), 6));
        let b2 = h.assert_closed("w2", w2, &h.scope(w2));
        assert_eq!((b2.visible, b2.points_settled), (Some(23), 23));
    });
}

/// test 20b — supersede and correct requested from W2 on memories projected in W1 land on W1's
/// stream (ADR-0057 D-M). Fault: `home_stream` returns the request stream ⇒ M1's W1 point is
/// never retired ⇒ W1 overshoot.
#[test]
fn supersede_from_another_workspace_keeps_both_streams_closed() {
    run_db_fixture::<Fixture, _>(
        "supersede_from_another_workspace_keeps_both_streams_closed",
        |mut h| {
            let w1 = h.workspace();
            let w2 = h.workspace();
            let user = h.gov.user_id;
            let e1 = h.evidence(w1, "a2 home m1");
            let m1 = h.memory(e1, "a2 home m1", TENANT_SHARED);
            let e2 = h.evidence(w1, "a2 home successor");
            let successor = h.memory(e2, "a2 home successor", TENANT_SHARED);
            let e3 = h.evidence(w1, "a2 home private");
            let private = h.memory(e3, "a2 home private", ("USER_PRIVATE", Some(user), None));
            h.drain(w1);
            let from_w2 = h.scope(w2);
            let w2_stream = stream(h.tenant_id, w2);
            governance_ops::supersede(&h.rt, &h.gateway, &from_w2, &w2_stream, m1, successor)
                .expect("supersede from W2");
            governance_ops::correct(&h.rt, &h.gateway, &from_w2, &w2_stream, private, "fixed")
                .expect("correct from W2");
            h.drain(w1);
            h.drain(w2);
            let b1 = h.assert_closed("home_w1", w1, &h.scope(w1));
            assert_eq!(b1.points_settled, 2, "successor + the correction's M2");
            let b2 = h.assert_closed("request_w2", w2, &from_w2);
            assert_eq!(
                (b2.visible, b2.done),
                (Some(0), 0),
                "W2 holds nothing of W1's"
            );
            assert_eq!(h.points_of(m1), 0, "the superseded M1's point is retired");
            assert_eq!(
                h.points_of(private),
                0,
                "the corrected memory's point is retired"
            );
        },
    );
}

/// test 20c — archive / unarchive from W2 re-project in W1 and write nothing into W2's family.
/// Fault: HEAD routing ⇒ the W2 worker projects the memory into W2.
#[test]
fn archive_from_another_workspace_adds_no_point_to_it() {
    run_db_fixture::<Fixture, _>(
        "archive_from_another_workspace_adds_no_point_to_it",
        |mut h| {
            let w1 = h.workspace();
            let w2 = h.workspace();
            let memories = h.fan_out(w1, "a2 archive home", 2);
            h.drain(w1);
            let from_w2 = h.scope(w2);
            let w2_stream = stream(h.tenant_id, w2);
            for op in [DestructiveOp::MemoryArchive, DestructiveOp::MemoryUnarchive] {
                governance_ops::archive(&h.rt, &h.gateway, &from_w2, &w2_stream, memories[0], op)
                    .expect("archive / unarchive from W2");
                h.drain(w1);
                h.drain(w2);
            }
            let permit = h.permit();
            // dep: Qdrant(*) — the ops count of W2's stream
            let in_w2 =
                h.rt.block_on(stream_count_of_version(
                    &IndexFace {
                        transport: h.transport.as_ref(),
                        permit: &permit,
                        collection: &h.collection,
                    },
                    h.tenant(),
                    WorkspaceId(w2),
                    "v1",
                    &[],
                ))
                .expect("W2 stream count");
            assert_eq!(in_w2, 0, "no point was written into W2's family");
            let b1 = h.assert_closed("archive_home_w1", w1, &h.scope(w1));
            assert_eq!(b1.points_settled, 2);
        },
    );
}

/// test 20d — `memory.annotate_affect` from W2 on a memory projected in W1 re-projects it in W1,
/// so the later supersede (routed to W1) retires the only point and W2 never holds one
/// (ADR-0057 D-M). Fault: `affect_repo::annotate` issues on the request stream ⇒ the W2 worker
/// writes a duplicate point into W2's family that nothing retires (W2 stream count 1).
#[test]
fn annotate_from_another_workspace_writes_no_point_into_it() {
    run_db_fixture::<Fixture, _>(
        "annotate_from_another_workspace_writes_no_point_into_it",
        |mut h| {
            let w1 = h.workspace();
            let w2 = h.workspace();
            let e1 = h.evidence(w1, "a2 annotate m1");
            let m1 = h.memory(e1, "a2 annotate m1", TENANT_SHARED);
            let e2 = h.evidence(w1, "a2 annotate successor");
            let successor = h.memory(e2, "a2 annotate successor", TENANT_SHARED);
            h.drain(w1);
            let from_w2 = h.scope(w2);
            let w2_stream = stream(h.tenant_id, w2);
            let inputs = affect_repo::parse_affects(&serde_json::json!({
                "affects": [{"kind": "EMOTION", "intensity": 5000, "confidence": 5000}]
            }))
            .expect("affect input");
            let half_life = MoodHalfLife::new(Duration::from_secs(3_600)).expect("half-life");
            // dep: PostgreSQL(role_gateway) — the real annotate op under the W2 write scope
            h.rt.block_on(affect_repo::annotate(
                &h.gateway,
                &from_w2,
                &w2_stream,
                MemoryId(m1),
                &inputs,
                half_life,
            ))
            .expect("annotate from W2");
            h.drain(w1);
            h.drain(w2);
            assert_eq!(
                w2_stream_count(&h, w2),
                0,
                "annotate wrote no point into W2"
            );
            governance_ops::supersede(&h.rt, &h.gateway, &from_w2, &w2_stream, m1, successor)
                .expect("supersede from W2");
            h.drain(w1);
            h.drain(w2);
            assert_eq!(w2_stream_count(&h, w2), 0, "W2 still holds nothing of W1's");
            let b1 = h.assert_closed("annotate_home_w1", w1, &h.scope(w1));
            assert_eq!(b1.points_settled, 1, "the successor only");
            let b2 = h.assert_closed("annotate_request_w2", w2, &from_w2);
            assert_eq!(
                (b2.visible, b2.done),
                (Some(0), 0),
                "W2's ledger is untouched"
            );
            assert_eq!(h.points_of(m1), 0, "the superseded M1's point is retired");
        },
    );
}

/// The ops count of `workspace`'s `v1` stream (every visibility class, ADR-0057 D-C).
fn w2_stream_count(h: &Handle, workspace: Uuid) -> u64 {
    let permit = h.permit();
    // dep: Qdrant(*) — the ops count of one workspace stream
    h.rt.block_on(stream_count_of_version(
        &IndexFace {
            transport: h.transport.as_ref(),
            permit: &permit,
            collection: &h.collection,
        },
        h.tenant(),
        WorkspaceId(workspace),
        "v1",
        &[],
    ))
    .expect("stream count")
}

/// test 21 — another user's private memories are outside the caller's view on BOTH sides.
/// Fault: the definer drops the `vis(m)` predicate (L counts u2's memory ⇒ loss).
#[test]
fn other_users_private_points_close_the_identity() {
    run_db_fixture::<Fixture, _>("other_users_private_points_close_the_identity", |mut h| {
        let ws = h.workspace();
        let me = h.gov.user_id;
        let other: Uuid = h
            .admin
            .query_one(
                "INSERT INTO control.users (state) VALUES ('ACTIVE') RETURNING user_id",
                &[],
            )
            .expect("second user")
            .get(0);
        let e = h.evidence(ws, "a2 private mix");
        h.memory(e, "mine", ("USER_PRIVATE", Some(me), None));
        h.memory(e, "theirs", ("USER_PRIVATE", Some(other), None));
        h.memory(e, "shared", TENANT_SHARED);
        h.drain(ws);
        let b = h.assert_closed("other_users_private", ws, &h.scope(ws));
        assert_eq!((b.visible, b.points_settled), (Some(2), 2));
    });
}

/// The definer called as `role_gateway` inside an owner transaction, so a test can fault the
/// subject predicate (rolled back) or pass a workspace array of its choosing.
fn definer_as_gateway(
    txn: &mut postgres::Transaction<'_>,
    tenant: Uuid,
    user: Uuid,
    workspace: Uuid,
    workspace_ids: &[Uuid],
) -> (i64, i64) {
    // dep: PostgreSQL(role_gateway) — role switch for the definer call
    txn.batch_execute(&format!(
        "SET LOCAL ROLE role_gateway; SET LOCAL humaux.tenant_id = '{tenant}'; \
         SET LOCAL humaux.user_id = '{user}';"
    ))
    .expect("act as role_gateway");
    let row = txn
        .query_one(
            "SELECT points_expected, points_settled FROM projection.stream_point_ledger(\
               $1, 'workspace', $2, 'private_memory', 'PRIVATE_MEMORY', 'v1', 'SECRET_MATERIAL', $3)",
            &[&tenant, &workspace, &workspace_ids],
        )
        .expect("definer call");
    (row.get(0), row.get(1))
}

/// test 21b — a memory the 0155 subject policy hides from the caller still has its point in the
/// caller's Qdrant count (Qdrant has no subject mirror), so the definer must count it too.
/// Fault: the definer is `SECURITY INVOKER` (role_gateway's RLS hides the rows ⇒ overshoot).
#[test]
fn subject_restricted_memory_closes_the_identity() {
    run_db_fixture::<Fixture, _>("subject_restricted_memory_closes_the_identity", |mut h| {
        let ws = h.workspace();
        h.fan_out(ws, "a2 subject gated", 2);
        h.drain(ws);
        let reader = h.scope(ws);
        let visible = h.reading("subject_gated_open", ws, &reader).value.visible;
        assert_eq!(visible, Some(2));
        let (tenant, user) = (h.tenant_id, h.gov.user_id);
        let mut txn = h.admin.transaction().expect("owner txn");
        txn.batch_execute(
            "CREATE OR REPLACE FUNCTION private.memory_subject_visibility_ok(p_tenant_id uuid, p_memory_id uuid) \
             RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog \
             AS $$ SELECT false $$;",
        )
        .expect("the subject policy now hides every memory from role_gateway");
        let (_, settled) = definer_as_gateway(&mut txn, tenant, user, ws, &[ws]);
        let hidden: i64 = txn
            .query_one(
                "SELECT count(*) FROM private.memory_records WHERE tenant_id = $1",
                &[&tenant],
            )
            .expect("plain RLS read")
            .get(0);
        txn.rollback().expect("restore the predicate");
        assert_eq!(
            hidden, 0,
            "precondition: role_gateway's own view sees nothing"
        );
        assert_eq!(
            Some(settled as u64),
            visible,
            "the definer's L equals the Qdrant count"
        );
    });
}

/// test 21c — the user arm comes from the GUC only and the workspace array only narrows: a
/// WORKSPACE_SHARED memory of a workspace the user is not a member of is not counted even when
/// the caller names it. Fault: drop the membership EXISTS.
#[test]
fn definer_ignores_a_foreign_user_and_a_non_member_workspace() {
    run_db_fixture::<Fixture, _>(
        "definer_ignores_a_foreign_user_and_a_non_member_workspace",
        |mut h| {
            let ws = h.workspace();
            let foreign_ws: Uuid = h
                .admin
                .query_one(
                    "INSERT INTO control.workspaces (tenant_id, name) VALUES ($1, 'not a member') \
                     RETURNING workspace_id",
                    &[&h.tenant_id],
                )
                .expect("non-member workspace")
                .get(0);
            let other: Uuid = h
                .admin
                .query_one(
                    "INSERT INTO control.users (state) VALUES ('ACTIVE') RETURNING user_id",
                    &[],
                )
                .expect("foreign user")
                .get(0);
            let e = h.evidence(ws, "a2 definer narrowing");
            h.memory(e, "member ws", ("WORKSPACE_SHARED", None, Some(ws)));
            h.memory(
                e,
                "foreign ws",
                ("WORKSPACE_SHARED", None, Some(foreign_ws)),
            );
            h.memory(e, "foreign user", ("USER_PRIVATE", Some(other), None));
            h.drain(ws);
            let (tenant, user) = (h.tenant_id, h.gov.user_id);
            let mut txn = h.admin.transaction().expect("owner txn");
            let (expected, settled) =
                definer_as_gateway(&mut txn, tenant, user, ws, &[ws, foreign_ws]);
            txn.rollback().expect("read only");
            assert_eq!(
                (expected, settled),
                (1, 1),
                "only the member workspace's memory"
            );
        },
    );
}

/// test 22 — G23-2 injection 1 in the point unit: a point deleted straight out of Qdrant reads
/// as a real loss. Fault: take `visible` from the registry instead of Qdrant.
#[test]
fn a_point_deleted_behind_the_ledger_is_invisible_loss() {
    run_db_fixture::<Fixture, _>(
        "a_point_deleted_behind_the_ledger_is_invisible_loss",
        |mut h| {
            let ws = h.workspace();
            let memories = h.fan_out(ws, "a2 loss", 3);
            h.drain(ws);
            h.delete_point_of(memories[1]);
            let out = h.reading("point_deleted", ws, &h.scope(ws));
            assert_eq!(
                out.degradations.as_slice(),
                &[DegradeCode::ProjectionInvisibleLoss]
            );
            assert!(!out.value.current);
            assert_eq!(out.value.completeness_ratio, Some(2.0 / 3.0));
        },
    );
}

// ---- test 36: audited retirements leave 0 or 1 point per memory (points_unsettled) ----

/// (a) 1→3 fan-out, the second upsert refused: point 1 stays, the ticket retires ⇒ Q=3, visible=1,
/// Closed. Fault: the definer leaves RETIRED_FAILED out of `unsettled` ⇒ overshoot.
#[test]
fn retired_fan_out_partial_is_closed() {
    run_db_fixture::<Fixture, _>("retired_fan_out_partial_is_closed", |mut h| {
        let ws = h.workspace();
        h.fan_out(ws, "a2 partial fan-out", 3);
        assert_eq!(h.run_refused(ws, 1, false), 1, "the fan-out ticket fails");
        h.retire(ws, "qdrant_upsert_rejected");
        let b = h.assert_closed("retired_fan_out_partial", ws, &h.scope(ws));
        assert_eq!(
            (b.visible, b.points_settled, b.points_unsettled),
            (Some(1), 0, 3)
        );
    });
}

/// (b) supersede M1 whose Qdrant delete is refused after the registry retire: M1's point stays,
/// the ticket retires ⇒ Q=1, visible=L+1, Closed. Fault: RETIRED_FAILED out of `unsettled`.
#[test]
fn retired_supersede_delete_rejected_is_closed() {
    run_db_fixture::<Fixture, _>("retired_supersede_delete_rejected_is_closed", |mut h| {
        let (ws, m1, m2) = two_memories(&mut h);
        let reader = h.scope(ws);
        governance_ops::supersede(&h.rt, &h.gateway, &reader, &stream(h.tenant_id, ws), m1, m2)
            .expect("supersede");
        assert_eq!(
            h.run_refused(ws, usize::MAX, true),
            1,
            "the retire ticket fails"
        );
        h.retire(ws, "qdrant_delete_rejected");
        let b = h.assert_closed("retired_supersede_delete", ws, &reader);
        assert_eq!(
            (b.visible, b.points_settled, b.points_unsettled),
            (Some(2), 1, 1)
        );
    });
}

/// (c) restore of a retired memory whose upsert is refused: earlier DONE tickets, no point now,
/// the restore ticket retires ⇒ Q=1, visible=L, Closed. Fault: L counts "some DONE" instead of
/// "latest DONE" ⇒ InvisibleLoss.
#[test]
fn retired_restore_failed_after_done_is_closed() {
    run_db_fixture::<Fixture, _>("retired_restore_failed_after_done_is_closed", |mut h| {
        let (ws, m1, m2) = two_memories(&mut h);
        let reader = h.scope(ws);
        let key = stream(h.tenant_id, ws);
        governance_ops::supersede(&h.rt, &h.gateway, &reader, &key, m1, m2).expect("supersede");
        h.drain(ws);
        governance_ops::restore(&h.rt, &h.gateway, &reader, &key, m1).expect("restore");
        assert_eq!(h.run_refused(ws, 0, false), 1, "the restore ticket fails");
        h.retire(ws, "qdrant_upsert_rejected");
        let b = h.assert_closed("retired_restore_failed", ws, &reader);
        assert_eq!(
            (b.visible, b.points_settled, b.points_unsettled),
            (Some(1), 1, 1)
        );
    });
}

// ---- card 35 S4: ledger v2 (ADR-0062 D-M/D-O) ----

/// T-M1 — a memory whose PRIMARY evidence is SECRET_MATERIAL can never hold a point, so the v2 definer counts it
/// neither in flight (F) nor as slack (Q); an ordinary memory failed by the same ticket still widens Q by one.
/// Fault: drop `AND r.indexable` from 0219's Q filter ⇒ Q = 2 (from its F filter ⇒ F = 2 before the run).
#[test]
fn a_non_indexable_memory_with_a_failed_ticket_widens_no_slack() {
    run_db_fixture::<Fixture, _>(
        "a_non_indexable_memory_with_a_failed_ticket_widens_no_slack",
        |mut h| {
            let ws = h.workspace();
            // Ticket 1: the secret Evidence, PRIMARY of `secret`; the worker skips it by policy.
            let secret_source = h.evidence(ws, "a2 secret primary source");
            let secret = h.memory(secret_source, "secret primary", TENANT_SHARED);
            h.admin
                .execute(
                    "UPDATE private.evidence_objects SET data_class = 'SECRET_MATERIAL' \
                     WHERE evidence_id = $1",
                    &[&secret_source],
                )
                .expect("mark the PRIMARY source secret");
            // Ticket 2: an ordinary Evidence, PRIMARY of `ordinary` and SUPPORTING of `secret`. It reaches both and
            // the worker reads the ticket evidence's class (ADR-0057 limit 15), so a refused upsert fails it for both.
            let ordinary_source = h.evidence(ws, "a2 ordinary source");
            h.memory(ordinary_source, "ordinary", TENANT_SHARED);
            h.admin
                .execute(
                    "INSERT INTO private.memory_evidence (memory_id, evidence_id, role, ordinal) \
                     VALUES ($1, $2, 'SUPPORTING', 1)",
                    &[&secret, &ordinary_source],
                )
                .expect("the ordinary ticket also reaches the secret memory");
            let reader = h.scope(ws);
            let pending = h.reading("non_indexable_pending", ws, &reader).value;
            assert_eq!(
                (pending.points_expected, pending.points_in_flight),
                (1, 1),
                "U and F count only the ordinary memory while both tickets are ISSUED"
            );
            assert_eq!(
                h.run_refused(ws, 0, false),
                1,
                "the ordinary ticket fails on the refused upsert; the secret one settles SKIPPED_BY_POLICY"
            );
            let failed = h.reading("non_indexable_failed", ws, &reader).value;
            assert_eq!(
                (
                    failed.points_expected,
                    failed.points_in_flight,
                    failed.points_unsettled
                ),
                (1, 0, 1),
                "Q is the ordinary memory only: the secret one's latest ticket is FAILED too but it holds no point"
            );
        },
    );
}

// ---- card 35 S5: the Q drain (ADR-0062 D-N; ADR-0057 D-H) ----
// Every test below owns a throwaway database (ruling E8: no reissue ever lands on the shared dev database) and calls
// the production door through `maintenance_repo::reissue_unsettled_tickets` as role_maintenance.

/// The a2 fixture in its own throwaway database `humaux_thread_c35_reissue_<pid>_<n>`.
struct Drain;

impl humaux_testkit::DbIntegrationFixture for Drain {
    type Handle = Handle;

    fn isolate() -> Result<Handle, humaux_testkit::DbFixtureSkipReason> {
        let db = throwaway_db::create("c35_reissue")?;
        Handle::in_throwaway(db.dsn(), Box::new(db))
    }
}

/// A cool-down every test ages its tickets past (see [`age`]).
const COOLDOWN: Duration = Duration::from_secs(3_600);

/// One call of the production door for the fixture tenant; returns the tickets issued.
fn reissue(h: &Handle) -> i64 {
    h.rt.block_on(maintenance_repo::reissue_unsettled_tickets(
        &h.maintenance,
        h.tenant_id,
        COOLDOWN,
        100,
    ))
    .expect("reissue door as role_maintenance")
}

/// Settles the ISSUED tickets of `ws`'s stream (only those carrying `evidence`, when given) FAILED `class` through
/// role_retrieval_worker's one legal edge, as the worker does; returns how many.
fn fail(h: &mut Handle, ws: Uuid, evidence: Option<Uuid>, class: &str) -> u64 {
    let tenant = h.tenant_id;
    let mut txn = h.admin.transaction().expect("owner txn");
    let seqs: Vec<i64> = txn
        .query(
            "SELECT sl.commit_seq FROM projection.stream_log sl \
               JOIN ops.outbox o ON o.tenant_id = sl.tenant_id AND o.commit_seq = sl.commit_seq \
              WHERE sl.tenant_id = $1 AND sl.scope_id = $2 AND sl.state = 'ISSUED' \
                AND ($3::uuid IS NULL OR o.evidence_id = $3)",
            &[&tenant, &ws, &evidence],
        )
        .expect("issued tickets")
        .iter()
        .map(|r| r.get(0))
        .collect();
    // dep: PostgreSQL(role_retrieval_worker) — the ISSUED -> FAILED settle edge (0167 guard)
    txn.batch_execute(&format!(
        "SET LOCAL ROLE role_retrieval_worker; SET LOCAL humaux.tenant_id = '{tenant}';"
    ))
    .expect("act as role_retrieval_worker");
    let failed = txn
        .execute(
            "UPDATE projection.stream_log SET state = 'FAILED', error_class = $1 \
              WHERE tenant_id = $2 AND scope_id = $3 AND commit_seq = ANY($4)",
            &[&class, &tenant, &ws, &seqs],
        )
        .expect("settle FAILED");
    txn.commit().expect("commit the settle");
    failed
}

/// Pushes every timestamp of `ws`'s tickets two hours back (owner write, no state change, so the 0167 guard is not
/// involved): past [`COOLDOWN`].
fn age(h: &mut Handle, ws: Uuid) {
    h.admin
        .execute(
            "UPDATE projection.stream_log SET issued_at = issued_at - interval '2 hours', \
               settled_at = settled_at - interval '2 hours', retired_at = retired_at - interval '2 hours', \
               lost_at = lost_at - interval '2 hours' \
             WHERE tenant_id = $1 AND scope_id = $2",
            &[&h.tenant_id, &ws],
        )
        .expect("age the stream's tickets");
}

/// Owner write of one settled lifecycle ticket bound to `evidence` on `ws`'s stream (0198's issue sequence), for a
/// ticket history no production path writes on demand (a TOMBSTONED ticket, a ticket on a second stream).
fn owner_ticket(h: &mut Handle, ws: Uuid, evidence: Uuid, state: &str, class: Option<&str>) -> i64 {
    let key = stream(h.tenant_id, ws);
    h.admin
        .query_one(
            "WITH k AS (UPDATE projection.stream_checkpoints SET issued_highwater = issued_highwater + 1 \
                         WHERE tenant_id = $1 AND scope_kind = $2 AND scope_id = $3 AND domain = $4 \
                           AND projection_kind = $5 AND projection_version = $6 RETURNING issued_highwater), \
                  t AS (INSERT INTO projection.stream_log (tenant_id, scope_kind, scope_id, domain, projection_kind, \
                          projection_version, stream_seq, commit_seq, state, error_class, settled_at) \
                        SELECT $1, $2, $3, $4, $5, $6, k.issued_highwater, nextval('ops.commit_seq_seq'), $7, $8, \
                               now() FROM k RETURNING commit_seq, stream_seq) \
             INSERT INTO ops.outbox (tenant_id, commit_seq, stream_seq, event_type, evidence_id, status) \
             SELECT $1, t.commit_seq, t.stream_seq, 'MEMORY_LIFECYCLE', $9, 'DONE' FROM t RETURNING commit_seq",
            &[
                &h.tenant_id,
                &key.scope_kind,
                &key.scope_id,
                &key.domain,
                &key.projection_kind,
                &key.projection_version,
                &state,
                &class,
                &evidence,
            ],
        )
        .expect("owner ticket on a provisioned stream")
        .get(0)
}

/// The tickets the door issued, as (stream scope_id, carrier evidence), oldest first.
fn reissued(h: &mut Handle) -> Vec<(Uuid, Uuid)> {
    h.admin
        .query(
            "SELECT sl.scope_id, o.evidence_id FROM projection.ticket_reissues r \
               JOIN projection.stream_log sl ON sl.tenant_id = r.tenant_id AND sl.commit_seq = r.reissued_commit_seq \
               JOIN ops.outbox o ON o.tenant_id = r.tenant_id AND o.commit_seq = r.reissued_commit_seq \
              WHERE r.tenant_id = $1 ORDER BY r.reissued_commit_seq",
            &[&h.tenant_id],
        )
        .expect("reissue markers")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// T-N1 (ADR-0057 D-H acceptance, ADR-0062 D-N): the three retired fixtures of test 36 — a partial fan-out, a
/// refused supersede delete, a refused restore upsert — each leave Q > 0; one door call issues one ticket per
/// fixture on its own stream and one projection run per stream drains every Q to 0 with the identity closed.
/// Fault: the door issues every ticket on one stream (not `t`'s) ⇒ two streams keep their Q ⇒ red.
#[test]
fn one_sweep_drains_the_three_retired_fixtures_to_zero_unsettled() {
    run_db_fixture::<Drain, _>(
        "one_sweep_drains_the_three_retired_fixtures_to_zero_unsettled",
        |mut h| {
            let fan = h.workspace();
            h.fan_out(fan, "a2 partial fan-out", 3);
            assert_eq!(h.run_refused(fan, 1, false), 1, "the fan-out ticket fails");
            h.retire(fan, "qdrant_upsert_rejected");

            let (sup, m1, m2) = two_memories(&mut h);
            let sup_reader = h.scope(sup);
            governance_ops::supersede(
                &h.rt,
                &h.gateway,
                &sup_reader,
                &stream(h.tenant_id, sup),
                m1,
                m2,
            )
            .expect("supersede");
            assert_eq!(
                h.run_refused(sup, usize::MAX, true),
                1,
                "the retire ticket fails"
            );
            h.retire(sup, "qdrant_delete_rejected");

            let (res, r1, r2) = two_memories(&mut h);
            let res_reader = h.scope(res);
            let key = stream(h.tenant_id, res);
            governance_ops::supersede(&h.rt, &h.gateway, &res_reader, &key, r1, r2)
                .expect("supersede");
            h.drain(res);
            governance_ops::restore(&h.rt, &h.gateway, &res_reader, &key, r1).expect("restore");
            assert_eq!(h.run_refused(res, 0, false), 1, "the restore ticket fails");
            h.retire(res, "qdrant_upsert_rejected");

            for ws in [fan, sup, res] {
                assert!(
                    h.reading("retired", ws, &h.scope(ws))
                        .value
                        .points_unsettled
                        > 0
                );
                age(&mut h, ws);
            }
            assert_eq!(reissue(&h), 3, "one ticket per retired fixture");
            let streams: Vec<Uuid> = reissued(&mut h).into_iter().map(|(s, _)| s).collect();
            assert_eq!(streams.len(), 3);
            for ws in [fan, sup, res] {
                assert!(
                    streams.contains(&ws),
                    "a ticket on {ws}'s own stream: {streams:?}"
                );
            }
            for (ws, label, visible) in [
                (fan, "fan_out", 3),
                (sup, "supersede", 1),
                (res, "restore", 2),
            ] {
                h.drain(ws);
                let b = h.assert_closed(&format!("drained_{label}"), ws, &h.scope(ws));
                assert_eq!(
                    (b.points_unsettled, b.visible, b.points_settled),
                    (0, Some(visible), visible),
                    "{label}: Q drained, every point settled"
                );
            }
        },
    );
}

/// T-N2 (ADR-0062 D-N): a deterministic failure (`card_unbuildable`) is reissued once; when the reissued ticket
/// fails the same way the next call leaves it; an operator retirement makes it eligible once more. Fault: drop the
/// "`t` is not itself a reissue" predicate ⇒ the second call issues again ⇒ red.
#[test]
fn a_deterministic_failure_is_reissued_once_then_left() {
    run_db_fixture::<Drain, _>(
        "a_deterministic_failure_is_reissued_once_then_left",
        |mut h| {
            let ws = h.workspace();
            h.fan_out(ws, "a2 unbuildable", 1);
            assert_eq!(fail(&mut h, ws, None, "card_unbuildable"), 1);
            age(&mut h, ws);
            assert_eq!(
                reissue(&h),
                1,
                "the first deterministic failure is retried once"
            );
            assert_eq!(
                fail(&mut h, ws, None, "card_unbuildable"),
                1,
                "the reissue fails the same way"
            );
            age(&mut h, ws);
            assert_eq!(
                reissue(&h),
                0,
                "a failed reissue waits for an operator retirement"
            );
            let retired =
                h.rt.block_on(humaux_adapters::stream_repo::retire_failed(
                    &h.maintenance,
                    &stream(h.tenant_id, ws),
                    "card_unbuildable",
                ))
                .expect("audited retirement of the class");
            assert_eq!(
                retired.len(),
                2,
                "the operator retires both failed tickets of the class"
            );
            age(&mut h, ws);
            assert_eq!(reissue(&h), 1, "once per retirement");
            assert_eq!(reissued(&mut h).len(), 2);
        },
    );
}

/// T-N3 (ADR-0062 D-N): a transient FAILED, a deterministic FAILED and a RETIRED_FAILED ticket settled inside the
/// cool-down are all left; aged past it, each is reissued. Fault: no cool-down for RETIRED_FAILED ⇒ the first call
/// issues 1 ⇒ red.
#[test]
fn every_class_waits_out_the_cooldown() {
    run_db_fixture::<Drain, _>("every_class_waits_out_the_cooldown", |mut h| {
        let ws = h.workspace();
        h.fan_out(ws, "a2 transient", 1);
        let e_transient = evidence_of_ticket(&mut h, ws, 0);
        h.fan_out(ws, "a2 deterministic", 1);
        let e_deterministic = evidence_of_ticket(&mut h, ws, 1);
        h.fan_out(ws, "a2 retired", 1);
        let e_retired = evidence_of_ticket(&mut h, ws, 2);
        assert_eq!(
            fail(&mut h, ws, Some(e_transient), "qdrant_upsert_rejected"),
            1
        );
        assert_eq!(
            fail(&mut h, ws, Some(e_deterministic), "card_unbuildable"),
            1
        );
        assert_eq!(fail(&mut h, ws, Some(e_retired), "registry_failed"), 1);
        h.retire(ws, "registry_failed");
        assert_eq!(reissue(&h), 0, "every class waits out the cool-down");
        age(&mut h, ws);
        assert_eq!(reissue(&h), 3, "past the cool-down every class is reissued");
    });
}

/// The ISSUED tickets of `ws`'s stream, as the daemon's LOST task finds them: carriers settled (the Evidence is no
/// longer being distilled, so the claim does not hold the ticket back) and past `LOST_AFTER`; returns how many went
/// LOST through the production `sweep_lost` as role_maintenance.
fn sweep_lost(h: &mut Handle, ws: Uuid) -> u64 {
    h.admin
        .execute(
            "UPDATE ops.outbox o SET status = 'DONE' FROM projection.stream_log sl \
              WHERE sl.tenant_id = $1 AND sl.scope_id = $2 AND sl.state = 'ISSUED' \
                AND o.tenant_id = sl.tenant_id AND o.commit_seq = sl.commit_seq",
            &[&h.tenant_id, &ws],
        )
        .expect("settle the carriers");
    h.rt.block_on(humaux_adapters::stream_repo::sweep_lost(
        &h.maintenance,
        h.tenant_id,
        Duration::from_secs(900),
        100,
    ))
    .expect("sweep_lost as role_maintenance")
}

/// T-N7 (ADR-0062 D-N, 0223): a LOST ticket's cool-down counts from its sweep (`lost_at`), not from its old
/// `issued_at`. An orphan two hours old is swept LOST and is not reissued inside the cool-down; past it, it is
/// reissued once; the reissued ticket, orphaned again and swept LOST, again waits the full cool-down. So reissues of
/// one memory are at least LOST_AFTER + cool-down apart, never one per cycle. Fault: 0220's clock (no `lost_at` in
/// `last_activity`) ⇒ the first call right after the sweep issues 1 ⇒ red.
#[test]
fn a_lost_ticket_waits_out_the_cooldown_from_its_sweep() {
    run_db_fixture::<Drain, _>(
        "a_lost_ticket_waits_out_the_cooldown_from_its_sweep",
        |mut h| {
            let ws = h.workspace();
            h.fan_out(ws, "a2 orphan", 1);
            age(&mut h, ws);
            assert_eq!(sweep_lost(&mut h, ws), 1, "the orphan goes LOST");
            assert_eq!(reissue(&h), 0, "swept just now: inside the cool-down");
            age(&mut h, ws);
            assert_eq!(reissue(&h), 1, "past the cool-down: reissued once");
            age(&mut h, ws);
            assert_eq!(
                sweep_lost(&mut h, ws),
                1,
                "the reissued ticket is orphaned too"
            );
            assert_eq!(
                reissue(&h),
                0,
                "a second LOST waits the full cool-down again"
            );
            age(&mut h, ws);
            assert_eq!(reissue(&h), 1);
            assert_eq!(
                reissued(&mut h).len(),
                2,
                "one reissue per cool-down window"
            );
        },
    );
}

/// The evidence carried by the `n`-th (0-based, commit order) ticket of `ws`'s stream.
fn evidence_of_ticket(h: &mut Handle, ws: Uuid, n: i64) -> Uuid {
    h.admin
        .query_one(
            "SELECT o.evidence_id FROM projection.stream_log sl \
               JOIN ops.outbox o ON o.tenant_id = sl.tenant_id AND o.commit_seq = sl.commit_seq \
              WHERE sl.tenant_id = $1 AND sl.scope_id = $2 ORDER BY sl.commit_seq OFFSET $3 LIMIT 1",
            &[&h.tenant_id, &ws, &n],
        )
        .expect("ticket evidence")
        .get(0)
}

/// T-N4 (ADR-0062 D-N): three memories of one Evidence whose ticket failed give one ticket, bound to that
/// Evidence. Fault: dedup by memory instead of (stream, PRIMARY evidence) ⇒ 3 ⇒ red.
#[test]
fn one_ticket_per_stream_and_evidence() {
    run_db_fixture::<Drain, _>("one_ticket_per_stream_and_evidence", |mut h| {
        let ws = h.workspace();
        h.fan_out(ws, "a2 one evidence", 3);
        let evidence = evidence_of_ticket(&mut h, ws, 0);
        assert_eq!(fail(&mut h, ws, None, "qdrant_upsert_rejected"), 1);
        age(&mut h, ws);
        assert_eq!(
            reissue(&h),
            1,
            "one ticket covers every memory of the Evidence"
        );
        assert_eq!(reissued(&mut h), vec![(ws, evidence)]);
    });
}

/// T-N5 (ADR-0062 D-N, ADR-0057 D-M): the reissue goes on the failed ticket's own stream, which is the Evidence's
/// home stream (the stream of its first ticket, `memory_governance_repo::home_stream`) — even when a later ticket of
/// another kind carries the same Evidence on a second stream. Fault: issue on the stream of the Evidence's last
/// ticket ⇒ the ticket lands on the second stream ⇒ red.
#[test]
fn the_reissue_stream_is_the_home_stream() {
    run_db_fixture::<Drain, _>("the_reissue_stream_is_the_home_stream", |mut h| {
        let home = h.workspace();
        h.fan_out(home, "a2 home", 1);
        let evidence = evidence_of_ticket(&mut h, home, 0);
        let other = h.workspace();
        h.evidence(other, "a2 provisions the second stream");
        assert_eq!(fail(&mut h, home, Some(evidence), "transient_exhausted"), 1);
        // A later, settled ticket of another kind carrying the same Evidence on the second stream.
        owner_ticket(&mut h, other, evidence, "DONE", None);
        age(&mut h, home);
        age(&mut h, other);
        let first_stream: Uuid = h
            .admin
            .query_one(
                "SELECT sl.scope_id FROM ops.outbox o JOIN projection.stream_log sl \
                   ON sl.tenant_id = o.tenant_id AND sl.commit_seq = o.commit_seq \
                  WHERE o.tenant_id = $1 AND o.evidence_id = $2 ORDER BY o.commit_seq LIMIT 1",
                &[&h.tenant_id, &evidence],
            )
            .expect("home stream, as memory_governance_repo::home_stream reads it")
            .get(0);
        assert_eq!(first_stream, home);
        assert_eq!(reissue(&h), 1);
        assert_eq!(
            reissued(&mut h),
            vec![(home, evidence)],
            "the ticket is on the home stream"
        );
    });
}

/// T-N6 (ADR-0062 D-N/D-O): a memory with a TOMBSTONED ticket and a secret-PRIMARY (non-indexable) memory are never
/// reissued, even when their latest ticket is FAILED; the ordinary memory beside them is. Fault: drop the
/// `indexable` term (or the tombstone exclusion) ⇒ 2 tickets ⇒ red.
#[test]
fn a_tombstoned_or_non_indexable_memory_is_never_reissued() {
    run_db_fixture::<Drain, _>(
        "a_tombstoned_or_non_indexable_memory_is_never_reissued",
        |mut h| {
            let ws = h.workspace();
            let secret_source = h.evidence(ws, "a2 secret primary source");
            let secret = h.memory(secret_source, "secret primary", TENANT_SHARED);
            h.admin
            .execute(
                "UPDATE private.evidence_objects SET data_class = 'SECRET_MATERIAL' WHERE evidence_id = $1",
                &[&secret_source],
            )
            .expect("mark the PRIMARY source secret");
            let ordinary_source = h.evidence(ws, "a2 ordinary source");
            h.memory(ordinary_source, "ordinary", TENANT_SHARED);
            h.admin
                .execute(
                    "INSERT INTO private.memory_evidence (memory_id, evidence_id, role, ordinal) \
                 VALUES ($1, $2, 'SUPPORTING', 1)",
                    &[&secret, &ordinary_source],
                )
                .expect("the ordinary ticket also reaches the secret memory");
            let tomb_source = h.evidence(ws, "a2 tombstoned source");
            h.memory(tomb_source, "tombstoned", TENANT_SHARED);
            assert_eq!(fail(&mut h, ws, None, "qdrant_upsert_rejected"), 3);
            owner_ticket(&mut h, ws, tomb_source, "TOMBSTONED", None);
            owner_ticket(
                &mut h,
                ws,
                tomb_source,
                "FAILED",
                Some("transient_exhausted"),
            );
            age(&mut h, ws);
            assert_eq!(reissue(&h), 1, "only the ordinary memory is drained");
            assert_eq!(reissued(&mut h), vec![(ws, ordinary_source)]);
        },
    );
}
