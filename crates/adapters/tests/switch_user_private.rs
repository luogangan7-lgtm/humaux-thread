//! `adapters::tests::switch_user_private` — ADR-0057 D-C: the §16.2 serve switch's ops count sees every visibility
//!   class of a stream, so a tenant with live USER_PRIVATE points can switch and a shadow missing one is refused.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-projection, humaux-testkit, sqlx]; services=[PostgreSQL(owner)
//!   w=[control.users], Qdrant(*)]; env=[]; modules=[adapters::retrieve, adapters::tests::support::a2_fixture,
//!   adapters::tests::support::governance_ops, domain::ids, humaux-testkit, projection::serving]
//! Called-by: [cargo-test]
//! Invariants: [the counts are the production ops producer (retrieve::stream_count_of_version, the one xtask's switch
//!   and soak use) over points the real worker projected; the verdict is projection::serving::evaluate_switch]
//! Spec: Baseline §16.2; §16.3; §17.1; ADR-0040; ADR-0057

use humaux_adapters::retrieve::{IndexFace, stream_count_of_version};
use humaux_domain::ids::WorkspaceId;
use humaux_projection::serving::{
    ActivationEvidence, ContinuationVerdict, SwitchCriteria, SwitchRejection, evaluate_switch,
};
use humaux_testkit::run_db_fixture;
use sqlx::types::Uuid;

#[path = "support/a2_fixture.rs"]
mod a2_fixture;
#[path = "support/governance_ops.rs"]
#[allow(dead_code)]
mod governance_ops;
use a2_fixture::{Fixture, Handle, TENANT_SHARED};

/// `shared` TENANT_SHARED plus `private` USER_PRIVATE memories (of two different users) on the
/// `version` stream of `workspace`, projected by the real worker.
fn project(h: &mut Handle, workspace: Uuid, version: &str, shared: usize, private: usize) {
    let other: Uuid = h
        .admin
        .query_one(
            "INSERT INTO control.users (state) VALUES ('ACTIVE') RETURNING user_id",
            &[],
        )
        .expect("second user")
        .get(0);
    let me = h.gov.user_id;
    let evidence = h.evidence_at(workspace, &format!("switch {version}"), version);
    for i in 0..shared {
        h.memory(evidence, &format!("shared {version} {i}"), TENANT_SHARED);
    }
    for i in 0..private {
        let owner = if i % 2 == 0 { me } else { other };
        h.memory(
            evidence,
            &format!("private {version} {i}"),
            ("USER_PRIVATE", Some(owner), None),
        );
    }
    h.drain_at(workspace, version);
}

/// The ops count of one version of `workspace`'s stream.
fn ops_count(h: &Handle, workspace: Uuid, version: &str) -> Option<(String, u64)> {
    let permit = h.permit();
    // dep: Qdrant(*) — the ops count of one stream version
    h.rt.block_on(stream_count_of_version(
        &IndexFace {
            transport: h.transport.as_ref(),
            permit: &permit,
            collection: &h.collection,
        },
        h.tenant(),
        WorkspaceId(workspace),
        version,
        &[],
    ))
    .map(|n| (version.to_owned(), n))
}

/// test 26 — first activation of a stream holding USER_PRIVATE points of two users: the count is
/// taken (every point of the stream) and the switch is not refused `VisibleUnavailable`. Fault:
/// the shared ops producer returns `None` whenever such a point exists. The xtask wiring around
/// it (`projection-serve`'s own entry) is pinned by
/// `xtask::switch_visible::tests::projection_serve_promotes_a_stream_holding_user_private_memories`.
#[test]
fn a_tenant_with_user_private_memories_can_switch() {
    run_db_fixture::<Fixture, _>("a_tenant_with_user_private_memories_can_switch", |mut h| {
        let ws = h.workspace();
        project(&mut h, ws, "v1", 2, 2);
        let shadow = ops_count(&h, ws, "v1");
        assert_eq!(
            shadow,
            Some(("v1".to_owned(), 4)),
            "every visibility class counts"
        );
        let verdict = evaluate_switch(&SwitchCriteria {
            shadow: ActivationEvidence::VisibleThrough(shadow),
            visible_serving: None,
            first_activation: true,
            shadow_open_gaps: 0,
            continuation: ContinuationVerdict::Pass,
        });
        assert_eq!(
            verdict,
            Ok(()),
            "a first activation with user-private points switches"
        );
    });
}

/// test 27 — a shadow version that lost one USER_PRIVATE point is refused `VisibleMismatch`. Fault:
/// count under a `user_id = None` scope (the deleted `ops_scope`): both sides omit every private
/// point, agree at 2 = 2, and the broken shadow promotes.
#[test]
fn a_shadow_missing_a_user_private_point_is_refused() {
    run_db_fixture::<Fixture, _>(
        "a_shadow_missing_a_user_private_point_is_refused",
        |mut h| {
            let ws = h.workspace();
            project(&mut h, ws, "v1", 2, 2);
            project(&mut h, ws, "v2", 2, 1);
            let verdict = evaluate_switch(&SwitchCriteria {
                shadow: ActivationEvidence::VisibleThrough(ops_count(&h, ws, "v2")),
                visible_serving: ops_count(&h, ws, "v1"),
                first_activation: false,
                shadow_open_gaps: 0,
                continuation: ContinuationVerdict::Pass,
            });
            let rejections = verdict.expect_err("the shadow is one private point short");
            assert!(
                rejections.contains(&SwitchRejection::VisibleMismatch),
                "{rejections:?}"
            );
            assert!(
                !rejections.contains(&SwitchRejection::VisibleUnavailable),
                "{rejections:?}"
            );
        },
    );
}
