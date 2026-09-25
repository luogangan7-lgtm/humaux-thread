//! Real PostgreSQL + same-cell Qdrant acceptance for the Phase 9 bounded public worker.

#[path = "support/contribution_fixture.rs"]
mod contribution_fixture;
#[path = "support/public_anonymous_seam.rs"]
mod public_anonymous_seam;
#[path = "support/public_qdrant_fixture.rs"]
mod public_qdrant_fixture;

use std::{num::NonZeroU32, sync::Mutex};

use contribution_fixture::ContributionFixture;
use humaux_adapters::{
    contribution_repo,
    postgres::PublicWorkerDbPool,
    public_projection::PublicProjectionAdapter,
    public_repo::{self, ProjectionIdentity},
};
use humaux_infra_cell::{IntraCellHttpTransport, IntraCellMethod, IntraCellRequest};
use public_anonymous_seam::{
    admit_assessed_release, drain_anonymous_queue, dsn_as_role, evaluate_anonymous_supported,
    job_status, seed_project_job,
};
use public_qdrant_fixture::{create_collection, delete_collection, setup};
use uuid::Uuid;

static SERIAL: Mutex<()> = Mutex::new(());

/// Reads the exact body the projection is keyed on, plus its SHA-256, as the moderator saw it.
fn canonical_body(fixture: &mut ContributionFixture, claim_id: Uuid) -> (String, [u8; 32]) {
    let row = fixture
        .admin
        .query_one(
            "SELECT content::text,sha256(convert_to(content::text,'UTF8')) \
             FROM public.claims WHERE claim_id=$1",
            &[&claim_id],
        )
        .expect("current public body");
    let hash: Vec<u8> = row.get(1);
    (row.get(0), hash.try_into().expect("SHA-256 length"))
}

/// Reads one Qdrant point's `projection_live` flag straight out of the collection.
fn projection_live(
    fixture: &ContributionFixture,
    qdrant: &public_qdrant_fixture::PublicQdrantFixture,
    point: Uuid,
) -> Option<serde_json::Value> {
    fixture
        .rt
        .block_on(qdrant.transport.execute(
            &qdrant.permit,
            IntraCellRequest {
                method: IntraCellMethod::Get,
                path: format!("/collections/{}/points/{point}", qdrant.collection),
                json_body: None,
                headers: Vec::new(),
            },
        ))
        .expect("point readback")
        .json_body
        .as_ref()
        .and_then(|value| value.pointer("/result/payload/projection_live"))
        .cloned()
}

/// A real assessed release is admitted on the **anonymous** seam, read back through
/// `control.anonymous_source_lineage`, evaluated SUPPORTED by the protected anonymous evaluator,
/// then projected once through the typed worker. The revoke fact excludes it from strict
/// hydration before the dispatch is consumed; consuming it writes a permanent tombstone, and a
/// delayed old LIVE write cannot resurrect it.
///
/// This oracle used to run on the tenant-scoped path (`admit_release` as `role_public_worker` ->
/// `drain_outbox` -> `run_once`). Migration `0124_phase9_independence_attestation:233` REVOKEd
/// that role's `SELECT` on `staging.contribution_releases` on purpose — the fence
/// `public_runtime.rs::legacy_release_admission_is_fenced_from_protected_rows` pins — so the
/// oracle was pinning a path the product had retired. Admission and evaluation now go through
/// the live anonymous seam; *projection* stays tenant-scoped because it reads only
/// `public.eligible_objects`, which carries no contributor or release link.
#[test]
#[ignore = "lane(a:qdrant) needs a per-run database and the same-cell Qdrant at HUMAUX_TEST_QDRANT_PORT; admission runs on the anonymous seam (0124 fenced the tenant-scoped path)"]
#[allow(clippy::too_many_lines)] // One end-to-end oracle keeps anonymous admission, PG revoke fence, and Qdrant tombstone causally ordered.
fn supported_projection_revoke_fences_hydrate_and_tombstones_old_live() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let Some(qdrant) = setup() else {
        return;
    };
    let mut fixture = ContributionFixture::new();
    let public = fixture
        .rt
        .block_on(PublicWorkerDbPool::connect(&dsn_as_role(
            &std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PG"),
            "role_public_worker",
        )))
        .expect("public role pool");
    fixture.rt.block_on(create_collection(&qdrant));
    let adapter =
        PublicProjectionAdapter::new(&qdrant.transport, &qdrant.permit, &qdrant.collection)
            .expect("public projection adapter");

    let admitted = admit_assessed_release(&mut fixture, &public, "public-runtime-qdrant-admit");
    let lineage: i64 = fixture
        .admin
        .query_one(
            "SELECT count(*) FROM control.anonymous_source_lineage \
             WHERE contribution_release_id=$1 AND anonymous_source_id=$2",
            &[&admitted.release_id, &admitted.source_id],
        )
        .expect("anonymous lineage read-back")
        .get(0);
    assert_eq!(
        lineage, 1,
        "the release reaches the public tier only through the protected lineage row"
    );
    // Admission enqueues its own projection dispatch, pinned to the pre-evaluation revision.
    // Consume it before the evaluation so the revoke half below counts only its own dispatches.
    drain_anonymous_queue(&fixture, &public, "public-runtime-qdrant-admit-projection");

    let evaluation =
        evaluate_anonymous_supported(&mut fixture, admitted.claim_id, admitted.object_revision);
    let (body, body_sha256) = canonical_body(&mut fixture, admitted.claim_id);
    let identity = ProjectionIdentity {
        object_id: admitted.claim_id,
        object_kind: "CLAIM".to_owned(),
        object_revision: evaluation.object_revision,
        evaluation_id: evaluation.evaluation_id,
        body_sha256,
    };
    let job = seed_project_job(
        &mut fixture,
        admitted.claim_id,
        evaluation.object_revision,
        &format!("public-runtime-qdrant-live-{}", Uuid::now_v7()),
    );
    assert_eq!(
        fixture
            .rt
            .block_on(public_repo::run_once(
                &public,
                fixture.auth.tenant_id(),
                "public-runtime-qdrant-live",
                8,
                &adapter,
            ))
            .expect("bounded live projection"),
        1
    );
    assert_eq!(job_status(&mut fixture, job), "DONE");

    let queried_live = fixture
        .rt
        .block_on(adapter.query_live(&body, NonZeroU32::new(8).unwrap()))
        .expect("production public query");
    assert_eq!(queried_live.len(), 1);
    assert_eq!(queried_live[0].identity, identity);
    assert!(
        fixture
            .rt
            .block_on(public_repo::hydrate_gateway(
                &fixture.gateway,
                &queried_live[0].identity,
            ))
            .expect("fresh strict pre-revoke hydrate")
            .is_some()
    );
    let point = queried_live[0].point_id;
    assert_eq!(
        projection_live(&fixture, &qdrant, point),
        Some(serde_json::json!(true))
    );

    assert!(
        fixture
            .rt
            .block_on(contribution_repo::revoke_release(
                &fixture.private,
                fixture.auth.tenant_id(),
                admitted.release_id,
            ))
            .expect("private revoke")
    );
    let stale_candidates = fixture
        .rt
        .block_on(adapter.query_live(&body, NonZeroU32::new(8).unwrap()))
        .expect("stale physical candidate after revoke");
    assert_eq!(stale_candidates.len(), 1);
    assert!(
        fixture
            .rt
            .block_on(public_repo::hydrate_gateway(
                &fixture.gateway,
                &stale_candidates[0].identity,
            ))
            .expect("fresh strict post-revoke hydrate")
            .is_none(),
        "the authoritative revoke fact gates reads before job consumption"
    );
    assert_eq!(
        fixture
            .rt
            .block_on(public_repo::run_anonymous_once(
                &public,
                "public-runtime-qdrant-revoke",
                8,
                &adapter,
            ))
            .expect("apply anonymous revoke"),
        1
    );
    assert_eq!(
        fixture
            .rt
            .block_on(public_repo::run_anonymous_once(
                &public,
                "public-runtime-qdrant-tombstone",
                8,
                &adapter,
            ))
            .expect("apply tombstone"),
        1
    );
    assert_eq!(
        projection_live(&fixture, &qdrant, point),
        Some(serde_json::json!(false))
    );
    assert!(
        fixture
            .rt
            .block_on(adapter.query_live(&body, NonZeroU32::new(8).unwrap()))
            .expect("query after permanent tombstone")
            .is_empty()
    );
    assert!(
        fixture
            .rt
            .block_on(adapter.write_live_revision(
                "CLAIM",
                admitted.claim_id,
                evaluation.object_revision,
                evaluation.evaluation_id,
                &body,
            ))
            .expect("delayed old live readback")
            .superseded
    );
    fixture.rt.block_on(delete_collection(&qdrant));
}
