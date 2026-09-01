//! Real PostgreSQL + same-cell Qdrant acceptance for the Phase 9 bounded public worker.

#[path = "support/contribution_fixture.rs"]
mod contribution_fixture;
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
use humaux_domain::public::ModerationState;
use humaux_infra_cell::{IntraCellHttpTransport, IntraCellMethod, IntraCellRequest};
use public_qdrant_fixture::{create_collection, delete_collection, setup};
use uuid::Uuid;

static SERIAL: Mutex<()> = Mutex::new(());

fn dsn_as_role(dsn: &str, role: &str) -> String {
    let sep = if dsn.contains('?') { '&' } else { '?' };
    format!("{dsn}{sep}options=-c%20role%3D{role}")
}

fn approve_supported(
    fixture: &mut ContributionFixture,
    public: &PublicWorkerDbPool,
    claim_id: Uuid,
    expected_revision: i64,
) -> (public_repo::EvaluationResult, String, [u8; 32]) {
    fixture
        .admin
        .execute(
            "INSERT INTO control.public_moderator_grants(user_id,grant_version,enabled) \
             VALUES($1,1,true) ON CONFLICT(user_id) DO UPDATE \
             SET grant_version=EXCLUDED.grant_version,enabled=EXCLUDED.enabled",
            &[&fixture.auth.user_id().expect("fixture user").0],
        )
        .expect("active global moderator grant");
    let row = fixture
        .admin
        .query_one(
            "SELECT content::text,sha256(convert_to(content::text,'UTF8')) \
             FROM public.claims WHERE claim_id=$1",
            &[&claim_id],
        )
        .expect("current public body");
    let canonical_body: String = row.get(0);
    let hash: Vec<u8> = row.get(1);
    let body_sha256: [u8; 32] = hash.clone().try_into().expect("SHA-256 length");
    let evaluation = fixture
        .rt
        .block_on(public_repo::evaluate_claim(
            public,
            &fixture.auth,
            &public_repo::EvaluateClaim {
                claim_id,
                expected_revision,
                expected_body_sha256: &hash,
                policy_version: "public-runtime-qdrant-v1",
                rationale: "authenticated real-PG moderator evaluation",
                target_state: ModerationState::Supported,
            },
        ))
        .expect("supported evaluation");
    (evaluation, canonical_body, body_sha256)
}

/// A real authenticated release becomes eligible, is projected once through the typed worker,
/// then is immediately excluded by the revoke fact before consumption. The subsequent bounded
/// consumer writes a permanent tombstone; a delayed old LIVE cannot resurrect it.
#[test]
#[allow(clippy::too_many_lines)] // One end-to-end oracle keeps authenticated release, PG revoke fence, and Qdrant tombstone causally ordered.
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
    let release = fixture.finalize_release();
    let admitted = fixture
        .rt
        .block_on(public_repo::admit_release(
            &public,
            fixture.auth.tenant_id(),
            release,
        ))
        .expect("public admission");
    let (evaluation, canonical_body, body_sha256) = approve_supported(
        &mut fixture,
        &public,
        admitted.claim_id,
        admitted.object_revision,
    );
    let identity = ProjectionIdentity {
        object_id: admitted.claim_id,
        object_kind: "CLAIM".to_owned(),
        object_revision: evaluation.object_revision,
        evaluation_id: evaluation.evaluation_id,
        body_sha256,
    };
    assert_eq!(
        fixture
            .rt
            .block_on(public_repo::drain_outbox(
                &public,
                fixture.auth.tenant_id(),
                8
            ))
            .expect("dispatch admission and evaluation"),
        3
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
        3
    );
    let queried_live = fixture
        .rt
        .block_on(adapter.query_live(&canonical_body, NonZeroU32::new(8).unwrap()))
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
    let live = fixture
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
        .expect("live point readback");
    assert_eq!(
        live.json_body
            .as_ref()
            .and_then(|v| v.pointer("/result/payload/projection_live")),
        Some(&serde_json::json!(true))
    );
    assert!(
        fixture
            .rt
            .block_on(contribution_repo::revoke_release(
                &fixture.private,
                fixture.auth.tenant_id(),
                release,
            ))
            .expect("private revoke")
    );
    let stale_candidates = fixture
        .rt
        .block_on(adapter.query_live(&canonical_body, NonZeroU32::new(8).unwrap()))
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
            .block_on(public_repo::drain_outbox(
                &public,
                fixture.auth.tenant_id(),
                8
            ))
            .expect("dispatch revoke"),
        1
    );
    assert_eq!(
        fixture
            .rt
            .block_on(public_repo::run_once(
                &public,
                fixture.auth.tenant_id(),
                "public-runtime-qdrant-revoke",
                8,
                &adapter
            ))
            .expect("apply revoke"),
        1
    );
    assert_eq!(
        fixture
            .rt
            .block_on(public_repo::drain_outbox(
                &public,
                fixture.auth.tenant_id(),
                8
            ))
            .expect("dispatch tombstone"),
        1
    );
    assert_eq!(
        fixture
            .rt
            .block_on(public_repo::run_once(
                &public,
                fixture.auth.tenant_id(),
                "public-runtime-qdrant-tombstone",
                8,
                &adapter
            ))
            .expect("apply tombstone"),
        1
    );
    let retired = fixture
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
        .expect("tombstone readback");
    assert_eq!(
        retired
            .json_body
            .as_ref()
            .and_then(|v| v.pointer("/result/payload/projection_live")),
        Some(&serde_json::json!(false))
    );
    assert!(
        fixture
            .rt
            .block_on(adapter.query_live(&canonical_body, NonZeroU32::new(8).unwrap()))
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
                &canonical_body
            ))
            .expect("delayed old live readback")
            .superseded
    );
    fixture.rt.block_on(delete_collection(&qdrant));
}
