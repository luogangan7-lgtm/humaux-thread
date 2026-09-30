//! `adapters::tests::qdrant_live` — ADR-0003 / §83.4 Layer 1B real-connectivity smoke test: upsert → search-visible
//!   confirmation against a real Qdrant instance on loopback, through the full `IntraCellResourceRegistry` →
//!   `authorize_cell_access` → `HttpIntraCellTransport` path — no mock transport, no injected DNS resolver, exactly
//!   the production wiring `crates/adapters/src/qdrant.rs`'s `upsert`/`scroll_by_ids`/`verify_visible_via_transport`
//!   callers would use.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-infra-cell, humaux-projection, humaux-testkit,
//!   serde_json, sqlx, tokio, uuid]; services=[Qdrant(*)]; env=[HUMAUX_TEST_QDRANT_PORT]; modules=[adapters::qdrant, domain::authority,
//!   domain::dataclass, domain::identity, domain::ids, domain::memory, humaux-testkit, infra-cell::permit,
//!   infra-cell::resource, infra-cell::transport, projection::card]
//! Called-by: [cargo-test]
//! Invariants: [§57.1 three-state gate: an unreachable Qdrant prints the missing object and returns not_applicable
//!   via skip_or_fail (red under HUMAUX_REQUIRE_QDRANT); cross-tenant filters are proven against the real cluster]
//! Spec: Baseline §57.1; ADR-0055
//!
//! §57.1: a three-state gate, not pass/fail — if Qdrant is unreachable at the configured loopback port this
//! prints which object is missing and returns (`not_applicable`) instead of failing the suite.

use std::collections::{BTreeMap, BTreeSet};
use std::net::TcpStream;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use humaux_adapters::qdrant::{
    DenseQuery, DenseQueryVersions, Distance, IndexablePayload, PointId, QdrantOperation,
    QdrantPointPayload, ShardingMethod, TenantPlacementRow, VisibleCountFilter, count,
    create_collection_body, ha_profile_for, query_dense, scroll_by_ids, subject_index_body,
    tenant_index_body, upsert, verify_visible_via_transport,
};
use humaux_domain::authority::{AuthorityClass, AuthorityStatus};
use humaux_domain::dataclass::DataClass;
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId, VisibilityClass};
use humaux_domain::ids::{TenantId, UserId, WorkspaceId};
use humaux_domain::memory::MemoryType;
use humaux_infra_cell::{
    CallerId, CellId, HttpIntraCellTransport, IntraCellHttpTransport, IntraCellMethod,
    IntraCellRequest, IntraCellResource, IntraCellResourceRegistry, ResourceEntry,
    authorize_cell_access,
};
use humaux_projection::card::EgressDisposition;
use humaux_testkit::{ExternalDep, skip_or_fail};
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

const DEFAULT_QDRANT_PORT: u16 = 6333;

fn qdrant_port() -> u16 {
    std::env::var("HUMAUX_TEST_QDRANT_PORT")
        .map(|raw| {
            let port: u16 = raw.parse().expect("HUMAUX_TEST_QDRANT_PORT must be a u16");
            assert_ne!(port, 0, "HUMAUX_TEST_QDRANT_PORT must be nonzero");
            port
        })
        .unwrap_or(DEFAULT_QDRANT_PORT)
}

fn qdrant_addr() -> String {
    format!("127.0.0.1:{}", qdrant_port())
}

fn qdrant_reachable() -> bool {
    // dep: Qdrant(*) — reachability probe for `qdrant_reachable`
    TcpStream::connect_timeout(&qdrant_addr().parse().unwrap(), Duration::from_millis(500)).is_ok()
}

fn registry(cell: CellId, caller: CallerId) -> IntraCellResourceRegistry {
    let mut entries = BTreeMap::new();
    entries.insert(
        IntraCellResource::QDRANT_REST,
        // `tls: false` — a local dev/CI Qdrant on loopback speaks plain HTTP by default (§83.4
        // 判据5 is a deploy-gate concern for a real Cell, not this smoke test's target).
        ResourceEntry::new(
            "127.0.0.1",
            qdrant_port(),
            cell,
            vec!["127.0.0.1/32".parse().unwrap()],
            BTreeSet::from([caller.clone()]),
            false,
        )
        .expect("127.0.0.1/32 is a reserved/loopback CIDR"),
    );
    IntraCellResourceRegistry::new(entries, cell, caller)
}

async fn delete_test_collection(
    transport: &HttpIntraCellTransport,
    permit: &humaux_infra_cell::CellAccessPermit,
    collection: &str,
) -> Result<(), String> {
    let response = transport
        .execute(
            permit,
            // dep: Qdrant(*) — Qdrant wire call for this fixture
            IntraCellRequest {
                method: IntraCellMethod::Delete,
                path: format!("/collections/{collection}"),
                json_body: None,
                headers: Vec::new(),
            },
        )
        .await
        .map_err(|error| format!("collection delete transport failure: {error}"))?;
    if (200..300).contains(&response.status) {
        Ok(())
    } else {
        Err(format!("collection delete returned {response:?}"))
    }
}

/// Real end-to-end proof of the ADR-0003 wiring: create a throwaway collection, upsert a point
/// with a real vector, and confirm §17.4 search-visibility through the actual HTTP path — then
/// delete the collection. `#[ignore]`d only by the reachability check inside, not by the
/// attribute, so `cargo test` always attempts it and reports which object is missing when it
/// cannot (§57.1), rather than silently skipping in CI.
// ponytail: one linear fn over clippy's 100-line default, not split into setup/act/assert
// helpers — this is a single sequential e2e narrative (create -> upsert -> verify -> teardown)
// against one real connection; splitting it would trade one readable story for several
// disconnected functions passing the same four or five values back and forth. Split it if a
// second live-connectivity test ever needs to reuse a piece of this.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn upsert_then_search_visible_round_trips_over_real_qdrant() {
    if !qdrant_reachable() {
        // 唯一判定点，见 `testkit::skip_or_fail` 的 doc。
        skip_or_fail(
            "upsert_then_search_visible_round_trips_over_real_qdrant",
            &format!("missing object: live Qdrant server at {}", qdrant_addr()),
            ExternalDep::Qdrant,
        );
        return;
    }

    let cell = CellId(Uuid::now_v7());
    let caller = CallerId("qdrant-live-test".to_string());
    let registry = registry(cell, caller);
    let transport = HttpIntraCellTransport::new(
        registry.clone(),
        Duration::from_secs(10),
        humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES,
    )
    .expect("client builds");
    let permit = authorize_cell_access(
        &registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(60),
    )
    .expect("same-cell, allowlisted caller must mint");
    let cleanup_transport = HttpIntraCellTransport::new(
        registry.clone(),
        Duration::from_secs(10),
        humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES,
    )
    .expect("cleanup client builds");
    let cleanup_permit = authorize_cell_access(
        &registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(60),
    )
    .expect("same-cell, allowlisted cleanup caller must mint");
    let collection = format!("adr0003_live_{}", Uuid::now_v7().simple());
    let collection_created = Arc::new(AtomicBool::new(false));
    let body_collection = collection.clone();
    let body_created = Arc::clone(&collection_created);

    // A spawned task turns assertion panics into JoinError, so the parent can delete only this
    // UUID collection before resuming the original panic. The second permit/transport keeps that
    // cleanup path usable after the body task has moved its own pair.
    let primary = tokio::spawn(async move {
        // Positive connectivity marker (§79: the SKIP branch above and a genuine full run must be
        // distinguishable from `test result: ok. 1 passed` alone — both print that same line).
        // Qdrant's root endpoint echoes its own version; printing it here is a transcript-visible
        // proof this ran against a real server, not merely that the SKIP branch above was not hit.
        let root = transport
            .execute(
                &permit,
                // dep: Qdrant(*) — Qdrant wire call for this fixture
                IntraCellRequest {
                    method: IntraCellMethod::Get,
                    path: "/".to_string(),
                    json_body: None,
                    headers: Vec::new(),
                },
            )
            .await
            .expect("Qdrant root endpoint request must not fail transport-side");
        let qdrant_version = root
            .json_body
            .as_ref()
            .and_then(|b| b.get("version"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        println!(
            "VERIFIED: live Qdrant {qdrant_version} at {}",
            qdrant_addr()
        );

        // --- setup: create collection + tenant index (§17.1) ---
        let create_body =
            create_collection_body(4, Distance::Cosine, 1, 1, 1, ShardingMethod::Auto);
        let create = transport
            .execute(
                &permit,
                // dep: Qdrant(*) — Qdrant wire call for this fixture
                IntraCellRequest {
                    method: IntraCellMethod::Put,
                    path: format!("/collections/{body_collection}"),
                    json_body: Some(create_body),
                    headers: Vec::new(),
                },
            )
            .await
            .expect("create collection request must not fail transport-side");
        assert!(
            (200..300).contains(&create.status),
            "collection create failed: {create:?}"
        );
        body_created.store(true, Ordering::Release);
        let index = transport
            .execute(
                &permit,
                // dep: Qdrant(*) — Qdrant wire call for this fixture
                IntraCellRequest {
                    method: IntraCellMethod::Put,
                    path: format!("/collections/{body_collection}/index"),
                    json_body: Some(tenant_index_body()),
                    headers: Vec::new(),
                },
            )
            .await
            .expect("tenant index request must not fail transport-side");
        assert!(
            (200..300).contains(&index.status),
            "tenant index create failed: {index:?}"
        );
        // §6.1.3/ADR-0029: real Qdrant must accept the `subject_ids` uuid payload index.
        let subject_index = transport
            .execute(
                &permit,
                // dep: Qdrant(*) — Qdrant wire call for this fixture
                IntraCellRequest {
                    method: IntraCellMethod::Put,
                    path: format!("/collections/{body_collection}/index"),
                    json_body: Some(subject_index_body()),
                    headers: Vec::new(),
                },
            )
            .await
            .expect("subject index request must not fail transport-side");
        assert!(
            (200..300).contains(&subject_index.status),
            "subject index create failed: {subject_index:?}"
        );

        // Two authorized points plus four higher-scoring exclusion witnesses. Asking for all
        // six below means a missing filter cannot hide behind the result limit.
        let tenant_id = TenantId::new();
        let user_id = UserId::new();
        let payload = QdrantPointPayload {
            tenant_id,
            workspace_id: WorkspaceId::new(),
            visibility_class: VisibilityClass::UserPrivate,
            visibility_user_id: Some(user_id),
            visibility_workspace_id: None,
            object_type: "memory_record".to_string(),
            memory_type: MemoryType::Fact,
            status: AuthorityStatus::Active,
            authority: AuthorityClass::PrivateKnowledge,
            created_at: OffsetDateTime::now_utc(),
            effective_at: OffsetDateTime::now_utc(),
            embedding_version: "v1".to_string(),
            projection_version: "v1".to_string(),
            source_stream_seq: 1,
            data_class: DataClass::Private,
            egress_disposition: EgressDisposition::Forbidden,
        };
        let other_tenant = QdrantPointPayload {
            tenant_id: TenantId::new(),
            ..payload.clone()
        }
        .into_indexable()
        .expect("other-tenant fixture is indexable");
        let other_user = QdrantPointPayload {
            visibility_user_id: Some(UserId::new()),
            ..payload.clone()
        }
        .into_indexable()
        .expect("other-user fixture is indexable");
        let other_version = QdrantPointPayload {
            projection_version: "retired-v0".to_owned(),
            ..payload.clone()
        }
        .into_indexable()
        .expect("retired-version fixture is indexable");
        let indexable: IndexablePayload = payload
            .into_indexable()
            .expect("Private data_class must be indexable");
        let point_id = PointId::Uuid(Uuid::now_v7());
        let distant_point_id = PointId::Uuid(Uuid::now_v7());
        let other_tenant_id = PointId::Uuid(Uuid::now_v7());
        let other_user_id = PointId::Uuid(Uuid::now_v7());
        let other_version_id = PointId::Uuid(Uuid::now_v7());
        let tombstoned_id = PointId::Uuid(Uuid::now_v7());
        let all_ids = [
            point_id,
            distant_point_id,
            other_tenant_id,
            other_user_id,
            other_version_id,
            tombstoned_id,
        ];
        let ha = ha_profile_for(QdrantOperation::NormalImmutableUpsert);
        upsert(
            &transport,
            &permit,
            &body_collection,
            &[
                (point_id, &indexable, vec![0.1, 0.2, 0.3, 0.4]),
                (distant_point_id, &indexable, vec![-0.1, -0.2, -0.3, -0.4]),
                (other_tenant_id, &other_tenant, vec![0.1, 0.2, 0.3, 0.4]),
                (other_user_id, &other_user, vec![0.1, 0.2, 0.3, 0.4]),
                (other_version_id, &other_version, vec![0.1, 0.2, 0.3, 0.4]),
                (tombstoned_id, &indexable, vec![0.1, 0.2, 0.3, 0.4]),
            ],
            ha,
        )
        .await
        .expect("upsert over the real transport must succeed");

        // --- §17.4: confirm search-visible, retrying briefly (Qdrant indexing is async) ---
        let mut confirmation = None;
        for _ in 0..20 {
            if let Ok(Some(c)) =
                verify_visible_via_transport(&transport, &permit, &body_collection, &all_ids).await
            {
                confirmation = Some(c);
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let confirmation = confirmation.expect("point must become search-visible within ~5s");
        assert!(all_ids.iter().all(|id| confirmation.contains(id)));

        // Count applies tenant/user/version authorization but has no tombstone overlay yet.
        let scope = AuthorizationScope::new(
            tenant_id,
            PrincipalId::new(),
            Some(user_id),
            BoundedSet::new(Vec::<WorkspaceId>::new()).expect("empty workspace set is valid"),
        );
        let filter = VisibleCountFilter::new(&scope, "v1").expect("non-empty projection_version");
        let raw_count = count(&transport, &permit, &body_collection, &filter)
            .await
            .expect("count over the real transport must succeed");
        assert_eq!(
            raw_count, 3,
            "two live points plus the not-yet-masked tombstone"
        );

        let placement = TenantPlacementRow {
            tenant_id,
            projection_family: humaux_adapters::qdrant::RetrievalFamily::PrivateMemoryV1,
            collection_name: body_collection.clone(),
            shard_key: None,
            placement_class: humaux_adapters::qdrant::PlacementClass::SharedFallback,
            point_count: 5,
            bytes_estimate: 0,
            promotion_state: humaux_adapters::qdrant::PromotionState::Stable,
        };
        let candidates = query_dense(
            &transport,
            &permit,
            &DenseQuery::new(
                &scope,
                &placement,
                DenseQueryVersions {
                    projection: "v1",
                    embedding: "v1",
                },
                vec![0.1, 0.2, 0.3, 0.4],
                6,
                vec![tombstoned_id],
                ha_profile_for(QdrantOperation::ReadYourWriteStrict),
            )
            .expect("typed dense query"),
        )
        .await
        .expect("dense query over real transport must succeed");
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.point_id)
                .collect::<Vec<_>>(),
            vec![point_id, distant_point_id],
            "only the two authorized, current-version, non-tombstoned vectors may rank"
        );
        assert!(candidates[0].score > candidates[1].score);

        // Direct scroll confirms the same id round-trips through the search path a second way.
        let observed = scroll_by_ids(
            &transport,
            &permit,
            &body_collection,
            &[point_id, distant_point_id],
        )
        .await
        .expect("scroll over the real transport must succeed");
        assert!(observed.contains(&point_id));
        assert!(observed.contains(&distant_point_id));
    });

    let primary = primary.await;
    let cleanup = if collection_created.load(Ordering::Acquire) {
        delete_test_collection(&cleanup_transport, &cleanup_permit, &collection).await
    } else {
        Ok(())
    };
    match primary {
        Ok(()) => cleanup.expect("test collection cleanup must succeed"),
        Err(error) if error.is_panic() => {
            if let Err(cleanup_error) = cleanup {
                eprintln!(
                    "qdrant live collection cleanup failed during test panic: {cleanup_error}"
                );
            }
            std::panic::resume_unwind(error.into_panic());
        }
        Err(error) => {
            if let Err(cleanup_error) = cleanup {
                eprintln!(
                    "qdrant live collection cleanup failed after task cancellation: {cleanup_error}"
                );
            }
            panic!("qdrant live test task was cancelled: {error}");
        }
    }
}

/// ADR-0055 D-B live witness — "no archived id in the candidate set": 10 equally-scoring points
/// for one scope, 4 with payload `archived=true`, 2 with `status="superseded"`, 4 legacy points
/// written WITHOUT the `archived` field (every pre-card-30 point). A dense query asking for all 10
/// must return exactly the 4 legacy ids: the nested `must_not[archived == true]` passes a missing
/// flag, and `status == "active"` drops the superseded pair. Fault: drop `.servable()` from
/// `DenseQuery::new` ⇒ 10 candidates ⇒ red.
#[allow(clippy::too_many_lines)] // one live narrative: create → flagged + legacy upsert → query → cleanup.
#[tokio::test]
async fn dense_query_never_returns_an_archived_or_non_active_point() {
    if !qdrant_reachable() {
        skip_or_fail(
            "dense_query_never_returns_an_archived_or_non_active_point",
            &format!("missing object: live Qdrant server at {}", qdrant_addr()),
            ExternalDep::Qdrant,
        );
        return;
    }
    let registry = registry(
        CellId(Uuid::now_v7()),
        CallerId("qdrant-live-test".to_string()),
    );
    let transport = Arc::new(
        HttpIntraCellTransport::new(
            registry.clone(),
            Duration::from_secs(10),
            humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES,
        )
        .expect("client builds"),
    );
    let permit = authorize_cell_access(
        &registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(60),
    )
    .expect("same-cell, allowlisted caller must mint");
    let collection = format!("c30_live_{}", Uuid::now_v7().simple());
    let (body_transport, body_registry, body_collection) =
        (Arc::clone(&transport), registry.clone(), collection.clone());
    let primary = tokio::spawn(async move {
        let (transport, collection) = (body_transport, body_collection);
        let permit = authorize_cell_access(
            &body_registry,
            IntraCellResource::QDRANT_REST,
            Duration::from_secs(60),
        )
        .expect("body permit");
        put_json(
            &transport,
            &permit,
            format!("/collections/{collection}"),
            create_collection_body(4, Distance::Cosine, 1, 1, 1, ShardingMethod::Auto),
        )
        .await;
        put_json(
            &transport,
            &permit,
            format!("/collections/{collection}/index"),
            tenant_index_body(),
        )
        .await;

        let tenant_id = TenantId::new();
        let payload = |status: AuthorityStatus| QdrantPointPayload {
            tenant_id,
            workspace_id: WorkspaceId::new(),
            visibility_class: VisibilityClass::TenantShared,
            visibility_user_id: None,
            visibility_workspace_id: None,
            object_type: "memory_record".to_string(),
            memory_type: MemoryType::Fact,
            status,
            authority: AuthorityClass::PrivateKnowledge,
            created_at: OffsetDateTime::now_utc(),
            effective_at: OffsetDateTime::now_utc(),
            embedding_version: "v1".to_string(),
            projection_version: "v1".to_string(),
            source_stream_seq: 1,
            data_class: DataClass::Internal,
            egress_disposition: EgressDisposition::Allowed,
        };
        let vector = vec![0.1, 0.2, 0.3, 0.4];
        let archived = payload(AuthorityStatus::Active)
            .into_indexable()
            .expect("indexable")
            .with_archived(true);
        let superseded = payload(AuthorityStatus::Superseded)
            .into_indexable()
            .expect("indexable");
        let ids = |n: usize| {
            (0..n)
                .map(|_| PointId::Uuid(Uuid::now_v7()))
                .collect::<Vec<_>>()
        };
        let (archived_ids, superseded_ids, legacy_ids) = (ids(4), ids(2), ids(4));
        let rows: Vec<_> = archived_ids
            .iter()
            .map(|id| (*id, &archived, vector.clone()))
            .chain(
                superseded_ids
                    .iter()
                    .map(|id| (*id, &superseded, vector.clone())),
            )
            .collect();
        upsert(
            transport.as_ref(),
            &permit,
            &collection,
            &rows,
            ha_profile_for(QdrantOperation::NormalImmutableUpsert),
        )
        .await
        .expect("flagged points upsert");
        // Legacy shape: the payload as it was written before card 30 — no `archived` key at all.
        let legacy = payload(AuthorityStatus::Active)
            .into_indexable()
            .expect("indexable");
        let points: Vec<serde_json::Value> = legacy_ids
            .iter()
            .map(|id| {
                let mut point = humaux_adapters::qdrant::upsert_point_body(*id, &legacy);
                let body = point.as_object_mut().expect("point object");
                body["payload"]
                    .as_object_mut()
                    .expect("payload object")
                    .remove("archived")
                    .expect("card-30 payloads carry the flag");
                body.insert("vector".to_owned(), serde_json::json!(vector));
                point
            })
            .collect();
        put_json(
            &transport,
            &permit,
            format!("/collections/{collection}/points?wait=true"),
            serde_json::json!({ "points": points }),
        )
        .await;

        let scope = AuthorizationScope::new(
            tenant_id,
            PrincipalId::new(),
            None,
            BoundedSet::new(Vec::<WorkspaceId>::new()).expect("empty workspace set is valid"),
        );
        let placement = TenantPlacementRow {
            tenant_id,
            projection_family: humaux_adapters::qdrant::RetrievalFamily::PrivateMemoryV1,
            collection_name: collection.clone(),
            shard_key: None,
            placement_class: humaux_adapters::qdrant::PlacementClass::SharedFallback,
            point_count: 10,
            bytes_estimate: 0,
            promotion_state: humaux_adapters::qdrant::PromotionState::Stable,
        };
        let candidates = query_dense(
            transport.as_ref(),
            &permit,
            &DenseQuery::new(
                &scope,
                &placement,
                DenseQueryVersions {
                    projection: "v1",
                    embedding: "v1",
                },
                vector.clone(),
                10,
                vec![],
                ha_profile_for(QdrantOperation::ReadYourWriteStrict),
            )
            .expect("typed dense query"),
        )
        .await
        .expect("dense query over real transport must succeed");
        let got: BTreeSet<String> = candidates
            .iter()
            .map(|c| format!("{:?}", c.point_id))
            .collect();
        let want: BTreeSet<String> = legacy_ids.iter().map(|id| format!("{id:?}")).collect();
        assert_eq!(
            got, want,
            "only the flagless legacy points may be candidates (no archived, no superseded)"
        );
    });
    let primary = primary.await;
    let cleanup = delete_test_collection(&transport, &permit, &collection).await;
    match primary {
        Ok(()) => cleanup.expect("test collection cleanup must succeed"),
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => panic!("qdrant live test task was cancelled: {error}"),
    }
}

async fn put_json(
    transport: &HttpIntraCellTransport,
    permit: &humaux_infra_cell::CellAccessPermit,
    path: String,
    body: serde_json::Value,
) {
    let response = transport
        .execute(
            permit,
            // dep: Qdrant(*) — Qdrant wire call for this fixture
            IntraCellRequest {
                method: IntraCellMethod::Put,
                path,
                json_body: Some(body),
                headers: Vec::new(),
            },
        )
        .await
        .expect("Qdrant setup request");
    assert!((200..300).contains(&response.status), "{response:?}");
}
