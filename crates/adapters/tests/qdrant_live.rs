//! ADR-0003 / §83.4 Layer 1B real-connectivity smoke test: upsert → search-visible confirmation
//! against a real Qdrant instance at `127.0.0.1:6333`, through the full
//! `IntraCellResourceRegistry` → `authorize_cell_access` → `HttpIntraCellTransport` path — no
//! mock transport, no injected DNS resolver, exactly the production wiring
//! `crates/adapters/src/qdrant.rs`'s `upsert`/`scroll_by_ids`/`verify_visible_via_transport`
//! callers would use.
//!
//! §57.1: a three-state gate, not pass/fail — if Qdrant is unreachable at `127.0.0.1:6333` this
//! prints which object is missing and returns (`not_applicable`) instead of failing the suite.

use std::collections::{BTreeMap, BTreeSet};
use std::net::TcpStream;
use std::time::Duration;

use humaux_adapters::qdrant::{
    Distance, IndexablePayload, PointId, QdrantOperation, QdrantPointPayload, ShardingMethod,
    VisibleCountFilter, count, create_collection_body, ha_profile_for, scroll_by_ids,
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
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

const QDRANT_ADDR: &str = "127.0.0.1:6333";

fn qdrant_reachable() -> bool {
    TcpStream::connect_timeout(&QDRANT_ADDR.parse().unwrap(), Duration::from_millis(500)).is_ok()
}

fn registry(cell: CellId, caller: CallerId) -> IntraCellResourceRegistry {
    let mut entries = BTreeMap::new();
    entries.insert(
        IntraCellResource::QDRANT_REST,
        // `tls: false` — a local dev/CI Qdrant on loopback speaks plain HTTP by default (§83.4
        // 判据5 is a deploy-gate concern for a real Cell, not this smoke test's target).
        ResourceEntry::new(
            "127.0.0.1",
            6333,
            cell,
            vec!["127.0.0.1/32".parse().unwrap()],
            BTreeSet::from([caller.clone()]),
            false,
        )
        .expect("127.0.0.1/32 is a reserved/loopback CIDR"),
    );
    IntraCellResourceRegistry::new(entries, cell, caller)
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
        println!(
            "SKIP (not_applicable): Qdrant unreachable at {QDRANT_ADDR} — missing object: live Qdrant server"
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

    // Positive connectivity marker (§79: the SKIP branch above and a genuine full run must be
    // distinguishable from `test result: ok. 1 passed` alone — both print that same line).
    // Qdrant's root endpoint echoes its own version; printing it here is a transcript-visible
    // proof this ran against a real server, not merely that the SKIP branch above was not hit.
    let root = transport
        .execute(
            &permit,
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
    println!("VERIFIED: live Qdrant {qdrant_version} at {QDRANT_ADDR}");

    let collection = format!("adr0003_live_{}", Uuid::now_v7().simple());

    // --- setup: create collection + tenant index (§17.1) ---
    let create_body = create_collection_body(4, Distance::Cosine, 1, 1, 1, ShardingMethod::Auto);
    let create = transport
        .execute(
            &permit,
            IntraCellRequest {
                method: IntraCellMethod::Put,
                path: format!("/collections/{collection}"),
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
    let index = transport
        .execute(
            &permit,
            IntraCellRequest {
                method: IntraCellMethod::Put,
                path: format!("/collections/{collection}/index"),
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

    // --- upsert one real point ---
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
    let indexable: IndexablePayload = payload
        .into_indexable()
        .expect("Private data_class must be indexable");
    let point_id = PointId::Uuid(Uuid::now_v7());
    let ha = ha_profile_for(QdrantOperation::NormalImmutableUpsert);
    upsert(
        &transport,
        &permit,
        &collection,
        &[(point_id, &indexable, vec![0.1, 0.2, 0.3, 0.4])],
        ha,
    )
    .await
    .expect("upsert over the real transport must succeed");

    // --- §17.4: confirm search-visible, retrying briefly (Qdrant indexing is async) ---
    let mut confirmation = None;
    for _ in 0..20 {
        if let Ok(Some(c)) =
            verify_visible_via_transport(&transport, &permit, &collection, &[point_id]).await
        {
            confirmation = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let confirmation = confirmation.expect("point must become search-visible within ~5s");
    assert!(confirmation.contains(&point_id));

    // --- §16.3/§23.1②: visible count reflects the one upserted point ---
    let scope = AuthorizationScope::new(
        tenant_id,
        PrincipalId::new(),
        Some(user_id),
        BoundedSet::new(Vec::<WorkspaceId>::new()).expect("empty workspace set is valid"),
    );
    let filter = VisibleCountFilter::new(&scope, "v1").expect("non-empty projection_version");
    let raw_count = count(&transport, &permit, &collection, &filter)
        .await
        .expect("count over the real transport must succeed");
    assert!(
        raw_count >= 1,
        "expected at least the one upserted point, got {raw_count}"
    );

    // Direct scroll confirms the same id round-trips through the search path a second way.
    let observed = scroll_by_ids(&transport, &permit, &collection, &[point_id])
        .await
        .expect("scroll over the real transport must succeed");
    assert!(observed.contains(&point_id));

    // --- teardown ---
    let _ = transport
        .execute(
            &permit,
            IntraCellRequest {
                method: IntraCellMethod::Delete,
                path: format!("/collections/{collection}"),
                json_body: None,
                headers: Vec::new(),
            },
        )
        .await;
}
