//! `adapters::tests::public_projection` — Real Qdrant acceptance tests for the public projection adapter (§17.6).
//! Depends-on: crates=[humaux-adapters, humaux-infra-cell, humaux-testkit, serde_json, tokio, uuid]; services=[Qdrant(*)];
//!   env=[HUMAUX_TEST_QDRANT_PORT]; modules=[adapters::public_projection, humaux-testkit, infra-cell::permit, infra-cell::resource, infra-cell::transport]
//! Called-by: [cargo-test]
//! Invariants: [writes only the public payload contract into a throwaway collection; no Qdrant port goes through
//!   skip_or_fail (red under HUMAUX_REQUIRE_QDRANT)]
//! Spec: Baseline §17.6; §79.2

use std::collections::{BTreeMap, BTreeSet};
use std::net::TcpStream;
use std::num::NonZeroU32;
use std::time::Duration;

use humaux_adapters::public_projection::PublicProjectionAdapter;
use humaux_infra_cell::{
    CallerId, CellId, HttpIntraCellTransport, IntraCellHttpTransport, IntraCellMethod,
    IntraCellRequest, IntraCellResource, IntraCellResourceRegistry, ResourceEntry,
    authorize_cell_access,
};
use humaux_testkit::{ExternalDep, skip_or_fail};
use serde_json::{Value, json};
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

fn setup() -> Option<(
    HttpIntraCellTransport,
    humaux_infra_cell::CellAccessPermit,
    String,
)> {
    // dep: Qdrant(*) — reachability probe for `setup`
    if TcpStream::connect_timeout(
        &format!("127.0.0.1:{}", qdrant_port()).parse().unwrap(),
        Duration::from_millis(500),
    )
    .is_err()
    {
        skip_or_fail(
            "public_projection_qdrant",
            &format!(
                "missing object: isolated Qdrant at 127.0.0.1:{}",
                qdrant_port()
            ),
            ExternalDep::Qdrant,
        );
        return None;
    }
    let cell = CellId(Uuid::now_v7());
    let caller = CallerId("public-projection-test".to_owned());
    let mut entries = BTreeMap::new();
    entries.insert(
        IntraCellResource::QDRANT_REST,
        ResourceEntry::new(
            "127.0.0.1",
            qdrant_port(),
            cell,
            vec!["127.0.0.1/32".parse().unwrap()],
            BTreeSet::from([caller.clone()]),
            false,
        )
        .unwrap(),
    );
    let registry = IntraCellResourceRegistry::new(entries, cell, caller);
    let permit = authorize_cell_access(
        &registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(60),
    )
    .unwrap();
    let transport = HttpIntraCellTransport::new(
        registry,
        Duration::from_secs(10),
        humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES,
    )
    .unwrap();
    Some((
        transport,
        permit,
        format!("phase9_public_projection_{}", Uuid::now_v7().simple()),
    ))
}

async fn create_collection(
    transport: &HttpIntraCellTransport,
    permit: &humaux_infra_cell::CellAccessPermit,
    collection: &str,
) {
    let response = transport
        .execute(
            permit,
            // dep: Qdrant(*) — Qdrant wire call for this fixture
            IntraCellRequest {
                method: IntraCellMethod::Put,
                path: format!("/collections/{collection}"),
                json_body: Some(
                    json!({"vectors": {}, "sparse_vectors": {"bm25": {"modifier": "idf"}}}),
                ),
                headers: Vec::new(),
            },
        )
        .await
        .unwrap();
    assert!(
        (200..300).contains(&response.status),
        "create failed: {response:?}"
    );
    for field in [
        "object_kind",
        "object_id",
        "object_revision",
        "evaluation_id",
        "body_sha256",
        "projection_live",
    ] {
        // dep: Qdrant(*) — Qdrant wire call for this fixture
        let response = transport.execute(permit, IntraCellRequest {
            method: IntraCellMethod::Put,
            path: format!("/collections/{collection}/index"),
            json_body: Some(json!({"field_name": field, "field_schema": {"type": if field == "object_revision" { "integer" } else if field == "projection_live" { "bool" } else { "keyword" }}})),
            headers: Vec::new(),
        }).await.unwrap();
        assert!(
            (200..300).contains(&response.status),
            "index {field} failed: {response:?}"
        );
    }
}

async fn delete_collection(
    transport: &HttpIntraCellTransport,
    permit: &humaux_infra_cell::CellAccessPermit,
    collection: &str,
) {
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
        .unwrap();
    assert!(
        (200..300).contains(&response.status),
        "delete failed: {response:?}"
    );
}

fn payload(readback: &humaux_adapters::public_projection::PublicProjectionReadback) -> &Value {
    readback
        .payload
        .as_ref()
        .expect("Qdrant must return projection payload")
}

#[tokio::test]
async fn delayed_old_live_cannot_resurrect_after_retire() {
    let Some((transport, permit, collection)) = setup() else {
        return;
    };
    create_collection(&transport, &permit, &collection).await;
    let adapter = PublicProjectionAdapter::new(&transport, &permit, &collection).unwrap();
    let object = Uuid::now_v7();
    let evaluation = Uuid::now_v7();
    let live = adapter
        .write_live_revision("CLAIM", object, 5, evaluation, "R5")
        .await
        .unwrap();
    assert_eq!(payload(&live)["projection_live"], true);
    let tombstone = adapter
        .retire_revision_fields(
            "CLAIM",
            object,
            5,
            evaluation,
            payload(&live)["body_sha256"].as_str().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(payload(&tombstone)["projection_live"], false);
    assert!(match tombstone.vector.as_ref() {
        None => true,
        Some(vector) => vector.as_object().is_some_and(|v| v.is_empty()),
    });
    let delayed = adapter
        .write_live_revision("CLAIM", object, 5, evaluation, "R5")
        .await
        .unwrap();
    assert!(delayed.superseded);
    assert_eq!(payload(&delayed)["projection_live"], false);
    assert!(match delayed.vector.as_ref() {
        None => true,
        Some(vector) => vector.as_object().is_some_and(|v| v.is_empty()),
    });
    delete_collection(&transport, &permit, &collection).await;
}

#[tokio::test]
async fn retiring_old_revision_does_not_touch_new_revision() {
    let Some((transport, permit, collection)) = setup() else {
        return;
    };
    create_collection(&transport, &permit, &collection).await;
    let adapter = PublicProjectionAdapter::new(&transport, &permit, &collection).unwrap();
    let object = Uuid::now_v7();
    let evaluation = Uuid::now_v7();
    let old = adapter
        .write_live_revision("CLAIM", object, 5, evaluation, "R5")
        .await
        .unwrap();
    let new = adapter
        .write_live_revision("CLAIM", object, 6, evaluation, "R6")
        .await
        .unwrap();
    adapter
        .retire_revision_fields(
            "CLAIM",
            object,
            5,
            evaluation,
            payload(&old)["body_sha256"].as_str().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(payload(&new)["object_revision"], 6);
    assert_eq!(payload(&new)["projection_live"], true);
    delete_collection(&transport, &permit, &collection).await;
}

/// An insert-only delayed LIVE may accept a pre-existing tombstone only when that tombstone is
/// the exact revision identity and has no vector. Corrupt metadata is fail-closed, not treated
/// as a benign `Superseded` result.
#[tokio::test]
async fn delayed_live_rejects_corrupted_tombstone_readback() {
    let Some((transport, permit, collection)) = setup() else {
        return;
    };
    create_collection(&transport, &permit, &collection).await;
    let adapter = PublicProjectionAdapter::new(&transport, &permit, &collection).unwrap();
    let object = Uuid::now_v7();
    let evaluation = Uuid::now_v7();
    let live = adapter
        .write_live_revision("CLAIM", object, 5, evaluation, "sealed public body")
        .await
        .unwrap();
    let retired = adapter
        .retire_revision_fields(
            "CLAIM",
            object,
            5,
            evaluation,
            payload(&live)["body_sha256"].as_str().unwrap(),
        )
        .await
        .unwrap();
    let corrupt = transport
        .execute(
            &permit,
            // dep: Qdrant(*) — Qdrant wire call for this fixture
            IntraCellRequest {
                method: IntraCellMethod::Put,
                path: format!("/collections/{collection}/points?wait=true"),
                json_body: Some(json!({"points":[{
                    "id":retired.point_id.to_string(),
                    "payload":{
                        "object_kind":"CLAIM","object_id":object.to_string(),
                        "object_revision":5,"evaluation_id":evaluation.to_string(),
                        "body_sha256":"00","projection_live":false
                    },"vector":{}
                }]})),
                headers: Vec::new(),
            },
        )
        .await
        .unwrap();
    assert!((200..300).contains(&corrupt.status));
    assert!(
        adapter
            .write_live_revision("CLAIM", object, 5, evaluation, "sealed public body")
            .await
            .is_err()
    );
    delete_collection(&transport, &permit, &collection).await;
}

/// A pre-existing LIVE point whose body hash belongs to different canonical text is malformed
/// for this request. Insert-only must not silently report it as an applied projection.
#[tokio::test]
async fn insert_only_live_rejects_canonical_body_hash_mismatch() {
    let Some((transport, permit, collection)) = setup() else {
        return;
    };
    create_collection(&transport, &permit, &collection).await;
    let adapter = PublicProjectionAdapter::new(&transport, &permit, &collection).unwrap();
    let object = Uuid::now_v7();
    let evaluation = Uuid::now_v7();
    adapter
        .write_live_revision("CLAIM", object, 5, evaluation, "canonical body one")
        .await
        .unwrap();
    assert!(
        adapter
            .write_live_revision("CLAIM", object, 5, evaluation, "canonical body two")
            .await
            .is_err()
    );
    delete_collection(&transport, &permit, &collection).await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // The production query plus independent raw global oracle share one real-Qdrant fixture.
async fn bm25_query_excludes_false_live_vector_and_uses_same_idf_corpus_filter() {
    let Some((transport, permit, collection)) = setup() else {
        return;
    };
    create_collection(&transport, &permit, &collection).await;
    let adapter = PublicProjectionAdapter::new(&transport, &permit, &collection).unwrap();
    let live_id = Uuid::now_v7();
    let live = adapter
        .write_live_revision("CLAIM", live_id, 7, Uuid::now_v7(), "rare public needle")
        .await
        .unwrap();
    let poison_id = Uuid::now_v7();
    let poison_payload = json!({
        "object_kind": "CLAIM",
        "object_id": poison_id.to_string(),
        "object_revision": 7,
        "evaluation_id": Uuid::now_v7().to_string(),
        "body_sha256": "00",
        "projection_live": false
    });
    let poisoned = transport
        .execute(
            &permit,
            // dep: Qdrant(*) — Qdrant wire call for this fixture
            IntraCellRequest {
                method: IntraCellMethod::Put,
                path: format!("/collections/{collection}/points?wait=true"),
                json_body: Some(json!({
                    "points": [{"id": poison_id.to_string(), "payload": poison_payload,
                        "vector": {"bm25": {"text": "rare public needle", "model": "qdrant/bm25"}}}]
                })),
                headers: Vec::new(),
            },
        )
        .await
        .unwrap();
    assert!(
        (200..300).contains(&poisoned.status),
        "poison seed failed: {poisoned:?}"
    );

    let scoped = adapter
        .query_live("rare public needle", NonZeroU32::new(10).unwrap())
        .await
        .expect("production BM25 query");
    assert_eq!(scoped.len(), 1, "false-live vector must be excluded");
    assert_eq!(scoped[0].point_id, live.point_id);
    let scoped_score = scoped[0].score;
    let filter = json!({"must": [{"key": "projection_live", "match": {"value": true}}]});
    let mut global_body = json!({
        "query": {"text": "rare public needle", "model": "qdrant/bm25"},
        "using": "bm25",
        "filter": filter,
        "limit": 10,
        "with_payload": false,
        "with_vector": false
    });
    let global = transport
        .execute(
            &permit,
            // dep: Qdrant(*) — Qdrant wire call for this fixture
            IntraCellRequest {
                method: IntraCellMethod::Post,
                path: format!("/collections/{collection}/points/query"),
                json_body: Some(std::mem::take(&mut global_body)),
                headers: Vec::new(),
            },
        )
        .await
        .unwrap();
    assert!(
        (200..300).contains(&global.status),
        "global BM25 query failed: {global:?}"
    );
    let global_score = global
        .json_body
        .as_ref()
        .and_then(|body| body.get("result"))
        .and_then(|result| result.get("points"))
        .and_then(Value::as_array)
        .and_then(|points| points.first())
        .and_then(|point| point.get("score"))
        .and_then(Value::as_f64)
        .expect("global BM25 score required");
    assert_ne!(
        scoped_score, global_score,
        "IDF corpus must affect BM25 score"
    );
    delete_collection(&transport, &permit, &collection).await;
}
