use std::collections::{BTreeMap, BTreeSet};
use std::net::TcpStream;
use std::time::Duration;

use humaux_infra_cell::{
    CallerId, CellAccessPermit, CellId, HttpIntraCellTransport, IntraCellHttpTransport,
    IntraCellMethod, IntraCellRequest, IntraCellResource, IntraCellResourceRegistry, ResourceEntry,
    authorize_cell_access,
};
use humaux_testkit::{ExternalDep, skip_or_fail};
use serde_json::json;
use uuid::Uuid;

pub struct PublicQdrantFixture {
    pub transport: HttpIntraCellTransport,
    pub permit: CellAccessPermit,
    pub collection: String,
}

pub fn setup() -> Option<PublicQdrantFixture> {
    let port = std::env::var("HUMAUX_TEST_QDRANT_PORT")
        .map(|raw| {
            raw.parse::<u16>()
                .expect("HUMAUX_TEST_QDRANT_PORT must be a u16")
        })
        .unwrap_or(6333);
    if TcpStream::connect_timeout(
        &format!("127.0.0.1:{port}")
            .parse()
            .expect("loopback socket"),
        Duration::from_millis(500),
    )
    .is_err()
    {
        skip_or_fail(
            "public_projection_qdrant",
            &format!("missing object: isolated Qdrant at 127.0.0.1:{port}"),
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
            port,
            cell,
            vec!["127.0.0.1/32".parse().expect("CIDR")],
            BTreeSet::from([caller.clone()]),
            false,
        )
        .expect("same-cell Qdrant entry"),
    );
    let registry = IntraCellResourceRegistry::new(entries, cell, caller);
    let permit = authorize_cell_access(
        &registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(60),
    )
    .expect("same-cell Qdrant permit");
    let transport = HttpIntraCellTransport::new(
        registry,
        Duration::from_secs(10),
        humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES,
    )
    .expect("same-cell Qdrant transport");
    Some(PublicQdrantFixture {
        transport,
        permit,
        collection: format!("phase9_public_projection_{}", Uuid::now_v7().simple()),
    })
}

pub async fn create_collection(fixture: &PublicQdrantFixture) {
    let response = fixture
        .transport
        .execute(
            &fixture.permit,
            IntraCellRequest {
                method: IntraCellMethod::Put,
                path: format!("/collections/{}", fixture.collection),
                json_body: Some(
                    json!({"vectors": {}, "sparse_vectors": {"bm25": {"modifier": "idf"}}}),
                ),
                headers: Vec::new(),
            },
        )
        .await
        .expect("create collection");
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
        let response = fixture.transport.execute(&fixture.permit, IntraCellRequest {
            method: IntraCellMethod::Put,
            path: format!("/collections/{}/index", fixture.collection),
            json_body: Some(json!({"field_name": field, "field_schema": {"type": if field == "object_revision" { "integer" } else if field == "projection_live" { "bool" } else { "keyword" }}})),
            headers: Vec::new(),
        }).await.expect("index field");
        assert!(
            (200..300).contains(&response.status),
            "index {field} failed: {response:?}"
        );
    }
}

pub async fn delete_collection(fixture: &PublicQdrantFixture) {
    let response = fixture
        .transport
        .execute(
            &fixture.permit,
            IntraCellRequest {
                method: IntraCellMethod::Delete,
                path: format!("/collections/{}", fixture.collection),
                json_body: None,
                headers: Vec::new(),
            },
        )
        .await
        .expect("delete collection");
    assert!(
        (200..300).contains(&response.status),
        "delete failed: {response:?}"
    );
}
