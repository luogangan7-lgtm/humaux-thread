//! T5.4+T5.6 — `adapters::qdrant` (§17) pure-logic tests. No DB, no network: everything this
//! file exercises is request/response JSON shaping and the §17.4 confirmation contract, none
//! of which needs the still-missing HTTP client (see `crates/adapters/src/qdrant.rs`'s module
//! doc for why that dependency is not in this build). The genuinely Qdrant-reachability-gated
//! assertions (§17.1 cross-tenant filter proof, §23.4 injection 2 against a real cluster) are
//! in `tests/qdrant_live.rs`, which reports `not_applicable` naming the missing HTTP client —
//! this file is not where that gap is papered over.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use humaux_adapters::qdrant::{
    DenseQuery, DenseQueryVersions, Distance, PlacementClass, PointId, PromotionState,
    QdrantOperation, QdrantPointPayload, QdrantTransportError, ReadConsistency, RetrievalFamily,
    ShardingMethod, TenantPlacementRow, VisibleCountFilter, WriteOrdering, condition_to_filter,
    count_body, create_collection_body, dense_query_body, ha_profile_for, may_advance_checkpoint,
    query_dense, shard_key_body, tenant_index_body, upsert, upsert_point_body, verify_visible,
    visible_count,
};
use humaux_domain::authority::{AuthorityClass, AuthorityStatus};
use humaux_domain::dataclass::DataClass;
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId, VisibilityClass};
use humaux_domain::ids::{TenantId, UserId, WorkspaceId};
use humaux_domain::memory::MemoryType;
use humaux_infra_cell::{
    CallerId, CellAccessPermit, CellId, IntraCellError, IntraCellHttpTransport, IntraCellMethod,
    IntraCellRequest, IntraCellResource, IntraCellResourceRegistry, IntraCellResponse,
    ResourceEntry, authorize_cell_access,
};
use humaux_projection::card::EgressDisposition;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

fn scope(tenant_id: TenantId, workspaces: &[WorkspaceId]) -> AuthorizationScope {
    AuthorizationScope::new(
        tenant_id,
        PrincipalId::new(),
        None,
        BoundedSet::new(workspaces.iter().copied()).unwrap(),
    )
}

fn dense_query(scope: &AuthorizationScope, vector: Vec<f32>, limit: u32) -> DenseQuery {
    DenseQuery::new(
        scope,
        &TenantPlacementRow {
            tenant_id: scope.tenant_id(),
            projection_family: RetrievalFamily::PrivateMemoryV1,
            collection_name: "private_memory_v1".to_owned(),
            shard_key: Some("tenant-shard".to_owned()),
            placement_class: PlacementClass::Dedicated,
            point_count: 0,
            bytes_estimate: 0,
            promotion_state: PromotionState::Stable,
        },
        DenseQueryVersions {
            projection: "v1",
            embedding: "embed-v1",
        },
        vector,
        limit,
        vec![PointId::Num(99)],
        ha_profile_for(QdrantOperation::ReadYourWriteStrict),
    )
    .expect("valid dense query")
}

fn qdrant_permit() -> CellAccessPermit {
    let cell = CellId(Uuid::now_v7());
    let caller = CallerId("qdrant-contract-test".to_owned());
    let mut entries = BTreeMap::new();
    entries.insert(
        IntraCellResource::QDRANT_REST,
        ResourceEntry::new(
            "127.0.0.1",
            64642,
            cell,
            vec!["127.0.0.1/32".parse().expect("valid loopback CIDR")],
            BTreeSet::from([caller.clone()]),
            false,
        )
        .expect("contract fixture resource"),
    );
    let registry = IntraCellResourceRegistry::new(entries, cell, caller);
    authorize_cell_access(
        &registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(30),
    )
    .expect("allowlisted same-cell test caller")
}

struct RecordedTransport {
    response: IntraCellResponse,
    observed: Arc<Mutex<Option<IntraCellRequest>>>,
}

#[async_trait::async_trait]
impl IntraCellHttpTransport for RecordedTransport {
    async fn execute(
        &self,
        _permit: &CellAccessPermit,
        request: IntraCellRequest,
    ) -> Result<IntraCellResponse, IntraCellError> {
        *self.observed.lock().expect("recording lock") = Some(request);
        Ok(self.response.clone())
    }
}

fn recorded_transport(
    response: IntraCellResponse,
) -> (RecordedTransport, Arc<Mutex<Option<IntraCellRequest>>>) {
    let observed = Arc::new(Mutex::new(None));
    (
        RecordedTransport {
            response,
            observed: Arc::clone(&observed),
        },
        observed,
    )
}

// ---- §17: three retrieval families, verbatim names ----

#[test]
fn retrieval_family_collection_names_match_spec_verbatim() {
    assert_eq!(
        RetrievalFamily::PrivateMemoryV1.collection_name(),
        "private_memory_v1"
    );
    assert_eq!(
        RetrievalFamily::PublicKnowledgeV1.collection_name(),
        "public_knowledge_v1"
    );
    assert_eq!(RetrievalFamily::CodeV1.collection_name(), "code_v1");
    assert_eq!(RetrievalFamily::ALL.len(), 3);
}

// ---- §17.1: tenant keyword index request body ----

#[test]
fn tenant_index_body_is_keyword_is_tenant() {
    let body = tenant_index_body();
    assert_eq!(body["field_name"], "tenant_id");
    assert_eq!(body["field_schema"]["type"], "keyword");
    assert_eq!(body["field_schema"]["is_tenant"], true);
}

#[test]
fn create_collection_body_carries_caller_supplied_size_not_a_hardcoded_one() {
    let a = create_collection_body(1024, Distance::Cosine, 1, 1, 1, ShardingMethod::Auto);
    let b = create_collection_body(768, Distance::Cosine, 1, 1, 1, ShardingMethod::Auto);
    assert_eq!(a["vectors"]["size"], 1024);
    assert_eq!(b["vectors"]["size"], 768);
    assert_eq!(a["vectors"]["distance"], "Cosine");
    assert!(a.get("sharding_method").is_none());
}

// ---- §17.3: PlacementClass::Dedicated needs custom sharding + a shard key, not just a column
// ---- write — this is the minimal failing check that shard_key isn't a decorative field.

#[test]
fn dedicated_placement_collection_body_declares_custom_sharding() {
    let body = create_collection_body(1024, Distance::Cosine, 3, 2, 1, ShardingMethod::Custom);
    assert_eq!(body["sharding_method"], "custom");
    assert_eq!(body["shard_number"], 3);
    assert_eq!(body["replication_factor"], 2);
    assert_eq!(body["write_consistency_factor"], 1);
}

#[test]
fn shard_key_body_carries_the_key_verbatim() {
    let body = shard_key_body("tenant-a");
    assert_eq!(body["shard_key"], "tenant-a");
}

// ---- §17 payload: at least the spec's fields, exact wire spelling ----

fn sample_payload() -> QdrantPointPayload {
    QdrantPointPayload {
        tenant_id: TenantId::new(),
        workspace_id: WorkspaceId::new(),
        visibility_class: VisibilityClass::UserPrivate,
        visibility_user_id: Some(UserId::new()),
        visibility_workspace_id: None,
        object_type: "MEMORY_RECORD".to_string(),
        memory_type: MemoryType::Decision,
        status: AuthorityStatus::Active,
        authority: AuthorityClass::ProjectDecision,
        created_at: OffsetDateTime::UNIX_EPOCH,
        effective_at: OffsetDateTime::UNIX_EPOCH,
        embedding_version: "text-embedding-v3".to_string(),
        projection_version: "card-v2".to_string(),
        source_stream_seq: 42,
        data_class: DataClass::Private,
        egress_disposition: EgressDisposition::Allowed,
    }
}

#[test]
fn payload_json_has_at_least_the_spec_fields() {
    // §17's payload list is a floor ("payload 至少：..."), not a ceiling — this must not
    // assert an exact field count, or adding a spec-mandated field (like data_class below)
    // becomes a breaking test change instead of the fix it is.
    let json = sample_payload().to_json();
    let obj = json.as_object().expect("payload must be a JSON object");
    let expected = [
        "tenant_id",
        "workspace_id",
        "visibility_class",
        "visibility_user_id",
        "visibility_workspace_id",
        "object_type",
        "memory_type",
        "status",
        "authority",
        "created_at",
        "effective_at",
        "embedding_version",
        "projection_version",
        "source_stream_seq",
    ];
    for key in expected {
        assert!(obj.contains_key(key), "missing §17 payload field `{key}`");
    }
}

#[test]
fn payload_carries_data_class_and_egress_disposition_wire_forms() {
    // §18.2: "没有 data_class/egress_disposition，SECRET_MATERIAL 在出境这一侧不可判定" — both
    // must be present on the index-write side, not just the card-build side.
    let mut p = sample_payload();
    p.data_class = DataClass::Sensitive;
    p.egress_disposition = EgressDisposition::PolicyGated;
    let json = p.to_json();
    assert_eq!(json["data_class"], "SENSITIVE");
    assert_eq!(json["egress_disposition"], "POLICY_GATED");
}

#[test]
fn secret_material_payload_cannot_become_an_indexable_payload() {
    // §18.2's index-write gate, at the type level: into_indexable() is the only way to reach
    // upsert_point_body, and it must refuse SECRET_MATERIAL.
    let mut p = sample_payload();
    p.data_class = DataClass::SecretMaterial;
    assert!(p.into_indexable().is_none());
}

#[test]
fn non_secret_payload_becomes_indexable_and_round_trips_through_upsert_body() {
    let p = sample_payload()
        .into_indexable()
        .expect("PRIVATE is indexable");
    let body = upsert_point_body(PointId::Num(7), &p);
    assert_eq!(body["id"], 7);
    assert_eq!(body["payload"]["source_stream_seq"], 42);
}

#[test]
fn payload_visibility_class_wire_matches_dense_rs_filter_spelling() {
    // adapters::qdrant's payload writer and projection::dense's query filter must agree on
    // this spelling (§6.1.2) or filtering silently returns nothing — this pins both sides.
    let mut p = sample_payload();
    p.visibility_class = VisibilityClass::UserPrivate;
    assert_eq!(p.to_json()["visibility_class"], "USER_PRIVATE");
    p.visibility_class = VisibilityClass::WorkspaceShared;
    assert_eq!(p.to_json()["visibility_class"], "WORKSPACE_SHARED");
    p.visibility_class = VisibilityClass::TenantShared;
    assert_eq!(p.to_json()["visibility_class"], "TENANT_SHARED");
}

#[test]
fn payload_memory_type_wire_matches_memory_records_check_constraint() {
    // §8.5 / migrations/0004_private_evidence_memory.sql's 12-value CHECK list, UPPER_SNAKE.
    let cases = [
        (MemoryType::Fact, "FACT"),
        (MemoryType::Preference, "PREFERENCE"),
        (MemoryType::Decision, "DECISION"),
        (MemoryType::Rejection, "REJECTION"),
        (MemoryType::State, "STATE"),
        (MemoryType::Issue, "ISSUE"),
        (MemoryType::Lesson, "LESSON"),
        (MemoryType::Constraint, "CONSTRAINT"),
        (MemoryType::Procedure, "PROCEDURE"),
        (MemoryType::Outcome, "OUTCOME"),
        (MemoryType::Reference, "REFERENCE"),
        (MemoryType::Note, "NOTE"),
    ];
    let mut p = sample_payload();
    for (variant, wire) in cases {
        p.memory_type = variant;
        assert_eq!(p.to_json()["memory_type"], wire);
    }
}

#[test]
fn payload_authority_class_wire_is_pascalcase_verbatim() {
    // §53.2 repo convention + migrations/0004's authority_class CHECK list, PascalCase.
    let mut p = sample_payload();
    p.authority = AuthorityClass::ExplicitTaskContext;
    assert_eq!(p.to_json()["authority"], "ExplicitTaskContext");
    p.authority = AuthorityClass::UserCorrection;
    assert_eq!(p.to_json()["authority"], "UserCorrection");
}

#[test]
fn payload_status_wire_is_lowercase() {
    // migrations/0004's `status` column comment: "DB wire values lowercase".
    let mut p = sample_payload();
    p.status = AuthorityStatus::Superseded;
    assert_eq!(p.to_json()["status"], "superseded");
}

#[test]
fn payload_optional_visibility_ids_are_null_when_absent() {
    let mut p = sample_payload();
    p.visibility_user_id = None;
    p.visibility_workspace_id = None;
    let json = p.to_json();
    assert!(json["visibility_user_id"].is_null());
    assert!(json["visibility_workspace_id"].is_null());
}

// ---- §6.1.2/§17.1: DenseQueryFilter -> Qdrant filter JSON (built via build_dense_filter,
// never hand-assembled — a hand-written Condition::Eq{"tenant_id",..} fixture here would
// itself demonstrate the exact bypass §17.1 forbids business code from performing) ----

#[test]
fn condition_to_filter_translates_scope_to_must_with_nested_visibility_disjunction() {
    let tenant_id = TenantId::new();
    let ws = WorkspaceId::new();
    let s = scope(tenant_id, &[ws]);
    let filter = humaux_projection::dense::build_dense_filter(&s, &[]);

    let json = condition_to_filter(&filter);
    let must = json["must"].as_array().expect("top-level must array");
    // tenant clause + visibility disjunction (no narrow_by terms).
    assert_eq!(must.len(), 2);
    assert_eq!(must[0]["key"], "tenant_id");
    assert_eq!(must[0]["match"]["value"], tenant_id.0.to_string());

    // visibility_disjunction: TenantShared arm + WorkspaceShared arm (scope has no user_id).
    let should = must[1]["should"].as_array().expect("nested should array");
    assert_eq!(should.len(), 2);
    assert_eq!(should[0]["key"], "visibility_class");
    assert_eq!(should[0]["match"]["value"], "TENANT_SHARED");
    // WorkspaceShared arm is itself a nested And -> {"must": [...]}.
    let ws_arm = should[1]["must"].as_array().expect("nested must array");
    assert_eq!(ws_arm[0]["match"]["value"], "WORKSPACE_SHARED");
    assert_eq!(ws_arm[1]["match"]["any"][0], ws.0.to_string());
}

// ---- §17.5: HA consistency profile per operation ----

#[test]
fn normal_upsert_is_weak_ordering_throughput_oriented() {
    let p = ha_profile_for(QdrantOperation::NormalImmutableUpsert);
    assert_eq!(p.write_ordering, WriteOrdering::Weak);
    assert!(p.read_consistency.is_none());
}

#[test]
fn correction_delete_supersede_is_strong_ordering() {
    let p = ha_profile_for(QdrantOperation::CorrectionDeleteSupersede);
    assert_eq!(p.write_ordering, WriteOrdering::Strong);
}

#[test]
fn read_your_write_strict_path_uses_quorum_or_stronger_read_consistency() {
    let p = ha_profile_for(QdrantOperation::ReadYourWriteStrict);
    assert!(
        matches!(
            p.read_consistency,
            Some(ReadConsistency::Quorum | ReadConsistency::Majority | ReadConsistency::All)
        ),
        "expected quorum/majority/all, got {:?}",
        p.read_consistency
    );
}

// ---- §16.3/§23.1②: visible count filter must be tagged with projection_version ----
#[test]
fn visible_count_filter_refuses_an_empty_projection_version() {
    let s = scope(TenantId::new(), &[]);
    assert!(VisibleCountFilter::new(&s, "").is_none());
}

#[test]
fn count_body_is_exact_and_carries_the_projection_version_clause() {
    let s = scope(TenantId::new(), &[]);
    let filter = VisibleCountFilter::new(&s, "card-v2").expect("non-empty version");
    let body = count_body(&filter);
    assert_eq!(body["exact"], true);
    let must = body["filter"]["must"].as_array().expect("must array");
    // tenant clause + visibility disjunction + projection_version narrow_by term.
    assert_eq!(must.len(), 3);
    assert_eq!(must[2]["key"], "projection_version");
    assert_eq!(must[2]["match"]["value"], "card-v2");
}

#[test]
fn dense_query_body_keeps_scope_version_tombstone_and_shard_in_one_request() {
    let tenant = TenantId::new();
    let user = UserId::new();
    let authorized = AuthorizationScope::new(
        tenant,
        PrincipalId::new(),
        Some(user),
        BoundedSet::new(Vec::<WorkspaceId>::new()).expect("empty workspace set is valid"),
    );
    let query = dense_query(&authorized, vec![0.1, 0.2], 3);
    let body = dense_query_body(&query);
    assert_eq!(body["query"], serde_json::json!([0.1_f32, 0.2_f32]));
    assert_eq!(body["limit"], 3);
    assert_eq!(body["with_payload"], false);
    assert_eq!(body["with_vector"], false);
    assert_eq!(body["shard_key"], "tenant-shard");
    assert!(body.get("consistency").is_none());
    let must = body["filter"]["must"].as_array().expect("scope filter");
    assert_eq!(must[0]["key"], "tenant_id");
    assert_eq!(must[0]["match"]["value"], tenant.0.to_string());
    let private_arm = must[1]["should"][1]["must"]
        .as_array()
        .expect("private visibility arm");
    assert_eq!(private_arm[0]["match"]["value"], "USER_PRIVATE");
    assert_eq!(
        private_arm[1]["match"]["value"],
        user.0.to_string(),
        "private vectors remain bound to the authorized user"
    );
    assert_eq!(must[2]["key"], "projection_version");
    assert_eq!(must[2]["match"]["value"], "v1");
    assert_eq!(must[3]["key"], "embedding_version");
    assert_eq!(must[3]["match"]["value"], "embed-v1");
    assert_eq!(body["filter"]["must_not"][0]["has_id"][0], 99);
}

#[test]
fn dense_query_rejects_empty_non_finite_or_zero_limit_inputs() {
    let scope = scope(TenantId::new(), &[]);
    let placement = TenantPlacementRow {
        tenant_id: scope.tenant_id(),
        projection_family: RetrievalFamily::PrivateMemoryV1,
        collection_name: "private_memory_v1".to_owned(),
        shard_key: None,
        placement_class: PlacementClass::SharedFallback,
        point_count: 0,
        bytes_estimate: 0,
        promotion_state: PromotionState::Stable,
    };
    for (vector, limit, version) in [
        (vec![], 1, "v1"),
        (vec![f32::NAN], 1, "v1"),
        (vec![f32::INFINITY], 1, "v1"),
        (vec![0.1], 0, "v1"),
        (vec![0.1], 1, ""),
    ] {
        assert!(
            DenseQuery::new(
                &scope,
                &placement,
                DenseQueryVersions {
                    projection: version,
                    embedding: "embed-v1",
                },
                vector,
                limit,
                vec![],
                ha_profile_for(QdrantOperation::NormalImmutableUpsert),
            )
            .is_err()
        );
    }
    assert!(
        DenseQuery::new(
            &scope,
            &placement,
            DenseQueryVersions {
                projection: "v1",
                embedding: "",
            },
            vec![0.1],
            1,
            vec![],
            ha_profile_for(QdrantOperation::NormalImmutableUpsert),
        )
        .is_err()
    );
    let foreign_placement = TenantPlacementRow {
        tenant_id: TenantId::new(),
        collection_name: "bad/collection".to_owned(),
        ..placement
    };
    assert!(
        DenseQuery::new(
            &scope,
            &foreign_placement,
            DenseQueryVersions {
                projection: "v1",
                embedding: "embed-v1",
            },
            vec![0.1],
            1,
            vec![],
            ha_profile_for(QdrantOperation::ReadYourWriteStrict),
        )
        .is_err()
    );
}

#[tokio::test]
async fn dense_query_uses_post_query_path_and_returns_typed_candidates() {
    let scope = scope(TenantId::new(), &[]);
    let query = dense_query(&scope, vec![0.1, 0.2], 2);
    let (transport, observed) = recorded_transport(IntraCellResponse {
        status: 200,
        json_body: Some(serde_json::json!({
            "result": {"points": [{"id": 7, "score": 0.9}]}
        })),
    });

    let candidates = query_dense(&transport, &qdrant_permit(), &query)
        .await
        .expect("well-formed dense response");

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].point_id, PointId::Num(7));
    assert_eq!(candidates[0].score, 0.9);
    let request = observed
        .lock()
        .expect("recording lock")
        .clone()
        .expect("query request");
    assert_eq!(request.method, IntraCellMethod::Post);
    assert_eq!(
        request.path,
        "/collections/private_memory_v1/points/query?consistency=quorum"
    );
    assert_eq!(request.json_body, Some(dense_query_body(&query)));
    assert!(
        request
            .json_body
            .as_ref()
            .is_some_and(|body| body.get("consistency").is_none())
    );
}

#[tokio::test]
async fn dense_query_rejects_malformed_or_duplicate_candidates() {
    let scope = scope(TenantId::new(), &[]);
    let query = dense_query(&scope, vec![0.1, 0.2], 2);
    for body in [
        serde_json::json!({"result": {"points": [{"id": 7, "score": "bad"}]}}),
        serde_json::json!({"result": {"points": [{"id": 7, "score": 0.9}, {"id": 7, "score": 0.8}]}}),
        serde_json::json!({"result": {"points": [{"id": 7, "score": 0.9}, {"id": 8, "score": 0.8}, {"id": 9, "score": 0.7}]}}),
        serde_json::json!({"result": {}}),
    ] {
        let (transport, _) = recorded_transport(IntraCellResponse {
            status: 200,
            json_body: Some(body),
        });
        assert!(matches!(
            query_dense(&transport, &qdrant_permit(), &query).await,
            Err(QdrantTransportError::UnexpectedResponseShape(_))
        ));
    }
}

#[tokio::test]
async fn upsert_controls_are_uri_parameters_not_point_body_fields() {
    let payload = sample_payload()
        .into_indexable()
        .expect("sample payload is indexable");
    let (transport, observed) = recorded_transport(IntraCellResponse {
        status: 200,
        json_body: Some(serde_json::json!({})),
    });
    upsert(
        &transport,
        &qdrant_permit(),
        "private_memory_v1",
        &[(PointId::Num(7), &payload, vec![0.1, 0.2])],
        ha_profile_for(QdrantOperation::CorrectionDeleteSupersede),
    )
    .await
    .expect("accepted upsert response");
    let request = observed
        .lock()
        .expect("recording lock")
        .clone()
        .expect("upsert request");
    assert_eq!(
        request.path,
        "/collections/private_memory_v1/points?wait=true&ordering=strong"
    );
    let body = request.json_body.expect("upsert body");
    assert!(body.get("points").is_some());
    assert!(body.get("wait").is_none());
    assert!(body.get("ordering").is_none());
}

#[tokio::test]
async fn dense_query_preserves_http_failure_without_candidate_fallback() {
    let scope = scope(TenantId::new(), &[]);
    let query = dense_query(&scope, vec![0.1, 0.2], 2);
    let (transport, _) = recorded_transport(IntraCellResponse {
        status: 503,
        json_body: Some(serde_json::json!({"status": {"error": "unavailable"}})),
    });
    assert!(matches!(
        query_dense(&transport, &qdrant_permit(), &query).await,
        Err(QdrantTransportError::NonSuccessStatus { status: 503, .. })
    ));
}

#[test]
fn visible_count_subtracts_tombstones_and_saturates_at_zero() {
    assert_eq!(visible_count(10, 3), 7);
    assert_eq!(visible_count(2, 5), 0, "stale tombstone must not underflow");
}

// ---- §17.4: search-visible confirmation contract + the injected fault (§23.4 injection 2) ----

#[test]
fn verify_visible_confirms_when_every_id_round_trips_a_real_search() {
    let ids = vec![PointId::Num(1), PointId::Num(2), PointId::Num(3)];
    // Honest `check_visible`: a real search really did return all three.
    let confirmation = verify_visible(&ids, |queried| queried.to_vec());
    assert!(confirmation.is_some());
    let confirmation = confirmation.unwrap();
    assert!(may_advance_checkpoint(&confirmation, &ids));
}

/// §17.4/§23.4 注入 2's actual red/green record: deleting `verify_visible`'s `all_confirmed`
/// check (i.e. always returning `Some`, exactly what an ack-only `check_visible` would let
/// through undetected) flips this test from pass to fail — confirmed by mutation: temporarily
/// replacing `verify_visible`'s body with
/// `Some(VisibilityConfirmation::from_verified_search(point_ids.to_vec()))` (skipping the
/// check) makes this the only test in the crate that turns red. A second test built from an
/// `|queried| queried.to_vec()` "ack-only" closure would be indistinguishable from
/// `verify_visible_confirms_when_every_id_round_trips_a_real_search` above (literally the same
/// closure) and therefore vacuously green under both a correct and a broken implementation —
/// that shape was tried and removed for being exactly such a false record.
#[test]
fn verify_visible_withholds_confirmation_when_some_ids_are_not_yet_visible() {
    let ids = vec![PointId::Num(1), PointId::Num(2), PointId::Num(3)];
    // Honest `check_visible`: an unoptimized segment means id 3 is not searchable yet.
    let confirmation = verify_visible(&ids, |queried| {
        queried
            .iter()
            .copied()
            .filter(|id| *id != PointId::Num(3))
            .collect()
    });
    assert!(
        confirmation.is_none(),
        "§17.4: a partial search result must never yield a confirmation eligible to advance the checkpoint"
    );
}

#[test]
fn verify_visible_confirmation_never_carries_ids_outside_the_requested_batch() {
    // check_visible returns a superset of the requested batch (e.g. a scroll page touching
    // neighboring ids) — the confirmation must only ever attest to the batch actually asked
    // for, never silently license advancing a checkpoint past ids nobody verified.
    let ids = vec![PointId::Num(1), PointId::Num(2)];
    let confirmation = verify_visible(&ids, |queried| {
        let mut observed = queried.to_vec();
        observed.push(PointId::Num(999));
        observed
    })
    .expect("both requested ids round-tripped");
    assert!(!confirmation.contains(&PointId::Num(999)));
}

// ---- §17.3: placement_class / promotion_state closed sets (Rust side of the migration
// 0068 CHECK contract test; the DB side lives in tests/tenant_placements_migration.rs) ----

#[test]
fn placement_class_db_strings_match_migration_0068_literal_check_values() {
    assert_eq!(
        PlacementClass::SharedFallback.as_db_str(),
        "SHARED_FALLBACK"
    );
    assert_eq!(PlacementClass::Dedicated.as_db_str(), "DEDICATED");
    assert_eq!(PlacementClass::ALL.len(), 2);
}

#[test]
fn promotion_state_db_strings_match_migration_0068_literal_check_values() {
    assert_eq!(PromotionState::Stable.as_db_str(), "STABLE");
    assert_eq!(
        PromotionState::PromotionPending.as_db_str(),
        "PROMOTION_PENDING"
    );
    assert_eq!(PromotionState::Promoted.as_db_str(), "PROMOTED");
    assert_eq!(PromotionState::ALL.len(), 3);
}
