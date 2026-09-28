//! `adapters::tests::private_projection_registry` — Ignored real-PostgreSQL acceptance for the private Qdrant point
//!   registry.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-infra-cell, humaux-projection, humaux-testkit,
//!   postgres, serde_json, sqlx, tokio]; services=[PostgreSQL(any) r=[ops.commit_seq_seq] w=[control.memberships,
//!   control.private_reasoning_domains, control.reasoning_domain_grants, control.tenants, control.users, ops.outbox,
//!   private.events, private.evidence_objects, private.memory_evidence, private.memory_records,
//!   projection.private_memory_points, projection.stream_checkpoints, projection.stream_log],
//!   PostgreSQL(role_gateway), PostgreSQL(role_retrieval_worker), Qdrant(*)]; env=[HUMAUX_TEST_PG_DSN,
//!   HUMAUX_TEST_QDRANT_PORT]; modules=[adapters::postgres, adapters::private_projection_registry, adapters::qdrant,
//!   adapters::read_materialize, adapters::retrieve, domain::authority, domain::dataclass, domain::identity,
//!   domain::ids, domain::memory, humaux-testkit, infra-cell::permit, infra-cell::resource, infra-cell::transport,
//!   projection::card, projection::serving]
//! Called-by: [cargo-test]
//! Invariants: [point ids bind to exactly one memory; cross-workspace, collision and unsupported point ids are typed
//!   errors; needs the isolated migrated PG + Qdrant fixture, so the tests are #[ignore] lane tests]
//! Spec: Baseline §17.4; §79.2
//!
//! Run explicitly against the isolated migrated fixture:
//! `cargo test -p humaux-adapters --test private_projection_registry -- --ignored`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use humaux_adapters::postgres::{RetrievalWorkerDbPool, RuntimeDbPool};
use humaux_adapters::private_projection_registry::{
    PrivateMemoryPointRegistration, PrivateProjectionRegistryError, ProjectionPointId,
    RegistrationOutcome, register_private_memory_point, resolve_private_memory_points,
    retire_private_memory_point,
};
use humaux_adapters::qdrant::{
    DenseCandidate, DenseQuery, DenseQueryVersions, Distance, PlacementClass, PointId,
    PromotionState, QdrantOperation, QdrantPointPayload, RetrievalFamily, ShardingMethod,
    TenantPlacementRow, create_collection_body, ha_profile_for, query_dense, tenant_index_body,
    upsert,
};
use humaux_adapters::read_materialize::MaterializedItem;
use humaux_adapters::retrieve::{
    ProcessingState, RetrieveError, TokenClaims, issue_consistency_token,
    materialize_private_read_serving,
};
use humaux_domain::authority::{AuthorityClass, AuthorityStatus, MemoryId};
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
use humaux_projection::serving::StreamFamily;
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

fn qdrant_port() -> u16 {
    std::env::var("HUMAUX_TEST_QDRANT_PORT")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(6333)
}

fn qdrant_registry(cell: CellId, caller: CallerId) -> IntraCellResourceRegistry {
    let mut entries = BTreeMap::new();
    entries.insert(
        IntraCellResource::QDRANT_REST,
        ResourceEntry::new(
            "127.0.0.1",
            qdrant_port(),
            cell,
            vec!["127.0.0.1/32".parse().expect("loopback CIDR")],
            BTreeSet::from([caller.clone()]),
            false,
        )
        .expect("loopback Qdrant resource"),
    );
    IntraCellResourceRegistry::new(entries, cell, caller)
}

async fn delete_qdrant_collection(
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
        .map_err(|error| error.to_string())?;
    if (200..300).contains(&response.status) {
        Ok(())
    } else {
        Err(format!("Qdrant cleanup returned {}", response.status))
    }
}

fn fixed_time(day: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_704_067_200 + day * 86_400).unwrap()
}

struct Handle {
    rt: tokio::runtime::Runtime,
    admin: Client,
    dsn: String,
    runtime: RuntimeDbPool,
    worker: RetrievalWorkerDbPool,
    tenant_id: Uuid,
    user_id: Uuid,
    memory_id: Uuid,
    body_sha256: Vec<u8>,
    authorization: AuthorizationScope,
    family: StreamFamily,
    extra_tenants: Vec<(Uuid, Uuid)>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        for (tenant_id, user_id) in &self.extra_tenants {
            self.admin
                .batch_execute(&format!(
                    "DELETE FROM projection.private_memory_points WHERE tenant_id = '{tenant_id}'; \
                     DELETE FROM projection.stream_log WHERE tenant_id = '{tenant_id}'; \
                     DELETE FROM projection.stream_checkpoints WHERE tenant_id = '{tenant_id}'; \
                     DELETE FROM ops.outbox WHERE tenant_id = '{tenant_id}'; \
                     DELETE FROM private.memory_evidence WHERE memory_id IN (SELECT memory_id FROM private.memory_records WHERE tenant_id = '{tenant_id}'); \
                     DELETE FROM private.memory_records WHERE tenant_id = '{tenant_id}'; \
                     DELETE FROM private.events WHERE event_id IN (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id = '{tenant_id}'); \
                     DELETE FROM private.evidence_objects WHERE tenant_id = '{tenant_id}'; \
                     DELETE FROM control.reasoning_domain_grants WHERE reasoning_domain_id IN (SELECT reasoning_domain_id FROM control.private_reasoning_domains WHERE tenant_id = '{tenant_id}'); \
                     DELETE FROM control.private_reasoning_domains WHERE tenant_id = '{tenant_id}'; \
                     DELETE FROM control.memberships WHERE tenant_id = '{tenant_id}'; \
                     DELETE FROM control.users WHERE user_id = '{user_id}'; \
                     DELETE FROM control.tenants WHERE tenant_id = '{tenant_id}';"
                ))
                .expect("extra registry fixture cleanup");
        }
        self.admin
            .batch_execute(&format!(
                "DELETE FROM projection.private_memory_points WHERE tenant_id = '{0}'; \
                 DELETE FROM projection.stream_log WHERE tenant_id = '{0}'; \
                 DELETE FROM projection.stream_checkpoints WHERE tenant_id = '{0}'; \
                 DELETE FROM ops.outbox WHERE tenant_id = '{0}'; \
                 DELETE FROM private.memory_evidence WHERE memory_id IN (SELECT memory_id FROM private.memory_records WHERE tenant_id = '{0}'); \
                 DELETE FROM private.memory_records WHERE tenant_id = '{0}'; \
                 DELETE FROM private.events WHERE event_id IN (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id = '{0}'); \
                 DELETE FROM private.evidence_objects WHERE tenant_id = '{0}'; \
                 DELETE FROM control.reasoning_domain_grants WHERE reasoning_domain_id IN (SELECT reasoning_domain_id FROM control.private_reasoning_domains WHERE tenant_id = '{0}'); \
                 DELETE FROM control.private_reasoning_domains WHERE tenant_id = '{0}'; \
                 DELETE FROM control.memberships WHERE tenant_id = '{0}'; \
                 DELETE FROM control.users WHERE user_id = '{1}'; \
                 DELETE FROM control.tenants WHERE tenant_id = '{0}';",
                self.tenant_id, self.user_id
            ))
            .expect("registry fixture cleanup");
    }
}

struct RegistryFixture;

fn seed_memory(
    admin: &mut Client,
    tenant_id: Uuid,
    user_id: Uuid,
    content: &serde_json::Value,
) -> Result<(Uuid, Vec<u8>), postgres::Error> {
    admin.execute(
        "INSERT INTO control.tenants(tenant_id, name, state) VALUES($1,$2,'ACTIVE')",
        &[&tenant_id, &format!("private-point-{tenant_id}")],
    )?;
    admin.execute(
        "INSERT INTO control.users(user_id, state) VALUES($1,'ACTIVE')",
        &[&user_id],
    )?;
    admin.execute(
        "INSERT INTO control.memberships(tenant_id,user_id,role,state) VALUES($1,$2,'member','ACTIVE')",
        &[&tenant_id, &user_id],
    )?;
    let reasoning_domain_id: Uuid = admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains(tenant_id,name) VALUES($1,'private projection registry fixture') RETURNING reasoning_domain_id",
            &[&tenant_id],
        )?
        .get(0);
    let evidence_id: Uuid = admin
        .query_one(
            "INSERT INTO private.evidence_objects(tenant_id,evidence_kind,payload_sha256,data_class,origin_class,visibility_class,reasoning_domain_id) \
             VALUES($1,'EVENT',sha256(convert_to('{}','UTF8')),'INTERNAL','DirectUserInput','TENANT_SHARED',$2) RETURNING evidence_id",
            &[&tenant_id, &reasoning_domain_id],
        )?
        .get(0);
    admin.execute(
        "INSERT INTO private.events(event_id,event_kind,payload) VALUES($1,'USER_MESSAGE','{}')",
        &[&evidence_id],
    )?;
    let mut txn = admin.transaction()?;
    let row = txn.query_one(
        "INSERT INTO private.memory_records \
            (tenant_id,memory_type,content,visibility_class,authority_class,confidence,status,asserted_at,updated_at) \
         VALUES($1,'FACT',$2,'TENANT_SHARED','PrivateKnowledge',1,'active','2024-01-01T00:00:00Z','2024-01-01T00:00:00Z') \
         RETURNING memory_id,sha256(convert_to(content::text,'UTF8'))",
        &[&tenant_id, content],
    )?;
    let memory_id: Uuid = row.get(0);
    let body_sha256: Vec<u8> = row.get(1);
    // dep: PostgreSQL(any) — pool/txn query execution
    txn.execute(
        "INSERT INTO private.memory_evidence(memory_id,evidence_id,role) VALUES($1,$2,'SUPPORTING')",
        &[&memory_id, &evidence_id],
    )?;
    txn.commit()?;
    Ok((memory_id, body_sha256))
}

impl DbIntegrationFixture for RegistryFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
        let ready: bool = admin
            .query_one(
                "SELECT to_regclass('projection.private_memory_points') IS NOT NULL \
                    AND has_table_privilege('role_gateway', 'projection.private_memory_points', 'SELECT') \
                    AND NOT has_table_privilege('role_gateway', 'projection.private_memory_points', 'INSERT')",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(format!("{e:?}")))?
            .get(0);
        if !ready {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "0119 private projection registry/RLS/grants are not applied to HUMAUX_TEST_PG_DSN"
                    .to_owned(),
            ));
        }

        let tenant_id = Uuid::new_v4();
        let user_id = Uuid::new_v4();
        let content = serde_json::json!({"body": "registry source"});
        let (memory_id, body_sha256) = seed_memory(&mut admin, tenant_id, user_id, &content)
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(format!("{e:?}")))?;

        let authorization = AuthorizationScope::new(
            TenantId(tenant_id),
            PrincipalId(user_id),
            Some(UserId(user_id)),
            BoundedSet::<WorkspaceId>::new([]).map_err(|e| {
                DbFixtureSkipReason::IsolationSetupFailed(format!("authorization scope: {e:?}"))
            })?,
        );
        let family = StreamFamily::new(
            TenantId(tenant_id),
            "tenant",
            tenant_id,
            "knowledge",
            "dense",
        );
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(format!("{e:?}")))?;
        let runtime = rt
            // dep: PostgreSQL(role_gateway) — open a role-scoped PG connection/pool for this test
            .block_on(RuntimeDbPool::connect(&dsn_as_role(&dsn, "role_gateway")))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(format!("{e:?}")))?;
        let worker = rt
            // dep: PostgreSQL(role_retrieval_worker) — open a role-scoped PG connection/pool for this test
            .block_on(RetrievalWorkerDbPool::connect(&dsn_as_role(
                &dsn,
                "role_retrieval_worker",
            )))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(format!("{e:?}")))?;
        Ok(Handle {
            rt,
            admin,
            dsn,
            runtime,
            worker,
            tenant_id,
            user_id,
            memory_id,
            body_sha256,
            authorization,
            family,
            extra_tenants: Vec::new(),
        })
    }
}

fn registration(handle: &Handle, point_id: ProjectionPointId) -> PrivateMemoryPointRegistration {
    PrivateMemoryPointRegistration {
        point_id,
        family: handle.family.clone(),
        projection_version: "private-v1".to_owned(),
        embedding_version: "embed-v1".to_owned(),
        memory_id: MemoryId(handle.memory_id),
        source_updated_at: fixed_time(0),
        body_sha256: handle.body_sha256.clone(),
    }
}

fn seed_ryw_overlay(handle: &mut Handle, stream_seq: i64) -> (Uuid, String) {
    let evidence_id = Uuid::new_v4();
    let reasoning_domain_id: Uuid = handle
        .admin
        .query_one(
            "SELECT reasoning_domain_id FROM control.private_reasoning_domains \
             WHERE tenant_id=$1 ORDER BY reasoning_domain_id LIMIT 1",
            &[&handle.tenant_id],
        )
        .expect("read fixture reasoning domain")
        .get(0);
    handle
        .admin
        .execute(
            "INSERT INTO private.evidence_objects \
               (evidence_id,tenant_id,evidence_kind,payload_sha256,data_class,origin_class,visibility_class,reasoning_domain_id) \
             VALUES($1,$2,'EVENT',sha256(convert_to('{}','UTF8')),'INTERNAL','DirectUserInput','TENANT_SHARED',$3)",
            &[&evidence_id, &handle.tenant_id, &reasoning_domain_id],
        )
        .expect("insert RYW evidence");
    handle
        .admin
        .execute(
            "INSERT INTO private.events(event_id,event_kind,payload) \
             VALUES($1,'USER_MESSAGE',$2)",
            &[
                &evidence_id,
                &serde_json::json!({"body": "not yet projected"}),
            ],
        )
        .expect("insert RYW event");
    handle
        .admin
        .execute(
            // §16.2: `serving` is what `serving_repo::serving_version_in_txn` selects on, and it
            // defaults to false. Without it the token-free
            // `materialize_private_read_serving` leg below can only answer
            // `ServingProjectionChanged` — the fail-closed verdict, on every database, which is
            // not the read this fixture is here to exercise.
            "INSERT INTO projection.stream_checkpoints \
               (tenant_id,scope_kind,scope_id,domain,projection_kind,projection_version,serving) \
             VALUES($1,'tenant',$1,'knowledge','dense','private-v1',true) \
             ON CONFLICT (tenant_id,scope_kind,scope_id,domain,projection_kind,projection_version) \
             DO UPDATE SET serving = true",
            &[&handle.tenant_id],
        )
        .expect("register RYW stream");
    let commit_seq: i64 = handle
        .admin
        .query_one("SELECT nextval('ops.commit_seq_seq')", &[])
        .expect("allocate RYW commit sequence")
        .get(0);
    handle
        .admin
        .execute(
            "INSERT INTO projection.stream_log \
               (tenant_id,scope_kind,scope_id,domain,projection_kind,projection_version,stream_seq,commit_seq,state) \
             VALUES($1,'tenant',$1,'knowledge','dense','private-v1',$2,$3,'ISSUED')",
            &[&handle.tenant_id, &stream_seq, &commit_seq],
        )
        .expect("insert RYW stream row");
    handle
        .admin
        .execute(
            "INSERT INTO ops.outbox(tenant_id,commit_seq,stream_seq,event_type,evidence_id) \
             VALUES($1,$2,$3,'EVIDENCE_ACCEPTED',$4)",
            &[&handle.tenant_id, &commit_seq, &stream_seq, &evidence_id],
        )
        .expect("insert RYW outbox row");
    let now = OffsetDateTime::now_utc();
    let token = issue_consistency_token(&TokenClaims {
        tenant_id: handle.tenant_id,
        workspace_id: None,
        scope_kind: "tenant".to_owned(),
        scope_id: handle.tenant_id,
        domain: "knowledge".to_owned(),
        projection_kind: "dense".to_owned(),
        projection_version: "private-v1".to_owned(),
        stream_seq,
        commit_seq,
        issued_at: now,
        expires_at: now + Duration::from_secs(300),
    });
    (evidence_id, token)
}

async fn create_qdrant_collection(
    transport: &HttpIntraCellTransport,
    permit: &humaux_infra_cell::CellAccessPermit,
    collection: &str,
) -> Result<(), String> {
    let create = transport
        .execute(
            permit,
            // dep: Qdrant(*) — Qdrant wire call for this fixture
            IntraCellRequest {
                method: IntraCellMethod::Put,
                path: format!("/collections/{collection}"),
                json_body: Some(create_collection_body(
                    4,
                    Distance::Cosine,
                    1,
                    1,
                    1,
                    ShardingMethod::Auto,
                )),
                headers: Vec::new(),
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    if !(200..300).contains(&create.status) {
        return Err(format!("Qdrant create returned {}", create.status));
    }
    let index = transport
        .execute(
            permit,
            // dep: Qdrant(*) — Qdrant wire call for this fixture
            IntraCellRequest {
                method: IntraCellMethod::Put,
                path: format!("/collections/{collection}/index"),
                json_body: Some(tenant_index_body()),
                headers: Vec::new(),
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    if (200..300).contains(&index.status) {
        Ok(())
    } else {
        Err(format!("Qdrant tenant index returned {}", index.status))
    }
}

async fn upsert_private_qdrant_points(
    transport: &HttpIntraCellTransport,
    permit: &humaux_infra_cell::CellAccessPermit,
    collection: &str,
    handle: &Handle,
    point: ProjectionPointId,
) -> Result<(), String> {
    let payload = QdrantPointPayload {
        tenant_id: TenantId(handle.tenant_id),
        workspace_id: WorkspaceId(handle.tenant_id),
        visibility_class: VisibilityClass::TenantShared,
        visibility_user_id: None,
        visibility_workspace_id: None,
        object_type: "memory_record".to_owned(),
        memory_type: MemoryType::Fact,
        status: AuthorityStatus::Active,
        authority: AuthorityClass::PrivateKnowledge,
        created_at: fixed_time(0),
        effective_at: fixed_time(0),
        embedding_version: "embed-v1".to_owned(),
        projection_version: "private-v1".to_owned(),
        source_stream_seq: 1,
        data_class: DataClass::Private,
        egress_disposition: EgressDisposition::Forbidden,
    };
    let wrong_embedding = QdrantPointPayload {
        embedding_version: "embed-v2".to_owned(),
        ..payload.clone()
    };
    let payload = payload
        .into_indexable()
        .ok_or_else(|| "private payload unexpectedly not indexable".to_owned())?;
    let wrong_embedding = wrong_embedding
        .into_indexable()
        .ok_or_else(|| "wrong-embedding payload unexpectedly not indexable".to_owned())?;
    upsert(
        transport,
        permit,
        collection,
        &[
            (
                PointId::Uuid(point.as_uuid()),
                &payload,
                vec![0.1, 0.2, 0.3, 0.4],
            ),
            (
                PointId::Uuid(Uuid::now_v7()),
                &wrong_embedding,
                vec![0.1, 0.2, 0.3, 0.4],
            ),
        ],
        ha_profile_for(QdrantOperation::NormalImmutableUpsert),
    )
    .await
    .map_err(|error| error.to_string())
}

async fn query_private_qdrant_points(
    transport: &HttpIntraCellTransport,
    permit: &humaux_infra_cell::CellAccessPermit,
    collection: &str,
    handle: &Handle,
    point: ProjectionPointId,
) -> Result<Vec<DenseCandidate>, String> {
    let placement = TenantPlacementRow {
        tenant_id: TenantId(handle.tenant_id),
        projection_family: RetrievalFamily::PrivateMemoryV1,
        collection_name: collection.to_owned(),
        shard_key: None,
        placement_class: PlacementClass::SharedFallback,
        point_count: 2,
        bytes_estimate: 0,
        promotion_state: PromotionState::Stable,
    };
    let query = DenseQuery::new(
        &handle.authorization,
        &placement,
        DenseQueryVersions {
            projection: "private-v1",
            embedding: "embed-v1",
        },
        vec![0.1, 0.2, 0.3, 0.4],
        2,
        Vec::new(),
        ha_profile_for(QdrantOperation::ReadYourWriteStrict),
    )
    .map_err(|error| error.to_string())?;
    for _ in 0..20 {
        let candidates = query_dense(transport, permit, &query)
            .await
            .map_err(|error| error.to_string())?;
        if !candidates.is_empty() {
            if candidates.len() == 1 && candidates[0].point_id == PointId::Uuid(point.as_uuid()) {
                return Ok(candidates);
            }
            return Err(format!(
                "embedding-version Qdrant filter returned {candidates:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err("real Qdrant query returned no current-version candidate".to_owned())
}

async fn assert_semantic_and_ryw_materialization(
    handle: &Handle,
    consistency_token: &str,
    overlay_evidence_id: Uuid,
    candidates: &[DenseCandidate],
) -> Result<(), String> {
    let bodies = materialize_private_read_serving(
        &handle.runtime,
        Some(consistency_token),
        &handle.authorization,
        &handle.family,
        "private-v1",
        "embed-v1",
        candidates,
    )
    .await
    .map_err(|error| error.to_string())?;
    match bodies.bodies.items.as_slice() {
        [
            MaterializedItem::Memory { memory_id, .. },
            MaterializedItem::TemporaryEvidence {
                evidence_id,
                stream_seq: 1,
                processing_state: ProcessingState::Issued,
                linked_memory_ids,
                ..
            },
        ] if *memory_id == handle.memory_id
            && *evidence_id == overlay_evidence_id
            && linked_memory_ids.is_empty() =>
        {
            Ok(())
        }
        items => Err(format!(
            "semantic Memory and RYW Evidence were not materialized together: {items:?}"
        )),
    }
}

async fn real_qdrant_to_pg_roundtrip(
    handle: &Handle,
    point: ProjectionPointId,
    consistency_token: &str,
    overlay_evidence_id: Uuid,
) -> Result<(), String> {
    let cell = CellId(Uuid::now_v7());
    let registry = qdrant_registry(cell, CallerId("private-registry-e2e".to_owned()));
    let transport = HttpIntraCellTransport::new(
        registry.clone(),
        Duration::from_secs(10),
        humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES,
    )
    .map_err(|error| format!("{error:?}"))?;
    let permit = authorize_cell_access(
        &registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(60),
    )
    .map_err(|error| format!("{error:?}"))?;
    let collection = format!("private_registry_e2e_{}", Uuid::now_v7().simple());
    create_qdrant_collection(&transport, &permit, &collection).await?;
    let body_result = async {
        upsert_private_qdrant_points(&transport, &permit, &collection, handle, point).await?;
        let candidates =
            query_private_qdrant_points(&transport, &permit, &collection, handle, point).await?;
        assert_semantic_and_ryw_materialization(
            handle,
            consistency_token,
            overlay_evidence_id,
            &candidates,
        )
        .await
    }
    .await;
    let cleanup_result = delete_qdrant_collection(&transport, &permit, &collection).await;
    body_result.and(cleanup_result)
}

async fn assert_registration_and_resolution(
    handle: &Handle,
    point: ProjectionPointId,
    first_registration: &PrivateMemoryPointRegistration,
) {
    assert_eq!(
        register_private_memory_point(&handle.worker, &handle.authorization, first_registration,)
            .await
            .unwrap(),
        RegistrationOutcome::Inserted
    );
    assert_eq!(
        register_private_memory_point(&handle.worker, &handle.authorization, first_registration,)
            .await
            .unwrap(),
        RegistrationOutcome::AlreadyRegistered
    );
    let resolved = resolve_private_memory_points(
        &handle.runtime,
        &handle.authorization,
        &handle.family,
        "private-v1",
        "embed-v1",
        &[ProjectionPointId::new(Uuid::new_v4()), point],
    )
    .await
    .unwrap();
    assert_eq!(
        resolved
            .iter()
            .map(|candidate| candidate.memory_id)
            .collect::<Vec<_>>(),
        vec![MemoryId(handle.memory_id)]
    );
    assert!(
        resolve_private_memory_points(
            &handle.runtime,
            &handle.authorization,
            &handle.family,
            "private-v1",
            "embed-v2",
            &[point],
        )
        .await
        .unwrap()
        .is_empty(),
        "a vector from another embedding space must not resolve"
    );
    let bodies = materialize_private_read_serving(
        &handle.runtime,
        None,
        &handle.authorization,
        &handle.family,
        "private-v1",
        "embed-v1",
        &[DenseCandidate {
            point_id: PointId::Uuid(point.as_uuid()),
            score: 0.9,
        }],
    )
    .await
    .unwrap();
    assert!(matches!(
        bodies.bodies.items.as_slice(),
        [MaterializedItem::Memory { memory_id, .. }] if *memory_id == handle.memory_id
    ));
}

async fn assert_scope_and_point_guards(handle: &Handle, point: ProjectionPointId) {
    assert!(matches!(
        materialize_private_read_serving(
            &handle.runtime,
            None,
            &handle.authorization,
            &handle.family,
            "private-v1",
            "embed-v1",
            &[DenseCandidate {
                point_id: PointId::Num(7),
                score: 0.9,
            }],
        )
        .await,
        Err(RetrieveError::UnsupportedPrivateProjectionPointId)
    ));
    let unauthorized_workspace_family = StreamFamily::new(
        TenantId(handle.tenant_id),
        "workspace",
        Uuid::new_v4(),
        "knowledge",
        "dense",
    );
    assert!(matches!(
        resolve_private_memory_points(
            &handle.runtime,
            &handle.authorization,
            &unauthorized_workspace_family,
            "private-v1",
            "embed-v1",
            &[point],
        )
        .await,
        Err(PrivateProjectionRegistryError::CrossWorkspace)
    ));
    assert!(
        resolve_private_memory_points(
            &handle.runtime,
            &handle.authorization,
            &handle.family,
            "private-v2",
            "embed-v1",
            &[point],
        )
        .await
        .unwrap()
        .is_empty()
    );
}

async fn assert_cross_tenant_collision_and_retirement(
    handle: &Handle,
    point: ProjectionPointId,
    first_registration: &PrivateMemoryPointRegistration,
    other_authorization: &AuthorizationScope,
    other_registration: &PrivateMemoryPointRegistration,
) {
    assert_eq!(
        register_private_memory_point(&handle.worker, other_authorization, other_registration,)
            .await
            .unwrap(),
        RegistrationOutcome::Inserted
    );
    assert!(
        resolve_private_memory_points(
            &handle.runtime,
            &handle.authorization,
            &handle.family,
            "private-v1",
            "embed-v1",
            &[other_registration.point_id],
        )
        .await
        .unwrap()
        .is_empty()
    );
    let mut collision = first_registration.clone();
    collision.embedding_version = "embed-v2".to_owned();
    assert!(matches!(
        register_private_memory_point(&handle.worker, &handle.authorization, &collision).await,
        Err(PrivateProjectionRegistryError::PointIdCollision)
    ));
    assert!(
        retire_private_memory_point(
            &handle.worker,
            &handle.authorization,
            &handle.family,
            "private-v1",
            "embed-v1",
            point,
        )
        .await
        .unwrap()
    );
    assert!(
        resolve_private_memory_points(
            &handle.runtime,
            &handle.authorization,
            &handle.family,
            "private-v1",
            "embed-v1",
            &[point],
        )
        .await
        .unwrap()
        .is_empty()
    );
    // ADR-0049: an identical registration after retirement (a `memory.restore`, which keeps
    // the source's identity) revives the binding instead of answering `AlreadyRegistered`
    // for a row nobody could resolve.
    assert_eq!(
        register_private_memory_point(&handle.worker, &handle.authorization, first_registration,)
            .await
            .unwrap(),
        RegistrationOutcome::Revived
    );
    assert_eq!(
        resolve_private_memory_points(
            &handle.runtime,
            &handle.authorization,
            &handle.family,
            "private-v1",
            "embed-v1",
            &[point],
        )
        .await
        .unwrap()
        .len(),
        1,
        "revived = resolvable again"
    );
    assert_eq!(
        register_private_memory_point(&handle.worker, &handle.authorization, first_registration,)
            .await
            .unwrap(),
        RegistrationOutcome::AlreadyRegistered,
        "a second identical registration of the live binding is the plain no-op"
    );
}

fn assert_hash_invalidation(handle: &mut Handle) {
    let hash_point = ProjectionPointId::new(Uuid::new_v4());
    let mut hash_registration = registration(handle, hash_point);
    hash_registration.projection_version = "private-hash-v1".to_owned();
    handle.rt.block_on(async {
        assert_eq!(
            register_private_memory_point(
                &handle.worker,
                &handle.authorization,
                &hash_registration,
            )
            .await
            .unwrap(),
            RegistrationOutcome::Inserted
        );
    });
    handle
        .admin
        .execute(
            "UPDATE private.memory_records SET content=$1, updated_at='2024-01-02T00:00:00Z' \
             WHERE tenant_id=$2 AND memory_id=$3",
            &[
                &serde_json::json!({"body": "mutated"}),
                &handle.tenant_id,
                &handle.memory_id,
            ],
        )
        .unwrap();
    handle.rt.block_on(async {
        assert!(
            resolve_private_memory_points(
                &handle.runtime,
                &handle.authorization,
                &handle.family,
                "private-hash-v1",
                "embed-v1",
                &[hash_point],
            )
            .await
            .unwrap()
            .is_empty()
        );
    });
}

fn assert_gateway_write_rejected(handle: &Handle) {
    // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
    let mut gateway = Client::connect(&dsn_as_role(&handle.dsn, "role_gateway"), NoTls).unwrap();
    let direct_write = gateway.batch_execute(&format!(
        "BEGIN; SET LOCAL humaux.tenant_id = '{}'; SET LOCAL humaux.user_id = '{}'; \
         INSERT INTO projection.private_memory_points \
         (point_id,tenant_id,scope_kind,scope_id,domain,projection_kind,projection_version,embedding_version,memory_id,source_updated_at,body_sha256) \
         VALUES('{}','{}','tenant','{}','knowledge','dense','private-v1','embed-v1','{}','2024-01-01T00:00:00Z',decode(repeat('00',32),'hex')); ROLLBACK;",
        handle.tenant_id,
        handle.user_id,
        Uuid::new_v4(),
        handle.tenant_id,
        handle.tenant_id,
        handle.memory_id
    ));
    assert!(
        direct_write.is_err(),
        "gateway must not write point bindings"
    );
}

#[test]
#[ignore = "lane(a:qdrant) requires an isolated PostgreSQL fixture migrated through 0119 and disposable Qdrant"]
fn private_point_registry_fails_closed_for_identity_and_permissions() {
    run_db_fixture::<RegistryFixture, _>("private_point_registry", |mut handle| {
        let point = ProjectionPointId::new(Uuid::new_v4());
        let first_registration = registration(&handle, point);
        let (overlay_evidence_id, consistency_token) = seed_ryw_overlay(&mut handle, 1);
        handle.rt.block_on(async {
            assert_registration_and_resolution(&handle, point, &first_registration).await;
            real_qdrant_to_pg_roundtrip(&handle, point, &consistency_token, overlay_evidence_id)
                .await
                .expect("real Qdrant candidate and RYW overlay must share one PG snapshot");
            assert_scope_and_point_guards(&handle, point).await;
        });

        let other_tenant = Uuid::new_v4();
        let other_user = Uuid::new_v4();
        let (other_memory, other_body_sha256) = seed_memory(
            &mut handle.admin,
            other_tenant,
            other_user,
            &serde_json::json!({"body": "other tenant source"}),
        )
        .unwrap();
        handle.extra_tenants.push((other_tenant, other_user));
        let other_authorization = AuthorizationScope::new(
            TenantId(other_tenant),
            PrincipalId(other_user),
            Some(UserId(other_user)),
            BoundedSet::<WorkspaceId>::new([]).unwrap(),
        );
        let other_registration = PrivateMemoryPointRegistration {
            point_id: ProjectionPointId::new(Uuid::new_v4()),
            family: StreamFamily::new(
                TenantId(other_tenant),
                "tenant",
                other_tenant,
                "knowledge",
                "dense",
            ),
            projection_version: "private-v1".to_owned(),
            embedding_version: "embed-v1".to_owned(),
            memory_id: MemoryId(other_memory),
            source_updated_at: fixed_time(0),
            body_sha256: other_body_sha256,
        };
        handle
            .rt
            .block_on(assert_cross_tenant_collision_and_retirement(
                &handle,
                point,
                &first_registration,
                &other_authorization,
                &other_registration,
            ));
        assert_hash_invalidation(&mut handle);
        assert_gateway_write_rejected(&handle);
    });
}
