//! `projection_worker::run_once` integration test (§17.4/§15.7) against a real Postgres and a
//! real Qdrant. Same convention as `outbox_batch_remember.rs`/`stream_repo.rs`: throwaway
//! `control.tenants` row + throwaway Qdrant collection, cleaned up on `Drop`.
//!
//! Three-state skip (§79.2): no DSN, unreachable Postgres/Qdrant, or a missing local gitleaks
//! binary all print a visible SKIP and return — see [`discover_gitleaks`]'s doc for why the
//! last one is resolved dynamically rather than via fixed env vars.

use std::collections::{BTreeMap, BTreeSet};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use humaux_adapters::postgres::{RetrievalWorkerDbPool, RuntimeDbPool};
use humaux_adapters::projection_worker::{CardEmbedder, ProjectionWorkerDeps, run_once};
use humaux_adapters::qdrant::{
    DenseCandidate, DenseQuery, DenseQueryVersions, Distance, PlacementClass, PointId,
    PromotionState, QdrantOperation, RetrievalFamily, ShardingMethod, TenantPlacementRow,
    create_collection_body, ha_profile_for, query_dense, tenant_index_body,
};
use humaux_adapters::remember::{self, RememberCommand};
use humaux_domain::error::ErrorCode;
use humaux_domain::evidence::{EvidenceOriginClass, payload_sha256};
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId};
use humaux_domain::ids::{TenantId, UserId};
use humaux_infra_cell::{
    CallerId, CellAccessPermit, CellId, HttpIntraCellTransport, IntraCellError,
    IntraCellHttpTransport, IntraCellMethod, IntraCellRequest, IntraCellResource,
    IntraCellResourceRegistry, IntraCellResponse, ResourceEntry, authorize_cell_access,
};
use humaux_local_secret_scan::{LocalSecretScanner, LocalSecretScannerConfig, SealedRetrievalCard};
use humaux_projection::serving::StreamFamily;
use humaux_retrieval_provider::adapters::TestDoubleProvider;
use humaux_retrieval_provider::contract::{
    CalibrationProfileId, EmbeddingModelDescriptor, EmbeddingProvider, ModelId,
    RerankModelDescriptor, RerankScoreSemantics,
};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sha2::{Digest, Sha256};
use sqlx::types::Uuid;

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

/// Locates a real gitleaks binary and computes the exact `(version, sha256)` pair
/// [`LocalSecretScanner::new`] will independently re-derive and check — mirrors that
/// function's own `run_version`/`sha256_hex` (a copy is unavoidable: those are private to
/// `humaux-local-secret-scan`). Checks `HUMAUX_TEST_GITLEAKS_BIN` first (the convention
/// `bins/gateway/tests/mcp_gateway.rs` already uses), then a couple of locally-staged fallback
/// paths, so this test exercises the real scan path in an environment that has gitleaks
/// staged without requiring three more env vars to be exported by hand.
fn discover_gitleaks() -> Option<(std::path::PathBuf, String, String)> {
    let candidates: Vec<std::path::PathBuf> =
        if let Ok(bin) = std::env::var("HUMAUX_TEST_GITLEAKS_BIN") {
            vec![bin.into()]
        } else {
            vec![
                "/private/tmp/gitleaks-8.30.1/gitleaks".into(),
                "/private/tmp/gitleaks-linux/gitleaks".into(),
            ]
        };
    for path in candidates {
        if !path.is_absolute() || !path.is_file() {
            continue;
        }
        let bytes = std::fs::read(&path).ok()?;
        let sha256 = Sha256::digest(&bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let output = Command::new(&path)
            .arg("version")
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .ok()?;
        if !output.status.success() {
            continue;
        }
        let version = String::from_utf8(output.stdout).ok()?.trim().to_owned();
        if version.is_empty() {
            continue;
        }
        return Some((path, version, sha256));
    }
    None
}

fn qdrant_port() -> u16 {
    std::env::var("HUMAUX_TEST_QDRANT_URL")
        .ok()
        .and_then(|url| url.rsplit(':').next().map(str::to_owned))
        .and_then(|port| port.parse().ok())
        .unwrap_or(6333)
}

struct Handle {
    rt: tokio::runtime::Runtime,
    admin: Client,
    gateway: RuntimeDbPool,
    transport: Arc<HttpIntraCellTransport>,
    registry: IntraCellResourceRegistry,
    collection: String,
    tenant_id: Uuid,
    reasoning_domain_id: Uuid,
    scanner: Arc<LocalSecretScanner>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM projection.private_memory_points WHERE tenant_id = '{0}'; \
             DELETE FROM ops.outbox WHERE tenant_id = '{0}'; \
             DELETE FROM projection.stream_log WHERE tenant_id = '{0}'; \
             DELETE FROM projection.stream_checkpoints WHERE tenant_id = '{0}'; \
             DELETE FROM private.memory_evidence USING private.memory_records m \
               WHERE memory_evidence.memory_id = m.memory_id AND m.tenant_id = '{0}'; \
             DELETE FROM private.memory_records WHERE tenant_id = '{0}'; \
             DELETE FROM private.events USING private.evidence_objects eo \
               WHERE events.event_id = eo.evidence_id AND eo.tenant_id = '{0}'; \
             DELETE FROM private.evidence_objects WHERE tenant_id = '{0}'; \
             DELETE FROM control.private_reasoning_domains WHERE tenant_id = '{0}'; \
             DELETE FROM control.workspaces WHERE tenant_id = '{0}'; \
             DELETE FROM control.tenants WHERE tenant_id = '{0}';",
            self.tenant_id
        ));
        if let Ok(runtime) = tokio::runtime::Runtime::new() {
            let transport = self.transport.clone();
            let registry = self.registry.clone();
            let collection = self.collection.clone();
            runtime.block_on(async move {
                if let Ok(permit) = authorize_cell_access(
                    &registry,
                    IntraCellResource::QDRANT_REST,
                    Duration::from_secs(30),
                ) {
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
            });
        }
    }
}

/// Builds a loopback Qdrant `IntraCellResource` registry/transport and creates one throwaway
/// collection (§17.1 tenant index included) — split out of [`Fixture::isolate`] purely to stay
/// under this repo's line-count lint.
async fn setup_qdrant_collection() -> Result<
    (
        IntraCellResourceRegistry,
        Arc<HttpIntraCellTransport>,
        String,
    ),
    DbFixtureSkipReason,
> {
    let cell_id = CellId(Uuid::new_v4());
    let caller = CallerId("projection_worker.rs test".to_owned());
    let mut entries = BTreeMap::new();
    entries.insert(
        IntraCellResource::QDRANT_REST,
        ResourceEntry::new(
            "127.0.0.1",
            qdrant_port(),
            cell_id,
            vec!["127.0.0.1/32".parse().expect("loopback CIDR")],
            BTreeSet::from([caller.clone()]),
            false,
        )
        .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(format!("{e:?}")))?,
    );
    let registry = IntraCellResourceRegistry::new(entries, cell_id, caller);
    let transport = Arc::new(
        HttpIntraCellTransport::new(
            registry.clone(),
            Duration::from_secs(10),
            humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES,
        )
        .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(format!("{e:?}")))?,
    );
    let collection = format!("test_private_memory_v1_{}", Uuid::new_v4().simple());
    let permit = authorize_cell_access(
        &registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(30),
    )
    .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(format!("{e:?}")))?;
    for (path, body) in [
        (
            format!("/collections/{collection}"),
            create_collection_body(4, Distance::Cosine, 1, 1, 1, ShardingMethod::Auto),
        ),
        (
            format!("/collections/{collection}/index"),
            tenant_index_body(),
        ),
    ] {
        transport
            .execute(
                &permit,
                IntraCellRequest {
                    method: IntraCellMethod::Put,
                    path,
                    json_body: Some(body),
                    headers: Vec::new(),
                },
            )
            .await
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(format!("{e:?}")))?;
    }
    Ok((registry, transport, collection))
}

struct Fixture;

impl DbIntegrationFixture for Fixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        let migrated: bool = admin
            .query_one(
                "SELECT to_regclass('private.ingest_tickets') IS NOT NULL \
                 AND to_regclass('projection.private_memory_points') IS NOT NULL",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !migrated {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "private.ingest_tickets / projection.private_memory_points missing — run \
                 `cargo xtask migrate` against HUMAUX_TEST_PG_DSN first"
                    .to_string(),
            ));
        }

        let Some((gitleaks_bin, gitleaks_version, gitleaks_sha256)) = discover_gitleaks() else {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "no local gitleaks binary found (HUMAUX_TEST_GITLEAKS_BIN unset and no staged \
                 fallback present)"
                    .to_string(),
            ));
        };
        let scanner = LocalSecretScanner::new(LocalSecretScannerConfig {
            executable: gitleaks_bin,
            expected_version: gitleaks_version,
            expected_executable_sha256: gitleaks_sha256,
            timeout: Duration::from_secs(5),
            max_payload_bytes: 64 * 1024,
            finding_exit_code: 1,
        })
        .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(format!("{e}")))?;

        let tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"projection_worker.rs throwaway tenant"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        let reasoning_domain_id: Uuid = admin
            .query_one(
                "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
                 VALUES ($1, 'throwaway') RETURNING reasoning_domain_id",
                &[&tenant_id],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let gateway = rt
            .block_on(RuntimeDbPool::connect(&dsn_as_role(&dsn, "role_gateway")))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        let (registry, transport, collection) = rt.block_on(setup_qdrant_collection())?;

        Ok(Handle {
            rt,
            admin,
            gateway,
            transport,
            registry,
            collection,
            tenant_id,
            reasoning_domain_id,
            scanner: Arc::new(scanner),
        })
    }
}

/// Local `CardEmbedder` wrapper around [`TestDoubleProvider`] — see
/// `crates/adapters/src/projection_worker.rs`'s `CardEmbedder` doc for why this indirection
/// (not `impl CardEmbedder for TestDoubleProvider` directly) is required: neither type is
/// local to `humaux-adapters`, so only a crate that depends on both (this test crate, via a
/// dev-dependency, exactly like `bins/retrieval-worker`) can bridge them.
struct TestEmbedder(Arc<TestDoubleProvider>);

#[async_trait]
impl CardEmbedder for TestEmbedder {
    async fn embed_cards(
        &self,
        tenant_id: TenantId,
        dimension: u32,
        cards: &[SealedRetrievalCard],
    ) -> Result<Vec<Vec<f32>>, ErrorCode> {
        Ok(self
            .0
            .embed_cards(tenant_id, dimension, cards)
            .await?
            .vectors)
    }
}

fn embedding_model() -> EmbeddingModelDescriptor {
    EmbeddingModelDescriptor {
        model_id: ModelId("projection-worker-test-embedding".to_owned()),
        model_revision: "embed-v1".to_owned(),
        dimension_options: vec![4],
        max_input_tokens: 4_096,
        batch_supported: true,
        dense_supported: true,
        sparse_supported: false,
    }
}

fn unused_rerank_model() -> RerankModelDescriptor {
    RerankModelDescriptor {
        model_id: ModelId("projection-worker-unused-reranker".to_owned()),
        model_revision: "unused-v1".to_owned(),
        max_documents: 5,
        max_input_tokens: 4_096,
        score_semantics: RerankScoreSemantics::RawLogit,
        calibration_profile: CalibrationProfileId("unused-v1".to_owned()),
    }
}

/// Writes one Evidence+Event (via `remember::remember`, `domain = "private_memory"` — the
/// same convention `outbox_batch_remember.rs`'s own fixture already establishes for this
/// domain) plus a TENANT_SHARED `private.memory_records` row bound to it through
/// `private.memory_evidence`, and returns the `stream_log` row's `stream_seq`.
///
/// `content` carries `title`/`key_claim`/`evidence_excerpt` directly so [`build_card`] always
/// produces a `Complete` card — this fixture is about `run_once`'s own contract, not §18.4's
/// partial-card substitution.
/// A real `control.users` row — `private.memory_records.visibility_user_id` is FK-constrained
/// to it, so `USER_PRIVATE` fixtures can't use a bare `Uuid::new_v4()`.
fn seed_user(handle: &mut Handle) -> Uuid {
    handle
        .admin
        .query_one(
            "INSERT INTO control.users DEFAULT VALUES RETURNING user_id",
            &[],
        )
        .expect("seed throwaway user")
        .get(0)
}

/// A real `control.workspaces` row (this fixture's tenant) — `private.memory_records`'s
/// `visibility_workspace_id` is FK-constrained to it, so `WORKSPACE_SHARED` fixtures can't use a
/// A throwaway `control.users` row with an ACTIVE membership in this fixture's tenant — the
/// acting member a `WORKSPACE_SHARED` evidence write needs to satisfy `evidence_objects`'
/// WITH CHECK (membership EXISTS for `humaux.user_id`).
fn seed_member(handle: &mut Handle) -> Uuid {
    let user_id = seed_user(handle);
    handle
        .admin
        .execute(
            "INSERT INTO control.memberships (tenant_id, user_id, role, state) \
             VALUES ($1, $2, 'MEMBER', 'ACTIVE')",
            &[&handle.tenant_id, &user_id],
        )
        .expect("seed ACTIVE membership");
    user_id
}

/// bare `Uuid::new_v4()`.
fn seed_workspace(handle: &mut Handle) -> Uuid {
    handle
        .admin
        .query_one(
            "INSERT INTO control.workspaces (tenant_id, name) VALUES ($1, 'throwaway') \
             RETURNING workspace_id",
            &[&handle.tenant_id],
        )
        .expect("seed throwaway workspace")
        .get(0)
}

fn seed_memory(handle: &mut Handle, scope_id: Uuid, content: &str) -> i64 {
    seed_memory_with_visibility(handle, scope_id, content, "TENANT_SHARED", None, None).0
}

/// Same as [`seed_memory`] but lets the caller choose the `private.memory_records` row's own
/// `visibility_class`/`visibility_user_id`/`visibility_workspace_id`; the Evidence's own
/// visibility (the `RememberCommand` fields) stays `TENANT_SHARED` here (see
/// [`seed_memory_with_visibility_and_evidence`] for the non-shared case), mirroring
/// `remember::token_workspace_id`'s "stream-routing binding, not Evidence visibility"
/// distinction the module doc's `## Scope` section already draws: the memory row is what §6.1.2
/// gates, not the Evidence that backs it. Returns `(stream_seq, memory_id)` — the latter lets a
/// caller resolve the deterministic Qdrant point id from `projection.private_memory_points`.
fn seed_memory_with_visibility(
    handle: &mut Handle,
    scope_id: Uuid,
    content: &str,
    visibility_class: &str,
    visibility_user_id: Option<Uuid>,
    visibility_workspace_id: Option<Uuid>,
) -> (i64, Uuid) {
    seed_memory_with_visibility_and_evidence(
        handle,
        scope_id,
        content,
        visibility_class,
        visibility_user_id,
        visibility_workspace_id,
        "TENANT_SHARED",
        None,
        None,
    )
}

/// Like [`seed_memory_with_visibility`] but the backing Evidence row gets its own
/// `visibility_*` triple too. `resolve_memory` INNER JOINs `private.evidence_objects`, so a
/// memory whose PRIMARY evidence is `USER_PRIVATE`/`WORKSPACE_SHARED` is only resolvable
/// because migration 0140 widened *both* policies for `role_retrieval_worker` — the earlier
/// fixture always wrote `TENANT_SHARED` evidence and therefore never exercised that half of
/// the join (the production shape `remember.rs` writes verbatim from the request).
#[allow(clippy::too_many_arguments)]
fn seed_memory_with_visibility_and_evidence(
    handle: &mut Handle,
    scope_id: Uuid,
    content: &str,
    visibility_class: &str,
    visibility_user_id: Option<Uuid>,
    visibility_workspace_id: Option<Uuid>,
    evidence_visibility_class: &str,
    evidence_visibility_user_id: Option<Uuid>,
    evidence_visibility_workspace_id: Option<Uuid>,
) -> (i64, Uuid) {
    // §6.1.2 WITH CHECK: writing USER_PRIVATE/WORKSPACE_SHARED evidence needs the acting
    // member in `humaux.user_id` (remember.rs:450 sets it from `authorization_user_id`) — and
    // for WORKSPACE_SHARED that member must hold an ACTIVE membership. The production writer
    // is always a member, so the fixture mirrors that instead of bypassing RLS.
    let acting_user = evidence_visibility_user_id
        .or_else(|| evidence_visibility_workspace_id.map(|_| seed_member(handle)));
    let cmd = RememberCommand {
        tenant_id: handle.tenant_id,
        authorization_user_id: acting_user,
        scope_kind: "workspace".to_owned(),
        scope_id,
        domain: "private_memory".to_owned(),
        projection_kind: "PRIVATE_MEMORY".to_owned(),
        projection_version: "v1".to_owned(),
        consistency_token_expires_at: time::OffsetDateTime::now_utc()
            + std::time::Duration::from_secs(3600),
        batch_id: None,
        payload_sha256: payload_sha256(content.as_bytes()),
        data_class: "INTERNAL".to_owned(),
        origin_class: EvidenceOriginClass::DirectUserInput,
        origin_principal_id: None,
        origin_connector_id: None,
        visibility_class: evidence_visibility_class.to_owned(),
        visibility_user_id: evidence_visibility_user_id,
        visibility_workspace_id: evidence_visibility_workspace_id,
        reasoning_domain_id: handle.reasoning_domain_id,
        occurred_at: None,
        event_kind: "MANUAL_NOTE".to_owned(),
        event_payload: serde_json::json!({ "content": content }),
    };
    let accepted = handle
        .rt
        .block_on(remember::remember(&handle.gateway, cmd))
        .expect("remember writes evidence + private_memory stream_log ticket");

    let content_json = serde_json::json!({
        "title": format!("title: {content}"),
        "key_claim": format!("key claim: {content}"),
        "evidence_excerpt": content,
    });
    let mut txn = handle.admin.transaction().expect("admin transaction");
    let memory_id: Uuid = txn
        .query_one(
            "INSERT INTO private.memory_records \
               (tenant_id, memory_type, content, visibility_class, visibility_user_id, \
                visibility_workspace_id, authority_class, confidence, status, asserted_at) \
             VALUES ($1,'NOTE',$2,$3,$4,$5,'PrivateKnowledge',0.9,'active',now()) \
             RETURNING memory_id",
            &[
                &handle.tenant_id,
                &content_json,
                &visibility_class,
                &visibility_user_id,
                &visibility_workspace_id,
            ],
        )
        .expect("insert memory_records row")
        .get(0);
    txn.execute(
        "INSERT INTO private.memory_evidence (memory_id, evidence_id, role, ordinal) \
         VALUES ($1, $2, 'PRIMARY', 0)",
        &[&memory_id, &accepted.evidence_id],
    )
    .expect("link memory to its evidence");
    txn.commit().expect("commit memory + evidence link");

    let stream_seq: i64 = handle
        .admin
        .query_one(
            "SELECT sl.stream_seq FROM projection.stream_log sl \
             JOIN ops.outbox ob ON ob.tenant_id = sl.tenant_id AND ob.commit_seq = sl.commit_seq \
             WHERE ob.evidence_id = $1",
            &[&accepted.evidence_id],
        )
        .expect("resolve the stream_log row remember() issued")
        .get(0);
    (stream_seq, memory_id)
}

/// Resolves the deterministic Qdrant point id `finish_row` registered for a given `memory_id`,
/// once `run_once` has processed it.
fn point_id_for_memory(handle: &mut Handle, memory_id: Uuid) -> Uuid {
    handle
        .admin
        .query_one(
            "SELECT point_id FROM projection.private_memory_points WHERE memory_id = $1",
            &[&memory_id],
        )
        .expect("registered point for this memory")
        .get(0)
}

/// Raw `POST /collections/{name}/points/scroll` with `with_payload: true` — the only way to
/// read a point's payload back out; [`humaux_adapters::qdrant::scroll_by_ids`] deliberately
/// hardcodes `with_payload: false` (it only proves presence, §17's read-your-write check), so
/// this test goes around the adapter layer to inspect the indexed payload directly, the same
/// way [`setup_qdrant_collection`] talks to the transport directly for collection setup.
fn scroll_payloads(
    handle: &Handle,
    permit: &CellAccessPermit,
    point_ids: &[Uuid],
) -> serde_json::Value {
    let body = serde_json::json!({
        "filter": { "must": [{ "has_id": point_ids.iter().map(ToString::to_string).collect::<Vec<_>>() }] },
        "limit": point_ids.len().max(1),
        "with_payload": true,
        "with_vector": false,
    });
    handle
        .rt
        .block_on(handle.transport.execute(
            permit,
            IntraCellRequest {
                method: IntraCellMethod::Post,
                path: format!("/collections/{}/points/scroll", handle.collection),
                json_body: Some(body),
                headers: Vec::new(),
            },
        ))
        .expect("scroll with payload succeeds")
        .json_body
        .expect("scroll response has a JSON body")
}

fn stream_log_state(
    handle: &mut Handle,
    key: &(Uuid, &str, Uuid, &str, &str, &str),
    stream_seq: i64,
) -> String {
    handle
        .admin
        .query_one(
            "SELECT state FROM projection.stream_log \
             WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND domain=$4 \
               AND projection_kind=$5 AND projection_version=$6 AND stream_seq=$7",
            &[&key.0, &key.1, &key.2, &key.3, &key.4, &key.5, &stream_seq],
        )
        .expect("stream_log row must exist")
        .get(0)
}

fn stream_log_error_class(
    handle: &mut Handle,
    key: &(Uuid, &str, Uuid, &str, &str, &str),
    stream_seq: i64,
) -> Option<String> {
    handle
        .admin
        .query_one(
            "SELECT error_class FROM projection.stream_log \
             WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND domain=$4 \
               AND projection_kind=$5 AND projection_version=$6 AND stream_seq=$7",
            &[&key.0, &key.1, &key.2, &key.3, &key.4, &key.5, &stream_seq],
        )
        .expect("stream_log row must exist")
        .get(0)
}

fn projection_highwater(handle: &mut Handle, key: &(Uuid, &str, Uuid, &str, &str, &str)) -> i64 {
    handle
        .admin
        .query_one(
            "SELECT projection_highwater FROM projection.stream_checkpoints \
             WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND domain=$4 \
               AND projection_kind=$5 AND projection_version=$6",
            &[&key.0, &key.1, &key.2, &key.3, &key.4, &key.5],
        )
        .expect("checkpoint row must exist")
        .get(0)
}

async fn deps_for(
    handle: &Handle,
    scope_id: Uuid,
    provider: Arc<TestDoubleProvider>,
) -> ProjectionWorkerDeps {
    let retrieval_dsn = std::env::var("HUMAUX_RETRIEVAL_WORKER_PG_DSN")
        .expect("HUMAUX_RETRIEVAL_WORKER_PG_DSN must be set for this test");
    let pool = RetrievalWorkerDbPool::connect(&retrieval_dsn)
        .await
        .expect("role_retrieval_worker connects");
    let permit = authorize_cell_access(
        &handle.registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(30),
    )
    .expect("retrieval worker Qdrant permit");
    ProjectionWorkerDeps {
        pool,
        embedder: Arc::new(TestEmbedder(provider)),
        scanner: handle.scanner.clone(),
        transport: handle.transport.clone(),
        permit,
        placement: TenantPlacementRow {
            tenant_id: TenantId(handle.tenant_id),
            projection_family: RetrievalFamily::PrivateMemoryV1,
            collection_name: handle.collection.clone(),
            shard_key: None,
            placement_class: PlacementClass::SharedFallback,
            point_count: 0,
            bytes_estimate: 0,
            promotion_state: PromotionState::Stable,
        },
        family: StreamFamily::new(
            TenantId(handle.tenant_id),
            "workspace",
            scope_id,
            "private_memory",
            "PRIVATE_MEMORY",
        ),
        embedding_version: "embed-v1".to_owned(),
        projection_version: "v1".to_owned(),
        dimension: 4,
    }
}

/// (1): one seeded memory settles `DONE`, lands in Qdrant, registers in
/// `projection.private_memory_points`, and the checkpoint's `projection_highwater` reaches
/// that seq.
#[test]
fn happy_path_indexes_and_advances_checkpoint() {
    run_db_fixture::<Fixture, _>(
        "happy_path_indexes_and_advances_checkpoint",
        |mut handle| {
            let scope_id = Uuid::new_v4();
            let stream_seq = seed_memory(&mut handle, scope_id, "first private memory");
            assert_eq!(stream_seq, 1, "first ticket on a fresh scope is seq 1");

            let provider = Arc::new(TestDoubleProvider::new(
                embedding_model(),
                unused_rerank_model(),
            ));
            let deps = handle.rt.block_on(deps_for(&handle, scope_id, provider));

            let outcome = handle
                .rt
                .block_on(run_once(&deps, 10))
                .expect("run_once succeeds");
            assert_eq!(outcome.done, 1);
            assert_eq!(outcome.failed, 0);
            assert_eq!(outcome.skipped_by_policy, 0);
            assert_eq!(outcome.projection_highwater, 1);

            let key = (
                handle.tenant_id,
                "workspace",
                scope_id,
                "private_memory",
                "PRIVATE_MEMORY",
                "v1",
            );
            assert_eq!(stream_log_state(&mut handle, &key, stream_seq), "DONE");
            assert_eq!(projection_highwater(&mut handle, &key), 1);

            let registered: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM projection.private_memory_points \
                 WHERE tenant_id = $1 AND scope_id = $2",
                    &[&handle.tenant_id, &scope_id],
                )
                .expect("registry query")
                .get(0);
            assert_eq!(registered, 1, "exactly one point registered");
        },
    );
}

/// (2): a second `run_once` against the same key, after the first has already settled every
/// row `DONE`, is a no-op (idempotent — nothing left `ISSUED`, `advance_prefix` reports the
/// same highwater unchanged).
#[test]
fn second_run_once_is_idempotent() {
    run_db_fixture::<Fixture, _>("second_run_once_is_idempotent", |mut handle| {
        let scope_id = Uuid::new_v4();
        seed_memory(&mut handle, scope_id, "idempotency fixture memory");

        let provider = Arc::new(TestDoubleProvider::new(
            embedding_model(),
            unused_rerank_model(),
        ));
        let deps = handle.rt.block_on(deps_for(&handle, scope_id, provider));

        let first = handle
            .rt
            .block_on(run_once(&deps, 10))
            .expect("first run_once succeeds");
        assert_eq!(first.done, 1);
        assert_eq!(first.projection_highwater, 1);

        let second = handle
            .rt
            .block_on(run_once(&deps, 10))
            .expect("second run_once succeeds");
        assert_eq!(second.done, 0);
        assert_eq!(second.failed, 0);
        assert_eq!(second.skipped_by_policy, 0);
        assert_eq!(second.projection_highwater, 1);
    });
}

/// F3 fixture: a [`CardEmbedder`] that always returns a vector one element longer than whatever
/// `dimension` it is asked for, ignoring the request — unlike [`TestDoubleProvider`], which
/// always honors the requested dimension and so can never exercise `resolve_and_embed`'s
/// post-embed length check. Proves that check is load-bearing: without it, this vector would
/// reach `qdrant::upsert` unchecked (task card F3).
struct WrongDimEmbedder;

#[async_trait]
impl CardEmbedder for WrongDimEmbedder {
    async fn embed_cards(
        &self,
        _tenant_id: TenantId,
        dimension: u32,
        cards: &[SealedRetrievalCard],
    ) -> Result<Vec<Vec<f32>>, ErrorCode> {
        Ok(cards
            .iter()
            .map(|_| vec![0.0_f32; dimension as usize + 1])
            .collect())
    }
}

/// F1 fixture: wraps a real [`IntraCellHttpTransport`] and forces every `/points/scroll` call
/// (the wire shape [`humaux_adapters::qdrant::scroll_by_ids`]/`verify_visible_via_transport`
/// issues) to report zero observed points, while forwarding every other call (the upsert PUT
/// included) unchanged to the real transport. The point genuinely lands in the real Qdrant
/// collection; only the worker's own visibility read is stubbed to "not visible yet" — proving
/// `finish_row` actually branches on `verify_visible_via_transport`'s result instead of always
/// settling `Done` after a successful upsert (task card F1).
struct ScrollStubTransport {
    inner: Arc<HttpIntraCellTransport>,
}

#[async_trait]
impl IntraCellHttpTransport for ScrollStubTransport {
    async fn execute(
        &self,
        permit: &CellAccessPermit,
        request: IntraCellRequest,
    ) -> Result<IntraCellResponse, IntraCellError> {
        if request.path.ends_with("/points/scroll") {
            return Ok(IntraCellResponse {
                status: 200,
                json_body: Some(serde_json::json!({ "result": { "points": [] } })),
            });
        }
        self.inner.execute(permit, request).await
    }
}

/// (4): the embedder returns a vector whose length disagrees with `deps.dimension` — must settle
/// `FAILED` with `error_class = 'embedding_dimension_mismatch'` before ever reaching Qdrant (no
/// registry row, checkpoint held at 0). Regression test for F3.
#[test]
fn dimension_mismatch_fails_before_reaching_qdrant() {
    run_db_fixture::<Fixture, _>(
        "dimension_mismatch_fails_before_reaching_qdrant",
        |mut handle| {
            let scope_id = Uuid::new_v4();
            let stream_seq = seed_memory(&mut handle, scope_id, "wrong dimension vector");

            let retrieval_dsn = std::env::var("HUMAUX_RETRIEVAL_WORKER_PG_DSN")
                .expect("HUMAUX_RETRIEVAL_WORKER_PG_DSN must be set for this test");
            let pool = handle
                .rt
                .block_on(RetrievalWorkerDbPool::connect(&retrieval_dsn))
                .expect("role_retrieval_worker connects");
            let permit = authorize_cell_access(
                &handle.registry,
                IntraCellResource::QDRANT_REST,
                Duration::from_secs(30),
            )
            .expect("retrieval worker Qdrant permit");
            let deps = ProjectionWorkerDeps {
                pool,
                embedder: Arc::new(WrongDimEmbedder),
                scanner: handle.scanner.clone(),
                transport: handle.transport.clone(),
                permit,
                placement: TenantPlacementRow {
                    tenant_id: TenantId(handle.tenant_id),
                    projection_family: RetrievalFamily::PrivateMemoryV1,
                    collection_name: handle.collection.clone(),
                    shard_key: None,
                    placement_class: PlacementClass::SharedFallback,
                    point_count: 0,
                    bytes_estimate: 0,
                    promotion_state: PromotionState::Stable,
                },
                family: StreamFamily::new(
                    TenantId(handle.tenant_id),
                    "workspace",
                    scope_id,
                    "private_memory",
                    "PRIVATE_MEMORY",
                ),
                embedding_version: "embed-v1".to_owned(),
                projection_version: "v1".to_owned(),
                dimension: 4,
            };

            let outcome = handle
                .rt
                .block_on(run_once(&deps, 10))
                .expect("run_once succeeds even with a per-row embedding failure");
            assert_eq!(outcome.failed, 1);
            assert_eq!(outcome.done, 0);
            assert_eq!(outcome.projection_highwater, 0);

            let key = (
                handle.tenant_id,
                "workspace",
                scope_id,
                "private_memory",
                "PRIVATE_MEMORY",
                "v1",
            );
            assert_eq!(stream_log_state(&mut handle, &key, stream_seq), "FAILED");
            assert_eq!(
                stream_log_error_class(&mut handle, &key, stream_seq),
                Some("embedding_dimension_mismatch".to_owned())
            );

            let registered: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM projection.private_memory_points \
                 WHERE tenant_id = $1 AND scope_id = $2",
                    &[&handle.tenant_id, &scope_id],
                )
                .expect("registry query")
                .get(0);
            assert_eq!(
                registered, 0,
                "a wrong-dimension vector must never reach qdrant::upsert/registration"
            );
        },
    );
}

/// (5): the visibility probe reports the point as not (yet) observed after a successful upsert —
/// must settle `FAILED` with `error_class = 'visibility_not_confirmed'` and must not advance the
/// checkpoint past it, proving `finish_row` actually awaits/branches on
/// `verify_visible_via_transport`'s result rather than unconditionally marking `Done` once the
/// upsert call itself succeeds. Regression test for F1.
#[test]
fn unconfirmed_visibility_fails_row_and_blocks_checkpoint() {
    run_db_fixture::<Fixture, _>(
        "unconfirmed_visibility_fails_row_and_blocks_checkpoint",
        |mut handle| {
            let scope_id = Uuid::new_v4();
            let stream_seq = seed_memory(&mut handle, scope_id, "never visible memory");

            let provider = Arc::new(TestDoubleProvider::new(
                embedding_model(),
                unused_rerank_model(),
            ));
            let retrieval_dsn = std::env::var("HUMAUX_RETRIEVAL_WORKER_PG_DSN")
                .expect("HUMAUX_RETRIEVAL_WORKER_PG_DSN must be set for this test");
            let pool = handle
                .rt
                .block_on(RetrievalWorkerDbPool::connect(&retrieval_dsn))
                .expect("role_retrieval_worker connects");
            let permit = authorize_cell_access(
                &handle.registry,
                IntraCellResource::QDRANT_REST,
                Duration::from_secs(30),
            )
            .expect("retrieval worker Qdrant permit");
            let stub_transport = Arc::new(ScrollStubTransport {
                inner: handle.transport.clone(),
            });
            let deps = ProjectionWorkerDeps {
                pool,
                embedder: Arc::new(TestEmbedder(provider)),
                scanner: handle.scanner.clone(),
                transport: stub_transport,
                permit,
                placement: TenantPlacementRow {
                    tenant_id: TenantId(handle.tenant_id),
                    projection_family: RetrievalFamily::PrivateMemoryV1,
                    collection_name: handle.collection.clone(),
                    shard_key: None,
                    placement_class: PlacementClass::SharedFallback,
                    point_count: 0,
                    bytes_estimate: 0,
                    promotion_state: PromotionState::Stable,
                },
                family: StreamFamily::new(
                    TenantId(handle.tenant_id),
                    "workspace",
                    scope_id,
                    "private_memory",
                    "PRIVATE_MEMORY",
                ),
                embedding_version: "embed-v1".to_owned(),
                projection_version: "v1".to_owned(),
                dimension: 4,
            };

            let outcome = handle
                .rt
                .block_on(run_once(&deps, 10))
                .expect("run_once succeeds even with an unconfirmed visibility probe");
            assert_eq!(outcome.failed, 1);
            assert_eq!(outcome.done, 0);
            assert_eq!(
                outcome.projection_highwater, 0,
                "an unconfirmed-visibility row must never let the checkpoint cross it"
            );

            let key = (
                handle.tenant_id,
                "workspace",
                scope_id,
                "private_memory",
                "PRIVATE_MEMORY",
                "v1",
            );
            assert_eq!(stream_log_state(&mut handle, &key, stream_seq), "FAILED");
            assert_eq!(
                stream_log_error_class(&mut handle, &key, stream_seq),
                Some("visibility_not_confirmed".to_owned())
            );
        },
    );
}

/// (3): a batch of two rows (seq 1, seq 2) where the embedder is forced to fail on the first
/// call — since `run_once` processes ascending `stream_seq`, that is seq 1. §15.7: seq 1
/// settles `FAILED`, seq 2 may settle `DONE`, but `projection_highwater` must stay at 0 (the
/// prefix before the first gap), never cross the `FAILED` row.
#[test]
fn failed_row_blocks_checkpoint_past_it() {
    run_db_fixture::<Fixture, _>("failed_row_blocks_checkpoint_past_it", |mut handle| {
        let scope_id = Uuid::new_v4();
        let seq1 = seed_memory(&mut handle, scope_id, "row that will fail to embed");
        let seq2 = seed_memory(&mut handle, scope_id, "row after the failure");
        assert_eq!((seq1, seq2), (1, 2));

        let provider = Arc::new(TestDoubleProvider::new(
            embedding_model(),
            unused_rerank_model(),
        ));
        provider.force_next_error(ErrorCode::ProviderTransient);
        let deps = handle.rt.block_on(deps_for(&handle, scope_id, provider));

        let outcome = handle
            .rt
            .block_on(run_once(&deps, 10))
            .expect("run_once succeeds even with a per-row embedding failure");
        assert_eq!(outcome.failed, 1);
        assert_eq!(outcome.done, 1);
        assert_eq!(
            outcome.projection_highwater, 0,
            "§15.7: FAILED seq 1 blocks the prefix"
        );

        let key = (
            handle.tenant_id,
            "workspace",
            scope_id,
            "private_memory",
            "PRIVATE_MEMORY",
            "v1",
        );
        assert_eq!(stream_log_state(&mut handle, &key, seq1), "FAILED");
        assert_eq!(stream_log_state(&mut handle, &key, seq2), "DONE");
        assert_eq!(
            projection_highwater(&mut handle, &key),
            0,
            "checkpoint must not have advanced past the FAILED row"
        );
    });
}

/// (T4): 0140's `role_retrieval_worker` RLS read bypass — before it existed, a
/// `USER_PRIVATE`/`WORKSPACE_SHARED` row's stream_log entry could never resolve under this
/// worker's tenant-only session and settled `FAILED` forever (module doc's old "RLS and
/// visibility" note). A batch mixing a `USER_PRIVATE` row (owned by user A) and a
/// `WORKSPACE_SHARED` row in the same stream both settle `DONE`, both land in Qdrant with their
/// real `visibility_user_id`/`visibility_workspace_id` payload fields populated (real
/// enforcement stays downstream at query time, §6.1.2 — see T5), and the checkpoint reaches the
/// batch's last seq.
#[test]
fn mixed_visibility_batch_indexes_both_and_populates_payload() {
    run_db_fixture::<Fixture, _>(
        "mixed_visibility_batch_indexes_both_and_populates_payload",
        |mut handle| {
            let scope_id = Uuid::new_v4();
            let user_a = seed_user(&mut handle);
            let workspace_w = seed_workspace(&mut handle);
            let (seq1, private_memory_id) = seed_memory_with_visibility(
                &mut handle,
                scope_id,
                "user A's private memory",
                "USER_PRIVATE",
                Some(user_a),
                None,
            );
            let (seq2, shared_memory_id) = seed_memory_with_visibility(
                &mut handle,
                scope_id,
                "workspace shared memory",
                "WORKSPACE_SHARED",
                None,
                Some(workspace_w),
            );
            assert_eq!((seq1, seq2), (1, 2));

            let provider = Arc::new(TestDoubleProvider::new(
                embedding_model(),
                unused_rerank_model(),
            ));
            let deps = handle.rt.block_on(deps_for(&handle, scope_id, provider));

            let outcome = handle
                .rt
                .block_on(run_once(&deps, 10))
                .expect("run_once succeeds");
            let key = (
                handle.tenant_id,
                "workspace",
                scope_id,
                "private_memory",
                "PRIVATE_MEMORY",
                "v1",
            );
            assert_eq!(outcome.done, 2);
            assert_eq!(outcome.failed, 0);
            assert_eq!(outcome.skipped_by_policy, 0);
            assert_eq!(outcome.projection_highwater, 2);

            assert_eq!(stream_log_state(&mut handle, &key, seq1), "DONE");
            assert_eq!(stream_log_state(&mut handle, &key, seq2), "DONE");

            let registered: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM projection.private_memory_points \
                     WHERE tenant_id = $1 AND scope_id = $2",
                    &[&handle.tenant_id, &scope_id],
                )
                .expect("registry query")
                .get(0);
            assert_eq!(
                registered, 2,
                "both rows registered despite mixed visibility"
            );

            let private_point = point_id_for_memory(&mut handle, private_memory_id);
            let shared_point = point_id_for_memory(&mut handle, shared_memory_id);
            let permit = authorize_cell_access(
                &handle.registry,
                IntraCellResource::QDRANT_REST,
                Duration::from_secs(30),
            )
            .expect("admin Qdrant permit");
            let scrolled = scroll_payloads(&handle, &permit, &[private_point, shared_point]);
            assert_mixed_visibility_payloads(
                &scrolled,
                private_point,
                user_a,
                shared_point,
                workspace_w,
            );
        },
    );
}

/// Asserts T4's scroll result contains exactly the `USER_PRIVATE` point (with its
/// `visibility_user_id`) and the `WORKSPACE_SHARED` point (with its `visibility_workspace_id`)
/// — split out of the test body purely to stay under this repo's line-count lint.
fn assert_mixed_visibility_payloads(
    scrolled: &serde_json::Value,
    private_point: Uuid,
    user_a: Uuid,
    shared_point: Uuid,
    workspace_w: Uuid,
) {
    let points = scrolled
        .get("result")
        .and_then(|r| r.get("points"))
        .and_then(|p| p.as_array())
        .expect("scroll result.points");
    assert_eq!(points.len(), 2, "both points present in Qdrant");
    for point in points {
        let payload = point.get("payload").expect("point has a payload");
        let id = point.get("id").and_then(|v| v.as_str()).unwrap_or_default();
        if id == private_point.to_string() {
            assert_eq!(
                payload.get("visibility_class").and_then(|v| v.as_str()),
                Some("USER_PRIVATE")
            );
            assert_eq!(
                payload.get("visibility_user_id").and_then(|v| v.as_str()),
                Some(user_a.to_string()).as_deref()
            );
        } else if id == shared_point.to_string() {
            assert_eq!(
                payload.get("visibility_class").and_then(|v| v.as_str()),
                Some("WORKSPACE_SHARED")
            );
            assert_eq!(
                payload
                    .get("visibility_workspace_id")
                    .and_then(|v| v.as_str()),
                Some(workspace_w.to_string()).as_deref()
            );
        } else {
            panic!("unexpected point id {id} in scroll result");
        }
    }
}

/// Builds a limit-10, `ReadYourWriteStrict` `DenseQuery` scoped to `user_id` (no workspace
/// grants), against `v1`/`embed-v1`, with a fixed finite non-zero probe vector — the caller only
/// ever asserts result-set membership, never ranking/score, so the exact vector value is
/// unimportant as long as it is valid.
fn dense_query_for_user(
    handle: &Handle,
    placement: &TenantPlacementRow,
    user_id: Uuid,
) -> DenseQuery {
    let scope = AuthorizationScope::new(
        TenantId(handle.tenant_id),
        PrincipalId(Uuid::new_v4()),
        Some(UserId(user_id)),
        BoundedSet::new([]).expect("empty workspace set is always within MAX_LEN"),
    );
    DenseQuery::new(
        &scope,
        placement,
        DenseQueryVersions {
            projection: "v1",
            embedding: "embed-v1",
        },
        vec![1.0_f32, 0.0, 0.0, 0.0],
        10,
        Vec::new(),
        ha_profile_for(QdrantOperation::ReadYourWriteStrict),
    )
    .expect("valid dense query")
}

/// (T5): §6.1.2's real visibility boundary — enforced downstream at Qdrant query time via
/// `projection::dense::visibility_disjunction`, not by 0140's RLS read bypass (that bypass only
/// lets the worker *index* the row; it grants no reader anything). Reuses T4's indexed points:
/// a `DenseQuery` built for user B (a different user of the same tenant, no membership in
/// `workspace_w`) returns 0 hits for the `USER_PRIVATE` point owned by user A, while the same
/// query built for user A does return it — proving the RLS widening in 0140 did not leak
/// `USER_PRIVATE` visibility to an unrelated query-time reader.
#[test]
fn cross_user_dense_query_still_enforces_user_private_visibility() {
    run_db_fixture::<Fixture, _>(
        "cross_user_dense_query_still_enforces_user_private_visibility",
        |mut handle| {
            let scope_id = Uuid::new_v4();
            let user_a = seed_user(&mut handle);
            let user_b = seed_user(&mut handle);
            let (_, private_memory_id) = seed_memory_with_visibility(
                &mut handle,
                scope_id,
                "user A's private memory for cross-user check",
                "USER_PRIVATE",
                Some(user_a),
                None,
            );

            let provider = Arc::new(TestDoubleProvider::new(
                embedding_model(),
                unused_rerank_model(),
            ));
            let deps = handle.rt.block_on(deps_for(&handle, scope_id, provider));
            let outcome = handle
                .rt
                .block_on(run_once(&deps, 10))
                .expect("run_once succeeds");
            assert_eq!(outcome.done, 1);

            let private_point = point_id_for_memory(&mut handle, private_memory_id);
            let placement = TenantPlacementRow {
                tenant_id: TenantId(handle.tenant_id),
                projection_family: RetrievalFamily::PrivateMemoryV1,
                collection_name: handle.collection.clone(),
                shard_key: None,
                placement_class: PlacementClass::SharedFallback,
                point_count: 1,
                bytes_estimate: 0,
                promotion_state: PromotionState::Stable,
            };
            let permit = handle
                .rt
                .block_on(async {
                    authorize_cell_access(
                        &handle.registry,
                        IntraCellResource::QDRANT_REST,
                        Duration::from_secs(30),
                    )
                })
                .expect("query permit");
            let query_as_a = dense_query_for_user(&handle, &placement, user_a);
            let query_as_b = dense_query_for_user(&handle, &placement, user_b);

            // Real Qdrant upsert-then-search has read-after-write lag under Weak ordering, so
            // poll like `private_projection_registry.rs`'s own `query_private_qdrant_points`
            // does, rather than asserting on the very first attempt.
            let hits_as_a: Vec<DenseCandidate> = handle.rt.block_on(async {
                for _ in 0..20 {
                    let candidates = query_dense(handle.transport.as_ref(), &permit, &query_as_a)
                        .await
                        .expect("dense query as user A succeeds");
                    if !candidates.is_empty() {
                        return candidates;
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                Vec::new()
            });
            assert_eq!(
                hits_as_a.len(),
                1,
                "user A's own USER_PRIVATE memory is visible to user A"
            );
            assert_eq!(hits_as_a[0].point_id, PointId::Uuid(private_point));

            let hits_as_b = handle
                .rt
                .block_on(query_dense(handle.transport.as_ref(), &permit, &query_as_b))
                .expect("dense query as user B succeeds");
            assert!(
                hits_as_b.is_empty(),
                "0140's RLS read bypass must not leak USER_PRIVATE visibility to user B's \
                 query-time dense read: got {hits_as_b:?}"
            );
        },
    );
}

/// 0140 must widen the evidence half of `resolve_memory`'s INNER JOIN too: a memory whose
/// PRIMARY evidence is `USER_PRIVATE`/`WORKSPACE_SHARED` (the shape `remember.rs` writes when
/// the request says so) has to index and advance the checkpoint, not settle FAILED and freeze
/// the tenant under §15.7. Fault F-evidence: drop the role clause from
/// `evidence_objects_tenant_and_visibility` ⇒ this test goes red (rows FAILED, highwater 0).
#[test]
fn non_tenant_shared_evidence_still_resolves_and_advances_checkpoint() {
    run_db_fixture::<Fixture, _>(
        "non_tenant_shared_evidence_still_resolves_and_advances_checkpoint",
        |mut handle| {
            let scope_id = Uuid::new_v4();
            let user_a = seed_user(&mut handle);
            let workspace_w = seed_workspace(&mut handle);
            let (seq1, _) = seed_memory_with_visibility_and_evidence(
                &mut handle,
                scope_id,
                "private memory backed by private evidence",
                "USER_PRIVATE",
                Some(user_a),
                None,
                "USER_PRIVATE",
                Some(user_a),
                None,
            );
            let (seq2, _) = seed_memory_with_visibility_and_evidence(
                &mut handle,
                scope_id,
                "shared memory backed by workspace-shared evidence",
                "WORKSPACE_SHARED",
                None,
                Some(workspace_w),
                "WORKSPACE_SHARED",
                None,
                Some(workspace_w),
            );
            assert_eq!((seq1, seq2), (1, 2));
            let provider = Arc::new(TestDoubleProvider::new(
                embedding_model(),
                unused_rerank_model(),
            ));
            let deps = handle.rt.block_on(deps_for(&handle, scope_id, provider));
            let outcome = handle
                .rt
                .block_on(run_once(&deps, 10))
                .expect("run_once succeeds");
            let key = (
                handle.tenant_id,
                "workspace",
                scope_id,
                "private_memory",
                "PRIVATE_MEMORY",
                "v1",
            );
            assert_eq!(
                (outcome.done, outcome.failed, outcome.skipped_by_policy),
                (2, 0, 0),
                "non-TENANT_SHARED evidence must resolve under 0140, not settle FAILED"
            );
            assert_eq!(outcome.projection_highwater, 2);
            assert_eq!(stream_log_state(&mut handle, &key, seq1), "DONE");
            assert_eq!(stream_log_state(&mut handle, &key, seq2), "DONE");
            let registered: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM projection.private_memory_points \
                     WHERE tenant_id = $1 AND scope_id = $2",
                    &[&handle.tenant_id, &scope_id],
                )
                .expect("registry query")
                .get(0);
            assert_eq!(registered, 2);
        },
    );
}
