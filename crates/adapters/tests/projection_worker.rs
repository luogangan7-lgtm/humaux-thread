//! `adapters::tests::projection_worker` — `projection_worker::run_once` integration test (§17.4/§15.7) against a real
//!   Postgres and a real Qdrant.
//! Depends-on: crates=[async-trait, humaux-adapters, humaux-domain, humaux-infra-cell, humaux-local-secret-scan,
//!   humaux-projection, humaux-retrieval-provider, humaux-testkit, postgres, serde_json, sha2, sqlx, time, tokio];
//!   services=[PostgreSQL(any) r=[ops.commit_seq_seq, private.ingest_tickets] w=[control.memberships,
//!   control.private_reasoning_domains, control.tenants, control.users, control.workspace_memberships,
//!   control.workspaces, ops.outbox, private.events, private.evidence_objects, private.memory_affects,
//!   private.memory_evidence, private.memory_records, private.memory_subjects, private.subjects, projection.private_memory_points,
//!   projection.stream_checkpoints, projection.stream_log] x=[private.memory_subject_visibility_ok],
//!   PostgreSQL(role_batch_issuer), PostgreSQL(role_gateway), PostgreSQL(role_retrieval_worker), Qdrant(*),
//!   subprocess(gitleaks)]; env=[HUMAUX_RETRIEVAL_WORKER_PG_DSN, HUMAUX_TEST_GITLEAKS_BIN, HUMAUX_TEST_PG_DSN,
//!   HUMAUX_TEST_QDRANT_URL]; modules=[adapters::postgres, adapters::projection_worker, adapters::qdrant,
//!   adapters::remember, domain::egress, domain::error, domain::evidence, domain::identity, domain::ids,
//!   domain::subject, humaux-local-secret-scan, humaux-testkit, infra-cell::permit, infra-cell::resource,
//!   infra-cell::transport, projection::serving, retrieval-provider::adapters, retrieval-provider::contract]
//! Called-by: [cargo-test]
//! Invariants: [uses a throwaway tenant and Qdrant collection cleaned up on Drop; a ticket is marked done only after
//!   search-visible confirmation; no DSN, unreachable PG/Qdrant or no local gitleaks is a visible SKIP]
//! Spec: Baseline §17.4; §15.7; §79.2; ADR-0052; ADR-0055; ADR-0056
//!
//! Same convention as `outbox_batch_remember.rs`/`stream_repo.rs`: throwaway
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
    create_collection_body, ha_profile_for, query_dense, subject_index_body, tenant_index_body,
};
use humaux_adapters::remember::{self, RememberCommand};
use humaux_domain::egress::ProcessorId;
use humaux_domain::error::ErrorCode;
use humaux_domain::evidence::{EvidenceOriginClass, payload_sha256};
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId};
use humaux_domain::ids::{TenantId, UserId};
use humaux_domain::subject::SubjectId;
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

/// Card 21 fix pass: this suite's fixed §7.4 worker identity. `advance_prefix` writes it into
/// `projection.stream_checkpoints.projection_processor_id` (migration 0171); the attribution
/// itself is asserted in `tests/stream_repo.rs`
/// (`a_checkpoint_carries_the_processor_id_of_the_worker_that_advanced_it`).
const TEST_PROCESSOR_ID: Uuid = Uuid::from_u128(0x0171_0001);

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
        // dep: subprocess(gitleaks) — spawn a child process for this fixture
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
                            // dep: Qdrant(*) — Qdrant wire call for this fixture
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
        (
            format!("/collections/{collection}/index"),
            subject_index_body(),
        ),
    ] {
        transport
            .execute(
                &permit,
                // dep: Qdrant(*) — Qdrant wire call for this fixture
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
        // dep: PostgreSQL(any) — open a role-scoped PG connection/pool for this test
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
            // dep: PostgreSQL(role_gateway) — open a role-scoped PG connection/pool for this test
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
        _memory_ids: &[Uuid],
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
/// A throwaway `control.users` row with an ACTIVE membership in this fixture's tenant AND an
/// ACTIVE `control.workspace_memberships` row for `workspace_id` — the acting member a
/// `WORKSPACE_SHARED` evidence write needs to satisfy `evidence_objects`' WITH CHECK (ADR-0035,
/// card 13: the WORKSPACE_SHARED arm now EXISTS-checks `control.workspace_memberships` for the
/// row's own workspace, not tenant `control.memberships`).
fn seed_member(handle: &mut Handle, workspace_id: Uuid) -> Uuid {
    let user_id = seed_user(handle);
    handle
        .admin
        .execute(
            "INSERT INTO control.memberships (tenant_id, user_id, role, state) \
             VALUES ($1, $2, 'MEMBER', 'ACTIVE')",
            &[&handle.tenant_id, &user_id],
        )
        .expect("seed ACTIVE membership");
    handle
        .admin
        .execute(
            "INSERT INTO control.workspace_memberships (tenant_id, workspace_id, user_id, role, state) \
             VALUES ($1, $2, $3, 'MEMBER', 'ACTIVE')",
            &[&handle.tenant_id, &workspace_id, &user_id],
        )
        .expect("seed ACTIVE workspace membership");
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
/// `visibility_*` triple too. `resolve_memories` INNER JOINs `private.evidence_objects`, so a
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
        .or_else(|| evidence_visibility_workspace_id.map(|ws| seed_member(handle, ws)));
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
        subjects: humaux_domain::subject::SubjectDeclaration::default(),
        affects: Vec::new(),
        mood_half_life: None,
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
    // dep: PostgreSQL(any) — pool/txn query execution
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
            // dep: Qdrant(*) — Qdrant wire call for this fixture
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

fn stream_log_attempts(
    handle: &mut Handle,
    key: &(Uuid, &str, Uuid, &str, &str, &str),
    stream_seq: i64,
) -> i32 {
    handle
        .admin
        .query_one(
            "SELECT attempts FROM projection.stream_log \
             WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND domain=$4 \
               AND projection_kind=$5 AND projection_version=$6 AND stream_seq=$7",
            &[&key.0, &key.1, &key.2, &key.3, &key.4, &key.5, &stream_seq],
        )
        .expect("stream_log row must exist")
        .get(0)
}

/// Exact point count of the fixture's throwaway collection.
fn collection_point_count(handle: &Handle) -> u64 {
    let permit = authorize_cell_access(
        &handle.registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(30),
    )
    .expect("count permit");
    handle
        .rt
        .block_on(handle.transport.execute(
            &permit,
            // dep: Qdrant(*) — Qdrant wire call for this fixture
            IntraCellRequest {
                method: IntraCellMethod::Post,
                path: format!("/collections/{}/points/count", handle.collection),
                json_body: Some(serde_json::json!({ "exact": true })),
                headers: Vec::new(),
            },
        ))
        .expect("count succeeds")
        .json_body
        .and_then(|body| body["result"]["count"].as_u64())
        .expect("count response carries result.count")
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
    // dep: PostgreSQL(role_retrieval_worker) — open a role-scoped PG connection/pool for this test
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
        processor_id: ProcessorId(TEST_PROCESSOR_ID),
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
        _memory_ids: &[Uuid],
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
                // dep: PostgreSQL(role_retrieval_worker) — open a role-scoped PG connection/pool for this test
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
                processor_id: ProcessorId(TEST_PROCESSOR_ID),
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

            // ADR-0049 D-C, the failed half: a MEMORY_LIFECYCLE ticket that settles FAILED
            // leaves its carrier outbox row FAILED, not DONE (card 24 review P1 — a failed
            // lifecycle projection must not read as drained work).
            let memory_id: Uuid = handle
                .admin
                .query_one(
                    "SELECT memory_id FROM private.memory_records WHERE tenant_id = $1",
                    &[&handle.tenant_id],
                )
                .expect("the one seeded memory")
                .get(0);
            let ticket = reissue_lifecycle_ticket(&mut handle, scope_id, memory_id);
            let outcome = handle
                .rt
                .block_on(run_once(&deps, 10))
                .expect("second run_once succeeds");
            assert_eq!(outcome.failed, 1, "{outcome:?}");
            assert_eq!(stream_log_state(&mut handle, &key, ticket), "FAILED");
            assert_eq!(
                outbox_status(&mut handle, ticket),
                "FAILED",
                "the carrier mirrors the ticket's terminal"
            );
        },
    );
}

/// (5): the visibility probe reports the point as not (yet) observed after a successful upsert —
/// ADR-0052 D-E: that is a retry, not a verdict. The row stays `ISSUED` with one attempt spent
/// and `error_class = 'visibility_not_confirmed'`, and the checkpoint does not cross it, proving
/// `finish_row` actually awaits/branches on `verify_visible_via_transport`'s result rather than
/// unconditionally marking `Done` once the upsert call itself succeeds (F1). Before card 27 this
/// settled `FAILED` for good (audit C4).
#[test]
fn unconfirmed_visibility_retries_row_and_blocks_checkpoint() {
    run_db_fixture::<Fixture, _>(
        "unconfirmed_visibility_retries_row_and_blocks_checkpoint",
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
                // dep: PostgreSQL(role_retrieval_worker) — open a role-scoped PG connection/pool for this test
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
                processor_id: ProcessorId(TEST_PROCESSOR_ID),
            };

            let outcome = handle
                .rt
                .block_on(run_once(&deps, 10))
                .expect("run_once succeeds even with an unconfirmed visibility probe");
            assert_eq!(outcome.retried, 1, "{outcome:?}");
            assert_eq!(outcome.failed, 0);
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
            assert_eq!(stream_log_state(&mut handle, &key, stream_seq), "ISSUED");
            assert_eq!(stream_log_attempts(&mut handle, &key, stream_seq), 1);
            assert_eq!(
                stream_log_error_class(&mut handle, &key, stream_seq),
                Some("visibility_not_confirmed".to_owned())
            );
        },
    );
}

/// ADR-0052 D-E fixture: an embedder whose vectors are exactly as wide as the worker asks for —
/// so the worker's own dimension check passes and the width reaches Qdrant, whose 4-d fixture
/// collection refuses an 8-d point with HTTP 400.
struct WideEmbedder;

#[async_trait]
impl CardEmbedder for WideEmbedder {
    async fn embed_cards(
        &self,
        _tenant_id: TenantId,
        dimension: u32,
        cards: &[SealedRetrievalCard],
        _memory_ids: &[Uuid],
    ) -> Result<Vec<Vec<f32>>, ErrorCode> {
        Ok(cards
            .iter()
            .map(|_| vec![0.25_f32; dimension as usize])
            .collect())
    }
}

/// ADR-0052 D-E: a permanent Qdrant refusal (HTTP 400, here a vector-width mismatch between the
/// worker's configured dimension and the collection's) settles the ticket `FAILED`
/// `qdrant_upsert_rejected` on the FIRST attempt — no retry budget spent on a request that can
/// never succeed — and leaves no point and no registry row. Fault injection: classify 400 as
/// transient in `QdrantTransportError::is_transient` ⇒ the row stays `ISSUED` and this is red.
#[test]
fn permanent_qdrant_400_fails_immediately_and_leaves_no_point() {
    run_db_fixture::<Fixture, _>(
        "permanent_qdrant_400_fails_immediately_and_leaves_no_point",
        |mut handle| {
            let scope_id = Uuid::new_v4();
            let stream_seq = seed_memory(&mut handle, scope_id, "eight wide vector");
            let provider = Arc::new(TestDoubleProvider::new(
                embedding_model(),
                unused_rerank_model(),
            ));
            let mut deps = handle.rt.block_on(deps_for(&handle, scope_id, provider));
            deps.embedder = Arc::new(WideEmbedder);
            deps.dimension = 8;

            let outcome = handle
                .rt
                .block_on(run_once(&deps, 10))
                .expect("run_once succeeds with a per-row Qdrant refusal");
            assert_eq!((outcome.failed, outcome.retried, outcome.done), (1, 0, 0));
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
                Some("qdrant_upsert_rejected".to_owned())
            );
            assert_eq!(stream_log_attempts(&mut handle, &key, stream_seq), 0);
            let registered: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM projection.private_memory_points WHERE tenant_id = $1",
                    &[&handle.tenant_id],
                )
                .expect("registry query")
                .get(0);
            assert_eq!(registered, 0, "a refused upsert registers nothing");
            assert_eq!(collection_point_count(&handle), 0, "and leaves no point");
        },
    );
}

/// Card 30b (ADR-0056 D-A): memories whose card text carries a date, an e-mail address, a phone
/// number, a 12-digit order number and a random UUID are ordinary private cards on the §7.5 B
/// seal — all five project `DONE`, none settles `secret_scan_rejected`. Fault: the
/// contribution-privacy rules back on `seal_card` ⇒ five FAILED ⇒ red.
#[test]
fn cards_with_date_email_phone_number_and_uuid_project_done() {
    run_db_fixture::<Fixture, _>(
        "cards_with_date_email_phone_number_and_uuid_project_done",
        |mut handle| {
            let scope_id = Uuid::new_v4();
            let seqs: Vec<i64> = [
                "the billing cutover is on 2026-10-15",
                "write to a@example.test about the invoice",
                "call +1 (415) 555-0123 for the on-call rota",
                "order 123456789012 shipped late",
                "incident 3f0c9a4e-1b7d-4c55-9e21-79869668a78d is closed",
            ]
            .into_iter()
            .map(|content| seed_memory(&mut handle, scope_id, content))
            .collect();
            let provider = Arc::new(TestDoubleProvider::new(
                embedding_model(),
                unused_rerank_model(),
            ));
            let deps = handle.rt.block_on(deps_for(&handle, scope_id, provider));
            let outcome = handle
                .rt
                .block_on(run_once(&deps, 10))
                .expect("run_once succeeds");
            assert_eq!((outcome.done, outcome.failed, outcome.retried), (5, 0, 0));
            let key = (
                handle.tenant_id,
                "workspace",
                scope_id,
                "private_memory",
                "PRIVATE_MEMORY",
                "v1",
            );
            for seq in seqs {
                assert_eq!(stream_log_state(&mut handle, &key, seq), "DONE");
                assert_eq!(stream_log_error_class(&mut handle, &key, seq), None);
            }
            assert_eq!(collection_point_count(&handle), 5);
        },
    );
}

/// ADR-0052 D-E, unchanged by ADR-0056: a card carrying a real gitleaks finding (the fake
/// GitHub-shaped vector of `contribution_scan.rs`) is a verdict on the card — `FAILED
/// secret_scan_rejected`, no retry, no point. Fault: gitleaks removed from the seal path ⇒ DONE
/// ⇒ red.
#[test]
fn a_card_with_a_real_gitleaks_finding_settles_failed_secret_scan_rejected() {
    run_db_fixture::<Fixture, _>(
        "a_card_with_a_real_gitleaks_finding_settles_failed_secret_scan_rejected",
        |mut handle| {
            let scope_id = Uuid::new_v4();
            let seq = seed_memory(
                &mut handle,
                scope_id,
                "the CI bot uses ghp_RkqFzVpLwNyHtBvDgXsWuCePjMoTnAiSyEkl for releases",
            );
            let provider = Arc::new(TestDoubleProvider::new(
                embedding_model(),
                unused_rerank_model(),
            ));
            let deps = handle.rt.block_on(deps_for(&handle, scope_id, provider));
            let outcome = handle
                .rt
                .block_on(run_once(&deps, 10))
                .expect("run_once succeeds with a per-row refusal");
            assert_eq!((outcome.failed, outcome.retried, outcome.done), (1, 0, 0));
            let key = (
                handle.tenant_id,
                "workspace",
                scope_id,
                "private_memory",
                "PRIVATE_MEMORY",
                "v1",
            );
            assert_eq!(stream_log_state(&mut handle, &key, seq), "FAILED");
            assert_eq!(
                stream_log_error_class(&mut handle, &key, seq),
                Some("secret_scan_rejected".to_owned())
            );
            assert_eq!(collection_point_count(&handle), 0, "not projected");
        },
    );
}

/// ADR-0052 D-E (b) fixture: forwards everything to the real transport, and right after the
/// upsert PUT lands it moves the memory's `updated_at` — so the registration that follows sees
/// the source changed (`SourceChanged`, D-A's race in miniature) and refuses to bind the point
/// the upsert just wrote.
struct SourceChangesAfterUpsert {
    inner: Arc<HttpIntraCellTransport>,
    admin_dsn: String,
    memory_id: std::sync::Mutex<Option<Uuid>>,
}

#[async_trait]
impl IntraCellHttpTransport for SourceChangesAfterUpsert {
    async fn execute(
        &self,
        permit: &CellAccessPermit,
        request: IntraCellRequest,
    ) -> Result<IntraCellResponse, IntraCellError> {
        let is_upsert = matches!(request.method, IntraCellMethod::Put)
            && request.path.contains("/points")
            && !request.path.contains("/points/");
        let response = self.inner.execute(permit, request).await?;
        let memory_id = *self.memory_id.lock().expect("fixture mutex");
        if let (true, Some(memory_id)) = (is_upsert, memory_id) {
            // A fresh OS thread: the sync client runs its own runtime, which it may not do on
            // this async task's thread.
            let dsn = self.admin_dsn.clone();
            std::thread::spawn(move || {
                // dep: PostgreSQL(any) — the fixture's owner client moves the source mid-row
                let mut admin = Client::connect(&dsn, NoTls).expect("owner connects");
                admin
                    .execute(
                        "UPDATE private.memory_records \
                         SET updated_at = updated_at + interval '1 second' WHERE memory_id = $1",
                        &[&memory_id],
                    )
                    .expect("move the source");
            })
            .join()
            .expect("owner thread");
        }
        Ok(response)
    }
}

/// ADR-0052 D-E (b): when the upsert landed but the registry refused the binding, `finish_row`
/// deletes the point it just wrote — otherwise that point is live in Qdrant and bound to nothing,
/// so no later retirement could ever find it. The refusal here (`SourceChanged`) is transient, so
/// the ticket is retried (ISSUED, attempts 1, `registry_failed`). Fault injection: delete the
/// compensating `delete_points` call ⇒ the collection keeps one orphan point and this is red.
#[test]
fn upsert_then_registry_refusal_deletes_the_orphan_point() {
    run_db_fixture::<Fixture, _>(
        "upsert_then_registry_refusal_deletes_the_orphan_point",
        |mut handle| {
            let scope_id = Uuid::new_v4();
            let (stream_seq, memory_id) = seed_memory_with_visibility(
                &mut handle,
                scope_id,
                "source moves after upsert",
                "TENANT_SHARED",
                None,
                None,
            );
            let admin_dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("fixture DSN");
            let provider = Arc::new(TestDoubleProvider::new(
                embedding_model(),
                unused_rerank_model(),
            ));
            let mut deps = handle.rt.block_on(deps_for(&handle, scope_id, provider));
            deps.transport = Arc::new(SourceChangesAfterUpsert {
                inner: handle.transport.clone(),
                admin_dsn,
                memory_id: std::sync::Mutex::new(Some(memory_id)),
            });

            let outcome = handle
                .rt
                .block_on(run_once(&deps, 10))
                .expect("run_once succeeds with a refused registration");
            assert_eq!((outcome.retried, outcome.failed, outcome.done), (1, 0, 0));
            let key = (
                handle.tenant_id,
                "workspace",
                scope_id,
                "private_memory",
                "PRIVATE_MEMORY",
                "v1",
            );
            assert_eq!(stream_log_state(&mut handle, &key, stream_seq), "ISSUED");
            assert_eq!(stream_log_attempts(&mut handle, &key, stream_seq), 1);
            assert_eq!(
                stream_log_error_class(&mut handle, &key, stream_seq),
                Some("registry_failed".to_owned())
            );
            assert_eq!(
                collection_point_count(&handle),
                0,
                "the point the refused registration would have bound is deleted"
            );
        },
    );
}

/// (3): a batch of two rows (seq 1, seq 2) where the embedder is forced to fail on the first
/// call — since `run_once` processes ascending `stream_seq`, that is seq 1. §15.7: seq 1
/// settles `FAILED`, seq 2 may settle `DONE`, but `projection_highwater` must stay at 0 (the
/// prefix before the first gap), never cross the `FAILED` row. The forced error is a PERMANENT
/// one (`ProviderPermanent`): since ADR-0052 a transient provider error is a retry and the row
/// would stay `ISSUED` (which blocks the prefix just the same, but is not what this test pins).
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
        provider.force_next_error(ErrorCode::ProviderPermanent);
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
        assert_eq!(
            stream_log_error_class(&mut handle, &key, seq1),
            Some("embedding_rejected".to_owned())
        );
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

/// 0140 must widen the evidence half of `resolve_memories`' INNER JOIN too: a memory whose
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

// ============================================================================
// §6.1.3 / ADR-0029 (card 9): subject-scoped projection payload, any-of dense prefilter, and the
// 0155 RESTRICTIVE subject-visibility policy's headless exemption.
// ============================================================================

fn seed_subject(handle: &mut Handle, name: &str) -> Uuid {
    handle
        .admin
        .query_one(
            "INSERT INTO private.subjects (tenant_id, kind, display_name) \
             VALUES ($1, 'PERSON', $2) RETURNING subject_id",
            &[&handle.tenant_id, &name],
        )
        .expect("owner seeds subject")
        .get(0)
}

fn link_subject(handle: &mut Handle, memory_id: Uuid, subject_id: Uuid) {
    handle
        .admin
        .execute(
            "INSERT INTO private.memory_subjects \
               (tenant_id, memory_id, subject_id, relation, source_kind, confidence_bp) \
             VALUES ($1, $2, $3, 'ABOUT', 'DECLARED', 10000)",
            &[&handle.tenant_id, &memory_id, &subject_id],
        )
        .expect("owner links memory to subject");
}

/// Payload `subject_ids` read back from Qdrant for one point (sorted, as the writer emits).
fn scrolled_subject_ids(scrolled: &serde_json::Value, point: Uuid) -> Vec<String> {
    scrolled["result"]["points"]
        .as_array()
        .expect("scroll result.points")
        .iter()
        .find(|p| p["id"].as_str() == Some(&point.to_string()))
        .unwrap_or_else(|| panic!("point {point} missing from scroll"))["payload"]["subject_ids"]
        .as_array()
        .expect("subject_ids payload array")
        .iter()
        .map(|v| v.as_str().expect("uuid string").to_owned())
        .collect()
}

fn sorted_ids(ids: &[Uuid]) -> Vec<String> {
    let mut v: Vec<String> = ids.iter().map(ToString::to_string).collect();
    v.sort();
    v
}

fn hit_points(hits: &[DenseCandidate]) -> BTreeSet<Uuid> {
    hits.iter()
        .map(|c| match c.point_id {
            PointId::Uuid(id) => id,
            PointId::Num(n) => panic!("unexpected numeric point id {n}"),
        })
        .collect()
}

/// Polls a dense query until it returns `expected` hits (real Qdrant read-after-write lag under
/// Weak ordering, same pattern as T5) and returns the final candidate list either way.
fn poll_dense(
    handle: &Handle,
    permit: &CellAccessPermit,
    query: &DenseQuery,
    expected: usize,
) -> Vec<DenseCandidate> {
    handle.rt.block_on(async {
        let mut last = Vec::new();
        for _ in 0..40 {
            last = query_dense(handle.transport.as_ref(), permit, query)
                .await
                .expect("dense query succeeds");
            if last.len() == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        last
    })
}

/// (S1, ADR-0029 D-C): two subjects A/B, four memories (A-only, B-only, A+B, and a USER_PRIVATE
/// A-only owned by user X) go through the real projection worker; the payload `subject_ids`
/// arrays are read back verbatim; `DenseQuery::with_subject_ids(&[A])` returns exactly the
/// A-linked points and never the B-only one (fault sentinel: stubbing
/// `DenseQueryFilter::about_any_of` to a no-op makes the B-only point appear here); the same
/// query as user Y (same tenant, not the owner) drops the USER_PRIVATE row — subject narrowing
/// is ANDed with §6.1.2 visibility, never a substitute for it.
#[test]
#[allow(clippy::too_many_lines)] // One causal chain: seed -> project -> payload -> filtered queries.
fn subject_scoped_dense_query_returns_only_memories_about_that_subject() {
    run_db_fixture::<Fixture, _>(
        "subject_scoped_dense_query_returns_only_memories_about_that_subject",
        |mut handle| {
            let scope_id = Uuid::new_v4();
            let user_x = seed_user(&mut handle);
            let user_y = seed_user(&mut handle);
            let subject_a = seed_subject(&mut handle, "Ada Customer");
            let subject_b = seed_subject(&mut handle, "Bob Customer");
            let (_, only_a) = seed_memory_with_visibility(
                &mut handle,
                scope_id,
                "about A only",
                "TENANT_SHARED",
                None,
                None,
            );
            let (_, only_b) = seed_memory_with_visibility(
                &mut handle,
                scope_id,
                "about B only",
                "TENANT_SHARED",
                None,
                None,
            );
            let (_, both) = seed_memory_with_visibility(
                &mut handle,
                scope_id,
                "about A and B",
                "TENANT_SHARED",
                None,
                None,
            );
            let (_, private_a) = seed_memory_with_visibility(
                &mut handle,
                scope_id,
                "user X private about A",
                "USER_PRIVATE",
                Some(user_x),
                None,
            );
            link_subject(&mut handle, only_a, subject_a);
            link_subject(&mut handle, only_b, subject_b);
            link_subject(&mut handle, both, subject_a);
            link_subject(&mut handle, both, subject_b);
            link_subject(&mut handle, private_a, subject_a);

            let provider = Arc::new(TestDoubleProvider::new(
                embedding_model(),
                unused_rerank_model(),
            ));
            let deps = handle.rt.block_on(deps_for(&handle, scope_id, provider));
            let outcome = handle
                .rt
                .block_on(run_once(&deps, 10))
                .expect("run_once succeeds");
            assert_eq!(
                (outcome.done, outcome.failed),
                (4, 0),
                "every subject-linked row projects"
            );

            let p_only_a = point_id_for_memory(&mut handle, only_a);
            let p_only_b = point_id_for_memory(&mut handle, only_b);
            let p_both = point_id_for_memory(&mut handle, both);
            let p_private_a = point_id_for_memory(&mut handle, private_a);
            let permit = authorize_cell_access(
                &handle.registry,
                IntraCellResource::QDRANT_REST,
                Duration::from_secs(30),
            )
            .expect("admin Qdrant permit");
            let scrolled =
                scroll_payloads(&handle, &permit, &[p_only_a, p_only_b, p_both, p_private_a]);
            assert_eq!(
                scrolled_subject_ids(&scrolled, p_only_a),
                sorted_ids(&[subject_a])
            );
            assert_eq!(
                scrolled_subject_ids(&scrolled, p_only_b),
                sorted_ids(&[subject_b])
            );
            assert_eq!(
                scrolled_subject_ids(&scrolled, p_both),
                sorted_ids(&[subject_a, subject_b])
            );
            assert_eq!(
                scrolled_subject_ids(&scrolled, p_private_a),
                sorted_ids(&[subject_a])
            );

            let placement = TenantPlacementRow {
                tenant_id: TenantId(handle.tenant_id),
                projection_family: RetrievalFamily::PrivateMemoryV1,
                collection_name: handle.collection.clone(),
                shard_key: None,
                placement_class: PlacementClass::SharedFallback,
                point_count: 4,
                bytes_estimate: 0,
                promotion_state: PromotionState::Stable,
            };
            let a = [SubjectId(subject_a)];
            let b = [SubjectId(subject_b)];
            let as_x_about_a =
                dense_query_for_user(&handle, &placement, user_x).with_subject_ids(&a);
            let hits = poll_dense(&handle, &permit, &as_x_about_a, 3);
            assert_eq!(
                hit_points(&hits),
                BTreeSet::from([p_only_a, p_both, p_private_a]),
                "subject A any-of: A-only + A+B + X's private A row; never the B-only point"
            );
            let as_y_about_a =
                dense_query_for_user(&handle, &placement, user_y).with_subject_ids(&a);
            let hits = poll_dense(&handle, &permit, &as_y_about_a, 2);
            assert_eq!(
                hit_points(&hits),
                BTreeSet::from([p_only_a, p_both]),
                "user Y is not authorized for X's USER_PRIVATE row: subject filter ANDs with visibility"
            );
            let as_x_about_b =
                dense_query_for_user(&handle, &placement, user_x).with_subject_ids(&b);
            let hits = poll_dense(&handle, &permit, &as_x_about_b, 2);
            assert_eq!(hit_points(&hits), BTreeSet::from([p_only_b, p_both]));
            let unscoped = dense_query_for_user(&handle, &placement, user_x);
            let hits = poll_dense(&handle, &permit, &unscoped, 4);
            assert_eq!(
                hits.len(),
                4,
                "no subject filter = the plain visibility read"
            );
        },
    );
}

/// Row count of one memory under `SET LOCAL ROLE {role}` with the tenant GUC set, inside a
/// rolled-back transaction on the superuser fixture connection.
fn count_as_role(handle: &mut Handle, role: &str, tenant: Uuid, memory_id: Uuid) -> i64 {
    let mut txn = handle.admin.transaction().expect("txn");
    // dep: PostgreSQL(any) — role switch before the scoped statements for `count_as_role`
    txn.batch_execute(&format!(
        "SET LOCAL ROLE {role}; SET LOCAL humaux.tenant_id = '{tenant}'; SET LOCAL humaux.user_id = '';"
    ))
    .expect("set role + GUCs");
    let n: i64 = txn
        .query_one(
            "SELECT count(*) FROM private.memory_records WHERE memory_id = $1",
            &[&memory_id],
        )
        .expect("count under role")
        .get(0);
    txn.rollback().expect("rollback");
    n
}

/// (S2, ADR-0029 D-B): migration 0155's RESTRICTIVE `memory_records_subject_visibility` policy.
/// A subject-linked TENANT_SHARED memory: `role_gateway` under the owning tenant reads it (1);
/// a gateway session under another tenant reads 0; with the predicate faulted to always-false
/// (inside a rolled-back transaction, so the fault never leaks) the gateway read goes to 0 —
/// proving the policy is wired through the function — while `role_retrieval_worker` still
/// reads the row in both states (the §15.7 watermark-freeze regression sentinel: the headless
/// role is exempt from the RESTRICTIVE policy, so a projection run never skips a row).
#[test]
fn subject_visibility_policy_gates_gateway_reads_but_never_the_retrieval_worker() {
    run_db_fixture::<Fixture, _>(
        "subject_visibility_policy_gates_gateway_reads_but_never_the_retrieval_worker",
        |mut handle| {
            let scope_id = Uuid::new_v4();
            let subject_a = seed_subject(&mut handle, "Gated Customer");
            let (_, memory_id) = seed_memory_with_visibility(
                &mut handle,
                scope_id,
                "gated by subject A",
                "TENANT_SHARED",
                None,
                None,
            );
            link_subject(&mut handle, memory_id, subject_a);
            let tenant = handle.tenant_id;

            assert_eq!(
                count_as_role(&mut handle, "role_gateway", tenant, memory_id),
                1
            );
            assert_eq!(
                count_as_role(&mut handle, "role_retrieval_worker", tenant, memory_id),
                1
            );
            assert_eq!(
                count_as_role(&mut handle, "role_gateway", Uuid::new_v4(), memory_id),
                0,
                "another tenant's gateway session is not authorized"
            );

            // Fault: the predicate answers false for every row. Superuser CREATE OR REPLACE
            // keeps the owner (role_migration_owner) and SECURITY DEFINER; rolled back below.
            let mut txn = handle.admin.transaction().expect("fault txn");
            txn.batch_execute(
                "CREATE OR REPLACE FUNCTION private.memory_subject_visibility_ok(p_tenant_id uuid, p_memory_id uuid) \
                 RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog \
                 AS $$ SELECT false $$;",
            )
            .expect("fault the predicate");
            let count_in = |txn: &mut postgres::Transaction<'_>, role: &str| -> i64 {
                let mut sp = txn.savepoint("role_probe").expect("savepoint");
                // dep: PostgreSQL(any) — role switch before the scoped statements for `subject_visibility_policy_gates_gateway_reads_but_never_the_retrieval_worker`
                sp.batch_execute(&format!(
                    "SET LOCAL ROLE {role}; SET LOCAL humaux.tenant_id = '{tenant}'; \
                     SET LOCAL humaux.user_id = '';"
                ))
                .expect("set role");
                let n: i64 = sp
                    .query_one(
                        "SELECT count(*) FROM private.memory_records WHERE memory_id = $1",
                        &[&memory_id],
                    )
                    .expect("count under role")
                    .get(0);
                sp.rollback().expect("rollback savepoint");
                n
            };
            assert_eq!(
                count_in(&mut txn, "role_gateway"),
                0,
                "faulted predicate hides the row from the gateway: the RESTRICTIVE policy is live"
            );
            assert_eq!(
                count_in(&mut txn, "role_retrieval_worker"),
                1,
                "role_retrieval_worker is exempt from the RESTRICTIVE policy (§15.7 sentinel)"
            );
            txn.rollback().expect("undo the fault");

            assert_eq!(
                count_as_role(&mut handle, "role_gateway", tenant, memory_id),
                1
            );

            // ACL (0155 A2): the definer predicate reads memory_subjects with the owner's
            // privileges, so only the two roles the policy names may call it. role_batch_issuer
            // has USAGE on schema private and no grant on the subject tables — a direct call
            // must be a permission error, never a boolean (the one-bit oracle the review found).
            let mut txn = handle.admin.transaction().expect("acl txn");
            // dep: PostgreSQL(role_batch_issuer) — role switch before the scoped statements for `subject_visibility_policy_gates_gateway_reads_but_never_the_retrieval_worker`
            txn.batch_execute(&format!(
                "SET LOCAL ROLE role_batch_issuer; SET LOCAL humaux.tenant_id = '{tenant}';"
            ))
            .expect("set role");
            let denied = txn
                .query_one(
                    "SELECT private.memory_subject_visibility_ok($1, $2)",
                    &[&tenant, &memory_id],
                )
                .expect_err("role_batch_issuer has no EXECUTE on the subject predicate");
            assert_eq!(
                denied.code().map(|c| c.code()),
                Some("42501"),
                "insufficient_privilege, not a boolean: {denied}"
            );
            txn.rollback().expect("rollback acl probe");
        },
    );
}

/// Adds a SECOND memory bound to the SAME Evidence as `sibling_of`, exactly the shape
/// `distill_repo::persist` writes when one pass extracts N memories from one Evidence
/// (`role='PRIMARY', ordinal 0` per memory, distill_repo.rs:457) — the multi-memory Evidence
/// every ticket issuer in the system can produce and that card 9 reported as unhandled.
fn attach_sibling_memory(handle: &mut Handle, sibling_of: Uuid, content: &str) -> Uuid {
    let evidence_id: Uuid = handle
        .admin
        .query_one(
            "SELECT evidence_id FROM private.memory_evidence WHERE memory_id = $1 \
             ORDER BY (role = 'PRIMARY') DESC, ordinal ASC LIMIT 1",
            &[&sibling_of],
        )
        .expect("the first memory's evidence")
        .get(0);
    let content_json = serde_json::json!({
        "title": format!("title: {content}"),
        "key_claim": format!("key claim: {content}"),
        "evidence_excerpt": content,
    });
    let mut txn = handle.admin.transaction().expect("sibling txn");
    let memory_id: Uuid = txn
        .query_one(
            "INSERT INTO private.memory_records \
               (tenant_id, memory_type, content, visibility_class, visibility_user_id, \
                visibility_workspace_id, authority_class, confidence, status, asserted_at) \
             VALUES ($1,'NOTE',$2,'TENANT_SHARED',NULL,NULL,'PrivateKnowledge',0.9,'active',now()) \
             RETURNING memory_id",
            &[&handle.tenant_id, &content_json],
        )
        .expect("insert sibling memory")
        .get(0);
    // dep: PostgreSQL(any) — pool/txn query execution
    txn.execute(
        "INSERT INTO private.memory_evidence (memory_id, evidence_id, role, ordinal) \
         VALUES ($1, $2, 'PRIMARY', 0)",
        &[&memory_id, &evidence_id],
    )
    .expect("link the sibling to the same evidence");
    txn.commit().expect("commit sibling");
    memory_id
}

/// Card 9's reported limit, closed by card 20 (ADR-0042): a ticket binds an EVIDENCE, and one
/// Evidence routinely carries N memories (`distill_repo` writes one `PRIMARY` row per extracted
/// memory against the single Evidence `remember` issued the ticket for). The worker used to
/// resolve `ORDER BY (role='PRIMARY') DESC, ordinal ASC LIMIT 1`, so memories 2..N of every
/// multi-output distill were never indexed, and a MEMORY_LIFECYCLE / 0155-backfill ticket aimed
/// at memory #2 silently re-projected memory #1 instead.
///
/// Both halves are asserted here on ONE causal chain: the remember-time ticket must index BOTH
/// memories, and the re-issued lifecycle ticket must re-project BOTH (not just the PRIMARY-first
/// one). Fault control: restore the `LIMIT 1` in `projection_worker::resolve_memories` and the
/// first `point_id_for_memory(sibling)` lookup panics — there is no registered point for it.
#[test]
fn one_ticket_projects_every_memory_its_evidence_carries() {
    run_db_fixture::<Fixture, _>(
        "one_ticket_projects_every_memory_its_evidence_carries",
        |mut handle| {
            let scope_id = Uuid::new_v4();
            let (_, memory_a) = seed_memory_with_visibility(
                &mut handle,
                scope_id,
                "first memory of a two-memory evidence",
                "TENANT_SHARED",
                None,
                None,
            );
            let memory_b =
                attach_sibling_memory(&mut handle, memory_a, "second memory of the same evidence");

            let provider = Arc::new(TestDoubleProvider::new(
                embedding_model(),
                unused_rerank_model(),
            ));
            let deps = handle.rt.block_on(deps_for(&handle, scope_id, provider));
            let outcome = handle
                .rt
                .block_on(run_once(&deps, 10))
                .expect("run_once succeeds");
            assert_eq!(
                (outcome.done, outcome.failed),
                (1, 0),
                "one ticket, one row settled DONE — N memories is not N rows"
            );

            let point_a = point_id_for_memory(&mut handle, memory_a);
            let point_b = point_id_for_memory(&mut handle, memory_b);
            assert_ne!(
                point_a, point_b,
                "each memory gets its own deterministic point id"
            );
            let permit = authorize_cell_access(
                &handle.registry,
                IntraCellResource::QDRANT_REST,
                Duration::from_secs(30),
            )
            .expect("admin Qdrant permit");
            let scrolled = scroll_payloads(&handle, &permit, &[point_a, point_b]);
            let indexed = scrolled["result"]["points"].as_array().map_or(0, Vec::len);
            assert_eq!(indexed, 2, "both memories are in the index: {scrolled}");

            // Second half: a lifecycle ticket aimed at the SIBLING re-projects it, rather than
            // resolving PRIMARY-first back to memory_a and dropping the sibling's change.
            let reissued = reissue_lifecycle_ticket(&mut handle, scope_id, memory_b);
            let outcome = handle
                .rt
                .block_on(run_once(&deps, 10))
                .expect("second run_once succeeds");
            assert_eq!((outcome.done, outcome.failed), (1, 0));
            let key = (
                handle.tenant_id,
                "workspace",
                scope_id,
                "private_memory",
                "PRIVATE_MEMORY",
                "v1",
            );
            assert_eq!(stream_log_state(&mut handle, &key, reissued), "DONE");
            assert_eq!(
                point_id_for_memory(&mut handle, memory_b),
                point_b,
                "same deterministic point id: the sibling's payload was rewritten in place"
            );
        },
    );
}

/// Re-issues one `MEMORY_LIFECYCLE` ticket for `memory_id` on the fixture's `v1` workspace
/// stream — the §60 `issue_stream_log_row` + `insert_outbox` sequence that migration 0155's
/// backfill block (C.) and `memory_governance_repo::issue_lifecycle_ticket` both perform, bound
/// to the memory's PRIMARY-first Evidence exactly as they bind it.
fn reissue_lifecycle_ticket(handle: &mut Handle, scope_id: Uuid, memory_id: Uuid) -> i64 {
    let mut txn = handle.admin.transaction().expect("ticket txn");
    let evidence_id: Uuid = txn
        .query_one(
            "SELECT evidence_id FROM private.memory_evidence WHERE memory_id = $1 \
             ORDER BY (role = 'PRIMARY') DESC, ordinal ASC LIMIT 1",
            &[&memory_id],
        )
        .expect("bound evidence")
        .get(0);
    let commit_seq: i64 = txn
        .query_one("SELECT nextval('ops.commit_seq_seq')", &[])
        .expect("commit seq")
        .get(0);
    let stream_seq: i64 = txn
        .query_one(
            "UPDATE projection.stream_checkpoints SET issued_highwater = issued_highwater + 1 \
             WHERE tenant_id = $1 AND scope_kind = 'workspace' AND scope_id = $2 \
               AND domain = 'private_memory' AND projection_kind = 'PRIVATE_MEMORY' \
               AND projection_version = 'v1' \
             RETURNING issued_highwater",
            &[&handle.tenant_id, &scope_id],
        )
        .expect("bump issued_highwater")
        .get(0);
    // dep: PostgreSQL(any) — pool/txn query execution
    txn.execute(
        "INSERT INTO projection.stream_log \
           (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, \
            stream_seq, commit_seq) \
         VALUES ($1, 'workspace', $2, 'private_memory', 'PRIVATE_MEMORY', 'v1', $3, $4)",
        &[&handle.tenant_id, &scope_id, &stream_seq, &commit_seq],
    )
    .expect("stream_log row");
    // dep: PostgreSQL(any) — pool/txn query execution
    txn.execute(
        "INSERT INTO ops.outbox (tenant_id, commit_seq, stream_seq, event_type, evidence_id) \
         VALUES ($1, $2, $3, 'MEMORY_LIFECYCLE', $4)",
        &[&handle.tenant_id, &commit_seq, &stream_seq, &evidence_id],
    )
    .expect("outbox row");
    txn.commit().expect("commit ticket");
    stream_seq
}

/// (S3, ADR-0029 D-A backfill): a point projected BEFORE its memory carried any subject link
/// has `subject_ids: []` and the any-of prefilter never matches it — the shape every point
/// projected before 0155 is in (no field at all, same non-match). Migration 0155's block C.
/// re-issues one MEMORY_LIFECYCLE ticket per such linked memory; this is that mechanism end to
/// end: link A after the first projection, re-issue the ticket the way 0155 does, run the
/// worker again — the SAME point id (registration is keyed by `updated_at` + `body_sha256`,
/// which a link does not touch) now carries `[A]`, the ticket settles DONE, and the A-scoped
/// dense query hits the point it could not see before.
#[test]
#[allow(clippy::too_many_lines)] // One causal chain: project -> stale miss -> link -> re-ticket -> rewritten hit.
fn reissued_lifecycle_ticket_reprojects_the_same_point_with_current_subject_ids() {
    run_db_fixture::<Fixture, _>(
        "reissued_lifecycle_ticket_reprojects_the_same_point_with_current_subject_ids",
        |mut handle| {
            let scope_id = Uuid::new_v4();
            let user = seed_user(&mut handle);
            let subject_a = seed_subject(&mut handle, "Late-linked Customer");
            let (_, memory_id) = seed_memory_with_visibility(
                &mut handle,
                scope_id,
                "linked after projection",
                "TENANT_SHARED",
                None,
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
                .expect("first run_once succeeds");
            assert_eq!((outcome.done, outcome.failed), (1, 0));
            let point = point_id_for_memory(&mut handle, memory_id);
            let permit = authorize_cell_access(
                &handle.registry,
                IntraCellResource::QDRANT_REST,
                Duration::from_secs(30),
            )
            .expect("admin Qdrant permit");
            let scrolled = scroll_payloads(&handle, &permit, &[point]);
            assert_eq!(
                scrolled_subject_ids(&scrolled, point),
                Vec::<String>::new(),
                "projected before any link: empty subject_ids"
            );
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
            let a = [SubjectId(subject_a)];
            let about_a = dense_query_for_user(&handle, &placement, user).with_subject_ids(&a);
            // Presence first (read-after-write lag), then the scoped miss is a real miss.
            assert_eq!(
                poll_dense(
                    &handle,
                    &permit,
                    &dense_query_for_user(&handle, &placement, user),
                    1
                )
                .len(),
                1
            );
            let stale = handle
                .rt
                .block_on(query_dense(handle.transport.as_ref(), &permit, &about_a))
                .expect("dense query succeeds");
            assert!(
                stale.is_empty(),
                "the stale payload cannot be matched by the any-of prefilter"
            );

            link_subject(&mut handle, memory_id, subject_a);
            let reissued = reissue_lifecycle_ticket(&mut handle, scope_id, memory_id);
            let outcome = handle
                .rt
                .block_on(run_once(&deps, 10))
                .expect("second run_once succeeds");
            assert_eq!(
                (outcome.done, outcome.failed),
                (1, 0),
                "the re-issued ticket projects"
            );
            let key = (
                handle.tenant_id,
                "workspace",
                scope_id,
                "private_memory",
                "PRIVATE_MEMORY",
                "v1",
            );
            assert_eq!(stream_log_state(&mut handle, &key, reissued), "DONE");
            assert_eq!(
                point_id_for_memory(&mut handle, memory_id),
                point,
                "same deterministic point id: the payload was rewritten in place"
            );
            let scrolled = scroll_payloads(&handle, &permit, &[point]);
            assert_eq!(
                scrolled_subject_ids(&scrolled, point),
                sorted_ids(&[subject_a])
            );
            let hits = poll_dense(&handle, &permit, &about_a, 1);
            assert_eq!(
                hit_points(&hits),
                BTreeSet::from([point]),
                "after re-projection the A-scoped prefilter sees the point"
            );
        },
    );
}

/// ADR-0049 (card 24 rehearsal, 2026-09-26): the MEMORY_LIFECYCLE ticket a `memory.supersede`
/// issues used to fail `registry_failed` every time — the registry binds live sources only
/// (`current_source_matches`), and ADR-0018 §4's "re-upsert with status=superseded" predates
/// it — which wedged the §15.4 prefix behind the ticket and broke every token-carrying recall
/// on the stream. A dead memory's ticket now retires its binding, removes its point, and
/// settles `DONE`. Fault control: make `resolve_and_embed` route the superseded memory through
/// `finish_row` again and this reads `(0, 1)` with `error_class = registry_failed`.
#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one causal chain — project, supersede, retire, restore, revive — reads best in one place; ADR-0049"
)]
fn a_superseded_memory_ticket_retires_its_point_and_settles_done() {
    run_db_fixture::<Fixture, _>(
        "a_superseded_memory_ticket_retires_its_point_and_settles_done",
        |mut handle| {
            let scope_id = Uuid::new_v4();
            let (_, memory_a) = seed_memory_with_visibility(
                &mut handle,
                scope_id,
                "the rule before it was replaced",
                "TENANT_SHARED",
                None,
                None,
            );
            let (_, memory_b) = seed_memory_with_visibility(
                &mut handle,
                scope_id,
                "the rule that replaced it",
                "TENANT_SHARED",
                None,
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
                .expect("first run_once succeeds");
            assert_eq!((outcome.done, outcome.failed), (2, 0), "{outcome:?}");
            let point_a = point_id_for_memory(&mut handle, memory_a);
            let point_b = point_id_for_memory(&mut handle, memory_b);

            // §36 supersede, as `memory_governance_repo` writes it (G59-4: status and
            // superseded_by flip together; `updated_at` is NOT touched — the registry identity
            // survives, which is what makes the restore below a revive, not a new point).
            handle
                .admin
                .execute(
                    "UPDATE private.memory_records \
                        SET status = 'superseded', superseded_by = $2, superseded_at = now() \
                      WHERE memory_id = $1",
                    &[&memory_a, &memory_b],
                )
                .expect("supersede memory_a");
            let ticket = reissue_lifecycle_ticket(&mut handle, scope_id, memory_a);
            let outcome = handle
                .rt
                .block_on(run_once(&deps, 10))
                .expect("second run_once succeeds");
            assert_eq!(
                (outcome.done, outcome.failed),
                (1, 0),
                "a dead memory's ticket settles DONE instead of wedging the prefix: {outcome:?}"
            );
            let key = (
                handle.tenant_id,
                "workspace",
                scope_id,
                "private_memory",
                "PRIVATE_MEMORY",
                "v1",
            );
            assert_eq!(stream_log_state(&mut handle, &key, ticket), "DONE");
            assert_eq!(stream_log_error_class(&mut handle, &key, ticket), None);
            assert_eq!(
                projection_highwater(&mut handle, &key),
                ticket,
                "the §15.4 prefix advances past the lifecycle ticket"
            );
            let live: bool = handle
                .admin
                .query_one(
                    "SELECT projection_live FROM projection.private_memory_points WHERE point_id = $1",
                    &[&point_a],
                )
                .expect("binding row survives, retired")
                .get(0);
            assert!(!live, "the superseded memory's binding is retired");
            let permit = authorize_cell_access(
                &handle.registry,
                IntraCellResource::QDRANT_REST,
                Duration::from_secs(30),
            )
            .expect("admin Qdrant permit");
            let scrolled = scroll_payloads(&handle, &permit, &[point_a, point_b]);
            let ids = scrolled["result"]["points"]
                .as_array()
                .map(|points| {
                    points
                        .iter()
                        .filter_map(|point| point["id"].as_str().map(str::to_owned))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            assert_eq!(
                ids,
                vec![point_b.to_string()],
                "the dead memory's point left the index, the live sibling stayed: {scrolled}"
            );
            assert_eq!(
                outbox_status(&mut handle, ticket),
                "DONE",
                "ADR-0049 D-C: the ticket's carrier outbox row is settled with the ticket"
            );

            // ADR-0020 restore, as `restore_atomically` writes it: status back, same
            // `updated_at`. The re-issued ticket must bring the SAME point back (identity
            // unchanged → the retired binding is revived, ADR-0049) — the rehearsal's
            // `restored_memory_is_servable_again = 0` shape is this leg failing.
            handle
                .admin
                .execute(
                    "UPDATE private.memory_records \
                        SET status = 'active', superseded_by = NULL, superseded_at = NULL \
                      WHERE memory_id = $1",
                    &[&memory_a],
                )
                .expect("restore memory_a");
            let restore_ticket = reissue_lifecycle_ticket(&mut handle, scope_id, memory_a);
            let outcome = handle
                .rt
                .block_on(run_once(&deps, 10))
                .expect("third run_once succeeds");
            assert_eq!((outcome.done, outcome.failed), (1, 0), "{outcome:?}");
            assert_eq!(stream_log_state(&mut handle, &key, restore_ticket), "DONE");
            assert_eq!(outbox_status(&mut handle, restore_ticket), "DONE");
            assert_eq!(
                point_id_for_memory(&mut handle, memory_a),
                point_a,
                "same identity, same deterministic point id"
            );
            let live: bool = handle
                .admin
                .query_one(
                    "SELECT projection_live FROM projection.private_memory_points WHERE point_id = $1",
                    &[&point_a],
                )
                .expect("binding row")
                .get(0);
            assert!(
                live,
                "the restored memory's binding is live again (revived)"
            );
            let scrolled = scroll_payloads(&handle, &permit, &[point_a, point_b]);
            assert_eq!(
                scrolled["result"]["points"].as_array().map_or(0, Vec::len),
                2,
                "the restored memory's point is back in the index: {scrolled}"
            );
        },
    );
}

/// `ops.outbox.status` of the carrier row behind one ticket of the fixture's `v1` workspace
/// stream (ADR-0049 D-C).
fn outbox_status(handle: &mut Handle, stream_seq: i64) -> String {
    handle
        .admin
        .query_one(
            "SELECT status FROM ops.outbox WHERE tenant_id = $1 AND stream_seq = $2 \
               AND event_type = 'MEMORY_LIFECYCLE'",
            &[&handle.tenant_id, &stream_seq],
        )
        .expect("carrier outbox row")
        .get(0)
}

// ---- ADR-0055 D-B: the `archived` payload flag follows the PG row on every ticket ----

/// The (`archived`, `status`) payload pair of one point, read back through a raw payload scroll.
/// `None` for `archived` = the point carries no flag (never the case after card 30).
fn lifecycle_flags(handle: &Handle, point: Uuid) -> (Option<bool>, String) {
    let permit = authorize_cell_access(
        &handle.registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(30),
    )
    .expect("admin Qdrant permit");
    let scrolled = scroll_payloads(handle, &permit, &[point]);
    let payload = &scrolled["result"]["points"][0]["payload"];
    assert!(
        payload.is_object(),
        "point {point} is in the index: {scrolled}"
    );
    (
        payload["archived"].as_bool(),
        payload["status"].as_str().unwrap_or_default().to_owned(),
    )
}

/// One lifecycle step the way `memory_governance_repo` performs it: the PG write, then the
/// existing MEMORY_LIFECYCLE ticket bound to the memory's PRIMARY Evidence, then one worker pass.
fn lifecycle_step(
    handle: &mut Handle,
    deps: &ProjectionWorkerDeps,
    scope_id: Uuid,
    memory_id: Uuid,
    sql: &str,
) {
    handle
        .admin
        .execute(sql, &[&memory_id])
        .expect("lifecycle PG write");
    reissue_lifecycle_ticket(handle, scope_id, memory_id);
    let outcome = handle.rt.block_on(run_once(deps, 10)).expect("run_once");
    assert_eq!(outcome.failed, 0, "{outcome:?}");
}

const ARCHIVE: &str = "UPDATE private.memory_records SET archived_at = now() WHERE memory_id = $1";
const UNARCHIVE: &str = "UPDATE private.memory_records SET archived_at = NULL WHERE memory_id = $1";

/// Seeds `n` TENANT_SHARED memories in a fresh scope, projects them once, and returns the scope,
/// the worker deps and the memory ids.
fn projected_memories(handle: &mut Handle, n: usize) -> (Uuid, ProjectionWorkerDeps, Vec<Uuid>) {
    let scope_id = Uuid::new_v4();
    let memories: Vec<Uuid> = (0..n)
        .map(|i| {
            seed_memory_with_visibility(
                handle,
                scope_id,
                &format!("card 30 lifecycle fixture memory {i}"),
                "TENANT_SHARED",
                None,
                None,
            )
            .1
        })
        .collect();
    let provider = Arc::new(TestDoubleProvider::new(
        embedding_model(),
        unused_rerank_model(),
    ));
    let deps = handle.rt.block_on(deps_for(handle, scope_id, provider));
    let outcome = handle
        .rt
        .block_on(run_once(&deps, 10))
        .expect("first run_once");
    assert_eq!(
        (outcome.done, outcome.failed),
        (u64::try_from(n).expect("n"), 0),
        "{outcome:?}"
    );
    (scope_id, deps, memories)
}

#[test]
fn archive_ticket_reupserts_the_same_point_with_archived_true() {
    run_db_fixture::<Fixture, _>(
        "archive_ticket_reupserts_the_same_point_with_archived_true",
        |mut handle| {
            let (scope_id, deps, memories) = projected_memories(&mut handle, 1);
            let point = point_id_for_memory(&mut handle, memories[0]);
            assert_eq!(
                lifecycle_flags(&handle, point),
                (Some(false), "active".to_owned())
            );
            lifecycle_step(&mut handle, &deps, scope_id, memories[0], ARCHIVE);
            assert_eq!(
                point_id_for_memory(&mut handle, memories[0]),
                point,
                "same point id"
            );
            assert_eq!(
                lifecycle_flags(&handle, point),
                (Some(true), "active".to_owned())
            );
        },
    );
}

#[test]
fn unarchive_ticket_reupserts_the_same_point_with_archived_false() {
    run_db_fixture::<Fixture, _>(
        "unarchive_ticket_reupserts_the_same_point_with_archived_false",
        |mut handle| {
            let (scope_id, deps, memories) = projected_memories(&mut handle, 1);
            let point = point_id_for_memory(&mut handle, memories[0]);
            lifecycle_step(&mut handle, &deps, scope_id, memories[0], ARCHIVE);
            assert_eq!(lifecycle_flags(&handle, point).0, Some(true));
            lifecycle_step(&mut handle, &deps, scope_id, memories[0], UNARCHIVE);
            assert_eq!(
                point_id_for_memory(&mut handle, memories[0]),
                point,
                "same point id"
            );
            assert_eq!(
                lifecycle_flags(&handle, point),
                (Some(false), "active".to_owned())
            );
        },
    );
}

/// ADR-0055 lifecycle matrix: archive survives supersede → restore. The superseded memory's
/// point is retired (ADR-0049); the restore revives the SAME point and the flag is read from the
/// row again — still archived, because restore never touches `archived_at`.
#[test]
fn restore_of_an_archived_memory_revives_the_point_with_archived_true() {
    run_db_fixture::<Fixture, _>(
        "restore_of_an_archived_memory_revives_the_point_with_archived_true",
        |mut handle| {
            let (scope_id, deps, memories) = projected_memories(&mut handle, 2);
            let (a, b) = (memories[0], memories[1]);
            let point = point_id_for_memory(&mut handle, a);
            lifecycle_step(&mut handle, &deps, scope_id, a, ARCHIVE);
            handle
                .admin
                .execute(
                    "UPDATE private.memory_records \
                        SET status = 'superseded', superseded_by = $2, superseded_at = now() \
                      WHERE memory_id = $1",
                    &[&a, &b],
                )
                .expect("supersede a");
            reissue_lifecycle_ticket(&mut handle, scope_id, a);
            handle
                .rt
                .block_on(run_once(&deps, 10))
                .expect("retire pass");
            let permit = authorize_cell_access(
                &handle.registry,
                IntraCellResource::QDRANT_REST,
                Duration::from_secs(30),
            )
            .expect("admin Qdrant permit");
            assert_eq!(
                scroll_payloads(&handle, &permit, &[point])["result"]["points"]
                    .as_array()
                    .map_or(0, Vec::len),
                0,
                "superseded ⇒ retired point"
            );
            lifecycle_step(
                &mut handle,
                &deps,
                scope_id,
                a,
                "UPDATE private.memory_records \
                    SET status = 'active', superseded_by = NULL, superseded_at = NULL \
                  WHERE memory_id = $1",
            );
            assert_eq!(
                point_id_for_memory(&mut handle, a),
                point,
                "revived, same point id"
            );
            assert_eq!(
                lifecycle_flags(&handle, point),
                (Some(true), "active".to_owned())
            );
        },
    );
}

/// An unrelated re-projection (affect annotate re-issues MEMORY_LIFECYCLE) must carry the
/// archive flag from PG, never reset it to the default.
#[test]
fn annotate_ticket_on_an_archived_memory_keeps_archived_true() {
    run_db_fixture::<Fixture, _>(
        "annotate_ticket_on_an_archived_memory_keeps_archived_true",
        |mut handle| {
            let (scope_id, deps, memories) = projected_memories(&mut handle, 1);
            let point = point_id_for_memory(&mut handle, memories[0]);
            lifecycle_step(&mut handle, &deps, scope_id, memories[0], ARCHIVE);
            lifecycle_step(
                &mut handle,
                &deps,
                scope_id,
                memories[0],
                "INSERT INTO private.memory_affects \
                   (tenant_id, memory_id, affect_kind, label, valence_bp, arousal_bp, intensity_bp, \
                    confidence_bp, evidence_id, observed_at) \
                 SELECT m.tenant_id, m.memory_id, 'EMOTION', 'JOY', 5000, 2000, 6000, 10000, \
                        me.evidence_id, now() \
                 FROM private.memory_records m \
                 JOIN private.memory_evidence me ON me.memory_id = m.memory_id \
                 WHERE m.memory_id = $1 LIMIT 1",
            );
            let permit = authorize_cell_access(
                &handle.registry,
                IntraCellResource::QDRANT_REST,
                Duration::from_secs(30),
            )
            .expect("admin Qdrant permit");
            let scrolled = scroll_payloads(&handle, &permit, &[point]);
            assert_eq!(
                scrolled["result"]["points"][0]["payload"]["affect_kinds"],
                serde_json::json!(["EMOTION"]),
                "the annotate ticket really re-projected: {scrolled}"
            );
            assert_eq!(
                lifecycle_flags(&handle, point),
                (Some(true), "active".to_owned())
            );
        },
    );
}

/// `memory.correct`: M1 (archived, projected) is superseded by a freshly inserted M2 whose own
/// ticket projects it. M2's point is live — `archived=false`, `status="active"` — whatever M1's
/// flags were (the flag is per memory row, never inherited).
#[test]
fn correct_ticket_projects_the_successor_with_archived_false_and_status_active() {
    run_db_fixture::<Fixture, _>(
        "correct_ticket_projects_the_successor_with_archived_false_and_status_active",
        |mut handle| {
            let (scope_id, deps, memories) = projected_memories(&mut handle, 1);
            let m1 = memories[0];
            lifecycle_step(&mut handle, &deps, scope_id, m1, ARCHIVE);
            let (_, m2) = seed_memory_with_visibility(
                &mut handle,
                scope_id,
                "card 30 corrected successor",
                "TENANT_SHARED",
                None,
                None,
            );
            handle
                .admin
                .execute(
                    "UPDATE private.memory_records \
                        SET status = 'superseded', superseded_by = $2, superseded_at = now() \
                      WHERE memory_id = $1",
                    &[&m1, &m2],
                )
                .expect("m2 corrects m1");
            let outcome = handle.rt.block_on(run_once(&deps, 10)).expect("m2 pass");
            assert_eq!(outcome.failed, 0, "{outcome:?}");
            let point = point_id_for_memory(&mut handle, m2);
            assert_eq!(
                lifecycle_flags(&handle, point),
                (Some(false), "active".to_owned())
            );
        },
    );
}
