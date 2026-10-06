//! `adapters::tests::support::a2_fixture` — the shared ADR-0057 fixture: a throwaway tenant with a governor, a real
//!   Qdrant collection, the real projection worker and both production producers of §23.1② A2.
//! Depends-on: crates=[async-trait, humaux-adapters, humaux-domain, humaux-infra-cell, humaux-local-secret-scan,
//!   humaux-projection, humaux-retrieval, humaux-retrieval-provider, humaux-telemetry, humaux-testkit, postgres,
//!   serde_json, sqlx, time, tokio]; services=[PostgreSQL(owner) w=[control.private_reasoning_domains,
//!   control.tenants, control.workspaces, ops.jobs, ops.outbox, private.events, private.evidence_objects,
//!   private.memory_evidence, private.memory_records, projection.memory_vectors, projection.private_memory_points,
//!   projection.rebuild_runs, projection.rebuild_tickets, projection.stream_checkpoints, projection.stream_log],
//!   PostgreSQL(role_gateway), PostgreSQL(role_maintenance),
//!   PostgreSQL(role_retrieval_worker), Qdrant(*)]; env=[HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_RETRIEVAL_WORKER_PG_DSN,
//!   HUMAUX_TEST_GITLEAKS_BIN, HUMAUX_TEST_GITLEAKS_SHA256, HUMAUX_TEST_GITLEAKS_VERSION, HUMAUX_TEST_PG_DSN,
//!   HUMAUX_TEST_QDRANT_URL]; modules=[adapters::postgres, adapters::projection_worker, adapters::qdrant,
//!   adapters::remember, adapters::retrieve, adapters::stream_repo, adapters::tests::support::governance_ops,
//!   domain::egress, domain::error, domain::evidence, domain::identity, domain::ids, domain::subject,
//!   humaux-local-secret-scan, humaux-testkit, infra-cell::permit, infra-cell::resource, infra-cell::transport,
//!   projection::serving, retrieval-provider::adapters, retrieval-provider::contract, retrieval::completeness,
//!   retrieval::envelope, telemetry::degrade]
//! Called-by: [adapters::tests::a2_point_identity, adapters::tests::projection_lag, adapters::tests::rebuild,
//!   adapters::tests::switch_user_private, maintenance::tests::drill, maintenance::tests::measure]
//! Invariants: [test-only, included by #[path]; both sides of A2 are the production producers
//!   (stream_repo::fetch_ledger_closure through the 0189 definer, retrieve::visible_count_of_version); throwaway
//!   tenant + collection cleaned up on Drop; a missing PG / Qdrant / gitleaks is a fixture error, never a silent pass]
//! Spec: Baseline §23.1②; §79.2; ADR-0057

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use humaux_adapters::postgres::{MaintenanceDbPool, RetrievalWorkerDbPool, RuntimeDbPool};
use humaux_adapters::projection_worker::{CardEmbedder, ProjectionWorkerDeps, run_once};
use humaux_adapters::qdrant::{
    Distance, PlacementClass, PromotionState, RetrievalFamily, ShardingMethod, TenantPlacementRow,
    create_collection_body, subject_index_body, tenant_index_body,
};
use humaux_adapters::remember::{self, RememberCommand};
use humaux_adapters::retrieve::{IndexFace, visible_count_of_version};
use humaux_adapters::stream_repo;
use humaux_domain::egress::ProcessorId;
use humaux_domain::error::ErrorCode;
use humaux_domain::evidence::{EvidenceOriginClass, payload_sha256};
use humaux_domain::identity::AuthorizationScope;
use humaux_domain::ids::{TenantId, WorkspaceId};
use humaux_infra_cell::{
    CallerId, CellAccessPermit, CellId, HttpIntraCellTransport, IntraCellError,
    IntraCellHttpTransport, IntraCellMethod, IntraCellRequest, IntraCellResource,
    IntraCellResourceRegistry, IntraCellResponse, ResourceEntry, authorize_cell_access,
};
use humaux_local_secret_scan::{LocalSecretScanner, LocalSecretScannerConfig, SealedRetrievalCard};
use humaux_projection::serving::StreamFamily;
use humaux_retrieval::completeness::LedgerClosure;
use humaux_retrieval::envelope::{ProjectionBlock, build_projection_block};
use humaux_retrieval_provider::adapters::TestDoubleProvider;
use humaux_retrieval_provider::contract::{
    CalibrationProfileId, EmbeddingModelDescriptor, EmbeddingProvider, ModelId,
    RerankModelDescriptor, RerankScoreSemantics,
};
use humaux_telemetry::degrade::Outcome;
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;

use super::governance_ops::{Governor, stream};

const TEST_PROCESSOR_ID: Uuid = Uuid::from_u128(0x0057_0001);

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

fn setup<T>(what: &str) -> impl FnOnce(T) -> DbFixtureSkipReason + '_
where
    T: std::fmt::Display,
{
    move |e| DbFixtureSkipReason::IsolationSetupFailed(format!("{what}: {e}"))
}

fn qdrant_port() -> u16 {
    std::env::var("HUMAUX_TEST_QDRANT_URL")
        .ok()
        .and_then(|url| url.rsplit(':').next().map(str::to_owned))
        .and_then(|port| port.parse().ok())
        .unwrap_or(6333)
}

pub struct Handle {
    pub rt: tokio::runtime::Runtime,
    pub admin: Client,
    pub gateway: RuntimeDbPool,
    pub retrieval: RetrievalWorkerDbPool,
    pub maintenance: MaintenanceDbPool,
    pub transport: Arc<HttpIntraCellTransport>,
    pub registry: IntraCellResourceRegistry,
    pub collection: String,
    pub tenant_id: Uuid,
    pub reasoning_domain_id: Uuid,
    pub scanner: Arc<LocalSecretScanner>,
    pub gov: Governor,
    /// The owner DSN every pool of this handle derives from.
    pub dsn: String,
    retrieval_dsn: String,
    /// The throwaway database this handle lives in (dropped last, after every pool), if any.
    _db: Option<Box<dyn std::any::Any>>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        Governor::cleanup(&mut self.admin, self.tenant_id);
        // Jobs go first, in one batch with the rows that produced them: the `ops.outbox` trigger
        // enqueues a DERIVED_DISTILL job per seeded Evidence, and a claimable job of a tenant with
        // no route binding is released NotReady forever, never DEAD (private-worker distill.rs) —
        // left behind, it is claimed ahead of every later tenant on the shared dev DB (card 31
        // rehearsal: 258 leaked jobs, memory_records=0). A failure is printed, not swallowed; gate
        // `no_leaked_distill_jobs` is the check.
        if let Err(error) = self.admin.batch_execute(&format!(
            "DELETE FROM ops.jobs WHERE tenant_id = '{0}'; \
             DELETE FROM projection.private_memory_points WHERE tenant_id = '{0}'; \
             DELETE FROM projection.memory_vectors WHERE tenant_id = '{0}'; \
             DELETE FROM projection.rebuild_tickets WHERE tenant_id = '{0}'; \
             DELETE FROM projection.rebuild_runs WHERE tenant_id = '{0}'; \
             DELETE FROM ops.outbox WHERE tenant_id = '{0}'; \
             DELETE FROM projection.stream_log WHERE tenant_id = '{0}'; \
             DELETE FROM projection.stream_checkpoints WHERE tenant_id = '{0}'; \
             DELETE FROM private.memory_evidence USING private.memory_records m \
               WHERE memory_evidence.memory_id = m.memory_id AND m.tenant_id = '{0}'; \
             DELETE FROM private.memory_records WHERE tenant_id = '{0}'; \
             DELETE FROM private.events USING private.evidence_objects eo \
               WHERE events.event_id = eo.evidence_id AND eo.tenant_id = '{0}'; \
             DELETE FROM private.evidence_objects WHERE tenant_id = '{0}'; \
             DELETE FROM control.private_reasoning_domains WHERE tenant_id = '{0}';",
            self.tenant_id
        )) {
            eprintln!(
                "a2_fixture cleanup failed for tenant {}: {error}",
                self.tenant_id
            );
        }
        // Best effort and kept apart: `control.audit_events` is append-only and references the
        // tenant, so a fixture that ran a governance op cannot delete its tenant row. In the same
        // batch as the deletes above, this refusal rolled the whole cleanup back.
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM control.workspaces WHERE tenant_id = '{0}'; \
             DELETE FROM control.tenants WHERE tenant_id = '{0}';",
            self.tenant_id
        ));
        if let Ok(permit) = authorize_cell_access(
            &self.registry,
            IntraCellResource::QDRANT_REST,
            Duration::from_secs(30),
        ) {
            let transport = self.transport.clone();
            let path = format!("/collections/{}", self.collection);
            let _ = self.rt.block_on(async move {
                // dep: Qdrant(*) — drop the throwaway collection
                transport
                    .execute(
                        &permit,
                        IntraCellRequest {
                            method: IntraCellMethod::Delete,
                            path,
                            json_body: None,
                            headers: Vec::new(),
                        },
                    )
                    .await
            });
        }
    }
}

pub struct Fixture;

impl DbIntegrationFixture for Fixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let retrieval = env("HUMAUX_RETRIEVAL_WORKER_PG_DSN")?;
        let maintenance = env("HUMAUX_MAINTENANCE_PG_DSN")?;
        Handle::open(dsn, retrieval, maintenance, None, qdrant_port())
    }
}

fn env(name: &str) -> Result<String, DbFixtureSkipReason> {
    std::env::var(name)
        .map_err(|_| DbFixtureSkipReason::IsolationSetupFailed(format!("missing object: {name}")))
}

impl Handle {
    /// The fixture in its own throwaway database `db` (owner DSN `dsn`): every runtime pool connects to it as its
    /// role, so ticket writes the shared dev database must never see (reissues, ADR-0062 E8) stay there. `db` is
    /// dropped after every pool.
    pub fn in_throwaway(
        dsn: String,
        db: Box<dyn std::any::Any>,
    ) -> Result<Self, DbFixtureSkipReason> {
        Self::in_throwaway_at(dsn, db, qdrant_port())
    }

    /// [`Self::in_throwaway`] whose collection lives in the Qdrant on loopback `qdrant_port` (a test-owned
    /// scratch container, ADR-0064 S3) instead of `HUMAUX_TEST_QDRANT_URL`'s.
    pub fn in_throwaway_at(
        dsn: String,
        db: Box<dyn std::any::Any>,
        qdrant_port: u16,
    ) -> Result<Self, DbFixtureSkipReason> {
        let retrieval = dsn_as_role(&dsn, "role_retrieval_worker");
        let maintenance = dsn_as_role(&dsn, "role_maintenance");
        Self::open(dsn, retrieval, maintenance, Some(db), qdrant_port)
    }

    fn open(
        dsn: String,
        retrieval_dsn: String,
        maintenance_dsn: String,
        db: Option<Box<dyn std::any::Any>>,
        qdrant_port: u16,
    ) -> Result<Self, DbFixtureSkipReason> {
        // dep: PostgreSQL(any) — the owner connection that seeds and cleans the fixture tenant
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
        let scanner = LocalSecretScanner::new(LocalSecretScannerConfig {
            executable: env("HUMAUX_TEST_GITLEAKS_BIN")?.into(),
            expected_version: env("HUMAUX_TEST_GITLEAKS_VERSION")?,
            expected_executable_sha256: env("HUMAUX_TEST_GITLEAKS_SHA256")?,
            timeout: Duration::from_secs(5),
            max_payload_bytes: 64 * 1024,
            finding_exit_code: 1,
        })
        .map_err(setup("gitleaks scanner"))?;
        let tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"a2_point_identity.rs throwaway tenant"],
            )
            .map_err(setup("tenant"))?
            .get(0);
        let reasoning_domain_id: Uuid = admin
            .query_one(
                "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
                 VALUES ($1, 'throwaway') RETURNING reasoning_domain_id",
                &[&tenant_id],
            )
            .map_err(setup("reasoning domain"))?
            .get(0);
        let gov = Governor::seed(&mut admin, tenant_id);
        let rt = tokio::runtime::Runtime::new().map_err(setup("runtime"))?;
        // dep: PostgreSQL(role_gateway) — governance ops and remember
        let gateway = rt
            .block_on(RuntimeDbPool::connect(&dsn_as_role(&dsn, "role_gateway")))
            .map_err(setup("role_gateway pool"))?;
        // dep: PostgreSQL(role_retrieval_worker) — the worker and the ledger closure
        let retrieval = rt
            .block_on(RetrievalWorkerDbPool::connect(&retrieval_dsn))
            .map_err(setup("role_retrieval_worker pool"))?;
        // dep: PostgreSQL(role_maintenance) — the audited FAILED retirement and the reissue door
        let maintenance = rt
            .block_on(MaintenanceDbPool::connect(&maintenance_dsn))
            .map_err(setup("role_maintenance pool"))?;
        let (registry, transport, collection) =
            rt.block_on(setup_qdrant_collection(qdrant_port))?;
        Ok(Handle {
            rt,
            admin,
            gateway,
            retrieval,
            maintenance,
            transport,
            registry,
            collection,
            tenant_id,
            reasoning_domain_id,
            scanner: Arc::new(scanner),
            gov,
            dsn,
            retrieval_dsn,
            _db: db,
        })
    }
}

async fn setup_qdrant_collection(
    port: u16,
) -> Result<
    (
        IntraCellResourceRegistry,
        Arc<HttpIntraCellTransport>,
        String,
    ),
    DbFixtureSkipReason,
> {
    let cell_id = CellId(Uuid::new_v4());
    let caller = CallerId("a2_point_identity.rs test".to_owned());
    let mut entries = BTreeMap::new();
    entries.insert(
        IntraCellResource::QDRANT_REST,
        ResourceEntry::new(
            "127.0.0.1",
            port,
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
    let collection = format!("test_a2_points_{}", Uuid::new_v4().simple());
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
        // dep: Qdrant(*) — collection setup for this fixture
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

fn provider() -> Arc<TestDoubleProvider> {
    Arc::new(TestDoubleProvider::new(
        EmbeddingModelDescriptor {
            model_id: ModelId("a2-point-identity-embedding".to_owned()),
            model_revision: "embed-v1".to_owned(),
            dimension_options: vec![4],
            max_input_tokens: 4_096,
            batch_supported: true,
            dense_supported: true,
            sparse_supported: false,
        },
        RerankModelDescriptor {
            model_id: ModelId("a2-point-identity-unused-reranker".to_owned()),
            model_revision: "unused-v1".to_owned(),
            max_documents: 5,
            max_input_tokens: 4_096,
            score_semantics: RerankScoreSemantics::RawLogit,
            calibration_profile: CalibrationProfileId("unused-v1".to_owned()),
        },
    ))
}

/// Forwards to the real transport; answers `400` to every point upsert after the first
/// `allowed_upserts`, and to every point delete when `reject_deletes` — a permanent Qdrant
/// refusal in the middle of a ticket (ADR-0052 D-E: 400 is not retried).
struct Refusing {
    inner: Arc<HttpIntraCellTransport>,
    allowed_upserts: usize,
    upserts: AtomicUsize,
    reject_deletes: bool,
}

#[async_trait]
impl IntraCellHttpTransport for Refusing {
    async fn execute(
        &self,
        permit: &CellAccessPermit,
        request: IntraCellRequest,
    ) -> Result<IntraCellResponse, IntraCellError> {
        let upsert = matches!(request.method, IntraCellMethod::Put)
            && request.path.contains("/points")
            && !request.path.contains("/points/");
        let delete = request.path.contains("/points/delete");
        let refused = (upsert
            && self.upserts.fetch_add(1, Ordering::SeqCst) >= self.allowed_upserts)
            || (delete && self.reject_deletes);
        if refused {
            return Ok(IntraCellResponse {
                status: 400,
                json_body: Some(serde_json::json!({ "status": { "error": "refused by fixture" } })),
            });
        }
        // dep: Qdrant(*) — every other call reaches the real collection
        self.inner.execute(permit, request).await
    }
}

impl Handle {
    pub fn tenant(&self) -> TenantId {
        TenantId(self.tenant_id)
    }

    /// A LEGACY workspace (no PROVISIONING write gate) the governor is a member of.
    pub fn workspace(&mut self) -> Uuid {
        let workspace: Uuid = self
            .admin
            .query_one(
                "INSERT INTO control.workspaces (tenant_id, name) VALUES ($1, $2) RETURNING workspace_id",
                &[&self.tenant_id, &format!("a2 {}", Uuid::new_v4().simple())],
            )
            .expect("seed workspace")
            .get(0);
        self.gov.join(&mut self.admin, workspace);
        workspace
    }

    pub fn scope(&self, workspace: Uuid) -> AuthorizationScope {
        self.gov.scope(workspace)
    }

    /// One TENANT_SHARED Evidence remembered on `workspace`'s stream (one EVIDENCE_ACCEPTED ticket).
    pub fn evidence(&mut self, workspace: Uuid, content: &str) -> Uuid {
        self.evidence_at(workspace, content, "v1")
    }

    /// [`Self::evidence`] on the `version` stream of the family.
    pub fn evidence_at(&mut self, workspace: Uuid, content: &str, version: &str) -> Uuid {
        let cmd = RememberCommand {
            tenant_id: self.tenant_id,
            authorization_user_id: Some(self.gov.user_id),
            scope_kind: "workspace".to_owned(),
            scope_id: workspace,
            domain: "private_memory".to_owned(),
            projection_kind: "PRIVATE_MEMORY".to_owned(),
            projection_version: version.to_owned(),
            consistency_token_expires_at: time::OffsetDateTime::now_utc()
                + Duration::from_secs(3600),
            batch_id: None,
            payload_sha256: payload_sha256(content.as_bytes()),
            data_class: "INTERNAL".to_owned(),
            origin_class: EvidenceOriginClass::DirectUserInput,
            origin_principal_id: None,
            origin_connector_id: None,
            visibility_class: "TENANT_SHARED".to_owned(),
            visibility_user_id: None,
            visibility_workspace_id: None,
            reasoning_domain_id: self.reasoning_domain_id,
            occurred_at: None,
            event_kind: "MANUAL_NOTE".to_owned(),
            event_payload: serde_json::json!({ "content": content }),
            subjects: humaux_domain::subject::SubjectDeclaration::default(),
            affects: Vec::new(),
            mood_half_life: None,
        };
        self.rt
            .block_on(remember::remember(&self.gateway, cmd))
            .expect("remember writes evidence + ticket")
            .evidence_id
    }

    /// One memory of `evidence` (its PRIMARY), with its own visibility.
    pub fn memory(
        &mut self,
        evidence: Uuid,
        content: &str,
        visibility: (&str, Option<Uuid>, Option<Uuid>),
    ) -> Uuid {
        let body = serde_json::json!({
            "title": format!("title: {content}"),
            "key_claim": format!("key claim: {content}"),
            "evidence_excerpt": content,
        });
        // §8.6: the memory and its evidence link commit together (deferred orphan check).
        let mut txn = self.admin.transaction().expect("owner txn");
        let memory: Uuid = txn
            .query_one(
                "INSERT INTO private.memory_records \
                   (tenant_id, memory_type, content, visibility_class, visibility_user_id, \
                    visibility_workspace_id, authority_class, confidence, status, asserted_at) \
                 VALUES ($1,'NOTE',$2,$3,$4,$5,'PrivateKnowledge',0.9,'active',now()) \
                 RETURNING memory_id",
                &[
                    &self.tenant_id,
                    &body,
                    &visibility.0,
                    &visibility.1,
                    &visibility.2,
                ],
            )
            .expect("insert memory")
            .get(0);
        txn.execute(
            "INSERT INTO private.memory_evidence (memory_id, evidence_id, role, ordinal) \
             VALUES ($1, $2, 'PRIMARY', 0)",
            &[&memory, &evidence],
        )
        .expect("link memory to its evidence");
        txn.commit().expect("commit memory + link");
        memory
    }

    /// `n` TENANT_SHARED memories of one new Evidence on `workspace` (a 1 → n fan-out).
    pub fn fan_out(&mut self, workspace: Uuid, label: &str, n: usize) -> Vec<Uuid> {
        let evidence = self.evidence(workspace, label);
        (0..n)
            .map(|i| self.memory(evidence, &format!("{label} {i}"), TENANT_SHARED))
            .collect()
    }

    pub fn deps(
        &self,
        workspace: Uuid,
        transport: Arc<dyn IntraCellHttpTransport>,
    ) -> ProjectionWorkerDeps {
        self.deps_at(workspace, transport, "v1")
    }

    /// [`Self::deps`] for the `version` stream of the family.
    pub fn deps_at(
        &self,
        workspace: Uuid,
        transport: Arc<dyn IntraCellHttpTransport>,
        version: &str,
    ) -> ProjectionWorkerDeps {
        // dep: PostgreSQL(role_retrieval_worker) — the worker's own pool for one drain
        let pool = self
            .rt
            .block_on(RetrievalWorkerDbPool::connect(&self.retrieval_dsn))
            .expect("role_retrieval_worker connects");
        ProjectionWorkerDeps {
            pool,
            embedder: Arc::new(TestEmbedder(provider())),
            scanner: self.scanner.clone(),
            transport,
            permit: self.permit(),
            placement: TenantPlacementRow {
                tenant_id: self.tenant(),
                projection_family: RetrievalFamily::PrivateMemoryV1,
                collection_name: self.collection.clone(),
                shard_key: None,
                placement_class: PlacementClass::SharedFallback,
                point_count: 0,
                bytes_estimate: 0,
                promotion_state: PromotionState::Stable,
            },
            family: StreamFamily::new(
                self.tenant(),
                "workspace",
                workspace,
                "private_memory",
                "PRIVATE_MEMORY",
            ),
            embedding_version: "embed-v1".to_owned(),
            projection_version: version.to_owned(),
            dimension: 4,
            processor_id: ProcessorId(TEST_PROCESSOR_ID),
        }
    }

    pub fn permit(&self) -> CellAccessPermit {
        authorize_cell_access(
            &self.registry,
            IntraCellResource::QDRANT_REST,
            Duration::from_secs(300),
        )
        .expect("Qdrant permit")
    }

    /// Runs the real worker on `workspace`'s stream until nothing is ISSUED; no ticket may fail.
    pub fn drain(&self, workspace: Uuid) {
        self.drain_at(workspace, "v1");
    }

    /// [`Self::drain`] on the `version` stream of the family.
    pub fn drain_at(&self, workspace: Uuid, version: &str) {
        let deps = self.deps_at(workspace, self.transport.clone(), version);
        loop {
            let outcome = self.rt.block_on(run_once(&deps, 50)).expect("run_once");
            assert_eq!(outcome.failed, 0, "{outcome:?}");
            if outcome.done + outcome.skipped_by_policy + outcome.retried == 0 {
                break;
            }
        }
    }

    /// One worker pass over `workspace` through a refusing transport.
    pub fn run_refused(
        &self,
        workspace: Uuid,
        allowed_upserts: usize,
        reject_deletes: bool,
    ) -> u64 {
        let deps = self.deps(
            workspace,
            Arc::new(Refusing {
                inner: self.transport.clone(),
                allowed_upserts,
                upserts: AtomicUsize::new(0),
                reject_deletes,
            }),
        );
        self.rt
            .block_on(run_once(&deps, 50))
            .expect("run_once")
            .failed
    }

    pub fn retire(&self, workspace: Uuid, class: &str) {
        let retired = self
            .rt
            .block_on(stream_repo::retire_failed(
                &self.maintenance,
                &stream(self.tenant_id, workspace),
                class,
            ))
            .expect("audited retirement");
        assert_eq!(retired.len(), 1, "exactly the refused ticket retires");
    }

    /// Both production sides of A2 for `reader` on `workspace`'s stream, judged by the one block
    /// builder. Prints `visible L F Q U done`.
    pub fn reading(
        &self,
        label: &str,
        workspace: Uuid,
        reader: &AuthorizationScope,
    ) -> Outcome<ProjectionBlock> {
        let key = stream(self.tenant_id, workspace);
        let ledger: LedgerClosure = self
            .rt
            .block_on(stream_repo::fetch_ledger_closure(
                &self.retrieval,
                &key,
                reader,
            ))
            .expect("ledger closure");
        let tombstoned: Vec<i64> = Vec::new();
        let permit = self.permit();
        let visible = self
            .rt
            .block_on(visible_count_of_version(
                &IndexFace {
                    transport: self.transport.as_ref(),
                    permit: &permit,
                    collection: &self.collection,
                },
                reader,
                WorkspaceId(workspace),
                "v1",
                &tombstoned,
            ))
            .expect("Qdrant count");
        let block =
            build_projection_block(&ledger, Some(visible), std::time::Duration::from_secs(60));
        let b = &block.value;
        println!(
            "a2 {label}: visible={visible} L={} F={} Q={} U={} done={} degradations={:?}",
            b.points_settled,
            b.points_in_flight,
            b.points_unsettled,
            b.points_expected,
            b.done,
            block.degradations
        );
        block
    }

    /// A2 closed on a settled stream: `current`, no degradation, `visible == points_settled + q`.
    pub fn assert_closed(
        &self,
        label: &str,
        workspace: Uuid,
        reader: &AuthorizationScope,
    ) -> ProjectionBlock {
        let block = self.reading(label, workspace, reader);
        assert!(
            block.degradations.is_empty(),
            "{label}: {:?}",
            block.degradations
        );
        assert!(block.value.current, "{label}: {:?}", block.value);
        assert_eq!(block.value.points_in_flight, 0, "{label}: settled stream");
        block.value
    }

    /// Exact Qdrant count over every point id ever registered for `memory`.
    pub fn points_of(&self, memory: Uuid) -> u64 {
        let ids: Vec<String> = self
            .admin_query_ids(memory)
            .into_iter()
            .map(|id| id.to_string())
            .collect();
        if ids.is_empty() {
            return 0;
        }
        let permit = self.permit();
        // dep: Qdrant(*) — exact count of one memory's registered points
        self.rt
            .block_on(self.transport.execute(
                &permit,
                IntraCellRequest {
                    method: IntraCellMethod::Post,
                    path: format!("/collections/{}/points/count", self.collection),
                    json_body: Some(serde_json::json!({
                        "filter": { "must": [{ "has_id": ids }] },
                        "exact": true
                    })),
                    headers: Vec::new(),
                },
            ))
            .expect("count")
            .json_body
            .and_then(|b| b["result"]["count"].as_u64())
            .expect("result.count")
    }

    pub fn admin_query_ids(&self, memory: Uuid) -> Vec<Uuid> {
        // A fresh owner connection: `points_of` borrows `self` immutably.
        // dep: PostgreSQL(any) — read the registry rows of one memory
        let mut admin = Client::connect(&self.dsn, NoTls).expect("owner connects");
        admin
            .query(
                "SELECT point_id FROM projection.private_memory_points WHERE memory_id = $1",
                &[&memory],
            )
            .expect("registry rows")
            .iter()
            .map(|r| r.get(0))
            .collect()
    }

    pub fn registry_live(&mut self, memory: Uuid) -> bool {
        self.admin
            .query_one(
                "SELECT COALESCE(bool_or(projection_live), false) \
                 FROM projection.private_memory_points WHERE memory_id = $1",
                &[&memory],
            )
            .expect("registry liveness")
            .get(0)
    }

    pub fn delete_point_of(&self, memory: Uuid) {
        let ids: Vec<String> = self
            .admin_query_ids(memory)
            .into_iter()
            .map(|id| id.to_string())
            .collect();
        let permit = self.permit();
        // dep: Qdrant(*) — G23-2 injection 1: a point removed behind the ledger's back
        self.rt
            .block_on(self.transport.execute(
                &permit,
                IntraCellRequest {
                    method: IntraCellMethod::Post,
                    path: format!("/collections/{}/points/delete?wait=true", self.collection),
                    json_body: Some(serde_json::json!({ "points": ids })),
                    headers: Vec::new(),
                },
            ))
            .expect("delete point");
    }
}

pub const TENANT_SHARED: (&str, Option<Uuid>, Option<Uuid>) = ("TENANT_SHARED", None, None);
