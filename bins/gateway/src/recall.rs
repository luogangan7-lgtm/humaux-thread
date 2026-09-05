//! Native authenticated semantic recall wiring.
//!
//! This module composes the existing request builder, sealed provider query, same-Cell Qdrant
//! candidate lookup, and the sole PostgreSQL final hydration boundary. It owns no alternate
//! search or body fallback.

use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use humaux_adapters::{
    placement_repo::tenant_placement,
    postgres::RuntimeDbPool,
    qdrant::{
        DenseQuery, DenseQueryVersions, QdrantOperation, RetrievalFamily, ha_profile_for,
        query_dense,
    },
    read_materialize::MaterializedItem,
    retrieve::{
        MaterializedPrivateReadServing, materialize_private_read_serving_about,
        private_read_projection_selector,
    },
};
use humaux_application::{
    retrieval_embedding_port::{
        RetrievalEmbeddingInput, RetrievalEmbeddingOutcome, RetrievalEmbeddingPort,
    },
    retrieve::{RetrievalIntent, prepare_request},
};
use humaux_domain::{
    error::ErrorCode, identity::AuthorizationScope, ids::WorkspaceId, subject::SubjectId,
};
use humaux_infra_cell::{
    CellAccessPermit, IntraCellHttpTransport, IntraCellResource, IntraCellResourceRegistry,
    authorize_cell_access,
};
use humaux_local_secret_scan::LocalSecretScanner;
use humaux_projection::serving::StreamFamily;
use humaux_protocol::{
    mcp::{ToolName, ToolOutput},
    mcp_catalog::CanonicalCatalog,
};
use humaux_retrieval::{
    completeness::{CensusResult, FreshnessClass},
    envelope::{
        CompletenessBlock, CompletenessInputs, CountScope, Envelope, EvidenceBlock, FreshnessBlock,
        KnowledgeBlock, LaneStatus, MandatoryReport, PendingEnvelope, PinnedReport, PipelineBlock,
        ProfileBlock, ProvenanceBlock, ProvenanceValue, build_projection_block,
        envelope_outcome_block,
    },
    planner::{PlannerDecision, QueryClass},
};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::context::ContextBootstrap;

/// Bootstrap-owned components for the one native private-memory dense lane.
///
/// Holds no [`TenantPlacementRow`](humaux_adapters::qdrant::TenantPlacementRow) — an earlier
/// shape baked in exactly one tenant's placement at construction time, which made every other
/// tenant's `recall.search` call assert against it and fail closed. [`search`] resolves a
/// placement per request instead, through [`tenant_placement`], so this runtime carries no
/// single-tenant assumption at all (ADR-0012 gateway wiring card).
///
/// Holds `port: Arc<dyn RetrievalEmbeddingPort>`, not a provider trait object — §4.2:
/// the gateway process must never hold a provider descriptor/credential; the real
/// implementation (`crate::retrieval_embedding_client::GatewayRetrievalEmbeddingClient`) RPCs
/// `humaux-retrieval-worker`, which alone calls the real provider.
pub struct SemanticRecallRuntime {
    scanner: Arc<LocalSecretScanner>,
    port: Arc<dyn RetrievalEmbeddingPort>,
    qdrant: Arc<dyn IntraCellHttpTransport>,
    cell_registry: IntraCellResourceRegistry,
    embedding_version: String,
    dimension: u32,
    /// Bounds [`RetrievalEmbeddingInput::deadline_unix_ms`] — the gateway's own configured
    /// handler timeout (`bins/gateway/src/guard.rs::GuardSettings::handler_timeout`), not a
    /// literal (§78.1).
    handler_timeout: Duration,
}

pub struct SemanticRecallVersions {
    pub embedding_version: String,
    pub dimension: u32,
}

impl SemanticRecallRuntime {
    pub fn new(
        scanner: Arc<LocalSecretScanner>,
        port: Arc<dyn RetrievalEmbeddingPort>,
        qdrant: Arc<dyn IntraCellHttpTransport>,
        cell_registry: IntraCellResourceRegistry,
        versions: SemanticRecallVersions,
        handler_timeout: Duration,
    ) -> Result<Self, ErrorCode> {
        let SemanticRecallVersions {
            embedding_version,
            dimension,
        } = versions;
        if embedding_version.trim().is_empty() || dimension == 0 || handler_timeout.is_zero() {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            scanner,
            port,
            qdrant,
            cell_registry,
            embedding_version,
            dimension,
            handler_timeout,
        })
    }

    fn qdrant_permit(&self) -> Result<CellAccessPermit, ErrorCode> {
        authorize_cell_access(
            &self.cell_registry,
            IntraCellResource::QDRANT_REST,
            Duration::from_secs(30),
        )
        .map_err(|_| ErrorCode::DependencyUnavailable)
    }

    fn deadline_unix_ms(&self) -> Result<i64, ErrorCode> {
        let deadline = SystemTime::now() + self.handler_timeout;
        i64::try_from(
            deadline
                .duration_since(UNIX_EPOCH)
                .map_err(|_| ErrorCode::Internal)?
                .as_millis(),
        )
        .map_err(|_| ErrorCode::Internal)
    }
}

pub struct RecallSearchRequest {
    pub query: String,
    pub workspace_id: WorkspaceId,
    pub consistency_token: Option<String>,
    pub mode: Option<String>,
    pub completeness_request: Option<String>,
    pub limit: Option<u32>,
    /// §6.1.3 / ADR-0029 D-A: any-of subject narrowing (`recall.search.subject_ids`). Empty =
    /// unscoped. Applied twice by design — as a Qdrant payload prefilter
    /// (`DenseQuery::with_subject_ids`) and re-checked at the PG hydrate gate
    /// (`materialize_private_read_serving_about`); the prefilter is never the authority.
    pub subject_ids: Vec<SubjectId>,
}

#[allow(clippy::too_many_lines)] // Keep the one native semantic-recall request/response chain together.
pub async fn search(
    pool: Arc<RuntimeDbPool>,
    runtime: Arc<SemanticRecallRuntime>,
    catalog: Arc<CanonicalCatalog>,
    authorization: AuthorizationScope,
    request_id: Uuid,
    bootstrap: ContextBootstrap,
    input: RecallSearchRequest,
) -> Result<PendingEnvelope<ToolOutput>, ErrorCode> {
    if input.mode.as_deref().is_some_and(|mode| mode != "semantic")
        || input.completeness_request.as_deref() == Some("required")
    {
        return Err(ErrorCode::DependencyUnavailable);
    }
    let authorization = authorization.narrow(input.workspace_id)?;
    if bootstrap.stream.tenant_id != authorization.tenant_id()
        || bootstrap.stream.scope_id != input.workspace_id.0
    {
        return Err(ErrorCode::Forbidden);
    }
    // §17.3 per-tenant placement, resolved fresh every request — `None` means "not indexed
    // yet for this tenant", never a fallback onto another tenant's collection.
    let placement = tenant_placement(
        &pool,
        authorization.tenant_id(),
        RetrievalFamily::PrivateMemoryV1,
    )
    .await
    .map_err(|_| {
        eprintln!("humaux-gateway: recall request_id={request_id} placement_lookup_failed");
        ErrorCode::DependencyUnavailable
    })?
    .ok_or_else(|| {
        eprintln!("humaux-gateway: recall request_id={request_id} placement_missing");
        ErrorCode::DependencyUnavailable
    })?;
    let family = StreamFamily::new(
        bootstrap.stream.tenant_id,
        bootstrap.stream.scope_kind.clone(),
        bootstrap.stream.scope_id,
        bootstrap.stream.domain.clone(),
        bootstrap.stream.projection_kind.clone(),
    );
    let intent = RetrievalIntent::new(input.query, Vec::new(), BTreeSet::new(), BTreeSet::new())
        .map_err(|_| ErrorCode::InvalidInput)?;
    let retrieval = prepare_request(intent, &bootstrap.profile).map_err(|_| ErrorCode::Internal)?;
    if !matches!(
        retrieval.planner_decision(),
        PlannerDecision::Class(QueryClass::Semantic)
    ) || input.limit.is_some_and(|limit| limit != retrieval.top_k())
    {
        return Err(ErrorCode::InvalidInput);
    }
    let trusted_query = retrieval.trusted_query().ok_or(ErrorCode::Internal)?;
    // Defense-in-depth local scan before the raw text crosses the wire to
    // `humaux-retrieval-worker` (which independently scans/seals it worker-side, ADR-0012 §2's
    // "raw query text ... never a sealed query" crossing the boundary).
    let sealed = runtime.scanner.seal_query(&trusted_query)?;
    let embedding_input = RetrievalEmbeddingInput {
        authorization: &authorization,
        workspace_id: input.workspace_id,
        request_id,
        logical_call_id: request_id,
        attempt_no: 1,
        profile_fingerprint: retrieval.profile_fingerprint_identity().as_str(),
        dimension: runtime.dimension,
        query: sealed.as_str(),
        deadline_unix_ms: runtime.deadline_unix_ms()?,
    };
    let (vector, embedding_model_id) = match runtime.port.embed_query(embedding_input).await? {
        RetrievalEmbeddingOutcome::Embedded {
            vector,
            dimension,
            model_id,
            ..
        } if dimension == runtime.dimension && vector.len() == runtime.dimension as usize => {
            (vector, model_id)
        }
        // A wrong-dimension vector is never truncated/padded to fit — ADR-0012 gateway wiring
        // card: "mismatch ⇒ DependencyUnavailable, never truncate".
        RetrievalEmbeddingOutcome::Embedded { .. }
        | RetrievalEmbeddingOutcome::Skipped
        | RetrievalEmbeddingOutcome::Unavailable { .. } => {
            eprintln!("humaux-gateway: recall request_id={request_id} query_embedding_unusable");
            return Err(ErrorCode::DependencyUnavailable);
        }
    };
    let projection_version = private_read_projection_selector(&pool, &authorization, &family)
        .await
        .map_err(|_| {
            eprintln!("humaux-gateway: recall request_id={request_id} projection_selector_failed");
            ErrorCode::DependencyUnavailable
        })?
        .ok_or_else(|| {
            eprintln!("humaux-gateway: recall request_id={request_id} no_serving_projection");
            ErrorCode::DependencyUnavailable
        })?;
    let dense = DenseQuery::new(
        &authorization,
        &placement,
        DenseQueryVersions {
            projection: &projection_version,
            embedding: &runtime.embedding_version,
        },
        vector,
        retrieval.top_k(),
        Vec::new(),
        ha_profile_for(QdrantOperation::ReadYourWriteStrict),
    )
    .map_err(|_| {
        eprintln!("humaux-gateway: recall request_id={request_id} dense_query_build_failed");
        ErrorCode::DependencyUnavailable
    })?
    .with_subject_ids(&input.subject_ids);
    let candidates = query_dense(runtime.qdrant.as_ref(), &runtime.qdrant_permit()?, &dense)
        .await
        .map_err(|error| {
            // ADR-0014: a `WriteDenied` here means the registered `QDRANT_REST` entry itself is
            // misconfigured (a read-only permit rejected a request `query_dense` never should
            // have shaped as a write) — a Forbidden, not a transient dependency failure a
            // caller could usefully retry.
            if matches!(
                error,
                humaux_adapters::qdrant::QdrantTransportError::Transport(
                    humaux_infra_cell::IntraCellError::WriteDenied
                )
            ) {
                ErrorCode::Forbidden
            } else {
                // Operator signal: transport error class only (no body/URL), §ADR-0014.
                eprintln!("humaux-gateway: recall request_id={request_id} qdrant_query_failed");
                ErrorCode::DependencyUnavailable
            }
        })?;
    let materialized = materialize_private_read_serving_about(
        &pool,
        input.consistency_token.as_deref(),
        &authorization,
        &family,
        &projection_version,
        &runtime.embedding_version,
        &candidates,
        &input.subject_ids,
    )
    .await
    .map_err(|error| match error {
        humaux_adapters::retrieve::RetrieveError::CrossTenant
        | humaux_adapters::retrieve::RetrieveError::CrossWorkspace
        | humaux_adapters::retrieve::RetrieveError::UntrustedStreamFamily => ErrorCode::Forbidden,
        humaux_adapters::retrieve::RetrieveError::TokenMalformed(_)
        | humaux_adapters::retrieve::RetrieveError::TokenExpired
        | humaux_adapters::retrieve::RetrieveError::TokenNotIssued => ErrorCode::InvalidInput,
        error => {
            // Operator signal, same discipline as the qdrant_query_failed line above: the
            // RetrieveError class only — `Db` carries driver text and is reduced to its name.
            let class = if matches!(error, humaux_adapters::retrieve::RetrieveError::Db(_)) {
                "db_error".to_owned()
            } else {
                error.to_string()
            };
            eprintln!(
                "humaux-gateway: recall request_id={request_id} materialize_failed class={class}"
            );
            ErrorCode::DependencyUnavailable
        }
    })?;
    accepted_output(
        materialized,
        &retrieval,
        &bootstrap,
        &runtime,
        &projection_version,
        &embedding_model_id,
        candidates.len(),
        &catalog,
    )
}

#[allow(clippy::too_many_arguments)] // One envelope-assembly step over the request's own fixed field set.
fn accepted_output(
    materialized: MaterializedPrivateReadServing,
    request: &humaux_retrieval::request::RetrievalRequest,
    bootstrap: &ContextBootstrap,
    runtime: &SemanticRecallRuntime,
    projection_version: &str,
    embedding_model_id: &str,
    candidate_count: usize,
    catalog: &CanonicalCatalog,
) -> Result<PendingEnvelope<ToolOutput>, ErrorCode> {
    let items = render_items(materialized.bodies.items);
    let returned = u32::try_from(items.len()).map_err(|_| ErrorCode::Internal)?;
    let candidate_count = u32::try_from(candidate_count).map_err(|_| ErrorCode::Internal)?;
    let projection = build_projection_block(&materialized.ledger, None);
    let pipeline = PipelineBlock {
        evidence: EvidenceBlock::no_batch(None, CountScope::AuthorizedView),
        knowledge: KnowledgeBlock {
            eligible: None,
            processed: None,
            waiting_key: None,
            failed: None,
            count_scope: CountScope::AuthorizedView,
        },
        projection: projection.value,
    };
    let provenance = ProvenanceBlock {
        binary_build: bootstrap.binary_build.clone(),
        projection_version: ProvenanceValue::Used {
            id: projection_version.to_owned(),
        },
        embedding_model_id: ProvenanceValue::Used {
            id: format!("{embedding_model_id}@{}", runtime.embedding_version),
        },
        rerank_model_id: ProvenanceValue::NotApplicable {},
        card_builder_version: ProvenanceValue::NotApplicable {},
        profile_fingerprint: request.profile_fingerprint_identity().clone(),
        profile: ProfileBlock {
            top_k: request.top_k(),
            cand_k: request.cand_k(),
            cand_k_formula: request.cand_k_formula(),
            lanes: vec!["dense".to_owned()],
        },
    };
    let lane_status = LaneStatus::Ok;
    let census = CensusResult::ok_without_enumeration();
    envelope_outcome_block(
        request,
        CompletenessInputs {
            lane_status: &lane_status,
            census: &census,
            ledger: &materialized.ledger,
            pipeline: &pipeline,
            provenance: &provenance,
            visible: None,
            context: None,
        },
        |outcome| {
            let value = serde_json::to_value(Envelope {
                items,
                completeness: CompletenessBlock {
                    class: outcome.class,
                    reason: outcome.reason,
                    exact: outcome.exact,
                    known_lower_bound: outcome.known_lower_bound,
                    lanes: std::collections::BTreeMap::from([("dense".to_owned(), lane_status)]),
                    candidate_count,
                    reranked_count: 0,
                    returned,
                    truncated: candidate_count > returned,
                    degradations: projection
                        .degradations
                        .iter()
                        .map(|code| code.line_format())
                        .collect(),
                },
                pipeline: pipeline.clone(),
                provenance: provenance.clone(),
                freshness: FreshnessBlock {
                    class: FreshnessClass::Unknown,
                    latest_evidence_at: None,
                    state_age_seconds: None,
                },
                grounding: materialized.grounding,
                mandatory: MandatoryReport::NotRun,
                pinned: PinnedReport::NotRun,
            })
            .map_err(|_| ErrorCode::Internal)?;
            catalog.validate_output(ToolName::Recall, &value)?;
            let text = serde_json::to_string(&value).map_err(|_| ErrorCode::Internal)?;
            Ok(ToolOutput {
                text,
                structured_content: value,
            })
        },
    )
}

fn render_items(items: Vec<MaterializedItem>) -> Vec<Value> {
    items
        .into_iter()
        .map(|item| match item {
            MaterializedItem::Memory { memory_id, content } => json!({
                "kind": "memory",
                "memory_id": memory_id,
                "content": content,
            }),
            MaterializedItem::TemporaryEvidence {
                evidence_id,
                stream_seq,
                processing_state,
                payload,
                linked_memory_ids,
            } => json!({
                "kind": "temporary_evidence",
                "evidence_id": evidence_id,
                "stream_seq": stream_seq,
                "processing_state": processing_state.as_db_str(),
                "payload": payload,
                "linked_memory_ids": linked_memory_ids,
            }),
            MaterializedItem::ArtifactUnavailable {
                evidence_id,
                stream_seq,
                processing_state,
                linked_memory_ids,
            } => json!({
                "kind": "artifact_unavailable",
                "evidence_id": evidence_id,
                "stream_seq": stream_seq,
                "processing_state": processing_state.as_db_str(),
                "linked_memory_ids": linked_memory_ids,
            }),
        })
        .collect()
}
