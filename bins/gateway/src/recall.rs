//! Native authenticated semantic recall wiring.
//!
//! This module composes the existing request builder, sealed provider query, same-Cell Qdrant
//! candidate lookup, and the sole PostgreSQL final hydration boundary. It owns no alternate
//! search or body fallback.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use humaux_adapters::{
    postgres::RuntimeDbPool,
    qdrant::{
        DenseQuery, DenseQueryVersions, QdrantOperation, TenantPlacementRow, ha_profile_for,
        query_dense,
    },
    read_materialize::MaterializedItem,
    retrieve::{
        MaterializedPrivateReadServing, materialize_private_read_serving,
        private_read_projection_selector,
    },
};
use humaux_application::retrieve::{RetrievalIntent, prepare_request};
use humaux_domain::{error::ErrorCode, identity::AuthorizationScope, ids::WorkspaceId};
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
use humaux_retrieval_provider::contract::{EmbeddingProvider, RetrievalQueryCallContext};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::context::ContextBootstrap;

/// Bootstrap-owned components for the one native private-memory dense lane.
pub struct SemanticRecallRuntime {
    scanner: Arc<LocalSecretScanner>,
    embedder: Arc<dyn EmbeddingProvider>,
    qdrant: Arc<dyn IntraCellHttpTransport>,
    cell_registry: IntraCellResourceRegistry,
    placement: TenantPlacementRow,
    embedding_version: String,
    dimension: u32,
}

pub struct SemanticRecallVersions {
    pub embedding_version: String,
    pub dimension: u32,
}

impl SemanticRecallRuntime {
    pub fn new(
        scanner: Arc<LocalSecretScanner>,
        embedder: Arc<dyn EmbeddingProvider>,
        qdrant: Arc<dyn IntraCellHttpTransport>,
        cell_registry: IntraCellResourceRegistry,
        placement: TenantPlacementRow,
        versions: SemanticRecallVersions,
    ) -> Result<Self, ErrorCode> {
        let SemanticRecallVersions {
            embedding_version,
            dimension,
        } = versions;
        if placement.projection_family != humaux_adapters::qdrant::RetrievalFamily::PrivateMemoryV1
            || embedding_version.trim().is_empty()
            || !embedder.model().dense_supported
            || !embedder.model().dimension_options.contains(&dimension)
        {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            scanner,
            embedder,
            qdrant,
            cell_registry,
            placement,
            embedding_version,
            dimension,
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
}

pub struct RecallSearchRequest {
    pub query: String,
    pub workspace_id: WorkspaceId,
    pub consistency_token: Option<String>,
    pub mode: Option<String>,
    pub completeness_request: Option<String>,
    pub limit: Option<u32>,
}

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
        || runtime.placement.tenant_id != authorization.tenant_id()
    {
        return Err(ErrorCode::Forbidden);
    }
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
    let sealed = runtime.scanner.seal_query(&trusted_query)?;
    let call_context = RetrievalQueryCallContext::new(
        &authorization,
        input.workspace_id,
        request_id,
        request_id,
        1,
    )
    .map_err(|_| ErrorCode::Forbidden)?;
    let embeddings = runtime
        .embedder
        .embed_queries(&call_context, runtime.dimension, &[sealed])
        .await?;
    let [vector] = embeddings.vectors.as_slice() else {
        return Err(ErrorCode::DependencyUnavailable);
    };
    let projection_version = private_read_projection_selector(&pool, &authorization, &family)
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?
        .ok_or(ErrorCode::DependencyUnavailable)?;
    let dense = DenseQuery::new(
        &authorization,
        &runtime.placement,
        DenseQueryVersions {
            projection: &projection_version,
            embedding: &runtime.embedding_version,
        },
        vector.clone(),
        retrieval.top_k(),
        Vec::new(),
        ha_profile_for(QdrantOperation::ReadYourWriteStrict),
    )
    .map_err(|_| ErrorCode::DependencyUnavailable)?;
    let candidates = query_dense(runtime.qdrant.as_ref(), &runtime.qdrant_permit()?, &dense)
        .await
        .map_err(|_| ErrorCode::DependencyUnavailable)?;
    let materialized = materialize_private_read_serving(
        &pool,
        input.consistency_token.as_deref(),
        &authorization,
        &family,
        &projection_version,
        &runtime.embedding_version,
        &candidates,
    )
    .await
    .map_err(|error| match error {
        humaux_adapters::retrieve::RetrieveError::CrossTenant
        | humaux_adapters::retrieve::RetrieveError::CrossWorkspace
        | humaux_adapters::retrieve::RetrieveError::UntrustedStreamFamily => ErrorCode::Forbidden,
        humaux_adapters::retrieve::RetrieveError::TokenMalformed(_)
        | humaux_adapters::retrieve::RetrieveError::TokenExpired
        | humaux_adapters::retrieve::RetrieveError::TokenNotIssued => ErrorCode::InvalidInput,
        _ => ErrorCode::DependencyUnavailable,
    })?;
    accepted_output(
        materialized,
        &retrieval,
        &bootstrap,
        &runtime,
        &projection_version,
        candidates.len(),
        &catalog,
    )
}

fn accepted_output(
    materialized: MaterializedPrivateReadServing,
    request: &humaux_retrieval::request::RetrievalRequest,
    bootstrap: &ContextBootstrap,
    runtime: &SemanticRecallRuntime,
    projection_version: &str,
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
            id: format!(
                "{}@{}",
                runtime.embedder.model().model_id.0,
                runtime.embedding_version
            ),
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
