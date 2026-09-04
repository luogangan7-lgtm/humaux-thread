//! Authenticated Memory reads through the existing final-body and Envelope boundaries.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use humaux_adapters::{
    context_repo::{
        self, BindingWriteOutcome, BindingWriteRequest, MaterializedMemory,
        MemoryEnumerationParams, materialize_memory_enumeration, materialize_memory_get,
    },
    memory_governance_repo::{self, SupersedeOutcome, SupersedeRequest},
    postgres::RuntimeDbPool,
    read_materialize::MaterializedItem,
};
use humaux_domain::{
    authority::MemoryId,
    confirm::DestructiveOp,
    error::ErrorCode,
    identity::AuthorizationScope,
    ids::{Scope, WorkspaceId},
};
use humaux_projection::{serving::StreamFamily, stream::StreamKey};
use humaux_retrieval::{
    completeness::{CensusResult, FreshnessClass},
    envelope::{
        CompletenessBlock, CompletenessInputs, CountScope, Envelope, EvidenceBlock, FreshnessBlock,
        KnowledgeBlock, LaneStatus, MandatoryReport, PendingEnvelope, PinnedReport, PipelineBlock,
        build_projection_block, envelope_outcome_block,
    },
    request::{RetrievalIntent, RetrievalRequest, build_request},
};
use serde::Serialize;
use uuid::Uuid;

use crate::{
    context::{ContextBootstrap, ContextItem, provenance},
    guard::ConfirmedWrite,
};

/// Pagination metadata deliberately omits snapshot totals and skipped/lost-access counts.
#[derive(Serialize)]
pub(crate) struct Pagination {
    snapshot_id: Uuid,
    next_cursor: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct EnumerationResult {
    content: Envelope<ContextItem>,
    pagination: Pagination,
}

pub(crate) async fn get<T>(
    pool: Arc<RuntimeDbPool>,
    authorization: AuthorizationScope,
    requested_workspace: Option<WorkspaceId>,
    bootstrap: ContextBootstrap,
    memory_id: MemoryId,
    accept: impl FnOnce(Envelope<ContextItem>) -> Result<T, ErrorCode>,
) -> Result<PendingEnvelope<T>, ErrorCode> {
    let (authorization, scope, family) =
        read_scope(authorization, requested_workspace, &bootstrap)?;
    let request = build_request(
        RetrievalIntent::trusted_memory_get(memory_id),
        &bootstrap.profile,
    )
    .map_err(|_| ErrorCode::Internal)?;
    let materialized = materialize_memory_get(
        &pool,
        &authorization,
        &scope,
        &family,
        &bootstrap.stream,
        memory_id,
    )
    .await?;
    accept_memory_envelope(
        materialized,
        &request,
        &bootstrap.binary_build,
        "direct_get",
        false,
        accept,
    )
}

pub(crate) async fn enumerate<T>(
    pool: Arc<RuntimeDbPool>,
    authorization: AuthorizationScope,
    requested_workspace: Option<WorkspaceId>,
    bootstrap: ContextBootstrap,
    params: MemoryEnumerationParams<'_>,
    accept: impl FnOnce(EnumerationResult) -> Result<T, ErrorCode>,
) -> Result<PendingEnvelope<T>, ErrorCode> {
    let (authorization, scope, family) =
        read_scope(authorization, requested_workspace, &bootstrap)?;
    let request = build_request(
        RetrievalIntent::trusted_memory_enumerate(),
        &bootstrap.profile,
    )
    .map_err(|_| ErrorCode::Internal)?;
    let page = materialize_memory_enumeration(
        &pool,
        &authorization,
        &scope,
        &family,
        &bootstrap.stream,
        params,
    )
    .await?;
    let pagination = Pagination {
        snapshot_id: page.snapshot_id,
        next_cursor: page.next_cursor,
    };
    accept_memory_envelope(
        page.memory,
        &request,
        &bootstrap.binary_build,
        "enumerate",
        pagination.next_cursor.is_some(),
        |content| {
            accept(EnumerationResult {
                content,
                pagination,
            })
        },
    )
}

/// Bootstrap policy for this cursor protocol; callers cannot extend it in MCP arguments.
pub(crate) const ENUMERATION_TTL: Duration = Duration::from_secs(15 * 60);

/// §36 `memory.supersede`, second (confirmed) call. The bound workspace must be the
/// bootstrap stream's workspace — the same `read_scope` rule `memory.get` applies — so the
/// lifecycle ticket lands on the stream whose ledger the reads consult.
pub(crate) async fn supersede(
    pool: Arc<RuntimeDbPool>,
    write: ConfirmedWrite,
    stream: StreamKey,
    target: MemoryId,
    successor: MemoryId,
) -> Result<SupersedeOutcome, ErrorCode> {
    let workspace = write
        .request
        .workspace_id()
        .ok_or(ErrorCode::DependencyUnavailable)?;
    let authorization = write.request.authorization().narrow(workspace)?;
    if stream.tenant_id != authorization.tenant_id() || stream.scope_id != workspace.0 {
        return Err(ErrorCode::DependencyUnavailable);
    }
    memory_governance_repo::supersede_atomically(
        &pool,
        &authorization,
        SupersedeRequest {
            request_id: write.request.request_id(),
            request_fingerprint: write.request_fingerprint,
            reservation_ttl: write.reservation_ttl,
            target,
            successor,
            stream,
            claim: write.claim,
            finished_audit: write.finished_audit,
        },
    )
    .await
}

/// §36 `memory.pin` / `memory.unpin`, second (confirmed) call (ADR-0019). The PINNED row is
/// scoped to the credential's bound workspace, which must be the bootstrap stream's
/// workspace — the same rule `memory.get` / `memory.supersede` apply — so `context.assemble`
/// reads it back through the same scope chain.
pub(crate) async fn write_binding(
    pool: Arc<RuntimeDbPool>,
    write: ConfirmedWrite,
    stream: StreamKey,
    op: DestructiveOp,
    memory: MemoryId,
) -> Result<BindingWriteOutcome, ErrorCode> {
    let workspace = write
        .request
        .workspace_id()
        .ok_or(ErrorCode::DependencyUnavailable)?;
    let authorization = write.request.authorization().narrow(workspace)?;
    if stream.tenant_id != authorization.tenant_id() || stream.scope_id != workspace.0 {
        return Err(ErrorCode::DependencyUnavailable);
    }
    let request = BindingWriteRequest {
        request_id: write.request.request_id(),
        request_fingerprint: write.request_fingerprint,
        reservation_ttl: write.reservation_ttl,
        memory,
        workspace,
        claim: write.claim,
        finished_audit: write.finished_audit,
    };
    match op {
        DestructiveOp::MemoryPin => {
            context_repo::pin_confirmed(&pool, &authorization, request).await
        }
        DestructiveOp::MemoryUnpin => {
            context_repo::unpin_confirmed(&pool, &authorization, request).await
        }
        DestructiveOp::MemorySupersede => Err(ErrorCode::InvalidInput),
    }
}

fn read_scope(
    authorization: AuthorizationScope,
    requested_workspace: Option<WorkspaceId>,
    bootstrap: &ContextBootstrap,
) -> Result<(AuthorizationScope, Scope, StreamFamily), ErrorCode> {
    let workspace = requested_workspace.ok_or(ErrorCode::DependencyUnavailable)?;
    let authorization = authorization.narrow(workspace)?;
    if bootstrap.stream.tenant_id != authorization.tenant_id()
        || bootstrap.stream.scope_id != workspace.0
    {
        return Err(ErrorCode::DependencyUnavailable);
    }
    let scope = Scope {
        tenant_id: authorization.tenant_id(),
        user_id: authorization.user_id(),
        workspace_id: Some(workspace),
        repository_id: None,
        task_id: None,
        run_id: None,
        agent_id: None,
    };
    let family = StreamFamily::new(
        bootstrap.stream.tenant_id,
        bootstrap.stream.scope_kind.clone(),
        bootstrap.stream.scope_id,
        bootstrap.stream.domain.clone(),
        bootstrap.stream.projection_kind.clone(),
    );
    Ok((authorization, scope, family))
}

fn accept_memory_envelope<T>(
    materialized: MaterializedMemory,
    request: &RetrievalRequest,
    binary_build: &str,
    lane: &str,
    truncated: bool,
    accept: impl FnOnce(Envelope<ContextItem>) -> Result<T, ErrorCode>,
) -> Result<PendingEnvelope<T>, ErrorCode> {
    let items = materialized
        .bodies
        .items
        .into_iter()
        .map(|item| match item {
            MaterializedItem::Memory { memory_id, content } => {
                Ok(ContextItem { memory_id, content })
            }
            _ => Err(ErrorCode::Internal),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let returned = u32::try_from(items.len()).map_err(|_| ErrorCode::Internal)?;
    let projection = build_projection_block(&materialized.ledger, None);
    // ponytail: neither an object nor a manifest page is an independent pipeline census.
    // Keep unknown counts; a page length must never become a whole-pipeline exact claim.
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
    let provenance = provenance(request, binary_build, vec![lane.to_owned()]);
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
        |final_outcome| {
            accept(Envelope {
                items,
                completeness: CompletenessBlock {
                    class: final_outcome.class,
                    reason: final_outcome.reason,
                    exact: final_outcome.exact,
                    known_lower_bound: final_outcome.known_lower_bound,
                    lanes: BTreeMap::from([(lane.to_owned(), lane_status)]),
                    candidate_count: 0,
                    reranked_count: 0,
                    returned,
                    truncated,
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
        },
    )
}
