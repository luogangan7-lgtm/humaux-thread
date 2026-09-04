//! Authenticated Context result: stable handoff diagnostics plus same-snapshot bodies.

use std::{
    collections::BTreeMap,
    io::Read,
    sync::{Arc, OnceLock},
};

use humaux_adapters::{
    context_repo::{MaterializedContext, assemble_materialized},
    postgres::RuntimeDbPool,
    read_materialize::MaterializedItem,
};
use humaux_domain::{
    context::ContextBudget,
    error::ErrorCode,
    identity::AuthorizationScope,
    ids::{Scope, WorkspaceId},
};
use humaux_projection::{serving::StreamFamily, stream::StreamKey};
use humaux_retrieval::{
    compiler::ContextOutcome,
    completeness::{CensusResult, FreshnessClass},
    envelope::{
        CompletenessBlock, CompletenessInputs, CountScope, Envelope, EvidenceBlock, FreshnessBlock,
        KnowledgeBlock, LaneStatus, MandatoryReport, PendingEnvelope, PinnedReport, PipelineBlock,
        ProfileBlock, ProvenanceBlock, ProvenanceValue, build_projection_block,
        envelope_outcome_block,
    },
    handoff::Handoff,
    request::{RegisteredRetrievalProfile, RetrievalIntent, RetrievalRequest, build_request},
};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::remember::RememberPolicy;

/// Trusted bootstrap state. Neither the stream nor the profile can come from MCP input.
#[derive(Debug, Clone)]
pub struct ContextBootstrap {
    budget: ContextBudget,
    pub(crate) profile: RegisteredRetrievalProfile,
    pub(crate) stream: StreamKey,
    pub(crate) binary_build: String,
}

impl ContextBootstrap {
    pub fn new(
        budget: ContextBudget,
        profile: RegisteredRetrievalProfile,
        write_policy: &RememberPolicy,
    ) -> Result<Self, ErrorCode> {
        Ok(Self {
            budget,
            profile,
            stream: write_policy.stream_key().clone(),
            binary_build: executable_fingerprint()?,
        })
    }

    pub(crate) const fn budget(&self) -> ContextBudget {
        self.budget
    }
}

/// One actually materialized Memory; membership and authority remain in the handoff.
#[derive(Debug, Clone, Serialize)]
pub struct ContextItem {
    pub memory_id: Uuid,
    pub content: Value,
    /// Q3/ADR-0024 D-C: `true` only on `memory.get` of an archived Memory. recall/context/
    /// enumerate never emit an archived item, so the field is skipped when `false` and those
    /// wire shapes stay byte-identical.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub archived: bool,
}

/// Composes existing contracts without putting manifest arrays into Envelope report fields.
#[derive(Debug, Clone, Serialize)]
pub struct ContextResult {
    pub handoff: Handoff,
    pub content: Envelope<ContextItem>,
}

fn executable_fingerprint() -> Result<String, ErrorCode> {
    static BUILD: OnceLock<String> = OnceLock::new();
    if let Some(build) = BUILD.get() {
        return Ok(build.clone());
    }
    let path = std::env::current_exe().map_err(|_| ErrorCode::DependencyUnavailable)?;
    let mut file = std::fs::File::open(path).map_err(|_| ErrorCode::DependencyUnavailable)?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|_| ErrorCode::DependencyUnavailable)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    let build = format!("sha256:{:x}", digest.finalize());
    let _ = BUILD.set(build.clone());
    Ok(build)
}

/// Reads only the configured workspace stream. A missing tenant route is not a synthetic
/// empty stream, and a client cannot select a version or broaden authorization.
pub async fn assemble<T>(
    pool: impl Into<Arc<RuntimeDbPool>>,
    authorization: AuthorizationScope,
    requested_workspace: Option<WorkspaceId>,
    bootstrap: ContextBootstrap,
    accept: impl FnOnce(ContextResult) -> Result<T, ErrorCode>,
) -> Result<PendingEnvelope<T>, ErrorCode> {
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
    let request = build_request(RetrievalIntent::trusted_context(), &bootstrap.profile)
        .map_err(|_| ErrorCode::Internal)?;
    let pool = pool.into();
    let family = StreamFamily::new(
        bootstrap.stream.tenant_id,
        bootstrap.stream.scope_kind.clone(),
        bootstrap.stream.scope_id,
        bootstrap.stream.domain.clone(),
        bootstrap.stream.projection_kind.clone(),
    );
    let materialized = assemble_materialized(
        &pool,
        &authorization,
        &scope,
        bootstrap.budget,
        &family,
        &bootstrap.stream,
    )
    .await?;
    into_result(materialized, &request, &bootstrap.binary_build, accept)
}

pub(crate) fn provenance(
    request: &RetrievalRequest,
    binary_build: &str,
    lanes: Vec<String>,
) -> ProvenanceBlock {
    ProvenanceBlock {
        binary_build: binary_build.to_owned(),
        projection_version: ProvenanceValue::NotApplicable {},
        embedding_model_id: ProvenanceValue::NotApplicable {},
        rerank_model_id: ProvenanceValue::NotApplicable {},
        card_builder_version: ProvenanceValue::NotApplicable {},
        profile_fingerprint: request.profile_fingerprint_identity().clone(),
        profile: ProfileBlock {
            top_k: request.top_k(),
            cand_k: request.cand_k(),
            cand_k_formula: request.cand_k_formula(),
            lanes,
        },
    }
}

fn into_result<T>(
    materialized: MaterializedContext,
    request: &RetrievalRequest,
    binary_build: &str,
    accept: impl FnOnce(ContextResult) -> Result<T, ErrorCode>,
) -> Result<PendingEnvelope<T>, ErrorCode> {
    let MaterializedContext {
        handoff,
        outcome,
        bodies,
        ledger,
        grounding,
    } = materialized;
    let items = bodies
        .items
        .into_iter()
        .map(|item| match item {
            MaterializedItem::Memory { memory_id, content } => Ok(ContextItem {
                memory_id,
                content,
                // context.assemble excludes archived rows at the candidate step (D-C).
                archived: false,
            }),
            _ => Err(ErrorCode::Internal),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let returned = u32::try_from(items.len()).map_err(|_| ErrorCode::Internal)?;
    let projection = build_projection_block(&ledger, None);
    // These counts were not read in an independently established authorized universe.
    // Never replace them with the number of Context items or stream rows.
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
    let provenance = provenance(
        request,
        binary_build,
        vec!["mandatory".to_owned(), "pinned".to_owned()],
    );
    let lane_status = if handoff.unavailable_selectors.is_empty() {
        LaneStatus::Ok
    } else {
        LaneStatus::Failed
    };
    let census = CensusResult::ok_without_enumeration();
    let mandatory = match &outcome {
        ContextOutcome::Compiled(compiled) => {
            MandatoryReport::from_compiled(handoff.counts.mandatory_expected, compiled)
        }
        ContextOutcome::Overflow(overflow) => MandatoryReport::from_overflow(overflow),
    };
    envelope_outcome_block(
        request,
        CompletenessInputs {
            lane_status: &lane_status,
            census: &census,
            ledger: &ledger,
            pipeline: &pipeline,
            provenance: &provenance,
            visible: None,
            context: Some(&outcome),
        },
        |final_outcome| {
            let content = Envelope {
                items,
                completeness: CompletenessBlock {
                    class: final_outcome.class,
                    reason: final_outcome.reason,
                    exact: final_outcome.exact,
                    known_lower_bound: final_outcome.known_lower_bound,
                    lanes: BTreeMap::from([
                        ("mandatory".to_owned(), lane_status),
                        ("pinned".to_owned(), LaneStatus::Ok),
                    ]),
                    candidate_count: 0,
                    reranked_count: 0,
                    returned,
                    truncated: false,
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
                grounding,
                mandatory,
                pinned: PinnedReport::Ran {
                    expected: handoff.counts.pinned_expected,
                    returned: handoff.counts.pinned_returned,
                },
            };
            accept(ContextResult { handoff, content })
        },
    )
}
