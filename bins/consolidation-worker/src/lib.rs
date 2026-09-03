//! `humaux-consolidation-worker`'s orchestration logic (§11.7/§11.8 T4.6+T4.7), split into a
//! library target so `tests/run_once_e2e.rs` can drive [`run_once`] against a real
//! `ConsolidationDbPool` — a binary-only crate has no target integration tests can link
//! against, which is exactly why no test ever exercised this function before this review
//! (major finding: `run_once` had zero callers and zero test coverage anywhere in the
//! workspace). `src/main.rs` is now the thin process-entry shell; this file is the tested part.
//!
//! §11.8 hard boundary this crate's dependency graph enforces structurally (see `Cargo.toml`'s
//! doc comment): it owns [`ConsolidationDbPool`] and nothing else capability-shaped — no
//! private-worker pool type, no BYOK decrypt client. Its only inference path is
//! [`PrivateReasoningPort`], a trait object built per run by the caller's factory
//! ([`run_once_bound`]'s `bind_port`) — this crate never names a concrete client type in its
//! orchestration; `src/main.rs` supplies the real `UdsInferenceClient` bound to the run id.
//!
//! §78 single-source: the input manifest hash and the rollup output parser live in
//! `humaux_adapters::consolidation_reasoner` and are shared with `humaux-private-worker` —
//! the two hop ends can only agree on "what was sent" if they hash the same bytes the same way.

pub mod inference_client;

use humaux_adapters::consolidate_repo::{
    self, ConsolidateRepoError, MaterializedInput, PublishOutcome,
};
use humaux_adapters::consolidation_reasoner::{compute_input_manifest_hash, parse_rollup_output};
use humaux_adapters::postgres::ConsolidationDbPool;
use humaux_application::consolidate::{
    NextStep, PrivateReasoningDomainId, PrivateReasoningError, PrivateReasoningPort,
    PrivateReasoningPurpose, PrivateReasoningResult, ReasoningRouteBindingId,
    ReasoningRouteBindingVersion, SealedPrivateReasoningRequest, next_step,
};
use humaux_domain::authority::{AuthorityClass, EvidenceId};
use humaux_domain::consolidate::AutoMutableMemoryId;
use uuid::Uuid;

/// What `build_rollup` must hand `consolidate_repo::publish_rollup`.
pub type RollupParts = (
    serde_json::Value,
    AuthorityClass,
    Vec<(AutoMutableMemoryId, EvidenceId)>,
);

/// The production `build_rollup` (§11.6/§11.9): the private worker's JSON reply parsed
/// fail-closed against exactly this run's materialized `(memory_id, evidence_id)` pairs —
/// `parse_rollup_output` can only ever pick sources from ids the run itself recorded, and the
/// rollup body is the reply's `content` string wrapped as `{"content": ...}`.
pub fn build_rollup(
    inputs: &[MaterializedInput],
    result: &PrivateReasoningResult,
) -> Result<RollupParts, PrivateReasoningError> {
    let allowed: Vec<(AutoMutableMemoryId, EvidenceId)> = inputs
        .iter()
        .map(|input| (input.memory_id, input.evidence_id))
        .collect();
    let (content, class, sources) = parse_rollup_output(&result.output_bytes, &allowed)
        .map_err(|code| PrivateReasoningError::new(format!("ROLLUP_OUTPUT_REJECTED:{code:?}")))?;
    Ok((serde_json::json!({ "content": content }), class, sources))
}

/// `&dyn PrivateReasoningPort` viewed as an owned port, so [`run_once`] can reuse
/// [`run_once_bound`] for callers that have no per-run state to bind.
struct BorrowedPort<'a>(&'a dyn PrivateReasoningPort);

#[async_trait::async_trait]
impl PrivateReasoningPort for BorrowedPort<'_> {
    async fn infer(
        &self,
        req: SealedPrivateReasoningRequest,
    ) -> Result<PrivateReasoningResult, PrivateReasoningError> {
        self.0.infer(req).await
    }
}

/// [`run_once_bound`] for a port that carries no per-run state; the closure receives the
/// materialized ids only.
#[allow(clippy::too_many_arguments)]
pub async fn run_once(
    pool: &ConsolidationDbPool,
    port: &dyn PrivateReasoningPort,
    tenant_id: Uuid,
    reasoning_domain_id: Uuid,
    binding_id: ReasoningRouteBindingId,
    binding_version: ReasoningRouteBindingVersion,
    workspace_id: Option<Uuid>,
    max_inputs: i64,
    build_rollup: impl FnOnce(
        &[AutoMutableMemoryId],
        &PrivateReasoningResult,
    ) -> Result<RollupParts, PrivateReasoningError>,
) -> Result<PublishOutcome, RunOnceError> {
    run_once_bound(
        pool,
        |_run_id| BorrowedPort(port),
        tenant_id,
        reasoning_domain_id,
        binding_id,
        binding_version,
        workspace_id,
        max_inputs,
        |inputs, result| {
            let ids: Vec<AutoMutableMemoryId> = inputs.iter().map(|m| m.memory_id).collect();
            build_rollup(&ids, result)
        },
    )
    .await
}

/// One consolidation run, start to finish: snapshot-bound selection (§11.7), then either skip
/// straight to `SUCCEEDED_NO_OUTPUT` (§11.7, empty input set) or call the sealed inference
/// port and publish. `bind_port` builds the port AFTER `create_run` so the port can carry the
/// run id (ADR-0015: `UdsInferenceClient` registers `consolidation_run_id` alongside the sealed
/// identifiers, since the sealed request itself — §11.8 — carries no run id); see module doc
/// for why this crate never names a concrete inference client type.
///
/// `binding_id` plus `binding_version` is the caller's exact immutable Phase 9 R3 authority.
/// This BYOK-capability-free worker only seals and forwards that pair; the private worker must
/// resolve and admit it immediately before private materialization or provider dispatch.
///
/// `build_rollup` receives the actual [`PrivateReasoningResult`] (not just the id list) so its
/// `output_bytes` are never silently discarded (§11.5.1: a `RunInference` call that then throws
/// its own result away is a wasted BYOK request in every sense that matters) — the closure, not
/// this function, decides how to turn that opaque result into `(content, rollup_class,
/// sources)`, since the sealed reasoning payload's wire format is still a later task's concern
/// (T4.4/T4.5); wiring a made-up format in here would be exactly the kind of speculative code
/// this workspace's own review guidance rejects. `sources` is `AutoMutableMemoryId` end to end —
/// `build_rollup` can therefore only ever pick from the ids it was itself handed (§11.8's
/// typestate has no public constructor for the type), never invent one out of thin air.
#[allow(clippy::too_many_arguments)]
pub async fn run_once_bound<P: PrivateReasoningPort>(
    pool: &ConsolidationDbPool,
    bind_port: impl FnOnce(Uuid) -> P,
    tenant_id: Uuid,
    reasoning_domain_id: Uuid,
    binding_id: ReasoningRouteBindingId,
    binding_version: ReasoningRouteBindingVersion,
    workspace_id: Option<Uuid>,
    max_inputs: i64,
    build_rollup: impl FnOnce(
        &[MaterializedInput],
        &PrivateReasoningResult,
    ) -> Result<RollupParts, PrivateReasoningError>,
) -> Result<PublishOutcome, RunOnceError> {
    let run_id =
        consolidate_repo::create_run(pool, tenant_id, reasoning_domain_id, workspace_id).await?;
    let port = bind_port(run_id);

    let inputs = consolidate_repo::select_and_materialize_inputs(
        pool,
        run_id,
        tenant_id,
        reasoning_domain_id,
        workspace_id,
        max_inputs,
    )
    .await?;
    let auto_ids: Vec<AutoMutableMemoryId> = inputs.iter().map(|m| m.memory_id).collect();

    match next_step(auto_ids.len()) {
        NextStep::SkipToNoOutput => {
            // §11.5.1: don't spend a BYOK inference call on a guaranteed-empty result.
            // `rollup_class` is unused on this branch (`publish_rollup` short-circuits on an
            // empty `sources` slice before its §11.9 ceiling check ever runs) — `PublicKnowledge`
            // is the lowest-ranked class, chosen only so the placeholder can never look like a
            // real classification decision if this branch's plumbing ever changes.
            Ok(consolidate_repo::publish_rollup(
                pool,
                run_id,
                tenant_id,
                workspace_id,
                serde_json::Value::Null,
                AuthorityClass::PublicKnowledge,
                None,
                &[],
            )
            .await?)
        }
        NextStep::RunInference => {
            // Reaching `port.infer` at all is the G11-R1 positive sentinel's real call site;
            // the fake-port version of that same call lives in
            // `humaux_application::consolidate`'s own unit tests (no network stack needed to
            // prove the trait boundary is usable), and `tests/run_once_e2e.rs` in this crate
            // drives this exact call site end to end against a real `ConsolidationDbPool`.
            let manifest_hash = compute_input_manifest_hash(&inputs);
            let req = SealedPrivateReasoningRequest {
                reasoning_domain_id: PrivateReasoningDomainId(reasoning_domain_id),
                binding_id,
                binding_version,
                input_manifest_hash: manifest_hash,
                purpose: PrivateReasoningPurpose::Consolidate,
                contribution_attempt: None,
            };
            let inference = port.infer(req).await?;
            let (content, rollup_class, sources) = build_rollup(&inputs, &inference)?;
            Ok(consolidate_repo::publish_rollup(
                pool,
                run_id,
                tenant_id,
                workspace_id,
                content,
                rollup_class,
                Some(&manifest_hash.0),
                &sources,
            )
            .await?)
        }
    }
}

#[derive(Debug)]
pub enum RunOnceError {
    Repo(ConsolidateRepoError),
    Reasoning(PrivateReasoningError),
}

impl From<ConsolidateRepoError> for RunOnceError {
    fn from(e: ConsolidateRepoError) -> Self {
        Self::Repo(e)
    }
}

impl From<PrivateReasoningError> for RunOnceError {
    fn from(e: PrivateReasoningError) -> Self {
        Self::Reasoning(e)
    }
}

impl std::fmt::Display for RunOnceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Repo(e) => write!(f, "{e}"),
            Self::Reasoning(e) => write!(f, "{e}"),
        }
    }
}
