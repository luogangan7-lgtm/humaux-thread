//! `humaux-consolidation-worker`'s orchestration logic (§11.7/§11.8 T4.6+T4.7), split into a
//! library target so `tests/run_once_e2e.rs` can drive [`run_once`] against a real
//! `ConsolidationDbPool` — a binary-only crate has no target integration tests can link
//! against, which is exactly why no test ever exercised this function before this review
//! (major finding: `run_once` had zero callers and zero test coverage anywhere in the
//! workspace). `src/main.rs` is now the thin process-entry shell; this file is the tested part.
//!
//! §11.8 hard boundary this crate's dependency graph enforces structurally (see `Cargo.toml`'s
//! doc comment): it owns [`ConsolidationDbPool`] and nothing else capability-shaped — no
//! `PrivateWorkerDbPool`, no BYOK decrypt client. Its only inference path is
//! [`PrivateReasoningPort`], a trait object supplied by whatever wires this binary for real (the
//! sealed mTLS RPC client that talks to `humaux-private-worker` is a later task, out of this
//! crate's file scope) — [`run_once`] takes `&dyn PrivateReasoningPort` rather than naming a
//! concrete client type for exactly that reason.

use humaux_adapters::consolidate_repo::{self, ConsolidateRepoError, PublishOutcome};
use humaux_adapters::postgres::ConsolidationDbPool;
use humaux_application::consolidate::{
    ContentSha256, NextStep, PrivateReasoningDomainId, PrivateReasoningError, PrivateReasoningPort,
    PrivateReasoningPurpose, PrivateReasoningResult, SealedPrivateReasoningRequest,
    UserReasoningProfileVersion, next_step,
};
use humaux_domain::authority::{AuthorityClass, EvidenceId};
use humaux_domain::consolidate::AutoMutableMemoryId;
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// §11.8 `input_manifest_hash`: SHA-256 over this run's *own* materialized inputs, in the exact
/// `(memory_id, input_version, source_hash, ordinal)` shape `private.memory_consolidation_inputs`
/// recorded them — this identifies precisely what was sent for inference (the whole reason the
/// field exists) rather than a placeholder constant no adversary or auditor could ever tie back
/// to a real snapshot.
fn compute_input_manifest_hash(inputs: &[consolidate_repo::MaterializedInput]) -> ContentSha256 {
    let mut hasher = Sha256::new();
    for input in inputs {
        hasher.update(input.memory_id.into_inner().0.as_bytes());
        hasher.update(input.input_version.to_be_bytes());
        hasher.update(&input.source_hash);
        hasher.update(input.ordinal.to_be_bytes());
    }
    let digest = hasher.finalize();
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&digest);
    ContentSha256(bytes)
}

/// One consolidation run, start to finish: snapshot-bound selection (§11.7), then either skip
/// straight to `SUCCEEDED_NO_OUTPUT` (§11.7, empty input set) or call the sealed inference
/// port and publish. `port` is `&dyn` — see module doc for why this crate never names a
/// concrete inference client type.
///
/// `profile_version` is caller-supplied rather than looked up here: `role_consolidation_worker`
/// has no path to a real `UserReasoningProfile` (that lookup needs `humaux.user_id` set under
/// `control.user_reasoning_profiles`'s owner-scoped RLS policy, which this binary — deliberately
/// BYOK-capability-free, see module doc — never sets); a caller that *does* hold that context
/// (T4.4/T4.5's own resolution step) passes the real version through instead of this binary
/// fabricating one.
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
pub async fn run_once(
    pool: &ConsolidationDbPool,
    port: &dyn PrivateReasoningPort,
    tenant_id: Uuid,
    reasoning_domain_id: Uuid,
    profile_version: UserReasoningProfileVersion,
    workspace_id: Option<Uuid>,
    max_inputs: i64,
    build_rollup: impl FnOnce(
        &[AutoMutableMemoryId],
        &PrivateReasoningResult,
    ) -> Result<
        (
            serde_json::Value,
            AuthorityClass,
            Vec<(AutoMutableMemoryId, EvidenceId)>,
        ),
        PrivateReasoningError,
    >,
) -> Result<PublishOutcome, RunOnceError> {
    let run_id =
        consolidate_repo::create_run(pool, tenant_id, reasoning_domain_id, workspace_id).await?;

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
                profile_version,
                input_manifest_hash: manifest_hash,
                purpose: PrivateReasoningPurpose::Consolidate,
            };
            let inference = port.infer(req).await?;
            let (content, rollup_class, sources) = build_rollup(&auto_ids, &inference)?;
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
