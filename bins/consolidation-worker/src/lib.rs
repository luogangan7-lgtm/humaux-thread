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
use humaux_adapters::jobs;
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
/// No `ops.jobs` fence: callers with no claimed job (the hop tests) settle nothing.
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
        None,
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
    fence: Option<&jobs::DerivedLease<'_>>,
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
                fence,
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
                fence,
            )
            .await?)
        }
    }
}

/// Deployment-owned inputs for one cross-tenant dispatch pass (ADR-0036, card 14). No tenant id
/// and no route-binding id: both come from the claimed job / the per-tenant resolver, which is
/// exactly what makes one process able to serve N tenants.
#[derive(Debug, Clone)]
pub struct DispatchConfig {
    /// Per-process lease owner; every terminal transition is fenced on it plus the claim's
    /// `attempt`, so two resident workers never both settle one job.
    pub lease_owner: String,
    pub lease_seconds: f64,
    /// Max jobs one pass claims.
    pub batch: i64,
    /// §11.7 selection cap handed to `select_and_materialize_inputs`.
    pub max_inputs: i64,
    /// At or past this many attempts a repeatedly-failing job is parked `DEAD` instead of being
    /// released for another pass — otherwise a poison tenant burns every pass forever.
    pub max_attempts: i32,
}

impl DispatchConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.lease_owner.trim().is_empty() {
            return Err("lease_owner");
        }
        if !self.lease_seconds.is_finite() || self.lease_seconds <= 0.0 {
            return Err("lease_seconds");
        }
        if self.batch <= 0 {
            return Err("batch");
        }
        if self.max_inputs <= 0 {
            return Err("max_inputs");
        }
        if self.max_attempts <= 0 {
            return Err("max_attempts");
        }
        Ok(())
    }
}

/// What one [`dispatch_pass`] did. `claimed == 0` is the "no input" signal `--run-once` exits on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DispatchReport {
    pub claimed: u32,
    pub published: u32,
    pub no_output: u32,
    pub stale_input: u32,
    /// Jobs released back to PENDING after an environmental failure (no route binding yet,
    /// provider blip, DB blip). Released with a backoff (`adapters::jobs::retry_backoff_seconds`),
    /// not instantly re-claimable.
    pub deferred: u32,
    /// Jobs released because this tenant is NOT READY yet — its `PRIVATE_CONSOLIDATE` route
    /// binding has not been admitted. Counted apart from [`Self::deferred`] because it never
    /// spends the retry budget: a tenant onboarded before its binding is admitted must not lose
    /// its consolidation work to `DEAD` (ADR-0036 D5).
    pub not_ready: u32,
    /// Jobs parked DEAD after exhausting `max_attempts`.
    pub dead: u32,
    /// Jobs whose lease was already reclaimed by the time this worker tried to settle them —
    /// nothing of theirs was committed by this process.
    pub lost_lease: u32,
}

/// The `reasoning_domain_id` `migrations/0164_derived_work_dispatch.sql`'s
/// `derived_consolidate_work_enqueue` trigger writes into the job payload. A payload this worker
/// cannot read is a permanent defect of that row, not a transient one — it is parked, never
/// retried forever.
fn payload_reasoning_domain(payload: &serde_json::Value) -> Option<Uuid> {
    payload
        .get("reasoning_domain_id")?
        .as_str()?
        .parse::<Uuid>()
        .ok()
}

/// One cross-tenant dispatch pass (ADR-0036): claim up to `config.batch` `DERIVED_CONSOLIDATE`
/// jobs through the owner SECURITY DEFINER `ops.claim_derived_work`, then run each one's
/// consolidation under ITS OWN tenant's RLS context — `run_once_bound` and every repo it calls
/// install `humaux.tenant_id` from the claimed job, so a pass for tenant A can read nothing of
/// tenant B's even though the claim that found it was cross-tenant.
///
/// `bind_port` receives `(tenant_id, run_id)`: the inference client is sealed per tenant AND per
/// run (ADR-0015), so it cannot be built once for the process the way a single-tenant worker's
/// could.
pub async fn dispatch_pass<P: PrivateReasoningPort>(
    pool: &ConsolidationDbPool,
    bind_port: impl Fn(Uuid, Uuid) -> P,
    config: &DispatchConfig,
) -> Result<DispatchReport, DispatchError> {
    config.validate().map_err(DispatchError::Config)?;
    let claimed = jobs::claim_derived_work_consolidation(
        pool,
        &[jobs::DerivedJobType::Consolidate],
        &config.lease_owner,
        config.lease_seconds,
        config.batch,
    )
    .await?;
    let mut report = DispatchReport {
        claimed: claimed.len() as u32,
        ..DispatchReport::default()
    };
    for job in claimed {
        let lease = jobs::DerivedLease::of(&job, &config.lease_owner);
        // Refresh before the (potentially long) inference hop rather than after it — a heartbeat
        // taken once the work is already done proves nothing about the lease that protected it.
        if !jobs::heartbeat_derived_consolidation(pool, &lease, config.lease_seconds).await? {
            report.lost_lease += 1;
            continue;
        }
        let outcome = run_claimed_job(pool, &bind_port, config, &job, &lease).await;
        let settle = match &outcome {
            // Already settled DONE inside `publish_rollup`'s own transaction (the fence): the
            // rollup and the terminal transition committed together, so there is no second
            // settle to do and no window between them for the lease to lapse in.
            Ok(PublishOutcome::Published { .. }) => {
                report.published += 1;
                continue;
            }
            // The fence found the job re-claimed: this worker's transaction rolled back and
            // wrote NOTHING. The worker that now holds the job publishes the one rollup.
            Ok(PublishOutcome::LostLease) => {
                report.lost_lease += 1;
                continue;
            }
            Ok(PublishOutcome::NoOutput) => {
                report.no_output += 1;
                jobs::DerivedWorkOutcome::Done
            }
            // §11.7: a stale run publishes nothing and the next run redoes it — that is a
            // release, not a completion.
            Ok(PublishOutcome::StaleInput) => {
                report.stale_input += 1;
                retry_or_park(config, &job, &mut report)
            }
            // Not a failure of this job — the tenant is not provisioned yet. Release it with a
            // backoff and DO NOT spend an attempt: parking it `DEAD` would drop that memory's
            // consolidation permanently (the 0164 enqueue trigger's idempotency key is per
            // memory_id with ON CONFLICT DO NOTHING, so the job is never re-emitted).
            Err(RunOnceError::NotReady(reason)) => {
                eprintln!(
                    "humaux-consolidation-worker: job {} (tenant {}) not ready: {reason}",
                    job.job_id, job.tenant_id
                );
                report.not_ready += 1;
                jobs::DerivedWorkOutcome::Retry
            }
            Err(error) => {
                // The pass keeps going: one tenant's environmental failure must not stop the
                // other tenants this cross-tenant pass claimed.
                eprintln!(
                    "humaux-consolidation-worker: job {} (tenant {}) failed: {error}",
                    job.job_id, job.tenant_id
                );
                retry_or_park(config, &job, &mut report)
            }
        };
        if !jobs::settle_derived_consolidation(pool, &lease, settle, config.lease_seconds).await? {
            report.lost_lease += 1;
        }
    }
    Ok(report)
}

fn retry_or_park(
    config: &DispatchConfig,
    job: &jobs::ClaimedJob,
    report: &mut DispatchReport,
) -> jobs::DerivedWorkOutcome {
    if job.attempt >= config.max_attempts {
        report.dead += 1;
        jobs::DerivedWorkOutcome::Dead
    } else {
        report.deferred += 1;
        jobs::DerivedWorkOutcome::Retry
    }
}

async fn run_claimed_job<P: PrivateReasoningPort>(
    pool: &ConsolidationDbPool,
    bind_port: &impl Fn(Uuid, Uuid) -> P,
    config: &DispatchConfig,
    job: &jobs::ClaimedJob,
    lease: &jobs::DerivedLease<'_>,
) -> Result<PublishOutcome, RunOnceError> {
    let Some(reasoning_domain_id) = payload_reasoning_domain(&job.payload) else {
        return Err(RunOnceError::Reasoning(PrivateReasoningError::new(
            "DERIVED_CONSOLIDATE payload carries no reasoning_domain_id",
        )));
    };
    // `consolidate_repo::resolve_consolidate_binding` calls this condition "environmental
    // (retryable)" itself: the tenant simply has no admitted route yet. It is NOT a defect of
    // this job, so it gets its own error class and never reaches `retry_or_park`.
    let (binding_id, binding_version) =
        consolidate_repo::resolve_consolidate_binding(pool, job.tenant_id, reasoning_domain_id)
            .await?
            .ok_or(RunOnceError::NotReady(
                "no admitted PRIVATE_CONSOLIDATE route binding for this tenant/domain",
            ))?;
    let tenant_id = job.tenant_id;
    run_once_bound(
        pool,
        |run_id| bind_port(tenant_id, run_id),
        tenant_id,
        reasoning_domain_id,
        binding_id,
        binding_version,
        // Tenant-scope rollups. A workspace-scoped pass needs the workspace in the job payload,
        // which needs an enqueue site that knows one — not something this card's triggers see.
        None,
        config.max_inputs,
        build_rollup,
        Some(lease),
    )
    .await
}

#[derive(Debug)]
pub enum DispatchError {
    Config(&'static str),
    Jobs(jobs::JobsError),
}

impl From<jobs::JobsError> for DispatchError {
    fn from(e: jobs::JobsError) -> Self {
        Self::Jobs(e)
    }
}

impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(field) => write!(f, "invalid dispatch configuration: {field}"),
            Self::Jobs(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for DispatchError {}

#[derive(Debug)]
pub enum RunOnceError {
    Repo(ConsolidateRepoError),
    Reasoning(PrivateReasoningError),
    /// The tenant is not provisioned for this hop yet (no admitted route binding). Retryable
    /// forever with a backoff — never parked `DEAD`, see [`dispatch_pass`].
    NotReady(&'static str),
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
            Self::NotReady(reason) => write!(f, "{reason}"),
        }
    }
}
