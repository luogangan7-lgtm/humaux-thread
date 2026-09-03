//! The Distill hop (§15.5 / §16.1.1 / §10.1, ADR-0016): accepted Evidence → 0..N
//! `private.memory_records`, owned by this process because §6.2.2 already gives
//! `role_private_worker` both the provider capability and the only INSERT on
//! `memory_records`/`memory_evidence` (no RPC needed, contrast ADR-0015's Consolidate hop).
//!
//! One [`run_once`] = one pass: claim up to `batch` `EVIDENCE_ACCEPTED` outbox rows, and per row
//! (a) resolve the admitted Distill route, load the Evidence, record the §16.1.1 processing
//! run (fingerprint first — before any bytes leave), (b) one provider round trip through the
//! shared pipeline (`humaux_adapters::distill_reasoner`), (c) parse fail-closed, authorize each
//! candidate against §10.1's origin-bound ceiling — over-ceiling candidates are rejected with a
//! reason and never downgraded — and commit the surviving memories, the run's completion and the
//! outbox DONE flip in ONE lease-fenced transaction (idempotency, ADR-0016 D5).
//!
//! Failure split (ADR-0016 D5): only *input-bound* rejections are terminal — the Evidence is
//! unavailable to this hop, or the reply failed the parser — and settle the row FAILED (the
//! ticket then fails as `distill_failed`). Every reasoning-side failure is environmental
//! (route not yet bound / not admitted, provider 429/5xx, disclosure ledger) and hands the row
//! back to PENDING for a later pass, so a transient outage can never wedge the tenant's §15.7
//! contiguous prefix. Either way the run row keeps `completed_at` NULL as the attempt marker.
//!
//! Tickets: remember already issued the `projection.stream_log` row for this Evidence; once the
//! outbox row is DONE the retrieval worker resolves it through `memory_evidence` (or settles it
//! as a no-op when the answer was "nothing memorable", `projection_worker` D6).

use humaux_adapters::{
    byok::UserReasoningProvider,
    contribution_reasoner::ContributionReasonerConfig,
    distill_reasoner::{
        DISTILL_PARSER_VERSION, DISTILL_PROCESSOR_KIND, DISTILL_PROCESSOR_VERSION,
        DistillEnvelopeInput, DistillReasoner, distill_prompt_contract, parse_distill_output,
    },
    distill_repo::{
        self, ClaimedEvidence, DbError, LoadedEvidence, NewMemory, OutboxTerminal,
        ProcessingRunStart,
    },
    postgres::PrivateWorkerDbPool,
};
use humaux_application::consolidate::PrivateReasoningError;
use humaux_domain::{
    authority::{AuthorityPolicy, CandidateRejection, NonEmptyVec},
    dataclass::DataClass,
    error::ErrorCode,
    evidence::{EvidenceOriginClass, payload_sha256},
    ids::{Scope, TenantId, UserId, WorkspaceId},
    memory::MemoryType,
    policy::OriginBoundAuthorityPolicy,
};
use humaux_projection::fingerprint::{ProcessingInputFingerprintInputs, source_hash};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Deployment-owned inputs for one pass (all from `HUMAUX_PRIVATE_WORKER_DISTILL_*`, §78.1).
#[derive(Debug, Clone)]
pub struct DistillConfig {
    pub tenant_id: Uuid,
    pub reasoning_domain_id: Uuid,
    /// Max outbox rows one pass claims.
    pub batch: i64,
    pub lease_seconds: f64,
    /// Per-process lease owner; the DONE flip is fenced on it (ADR-0016 D5).
    pub lease_owner: String,
}

impl DistillConfig {
    pub fn validate(&self) -> Result<(), ErrorCode> {
        if self.tenant_id.is_nil()
            || self.reasoning_domain_id.is_nil()
            || self.batch <= 0
            || !self.lease_seconds.is_finite()
            || self.lease_seconds <= 0.0
            || self.lease_owner.trim().is_empty()
        {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(())
    }
}

/// What one pass did. `rejected` is the §10.1 `memory_candidate_rejections_total` count for
/// this pass (every reason); the per-reason line is emitted where it happens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DistillPassReport {
    pub claimed: u32,
    pub done: u32,
    pub failed: u32,
    /// Rows handed back to PENDING after a retryable (reasoning-side) failure.
    pub deferred: u32,
    /// Rows whose lease was reclaimed before this worker could settle them (nothing written).
    pub lost_lease: u32,
    pub memories: u32,
    pub rejected: u32,
}

#[derive(Debug)]
pub enum DistillError {
    Db(DbError),
    Reasoning(PrivateReasoningError),
    Config(ErrorCode),
}

impl std::fmt::Display for DistillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(error) => write!(f, "distill database error: {error}"),
            Self::Reasoning(error) => write!(f, "distill reasoning error: {error}"),
            Self::Config(code) => write!(f, "distill configuration rejected: {code:?}"),
        }
    }
}

impl std::error::Error for DistillError {}

impl From<DbError> for DistillError {
    fn from(value: DbError) -> Self {
        Self::Db(value)
    }
}

impl From<PrivateReasoningError> for DistillError {
    fn from(value: PrivateReasoningError) -> Self {
        Self::Reasoning(value)
    }
}

/// §10.1 closed reason set → the `memory_candidate_rejections_total{reason}` label values
/// (`Baseline_2.9.md` metrics registry: origin_authority_ceiling | untrusted_instruction |
/// cross_tenant_evidence | missing_confirmation).
fn rejection_reason(rejection: CandidateRejection) -> &'static str {
    match rejection {
        CandidateRejection::OriginAuthorityCeiling => "origin_authority_ceiling",
        CandidateRejection::UntrustedInstruction => "untrusted_instruction",
        CandidateRejection::CrossTenantEvidence => "cross_tenant_evidence",
        CandidateRejection::MissingConfirmation => "missing_confirmation",
    }
}

/// ADR-0016 D2: the acting identity the §11.1 context carries — the Evidence's own principal
/// when its origin is a user-shaped one, else the reasoning domain owner.
fn acting_user(evidence: &LoadedEvidence, owner_user_id: Uuid) -> Uuid {
    match evidence.origin_class {
        EvidenceOriginClass::DirectUserInput
        | EvidenceOriginClass::UserConfirmed
        | EvidenceOriginClass::TenantAdmin => evidence.origin_principal_id.unwrap_or(owner_user_id),
        _ => owner_user_id,
    }
}

/// `memory_records.content` shape this hop writes — the fields `projection_worker::card_input`
/// reads (`title` + `key_claim`; `build_card` needs at least one of `key_claim`/
/// `evidence_excerpt`) and `consolidation_reasoner` forwards as opaque JSON.
fn memory_content(text: &str) -> Value {
    serde_json::json!({
        "title": text.chars().take(80).collect::<String>(),
        "key_claim": text,
    })
}

/// One pass over the tenant's pending Evidence. Never panics on a provider/parse failure —
/// a parse failure settles the row FAILED, a reasoning failure hands it back to PENDING, and the
/// pass continues; a database failure aborts the pass (leases expire and the rows are reclaimed
/// next time).
pub async fn run_once(
    pool: &PrivateWorkerDbPool,
    provider: &dyn UserReasoningProvider,
    config: ContributionReasonerConfig,
    distill: &DistillConfig,
) -> Result<DistillPassReport, DistillError> {
    distill.validate().map_err(DistillError::Config)?;
    let reasoner = DistillReasoner::new(pool, provider, config).map_err(DistillError::Config)?;
    let claimed = distill_repo::claim_pending_evidence(
        pool,
        distill.tenant_id,
        &distill.lease_owner,
        distill.batch,
        distill.lease_seconds,
    )
    .await?;
    let mut report = DistillPassReport {
        claimed: claimed.len() as u32,
        ..DistillPassReport::default()
    };
    for row in claimed {
        process_claimed(pool, &reasoner, distill, row, &mut report).await?;
    }
    Ok(report)
}

/// Provider-side outcome of one claimed row, before the write leg.
struct Inferred {
    evidence: LoadedEvidence,
    processing_run_id: Uuid,
    output_bytes: Vec<u8>,
    disclosure_id: Uuid,
}

async fn process_claimed(
    pool: &PrivateWorkerDbPool,
    reasoner: &DistillReasoner<'_>,
    distill: &DistillConfig,
    row: ClaimedEvidence,
    report: &mut DistillPassReport,
) -> Result<(), DistillError> {
    let inferred = match infer_claimed(pool, reasoner, distill, row).await {
        Ok(Some(inferred)) => inferred,
        Ok(None) => {
            eprintln!(
                "humaux-private-worker: distill evidence={} failed: no_output",
                row.evidence_id
            );
            return settle_failed(pool, distill, row, report).await;
        }
        Err(DistillError::Reasoning(_)) => {
            return settle_deferred(pool, distill, row, report).await;
        }
        Err(other) => return Err(other),
    };
    let candidates = match parse_distill_output(&inferred.output_bytes) {
        Ok(candidates) => candidates,
        Err(error) => {
            // Wire-level class only (ErrorCode / redacted reasoning error), never payload text.
            eprintln!(
                "humaux-private-worker: distill evidence={} failed: {error:?}",
                row.evidence_id
            );
            return settle_failed(pool, distill, row, report).await;
        }
    };

    let evidence = &inferred.evidence;
    let scope = Scope {
        tenant_id: TenantId(distill.tenant_id),
        user_id: evidence.visibility_user_id.map(UserId),
        workspace_id: evidence.visibility_workspace_id.map(WorkspaceId),
        repository_id: None,
        task_id: None,
        run_id: None,
        agent_id: None,
    };
    let mut txn =
        distill_repo::begin_write_context(pool, distill.tenant_id, evidence.rls_user_id).await?;
    let mut inserted: i32 = 0;
    for candidate in &candidates {
        let basis =
            NonEmptyVec::new(vec![evidence.origin_class]).expect("one-element basis is non-empty");
        match OriginBoundAuthorityPolicy.authorize(
            candidate.class,
            candidate.memory_type,
            basis,
            &scope,
        ) {
            Ok(authorized) => {
                let content = memory_content(&candidate.content);
                distill_repo::insert_memory(
                    &mut txn,
                    distill.tenant_id,
                    evidence,
                    &NewMemory {
                        content: &content,
                        memory_type: candidate.memory_type,
                        class: authorized.0,
                        confidence: candidate.confidence,
                    },
                )
                .await?;
                inserted += 1;
            }
            Err(rejection) => {
                // ponytail: no metrics facility exists in this crate tree yet (grepped:
                // `memory_candidate_rejections` has no code consumer); this structured line IS
                // the counter until a §53 emitter lands — same name, same label.
                eprintln!(
                    "memory_candidate_rejections_total{{reason=\"{}\"}} 1 tenant={} evidence={} requested={:?} origin={:?}",
                    rejection_reason(rejection),
                    distill.tenant_id,
                    evidence.evidence_id,
                    candidate.class,
                    evidence.origin_class,
                );
                report.rejected += 1;
            }
        }
    }
    let output_digest: [u8; 32] = Sha256::digest(&inferred.output_bytes).into();
    distill_repo::finish_processing_run(
        &mut txn,
        inferred.processing_run_id,
        &output_digest,
        inserted,
        Some(&inferred.disclosure_id.to_string()),
    )
    .await?;
    if !distill_repo::complete_outbox(
        &mut txn,
        row.outbox_id,
        &distill.lease_owner,
        OutboxTerminal::Done,
    )
    .await?
    {
        txn.rollback().await?;
        report.lost_lease += 1;
        return Ok(());
    }
    txn.commit().await?;
    report.done += 1;
    report.memories += inserted as u32;
    Ok(())
}

/// (a)+(b): admission, evidence load, processing-run start (committed before the call), then
/// the provider round trip. `Ok(None)` = the Evidence is unavailable to this hop.
async fn infer_claimed(
    pool: &PrivateWorkerDbPool,
    reasoner: &DistillReasoner<'_>,
    distill: &DistillConfig,
    row: ClaimedEvidence,
) -> Result<Option<Inferred>, DistillError> {
    let contract = distill_prompt_contract();
    let mut txn = distill_repo::begin_read_context(pool, distill.tenant_id).await?;
    let (binding_id, binding_version) =
        distill_repo::resolve_distill_binding(&mut txn, distill.reasoning_domain_id)
            .await?
            .ok_or_else(|| PrivateReasoningError::new("distill route binding not found"))?;
    let admission = reasoner
        .admit(
            &mut txn,
            distill.tenant_id,
            distill.reasoning_domain_id,
            binding_id,
            binding_version,
        )
        .await?;
    let Some(evidence) = distill_repo::load_evidence(
        &mut txn,
        distill.tenant_id,
        distill.reasoning_domain_id,
        admission.owner_user_id,
        row.evidence_id,
    )
    .await?
    else {
        txn.rollback().await?;
        return Ok(None);
    };
    let context_snapshot_seq =
        distill_repo::context_snapshot_seq(&mut txn, distill.tenant_id).await?;

    // §16.1 evidence axis: the payload bytes as this run reads them (canonical jsonb
    // rendering — the raw remember bytes are not retained; their digest is stored verbatim in
    // `evidence_payload_sha256[]` below). Sole constructor `payload_sha256` (§48.0①).
    let payload_bytes = serde_json::to_vec(&evidence.payload)
        .map_err(|_| PrivateReasoningError::new("evidence payload serialization"))?;
    let evidence_hashes = [payload_sha256(&payload_bytes)];
    let prompt_hash = hex::encode(contract.sha256.0);
    let prompt_version = contract.version.to_string();
    let fingerprint = source_hash(&ProcessingInputFingerprintInputs {
        evidence_payload_sha256: &evidence_hashes,
        processor_kind: DISTILL_PROCESSOR_KIND,
        processor_version: DISTILL_PROCESSOR_VERSION,
        model_provider: &admission.locator.processor_id,
        model_id: &admission.locator.provider_model_id,
        model_revision: admission.locator.model_revision.as_deref().unwrap_or(""),
        prompt_version: &prompt_version,
        prompt_hash: &prompt_hash,
        embedding_version: None,
        parser_version: DISTILL_PARSER_VERSION,
        card_builder_version: None,
        context_snapshot_seq: u64::try_from(context_snapshot_seq).unwrap_or_default(),
    });
    let processing_run_id = distill_repo::start_processing_run(
        &mut txn,
        distill.tenant_id,
        &ProcessingRunStart {
            evidence_id: evidence.evidence_id,
            processor_kind: DISTILL_PROCESSOR_KIND,
            processor_version: DISTILL_PROCESSOR_VERSION,
            model_provider: &admission.locator.processor_id,
            model_id: &admission.locator.provider_model_id,
            model_revision: admission.locator.model_revision.as_deref().unwrap_or(""),
            prompt_version: &prompt_version,
            prompt_hash: &prompt_hash,
            parser_version: DISTILL_PARSER_VERSION,
            evidence_payload_sha256: vec![evidence.payload_sha256.clone()],
            source_hash: fingerprint.as_bytes(),
            context_snapshot_seq,
        },
    )
    .await?;
    txn.commit().await?;

    let envelope = DistillEnvelopeInput {
        origin_class: &evidence.origin_class_wire,
        max_class: evidence.origin_class.authority_ceiling(MemoryType::Fact),
        occurred_at: evidence.occurred_at,
        payload: &evidence.payload,
    };
    let principal = acting_user(&evidence, admission.owner_user_id);
    let acting = if principal == admission.owner_user_id
        || distill_repo::active_member(pool, distill.tenant_id, principal).await?
    {
        principal
    } else {
        admission.owner_user_id
    };
    let result = reasoner
        .infer(
            &admission,
            acting,
            evidence.evidence_id,
            DataClass::parse_or_secret(&evidence.data_class),
            &envelope,
        )
        .await?;
    Ok(Some(Inferred {
        evidence,
        processing_run_id,
        output_bytes: result.output_bytes,
        disclosure_id: result.disclosure_id,
    }))
}

/// Retryable attempt: the row goes back to PENDING (lease-fenced) and is claimed again next
/// pass — never FAILED, which nothing re-claims (`distill_repo::release_outbox`).
async fn settle_deferred(
    pool: &PrivateWorkerDbPool,
    distill: &DistillConfig,
    row: ClaimedEvidence,
    report: &mut DistillPassReport,
) -> Result<(), DistillError> {
    if distill_repo::release_outbox(pool, distill.tenant_id, row.outbox_id, &distill.lease_owner)
        .await?
    {
        report.deferred += 1;
    } else {
        report.lost_lease += 1;
    }
    Ok(())
}

/// Terminal, input-bound rejection: Evidence unavailable to this hop, or parser fail-closed.
async fn settle_failed(
    pool: &PrivateWorkerDbPool,
    distill: &DistillConfig,
    row: ClaimedEvidence,
    report: &mut DistillPassReport,
) -> Result<(), DistillError> {
    if distill_repo::fail_outbox(pool, distill.tenant_id, row.outbox_id, &distill.lease_owner)
        .await?
    {
        report.failed += 1;
    } else {
        report.lost_lease += 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_is_closed() {
        let ok = DistillConfig {
            tenant_id: Uuid::from_u128(1),
            reasoning_domain_id: Uuid::from_u128(2),
            batch: 10,
            lease_seconds: 30.0,
            lease_owner: "worker-a".into(),
        };
        assert!(ok.validate().is_ok());
        for bad in [
            DistillConfig {
                tenant_id: Uuid::nil(),
                ..ok.clone()
            },
            DistillConfig {
                batch: 0,
                ..ok.clone()
            },
            DistillConfig {
                lease_seconds: 0.0,
                ..ok.clone()
            },
            DistillConfig {
                lease_owner: " ".into(),
                ..ok.clone()
            },
        ] {
            assert!(bad.validate().is_err());
        }
    }

    #[test]
    fn memory_content_carries_the_card_fields() {
        let content = memory_content("New services must expose a health endpoint.");
        assert_eq!(
            content["key_claim"],
            "New services must expose a health endpoint."
        );
        assert_eq!(
            content["title"],
            "New services must expose a health endpoint."
        );
        let long = memory_content(&"x".repeat(200));
        assert_eq!(long["title"].as_str().map(str::len), Some(80));
    }

    #[test]
    fn rejection_reasons_match_the_metrics_registry_labels() {
        assert_eq!(
            rejection_reason(CandidateRejection::OriginAuthorityCeiling),
            "origin_authority_ceiling"
        );
        assert_eq!(
            rejection_reason(CandidateRejection::UntrustedInstruction),
            "untrusted_instruction"
        );
    }
}
