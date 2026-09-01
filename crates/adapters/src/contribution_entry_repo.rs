//! §12.1.1: authenticated private candidate storage and exact-byte manual finalization.
//!
//! Provider/scanner work happens outside the short input lock. Every durable step rechecks
//! the actual policy, membership, profile and source hashes. No public pool can read candidates.

use std::collections::BTreeSet;

use async_trait::async_trait;
use humaux_application::{
    consolidate::{
        ContentSha256, LogicalReasoningCallId, PrivateReasoningPurpose,
        SealedPrivateReasoningRequest, UserReasoningProfileVersion,
    },
    contribute::{
        AssessedContributionCandidatePort, AssessedStoredCandidate, ConfirmContribution,
        ContributionCandidateId, ContributionCandidatePort, ContributionGate,
        ContributionPreparationInput, ContributionReleaseId, ContributionSourceSnapshot,
        PreparationSnapshot, PrepareContribution, PublicCoverageDigest, StoredCandidate,
    },
};
use humaux_domain::{
    authority::{EvidenceId, MemoryId},
    error::ErrorCode,
    evidence::{EvidencePayloadSha256, payload_sha256},
    identity::{AuthorizationScope, VisibilityClass, VisibilityDescriptor, can_read},
    ids::{UserId, WorkspaceId},
    public::{ContributionPolicy, ReleaseSource, RightsProvenance},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{Row, types::Uuid};

use crate::{
    contribution_scan::ContributionScanReceipt,
    postgres::{PrivateWorkerDbPool, RuntimeDbPool},
    reasoning_route_admission::{
        ReasoningAdmissionError, ReasoningAdmissionLocator, resolve_user_reasoning_admission,
    },
    remember,
};

type Txn<'a> = sqlx::Transaction<'a, sqlx::Postgres>;

fn db_error(error: sqlx::Error) -> ErrorCode {
    match error {
        sqlx::Error::RowNotFound => ErrorCode::NotFound,
        sqlx::Error::Database(ref e) => match e.code().as_deref() {
            Some("42501") => ErrorCode::Forbidden,
            Some("23503") => ErrorCode::TenantBoundary,
            Some("23505" | "40001" | "40P01") => ErrorCode::Conflict,
            Some("23514" | "22023") => ErrorCode::InvalidInput,
            _ => ErrorCode::Internal,
        },
        _ => ErrorCode::DependencyUnavailable,
    }
}

pub(crate) async fn scope(txn: &mut Txn<'_>, auth: &AuthorizationScope) -> Result<(), ErrorCode> {
    let user = auth.user_id().ok_or(ErrorCode::Unauthorized)?;
    if auth.principal().0 != user.0 {
        return Err(ErrorCode::Forbidden);
    }
    sqlx::query(
        "SELECT set_config('humaux.tenant_id',$1,true), set_config('humaux.user_id',$2,true)",
    )
    .bind(auth.tenant_id().0.to_string())
    .bind(user.0.to_string())
    .execute(&mut **txn)
    .await
    .map_err(db_error)?;
    sqlx::query("SELECT ops.lock_contribution_inputs()")
        .execute(&mut **txn)
        .await
        .map_err(db_error)?;
    let active: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM control.memberships m JOIN control.users u USING(user_id) \
         JOIN control.tenants t USING(tenant_id) WHERE m.tenant_id=$1 AND m.user_id=$2 \
         AND m.state='ACTIVE' AND u.state='ACTIVE' AND t.state='ACTIVE')",
    )
    .bind(auth.tenant_id().0)
    .bind(user.0)
    .fetch_one(&mut **txn)
    .await
    .map_err(db_error)?;
    if !active {
        return Err(ErrorCode::Forbidden);
    }
    Ok(())
}

fn source_key(source: ReleaseSource) -> (&'static str, Uuid) {
    match source {
        ReleaseSource::Evidence(id) => ("e", id.0),
        ReleaseSource::Memory(id) => ("m", id.0),
    }
}

// §12.1.1 / migration 0107: this serialization is also checked by the DB's deferred seal.
fn manifest(sources: &[ContributionSourceSnapshot]) -> ContentSha256 {
    let bytes = sources
        .iter()
        .map(|s| {
            let (kind, id) = source_key(s.source);
            format!("{kind}:{id}:{}", hex::encode(s.current_hash.0))
        })
        .collect::<Vec<_>>()
        .join("|");
    ContentSha256(Sha256::digest(bytes.as_bytes()).into())
}

/// Re-read the exact, probe-scoped public snapshot inside finalization's transaction.
///
/// The coverage function derives its UUID from the selected public objects and their revisions.
/// Requiring this whole binding to match is a compare-and-swap policy: a public change that
/// fills the assessed gap makes the pre-confirmation assessment stale and fail closed.
async fn current_coverage_binding(
    txn: &mut Txn<'_>,
    probe: &[u8],
) -> Result<(Uuid, i32, Vec<u8>), ErrorCode> {
    let rows = sqlx::query(
        "SELECT snapshot_id,coverage_version,summary \
         FROM public.phase9_public_coverage_for_probe($1,$2)",
    )
    .bind(probe)
    .bind(32_i32)
    .fetch_all(&mut **txn)
    .await
    .map_err(db_error)?;
    let first = rows.first().ok_or(ErrorCode::NotFound)?;
    let snapshot_id: Uuid = first.try_get("snapshot_id").map_err(db_error)?;
    let coverage_version: i32 = first.try_get("coverage_version").map_err(db_error)?;
    let coverage_version_u32 =
        u32::try_from(coverage_version).map_err(|_| ErrorCode::InvalidInput)?;
    let mut summaries = Vec::with_capacity(rows.len());
    for row in rows {
        if row.try_get::<Uuid, _>("snapshot_id").map_err(db_error)? != snapshot_id
            || row
                .try_get::<i32, _>("coverage_version")
                .map_err(db_error)?
                != coverage_version
        {
            return Err(ErrorCode::Conflict);
        }
        if let Some(summary) = row
            .try_get::<Option<String>, _>("summary")
            .map_err(db_error)?
        {
            summaries.push(summary);
        }
    }
    let digest = PublicCoverageDigest::new(snapshot_id, coverage_version_u32, summaries)?;
    Ok((
        snapshot_id,
        coverage_version,
        digest.binding().digest_sha256().0.to_vec(),
    ))
}

fn visible(row: &sqlx::postgres::PgRow, auth: &AuthorizationScope) -> Result<bool, ErrorCode> {
    let class: String = row.try_get("visibility_class").map_err(db_error)?;
    let class = match class.as_str() {
        "TENANT_SHARED" => VisibilityClass::TenantShared,
        "USER_PRIVATE" => VisibilityClass::UserPrivate,
        "WORKSPACE_SHARED" => VisibilityClass::WorkspaceShared,
        _ => return Err(ErrorCode::InvalidInput),
    };
    Ok(can_read(
        auth,
        &VisibilityDescriptor {
            class,
            user_id: row
                .try_get::<Option<Uuid>, _>("visibility_user_id")
                .map_err(db_error)?
                .map(UserId),
            workspace_id: row
                .try_get::<Option<Uuid>, _>("visibility_workspace_id")
                .map_err(db_error)?
                .map(WorkspaceId),
        },
    ))
}

async fn load_execution(
    txn: &mut Txn<'_>,
    request: &ContributionPreparationInput,
) -> Result<PreparationSnapshot, ErrorCode> {
    load_execution_with_admission(txn, request)
        .await
        .map(|(snapshot, _)| snapshot)
}

/// Loads the complete trusted execution preparation inside a caller-owned transaction.
///
/// The transactional production ingress uses this seam so current authorization, policy,
/// rights, route admission, and source hashes cannot change between preparation and enqueue.
pub(crate) async fn load_execution_preparation_in_txn(
    txn: &mut Txn<'_>,
    request: &ContributionPreparationInput,
) -> Result<PreparationSnapshot, ErrorCode> {
    scope(txn, &request.authorization).await?;
    load_execution(txn, request).await
}

#[allow(clippy::too_many_lines)] // Keep the complete source/authorization snapshot in one transaction.
pub(crate) async fn load_with_admission(
    txn: &mut Txn<'_>,
    request: &PrepareContribution,
) -> Result<(PreparationSnapshot, ReasoningAdmissionLocator), ErrorCode> {
    load_execution_with_admission(txn, &ContributionPreparationInput::from(request)).await
}

async fn load_execution_with_admission(
    txn: &mut Txn<'_>,
    request: &ContributionPreparationInput,
) -> Result<(PreparationSnapshot, ReasoningAdmissionLocator), ErrorCode> {
    let admission = resolve_user_reasoning_admission(
        txn,
        request.binding_id,
        request.binding_version,
        request.reasoning_domain_id,
        PrivateReasoningPurpose::ContributionDeidentify,
    )
    .await
    .map_err(|error| match error {
        ReasoningAdmissionError::Database => ErrorCode::DependencyUnavailable,
        ReasoningAdmissionError::InvalidLocator => ErrorCode::Conflict,
    })?
    .ok_or(ErrorCode::Forbidden)?;
    if admission.tenant_id != request.authorization.tenant_id().0 {
        return Err(ErrorCode::TenantBoundary);
    }
    let snapshot = load_current_with_profile(
        txn,
        request,
        UserReasoningProfileVersion(admission.profile_version),
    )
    .await?;
    Ok((snapshot, admission))
}

#[allow(clippy::too_many_lines)] // Current source/rights recheck, deliberately no route resolver.
async fn load_current_with_profile(
    txn: &mut Txn<'_>,
    request: &ContributionPreparationInput,
    resolved_profile_version: UserReasoningProfileVersion,
) -> Result<PreparationSnapshot, ErrorCode> {
    let auth = &request.authorization;
    let user = auth.user_id().ok_or(ErrorCode::Unauthorized)?;
    if request.requested_sources.is_empty() {
        return Err(ErrorCode::InvalidInput);
    }
    let domain_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM control.private_reasoning_domains d \
         WHERE d.tenant_id=$1 AND d.owner_user_id=$2 AND d.reasoning_domain_id=$3 \
         AND d.status='ACTIVE'",
    )
    .bind(auth.tenant_id().0)
    .bind(user.0)
    .bind(request.reasoning_domain_id.0)
    .fetch_one(&mut **txn)
    .await
    .map_err(db_error)?;
    if domain_count == 0 {
        return Err(ErrorCode::Forbidden);
    }
    if domain_count != 1 {
        return Err(ErrorCode::Conflict);
    }
    let policy = sqlx::query(
        "SELECT policy_id,policy_version,rights_basis,source_license,publisher,contributor_attestation,redistribution_policy \
         FROM control.contribution_policies WHERE tenant_id=$1 AND effective_to IS NULL \
         AND allow_public_contribution AND contribution_mode='MANUAL'")
        .bind(auth.tenant_id().0).fetch_optional(&mut **txn).await.map_err(db_error)?
        .ok_or(ErrorCode::Forbidden)?;
    let rights = RightsProvenance::new(
        policy
            .try_get::<Option<String>, _>("rights_basis")
            .map_err(db_error)?
            .ok_or(ErrorCode::Forbidden)?,
        policy.try_get("source_license").map_err(db_error)?,
        policy.try_get("publisher").map_err(db_error)?,
        policy
            .try_get("contributor_attestation")
            .map_err(db_error)?,
        policy.try_get("redistribution_policy").map_err(db_error)?,
    )
    .map_err(|_| ErrorCode::Forbidden)?;
    let mut keys = BTreeSet::new();
    for source in &request.requested_sources {
        if !keys.insert(source_key(*source)) {
            return Err(ErrorCode::InvalidInput);
        }
    }
    let mut sources = Vec::with_capacity(keys.len());
    for (kind, id) in keys {
        let row = if kind == "e" {
            sqlx::query("SELECT payload_sha256 AS hash,visibility_class,visibility_user_id,visibility_workspace_id \
             FROM private.evidence_objects WHERE tenant_id=$1 AND evidence_id=$2 AND reasoning_domain_id=$3")
        } else {
            sqlx::query("SELECT sha256(convert_to(m.content::text,'UTF8')) AS hash,m.visibility_class,m.visibility_user_id,m.visibility_workspace_id \
             FROM private.memory_records m WHERE m.tenant_id=$1 AND m.memory_id=$2 AND m.status='active' \
             AND EXISTS(SELECT 1 FROM private.memory_evidence me WHERE me.memory_id=m.memory_id) \
             AND NOT EXISTS(SELECT 1 FROM private.memory_evidence me LEFT JOIN private.evidence_objects e ON e.evidence_id=me.evidence_id \
               WHERE me.memory_id=m.memory_id AND (e.evidence_id IS NULL OR e.reasoning_domain_id IS DISTINCT FROM $3))")
        }.bind(auth.tenant_id().0).bind(id).bind(request.reasoning_domain_id.0)
          .fetch_optional(&mut **txn).await.map_err(db_error)?.ok_or(ErrorCode::Forbidden)?;
        if !visible(&row, auth)? {
            return Err(ErrorCode::Forbidden);
        }
        if kind == "m" {
            let backing = sqlx::query(
                "SELECT e.visibility_class,e.visibility_user_id,e.visibility_workspace_id \
                FROM private.memory_evidence me JOIN private.evidence_objects e USING(evidence_id) \
                WHERE me.memory_id=$1 AND e.tenant_id=$2",
            )
            .bind(id)
            .bind(auth.tenant_id().0)
            .fetch_all(&mut **txn)
            .await
            .map_err(db_error)?;
            for evidence in backing {
                if !visible(&evidence, auth)? {
                    return Err(ErrorCode::Forbidden);
                }
            }
        }
        let hash: Vec<u8> = row.try_get("hash").map_err(db_error)?;
        sources.push(ContributionSourceSnapshot {
            source: if kind == "e" {
                ReleaseSource::Evidence(EvidenceId(id))
            } else {
                ReleaseSource::Memory(MemoryId(id))
            },
            current_hash: ContentSha256(hash.try_into().map_err(|_| ErrorCode::InvalidInput)?),
        });
    }
    let source_manifest_hash = manifest(&sources);
    let snapshot = PreparationSnapshot {
        authorization: auth.clone(),
        sources,
        source_manifest_hash,
        policy_id: policy.try_get("policy_id").map_err(db_error)?,
        policy_version: policy.try_get("policy_version").map_err(db_error)?,
        policy: ContributionPolicy::Manual,
        rights,
        reasoning: SealedPrivateReasoningRequest {
            reasoning_domain_id: request.reasoning_domain_id,
            binding_id: request.binding_id,
            binding_version: request.binding_version,
            input_manifest_hash: source_manifest_hash,
            purpose: PrivateReasoningPurpose::ContributionDeidentify,
            contribution_attempt: None,
        },
        resolved_profile_version,
    };
    Ok(snapshot)
}

async fn load_legacy_current_with_profile(
    txn: &mut Txn<'_>,
    request: &PrepareContribution,
    resolved_profile_version: UserReasoningProfileVersion,
) -> Result<PreparationSnapshot, ErrorCode> {
    load_current_with_profile(
        txn,
        &ContributionPreparationInput::from(request),
        resolved_profile_version,
    )
    .await
}

async fn successful_reasoning_profile(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
    model_call_id: Uuid,
    request: &PrepareContribution,
) -> Result<UserReasoningProfileVersion, ErrorCode> {
    let profile_version: Option<i64> = sqlx::query_scalar(
        "SELECT profile_version FROM ops.model_call_ledger WHERE tenant_id=$1 AND model_call_id=$2 AND status='SUCCEEDED' AND purpose='CONTRIBUTION_DEIDENTIFY' AND call_kind='TYPED_ASSESSMENT' AND reasoning_domain_id=$3 AND binding_id=$4 AND binding_version=$5",
    )
    .bind(tenant_id)
    .bind(model_call_id)
    .bind(request.reasoning_domain_id.0)
    .bind(request.binding_id.0)
    .bind(request.binding_version.0)
    .fetch_optional(&mut **txn)
    .await
    .map_err(db_error)?;
    let profile_version = profile_version.ok_or(ErrorCode::Conflict)?;
    if profile_version <= 0 {
        return Err(ErrorCode::Conflict);
    }
    Ok(UserReasoningProfileVersion(profile_version))
}

/// The private worker's candidate port. It exposes no raw pool or public DB capability.
pub struct ContributionEntryRepo<'a> {
    pool: &'a PrivateWorkerDbPool,
}

impl<'a> ContributionEntryRepo<'a> {
    /// Binds the port to a checked private-worker pool.
    pub fn new(pool: &'a PrivateWorkerDbPool) -> Self {
        Self { pool }
    }
}

// The assessed path needs candidate plus assessment in one DB transaction.  This is the same
// storage invariant as the legacy port, kept here until that older port can be contract-dropped.
async fn store_assessed_candidate_in_txn(
    txn: &mut Txn<'_>,
    candidate: StoredCandidate<'_, ContributionScanReceipt>,
) -> Result<ContributionCandidateId, ErrorCode> {
    let saved = candidate.preparation;
    scope(txn, &saved.authorization).await?;
    let current_request = PrepareContribution {
        authorization: saved.authorization.clone(),
        requested_sources: saved.sources.iter().map(|s| s.source).collect(),
        reasoning_domain_id: saved.reasoning.reasoning_domain_id,
        binding_id: saved.reasoning.binding_id,
        binding_version: saved.reasoning.binding_version,
        coverage_probe_call_id: LogicalReasoningCallId(candidate.model_call_id),
        assessment_call_id: LogicalReasoningCallId(candidate.model_call_id),
    };
    let receipt_profile = successful_reasoning_profile(
        txn,
        saved.authorization.tenant_id().0,
        candidate.model_call_id,
        &current_request,
    )
    .await?;
    let current = load_legacy_current_with_profile(txn, &current_request, receipt_profile).await?;
    if current.sources != saved.sources
        || current.source_manifest_hash != saved.source_manifest_hash
        || current.policy_id != saved.policy_id
        || current.policy_version != saved.policy_version
        || current.rights != saved.rights
        || current.policy != saved.policy
        || current.reasoning != saved.reasoning
        || current.resolved_profile_version != saved.resolved_profile_version
        || saved.reasoning.input_manifest_hash != saved.source_manifest_hash
        || saved.reasoning.purpose != PrivateReasoningPurpose::ContributionDeidentify
        || candidate.disclosed_bytes.is_empty()
        || candidate.provider_trace.0.trim().is_empty()
        || candidate.provider_trace.0 != candidate.model_call_id.to_string()
        || payload_sha256(candidate.disclosed_bytes) != candidate.payload_sha256
        || candidate.scan_receipt.payload_sha256() != candidate.payload_sha256
    {
        return Err(ErrorCode::Conflict);
    }
    std::str::from_utf8(candidate.disclosed_bytes).map_err(|_| ErrorCode::InvalidInput)?;
    let receipt = candidate.scan_receipt;
    let scan = json!({"privacy_rules_version":receipt.privacy_rules_version(),
        "privacy_rules_digest":receipt.privacy_rules_digest(),"gitleaks_version":receipt.gitleaks_version(),
        "gitleaks_binary_sha256":receipt.gitleaks_binary_sha256()});
    let policy = json!({"policy":"MANUAL","consent_version":format!("{}:{}",current.policy_id,current.policy_version),
        "policy_id":current.policy_id.to_string(),"policy_version":current.policy_version,
        "principal_id":saved.authorization.principal().0.to_string(),
        "allowed_workspace_ids":saved.authorization.allowed_workspace_ids().iter().map(|id|id.0.to_string()).collect::<Vec<_>>()});
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO staging.contribution_candidates(tenant_id,user_id,policy_id,policy_version,policy_snapshot, \
         reasoning_domain_id,profile_version,binding_id,binding_version,model_call_id,source_manifest_hash,source_count,disclosed_payload,disclosed_payload_sha256, \
         provider_trace,scan_receipt,rights_basis,source_license,publisher,contributor_attestation,redistribution_policy) \
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21) RETURNING candidate_id",
    )
    .bind(saved.authorization.tenant_id().0)
    .bind(saved.authorization.user_id().ok_or(ErrorCode::Unauthorized)?.0)
    .bind(current.policy_id)
    .bind(current.policy_version)
    .bind(policy)
    .bind(current.reasoning.reasoning_domain_id.0)
    .bind(current.resolved_profile_version.0)
    .bind(current.reasoning.binding_id.0)
    .bind(current.reasoning.binding_version.0)
    .bind(candidate.model_call_id)
    .bind(current.source_manifest_hash.0.to_vec())
    .bind(i32::try_from(current.sources.len()).map_err(|_| ErrorCode::InvalidInput)?)
    .bind(candidate.disclosed_bytes)
    .bind(hex::decode(candidate.payload_sha256.to_hex()).map_err(|_| ErrorCode::Internal)?)
    .bind(&candidate.provider_trace.0)
    .bind(scan)
    .bind(current.rights.rights_basis())
    .bind(current.rights.source_license())
    .bind(current.rights.publisher())
    .bind(current.rights.contributor_attestation())
    .bind(current.rights.redistribution_policy())
    .fetch_one(&mut **txn)
    .await
    .map_err(db_error)?;
    for (ordinal, source) in current.sources.iter().enumerate() {
        let (evidence_id, memory_id) = match source.source {
            ReleaseSource::Evidence(id) => (Some(id.0), None),
            ReleaseSource::Memory(id) => (None, Some(id.0)),
        };
        sqlx::query(
            "INSERT INTO staging.contribution_candidate_sources(tenant_id,candidate_id,evidence_id,memory_id,source_hash,ordinal) \
             VALUES($1,$2,$3,$4,$5,$6)",
        )
        .bind(saved.authorization.tenant_id().0)
        .bind(id)
        .bind(evidence_id)
        .bind(memory_id)
        .bind(source.current_hash.0.to_vec())
        .bind(i32::try_from(ordinal).map_err(|_| ErrorCode::InvalidInput)?)
        .execute(&mut **txn)
        .await
        .map_err(db_error)?;
    }
    Ok(ContributionCandidateId(id))
}

#[allow(clippy::too_many_lines)] // Candidate store/finalize keep each current-state recheck and write in one transaction.
#[async_trait]
impl ContributionCandidatePort for ContributionEntryRepo<'_> {
    type ScanReceipt = ContributionScanReceipt;

    async fn load_execution_preparation(
        &self,
        request: &ContributionPreparationInput,
    ) -> Result<PreparationSnapshot, ErrorCode> {
        let mut txn = self.pool.pool().begin().await.map_err(db_error)?;
        let result = load_execution_preparation_in_txn(&mut txn, request).await?;
        txn.commit().await.map_err(db_error)?;
        Ok(result)
    }

    async fn store_prepared(
        &self,
        candidate: StoredCandidate<'_, Self::ScanReceipt>,
    ) -> Result<ContributionCandidateId, ErrorCode> {
        let saved = candidate.preparation;
        let mut txn = self.pool.pool().begin().await.map_err(db_error)?;
        scope(&mut txn, &saved.authorization).await?;
        let current_request = PrepareContribution {
            authorization: saved.authorization.clone(),
            requested_sources: saved.sources.iter().map(|s| s.source).collect(),
            reasoning_domain_id: saved.reasoning.reasoning_domain_id,
            binding_id: saved.reasoning.binding_id,
            binding_version: saved.reasoning.binding_version,
            coverage_probe_call_id: LogicalReasoningCallId(candidate.model_call_id),
            assessment_call_id: LogicalReasoningCallId(candidate.model_call_id),
        };
        let receipt_profile = successful_reasoning_profile(
            &mut txn,
            saved.authorization.tenant_id().0,
            candidate.model_call_id,
            &current_request,
        )
        .await?;
        let current =
            load_legacy_current_with_profile(&mut txn, &current_request, receipt_profile).await?;
        if current.sources != saved.sources
            || current.source_manifest_hash != saved.source_manifest_hash
            || current.policy_id != saved.policy_id
            || current.policy_version != saved.policy_version
            || current.rights != saved.rights
            || current.policy != saved.policy
            || current.reasoning != saved.reasoning
            || current.resolved_profile_version != saved.resolved_profile_version
            || saved.reasoning.input_manifest_hash != saved.source_manifest_hash
            || saved.reasoning.purpose != PrivateReasoningPurpose::ContributionDeidentify
            || candidate.disclosed_bytes.is_empty()
            || candidate.provider_trace.0.trim().is_empty()
            || candidate.provider_trace.0 != candidate.model_call_id.to_string()
            || payload_sha256(candidate.disclosed_bytes) != candidate.payload_sha256
            || candidate.scan_receipt.payload_sha256() != candidate.payload_sha256
        {
            return Err(ErrorCode::Conflict);
        }
        // Valid UTF-8 is part of the preview/confirmation contract; never lossy-convert bytes.
        std::str::from_utf8(candidate.disclosed_bytes).map_err(|_| ErrorCode::InvalidInput)?;
        let receipt = candidate.scan_receipt;
        let scan = json!({"privacy_rules_version":receipt.privacy_rules_version(),
            "privacy_rules_digest":receipt.privacy_rules_digest(),"gitleaks_version":receipt.gitleaks_version(),
            "gitleaks_binary_sha256":receipt.gitleaks_binary_sha256()});
        let policy = json!({"policy":"MANUAL","consent_version":format!("{}:{}",current.policy_id,current.policy_version),
            "policy_id":current.policy_id.to_string(),"policy_version":current.policy_version,
            "principal_id":saved.authorization.principal().0.to_string(),
            "allowed_workspace_ids":saved.authorization.allowed_workspace_ids().iter().map(|id|id.0.to_string()).collect::<Vec<_>>()});
        let id:Uuid=sqlx::query_scalar(
            "INSERT INTO staging.contribution_candidates(tenant_id,user_id,policy_id,policy_version,policy_snapshot, \
             reasoning_domain_id,profile_version,binding_id,binding_version,model_call_id,source_manifest_hash,source_count,disclosed_payload,disclosed_payload_sha256, \
             provider_trace,scan_receipt,rights_basis,source_license,publisher,contributor_attestation,redistribution_policy) \
             VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21) RETURNING candidate_id")
            .bind(saved.authorization.tenant_id().0).bind(saved.authorization.user_id().ok_or(ErrorCode::Unauthorized)?.0)
            .bind(current.policy_id).bind(current.policy_version).bind(policy)
            .bind(current.reasoning.reasoning_domain_id.0).bind(current.resolved_profile_version.0)
            .bind(current.reasoning.binding_id.0).bind(current.reasoning.binding_version.0)
            .bind(candidate.model_call_id)
            .bind(current.source_manifest_hash.0.to_vec()).bind(i32::try_from(current.sources.len()).map_err(|_|ErrorCode::InvalidInput)?)
            .bind(candidate.disclosed_bytes).bind(hex::decode(candidate.payload_sha256.to_hex()).map_err(|_|ErrorCode::Internal)?)
            .bind(&candidate.provider_trace.0).bind(scan).bind(current.rights.rights_basis())
            .bind(current.rights.source_license()).bind(current.rights.publisher())
            .bind(current.rights.contributor_attestation()).bind(current.rights.redistribution_policy())
            .fetch_one(&mut *txn).await.map_err(db_error)?;
        for (ordinal, source) in current.sources.iter().enumerate() {
            let (e, m) = match source.source {
                ReleaseSource::Evidence(id) => (Some(id.0), None),
                ReleaseSource::Memory(id) => (None, Some(id.0)),
            };
            sqlx::query("INSERT INTO staging.contribution_candidate_sources(tenant_id,candidate_id,evidence_id,memory_id,source_hash,ordinal) \
                         VALUES($1,$2,$3,$4,$5,$6)")
                .bind(saved.authorization.tenant_id().0).bind(id).bind(e).bind(m).bind(source.current_hash.0.to_vec())
                .bind(i32::try_from(ordinal).map_err(|_|ErrorCode::InvalidInput)?)
                .execute(&mut *txn).await.map_err(db_error)?;
        }
        txn.commit().await.map_err(db_error)?;
        Ok(ContributionCandidateId(id))
    }

    async fn finalize_confirmed(
        &self,
        confirmation: ConfirmContribution,
    ) -> Result<ContributionReleaseId, ErrorCode> {
        let auth = &confirmation.authorization;
        let mut txn = self.pool.pool().begin().await.map_err(db_error)?;
        scope(&mut txn, auth).await?;
        let candidate =
            sqlx::query("SELECT * FROM staging.contribution_candidates WHERE candidate_id=$1")
                .bind(confirmation.candidate_id.0)
                .fetch_optional(&mut *txn)
                .await
                .map_err(db_error)?
                .ok_or(ErrorCode::NotFound)?;
        let actual: Vec<u8> = candidate
            .try_get("disclosed_payload_sha256")
            .map_err(db_error)?;
        if hex::encode(&actual) != confirmation.candidate_payload_sha256.to_hex() {
            return Err(ErrorCode::Conflict);
        }
        let existing:Option<Uuid>=sqlx::query_scalar("SELECT contribution_release_id FROM staging.contribution_releases WHERE candidate_id=$1")
            .bind(confirmation.candidate_id.0).fetch_optional(&mut *txn).await.map_err(db_error)?;
        if let Some(id) = existing {
            // A replay returns the historical identity; it never reactivates a revoked release.
            txn.commit().await.map_err(db_error)?;
            return Ok(ContributionReleaseId(id));
        }
        let rows=sqlx::query("SELECT evidence_id,memory_id FROM staging.contribution_candidate_sources WHERE candidate_id=$1 ORDER BY ordinal")
            .bind(confirmation.candidate_id.0).fetch_all(&mut *txn).await.map_err(db_error)?;
        let mut requested = Vec::with_capacity(rows.len());
        for row in &rows {
            let e: Option<Uuid> = row.try_get("evidence_id").map_err(db_error)?;
            let m: Option<Uuid> = row.try_get("memory_id").map_err(db_error)?;
            requested.push(match (e, m) {
                (Some(id), None) => ReleaseSource::Evidence(EvidenceId(id)),
                (None, Some(id)) => ReleaseSource::Memory(MemoryId(id)),
                _ => return Err(ErrorCode::InvalidInput),
            });
        }
        let model_call_id = candidate
            .try_get::<Option<Uuid>, _>("model_call_id")
            .map_err(db_error)?
            .ok_or(ErrorCode::Conflict)?;
        let current_request = PrepareContribution {
            authorization: auth.clone(),
            requested_sources: requested,
            reasoning_domain_id: humaux_application::consolidate::PrivateReasoningDomainId(
                candidate.try_get("reasoning_domain_id").map_err(db_error)?,
            ),
            binding_id: humaux_application::consolidate::ReasoningRouteBindingId(
                candidate
                    .try_get::<Option<Uuid>, _>("binding_id")
                    .map_err(db_error)?
                    .ok_or(ErrorCode::Conflict)?,
            ),
            binding_version: humaux_application::consolidate::ReasoningRouteBindingVersion(
                candidate
                    .try_get::<Option<i64>, _>("binding_version")
                    .map_err(db_error)?
                    .ok_or(ErrorCode::Conflict)?,
            ),
            coverage_probe_call_id: LogicalReasoningCallId(model_call_id),
            assessment_call_id: LogicalReasoningCallId(model_call_id),
        };
        let receipt_profile = successful_reasoning_profile(
            &mut txn,
            auth.tenant_id().0,
            model_call_id,
            &current_request,
        )
        .await?;
        let current =
            load_legacy_current_with_profile(&mut txn, &current_request, receipt_profile).await?;
        let old_manifest: Vec<u8> = candidate
            .try_get("source_manifest_hash")
            .map_err(db_error)?;
        let old_version: i64 = candidate.try_get("policy_version").map_err(db_error)?;
        let old_profile_version: i64 = candidate.try_get("profile_version").map_err(db_error)?;
        if current.source_manifest_hash.0.as_slice() != old_manifest
            || current.policy_version != old_version
            || current.resolved_profile_version.0 != old_profile_version
        {
            return Err(ErrorCode::Conflict);
        }
        let assessment = sqlx::query(
            "SELECT probe_bytes,probe_sha256,probe_source_manifest_hash,coverage_digest_id, \
                    coverage_version,coverage_digest_sha256,candidate_payload_sha256,assessment_digest, \
                    assessment_receipt \
             FROM staging.contribution_candidate_phase9_assessments WHERE candidate_id=$1",
        )
        .bind(confirmation.candidate_id.0)
        .fetch_optional(&mut *txn)
        .await
        .map_err(db_error)?;
        let assessment_digest = if let Some(assessment) = assessment {
            let probe_bytes: Vec<u8> = assessment.try_get("probe_bytes").map_err(db_error)?;
            let probe_sha256: Vec<u8> = assessment.try_get("probe_sha256").map_err(db_error)?;
            let probe_manifest: Vec<u8> = assessment
                .try_get("probe_source_manifest_hash")
                .map_err(db_error)?;
            let candidate_sha256: Vec<u8> = assessment
                .try_get("candidate_payload_sha256")
                .map_err(db_error)?;
            let receipt: serde_json::Value =
                assessment.try_get("assessment_receipt").map_err(db_error)?;
            let (current_id, current_version, current_sha256) =
                current_coverage_binding(&mut txn, &probe_bytes).await?;
            let stored_id: Uuid = assessment.try_get("coverage_digest_id").map_err(db_error)?;
            let stored_version: i32 = assessment.try_get("coverage_version").map_err(db_error)?;
            let stored_sha256: Vec<u8> = assessment
                .try_get("coverage_digest_sha256")
                .map_err(db_error)?;
            if hex::encode(&probe_sha256) != payload_sha256(&probe_bytes).to_hex()
                || probe_manifest != old_manifest
                || candidate_sha256 != actual
                || stored_id != current_id
                || stored_version != current_version
                || stored_sha256 != current_sha256
                || receipt
                    .get("probe_sha256")
                    .and_then(serde_json::Value::as_str)
                    != Some(&hex::encode(&probe_sha256))
                || receipt
                    .get("coverage_digest_id")
                    .and_then(serde_json::Value::as_str)
                    != Some(&stored_id.to_string())
                || receipt
                    .get("coverage_version")
                    .and_then(serde_json::Value::as_str)
                    != Some(&stored_version.to_string())
                || receipt
                    .get("coverage_digest_sha256")
                    .and_then(serde_json::Value::as_str)
                    != Some(&hex::encode(&stored_sha256))
                || receipt
                    .get("candidate_payload_sha256")
                    .and_then(serde_json::Value::as_str)
                    != Some(&hex::encode(&candidate_sha256))
                || !["novelty", "quality", "generality", "grounding"]
                    .iter()
                    .all(|gate| {
                        receipt.get(*gate).and_then(serde_json::Value::as_str) == Some("PASS")
                    })
            {
                return Err(ErrorCode::Conflict);
            }
            Some(
                assessment
                    .try_get::<Vec<u8>, _>("assessment_digest")
                    .map_err(db_error)?,
            )
        } else {
            None
        };
        let id:Uuid=sqlx::query_scalar(
            "INSERT INTO staging.contribution_releases(tenant_id,policy_snapshot,privacy_scan_outcome,secret_scan_outcome, \
             rights_basis,source_license,publisher,contributor_attestation,redistribution_policy,candidate_id,confirmation_id, \
             disclosed_payload,disclosed_payload_sha256,scan_receipt) \
             SELECT tenant_id,policy_snapshot,'PASSED','PASSED',rights_basis,source_license,publisher,contributor_attestation, \
             redistribution_policy,candidate_id,$2,disclosed_payload,disclosed_payload_sha256,scan_receipt \
             FROM staging.contribution_candidates WHERE candidate_id=$1 RETURNING contribution_release_id")
            .bind(confirmation.candidate_id.0).bind(confirmation.confirmation_id)
            .fetch_one(&mut *txn).await.map_err(db_error)?;
        sqlx::query("INSERT INTO staging.contribution_release_sources(tenant_id,contribution_release_id,evidence_id,memory_id,ordinal) \
             SELECT tenant_id,$2,evidence_id,memory_id,ordinal FROM staging.contribution_candidate_sources WHERE candidate_id=$1")
            .bind(confirmation.candidate_id.0).bind(id).execute(&mut *txn).await.map_err(db_error)?;
        let anonymous_dispatch = if let Some(assessment_digest) = assessment_digest {
            let anonymous_source_id: Uuid = sqlx::query_scalar(
                "INSERT INTO control.anonymous_source_lineage(contribution_release_id,tenant_id) \
                 VALUES($1,$2) RETURNING anonymous_source_id",
            )
            .bind(id)
            .bind(auth.tenant_id().0)
            .fetch_one(&mut *txn)
            .await
            .map_err(db_error)?;
            let envelope_sha256: Vec<u8> = sqlx::query_scalar(
                "WITH payload AS ( \
                   SELECT tenant_id, \
                          jsonb_build_object('text', convert_from(disclosed_payload,'UTF8')) AS sanitized_content, \
                          policy_version::text AS policy_version, \
                          sha256(convert_to(policy_snapshot::text,'UTF8')) AS policy_digest \
                   FROM staging.contribution_candidates \
                   WHERE candidate_id=$1 \
                 ) \
                 INSERT INTO staging.sanitized_public_candidates( \
                   tenant_id,anonymous_source_id,sanitized_content,content_sha256,policy_version, \
                   policy_digest,assessment_outcome,assessment_digest,envelope_sha256) \
                 SELECT payload.tenant_id,$2,payload.sanitized_content, \
                   sha256(convert_to(payload.sanitized_content::text,'UTF8')), \
                   payload.policy_version,payload.policy_digest,'PASSED',$3, \
                   sha256( \
                     sha256(convert_to(payload.sanitized_content::text,'UTF8')) \
                     || payload.policy_digest \
                     || $3 \
                     || convert_to(payload.policy_version||':PASSED','UTF8')) \
                 FROM payload RETURNING envelope_sha256",
            )
            .bind(confirmation.candidate_id.0)
            .bind(anonymous_source_id)
            .bind(assessment_digest)
            .fetch_one(&mut *txn)
            .await
            .map_err(db_error)?;
            Some((anonymous_source_id, envelope_sha256))
        } else {
            None
        };
        let seq = remember::next_commit_seq(&mut txn)
            .await
            .map_err(|_| ErrorCode::DependencyUnavailable)?;
        if let Some((anonymous_source_id, envelope_sha256)) = anonymous_dispatch {
            sqlx::query(
                "INSERT INTO ops.outbox(tenant_id,commit_seq,event_type,anonymous_source_id,candidate_envelope_sha256,anonymous_source_revision) \
                 VALUES($1,$2,'PUBLIC_ANONYMOUS_RELEASE',$3,$4,1)",
            )
            .bind(auth.tenant_id().0)
            .bind(seq)
            .bind(anonymous_source_id)
            .bind(envelope_sha256)
            .execute(&mut *txn)
            .await
            .map_err(db_error)?;
        } else {
            sqlx::query(
                "INSERT INTO ops.outbox(tenant_id,commit_seq,event_type,contribution_release_id) \
                 VALUES($1,$2,'PUBLIC_RELEASE',$3)",
            )
            .bind(auth.tenant_id().0)
            .bind(seq)
            .bind(id)
            .execute(&mut *txn)
            .await
            .map_err(db_error)?;
        }
        txn.commit().await.map_err(db_error)?;
        Ok(ContributionReleaseId(id))
    }
}

#[async_trait]
impl AssessedContributionCandidatePort for ContributionEntryRepo<'_> {
    async fn store_assessed_prepared(
        &self,
        assessed: AssessedStoredCandidate<'_, Self::ScanReceipt>,
    ) -> Result<ContributionCandidateId, ErrorCode> {
        let (candidate, coverage_probe, public_coverage, probe_scan_receipt, assessment) =
            assessed.into_parts();
        if coverage_probe.source_manifest_hash() != candidate.preparation.source_manifest_hash
            || probe_scan_receipt.payload_sha256() != coverage_probe.output_sha256()
            || !matches!(assessment.novelty, ContributionGate::Pass)
            || !matches!(assessment.quality, ContributionGate::Pass)
            || !matches!(assessment.generality, ContributionGate::Pass)
            || !matches!(assessment.grounding, ContributionGate::Pass)
        {
            return Err(ErrorCode::Conflict);
        }
        let authorization = candidate.preparation.authorization.clone();
        let candidate_hash = candidate.payload_sha256.to_hex();
        let binding = public_coverage.binding();
        let probe_receipt = probe_scan_receipt;
        let probe_scan = json!({"privacy_rules_version":probe_receipt.privacy_rules_version(),
            "privacy_rules_digest":probe_receipt.privacy_rules_digest(),"gitleaks_version":probe_receipt.gitleaks_version(),
            "gitleaks_binary_sha256":probe_receipt.gitleaks_binary_sha256()});
        let receipt = json!({
            "probe_sha256": coverage_probe.output_sha256().to_hex(),
            "coverage_digest_id": binding.digest_id().to_string(),
            "coverage_version": binding.coverage_version().to_string(),
            "coverage_digest_sha256": hex::encode(binding.digest_sha256().0),
            "candidate_payload_sha256": candidate_hash,
            "novelty":"PASS", "quality":"PASS", "generality":"PASS", "grounding":"PASS",
        });
        let mut txn = self.pool.pool().begin().await.map_err(db_error)?;
        let id = store_assessed_candidate_in_txn(&mut txn, candidate).await?;
        sqlx::query(
            "INSERT INTO staging.contribution_candidate_phase9_assessments(\
             candidate_id,tenant_id,probe_bytes,probe_sha256,probe_source_manifest_hash,probe_provider_trace,probe_scan_receipt,\
             coverage_digest_id,coverage_version,coverage_digest_sha256,candidate_payload_sha256,assessment_digest,\
             assessment_provider_trace,assessment_receipt) \
             VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,sha256(convert_to($13::jsonb::text,'UTF8')),$12,$13)",
        )
        .bind(id.0)
        .bind(authorization.tenant_id().0)
        .bind(coverage_probe.public_safe_bytes())
        .bind(hex::decode(coverage_probe.output_sha256().to_hex()).map_err(|_| ErrorCode::Internal)?)
        .bind(coverage_probe.source_manifest_hash().0.to_vec())
        .bind(&coverage_probe.provider_trace().0)
        .bind(probe_scan)
        .bind(binding.digest_id())
        .bind(i32::try_from(binding.coverage_version()).map_err(|_| ErrorCode::InvalidInput)?)
        .bind(binding.digest_sha256().0.to_vec())
        .bind(hex::decode(candidate_hash).map_err(|_| ErrorCode::Internal)?)
        .bind(&assessment.deidentified_candidate.provider_trace.0)
        .bind(receipt)
        .execute(&mut *txn)
        .await
        .map_err(db_error)?;
        txn.commit().await.map_err(db_error)?;
        Ok(id)
    }
}

/// The private preview service's response. Debug deliberately excludes the candidate bytes.
pub struct CandidatePreview {
    /// The candidate identity, not a release identity.
    pub candidate_id: ContributionCandidateId,
    /// Exact bytes presented for confirmation; no normalization is permitted.
    pub disclosed_bytes: Vec<u8>,
    /// Hash bound into the gateway confirmation receipt.
    pub payload_sha256: EvidencePayloadSha256,
    /// Version bound into the gateway confirmation receipt.
    pub policy_version: i64,
}

impl std::fmt::Debug for CandidatePreview {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CandidatePreview")
            .field("candidate_id", &self.candidate_id)
            .field("payload_sha256", &self.payload_sha256)
            .field("policy_version", &self.policy_version)
            .finish_non_exhaustive()
    }
}

/// Loads a preview through the authenticated private worker, never a staging grant to gateway.
pub async fn preview(
    pool: &PrivateWorkerDbPool,
    auth: &AuthorizationScope,
    id: ContributionCandidateId,
) -> Result<CandidatePreview, ErrorCode> {
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    scope(&mut txn, auth).await?;
    let row=sqlx::query("SELECT disclosed_payload,disclosed_payload_sha256,policy_version FROM staging.contribution_candidates WHERE candidate_id=$1")
        .bind(id.0).fetch_one(&mut *txn).await.map_err(db_error)?;
    let bytes: Vec<u8> = row.try_get("disclosed_payload").map_err(db_error)?;
    let digest = payload_sha256(&bytes);
    let stored: Vec<u8> = row.try_get("disclosed_payload_sha256").map_err(db_error)?;
    if digest.to_hex() != hex::encode(stored) {
        return Err(ErrorCode::Conflict);
    }
    let result = CandidatePreview {
        candidate_id: id,
        disclosed_bytes: bytes,
        payload_sha256: digest,
        policy_version: row.try_get("policy_version").map_err(db_error)?,
    };
    txn.commit().await.map_err(db_error)?;
    Ok(result)
}

/// Records explicit authenticated consent. The compound FK binds user, exact hash and version;
/// gateway has no SELECT privilege on staging. The caller supplies its configured consent TTL.
pub async fn record_confirmation(
    pool: &RuntimeDbPool,
    auth: &AuthorizationScope,
    id: ContributionCandidateId,
    hash: EvidencePayloadSha256,
    policy_version: i64,
    valid_for: std::time::Duration,
) -> Result<Uuid, ErrorCode> {
    let seconds = i64::try_from(valid_for.as_secs()).map_err(|_| ErrorCode::InvalidInput)?;
    if seconds <= 0 || policy_version <= 0 {
        return Err(ErrorCode::InvalidInput);
    }
    let mut txn = pool.pool().begin().await.map_err(db_error)?;
    scope(&mut txn, auth).await?;
    let receipt:Uuid=sqlx::query_scalar("INSERT INTO control.contribution_confirmations(tenant_id,user_id,candidate_id, \
        disclosed_payload_sha256,policy_version,expires_at) VALUES($1,$2,$3,$4,$5,clock_timestamp()+$6::bigint*interval '1 second') \
        RETURNING confirmation_id")
        .bind(auth.tenant_id().0).bind(auth.user_id().ok_or(ErrorCode::Unauthorized)?.0).bind(id.0)
        .bind(hex::decode(hash.to_hex()).map_err(|_|ErrorCode::Internal)?).bind(policy_version).bind(seconds)
        .fetch_one(&mut *txn).await.map_err(db_error)?;
    txn.commit().await.map_err(db_error)?;
    Ok(receipt)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn load_without_caller_minted_ids(
        repo: &ContributionEntryRepo<'_>,
        request: &ContributionPreparationInput,
    ) -> Result<PreparationSnapshot, ErrorCode> {
        repo.load_execution_preparation(request).await
    }

    #[test]
    fn contribution_execution_production_adapter_exposes_id_free_preparation() {
        let _ = load_without_caller_minted_ids;
    }
}
