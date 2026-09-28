//! `adapters::contribution_execution_repo` — Typed bindings for migration 0131's eight exposed contribution execution
//!   commands.
//! Depends-on: crates=[hex, humaux-application, humaux-domain, serde_json, sha2, sqlx]; services=[PostgreSQL(any) r=[private.contribution_execution_sources, private.contribution_executions, private.reserve_contribution_a, private.reserve_contribution_b] x=[private.commit_contribution_candidate, private.complete_contribution_a_exact, private.complete_contribution_b_exact, private.enqueue_contribution_execution, private.mark_contribution_reconciliation_required, private.reserve_contribution_a, private.reserve_contribution_b, private.settle_contribution_terminal_job]]; env=[]; modules=[adapters::disclosure, adapters::postgres, adapters::reasoning_route_admission, application::consolidate, application::contribute, application::contribution_execution, domain::dataclass, domain::evidence, domain::public]
//! Called-by: [adapters::contribution_execution_ingress, adapters::contribution_reasoner, humaux-private-worker, tests]
//! Invariants: [execution state transitions are read back from PostgreSQL; an unknown state or invalid input is a
//!   typed ContributionExecutionRepoError, never assumed progress]
//! Spec: none

use humaux_application::{
    consolidate::{
        ContentSha256, LogicalReasoningCallId, PrivateReasoningPurpose, ReasoningIntentSha256,
    },
    contribute::{ContributionGate, ContributionSourceSnapshot},
    contribution_execution::{
        AssessmentCompletion, AssessmentCompletionValue, CompletionScanReceipt,
        ContributionExecutionEnqueueInput, ContributionExecutionId, ContributionExecutionStage,
        ContributionLogicalCallIds, CoverageCompletion, CoverageCompletionValue,
        ExactContributionCallBinding, PersistedReservationStatus, ReservationDecision,
        ReservedContributionCall,
    },
};
use humaux_domain::dataclass::DataClass;
use humaux_domain::public::ReleaseSource;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::types::{Json, Uuid};
use sqlx::{Row, Transaction};

use crate::{
    disclosure::DisclosureScope, postgres::PrivateWorkerDbPool,
    reasoning_route_admission::ReasoningAdmissionLocator,
};

type Txn<'a> = Transaction<'a, sqlx::Postgres>;
type SourceArrays = (Vec<String>, Vec<Uuid>, Vec<Vec<u8>>);

/// The closed route witness observed during preparation and compared by the 0133 reserve guard.
/// It deliberately omits health observations because a newer healthy observation is admissible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedRouteExpectation {
    schema_version: i64,
    tenant_id: Uuid,
    binding_id: Uuid,
    binding_version: i64,
    reasoning_domain_id: Uuid,
    purpose: &'static str,
    route_policy_id: Uuid,
    route_policy_version: i64,
    profile_id: Uuid,
    profile_version: i64,
    provider_account_id: Uuid,
    processor_id: String,
    processor_model_id: Uuid,
    provider_model_id: String,
    model_revision: Option<String>,
    provider_endpoint_id: Uuid,
    egress_processor_id: Uuid,
    endpoint_ref: String,
    region: String,
    service_tier: String,
    credential_ref: Uuid,
    billing_account_id: Option<Uuid>,
    billing_instrument_id: Option<Uuid>,
}

impl PreparedRouteExpectation {
    pub(crate) fn from_admission(admission: &ReasoningAdmissionLocator) -> Self {
        Self {
            schema_version: 1,
            tenant_id: admission.tenant_id,
            binding_id: admission.binding_id.0,
            binding_version: admission.binding_version.0,
            reasoning_domain_id: admission.reasoning_domain_id.0,
            purpose: match admission.purpose {
                PrivateReasoningPurpose::Distill => "PRIVATE_DISTILL_TEXT",
                PrivateReasoningPurpose::Consolidate => "PRIVATE_CONSOLIDATE",
                PrivateReasoningPurpose::Vision => "PRIVATE_DISTILL_VISION",
                PrivateReasoningPurpose::ContributionDeidentify => "CONTRIBUTION_DEIDENTIFY",
            },
            route_policy_id: admission.route_policy_id,
            route_policy_version: admission.route_policy_version,
            profile_id: admission.profile_id,
            profile_version: admission.profile_version,
            provider_account_id: admission.provider_account_id,
            processor_id: admission.processor_id.clone(),
            processor_model_id: admission.processor_model_id,
            provider_model_id: admission.provider_model_id.clone(),
            model_revision: admission.model_revision.clone(),
            provider_endpoint_id: admission.provider_endpoint_id,
            egress_processor_id: admission.egress_processor_id.0,
            endpoint_ref: admission.endpoint_ref.clone(),
            region: admission.region.clone(),
            service_tier: admission.service_tier.clone(),
            credential_ref: admission.credential_ref,
            billing_account_id: admission.billing_account_id,
            billing_instrument_id: admission.billing_instrument_id,
        }
    }

    fn json(&self) -> serde_json::Value {
        json!({
            "schema_version": self.schema_version,
            "tenant_id": self.tenant_id.to_string(),
            "binding_id": self.binding_id.to_string(),
            "binding_version": self.binding_version,
            "reasoning_domain_id": self.reasoning_domain_id.to_string(),
            "purpose": self.purpose,
            "route_policy_id": self.route_policy_id.to_string(),
            "route_policy_version": self.route_policy_version,
            "profile_id": self.profile_id.to_string(),
            "profile_version": self.profile_version,
            "provider_account_id": self.provider_account_id.to_string(),
            "processor_id": self.processor_id,
            "processor_model_id": self.processor_model_id.to_string(),
            "provider_model_id": self.provider_model_id,
            "model_revision": self.model_revision,
            "provider_endpoint_id": self.provider_endpoint_id.to_string(),
            "egress_processor_id": self.egress_processor_id.to_string(),
            "endpoint_ref": self.endpoint_ref,
            "region": self.region,
            "service_tier": self.service_tier,
            "credential_ref": self.credential_ref.to_string(),
            "billing_account_id": self.billing_account_id.map(|id| id.to_string()),
            "billing_instrument_id": self.billing_instrument_id.map(|id| id.to_string()),
        })
    }
}

#[derive(Debug)]
pub enum ContributionExecutionRepoError {
    Db(sqlx::Error),
    InvalidInput,
    UnknownState(String),
}

impl From<sqlx::Error> for ContributionExecutionRepoError {
    fn from(value: sqlx::Error) -> Self {
        Self::Db(value)
    }
}

impl std::fmt::Display for ContributionExecutionRepoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(error) => write!(f, "contribution execution database error: {error}"),
            Self::InvalidInput => f.write_str("invalid contribution execution repository input"),
            Self::UnknownState(state) => {
                write!(f, "unknown contribution execution state {state:?}")
            }
        }
    }
}

impl std::error::Error for ContributionExecutionRepoError {}

impl ContributionExecutionRepoError {
    /// The sole error for which an already obtained exact provider response may be retried as
    /// a lease-free late completion. Other `55000` invariant failures must stay closed.
    pub fn is_fresh_lease_required(&self) -> bool {
        matches!(
            self,
            Self::Db(sqlx::Error::Database(error))
                if error.code().as_deref() == Some("55000")
                    && error.message() == "fresh contribution job lease required"
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContributionJobLease {
    job_id: Uuid,
    lease_owner: String,
    attempt: i32,
}

impl ContributionJobLease {
    pub fn try_new(
        job_id: Uuid,
        lease_owner: String,
        attempt: i32,
    ) -> Result<Self, ContributionExecutionRepoError> {
        if job_id.is_nil() || lease_owner.trim().is_empty() || attempt <= 0 {
            return Err(ContributionExecutionRepoError::InvalidInput);
        }
        Ok(Self {
            job_id,
            lease_owner,
            attempt,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ContributionReserveInput {
    model_call_id: Uuid,
    disclosure_id: Uuid,
    intent_sha256: ReasoningIntentSha256,
    grant_id: Uuid,
    scope: Option<DisclosureScope>,
    data_class: DataClass,
    payload_sha256: ContentSha256,
    payload_bytes: i64,
    prepared_route: Option<PreparedRouteExpectation>,
}

/// ID-free reservation parameters derived from one prepared outbound call. Durable model-call
/// and disclosure identities are minted only inside the repository immediately before the
/// typed SQL command; neither the worker nor the reasoner can create retry identities.
#[derive(Debug, Clone)]
pub struct ContributionReservePlan {
    intent_sha256: ReasoningIntentSha256,
    grant_id: Uuid,
    scope: Option<DisclosureScope>,
    data_class: DataClass,
    payload_sha256: ContentSha256,
    payload_bytes: i64,
    prepared_route: Option<PreparedRouteExpectation>,
}

impl ContributionReservePlan {
    pub fn try_new(
        intent_sha256: ReasoningIntentSha256,
        grant_id: Uuid,
        scope: Option<DisclosureScope>,
        data_class: DataClass,
        payload_sha256: ContentSha256,
        payload_bytes: i64,
    ) -> Result<Self, ContributionExecutionRepoError> {
        if grant_id.is_nil()
            || scope
                .is_some_and(|scope| scope.scope_kind.trim().is_empty() || scope.scope_id.is_nil())
            || payload_bytes < 0
        {
            return Err(ContributionExecutionRepoError::InvalidInput);
        }
        Ok(Self {
            intent_sha256,
            grant_id,
            scope,
            data_class,
            payload_sha256,
            payload_bytes,
            prepared_route: None,
        })
    }

    pub(crate) fn with_prepared_route(mut self, prepared_route: PreparedRouteExpectation) -> Self {
        self.prepared_route = Some(prepared_route);
        self
    }

    fn mint_input(&self) -> ContributionReserveInput {
        ContributionReserveInput {
            model_call_id: Uuid::now_v7(),
            disclosure_id: Uuid::now_v7(),
            intent_sha256: self.intent_sha256,
            grant_id: self.grant_id,
            scope: self.scope,
            data_class: self.data_class,
            payload_sha256: self.payload_sha256,
            payload_bytes: self.payload_bytes,
            prepared_route: self.prepared_route.clone(),
        }
    }
}

impl ContributionReserveInput {
    #[allow(clippy::too_many_arguments)] // mirrors the fixed 0131 reserve signature
    pub fn try_new(
        model_call_id: Uuid,
        disclosure_id: Uuid,
        intent_sha256: ReasoningIntentSha256,
        grant_id: Uuid,
        scope: Option<DisclosureScope>,
        data_class: DataClass,
        payload_sha256: ContentSha256,
        payload_bytes: i64,
    ) -> Result<Self, ContributionExecutionRepoError> {
        if model_call_id.is_nil()
            || disclosure_id.is_nil()
            || grant_id.is_nil()
            || scope
                .is_some_and(|scope| scope.scope_kind.trim().is_empty() || scope.scope_id.is_nil())
            || payload_bytes < 0
        {
            return Err(ContributionExecutionRepoError::InvalidInput);
        }
        Ok(Self {
            model_call_id,
            disclosure_id,
            intent_sha256,
            grant_id,
            scope,
            data_class,
            payload_sha256,
            payload_bytes,
            prepared_route: None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnqueuedContributionExecution {
    pub execution_id: ContributionExecutionId,
    pub job_id: Uuid,
    pub logical_call_ids: ContributionLogicalCallIds,
    pub candidate_id: Uuid,
    pub created: bool,
}

/// Closed source kind persisted in `private.contribution_execution_sources`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContributionExecutionSourceKind {
    Evidence,
    Memory,
}

impl ContributionExecutionSourceKind {
    pub const fn as_db_str(self) -> &'static str {
        match self {
            Self::Evidence => "EVIDENCE",
            Self::Memory => "MEMORY",
        }
    }
}

/// One ordered, frozen source reference used by an execution. The source body is deliberately
/// absent: the execution read seam carries only the durable identity and change-detection hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContributionExecutionSource {
    pub ordinal: i32,
    pub kind: ContributionExecutionSourceKind,
    pub source_id: Uuid,
    pub source_hash: ContentSha256,
}

/// The reconstructible coverage context required to build the frozen B prompt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenCoverageSnapshot {
    pub snapshot_id: Uuid,
    pub version: i32,
    pub canonical_summaries: Vec<u8>,
    pub digest_sha256: ContentSha256,
}

/// Typed execution read model for the private reasoner seam. It intentionally contains no
/// preparation blob, policy decision, route resolution, or provider payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContributionExecutionRead {
    pub execution_id: ContributionExecutionId,
    pub tenant_id: Uuid,
    pub user_id: Uuid,
    pub state: humaux_application::contribution_execution::ContributionExecutionState,
    pub reasoning_domain_id: Uuid,
    pub binding_id: Uuid,
    pub binding_version: i64,
    pub input_manifest_hash: ContentSha256,
    pub coverage_contract_version: i64,
    pub coverage_prompt_contract_sha256: ContentSha256,
    pub assessment_contract_version: i64,
    pub assessment_prompt_contract_sha256: ContentSha256,
    pub logical_call_ids: ContributionLogicalCallIds,
    pub sources: Vec<ContributionExecutionSource>,
    pub coverage: Option<FrozenCoverageSnapshot>,
}

pub struct ContributionExecutionRepo<'a> {
    pool: &'a PrivateWorkerDbPool,
}

impl<'a> ContributionExecutionRepo<'a> {
    pub const fn new(pool: &'a PrivateWorkerDbPool) -> Self {
        Self { pool }
    }

    /// Loads only the durable execution root and its append-only source manifest. The private
    /// worker must be able to reconstruct a B prompt from this value without trusting a jobs
    /// payload or rebuilding preparation/policy decisions. Every row is parsed closed and the
    /// manifest is checked against the canonical migration-0131 digest before it is returned.
    #[allow(clippy::too_many_lines)] // One read transaction validates every frozen execution axis.
    pub async fn load(
        &self,
        tenant_id: Uuid,
        execution_id: ContributionExecutionId,
    ) -> Result<Option<ContributionExecutionRead>, ContributionExecutionRepoError> {
        // dep: PostgreSQL(any) — opens a PostgreSQL transaction
        let mut txn = self.pool.pool().begin().await?;
        set_tenant_local(&mut txn, tenant_id).await?;
        let root = sqlx::query(
            "SELECT execution_id,tenant_id,user_id,state::text,reasoning_domain_id,
                    binding_id,binding_version,input_manifest_hash,source_count,
                    coverage_contract_version,coverage_prompt_contract_sha256,
                    assessment_contract_version,assessment_prompt_contract_sha256,
                    coverage_request_id,assessment_request_id,
                    coverage_snapshot_id,coverage_version,coverage_summaries_canonical,
                    coverage_digest_sha256
             FROM private.contribution_executions
             WHERE tenant_id=$1 AND execution_id=$2",
        )
        .bind(tenant_id)
        .bind(execution_id.as_uuid())
        .fetch_optional(&mut *txn)
        .await?;
        let Some(root) = root else {
            txn.commit().await?;
            return Ok(None);
        };

        let actual_execution_id =
            ContributionExecutionId::try_from_uuid(root.try_get("execution_id")?)
                .map_err(|_| ContributionExecutionRepoError::InvalidInput)?;
        if actual_execution_id != execution_id || root.try_get::<Uuid, _>("tenant_id")? != tenant_id
        {
            return Err(ContributionExecutionRepoError::InvalidInput);
        }
        let state = parse_state(&root.try_get::<String, _>("state")?)?;
        let source_count: i32 = root.try_get("source_count")?;
        if source_count <= 0 {
            return Err(ContributionExecutionRepoError::InvalidInput);
        }
        let input_manifest_hash = hash32(root.try_get("input_manifest_hash")?)?;
        let coverage_contract_version: i64 = root.try_get("coverage_contract_version")?;
        let coverage_prompt_contract_sha256 =
            hash32(root.try_get("coverage_prompt_contract_sha256")?)?;
        let assessment_contract_version: i64 = root.try_get("assessment_contract_version")?;
        let assessment_prompt_contract_sha256 =
            hash32(root.try_get("assessment_prompt_contract_sha256")?)?;
        if coverage_contract_version <= 0 || assessment_contract_version <= 0 {
            return Err(ContributionExecutionRepoError::InvalidInput);
        }
        let coverage_request_id: Uuid = root.try_get("coverage_request_id")?;
        let assessment_request_id: Uuid = root.try_get("assessment_request_id")?;
        let logical_call_ids = ContributionLogicalCallIds::try_new(
            LogicalReasoningCallId(coverage_request_id),
            LogicalReasoningCallId(assessment_request_id),
        )
        .map_err(|_| ContributionExecutionRepoError::InvalidInput)?;

        let rows = sqlx::query(
            "SELECT ordinal,evidence_id,memory_id,source_hash,
                    CASE WHEN evidence_id IS NOT NULL THEN 'EVIDENCE'
                         WHEN memory_id IS NOT NULL THEN 'MEMORY' END AS source_kind
             FROM private.contribution_execution_sources
             WHERE tenant_id=$1 AND execution_id=$2
             ORDER BY ordinal",
        )
        .bind(tenant_id)
        .bind(execution_id.as_uuid())
        .fetch_all(&mut *txn)
        .await?;
        if rows.len() != source_count as usize {
            return Err(ContributionExecutionRepoError::InvalidInput);
        }
        let mut sources = Vec::with_capacity(rows.len());
        for row in &rows {
            let ordinal: i32 = row.try_get("ordinal")?;
            let evidence_id: Option<Uuid> = row.try_get("evidence_id")?;
            let memory_id: Option<Uuid> = row.try_get("memory_id")?;
            let kind =
                parse_source_kind(row.try_get::<Option<String>, _>("source_kind")?.as_deref())?;
            let source_id = match (kind, evidence_id, memory_id) {
                (ContributionExecutionSourceKind::Evidence, Some(id), None) if !id.is_nil() => id,
                (ContributionExecutionSourceKind::Memory, None, Some(id)) if !id.is_nil() => id,
                _ => return Err(ContributionExecutionRepoError::InvalidInput),
            };
            sources.push(ContributionExecutionSource {
                ordinal,
                kind,
                source_id,
                source_hash: hash32(row.try_get("source_hash")?)?,
            });
        }
        validate_sources(source_count, &sources, input_manifest_hash)?;

        let coverage = coverage_snapshot(&root)?;
        if matches!(
            state,
            humaux_application::contribution_execution::ContributionExecutionState::ReadyB
        ) && coverage.is_none()
        {
            return Err(ContributionExecutionRepoError::InvalidInput);
        }
        let user_id: Uuid = root.try_get("user_id")?;
        let reasoning_domain_id: Uuid = root.try_get("reasoning_domain_id")?;
        let binding_id: Uuid = root.try_get("binding_id")?;
        let binding_version: i64 = root.try_get("binding_version")?;
        if user_id.is_nil()
            || reasoning_domain_id.is_nil()
            || binding_id.is_nil()
            || binding_version <= 0
        {
            return Err(ContributionExecutionRepoError::InvalidInput);
        }
        let result = ContributionExecutionRead {
            execution_id: actual_execution_id,
            tenant_id,
            user_id,
            state,
            reasoning_domain_id,
            binding_id,
            binding_version,
            input_manifest_hash,
            coverage_contract_version,
            coverage_prompt_contract_sha256,
            assessment_contract_version,
            assessment_prompt_contract_sha256,
            logical_call_ids,
            sources,
            coverage,
        };
        txn.commit().await?;
        Ok(Some(result))
    }

    /// Binds one fixed scanner receipt to PostgreSQL's own `jsonb::text` representation.
    /// Completion SQL verifies this same digest, so Rust deliberately does not implement a
    /// competing JSON canonicalizer.
    pub async fn bind_completion_scan_receipt(
        &self,
        tenant_id: Uuid,
        scan_receipt: serde_json::Value,
    ) -> Result<CompletionScanReceipt, ContributionExecutionRepoError> {
        if !scan_receipt.is_object() {
            return Err(ContributionExecutionRepoError::InvalidInput);
        }
        // dep: PostgreSQL(any) — opens a PostgreSQL transaction
        let mut txn = self.pool.pool().begin().await?;
        set_tenant_local(&mut txn, tenant_id).await?;
        let digest: Vec<u8> =
            sqlx::query_scalar("SELECT sha256(convert_to($1::jsonb::text, 'UTF8'))")
                .bind(&scan_receipt)
                .fetch_one(&mut *txn)
                .await?;
        txn.commit().await?;
        CompletionScanReceipt::try_new(scan_receipt, hash32(digest)?)
            .map_err(|_| ContributionExecutionRepoError::InvalidInput)
    }

    /// Mints the five durable identities exactly once for this command attempt. The repository
    /// calculates the canonical fingerprint itself before SQL sees it, so unordered caller maps
    /// can never acquire idempotency authority. SQL remains the idempotency arbiter and returns
    /// the earlier identities on a matching retry.
    pub async fn enqueue(
        &self,
        input: &ContributionExecutionEnqueueInput,
    ) -> Result<EnqueuedContributionExecution, ContributionExecutionRepoError> {
        let preparation = input.preparation();
        let tenant_id = preparation.authorization.tenant_id().0;
        let fingerprint = enqueue_fingerprint_v1(input)?;
        // dep: PostgreSQL(any) — opens a PostgreSQL transaction
        let mut txn = self.pool.pool().begin().await?;
        set_tenant_local(&mut txn, tenant_id).await?;
        let returned = self
            .enqueue_with_fingerprint_in_txn(&mut txn, input, fingerprint)
            .await?;
        txn.commit().await?;
        Ok(returned)
    }

    /// Serializes a production ingress key before it reads any preparation state.
    ///
    /// The migration-0131 function takes this exact lock again. The transactional ingress uses
    /// READ COMMITTED deliberately, so a waiter acquires a fresh statement snapshot after the
    /// earlier same-key creator commits, then freezes preparation with the contribution-inputs
    /// lock until this transaction enqueues.
    pub(crate) async fn lock_enqueue_key_in_txn(
        txn: &mut Txn<'_>,
        idempotency_key: &str,
    ) -> Result<(), ContributionExecutionRepoError> {
        if idempotency_key.trim().is_empty() {
            return Err(ContributionExecutionRepoError::InvalidInput);
        }
        sqlx::query(
            "SELECT pg_advisory_xact_lock(hashtextextended(\
             'contribution-enqueue:' || $1, 0))",
        )
        .bind(idempotency_key)
        .execute(&mut **txn)
        .await?;
        Ok(())
    }

    /// Enqueues the complete trusted snapshot with the v2 fingerprint while retaining exact
    /// replay for already-persisted v1 roots. The caller must own the transaction that loaded the
    /// preparation and must have acquired [`Self::lock_enqueue_key_in_txn`] before that load.
    pub(crate) async fn enqueue_compatible_in_txn(
        &self,
        txn: &mut Txn<'_>,
        input: &ContributionExecutionEnqueueInput,
    ) -> Result<EnqueuedContributionExecution, ContributionExecutionRepoError> {
        let preparation = input.preparation();
        let tenant_id = preparation.authorization.tenant_id().0;
        set_tenant_local(txn, tenant_id).await?;
        Self::lock_enqueue_key_in_txn(txn, input.idempotency_key()).await?;

        let fingerprint_v1 = enqueue_fingerprint_v1(input)?;
        let fingerprint_v2 = enqueue_fingerprint_v2(input)?;
        let existing = sqlx::query(
            "SELECT enqueue_fingerprint,coverage_prompt_contract_sha256,\
                    assessment_prompt_contract_sha256,rights_basis,source_license,publisher,\
                    contributor_attestation,redistribution_policy \
             FROM private.contribution_executions \
             WHERE tenant_id=$1 AND enqueue_idempotency_key=$2",
        )
        .bind(tenant_id)
        .bind(input.idempotency_key())
        .fetch_optional(&mut **txn)
        .await?;

        let fingerprint = if let Some(existing) = existing {
            let existing_fingerprint = hash32(existing.try_get("enqueue_fingerprint")?)?.0;
            if existing_fingerprint == fingerprint_v2 {
                fingerprint_v2
            } else {
                let rights = &preparation.rights;
                let legacy_omitted_axes_match =
                    hash32(existing.try_get("coverage_prompt_contract_sha256")?)?
                        == input.coverage_contract().sha256()
                        && hash32(existing.try_get("assessment_prompt_contract_sha256")?)?
                            == input.assessment_contract().sha256()
                        && existing.try_get::<String, _>("rights_basis")? == rights.rights_basis()
                        && existing
                            .try_get::<Option<String>, _>("source_license")?
                            .as_deref()
                            == rights.source_license()
                        && existing
                            .try_get::<Option<String>, _>("publisher")?
                            .as_deref()
                            == rights.publisher()
                        && existing
                            .try_get::<Option<String>, _>("contributor_attestation")?
                            .as_deref()
                            == rights.contributor_attestation()
                        && existing
                            .try_get::<Option<String>, _>("redistribution_policy")?
                            .as_deref()
                            == rights.redistribution_policy();
                if existing_fingerprint == fingerprint_v1 && legacy_omitted_axes_match {
                    fingerprint_v1
                } else {
                    // The existing 0131 root remains the arbiter: passing v2 makes any semantic
                    // drift fail with its canonical 23505 conflict and creates no replacement.
                    fingerprint_v2
                }
            }
        } else {
            fingerprint_v2
        };

        self.enqueue_with_fingerprint_in_txn(txn, input, fingerprint)
            .await
    }

    async fn enqueue_with_fingerprint_in_txn(
        &self,
        txn: &mut Txn<'_>,
        input: &ContributionExecutionEnqueueInput,
        fingerprint: [u8; 32],
    ) -> Result<EnqueuedContributionExecution, ContributionExecutionRepoError> {
        let preparation = input.preparation();
        let tenant_id = preparation.authorization.tenant_id().0;
        let user_id = preparation
            .authorization
            .user_id()
            .ok_or(ContributionExecutionRepoError::InvalidInput)?
            .0;
        if preparation.policy.as_db_str() != "MANUAL" {
            return Err(ContributionExecutionRepoError::InvalidInput);
        }
        let execution_id = Uuid::now_v7();
        let job_id = Uuid::now_v7();
        let logical_call_ids = ContributionLogicalCallIds::try_new(
            LogicalReasoningCallId(Uuid::now_v7()),
            LogicalReasoningCallId(Uuid::now_v7()),
        )
        .map_err(|_| ContributionExecutionRepoError::InvalidInput)?;
        let candidate_id = Uuid::now_v7();
        let (source_kinds, source_ids, source_hashes) = sources(preparation.sources.as_slice())?;
        let policy_snapshot = serde_json::json!({"policy": preparation.policy.as_db_str()});
        let row = sqlx::query("SELECT * FROM private.enqueue_contribution_execution($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28)")
            .bind(tenant_id).bind(execution_id).bind(job_id).bind(logical_call_ids.coverage().0).bind(logical_call_ids.assessment().0).bind(candidate_id)
            .bind(input.idempotency_key()).bind(fingerprint.to_vec()).bind(user_id).bind(preparation.reasoning.reasoning_domain_id.0).bind(preparation.source_manifest_hash.0)
            .bind(preparation.policy_id).bind(preparation.policy_version).bind(policy_snapshot).bind(preparation.rights.rights_basis())
            .bind(preparation.rights.source_license()).bind(preparation.rights.publisher()).bind(preparation.rights.contributor_attestation()).bind(preparation.rights.redistribution_policy())
            .bind(preparation.reasoning.binding_id.0).bind(preparation.reasoning.binding_version.0).bind(input.coverage_contract().version()).bind(input.assessment_contract().version())
            .bind(input.coverage_contract().sha256().0.to_vec()).bind(input.assessment_contract().sha256().0.to_vec()).bind(source_kinds).bind(source_ids).bind(source_hashes)
            .fetch_one(&mut **txn).await?;
        let execution_id = ContributionExecutionId::try_from_uuid(row.try_get("execution_id")?)
            .map_err(|_| ContributionExecutionRepoError::InvalidInput)?;
        let job_id: Uuid = row.try_get("job_id")?;
        let candidate_id: Uuid = row.try_get("candidate_id")?;
        if job_id.is_nil() || candidate_id.is_nil() {
            return Err(ContributionExecutionRepoError::InvalidInput);
        }
        let returned = EnqueuedContributionExecution {
            execution_id,
            job_id,
            logical_call_ids: ContributionLogicalCallIds::try_new(
                LogicalReasoningCallId(row.try_get("coverage_request_id")?),
                LogicalReasoningCallId(row.try_get("assessment_request_id")?),
            )
            .map_err(|_| ContributionExecutionRepoError::InvalidInput)?,
            candidate_id,
            created: row.try_get("created")?,
        };
        Ok(returned)
    }

    pub async fn reserve_a(
        &self,
        tenant_id: Uuid,
        execution_id: ContributionExecutionId,
        lease: &ContributionJobLease,
        input: &ContributionReserveInput,
    ) -> Result<ReservationDecision, ContributionExecutionRepoError> {
        self.reserve(
            "private.reserve_contribution_a",
            ContributionExecutionStage::Coverage,
            tenant_id,
            execution_id,
            lease,
            input,
        )
        .await
    }

    /// Repository-owned durable-id seam for a newly prepared A attempt.
    pub async fn reserve_a_new(
        &self,
        tenant_id: Uuid,
        execution_id: ContributionExecutionId,
        lease: &ContributionJobLease,
        plan: &ContributionReservePlan,
    ) -> Result<ReservationDecision, ContributionExecutionRepoError> {
        let input = plan.mint_input();
        self.reserve_a(tenant_id, execution_id, lease, &input).await
    }

    pub async fn reserve_b(
        &self,
        tenant_id: Uuid,
        execution_id: ContributionExecutionId,
        lease: &ContributionJobLease,
        input: &ContributionReserveInput,
    ) -> Result<ReservationDecision, ContributionExecutionRepoError> {
        self.reserve(
            "private.reserve_contribution_b",
            ContributionExecutionStage::Assessment,
            tenant_id,
            execution_id,
            lease,
            input,
        )
        .await
    }

    /// Repository-owned durable-id seam for a newly prepared B attempt.
    pub async fn reserve_b_new(
        &self,
        tenant_id: Uuid,
        execution_id: ContributionExecutionId,
        lease: &ContributionJobLease,
        plan: &ContributionReservePlan,
    ) -> Result<ReservationDecision, ContributionExecutionRepoError> {
        let input = plan.mint_input();
        self.reserve_b(tenant_id, execution_id, lease, &input).await
    }

    async fn reserve(
        &self,
        function: &str,
        stage: ContributionExecutionStage,
        tenant_id: Uuid,
        execution_id: ContributionExecutionId,
        lease: &ContributionJobLease,
        input: &ContributionReserveInput,
    ) -> Result<ReservationDecision, ContributionExecutionRepoError> {
        let sql = match function {
            "private.reserve_contribution_a" => {
                "SELECT * FROM private.reserve_contribution_a($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb)"
            }
            "private.reserve_contribution_b" => {
                "SELECT * FROM private.reserve_contribution_b($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb)"
            }
            _ => return Err(ContributionExecutionRepoError::InvalidInput),
        };
        let prepared_route = input
            .prepared_route
            .as_ref()
            .ok_or(ContributionExecutionRepoError::InvalidInput)?;
        // dep: PostgreSQL(any) — opens a PostgreSQL transaction
        let mut txn = self.pool.pool().begin().await?;
        set_tenant_local(&mut txn, tenant_id).await?;
        let row = sqlx::query(sql)
            .bind(tenant_id)
            .bind(execution_id.as_uuid())
            .bind(lease.job_id)
            .bind(&lease.lease_owner)
            .bind(lease.attempt)
            .bind(input.model_call_id)
            .bind(input.disclosure_id)
            .bind(input.intent_sha256.0.to_vec())
            .bind(input.grant_id)
            .bind(input.scope.map(|scope| scope.scope_kind))
            .bind(input.scope.map(|scope| scope.scope_id))
            .bind(input.data_class.as_str())
            .bind(input.payload_sha256.0.to_vec())
            .bind(input.payload_bytes)
            .bind(Json(prepared_route.json()))
            .fetch_one(&mut *txn)
            .await?;
        let binding = ExactContributionCallBinding::try_new(
            execution_id,
            stage,
            row.try_get("model_call_id")?,
            LogicalReasoningCallId(row.try_get("request_id")?),
            input.intent_sha256,
            row.try_get("disclosure_id")?,
        )
        .map_err(|_| ContributionExecutionRepoError::InvalidInput)?;
        let reservation = ReservedContributionCall::new(
            binding,
            row.try_get("processor_id")?,
            row.try_get("provider_model_id")?,
            row.try_get("model_revision")?,
            row.try_get("egress_processor_id")?,
            row.try_get("credential_ref")?,
        );
        let status = if row.try_get("newly_reserved")? {
            PersistedReservationStatus::NewlyReserved
        } else {
            PersistedReservationStatus::ExistingReserved
        };
        txn.commit().await?;
        Ok(ReservationDecision::from_persisted(reservation, status))
    }

    pub async fn complete_a_exact(
        &self,
        tenant_id: Uuid,
        completion: &CoverageCompletion,
        lease: Option<&ContributionJobLease>,
    ) -> Result<
        humaux_application::contribution_execution::ContributionExecutionState,
        ContributionExecutionRepoError,
    > {
        let binding = completion.binding();
        let (
            outcome,
            probe,
            snapshot,
            version,
            summaries,
            digest,
            receipt,
            receipt_sha,
            trace,
            request,
            error,
        ) = match completion.value() {
            CoverageCompletionValue::Usable {
                coverage_probe_sha256,
                coverage,
                receipt,
            } => (
                "USABLE",
                Some(evidence_bytes(coverage_probe_sha256)?),
                Some(coverage.binding().digest_id()),
                Some(coverage.binding().coverage_version() as i32),
                Some(coverage.canonical_bytes()),
                Some(coverage.binding().digest_sha256().0.to_vec()),
                Some(receipt.scan_receipt().value()),
                Some(receipt.scan_receipt().sha256().0.to_vec()),
                Some(receipt.provider_trace().0.as_str()),
                receipt.provider_request_id(),
                None,
            ),
            CoverageCompletionValue::RejectedSafety(receipt) => (
                "REJECTED_SAFETY",
                None,
                None,
                None,
                None,
                None,
                Some(receipt.scan_receipt().value()),
                Some(receipt.scan_receipt().sha256().0.to_vec()),
                Some(receipt.provider_trace().0.as_str()),
                receipt.provider_request_id(),
                None,
            ),
            CoverageCompletionValue::FailedTerminal(failure) => (
                "FAILED_TERMINAL",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                failure.provider_request_id(),
                Some(failure.error_class()),
            ),
        };
        self.complete_a_sql(
            tenant_id,
            binding,
            lease,
            outcome,
            probe,
            snapshot,
            version,
            summaries,
            digest,
            receipt,
            receipt_sha,
            trace,
            request,
            error,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)] // one-to-one nullable 0131 A completion bind list
    async fn complete_a_sql(
        &self,
        tenant_id: Uuid,
        binding: ExactContributionCallBinding,
        lease: Option<&ContributionJobLease>,
        outcome: &str,
        probe: Option<Vec<u8>>,
        snapshot: Option<Uuid>,
        version: Option<i32>,
        summaries: Option<Vec<u8>>,
        digest: Option<Vec<u8>>,
        receipt: Option<&serde_json::Value>,
        receipt_sha: Option<Vec<u8>>,
        trace: Option<&str>,
        request: Option<&str>,
        error: Option<&str>,
    ) -> Result<
        humaux_application::contribution_execution::ContributionExecutionState,
        ContributionExecutionRepoError,
    > {
        // dep: PostgreSQL(any) — opens a PostgreSQL transaction
        let mut txn = self.pool.pool().begin().await?;
        set_tenant_local(&mut txn, tenant_id).await?;
        let state: String = sqlx::query_scalar("SELECT private.complete_contribution_a_exact($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)::text")
            .bind(tenant_id).bind(binding.execution_id().as_uuid()).bind(binding.model_call_id()).bind(binding.request_id().0).bind(binding.intent_sha256().0.to_vec()).bind(binding.disclosure_id()).bind(outcome).bind(probe).bind(snapshot).bind(version).bind(summaries).bind(digest).bind(receipt).bind(receipt_sha).bind(trace).bind(request).bind(error).bind(lease.map(|lease| lease.job_id)).bind(lease.map(|lease| lease.lease_owner.as_str())).bind(lease.map(|lease| lease.attempt)).fetch_one(&mut *txn).await?;
        let state = parse_state(&state)?;
        txn.commit().await?;
        Ok(state)
    }

    pub async fn complete_b_exact(
        &self,
        tenant_id: Uuid,
        completion: &AssessmentCompletion,
        lease: Option<&ContributionJobLease>,
    ) -> Result<
        humaux_application::contribution_execution::ContributionExecutionState,
        ContributionExecutionRepoError,
    > {
        let binding = completion.binding();
        let (
            outcome,
            output,
            output_sha,
            gates,
            body,
            body_sha,
            receipt,
            receipt_sha,
            trace,
            request,
            error,
        ) = match completion.value() {
            AssessmentCompletionValue::ReadyCandidate {
                assessment,
                candidate,
            } => (
                "READY_CANDIDATE",
                Some(assessment.output_canonical()),
                Some(assessment.output_sha256().0.to_vec()),
                Some(assessment.gates()),
                Some(candidate.body()),
                Some(evidence_bytes(&candidate.sha256())?),
                Some(candidate.scan_receipt().value()),
                Some(candidate.scan_receipt().sha256().0.to_vec()),
                Some(assessment.provider_trace().0.as_str()),
                assessment.provider_request_id(),
                None,
            ),
            AssessmentCompletionValue::NotContributable(assessment) => (
                "NOT_CONTRIBUTABLE",
                Some(assessment.output_canonical()),
                Some(assessment.output_sha256().0.to_vec()),
                Some(assessment.gates()),
                None,
                None,
                None,
                None,
                Some(assessment.provider_trace().0.as_str()),
                assessment.provider_request_id(),
                None,
            ),
            AssessmentCompletionValue::RejectedSafety {
                assessment,
                candidate,
            } => (
                "REJECTED_SAFETY",
                Some(assessment.output_canonical()),
                Some(assessment.output_sha256().0.to_vec()),
                Some(assessment.gates()),
                Some(candidate.body()),
                Some(evidence_bytes(&candidate.sha256())?),
                Some(candidate.scan_receipt().value()),
                Some(candidate.scan_receipt().sha256().0.to_vec()),
                Some(assessment.provider_trace().0.as_str()),
                assessment.provider_request_id(),
                None,
            ),
            AssessmentCompletionValue::FailedTerminal(failure) => (
                "FAILED_TERMINAL",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                failure.provider_request_id(),
                Some(failure.error_class()),
            ),
        };
        // dep: PostgreSQL(any) — opens a PostgreSQL transaction
        let mut txn = self.pool.pool().begin().await?;
        set_tenant_local(&mut txn, tenant_id).await?;
        let state: String = sqlx::query_scalar("SELECT private.complete_contribution_b_exact($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)::text")
            .bind(tenant_id).bind(binding.execution_id().as_uuid()).bind(binding.model_call_id()).bind(binding.request_id().0).bind(binding.intent_sha256().0.to_vec()).bind(binding.disclosure_id()).bind(outcome).bind(output).bind(output_sha)
            .bind(gates.map(|v| gate(v[0]))).bind(gates.map(|v| gate(v[1]))).bind(gates.map(|v| gate(v[2]))).bind(gates.map(|v| gate(v[3]))).bind(body).bind(body_sha).bind(receipt).bind(receipt_sha).bind(trace).bind(request).bind(error).bind(lease.map(|lease| lease.job_id)).bind(lease.map(|lease| lease.lease_owner.as_str())).bind(lease.map(|lease| lease.attempt)).fetch_one(&mut *txn).await?;
        let state = parse_state(&state)?;
        txn.commit().await?;
        Ok(state)
    }

    pub async fn commit_candidate(
        &self,
        tenant_id: Uuid,
        execution_id: ContributionExecutionId,
        lease: &ContributionJobLease,
    ) -> Result<Uuid, ContributionExecutionRepoError> {
        self.scalar_uuid(
            "SELECT private.commit_contribution_candidate($1,$2,$3,$4,$5)",
            tenant_id,
            execution_id,
            lease,
        )
        .await
    }
    pub async fn settle_terminal_job(
        &self,
        tenant_id: Uuid,
        execution_id: ContributionExecutionId,
        lease: &ContributionJobLease,
    ) -> Result<String, ContributionExecutionRepoError> {
        self.scalar_text(
            "SELECT private.settle_contribution_terminal_job($1,$2,$3,$4,$5)",
            tenant_id,
            execution_id,
            lease,
        )
        .await
    }
    pub async fn mark_reconciliation_required(
        &self,
        tenant_id: Uuid,
        execution_id: ContributionExecutionId,
        lease: &ContributionJobLease,
    ) -> Result<bool, ContributionExecutionRepoError> {
        // dep: PostgreSQL(any) — opens a PostgreSQL transaction
        let mut txn = self.pool.pool().begin().await?;
        set_tenant_local(&mut txn, tenant_id).await?;
        let result = sqlx::query_scalar(
            "SELECT private.mark_contribution_reconciliation_required($1,$2,$3,$4,$5)",
        )
        .bind(tenant_id)
        .bind(execution_id.as_uuid())
        .bind(lease.job_id)
        .bind(&lease.lease_owner)
        .bind(lease.attempt)
        .fetch_one(&mut *txn)
        .await?;
        txn.commit().await?;
        Ok(result)
    }
    async fn scalar_uuid(
        &self,
        sql: &str,
        tenant_id: Uuid,
        execution_id: ContributionExecutionId,
        lease: &ContributionJobLease,
    ) -> Result<Uuid, ContributionExecutionRepoError> {
        // dep: PostgreSQL(any) — opens a PostgreSQL transaction
        let mut txn = self.pool.pool().begin().await?;
        set_tenant_local(&mut txn, tenant_id).await?;
        let result = sqlx::query_scalar(sql)
            .bind(tenant_id)
            .bind(execution_id.as_uuid())
            .bind(lease.job_id)
            .bind(&lease.lease_owner)
            .bind(lease.attempt)
            .fetch_one(&mut *txn)
            .await?;
        txn.commit().await?;
        Ok(result)
    }
    async fn scalar_text(
        &self,
        sql: &str,
        tenant_id: Uuid,
        execution_id: ContributionExecutionId,
        lease: &ContributionJobLease,
    ) -> Result<String, ContributionExecutionRepoError> {
        // dep: PostgreSQL(any) — opens a PostgreSQL transaction
        let mut txn = self.pool.pool().begin().await?;
        set_tenant_local(&mut txn, tenant_id).await?;
        let result = sqlx::query_scalar(sql)
            .bind(tenant_id)
            .bind(execution_id.as_uuid())
            .bind(lease.job_id)
            .bind(&lease.lease_owner)
            .bind(lease.attempt)
            .fetch_one(&mut *txn)
            .await?;
        if result != "DONE" && result != "FAILED" {
            return Err(ContributionExecutionRepoError::InvalidInput);
        }
        txn.commit().await?;
        Ok(result)
    }
}

async fn set_tenant_local(
    txn: &mut Txn<'_>,
    tenant_id: Uuid,
) -> Result<(), ContributionExecutionRepoError> {
    sqlx::query(&format!("SET LOCAL humaux.tenant_id = '{tenant_id}'"))
        .execute(&mut **txn)
        .await?;
    Ok(())
}
fn parse_state(
    value: &str,
) -> Result<
    humaux_application::contribution_execution::ContributionExecutionState,
    ContributionExecutionRepoError,
> {
    humaux_application::contribution_execution::ContributionExecutionState::try_from_db(value)
        .map_err(|_| ContributionExecutionRepoError::UnknownState(value.into()))
}

fn hash32(value: Vec<u8>) -> Result<ContentSha256, ContributionExecutionRepoError> {
    if value.len() != 32 {
        return Err(ContributionExecutionRepoError::InvalidInput);
    }
    Ok(ContentSha256(value.try_into().map_err(|_| {
        ContributionExecutionRepoError::InvalidInput
    })?))
}

fn parse_source_kind(
    value: Option<&str>,
) -> Result<ContributionExecutionSourceKind, ContributionExecutionRepoError> {
    match value {
        Some("EVIDENCE") => Ok(ContributionExecutionSourceKind::Evidence),
        Some("MEMORY") => Ok(ContributionExecutionSourceKind::Memory),
        _ => Err(ContributionExecutionRepoError::InvalidInput),
    }
}

fn validate_sources(
    source_count: i32,
    sources: &[ContributionExecutionSource],
    input_manifest_hash: ContentSha256,
) -> Result<(), ContributionExecutionRepoError> {
    if source_count <= 0 || sources.len() != source_count as usize {
        return Err(ContributionExecutionRepoError::InvalidInput);
    }
    if sources
        .iter()
        .enumerate()
        .any(|(expected, source)| source.ordinal != expected as i32)
        || manifest_digest(sources) != input_manifest_hash.0
    {
        return Err(ContributionExecutionRepoError::InvalidInput);
    }
    Ok(())
}

fn coverage_snapshot(
    row: &sqlx::postgres::PgRow,
) -> Result<Option<FrozenCoverageSnapshot>, ContributionExecutionRepoError> {
    let snapshot_id: Option<Uuid> = row.try_get("coverage_snapshot_id")?;
    let version: Option<i32> = row.try_get("coverage_version")?;
    let canonical: Option<Vec<u8>> = row.try_get("coverage_summaries_canonical")?;
    let digest: Option<Vec<u8>> = row.try_get("coverage_digest_sha256")?;
    match (snapshot_id, version, canonical, digest) {
        (None, None, None, None) => Ok(None),
        (Some(snapshot_id), Some(version), Some(canonical), Some(digest))
            if !snapshot_id.is_nil() && version > 0 && !canonical.is_empty() =>
        {
            let digest_sha256 = hash32(digest)?;
            if Sha256::digest(&canonical).as_slice() != digest_sha256.0.as_slice() {
                return Err(ContributionExecutionRepoError::InvalidInput);
            }
            Ok(Some(FrozenCoverageSnapshot {
                snapshot_id,
                version,
                canonical_summaries: canonical,
                digest_sha256,
            }))
        }
        _ => Err(ContributionExecutionRepoError::InvalidInput),
    }
}

fn manifest_digest(sources: &[ContributionExecutionSource]) -> [u8; 32] {
    // Keep this byte-for-byte aligned with migration 0131's deferred manifest trigger: the
    // stored ordinal is execution order, while the manifest hash canonicalizes by type then id.
    let mut entries = sources
        .iter()
        .map(|source| {
            format!(
                "{}:{}:{}",
                match source.kind {
                    ContributionExecutionSourceKind::Evidence => "e",
                    ContributionExecutionSourceKind::Memory => "m",
                },
                source.source_id,
                hex::encode(source.source_hash.0),
            )
        })
        .collect::<Vec<_>>();
    entries.sort_unstable();
    Sha256::digest(entries.join("|").as_bytes()).into()
}

fn gate(value: ContributionGate) -> &'static str {
    match value {
        ContributionGate::Pass => "PASS",
        ContributionGate::Fail => "FAIL",
    }
}
fn evidence_bytes(
    value: &humaux_domain::evidence::EvidencePayloadSha256,
) -> Result<Vec<u8>, ContributionExecutionRepoError> {
    hex::decode(value.to_hex()).map_err(|_| ContributionExecutionRepoError::InvalidInput)
}
fn sources(
    values: &[ContributionSourceSnapshot],
) -> Result<SourceArrays, ContributionExecutionRepoError> {
    if values.is_empty() {
        return Err(ContributionExecutionRepoError::InvalidInput);
    }
    Ok(values
        .iter()
        .map(|source| match source.source {
            ReleaseSource::Evidence(id) => {
                ("EVIDENCE".to_owned(), id.0, source.current_hash.0.to_vec())
            }
            ReleaseSource::Memory(id) => {
                ("MEMORY".to_owned(), id.0, source.current_hash.0.to_vec())
            }
        })
        .fold(
            (Vec::new(), Vec::new(), Vec::new()),
            |(mut kinds, mut ids, mut hashes), (kind, id, hash)| {
                kinds.push(kind);
                ids.push(id);
                hashes.push(hash);
                (kinds, ids, hashes)
            },
        ))
}
fn enqueue_fingerprint_v1(
    input: &ContributionExecutionEnqueueInput,
) -> Result<[u8; 32], ContributionExecutionRepoError> {
    let preparation = input.preparation();
    let user_id = preparation
        .authorization
        .user_id()
        .ok_or(ContributionExecutionRepoError::InvalidInput)?
        .0;
    if preparation.policy.as_db_str() != "MANUAL" {
        return Err(ContributionExecutionRepoError::InvalidInput);
    }
    let mut bytes = b"humaux.phase9.contribution-execution.enqueue\0".to_vec();
    append_fingerprint_v1_axes(
        &mut bytes,
        preparation.authorization.tenant_id().0,
        user_id,
        preparation.sources.as_slice(),
        preparation.source_manifest_hash,
        preparation.policy_id,
        preparation.policy_version,
        preparation.reasoning.reasoning_domain_id.0,
        preparation.reasoning.binding_id.0,
        preparation.reasoning.binding_version.0,
        input.coverage_contract().version(),
        input.assessment_contract().version(),
    );
    Ok(Sha256::digest(bytes).into())
}

fn enqueue_fingerprint_v2(
    input: &ContributionExecutionEnqueueInput,
) -> Result<[u8; 32], ContributionExecutionRepoError> {
    let preparation = input.preparation();
    let user_id = preparation
        .authorization
        .user_id()
        .ok_or(ContributionExecutionRepoError::InvalidInput)?
        .0;
    if preparation.policy.as_db_str() != "MANUAL" {
        return Err(ContributionExecutionRepoError::InvalidInput);
    }
    let mut bytes = b"humaux.phase9.contribution-execution.enqueue.v2\0".to_vec();
    append_fingerprint_v1_axes(
        &mut bytes,
        preparation.authorization.tenant_id().0,
        user_id,
        preparation.sources.as_slice(),
        preparation.source_manifest_hash,
        preparation.policy_id,
        preparation.policy_version,
        preparation.reasoning.reasoning_domain_id.0,
        preparation.reasoning.binding_id.0,
        preparation.reasoning.binding_version.0,
        input.coverage_contract().version(),
        input.assessment_contract().version(),
    );
    bytes.extend_from_slice(&input.coverage_contract().sha256().0);
    bytes.extend_from_slice(&input.assessment_contract().sha256().0);
    append_fingerprint_text(
        &mut bytes,
        b"rights_basis",
        Some(preparation.rights.rights_basis()),
    );
    append_fingerprint_text(
        &mut bytes,
        b"source_license",
        preparation.rights.source_license(),
    );
    append_fingerprint_text(&mut bytes, b"publisher", preparation.rights.publisher());
    append_fingerprint_text(
        &mut bytes,
        b"contributor_attestation",
        preparation.rights.contributor_attestation(),
    );
    append_fingerprint_text(
        &mut bytes,
        b"redistribution_policy",
        preparation.rights.redistribution_policy(),
    );
    Ok(Sha256::digest(bytes).into())
}

#[allow(clippy::too_many_arguments)] // canonical 0131 v1 axes must remain byte-for-byte stable
fn append_fingerprint_v1_axes(
    bytes: &mut Vec<u8>,
    tenant: Uuid,
    user: Uuid,
    sources: &[ContributionSourceSnapshot],
    manifest: ContentSha256,
    policy: Uuid,
    policy_version: i64,
    domain: Uuid,
    binding: Uuid,
    binding_version: i64,
    coverage_version: i64,
    assessment_version: i64,
) {
    for id in [tenant, user, policy, domain, binding] {
        bytes.extend_from_slice(id.as_bytes());
    }
    bytes.extend_from_slice(&policy_version.to_be_bytes());
    bytes.extend_from_slice(&binding_version.to_be_bytes());
    bytes.extend_from_slice(&coverage_version.to_be_bytes());
    bytes.extend_from_slice(&assessment_version.to_be_bytes());
    bytes.extend_from_slice(&manifest.0);
    bytes.extend_from_slice(&(sources.len() as u32).to_be_bytes());
    for source in sources {
        match source.source {
            ReleaseSource::Evidence(id) => {
                bytes.push(0);
                bytes.extend_from_slice(id.0.as_bytes())
            }
            ReleaseSource::Memory(id) => {
                bytes.push(1);
                bytes.extend_from_slice(id.0.as_bytes())
            }
        };
        bytes.extend_from_slice(&source.current_hash.0);
    }
}

fn append_fingerprint_text(bytes: &mut Vec<u8>, field: &[u8], value: Option<&str>) {
    bytes.extend_from_slice(&(field.len() as u16).to_be_bytes());
    bytes.extend_from_slice(field);
    match value {
        Some(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
            bytes.extend_from_slice(value.as_bytes());
        }
        None => bytes.push(0),
    }
}

#[cfg(test)]
mod read_contract_tests {
    use super::*;

    fn source(ordinal: i32) -> ContributionExecutionSource {
        ContributionExecutionSource {
            ordinal,
            kind: ContributionExecutionSourceKind::Evidence,
            source_id: Uuid::from_u128(1),
            source_hash: ContentSha256([7; 32]),
        }
    }

    #[test]
    fn read_parsers_fail_closed_for_unknown_state_type_and_hash_shape() {
        assert!(matches!(
            parse_state("UNKNOWN"),
            Err(ContributionExecutionRepoError::UnknownState(_))
        ));
        assert!(parse_source_kind(Some("OTHER")).is_err());
        assert!(parse_source_kind(None).is_err());
        assert!(hash32(vec![0; 31]).is_err());
        assert!(hash32(vec![0; 33]).is_err());
    }

    #[test]
    fn read_manifest_rejects_ordinal_drift() {
        let valid = [source(0)];
        let digest = ContentSha256(manifest_digest(&valid));
        assert!(validate_sources(1, &valid, digest).is_ok());
        let invalid = [source(1)];
        assert!(validate_sources(1, &invalid, digest).is_err());
        let mut hash_invalid = [source(0)];
        hash_invalid[0].source_hash.0[0] ^= 1;
        assert!(validate_sources(1, &hash_invalid, digest).is_err());
        assert!(validate_sources(2, &valid, digest).is_err());
    }

    #[test]
    fn id_free_reservation_plan_rejects_invalid_metadata() {
        let valid = || {
            ContributionReservePlan::try_new(
                ReasoningIntentSha256([1; 32]),
                Uuid::from_u128(1),
                None,
                DataClass::Private,
                ContentSha256([2; 32]),
                12,
            )
        };
        assert!(valid().is_ok());
        assert!(
            ContributionReservePlan::try_new(
                ReasoningIntentSha256([1; 32]),
                Uuid::nil(),
                None,
                DataClass::Private,
                ContentSha256([2; 32]),
                12,
            )
            .is_err()
        );
        assert!(
            ContributionReservePlan::try_new(
                ReasoningIntentSha256([1; 32]),
                Uuid::from_u128(1),
                Some(DisclosureScope {
                    scope_kind: "",
                    scope_id: Uuid::from_u128(2),
                }),
                DataClass::Private,
                ContentSha256([2; 32]),
                12,
            )
            .is_err()
        );
        assert!(
            ContributionReservePlan::try_new(
                ReasoningIntentSha256([1; 32]),
                Uuid::from_u128(1),
                None,
                DataClass::Private,
                ContentSha256([2; 32]),
                -1,
            )
            .is_err()
        );
    }

    #[test]
    fn prepared_route_expectation_is_closed_and_carried_to_reservation_input() {
        let expected = PreparedRouteExpectation {
            schema_version: 1,
            tenant_id: Uuid::from_u128(11),
            binding_id: Uuid::from_u128(12),
            binding_version: 13,
            reasoning_domain_id: Uuid::from_u128(14),
            purpose: "CONTRIBUTION_DEIDENTIFY",
            route_policy_id: Uuid::from_u128(1),
            route_policy_version: 2,
            profile_id: Uuid::from_u128(3),
            profile_version: 4,
            provider_account_id: Uuid::from_u128(5),
            processor_id: "processor".into(),
            processor_model_id: Uuid::from_u128(15),
            provider_model_id: "model".into(),
            model_revision: Some("revision".into()),
            provider_endpoint_id: Uuid::from_u128(6),
            egress_processor_id: Uuid::from_u128(7),
            endpoint_ref: "endpoint".into(),
            region: "region".into(),
            service_tier: "tier".into(),
            credential_ref: Uuid::from_u128(8),
            billing_account_id: Some(Uuid::from_u128(9)),
            billing_instrument_id: None,
        };
        let plan = ContributionReservePlan::try_new(
            ReasoningIntentSha256([1; 32]),
            Uuid::from_u128(10),
            None,
            DataClass::Private,
            ContentSha256([2; 32]),
            12,
        )
        .unwrap()
        .with_prepared_route(expected.clone());

        let json = expected.json();
        assert_eq!(json.as_object().unwrap().len(), 23);
        assert_eq!(json["schema_version"], 1);
        assert_eq!(json["tenant_id"], Uuid::from_u128(11).to_string());
        assert_eq!(json["binding_version"], 13);
        assert_eq!(json["purpose"], "CONTRIBUTION_DEIDENTIFY");
        assert_eq!(json["route_policy_id"], Uuid::from_u128(1).to_string());
        assert_eq!(json["route_policy_version"], 2);
        assert_eq!(json["processor_model_id"], Uuid::from_u128(15).to_string());
        assert_eq!(json["model_revision"], "revision");
        assert_eq!(json["billing_instrument_id"], serde_json::Value::Null);
        assert!(plan.mint_input().prepared_route.is_some());
    }
}
