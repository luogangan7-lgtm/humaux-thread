//! `adapters::private_inference_rpc` — §11.8 ADR-0012-pattern repository for
//! `ops.private_inference_rpc_calls` (migration 0143).
//!
//! One module for both sides of the RPC, same reasoning
//! `crate::retrieval_embedding_rpc`'s module doc gives for its own identical split:
//! [`ConsolidationRegistrations`] runs on [`ConsolidationDbPool`] (`role_consolidation_worker`,
//! INSERT + SELECT only — it never claims its own registration) and
//! [`PrivateWorkerInferenceCalls`] runs on [`PrivateWorkerDbPool`] (`role_private_worker`,
//! SELECT + a column-narrow claim/finish UPDATE only). Splitting this into two files would only
//! duplicate the row type and the `set_config('humaux.tenant_id', ...)` RLS-context helper both
//! sides need identically.

use sqlx::Row;
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;
use std::time::Duration;

use crate::postgres::{ConsolidationDbPool, PrivateWorkerDbPool};

#[derive(Debug)]
pub enum PrivateInferenceRpcError {
    Db(sqlx::Error),
    InvalidInput,
}

impl From<sqlx::Error> for PrivateInferenceRpcError {
    fn from(error: sqlx::Error) -> Self {
        Self::Db(error)
    }
}

impl std::fmt::Display for PrivateInferenceRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(error) => write!(f, "private inference rpc DB error: {error}"),
            Self::InvalidInput => write!(f, "private inference rpc call metadata is invalid"),
        }
    }
}

impl std::error::Error for PrivateInferenceRpcError {}

async fn set_tenant_local(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT set_config('humaux.tenant_id', $1, true)")
        .bind(tenant_id.to_string())
        .execute(&mut **txn)
        .await?;
    Ok(())
}

/// §11.8's closed `PrivateReasoningPurpose` set, DB wire form — literally the Rust enum's
/// variant name (matches migration 0143's own CHECK list).
pub fn purpose_db_str(
    purpose: humaux_application::consolidate::PrivateReasoningPurpose,
) -> &'static str {
    use humaux_application::consolidate::PrivateReasoningPurpose as P;
    match purpose {
        P::Distill => "Distill",
        P::Consolidate => "Consolidate",
        P::Vision => "Vision",
        P::ContributionDeidentify => "ContributionDeidentify",
    }
}

// ============================================================================
// Consolidation side (`role_consolidation_worker`, `ConsolidationDbPool`) — register only.
// ============================================================================

/// The registration a [`humaux_application::consolidate::PrivateReasoningPort`] implementation
/// reserves before it ever dials the private worker (idempotency anchor, mirrors
/// `retrieval_embedding_rpc::RegisterCall`).
pub struct RegisterCall {
    pub call_id: Uuid,
    pub tenant_id: Uuid,
    pub reasoning_domain_id: Uuid,
    pub binding_id: Uuid,
    pub binding_version: i64,
    pub purpose: &'static str,
    pub input_manifest_hash: [u8; 32],
    /// §11.8/ADR-0015 (migration 0145): the `private.memory_consolidation_runs` row a
    /// `Consolidate` call reasons over — the private worker locates the run's recorded inputs
    /// by this id and re-verifies `input_manifest_hash` over them. `None` for every other
    /// purpose (the sealed request itself stays identifiers + manifest hash only).
    pub consolidation_run_id: Option<Uuid>,
    pub ttl: Duration,
}

pub struct ConsolidationRegistrations<'a> {
    pool: &'a ConsolidationDbPool,
}

impl<'a> ConsolidationRegistrations<'a> {
    pub fn new(pool: &'a ConsolidationDbPool) -> Self {
        Self { pool }
    }

    /// Inserts one registration row keyed by `call_id` — the caller (`UdsInferenceClient`)
    /// mints one fresh `call_id` per logical `run_once` inference call and never retries with
    /// a different id for the same logical attempt (there is no cross-process retry path for
    /// this call site today; a transport failure surfaces as `PrivateReasoningError` and the
    /// whole consolidation run fails, to be retried as a fresh run — §11.7's run/retry unit is
    /// the whole run, not a sub-step).
    pub async fn register(&self, call: &RegisterCall) -> Result<(), PrivateInferenceRpcError> {
        if call.binding_version <= 0 || call.ttl.is_zero() {
            return Err(PrivateInferenceRpcError::InvalidInput);
        }
        let mut txn = self.pool.pool().begin().await?;
        set_tenant_local(&mut txn, call.tenant_id).await?;
        sqlx::query(
            "INSERT INTO ops.private_inference_rpc_calls \
               (call_id, tenant_id, reasoning_domain_id, binding_id, binding_version, purpose, \
                input_manifest_hash, expires_at, consolidation_run_id) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, clock_timestamp() + make_interval(secs => $8), $9)",
        )
        .bind(call.call_id)
        .bind(call.tenant_id)
        .bind(call.reasoning_domain_id)
        .bind(call.binding_id)
        .bind(call.binding_version)
        .bind(call.purpose)
        .bind(call.input_manifest_hash.to_vec())
        .bind(call.ttl.as_secs_f64())
        .bind(call.consolidation_run_id)
        .execute(&mut *txn)
        .await?;
        txn.commit().await?;
        Ok(())
    }
}

// ============================================================================
// Private-worker side (`role_private_worker`, `PrivateWorkerDbPool`) — claim + finish.
// ============================================================================

/// A freshly claimed registration — every field the private worker needs to rebuild a
/// `SealedPrivateReasoningRequest` without trusting anything the RPC body itself asserted.
#[derive(Debug, Clone)]
pub struct ClaimedRegistration {
    pub tenant_id: Uuid,
    pub reasoning_domain_id: Uuid,
    pub binding_id: Uuid,
    pub binding_version: i64,
    pub purpose: String,
    pub input_manifest_hash: Vec<u8>,
    /// See [`RegisterCall::consolidation_run_id`].
    pub consolidation_run_id: Option<Uuid>,
}

/// A previously completed call's stored outcome, replayed verbatim on a duplicate `call_id`
/// (§11.8/ADR-0012 "idempotency by call_id" — zero provider calls on replay).
#[derive(Debug, Clone)]
pub struct StoredOutcome {
    pub outcome: String,
    pub response_output_bytes: Option<Vec<u8>>,
    pub response_output_sha256: Option<Vec<u8>>,
    pub response_provider_trace: Option<String>,
    pub response_model_call_id: Option<Uuid>,
    pub response_failure_message: Option<String>,
}

/// [`PrivateWorkerInferenceCalls::load_and_claim`]'s branch outcomes — mirrors
/// `retrieval_embedding_rpc::ClaimOutcome` one for one.
pub enum ClaimOutcome {
    NotFound,
    Expired,
    AlreadyClaimed,
    Replay(StoredOutcome),
    Claimed(ClaimedRegistration),
}

/// The outcome to persist once a claimed call has actually run.
pub enum FinishOutcome {
    Completed {
        output_bytes: Vec<u8>,
        output_sha256: [u8; 32],
        provider_trace: String,
        model_call_id: Uuid,
    },
    Failed {
        message: String,
    },
}

pub struct PrivateWorkerInferenceCalls<'a> {
    pool: &'a PrivateWorkerDbPool,
}

impl<'a> PrivateWorkerInferenceCalls<'a> {
    pub fn new(pool: &'a PrivateWorkerDbPool) -> Self {
        Self { pool }
    }

    /// One transaction: locks the row, judges it, and — only on the fresh-claim branch —
    /// atomically transitions `REGISTERED -> CLAIMED`. See
    /// `retrieval_embedding_rpc::RetrievalWorkerEmbeddingCalls::load_and_claim`'s doc for the
    /// full per-branch reasoning this mirrors.
    pub async fn load_and_claim(
        &self,
        call_id: Uuid,
        tenant_hint: Uuid,
        claimed_by: &str,
    ) -> Result<ClaimOutcome, PrivateInferenceRpcError> {
        let mut txn = self.pool.pool().begin().await?;
        set_tenant_local(&mut txn, tenant_hint).await?;
        let row = sqlx::query(
            "SELECT tenant_id, reasoning_domain_id, binding_id, binding_version, purpose, \
                    input_manifest_hash, expires_at, state, outcome, response_output_bytes, \
                    response_output_sha256, response_provider_trace, response_model_call_id, \
                    response_failure_message, consolidation_run_id \
             FROM ops.private_inference_rpc_calls \
             WHERE call_id = $1 AND tenant_id = $2 \
             FOR UPDATE",
        )
        .bind(call_id)
        .bind(tenant_hint)
        .fetch_optional(&mut *txn)
        .await?;
        let Some(row) = row else {
            txn.commit().await?;
            return Ok(ClaimOutcome::NotFound);
        };
        let expires_at: OffsetDateTime = row.try_get("expires_at")?;
        if expires_at <= OffsetDateTime::now_utc() {
            txn.commit().await?;
            return Ok(ClaimOutcome::Expired);
        }
        let state: String = row.try_get("state")?;
        if state == "COMPLETED" {
            txn.commit().await?;
            return Ok(ClaimOutcome::Replay(StoredOutcome {
                outcome: row.try_get("outcome")?,
                response_output_bytes: row.try_get("response_output_bytes")?,
                response_output_sha256: row.try_get("response_output_sha256")?,
                response_provider_trace: row.try_get("response_provider_trace")?,
                response_model_call_id: row.try_get("response_model_call_id")?,
                response_failure_message: row.try_get("response_failure_message")?,
            }));
        }
        if state == "CLAIMED" {
            txn.commit().await?;
            return Ok(ClaimOutcome::AlreadyClaimed);
        }
        let claimed = sqlx::query(
            "UPDATE ops.private_inference_rpc_calls \
             SET state = 'CLAIMED', claimed_at = clock_timestamp(), claimed_by = $3 \
             WHERE call_id = $1 AND tenant_id = $2 AND state = 'REGISTERED' \
             RETURNING 1",
        )
        .bind(call_id)
        .bind(tenant_hint)
        .bind(claimed_by)
        .fetch_optional(&mut *txn)
        .await?;
        if claimed.is_none() {
            txn.commit().await?;
            return Ok(ClaimOutcome::AlreadyClaimed);
        }
        txn.commit().await?;
        Ok(ClaimOutcome::Claimed(ClaimedRegistration {
            tenant_id: row.try_get("tenant_id")?,
            reasoning_domain_id: row.try_get("reasoning_domain_id")?,
            binding_id: row.try_get("binding_id")?,
            binding_version: row.try_get("binding_version")?,
            purpose: row.try_get("purpose")?,
            input_manifest_hash: row.try_get("input_manifest_hash")?,
            consolidation_run_id: row.try_get("consolidation_run_id")?,
        }))
    }

    /// Persists the actual outcome of a claimed call. A failed persist here leaves the row
    /// stuck `CLAIMED` until TTL expiry — same documented tradeoff
    /// `retrieval_embedding_rpc::RetrievalWorkerEmbeddingCalls::finish`'s doc accepts, logged
    /// by the RPC handler rather than by this repo function.
    pub async fn finish(
        &self,
        call_id: Uuid,
        tenant_id: Uuid,
        outcome: FinishOutcome,
    ) -> Result<(), PrivateInferenceRpcError> {
        let mut txn = self.pool.pool().begin().await?;
        set_tenant_local(&mut txn, tenant_id).await?;
        match outcome {
            FinishOutcome::Completed {
                output_bytes,
                output_sha256,
                provider_trace,
                model_call_id,
            } => {
                sqlx::query(
                    "UPDATE ops.private_inference_rpc_calls \
                     SET state = 'COMPLETED', finished_at = clock_timestamp(), outcome = 'COMPLETED', \
                         response_output_bytes = $3, response_output_sha256 = $4, \
                         response_provider_trace = $5, response_model_call_id = $6 \
                     WHERE call_id = $1 AND tenant_id = $2 AND state = 'CLAIMED'",
                )
                .bind(call_id)
                .bind(tenant_id)
                .bind(output_bytes)
                .bind(output_sha256.to_vec())
                .bind(provider_trace)
                .bind(model_call_id)
                .execute(&mut *txn)
                .await?;
            }
            FinishOutcome::Failed { message } => {
                sqlx::query(
                    "UPDATE ops.private_inference_rpc_calls \
                     SET state = 'COMPLETED', finished_at = clock_timestamp(), outcome = 'FAILED', \
                         response_failure_message = $3 \
                     WHERE call_id = $1 AND tenant_id = $2 AND state = 'CLAIMED'",
                )
                .bind(call_id)
                .bind(tenant_id)
                .bind(message)
                .execute(&mut *txn)
                .await?;
            }
        }
        txn.commit().await?;
        Ok(())
    }
}
