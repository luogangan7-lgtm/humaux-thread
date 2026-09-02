//! `adapters::retrieval_embedding_rpc` — ADR-0012 repository for
//! `ops.retrieval_embedding_rpc_calls` (migration 0141).
//!
//! One module for both sides of the RPC because they share one row shape under one RLS-scoped
//! table with disjoint column GRANTs (migration 0141): [`GatewayRetrievalEmbeddingRegistrations`]
//! runs on [`RuntimeDbPool`] (`role_gateway`, INSERT + SELECT only — no UPDATE, it never claims
//! its own registration) and [`RetrievalWorkerEmbeddingCalls`] runs on
//! [`RetrievalWorkerDbPool`] (`role_retrieval_worker`, SELECT + a column-narrow claim/finish
//! UPDATE only). Splitting this into two files would only duplicate the row type and the
//! `set_config('humaux.tenant_id', ...)` RLS-context helper both sides need identically.

use sqlx::Row;
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;
use std::time::Duration;

use crate::postgres::{RetrievalWorkerDbPool, RuntimeDbPool};

#[derive(Debug)]
pub enum RetrievalEmbeddingRpcError {
    Db(sqlx::Error),
    InvalidInput,
}

impl From<sqlx::Error> for RetrievalEmbeddingRpcError {
    fn from(error: sqlx::Error) -> Self {
        Self::Db(error)
    }
}

impl std::fmt::Display for RetrievalEmbeddingRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(error) => write!(f, "retrieval embedding rpc DB error: {error}"),
            Self::InvalidInput => write!(f, "retrieval embedding rpc call metadata is invalid"),
        }
    }
}

impl std::error::Error for RetrievalEmbeddingRpcError {}

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

// ============================================================================
// Gateway side (`role_gateway`, `RuntimeDbPool`) — register only.
// ============================================================================

/// The registration a real [`humaux_application::retrieval_embedding_port::RetrievalEmbeddingPort`]
/// implementation reserves before it ever dials the worker (ADR-0012 §决定2's "registration =
/// idempotency anchor").
pub struct RegisterCall {
    pub call_id: Uuid,
    pub tenant_id: Uuid,
    pub principal_id: Uuid,
    pub user_id: Uuid,
    pub workspace_id: Uuid,
    pub request_id: Uuid,
    pub logical_call_id: Uuid,
    pub attempt_no: i32,
    pub profile_fingerprint: String,
    pub query_sha256: [u8; 32],
    pub ttl: Duration,
}

pub struct GatewayRetrievalEmbeddingRegistrations<'a> {
    pool: &'a RuntimeDbPool,
}

impl<'a> GatewayRetrievalEmbeddingRegistrations<'a> {
    pub fn new(pool: &'a RuntimeDbPool) -> Self {
        Self { pool }
    }

    /// Inserts one registration row, or — if a row already exists for this caller's
    /// `(tenant_id, logical_call_id, attempt_no)` (migration 0141's unique index) — leaves it
    /// untouched and returns *that* row's `call_id` instead. This is the idempotency anchor for
    /// a gateway-side retry: a caller retry must reuse the same registration/`call_id`, never
    /// mint and register a fresh one for the same logical attempt (query_embed_rpc card,
    /// "Failure semantics"). Migration 0141's own CHECK constraints (non-nil
    /// principal/user/workspace, `attempt_no > 0`, `expires_at > registered_at`) are the real
    /// fail-closed boundary; the checks here only avoid a wasted round trip for input this
    /// side can already tell is malformed.
    pub async fn register(&self, call: &RegisterCall) -> Result<Uuid, RetrievalEmbeddingRpcError> {
        if call.attempt_no <= 0
            || call.profile_fingerprint.trim().is_empty()
            || call.ttl.is_zero()
            || call.principal_id.is_nil()
            || call.user_id.is_nil()
            || call.workspace_id.is_nil()
        {
            return Err(RetrievalEmbeddingRpcError::InvalidInput);
        }
        let mut txn = self.pool.pool().begin().await?;
        set_tenant_local(&mut txn, call.tenant_id).await?;
        let inserted = sqlx::query(
            "INSERT INTO ops.retrieval_embedding_rpc_calls \
               (call_id, tenant_id, principal_id, user_id, workspace_id, request_id, \
                logical_call_id, attempt_no, profile_fingerprint, query_sha256, expires_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, \
                     clock_timestamp() + make_interval(secs => $11)) \
             ON CONFLICT (tenant_id, logical_call_id, attempt_no) DO NOTHING \
             RETURNING call_id",
        )
        .bind(call.call_id)
        .bind(call.tenant_id)
        .bind(call.principal_id)
        .bind(call.user_id)
        .bind(call.workspace_id)
        .bind(call.request_id)
        .bind(call.logical_call_id)
        .bind(call.attempt_no)
        .bind(&call.profile_fingerprint)
        .bind(call.query_sha256.to_vec())
        .bind(call.ttl.as_secs_f64())
        .fetch_optional(&mut *txn)
        .await?;
        let call_id = match inserted {
            Some(row) => row.try_get("call_id")?,
            None => {
                let existing = sqlx::query(
                    "SELECT call_id FROM ops.retrieval_embedding_rpc_calls \
                     WHERE tenant_id = $1 AND logical_call_id = $2 AND attempt_no = $3",
                )
                .bind(call.tenant_id)
                .bind(call.logical_call_id)
                .bind(call.attempt_no)
                .fetch_one(&mut *txn)
                .await?;
                existing.try_get("call_id")?
            }
        };
        txn.commit().await?;
        Ok(call_id)
    }
}

// ============================================================================
// Worker side (`role_retrieval_worker`, `RetrievalWorkerDbPool`) — claim + finish.
// ============================================================================

/// A freshly claimed registration — every field the worker needs to re-derive an
/// `AuthorizationScope` and build a `RetrievalQueryCallContext` without trusting anything the
/// RPC body itself asserted about identity.
#[derive(Debug, Clone)]
pub struct ClaimedRegistration {
    pub tenant_id: Uuid,
    pub principal_id: Uuid,
    pub user_id: Uuid,
    pub workspace_id: Uuid,
    pub request_id: Uuid,
    pub logical_call_id: Uuid,
    pub attempt_no: i32,
    pub profile_fingerprint: String,
}

/// A previously completed call's stored outcome, replayed verbatim on a duplicate `call_id`
/// (ADR-0012 §2 "idempotency by call_id" — zero provider calls on replay).
#[derive(Debug, Clone)]
pub struct StoredOutcome {
    pub outcome: String,
    pub response_vector: Option<Vec<f32>>,
    pub response_provider_id: Option<String>,
    pub response_model_id: Option<String>,
    pub response_model_revision: Option<String>,
    pub response_dimension: Option<i32>,
    pub response_failure_code: Option<String>,
}

/// [`RetrievalWorkerEmbeddingCalls::load_and_claim`]'s three-way branch — every distinct
/// action the RPC handler must take, named explicitly so a caller cannot mistake "already
/// claimed by someone else" for "freshly claimed by me".
pub enum ClaimOutcome {
    NotFound,
    Expired,
    /// `query_sha256` in the RPC body did not match the registration — ADR-0012 §2 "409, zero
    /// side effects": the row is left completely untouched (no claim).
    QueryMismatch,
    /// `claimed_at` is set but `completed_at` is not — a concurrent delivery or a
    /// crash-after-dispatch-before-persist. ADR-0012 §2: `Unavailable(WorkerBusy)`, zero
    /// provider calls, no further row mutation.
    AlreadyClaimed,
    Replay(StoredOutcome),
    Claimed(ClaimedRegistration),
}

pub struct RetrievalWorkerEmbeddingCalls<'a> {
    pool: &'a RetrievalWorkerDbPool,
}

impl<'a> RetrievalWorkerEmbeddingCalls<'a> {
    pub fn new(pool: &'a RetrievalWorkerDbPool) -> Self {
        Self { pool }
    }

    /// One transaction: locks the row, judges it, and — only on the fresh-claim branch —
    /// atomically transitions `REGISTERED -> CLAIMED` before returning. Every other branch
    /// commits with the row unchanged, matching ADR-0012 §2's "zero side effects" contract for
    /// query mismatch and "zero provider calls" for replay/busy.
    pub async fn load_and_claim(
        &self,
        call_id: Uuid,
        tenant_hint: Uuid,
        query_sha256: [u8; 32],
        claimed_by: &str,
    ) -> Result<ClaimOutcome, RetrievalEmbeddingRpcError> {
        let mut txn = self.pool.pool().begin().await?;
        set_tenant_local(&mut txn, tenant_hint).await?;
        let row = sqlx::query(
            "SELECT tenant_id, principal_id, user_id, workspace_id, request_id, \
                    logical_call_id, attempt_no, profile_fingerprint, query_sha256, \
                    expires_at, state, outcome, response_vector, response_provider_id, \
                    response_model_id, response_model_revision, response_dimension, \
                    response_failure_code \
             FROM ops.retrieval_embedding_rpc_calls \
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
        // §决定2/ADR-0012 §2: the integrity check must gate every branch, including replay —
        // a completed row whose stored query no longer matches the RPC body is a mismatch,
        // not a servable replay, or a call_id collision would silently hand back someone
        // else's vector instead of 409ing.
        let stored_sha256: Vec<u8> = row.try_get("query_sha256")?;
        if stored_sha256 != query_sha256 {
            txn.commit().await?;
            return Ok(ClaimOutcome::QueryMismatch);
        }
        if state == "COMPLETED" {
            txn.commit().await?;
            return Ok(ClaimOutcome::Replay(StoredOutcome {
                outcome: row.try_get("outcome")?,
                response_vector: row.try_get("response_vector")?,
                response_provider_id: row.try_get("response_provider_id")?,
                response_model_id: row.try_get("response_model_id")?,
                response_model_revision: row.try_get("response_model_revision")?,
                response_dimension: row.try_get("response_dimension")?,
                response_failure_code: row.try_get("response_failure_code")?,
            }));
        }
        if state == "CLAIMED" {
            txn.commit().await?;
            return Ok(ClaimOutcome::AlreadyClaimed);
        }
        let claimed = sqlx::query(
            "UPDATE ops.retrieval_embedding_rpc_calls \
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
            // Lost a race against another claimant between the SELECT above and this UPDATE.
            txn.commit().await?;
            return Ok(ClaimOutcome::AlreadyClaimed);
        }
        txn.commit().await?;
        Ok(ClaimOutcome::Claimed(ClaimedRegistration {
            tenant_id: row.try_get("tenant_id")?,
            principal_id: row.try_get("principal_id")?,
            user_id: row.try_get("user_id")?,
            workspace_id: row.try_get("workspace_id")?,
            request_id: row.try_get("request_id")?,
            logical_call_id: row.try_get("logical_call_id")?,
            attempt_no: row.try_get("attempt_no")?,
            profile_fingerprint: row.try_get("profile_fingerprint")?,
        }))
    }

    /// The outcome to persist once a claimed call has actually run — `Embedded` requires the
    /// full non-secret model-stamp field set (mirrors migration 0141's own CHECK).
    pub async fn finish(
        &self,
        call_id: Uuid,
        tenant_id: Uuid,
        outcome: FinishOutcome,
    ) -> Result<(), RetrievalEmbeddingRpcError> {
        let (outcome_str, vector, provider_id, model_id, model_revision, dimension, failure_code) =
            match outcome {
                FinishOutcome::Embedded {
                    vector,
                    provider_id,
                    model_id,
                    model_revision,
                    dimension,
                } => (
                    "EMBEDDED",
                    Some(vector),
                    Some(provider_id),
                    Some(model_id),
                    Some(model_revision),
                    Some(dimension as i32),
                    None,
                ),
                FinishOutcome::Skipped => ("SKIPPED", None, None, None, None, None, None),
                FinishOutcome::Unavailable { failure_code } => (
                    "UNAVAILABLE",
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(failure_code),
                ),
            };
        let mut txn = self.pool.pool().begin().await?;
        set_tenant_local(&mut txn, tenant_id).await?;
        sqlx::query(
            "UPDATE ops.retrieval_embedding_rpc_calls \
             SET state = 'COMPLETED', finished_at = clock_timestamp(), outcome = $3, \
                 response_vector = $4, response_provider_id = $5, response_model_id = $6, \
                 response_model_revision = $7, response_dimension = $8, response_failure_code = $9 \
             WHERE call_id = $1 AND tenant_id = $2 AND state = 'CLAIMED'",
        )
        .bind(call_id)
        .bind(tenant_id)
        .bind(outcome_str)
        .bind(vector)
        .bind(provider_id)
        .bind(model_id)
        .bind(model_revision)
        .bind(dimension)
        .bind(failure_code)
        .execute(&mut *txn)
        .await?;
        txn.commit().await?;
        Ok(())
    }
}

/// [`RetrievalWorkerEmbeddingCalls::finish`]'s input — a closed set matching migration 0141's
/// `outcome` CHECK exactly.
pub enum FinishOutcome {
    Embedded {
        vector: Vec<f32>,
        provider_id: String,
        model_id: String,
        model_revision: String,
        dimension: u32,
    },
    Skipped,
    Unavailable {
        failure_code: String,
    },
}
