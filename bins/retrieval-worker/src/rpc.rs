//! `retrieval-worker::rpc` — ADR-0012 query-embedding RPC — the Unix-domain-socket side `humaux-retrieval-worker`
//!   serves.
//! Depends-on: crates=[axum, humaux-adapters, humaux-domain, humaux-local-secret-scan, humaux-retrieval,
//!   humaux-retrieval-provider, serde, sha2, tokio, tracing,
//!   uuid]; services=[]; env=[]; modules=[adapters::postgres, adapters::retrieval_embedding_rpc, domain::error,
//!   domain::identity, domain::ids, humaux-local-secret-scan, retrieval-provider::contract, retrieval::request]
//! Called-by: [retrieval-worker::main, tests]
//! Invariants: [a malformed or unauthenticated RPC frame is rejected before it reaches the embedding provider call]
//! Spec: Baseline §2; §6; §7.5; ADR-0012; ADR-0056
//!
//! §决定2: the worker authenticates the **caller process** via the kernel peer credential
//! (`UnixStream::peer_cred()`), not the request body — [`PeerIdentity::connect_info`] captures
//! the credential once per accepted connection and [`require_gateway_uid`] rejects with
//! `403 FORBIDDEN` before the JSON body extractor for that request ever runs, matching §决定2's
//! "读 body 之前" ordering. §决定3: authorization still flows through the one closed
//! `IntraCellResource::RETRIEVAL_EMBEDDING_RPC` registry/permit — this module does not invent a
//! second identity/authorization type.

use std::sync::Arc;

use axum::extract::{ConnectInfo, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router, middleware};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use humaux_adapters::postgres::RetrievalWorkerDbPool;
use humaux_adapters::retrieval_embedding_rpc::{
    ClaimOutcome, ClaimedRegistration, FinishOutcome, RetrievalWorkerEmbeddingCalls, StoredOutcome,
};
use humaux_domain::error::ErrorCode;
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId};
use humaux_domain::ids::{TenantId, UserId, WorkspaceId};
use humaux_local_secret_scan::LocalSecretScanner;
use humaux_retrieval_provider::contract::{EmbeddingProvider, RetrievalQueryCallContext};

/// Wire request — ADR-0012 §2 verbatim field set. `query` is the only sensitive field, and it
/// is exactly what the design says crosses the wire: raw text, never sealed/embedded on the
/// Gateway side.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryEmbeddingRpcRequest {
    schema_version: u16,
    call_id: Uuid,
    tenant_hint: Uuid,
    query: String,
}

/// Wire response envelope — a closed `outcome` tag plus the fields that apply to it. Never a
/// route id, credential, or provider request/response body.
#[derive(Debug, Serialize)]
struct QueryEmbeddingRpcEnvelope {
    schema_version: u16,
    call_id: Uuid,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    vector: Option<Vec<f32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model_revision: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dimension: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure_code: Option<String>,
}

const SCHEMA_VERSION: u16 = 1;

/// Per-connection identity captured once at accept time. `ok = false` means `peer_cred()`
/// itself failed (e.g. an already-closed socket) — treated identically to a uid mismatch, both
/// fail closed.
#[derive(Debug, Clone, Copy)]
pub struct PeerIdentity {
    uid: u32,
    ok: bool,
}

impl
    axum::extract::connect_info::Connected<
        axum::serve::IncomingStream<'_, tokio::net::UnixListener>,
    > for PeerIdentity
{
    fn connect_info(stream: axum::serve::IncomingStream<'_, tokio::net::UnixListener>) -> Self {
        match stream.io().peer_cred() {
            Ok(cred) => PeerIdentity {
                uid: cred.uid(),
                ok: true,
            },
            Err(_) => PeerIdentity { uid: 0, ok: false },
        }
    }
}

pub struct RpcState {
    pub expected_gateway_uid: u32,
    pub calls: RetrievalWorkerDbPool,
    pub scanner: Arc<LocalSecretScanner>,
    pub embedder: Arc<dyn EmbeddingProvider>,
    pub dimension: u32,
    pub provider_id: String,
}

/// §决定2: rejects before the handler's `Json<QueryEmbeddingRpcRequest>` extractor ever parses
/// the body. Runs as an outer `middleware::from_fn_with_state` layer, so `next.run` is the
/// first point the body is touched at all on the accepted path.
async fn require_gateway_uid(
    State(state): State<Arc<RpcState>>,
    ConnectInfo(peer): ConnectInfo<PeerIdentity>,
    request: axum::extract::Request,
    next: middleware::Next,
) -> Response {
    if !peer.ok || peer.uid != state.expected_gateway_uid {
        return StatusCode::FORBIDDEN.into_response();
    }
    next.run(request).await
}

pub fn router(state: Arc<RpcState>) -> Router {
    Router::new()
        .route(
            "/internal/v1/retrieval/query-embedding",
            post(query_embedding_handler),
        )
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_gateway_uid,
        ))
        .with_state(state)
}

enum WorkerRpcError {
    NotFound,
    QueryMismatch,
    Expired,
}

async fn query_embedding_handler(
    State(state): State<Arc<RpcState>>,
    Json(wire): Json<QueryEmbeddingRpcRequest>,
) -> Response {
    if wire.schema_version != SCHEMA_VERSION {
        return (
            StatusCode::BAD_REQUEST,
            "unsupported retrieval embedding RPC schema",
        )
            .into_response();
    }
    match execute(&state, wire).await {
        Ok(envelope) => (StatusCode::OK, Json(envelope)).into_response(),
        Err(WorkerRpcError::NotFound) => StatusCode::NOT_FOUND.into_response(),
        Err(WorkerRpcError::QueryMismatch) => StatusCode::CONFLICT.into_response(),
        Err(WorkerRpcError::Expired) => StatusCode::GONE.into_response(),
    }
}

fn envelope_from_stored(call_id: Uuid, stored: StoredOutcome) -> QueryEmbeddingRpcEnvelope {
    let outcome: &'static str = match stored.outcome.as_str() {
        "EMBEDDED" => "EMBEDDED",
        "SKIPPED" => "SKIPPED",
        _ => "UNAVAILABLE",
    };
    QueryEmbeddingRpcEnvelope {
        schema_version: SCHEMA_VERSION,
        call_id,
        outcome,
        vector: stored.response_vector,
        provider_id: stored.response_provider_id,
        model_id: stored.response_model_id,
        model_revision: stored.response_model_revision,
        dimension: stored
            .response_dimension
            .and_then(|d| u32::try_from(d).ok()),
        failure_code: stored.response_failure_code,
    }
}

fn unavailable_envelope(call_id: Uuid, reason: &str) -> QueryEmbeddingRpcEnvelope {
    QueryEmbeddingRpcEnvelope {
        schema_version: SCHEMA_VERSION,
        call_id,
        outcome: "UNAVAILABLE",
        vector: None,
        provider_id: None,
        model_id: None,
        model_revision: None,
        dimension: None,
        failure_code: Some(reason.to_owned()),
    }
}

async fn execute(
    state: &Arc<RpcState>,
    wire: QueryEmbeddingRpcRequest,
) -> Result<QueryEmbeddingRpcEnvelope, WorkerRpcError> {
    let query_sha256: [u8; 32] = Sha256::digest(wire.query.as_bytes()).into();
    let repo = RetrievalWorkerEmbeddingCalls::new(&state.calls);
    let claim = repo
        .load_and_claim(
            wire.call_id,
            wire.tenant_hint,
            query_sha256,
            "role_retrieval_worker",
        )
        .await
        .map_err(|_| WorkerRpcError::NotFound)?;

    let registration = match claim {
        ClaimOutcome::NotFound => return Err(WorkerRpcError::NotFound),
        ClaimOutcome::Expired => return Err(WorkerRpcError::Expired),
        ClaimOutcome::QueryMismatch => return Err(WorkerRpcError::QueryMismatch),
        ClaimOutcome::AlreadyClaimed => {
            return Ok(unavailable_envelope(wire.call_id, "WORKER_BUSY"));
        }
        ClaimOutcome::Replay(stored) => return Ok(envelope_from_stored(wire.call_id, stored)),
        ClaimOutcome::Claimed(registration) => registration,
    };

    let envelope = embed(state, wire.call_id, &wire.query, &registration).await;
    let finish_outcome = match &envelope {
        e if e.outcome == "EMBEDDED" => FinishOutcome::Embedded {
            vector: e.vector.clone().unwrap_or_default(),
            provider_id: e.provider_id.clone().unwrap_or_default(),
            model_id: e.model_id.clone().unwrap_or_default(),
            model_revision: e.model_revision.clone().unwrap_or_default(),
            dimension: e.dimension.unwrap_or_default(),
        },
        e if e.outcome == "SKIPPED" => FinishOutcome::Skipped,
        e => FinishOutcome::Unavailable {
            failure_code: e
                .failure_code
                .clone()
                .unwrap_or_else(|| "UNKNOWN".to_owned()),
        },
    };
    // A failed persist leaves the row stuck in `CLAIMED` (finish's own `WHERE state =
    // 'CLAIMED'` never matches again once it's already there) — every retry within the row's
    // TTL then misreads as `AlreadyClaimed`/`WORKER_BUSY` with zero visibility into why. This
    // is the only signal an operator gets before that TTL expires, so it must not be silent.
    if let Err(error) = repo
        .finish(wire.call_id, registration.tenant_id, finish_outcome)
        .await
    {
        tracing::error!(
            call_id = %wire.call_id,
            tenant_id = %registration.tenant_id,
            %error,
            "retrieval_embedding_rpc: finish() failed to persist call outcome; row stuck CLAIMED until TTL expiry"
        );
    }
    Ok(envelope)
}

/// Re-derives an `AuthorizationScope` from the claimed registration row (never trusting the
/// RPC body's own `tenant_hint` as authorization evidence — ADR-0012 §2), seals the raw query
/// text with the worker-local scanner, and calls the real `EmbeddingProvider` exactly once
/// with a one-element slice (query_embed_rpc card §6 resolution).
///
/// ponytail: builds the sealed query against the platform-default registered retrieval
/// profile rather than reconciling it against the caller's own `profile_fingerprint` (that
/// would need this process to read the same `HUMAUX_GATEWAY_RETRIEVAL_PROFILE_*` config the
/// Gateway resolved from, which is out of this card's file scope) — upgrade to a real
/// stored-profile equality check once a profile registry object exists to compare against.
async fn embed(
    state: &Arc<RpcState>,
    call_id: Uuid,
    query: &str,
    registration: &ClaimedRegistration,
) -> QueryEmbeddingRpcEnvelope {
    let Some(authorization) = build_authorization(registration) else {
        return unavailable_envelope(call_id, "INVALID_REGISTRATION");
    };
    let Ok(call_context) = RetrievalQueryCallContext::new(
        &authorization,
        WorkspaceId(registration.workspace_id),
        registration.request_id,
        registration.logical_call_id,
        registration.attempt_no,
    ) else {
        return unavailable_envelope(call_id, "INVALID_REGISTRATION");
    };
    let Ok(profile) =
        humaux_retrieval::request::resolve_registered_retrieval_profile(&Default::default())
    else {
        return unavailable_envelope(call_id, "PROFILE_UNAVAILABLE");
    };
    let Ok(intent) = humaux_retrieval::request::RetrievalIntent::new(
        query.to_owned(),
        Vec::new(),
        Default::default(),
        Default::default(),
    ) else {
        return unavailable_envelope(call_id, "INVALID_QUERY");
    };
    let Ok(request) = humaux_retrieval::request::build_request(intent, &profile) else {
        return unavailable_envelope(call_id, "INVALID_QUERY");
    };
    // ADR-0056 D-C: the recall path's one query seal (§7.5 C, the gateway no longer scans). It
    // blocks on the pinned gitleaks spawn, so it runs off the async workers (ADR-0055 item 3).
    // Only a gitleaks finding is SCAN_REJECTED (the gateway's FORBIDDEN); a scanner that did not
    // run is SCANNER_UNAVAILABLE, never a verdict on the query.
    let scanner = Arc::clone(&state.scanner);
    let sealed = match tokio::task::spawn_blocking(move || {
        let trusted_query = request.trusted_query().ok_or(ErrorCode::InvalidInput)?;
        scanner.seal_query(&trusted_query)
    })
    .await
    {
        Ok(Ok(sealed)) => sealed,
        Ok(Err(ErrorCode::Forbidden)) => return unavailable_envelope(call_id, "SCAN_REJECTED"),
        Ok(Err(ErrorCode::InvalidInput)) => return unavailable_envelope(call_id, "INVALID_QUERY"),
        Ok(Err(_)) | Err(_) => return unavailable_envelope(call_id, "SCANNER_UNAVAILABLE"),
    };
    match state
        .embedder
        .embed_queries(
            &call_context,
            state.dimension,
            std::slice::from_ref(&sealed),
        )
        .await
    {
        Ok(batch) => match batch.vectors.into_iter().next() {
            Some(vector) => QueryEmbeddingRpcEnvelope {
                schema_version: SCHEMA_VERSION,
                call_id,
                outcome: "EMBEDDED",
                vector: Some(vector),
                provider_id: Some(state.provider_id.clone()),
                model_id: Some(state.embedder.model().model_id.0.clone()),
                model_revision: Some(state.embedder.model().model_revision.clone()),
                dimension: Some(batch.dimension),
                failure_code: None,
            },
            None => unavailable_envelope(call_id, "EMPTY_BATCH"),
        },
        Err(error) => unavailable_envelope(call_id, error.as_str()),
    }
}

fn build_authorization(registration: &ClaimedRegistration) -> Option<AuthorizationScope> {
    let workspace_id = WorkspaceId(registration.workspace_id);
    let allowed = BoundedSet::new([workspace_id]).ok()?;
    Some(AuthorizationScope::new(
        TenantId(registration.tenant_id),
        PrincipalId(registration.principal_id),
        Some(UserId(registration.user_id)),
        allowed,
    ))
}
