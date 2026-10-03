//! `private-worker::inference_rpc` — §11.8 ADR-0012-pattern inference-only RPC — the Unix-domain-socket side
//!   `humaux-private-worker` serves for `humaux-consolidation-worker`.
//! Depends-on: crates=[axum, hex, humaux-adapters, humaux-application, serde, tokio, uuid]; services=[PostgreSQL(role_private_worker), UDS(serve)]; env=[]; modules=[adapters::consolidation_reasoner, adapters::contribution_reasoner, adapters::disclosure, adapters::postgres, adapters::private_inference_rpc, adapters::reasoning_route_admission, application::consolidate, humaux-private-worker]
//! Called-by: [private-worker::main, tests]
//! Invariants: [the caller is authenticated by kernel peer credential before the body is read; every field reasoned
//!   over comes from the claimed ops.private_inference_rpc_calls row, never the wire body; unknown or expired calls
//!   are NotFound/Expired; a consolidation call that SUCCEEDED or met a rejected key is reported as worker-observed
//!   route health (ruling E3)]
//! Spec: Baseline §11.8; ADR-0015; ADR-0060 D-E; ADR-0060 D-M; ADR-0060 E3
//!
//! Mirrors
//! `bins/retrieval-worker/src/rpc.rs` verbatim in shape: the worker authenticates the **caller
//! process** via the kernel peer credential (`UnixStream::peer_cred()`), not the request body,
//! before the JSON body extractor for that request ever runs (§决定2 ordering), and every field
//! the handler actually reasons over comes from the claimed `ops.private_inference_rpc_calls`
//! row — never from the wire body — so a hostile/buggy caller cannot smuggle a different
//! `reasoning_domain_id`/`binding_id`/`purpose` than what it registered.
//!
//! §11.8 hard boundary (enforced by construction, not by this file's own discipline): this
//! handler dispatches on the CLAIMED row's purpose to exactly one [`PrivateReasoningPort`]
//! implementation — [`ConsolidationReasoner`] for `Consolidate` (ADR-0015: the run id comes
//! from the claimed registration row, never the wire body), [`ContributionReasoner`] for
//! everything else — and never builds a `PgPool`/repository capability from the RPC body, nor
//! accepts a `memory_id` to mutate.

use std::sync::Arc;

use axum::extract::{ConnectInfo, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router, middleware};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use humaux_adapters::consolidation_reasoner::{ConsolidationCallBinding, ConsolidationReasoner};
use humaux_adapters::contribution_reasoner::{ContributionReasoner, ContributionReasonerConfig};
use humaux_adapters::disclosure::DeletionCapability;
use humaux_adapters::postgres::PrivateWorkerDbPool;
use humaux_adapters::private_inference_rpc::{
    ClaimOutcome, ClaimedRegistration, FinishOutcome, PrivateWorkerInferenceCalls, StoredOutcome,
};
use humaux_adapters::reasoning_route_admission::ProviderFor;
use humaux_application::consolidate::{
    PrivateReasoningDomainId, PrivateReasoningPort, PrivateReasoningPurpose,
    ReasoningRouteBindingId, ReasoningRouteBindingVersion, SealedPrivateReasoningRequest,
};

const SCHEMA_VERSION: u16 = 1;

/// Wire request — `call_id` + `tenant_hint` only. The sealed request's actual fields
/// (`reasoning_domain_id`/`binding_id`/`binding_version`/`purpose`/`input_manifest_hash`) are
/// never re-accepted from the wire: `humaux-consolidation-worker` already committed the
/// canonical copy to `ops.private_inference_rpc_calls` at register time (§11.8 "not trusting
/// anything the RPC body itself asserted about identity" — same reasoning
/// `retrieval_embedding_rpc`'s handler gives for re-deriving its `AuthorizationScope` from the
/// claimed row rather than the wire body).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrivateInferenceRpcRequest {
    schema_version: u16,
    call_id: Uuid,
    tenant_hint: Uuid,
}

#[derive(Debug, Serialize)]
struct PrivateInferenceRpcEnvelope {
    schema_version: u16,
    call_id: Uuid,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_bytes_hex: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_sha256_hex: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_trace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model_call_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure_message: Option<String>,
}

/// Per-connection identity captured once at accept time, same shape
/// `bins/retrieval-worker/src/rpc.rs::PeerIdentity` uses for its own gateway-uid check.
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

/// What every RPC call shares for the process lifetime.
pub struct RpcState {
    pub expected_consolidation_uid: u32,
    pub calls: PrivateWorkerDbPool,
    pub config: ContributionReasonerConfig,
    /// ADR-0060 D-E: maps the registered binding's admitted route to its provider instance; the
    /// wire never names a route.
    pub providers: Box<ProviderFor>,
    /// Ruling E3 (`HUMAUX_PRIVATE_WORKER_HEALTH_RENEW_SECS`): validity of worker-observed health.
    pub health_renew_seconds: i64,
}

/// §决定2: rejects before the handler's `Json<PrivateInferenceRpcRequest>` extractor ever
/// parses the body.
async fn require_consolidation_uid(
    State(state): State<Arc<RpcState>>,
    ConnectInfo(peer): ConnectInfo<PeerIdentity>,
    request: axum::extract::Request,
    next: middleware::Next,
) -> Response {
    if !peer.ok || peer.uid != state.expected_consolidation_uid {
        return StatusCode::FORBIDDEN.into_response();
    }
    next.run(request).await
}

pub fn router(state: Arc<RpcState>) -> Router {
    Router::new()
        .route("/internal/v1/private/infer", post(infer_handler))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_consolidation_uid,
        ))
        .with_state(state)
}

/// Binds the RPC listener, replacing a stale socket file (same shape
/// `bins/retrieval-worker/src/main.rs::rpc_mode` uses). Split from [`serve`] so a caller binds
/// synchronously before spawning the accept loop — the first dial must never race it.
pub fn bind_socket(socket_path: &std::path::Path) -> std::io::Result<tokio::net::UnixListener> {
    let _ = std::fs::remove_file(socket_path);
    // dep: UDS(serve) — unix-socket RPC
    tokio::net::UnixListener::bind(socket_path)
}

/// The one accept loop for this RPC: the binary's `--serve-rpc` mode and
/// `bins/consolidation-worker/tests/consolidation_hop_e2e.rs`'s in-process harness both run
/// exactly this, so what T1–T5 prove over the socket is what production serves.
pub async fn serve(
    listener: tokio::net::UnixListener,
    state: Arc<RpcState>,
) -> std::io::Result<()> {
    axum::serve(
        listener,
        router(state).into_make_service_with_connect_info::<PeerIdentity>(),
    )
    .await
}

enum WorkerRpcError {
    NotFound,
    Expired,
}

async fn infer_handler(
    State(state): State<Arc<RpcState>>,
    Json(wire): Json<PrivateInferenceRpcRequest>,
) -> Response {
    if wire.schema_version != SCHEMA_VERSION {
        return (
            StatusCode::BAD_REQUEST,
            "unsupported private inference RPC schema",
        )
            .into_response();
    }
    match execute(&state, wire).await {
        Ok(envelope) => (StatusCode::OK, Json(envelope)).into_response(),
        Err(WorkerRpcError::NotFound) => StatusCode::NOT_FOUND.into_response(),
        Err(WorkerRpcError::Expired) => StatusCode::GONE.into_response(),
    }
}

fn envelope_from_stored(call_id: Uuid, stored: StoredOutcome) -> PrivateInferenceRpcEnvelope {
    PrivateInferenceRpcEnvelope {
        schema_version: SCHEMA_VERSION,
        call_id,
        outcome: if stored.outcome == "COMPLETED" && stored.response_output_bytes.is_some() {
            "COMPLETED"
        } else {
            "FAILED"
        },
        output_bytes_hex: stored.response_output_bytes.as_deref().map(hex::encode),
        output_sha256_hex: stored.response_output_sha256.as_deref().map(hex::encode),
        provider_trace: stored.response_provider_trace,
        model_call_id: stored.response_model_call_id,
        failure_message: stored.response_failure_message,
    }
}

fn failed_envelope(call_id: Uuid, message: String) -> PrivateInferenceRpcEnvelope {
    PrivateInferenceRpcEnvelope {
        schema_version: SCHEMA_VERSION,
        call_id,
        outcome: "FAILED",
        output_bytes_hex: None,
        output_sha256_hex: None,
        provider_trace: None,
        model_call_id: None,
        failure_message: Some(message),
    }
}

/// §11.8's closed purpose set, DB string -> Rust, the read-side inverse of
/// `private_inference_rpc::purpose_db_str`.
fn purpose_from_db_str(s: &str) -> Option<PrivateReasoningPurpose> {
    Some(match s {
        "Distill" => PrivateReasoningPurpose::Distill,
        "Consolidate" => PrivateReasoningPurpose::Consolidate,
        "Vision" => PrivateReasoningPurpose::Vision,
        "ContributionDeidentify" => PrivateReasoningPurpose::ContributionDeidentify,
        _ => return None,
    })
}

async fn execute(
    state: &Arc<RpcState>,
    wire: PrivateInferenceRpcRequest,
) -> Result<PrivateInferenceRpcEnvelope, WorkerRpcError> {
    let repo = PrivateWorkerInferenceCalls::new(&state.calls);
    let claim = repo
        .load_and_claim(wire.call_id, wire.tenant_hint, "role_private_worker")
        .await
        .map_err(|_| WorkerRpcError::NotFound)?;

    let registration = match claim {
        ClaimOutcome::NotFound => return Err(WorkerRpcError::NotFound),
        ClaimOutcome::Expired => return Err(WorkerRpcError::Expired),
        ClaimOutcome::AlreadyClaimed => {
            return Ok(failed_envelope(wire.call_id, "WORKER_BUSY".to_owned()));
        }
        ClaimOutcome::Replay(stored) => return Ok(envelope_from_stored(wire.call_id, stored)),
        ClaimOutcome::Claimed(registration) => registration,
    };

    let (envelope, route) = infer(state, wire.call_id, &registration).await;
    let finish_outcome = match &envelope {
        e if e.outcome == "COMPLETED" => FinishOutcome::Completed {
            output_bytes: e
                .output_bytes_hex
                .as_deref()
                .and_then(|h| hex::decode(h).ok())
                .unwrap_or_default(),
            output_sha256: e
                .output_sha256_hex
                .as_deref()
                .and_then(|h| hex::decode(h).ok())
                .and_then(|v| <[u8; 32]>::try_from(v).ok())
                .unwrap_or([0u8; 32]),
            provider_trace: e.provider_trace.clone().unwrap_or_default(),
            model_call_id: e.model_call_id.unwrap_or_else(Uuid::nil),
        },
        e => FinishOutcome::Failed {
            message: e
                .failure_message
                .clone()
                .unwrap_or_else(|| "UNKNOWN".to_owned()),
        },
    };
    // Same documented tradeoff `retrieval_embedding_rpc`'s handler accepts for its own
    // `finish()` failure: a failed persist here leaves the row stuck `CLAIMED` until TTL
    // expiry, logged rather than silent.
    if let Err(error) = repo
        .finish(wire.call_id, registration.tenant_id, finish_outcome)
        .await
    {
        eprintln!(
            "private_inference_rpc: finish() failed to persist call outcome for {}: {error} \
             (row stuck CLAIMED until TTL expiry)",
            wire.call_id
        );
    }
    // ADR-0060 D-M: one finish line per call naming its admitted route (static fields only).
    eprintln!(
        "humaux-private-worker: rpc call={} tenant={} purpose={} {} outcome={}",
        wire.call_id,
        registration.tenant_id,
        registration.purpose,
        route.as_deref().unwrap_or("route=-"),
        envelope.outcome
    );
    Ok(envelope)
}

/// §11.8: rebuilds the sealed request from the claimed registration row (never the wire body —
/// module doc) and dispatches on its purpose to the one production [`PrivateReasoningPort`]
/// implementation for that purpose (module doc). Binding decision #2's exact call site. Also
/// returns the admitted route's fields when the reasoner got that far (ADR-0060 D-M).
async fn infer(
    state: &Arc<RpcState>,
    call_id: Uuid,
    registration: &ClaimedRegistration,
) -> (PrivateInferenceRpcEnvelope, Option<String>) {
    let Some(purpose) = purpose_from_db_str(&registration.purpose) else {
        return (
            failed_envelope(call_id, "INVALID_REGISTRATION".to_owned()),
            None,
        );
    };
    let Ok(input_manifest_hash) = <[u8; 32]>::try_from(registration.input_manifest_hash.as_slice())
    else {
        return (
            failed_envelope(call_id, "INVALID_REGISTRATION".to_owned()),
            None,
        );
    };
    let sealed = SealedPrivateReasoningRequest {
        reasoning_domain_id: PrivateReasoningDomainId(registration.reasoning_domain_id),
        binding_id: ReasoningRouteBindingId(registration.binding_id),
        binding_version: ReasoningRouteBindingVersion(registration.binding_version),
        input_manifest_hash: humaux_application::consolidate::ContentSha256(input_manifest_hash),
        purpose,
        contribution_attempt: None,
    };
    let (result, route) = match purpose {
        PrivateReasoningPurpose::Consolidate => {
            let Some(consolidation_run_id) = registration.consolidation_run_id else {
                return (
                    failed_envelope(call_id, "INVALID_REGISTRATION".to_owned()),
                    None,
                );
            };
            let Ok(reasoner) = ConsolidationReasoner::new(
                &state.calls,
                state.providers.as_ref(),
                clone_config(&state.config),
                ConsolidationCallBinding {
                    tenant_id: registration.tenant_id,
                    consolidation_run_id,
                },
            ) else {
                return (failed_envelope(call_id, "INVALID_CONFIG".to_owned()), None);
            };
            let result = reasoner.infer(sealed).await;
            if let Some((model_call_id, credential_rejected)) = reasoner.observed_call() {
                crate::observe_route_health(
                    &state.calls,
                    registration.tenant_id,
                    model_call_id,
                    credential_rejected,
                    state.health_renew_seconds,
                )
                .await;
            }
            (result, reasoner.route_fields().map(str::to_owned))
        }
        PrivateReasoningPurpose::Distill
        | PrivateReasoningPurpose::Vision
        | PrivateReasoningPurpose::ContributionDeidentify => {
            let Ok(reasoner) = ContributionReasoner::new_for_execution(
                &state.calls,
                state.providers.as_ref(),
                clone_config(&state.config),
            ) else {
                return (failed_envelope(call_id, "INVALID_CONFIG".to_owned()), None);
            };
            (reasoner.infer(sealed).await, None)
        }
    };
    let envelope = match result {
        Ok(result) => PrivateInferenceRpcEnvelope {
            schema_version: SCHEMA_VERSION,
            call_id,
            outcome: "COMPLETED",
            output_bytes_hex: Some(hex::encode(&result.output_bytes)),
            output_sha256_hex: Some(hex::encode(result.output_sha256.0)),
            provider_trace: Some(result.provider_trace.0),
            model_call_id: Some(result.model_call_id),
            failure_message: None,
        },
        Err(error) => failed_envelope(call_id, error.to_string()),
    };
    (envelope, route)
}

/// `ContributionReasonerConfig` has no `Clone` (`Duration`/`String`/enum fields only, but no
/// derive) — this handler's config is fixed for the process lifetime, so a field-by-field copy
/// here is simpler than adding a derive this crate's own file scope does not include. `pub`
/// because the binary's `--distill-*` modes (ADR-0016) hand one copy per pass to the same type.
pub fn clone_config(config: &ContributionReasonerConfig) -> ContributionReasonerConfig {
    ContributionReasonerConfig {
        permit_ttl: config.permit_ttl,
        deletion_capability: match config.deletion_capability {
            DeletionCapability::Supported => DeletionCapability::Supported,
            DeletionCapability::Unsupported => DeletionCapability::Unsupported,
            DeletionCapability::Unknown => DeletionCapability::Unknown,
        },
        system_prompt: config.system_prompt.clone(),
        json_schema: config.json_schema.clone(),
        max_output_tokens: config.max_output_tokens,
    }
}
