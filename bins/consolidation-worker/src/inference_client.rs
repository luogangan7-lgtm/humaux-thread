//! `consolidation-worker::inference_client` — §11.8 `UdsInferenceClient` — `humaux-consolidation-worker`'s
//!   [`PrivateReasoningPort`] implementation: registers a call row on [`ConsolidationDbPool`]
//!   (`role_consolidation_worker`), then RPCs `humaux-private-worker` over the ADR-0012 Unix domain socket.
//! Depends-on: crates=[async-trait, hex, humaux-adapters, humaux-application, serde, serde_json, sha2, tokio, uuid];
//!   services=[UDS(private-worker)]; env=[]; modules=[adapters::postgres, adapters::private_inference_rpc,
//!   application::consolidate]
//! Called-by: [consolidation-worker::main, tests]
//! Invariants: [one fresh UDS connection per call to the private worker's inference socket; a dial/IO/parse failure
//!   is TRANSPORT/INVALID_RESPONSE mapped to PrivateReasoningError, never a local fallback inference]
//! Spec: none
//!
//! Mirrors
//! `bins/gateway/src/retrieval_embedding_client.rs::GatewayRetrievalEmbeddingClient` verbatim
//! in transport shape (§决定1: hand-rolled HTTP/1.1 framing over a fresh UDS connection per
//! call, no generic RPC framework) — see that module's doc for the full reasoning this one
//! does not repeat.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use uuid::Uuid;

use humaux_adapters::postgres::ConsolidationDbPool;
use humaux_adapters::private_inference_rpc::{ConsolidationRegistrations, RegisterCall};
use humaux_application::consolidate::{
    PrivateReasoningError, PrivateReasoningPort, PrivateReasoningResult, ProviderTraceRef,
    SealedPrivateReasoningRequest,
};

const SCHEMA_VERSION: u16 = 1;
/// Same cap `retrieval_embedding_client`'s `MAX_RESPONSE_BYTES` documents: private-worker's
/// `Json` response body is always a fully materialized `Content-Length` body of modest size
/// (hex-encoded rollup content plus a few identifiers) — anything past this is a misbehaving
/// peer or a protocol bug, never a legitimate reply.
const MAX_RESPONSE_BYTES: usize = 8 << 20;

#[derive(Debug, Serialize)]
struct PrivateInferenceRpcRequest {
    schema_version: u16,
    call_id: Uuid,
    tenant_hint: Uuid,
}

#[derive(Debug, Deserialize)]
struct PrivateInferenceRpcEnvelope {
    #[allow(dead_code)]
    schema_version: u16,
    call_id: Uuid,
    outcome: String,
    output_bytes_hex: Option<String>,
    output_sha256_hex: Option<String>,
    provider_trace: Option<String>,
    model_call_id: Option<Uuid>,
    failure_message: Option<String>,
}

pub struct UdsInferenceClient<'a> {
    pool: &'a ConsolidationDbPool,
    socket_path: String,
    tenant_id: Uuid,
    call_ttl: Duration,
    dial_timeout: Duration,
    /// ADR-0015: bound at construction (`run_once_bound`'s `bind_port` runs after
    /// `create_run`) and registered next to the sealed identifiers so the private worker can
    /// locate this run's inputs — the sealed request itself (§11.8) carries no run id.
    consolidation_run_id: Uuid,
}

impl<'a> UdsInferenceClient<'a> {
    pub fn new(
        pool: &'a ConsolidationDbPool,
        socket_path: impl Into<String>,
        tenant_id: Uuid,
        call_ttl: Duration,
        dial_timeout: Duration,
        consolidation_run_id: Uuid,
    ) -> Self {
        Self {
            pool,
            socket_path: socket_path.into(),
            tenant_id,
            call_ttl,
            dial_timeout,
            consolidation_run_id,
        }
    }

    async fn dial(
        &self,
        wire: &PrivateInferenceRpcRequest,
    ) -> Result<PrivateInferenceRpcEnvelope, String> {
        tokio::time::timeout(self.dial_timeout, self.dial_inner(wire))
            .await
            .unwrap_or_else(|_| Err("TRANSPORT_TIMEOUT".to_owned()))
    }

    async fn dial_inner(
        &self,
        wire: &PrivateInferenceRpcRequest,
    ) -> Result<PrivateInferenceRpcEnvelope, String> {
        // dep: UDS(private-worker) — dials the private worker's inference RPC socket (ADR-0012)
        let mut stream = UnixStream::connect(&self.socket_path)
            .await
            .map_err(|_| "TRANSPORT".to_owned())?;
        let body = serde_json::to_vec(wire).map_err(|_| "INVALID_RESPONSE".to_owned())?;
        let head = format!(
            "POST /internal/v1/private/infer HTTP/1.1\r\n\
             Host: localhost\r\n\
             Content-Type: application/json\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\r\n",
            body.len()
        );
        stream
            .write_all(head.as_bytes())
            .await
            .map_err(|_| "TRANSPORT".to_owned())?;
        stream
            .write_all(&body)
            .await
            .map_err(|_| "TRANSPORT".to_owned())?;
        let mut raw = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            let n = stream
                .read(&mut chunk)
                .await
                .map_err(|_| "TRANSPORT".to_owned())?;
            if n == 0 {
                break;
            }
            if raw.len() + n > MAX_RESPONSE_BYTES {
                return Err("TRANSPORT_OVERSIZED_RESPONSE".to_owned());
            }
            raw.extend_from_slice(&chunk[..n]);
        }
        parse_http_response(&raw)
    }
}

fn parse_http_response(raw: &[u8]) -> Result<PrivateInferenceRpcEnvelope, String> {
    let header_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| "TRANSPORT".to_owned())?;
    let head = std::str::from_utf8(&raw[..header_end]).map_err(|_| "TRANSPORT".to_owned())?;
    let status_line = head.lines().next().ok_or_else(|| "TRANSPORT".to_owned())?;
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| "TRANSPORT".to_owned())?;
    if status != 200 {
        return Err(format!("TRANSPORT_HTTP_{status}"));
    }
    let body = &raw[header_end + 4..];
    serde_json::from_slice(body).map_err(|_| "INVALID_RESPONSE".to_owned())
}

#[async_trait::async_trait]
impl PrivateReasoningPort for UdsInferenceClient<'_> {
    async fn infer(
        &self,
        req: SealedPrivateReasoningRequest,
    ) -> Result<PrivateReasoningResult, PrivateReasoningError> {
        let call_id = Uuid::now_v7();
        let purpose = humaux_adapters::private_inference_rpc::purpose_db_str(req.purpose);
        ConsolidationRegistrations::new(self.pool)
            .register(&RegisterCall {
                call_id,
                tenant_id: self.tenant_id,
                reasoning_domain_id: req.reasoning_domain_id.0,
                binding_id: req.binding_id.0,
                binding_version: req.binding_version.0,
                purpose,
                input_manifest_hash: req.input_manifest_hash.0,
                consolidation_run_id: Some(self.consolidation_run_id),
                ttl: self.call_ttl,
            })
            .await
            .map_err(|error| PrivateReasoningError::new(error.to_string()))?;

        let wire = PrivateInferenceRpcRequest {
            schema_version: SCHEMA_VERSION,
            call_id,
            tenant_hint: self.tenant_id,
        };
        let envelope = match self.dial(&wire).await {
            Ok(envelope) if envelope.call_id == call_id => envelope,
            Ok(_) => {
                return Err(PrivateReasoningError::new("INVALID_RESPONSE".to_owned()));
            }
            Err(reason) => return Err(PrivateReasoningError::new(reason)),
        };
        if envelope.outcome != "COMPLETED" {
            return Err(PrivateReasoningError::new(
                envelope
                    .failure_message
                    .unwrap_or_else(|| "UNKNOWN".to_owned()),
            ));
        }
        let (Some(output_hex), Some(sha_hex), Some(trace), Some(model_call_id)) = (
            envelope.output_bytes_hex,
            envelope.output_sha256_hex,
            envelope.provider_trace,
            envelope.model_call_id,
        ) else {
            return Err(PrivateReasoningError::new("INVALID_RESPONSE".to_owned()));
        };
        let output_bytes =
            hex::decode(&output_hex).map_err(|_| PrivateReasoningError::new("INVALID_RESPONSE"))?;
        let sha_bytes =
            hex::decode(&sha_hex).map_err(|_| PrivateReasoningError::new("INVALID_RESPONSE"))?;
        let output_sha256: [u8; 32] = sha_bytes
            .try_into()
            .map_err(|_| PrivateReasoningError::new("INVALID_RESPONSE"))?;
        // §11.8: proves the wire body's `output_bytes` is the actual referent of
        // `output_sha256`, not a mismatched pair a compromised/buggy peer sent — the same
        // integrity discipline `retrieval_embedding_rpc`'s `query_sha256` check applies at the
        // registration boundary, applied here at the response boundary instead.
        let computed: [u8; 32] = Sha256::digest(&output_bytes).into();
        if computed != output_sha256 {
            return Err(PrivateReasoningError::new(
                "RESPONSE_DIGEST_MISMATCH".to_owned(),
            ));
        }
        Ok(PrivateReasoningResult {
            output_bytes,
            output_sha256: humaux_application::consolidate::ContentSha256(output_sha256),
            provider_trace: ProviderTraceRef(trace),
            model_call_id,
            binding_id: req.binding_id,
            binding_version: req.binding_version,
        })
    }
}
