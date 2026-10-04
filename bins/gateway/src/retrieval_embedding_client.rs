//! `gateway::retrieval_embedding_client` — ADR-0012 Gateway-side `RetrievalEmbeddingPort` — registers a call row on
//!   [`RuntimeDbPool`] (`role_gateway`), then RPCs `humaux-retrieval-worker` over the ADR-0012 Unix domain socket.
//! Depends-on: crates=[async-trait, humaux-adapters, humaux-application, humaux-domain, humaux-infra-cell, serde,
//!   serde_json, sha2, tokio, uuid]; services=[UDS(retrieval-worker)]; env=[]; modules=[adapters::postgres,
//!   adapters::retrieval_embedding_rpc, application::retrieval_embedding_port, domain::error, infra-cell::permit,
//!   infra-cell::resource]
//! Called-by: [gateway::bootstrap, gateway::status, tests]
//! Invariants: [the RPC still flows through the one closed IntraCellResource::RETRIEVAL_EMBEDDING_RPC registry/permit even though the dial bypasses IntraCellHttpTransport (a UDS path plus kernel peer-credential auth)]
//! Spec: Baseline §2; §83.4; ADR-0012
//!
//! §决定3: authorization for the RPC still flows through the one closed
//! `IntraCellResource::RETRIEVAL_EMBEDDING_RPC` registry/permit — [`GatewayRetrievalEmbeddingClient`]
//! mints a [`CellAccessPermit`] every call, even though the actual dial (§决定1/2) bypasses
//! `IntraCellHttpTransport` entirely (that transport is TCP/CIDR-shaped; this resource's
//! transport is a UDS path plus kernel peer-credential auth, which lives worker-side).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use uuid::Uuid;

use humaux_adapters::postgres::RuntimeDbPool;
use humaux_adapters::retrieval_embedding_rpc::{
    GatewayRetrievalEmbeddingRegistrations, RegisterCall,
};
use humaux_application::retrieval_embedding_port::{
    RetrievalEmbeddingInput, RetrievalEmbeddingOutcome, RetrievalEmbeddingPort,
};
use humaux_domain::error::ErrorCode;
use humaux_infra_cell::{
    CellAccessPermit, IntraCellResource, IntraCellResourceRegistry, authorize_cell_access,
};

const SCHEMA_VERSION: u16 = 1;
const QUERY_EMBEDDING_PATH: &str = "/internal/v1/retrieval/query-embedding";
/// ADR-0061 D-F: the worker's readiness route, behind the same peer-uid layer.
const READYZ_PATH: &str = "/internal/v1/retrieval/readyz";

/// Worker response cap — the envelope is a small JSON object plus one dense vector (a few KB at
/// realistic dimensions); anything past this is either a misbehaving/hostile peer or a protocol
/// bug, never a legitimate reply. Bounds `dial`'s read regardless of the caller's deadline, so a
/// peer that dribbles bytes forever within its timeout budget still cannot grow the gateway's
/// memory unboundedly.
const MAX_RESPONSE_BYTES: usize = 1 << 20;

#[derive(Debug, Serialize)]
struct QueryEmbeddingRpcRequest {
    schema_version: u16,
    call_id: Uuid,
    tenant_hint: Uuid,
    query: String,
}

#[derive(Debug, Deserialize)]
struct QueryEmbeddingRpcEnvelope {
    #[allow(dead_code)]
    schema_version: u16,
    call_id: Uuid,
    outcome: String,
    vector: Option<Vec<f32>>,
    provider_id: Option<String>,
    model_id: Option<String>,
    model_revision: Option<String>,
    dimension: Option<u32>,
    failure_code: Option<String>,
}

pub struct GatewayRetrievalEmbeddingClient {
    pool: Arc<RuntimeDbPool>,
    socket_path: String,
    cell_registry: IntraCellResourceRegistry,
    permit_ttl: Duration,
}

impl GatewayRetrievalEmbeddingClient {
    pub fn new(
        pool: Arc<RuntimeDbPool>,
        socket_path: impl Into<String>,
        cell_registry: IntraCellResourceRegistry,
        permit_ttl: Duration,
    ) -> Self {
        Self {
            pool,
            socket_path: socket_path.into(),
            cell_registry,
            permit_ttl,
        }
    }

    fn permit(&self) -> Result<CellAccessPermit, ErrorCode> {
        authorize_cell_access(
            &self.cell_registry,
            IntraCellResource::RETRIEVAL_EMBEDDING_RPC,
            self.permit_ttl,
        )
        .map_err(|_| ErrorCode::Forbidden)
    }
}

#[async_trait]
impl RetrievalEmbeddingPort for GatewayRetrievalEmbeddingClient {
    async fn embed_query(
        &self,
        input: RetrievalEmbeddingInput<'_>,
    ) -> Result<RetrievalEmbeddingOutcome, ErrorCode> {
        // §决定3: minted even though this resource's actual transport is a raw UDS dial below,
        // not `IntraCellHttpTransport` — see module doc.
        let _permit = self.permit()?;

        let user_id = input
            .authorization
            .user_id()
            .ok_or(ErrorCode::Unauthorized)?;
        if !input
            .authorization
            .allowed_workspace_ids()
            .contains(&input.workspace_id)
        {
            return Err(ErrorCode::Forbidden);
        }

        // Candidate `call_id` for a *fresh* registration only — `register` below reuses the
        // existing row's `call_id` instead when this caller has already registered the same
        // `(tenant_id, logical_call_id, attempt_no)` (a retry after an ambiguous outcome must
        // never mint a second real registration/provider call, ADR-0012 §2).
        let candidate_call_id = Uuid::now_v7();
        let tenant_id = input.authorization.tenant_id();
        let query_sha256: [u8; 32] = Sha256::digest(input.query.as_bytes()).into();
        let ttl = Duration::from_millis(
            (input.deadline_unix_ms
                - i64::try_from(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_err(|_| ErrorCode::Internal)?
                        .as_millis(),
                )
                .map_err(|_| ErrorCode::Internal)?)
            .max(0) as u64,
        );
        if ttl.is_zero() {
            return Err(ErrorCode::InvalidInput);
        }

        let call_id = GatewayRetrievalEmbeddingRegistrations::new(&self.pool)
            .register(&RegisterCall {
                call_id: candidate_call_id,
                tenant_id: tenant_id.0,
                principal_id: input.authorization.principal().0,
                user_id: user_id.0,
                workspace_id: input.workspace_id.0,
                request_id: input.request_id,
                logical_call_id: input.logical_call_id,
                attempt_no: input.attempt_no,
                profile_fingerprint: input.profile_fingerprint.to_owned(),
                query_sha256,
                ttl,
            })
            .await
            .map_err(|_| ErrorCode::DependencyUnavailable)?;

        let wire = QueryEmbeddingRpcRequest {
            schema_version: SCHEMA_VERSION,
            call_id,
            tenant_hint: tenant_id.0,
            query: input.query.to_owned(),
        };
        // ADR-0012 §2 "All worker failures ... degrade the dense lane only": a dial
        // timeout/transport error or an unparseable response is exactly that kind of failure,
        // not a caller-fault `Err` — fold it into `Unavailable` rather than returning early.
        let envelope = match self.dial(&wire, ttl).await {
            Ok(envelope) if envelope.call_id == call_id => envelope,
            Ok(_) => {
                return Ok(RetrievalEmbeddingOutcome::Unavailable {
                    reason: "INVALID_RESPONSE".to_owned(),
                });
            }
            Err(reason) => return Ok(RetrievalEmbeddingOutcome::Unavailable { reason }),
        };
        Ok(match envelope.outcome.as_str() {
            "EMBEDDED"
                if envelope.vector.is_some()
                    && envelope.provider_id.is_some()
                    && envelope.model_id.is_some()
                    && envelope.model_revision.is_some()
                    && envelope.dimension.is_some() =>
            {
                RetrievalEmbeddingOutcome::Embedded {
                    vector: envelope.vector.unwrap(),
                    provider_id: envelope.provider_id.unwrap(),
                    model_id: envelope.model_id.unwrap(),
                    model_revision: envelope.model_revision.unwrap(),
                    dimension: envelope.dimension.unwrap(),
                }
            }
            "SKIPPED" => RetrievalEmbeddingOutcome::Skipped,
            _ => RetrievalEmbeddingOutcome::Unavailable {
                reason: envelope
                    .failure_code
                    .unwrap_or_else(|| "INVALID_RESPONSE".to_owned()),
            },
        })
    }
}

impl GatewayRetrievalEmbeddingClient {
    /// One request/response over a fresh Unix domain socket connection — ADR-0012 §决定1: "No
    /// generic RPC framework." §83.4/G80-3 (`crates/infra-network`'s own module doc) is the
    /// *general* choke point for a real network dependency (`reqwest`/`hyper` as a direct
    /// crate dependency is confined to `infra-network` workspace-wide); a same-host UDS dial
    /// is not that boundary's concern, but staying off `hyper`/`reqwest` here keeps this
    /// binary's dependency graph inside that same closed set rather than opening a second
    /// legal way to depend on an HTTP client crate. The wire shape is fixed and both ends are
    /// this workspace's own code, so hand-rolled HTTP/1.1 framing (one request, `Connection:
    /// close`, read to EOF) is the whole client — not a general-purpose HTTP stack.
    ///
    /// ponytail: connection-per-call, no keep-alive/pool, no chunked-response support (the
    /// worker's `Json` response body is always a fully materialized `Content-Length` body) —
    /// add a keep-alive pool if per-call latency ever needs to drop below one UDS handshake.
    /// `deadline` bounds the whole dial (connect + write + read) — ADR-0012 §2 requires worker
    /// failures to degrade the dense lane, not hang the gateway request; a hung/malicious peer
    /// now times out instead of blocking forever, and the read is capped at
    /// [`MAX_RESPONSE_BYTES`] regardless of `deadline` so a peer that drips bytes slowly within
    /// budget still cannot grow memory unboundedly. Returns the short machine-stable reason tag
    /// [`RetrievalEmbeddingOutcome::Unavailable`] wants on any failure — there is no caller-fault
    /// distinction left to make here (see that variant's doc).
    async fn dial(
        &self,
        wire: &QueryEmbeddingRpcRequest,
        deadline: Duration,
    ) -> Result<QueryEmbeddingRpcEnvelope, String> {
        tokio::time::timeout(deadline, self.dial_inner(wire))
            .await
            .unwrap_or_else(|_| Err("TRANSPORT_TIMEOUT".to_owned()))
    }

    async fn dial_inner(
        &self,
        wire: &QueryEmbeddingRpcRequest,
    ) -> Result<QueryEmbeddingRpcEnvelope, String> {
        let body = serde_json::to_vec(wire).map_err(|_| "INVALID_RESPONSE".to_owned())?;
        let raw = self.exchange("POST", QUERY_EMBEDDING_PATH, &body).await?;
        parse_http_response(&raw)
    }

    /// One `Connection: close` request over a fresh connection to the worker's socket; returns the
    /// raw response, capped at [`MAX_RESPONSE_BYTES`].
    async fn exchange(&self, method: &str, path: &str, body: &[u8]) -> Result<Vec<u8>, String> {
        // dep: UDS(retrieval-worker) — dials humaux-retrieval-worker's ADR-0012 embedding RPC socket
        let mut stream = UnixStream::connect(&self.socket_path)
            .await
            .map_err(|_| "TRANSPORT".to_owned())?;
        let head = format!(
            "{method} {path} HTTP/1.1\r\n\
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
            .write_all(body)
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
        Ok(raw)
    }

    /// ADR-0061 D-F `retrieval_rpc` readiness: one `GET /internal/v1/retrieval/readyz` round trip
    /// over the same socket, permit and worker-side peer-uid layer a recall uses — the worker
    /// answers it with a fresh `role_retrieval_worker` connection, so a bare `connect()` that
    /// succeeds while the worker cannot reach PostgreSQL is not a pass. `Err` carries the transport
    /// reason or the worker's non-200 status and body; never a provider call (ADR-0061 D-F).
    ///
    /// # Errors
    /// The permit refusal, a transport failure, the deadline, or a non-200 answer.
    pub async fn ping(&self, deadline: Duration) -> Result<(), String> {
        let _permit = self
            .permit()
            .map_err(|_| "IntraCellResource::RETRIEVAL_EMBEDDING_RPC permit refused".to_owned())?;
        let raw = tokio::time::timeout(deadline, self.exchange("GET", READYZ_PATH, &[]))
            .await
            .unwrap_or_else(|_| Err("TRANSPORT_TIMEOUT".to_owned()))?;
        match split_http_response(&raw)? {
            (200, _) => Ok(()),
            (status, body) => Err(format!(
                "TRANSPORT_HTTP_{status}: {}",
                String::from_utf8_lossy(body).trim()
            )),
        }
    }
}

/// Splits one fixed-shape `HTTP/1.1 <status> ...\r\n...\r\n\r\n<body>` response into its status
/// code and body.
fn split_http_response(raw: &[u8]) -> Result<(u16, &[u8]), String> {
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
    Ok((status, &raw[header_end + 4..]))
}

/// Parses one fixed-shape `HTTP/1.1 <status> ...\r\n...\r\n\r\n<json body>` response — the
/// exact shape [`GatewayRetrievalEmbeddingClient::dial`] sends `Connection: close` to obtain.
/// Not a general HTTP parser: no chunked transfer-encoding, no header folding, no redirects.
fn parse_http_response(raw: &[u8]) -> Result<QueryEmbeddingRpcEnvelope, String> {
    let (status, body) = split_http_response(raw)?;
    if status != 200 {
        return Err(format!("TRANSPORT_HTTP_{status}"));
    }
    serde_json::from_slice(body).map_err(|_| "INVALID_RESPONSE".to_owned())
}
