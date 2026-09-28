//! `application::retrieval_embedding_port` — ADR-0012's sole cross-process boundary type the Gateway depends on for
//!   query embedding.
//! Depends-on: crates=[async-trait, humaux-domain, uuid]; services=[]; env=[]; modules=[domain::error,
//!   domain::identity, domain::ids]
//! Called-by: [gateway::bootstrap, gateway::recall, gateway::retrieval_embedding_client, tests]
//! Invariants: []
//! Spec: ADR-0012; §4.2; §2
//!
//! §4.2 keeps `role_gateway`/`bins/gateway` free of the PLATFORM_RETRIEVAL provider credential
//! and the `role_retrieval_worker` DB pool (both live only behind
//! `humaux_retrieval_provider::adapters::DashscopeEmbeddingProvider`, worker-side). This trait
//! is the narrow seam `bins/gateway/src/recall.rs` calls instead of
//! `humaux_retrieval_provider::contract::EmbeddingProvider` directly — a real implementation
//! (`bins/gateway/src/retrieval_embedding_client.rs`) registers a call row via `RuntimeDbPool`
//! and RPCs `humaux-retrieval-worker` over the ADR-0012 Unix domain socket; the worker alone
//! calls the real `EmbeddingProvider`.
//!
//! Deliberately carries only what ADR-0012 §2 says crosses the wire in each direction: raw
//! query text and routing/identity metadata out, a vector plus non-secret model-stamp fields
//! back. Never: `AuthorizationScope` itself, a sealed query, a provider credential, or an
//! `EgressPermit` — those stay inside the worker process that already owns them.

use async_trait::async_trait;
use humaux_domain::error::ErrorCode;
use humaux_domain::identity::AuthorizationScope;
use humaux_domain::ids::WorkspaceId;
use uuid::Uuid;

/// One [`RetrievalEmbeddingPort::embed_query`] call's trusted inputs. `authorization` supplies
/// tenant/principal/user/workspace identity for the registration row the port's real
/// implementation reserves before ever dialing the worker (ADR-0012 §决定2's "registration =
/// idempotency anchor"); `query` is the raw text that crosses the wire — sealing/scanning
/// happens worker-side, not here, so this type carries no `SealedRetrievalQuery`.
#[derive(Debug, Clone, Copy)]
pub struct RetrievalEmbeddingInput<'a> {
    pub authorization: &'a AuthorizationScope,
    pub workspace_id: WorkspaceId,
    pub request_id: Uuid,
    pub logical_call_id: Uuid,
    pub attempt_no: i32,
    /// §55.1 `ProfileFingerprint::as_str()` — opaque provenance metadata only, never
    /// reinterpreted by the port itself.
    pub profile_fingerprint: &'a str,
    pub dimension: u32,
    pub query: &'a str,
    pub deadline_unix_ms: i64,
}

/// A completed call's outcome — mirrors `ops.retrieval_embedding_rpc_calls.outcome`'s closed
/// set (migration 0141) so a caller can match exhaustively without a wildcard arm hiding a
/// state this port can actually return.
#[derive(Debug, Clone, PartialEq)]
pub enum RetrievalEmbeddingOutcome {
    /// Non-secret model-stamp fields only (ADR-0012 §2: "vector + non-secret
    /// `EmbeddingModelStamp`") — no route/provider/model identifier a caller could use to
    /// reconstruct a credential path.
    Embedded {
        vector: Vec<f32>,
        provider_id: String,
        model_id: String,
        model_revision: String,
        dimension: u32,
    },
    /// The call was structurally valid but nothing needed embedding (e.g. an empty batch) —
    /// distinct from [`Self::Unavailable`], which is a real failure the dense lane must degrade
    /// around.
    Skipped,
    /// Every worker-side *or transport-side* failure family (rate limit / timeout / auth
    /// failure / budget exhausted / worker busy / provider `OutcomeUnknown` / RPC dial failure
    /// / unparseable response) — ADR-0012 §2 "All worker failures ... degrade the dense lane
    /// only". A dial timeout or a malformed envelope is exactly as unhelpful to the caller as a
    /// worker-reported failure and gets the identical treatment: fold it in here rather than a
    /// hard `Err`, so `bins/gateway`'s dense lane always degrades on this port and never has a
    /// second failure shape to branch on. `reason` is a short machine-stable tag (e.g.
    /// `"TRANSPORT"`, `"INVALID_RESPONSE"`), never a provider error message (no secret/PII
    /// surface).
    Unavailable { reason: String },
}

/// [`RetrievalEmbeddingPort::embed_query`] failure — reserved for a caller-fault the port
/// rejects before ever dialing the worker (malformed authorization, expired deadline, a
/// dependency the registration INSERT itself needs being down). Every failure downstream of a
/// successful registration is a completed-but-unhelpful
/// [`RetrievalEmbeddingOutcome::Unavailable`], not an `Err` — see that variant's doc. §52/CLAUDE.md
/// closed two-error-enum rule: no bespoke port error type, `ErrorCode` only.
pub type RetrievalEmbeddingPortError = ErrorCode;

/// ADR-0012's sole Gateway-side embedding seam. See module doc for why this exists instead of
/// `bins/gateway` depending on `EmbeddingProvider` directly.
#[async_trait]
pub trait RetrievalEmbeddingPort: Send + Sync {
    async fn embed_query(
        &self,
        input: RetrievalEmbeddingInput<'_>,
    ) -> Result<RetrievalEmbeddingOutcome, ErrorCode>;
}
