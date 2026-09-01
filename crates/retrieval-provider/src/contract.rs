//! `retrieval-provider::contract` — T7.1: §19 "Retrieval Provider Descriptor" / "Provider
//! Credential 边界" / "Provider Plane 测试 · Contract Tests" (spec lines ~3465-4131).
//!
//! Scope, verbatim from §19 "Retrieval Provider Plane": this module (and the whole
//! `retrieval-provider` crate) covers only **external managed neural retrieval** — dense
//! embedding and rerank. Qdrant Cluster's built-in BM25 sparse lane is Data Cell-internal
//! (§17.6) and never routes through here.
//!
//! ## Sealed egress types (§1.2.3 / §41.4)
//!
//! [`SealedRetrievalQuery`] / [`SealedRetrievalCard`] are canonical scanner-attested values
//! from `humaux-local-secret-scan`. Sealing performs only the pinned local privacy/Gitleaks
//! scan; it neither authorizes data access nor substitutes for the egress permit, disclosure
//! reserve/finalize path, or provider call gates that this crate owns.

//!
//! ## Provider identifiers
//!
//! [`ProviderId`]/[`RegionId`]/[`PricingProfileId`]/[`ModelId`]/[`CalibrationProfileId`] are
//! plain string newtypes kept local to this crate — same precedent
//! `humaux_infra_cell::permit::CallerId` already sets ("kept local to this crate rather than
//! added to `domain::ids`'s seven frozen newtypes"): the §7 Processor Registry
//! (`control.processors` et al.) and §19's own `control.retrieval_provider_routes` /
//! `control.provider_pricing_versions` registries that would give these a DB-backed identity
//! are later tasks' deliverables (T7.2+), out of T7.1's scope. [`RetrievalProviderDescriptor
//! ::data_policy_id`] reuses [`humaux_domain::egress::ProcessorId`] directly — the spec names
//! its type as `ProcessorId`, and that type already exists (T4.1), so this module does not
//! mint a second one.

use humaux_domain::egress::ProcessorId;
use humaux_domain::error::ErrorCode;
use humaux_domain::ids::TenantId;

// ============================================================================
// §1.2.3 / §41.4 canonical sealed egress types.
// ============================================================================

pub use humaux_adapters::retrieval_query_source::RetrievalQueryCallContext;
/// Canonical scanner-attested types live in `humaux-local-secret-scan`; this crate merely
/// consumes them at external provider boundaries. There is no local constructor or wire decode.
pub use humaux_local_secret_scan::{SealedRetrievalCard, SealedRetrievalQuery};

// ============================================================================
// §19 "Provider Descriptor" — identifiers.
// ============================================================================

/// Provider registry key (e.g. `"dashscope"`, `"custom"`) — §19 "one provider implementation
/// initially, multi-provider architecture from day one".
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProviderId(pub String);

/// Provider/region pair key (e.g. `"cn-beijing"`) — §7's `TenantDataPolicy::home_region` names
/// the same concept; kept local here rather than promoted to `domain::ids` (module doc).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RegionId(pub String);

/// §19 "Pricing Registry" (`control.provider_pricing_versions`) row key. Registry table is a
/// later task's deliverable (T7.1 scope note, digest task list item 4) — this newtype exists so
/// [`RetrievalProviderDescriptor`] can name the field today without a DB dependency.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PricingProfileId(pub String);

/// Embedding/rerank model identifier (e.g. `"text-embedding-v4"`, `"qwen3-rerank"` — §19's own
/// pricing table literal names).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelId(pub String);

/// §19 "Rerank Provider Failover 规则": "任何相关性 Gate 必须绑定
/// `(provider, model, revision, calibration_profile)`". The calibration registry itself is a
/// later task's deliverable (digest task list item 6) — this newtype lets
/// [`RerankModelDescriptor`] carry the binding key today.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CalibrationProfileId(pub String);

/// §19 "Retrieval Provider Plane" scope line, made a typed flag pair instead of a
/// free-form capability string set: "职责只针对外部 managed neural retrieval: dense embedding,
/// rerank" — there are exactly two capabilities this Plane ever expresses opinions about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetrievalCapabilities {
    pub dense_embedding: bool,
    pub rerank: bool,
}

/// §19 "Provider Descriptor" — Embedding Model Descriptor, field list verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingModelDescriptor {
    pub model_id: ModelId,
    pub model_revision: String,
    /// e.g. `text-embedding-v4`'s Matryoshka dimension set. [`validate_embedding_batch`]
    /// rejects any `dimension` not present here (§19 "Incompatible Model Change": "dimension
    /// changes" is a Projection Migration, never a request-level choice).
    pub dimension_options: Vec<u32>,
    pub max_input_tokens: u32,
    pub batch_supported: bool,
    pub dense_supported: bool,
    pub sparse_supported: bool,
}

/// §19 "Rerank Provider Failover 规则": "不同 reranker：score distribution/calibration/absolute
/// threshold/relative threshold 不能默认相同". A closed, typed set rather than a free-form
/// string (§78.2 "禁止 stringly-typed domain") — the three shapes a rerank score can plausibly
/// take before any threshold gate may interpret it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RerankScoreSemantics {
    /// Score is a calibrated probability in `[0, 1]`.
    Probability,
    /// Score is an uncalibrated raw logit — unbounded, only relative ordering is meaningful.
    RawLogit,
    /// Score has been provider-normalized into `[0, 1]` without being a calibrated probability.
    Normalized01,
}

impl RerankScoreSemantics {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Probability => "PROBABILITY",
            Self::RawLogit => "RAW_LOGIT",
            Self::Normalized01 => "NORMALIZED_0_1",
        }
    }
}

/// §19 "Provider Descriptor" — Rerank Model Descriptor, field list verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RerankModelDescriptor {
    pub model_id: ModelId,
    pub model_revision: String,
    pub max_documents: u32,
    pub max_input_tokens: u32,
    pub score_semantics: RerankScoreSemantics,
    pub calibration_profile: CalibrationProfileId,
}

/// §19 "Provider Descriptor" — `RetrievalProviderDescriptor`, field list verbatim (spec's own
/// Rust code block, lines ~3504-3517).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievalProviderDescriptor {
    pub provider_id: ProviderId,
    pub region: RegionId,
    pub capabilities: RetrievalCapabilities,
    pub embedding_models: Vec<EmbeddingModelDescriptor>,
    pub rerank_models: Vec<RerankModelDescriptor>,
    pub rpm_limit: Option<u64>,
    pub tpm_limit: Option<u64>,
    pub pricing_profile_id: PricingProfileId,
    /// §19 "Provider Credential 边界": the platform Retrieval Credential's data policy —
    /// spec's own field type is `ProcessorId` (§7), not a fresh newtype.
    pub data_policy_id: ProcessorId,
}

// ============================================================================
// §19 request/response shapes shared by every `EmbeddingProvider`/`RerankProvider`.
// ============================================================================

/// One [`EmbeddingProvider`] call's result. `vectors[i]` corresponds to the `i`-th input text
/// in the slice the caller passed — order-preserving, never a provider-declared re-ordering
/// (unlike [`RerankBatch`], which is explicitly reordered by relevance).
#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddingBatch {
    pub dimension: u32,
    pub vectors: Vec<Vec<f32>>,
    /// §19.1 `ModelCallLedger.input_tokens` — provider-reported when available, this crate's
    /// own char-count estimate otherwise (see [`validate_embedding_batch`]'s doc).
    pub input_tokens: u64,
}

/// One reranked candidate: its original index into the caller's `candidates` slice, plus the
/// provider's score for it (semantics fixed by [`RerankModelDescriptor::score_semantics`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RerankedItem {
    pub candidate_index: usize,
    pub score: f64,
}

/// One [`RerankProvider`] call's result — `items` is sorted **descending by `score`** (§19
/// "rerank ordering" Contract Test: this is the one field every rerank provider must agree on
/// regardless of `score_semantics`).
#[derive(Debug, Clone, PartialEq)]
pub struct RerankBatch {
    pub items: Vec<RerankedItem>,
}

/// Client-side pre-flight check shared by every `EmbeddingProvider` implementation — run
/// *before* any network call, so a caller never pays for a request the provider was always
/// going to refuse. Empty `texts` is not an error: it returns `Ok(())` and the caller is
/// expected to short-circuit to an empty [`EmbeddingBatch`] without touching the network at
/// all (§19 "Retrieval 成本模型": zero-item batches are free, never worth a round trip).
///
/// §19.1's own request-token formula gives no separate embedding-side ceiling beyond
/// `max_input_tokens` per item — unlike rerank's `query_tokens * document_count +
/// sum(document_tokens)` (see [`validate_rerank_batch`]), an embedding request has no
/// query/candidate cross term.
///
/// Token counting here is `str::chars().count()` — a Unicode scalar count, not a real
/// tokenizer's subword count, and not `str::len()`'s UTF-8 byte count either (which would
/// silently over-charge multi-byte text against `max_input_tokens`).
/// // ponytail: this is an estimate ceiling, not the provider's actual tokenizer; swap in the
/// // real DashScope tokenizer once cost accounting (Phase 7 `cost` module) needs the exact
/// // count instead of a conservative pre-flight bound.
pub fn validate_embedding_batch(
    model: &EmbeddingModelDescriptor,
    dimension: u32,
    texts: &[&str],
) -> Result<(), ErrorCode> {
    if !model.dimension_options.contains(&dimension) {
        return Err(ErrorCode::InvalidInput);
    }
    if texts.is_empty() {
        return Ok(());
    }
    if !model.batch_supported && texts.len() > 1 {
        return Err(ErrorCode::InvalidInput);
    }
    if texts
        .iter()
        .any(|t| t.chars().count() as u64 > u64::from(model.max_input_tokens))
    {
        return Err(ErrorCode::InvalidInput);
    }
    Ok(())
}

/// Client-side pre-flight check shared by every `RerankProvider` implementation — §19's own
/// formula, verbatim: "`query_tokens * document_count + sum(document_tokens)`" against
/// `model.max_input_tokens`, and `candidates.len()` against `model.max_documents` ("qwen3-rerank
/// 官方单次最多 500 文档"). Empty `candidates` returns `Ok(())`, same zero-item short-circuit
/// contract as [`validate_embedding_batch`].
pub fn validate_rerank_batch(
    model: &RerankModelDescriptor,
    query: &str,
    candidates: &[&str],
) -> Result<(), ErrorCode> {
    if candidates.is_empty() {
        return Ok(());
    }
    if candidates.len() as u64 > u64::from(model.max_documents) {
        return Err(ErrorCode::InvalidInput);
    }
    let query_tokens = query.chars().count() as u64;
    let document_count = candidates.len() as u64;
    let document_tokens: u64 = candidates.iter().map(|c| c.chars().count() as u64).sum();
    let request_tokens = query_tokens
        .saturating_mul(document_count)
        .saturating_add(document_tokens);
    if request_tokens > u64::from(model.max_input_tokens) {
        return Err(ErrorCode::InvalidInput);
    }
    Ok(())
}

// ============================================================================
// §19 "Retrieval Provider Plane" — the two provider traits.
// ============================================================================

/// §19's "Managed Dense Embedding" capability. Spec §1.2/§41.4 name this pair `Embedder`/
/// `Reranker`; named `EmbeddingProvider`/`RerankProvider` here to match this crate's own
/// Provider Plane vocabulary (`RetrievalProviderDescriptor`, `EmbeddingModelDescriptor`) — same
/// compile-time type-narrowing guarantee §1.2.3/§41.4 requires, no semantic difference.
///
/// Deliberately two methods, not one taking a shared "sealed text" abstraction: §1.2.3's own
/// line 1284 distinguishes "Dense write" ([`SealedRetrievalCard`]) from "Dense query"
/// ([`SealedRetrievalQuery`]) as different pipeline stages with different callers — collapsing
/// them into one generic method would let a caller accidentally embed a card where a query was
/// meant (or vice versa) and have the type system say nothing about it.
///
/// Query calls take a [`RetrievalQueryCallContext`] so tenant/principal/user/workspace and
/// request/logical-call/attempt identity arrive together from the trusted authorization path.
/// Card calls retain the established tenant-scoped source flow. One provider instance still
/// legitimately serves every tenant sharing a region/model route (§19 "统一 provider routing").
#[async_trait::async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// The model this instance is bound to — callers use this to check
    /// `dimension_options`/`max_input_tokens`/`batch_supported` before building a request.
    fn model(&self) -> &EmbeddingModelDescriptor;

    /// Embeds `queries` (dense query side, §1.2.3 line 1392) at `dimension`.
    async fn embed_queries(
        &self,
        context: &RetrievalQueryCallContext<'_>,
        dimension: u32,
        queries: &[SealedRetrievalQuery],
    ) -> Result<EmbeddingBatch, ErrorCode>;

    /// Embeds `cards` (dense write side, §1.2.3 line 1387) at `dimension`.
    async fn embed_cards(
        &self,
        tenant_id: TenantId,
        dimension: u32,
        cards: &[SealedRetrievalCard],
    ) -> Result<EmbeddingBatch, ErrorCode>;
}

/// §19's "Managed Rerank" capability (§1.2.3 line 1285: "Rerank -> SealedRetrievalQuery +
/// bounded SealedRetrievalCards"). See [`EmbeddingProvider`]'s doc for the naming note and the
/// `tenant_id`-as-parameter reasoning, both identical here.
#[async_trait::async_trait]
pub trait RerankProvider: Send + Sync {
    /// The model this instance is bound to.
    fn model(&self) -> &RerankModelDescriptor;

    /// Reranks `candidates` against `query`. §19 "Rerank Provider Failover 规则": any caller
    /// applying a relevance threshold to `RerankBatch::items[..].score` must bind that
    /// threshold to `(provider, model().model_revision, model().calibration_profile)` — this
    /// trait has no way to enforce that at the call site, it only documents the obligation the
    /// spec places on the caller.
    async fn rerank(
        &self,
        tenant_id: TenantId,
        query: &SealedRetrievalQuery,
        candidates: &[SealedRetrievalCard],
    ) -> Result<RerankBatch, ErrorCode>;
}
