//! `retrieval-provider::adapters` — T7.1: §19 "dashscope" and "custom" adapter slots (spec's
//! own `retrieval-provider/` tree, line ~3483) plus the Contract Tests' test double.
//!
//! §19 Architecture Gate: "Only retrieval-provider/adapters may import provider client" — this
//! file (and the crate it lives in) is the one place in the workspace that names DashScope's
//! wire shapes. `crates/adapters/src/dashscope.rs` is a stale T0.x placeholder predating this
//! crate's Phase 7 skeleton (`0deb953` "p7-prep") — flagged separately (see this task's final
//! report), not touched here (`crates/adapters` is not this task's file).
//!
//! ## Credential wiring
//!
//! [`DashscopeEmbeddingProvider::new`] builds its [`HttpExternalCall`] with a
//! `humaux_infra_egress::http::EnvCredentialSource` reading `DASHSCOPE_API_KEY` — that type's
//! own doc: a dev/OSS-default implementation, not the production credential path (a real
//! deployment's `RetrievalCredentialSource` belongs behind OpenBao, §7/"Key Hierarchy"). The
//! transport injects `Authorization: Bearer <key>` + `Content-Type: application/json` on every
//! call (`crates/infra-egress/src/http.rs` module doc, "Credential injection") — this adapter
//! never sees or formats the plaintext key itself.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use humaux_adapters::disclosure::{self, DeletionCapability, DisclosureOutcome, DisclosureSource};
use humaux_adapters::model_call_ledger;
use humaux_adapters::postgres::RetrievalWorkerDbPool;
use humaux_adapters::provider_budget::{self, ProviderBudgetRequest};
use humaux_adapters::retrieval_query_source;
use humaux_domain::dataclass::DataClass;
use humaux_domain::egress::{
    self, AuthorizedEgressPayload, ExternalCall, PrivateDataPurpose, ProcessorId,
};
use humaux_domain::error::ErrorCode;
use humaux_domain::ids::TenantId;
use humaux_infra_egress::http::{EnvCredentialSource, HttpEgressConfig, HttpExternalCall};
use serde::{Deserialize, Serialize};

use crate::admission;
use crate::contract::{
    EmbeddingBatch, EmbeddingModelDescriptor, EmbeddingProvider, RerankBatch,
    RerankModelDescriptor, RerankProvider, RerankedItem, RetrievalQueryCallContext,
    SealedRetrievalCard, SealedRetrievalQuery, validate_embedding_batch, validate_rerank_batch,
};
use crate::metrics;

// ============================================================================
// Test double — backs every Contract Test in `tests/contract_tests.rs`.
// ============================================================================

/// §19 "Provider Plane 测试 · Contract Tests" test double: a deterministic, network-free
/// `EmbeddingProvider` + `RerankProvider` pair. Vectors/scores are a pure function of the
/// sealed text's own content — same input always produces the same output, so a Contract Test
/// can assert on shape (dimension, ordering, count) without pinning a specific provider's
/// actual numbers.
pub struct TestDoubleProvider {
    embedding_model: EmbeddingModelDescriptor,
    rerank_model: RerankModelDescriptor,
    /// §19 "provider error mapping" Contract Test seam: `force_next_error` makes the *next*
    /// call fail with a caller-chosen `ErrorCode` instead of running its normal deterministic
    /// logic, proving the trait's `Result<_, ErrorCode>` plumbing carries the code through
    /// unaltered. Cleared by that one call (`take_forced_error`), not sticky.
    forced_error: Mutex<Option<ErrorCode>>,
}

impl TestDoubleProvider {
    pub fn new(
        embedding_model: EmbeddingModelDescriptor,
        rerank_model: RerankModelDescriptor,
    ) -> Self {
        Self {
            embedding_model,
            rerank_model,
            forced_error: Mutex::new(None),
        }
    }

    /// Makes the next `embed_queries`/`embed_cards`/`rerank` call return `code` instead of its
    /// normal result — see struct doc.
    pub fn force_next_error(&self, code: ErrorCode) {
        *self
            .forced_error
            .lock()
            .expect("forced_error mutex poisoned") = Some(code);
    }

    fn take_forced_error(&self) -> Option<ErrorCode> {
        self.forced_error
            .lock()
            .expect("forced_error mutex poisoned")
            .take()
    }

    /// Deterministic pseudo-embedding: spreads each UTF-8 byte's magnitude across `dimension`
    /// slots by position modulo `dimension`, so equal text always yields an equal vector and
    /// different text (almost always) yields a different one, without needing a real model.
    fn deterministic_vector(text: &str, dimension: u32) -> Vec<f32> {
        let dimension = dimension.max(1) as usize;
        let mut vector = vec![0f32; dimension];
        for (i, b) in text.bytes().enumerate() {
            vector[i % dimension] += f32::from(b) / 255.0;
        }
        vector
    }

    fn embed_texts(&self, dimension: u32, texts: &[&str]) -> Result<EmbeddingBatch, ErrorCode> {
        if let Some(err) = self.take_forced_error() {
            return Err(err);
        }
        // Dimension must be checked even for an empty batch (§19 "Incompatible Model Change") —
        // `validate_embedding_batch` itself does that ahead of its own empty short-circuit, so
        // it must run before, not after, this function's own empty-batch return.
        validate_embedding_batch(&self.embedding_model, dimension, texts)?;
        if texts.is_empty() {
            return Ok(EmbeddingBatch {
                dimension,
                vectors: Vec::new(),
                input_tokens: 0,
            });
        }
        let input_tokens = texts.iter().map(|t| t.chars().count() as u64).sum();
        let vectors = texts
            .iter()
            .map(|t| Self::deterministic_vector(t, dimension))
            .collect();
        Ok(EmbeddingBatch {
            dimension,
            vectors,
            input_tokens,
        })
    }
}

#[async_trait::async_trait]
impl EmbeddingProvider for TestDoubleProvider {
    fn model(&self) -> &EmbeddingModelDescriptor {
        &self.embedding_model
    }

    async fn embed_queries(
        &self,
        _context: &RetrievalQueryCallContext<'_>,
        dimension: u32,
        queries: &[SealedRetrievalQuery],
    ) -> Result<EmbeddingBatch, ErrorCode> {
        let texts: Vec<&str> = queries.iter().map(SealedRetrievalQuery::as_str).collect();
        self.embed_texts(dimension, &texts)
    }

    async fn embed_cards(
        &self,
        _tenant_id: TenantId,
        dimension: u32,
        cards: &[SealedRetrievalCard],
    ) -> Result<EmbeddingBatch, ErrorCode> {
        let texts: Vec<&str> = cards.iter().map(SealedRetrievalCard::as_str).collect();
        self.embed_texts(dimension, &texts)
    }
}

/// Deterministic relevance proxy: shared-character count between `query` and `candidate` (not
/// a real similarity metric — just something that varies predictably with overlap so a
/// Contract Test can construct a candidate set with a known descending order).
fn overlap_score(query: &str, candidate: &str) -> f64 {
    let query_chars: std::collections::HashSet<char> = query.chars().collect();
    candidate
        .chars()
        .filter(|c| query_chars.contains(c))
        .count() as f64
}

#[async_trait::async_trait]
impl RerankProvider for TestDoubleProvider {
    fn model(&self) -> &RerankModelDescriptor {
        &self.rerank_model
    }

    async fn rerank(
        &self,
        _tenant_id: TenantId,
        query: &SealedRetrievalQuery,
        candidates: &[SealedRetrievalCard],
    ) -> Result<RerankBatch, ErrorCode> {
        if let Some(err) = self.take_forced_error() {
            return Err(err);
        }
        if candidates.is_empty() {
            return Ok(RerankBatch { items: Vec::new() });
        }
        let candidate_strs: Vec<&str> =
            candidates.iter().map(SealedRetrievalCard::as_str).collect();
        validate_rerank_batch(&self.rerank_model, query.as_str(), &candidate_strs)?;

        let mut items: Vec<RerankedItem> = candidate_strs
            .iter()
            .enumerate()
            .map(|(candidate_index, candidate)| RerankedItem {
                candidate_index,
                score: overlap_score(query.as_str(), candidate),
            })
            .collect();
        // §19 "rerank ordering" Contract Test: descending by score. A stable sort keeps ties in
        // their original candidate order rather than an arbitrary one.
        items.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(RerankBatch { items })
    }
}

// ============================================================================
// DashScope embedding adapter — the one real `EmbeddingProvider` in this crate.
// ============================================================================

const DASHSCOPE_EMBEDDINGS_ENDPOINT: &str =
    "https://dashscope.aliyuncs.com/compatible-mode/v1/embeddings";

/// The env var `EnvCredentialSource` reads for this adapter's `Authorization` header — named
/// here rather than inlined at the one construction call site below so the identifier a
/// deployer must set (and the identifier `dashscope_live_smoke` checks for) are visibly the
/// same string, not two literals that could drift.
const DASHSCOPE_API_KEY_ENV_VAR: &str = "DASHSCOPE_API_KEY";

/// [`DashscopeEmbeddingProvider::embed`]'s `EgressPermit` TTL — named and documented here
/// rather than inlined at the `egress::authorize` call site (§78.1 "禁止硬编码...TTL"). Coupled
/// to §53 INV-3's 60s stale-reservation sweep window: a permit (and the disclosure/ledger rows
/// it authorizes) that is still unfinalized 60s after `authorize` is exactly what that sweep
/// looks for, so this TTL must stay well under 60s — 30s leaves headroom for the sweep's own
/// polling interval without ever legitimately needing the full window.
const EGRESS_PERMIT_TTL: Duration = Duration::from_secs(30);

/// §19.2's own error-classification rules, as a pure function over a raw HTTP status: "429：有界
/// 退避；401：根据 provider/key domain 进入 invalid/waiting_key；5xx：transient retry".
///
/// This used to be a second, hand-rolled copy of the same rule `admission::
/// classify_provider_status` already implements, and the two disagreed (401 unconditionally
/// `WaitingKey` here vs. `Unauthorized` for `ProviderKeyDomain::PlatformManaged` there — the
/// only credential domain that exists today, §19 "Provider Credential 边界"; no
/// `CUSTOMER_RETRIEVAL_BYOK` trust domain exists yet). Now a thin delegate to that one
/// function, so there is a single source of truth for the mapping — this function has exactly
/// one call site (below) to update once `CUSTOMER_RETRIEVAL_BYOK` lands.
///
/// Kept as `u16 -> ErrorCode` (not `-> Option<ErrorCode>`, unlike `classify_provider_status`)
/// because `tests/contract_tests.rs` already compiles against this exact signature and that
/// file is outside this fix's file ownership. A 2xx status has no meaningful `ErrorCode` —
/// `classify_provider_status` returns `None` for it; since this function cannot return `None`
/// it surfaces `Internal` instead — loud and obviously wrong, rather than the old code's
/// `ProviderPermanent`, which looked like a plausible real classification and would have
/// silently misclassified a successful call as permanently failed. Not reachable from a real
/// 2xx today: `HttpExternalCall`'s own `status_classifier` slot only ever runs after that
/// crate's own `response.status().is_success()` check has already failed, so this function
/// never actually sees a genuine 2xx from a live call.
///
/// This is now [`DashscopeEmbeddingProvider::new`]'s one production call site
/// (`HttpExternalCall::new`'s `status_classifier` argument) — before that wiring existed, this
/// function was reachable only from its own `#[cfg(test)]` unit tests below, and a live 401
/// landed on the wire as the same `ProviderPermanent` a real 500 would have produced.
pub fn map_dashscope_status(status: u16) -> ErrorCode {
    admission::classify_provider_status(status, admission::ProviderKeyDomain::PlatformManaged)
        .unwrap_or(ErrorCode::Internal)
}

/// DashScope's OpenAI-compatible-mode `/v1/embeddings` request shape (§19: production model is
/// `text-embedding-v4`, `dimensions` selects one of its Matryoshka options).
#[derive(Serialize)]
struct DashscopeEmbeddingRequest<'a> {
    model: &'a str,
    input: &'a [&'a str],
    dimensions: u32,
    encoding_format: &'static str,
}

#[derive(Deserialize)]
struct DashscopeEmbeddingItem {
    embedding: Vec<f32>,
    index: usize,
}

#[derive(Deserialize, Default)]
struct DashscopeUsage {
    #[serde(default)]
    total_tokens: u64,
}

#[derive(Deserialize)]
struct DashscopeEmbeddingResponse {
    data: Vec<DashscopeEmbeddingItem>,
    #[serde(default)]
    usage: DashscopeUsage,
}

/// §19 "dashscope" adapter slot: the real `EmbeddingProvider` backed by Alibaba Cloud
/// DashScope's compatible-mode embeddings endpoint, via `infra-egress`'s `HttpExternalCall`
/// (module doc: never a self-built client, §83.4/ADR-0003).
///
/// Rerank is deliberately **not** implemented here in T7.1: §17.6 fixes the standard rerank
/// path to Qdrant's built-in BM25 sparse lane, and this task ships no live DashScope rerank key
/// (task brief's own credential note) — `TestDoubleProvider` is the only `RerankProvider` this
/// crate wires today. A `DashscopeRerankProvider` is a later task's deliverable once a real key
/// exists to smoke-test against (digest task list item 6/`RerankScoreSemantics` calibration
/// work), following this same `HttpExternalCall` shape.
///
/// One instance == one `DisclosureSource` (`source` field): every disclosed row this instance
/// writes attributes to the same evidence/memory/rollup/release object.
/// // ponytail: real callers embedding a batch of `SealedRetrievalCard`s that come from more
/// // than one memory need one instance per source (or per-call source threading) — deferred
/// // until a later task's real caller shows whether that granularity is ever actually needed;
/// // `EmbeddingProvider`'s trait signature (contract.rs) deliberately carries no
/// // `DisclosureSource` parameter so ledger attribution stays this adapter's own concern, not
/// // every implementation's.
/// Maps this adapter's free-form `region: String` constructor arg onto `metrics::Region`'s
/// closed v1 bootstrap set (that enum's own R5 low-cardinality doc) for the
/// `retrieval_provider_*{region}` labels §41.2 requires.
/// // ponytail: an unrecognized region falls back to the first bootstrap region rather than
/// // widening `metrics::Region` to an open string — widen the enum (and this map) together
/// // when a real deployment onboards a fifth region.
fn metrics_region(region: &str) -> metrics::Region {
    match region {
        "cn-hangzhou" => metrics::Region::CnHangzhou,
        "cn-shanghai" => metrics::Region::CnShanghai,
        "cn-beijing" => metrics::Region::CnBeijing,
        "ap-southeast-1" => metrics::Region::ApSoutheast1,
        _ => metrics::Region::CnHangzhou,
    }
}

/// §19's in-process policy/health admission input. The authoritative four-tier sliding-window
/// TPM/RPM budget is enforced separately by `provider_budget::reserve_provider_budget` after
/// the model-call ledger reservation and before disclosure provenance. This placeholder remains
/// only for policy entitlement, provider-health and future queue/shed decisions: those inputs do
/// not yet have a real config source, so every ceiling is non-binding and the plan is permissive.
/// // ponytail: replace this policy/health placeholder only when a real entitlement or circuit-
/// // breaker caller exists; do not duplicate the DB-backed budget already enforced below.
fn unconstrained_admission_state() -> (admission::AdmissionBudgets, admission::PlanEntitlement) {
    let unlimited_tier = admission::TierBudget {
        tpm_limit: u64::MAX,
        current_tpm: 0,
        rpm_limit: u64::MAX,
        current_rpm: 0,
    };
    let budgets = admission::AdmissionBudgets {
        global: unlimited_tier,
        region: unlimited_tier,
        tenant: unlimited_tier,
        purpose: unlimited_tier,
    };
    let plan = admission::PlanEntitlement {
        purpose_allowed: true,
        monthly_quota: admission::TokenBudget {
            max_tokens: u64::MAX,
            used_tokens: 0,
        },
        fair_weight: 1,
        max_burst_tokens: u64::MAX,
    };
    (budgets, plan)
}

/// §19 Provider Plane Architecture Gate check 6/7: "Every call participates in provider
/// admission control" — runs the real [`admission::decide`] and maps its five-state output
/// onto the caller-facing [`ErrorCode`]. See [`unconstrained_admission_state`]'s doc for the
/// boundary between this policy/health decision and the authoritative persistent budget.
fn admission_gate(estimated_input_tokens: u64) -> Result<(), ErrorCode> {
    let (budgets, plan) = unconstrained_admission_state();
    let request = admission::AdmissionRequest {
        estimated_input_tokens,
        tenant_priority: admission::TenantPriority::Normal,
        plan_entitlement: plan,
        // `retrieval-provider::health`'s circuit breaker is not wired to this call site yet
        // (separate module, not this task's file) — always `Healthy` until it is.
        provider_health: admission::ProviderHealthState::Healthy,
        deadline: None,
    };
    match admission::decide(&request, &budgets) {
        admission::AdmissionDecision::Accept => Ok(()),
        admission::AdmissionDecision::RejectPolicy => Err(ErrorCode::Forbidden),
        admission::AdmissionDecision::RejectQuota => Err(ErrorCode::QuotaExhausted),
        // No queueing mechanism exists in this synchronous adapter — `Queue` and `Shed` both
        // surface as an immediate backpressure signal for the caller to retry.
        admission::AdmissionDecision::Queue | admission::AdmissionDecision::Shed => {
            Err(ErrorCode::RateLimited)
        }
    }
}

/// Parses and validates one DashScope `/v1/embeddings` response body against §19 Failure
/// Tests' "provider returns fewer docs" / "duplicate document result" / "malformed response"
/// bullets — none of the three may produce an `Ok`: `data.len()` must equal `texts_len`, every
/// `index` must land in `0..texts_len` exactly once (a clean permutation, not just a length
/// match), and every `embedding.len()` must equal `dimension`. `dimension` on the returned
/// batch is therefore always the caller's own verified argument, never an unchecked echo.
///
/// §19.1 `ModelCallLedger.input_tokens` doc: provider-reported when available (`usage` present
/// Provider id the route table (`control.retrieval_provider_routes.embedding_provider_id`,
/// migration 0088) and worker configuration use for this adapter. §78.2: one spelling, here.
pub const DASHSCOPE_PROVIDER_ID: &str = "dashscope";

/// §19 Provider Plane Architecture Gate 3/7 ("Only retrieval-provider/adapters may import
/// provider client"): binaries select an embedding provider by id (config / route table,
/// §78.1) and receive a trait object; the concrete client type never leaves this module.
/// Unknown ids are `InvalidInput` — there is no default provider (§78.1).
pub fn embedding_provider_for(
    provider_id: &str,
    pool: RetrievalWorkerDbPool,
    processor: ProcessorId,
    model: EmbeddingModelDescriptor,
    region: impl Into<String>,
    source: DisclosureSource,
) -> Result<Arc<dyn EmbeddingProvider>, ErrorCode> {
    match provider_id {
        DASHSCOPE_PROVIDER_ID => Ok(Arc::new(DashscopeEmbeddingProvider::new(
            pool, processor, model, region, source,
        )?)),
        _ => Err(ErrorCode::InvalidInput),
    }
}

/// and non-zero), `estimated_input_tokens` (this crate's own char-count estimate) otherwise —
/// a response that omits `usage` must not silently record a real, billed call as zero tokens.
fn parse_embedding_response(
    response_bytes: &[u8],
    texts_len: usize,
    dimension: u32,
    estimated_input_tokens: u64,
) -> Result<EmbeddingBatch, ErrorCode> {
    let parsed: DashscopeEmbeddingResponse =
        serde_json::from_slice(response_bytes).map_err(|_| ErrorCode::ProviderPermanent)?;
    if parsed.data.len() != texts_len {
        return Err(ErrorCode::ProviderPermanent);
    }
    let mut slots: Vec<Option<Vec<f32>>> = vec![None; texts_len];
    for item in parsed.data {
        if item.embedding.len() != dimension as usize {
            return Err(ErrorCode::ProviderPermanent);
        }
        match slots.get_mut(item.index) {
            Some(slot @ None) => *slot = Some(item.embedding),
            // Out-of-range index, or this index already filled (duplicate) — either way
            // `data` is not a clean permutation of `0..texts_len`.
            _ => return Err(ErrorCode::ProviderPermanent),
        }
    }
    let vectors: Vec<Vec<f32>> = slots
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or(ErrorCode::ProviderPermanent)?;
    let input_tokens = if parsed.usage.total_tokens > 0 {
        parsed.usage.total_tokens
    } else {
        estimated_input_tokens
    };
    Ok(EmbeddingBatch {
        dimension,
        vectors,
        input_tokens,
    })
}

pub struct DashscopeEmbeddingProvider {
    transport: Arc<dyn ExternalCall>,
    pool: RetrievalWorkerDbPool,
    processor: ProcessorId,
    model: EmbeddingModelDescriptor,
    region: String,
    source: DisclosureSource,
}

enum EmbeddingDispatchPayload<'a> {
    Query(retrieval_query_source::SerializedRetrievalQueryBatch<'a>),
    Card(AuthorizedEgressPayload),
}

impl EmbeddingDispatchPayload<'_> {
    fn payload(&self) -> &AuthorizedEgressPayload {
        match self {
            Self::Query(wire) => wire.payload(),
            Self::Card(payload) => payload,
        }
    }

    fn into_payload(self) -> AuthorizedEgressPayload {
        match self {
            Self::Query(wire) => wire.into_payload(),
            Self::Card(payload) => payload,
        }
    }
}

struct PreparedEmbeddingDispatch {
    payload: AuthorizedEgressPayload,
    permit: egress::EgressPermit,
    disclosure_id: uuid::Uuid,
    model_call_id: uuid::Uuid,
    provider_budget_reservation_id: uuid::Uuid,
}

impl DashscopeEmbeddingProvider {
    /// `pool` must be a [`RetrievalWorkerDbPool`] connection (§6.2.1 `role_retrieval_worker`) —
    /// the only role §6.2.2 grants `ops.data_disclosures`/`ops.data_disclosure_sources` writes
    /// to for `RETRIEVAL_EMBEDDING`/`RETRIEVAL_RERANK` purposes.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::Internal`] if the underlying `HttpExternalCall` fails to build (TLS-backend
    /// init failure only, per that type's own doc — a process-startup-time condition, not a
    /// per-call one). Collapsed to `ErrorCode` rather than propagating `reqwest::Error` so this
    /// crate does not need a direct `humaux-infra-network`/`reqwest` dependency just to name
    /// that error type (module doc: this crate reaches HTTP only through `infra-egress`).
    pub fn new(
        pool: RetrievalWorkerDbPool,
        processor: ProcessorId,
        model: EmbeddingModelDescriptor,
        region: impl Into<String>,
        source: DisclosureSource,
    ) -> Result<Self, ErrorCode> {
        let transport = HttpExternalCall::new(
            DASHSCOPE_EMBEDDINGS_ENDPOINT,
            processor,
            HttpEgressConfig::default(),
            Arc::new(EnvCredentialSource::new(DASHSCOPE_API_KEY_ENV_VAR)),
            // §19.2 blocker fix: wires this adapter's own status classification in — before
            // this, `HttpExternalCall::call` collapsed every non-2xx into one
            // `ProviderPermanent` and `map_dashscope_status` had zero live callers.
            map_dashscope_status,
        )
        .map_err(|_| ErrorCode::Internal)?;
        Ok(Self {
            transport: Arc::new(transport),
            pool,
            processor,
            model,
            region: region.into(),
            source,
        })
    }

    #[cfg(test)]
    fn with_test_transport(
        transport: Arc<dyn ExternalCall>,
        pool: RetrievalWorkerDbPool,
        processor: ProcessorId,
        model: EmbeddingModelDescriptor,
        region: impl Into<String>,
        source: DisclosureSource,
    ) -> Self {
        Self {
            transport,
            pool,
            processor,
            model,
            region: region.into(),
            source,
        }
    }

    async fn reserve_embedding_budget(
        &self,
        tenant_id: TenantId,
        model_call_id: uuid::Uuid,
        estimated_input_tokens: u64,
    ) -> Result<uuid::Uuid, ErrorCode> {
        let result = provider_budget::reserve_provider_budget(
            &self.pool,
            &ProviderBudgetRequest {
                tenant_id,
                model_call_id,
                provider_id: "dashscope",
                model_id: &self.model.model_id.0,
                region: &self.region,
                purpose: PrivateDataPurpose::RetrievalEmbedding,
                estimated_tokens: estimated_input_tokens,
                ttl: EGRESS_PERMIT_TTL,
            },
        )
        .await;
        match result {
            Ok(reservation) => Ok(reservation.reservation_id),
            Err(err) => {
                self.finalize_ledger_only(tenant_id, model_call_id, Duration::ZERO, err)
                    .await?;
                Err(err)
            }
        }
    }

    async fn prepare_embedding_dispatch(
        &self,
        tenant_id: TenantId,
        dimension: u32,
        data_class: DataClass,
        query_call: Option<(&RetrievalQueryCallContext<'_>, &[SealedRetrievalQuery])>,
        texts: &[&str],
        estimated_input_tokens: u64,
        card_sources: &[DisclosureSource],
    ) -> Result<PreparedEmbeddingDispatch, ErrorCode> {
        let dispatch_payload = match query_call {
            Some((_, queries)) => EmbeddingDispatchPayload::Query(
                retrieval_query_source::SerializedRetrievalQueryBatch::new(
                    &self.model.model_id.0,
                    dimension,
                    queries,
                )
                .map_err(|_| ErrorCode::InvalidInput)?,
            ),
            None => {
                let body = DashscopeEmbeddingRequest {
                    model: &self.model.model_id.0,
                    input: texts,
                    dimensions: dimension,
                    encoding_format: "float",
                };
                let bytes = serde_json::to_vec(&body).map_err(|_| ErrorCode::Internal)?;
                EmbeddingDispatchPayload::Card(AuthorizedEgressPayload::new(bytes))
            }
        };
        let permit = egress::authorize(
            tenant_id,
            self.processor,
            PrivateDataPurpose::RetrievalEmbedding,
            data_class,
            dispatch_payload.payload(),
            EGRESS_PERMIT_TTL,
        )?;
        let reservation = model_call_ledger::reserve_call(
            &self.pool,
            &model_call_ledger::ReserveCall {
                request_id: query_call.map(|(context, _)| context.request_id()),
                tenant_id: tenant_id.0,
                workspace_id: query_call.map(|(context, _)| context.workspace_id().0),
                purpose: Some("embedding".to_string()),
                provider: "dashscope".to_string(),
                model: Some(self.model.model_id.0.clone()),
                model_revision: Some(self.model.model_revision.clone()),
                estimated_cost: None,
            },
        )
        .await
        .map_err(|_| ErrorCode::Internal)?;
        let provider_budget_reservation_id = self
            .reserve_embedding_budget(tenant_id, reservation.model_call_id, estimated_input_tokens)
            .await?;
        let disclosure = match (&dispatch_payload, query_call) {
            (EmbeddingDispatchPayload::Query(wire), Some((context, _))) => {
                retrieval_query_source::reserve_retrieval_query_batch(
                    &self.pool,
                    context,
                    wire,
                    &permit,
                    &self.region,
                )
                .await
                .map(|value| value.disclosure_id)
            }
            (EmbeddingDispatchPayload::Card(payload), None) => disclosure::reserve_retrieval(
                &self.pool,
                &permit,
                &self.region,
                payload,
                None,
                if card_sources.is_empty() {
                    std::slice::from_ref(&self.source)
                } else {
                    card_sources
                },
            )
            .await
            .map_err(retrieval_query_source::RetrievalQuerySourceError::Disclosure),
            _ => unreachable!("query call and dispatch payload are constructed together"),
        };
        let disclosure_id = match disclosure {
            Ok(disclosure_id) => disclosure_id,
            Err(_) => {
                self.finalize_budgeted_ledger_only(
                    tenant_id,
                    reservation.model_call_id,
                    provider_budget_reservation_id,
                    Duration::ZERO,
                    ErrorCode::Internal,
                )
                .await?;
                return Err(ErrorCode::Internal);
            }
        };
        Ok(PreparedEmbeddingDispatch {
            payload: dispatch_payload.into_payload(),
            permit,
            disclosure_id,
            model_call_id: reservation.model_call_id,
            provider_budget_reservation_id,
        })
    }

    /// The full §7.3/§7.4/§19 sequence for one real call: pre-flight validate → policy/health
    /// admission → mint `EgressPermit` → reserve the `ModelCallLedger` row → reserve the
    /// authoritative persistent provider budget → reserve disclosure provenance → mark the
    /// budget dispatched → `HttpExternalCall::call` → finalize disclosure → parse+validate the
    /// response → record §41.2 metrics → finalize the model-call ledger → settle the budget.
    ///
    /// Empty `texts` short-circuits before any of that — no permit, no ledger row, no network
    /// call (`validate_embedding_batch`'s own doc: a zero-item batch is never worth a round
    /// trip, and §7.4's ledger has nothing truthful to record for a call that never happened).
    /// Dimension is still checked first even for an empty batch (§19 "Incompatible Model
    /// Change") — `validate_embedding_batch` itself orders it that way.
    async fn embed(
        &self,
        tenant_id: TenantId,
        dimension: u32,
        data_class: DataClass,
        query_call: Option<(&RetrievalQueryCallContext<'_>, &[SealedRetrievalQuery])>,
        texts: &[&str],
        card_sources: &[DisclosureSource],
    ) -> Result<EmbeddingBatch, ErrorCode> {
        validate_embedding_batch(&self.model, dimension, texts)?;
        if texts.is_empty() {
            return Ok(EmbeddingBatch {
                dimension,
                vectors: Vec::new(),
                input_tokens: 0,
            });
        }

        // §19.1 `ModelCallLedger.input_tokens` doc: provider-reported when available, this
        // crate's own char-count estimate otherwise. Doubles as the admission request's
        // `estimated_input_tokens` (§19 Admission Controller field 1) and as the fallback
        // token count for every exit branch below that never gets a provider-reported number.
        let estimated_input_tokens: u64 = texts.iter().map(|t| t.chars().count() as u64).sum();

        // §19 Provider Plane Architecture Gate check 6/7: "Every call participates in provider
        // admission control".
        admission_gate(estimated_input_tokens)?;

        let dispatch = self
            .prepare_embedding_dispatch(
                tenant_id,
                dimension,
                data_class,
                query_call,
                texts,
                estimated_input_tokens,
                card_sources,
            )
            .await?;

        let region_label = metrics_region(&self.region);
        let (response_bytes, latency) = self
            .call_and_finalize_disclosure(
                tenant_id,
                &dispatch.permit,
                &dispatch.payload,
                dispatch.disclosure_id,
                dispatch.model_call_id,
                dispatch.provider_budget_reservation_id,
                region_label,
                estimated_input_tokens,
            )
            .await?;

        // §19 Failure Tests: "provider returns fewer docs" / "duplicate document result" /
        // "malformed response" must all be rejected, not silently accepted with zero-filled
        // gaps — see `parse_embedding_response`'s own doc.
        let batch_result = parse_embedding_response(
            &response_bytes,
            texts.len(),
            dimension,
            estimated_input_tokens,
        );

        // §41.2 result-label altitude: a malformed/short/duplicate provider *content* problem
        // is still "ok" here — the transport call itself succeeded (2xx); see
        // `health::ProviderCallOutcome::metric_result_label`'s doc for the identical bucketing.
        let (metric_tokens, ledger_outcome, ledger_error_class) = match &batch_result {
            Ok(batch) => (
                batch.input_tokens,
                model_call_ledger::ModelCallOutcome::Succeeded,
                None,
            ),
            Err(err) => (
                estimated_input_tokens,
                model_call_ledger::ModelCallOutcome::Failed,
                Some(format!("{err:?}")),
            ),
        };
        self.record_call_outcome(
            tenant_id,
            dispatch.model_call_id,
            dispatch.provider_budget_reservation_id,
            region_label,
            "ok",
            metric_tokens,
            latency,
            ledger_outcome,
            ledger_error_class,
        )
        .await?;

        batch_result
    }

    /// `HttpExternalCall::call` plus its two immediate, always-run obligations: finalize the
    /// disclosure row (§53 INV-3, best-effort, same as before) and — on a transport-level
    /// failure only — record the §41.2 metrics / §19.1 ledger tail via
    /// [`Self::record_call_outcome`] before propagating the error (a transport failure has no
    /// response body left to parse, so [`Self::embed`] cannot run its own success-path
    /// recording on this branch). Returns the response body and measured latency on success,
    /// for [`Self::embed`] to parse/validate and record itself.
    #[allow(clippy::too_many_arguments)]
    async fn call_and_finalize_disclosure(
        &self,
        tenant_id: TenantId,
        permit: &egress::EgressPermit,
        payload: &AuthorizedEgressPayload,
        disclosure_id: uuid::Uuid,
        model_call_id: uuid::Uuid,
        provider_budget_reservation_id: uuid::Uuid,
        region_label: metrics::Region,
        estimated_input_tokens: u64,
    ) -> Result<(Vec<u8>, Duration), ErrorCode> {
        if let Err(err) = provider_budget::mark_provider_budget_dispatched(
            &self.pool,
            tenant_id,
            provider_budget_reservation_id,
        )
        .await
        {
            let _ = disclosure::finalize_retrieval(
                &self.pool,
                tenant_id.0,
                disclosure_id,
                DisclosureOutcome::Failed,
                DeletionCapability::Unknown,
            )
            .await;
            self.finalize_budgeted_ledger_only(
                tenant_id,
                model_call_id,
                provider_budget_reservation_id,
                Duration::ZERO,
                err,
            )
            .await?;
            return Err(err);
        }
        let started_at = Instant::now();
        let call_result = self.transport.call(permit, payload).await;
        let latency = started_at.elapsed();

        let outcome = match &call_result {
            Ok(_) => DisclosureOutcome::Success,
            Err(ErrorCode::Forbidden) => DisclosureOutcome::Denied,
            Err(_) => DisclosureOutcome::Failed,
        };
        // Best-effort: a finalize failure must not mask the call's own result (§53 INV-3's
        // stale-reservation sweep is the backstop for a ledger row this can't close).
        let _ = disclosure::finalize_retrieval(
            &self.pool,
            tenant_id.0,
            disclosure_id,
            outcome,
            DeletionCapability::Unknown,
        )
        .await;

        match call_result {
            Ok(bytes) => Ok((bytes, latency)),
            Err(err @ (ErrorCode::Internal | ErrorCode::Forbidden)) => {
                // Neither case reached provider network I/O: `Internal` is credential/header
                // construction failure and `Forbidden` is a local egress-policy denial. The
                // persistent budget was deliberately marked dispatched before invoking the
                // transport and therefore settles conservatively as consumed even when the
                // transport rejects locally. Metrics still omit these from §41.2's frozen set,
                // while the already-reserved model-call ledger is closed for §53 INV-3.
                self.finalize_budgeted_ledger_only(
                    tenant_id,
                    model_call_id,
                    provider_budget_reservation_id,
                    latency,
                    err,
                )
                .await?;
                Err(err)
            }
            Err(err) => {
                // Transport-level failure: the call never reached "2xx with a body", so the
                // §41.2 result label is the transport-status bucket, not "ok" (contrast the
                // content-only failures `embed` itself handles, which health.rs's own doc
                // buckets as "ok"). `Internal`/`Forbidden` are handled in the arm above and
                // never reach this match.
                let result_label = match err {
                    ErrorCode::ProviderRateLimited => "http_429",
                    ErrorCode::WaitingKey | ErrorCode::Unauthorized => "http_401",
                    ErrorCode::ProviderTransient => "timeout",
                    ErrorCode::ProviderPermanent => "http_5xx",
                    other => {
                        debug_assert!(
                            false,
                            "unexpected transport ErrorCode reaching the §41.2 result-label \
                             match: {other:?} — add an explicit arm rather than folding it \
                             into a guess"
                        );
                        "http_5xx"
                    }
                };
                // §19.1 `ModelCallLedger.input_tokens`: "provider-reported when available,
                // this crate's own char-count estimate otherwise" — a rate-limit/401/5xx/
                // timeout response carries no provider-reported `usage` at all (there is no
                // parsed body), so the estimate is what §19.1 calls for here, not `0`. `0`
                // stays reserved for [`Self::finalize_ledger_only`]'s Internal/Forbidden
                // branch, where the request was never attempted at all.
                self.record_call_outcome(
                    tenant_id,
                    model_call_id,
                    provider_budget_reservation_id,
                    region_label,
                    result_label,
                    estimated_input_tokens,
                    latency,
                    model_call_ledger::ModelCallOutcome::Failed,
                    Some(format!("{err:?}")),
                )
                .await?;
                Err(err)
            }
        }
    }

    /// §53 INV-3's ledger-close obligation for a call that never reached the provider at all
    /// (`Internal`/`Forbidden` — see the caller's own doc). Deliberately does not call
    /// [`metrics::record_provider_call`]: §41.2's `retrieval_provider_requests_total.result`
    /// set is frozen at six provider-call outcomes, and neither of these is one.
    async fn finalize_ledger_only(
        &self,
        tenant_id: TenantId,
        model_call_id: uuid::Uuid,
        latency: Duration,
        err: ErrorCode,
    ) -> Result<(), ErrorCode> {
        let latency_ms = i32::try_from(latency.as_millis()).unwrap_or(i32::MAX);
        model_call_ledger::finalize_call(
            &self.pool,
            tenant_id.0,
            model_call_id,
            model_call_ledger::ModelCallOutcome::Failed,
            &model_call_ledger::FinalizeCall {
                input_tokens: Some(0),
                latency_ms: Some(latency_ms),
                error_class: Some(format!("{err:?}")),
                ..Default::default()
            },
        )
        .await
        .map_err(|_| ErrorCode::Internal)?;
        Ok(())
    }

    /// Closes a budgeted pre-send/local failure in one database transaction. The ledger may
    /// already be terminal on a dispatch-mark conflict; settlement remains idempotent and uses
    /// the durable dispatch fact rather than the caller's interpretation of the failure.
    async fn finalize_budgeted_ledger_only(
        &self,
        tenant_id: TenantId,
        model_call_id: uuid::Uuid,
        provider_budget_reservation_id: uuid::Uuid,
        latency: Duration,
        err: ErrorCode,
    ) -> Result<(), ErrorCode> {
        let latency_ms = i32::try_from(latency.as_millis()).unwrap_or(i32::MAX);
        provider_budget::finalize_and_settle_provider_budget(
            &self.pool,
            tenant_id,
            provider_budget_reservation_id,
            model_call_id,
            model_call_ledger::ModelCallOutcome::Failed,
            &model_call_ledger::FinalizeCall {
                input_tokens: Some(0),
                latency_ms: Some(latency_ms),
                error_class: Some(format!("{err:?}")),
                ..Default::default()
            },
        )
        .await?;
        Ok(())
    }

    /// §41.2 metrics + §19.1 `ModelCallLedger.finalize()` for one completed call — the shared
    /// tail both [`Self::embed`] exit branches (transport failure, and transport success
    /// regardless of whether the response then parsed/validated) run.
    #[allow(clippy::too_many_arguments)]
    async fn record_call_outcome(
        &self,
        tenant_id: TenantId,
        model_call_id: uuid::Uuid,
        provider_budget_reservation_id: uuid::Uuid,
        region_label: metrics::Region,
        result_label: &'static str,
        tokens: u64,
        latency: Duration,
        outcome: model_call_ledger::ModelCallOutcome,
        error_class: Option<String>,
    ) -> Result<(), ErrorCode> {
        metrics::record_provider_call(
            metrics::Provider::DashScope,
            admission::RetrievalPurpose::Embedding,
            region_label,
            result_label,
            tokens,
            0,
            metrics::Currency::Cny,
            latency.as_secs_f64(),
        );
        let latency_ms = i32::try_from(latency.as_millis()).unwrap_or(i32::MAX);
        provider_budget::finalize_and_settle_provider_budget(
            &self.pool,
            tenant_id,
            provider_budget_reservation_id,
            model_call_id,
            outcome,
            &model_call_ledger::FinalizeCall {
                input_tokens: Some(tokens as i64),
                latency_ms: Some(latency_ms),
                error_class,
                ..Default::default()
            },
        )
        .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl EmbeddingProvider for DashscopeEmbeddingProvider {
    fn model(&self) -> &EmbeddingModelDescriptor {
        &self.model
    }

    async fn embed_queries(
        &self,
        context: &RetrievalQueryCallContext<'_>,
        dimension: u32,
        queries: &[SealedRetrievalQuery],
    ) -> Result<EmbeddingBatch, ErrorCode> {
        let texts: Vec<&str> = queries.iter().map(SealedRetrievalQuery::as_str).collect();
        let data_class = queries
            .iter()
            .map(SealedRetrievalQuery::data_class)
            .max()
            .unwrap_or(DataClass::SecretMaterial);
        self.embed(
            context.authorization().tenant_id(),
            dimension,
            data_class,
            Some((context, queries)),
            &texts,
            &[],
        )
        .await
    }

    async fn embed_cards(
        &self,
        tenant_id: TenantId,
        dimension: u32,
        cards: &[SealedRetrievalCard],
    ) -> Result<EmbeddingBatch, ErrorCode> {
        let texts: Vec<&str> = cards.iter().map(SealedRetrievalCard::as_str).collect();
        let data_class = cards
            .iter()
            .map(SealedRetrievalCard::data_class)
            .max()
            .unwrap_or(DataClass::SecretMaterial);
        self.embed(
            tenant_id,
            dimension,
            data_class,
            None,
            &texts,
            std::slice::from_ref(&self.source),
        )
        .await
    }

    async fn embed_cards_for_memories(
        &self,
        tenant_id: TenantId,
        dimension: u32,
        cards: &[SealedRetrievalCard],
        memory_ids: &[uuid::Uuid],
    ) -> Result<EmbeddingBatch, ErrorCode> {
        if memory_ids.len() != cards.len() || memory_ids.is_empty() {
            return Err(ErrorCode::InvalidInput);
        }
        let texts: Vec<&str> = cards.iter().map(SealedRetrievalCard::as_str).collect();
        let data_class = cards
            .iter()
            .map(SealedRetrievalCard::data_class)
            .max()
            .unwrap_or(DataClass::SecretMaterial);
        let sources: Vec<DisclosureSource> = memory_ids
            .iter()
            .copied()
            .map(DisclosureSource::Memory)
            .collect();
        self.embed(tenant_id, dimension, data_class, None, &texts, &sources)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{BTreeMap, BTreeSet};
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId};
    use humaux_domain::ids::{UserId, WorkspaceId};
    use humaux_local_secret_scan::{LocalSecretScanner, LocalSecretScannerConfig};
    use humaux_retrieval::request::{RetrievalIntent, build_request};
    use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
    use postgres::config::Host;
    use postgres::{Client, NoTls};

    const QUERY_FIXTURE_DB: &str = "humaux_thread_request_guard_20260828";

    struct DeterministicEmbeddingTransport {
        calls: AtomicUsize,
        payloads: Mutex<Vec<Vec<u8>>>,
        forced_error: Mutex<Option<ErrorCode>>,
        pre_send_probe: Mutex<Option<(String, uuid::Uuid, uuid::Uuid)>>,
        observed_sources_before_send: AtomicUsize,
    }

    impl DeterministicEmbeddingTransport {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                payloads: Mutex::new(Vec::new()),
                forced_error: Mutex::new(None),
                pre_send_probe: Mutex::new(None),
                observed_sources_before_send: AtomicUsize::new(0),
            }
        }

        fn arm_pre_send_probe(
            &self,
            owner_dsn: String,
            tenant_id: uuid::Uuid,
            logical_call_id: uuid::Uuid,
        ) {
            *self.pre_send_probe.lock().expect("probe mutex") =
                Some((owner_dsn, tenant_id, logical_call_id));
        }

        fn force_next_error(&self, error: ErrorCode) {
            *self.forced_error.lock().expect("forced error mutex") = Some(error);
        }
    }

    #[async_trait::async_trait]
    impl ExternalCall for DeterministicEmbeddingTransport {
        async fn call(
            &self,
            permit: &egress::EgressPermit,
            payload: &AuthorizedEgressPayload,
        ) -> Result<Vec<u8>, ErrorCode> {
            if permit.payload_sha256() != payload.sha256()
                || permit.purpose() != PrivateDataPurpose::RetrievalEmbedding
            {
                return Err(ErrorCode::Forbidden);
            }
            let value: serde_json::Value =
                serde_json::from_slice(payload.bytes()).map_err(|_| ErrorCode::Internal)?;
            let input_len = value["input"].as_array().ok_or(ErrorCode::Internal)?.len();
            let dimension = value["dimensions"].as_u64().ok_or(ErrorCode::Internal)? as usize;
            let pre_send_probe = { self.pre_send_probe.lock().expect("probe mutex").take() };
            if let Some((dsn, tenant_id, logical_call_id)) = pre_send_probe {
                let count = tokio::task::spawn_blocking(move || {
                    let mut client =
                        Client::connect(&dsn, NoTls).map_err(|_| ErrorCode::Internal)?;
                    Ok::<i64, ErrorCode>(
                        client
                            .query_one(
                                "SELECT count(*) FROM private.retrieval_query_sources q \
                                 JOIN ops.data_disclosure_sources s ON s.query_source_id=q.query_source_id \
                                 WHERE q.tenant_id=$1 AND q.logical_call_id=$2",
                                &[&tenant_id, &logical_call_id],
                            )
                            .map_err(|_| ErrorCode::Internal)?
                            .get(0),
                    )
                })
                .await
                .map_err(|_| ErrorCode::Internal)??;
                self.observed_sources_before_send
                    .store(count as usize, Ordering::SeqCst);
            }
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.payloads
                .lock()
                .expect("payload mutex")
                .push(payload.bytes().to_vec());
            if let Some(error) = self.forced_error.lock().expect("forced error mutex").take() {
                return Err(error);
            }
            let data: Vec<serde_json::Value> = (0..input_len)
                .map(|index| {
                    serde_json::json!({
                        "embedding": vec![index as f32; dimension],
                        "index": index
                    })
                })
                .collect();
            serde_json::to_vec(&serde_json::json!({
                "data": data,
                "usage": {"total_tokens": input_len}
            }))
            .map_err(|_| ErrorCode::Internal)
        }
    }

    struct QueryProviderHandle {
        rt: tokio::runtime::Runtime,
        pool: Option<RetrievalWorkerDbPool>,
        admin: Client,
        tenant_id: uuid::Uuid,
        user_id: uuid::Uuid,
        workspace_id: uuid::Uuid,
        evidence_id: uuid::Uuid,
        owner_dsn: String,
    }

    struct QueryProviderFixture;

    fn checked_query_dsn(
        name: &str,
        expected_user: Option<&str>,
    ) -> Result<String, DbFixtureSkipReason> {
        let dsn = std::env::var(name).map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let config = postgres::Config::from_str(&dsn)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
        let host_ok = matches!(config.get_hosts(), [Host::Tcp(host)] if host == "127.0.0.1");
        let port_ok = matches!(config.get_ports(), [61719]);
        if !host_ok
            || !port_ok
            || config.get_dbname() != Some(QUERY_FIXTURE_DB)
            || expected_user.is_some_and(|user| config.get_user() != Some(user))
        {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "query provider fixture requires exact 127.0.0.1:61719 guard DSNs".into(),
            ));
        }
        Ok(dsn)
    }

    fn seed_query_provider_budget_limits(
        admin: &mut Client,
        tenant_id: uuid::Uuid,
    ) -> Result<(), DbFixtureSkipReason> {
        for (row_tenant, region, purpose) in [
            (None, None, None),
            (None, Some("cn-hangzhou"), None),
            (Some(tenant_id), None, None),
            (Some(tenant_id), None, Some("RETRIEVAL_EMBEDDING")),
        ] {
            admin
                .execute(
                    "INSERT INTO control.retrieval_provider_admission_limits \
                       (tenant_id,provider_id,region,purpose,tpm_limit,rpm_limit,effective_from) \
                     VALUES($1,'dashscope',$2,$3,1000000000,1000000000, \
                            clock_timestamp()-interval '1 second') \
                     ON CONFLICT (provider_id,region,tenant_id,purpose) \
                       WHERE effective_to IS NULL \
                     DO UPDATE SET tpm_limit=EXCLUDED.tpm_limit,rpm_limit=EXCLUDED.rpm_limit",
                    &[&row_tenant, &region, &purpose],
                )
                .map_err(|error| {
                    DbFixtureSkipReason::IsolationSetupFailed(format!(
                        "query provider budget limit seed failed: {error}"
                    ))
                })?;
        }
        Ok(())
    }

    fn seed_query_provider_tenant(admin: &mut Client) -> Result<uuid::Uuid, DbFixtureSkipReason> {
        let tenant_id = admin
            .query_one(
                "INSERT INTO control.tenants(name) VALUES('query provider SUT fixture') \
                 RETURNING tenant_id",
                &[],
            )
            .map_err(|error| DbFixtureSkipReason::IsolationSetupFailed(error.to_string()))?
            .get(0);
        seed_query_provider_budget_limits(admin, tenant_id)?;
        Ok(tenant_id)
    }

    impl DbIntegrationFixture for QueryProviderFixture {
        type Handle = QueryProviderHandle;

        fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
            let owner_dsn = checked_query_dsn("HUMAUX_TEST_PG_DSN", None)?;
            let retrieval_dsn = checked_query_dsn(
                "HUMAUX_RETRIEVAL_WORKER_PG_DSN",
                Some("role_retrieval_worker"),
            )?;
            let mut admin = Client::connect(&owner_dsn, NoTls)
                .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
            let ready: bool = admin
                .query_one(
                    "SELECT to_regclass('private.retrieval_query_sources') IS NOT NULL \
                       AND EXISTS (SELECT 1 FROM information_schema.columns \
                         WHERE table_schema='private' AND table_name='retrieval_query_sources' \
                           AND column_name='query_ordinal')",
                    &[],
                )
                .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
                .get(0);
            if !ready {
                return Err(DbFixtureSkipReason::IsolationSetupFailed(
                    "migration 0118 is not applied".into(),
                ));
            }
            let role_ok: bool = Client::connect(&retrieval_dsn, NoTls)
                .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?
                .query_one(
                    "SELECT session_user='role_retrieval_worker' AND current_user='role_retrieval_worker' \
                       AND NOT (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user)",
                    &[],
                )
                .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
                .get(0);
            if !role_ok {
                return Err(DbFixtureSkipReason::IsolationSetupFailed(
                    "retrieval worker role identity mismatch".into(),
                ));
            }
            let tenant_id = seed_query_provider_tenant(&mut admin)?;
            let user_id = uuid::Uuid::now_v7();
            admin
                .execute(
                    "INSERT INTO control.users(user_id,state) VALUES($1,'ACTIVE')",
                    &[&user_id],
                )
                .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
            let workspace_id: uuid::Uuid = admin
                .query_one(
                    "INSERT INTO control.workspaces(tenant_id,name) VALUES($1,'query provider workspace') RETURNING workspace_id",
                    &[&tenant_id],
                )
                .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
                .get(0);
            admin
                .execute(
                    "INSERT INTO control.memberships(tenant_id,user_id,role,state) VALUES($1,$2,'member','ACTIVE')",
                    &[&tenant_id, &user_id],
                )
                .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
            let domain_id: uuid::Uuid = admin
                .query_one(
                    "INSERT INTO control.private_reasoning_domains(tenant_id,name) VALUES($1,'query provider domain') RETURNING reasoning_domain_id",
                    &[&tenant_id],
                )
                .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
                .get(0);
            let evidence_id: uuid::Uuid = admin
                .query_one(
                    "INSERT INTO private.evidence_objects \
                       (tenant_id,evidence_kind,payload_sha256,data_class,origin_class,visibility_class,reasoning_domain_id) \
                     VALUES($1,'EVENT',$2,'PRIVATE','AuthenticatedAgent','TENANT_SHARED',$3) RETURNING evidence_id",
                    &[&tenant_id, &vec![11_u8; 32], &domain_id],
                )
                .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
                .get(0);
            admin
                .execute(
                    "INSERT INTO private.events(event_id,event_kind,payload) VALUES($1,'TOOL_CALL','{}'::jsonb)",
                    &[&evidence_id],
                )
                .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
            let pool = rt
                .block_on(RetrievalWorkerDbPool::connect(&retrieval_dsn))
                .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
            Ok(QueryProviderHandle {
                rt,
                pool: Some(pool),
                admin,
                tenant_id,
                user_id,
                workspace_id,
                evidence_id,
                owner_dsn,
            })
        }
    }

    fn sealed_query_fixture(text: &str) -> SealedRetrievalQuery {
        let scanner = LocalSecretScanner::new(LocalSecretScannerConfig {
            executable: PathBuf::from(
                std::env::var("HUMAUX_TEST_GITLEAKS_BIN")
                    .expect("provider SUT fixture requires HUMAUX_TEST_GITLEAKS_BIN"),
            ),
            expected_version: std::env::var("HUMAUX_TEST_GITLEAKS_VERSION")
                .expect("provider SUT fixture requires HUMAUX_TEST_GITLEAKS_VERSION"),
            expected_executable_sha256: std::env::var("HUMAUX_TEST_GITLEAKS_SHA256")
                .expect("provider SUT fixture requires HUMAUX_TEST_GITLEAKS_SHA256"),
            timeout: Duration::from_secs(5),
            max_payload_bytes: 64 * 1024,
            finding_exit_code: 1,
        })
        .expect("pinned scanner fixture");
        let profile =
            humaux_retrieval::request::resolve_registered_retrieval_profile(&BTreeMap::new())
                .expect("registered profile");
        let request = build_request(
            RetrievalIntent::new(text.to_owned(), vec![], BTreeSet::new(), BTreeSet::new())
                .expect("text intent"),
            &profile,
        )
        .expect("trusted request");
        scanner
            .seal_query(&request.trusted_query().expect("text query"))
            .expect("clean sealed query")
    }

    fn query_provider_and_authorization(
        handle: &mut QueryProviderHandle,
        transport: Arc<DeterministicEmbeddingTransport>,
    ) -> (DashscopeEmbeddingProvider, AuthorizationScope, WorkspaceId) {
        let provider = DashscopeEmbeddingProvider::with_test_transport(
            transport,
            handle.pool.take().expect("retrieval pool"),
            ProcessorId(uuid::Uuid::now_v7()),
            EmbeddingModelDescriptor {
                model_id: crate::contract::ModelId("text-embedding-v4".into()),
                model_revision: "fixture".into(),
                dimension_options: vec![2],
                max_input_tokens: 128,
                batch_supported: true,
                dense_supported: true,
                sparse_supported: false,
            },
            "cn-hangzhou",
            DisclosureSource::Evidence(handle.evidence_id),
        );
        let workspace_id = WorkspaceId(handle.workspace_id);
        let authorization = AuthorizationScope::new(
            TenantId(handle.tenant_id),
            PrincipalId(uuid::Uuid::now_v7()),
            Some(UserId(handle.user_id)),
            BoundedSet::new([workspace_id]).expect("bounded workspace"),
        );
        (provider, authorization, workspace_id)
    }

    fn assert_provider_budget_state(
        handle: &mut QueryProviderHandle,
        request_id: uuid::Uuid,
        expected_status: &str,
        expected_dispatched: bool,
    ) {
        let row = handle
            .admin
            .query_one(
                "SELECT b.status,b.dispatched_at IS NOT NULL,count(a.limit_id) \
                 FROM ops.model_call_ledger l \
                 JOIN ops.retrieval_provider_budget_reservations b \
                   ON b.tenant_id=l.tenant_id AND b.model_call_id=l.model_call_id \
                 JOIN ops.retrieval_provider_budget_allocations a \
                   ON a.tenant_id=b.tenant_id AND a.reservation_id=b.reservation_id \
                 WHERE l.tenant_id=$1 AND l.request_id=$2 \
                 GROUP BY b.status,b.dispatched_at",
                &[&handle.tenant_id, &request_id],
            )
            .expect("one provider budget reservation for request");
        assert_eq!(row.get::<_, String>(0), expected_status);
        assert_eq!(row.get::<_, bool>(1), expected_dispatched);
        assert_eq!(row.get::<_, i64>(2), 4);
    }

    fn trusted_query_context<'a>(
        authorization: &'a AuthorizationScope,
        workspace_id: WorkspaceId,
    ) -> (uuid::Uuid, uuid::Uuid, RetrievalQueryCallContext<'a>) {
        let request_id = uuid::Uuid::now_v7();
        let logical_call_id = uuid::Uuid::now_v7();
        let context = RetrievalQueryCallContext::new(
            authorization,
            workspace_id,
            request_id,
            logical_call_id,
            1,
        )
        .expect("trusted query call context");
        (request_id, logical_call_id, context)
    }

    fn assert_failed_ledger_without_budget(
        handle: &mut QueryProviderHandle,
        request_id: uuid::Uuid,
    ) {
        let row = handle
            .admin
            .query_one(
                "SELECT l.status,count(b.reservation_id) \
                 FROM ops.model_call_ledger l \
                 LEFT JOIN ops.retrieval_provider_budget_reservations b \
                   ON b.tenant_id=l.tenant_id AND b.model_call_id=l.model_call_id \
                 WHERE l.tenant_id=$1 AND l.request_id=$2 GROUP BY l.status",
                &[&handle.tenant_id, &request_id],
            )
            .expect("one failed ledger without budget reservation");
        assert_eq!(row.get::<_, String>(0), "FAILED");
        assert_eq!(row.get::<_, i64>(1), 0);
    }

    fn assert_committed_query_provenance(
        handle: &mut QueryProviderHandle,
        authorization: &AuthorizationScope,
        request_id: uuid::Uuid,
        logical_call_id: uuid::Uuid,
        queries: &[SealedRetrievalQuery],
        expected_wire: &[u8],
    ) {
        let rows = handle
            .admin
            .query(
                "SELECT q.query_ordinal,s.ordinal,q.profile_fingerprint,q.classifier_revision, \
                        q.query_sha256,q.query_bytes,q.wire_payload_sha256,q.wire_payload_bytes, \
                        d.payload_sha256,d.payload_bytes,q.principal_id,q.user_id,q.workspace_id, \
                        q.request_id,q.logical_call_id,q.attempt_no,s.source_kind \
                 FROM private.retrieval_query_sources q \
                 JOIN ops.data_disclosure_sources s ON s.query_source_id=q.query_source_id \
                 JOIN ops.data_disclosures d ON d.disclosure_id=s.disclosure_id \
                 WHERE q.tenant_id=$1 AND q.logical_call_id=$2 ORDER BY q.query_ordinal",
                &[&handle.tenant_id, &logical_call_id],
            )
            .expect("read committed pre-send provenance");
        assert_eq!(rows.len(), queries.len());
        let wire_sha = AuthorizedEgressPayload::new(expected_wire.to_vec()).sha256();
        for (ordinal, (row, query)) in rows.iter().zip(queries).enumerate() {
            assert_eq!(row.get::<_, i32>(0), ordinal as i32);
            assert_eq!(row.get::<_, i32>(1), ordinal as i32);
            assert_eq!(
                row.get::<_, String>(2),
                query.profile_fingerprint_identity().as_str()
            );
            assert_eq!(row.get::<_, String>(3), query.classifier_revision());
            assert_eq!(row.get::<_, Vec<u8>>(4), query.payload_sha256_bytes());
            assert_eq!(row.get::<_, i64>(5), query.payload_bytes() as i64);
            assert_eq!(row.get::<_, Vec<u8>>(6), wire_sha);
            assert_eq!(row.get::<_, i64>(7), expected_wire.len() as i64);
            assert_eq!(row.get::<_, Vec<u8>>(8), wire_sha);
            assert_eq!(row.get::<_, i64>(9), expected_wire.len() as i64);
            assert_eq!(row.get::<_, uuid::Uuid>(10), authorization.principal().0);
            assert_eq!(row.get::<_, uuid::Uuid>(11), handle.user_id);
            assert_eq!(row.get::<_, uuid::Uuid>(12), handle.workspace_id);
            assert_eq!(row.get::<_, uuid::Uuid>(13), request_id);
            assert_eq!(row.get::<_, uuid::Uuid>(14), logical_call_id);
            assert_eq!(row.get::<_, i32>(15), 1);
            assert_eq!(row.get::<_, String>(16), "RETRIEVAL_QUERY");
        }
    }

    fn query_provenance_counts(handle: &mut QueryProviderHandle) -> (i64, i64, i64) {
        let row = handle
            .admin
            .query_one(
                "SELECT \
                   (SELECT count(*) FROM private.retrieval_query_sources WHERE tenant_id=$1), \
                   (SELECT count(*) FROM ops.data_disclosures WHERE tenant_id=$1), \
                   (SELECT count(*) FROM ops.data_disclosure_sources WHERE tenant_id=$1)",
                &[&handle.tenant_id],
            )
            .expect("query provenance counts");
        (row.get(0), row.get(1), row.get(2))
    }

    fn assert_source_failure_is_presend_and_atomic(
        handle: &mut QueryProviderHandle,
        provider: &DashscopeEmbeddingProvider,
        transport: &DeterministicEmbeddingTransport,
        authorization: &AuthorizationScope,
        workspace_id: WorkspaceId,
        logical_call_id: uuid::Uuid,
        queries: &[SealedRetrievalQuery],
    ) {
        let before = query_provenance_counts(handle);
        let conflicting_request_id = uuid::Uuid::now_v7();
        let conflicting_context = RetrievalQueryCallContext::new(
            authorization,
            workspace_id,
            conflicting_request_id,
            logical_call_id,
            1,
        )
        .expect("second request identity with conflicting logical attempt");
        let failure = handle
            .rt
            .block_on(provider.embed_queries(&conflicting_context, 2, queries));
        assert_eq!(failure.err(), Some(ErrorCode::Internal));
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            query_provenance_counts(handle),
            before,
            "failed source reservation leaves no source/disclosure residue"
        );
        assert_provider_budget_state(handle, conflicting_request_id, "RELEASED", false);
    }

    #[test]
    fn dashscope_query_sut_commits_exact_batch_provenance_before_transport() {
        run_db_fixture::<QueryProviderFixture, _>(
            "dashscope_query_provenance_sut",
            |mut handle| {
                let transport = Arc::new(DeterministicEmbeddingTransport::new());
                let (provider, authorization, workspace_id) =
                    query_provider_and_authorization(&mut handle, transport.clone());
                let (request_id, logical_call_id, context) =
                    trusted_query_context(&authorization, workspace_id);
                transport.arm_pre_send_probe(
                    handle.owner_dsn.clone(),
                    handle.tenant_id,
                    logical_call_id,
                );
                let queries = [
                    sealed_query_fixture("first provider query"),
                    sealed_query_fixture("second provider query"),
                ];
                let expected_wire = retrieval_query_source::SerializedRetrievalQueryBatch::new(
                    &provider.model.model_id.0,
                    2,
                    &queries,
                )
                .expect("deterministic wire serialization")
                .payload()
                .bytes()
                .to_vec();

                let batch = handle
                    .rt
                    .block_on(provider.embed_queries(&context, 2, &queries))
                    .expect("actual query SUT succeeds through deterministic transport");
                assert_eq!(batch.vectors.len(), queries.len());
                assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
                assert_provider_budget_state(&mut handle, request_id, "CONSUMED", true);
                assert_eq!(
                    transport
                        .observed_sources_before_send
                        .load(Ordering::SeqCst),
                    queries.len(),
                    "all query source relations must be committed before transport begins",
                );
                assert_eq!(
                    transport.payloads.lock().expect("payload mutex").as_slice(),
                    std::slice::from_ref(&expected_wire),
                );

                assert_committed_query_provenance(
                    &mut handle,
                    &authorization,
                    request_id,
                    logical_call_id,
                    &queries,
                    &expected_wire,
                );
                assert_source_failure_is_presend_and_atomic(
                    &mut handle,
                    &provider,
                    &transport,
                    &authorization,
                    workspace_id,
                    logical_call_id,
                    &queries,
                );
            },
        );
    }

    #[test]
    fn persistent_budget_rejection_stops_before_transport_and_disclosure() {
        run_db_fixture::<QueryProviderFixture, _>("dashscope_budget_rejection", |mut handle| {
            handle
                .admin
                .execute(
                    "UPDATE control.retrieval_provider_admission_limits SET tpm_limit=1 \
                     WHERE tenant_id=$1 AND provider_id='dashscope' \
                       AND region IS NULL AND purpose='RETRIEVAL_EMBEDDING'",
                    &[&handle.tenant_id],
                )
                .expect("narrow tenant purpose budget");
            let transport = Arc::new(DeterministicEmbeddingTransport::new());
            let (provider, authorization, workspace_id) =
                query_provider_and_authorization(&mut handle, transport.clone());
            let (request_id, _, context) = trusted_query_context(&authorization, workspace_id);
            let queries = [sealed_query_fixture("budget must stop this provider query")];
            let before = query_provenance_counts(&mut handle);

            assert_eq!(
                handle
                    .rt
                    .block_on(provider.embed_queries(&context, 2, &queries)),
                Err(ErrorCode::CostBudgetExceeded)
            );
            assert_eq!(transport.calls.load(Ordering::SeqCst), 0);
            assert_eq!(query_provenance_counts(&mut handle), before);
            assert_failed_ledger_without_budget(&mut handle, request_id);
        });
    }

    #[test]
    fn mark_failure_stops_before_transport_and_releases_budget() {
        run_db_fixture::<QueryProviderFixture, _>("dashscope_mark_failure", |mut handle| {
            let transport = Arc::new(DeterministicEmbeddingTransport::new());
            let (provider, authorization, workspace_id) =
                query_provider_and_authorization(&mut handle, transport.clone());
            let (request_id, _, context) = trusted_query_context(&authorization, workspace_id);
            let queries = [sealed_query_fixture("mark failure query")];
            let texts: Vec<&str> = queries.iter().map(SealedRetrievalQuery::as_str).collect();
            let estimated_tokens = texts.iter().map(|text| text.chars().count() as u64).sum();
            let dispatch = handle
                .rt
                .block_on(provider.prepare_embedding_dispatch(
                    TenantId(handle.tenant_id),
                    2,
                    queries[0].data_class(),
                    Some((&context, &queries)),
                    &texts,
                    estimated_tokens,
                    &[],
                ))
                .expect("prepare dispatch before forced ledger terminal state");
            handle
                .rt
                .block_on(model_call_ledger::finalize_call(
                    &provider.pool,
                    handle.tenant_id,
                    dispatch.model_call_id,
                    model_call_ledger::ModelCallOutcome::Failed,
                    &model_call_ledger::FinalizeCall::default(),
                ))
                .expect("force ledger terminal before dispatch mark");

            let result = handle.rt.block_on(provider.call_and_finalize_disclosure(
                TenantId(handle.tenant_id),
                &dispatch.permit,
                &dispatch.payload,
                dispatch.disclosure_id,
                dispatch.model_call_id,
                dispatch.provider_budget_reservation_id,
                metrics_region("cn-hangzhou"),
                estimated_tokens,
            ));
            assert_eq!(result, Err(ErrorCode::Conflict));
            assert_eq!(transport.calls.load(Ordering::SeqCst), 0);
            assert_provider_budget_state(&mut handle, request_id, "RELEASED", false);
        });
    }

    #[test]
    fn dispatched_transport_failure_consumes_budget() {
        run_db_fixture::<QueryProviderFixture, _>("dashscope_dispatched_failure", |mut handle| {
            let transport = Arc::new(DeterministicEmbeddingTransport::new());
            transport.force_next_error(ErrorCode::ProviderTransient);
            let (provider, authorization, workspace_id) =
                query_provider_and_authorization(&mut handle, transport.clone());
            let (request_id, _, context) = trusted_query_context(&authorization, workspace_id);
            let queries = [sealed_query_fixture("dispatched timeout consumes budget")];

            assert_eq!(
                handle
                    .rt
                    .block_on(provider.embed_queries(&context, 2, &queries)),
                Err(ErrorCode::ProviderTransient)
            );
            assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
            assert_provider_budget_state(&mut handle, request_id, "CONSUMED", true);
        });
    }

    /// §19.2 "provider error mapping" — the real-HTTP-status half (see `map_dashscope_status`'s
    /// own doc for why it is not yet reachable from a live call). Pinned equal to
    /// `admission::classify_provider_status(_, ProviderKeyDomain::PlatformManaged)` (that
    /// function's own tests cover it directly) — this test exists to catch the two functions
    /// drifting apart again, not to re-derive the mapping.
    #[test]
    fn dashscope_status_mapping_matches_19_2() {
        // 401 under the only credential domain that exists today (§19 "Provider Credential
        // 边界": no `CUSTOMER_RETRIEVAL_BYOK` yet) is an ops misconfiguration, not something a
        // tenant is "waiting" on — `Unauthorized`, not `WaitingKey`.
        assert_eq!(map_dashscope_status(401), ErrorCode::Unauthorized);
        assert_eq!(map_dashscope_status(403), ErrorCode::ProviderPermanent);
        assert_eq!(map_dashscope_status(429), ErrorCode::ProviderRateLimited);
        assert_eq!(map_dashscope_status(500), ErrorCode::ProviderTransient);
        assert_eq!(map_dashscope_status(503), ErrorCode::ProviderTransient);
        assert_eq!(map_dashscope_status(599), ErrorCode::ProviderTransient);
        // Anything else (400 malformed request, 404, ...) is not retryable by this adapter —
        // §19 Failure Tests' "malformed response" bucket, collapsed to the same non-retryable
        // code as 403 rather than a fresh classification.
        assert_eq!(map_dashscope_status(400), ErrorCode::ProviderPermanent);
        assert_eq!(map_dashscope_status(404), ErrorCode::ProviderPermanent);
        // 2xx has no `ErrorCode` — this function cannot say `None` (unlike the function it
        // delegates to), so it surfaces `Internal` as a loud "this should never be called with
        // a success status" signal rather than a plausible-looking wrong classification.
        assert_eq!(map_dashscope_status(200), ErrorCode::Internal);
    }

    /// §19 "Incompatible Model Change": dimension is never a free per-request choice, even for
    /// an empty batch (see the fix at both `TestDoubleProvider::embed_texts`'s and
    /// `DashscopeEmbeddingProvider::embed`'s own call sites — this pins the shared
    /// `TestDoubleProvider` half, which is what every Contract Test in `tests/` runs against).
    #[test]
    fn empty_batch_still_rejects_a_dimension_outside_model_options() {
        let provider = TestDoubleProvider::new(
            EmbeddingModelDescriptor {
                model_id: crate::contract::ModelId("m".to_string()),
                model_revision: "r".to_string(),
                dimension_options: vec![256],
                max_input_tokens: 32,
                batch_supported: true,
                dense_supported: true,
                sparse_supported: false,
            },
            RerankModelDescriptor {
                model_id: crate::contract::ModelId("rm".to_string()),
                model_revision: "r".to_string(),
                max_documents: 4,
                max_input_tokens: 32,
                score_semantics: crate::contract::RerankScoreSemantics::RawLogit,
                calibration_profile: crate::contract::CalibrationProfileId("c".to_string()),
            },
        );
        let result = provider.embed_texts(999, &[]);
        assert_eq!(result.err(), Some(ErrorCode::InvalidInput));
    }

    /// §19 Provider Plane Architecture Gate check 6/7: `admission_gate` must actually run the
    /// real decision, not just always return `Ok` — the unconstrained placeholder budgets
    /// (`unconstrained_admission_state`'s doc) admit any request today, but this pins that the
    /// call happens and does not panic across the full `u64` range the estimate can take.
    #[test]
    fn admission_gate_admits_under_the_unconstrained_placeholder_budgets() {
        assert_eq!(admission_gate(0), Ok(()));
        assert_eq!(admission_gate(1_000_000), Ok(()));
        assert_eq!(admission_gate(u64::MAX), Ok(()));
    }

    fn response_json(data: &str, usage: Option<u64>) -> Vec<u8> {
        match usage {
            Some(t) => {
                format!(r#"{{"data":[{data}],"usage":{{"total_tokens":{t}}}}}"#).into_bytes()
            }
            None => format!(r#"{{"data":[{data}]}}"#).into_bytes(),
        }
    }

    /// §19 Failure Tests "provider returns fewer docs" / Provider Plane 测试 blocker fix: a
    /// response with fewer `data` items than `texts_len` must be rejected, not zero-filled.
    #[test]
    fn parse_embedding_response_rejects_fewer_docs_than_requested() {
        let bytes = response_json(r#"{"embedding":[1.0,2.0],"index":0}"#, Some(2));
        let result = parse_embedding_response(&bytes, 2, 2, 10);
        assert_eq!(result.err(), Some(ErrorCode::ProviderPermanent));
    }

    /// §19 Failure Tests "duplicate document result": two items claiming the same `index` must
    /// be rejected, not have the second silently overwrite the first.
    #[test]
    fn parse_embedding_response_rejects_a_duplicate_index() {
        let bytes = response_json(
            r#"{"embedding":[1.0,2.0],"index":0},{"embedding":[3.0,4.0],"index":0}"#,
            Some(4),
        );
        let result = parse_embedding_response(&bytes, 2, 2, 10);
        assert_eq!(result.err(), Some(ErrorCode::ProviderPermanent));
    }

    /// §80.1 注错红转绿: `texts_len = 1` makes the length guard (`data.len() != texts_len`)
    /// fire on its own for a 2-item duplicate — so this alone does not isolate the separate
    /// duplicate-slot guard (`Some(slot @ None) => ...`, else reject). Paired with the next
    /// test, the two together kill the reviewer's own repro (deleting *both* guards turns
    /// `data=[{index:0},{index:0}], texts_len=1` into `Ok([[3.0,4.0]])`); this one alone still
    /// pins that a duplicate is never silently accepted regardless of which single guard a
    /// future edit weakens.
    #[test]
    fn parse_embedding_response_rejects_a_duplicate_index_with_a_single_requested_text() {
        let bytes = response_json(
            r#"{"embedding":[1.0,2.0],"index":0},{"embedding":[3.0,4.0],"index":0}"#,
            Some(4),
        );
        let result = parse_embedding_response(&bytes, 1, 2, 10);
        assert_eq!(result.err(), Some(ErrorCode::ProviderPermanent));
    }

    /// §80.1 注错红转绿: `data.len() > texts_len` (3 items for 2 requested), pinning the
    /// explicit length guard rather than relying on the duplicate-slot guard's pigeonhole
    /// side-effect alone (see the previous test's own doc for why the two overlap).
    #[test]
    fn parse_embedding_response_rejects_more_docs_than_requested() {
        let bytes = response_json(
            r#"{"embedding":[1.0,2.0],"index":0},{"embedding":[3.0,4.0],"index":1},{"embedding":[5.0,6.0],"index":0}"#,
            Some(6),
        );
        let result = parse_embedding_response(&bytes, 2, 2, 10);
        assert_eq!(result.err(), Some(ErrorCode::ProviderPermanent));
    }

    /// An `index` outside `0..texts_len` is not a valid permutation either — same rejection.
    #[test]
    fn parse_embedding_response_rejects_an_out_of_range_index() {
        let bytes = response_json(
            r#"{"embedding":[1.0,2.0],"index":0},{"embedding":[3.0,4.0],"index":5}"#,
            Some(4),
        );
        let result = parse_embedding_response(&bytes, 2, 2, 10);
        assert_eq!(result.err(), Some(ErrorCode::ProviderPermanent));
    }

    /// §19 Failure Tests "malformed response": an item whose `embedding.len()` disagrees with
    /// the requested `dimension` must be rejected rather than returned as a shorter vector.
    #[test]
    fn parse_embedding_response_rejects_wrong_embedding_length() {
        let bytes = response_json(r#"{"embedding":[1.0],"index":0}"#, Some(1));
        let result = parse_embedding_response(&bytes, 1, 2, 10);
        assert_eq!(result.err(), Some(ErrorCode::ProviderPermanent));
    }

    /// Not valid JSON at all — still a "malformed response" rejection.
    #[test]
    fn parse_embedding_response_rejects_invalid_json() {
        let result = parse_embedding_response(b"not json", 1, 2, 10);
        assert_eq!(result.err(), Some(ErrorCode::ProviderPermanent));
    }

    /// A clean, correctly-shaped response is accepted, and `usage.total_tokens` (when present
    /// and non-zero) wins over the char-count estimate.
    #[test]
    fn parse_embedding_response_accepts_a_clean_response_and_prefers_reported_tokens() {
        let bytes = response_json(
            r#"{"embedding":[1.0,2.0],"index":1},{"embedding":[3.0,4.0],"index":0}"#,
            Some(7),
        );
        let batch = parse_embedding_response(&bytes, 2, 2, 10).expect("well-formed response");
        assert_eq!(batch.dimension, 2);
        assert_eq!(
            batch.vectors,
            vec![vec![3.0, 4.0], vec![1.0, 2.0]],
            "order-preserving"
        );
        assert_eq!(batch.input_tokens, 7, "provider-reported usage wins");
    }

    /// §19.1 `ModelCallLedger.input_tokens` fix: an omitted/zero `usage` must fall back to the
    /// char-count estimate, not silently record a real call as zero tokens.
    #[test]
    fn parse_embedding_response_falls_back_to_estimate_when_usage_is_absent() {
        let bytes = response_json(r#"{"embedding":[1.0,2.0],"index":0}"#, None);
        let batch = parse_embedding_response(&bytes, 1, 2, 42).expect("well-formed response");
        assert_eq!(batch.input_tokens, 42);
    }

    #[test]
    fn deterministic_vector_is_pure_and_dimension_stable() {
        let a = TestDoubleProvider::deterministic_vector("hello", 8);
        let b = TestDoubleProvider::deterministic_vector("hello", 8);
        assert_eq!(a, b, "same input must produce the same output");
        assert_eq!(a.len(), 8);

        let c = TestDoubleProvider::deterministic_vector("world", 8);
        assert_ne!(a, c, "different input should (almost always) differ");
    }

    #[test]
    fn deterministic_vector_never_divides_by_zero_dimension() {
        // `dimension.max(1)` guard (struct method's own doc) — a caller that somehow reaches
        // this with `dimension = 0` must not panic on `i % dimension`.
        let v = TestDoubleProvider::deterministic_vector("x", 0);
        assert_eq!(v.len(), 1);
    }
}
