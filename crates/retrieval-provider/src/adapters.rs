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
    RerankModelDescriptor, RerankProvider, RerankedItem, SealedRetrievalCard, SealedRetrievalQuery,
    validate_embedding_batch, validate_rerank_batch,
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
        _tenant_id: TenantId,
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

/// §19 Provider Admission Controller input this adapter has no real config source for yet: the
/// DB-backed `control.retrieval_provider_admission_limits` loader for [`admission::
/// AdmissionBudgets`]'s Global/Region tiers (`admission` module doc: "a later adapters-layer
/// task") does not exist, and neither does a per-tenant plan lookup. Every tier ceiling here is
/// `u64::MAX` (never binds) and the plan is fully permissive, so [`admission::decide`] still
/// runs for real on every call (§19 Provider Plane Architecture Gate check 6/7) — only the
/// *thresholds* are a placeholder.
/// // ponytail: unconstrained budgets/plan; replace with the real DB-backed loader once it
/// // lands, following the same shape `model_call_ledger::load_pricing_versions` already
/// // establishes for the sibling pricing-registry gap.
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
/// onto the caller-facing [`ErrorCode`]. See [`unconstrained_admission_state`]'s doc for why
/// the thresholds behind it are a placeholder while the call itself is not.
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
    transport: HttpExternalCall,
    pool: RetrievalWorkerDbPool,
    processor: ProcessorId,
    model: EmbeddingModelDescriptor,
    region: String,
    source: DisclosureSource,
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
            transport,
            pool,
            processor,
            model,
            region: region.into(),
            source,
        })
    }

    /// The full §7.3/§7.4/§19 sequence for one real call: pre-flight validate → admission
    /// check → mint `EgressPermit` → reserve the `ModelCallLedger` row → reserve the
    /// disclosure row → `HttpExternalCall::call` → finalize the disclosure row (always, on
    /// every branch — an unfinalized row past 60s is exactly what §53 INV-3 watches for) →
    /// parse+validate the response → record the §41.2 metrics → finalize the `ModelCallLedger`
    /// row (via [`Self::record_call_outcome`]).
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
        texts: &[&str],
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

        let body = DashscopeEmbeddingRequest {
            model: &self.model.model_id.0,
            input: texts,
            dimensions: dimension,
            encoding_format: "float",
        };
        let bytes = serde_json::to_vec(&body).map_err(|_| ErrorCode::Internal)?;
        let payload = AuthorizedEgressPayload::new(bytes);

        // §7.5.1: `authorize` itself refuses SECRET_MATERIAL for this purpose — DataClass::Private
        // is this adapter's only legal grade (sealed retrieval text is never SECRET_MATERIAL by
        // construction, §7.5's own line).
        let permit = egress::authorize(
            tenant_id,
            self.processor,
            PrivateDataPurpose::RetrievalEmbedding,
            DataClass::Private,
            &payload,
            EGRESS_PERMIT_TTL,
        )?;

        // §19.1 ModelCallLedger reserve() — before the network call, same "identity + estimate
        // columns first" shape `disclosure::reserve_retrieval` below already uses.
        let reservation = model_call_ledger::reserve_call(
            &self.pool,
            &model_call_ledger::ReserveCall {
                request_id: None,
                tenant_id: tenant_id.0,
                workspace_id: None,
                purpose: Some("embedding".to_string()),
                provider: "dashscope".to_string(),
                model: Some(self.model.model_id.0.clone()),
                model_revision: Some(self.model.model_revision.clone()),
                // Pricing registry (`cost::compute_cost` against a DB-loaded
                // `pricing::PricingVersion`) is not wired to this call site yet — same "later
                // task's deliverable" gap `model_call_ledger.rs`'s own module doc names.
                estimated_cost: None,
            },
        )
        .await
        .map_err(|_| ErrorCode::Internal)?;

        let disclosure_id = disclosure::reserve_retrieval(
            &self.pool,
            &permit,
            &self.region,
            &payload,
            None,
            std::slice::from_ref(&self.source),
        )
        .await
        .map_err(|_| ErrorCode::Internal)?;

        let region_label = metrics_region(&self.region);
        let (response_bytes, latency) = self
            .call_and_finalize_disclosure(
                tenant_id,
                &permit,
                &payload,
                disclosure_id,
                reservation.model_call_id,
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
            reservation.model_call_id,
            region_label,
            "ok",
            metric_tokens,
            latency,
            ledger_outcome,
            ledger_error_class,
        )
        .await;

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
        region_label: metrics::Region,
        estimated_input_tokens: u64,
    ) -> Result<(Vec<u8>, Duration), ErrorCode> {
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
                // Neither case ever reached the provider: `Internal` is a missing/malformed
                // platform credential (`HttpExternalCall`'s own credential-resolution/header-
                // build failure), `Forbidden` is a permit/processor/payload-digest/purpose
                // policy denial (§7.3) — both fail before any request leaves this process. The
                // old `_ => "http_5xx"` catch-all folded them into §41.2's frozen provider-call
                // result set, making a missing `DASHSCOPE_API_KEY` indistinguishable from a
                // real DashScope 500 on the dashboard. Close the already-reserved ledger row
                // (§53 INV-3 obligation stands regardless of *why* the call failed) without
                // emitting a `retrieval_provider_requests_total` sample for a call that was
                // never actually a provider call.
                self.finalize_ledger_only(tenant_id, model_call_id, latency, err)
                    .await;
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
                    region_label,
                    result_label,
                    estimated_input_tokens,
                    latency,
                    model_call_ledger::ModelCallOutcome::Failed,
                    Some(format!("{err:?}")),
                )
                .await;
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
    ) {
        let latency_ms = i32::try_from(latency.as_millis()).unwrap_or(i32::MAX);
        let _ = model_call_ledger::finalize_call(
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
        .await;
    }

    /// §41.2 metrics + §19.1 `ModelCallLedger.finalize()` for one completed call — the shared
    /// tail both [`Self::embed`] exit branches (transport failure, and transport success
    /// regardless of whether the response then parsed/validated) run.
    #[allow(clippy::too_many_arguments)]
    async fn record_call_outcome(
        &self,
        tenant_id: TenantId,
        model_call_id: uuid::Uuid,
        region_label: metrics::Region,
        result_label: &'static str,
        tokens: u64,
        latency: Duration,
        outcome: model_call_ledger::ModelCallOutcome,
        error_class: Option<String>,
    ) {
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
        let _ = model_call_ledger::finalize_call(
            &self.pool,
            tenant_id.0,
            model_call_id,
            outcome,
            &model_call_ledger::FinalizeCall {
                input_tokens: Some(tokens as i64),
                latency_ms: Some(latency_ms),
                error_class,
                ..Default::default()
            },
        )
        .await;
    }
}

#[async_trait::async_trait]
impl EmbeddingProvider for DashscopeEmbeddingProvider {
    fn model(&self) -> &EmbeddingModelDescriptor {
        &self.model
    }

    async fn embed_queries(
        &self,
        tenant_id: TenantId,
        dimension: u32,
        queries: &[SealedRetrievalQuery],
    ) -> Result<EmbeddingBatch, ErrorCode> {
        let texts: Vec<&str> = queries.iter().map(SealedRetrievalQuery::as_str).collect();
        self.embed(tenant_id, dimension, &texts).await
    }

    async fn embed_cards(
        &self,
        tenant_id: TenantId,
        dimension: u32,
        cards: &[SealedRetrievalCard],
    ) -> Result<EmbeddingBatch, ErrorCode> {
        let texts: Vec<&str> = cards.iter().map(SealedRetrievalCard::as_str).collect();
        self.embed(tenant_id, dimension, &texts).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
