//! `adapters::byok` — T4.4/T4.5: `UserReasoningProvider` contract, `PrivateInferenceContext`,
//!   `UserReasoningProfile`/`PrivateReasoningDomain` DB-facing types, the §11.3 Provider Error state machine, and the
//!   §11.4 custom-endpoint SSRF guard chain.
//! Depends-on: crates=[async-trait, humaux-domain, humaux-infra-egress, serde_json, tokio, uuid]; services=[]; env=[CARGO_MANIFEST_DIR]; modules=[adapters::byok::ssrf, domain::egress, domain::error, domain::evidence, domain::ids, infra-egress::raw, infra-egress::resolver]
//! Called-by: [adapters::consolidation_reasoner, adapters::contribution_reasoner, adapters::distill_reasoner, gateway::mcp_application, private-worker::distill, private-worker::inference_rpc, private-worker::main, tests]
//! Invariants: [the plaintext BYOK key never becomes a struct field and never prints (only CredentialFingerprint
//!   does); permit tenant/purpose/payload mismatches are refused before any provider call; provider failures surface
//!   as typed ReasoningProviderError]
//! Spec: Baseline §11.1; §48.0; §83.4; §11.3; §4.2; ADR-0058
//!
//! ## Plaintext key discipline (§11.1, this Phase's security red line)
//!
//! The plaintext BYOK API key **never becomes a field on any struct in this module** — not
//! [`PrivateInferenceContext`], not [`ReasoningProviderDescriptor`], nothing. It exists only as
//! a local, non-`Clone` [`PlaintextApiKey`] value inside the one function that builds an
//! outbound HTTP request ([`OpenAiCompatibleProvider::build_openai_request`]) and is dropped at the
//! end of that call. [`PlaintextApiKey`]'s own `Debug`/`Display` never print the secret — see
//! its doc — so even a stray `{:?}` on a value that (incorrectly) captured one cannot leak it.
//! Every place that would otherwise want to reference "which key" (logs, error messages,
//! `PrivateInferenceContext`) uses [`CredentialFingerprint`] instead, which is a one-way digest
//! (`humaux_domain::evidence::payload_sha256`, §48.0① — reused rather than adding a second
//! SHA-256 call site) truncated to 8 hex chars, never reversible to the key.
//!
//! ## Pending wiring (interface-only in this task, real backends land elsewhere)
//!
//! Three externally-facing concerns are injected via trait rather than implemented here,
//! because their concrete backend is not this task's file scope and — for two of them — is
//! still a stub as of this writing:
//!
//! - [`CredentialDecryptor`]: OpenBao decrypt (`crates/adapters/src/openbao.rs` is still a
//!   T0.x placeholder). The real impl decrypts a [`CredentialRef`] to a [`PlaintextApiKey`]
//!   right before the HTTP call and nowhere else (§11.1).
//! - [`OpenAiCompatTransport`]: the actual network send. `crates/infra-egress/src/http.rs` now
//!   exists (§83.4 G80-3's sole `reqwest::Client` construction site) but its
//!   `humaux_domain::egress::ExternalCall` contract is deliberately coarse — `Result<Vec<u8>,
//!   ErrorCode>`, collapsing every non-2xx response into `ErrorCode::ProviderPermanent` with no
//!   status code, headers, or `Retry-After` value reaching the caller. §11.3's state machine
//!   needs exactly that HTTP-level detail (401 vs 403 vs 429-with-Retry-After vs 5xx are four
//!   different outcomes, not one), so this module defines its own narrower transport contract
//!   instead of squeezing through `ExternalCall`'s current shape. A real
//!   [`OpenAiCompatTransport`] impl still MUST go through `crates/infra-egress/src/http.rs` for
//!   its actual socket — constructing `reqwest::Client`/`hyper::Client` anywhere else is a hard
//!   `architecture-check` failure (§83.4 G80-3 判据1) — either by widening `ExternalCall`'s
//!   return shape (a later task, out of this file's scope to decide) or by adding a sibling
//!   trait in `infra-egress` that shares its one `reqwest::Client`. This module builds no
//!   `reqwest`/`hyper` client of its own.
//! - Retry backoff sleep: [`OpenAiCompatibleProvider`]'s retry loop calls
//!   `std::thread::sleep` directly rather than an async timer. ponytail: acceptable for a
//!   background worker process (never a request-serving hot path — `humaux-private-worker`,
//!   §4.2), not acceptable if this code is ever reused on a request path; upgrade to
//!   `tokio::time::sleep` (making `tokio` a real, not dev-only, dependency of this crate) if
//!   that ever happens.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use uuid::Uuid;

use humaux_domain::egress::{AuthorizedEgressPayload, EgressPermit, PrivateDataPurpose};
use humaux_domain::error::ErrorCode;
use humaux_domain::evidence::payload_sha256;
use humaux_domain::ids::{TenantId, UserId};

pub mod ssrf;

// =============================================================================
// §11.2 capability closed set
// =============================================================================

/// §11.2 "能力至少" closed set. `==` the capability CHECKs of `control.user_reasoning_profiles`,
/// `control.processor_models` and `control.reasoning_profiles` (last widened by
/// `migrations/0195_reasoning_capabilities_tool_calls.sql`) — [`ReasoningCapability::as_str`] /
/// [`ReasoningCapability::parse`] are the §78.2 contract-test surface for those columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReasoningCapability {
    Text,
    Vision,
    StructuredOutput,
    TokenUsage,
    /// ADR-0058 D-M: the endpoint answers through one declared `tools` function (`tool_calls`).
    ToolCalls,
    /// ADR-0058 D-M: the endpoint accepts `"reasoning_split": true` and keeps its reasoning out of
    /// the answer.
    ReasoningSplit,
}

impl ReasoningCapability {
    /// Every variant, in the order of the 0195 CHECK arrays.
    pub const ALL: [ReasoningCapability; 6] = [
        ReasoningCapability::Text,
        ReasoningCapability::Vision,
        ReasoningCapability::StructuredOutput,
        ReasoningCapability::TokenUsage,
        ReasoningCapability::ToolCalls,
        ReasoningCapability::ReasoningSplit,
    ];

    /// The wire / DB value.
    pub const fn as_str(self) -> &'static str {
        match self {
            ReasoningCapability::Text => "TEXT",
            ReasoningCapability::Vision => "VISION",
            ReasoningCapability::StructuredOutput => "STRUCTURED_OUTPUT",
            ReasoningCapability::TokenUsage => "TOKEN_USAGE",
            ReasoningCapability::ToolCalls => "TOOL_CALLS",
            ReasoningCapability::ReasoningSplit => "REASONING_SPLIT",
        }
    }

    /// Inverse of [`Self::as_str`]; `None` for any value outside the closed set.
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.as_str() == s)
    }
}

impl fmt::Display for ReasoningCapability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// §11.1 `ReasoningProviderDescriptor` — capability/identity metadata a
/// [`UserReasoningProvider`] exposes about itself, consulted *before* any external call so an
/// unsupported-capability request can fail without spending a network round trip (§11.3
/// "unsupported VISION / STRUCTURED_OUTPUT -> fail before external call").
#[derive(Debug, Clone)]
pub struct ReasoningProviderDescriptor {
    pub provider_id: String,
    pub model_id: String,
    /// Frozen revision configured on this provider instance; `None` is an exact identity value.
    pub model_revision: Option<String>,
    pub capabilities: Vec<ReasoningCapability>,
    /// §11.4: `Some` only for a user-supplied custom/OpenAI-compatible `base_url`. Informational
    /// only — the actual SSRF choke point is [`OpenAiCompatibleProvider::new`], which calls
    /// [`ssrf::validate_custom_endpoint`] on the real `base_url` a provider will send to and
    /// refuses to construct on failure, so an unvalidated endpoint cannot become a live
    /// provider regardless of what this field claims.
    pub custom_endpoint: Option<String>,
}

impl ReasoningProviderDescriptor {
    /// §11.3 "unsupported VISION / STRUCTURED_OUTPUT -> fail before external call": the sole
    /// pre-flight capability gate every [`UserReasoningProvider`] method must run before
    /// building a request.
    pub fn require_capability(
        &self,
        needed: ReasoningCapability,
    ) -> Result<(), ReasoningProviderError> {
        if self.capabilities.contains(&needed) {
            Ok(())
        } else {
            Err(ReasoningProviderError::UnsupportedCapability(needed))
        }
    }
}

// =============================================================================
// CredentialRef / CredentialFingerprint / PlaintextApiKey (§11.1 plaintext-key discipline)
// =============================================================================

/// Pointer to `control.credentials` (a CredentialRef *locator*, §3's comment on that table —
/// never the secret bytes). `humaux_domain` does not yet define a canonical `CredentialRef`
/// type (out of this task's file scope: `crates/domain/src/egress.rs` T4.1 does not need one,
/// and no other domain module claims it) — this is the adapter-local shape §11.1's own text
/// names verbatim ("`PrivateInferenceContext` 只持: ... CredentialRef ..."), matching the
/// `RememberError`-style "adapter boundary, not Domain" precedent (`crates/adapters/src/
/// remember.rs`'s doc comment on why its error type is not `humaux_domain::error::ErrorCode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CredentialRef {
    credential_id: Uuid,
}

impl CredentialRef {
    pub fn new(credential_id: Uuid) -> Self {
        Self { credential_id }
    }

    pub fn credential_id(&self) -> Uuid {
        self.credential_id
    }
}

/// One-way digest of a plaintext API key, safe to log/store (§11.1 "日志只留 credential
/// fingerprint"). Reuses [`payload_sha256`] (§48.0① G80-22's sole `EvidencePayloadSha256`
/// construction point) rather than adding a second SHA-256 call site to this crate — the
/// fingerprint is a legitimate consumer of a content digest, just truncated for display.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CredentialFingerprint([u8; 4]);

impl CredentialFingerprint {
    /// Computed from the plaintext key's own bytes; never stores or returns them.
    pub fn of_plaintext(key: &PlaintextApiKey) -> Self {
        let digest = payload_sha256(key.0.as_bytes());
        let hex = digest.to_hex();
        let mut out = [0u8; 4];
        // First 4 bytes (8 hex chars) of the digest — plenty to distinguish "which key was
        // used" in a log line without meaningfully narrowing a brute-force search space back
        // toward the original key (this is a fingerprint for operators, not a password hash).
        for (i, chunk) in hex.as_bytes().chunks(2).take(4).enumerate() {
            out[i] = u8::from_str_radix(std::str::from_utf8(chunk).unwrap(), 16).unwrap_or(0);
        }
        Self(out)
    }

    pub fn to_hex(self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }
}

impl fmt::Display for CredentialFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fp:{}", self.to_hex())
    }
}

/// A decrypted BYOK API key. §11.1's hard line lives here: this type is deliberately **not**
/// `Clone`/`Copy` (no accidental fan-out of the secret to a second owner) and its `Debug`/
/// `Display` never print `0` — only [`CredentialFingerprint`] is safe to log. The only
/// sanctioned reader is [`PlaintextApiKey::expose`], named the way `secrecy`-style crates name
/// their equivalent method so a `grep`/audit for "who reads the raw key" has exactly one hit
/// per call site.
pub struct PlaintextApiKey(String);

impl PlaintextApiKey {
    pub fn new(key: String) -> Self {
        Self(key)
    }

    /// The only way to read the raw key — named so a caller cannot pretend this is an
    /// ordinary field access. Callers must not store the returned `&str` beyond the immediate
    /// HTTP request build (§11.1).
    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn fingerprint(&self) -> CredentialFingerprint {
        CredentialFingerprint::of_plaintext(self)
    }
}

impl fmt::Debug for PlaintextApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PlaintextApiKey({})", self.fingerprint())
    }
}

impl fmt::Display for PlaintextApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.fingerprint())
    }
}

// =============================================================================
// §11.2.1 PrivateReasoningDomain identity + ingress rule
// =============================================================================

/// `control.private_reasoning_domains.reasoning_domain_id` (mint/parse shape hand-matches
/// `domain::ids::uuid_newtype!` — that macro is private to `ids.rs` and this id is not among
/// the frozen seven §59 ids, same deviation `domain::identity::PrincipalId` already takes for
/// the identical reason, per that type's own doc comment).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReasoningDomainId(pub Uuid);

impl ReasoningDomainId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for ReasoningDomainId {
    fn default() -> Self {
        Self::new()
    }
}

/// §11.2.1 Ingress table: who is producing this content decides whether an LLM stage may run
/// at all, independent of `workspace_id`'s (mere) visibility scope.
#[derive(Debug, Clone, Copy)]
pub enum ProcessingPrincipal {
    /// "用户直接输入/上传 -> reasoning_domain = 该用户".
    DirectUser(ReasoningDomainId),
    /// "Agent on-behalf-of 用户 -> reasoning_domain = Grant 明确绑定的用户域" — the caller
    /// must already have resolved and validated the `reasoning_domain_grants` row (workspace
    /// scope / purpose / not expired / not revoked) before constructing this variant; this
    /// type only carries the *result* of that lookup, it does not perform it.
    AgentOnBehalfOf(ReasoningDomainId),
    /// "无法确定 processing principal -> deterministic-only；不得排队 LLM 蒸馏".
    Unknown,
}

/// §11.2.1 Ingress rule, made a total function so "no principal -> no LLM" cannot be
/// forgotten at a call site: `None` means "persist Evidence via the deterministic path only,
/// never enqueue an LLM stage for it" — there is no other way to read a `None` out of this
/// function.
pub fn reasoning_domain_for_ingress(principal: ProcessingPrincipal) -> Option<ReasoningDomainId> {
    match principal {
        ProcessingPrincipal::DirectUser(id) | ProcessingPrincipal::AgentOnBehalfOf(id) => Some(id),
        ProcessingPrincipal::Unknown => None,
    }
}

/// §11.2.1 "一次 LLM Memory/Consolidation 的输入必须全部属于同一个 reasoning domain" —
/// "禁止把 A+B 私人内容放进一个 prompt 再挑一个人的 Key 处理". `domains` must be non-empty
/// (an LLM call with zero inputs is not this function's concern — the caller should not have
/// reached here at all in that case).
pub fn require_single_reasoning_domain(
    domains: &[ReasoningDomainId],
) -> Result<ReasoningDomainId, ReasoningProviderError> {
    match domains.split_first() {
        None => Err(ReasoningProviderError::NoProcessingPrincipal),
        Some((first, rest)) => {
            if rest.iter().all(|d| d == first) {
                Ok(*first)
            } else {
                Err(ReasoningProviderError::MixedReasoningDomain)
            }
        }
    }
}

// =============================================================================
// §11.1 PrivateInferenceContext
// =============================================================================

/// §11.1: "`PrivateInferenceContext` 只持: tenant/user/scope, CredentialRef, EgressPermit,
/// provider/model/profile version, trace/request id". Every field is private — the plaintext
/// key is not among them (see module doc); the only way to build one is [`Self::new`], which
/// enforces the two structural invariants a `PrivateInferenceContext` must never violate.
#[derive(Debug)]
pub struct PrivateInferenceContext {
    tenant_id: TenantId,
    user_id: UserId,
    reasoning_domain_id: ReasoningDomainId,
    credential_ref: CredentialRef,
    egress_permit: EgressPermit,
    provider_id: String,
    model_id: String,
    profile_version: i64,
    trace_id: String,
}

/// Why [`PrivateInferenceContext::new`] can fail: both are structural mismatches between the
/// permit handed in and the context it is meant to authorize, never a provider/network error
/// (those are [`ReasoningProviderError`]'s concern once a call is actually attempted).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InferenceContextError {
    /// The `EgressPermit` was minted for a different tenant than this context's `tenant_id`.
    PermitTenantMismatch,
    /// The `EgressPermit` was minted for a `PrivateDataPurpose` other than `UserReasoning`
    /// (e.g. `RetrievalEmbedding`) — §7's Trust Domain separation means a retrieval-purposed
    /// permit must never authorize a USER_REASONING call.
    PermitWrongPurpose,
}

impl fmt::Display for InferenceContextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PermitTenantMismatch => write!(f, "EgressPermit tenant does not match context"),
            Self::PermitWrongPurpose => {
                write!(f, "EgressPermit purpose is not UserReasoning")
            }
        }
    }
}

impl PrivateInferenceContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: TenantId,
        user_id: UserId,
        reasoning_domain_id: ReasoningDomainId,
        credential_ref: CredentialRef,
        egress_permit: EgressPermit,
        provider_id: impl Into<String>,
        model_id: impl Into<String>,
        profile_version: i64,
        trace_id: impl Into<String>,
    ) -> Result<Self, InferenceContextError> {
        if egress_permit.tenant_id() != tenant_id {
            return Err(InferenceContextError::PermitTenantMismatch);
        }
        if egress_permit.purpose() != PrivateDataPurpose::UserReasoning {
            return Err(InferenceContextError::PermitWrongPurpose);
        }
        Ok(Self {
            tenant_id,
            user_id,
            reasoning_domain_id,
            credential_ref,
            egress_permit,
            provider_id: provider_id.into(),
            model_id: model_id.into(),
            profile_version,
            trace_id: trace_id.into(),
        })
    }

    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    pub fn user_id(&self) -> UserId {
        self.user_id
    }

    pub fn reasoning_domain_id(&self) -> ReasoningDomainId {
        self.reasoning_domain_id
    }

    pub fn credential_ref(&self) -> CredentialRef {
        self.credential_ref
    }

    pub fn egress_permit(&self) -> &EgressPermit {
        &self.egress_permit
    }

    pub fn provider_id(&self) -> &str {
        &self.provider_id
    }

    pub fn model_id(&self) -> &str {
        &self.model_id
    }

    pub fn profile_version(&self) -> i64 {
        self.profile_version
    }

    pub fn trace_id(&self) -> &str {
        &self.trace_id
    }
}

// =============================================================================
// Request/response shapes
// =============================================================================

#[derive(Debug, Clone, Default)]
pub struct TokenUsage {
    /// `usage.prompt_tokens`。
    pub input_tokens: Option<u64>,
    /// `usage.completion_tokens`。
    pub output_tokens: Option<u64>,
    /// `usage.completion_tokens_details.reasoning_tokens`（推理模型吃掉的那部分——
    /// M3 实测：`max_tokens` 太小时 reasoning 吃光预算，answer 为空而 HTTP 200）。
    pub reasoning_tokens: Option<u64>,
    /// `usage.prompt_tokens_details.cached_tokens`（DOD-059 cache-hit 记账的通道开口；
    /// 目前只上报不持久化，ledger 侧的列是独立 EXPAND 任务）。
    pub cached_input_tokens: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct StructuredReasoningRequest {
    pub system_prompt: String,
    pub user_prompt: String,
    /// JSON Schema the response must conform to. ponytail: this module checks only that the
    /// response parses as JSON, not full schema conformance (no `jsonschema`-style crate is a
    /// dependency of this workspace yet) — upgrade path: validate against this field once the
    /// Observation Candidate schema is finalized and a validator dependency is justified.
    pub json_schema: String,
    pub max_output_tokens: u32,
    /// Where the provider is asked to put the structured answer (ADR-0058 D-M).
    pub output: OutputChannel,
}

/// The channel a structured answer travels on (ADR-0058 D-M).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputChannel {
    /// `choices[0].message.content`, `<think>` blocks stripped — the v1 wire body, byte-identical.
    Content,
    /// Exactly one call of this side-effect-free function whose `parameters` are
    /// [`StructuredReasoningRequest::json_schema`]; its `arguments` string is the answer. Only for a
    /// descriptor that declares [`ReasoningCapability::ToolCalls`] (ADR-0058 D-M: the channel is
    /// chosen from capabilities, never from a provider name). No `tool_choice` is sent: measured
    /// live (ADR-0058 W1) a named-function `tool_choice` was accepted but not honoured. The
    /// arguments still go through the caller's fail-closed parser — the tool call is a transport,
    /// never the validation.
    Tool(&'static str),
}

#[derive(Debug, Clone)]
pub struct StructuredReasoningResponse {
    pub json: String,
    pub usage: TokenUsage,
    /// ADR-0058 R9: on [`OutputChannel::Tool`] the reply carried no tool call and `json` is its
    /// `content` object instead. Always `false` on [`OutputChannel::Content`].
    pub channel_fallback: bool,
}

#[derive(Debug, Clone)]
pub struct VisionReasoningRequest {
    pub prompt: String,
    /// Pointers into `private.artifacts`, never raw image bytes inline on this struct — the
    /// provider adapter resolves them to bytes only at the point it builds the outbound
    /// request body.
    pub artifact_refs: Vec<Uuid>,
    pub max_output_tokens: u32,
}

#[derive(Debug, Clone)]
pub struct VisionReasoningResponse {
    pub text: String,
    pub usage: TokenUsage,
}

// =============================================================================
// §11.3 Provider Error Semantics
// =============================================================================

/// §11.3's full state machine, as an adapter-local error type — same "adapter boundary, not
/// `humaux_domain::error::ErrorCode`" shape as `RememberError`/`OutboxError`/`JobsError`
/// (`crates/adapters/src/remember.rs`'s doc comment). `ErrorCode` already has `WaitingKey` /
/// `ProviderPermanent` / `ProviderTransient` / `ProviderRateLimited` variants (§52.1) for the
/// terminal wire-facing shape; mapping this richer type down to those (with the
/// `Retry-After`/attempt-count detail necessarily dropped) is `crates/protocol/src/
/// error_map.rs`'s job, out of this task's scope — same division of labor `RememberError`
/// already documents.
#[derive(Debug)]
pub enum ReasoningProviderError {
    /// 401 / invalid credential (§11.3). Caller must not increment retry_count and must not
    /// enqueue to DLQ — enforced by construction here: this variant carries no retry
    /// information at all, so there is nothing for a caller to increment even by accident.
    WaitingKey {
        fingerprint: Option<CredentialFingerprint>,
    },
    /// 403 / provider policy denied (§11.3).
    ProviderPermanent { message: String },
    /// 429 (honor `Retry-After` when present) or 5xx/timeout (bounded transient retry +
    /// jitter is the caller's job — this variant only reports what the provider said).
    RetryWait { retry_after: Option<Duration> },
    /// Bounded same-provider schema repair budget exhausted (§11.3). Never produced by
    /// switching to a different `UserReasoningProvider` — see
    /// [`complete_structured_with_bounded_repair`]'s doc for why that is structurally true,
    /// not just a convention.
    FailedOutputSchema { attempts: u32 },
    /// §11.3 "unsupported VISION / STRUCTURED_OUTPUT -> fail before external call".
    UnsupportedCapability(ReasoningCapability),
    /// §11.2.1: input Evidence for one LLM call spans more than one reasoning domain.
    MixedReasoningDomain,
    /// §11.2.1: no processing principal could be determined for this content at all —
    /// deterministic-only, never reaches this module's LLM call path in the first place; kept
    /// here as the error [`require_single_reasoning_domain`] returns for an empty input list,
    /// which is a caller bug (see that function's doc), not a real ingress event.
    NoProcessingPrincipal,
    /// §11.4: the custom endpoint failed SSRF validation — fails before any external call,
    /// same "fail-fast" shape as `UnsupportedCapability`.
    EndpointRejected(ssrf::SsrfError),
    /// §7.3: `ctx`'s `EgressPermit` had already expired when this call attempted to use it —
    /// a stale reservation must never be honored (`EgressPermit::is_expired`).
    EgressPermitExpired,
    /// §7.3 "Permit 不能被拿去发送另一份正文": the outbound bytes this call is actually about
    /// to send do not hash to the digest the `EgressPermit` was minted for.
    EgressPermitPayloadMismatch,
    /// Transport-layer failure not covered by the above (connection reset mid-stream, TLS
    /// error, etc.) — surfaced by [`OpenAiCompatTransport`] impls for anything that isn't a
    /// clean HTTP status response.
    Transport(String),
}

impl ReasoningProviderError {
    /// The class of [`Self::WaitingKey`] (§11: known blocked — a worker parks the job and never
    /// counts the call toward DEAD, ADR-0058 D-F/D-H).
    pub const WAITING_KEY_CLASS: &'static str = "WAITING_KEY";
    /// The class of [`Self::FailedOutputSchema`]: a worker spends its malformed re-ask budget on it
    /// exactly as on a reply its own parser refused (ADR-0048 D-D, ADR-0058 D-M).
    pub const FAILED_OUTPUT_SCHEMA_CLASS: &'static str = "FAILED_OUTPUT_SCHEMA";

    /// ADR-0058 D-N: one static class per variant (closed set; never provider or payload text) — what
    /// a worker logs and stores as `last_error_class`, so an operator can tell a 401 from a timeout
    /// from a refusal without a SQL session.
    pub const fn class(&self) -> &'static str {
        match self {
            Self::WaitingKey { .. } => Self::WAITING_KEY_CLASS,
            Self::ProviderPermanent { .. } => "PROVIDER_PERMANENT",
            Self::RetryWait { .. } => "RETRY_WAIT",
            Self::FailedOutputSchema { .. } => Self::FAILED_OUTPUT_SCHEMA_CLASS,
            Self::UnsupportedCapability(_) => "UNSUPPORTED_CAPABILITY",
            Self::MixedReasoningDomain => "MIXED_REASONING_DOMAIN",
            Self::NoProcessingPrincipal => "NO_PROCESSING_PRINCIPAL",
            Self::EndpointRejected(_) => "ENDPOINT_REJECTED",
            Self::EgressPermitExpired => "EGRESS_PERMIT_EXPIRED",
            Self::EgressPermitPayloadMismatch => "EGRESS_PERMIT_PAYLOAD_MISMATCH",
            Self::Transport(_) => "TRANSPORT",
        }
    }
}

impl fmt::Display for ReasoningProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WaitingKey { fingerprint } => match fingerprint {
                Some(fp) => write!(f, "WAITING_KEY ({fp})"),
                None => write!(f, "WAITING_KEY"),
            },
            Self::ProviderPermanent { message } => write!(f, "PROVIDER_PERMANENT: {message}"),
            Self::RetryWait { retry_after } => match retry_after {
                Some(d) => write!(f, "RETRY_WAIT (retry-after {d:?})"),
                None => write!(f, "RETRY_WAIT"),
            },
            Self::FailedOutputSchema { attempts } => {
                write!(f, "FAILED_OUTPUT_SCHEMA (after {attempts} attempt(s))")
            }
            Self::UnsupportedCapability(cap) => write!(f, "unsupported capability: {cap}"),
            Self::MixedReasoningDomain => {
                write!(f, "§11.2.1: mixed reasoning_domain in one LLM input")
            }
            Self::NoProcessingPrincipal => {
                write!(f, "§11.2.1: no processing principal (deterministic-only)")
            }
            Self::EndpointRejected(e) => write!(f, "custom endpoint rejected: {e}"),
            Self::EgressPermitExpired => write!(f, "§7.3: egress permit expired before send"),
            Self::EgressPermitPayloadMismatch => {
                write!(f, "§7.3: outbound payload does not match the egress permit")
            }
            Self::Transport(msg) => write!(f, "transport error: {msg}"),
        }
    }
}

impl From<ssrf::SsrfError> for ReasoningProviderError {
    fn from(e: ssrf::SsrfError) -> Self {
        Self::EndpointRejected(e)
    }
}

/// §52's ErrorCode/DegradeCode dichotomy owns the terminal wire shape (§52.1) — this is a
/// best-effort, lossy summary for logging/metrics, not the real `error_map.rs` mapping
/// (out of scope, see the enum's own doc). Only the unambiguous terminal cases map cleanly;
/// `RetryWait`/`FailedOutputSchema`/domain-binding errors have no single matching wire code
/// yet and fall to `Internal`.
impl ReasoningProviderError {
    pub fn as_error_code(&self) -> ErrorCode {
        match self {
            Self::WaitingKey { .. } => ErrorCode::WaitingKey,
            Self::ProviderPermanent { .. } => ErrorCode::ProviderPermanent,
            Self::RetryWait { .. } => ErrorCode::ProviderTransient,
            Self::UnsupportedCapability(_) => ErrorCode::InvalidInput,
            // §7.3's own trait doc: "reject with ErrorCode::Forbidden on mismatch" — the
            // caller held an EgressPermit but it did not authorize this send.
            Self::EgressPermitExpired | Self::EgressPermitPayloadMismatch => ErrorCode::Forbidden,
            _ => ErrorCode::Internal,
        }
    }
}

/// §11.3's HTTP-status half of the state machine, as a pure function so it is unit-testable
/// without any network or trait object at all. `retry_after` is only consulted for 429.
pub fn classify_http_status(
    status: u16,
    retry_after: Option<Duration>,
) -> Option<ReasoningProviderError> {
    match status {
        200..=299 => None,
        401 => Some(ReasoningProviderError::WaitingKey { fingerprint: None }),
        403 => Some(ReasoningProviderError::ProviderPermanent {
            message: format!("provider returned {status} (policy denied)"),
        }),
        // §11.3 groups "timeout" with 5xx as transient, not permanent — 408 Request Timeout
        // and 425 Too Early are provider-side timing signals, not policy denials.
        408 | 425 => Some(ReasoningProviderError::RetryWait { retry_after }),
        429 => Some(ReasoningProviderError::RetryWait { retry_after }),
        500..=599 => Some(ReasoningProviderError::RetryWait { retry_after: None }),
        other => Some(ReasoningProviderError::ProviderPermanent {
            message: format!("unexpected status {other}"),
        }),
    }
}

// =============================================================================
// §11.1 UserReasoningProvider contract
// =============================================================================

#[async_trait::async_trait]
pub trait UserReasoningProvider: Send + Sync {
    fn descriptor(&self) -> &ReasoningProviderDescriptor;

    /// Exact endpoint used by the provider instance for the next HTTP request. Implementations
    /// must return runtime state that drives the send, never optional descriptor metadata.
    fn endpoint_ref(&self) -> &str;

    fn model_revision(&self) -> Option<&str>;

    async fn complete_structured(
        &self,
        ctx: &PrivateInferenceContext,
        request: StructuredReasoningRequest,
    ) -> Result<StructuredReasoningResponse, ReasoningProviderError>;

    async fn analyze_vision(
        &self,
        ctx: &PrivateInferenceContext,
        request: VisionReasoningRequest,
    ) -> Result<VisionReasoningResponse, ReasoningProviderError>;
}

// =============================================================================
// Injection points (pending wiring — see module doc)
// =============================================================================

/// Resolves a [`CredentialRef`] to a decrypted key. The real impl lives behind OpenBao
/// (`crates/adapters/src/openbao.rs`, still a placeholder — pending wiring, see module doc)
/// and must decrypt as close to the HTTP call as possible (§11.1).
#[async_trait::async_trait]
pub trait CredentialDecryptor: Send + Sync {
    async fn resolve(
        &self,
        credential_ref: CredentialRef,
    ) -> Result<PlaintextApiKey, ReasoningProviderError>;
}

/// One outbound HTTP header value. Deliberately **not** `Clone` (no accidental second owner
/// of a value that may be `Authorization: Bearer <plaintext key>`, same rationale as
/// [`PlaintextApiKey`]) and its `Debug` never prints the value — a stray `{req:?}` on the
/// [`OpenAiHttpRequest`] that carries this can never reproduce §11.1's red line again.
/// [`HeaderValue::expose`] is the one sanctioned reader, named like [`PlaintextApiKey::expose`]
/// so a grep for "who reads a raw header value" has exactly one hit per call site.
pub struct HeaderValue(String);

impl HeaderValue {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for HeaderValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HeaderValue(redacted)")
    }
}

/// One outbound OpenAI-compatible HTTP request, already fully built (headers include
/// `Authorization`, added by [`OpenAiCompatibleProvider::build_openai_request`] only — never by a
/// [`OpenAiCompatTransport`] impl, which must not need to know the key at all). Not `Clone`:
/// this struct's `headers` may carry the plaintext BYOK key, and §11.1's fan-out discipline
/// (see [`PlaintextApiKey`]) means there must be exactly one owner between decrypt and send.
#[derive(Debug)]
pub struct OpenAiHttpRequest {
    pub url: String,
    pub headers: Vec<(String, HeaderValue)>,
    pub body: Vec<u8>,
}

/// The HTTP-level detail §11.3 needs and `ExternalCall` does not currently expose (see module
/// doc) — status code and a parsed `Retry-After`, in addition to the body.
#[derive(Debug, Clone)]
pub struct OpenAiHttpOutcome {
    pub status: u16,
    pub retry_after: Option<Duration>,
    pub body: Vec<u8>,
}

/// §11.4's body-size bound is enforced by the transport impl while it is still streaming the
/// response — this trait signature cannot itself observe an in-progress stream, only the
/// finished [`OpenAiHttpOutcome`] — so [`ssrf::CustomEndpointPolicy::max_response_bytes`] is
/// passed in for the impl to honor, not inferred from the (already-complete) `body` field.
#[async_trait::async_trait]
pub trait OpenAiCompatTransport: Send + Sync {
    async fn send(
        &self,
        request: OpenAiHttpRequest,
        policy: &ssrf::CustomEndpointPolicy,
    ) -> Result<OpenAiHttpOutcome, ReasoningProviderError>;
}

// =============================================================================
// Chat envelope 解析（§11.3 的 provider 侧错误通道在这里翻译）
// =============================================================================

/// `choices[0].message.content` + usage 四字段。
struct ChatEnvelope {
    content: String,
    usage: TokenUsage,
    /// ADR-0058 R9: [`tool_arguments`] took the answer from `content`.
    channel_fallback: bool,
}

/// 解析 OpenAI 形状的 chat envelope，**顺序即语义**：
///
/// 1. 整体不是 JSON ⇒ [`ReasoningProviderError::ProviderPermanent`]（端点坏了，重试无益）。
///    注意这与旧行为不同：旧代码把"body 不是 JSON"判成 `FailedOutputSchema`——那是把
///    端点故障算在模型输出头上，bounded repair 会白白重试一个坏端点。
/// 2. `base_resp` 存在且 `status_code != 0` ⇒ [`classify_base_resp`]，**绝不往下走**。
///    这是部分 provider 的独立错误通道：HTTP 200 + 非零 status_code 是失败（实测：限流走
///    200 + 2062，不走 429）。字段缺席（真 OpenAI 端点）⇒ 无害通过——判据是
///    「存在且非零 = 错」这条**通用规则**，不引 provider-name 分支。
/// 3. 取 `choices[0].message.content`；缺 ⇒ `ProviderPermanent`。
///    **显式忽略 `message.reasoning_content`**：M3 实测 reasoning 可能落在这个独立字段，
///    它绝不许漏进结构化输出。
/// 4. 解析 usage 四字段（缺哪个哪个 `None`，不编造）。
///
/// ADR-0058 D-M: on [`OutputChannel::Tool`] step 3 reads the one tool call instead
/// ([`tool_arguments`]); a tool-call reply carries no `content` key at all (measured live, W1).
/// ADR-0058 R9: a reply with no tool call may carry the answer object in `content`.
fn parse_chat_envelope(
    body: &[u8],
    channel: OutputChannel,
) -> Result<ChatEnvelope, ReasoningProviderError> {
    let v: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| ReasoningProviderError::ProviderPermanent {
            message: "malformed envelope: response body is not JSON".to_string(),
        })?;

    if let Some(base) = v.get("base_resp") {
        let code = base.get("status_code").and_then(serde_json::Value::as_i64);
        if let Some(code) = code
            && code != 0
        {
            let msg = base
                .get("status_msg")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            return Err(classify_base_resp(code, msg));
        }
    }

    let choice = v.get("choices").and_then(|c| c.get(0));
    let (content, channel_fallback) = match channel {
        OutputChannel::Content => (
            choice
                .and_then(|c| c.get("message"))
                .and_then(|m| m.get("content"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| ReasoningProviderError::ProviderPermanent {
                    message: "malformed envelope: choices[0].message.content missing".to_string(),
                })?
                .to_string(),
            false,
        ),
        OutputChannel::Tool(name) => tool_arguments(choice, name)?,
    };

    let usage = v.get("usage");
    let get = |path: &[&str]| -> Option<u64> {
        let mut cur = usage?;
        for k in path {
            cur = cur.get(k)?;
        }
        cur.as_u64()
    };
    Ok(ChatEnvelope {
        content,
        channel_fallback,
        usage: TokenUsage {
            input_tokens: get(&["prompt_tokens"]),
            output_tokens: get(&["completion_tokens"]),
            reasoning_tokens: get(&["completion_tokens_details", "reasoning_tokens"]),
            cached_input_tokens: get(&["prompt_tokens_details", "cached_tokens"]),
        },
    })
}

/// ADR-0058 D-M: the `arguments` string of the ONE `function` call named `name` in `choice`
/// (`.1 == false`). A truncated reply (`finish_reason == "length"`), two calls, another name or
/// non-string arguments is the model failing the output contract — `FailedOutputSchema`, which
/// the caller re-asks once like any malformed reply (ADR-0048 D-D). A missing `choices[0]` is an
/// endpoint fault, as on the content channel.
///
/// ADR-0058 R9: a reply with NO tool call whose `content` (reasoning blocks stripped) is a JSON
/// object returns that object (`.1 == true`) — the same payload arriving in the other place, for
/// the caller's one fail-closed parser to accept or refuse. Zero calls with any other content is
/// `FailedOutputSchema`.
fn tool_arguments(
    choice: Option<&serde_json::Value>,
    name: &str,
) -> Result<(String, bool), ReasoningProviderError> {
    use serde_json::Value;
    let schema_failure = || ReasoningProviderError::FailedOutputSchema { attempts: 1 };
    let choice = choice.ok_or_else(|| ReasoningProviderError::ProviderPermanent {
        message: "malformed envelope: choices[0] missing".to_string(),
    })?;
    if choice.get("finish_reason").and_then(Value::as_str) == Some("length") {
        return Err(schema_failure());
    }
    let calls = choice
        .get("message")
        .and_then(|m| m.get("tool_calls"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    if calls.is_empty() {
        let content = choice
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(Value::as_str)
            .map(strip_think_blocks)
            .ok_or_else(schema_failure)?;
        return match serde_json::from_str::<Value>(&content) {
            Ok(Value::Object(_)) => Ok((content, true)),
            _ => Err(schema_failure()),
        };
    }
    let [call] = calls else {
        return Err(schema_failure());
    };
    let function = call.get("function");
    if call.get("type").and_then(Value::as_str) != Some("function")
        || function.and_then(|f| f.get("name")).and_then(Value::as_str) != Some(name)
    {
        return Err(schema_failure());
    }
    function
        .and_then(|f| f.get("arguments"))
        .and_then(Value::as_str)
        .map(|arguments| (arguments.to_owned(), false))
        .ok_or_else(schema_failure)
}

/// `base_resp.status_code`（一种 HTTP 200 内的错误信封）⇒ §11.3 语义。**不加新枚举变体**，闭集复用：
///
/// - `1004`（鉴权失败）⇒ [`ReasoningProviderError::WaitingKey`]——BYOK 域里它与 HTTP 401
///   同语义（§11.3：key 无效不是平台故障，是等新 key）。
/// - `1002 | 1039 | 2062` ⇒ [`ReasoningProviderError::RetryWait`]。**`2062` 是实测的
///   Token Plan 限流码，走 HTTP 200 + 空 content**（2026-07-29 教训）——漏掉它，
///   限流会被判成永久错误，bounded repair 直接放弃。
/// - 其余非零 ⇒ `ProviderPermanent` 带原文，fail-closed。
///
// ponytail: 码表 best-effort（四个码来自实测与既往教训）；对照官方码表校准是升级路径，
// 但闸不依赖码表完备——任何非零码都不可能被当成功。
fn classify_base_resp(status_code: i64, status_msg: &str) -> ReasoningProviderError {
    match status_code {
        1004 => ReasoningProviderError::WaitingKey { fingerprint: None },
        1002 | 1039 | 2062 => ReasoningProviderError::RetryWait { retry_after: None },
        code => ReasoningProviderError::ProviderPermanent {
            message: format!("base_resp {code}: {status_msg}"),
        },
    }
}

/// 剥掉每个 `<think>…</think>` 段；**未闭合的 `<think>` 剥到串尾**——那是 `max_tokens`
/// 太小、reasoning 吃光预算的实测形态（M3：HTTP 200、无任何错误标志、content 只有半截
/// 思考块）。剥后的空串过不了 JSON 校验 ⇒ `FailedOutputSchema`——静默 200 转显式红。
fn strip_think_blocks(content: &str) -> String {
    let mut out = String::with_capacity(content.len());
    let mut rest = content;
    while let Some(start) = rest.find("<think>") {
        out.push_str(&rest[..start]);
        match rest[start..].find("</think>") {
            Some(end_rel) => rest = &rest[start + end_rel + "</think>".len()..],
            None => return out, // 未闭合：剥到串尾
        }
    }
    out.push_str(rest);
    out
}

// =============================================================================
// 生产 transport：经 infra-egress 的 raw 出口
// =============================================================================

/// [`OpenAiCompatTransport`] 的第一个生产实现，底层是
/// `humaux_infra_egress::raw::RawHttpPost`（Layer 1A 的 BYOK 域出口）。
///
/// 为什么不复用 `HttpExternalCall`：那是 PlatformManaged 域（§19）——它自己注
/// `Authorization`、自己按 `status_classifier` 分类（401 = `Unauthorized`）。BYOK 域这
/// 两件事都归本模块：明文 key 的唯一展开点在 [`OpenAiCompatibleProvider`] 的
/// `build_openai_request`，401 的语义是 `WaitingKey`（§11.3）。
pub struct EgressHttpTransport {
    raw: humaux_infra_egress::raw::RawHttpPost,
}

/// ADR-0039：把 §11.4 的判定装进出网 client 的**唯一** DNS resolver。
///
/// 「检查」（[`ssrf::validate_custom_endpoint`]，provider 构造时跑一次）和「拨号」
/// （client 建连时那次解析）在此之前是两次独立的解析，答案可以不同——那就是 DNS
/// rebinding 窗口。本类型让拨号那次解析跑的是**同一个** `DnsResolver` + **同一个**
/// [`ssrf::is_forbidden_ip`]，所以一个骗过了构造期检查、随后改指 `169.254.169.254`
/// 的域名，会在**建连之前**被这里拒掉（不是记一行日志）。
///
/// 判据与 `validate_custom_endpoint` 逐字同源：**任一**解析出的地址落在私网/保留/环回/
/// link-local 段即整体拒——不是「有一个公网的就算过」。
pub struct SsrfCheckedResolver {
    inner: Arc<dyn ssrf::DnsResolver>,
}

impl SsrfCheckedResolver {
    /// `inner` 应当就是调用方交给 [`OpenAiCompatibleProvider::new`] 的那个 resolver
    /// （静态 DNS pin、测试替身，等等）——两处传同一个，检查与拨号才是同一套判定。
    #[must_use]
    pub fn new(inner: Arc<dyn ssrf::DnsResolver>) -> Self {
        Self { inner }
    }
}

impl humaux_infra_egress::resolver::CheckedDnsResolve for SsrfCheckedResolver {
    fn resolve_checked(&self, host: &str) -> Result<Vec<std::net::IpAddr>, String> {
        let addrs = self.inner.resolve(host).map_err(|e| e.to_string())?;
        if addrs.is_empty() {
            return Err(ssrf::SsrfError::NoAddressResolved(host.to_string()).to_string());
        }
        for ip in &addrs {
            if ssrf::is_forbidden_ip(*ip) {
                return Err(ssrf::SsrfError::ResolvedIpForbidden {
                    host: host.to_string(),
                    ip: *ip,
                }
                .to_string());
            }
        }
        Ok(addrs)
    }
}

impl EgressHttpTransport {
    /// 唯一构造：`resolver` 会被 [`SsrfCheckedResolver`] 包起来装成 client 的唯一 DNS
    /// resolver，拨号那次解析跑的就是它。
    ///
    /// 卡 17 的复审发现：曾经还有一个 `new(request_timeout)`，内部硬接
    /// `ssrf::SystemDnsResolver`。它让「拨号用哪个 resolver」与「检查用哪个 resolver」变成
    /// 两个互不相干的实参——生产唯一调用点（`bins/private-worker`）正好把运维配的
    /// `HUMAUX_PRIVATE_WORKER_DNS_PINS` 交给了检查、把系统 DNS 留给了拨号，缺口原样还在，
    /// 而且在**恰恰需要 pin 的那种节点上**拨号会直接被 `is_forbidden_ip` 拒掉。那个构造已
    /// 删除；生产不要直接调本函数，调
    /// [`OpenAiCompatibleProvider::with_egress_transport`]——那里 resolver 只传一次，检查与
    /// 拨号由同一个值派生，分叉在类型上就不可表达。
    ///
    /// # Errors
    /// 底层 client 构造失败 ⇒ [`ReasoningProviderError::Transport`]。
    pub fn with_resolver(
        request_timeout: Duration,
        resolver: Arc<dyn ssrf::DnsResolver>,
    ) -> Result<Self, ReasoningProviderError> {
        humaux_infra_egress::raw::RawHttpPost::new(
            request_timeout,
            Arc::new(SsrfCheckedResolver::new(resolver)),
        )
        .map(|raw| Self { raw })
        .map_err(|e| ReasoningProviderError::Transport(format!("{e:?}")))
    }
}

#[async_trait::async_trait]
impl OpenAiCompatTransport for EgressHttpTransport {
    async fn send(
        &self,
        request: OpenAiHttpRequest,
        policy: &ssrf::CustomEndpointPolicy,
    ) -> Result<OpenAiHttpOutcome, ReasoningProviderError> {
        use humaux_infra_egress::raw::RawSendError;
        // `HeaderValue::expose()` 只在本函数局部展开，随 `headers` 一起在 send 结束后落地
        // ——与 `build_openai_request` 的单点纪律衔接（§11.1）。
        let headers: Vec<(String, String)> = request
            .headers
            .iter()
            .map(|(name, value)| (name.clone(), value.expose().to_string()))
            .collect();
        let outcome = self
            .raw
            .send(
                &request.url,
                &headers,
                request.body,
                usize::try_from(policy.max_response_bytes).unwrap_or(usize::MAX),
            )
            .await
            .map_err(|e| match e {
                // §11.3：超时是 transient——`RetryWait{None}`，不是永久故障。
                RawSendError::Timeout => ReasoningProviderError::RetryWait { retry_after: None },
                RawSendError::NonHttpsEndpoint => {
                    ReasoningProviderError::Transport("non-https endpoint".to_string())
                }
                RawSendError::BodyTooLarge { limit } => ReasoningProviderError::Transport(format!(
                    "response body exceeded {limit} bytes"
                )),
                RawSendError::Network(msg) => ReasoningProviderError::Transport(msg),
                // ADR-0039：出网策略在建连之前拒了 —— 明确说出来，不要混进泛化的网络错误。
                RawSendError::EgressRefused { host, reason } => ReasoningProviderError::Transport(
                    format!("egress policy refused {host}: {reason}"),
                ),
            })?;
        Ok(OpenAiHttpOutcome {
            status: outcome.status,
            retry_after: outcome.retry_after,
            body: outcome.body,
        })
    }
}

// =============================================================================
// Generic OpenAI-compatible provider
// =============================================================================

/// §11.1's generic client: one implementation of [`UserReasoningProvider`] for every
/// OpenAI-compatible endpoint, so `humaux-private-worker` never grows a provider-name
/// `match` (module doc: "Private Worker 不直接维护 provider-name 条件分支").
pub struct OpenAiCompatibleProvider<T: OpenAiCompatTransport, D: CredentialDecryptor> {
    descriptor: ReasoningProviderDescriptor,
    base_url: String,
    transport: T,
    decryptor: D,
    policy: ssrf::CustomEndpointPolicy,
}

impl<T: OpenAiCompatTransport, D: CredentialDecryptor> OpenAiCompatibleProvider<T, D> {
    /// §11.4: `base_url` is SSRF input and is validated **here**, the sole choke point every
    /// custom/OpenAI-compatible endpoint must pass through before a provider using it can even
    /// exist — a provider is never constructed over an endpoint
    /// [`ssrf::validate_custom_endpoint`] rejects. `resolver` is injected (production callers
    /// pass [`ssrf::SystemDnsResolver`]) so this stays unit-testable without real DNS, same
    /// reason the `ssrf` module itself takes a resolver.
    pub fn new(
        descriptor: ReasoningProviderDescriptor,
        base_url: String,
        transport: T,
        decryptor: D,
        policy: ssrf::CustomEndpointPolicy,
        resolver: &dyn ssrf::DnsResolver,
    ) -> Result<Self, ReasoningProviderError> {
        ssrf::validate_custom_endpoint(&base_url, resolver)?;
        Ok(Self {
            descriptor,
            base_url,
            transport,
            decryptor,
            policy,
        })
    }

    /// Builds the outbound request, decrypting the key in this function only (§11.1) — the
    /// returned [`OpenAiHttpRequest`] carries the `Authorization` header already set; the
    /// [`PlaintextApiKey`] local goes out of scope at the end of this call and is never
    /// returned, stored, or logged (only its `Display`/`Debug`, which print a fingerprint —
    /// see [`PlaintextApiKey`] — would appear in a log, and nothing here logs it at all). The
    /// returned struct's own `Debug` never prints the header value either (see [`HeaderValue`]).
    async fn build_openai_request(
        &self,
        ctx: &PrivateInferenceContext,
        body: Vec<u8>,
    ) -> Result<OpenAiHttpRequest, ReasoningProviderError> {
        let key = self.decryptor.resolve(ctx.credential_ref()).await?;
        Ok(OpenAiHttpRequest {
            url: self.base_url.clone(),
            headers: vec![
                (
                    "Authorization".to_string(),
                    HeaderValue::new(format!("Bearer {}", key.expose())),
                ),
                (
                    "Content-Type".to_string(),
                    HeaderValue::new("application/json"),
                ),
            ],
            body,
        })
    }

    /// One attempt: verify the `EgressPermit`, build, send, classify. Never loops —
    /// looping/backoff is [`complete_structured_with_bounded_repair`]'s job, kept as a
    /// *separate* function so a single-attempt caller (e.g. a health check) does not have to
    /// pull in retry semantics.
    async fn send_once(
        &self,
        ctx: &PrivateInferenceContext,
        body: Vec<u8>,
    ) -> Result<Vec<u8>, ReasoningProviderError> {
        // §7.3: a stale or mismatched permit must never reach the transport — checked against
        // the exact bytes this call is about to send, not merely "some EgressPermit exists"
        // (that weaker check already happened at `PrivateInferenceContext::new`).
        if ctx.egress_permit().is_expired(Instant::now()) {
            return Err(ReasoningProviderError::EgressPermitExpired);
        }
        let payload = AuthorizedEgressPayload::new(body.clone());
        if payload.sha256() != ctx.egress_permit().payload_sha256() {
            return Err(ReasoningProviderError::EgressPermitPayloadMismatch);
        }

        let request = self.build_openai_request(ctx, body).await?;
        let outcome = self.transport.send(request, &self.policy).await?;
        // §11.4: `Retry-After` originates from a header on an endpoint this module treats as
        // untrusted input — clamped here, where it crosses into our system, before it can ever
        // reach a `std::thread::sleep` call (`complete_structured_with_bounded_repair`).
        let retry_after = outcome
            .retry_after
            .map(|d| d.min(self.policy.max_retry_after));
        match classify_http_status(outcome.status, retry_after) {
            None => Ok(outcome.body),
            Some(err) => Err(err),
        }
    }
}

impl<D: CredentialDecryptor> OpenAiCompatibleProvider<EgressHttpTransport, D> {
    /// ADR-0039 判据0 —— **resolver 只传一次**：检查（[`ssrf::validate_custom_endpoint`]）和
    /// 拨号（[`EgressHttpTransport`] 装进 client 的那个 resolver）都从这一个 `resolver` 派生，
    /// 所以「检查过的地址」和「实际连上的地址」在类型上就不可能来自两个不同的解析器。
    ///
    /// 这是生产构造出网 provider 的**唯一**入口。用 [`Self::new`] + 手搭 transport 的写法把
    /// 两个 resolver 当成两个独立实参，卡 17 的复审就是在生产唯一调用点上抓到那个分叉的
    /// （运维的 `HUMAUX_PRIVATE_WORKER_DNS_PINS` 只到了检查侧）；`architecture-check` 的
    /// ADR-0039 判据5/6/7 现在把这条路钉死。
    ///
    /// # Errors
    /// - `base_url` 过不了 §11.4 SSRF 闸 ⇒ 与 [`Self::new`] 同款错误；
    /// - 底层 client 构造失败 ⇒ [`ReasoningProviderError::Transport`]。
    pub fn with_egress_transport(
        descriptor: ReasoningProviderDescriptor,
        base_url: String,
        request_timeout: Duration,
        decryptor: D,
        policy: ssrf::CustomEndpointPolicy,
        resolver: Arc<dyn ssrf::DnsResolver>,
    ) -> Result<Self, ReasoningProviderError> {
        let transport = EgressHttpTransport::with_resolver(request_timeout, Arc::clone(&resolver))?;
        Self::new(
            descriptor,
            base_url,
            transport,
            decryptor,
            policy,
            resolver.as_ref(),
        )
    }
}

/// Pure, deterministic wire body for [`UserReasoningProvider::complete_structured`] — factored
/// out of the trait impl so a caller minting the [`EgressPermit`] that will authorize this
/// exact call (§7.3) can compute the identical bytes ahead of time, and so tests can do the
/// same without duplicating the format string.
///
/// ponytail: hand-built minimal JSON, no serde_json struct — this crate already depends on
/// serde_json (email.rs) but the OpenAI chat-completions body shape is not yet frozen by any
/// spec section this task owns; upgrade to a typed request struct once that shape is.
///
/// ADR-0058 D-M: [`OutputChannel::Content`] is the v1 body byte for byte; [`OutputChannel::Tool`]
/// appends one function whose `parameters` are `json_schema` verbatim (it must be a JSON object —
/// the rendered contracts are). `"reasoning_split":true` is a provider-specific field: it is sent
/// only on the tool channel and only when the descriptor declares
/// [`ReasoningCapability::ReasoningSplit`] — an endpoint that validates its body rejects an
/// unknown field.
pub fn structured_request_body(
    descriptor: &ReasoningProviderDescriptor,
    request: &StructuredReasoningRequest,
) -> Vec<u8> {
    let head = format!(
        "{{\"model\":{:?},\"messages\":[{{\"role\":\"system\",\"content\":{:?}}},{{\"role\":\"user\",\"content\":{:?}}}],\"max_tokens\":{}",
        descriptor.model_id, request.system_prompt, request.user_prompt, request.max_output_tokens
    );
    match request.output {
        OutputChannel::Content => format!("{head}}}"),
        OutputChannel::Tool(name) => format!(
            "{head},\"tools\":[{{\"type\":\"function\",\"function\":{{\"name\":{name:?},\"parameters\":{}}}}}]{}}}",
            request.json_schema,
            if descriptor
                .capabilities
                .contains(&ReasoningCapability::ReasoningSplit)
            {
                ",\"reasoning_split\":true"
            } else {
                ""
            }
        ),
    }
    .into_bytes()
}

/// Pure, deterministic wire body for [`UserReasoningProvider::analyze_vision`] — see
/// [`structured_request_body`]'s doc for why this is a free function, not inline in the trait
/// impl.
fn vision_request_body(
    descriptor: &ReasoningProviderDescriptor,
    request: &VisionReasoningRequest,
) -> Vec<u8> {
    format!(
        "{{\"model\":{:?},\"prompt\":{:?},\"artifact_count\":{}}}",
        descriptor.model_id,
        request.prompt,
        request.artifact_refs.len()
    )
    .into_bytes()
}

#[async_trait::async_trait]
impl<T: OpenAiCompatTransport, D: CredentialDecryptor> UserReasoningProvider
    for OpenAiCompatibleProvider<T, D>
{
    fn descriptor(&self) -> &ReasoningProviderDescriptor {
        &self.descriptor
    }

    fn endpoint_ref(&self) -> &str {
        &self.base_url
    }

    fn model_revision(&self) -> Option<&str> {
        self.descriptor.model_revision.as_deref()
    }

    async fn complete_structured(
        &self,
        ctx: &PrivateInferenceContext,
        request: StructuredReasoningRequest,
    ) -> Result<StructuredReasoningResponse, ReasoningProviderError> {
        self.descriptor
            .require_capability(ReasoningCapability::StructuredOutput)?;
        if let OutputChannel::Tool(_) = request.output {
            self.descriptor
                .require_capability(ReasoningCapability::ToolCalls)?;
        }
        let body = structured_request_body(&self.descriptor, &request);
        let bytes = self.send_once(ctx, body).await?;
        // envelope 解析（含 base_resp 独立错误通道——HTTP 200 不等于成功）。
        let envelope = parse_chat_envelope(&bytes, request.output)?;
        let json = match request.output {
            // 剥 <think> 块（含未闭合形态：reasoning 吃光 max_tokens 的实测静默失败）。
            OutputChannel::Content => strip_think_blocks(&envelope.content),
            // ADR-0058 D-M: the reasoning never reaches the arguments string (it travels in
            // `reasoning_content` under REASONING_SPLIT, and a tool call's arguments carry no
            // `<think>` block); an R9 `content` answer was stripped by `tool_arguments`.
            OutputChannel::Tool(_) => envelope.content,
        };
        // §11.3 "invalid structured output -> schema validation failure"：剥后必须还是
        // JSON。剥后的空串在这里自然失败——不单设空检查，同一条判据覆盖两种坏法。
        if serde_json::from_str::<serde_json::Value>(&json).is_err() {
            return Err(ReasoningProviderError::FailedOutputSchema { attempts: 1 });
        }
        Ok(StructuredReasoningResponse {
            json,
            usage: envelope.usage,
            channel_fallback: envelope.channel_fallback,
        })
    }

    async fn analyze_vision(
        &self,
        ctx: &PrivateInferenceContext,
        request: VisionReasoningRequest,
    ) -> Result<VisionReasoningResponse, ReasoningProviderError> {
        self.descriptor
            .require_capability(ReasoningCapability::Vision)?;
        let body = vision_request_body(&self.descriptor, &request);
        let bytes = self.send_once(ctx, body).await?;
        let text = String::from_utf8(bytes)
            .map_err(|e| ReasoningProviderError::Transport(e.to_string()))?;
        // ponytail: `text` is the raw response body verbatim (same TOKEN_USAGE gap as
        // `complete_structured` above), and the outbound body sends only
        // `artifact_refs.len()`, never resolved image bytes — `VisionReasoningRequest`'s own
        // doc already names artifact-ref resolution as the provider adapter's job; upgrade
        // path is resolving each ref to bytes (via whatever `private.artifacts` reader lands)
        // and embedding them in the request body once a real OpenAI-compatible vision wire
        // shape is frozen.
        Ok(VisionReasoningResponse {
            text,
            usage: TokenUsage::default(),
        })
    }
}

// =============================================================================
// Bounded same-domain schema repair (§11.3 closing paragraph)
// =============================================================================

/// §11.3: "所有修复/重试仍在同一个 USER_REASONING trust domain。不得因为结构化 JSON 失败就
/// 调用企业 Public Key 帮忙修格式". Taking `provider: &dyn UserReasoningProvider` by
/// reference — never by name, never re-resolved mid-loop — makes "retry stays on the same
/// provider instance" a type-level fact: there is no second `&dyn UserReasoningProvider` in
/// scope this function could switch to even if it wanted to.
pub async fn complete_structured_with_bounded_repair(
    provider: &dyn UserReasoningProvider,
    ctx: &PrivateInferenceContext,
    request: StructuredReasoningRequest,
    max_attempts: u32,
) -> Result<StructuredReasoningResponse, ReasoningProviderError> {
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        match provider.complete_structured(ctx, request.clone()).await {
            Ok(resp) => return Ok(resp),
            // §11.3: 401 short-circuits immediately — no retry loop entered a second time,
            // which is the operational meaning of "retry_count 不增加" at this layer (there is
            // nothing here for a caller to increment: this function returns on the first
            // WaitingKey without looping back).
            Err(ReasoningProviderError::WaitingKey { fingerprint }) => {
                return Err(ReasoningProviderError::WaitingKey { fingerprint });
            }
            Err(ReasoningProviderError::UnsupportedCapability(cap)) => {
                return Err(ReasoningProviderError::UnsupportedCapability(cap));
            }
            Err(ReasoningProviderError::ProviderPermanent { message }) => {
                return Err(ReasoningProviderError::ProviderPermanent { message });
            }
            Err(ReasoningProviderError::RetryWait { retry_after }) => {
                if attempts >= max_attempts {
                    return Err(ReasoningProviderError::RetryWait { retry_after });
                }
                // ponytail: blocking sleep inside an async fn — see module doc "pending
                // wiring" note (acceptable in a background worker, not on a request path).
                std::thread::sleep(retry_after.unwrap_or_else(|| backoff_for_attempt(attempts)));
            }
            Err(ReasoningProviderError::FailedOutputSchema { .. }) => {
                if attempts >= max_attempts {
                    return Err(ReasoningProviderError::FailedOutputSchema { attempts });
                }
                // Same provider, same credential, same reasoning domain — only the prompt
                // asking for a repair would change on a real repair pass; this scaffold
                // re-sends the identical request (bounded budget is what this task's
                // acceptance test checks: attempts exhaust into `FailedOutputSchema`, never
                // an unbounded loop or a different provider).
            }
            Err(other) => return Err(other),
        }
    }
}

/// Exponential backoff with a fixed cap, used only when a 5xx/timeout carried no explicit
/// `Retry-After` (§11.3 "bounded transient retry + jitter"). ponytail: `attempt.min(4)` caps
/// growth at 16x the base instead of a real jitter RNG — no randomness dependency is
/// otherwise needed in this crate; add jitter if thundering-herd retries are ever observed.
fn backoff_for_attempt(attempt: u32) -> Duration {
    let base = Duration::from_millis(200);
    base * 2u32.pow(attempt.min(4))
}

/// ADR-0058 R8: whether any string of `value` — object keys included, at any depth — carries
/// U+0000, which PostgreSQL `text`/`jsonb` cannot store. Every model-reply parser refuses such a
/// reply as malformed before any write (refused, never stripped: ADR-0048), and the gateway
/// refuses such caller input as `INVALID_INPUT` at validation.
pub fn json_has_nul(value: &serde_json::Value) -> bool {
    use serde_json::Value;
    match value {
        Value::String(text) => text.contains('\0'),
        Value::Array(items) => items.iter().any(json_has_nul),
        Value::Object(map) => map
            .iter()
            .any(|(key, item)| key.contains('\0') || json_has_nul(item)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ADR-0058 R8 — fault: `Value::String` answers `false`.
    #[test]
    fn json_has_nul_finds_u0000_in_any_string_or_key_at_any_depth() {
        for nul in [
            serde_json::json!("a\u{0}b"),
            serde_json::json!({ "memories": [{ "affects": [{ "label": "\u{0}" }] }] }),
            serde_json::json!({ "k\u{0}": 1 }),
            serde_json::json!([[["\u{0}"]]]),
        ] {
            assert!(json_has_nul(&nul), "{nul}");
        }
        for clean in [
            serde_json::json!({ "content": "a\\u0000b", "n": 0, "b": false, "x": null }),
            serde_json::json!("\u{1}"),
        ] {
            assert!(!json_has_nul(&clean), "{clean}");
        }
    }

    /// ADR-0058 D-N — fault: map two variants to one class.
    #[test]
    fn every_provider_error_variant_has_its_own_class() {
        let all = [
            ReasoningProviderError::WaitingKey { fingerprint: None },
            ReasoningProviderError::ProviderPermanent {
                message: String::new(),
            },
            ReasoningProviderError::RetryWait { retry_after: None },
            ReasoningProviderError::FailedOutputSchema { attempts: 1 },
            ReasoningProviderError::UnsupportedCapability(ReasoningCapability::Vision),
            ReasoningProviderError::MixedReasoningDomain,
            ReasoningProviderError::NoProcessingPrincipal,
            ReasoningProviderError::EndpointRejected(ssrf::SsrfError::NonHttps),
            ReasoningProviderError::EgressPermitExpired,
            ReasoningProviderError::EgressPermitPayloadMismatch,
            ReasoningProviderError::Transport(String::new()),
        ];
        let classes: std::collections::BTreeSet<&str> = all.iter().map(|e| e.class()).collect();
        assert_eq!(classes.len(), all.len(), "one class per variant");
        assert_eq!(
            all[0].class(),
            ReasoningProviderError::WAITING_KEY_CLASS,
            "the 401 class is the constant workers compare with"
        );
    }
    use humaux_domain::egress::{ProcessorId, authorize};
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Matches this crate's existing convention (`tests/outbox_batch_remember.rs`,
    /// `tests/retrieve_read_your_writes.rs`): drive an async body from a plain `#[test]` via
    /// `Runtime::block_on` rather than `#[tokio::test]` — the crate's dev-dependency `tokio`
    /// entry carries `rt-multi-thread` only, not `macros`, so `#[tokio::test]` is not
    /// available without touching the shared `Cargo.toml` a second time for a feature flag
    /// this convention already avoids needing.
    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Runtime::new().expect("tokio runtime")
    }

    fn user_reasoning_permit(tenant: TenantId) -> EgressPermit {
        permit_for_payload(tenant, b"x", Duration::from_secs(60))
    }

    fn permit_for_payload(tenant: TenantId, payload_bytes: &[u8], ttl: Duration) -> EgressPermit {
        let payload = AuthorizedEgressPayload::new(payload_bytes.to_vec());
        authorize(
            tenant,
            ProcessorId(Uuid::now_v7()),
            PrivateDataPurpose::UserReasoning,
            // §7.5.1: only the two retrieval purposes carry a `data_class` ceiling —
            // `UserReasoning` mints normally for any grade, `Private` matches this fixture's
            // ordinary-content test bodies.
            humaux_domain::dataclass::DataClass::Private,
            &payload,
            ttl,
        )
        .expect("UserReasoning authorize is unrestricted by data_class")
    }

    fn ctx_with_permit(tenant: TenantId, permit: EgressPermit) -> PrivateInferenceContext {
        PrivateInferenceContext::new(
            tenant,
            UserId::new(),
            ReasoningDomainId::new(),
            CredentialRef::new(Uuid::now_v7()),
            permit,
            "openai-compatible",
            "test-model",
            1,
            "trace-1",
        )
        .expect("valid context")
    }

    /// §7.3: every test below that actually drives a call through `send_once` needs a permit
    /// bound to the *exact* bytes that call will send — `structured_request_body` is the same
    /// pure function `complete_structured` itself calls, so this fixture and the code under
    /// test can never silently drift apart.
    fn ctx(tenant: TenantId) -> PrivateInferenceContext {
        let body = structured_request_body(&descriptor(), &structured_request());
        ctx_with_permit(
            tenant,
            permit_for_payload(tenant, &body, Duration::from_secs(60)),
        )
    }

    fn resolver_for(ip: IpAddr) -> ssrf::FakeResolver {
        ssrf::FakeResolver(vec![ip])
    }

    fn example_public_ip() -> IpAddr {
        // 93.184.216.34 — example.com's long-standing public address, same constant
        // `ssrf::tests::public_ip` uses (kept as a local literal rather than exporting that
        // test-only helper across the module boundary for one constant).
        IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))
    }

    // -------------------------------------------------------------------
    // Plaintext key never leaks into Debug output.
    // -------------------------------------------------------------------

    #[test]
    fn plaintext_api_key_debug_never_contains_the_secret() {
        let key = PlaintextApiKey::new("sk-super-secret-value-12345".to_string());
        let debug = format!("{key:?}");
        let display = format!("{key}");
        assert!(!debug.contains("sk-super-secret-value-12345"));
        assert!(!display.contains("sk-super-secret-value-12345"));
        assert!(debug.contains("fp:"));
    }

    /// Exercises the *real* `build_openai_request` path (not a hand-made `PlaintextApiKey`):
    /// a sentinel key is decrypted, built into an `OpenAiHttpRequest`, and handed to a
    /// transport that `{:?}`-prints everything it receives before this test ever inspects it.
    /// Regression coverage for the blocker this file's review found: `OpenAiHttpRequest`
    /// deriving plain `Debug`/`Clone` over `Vec<(String, String)>` headers printed
    /// `"Bearer sk-..."` verbatim.
    #[test]
    fn build_openai_request_debug_never_contains_plaintext_key_material() {
        const SENTINEL: &str = "sk-SUPER-SECRET-REVIEW-PROBE";

        struct SentinelDecryptor;
        #[async_trait::async_trait]
        impl CredentialDecryptor for SentinelDecryptor {
            async fn resolve(
                &self,
                _credential_ref: CredentialRef,
            ) -> Result<PlaintextApiKey, ReasoningProviderError> {
                Ok(PlaintextApiKey::new(SENTINEL.to_string()))
            }
        }

        struct CapturingTransport(Mutex<Option<String>>);
        #[async_trait::async_trait]
        impl OpenAiCompatTransport for CapturingTransport {
            async fn send(
                &self,
                request: OpenAiHttpRequest,
                _policy: &ssrf::CustomEndpointPolicy,
            ) -> Result<OpenAiHttpOutcome, ReasoningProviderError> {
                *self.0.lock().unwrap() = Some(format!("{request:?}"));
                Ok(OpenAiHttpOutcome {
                    status: 200,
                    retry_after: None,
                    body: b"{}".to_vec(),
                })
            }
        }

        rt().block_on(async {
            let resolver = resolver_for(example_public_ip());
            let transport = CapturingTransport(Mutex::new(None));
            let provider = OpenAiCompatibleProvider::new(
                descriptor(),
                "https://api.example.com/v1/chat/completions".to_string(),
                transport,
                SentinelDecryptor,
                ssrf::CustomEndpointPolicy::default(),
                &resolver,
            )
            .expect("valid endpoint");
            let tenant = TenantId::new();
            let c = ctx(tenant);

            let _ = provider.complete_structured(&c, structured_request()).await;

            let captured = provider
                .transport
                .0
                .lock()
                .unwrap()
                .clone()
                .expect("transport was called");
            assert!(!captured.contains(SENTINEL));
            assert!(captured.contains("HeaderValue(redacted)"));
        });
    }

    // -------------------------------------------------------------------
    // §11.3 401 -> WAITING_KEY, no retry loop entered.
    // -------------------------------------------------------------------

    struct CountingTransport {
        calls: AtomicU32,
        status: u16,
    }

    #[async_trait::async_trait]
    impl OpenAiCompatTransport for CountingTransport {
        async fn send(
            &self,
            _request: OpenAiHttpRequest,
            _policy: &ssrf::CustomEndpointPolicy,
        ) -> Result<OpenAiHttpOutcome, ReasoningProviderError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(OpenAiHttpOutcome {
                status: self.status,
                retry_after: None,
                body: b"{}".to_vec(),
            })
        }
    }

    struct StaticDecryptor;

    #[async_trait::async_trait]
    impl CredentialDecryptor for StaticDecryptor {
        async fn resolve(
            &self,
            _credential_ref: CredentialRef,
        ) -> Result<PlaintextApiKey, ReasoningProviderError> {
            Ok(PlaintextApiKey::new("sk-test-key".to_string()))
        }
    }

    fn descriptor() -> ReasoningProviderDescriptor {
        ReasoningProviderDescriptor {
            provider_id: "openai-compatible".to_string(),
            model_id: "test-model".to_string(),
            model_revision: None,
            capabilities: vec![
                ReasoningCapability::StructuredOutput,
                ReasoningCapability::Vision,
                ReasoningCapability::ToolCalls,
                ReasoningCapability::ReasoningSplit,
            ],
            custom_endpoint: None,
        }
    }

    /// The same descriptor minus `capability`.
    fn descriptor_without(capability: ReasoningCapability) -> ReasoningProviderDescriptor {
        let mut d = descriptor();
        d.capabilities.retain(|c| *c != capability);
        d
    }

    fn structured_request() -> StructuredReasoningRequest {
        StructuredReasoningRequest {
            system_prompt: "s".to_string(),
            user_prompt: "u".to_string(),
            json_schema: "{}".to_string(),
            max_output_tokens: 64,
            output: OutputChannel::Content,
        }
    }

    #[test]
    fn status_401_is_waiting_key_and_never_retries() {
        rt().block_on(async {
            let transport = CountingTransport {
                calls: AtomicU32::new(0),
                status: 401,
            };
            let resolver = resolver_for(example_public_ip());
            let provider = OpenAiCompatibleProvider::new(
                descriptor(),
                "https://api.example.com/v1/chat/completions".to_string(),
                transport,
                StaticDecryptor,
                ssrf::CustomEndpointPolicy::default(),
                &resolver,
            )
            .expect("valid endpoint");
            let tenant = TenantId::new();
            let c = ctx(tenant);

            let result =
                complete_structured_with_bounded_repair(&provider, &c, structured_request(), 5)
                    .await;

            assert!(matches!(
                result,
                Err(ReasoningProviderError::WaitingKey { .. })
            ));
            // The whole point of §11.3's "retry_count 不增加": exactly one transport call was
            // made, regardless of `max_attempts` being 5 — a WaitingKey never loops.
            assert_eq!(provider.transport.calls.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn status_429_honors_retry_after_then_succeeds() {
        struct FlakyTransport {
            calls: Mutex<u32>,
        }
        #[async_trait::async_trait]
        impl OpenAiCompatTransport for FlakyTransport {
            async fn send(
                &self,
                _request: OpenAiHttpRequest,
                _policy: &ssrf::CustomEndpointPolicy,
            ) -> Result<OpenAiHttpOutcome, ReasoningProviderError> {
                let mut calls = self.calls.lock().unwrap();
                *calls += 1;
                if *calls == 1 {
                    Ok(OpenAiHttpOutcome {
                        status: 429,
                        retry_after: Some(Duration::from_millis(1)),
                        body: Vec::new(),
                    })
                } else {
                    // envelope 形状（parse_chat_envelope 之后，裸 JSON body 不再是合法响应）。
                    Ok(OpenAiHttpOutcome {
                        status: 200,
                        retry_after: None,
                        body: br#"{"choices":[{"message":{"content":"{\"ok\":true}"}}]}"#.to_vec(),
                    })
                }
            }
        }
        rt().block_on(async {
            let transport = FlakyTransport {
                calls: Mutex::new(0),
            };
            let resolver = resolver_for(example_public_ip());
            let provider = OpenAiCompatibleProvider::new(
                descriptor(),
                "https://api.example.com/v1/chat/completions".to_string(),
                transport,
                StaticDecryptor,
                ssrf::CustomEndpointPolicy::default(),
                &resolver,
            )
            .expect("valid endpoint");
            let tenant = TenantId::new();
            let c = ctx(tenant);

            let result =
                complete_structured_with_bounded_repair(&provider, &c, structured_request(), 5)
                    .await;

            assert!(result.is_ok());
            assert_eq!(*provider.transport.calls.lock().unwrap(), 2);
        });
    }

    /// 固定 envelope 的 transport——G1–G4 的共用夹具。
    struct CannedTransport(&'static [u8]);
    #[async_trait::async_trait]
    impl OpenAiCompatTransport for CannedTransport {
        async fn send(
            &self,
            _request: OpenAiHttpRequest,
            _policy: &ssrf::CustomEndpointPolicy,
        ) -> Result<OpenAiHttpOutcome, ReasoningProviderError> {
            Ok(OpenAiHttpOutcome {
                status: 200,
                retry_after: None,
                body: self.0.to_vec(),
            })
        }
    }

    async fn call_canned(
        body: &'static [u8],
    ) -> Result<StructuredReasoningResponse, ReasoningProviderError> {
        let resolver = resolver_for(example_public_ip());
        let provider = OpenAiCompatibleProvider::new(
            descriptor(),
            "https://api.example.com/v1/chat/completions".to_string(),
            CannedTransport(body),
            StaticDecryptor,
            ssrf::CustomEndpointPolicy::default(),
            &resolver,
        )
        .expect("valid endpoint");
        let tenant = TenantId::new();
        let c = ctx(tenant);
        provider.complete_structured(&c, structured_request()).await
    }

    /// G1：**HTTP 200 不等于成功**——`base_resp.status_code != 0` 是 MiniMax 的独立错误
    /// 通道（实测：限流走 200 + 2062，不走 429）。三个码各按 §11.3 语义分类。
    /// 注错：注释掉 parse_chat_envelope 的 base_resp 分支 ⇒ body 是合法 envelope、content
    /// 可解析 ⇒ Ok ⇒ 三条断言全红。
    #[test]
    fn g1_nonzero_base_resp_fails_even_with_http_200() {
        rt().block_on(async {
            let auth = call_canned(
                br#"{"base_resp":{"status_code":1004,"status_msg":"auth failed"},"choices":[{"message":{"content":"{}"}}]}"#,
            )
            .await;
            assert!(
                matches!(auth, Err(ReasoningProviderError::WaitingKey { .. })),
                "1004 是 BYOK 鉴权失败 = WaitingKey（§11.3），实得 {auth:?}"
            );

            let throttle = call_canned(
                br#"{"base_resp":{"status_code":2062,"status_msg":"token plan rate limit"},"choices":[{"message":{"content":"{}"}}]}"#,
            )
            .await;
            assert!(
                matches!(throttle, Err(ReasoningProviderError::RetryWait { .. })),
                "2062 是实测限流码（HTTP 200 形态）——判成永久错误会让 bounded repair 直接\
                 放弃，实得 {throttle:?}"
            );

            let unknown = call_canned(
                br#"{"base_resp":{"status_code":9999,"status_msg":"?"},"choices":[{"message":{"content":"{}"}}]}"#,
            )
            .await;
            assert!(
                matches!(unknown, Err(ReasoningProviderError::ProviderPermanent { .. })),
                "未知非零码 fail-closed 落 ProviderPermanent，实得 {unknown:?}"
            );
        });
    }

    /// G2：`<think>` 块必须剥掉，剥后才是结构化输出。
    /// 注错：strip_think_blocks 改恒等返回 ⇒ 整串非法 JSON ⇒ FailedOutputSchema ⇒ 红。
    #[test]
    fn g2_think_blocks_are_stripped_from_the_structured_output() {
        rt().block_on(async {
            let resp = call_canned(
                br#"{"choices":[{"message":{"content":"<think>reasoning here</think>{\"answer\":4}"}}]}"#,
            )
            .await
            .expect("剥掉 think 块之后是合法 JSON");
            assert_eq!(resp.json, r#"{"answer":4}"#);
            assert!(!resp.json.contains("<think>"));
        });
    }

    /// G3：**未闭合的 `<think>`**——max_tokens 太小、reasoning 吃光预算的实测形态：
    /// HTTP 200、base_resp=0、无任何错误标志、content 只有半截思考块。必须变显式红
    /// （FailedOutputSchema），不许把半截思考块当成结构化输出交出去。
    /// 注错：删掉剥后 JSON 校验、直接 Ok 包裹 ⇒ 返回 Ok("") ⇒ 红。
    #[test]
    fn g3_an_unterminated_think_block_is_a_loud_schema_failure_not_silent_200() {
        rt().block_on(async {
            let r = call_canned(
                br#"{"choices":[{"message":{"content":"<think>The user is asking me to"}}]}"#,
            )
            .await;
            assert!(
                matches!(r, Err(ReasoningProviderError::FailedOutputSchema { .. })),
                "reasoning 吃光预算的静默 200 必须转显式 schema 失败，实得 {r:?}"
            );
        });
    }

    /// G4：usage 四字段逐一解析为精确值（含 reasoning/cached 两个嵌套字段）。
    /// 注错：解析回退 TokenUsage::default() ⇒ 四条断言红。
    #[test]
    fn g4_usage_fields_are_parsed_not_defaulted() {
        rt().block_on(async {
            let resp = call_canned(
                br#"{"choices":[{"message":{"content":"{}"}}],"usage":{"prompt_tokens":10,"completion_tokens":57,"completion_tokens_details":{"reasoning_tokens":50},"prompt_tokens_details":{"cached_tokens":3}}}"#,
            )
            .await
            .expect("合法 envelope");
            assert_eq!(resp.usage.input_tokens, Some(10));
            assert_eq!(resp.usage.output_tokens, Some(57));
            assert_eq!(resp.usage.reasoning_tokens, Some(50));
            assert_eq!(resp.usage.cached_input_tokens, Some(3));
        });
    }

    /// 语义修正的另一半：body **整体**不是 JSON = 端点坏了 = ProviderPermanent。
    /// 旧行为把它判成 FailedOutputSchema，bounded repair 会拿预算白白重试一个坏端点。
    #[test]
    fn non_json_body_is_provider_permanent_not_schema_failure() {
        rt().block_on(async {
            let r = call_canned(b"not json at all").await;
            assert!(
                matches!(r, Err(ReasoningProviderError::ProviderPermanent { .. })),
                "坏端点不该消耗 repair 预算，实得 {r:?}"
            );
        });
    }

    /// `reasoning_content` 独立字段（M3 实测形态之一）绝不许漏进结构化输出。
    #[test]
    fn reasoning_content_side_field_never_reaches_the_output() {
        rt().block_on(async {
            let resp = call_canned(
                br#"{"choices":[{"message":{"content":"{\"a\":1}","reasoning_content":"secret chain of thought"}}]}"#,
            )
            .await
            .expect("合法 envelope");
            assert_eq!(resp.json, r#"{"a":1}"#);
            assert!(!resp.json.contains("chain of thought"));
        });
    }

    #[test]
    fn failed_output_schema_after_budget_exhausted() {
        // 语义修正（parse_chat_envelope 落地时同步）：body 整体不是 JSON = **端点坏了**
        // = ProviderPermanent，bounded repair 不该拿预算重试一个坏端点——那个情形由下面的
        // `non_json_body_is_provider_permanent_not_schema_failure` 单独钉。本条测的是
        // 「envelope 合法但**模型输出**不是 JSON」——这才是 FailedOutputSchema 的语义。
        struct GarbageBodyTransport(AtomicU32);
        #[async_trait::async_trait]
        impl OpenAiCompatTransport for GarbageBodyTransport {
            async fn send(
                &self,
                _request: OpenAiHttpRequest,
                _policy: &ssrf::CustomEndpointPolicy,
            ) -> Result<OpenAiHttpOutcome, ReasoningProviderError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(OpenAiHttpOutcome {
                    status: 200,
                    retry_after: None,
                    body: br#"{"choices":[{"message":{"content":"not json"}}]}"#.to_vec(),
                })
            }
        }
        rt().block_on(async {
            let garbage = GarbageBodyTransport(AtomicU32::new(0));
            let resolver = resolver_for(example_public_ip());
            let provider = OpenAiCompatibleProvider::new(
                descriptor(),
                "https://api.example.com/v1/chat/completions".to_string(),
                garbage,
                StaticDecryptor,
                ssrf::CustomEndpointPolicy::default(),
                &resolver,
            )
            .expect("valid endpoint");
            let tenant = TenantId::new();
            let c = ctx(tenant);

            let result =
                complete_structured_with_bounded_repair(&provider, &c, structured_request(), 3)
                    .await;

            assert!(matches!(
                result,
                Err(ReasoningProviderError::FailedOutputSchema { attempts: 3 })
            ));
            assert_eq!(provider.transport.0.load(Ordering::SeqCst), 3);
        });
    }

    #[test]
    fn unsupported_capability_fails_before_any_external_call() {
        rt().block_on(async {
            let transport = CountingTransport {
                calls: AtomicU32::new(0),
                status: 200,
            };
            let mut d = descriptor();
            d.capabilities = vec![ReasoningCapability::Vision]; // no StructuredOutput
            let resolver = resolver_for(example_public_ip());
            let provider = OpenAiCompatibleProvider::new(
                d,
                "https://api.example.com/v1/chat/completions".to_string(),
                transport,
                StaticDecryptor,
                ssrf::CustomEndpointPolicy::default(),
                &resolver,
            )
            .expect("valid endpoint");
            let tenant = TenantId::new();
            let c = ctx(tenant);

            let result = provider.complete_structured(&c, structured_request()).await;

            assert!(matches!(
                result,
                Err(ReasoningProviderError::UnsupportedCapability(
                    ReasoningCapability::StructuredOutput
                ))
            ));
            assert_eq!(provider.transport.calls.load(Ordering::SeqCst), 0);
        });
    }

    // -------------------------------------------------------------------
    // §11.2.1 mixed reasoning-domain input is rejected.
    // -------------------------------------------------------------------

    #[test]
    fn single_domain_input_is_accepted() {
        let d = ReasoningDomainId::new();
        assert_eq!(require_single_reasoning_domain(&[d, d, d]).unwrap(), d);
    }

    #[test]
    fn mixed_domain_input_is_rejected() {
        let a = ReasoningDomainId::new();
        let b = ReasoningDomainId::new();
        assert!(matches!(
            require_single_reasoning_domain(&[a, b]),
            Err(ReasoningProviderError::MixedReasoningDomain)
        ));
    }

    #[test]
    fn empty_input_is_no_processing_principal() {
        assert!(matches!(
            require_single_reasoning_domain(&[]),
            Err(ReasoningProviderError::NoProcessingPrincipal)
        ));
    }

    #[test]
    fn unknown_principal_ingress_is_deterministic_only() {
        assert_eq!(
            reasoning_domain_for_ingress(ProcessingPrincipal::Unknown),
            None
        );
        let d = ReasoningDomainId::new();
        assert_eq!(
            reasoning_domain_for_ingress(ProcessingPrincipal::DirectUser(d)),
            Some(d)
        );
    }

    // -------------------------------------------------------------------
    // §7/§11.1: an EgressPermit for the wrong tenant/purpose cannot build a context.
    // -------------------------------------------------------------------

    #[test]
    fn context_rejects_permit_for_a_different_tenant() {
        let ctx_tenant = TenantId::new();
        let permit_tenant = TenantId::new();
        let result = PrivateInferenceContext::new(
            ctx_tenant,
            UserId::new(),
            ReasoningDomainId::new(),
            CredentialRef::new(Uuid::now_v7()),
            user_reasoning_permit(permit_tenant),
            "p",
            "m",
            1,
            "t",
        );
        assert_eq!(
            result.unwrap_err(),
            InferenceContextError::PermitTenantMismatch
        );
    }

    #[test]
    fn context_rejects_a_non_user_reasoning_permit() {
        let tenant = TenantId::new();
        let payload = AuthorizedEgressPayload::new(b"x".to_vec());
        let retrieval_permit = authorize(
            tenant,
            ProcessorId(Uuid::now_v7()),
            PrivateDataPurpose::RetrievalEmbedding,
            humaux_domain::dataclass::DataClass::Private,
            &payload,
            Duration::from_secs(60),
        )
        .expect("Private data_class is unrestricted for RetrievalEmbedding");
        let result = PrivateInferenceContext::new(
            tenant,
            UserId::new(),
            ReasoningDomainId::new(),
            CredentialRef::new(Uuid::now_v7()),
            retrieval_permit,
            "p",
            "m",
            1,
            "t",
        );
        assert_eq!(
            result.unwrap_err(),
            InferenceContextError::PermitWrongPurpose
        );
    }

    // -------------------------------------------------------------------
    // §78.2 contract surface.
    // -------------------------------------------------------------------

    /// §78.2 "DB enum 与 Rust enum 走 contract test 对账": runs against the real migration
    /// file text (same `include_str!` pattern as `jobs.rs::contract_tests`), not a hardcoded
    /// copy of the CHECK list. 0195 is the migration that last defines all three capability
    /// CHECKs; the live constraints are compared too (`byok_egress_rebinding.rs`, real PG).
    const MIGRATION_0195_SQL: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations/0195_reasoning_capabilities_tool_calls.sql"
    ));

    fn migration_0195_capability_checks() -> Vec<Vec<String>> {
        let needle = "capabilities <@ ARRAY[";
        MIGRATION_0195_SQL
            .match_indices(needle)
            .map(|(at, _)| {
                let start = at + needle.len();
                let end = MIGRATION_0195_SQL[start..]
                    .find(']')
                    .expect("unterminated capabilities ARRAY[...] literal")
                    + start;
                MIGRATION_0195_SQL[start..end]
                    .split(',')
                    .map(|s| s.trim().trim_matches('\'').to_string())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn capability_wire_form_matches_migration_check_constraint() {
        let checks = migration_0195_capability_checks();
        assert_eq!(checks.len(), 3, "0048 + two 0128 tables");
        let wire: Vec<String> = ReasoningCapability::ALL
            .iter()
            .map(|c| c.as_str().to_string())
            .collect();
        for db in checks {
            assert_eq!(wire, db);
        }
        for c in ReasoningCapability::ALL {
            assert_eq!(ReasoningCapability::parse(c.as_str()), Some(c));
        }
    }

    #[test]
    fn classify_http_status_matches_11_3_table() {
        assert!(classify_http_status(200, None).is_none());
        assert!(matches!(
            classify_http_status(401, None),
            Some(ReasoningProviderError::WaitingKey { .. })
        ));
        assert!(matches!(
            classify_http_status(403, None),
            Some(ReasoningProviderError::ProviderPermanent { .. })
        ));
        // §11.3 groups timeout with 5xx as transient, not permanent.
        assert!(matches!(
            classify_http_status(408, None),
            Some(ReasoningProviderError::RetryWait { .. })
        ));
        let ra = Some(Duration::from_secs(30));
        assert!(matches!(
            classify_http_status(429, ra),
            Some(ReasoningProviderError::RetryWait { retry_after: Some(d) }) if d == Duration::from_secs(30)
        ));
        assert!(matches!(
            classify_http_status(503, None),
            Some(ReasoningProviderError::RetryWait { retry_after: None })
        ));
    }

    // -------------------------------------------------------------------
    // §11.4: an unvalidated custom endpoint cannot become a live provider.
    // -------------------------------------------------------------------

    #[test]
    fn provider_construction_rejects_ssrf_forbidden_endpoint() {
        let resolver = resolver_for(example_public_ip());
        let transport = CountingTransport {
            calls: AtomicU32::new(0),
            status: 200,
        };
        let err = OpenAiCompatibleProvider::new(
            descriptor(),
            "https://169.254.169.254/latest/meta-data".to_string(),
            transport,
            StaticDecryptor,
            ssrf::CustomEndpointPolicy::default(),
            &resolver,
        )
        .err()
        .expect("cloud-metadata endpoint must be rejected at construction");
        assert!(matches!(err, ReasoningProviderError::EndpointRejected(_)));
    }

    // -------------------------------------------------------------------
    // §7.3: the EgressPermit must actually gate what gets sent.
    // -------------------------------------------------------------------

    #[test]
    fn expired_egress_permit_is_rejected_before_any_send() {
        rt().block_on(async {
            let resolver = resolver_for(example_public_ip());
            let transport = CountingTransport {
                calls: AtomicU32::new(0),
                status: 200,
            };
            let provider = OpenAiCompatibleProvider::new(
                descriptor(),
                "https://api.example.com/v1/chat/completions".to_string(),
                transport,
                StaticDecryptor,
                ssrf::CustomEndpointPolicy::default(),
                &resolver,
            )
            .expect("valid endpoint");
            let tenant = TenantId::new();
            let body = structured_request_body(&descriptor(), &structured_request());
            // TTL of 0 — `Instant::now()` moving forward at all before the send is checked
            // puts the permit at-or-past its own expiry (same technique as
            // `domain::egress::tests::expiry_is_observable_by_the_adapter`).
            let permit = permit_for_payload(tenant, &body, Duration::from_millis(0));
            std::thread::sleep(Duration::from_millis(1));
            let c = ctx_with_permit(tenant, permit);

            let result = provider.complete_structured(&c, structured_request()).await;

            assert!(matches!(
                result,
                Err(ReasoningProviderError::EgressPermitExpired)
            ));
            assert_eq!(provider.transport.calls.load(Ordering::SeqCst), 0);
        });
    }

    #[test]
    fn egress_permit_for_a_different_payload_is_rejected_before_any_send() {
        rt().block_on(async {
            let resolver = resolver_for(example_public_ip());
            let transport = CountingTransport {
                calls: AtomicU32::new(0),
                status: 200,
            };
            let provider = OpenAiCompatibleProvider::new(
                descriptor(),
                "https://api.example.com/v1/chat/completions".to_string(),
                transport,
                StaticDecryptor,
                ssrf::CustomEndpointPolicy::default(),
                &resolver,
            )
            .expect("valid endpoint");
            let tenant = TenantId::new();
            // §7.3's own worked case, reproduced: a permit minted for one payload must not
            // authorize sending a different one — here, the unrelated chat-completions JSON
            // `complete_structured` actually builds.
            let permit = permit_for_payload(
                tenant,
                b"THIS-IS-THE-AUTHORIZED-PAYLOAD",
                Duration::from_secs(60),
            );
            let c = ctx_with_permit(tenant, permit);

            let result = provider.complete_structured(&c, structured_request()).await;

            assert!(matches!(
                result,
                Err(ReasoningProviderError::EgressPermitPayloadMismatch)
            ));
            assert_eq!(provider.transport.calls.load(Ordering::SeqCst), 0);
        });
    }

    // -------------------------------------------------------------------
    // §11.4: an untrusted provider-supplied Retry-After must not park a caller for days.
    // -------------------------------------------------------------------

    #[test]
    fn huge_retry_after_from_endpoint_is_clamped_to_policy_ceiling() {
        struct HugeRetryAfterTransport;
        #[async_trait::async_trait]
        impl OpenAiCompatTransport for HugeRetryAfterTransport {
            async fn send(
                &self,
                _request: OpenAiHttpRequest,
                _policy: &ssrf::CustomEndpointPolicy,
            ) -> Result<OpenAiHttpOutcome, ReasoningProviderError> {
                Ok(OpenAiHttpOutcome {
                    status: 429,
                    // ~11 days — a hostile/misconfigured custom endpoint's header value.
                    retry_after: Some(Duration::from_secs(999_999)),
                    body: Vec::new(),
                })
            }
        }
        rt().block_on(async {
            let resolver = resolver_for(example_public_ip());
            let policy = ssrf::CustomEndpointPolicy {
                max_retry_after: Duration::from_secs(5),
                ..Default::default()
            };
            let provider = OpenAiCompatibleProvider::new(
                descriptor(),
                "https://api.example.com/v1/chat/completions".to_string(),
                HugeRetryAfterTransport,
                StaticDecryptor,
                policy,
                &resolver,
            )
            .expect("valid endpoint");
            let tenant = TenantId::new();
            let c = ctx(tenant);

            let result = provider.complete_structured(&c, structured_request()).await;

            assert!(matches!(
                result,
                Err(ReasoningProviderError::RetryWait { retry_after: Some(d) })
                    if d == Duration::from_secs(5)
            ));
        });
    }

    // -------------------------------------------------------------------
    // ADR-0058 D-M: the emit_distillation tool channel.
    // -------------------------------------------------------------------

    const TOOL: &str = "emit_distillation";

    fn tool_request() -> StructuredReasoningRequest {
        StructuredReasoningRequest {
            json_schema: r#"{"type":"object"}"#.to_string(),
            output: OutputChannel::Tool(TOOL),
            ..structured_request()
        }
    }

    async fn call_tool_canned(
        body: &'static [u8],
    ) -> Result<StructuredReasoningResponse, ReasoningProviderError> {
        let resolver = resolver_for(example_public_ip());
        let provider = OpenAiCompatibleProvider::new(
            descriptor(),
            "https://api.example.com/v1/chat/completions".to_string(),
            CannedTransport(body),
            StaticDecryptor,
            ssrf::CustomEndpointPolicy::default(),
            &resolver,
        )
        .expect("valid endpoint");
        let tenant = TenantId::new();
        let body = structured_request_body(&descriptor(), &tool_request());
        let c = ctx_with_permit(
            tenant,
            permit_for_payload(tenant, &body, Duration::from_secs(60)),
        );
        provider.complete_structured(&c, tool_request()).await
    }

    /// Fault: omit `reasoning_split` (or the tool) from the Tool body.
    #[test]
    fn tool_channel_body_carries_one_tool_and_reasoning_split() {
        let body: serde_json::Value =
            serde_json::from_slice(&structured_request_body(&descriptor(), &tool_request()))
                .expect("the tool body is JSON");
        let tools = body["tools"].as_array().expect("tools array");
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["type"], "function");
        assert_eq!(tools[0]["function"]["name"], TOOL);
        assert_eq!(
            tools[0]["function"]["parameters"],
            serde_json::json!({"type":"object"})
        );
        assert_eq!(body["reasoning_split"], true);
        // W1 (ADR-0058): a named tool_choice is accepted but not honoured, so it is never sent.
        assert!(body.get("tool_choice").is_none());
        assert_eq!(body["max_tokens"], 64);
    }

    /// ADR-0058 D-M (ruling 2026-10-02 10:35, test 2) — fault: send `reasoning_split`
    /// unconditionally on the tool channel.
    #[test]
    fn tool_channel_without_reasoning_split_capability_sends_no_reasoning_split() {
        let body: serde_json::Value = serde_json::from_slice(&structured_request_body(
            &descriptor_without(ReasoningCapability::ReasoningSplit),
            &tool_request(),
        ))
        .expect("the tool body is JSON");
        assert_eq!(body["tools"].as_array().map(Vec::len), Some(1));
        assert!(body.get("reasoning_split").is_none());
    }

    /// ADR-0058 D-M — fault: drop the TOOL_CALLS gate in `complete_structured`. A tool request to a
    /// provider that does not declare tool calls fails before any byte is sent.
    #[test]
    fn a_tool_request_without_tool_calls_capability_fails_before_the_network() {
        rt().block_on(async {
            let resolver = resolver_for(example_public_ip());
            let provider = OpenAiCompatibleProvider::new(
                descriptor_without(ReasoningCapability::ToolCalls),
                "https://api.example.com/v1/chat/completions".to_string(),
                CannedTransport(b"{}"),
                StaticDecryptor,
                ssrf::CustomEndpointPolicy::default(),
                &resolver,
            )
            .expect("valid endpoint");
            let tenant = TenantId::new();
            let body = structured_request_body(
                &descriptor_without(ReasoningCapability::ToolCalls),
                &tool_request(),
            );
            let c = ctx_with_permit(
                tenant,
                permit_for_payload(tenant, &body, Duration::from_secs(60)),
            );
            assert!(matches!(
                provider.complete_structured(&c, tool_request()).await,
                Err(ReasoningProviderError::UnsupportedCapability(
                    ReasoningCapability::ToolCalls
                ))
            ));
        });
    }

    /// Fault: emit `tools` on the Content channel.
    #[test]
    fn content_channel_body_is_byte_identical_to_v1() {
        assert_eq!(
            String::from_utf8(structured_request_body(
                &descriptor(),
                &structured_request()
            ))
            .expect("utf-8"),
            r#"{"model":"test-model","messages":[{"role":"system","content":"s"},{"role":"user","content":"u"}],"max_tokens":64}"#
        );
    }

    /// Fault: take `tool_calls[0]` without the count / name check. ADR-0058 R9: two calls or another
    /// name stay a schema failure even when `content` carries a valid object, and zero calls are one
    /// only when `content` is not a JSON object.
    #[test]
    fn zero_two_or_misnamed_tool_calls_are_a_schema_failure() {
        rt().block_on(async {
            let ok = call_tool_canned(
                br#"{"choices":[{"finish_reason":"tool_calls","message":{"tool_calls":[{"type":"function","function":{"name":"emit_distillation","arguments":"{\"memories\":[]}"}}]}}],"base_resp":{"status_code":0}}"#,
            )
            .await
            .expect("one named call is the answer");
            assert_eq!(ok.json, r#"{"memories":[]}"#);
            assert!(!ok.channel_fallback);
            for (label, body) in [
                (
                    "zero calls, content not JSON",
                    &br#"{"choices":[{"finish_reason":"stop","message":{"content":"memories: none"}}]}"#[..],
                ),
                (
                    "zero calls, content a JSON array",
                    br#"{"choices":[{"finish_reason":"stop","message":{"content":"[{\"memories\":[]}]"}}]}"#,
                ),
                (
                    "zero calls, no content",
                    br#"{"choices":[{"finish_reason":"stop","message":{"role":"assistant"}}]}"#,
                ),
                (
                    "two calls",
                    br#"{"choices":[{"message":{"content":"{\"memories\":[]}","tool_calls":[{"type":"function","function":{"name":"emit_distillation","arguments":"{\"memories\":[]}"}},{"type":"function","function":{"name":"emit_distillation","arguments":"{\"memories\":[]}"}}]}}]}"#,
                ),
                (
                    "other name",
                    br#"{"choices":[{"message":{"content":"{\"memories\":[]}","tool_calls":[{"type":"function","function":{"name":"run_shell","arguments":"{\"memories\":[]}"}}]}}]}"#,
                ),
                (
                    "arguments not a string",
                    br#"{"choices":[{"message":{"tool_calls":[{"type":"function","function":{"name":"emit_distillation","arguments":{"memories":[]}}}]}}]}"#,
                ),
                (
                    "arguments not JSON",
                    br#"{"choices":[{"message":{"tool_calls":[{"type":"function","function":{"name":"emit_distillation","arguments":"memories: none"}}]}}]}"#,
                ),
            ] {
                assert!(
                    matches!(
                        call_tool_canned(body).await,
                        Err(ReasoningProviderError::FailedOutputSchema { attempts: 1 })
                    ),
                    "{label}"
                );
            }
        });
    }

    /// ADR-0058 R9 — fault: no fallback (zero tool calls is always a schema failure). A reply with no
    /// tool call whose `content`, reasoning block stripped, is a JSON object answers from `content`
    /// and says so; an empty `tool_calls` array is zero calls.
    #[test]
    fn a_tool_reply_without_a_tool_call_answers_from_its_content_object() {
        rt().block_on(async {
            for body in [
                &br#"{"choices":[{"finish_reason":"stop","message":{"content":"<think>plan</think>{\"memories\":[]}"}}],"base_resp":{"status_code":0}}"#[..],
                br#"{"choices":[{"finish_reason":"stop","message":{"content":"{\"memories\":[]}","tool_calls":[]}}]}"#,
            ] {
                let answer = call_tool_canned(body)
                    .await
                    .expect("the content object is the answer");
                assert_eq!(answer.json, r#"{"memories":[]}"#);
                assert!(answer.channel_fallback);
            }
        });
    }

    /// Fault: drop the `finish_reason == "length"` check (a truncated reply whose one call
    /// happens to parse would be taken as the answer).
    #[test]
    fn finish_reason_length_is_a_schema_failure() {
        rt().block_on(async {
            assert!(matches!(
                call_tool_canned(
                    br#"{"choices":[{"finish_reason":"length","message":{"tool_calls":[{"type":"function","function":{"name":"emit_distillation","arguments":"{\"memories\":[]}"}}]}}]}"#,
                )
                .await,
                Err(ReasoningProviderError::FailedOutputSchema { attempts: 1 })
            ));
        });
    }

    /// R-32 (a) "disable implicit retries": one `complete_structured` = one transport send, even
    /// on a retryable status. Fault: wrap `send_once` in a retry loop.
    #[test]
    fn complete_structured_sends_exactly_once_on_retry_wait() {
        rt().block_on(async {
            let resolver = resolver_for(example_public_ip());
            let provider = OpenAiCompatibleProvider::new(
                descriptor(),
                "https://api.example.com/v1/chat/completions".to_string(),
                CountingTransport {
                    calls: AtomicU32::new(0),
                    status: 503,
                },
                StaticDecryptor,
                ssrf::CustomEndpointPolicy::default(),
                &resolver,
            )
            .expect("valid endpoint");
            let tenant = TenantId::new();
            let result = provider
                .complete_structured(&ctx(tenant), structured_request())
                .await;
            assert!(matches!(
                result,
                Err(ReasoningProviderError::RetryWait { .. })
            ));
            assert_eq!(provider.transport.calls.load(Ordering::SeqCst), 1);
        });
    }
}
