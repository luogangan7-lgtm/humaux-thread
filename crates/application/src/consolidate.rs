//! `application::consolidate` — Phase 4 T4.6+T4.7: Consolidation worker orchestration
//! (§11.6-11.9, G11-1/G80-29). Pure logic only: DB SQL for `private.memory_consolidation_*`
//! lives in `adapters::consolidate_repo` (through `ConsolidationDbPool`, never a bare
//! `sqlx::PgPool` — G6-DB1/G80-40), and the `ConsolidationRunState`/`AutoMutableMemoryId`/
//! rollup-authority-ceiling invariants live in `humaux_domain::consolidate` (no I/O there
//! either, §3/§78.3). This module is the seam between the two: the `PrivateReasoningPort`
//! boundary the worker calls through, and the pure run-state decisions built on top of it.

use humaux_domain::authority::{AuthorityClass, EvidenceId, MemoryId};
pub use humaux_domain::consolidate::{
    AutoMutableMemoryId, ClassifiedMemoryId, ConsolidationRunState, RollupAuthorityViolation,
    check_rollup_authority_ceiling, classify,
};

/// §11.2.1: which `USER_REASONING` profile a `PrivateReasoningPort` request is bound to —
/// `PrivateReasoningDomainId` scopes "谁的 Key 能处理" separately from tenant/user visibility.
/// Minted here (not `domain::ids`) for the same reason `MemoryId`/`EvidenceId` are minted in
/// `domain::authority`: this module is its first consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PrivateReasoningDomainId(pub uuid::Uuid);

/// §11.2/§74's `UserReasoningProfile` version marker — opaque here, T4.4/T4.5's concern owns
/// its shape; `PrivateReasoningPort` only needs to bind a request to one immutable version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UserReasoningProfileVersion(pub i64);

/// SHA-256 over the request's input manifest — identifies *what* was sent for inference
/// without the payload itself needing to leave the sealed RPC body (§11.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentSha256(pub [u8; 32]);

/// §11.8: one LLM call's purpose. A fixed, closed set — no `Other(String)` — because the
/// receiving `humaux-private-worker` RPC handler branches its own audit/quota accounting on
/// this value (§11.5 BYOK usage accounting) and a stringly-typed escape hatch would defeat
/// that accounting (§78.2 "禁止 stringly-typed domain").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivateReasoningPurpose {
    Distill,
    Consolidate,
    Vision,
}

/// §11.8 verbatim: the sealed request `humaux-consolidation-worker` sends over internal mTLS.
/// Carries no DB capability and no raw content — only identifiers and a hash of what the
/// caller wants inferred over; the private-worker RPC handler fetches/decrypts everything else
/// itself (§11.8: "payload 只通过内部 mTLS RPC body 传递，不携带 DB capability").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedPrivateReasoningRequest {
    pub reasoning_domain_id: PrivateReasoningDomainId,
    pub profile_version: UserReasoningProfileVersion,
    pub input_manifest_hash: ContentSha256,
    pub purpose: PrivateReasoningPurpose,
}

/// Opaque reference into the provider call log (§11.5 usage accounting) — this crate never
/// interprets it, only threads it through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderTraceRef(pub String);

/// §11.8 verbatim: what `PrivateReasoningPort::infer` returns on success.
///
/// `output_bytes` is the inference output *over private memory content* (§11.8) — a derived
/// `#[derive(Debug)]` would print it verbatim on any stray `{:?}` (a log line, a panic
/// message), which is exactly the class of leak `crates/application/src/auth.rs`'s
/// `EncodedPasswordHash`/`PlaintextCode`/`CodeHash` hand-written `Debug` impls already exist to
/// prevent for other private-content-shaped fields. [`Debug`] below is hand-written for the
/// same reason: only the identifiers survive, never the bytes.
#[derive(Clone, PartialEq, Eq)]
pub struct PrivateReasoningResult {
    pub output_bytes: Vec<u8>,
    pub output_sha256: ContentSha256,
    pub provider_trace: ProviderTraceRef,
}

impl std::fmt::Debug for PrivateReasoningResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrivateReasoningResult")
            .field(
                "output_bytes",
                &format_args!("[REDACTED; {} bytes]", self.output_bytes.len()),
            )
            .field("output_sha256", &self.output_sha256)
            .field("provider_trace", &self.provider_trace)
            .finish()
    }
}

/// `PrivateReasoningPort::infer`'s failure — adapter-local (the RPC transport error, or the
/// private-worker's own `ErrorCode`/`DegradeCode` collapsed to a message), not one of the
/// workspace's two frozen domain error enums (§52) reused for a third purpose.
///
/// The wrapped message is upstream text this crate does not control — an mTLS transport error
/// or a provider SDK error string, either of which can carry request fragments (verified live:
/// constructing one from a plaintext-secret-bearing upstream string and printing it with `{}`
/// reproduces the secret verbatim). Field is private with a redacting [`Display`]/[`Debug`]
/// (same convention as `auth.rs`'s `EncodedPasswordHash`/`CodeHash`) — callers get a stable
/// fingerprint for log correlation, never the raw text.
#[derive(Clone, PartialEq, Eq)]
pub struct PrivateReasoningError(String);

impl PrivateReasoningError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    /// Short, stable, non-reversible correlation tag for logs — enough to match two log lines
    /// about the same underlying error without ever printing its text.
    fn fingerprint(&self) -> String {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(self.0.as_bytes());
        hex::encode(&digest[..4])
    }
}

impl std::fmt::Debug for PrivateReasoningError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PrivateReasoningError(REDACTED, fingerprint={})",
            self.fingerprint()
        )
    }
}

impl std::fmt::Display for PrivateReasoningError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PrivateReasoningPort error (redacted, fingerprint={})",
            self.fingerprint()
        )
    }
}

impl std::error::Error for PrivateReasoningError {}

/// §11.8 verbatim: `humaux-consolidation-worker`'s only inference interface. Implemented in
/// `humaux-private-worker` (over internal mTLS) — this crate defines the boundary, not the
/// transport; a fake implementation drives this module's own tests without any network stack.
///
/// §11.8 hard boundary the trait's *implementation* must hold (enforced at the private-worker
/// RPC handler, out of this crate's file scope, tracked as G11-R1):
/// ```text
/// MAY decrypt USER BYOK and call UserReasoningProvider
/// MUST NOT accept memory_id to mutate
/// MUST NOT write memory_records / memory_evidence / rollups
/// MUST NOT receive any PgPool/Repository capability from caller
/// ```
#[async_trait::async_trait]
pub trait PrivateReasoningPort: Send + Sync {
    async fn infer(
        &self,
        req: SealedPrivateReasoningRequest,
    ) -> Result<PrivateReasoningResult, PrivateReasoningError>;
}

/// §11.7 Run State: what a caller who just finished [`adapters::consolidate_repo::
/// select_and_materialize_inputs`][select] should do next, given whether the snapshot
/// produced any candidates. Pure decision, no I/O — the actual `RUNNING`/`SUCCEEDED_NO_OUTPUT`
/// DB writes are the repo's job; this is the piece of logic a worker's control loop can unit
/// test without a database.
///
/// [select]: ../../humaux_adapters/consolidate_repo/fn.select_and_materialize_inputs.html
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NextStep {
    /// Non-empty input set: call [`PrivateReasoningPort::infer`] next.
    RunInference,
    /// Empty input set: skip inference entirely and publish `SUCCEEDED_NO_OUTPUT` directly —
    /// §11.7 "健康终态，不得与 FAILED 混在一起", and there is nothing to send a `Distill`/
    /// `Consolidate` request about, so calling the port at all would just burn a BYOK request
    /// for a guaranteed-empty result (§11.5.1 "全自动不等于偷偷烧用户 BYOK").
    SkipToNoOutput,
}

pub fn next_step(input_count: usize) -> NextStep {
    if input_count == 0 {
        NextStep::SkipToNoOutput
    } else {
        NextStep::RunInference
    }
}

/// One source Memory's Authority, as the caller (which already holds each source's Authority
/// row for other reasons — it just read them to build the rollup content) reports it — used
/// only for [`validate_rollup_before_publish`]'s ceiling check, never persisted by this crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceAuthority {
    pub memory_id: MemoryId,
    pub evidence_id: EvidenceId,
    pub class: AuthorityClass,
}

/// §11.9 pre-publish gate: reject a rollup whose proposed class would outrank every source it
/// closes over, before `adapters::consolidate_repo::publish_rollup` ever runs — the repo layer
/// enforces the FK-based source closure (§11.6), this enforces the Authority ceiling (§11.9),
/// and the two checks are independent (see `RollupAuthorityViolation::NoSourceClosure`, which
/// this function surfaces for an empty `sources` too, though callers should already have
/// routed empty-sources runs to [`NextStep::SkipToNoOutput`] upstream).
pub fn validate_rollup_before_publish(
    rollup_class: AuthorityClass,
    sources: &[SourceAuthority],
) -> Result<(), RollupAuthorityViolation> {
    let classes: Vec<AuthorityClass> = sources.iter().map(|s| s.class).collect();
    check_rollup_authority_ceiling(rollup_class, &classes)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakePort {
        reply: Result<PrivateReasoningResult, PrivateReasoningError>,
    }

    #[async_trait::async_trait]
    impl PrivateReasoningPort for FakePort {
        async fn infer(
            &self,
            _req: SealedPrivateReasoningRequest,
        ) -> Result<PrivateReasoningResult, PrivateReasoningError> {
            self.reply.clone()
        }
    }

    /// G11-R1 positive sentinel (§11.8): `consolidation -> PrivateReasoningPort.infer() ->
    /// valid result`.
    #[tokio::test]
    async fn private_reasoning_port_positive_sentinel() {
        let port = FakePort {
            reply: Ok(PrivateReasoningResult {
                output_bytes: b"rollup text".to_vec(),
                output_sha256: ContentSha256([7u8; 32]),
                provider_trace: ProviderTraceRef("trace-1".into()),
            }),
        };
        let req = SealedPrivateReasoningRequest {
            reasoning_domain_id: PrivateReasoningDomainId(uuid::Uuid::nil()),
            profile_version: UserReasoningProfileVersion(1),
            input_manifest_hash: ContentSha256([1u8; 32]),
            purpose: PrivateReasoningPurpose::Consolidate,
        };
        let result = port.infer(req).await.expect("fake port succeeds");
        assert_eq!(result.output_bytes, b"rollup text");
    }

    #[test]
    fn next_step_empty_input_skips_inference() {
        assert_eq!(next_step(0), NextStep::SkipToNoOutput);
        assert_eq!(next_step(1), NextStep::RunInference);
        assert_eq!(next_step(160), NextStep::RunInference);
    }

    #[test]
    fn validate_rollup_before_publish_rejects_ceiling_violation() {
        let sources = [SourceAuthority {
            memory_id: MemoryId::new(),
            evidence_id: EvidenceId::new(),
            class: AuthorityClass::UserPreference,
        }];
        let err = validate_rollup_before_publish(AuthorityClass::ProjectConstraint, &sources)
            .unwrap_err();
        assert_eq!(
            err,
            RollupAuthorityViolation::ExceedsSource {
                rollup_class: AuthorityClass::ProjectConstraint,
                max_source_class: AuthorityClass::UserPreference,
            }
        );
    }

    #[test]
    fn validate_rollup_before_publish_accepts_ceiling_respected() {
        let sources = [SourceAuthority {
            memory_id: MemoryId::new(),
            evidence_id: EvidenceId::new(),
            class: AuthorityClass::ProjectDecision,
        }];
        assert!(validate_rollup_before_publish(AuthorityClass::UserPreference, &sources).is_ok());
    }
}
