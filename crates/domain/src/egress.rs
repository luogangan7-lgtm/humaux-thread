//! `domain::egress` — §7.3's `EgressPermit` unique-egress topology and §83.4's
//! `OutboundPurpose` registry closure (T4.1).
//!
//! §7.0's frozen判定线: "未经 `EgressPermit` 且未记账的出境 = 违规". §7.3 makes that a
//! *topology*, not a discipline: `EgressPermit`'s fields are all private and it has no `pub`
//! constructor, `Default`, `Clone`, or `Deserialize` — the only way any code, anywhere in the
//! workspace, can obtain one is [`authorize`], the sole `pub` entry point in this module. A
//! struct literal `EgressPermit { .. }` outside this file does not compile (private fields);
//! [`EgressPermit::issue`] itself is not even `pub(crate)` so that no *other* module inside
//! this crate can mint one either — [`authorize`] is the single caller, enforced by ordinary
//! Rust privacy rather than convention (repo CLAUDE.md 铁律: "把纪律换成拓扑").
//!
//! §83.4 additionally closes the transport side: [`OutboundPurpose`]'s variant set is the
//! `external-egress-registry` fenced block's first column, spelled identically (SCREAMING_SNAKE
//! variant idents, `#[allow(non_camel_case_types)]`) so `xtask architecture-check`'s G80-3 set
//! comparison needs no case-folding step to drift out of sync with the registry text it quotes.
//! Its three `private-data` variants (`USER_REASONING` / `RETRIEVAL_EMBEDDING` /
//! `RETRIEVAL_RERANK`) each carry an owned [`EgressPermit`] — the registry's "必须类型化携带
//! EgressPermit" requirement becomes "there is no `OutboundPurpose::USER_REASONING` value that
//! does not already contain one", checked by the compiler, not by a runtime assertion.
//!
//! §7.5.1's fail-closed rule now has one executable checkpoint: [`authorize`] refuses to mint
//! a permit for a `RETRIEVAL_EMBEDDING`/`RETRIEVAL_RERANK` purpose whose caller-supplied
//! [`DataClass`] is [`DataClass::SecretMaterial`] — §7.5's "SECRET_MATERIAL 的 Card/Query 不得
//! 进入外部 embedding/rerank" line, made structural rather than a discipline every retrieval
//! caller has to remember. `USER_REASONING` carries no such per-purpose ceiling (§7.5 only
//! names the two retrieval purposes), so [`authorize`] leaves it unrestricted by `data_class`.
//!
//! Deliberately out of this module's scope (later tasks, tracked so nothing here silently
//! forecloses them):
//! - §83.4's `NetworkPermit` (a thinner wrapper `ExternalHttpTransport::execute` would take)
//!   is not introduced: this task's own scope names `ExternalCall`'s signature as
//!   `(EgressPermit, AuthorizedEgressPayload)` directly, and folding the private-data purposes'
//!   permit straight into [`OutboundPurpose`] gives the identical compile-time guarantee with
//!   one fewer type. Add `NetworkPermit` only if a non-private-data `OutboundPurpose` variant
//!   later needs its own permit shape that isn't `EgressPermit`.
//! - The Processor Registry (`control.processors` et al., §7 末) — the DB-backed table that
//!   would resolve a real `ProcessorId` — belongs to a later task; [`ProcessorId`] here is a
//!   minimal local newtype, not that eventual identifier.
//! - §7.2's *tenant-level* switch (`control.egress_policies.private_retrieval_external_allowed`,
//!   `migrations/0051_egress_policies.sql`) is **not** consulted by [`authorize`] — the table
//!   exists, but nothing in this module reads it yet. A caller must check it itself before
//!   calling `authorize` for a retrieval purpose. Similarly, [`authorize`] does not reserve an
//!   `ops.data_disclosures` row (§7.4) — `humaux_adapters::disclosure::reserve_private`/
//!   `reserve_retrieval` already exist and read `permit.data_class()`/`permit.purpose()`
//!   directly, but the caller must sequence `authorize` → `reserve` → [`ExternalCall::call`] →
//!   `finalize` itself; nothing here enforces that ordering. §7.0's judgment line is therefore
//!   only half-closed by this module: the *topology* (nobody mints a permit except here, and a
//!   `SECRET_MATERIAL` retrieval permit cannot be minted at all) is real, but "was this tenant
//!   allowed to egress at all" and "was this call recorded" remain caller discipline, not
//!   compiler-enforced facts, until a later task threads them through here.
//!
//! [`DataClass`]: crate::dataclass::DataClass

use std::fmt;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::dataclass::DataClass;
use crate::error::ErrorCode;
use crate::ids::TenantId;

/// Egress processor identifier (§7 Processor Registry `control.processors.processor_id`,
/// eventual DB-backed shape). Local to this module for T4.1 — see module doc's scope note.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProcessorId(pub Uuid);

/// §7.3's `purpose` field on [`EgressPermit`]: the private-data subset of §83.4's
/// [`OutboundPurpose`] registry — the only three purposes an `EgressPermit` can ever be issued
/// for (the other seven registry entries are non-private and carry no `EgressPermit` at all,
/// §83.4 registry column 2). Kept as its own small enum rather than reusing `OutboundPurpose`
/// directly: `OutboundPurpose`'s private-data variants already contain an `EgressPermit`, so a
/// `permit.purpose: OutboundPurpose` field would make `EgressPermit` contain `OutboundPurpose`
/// contain `EgressPermit` — an infinite-size cycle. This tag breaks that cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivateDataPurpose {
    /// §83.4 registry row 1 / §11.
    UserReasoning,
    /// §83.4 registry row 2 / §19.
    RetrievalEmbedding,
    /// §83.4 registry row 3 / §19.
    RetrievalRerank,
}

/// §7.3: the sole out-bound authorization token. Every field is private; there is no `pub`
/// constructor, `Default`, `Clone`, or `Deserialize` impl (deriving any of those would open a
/// second construction path, defeating the point) — the only way to obtain one is [`authorize`].
#[derive(Debug)]
pub struct EgressPermit {
    grant_id: Uuid,
    tenant_id: TenantId,
    processor: ProcessorId,
    purpose: PrivateDataPurpose,
    /// §7.3 frozen field list / §7.4 ledger `data_class` column's authority — see [`authorize`]
    /// for the one fail-closed rule this field lets this module enforce (§7.5.1).
    data_class: DataClass,
    payload_sha256: [u8; 32],
    expires_at: Instant,
}

impl EgressPermit {
    /// The sole signing entry point — not `pub`, not even `pub(crate)`: only [`authorize`],
    /// which lives in this same module, can name it. §7.3: "`EgressPermit` 在 policy crate 外
    /// 无任何构造路径".
    fn issue(
        tenant_id: TenantId,
        processor: ProcessorId,
        purpose: PrivateDataPurpose,
        data_class: DataClass,
        payload_sha256: [u8; 32],
        ttl: Duration,
    ) -> Self {
        Self {
            grant_id: Uuid::now_v7(),
            tenant_id,
            processor,
            purpose,
            data_class,
            payload_sha256,
            expires_at: Instant::now() + ttl,
        }
    }

    /// §7.4 disclosure-ledger join key (`ops.data_disclosures.grant_id`).
    pub fn grant_id(&self) -> Uuid {
        self.grant_id
    }

    /// Read-only view for the ledger writer and the transport adapter — construction stays
    /// confined to [`issue`](Self::issue).
    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    /// Read-only view of the processor this permit was minted for.
    pub fn processor(&self) -> ProcessorId {
        self.processor
    }

    /// Read-only view of which private-data purpose authorized this permit.
    pub fn purpose(&self) -> PrivateDataPurpose {
        self.purpose
    }

    /// §7.4 ledger `data_class` column's authoritative source — the sensitivity grade
    /// [`authorize`] was told this payload carries.
    pub fn data_class(&self) -> DataClass {
        self.data_class
    }

    /// The exact payload digest this permit authorizes — an [`ExternalCall`] adapter must
    /// reject any [`AuthorizedEgressPayload`] whose own digest does not equal this one (§7.3:
    /// "Permit 不能被拿去发送另一份正文").
    pub fn payload_sha256(&self) -> [u8; 32] {
        self.payload_sha256
    }

    /// Whether this permit has expired as of `now` (a stale permit must not be honored by the
    /// egress adapter — §7.3's reservation is time-bounded, not indefinite).
    pub fn is_expired(&self, now: Instant) -> bool {
        now >= self.expires_at
    }
}

/// §7.3's other topology half: the payload an [`ExternalCall`] adapter is allowed to send.
/// `sha256` is derived from `bytes` at construction time — there is no way to build one whose
/// digest lies about its own content, so a mismatch against a permit can only mean "this is
/// genuinely a different payload than the one that was authorized", never a spoofed digest.
///
/// No `Clone`: `bytes` is the literal outbound private payload (e.g. §11's plaintext user
/// reasoning content) — one owner is enough for the reserve → call → finalize sequence this
/// type exists for, and a second owner is a second place plaintext can leak from. Add it back
/// only against a concrete caller need, not preemptively.
pub struct AuthorizedEgressPayload {
    bytes: Vec<u8>,
    // ponytail: `Vec<u8>` not `bytes::Bytes` — nothing in this task's scope shares this buffer
    // across clones yet; swap to `bytes::Bytes` if a zero-copy fan-out need shows up later.
    sha256: [u8; 32],
}

/// Deliberately redacting: the derived `Debug` this replaces printed `bytes` in full — every
/// one of `tracing::debug!(?payload)`, a panicking `.expect()`, or a failed `assert_eq!` would
/// write this payload's plaintext (§11 user reasoning text, or worse, an embedded credential)
/// straight into logs/test output. Same shape as `PlaintextApiKey`/`PlaintextCode`/`CodeHash`/
/// `EncodedPasswordHash`'s hand-written `Debug` impls elsewhere in this workspace (§7.5).
impl fmt::Debug for AuthorizedEgressPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthorizedEgressPayload")
            .field("len", &self.bytes.len())
            .field("sha256_prefix", &hex_prefix(&self.sha256))
            .finish()
    }
}

/// First 4 bytes of a digest, hex-encoded — enough to eyeball-correlate a log line with a
/// specific payload without printing anything that helps reconstruct its content.
fn hex_prefix(sha256: &[u8; 32]) -> String {
    sha256[..4].iter().map(|b| format!("{b:02x}")).collect()
}

impl AuthorizedEgressPayload {
    /// Computes and stores the payload's own SHA-256 alongside it — see the struct's doc for
    /// why this makes a mismatched digest structurally impossible to construct.
    pub fn new(bytes: Vec<u8>) -> Self {
        let sha256 = Sha256::digest(&bytes).into();
        Self { bytes, sha256 }
    }

    /// The exact bytes an [`ExternalCall`] adapter is authorized to transmit.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// This payload's own digest, computed once at construction (see struct doc).
    pub fn sha256(&self) -> [u8; 32] {
        self.sha256
    }
}

/// §7.3: "所有出境 adapter 的唯一签名 —— 没有 permit 就没有函数可调". Object-safe
/// (`#[async_trait]`, matching this workspace's existing convention in `humaux-adapters`) so an
/// application-layer caller can hold `Arc<dyn ExternalCall>` and swap implementations without
/// ever naming `humaux-infra-egress` directly.
#[async_trait::async_trait]
pub trait ExternalCall: Send + Sync {
    /// Sends `payload` under `permit`. An implementation MUST verify
    /// `payload.sha256() == permit.payload_sha256()` before doing anything network-visible and
    /// reject with [`ErrorCode::Forbidden`] on mismatch (§7.3) — this trait's signature makes
    /// skipping the permit impossible, but it cannot make an implementation perform the check;
    /// `crates/infra-egress/src/http.rs` is where that verification actually lives.
    async fn call(
        &self,
        permit: &EgressPermit,
        payload: &AuthorizedEgressPayload,
    ) -> Result<Vec<u8>, ErrorCode>;
}

/// §83.4's logical `external-egress-registry`, column 1, verbatim order — the exact set
/// `xtask architecture-check`'s G80-3 (c) compares [`OutboundPurpose`]'s variant names against.
#[allow(non_camel_case_types)]
#[derive(Debug)]
pub enum OutboundPurpose {
    /// private-data · §11 — must carry the permit that authorized it (§83.4 判据3).
    USER_REASONING(EgressPermit),
    /// private-data · §19.
    RETRIEVAL_EMBEDDING(EgressPermit),
    /// private-data · §19.
    RETRIEVAL_RERANK(EgressPermit),
    /// released-only · §12.
    PUBLIC_REASONING,
    /// metadata · §71.
    BILLING,
    /// metadata · §74.
    TRANSACTIONAL_EMAIL,
    /// repo-data · §27.
    GIT_PROVIDER,
    /// metadata · §33.
    OAUTH_METADATA,
    /// public-data · §12.
    PUBLIC_SOURCE_FETCH,
    /// metadata · §42.
    DEADMAN_HEALTHCHECK,
}

/// §7.3's sole `pub` entry point: mints an [`EgressPermit`] bound to `payload`'s own digest,
/// after the one fail-closed check this module owns (§7.5.1 below).
///
/// Still policy-free beyond that (see this module's doc "out of scope" note): the §7.2
/// tenant-level `private_retrieval_external_allowed` switch and the §7.4 disclosure-ledger
/// reservation are caller-side steps this function does not perform — its job is the §7.3
/// topology guarantee (nobody can mint a permit except here, a permit is forever bound to one
/// exact payload digest) plus the §7.5.1 data-class ceiling below.
///
/// # Errors
///
/// [`ErrorCode::Forbidden`] if `purpose` is [`PrivateDataPurpose::RetrievalEmbedding`] or
/// [`PrivateDataPurpose::RetrievalRerank`] and `data_class` is [`DataClass::SecretMaterial`]
/// — §7.5: "SECRET_MATERIAL 的 Card/Query 不得进入外部 embedding/rerank". This is the only
/// case `authorize` refuses; every other `(purpose, data_class)` combination mints normally.
pub fn authorize(
    tenant_id: TenantId,
    processor: ProcessorId,
    purpose: PrivateDataPurpose,
    data_class: DataClass,
    payload: &AuthorizedEgressPayload,
    ttl: Duration,
) -> Result<EgressPermit, ErrorCode> {
    let is_external_retrieval = matches!(
        purpose,
        PrivateDataPurpose::RetrievalEmbedding | PrivateDataPurpose::RetrievalRerank
    );
    if is_external_retrieval && data_class == DataClass::SecretMaterial {
        return Err(ErrorCode::Forbidden);
    }
    Ok(EgressPermit::issue(
        tenant_id,
        processor,
        purpose,
        data_class,
        payload.sha256(),
        ttl,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(bytes: &[u8]) -> AuthorizedEgressPayload {
        AuthorizedEgressPayload::new(bytes.to_vec())
    }

    #[test]
    fn authorize_binds_permit_to_payload_digest() {
        let p = payload(b"hello");
        let permit = authorize(
            TenantId::new(),
            ProcessorId(Uuid::now_v7()),
            PrivateDataPurpose::UserReasoning,
            DataClass::Private,
            &p,
            Duration::from_secs(60),
        )
        .unwrap();
        assert_eq!(permit.payload_sha256(), p.sha256());
        assert_eq!(permit.data_class(), DataClass::Private);
    }

    /// §7.5.1 fail-closed checkpoint: a `SECRET_MATERIAL`-graded payload must never mint a
    /// permit for either external-retrieval purpose (§7.5's own two named purposes).
    #[test]
    fn authorize_refuses_secret_material_for_both_retrieval_purposes() {
        let p = payload(b"secret");
        for purpose in [
            PrivateDataPurpose::RetrievalEmbedding,
            PrivateDataPurpose::RetrievalRerank,
        ] {
            let result = authorize(
                TenantId::new(),
                ProcessorId(Uuid::now_v7()),
                purpose,
                DataClass::SecretMaterial,
                &p,
                Duration::from_secs(60),
            );
            assert_eq!(result.err(), Some(ErrorCode::Forbidden));
        }
    }

    /// §7.5 names only the two retrieval purposes — `USER_REASONING` carries no `data_class`
    /// ceiling, so `SECRET_MATERIAL` must still mint normally for it.
    #[test]
    fn authorize_does_not_restrict_user_reasoning_by_data_class() {
        let p = payload(b"secret");
        let permit = authorize(
            TenantId::new(),
            ProcessorId(Uuid::now_v7()),
            PrivateDataPurpose::UserReasoning,
            DataClass::SecretMaterial,
            &p,
            Duration::from_secs(60),
        );
        assert!(permit.is_ok());
    }

    /// §7.5: a derived a printed `AuthorizedEgressPayload` must never leak the plaintext
    /// bytes it carries, in `{:?}` or `{:#?}` — same discipline as `PlaintextApiKey`/
    /// `CodeHash`'s hand-written `Debug` impls.
    #[test]
    fn authorized_egress_payload_debug_never_prints_plaintext() {
        let secret = b"sk-live-USER-BYOK-KEY-abc123 plus private user reasoning text";
        let p = AuthorizedEgressPayload::new(secret.to_vec());
        let rendered = format!("{p:?}");
        assert!(!rendered.contains("sk-live"));
        assert!(!rendered.contains("private user reasoning"));
        assert!(rendered.contains("len"));
    }

    #[test]
    fn payload_digest_is_content_derived_not_asserted() {
        // §7.3: a payload's digest is computed from its own bytes — two different byte
        // strings can never collide into "the same authorized content" by construction here
        // (an actual SHA-256 collision is not this test's concern; the point is there is no
        // API to hand-supply a mismatched digest at all).
        let a = payload(b"alpha");
        let b = payload(b"beta");
        assert_ne!(a.sha256(), b.sha256());
    }

    #[test]
    fn expiry_is_observable_by_the_adapter() {
        let p = payload(b"x");
        let permit = authorize(
            TenantId::new(),
            ProcessorId(Uuid::now_v7()),
            PrivateDataPurpose::RetrievalEmbedding,
            DataClass::Private,
            &p,
            Duration::from_millis(0),
        )
        .unwrap();
        // TTL of 0 — `Instant::now()` moving forward at all (which it always does between
        // `issue` and this check) puts us at-or-past `expires_at`.
        std::thread::sleep(Duration::from_millis(1));
        assert!(permit.is_expired(Instant::now()));
    }

    #[test]
    fn outbound_purpose_private_variants_carry_a_permit_by_construction() {
        // Not a runtime assertion about behavior — a compile-time fact demonstrated by
        // construction: this line would not compile if `USER_REASONING` took no payload.
        let p = payload(b"x");
        let permit = authorize(
            TenantId::new(),
            ProcessorId(Uuid::now_v7()),
            PrivateDataPurpose::UserReasoning,
            DataClass::Private,
            &p,
            Duration::from_secs(60),
        )
        .unwrap();
        let purpose = OutboundPurpose::USER_REASONING(permit);
        match purpose {
            OutboundPurpose::USER_REASONING(inner) => {
                assert_eq!(inner.payload_sha256(), p.sha256());
            }
            _ => unreachable!(),
        }
    }
}
