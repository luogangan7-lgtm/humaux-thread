//! `domain::evidence` — `EvidenceOriginClass` / `InstructionDisposition` (§8.7 / §59).
//!
//! §8.7: `origin_class` is stamped by **ingress path + AuthContext**, never self-reported by
//! the client/model through a JSON parameter — this is the first boundary against private
//! Memory Poisoning. Taint must survive derivation: an Observation/Memory's provenance must
//! trace back to its origin Evidence, and Agent/LLM summarization must never upgrade
//! `origin_class` (§59.1 I7: AuthorityPolicy must not let summarization launder
//! UploadedArtifact/ExternalContent/ToolResult/AuthenticatedAgent into a high-authority
//! class). `SystemMigration` only inherits the legacy Evidence's original origin; with no
//! recoverable legacy origin it defaults to the lower-privilege treatment.
//!
//! Note: the exhaustiveness test below pins variant *count and shape*, not the verbatim
//! wire name of each variant. Verbatim string pinning is deferred to the §78.2 DB↔Rust
//! contract-test card, not covered here.

/// Evidence's ingress origin classification, a frozen closed set of 9 variants (§8.7).
///
/// A user uploading a document != the user personally asserting every sentence in it:
/// `UploadedArtifact` / `ExternalContent` remain low behavioral-authority sources even when
/// the upload action itself was performed by an authenticated user.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceOriginClass {
    /// The user typed or spoke this directly (§8.7).
    DirectUserInput,
    /// The user explicitly confirmed this content (§8.7).
    UserConfirmed,
    /// A tenant administrator asserted this (§8.7).
    TenantAdmin,
    /// An authenticated agent produced this (§8.7).
    AuthenticatedAgent,
    /// A trusted connector ingested this (§8.7).
    TrustedConnector,
    /// A tool call returned this (§8.7).
    ToolResult,
    /// The user uploaded this as a file/artifact (§8.7) — low behavioral authority.
    UploadedArtifact,
    /// This was ingested from outside the trust boundary (§8.7) — low behavioral authority.
    ExternalContent,
    /// Carried over from a legacy Evidence during system migration (§8.7).
    SystemMigration,
}

/// Whether an instruction inside one Evidence is eligible to influence Agent behavior
/// (the behavioral-authority binary in the §8.7 context).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstructionDisposition {
    /// Data only — must never be executed as an instruction to the Agent.
    DataOnly,
    /// Eligible to influence Agent behavior (still bound by the §59.1 I7 ceiling).
    BehaviorEligible,
}

/// Content-addressed identity anchor for one Evidence's raw payload bytes (§8.1 / §48.0①).
///
/// Backs `private.evidence_objects.payload_sha256` and is the anchor for authority /
/// migration / provenance / idempotency (§8.1) — never for "semantic dedup", that is
/// `canonical_text_sha256`'s job (§8.1), a separate derived field this type has no relation
/// to.
///
/// **Sole construction point is [`payload_sha256`]** (§48.0① G80-22): the field is private,
/// there is no `pub` constructor, no `From<Vec<u8>>`, and no `Default`. All four write paths
/// (ingest / replay §68.1 / cutover reconciliation §68.3 step 5③ / repair §65) must go through
/// that one free function so the encoding is enforced at a single call site instead of being
/// re-derived per caller.
///
/// **Widened 口径 (card 21, ADR-0016's registered debt).** There is a second, non-hashing way
/// to obtain this type — [`EvidencePayloadSha256::from_stored_digest`], the read-back of an
/// already-persisted digest — because a §16.1 fingerprint that cannot be recomputed from the
/// stored row is not an audit fingerprint. `architecture-check` therefore no longer counts "one
/// construction site" but "**every** `EvidencePayloadSha256(` construction site lives in this
/// module, and there are exactly two: one hasher, one read-back". That still forbids what
/// G80-22 exists to forbid — a second crate deciding the §8.1 encoding for itself — while
/// allowing the one operation that decides nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EvidencePayloadSha256([u8; 32]);

impl EvidencePayloadSha256 {
    /// Lowercase hex rendering of the 32-byte digest (§8.1) — a read-only projection, not a
    /// second construction path.
    pub fn to_hex(&self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Raw digest bytes, for writing into a `bytea` column (§8.1) — a read-only projection,
    /// the mirror image of [`Self::from_stored_digest`].
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// **Read-back**, not a second hasher (ADR-0016's registered debt; §48.0① G80-22's
    /// widened 口径).
    ///
    /// Adopts a digest that is *already persisted* — `private.evidence_objects.payload_sha256`
    /// — so a later run can name the same Evidence anchor without re-deriving it from bytes it
    /// no longer has. The raw remember-time bytes are not retained, so before this existed the
    /// only way to obtain an [`EvidencePayloadSha256`] was to hash *something*, and the §16.1
    /// distill fingerprint hashed a re-rendered canonical jsonb of `events.payload` instead.
    /// That made `private.processing_runs.source_hash` un-recomputable from storage: the run
    /// row's own `evidence_payload_sha256[]` (the Evidence's real §8.1 anchor) was **not** the
    /// value the fingerprint had hashed, contradicting migration 0064's column comment ("the
    /// set `source_hash` hashes") and voiding the §16.1.1 audit property that a fingerprint can
    /// be reproduced from the persisted row alone.
    ///
    /// This function performs no hashing, no normalization and no transcoding — it only
    /// validates width. `payload_sha256` therefore remains the sole point where the §8.1
    /// *encoding* is decided, which is what G80-22 actually protects; a read-back cannot
    /// introduce a second encoding because it never encodes anything. Fail-closed on any
    /// length other than 32 bytes (`None`), so a truncated or NULL-ish column can never be
    /// laundered into a well-typed anchor.
    pub fn from_stored_digest(stored: &[u8]) -> Option<Self> {
        let digest: [u8; 32] = <[u8; 32]>::try_from(stored).ok()?;
        Some(EvidencePayloadSha256(digest))
    }
}

/// Sole constructor for [`EvidencePayloadSha256`] (§48.0① G80-22).
///
/// Hashes exactly the bytes given: **no trim, no Unicode NFC/NFKC normalization, no newline
/// rewriting, no transcoding** (§8.1). The encoding is written into the function body, not a
/// parameter — changing it requires changing this signature, visible at compile time, so the
/// §68.3 step 5③ byte-for-byte reconciliation can never silently compare two different
/// encodings.
pub fn payload_sha256(bytes: &[u8]) -> EvidencePayloadSha256 {
    use sha2::{Digest, Sha256};
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    EvidencePayloadSha256(digest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evidence_origin_class_has_exactly_nine_variants() {
        let all = [
            EvidenceOriginClass::DirectUserInput,
            EvidenceOriginClass::UserConfirmed,
            EvidenceOriginClass::TenantAdmin,
            EvidenceOriginClass::AuthenticatedAgent,
            EvidenceOriginClass::TrustedConnector,
            EvidenceOriginClass::ToolResult,
            EvidenceOriginClass::UploadedArtifact,
            EvidenceOriginClass::ExternalContent,
            EvidenceOriginClass::SystemMigration,
        ];
        assert_eq!(all.len(), 9);

        fn assert_exhaustive(c: EvidenceOriginClass) {
            match c {
                EvidenceOriginClass::DirectUserInput
                | EvidenceOriginClass::UserConfirmed
                | EvidenceOriginClass::TenantAdmin
                | EvidenceOriginClass::AuthenticatedAgent
                | EvidenceOriginClass::TrustedConnector
                | EvidenceOriginClass::ToolResult
                | EvidenceOriginClass::UploadedArtifact
                | EvidenceOriginClass::ExternalContent
                | EvidenceOriginClass::SystemMigration => {}
            }
        }
        for c in all {
            assert_exhaustive(c);
        }
    }

    #[test]
    fn instruction_disposition_has_exactly_two_variants() {
        fn assert_exhaustive(d: InstructionDisposition) {
            match d {
                InstructionDisposition::DataOnly | InstructionDisposition::BehaviorEligible => {}
            }
        }
        assert_exhaustive(InstructionDisposition::DataOnly);
        assert_exhaustive(InstructionDisposition::BehaviorEligible);
    }

    /// The read-back is the exact inverse of the hasher's own output: no re-hashing, no
    /// normalization. This is the property that makes §16.1's fingerprint recomputable from
    /// `private.processing_runs` alone. Fault injection: make `from_stored_digest` hash its
    /// argument instead of adopting it and this goes red.
    #[test]
    fn from_stored_digest_adopts_the_persisted_bytes_without_rehashing() {
        let hashed = payload_sha256(b"{\"a\":1}");
        let persisted: Vec<u8> = hashed.as_bytes().to_vec();
        let read_back = EvidencePayloadSha256::from_stored_digest(&persisted)
            .expect("32-byte digest reads back");
        assert_eq!(read_back, hashed);
        assert_eq!(read_back.to_hex(), hashed.to_hex());
        // Not the same thing as hashing the stored digest — the difference the old fingerprint
        // path got wrong in the other direction (it hashed a re-rendering instead of reading).
        assert_ne!(payload_sha256(&persisted), hashed);
    }

    /// §8.1 fail-closed: anything that is not exactly 32 bytes is not an anchor.
    #[test]
    fn from_stored_digest_refuses_any_width_but_32() {
        assert!(EvidencePayloadSha256::from_stored_digest(&[]).is_none());
        assert!(EvidencePayloadSha256::from_stored_digest(&[0u8; 31]).is_none());
        assert!(EvidencePayloadSha256::from_stored_digest(&[0u8; 33]).is_none());
        assert!(EvidencePayloadSha256::from_stored_digest(&[0u8; 32]).is_some());
    }

    /// §8.1: two calls on identical bytes must produce the identical anchor.
    #[test]
    fn payload_sha256_is_stable_for_identical_bytes() {
        let a = payload_sha256(b"hello world");
        let b = payload_sha256(b"hello world");
        assert_eq!(a, b);
        assert_eq!(a.to_hex(), b.to_hex());
    }

    /// §8.1: a single-byte difference must change the digest (avalanche sanity check, not a
    /// SHA-256 correctness proof).
    #[test]
    fn payload_sha256_changes_on_one_byte_difference() {
        let a = payload_sha256(b"hello world");
        let b = payload_sha256(b"hello worlD");
        assert_ne!(a, b);
    }

    /// §8.1 / §48.0①: "不 trim、不做 Unicode NFC/NFKC、不改换行、不转码" — a BOM prefix, a
    /// CRLF-vs-LF line ending, and an NFC-vs-NFD form of the same visible text must each hash
    /// differently. Any of these coming out equal would mean a normalization step crept in.
    #[test]
    fn payload_sha256_does_not_normalize_bom_crlf_or_unicode_form() {
        let with_bom = payload_sha256("\u{FEFF}hello".as_bytes());
        let without_bom = payload_sha256("hello".as_bytes());
        assert_ne!(with_bom, without_bom, "BOM must not be stripped");

        let crlf = payload_sha256(b"line1\r\nline2");
        let lf = payload_sha256(b"line1\nline2");
        assert_ne!(crlf, lf, "CRLF must not be rewritten to LF");

        // "café": NFC is a single U+00E9, NFD is 'e' + combining acute U+0301. Same rendered
        // text, different bytes.
        let nfc = payload_sha256("caf\u{00E9}".as_bytes());
        let nfd = payload_sha256("cafe\u{0301}".as_bytes());
        assert_ne!(
            nfc, nfd,
            "NFC and NFD forms must not be normalized to one hash"
        );
    }

    /// Correctness anchor against the published SHA-256 test vectors (NIST/RFC), so the
    /// "raw-byte SHA-256" claim isn't only self-referential.
    #[test]
    fn payload_sha256_matches_known_sha256_vectors() {
        assert_eq!(
            payload_sha256(b"").to_hex(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            payload_sha256(b"abc").to_hex(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
