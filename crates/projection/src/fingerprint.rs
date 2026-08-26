//! `projection::fingerprint` — processing input fingerprint (§16.1 / §16.1.1).
//!
//! `source_hash` proves the *processing input* (Evidence set + processor/model/prompt/parser
//! versions + context snapshot) was identical between two runs — it does **not** prove the
//! external LLM/VLM produced byte-identical output. §1.2.1: distillation reads live Memory
//! context (§11 correction checks) and can write back Evidence (§36 user `correct`), so it is
//! not a pure function of Evidence alone — replay guarantees stop at "same input", they do
//! not extend to "same Memory output" (§16.1.1's own framing: "所谓精准重建只适用于确定性
//! Projection...不适用于重新调用外部 LLM"). Comparing `output_digest` bytes is a separate,
//! narrower question this module has no opinion on — see [`source_hash`]'s rustdoc for why an
//! `output_digest` change alone can never move a `SourceHash`.

use humaux_domain::evidence::EvidencePayloadSha256;
use sha2::{Digest, Sha256};

/// Every axis §1.3's frozen versioning list requires, spelled out field by field instead of
/// §16.1's shorthand `H(evidence_payload_sha256[] ‖ processor/model/prompt/parser version ‖
/// context_snapshot_seq)` — a typed struct so two adjacent axes can never be silently
/// concatenated ambiguously the way raw `String` gluing would allow (e.g. `model_id="a"` +
/// `model_revision="bc"` colliding with `model_id="ab"` + `model_revision="c"` under naive
/// `format!("{a}{b}")`).
///
/// `embedding_version` / `card_builder_version` are `None` when the current projection kind
/// doesn't use them (§16.1's G16-4: "card_builder_version (when applicable)") — see
/// [`source_hash`]'s rustdoc for why `None` can never collide with `Some("")`.
#[derive(Debug, Clone, Copy)]
pub struct ProcessingInputFingerprintInputs<'a> {
    /// §16.1: `payload_sha256` of every Evidence this processing run depends on. Order does
    /// **not** matter — see [`source_hash`]'s rustdoc for the canonicalization.
    pub evidence_payload_sha256: &'a [EvidencePayloadSha256],
    pub processor_kind: &'a str,
    pub processor_version: &'a str,
    pub model_provider: &'a str,
    pub model_id: &'a str,
    pub model_revision: &'a str,
    pub prompt_version: &'a str,
    pub prompt_hash: &'a str,
    pub embedding_version: Option<&'a str>,
    pub parser_version: &'a str,
    pub card_builder_version: Option<&'a str>,
    /// §1.2.1/§16.1: the memory-visibility upper bound read while processing — distillation
    /// is not a pure function of Evidence alone, so this is the fingerprint's third leg, not
    /// an optional extra. This is the exact field G16-4's registered 注错 removes from the
    /// constructor to prove the sensitivity test would catch it (changing only the snapshot
    /// must move `source_hash`; deleting this field from the encoding is the regression).
    pub context_snapshot_seq: u64,
}

/// Content-addressed identity anchor for one processing run's *input* (§16.1/§16.1.1) — never
/// for its output. The external LLM/VLM's own output bytes are not part of this hash: remote
/// inference cannot promise bit-for-bit determinism (§1.2.1 "2.4 现行落点"), so this
/// fingerprint only covers what the caller controlled going in. `private.processing_runs`
/// keeps `output_digest` as a *separate* column precisely so a rerun under the same
/// `source_hash` can carry a different `output_digest` without that being treated as data
/// corruption (§16.1.1: "同一 fingerprint 可以有多次 processing run / 不同 output digest；
/// 旧输出不被覆盖").
///
/// **Sole construction point is [`source_hash`]** (§48.0/§1.3 G80-11 — same single-crate
/// convergence family as `evidence::payload_sha256` G80-22 and the §1.12 canonical tool
/// schema): the field is private, there is no `pub` constructor, no `From<[u8; 32]>`, and no
/// `Default`. `architecture-check` asserts exactly one construction site of `SourceHash(`
/// workspace-wide.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SourceHash([u8; 32]);

impl SourceHash {
    /// Lowercase hex rendering of the 32-byte digest — a read-only projection, not a second
    /// construction path.
    pub fn to_hex(&self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Raw digest bytes, for writing into `private.processing_runs.source_hash` /
    /// `projection.retrieval_cards.source_hash` (both `bytea`).
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Sole constructor for [`SourceHash`] (§48.0/§1.3 G80-11).
///
/// **Encoding.** Every scalar field is length-prefixed (an 8-byte little-endian byte count)
/// before its bytes, so no two adjacent fields can collide by shifting a boundary — the
/// defense naive `format!("{a}{b}")` string gluing does not have. An absent optional field
/// (`embedding_version` / `card_builder_version` = `None`) is encoded as a single `0x00`
/// presence byte with **no** length/bytes following it; a present field (including an empty
/// string) is `0x01` followed by its length-prefixed bytes — `None` can therefore never
/// collide with `Some("")`.
///
/// **Order-independence and deduplication.** `evidence_payload_sha256` is hashed as a true
/// *set*, not a sequence and not a multiset: each element's fixed-width (64-character) hex
/// rendering is sorted then deduplicated before encoding, so (a) two callers who assembled
/// the same evidence dependency set in a different iteration order, and (b) a caller whose
/// join emitted the same payload twice (`private.memory_evidence`'s primary key is
/// `(memory_id, evidence_id, role)`, so one Evidence legitimately appears under two roles —
/// an assembler joining through it without `DISTINCT` would otherwise double-count it) both
/// still produce the same `source_hash` as a caller who saw the set once, in canonical order
/// (§16.1 defines the fingerprint over the evidence the run depended on, not the order it was
/// read in or how many roles linked it — nothing in §16.1's prose treats either as part of
/// the identity). The (deduplicated) element count is length-prefixed and each hex string is
/// a fixed 64 bytes, so the array section is self-delimiting with no separate per-element
/// length prefix needed.
pub fn source_hash(inputs: &ProcessingInputFingerprintInputs<'_>) -> SourceHash {
    let mut hasher = Sha256::new();

    let mut hexes: Vec<String> = inputs
        .evidence_payload_sha256
        .iter()
        .map(EvidencePayloadSha256::to_hex)
        .collect();
    hexes.sort_unstable();
    hexes.dedup(); // set, not multiset — see rustdoc above (private.memory_evidence role dup).
    hasher.update((hexes.len() as u64).to_le_bytes());
    for hex in &hexes {
        hasher.update(hex.as_bytes()); // fixed 64 ASCII bytes each — self-delimiting.
    }

    write_field(&mut hasher, inputs.processor_kind.as_bytes());
    write_field(&mut hasher, inputs.processor_version.as_bytes());
    write_field(&mut hasher, inputs.model_provider.as_bytes());
    write_field(&mut hasher, inputs.model_id.as_bytes());
    write_field(&mut hasher, inputs.model_revision.as_bytes());
    write_field(&mut hasher, inputs.prompt_version.as_bytes());
    write_field(&mut hasher, inputs.prompt_hash.as_bytes());
    write_optional_field(&mut hasher, inputs.embedding_version);
    write_field(&mut hasher, inputs.parser_version.as_bytes());
    write_optional_field(&mut hasher, inputs.card_builder_version);
    write_field(&mut hasher, &inputs.context_snapshot_seq.to_le_bytes());

    SourceHash(hasher.finalize().into())
}

fn write_field(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn write_optional_field(hasher: &mut Sha256, field: Option<&str>) {
    match field {
        None => hasher.update([0u8]),
        Some(s) => {
            hasher.update([1u8]);
            write_field(hasher, s.as_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence(bytes: &[u8]) -> EvidencePayloadSha256 {
        humaux_domain::evidence::payload_sha256(bytes)
    }

    /// Baseline inputs every G16-4 per-axis test starts from and mutates exactly one field
    /// of ("固定 Evidence 后逐轴只改一项").
    fn baseline<'a>(ev: &'a [EvidencePayloadSha256]) -> ProcessingInputFingerprintInputs<'a> {
        ProcessingInputFingerprintInputs {
            evidence_payload_sha256: ev,
            processor_kind: "distill",
            processor_version: "1",
            model_provider: "alibaba",
            model_id: "qwen3-max",
            model_revision: "2026-08-01",
            prompt_version: "3",
            prompt_hash: "prompt-hash-abc",
            embedding_version: Some("v4"),
            parser_version: "2",
            card_builder_version: Some("1"),
            context_snapshot_seq: 100,
        }
    }

    #[test]
    fn identical_inputs_produce_identical_hash() {
        let ev = [evidence(b"one"), evidence(b"two")];
        assert_eq!(source_hash(&baseline(&ev)), source_hash(&baseline(&ev)));
    }

    #[test]
    fn g16_4_axis_model_revision_changes_hash() {
        let ev = [evidence(b"one")];
        let a = baseline(&ev);
        let mut b = baseline(&ev);
        b.model_revision = "2026-09-01";
        assert_ne!(source_hash(&a), source_hash(&b));
    }

    #[test]
    fn g16_4_axis_prompt_hash_changes_hash() {
        let ev = [evidence(b"one")];
        let a = baseline(&ev);
        let mut b = baseline(&ev);
        b.prompt_hash = "prompt-hash-xyz";
        assert_ne!(source_hash(&a), source_hash(&b));
    }

    #[test]
    fn g16_4_axis_parser_version_changes_hash() {
        let ev = [evidence(b"one")];
        let a = baseline(&ev);
        let mut b = baseline(&ev);
        b.parser_version = "3";
        assert_ne!(source_hash(&a), source_hash(&b));
    }

    #[test]
    fn g16_4_axis_card_builder_version_changes_hash_when_applicable() {
        let ev = [evidence(b"one")];
        let a = baseline(&ev);
        let mut b = baseline(&ev);
        b.card_builder_version = Some("2");
        assert_ne!(source_hash(&a), source_hash(&b));

        // "(when applicable)": None vs Some must also differ, not just Some vs Some.
        let mut c = baseline(&ev);
        c.card_builder_version = None;
        assert_ne!(source_hash(&a), source_hash(&c));
    }

    #[test]
    fn g16_4_axis_context_snapshot_seq_changes_hash() {
        let ev = [evidence(b"one")];
        let a = baseline(&ev);
        let mut b = baseline(&ev);
        b.context_snapshot_seq = 101;
        assert_ne!(source_hash(&a), source_hash(&b));
    }

    #[test]
    fn g16_4_axis_evidence_payload_hash_changes_hash() {
        let ev_a = [evidence(b"one"), evidence(b"two")];
        let ev_b = [evidence(b"one"), evidence(b"THREE")];
        assert_ne!(source_hash(&baseline(&ev_a)), source_hash(&baseline(&ev_b)));
    }

    /// §16.1's remaining §1.3 axes not individually named by G16-4's six-item list, held to
    /// the same "any declared axis changes ⇒ different source_hash" rule the formula itself
    /// states — under-testing these would leave a silent identity collision on an axis §1.3
    /// still requires ("缺任意一项...回放/模型对比/精准重建...全部无法回答").
    #[test]
    fn remaining_versioning_axes_each_change_hash() {
        let ev = [evidence(b"one")];
        let a = baseline(&ev);

        let mut processor_kind = baseline(&ev);
        processor_kind.processor_kind = "extract";
        assert_ne!(source_hash(&a), source_hash(&processor_kind));

        let mut processor_version = baseline(&ev);
        processor_version.processor_version = "2";
        assert_ne!(source_hash(&a), source_hash(&processor_version));

        let mut model_provider = baseline(&ev);
        model_provider.model_provider = "openai";
        assert_ne!(source_hash(&a), source_hash(&model_provider));

        let mut model_id = baseline(&ev);
        model_id.model_id = "qwen3.5-max";
        assert_ne!(source_hash(&a), source_hash(&model_id));

        let mut prompt_version = baseline(&ev);
        prompt_version.prompt_version = "4";
        assert_ne!(source_hash(&a), source_hash(&prompt_version));

        let mut embedding_version = baseline(&ev);
        embedding_version.embedding_version = Some("v5");
        assert_ne!(source_hash(&a), source_hash(&embedding_version));
    }

    /// §16.1.1's正对照: an external LLM/VLM output is not part of the fingerprint at all.
    /// Two runs with byte-for-byte identical inputs must hash identically regardless of what
    /// the (separately tracked) `output_digest` for each run turned out to be — `source_hash`
    /// has no output parameter to smuggle a comparison through, so this pins that shape
    /// directly rather than relying on the type signature alone to make the point.
    #[test]
    fn output_digest_never_moves_source_hash() {
        let ev = [evidence(b"one")];
        let inputs = baseline(&ev);

        let run_1_hash = source_hash(&inputs);
        let _run_1_output_digest = payload_digest_stub(b"llm output attempt 1");

        let run_2_hash = source_hash(&inputs); // same inputs, second "run"
        let _run_2_output_digest = payload_digest_stub(b"a completely different llm output");

        assert_eq!(
            run_1_hash, run_2_hash,
            "same fingerprint, independent of output bytes"
        );
        assert_ne!(
            _run_1_output_digest, _run_2_output_digest,
            "sanity: the two stand-in outputs actually differ"
        );
    }

    /// Stand-in for whatever produces `private.processing_runs.output_digest` — this module
    /// does not define that type (it is not this task's concern), only demonstrates that
    /// varying it has zero effect on `source_hash`.
    fn payload_digest_stub(bytes: &[u8]) -> [u8; 32] {
        Sha256::digest(bytes).into()
    }

    #[test]
    fn evidence_order_does_not_affect_hash() {
        let forward = [evidence(b"alpha"), evidence(b"beta"), evidence(b"gamma")];
        let reversed = [evidence(b"gamma"), evidence(b"beta"), evidence(b"alpha")];
        assert_eq!(
            source_hash(&baseline(&forward)),
            source_hash(&baseline(&reversed))
        );
    }

    /// Pins the set-not-multiset ruling the rustdoc claims: the same Evidence appearing
    /// twice (e.g. via two `private.memory_evidence` roles on the same evidence_id — its PK
    /// is `(memory_id, evidence_id, role)`) must hash identically to it appearing once. Code
    /// review finding (major): before `hexes.dedup()` was added, `[A]` and `[A, A]` hashed
    /// differently, contradicting the "hashed as a set" rustdoc and leaving an assembler's
    /// DISTINCT-or-not choice invisible at the call site.
    #[test]
    fn duplicate_evidence_payload_sha256_does_not_change_hash() {
        let once = [evidence(b"one")];
        let twice = [evidence(b"one"), evidence(b"one")];
        assert_eq!(
            source_hash(&baseline(&once)),
            source_hash(&baseline(&twice)),
            "same evidence dependency set, represented once vs. twice, must hash identically"
        );
    }

    /// §80.1 admission condition ("一道闸没有「注错红转绿」记录就不算存在"): registers the
    /// G16-4/G80-34 注错 ("从 source_hash 构造器删掉 context_snapshot_seq，只改 snapshot 后
    /// hash 不变 ⇒ 红") as a red/green artifact in this file, alongside the real constructor's
    /// own green pin (`g16_4_axis_context_snapshot_seq_changes_hash` above) — a reviewer no
    /// longer has to repeat the injection by hand to confirm the gate is real.
    #[test]
    fn g16_4_injection_record_context_snapshot_seq_removed_from_encoding_is_insensitive() {
        // Same encoding as `source_hash`, minus the context_snapshot_seq `write_field` call —
        // the literal 注错 §80.1 registers.
        fn source_hash_without_context_snapshot_seq(
            inputs: &ProcessingInputFingerprintInputs<'_>,
        ) -> [u8; 32] {
            let mut hasher = Sha256::new();
            let mut hexes: Vec<String> = inputs
                .evidence_payload_sha256
                .iter()
                .map(EvidencePayloadSha256::to_hex)
                .collect();
            hexes.sort_unstable();
            hexes.dedup();
            hasher.update((hexes.len() as u64).to_le_bytes());
            for hex in &hexes {
                hasher.update(hex.as_bytes());
            }
            write_field(&mut hasher, inputs.processor_kind.as_bytes());
            write_field(&mut hasher, inputs.processor_version.as_bytes());
            write_field(&mut hasher, inputs.model_provider.as_bytes());
            write_field(&mut hasher, inputs.model_id.as_bytes());
            write_field(&mut hasher, inputs.model_revision.as_bytes());
            write_field(&mut hasher, inputs.prompt_version.as_bytes());
            write_field(&mut hasher, inputs.prompt_hash.as_bytes());
            write_optional_field(&mut hasher, inputs.embedding_version);
            write_field(&mut hasher, inputs.parser_version.as_bytes());
            write_optional_field(&mut hasher, inputs.card_builder_version);
            // context_snapshot_seq write_field(...) intentionally omitted — this is the 注错.
            hasher.finalize().into()
        }

        let ev = [evidence(b"one")];
        let a = baseline(&ev);
        let mut b = baseline(&ev);
        b.context_snapshot_seq = 101;

        // 红: with the axis deleted from the encoding, changing only context_snapshot_seq no
        // longer moves the hash — the fault as registered actually manifests this way.
        assert_eq!(
            source_hash_without_context_snapshot_seq(&a),
            source_hash_without_context_snapshot_seq(&b),
            "注错 sanity: stripped encoding must collide when only context_snapshot_seq differs"
        );

        // 绿: the real constructor (which does encode the axis) does not collide.
        assert_ne!(source_hash(&a), source_hash(&b));
    }

    #[test]
    fn none_and_some_empty_string_do_not_collide() {
        let ev = [evidence(b"one")];
        let mut none = baseline(&ev);
        none.card_builder_version = None;
        let mut some_empty = baseline(&ev);
        some_empty.card_builder_version = Some("");
        assert_ne!(source_hash(&none), source_hash(&some_empty));
    }
}
