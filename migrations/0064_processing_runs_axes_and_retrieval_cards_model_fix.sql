-- Code-review follow-up on 0062/0063 (blocker/major, root-cause fixes — 0062/0063 are
-- already applied and stay untouched; every change here is a new forward migration).

-- ---------------------------------------------------------------------------------------
-- (major) private.processing_runs: `processor` text + `model` text collapse 12 §1.3
-- fingerprint axes (processor_kind, processor_version, model_provider, model_id,
-- model_revision, prompt_version, prompt_hash, embedding_version, parser_version,
-- card_builder_version, evidence_payload_sha256[], context_snapshot_seq) into 2 persisted
-- columns — a stored row can never recompute its own source_hash, so §1.3's 回放/模型对比/
-- 精准重建/diff questions stay unanswerable from the database alone. Table has zero
-- production rows (0062's own header claim, still true: the only INSERT anywhere in the
-- codebase is crates/adapters/tests/processing_runs_fingerprint_rerun.rs's throwaway-tenant
-- fixture, which deletes its own rows on Drop), so this is a straight rename/split/add, no
-- backfill required.
ALTER TABLE private.processing_runs
  RENAME COLUMN processor TO processor_kind;
ALTER TABLE private.processing_runs
  RENAME COLUMN model TO model_id;

ALTER TABLE private.processing_runs
  ADD COLUMN processor_version text,
  ADD COLUMN model_provider text,
  ADD COLUMN model_revision text,
  ADD COLUMN prompt_hash text,
  ADD COLUMN parser_version text,
  ADD COLUMN embedding_version text,
  ADD COLUMN card_builder_version text,
  ADD COLUMN evidence_payload_sha256 bytea[];

-- Every axis that is a plain (non-Option) field in
-- `humaux_projection::fingerprint::ProcessingInputFingerprintInputs` is NOT NULL here too —
-- embedding_version / card_builder_version stay nullable because they are `Option<&str>`
-- in that same struct (not every processing run is an embed or a card build).
ALTER TABLE private.processing_runs
  ALTER COLUMN processor_version SET NOT NULL,
  ALTER COLUMN model_provider SET NOT NULL,
  ALTER COLUMN model_revision SET NOT NULL,
  ALTER COLUMN prompt_hash SET NOT NULL,
  ALTER COLUMN parser_version SET NOT NULL,
  ALTER COLUMN evidence_payload_sha256 SET NOT NULL;

ALTER TABLE private.processing_runs
  ADD CONSTRAINT processing_runs_evidence_payload_sha256_nonempty
    CHECK (cardinality(evidence_payload_sha256) > 0);

-- Reuses projection.bytea_array_all_32_bytes (0063) rather than defining a second copy of
-- the same per-element width scan — same helper, same 32-byte SHA-256 digest width as
-- projection.retrieval_cards.evidence_payload_sha256.
ALTER TABLE private.processing_runs
  ADD CONSTRAINT processing_runs_evidence_payload_sha256_width
    CHECK (projection.bytea_array_all_32_bytes(evidence_payload_sha256));

COMMENT ON COLUMN private.processing_runs.processor_kind IS
  '§1.3/§16.1.1: processing-input fingerprint axis (renamed from `processor` — 0062 collapsed processor_kind+processor_version into one column).';
COMMENT ON COLUMN private.processing_runs.processor_version IS '§1.3/§16.1.1: fingerprint axis.';
COMMENT ON COLUMN private.processing_runs.model_provider IS '§1.3/§16.1.1: fingerprint axis.';
COMMENT ON COLUMN private.processing_runs.model_id IS
  '§1.3/§16.1.1: fingerprint axis (renamed from `model` — 0062 collapsed model_provider+model_id+model_revision into one column).';
COMMENT ON COLUMN private.processing_runs.model_revision IS '§1.3/§16.1.1/G16-4: fingerprint axis.';
COMMENT ON COLUMN private.processing_runs.prompt_hash IS '§1.3/§16.1.1: fingerprint axis.';
COMMENT ON COLUMN private.processing_runs.parser_version IS '§1.3/§16.1.1: fingerprint axis.';
COMMENT ON COLUMN private.processing_runs.embedding_version IS
  '§1.3/§16.1.1: fingerprint axis, null when this run is not an embed (Option in ProcessingInputFingerprintInputs).';
COMMENT ON COLUMN private.processing_runs.card_builder_version IS
  '§1.3/§16.1.1: fingerprint axis, null when this run is not a card build (Option in ProcessingInputFingerprintInputs).';
COMMENT ON COLUMN private.processing_runs.evidence_payload_sha256 IS
  '§16.1/§16.1.1: payload_sha256 of every Evidence this processing run depends on — the set humaux_projection::fingerprint::source_hash hashes. Non-empty, each element 32 bytes.';

-- ---------------------------------------------------------------------------------------
-- (major) projection.retrieval_cards.model_id/model_revision: 0063 added these NOT NULL,
-- but §18.1 freezes RetrievalCard assembly as a pure zero-model-call Rust function
-- (projection::card::build_card) — its CardInput carries no model field at all. The NOT
-- NULL forced the (not yet written) writer to invent a placeholder, which §68/§16.3 cannot
-- distinguish from a real model identity. Root-cause fix: make the columns nullable *and*
-- enforce by CHECK that they stay null (not just permit it) — the honest value for a card
-- that never called a model is absence, not a placeholder string.
ALTER TABLE projection.retrieval_cards
  ALTER COLUMN model_id DROP NOT NULL,
  ALTER COLUMN model_revision DROP NOT NULL;

ALTER TABLE projection.retrieval_cards
  ADD CONSTRAINT retrieval_cards_model_fields_absent
    CHECK (model_id IS NULL AND model_revision IS NULL);

COMMENT ON COLUMN projection.retrieval_cards.model_id IS
  '§16.1/§1.3: model identity axis feeding source_hash — always NULL on this table (§18.1: RetrievalCard assembly makes zero model calls; see retrieval_cards_model_fields_absent). A future Dense/Sparse projection-kind tracking table is where a real model_id would live.';
COMMENT ON COLUMN projection.retrieval_cards.model_revision IS
  '§16.1/§1.3/G16-4: model revision axis feeding source_hash — always NULL on this table, same reason as model_id (retrieval_cards_model_fields_absent).';

-- ---------------------------------------------------------------------------------------
-- (minor) projection.retrieval_cards_projection_type_known admitted all six §16 Projection
-- kinds on a table that, by 0063's own comment and by 0074's RetrievalCard-only CHECKs
-- (retrieval_cards_card_text_contains_title, retrieval_cards_data_class_known_not_secret),
-- only ever holds RetrievalCard rows — projection_type stopped discriminating anything.
ALTER TABLE projection.retrieval_cards
  DROP CONSTRAINT retrieval_cards_projection_type_known;
ALTER TABLE projection.retrieval_cards
  ADD CONSTRAINT retrieval_cards_projection_type_known
    CHECK (projection_type = 'RetrievalCard');

COMMENT ON COLUMN projection.retrieval_cards.projection_type IS
  '§16.1: narrowed to the single kind this physical table holds (0064) — a future projection kind gets its own tracking table with its own literal, not a widened set here.';

-- ---------------------------------------------------------------------------------------
-- (minor) private.processing_runs: completed_at/output_digest/output_count carried no
-- CHECK tying them together, so a row could have completed_at set with output_count still
-- NULL — indistinguishable from a run that "从没记过" its count (§1.4 坑5).
ALTER TABLE private.processing_runs
  ADD CONSTRAINT processing_runs_completed_has_output
    CHECK (completed_at IS NULL OR (output_digest IS NOT NULL AND output_count IS NOT NULL));
