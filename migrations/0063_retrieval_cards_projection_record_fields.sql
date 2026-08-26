-- T5.1 (§16.1 Projection 记录字段): completes projection.retrieval_cards's nine-field
-- Projection record shape. 0007 only laid the skeleton (card_id/tenant_id/memory_id/
-- created_at) — this migration adds the remaining nine columns without editing the
-- already-applied 0007. Table has zero rows in every environment this has run (fresh
-- skeleton table), so every NOT NULL column below adds cleanly with no backfill.

-- projection_type: closed set = the six reconstructible Projection kinds §16's own opening
-- line enumerates ("所有 Projection 可重建：RetrievalCard、Dense Embedding、Sparse
-- representation、Qdrant point、Relation projection、Code projection"). This table currently
-- only ever holds RetrievalCard rows (§18), but the column is part of the shared nine-field
-- shape §16.1 defines once for every projection-kind's own tracking table — the CHECK stays
-- the full six-value set rather than narrowing to a single literal, so it does not have to be
-- redefined if a future projection kind's tracking table copies this same migration shape.
ALTER TABLE projection.retrieval_cards
  ADD COLUMN projection_type text,
  ADD COLUMN projection_version text,
  ADD COLUMN source_stream_seq bigint,
  ADD COLUMN evidence_payload_sha256 bytea[],
  ADD COLUMN context_snapshot_seq bigint,
  ADD COLUMN source_hash bytea,
  ADD COLUMN model_id text,
  ADD COLUMN model_revision text,
  ADD COLUMN status text;

ALTER TABLE projection.retrieval_cards
  ALTER COLUMN projection_type SET NOT NULL,
  ALTER COLUMN projection_version SET NOT NULL,
  ALTER COLUMN source_stream_seq SET NOT NULL,
  ALTER COLUMN evidence_payload_sha256 SET NOT NULL,
  ALTER COLUMN context_snapshot_seq SET NOT NULL,
  ALTER COLUMN source_hash SET NOT NULL,
  ALTER COLUMN model_id SET NOT NULL,
  ALTER COLUMN model_revision SET NOT NULL,
  ALTER COLUMN status SET NOT NULL;

ALTER TABLE projection.retrieval_cards
  ADD CONSTRAINT retrieval_cards_projection_type_known
    CHECK (projection_type IN (
      'RetrievalCard', 'DenseEmbedding', 'SparseRepresentation',
      'QdrantPoint', 'RelationProjection', 'CodeProjection'
    ));

-- §16.1: "evidence_payload_sha256[] 是新增且不可省" — an empty array is the same omission
-- the spec is closing (cardinality(), not array_length(): array_length() returns NULL, not
-- 0, for an empty array and a CHECK treats NULL as satisfied — same pitfall 0061 already hit
-- and documented on control.reasoning_domain_grants.purposes).
ALTER TABLE projection.retrieval_cards
  ADD CONSTRAINT retrieval_cards_evidence_payload_sha256_nonempty
    CHECK (cardinality(evidence_payload_sha256) > 0);

-- Each element must be a full SHA-256 digest (32 bytes) — the same width
-- private.evidence_objects.payload_sha256 and this table's own new source_hash column use.
-- A CHECK constraint's expression cannot itself contain a subquery (`unnest()` needs one),
-- so the per-element scan is wrapped in a small IMMUTABLE helper function instead.
CREATE FUNCTION projection.bytea_array_all_32_bytes(arr bytea[]) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$
  SELECT NOT EXISTS (SELECT 1 FROM unnest(arr) AS h WHERE octet_length(h) <> 32);
$$;

ALTER FUNCTION projection.bytea_array_all_32_bytes(bytea[]) OWNER TO role_migration_owner;

ALTER TABLE projection.retrieval_cards
  ADD CONSTRAINT retrieval_cards_evidence_payload_sha256_width
    CHECK (projection.bytea_array_all_32_bytes(evidence_payload_sha256));

ALTER TABLE projection.retrieval_cards
  ADD CONSTRAINT retrieval_cards_source_hash_width CHECK (octet_length(source_hash) = 32);

-- §18.4: "拼装是纯函数，只因源字段缺失而降级：card_status = complete | partial |
-- unbuildable" — this table only ever holds RetrievalCard rows today (see projection_type
-- comment above), so the RetrievalCard-specific closed set from §18.4 is the correct one
-- for `status` here (a future non-RetrievalCard projection-kind table would define its own
-- status closed set, not reuse this one).
ALTER TABLE projection.retrieval_cards
  ADD CONSTRAINT retrieval_cards_status_known
    CHECK (status IN ('complete', 'partial', 'unbuildable'));

COMMENT ON COLUMN projection.retrieval_cards.projection_type IS
  '§16.1: which of the six reconstructible Projection kinds this row is (closed set, §16 opening line).';
COMMENT ON COLUMN projection.retrieval_cards.projection_version IS
  '§16.1 / §16.2: model-changeover version axis; read routing only ever targets the `serving` version (§16.2).';
COMMENT ON COLUMN projection.retrieval_cards.source_stream_seq IS
  '§16.1: the projection.stream_log row this card was produced from; source_commit_seq is audit-only (§15) and not carried here.';
COMMENT ON COLUMN projection.retrieval_cards.evidence_payload_sha256 IS
  '§16.1: payload_sha256 of every Evidence this card depends on (order-independent content anchor set — see fingerprint::source_hash rustdoc for the canonical sort). Non-empty, each element 32 bytes.';
COMMENT ON COLUMN projection.retrieval_cards.context_snapshot_seq IS
  '§1.2.1/§16.1: the memory-visibility upper bound read while building this card — distillation is not a pure function of Evidence alone.';
COMMENT ON COLUMN projection.retrieval_cards.source_hash IS
  '§16.1/§16.1.1: processing input fingerprint = H(evidence_payload_sha256[], processor/model/prompt/parser version, context_snapshot_seq). Sole construction point: humaux_projection::fingerprint::source_hash (G80-11).';
COMMENT ON COLUMN projection.retrieval_cards.model_id IS
  '§16.1/§1.3: model identity axis feeding source_hash.';
COMMENT ON COLUMN projection.retrieval_cards.model_revision IS
  '§16.1/§1.3/G16-4: model revision axis feeding source_hash — G16-4 requires this alone to change source_hash.';
COMMENT ON COLUMN projection.retrieval_cards.status IS
  '§18.4: card_status closed set (complete | partial | unbuildable) — a card degrades on missing source fields rather than failing the write outright.';
