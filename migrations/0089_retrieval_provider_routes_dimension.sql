-- §19 Provider Route / Projection Compatibility fix (T7.2 follow-up, code-review finding on
-- `crates/retrieval-provider/src/router.rs::ProjectionRef`/`projection_compatible`).
-- 0088 created `control.retrieval_provider_routes` with no `dimension` column, so the
-- Router's Projection Compatibility gate could only compare `(embedding_provider_id,
-- embedding_model_id)` — but `contract.rs::EmbeddingModelDescriptor.dimension_options:
-- Vec<u32>` (and the dashscope `text-embedding-v4` fixture: 8 Matryoshka options, 64..2048)
-- proves one `(provider_id, model_id)` pair legitimately spans multiple valid dimensions.
-- §19's "Compatible Failover" list requires "same dimension" as an independent criterion
-- alongside "same provider/model semantics" — this migration adds the column the gate needs
-- to actually check it. Cannot edit 0088 in place (already applied, rule ③) — pure ADD
-- COLUMN + one CHECK, same EXPAND-only shape 0094 used to widen `ops.model_call_ledger`.
--
-- Nullable, tied to the embedding pair exactly like `embedding_model_id` already is
-- (`(embedding_provider_id IS NULL) = (embedding_model_id IS NULL)` in 0088): a rerank-only
-- row has no embedding dimension to name, and an embedding row must always name one — a
-- lone provider/model with no dimension is as much a garbage value as a lone provider with
-- no model.
ALTER TABLE control.retrieval_provider_routes
  ADD COLUMN embedding_dimension integer CHECK (embedding_dimension IS NULL OR embedding_dimension > 0);

ALTER TABLE control.retrieval_provider_routes
  ADD CONSTRAINT retrieval_provider_routes_dimension_with_embedding
  CHECK ((embedding_provider_id IS NULL) = (embedding_dimension IS NULL));

COMMENT ON COLUMN control.retrieval_provider_routes.embedding_dimension IS
  'Embedding vector dimension this route resolves to (e.g. one of a Matryoshka model''s '
  'dimension_options — see contract.rs::EmbeddingModelDescriptor). NULL only on rerank-only '
  'rows. Required together with embedding_provider_id/embedding_model_id: router.rs''s '
  'ProjectionRef/projection_compatible treats (provider_id, model_id, dimension) as the '
  'compatibility key, not just the first two — a fixed (provider_id, model_id) pair can '
  'legitimately span multiple dimensions (Matryoshka models), so dimension is an independent '
  'axis of an Incompatible Model Change (§19 Embedding Provider Failover 硬规则), not implied '
  'by provider/model alone.';
