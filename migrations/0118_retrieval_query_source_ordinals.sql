-- One durable metadata source per query in a provider batch. Existing single-query rows map
-- forward to ordinal zero; no historical row is rewritten or deleted.
ALTER TABLE private.retrieval_query_sources
  ADD COLUMN query_ordinal integer NOT NULL DEFAULT 0,
  ADD CONSTRAINT retrieval_query_sources_query_ordinal_nonnegative
    CHECK (query_ordinal >= 0),
  DROP CONSTRAINT retrieval_query_sources_attempt_unique,
  ADD CONSTRAINT retrieval_query_sources_attempt_ordinal_unique
    UNIQUE (tenant_id, logical_call_id, attempt_no, query_ordinal);

COMMENT ON COLUMN private.retrieval_query_sources.query_ordinal IS
  'Stable zero-based position of this sealed query in one provider batch.';
