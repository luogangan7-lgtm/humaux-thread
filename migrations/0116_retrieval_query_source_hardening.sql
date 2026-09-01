-- Forward hardening for 0115, which has already been applied to the isolated guard database.
-- Native query sealing admits only private, nonempty query and wire metadata.
ALTER TABLE private.retrieval_query_sources
  ADD CONSTRAINT retrieval_query_sources_private_data_class
    CHECK (data_class = 'PRIVATE'),
  ADD CONSTRAINT retrieval_query_sources_query_bytes_positive
    CHECK (query_bytes > 0),
  ADD CONSTRAINT retrieval_query_sources_wire_payload_bytes_positive
    CHECK (wire_payload_bytes > 0);

COMMENT ON CONSTRAINT retrieval_query_sources_private_data_class
  ON private.retrieval_query_sources IS
  'Canonical sealed native retrieval queries are PRIVATE only.';
