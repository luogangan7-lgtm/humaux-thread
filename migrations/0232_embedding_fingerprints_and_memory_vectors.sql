-- §46 EXPAND_CONTRACT (card 37 S1; ADR-0064 D-B, D-C, D-D). Stored embedding vectors in PostgreSQL, keyed by the
-- vector space they live in, so the Qdrant projection can be rebuilt from PostgreSQL without a provider call:
--   projection.embedding_fingerprints  one row per embedding label: the sha256 of the provider / model / revision /
--                                      dimension / task / preprocessing / contract / dtype / normalization / distance
--                                      encoding (humaux_projection::embedding_fingerprint). Cluster-level, no tenant,
--                                      no RLS. UNIQUE (embedding_version): one label = one fingerprint (D-C).
--                                      Writer: the retrieval worker's boot binding (card 37 S2, INSERT .. ON CONFLICT
--                                      DO NOTHING); readers: the worker, humaux-maintenance projection rebuild.
--   projection.memory_vectors          the provider's raw vector per (tenant, memory, fingerprint, sealed-card sha256).
--                                      Written by the retrieval worker in the registry transaction (S2); purged by
--                                      UPDATE (vector -> NULL) with the last live point (D-D), never deleted.
--                                      FORCE RLS on the tenant GUC. role_gateway holds nothing (F24, finding 20).
--   projection.private_memory_points   + fingerprint_sha256, input_sha256 (both NULL = registered before card 37,
--                                      "legacy"; both 32 bytes otherwise) and a MATCH SIMPLE FK to memory_vectors:
--                                      the database refuses a fingerprinted registry row without its vector row.
--
-- Grants (ADR-0064 E9, enumerated): the 0011 default privileges on new projection tables (role_gateway SELECT,
-- role_retrieval_worker SELECT/INSERT/UPDATE, role_maintenance SELECT) are REVOKEd first, then:
--   embedding_fingerprints  role_retrieval_worker SELECT, INSERT; role_maintenance SELECT.
--   memory_vectors          role_retrieval_worker SELECT, INSERT, UPDATE (vector, purged_at); role_maintenance SELECT.
-- private_memory_points keeps its 0011 domain-default grants: role_retrieval_worker's table-level UPDATE already
-- covers the two new columns (backfill-on-touch, D-B), so no column grant is added.
--
-- Locks (measured on a throwaway migrated to 0231 and seeded with 16,703 registry rows, the dev registry size, F3):
--   the two CREATE TABLEs take locks only on the new relations; ALTER TABLE projection.private_memory_points takes
--   ACCESS EXCLUSIVE for the ADD COLUMNs + ADD CONSTRAINT CHECK (one validation scan) and SHARE ROW EXCLUSIVE on
--   projection.memory_vectors for the FK (one validation scan; every existing row is NULL, MATCH SIMPLE). The whole
--   file took 11.2 ms on that copy (2026-10-06); the registry's readers (gateway) and writer (retrieval worker)
--   wait at most that long.

CREATE TABLE projection.embedding_fingerprints (
  fingerprint_sha256          bytea       PRIMARY KEY CHECK (octet_length(fingerprint_sha256) = 32),
  embedding_version           text        NOT NULL UNIQUE CHECK (length(embedding_version) BETWEEN 1 AND 128),
  provider                    text        NOT NULL CHECK (length(provider) > 0),
  model_id                    text        NOT NULL CHECK (length(model_id) > 0),
  model_revision              text        NOT NULL CHECK (length(model_revision) > 0),
  dimension                   integer     NOT NULL CHECK (dimension > 0),
  task_type                   text        NOT NULL CHECK (length(task_type) > 0),
  preprocessing_version       text        NOT NULL CHECK (length(preprocessing_version) > 0),
  projection_contract_version text        NOT NULL CHECK (length(projection_contract_version) > 0),
  dtype                       text        NOT NULL CHECK (dtype = 'float32'),
  normalization               text        NOT NULL CHECK (length(normalization) > 0),
  distance                    text        NOT NULL CHECK (distance = 'Cosine'),
  created_at                  timestamptz NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE projection.embedding_fingerprints OWNER TO role_migration_owner;
REVOKE ALL ON projection.embedding_fingerprints
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin, role_health_reader;
GRANT SELECT, INSERT ON projection.embedding_fingerprints TO role_retrieval_worker;
GRANT SELECT ON projection.embedding_fingerprints TO role_maintenance;

CREATE TABLE projection.memory_vectors (
  tenant_id          uuid        NOT NULL,
  memory_id          uuid        NOT NULL,
  fingerprint_sha256 bytea       NOT NULL REFERENCES projection.embedding_fingerprints (fingerprint_sha256),
  input_sha256       bytea       NOT NULL,
  dimension          integer     NOT NULL CHECK (dimension > 0),
  vector             real[],
  created_at         timestamptz NOT NULL DEFAULT clock_timestamp(),
  purged_at          timestamptz,
  PRIMARY KEY (tenant_id, memory_id, fingerprint_sha256, input_sha256),
  CONSTRAINT memory_vectors_memory_tenant_fk
    FOREIGN KEY (tenant_id, memory_id) REFERENCES private.memory_records (tenant_id, memory_id),
  CONSTRAINT memory_vectors_sha_lengths
    CHECK (octet_length(fingerprint_sha256) = 32 AND octet_length(input_sha256) = 32),
  -- ADR-0064 D-D: a purged row keeps its key (a ~150 B tombstone) and loses its bytes.
  CONSTRAINT memory_vectors_purge_consistent CHECK ((vector IS NULL) = (purged_at IS NOT NULL)),
  CONSTRAINT memory_vectors_shape
    CHECK (vector IS NULL OR (array_ndims(vector) = 1 AND cardinality(vector) = dimension))
);

ALTER TABLE projection.memory_vectors OWNER TO role_migration_owner;
ALTER TABLE projection.memory_vectors ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection.memory_vectors FORCE ROW LEVEL SECURITY;
CREATE POLICY memory_vectors_tenant_isolation ON projection.memory_vectors
  USING (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid)
  WITH CHECK (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid);
REVOKE ALL ON projection.memory_vectors
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin, role_health_reader;
GRANT SELECT, INSERT ON projection.memory_vectors TO role_retrieval_worker;
GRANT UPDATE (vector, purged_at) ON projection.memory_vectors TO role_retrieval_worker;
GRANT SELECT ON projection.memory_vectors TO role_maintenance;

ALTER TABLE projection.private_memory_points
  ADD COLUMN fingerprint_sha256 bytea,
  ADD COLUMN input_sha256 bytea,
  ADD CONSTRAINT private_memory_points_vector_identity_pair CHECK (
    (fingerprint_sha256 IS NULL AND input_sha256 IS NULL)
    OR (octet_length(fingerprint_sha256) = 32 AND octet_length(input_sha256) = 32)),
  -- ADR-0064 D-B: MATCH SIMPLE exempts legacy rows (both NULL); a fingerprinted row has its vector row.
  ADD CONSTRAINT private_memory_points_vector_fk
    FOREIGN KEY (tenant_id, memory_id, fingerprint_sha256, input_sha256)
    REFERENCES projection.memory_vectors (tenant_id, memory_id, fingerprint_sha256, input_sha256);

COMMENT ON TABLE projection.embedding_fingerprints IS
  'ADR-0064 D-C: one embedding label = one vector-space fingerprint (sha256 of the canonical encoding). Writer: the '
  'retrieval worker boot binding. role_retrieval_worker SELECT, INSERT; role_maintenance SELECT.';
COMMENT ON TABLE projection.memory_vectors IS
  'ADR-0064 D-B/D-D: the provider''s raw vector per (tenant, memory, fingerprint, sealed-card sha256), written in the '
  'registry transaction, purged by UPDATE with the last live point. role_gateway holds nothing.';
COMMENT ON COLUMN projection.private_memory_points.fingerprint_sha256 IS
  'ADR-0064 D-B: the fingerprint of the stored vector this point was projected from; NULL = registered before card 37.';
