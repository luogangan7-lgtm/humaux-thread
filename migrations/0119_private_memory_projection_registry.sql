-- Private Qdrant point identity is resolved only through PostgreSQL.  `point_id` is opaque
-- routing state, never an encoding of `private.memory_records.memory_id`; Qdrant remains a
-- rebuildable candidate store and cannot become an identity authority.
--
-- `memory_records` has no immutable revision yet.  `source_updated_at` is therefore an exact
-- current-source fence, not a field called or treated as a revision.  A future immutable
-- Memory revision may replace it only through a separately frozen contract.
CREATE TABLE projection.private_memory_points (
  point_id            uuid        PRIMARY KEY,
  tenant_id           uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  scope_kind          text        NOT NULL CHECK (length(scope_kind) BETWEEN 1 AND 64),
  scope_id            uuid        NOT NULL,
  domain              text        NOT NULL CHECK (length(domain) BETWEEN 1 AND 128),
  projection_kind     text        NOT NULL CHECK (length(projection_kind) BETWEEN 1 AND 128),
  projection_version  text        NOT NULL CHECK (length(projection_version) BETWEEN 1 AND 128),
  embedding_version   text        NOT NULL CHECK (length(embedding_version) BETWEEN 1 AND 128),
  memory_id           uuid        NOT NULL,
  source_updated_at   timestamptz NOT NULL,
  body_sha256         bytea       NOT NULL CHECK (octet_length(body_sha256) = 32),
  projection_live     boolean     NOT NULL DEFAULT true,
  retired_at          timestamptz,
  created_at          timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT private_memory_points_memory_tenant_fk
    FOREIGN KEY (tenant_id, memory_id)
    REFERENCES private.memory_records (tenant_id, memory_id),
  CONSTRAINT private_memory_points_exact_identity_unique
    UNIQUE (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version,
            embedding_version, memory_id, source_updated_at, body_sha256),
  CONSTRAINT private_memory_points_live_retired_consistent
    CHECK ((projection_live AND retired_at IS NULL)
        OR (NOT projection_live AND retired_at IS NOT NULL))
);

-- Migrations may run as a deployment/bootstrap role.  Freeze the Canonical owner explicitly
-- instead of relying on whichever role executed this file.
ALTER TABLE projection.private_memory_points OWNER TO role_migration_owner;

COMMENT ON TABLE projection.private_memory_points IS
  'Private Qdrant point_id -> Memory identity registry. PG is the sole binding authority; point ids are opaque and Qdrant payload is never trusted for identity.';
COMMENT ON COLUMN projection.private_memory_points.source_updated_at IS
  'Exact mutable-source fence, not an immutable Memory revision. Resolver compares it with private.memory_records.updated_at before hydrate.';

-- 0012 only blanket-applied tenant RLS to tables that existed at that point.  This later
-- tenant-bearing table needs the same forced policy explicitly.  Domain-default grants from
-- 0011 keep role_gateway SELECT-only and role_retrieval_worker as the projection writer; no
-- gateway write grant is introduced here.
ALTER TABLE projection.private_memory_points ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection.private_memory_points FORCE ROW LEVEL SECURITY;
CREATE POLICY private_memory_points_tenant_isolation ON projection.private_memory_points
  USING (tenant_id = current_setting('humaux.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('humaux.tenant_id', true)::uuid);
