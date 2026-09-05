-- §6.1.1 / §6.1.3 (new: Subject / Aboutness Axis) / §46 forward-fix / ADR-0027 (Card 7).
--
-- Two things land together, both foundations the subject-scoped-memory design (research Q7/Q8)
-- rests on and nothing else can be built without:
--
-- A. private.visibility_allowed(p_class, p_tenant_ok, p_user_ok, p_workspace_ok) — the single
--    SQL home of the §6.1.1 intra-tenant visibility disjunction that today is inlined three
--    times (0012's evidence_objects / memory_records / memory_rollups policies) and mirrored in
--    Rust at humaux_domain::identity::can_read. A pure IMMUTABLE PARALLEL SAFE lookup: given the
--    row's visibility_class and the three per-arm booleans the caller already computed, it picks
--    the arm matching the class (TENANT_SHARED→p_tenant_ok, USER_PRIVATE→p_user_ok,
--    WORKSPACE_SHARED→p_workspace_ok, anything else→false) — algebraically identical to the OR
--    chain it replaces. The three policies are then re-pointed at it via ALTER POLICY (§46: never
--    edit 0012; forward-fix only), preserving every headless-role arm 0140/0145/0146/0147 added.
--
--    Eager-evaluation guard: extracting the disjunction into a function makes its arguments
--    evaluated eagerly (SQL evaluates call arguments before the call), where the original OR
--    chain short-circuited them. A previously-skipped `current_setting('humaux.user_id',
--    true)::uuid` on a session with an unset user_id GUC (empty string) would newly raise
--    `invalid input syntax for type uuid: ""`. Every user_id cast passed into the function is
--    therefore wrapped `NULLIF(current_setting(...), '')::uuid` — empty→NULL→arm false, never an
--    error. This converts a would-be error into a fail-closed miss and changes no legitimate
--    read (a valid uuid GUC is untouched by NULLIF; only the unset case, which the original would
--    have errored on, becomes a clean false). Tenant casts are left byte-for-byte as the live
--    policies have them (they already sit at top-level AND, always eagerly evaluated).
--
-- B. The subject registry (§6.1.3): private.subjects (the aboutness anchor — a Person or
--    Organisation a memory can be *about*), private.subject_keys (tenant-scoped external-identity
--    dedup keys, never globally unique — a CRM id means nothing across tenants), private.
--    subject_roles (Customer today; the role a subject plays, orthogonal to ScopeKind). All three
--    are tenant-scoped (§62 tenant policy, FORCE RLS, owner role_migration_owner) with NAMED
--    §6.2.2 grants: gateway/private_worker SELECT+INSERT (registration is a user action; card 8's
--    deterministic resolve hook writes from the private worker), retrieval_worker/maintenance
--    SELECT, every other role —.
--
-- §46 forward-fix (EXPAND_CONTRACT, next free number after 0152). No DML; the only existing
-- objects touched are the three policies (ALTER POLICY, same shape prior policy migrations use).

-- ============================================================================
-- A. private.visibility_allowed — single SQL home of the §6.1.1 visibility disjunction.
-- ============================================================================
CREATE FUNCTION private.visibility_allowed(
  p_class        text,
  p_tenant_ok    boolean,
  p_user_ok      boolean,
  p_workspace_ok boolean
) RETURNS boolean
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
RETURN CASE p_class
  WHEN 'TENANT_SHARED'    THEN p_tenant_ok
  WHEN 'USER_PRIVATE'     THEN p_user_ok
  WHEN 'WORKSPACE_SHARED' THEN p_workspace_ok
  ELSE false
END;

-- The visibility predicate of three private tables is security code: owned by role_migration_owner
-- like every other guard function (as 0104 does explicitly — 0011's blanket re-own loop has already
-- run and cannot reach a function created after it).
ALTER FUNCTION private.visibility_allowed(text, boolean, boolean, boolean) OWNER TO role_migration_owner;

COMMENT ON FUNCTION private.visibility_allowed(text, boolean, boolean, boolean) IS
  '§6.1.1 intra-tenant visibility disjunction, single SQL home (ADR-0027, migration 0153). Pure '
  'lookup: TENANT_SHARED→p_tenant_ok, USER_PRIVATE→p_user_ok, WORKSPACE_SHARED→p_workspace_ok, '
  'else false. Mirrors humaux_domain::identity::can_read; the exhaustive scope×class parity test '
  'pins the two in agreement. Callers pass the per-arm booleans they already compute (guarding '
  'user_id casts with NULLIF so eager argument evaluation cannot error on an unset GUC).';

-- Re-point 0012's three policies at the function (§46 forward-fix). Each keeps its tenant
-- boundary and every headless-role arm exactly as the live policy carries it (0140/0145/0146/0147);
-- only the user-facing three-arm visibility disjunction becomes a visibility_allowed(...) call.

-- evidence_objects: tenant + visibility (no role arm; the retrieval-worker read is a separate
-- policy, evidence_objects_retrieval_worker_read, untouched here). USING == WITH CHECK.
ALTER POLICY evidence_objects_tenant_and_visibility ON private.evidence_objects
USING (
  tenant_id = current_setting('humaux.tenant_id', true)::uuid
  AND private.visibility_allowed(visibility_class, true,
        visibility_user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid,
        EXISTS (SELECT 1 FROM control.memberships m
                WHERE m.tenant_id = evidence_objects.tenant_id
                  AND m.user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid
                  AND m.state = 'ACTIVE'))
)
WITH CHECK (
  tenant_id = current_setting('humaux.tenant_id', true)::uuid
  AND private.visibility_allowed(visibility_class, true,
        visibility_user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid,
        EXISTS (SELECT 1 FROM control.memberships m
                WHERE m.tenant_id = evidence_objects.tenant_id
                  AND m.user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid
                  AND m.state = 'ACTIVE'))
);

-- memory_records: role_migration_owner cluster-scan arm (§59.1 I4) + headless read arm
-- (0140/0145) on USING; private_worker constrained write arm (0147) on WITH CHECK. Both keep the
-- user-facing disjunction as visibility_allowed(...).
ALTER POLICY memory_records_tenant_and_visibility ON private.memory_records
USING (
  current_user = 'role_migration_owner'
  OR (
    tenant_id = current_setting('humaux.tenant_id', true)::uuid
    AND (
      current_user IN ('role_retrieval_worker', 'role_consolidation_worker', 'role_private_worker')
      OR private.visibility_allowed(visibility_class, true,
           visibility_user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid,
           EXISTS (SELECT 1 FROM control.memberships m
                   WHERE m.tenant_id = memory_records.tenant_id
                     AND m.user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid
                     AND m.state = 'ACTIVE'))
    )
  )
)
WITH CHECK (
  tenant_id = current_setting('humaux.tenant_id', true)::uuid
  AND (
    (current_user = 'role_private_worker'
     AND (visibility_class = 'TENANT_SHARED'
          OR (visibility_class = 'WORKSPACE_SHARED' AND visibility_workspace_id IS NOT NULL)
          OR (visibility_class = 'USER_PRIVATE' AND visibility_user_id IS NOT NULL)))
    OR private.visibility_allowed(visibility_class, true,
         visibility_user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid,
         EXISTS (SELECT 1 FROM control.memberships m
                 WHERE m.tenant_id = memory_records.tenant_id
                   AND m.user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid
                   AND m.state = 'ACTIVE'))
  )
);

-- memory_rollups: headless read arm (0146) on USING; consolidation_worker constrained write arm
-- (0146) on WITH CHECK.
ALTER POLICY memory_rollups_tenant_and_visibility ON private.memory_rollups
USING (
  tenant_id = current_setting('humaux.tenant_id', true)::uuid
  AND (
    current_user IN ('role_retrieval_worker', 'role_consolidation_worker', 'role_private_worker')
    OR private.visibility_allowed(visibility_class, true,
         visibility_user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid,
         EXISTS (SELECT 1 FROM control.memberships m
                 WHERE m.tenant_id = memory_rollups.tenant_id
                   AND m.user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid
                   AND m.state = 'ACTIVE'))
  )
)
WITH CHECK (
  tenant_id = current_setting('humaux.tenant_id', true)::uuid
  AND (
    (current_user = 'role_consolidation_worker'
     AND (visibility_class = 'TENANT_SHARED'
          OR (visibility_class = 'WORKSPACE_SHARED' AND visibility_workspace_id IS NOT NULL)))
    OR private.visibility_allowed(visibility_class, true,
         visibility_user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid,
         EXISTS (SELECT 1 FROM control.memberships m
                 WHERE m.tenant_id = memory_rollups.tenant_id
                   AND m.user_id = NULLIF(current_setting('humaux.user_id', true), '')::uuid
                   AND m.state = 'ACTIVE'))
  )
);

-- ============================================================================
-- B. Subject registry (§6.1.3). Three tenant-scoped tables, owner role_migration_owner.
-- ============================================================================

-- private.subjects — the aboutness anchor. `kind` is the closed SubjectKind set (§78.2 DB<->Rust
-- contract with humaux_domain::subject::SubjectKind). `merged_into` is a self-FK for subject
-- de-duplication/merge (NULL = a live head; non-NULL = merged away into the named subject).
CREATE TABLE private.subjects (
  subject_id   uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id    uuid NOT NULL REFERENCES control.tenants(tenant_id) ON DELETE CASCADE,
  kind         text NOT NULL CHECK (kind IN ('PERSON', 'ORGANISATION')),
  display_name text NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  -- Self-FK: the subject this one was merged into (NULL = current head).
  merged_into  uuid,
  -- Constrained redundancy (same shape as 0104's evidence_objects_tenant_id_unique): lets every
  -- child FK carry the tenant leg. RI checks bypass RLS, so a single-column FK on subject_id would
  -- let tenant B attach rows to (and probe the existence of) tenant A's subject ids.
  CONSTRAINT subjects_tenant_id_unique UNIQUE (tenant_id, subject_id),
  CONSTRAINT subjects_merged_into_fkey
    FOREIGN KEY (tenant_id, merged_into) REFERENCES private.subjects (tenant_id, subject_id)
    ON DELETE SET NULL (merged_into)
);

-- private.subject_keys — external-identity dedup keys. NEVER globally unique: the UNIQUE carries
-- tenant_id, so the same CRM id in two tenants is two distinct keys. `key_kind` is the closed
-- SubjectKeyKind set.
CREATE TABLE private.subject_keys (
  subject_key_id uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id      uuid NOT NULL REFERENCES control.tenants(tenant_id) ON DELETE CASCADE,
  subject_id     uuid NOT NULL,
  key_kind       text NOT NULL CHECK (key_kind IN ('CRM')),
  key_value      text NOT NULL,
  created_at     timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, key_kind, key_value),
  -- Tenant-leg FK (see subjects_tenant_id_unique): a key can only point at its own tenant's subject.
  CONSTRAINT subject_keys_subject_id_fkey
    FOREIGN KEY (tenant_id, subject_id) REFERENCES private.subjects (tenant_id, subject_id)
    ON DELETE CASCADE
);

-- private.subject_roles — the role a subject plays (Customer today). Orthogonal to ScopeKind:
-- Customer is a role a subject *is*, not a scope a memory is *filed under*.
CREATE TABLE private.subject_roles (
  tenant_id  uuid NOT NULL REFERENCES control.tenants(tenant_id) ON DELETE CASCADE,
  subject_id uuid NOT NULL,
  role       text NOT NULL CHECK (role IN ('CUSTOMER')),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, subject_id, role),
  -- Tenant-leg FK (see subjects_tenant_id_unique); the PK's leading columns already index it.
  CONSTRAINT subject_roles_subject_id_fkey
    FOREIGN KEY (tenant_id, subject_id) REFERENCES private.subjects (tenant_id, subject_id)
    ON DELETE CASCADE
);

-- FK-column indexes for cascade/lookup on the composite (tenant_id, subject_id) legs.
CREATE INDEX subject_keys_subject_id_idx ON private.subject_keys (tenant_id, subject_id);
CREATE INDEX subjects_merged_into_idx ON private.subjects (tenant_id, merged_into) WHERE merged_into IS NOT NULL;

-- §62 tenant isolation: ENABLE + FORCE RLS, USING/WITH CHECK on tenant_id == session GUC.
ALTER TABLE private.subjects ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.subjects FORCE ROW LEVEL SECURITY;
CREATE POLICY subjects_tenant_isolation ON private.subjects
  USING (tenant_id = current_setting('humaux.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('humaux.tenant_id', true)::uuid);

ALTER TABLE private.subject_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.subject_keys FORCE ROW LEVEL SECURITY;
CREATE POLICY subject_keys_tenant_isolation ON private.subject_keys
  USING (tenant_id = current_setting('humaux.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('humaux.tenant_id', true)::uuid);

ALTER TABLE private.subject_roles ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.subject_roles FORCE ROW LEVEL SECURITY;
CREATE POLICY subject_roles_tenant_isolation ON private.subject_roles
  USING (tenant_id = current_setting('humaux.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('humaux.tenant_id', true)::uuid);

-- Owner + NAMED §6.2.2 grants. The migration connection creates these as its own login role, and
-- the private-schema §6.2.1 default privileges auto-grant every runtime role its domain default —
-- so, exactly as 0152 does, each table is re-owned to role_migration_owner and REVOKE ALL strips
-- the inherited defaults down to nothing before the NAMED matrix is granted back (a NAMED §6.2.2
-- table overrides the §6.2.1 default entirely; a role absent below gets nothing, not the default).
-- gateway/private_worker register (SELECT+INSERT); retrieval_worker/maintenance read only.
ALTER TABLE private.subjects      OWNER TO role_migration_owner;
ALTER TABLE private.subject_keys  OWNER TO role_migration_owner;
ALTER TABLE private.subject_roles OWNER TO role_migration_owner;

REVOKE ALL ON private.subjects, private.subject_keys, private.subject_roles FROM PUBLIC,
  role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;

GRANT SELECT, INSERT ON private.subjects      TO role_gateway, role_private_worker;
GRANT SELECT         ON private.subjects      TO role_retrieval_worker, role_maintenance;
GRANT SELECT, INSERT ON private.subject_keys  TO role_gateway, role_private_worker;
GRANT SELECT         ON private.subject_keys  TO role_retrieval_worker, role_maintenance;
GRANT SELECT, INSERT ON private.subject_roles TO role_gateway, role_private_worker;
GRANT SELECT         ON private.subject_roles TO role_retrieval_worker, role_maintenance;

COMMENT ON TABLE private.subjects IS
  '§6.1.3 aboutness anchor: a Person or Organisation a memory can be about. Tenant-scoped, closed '
  'SubjectKind, self-FK merged_into for de-dup. ADR-0027, card 7.';
COMMENT ON TABLE private.subject_keys IS
  '§6.1.3 external-identity dedup keys, tenant-scoped, never globally unique (UNIQUE carries '
  'tenant_id). ADR-0027, card 7.';
COMMENT ON TABLE private.subject_roles IS
  '§6.1.3 the role a subject plays (Customer today), orthogonal to ScopeKind. ADR-0027, card 7.';
