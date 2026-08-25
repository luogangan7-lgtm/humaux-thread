-- §48.0 / §8: Evidence-first data model core + §59.1 Authority columns on memory_records
-- (T0.8). This is the load-bearing file of T1.1: evidence_objects is the unified identity
-- anchor (§8.1), events/artifacts share its PK via real FK (not polymorphic kind+id),
-- memory_records carries the Authority contract (G59-4/G59-5) and the seven time fields
-- (§9), memory_evidence is the sole Memory<->Evidence provenance relation with a DEFERRABLE
-- constraint trigger closing the orphan-Memory hole (§8.6), and evidence_edges is the sole
-- Evidence<->Evidence relation (§8.1 EvidenceEdge, origin_parent_ids[] 取消).

-- ① Evidence identity anchor. DDL is §48.0①, verbatim (data_class closed set per §7.5
-- line "PUBLIC | INTERNAL | PRIVATE | SENSITIVE | SECRET_MATERIAL"; evidence_kind is the
-- two-way EVENT/ARTIFACT split the shared-PK FK below depends on, not the finer Event/
-- Artifact sub-kind, which lives on the subtype tables themselves).
CREATE TABLE private.evidence_objects (
  evidence_id      uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id        uuid NOT NULL REFERENCES control.tenants(tenant_id),
  evidence_kind    text NOT NULL CHECK (evidence_kind IN ('EVENT','ARTIFACT')),
  -- §48.0① construction note: only `evidence::payload_sha256(bytes)` (T1.6, the sole
  -- constructor, G80-22) is allowed to produce this value; this column only enforces
  -- NOT NULL, it cannot enforce "computed by the right function" — that's the
  -- architecture-check's job, not the DDL's (§48.0① 正文).
  payload_sha256   bytea NOT NULL,
  data_class       text NOT NULL CHECK (data_class IN ('PUBLIC','INTERNAL','PRIVATE','SENSITIVE','SECRET_MATERIAL')),
  origin_class     text NOT NULL CHECK (origin_class IN (
                     'DirectUserInput','UserConfirmed','TenantAdmin','AuthenticatedAgent',
                     'TrustedConnector','ToolResult','UploadedArtifact','ExternalContent','SystemMigration')),
  origin_principal_id uuid,
  origin_connector_id uuid,
  visibility_class text NOT NULL CHECK (visibility_class IN ('USER_PRIVATE','WORKSPACE_SHARED','TENANT_SHARED')),
  visibility_user_id uuid REFERENCES control.users(user_id),
  visibility_workspace_id uuid REFERENCES control.workspaces(workspace_id),
  reasoning_domain_id uuid NOT NULL REFERENCES control.private_reasoning_domains(reasoning_domain_id),
  -- §8.1 visibility CHECK: class and the non-NULL scoping column must agree exactly; the
  -- three branches are mutually exclusive and exhaustive over the three visibility classes.
  CONSTRAINT evidence_objects_visibility_matches_class CHECK (
    (visibility_class='USER_PRIVATE' AND visibility_user_id IS NOT NULL AND visibility_workspace_id IS NULL)
 OR (visibility_class='WORKSPACE_SHARED' AND visibility_user_id IS NULL AND visibility_workspace_id IS NOT NULL)
 OR (visibility_class='TENANT_SHARED' AND visibility_user_id IS NULL AND visibility_workspace_id IS NULL)
  ),
  occurred_at      timestamptz,
  observed_at      timestamptz NOT NULL DEFAULT now(),
  created_at       timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE private.evidence_objects IS
  '§8.1 unified Evidence identity anchor; private.events/private.artifacts PK = evidence_id via real FK below.';
COMMENT ON COLUMN private.evidence_objects.payload_sha256 IS
  '§48.0①: raw-byte SHA-256, no trim/NFC/NFKC/transcode; sole constructor evidence::payload_sha256 (T1.6, G80-22).';

CREATE INDEX idx_evidence_objects_tenant ON private.evidence_objects (tenant_id);

-- §8.2 Event — raw, immutable. event_id IS evidence_id (real FK, not a second identity).
CREATE TABLE private.events (
  event_id   uuid PRIMARY KEY REFERENCES private.evidence_objects(evidence_id),
  event_kind text NOT NULL CHECK (event_kind IN (
              'USER_MESSAGE','ASSISTANT_MESSAGE','TOOL_CALL','TOOL_RESULT','MANUAL_NOTE',
              'USER_CORRECTION','TASK_EVENT','GIT_EVENT','SYSTEM_IMPORT')),
  -- §8.2: original payload/structure only, never an LLM-rewritten "better version".
  payload    jsonb NOT NULL
);

COMMENT ON CONSTRAINT events_event_id_fkey ON private.events IS
  '§48.0①: events_evidence_fk — private.events.event_id == evidence_objects.evidence_id (§8.1).';

-- §8.3 Artifact — bytes authority is S3 (§5); this row is identity/hash/manifest/locator.
CREATE TABLE private.artifacts (
  artifact_id        uuid PRIMARY KEY REFERENCES private.evidence_objects(evidence_id),
  artifact_kind      text NOT NULL CHECK (artifact_kind IN (
                      'PDF','DOCX','PPTX','XLSX','IMAGE','MARKDOWN','HTML','TEXT',
                      'REPOSITORY_SNAPSHOT','AUDIO','VIDEO')),
  object_locator     text NOT NULL,
  processing_manifest jsonb NOT NULL DEFAULT '{}'::jsonb
);

COMMENT ON CONSTRAINT artifacts_artifact_id_fkey ON private.artifacts IS
  '§48.0①: artifacts_evidence_fk — private.artifacts.artifact_id == evidence_objects.evidence_id (§8.1).';

-- §15.6 private.ingest_tickets — expected-count anchor for Knowledge Processing
-- Completeness; declared here (not §15) because it FKs into private.events above and its
-- GRANT row is one of the 14 §6.2.2 named tables T1.2 must cover.
CREATE TABLE private.ingest_tickets (
  ticket_id  uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id  uuid NOT NULL REFERENCES control.tenants(tenant_id),
  scope_kind text NOT NULL,
  scope_id   uuid NOT NULL,
  batch_id   uuid NOT NULL,
  client_batch_id text NOT NULL,
  ordinal    int  NOT NULL,
  issued_at  timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  state text NOT NULL DEFAULT 'ISSUED'
    CHECK (state IN ('ISSUED','REDEEMED','EXPIRED')),
  redeemed_event_id uuid REFERENCES private.events(event_id),
  CHECK ((state = 'REDEEMED') = (redeemed_event_id IS NOT NULL)),
  UNIQUE (tenant_id, client_batch_id, ordinal)
);

COMMENT ON TABLE private.ingest_tickets IS
  '§15.6: expected is external — declared by begin_batch (role_batch_issuer, event_id column) before any Evidence is written.';

-- §8.5 MemoryRecord + §59.1 Authority contract (G59-4/G59-5) + §9 seven time fields.
CREATE TABLE private.memory_records (
  memory_id   uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id   uuid NOT NULL REFERENCES control.tenants(tenant_id),
  memory_type text NOT NULL CHECK (memory_type IN (
              'FACT','PREFERENCE','DECISION','REJECTION','STATE','ISSUE','LESSON',
              'CONSTRAINT','PROCEDURE','OUTCOME','REFERENCE','NOTE')),
  content     jsonb NOT NULL,

  -- §6.1.1: private tenant-scoped tables must carry visibility_class + visibility_user_id/
  -- visibility_workspace_id, same three-branch CHECK as evidence_objects (§8.1) — Memory
  -- gets its own copy rather than a join-through-evidence lookup so RLS can filter this
  -- table directly (§6.1.2: every private query needs the disjunction inline, not a
  -- correlated subquery per row).
  visibility_class text NOT NULL CHECK (visibility_class IN ('USER_PRIVATE','WORKSPACE_SHARED','TENANT_SHARED')),
  visibility_user_id uuid REFERENCES control.users(user_id),
  visibility_workspace_id uuid REFERENCES control.workspaces(workspace_id),
  CONSTRAINT memory_records_visibility_matches_class CHECK (
    (visibility_class='USER_PRIVATE' AND visibility_user_id IS NOT NULL AND visibility_workspace_id IS NULL)
 OR (visibility_class='WORKSPACE_SHARED' AND visibility_user_id IS NULL AND visibility_workspace_id IS NOT NULL)
 OR (visibility_class='TENANT_SHARED' AND visibility_user_id IS NULL AND visibility_workspace_id IS NULL)
  ),

  -- §59.1 Authority — four required columns (G59-5) + status/superseded_by pair (G59-4).
  -- Column names carry the `authority_` prefix per G59-5's own wording ("authority_class /
  -- confidence / status / asserted_at 四列 NOT NULL"); `status`/`superseded_by` stay
  -- unprefixed to match the G59-4 CHECK expression quoted verbatim from §59.1.
  authority_class  text NOT NULL CHECK (authority_class IN (
                    'PublicKnowledge','PrivateKnowledge','UserPreference','ProjectDecision',
                    'UserCorrection','ProjectConstraint','ExplicitTaskContext')),
  confidence       real NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
  status           text NOT NULL CHECK (status IN ('active','superseded','revoked','expired')),
  asserted_at      timestamptz NOT NULL,
  superseded_by    uuid REFERENCES private.memory_records(memory_id),
  -- G59-4, quoted verbatim from §59.1: CHECK ((status = 'superseded') = (superseded_by IS NOT NULL))
  CONSTRAINT memory_records_g59_4_status_superseded_by CHECK ((status = 'superseded') = (superseded_by IS NOT NULL)),

  -- §9 seven time fields. occurred_at/effective_* participate in temporal::rank_time /
  -- visible_at (T1.7); observed_at/created_at/updated_at are audit-only and must never be
  -- read by anything sorting or filtering (enforced by MemoryRow's crate-private fields in
  -- T1.7, not by this DDL).
  occurred_at     timestamptz,
  observed_at     timestamptz NOT NULL DEFAULT now(),
  effective_from  timestamptz,
  effective_to    timestamptz,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  superseded_at   timestamptz,

  -- §9: effective_from/effective_to may only carry a value for the state/fact/decision
  -- claim types; every other memory_type is CHECK-forced to NULL (V2 does not restore
  -- decorative full-table bitemporal, §9 first paragraph).
  CONSTRAINT memory_records_effective_only_for_state_fact_decision CHECK (
    memory_type IN ('STATE','FACT','DECISION') OR (effective_from IS NULL AND effective_to IS NULL)
  )
);

COMMENT ON TABLE private.memory_records IS
  '§8.5 MemoryRecord + §59.1 Authority (G59-4/G59-5) + §9 seven time fields (T0.8/T1.1).';
COMMENT ON COLUMN private.memory_records.status IS
  '§59.1 AuthorityStatus, DB wire values lowercase to match the G59-4 CHECK quoted from §59.1 verbatim.';

CREATE INDEX idx_memory_records_tenant ON private.memory_records (tenant_id);

-- §65 "authority superseded_by consistency scan" (daily, §59.1 I4's repair-job half,
-- T0.8). The CHECK above makes violations un-insertable on a healthy schema; this
-- function exists so the daily job counts independently rather than trusting the
-- constraint never to have been dropped (G59-4 注错 b: constraint-side and repair-job-side
-- red/green evidence must be separate, a hardcoded `SELECT 0` cannot stand in for it).
--
-- SECURITY DEFINER + pinned search_path: this is a cluster-wide daily scan (§65), not a
-- tenant request — it must see every row regardless of `humaux.tenant_id`/`humaux.user_id`
-- session context, which the caller (role_maintenance, §6.2.2) never carries for a batch
-- job. A plain STABLE function runs with the *caller's* row security, so under 0012's
-- FORCE ROW LEVEL SECURITY on private.memory_records it would silently read 0 forever —
-- indistinguishable from "no violations" (exactly the hardcoded-SELECT-0 failure mode
-- this function's own comment above says it exists to rule out). Ownership is
-- role_migration_owner (0011's blanket ALTER FUNCTION ... OWNER TO loop, which runs after
-- this file); 0012's memory_records_tenant_and_visibility policy carries an explicit
-- `current_user = 'role_migration_owner'` bypass branch for exactly this function — not
-- BYPASSRLS on any of the frozen 8 roles (§6.2.0), and not weakening FORCE for anyone else.
CREATE FUNCTION ops.i4_authority_consistency_scan() RETURNS bigint
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, private AS $$
  SELECT count(*)
  FROM private.memory_records
  WHERE (status = 'superseded') IS DISTINCT FROM (superseded_by IS NOT NULL);
$$;

COMMENT ON FUNCTION ops.i4_authority_consistency_scan() IS
  '§65 daily job / §59.1 I4: independent violation count for (status=''superseded'') <=> (superseded_by IS NOT NULL). Must read 0 on a schema where memory_records_g59_4_status_superseded_by is intact.';

-- ③ Memory provenance — sole link table (§8.6), verbatim from §48.0③.
CREATE TABLE private.memory_evidence (
  memory_id    uuid NOT NULL REFERENCES private.memory_records(memory_id) ON DELETE CASCADE,
  evidence_id  uuid NOT NULL REFERENCES private.evidence_objects(evidence_id) ON DELETE RESTRICT,
  role         text NOT NULL CHECK (role IN ('PRIMARY','SUPPORTING','CONTRADICTING','CORRECTION')),
  ordinal      int NOT NULL DEFAULT 0,
  created_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (memory_id, evidence_id, role)
);

-- Sole Evidence<->Evidence relation (§8.1 EvidenceEdge); origin_parent_ids[] 取消.
CREATE TABLE private.evidence_edges (
  child_evidence_id  uuid NOT NULL REFERENCES private.evidence_objects(evidence_id) ON DELETE CASCADE,
  parent_evidence_id uuid NOT NULL REFERENCES private.evidence_objects(evidence_id) ON DELETE RESTRICT,
  relation           text NOT NULL CHECK (relation IN ('DERIVED_FROM','IMPORTED_FROM','CORRECTION_OF','SNAPSHOT_OF')),
  ordinal            int NOT NULL DEFAULT 0,
  created_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (child_evidence_id, parent_evidence_id, relation)
);

-- §8.6 / §48.0③: DEFERRABLE INITIALLY DEFERRED constraint trigger — allows the same
-- transaction to INSERT a Memory then its memory_evidence link(s) in either order, but
-- rejects the COMMIT if a memory_id ends up with zero evidence links.
CREATE FUNCTION private.check_memory_has_evidence() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM private.memory_evidence me WHERE me.memory_id = NEW.memory_id
  ) THEN
    RAISE EXCEPTION 'orphan Memory %: no private.memory_evidence link at COMMIT (§8.6)', NEW.memory_id
      USING ERRCODE = 'foreign_key_violation';
  END IF;
  RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER memory_records_requires_evidence
AFTER INSERT ON private.memory_records
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION private.check_memory_has_evidence();

COMMENT ON TRIGGER memory_records_requires_evidence ON private.memory_records IS
  '§8.6/§48.0③: every new Memory must have >=1 private.memory_evidence link by COMMIT time.';
