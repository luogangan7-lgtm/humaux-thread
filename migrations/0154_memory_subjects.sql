-- §6.1.3 (Subject / Aboutness Axis — memory linkage) / §6.2.2 / §46 forward-fix / ADR-0028 (Card 8).
--
-- Card 7 (0153) landed the registry (private.subjects / subject_keys / subject_roles) but nothing
-- links a memory to a subject. This migration lands the linkage and the ONE deterministic resolve
-- hook every production memory/rollup writer calls (§6.1.3 "resolution order"):
--
--   1. explicit SubjectId  → source_kind DECLARED
--   2. exact trusted external key (private.subject_keys, tenant-scoped) → source_kind EXTERNAL_KEY
--   3. structured-evidence rule → source_kind INHERITED: the memory's PRIMARY Evidence's declared
--      subjects (evidence_subjects), the predecessor of a USER_CORRECTION version, and a rollup's
--      source memories. NO model output is ever trusted for linking (research Q7 rejected list).
--
-- Tables (all tenant-scoped, ENABLE + FORCE RLS, owner role_migration_owner, NAMED §6.2.2 grants):
--
--   private.evidence_subjects        — the write-side carrier of a subject declaration made at
--                                      remember.put ("this document is about X"), written in the
--                                      SAME transaction as the Evidence (adapters::remember::
--                                      remember_in_txn). Memory rows are born asynchronously in
--                                      the Distill hop (ADR-0016), so the declaration must
--                                      survive on the Evidence until then.
--   private.memory_subjects          — the linkage of record (D-A): M:N memory ↔ subject with the
--                                      closed relation/source_kind sets and confidence_bp.
--   private.memory_subject_mentions  — revision-bound byte-span locators (D-A): where in the
--                                      exact stored revision a subject is mentioned, so a later
--                                      erase (research Q10) can rewrite deterministically.
--   private.memory_rollup_subjects   — a consolidation rollup inherits its source memories'
--                                      subjects (mirrors memory_rollup_sources ↔ memory_evidence).
--
-- Every FK into private.subjects / memory_records / memory_rollups / evidence_objects carries the
-- tenant leg (0104/0153 same law): RI checks bypass RLS, so a single-column FK would let tenant B
-- attach rows to — and probe the existence of — tenant A's ids. memory_rollups had no
-- (tenant_id, rollup_id) UNIQUE yet; it is added here as constrained redundancy (0104 shape).
--
-- The hook itself is private.link_memory_subjects(...) / private.link_rollup_subjects(...): plain
-- SECURITY INVOKER SQL (runs under the caller's role + RLS, needs the caller's NAMED grants —
-- never a definer bypass). One body, three callers: adapters::distill_repo::insert_memory (the
-- single Rust INSERT INTO private.memory_records — Distill hop, memory.confirm, memory.correct all
-- route through it), the explicit-link pass of memory.confirm / memory.correct (same transaction
-- as the insert), and the correction trigger below. There is NO post-commit declaration path:
-- a remember.put declaration is committed with its Evidence, before the outbox row is visible to
-- the Distill hop, so no lock or back-fill is needed to keep the two in step.
--
-- §46 forward-fix (EXPAND_CONTRACT, next free number after 0153). No DML; the only existing objects
-- touched are memory_rollups (one UNIQUE constraint) and memory_records (one AFTER UPDATE trigger).

-- ============================================================================
-- A. Constrained redundancy on memory_rollups so rollup children can carry the tenant leg.
-- ============================================================================
ALTER TABLE private.memory_rollups
  ADD CONSTRAINT memory_rollups_tenant_id_unique UNIQUE (tenant_id, rollup_id);

-- ============================================================================
-- B. Tables.
-- ============================================================================

-- Write-side declaration carrier (remember.put subject_ids / subject_keys). source_kind is the
-- resolve rule that produced the id — DECLARED (explicit SubjectId) or EXTERNAL_KEY (exact trusted
-- key). §78.2 DB<->Rust contract with humaux_domain::subject::SubjectLinkSource (INHERITED never
-- appears here: a declaration is by definition not inherited).
CREATE TABLE private.evidence_subjects (
  tenant_id   uuid NOT NULL REFERENCES control.tenants(tenant_id) ON DELETE CASCADE,
  evidence_id uuid NOT NULL,
  subject_id  uuid NOT NULL,
  source_kind text NOT NULL CHECK (source_kind IN ('DECLARED', 'EXTERNAL_KEY')),
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, evidence_id, subject_id),
  CONSTRAINT evidence_subjects_evidence_id_fkey
    FOREIGN KEY (tenant_id, evidence_id) REFERENCES private.evidence_objects (tenant_id, evidence_id)
    ON DELETE CASCADE,
  CONSTRAINT evidence_subjects_subject_id_fkey
    FOREIGN KEY (tenant_id, subject_id) REFERENCES private.subjects (tenant_id, subject_id)
    ON DELETE CASCADE
);

-- The linkage of record. relation = SubjectLinkRelation {ABOUT, MENTIONS}; source_kind =
-- SubjectLinkSource {DECLARED, EXTERNAL_KEY, INHERITED}; confidence_bp in basis points
-- (10000 = certain — every deterministic rule in this card is certain; a later probabilistic
-- extractor writes lower values).
CREATE TABLE private.memory_subjects (
  tenant_id     uuid NOT NULL REFERENCES control.tenants(tenant_id) ON DELETE CASCADE,
  memory_id     uuid NOT NULL,
  subject_id    uuid NOT NULL,
  relation      text NOT NULL CHECK (relation IN ('ABOUT', 'MENTIONS')),
  source_kind   text NOT NULL CHECK (source_kind IN ('DECLARED', 'EXTERNAL_KEY', 'INHERITED')),
  confidence_bp smallint NOT NULL CHECK (confidence_bp BETWEEN 0 AND 10000),
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, memory_id, subject_id),
  CONSTRAINT memory_subjects_memory_id_fkey
    FOREIGN KEY (tenant_id, memory_id) REFERENCES private.memory_records (tenant_id, memory_id)
    ON DELETE CASCADE,
  CONSTRAINT memory_subjects_subject_id_fkey
    FOREIGN KEY (tenant_id, subject_id) REFERENCES private.subjects (tenant_id, subject_id)
    ON DELETE CASCADE
);

-- Revision-bound byte spans. revision_sha256 = sha256 of the exact stored revision bytes
-- (convert_to(memory_records.content::text, 'UTF8') — jsonb's canonical rendering, the only
-- byte string both this writer and a later erase can recompute identically); [span_start,
-- span_end) is a 0-based byte range into those bytes.
CREATE TABLE private.memory_subject_mentions (
  tenant_id       uuid NOT NULL REFERENCES control.tenants(tenant_id) ON DELETE CASCADE,
  memory_id       uuid NOT NULL,
  subject_id      uuid NOT NULL,
  revision_sha256 bytea NOT NULL CHECK (octet_length(revision_sha256) = 32),
  span_start      integer NOT NULL CHECK (span_start >= 0),
  span_end        integer NOT NULL,
  CONSTRAINT memory_subject_mentions_span_check CHECK (span_end > span_start),
  PRIMARY KEY (tenant_id, memory_id, subject_id, revision_sha256, span_start),
  CONSTRAINT memory_subject_mentions_link_fkey
    FOREIGN KEY (tenant_id, memory_id, subject_id)
    REFERENCES private.memory_subjects (tenant_id, memory_id, subject_id)
    ON DELETE CASCADE
);

-- Rollup inheritance (§11.6 "a rollup expands back to its sources" — and to their subjects).
CREATE TABLE private.memory_rollup_subjects (
  tenant_id   uuid NOT NULL REFERENCES control.tenants(tenant_id) ON DELETE CASCADE,
  rollup_id   uuid NOT NULL,
  subject_id  uuid NOT NULL,
  source_kind text NOT NULL CHECK (source_kind IN ('INHERITED')),
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, rollup_id, subject_id),
  CONSTRAINT memory_rollup_subjects_rollup_id_fkey
    FOREIGN KEY (tenant_id, rollup_id) REFERENCES private.memory_rollups (tenant_id, rollup_id)
    ON DELETE CASCADE,
  CONSTRAINT memory_rollup_subjects_subject_id_fkey
    FOREIGN KEY (tenant_id, subject_id) REFERENCES private.subjects (tenant_id, subject_id)
    ON DELETE CASCADE
);

-- Reverse lookups ("every memory about subject S") and FK cascade legs.
CREATE INDEX evidence_subjects_subject_idx      ON private.evidence_subjects (tenant_id, subject_id);
CREATE INDEX memory_subjects_subject_idx        ON private.memory_subjects (tenant_id, subject_id);
CREATE INDEX memory_rollup_subjects_subject_idx ON private.memory_rollup_subjects (tenant_id, subject_id);

-- ============================================================================
-- C. RLS: §62 tenant clause AND the parent row's own visibility (memory_evidence /
--    memory_rollup_sources shape, 0012). The tenant clause is spelled literally so the §48.2
--    four-item enumeration recognises it; the EXISTS delegates "may this caller see the parent"
--    to the parent table's policy, so no visibility disjunction is re-derived here.
-- ============================================================================
ALTER TABLE private.evidence_subjects ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.evidence_subjects FORCE ROW LEVEL SECURITY;
CREATE POLICY evidence_subjects_tenant_and_parent ON private.evidence_subjects
  USING (tenant_id = current_setting('humaux.tenant_id', true)::uuid
         AND EXISTS (SELECT 1 FROM private.evidence_objects e
                     WHERE e.evidence_id = evidence_subjects.evidence_id
                       AND e.tenant_id = evidence_subjects.tenant_id))
  WITH CHECK (tenant_id = current_setting('humaux.tenant_id', true)::uuid
         AND EXISTS (SELECT 1 FROM private.evidence_objects e
                     WHERE e.evidence_id = evidence_subjects.evidence_id
                       AND e.tenant_id = evidence_subjects.tenant_id));

ALTER TABLE private.memory_subjects ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.memory_subjects FORCE ROW LEVEL SECURITY;
CREATE POLICY memory_subjects_tenant_and_parent ON private.memory_subjects
  USING (tenant_id = current_setting('humaux.tenant_id', true)::uuid
         AND EXISTS (SELECT 1 FROM private.memory_records mr
                     WHERE mr.memory_id = memory_subjects.memory_id
                       AND mr.tenant_id = memory_subjects.tenant_id))
  WITH CHECK (tenant_id = current_setting('humaux.tenant_id', true)::uuid
         AND EXISTS (SELECT 1 FROM private.memory_records mr
                     WHERE mr.memory_id = memory_subjects.memory_id
                       AND mr.tenant_id = memory_subjects.tenant_id));

ALTER TABLE private.memory_subject_mentions ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.memory_subject_mentions FORCE ROW LEVEL SECURITY;
CREATE POLICY memory_subject_mentions_tenant_and_parent ON private.memory_subject_mentions
  USING (tenant_id = current_setting('humaux.tenant_id', true)::uuid
         AND EXISTS (SELECT 1 FROM private.memory_records mr
                     WHERE mr.memory_id = memory_subject_mentions.memory_id
                       AND mr.tenant_id = memory_subject_mentions.tenant_id))
  WITH CHECK (tenant_id = current_setting('humaux.tenant_id', true)::uuid
         AND EXISTS (SELECT 1 FROM private.memory_records mr
                     WHERE mr.memory_id = memory_subject_mentions.memory_id
                       AND mr.tenant_id = memory_subject_mentions.tenant_id));

ALTER TABLE private.memory_rollup_subjects ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.memory_rollup_subjects FORCE ROW LEVEL SECURITY;
CREATE POLICY memory_rollup_subjects_tenant_and_parent ON private.memory_rollup_subjects
  USING (tenant_id = current_setting('humaux.tenant_id', true)::uuid
         AND EXISTS (SELECT 1 FROM private.memory_rollups r
                     WHERE r.rollup_id = memory_rollup_subjects.rollup_id
                       AND r.tenant_id = memory_rollup_subjects.tenant_id))
  WITH CHECK (tenant_id = current_setting('humaux.tenant_id', true)::uuid
         AND EXISTS (SELECT 1 FROM private.memory_rollups r
                     WHERE r.rollup_id = memory_rollup_subjects.rollup_id
                       AND r.tenant_id = memory_rollup_subjects.tenant_id));

-- ============================================================================
-- D. The shared deterministic resolve hook.
-- ============================================================================

-- Links `p_memory_id` to its subjects, idempotently, from three deterministic sources and
-- records byte-span mentions. SECURITY INVOKER: runs under the caller's role/RLS.
--   p_subject_ids / p_source_kinds — explicit links the caller already resolved (parallel
--   arrays; NULL when none): rule 1 DECLARED, rule 2 EXTERNAL_KEY.
--   rule 3a — the memory's PRIMARY Evidence's declarations (evidence_subjects) → INHERITED
--   (§6.1.3 ③: this is how a remember.put declaration reaches the Distill-born memory; the
--   Evidence row keeps whether the declaration was by id or by key).
--   rule 3b — a USER_CORRECTION version (§Q4/ADR-0025) inherits the predecessor's links
--   (INHERITED): only when the predecessor's superseded_by names this memory AND this memory's
--   PRIMARY Evidence is a USER_CORRECTION event; an explicit memory.supersede never inherits.
-- Mentions: for every linked subject, the first exact occurrence of its display_name or any of
-- its key values inside the stored revision bytes becomes one [span_start, span_end) row bound
-- to sha256(revision). Deterministic substring match only — no model, no fuzzy match.
CREATE FUNCTION private.link_memory_subjects(
  p_tenant_id    uuid,
  p_memory_id    uuid,
  p_subject_ids  uuid[] DEFAULT NULL,
  p_source_kinds text[] DEFAULT NULL
) RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_linked integer := 0;
  v_rows integer;
BEGIN
  IF (p_subject_ids IS NULL) <> (p_source_kinds IS NULL)
     OR coalesce(array_length(p_subject_ids, 1), 0) <> coalesce(array_length(p_source_kinds, 1), 0) THEN
    RAISE EXCEPTION 'link_memory_subjects: subject/source arrays must be parallel'
      USING ERRCODE = 'invalid_parameter_value';
  END IF;

  -- Rules 1/2 (explicit, already resolved by the caller under this tenant's RLS).
  IF p_subject_ids IS NOT NULL THEN
    INSERT INTO private.memory_subjects (tenant_id, memory_id, subject_id, relation, source_kind, confidence_bp)
    SELECT p_tenant_id, p_memory_id, s.id, 'ABOUT', s.kind, 10000
      FROM unnest(p_subject_ids, p_source_kinds) AS s(id, kind)
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_linked := v_linked + v_rows;
  END IF;

  -- Rule 3a: the PRIMARY Evidence's declarations (INHERITED — §6.1.3 ③).
  INSERT INTO private.memory_subjects (tenant_id, memory_id, subject_id, relation, source_kind, confidence_bp)
  SELECT DISTINCT es.tenant_id, p_memory_id, es.subject_id, 'ABOUT', 'INHERITED', 10000
    FROM private.memory_evidence me
    JOIN private.evidence_subjects es
      ON es.tenant_id = p_tenant_id AND es.evidence_id = me.evidence_id
   WHERE me.memory_id = p_memory_id AND me.role = 'PRIMARY'
  ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS v_rows = ROW_COUNT;
  v_linked := v_linked + v_rows;

  -- Rule 3b: a USER_CORRECTION version inherits its predecessor's links.
  INSERT INTO private.memory_subjects (tenant_id, memory_id, subject_id, relation, source_kind, confidence_bp)
  SELECT DISTINCT ms.tenant_id, p_memory_id, ms.subject_id, ms.relation, 'INHERITED', ms.confidence_bp
    FROM private.memory_records prev
    JOIN private.memory_subjects ms
      ON ms.tenant_id = p_tenant_id AND ms.memory_id = prev.memory_id
   WHERE prev.tenant_id = p_tenant_id AND prev.superseded_by = p_memory_id
     AND EXISTS (SELECT 1 FROM private.memory_evidence me
                 JOIN private.events ev ON ev.event_id = me.evidence_id
                 WHERE me.memory_id = p_memory_id AND me.role = 'PRIMARY'
                   AND ev.event_kind = 'USER_CORRECTION')
  ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS v_rows = ROW_COUNT;
  v_linked := v_linked + v_rows;

  -- Mentions: first exact byte occurrence of display_name / any key value in the stored revision.
  INSERT INTO private.memory_subject_mentions
    (tenant_id, memory_id, subject_id, revision_sha256, span_start, span_end)
  SELECT p_tenant_id, p_memory_id, hit.subject_id, sha256(rev.body),
         hit.pos - 1, hit.pos - 1 + octet_length(hit.needle)
    FROM (SELECT convert_to(m.content::text, 'UTF8') AS body
            FROM private.memory_records m
           WHERE m.tenant_id = p_tenant_id AND m.memory_id = p_memory_id) rev,
         LATERAL (
           SELECT ms.subject_id, n.needle, position(n.needle IN rev.body) AS pos
             FROM private.memory_subjects ms
             JOIN LATERAL (
               SELECT convert_to(s.display_name, 'UTF8') AS needle
                 FROM private.subjects s
                WHERE s.tenant_id = p_tenant_id AND s.subject_id = ms.subject_id
               UNION
               SELECT convert_to(k.key_value, 'UTF8')
                 FROM private.subject_keys k
                WHERE k.tenant_id = p_tenant_id AND k.subject_id = ms.subject_id
             ) n ON octet_length(n.needle) > 0
            WHERE ms.tenant_id = p_tenant_id AND ms.memory_id = p_memory_id
         ) hit
   WHERE hit.pos > 0
  ON CONFLICT DO NOTHING;

  RETURN v_linked;
END;
$$;

-- A rollup inherits the union of its source memories' subjects (INHERITED). SECURITY INVOKER:
-- role_consolidation_worker's own SELECT on memory_subjects / memory_rollup_sources and INSERT
-- on memory_rollup_subjects.
CREATE FUNCTION private.link_rollup_subjects(
  p_tenant_id uuid,
  p_rollup_id uuid
) RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_rows integer;
BEGIN
  INSERT INTO private.memory_rollup_subjects (tenant_id, rollup_id, subject_id, source_kind)
  SELECT DISTINCT p_tenant_id, p_rollup_id, ms.subject_id, 'INHERITED'
    FROM private.memory_rollup_sources rs
    JOIN private.memory_subjects ms
      ON ms.tenant_id = p_tenant_id AND ms.memory_id = rs.memory_id
   WHERE rs.rollup_id = p_rollup_id
  ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$;

-- Correction inheritance fires where the correction becomes visible: the arbiter UPDATE that sets
-- superseded_by (memory_governance_repo::correct_atomically, same transaction as M2's insert).
-- link_memory_subjects's rule 3b is a no-op for a non-correction successor, so memory.supersede
-- and memory.restore pass through unchanged.
CREATE FUNCTION private.memory_subjects_on_supersede() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM private.link_memory_subjects(NEW.tenant_id, NEW.superseded_by);
  RETURN NULL;
END;
$$;

CREATE TRIGGER memory_subjects_on_supersede
  AFTER UPDATE OF superseded_by ON private.memory_records
  FOR EACH ROW
  WHEN (NEW.superseded_by IS NOT NULL AND NEW.superseded_by IS DISTINCT FROM OLD.superseded_by)
  EXECUTE FUNCTION private.memory_subjects_on_supersede();

ALTER FUNCTION private.link_memory_subjects(uuid, uuid, uuid[], text[]) OWNER TO role_migration_owner;
ALTER FUNCTION private.link_rollup_subjects(uuid, uuid) OWNER TO role_migration_owner;
ALTER FUNCTION private.memory_subjects_on_supersede() OWNER TO role_migration_owner;

COMMENT ON FUNCTION private.link_memory_subjects(uuid, uuid, uuid[], text[]) IS
  '§6.1.3 deterministic resolve hook (ADR-0028, migration 0154): explicit DECLARED/EXTERNAL_KEY '
  'links + PRIMARY-Evidence declarations (INHERITED) + USER_CORRECTION predecessor inheritance + '
  'byte-span mentions over sha256(convert_to(content::text)). SECURITY INVOKER, idempotent. '
  'Sole implementation; every memory writer calls it.';
COMMENT ON FUNCTION private.link_rollup_subjects(uuid, uuid) IS
  '§6.1.3 rollup inheritance (ADR-0028): a rollup links to the union of its source memories'' '
  'subjects (INHERITED). Called by consolidate_repo::publish_rollup in the publish transaction.';

-- ============================================================================
-- E. Owner + NAMED §6.2.2 grants (0152/0153 recipe: re-own, strip the §6.2.1 private-domain
--    defaults, grant the matrix back). evidence_subjects: gateway declares (INSERT) and reads,
--    private_worker/retrieval_worker/maintenance read. memory_subjects + mentions: gateway and
--    private_worker write (both hold INSERT on memory_records — the hook runs under whichever
--    role inserted the memory), consolidation_worker/retrieval_worker/maintenance read.
--    memory_rollup_subjects: consolidation_worker writes, gateway/retrieval_worker/maintenance read.
-- ============================================================================
ALTER TABLE private.evidence_subjects       OWNER TO role_migration_owner;
ALTER TABLE private.memory_subjects         OWNER TO role_migration_owner;
ALTER TABLE private.memory_subject_mentions OWNER TO role_migration_owner;
ALTER TABLE private.memory_rollup_subjects  OWNER TO role_migration_owner;

REVOKE ALL ON private.evidence_subjects, private.memory_subjects,
              private.memory_subject_mentions, private.memory_rollup_subjects
  FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
       role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;

GRANT SELECT, INSERT ON private.evidence_subjects TO role_gateway;
GRANT SELECT         ON private.evidence_subjects TO role_private_worker, role_retrieval_worker, role_maintenance;

GRANT SELECT, INSERT ON private.memory_subjects TO role_gateway, role_private_worker;
GRANT SELECT         ON private.memory_subjects TO role_consolidation_worker, role_retrieval_worker, role_maintenance;

GRANT SELECT, INSERT ON private.memory_subject_mentions TO role_gateway, role_private_worker;
GRANT SELECT         ON private.memory_subject_mentions TO role_consolidation_worker, role_retrieval_worker, role_maintenance;

GRANT SELECT, INSERT ON private.memory_rollup_subjects TO role_consolidation_worker;
GRANT SELECT         ON private.memory_rollup_subjects TO role_gateway, role_retrieval_worker, role_maintenance;

COMMENT ON TABLE private.evidence_subjects IS
  '§6.1.3 write-side subject declaration on an Evidence (remember.put subject_ids/subject_keys, '
  'same transaction as the Evidence); the Distill-born memory inherits it (INHERITED) through '
  'link_memory_subjects. ADR-0028, card 8.';
COMMENT ON TABLE private.memory_subjects IS
  '§6.1.3 memory<->subject linkage of record: closed relation/source_kind, confidence_bp. '
  'ADR-0028, card 8.';
COMMENT ON TABLE private.memory_subject_mentions IS
  '§6.1.3 revision-bound byte spans of subject mentions (sha256 of convert_to(content::text)), '
  'for deterministic erasure (research Q10). ADR-0028, card 8.';
COMMENT ON TABLE private.memory_rollup_subjects IS
  '§6.1.3 rollup<->subject inheritance from memory_rollup_sources. ADR-0028, card 8.';
