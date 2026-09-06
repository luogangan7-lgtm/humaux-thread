-- §8.5.1 (Affect annotation axis — write-side carrier) / §6.2.2 / §46 forward-fix / ADR-0030 (Card E1).
--
-- 0156 landed private.memory_affects (0..N immutable affect rows per memory). remember.put has no
-- memory_id to annotate: the memory is born asynchronously in the Distill hop (ADR-0016), exactly
-- the situation 0154 solved for subjects with private.evidence_subjects. This migration lands the
-- same shape for affects:
--
--   private.evidence_affects — the affects declared at remember.put, written by the gateway in the
--                              SAME transaction as the Evidence (adapters::remember::remember_in_txn,
--                              through affect_repo's ONE row issuer). Same columns / CHECKs as
--                              memory_affects minus memory_id; immutable like it.
--   memory_affects_inherit_from_evidence — AFTER INSERT trigger on private.memory_evidence, PRIMARY
--                              rows only: every memory born from that Evidence (Distill hop,
--                              memory.confirm, memory.correct — all through distill_repo::insert_memory,
--                              the workspace's single memory INSERT) receives a copy of the Evidence's
--                              affect rows in the memory's own transaction. SECURITY INVOKER: runs under
--                              the inserting role (private_worker for the hop, gateway for confirm /
--                              correct), which already holds SELECT on evidence_affects and INSERT on
--                              memory_affects (§6.2.2). The copy is rule 3a of §6.1.3 applied to the
--                              affect axis: provenance (evidence_id), target subject / scope, observed_at
--                              and the write-time half-life travel verbatim; nothing is re-derived.
--
-- Nothing is ever written to memory_affects in a second transaction after the fact — the card-8 P0
-- race with the Distill hop cannot occur because the hop itself performs the copy.
--
-- §46 forward-fix (EXPAND_CONTRACT, next free number after 0156). No DML; no existing object is
-- altered (memory_records policies untouched — the 0155 rls_check hash pin is unchanged).

CREATE TABLE private.evidence_affects (
  tenant_id          uuid NOT NULL REFERENCES control.tenants(tenant_id) ON DELETE CASCADE,
  affect_id          uuid NOT NULL DEFAULT uuidv7(),
  evidence_id        uuid NOT NULL,
  affect_kind        text NOT NULL CHECK (affect_kind IN ('EMOTION', 'MOOD')),
  label              text
    CHECK (label IS NULL OR label IN (
      'JOY', 'SADNESS', 'ANGER', 'FEAR', 'DISGUST', 'SURPRISE',
      'AFFECTION', 'ANXIETY', 'FRUSTRATION', 'CALM', 'EXCITEMENT', 'RELIEF')),
  valence_bp         smallint CHECK (valence_bp   IS NULL OR valence_bp   BETWEEN -10000 AND 10000),
  arousal_bp         smallint CHECK (arousal_bp   IS NULL OR arousal_bp   BETWEEN -10000 AND 10000),
  dominance_bp       smallint CHECK (dominance_bp IS NULL OR dominance_bp BETWEEN -10000 AND 10000),
  intensity_bp       smallint NOT NULL CHECK (intensity_bp  BETWEEN 0 AND 10000),
  confidence_bp      smallint NOT NULL CHECK (confidence_bp BETWEEN 0 AND 10000),
  target_subject_id  uuid,
  target_scope_kind  text
    CHECK (target_scope_kind IS NULL OR target_scope_kind IN ('WORKSPACE', 'REPOSITORY', 'TASK', 'RUN', 'AGENT')),
  target_scope_id    uuid,
  observed_at        timestamptz NOT NULL,
  half_life_seconds  integer CHECK (half_life_seconds IS NULL OR half_life_seconds > 0),
  created_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, affect_id),
  CONSTRAINT evidence_affects_scope_ref_check
    CHECK ((target_scope_kind IS NULL) = (target_scope_id IS NULL)),
  CONSTRAINT evidence_affects_mood_half_life_check
    CHECK ((affect_kind = 'MOOD') = (half_life_seconds IS NOT NULL)),
  CONSTRAINT evidence_affects_evidence_id_fkey
    FOREIGN KEY (tenant_id, evidence_id) REFERENCES private.evidence_objects (tenant_id, evidence_id)
    ON DELETE CASCADE,
  CONSTRAINT evidence_affects_target_subject_id_fkey
    FOREIGN KEY (tenant_id, target_subject_id) REFERENCES private.subjects (tenant_id, subject_id)
    ON DELETE CASCADE
);

CREATE INDEX evidence_affects_evidence_idx ON private.evidence_affects (tenant_id, evidence_id);

-- Immutable like memory_affects: the 0156 owner trigger function is reused verbatim (same
-- message, same 23514). DELETE is denied by grants alone so the FK cascades still run.
CREATE TRIGGER evidence_affects_immutable
  BEFORE UPDATE ON private.evidence_affects
  FOR EACH ROW EXECUTE FUNCTION private.reject_memory_affect_update();

-- ============================================================================
-- The copy hook: PRIMARY memory_evidence link → the Evidence's affects become the memory's.
-- SECURITY INVOKER (0154 link_memory_subjects shape): the inserting role's own grants + RLS.
-- ============================================================================
CREATE FUNCTION private.memory_affects_inherit_from_evidence() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO private.memory_affects
    (tenant_id, memory_id, affect_kind, label, valence_bp, arousal_bp, dominance_bp,
     intensity_bp, confidence_bp, evidence_id, target_subject_id, target_scope_kind,
     target_scope_id, observed_at, half_life_seconds)
  SELECT ea.tenant_id, NEW.memory_id, ea.affect_kind, ea.label, ea.valence_bp, ea.arousal_bp,
         ea.dominance_bp, ea.intensity_bp, ea.confidence_bp, ea.evidence_id, ea.target_subject_id,
         ea.target_scope_kind, ea.target_scope_id, ea.observed_at, ea.half_life_seconds
    FROM private.evidence_affects ea
    JOIN private.memory_records mr
      ON mr.memory_id = NEW.memory_id AND mr.tenant_id = ea.tenant_id
   WHERE ea.evidence_id = NEW.evidence_id
   ORDER BY ea.created_at, ea.affect_id;
  RETURN NULL;
END;
$$;
ALTER FUNCTION private.memory_affects_inherit_from_evidence() OWNER TO role_migration_owner;

CREATE TRIGGER memory_affects_inherit_from_evidence
  AFTER INSERT ON private.memory_evidence
  FOR EACH ROW
  WHEN (NEW.role = 'PRIMARY')
  EXECUTE FUNCTION private.memory_affects_inherit_from_evidence();

-- ============================================================================
-- RLS: §62 tenant clause (spelled literally for the §48.2 enumeration) AND the parent Evidence's
-- visibility, delegated to evidence_objects' own policies through EXISTS (0154 evidence_subjects).
-- ============================================================================
ALTER TABLE private.evidence_affects ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.evidence_affects FORCE ROW LEVEL SECURITY;
CREATE POLICY evidence_affects_tenant_and_parent ON private.evidence_affects
  USING (tenant_id = current_setting('humaux.tenant_id', true)::uuid
         AND EXISTS (SELECT 1 FROM private.evidence_objects e
                     WHERE e.evidence_id = evidence_affects.evidence_id
                       AND e.tenant_id = evidence_affects.tenant_id))
  WITH CHECK (tenant_id = current_setting('humaux.tenant_id', true)::uuid
         AND EXISTS (SELECT 1 FROM private.evidence_objects e
                     WHERE e.evidence_id = evidence_affects.evidence_id
                       AND e.tenant_id = evidence_affects.tenant_id));

-- ============================================================================
-- Owner + NAMED §6.2.2 grants (0154 evidence_subjects recipe). gateway declares at remember.put
-- (SELECT, INSERT); private_worker reads (the copy trigger under the Distill hop); maintenance
-- reads. Nobody holds UPDATE or DELETE.
-- ============================================================================
ALTER TABLE private.evidence_affects OWNER TO role_migration_owner;

REVOKE ALL ON private.evidence_affects
  FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
       role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;

GRANT SELECT, INSERT ON private.evidence_affects TO role_gateway;
GRANT SELECT         ON private.evidence_affects TO role_private_worker, role_maintenance;

COMMENT ON TABLE private.evidence_affects IS
  '§8.5.1 affect annotation axis, write-side carrier (ADR-0030, card E1): the affects declared at '
  'remember.put, written in the Evidence transaction; copied onto every memory born from that '
  'Evidence by the memory_evidence PRIMARY trigger. Immutable; same closed sets as memory_affects.';
COMMENT ON TRIGGER memory_affects_inherit_from_evidence ON private.memory_evidence IS
  'ADR-0030 D-C: a memory inherits its PRIMARY Evidence''s declared affects in its own transaction '
  '(rule 3a of §6.1.3 applied to the affect axis). SECURITY INVOKER.';
