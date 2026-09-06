-- §8.5.1 (Affect annotation axis) / §6.2.2 / §46 forward-fix / ADR-0030 (Card E1).
--
-- A Memory may carry 0..N affect annotations: "what emotional state was observed when this
-- memory was formed". This is an ORTHOGONAL measurement axis next to MemoryType (what the memory
-- is), the Subject axis (who it is about, 0153/0154) and Authority/Evidence (why believe it).
-- MemoryType is NOT extended — no `EMOTION` memory type, no EmotionFact/EmotionDecision pollution.
--
-- Representation (research ruling, ADR-0030 D-A/D-B):
--   * VAD primary: valence / arousal / dominance in normalised basis points -10000..+10000
--     (smallint; NEVER float — no NaN, no rounding, no equality edge cases).
--   * intensity_bp / confidence_bp 0..10000, stored separately (high-intensity sadness need not be
--     high-arousal, so |arousal| is not intensity).
--   * label: optional closed EmotionLabel (12) — UI / explicit filter / human explanation only.
--     It never decides Authority and is not a diagnosis.
--   * affect_kind EMOTION | MOOD: an EMOTION is an event-bound historical observation — its raw
--     intensity never decays; a MOOD is a diffuse state whose EFFECTIVE intensity is derived at
--     read time as raw * 2^(-Δt / half_life). half_life_seconds is a frozen product/calibration
--     policy (HUMAUX_GATEWAY_MOOD_HALF_LIFE_SECONDS, §78.1) applied at WRITE time and stored per
--     row, so a later policy change never rewrites history. Decay is NOT a lifecycle mutation.
--   * evidence_id: provenance of the observation (the memory's PRIMARY Evidence).
--   * target_subject_id: optional "about whom" for the affect (tenant-leg FK → private.subjects,
--     ON DELETE CASCADE so a subject ERASE (§37) is terminal for the affect too).
--   * target_scope_kind/id: optional typed ScopeRef (closed kinds = the §59 Scope layers).
--
-- Rows are IMMUTABLE: no runtime role holds UPDATE or DELETE; an owner trigger rejects UPDATE
-- (0148 confirm_tokens shape). "I was not actually angry" is memory.correct — a new version whose
-- affects are re-supplied; the old rows ride with the superseded version. ON DELETE CASCADE from
-- memory_records keeps the §37 purge terminal (the cascade is a DELETE by the owner, not by a
-- runtime role; the trigger guards UPDATE only so cascades still pass).
--
-- RLS: §62 tenant clause AND the parent memory's own visibility via EXISTS(memory_records)
-- (memory_subjects / memory_evidence shape, 0154).
--
-- §46 forward-fix (EXPAND_CONTRACT, next free number after 0155). No DML; no existing object
-- is altered (memory_records policies untouched — the 0155 rls_check hash pin is unchanged).

CREATE TABLE private.memory_affects (
  tenant_id          uuid NOT NULL REFERENCES control.tenants(tenant_id) ON DELETE CASCADE,
  affect_id          uuid NOT NULL DEFAULT uuidv7(),
  memory_id          uuid NOT NULL,
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
  evidence_id        uuid NOT NULL,
  target_subject_id  uuid,
  target_scope_kind  text
    CHECK (target_scope_kind IS NULL OR target_scope_kind IN ('WORKSPACE', 'REPOSITORY', 'TASK', 'RUN', 'AGENT')),
  target_scope_id    uuid,
  observed_at        timestamptz NOT NULL,
  half_life_seconds  integer CHECK (half_life_seconds IS NULL OR half_life_seconds > 0),
  created_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, affect_id),
  CONSTRAINT memory_affects_scope_ref_check
    CHECK ((target_scope_kind IS NULL) = (target_scope_id IS NULL)),
  -- MOOD ⇔ half-life present; EMOTION never decays (no half-life to decay by).
  CONSTRAINT memory_affects_mood_half_life_check
    CHECK ((affect_kind = 'MOOD') = (half_life_seconds IS NOT NULL)),
  CONSTRAINT memory_affects_memory_id_fkey
    FOREIGN KEY (tenant_id, memory_id) REFERENCES private.memory_records (tenant_id, memory_id)
    ON DELETE CASCADE,
  CONSTRAINT memory_affects_evidence_id_fkey
    FOREIGN KEY (tenant_id, evidence_id) REFERENCES private.evidence_objects (tenant_id, evidence_id)
    ON DELETE CASCADE,
  CONSTRAINT memory_affects_target_subject_id_fkey
    FOREIGN KEY (tenant_id, target_subject_id) REFERENCES private.subjects (tenant_id, subject_id)
    ON DELETE CASCADE
);

CREATE INDEX memory_affects_memory_idx ON private.memory_affects (tenant_id, memory_id);
CREATE INDEX memory_affects_target_subject_idx ON private.memory_affects (tenant_id, target_subject_id)
  WHERE target_subject_id IS NOT NULL;

-- ============================================================================
-- Immutability: the only legal write is INSERT. Owner trigger, ordinary invoker; runtime roles
-- have no DDL (§6.2.1) so it cannot be dropped by them. DELETE is denied by grants alone so the
-- FK cascades (memory purge / subject erase) still run.
-- ============================================================================
CREATE FUNCTION private.reject_memory_affect_update() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog
AS $$
BEGIN
  RAISE EXCEPTION 'memory affects are immutable; correct the memory instead' USING ERRCODE = '23514';
END;
$$;
ALTER FUNCTION private.reject_memory_affect_update() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION private.reject_memory_affect_update() FROM PUBLIC;
CREATE TRIGGER memory_affects_immutable
  BEFORE UPDATE ON private.memory_affects
  FOR EACH ROW EXECUTE FUNCTION private.reject_memory_affect_update();

-- ============================================================================
-- RLS: §62 tenant clause (spelled literally for the §48.2 enumeration) AND the parent memory's
-- visibility, delegated to memory_records' own policies through EXISTS (0154 shape).
-- ============================================================================
ALTER TABLE private.memory_affects ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.memory_affects FORCE ROW LEVEL SECURITY;
CREATE POLICY memory_affects_tenant_and_parent ON private.memory_affects
  USING (tenant_id = current_setting('humaux.tenant_id', true)::uuid
         AND EXISTS (SELECT 1 FROM private.memory_records mr
                     WHERE mr.memory_id = memory_affects.memory_id
                       AND mr.tenant_id = memory_affects.tenant_id))
  WITH CHECK (tenant_id = current_setting('humaux.tenant_id', true)::uuid
         AND EXISTS (SELECT 1 FROM private.memory_records mr
                     WHERE mr.memory_id = memory_affects.memory_id
                       AND mr.tenant_id = memory_affects.tenant_id));

-- ============================================================================
-- Owner + NAMED §6.2.2 grants (0154 recipe). gateway annotates (memory.annotate_affect /
-- memory.correct affects) and reads; private_worker may emit affects for Distill-born memories
-- (SELECT, INSERT); retrieval_worker reads them into the projection payload; maintenance reads.
-- Nobody holds UPDATE or DELETE.
-- ============================================================================
ALTER TABLE private.memory_affects OWNER TO role_migration_owner;

REVOKE ALL ON private.memory_affects
  FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
       role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;

GRANT SELECT, INSERT ON private.memory_affects TO role_gateway, role_private_worker;
GRANT SELECT         ON private.memory_affects TO role_retrieval_worker, role_maintenance;

COMMENT ON TABLE private.memory_affects IS
  '§8.5.1 affect annotation axis (ADR-0030, card E1): 0..N immutable EMOTION|MOOD observations per '
  'memory — VAD basis points (-10000..10000), intensity/confidence (0..10000), optional closed '
  'EmotionLabel, PRIMARY-Evidence provenance, optional target subject / typed ScopeRef, observed_at, '
  'MOOD half_life_seconds (write-time policy). EMOTION intensity never decays; MOOD effective '
  'intensity is derived at read time. Correction = new memory version, never UPDATE.';
COMMENT ON TRIGGER memory_affects_immutable ON private.memory_affects IS
  'ADR-0030 D-E: affect rows never mutate; a correction is memory.correct (new version).';
