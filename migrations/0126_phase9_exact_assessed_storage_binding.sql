-- Phase 9 additive storage hardening.  An assessed candidate may finalize only through one
-- exact protected release -> random lineage -> sealed envelope -> anonymous outbox chain.
-- Legacy candidates without an assessment retain their release-addressed expand path.
-- This migration proves relational consistency, not model-call authenticity: a trusted role can
-- still assemble a field-identical chain until R3 binds provider_trace to ModelCallLedger.

-- The assessment row already carries every assessed input.  Make its candidate side a real
-- relational binding as well as the 0121 insertion guard: candidate bytes are sealed by the
-- candidate hash CHECK, while manifest and the assessment output provider trace are exact.
ALTER TABLE staging.contribution_candidates
  ADD CONSTRAINT contribution_candidates_phase9_assessment_binding_unique
    UNIQUE(tenant_id,candidate_id,disclosed_payload_sha256,source_manifest_hash,provider_trace);
ALTER TABLE staging.contribution_candidate_phase9_assessments
  ADD CONSTRAINT phase9_assessment_exact_candidate_fk
    FOREIGN KEY(tenant_id,candidate_id,candidate_payload_sha256,
      probe_source_manifest_hash,assessment_provider_trace)
    REFERENCES staging.contribution_candidates(
      tenant_id,candidate_id,disclosed_payload_sha256,source_manifest_hash,provider_trace)
    DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE staging.sanitized_public_candidates
  ADD CONSTRAINT sanitized_public_candidates_phase9_source_unique
    UNIQUE(anonymous_source_id);

-- Revocation runs without an end-user GUC.  This owner-only branch lets the deferred definer
-- inspect the candidate without granting any runtime role a new table capability.
CREATE POLICY contribution_candidates_exact_binding_owner_read
  ON staging.contribution_candidates FOR SELECT TO role_migration_owner USING(true);

CREATE FUNCTION staging.assert_phase9_exact_assessed_release(p_release_id uuid)
RETURNS void
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE
  r staging.contribution_releases;
  c staging.contribution_candidates;
  a staging.contribution_candidate_phase9_assessments;
  lineage_count bigint;
  sealed_count bigint;
  anonymous_release_count bigint;
  anonymous_revoke_count bigint;
  legacy_release_count bigint;
  legacy_revoke_count bigint;
BEGIN
  IF p_release_id IS NULL THEN RETURN; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'phase9-exact-assessed-storage:'||p_release_id::text,0));
  SELECT * INTO r FROM staging.contribution_releases
    WHERE contribution_release_id=p_release_id;
  IF NOT FOUND THEN RETURN; END IF;

  SELECT count(*) INTO lineage_count FROM control.anonymous_source_lineage l
    WHERE l.contribution_release_id=r.contribution_release_id AND l.tenant_id=r.tenant_id;
  SELECT count(*) INTO legacy_release_count FROM ops.outbox o
    WHERE o.tenant_id=r.tenant_id AND o.contribution_release_id=r.contribution_release_id
      AND o.event_type='PUBLIC_RELEASE';
  SELECT count(*) INTO legacy_revoke_count FROM ops.outbox o
    WHERE o.tenant_id=r.tenant_id AND o.contribution_release_id=r.contribution_release_id
      AND o.event_type='PUBLIC_REVOKE';

  IF r.candidate_id IS NULL THEN
    IF lineage_count<>0 OR legacy_release_count<>1
      OR (r.state='ACTIVE' AND legacy_revoke_count<>0)
      OR (r.state='REVOKED' AND legacy_revoke_count<>1) THEN
      RAISE EXCEPTION 'legacy release must retain only its exact release-addressed storage chain'
        USING ERRCODE='23514';
    END IF;
    RETURN;
  END IF;

  SELECT * INTO c FROM staging.contribution_candidates
    WHERE tenant_id=r.tenant_id AND candidate_id=r.candidate_id;
  SELECT * INTO a FROM staging.contribution_candidate_phase9_assessments
    WHERE tenant_id=r.tenant_id AND candidate_id=r.candidate_id;

  -- A finalized candidate with no assessment remains the approved legacy expand path.
  IF a.candidate_id IS NULL THEN
    IF c.candidate_id IS NULL OR lineage_count<>0 OR legacy_release_count<>1
      OR (r.state='ACTIVE' AND legacy_revoke_count<>0)
      OR (r.state='REVOKED' AND legacy_revoke_count<>1) THEN
      RAISE EXCEPTION 'unassessed candidate release must retain only its exact legacy storage chain'
        USING ERRCODE='23514';
    END IF;
    RETURN;
  END IF;

  IF c.candidate_id IS NULL OR
    ROW(r.disclosed_payload,r.disclosed_payload_sha256,r.scan_receipt,r.policy_snapshot,
      r.rights_basis,r.source_license,r.publisher,r.contributor_attestation,
      r.redistribution_policy)
    IS DISTINCT FROM
    ROW(c.disclosed_payload,c.disclosed_payload_sha256,c.scan_receipt,c.policy_snapshot,
      c.rights_basis,c.source_license,c.publisher,c.contributor_attestation,
      c.redistribution_policy) THEN
    RAISE EXCEPTION 'assessed release must preserve its exact candidate bytes and snapshots'
      USING ERRCODE='23514';
  END IF;

  SELECT count(*) INTO sealed_count
  FROM control.anonymous_source_lineage l
  JOIN staging.sanitized_public_candidates s
    ON s.tenant_id=l.tenant_id AND s.anonymous_source_id=l.anonymous_source_id
  WHERE l.contribution_release_id=r.contribution_release_id AND l.tenant_id=r.tenant_id
    AND s.sanitized_content=jsonb_build_object('text',convert_from(c.disclosed_payload,'UTF8'))
    AND s.content_sha256=sha256(convert_to(s.sanitized_content::text,'UTF8'))
    AND s.policy_version=c.policy_version::text
    AND s.policy_digest=sha256(convert_to(c.policy_snapshot::text,'UTF8'))
    AND s.assessment_outcome='PASSED' AND s.assessment_digest=a.assessment_digest;

  SELECT count(*) FILTER(WHERE o.event_type='PUBLIC_ANONYMOUS_RELEASE'
      AND o.anonymous_source_revision=1),
    count(*) FILTER(WHERE o.event_type='PUBLIC_ANONYMOUS_REVOKE'
      AND o.anonymous_source_revision=2)
    INTO anonymous_release_count,anonymous_revoke_count
  FROM control.anonymous_source_lineage l
  JOIN staging.sanitized_public_candidates s
    ON s.tenant_id=l.tenant_id AND s.anonymous_source_id=l.anonymous_source_id
  JOIN ops.outbox o ON o.tenant_id=l.tenant_id
    AND o.anonymous_source_id=l.anonymous_source_id
    AND o.candidate_envelope_sha256=s.envelope_sha256
  WHERE l.contribution_release_id=r.contribution_release_id AND l.tenant_id=r.tenant_id;

  IF lineage_count<>1 OR sealed_count<>1 OR anonymous_release_count<>1
    OR legacy_release_count<>0 OR legacy_revoke_count<>0
    OR (r.state='ACTIVE' AND anonymous_revoke_count<>0)
    OR (r.state='REVOKED' AND anonymous_revoke_count<>1) THEN
    RAISE EXCEPTION 'assessed release requires one exact candidate, lineage, envelope and anonymous outbox chain'
      USING ERRCODE='23514';
  END IF;
END;
$$;

CREATE FUNCTION staging.require_phase9_exact_assessed_storage_binding()
RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE
  old_release_id uuid;
  new_release_id uuid;
BEGIN
  IF TG_TABLE_SCHEMA='staging' AND TG_TABLE_NAME='contribution_candidates' THEN
    IF TG_OP<>'INSERT' THEN
      SELECT contribution_release_id INTO old_release_id FROM staging.contribution_releases
        WHERE candidate_id=OLD.candidate_id;
    END IF;
    IF TG_OP<>'DELETE' THEN
      SELECT contribution_release_id INTO new_release_id FROM staging.contribution_releases
        WHERE candidate_id=NEW.candidate_id;
    END IF;
  ELSIF TG_TABLE_SCHEMA='staging' AND TG_TABLE_NAME='contribution_releases' THEN
    IF TG_OP<>'INSERT' THEN old_release_id:=OLD.contribution_release_id; END IF;
    IF TG_OP<>'DELETE' THEN new_release_id:=NEW.contribution_release_id; END IF;
  ELSIF TG_TABLE_SCHEMA='staging'
      AND TG_TABLE_NAME='contribution_candidate_phase9_assessments' THEN
    IF TG_OP<>'INSERT' THEN
      SELECT contribution_release_id INTO old_release_id FROM staging.contribution_releases
        WHERE candidate_id=OLD.candidate_id;
    END IF;
    IF TG_OP<>'DELETE' THEN
      SELECT contribution_release_id INTO new_release_id FROM staging.contribution_releases
        WHERE candidate_id=NEW.candidate_id;
    END IF;
  ELSIF TG_TABLE_SCHEMA='control' AND TG_TABLE_NAME='anonymous_source_lineage' THEN
    IF TG_OP<>'INSERT' THEN old_release_id:=OLD.contribution_release_id; END IF;
    IF TG_OP<>'DELETE' THEN new_release_id:=NEW.contribution_release_id; END IF;
  ELSIF TG_TABLE_SCHEMA='staging' AND TG_TABLE_NAME='sanitized_public_candidates' THEN
    IF TG_OP<>'INSERT' THEN
      SELECT contribution_release_id INTO old_release_id FROM control.anonymous_source_lineage
        WHERE anonymous_source_id=OLD.anonymous_source_id;
    END IF;
    IF TG_OP<>'DELETE' THEN
      SELECT contribution_release_id INTO new_release_id FROM control.anonymous_source_lineage
        WHERE anonymous_source_id=NEW.anonymous_source_id;
    END IF;
  ELSIF TG_TABLE_SCHEMA='ops' AND TG_TABLE_NAME='outbox' THEN
    IF TG_OP<>'INSERT' AND OLD.event_type IN ('PUBLIC_RELEASE','PUBLIC_REVOKE') THEN
      old_release_id:=OLD.contribution_release_id;
    ELSIF TG_OP<>'INSERT' AND OLD.event_type IN
        ('PUBLIC_ANONYMOUS_RELEASE','PUBLIC_ANONYMOUS_REVOKE') THEN
      SELECT contribution_release_id INTO old_release_id FROM control.anonymous_source_lineage
        WHERE anonymous_source_id=OLD.anonymous_source_id;
    END IF;
    IF TG_OP<>'DELETE' AND NEW.event_type IN ('PUBLIC_RELEASE','PUBLIC_REVOKE') THEN
      new_release_id:=NEW.contribution_release_id;
    ELSIF TG_OP<>'DELETE' AND NEW.event_type IN
        ('PUBLIC_ANONYMOUS_RELEASE','PUBLIC_ANONYMOUS_REVOKE') THEN
      SELECT contribution_release_id INTO new_release_id FROM control.anonymous_source_lineage
        WHERE anonymous_source_id=NEW.anonymous_source_id;
    END IF;
  END IF;

  PERFORM staging.assert_phase9_exact_assessed_release(old_release_id);
  IF new_release_id IS DISTINCT FROM old_release_id THEN
    PERFORM staging.assert_phase9_exact_assessed_release(new_release_id);
  END IF;
  RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER phase9_exact_candidate_binding
  AFTER UPDATE OR DELETE ON staging.contribution_candidates
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
  EXECUTE FUNCTION staging.require_phase9_exact_assessed_storage_binding();
CREATE CONSTRAINT TRIGGER phase9_exact_release_binding
  AFTER INSERT OR UPDATE ON staging.contribution_releases
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
  EXECUTE FUNCTION staging.require_phase9_exact_assessed_storage_binding();
CREATE CONSTRAINT TRIGGER phase9_exact_assessment_binding
  AFTER INSERT OR UPDATE OR DELETE ON staging.contribution_candidate_phase9_assessments
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
  EXECUTE FUNCTION staging.require_phase9_exact_assessed_storage_binding();
CREATE CONSTRAINT TRIGGER phase9_exact_lineage_binding
  AFTER INSERT OR UPDATE OR DELETE ON control.anonymous_source_lineage
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
  EXECUTE FUNCTION staging.require_phase9_exact_assessed_storage_binding();
CREATE CONSTRAINT TRIGGER phase9_exact_envelope_binding
  AFTER INSERT OR UPDATE OR DELETE ON staging.sanitized_public_candidates
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
  EXECUTE FUNCTION staging.require_phase9_exact_assessed_storage_binding();
CREATE CONSTRAINT TRIGGER phase9_exact_outbox_binding
  AFTER INSERT OR UPDATE OR DELETE ON ops.outbox
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
  EXECUTE FUNCTION staging.require_phase9_exact_assessed_storage_binding();

ALTER FUNCTION staging.assert_phase9_exact_assessed_release(uuid) OWNER TO role_migration_owner;
ALTER FUNCTION staging.require_phase9_exact_assessed_storage_binding() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION staging.assert_phase9_exact_assessed_release(uuid),
  staging.require_phase9_exact_assessed_storage_binding()
  FROM PUBLIC,role_gateway,role_private_worker,role_consolidation_worker,
    role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;

-- Validate pre-existing rows during rollout; successor writes are covered by the deferred
-- triggers.  The function is owner-only and returns no protected fields.
DO $$ DECLARE release_id uuid; BEGIN
  FOR release_id IN SELECT r.contribution_release_id FROM staging.contribution_releases r LOOP
    PERFORM staging.assert_phase9_exact_assessed_release(release_id);
  END LOOP;
END $$;

COMMENT ON FUNCTION staging.assert_phase9_exact_assessed_release(uuid) IS
  'Owner-only deferred relational storage-chain gate. It returns no protected data; R3 ModelCallLedger binding separately proves model-call authenticity.';
