-- §11.2.1 / §12.1.1: the candidate is bound to the actual domain owner/profile,
-- headless grant and every backing Evidence. Authorization is checked again at release.
-- These are local forward fixes to 0107; no receipt or authorization is backfilled.
CREATE TRIGGER contribution_input_change BEFORE INSERT OR UPDATE OR DELETE
  ON control.reasoning_domain_grants FOR EACH ROW
  EXECUTE FUNCTION ops.guard_contribution_input_change();
CREATE TRIGGER contribution_input_change BEFORE INSERT OR UPDATE OR DELETE
  ON private.memory_evidence FOR EACH ROW
  EXECUTE FUNCTION ops.guard_contribution_input_change();

CREATE FUNCTION staging.guard_contribution_authorization()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE c staging.contribution_candidates; principal uuid; workspaces uuid[];
BEGIN
  IF NEW.candidate_id IS NULL THEN RETURN NEW; END IF;
  PERFORM ops.lock_contribution_inputs();
  SELECT * INTO c FROM staging.contribution_candidates WHERE candidate_id=NEW.candidate_id;
  IF c.candidate_id IS NULL OR NOT (c.policy_snapshot ? 'principal_id') OR
     jsonb_typeof(c.policy_snapshot->'allowed_workspace_ids') IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION 'candidate lacks authenticated principal scope' USING ERRCODE='42501';
  END IF;
  principal:=(c.policy_snapshot->>'principal_id')::uuid;
  SELECT coalesce(array_agg(value::uuid),'{}'::uuid[]) INTO workspaces
    FROM jsonb_array_elements_text(c.policy_snapshot->'allowed_workspace_ids');
  IF principal IS NULL OR NOT EXISTS(
    SELECT 1 FROM control.private_reasoning_domains d
      JOIN control.user_reasoning_profiles p ON p.profile_id=d.user_reasoning_profile_id
    WHERE d.reasoning_domain_id=c.reasoning_domain_id AND d.tenant_id=c.tenant_id
      AND d.owner_user_id=c.user_id AND d.status='ACTIVE'
      AND p.tenant_id=d.tenant_id AND p.user_id=d.owner_user_id
      AND p.enabled AND p.profile_version=c.profile_version
      AND p.capabilities @> ARRAY['TEXT']::text[])
    OR (principal<>c.user_id AND NOT EXISTS(
      SELECT 1 FROM control.reasoning_domain_grants g
      WHERE g.reasoning_domain_id=c.reasoning_domain_id AND g.principal_id=principal
        AND g.revoked_at IS NULL AND g.expires_at>clock_timestamp()
        AND g.purposes @> ARRAY['USER_REASONING']::text[]
        AND (g.workspace_id IS NULL OR g.workspace_id=ANY(workspaces)))) THEN
    RAISE EXCEPTION 'domain, bound profile or USER_REASONING grant is stale' USING ERRCODE='42501';
  END IF;
  IF EXISTS(SELECT 1 FROM staging.contribution_candidate_sources s
    WHERE s.candidate_id=c.candidate_id AND s.memory_id IS NOT NULL
      AND NOT EXISTS(SELECT 1 FROM private.memory_evidence me WHERE me.memory_id=s.memory_id)) THEN
    RAISE EXCEPTION 'memory lacks backing Evidence' USING ERRCODE='42501';
  END IF;
  IF EXISTS(
    WITH input_rows AS (
      SELECT m.memory_id AS id,m.tenant_id,m.visibility_class,m.visibility_user_id,
        m.visibility_workspace_id,c.reasoning_domain_id AS reasoning_domain_id
      FROM staging.contribution_candidate_sources s
      LEFT JOIN private.memory_records m ON m.memory_id=s.memory_id
      WHERE s.candidate_id=c.candidate_id AND s.memory_id IS NOT NULL
      UNION ALL
      SELECT e.evidence_id,e.tenant_id,e.visibility_class,e.visibility_user_id,
        e.visibility_workspace_id,e.reasoning_domain_id
      FROM staging.contribution_candidate_sources s
      LEFT JOIN private.evidence_objects e ON e.evidence_id=s.evidence_id
      WHERE s.candidate_id=c.candidate_id AND s.evidence_id IS NOT NULL
      UNION ALL
      SELECT e.evidence_id,e.tenant_id,e.visibility_class,e.visibility_user_id,
        e.visibility_workspace_id,e.reasoning_domain_id
      FROM staging.contribution_candidate_sources s
      JOIN private.memory_evidence me ON me.memory_id=s.memory_id
      LEFT JOIN private.evidence_objects e ON e.evidence_id=me.evidence_id
      WHERE s.candidate_id=c.candidate_id
    ) SELECT 1 FROM input_rows i WHERE i.id IS NULL OR i.tenant_id IS DISTINCT FROM c.tenant_id
      OR i.reasoning_domain_id IS DISTINCT FROM c.reasoning_domain_id
      OR NOT coalesce(CASE i.visibility_class
          WHEN 'TENANT_SHARED' THEN true
          WHEN 'USER_PRIVATE' THEN i.visibility_user_id=c.user_id
          WHEN 'WORKSPACE_SHARED' THEN i.visibility_workspace_id=ANY(workspaces)
          ELSE false END,false)
      OR (principal<>c.user_id AND NOT EXISTS(
        SELECT 1 FROM control.reasoning_domain_grants g
        WHERE g.reasoning_domain_id=c.reasoning_domain_id AND g.principal_id=principal
          AND g.revoked_at IS NULL AND g.expires_at>clock_timestamp()
          AND g.purposes @> ARRAY['USER_REASONING']::text[]
          AND (g.workspace_id IS NULL OR (g.workspace_id=i.visibility_workspace_id
            AND g.workspace_id=ANY(workspaces)))))
  ) THEN
    RAISE EXCEPTION 'candidate source or backing Evidence is no longer authorized' USING ERRCODE='42501';
  END IF;
  RETURN NEW;
END;
$$;
ALTER FUNCTION staging.guard_contribution_authorization() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION staging.guard_contribution_authorization() FROM PUBLIC;
CREATE TRIGGER finalized_authorization_guard BEFORE INSERT ON staging.contribution_releases
  FOR EACH ROW EXECUTE FUNCTION staging.guard_contribution_authorization();
