-- Project Continuity W2: one Gateway-only raw storage reader. Status assembly,
-- source lifecycle decisions, Handoff and result hashing remain in Rust in one RR.

CREATE FUNCTION private.read_continuity_project_storage_v1(
  p_tenant_id uuid,
  p_project_id uuid,
  p_requested_workspace_id uuid,
  p_principal_id uuid,
  p_user_id uuid,
  p_authorized_workspace_ids uuid[]
) RETURNS TABLE(
  project_id uuid,
  tenant_id uuid,
  workspace_id uuid,
  storage_snapshot_seq bigint,
  storage_snapshot_token text,
  facet_kind text,
  slot_version bigint,
  slot_state text,
  current_version_id uuid,
  facet_version bigint,
  body_text text,
	  stored_body_sha256 bytea,
	  body_hash_closed boolean,
	  selected_is_latest boolean,
  memory_links_exact boolean,
  memory_source_ids uuid[],
  memory_source_hashes bytea[],
  evidence_links_exact boolean,
  evidence_source_ids uuid[],
  evidence_source_hashes bytea[]
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO pg_catalog
AS $$
DECLARE
  nil_uuid constant uuid := '00000000-0000-0000-0000-000000000000'::uuid;
  tenant_setting text := current_setting('humaux.tenant_id',true);
  workspace_setting text := current_setting('humaux.workspace_id',true);
  principal_setting text := current_setting('humaux.principal_id',true);
  user_setting text := current_setting('humaux.user_id',true);
  workspace_count integer := cardinality(p_authorized_workspace_ids);
BEGIN
  IF p_tenant_id IS NULL OR p_tenant_id=nil_uuid
    OR p_project_id IS NULL OR p_project_id=nil_uuid
    OR uuid_extract_version(p_project_id)<>7
    OR p_principal_id IS NULL OR p_principal_id=nil_uuid
    OR p_user_id=nil_uuid
    OR p_authorized_workspace_ids IS NULL
    OR workspace_count>256
    OR (workspace_count>0 AND (array_ndims(p_authorized_workspace_ids)<>1
      OR array_lower(p_authorized_workspace_ids,1)<>1))
    OR (CASE WHEN pg_catalog.pg_input_is_valid(tenant_setting,'uuid')
      THEN tenant_setting::uuid IS DISTINCT FROM p_tenant_id ELSE true END)
    OR (CASE WHEN pg_catalog.pg_input_is_valid(workspace_setting,'uuid')
      THEN workspace_setting::uuid IS DISTINCT FROM nil_uuid ELSE true END)
    OR (CASE WHEN pg_catalog.pg_input_is_valid(principal_setting,'uuid')
      THEN principal_setting::uuid IS DISTINCT FROM p_principal_id ELSE true END)
    OR (CASE WHEN pg_catalog.pg_input_is_valid(user_setting,'uuid')
      THEN user_setting::uuid IS DISTINCT FROM coalesce(p_user_id,nil_uuid) ELSE true END) THEN
    RAISE EXCEPTION 'invalid continuity read authority context' USING ERRCODE='42501';
  END IF;

  IF EXISTS (SELECT 1 FROM pg_catalog.unnest(p_authorized_workspace_ids) item(id)
      WHERE item.id IS NULL OR item.id=nil_uuid)
    OR (SELECT pg_catalog.count(*)<>pg_catalog.count(DISTINCT item.id)
        FROM pg_catalog.unnest(p_authorized_workspace_ids) item(id))
    OR p_authorized_workspace_ids IS DISTINCT FROM
      ARRAY(SELECT item.id FROM pg_catalog.unnest(p_authorized_workspace_ids) item(id)
        ORDER BY item.id) THEN
    RAISE EXCEPTION 'invalid continuity read authority set' USING ERRCODE='42501';
  END IF;

  RETURN QUERY
  SELECT project.project_id,project.tenant_id,project.workspace_id,
    pg_snapshot_xmin(pg_current_snapshot())::text::bigint,pg_current_snapshot()::text,
    slot.facet_kind,slot.slot_version,slot.slot_state,slot.current_version_id,
    version.facet_version,version.body::text,version.body_sha256,
    CASE WHEN version.facet_version_id IS NULL THEN NULL
      ELSE version.body_sha256=sha256(convert_to(version.body::text,'UTF8')) END,
    CASE WHEN version.facet_version_id IS NULL THEN NOT EXISTS (
      SELECT 1 FROM private.continuity_facet_versions orphan
      WHERE orphan.tenant_id=slot.tenant_id
        AND orphan.workspace_id=slot.workspace_id
        AND orphan.project_id=slot.project_id
        AND orphan.facet_kind=slot.facet_kind)
    ELSE version.facet_version=(
      SELECT pg_catalog.max(candidate.facet_version)
      FROM private.continuity_facet_versions candidate
      WHERE candidate.tenant_id=version.tenant_id
        AND candidate.workspace_id=version.workspace_id
        AND candidate.project_id=version.project_id
        AND candidate.facet_kind=version.facet_kind) END,
    CASE WHEN version.facet_version_id IS NULL THEN true ELSE NOT EXISTS (
      SELECT 1 FROM private.continuity_facet_memory_links damaged
      WHERE damaged.facet_version_id=version.facet_version_id
        AND damaged.tenant_id=version.tenant_id
        AND (damaged.tenant_id,damaged.workspace_id,damaged.project_id,
             damaged.facet_kind,damaged.facet_version,damaged.facet_version_id)
          IS DISTINCT FROM
            (version.tenant_id,version.workspace_id,version.project_id,
             version.facet_kind,version.facet_version,version.facet_version_id)) END,
    coalesce((SELECT array_agg(link.memory_id ORDER BY link.memory_id)
      FROM private.continuity_facet_memory_links link
      WHERE link.tenant_id=version.tenant_id AND link.workspace_id=version.workspace_id
        AND link.project_id=version.project_id AND link.facet_kind=version.facet_kind
        AND link.facet_version=version.facet_version
        AND link.facet_version_id=version.facet_version_id),ARRAY[]::uuid[]),
    coalesce((SELECT array_agg(link.memory_sha256 ORDER BY link.memory_id)
      FROM private.continuity_facet_memory_links link
      WHERE link.tenant_id=version.tenant_id AND link.workspace_id=version.workspace_id
        AND link.project_id=version.project_id AND link.facet_kind=version.facet_kind
        AND link.facet_version=version.facet_version
        AND link.facet_version_id=version.facet_version_id),ARRAY[]::bytea[]),
    CASE WHEN version.facet_version_id IS NULL THEN true ELSE NOT EXISTS (
      SELECT 1 FROM private.continuity_facet_evidence_links damaged
      WHERE damaged.facet_version_id=version.facet_version_id
        AND damaged.tenant_id=version.tenant_id
        AND (damaged.tenant_id,damaged.workspace_id,damaged.project_id,
             damaged.facet_kind,damaged.facet_version,damaged.facet_version_id)
          IS DISTINCT FROM
            (version.tenant_id,version.workspace_id,version.project_id,
             version.facet_kind,version.facet_version,version.facet_version_id)) END,
    coalesce((SELECT array_agg(link.evidence_id ORDER BY link.evidence_id)
      FROM private.continuity_facet_evidence_links link
      WHERE link.tenant_id=version.tenant_id AND link.workspace_id=version.workspace_id
        AND link.project_id=version.project_id AND link.facet_kind=version.facet_kind
        AND link.facet_version=version.facet_version
        AND link.facet_version_id=version.facet_version_id),ARRAY[]::uuid[]),
    coalesce((SELECT array_agg(link.evidence_sha256 ORDER BY link.evidence_id)
      FROM private.continuity_facet_evidence_links link
      WHERE link.tenant_id=version.tenant_id AND link.workspace_id=version.workspace_id
        AND link.project_id=version.project_id AND link.facet_kind=version.facet_kind
        AND link.facet_version=version.facet_version
        AND link.facet_version_id=version.facet_version_id),ARRAY[]::bytea[])
  FROM private.continuity_projects project
  LEFT JOIN private.continuity_facet_slots slot
    ON slot.tenant_id=project.tenant_id AND slot.workspace_id=project.workspace_id
   AND slot.project_id=project.project_id
  LEFT JOIN private.continuity_facet_versions version
    ON version.tenant_id=slot.tenant_id AND version.workspace_id=slot.workspace_id
   AND version.project_id=slot.project_id AND version.facet_kind=slot.facet_kind
   AND version.facet_version=slot.slot_version
   AND version.facet_version_id=slot.current_version_id
  WHERE project.tenant_id=p_tenant_id AND project.project_id=p_project_id
    AND project.lifecycle_state='ACTIVE'
    AND project.workspace_id=ANY(p_authorized_workspace_ids)
    AND (p_requested_workspace_id IS NULL
      OR project.workspace_id=p_requested_workspace_id)
  ORDER BY slot.facet_kind NULLS FIRST;
END;
$$;

ALTER FUNCTION private.read_continuity_project_storage_v1(
  uuid,uuid,uuid,uuid,uuid,uuid[]) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION private.read_continuity_project_storage_v1(
  uuid,uuid,uuid,uuid,uuid,uuid[]) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION private.read_continuity_project_storage_v1(
  uuid,uuid,uuid,uuid,uuid,uuid[]) TO role_gateway;

COMMENT ON FUNCTION private.read_continuity_project_storage_v1(
  uuid,uuid,uuid,uuid,uuid,uuid[]) IS
  '§25.3.1 W2 raw authorized storage read; caller owns one RR READ ONLY transaction and same-snapshot source/Handoff validation.';
