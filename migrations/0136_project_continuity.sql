-- Project Continuity W1: registry, immutable version/source lineage, eager slots,
-- and the two role_gateway-only atomic commands. W2 read assembly is deliberately absent.

CREATE TABLE private.continuity_projects (
  project_id uuid PRIMARY KEY CHECK (uuid_extract_version(project_id)=7),
  tenant_id uuid NOT NULL,
  workspace_id uuid NOT NULL,
  title text NOT NULL CHECK (char_length(title) BETWEEN 1 AND 256),
  lifecycle_state text NOT NULL DEFAULT 'ACTIVE'
    CHECK (lifecycle_state IN ('ACTIVE','ARCHIVED')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT continuity_projects_parent_key UNIQUE (tenant_id,workspace_id,project_id),
  CONSTRAINT continuity_projects_workspace_fkey FOREIGN KEY (tenant_id,workspace_id)
    REFERENCES control.workspaces(tenant_id,workspace_id)
);

CREATE TABLE private.continuity_facet_versions (
  tenant_id uuid NOT NULL,
  workspace_id uuid NOT NULL,
  project_id uuid NOT NULL,
  facet_kind text NOT NULL CONSTRAINT continuity_facet_versions_kind_closed CHECK (facet_kind IN (
    'GOAL','CURRENT_STATE','DECISIONS','REJECTIONS','CONSTRAINTS','KNOWN_ISSUES',
    'NEXT_ACTIONS','ACTIVE_TASKS','RECENT_CHANGES','CODE','TESTS','CONFIG',
    'MIGRATIONS','PROCEDURES','OUTCOMES')),
  facet_version_id uuid PRIMARY KEY DEFAULT uuidv7()
    CHECK (uuid_extract_version(facet_version_id)=7),
  facet_version bigint NOT NULL CHECK (facet_version > 0),
  body jsonb NOT NULL,
  body_sha256 bytea NOT NULL CHECK (octet_length(body_sha256)=32),
  authored_by_principal_id uuid NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT continuity_facet_versions_project_fkey
    FOREIGN KEY (tenant_id,workspace_id,project_id)
    REFERENCES private.continuity_projects(tenant_id,workspace_id,project_id),
  CONSTRAINT continuity_facet_versions_number_key
    UNIQUE (tenant_id,workspace_id,project_id,facet_kind,facet_version),
  CONSTRAINT continuity_facet_versions_exact_key
    UNIQUE (tenant_id,workspace_id,project_id,facet_kind,facet_version,facet_version_id),
  CONSTRAINT continuity_facet_versions_body_hash
    CHECK (body_sha256=sha256(convert_to(body::text,'UTF8')))
);

CREATE TABLE private.continuity_facet_memory_links (
  tenant_id uuid NOT NULL,
  workspace_id uuid NOT NULL,
  project_id uuid NOT NULL,
  facet_kind text NOT NULL,
  facet_version bigint NOT NULL,
  facet_version_id uuid NOT NULL,
  memory_id uuid NOT NULL CHECK (memory_id<>'00000000-0000-0000-0000-000000000000'::uuid),
  memory_sha256 bytea NOT NULL CHECK (octet_length(memory_sha256)=32),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,workspace_id,project_id,facet_kind,facet_version_id,memory_id),
  CONSTRAINT continuity_memory_links_version_fkey FOREIGN KEY
    (tenant_id,workspace_id,project_id,facet_kind,facet_version,facet_version_id)
    REFERENCES private.continuity_facet_versions
      (tenant_id,workspace_id,project_id,facet_kind,facet_version,facet_version_id),
  CONSTRAINT continuity_memory_links_source_fkey FOREIGN KEY (tenant_id,memory_id)
    REFERENCES private.memory_records(tenant_id,memory_id)
);

CREATE TABLE private.continuity_facet_evidence_links (
  tenant_id uuid NOT NULL,
  workspace_id uuid NOT NULL,
  project_id uuid NOT NULL,
  facet_kind text NOT NULL,
  facet_version bigint NOT NULL,
  facet_version_id uuid NOT NULL,
  evidence_id uuid NOT NULL CHECK (evidence_id<>'00000000-0000-0000-0000-000000000000'::uuid),
  evidence_sha256 bytea NOT NULL CHECK (octet_length(evidence_sha256)=32),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,workspace_id,project_id,facet_kind,facet_version_id,evidence_id),
  CONSTRAINT continuity_evidence_links_version_fkey FOREIGN KEY
    (tenant_id,workspace_id,project_id,facet_kind,facet_version,facet_version_id)
    REFERENCES private.continuity_facet_versions
      (tenant_id,workspace_id,project_id,facet_kind,facet_version,facet_version_id),
  CONSTRAINT continuity_evidence_links_source_fkey FOREIGN KEY (tenant_id,evidence_id)
    REFERENCES private.evidence_objects(tenant_id,evidence_id)
);

CREATE TABLE private.continuity_facet_slots (
  tenant_id uuid NOT NULL,
  workspace_id uuid NOT NULL,
  project_id uuid NOT NULL,
  facet_kind text NOT NULL CONSTRAINT continuity_facet_slots_kind_closed CHECK (facet_kind IN (
    'GOAL','CURRENT_STATE','DECISIONS','REJECTIONS','CONSTRAINTS','KNOWN_ISSUES',
    'NEXT_ACTIONS','ACTIVE_TASKS','RECENT_CHANGES','CODE','TESTS','CONFIG',
    'MIGRATIONS','PROCEDURES','OUTCOMES')),
  slot_version bigint NOT NULL DEFAULT 0 CHECK (slot_version>=0),
  current_version_id uuid,
  slot_state text CHECK (slot_state IN ('CURRENT','STALE','CONFLICTED')),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,workspace_id,project_id,facet_kind),
  CONSTRAINT continuity_facet_slots_project_fkey
    FOREIGN KEY (tenant_id,workspace_id,project_id)
    REFERENCES private.continuity_projects(tenant_id,workspace_id,project_id),
  CONSTRAINT continuity_facet_slots_shape CHECK (
    (slot_version=0 AND current_version_id IS NULL AND slot_state IS NULL)
    OR (slot_version>0 AND current_version_id IS NOT NULL AND slot_state IS NOT NULL)),
  CONSTRAINT continuity_facet_slots_current_fkey FOREIGN KEY
    (tenant_id,workspace_id,project_id,facet_kind,slot_version,current_version_id)
    REFERENCES private.continuity_facet_versions
      (tenant_id,workspace_id,project_id,facet_kind,facet_version,facet_version_id)
    DEFERRABLE INITIALLY DEFERRED
);

CREATE FUNCTION private.continuity_project_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path TO pg_catalog AS $$
BEGIN
  IF TG_OP='DELETE' THEN
    RAISE EXCEPTION 'continuity projects cannot be deleted' USING ERRCODE='23514';
  END IF;
  IF NEW.project_id IS DISTINCT FROM OLD.project_id
    OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
    OR NEW.workspace_id IS DISTINCT FROM OLD.workspace_id
    OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
    RAISE EXCEPTION 'continuity project parent is immutable' USING ERRCODE='23514';
  END IF;
  IF OLD.lifecycle_state='ARCHIVED' AND NEW IS DISTINCT FROM OLD THEN
    RAISE EXCEPTION 'archived continuity project is immutable' USING ERRCODE='23514';
  END IF;
  IF NEW.lifecycle_state<>OLD.lifecycle_state
    AND NOT (OLD.lifecycle_state='ACTIVE' AND NEW.lifecycle_state='ARCHIVED') THEN
    RAISE EXCEPTION 'invalid continuity project lifecycle transition' USING ERRCODE='23514';
  END IF;
  IF NEW.updated_at<OLD.updated_at THEN
    RAISE EXCEPTION 'continuity project updated_at cannot move backward' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END
$$;
CREATE TRIGGER continuity_project_mutation_guard
  BEFORE UPDATE OR DELETE ON private.continuity_projects
  FOR EACH ROW EXECUTE FUNCTION private.continuity_project_guard();

CREATE FUNCTION private.continuity_append_only_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path TO pg_catalog AS $$
BEGIN
  RAISE EXCEPTION 'continuity versions and source links are append-only' USING ERRCODE='23514';
END
$$;
CREATE TRIGGER continuity_facet_versions_append_only
  BEFORE UPDATE OR DELETE ON private.continuity_facet_versions
  FOR EACH ROW EXECUTE FUNCTION private.continuity_append_only_guard();
CREATE TRIGGER continuity_facet_memory_links_append_only
  BEFORE UPDATE OR DELETE ON private.continuity_facet_memory_links
  FOR EACH ROW EXECUTE FUNCTION private.continuity_append_only_guard();
CREATE TRIGGER continuity_facet_evidence_links_append_only
  BEFORE UPDATE OR DELETE ON private.continuity_facet_evidence_links
  FOR EACH ROW EXECUTE FUNCTION private.continuity_append_only_guard();

CREATE FUNCTION private.continuity_version_requires_source() RETURNS trigger
LANGUAGE plpgsql SET search_path TO pg_catalog AS $$
BEGIN
  IF NOT EXISTS (
      SELECT 1 FROM private.continuity_facet_memory_links link
      WHERE (link.tenant_id,link.workspace_id,link.project_id,link.facet_kind,
             link.facet_version,link.facet_version_id)=
            (NEW.tenant_id,NEW.workspace_id,NEW.project_id,NEW.facet_kind,
             NEW.facet_version,NEW.facet_version_id))
    AND NOT EXISTS (
      SELECT 1 FROM private.continuity_facet_evidence_links link
      WHERE (link.tenant_id,link.workspace_id,link.project_id,link.facet_kind,
             link.facet_version,link.facet_version_id)=
            (NEW.tenant_id,NEW.workspace_id,NEW.project_id,NEW.facet_kind,
             NEW.facet_version,NEW.facet_version_id)) THEN
    RAISE EXCEPTION 'continuity facet version requires an exact source'
      USING ERRCODE='23514';
  END IF;
  RETURN NULL;
END
$$;
CREATE CONSTRAINT TRIGGER continuity_facet_version_source_required
  AFTER INSERT ON private.continuity_facet_versions DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION private.continuity_version_requires_source();

ALTER TABLE private.continuity_projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.continuity_facet_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.continuity_facet_memory_links ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.continuity_facet_evidence_links ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.continuity_facet_slots ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.continuity_projects FORCE ROW LEVEL SECURITY;
ALTER TABLE private.continuity_facet_versions FORCE ROW LEVEL SECURITY;
ALTER TABLE private.continuity_facet_memory_links FORCE ROW LEVEL SECURITY;
ALTER TABLE private.continuity_facet_evidence_links FORCE ROW LEVEL SECURITY;
ALTER TABLE private.continuity_facet_slots FORCE ROW LEVEL SECURITY;

CREATE POLICY continuity_projects_tenant ON private.continuity_projects
  USING (current_user='role_migration_owner'
    OR tenant_id=current_setting('humaux.tenant_id',true)::uuid)
  WITH CHECK (current_user='role_migration_owner'
    OR tenant_id=current_setting('humaux.tenant_id',true)::uuid);
CREATE POLICY continuity_facet_versions_tenant ON private.continuity_facet_versions
  USING (current_user='role_migration_owner'
    OR tenant_id=current_setting('humaux.tenant_id',true)::uuid)
  WITH CHECK (current_user='role_migration_owner'
    OR tenant_id=current_setting('humaux.tenant_id',true)::uuid);
CREATE POLICY continuity_facet_memory_links_tenant ON private.continuity_facet_memory_links
  USING (current_user='role_migration_owner'
    OR tenant_id=current_setting('humaux.tenant_id',true)::uuid)
  WITH CHECK (current_user='role_migration_owner'
    OR tenant_id=current_setting('humaux.tenant_id',true)::uuid);
CREATE POLICY continuity_facet_evidence_links_tenant ON private.continuity_facet_evidence_links
  USING (current_user='role_migration_owner'
    OR tenant_id=current_setting('humaux.tenant_id',true)::uuid)
  WITH CHECK (current_user='role_migration_owner'
    OR tenant_id=current_setting('humaux.tenant_id',true)::uuid);
CREATE POLICY continuity_facet_slots_tenant ON private.continuity_facet_slots
  USING (current_user='role_migration_owner'
    OR tenant_id=current_setting('humaux.tenant_id',true)::uuid)
  WITH CHECK (current_user='role_migration_owner'
    OR tenant_id=current_setting('humaux.tenant_id',true)::uuid);

-- The legacy Evidence policy remains byte-for-byte untouched.  The SELECT pair makes the
-- marker-on read set exact; the UPDATE pair supplies the policy path PostgreSQL also applies
-- to SELECT FOR SHARE while its WITH CHECK clauses forbid real marker-on mutation.
CREATE POLICY continuity_evidence_owner_exact_allow ON private.evidence_objects
  AS PERMISSIVE FOR SELECT TO role_migration_owner USING (
    current_setting('humaux.continuity_publish',true)='1' AND CASE
      WHEN pg_catalog.pg_input_is_valid(current_setting('humaux.tenant_id',true),'uuid')
       AND pg_catalog.pg_input_is_valid(current_setting('humaux.workspace_id',true),'uuid')
       AND pg_catalog.pg_input_is_valid(current_setting('humaux.user_id',true),'uuid')
      THEN tenant_id=current_setting('humaux.tenant_id',true)::uuid
       AND (
         visibility_class='TENANT_SHARED'
         OR (visibility_class='USER_PRIVATE'
           AND current_setting('humaux.user_id',true)::uuid<>
             '00000000-0000-0000-0000-000000000000'::uuid
           AND visibility_user_id=current_setting('humaux.user_id',true)::uuid)
         OR (visibility_class='WORKSPACE_SHARED'
           AND visibility_workspace_id=current_setting('humaux.workspace_id',true)::uuid))
      ELSE false
    END);
CREATE POLICY continuity_evidence_owner_exact_guard ON private.evidence_objects
  AS RESTRICTIVE FOR SELECT TO role_migration_owner USING (
    current_setting('humaux.continuity_publish',true) IS DISTINCT FROM '1' OR CASE
      WHEN pg_catalog.pg_input_is_valid(current_setting('humaux.tenant_id',true),'uuid')
       AND pg_catalog.pg_input_is_valid(current_setting('humaux.workspace_id',true),'uuid')
       AND pg_catalog.pg_input_is_valid(current_setting('humaux.user_id',true),'uuid')
      THEN tenant_id=current_setting('humaux.tenant_id',true)::uuid
       AND (
         visibility_class='TENANT_SHARED'
         OR (visibility_class='USER_PRIVATE'
           AND current_setting('humaux.user_id',true)::uuid<>
             '00000000-0000-0000-0000-000000000000'::uuid
           AND visibility_user_id=current_setting('humaux.user_id',true)::uuid)
         OR (visibility_class='WORKSPACE_SHARED'
           AND visibility_workspace_id=current_setting('humaux.workspace_id',true)::uuid))
      ELSE false
    END);
CREATE POLICY continuity_evidence_owner_lock_allow ON private.evidence_objects
  AS PERMISSIVE FOR UPDATE TO role_migration_owner USING (
    current_setting('humaux.continuity_publish',true)='1' AND CASE
      WHEN pg_catalog.pg_input_is_valid(current_setting('humaux.tenant_id',true),'uuid')
       AND pg_catalog.pg_input_is_valid(current_setting('humaux.workspace_id',true),'uuid')
       AND pg_catalog.pg_input_is_valid(current_setting('humaux.user_id',true),'uuid')
      THEN tenant_id=current_setting('humaux.tenant_id',true)::uuid
       AND (
         visibility_class='TENANT_SHARED'
         OR (visibility_class='USER_PRIVATE'
           AND current_setting('humaux.user_id',true)::uuid<>
             '00000000-0000-0000-0000-000000000000'::uuid
           AND visibility_user_id=current_setting('humaux.user_id',true)::uuid)
         OR (visibility_class='WORKSPACE_SHARED'
           AND visibility_workspace_id=current_setting('humaux.workspace_id',true)::uuid))
      ELSE false
    END) WITH CHECK (false);
CREATE POLICY continuity_evidence_owner_lock_guard ON private.evidence_objects
  AS RESTRICTIVE FOR UPDATE TO role_migration_owner USING (
    current_setting('humaux.continuity_publish',true) IS DISTINCT FROM '1' OR CASE
      WHEN pg_catalog.pg_input_is_valid(current_setting('humaux.tenant_id',true),'uuid')
       AND pg_catalog.pg_input_is_valid(current_setting('humaux.workspace_id',true),'uuid')
       AND pg_catalog.pg_input_is_valid(current_setting('humaux.user_id',true),'uuid')
      THEN tenant_id=current_setting('humaux.tenant_id',true)::uuid
       AND (
         visibility_class='TENANT_SHARED'
         OR (visibility_class='USER_PRIVATE'
           AND current_setting('humaux.user_id',true)::uuid<>
             '00000000-0000-0000-0000-000000000000'::uuid
           AND visibility_user_id=current_setting('humaux.user_id',true)::uuid)
         OR (visibility_class='WORKSPACE_SHARED'
           AND visibility_workspace_id=current_setting('humaux.workspace_id',true)::uuid))
      ELSE false
    END) WITH CHECK (
      current_setting('humaux.continuity_publish',true) IS DISTINCT FROM '1');

CREATE FUNCTION private.assert_continuity_context(
  p_tenant_id uuid,p_workspace_id uuid,p_principal_id uuid,p_user_id uuid)
RETURNS void LANGUAGE plpgsql STABLE SECURITY INVOKER
SET search_path TO pg_catalog AS $$
DECLARE
  nil uuid := '00000000-0000-0000-0000-000000000000'::uuid;
BEGIN
  IF p_tenant_id IS NULL OR p_workspace_id IS NULL OR p_principal_id IS NULL
    OR NOT (CASE
      WHEN current_setting('humaux.tenant_id',true) IS NOT NULL
       AND current_setting('humaux.workspace_id',true) IS NOT NULL
       AND current_setting('humaux.principal_id',true) IS NOT NULL
       AND current_setting('humaux.user_id',true) IS NOT NULL
       AND pg_input_is_valid(current_setting('humaux.tenant_id',true),'uuid')
       AND pg_input_is_valid(current_setting('humaux.workspace_id',true),'uuid')
       AND pg_input_is_valid(current_setting('humaux.principal_id',true),'uuid')
       AND pg_input_is_valid(current_setting('humaux.user_id',true),'uuid') THEN
        current_setting('humaux.tenant_id',true)::uuid=p_tenant_id
        AND current_setting('humaux.workspace_id',true)::uuid=p_workspace_id
        AND current_setting('humaux.principal_id',true)::uuid=p_principal_id
        AND NULLIF(current_setting('humaux.user_id',true)::uuid,nil)
          IS NOT DISTINCT FROM p_user_id
      ELSE false
    END) THEN
    RAISE EXCEPTION 'continuity transaction context mismatch' USING ERRCODE='42501';
  END IF;
END
$$;

CREATE FUNCTION private.register_continuity_project(
  p_tenant_id uuid,p_workspace_id uuid,p_project_id uuid,p_principal_id uuid,
  p_user_id uuid,p_title text)
RETURNS uuid LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path TO pg_catalog AS $$
DECLARE
  project_row private.continuity_projects%ROWTYPE;
  slot_count bigint;
BEGIN
  PERFORM private.assert_continuity_context(
    p_tenant_id,p_workspace_id,p_principal_id,p_user_id);
  IF p_project_id IS NULL OR p_title IS NULL OR uuid_extract_version(p_project_id)<>7
    OR char_length(p_title) NOT BETWEEN 1 AND 256 THEN
    RAISE EXCEPTION 'invalid continuity project identity or title' USING ERRCODE='22023';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM control.workspaces workspace
    WHERE workspace.tenant_id=p_tenant_id AND workspace.workspace_id=p_workspace_id) THEN
    RAISE EXCEPTION 'continuity workspace unavailable' USING ERRCODE='42501';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(p_project_id::text,0));
  SELECT * INTO project_row FROM private.continuity_projects project
    WHERE project.project_id=p_project_id;
  IF FOUND THEN
    SELECT count(*) INTO slot_count FROM private.continuity_facet_slots slot
      WHERE slot.tenant_id=p_tenant_id AND slot.workspace_id=p_workspace_id
        AND slot.project_id=p_project_id;
    IF project_row.tenant_id<>p_tenant_id OR project_row.workspace_id<>p_workspace_id
      OR project_row.title<>p_title OR project_row.lifecycle_state<>'ACTIVE'
      OR slot_count<>15 THEN
      RAISE EXCEPTION 'continuity project retry does not match canonical registry state'
        USING ERRCODE='23514';
    END IF;
    RETURN p_project_id;
  END IF;
  INSERT INTO private.continuity_projects(project_id,tenant_id,workspace_id,title)
    VALUES(p_project_id,p_tenant_id,p_workspace_id,p_title);
  INSERT INTO private.continuity_facet_slots(
    tenant_id,workspace_id,project_id,facet_kind)
  SELECT p_tenant_id,p_workspace_id,p_project_id,facet_kind
  FROM unnest(ARRAY[
    'GOAL','CURRENT_STATE','DECISIONS','REJECTIONS','CONSTRAINTS','KNOWN_ISSUES',
    'NEXT_ACTIONS','ACTIVE_TASKS','RECENT_CHANGES','CODE','TESTS','CONFIG',
    'MIGRATIONS','PROCEDURES','OUTCOMES']::text[]) AS facet_kind;
  RETURN p_project_id;
END
$$;

CREATE FUNCTION private.publish_continuity_facet(
  p_tenant_id uuid,p_workspace_id uuid,p_project_id uuid,p_principal_id uuid,
  p_user_id uuid,p_facet_kind text,p_expected_slot_version bigint,
  p_published_state text,p_body jsonb,
  p_memory_ids uuid[],p_memory_hashes bytea[],
  p_evidence_ids uuid[],p_evidence_hashes bytea[])
RETURNS TABLE(facet_version_id uuid,facet_version bigint,slot_version bigint,body_sha256 bytea)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path TO pg_catalog
SET humaux.continuity_publish TO '1' AS $$
DECLARE
  locked_version bigint;
  new_version_id uuid := uuidv7();
  new_body_hash bytea := sha256(convert_to(p_body::text,'UTF8'));
  affected bigint;
  locked_source_count bigint;
  memory_count bigint := cardinality(p_memory_ids);
  evidence_count bigint := cardinality(p_evidence_ids);
BEGIN
  PERFORM private.assert_continuity_context(
    p_tenant_id,p_workspace_id,p_principal_id,p_user_id);
  IF p_project_id IS NULL OR p_facet_kind IS NULL OR p_expected_slot_version IS NULL
    OR p_published_state IS NULL OR p_body IS NULL OR p_expected_slot_version<0
    OR p_published_state NOT IN ('CURRENT','CONFLICTED') OR p_facet_kind NOT IN (
      'GOAL','CURRENT_STATE','DECISIONS','REJECTIONS','CONSTRAINTS','KNOWN_ISSUES',
      'NEXT_ACTIONS','ACTIVE_TASKS','RECENT_CHANGES','CODE','TESTS','CONFIG',
      'MIGRATIONS','PROCEDURES','OUTCOMES') THEN
    RAISE EXCEPTION 'invalid continuity publication scalar' USING ERRCODE='22023';
  END IF;
  IF p_memory_ids IS NULL OR p_memory_hashes IS NULL
    OR p_evidence_ids IS NULL OR p_evidence_hashes IS NULL
    OR memory_count<>cardinality(p_memory_hashes)
    OR evidence_count<>cardinality(p_evidence_hashes)
    OR memory_count+evidence_count=0
    OR (memory_count>0 AND (array_ndims(p_memory_ids)<>1 OR array_lower(p_memory_ids,1)<>1
      OR array_ndims(p_memory_hashes)<>1 OR array_lower(p_memory_hashes,1)<>1))
    OR (evidence_count>0 AND (array_ndims(p_evidence_ids)<>1 OR array_lower(p_evidence_ids,1)<>1
      OR array_ndims(p_evidence_hashes)<>1 OR array_lower(p_evidence_hashes,1)<>1))
    OR EXISTS (SELECT 1 FROM unnest(p_memory_ids,p_memory_hashes) source(id,hash)
      WHERE id IS NULL OR id='00000000-0000-0000-0000-000000000000'::uuid
        OR hash IS NULL OR octet_length(hash)<>32)
    OR EXISTS (SELECT 1 FROM unnest(p_evidence_ids,p_evidence_hashes) source(id,hash)
      WHERE id IS NULL OR id='00000000-0000-0000-0000-000000000000'::uuid
        OR hash IS NULL OR octet_length(hash)<>32)
    OR (SELECT count(*)<>count(DISTINCT id) FROM unnest(p_memory_ids) source(id))
    OR (SELECT count(*)<>count(DISTINCT id) FROM unnest(p_evidence_ids) source(id)) THEN
    RAISE EXCEPTION 'invalid continuity publication source arrays' USING ERRCODE='22023';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM private.continuity_projects project
    WHERE project.tenant_id=p_tenant_id AND project.workspace_id=p_workspace_id
      AND project.project_id=p_project_id AND project.lifecycle_state='ACTIVE') THEN
    RAISE EXCEPTION 'continuity project unavailable' USING ERRCODE='42501';
  END IF;
  SELECT slot.slot_version INTO locked_version FROM private.continuity_facet_slots slot
    WHERE slot.tenant_id=p_tenant_id AND slot.workspace_id=p_workspace_id
      AND slot.project_id=p_project_id AND slot.facet_kind=p_facet_kind
    FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'continuity slot missing or corrupt' USING ERRCODE='P9I01';
  END IF;
  IF locked_version<>p_expected_slot_version THEN
    RAISE EXCEPTION 'continuity slot version conflict' USING ERRCODE='P9C01';
  END IF;
  SELECT count(*) INTO locked_source_count FROM (
    SELECT memory.memory_id FROM private.memory_records memory
      JOIN unnest(p_memory_ids,p_memory_hashes) source(id,hash)
        ON memory.tenant_id=p_tenant_id AND memory.memory_id=source.id
       AND memory.status='active'
       AND sha256(convert_to(memory.content::text,'UTF8'))=source.hash
       AND (memory.visibility_class='TENANT_SHARED'
         OR (memory.visibility_class='USER_PRIVATE' AND p_user_id IS NOT NULL
           AND memory.visibility_user_id=p_user_id)
         OR (memory.visibility_class='WORKSPACE_SHARED'
           AND memory.visibility_workspace_id=p_workspace_id))
      ORDER BY memory.memory_id FOR SHARE OF memory
  ) locked_memory;
  IF locked_source_count<>memory_count THEN
    RAISE EXCEPTION 'continuity memory source unavailable or changed' USING ERRCODE='23514';
  END IF;
  SELECT count(*) INTO locked_source_count FROM (
    SELECT evidence.evidence_id FROM private.evidence_objects evidence
      JOIN unnest(p_evidence_ids,p_evidence_hashes) source(id,hash)
        ON evidence.tenant_id=p_tenant_id AND evidence.evidence_id=source.id
       AND evidence.payload_sha256=source.hash
       AND (evidence.visibility_class='TENANT_SHARED'
         OR (evidence.visibility_class='USER_PRIVATE' AND p_user_id IS NOT NULL
           AND evidence.visibility_user_id=p_user_id)
         OR (evidence.visibility_class='WORKSPACE_SHARED'
           AND evidence.visibility_workspace_id=p_workspace_id))
      ORDER BY evidence.evidence_id FOR SHARE OF evidence
  ) locked_evidence;
  IF locked_source_count<>evidence_count THEN
    RAISE EXCEPTION 'continuity evidence source unavailable or changed' USING ERRCODE='23514';
  END IF;
  INSERT INTO private.continuity_facet_versions(
    tenant_id,workspace_id,project_id,facet_kind,facet_version_id,facet_version,
    body,body_sha256,authored_by_principal_id)
  VALUES(p_tenant_id,p_workspace_id,p_project_id,p_facet_kind,new_version_id,
    p_expected_slot_version+1,p_body,new_body_hash,p_principal_id);
  INSERT INTO private.continuity_facet_memory_links(
    tenant_id,workspace_id,project_id,facet_kind,facet_version,facet_version_id,
    memory_id,memory_sha256)
  SELECT p_tenant_id,p_workspace_id,p_project_id,p_facet_kind,
    p_expected_slot_version+1,new_version_id,source.id,source.hash
  FROM unnest(p_memory_ids,p_memory_hashes) source(id,hash);
  INSERT INTO private.continuity_facet_evidence_links(
    tenant_id,workspace_id,project_id,facet_kind,facet_version,facet_version_id,
    evidence_id,evidence_sha256)
  SELECT p_tenant_id,p_workspace_id,p_project_id,p_facet_kind,
    p_expected_slot_version+1,new_version_id,source.id,source.hash
  FROM unnest(p_evidence_ids,p_evidence_hashes) source(id,hash);
  UPDATE private.continuity_facet_slots slot SET
    slot_version=p_expected_slot_version+1,current_version_id=new_version_id,
    slot_state=p_published_state,updated_at=now()
  WHERE slot.tenant_id=p_tenant_id AND slot.workspace_id=p_workspace_id
    AND slot.project_id=p_project_id AND slot.facet_kind=p_facet_kind
    AND slot.slot_version=p_expected_slot_version;
  GET DIAGNOSTICS affected=ROW_COUNT;
  IF affected<>1 THEN
    RAISE EXCEPTION 'continuity slot CAS invariant failed' USING ERRCODE='P9I01';
  END IF;
  RETURN QUERY SELECT new_version_id,p_expected_slot_version+1,
    p_expected_slot_version+1,new_body_hash;
END
$$;

ALTER TABLE private.continuity_projects OWNER TO role_migration_owner;
ALTER TABLE private.continuity_facet_versions OWNER TO role_migration_owner;
ALTER TABLE private.continuity_facet_memory_links OWNER TO role_migration_owner;
ALTER TABLE private.continuity_facet_evidence_links OWNER TO role_migration_owner;
ALTER TABLE private.continuity_facet_slots OWNER TO role_migration_owner;
ALTER FUNCTION private.continuity_project_guard() OWNER TO role_migration_owner;
ALTER FUNCTION private.continuity_append_only_guard() OWNER TO role_migration_owner;
ALTER FUNCTION private.continuity_version_requires_source() OWNER TO role_migration_owner;
ALTER FUNCTION private.assert_continuity_context(uuid,uuid,uuid,uuid)
  OWNER TO role_migration_owner;
ALTER FUNCTION private.register_continuity_project(uuid,uuid,uuid,uuid,uuid,text)
  OWNER TO role_migration_owner;
ALTER FUNCTION private.publish_continuity_facet(uuid,uuid,uuid,uuid,uuid,text,bigint,
  text,jsonb,uuid[],bytea[],uuid[],bytea[]) OWNER TO role_migration_owner;

REVOKE ALL ON private.continuity_projects,private.continuity_facet_versions,
  private.continuity_facet_memory_links,private.continuity_facet_evidence_links,
  private.continuity_facet_slots
  FROM PUBLIC,role_admin,role_gateway,role_private_worker,role_consolidation_worker,
    role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;
REVOKE ALL ON FUNCTION private.continuity_project_guard(),
  private.continuity_append_only_guard(),private.continuity_version_requires_source(),
  private.assert_continuity_context(uuid,uuid,uuid,uuid),
  private.register_continuity_project(uuid,uuid,uuid,uuid,uuid,text),
  private.publish_continuity_facet(uuid,uuid,uuid,uuid,uuid,text,bigint,text,jsonb,
    uuid[],bytea[],uuid[],bytea[])
  FROM PUBLIC,role_admin,role_gateway,role_private_worker,role_consolidation_worker,
    role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;
GRANT EXECUTE ON FUNCTION
  private.register_continuity_project(uuid,uuid,uuid,uuid,uuid,text),
  private.publish_continuity_facet(uuid,uuid,uuid,uuid,uuid,text,bigint,text,jsonb,
    uuid[],bytea[],uuid[],bytea[])
  TO role_gateway;
