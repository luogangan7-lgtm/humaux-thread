-- Phase 9 anonymous dispatch.  This is an expand-only seam: legacy releases retain their
-- release-addressed events, while assessed releases dispatch only a random source and sealed
-- envelope digest to the public worker.

ALTER TABLE ops.outbox
  ADD COLUMN anonymous_source_id uuid,
  ADD COLUMN candidate_envelope_sha256 bytea,
  ADD COLUMN anonymous_source_revision bigint;

ALTER TABLE ops.outbox
  DROP CONSTRAINT outbox_row_class,
  ADD CONSTRAINT outbox_row_class CHECK (
    (event_type='PUBLIC_OBJECT_CHANGED' AND contribution_release_id IS NULL
      AND anonymous_source_id IS NULL AND candidate_envelope_sha256 IS NULL AND anonymous_source_revision IS NULL
      AND evidence_id IS NULL AND stream_seq IS NULL
      AND (public_claim_id IS NULL)<>(public_synthesis_id IS NULL)
      AND object_revision IS NOT NULL AND object_revision>0)
    OR (event_type IN ('PUBLIC_RELEASE','PUBLIC_REVOKE') AND contribution_release_id IS NOT NULL
      AND anonymous_source_id IS NULL AND candidate_envelope_sha256 IS NULL AND anonymous_source_revision IS NULL
      AND evidence_id IS NULL AND stream_seq IS NULL AND public_claim_id IS NULL
      AND public_synthesis_id IS NULL AND object_revision IS NULL)
    OR (event_type IN ('PUBLIC_ANONYMOUS_RELEASE','PUBLIC_ANONYMOUS_REVOKE')
      AND contribution_release_id IS NULL AND anonymous_source_id IS NOT NULL
      AND octet_length(candidate_envelope_sha256)=32
      AND ((event_type='PUBLIC_ANONYMOUS_RELEASE' AND anonymous_source_revision=1)
        OR (event_type='PUBLIC_ANONYMOUS_REVOKE' AND anonymous_source_revision=2))
      AND evidence_id IS NULL AND stream_seq IS NULL AND public_claim_id IS NULL
      AND public_synthesis_id IS NULL AND object_revision IS NULL)
    OR (event_type NOT IN ('PUBLIC_RELEASE','PUBLIC_REVOKE','PUBLIC_ANONYMOUS_RELEASE',
      'PUBLIC_ANONYMOUS_REVOKE','PUBLIC_OBJECT_CHANGED')
      AND contribution_release_id IS NULL AND anonymous_source_id IS NULL
      AND candidate_envelope_sha256 IS NULL AND anonymous_source_revision IS NULL
      AND evidence_id IS NOT NULL AND stream_seq IS NOT NULL
      AND public_claim_id IS NULL AND public_synthesis_id IS NULL AND object_revision IS NULL)
  );

CREATE UNIQUE INDEX outbox_anonymous_source_event_unique
  ON ops.outbox(anonymous_source_id,event_type)
  WHERE anonymous_source_id IS NOT NULL;

CREATE OR REPLACE FUNCTION staging.require_contribution_outbox()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog,control,staging,ops AS $$
DECLARE required_event text; anonymous_event text; is_assessed boolean;
BEGIN
  required_event := CASE WHEN TG_OP='INSERT' THEN 'PUBLIC_RELEASE'
    WHEN NEW.state='REVOKED' AND OLD.state='ACTIVE' THEN 'PUBLIC_REVOKE' ELSE NULL END;
  IF required_event IS NULL THEN RETURN NULL; END IF;
  -- Revoke workers carry tenant scope but no end-user identity, so the user-scoped assessment
  -- table is intentionally invisible here. The protected random bridge is the durable marker
  -- that this release completed the assessed path; the deferred INSERT check sees it at COMMIT.
  SELECT NEW.candidate_id IS NOT NULL AND EXISTS(SELECT 1
    FROM control.anonymous_source_lineage lineage
    WHERE lineage.contribution_release_id=NEW.contribution_release_id
      AND lineage.tenant_id=NEW.tenant_id)
    INTO is_assessed;
  anonymous_event := CASE required_event WHEN 'PUBLIC_RELEASE' THEN 'PUBLIC_ANONYMOUS_RELEASE'
    WHEN 'PUBLIC_REVOKE' THEN 'PUBLIC_ANONYMOUS_REVOKE' END;
  IF NOT EXISTS(SELECT 1 FROM staging.contribution_release_sources s
      WHERE s.contribution_release_id=NEW.contribution_release_id)
    OR (is_assessed AND NOT EXISTS(
      SELECT 1 FROM control.anonymous_source_lineage lineage
      JOIN staging.sanitized_public_candidates sealed
        ON sealed.tenant_id=lineage.tenant_id AND sealed.anonymous_source_id=lineage.anonymous_source_id
      JOIN ops.outbox o ON o.tenant_id=NEW.tenant_id AND o.event_type=anonymous_event
        AND o.anonymous_source_id=lineage.anonymous_source_id
        AND o.candidate_envelope_sha256=sealed.envelope_sha256
      WHERE lineage.contribution_release_id=NEW.contribution_release_id))
    OR (NOT is_assessed AND NOT EXISTS(SELECT 1 FROM ops.outbox o
      WHERE o.tenant_id=NEW.tenant_id AND o.contribution_release_id=NEW.contribution_release_id
        AND o.event_type=required_event)) THEN
    RAISE EXCEPTION 'release transition needs sources and its exact legacy or anonymous outbox in one transaction'
      USING ERRCODE='23514';
  END IF;
  RETURN NULL;
END;
$$;

CREATE TABLE public.anonymous_source_authority_events (
  authority_seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  anonymous_source_id uuid NOT NULL CHECK(uuid_extract_version(anonymous_source_id)=4),
  candidate_envelope_sha256 bytea NOT NULL CHECK(octet_length(candidate_envelope_sha256)=32),
  source_revision bigint NOT NULL CHECK(source_revision>0),
  event_type text NOT NULL CHECK(event_type IN ('ADMIT','REVOKE')),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(anonymous_source_id,candidate_envelope_sha256,source_revision)
);

CREATE TABLE ops.anonymous_public_revocations (
  anonymous_source_id uuid NOT NULL CHECK(uuid_extract_version(anonymous_source_id)=4),
  candidate_envelope_sha256 bytea NOT NULL CHECK(octet_length(candidate_envelope_sha256)=32),
  revoke_event_id uuid NOT NULL UNIQUE REFERENCES ops.outbox(outbox_id),
  PRIMARY KEY(anonymous_source_id,candidate_envelope_sha256)
);
CREATE FUNCTION ops.guard_anonymous_public_revocation_fact()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog,ops AS $$
BEGIN
  IF NOT EXISTS(SELECT 1 FROM ops.outbox o WHERE o.outbox_id=NEW.revoke_event_id
    AND o.event_type='PUBLIC_ANONYMOUS_REVOKE' AND o.anonymous_source_id=NEW.anonymous_source_id
    AND o.candidate_envelope_sha256=NEW.candidate_envelope_sha256) THEN
    RAISE EXCEPTION 'anonymous revocation fence needs its exact private outbox event' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER anonymous_public_revocation_fact_guard BEFORE INSERT ON ops.anonymous_public_revocations
  FOR EACH ROW EXECUTE FUNCTION ops.guard_anonymous_public_revocation_fact();
CREATE FUNCTION ops.record_anonymous_public_revocation_fact()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,ops AS $$
BEGIN
  IF NEW.event_type='PUBLIC_ANONYMOUS_REVOKE' THEN
    INSERT INTO ops.anonymous_public_revocations(anonymous_source_id,candidate_envelope_sha256,revoke_event_id)
      VALUES(NEW.anonymous_source_id,NEW.candidate_envelope_sha256,NEW.outbox_id);
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER anonymous_public_revocation_fact AFTER INSERT ON ops.outbox
  FOR EACH ROW EXECUTE FUNCTION ops.record_anonymous_public_revocation_fact();

CREATE OR REPLACE VIEW public.eligible_objects WITH (security_invoker=true) AS
  SELECT c.claim_id AS object_id,'CLAIM'::text AS object_kind,c.content,c.object_revision,
    c.current_evaluation_id
  FROM public.claims c JOIN public.claim_trust_evaluations e ON e.evaluation_id=c.current_evaluation_id
  WHERE c.moderation_state='SUPPORTED' AND e.moderation_state='SUPPORTED'
    AND public.public_receipt_matches(c.claim_id,NULL,c.current_evaluation_id,c.object_revision,c.content)
    AND NOT EXISTS(SELECT 1 FROM public.source_closure closure JOIN public.sources source
      ON source.source_id=closure.root_source_id JOIN ops.anonymous_public_revocations fence
      ON fence.anonymous_source_id=source.source_id
       AND fence.candidate_envelope_sha256=source.anonymous_envelope_sha256
      WHERE closure.claim_id=c.claim_id AND closure.is_current)
  UNION ALL
  SELECT s.synthesis_id,'SYNTHESIS',s.content,s.object_revision,s.current_evaluation_id
  FROM public.syntheses s JOIN public.claim_trust_evaluations e ON e.evaluation_id=s.current_evaluation_id
  WHERE s.moderation_state='SUPPORTED' AND e.moderation_state='SUPPORTED'
    AND public.public_receipt_matches(NULL,s.synthesis_id,s.current_evaluation_id,s.object_revision,s.content)
    AND NOT EXISTS(SELECT 1 FROM public.source_closure closure JOIN public.sources source
      ON source.source_id=closure.root_source_id JOIN ops.anonymous_public_revocations fence
      ON fence.anonymous_source_id=source.source_id
       AND fence.candidate_envelope_sha256=source.anonymous_envelope_sha256
      WHERE closure.synthesis_id=s.synthesis_id AND closure.is_current);

CREATE OR REPLACE FUNCTION ops.guard_outbox_identity()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog,control,staging,ops AS $$
DECLARE release_state text; actual_revision bigint; lineage_release_id uuid; lineage_tenant_id uuid;
BEGIN
  IF TG_OP='UPDATE' THEN
    IF (to_jsonb(NEW)-ARRAY['status','lease_owner','lease_expires_at','processed_at']) IS DISTINCT FROM
       (to_jsonb(OLD)-ARRAY['status','lease_owner','lease_expires_at','processed_at']) THEN
      RAISE EXCEPTION 'outbox event identity is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
  END IF;
  IF current_user='role_public_worker' AND NEW.event_type<>'PUBLIC_OBJECT_CHANGED' THEN
    RAISE EXCEPTION 'public producer may emit only public object changes' USING ERRCODE='42501';
  END IF;
  IF NEW.event_type='PUBLIC_OBJECT_CHANGED' AND current_user NOT IN ('role_public_worker','role_migration_owner')
    AND NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=current_user AND rolsuper) THEN
    RAISE EXCEPTION 'only the public producer may create object events' USING ERRCODE='42501';
  END IF;
  IF NEW.contribution_release_id IS NOT NULL THEN
    PERFORM staging.lock_contribution_release(NEW.contribution_release_id,false);
    SELECT r.state INTO release_state FROM staging.contribution_releases r
      WHERE r.contribution_release_id=NEW.contribution_release_id AND r.tenant_id=NEW.tenant_id;
    IF release_state IS DISTINCT FROM (CASE NEW.event_type WHEN 'PUBLIC_RELEASE' THEN 'ACTIVE'
      WHEN 'PUBLIC_REVOKE' THEN 'REVOKED' ELSE NULL END) OR release_state IS NULL THEN
      RAISE EXCEPTION 'outbox event does not match visible release transition' USING ERRCODE='23514';
    END IF;
  ELSIF NEW.event_type IN ('PUBLIC_ANONYMOUS_RELEASE','PUBLIC_ANONYMOUS_REVOKE') THEN
    SELECT l.contribution_release_id,l.tenant_id INTO lineage_release_id,lineage_tenant_id
      FROM control.anonymous_source_lineage l
      JOIN staging.sanitized_public_candidates c
        ON c.tenant_id=l.tenant_id AND c.anonymous_source_id=l.anonymous_source_id
      WHERE l.anonymous_source_id=NEW.anonymous_source_id
        AND c.envelope_sha256=NEW.candidate_envelope_sha256;
    IF NOT FOUND OR lineage_tenant_id IS DISTINCT FROM NEW.tenant_id THEN
      RAISE EXCEPTION 'anonymous outbox event is not bound to one sealed private release' USING ERRCODE='23514';
    END IF;
    PERFORM staging.lock_contribution_release(lineage_release_id,false);
    SELECT state INTO release_state FROM staging.contribution_releases
      WHERE contribution_release_id=lineage_release_id AND tenant_id=NEW.tenant_id;
    IF release_state IS DISTINCT FROM (CASE NEW.event_type WHEN 'PUBLIC_ANONYMOUS_RELEASE' THEN 'ACTIVE'
      WHEN 'PUBLIC_ANONYMOUS_REVOKE' THEN 'REVOKED' END) THEN
      RAISE EXCEPTION 'anonymous outbox event does not match protected release transition' USING ERRCODE='23514';
    END IF;
  ELSIF NEW.event_type='PUBLIC_OBJECT_CHANGED' THEN
    IF NEW.public_claim_id IS NOT NULL THEN
      SELECT object_revision INTO actual_revision FROM public.claims WHERE claim_id=NEW.public_claim_id;
    ELSE
      SELECT object_revision INTO actual_revision FROM public.syntheses WHERE synthesis_id=NEW.public_synthesis_id;
    END IF;
    IF actual_revision IS DISTINCT FROM NEW.object_revision THEN
      RAISE EXCEPTION 'object event revision is not current' USING ERRCODE='23514';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

-- The public worker supplies exactly the opaque pair.  The definer checks the sealed envelope,
-- appends REVOKE facts, and transitions affected objects.  The caller writes required object
-- outbox rows in the same surrounding transaction from this returned revision list.
CREATE FUNCTION public.revoke_anonymous_source(p_anonymous_source_id uuid,p_envelope_sha256 bytea,p_source_revision bigint)
RETURNS TABLE(claim_id uuid,synthesis_id uuid,object_revision bigint)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,staging,public AS $$
DECLARE current_row record; next_revision bigint; has_survivor boolean;
BEGIN
  IF uuid_extract_version(p_anonymous_source_id)<>4 OR octet_length(p_envelope_sha256)<>32 OR p_source_revision<>2 THEN
    RAISE EXCEPTION 'anonymous revocation requires one random source and exact envelope' USING ERRCODE='22023';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM ops.outbox WHERE event_type='PUBLIC_ANONYMOUS_REVOKE'
    AND anonymous_source_id=p_anonymous_source_id AND candidate_envelope_sha256=p_envelope_sha256
    AND anonymous_source_revision=p_source_revision) THEN
    RAISE EXCEPTION 'anonymous revocation requires its exact private outbox authority' USING ERRCODE='42501';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'phase9-anonymous-revoke:'||p_anonymous_source_id::text||':'||encode(p_envelope_sha256,'hex'),0));
  IF NOT EXISTS(SELECT 1 FROM staging.sanitized_public_candidates
    WHERE anonymous_source_id=p_anonymous_source_id AND envelope_sha256=p_envelope_sha256) THEN
    RAISE EXCEPTION 'anonymous revocation envelope is not sealed' USING ERRCODE='23503';
  END IF;
  IF EXISTS(SELECT 1 FROM public.anonymous_source_authority_events
    WHERE anonymous_source_id=p_anonymous_source_id AND candidate_envelope_sha256=p_envelope_sha256
      AND source_revision>=p_source_revision) THEN
    RETURN;
  END IF;
  INSERT INTO public.anonymous_source_authority_events(anonymous_source_id,candidate_envelope_sha256,
    source_revision,event_type) VALUES(p_anonymous_source_id,p_envelope_sha256,p_source_revision,'REVOKE');
  FOR current_row IN SELECT current_object.claim_id,current_object.synthesis_id,current_object.object_revision
    FROM public.current_anonymous_source_objects AS current_object
    WHERE current_object.anonymous_source_id=p_anonymous_source_id
      AND current_object.candidate_envelope_sha256=p_envelope_sha256
  LOOP
    INSERT INTO public.anonymous_source_lifecycle_events(anonymous_source_id,candidate_envelope_sha256,
      claim_id,synthesis_id,object_revision,event_type)
      VALUES(p_anonymous_source_id,p_envelope_sha256,current_row.claim_id,current_row.synthesis_id,
        current_row.object_revision,'REVOKE');
    IF current_row.claim_id IS NOT NULL THEN
      SELECT EXISTS(SELECT 1 FROM public.current_public_roots(current_row.claim_id,NULL) root
        WHERE root.root_source_id<>p_anonymous_source_id) INTO has_survivor;
      UPDATE public.claims AS target
        SET moderation_state=CASE WHEN has_survivor THEN 'UNDER_REVIEW' ELSE 'REVOKED' END,
          current_evaluation_id=NULL,object_revision=target.object_revision+1
        WHERE target.claim_id=current_row.claim_id
          AND (target.moderation_state IS DISTINCT FROM CASE WHEN has_survivor THEN 'UNDER_REVIEW' ELSE 'REVOKED' END
            OR target.current_evaluation_id IS NOT NULL)
        RETURNING target.object_revision INTO next_revision;
      IF FOUND THEN claim_id:=current_row.claim_id; synthesis_id:=NULL; object_revision:=next_revision; RETURN NEXT; END IF;
    ELSE
      SELECT EXISTS(SELECT 1 FROM public.current_public_roots(NULL,current_row.synthesis_id) root
        WHERE root.root_source_id<>p_anonymous_source_id) INTO has_survivor;
      UPDATE public.syntheses AS target
        SET moderation_state=CASE WHEN has_survivor THEN 'UNDER_REVIEW' ELSE 'REVOKED' END,
          current_evaluation_id=NULL,object_revision=target.object_revision+1
        WHERE target.synthesis_id=current_row.synthesis_id
          AND (target.moderation_state IS DISTINCT FROM CASE WHEN has_survivor THEN 'UNDER_REVIEW' ELSE 'REVOKED' END
            OR target.current_evaluation_id IS NOT NULL)
        RETURNING target.object_revision INTO next_revision;
      IF FOUND THEN claim_id:=NULL; synthesis_id:=current_row.synthesis_id; object_revision:=next_revision; RETURN NEXT; END IF;
    END IF;
  END LOOP;
END;
$$;

CREATE FUNCTION public.admit_anonymous_source(p_anonymous_source_id uuid,p_envelope_sha256 bytea,p_source_revision bigint)
RETURNS uuid
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,staging,public AS $$
DECLARE sealed staging.sanitized_public_candidates; admitted_claim_id uuid; admitted_revision bigint;
BEGIN
  IF uuid_extract_version(p_anonymous_source_id)<>4 OR octet_length(p_envelope_sha256)<>32 OR p_source_revision<>1 THEN
    RAISE EXCEPTION 'anonymous admission requires source revision one and exact envelope' USING ERRCODE='22023';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM ops.outbox WHERE event_type='PUBLIC_ANONYMOUS_RELEASE'
    AND anonymous_source_id=p_anonymous_source_id AND candidate_envelope_sha256=p_envelope_sha256
    AND anonymous_source_revision=p_source_revision) THEN
    RAISE EXCEPTION 'anonymous admission requires its exact private outbox authority' USING ERRCODE='42501';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'phase9-anonymous-source:'||p_anonymous_source_id::text||':'||encode(p_envelope_sha256,'hex'),0));
  IF EXISTS(SELECT 1 FROM public.anonymous_source_authority_events
    WHERE anonymous_source_id=p_anonymous_source_id AND candidate_envelope_sha256=p_envelope_sha256
      AND source_revision>p_source_revision) THEN
    RETURN NULL;
  END IF;
  IF EXISTS(SELECT 1 FROM ops.anonymous_public_revocations
    WHERE anonymous_source_id=p_anonymous_source_id AND candidate_envelope_sha256=p_envelope_sha256) THEN
    RETURN NULL;
  END IF;
  SELECT * INTO sealed FROM staging.sanitized_public_candidates
    WHERE anonymous_source_id=p_anonymous_source_id AND envelope_sha256=p_envelope_sha256;
  IF NOT FOUND THEN RAISE EXCEPTION 'anonymous admission envelope is not sealed' USING ERRCODE='23503'; END IF;
  INSERT INTO public.anonymous_source_authority_events(anonymous_source_id,candidate_envelope_sha256,
    source_revision,event_type) VALUES(p_anonymous_source_id,p_envelope_sha256,p_source_revision,'ADMIT')
    ON CONFLICT(anonymous_source_id,candidate_envelope_sha256,source_revision) DO NOTHING;
  SELECT c.claim_id INTO admitted_claim_id FROM public.claims c JOIN public.provenance_edges e USING(claim_id)
    WHERE e.source_id=p_anonymous_source_id;
  IF FOUND THEN RETURN admitted_claim_id; END IF;
  INSERT INTO public.sources(source_id,source_type,content_hash,source_license,rights_basis,
    redistribution_policy,trust_class,lineage_mode,anonymous_envelope_sha256)
  VALUES(p_anonymous_source_id,'ANONYMOUS_USER_CONTRIBUTION',encode(sealed.content_sha256,'hex'),
    'PUBLIC_SAFE','SANITIZED_PHASE9','PUBLIC_SAFE','USER_CONTRIBUTION','ANONYMOUS_RELEASE',p_envelope_sha256)
  ON CONFLICT(source_id) DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM public.sources WHERE source_id=p_anonymous_source_id
      AND anonymous_envelope_sha256=p_envelope_sha256 AND lineage_mode='ANONYMOUS_RELEASE') THEN
    RAISE EXCEPTION 'anonymous source conflicts with a different identity' USING ERRCODE='23505';
  END IF;
  INSERT INTO public.claims(content,moderation_state) VALUES(sealed.sanitized_content,'UNDER_REVIEW')
    RETURNING claim_id,object_revision INTO admitted_claim_id,admitted_revision;
  INSERT INTO public.provenance_edges(claim_id,source_id) VALUES(admitted_claim_id,p_anonymous_source_id);
  INSERT INTO public.source_closure(claim_id,root_source_id) VALUES(admitted_claim_id,p_anonymous_source_id);
  INSERT INTO public.anonymous_source_lifecycle_events(anonymous_source_id,candidate_envelope_sha256,
    claim_id,object_revision,event_type)
    VALUES(p_anonymous_source_id,p_envelope_sha256,admitted_claim_id,admitted_revision,'ADMIT');
  RETURN admitted_claim_id;
END;
$$;

ALTER FUNCTION ops.guard_outbox_identity() OWNER TO role_migration_owner;
ALTER TABLE public.anonymous_source_authority_events OWNER TO role_migration_owner;
ALTER TABLE ops.anonymous_public_revocations OWNER TO role_migration_owner;
ALTER VIEW public.eligible_objects OWNER TO role_migration_owner;
ALTER FUNCTION ops.guard_anonymous_public_revocation_fact() OWNER TO role_migration_owner;
ALTER FUNCTION ops.record_anonymous_public_revocation_fact() OWNER TO role_migration_owner;
ALTER FUNCTION public.revoke_anonymous_source(uuid,bytea,bigint) OWNER TO role_migration_owner;
ALTER FUNCTION public.admit_anonymous_source(uuid,bytea,bigint) OWNER TO role_migration_owner;
REVOKE ALL ON public.anonymous_source_authority_events FROM PUBLIC,role_gateway,role_private_worker,
  role_consolidation_worker,role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;
REVOKE ALL ON ops.anonymous_public_revocations FROM PUBLIC,role_gateway,role_private_worker,
  role_consolidation_worker,role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;
GRANT SELECT ON ops.anonymous_public_revocations TO role_gateway,role_public_worker,role_retrieval_worker,role_maintenance;
REVOKE ALL ON FUNCTION public.revoke_anonymous_source(uuid,bytea,bigint),public.admit_anonymous_source(uuid,bytea),
  public.admit_anonymous_source(uuid,bytea,bigint) FROM PUBLIC,role_public_worker;
GRANT EXECUTE ON FUNCTION public.revoke_anonymous_source(uuid,bytea,bigint),
  public.admit_anonymous_source(uuid,bytea,bigint) TO role_public_worker;
REVOKE INSERT ON public.anonymous_source_lifecycle_events FROM role_public_worker;
REVOKE USAGE ON SEQUENCE public.anonymous_source_lifecycle_events_lifecycle_seq_seq FROM role_public_worker;
REVOKE INSERT(anonymous_source_id,candidate_envelope_sha256,anonymous_source_revision) ON ops.outbox FROM role_public_worker;
GRANT INSERT(tenant_id,commit_seq,event_type,anonymous_source_id,candidate_envelope_sha256,anonymous_source_revision)
  ON ops.outbox TO role_private_worker;
REVOKE EXECUTE ON FUNCTION staging.read_sanitized_public_candidate(uuid,bytea),
  public.read_anonymous_release_binding(uuid) FROM role_public_worker;

COMMENT ON FUNCTION public.revoke_anonymous_source(uuid,bytea,bigint) IS
  'Anonymous pair-only revocation. It never accepts or returns release, tenant, contributor, or content.';
