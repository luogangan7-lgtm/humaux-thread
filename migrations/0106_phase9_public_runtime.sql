-- ADR-0009 / §12.6 / §13 / §14: immutable trust receipts and immediate revoke fence.
-- This is additive. Legacy objects without receipts are never eligible for public reads.

CREATE TABLE control.public_moderator_grants (
  user_id uuid PRIMARY KEY REFERENCES control.users(user_id),
  grant_version bigint NOT NULL CHECK (grant_version > 0),
  enabled boolean NOT NULL DEFAULT false
);
-- Provisioning this global privilege is an explicit admin operation, not tenant self-service.
REVOKE ALL ON control.public_moderator_grants FROM role_gateway, role_private_worker,
  role_consolidation_worker, role_public_worker, role_retrieval_worker, role_batch_issuer,
  role_maintenance;
GRANT SELECT ON control.public_moderator_grants TO role_public_worker, role_maintenance;
ALTER TABLE control.public_moderator_grants OWNER TO role_migration_owner;

CREATE TABLE ops.public_release_revocations (
  release_id uuid PRIMARY KEY REFERENCES staging.contribution_releases(contribution_release_id),
  revoke_event_id uuid NOT NULL UNIQUE REFERENCES ops.outbox(outbox_id)
);
REVOKE ALL ON ops.public_release_revocations FROM role_gateway, role_private_worker,
  role_consolidation_worker, role_public_worker, role_retrieval_worker, role_batch_issuer,
  role_maintenance;
GRANT INSERT ON ops.public_release_revocations TO role_private_worker;
GRANT SELECT ON ops.public_release_revocations TO role_gateway, role_public_worker,
  role_retrieval_worker, role_maintenance;
ALTER TABLE ops.public_release_revocations OWNER TO role_migration_owner;

CREATE FUNCTION ops.guard_public_revocation_fact()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path = pg_catalog AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM staging.contribution_releases r JOIN ops.outbox o
      ON o.contribution_release_id=r.contribution_release_id AND o.tenant_id=r.tenant_id
    WHERE r.contribution_release_id=NEW.release_id AND r.state='REVOKED'
      AND o.outbox_id=NEW.revoke_event_id AND o.event_type='PUBLIC_REVOKE'
      AND r.tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid
  ) THEN
    RAISE EXCEPTION 'revocation fact needs the visible committed transition'
      USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER public_revocation_fact_guard BEFORE INSERT ON ops.public_release_revocations
  FOR EACH ROW EXECUTE FUNCTION ops.guard_public_revocation_fact();

CREATE FUNCTION ops.record_public_revocation_fact()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path = pg_catalog AS $$
BEGIN
  IF NEW.event_type='PUBLIC_REVOKE' THEN
    INSERT INTO ops.public_release_revocations(release_id,revoke_event_id)
      VALUES(NEW.contribution_release_id,NEW.outbox_id);
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER public_revocation_fact AFTER INSERT ON ops.outbox
  FOR EACH ROW EXECUTE FUNCTION ops.record_public_revocation_fact();

-- Only verified, versioned source identities enter support clustering. NULL is unknown.
ALTER TABLE public.sources
  ADD COLUMN verified_organization text,
  ADD COLUMN canonical_url text,
  ADD COLUMN document_fingerprint text,
  ADD COLUMN upstream_root text,
  ADD COLUMN identity_policy_version text,
  ADD COLUMN trusted_by_policy boolean;
CREATE UNIQUE INDEX public_source_release_unique ON public.sources(contribution_release_id)
  WHERE contribution_release_id IS NOT NULL;
ALTER TABLE public.claims
  ADD COLUMN current_evaluation_id uuid,
  ADD COLUMN object_revision bigint NOT NULL DEFAULT 1 CHECK(object_revision > 0),
  ADD COLUMN intake_release_id uuid REFERENCES staging.contribution_releases(contribution_release_id),
  ADD COLUMN supersedes_claim_id uuid REFERENCES public.claims(claim_id);
CREATE UNIQUE INDEX public_claim_intake_release_unique ON public.claims(intake_release_id)
  WHERE intake_release_id IS NOT NULL;
ALTER TABLE public.syntheses
  ADD COLUMN moderation_state text NOT NULL DEFAULT 'UNDER_REVIEW'
    CHECK(moderation_state IN ('PUBLIC_STAGING','UNDER_REVIEW','SUPPORTED','QUARANTINED','REVOKED')),
  ADD COLUMN current_evaluation_id uuid,
  ADD COLUMN object_revision bigint NOT NULL DEFAULT 1 CHECK(object_revision > 0),
  ADD COLUMN supersedes_synthesis_id uuid REFERENCES public.syntheses(synthesis_id);

CREATE TABLE public.claim_trust_evaluations (
  evaluation_id uuid PRIMARY KEY DEFAULT uuidv7(),
  claim_id uuid REFERENCES public.claims(claim_id),
  synthesis_id uuid REFERENCES public.syntheses(synthesis_id),
  object_revision bigint NOT NULL CHECK(object_revision > 0),
  body_sha256 bytea NOT NULL CHECK(octet_length(body_sha256)=32),
  policy_version text NOT NULL CHECK(btrim(policy_version)<>''),
  moderation_state text NOT NULL CHECK(moderation_state IN ('UNDER_REVIEW','SUPPORTED','QUARANTINED')),
  evaluator_user_id uuid REFERENCES control.users(user_id),
  evaluator_grant_version bigint,
  rationale text NOT NULL CHECK(btrim(rationale)<>''),
  support_count integer NOT NULL CHECK(support_count>0),
  independent_support_count integer NOT NULL CHECK(independent_support_count>0 AND independent_support_count<=support_count),
  trusted_source_count integer CHECK(trusted_source_count>=0 AND trusted_source_count<=support_count),
  identity_incomplete boolean NOT NULL,
  contradiction_count integer NOT NULL CHECK(contradiction_count>=0),
  checks_complete boolean NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK((claim_id IS NULL)<>(synthesis_id IS NULL)),
  CHECK(moderation_state<>'SUPPORTED' OR
    (checks_complete AND evaluator_user_id IS NOT NULL AND evaluator_grant_version IS NOT NULL))
);
ALTER TABLE public.claims ADD CONSTRAINT claim_current_evaluation_fk
  FOREIGN KEY(current_evaluation_id) REFERENCES public.claim_trust_evaluations(evaluation_id);
ALTER TABLE public.syntheses ADD CONSTRAINT synthesis_current_evaluation_fk
  FOREIGN KEY(current_evaluation_id) REFERENCES public.claim_trust_evaluations(evaluation_id);

CREATE TABLE public.claim_trust_evaluation_sources (
  evaluation_id uuid NOT NULL REFERENCES public.claim_trust_evaluations(evaluation_id),
  root_source_id uuid NOT NULL REFERENCES public.sources(source_id),
  source_content_hash text NOT NULL CHECK(btrim(source_content_hash)<>''),
  contribution_release_id uuid REFERENCES staging.contribution_releases(contribution_release_id),
  PRIMARY KEY(evaluation_id,root_source_id)
);
CREATE TABLE public.poisoning_signals (
  evaluation_id uuid NOT NULL REFERENCES public.claim_trust_evaluations(evaluation_id),
  signal_code text NOT NULL CHECK(btrim(signal_code)<>''),
  check_version text NOT NULL CHECK(btrim(check_version)<>''),
  detected boolean NOT NULL,
  PRIMARY KEY(evaluation_id,signal_code)
);

CREATE FUNCTION public.require_trust_root_seal()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE expected_count integer; actual_count bigint;
BEGIN
  SELECT support_count INTO expected_count FROM public.claim_trust_evaluations
    WHERE evaluation_id=NEW.evaluation_id;
  SELECT count(*) INTO actual_count FROM public.claim_trust_evaluation_sources
    WHERE evaluation_id=NEW.evaluation_id;
  IF expected_count IS NULL OR actual_count<>expected_count THEN
    RAISE EXCEPTION 'trust roots must be sealed in the evaluation transaction' USING ERRCODE='23514';
  END IF;
  RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER trust_roots_complete AFTER INSERT ON public.claim_trust_evaluations
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.require_trust_root_seal();
CREATE CONSTRAINT TRIGGER trust_roots_sealed AFTER INSERT ON public.claim_trust_evaluation_sources
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.require_trust_root_seal();
REVOKE UPDATE ON public.claim_trust_evaluations,public.claim_trust_evaluation_sources,
  public.poisoning_signals FROM role_public_worker;
REVOKE DELETE ON public.claim_trust_evaluations,public.claim_trust_evaluation_sources,
  public.poisoning_signals FROM role_gateway,role_private_worker,role_consolidation_worker,
  role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;
ALTER TABLE public.claim_trust_evaluations OWNER TO role_migration_owner;
ALTER TABLE public.claim_trust_evaluation_sources OWNER TO role_migration_owner;
ALTER TABLE public.poisoning_signals OWNER TO role_migration_owner;

-- A cycle OR a reachable unrooted branch invalidates the complete root set. UNION of only
-- successful branches would hide unsupported prose. The direct graph remains authority.
CREATE FUNCTION public.current_public_roots(target_claim uuid,target_synthesis uuid)
RETURNS TABLE(root_source_id uuid) LANGUAGE sql STABLE SECURITY INVOKER
SET search_path=pg_catalog AS $$
  WITH RECURSIVE nodes(kind,id,path,cycle) AS (
    SELECT CASE WHEN target_claim IS NOT NULL THEN 'C' ELSE 'S' END,
      COALESCE(target_claim,target_synthesis),
      ARRAY[COALESCE(target_claim,target_synthesis)],false
      WHERE (target_claim IS NULL)<>(target_synthesis IS NULL)
    UNION ALL
    SELECT CASE WHEN i.claim_id IS NOT NULL THEN 'C' ELSE 'S' END,
      COALESCE(i.claim_id,i.input_synthesis_id),
      n.path||COALESCE(i.claim_id,i.input_synthesis_id),
      COALESCE(i.claim_id,i.input_synthesis_id)=ANY(n.path)
    FROM nodes n JOIN public.synthesis_inputs i ON n.kind='S' AND i.synthesis_id=n.id
    WHERE NOT n.cycle
  )
  SELECT DISTINCT p.source_id FROM nodes n JOIN public.provenance_edges p
    ON n.kind='C' AND n.id=p.claim_id
  WHERE NOT EXISTS (SELECT 1 FROM nodes x WHERE x.cycle OR
    (x.kind='C' AND NOT EXISTS(SELECT 1 FROM public.provenance_edges e WHERE e.claim_id=x.id)) OR
    (x.kind='S' AND NOT EXISTS(SELECT 1 FROM public.synthesis_inputs i WHERE i.synthesis_id=x.id)))
$$;

CREATE FUNCTION public.public_receipt_matches(target_claim uuid,target_synthesis uuid,
  receipt uuid,revision bigint,body jsonb)
RETURNS boolean LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
  SELECT EXISTS (
    SELECT 1 FROM public.claim_trust_evaluations e
    WHERE e.evaluation_id=receipt AND e.claim_id IS NOT DISTINCT FROM target_claim
      AND e.synthesis_id IS NOT DISTINCT FROM target_synthesis
      AND e.object_revision=revision AND e.body_sha256=sha256(convert_to(body::text,'UTF8'))
      AND EXISTS(SELECT 1 FROM public.claim_trust_evaluation_sources r WHERE r.evaluation_id=e.evaluation_id)
      AND e.support_count=(SELECT count(*) FROM public.claim_trust_evaluation_sources r WHERE r.evaluation_id=e.evaluation_id)
      AND NOT EXISTS (
        SELECT r.root_source_id FROM public.claim_trust_evaluation_sources r WHERE r.evaluation_id=e.evaluation_id
        EXCEPT SELECT g.root_source_id FROM public.current_public_roots(target_claim,target_synthesis) g)
      AND NOT EXISTS (
        SELECT g.root_source_id FROM public.current_public_roots(target_claim,target_synthesis) g
        EXCEPT SELECT r.root_source_id FROM public.claim_trust_evaluation_sources r WHERE r.evaluation_id=e.evaluation_id)
      AND NOT EXISTS (
        SELECT r.root_source_id FROM public.claim_trust_evaluation_sources r WHERE r.evaluation_id=e.evaluation_id
        EXCEPT SELECT c.root_source_id FROM public.source_closure c WHERE c.is_current
          AND c.claim_id IS NOT DISTINCT FROM target_claim AND c.synthesis_id IS NOT DISTINCT FROM target_synthesis)
      AND NOT EXISTS (
        SELECT c.root_source_id FROM public.source_closure c WHERE c.is_current
          AND c.claim_id IS NOT DISTINCT FROM target_claim AND c.synthesis_id IS NOT DISTINCT FROM target_synthesis
        EXCEPT SELECT r.root_source_id FROM public.claim_trust_evaluation_sources r WHERE r.evaluation_id=e.evaluation_id)
      AND NOT EXISTS(SELECT 1 FROM public.claim_trust_evaluation_sources r JOIN public.sources s
        ON s.source_id=r.root_source_id WHERE r.evaluation_id=e.evaluation_id AND
        (r.source_content_hash IS DISTINCT FROM s.content_hash OR
         r.contribution_release_id IS DISTINCT FROM s.contribution_release_id))
      AND NOT EXISTS(SELECT 1 FROM public.claim_trust_evaluation_sources r
        JOIN ops.public_release_revocations v ON v.release_id=r.contribution_release_id
        WHERE r.evaluation_id=e.evaluation_id)
  )
$$;

CREATE VIEW public.eligible_objects WITH (security_invoker=true) AS
  SELECT c.claim_id AS object_id,'CLAIM'::text AS object_kind,c.content,c.object_revision,
    c.current_evaluation_id
  FROM public.claims c JOIN public.claim_trust_evaluations e ON e.evaluation_id=c.current_evaluation_id
  WHERE c.moderation_state='SUPPORTED' AND e.moderation_state='SUPPORTED'
    AND public.public_receipt_matches(c.claim_id,NULL,c.current_evaluation_id,c.object_revision,c.content)
  UNION ALL
  SELECT s.synthesis_id,'SYNTHESIS',s.content,s.object_revision,s.current_evaluation_id
  FROM public.syntheses s JOIN public.claim_trust_evaluations e ON e.evaluation_id=s.current_evaluation_id
  WHERE s.moderation_state='SUPPORTED' AND e.moderation_state='SUPPORTED'
    AND public.public_receipt_matches(NULL,s.synthesis_id,s.current_evaluation_id,s.object_revision,s.content);
ALTER VIEW public.eligible_objects OWNER TO role_migration_owner;

CREATE FUNCTION public.guard_public_body()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE cid uuid; sid uuid;
BEGIN
  IF TG_TABLE_NAME='claims' THEN cid:=NEW.claim_id; ELSE sid:=NEW.synthesis_id; END IF;
  IF TG_OP='UPDATE' THEN
    IF NEW.content IS DISTINCT FROM OLD.content THEN
      RAISE EXCEPTION 'public bodies are immutable; create a replacement object' USING ERRCODE='23514';
    END IF;
    IF ROW(NEW.current_evaluation_id,NEW.moderation_state) IS DISTINCT FROM
       ROW(OLD.current_evaluation_id,OLD.moderation_state) THEN
      IF NEW.object_revision<>OLD.object_revision+1 THEN
        RAISE EXCEPTION 'public decision requires next revision' USING ERRCODE='23514';
      END IF;
    ELSIF NEW.object_revision<>OLD.object_revision THEN
      RAISE EXCEPTION 'public revision requires a decision change' USING ERRCODE='23514';
    END IF;
  END IF;
  IF NEW.moderation_state='SUPPORTED' THEN
    IF NOT public.public_receipt_matches(cid,sid,NEW.current_evaluation_id,NEW.object_revision,NEW.content)
      OR NOT EXISTS(SELECT 1 FROM public.claim_trust_evaluations e
        WHERE e.evaluation_id=NEW.current_evaluation_id AND e.moderation_state='SUPPORTED') THEN
      RAISE EXCEPTION 'supported object needs a current complete receipt' USING ERRCODE='23514';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER claim_body_guard BEFORE INSERT OR UPDATE ON public.claims
  FOR EACH ROW EXECUTE FUNCTION public.guard_public_body();
CREATE TRIGGER synthesis_body_guard BEFORE INSERT OR UPDATE ON public.syntheses
  FOR EACH ROW EXECUTE FUNCTION public.guard_public_body();

CREATE FUNCTION public.guard_trust_evaluation()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
  IF NEW.moderation_state='SUPPORTED' AND NOT EXISTS(
    SELECT 1 FROM control.public_moderator_grants g JOIN control.users u USING(user_id)
    JOIN control.memberships m ON m.user_id=u.user_id
    WHERE g.user_id=NEW.evaluator_user_id AND g.grant_version=NEW.evaluator_grant_version
      AND g.enabled AND u.state='ACTIVE' AND m.state='ACTIVE'
      AND m.tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid
      AND u.user_id=NULLIF(current_setting('humaux.user_id',true),'')::uuid
  ) THEN
    RAISE EXCEPTION 'supported evaluation requires active global moderator grant'
      USING ERRCODE='42501';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER trust_evaluation_guard BEFORE INSERT ON public.claim_trust_evaluations
  FOR EACH ROW EXECUTE FUNCTION public.guard_trust_evaluation();

ALTER TABLE ops.outbox
  ADD COLUMN public_claim_id uuid REFERENCES public.claims(claim_id),
  ADD COLUMN public_synthesis_id uuid REFERENCES public.syntheses(synthesis_id),
  ADD COLUMN object_revision bigint,
  DROP CONSTRAINT outbox_row_class,
  ADD CONSTRAINT outbox_row_class CHECK (
    (event_type='PUBLIC_OBJECT_CHANGED' AND contribution_release_id IS NULL
      AND evidence_id IS NULL AND stream_seq IS NULL
      AND (public_claim_id IS NULL)<>(public_synthesis_id IS NULL)
      AND object_revision IS NOT NULL AND object_revision>0)
    OR (event_type IN ('PUBLIC_RELEASE','PUBLIC_REVOKE') AND contribution_release_id IS NOT NULL
      AND evidence_id IS NULL AND stream_seq IS NULL AND public_claim_id IS NULL
      AND public_synthesis_id IS NULL AND object_revision IS NULL)
    OR (event_type NOT IN ('PUBLIC_RELEASE','PUBLIC_REVOKE','PUBLIC_OBJECT_CHANGED')
      AND evidence_id IS NOT NULL AND stream_seq IS NOT NULL AND contribution_release_id IS NULL
      AND public_claim_id IS NULL AND public_synthesis_id IS NULL AND object_revision IS NULL)
  );
CREATE UNIQUE INDEX outbox_public_claim_revision_unique ON ops.outbox(public_claim_id,object_revision)
  WHERE public_claim_id IS NOT NULL;
CREATE UNIQUE INDEX outbox_public_synthesis_revision_unique ON ops.outbox(public_synthesis_id,object_revision)
  WHERE public_synthesis_id IS NOT NULL;
GRANT INSERT(tenant_id,commit_seq,event_type,public_claim_id,public_synthesis_id,object_revision)
  ON ops.outbox TO role_public_worker;
GRANT USAGE ON SEQUENCE ops.commit_seq_seq TO role_public_worker;

CREATE OR REPLACE FUNCTION ops.guard_outbox_identity()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE release_state text; actual_revision bigint;
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

-- Publication cannot omit the durable event even when a caller uses direct SQL.
CREATE FUNCTION public.require_object_outbox()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE cid uuid; sid uuid;
BEGIN
  IF TG_OP='INSERT' AND NEW.moderation_state IN ('PUBLIC_STAGING','UNDER_REVIEW') THEN RETURN NULL; END IF;
  IF TG_OP='UPDATE' AND ROW(NEW.current_evaluation_id,NEW.moderation_state) IS NOT DISTINCT FROM
    ROW(OLD.current_evaluation_id,OLD.moderation_state) THEN RETURN NULL; END IF;
  IF TG_TABLE_NAME='claims' THEN cid:=NEW.claim_id; ELSE sid:=NEW.synthesis_id; END IF;
  IF NOT EXISTS(SELECT 1 FROM ops.outbox o WHERE o.event_type='PUBLIC_OBJECT_CHANGED'
    AND o.public_claim_id IS NOT DISTINCT FROM cid AND o.public_synthesis_id IS NOT DISTINCT FROM sid
    AND o.object_revision=NEW.object_revision) THEN
    RAISE EXCEPTION 'public decision must commit its outbox event' USING ERRCODE='23514';
  END IF;
  RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER claim_requires_outbox AFTER INSERT OR UPDATE ON public.claims
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.require_object_outbox();
CREATE CONSTRAINT TRIGGER synthesis_requires_outbox AFTER INSERT OR UPDATE ON public.syntheses
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.require_object_outbox();

ALTER TABLE ops.jobs
  ADD COLUMN outbox_event_id uuid REFERENCES ops.outbox(outbox_id),
  ADD COLUMN consumer text,
  ADD CONSTRAINT jobs_outbox_consumer_pair CHECK((outbox_event_id IS NULL)=(consumer IS NULL));
CREATE UNIQUE INDEX jobs_outbox_consumer_unique ON ops.jobs(outbox_event_id,consumer)
  WHERE outbox_event_id IS NOT NULL;

-- Identity metadata may be filled before evaluation but cannot change an old receipt.
CREATE FUNCTION public.guard_evaluated_source_identity()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
  IF ROW(NEW.verified_organization,NEW.canonical_url,NEW.document_fingerprint,NEW.upstream_root,
    NEW.identity_policy_version,NEW.trusted_by_policy) IS DISTINCT FROM
     ROW(OLD.verified_organization,OLD.canonical_url,OLD.document_fingerprint,OLD.upstream_root,
    OLD.identity_policy_version,OLD.trusted_by_policy)
    AND EXISTS(SELECT 1 FROM public.claim_trust_evaluation_sources WHERE root_source_id=OLD.source_id) THEN
    RAISE EXCEPTION 'evaluated source identity is immutable' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER evaluated_source_identity BEFORE UPDATE ON public.sources
  FOR EACH ROW EXECUTE FUNCTION public.guard_evaluated_source_identity();

DO $$ DECLARE f text; BEGIN
  FOREACH f IN ARRAY ARRAY[
    'ops.guard_public_revocation_fact()','ops.record_public_revocation_fact()',
    'public.current_public_roots(uuid,uuid)',
    'public.public_receipt_matches(uuid,uuid,uuid,bigint,jsonb)',
    'public.guard_public_body()','public.guard_trust_evaluation()',
    'public.require_object_outbox()','public.guard_evaluated_source_identity()',
    'public.require_trust_root_seal()']
  LOOP
    EXECUTE 'ALTER FUNCTION '||f||' OWNER TO role_migration_owner';
    EXECUTE 'REVOKE ALL ON FUNCTION '||f||' FROM PUBLIC';
  END LOOP;
END $$;
GRANT EXECUTE ON FUNCTION public.current_public_roots(uuid,uuid),
  public.public_receipt_matches(uuid,uuid,uuid,bigint,jsonb)
  TO role_gateway,role_public_worker,role_retrieval_worker,role_maintenance;
