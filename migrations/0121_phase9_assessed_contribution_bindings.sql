-- Phase 9 follow-up: persist the two USER_REASONING bindings privately and expose only
-- narrow, bounded read contracts across the private/public boundary.  This is additive while
-- 0120's legacy public-source identity columns remain in their approved expand window.

CREATE TABLE staging.contribution_candidate_phase9_assessments (
  candidate_id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  probe_bytes bytea NOT NULL CHECK(octet_length(probe_bytes) BETWEEN 1 AND 4096),
  probe_sha256 bytea NOT NULL CHECK(octet_length(probe_sha256)=32 AND probe_sha256=sha256(probe_bytes)),
  probe_source_manifest_hash bytea NOT NULL CHECK(octet_length(probe_source_manifest_hash)=32),
  probe_provider_trace text NOT NULL CHECK(btrim(probe_provider_trace)<>''),
  probe_scan_receipt jsonb NOT NULL CHECK(jsonb_typeof(probe_scan_receipt)='object'),
  coverage_digest_id uuid NOT NULL,
  coverage_version integer NOT NULL CHECK(coverage_version>0),
  coverage_digest_sha256 bytea NOT NULL CHECK(octet_length(coverage_digest_sha256)=32),
  candidate_payload_sha256 bytea NOT NULL CHECK(octet_length(candidate_payload_sha256)=32),
  assessment_digest bytea NOT NULL CHECK(octet_length(assessment_digest)=32),
  assessment_provider_trace text NOT NULL CHECK(btrim(assessment_provider_trace)<>''),
  assessment_receipt jsonb NOT NULL CHECK(
    jsonb_typeof(assessment_receipt)='object'
    AND assessment_receipt - ARRAY['probe_sha256','coverage_digest_id','coverage_version',
      'coverage_digest_sha256','candidate_payload_sha256','novelty','quality','generality','grounding'] = '{}'::jsonb
    AND assessment_receipt @> '{"novelty":"PASS","quality":"PASS","generality":"PASS","grounding":"PASS"}'::jsonb
    AND assessment_digest=sha256(convert_to(assessment_receipt::text,'UTF8'))),
  created_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY(tenant_id,candidate_id)
    REFERENCES staging.contribution_candidates(tenant_id,candidate_id)
);

CREATE FUNCTION staging.guard_phase9_assessment_binding()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog,staging AS $$
DECLARE candidate_hash bytea; manifest_hash bytea;
BEGIN
  SELECT disclosed_payload_sha256,source_manifest_hash INTO candidate_hash,manifest_hash
    FROM staging.contribution_candidates
    WHERE tenant_id=NEW.tenant_id AND candidate_id=NEW.candidate_id;
  IF NOT FOUND OR NEW.candidate_payload_sha256 IS DISTINCT FROM candidate_hash
    OR NEW.probe_source_manifest_hash IS DISTINCT FROM manifest_hash
    OR NEW.assessment_receipt->>'probe_sha256' IS DISTINCT FROM encode(NEW.probe_sha256,'hex')
    OR NEW.assessment_receipt->>'coverage_digest_id' IS DISTINCT FROM NEW.coverage_digest_id::text
    OR NEW.assessment_receipt->>'coverage_version' IS DISTINCT FROM NEW.coverage_version::text
    OR NEW.assessment_receipt->>'coverage_digest_sha256' IS DISTINCT FROM encode(NEW.coverage_digest_sha256,'hex')
    OR NEW.assessment_receipt->>'candidate_payload_sha256' IS DISTINCT FROM encode(NEW.candidate_payload_sha256,'hex') THEN
    RAISE EXCEPTION 'phase9 assessment receipt must bind the stored candidate, probe and coverage exactly'
      USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER phase9_assessment_binding_guard
  BEFORE INSERT OR UPDATE ON staging.contribution_candidate_phase9_assessments
  FOR EACH ROW EXECUTE FUNCTION staging.guard_phase9_assessment_binding();

ALTER TABLE staging.contribution_candidate_phase9_assessments ENABLE ROW LEVEL SECURITY;
ALTER TABLE staging.contribution_candidate_phase9_assessments FORCE ROW LEVEL SECURITY;
CREATE POLICY contribution_candidate_phase9_assessments_owner
  ON staging.contribution_candidate_phase9_assessments
  USING(tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid
    AND EXISTS(SELECT 1 FROM staging.contribution_candidates c
      WHERE c.candidate_id=contribution_candidate_phase9_assessments.candidate_id
        AND c.user_id=NULLIF(current_setting('humaux.user_id',true),'')::uuid))
  WITH CHECK(tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid
    AND EXISTS(SELECT 1 FROM staging.contribution_candidates c
      WHERE c.candidate_id=contribution_candidate_phase9_assessments.candidate_id
        AND c.user_id=NULLIF(current_setting('humaux.user_id',true),'')::uuid));

-- FORCE RLS means a SECURITY DEFINER function is still subject to the table owner's policy.
-- This policy is deliberately owner-only; public workers keep no table SELECT privilege.
CREATE POLICY sanitized_public_candidates_function_owner_read
  ON staging.sanitized_public_candidates FOR SELECT TO role_migration_owner USING(true);

CREATE FUNCTION staging.read_sanitized_public_candidate(
  p_anonymous_source_id uuid,
  p_envelope_sha256 bytea
)
RETURNS TABLE(
  anonymous_source_id uuid,
  sanitized_content jsonb,
  content_sha256 bytea,
  policy_version text,
  policy_digest bytea,
  assessment_outcome text,
  assessment_digest bytea,
  envelope_sha256 bytea
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,staging AS $$
  SELECT s.anonymous_source_id,s.sanitized_content,s.content_sha256,s.policy_version,
    s.policy_digest,s.assessment_outcome,s.assessment_digest,s.envelope_sha256
  FROM staging.sanitized_public_candidates s
  WHERE s.anonymous_source_id=p_anonymous_source_id
    AND s.envelope_sha256=p_envelope_sha256
    AND octet_length(p_envelope_sha256)=32
$$;

CREATE FUNCTION public.read_anonymous_release_binding(p_release_id uuid)
RETURNS TABLE(
  anonymous_source_id uuid,
  envelope_sha256 bytea
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,control,staging AS $$
  SELECT lineage.anonymous_source_id,candidate.envelope_sha256
  FROM control.anonymous_source_lineage lineage
  JOIN staging.sanitized_public_candidates candidate
    ON candidate.tenant_id=lineage.tenant_id
   AND candidate.anonymous_source_id=lineage.anonymous_source_id
  WHERE lineage.contribution_release_id=p_release_id
$$;

-- Anonymous public sources can originate only from one sealed staging envelope.  The public
-- worker retains legacy source insertion during the expand window, but its direct anonymous
-- branch is rejected by the trigger below; this function is the only public-worker path.
CREATE FUNCTION public.admit_anonymous_source(p_anonymous_source_id uuid,p_envelope_sha256 bytea)
RETURNS uuid
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,staging,public AS $$
DECLARE sealed staging.sanitized_public_candidates; admitted_claim_id uuid; admitted_revision bigint;
BEGIN
  IF uuid_extract_version(p_anonymous_source_id)<>4 OR octet_length(p_envelope_sha256)<>32 THEN
    RAISE EXCEPTION 'anonymous admission requires one random source and exact envelope' USING ERRCODE='22023';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'phase9-anonymous-admit:'||p_anonymous_source_id::text||':'||encode(p_envelope_sha256,'hex'),0));
  SELECT * INTO sealed FROM staging.sanitized_public_candidates
    WHERE anonymous_source_id=p_anonymous_source_id AND envelope_sha256=p_envelope_sha256;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'anonymous admission envelope is not sealed' USING ERRCODE='23503';
  END IF;
  INSERT INTO public.sources(source_id,source_type,content_hash,source_license,rights_basis,
    redistribution_policy,trust_class,lineage_mode,anonymous_envelope_sha256)
  VALUES(p_anonymous_source_id,'ANONYMOUS_USER_CONTRIBUTION',encode(sealed.content_sha256,'hex'),
    'PUBLIC_SAFE','SANITIZED_PHASE9','PUBLIC_SAFE','USER_CONTRIBUTION','ANONYMOUS_RELEASE',p_envelope_sha256)
  ON CONFLICT(source_id) DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM public.sources WHERE source_id=p_anonymous_source_id
      AND anonymous_envelope_sha256=p_envelope_sha256 AND lineage_mode='ANONYMOUS_RELEASE') THEN
    RAISE EXCEPTION 'anonymous source conflicts with a different identity' USING ERRCODE='23505';
  END IF;
  SELECT claim_id INTO admitted_claim_id FROM public.claims c
    JOIN public.provenance_edges e USING(claim_id)
    WHERE e.source_id=p_anonymous_source_id AND c.content=sealed.sanitized_content;
  IF NOT FOUND THEN
    INSERT INTO public.claims(content,moderation_state) VALUES(sealed.sanitized_content,'PUBLIC_STAGING')
      RETURNING claim_id INTO admitted_claim_id;
    INSERT INTO public.provenance_edges(claim_id,source_id) VALUES(admitted_claim_id,p_anonymous_source_id);
    INSERT INTO public.source_closure(claim_id,root_source_id) VALUES(admitted_claim_id,p_anonymous_source_id);
    UPDATE public.claims
      SET moderation_state='UNDER_REVIEW',
          object_revision=object_revision+1
      WHERE claim_id=admitted_claim_id
      RETURNING object_revision INTO admitted_revision;
    INSERT INTO public.anonymous_source_lifecycle_events(anonymous_source_id,candidate_envelope_sha256,
      claim_id,object_revision,event_type)
      VALUES(p_anonymous_source_id,p_envelope_sha256,admitted_claim_id,admitted_revision,'ADMIT');
  END IF;
  RETURN admitted_claim_id;
END;
$$;

CREATE FUNCTION public.guard_anonymous_source_admission()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
  IF NEW.lineage_mode='ANONYMOUS_RELEASE' AND current_user<>'role_migration_owner' THEN
    RAISE EXCEPTION 'anonymous public source insertion requires sealed admission function' USING ERRCODE='42501';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER anonymous_source_admission_guard
  BEFORE INSERT ON public.sources FOR EACH ROW EXECUTE FUNCTION public.guard_anonymous_source_admission();

CREATE FUNCTION public.guard_anonymous_lifecycle_admission()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
  IF NEW.event_type='ADMIT' AND current_user<>'role_migration_owner' THEN
    RAISE EXCEPTION 'anonymous ADMIT requires sealed admission function' USING ERRCODE='42501';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER anonymous_lifecycle_admission_guard
  BEFORE INSERT ON public.anonymous_source_lifecycle_events FOR EACH ROW
  EXECUTE FUNCTION public.guard_anonymous_lifecycle_admission();

-- Private USER_REASONING never receives generic SELECT on public tables.  The function takes
-- only a scanned public-safe probe and returns at most 32 de-identified current public objects.
CREATE FUNCTION public.phase9_public_coverage_for_probe(p_probe bytea,p_limit integer)
RETURNS TABLE(snapshot_id uuid,coverage_version integer,summary text)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,public AS $$
  WITH query AS (
    SELECT plainto_tsquery('simple',convert_from(p_probe,'UTF8')) AS terms
    WHERE octet_length(p_probe) BETWEEN 1 AND 4096 AND p_limit BETWEEN 1 AND 32
  ), selected AS (
    SELECT e.object_id,e.object_revision,COALESCE(lifecycle.lifecycle_seq,0) AS lifecycle_seq,
      e.content::text AS summary,
      ts_rank_cd(to_tsvector('simple',e.content::text),query.terms) AS relevance
    FROM public.eligible_objects e
    LEFT JOIN LATERAL (
      SELECT max(o.lifecycle_seq) AS lifecycle_seq FROM public.current_anonymous_source_objects o
      WHERE COALESCE(o.claim_id,o.synthesis_id)=e.object_id AND o.object_revision=e.object_revision
    ) lifecycle ON true
    CROSS JOIN query
    WHERE to_tsvector('simple',e.content::text) @@ query.terms
    ORDER BY relevance DESC,lifecycle.lifecycle_seq DESC,e.object_id
    LIMIT p_limit
  ), snapshot AS (
    SELECT encode(sha256(convert_to('phase9-coverage-v1:'||COALESCE(string_agg(
      object_id::text||':'||object_revision::text||':'||lifecycle_seq::text||':'||
      encode(sha256(convert_to(summary,'UTF8')),'hex'),'|' ORDER BY relevance DESC,lifecycle_seq DESC,object_id),
      'empty'),'UTF8')),'hex') AS fingerprint
    FROM selected
  )
  SELECT (substr(snapshot.fingerprint,1,8)||'-'||substr(snapshot.fingerprint,9,4)||'-'||
      substr(snapshot.fingerprint,13,4)||'-'||substr(snapshot.fingerprint,17,4)||'-'||
      substr(snapshot.fingerprint,21,12))::uuid,1,left(selected.summary,256)
  FROM snapshot LEFT JOIN selected ON true
$$;

ALTER TABLE staging.contribution_candidate_phase9_assessments OWNER TO role_migration_owner;
ALTER FUNCTION staging.read_sanitized_public_candidate(uuid,bytea) OWNER TO role_migration_owner;
ALTER FUNCTION public.read_anonymous_release_binding(uuid) OWNER TO role_migration_owner;
ALTER FUNCTION public.admit_anonymous_source(uuid,bytea) OWNER TO role_migration_owner;
ALTER FUNCTION public.phase9_public_coverage_for_probe(bytea,integer) OWNER TO role_migration_owner;
ALTER FUNCTION staging.guard_phase9_assessment_binding() OWNER TO role_migration_owner;
ALTER FUNCTION public.guard_anonymous_source_admission() OWNER TO role_migration_owner;
ALTER FUNCTION public.guard_anonymous_lifecycle_admission() OWNER TO role_migration_owner;
REVOKE ALL ON staging.contribution_candidate_phase9_assessments FROM PUBLIC,role_gateway,
  role_private_worker,role_consolidation_worker,role_public_worker,role_retrieval_worker,
  role_batch_issuer,role_maintenance;
GRANT SELECT,INSERT ON staging.contribution_candidate_phase9_assessments TO role_private_worker;
REVOKE ALL ON FUNCTION staging.read_sanitized_public_candidate(uuid,bytea) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION staging.read_sanitized_public_candidate(uuid,bytea) TO role_public_worker;
REVOKE ALL ON FUNCTION public.read_anonymous_release_binding(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.read_anonymous_release_binding(uuid) TO role_public_worker;
REVOKE ALL ON FUNCTION public.admit_anonymous_source(uuid,bytea) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.admit_anonymous_source(uuid,bytea) TO role_public_worker;
REVOKE ALL ON FUNCTION public.phase9_public_coverage_for_probe(bytea,integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.phase9_public_coverage_for_probe(bytea,integer) TO role_private_worker;
REVOKE SELECT ON public.current_anonymous_source_objects,public.claims,public.syntheses
  FROM role_private_worker;
REVOKE SELECT(candidate_envelope_id,tenant_id,anonymous_source_id,sanitized_content,content_sha256,
  policy_version,policy_digest,assessment_outcome,assessment_digest,envelope_sha256,created_at)
  ON staging.sanitized_public_candidates FROM role_public_worker;

COMMENT ON FUNCTION staging.read_sanitized_public_candidate(uuid,bytea) IS
  'Exact anonymous-source plus envelope lookup. No tenant or candidate id is returned.';
COMMENT ON FUNCTION public.read_anonymous_release_binding(uuid) IS
  'Maps one release id to its sealed random anonymous source and envelope without exposing table access.';
COMMENT ON FUNCTION public.phase9_public_coverage_for_probe(bytea,integer) IS
  'Bounded public-safe coverage only; no contributor, release, tenant, or private lineage fields.';
