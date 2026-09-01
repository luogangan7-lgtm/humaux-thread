-- Phase 9 protected anti-Sybil boundary.  Contributor identity remains behind the
-- anonymous-source bridge; the public plane receives only one immutable aggregate receipt.

CREATE TABLE public.claim_independence_attestations (
  attestation_id uuid PRIMARY KEY DEFAULT uuidv7(),
  claim_id uuid NOT NULL REFERENCES public.claims(claim_id),
  object_revision bigint NOT NULL CHECK(object_revision>0),
  body_sha bytea NOT NULL CHECK(octet_length(body_sha)=32),
  root_set_sha bytea NOT NULL CHECK(octet_length(root_set_sha)=32),
  support_count integer NOT NULL CHECK(support_count>0),
  independent_count integer NOT NULL CHECK(independent_count>0 AND independent_count<=support_count),
  sybil_risk text NOT NULL CHECK(sybil_risk IN ('CONCENTRATED','NO_CONCENTRATION_OBSERVED')),
  policy_version text NOT NULL CHECK(btrim(policy_version)<>''),
  policy_digest bytea NOT NULL CHECK(octet_length(policy_digest)=32),
  checks_complete boolean NOT NULL CHECK(checks_complete),
  attested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(claim_id,object_revision),
  CHECK((sybil_risk='NO_CONCENTRATION_OBSERVED')=(independent_count=support_count))
);

CREATE FUNCTION public.guard_claim_independence_attestation_append_only()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
  RAISE EXCEPTION 'claim independence attestations are append-only' USING ERRCODE='23514';
END;
$$;
CREATE TRIGGER claim_independence_attestation_append_only
  BEFORE UPDATE OR DELETE ON public.claim_independence_attestations
  FOR EACH ROW EXECUTE FUNCTION public.guard_claim_independence_attestation_append_only();

-- FORCE RLS remains enabled.  These owner-only SELECT branches are reachable only through the
-- narrowly ACLed SECURITY DEFINER function below; no runtime role gains protected table access.
CREATE POLICY anonymous_source_lineage_attestation_owner_read
  ON control.anonymous_source_lineage FOR SELECT TO role_migration_owner
  USING(true);
CREATE POLICY contribution_releases_attestation_owner_read
  ON staging.contribution_releases FOR SELECT TO role_migration_owner
  USING(true);
CREATE POLICY phase9_assessments_attestation_owner_read
  ON staging.contribution_candidate_phase9_assessments FOR SELECT TO role_migration_owner
  USING(true);

CREATE FUNCTION public.attest_current_claim_independence(
  p_claim_id uuid,
  p_expected_revision bigint,
  p_expected_body_sha bytea
)
RETURNS public.claim_independence_attestations
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE
  v_current_revision bigint;
  v_current_body_sha bytea;
  v_root_count bigint;
  v_bound_count bigint;
  v_root_set_sha bytea;
  v_independent_count bigint;
  v_policy_version constant text := 'phase9-independence-v1';
  v_policy_digest constant bytea := sha256(convert_to(
    'phase9-independence-v1|principal_id|content_hash|verified_organization|canonical_url|document_fingerprint|upstream_root|transitive-connected-components',
    'UTF8'));
  v_receipt public.claim_independence_attestations;
BEGIN
  IF p_claim_id IS NULL OR p_expected_revision<=0 OR octet_length(p_expected_body_sha)<>32 THEN
    RAISE EXCEPTION 'independence attestation requires claim, positive revision and body digest'
      USING ERRCODE='22023';
  END IF;

  PERFORM pg_advisory_xact_lock(hashtextextended(
    'phase9-claim-independence:'||p_claim_id::text||':'||p_expected_revision::text,0));

  -- Match the existing evaluator's graph-before-claim lock order.  The coarse locks keep one
  -- protected snapshot correct; replace them with a shared per-claim graph lock only when
  -- measured attestation throughput requires it.
  LOCK TABLE public.provenance_edges,public.sources,public.anonymous_source_authority_events,
    public.anonymous_source_lifecycle_events,control.anonymous_source_lineage,
    staging.contribution_releases,staging.contribution_candidate_phase9_assessments,
    staging.sanitized_public_candidates,ops.anonymous_public_revocations IN SHARE MODE;

  SELECT c.object_revision,sha256(convert_to(c.content::text,'UTF8'))
    INTO v_current_revision,v_current_body_sha
    FROM public.claims c WHERE c.claim_id=p_claim_id FOR SHARE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'claim does not exist' USING ERRCODE='P0002';
  END IF;
  IF v_current_revision IS DISTINCT FROM p_expected_revision
    OR v_current_body_sha IS DISTINCT FROM p_expected_body_sha THEN
    RAISE EXCEPTION 'claim revision or body is stale' USING ERRCODE='40001';
  END IF;

  WITH RECURSIVE roots AS MATERIALIZED (
    SELECT s.source_id,s.content_hash,
      CASE WHEN s.identity_policy_version IS NOT NULL THEN s.verified_organization END
        AS verified_organization,
      CASE WHEN s.identity_policy_version IS NOT NULL THEN s.canonical_url END AS canonical_url,
      CASE WHEN s.identity_policy_version IS NOT NULL THEN s.document_fingerprint END
        AS document_fingerprint,
      CASE WHEN s.identity_policy_version IS NOT NULL THEN s.upstream_root END AS upstream_root,
      s.anonymous_envelope_sha256
    FROM public.current_public_roots(p_claim_id,NULL) r
    JOIN public.sources s ON s.source_id=r.root_source_id
    WHERE s.lineage_mode='ANONYMOUS_RELEASE'
      AND s.contribution_release_id IS NULL
      AND s.publisher IS NULL AND s.source_url IS NULL
      AND s.verified_organization IS NULL AND s.canonical_url IS NULL
      AND s.document_fingerprint IS NULL AND s.upstream_root IS NULL
      AND s.identity_policy_version IS NULL AND s.trusted_by_policy IS NULL
  ), raw_bindings AS MATERIALIZED (
    SELECT roots.*,
      CASE
        WHEN jsonb_typeof(release.policy_snapshot->'principal_id')='string'
          AND release.policy_snapshot->>'principal_id' ~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN (release.policy_snapshot->>'principal_id')::uuid
      END AS principal_id,
      release.state AS release_state,release.revoked_at,release.candidate_id,
      assessed.candidate_id AS assessed_candidate_id,sealed.anonymous_source_id AS sealed_source_id,
      sealed.envelope_sha256 AS sealed_envelope_sha,authority.event_type AS authority_event,
      authority.source_revision AS authority_revision,
      current_object.anonymous_source_id AS current_object_source_id,
      revocation.anonymous_source_id AS revoked_source_id
    FROM roots
    LEFT JOIN control.anonymous_source_lineage lineage
      ON lineage.anonymous_source_id=roots.source_id
    LEFT JOIN staging.contribution_releases release
      ON release.contribution_release_id=lineage.contribution_release_id
     AND release.tenant_id=lineage.tenant_id
    LEFT JOIN staging.contribution_candidate_phase9_assessments assessed
      ON assessed.candidate_id=release.candidate_id AND assessed.tenant_id=release.tenant_id
    LEFT JOIN staging.sanitized_public_candidates sealed
      ON sealed.tenant_id=lineage.tenant_id
     AND sealed.anonymous_source_id=lineage.anonymous_source_id
     AND sealed.envelope_sha256=roots.anonymous_envelope_sha256
     AND sealed.assessment_digest=assessed.assessment_digest
     AND encode(sealed.content_sha256,'hex')=roots.content_hash
    LEFT JOIN LATERAL (
      SELECT event.event_type,event.source_revision
      FROM public.anonymous_source_authority_events event
      WHERE event.anonymous_source_id=roots.source_id
        AND event.candidate_envelope_sha256=roots.anonymous_envelope_sha256
      ORDER BY event.source_revision DESC,event.authority_seq DESC LIMIT 1
    ) authority ON true
    LEFT JOIN public.current_anonymous_source_objects current_object
      ON current_object.anonymous_source_id=roots.source_id
     AND current_object.candidate_envelope_sha256=roots.anonymous_envelope_sha256
     AND current_object.claim_id=p_claim_id
     AND current_object.object_revision=p_expected_revision
    LEFT JOIN ops.anonymous_public_revocations revocation
      ON revocation.anonymous_source_id=roots.source_id
     AND revocation.candidate_envelope_sha256=roots.anonymous_envelope_sha256
  ), bindings AS MATERIALIZED (
    SELECT source_id,content_hash,verified_organization,canonical_url,document_fingerprint,
      upstream_root,principal_id
    FROM raw_bindings
    WHERE principal_id IS NOT NULL
      AND release_state='ACTIVE' AND revoked_at IS NULL AND candidate_id IS NOT NULL
      AND assessed_candidate_id=candidate_id
      AND sealed_source_id=source_id AND sealed_envelope_sha=anonymous_envelope_sha256
      AND authority_event='ADMIT' AND authority_revision=1
      AND current_object_source_id=source_id AND revoked_source_id IS NULL
  ), edges(a,b) AS (
    SELECT left_root.source_id,right_root.source_id
    FROM bindings left_root CROSS JOIN bindings right_root
    WHERE left_root.source_id=right_root.source_id
      OR left_root.principal_id=right_root.principal_id
      OR left_root.content_hash=right_root.content_hash
      OR (left_root.verified_organization IS NOT NULL
        AND left_root.verified_organization=right_root.verified_organization)
      OR (left_root.canonical_url IS NOT NULL AND left_root.canonical_url=right_root.canonical_url)
      OR (left_root.document_fingerprint IS NOT NULL
        AND left_root.document_fingerprint=right_root.document_fingerprint)
      OR (left_root.upstream_root IS NOT NULL AND left_root.upstream_root=right_root.upstream_root)
  ), reach(source_id,reachable_id) AS (
    SELECT a,b FROM edges
    UNION
    SELECT reach.source_id,edges.b FROM reach JOIN edges ON edges.a=reach.reachable_id
  ), components AS (
    SELECT source_id,min(reachable_id::text)::uuid AS component_id FROM reach GROUP BY source_id
  )
  SELECT (SELECT count(*) FROM public.current_public_roots(p_claim_id,NULL)),
    (SELECT count(*) FROM bindings),
    (SELECT sha256(convert_to(string_agg(source_id::text,'|' ORDER BY source_id),'UTF8'))
      FROM roots),
    (SELECT count(DISTINCT component_id) FROM components)
  INTO v_root_count,v_bound_count,v_root_set_sha,v_independent_count;

  IF v_root_count<=0 OR v_bound_count IS DISTINCT FROM v_root_count
    OR v_root_set_sha IS NULL OR v_independent_count<=0 THEN
    RAISE EXCEPTION 'claim roots lack complete current protected independence facts'
      USING ERRCODE='23514';
  END IF;

  SELECT * INTO v_receipt FROM public.claim_independence_attestations receipt
    WHERE receipt.claim_id=p_claim_id AND receipt.object_revision=p_expected_revision;
  IF FOUND THEN
    IF v_receipt.body_sha IS DISTINCT FROM p_expected_body_sha
      OR v_receipt.root_set_sha IS DISTINCT FROM v_root_set_sha
      OR v_receipt.support_count IS DISTINCT FROM v_root_count::integer
      OR v_receipt.independent_count IS DISTINCT FROM v_independent_count::integer
      OR v_receipt.sybil_risk IS DISTINCT FROM (CASE
        WHEN v_independent_count=v_root_count THEN 'NO_CONCENTRATION_OBSERVED' ELSE 'CONCENTRATED' END)
      OR v_receipt.policy_version IS DISTINCT FROM v_policy_version
      OR v_receipt.policy_digest IS DISTINCT FROM v_policy_digest
      OR NOT v_receipt.checks_complete THEN
      RAISE EXCEPTION 'existing independence attestation is stale for current roots or policy'
        USING ERRCODE='40001';
    END IF;
    RETURN v_receipt;
  END IF;

  INSERT INTO public.claim_independence_attestations(
    claim_id,object_revision,body_sha,root_set_sha,support_count,independent_count,sybil_risk,
    policy_version,policy_digest,checks_complete)
  VALUES(p_claim_id,p_expected_revision,p_expected_body_sha,v_root_set_sha,v_root_count::integer,
    v_independent_count::integer,CASE WHEN v_independent_count=v_root_count
      THEN 'NO_CONCENTRATION_OBSERVED' ELSE 'CONCENTRATED' END,
    v_policy_version,v_policy_digest,true)
  RETURNING * INTO v_receipt;
  RETURN v_receipt;
END;
$$;

ALTER TABLE public.claim_independence_attestations OWNER TO role_migration_owner;
ALTER FUNCTION public.guard_claim_independence_attestation_append_only()
  OWNER TO role_migration_owner;
ALTER FUNCTION public.attest_current_claim_independence(uuid,bigint,bytea)
  OWNER TO role_migration_owner;

REVOKE ALL ON public.claim_independence_attestations FROM PUBLIC,role_gateway,
  role_private_worker,role_consolidation_worker,role_public_worker,role_retrieval_worker,
  role_batch_issuer,role_maintenance;
GRANT SELECT ON public.claim_independence_attestations TO role_gateway,role_public_worker,
  role_retrieval_worker,role_maintenance;
REVOKE SELECT ON staging.contribution_releases FROM role_public_worker;
REVOKE ALL ON FUNCTION public.guard_claim_independence_attestation_append_only() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.attest_current_claim_independence(uuid,bigint,bytea) FROM PUBLIC,
  role_gateway,role_private_worker,role_consolidation_worker,role_public_worker,
  role_retrieval_worker,role_batch_issuer,role_maintenance;
GRANT EXECUTE ON FUNCTION public.attest_current_claim_independence(uuid,bigint,bytea)
  TO role_public_worker;

COMMENT ON TABLE public.claim_independence_attestations IS
  'Append-only aggregate anti-Sybil receipt. Contains no contributor, tenant, release, group, pseudonym, nullifier, or root-list identity.';
COMMENT ON FUNCTION public.attest_current_claim_independence(uuid,bigint,bytea) IS
  'Protected claim-only independence attestation. Fails closed unless every anonymous direct root resolves to one current active assessed principal; returns only the aggregate receipt.';
