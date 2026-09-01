-- Phase 9 claim evaluations advance object_revision. Keep each still-authoritative anonymous
-- direct root on that exact successor revision so eligible_objects cannot outlive its receipt.

CREATE FUNCTION public.carry_forward_anonymous_claim_lifecycle(
  p_claim_id uuid,
  p_old_revision bigint,
  p_new_revision bigint
) RETURNS void
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,public,ops AS $$
BEGIN
  IF p_claim_id IS NULL OR p_old_revision<=0 OR p_new_revision<>p_old_revision+1 THEN
    RAISE EXCEPTION 'anonymous lifecycle carry-forward requires adjacent exact claim revisions'
      USING ERRCODE='22023';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'phase9-anonymous-lifecycle:'||p_claim_id::text||':'||p_new_revision::text,0));
  IF NOT EXISTS(SELECT 1 FROM public.claims claim
    WHERE claim.claim_id=p_claim_id AND claim.object_revision=p_new_revision) THEN
    RAISE EXCEPTION 'anonymous lifecycle carry-forward claim revision is stale' USING ERRCODE='40001';
  END IF;

  INSERT INTO public.anonymous_source_lifecycle_events(
    anonymous_source_id,candidate_envelope_sha256,claim_id,object_revision,event_type
  )
  SELECT current_object.anonymous_source_id,current_object.candidate_envelope_sha256,
    p_claim_id,p_new_revision,'ADMIT'
  FROM public.current_anonymous_source_objects current_object
  JOIN public.sources source ON source.source_id=current_object.anonymous_source_id
  JOIN public.provenance_edges edge
    ON edge.claim_id=p_claim_id AND edge.source_id=current_object.anonymous_source_id
  JOIN public.source_closure closure
    ON closure.claim_id=p_claim_id AND closure.root_source_id=current_object.anonymous_source_id
   AND closure.is_current
  JOIN LATERAL (
    SELECT authority.event_type,authority.source_revision
    FROM public.anonymous_source_authority_events authority
    WHERE authority.anonymous_source_id=current_object.anonymous_source_id
      AND authority.candidate_envelope_sha256=current_object.candidate_envelope_sha256
    ORDER BY authority.source_revision DESC,authority.authority_seq DESC LIMIT 1
  ) authority ON authority.event_type='ADMIT' AND authority.source_revision=1
  WHERE current_object.claim_id=p_claim_id AND current_object.synthesis_id IS NULL
    AND current_object.object_revision=p_old_revision
    AND source.lineage_mode='ANONYMOUS_RELEASE'
    AND source.anonymous_envelope_sha256=current_object.candidate_envelope_sha256
    AND source.contribution_release_id IS NULL AND source.publisher IS NULL AND source.source_url IS NULL
    AND NOT EXISTS(SELECT 1 FROM ops.anonymous_public_revocations revoked
      WHERE revoked.anonymous_source_id=current_object.anonymous_source_id
        AND revoked.candidate_envelope_sha256=current_object.candidate_envelope_sha256)
    AND NOT EXISTS(SELECT 1 FROM public.anonymous_source_lifecycle_events copied
      WHERE copied.anonymous_source_id=current_object.anonymous_source_id
        AND copied.candidate_envelope_sha256=current_object.candidate_envelope_sha256
        AND copied.claim_id=p_claim_id AND copied.synthesis_id IS NULL
        AND copied.object_revision=p_new_revision AND copied.event_type='ADMIT');
END;
$$;

ALTER FUNCTION public.carry_forward_anonymous_claim_lifecycle(uuid,bigint,bigint)
  OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION public.carry_forward_anonymous_claim_lifecycle(uuid,bigint,bigint)
  FROM PUBLIC,role_gateway,role_private_worker,role_consolidation_worker,role_retrieval_worker,
    role_batch_issuer,role_maintenance;
GRANT EXECUTE ON FUNCTION public.carry_forward_anonymous_claim_lifecycle(uuid,bigint,bigint)
  TO role_public_worker;

COMMENT ON FUNCTION public.carry_forward_anonymous_claim_lifecycle(uuid,bigint,bigint) IS
  'Copies only current, unrevoked anonymous direct-root ADMIT receipts to an adjacent claim revision; returns no protected identity.';
