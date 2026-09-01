-- Phase 9: global anonymous workers cannot carry tenant context. Only this owner policy and
-- these dispatch-bound definers may recover it from a live opaque dispatch authority.

CREATE POLICY outbox_phase9_owner_dispatch_read ON ops.outbox
  FOR SELECT TO role_migration_owner USING(true);

-- 0123 deliberately removed role_public_worker SELECT on the tenant-bearing outbox.  The
-- pre-existing deferred claim/synthesis guard still has to prove that a matching event was
-- committed, so run only that boolean integrity check as the migration owner.  It returns no
-- outbox data and keeps an attacker-controlled schema (including pg_temp) out of resolution.
ALTER FUNCTION public.require_object_outbox() SECURITY DEFINER;
ALTER FUNCTION public.require_object_outbox() SET search_path = pg_catalog;
ALTER FUNCTION public.require_object_outbox() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION public.require_object_outbox() FROM PUBLIC;

CREATE FUNCTION public.admit_anonymous_dispatch(
  p_dispatch_id uuid,p_lease_owner text,p_attempt integer
) RETURNS TABLE(claim_id uuid,object_revision bigint)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE source_tenant_id uuid; source_id uuid; envelope bytea; source_revision bigint;
  admitted_claim_id uuid; admitted_revision bigint;
BEGIN
  SELECT o.tenant_id,o.anonymous_source_id,o.candidate_envelope_sha256,o.anonymous_source_revision
    INTO source_tenant_id,source_id,envelope,source_revision
    FROM ops.public_anonymous_dispatches d JOIN ops.outbox o ON o.outbox_id=d.outbox_event_id
    WHERE d.dispatch_id=p_dispatch_id AND d.job_type='PUBLIC_ANONYMOUS_RELEASE_APPLY'
      AND d.status='PROCESSING' AND d.lease_owner=p_lease_owner AND d.attempt=p_attempt
      AND d.lease_expires_at > clock_timestamp() AND o.event_type='PUBLIC_ANONYMOUS_RELEASE'
      AND d.payload->>'anonymous_source_id'=o.anonymous_source_id::text
      AND d.payload->>'envelope_sha256'=encode(o.candidate_envelope_sha256,'hex')
      AND (d.payload->>'source_revision')::bigint=o.anonymous_source_revision
      AND o.anonymous_source_revision=1;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'anonymous admission requires its exact live dispatch authority' USING ERRCODE='42501';
  END IF;
  PERFORM set_config('humaux.tenant_id',source_tenant_id::text,true);
  SELECT public.admit_anonymous_source(source_id,envelope,source_revision) INTO admitted_claim_id;
  IF admitted_claim_id IS NULL THEN RETURN; END IF;
  SELECT claim.object_revision INTO admitted_revision FROM public.claims claim
    WHERE claim.claim_id=admitted_claim_id;
  claim_id := admitted_claim_id;
  object_revision := admitted_revision;
  RETURN NEXT;
END;
$$;

CREATE FUNCTION public.revoke_anonymous_dispatch(
  p_dispatch_id uuid,p_lease_owner text,p_attempt integer
) RETURNS TABLE(claim_id uuid,synthesis_id uuid,object_revision bigint)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE source_tenant_id uuid; source_id uuid; envelope bytea; source_revision bigint;
BEGIN
  SELECT o.tenant_id,o.anonymous_source_id,o.candidate_envelope_sha256,o.anonymous_source_revision
    INTO source_tenant_id,source_id,envelope,source_revision
    FROM ops.public_anonymous_dispatches d JOIN ops.outbox o ON o.outbox_id=d.outbox_event_id
    WHERE d.dispatch_id=p_dispatch_id AND d.job_type='PUBLIC_ANONYMOUS_REVOKE_APPLY'
      AND d.status='PROCESSING' AND d.lease_owner=p_lease_owner AND d.attempt=p_attempt
      AND d.lease_expires_at > clock_timestamp() AND o.event_type='PUBLIC_ANONYMOUS_REVOKE'
      AND d.payload->>'anonymous_source_id'=o.anonymous_source_id::text
      AND d.payload->>'envelope_sha256'=encode(o.candidate_envelope_sha256,'hex')
      AND (d.payload->>'source_revision')::bigint=o.anonymous_source_revision
      AND o.anonymous_source_revision=2;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'anonymous revocation requires its exact live dispatch authority' USING ERRCODE='42501';
  END IF;
  PERFORM set_config('humaux.tenant_id',source_tenant_id::text,true);
  RETURN QUERY SELECT * FROM public.revoke_anonymous_source(source_id,envelope,source_revision);
END;
$$;

CREATE OR REPLACE FUNCTION ops.enqueue_public_projection_from_anonymous_dispatch(
  p_dispatch_id uuid,p_lease_owner text,p_attempt integer,p_claim_id uuid,p_synthesis_id uuid,
  p_object_revision bigint
) RETURNS void
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE source_tenant_id uuid; projection_outbox_id uuid; bound_source_id uuid; bound_envelope bytea;
BEGIN
  IF (p_claim_id IS NULL) = (p_synthesis_id IS NULL) OR p_object_revision <= 0 THEN
    RAISE EXCEPTION 'anonymous projection requires exactly one object and a positive revision' USING ERRCODE='22023';
  END IF;
  SELECT o.tenant_id,o.anonymous_source_id,o.candidate_envelope_sha256
    INTO source_tenant_id,bound_source_id,bound_envelope
    FROM ops.public_anonymous_dispatches d JOIN ops.outbox o ON o.outbox_id=d.outbox_event_id
    WHERE d.dispatch_id=p_dispatch_id AND d.job_type IN ('PUBLIC_ANONYMOUS_RELEASE_APPLY',
      'PUBLIC_ANONYMOUS_REVOKE_APPLY') AND d.status='PROCESSING' AND d.lease_owner=p_lease_owner
      AND d.attempt=p_attempt AND d.lease_expires_at > clock_timestamp()
      AND ((d.job_type='PUBLIC_ANONYMOUS_RELEASE_APPLY' AND o.event_type='PUBLIC_ANONYMOUS_RELEASE'
          AND o.anonymous_source_revision=1)
        OR (d.job_type='PUBLIC_ANONYMOUS_REVOKE_APPLY' AND o.event_type='PUBLIC_ANONYMOUS_REVOKE'
          AND o.anonymous_source_revision=2))
      AND d.payload->>'anonymous_source_id'=o.anonymous_source_id::text
      AND d.payload->>'envelope_sha256'=encode(o.candidate_envelope_sha256,'hex')
      AND (d.payload->>'source_revision')::bigint=o.anonymous_source_revision;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'anonymous projection has no live source dispatch authority' USING ERRCODE='42501';
  END IF;
  PERFORM set_config('humaux.tenant_id',source_tenant_id::text,true);
  IF (p_claim_id IS NOT NULL AND NOT EXISTS(
      SELECT 1 FROM public.claims claim
      JOIN public.source_closure closure ON closure.claim_id=claim.claim_id
      JOIN public.sources source ON source.source_id=closure.root_source_id
      WHERE claim.claim_id=p_claim_id AND claim.object_revision=p_object_revision
        AND closure.is_current AND source.source_id=bound_source_id
        AND source.anonymous_envelope_sha256=bound_envelope))
    OR (p_synthesis_id IS NOT NULL AND NOT EXISTS(
      SELECT 1 FROM public.syntheses synthesis
      JOIN public.source_closure closure ON closure.synthesis_id=synthesis.synthesis_id
      JOIN public.sources source ON source.source_id=closure.root_source_id
      WHERE synthesis.synthesis_id=p_synthesis_id AND synthesis.object_revision=p_object_revision
        AND closure.is_current AND source.source_id=bound_source_id
        AND source.anonymous_envelope_sha256=bound_envelope)) THEN
    RAISE EXCEPTION 'anonymous projection object revision is not rooted in this exact source pair'
      USING ERRCODE='42501';
  END IF;
  INSERT INTO ops.outbox(tenant_id,commit_seq,event_type,public_claim_id,public_synthesis_id,object_revision)
    VALUES(source_tenant_id,nextval('ops.commit_seq_seq'),'PUBLIC_OBJECT_CHANGED',
      p_claim_id,p_synthesis_id,p_object_revision)
    ON CONFLICT DO NOTHING
    RETURNING outbox_id INTO projection_outbox_id;
  IF projection_outbox_id IS NOT NULL THEN
    INSERT INTO ops.public_anonymous_dispatches(outbox_event_id,job_type,payload)
      VALUES(projection_outbox_id,'PUBLIC_PROJECT',jsonb_build_object(
        'object_id',coalesce(p_claim_id,p_synthesis_id)::text,
        'object_kind',CASE WHEN p_claim_id IS NULL THEN 'SYNTHESIS' ELSE 'CLAIM' END,
        'object_revision',p_object_revision));
    UPDATE ops.outbox SET status='DONE',processed_at=clock_timestamp()
      WHERE outbox_id=projection_outbox_id;
  END IF;
END;
$$;

ALTER FUNCTION public.admit_anonymous_dispatch(uuid,text,integer) OWNER TO role_migration_owner;
ALTER FUNCTION public.revoke_anonymous_dispatch(uuid,text,integer) OWNER TO role_migration_owner;
ALTER FUNCTION ops.enqueue_public_projection_from_anonymous_dispatch(uuid,text,integer,uuid,uuid,bigint)
  OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION public.admit_anonymous_source(uuid,bytea,bigint),
  public.revoke_anonymous_source(uuid,bytea,bigint) FROM role_public_worker;
REVOKE ALL ON FUNCTION public.admit_anonymous_dispatch(uuid,text,integer),
  public.revoke_anonymous_dispatch(uuid,text,integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.admit_anonymous_dispatch(uuid,text,integer),
  public.revoke_anonymous_dispatch(uuid,text,integer) TO role_public_worker;

COMMENT ON FUNCTION public.admit_anonymous_dispatch(uuid,text,integer) IS
  'Global anonymous dispatch bridge. It derives tenant and source pair only from a live exact dispatch and returns no tenant identity.';
COMMENT ON FUNCTION public.require_object_outbox() IS
  'Deferred boolean integrity gate. SECURITY DEFINER preserves the no-direct-outbox-read boundary after 0123; it returns no tenant-bearing row.';
