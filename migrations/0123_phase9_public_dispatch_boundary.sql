-- Phase 9 P0: the public worker must never directly read tenant-bearing outbox/job rows.
-- These narrow SECURITY DEFINER entry points keep the durable queue private while preserving
-- both legacy release-addressed events and anonymous pair-only public dispatch.

CREATE FUNCTION ops.emit_public_object_changed(
  p_tenant_id uuid,p_claim_id uuid,p_synthesis_id uuid,p_object_revision bigint
) RETURNS void
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,ops AS $$
BEGIN
  IF (p_claim_id IS NULL) = (p_synthesis_id IS NULL) OR p_object_revision <= 0 THEN
    RAISE EXCEPTION 'public object event requires exactly one object and a positive revision' USING ERRCODE='22023';
  END IF;
  INSERT INTO ops.outbox(tenant_id,commit_seq,event_type,public_claim_id,public_synthesis_id,object_revision)
    VALUES(p_tenant_id,nextval('ops.commit_seq_seq'),'PUBLIC_OBJECT_CHANGED',
      p_claim_id,p_synthesis_id,p_object_revision);
END;
$$;

CREATE TABLE ops.public_anonymous_dispatches (
  dispatch_id uuid PRIMARY KEY DEFAULT uuidv7(),
  outbox_event_id uuid NOT NULL UNIQUE REFERENCES ops.outbox(outbox_id),
  job_type text NOT NULL CHECK(job_type IN ('PUBLIC_ANONYMOUS_RELEASE_APPLY',
    'PUBLIC_ANONYMOUS_REVOKE_APPLY','PUBLIC_PROJECT')),
  status text NOT NULL DEFAULT 'PENDING' CHECK(status IN ('PENDING','PROCESSING','RETRY_WAIT','DONE','FAILED','DEAD')),
  attempt integer NOT NULL DEFAULT 0 CHECK(attempt >= 0),
  next_retry_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  lease_owner text,
  lease_expires_at timestamptz,
  payload jsonb NOT NULL,
  last_error_class text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION ops.enqueue_anonymous_public_dispatch()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,ops AS $$
DECLARE dispatch_kind text; dispatch_payload jsonb;
BEGIN
  IF NEW.event_type NOT IN ('PUBLIC_ANONYMOUS_RELEASE','PUBLIC_ANONYMOUS_REVOKE') THEN
    RETURN NEW;
  END IF;
  dispatch_kind := CASE NEW.event_type WHEN 'PUBLIC_ANONYMOUS_RELEASE'
    THEN 'PUBLIC_ANONYMOUS_RELEASE_APPLY' ELSE 'PUBLIC_ANONYMOUS_REVOKE_APPLY' END;
  dispatch_payload := jsonb_build_object('anonymous_source_id',NEW.anonymous_source_id::text,
    'envelope_sha256',encode(NEW.candidate_envelope_sha256,'hex'),
    'source_revision',NEW.anonymous_source_revision);
  INSERT INTO ops.public_anonymous_dispatches(outbox_event_id,job_type,payload)
    VALUES(NEW.outbox_id,dispatch_kind,dispatch_payload)
    ON CONFLICT(outbox_event_id) DO NOTHING;
  UPDATE ops.outbox SET status='DONE',processed_at=clock_timestamp() WHERE outbox_id=NEW.outbox_id;
  RETURN NEW;
END;
$$;
CREATE TRIGGER anonymous_public_dispatch_enqueue AFTER INSERT ON ops.outbox
  FOR EACH ROW EXECUTE FUNCTION ops.enqueue_anonymous_public_dispatch();

CREATE FUNCTION ops.enqueue_public_projection_from_anonymous_dispatch(
  p_dispatch_id uuid,p_lease_owner text,p_attempt integer,p_claim_id uuid,p_synthesis_id uuid,
  p_object_revision bigint
) RETURNS void
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,ops AS $$
DECLARE source_tenant_id uuid; projection_outbox_id uuid; bound_source_id uuid; bound_envelope bytea;
BEGIN
  IF (p_claim_id IS NULL) = (p_synthesis_id IS NULL) OR p_object_revision <= 0 THEN
    RAISE EXCEPTION 'anonymous projection requires exactly one object and a positive revision' USING ERRCODE='22023';
  END IF;
  SELECT o.tenant_id,(d.payload->>'anonymous_source_id')::uuid,
      decode(d.payload->>'envelope_sha256','hex') INTO source_tenant_id,bound_source_id,bound_envelope
    FROM ops.public_anonymous_dispatches d JOIN ops.outbox o ON o.outbox_id=d.outbox_event_id
    WHERE d.dispatch_id=p_dispatch_id AND d.job_type IN ('PUBLIC_ANONYMOUS_RELEASE_APPLY',
      'PUBLIC_ANONYMOUS_REVOKE_APPLY') AND d.status='PROCESSING' AND d.lease_owner=p_lease_owner
      AND d.attempt=p_attempt AND d.lease_expires_at > clock_timestamp();
  IF NOT FOUND THEN
    RAISE EXCEPTION 'anonymous projection has no live source dispatch authority' USING ERRCODE='42501';
  END IF;
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
      WHERE synthesis.synthesis_id=p_synthesis_id
        AND synthesis.object_revision=p_object_revision
        AND closure.is_current AND source.source_id=bound_source_id
        AND source.anonymous_envelope_sha256=bound_envelope)) THEN
    RAISE EXCEPTION 'anonymous projection object revision is not rooted in this exact source pair'
      USING ERRCODE='42501';
  END IF;
  INSERT INTO ops.outbox(tenant_id,commit_seq,event_type,public_claim_id,public_synthesis_id,object_revision)
    VALUES(source_tenant_id,nextval('ops.commit_seq_seq'),'PUBLIC_OBJECT_CHANGED',
      p_claim_id,p_synthesis_id,p_object_revision)
    RETURNING outbox_id INTO projection_outbox_id;
  INSERT INTO ops.public_anonymous_dispatches(outbox_event_id,job_type,payload)
    VALUES(projection_outbox_id,'PUBLIC_PROJECT',jsonb_build_object(
      'object_id',coalesce(p_claim_id,p_synthesis_id)::text,
      'object_kind',CASE WHEN p_claim_id IS NULL THEN 'SYNTHESIS' ELSE 'CLAIM' END,
      'object_revision',p_object_revision));
  UPDATE ops.outbox SET status='DONE',processed_at=clock_timestamp() WHERE outbox_id=projection_outbox_id;
END;
$$;

CREATE FUNCTION ops.dispatch_public_outbox(p_tenant_id uuid,p_limit bigint)
RETURNS bigint
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,ops AS $$
DECLARE event_row record; dispatched bigint := 0; job_type text; job_payload jsonb;
BEGIN
  IF p_limit <= 0 THEN
    RAISE EXCEPTION 'public dispatch limit must be positive' USING ERRCODE='22023';
  END IF;
  PERFORM set_config('humaux.tenant_id',p_tenant_id::text,true);
  FOR event_row IN
    SELECT outbox_id,event_type,contribution_release_id,anonymous_source_id,candidate_envelope_sha256,
      anonymous_source_revision,public_claim_id,public_synthesis_id,object_revision
    FROM ops.outbox WHERE tenant_id=p_tenant_id AND status='PENDING'
      AND event_type IN ('PUBLIC_RELEASE','PUBLIC_REVOKE','PUBLIC_OBJECT_CHANGED')
    ORDER BY commit_seq FOR UPDATE SKIP LOCKED LIMIT p_limit
  LOOP
    CASE event_row.event_type
      WHEN 'PUBLIC_RELEASE' THEN
        job_type := 'PUBLIC_RELEASE_APPLY';
        job_payload := jsonb_build_object('release_id',event_row.contribution_release_id::text);
      WHEN 'PUBLIC_REVOKE' THEN
        job_type := 'PUBLIC_REVOKE_APPLY';
        job_payload := jsonb_build_object('release_id',event_row.contribution_release_id::text);
      WHEN 'PUBLIC_OBJECT_CHANGED' THEN
        job_type := 'PUBLIC_PROJECT';
        job_payload := jsonb_build_object('object_id',coalesce(event_row.public_claim_id,event_row.public_synthesis_id)::text,
          'object_kind',CASE WHEN event_row.public_claim_id IS NULL THEN 'SYNTHESIS' ELSE 'CLAIM' END,
          'object_revision',event_row.object_revision);
      ELSE
        RAISE EXCEPTION 'unexpected public dispatch event type' USING ERRCODE='23514';
    END CASE;
    INSERT INTO ops.jobs(tenant_id,job_type,status,next_retry_at,idempotency_key,payload,outbox_event_id,consumer)
      VALUES(p_tenant_id,job_type,'PENDING',clock_timestamp(),
        'phase9-public-runtime:'||event_row.outbox_id::text,job_payload,event_row.outbox_id,
        'phase9-public-runtime')
      ON CONFLICT (outbox_event_id,consumer) WHERE outbox_event_id IS NOT NULL DO NOTHING;
    IF event_row.event_type='PUBLIC_OBJECT_CHANGED' AND event_row.public_synthesis_id IS NOT NULL THEN
      INSERT INTO ops.jobs(tenant_id,job_type,status,next_retry_at,idempotency_key,payload,outbox_event_id,consumer)
        VALUES(p_tenant_id,'PUBLIC_SYNTHESIS_REBUILD','PENDING',clock_timestamp(),
          'phase9-public-runtime-rebuild:'||event_row.outbox_id::text,
          jsonb_build_object('synthesis_id',event_row.public_synthesis_id::text,
            'object_revision',event_row.object_revision),event_row.outbox_id,
          'phase9-public-synthesis-rebuild')
        ON CONFLICT (outbox_event_id,consumer) WHERE outbox_event_id IS NOT NULL DO NOTHING;
    END IF;
    UPDATE ops.outbox SET status='DONE',processed_at=clock_timestamp() WHERE outbox_id=event_row.outbox_id;
    dispatched := dispatched + 1;
  END LOOP;
  RETURN dispatched;
END;
$$;

CREATE FUNCTION ops.claim_public_dispatch_jobs(
  p_tenant_id uuid,p_lease_owner text,p_lease_seconds double precision,p_limit bigint
) RETURNS TABLE(job_id uuid,job_type text,attempt integer,payload jsonb)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,ops AS $$
BEGIN
  IF p_limit <= 0 OR p_lease_seconds <= 0 OR length(trim(p_lease_owner))=0 THEN
    RAISE EXCEPTION 'invalid public dispatch claim' USING ERRCODE='22023';
  END IF;
  PERFORM set_config('humaux.tenant_id',p_tenant_id::text,true);
  RETURN QUERY
    WITH picked AS (
      SELECT j.job_id FROM ops.jobs j
      WHERE j.tenant_id=p_tenant_id AND j.status IN ('PENDING','RETRY_WAIT')
        AND j.next_retry_at <= clock_timestamp()
        AND j.job_type IN ('PUBLIC_RELEASE_APPLY','PUBLIC_REVOKE_APPLY','PUBLIC_PROJECT')
      ORDER BY j.priority DESC,j.next_retry_at,j.created_at FOR UPDATE SKIP LOCKED LIMIT p_limit
    )
    UPDATE ops.jobs j SET status='PROCESSING',lease_owner=p_lease_owner,
      lease_expires_at=clock_timestamp()+make_interval(secs => p_lease_seconds),attempt=j.attempt+1
    FROM picked WHERE j.job_id=picked.job_id
    RETURNING j.job_id,j.job_type,j.attempt,j.payload;
END;
$$;

CREATE FUNCTION ops.claim_global_anonymous_public_dispatches(
  p_lease_owner text,p_lease_seconds double precision,p_limit bigint
) RETURNS TABLE(dispatch_id uuid,job_type text,attempt integer,payload jsonb)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,ops AS $$
BEGIN
  IF p_limit <= 0 OR p_lease_seconds <= 0 OR length(trim(p_lease_owner))=0 THEN
    RAISE EXCEPTION 'invalid global anonymous dispatch claim' USING ERRCODE='22023';
  END IF;
  RETURN QUERY
    WITH picked AS (
      SELECT d.dispatch_id FROM ops.public_anonymous_dispatches d
      WHERE d.status IN ('PENDING','RETRY_WAIT') AND d.next_retry_at <= clock_timestamp()
      ORDER BY d.next_retry_at,d.created_at FOR UPDATE SKIP LOCKED LIMIT p_limit
    )
    UPDATE ops.public_anonymous_dispatches d SET status='PROCESSING',lease_owner=p_lease_owner,
      lease_expires_at=clock_timestamp()+make_interval(secs => p_lease_seconds),attempt=d.attempt+1
    FROM picked WHERE d.dispatch_id=picked.dispatch_id
    RETURNING d.dispatch_id,d.job_type,d.attempt,d.payload;
END;
$$;

CREATE FUNCTION ops.global_anonymous_public_dispatch_live_lease(
  p_dispatch_id uuid,p_lease_owner text,p_attempt integer
) RETURNS boolean
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,ops AS $$
  SELECT EXISTS(SELECT 1 FROM ops.public_anonymous_dispatches WHERE dispatch_id=p_dispatch_id
    AND lease_owner=p_lease_owner AND attempt=p_attempt AND status='PROCESSING'
    AND lease_expires_at > clock_timestamp())
$$;

CREATE FUNCTION ops.complete_global_anonymous_public_dispatch(
  p_dispatch_id uuid,p_lease_owner text,p_attempt integer
) RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,ops AS $$
BEGIN
  UPDATE ops.public_anonymous_dispatches SET status='DONE' WHERE dispatch_id=p_dispatch_id
    AND lease_owner=p_lease_owner AND attempt=p_attempt AND status='PROCESSING'
    AND lease_expires_at > clock_timestamp();
  RETURN FOUND;
END;
$$;

CREATE FUNCTION ops.fail_global_anonymous_public_dispatch(
  p_dispatch_id uuid,p_lease_owner text,p_attempt integer,p_error_class text,
  p_retryable boolean,p_max_attempts integer,p_retry_after_seconds double precision
) RETURNS text
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,ops AS $$
DECLARE next_status text;
BEGIN
  UPDATE ops.public_anonymous_dispatches SET status=CASE WHEN NOT p_retryable THEN 'FAILED'
      WHEN attempt >= p_max_attempts THEN 'DEAD' ELSE 'RETRY_WAIT' END,
    next_retry_at=CASE WHEN p_retryable AND attempt < p_max_attempts
      THEN clock_timestamp()+make_interval(secs => p_retry_after_seconds) ELSE next_retry_at END,
    last_error_class=p_error_class
  WHERE dispatch_id=p_dispatch_id AND lease_owner=p_lease_owner AND attempt=p_attempt
    AND status='PROCESSING' AND lease_expires_at > clock_timestamp()
  RETURNING status INTO next_status;
  RETURN next_status;
END;
$$;

CREATE FUNCTION ops.public_dispatch_live_lease(
  p_tenant_id uuid,p_job_id uuid,p_lease_owner text,p_attempt integer
) RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,ops AS $$
BEGIN
  PERFORM set_config('humaux.tenant_id',p_tenant_id::text,true);
  RETURN EXISTS(SELECT 1 FROM ops.jobs WHERE tenant_id=p_tenant_id AND job_id=p_job_id
    AND lease_owner=p_lease_owner AND attempt=p_attempt AND status='PROCESSING'
    AND lease_expires_at > clock_timestamp());
END;
$$;

CREATE FUNCTION ops.complete_public_dispatch_job(
  p_tenant_id uuid,p_job_id uuid,p_lease_owner text,p_attempt integer
) RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,ops AS $$
BEGIN
  PERFORM set_config('humaux.tenant_id',p_tenant_id::text,true);
  UPDATE ops.jobs SET status='DONE' WHERE tenant_id=p_tenant_id AND job_id=p_job_id
    AND lease_owner=p_lease_owner AND attempt=p_attempt AND status='PROCESSING'
    AND lease_expires_at > clock_timestamp();
  RETURN FOUND;
END;
$$;

CREATE FUNCTION ops.fail_public_dispatch_job(
  p_tenant_id uuid,p_job_id uuid,p_lease_owner text,p_attempt integer,p_error_class text,
  p_retryable boolean,p_max_attempts integer,p_retry_after_seconds double precision
) RETURNS text
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,ops AS $$
DECLARE next_status text;
BEGIN
  PERFORM set_config('humaux.tenant_id',p_tenant_id::text,true);
  UPDATE ops.jobs SET status=CASE WHEN NOT p_retryable THEN 'FAILED'
      WHEN attempt >= p_max_attempts THEN 'DEAD' ELSE 'RETRY_WAIT' END,
    next_retry_at=CASE WHEN p_retryable AND attempt < p_max_attempts
      THEN clock_timestamp()+make_interval(secs => p_retry_after_seconds) ELSE next_retry_at END,
    last_error_class=p_error_class
  WHERE tenant_id=p_tenant_id AND job_id=p_job_id AND lease_owner=p_lease_owner
    AND attempt=p_attempt AND status='PROCESSING' AND lease_expires_at > clock_timestamp()
  RETURNING status INTO next_status;
  RETURN next_status;
END;
$$;

ALTER FUNCTION ops.emit_public_object_changed(uuid,uuid,uuid,bigint) OWNER TO role_migration_owner;
ALTER TABLE ops.public_anonymous_dispatches OWNER TO role_migration_owner;
ALTER FUNCTION ops.enqueue_anonymous_public_dispatch() OWNER TO role_migration_owner;
ALTER FUNCTION ops.enqueue_public_projection_from_anonymous_dispatch(uuid,text,integer,uuid,uuid,bigint) OWNER TO role_migration_owner;
ALTER FUNCTION ops.dispatch_public_outbox(uuid,bigint) OWNER TO role_migration_owner;
ALTER FUNCTION ops.claim_public_dispatch_jobs(uuid,text,double precision,bigint) OWNER TO role_migration_owner;
ALTER FUNCTION ops.public_dispatch_live_lease(uuid,uuid,text,integer) OWNER TO role_migration_owner;
ALTER FUNCTION ops.complete_public_dispatch_job(uuid,uuid,text,integer) OWNER TO role_migration_owner;
ALTER FUNCTION ops.fail_public_dispatch_job(uuid,uuid,text,integer,text,boolean,integer,double precision) OWNER TO role_migration_owner;
ALTER FUNCTION ops.claim_global_anonymous_public_dispatches(text,double precision,bigint) OWNER TO role_migration_owner;
ALTER FUNCTION ops.global_anonymous_public_dispatch_live_lease(uuid,text,integer) OWNER TO role_migration_owner;
ALTER FUNCTION ops.complete_global_anonymous_public_dispatch(uuid,text,integer) OWNER TO role_migration_owner;
ALTER FUNCTION ops.fail_global_anonymous_public_dispatch(uuid,text,integer,text,boolean,integer,double precision) OWNER TO role_migration_owner;

REVOKE ALL ON ops.outbox,ops.jobs FROM role_public_worker;
REVOKE ALL ON ops.public_anonymous_dispatches FROM PUBLIC,role_gateway,role_private_worker,
  role_consolidation_worker,role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;
REVOKE ALL ON SEQUENCE ops.commit_seq_seq FROM role_public_worker;
REVOKE ALL ON FUNCTION ops.emit_public_object_changed(uuid,uuid,uuid,bigint),
  ops.dispatch_public_outbox(uuid,bigint),
  ops.claim_public_dispatch_jobs(uuid,text,double precision,bigint),
  ops.public_dispatch_live_lease(uuid,uuid,text,integer),
  ops.complete_public_dispatch_job(uuid,uuid,text,integer),
  ops.fail_public_dispatch_job(uuid,uuid,text,integer,text,boolean,integer,double precision),
  ops.claim_global_anonymous_public_dispatches(text,double precision,bigint),
  ops.global_anonymous_public_dispatch_live_lease(uuid,text,integer),
  ops.complete_global_anonymous_public_dispatch(uuid,text,integer),
  ops.fail_global_anonymous_public_dispatch(uuid,text,integer,text,boolean,integer,double precision),
  ops.enqueue_public_projection_from_anonymous_dispatch(uuid,text,integer,uuid,uuid,bigint)
  FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.emit_public_object_changed(uuid,uuid,uuid,bigint),
  ops.dispatch_public_outbox(uuid,bigint),
  ops.claim_public_dispatch_jobs(uuid,text,double precision,bigint),
  ops.public_dispatch_live_lease(uuid,uuid,text,integer),
  ops.complete_public_dispatch_job(uuid,uuid,text,integer),
  ops.fail_public_dispatch_job(uuid,uuid,text,integer,text,boolean,integer,double precision),
  ops.claim_global_anonymous_public_dispatches(text,double precision,bigint),
  ops.global_anonymous_public_dispatch_live_lease(uuid,text,integer),
  ops.complete_global_anonymous_public_dispatch(uuid,text,integer),
  ops.fail_global_anonymous_public_dispatch(uuid,text,integer,text,boolean,integer,double precision),
  ops.enqueue_public_projection_from_anonymous_dispatch(uuid,text,integer,uuid,uuid,bigint)
  TO role_public_worker;

COMMENT ON FUNCTION ops.claim_global_anonymous_public_dispatches(text,double precision,bigint) IS
  'Phase 9 anonymous boundary: returns only a public job tuple; no tenant_id or queue metadata.';
