-- Phase 9: execution-root reservation is authorized again immediately before the first
-- durable model-call row. The provider call remains outside this short transaction.

-- 0131 roots do not record creator-principal provenance. Refuse cutover while any root
-- can still cause a new provider-side effect; terminal history is intentionally retained.
DO $$
BEGIN
  -- Serialize the cutover with every contribution-root ingress and authority input
  -- mutation. The transaction-scoped advisory lock remains held through DDL commit.
  PERFORM ops.lock_contribution_inputs();
  -- 0131's SQL command predates the global-lock call now added below.  Take the DDL lock
  -- before the drain predicate so a direct in-flight 0131 caller cannot appear after it.
  LOCK TABLE private.contribution_executions IN ACCESS EXCLUSIVE MODE;

  IF EXISTS (
    SELECT 1
    FROM private.contribution_executions
    WHERE state IN ('READY_A','A_RESERVED','READY_B','B_RESERVED','READY_CANDIDATE')
  ) THEN
    RAISE EXCEPTION '0133 hard stop: drain every pre-0133 nonterminal contribution execution'
      USING ERRCODE = '55000';
  END IF;
END;
$$;

-- WORKSPACE_SHARED authority below reads the workspace's current tenant.  Serialize that
-- mutable lookup with enqueue/reservation so an admin cannot move/delete the workspace
-- between the exact authority recheck and the reservation transaction commit.
CREATE TRIGGER contribution_input_change
  BEFORE UPDATE OR DELETE ON control.workspaces
  FOR EACH ROW EXECUTE FUNCTION ops.guard_contribution_input_change();

-- A pre-0133 terminal root may be retained for audit, but its historic source/backing
-- closure cannot be reconstructed from today's mutable provenance tables.  New roots seal
-- the complete closure at enqueue; any root that can still reserve must carry the V1 seal.
ALTER TABLE private.contribution_executions
  ADD COLUMN source_backing_closure_version smallint,
  ADD COLUMN source_backing_closure_sha256 bytea,
  ADD COLUMN backing_link_count integer,
  ADD CONSTRAINT contribution_executions_source_backing_closure_shape CHECK (
    (
      source_backing_closure_version IS NULL
      AND source_backing_closure_sha256 IS NULL
      AND backing_link_count IS NULL
      AND state IN ('DONE','NOT_CONTRIBUTABLE','REJECTED_SAFETY','FAILED_TERMINAL')
    )
    OR (
      source_backing_closure_version IS NOT NULL
      AND source_backing_closure_sha256 IS NOT NULL
      AND backing_link_count IS NOT NULL
      AND source_backing_closure_version = 1
      AND octet_length(source_backing_closure_sha256) = 32
      AND backing_link_count >= 0
    )
  );

-- Canonical SQL-owned V1 closure.  The byte stream has a fixed domain/version header,
-- fixed-width UUID/integer fields, length-prefixed UTF-8 text, explicit nullable-UUID tags,
-- and deterministic row ordering.  No caller can provide either the count or the digest.
CREATE FUNCTION private.compute_contribution_source_backing_closure_v1(
  p_tenant_id uuid,
  p_user_id uuid,
  p_reasoning_domain_id uuid,
  p_source_kinds text[],
  p_source_ids uuid[],
  p_source_hashes bytea[]
) RETURNS TABLE(
  direct_count bigint,
  backing_link_count integer,
  closure_sha256 bytea
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  source_len integer;
  current_backing_count bigint;
  manifest_bytes bytea;
BEGIN
  IF p_tenant_id IS NULL OR p_user_id IS NULL OR p_reasoning_domain_id IS NULL
     OR p_tenant_id = '00000000-0000-0000-0000-000000000000'::uuid
     OR p_user_id = '00000000-0000-0000-0000-000000000000'::uuid
     OR p_reasoning_domain_id = '00000000-0000-0000-0000-000000000000'::uuid
     OR array_ndims(p_source_kinds) IS DISTINCT FROM 1
     OR array_ndims(p_source_ids) IS DISTINCT FROM 1
     OR array_ndims(p_source_hashes) IS DISTINCT FROM 1
     OR array_lower(p_source_kinds,1) IS DISTINCT FROM 1
     OR array_lower(p_source_ids,1) IS DISTINCT FROM 1
     OR array_lower(p_source_hashes,1) IS DISTINCT FROM 1 THEN
    RAISE EXCEPTION 'invalid contribution closure identity or array shape'
      USING ERRCODE = '22023';
  END IF;

  source_len := cardinality(p_source_ids);
  IF source_len = 0
     OR source_len IS DISTINCT FROM cardinality(p_source_kinds)
     OR source_len IS DISTINCT FROM cardinality(p_source_hashes)
     OR array_position(p_source_kinds,NULL) IS NOT NULL
     OR array_position(p_source_ids,NULL) IS NOT NULL
     OR array_position(p_source_hashes,NULL) IS NOT NULL
     OR EXISTS (
       SELECT 1
       FROM generate_subscripts(p_source_ids,1) AS source(i)
       WHERE p_source_kinds[source.i] NOT IN ('EVIDENCE','MEMORY')
          OR p_source_ids[source.i] = '00000000-0000-0000-0000-000000000000'::uuid
          OR octet_length(p_source_hashes[source.i]) <> 32
     )
     OR EXISTS (
       SELECT 1
       FROM generate_subscripts(p_source_ids,1) AS source(i)
       GROUP BY p_source_kinds[source.i],p_source_ids[source.i]
       HAVING count(*) > 1
     ) THEN
    RAISE EXCEPTION 'typed contribution closure sources must be nonempty, valid, and unique'
      USING ERRCODE = '22023';
  END IF;

  PERFORM set_config('humaux.tenant_id',p_tenant_id::text,true);
  PERFORM set_config('humaux.user_id',p_user_id::text,true);

  -- Validate every direct row against current SQL authority.  WORKSPACE_SHARED follows the
  -- current schema's tenant-membership visibility rule; the workspace itself must still be
  -- in the same tenant.  This is the same boundary used by the RLS policy in migration 0012.
  IF EXISTS (
    SELECT 1
    FROM generate_subscripts(p_source_ids,1) AS source(i)
    LEFT JOIN private.evidence_objects evidence
      ON p_source_kinds[source.i] = 'EVIDENCE'
     AND evidence.tenant_id = p_tenant_id
     AND evidence.evidence_id = p_source_ids[source.i]
    LEFT JOIN private.memory_records memory
      ON p_source_kinds[source.i] = 'MEMORY'
     AND memory.tenant_id = p_tenant_id
     AND memory.memory_id = p_source_ids[source.i]
    WHERE
      (
        p_source_kinds[source.i] = 'EVIDENCE'
        AND (
          evidence.evidence_id IS NULL
          OR evidence.reasoning_domain_id IS DISTINCT FROM p_reasoning_domain_id
          OR evidence.payload_sha256 IS DISTINCT FROM p_source_hashes[source.i]
          OR NOT CASE evidence.visibility_class
            WHEN 'TENANT_SHARED' THEN
              evidence.visibility_user_id IS NULL
              AND evidence.visibility_workspace_id IS NULL
            WHEN 'USER_PRIVATE' THEN
              evidence.visibility_user_id = p_user_id
              AND evidence.visibility_workspace_id IS NULL
            WHEN 'WORKSPACE_SHARED' THEN
              evidence.visibility_user_id IS NULL
              AND evidence.visibility_workspace_id IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM control.workspaces workspace
                WHERE workspace.tenant_id = p_tenant_id
                  AND workspace.workspace_id = evidence.visibility_workspace_id
              )
            ELSE false
          END
        )
      )
      OR (
        p_source_kinds[source.i] = 'MEMORY'
        AND (
          memory.memory_id IS NULL
          OR memory.status IS DISTINCT FROM 'active'
          OR sha256(convert_to(memory.content::text,'UTF8'))
             IS DISTINCT FROM p_source_hashes[source.i]
          OR NOT CASE memory.visibility_class
            WHEN 'TENANT_SHARED' THEN
              memory.visibility_user_id IS NULL
              AND memory.visibility_workspace_id IS NULL
            WHEN 'USER_PRIVATE' THEN
              memory.visibility_user_id = p_user_id
              AND memory.visibility_workspace_id IS NULL
            WHEN 'WORKSPACE_SHARED' THEN
              memory.visibility_user_id IS NULL
              AND memory.visibility_workspace_id IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM control.workspaces workspace
                WHERE workspace.tenant_id = p_tenant_id
                  AND workspace.workspace_id = memory.visibility_workspace_id
              )
            ELSE false
          END
          OR NOT EXISTS (
            SELECT 1 FROM private.memory_evidence backing
            WHERE backing.memory_id = p_source_ids[source.i]
          )
        )
      )
  ) THEN
    RAISE EXCEPTION 'contribution direct source is unresolved, stale, or unauthorized'
      USING ERRCODE = '42501';
  END IF;

  -- A LEFT JOIN is intentional: an RLS-hidden, deleted, cross-tenant, or otherwise missing
  -- backing Evidence remains a row in memory_evidence and therefore fails closed here.
  IF EXISTS (
    SELECT 1
    FROM generate_subscripts(p_source_ids,1) AS source(i)
    JOIN private.memory_evidence backing
      ON p_source_kinds[source.i] = 'MEMORY'
     AND backing.memory_id = p_source_ids[source.i]
    LEFT JOIN private.evidence_objects evidence
      ON evidence.tenant_id = p_tenant_id
     AND evidence.evidence_id = backing.evidence_id
    WHERE evidence.evidence_id IS NULL
       OR evidence.reasoning_domain_id IS DISTINCT FROM p_reasoning_domain_id
       OR NOT CASE evidence.visibility_class
         WHEN 'TENANT_SHARED' THEN
           evidence.visibility_user_id IS NULL
           AND evidence.visibility_workspace_id IS NULL
         WHEN 'USER_PRIVATE' THEN
           evidence.visibility_user_id = p_user_id
           AND evidence.visibility_workspace_id IS NULL
         WHEN 'WORKSPACE_SHARED' THEN
           evidence.visibility_user_id IS NULL
           AND evidence.visibility_workspace_id IS NOT NULL
           AND EXISTS (
             SELECT 1 FROM control.workspaces workspace
             WHERE workspace.tenant_id = p_tenant_id
               AND workspace.workspace_id = evidence.visibility_workspace_id
           )
         ELSE false
       END
  ) THEN
    RAISE EXCEPTION 'contribution backing closure is unresolved or unauthorized'
      USING ERRCODE = '42501';
  END IF;

  SELECT count(*) INTO current_backing_count
  FROM generate_subscripts(p_source_ids,1) AS source(i)
  JOIN private.memory_evidence backing
    ON p_source_kinds[source.i] = 'MEMORY'
   AND backing.memory_id = p_source_ids[source.i];
  IF current_backing_count > 2147483647 THEN
    RAISE EXCEPTION 'contribution backing closure is too large' USING ERRCODE = '54000';
  END IF;

  WITH direct_rows AS (
    SELECT source.i-1 AS direct_ordinal,
      p_source_kinds[source.i] AS source_kind,
      p_source_ids[source.i] AS source_id,
      p_source_hashes[source.i] AS source_hash,
      evidence.data_class AS evidence_data_class,
      evidence.reasoning_domain_id AS evidence_domain_id,
      coalesce(evidence.visibility_class,memory.visibility_class) AS visibility_class,
      coalesce(evidence.visibility_user_id,memory.visibility_user_id) AS visibility_user_id,
      coalesce(evidence.visibility_workspace_id,memory.visibility_workspace_id)
        AS visibility_workspace_id,
      memory.status AS memory_status
    FROM generate_subscripts(p_source_ids,1) AS source(i)
    LEFT JOIN private.evidence_objects evidence
      ON p_source_kinds[source.i] = 'EVIDENCE'
     AND evidence.tenant_id = p_tenant_id
     AND evidence.evidence_id = p_source_ids[source.i]
    LEFT JOIN private.memory_records memory
      ON p_source_kinds[source.i] = 'MEMORY'
     AND memory.tenant_id = p_tenant_id
     AND memory.memory_id = p_source_ids[source.i]
  ),
  encoded_rows AS (
    SELECT 1 AS row_tag,direct.direct_ordinal,NULL::uuid AS parent_memory_id,
      direct.source_id AS evidence_id,NULL::text AS backing_role,NULL::integer AS backing_ordinal,
      decode('01','hex')
      || int4send(direct.direct_ordinal)
      || uuid_send(direct.source_id)
      || direct.source_hash
      || int4send(octet_length(convert_to(direct.evidence_data_class,'UTF8')))
      || convert_to(direct.evidence_data_class,'UTF8')
      || uuid_send(direct.evidence_domain_id)
      || int4send(octet_length(convert_to(direct.visibility_class,'UTF8')))
      || convert_to(direct.visibility_class,'UTF8')
      || CASE WHEN direct.visibility_user_id IS NULL
           THEN decode('00','hex') ELSE decode('01','hex')||uuid_send(direct.visibility_user_id)
         END
      || CASE WHEN direct.visibility_workspace_id IS NULL
           THEN decode('00','hex')
           ELSE decode('01','hex')||uuid_send(direct.visibility_workspace_id)
         END AS row_bytes
    FROM direct_rows direct WHERE direct.source_kind = 'EVIDENCE'
    UNION ALL
    SELECT 2,direct.direct_ordinal,direct.source_id,NULL::uuid,NULL::text,NULL::integer,
      decode('02','hex')
      || int4send(direct.direct_ordinal)
      || uuid_send(direct.source_id)
      || direct.source_hash
      || int4send(octet_length(convert_to(direct.memory_status,'UTF8')))
      || convert_to(direct.memory_status,'UTF8')
      || int4send(octet_length(convert_to(direct.visibility_class,'UTF8')))
      || convert_to(direct.visibility_class,'UTF8')
      || CASE WHEN direct.visibility_user_id IS NULL
           THEN decode('00','hex') ELSE decode('01','hex')||uuid_send(direct.visibility_user_id)
         END
      || CASE WHEN direct.visibility_workspace_id IS NULL
           THEN decode('00','hex')
           ELSE decode('01','hex')||uuid_send(direct.visibility_workspace_id)
         END
    FROM direct_rows direct WHERE direct.source_kind = 'MEMORY'
    UNION ALL
    SELECT 3,direct.direct_ordinal,direct.source_id,backing.evidence_id,backing.role,
      backing.ordinal,
      decode('03','hex')
      || int4send(direct.direct_ordinal)
      || uuid_send(direct.source_id)
      || uuid_send(backing.evidence_id)
      || int4send(octet_length(convert_to(backing.role,'UTF8')))
      || convert_to(backing.role,'UTF8')
      || int4send(backing.ordinal)
      || evidence.payload_sha256
      || int4send(octet_length(convert_to(evidence.data_class,'UTF8')))
      || convert_to(evidence.data_class,'UTF8')
      || uuid_send(evidence.reasoning_domain_id)
      || int4send(octet_length(convert_to(evidence.visibility_class,'UTF8')))
      || convert_to(evidence.visibility_class,'UTF8')
      || CASE WHEN evidence.visibility_user_id IS NULL
           THEN decode('00','hex') ELSE decode('01','hex')||uuid_send(evidence.visibility_user_id)
         END
      || CASE WHEN evidence.visibility_workspace_id IS NULL
           THEN decode('00','hex')
           ELSE decode('01','hex')||uuid_send(evidence.visibility_workspace_id)
         END
    FROM direct_rows direct
    JOIN private.memory_evidence backing
      ON direct.source_kind = 'MEMORY' AND backing.memory_id = direct.source_id
    JOIN private.evidence_objects evidence
      ON evidence.tenant_id = p_tenant_id AND evidence.evidence_id = backing.evidence_id
  )
  SELECT
    convert_to('HUMAUX_CONTRIBUTION_SOURCE_BACKING_CLOSURE','UTF8')
    || decode('00','hex') || int2send(1::smallint)
    || uuid_send(p_tenant_id) || uuid_send(p_user_id) || uuid_send(p_reasoning_domain_id)
    || string_agg(
      encoded.row_bytes,decode('','hex') ORDER BY encoded.row_tag,encoded.direct_ordinal,
      encoded.parent_memory_id NULLS FIRST,encoded.evidence_id NULLS FIRST,
      convert_to(encoded.backing_role,'UTF8') NULLS FIRST,
      encoded.backing_ordinal NULLS FIRST
    )
  INTO manifest_bytes
  FROM encoded_rows encoded;

  IF manifest_bytes IS NULL THEN
    RAISE EXCEPTION 'contribution closure encoder produced no direct rows'
      USING ERRCODE = '23514';
  END IF;
  RETURN QUERY SELECT source_len::bigint,current_backing_count::integer,sha256(manifest_bytes);
END;
$$;

CREATE FUNCTION private.contribution_execution_closure_seal_immutable()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  IF ROW(
    OLD.source_backing_closure_version,
    OLD.source_backing_closure_sha256,
    OLD.backing_link_count
  ) IS DISTINCT FROM ROW(
    NEW.source_backing_closure_version,
    NEW.source_backing_closure_sha256,
    NEW.backing_link_count
  ) THEN
    RAISE EXCEPTION 'contribution source/backing closure seal is immutable'
      USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER contribution_execution_closure_seal_immutable
  BEFORE UPDATE ON private.contribution_executions
  FOR EACH ROW EXECUTE FUNCTION private.contribution_execution_closure_seal_immutable();

-- The existing deferred trigger OID remains installed.  Its replacement verifies both the
-- original direct manifest and the V1 source/backing seal through the one canonical helper.
CREATE OR REPLACE FUNCTION private.require_contribution_source_manifest()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  root private.contribution_executions%ROWTYPE;
  target_execution_id uuid;
  actual_count bigint;
  actual_hash bytea;
  actual_ordinals integer[];
  source_kinds text[];
  source_ids uuid[];
  source_hashes bytea[];
  closure_direct_count bigint;
  closure_backing_count integer;
  closure_hash bytea;
BEGIN
  target_execution_id := NEW.execution_id;
  SELECT * INTO root
  FROM private.contribution_executions execution
  WHERE execution.execution_id = target_execution_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'execution source manifest has no durable root' USING ERRCODE = '23503';
  END IF;

  SELECT count(*),array_agg(source.ordinal ORDER BY source.ordinal),
    array_agg(
      CASE WHEN source.evidence_id IS NOT NULL THEN 'EVIDENCE' ELSE 'MEMORY' END
      ORDER BY source.ordinal
    ),
    array_agg(coalesce(source.evidence_id,source.memory_id) ORDER BY source.ordinal),
    array_agg(source.source_hash ORDER BY source.ordinal),
    sha256(convert_to(string_agg(
      (CASE WHEN source.evidence_id IS NOT NULL THEN 'e' ELSE 'm' END)||':'||
      coalesce(source.evidence_id,source.memory_id)::text||':'||encode(source.source_hash,'hex'),
      '|' ORDER BY (CASE WHEN source.evidence_id IS NOT NULL THEN 'e' ELSE 'm' END),
        coalesce(source.evidence_id,source.memory_id)
    ),'UTF8'))
  INTO actual_count,actual_ordinals,source_kinds,source_ids,source_hashes,actual_hash
  FROM private.contribution_execution_sources source
  WHERE source.tenant_id = root.tenant_id
    AND source.execution_id = target_execution_id;

  IF actual_count <> root.source_count
     OR actual_count = 0
     OR actual_ordinals IS DISTINCT FROM ARRAY(
       SELECT generate_series(0,root.source_count-1)
     )
     OR actual_hash IS DISTINCT FROM root.input_manifest_hash THEN
    RAISE EXCEPTION 'execution sources must equal the frozen canonical input manifest'
      USING ERRCODE = '23514';
  END IF;

  SELECT closure.direct_count,closure.backing_link_count,closure.closure_sha256
  INTO closure_direct_count,closure_backing_count,closure_hash
  FROM private.compute_contribution_source_backing_closure_v1(
    root.tenant_id,root.user_id,root.reasoning_domain_id,
    source_kinds,source_ids,source_hashes
  ) closure;

  IF root.source_backing_closure_version IS DISTINCT FROM 1
     OR closure_direct_count IS DISTINCT FROM root.source_count::bigint
     OR closure_backing_count IS DISTINCT FROM root.backing_link_count
     OR closure_hash IS DISTINCT FROM root.source_backing_closure_sha256 THEN
    RAISE EXCEPTION 'execution sources/backing must equal the frozen V1 closure seal'
      USING ERRCODE = '23514';
  END IF;
  RETURN NULL;
END;
$$;

-- Replace the same-signature 0131 command.  The global contribution-input lock is acquired
-- before the per-key idempotency lock and held through root/source insert and deferred checks.
CREATE OR REPLACE FUNCTION private.enqueue_contribution_execution(
  p_tenant_id uuid,
  p_execution_id uuid,
  p_job_id uuid,
  p_coverage_request_id uuid,
  p_assessment_request_id uuid,
  p_candidate_id uuid,
  p_enqueue_idempotency_key text,
  p_enqueue_fingerprint bytea,
  p_user_id uuid,
  p_reasoning_domain_id uuid,
  p_input_manifest_hash bytea,
  p_policy_id uuid,
  p_policy_version bigint,
  p_policy_snapshot jsonb,
  p_rights_basis text,
  p_source_license text,
  p_publisher text,
  p_contributor_attestation text,
  p_redistribution_policy text,
  p_binding_id uuid,
  p_binding_version bigint,
  p_coverage_contract_version bigint,
  p_assessment_contract_version bigint,
  p_coverage_prompt_contract_sha256 bytea,
  p_assessment_prompt_contract_sha256 bytea,
  p_source_kinds text[],
  p_source_ids uuid[],
  p_source_hashes bytea[]
) RETURNS TABLE(
  execution_id uuid,
  job_id uuid,
  coverage_request_id uuid,
  assessment_request_id uuid,
  candidate_id uuid,
  created boolean
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  existing_job ops.jobs%ROWTYPE;
  existing_execution private.contribution_executions%ROWTYPE;
  source_len integer;
  i integer;
  canonical_input_manifest_hash bytea;
  closure_direct_count bigint;
  closure_backing_count integer;
  closure_hash bytea;
BEGIN
  IF p_execution_id IS NULL OR p_job_id IS NULL OR p_coverage_request_id IS NULL
     OR p_assessment_request_id IS NULL OR p_candidate_id IS NULL
     OR p_coverage_request_id = p_assessment_request_id
     OR octet_length(p_enqueue_fingerprint) <> 32
     OR octet_length(p_input_manifest_hash) <> 32
     OR p_enqueue_idempotency_key IS NULL OR btrim(p_enqueue_idempotency_key) = '' THEN
    RAISE EXCEPTION 'invalid contribution execution identities or fingerprint'
      USING ERRCODE = '22023';
  END IF;

  -- This shared lock prevents a memory_evidence INSERT phantom between seal computation and
  -- commit.  Every relevant Evidence/Memory/link mutation already takes the same lock.
  PERFORM ops.lock_contribution_inputs();
  PERFORM set_config('humaux.tenant_id',p_tenant_id::text,true);
  PERFORM set_config('humaux.user_id',p_user_id::text,true);

  PERFORM pg_advisory_xact_lock(hashtextextended(
    'contribution-enqueue:'||p_enqueue_idempotency_key,0
  ));
  SELECT * INTO existing_job
  FROM ops.jobs job
  WHERE job.idempotency_key = p_enqueue_idempotency_key
  FOR UPDATE;
  IF FOUND THEN
    SELECT root.* INTO existing_execution
    FROM ops.contribution_execution_job_links link
    JOIN private.contribution_executions root
      ON (root.tenant_id,root.execution_id) = (link.tenant_id,link.execution_id)
    WHERE link.job_id = existing_job.job_id
    FOR UPDATE OF root;
    IF NOT FOUND OR existing_job.tenant_id <> p_tenant_id
       OR existing_job.job_type <> 'CONTRIBUTION_EXECUTE'
       OR existing_execution.enqueue_idempotency_key <> p_enqueue_idempotency_key THEN
      RAISE EXCEPTION 'idempotency key resolves to an invalid contribution execution chain'
        USING ERRCODE = '23514';
    END IF;
    IF existing_execution.enqueue_fingerprint IS DISTINCT FROM p_enqueue_fingerprint THEN
      RAISE EXCEPTION 'contribution enqueue idempotency fingerprint conflict'
        USING ERRCODE = '23505';
    END IF;
    RETURN QUERY SELECT existing_execution.execution_id,existing_job.job_id,
      existing_execution.coverage_request_id,existing_execution.assessment_request_id,
      existing_execution.candidate_id,false;
    RETURN;
  END IF;

  SELECT closure.direct_count,closure.backing_link_count,closure.closure_sha256
  INTO closure_direct_count,closure_backing_count,closure_hash
  FROM private.compute_contribution_source_backing_closure_v1(
    p_tenant_id,p_user_id,p_reasoning_domain_id,
    p_source_kinds,p_source_ids,p_source_hashes
  ) closure;
  source_len := closure_direct_count::integer;

  SELECT sha256(convert_to(string_agg(
    (CASE p_source_kinds[source.i] WHEN 'EVIDENCE' THEN 'e' ELSE 'm' END)||':'||
    p_source_ids[source.i]::text||':'||encode(p_source_hashes[source.i],'hex'),
    '|' ORDER BY (CASE p_source_kinds[source.i] WHEN 'EVIDENCE' THEN 'e' ELSE 'm' END),
      p_source_ids[source.i]
  ),'UTF8'))
  INTO canonical_input_manifest_hash
  FROM generate_subscripts(p_source_ids,1) AS source(i);
  IF canonical_input_manifest_hash IS DISTINCT FROM p_input_manifest_hash THEN
    RAISE EXCEPTION 'contribution input manifest hash is not canonical'
      USING ERRCODE = '23514';
  END IF;

  INSERT INTO ops.jobs(
    job_id,tenant_id,job_type,status,next_retry_at,idempotency_key,payload
  ) VALUES (
    p_job_id,p_tenant_id,'CONTRIBUTION_EXECUTE','PENDING',clock_timestamp(),
    p_enqueue_idempotency_key,
    jsonb_build_object('schema_version',1,'execution_id',p_execution_id::text)
  );
  INSERT INTO private.contribution_executions(
    execution_id,tenant_id,user_id,enqueue_idempotency_key,enqueue_fingerprint,
    reasoning_domain_id,input_manifest_hash,source_count,
    source_backing_closure_version,source_backing_closure_sha256,backing_link_count,
    policy_id,policy_version,policy_snapshot,rights_basis,source_license,publisher,
    contributor_attestation,redistribution_policy,coverage_request_id,assessment_request_id,
    candidate_id,coverage_contract_version,assessment_contract_version,
    coverage_prompt_contract_sha256,assessment_prompt_contract_sha256,binding_id,binding_version
  ) VALUES (
    p_execution_id,p_tenant_id,p_user_id,p_enqueue_idempotency_key,p_enqueue_fingerprint,
    p_reasoning_domain_id,p_input_manifest_hash,source_len,
    1,closure_hash,closure_backing_count,
    p_policy_id,p_policy_version,p_policy_snapshot,p_rights_basis,p_source_license,p_publisher,
    p_contributor_attestation,p_redistribution_policy,p_coverage_request_id,
    p_assessment_request_id,p_candidate_id,p_coverage_contract_version,
    p_assessment_contract_version,p_coverage_prompt_contract_sha256,
    p_assessment_prompt_contract_sha256,p_binding_id,p_binding_version
  );
  FOR i IN 1..source_len LOOP
    INSERT INTO private.contribution_execution_sources(
      tenant_id,execution_id,ordinal,evidence_id,memory_id,source_hash
    ) VALUES (
      p_tenant_id,p_execution_id,i-1,
      CASE WHEN p_source_kinds[i] = 'EVIDENCE' THEN p_source_ids[i] END,
      CASE WHEN p_source_kinds[i] = 'MEMORY' THEN p_source_ids[i] END,
      p_source_hashes[i]
    );
  END LOOP;
  INSERT INTO ops.contribution_execution_job_links(job_id,tenant_id,execution_id)
    VALUES (p_job_id,p_tenant_id,p_execution_id);
  RETURN QUERY SELECT p_execution_id,p_job_id,p_coverage_request_id,
    p_assessment_request_id,p_candidate_id,true;
END;
$$;

CREATE FUNCTION private.assert_contribution_prepared_route_shape(p_expected_route jsonb)
RETURNS void
LANGUAGE plpgsql
IMMUTABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  required_keys constant text[] := ARRAY[
    'schema_version','tenant_id','binding_id','binding_version','reasoning_domain_id','purpose',
    'route_policy_id','route_policy_version','profile_id','profile_version',
    'provider_account_id','processor_id','processor_model_id','provider_model_id','model_revision',
    'provider_endpoint_id','egress_processor_id','endpoint_ref','region','service_tier',
    'credential_ref','billing_account_id','billing_instrument_id'
  ];
BEGIN
  IF p_expected_route IS NULL
     OR jsonb_typeof(p_expected_route) IS DISTINCT FROM 'object'
     OR (SELECT count(*) FROM jsonb_object_keys(p_expected_route)) <> cardinality(required_keys)
     OR NOT (p_expected_route ?& required_keys)
     OR jsonb_typeof(p_expected_route->'schema_version') IS DISTINCT FROM 'number'
     OR jsonb_typeof(p_expected_route->'tenant_id') IS DISTINCT FROM 'string'
     OR jsonb_typeof(p_expected_route->'binding_id') IS DISTINCT FROM 'string'
     OR jsonb_typeof(p_expected_route->'binding_version') IS DISTINCT FROM 'number'
     OR jsonb_typeof(p_expected_route->'reasoning_domain_id') IS DISTINCT FROM 'string'
     OR jsonb_typeof(p_expected_route->'purpose') IS DISTINCT FROM 'string'
     OR jsonb_typeof(p_expected_route->'route_policy_id') IS DISTINCT FROM 'string'
     OR jsonb_typeof(p_expected_route->'route_policy_version') IS DISTINCT FROM 'number'
     OR jsonb_typeof(p_expected_route->'profile_id') IS DISTINCT FROM 'string'
     OR jsonb_typeof(p_expected_route->'profile_version') IS DISTINCT FROM 'number'
     OR jsonb_typeof(p_expected_route->'provider_account_id') IS DISTINCT FROM 'string'
     OR jsonb_typeof(p_expected_route->'processor_id') IS DISTINCT FROM 'string'
     OR jsonb_typeof(p_expected_route->'processor_model_id') IS DISTINCT FROM 'string'
     OR jsonb_typeof(p_expected_route->'provider_model_id') IS DISTINCT FROM 'string'
     OR jsonb_typeof(p_expected_route->'model_revision') NOT IN ('string','null')
     OR jsonb_typeof(p_expected_route->'provider_endpoint_id') IS DISTINCT FROM 'string'
     OR jsonb_typeof(p_expected_route->'egress_processor_id') IS DISTINCT FROM 'string'
     OR jsonb_typeof(p_expected_route->'endpoint_ref') IS DISTINCT FROM 'string'
     OR jsonb_typeof(p_expected_route->'region') IS DISTINCT FROM 'string'
     OR jsonb_typeof(p_expected_route->'service_tier') IS DISTINCT FROM 'string'
     OR jsonb_typeof(p_expected_route->'credential_ref') IS DISTINCT FROM 'string'
     OR jsonb_typeof(p_expected_route->'billing_account_id') NOT IN ('string','null')
     OR jsonb_typeof(p_expected_route->'billing_instrument_id') NOT IN ('string','null')
     OR (p_expected_route->>'schema_version') IS DISTINCT FROM '1'
     OR (p_expected_route->>'binding_version') !~ '^[1-9][0-9]*$'
     OR (p_expected_route->>'route_policy_version') !~ '^[1-9][0-9]*$'
     OR (p_expected_route->>'profile_version') !~ '^[1-9][0-9]*$'
     OR (p_expected_route->>'purpose') IS DISTINCT FROM 'CONTRIBUTION_DEIDENTIFY'
     OR btrim(p_expected_route->>'processor_id') = ''
     OR btrim(p_expected_route->>'provider_model_id') = ''
     OR btrim(p_expected_route->>'endpoint_ref') = ''
     OR btrim(p_expected_route->>'region') = ''
     OR btrim(p_expected_route->>'service_tier') = '' THEN
    RAISE EXCEPTION 'prepared contribution route must be one exact closed dispatch tuple'
      USING ERRCODE = '22023';
  END IF;

  -- Cast only after the JSON type/closed-key checks, so malformed UUIDs and overflowing
  -- versions fail closed at the wrapper boundary.
  IF (p_expected_route->>'tenant_id')::uuid = '00000000-0000-0000-0000-000000000000'::uuid
     OR (p_expected_route->>'binding_id')::uuid = '00000000-0000-0000-0000-000000000000'::uuid
     OR (p_expected_route->>'binding_version')::bigint <= 0
     OR (p_expected_route->>'reasoning_domain_id')::uuid = '00000000-0000-0000-0000-000000000000'::uuid
     OR (p_expected_route->>'route_policy_id')::uuid = '00000000-0000-0000-0000-000000000000'::uuid
     OR (p_expected_route->>'route_policy_version')::bigint <= 0
     OR (p_expected_route->>'profile_id')::uuid = '00000000-0000-0000-0000-000000000000'::uuid
     OR (p_expected_route->>'profile_version')::bigint <= 0
     OR (p_expected_route->>'provider_account_id')::uuid = '00000000-0000-0000-0000-000000000000'::uuid
     OR (p_expected_route->>'processor_model_id')::uuid = '00000000-0000-0000-0000-000000000000'::uuid
     OR (p_expected_route->>'provider_endpoint_id')::uuid = '00000000-0000-0000-0000-000000000000'::uuid
     OR (p_expected_route->>'egress_processor_id')::uuid = '00000000-0000-0000-0000-000000000000'::uuid
     OR (p_expected_route->>'credential_ref')::uuid = '00000000-0000-0000-0000-000000000000'::uuid
     OR (jsonb_typeof(p_expected_route->'billing_account_id') = 'string'
         AND (p_expected_route->>'billing_account_id')::uuid = '00000000-0000-0000-0000-000000000000'::uuid)
     OR (jsonb_typeof(p_expected_route->'billing_instrument_id') = 'string'
         AND (p_expected_route->>'billing_instrument_id')::uuid = '00000000-0000-0000-0000-000000000000'::uuid) THEN
    RAISE EXCEPTION 'prepared contribution route contains an invalid dispatch identity'
      USING ERRCODE = '22023';
  END IF;
END;
$$;

CREATE FUNCTION private.assert_current_contribution_reservation_authority(
  p_call ops.model_call_ledger
)
RETURNS void
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  root private.contribution_executions%ROWTYPE;
  root_ids uuid[];
  root_match_count bigint;
  admission record;
  expected_route jsonb;
  current_route jsonb;
  current_source_count bigint;
  current_manifest_hash bytea;
  current_ordinals integer[];
  current_source_kinds text[];
  current_source_ids uuid[];
  current_source_hashes bytea[];
  current_backing_count integer;
  current_closure_hash bytea;
BEGIN
  -- This lock is also taken by tenant/user/membership/domain/source/policy input mutations.
  -- It is released at commit, before any provider I/O.
  PERFORM ops.lock_contribution_inputs();

  SELECT array_agg(execution.execution_id ORDER BY execution.execution_id), count(*)
  INTO root_ids, root_match_count
  FROM private.contribution_executions execution
  WHERE execution.tenant_id = p_call.tenant_id
    AND (execution.coverage_request_id = p_call.request_id
         OR execution.assessment_request_id = p_call.request_id);

  -- The pre-0131 synchronous reasoner path has no execution root and remains compatible.
  IF root_match_count = 0 THEN
    RETURN;
  END IF;

  -- The two request columns are each UNIQUE, but a coverage request on one root may
  -- equal an assessment request on another. Never select an arbitrary root.
  IF root_match_count <> 1 THEN
    RAISE EXCEPTION 'contribution reservation request maps to multiple execution roots'
      USING ERRCODE = '23514';
  END IF;

  SELECT * INTO STRICT root
  FROM private.contribution_executions execution
  WHERE execution.execution_id = root_ids[1]
  FOR UPDATE;

  PERFORM set_config('humaux.tenant_id', root.tenant_id::text, true);
  PERFORM set_config('humaux.user_id', root.user_id::text, true);

  IF NOT EXISTS (
    SELECT 1
    FROM control.tenants tenant
    JOIN control.memberships membership ON membership.tenant_id = tenant.tenant_id
    JOIN control.users app_user ON app_user.user_id = membership.user_id
    WHERE tenant.tenant_id = root.tenant_id
      AND membership.user_id = root.user_id
      AND tenant.state = 'ACTIVE'
      AND membership.state = 'ACTIVE'
      AND app_user.state = 'ACTIVE'
  ) THEN
    RAISE EXCEPTION 'contribution reservation tenant, user, or membership is not active'
      USING ERRCODE = '42501';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM control.private_reasoning_domains domain
    WHERE domain.tenant_id = root.tenant_id
      AND domain.reasoning_domain_id = root.reasoning_domain_id
      AND domain.owner_user_id = root.user_id
      AND domain.status = 'ACTIVE'
  ) THEN
    RAISE EXCEPTION 'contribution reservation domain is not active and self-owned'
      USING ERRCODE = '42501';
  END IF;

  IF root.source_backing_closure_version IS DISTINCT FROM 1
     OR root.source_backing_closure_sha256 IS NULL
     OR root.backing_link_count IS NULL THEN
    RAISE EXCEPTION 'execution root lacks a reservable source/backing closure seal'
      USING ERRCODE = '23514';
  END IF;

  SELECT count(*),array_agg(source.ordinal ORDER BY source.ordinal),
    array_agg(
      CASE WHEN source.evidence_id IS NOT NULL THEN 'EVIDENCE' ELSE 'MEMORY' END
      ORDER BY source.ordinal
    ),
    array_agg(coalesce(source.evidence_id,source.memory_id) ORDER BY source.ordinal),
    array_agg(source.source_hash ORDER BY source.ordinal),
    sha256(convert_to(string_agg(
    (CASE WHEN source.evidence_id IS NOT NULL THEN 'e' ELSE 'm' END)||':'||
    coalesce(source.evidence_id,source.memory_id)::text||':'||encode(source.source_hash,'hex'),
    '|' ORDER BY (CASE WHEN source.evidence_id IS NOT NULL THEN 'e' ELSE 'm' END),
      coalesce(source.evidence_id,source.memory_id)
  ),'UTF8'))
  INTO current_source_count,current_ordinals,current_source_kinds,current_source_ids,
    current_source_hashes,current_manifest_hash
  FROM private.contribution_execution_sources source
  WHERE source.tenant_id = root.tenant_id
    AND source.execution_id = root.execution_id;

  IF current_source_count <> root.source_count
     OR current_source_count = 0
     OR current_ordinals IS DISTINCT FROM ARRAY(
       SELECT generate_series(0,root.source_count-1)
     )
     OR current_manifest_hash IS DISTINCT FROM root.input_manifest_hash THEN
    RAISE EXCEPTION 'contribution reservation sources do not equal the frozen manifest'
      USING ERRCODE = '23514';
  END IF;

  SELECT closure.direct_count,closure.backing_link_count,closure.closure_sha256
  INTO current_source_count,current_backing_count,current_closure_hash
  FROM private.compute_contribution_source_backing_closure_v1(
    root.tenant_id,root.user_id,root.reasoning_domain_id,
    current_source_kinds,current_source_ids,current_source_hashes
  ) closure;
  IF current_source_count IS DISTINCT FROM root.source_count::bigint
     OR current_backing_count IS DISTINCT FROM root.backing_link_count
     OR current_closure_hash IS DISTINCT FROM root.source_backing_closure_sha256 THEN
    RAISE EXCEPTION 'contribution reservation source/backing closure differs from root seal'
      USING ERRCODE = '23514';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM control.contribution_policies policy
    WHERE policy.tenant_id = root.tenant_id
      AND policy.policy_id = root.policy_id
      AND policy.policy_version = root.policy_version
      AND policy.effective_to IS NULL
      AND policy.allow_public_contribution
      AND policy.contribution_mode = 'MANUAL'
  ) THEN
    RAISE EXCEPTION 'contribution reservation policy is not the enabled MANUAL open head'
      USING ERRCODE = '42501';
  END IF;

  IF (root.coverage_request_id = p_call.request_id
      AND (p_call.call_kind IS DISTINCT FROM 'COVERAGE_PROBE'
           OR root.state IS DISTINCT FROM 'READY_A'))
     OR (root.assessment_request_id = p_call.request_id
      AND (p_call.call_kind IS DISTINCT FROM 'TYPED_ASSESSMENT'
           OR root.state IS DISTINCT FROM 'READY_B')) THEN
    RAISE EXCEPTION 'contribution reservation request does not match its current root stage'
      USING ERRCODE = '23514';
  END IF;

  BEGIN
    expected_route := NULLIF(
      current_setting('humaux.contribution_prepared_route', true), ''
    )::jsonb;
  EXCEPTION WHEN others THEN
    RAISE EXCEPTION 'contribution reservation lacks a valid prepared route expectation'
      USING ERRCODE = '22023';
  END;
  PERFORM private.assert_contribution_prepared_route_shape(expected_route);

  SELECT * INTO admission
  FROM control.resolve_user_reasoning_admission(
    root.binding_id,
    root.binding_version,
    root.reasoning_domain_id,
    'CONTRIBUTION_DEIDENTIFY'
  );
  IF NOT FOUND OR admission.tenant_id IS DISTINCT FROM root.tenant_id THEN
    RAISE EXCEPTION 'contribution reservation frozen Binding is not currently admissible'
      USING ERRCODE = '42501';
  END IF;

  current_route := jsonb_build_object(
    'schema_version', 1,
    'tenant_id', admission.tenant_id,
    'binding_id', admission.binding_id,
    'binding_version', admission.binding_version,
    'reasoning_domain_id', admission.reasoning_domain_id,
    'purpose', admission.purpose,
    'route_policy_id', admission.route_policy_id,
    'route_policy_version', admission.route_policy_version,
    'profile_id', admission.profile_id,
    'profile_version', admission.profile_version,
    'provider_account_id', admission.provider_account_id,
    'processor_id', admission.processor_id,
    'processor_model_id', admission.processor_model_id,
    'provider_model_id', admission.provider_model_id,
    'model_revision', admission.model_revision,
    'provider_endpoint_id', admission.provider_endpoint_id,
    'egress_processor_id', admission.egress_processor_id,
    'endpoint_ref', admission.endpoint_ref,
    'region', admission.region,
    'service_tier', admission.service_tier,
    'credential_ref', admission.credential_ref,
    'billing_account_id', admission.billing_account_id,
    'billing_instrument_id', admission.billing_instrument_id
  );

  IF current_route IS DISTINCT FROM expected_route
     OR p_call.tenant_id IS DISTINCT FROM admission.tenant_id
     OR p_call.reasoning_domain_id IS DISTINCT FROM root.reasoning_domain_id
     OR p_call.binding_id IS DISTINCT FROM root.binding_id
     OR p_call.binding_version IS DISTINCT FROM root.binding_version
     OR p_call.route_policy_id IS DISTINCT FROM admission.route_policy_id
     OR p_call.route_policy_version IS DISTINCT FROM admission.route_policy_version
     OR p_call.profile_id IS DISTINCT FROM admission.profile_id
     OR p_call.profile_version IS DISTINCT FROM admission.profile_version
     OR p_call.provider_account_id IS DISTINCT FROM admission.provider_account_id
     OR p_call.provider IS DISTINCT FROM admission.processor_id
     OR p_call.model IS DISTINCT FROM admission.provider_model_id
     OR p_call.model_revision IS DISTINCT FROM admission.model_revision
     OR p_call.provider_endpoint_id IS DISTINCT FROM admission.provider_endpoint_id
     OR p_call.egress_processor_id IS DISTINCT FROM admission.egress_processor_id
     OR p_call.credential_ref IS DISTINCT FROM admission.credential_ref
     OR p_call.billing_account_id IS DISTINCT FROM admission.billing_account_id
     OR p_call.billing_instrument_id IS DISTINCT FROM admission.billing_instrument_id
     OR p_call.provider_health_observation_id IS DISTINCT FROM
        admission.provider_health_observation_id
     OR p_call.account_health_observation_id IS DISTINCT FROM
        admission.account_health_observation_id THEN
    RAISE EXCEPTION 'contribution reservation route differs from current or prepared authority'
      USING ERRCODE = '40001';
  END IF;
END;
$$;

CREATE FUNCTION ops.contribution_reservation_authority_validate()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  IF NEW.purpose = 'CONTRIBUTION_DEIDENTIFY' THEN
    PERFORM private.assert_current_contribution_reservation_authority(NEW);
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER contribution_reservation_authority_validate
  BEFORE INSERT ON ops.model_call_ledger
  FOR EACH ROW
  WHEN (NEW.purpose = 'CONTRIBUTION_DEIDENTIFY')
  EXECUTE FUNCTION ops.contribution_reservation_authority_validate();

CREATE FUNCTION private.reserve_contribution_a(
  p_tenant_id uuid,p_execution_id uuid,p_job_id uuid,p_lease_owner text,p_attempt integer,
  p_model_call_id uuid,p_disclosure_id uuid,p_intent_sha256 bytea,p_grant_id uuid,
  p_scope_kind text,p_scope_id uuid,p_data_class text,p_payload_sha256 bytea,p_payload_bytes bigint,
  p_expected_route jsonb
) RETURNS TABLE(
  model_call_id uuid,disclosure_id uuid,request_id uuid,processor_id text,
  provider_model_id text,model_revision text,egress_processor_id uuid,
  credential_ref uuid,newly_reserved boolean
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  PERFORM ops.lock_contribution_inputs();
  PERFORM private.assert_contribution_prepared_route_shape(p_expected_route);
  PERFORM set_config('humaux.contribution_prepared_route', p_expected_route::text, true);
  RETURN QUERY SELECT reservation.*
  FROM private.reserve_contribution_execution_call(
    p_tenant_id,p_execution_id,p_job_id,p_lease_owner,p_attempt,'COVERAGE_PROBE',
    p_model_call_id,p_disclosure_id,p_intent_sha256,p_grant_id,p_scope_kind,p_scope_id,
    p_data_class,p_payload_sha256,p_payload_bytes
  ) reservation;
END;
$$;

CREATE FUNCTION private.reserve_contribution_b(
  p_tenant_id uuid,p_execution_id uuid,p_job_id uuid,p_lease_owner text,p_attempt integer,
  p_model_call_id uuid,p_disclosure_id uuid,p_intent_sha256 bytea,p_grant_id uuid,
  p_scope_kind text,p_scope_id uuid,p_data_class text,p_payload_sha256 bytea,p_payload_bytes bigint,
  p_expected_route jsonb
) RETURNS TABLE(
  model_call_id uuid,disclosure_id uuid,request_id uuid,processor_id text,
  provider_model_id text,model_revision text,egress_processor_id uuid,
  credential_ref uuid,newly_reserved boolean
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  PERFORM ops.lock_contribution_inputs();
  PERFORM private.assert_contribution_prepared_route_shape(p_expected_route);
  PERFORM set_config('humaux.contribution_prepared_route', p_expected_route::text, true);
  RETURN QUERY SELECT reservation.*
  FROM private.reserve_contribution_execution_call(
    p_tenant_id,p_execution_id,p_job_id,p_lease_owner,p_attempt,'TYPED_ASSESSMENT',
    p_model_call_id,p_disclosure_id,p_intent_sha256,p_grant_id,p_scope_kind,p_scope_id,
    p_data_class,p_payload_sha256,p_payload_bytes
  ) reservation;
END;
$$;

ALTER FUNCTION private.compute_contribution_source_backing_closure_v1(
  uuid,uuid,uuid,text[],uuid[],bytea[]
) OWNER TO role_migration_owner;
ALTER FUNCTION private.contribution_execution_closure_seal_immutable()
  OWNER TO role_migration_owner;
ALTER FUNCTION private.require_contribution_source_manifest()
  OWNER TO role_migration_owner;
ALTER FUNCTION private.enqueue_contribution_execution(
  uuid,uuid,uuid,uuid,uuid,uuid,text,bytea,uuid,uuid,bytea,uuid,bigint,jsonb,
  text,text,text,text,text,uuid,bigint,bigint,bigint,bytea,bytea,text[],uuid[],bytea[]
) OWNER TO role_migration_owner;
ALTER FUNCTION private.assert_contribution_prepared_route_shape(jsonb)
  OWNER TO role_migration_owner;
ALTER FUNCTION private.assert_current_contribution_reservation_authority(ops.model_call_ledger)
  OWNER TO role_migration_owner;
ALTER FUNCTION ops.contribution_reservation_authority_validate()
  OWNER TO role_migration_owner;
ALTER FUNCTION private.reserve_contribution_a(
  uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint,jsonb
) OWNER TO role_migration_owner;
ALTER FUNCTION private.reserve_contribution_b(
  uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint,jsonb
) OWNER TO role_migration_owner;

REVOKE ALL ON FUNCTION
  private.compute_contribution_source_backing_closure_v1(
    uuid,uuid,uuid,text[],uuid[],bytea[]
  ),
  private.contribution_execution_closure_seal_immutable(),
  private.require_contribution_source_manifest(),
  private.assert_contribution_prepared_route_shape(jsonb),
  private.assert_current_contribution_reservation_authority(ops.model_call_ledger),
  ops.contribution_reservation_authority_validate(),
  private.reserve_contribution_a(
    uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint,jsonb
  ),
  private.reserve_contribution_b(
    uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint,jsonb
  )
  FROM PUBLIC,role_admin,role_gateway,role_private_worker,role_consolidation_worker,
    role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;

REVOKE ALL ON FUNCTION private.enqueue_contribution_execution(
  uuid,uuid,uuid,uuid,uuid,uuid,text,bytea,uuid,uuid,bytea,uuid,bigint,jsonb,
  text,text,text,text,text,uuid,bigint,bigint,bigint,bytea,bytea,text[],uuid[],bytea[]
) FROM PUBLIC,role_admin,role_gateway,role_private_worker,role_consolidation_worker,
  role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;

REVOKE EXECUTE ON FUNCTION
  private.reserve_contribution_a(
    uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint
  ),
  private.reserve_contribution_b(
    uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint
  )
  FROM role_private_worker;

GRANT EXECUTE ON FUNCTION
  private.enqueue_contribution_execution(
    uuid,uuid,uuid,uuid,uuid,uuid,text,bytea,uuid,uuid,bytea,uuid,bigint,jsonb,
    text,text,text,text,text,uuid,bigint,bigint,bigint,bytea,bytea,text[],uuid[],bytea[]
  ),
  private.reserve_contribution_a(
    uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint,jsonb
  ),
  private.reserve_contribution_b(
    uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint,jsonb
  )
  TO role_private_worker;
