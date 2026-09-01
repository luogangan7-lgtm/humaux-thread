-- Phase 9 G80-45: keep legacy receipt material owner-only while preserving the two
-- trusted trigger paths and runtime hydration through an owner-rights relational SSOT.

DO $$
BEGIN
  IF to_regprocedure('public.require_trust_root_seal()') IS NULL
    OR to_regprocedure('public.guard_evaluated_source_identity()') IS NULL
    OR to_regprocedure('public.public_receipt_matches(uuid,uuid,uuid,bigint,jsonb)') IS NULL THEN
    RAISE EXCEPTION '0135 requires the 0106 legacy receipt functions' USING ERRCODE='55000';
  END IF;

  IF NOT EXISTS(
    SELECT 1 FROM pg_trigger trigger_row
    WHERE trigger_row.tgrelid='public.claim_trust_evaluations'::regclass
      AND trigger_row.tgname='trust_roots_complete' AND trigger_row.tgenabled='O'
      AND trigger_row.tgfoid='public.require_trust_root_seal()'::regprocedure
  ) OR NOT EXISTS(
    SELECT 1 FROM pg_trigger trigger_row
    WHERE trigger_row.tgrelid='public.claim_trust_evaluation_sources'::regclass
      AND trigger_row.tgname='trust_roots_sealed' AND trigger_row.tgenabled='O'
      AND trigger_row.tgfoid='public.require_trust_root_seal()'::regprocedure
  ) THEN
    RAISE EXCEPTION '0135 requires both deferred legacy root-seal triggers' USING ERRCODE='55000';
  END IF;

  IF (SELECT prosecdef FROM pg_proc
      WHERE oid='public.require_trust_root_seal()'::regprocedure)
    OR (SELECT prosecdef FROM pg_proc
      WHERE oid='public.guard_evaluated_source_identity()'::regprocedure) THEN
    RAISE EXCEPTION '0135 narrow trigger allowlist precondition failed' USING ERRCODE='55000';
  END IF;
END;
$$;

-- This view is the sole relational definition of the exact 0106 legacy receipt predicate.
-- It exposes only the tuple needed by its two trusted consumers, never runtime identity.
CREATE VIEW public._legacy_receipt_match_basis
WITH (security_barrier=true,security_invoker=false) AS
  SELECT evaluation.claim_id,evaluation.synthesis_id,evaluation.evaluation_id,
    evaluation.object_revision,evaluation.body_sha256,evaluation.moderation_state
  FROM public.claim_trust_evaluations evaluation
  WHERE EXISTS(
      SELECT 1 FROM public.claim_trust_evaluation_sources receipt_root
      WHERE receipt_root.evaluation_id=evaluation.evaluation_id
    )
    AND evaluation.support_count=(
      SELECT count(*) FROM public.claim_trust_evaluation_sources receipt_root
      WHERE receipt_root.evaluation_id=evaluation.evaluation_id
    )
    AND NOT EXISTS(
      SELECT receipt_root.root_source_id
      FROM public.claim_trust_evaluation_sources receipt_root
      WHERE receipt_root.evaluation_id=evaluation.evaluation_id
      EXCEPT
      SELECT graph_root.root_source_id
      FROM public.current_public_roots(evaluation.claim_id,evaluation.synthesis_id) graph_root
    )
    AND NOT EXISTS(
      SELECT graph_root.root_source_id
      FROM public.current_public_roots(evaluation.claim_id,evaluation.synthesis_id) graph_root
      EXCEPT
      SELECT receipt_root.root_source_id
      FROM public.claim_trust_evaluation_sources receipt_root
      WHERE receipt_root.evaluation_id=evaluation.evaluation_id
    )
    AND NOT EXISTS(
      SELECT receipt_root.root_source_id
      FROM public.claim_trust_evaluation_sources receipt_root
      WHERE receipt_root.evaluation_id=evaluation.evaluation_id
      EXCEPT
      SELECT closure_root.root_source_id FROM public.source_closure closure_root
      WHERE closure_root.is_current
        AND closure_root.claim_id IS NOT DISTINCT FROM evaluation.claim_id
        AND closure_root.synthesis_id IS NOT DISTINCT FROM evaluation.synthesis_id
    )
    AND NOT EXISTS(
      SELECT closure_root.root_source_id FROM public.source_closure closure_root
      WHERE closure_root.is_current
        AND closure_root.claim_id IS NOT DISTINCT FROM evaluation.claim_id
        AND closure_root.synthesis_id IS NOT DISTINCT FROM evaluation.synthesis_id
      EXCEPT
      SELECT receipt_root.root_source_id
      FROM public.claim_trust_evaluation_sources receipt_root
      WHERE receipt_root.evaluation_id=evaluation.evaluation_id
    )
    AND NOT EXISTS(
      SELECT 1 FROM public.claim_trust_evaluation_sources receipt_root
      JOIN public.sources source ON source.source_id=receipt_root.root_source_id
      WHERE receipt_root.evaluation_id=evaluation.evaluation_id
        AND (receipt_root.source_content_hash IS DISTINCT FROM source.content_hash
          OR receipt_root.contribution_release_id IS DISTINCT FROM source.contribution_release_id)
    )
    AND NOT EXISTS(
      SELECT 1 FROM public.claim_trust_evaluation_sources receipt_root
      JOIN ops.public_release_revocations revocation
        ON revocation.release_id=receipt_root.contribution_release_id
      WHERE receipt_root.evaluation_id=evaluation.evaluation_id
    );

ALTER VIEW public._legacy_receipt_match_basis OWNER TO role_migration_owner;
REVOKE ALL ON public._legacy_receipt_match_basis
  FROM PUBLIC,role_admin,role_gateway,role_private_worker,role_consolidation_worker,
    role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;

CREATE OR REPLACE FUNCTION public.public_receipt_matches(target_claim uuid,
  target_synthesis uuid,receipt uuid,revision bigint,body jsonb)
RETURNS boolean LANGUAGE sql STABLE SECURITY INVOKER
SET search_path TO pg_catalog, pg_temp AS $$
  SELECT EXISTS(
    SELECT 1 FROM public._legacy_receipt_match_basis receipt_basis
    WHERE receipt_basis.claim_id IS NOT DISTINCT FROM target_claim
      AND receipt_basis.synthesis_id IS NOT DISTINCT FROM target_synthesis
      AND receipt_basis.evaluation_id=receipt
      AND receipt_basis.object_revision=revision
      AND receipt_basis.body_sha256=sha256(convert_to(body::text,'UTF8'))
  )
$$;
ALTER FUNCTION public.public_receipt_matches(uuid,uuid,uuid,bigint,jsonb)
  OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION public.public_receipt_matches(uuid,uuid,uuid,bigint,jsonb)
  FROM PUBLIC,role_admin,role_gateway,role_private_worker,role_consolidation_worker,
    role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;

-- The legacy branches consume the basis directly so owner-rights rewriting covers the
-- protected receipt relations. The identity-free anonymous branch remains unchanged.
CREATE OR REPLACE VIEW public.eligible_objects
WITH (security_barrier=true,security_invoker=false) AS
  SELECT claim.claim_id AS object_id,'CLAIM'::text AS object_kind,claim.content,
    claim.object_revision,claim.current_evaluation_id
  FROM public.claims claim
  JOIN public._legacy_receipt_match_basis receipt_basis
    ON receipt_basis.claim_id=claim.claim_id
   AND receipt_basis.synthesis_id IS NULL
   AND receipt_basis.evaluation_id=claim.current_evaluation_id
   AND receipt_basis.object_revision=claim.object_revision
   AND receipt_basis.body_sha256=sha256(convert_to(claim.content::text,'UTF8'))
  WHERE claim.current_anonymous_trust_receipt_id IS NULL
    AND claim.moderation_state='SUPPORTED'
    AND receipt_basis.moderation_state='SUPPORTED'
  UNION ALL
  SELECT claim.claim_id,'CLAIM'::text,claim.content,claim.object_revision,
    claim.current_anonymous_trust_receipt_id AS current_evaluation_id
  FROM public.claims claim
  JOIN public.anonymous_claim_trust_receipts receipt
    ON receipt.anonymous_trust_receipt_id=claim.current_anonymous_trust_receipt_id
  WHERE claim.current_evaluation_id IS NULL AND claim.moderation_state='SUPPORTED'
    AND receipt.moderation_state='SUPPORTED'
    AND public.anonymous_trust_receipt_matches(claim.claim_id,
      claim.current_anonymous_trust_receipt_id,claim.object_revision,claim.content)
  UNION ALL
  SELECT synthesis.synthesis_id,'SYNTHESIS'::text,synthesis.content,
    synthesis.object_revision,synthesis.current_evaluation_id
  FROM public.syntheses synthesis
  JOIN public._legacy_receipt_match_basis receipt_basis
    ON receipt_basis.claim_id IS NULL
   AND receipt_basis.synthesis_id=synthesis.synthesis_id
   AND receipt_basis.evaluation_id=synthesis.current_evaluation_id
   AND receipt_basis.object_revision=synthesis.object_revision
   AND receipt_basis.body_sha256=sha256(convert_to(synthesis.content::text,'UTF8'))
  WHERE synthesis.moderation_state='SUPPORTED'
    AND receipt_basis.moderation_state='SUPPORTED';
ALTER VIEW public.eligible_objects OWNER TO role_migration_owner;
REVOKE ALL ON public.eligible_objects
  FROM PUBLIC,role_admin,role_gateway,role_private_worker,role_consolidation_worker,
    role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;
GRANT SELECT ON public.eligible_objects
  TO role_gateway,role_public_worker,role_retrieval_worker,role_maintenance;

ALTER FUNCTION public.require_trust_root_seal() OWNER TO role_migration_owner;
ALTER FUNCTION public.require_trust_root_seal() SECURITY DEFINER;
ALTER FUNCTION public.require_trust_root_seal() SET search_path TO pg_catalog, pg_temp;
ALTER FUNCTION public.guard_evaluated_source_identity() OWNER TO role_migration_owner;
ALTER FUNCTION public.guard_evaluated_source_identity() SECURITY DEFINER;
ALTER FUNCTION public.guard_evaluated_source_identity() SET search_path TO pg_catalog, pg_temp;

-- Installed triggers can fire for legal DML, but no runtime role can call either function.
REVOKE ALL ON FUNCTION public.require_trust_root_seal(),
  public.guard_evaluated_source_identity()
  FROM PUBLIC,role_admin,role_gateway,role_private_worker,role_consolidation_worker,
    role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;
