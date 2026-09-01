-- Phase 9 G70-1 / G80-45: anonymous trust authority stays protected while the public
-- plane receives only an identity-free, append-only receipt.  Legacy trust receipts remain
-- valid for legacy roots, but cannot become current for an anonymous-root claim.

ALTER TABLE public.claims
  ADD COLUMN current_anonymous_trust_receipt_id uuid;

CREATE TABLE control.anonymous_claim_trust_authorities (
  anonymous_trust_authority_id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  claim_id uuid NOT NULL REFERENCES public.claims(claim_id),
  object_revision bigint NOT NULL CHECK(object_revision>0),
  body_sha256 bytea NOT NULL CHECK(octet_length(body_sha256)=32),
  evaluator_user_id uuid NOT NULL REFERENCES control.users(user_id),
  evaluator_grant_version bigint NOT NULL CHECK(evaluator_grant_version>0),
  rationale text NOT NULL CHECK(btrim(rationale)<>''),
  policy_version text NOT NULL CHECK(btrim(policy_version)<>''),
  moderation_state text NOT NULL
    CHECK(moderation_state IN ('UNDER_REVIEW','SUPPORTED','QUARANTINED')),
  independence_attestation_id uuid NOT NULL
    REFERENCES public.claim_independence_attestations(attestation_id),
  public_receipt_id uuid NOT NULL,
  review_commitment bytea NOT NULL UNIQUE CHECK(octet_length(review_commitment)=32),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(public_receipt_id),
  UNIQUE(claim_id,object_revision)
);

CREATE TABLE public.anonymous_claim_trust_receipts (
  anonymous_trust_receipt_id uuid PRIMARY KEY,
  claim_id uuid NOT NULL REFERENCES public.claims(claim_id),
  object_revision bigint NOT NULL CHECK(object_revision>0),
  body_sha256 bytea NOT NULL CHECK(octet_length(body_sha256)=32),
  policy_version text NOT NULL CHECK(btrim(policy_version)<>''),
  moderation_state text NOT NULL
    CHECK(moderation_state IN ('UNDER_REVIEW','SUPPORTED','QUARANTINED')),
  independence_attestation_id uuid NOT NULL
    REFERENCES public.claim_independence_attestations(attestation_id),
  root_set_sha256 bytea NOT NULL CHECK(octet_length(root_set_sha256)=32),
  support_count integer NOT NULL CHECK(support_count>0),
  independent_support_count integer NOT NULL
    CHECK(independent_support_count>0 AND independent_support_count<=support_count),
  sybil_risk text NOT NULL
    CHECK(sybil_risk IN ('CONCENTRATED','NO_CONCENTRATION_OBSERVED')),
  checks_complete boolean NOT NULL CHECK(checks_complete),
  review_commitment bytea NOT NULL UNIQUE CHECK(octet_length(review_commitment)=32),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(claim_id,object_revision),
  CHECK((sybil_risk='NO_CONCENTRATION_OBSERVED')=
    (independent_support_count=support_count))
);

ALTER TABLE control.anonymous_claim_trust_authorities
  ADD CONSTRAINT anonymous_trust_authority_public_receipt_fk
  FOREIGN KEY(public_receipt_id)
  REFERENCES public.anonymous_claim_trust_receipts(anonymous_trust_receipt_id)
  DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE public.claims
  ADD CONSTRAINT claim_current_anonymous_trust_receipt_fk
  FOREIGN KEY(current_anonymous_trust_receipt_id)
  REFERENCES public.anonymous_claim_trust_receipts(anonymous_trust_receipt_id)
  DEFERRABLE INITIALLY DEFERRED,
  ADD CONSTRAINT claim_current_trust_pointer_disjoint
  CHECK(current_evaluation_id IS NULL OR current_anonymous_trust_receipt_id IS NULL),
  ADD CONSTRAINT claim_supported_exactly_one_trust_pointer
  CHECK(moderation_state<>'SUPPORTED' OR
    ((current_evaluation_id IS NULL)<>(current_anonymous_trust_receipt_id IS NULL)));

ALTER TABLE control.anonymous_claim_trust_authorities ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.anonymous_claim_trust_authorities FORCE ROW LEVEL SECURITY;
CREATE POLICY anonymous_claim_trust_authority_tenant_isolation
  ON control.anonymous_claim_trust_authorities
  USING(tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid)
  WITH CHECK(tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid);
-- FORCE RLS also applies to the table owner.  Only the narrow owner functions below use this
-- policy; no runtime role receives direct table privileges.
CREATE POLICY anonymous_claim_trust_authority_owner_read
  ON control.anonymous_claim_trust_authorities FOR SELECT TO role_migration_owner
  USING(true);

CREATE FUNCTION control.guard_anonymous_claim_trust_authority_append_only()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
  RAISE EXCEPTION 'anonymous claim trust authorities are append-only' USING ERRCODE='23514';
END;
$$;
CREATE TRIGGER anonymous_claim_trust_authority_append_only
  BEFORE UPDATE OR DELETE ON control.anonymous_claim_trust_authorities
  FOR EACH ROW EXECUTE FUNCTION control.guard_anonymous_claim_trust_authority_append_only();

CREATE FUNCTION public.guard_anonymous_claim_trust_receipt_append_only()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
  RAISE EXCEPTION 'anonymous claim trust receipts are append-only' USING ERRCODE='23514';
END;
$$;
CREATE TRIGGER anonymous_claim_trust_receipt_append_only
  BEFORE UPDATE OR DELETE ON public.anonymous_claim_trust_receipts
  FOR EACH ROW EXECUTE FUNCTION public.guard_anonymous_claim_trust_receipt_append_only();

CREATE FUNCTION public.require_anonymous_receipt_authority()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF NOT EXISTS(
    SELECT 1 FROM control.anonymous_claim_trust_authorities authority
    WHERE authority.public_receipt_id=NEW.anonymous_trust_receipt_id
      AND authority.claim_id=NEW.claim_id
      AND authority.object_revision=NEW.object_revision
      AND authority.body_sha256=NEW.body_sha256
      AND authority.policy_version=NEW.policy_version
      AND authority.moderation_state=NEW.moderation_state
      AND authority.independence_attestation_id=NEW.independence_attestation_id
      AND authority.review_commitment=NEW.review_commitment
  ) THEN
    RAISE EXCEPTION 'anonymous trust receipt requires one matching protected authority'
      USING ERRCODE='23514';
  END IF;
  RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER anonymous_receipt_authority_complete
  AFTER INSERT ON public.anonymous_claim_trust_receipts
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
  EXECUTE FUNCTION public.require_anonymous_receipt_authority();

CREATE FUNCTION public.anonymous_trust_receipt_matches(
  p_claim_id uuid,p_receipt_id uuid,p_revision bigint,p_body jsonb
) RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
  SELECT EXISTS(
    SELECT 1
    FROM public.anonymous_claim_trust_receipts receipt
    JOIN public.claim_independence_attestations attestation
      ON attestation.attestation_id=receipt.independence_attestation_id
    WHERE receipt.anonymous_trust_receipt_id=p_receipt_id
      AND receipt.claim_id=p_claim_id
      AND receipt.object_revision=p_revision
      AND receipt.body_sha256=sha256(convert_to(p_body::text,'UTF8'))
      AND receipt.moderation_state='SUPPORTED'
      AND receipt.checks_complete
      AND receipt.claim_id=attestation.claim_id
      AND receipt.object_revision=attestation.object_revision
      AND receipt.body_sha256=attestation.body_sha
      AND receipt.root_set_sha256=attestation.root_set_sha
      AND receipt.support_count=attestation.support_count
      AND receipt.independent_support_count=attestation.independent_count
      AND receipt.sybil_risk=attestation.sybil_risk
      AND attestation.checks_complete
      AND EXISTS(
        SELECT 1 FROM control.anonymous_claim_trust_authorities authority
        JOIN control.public_moderator_grants moderator
          ON moderator.user_id=authority.evaluator_user_id
         AND moderator.grant_version=authority.evaluator_grant_version
        JOIN control.users evaluator ON evaluator.user_id=authority.evaluator_user_id
        JOIN control.memberships membership
          ON membership.user_id=authority.evaluator_user_id
         AND membership.tenant_id=authority.tenant_id
        WHERE authority.public_receipt_id=receipt.anonymous_trust_receipt_id
          AND authority.claim_id=receipt.claim_id
          AND authority.object_revision=receipt.object_revision
          AND authority.body_sha256=receipt.body_sha256
          AND authority.policy_version=receipt.policy_version
          AND authority.moderation_state=receipt.moderation_state
          AND authority.independence_attestation_id=receipt.independence_attestation_id
          AND authority.review_commitment=receipt.review_commitment
          AND moderator.enabled AND evaluator.state='ACTIVE' AND membership.state='ACTIVE'
      )
      AND NOT EXISTS(
        SELECT root.root_source_id FROM public.current_public_roots(p_claim_id,NULL) root
        EXCEPT
        SELECT current_object.anonymous_source_id
        FROM public.current_anonymous_source_objects current_object
        WHERE current_object.claim_id=p_claim_id
          AND current_object.synthesis_id IS NULL
          AND current_object.object_revision=p_revision
      )
      AND NOT EXISTS(
        SELECT current_object.anonymous_source_id
        FROM public.current_anonymous_source_objects current_object
        WHERE current_object.claim_id=p_claim_id
          AND current_object.synthesis_id IS NULL
          AND current_object.object_revision=p_revision
        EXCEPT
        SELECT root.root_source_id FROM public.current_public_roots(p_claim_id,NULL) root
      )
      AND NOT EXISTS(
        SELECT 1 FROM public.current_public_roots(p_claim_id,NULL) root
        JOIN public.sources source ON source.source_id=root.root_source_id
        JOIN ops.anonymous_public_revocations revoked
          ON revoked.anonymous_source_id=source.source_id
         AND revoked.candidate_envelope_sha256=source.anonymous_envelope_sha256
      )
  )
$$;

CREATE FUNCTION public.require_current_supported_receipt()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF NEW.moderation_state='SUPPORTED' THEN
    IF NEW.current_evaluation_id IS NOT NULL THEN
      IF NEW.current_anonymous_trust_receipt_id IS NOT NULL
        OR NOT public.public_receipt_matches(
          NEW.claim_id,NULL,NEW.current_evaluation_id,NEW.object_revision,NEW.content)
        OR NOT EXISTS(
          SELECT 1 FROM public.claim_trust_evaluations evaluation
          WHERE evaluation.evaluation_id=NEW.current_evaluation_id
            AND evaluation.moderation_state='SUPPORTED'
        ) THEN
        RAISE EXCEPTION 'supported legacy claim needs one current complete legacy receipt'
          USING ERRCODE='23514';
      END IF;
    ELSIF NEW.current_anonymous_trust_receipt_id IS NULL
      OR NOT public.anonymous_trust_receipt_matches(
        NEW.claim_id,NEW.current_anonymous_trust_receipt_id,NEW.object_revision,NEW.content) THEN
      RAISE EXCEPTION 'supported anonymous claim needs one current protected receipt'
        USING ERRCODE='23514';
    END IF;
  END IF;
  RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER claim_supported_receipt_complete
  AFTER INSERT OR UPDATE ON public.claims
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
  EXECUTE FUNCTION public.require_current_supported_receipt();

-- The BEFORE trigger owns only immutable-body, adjacent-revision and pointer-transition shape.
-- Receipt completeness is checked at COMMIT so an anonymous evaluation can advance the claim,
-- carry lifecycle, attest the successor revision, and then seal authority+receipt atomically.
CREATE OR REPLACE FUNCTION public.guard_public_body()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE decision_changed boolean;
BEGIN
  IF TG_TABLE_NAME='claims' THEN
    IF TG_OP='UPDATE' AND NEW.content IS DISTINCT FROM OLD.content THEN
      RAISE EXCEPTION 'public bodies are immutable; create a replacement object'
        USING ERRCODE='23514';
    END IF;
    IF TG_OP='UPDATE' THEN
      decision_changed := ROW(NEW.current_evaluation_id,
        NEW.current_anonymous_trust_receipt_id,NEW.moderation_state) IS DISTINCT FROM
        ROW(OLD.current_evaluation_id,OLD.current_anonymous_trust_receipt_id,OLD.moderation_state);
      IF decision_changed THEN
        -- Expand-window backfill may mechanically move one already-current anonymous legacy
        -- receipt to its safe pointer without inventing a new public revision.
        IF (current_user='role_migration_owner' OR EXISTS(
            SELECT 1 FROM pg_roles role_row
            WHERE role_row.rolname=current_user AND role_row.rolsuper))
          AND NEW.object_revision=OLD.object_revision
          AND OLD.current_evaluation_id IS NOT NULL
          AND NEW.current_evaluation_id IS NULL
          AND OLD.current_anonymous_trust_receipt_id IS NULL
          AND NEW.current_anonymous_trust_receipt_id IS NOT NULL
          AND NEW.moderation_state=OLD.moderation_state THEN
          NULL;
        ELSIF NEW.object_revision<>OLD.object_revision+1 THEN
          RAISE EXCEPTION 'public decision requires next revision' USING ERRCODE='23514';
        END IF;
      ELSIF NEW.object_revision<>OLD.object_revision THEN
        RAISE EXCEPTION 'public revision requires a decision change' USING ERRCODE='23514';
      END IF;
    END IF;
  ELSE
    IF TG_OP='UPDATE' THEN
      IF NEW.content IS DISTINCT FROM OLD.content THEN
        RAISE EXCEPTION 'public bodies are immutable; create a replacement object'
          USING ERRCODE='23514';
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
  END IF;
  RETURN NEW;
END;
$$;

-- The legacy writer remains for compatibility, but an anonymous root can never enter the
-- identity-bearing legacy receipt again.
CREATE OR REPLACE FUNCTION public.guard_trust_evaluation()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
  IF NEW.claim_id IS NOT NULL AND EXISTS(
    SELECT 1 FROM public.current_public_roots(NEW.claim_id,NULL) root
    JOIN public.sources source ON source.source_id=root.root_source_id
    WHERE source.lineage_mode='ANONYMOUS_RELEASE'
  ) THEN
    RAISE EXCEPTION 'anonymous-root claims require the protected anonymous evaluator'
      USING ERRCODE='42501';
  END IF;
  IF NEW.moderation_state='SUPPORTED' AND NOT EXISTS(
    SELECT 1 FROM control.public_moderator_grants grant_row
    JOIN control.users evaluator USING(user_id)
    JOIN control.memberships membership ON membership.user_id=evaluator.user_id
    WHERE grant_row.user_id=NEW.evaluator_user_id
      AND grant_row.grant_version=NEW.evaluator_grant_version
      AND grant_row.enabled AND evaluator.state='ACTIVE' AND membership.state='ACTIVE'
      AND membership.tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid
      AND evaluator.user_id=NULLIF(current_setting('humaux.user_id',true),'')::uuid
  ) THEN
    RAISE EXCEPTION 'supported evaluation requires active global moderator grant'
      USING ERRCODE='42501';
  END IF;
  RETURN NEW;
END;
$$;

CREATE FUNCTION public.evaluate_anonymous_claim(
  p_tenant_id uuid,p_evaluator_user_id uuid,p_claim_id uuid,p_expected_revision bigint,
  p_expected_body_sha256 bytea,p_policy_version text,p_rationale text,p_target_state text
) RETURNS TABLE(anonymous_trust_receipt_id uuid,object_revision bigint,moderation_state text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE
  current_revision bigint;
  current_body_sha bytea;
  next_revision bigint;
  grant_version bigint;
  scope_tenant_id uuid;
  scope_evaluator_user_id uuid;
  attestation public.claim_independence_attestations;
  authority_id uuid := uuidv7();
  receipt_id uuid := uuidv7();
  commitment bytea;
  root_count bigint;
BEGIN
  IF p_tenant_id IS NULL OR p_evaluator_user_id IS NULL OR p_claim_id IS NULL
    OR p_expected_revision<=0 OR octet_length(p_expected_body_sha256)<>32
    OR btrim(p_policy_version)='' OR btrim(p_rationale)=''
    OR p_target_state NOT IN ('UNDER_REVIEW','SUPPORTED','QUARANTINED') THEN
    RAISE EXCEPTION 'anonymous evaluation arguments are invalid' USING ERRCODE='22023';
  END IF;
  scope_tenant_id:=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid;
  scope_evaluator_user_id:=NULLIF(current_setting('humaux.user_id',true),'')::uuid;
  IF scope_tenant_id IS DISTINCT FROM p_tenant_id
    OR scope_evaluator_user_id IS DISTINCT FROM p_evaluator_user_id THEN
    RAISE EXCEPTION 'anonymous evaluation parameters do not match caller transaction scope'
      USING ERRCODE='42501';
  END IF;

  SELECT moderator.grant_version INTO grant_version
  FROM control.public_moderator_grants moderator
  JOIN control.users evaluator ON evaluator.user_id=moderator.user_id
  JOIN control.memberships membership
    ON membership.user_id=evaluator.user_id AND membership.tenant_id=p_tenant_id
  WHERE moderator.user_id=p_evaluator_user_id AND moderator.enabled
    AND evaluator.state='ACTIVE' AND membership.state='ACTIVE';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'anonymous evaluation requires active moderator grant and membership'
      USING ERRCODE='42501';
  END IF;

  PERFORM ops.lock_contribution_inputs();
  LOCK TABLE public.provenance_edges,public.sources,public.synthesis_inputs,
    public.source_closure,public.relations,public.anonymous_source_authority_events,
    public.anonymous_source_lifecycle_events,control.anonymous_source_lineage,
    staging.contribution_releases,staging.contribution_candidate_phase9_assessments,
    staging.sanitized_public_candidates,ops.anonymous_public_revocations IN SHARE MODE;

  SELECT claim.object_revision,sha256(convert_to(claim.content::text,'UTF8'))
    INTO current_revision,current_body_sha
    FROM public.claims claim
    WHERE claim.claim_id=p_claim_id AND claim.intake_release_id IS NULL
    FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'anonymous claim does not exist or has legacy intake authority'
      USING ERRCODE='P0002';
  END IF;
  IF current_revision IS DISTINCT FROM p_expected_revision
    OR current_body_sha IS DISTINCT FROM p_expected_body_sha256 THEN
    RAISE EXCEPTION 'anonymous claim revision or body is stale' USING ERRCODE='40001';
  END IF;

  SELECT count(*) INTO root_count FROM public.current_public_roots(p_claim_id,NULL);
  IF root_count<=0 OR EXISTS(
    SELECT 1 FROM public.current_public_roots(p_claim_id,NULL) root
    LEFT JOIN public.sources source ON source.source_id=root.root_source_id
    LEFT JOIN control.anonymous_source_lineage lineage
      ON lineage.anonymous_source_id=source.source_id AND lineage.tenant_id=p_tenant_id
    LEFT JOIN public.current_anonymous_source_objects current_object
      ON current_object.anonymous_source_id=source.source_id
     AND current_object.candidate_envelope_sha256=source.anonymous_envelope_sha256
     AND current_object.claim_id=p_claim_id AND current_object.synthesis_id IS NULL
     AND current_object.object_revision=current_revision
    WHERE source.source_id IS NULL OR source.lineage_mode<>'ANONYMOUS_RELEASE'
      OR source.source_type<>'ANONYMOUS_USER_CONTRIBUTION'
      OR source.contribution_release_id IS NOT NULL OR source.publisher IS NOT NULL
      OR source.source_url IS NOT NULL OR source.verified_organization IS NOT NULL
      OR source.canonical_url IS NOT NULL OR source.document_fingerprint IS NOT NULL
      OR source.upstream_root IS NOT NULL OR source.identity_policy_version IS NOT NULL
      OR source.trusted_by_policy IS NOT NULL OR source.anonymous_envelope_sha256 IS NULL
      OR lineage.anonymous_source_id IS NULL OR current_object.anonymous_source_id IS NULL
  ) OR root_count IS DISTINCT FROM (
    SELECT count(*) FROM public.provenance_edges edge WHERE edge.claim_id=p_claim_id
  ) THEN
    RAISE EXCEPTION 'claim is not the exact current anonymous direct-root universe for tenant'
      USING ERRCODE='42501';
  END IF;

  next_revision := current_revision+1;
  UPDATE public.claims claim
    SET current_evaluation_id=NULL,current_anonymous_trust_receipt_id=receipt_id,
      moderation_state=p_target_state,object_revision=next_revision
    WHERE claim.claim_id=p_claim_id AND claim.object_revision=current_revision;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'anonymous claim revision changed during evaluation' USING ERRCODE='40001';
  END IF;
  PERFORM public.carry_forward_anonymous_claim_lifecycle(
    p_claim_id,current_revision,next_revision);
  SELECT * INTO attestation FROM public.attest_current_claim_independence(
    p_claim_id,next_revision,current_body_sha);

  commitment := sha256(convert_to(
    authority_id::text||'|'||receipt_id::text||'|'||p_claim_id::text||'|'||
      next_revision::text||'|'||encode(current_body_sha,'hex')||'|'||p_policy_version||'|'||
      p_target_state,'UTF8'));
  INSERT INTO control.anonymous_claim_trust_authorities(
    anonymous_trust_authority_id,tenant_id,claim_id,object_revision,body_sha256,
    evaluator_user_id,evaluator_grant_version,rationale,policy_version,moderation_state,
    independence_attestation_id,public_receipt_id,review_commitment)
  VALUES(authority_id,p_tenant_id,p_claim_id,next_revision,current_body_sha,
    p_evaluator_user_id,grant_version,p_rationale,p_policy_version,p_target_state,
    attestation.attestation_id,receipt_id,commitment);
  INSERT INTO public.anonymous_claim_trust_receipts(
    anonymous_trust_receipt_id,claim_id,object_revision,body_sha256,policy_version,
    moderation_state,independence_attestation_id,root_set_sha256,support_count,
    independent_support_count,sybil_risk,checks_complete,review_commitment)
  VALUES(receipt_id,p_claim_id,next_revision,current_body_sha,p_policy_version,p_target_state,
    attestation.attestation_id,attestation.root_set_sha,attestation.support_count,
    attestation.independent_count,attestation.sybil_risk,attestation.checks_complete,commitment);
  PERFORM ops.emit_public_object_changed(p_tenant_id,p_claim_id,NULL,next_revision);

  anonymous_trust_receipt_id := receipt_id;
  object_revision := next_revision;
  moderation_state := p_target_state;
  RETURN NEXT;
END;
$$;

-- Fail closed unless every current identity-bearing receipt on an anonymous-root claim can be
-- moved to a matching protected authority and safe receipt.  Historical legacy rows remain
-- immutable for audit, but are no longer reachable from the current claim pointer.
DO $$
DECLARE row_to_move record; attestation public.claim_independence_attestations;
  tenant_count bigint; root_count bigint; source_tenant_id uuid;
  authority_id uuid; receipt_id uuid; commitment bytea;
BEGIN
  FOR row_to_move IN
    SELECT claim.claim_id,claim.object_revision,claim.content,claim.moderation_state,
      evaluation.evaluation_id,evaluation.body_sha256,evaluation.policy_version,
      evaluation.evaluator_user_id,evaluation.evaluator_grant_version,evaluation.rationale
    FROM public.claims claim
    JOIN public.claim_trust_evaluations evaluation
      ON evaluation.evaluation_id=claim.current_evaluation_id
    WHERE EXISTS(
      SELECT 1 FROM public.current_public_roots(claim.claim_id,NULL) root
      JOIN public.sources source ON source.source_id=root.root_source_id
      WHERE source.lineage_mode='ANONYMOUS_RELEASE'
    )
    FOR UPDATE OF claim
  LOOP
    SELECT count(DISTINCT lineage.tenant_id),min(lineage.tenant_id),count(*)
      INTO tenant_count,source_tenant_id,root_count
    FROM public.current_public_roots(row_to_move.claim_id,NULL) root
    JOIN public.sources source ON source.source_id=root.root_source_id
    JOIN control.anonymous_source_lineage lineage
      ON lineage.anonymous_source_id=source.source_id
    JOIN public.current_anonymous_source_objects current_object
      ON current_object.anonymous_source_id=source.source_id
     AND current_object.candidate_envelope_sha256=source.anonymous_envelope_sha256
     AND current_object.claim_id=row_to_move.claim_id
     AND current_object.object_revision=row_to_move.object_revision
    WHERE source.lineage_mode='ANONYMOUS_RELEASE'
      AND source.contribution_release_id IS NULL;
    IF tenant_count<>1 OR root_count<>(
      SELECT count(*) FROM public.current_public_roots(row_to_move.claim_id,NULL)
    ) OR row_to_move.evaluator_user_id IS NULL OR row_to_move.evaluator_grant_version IS NULL
      OR NOT EXISTS(
        SELECT 1 FROM control.public_moderator_grants moderator
        JOIN control.users evaluator ON evaluator.user_id=moderator.user_id
        JOIN control.memberships membership ON membership.user_id=evaluator.user_id
        WHERE moderator.user_id=row_to_move.evaluator_user_id
          AND moderator.grant_version=row_to_move.evaluator_grant_version
          AND moderator.enabled AND evaluator.state='ACTIVE' AND membership.state='ACTIVE'
          AND membership.tenant_id=source_tenant_id
      ) THEN
      RAISE EXCEPTION 'cannot safely move current anonymous legacy evaluation for claim %',
        row_to_move.claim_id USING ERRCODE='23514';
    END IF;
    PERFORM set_config('humaux.tenant_id',source_tenant_id::text,true);
    PERFORM set_config('humaux.user_id',row_to_move.evaluator_user_id::text,true);
    SELECT * INTO attestation FROM public.attest_current_claim_independence(
      row_to_move.claim_id,row_to_move.object_revision,row_to_move.body_sha256);
    authority_id:=uuidv7(); receipt_id:=uuidv7();
    commitment:=sha256(convert_to(authority_id::text||'|'||receipt_id::text||'|'||
      row_to_move.claim_id::text||'|'||row_to_move.object_revision::text||'|'||
      encode(row_to_move.body_sha256,'hex')||'|'||row_to_move.policy_version||'|'||
      row_to_move.moderation_state,'UTF8'));
    INSERT INTO control.anonymous_claim_trust_authorities(
      anonymous_trust_authority_id,tenant_id,claim_id,object_revision,body_sha256,
      evaluator_user_id,evaluator_grant_version,rationale,policy_version,moderation_state,
      independence_attestation_id,public_receipt_id,review_commitment)
    VALUES(authority_id,source_tenant_id,row_to_move.claim_id,row_to_move.object_revision,
      row_to_move.body_sha256,row_to_move.evaluator_user_id,row_to_move.evaluator_grant_version,
      row_to_move.rationale,row_to_move.policy_version,row_to_move.moderation_state,
      attestation.attestation_id,receipt_id,commitment);
    INSERT INTO public.anonymous_claim_trust_receipts(
      anonymous_trust_receipt_id,claim_id,object_revision,body_sha256,policy_version,
      moderation_state,independence_attestation_id,root_set_sha256,support_count,
      independent_support_count,sybil_risk,checks_complete,review_commitment)
    VALUES(receipt_id,row_to_move.claim_id,row_to_move.object_revision,row_to_move.body_sha256,
      row_to_move.policy_version,row_to_move.moderation_state,attestation.attestation_id,
      attestation.root_set_sha,attestation.support_count,attestation.independent_count,
      attestation.sybil_risk,attestation.checks_complete,commitment);
    UPDATE public.claims SET current_evaluation_id=NULL,
      current_anonymous_trust_receipt_id=receipt_id
      WHERE claim_id=row_to_move.claim_id;
  END LOOP;
END;
$$;

-- Owner-safe serving and history surfaces keep the existing wire alias `evaluation_id` while
-- direct legacy trust-table SELECT disappears from gateway/public/retrieval roles.
CREATE OR REPLACE VIEW public.eligible_objects
WITH (security_barrier=true,security_invoker=false) AS
  SELECT claim.claim_id AS object_id,'CLAIM'::text AS object_kind,claim.content,
    claim.object_revision,claim.current_evaluation_id
  FROM public.claims claim
  JOIN public.claim_trust_evaluations evaluation
    ON evaluation.evaluation_id=claim.current_evaluation_id
  WHERE claim.current_anonymous_trust_receipt_id IS NULL
    AND claim.moderation_state='SUPPORTED' AND evaluation.moderation_state='SUPPORTED'
    AND public.public_receipt_matches(claim.claim_id,NULL,claim.current_evaluation_id,
      claim.object_revision,claim.content)
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
  JOIN public.claim_trust_evaluations evaluation
    ON evaluation.evaluation_id=synthesis.current_evaluation_id
  WHERE synthesis.moderation_state='SUPPORTED' AND evaluation.moderation_state='SUPPORTED'
    AND public.public_receipt_matches(NULL,synthesis.synthesis_id,synthesis.current_evaluation_id,
      synthesis.object_revision,synthesis.content);

CREATE FUNCTION public.public_projection_identities(
  p_object_id uuid,p_object_kind text,p_through_revision bigint
) RETURNS TABLE(object_id uuid,object_revision bigint,evaluation_id uuid,body_sha256 bytea)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
  SELECT evaluation.claim_id,evaluation.object_revision,evaluation.evaluation_id,
    evaluation.body_sha256
  FROM public.claim_trust_evaluations evaluation
  WHERE p_object_kind='CLAIM' AND evaluation.claim_id=p_object_id
    AND evaluation.object_revision<=p_through_revision
  UNION ALL
  SELECT evaluation.synthesis_id,evaluation.object_revision,evaluation.evaluation_id,
    evaluation.body_sha256
  FROM public.claim_trust_evaluations evaluation
  WHERE p_object_kind='SYNTHESIS' AND evaluation.synthesis_id=p_object_id
    AND evaluation.object_revision<=p_through_revision
  UNION ALL
  SELECT receipt.claim_id,receipt.object_revision,receipt.anonymous_trust_receipt_id,
    receipt.body_sha256
  FROM public.anonymous_claim_trust_receipts receipt
  WHERE p_object_kind='CLAIM' AND receipt.claim_id=p_object_id
    AND receipt.object_revision<=p_through_revision
$$;

-- Existing anonymous revoke code clears current_evaluation_id.  This BEFORE trigger extends
-- the same fail-closed transition to the mechanically disjoint anonymous pointer.
CREATE FUNCTION public.clear_anonymous_receipt_on_state_exit()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
  IF OLD.current_anonymous_trust_receipt_id IS NOT NULL
    AND NEW.current_anonymous_trust_receipt_id=OLD.current_anonymous_trust_receipt_id
    AND NEW.current_evaluation_id IS NULL
    AND OLD.moderation_state='SUPPORTED' AND NEW.moderation_state<>'SUPPORTED' THEN
    NEW.current_anonymous_trust_receipt_id:=NULL;
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER aa_claim_clear_anonymous_receipt_on_state_exit
  BEFORE UPDATE ON public.claims FOR EACH ROW
  EXECUTE FUNCTION public.clear_anonymous_receipt_on_state_exit();

ALTER TABLE control.anonymous_claim_trust_authorities OWNER TO role_migration_owner;
ALTER TABLE public.anonymous_claim_trust_receipts OWNER TO role_migration_owner;
ALTER VIEW public.eligible_objects OWNER TO role_migration_owner;
ALTER FUNCTION control.guard_anonymous_claim_trust_authority_append_only()
  OWNER TO role_migration_owner;
ALTER FUNCTION public.guard_anonymous_claim_trust_receipt_append_only()
  OWNER TO role_migration_owner;
ALTER FUNCTION public.require_anonymous_receipt_authority() OWNER TO role_migration_owner;
ALTER FUNCTION public.anonymous_trust_receipt_matches(uuid,uuid,bigint,jsonb)
  OWNER TO role_migration_owner;
ALTER FUNCTION public.require_current_supported_receipt() OWNER TO role_migration_owner;
ALTER FUNCTION public.evaluate_anonymous_claim(uuid,uuid,uuid,bigint,bytea,text,text,text)
  OWNER TO role_migration_owner;
ALTER FUNCTION public.public_projection_identities(uuid,text,bigint) OWNER TO role_migration_owner;
ALTER FUNCTION public.clear_anonymous_receipt_on_state_exit() OWNER TO role_migration_owner;

REVOKE ALL ON control.anonymous_claim_trust_authorities,
  public.anonymous_claim_trust_receipts
  FROM PUBLIC,role_gateway,role_private_worker,role_consolidation_worker,
  role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;
GRANT SELECT ON public.anonymous_claim_trust_receipts
  TO role_gateway,role_public_worker,role_retrieval_worker,role_maintenance;

REVOKE ALL ON FUNCTION control.guard_anonymous_claim_trust_authority_append_only(),
  public.guard_anonymous_claim_trust_receipt_append_only(),
  public.require_anonymous_receipt_authority(),
  public.anonymous_trust_receipt_matches(uuid,uuid,bigint,jsonb),
  public.require_current_supported_receipt(),
  public.evaluate_anonymous_claim(uuid,uuid,uuid,bigint,bytea,text,text,text),
  public.public_projection_identities(uuid,text,bigint),
  public.clear_anonymous_receipt_on_state_exit()
  FROM PUBLIC,role_gateway,role_private_worker,role_consolidation_worker,
  role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;
GRANT EXECUTE ON FUNCTION
  public.evaluate_anonymous_claim(uuid,uuid,uuid,bigint,bytea,text,text,text)
  TO role_private_worker;
GRANT EXECUTE ON FUNCTION public.anonymous_trust_receipt_matches(uuid,uuid,bigint,jsonb),
  public.public_projection_identities(uuid,text,bigint)
  TO role_gateway,role_public_worker,role_retrieval_worker,role_maintenance;

REVOKE SELECT ON public.claim_trust_evaluations,
  public.claim_trust_evaluation_sources,public.poisoning_signals
  FROM role_gateway,role_public_worker,role_retrieval_worker;
REVOKE INSERT ON public.claim_trust_evaluations,
  public.claim_trust_evaluation_sources,public.poisoning_signals FROM role_public_worker;
GRANT INSERT(evaluation_id,claim_id,synthesis_id,object_revision,body_sha256,policy_version,
  moderation_state,evaluator_user_id,evaluator_grant_version,rationale,support_count,
  independent_support_count,trusted_source_count,identity_incomplete,contradiction_count,
  checks_complete)
  ON public.claim_trust_evaluations TO role_public_worker;
GRANT INSERT(evaluation_id,root_source_id,source_content_hash,contribution_release_id)
  ON public.claim_trust_evaluation_sources TO role_public_worker;
GRANT INSERT(evaluation_id,signal_code,check_version,detected)
  ON public.poisoning_signals TO role_public_worker;

COMMENT ON TABLE control.anonymous_claim_trust_authorities IS
  'Protected append-only moderator authority for an anonymous claim receipt. Runtime access is only through narrow owner functions.';
COMMENT ON TABLE public.anonymous_claim_trust_receipts IS
  'Identity-free append-only anonymous trust receipt. Contains no tenant, release, contributor, publisher, evaluator, grant, rationale, or protected row identifier.';
COMMENT ON FUNCTION public.evaluate_anonymous_claim(uuid,uuid,uuid,bigint,bytea,text,text,text) IS
  'Private-worker-only anonymous evaluator. Verifies exact caller tenant/user scope, seals protected authority plus safe receipt, advances lifecycle, and emits one object event atomically.';
