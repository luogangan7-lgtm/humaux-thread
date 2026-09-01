-- Phase 9 additive seam: a release-scoped random public identity, a sealed sanitized
-- candidate, and append-only anonymous public lifecycle facts.  Existing release-linked
-- public columns remain untouched until the separately approved contract phase.

-- One random v4 UUID per release prevents a stable contributor pseudonym.  The release PK
-- makes retry lookup idempotent: a second attempt cannot mint another public identity for
-- the same release.
CREATE TABLE control.anonymous_source_lineage (
  contribution_release_id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  anonymous_source_id uuid NOT NULL DEFAULT uuidv4(),
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT anonymous_source_lineage_release_tenant_fk
    FOREIGN KEY(tenant_id,contribution_release_id)
    REFERENCES staging.contribution_releases(tenant_id,contribution_release_id),
  CONSTRAINT anonymous_source_lineage_source_unique UNIQUE(anonymous_source_id),
  CONSTRAINT anonymous_source_lineage_tenant_source_unique
    UNIQUE(tenant_id,anonymous_source_id),
  CONSTRAINT anonymous_source_lineage_source_v4
    CHECK(uuid_extract_version(anonymous_source_id)=4)
);

-- The candidate is outside public.* until admission.  Its envelope digest binds the
-- normalized JSON content to the exact policy and assessment receipts.  Its tenant stays
-- protected here; no user, release, publisher, or evaluator identity enters public.*.
CREATE TABLE staging.sanitized_public_candidates (
  candidate_envelope_id uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id uuid NOT NULL REFERENCES control.tenants(tenant_id),
  anonymous_source_id uuid NOT NULL,
  sanitized_content jsonb NOT NULL,
  content_sha256 bytea NOT NULL CHECK(octet_length(content_sha256)=32),
  policy_version text NOT NULL CHECK(btrim(policy_version)<>''),
  policy_digest bytea NOT NULL CHECK(octet_length(policy_digest)=32),
  assessment_outcome text NOT NULL,
  assessment_digest bytea NOT NULL CHECK(octet_length(assessment_digest)=32),
  envelope_sha256 bytea NOT NULL CHECK(octet_length(envelope_sha256)=32),
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT sanitized_public_candidates_tenant_source_fk
    FOREIGN KEY(tenant_id,anonymous_source_id)
    REFERENCES control.anonymous_source_lineage(tenant_id,anonymous_source_id),
  CONSTRAINT sanitized_public_candidates_content_sealed CHECK(
    content_sha256=sha256(convert_to(sanitized_content::text,'UTF8'))),
  CONSTRAINT sanitized_public_candidates_assessment_passed CHECK(
    assessment_outcome='PASSED'),
  CONSTRAINT sanitized_public_candidates_envelope_sealed CHECK(
    envelope_sha256=sha256(content_sha256||policy_digest||assessment_digest||
      convert_to(policy_version||':'||assessment_outcome,'UTF8'))),
  CONSTRAINT sanitized_public_candidates_source_envelope_unique
    UNIQUE(anonymous_source_id,envelope_sha256)
);

-- Reuse public.sources as the sole direct-provenance root.  LEGACY is the exact old
-- contract; ANONYMOUS_RELEASE is an additive identity-free variant whose source_id is the
-- per-release random bridge and whose public row has no release, publisher, URL, tenant, or
-- evaluator link.
ALTER TABLE public.sources
  DROP CONSTRAINT sources_source_type_closed,
  DROP CONSTRAINT sources_release_type,
  ADD COLUMN lineage_mode text NOT NULL DEFAULT 'LEGACY',
  ADD COLUMN anonymous_envelope_sha256 bytea,
  ADD CONSTRAINT sources_source_type_closed CHECK(source_type IN (
    'USER_CONTRIBUTION','ANONYMOUS_USER_CONTRIBUTION','OFFICIAL_DOCUMENT','PUBLIC_WEB',
    'OPEN_SOURCE_DOCUMENT','ADMIN_IMPORT')),
  ADD CONSTRAINT sources_lineage_mode_closed CHECK(lineage_mode IN ('LEGACY','ANONYMOUS_RELEASE')),
  ADD CONSTRAINT sources_release_type CHECK(
    (lineage_mode='LEGACY'
      AND anonymous_envelope_sha256 IS NULL
      AND ((source_type='USER_CONTRIBUTION')=(contribution_release_id IS NOT NULL)))
    OR
    (lineage_mode='ANONYMOUS_RELEASE'
      AND source_type='ANONYMOUS_USER_CONTRIBUTION'
      AND contribution_release_id IS NULL
      AND publisher IS NULL AND source_url IS NULL
      AND verified_organization IS NULL AND canonical_url IS NULL
      AND document_fingerprint IS NULL AND upstream_root IS NULL
      AND identity_policy_version IS NULL AND trusted_by_policy IS NULL
      AND anonymous_envelope_sha256 IS NOT NULL
      AND octet_length(anonymous_envelope_sha256)=32
      AND uuid_extract_version(source_id)=4)),
  ADD CONSTRAINT sources_anonymous_envelope_unique
    UNIQUE(source_id,anonymous_envelope_sha256);

CREATE FUNCTION public.guard_anonymous_source_identity()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
  IF TG_OP='UPDATE' AND ROW(NEW.lineage_mode,NEW.anonymous_envelope_sha256)
    IS DISTINCT FROM ROW(OLD.lineage_mode,OLD.anonymous_envelope_sha256) THEN
    RAISE EXCEPTION 'anonymous public source identity is immutable' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER anonymous_public_source_identity_guard
  BEFORE UPDATE ON public.sources FOR EACH ROW
  EXECUTE FUNCTION public.guard_anonymous_source_identity();

-- lifecycle_seq is a total append order so a later fresh-current predicate can select the
-- latest ADMIT/REVOKE fact for one anonymous source + object revision without timestamp ties.
CREATE TABLE public.anonymous_source_lifecycle_events (
  lifecycle_event_id uuid PRIMARY KEY DEFAULT uuidv7(),
  lifecycle_seq bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
  anonymous_source_id uuid NOT NULL,
  candidate_envelope_sha256 bytea NOT NULL
    CHECK(octet_length(candidate_envelope_sha256)=32),
  claim_id uuid,
  synthesis_id uuid,
  object_revision bigint NOT NULL,
  event_type text NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT anonymous_lifecycle_claim_fk
    FOREIGN KEY(claim_id) REFERENCES public.claims(claim_id),
  CONSTRAINT anonymous_lifecycle_synthesis_fk
    FOREIGN KEY(synthesis_id) REFERENCES public.syntheses(synthesis_id),
  CONSTRAINT anonymous_lifecycle_exactly_one_target
    CHECK((claim_id IS NULL)<>(synthesis_id IS NULL)),
  CONSTRAINT anonymous_lifecycle_positive_revision CHECK(object_revision>0),
  CONSTRAINT anonymous_lifecycle_event_type CHECK(event_type IN ('ADMIT','REVOKE')),
  CONSTRAINT anonymous_lifecycle_candidate_envelope_fk
    FOREIGN KEY(anonymous_source_id,candidate_envelope_sha256)
    REFERENCES public.sources(source_id,anonymous_envelope_sha256)
);
CREATE UNIQUE INDEX anonymous_lifecycle_claim_event_unique
  ON public.anonymous_source_lifecycle_events(
    anonymous_source_id,claim_id,object_revision,event_type)
  WHERE claim_id IS NOT NULL;
CREATE UNIQUE INDEX anonymous_lifecycle_synthesis_event_unique
  ON public.anonymous_source_lifecycle_events(
    anonymous_source_id,synthesis_id,object_revision,event_type)
  WHERE synthesis_id IS NOT NULL;

-- Serialize one source/object/revision state machine.  The derived current view remains the
-- only fresh predicate: a REVOKE must follow ADMIT, and the same revision cannot be admitted
-- again after revocation.
CREATE FUNCTION public.guard_anonymous_lifecycle_transition()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE target_id uuid := COALESCE(NEW.claim_id,NEW.synthesis_id);
BEGIN
  IF target_id IS NULL THEN
    RAISE EXCEPTION 'anonymous lifecycle requires exactly one target' USING ERRCODE='23514';
  END IF;
  PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'anonymous-lifecycle:'||NEW.anonymous_source_id::text||':'||target_id::text||':'||
      NEW.object_revision::text,0));
  -- At-least-once dispatch: an exact replay is a successful no-op, including a delayed
  -- ADMIT replay after the same revision has since been revoked.  It never reactivates.
  IF EXISTS(
    SELECT 1 FROM public.anonymous_source_lifecycle_events e
    WHERE e.anonymous_source_id=NEW.anonymous_source_id
      AND e.candidate_envelope_sha256=NEW.candidate_envelope_sha256
      AND e.claim_id IS NOT DISTINCT FROM NEW.claim_id
      AND e.synthesis_id IS NOT DISTINCT FROM NEW.synthesis_id
      AND e.object_revision=NEW.object_revision AND e.event_type=NEW.event_type) THEN
    RETURN NULL;
  ELSIF NEW.event_type='ADMIT' AND EXISTS(
    SELECT 1 FROM public.anonymous_source_lifecycle_events e
    WHERE e.anonymous_source_id=NEW.anonymous_source_id
      AND e.claim_id IS NOT DISTINCT FROM NEW.claim_id
      AND e.synthesis_id IS NOT DISTINCT FROM NEW.synthesis_id
      AND e.object_revision=NEW.object_revision) THEN
    RAISE EXCEPTION 'anonymous lifecycle ADMIT must be the first revision event'
      USING ERRCODE='23514';
  ELSIF NEW.event_type='REVOKE' AND (
    NOT EXISTS(
      SELECT 1 FROM public.anonymous_source_lifecycle_events e
      WHERE e.anonymous_source_id=NEW.anonymous_source_id
        AND e.claim_id IS NOT DISTINCT FROM NEW.claim_id
        AND e.synthesis_id IS NOT DISTINCT FROM NEW.synthesis_id
        AND e.object_revision=NEW.object_revision AND e.event_type='ADMIT'
        AND e.candidate_envelope_sha256=NEW.candidate_envelope_sha256)
    OR EXISTS(
      SELECT 1 FROM public.anonymous_source_lifecycle_events e
      WHERE e.anonymous_source_id=NEW.anonymous_source_id
        AND e.claim_id IS NOT DISTINCT FROM NEW.claim_id
        AND e.synthesis_id IS NOT DISTINCT FROM NEW.synthesis_id
        AND e.object_revision=NEW.object_revision AND e.event_type='REVOKE')) THEN
    RAISE EXCEPTION 'anonymous lifecycle REVOKE requires one live ADMIT'
      USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER anonymous_lifecycle_transition_guard
  BEFORE INSERT ON public.anonymous_source_lifecycle_events FOR EACH ROW
  EXECUTE FUNCTION public.guard_anonymous_lifecycle_transition();

CREATE VIEW public.current_anonymous_source_objects
WITH (security_barrier=true,security_invoker=true) AS
SELECT anonymous_source_id,candidate_envelope_sha256,claim_id,synthesis_id,
  object_revision,lifecycle_seq
FROM (
  SELECT DISTINCT ON (anonymous_source_id,claim_id,synthesis_id,object_revision)
    anonymous_source_id,candidate_envelope_sha256,claim_id,synthesis_id,
    object_revision,lifecycle_seq,event_type
  FROM public.anonymous_source_lifecycle_events
  ORDER BY anonymous_source_id,claim_id,synthesis_id,object_revision,lifecycle_seq DESC
) latest
WHERE event_type='ADMIT';

ALTER TABLE control.anonymous_source_lineage ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.anonymous_source_lineage FORCE ROW LEVEL SECURITY;
CREATE POLICY anonymous_source_lineage_tenant_isolation
  ON control.anonymous_source_lineage
  USING(tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid)
  WITH CHECK(tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid);
ALTER TABLE staging.sanitized_public_candidates ENABLE ROW LEVEL SECURITY;
ALTER TABLE staging.sanitized_public_candidates FORCE ROW LEVEL SECURITY;
CREATE POLICY sanitized_public_candidates_tenant_isolation
  ON staging.sanitized_public_candidates
  USING(tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid)
  WITH CHECK(tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid);

ALTER TABLE control.anonymous_source_lineage OWNER TO role_migration_owner;
ALTER TABLE staging.sanitized_public_candidates OWNER TO role_migration_owner;
ALTER TABLE public.anonymous_source_lifecycle_events OWNER TO role_migration_owner;
ALTER VIEW public.current_anonymous_source_objects OWNER TO role_migration_owner;
ALTER SEQUENCE public.anonymous_source_lifecycle_events_lifecycle_seq_seq
  OWNER TO role_migration_owner;
ALTER FUNCTION public.guard_anonymous_source_identity() OWNER TO role_migration_owner;
ALTER FUNCTION public.guard_anonymous_lifecycle_transition() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION public.guard_anonymous_source_identity(),
  public.guard_anonymous_lifecycle_transition() FROM PUBLIC;

-- Freeze the new seam to its two producers and read consumers.  In particular, public
-- readers cannot resolve anonymous_source_id back to a release or inspect pre-admission
-- candidates.  The lineage INSERT grant excludes anonymous_source_id, forcing the random
-- database default instead of caller-selected stable values.
REVOKE ALL ON control.anonymous_source_lineage,
  staging.sanitized_public_candidates,
  public.anonymous_source_lifecycle_events,
  public.current_anonymous_source_objects
  FROM PUBLIC,role_gateway,role_private_worker,role_consolidation_worker,
  role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;
REVOKE ALL ON SEQUENCE public.anonymous_source_lifecycle_events_lifecycle_seq_seq
  FROM PUBLIC,role_gateway,role_private_worker,role_consolidation_worker,
  role_public_worker,role_retrieval_worker,role_batch_issuer,role_maintenance;

GRANT SELECT ON control.anonymous_source_lineage TO role_private_worker,role_maintenance;
GRANT INSERT(contribution_release_id,tenant_id) ON control.anonymous_source_lineage
  TO role_private_worker;

GRANT SELECT ON staging.sanitized_public_candidates TO role_private_worker,role_maintenance;
GRANT SELECT(anonymous_source_id,sanitized_content,content_sha256,policy_version,
  policy_digest,assessment_outcome,assessment_digest,envelope_sha256)
  ON staging.sanitized_public_candidates TO role_public_worker;
GRANT INSERT(tenant_id,anonymous_source_id,sanitized_content,content_sha256,policy_version,
  policy_digest,assessment_outcome,assessment_digest,envelope_sha256)
  ON staging.sanitized_public_candidates TO role_private_worker;

GRANT SELECT ON public.anonymous_source_lifecycle_events
  TO role_gateway,role_public_worker,role_retrieval_worker,role_maintenance;
GRANT INSERT(anonymous_source_id,candidate_envelope_sha256,claim_id,synthesis_id,
  object_revision,event_type)
  ON public.anonymous_source_lifecycle_events TO role_public_worker;
GRANT USAGE ON SEQUENCE public.anonymous_source_lifecycle_events_lifecycle_seq_seq
  TO role_public_worker;
GRANT SELECT ON public.current_anonymous_source_objects
  TO role_gateway,role_public_worker,role_retrieval_worker,role_maintenance;

COMMENT ON TABLE control.anonymous_source_lineage IS
  'Protected release-to-random-v4 anonymous source bridge. One ID per release; never a contributor pseudonym.';
COMMENT ON TABLE staging.sanitized_public_candidates IS
  'Pre-admission contributor-identity-free candidate envelope sealed to content, policy, and assessment digests.';
COMMENT ON TABLE public.anonymous_source_lifecycle_events IS
  'Anonymous append-only ADMIT/REVOKE facts. No contributor, release, publisher, or evaluator identity.';
