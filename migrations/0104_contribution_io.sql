-- §12/§13/§14, ADR-0008: release-bound contribution IO. No runtime DELETE,
-- SECURITY DEFINER, second outbox, or second provenance authority.

-- The tenant column is constrained redundancy: the parent release remains its source.
ALTER TABLE staging.contribution_releases
  ADD CONSTRAINT contribution_releases_tenant_id_unique
    UNIQUE (tenant_id, contribution_release_id);
ALTER TABLE private.evidence_objects
  ADD CONSTRAINT evidence_objects_tenant_id_unique UNIQUE (tenant_id, evidence_id);
ALTER TABLE private.memory_records
  ADD CONSTRAINT memory_records_tenant_id_unique UNIQUE (tenant_id, memory_id);
ALTER TABLE staging.contribution_release_sources ADD COLUMN tenant_id uuid;
UPDATE staging.contribution_release_sources s SET tenant_id = r.tenant_id
FROM staging.contribution_releases r
WHERE r.contribution_release_id = s.contribution_release_id;
ALTER TABLE staging.contribution_release_sources
  ALTER COLUMN tenant_id SET NOT NULL,
  ADD CONSTRAINT release_sources_tenant_release_fk
    FOREIGN KEY (tenant_id, contribution_release_id)
    REFERENCES staging.contribution_releases (tenant_id, contribution_release_id),
  ADD CONSTRAINT release_sources_tenant_evidence_fk
    FOREIGN KEY (tenant_id, evidence_id)
    REFERENCES private.evidence_objects (tenant_id, evidence_id),
  ADD CONSTRAINT release_sources_tenant_memory_fk
    FOREIGN KEY (tenant_id, memory_id)
    REFERENCES private.memory_records (tenant_id, memory_id);

-- Evidence events retain their stream identity. Release events have no Evidence/stream.
ALTER TABLE ops.outbox
  ADD COLUMN contribution_release_id uuid,
  ALTER COLUMN evidence_id DROP NOT NULL,
  ALTER COLUMN stream_seq DROP NOT NULL,
  ADD CONSTRAINT outbox_tenant_release_fk
    FOREIGN KEY (tenant_id, contribution_release_id)
    REFERENCES staging.contribution_releases (tenant_id, contribution_release_id),
  ADD CONSTRAINT outbox_row_class CHECK (
    (contribution_release_id IS NULL AND evidence_id IS NOT NULL AND stream_seq IS NOT NULL
      AND event_type NOT IN ('PUBLIC_RELEASE', 'PUBLIC_REVOKE'))
    OR
    (contribution_release_id IS NOT NULL AND evidence_id IS NULL AND stream_seq IS NULL
      AND event_type IN ('PUBLIC_RELEASE', 'PUBLIC_REVOKE'))
  );
CREATE UNIQUE INDEX outbox_release_event_unique
  ON ops.outbox (contribution_release_id, event_type)
  WHERE contribution_release_id IS NOT NULL;

-- A user contribution's public identity has a real release parent, never JSON pseudo-FK.
ALTER TABLE public.sources
  ADD COLUMN contribution_release_id uuid
    REFERENCES staging.contribution_releases (contribution_release_id),
  ADD CONSTRAINT sources_release_type CHECK (
    (source_type = 'USER_CONTRIBUTION') = (contribution_release_id IS NOT NULL)
  );
CREATE INDEX sources_contribution_release_idx ON public.sources (contribution_release_id)
  WHERE contribution_release_id IS NOT NULL;

-- A pair occurs once even when inactive; repeated refresh cannot pile up old duplicates.
ALTER TABLE public.source_closure
  DROP CONSTRAINT source_closure_pkey,
  ALTER COLUMN claim_id DROP NOT NULL,
  ADD COLUMN synthesis_id uuid REFERENCES public.syntheses (synthesis_id),
  ADD COLUMN depth integer NOT NULL DEFAULT 1 CHECK (depth >= 1),
  ADD COLUMN is_current boolean NOT NULL DEFAULT true,
  ADD CONSTRAINT source_closure_exactly_one_target CHECK (
    (claim_id IS NULL) <> (synthesis_id IS NULL)
  );
CREATE UNIQUE INDEX source_closure_claim_root_unique
  ON public.source_closure (claim_id, root_source_id) WHERE claim_id IS NOT NULL;
CREATE UNIQUE INDEX source_closure_synthesis_root_unique
  ON public.source_closure (synthesis_id, root_source_id) WHERE synthesis_id IS NOT NULL;

-- §6.2.2: authorize only the real producer and immutable event identity columns.
REVOKE UPDATE ON staging.contribution_releases FROM role_private_worker;
GRANT SELECT, INSERT ON staging.contribution_releases TO role_private_worker;
GRANT UPDATE (state, revoked_at) ON staging.contribution_releases TO role_private_worker;
REVOKE UPDATE ON staging.contribution_release_sources FROM role_private_worker;
GRANT SELECT, INSERT ON staging.contribution_release_sources TO role_private_worker;
GRANT INSERT (tenant_id, commit_seq, event_type, contribution_release_id)
  ON ops.outbox TO role_private_worker;
GRANT USAGE ON SEQUENCE ops.commit_seq_seq TO role_private_worker;
-- Gateway must not gain release production merely because outbox gained a column.
REVOKE INSERT ON ops.outbox FROM role_gateway;
GRANT INSERT (tenant_id, commit_seq, stream_seq, event_type, evidence_id)
  ON ops.outbox TO role_gateway;

-- Canonical lock namespace. INVOKER and a pinned search_path preserve actual caller RLS.
CREATE FUNCTION staging.lock_contribution_release(release_id uuid, exclusive_lock boolean)
RETURNS void LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path = pg_catalog AS $$
BEGIN
  -- Release admission/revocation needs a fresh post-lock snapshot. A caller must not
  -- smuggle a pre-lock REPEATABLE READ snapshot through a direct SQL write.
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'release IO requires READ COMMITTED' USING ERRCODE = '40001';
  END IF;
  IF release_id IS NULL OR exclusive_lock IS NULL THEN
    RAISE EXCEPTION 'release lock requires non-null arguments' USING ERRCODE = '22023';
  END IF;
  IF exclusive_lock THEN
    PERFORM pg_catalog.pg_advisory_xact_lock(
      pg_catalog.hashtextextended('contribution-release:' || release_id::text, 0));
  ELSE
    PERFORM pg_catalog.pg_advisory_xact_lock_shared(
      pg_catalog.hashtextextended('contribution-release:' || release_id::text, 0));
  END IF;
END;
$$;

CREATE FUNCTION staging.assert_active_contribution_release(release_id uuid)
RETURNS staging.contribution_releases
LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path = pg_catalog AS $$
DECLARE
  release_row staging.contribution_releases;
BEGIN
  -- VOLATILE's separate SELECT below takes a fresh post-lock snapshot.
  PERFORM staging.lock_contribution_release(release_id, false);
  SELECT r.* INTO release_row FROM staging.contribution_releases r
    WHERE r.contribution_release_id = release_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'release is not visible in job tenant context' USING ERRCODE = '42501';
  END IF;
  IF release_row.state <> 'ACTIVE'
      OR release_row.privacy_scan_outcome <> 'PASSED'
      OR release_row.secret_scan_outcome <> 'PASSED'
      OR btrim(release_row.rights_basis) = '' THEN
    RAISE EXCEPTION 'release is not eligible for public admission' USING ERRCODE = '23514';
  END IF;
  RETURN release_row;
END;
$$;

CREATE FUNCTION staging.guard_contribution_release()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path = pg_catalog AS $$
BEGIN
  PERFORM staging.lock_contribution_release(NEW.contribution_release_id, true);
  IF TG_OP = 'INSERT' THEN
    IF NEW.state <> 'ACTIVE' THEN
      RAISE EXCEPTION 'a release starts ACTIVE' USING ERRCODE = '23514';
    END IF;
    IF NEW.privacy_scan_outcome <> 'PASSED' OR NEW.secret_scan_outcome <> 'PASSED'
        OR NEW.rights_basis !~ '[^[:space:]]'
        OR jsonb_typeof(NEW.policy_snapshot->'consent_version') IS DISTINCT FROM 'string'
        OR COALESCE(NEW.policy_snapshot->>'consent_version', '') !~ '[^[:space:]]' THEN
      RAISE EXCEPTION 'release needs passed scans, rights, and consent version'
        USING ERRCODE = '23514';
    END IF;
  ELSE
    IF (to_jsonb(NEW) - ARRAY['state', 'revoked_at'])
        IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['state', 'revoked_at']) THEN
      RAISE EXCEPTION 'release identity and snapshot are immutable' USING ERRCODE = '23514';
    END IF;
    IF OLD.state = 'REVOKED' AND NEW IS DISTINCT FROM OLD THEN
      RAISE EXCEPTION 'a revoked release is immutable' USING ERRCODE = '23514';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER contribution_release_guard
  BEFORE INSERT OR UPDATE ON staging.contribution_releases
  FOR EACH ROW EXECUTE FUNCTION staging.guard_contribution_release();

CREATE FUNCTION staging.guard_contribution_release_source()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path = pg_catalog AS $$
BEGIN
  PERFORM staging.lock_contribution_release(NEW.contribution_release_id, true);
  IF EXISTS (SELECT 1 FROM ops.outbox o
      WHERE o.contribution_release_id = NEW.contribution_release_id
        AND o.event_type = 'PUBLIC_RELEASE') THEN
    RAISE EXCEPTION 'released source snapshot is immutable' USING ERRCODE = '23514';
  END IF;
  -- FKs prove tenancy even for blind UUIDs; these reads additionally prove visibility.
  IF NEW.evidence_id IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM private.evidence_objects e WHERE e.evidence_id = NEW.evidence_id) THEN
    RAISE EXCEPTION 'evidence is not visible to release producer' USING ERRCODE = '42501';
  END IF;
  IF NEW.memory_id IS NOT NULL AND NOT EXISTS (
      SELECT 1 FROM private.memory_records m WHERE m.memory_id = NEW.memory_id) THEN
    RAISE EXCEPTION 'memory is not visible to release producer' USING ERRCODE = '42501';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER contribution_release_source_guard
  BEFORE INSERT ON staging.contribution_release_sources
  FOR EACH ROW EXECUTE FUNCTION staging.guard_contribution_release_source();

-- Deferred: sources + the matching durable event may follow the state write in the TX.
CREATE FUNCTION staging.require_contribution_outbox()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path = pg_catalog AS $$
DECLARE
  required_event text;
BEGIN
  required_event := CASE WHEN TG_OP = 'INSERT' THEN 'PUBLIC_RELEASE'
    WHEN NEW.state = 'REVOKED' AND OLD.state = 'ACTIVE' THEN 'PUBLIC_REVOKE'
    ELSE NULL END;
  IF required_event IS NOT NULL AND (
      NOT EXISTS (SELECT 1 FROM staging.contribution_release_sources s
        WHERE s.contribution_release_id = NEW.contribution_release_id)
      OR NOT EXISTS (SELECT 1 FROM ops.outbox o
        WHERE o.tenant_id = NEW.tenant_id
          AND o.contribution_release_id = NEW.contribution_release_id
          AND o.event_type = required_event)) THEN
    RAISE EXCEPTION 'release transition needs sources and matching outbox in same transaction'
      USING ERRCODE = '23514';
  END IF;
  RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER contribution_release_requires_outbox
  AFTER INSERT OR UPDATE ON staging.contribution_releases
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
  EXECUTE FUNCTION staging.require_contribution_outbox();

CREATE FUNCTION public.guard_contribution_source()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path = pg_catalog AS $$
DECLARE
  release_row staging.contribution_releases;
BEGIN
  IF TG_OP = 'UPDATE' THEN
    IF ROW(NEW.source_id, NEW.source_type, NEW.contribution_release_id, NEW.content_hash,
        NEW.source_url, NEW.publisher, NEW.source_license, NEW.rights_basis,
        NEW.redistribution_policy, NEW.created_at)
      IS DISTINCT FROM
       ROW(OLD.source_id, OLD.source_type, OLD.contribution_release_id, OLD.content_hash,
        OLD.source_url, OLD.publisher, OLD.source_license, OLD.rights_basis,
        OLD.redistribution_policy, OLD.created_at) THEN
      RAISE EXCEPTION 'public source identity and rights are immutable' USING ERRCODE = '23514';
    END IF;
  ELSIF NEW.source_type = 'USER_CONTRIBUTION' THEN
    release_row := staging.assert_active_contribution_release(NEW.contribution_release_id);
    IF ROW(NEW.publisher, NEW.source_license, NEW.rights_basis, NEW.redistribution_policy)
      IS DISTINCT FROM ROW(release_row.publisher, release_row.source_license,
        release_row.rights_basis, release_row.redistribution_policy) THEN
      RAISE EXCEPTION 'public source rights must match release snapshot' USING ERRCODE = '23514';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER public_source_contribution_guard BEFORE INSERT OR UPDATE ON public.sources
  FOR EACH ROW EXECUTE FUNCTION public.guard_contribution_source();

CREATE FUNCTION public.guard_contribution_provenance()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path = pg_catalog AS $$
DECLARE
  release_id uuid;
BEGIN
  SELECT s.contribution_release_id INTO release_id FROM public.sources s
    WHERE s.source_id = NEW.source_id;
  IF release_id IS NOT NULL THEN
    PERFORM staging.assert_active_contribution_release(release_id);
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER public_provenance_contribution_guard
  BEFORE INSERT OR UPDATE ON public.provenance_edges
  FOR EACH ROW EXECUTE FUNCTION public.guard_contribution_provenance();

CREATE FUNCTION ops.guard_outbox_identity()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path = pg_catalog AS $$
DECLARE
  release_state text;
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.contribution_release_id IS NOT NULL THEN
      PERFORM staging.lock_contribution_release(NEW.contribution_release_id, false);
      SELECT r.state INTO release_state FROM staging.contribution_releases r
        WHERE r.contribution_release_id = NEW.contribution_release_id
          AND r.tenant_id = NEW.tenant_id;
      IF release_state IS NULL OR release_state IS DISTINCT FROM
        (CASE NEW.event_type WHEN 'PUBLIC_RELEASE' THEN 'ACTIVE'
          WHEN 'PUBLIC_REVOKE' THEN 'REVOKED' ELSE NULL END) THEN
        RAISE EXCEPTION 'outbox event does not match visible release transition'
          USING ERRCODE = '23514';
      END IF;
    END IF;
    RETURN NEW;
  END IF;
  IF ROW(NEW.outbox_id, NEW.tenant_id, NEW.commit_seq, NEW.stream_seq, NEW.event_type,
      NEW.evidence_id, NEW.contribution_release_id, NEW.created_at)
    IS DISTINCT FROM
     ROW(OLD.outbox_id, OLD.tenant_id, OLD.commit_seq, OLD.stream_seq, OLD.event_type,
      OLD.evidence_id, OLD.contribution_release_id, OLD.created_at) THEN
    RAISE EXCEPTION 'outbox event identity is immutable' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER outbox_identity_guard BEFORE INSERT OR UPDATE ON ops.outbox
  FOR EACH ROW EXECUTE FUNCTION ops.guard_outbox_identity();

-- All guard code is owned by the migration role, never alterable by runtime roles.
ALTER FUNCTION staging.lock_contribution_release(uuid, boolean) OWNER TO role_migration_owner;
ALTER FUNCTION staging.assert_active_contribution_release(uuid) OWNER TO role_migration_owner;
ALTER FUNCTION staging.guard_contribution_release() OWNER TO role_migration_owner;
ALTER FUNCTION staging.guard_contribution_release_source() OWNER TO role_migration_owner;
ALTER FUNCTION staging.require_contribution_outbox() OWNER TO role_migration_owner;
ALTER FUNCTION public.guard_contribution_source() OWNER TO role_migration_owner;
ALTER FUNCTION public.guard_contribution_provenance() OWNER TO role_migration_owner;
ALTER FUNCTION ops.guard_outbox_identity() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION staging.lock_contribution_release(uuid, boolean),
  staging.assert_active_contribution_release(uuid), staging.guard_contribution_release(),
  staging.guard_contribution_release_source(), staging.require_contribution_outbox(),
  public.guard_contribution_source(), public.guard_contribution_provenance(),
  ops.guard_outbox_identity() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION staging.lock_contribution_release(uuid, boolean),
  staging.assert_active_contribution_release(uuid)
  TO role_private_worker, role_public_worker;
