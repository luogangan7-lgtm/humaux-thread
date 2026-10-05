-- §46 FORWARD_ONLY (card 36 S5; ADR-0063 D-B, D-D). ops.model_call_ledger becomes a RANGE (called_at) parent under
-- its own name: the heap is renamed, stripped, attached as one leaf with bounds derived from its data, sealed and
-- registered by control.partition_adopt_leaf, and the current UTC month plus 3 are pre-created by
-- control.partition_create_month (0224). No row is copied or rewritten. No DEFAULT partition.
--
-- Contract changes (ADR-0063 E3):
--   * PK (model_call_id) -> (model_call_id, called_at). The four global UNIQUEs (tenant_id, request_id),
--     (tenant_id, model_call_id), (tenant_id, model_call_id, egress_processor_id) and the 6-column reasoning binding
--     set move, with the global model_call_id PK, to the narrow non-partitioned ops.model_call_identity, filled by the
--     BEFORE INSERT claim trigger; the ledger keeps lookup indexes on (tenant_id, request_id) and
--     (tenant_id, model_call_id) and FOREIGN KEY (model_call_id) -> ops.model_call_identity (D-B invariant 2).
--   * dedupe (ADR-0063 D-B, main-line ruling E3-b): the claim inserts the identity row with no ON CONFLICT, so any
--     duplicate raises 23505 naming the moved former UNIQUE (model_call_identity_request_id_unique for a second
--     (tenant_id, request_id), model_call_identity_pkey for a second model_call_id), after waiting on an uncommitted
--     duplicate as the ledger's own index did, exactly like the events and audit claims; a plain duplicate INSERT
--     keeps erring. The two writers in crates/adapters/src/model_call_ledger.rs drop their ON CONFLICT (tenant_id,
--     request_id) DO NOTHING in the same change and dedupe explicitly: the `model-call-request:` advisory lock, a
--     lookup, an INSERT only when the key is absent, otherwise the existing row.
--   * the five inbound FKs (budget reservations, data disclosures, contribution candidates, contribution executions
--     x2) are re-pointed, names and column lists unchanged, from the ledger to ops.model_call_identity (D-B invariant
--     1: no FK into a partitioned table).
--   * model_call_id joins the guard's immutable columns: with the PK now (model_call_id, called_at), an UPDATE to
--     another call's id in another month would otherwise duplicate an id the old PK kept globally unique.
--   * the guard already freezes called_at (the partition key) and refuses DELETE and TRUNCATE for every role, the
--     owner included, so no key-freeze trigger and no identity release trigger is needed; a partition drop fires no
--     row trigger and identity rows persist as tombstones (D-B invariant 3).
--
-- Functions (caller / fence / owner role_migration_owner; EXECUTE revoked from PUBLIC, no runtime grant):
--   ops.model_call_identity_claim()   caller: trigger zz_model_call_identity_claim (BEFORE INSERT on
--                                     ops.model_call_ledger). Fence: inserts exactly NEW's nine identity columns;
--                                     a duplicate raises 23505 (no skip branch).
--                                     SECURITY DEFINER (four runtime roles insert ledger rows and hold nothing on the
--                                     identity table); FORCE RLS on the identity table applies to the owner, so the
--                                     insert is checked against the caller's tenant GUC, before any unique check,
--                                     like the parent's own policy; only a session whose login role bypasses RLS
--                                     (the migrate / test superuser) has the check scoped to the row's own tenant,
--                                     as the parent's policy never applied to it.
--   ops.model_call_ledger_guard_mutation()  unchanged caller (the row and TRUNCATE guards), invoker as before;
--                                     replaced to freeze model_call_id.
--   private.assert_current_contribution_reservation_authority(ops.model_call_ledger)  0133's definer, body (prosrc)
--                                     and attributes unchanged, re-created on the parent's rowtype: the 0133 one is
--                                     bound to the heap's rowtype, which the RENAME turns into one leaf's (F10 gap:
--                                     pg_depend from pg_proc onto the rowtype, not onto the table). Caller: the
--                                     contribution_reservation_authority_validate trigger. Owner-only EXECUTE.
--
-- Locks (pg_locks before COMMIT on a throwaway, card 36 S5): ACCESS EXCLUSIVE on ops.model_call_ledger (taken first,
--   so no lock upgrade can deadlock with a writer), its leaves and the new ops.model_call_identity; ACCESS EXCLUSIVE
--   also on every table at the other end of a dropped FK (DROP CONSTRAINT removes its RI triggers there): the four
--   referencing tables (ops.data_disclosures, ops.retrieval_provider_budget_reservations,
--   private.contribution_executions, staging.contribution_candidates) and the twelve referenced ones
--   (control.tenants, control.workspaces, control.provider_accounts, control.provider_billing_accounts,
--   control.provider_billing_instruments, control.provider_endpoints, control.reasoning_credential_bindings,
--   control.reasoning_profiles, control.reasoning_route_bindings, control.reasoning_route_policies,
--   ops.reasoning_account_health_observations, ops.reasoning_provider_health_observations); all for the whole
--   transaction, so every writer is stopped first (runbook). Measured on a throwaway restored from a pg_dump of dev
--   and made FK-consistent (61,031 ledger rows, 2,547 RESERVED; 38,190 budget reservations, 8,089 disclosures, 1,071
--   candidates and 1,226 executions re-pointed; 2026-10-05): at most 922 ms from transaction start to the migrate
--   process exit (ADR-0063 "Conversion wall clock").

LOCK TABLE ops.model_call_ledger IN ACCESS EXCLUSIVE MODE;

-- D-D 7b precheck, repeated as body for the test throwaways that apply bodies without manifests.
DO $$
DECLARE
  v_inbound text[];
  v_orphans text;
  v_n       bigint;
  r         record;
BEGIN
  IF (SELECT c.relkind FROM pg_class c WHERE c.oid = 'ops.model_call_ledger'::regclass) <> 'r' THEN
    RAISE EXCEPTION 'c36 precheck: ops.model_call_ledger is not a plain heap';
  END IF;
  -- F10: a view or BEGIN ATOMIC body would stay bound to the renamed heap.
  IF EXISTS (SELECT 1 FROM pg_depend d
              WHERE d.refobjid = 'ops.model_call_ledger'::regclass
                AND d.classid IN ('pg_rewrite'::regclass, 'pg_proc'::regclass)) THEN
    RAISE EXCEPTION 'c36 precheck: a view or function body depends on ops.model_call_ledger';
  END IF;
  -- F10 for the rowtype: a function taking ops.model_call_ledger would stay bound to the renamed heap's type, i.e.
  -- one leaf's rowtype, which a row routed to any other leaf cannot be passed as. Exactly the one this migration
  -- rebinds below.
  IF (SELECT array_agg(d.objid::regprocedure::text) FROM pg_depend d
       WHERE d.refclassid = 'pg_type'::regclass AND d.classid <> 'pg_type'::regclass
         AND d.refobjid = (SELECT c.reltype FROM pg_class c WHERE c.oid = 'ops.model_call_ledger'::regclass))
     IS DISTINCT FROM
     ARRAY['private.assert_current_contribution_reservation_authority(ops.model_call_ledger)'] THEN
    RAISE EXCEPTION 'c36 precheck: objects depending on the ops.model_call_ledger rowtype are not exactly '
      'private.assert_current_contribution_reservation_authority(ops.model_call_ledger)';
  END IF;
  -- F3: exactly the five FKs this migration re-points.
  SELECT array_agg(format('%s.%s', co.conrelid::regclass, co.conname)
                   ORDER BY format('%s.%s', co.conrelid::regclass, co.conname)) INTO v_inbound
    FROM pg_constraint co
   WHERE co.contype = 'f' AND co.confrelid = 'ops.model_call_ledger'::regclass AND co.conparentid = 0;
  IF v_inbound IS DISTINCT FROM ARRAY[
       'ops.data_disclosures.data_disclosures_reasoning_model_call_fk',
       'ops.retrieval_provider_budget_reservations.retrieval_provider_budget_reservations_model_call_fk',
       'private.contribution_executions.contribution_executions_assessment_model_call_fk',
       'private.contribution_executions.contribution_executions_coverage_model_call_fk',
       'staging.contribution_candidates.contribution_candidates_reasoning_model_call_exact_fk'] THEN
    RAISE EXCEPTION 'c36 precheck: foreign keys into ops.model_call_ledger are %, expected the five referencers',
      v_inbound;
  END IF;
  -- Orphan rows (written under session_replication_role = replica) would make a re-created FK refuse mid-way;
  -- refuse up front with a count per FK. Never weakened to NOT VALID-and-leave (main-line dev orphan note). Every FK
  -- this migration re-creates is one of these (12 outbound, 5 inbound), each MATCH SIMPLE: a row with a NULL in its
  -- columns references nothing.
  FOR r IN
    SELECT co.conname, co.conrelid::regclass AS src, co.confrelid::regclass AS dst, co.confmatchtype,
           (SELECT string_agg(format('s.%I IS NOT NULL', a.attname), ' AND ' ORDER BY k.n)
              FROM unnest(co.conkey) WITH ORDINALITY k(att, n)
              JOIN pg_attribute a ON a.attrelid = co.conrelid AND a.attnum = k.att) AS present,
           (SELECT string_agg(format('d.%I = s.%I', b.attname, a.attname), ' AND ' ORDER BY k.n)
              FROM unnest(co.conkey, co.confkey) WITH ORDINALITY k(att, ref, n)
              JOIN pg_attribute a ON a.attrelid = co.conrelid AND a.attnum = k.att
              JOIN pg_attribute b ON b.attrelid = co.confrelid AND b.attnum = k.ref) AS joined
      FROM pg_constraint co
     WHERE co.contype = 'f' AND co.conparentid = 0
       AND 'ops.model_call_ledger'::regclass IN (co.conrelid, co.confrelid)
     ORDER BY co.conname
  LOOP
    IF r.confmatchtype <> 's' THEN
      RAISE EXCEPTION 'c36 precheck: % is not MATCH SIMPLE', r.conname;
    END IF;
    EXECUTE format('SELECT count(*) FROM %s s WHERE %s AND NOT EXISTS (SELECT 1 FROM %s d WHERE %s)', r.src,
                   r.present, r.dst, r.joined) INTO v_n;
    IF v_n > 0 THEN
      v_orphans := concat_ws('; ', v_orphans, format('%s: %s orphan rows - repair data first', r.conname, v_n));
    END IF;
  END LOOP;
  IF v_orphans IS NOT NULL THEN
    RAISE EXCEPTION 'c36 precheck: %', v_orphans;
  END IF;
END
$$;

-- D-D step 1: the snapshot only the 7a block below reads (never a manifest check: migration-rehearsal EXPLAINs those
-- at HEAD, where these temp tables do not exist).
CREATE TEMP TABLE c36_pre_ledger ON COMMIT DROP AS
  SELECT count(*) AS n, count(*) FILTER (WHERE status = 'RESERVED') AS reserved FROM ops.model_call_ledger;
CREATE TEMP TABLE c36_fp_ledger ON COMMIT DROP AS
  SELECT * FROM control.partition_catalog_fingerprint('ops.model_call_ledger');

-- D-D step 2 / D-B: the identity table, backfilled from the heap, and the five inbound FKs re-pointed to it.
CREATE TABLE ops.model_call_identity (
  model_call_id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  request_id uuid NOT NULL,
  called_at timestamptz NOT NULL,
  egress_processor_id uuid,
  binding_id uuid,
  binding_version bigint,
  reasoning_domain_id uuid,
  profile_version bigint,
  CONSTRAINT model_call_identity_request_id_unique UNIQUE (tenant_id, request_id),
  CONSTRAINT model_call_identity_tenant_model_call_unique UNIQUE (tenant_id, model_call_id),
  CONSTRAINT model_call_identity_reasoning_recipient_unique UNIQUE (tenant_id, model_call_id, egress_processor_id),
  CONSTRAINT model_call_identity_reasoning_binding_unique
    UNIQUE (tenant_id, model_call_id, binding_id, binding_version, reasoning_domain_id, profile_version)
);
COMMENT ON TABLE ops.model_call_identity IS
  'ADR-0063 D-B: one row per ops.model_call_ledger row ever inserted; the FK target of the five referencers and the '
  'global uniqueness of model_call_id and (tenant_id, request_id). Partition drops leave its rows as tombstones.';
INSERT INTO ops.model_call_identity (model_call_id, tenant_id, request_id, called_at, egress_processor_id, binding_id,
                                     binding_version, reasoning_domain_id, profile_version)
  SELECT l.model_call_id, l.tenant_id, l.request_id, l.called_at, l.egress_processor_id, l.binding_id,
         l.binding_version, l.reasoning_domain_id, l.profile_version
    FROM ops.model_call_ledger l;
ALTER TABLE ops.model_call_identity OWNER TO role_migration_owner;
-- §62 four items: the parent's tenant policy verbatim, forced, so the owner's claim needs the tenant GUC the parent
-- insert needs.
ALTER TABLE ops.model_call_identity ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.model_call_identity FORCE ROW LEVEL SECURITY;
CREATE POLICY model_call_identity_tenant_isolation ON ops.model_call_identity
  USING (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid)
  WITH CHECK (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid);
-- Reached only through the owner's claim trigger and the referencers' FKs. 0011's default privileges fired for the
-- migrate principal: revoke them all.
REVOKE ALL ON ops.model_call_identity
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin, role_health_reader;

ALTER TABLE ops.retrieval_provider_budget_reservations DROP CONSTRAINT retrieval_provider_budget_reservations_model_call_fk;
ALTER TABLE ops.retrieval_provider_budget_reservations
  ADD CONSTRAINT retrieval_provider_budget_reservations_model_call_fk FOREIGN KEY (tenant_id, model_call_id)
    REFERENCES ops.model_call_identity (tenant_id, model_call_id) NOT VALID;
ALTER TABLE ops.retrieval_provider_budget_reservations
  VALIDATE CONSTRAINT retrieval_provider_budget_reservations_model_call_fk;
ALTER TABLE ops.data_disclosures DROP CONSTRAINT data_disclosures_reasoning_model_call_fk;
ALTER TABLE ops.data_disclosures
  ADD CONSTRAINT data_disclosures_reasoning_model_call_fk FOREIGN KEY (tenant_id, model_call_id, processor_id)
    REFERENCES ops.model_call_identity (tenant_id, model_call_id, egress_processor_id) NOT VALID;
ALTER TABLE ops.data_disclosures VALIDATE CONSTRAINT data_disclosures_reasoning_model_call_fk;
ALTER TABLE staging.contribution_candidates DROP CONSTRAINT contribution_candidates_reasoning_model_call_exact_fk;
ALTER TABLE staging.contribution_candidates
  ADD CONSTRAINT contribution_candidates_reasoning_model_call_exact_fk
    FOREIGN KEY (tenant_id, model_call_id, binding_id, binding_version, reasoning_domain_id, profile_version)
    REFERENCES ops.model_call_identity
      (tenant_id, model_call_id, binding_id, binding_version, reasoning_domain_id, profile_version) NOT VALID;
ALTER TABLE staging.contribution_candidates VALIDATE CONSTRAINT contribution_candidates_reasoning_model_call_exact_fk;
ALTER TABLE private.contribution_executions DROP CONSTRAINT contribution_executions_coverage_model_call_fk;
ALTER TABLE private.contribution_executions DROP CONSTRAINT contribution_executions_assessment_model_call_fk;
ALTER TABLE private.contribution_executions
  ADD CONSTRAINT contribution_executions_coverage_model_call_fk FOREIGN KEY (tenant_id, coverage_model_call_id)
    REFERENCES ops.model_call_identity (tenant_id, model_call_id) NOT VALID;
ALTER TABLE private.contribution_executions
  ADD CONSTRAINT contribution_executions_assessment_model_call_fk FOREIGN KEY (tenant_id, assessment_model_call_id)
    REFERENCES ops.model_call_identity (tenant_id, model_call_id) NOT VALID;
ALTER TABLE private.contribution_executions VALIDATE CONSTRAINT contribution_executions_coverage_model_call_fk;
ALTER TABLE private.contribution_executions VALIDATE CONSTRAINT contribution_executions_assessment_model_call_fk;

-- D-D step 3: the parent re-creates policy, triggers, PK, indexes and FKs; ATTACH clones the FKs, indexes and row
-- triggers onto the leaf. CHECK and NOT NULL constraints stay (ATTACH requires and merges them).
ALTER TABLE ops.model_call_ledger RENAME TO model_call_ledger_legacy;
DROP POLICY model_call_ledger_tenant_isolation ON ops.model_call_ledger_legacy;
DROP TRIGGER contribution_reservation_authority_validate ON ops.model_call_ledger_legacy;
DROP TRIGGER model_call_ledger_guard_mutation ON ops.model_call_ledger_legacy;
DROP TRIGGER model_call_ledger_private_disclosure_present ON ops.model_call_ledger_legacy;
DROP TRIGGER model_call_ledger_reject_truncate ON ops.model_call_ledger_legacy;
DROP TRIGGER reasoning_model_call_validate ON ops.model_call_ledger_legacy;
ALTER TABLE ops.model_call_ledger_legacy DROP CONSTRAINT model_call_ledger_pkey;
ALTER TABLE ops.model_call_ledger_legacy DROP CONSTRAINT model_call_ledger_request_id_unique;
ALTER TABLE ops.model_call_ledger_legacy DROP CONSTRAINT model_call_ledger_tenant_model_call_unique;
ALTER TABLE ops.model_call_ledger_legacy DROP CONSTRAINT model_call_ledger_reasoning_recipient_unique;
ALTER TABLE ops.model_call_ledger_legacy DROP CONSTRAINT model_call_ledger_reasoning_binding_unique;
ALTER TABLE ops.model_call_ledger_legacy DROP CONSTRAINT model_call_ledger_reasoning_account_health_fk;
ALTER TABLE ops.model_call_ledger_legacy DROP CONSTRAINT model_call_ledger_reasoning_billing_account_fk;
ALTER TABLE ops.model_call_ledger_legacy DROP CONSTRAINT model_call_ledger_reasoning_billing_instrument_fk;
ALTER TABLE ops.model_call_ledger_legacy DROP CONSTRAINT model_call_ledger_reasoning_binding_fk;
ALTER TABLE ops.model_call_ledger_legacy DROP CONSTRAINT model_call_ledger_reasoning_credential_authority_fk;
ALTER TABLE ops.model_call_ledger_legacy DROP CONSTRAINT model_call_ledger_reasoning_endpoint_fk;
ALTER TABLE ops.model_call_ledger_legacy DROP CONSTRAINT model_call_ledger_reasoning_policy_fk;
ALTER TABLE ops.model_call_ledger_legacy DROP CONSTRAINT model_call_ledger_reasoning_profile_fk;
ALTER TABLE ops.model_call_ledger_legacy DROP CONSTRAINT model_call_ledger_reasoning_provider_account_fk;
ALTER TABLE ops.model_call_ledger_legacy DROP CONSTRAINT model_call_ledger_reasoning_provider_health_fk;
ALTER TABLE ops.model_call_ledger_legacy DROP CONSTRAINT model_call_ledger_tenant_id_fkey;
ALTER TABLE ops.model_call_ledger_legacy DROP CONSTRAINT model_call_ledger_workspace_id_fkey;
DROP INDEX ops.idx_model_call_ledger_tenant;

-- D-D step 4: LIKE is not enough (R-36); everything else is explicit, in the legacy definitions' own text.
CREATE TABLE ops.model_call_ledger (
  LIKE ops.model_call_ledger_legacy INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING GENERATED INCLUDING STORAGE
    INCLUDING COMMENTS INCLUDING COMPRESSION,
  CONSTRAINT model_call_ledger_pkey PRIMARY KEY (model_call_id, called_at)
) PARTITION BY RANGE (called_at);
COMMENT ON COLUMN ops.model_call_ledger.request_id IS
  'Caller-supplied idempotency key for the logical call (distinct from model_call_id, the row''s own identity) — a '
  'retry that reuses the same request_id hits ops.model_call_identity''s model_call_identity_request_id_unique '
  'through the zz_model_call_identity_claim trigger, which raises 23505, instead of producing a second reservation; '
  'the writers look the key up under the model-call-request advisory lock first (ADR-0063 D-B, ruling E3-b).';
ALTER TABLE ops.model_call_ledger
  ADD CONSTRAINT model_call_ledger_reasoning_account_health_fk FOREIGN KEY (tenant_id, account_health_observation_id)
    REFERENCES ops.reasoning_account_health_observations (tenant_id, observation_id);
ALTER TABLE ops.model_call_ledger
  ADD CONSTRAINT model_call_ledger_reasoning_billing_account_fk FOREIGN KEY (tenant_id, billing_account_id)
    REFERENCES control.provider_billing_accounts (tenant_id, billing_account_id);
ALTER TABLE ops.model_call_ledger
  ADD CONSTRAINT model_call_ledger_reasoning_billing_instrument_fk FOREIGN KEY (tenant_id, billing_instrument_id)
    REFERENCES control.provider_billing_instruments (tenant_id, billing_instrument_id);
ALTER TABLE ops.model_call_ledger
  ADD CONSTRAINT model_call_ledger_reasoning_binding_fk FOREIGN KEY (tenant_id, binding_id, binding_version)
    REFERENCES control.reasoning_route_bindings (tenant_id, binding_id, binding_version);
ALTER TABLE ops.model_call_ledger
  ADD CONSTRAINT model_call_ledger_reasoning_credential_authority_fk
    FOREIGN KEY (tenant_id, credential_ref, provider_account_id)
    REFERENCES control.reasoning_credential_bindings (tenant_id, credential_ref, provider_account_id);
ALTER TABLE ops.model_call_ledger
  ADD CONSTRAINT model_call_ledger_reasoning_endpoint_fk
    FOREIGN KEY (tenant_id, provider_endpoint_id, egress_processor_id)
    REFERENCES control.provider_endpoints (tenant_id, endpoint_id, egress_processor_id);
ALTER TABLE ops.model_call_ledger
  ADD CONSTRAINT model_call_ledger_reasoning_policy_fk FOREIGN KEY (tenant_id, route_policy_id, route_policy_version)
    REFERENCES control.reasoning_route_policies (tenant_id, route_policy_id, policy_version);
ALTER TABLE ops.model_call_ledger
  ADD CONSTRAINT model_call_ledger_reasoning_profile_fk FOREIGN KEY (tenant_id, profile_id, profile_version)
    REFERENCES control.reasoning_profiles (tenant_id, profile_id, profile_version);
ALTER TABLE ops.model_call_ledger
  ADD CONSTRAINT model_call_ledger_reasoning_provider_account_fk FOREIGN KEY (tenant_id, provider_account_id)
    REFERENCES control.provider_accounts (tenant_id, provider_account_id);
ALTER TABLE ops.model_call_ledger
  ADD CONSTRAINT model_call_ledger_reasoning_provider_health_fk
    FOREIGN KEY (tenant_id, provider_health_observation_id)
    REFERENCES ops.reasoning_provider_health_observations (tenant_id, observation_id);
ALTER TABLE ops.model_call_ledger
  ADD CONSTRAINT model_call_ledger_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES control.tenants (tenant_id);
ALTER TABLE ops.model_call_ledger
  ADD CONSTRAINT model_call_ledger_workspace_id_fkey FOREIGN KEY (workspace_id)
    REFERENCES control.workspaces (workspace_id);
-- The rowtype rebind (precheck above): 0133's authority check, called with NEW by the
-- contribution_reservation_authority_validate trigger on every leaf, takes the parent's rowtype, to which each leaf's
-- row coerces (a partition's rowtype converts to its parent's). Same body (prosrc), same attributes as 0133.
DO $$
BEGIN
  EXECUTE format(
    'CREATE FUNCTION private.assert_current_contribution_reservation_authority(p_call ops.model_call_ledger) '
    'RETURNS void LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog AS %L',
    (SELECT p.prosrc FROM pg_proc p
      WHERE p.oid = 'private.assert_current_contribution_reservation_authority(ops.model_call_ledger_legacy)'
                    ::regprocedure));
END
$$;
DROP FUNCTION private.assert_current_contribution_reservation_authority(ops.model_call_ledger_legacy);
ALTER FUNCTION private.assert_current_contribution_reservation_authority(ops.model_call_ledger)
  OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION private.assert_current_contribution_reservation_authority(ops.model_call_ledger)
FROM PUBLIC, role_admin, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance;

-- D-B invariant 2: every ledger row has its identity row (declared addition in the 7a block).
ALTER TABLE ops.model_call_ledger
  ADD CONSTRAINT model_call_ledger_model_call_id_fkey FOREIGN KEY (model_call_id)
    REFERENCES ops.model_call_identity (model_call_id);
CREATE INDEX idx_model_call_ledger_tenant ON ops.model_call_ledger USING btree (tenant_id);
-- L2: the writers' lookups by (tenant_id, request_id) and the referencers' (tenant_id, model_call_id) probe every
-- leaf through these.
CREATE INDEX model_call_ledger_tenant_request_idx ON ops.model_call_ledger USING btree (tenant_id, request_id);
CREATE INDEX model_call_ledger_tenant_model_call_idx ON ops.model_call_ledger USING btree (tenant_id, model_call_id);
ALTER TABLE ops.model_call_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.model_call_ledger FORCE ROW LEVEL SECURITY;
CREATE POLICY model_call_ledger_tenant_isolation ON ops.model_call_ledger
  USING (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid)
  WITH CHECK (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid);
-- The legacy triggers in their own text. The row ones are cloned onto every leaf; the TRUNCATE guard is copied onto
-- each leaf by control.partition_adopt_leaf (P5).
-- c36:insert-validator begin
CREATE TRIGGER contribution_reservation_authority_validate BEFORE INSERT ON ops.model_call_ledger
  FOR EACH ROW WHEN ((new.purpose = 'CONTRIBUTION_DEIDENTIFY'::text))
  EXECUTE FUNCTION ops.contribution_reservation_authority_validate();
-- c36:insert-validator end
CREATE TRIGGER model_call_ledger_guard_mutation BEFORE DELETE OR UPDATE ON ops.model_call_ledger
  FOR EACH ROW EXECUTE FUNCTION ops.model_call_ledger_guard_mutation();
-- c36:deferred-disclosure-trigger begin
CREATE CONSTRAINT TRIGGER model_call_ledger_private_disclosure_present AFTER INSERT ON ops.model_call_ledger
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
  WHEN ((new.purpose = ANY (ARRAY['PRIVATE_DISTILL_TEXT'::text, 'PRIVATE_DISTILL_VISION'::text,
                                  'PRIVATE_CONSOLIDATE'::text])))
  EXECUTE FUNCTION ops.model_call_ledger_private_disclosure_present();
-- c36:deferred-disclosure-trigger end
CREATE TRIGGER model_call_ledger_reject_truncate BEFORE TRUNCATE ON ops.model_call_ledger
  FOR EACH STATEMENT EXECUTE FUNCTION ops.model_call_ledger_guard_mutation();
CREATE TRIGGER reasoning_model_call_validate BEFORE INSERT ON ops.model_call_ledger
  FOR EACH ROW EXECUTE FUNCTION ops.reasoning_model_call_validate();

CREATE FUNCTION ops.model_call_identity_claim()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  v_guc     text := current_setting('humaux.tenant_id', true);
  v_bypass  boolean := (SELECT r.rolsuper OR r.rolbypassrls FROM pg_roles r WHERE r.rolname = session_user);
BEGIN
  -- FORCE RLS on the identity table applies to this owner definer: a session subject to RLS is checked against its
  -- own tenant GUC (WITH CHECK runs before the unique check, so another tenant's row is refused 42501, as the parent's
  -- policy refuses it). A session that bypasses RLS on the parent gets the row's own tenant for this one insert.
  IF v_bypass THEN
    PERFORM set_config('humaux.tenant_id', NEW.tenant_id::text, true);
  END IF;
  -- ADR-0063 D-B, ruling E3-b: any duplicate raises 23505 on the identity table's copy of the former UNIQUE (it waits
  -- on an uncommitted duplicate as the ledger's own index did); no skip branch, like the events and audit claims, so
  -- a plain duplicate INSERT keeps erring. The writers dedupe before inserting (crates/adapters model_call_ledger).
  INSERT INTO ops.model_call_identity (model_call_id, tenant_id, request_id, called_at, egress_processor_id,
                                       binding_id, binding_version, reasoning_domain_id, profile_version)
  VALUES (NEW.model_call_id, NEW.tenant_id, NEW.request_id, NEW.called_at, NEW.egress_processor_id, NEW.binding_id,
          NEW.binding_version, NEW.reasoning_domain_id, NEW.profile_version);
  IF v_bypass THEN
    PERFORM set_config('humaux.tenant_id', coalesce(v_guc, ''), true);
  END IF;
  RETURN NEW;
END;
$$;
ALTER FUNCTION ops.model_call_identity_claim() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION ops.model_call_identity_claim() FROM PUBLIC;
-- `zz_`: PostgreSQL fires same-timing row triggers in name order (P2), so the claim runs after both BEFORE INSERT
-- validators: validate, then claim the identity, as the former unique index was checked after them.
CREATE TRIGGER zz_model_call_identity_claim BEFORE INSERT ON ops.model_call_ledger
  FOR EACH ROW EXECUTE FUNCTION ops.model_call_identity_claim();

-- 0168's guard with model_call_id added to the immutable identity columns (contract change above).
CREATE OR REPLACE FUNCTION ops.model_call_ledger_guard_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
  IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
    RAISE EXCEPTION 'ops.model_call_ledger is append-only (§19.1) — % not permitted', TG_OP
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF OLD.model_call_id IS DISTINCT FROM NEW.model_call_id
     OR OLD.request_id IS DISTINCT FROM NEW.request_id
     OR OLD.tenant_id IS DISTINCT FROM NEW.tenant_id
     OR OLD.workspace_id IS DISTINCT FROM NEW.workspace_id
     OR OLD.purpose IS DISTINCT FROM NEW.purpose
     OR OLD.provider IS DISTINCT FROM NEW.provider
     OR OLD.model IS DISTINCT FROM NEW.model
     OR OLD.model_revision IS DISTINCT FROM NEW.model_revision
     OR OLD.called_at IS DISTINCT FROM NEW.called_at
     OR OLD.estimated_cost IS DISTINCT FROM NEW.estimated_cost
     OR OLD.reasoning_domain_id IS DISTINCT FROM NEW.reasoning_domain_id
     OR OLD.call_kind IS DISTINCT FROM NEW.call_kind
     OR OLD.intent_sha256 IS DISTINCT FROM NEW.intent_sha256
     OR OLD.binding_id IS DISTINCT FROM NEW.binding_id
     OR OLD.binding_version IS DISTINCT FROM NEW.binding_version
     OR OLD.route_policy_id IS DISTINCT FROM NEW.route_policy_id
     OR OLD.route_policy_version IS DISTINCT FROM NEW.route_policy_version
     OR OLD.profile_id IS DISTINCT FROM NEW.profile_id
     OR OLD.profile_version IS DISTINCT FROM NEW.profile_version
     OR OLD.provider_account_id IS DISTINCT FROM NEW.provider_account_id
     OR OLD.provider_endpoint_id IS DISTINCT FROM NEW.provider_endpoint_id
     OR OLD.egress_processor_id IS DISTINCT FROM NEW.egress_processor_id
     OR OLD.credential_ref IS DISTINCT FROM NEW.credential_ref
     OR OLD.billing_account_id IS DISTINCT FROM NEW.billing_account_id
     OR OLD.billing_instrument_id IS DISTINCT FROM NEW.billing_instrument_id
     OR OLD.provider_health_observation_id IS DISTINCT FROM NEW.provider_health_observation_id
     OR OLD.account_health_observation_id IS DISTINCT FROM NEW.account_health_observation_id
     OR OLD.billing_responsibility IS DISTINCT FROM NEW.billing_responsibility
     OR OLD.admitted_at IS DISTINCT FROM NEW.admitted_at THEN
    RAISE EXCEPTION 'ops.model_call_ledger identity/reservation columns are immutable after INSERT (§19.1)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF OLD.status <> 'RESERVED' AND NEW.status IS DISTINCT FROM OLD.status THEN
    RAISE EXCEPTION 'ops.model_call_ledger already finalized — status cannot change again (§19.1)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF (OLD.input_tokens IS NOT NULL AND OLD.input_tokens IS DISTINCT FROM NEW.input_tokens)
  OR (OLD.output_tokens IS NOT NULL AND OLD.output_tokens IS DISTINCT FROM NEW.output_tokens)
  OR (OLD.billable_tokens IS NOT NULL AND OLD.billable_tokens IS DISTINCT FROM NEW.billable_tokens)
  OR (OLD.candidate_count IS NOT NULL AND OLD.candidate_count IS DISTINCT FROM NEW.candidate_count)
  OR (OLD.candidate_tokens IS NOT NULL AND OLD.candidate_tokens IS DISTINCT FROM NEW.candidate_tokens)
  OR (OLD.cache_hit IS NOT NULL AND OLD.cache_hit IS DISTINCT FROM NEW.cache_hit)
  OR (OLD.latency_ms IS NOT NULL AND OLD.latency_ms IS DISTINCT FROM NEW.latency_ms)
  OR (OLD.actual_cost IS NOT NULL AND OLD.actual_cost IS DISTINCT FROM NEW.actual_cost)
  OR (OLD.error_class IS NOT NULL AND OLD.error_class IS DISTINCT FROM NEW.error_class)
  OR (OLD.provider_request_id IS NOT NULL AND OLD.provider_request_id IS DISTINCT FROM NEW.provider_request_id) THEN
    RAISE EXCEPTION 'ops.model_call_ledger outcome columns can only be set once, from NULL (§19.1)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF OLD.purpose = 'CONTRIBUTION_DEIDENTIFY' AND NEW.actual_cost IS NOT NULL THEN
    RAISE EXCEPTION 'USER-paid reasoning calls never record platform actual cost'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

ALTER TABLE ops.model_call_ledger OWNER TO role_migration_owner;
-- 0011's default privileges fired for the migrate principal: revoke them, then the exact legacy cells.
REVOKE ALL ON ops.model_call_ledger
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin, role_health_reader;
GRANT SELECT, INSERT, UPDATE ON ops.model_call_ledger
TO role_gateway, role_private_worker, role_public_worker, role_retrieval_worker;
GRANT SELECT ON ops.model_call_ledger TO role_consolidation_worker, role_maintenance;

-- D-D steps 5-6: bounds from data, all UTC. B = the month after max(key, now()); L = MINVALUE when history predates
-- the current month (the transitional `_hist` leaf, L4), else the current month (an empty table: production).
DO $$
DECLARE
  v_month timestamptz := date_trunc('month', now(), 'UTC');
  v_min   timestamptz;
  v_max   timestamptz;
  v_lower timestamptz;
  v_upper timestamptz;
  v_leaf  text;
  v_rel   regclass;
BEGIN
  SELECT min(called_at), max(called_at) INTO v_min, v_max FROM ops.model_call_ledger_legacy;
  v_upper := date_add(date_trunc('month', greatest(v_max, now()), 'UTC'), interval '1 month', 'UTC');
  v_lower := CASE WHEN v_min < v_month THEN NULL ELSE v_month END;
  -- `_pYYYYMM` only for exactly one UTC month; anything wider is the transitional `_hist` leaf (L4). Renamed before
  -- ATTACH so the indexes ATTACH builds carry the leaf's own name.
  v_leaf := CASE WHEN v_lower IS NOT NULL AND v_upper = date_add(v_lower, interval '1 month', 'UTC')
                 THEN 'model_call_ledger_p' || to_char(v_lower AT TIME ZONE 'UTC', 'YYYYMM')
                 ELSE 'model_call_ledger_hist' END;
  EXECUTE format('ALTER TABLE ops.model_call_ledger_legacy RENAME TO %I', v_leaf);
  v_rel := format('ops.%I', v_leaf)::regclass;
  -- A valid implying CHECK lets ATTACH skip its scan (P7); the partition constraint replaces it afterwards.
  EXECUTE format('ALTER TABLE %s ADD CONSTRAINT model_call_ledger_legacy_bound'
                 ' CHECK (called_at IS NOT NULL AND called_at < %L%s) NOT VALID', v_rel, v_upper,
                 CASE WHEN v_lower IS NULL THEN '' ELSE format(' AND called_at >= %L', v_lower) END);
  EXECUTE format('ALTER TABLE %s VALIDATE CONSTRAINT model_call_ledger_legacy_bound', v_rel);
  EXECUTE format('ALTER TABLE ops.model_call_ledger ATTACH PARTITION %s FOR VALUES FROM (%s) TO (%L)', v_rel,
                 coalesce(quote_literal(v_lower), 'MINVALUE'), v_upper);
  EXECUTE format('ALTER TABLE %s DROP CONSTRAINT model_call_ledger_legacy_bound', v_rel);
  PERFORM control.partition_adopt_leaf('MODEL_CALL_LEDGER', v_rel);
  -- D-F: the current month plus 3 future months; never a DEFAULT partition.
  WHILE v_upper < date_add(v_month, interval '4 months', 'UTC') LOOP
    PERFORM control.partition_create_month('MODEL_CALL_LEDGER', v_upper);
    v_upper := date_add(v_upper, interval '1 month', 'UTC');
  END LOOP;
END
$$;

-- D-D 7a: last statement. Rows, RESERVED rows, identity rows and the normalised catalog (columns, CHECK/FK,
-- triggers, policies, RLS flags, owner, ACL) must equal the snapshot, the declared additions aside (the claim
-- trigger and the identity FK); any drift aborts the whole transaction.
DO $$
DECLARE
  v_drift text;
BEGIN
  IF (SELECT count(*) FROM ops.model_call_ledger) <> (SELECT p.n FROM c36_pre_ledger p)
     OR (SELECT count(*) FROM ops.model_call_ledger WHERE status = 'RESERVED')
        <> (SELECT p.reserved FROM c36_pre_ledger p) THEN
    RAISE EXCEPTION 'c36 count drift: ops.model_call_ledger % rows (% RESERVED), % (% RESERVED) before',
      (SELECT count(*) FROM ops.model_call_ledger),
      (SELECT count(*) FROM ops.model_call_ledger WHERE status = 'RESERVED'),
      (SELECT p.n FROM c36_pre_ledger p), (SELECT p.reserved FROM c36_pre_ledger p);
  END IF;
  IF (SELECT count(*) FROM ops.model_call_identity) <> (SELECT count(*) FROM ops.model_call_ledger) THEN
    RAISE EXCEPTION 'c36 identity drift: ops.model_call_identity % rows, ops.model_call_ledger %',
      (SELECT count(*) FROM ops.model_call_identity), (SELECT count(*) FROM ops.model_call_ledger);
  END IF;
  CREATE TEMP TABLE c36_fp_now ON COMMIT DROP AS
    SELECT * FROM control.partition_catalog_fingerprint('ops.model_call_ledger') f
     WHERE NOT ((f.kind = 'trigger'
                 AND starts_with(f.item, 'CREATE TRIGGER zz_model_call_identity_claim BEFORE INSERT '))
                OR (f.kind = 'constraint'
                    AND f.item = 'model_call_ledger_model_call_id_fkey FOREIGN KEY (model_call_id) '
                                 'REFERENCES ops.model_call_identity(model_call_id)'));
  IF (SELECT count(*) FROM control.partition_catalog_fingerprint('ops.model_call_ledger'))
     - (SELECT count(*) FROM c36_fp_now) <> 2 THEN
    RAISE EXCEPTION 'c36 fingerprint drift: ops.model_call_ledger declared additions (zz_model_call_identity_claim, '
      'model_call_ledger_model_call_id_fkey) not all present';
  END IF;
  SELECT format('%s %s: %s', d.side, d.kind, d.item) INTO v_drift
    FROM ((SELECT 'missing' AS side, s.kind, s.item FROM c36_fp_ledger s
           EXCEPT ALL SELECT 'missing', n.kind, n.item FROM c36_fp_now n)
          UNION ALL
          (SELECT 'unexpected', n.kind, n.item FROM c36_fp_now n
           EXCEPT ALL SELECT 'unexpected', s.kind, s.item FROM c36_fp_ledger s)) d
   ORDER BY d.side, d.kind, d.item LIMIT 1;
  IF v_drift IS NOT NULL THEN
    RAISE EXCEPTION 'c36 fingerprint drift: ops.model_call_ledger %', v_drift;
  END IF;
  DROP TABLE c36_fp_now;
END
$$;
