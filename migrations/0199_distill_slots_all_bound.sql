-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0198). ADR-0058 ruling R5 (card 32):
-- `--distill-once` says why it stopped. An empty `ops.claim_derived_work_v2` answer means either
-- that every provider slot was bound (READY work may be left behind) or that a slot was free and no
-- READY job could be taken. role_private_worker holds no grant on ops.provider_slots (0190), so
-- the drain asks this owner definer once, right after its empty claim.
--
-- Read by MVCC only, no lock: a probe that locked slot rows would make a concurrent claim's
-- `FOR UPDATE SKIP LOCKED` slot pick miss a free slot. Exposes one boolean about the
-- deployment-wide slot pool, which the claimer already uses; no tenant data.

CREATE FUNCTION ops.distill_slots_all_bound() RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
  SELECT NOT EXISTS (SELECT 1 FROM ops.provider_slots s WHERE s.job_id IS NULL);
$$;

ALTER FUNCTION ops.distill_slots_all_bound() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION ops.distill_slots_all_bound()
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker,
  role_public_worker, role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;
GRANT EXECUTE ON FUNCTION ops.distill_slots_all_bound() TO role_private_worker;

COMMENT ON FUNCTION ops.distill_slots_all_bound() IS
  'ADR-0058 R5 (card 32): true when every one of the four provider slots is bound — the drain''s stopped=no_slot versus stopped=no_work after an empty claim. MVCC read, no lock. Owner definer; EXECUTE role_private_worker only.';
