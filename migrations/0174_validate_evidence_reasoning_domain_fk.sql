-- 0174 — VALIDATE the (tenant_id, reasoning_domain_id) FK that 0159 added NOT VALID.
--
-- 0159 left `evidence_objects_reasoning_domain_tenant_fk` NOT VALID because the shared dev
-- database held two `private.evidence_objects` rows whose (tenant_id, reasoning_domain_id)
-- had no `control.private_reasoning_domains` row — test residue from a throwaway tenant whose
-- cleanup missed them (delivery report §7.2 lists both ids). NOT VALID already refused every
-- NEW violating write; what was missing was the historical proof and the planner's ability to
-- use the constraint. The two rows were dispositioned (backed up, then deleted) with the user's
-- approval on 2026-09-26; this migration turns the constraint's proof on.
--
-- §46 forward-fix (FORWARD_ONLY): VALIDATE takes a SHARE UPDATE EXCLUSIVE lock and scans the
-- table once; it neither changes the schema nor touches rows. On a database where the
-- constraint is already valid (fresh deployments, where 0159's own VALIDATE succeeded) this
-- is a no-op. The manifest's precheck refuses to run while a violating row still exists, so
-- the migration fails BEFORE the scan with a named reason instead of mid-flight.
ALTER TABLE private.evidence_objects
  VALIDATE CONSTRAINT evidence_objects_reasoning_domain_tenant_fk;
