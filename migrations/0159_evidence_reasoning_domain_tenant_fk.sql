-- §11.2.1 / §48.0① / §46 forward-fix / ADR-0032 (card 11): an Evidence's reasoning domain
-- belongs to the Evidence's OWN tenant — enforced by the database, not only by the writer.
--
-- 0004 declared `private.evidence_objects.reasoning_domain_id` with a single-column FK to
-- control.private_reasoning_domains(reasoning_domain_id). RI checks bypass RLS, so nothing at
-- the DB layer refused a row of tenant B that pointed at tenant A's domain. That was harmless
-- while one gateway process wrote for exactly one tenant (the domain was a boot constant of
-- that tenant, ADR-0031 D-B); card 11 lifts `remember.put` to N (tenant, workspace) pairs per
-- process, and the write-side resolve (adapters::remember::resolve_reasoning_domain) now picks
-- the caller tenant's domain per request. This composite FK is the invariant the resolve relies
-- on: a cross-tenant domain is a 23503 even if the resolve is bypassed. 0131 already gave the
-- target table the (tenant_id, reasoning_domain_id) UNIQUE this FK needs — same shape as
-- private.contribution_executions' `contribution_executions_domain_fk`.
--
-- The 0004 single-column FK stays (dropping it changes nothing the composite does not cover
-- and would touch a frozen migration's object for no gain). No DML.
--
-- NOT VALID + VALIDATE is the 0099/0100 lock-class split: ADD ... NOT VALID is O(1) and already
-- enforces the composite for every new write; VALIDATE scans under a lesser lock. The VALIDATE
-- is conditional on the table being clean: a database whose fixture teardown bypassed RI (the
-- shared dev DB carries Evidence rows whose tenant and domain rows are gone — they violate the
-- 0004 FK too) keeps the constraint NOT VALID with a WARNING naming the count, and every NEW
-- write is still refused; production, where RI has always held, ends validated. `VALIDATE`
-- would otherwise fail the whole migration on rows this migration must not delete.

ALTER TABLE private.evidence_objects
  ADD CONSTRAINT evidence_objects_reasoning_domain_tenant_fk
    FOREIGN KEY (tenant_id, reasoning_domain_id)
    REFERENCES control.private_reasoning_domains(tenant_id, reasoning_domain_id) NOT VALID;

DO $$
DECLARE
  orphans bigint;
BEGIN
  SELECT count(*) INTO orphans
  FROM private.evidence_objects e
  WHERE NOT EXISTS (
    SELECT 1 FROM control.private_reasoning_domains d
    WHERE d.tenant_id = e.tenant_id AND d.reasoning_domain_id = e.reasoning_domain_id);
  IF orphans = 0 THEN
    ALTER TABLE private.evidence_objects
      VALIDATE CONSTRAINT evidence_objects_reasoning_domain_tenant_fk;
  ELSE
    RAISE WARNING '0159: % pre-existing evidence_objects row(s) reference no (tenant, reasoning domain) pair; evidence_objects_reasoning_domain_tenant_fk stays NOT VALID (enforced for every new write) until they are repaired and VALIDATE CONSTRAINT is run', orphans;
  END IF;
END
$$;
