-- §7.4 / §77 append-only hardening for `ops.data_disclosures` / `ops.data_disclosure_sources`
-- (0047, already applied — checksum-locked, cannot be edited; this migration is the
-- forward-fix, same pattern 0041 Fix 5 used for control.audit_events/ops.audit_batches).
--
-- Fix A (blocker): 0047 only installed `BEFORE UPDATE OR DELETE ... FOR EACH ROW` triggers on
-- both tables. TRUNCATE does not fire a FOR EACH ROW trigger — `begin; truncate
-- ops.data_disclosures cascade; rollback;` bypasses append-only entirely today, cascading to
-- ops.data_disclosure_sources. `ops.data_disclosure_sources_reject_mutation` already RAISEs
-- unconditionally (never reads NEW/OLD), so it needs no change, just the extra trigger.
-- `ops.data_disclosures_guard_mutation` does read NEW/OLD for its UPDATE-only checks, so it is
-- replaced (CREATE OR REPLACE — an ordinary forward-fixing DDL statement, not an edit to
-- 0047's own file) to reject TRUNCATE before ever touching them.
--
-- Fix B (minor): the same replacement closes a second 0047 gap — `deletion_requested_at` /
-- `deletion_confirmed_at` (§37 deletion-propagation receipts) were writable indefinitely after
-- being set, including back to NULL, on a table whose whole point is append-only. §37 only
-- ever appends a receipt once; this pins that as NULL -> value exactly once, mirroring the
-- existing `finalized_at`/`outcome` one-shot check already in this function.

CREATE OR REPLACE FUNCTION ops.data_disclosures_guard_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
    RAISE EXCEPTION 'ops.data_disclosures is append-only (§7.4) — % not permitted', TG_OP
      USING ERRCODE = 'insufficient_privilege';
  END IF;

  -- Identity/reservation columns are fixed for the row's whole lifetime — only finalize()'s
  -- own columns and the §37 deletion-propagation columns may ever change.
  IF OLD.grant_id            IS DISTINCT FROM NEW.grant_id
     OR OLD.tenant_id        IS DISTINCT FROM NEW.tenant_id
     OR OLD.scope_kind       IS DISTINCT FROM NEW.scope_kind
     OR OLD.scope_id         IS DISTINCT FROM NEW.scope_id
     OR OLD.processor_id     IS DISTINCT FROM NEW.processor_id
     OR OLD.region           IS DISTINCT FROM NEW.region
     OR OLD.data_class       IS DISTINCT FROM NEW.data_class
     OR OLD.purpose          IS DISTINCT FROM NEW.purpose
     OR OLD.payload_sha256   IS DISTINCT FROM NEW.payload_sha256
     OR OLD.payload_bytes    IS DISTINCT FROM NEW.payload_bytes
     OR OLD.reserved_at      IS DISTINCT FROM NEW.reserved_at
  THEN
    RAISE EXCEPTION 'ops.data_disclosures identity/reservation columns are immutable after INSERT (§7.4)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;

  -- finalize() may run exactly once: reject a second attempt to change finalized_at/outcome
  -- once already set (no re-finalizing, no backdating).
  IF OLD.finalized_at IS NOT NULL
     AND (OLD.finalized_at IS DISTINCT FROM NEW.finalized_at
          OR OLD.outcome IS DISTINCT FROM NEW.outcome)
  THEN
    RAISE EXCEPTION 'ops.data_disclosures already finalized — finalized_at/outcome cannot change again (§7.4)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;

  -- §37: deletion-propagation receipts append once each, NULL -> value, never rewritten or
  -- cleared again once set (0047 left both columns writable indefinitely).
  IF OLD.deletion_requested_at IS NOT NULL
     AND OLD.deletion_requested_at IS DISTINCT FROM NEW.deletion_requested_at
  THEN
    RAISE EXCEPTION 'ops.data_disclosures.deletion_requested_at is monotonic (§37) — cannot change once set'
      USING ERRCODE = 'insufficient_privilege';
  END IF;

  IF OLD.deletion_confirmed_at IS NOT NULL
     AND OLD.deletion_confirmed_at IS DISTINCT FROM NEW.deletion_confirmed_at
  THEN
    RAISE EXCEPTION 'ops.data_disclosures.deletion_confirmed_at is monotonic (§37) — cannot change once set'
      USING ERRCODE = 'insufficient_privilege';
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER data_disclosures_reject_truncate
BEFORE TRUNCATE ON ops.data_disclosures
FOR EACH STATEMENT EXECUTE FUNCTION ops.data_disclosures_guard_mutation();

CREATE TRIGGER data_disclosure_sources_reject_truncate
BEFORE TRUNCATE ON ops.data_disclosure_sources
FOR EACH STATEMENT EXECUTE FUNCTION ops.data_disclosure_sources_reject_mutation();
