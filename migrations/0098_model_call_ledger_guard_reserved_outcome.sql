-- §19.1 ModelCallLedger follow-up (T7.4 remediation, code-review finding #6, minor). 0094's
-- `ops.model_call_ledger_guard_mutation` only protects the outcome columns
-- (input_tokens/billable_tokens/candidate_count/candidate_tokens/cache_hit/latency_ms/
-- actual_cost/error_class/provider_request_id) once `OLD.status <> 'RESERVED'` — while a row is
-- still RESERVED, any of the four runtime roles holding `arw` on this table can rewrite those
-- columns repeatedly with no trace. Verified live: `UPDATE ops.model_call_ledger SET
-- actual_cost = 2.0, billable_tokens = 7 WHERE status = 'RESERVED'` succeeded (twice), restored
-- afterwards.
--
-- Cannot edit 0094's trigger function definition in place (already applied, rule ③) —
-- `CREATE OR REPLACE FUNCTION` on the same function name/signature is the standard forward-only
-- way to change a trigger's body without dropping and recreating the trigger object itself
-- (the `CREATE TRIGGER` from 0094 keeps pointing at this function by name; no
-- `DROP TRIGGER`/`CREATE TRIGGER` needed here).
--
-- Fix shape follows the fix_hint exactly: reject any UPDATE where an outcome column goes from
-- NON-NULL to a *different* non-null value, regardless of `status` — this is strictly tighter
-- than "outcome columns are RESERVED-writable" while still allowing finalize()'s own NULL ->
-- value transition (the only legitimate write these columns ever need) and allowing the
-- already-guarded post-finalize case to keep working exactly as before (OLD is never NULL once
-- finalize() has run, so "goes from NON-NULL to a different value" already covered that case;
-- this migration only widens the same rule to also apply while status = 'RESERVED').
CREATE OR REPLACE FUNCTION ops.model_call_ledger_guard_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'ops.model_call_ledger is append-only (§19.1) — DELETE not permitted'
      USING ERRCODE = 'insufficient_privilege';
  END IF;

  IF OLD.request_id      IS DISTINCT FROM NEW.request_id
     OR OLD.tenant_id     IS DISTINCT FROM NEW.tenant_id
     OR OLD.workspace_id  IS DISTINCT FROM NEW.workspace_id
     OR OLD.purpose       IS DISTINCT FROM NEW.purpose
     OR OLD.provider      IS DISTINCT FROM NEW.provider
     OR OLD.model         IS DISTINCT FROM NEW.model
     OR OLD.model_revision IS DISTINCT FROM NEW.model_revision
     OR OLD.called_at     IS DISTINCT FROM NEW.called_at
     OR OLD.estimated_cost IS DISTINCT FROM NEW.estimated_cost
  THEN
    RAISE EXCEPTION 'ops.model_call_ledger identity/reservation columns are immutable after INSERT (§19.1)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;

  IF OLD.status <> 'RESERVED' AND NEW.status IS DISTINCT FROM OLD.status THEN
    RAISE EXCEPTION 'ops.model_call_ledger already finalized — status cannot change again (§19.1)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;

  -- Finding #6: an outcome column may move NULL -> value exactly once (finalize()'s own
  -- write), at any status — but once it holds a value, it can never move to a *different*
  -- value, whether the row is still RESERVED or already finalized. This closes the RESERVED-
  -- state window finding #6 found open while keeping finalize()'s single legitimate write
  -- (and the pre-existing post-finalize immutability) unchanged.
  IF (OLD.input_tokens        IS NOT NULL AND OLD.input_tokens        IS DISTINCT FROM NEW.input_tokens)
  OR (OLD.billable_tokens     IS NOT NULL AND OLD.billable_tokens     IS DISTINCT FROM NEW.billable_tokens)
  OR (OLD.candidate_count     IS NOT NULL AND OLD.candidate_count     IS DISTINCT FROM NEW.candidate_count)
  OR (OLD.candidate_tokens    IS NOT NULL AND OLD.candidate_tokens    IS DISTINCT FROM NEW.candidate_tokens)
  OR (OLD.cache_hit           IS NOT NULL AND OLD.cache_hit           IS DISTINCT FROM NEW.cache_hit)
  OR (OLD.latency_ms          IS NOT NULL AND OLD.latency_ms          IS DISTINCT FROM NEW.latency_ms)
  OR (OLD.actual_cost         IS NOT NULL AND OLD.actual_cost         IS DISTINCT FROM NEW.actual_cost)
  OR (OLD.error_class         IS NOT NULL AND OLD.error_class         IS DISTINCT FROM NEW.error_class)
  OR (OLD.provider_request_id IS NOT NULL AND OLD.provider_request_id IS DISTINCT FROM NEW.provider_request_id)
  THEN
    RAISE EXCEPTION 'ops.model_call_ledger outcome columns can only be set once, from NULL (§19.1)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;

  RETURN NEW;
END;
$$;
