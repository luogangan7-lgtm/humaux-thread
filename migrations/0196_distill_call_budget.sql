-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0195). ADR-0058 D-T (card 32 review, guard
-- (d) of main-line ruling E1): a distill provider request — the first one of a claim, a re-ask, and the
-- T6 resend after EXECUTION_UNCERTAIN alike — is admitted only while the tenant's §72.3 provider
-- budget for the distill purpose admits it. Before this migration nothing did: `ops.begin_call`
-- checks lease, generation, slot and deadline, and the ledger reservation is a plain INSERT.
--
-- The budget is a sliding window over the attempt ledger itself (§72.3: "token bucket / sliding
-- window", never one fixed window): at most p_max_calls admitted requests of one tenant whose
-- `begun_at` lies inside the last p_window_seconds. Both numbers are deployment configuration
-- passed by the caller (HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS / _MAX_CALLS, §78.1: no
-- literal here). The window needs no new state: ops.distill_calls already holds one row per
-- admitted request.
--
-- Atomicity: the caller (adapters::jobs::begin_distill_call) runs this function and ops.begin_call
-- in ONE transaction. The tenant's advisory lock taken here is held until that transaction ends, so
-- two concurrent admissions of one tenant cannot both read "one call left" (the rejected
-- count-then-insert race): the second waits for the first's ops.distill_calls row or rollback.

CREATE INDEX distill_calls_tenant_begun_idx ON ops.distill_calls (tenant_id, begun_at);

-- ---------------------------------------------------------------------------------------------
-- admit_distill_budget: the §72.3 tenant/purpose budget check in front of ops.begin_call.
--   Caller: adapters::jobs::begin_distill_call (role_private_worker), first statement of the
--   admission transaction; ops.begin_call is the second.
--   Fence: none of its own — it only reads; the slot/lease/generation fence is begin_call's.
--   Lock: pg_advisory_xact_lock on 'distill-budget:<tenant>' (transaction scope).
--   Owner: role_migration_owner (passes the owner arm of distill_calls_tenant_isolation).
--   Returns true = within budget (begin_call may follow); false = over budget (no request).
-- ---------------------------------------------------------------------------------------------
CREATE FUNCTION ops.admit_distill_budget(
  p_tenant_id uuid, p_window_seconds double precision, p_max_calls integer
) RETURNS boolean
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  IF p_tenant_id IS NULL OR p_window_seconds IS NULL OR p_window_seconds <= 0
     OR p_max_calls IS NULL OR p_max_calls < 1 THEN
    RAISE EXCEPTION 'invalid distill budget' USING ERRCODE = '22023';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('distill-budget:' || p_tenant_id::text, 0));
  RETURN (SELECT count(*) FROM ops.distill_calls c
          WHERE c.tenant_id = p_tenant_id
            AND c.begun_at > clock_timestamp() - make_interval(secs => p_window_seconds))
         < p_max_calls;
END;
$$;

ALTER FUNCTION ops.admit_distill_budget(uuid, double precision, integer) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION ops.admit_distill_budget(uuid, double precision, integer)
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker,
  role_public_worker, role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;
GRANT EXECUTE ON FUNCTION ops.admit_distill_budget(uuid, double precision, integer) TO role_private_worker;

COMMENT ON FUNCTION ops.admit_distill_budget(uuid, double precision, integer) IS
  'ADR-0058 D-T (card 32): §72.3 tenant distill budget — at most p_max_calls admitted requests (ops.distill_calls) in the last p_window_seconds; tenant advisory lock held to the end of the caller''s admission transaction (then ops.begin_call). Owner definer; EXECUTE role_private_worker only.';
