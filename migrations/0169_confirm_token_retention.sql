-- §33.10 rule 9 / ADR-0018 / card 21 (card 1 review P2, folded): `control.confirm_tokens` had
-- no way to shrink. 0148 gave it SELECT+INSERT+UPDATE(consumed_at) for role_gateway and SELECT
-- for role_maintenance — no DELETE to anybody, no index on `expires_at`, and no sweep. Every
-- token ever minted stayed forever: the unconditional-growth shape §53's INV set exists to
-- catch, on a table whose rows are worthless the moment they expire (a token is single-use and
-- time-bound by construction, and `nonce_sha256` is UNIQUE, so the surviving rows are pure
-- index bloat on the one lookup the gate performs per confirmed call).
--
-- Three parts, all forward (§46, first free number after 0168):
--   1. a partial index on `expires_at` for the rows the sweep actually scans;
--   2. a DELETE grant to `role_maintenance` ONLY (the gateway still cannot delete — it mints
--      and consumes; a minter that can also erase its own audit trail is the shape §37.2
--      refuses), and
--   3. `control.sweep_confirm_tokens(retention_interval)`, an owner SECURITY DEFINER sweep with
--      a pinned `search_path`, EXECUTE to `role_maintenance` alone.
--
-- Retention rule, stated once, here: a row is deletable when it can no longer gate anything AND
-- is no longer wanted as audit — i.e. `expires_at < now()` and either it was never consumed
-- (nothing happened; there is no audit event to keep) or it was consumed longer ago than the
-- caller's retention interval. The consumed-recently case is deliberately kept: the §9 audit
-- answer "which confirm token authorized this destructive call" must outlive the call itself.
-- The interval is the CALLER's, not a literal in here (§78.1): retention is deployment policy.

CREATE INDEX confirm_tokens_expires_at_idx
  ON control.confirm_tokens (expires_at)
  WHERE consumed_at IS NULL;

CREATE INDEX confirm_tokens_consumed_at_idx
  ON control.confirm_tokens (consumed_at)
  WHERE consumed_at IS NOT NULL;

GRANT DELETE ON control.confirm_tokens TO role_maintenance;

-- SECURITY DEFINER so the sweep is one audited door with one definition of "deletable", rather
-- than a DELETE predicate every operator re-types. `search_path` pinned to pg_catalog (§6.2.1
-- house rule for definer functions). RLS: `control.confirm_tokens` FORCEs RLS and its policy is
-- `tenant_id = current_setting('humaux.tenant_id')`, which the owner is subject to as well —
-- the sweep is therefore per-tenant by construction, exactly like every other runtime access,
-- and cannot be turned into a cross-tenant erase by calling it without a tenant context (with
-- no setting the policy matches nothing and the sweep deletes nothing).
CREATE FUNCTION control.sweep_confirm_tokens(p_consumed_retention interval)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  v_deleted bigint;
BEGIN
  IF p_consumed_retention IS NULL OR p_consumed_retention < interval '0' THEN
    RAISE EXCEPTION 'sweep_confirm_tokens: retention must be a non-negative interval'
      USING ERRCODE = '22023';
  END IF;
  DELETE FROM control.confirm_tokens
   WHERE expires_at < now()
     AND (consumed_at IS NULL OR consumed_at < now() - p_consumed_retention);
  GET DIAGNOSTICS v_deleted = ROW_COUNT;
  RETURN v_deleted;
END;
$$;
ALTER FUNCTION control.sweep_confirm_tokens(interval) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.sweep_confirm_tokens(interval) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control.sweep_confirm_tokens(interval) TO role_maintenance;

COMMENT ON FUNCTION control.sweep_confirm_tokens(interval) IS
  '§33.10 rule 9 / card 21: deletes expired confirm tokens — unconsumed ones as soon as they expire, consumed ones after the caller''s audit-retention interval. Per-tenant by RLS; EXECUTE to role_maintenance only.';
