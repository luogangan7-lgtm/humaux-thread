-- §33.10 rule 9 / ADR-0018: two-step confirm fallback for destructive MCP actions.
-- `control.confirm_tokens` holds the server side of a `confirm_token`: never the token
-- itself, only sha256(nonce). One row = one (tenant, user, operation, target, successor)
-- binding — `successor_id` pins the operation's second argument (memory.supersede's
-- `replacement_memory_id`) so the confirmed call executes exactly the action the first call
-- named, never a re-targeted one — single-use (`consumed_at`), verified+consumed by `role_gateway` in the SAME transaction
-- as the mutation it gates (crates/adapters/src/memory_governance_repo.rs). No body, no
-- bearer, no secret bytes; expiry is server policy (HUMAUX_GATEWAY_CONFIRM_TOKEN_TTL_SECONDS).
--
-- First consumer: `memory.supersede` (§36). 0011 already granted role_gateway
-- UPDATE (status, superseded_by) on private.memory_records; the §9 audit column
-- `superseded_at` was left out of that column list, so the one supersede UPDATE could not
-- stamp its own time. Added here as a column-level grant (§6.2.2 row updated in the same
-- change, §48.2 "表集合派生"/"授权逐条相等").
CREATE TABLE control.confirm_tokens (
  confirm_token_id uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id    uuid NOT NULL REFERENCES control.tenants(tenant_id) ON DELETE CASCADE,
  user_id      uuid NOT NULL REFERENCES control.users(user_id) ON DELETE CASCADE
               CHECK (user_id <> '00000000-0000-0000-0000-000000000000'),
  operation    text NOT NULL CHECK (operation ~ '^[a-z][a-z0-9_.]{0,95}$'),
  target_id    uuid NOT NULL,
  -- NULL only for a gated operation with no second argument (none wired yet).
  successor_id uuid CHECK (successor_id <> target_id),
  nonce_sha256 bytea NOT NULL UNIQUE CHECK (octet_length(nonce_sha256) = 32),
  issued_at    timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(issued_at)),
  expires_at   timestamptz NOT NULL CHECK (isfinite(expires_at)),
  consumed_at  timestamptz,
  CHECK (expires_at > issued_at),
  CHECK (consumed_at IS NULL OR consumed_at >= issued_at)
);
ALTER TABLE control.confirm_tokens OWNER TO role_migration_owner;
ALTER TABLE control.confirm_tokens ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.confirm_tokens FORCE ROW LEVEL SECURITY;
CREATE POLICY confirm_tokens_tenant ON control.confirm_tokens
  USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

REVOKE ALL ON control.confirm_tokens FROM PUBLIC, role_gateway, role_private_worker,
  role_consolidation_worker, role_public_worker, role_retrieval_worker, role_batch_issuer,
  role_maintenance, role_admin;
GRANT SELECT, INSERT ON control.confirm_tokens TO role_gateway;
GRANT UPDATE (consumed_at) ON control.confirm_tokens TO role_gateway;
GRANT SELECT ON control.confirm_tokens TO role_maintenance;

-- Single-use is a mechanism, not a discipline (§37.2 wording): the only legal UPDATE flips
-- consumed_at NULL -> non-NULL once, before expiry, and changes nothing else. Owner trigger,
-- ordinary invoker; runtime roles have no DDL (§6.2.1) so it cannot be dropped by them.
CREATE FUNCTION control.check_confirm_token_consume() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog
AS $$
BEGIN
  IF OLD.consumed_at IS NOT NULL
     OR NEW.consumed_at IS NULL
     OR NEW.consumed_at > NEW.expires_at
     OR (to_jsonb(NEW) - 'consumed_at') <> (to_jsonb(OLD) - 'consumed_at') THEN
    RAISE EXCEPTION 'confirm tokens are single-use and immutable' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
ALTER FUNCTION control.check_confirm_token_consume() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.check_confirm_token_consume() FROM PUBLIC;
CREATE TRIGGER confirm_token_single_use
  BEFORE UPDATE ON control.confirm_tokens
  FOR EACH ROW EXECUTE FUNCTION control.check_confirm_token_consume();

COMMENT ON TABLE control.confirm_tokens IS
  '§33.10 rule 9 / ADR-0018: sha256(nonce) of an issued confirm_token bound to (tenant,user,operation,target,successor); single-use, consumed in the gated mutation''s own transaction. Permissions: §6.2.2 only. No token bytes stored.';

-- memory.supersede stamps §9 superseded_at in the same UPDATE as status/superseded_by.
GRANT UPDATE (superseded_at) ON private.memory_records TO role_gateway;
