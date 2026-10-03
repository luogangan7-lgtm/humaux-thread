-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0203). Card 33 / ADR-0059 D-H; §73.5.
--
-- 0202's pepper epoch never closed: control.api_key_rehash accepted any key behind the epoch for as
-- long as it stayed behind, and keys stay behind indefinitely (a key never used during the window, a
-- key minted under the new pepper before the advance). A holder of the role_gateway DB credential, who
-- reads every (api_key_id, key_hash) through control.api_key_lookup, could therefore rewrite those
-- verifiers at any later time with no rotation open. The window becomes explicit state:
--   * control.credential_pepper_state.rehash_open, false by default (so the windows 0202 left open on
--     existing databases are closed by this migration);
--   * control.credential_pepper_epoch_advance() moves to the next epoch AND opens the window
--     (runbook Rotate, pepper phase 3);
--   * control.credential_pepper_epoch_close() closes it (phase 4), role_maintenance only;
--   * control.api_key_rehash changes nothing while the window is closed, whatever a key's epoch.
-- Residual (Baseline §45, ADR-0059 L12) is unchanged in kind and now bounded in time: only while the
-- window is open can a role_gateway credential holder rewrite a not-yet-rehashed verifier, once, audited.

ALTER TABLE control.credential_pepper_state
  ADD COLUMN rehash_open boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN control.credential_pepper_state.rehash_open IS
  '§73.5 / ADR-0059 D-H: true only between control.credential_pepper_epoch_advance() and '
  'control.credential_pepper_epoch_close(); control.api_key_rehash is a no-op while false.';

CREATE OR REPLACE FUNCTION control.credential_pepper_epoch_advance()
RETURNS integer
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
  UPDATE control.credential_pepper_state SET epoch = epoch + 1, rehash_open = true WHERE id
  RETURNING epoch;
$$;

COMMENT ON FUNCTION control.credential_pepper_epoch_advance() IS
  '§73.5 / ADR-0059 D-H: moves to the next pepper epoch and opens the rehash window (runbook Rotate, '
  'pepper phase 3). role_maintenance only; role_gateway can never move the epoch or open the window.';

CREATE FUNCTION control.credential_pepper_epoch_close()
RETURNS integer
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
  UPDATE control.credential_pepper_state SET rehash_open = false WHERE id RETURNING epoch;
$$;
ALTER FUNCTION control.credential_pepper_epoch_close() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.credential_pepper_epoch_close() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control.credential_pepper_epoch_close() TO role_maintenance;

COMMENT ON FUNCTION control.credential_pepper_epoch_close() IS
  '§73.5 / ADR-0059 D-H: closes the rehash window (runbook Rotate, pepper phase 4); afterwards no '
  'key''s verifier can be rewritten until the next advance. role_maintenance only.';

CREATE OR REPLACE FUNCTION control.api_key_rehash(p_api_key_id uuid, p_old bytea, p_new bytea)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  v_tenant uuid;
  v_caller_tenant text := current_setting('humaux.tenant_id', true);
BEGIN
  IF NOT (SELECT rehash_open FROM control.credential_pepper_state WHERE id) THEN
    RETURN false;
  END IF;
  UPDATE control.api_keys
     SET key_hash = p_new,
         pepper_epoch = control.credential_pepper_epoch(),
         updated_at = now()
   WHERE api_key_id = p_api_key_id
     AND key_hash = p_old
     AND status IN ('ACTIVE', 'ROTATING')
     AND pepper_epoch < control.credential_pepper_epoch()
  RETURNING tenant_id INTO v_tenant;
  IF NOT FOUND THEN
    RETURN false;
  END IF;
  PERFORM set_config('humaux.tenant_id', v_tenant::text, true);
  PERFORM control.audit_event_insert(
    uuidv7(), now(), v_tenant, 'SERVICE_CREDENTIAL', p_api_key_id::text,
    'api_key.pepper_rehash', 'api_key', p_api_key_id::text, 'SUCCESS', '', '', NULL::inet, '',
    '{}'::text[], NULL, NULL, '{}'::jsonb);
  PERFORM set_config('humaux.tenant_id', coalesce(v_caller_tenant, ''), true);
  RETURN true;
END;
$$;

COMMENT ON FUNCTION control.api_key_rehash(uuid, bytea, bytea) IS
  '§73.5 / ADR-0059 D-H: rehash-on-use after a pepper rotation. Compare-and-set on key_hash, only '
  'while the rehash window is open (control.credential_pepper_state.rehash_open) and only for a key '
  'behind control.credential_pepper_epoch(), at most once per epoch; audited (api_key.pepper_rehash '
  'in the key''s tenant). Never NULL: false when nothing was rewritten.';
