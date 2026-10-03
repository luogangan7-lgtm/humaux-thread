-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0201). Card 33 / ADR-0059 D-H; §73.5.
--
-- API-key pepper rotation without re-issuing every key: while the gateway holds the previous pepper
-- (HUMAUX_GATEWAY_CREDENTIAL_PEPPER_PREVIOUS_HEX), a key that verifies only under it is rehashed to
-- the current pepper on its next successful use. The raw key is never stored and the database never
-- holds a pepper, so only the request path can compute the new verifier, and the database cannot
-- check it. role_gateway can read every (api_key_id, key_hash) through control.api_key_lookup, so a
-- compare-and-set alone would hand it a standing cross-tenant verifier overwrite. The write is
-- therefore bounded by a pepper epoch:
--   * control.credential_pepper_state holds one row, epoch; only role_maintenance advances it
--     (control.credential_pepper_epoch_advance, `humaux-maintenance apikey pepper-epoch advance`);
--   * control.api_keys.pepper_epoch records the epoch each verifier was written under;
--   * control.api_key_rehash rewrites a verifier only for a key behind the epoch, and moves it to the
--     current epoch, so each key is rehashed at most once per epoch; every rewrite writes one
--     api_key.pepper_rehash audit row in the key's tenant. With no rotation open every key is at the
--     current epoch and the function changes nothing.
-- Residual (Baseline §45, ADR-0059 L12): while an epoch is open, a holder of the role_gateway DB
-- credential can rewrite each not-yet-rehashed key's verifier once; every such rewrite is audited.

CREATE TABLE control.credential_pepper_state (
  id    boolean PRIMARY KEY DEFAULT true CHECK (id),
  epoch integer NOT NULL DEFAULT 0 CHECK (epoch >= 0)
);
INSERT INTO control.credential_pepper_state DEFAULT VALUES;
ALTER TABLE control.credential_pepper_state OWNER TO role_migration_owner;
-- 0011's default privileges grant the control.* domain default to new tables; this one is owner-only.
REVOKE ALL ON control.credential_pepper_state FROM PUBLIC,
  role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;

COMMENT ON TABLE control.credential_pepper_state IS
  '§73.5 / ADR-0059 D-H: singleton API-key pepper epoch. Owner-only; advanced only through '
  'control.credential_pepper_epoch_advance() (role_maintenance), read through '
  'control.credential_pepper_epoch().';

CREATE FUNCTION control.credential_pepper_epoch()
RETURNS integer
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
  SELECT epoch FROM control.credential_pepper_state WHERE id;
$$;
ALTER FUNCTION control.credential_pepper_epoch() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.credential_pepper_epoch() FROM PUBLIC;

COMMENT ON FUNCTION control.credential_pepper_epoch() IS
  '§73.5 / ADR-0059 D-H: the current API-key pepper epoch. Default of control.api_keys.pepper_epoch; '
  'every api_keys writer is an owner definer, so no non-owner role holds EXECUTE.';

CREATE FUNCTION control.credential_pepper_epoch_advance()
RETURNS integer
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
  UPDATE control.credential_pepper_state SET epoch = epoch + 1 WHERE id RETURNING epoch;
$$;
ALTER FUNCTION control.credential_pepper_epoch_advance() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.credential_pepper_epoch_advance() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control.credential_pepper_epoch_advance() TO role_maintenance;

COMMENT ON FUNCTION control.credential_pepper_epoch_advance() IS
  '§73.5 / ADR-0059 D-H: opens a rehash window (runbook Rotate, pepper phase 3). role_maintenance '
  'only; role_gateway can never move the epoch.';

ALTER TABLE control.api_keys
  ADD COLUMN pepper_epoch integer NOT NULL DEFAULT control.credential_pepper_epoch();

COMMENT ON COLUMN control.api_keys.pepper_epoch IS
  '§73.5 / ADR-0059 D-H: the pepper epoch key_hash was written under; existing keys start at the '
  'initial epoch 0.';

CREATE FUNCTION control.api_key_rehash(p_api_key_id uuid, p_old bytea, p_new bytea)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
  WITH u AS (
    UPDATE control.api_keys
       SET key_hash = p_new,
           pepper_epoch = control.credential_pepper_epoch(),
           updated_at = now()
     WHERE api_key_id = p_api_key_id
       AND key_hash = p_old
       AND status IN ('ACTIVE', 'ROTATING')
       AND pepper_epoch < control.credential_pepper_epoch()
    RETURNING tenant_id, api_key_id
  ), a AS (
    SELECT control.audit_event_insert(
      uuidv7(), now(), u.tenant_id, 'SERVICE_CREDENTIAL', u.api_key_id::text,
      'api_key.pepper_rehash', 'api_key', u.api_key_id::text, 'SUCCESS', '', '', NULL::inet, '',
      '{}'::text[], NULL, NULL, '{}'::jsonb)
    FROM u
  )
  SELECT count(*) = 1 FROM a;
$$;
ALTER FUNCTION control.api_key_rehash(uuid, bytea, bytea) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.api_key_rehash(uuid, bytea, bytea) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control.api_key_rehash(uuid, bytea, bytea) TO role_gateway;

COMMENT ON FUNCTION control.api_key_rehash(uuid, bytea, bytea) IS
  '§73.5 / ADR-0059 D-H: rehash-on-use after a pepper rotation. Compare-and-set on key_hash, '
  'epoch-gated (only a key behind control.credential_pepper_epoch(), at most once per epoch), audited '
  '(api_key.pepper_rehash in the key''s tenant). Never NULL: false when nothing was rewritten.';
