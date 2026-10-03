-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0202). Card 33 / ADR-0059 D-H; §73.5, §77.
--
-- 0202's control.api_key_rehash wrote its api_key.pepper_rehash audit row through
-- control.audit_event_insert without the tenant GUC. control.audit_events is FORCE RLS and its one
-- policy checks tenant_id against humaux.tenant_id for every role including the owner, and the
-- gateway calls the door before any tenant is known, so every successful rehash failed with 42501
-- and rolled back. The body is replaced (owner, grants and comment survive CREATE OR REPLACE): the
-- tenant GUC is set to the rewritten key's own tenant only around the audit insert and restored to
-- the caller's value before returning (0117 set_config precedent). Behaviour is otherwise
-- unchanged: compare-and-set on key_hash, epoch-gated, at most once per epoch, never NULL.

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
