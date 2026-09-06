-- §6.3 / §6.2.2 / §46 forward-fix / ADR-0033 (card 12): the membership lifecycle admin path.
--
-- Until now no runtime or ops role could write control.memberships at all (every non-owner
-- role holds §6.2.1's control-domain default, SELECT only) and 0003 documented in its own DDL
-- that the §6.3 security_epoch bump on membership removal/suspension was deliberately NOT
-- wired there. This migration gives the §6.3 MembershipState machine (domain::identity,
-- ADR-0033) exactly the writes it needs, to exactly one role — role_maintenance, the ops-pool
-- role `xtask member` runs under — and nothing wider:
--   1. control.memberships: INSERT (invite = a new INVITED row) plus column-level
--      UPDATE(state, role, updated_at). The row's identity columns (membership_id, tenant_id,
--      user_id, created_at) stay unwritable; the 0160 role trigger + CHECK and the 0012 FORCE
--      RLS tenant policy keep applying (the adapter sets humaux.tenant_id per transaction).
--      Column-level UPDATE also satisfies SELECT ... FOR UPDATE's privilege requirement, which
--      the adapter uses to serialize the last-OWNER count against concurrent removals.
--   2. control.users.security_epoch: NOT a column grant — role_maintenance could then also
--      *lower* an epoch and revive every credential the bump had invalidated. The only write
--      is the owner SECURITY DEFINER control.bump_user_security_epoch(uuid), which can only
--      increment (0037 api_key_lookup / 0041 audit_event_insert pattern). control.users has no
--      RLS (0003/0036: no tenant_id — a user spans tenants), so the definer needs no policy.
--   3. control.audit_event_insert(...): EXECUTE to role_maintenance so every mutation appends
--      its §77 audit row in the same transaction (the function's FORCE-RLS tenant policy
--      still applies; the adapter's SET LOCAL humaux.tenant_id covers it).
-- No new table, no policy change, no DML. §6.2.2 gains a control.memberships column
-- (role_maintenance: SELECT, INSERT, UPDATE(state, role, updated_at); every other non-owner
-- role keeps SELECT / —), transcribed into rls_check's MATRIX in the same change.

GRANT INSERT ON control.memberships TO role_maintenance;
GRANT UPDATE (state, role, updated_at) ON control.memberships TO role_maintenance;

CREATE FUNCTION control.bump_user_security_epoch(p_user_id uuid)
RETURNS bigint
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, control
AS $$
  UPDATE control.users
     SET security_epoch = security_epoch + 1,
         updated_at = now()
   WHERE user_id = p_user_id
  RETURNING security_epoch;
$$;

ALTER FUNCTION control.bump_user_security_epoch(uuid) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.bump_user_security_epoch(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control.bump_user_security_epoch(uuid) TO role_maintenance;

COMMENT ON FUNCTION control.bump_user_security_epoch(uuid) IS
  '§6.3 the sole security_epoch writer for control.users: increments (never sets) one '
  'user''s epoch and returns the new value. SECURITY DEFINER so role_maintenance (the '
  '§6.3 membership lifecycle admin path, ADR-0033) can invalidate every credential/session '
  'snapshotted under the old epoch without holding UPDATE on the column itself.';

GRANT EXECUTE ON FUNCTION control.audit_event_insert(
  uuid, timestamptz, uuid, text, text, text, text, text, text, text, text, inet, text,
  text[], text, text, jsonb
) TO role_maintenance;
