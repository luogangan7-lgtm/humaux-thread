-- §12.6: every manual review is a privileged global decision, including demotion and
-- quarantine. Restricting only SUPPORTED would let ordinary tenant users degrade the pool.
CREATE TRIGGER contribution_input_change BEFORE INSERT OR UPDATE OR DELETE
  ON control.public_moderator_grants FOR EACH ROW
  EXECUTE FUNCTION ops.guard_contribution_input_change();

CREATE OR REPLACE FUNCTION public.guard_trust_evaluation()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog AS $$
BEGIN
  PERFORM ops.lock_contribution_inputs();
  IF NOT EXISTS(
    SELECT 1 FROM control.public_moderator_grants g JOIN control.users u USING(user_id)
    JOIN control.memberships m ON m.user_id=u.user_id
    JOIN control.tenants t ON t.tenant_id=m.tenant_id
    WHERE g.user_id=NEW.evaluator_user_id AND g.grant_version=NEW.evaluator_grant_version
      AND g.enabled AND u.state='ACTIVE' AND m.state='ACTIVE' AND t.state='ACTIVE'
      AND m.tenant_id=NULLIF(current_setting('humaux.tenant_id',true),'')::uuid
      AND u.user_id=NULLIF(current_setting('humaux.user_id',true),'')::uuid) THEN
    RAISE EXCEPTION 'manual public review requires active moderator authorization' USING ERRCODE='42501';
  END IF;
  RETURN NEW;
END;
$$;
ALTER FUNCTION public.guard_trust_evaluation() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION public.guard_trust_evaluation() FROM PUBLIC;
