-- §62 RLS tenant policy hardening: wrap every `current_setting('humaux.tenant_id', true)`
-- read inside every policy expression in NULLIF(..., '').
--
-- Why (observed on PG 18.6, 2026-08-26 CI probe): after a `SET LOCAL humaux.tenant_id`
-- transaction commits, the custom GUC reverts to an *empty string* placeholder for the
-- rest of the session — not to "unset". `''::uuid` then raises 22P02 instead of the
-- policy predicate evaluating to NULL. Both outcomes are fail-closed, but inconsistent:
-- fresh session -> 0 rows, reused session -> ERROR. NULLIF makes every no-context read
-- deterministically 0 rows. (§62: transactions must SET LOCAL; this covers the ones
-- that forgot.)
--
-- Mechanics: policies are rewritten *textually* from their own deparsed expressions
-- (pg_policies.qual / with_check), so the direct `tenant_id = ...` template and the
-- EXISTS-subquery policies on link tables (private.events / private.artifacts / ...)
-- are both preserved shape-for-shape; only the GUC read changes.
DO $$
DECLARE
  pol record;
  new_qual text;
  new_check text;
  guc_old constant text := $g$current_setting('humaux.tenant_id'::text, true)$g$;
  guc_new constant text := $g$NULLIF(current_setting('humaux.tenant_id'::text, true), '')$g$;
BEGIN
  FOR pol IN
    SELECT schemaname, tablename, policyname, cmd, roles, qual, with_check
    FROM pg_policies
    WHERE (qual LIKE '%humaux.tenant_id%' OR with_check LIKE '%humaux.tenant_id%')
      AND qual NOT LIKE '%NULLIF%'
  LOOP
    new_qual := replace(pol.qual, guc_old, guc_new);
    new_check := replace(pol.with_check, guc_old, guc_new);
    EXECUTE format('DROP POLICY %I ON %I.%I', pol.policyname, pol.schemaname, pol.tablename);
    IF new_check IS NOT NULL THEN
      EXECUTE format(
        'CREATE POLICY %I ON %I.%I USING (%s) WITH CHECK (%s)',
        pol.policyname, pol.schemaname, pol.tablename, new_qual, new_check
      );
    ELSE
      EXECUTE format(
        'CREATE POLICY %I ON %I.%I USING (%s)',
        pol.policyname, pol.schemaname, pol.tablename, new_qual
      );
    END IF;
  END LOOP;
END $$;
