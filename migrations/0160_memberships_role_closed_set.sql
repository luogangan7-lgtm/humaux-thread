-- §6.3 / §78.2 / §46 forward-fix / ADR-0032 (card 11): control.memberships.role becomes a closed
-- set the database enforces — OWNER | ADMIN | MEMBER — with one canonical spelling.
--
-- 0003 declared `role text NOT NULL` with no CHECK, and the tree seeds it four ways ('member',
-- 'MEMBER', 'owner', 'OWNER'). That was tolerable while nothing read the column; card 11 makes
-- the first role-based authorization decision on it (`remember.put` may write TENANT_SHARED
-- Evidence only for an ACTIVE OWNER/ADMIN member, operation_receipt::TENANT_SHARED_WRITER_ROLES),
-- and an exact-match gate over a free-text column is a silent deny for every other spelling.
-- Three steps, in order:
--   1. backfill: every existing row's spelling is folded to upper case (the only rows that
--      change are the lower-case seeds; no role is re-assigned);
--   2. canonicalize on write: a BEFORE trigger folds NEW.role to upper case so a writer that
--      still spells 'owner' lands as 'OWNER' — the reader compares canonical values only;
--   3. close the set: CHECK (role IN ('OWNER','ADMIN','MEMBER')) — widening it is a new
--      migration, never an app-side string (same discipline as 0049's status CHECK).
-- The trigger is SECURITY INVOKER and touches only NEW; it fires after
-- `contribution_input_change` (BEFORE triggers run in name order) and before the CHECK.
-- Nothing else on the table changes: PK, UNIQUE (tenant_id, user_id), FKs, RLS policies and the
-- §6.2.2 grants are untouched.

UPDATE control.memberships SET role = upper(role) WHERE role <> upper(role);

CREATE FUNCTION control.memberships_canonical_role()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
  NEW.role := upper(NEW.role);
  RETURN NEW;
END;
$$;

ALTER FUNCTION control.memberships_canonical_role() OWNER TO role_migration_owner;

CREATE TRIGGER memberships_canonical_role
BEFORE INSERT OR UPDATE OF role ON control.memberships
FOR EACH ROW EXECUTE FUNCTION control.memberships_canonical_role();

ALTER TABLE control.memberships
  ADD CONSTRAINT memberships_role_known
    CHECK (role IN ('OWNER', 'ADMIN', 'MEMBER'));
