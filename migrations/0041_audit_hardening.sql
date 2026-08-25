-- §46.1 migration — fixer-review follow-up to 0034_audit. 0034 is already applied (checksum
-- recorded in ops.schema_migrations) and per repo policy applied migrations are
-- additive-only, so every fix below lands as new DDL/DML rather than editing 0034 in place —
-- same pattern as 0037_edge_security_fixes' follow-up to 0035.
--
-- Five independent fixes, each scoped to what this task's file group (crates/domain/src/
-- audit.rs, crates/adapters/src/audit_sink.rs, migrations/*) can close without touching a
-- file outside it (docs/architecture/Baseline_2.8.md §6.2.2 and xtask/src/rls_check.rs's
-- MATRIX are both owned by other in-flight work and are explicitly out of scope here):
--
--   1. control.audit_events gets a monotonic audit_seq column so an Audit Batch's
--      seq_start/seq_end range is checkable against something real (previously referenced
--      no column anywhere in the schema).
--   2. ops.audit_batches' UNIQUE(seq_start) is replaced with a range-exclusion constraint so
--      overlapping seq ranges can no longer both insert.
--   3. The eight §77 "AuditEvent 最少" text columns 0034 left NULLable become NOT NULL
--      DEFAULT '' (matching the NOT NULL DEFAULT treatment risk_tags/metadata already got in
--      0034) — the table is still empty, so this is free.
--   4. control.audit_events gets a real write path (currently zero runtime roles can INSERT
--      at all — §77 "全部审计" is unimplementable as shipped) and ops.audit_batches' write
--      path is narrowed from "four unrelated request-path roles get INSERT+UPDATE via
--      §6.2.1 domain default" to a single designated exporter — via a SECURITY DEFINER
--      function (control.audit_event_insert) for the first and a BEFORE INSERT identity
--      trigger for the second, **not** direct table GRANTs. Both techniques leave the
--      tables' own information_schema.table_privileges exactly at §6.2.1 domain default
--      (0037's api_key_lookup/api_key_touch_last_used precedent for the function form,
--      documented there as leaving rls-check's domain-default gate "untouched") — the
--      alternative (direct GRANT + registering both tables in §6.2.2) is the more complete
--      fix the review's fix_hint names, but requires editing the spec doc and xtask/src/
--      rls_check.rs's MATRIX, both outside this task's file group; flagged as a follow-up
--      task rather than done here under someone else's file.
--   5. Both tables get a BEFORE TRUNCATE statement trigger closing the one owner-only path
--      (TRUNCATE, unlike DELETE, does not fire a FOR EACH ROW trigger) the existing
--      BEFORE UPDATE OR DELETE append-only triggers leave open.
--
-- Not in this migration (flagged, not silently dropped):
--   - §6.2.2 registration + xtask/src/rls_check.rs MATRIX extension for the two tables
--     touched by fix 4 (needs the two out-of-scope files above).
--   - Backfilling the same BEFORE TRUNCATE gap onto 0032's control.credit_ledger — a
--     different subsystem's table (referral/credit ledger), not audit, out of this task's
--     file group.
--   - Renumbering the 0034_* migration-id collision (0034_audit / 0034_email_auth_identity /
--     0034_notification_plane) — the other two files belong to other in-flight tasks.

-- =============================================================================
-- Fix 1: monotonic seq column control.audit_events.audit_seq (major, §77 Audit Batch).
-- GENERATED ALWAYS: no writer (including the SECURITY DEFINER function in fix 4) supplies
-- it, so it is a pure, gapless-per-commit ordinal PostgreSQL itself assigns — the property
-- an Audit Batch's seq_start/seq_end range needs to mean anything.
-- =============================================================================

ALTER TABLE control.audit_events
  ADD COLUMN audit_seq bigint GENERATED ALWAYS AS IDENTITY;

ALTER TABLE control.audit_events
  ADD CONSTRAINT audit_events_audit_seq_key UNIQUE (audit_seq);

COMMENT ON COLUMN control.audit_events.audit_seq IS
  '§77 Audit Batch seq_start/seq_end range target — did not exist before this migration '
  '(0034''s ops.audit_batches referenced a sequence that was nowhere in the schema).';

-- =============================================================================
-- Fix 2: ops.audit_batches range-exclusion (major, §77 Audit Batch coverage accounting).
-- UNIQUE(seq_start) alone permits e.g. (1,100) and (50,200) to both insert — two batches
-- both covering seq 50..100. int8range has a native GiST opclass (no btree_gist extension
-- needed) so an overlap of any two [seq_start, seq_end] ranges is rejected at INSERT time.
-- =============================================================================

ALTER TABLE ops.audit_batches DROP CONSTRAINT audit_batches_seq_start_key;

ALTER TABLE ops.audit_batches
  ADD CONSTRAINT audit_batches_seq_range_excl
    EXCLUDE USING gist (int8range(seq_start, seq_end, '[]') WITH &&);

COMMENT ON CONSTRAINT audit_batches_seq_range_excl ON ops.audit_batches IS
  '§77 Audit Batch: no two batches may cover an overlapping seq range (replaces 0034''s '
  'UNIQUE(seq_start), which only caught an identical seq_start, not an overlap).';

-- =============================================================================
-- Fix 3: §77 "AuditEvent 最少" field set — mandatory-at-construction in
-- humaux_domain::audit::AuditEvent, but eight of 0034's ADD COLUMNs left them NULLable.
-- Table is empty (0034's own comment: "尚无写路径产生行"), so NOT NULL DEFAULT '' is free —
-- same treatment 0034 already gave risk_tags/metadata. client_ip (inet) is left NULLable:
-- it has no meaningful empty-string sentinel, and unlike the other eight it is genuinely
-- absent for a background/system-originated action with no request socket at all.
-- =============================================================================

ALTER TABLE control.audit_events
  ALTER COLUMN actor_type      SET DEFAULT '',
  ALTER COLUMN actor_id        SET DEFAULT '',
  ALTER COLUMN resource_type   SET DEFAULT '',
  ALTER COLUMN resource_id     SET DEFAULT '',
  ALTER COLUMN result          SET DEFAULT '',
  ALTER COLUMN request_id      SET DEFAULT '',
  ALTER COLUMN trace_id        SET DEFAULT '',
  ALTER COLUMN user_agent_hash SET DEFAULT '';

ALTER TABLE control.audit_events
  ALTER COLUMN actor_type      SET NOT NULL,
  ALTER COLUMN actor_id        SET NOT NULL,
  ALTER COLUMN resource_type   SET NOT NULL,
  ALTER COLUMN resource_id     SET NOT NULL,
  ALTER COLUMN result          SET NOT NULL,
  ALTER COLUMN request_id      SET NOT NULL,
  ALTER COLUMN trace_id        SET NOT NULL,
  ALTER COLUMN user_agent_hash SET NOT NULL;

-- =============================================================================
-- Fix 4a: control.audit_events write path (major, §77 "全部审计"). SECURITY DEFINER,
-- owned by role_migration_owner, so information_schema.table_privileges for
-- control.audit_events stays at exactly §6.2.1 domain default (R only, all runtime
-- roles) — this is the 0037 api_key_lookup pattern, not a direct GRANT.
--
-- The table's own FORCE ROW LEVEL SECURITY (inherited from 0012/0031, unchanged here)
-- still applies to this function's execution — SECURITY DEFINER changes which role's
-- *privileges* the INSERT runs with, not RLS, and role_migration_owner is not
-- BYPASSRLS. The caller must therefore have already set `humaux.tenant_id` (via the
-- normal per-request GUC path) before calling; a call with the GUC unset or set to a
-- different tenant than p_tenant_id fails the table's own
-- `tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid` policy, same
-- as every other tenant-scoped write in this schema. For an event with no resolvable
-- tenant (e.g. a failed MCP_AUTH_LOGIN — §77 lists failed authentications among the ten
-- MCP events, and a failed auth by definition has no established tenant), the caller
-- passes p_tenant_id = the reserved system tenant seeded below and sets the GUC to that
-- same id first.
-- =============================================================================

CREATE FUNCTION control.audit_event_insert(
  p_event_id           uuid,
  p_ts                 timestamptz,
  p_tenant_id           uuid,
  p_actor_type          text,
  p_actor_id            text,
  p_action              text,
  p_resource_type       text,
  p_resource_id         text,
  p_result              text,
  p_request_id          text,
  p_trace_id            text,
  p_client_ip           inet,
  p_user_agent_hash     text,
  p_risk_tags           text[],
  p_before_fingerprint  text,
  p_after_fingerprint   text,
  p_metadata            jsonb
) RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, control
AS $$
  INSERT INTO control.audit_events (
    audit_event_id, occurred_at, tenant_id, actor_type, actor_id, action,
    resource_type, resource_id, result, request_id, trace_id, client_ip,
    user_agent_hash, risk_tags, before_fingerprint, after_fingerprint, metadata
  ) VALUES (
    p_event_id, p_ts, p_tenant_id, p_actor_type, p_actor_id, p_action,
    p_resource_type, p_resource_id, p_result, p_request_id, p_trace_id, p_client_ip,
    p_user_agent_hash, p_risk_tags, p_before_fingerprint, p_after_fingerprint, p_metadata
  )
  RETURNING audit_event_id;
$$;

ALTER FUNCTION control.audit_event_insert(
  uuid, timestamptz, uuid, text, text, text, text, text, text, text, text, inet, text,
  text[], text, text, jsonb
) OWNER TO role_migration_owner;

REVOKE ALL ON FUNCTION control.audit_event_insert(
  uuid, timestamptz, uuid, text, text, text, text, text, text, text, text, inet, text,
  text[], text, text, jsonb
) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION control.audit_event_insert(
  uuid, timestamptz, uuid, text, text, text, text, text, text, text, text, inet, text,
  text[], text, text, jsonb
) TO role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker;

COMMENT ON FUNCTION control.audit_event_insert(
  uuid, timestamptz, uuid, text, text, text, text, text, text, text, text, inet, text,
  text[], text, text, jsonb
) IS
  '§77 the sole INSERT path for control.audit_events. SECURITY DEFINER so the five runtime '
  'roles (§6.2.1 domain default: R-only on control.*) can write an audit row without a '
  'direct table GRANT; the table''s own FORCE RLS tenant policy still gates every call '
  '(caller must set humaux.tenant_id first, same as any other tenant-scoped write).';

-- =============================================================================
-- Fix 4b: ops.audit_batches single-exporter write path (major, §48.2 "发票权唯一"-style
-- single-role pattern). Two sub-problems, opposite directions:
--
--   - role_gateway/role_private_worker/role_public_worker/role_retrieval_worker hold
--     INSERT+UPDATE on this table via 0011's ALTER DEFAULT PRIVILEGES (§6.2.1 ops.*
--     domain default) — four request-path roles that should not be able to append a batch
--     row at all.
--   - role_maintenance (the ops-pool/repair-job role, the closest existing fit to "periodic
--     export job" among the closed §6.2.0 role set — no new role is introduced; §6.2.0
--     freezes the full set) has *no* INSERT at all: its own domain default is R-only on
--     every schema, ops.* included.
--
-- A direct GRANT/REVOKE fix for either side would diverge information_schema.
-- table_privileges from §6.2.1 domain default for a table not named in §6.2.2, which would
-- flip rls-check's check_domain_default_grants red for the wrong reason (an undocumented
-- special case) rather than the right one; registering the table in §6.2.2 to make that
-- divergence legible is the out-of-scope follow-up noted at the top of this file.
--
-- Until then: role_maintenance's write path is a SECURITY DEFINER function (0037's
-- api_key_lookup precedent — see fix 4a above), so the table's own GRANTs stay untouched;
-- the four over-granted roles' raw path is closed by a BEFORE INSERT trigger keyed on
-- `session_user` — deliberately *not* `current_user`, which inside a SECURITY DEFINER
-- function body reads as the function owner (role_migration_owner) rather than the caller,
-- and would make the trigger either block the legitimate function-mediated insert or (worse)
-- pass anyone who can reach any role_migration_owner-owned SECURITY DEFINER function.
-- `session_user` is the originally-authenticated connection role and is unaffected by
-- SECURITY DEFINER, so it correctly names the real caller in both paths. Same
-- defense-in-depth relationship as the existing append-only UPDATE/DELETE trigger: it backs
-- up the (currently over-broad) GRANT rather than replacing it.
-- =============================================================================

CREATE FUNCTION ops.audit_batches_restrict_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF session_user <> 'role_maintenance' THEN
    RAISE EXCEPTION
      'ops.audit_batches INSERT is restricted to role_maintenance (§48.2 single-exporter '
      'pattern) — got %', session_user
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER audit_batches_restrict_insert
BEFORE INSERT ON ops.audit_batches
FOR EACH ROW EXECUTE FUNCTION ops.audit_batches_restrict_insert();

ALTER FUNCTION ops.audit_batches_restrict_insert() OWNER TO role_migration_owner;

COMMENT ON TRIGGER audit_batches_restrict_insert ON ops.audit_batches IS
  '§48.2 single-exporter pattern: only a connection whose session_user is role_maintenance '
  'may insert a batch row (via ops.audit_batch_insert below), even though §6.2.1 ops.* '
  'domain default nominally grants INSERT to four other roles too (see comment above this '
  'trigger''s CREATE FUNCTION).';

CREATE FUNCTION ops.audit_batch_insert(
  p_seq_start            bigint,
  p_seq_end              bigint,
  p_previous_batch_hash  bytea,
  p_payload_hash         bytea,
  p_exported_object      text
) RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, ops
AS $$
  INSERT INTO ops.audit_batches (
    seq_start, seq_end, previous_batch_hash, payload_hash, exported_object
  ) VALUES (
    p_seq_start, p_seq_end, p_previous_batch_hash, p_payload_hash, p_exported_object
  )
  RETURNING audit_batch_id;
$$;

ALTER FUNCTION ops.audit_batch_insert(bigint, bigint, bytea, bytea, text)
  OWNER TO role_migration_owner;

REVOKE ALL ON FUNCTION ops.audit_batch_insert(bigint, bigint, bytea, bytea, text)
  FROM PUBLIC;

GRANT EXECUTE ON FUNCTION ops.audit_batch_insert(bigint, bigint, bytea, bytea, text)
  TO role_maintenance;

COMMENT ON FUNCTION ops.audit_batch_insert(bigint, bigint, bytea, bytea, text) IS
  '§77 the sole INSERT path for ops.audit_batches, granted only to role_maintenance — '
  'role_migration_owner ownership (SECURITY DEFINER) is what lets the insert itself succeed '
  'despite role_maintenance''s own §6.2.1 domain default being R-only on ops.*; the sibling '
  'audit_batches_restrict_insert trigger still applies (keyed on session_user, which '
  'SECURITY DEFINER does not change), so this function is the single legitimate caller.';

-- =============================================================================
-- Fix 5: BEFORE TRUNCATE guard (minor, §77 Operational/Immutable Audit append-only
-- contract). TRUNCATE does not fire a FOR EACH ROW trigger, so 0034's BEFORE UPDATE OR
-- DELETE triggers leave TRUNCATE as a second owner-only bypass alongside the already-
-- documented ALTER TABLE DISABLE TRIGGER path. Only role_migration_owner holds TRUNCATE
-- (rls-check 全域禁动词 already covers that), so this closes the cheaper of the two, same
-- reject function, FOR EACH STATEMENT (TG_OP already renders 'TRUNCATE' correctly there).
-- =============================================================================

CREATE TRIGGER audit_events_reject_truncate
BEFORE TRUNCATE ON control.audit_events
FOR EACH STATEMENT EXECUTE FUNCTION control.audit_events_reject_mutation();

CREATE TRIGGER audit_batches_reject_truncate
BEFORE TRUNCATE ON ops.audit_batches
FOR EACH STATEMENT EXECUTE FUNCTION ops.audit_batches_reject_mutation();

-- =============================================================================
-- Fix 6 (minor, §77 MCP_AUTH_LOGIN): a failed authentication has no resolvable tenant, but
-- control.audit_events.tenant_id is NOT NULL with an FK to control.tenants. Reserved system
-- tenant row, fixed nil-UUID id (matches humaux_domain::audit::SYSTEM_TENANT_ID) rather than
-- the table's own uuidv7() default, so the id is a stable, callable-by-name sentinel and
-- not a value that has to be looked up. Consequence, accepted as the desired shape (fix_hint
-- names this explicitly): the existing tenant_id = current_setting(...) RLS policy means a
-- real tenant's connection never sees these rows — they are readable only by a connection
-- that has explicitly set humaux.tenant_id to this same sentinel (i.e. an operator/security
-- tooling path, not a regular tenant request).
-- =============================================================================

INSERT INTO control.tenants (tenant_id, name, state)
VALUES (
  '00000000-0000-0000-0000-000000000000'::uuid,
  'SYSTEM_RESERVED (§77 unattributable security audit events, e.g. failed MCP_AUTH_LOGIN)',
  'ACTIVE'
)
ON CONFLICT (tenant_id) DO NOTHING;

COMMENT ON TABLE control.tenants IS
  '§6.3 TenantState lifecycle; tenant_id is the RLS boundary root for every other schema '
  '(§6.1). The nil-UUID row is reserved (§77, 0041_audit_hardening) for security audit '
  'events with no resolvable real tenant — not a customer tenant, excluded from any '
  'customer-facing tenant listing by the application layer, not by a DB constraint.';
