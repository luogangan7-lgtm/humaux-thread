-- §6.2 Database Role 隔离 (T1.2): 8-role closed set (§6.2.0), §6.2.1 domain defaults, and
-- §6.2.2's 14-table/column-level GRANT matrix which *overrides* the domain default on
-- those 14 tables entirely (not additive). Grant-matrix cell values are transcribed to
-- match xtask/src/rls_check.rs's `MATRIX` constant exactly (T1.3, already landed) — that
-- module is the live CI comparator this migration must satisfy, so it is the source of
-- truth for exact verb/column spelling, not a second independent reading of §6.2.2.

-- =============================================================================
-- §6.2.0: role closed set. LOGIN is required — rls-check's "角色全集相等" compares
-- pg_roles(rolcanlogin AND NOT superuser) against this exact 8-name set. Passwords are
-- DEV-ONLY placeholders for this local container (whose own superuser password is the
-- equally-dev-only 'devlocal', per the DSN this environment was handed): a real
-- deployment must ALTER ROLE ... PASSWORD from a secrets manager (§5 OpenBao) outside
-- any migration file, never read one from an env var inside SQL (migrations are
-- declarative and replayable, secrets are not).
-- =============================================================================

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_gateway') THEN
    CREATE ROLE role_gateway LOGIN PASSWORD 'devlocal_role_gateway'
      NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_private_worker') THEN
    CREATE ROLE role_private_worker LOGIN PASSWORD 'devlocal_role_private_worker'
      NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_consolidation_worker') THEN
    CREATE ROLE role_consolidation_worker LOGIN PASSWORD 'devlocal_role_consolidation_worker'
      NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_public_worker') THEN
    CREATE ROLE role_public_worker LOGIN PASSWORD 'devlocal_role_public_worker'
      NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_retrieval_worker') THEN
    CREATE ROLE role_retrieval_worker LOGIN PASSWORD 'devlocal_role_retrieval_worker'
      NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_batch_issuer') THEN
    -- §6.2.0: independent connection pool, kept out of runtime — that separation is a
    -- pooling/deployment property (T1.4's BatchIssuerDbPool), not a DB role attribute;
    -- this role is LOGIN like every other member of the frozen 8-name set.
    CREATE ROLE role_batch_issuer LOGIN PASSWORD 'devlocal_role_batch_issuer'
      NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_maintenance') THEN
    CREATE ROLE role_maintenance LOGIN PASSWORD 'devlocal_role_maintenance'
      NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_migration_owner') THEN
    CREATE ROLE role_migration_owner LOGIN PASSWORD 'devlocal_role_migration_owner'
      NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
END
$$;

-- =============================================================================
-- Ownership: §6.2.1's domain-default table gives role_migration_owner `owner` across
-- all 7 schemas. Reassigning here (not at CREATE TABLE time) keeps every table DDL
-- above free of a role dependency that did not exist yet when those files ran.
-- rls-check's "全域禁动词" only forbids the 7 *other* roles from owning anything and its
-- "发票权唯一"/invoice query excludes exactly 'role_migration_owner' as the implicit-ALL
-- owner — so ownership must land on this specific role, not stay on the migration
-- runner's own connection role.
-- =============================================================================

ALTER SCHEMA control OWNER TO role_migration_owner;
ALTER SCHEMA private OWNER TO role_migration_owner;
ALTER SCHEMA staging OWNER TO role_migration_owner;
ALTER SCHEMA public OWNER TO role_migration_owner;
ALTER SCHEMA projection OWNER TO role_migration_owner;
ALTER SCHEMA coord OWNER TO role_migration_owner;
ALTER SCHEMA ops OWNER TO role_migration_owner;

DO $$
DECLARE
  rec record;
BEGIN
  FOR rec IN
    SELECT schemaname, tablename FROM pg_tables
    WHERE schemaname IN ('control','private','staging','public','projection','coord','ops')
  LOOP
    EXECUTE format('ALTER TABLE %I.%I OWNER TO role_migration_owner', rec.schemaname, rec.tablename);
  END LOOP;

  FOR rec IN
    SELECT schemaname, viewname AS tablename FROM pg_views
    WHERE schemaname IN ('control','private','staging','public','projection','coord','ops')
  LOOP
    EXECUTE format('ALTER VIEW %I.%I OWNER TO role_migration_owner', rec.schemaname, rec.tablename);
  END LOOP;

  FOR rec IN
    SELECT n.nspname AS schemaname, p.proname AS fname, p.oid::regprocedure::text AS sig
    FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname IN ('control','private','staging','public','projection','coord','ops')
  LOOP
    EXECUTE format('ALTER FUNCTION %s OWNER TO role_migration_owner', rec.sig);
  END LOOP;
END
$$;

-- USAGE alone grants no data access, but a blanket grant to every non-owner role on every
-- schema still overstates blast radius beyond what §6.2.1's row per role implies, and
-- USAGE sits outside every rls-check enumeration (table/column GRANTs only) so nothing
-- would ever flag an unjustified one. Grant USAGE only on the schemas each role's §6.2.1
-- row is non-'—' in, plus each role's explicit §6.2.2 override cells that land in a
-- schema its domain-default row is '—' for: role_batch_issuer/private (ingest_tickets)
-- and role_private_worker/projection (stream_log, stream_checkpoints) below.
DO $$
DECLARE
  s text;
BEGIN
  FOREACH s IN ARRAY ARRAY['control','private','public','projection','coord','ops']
  LOOP
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO role_gateway', s);
  END LOOP;
  FOREACH s IN ARRAY ARRAY['control','private','staging','coord','ops']
  LOOP
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO role_private_worker', s);
  END LOOP;
  -- role_private_worker's projection.* domain default is '—', but §6.2.2 gives it an
  -- explicit column-limited override on projection.stream_log / stream_checkpoints (the
  -- state-machine columns it drives, §15.2) — needs schema USAGE for that override alone.
  GRANT USAGE ON SCHEMA projection TO role_private_worker;
  FOREACH s IN ARRAY ARRAY['control','private','coord','ops']
  LOOP
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO role_consolidation_worker', s);
  END LOOP;
  FOREACH s IN ARRAY ARRAY['control','staging','public','coord','ops']
  LOOP
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO role_public_worker', s);
  END LOOP;
  FOREACH s IN ARRAY ARRAY['control','private','public','projection','coord','ops']
  LOOP
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO role_retrieval_worker', s);
  END LOOP;
  -- role_batch_issuer: domain default '—' everywhere (§6.2.1); private schema USAGE only
  -- because of the explicit private.ingest_tickets override in §6.2.2 below.
  GRANT USAGE ON SCHEMA private TO role_batch_issuer;
  FOREACH s IN ARRAY ARRAY['control','private','staging','public','projection','coord','ops']
  LOOP
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO role_maintenance', s);
  END LOOP;
END
$$;

-- =============================================================================
-- §6.2.1 domain defaults. Applied to ALL TABLES IN SCHEMA, then the 14 §6.2.2-named
-- tables are stripped back out immediately below so their grants come *only* from the
-- exact matrix (§6.2.2 "覆盖" the default, not additive) — rls-check's "授权逐条相等"
-- diffs each named table's actual grants against MATRIX with no domain-default residue
-- tolerated.
-- =============================================================================

-- role_gateway: control R · private R+W · public R · projection R · coord R+W · ops R+W
GRANT SELECT ON ALL TABLES IN SCHEMA control TO role_gateway;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA private TO role_gateway;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO role_gateway;
GRANT SELECT ON ALL TABLES IN SCHEMA projection TO role_gateway;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA coord TO role_gateway;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA ops TO role_gateway;

-- role_private_worker: control R · private R+W · staging W(release) · coord R · ops R+W
GRANT SELECT ON ALL TABLES IN SCHEMA control TO role_private_worker;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA private TO role_private_worker;
GRANT INSERT, UPDATE ON ALL TABLES IN SCHEMA staging TO role_private_worker;
GRANT SELECT ON ALL TABLES IN SCHEMA coord TO role_private_worker;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA ops TO role_private_worker;

-- role_consolidation_worker: control R · private R · coord R · ops R
GRANT SELECT ON ALL TABLES IN SCHEMA control TO role_consolidation_worker;
GRANT SELECT ON ALL TABLES IN SCHEMA private TO role_consolidation_worker;
GRANT SELECT ON ALL TABLES IN SCHEMA coord TO role_consolidation_worker;
GRANT SELECT ON ALL TABLES IN SCHEMA ops TO role_consolidation_worker;

-- role_public_worker: control R · staging R(release) · public R+W · coord R · ops R+W
GRANT SELECT ON ALL TABLES IN SCHEMA control TO role_public_worker;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO role_public_worker;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO role_public_worker;
GRANT SELECT ON ALL TABLES IN SCHEMA coord TO role_public_worker;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA ops TO role_public_worker;

-- role_retrieval_worker: control R · private R(RetrievalCard 面) · public R · projection R+W · coord R · ops R+W
GRANT SELECT ON ALL TABLES IN SCHEMA control TO role_retrieval_worker;
GRANT SELECT ON ALL TABLES IN SCHEMA private TO role_retrieval_worker;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO role_retrieval_worker;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA projection TO role_retrieval_worker;
GRANT SELECT ON ALL TABLES IN SCHEMA coord TO role_retrieval_worker;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA ops TO role_retrieval_worker;

-- role_batch_issuer: domain default is '-' everywhere (§6.2.1 "它只在 §6.2.2 里有一行非
-- 空") — nothing granted here; its sole grant is the private.ingest_tickets matrix cell.

-- role_maintenance: R on all 7 domains, no W by default (§6.2.1: "在域表里只有 R，是刻意
-- 的：修复动作必须逐条落到 §6.2.2 才成立").
GRANT SELECT ON ALL TABLES IN SCHEMA control TO role_maintenance;
GRANT SELECT ON ALL TABLES IN SCHEMA private TO role_maintenance;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO role_maintenance;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO role_maintenance;
GRANT SELECT ON ALL TABLES IN SCHEMA projection TO role_maintenance;
GRANT SELECT ON ALL TABLES IN SCHEMA coord TO role_maintenance;
GRANT SELECT ON ALL TABLES IN SCHEMA ops TO role_maintenance;

-- Strip the 14 §6.2.2-named tables back to zero grants for the 7 non-owner roles before
-- laying down the exact matrix below — the sole point of this block is "no domain-
-- default residue survives on a named table" (see block comment above).
DO $$
DECLARE
  r text;
  t text;
BEGIN
  FOREACH r IN ARRAY ARRAY['role_gateway','role_private_worker','role_consolidation_worker',
                            'role_public_worker','role_retrieval_worker','role_batch_issuer',
                            'role_maintenance']
  LOOP
    FOREACH t IN ARRAY ARRAY[
      'private.ingest_tickets','private.events','projection.stream_log',
      'projection.stream_checkpoints','ops.outbox','ops.jobs','control.quota_windows',
      'private.evidence_objects','private.memory_records','private.memory_evidence',
      'private.memory_consolidation_runs','private.memory_consolidation_inputs',
      'private.memory_rollups','private.memory_rollup_sources'
    ]
    LOOP
      EXECUTE format('REVOKE ALL ON %s FROM %I', t, r);
    END LOOP;
  END LOOP;
END
$$;

-- =============================================================================
-- §6.2.2 exact matrix — transcribed to match xtask/src/rls_check.rs::MATRIX cell by
-- cell (verb spelling and column lists identical, since that module is the live
-- comparator).
-- =============================================================================

-- private.ingest_tickets
GRANT SELECT, UPDATE ON private.ingest_tickets TO role_gateway;
GRANT SELECT ON private.ingest_tickets TO role_private_worker;
GRANT INSERT, SELECT ON private.ingest_tickets TO role_batch_issuer;
GRANT SELECT, UPDATE ON private.ingest_tickets TO role_maintenance;

-- private.events
GRANT SELECT, INSERT ON private.events TO role_gateway;
GRANT SELECT ON private.events TO role_private_worker;
GRANT SELECT ON private.events TO role_maintenance;

-- projection.stream_log
GRANT SELECT, INSERT ON projection.stream_log TO role_gateway;
GRANT SELECT ON projection.stream_log TO role_private_worker;
GRANT UPDATE (state, error_class) ON projection.stream_log TO role_private_worker;
GRANT SELECT, UPDATE ON projection.stream_log TO role_retrieval_worker;
GRANT SELECT, UPDATE ON projection.stream_log TO role_maintenance;

-- projection.stream_checkpoints
GRANT SELECT ON projection.stream_checkpoints TO role_gateway;
GRANT INSERT (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version)
  ON projection.stream_checkpoints TO role_gateway;
GRANT UPDATE (issued_highwater) ON projection.stream_checkpoints TO role_gateway;
GRANT SELECT ON projection.stream_checkpoints TO role_private_worker;
GRANT SELECT ON projection.stream_checkpoints TO role_retrieval_worker;
GRANT UPDATE (evidence_highwater, knowledge_highwater, projection_highwater)
  ON projection.stream_checkpoints TO role_retrieval_worker;
GRANT SELECT ON projection.stream_checkpoints TO role_maintenance;
GRANT UPDATE (serving, shadow) ON projection.stream_checkpoints TO role_maintenance;

-- ops.outbox
GRANT INSERT ON ops.outbox TO role_gateway;
GRANT SELECT, UPDATE ON ops.outbox TO role_private_worker;
GRANT SELECT, UPDATE ON ops.outbox TO role_public_worker;
GRANT SELECT, UPDATE ON ops.outbox TO role_retrieval_worker;
GRANT SELECT ON ops.outbox TO role_maintenance;

-- ops.jobs
GRANT SELECT, INSERT, UPDATE ON ops.jobs TO role_gateway;
GRANT SELECT, INSERT, UPDATE ON ops.jobs TO role_private_worker;
GRANT SELECT ON ops.jobs TO role_consolidation_worker;
GRANT UPDATE (status, lease_owner, lease_expires_at) ON ops.jobs TO role_consolidation_worker;
GRANT SELECT, INSERT, UPDATE ON ops.jobs TO role_public_worker;
GRANT SELECT, INSERT, UPDATE ON ops.jobs TO role_retrieval_worker;
GRANT SELECT ON ops.jobs TO role_maintenance;
GRANT UPDATE (status, lease_owner, lease_expires_at) ON ops.jobs TO role_maintenance;

-- control.quota_windows (§6.2.2 explicit vacancy note: no INSERT to any role)
GRANT SELECT ON control.quota_windows TO role_gateway;
GRANT UPDATE (reserved, consumed) ON control.quota_windows TO role_gateway;
GRANT SELECT ON control.quota_windows TO role_private_worker;
GRANT SELECT ON control.quota_windows TO role_maintenance;

-- private.evidence_objects
GRANT SELECT, INSERT ON private.evidence_objects TO role_gateway;
GRANT SELECT ON private.evidence_objects TO role_private_worker;
GRANT SELECT ON private.evidence_objects TO role_consolidation_worker;
GRANT SELECT ON private.evidence_objects TO role_retrieval_worker;
GRANT SELECT ON private.evidence_objects TO role_maintenance;

-- private.memory_records
GRANT SELECT, INSERT ON private.memory_records TO role_gateway;
GRANT UPDATE (status, superseded_by) ON private.memory_records TO role_gateway;
GRANT SELECT, INSERT ON private.memory_records TO role_private_worker;
GRANT SELECT ON private.memory_records TO role_consolidation_worker;
GRANT SELECT ON private.memory_records TO role_retrieval_worker;
GRANT SELECT ON private.memory_records TO role_maintenance;

-- private.memory_evidence
GRANT SELECT, INSERT ON private.memory_evidence TO role_gateway;
GRANT SELECT, INSERT ON private.memory_evidence TO role_private_worker;
GRANT SELECT ON private.memory_evidence TO role_consolidation_worker;
GRANT SELECT ON private.memory_evidence TO role_retrieval_worker;
GRANT SELECT ON private.memory_evidence TO role_maintenance;

-- private.memory_consolidation_runs
GRANT SELECT, INSERT ON private.memory_consolidation_runs TO role_consolidation_worker;
GRANT UPDATE (status, input_snapshot_seq, manifest_hash, output_digest, finished_at, error_class)
  ON private.memory_consolidation_runs TO role_consolidation_worker;
GRANT SELECT ON private.memory_consolidation_runs TO role_maintenance;

-- private.memory_consolidation_inputs
GRANT SELECT, INSERT ON private.memory_consolidation_inputs TO role_consolidation_worker;
GRANT SELECT ON private.memory_consolidation_inputs TO role_maintenance;

-- private.memory_rollups
GRANT SELECT ON private.memory_rollups TO role_gateway;
GRANT SELECT, INSERT ON private.memory_rollups TO role_consolidation_worker;
GRANT SELECT ON private.memory_rollups TO role_retrieval_worker;
GRANT SELECT ON private.memory_rollups TO role_maintenance;

-- private.memory_rollup_sources
GRANT SELECT ON private.memory_rollup_sources TO role_gateway;
GRANT SELECT, INSERT ON private.memory_rollup_sources TO role_consolidation_worker;
GRANT SELECT ON private.memory_rollup_sources TO role_retrieval_worker;
GRANT SELECT ON private.memory_rollup_sources TO role_maintenance;

-- =============================================================================
-- §6.2.1 domain defaults for tables THIS migration cannot yet see: any table created by a
-- migration numbered above 0011. migrate.rs (§46 "按序应用") applies every migrations/*.sql
-- file over ONE PostgreSQL session/connection in filename order and never issues SET ROLE,
-- so `current_user` captured below IS the exact role every later-numbered migration's
-- CREATE TABLE will run as — not role_migration_owner, which nothing actually connects or
-- creates-as at migration time (the ownership loop above only reassigns retroactively,
-- table by table, for objects that already exist when 0011 runs). Without this, a table
-- created above 0011 is owned by the connecting role with ZERO grants to any of the 8
-- roles until a human remembers to re-run/extend the grant loops by hand — reproduced:
-- ops.column_vitality (0030) ends up owned by `postgres`, no grantee row but `postgres`.
-- xtask/src/rls_check.rs::check_domain_default_grants is the CI-side half of this (already
-- landed, out of this file's scope) — it enumerates every BASE TABLE §6.2.2 doesn't name
-- and compares its grants against the same §6.2.1 domain-default table this block encodes.
-- =============================================================================

DO $$
DECLARE
  runner text := current_user;
  s text;
BEGIN
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA control GRANT SELECT ON TABLES TO role_gateway', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA private GRANT SELECT, INSERT, UPDATE ON TABLES TO role_gateway', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public GRANT SELECT ON TABLES TO role_gateway', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA projection GRANT SELECT ON TABLES TO role_gateway', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA coord GRANT SELECT, INSERT, UPDATE ON TABLES TO role_gateway', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA ops GRANT SELECT, INSERT, UPDATE ON TABLES TO role_gateway', runner);

  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA control GRANT SELECT ON TABLES TO role_private_worker', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA private GRANT SELECT, INSERT, UPDATE ON TABLES TO role_private_worker', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA staging GRANT INSERT, UPDATE ON TABLES TO role_private_worker', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA coord GRANT SELECT ON TABLES TO role_private_worker', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA ops GRANT SELECT, INSERT, UPDATE ON TABLES TO role_private_worker', runner);

  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA control GRANT SELECT ON TABLES TO role_consolidation_worker', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA private GRANT SELECT ON TABLES TO role_consolidation_worker', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA coord GRANT SELECT ON TABLES TO role_consolidation_worker', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA ops GRANT SELECT ON TABLES TO role_consolidation_worker', runner);

  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA control GRANT SELECT ON TABLES TO role_public_worker', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA staging GRANT SELECT ON TABLES TO role_public_worker', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public GRANT SELECT, INSERT, UPDATE ON TABLES TO role_public_worker', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA coord GRANT SELECT ON TABLES TO role_public_worker', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA ops GRANT SELECT, INSERT, UPDATE ON TABLES TO role_public_worker', runner);

  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA control GRANT SELECT ON TABLES TO role_retrieval_worker', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA private GRANT SELECT ON TABLES TO role_retrieval_worker', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public GRANT SELECT ON TABLES TO role_retrieval_worker', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA projection GRANT SELECT, INSERT, UPDATE ON TABLES TO role_retrieval_worker', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA coord GRANT SELECT ON TABLES TO role_retrieval_worker', runner);
  EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA ops GRANT SELECT, INSERT, UPDATE ON TABLES TO role_retrieval_worker', runner);

  -- role_batch_issuer: domain default '—' everywhere (§6.2.1) — no default-privilege rows.

  FOREACH s IN ARRAY ARRAY['control','private','staging','public','projection','coord','ops']
  LOOP
    EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT SELECT ON TABLES TO role_maintenance', runner, s);
  END LOOP;
END
$$;

-- =============================================================================
-- §6.2.2 note: "role_migration_owner 拥有的 stream_log state 迁移 BEFORE UPDATE 触发器
-- 与 stream_checkpoints updated_at 触发器" — the latter was created in 0007 (immediately
-- reassigned to role_migration_owner by the ownership loop above); the former lands
-- here, now that the owner role exists.  §15.2's Private processing transition list and
-- §6.2.2's two named repair transitions are the frozen legal-transition set per writer
-- role; a transition outside the caller's set is rejected outright, not silently
-- clamped (§50 fail-loud).
-- =============================================================================

CREATE FUNCTION projection.stream_log_guard_state_transition() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  actor text := current_user;
  legal boolean := false;
BEGIN
  IF OLD.state = NEW.state THEN
    RETURN NEW;
  END IF;

  IF actor = 'role_private_worker' THEN
    -- §15.2 "Private processing transition", verbatim: "PROCESSING -> WAITING_KEY |
    -- RETRY_WAIT | FAILED | ISSUED(next reclaim) | next stage" — PROCESSING -> ISSUED is
    -- the next-reclaim edge, not a typo of WAITING_KEY/RETRY_WAIT -> ISSUED below.
    legal := (OLD.state, NEW.state) IN (
      ('ISSUED','PROCESSING'), ('PROCESSING','WAITING_KEY'), ('PROCESSING','RETRY_WAIT'),
      ('PROCESSING','FAILED'), ('PROCESSING','ISSUED'), ('WAITING_KEY','ISSUED'), ('RETRY_WAIT','ISSUED')
    );
  ELSIF actor = 'role_retrieval_worker' THEN
    -- §6.2.2 line, verbatim: "投影侧终态（ISSUED -> DONE | SKIPPED_BY_POLICY | FAILED）在
    -- role_retrieval_worker" — the parenthesised pair IS the full source-state set (ISSUED
    -- only), not a floor; PROCESSING/WAITING_KEY/RETRY_WAIT never settle straight to a
    -- terminal state through this role.
    legal := OLD.state = 'ISSUED' AND NEW.state IN ('DONE','SKIPPED_BY_POLICY','FAILED');
  ELSIF actor = 'role_maintenance' THEN
    -- §15.2 sweep (ISSUED -> LOST) + §37.2 retention::tombstone (* -> TOMBSTONED).
    legal := (OLD.state = 'ISSUED' AND NEW.state = 'LOST') OR NEW.state = 'TOMBSTONED';
  END IF;

  IF NOT legal THEN
    RAISE EXCEPTION 'illegal projection.stream_log state transition % -> % by role % (§6.2.2/§15.2)',
      OLD.state, NEW.state, actor
      USING ERRCODE = 'check_violation';
  END IF;

  -- 0007's CHECK requires settled_at IS NOT NULL exactly on the four TERMINAL states
  -- (§15.2). No role's §6.2.2 grant includes settled_at (role_private_worker's is
  -- column-limited to state,error_class — see §6.2.2 table) precisely because this owner
  -- trigger, not the caller's UPDATE, is the single writer of it: without this, the one
  -- PROCESSING -> FAILED edge role_private_worker is legally allowed to make above is
  -- un-satisfiable at the CHECK and the branch is dead in practice. COALESCE leaves an
  -- explicitly-supplied value alone (no other role currently supplies one).
  IF NEW.state IN ('DONE','SKIPPED_BY_POLICY','FAILED','TOMBSTONED') THEN
    NEW.settled_at := COALESCE(NEW.settled_at, now());
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER stream_log_guard_state_transition
BEFORE UPDATE ON projection.stream_log
FOR EACH ROW EXECUTE FUNCTION projection.stream_log_guard_state_transition();

ALTER FUNCTION projection.stream_log_guard_state_transition() OWNER TO role_migration_owner;
