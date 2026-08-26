-- §20.1 Predicate Registry — the sole entry point into §22 EXACT completeness
-- ("这一跳不落地，Completeness 契约就只是一句口号：没有谓词就没有 enumerable_scope，没有
-- scope 就没有真分母"). §50 typed config registry semantics apply (垃圾值 fail-loud);
-- Rust-side fail-loud validation lives in crates/retrieval/src/predicate_registry.rs
-- (`load_registry`), this table only carries the CHECK constraints that make a structurally
-- empty/garbage row impossible to persist in the first place.
--
-- Schema placement deviates from §20.1's literal dotted example "retrieval.predicates":
-- this workspace's canonical schema set is exactly the 7 named in §48 (control / private /
-- staging / public / projection / coord / ops) — there is no `retrieval` schema, and adding
-- an 8th would require extending xtask/src/rls_check.rs's SCHEMAS const (a shared CI file
-- outside this task's own migration-number lane). `control` already hosts every other
-- cross-cutting platform-config table (control.plans/quotas/...), and §6.2.1's domain
-- default already gives every runtime role SELECT there — role_gateway included, which is
-- the role that needs to read this registry synchronously on the request path while
-- building a RetrievalRequest (§55.1). No §6.2.2 grant-matrix row is needed as a result.
--
-- tenant_id is nullable and NOT part of §20.1's field table: predicate definitions are
-- platform config (owner_module accountable, not tenant data) — every row this task seeds
-- is system-wide (tenant_id IS NULL). The column exists only so this table can satisfy the
-- repo's blanket RLS requirement (CLAUDE.md 硬边界 ④) with the same "IS NULL OR" template
-- 0012_rls.sql already uses for coord.locks / ops.consistency_reports, leaving room for a
-- future per-tenant predicate override without a schema change.

CREATE TABLE control.retrieval_predicates (
  predicate_id      text PRIMARY KEY,
  tenant_id         uuid,
  sql_predicate     text        NOT NULL CHECK (sql_predicate <> ''),
  -- §20.1 "该谓词依赖的列（必须全部有索引）" — an empty array can never satisfy that
  -- precondition, so it is not a valid row at all, not just an unindexed one.
  required_columns  text[]      NOT NULL CHECK (cardinality(required_columns) > 0),
  -- §20.1 "计 total 的 FROM + 租户过滤" — the real denominator; empty means the row cannot
  -- ever be judged enumerable (§20.2 rule 3), so it is rejected outright rather than stored
  -- and silently failing every judgement at query time.
  enumerable_scope  text        NOT NULL CHECK (enumerable_scope <> ''),
  -- §20.1 "触发该谓词的中/英表层模式" — §20.3 requires >=1 pattern for a candidate to ever
  -- be reachable via §20.2 rule 2.
  surface_patterns  text[]      NOT NULL CHECK (cardinality(surface_patterns) > 0),
  owner_module      text        NOT NULL CHECK (owner_module <> ''),
  created_at        timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE control.retrieval_predicates IS
  '§20.1 predicate registry: the sole entry point from natural-language surface pattern to a '
  '§22 EXACT-eligible SQL predicate. Fail-loud row-shape enforced by CHECK constraints here; '
  'the Rust-side loader (retrieval::predicate_registry::load_registry) additionally rejects '
  'duplicate predicate_id and blank surface_patterns entries before this data reaches the '
  'planner (§20.2).';

ALTER TABLE control.retrieval_predicates OWNER TO role_migration_owner;
ALTER TABLE control.retrieval_predicates ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.retrieval_predicates FORCE ROW LEVEL SECURITY;

CREATE POLICY retrieval_predicates_tenant_isolation ON control.retrieval_predicates
USING (tenant_id IS NULL OR tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id IS NULL OR tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

-- §20.1's own worked example row, seeded verbatim except one column-name correction:
-- required_columns/enumerable_scope name "workspace_id", but private.memory_records
-- (migrations/0004_private_evidence_memory.sql) names that column
-- `visibility_workspace_id` — §20.1's prose is schema-shorthand, not a literal DDL
-- transcription (same class of deviation the 0074 header note already documents for this
-- table family). The real column name is used below and in migration 0078's index.
INSERT INTO control.retrieval_predicates
  (predicate_id, tenant_id, sql_predicate, required_columns, enumerable_scope, surface_patterns, owner_module)
VALUES (
  'rejected_decisions_v1',
  NULL,
  'memory_type=''REJECTION'' AND superseded_at IS NULL',
  ARRAY['memory_type', 'superseded_at', 'visibility_workspace_id'],
  'private.memory_records WHERE visibility_workspace_id = $1',
  ARRAY['所有被否决', '否掉过哪些', 'all rejected'],
  'retrieval::planner'
);
