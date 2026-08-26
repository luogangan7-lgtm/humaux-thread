-- §19 Retrieval Provider Plane / Provider Route (T7.2): `control.retrieval_provider_routes`.
-- One new `control` table, additive only, no existing table touched — same shape as
-- 0077_retrieval_predicates_registry.sql, which this migration follows column-for-column for
-- its RLS/ownership/grant reasoning.
--
-- Nullable `tenant_id` + RLS design (task brief: "tenant_id 可空但仍要 RLS ... nullable
-- tenant 的行如何不泄漏跨租户必须有明确设计"): a NULL `tenant_id` row is a **platform-wide
-- default route** (owner_module-accountable config, not tenant data) — exactly
-- `control.retrieval_predicates`'s own precedent and stated reasoning (0077's header:
-- "predicate definitions are platform config ... every row this task seeds is system-wide").
-- The tenant-isolation policy below reuses 0077's literal `tenant_id IS NULL OR tenant_id =
-- NULLIF(...)::uuid` template verbatim: a session scoped to tenant A can SELECT its own rows
-- *and* the shared NULL-tenant defaults, never tenant B's rows — no cross-tenant leak, because
-- a NULL row carries no tenant-specific content by construction (it is admin-authored
-- platform config, same content every tenant that falls through to it would see). This is the
-- "策略允许 NULL 表示全局默认" branch from the task brief, not the "拆表" branch: a second
-- table would duplicate the whole column set for zero behavioral difference, since §6.2.1's
-- domain default already makes every runtime role SELECT-only on `control.*` (see below) —
-- there is no separate write-surface to isolate.
--
-- "只读" for the global branch: §6.2.1's domain default grants **no runtime role any
-- INSERT/UPDATE on `control` at all** (every one of the seven non-owner roles maps to
-- DEFAULT_R for the `control` schema column) — so in practice *no* runtime role can write a
-- NULL-tenant (or any) row through its own grant, regardless of what WITH CHECK would permit;
-- only `role_migration_owner` (via a migration file, run as the superuser DSN — see
-- 0077/0083/0084's identical "no new GRANT statement" reasoning) or a future SECURITY DEFINER
-- function can create one. WITH CHECK still carries the same `tenant_id IS NULL OR ...` shape
-- as USING (required for `rls-check`'s "RLS 四项" cmd='ALL' both-clauses match) as defense in
-- depth, not as the sole enforcement of read-only-ness.

CREATE TABLE control.retrieval_provider_routes (
  route_id      uuid PRIMARY KEY DEFAULT uuidv7(),

  -- NULL = platform-wide default route (see header note above).
  tenant_id     uuid REFERENCES control.tenants(tenant_id),
  -- NULL = region-agnostic (matches any region the Router is resolving for).
  region        text,

  -- §19 Provider Route field table names this bare `purpose`, with no closed set spelled
  -- out at that point in the spec. Reused verbatim rather than inventing a third taxonomy:
  -- the wire-form spelling `humaux_domain::egress::OutboundPurpose`/`PrivateDataPurpose`
  -- already established for exactly these two Retrieval Provider Plane call kinds
  -- (`disclosure.rs::purpose_as_db_str`, `control.reasoning_domain_grants.purposes`'s closed
  -- set from migration 0061). `USER_REASONING` is deliberately excluded from this table's
  -- set — §19 Retrieval Provider Plane is scoped to "外部 managed neural retrieval (dense
  -- embedding + rerank)" only and explicitly "不扩展成通用聊天 LLM Gateway"; a route that
  -- resolved USER_REASONING providers would be exactly that expansion.
  purpose       text NOT NULL
                CHECK (purpose = ANY (ARRAY['RETRIEVAL_EMBEDDING', 'RETRIEVAL_RERANK']::text[])),

  embedding_provider_id text,
  embedding_model_id    text,
  rerank_provider_id    text,
  rerank_model_id       text,

  -- Higher priority value wins (Router picks the highest-`priority` matching, enabled,
  -- in-window candidate; ties broken by `route_id` for a total order — see router.rs).
  priority      integer NOT NULL DEFAULT 0,
  enabled       boolean NOT NULL DEFAULT true,

  effective_from timestamptz NOT NULL DEFAULT now(),
  effective_to   timestamptz,

  created_at    timestamptz NOT NULL DEFAULT now(),

  -- §20.1-style fail-loud row shape: a route naming neither an embedding nor a rerank
  -- provider/model pair can never resolve to anything, so it is rejected at write time
  -- rather than stored as a silently-dead row the Router would just skip forever. Each pair
  -- (`embedding_provider_id`, `embedding_model_id`) / (`rerank_provider_id`,
  -- `rerank_model_id`) is required together — a lone provider with no model (or vice versa)
  -- is equally a garbage value.
  CHECK ((embedding_provider_id IS NULL) = (embedding_model_id IS NULL)),
  CHECK ((rerank_provider_id IS NULL) = (rerank_model_id IS NULL)),
  CHECK (
    (purpose = 'RETRIEVAL_EMBEDDING' AND embedding_provider_id IS NOT NULL)
    OR
    (purpose = 'RETRIEVAL_RERANK' AND rerank_provider_id IS NOT NULL)
  ),
  CHECK (effective_to IS NULL OR effective_to > effective_from)
);

COMMENT ON TABLE control.retrieval_provider_routes IS
  '§19 Provider Route: control-plane row a `router::resolve` call filters/ranks against to '
  'produce one Resolved Retrieval Route for a given (tenant, region, purpose). tenant_id '
  'NULL = platform-wide default (see migration header); region NULL = region-agnostic. '
  'Never written by a runtime role (control schema §6.2.1 domain default is SELECT-only) — '
  'seeded/edited via migration or a future admin SECURITY DEFINER path only.';

COMMENT ON COLUMN control.retrieval_provider_routes.priority IS
  'Higher value = more preferred. router::resolve orders candidates by priority DESC, '
  'route_id DESC as the final deterministic tie-break (§19 Router input list has no '
  'explicit tie-break rule; this is the one this migration/router.rs fixes).';

-- Router's hot lookup: purpose is always supplied (NOT NULL), enabled is always filtered on,
-- and priority DESC is the primary candidate ordering — see router.rs's own doc for the
-- exact SQL shape a future adapters-crate reader would issue against this index.
CREATE INDEX retrieval_provider_routes_lookup
  ON control.retrieval_provider_routes (purpose, enabled, priority DESC);

ALTER TABLE control.retrieval_provider_routes OWNER TO role_migration_owner;
ALTER TABLE control.retrieval_provider_routes ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.retrieval_provider_routes FORCE ROW LEVEL SECURITY;

CREATE POLICY retrieval_provider_routes_tenant_isolation ON control.retrieval_provider_routes
USING (tenant_id IS NULL OR tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id IS NULL OR tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

-- §6.2.1 domain-default reasoning, same as 0077/0083/0084: `control` schema's domain default
-- already gives every non-owner runtime role SELECT (and no INSERT/UPDATE/DELETE) — no new
-- GRANT statement needed, and none would be safe to add without also editing the §6.2.2
-- MATRIX (out of this task's file scope).
