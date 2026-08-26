-- §19 Provider Admission Controller / §19.2 Provider Budget: the two hierarchy tiers that are
-- *not* tenant-scoped — Global Provider Budget and Region Budget (§19 "Global Provider Budget
-- -> Region Budget -> Tenant Budget -> Purpose Budget"). The Tenant/Purpose tiers and the
-- "monthly plan quota" reuse existing generic tables instead of a new one here:
--   - Tenant/Purpose token windows: `control.quota_windows` (§72, migrations/0003), keyed by
--     `entitlement_key` values this task's adapters layer defines (e.g.
--     'provider_tokens:tenant', 'provider_tokens:purpose:embedding') — already tenant-scoped,
--     already has the reserved/consumed/hard_limit shape §19.2 needs, no schema change.
--   - "weight / queue priority / maximum burst" (§19 SaaS Tenant Fairness) and "monthly plan
--     quota": `control.entitlement_grants` -> `control.entitlement_snapshots.effective`
--     (§76, migrations/0033) — §76 freezes this as the *sole* runtime read path for anything
--     plan-derived; a second parallel table here would violate that frozen rule, not honor
--     the "ALTER 不重建" instruction.
-- What neither existing table can hold: a budget ceiling with **no** tenant_id at all (Global
-- scope) or scoped only by region (Region scope) — `control.quota_windows`'s PK requires a
-- non-null `tenant_id` FK. Hence one new table, additive only.
--
-- rustdoc home: `humaux_retrieval_provider::admission` (§19/§19.2) — this table is the
-- config source an adapters-layer loader (later task) folds into that module's in-memory
-- `AdmissionBudgets.global`/`.region` `TierBudget`s; the admission decision itself stays
-- pure/in-memory (no per-request DB round trip), same "Domain 纯内存，adapters 做真实 DB"
-- split `crates/application/src/scheduler.rs` already established for §32.0.

CREATE TABLE control.retrieval_provider_admission_limits (
  limit_id        uuid        PRIMARY KEY DEFAULT uuidv7(),
  -- Nullable and otherwise unused by this table's real semantics — exists only so this
  -- platform-wide config table can satisfy the repo's blanket RLS requirement (CLAUDE.md
  -- 硬边界 ④) via the same "IS NULL OR" template `control.retrieval_predicates`
  -- (migrations/0077) already uses for the identical shape (system-wide config, not tenant
  -- data). Every row this task's adapters layer writes has tenant_id IS NULL.
  tenant_id       uuid,
  provider_id     text        NOT NULL CHECK (provider_id <> ''),
  -- NULL = Global Provider Budget tier; non-null = Region Budget tier for that region
  -- (§19 "Global Provider Budget -> Region Budget -> ..."). Two separate tiers modeled by
  -- one nullable column rather than two tables, mirroring how the Tenant/Purpose tiers below
  -- already fold into one hierarchy in `humaux_retrieval_provider::admission::AdmissionBudgets`.
  region          text,
  -- §19.2 "provider RPM/TPM limiter" — the two rate ceilings this tier enforces. §78.1: no
  -- default baked in here or in Rust; every row is an explicit, ops-authored value.
  tpm_limit       bigint      NOT NULL CHECK (tpm_limit > 0),
  rpm_limit       bigint      NOT NULL CHECK (rpm_limit > 0),
  effective_from  timestamptz NOT NULL DEFAULT now(),
  effective_to    timestamptz,
  created_at      timestamptz NOT NULL DEFAULT now(),
  CHECK (effective_to IS NULL OR effective_to > effective_from)
);

COMMENT ON TABLE control.retrieval_provider_admission_limits IS
  '§19.2 Global Provider Budget / Region Budget tiers (TPM/RPM ceilings). tenant_id is always '
  'NULL in practice (platform config, not tenant data) — see column comment. Tenant/Purpose '
  'tiers and monthly plan quota live in control.quota_windows / control.entitlement_snapshots '
  'instead, per this migration''s header comment.';

COMMENT ON COLUMN control.retrieval_provider_admission_limits.region IS
  'NULL = Global Provider Budget row; non-NULL = Region Budget row scoped to that region '
  '(§19 hierarchy: Global -> Region -> Tenant -> Purpose).';

CREATE INDEX retrieval_provider_admission_limits_lookup
  ON control.retrieval_provider_admission_limits (provider_id, region);

ALTER TABLE control.retrieval_provider_admission_limits OWNER TO role_migration_owner;
ALTER TABLE control.retrieval_provider_admission_limits ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.retrieval_provider_admission_limits FORCE ROW LEVEL SECURITY;

-- §62/§48.2 tenant policy template, "IS NULL OR" form (migrations/0077's identical-shape
-- precedent) — every row here is system-wide (tenant_id IS NULL), so every tenant session
-- reads it, and no session can ever forge a tenant-scoped row via WITH CHECK because none of
-- this task's write paths set tenant_id at all.
CREATE POLICY retrieval_provider_admission_limits_tenant_isolation
  ON control.retrieval_provider_admission_limits
  USING (tenant_id IS NULL OR tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id IS NULL OR tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

-- No explicit GRANT: `control` schema's §6.2.1 domain default (SELECT for every non-owner
-- runtime role, laid down once by 0011's `ALTER DEFAULT PRIVILEGES`) already applies to any
-- table role_migration_owner creates afterward — same reasoning migrations/0033's header
-- comment gives for control.entitlement_grants/entitlement_snapshots. No role gets INSERT/
-- UPDATE here: these rows are ops-authored config, written outside the runtime-role path
-- (same "explicit vacancy, not oversight" shape control.quota_windows already documents,
-- migrations/0011 line ~285) — a later task that builds an admin-config write path returns
-- here to add the specific GRANT, same as that table's own precedent.
