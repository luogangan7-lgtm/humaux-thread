-- §19.2 Tenant Budget / Purpose Budget tiers of the "Global Provider Budget -> Region Budget
-- -> Tenant Budget -> Purpose Budget" hierarchy — corrects course from 0091's header comment,
-- which proposed folding these two tiers into `control.quota_windows` instead.
--
-- Why that plan does not hold: §72 freezes "Quota / RateLimit / Budget 三者使用独立计数器和
-- 独立错误类型" and gives the discriminating worked example
--   RateLimit: PASS / MonthlyQuota: PASS / ProviderBudget: FAIL
-- as three *independently* evaluable outcomes on the same request. `control.quota_windows`
-- (migrations/0003) is system 2 in that split — ENTITLEMENT/QUOTA, BMO-denominated, keyed by
-- `entitlement_key`. Provider token budget is system 3 — ProviderCost — and §72.4 says its
-- real external cost "由 Provider Token/Cost Budget 单独保护": *separately*, not via the same
-- counter/table system 2 already owns. Routing Tenant/Purpose provider-token windows through
-- `quota_windows` would make one row's `reserved`/`consumed` do double duty for two of the
-- three systems §72 exists to keep apart — the same collapse §72's own worked example is
-- written to rule out.
--
-- §76's "sole read path" freeze (the other argument 0091's header leaned on) covers
-- `entitlement_snapshots` (plan-derived weight/burst/quota, §19 SaaS overrides) — it does not
-- extend to `quota_windows`, a different table entirely.
--
-- Fix, staying additive-only (0091 already applied, not edited, per this repo's migration
-- discipline): widen 0091's own table with one nullable `purpose` column rather than create a
-- second, column-for-column-duplicate table. 0091's `region` column already folds Global vs.
-- Region into one table this same way; `purpose` folds Tenant vs. Purpose the identical way:
--   tenant_id NULL,     purpose NULL     -> Global tier row (0091, unchanged)
--   tenant_id NULL,     purpose non-NULL -> not a valid combination (CHECK below)
--   tenant_id NOT NULL, purpose NULL     -> Tenant Budget tier row (this migration)
--   tenant_id NOT NULL, purpose non-NULL -> Purpose Budget tier row (this migration)
-- Existing Global/Region rows are untouched: the new column defaults to NULL, which is their
-- correct value (no purpose scoping applies to platform-wide tiers).
--
-- rustdoc home: `humaux_retrieval_provider::admission` (§19/§19.2), same as 0091 — the
-- adapters-layer config loader (later task) that turns rows here into `AdmissionBudgets`'s
-- `tenant`/`purpose` `TierBudget`s is unaffected by which table backs Global/Region.

ALTER TABLE control.retrieval_provider_admission_limits
  ADD COLUMN purpose text
    CHECK (purpose = ANY (ARRAY['RETRIEVAL_EMBEDDING', 'RETRIEVAL_RERANK']::text[]));

-- A Purpose-tier row without a tenant is not a real tier in the §19 hierarchy (Purpose Budget
-- is scoped *within* a tenant, never platform-wide) — reject it at write time rather than
-- store a row an adapters-layer loader would have to special-case away.
ALTER TABLE control.retrieval_provider_admission_limits
  ADD CONSTRAINT retrieval_provider_admission_limits_purpose_needs_tenant
    CHECK (purpose IS NULL OR tenant_id IS NOT NULL);

COMMENT ON COLUMN control.retrieval_provider_admission_limits.purpose IS
  'NULL with tenant_id NULL = Global tier row (0091). NULL with tenant_id NOT NULL = Tenant '
  'Budget tier row for that tenant, all purposes. Non-NULL = Purpose Budget tier row, scoped '
  'to (tenant_id, purpose). §19 hierarchy: Global -> Region -> Tenant -> Purpose. §72.4 '
  'ProviderCost system — independent of control.quota_windows (§72 ENTITLEMENT/QUOTA system), '
  'see this file''s header comment.';

-- Tenant/Purpose-tier lookup mirrors 0091's own (provider_id, region) index for the
-- Global/Region tiers — same shape, scoped by tenant instead.
CREATE INDEX retrieval_provider_admission_limits_tenant_lookup
  ON control.retrieval_provider_admission_limits (tenant_id, provider_id, purpose)
  WHERE tenant_id IS NOT NULL;
