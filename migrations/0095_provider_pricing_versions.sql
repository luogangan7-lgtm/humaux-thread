-- §19 Pricing Registry (T7.4): "模型价格不能硬编码在 Rust" —
-- `control.provider_pricing_versions` is the runtime-authoritative source
-- `humaux_retrieval_provider::pricing::resolve` reads (via a repo query in
-- `crates/adapters/src/model_call_ledger.rs`), never a Rust constant. Column list verbatim
-- from §19's table.
--
-- No tenant_id: platform-wide pricing, not tenant-scoped (§62/§48.2's RLS four-item
-- enumeration only scans for a literal `tenant_id` column — correctly skips this table, same
-- as every other tenant-free control.* platform table). `control.*` domain default gives every
-- runtime role read-only `R` (§6.2.1) — writing a new price version is an admin/migration-time
-- action, not a request-path one, matching "价格不能硬编码" 的另一半: prices change by a
-- deliberate, audited write (`source_ref`/`verified_at` below), not by application code.
--
-- `effective_to IS NULL` means "still current, open-ended" — §19 "允许价格更新而不重写历史
-- 账单": a price update is always a new row (a new `effective_from`, and the prior row's
-- `effective_to` closed to match), never an UPDATE of an existing row's price columns. This
-- migration does not itself enforce non-overlapping windows per (provider_id, model_id,
-- region) with a DB constraint — ponytail: a plain unique/exclusion constraint here would
-- need the `btree_gist` extension for a range-exclusion, which is real cost for a single-
-- writer admin-only table; add it if a second writer path is ever introduced. The one-shot
-- append discipline is application-level (`resolve()`'s own doc), verified by this task's
-- pricing-snapshot test picking the correct row out of a hand-built overlapping-adjacent set.
CREATE TABLE control.provider_pricing_versions (
  provider_pricing_version_id uuid    PRIMARY KEY DEFAULT uuidv7(),
  provider_id                  text    NOT NULL,
  model_id                     text    NOT NULL,
  region                       text    NOT NULL,

  pricing_version              text    NOT NULL,
  currency                     text    NOT NULL,

  -- §19: currency units per 1,000,000 input tokens (matches the bootstrap snapshot's own
  -- "¥0.5 / 1M input tokens" unit) — humaux_retrieval_provider::cost::compute_cost divides by
  -- 1_000_000.0, this column is never itself a per-token or per-1K price.
  input_token_price            double precision NOT NULL CHECK (input_token_price >= 0),
  output_token_price           double precision CHECK (output_token_price IS NULL OR output_token_price >= 0),
  request_price                double precision CHECK (request_price IS NULL OR request_price >= 0),
  -- Fraction off input_token_price, 0..1 (e.g. 0.5 == the bootstrap snapshot's "Batch ¥0.25"
  -- being half of non-batch "¥0.5").
  batch_discount                double precision CHECK (batch_discount IS NULL OR (batch_discount >= 0 AND batch_discount <= 1)),

  effective_from                timestamptz NOT NULL,
  effective_to                  timestamptz,
  -- §19: "运行时成本唯一真源是 control.provider_pricing_versions + verified_at/source_ref" —
  -- both required, not optional metadata: a price row nobody verified or sourced is exactly
  -- the "硬编码在别处、这张表只是摆设" failure mode this table exists to prevent.
  source_ref                    text    NOT NULL,
  verified_at                   timestamptz NOT NULL,

  CONSTRAINT provider_pricing_versions_window CHECK (
    effective_to IS NULL OR effective_to > effective_from
  )
);

COMMENT ON TABLE control.provider_pricing_versions IS
  '§19 Pricing Registry — the sole runtime source of model pricing. "历史调用永远按当时 '
  'pricing snapshot 归因": a ModelCallLedger row''s cost is computed once (at reserve()/'
  'finalize() time) against whichever row here covers that call''s timestamp, and never '
  'recomputed later against a newer row — this table changing never rewrites a past '
  'ModelCallLedger.estimated_cost/actual_cost.';
COMMENT ON COLUMN control.provider_pricing_versions.pricing_version IS
  'Free-text version tag for this snapshot (e.g. a provider price-sheet revision or the '
  'bootstrap date) — not a foreign key, just an audit label alongside source_ref/verified_at.';

CREATE INDEX idx_provider_pricing_versions_lookup
  ON control.provider_pricing_versions (provider_id, model_id, region, effective_from);

ALTER TABLE control.provider_pricing_versions OWNER TO role_migration_owner;

-- §19 "下列价格只是 2026-08 bootstrap pricing snapshot... 不是 Architecture Constant" — seeded
-- here as DATA (a migration-time INSERT under role_migration_owner), not as a Rust literal
-- (§78.1/§19's own frozen rule: "价格不能硬编码在 Rust"). `source_ref` names this spec section
-- verbatim so a later audit can trace exactly where the bootstrap numbers came from.
INSERT INTO control.provider_pricing_versions
  (provider_id, model_id, region, pricing_version, currency,
   input_token_price, batch_discount, effective_from, source_ref, verified_at)
VALUES
  ('dashscope', 'qwen3-rerank', 'cn-beijing', '2026-08-bootstrap', 'CNY',
   0.5, NULL, '2026-08-01T00:00:00Z', 'Baseline_2.8.md §19 2026-08 官方原价', '2026-08-01T00:00:00Z'),
  ('dashscope', 'qwen3.7-text-embedding', 'cn-beijing', '2026-08-bootstrap', 'CNY',
   0.5, NULL, '2026-08-01T00:00:00Z', 'Baseline_2.8.md §19 2026-08 官方原价', '2026-08-01T00:00:00Z'),
  ('dashscope', 'text-embedding-v4', 'cn-beijing', '2026-08-bootstrap', 'CNY',
   0.5, 0.5, '2026-08-01T00:00:00Z', 'Baseline_2.8.md §19 2026-08 官方原价 (batch ¥0.25 == 0.5 discount off ¥0.5)', '2026-08-01T00:00:00Z');
