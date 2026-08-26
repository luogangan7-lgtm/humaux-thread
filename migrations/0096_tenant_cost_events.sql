-- §19 Tenant Full Cost Ledger (T7.4): "ModelCallLedger 只解释模型账单" — real SaaS unit
-- economics also needs storage byte-hours / artifact GB-month / vector points / DB rows /
-- network egress / parser pages / worker compute units / email sends / external model tokens.
-- `ops.tenant_cost_events` is the one normalized event stream all of those feed, so "一个
-- tenant 的毛成本由什么构成" is a single `GROUP BY cost_type` query, not N separate tables.
-- Column list verbatim from §19's table.
--
-- `cost_type` intentionally has NO closed CHECK yet: §19's own list of nine cost kinds
-- (storage_byte_hours / artifact_gb_month / vector_points / db_rows / network_egress /
-- parser_pages / worker_compute_units / email_sends / external_model_tokens) is prose, not a
-- table this migration is asked to freeze into a DB enum — the one writer this task actually
-- wires (`humaux_adapters::model_call_ledger::finalize_call`, cost_type
-- 'external_model_tokens') is covered by a Rust-side wire-string test instead
-- (`crates/adapters/src/model_call_ledger.rs`'s own `#[cfg(test)]`), same split
-- `ops.model_call_ledger.purpose`'s pre-0081 state used before a real closed set existed.
-- Widen with a CHECK once a second writer needs its own cost_type value verified against a
-- known set.
CREATE TABLE ops.tenant_cost_events (
  tenant_cost_event_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id              uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  cost_type               text        NOT NULL,
  quantity                 double precision NOT NULL CHECK (quantity >= 0),
  unit                     text        NOT NULL,
  estimated_unit_cost      double precision CHECK (estimated_unit_cost IS NULL OR estimated_unit_cost >= 0),
  estimated_cost           double precision NOT NULL CHECK (estimated_cost >= 0),
  source                   text        NOT NULL,
  -- §19 "period": the billing/reporting window this event rolls up into, not a per-event
  -- instant — `date_trunc('month', occurred_at)` shape, stored as the period's start so
  -- `GROUP BY period` is a plain equality group, not a range re-derivation per query.
  period                   date        NOT NULL,
  occurred_at              timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE ops.tenant_cost_events IS
  '§19 Tenant Full Cost Ledger: the normalized cost-event stream underneath "一个 tenant 的毛'
  '成本由什么构成 / Free 50 MCP 是否真正亏损 / 哪个功能的边际成本最高" — no first-version '
  'precision-accounting requirement (§19 own words), just enough structure to answer those '
  'three questions with a GROUP BY.';
COMMENT ON COLUMN ops.tenant_cost_events.source IS
  'Free-text provenance of this event — e.g. the ModelCallLedger request_id for an '
  'external_model_tokens row (humaux_adapters::model_call_ledger::finalize_call), or a batch '
  'job name for a periodic storage/egress rollup. Not a foreign key: sources are '
  'heterogeneous by design (§19''s nine cost kinds have no single shared parent table).';

CREATE INDEX idx_tenant_cost_events_tenant_period ON ops.tenant_cost_events (tenant_id, period);

ALTER TABLE ops.tenant_cost_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.tenant_cost_events FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_cost_events_tenant_isolation ON ops.tenant_cost_events
USING (
  tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
)
WITH CHECK (
  tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
);

-- Not one of §6.2.2's named tables ⇒ §6.2.1 domain default already covers every writer this
-- task wires (`ops.* = R + W` for role_gateway/role_private_worker/role_public_worker/
-- role_retrieval_worker) — no new GRANT statement, same reasoning as 0083/0084/0094.
ALTER TABLE ops.tenant_cost_events OWNER TO role_migration_owner;
