-- §19 Tenant Full Cost Ledger follow-up (T7.4 remediation, code-review finding #8, minor).
-- 0096's `period date NOT NULL` had no default, no CHECK, and no derivation from
-- `occurred_at` — the sole writer (`record_external_model_cost_event`) takes it verbatim as a
-- caller argument, so "period must be a month-start date so `GROUP BY period` is a plain
-- equality group" (0096's own comment) rested entirely on caller discipline with nothing to
-- catch a mid-month or wrong-month value.
--
-- Cannot edit 0096's column definition in place (already applied, rule ③) — ALTER COLUMN SET
-- DEFAULT plus a new CHECK is the standard additive way to retrofit both a default and a
-- validation rule onto an existing NOT NULL column.
ALTER TABLE ops.tenant_cost_events
  ALTER COLUMN period SET DEFAULT date_trunc('month', now())::date;

-- NOT VALID + separate VALIDATE CONSTRAINT: table already holds rows (0096 is applied, this
-- is not a fresh table) — a validated-on-add CHECK would need ACCESS EXCLUSIVE for a full
-- table scan; NOT VALID takes a much shorter lock to add the constraint (enforced for all new
-- writes immediately) and VALIDATE CONSTRAINT then scans under a lesser lock. Same
-- lock-minimizing shape finding #9 (0094's request_id default) asks for on the write side.
ALTER TABLE ops.tenant_cost_events
  ADD CONSTRAINT tenant_cost_events_period_is_month_start
    CHECK (period = date_trunc('month', period)::date) NOT VALID;
ALTER TABLE ops.tenant_cost_events
  VALIDATE CONSTRAINT tenant_cost_events_period_is_month_start;
