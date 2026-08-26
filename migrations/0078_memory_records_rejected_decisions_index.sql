-- §20.1 / §20.2 rule 3: `rejected_decisions_v1` (seeded in migration 0077) can only ever be
-- judged EXACT once its three `required_columns` — memory_type, superseded_at,
-- visibility_workspace_id (§20.1's own `workspace_id` corrected to the real column name, see
-- 0077's header note) — are indexed. Before this migration only
-- idx_memory_records_tenant (tenant_id) exists on private.memory_records
-- (migrations/0004_private_evidence_memory.sql line 162) — the planner's step-3 index check
-- would fail for every one of those three columns today, forcing CANNOT_ESTABLISH on the
-- one predicate this registry ships with. A single partial index over exactly this
-- predicate's own WHERE-shape both satisfies the "all three required_columns indexed"
-- precondition and is what actually serves `enumerable_scope`'s live query.

CREATE INDEX idx_memory_records_rejected_decisions_v1
  ON private.memory_records (visibility_workspace_id)
  WHERE memory_type = 'REJECTION' AND superseded_at IS NULL;
