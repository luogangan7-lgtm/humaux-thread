-- T3.6/T3.7 (§32.1 G32-1/G80-38): ops.scheduler_leases (0008) already has exactly the shape
-- §32.1 needs — PK(schedule_id, planned_at) + UNIQUE(idempotency_key) — no ALTER required
-- there. ops.jobs (0008) carries idempotency_key but only a composite
-- UNIQUE(tenant_id, idempotency_key); §32.1's "数据库只允许一条逻辑 job" final-arbiter
-- requirement needs `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING` to name an index
-- on idempotency_key alone (Postgres' ON CONFLICT target must match an existing unique index
-- verbatim — the composite index does not satisfy a single-column conflict target).
--
-- Ownership note (task brief): J3 (begin_batch/remember idempotency work) may also want a
-- plain UNIQUE(idempotency_key) on ops.jobs for its own callers. `IF NOT EXISTS` makes
-- whichever of {this migration, J3's} lands second a no-op rather than a duplicate-object
-- error; do not DROP this index without confirming both T3.6/T3.7's scheduler enqueue path
-- and any J3 consumer have stopped relying on it.
CREATE UNIQUE INDEX IF NOT EXISTS ops_jobs_idempotency_key_key
  ON ops.jobs (idempotency_key);

COMMENT ON INDEX ops.ops_jobs_idempotency_key_key IS
  '§32.1 G32-1/G80-38: scheduler exactly-once enqueue final arbiter — adapters::scheduler''s '
  'INSERT ... ON CONFLICT (idempotency_key) DO NOTHING targets this index, not the '
  'composite UNIQUE(tenant_id, idempotency_key) from 0008. Owned jointly with J3 (see file '
  'header); landed by T3.6/T3.7.';
