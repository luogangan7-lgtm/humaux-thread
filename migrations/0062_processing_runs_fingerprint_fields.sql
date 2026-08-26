-- T5.1 (§16.1.1 Processing Input Fingerprint / Output Digest): completes
-- `private.processing_runs`'s field list. 0005 already created
-- processing_run_id/source_hash/output_digest/context_snapshot_seq/started_at — this
-- migration adds the remaining §16.1.1 fields (provider_request_id?/output_count) and
-- renames finished_at -> completed_at to match the spec's own literal field name (§1.4 坑4:
-- a column name that drifts from the name the spec measures by is the exact failure mode
-- that section exists to close). Table has zero rows in every environment this has run
-- (fresh skeleton table, no INSERT into private.processing_runs exists anywhere in the
-- codebase yet — grepped), so both the rename and the two ADD COLUMNs are backfill-free.

ALTER TABLE private.processing_runs
  RENAME COLUMN finished_at TO completed_at;

-- provider_request_id: optional per spec's own "provider_request_id?" — populated only when
-- this run made an external provider call (LLM/VLM extraction); a purely local deterministic
-- projection rebuild (e.g. Dense/BM25 re-embed with no LLM step) has none. Nullable, no
-- default: absence is the honest value, not an empty-string placeholder standing in for it.
ALTER TABLE private.processing_runs
  ADD COLUMN provider_request_id text;

-- output_count: how many discrete outputs (e.g. Observations) this run produced. Same
-- nullability shape as the pre-existing output_digest/completed_at columns — both are only
-- known once the run finishes; a run still in flight has none of the three.
ALTER TABLE private.processing_runs
  ADD COLUMN output_count integer;

ALTER TABLE private.processing_runs
  ADD CONSTRAINT processing_runs_output_count_nonnegative
    CHECK (output_count IS NULL OR output_count >= 0);

COMMENT ON COLUMN private.processing_runs.provider_request_id IS
  '§16.1.1: optional external provider call id (LLM/VLM); null for provider-less deterministic runs.';
COMMENT ON COLUMN private.processing_runs.output_count IS
  '§16.1.1: count of outputs this run produced; null until the run completes (mirrors output_digest/completed_at nullability).';
COMMENT ON COLUMN private.processing_runs.completed_at IS
  '§16.1.1 (renamed from finished_at — zero rows anywhere this migration has run).';

-- §16.1.1's own query pattern is "every run for this source_hash" ("同一 fingerprint 可以有
-- 多次 processing run / 不同 output digest；旧输出不被覆盖"), not a single-row lookup by PK —
-- the T5.1 DB test (item 4: independent processing_run_id + output_digest per rerun, no row
-- overwritten) reads this way.
CREATE INDEX processing_runs_source_hash_idx ON private.processing_runs (source_hash);
