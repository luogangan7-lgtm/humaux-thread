-- §15.3 / §7.4 / card 21 fix pass (reviewer P1): a `projection.stream_checkpoints` row said
-- WHAT the watermark is and WHEN it last moved (`updated_at`, owner trigger), never WHO moved
-- it. Card 21's own acceptance asks for "a checkpoint written by one worker is attributed to
-- it", and the delivery claimed it — but the table had no column that could carry the answer,
-- so the assertion was impossible to write and was silently substituted with an
-- `ops.data_disclosures` row. This adds the missing column instead of retracting the claim.
--
-- Shape follows 0167's `retired_at` / `retired_by` precedent on `projection.stream_log`, and the
-- §48.0④ rule it was measured against: an audit column recording a fact about a write is NOT a
-- second source of truth for a quantity that can be recomputed (which is why `open_gap_count`
-- and `deleted_count` are banned). "Which process last advanced this watermark" is recomputable
-- from nothing at all — there is no other place it is written.
--
-- Nullable with no default on purpose: every row that already exists was advanced before this
-- column existed, and back-filling it with any value would be inventing attribution. NULL reads
-- "never advanced by an attributed writer", which is exactly true of those rows.
--
-- Grant: `role_retrieval_worker` already holds the column-limited UPDATE on the three highwater
-- columns (0011); this extends that same cell by one column. No other role can write it — the
-- gateway/consolidation issuer touches `issued_highwater` only, `role_maintenance` the two
-- read-routing flags. §6.2.2's matrix row and `xtask/src/rls_check.rs`'s MATRIX carry the new
-- column in this same change (§6.2.2/§48.2).

ALTER TABLE projection.stream_checkpoints
  ADD COLUMN projection_processor_id uuid;

COMMENT ON COLUMN projection.stream_checkpoints.projection_processor_id IS
  '§15.4/§7.4: the §7.4 ProcessorId of the worker whose advance_prefix last moved projection_highwater. NULL = advanced before migration 0171, or never advanced. Written only by role_retrieval_worker, in the same statement as the watermark.';

GRANT UPDATE (projection_processor_id) ON projection.stream_checkpoints TO role_retrieval_worker;
