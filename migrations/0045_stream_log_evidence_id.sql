-- T3.8 (§15.5 read-your-writes overlay): `projection.stream_log` (0007) carries the
-- six-column stream identity + stream_seq/commit_seq/state, but no column naming *which*
-- Evidence a given ledger row is about. `ops.outbox` does carry `evidence_id` alongside its
-- own `stream_seq`, but `role_gateway` (the recall/context request-path role, §6.2.1) has
-- only `INSERT` on `ops.outbox` (0008/0011 GRANT — no `SELECT`), so the serving-path PG
-- delta overlay this task implements cannot join through it. Adding the FK directly to
-- `stream_log` — the table `role_gateway` already has table-level `SELECT` on (0011) — is
-- the minimal fix; it does not create a second identity source (evidence_id remains
-- `private.evidence_objects.evidence_id`, referenced, not duplicated), it only lets a
-- ledger row say what it is a ledger row *of*.
--
-- Nullable: derived-projection streams (retrieval_card/dense/sparse/graph/code, §16) may
-- track progress on units that are not a single Evidence 1:1; only the ingest/knowledge
-- stream `remember()` writes to is expected to populate this column.
ALTER TABLE projection.stream_log
  ADD COLUMN evidence_id uuid REFERENCES private.evidence_objects(evidence_id);

COMMENT ON COLUMN projection.stream_log.evidence_id IS
  'T3.8/§15.5: which private.evidence_objects row this ledger entry tracks (nullable — only '
  'populated on the ingest stream remember() writes to). Table-level SELECT/INSERT already '
  'granted to role_gateway via 0011 covers this column; no new GRANT needed.';
