-- T3.8 (§15.5 read-your-writes overlay) — supersedes 0045's approach within the same task,
-- before anything outside this task depended on it (0045 only just landed; `remember()`
-- (T3.2, `crates/adapters/src/remember.rs::issue_stream_log_row`) does not populate the
-- `evidence_id` column 0045 added — that write path only ever binds the six stream-identity
-- columns plus `stream_seq`/`commit_seq`, verbatim to §60's pseudocode, which this task does
-- not own and must not edit). `ops.outbox` already carries `(tenant_id, commit_seq,
-- evidence_id)` for the exact same row, written in the same transaction
-- (`remember::insert_outbox`) — and `commit_seq` is the one column here that is genuinely
-- global-unique (`ops.commit_seq_seq`, "全局审计总序"), so joining `stream_log` to `outbox`
-- on `(tenant_id, commit_seq)` cannot cross-match two different streams' same-numbered
-- `stream_seq` the way joining on `(tenant_id, stream_seq)` alone could. `role_gateway` only
-- had `a` (INSERT) on `ops.outbox` (0011) — this migration adds the missing `SELECT` so the
-- read path can perform that join; the outbox row itself is not sensitive beyond what
-- `evidence_objects`/`stream_log` already expose to this role.
ALTER TABLE projection.stream_log DROP COLUMN evidence_id;

GRANT SELECT ON ops.outbox TO role_gateway;

COMMENT ON TABLE ops.outbox IS
  '§60.1: canonical name for what §60''s pseudocode/other sections call "outbox_event" — §48.2 '
  'enumerates ops.outbox only, the old name resolves to nothing. role_gateway holds SELECT '
  '(T3.8, §15.5) alongside its existing INSERT — the read-your-writes overlay joins '
  'projection.stream_log to this table on (tenant_id, commit_seq) to recover the evidence_id '
  'a stream_log row does not itself carry.';
