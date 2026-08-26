-- §60/§15.1 `next_commit_seq` — the "全局审计总序" `commit_seq` §15.1's
-- `INSERT INTO projection.stream_log (...) VALUES (..., $stream_seq, $commit_seq)` and
-- `ops.outbox.commit_seq` both bind against. No migration through 0042 ever created the
-- generator itself (grep across migrations/*.sql for `commit_seq`: every hit is a column
-- definition or a comment, never a `CREATE SEQUENCE`) — remember()'s transaction B (§60)
-- has nothing to call. A bare `CREATE SEQUENCE` is the whole mechanism §15.1 needs:
-- "commit_seq 全库单调" only requires monotonic-and-unique, not gap-free, which is exactly
-- what `nextval()` already guarantees without a table/row to contend on.
CREATE SEQUENCE ops.commit_seq_seq AS bigint;

COMMENT ON SEQUENCE ops.commit_seq_seq IS
  '§15.1/§60: sole generator for commit_seq — "全局审计总序,不参与完整性判定". Every call site
   goes through remember()''s next_commit_seq(tx), never a bare nextval() at a second call
   site (mirrors the §48.0① single-construction-point convention this workspace uses for
   payload_sha256/Authority::new).';

-- Ownership follows the 0011 convention (`ALTER TABLE %I.%I OWNER TO role_migration_owner`
-- for every catalog object existing at 0011 time) — this sequence is created nine
-- migrations later so it needs its own explicit ALTER to land in the same place.
ALTER SEQUENCE ops.commit_seq_seq OWNER TO role_migration_owner;

-- §60.1 authorization table only lists `role_batch_issuer` / "runtime role" against
-- `private.ingest_tickets` / `private.events` / `ops.outbox` / `projection.stream_log`; it
-- is silent on the sequence because the sequence did not exist when that table was
-- written. `next_commit_seq` runs inside remember()'s transaction B (role_gateway, §34.2),
-- so role_gateway is the one runtime role that needs USAGE — no other runtime role calls
-- remember()'s write path, and role_batch_issuer's transaction A never touches commit_seq
-- (§60 "只发票。不碰 events / outbox / stream_log" covers commit_seq by the same reasoning:
-- begin_batch's insert_ingest_tickets never binds a commit_seq column).
GRANT USAGE, SELECT ON SEQUENCE ops.commit_seq_seq TO role_gateway;
