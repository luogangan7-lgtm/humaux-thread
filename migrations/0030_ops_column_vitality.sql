-- §9.1: ops.column_vitality — daily decorative-column probe results.
--
-- One row per (table, column, window_start) daily run of the §9.1 probe:
--   SELECT count(*) FILTER (WHERE col IS DISTINCT FROM <default>) AS live, count(*) AS total
--   FROM <table> WHERE created_at > now() - interval '90 days';
-- `verdict` is the probe's own three-state read of (total, live), not re-derived downstream:
--   total = 0            -> 'no_data' (not scanned yet; neither pass nor fail)
--   total > 0, live = 0  -> 'fail'    (decorative column; §9.1 requires a ruling in
--                                      docs/decisions/columns.md — keep+rationale or DROP COLUMN)
--   total > 0, live > 0  -> 'pass'
-- This table only records probe outcomes; it does not itself gate CI — the CI job reads the
-- latest row per (table, column) and fails the build on any 'fail' without a matching
-- docs/decisions/columns.md entry (§9.1).

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE ops.column_vitality (
  table_name   text        NOT NULL,
  column_name  text        NOT NULL,
  window_start timestamptz NOT NULL,
  total        bigint      NOT NULL,
  live         bigint      NOT NULL,
  checked_at   timestamptz NOT NULL DEFAULT now(),
  verdict      text        NOT NULL
               CHECK (verdict IN ('pass', 'fail', 'no_data')),
  -- §9.1 三态一致性：no_data 恰好对应 total=0；fail 恰好对应 total>0 且 live=0；
  -- 其余（total>0 且 live>0）恰好是 pass —— 判定不能在写入时口径漂移。
  CHECK (
    (verdict = 'no_data' AND total = 0)
    OR (verdict = 'fail' AND total > 0 AND live = 0)
    OR (verdict = 'pass' AND total > 0 AND live > 0)
  ),
  PRIMARY KEY (table_name, column_name, window_start)
);

COMMENT ON TABLE ops.column_vitality IS
  '§9.1 装饰列检测：90 天窗口内 total>0 且 live=0 ⇒ FAIL，须在 docs/decisions/columns.md '
  '登记裁决（保留+理由，或 DROP COLUMN），未登记不得合并；total=0 ⇒ no_data，不判定。';

-- CI's daily read is "which columns are currently failing" — this partial index is that
-- lookup path.
CREATE INDEX idx_column_vitality_fail
  ON ops.column_vitality (table_name, column_name)
  WHERE verdict = 'fail';
