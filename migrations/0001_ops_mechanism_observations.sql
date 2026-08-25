-- §1.14 / §1.14.1: Runtime MechanismObservation authority.
--
-- MechanismSpec (ch/mechanism/activation_kind/min_denominator/probe/bootstrap_*/note)
-- lives ONLY in the canonical md `mechanism-registry` fence (docs/architecture/Baseline_2.8.md
-- §1.14) — that block is the sole static-side copy (frozen). This table is the OTHER half:
-- the per-deployment/cell runtime truth. §1.14.1 freezes the field list to exactly the 9
-- columns below; do not add `ch`/`mechanism` text columns here — that would duplicate the
-- static registry into the DB and reproduce the exact drift §1.14 exists to kill.
-- `mechanism_id` is the join key back to a canonical registry row; the registry itself is
-- not a DB table (§1.14.1: "MechanismSpec authority = canonical static registry (repo)").
--
-- derived_status is computed by (MechanismSpec, latest fresh Observation, e2e evidence) —
-- never written by hand, never read back into Git (§1.14 "不许把 Runtime Observation
-- 回灌进 Git Spec").

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE ops.mechanism_observations (
  observation_id  uuid        PRIMARY KEY DEFAULT uuidv7(),
  deployment_id   uuid        NOT NULL,
  cell_id         uuid        NOT NULL,
  mechanism_id    text        NOT NULL,
  value           bigint      NOT NULL,
  -- §1.14 坑5: value==0 && scanned_n==0 是「没扫到」不是「没有」；两者必须分得开，
  -- 探针未接线时 scanned_n 留空（NULL），不得压成 0（§4.4：禁止 scanned_n=0 冒充过期）。
  scanned_n       bigint,
  measured_at     timestamptz NOT NULL,
  -- §1.14.1: ACTIVE | NOT_APPLICABLE_YET | STALE，闭集三值；contract test 与
  -- Rust 端 derived_status 枚举对账（repo CLAUDE.md 硬边界：DB enum 与 Rust enum 走 contract test）。
  derived_status  text        NOT NULL
                  CHECK (derived_status IN ('ACTIVE', 'NOT_APPLICABLE_YET', 'STALE')),
  probe_version   text        NOT NULL,
  binary_build    text        NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE ops.mechanism_observations IS
  '§1.14.1 Runtime MechanismObservation authority — append-only per (deployment, cell, mechanism) '
  'time series; latest row per key is the live truth G3/G4/G5 read. Never joined against or '
  'backfilled from the canonical md bootstrap_value/bootstrap_measured_at columns.';

-- G3/G4/G5 all resolve "the latest fresh observation for (deployment, cell, mechanism)" —
-- this index is that lookup path, ordered so the newest row sorts first.
CREATE INDEX idx_mechanism_observations_latest
  ON ops.mechanism_observations (deployment_id, cell_id, mechanism_id, measured_at DESC);
