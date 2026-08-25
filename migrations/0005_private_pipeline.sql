-- §48 private pipeline support tables: processing_runs (§48.0② context_snapshot_seq),
-- observations (§8.4), and the four Consolidation/Dream derived tables (§11.6/§11.7),
-- four of which are named in the §6.2.2 GRANT matrix (T1.2 needs them to exist).

-- §48.0②: "回放三元组的第三元" — distillation is not a pure function of Evidence alone;
-- context_snapshot_seq anchors which context snapshot a run used. Built as NOT NULL from
-- the start (this is a fresh table, not the ALTER TABLE retrofit §48.0② describes against
-- a pre-existing table).
CREATE TABLE private.processing_runs (
  processing_run_id   uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id            uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  evidence_id          uuid        NOT NULL REFERENCES private.evidence_objects(evidence_id),
  processor            text        NOT NULL,
  model                text        NOT NULL,
  prompt_version        text        NOT NULL,
  source_hash           bytea       NOT NULL,
  output_digest         bytea,
  context_snapshot_seq  bigint      NOT NULL,
  started_at            timestamptz NOT NULL DEFAULT now(),
  finished_at           timestamptz,
  created_at            timestamptz NOT NULL DEFAULT now()
);

-- §8.4 Observation — not a final fact; must reference the Evidence(s) it was derived from.
-- Single-primary-evidence FK here; multi-evidence observations use the same
-- link-table-not-array discipline as evidence_edges (§8.1) if/when a task needs them —
-- flagged rather than speculatively built (YAGNI, no consumer of multi-evidence
-- Observation exists yet in this Phase).
CREATE TABLE private.observations (
  observation_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id       uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  evidence_id     uuid        NOT NULL REFERENCES private.evidence_objects(evidence_id),
  processing_run_id uuid      NOT NULL REFERENCES private.processing_runs(processing_run_id),
  processor       text        NOT NULL,
  model           text        NOT NULL,
  content         jsonb       NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now()
);

-- §11.6/§11.7 Consolidation/Dream derived objects. Column names on the UPDATE-able subset
-- of memory_consolidation_runs match §6.2.2's column-level GRANT exactly
-- (status,input_snapshot_seq,manifest_hash,output_digest,finished_at,error_class).
CREATE TABLE private.memory_consolidation_runs (
  run_id               uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id            uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  reasoning_domain_id  uuid        NOT NULL REFERENCES control.private_reasoning_domains(reasoning_domain_id),
  workspace_id         uuid        REFERENCES control.workspaces(workspace_id),
  status               text        NOT NULL DEFAULT 'PENDING' CHECK (status IN (
                        'PENDING','SELECTING','RUNNING','SUCCEEDED','SUCCEEDED_NO_OUTPUT',
                        'STALE_INPUT','FAILED','CANCELLED')),
  input_snapshot_seq   bigint,
  manifest_hash        bytea,
  output_digest        bytea,
  lease_owner          text,
  lease_expires_at     timestamptz,
  started_at           timestamptz,
  finished_at          timestamptz,
  error_class          text,
  created_at           timestamptz NOT NULL DEFAULT now()
);

-- §11.7: "只把本次 snapshot 选中了哪些 ID 写入运行清单；不修改被选中的 Memory/Evidence."
CREATE TABLE private.memory_consolidation_inputs (
  run_id        uuid   NOT NULL REFERENCES private.memory_consolidation_runs(run_id),
  memory_id     uuid   NOT NULL REFERENCES private.memory_records(memory_id),
  -- Snapshot-time version marker used at publish time to detect STALE_INPUT (§11.7: "验证
  -- input memory version/source_hash" against what changed since selection). No standalone
  -- version counter exists yet on memory_records; consolidation writes it, nothing FKs
  -- into it, so no external contract depends on its exact source until that counter lands.
  input_version bigint NOT NULL,
  source_hash   bytea  NOT NULL,
  ordinal       int    NOT NULL DEFAULT 0,
  PRIMARY KEY (run_id, memory_id)
);

-- §11.6: "Rollup 必须能展开回全部 source memory_id / evidence_id；没有 source closure 的
-- rollup 不可发布" — rollups is the published artifact, rollup_sources is that closure.
CREATE TABLE private.memory_rollups (
  rollup_id  uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id  uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  run_id     uuid        NOT NULL REFERENCES private.memory_consolidation_runs(run_id),
  content    jsonb       NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE private.memory_rollup_sources (
  rollup_id   uuid NOT NULL REFERENCES private.memory_rollups(rollup_id),
  memory_id   uuid NOT NULL REFERENCES private.memory_records(memory_id),
  evidence_id uuid NOT NULL REFERENCES private.evidence_objects(evidence_id),
  PRIMARY KEY (rollup_id, memory_id, evidence_id)
);
