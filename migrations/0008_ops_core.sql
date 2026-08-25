-- §48 ops.* canonical tables. ops.outbox / ops.jobs get real field lists (§60/§31, both
-- named in the §6.2.2 GRANT matrix); the rest are skeleton. ops.mechanism_observations
-- already exists from 0001.

-- §60 Transactional Outbox — same-transaction write with the Evidence-accepting event
-- (§60 "DB 权威写与 Outbox 在同一事务"). Column set inferred from the §60 pseudocode's
-- insert_outbox(tx, commit_seq, stream_seq, event_type, evidence_id) call shape.
CREATE TABLE ops.outbox (
  outbox_id    uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id    uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  commit_seq   bigint      NOT NULL,
  stream_seq   bigint      NOT NULL,
  event_type   text        NOT NULL,
  evidence_id  uuid        NOT NULL REFERENCES private.evidence_objects(evidence_id),
  status       text        NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','PROCESSING','DONE','FAILED')),
  lease_owner  text,
  lease_expires_at timestamptz,
  created_at   timestamptz NOT NULL DEFAULT now(),
  processed_at timestamptz
);

COMMENT ON TABLE ops.outbox IS
  '§60.1: canonical name for what §60''s pseudocode/other sections call "outbox_event" — §48.2 enumerates ops.outbox only, the old name resolves to nothing.';

-- §31 Durable Jobs — field list verbatim; stream_key/stream_seq are typed columns for
-- pipeline jobs, never hidden in payload (§15.2 requirement).
CREATE TABLE ops.jobs (
  job_id            uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id         uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  job_type          text        NOT NULL,
  priority          int         NOT NULL DEFAULT 0,
  status            text        NOT NULL DEFAULT 'PENDING' CHECK (status IN
                    ('PENDING','PROCESSING','WAITING_KEY','RETRY_WAIT','DONE','FAILED','DEAD')),
  attempt           int         NOT NULL DEFAULT 0,
  next_retry_at     timestamptz,
  lease_owner       text,
  lease_expires_at  timestamptz,
  idempotency_key   text        NOT NULL,
  stream_key        text,
  stream_seq        bigint,
  payload           jsonb       NOT NULL DEFAULT '{}'::jsonb,
  last_error_class  text,
  created_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, idempotency_key)
);

COMMENT ON COLUMN ops.jobs.status IS
  '§61 SKIP LOCKED claim SQL reads/writes this column name verbatim; do not rename to state (ch=32 note: ops.jobs uses status, projection.stream_log uses state — the two are not interchangeable).';

-- §32.1 Scheduler Singleton / Failover Gate.
CREATE TABLE ops.scheduler_leases (
  schedule_id      text        NOT NULL,
  planned_at        timestamptz NOT NULL,
  idempotency_key   text        NOT NULL,
  leader_owner      text,
  lease_expires_at  timestamptz,
  created_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (schedule_id, planned_at),
  UNIQUE (idempotency_key)
);

-- Remaining §48 ops.* canonical tables — skeleton.
CREATE TABLE ops.model_call_ledger (
  model_call_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id      uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  provider       text        NOT NULL,
  called_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ops.stage_runs (
  stage_run_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id     uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  stage_name    text        NOT NULL,
  started_at    timestamptz NOT NULL DEFAULT now(),
  finished_at   timestamptz
);

CREATE TABLE ops.data_disclosure_sources (
  data_disclosure_source_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id                  uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  created_at                 timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ops.selection_snapshots (
  selection_snapshot_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id               uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  created_at              timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ops.selection_snapshot_items (
  selection_snapshot_id uuid NOT NULL REFERENCES ops.selection_snapshots(selection_snapshot_id),
  item_id                uuid NOT NULL,
  PRIMARY KEY (selection_snapshot_id, item_id)
);

CREATE TABLE ops.restore_drills (
  restore_drill_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  performed_at      timestamptz NOT NULL DEFAULT now(),
  succeeded          boolean     NOT NULL
);

CREATE TABLE ops.consistency_reports (
  consistency_report_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id               uuid        REFERENCES control.tenants(tenant_id),
  generated_at            timestamptz NOT NULL DEFAULT now(),
  report                  jsonb       NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE ops.source_acquisition_jobs (
  source_acquisition_job_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  source_url                 text        NOT NULL,
  created_at                 timestamptz NOT NULL DEFAULT now()
);
