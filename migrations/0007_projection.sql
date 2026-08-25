-- §15 projection.* stream watermark core (§15.1/§15.3, DDL verbatim) + §48 remaining
-- projection.* canonical tables (skeleton).

-- §15.1 (a): per-stream dense sequence — 12 columns, 9-state closed set (§15.2 TERMINAL/
-- SETTLED_OK/GAP/PENDING classification reads this CHECK's set, does not redefine it).
CREATE TABLE projection.stream_log (
  tenant_id uuid NOT NULL, scope_kind text NOT NULL, scope_id uuid NOT NULL,
  domain text NOT NULL, projection_kind text NOT NULL, projection_version text NOT NULL,
  stream_seq  bigint NOT NULL,
  commit_seq  bigint NOT NULL,
  state       text NOT NULL DEFAULT 'ISSUED' CHECK (state IN
    ('ISSUED','PROCESSING','WAITING_KEY','RETRY_WAIT','LOST',
      'DONE','SKIPPED_BY_POLICY','FAILED','TOMBSTONED')),
  error_class text,
  issued_at   timestamptz NOT NULL DEFAULT now(),
  settled_at  timestamptz,
  CHECK ((state IN ('DONE','SKIPPED_BY_POLICY','FAILED','TOMBSTONED')) = (settled_at IS NOT NULL)),
  PRIMARY KEY (tenant_id, scope_kind, scope_id, domain,
               projection_kind, projection_version, stream_seq)
);

COMMENT ON TABLE projection.stream_log IS
  '§15.1: per-stream dense seq ledger, the sole "expected set" definition (1..max(stream_seq)); commit_seq is audit-only, never used for completeness.';

-- §15.3: four highwaters + read-routing flags, open_gap_count already excluded (§48.0④ —
-- the aggregate counter cannot prove prefix contiguity, deleted in favor of stream_log).
CREATE TABLE projection.stream_checkpoints (
  tenant_id uuid NOT NULL, scope_kind text NOT NULL, scope_id uuid NOT NULL,
  domain text NOT NULL, projection_kind text NOT NULL, projection_version text NOT NULL,
  issued_highwater     bigint NOT NULL DEFAULT 0,
  evidence_highwater   bigint NOT NULL DEFAULT 0,
  knowledge_highwater  bigint NOT NULL DEFAULT 0,
  projection_highwater bigint NOT NULL DEFAULT 0,
  serving boolean NOT NULL DEFAULT false,
  shadow  boolean NOT NULL DEFAULT false,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, scope_kind, scope_id, domain,
               projection_kind, projection_version)
);

-- role_migration_owner-owned trigger (§6.2.2 note on this table): updated_at is never in
-- any GRANT, it is written only here.
CREATE FUNCTION projection.stream_checkpoints_touch_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

CREATE TRIGGER stream_checkpoints_touch_updated_at
BEFORE UPDATE ON projection.stream_checkpoints
FOR EACH ROW EXECUTE FUNCTION projection.stream_checkpoints_touch_updated_at();

-- §15.2: "processing_gaps 降级为 stream_log 上的视图，不再独立写入" — verbatim.
CREATE VIEW projection.processing_gaps AS
SELECT tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version,
       stream_seq, commit_seq, state, error_class, issued_at
FROM projection.stream_log WHERE state IN ('FAILED','LOST');

-- §48 remaining projection.* canonical tables — skeleton.
CREATE TABLE projection.retrieval_cards (
  card_id      uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id    uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  memory_id    uuid        REFERENCES private.memory_records(memory_id),
  created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE projection.tenant_placements (
  tenant_id  uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  cell_id    uuid        NOT NULL,
  placed_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, cell_id)
);

CREATE TABLE projection.index_runs (
  index_run_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id    uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  started_at   timestamptz NOT NULL DEFAULT now(),
  finished_at  timestamptz
);
