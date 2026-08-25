-- §48 private.* remaining canonical tables — skeleton (not detailed by any task card this
-- wave; kept minimal so the schema is complete and later tasks can ALTER TABLE ADD COLUMN
-- rather than CREATE a table §48 already promised exists).

CREATE TABLE private.conversations (
  conversation_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id        uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  workspace_id     uuid        REFERENCES control.workspaces(workspace_id),
  created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE private.messages (
  message_id      uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id        uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  conversation_id  uuid        NOT NULL REFERENCES private.conversations(conversation_id),
  event_id         uuid        REFERENCES private.events(event_id),
  created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE private.artifact_parts (
  artifact_part_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id         uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  artifact_id       uuid        NOT NULL REFERENCES private.artifacts(artifact_id),
  ordinal           int         NOT NULL DEFAULT 0,
  created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE private.context_bindings (
  context_binding_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id            uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  memory_id            uuid        REFERENCES private.memory_records(memory_id),
  created_at            timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE private.entities (
  entity_id  uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id  uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  name       text        NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE private.relations (
  relation_id     uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id        uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  from_entity_id   uuid        NOT NULL REFERENCES private.entities(entity_id),
  to_entity_id     uuid        NOT NULL REFERENCES private.entities(entity_id),
  relation_kind    text        NOT NULL,
  created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE private.code_snapshots (
  code_snapshot_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id         uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  repository_id     uuid        NOT NULL REFERENCES control.repositories(repository_id),
  created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE private.code_files (
  code_file_id      uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id          uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  code_snapshot_id   uuid        NOT NULL REFERENCES private.code_snapshots(code_snapshot_id),
  path               text        NOT NULL,
  created_at         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE private.code_symbols (
  code_symbol_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id       uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  code_file_id    uuid        NOT NULL REFERENCES private.code_files(code_file_id),
  symbol_name     text        NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE private.code_edges (
  from_symbol_id uuid        NOT NULL REFERENCES private.code_symbols(code_symbol_id),
  to_symbol_id   uuid        NOT NULL REFERENCES private.code_symbols(code_symbol_id),
  edge_kind      text        NOT NULL,
  created_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (from_symbol_id, to_symbol_id, edge_kind)
);

CREATE TABLE private.worktree_overlays (
  worktree_overlay_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id            uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  repository_id        uuid        NOT NULL REFERENCES control.repositories(repository_id),
  created_at           timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE private.contribution_exports (
  contribution_export_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id                uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  memory_id                uuid        REFERENCES private.memory_records(memory_id),
  created_at                timestamptz NOT NULL DEFAULT now()
);
