-- §48 coord.* canonical tables — skeleton (task/canvas coordination; no task card this
-- wave defines their full field set).

CREATE TABLE coord.tasks (
  task_id    uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id  uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  title      text        NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE coord.task_runs (
  task_run_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id    uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  task_id      uuid        NOT NULL REFERENCES coord.tasks(task_id),
  started_at   timestamptz NOT NULL DEFAULT now(),
  finished_at  timestamptz
);

CREATE TABLE coord.leases (
  lease_id   uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id  uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  resource   text        NOT NULL,
  owner      text        NOT NULL,
  expires_at timestamptz NOT NULL
);

CREATE TABLE coord.locks (
  resource   text        PRIMARY KEY,
  tenant_id  uuid        REFERENCES control.tenants(tenant_id),
  owner      text        NOT NULL,
  expires_at timestamptz NOT NULL
);

CREATE TABLE coord.canvases (
  canvas_id  uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id  uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  title      text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE coord.canvas_elements (
  canvas_id  uuid        NOT NULL REFERENCES coord.canvases(canvas_id),
  element_key text       NOT NULL,
  content     jsonb       NOT NULL DEFAULT '{}'::jsonb,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (canvas_id, element_key)
);

CREATE TABLE coord.handoffs (
  handoff_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id  uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  task_id    uuid        REFERENCES coord.tasks(task_id),
  summary    text,
  created_at timestamptz NOT NULL DEFAULT now()
);
