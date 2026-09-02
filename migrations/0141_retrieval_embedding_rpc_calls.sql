-- ADR-0012: gateway -> humaux-retrieval-worker query-embedding RPC registration/idempotency
-- anchor. §4.2 keeps `role_gateway` free of the PLATFORM_RETRIEVAL provider credential and the
-- `role_retrieval_worker` pool; this table is the sole cross-process handshake surface — the
-- opaque metadata a `role_gateway` INSERT reserves, and the narrow columns a
-- `role_retrieval_worker` claim/finish UPDATE is allowed to touch. No query text, vector, or
-- provider credential is ever a column here; `query_sha256` only proves the RPC body the
-- worker receives out-of-band matches the row the gateway registered in-band (query_embed_rpc
-- card §2 "query mismatch -> 409, zero side effects").
CREATE TABLE ops.retrieval_embedding_rpc_calls (
  call_id             uuid PRIMARY KEY,

  tenant_id           uuid NOT NULL,
  principal_id        uuid NOT NULL CHECK (principal_id <> '00000000-0000-0000-0000-000000000000'),
  user_id             uuid NOT NULL CHECK (user_id <> '00000000-0000-0000-0000-000000000000'),
  workspace_id        uuid NOT NULL CHECK (workspace_id <> '00000000-0000-0000-0000-000000000000'),

  request_id          uuid NOT NULL,
  logical_call_id     uuid NOT NULL,
  attempt_no          integer NOT NULL CHECK (attempt_no > 0),

  -- §55.1 `ProfileFingerprint` wire form (`crates/retrieval/src/request.rs`): "sha256:" + 64 hex.
  profile_fingerprint text NOT NULL CHECK (profile_fingerprint ~ '^sha256:[0-9a-f]{64}$'),
  query_sha256        bytea NOT NULL CHECK (octet_length(query_sha256) = 32),

  registered_at       timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(registered_at)),
  expires_at          timestamptz NOT NULL CHECK (isfinite(expires_at)),
  registered_by       name NOT NULL DEFAULT current_user,

  state               text NOT NULL DEFAULT 'REGISTERED'
                         CHECK (state IN ('REGISTERED', 'CLAIMED', 'COMPLETED')),
  claimed_at          timestamptz CHECK (claimed_at IS NULL OR isfinite(claimed_at)),
  claimed_by          name,
  finished_at         timestamptz CHECK (finished_at IS NULL OR isfinite(finished_at)),

  outcome             text CHECK (outcome IS NULL OR outcome IN ('EMBEDDED', 'SKIPPED', 'UNAVAILABLE')),
  response_vector         real[],
  response_provider_id    text,
  response_model_id       text,
  response_model_revision text,
  response_dimension      integer CHECK (response_dimension IS NULL OR response_dimension > 0),
  response_failure_code   text,

  CHECK (expires_at > registered_at),
  CHECK (registered_by = 'role_gateway'::name),
  CHECK ((state = 'REGISTERED') = (claimed_at IS NULL AND claimed_by IS NULL)),
  CHECK (state <> 'CLAIMED' OR claimed_by = 'role_retrieval_worker'::name),
  CHECK ((state = 'COMPLETED') = (finished_at IS NOT NULL)),
  CHECK ((state = 'COMPLETED') = (outcome IS NOT NULL)),
  CHECK (
    (outcome = 'EMBEDDED')
    = (
      response_vector IS NOT NULL
      AND response_provider_id IS NOT NULL
      AND response_model_id IS NOT NULL
      AND response_dimension IS NOT NULL
    )
  )
);

CREATE INDEX retrieval_embedding_rpc_calls_tenant_expiry_idx
  ON ops.retrieval_embedding_rpc_calls (tenant_id, expires_at);

ALTER TABLE ops.retrieval_embedding_rpc_calls OWNER TO role_migration_owner;
ALTER TABLE ops.retrieval_embedding_rpc_calls ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.retrieval_embedding_rpc_calls FORCE ROW LEVEL SECURITY;

-- §62 canonical tenant clause, 0031's NULLIF-hardened form verbatim (xtask rls-check pins the
-- deparsed shape; see that checker's own doc for why the NULLIF wrapper matters).
CREATE POLICY retrieval_embedding_rpc_calls_tenant ON ops.retrieval_embedding_rpc_calls
  USING (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid)
  WITH CHECK (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid);

REVOKE ALL ON ops.retrieval_embedding_rpc_calls FROM PUBLIC, role_gateway, role_private_worker,
  role_consolidation_worker, role_public_worker, role_retrieval_worker, role_batch_issuer,
  role_maintenance, role_admin;

-- role_gateway: registers a call (INSERT, only the columns it can legitimately supply) and may
-- read back what it and the worker have written (idempotent-retry inspection). It gets no
-- UPDATE at all — it never claims/finishes its own registration.
GRANT SELECT, INSERT (
  call_id, tenant_id, principal_id, user_id, workspace_id, request_id, logical_call_id,
  attempt_no, profile_fingerprint, query_sha256, expires_at
) ON ops.retrieval_embedding_rpc_calls TO role_gateway;

-- role_retrieval_worker: reads a registration to claim it, then writes only the
-- claim/completion columns — never the registration identity/metadata columns role_gateway
-- alone may set.
GRANT SELECT, UPDATE (
  state, claimed_at, claimed_by, finished_at, outcome, response_vector, response_provider_id,
  response_model_id, response_model_revision, response_dimension, response_failure_code
) ON ops.retrieval_embedding_rpc_calls TO role_retrieval_worker;

COMMENT ON TABLE ops.retrieval_embedding_rpc_calls IS
  'ADR-0012: gateway-registered / retrieval-worker-claimed query-embedding RPC calls. Idempotency anchor by call_id; never stores query text, vectors are the only response payload, no credential.';
