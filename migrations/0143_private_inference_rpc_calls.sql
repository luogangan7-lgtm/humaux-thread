-- §11.8 ADR-0012-pattern RPC: humaux-consolidation-worker -> humaux-private-worker
-- inference-only call. §4.2/§11.8: consolidation holds ConsolidationDbPool only, private-worker
-- holds PrivateWorkerDbPool only; this table is the sole cross-process handshake surface —
-- role_consolidation_worker registers (opaque metadata only), role_private_worker
-- claims/finishes (narrow columns only). Mirrors migrations/0141_retrieval_embedding_rpc_calls
-- .sql's shape verbatim (same two-role split, same state machine) — see that migration's
-- header for the reasoning this one does not repeat.
--
-- Never a column here: sealed input plaintext, AuthorizationScope, credentials, route/model
-- identifiers beyond the opaque binding_id/binding_version pair, or the API key. `input_manifest_hash`
-- only proves *what* was requested; `humaux-private-worker` resolves/decrypts everything else
-- itself from its own DB capability (§11.8 "payload 只通过内部 mTLS RPC body 传递，不携带 DB
-- capability").
CREATE TABLE ops.private_inference_rpc_calls (
  call_id             uuid PRIMARY KEY,

  tenant_id           uuid NOT NULL,
  reasoning_domain_id uuid NOT NULL,
  binding_id          uuid NOT NULL,
  binding_version     bigint NOT NULL CHECK (binding_version > 0),
  purpose             text NOT NULL CHECK (purpose IN ('Distill', 'Consolidate', 'Vision', 'ContributionDeidentify')),
  input_manifest_hash bytea NOT NULL CHECK (octet_length(input_manifest_hash) = 32),

  registered_at       timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(registered_at)),
  expires_at          timestamptz NOT NULL CHECK (isfinite(expires_at)),
  registered_by       name NOT NULL DEFAULT current_user,

  state               text NOT NULL DEFAULT 'REGISTERED'
                         CHECK (state IN ('REGISTERED', 'CLAIMED', 'COMPLETED')),
  claimed_at          timestamptz CHECK (claimed_at IS NULL OR isfinite(claimed_at)),
  claimed_by          name,
  finished_at         timestamptz CHECK (finished_at IS NULL OR isfinite(finished_at)),

  outcome                  text CHECK (outcome IS NULL OR outcome IN ('COMPLETED', 'FAILED')),
  -- §11.8 `PrivateReasoningResult`, verbatim field set minus the caller-known binding/domain
  -- (already columns above). `response_output_bytes` is the inference output over private
  -- memory content — same shape retrieval_embedding_rpc_calls's `response_vector` already
  -- stores as the sole payload of a successful RPC reply, so a same-call_id replay never
  -- re-dispatches the provider (§11.8/ADR-0012 idempotency-by-call_id).
  response_output_bytes    bytea,
  response_output_sha256   bytea CHECK (response_output_sha256 IS NULL OR octet_length(response_output_sha256) = 32),
  response_provider_trace  text,
  response_model_call_id   uuid,
  response_failure_message text,

  CHECK (expires_at > registered_at),
  CHECK (registered_by = 'role_consolidation_worker'::name),
  CHECK ((state = 'REGISTERED') = (claimed_at IS NULL AND claimed_by IS NULL)),
  CHECK (state <> 'CLAIMED' OR claimed_by = 'role_private_worker'::name),
  CHECK ((state = 'COMPLETED') = (finished_at IS NOT NULL)),
  CHECK ((state = 'COMPLETED') = (outcome IS NOT NULL)),
  CHECK (
    (outcome = 'COMPLETED')
    = (
      response_output_bytes IS NOT NULL
      AND response_output_sha256 IS NOT NULL
      AND response_model_call_id IS NOT NULL
    )
  )
);

CREATE INDEX private_inference_rpc_calls_tenant_expiry_idx
  ON ops.private_inference_rpc_calls (tenant_id, expires_at);

ALTER TABLE ops.private_inference_rpc_calls OWNER TO role_migration_owner;
ALTER TABLE ops.private_inference_rpc_calls ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.private_inference_rpc_calls FORCE ROW LEVEL SECURITY;

-- §62 canonical tenant clause, verbatim (matches 0141's own policy).
CREATE POLICY private_inference_rpc_calls_tenant ON ops.private_inference_rpc_calls
  USING (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid)
  WITH CHECK (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid);

REVOKE ALL ON ops.private_inference_rpc_calls FROM PUBLIC, role_gateway, role_private_worker,
  role_consolidation_worker, role_public_worker, role_retrieval_worker, role_batch_issuer,
  role_maintenance, role_admin;

-- role_consolidation_worker: registers a call (INSERT, only the columns it can legitimately
-- supply) and may read back its own registrations (idempotent-retry inspection). No UPDATE —
-- it never claims/finishes its own registration.
GRANT SELECT, INSERT (
  call_id, tenant_id, reasoning_domain_id, binding_id, binding_version, purpose,
  input_manifest_hash, expires_at
) ON ops.private_inference_rpc_calls TO role_consolidation_worker;

-- role_private_worker: reads a registration to claim it, then writes only the
-- claim/completion columns.
GRANT SELECT, UPDATE (
  state, claimed_at, claimed_by, finished_at, outcome, response_output_bytes,
  response_output_sha256, response_provider_trace, response_model_call_id,
  response_failure_message
) ON ops.private_inference_rpc_calls TO role_private_worker;

COMMENT ON TABLE ops.private_inference_rpc_calls IS
  '§11.8: consolidation-registered / private-worker-claimed inference RPC calls. Idempotency anchor by call_id; never stores sealed input plaintext or a credential.';
