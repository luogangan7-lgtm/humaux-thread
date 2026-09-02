-- ADR-0012 gateway-side retry idempotency: `call_id` alone (0141's PRIMARY KEY) only protects
-- the *worker* against a duplicate wire call. A caller retry after an ambiguous outcome (e.g.
-- an RPC transport timeout the gateway cannot distinguish from "the worker already ran it")
-- must reuse the same registration row, never mint a fresh `call_id` and register a second row
-- for the same logical attempt (query_embed_rpc card, "Failure semantics": "never mint a new
-- call_id after an ambiguous outcome"). This unique index is what lets
-- `GatewayRetrievalEmbeddingRegistrations::register` do `INSERT ... ON CONFLICT DO NOTHING`
-- keyed on the caller's own logical identity and fall back to the existing row's `call_id` on
-- conflict instead of inserting a duplicate registration (and dispatching a second real
-- provider call).
CREATE UNIQUE INDEX retrieval_embedding_rpc_calls_logical_call_idx
  ON ops.retrieval_embedding_rpc_calls (tenant_id, logical_call_id, attempt_no);
