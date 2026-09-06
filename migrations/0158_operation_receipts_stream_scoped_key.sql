-- §34.0.1 / ADR-0032 (card 11): the local-write idempotency receipt is keyed per stream scope.
--
-- ADR-0031 lifted the read routes to per-request stream identity; card 11 lifts `remember.put`
-- the same way (principal tenant + requested workspace + the process's configured family), so
-- ONE gateway process now commits writes for N (tenant, workspace) pairs and one principal may
-- hold several of them. A caller's logical operation key (`idempotency_key`, §34.0.1) is
-- therefore scoped by the stream it was issued against: the same key under two different
-- `(scope_kind, scope_id)` pairs is two operations, never a CONFLICT (Q9 ruling, archived in
-- research/webgpt-spec-silence-rulings-20260903.md). `projection_version` stays a payload
-- column — it is NEVER part of the key (same ruling; the §16.3 serving switch must not reopen
-- a committed key).
--
-- §46 forward fix (EXPAND_CONTRACT): the primary key is replaced in place. No row is rewritten,
-- deleted or reopened — every existing row is unique under the old 4-column key and therefore
-- under the 6-column superset; the trigger, RLS policy and §6.2.2 grants are untouched.
ALTER TABLE control.operation_receipts
  DROP CONSTRAINT operation_receipts_pkey,
  ADD PRIMARY KEY (tenant_id, principal_id, scope_kind, scope_id, operation, idempotency_key);

COMMENT ON TABLE control.operation_receipts IS
  '§34.0.1: append-only local-write idempotency facts keyed per (tenant, principal, scope_kind, scope_id, operation, idempotency_key) — the stream scope is part of the key (ADR-0032), projection_version never is. Expiry or Evidence deletion never makes a key reusable. Permissions: §6.2.2 only. No secrets or bodies.';
