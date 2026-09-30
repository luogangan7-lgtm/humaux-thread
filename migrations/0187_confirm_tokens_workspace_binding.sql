-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0186). ADR-0054 D-C (card 29): a
-- confirm token binds the (tenant, workspace) it was minted for. `mint_with_audit` INSERTs the
-- narrowed request workspace; `consume_in_txn` matches it (`AND workspace_id = $7`), so a token
-- minted in W1 and presented in W2 of the same tenant matches zero rows = CONFLICT (ADR-0018 D-B).
--
-- EXPAND only: a nullable column + FK, deliberately no CHECK and no backfill, so the old and the
-- new gateway binary both run against this schema (§46.1). The pre-card-29 binary INSERTs without
-- workspace_id (NULL, FK unchecked under MATCH SIMPLE) and consumes without matching it; the
-- ADR-0054 binary always INSERTs the narrowed workspace and consumes only on equality, so a NULL
-- row — pre-0187 or minted by an old binary during the rollout — can never be consumed by it
-- (`NULL = x` is never true): an in-flight token answers CONFLICT exactly like an expired one and
-- the client re-mints (TTL 300 s). The binding's safety therefore rests on the consume predicate,
-- not on a constraint. A generic backfill has no source (candidate targets, TENANT_SHARED /
-- USER_PRIVATE memories carry no workspace) and would have to defeat the 0148 single-use trigger
-- and FORCE RLS. CONTRACT is a later migration, after the §46.1 observation window with only
-- ADR-0054 binaries serving: `CHECK (workspace_id IS NOT NULL) NOT VALID`, then VALIDATE once
-- control.sweep_confirm_tokens (0169) has removed every NULL row (a CHECK is enforced on INSERT
-- even while NOT VALID, so shipping it here would make every old-binary mint fail with 23514).

-- The FK is composite (card 7 subjects pattern): a token can only name a workspace of its own
-- tenant; MATCH SIMPLE leaves the NULL legacy rows unchecked. Grants unchanged: role_gateway's
-- table-level INSERT covers the new column, UPDATE stays consumed_at-only and the single-use
-- trigger already makes workspace_id immutable. No DML.

ALTER TABLE control.confirm_tokens
  ADD COLUMN workspace_id uuid,
  ADD CONSTRAINT confirm_tokens_tenant_workspace_fk FOREIGN KEY (tenant_id, workspace_id)
    REFERENCES control.workspaces (tenant_id, workspace_id) ON DELETE CASCADE;

COMMENT ON COLUMN control.confirm_tokens.workspace_id IS
  'ADR-0054: the workspace the token was minted for; consume requires equality; NULL rows (pre-0187 or minted by a pre-ADR-0054 binary) can never be consumed by an ADR-0054 gateway';
