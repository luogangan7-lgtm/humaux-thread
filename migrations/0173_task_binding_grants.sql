-- Card 22c (ADR-0046, ruling 2026-09-20 §五): ExplicitTaskContext stops being a property of a
-- stored row and becomes the USE authority of one *current task context instance*, proven by a
-- verified, non-inheritable task-binding authorization.
--
-- Three invariants this migration is the DB half of:
--   I-STORE      every MemoryRecord's stored authority <= its authority-basis origin ceiling <= 5.
--   I-TASK       an effective context authority of 6 implies a live grant row naming the current
--                task, its epoch, the exact binding, the exact memory and that memory's exact
--                canonical payload hash, with purpose ADOPT_TASK_INSTRUCTION.
--   I-NONINHERIT derivation, copy, retrieval, rerank, migration, task change and version
--                replacement can neither create nor inherit such a grant.
--
-- EXPAND_ONLY and NULL-safe: one NOT VALID CHECK, one UNIQUE on an already-unique key, one
-- defaulted column, one new table. No DELETE, no UPDATE of user rows, no backfill.

-- ---------------------------------------------------------------------------------------------
-- 1. I-STORE, DB side. NOT VALID and not VALIDATE'd here, deliberately: humaux_thread_dev holds
--    four fixture-born rows with authority_class = 'ExplicitTaskContext' (listed with
--    memory_id/tenant/created_at in ADR-0046). NOT VALID still checks every NEW row and every
--    row an UPDATE touches, so no new stored 6 can be created from this moment; the four legacy
--    rows are dispositioned by a card-24 housekeeping step that ends in
--    `ALTER TABLE ... VALIDATE CONSTRAINT`. They are NOT mass-downgraded (ruling §五.3: a
--    migration must not launder a historical illegal authority into a legal lower one).
--    The 9-value authority_class CHECK of 0004 is untouched: the enum is not shrinking, the
--    *write* of one of its values is.
ALTER TABLE private.memory_records
  ADD CONSTRAINT memory_records_stored_authority_v2_check
  CHECK (authority_class <> 'ExplicitTaskContext') NOT VALID;

COMMENT ON CONSTRAINT memory_records_stored_authority_v2_check ON private.memory_records IS
  'Card 22c / ADR-0046 I-STORE: a stored Memory row never carries ExplicitTaskContext. 6 is the use authority of a current task context instance, proven by private.task_binding_grants, not a content property. NOT VALID because of four pre-existing fixture rows (ADR-0046); VALIDATE is a card-24 step.';

-- ---------------------------------------------------------------------------------------------
-- 2. The composite key the grant's FK references. `context_binding_id` is already the PK, so
--    this UNIQUE can never reject an existing or future row; its only job is to make
--    (tenant_id, context_binding_id, scope_kind, scope_id, memory_id, mode) a referenceable key,
--    which is what forces a grant to agree with its binding's *whole shape* rather than just
--    pointing at a binding id. Change the binding's task or memory and the FK has nowhere to
--    land (bindings are revoke-only in practice, so this is a shape contract, not a live race).
ALTER TABLE private.context_bindings
  ADD CONSTRAINT context_bindings_task_grant_identity_v2_uq
  UNIQUE (tenant_id, context_binding_id, scope_kind, scope_id, memory_id, mode);

-- ---------------------------------------------------------------------------------------------
-- 3. Task authorization epoch. coord.tasks had no lifecycle counter; the grant stores the epoch
--    it was issued under and admission requires equality, so closing/reopening a task (or any
--    other authority-invalidating lifecycle event) invalidates every grant issued before it by
--    bumping this one number. Additive with a DEFAULT 0: every existing task keeps a stable
--    epoch and every existing grant-less binding is unaffected.
--    There is no task lifecycle operation in this tree today, so this column has exactly one
--    future writer (documented in ADR-0046): whoever adds that operation bumps it. Nothing in
--    card 22c writes it.
ALTER TABLE coord.tasks
  ADD COLUMN authorization_epoch bigint NOT NULL DEFAULT 0
  CONSTRAINT tasks_authorization_epoch_nonnegative CHECK (authorization_epoch >= 0);

COMMENT ON COLUMN coord.tasks.authorization_epoch IS
  'Card 22c / ADR-0046: monotonic task-authorization epoch. private.task_binding_grants stores the epoch at issue time and admission requires equality, so a task lifecycle change invalidates every grant issued before it. Default 0; no writer in this tree yet.';

-- ---------------------------------------------------------------------------------------------
-- 4. The authorization object itself.
--
--    What each column is load-bearing for (ruling §二.1: an effective authorization binds all of
--    these or it is not an authorization):
--      tenant_id + task_id + task_epoch      -- WHICH task, in WHICH lifecycle generation
--      context_binding_id + scope/mode       -- WHICH binding obligation (composite FK below)
--      memory_id + payload_sha256            -- WHICH memory, at WHICH exact content
--      purpose                               -- reference vs adopt-as-instruction (only the
--                                               latter can ever produce 6)
--      issuer_kind + issued_by_principal_id  -- WHO approved, and under which authority shape
--      authorization_evidence_id             -- the approval's own Evidence row (never linked
--                                               into the target memory's memory_evidence: a
--                                               task receipt is not a general authority basis)
--      operation_id                          -- the approving operation, unique per tenant, so a
--                                               replayed operation cannot mint a second grant
--      policy_version                        -- which frozen contract admitted it
--      issued_at / expires_at / revoked_at   -- validity window and revocation
--
--    There is NO memory_revision column (the ruling's template has one): Memory rows in this
--    schema are immutable and supersede mints a NEW memory_id, so "exact version" is exactly
--    (memory_id, payload_sha256). ADR-0046 records that deviation.
--
--    grant_authority is pinned to 6 by CHECK rather than being a free number: this table exists
--    only to carry ExplicitTaskContext. A row that wanted to grant something else would be a
--    second, unreviewed authority ladder.
CREATE TABLE private.task_binding_grants (
  tenant_id                 uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  context_binding_id        uuid        NOT NULL,
  scope_kind                text        NOT NULL CHECK (scope_kind = 'TASK'),
  task_id                   uuid        NOT NULL,
  memory_id                 uuid        NOT NULL,
  mode                      text        NOT NULL CHECK (mode = 'MANDATORY'),
  task_epoch                bigint      NOT NULL CHECK (task_epoch >= 0),
  payload_sha256            bytea       NOT NULL CHECK (octet_length(payload_sha256) = 32),
  grant_authority           smallint    NOT NULL CHECK (grant_authority = 6),
  purpose                   text        NOT NULL CHECK (purpose = 'ADOPT_TASK_INSTRUCTION'),
  issuer_kind               text        NOT NULL
                                        CHECK (issuer_kind IN ('AUTHENTICATED_TASK_REQUEST', 'TENANT_POLICY')),
  issued_by_principal_id    uuid        NOT NULL,
  authorization_evidence_id uuid        NOT NULL,
  operation_id              uuid        NOT NULL,
  policy_version            text        NOT NULL CHECK (policy_version <> ''),
  issued_at                 timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(issued_at)),
  -- NULL = no wall-clock expiry; the task's own epoch and the revoke path are the lifetime.
  expires_at                timestamptz CHECK (expires_at IS NULL OR isfinite(expires_at)),
  revoked_at                timestamptz,
  PRIMARY KEY (tenant_id, context_binding_id),
  UNIQUE (tenant_id, operation_id),
  CHECK (expires_at IS NULL OR expires_at > issued_at),
  CHECK (revoked_at IS NULL OR revoked_at >= issued_at),
  CONSTRAINT task_binding_grants_binding_fk
    FOREIGN KEY (tenant_id, context_binding_id, scope_kind, task_id, memory_id, mode)
    REFERENCES private.context_bindings (tenant_id, context_binding_id, scope_kind, scope_id, memory_id, mode)
    ON DELETE CASCADE,
  CONSTRAINT task_binding_grants_authorization_evidence_fk
    FOREIGN KEY (tenant_id, authorization_evidence_id)
    REFERENCES private.evidence_objects (tenant_id, evidence_id)
    ON DELETE RESTRICT
);

ALTER TABLE private.task_binding_grants OWNER TO role_migration_owner;
ALTER TABLE private.task_binding_grants ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.task_binding_grants FORCE ROW LEVEL SECURITY;

-- The grant never widens what the binding itself is visible for: both policies re-derive the
-- binding through private.context_bindings, which carries its own FORCE'd tenant policy, so a
-- grant is reachable exactly when its binding is.
CREATE POLICY task_grant_gateway_v2 ON private.task_binding_grants
  FOR ALL TO role_gateway
  USING (
    tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
    AND EXISTS (SELECT 1 FROM private.context_bindings cb
                 WHERE cb.tenant_id = task_binding_grants.tenant_id
                   AND cb.context_binding_id = task_binding_grants.context_binding_id)
  )
  WITH CHECK (
    tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
    AND EXISTS (SELECT 1 FROM private.context_bindings cb
                 WHERE cb.tenant_id = task_binding_grants.tenant_id
                   AND cb.context_binding_id = task_binding_grants.context_binding_id)
  );

CREATE POLICY task_grant_reader_v2 ON private.task_binding_grants
  FOR SELECT TO role_retrieval_worker
  USING (
    tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
    AND EXISTS (SELECT 1 FROM private.context_bindings cb
                 WHERE cb.tenant_id = task_binding_grants.tenant_id
                   AND cb.context_binding_id = task_binding_grants.context_binding_id)
  );

-- §6.2.2 row (added in the same change). The distill / consolidation / public workers hold ZERO
-- privileges here on purpose: a background summarizer that could mint a grant would be exactly
-- the "后台蒸馏自动提权" §10.1 rule 2 forbids. role_maintenance and role_admin likewise — a
-- repair role that can revive a revoked authorization is not a repair role.
REVOKE ALL ON private.task_binding_grants FROM PUBLIC, role_gateway, role_private_worker,
  role_consolidation_worker, role_public_worker, role_retrieval_worker, role_batch_issuer,
  role_maintenance, role_admin;
GRANT SELECT, INSERT ON private.task_binding_grants TO role_gateway;
GRANT UPDATE (revoked_at) ON private.task_binding_grants TO role_gateway;
GRANT SELECT ON private.task_binding_grants TO role_retrieval_worker;

-- Revoke-only is a mechanism, not a review rule (same shape as 0148's single-use trigger):
-- the ONLY legal UPDATE flips revoked_at NULL -> non-NULL once and changes nothing else.
-- Redirecting a grant to another task/memory/hash, clearing revoked_at, or extending expiry
-- all raise 23514. Owner function, ordinary invoker; runtime roles hold no DDL (§6.2.1) so
-- they cannot drop it.
CREATE FUNCTION private.check_task_grant_revoke_only() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog
AS $$
BEGIN
  IF OLD.revoked_at IS NOT NULL
     OR NEW.revoked_at IS NULL
     OR (to_jsonb(NEW) - 'revoked_at') <> (to_jsonb(OLD) - 'revoked_at') THEN
    RAISE EXCEPTION 'task binding grants are revoke-only and otherwise immutable' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
ALTER FUNCTION private.check_task_grant_revoke_only() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION private.check_task_grant_revoke_only() FROM PUBLIC;
CREATE TRIGGER task_grant_revoke_only_v2
  BEFORE UPDATE ON private.task_binding_grants
  FOR EACH ROW EXECUTE FUNCTION private.check_task_grant_revoke_only();

COMMENT ON TABLE private.task_binding_grants IS
  'Card 22c / ADR-0046 I-TASK: the verified, non-inheritable authorization that lets ONE current task context instance use a memory at ExplicitTaskContext. Binds tenant+task+task_epoch, the exact binding, the exact memory and its exact canonical payload hash, purpose ADOPT_TASK_INSTRUCTION, an issuer, an authorization Evidence row, the approving operation and a policy version. Never copied by derivation, consolidation, supersede, retrieval or migration; revoke-only. Permissions: §6.2.2 only.';
