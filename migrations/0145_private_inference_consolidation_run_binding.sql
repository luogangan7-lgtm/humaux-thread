-- §11.8 consolidation inference hop, second half: the private worker must be able to LOCATE
-- and READ the inputs it reasons over. Before this migration (verified live, task card
-- "real_consolidation_reasoner"):
--   1. `ops.private_inference_rpc_calls` (0143) carried `reasoning_domain_id` +
--      `input_manifest_hash` but no run id, and `SealedPrivateReasoningRequest` (§11.8) is
--      identifiers + manifest hash only — so `humaux-private-worker` had no way to find which
--      `private.memory_consolidation_runs` row a Consolidate call refers to.
--   2. `role_private_worker` had zero grants on `private.memory_consolidation_runs` /
--      `private.memory_consolidation_inputs` (0011 grants only role_consolidation_worker +
--      role_maintenance).
--   3. `private.memory_records`' `memory_records_tenant_and_visibility` (after 0140) admits a
--      WORKSPACE_SHARED row to a headless session only for `role_retrieval_worker`; both
--      consolidation roles set `humaux.tenant_id` alone (no acting user), so a WORKSPACE_SHARED
--      memory was invisible to selection AND to the private worker's materialization. Same for
--      `private.evidence_objects` (0140's `evidence_objects_retrieval_worker_read` is
--      `TO role_retrieval_worker` only).
-- ADR-0015 records the reasoning; §46: forward-fix only, 0001–0144 untouched.

-- (1) Registration row carries the run id. Registered by role_consolidation_worker together
-- with the sealed identifiers; read by role_private_worker after claim. NULL for every other
-- purpose (contribution calls have no consolidation run). FK to the run row so a registration
-- can never point at a run that does not exist — the private worker re-verifies the run's
-- tenant / reasoning_domain_id against the sealed request and recomputes the manifest hash
-- over the run's recorded inputs before any provider call (ADR-0015 "integrity gate").
ALTER TABLE ops.private_inference_rpc_calls
  ADD COLUMN consolidation_run_id uuid REFERENCES private.memory_consolidation_runs(run_id);

COMMENT ON COLUMN ops.private_inference_rpc_calls.consolidation_run_id IS
  '§11.8/ADR-0015: the private.memory_consolidation_runs row a Consolidate call reasons over; NULL for non-consolidation purposes. The private worker locates inputs by this id and re-verifies input_manifest_hash over them.';

-- 0143's column-narrow INSERT grant widened by exactly this one column (§6.2.2 cell updated in
-- lockstep; `cargo xtask rls-check` pins the column list).
GRANT INSERT (consolidation_run_id) ON ops.private_inference_rpc_calls TO role_consolidation_worker;

-- (2) role_private_worker reads the run + its input manifest (SELECT only — §11.6 MUST NOT:
-- it never writes runs, inputs, rollups, memory_records or memory_evidence). Both tables
-- already carry a PERMISSIVE `FOR ALL` tenant policy that applies to every role (0012:
-- `memory_consolidation_runs_tenant_isolation` = the §62 NULLIF tenant clause;
-- `memory_consolidation_inputs_tenant` = EXISTS(runs r ... r.tenant_id = <tenant clause>)),
-- so no role-specific policy is added here — a second PERMISSIVE policy would only be OR'd
-- with the one that already covers this role.
GRANT SELECT ON private.memory_consolidation_runs TO role_private_worker;
GRANT SELECT ON private.memory_consolidation_inputs TO role_private_worker;

-- (3) memory_records: 0140's `current_user = 'role_retrieval_worker'` disjunct widened to the
-- two headless consolidation roles. Everything else is byte-identical to 0140's USING (fetched
-- with pg_get_expr before writing this file); WITH CHECK untouched — neither role writes this
-- table through this path (role_private_worker's INSERT grant is the Distill path, not this).
-- Same reasoning as 0140: a headless, tenant-pinned worker with no acting user can never
-- satisfy the membership branch; §6.1.2 enforces per-query visibility at read time downstream,
-- and the rollup a consolidation run publishes inherits the run's own workspace scoping
-- (`consolidate_repo::publish_rollup`), never widening a WORKSPACE_SHARED input to the tenant.
ALTER POLICY memory_records_tenant_and_visibility ON private.memory_records
USING (
  current_user = 'role_migration_owner'
  OR (
    tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
    AND (
      current_user IN ('role_retrieval_worker', 'role_consolidation_worker', 'role_private_worker')
      OR visibility_class = 'TENANT_SHARED'
      OR (visibility_class = 'USER_PRIVATE'
          AND visibility_user_id = current_setting('humaux.user_id', true)::uuid)
      OR (visibility_class = 'WORKSPACE_SHARED'
          AND EXISTS (
            SELECT 1 FROM control.memberships m
            WHERE m.tenant_id = memory_records.tenant_id
              AND m.user_id = current_setting('humaux.user_id', true)::uuid
              AND m.state = 'ACTIVE'
          ))
    )
  )
);

-- (4) evidence_objects: widen the ROLE LIST of 0140's separate permissive SELECT policy rather
-- than adding a 7th policy — the W1 continuity census (xtask rls-check
-- `check_w1_continuity_boundary`) pins this table's policy count at 6 and the legacy policy's
-- text; the widened policy's USING stays the verbatim §62 NULLIF tenant clause.
ALTER POLICY evidence_objects_retrieval_worker_read ON private.evidence_objects
  TO role_retrieval_worker, role_consolidation_worker, role_private_worker;
