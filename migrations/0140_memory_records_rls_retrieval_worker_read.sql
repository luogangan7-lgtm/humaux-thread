-- §46 new migration (0012 stays immutable). Read-only RLS bypass for
-- `role_retrieval_worker` on the two tables `adapters::projection_worker::resolve_memory`
-- INNER JOINs (`private.memory_records` and `private.evidence_objects`), mirroring the
-- `role_migration_owner` clause 0012 already has on memory_records.
--
-- Why both tables: the worker is a headless, `role_retrieval_worker`-owned consumer with a
-- tenant-only session (it impersonates no principal; `humaux.user_id` is pinned to a nil
-- sentinel purely so `current_setting(...)::uuid` never sees an empty string — PG folds that
-- cast at plan time). Before this migration the `USER_PRIVATE`/`WORKSPACE_SHARED` branches of
-- BOTH policies required `humaux.user_id` to match the row's owner or an active membership,
-- which a tenant-only session can never satisfy. Every such memory — or every memory whose
-- PRIMARY evidence carries that visibility (remember.rs writes the request's visibility_class
-- into evidence_objects verbatim; 40 such memories exist in the dev fixture set alone) —
-- resolved to "no visible row", settled FAILED, and §15.7's contiguous-done-prefix rule then
-- froze `projection_highwater` for the whole tenant. Widening only memory_records (the first
-- draft of this migration) left the evidence half of the join in place: the fixture's evidence
-- was always TENANT_SHARED, so the tests were green while production would still freeze.
--
-- Join chain audited: `private.memory_evidence` policy is tenant-only via EXISTS(memory_records)
-- and `private.events` inherits via EXISTS(evidence_objects) — both become visible to the worker
-- once these two policies carry the role clause; no further table needs widening.
--
-- §6.1.2 enforces real per-query visibility downstream at Qdrant read time
-- (visibility_class/visibility_user_id/visibility_workspace_id payload fields +
-- `humaux_projection::dense::visibility_disjunction`), so gating this worker's own row reads by
-- user identity was redundant defense-in-depth, not the visibility boundary.
--
-- The `current_user = 'role_retrieval_worker'` disjunct is inserted INSIDE the existing
-- tenant-equality branch (never OR'd at the top level) so the worker stays hard tenant-scoped.
-- The §62 tenant clause keeps 0031's NULLIF form byte-for-byte (xtask rls-check pins it).
-- WITH CHECK is untouched on both tables — this worker never writes either — so the widening
-- is read-only in name and effect.
ALTER POLICY memory_records_tenant_and_visibility ON private.memory_records
USING (
  current_user = 'role_migration_owner'
  OR (
    tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
    AND (
      current_user = 'role_retrieval_worker'
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
-- evidence_objects: the legacy policy `evidence_objects_tenant_and_visibility` is frozen by the
-- W1 continuity census (xtask rls-check `check_w1_continuity_boundary` pins its qual/with_check
-- sha256 and the W1 ruling of 2026-08-31 rejects rewriting it), so the worker's read path is a
-- SEPARATE permissive SELECT policy scoped to the role. PostgreSQL ORs permissive policies, so
-- the worker sees every row of its tenant; every other role is unaffected. The census's policy
-- count on this table moves 5 -> 6 in lockstep (xtask/src/rls_check.rs, same commit).
CREATE POLICY evidence_objects_retrieval_worker_read ON private.evidence_objects
  AS PERMISSIVE FOR SELECT TO role_retrieval_worker
  USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);
GRANT SELECT ON private.memory_records TO role_retrieval_worker;
GRANT SELECT ON private.evidence_objects TO role_retrieval_worker;
