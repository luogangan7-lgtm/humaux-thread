-- T4.8 follow-up (review finding, blocker, §6.2.1/§6.2.2/§48.2 表集合派生).
--
-- 0054's file header claimed "role_maintenance ... only holds SELECT on
-- ops.deletion_plan_steps [and control.deletion_requests]" and described the
-- SECURITY DEFINER functions as "a chokepoint, not a table grant" — true for
-- control.deletion_requests (whose §6.2.1 domain default for the `control` schema is
-- SELECT-only for every runtime role), but never verified against
-- ops.deletion_plan_steps's own actual grants. Verified against
-- information_schema.role_table_grants before this migration: because
-- ops.deletion_plan_steps is a table in the `ops` schema and was never added to
-- §6.2.2's named-table matrix, it fell back to §6.2.1's *domain default* for `ops`,
-- which is SELECT+INSERT+UPDATE for role_gateway/role_private_worker/
-- role_public_worker/role_retrieval_worker (0011) — the request-path role
-- (role_gateway) could INSERT or UPDATE a QDRANT_POINTS/OBJECT_BYTES step row for any
-- deletion_request_id in its own tenant, forging a completed purge and flipping the
-- §41.2 `tombstoned_unpurged_over_sla` gauge green without anything ever having been
-- purged.
--
-- Fix: a §6.2.2 named-table override — REVOKE the domain-default INSERT/UPDATE from
-- the four runtime roles that never needed it (writes go through
-- `ops.record_deletion_plan_step`, SECURITY DEFINER, owned by role_migration_owner —
-- it needs no caller-side table grant). role_consolidation_worker's domain-default
-- SELECT and role_maintenance's domain-default SELECT are both correct as-is and are
-- reaffirmed (not just left alone) so the named-table override is a complete,
-- self-contained cell set, not a partial diff off a default this table no longer
-- follows. role_maintenance additionally gets a direct INSERT grant per this
-- finding's own remediation, matching the sibling `ops.jobs`/`ops.outbox` pattern of
-- giving the maintenance role real table access on its own audit tables rather than
-- only function EXECUTE.
--
-- This is a named-table override, not a widening: it strictly narrows the four
-- runtime roles' access (SELECT-only, was SELECT+INSERT+UPDATE) and leaves
-- role_maintenance/role_consolidation_worker exactly where they already were plus one
-- explicit INSERT. xtask/src/rls_check.rs's MATRIX and
-- docs/architecture/Baseline_2.8.md §6.2.2 are updated in the same review pass so
-- `cargo xtask rls-check`'s "授权逐条相等"/"域默认授权"/"表集合派生" checks stay
-- accurate against this new reality instead of flagging it as drift.

REVOKE INSERT, UPDATE ON ops.deletion_plan_steps
  FROM role_gateway, role_private_worker, role_public_worker, role_retrieval_worker;

-- Reaffirm explicitly (idempotent no-ops if already exactly this) so this migration is
-- a complete override, not a partial diff off a domain default this table no longer
-- follows once it is named in §6.2.2.
GRANT SELECT ON ops.deletion_plan_steps
  TO role_gateway, role_private_worker, role_public_worker, role_retrieval_worker,
     role_consolidation_worker, role_maintenance;
GRANT INSERT ON ops.deletion_plan_steps TO role_maintenance;
