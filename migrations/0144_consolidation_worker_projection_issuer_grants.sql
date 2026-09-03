-- §15.1/§11.6: `role_consolidation_worker` is the writer (and therefore the §15.1 issuer, per
-- that section's "issuer = whoever produces the Evidence/derived row") of the private_memory
-- stream ticket + outbox row `consolidate_repo::publish_rollup` now emits in the same
-- transaction as its `private.memory_rollups` write. Before this migration it had zero grant on
-- `projection.stream_log` / `ops.outbox` (§6.2.2 listed neither cell — both fell back to the
-- role's own domain default, `—` for `projection.*`/`ops.*` beyond `SELECT`), so that INSERT
-- failed with `permission denied` the moment it ran. Same reasoning applies to
-- `projection.stream_checkpoints`: `role_consolidation_worker`'s domain default is
-- `projection.* = R` (SELECT only, §6.2.1), but the one existing issuer function
-- (`remember::issue_stream_log_row`, now `pub(crate)` and shared by this call site) always
-- does a bootstrap `INSERT ... ON CONFLICT DO NOTHING` followed by an `UPDATE
-- issued_highwater` — both fail at the grant check regardless of whether the conflict clause
-- would make the INSERT a no-op. Grants mirror `role_gateway`'s own column-scoped shape
-- exactly (`migrations/0011_roles_and_grants.sql` for stream_log/stream_checkpoints,
-- `migrations/0104_contribution_io.sql` for the outbox column narrowing) — never broadened to
-- unrestricted table-level `INSERT`.

-- role_consolidation_worker's schema list (migrations/0011_roles_and_grants.sql) is
-- ['control','private','coord','ops'] — no `projection` — same gap `role_private_worker` had
-- before that same migration's explicit `GRANT USAGE ON SCHEMA projection TO
-- role_private_worker` line for the identical column-limited-override reason.
GRANT USAGE ON SCHEMA projection TO role_consolidation_worker;

GRANT SELECT, INSERT ON projection.stream_log TO role_consolidation_worker;

-- `projection.*`'s domain default for this role is `—` (migrations/0011_roles_and_grants.sql
-- §6.2.1: consolidation only defaults to SELECT on control/private/coord/ops, never
-- projection) — an explicit table-level SELECT is therefore required here too, not just the
-- column-scoped write grants below.
GRANT SELECT ON projection.stream_checkpoints TO role_consolidation_worker;
GRANT INSERT (
  tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version
) ON projection.stream_checkpoints TO role_consolidation_worker;
GRANT UPDATE (issued_highwater) ON projection.stream_checkpoints TO role_consolidation_worker;

GRANT SELECT ON ops.outbox TO role_consolidation_worker;
GRANT INSERT (
  tenant_id, commit_seq, stream_seq, event_type, evidence_id
) ON ops.outbox TO role_consolidation_worker;

-- §15.1 `next_commit_seq` — the sole caller of `ops.commit_seq_seq`
-- (migrations/0043_commit_sequence.sql) — is now also called from `consolidate_repo::
-- publish_rollup`'s ticket issuance; only `role_gateway` held USAGE before this migration.
GRANT USAGE, SELECT ON SEQUENCE ops.commit_seq_seq TO role_consolidation_worker;
