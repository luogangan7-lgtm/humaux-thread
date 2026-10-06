-- §46 FORWARD_ONLY (card 37 S1; ADR-0064 D-J, D-K, D-O, section 10.3 as corrected by 10.11 C; E17: local_only).
-- The receipts that make "verified" and "restorable" facts the database derives, never a writer's claim:
--   ops.backup_sets            label identity across time: the manifest sha256 of a set's FIRST verification.
--   ops.backup_receipts        one append-only row per backup arm run (VERIFIED or FAILED) for the one local posix
--                              repository; VERIFIED is derived by CHECK backup_receipts_verified_derived (verify exit
--                              0 and the pulled-back manifest equal to the label's first-verified manifest, FK to
--                              ops.backup_sets) and shaped by CHECK backup_receipts_shape (a FAILED row names its
--                              failure; a VERIFIED row has label, manifest, verify exit, start and stop). A budget
--                              refusal is a FAILED row with a NULL label carrying the budget numbers (10.11 A/C).
--   ops.restore_witnesses      the drill's own witnesses A/B that define its target T (D-L, ruling E2).
--   ops.restore_drills         + the D-O receipt columns and repo_intact (10.11 H); succeeded is derived by CHECK
--                              restore_drills_succeeded_derived, so no writer can claim a drill it did not pass.
--   ops.wal_archive_failures   one row per observed WAL-archive incident (D-K latch, 10.11 D); survives the crash
--                              that resets pg_stat_archiver.
-- All five are cluster-level (no tenant_id, no RLS) and owned by role_migration_owner. Writer of all five: the
-- humaux-maintenance backup / restore arms and its DR_EVIDENCE task (card 37 S4-S6), as role_maintenance.
--
-- Grants (ADR-0064 E9): the 0011 ops default privileges (SELECT/INSERT/UPDATE to role_gateway and the private,
-- public and retrieval workers; SELECT to role_consolidation_worker and role_maintenance) are REVOKEd from the four
-- new tables, then role_maintenance INSERT, SELECT on each. ops.restore_drills: INSERT, UPDATE revoked from
-- role_gateway, role_private_worker, role_public_worker, role_retrieval_worker (no code writes it, F1); their and
-- role_consolidation_worker's SELECT stay; role_maintenance gains INSERT.
--
-- Precheck: ops.restore_drills has 0 rows (the derived-success CHECK is added to an empty table) — that is why no
-- backup is needed (backup_restore_requirement). Nothing here names an offsite copy (E17, gate c37_no_offsite_claim).
--
-- Locks: CREATE TABLE locks only the new tables; ALTER TABLE ops.restore_drills takes ACCESS EXCLUSIVE on a 0-row
-- table for the ADD COLUMNs and the CHECK (no reader or writer exists, F1). The whole file took 19.5 ms on a
-- throwaway migrated to 0233 (2026-10-06).

CREATE TABLE ops.backup_sets (
  backup_label      text        PRIMARY KEY CHECK (length(backup_label) > 0),
  manifest_sha256   bytea       NOT NULL CHECK (octet_length(manifest_sha256) = 32),
  first_verified_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT backup_sets_label_manifest_unique UNIQUE (backup_label, manifest_sha256)
);

CREATE TABLE ops.backup_receipts (
  receipt_id               uuid        PRIMARY KEY DEFAULT uuidv7(),
  backup_label             text,
  backup_type              text        NOT NULL CHECK (backup_type IN ('full', 'diff', 'incr')),
  backup_started_at        timestamptz,
  backup_stopped_at        timestamptz,
  manifest_sha256          bytea       CHECK (manifest_sha256 IS NULL OR octet_length(manifest_sha256) = 32),
  verify_exit              integer,
  verified_manifest_sha256 bytea,
  outcome                  text        NOT NULL CHECK (outcome IN ('VERIFIED', 'FAILED')),
  failure                  text,
  repo_bytes               bigint,
  repo_free_bytes          bigint,
  set_repo_bytes           bigint,
  repo_max_bytes           bigint,
  min_free_bytes           bigint,
  estimate_bytes           bigint,
  recorded_at              timestamptz NOT NULL DEFAULT clock_timestamp(),
  -- ADR-0064 D-J: the verified manifest is the one first verified under this label (MATCH SIMPLE: FAILED rows
  -- carry NULL and pass).
  CONSTRAINT backup_receipts_verified_set_fk FOREIGN KEY (backup_label, verified_manifest_sha256)
    REFERENCES ops.backup_sets (backup_label, manifest_sha256),
  CONSTRAINT backup_receipts_verified_derived CHECK (
    (outcome = 'VERIFIED') = (failure IS NULL AND verify_exit = 0 AND verified_manifest_sha256 IS NOT NULL
                              AND verified_manifest_sha256 = manifest_sha256)),
  -- 10.11 C: without this a VERIFIED row with a NULL manifest or a NULL verify_exit makes the derived CHECK NULL,
  -- and NULL passes.
  CONSTRAINT backup_receipts_shape CHECK (
    (outcome = 'FAILED' AND failure IS NOT NULL)
    OR (outcome = 'VERIFIED' AND backup_label IS NOT NULL AND manifest_sha256 IS NOT NULL
        AND verify_exit IS NOT NULL AND backup_started_at IS NOT NULL AND backup_stopped_at IS NOT NULL))
);

CREATE TABLE ops.restore_witnesses (
  witness_id uuid        PRIMARY KEY DEFAULT uuidv7(),
  drill_id   uuid        NOT NULL,
  kind       text        NOT NULL CHECK (kind IN ('A', 'B')),
  written_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT restore_witnesses_one_per_kind UNIQUE (drill_id, kind)
);

CREATE TABLE ops.wal_archive_failures (
  observed_at timestamptz PRIMARY KEY DEFAULT clock_timestamp()
);

ALTER TABLE ops.restore_drills
  ADD COLUMN backup_label text,
  ADD COLUMN backup_manifest_sha256 bytea,
  ADD COLUMN target_time timestamptz,
  ADD COLUMN started_at timestamptz,
  ADD COLUMN finished_at timestamptz,
  ADD COLUMN manifest_matches boolean,
  ADD COLUMN witness_a_present boolean,
  ADD COLUMN witness_b_absent boolean,
  ADD COLUMN server_version_matches boolean,
  ADD COLUMN migrations_drift integer,
  ADD COLUMN rls_unforced integer,
  ADD COLUMN isolation_violations integer,
  ADD COLUMN isolation_pairs integer,
  ADD COLUMN payload_digest_mismatches integer,
  ADD COLUMN provider_calls bigint,
  ADD COLUMN drill_archiver_attempts bigint,
  ADD COLUMN rebuild_equivalent boolean,
  ADD COLUMN rebuild_points bigint,
  ADD COLUMN legacy_points_without_vector bigint,
  ADD COLUMN unprojected_at_target bigint,
  ADD COLUMN restored_in_flight jsonb,
  ADD COLUMN residue integer,
  ADD COLUMN rto_seconds numeric,
  ADD COLUMN phase_seconds jsonb,
  ADD COLUMN failure text,
  ADD COLUMN repo_intact boolean,
  -- ADR-0064 D-O (finding 5) + 10.11 H: success is derived; a NULL check (not reached) can never read as passed.
  ADD CONSTRAINT restore_drills_succeeded_derived CHECK (
    succeeded = (
      COALESCE(manifest_matches AND witness_a_present AND witness_b_absent AND server_version_matches
               AND rebuild_equivalent AND repo_intact, false)
      AND COALESCE(migrations_drift = 0 AND rls_unforced = 0 AND isolation_violations = 0 AND isolation_pairs = 2
                   AND payload_digest_mismatches = 0 AND provider_calls = 0 AND drill_archiver_attempts = 0
                   AND legacy_points_without_vector = 0 AND residue = 0 AND rebuild_points IS NOT NULL, false)
      AND failure IS NULL));

ALTER TABLE ops.backup_sets OWNER TO role_migration_owner;
ALTER TABLE ops.backup_receipts OWNER TO role_migration_owner;
ALTER TABLE ops.restore_witnesses OWNER TO role_migration_owner;
ALTER TABLE ops.wal_archive_failures OWNER TO role_migration_owner;
REVOKE ALL ON ops.backup_sets, ops.backup_receipts, ops.restore_witnesses, ops.wal_archive_failures
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin, role_health_reader;
GRANT SELECT, INSERT ON ops.backup_sets, ops.backup_receipts, ops.restore_witnesses, ops.wal_archive_failures
  TO role_maintenance;
REVOKE INSERT, UPDATE ON ops.restore_drills
FROM role_gateway, role_private_worker, role_public_worker, role_retrieval_worker;
GRANT INSERT ON ops.restore_drills TO role_maintenance;

COMMENT ON TABLE ops.backup_sets IS
  'ADR-0064 D-J: the manifest sha256 of each backup set''s first verification (label identity across time). '
  'role_maintenance INSERT, SELECT.';
COMMENT ON TABLE ops.backup_receipts IS
  'ADR-0064 D-J / 10.11 C: append-only backup arm receipts for the local repository (local_only); VERIFIED derived '
  'by backup_receipts_verified_derived, shaped by backup_receipts_shape. role_maintenance INSERT, SELECT.';
COMMENT ON TABLE ops.restore_witnesses IS
  'ADR-0064 D-L/D-O: the restore drill''s witnesses A and B; T lies between them. role_maintenance INSERT, SELECT.';
COMMENT ON TABLE ops.wal_archive_failures IS
  'ADR-0064 D-K / 10.11 D: one row per observed WAL-archive incident; WalArchiveFailing stays latched until a later '
  'VERIFIED full. role_maintenance INSERT, SELECT.';
