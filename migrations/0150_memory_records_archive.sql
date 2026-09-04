-- §36 (governance op set) / Q3 ARCHIVE ruling / ADR-0024: `memory.archive` is the one
-- governance verb with no landing place in the data model — 'archived' is absent from the
-- private.memory_records status CHECK (0004:131, active/superseded/revoked/expired only) and
-- from humaux_domain::authority::AuthorityStatus. The Q3 decision (recorded in ADR-0024) is a
-- separate `archived_at timestamptz` column, NOT a fifth AuthorityStatus: archive is
-- orthogonal to adjudication (an archived memory keeps whatever status it had — usually
-- 'active') and G59-4's frozen `(status='superseded') = (superseded_by IS NOT NULL)` pairing
-- stays byte-for-byte untouched. "Stop showing me this without destroying it" is a visibility
-- fact, not an authority transition.
--
-- Read semantics (ADR-0024 D-C): recall.search / context.assemble exclude `archived_at IS NOT
-- NULL` rows at the PG hydrate step; memory.get still returns the row (with `archived:true`);
-- memory.enumerate excludes archived by default. The partial index below is the one recall/
-- context/enumerate actually probe (they all carry `archived_at IS NULL`), so it stays small
-- and does not bloat as rows are archived.
--
-- The write side (ADR-0024 D-B) reuses card 3's lifecycle log verbatim: archive appends an
-- ARCHIVE event (reason USER_ARCHIVE, no undo_deadline — unarchivable any time via
-- memory.unarchive, never via memory.restore) and stamps `archived_at`; unarchive appends a
-- RESTORE event naming the ARCHIVE it reverses and clears `archived_at`. The gateway writes
-- `archived_at` in the same UPDATE that stamps `lifecycle_head_event_id`, so this migration
-- grants it exactly one more column-level UPDATE — never table-level (§6.2.2). No new table,
-- no data touched; §46 forward-fix, next free number after 0149.

ALTER TABLE private.memory_records ADD COLUMN archived_at timestamptz;
COMMENT ON COLUMN private.memory_records.archived_at IS
  'Q3/ADR-0024: NULL = live; non-NULL = archived (hidden from recall/context/enumerate, still '
  'returned by memory.get). NOT a fifth AuthorityStatus — orthogonal to status/G59-4. Set by '
  'memory.archive, cleared by memory.unarchive, both via the card 3 lifecycle log.';

-- Q3: partial index WHERE archived_at IS NULL — the predicate every non-get read carries.
CREATE INDEX idx_memory_records_live
  ON private.memory_records (tenant_id)
  WHERE archived_at IS NULL;

-- §6.2.2: role_gateway already holds column-level UPDATE on
-- (status, superseded_by, superseded_at, lifecycle_head_event_id) from 0148/0149; archive adds
-- exactly `archived_at`. Never widened to table-level UPDATE (matrix row in xtask/rls_check.rs
-- pins the exact column set).
GRANT UPDATE (archived_at) ON private.memory_records TO role_gateway;
