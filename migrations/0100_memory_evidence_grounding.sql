-- §8.8 Grounding Validity / §11.10 Grounding Revalidation Pipeline — the two per-edge columns
-- the deterministic detector stage reads ("compare recorded version token -> reverse lookup
-- private.memory_evidence", §11.10). They go on this table because §8.8 says so in its own
-- words: Humaux "不新增 Claim 表，直接作用于 MemoryRecord + memory_evidence + EvidenceObject"
-- — a grounding edge IS a private.memory_evidence row (§8.6 / 0004③, the sole Memory<->
-- Evidence relation), so a sidecar table would be a second place the same edge lives.
--
-- Closed-set shape: `text` + a named CHECK, identical to every other closed set in this
-- migrations/ tree (this table's own `role`, evidence_objects' `evidence_kind`/`data_class`/
-- `origin_class`, memory_records' `memory_type`/`authority_class`/`status`). No native
-- `CREATE TYPE ... AS ENUM` exists anywhere in this repo, and the §78.2 DB-vs-Rust contract
-- tests reconcile the Rust enum against `pg_get_constraintdef` text (crates/adapters/tests/
-- disclosure_ledger.rs::wire_strings_match_live_db_check_constraints is the reference
-- implementation) — a pg_enum-backed column would be invisible to that mechanism, and a PG
-- enum cannot drop a label, which would make every future narrowing FORWARD_ONLY.
--
-- Wire values are SCREAMING_SNAKE, the form §8.8's own state definitions and §11.10's census
-- sentence ("重新 resolve 所有 `LIVE` grounding edge") use. The Rust side is
-- `domain::grounding::GroundingMode::{Live,Snapshot,Immutable}`; the two sets are reconciled
-- by crates/adapters/tests/memory_evidence_grounding.rs (§78.2), not by convention.

-- DEFAULT 'LIVE' — the only value that does not manufacture a verdict the row cannot support.
-- Rows predating this column never recorded what they were grounded against, so
-- `recorded_version` backfills to NULL, and §8.8's derivation splits on mode:
--   LIVE + recorded_version NULL -> RECHECK_REQUIRED (cannot prove "还是同一版" ⇒ no longer
--                                   entitled to be assumed current truth)
--   SNAPSHOT / IMMUTABLE         -> excluded from the derivation entirely ⇒ vacuously CURRENT
-- Defaulting to SNAPSHOT or IMMUTABLE would therefore hand every legacy edge a free CURRENT
-- verdict — silently granting "继续安全假设为当前真值" to rows with zero evidence for it,
-- which is exactly what §8.8's "派生状态，不新增可手改 stale flag" exists to prevent. LIVE +
-- NULL routes them into the RECHECK_REQUIRED branch instead: a grounding debt that surfaces
-- and gets paid, not a claim nobody checked. The same reasoning covers writers that omit the
-- column (today that is every writer — no INSERT in the tree names it yet): an unclassified
-- new edge starts as debt, never as CURRENT.
--
-- Fast default (PG11+): a constant DEFAULT on ADD COLUMN backfills without rewriting the
-- table, so this statement is O(1) under its ACCESS EXCLUSIVE lock even though the table is
-- populated (1537 rows in the dev DB at authoring time, and one row per Memory<->Evidence
-- link in production).
ALTER TABLE private.memory_evidence
  ADD COLUMN grounding_mode   text NOT NULL DEFAULT 'LIVE',
  ADD COLUMN recorded_version text;

-- NOT VALID + separate VALIDATE CONSTRAINT rather than an inline CHECK on the ADD COLUMN
-- above: the fast default makes every existing row provably 'LIVE', but PostgreSQL does not
-- know that and would scan the whole table under the ADD COLUMN's ACCESS EXCLUSIVE lock. This
-- is the lock-class split 0099 established for the same situation — ADD CONSTRAINT NOT VALID
-- is O(1) and already enforces the check for all new writes; VALIDATE then scans under a
-- lesser lock that does not block reads or writes.
ALTER TABLE private.memory_evidence
  ADD CONSTRAINT memory_evidence_grounding_mode_check
    CHECK (grounding_mode IN ('LIVE', 'SNAPSHOT', 'IMMUTABLE')) NOT VALID;
ALTER TABLE private.memory_evidence
  VALIDATE CONSTRAINT memory_evidence_grounding_mode_check;

COMMENT ON COLUMN private.memory_evidence.grounding_mode IS
  '§8.8 GroundingMode. Only LIVE participates in derive_grounding_state — a SNAPSHOT/IMMUTABLE edge''s source may move freely without making the statement stale.';
COMMENT ON COLUMN private.memory_evidence.recorded_version IS
  '§8.8 GroundingVersionToken: resolver-owned opaque string, equality comparison only — never parsed, ordered, or read as "newer/older". NULL = never recorded; a LIVE edge with NULL derives RECHECK_REQUIRED.';
