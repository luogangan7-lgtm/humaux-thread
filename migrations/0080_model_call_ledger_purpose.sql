-- §19.1 ModelCallLedger field table names `purpose` among the fields every external call
-- record carries; 0008's skeleton `ops.model_call_ledger` never added it. T6.2 needs just
-- this one field now — not the full §19.1 field list (request_id/model/tokens/cost/... is a
-- Phase 7 Managed Retrieval Provider Plane deliverable, out of this task's scope) — to give
-- G20-2/G80-39's e2e half something to query: "does ModelCallLedger ever carry
-- purpose='query_rewrite' for an online recall/context/continuity call" (§20.0).
--
-- No CHECK constraint on the value set yet: the closed purpose enum belongs to that same
-- later Phase 7 delivery — `provider`, this table's sibling skeleton column (0008), already
-- established the "plain text until the real type lands" precedent this column follows.

ALTER TABLE ops.model_call_ledger
  ADD COLUMN purpose text;

COMMENT ON COLUMN ops.model_call_ledger.purpose IS
  '§19.1 ModelCallLedger field table. No CHECK yet (§19.1''s full closed purpose set is a '
  'Phase 7 delivery) — G20-2/G80-39''s e2e check (§20.0) only needs to detect the single '
  'forbidden value ''query_rewrite'' appearing on an online recall/context/continuity call.';
