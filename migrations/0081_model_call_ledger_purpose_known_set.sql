-- Minor gate-hardening follow-up to 0080 (§20#G20-2/G80-39 audit finding, this task's
-- reserved 0080-0082 migration band): 0080 left `ops.model_call_ledger.purpose` free text
-- with no CHECK at all, deferring the full §19.1 closed purpose enum to Phase 7. With zero
-- constraint on the value, the sole e2e half of the G20-2/G80-39 gate — a literal
-- `purpose = 'query_rewrite'` string match — has no protection against a future writer
-- spelling the value `'queryRewrite'`, `'rewrite'`, or `'QUERY_REWRITE'`: the gate would then
-- stay green forever regardless of what the online recall path actually does, amplifying
-- rather than independently covering the gate's other known e2e gaps.
--
-- Cannot edit 0080 itself — it is already applied elsewhere in this migration lineage
-- (0083/0084 already build on top of it) — so this lands as a new EXPAND-only migration.
-- Closes the value set to what is known today (`'query_rewrite'`, the one literal
-- G20-2/G80-39's e2e half currently queries for) rather than inventing the rest of Phase 7's
-- full enum here; that later delivery widens this same CHECK the way 0061 incrementally
-- widened `control.reasoning_domain_grants.purposes`.

ALTER TABLE ops.model_call_ledger
  ADD CONSTRAINT model_call_ledger_purpose_known
    CHECK (purpose IS NULL OR purpose = ANY (ARRAY['query_rewrite']::text[]));

COMMENT ON COLUMN ops.model_call_ledger.purpose IS
  '§19.1 ModelCallLedger field table. Closed to the one value known today (''query_rewrite'', '
  'the literal G20-2/G80-39 e2e half (§20.0) queries for) — still NOT the full §19.1 closed '
  'purpose set, which is a Phase 7 delivery that widens this same CHECK, the pattern 0061 used '
  'for reasoning_domain_grants.purposes.';
