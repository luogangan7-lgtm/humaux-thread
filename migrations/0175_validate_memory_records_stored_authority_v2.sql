-- 0175 — VALIDATE the stored-authority CHECK that 0173 added NOT VALID.
--
-- 0173 (ADR-0046) added `memory_records_stored_authority_v2_check`
-- (`authority_class <> 'ExplicitTaskContext'`) NOT VALID because the shared dev database held
-- four legacy fixture rows (`{"fixture": "operation receipt scoped context"}`, tenants
-- d0a7e258… and 5c853c6f…) that carried the class the store gate no longer accepts. They were
-- test residue, never user memories; delivery report §7.3 listed them and the user approved
-- their disposition on 2026-09-26 (backed up, then deleted with their memory_evidence and
-- context_bindings rows). This migration turns the CHECK's proof on.
--
-- §46 forward-fix (FORWARD_ONLY): VALIDATE scans once, changes no schema and no rows; a no-op
-- where the CHECK is already valid. The manifest's precheck refuses to run while an offending
-- row exists — a migration must never mass-downgrade or delete authority rows (ADR-0046 ruling
-- §五.3).
ALTER TABLE private.memory_records
  VALIDATE CONSTRAINT memory_records_stored_authority_v2_check;
