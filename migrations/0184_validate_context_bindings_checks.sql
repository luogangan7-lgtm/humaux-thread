-- §46 forward-fix (FORWARD_ONLY, 0175 precedent; card 27, ADR-0052 D-H). Audit DM-7: the three
-- presence CHECKs on private.context_bindings were added NOT VALID, so the planner and every reader
-- still have to assume a legacy row may violate them. Dev (2026-09-29): 47 rows, 0 violators.
-- VALIDATE scans the table under SHARE UPDATE EXCLUSIVE and changes no row; the CHECK text is
-- unchanged. `SET NOT NULL` (the audit's second half) is not part of card 27.
ALTER TABLE private.context_bindings VALIDATE CONSTRAINT context_bindings_created_by_present;
ALTER TABLE private.context_bindings VALIDATE CONSTRAINT context_bindings_memory_id_present;
ALTER TABLE private.context_bindings VALIDATE CONSTRAINT context_bindings_scope_id_present;
