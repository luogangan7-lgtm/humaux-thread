-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0193). ADR-0058 D-P (card 32 scope 6,
-- ADR-0030 D-C): the distill hop may INFER an affect for a user-origin Evidence, so an affect row
-- now says who asserted it.
--
--   * origin = 'EXPLICIT' for every row a principal declared (memory.annotate_affect,
--     memory.correct, the 0157 evidence_affects copy) — the default, so every existing row and
--     every existing writer keeps its meaning without a code change;
--   * origin = 'DISTILL' for a row the private worker inferred (adapters::affect_repo::
--     insert_inferred_in_txn, the only writer of that value). Its confidence is capped at 5000 bp
--     by memory_affects_inferred_confidence_ceiling — a schema invariant pinned to
--     humaux_domain::affect::INFERRED_CONFIDENCE_CEILING_BP by contract test, not a tunable.
--
-- "Explicit remains authoritative" is enforced at read time by the one affect read
-- (affect_repo::AFFECTS_FOR_MEMORIES_SQL drops a memory's DISTILL rows when it has an EXPLICIT
-- row), not here. No grant change: role_private_worker already holds INSERT (0156) and every
-- reader holds table-level SELECT, which covers the new column.
--
-- Locks: one ACCESS EXCLUSIVE on private.memory_affects for a fast-default ADD COLUMN plus the
-- verification scan of the two CHECKs (dev: tens of rows, no live writer); no rewrite. The
-- BEFORE UPDATE immutability trigger does not fire (no UPDATE).
ALTER TABLE private.memory_affects
    ADD COLUMN origin text NOT NULL DEFAULT 'EXPLICIT',
    ADD CONSTRAINT memory_affects_origin_check CHECK (origin IN ('EXPLICIT', 'DISTILL')),
    ADD CONSTRAINT memory_affects_inferred_confidence_ceiling
        CHECK (origin = 'EXPLICIT' OR confidence_bp <= 5000);

COMMENT ON COLUMN private.memory_affects.origin IS
    'ADR-0058 D-P: EXPLICIT (declared by a principal) | DISTILL (inferred by the private worker; confidence_bp <= 5000; shadowed by any EXPLICIT row of the same memory in affect_repo::AFFECTS_FOR_MEMORIES_SQL).';
