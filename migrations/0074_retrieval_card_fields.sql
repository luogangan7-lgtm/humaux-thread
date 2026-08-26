-- §18.2 RetrievalCard structural fields + §18.4's single frozen version axis
-- (`card_builder_version`; `card_template_hash` is that version's own template content
-- fingerprint, not a second axis) — ALTER onto the P1 skeleton
-- (migrations/0007_projection.sql created `projection.retrieval_cards` with only
-- card_id/tenant_id/memory_id/created_at).
--
-- projection.retrieval_cards has zero rows in every environment this migration runs against
-- (verified live: `select count(*) from projection.retrieval_cards` = 0 — the P1 skeleton has
-- no writer yet, `projection::card::build_card`, T5.7, is this task's own new code and still
-- IO-free). Same reasoning 0058_memory_rollups_authority_visibility.sql documents: every new
-- NOT NULL column below carries a throwaway DEFAULT only so `ADD COLUMN ... NOT NULL` succeeds
-- uniformly regardless of row count (§46 discipline: do not special-case "table happens to be
-- empty today"), then the DEFAULT is dropped immediately so a future INSERT that forgets a
-- column fails loudly, not silently.
--
-- Row-level RLS is untouched by this migration: `retrieval_cards` already carries a bare
-- `tenant_id` column, so 0012_rls.sql's blanket DO-loop already gave it ENABLE + FORCE ROW
-- LEVEL SECURITY plus a `retrieval_cards_tenant_isolation` policy, and 0031's textual rewrite
-- already NULLIF-wrapped that policy's `current_setting` read (verified live: `\d
-- projection.retrieval_cards` shows `USING ((tenant_id = (NULLIF(current_setting(...), ''))::
-- uuid))`). New columns land under an existing table-level policy that already covers the
-- whole row, not a new one. Grants are the same story: `retrieval_cards` is not one of
-- 0011_roles_and_grants.sql's 14 §6.2.2-named tables, so it only ever carried the §6.2.1
-- projection-schema domain default (`GRANT ... ON ALL TABLES IN SCHEMA projection ...`) —
-- table-level grants cover new columns automatically, no GRANT statement is needed here.

ALTER TABLE projection.retrieval_cards
  ADD COLUMN schema_version       int         NOT NULL DEFAULT 0,
  ADD COLUMN card_builder_version text        NOT NULL DEFAULT '',
  ADD COLUMN card_template_hash   text        NOT NULL DEFAULT '',
  ADD COLUMN memory_type          text        NOT NULL DEFAULT '',
  ADD COLUMN data_class           text        NOT NULL DEFAULT '',
  ADD COLUMN egress_disposition   text        NOT NULL DEFAULT '',
  ADD COLUMN workspace_id         uuid        REFERENCES control.workspaces(workspace_id),
  ADD COLUMN topic                text,
  ADD COLUMN effective_from       timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN title                text        NOT NULL DEFAULT '',
  ADD COLUMN key_claim            text        NOT NULL DEFAULT '',
  -- §18.2 "entities[]" — a genuinely-empty list is not a missing field (card::CardInput's own
  -- doc: most memory types carry none), so this DEFAULT stays after the dance below, unlike
  -- every placeholder-only DEFAULT ('') on this table.
  ADD COLUMN entities             jsonb       NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN evidence_excerpt     text        NOT NULL DEFAULT '',
  -- §18.3 "被索引文本" — projection::card::assemble()'s output; NOT NULL, checked below to
  -- always contain `title` in full (the DB-side mirror of card.rs's write-path
  -- `debug_assert!`, §18.3).
  --
  -- §18.4's `card_status` concept is deliberately NOT a new column here: migration
  -- 0063_retrieval_cards_projection_record_fields.sql (T5.1, same table, applied earlier in
  -- file order) already added `status text ... CHECK (status IN ('complete', 'partial',
  -- 'unbuildable'))` for exactly this field — verbatim the lowercase three-literal spelling
  -- §18.4's own prose uses. Adding a second `card_status` column here would duplicate that
  -- concept under a different name/casing on the same row; `projection::card::CardStatus::
  -- as_str()` (card.rs) is written to emit that same lowercase spelling so a future writer
  -- binds it straight into 0063's `status` column with no translation layer.
  ADD COLUMN card_text            text        NOT NULL DEFAULT '';

ALTER TABLE projection.retrieval_cards
  ALTER COLUMN schema_version       DROP DEFAULT,
  ALTER COLUMN card_builder_version DROP DEFAULT,
  ALTER COLUMN card_template_hash   DROP DEFAULT,
  ALTER COLUMN memory_type          DROP DEFAULT,
  ALTER COLUMN data_class           DROP DEFAULT,
  ALTER COLUMN egress_disposition   DROP DEFAULT,
  ALTER COLUMN effective_from       DROP DEFAULT,
  ALTER COLUMN title                DROP DEFAULT,
  ALTER COLUMN key_claim            DROP DEFAULT,
  ALTER COLUMN evidence_excerpt     DROP DEFAULT,
  ALTER COLUMN card_text            DROP DEFAULT;

-- §18.2 "data_class=SECRET_MATERIAL 不生成卡" made structural, not just a builder-side
-- discipline: this CHECK is the DB-side half of `projection::card::build_card`'s early
-- `ExcludedSecret` return — the four literals quoted verbatim from
-- `domain::dataclass::DataClass::as_str` (migrations/0004_private_evidence_memory.sql already
-- uses the same five-literal set on `private.evidence_objects.data_class`; this table omits
-- the fifth on purpose).
ALTER TABLE projection.retrieval_cards
  ADD CONSTRAINT retrieval_cards_data_class_known_not_secret CHECK (
    data_class IN ('PUBLIC', 'INTERNAL', 'PRIVATE', 'SENSITIVE')
  );

-- §18.2's three frozen egress_disposition literals (projection::card::EgressDisposition).
ALTER TABLE projection.retrieval_cards
  ADD CONSTRAINT retrieval_cards_egress_disposition_known CHECK (
    egress_disposition IN ('ALLOWED', 'POLICY_GATED', 'FORBIDDEN')
  );

-- §8.5's 12-variant closed set (projection::card::memory_type_wire's exhaustive match).
ALTER TABLE projection.retrieval_cards
  ADD CONSTRAINT retrieval_cards_memory_type_known CHECK (
    memory_type IN (
      'FACT', 'PREFERENCE', 'DECISION', 'REJECTION', 'STATE', 'ISSUE', 'LESSON',
      'CONSTRAINT', 'PROCEDURE', 'OUTCOME', 'REFERENCE', 'NOTE'
    )
  );

-- §18.3 verbatim, DB-side: "被索引文本" must always contain `title` in full. `position(...)
-- > 0` on an empty `title` is `position('' in card_text)` = 1 (SQL defines the empty needle as
-- found at position 1 of any haystack, including an empty one) — never a false failure on an
-- empty title, only a real guard against a template edit that drops `title` from `card_text`.
ALTER TABLE projection.retrieval_cards
  ADD CONSTRAINT retrieval_cards_card_text_contains_title CHECK (
    position(title in card_text) > 0
  );
