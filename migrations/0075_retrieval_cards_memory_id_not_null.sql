-- §18.2 field table (`memory_id`) / §18.4 "projection.retrieval_cards 缺行必须计入 §15
-- processing gap" / §22.1 coverage — `memory_id` came from the 0007 P1 skeleton and was never
-- tightened by 0063 or 0074 (both landed nullable-by-omission alongside it). §18.2 lists
-- `memory_id` as a card field, and both §18.4's "缺行" detection and §22.1's coverage query
-- LEFT JOIN `private.memory_records` on `memory_id` — a NULL there is a card row that
-- structurally cannot be matched back to the memory it was built from: it silently reads as a
-- missing card for that memory (the exact §18.4 failure mode) *and* as an untraceable row no
-- coverage query can attribute. 0074 already ran the identical NOT NULL / DROP DEFAULT dance on
-- eleven sibling columns of this same table and is applied — additive, not editable — so this
-- one-column gap is closed here instead (T5.8 review, major finding).
--
-- `memory_id` has no DEFAULT to drop (unlike 0074's columns): the table has zero rows in every
-- environment this migration runs against (verified live: `select count(*) from
-- projection.retrieval_cards` = 0 — same "P1 skeleton has no writer yet" fact 0074's own
-- comment documents), so `SET NOT NULL` needs no backfill and no throwaway default at all.

ALTER TABLE projection.retrieval_cards
  ALTER COLUMN memory_id SET NOT NULL;

COMMENT ON COLUMN projection.retrieval_cards.memory_id IS
  '§18.2: the memory this card was built from. NOT NULL (0075) — a card row that cannot be '
  'matched back to its memory is unusable by both §18.4''s missing-row detection and §22.1''s '
  'coverage query, both of which LEFT JOIN private.memory_records on this column.';
