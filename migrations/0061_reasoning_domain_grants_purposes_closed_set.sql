-- Fixer-review follow-up to 0049_reasoning_domain_columns: closes the empty-purposes gap on
-- control.reasoning_domain_grants without editing the already-applied 0049 (same pattern
-- 0057_data_disclosures_truncate_guard used for 0047).
--
-- §11.2.1: "Service Credential ... 还必须带 reasoning_domain_grant + workspace scope + purpose
-- + expiry" — 0049 already made this literal for `expires_at` (NOT NULL, no default: "a grant
-- without an expiry is not a valid grant"). The identical reasoning applies to `purposes`:
-- 0049 left it `NOT NULL DEFAULT '{}'`, so a grant with a *present but empty* purposes array —
-- or one relying on the default — was (and, until this migration, still is) a valid row despite
-- authorizing nothing. There was also no closed-set CHECK on the array's contents, unlike
-- 0048's `capabilities <@ ARRAY[...]` sibling. The closed set below is the same
-- USER_REASONING/RETRIEVAL_EMBEDDING/RETRIEVAL_RERANK wire form `disclosure.rs::
-- purpose_as_db_str` already uses for `PrivateDataPurpose` — no third spelling introduced.
--
-- Table has zero rows anywhere this migration has run (0049's own comment: "no INSERT into
-- control.reasoning_domain_grants exists anywhere in the codebase yet"), so both constraints
-- add cleanly with no backfill.

ALTER TABLE control.reasoning_domain_grants
  ALTER COLUMN purposes DROP DEFAULT;

-- `array_length(purposes, 1) > 0` would NOT reject `'{}'` here: Postgres's `array_length`
-- returns NULL (not 0) for an empty array, and `NULL > 0` is NULL, which a CHECK constraint
-- treats as satisfied — verified live against this exact column. `cardinality()` returns a
-- real `0` for an empty array, so this is the form that actually rejects it.
ALTER TABLE control.reasoning_domain_grants
  ADD CONSTRAINT reasoning_domain_grants_purposes_nonempty
    CHECK (cardinality(purposes) > 0);

ALTER TABLE control.reasoning_domain_grants
  ADD CONSTRAINT reasoning_domain_grants_purposes_known
    CHECK (purposes <@ ARRAY['USER_REASONING', 'RETRIEVAL_EMBEDDING', 'RETRIEVAL_RERANK']::text[]);

COMMENT ON COLUMN control.reasoning_domain_grants.purposes IS
  '§11.2.1: mandatory, non-empty, closed set matching humaux_domain::egress::PrivateDataPurpose '
  '(wire form: disclosure.rs::purpose_as_db_str) — "a grant without a purpose is not a valid '
  'grant", same reasoning 0049 already applied to expires_at. No DEFAULT: a caller must supply '
  'it explicitly.';
