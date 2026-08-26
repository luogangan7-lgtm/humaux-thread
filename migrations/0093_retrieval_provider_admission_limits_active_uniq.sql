-- §19 "Global Provider Budget -> Region Budget -> Tenant Budget -> Purpose Budget": nothing in
-- 0091/0092 stops two overlapping *active* rows for the same tier key
-- (provider_id, region, tenant_id, purpose) from coexisting — only a plain btree lookup index
-- exists, no uniqueness or exclusion constraint over the `[effective_from, effective_to)`
-- range. Two active rows for the same key would fold a nondeterministic TPM/RPM ceiling into
-- the corresponding `TierBudget` once a loader reads this table, the same way
-- `control.retrieval_provider_routes` (0088) needed its `priority` column precisely to make
-- its own multi-row lookup deterministic — the difference here is this table's rows are meant
-- to be non-overlapping ceilings, not ranked candidates, so uniqueness (not a priority column)
-- is the right fix.
--
-- "Active" = the currently-in-force row for a key = `effective_to IS NULL` (the same "open
-- window" convention 0091/0092 and 0088 already use for a row with no expiry yet). A
-- superseded row (`effective_to` set) is historical and may legitimately share a key with the
-- row that replaced it.
--
-- `NULLS NOT DISTINCT` (PostgreSQL 15+, this repo's baseline is 18.6, §5) makes the two
-- platform-wide-config NULLs — `region` (Global tier) and `tenant_id`/`purpose` (tiers that
-- do not apply) — collide with each other for uniqueness purposes, same as any other value
-- would; without it, standard SQL NULL semantics would let unlimited duplicate Global rows
-- through, since each NULL region would compare distinct from every other.

CREATE UNIQUE INDEX retrieval_provider_admission_limits_active_uniq
  ON control.retrieval_provider_admission_limits (provider_id, region, tenant_id, purpose)
  NULLS NOT DISTINCT
  WHERE effective_to IS NULL;
