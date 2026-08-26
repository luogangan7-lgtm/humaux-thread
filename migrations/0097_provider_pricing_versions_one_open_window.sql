-- §19 Pricing Registry follow-up (T7.4 remediation, code-review finding #2). 0095 documented
-- "价格更新永远是新增一行 + 关闭旧行的 effective_to" as an application-level discipline but
-- enforced nothing — the ordinary update path (insert a corrective/new row without closing the
-- prior one) leaves two-or-more rows open-ended (`effective_to IS NULL`) for the same
-- (provider_id, model_id, region). Verified live: doing exactly that leaves both rows covering
-- every `at` on/after the newer `effective_from`, and
-- `humaux_retrieval_provider::pricing::resolve` (called with rows the caller ordered
-- `effective_from DESC`) then silently picks the newer price for a HISTORICAL call whose
-- `called_at` is after the new `effective_from` — recomputing an already-billed call at the
-- new price, which is exactly what §19's "历史调用永远按当时 pricing snapshot 归因" forbids.
--
-- Cannot edit 0095 in place (already applied, rule ③) — this is the same
-- EXPAND_CONTRACT-style follow-on 0093 used to backfill a uniqueness constraint 0091/0092
-- omitted (`retrieval_provider_admission_limits_active_uniq`), same "active row" = `effective_to
-- IS NULL` convention. Loud stand-down at write time: any writer that tries to insert a second
-- open-ended row for the same key now gets a unique-violation instead of silently creating an
-- ambiguous overlap — the writer is forced to close the prior row's `effective_to` first, which
-- is the actual invariant 0095's comment already claimed but never enforced.
--
-- No `btree_gist`/range-exclusion needed here (unlike a general overlap-exclusion constraint):
-- this only needs to rule out *two simultaneously-open* windows, which a plain partial unique
-- index over the fixed sentinel `effective_to IS NULL` already does in one line. ponytail:
-- backdated *closed* windows (two historical rows whose finite ranges overlap) are still
-- unenforced — add a `btree_gist` EXCLUDE over `tstzrange(effective_from, effective_to)` if a
-- writer path that can backdate a close ever exists; today's only writer (migration-time admin
-- insert) only ever opens a new row and closes the prior one going forward.
CREATE UNIQUE INDEX provider_pricing_versions_one_open_window
  ON control.provider_pricing_versions (provider_id, model_id, region)
  WHERE effective_to IS NULL;
