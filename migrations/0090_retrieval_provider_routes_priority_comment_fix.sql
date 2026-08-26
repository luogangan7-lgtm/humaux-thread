-- §19 code-review finding: 0088's `COMMENT ON COLUMN ... priority` states the tie-break
-- order is "priority DESC, route_id DESC" — full stop, no mention of tenant/region. But
-- `router.rs::ordering_key` actually sorts by `(tenant_id.is_some(), region.is_some(),
-- priority, route_id)`: tenant-specificity and region-specificity are the two coarsest keys,
-- unconditionally outranking `priority`. This table is migration/admin-authored only (no
-- runtime role can write it, 0088's own header), so the column comment is the operator's
-- primary reference for how `priority` behaves — as written it materially misrepresents the
-- real precedence and could lead an operator to configure a "high priority global override"
-- that structurally can never win against any tenant-specific row. Cannot edit 0088's
-- COMMENT in place (already applied, rule ③); COMMENT ON COLUMN is idempotent/replaces the
-- prior comment, so re-issuing it here corrects the record without touching 0088's DDL.
COMMENT ON COLUMN control.retrieval_provider_routes.priority IS
  'Higher value = more preferred, but NOT the primary sort key. router::ordering_key sorts '
  'by (tenant_id IS NOT NULL, region IS NOT NULL, priority, route_id) — a tenant-specific '
  'row always outranks a platform-wide (tenant_id NULL) row, and among rows tied on tenant '
  'specificity a region-specific row always outranks a region-agnostic one, regardless of '
  'priority. priority DESC only breaks ties among rows equally tenant/region-specific, and '
  'route_id DESC is the final deterministic tie-break after that. A "high priority global '
  'default" can never outrank any tenant-specific row on the same purpose/region.';
