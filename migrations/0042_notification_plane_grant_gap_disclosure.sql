-- §74.7 Notification Plane grant-gap correction — fixes a review blocker on
-- 0034_notification_plane.sql (already applied, immutable; addressed here by a new
-- migration, not an edit).
--
-- 0034's header comment asserted "§6.2.1 domain-default GRANTs apply automatically ...
-- no explicit GRANT statement belongs in this file" and cited ops.column_vitality as
-- precedent — a table in the `ops.*` schema, whose §6.2.1 domain default differs from
-- `control.*`. Live-verified against information_schema.role_table_grants: every one of
-- the five runtime roles (§6.2.0: role_gateway / role_private_worker /
-- role_consolidation_worker / role_public_worker / role_retrieval_worker) holds
-- SELECT-only on control.notifications and control.notification_preferences — §6.2.1's
-- control.* domain default, no exception for any role. No runtime role can INSERT or
-- UPDATE either table today; 0034's "already covered" claim was wrong.
--
-- This migration does not invent a write-grant. §6.2.2 requires a named-table row before
-- any role gets write access to a control.* table (0011's own header comment: the
-- xtask/src/rls_check.rs MATRIX constant "is the live CI comparator this migration must
-- satisfy" for any such row), and picking which role owns writing notifications /
-- preferences is an architecture decision this spec has not frozen — the same position
-- §6.2.2 states outright for the sibling gap on control.quota_windows ("本表对这张表不给
-- 任何 INSERT：这是显式空缺，不是遗漏"). Assigning an owner role unilaterally here, without
-- a matching xtask MATRIX row and §6.2.2 doc row landing in the same change, would either
-- desync the live grant from the CI comparator or require an architecture-level spec edit
-- + ADR that is out of this fix's file scope. What belongs here is retracting the false
-- "already covered" claim at the point future engineers will actually check it — the table
-- comment — so the gap is an explicit, discoverable, tracked open item (matching the
-- quota_windows precedent) instead of a silent false assertion.

COMMENT ON TABLE control.notifications IS
  '§74.7: platform-authoritative user notification record; category/severity closed sets '
  'mirror application::notify::NotificationCategory/Severity verbatim (§78.2 contract test). '
  'KNOWN GAP (tracked, not silently covered — see 0042_notification_plane_grant_gap_disclosure): '
  '§6.2.1 domain default gives every runtime role SELECT-only on control.*, verified live via '
  'information_schema.role_table_grants — no runtime role can INSERT or UPDATE this table. '
  'Closing it needs a §6.2.2 named-table row (picking the writer role) plus a matching '
  'xtask/src/rls_check.rs MATRIX entry, same shape as the acknowledged control.quota_windows gap.';

COMMENT ON TABLE control.notification_preferences IS
  '§74.7: per-user per-category channel opt-in. §74.7 principle "安全关键通知可忽略用户营销'
  '偏好" is enforced Rust-side (application::notify::resolve_channels), not by a DB CHECK — '
  'a SECURITY-severity notification always writes email=true (and in_app=true) to '
  'ops.notification_deliveries regardless of what this table says for that (user_id, category). '
  'KNOWN GAP (tracked, not silently covered — see 0042_notification_plane_grant_gap_disclosure): '
  'same §6.2.1 SELECT-only default as control.notifications — no runtime role can write a '
  'user''s own preference row yet.';
