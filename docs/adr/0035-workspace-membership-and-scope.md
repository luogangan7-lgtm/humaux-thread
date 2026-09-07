# ADR-0035: Workspace-level membership and multi-workspace AuthorizationScope

Status: Accepted (2026-09-07, card 13)
Supersedes: the ADR-0034 draft (tenant-level "member = every workspace of the tenant" derivation).

## Context

§6.1.1 already requires that `WORKSPACE_SHARED(W)` be readable **iff** the reader holds an ACTIVE
Tenant Membership **AND** an ACTIVE `WorkspaceMembership(T, W, U)` **AND** ordinary RBAC. The
implementation did not honour it: there was no workspace-level membership table, and 0012's (later
0153's) `WORKSPACE_SHARED` RLS arm read "an ACTIVE **tenant** membership exists", which grants every
tenant member every workspace's `WORKSPACE_SHARED` data. The ADR-0034 draft codified that below-Canon
behaviour (`allowed_workspace_ids` = all workspaces of the tenant where the user has an ACTIVE tenant
membership).

Web-GPT deep analysis (market survey Notion / Slack / Linear / Mem0 / Zep + a 2.10 Canon fact-check),
archived at `research/webgpt-workspace-membership-model-20260907.md`, ruled: a Tenant Membership is
**admission only** — it does not implicitly grant every Workspace. Create an explicit
`control.workspace_memberships`. This ADR implements what §6.1.1 already required; it is a
forward-fix, not new architecture.

## Decision

- **D-A / D-B (migration 0162).** `control.workspace_memberships (tenant_id, workspace_id, user_id,
  role CHECK OWNER|MEMBER, state CHECK ACTIVE|SUSPENDED|REMOVED, created_at, updated_at)`, PK
  `(tenant_id, workspace_id, user_id)`, composite tenant-leg FKs to `control.memberships` and
  `control.workspaces` (both UNIQUE targets already exist — `control.workspaces` already carries
  `workspaces_tenant_workspace_key`, so no UNIQUE is added), partial index
  `(tenant_id, user_id, workspace_id) WHERE state='ACTIVE'`. ENABLE + FORCE RLS with a self-read
  policy (tenant GUC AND user GUC match the row) — a request only ever needs its own memberships.
  No runtime role gets INSERT/UPDATE; the sole writer is the owner SECURITY DEFINER
  `control.set_workspace_membership(...)` (EXECUTE to `role_maintenance`, the §6.3 admin path).
  §6.2.2: gateway / retrieval_worker / maintenance SELECT; every write cell `—`. A backfill in the
  **same** migration, **before** 0163 re-points the policy, makes every current ACTIVE tenant
  member an ACTIVE MEMBER of every existing workspace of that tenant, so no one loses access at
  cutover. **Freeze going forward:** a new workspace inserts its creator OWNER; a tenant invite adds
  only the named/default workspace; a new tenant membership never fans out to all workspaces.

- **D-C (migration 0163).** Re-point the `WORKSPACE_SHARED` arm **only** of the three visibility
  policies (evidence_objects / memory_records / memory_rollups). The `p_workspace_ok` boolean passed
  to card 7's pure `private.visibility_allowed(...)` changes from an `EXISTS` over
  `control.memberships` (tenant membership) to an `EXISTS` over `control.workspace_memberships`
  matching the row's own `visibility_workspace_id`. `private.visibility_allowed` is unchanged; every
  other policy arm is reproduced verbatim; 0012/0153 bytes are untouched (§46 forward-fix via ALTER
  POLICY). The frozen §6.1.1 policy-definition hash in `xtask/src/rls_check.rs` is re-pinned in the
  same change: the migration-file integrity hash is unchanged; the current policy-definition hash is
  intentionally new — a legal forward-fix, not drift.

- **D-D (Rust, per request).** `credential_repo::lookup` stays the single cheap `api_key_lookup`
  fetch (no membership work — a machine credential or unknown prefix does no extra DB work, which
  also fixes the review P1 HMAC-ordering: the membership read moved out of the pre-HMAC lookup).
  `credential_repo::load_live_workspace_ids(pool, tenant, user)` is called in `bins/gateway/src/auth.rs`
  **after** `validate_api_key` succeeds and **only** when the credential carries a `user_id`:
  `SELECT workspace_id FROM control.workspace_memberships WHERE tenant_id=$1 AND user_id=$2 AND
  state='ACTIVE' ORDER BY workspace_id LIMIT 257` (257 mechanically proves no silent truncation to
  the 256 `BoundedSet` cap; a set of >256 → `INVALID_INPUT`). `derive_workspace_scope(live,
  credential_ws, requested_ws)` = `live ∩ {credential_ws if bound} ∩ {requested_ws if present}`;
  empty after any narrowing constraint → `Forbidden` (a stale api_key must not survive membership
  revocation). The credential's bound workspace is a default route and a restriction only, never a
  widening. A machine credential's live set is its bound singleton / empty, so the same derivation
  reproduces the §73.5.1 (0111) behaviour with no membership read. Card 9's Qdrant prefilter already
  filters on `allowed_workspace_ids`; cross-workspace candidates are filtered there and re-checked in
  PG RLS.

- **D-E (registered pin/unpin binding scope).** `BindingWritten`
  (`contracts/mcp/memory.output.schema.json` + `bins/gateway/src/mcp_application.rs`) reports the
  binding's scope `{ kind: "WORKSPACE", id }` — the credential's WORKSPACE route the pin/unpin landed
  on. The protocol catalog assertion (`crates/protocol/src/mcp_catalog.rs`) is updated for the field.
  This settles card 2's registered pin/unpin debt.

- **D-F (spec).** Baseline §6.1.1 carries the frozen rule (TenantMembership is admission, not
  workspace authorization; `WORKSPACE_SHARED(W)` needs active tenant AND active WorkspaceMembership
  AND RBAC; `allowed_workspace_ids` derived from live WorkspaceMembership every request; a bound
  workspace is a default + restriction, never a widening; TENANT_SHARED stays the tenant-wide
  mechanism). §73.5.1 is fixed to distinguish a machine key (empty set) from a PAT (live
  workspace-membership set narrowed by any bound workspace). §6.2.2 gains the
  `control.workspace_memberships` row.

## Consequences

- Acceptance is mechanically real: Alice (tenant ACTIVE, workspaces {W1,W2}) cannot read a
  `WORKSPACE_SHARED(W3)` memory; Bob ({W2,W3}) can — under domain `can_read`, live PG RLS, and a
  gateway e2e. Faults each turn a named test red: intersection→union (an unbound PAT sees a
  non-member workspace); dropping the workspace-membership EXISTS (WORKSPACE_SHARED collapses to
  tenant-wide); skipping the backfill (a current tenant member loses all access).
- Cost: the derivation adds one indexed `control.workspace_memberships` read per PAT request (after
  a valid key only). Machine credentials and bad keys pay nothing.

## Rejected

- The ADR-0034 tenant-level derivation (member = every workspace of the tenant) — below Canon §6.1.1.
- Returning the workspace set from the `api_key_lookup` definer (widens the owner surface, and does
  membership work before the HMAC check).
- Caching the workspace set (a revocation must take effect on the next request).
- A second visibility function / a fourth SQL copy of the disjunction (card 7's
  `private.visibility_allowed` is reused unchanged).

## 主线裁定（2026-09-07，opus 审查后；三项均为 P2，无 P0/P1）
- **ADR-0034 草案**：标记为 `Superseded by ADR-0035（从未实现）` 并在正文顶部加横幅说明其描述的 `credential_repo::lookup` 单事务派生、`effective_workspaces`、`member_workspace_ids` 与两个测试名在最终实现里都不存在。保留而非删除，是为了留住「先按租户级做→审查发现 WORKSPACE_SHARED 与 TENANT_SHARED 塌缩→网页 GPT 市面调研裁定 workspace 级」这条决策轨迹。
- **0162 definer 注释更正**：原注释称「owner rights bypass FORCE RLS，写入不需要 policy」是错的——`role_migration_owner` 是 NOSUPERUSER/NOBYPASSRLS（0011），本表 FORCE RLS，故 SECURITY DEFINER 的写入**仍受**上方 permissive tenant policy 的 WITH CHECK 约束：admin 调用方必须为该事务 `SET humaux.tenant_id = p_tenant_id`（0161 的 adapter 模式），否则 fail closed。注释已按此更正；因 0162 尚未提交且本次为纯注释改动（DB 终态字节相同），同步了 `ops.schema_migrations.checksum`（a123b30bd36388d3），`cargo xtask migrate` 复验 drift 0。写路径的实际调用方与测试随 §6.3 admin 接线时补（升级信号）。
- **共享夹具越界（`crates/adapters/tests/support/operation_receipt_fixture.rs`）**：接受。新模型从 `control.workspace_memberships` 派生 `allowed_workspace_ids`，夹具必须给自己的 owner user、peer user 与凭据绑定 workspace 播种 ACTIVE workspace membership——这是 D-B 生产 backfill 在测试台的对应物；不改则每个用户绑定凭据的测试都会 401/403。属主线所有的跨卡测试基座。
- 故障注入五项全部被抓：交集→并集、WORKSPACE_SHARED 退回 tenant membership、删 backfill（0162 manifest 自带 postcheck 由 t 翻 f）、给 role_gateway INSERT（rls-check 授权逐条相等红）、把派生移到 validate_api_key 之前（新增确定性探针测试）。
