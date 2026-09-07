# ADR-0034: AuthorizationScope 的 allowed_workspace_ids 按请求从 live membership 推导（多 workspace 一凭据；Card 13）

日期：2026-09-07 · 状态：**Superseded by ADR-0035（从未实现；仅保留决策轨迹）** · 影响面：`crates/adapters/src/credential_repo.rs`（`lookup` 变成一事务：`api_key_lookup($1)` + 同事务内 `SET LOCAL humaux.tenant_id` 后对 `control.workspaces × control.memberships(state='ACTIVE')` 的一次只读查询，新字段 `member_workspace_ids`）/ `bins/gateway/src/auth.rs`（`authenticated_binding` 的 workspace 集合 = live 集合 ∩ 凭据绑定；纯函数 `effective_workspaces`）/ `crates/domain/src/identity.rs`（`AuthorizationScope` 文档：集合的来源）/ Baseline §6.1.1（推导规则）/ 测试 `crates/adapters/tests/auth_scope_rls.rs::credential_lookup_derives_member_workspaces_live` + `bins/gateway/tests/mcp_gateway.rs::native_mcp_multi_workspace_scope_from_live_memberships`。前置：卡 1–12 与 E1 在树上（HEAD a0256b0，迁移至 0161）。**无新迁移、无新表、无新 grant**（`role_gateway` 对 `control.workspaces` / `control.memberships` 的 SELECT 是 0011 的 control 域默认；§6.2.2 与 rls_check 矩阵不变）。


> **本 ADR 已被 ADR-0035 取代，且其设计从未落地。** 卡 13 最初按「租户级 membership ⇒ 该租户全部 workspace」实现，opus 审查指出这会让 WORKSPACE_SHARED 与 TENANT_SHARED 在读权限上塌缩、且同租户内「第三个 workspace 被拒」无法成立；随后的网页 GPT 市面调研（Notion teamspace / Slack private channel / Linear private team / Mem0 project membership，归档 research/webgpt-workspace-membership-model-20260907.md）裁定新建 `control.workspace_memberships`，并指出 2.10 §6.1.1 本已要求 workspace 级 membership、0012 的 tenant-only SQL 低于 Canon。本文中描述的 `credential_repo::lookup` 单事务派生、`effective_workspaces`、`member_workspace_ids` 字段与两个测试名，在最终实现里都不存在——请以 ADR-0035 为准。

## 背景
§6.1.1 把 `allowed_workspace_ids` 定为 `BoundedSet<WorkspaceId>`（复数），`domain::identity` 结构上也支持集合，但仓库里没有任何路径填入多于一个：`bins/gateway/src/auth.rs::authenticated_binding` 只从 `control.api_keys.workspace_id`（0111，签发时烤死的单个可空列）造集合——有绑定 = 单元素，无绑定 = 空集。一个人在同一租户下有两个项目，就得持两把凭据、开两个 session，卡 10/11 的「一个进程服务 N 个 (tenant, workspace)」对最常见的场景（一个人、几个项目）落不到地。

仓库里的 membership 粒度事实（改动前先核过，不是假设）：`control.memberships` 是 **Tenant × User**（0003：`UNIQUE (tenant_id, user_id)`，无 `workspace_id`），§6 的 scope 树把 Workspaces 挂在 Tenant 下、Membership 是租户级对象；0012 的 RLS 策略本身也把 `WORKSPACE_SHARED` 读成「同租户存在 ACTIVE membership」并在注释里明写「control.memberships is tenant-scoped, not workspace-scoped … AuthorizationScope/can_read is where the narrower check lands」。所以「用户是 ACTIVE 成员的 workspace」在本仓库的可执行定义只有一个：**该用户持 ACTIVE membership 的租户下的全部 workspace**。

## 决定

### D-A 集合按请求从 live membership 推导；凭据绑定是缺省路由 + 收窄，不是授权
- `credential_repo::lookup` 从单条语句改为**一个事务**（READ COMMITTED，只读）：① `control.api_key_lookup($1)`（0111/0161 的 gateway-only definer，不变）；② 若该行有 `user_id`：`SELECT set_config('humaux.tenant_id', <该行 tenant_id>, true)`（`control.workspaces` / `control.memberships` 都是 FORCE RLS + 租户策略，0012/0112；不设 GUC gateway 什么也读不到，这是 fail-closed 的方向）→ `SELECT array_agg(w.workspace_id) FROM control.workspaces w WHERE w.tenant_id = $tenant AND EXISTS (SELECT 1 FROM control.memberships m WHERE m.tenant_id = w.tenant_id AND m.user_id = $user AND m.state = 'ACTIVE')`；③ COMMIT（`set_config(..., is_local=true)` 随事务结束消失，池连接不带状态回池）。机器凭据（`user_id IS NULL`）不查第二条，集合恒空。
- `authenticated_binding`（§73.5.1 的其余判据一字不动：tenant/user/membership ACTIVE、epoch 相等、scope 闭集）：
  - PAT：`allowed = effective_workspaces(live, bound)` = `bound` 为 `Some(w)` 时 `live ∩ {w}`，为 `None` 时 `live`。**永远是交集**（卡片 scope 原文：「the resulting set is always the intersection of live memberships with any credential-scoped restriction」；验收的故障注入就是把它换成并集）。`w ∉ live` ⇒ 空集（只剩 TENANT_SHARED + 本人 USER_PRIVATE），不是 401——集合空是 §73.5.1 早就定义过的合法状态，且没有扩权路径。
  - 机器凭据：不变（有绑定 = 单元素，无绑定 = 空集）。机器 principal 没有 membership，卡片 scope 只覆盖用户。
- **绑定的两个角色，分开说清**（卡片 D-A「default, not a ceiling」与验收「still narrows to exactly that one」在此处一并满足）：`control.api_keys.workspace_id` ① 是**缺省路由**（`bound_workspace_id`：请求不带 `workspace_id` 时走它，guard 的 `routed_workspace = requested.or(bound)` 不变）；② 是 live 集合上的**可选收窄**。它**不是授权来源**：ceiling 是 membership 集合，绑定只能在 ceiling 之内再缩，永远不能把一个 membership 里没有的 workspace 放进集合。0111 时代「绑定列 = 授权」的读法自本卡起作废（§73.5.1 那句「无 workspace 凭据产生空 workspace 授权集合」对 PAT 不再成立，见「主线待办」）。
- `narrow(workspace)`（`domain::identity`）不改：不在集合内 ⇒ `FORBIDDEN`。因为集合现在就是 membership，「非成员 workspace ⇒ Forbidden」是这条既有规则的直接后果，不需要第二处判断。
- **不缓存**：每个请求一次推导，与 ADR-0031 D-A 的「按请求推导，不按进程缓存」同理；卡 12 的 suspend/remove 在下一请求生效依赖它（membership 变 SUSPENDED ⇒ live 集合为空 **且** api_key_lookup 的 membership_state ≠ ACTIVE ⇒ 401；两道都是 live 读，不靠 epoch 也成立——adapter 测试用裸 `UPDATE memberships SET state='SUSPENDED'`（不动 epoch）证明集合本身清空）。
- 快照偏斜（READ COMMITTED 两条语句可能各自快照）只会朝**更严**的方向：membership 在 ①② 之间 ACTIVE→SUSPENDED ⇒ ② 空集；SUSPENDED→ACTIVE ⇒ ① 已判 401。没有能从偏斜里多出一个 workspace 的交错，所以不升 REPEATABLE READ（省一次往返）。

### D-B 读路由零改动：任一成员 workspace 都可请求
- `memory.get / memory.enumerate / recall.search / context.assemble` 的 `read_scope`（ADR-0031）本来就是「请求 workspace → `narrow` → 按请求推导 stream → serving 闸」；集合变成多元素后它们自动接受任一成员 workspace，`WORKSPACE_SHARED` 的跨 workspace 读仍要求该 workspace 在集合内（= 成员资格），`USER_PRIVATE` 仍绑 on-behalf-of user（`can_read` 不变）。无绑定的 PAT 不带 `workspace_id` 的读仍是 `DEPENDENCY_UNAVAILABLE`（没有缺省路由；不猜）。
- `memory.enumerate` 的候选谓词是租户级、隔离靠请求 workspace（ADR-0031 三 pair 测试已钉），所以同一凭据对 A、B 各自 enumerate 只回各自的 memory（本卡 e2e 断言）。

### D-C e2e：一凭据两 workspace；第三个 Forbidden；suspend 后全无
`native_mcp_multi_workspace_scope_from_live_memberships`（纯 PG，非 ignore）：peer 用户（ACTIVE member）持两把 PAT——`bound`（绑定 A）与 `unbound`（`workspace_id=NULL`）；A、B 两个 workspace 都 provisioned；A、B 各一条 WORKSPACE_SHARED memory。`unbound`：get A@A 200、get B@B 200（同一 bearer，不重签）、enumerate@B = {B}、enumerate@A = {A}、get B@A `NOT_FOUND`、指名另一租户的 workspace 与随机 UUID 皆 403、不带 workspace 为 tool error `DEPENDENCY_UNAVAILABLE`。`bound`：get B@B **403**（`{A,B} ∩ {A} = {A}`；把交集换成并集这条即红）、不带 workspace 走缺省路由 A 200。`membership_repo::apply(Suspend)` 后三种组合下一请求皆 401。开头 9 次 `bound` 的 memory.get 记 p50。
adapter 侧 `credential_lookup_derives_member_workspaces_live`（真 `role_gateway` 池）：绑定 A 的 PAT lookup ⇒ `member_workspace_ids = {A}`；owner 再建 workspace B、**不重签** ⇒ `{A, B}`；裸 `UPDATE memberships SET state='SUSPENDED'` ⇒ `[]` 且 `membership_state()='SUSPENDED'`；恢复 ACTIVE ⇒ 回到 `{A,B}`；把凭据改成机器凭据（`user_id=NULL`）⇒ `[]`。
单测 `auth::tests::pat_scope_is_intersection_of_live_memberships_and_binding`：`{A,B}` × `None` = `{A,B}`；× `Some(A)` = `{A}`；× `Some(C∉live)` = `{}`；`{}` × 任意 = `{}`。并集实现在第三条即红。

## 速度
请求路径新增：PAT 每请求 **一次**只读事务里的 2 条短语句（`set_config` + 索引读：`workspaces(tenant_id)` × `memberships UNIQUE(tenant_id,user_id)`），外加 BEGIN/COMMIT；机器凭据零新增。实测 p50（同机同库，n=9，`bound` PAT 的 `memory.get`，日志 `scratchpad/gates13/pre_change_p50.log` / `post_change_*.log`）：见文末「实测」。没有跨请求缓存（D-A）。

## 已知局限 / 升级信号
- **membership 粒度 = 租户**：本卡不新建 workspace 级 membership 表（不在允许面内，且 0012 的策略、§6 的对象树都是租户级）。若产品需要「同租户内只给某几个 workspace」，那是一张新卡：`control.workspace_memberships(tenant_id, workspace_id, user_id, state)` 进 §6.2.2 + rls_check，`credential_repo` 的第二条查询改成 join 它，`effective_workspaces` 与 gateway 侧不用动。届时「第三个 workspace Forbidden」的 e2e 断言可以换成同租户的非成员 workspace。
- `BoundedSet::MAX_LEN = 256`：一个租户超过 256 个 workspace 时 `BoundedSet::new` 是 `INVALID_INPUT`（既有 `?` 路径，现在会在真实数据上触发）。升级信号 = 第一个这样的租户；届时把上限做成构造参数（identity.rs 的 ponytail 注已写明）。
- **§73.5.1 与 0112 注释**：Baseline §73.5.1「版本 1 的无 workspace 凭证产生空 workspace 授权集合…不隐式获得全部 workspace」对 PAT 不再成立（机器凭据仍成立）。该节不在本卡允许改动面内——主线待办：把那句改为「机器凭据：空集合；PAT：live membership 集合（ADR-0034），绑定列只收窄」。
- **卡 2 登记的 pin/unpin 债**（ADR-0019「审查 P2」：按 binding_id/scope 精确 unpin 并在结果里报 scope）：读代码核对——`memory.rs::write_binding` 已把 binding 落在**请求路由到的** workspace（`requested.or(bound)` → `narrow`），`context_repo::active_pinned_binding_in_txn` 也按 `(tenant, WORKSPACE, 该 workspace, memory)` 精确取行、unpin 按 `binding_id` 撤，多 workspace scope 下每个 workspace 各自一行、互不串。**未完成的一半是「结果里报 scope」**：`mcp_application.rs` 的 `BindingWritten` 结果与 `contracts/mcp/memory.output.schema.json` 的 `BindingWritten` 分支都不在本卡允许面内。主线待办（一处改动、无新 op、无新 output 分支）：`BindingWritten` 加 `scope: {kind: "WORKSPACE", id: <workspace uuid>}`，schema 同步；读侧 `scope_chain` 本来就按整条链匹配 PINNED，不用动。

## 否决 / 未做
- **在 `api_key_lookup` 的 SQL 函数里一并返回 workspace 数组**：要改 0111 的 definer（新迁移），不在允许面内；而且 definer 以 owner 身份跑，多返回一个跨租户能力面。gateway 自己在 SET LOCAL 之后读、受 RLS 约束，更窄。
- **单语句 CTE `WITH guc AS (SELECT set_config(...)) SELECT … FROM control.workspaces`**：RLS 谓词的求值顺序相对 CTE 不受保证，安全路径不赌 planner。多两次往返换确定性。
- **`raw_sql` 多语句一趟（隐式事务）**：要把 UUID 格式化进 SQL 文本、结果集拼接后取最后一行——可行但难读；先用与 `request_guard_repo::read_effective_entitlements` 同形的显式事务，实测 p50 若不可接受再换。
- **并集 / 「绑定列是授权」**：见 D-A；故障注入项。
- **进程级或 TTL 缓存 membership 集合**：卡 12 的「下一请求生效」会被缓存打破。
- **新建 workspace 级 membership 表**：见「已知局限」。

## 实测
（填于 gate 跑完之后；见 StructuredOutput 的 gates 记录。）
