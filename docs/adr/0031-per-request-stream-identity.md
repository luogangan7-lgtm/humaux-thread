# ADR-0031: 读路由的 stream identity 按请求推导（无 registry 表；Card 10）

日期：2026-09-06 · 状态：Accepted；D-B retired by ADR-0054（card 29：14 个 governance / subject / affect 写操作按请求推导 (tenant, workspace)，默认写 pair 不再需要）· 影响面：`bins/gateway/src/context.rs`（`ContextBootstrap::request_stream` + `provisioned_request_stream`：serving 读作为 pair 存在性闸）/ `bins/gateway/src/memory.rs`（`read_scope` 改为按请求推导；新增 `write_scope` 保留写路由的 bootstrap 常量比对）/ `bins/gateway/src/recall.rs`（去掉常量比对，family 按请求推导）/ `bins/gateway/src/bootstrap.rs`（模块文档 + key 表注释）/ `bins/gateway/src/mcp_application.rs`（注释）/ Baseline §34.0.1（Q9 段）/ 测试：`bins/gateway/tests/mcp_gateway.rs::native_mcp_one_process_serves_three_stream_pairs_per_request`（`#[ignore]`，与 real-Qdrant 兄弟测试同约束，gate 用 `--ignored` 显式跑）。前置：卡 1–9 与 E1 在树上（HEAD 2e43ec7，迁移至 0157）。研究档案：`research/webgpt-spec-silence-rulings-20260903.md` Q9（STREAM IDENTITY）。

## 背景
一个 gateway 进程在启动时把 `(tenant, workspace)` 烤进 `HUMAUX_GATEWAY_REMEMBER_TENANT_ID / WORKSPACE_ID`，三条读路由（`memory.get/enumerate`、`recall.search`、`context.assemble`）用常量比对硬拒一切别的 pair —— 服务第二个 workspace 就要第二个进程。部署报告把它列为多用户的第一个未闭合项。树上已有先例：`recall.search` 每请求新鲜解析 per-tenant Qdrant placement（§17.3），所以这是「用推导替换常量」，不是新架构。计划稿曾提议 0156 建 control-plane stream registry；Q9 裁决否决了它。

## 决定

### D-A 无 registry 表：6 元组 StreamKey 本身就是身份
- 不建任何表。每个已认证请求：guard 的 `credential.authorize` 已按成员资格收窄（越界 ⇒ `FORBIDDEN`；路由内再 `narrow` 一次只是进程内调用方的纵深防御，不是承重闸）→ `ContextBootstrap::request_stream(tenant_id, workspace)` = `StreamFamily{principal tenant, 进程配置 scope_kind, 请求 workspace, 进程配置 domain, projection_kind}` + `with_version(进程配置 projection_version)`。这是唯一的推导点；三条读路由都经它，`context_repo::materialized_identity` 继续在 adapter 侧复核 family/key/scope 三者一致。
- **pair 存在性闸 = §16.2 的 serving 读**（`provisioned_request_stream` → `private_read_projection_selector(family)`，与 `recall.search` 同一条查找）：family 没有 `serving` 行 ⇒ `DEPENDENCY_UNAVAILABLE`。这是推导额外加的唯一一次 PG 往返，且是卡片指明可用的既有 serving 读。没有它，`memory.get/enumerate` 与 `context.assemble` 对一个从未 provision 的 (tenant, workspace) 会读到 §15.4「无 checkpoint 行 ⇒ 全 0」的账本并以 200 报「complete」——就是审查抓到的 P0。账本 key 仍取进程配置的 `projection_version`（Q9），serving 值只证明 pair 存在。
- **按请求推导，不按进程缓存。** 值是纯函数（无 I/O），缓存无收益且是跨请求串扰的唯一可能来源；验收测试的并发交错分支就是它的负对照（进程级缓存 ⇒ 某些请求以另一 pair 的身份被服务 ⇒ 红）。
- 未 provision 的 pair 由**既有**查找关闭：成员资格收窄 `FORBIDDEN`（非成员 workspace）；serving-projection 读 `DEPENDENCY_UNAVAILABLE`（四条读路由一致；recall 另加 placement 读）；对象不可见 `NOT_FOUND`。没有新的 lookup。部署侧含义：一个 workspace 的读路由在 ops 把该 family 的 `v1` 行置 `serving`（`xtask projection-serve`）之前一律 `DEPENDENCY_UNAVAILABLE`，与 recall 此前已有的要求相同；in-process 测试 fixture 在 `application_with_budget` 里按同样方式 provision 自己的 bootstrap pair（`provision_stream_pair`）。
- `recall.search` 原来对不匹配 pair 返回 `FORBIDDEN`，`memory`/`context` 返回 `DEPENDENCY_UNAVAILABLE`；现在两者都在 `narrow` 处统一为 `FORBIDDEN`（越界 workspace），已 provision 的 pair 直接被服务。

### D-B 写路由维持 bootstrap 绑定到卡 11
- `remember.put`、confirm-gated 治理动词、`subject_register` / `subject_link_key` / `annotate_affect`（它们向 bootstrap 流签票或落行）继续经 `write_scope` / `run_confirmed_write` 的常量比对——一个进程仍只有一条写流。读路由与写路由的不对称是刻意的、有期限的（卡 11）。
- `HUMAUX_GATEWAY_REMEMBER_TENANT_ID / WORKSPACE_ID`：读路由不再消费（deprecated for reads）；因为写路由与读路由同进程，启动时仍必填。卡 11 抬写路由时一并移除。bootstrap key 表与 e2e env map 已注明。

### D-C receipts 不动
- §34.0.1 receipt 主键 = `(tenant_id, principal_id, operation, idempotency_key)` + `request_fingerprint` 绑定；`projection_version` 只是 payload 列，**不在键内**。一个进程服务 N 个 pair 不需要任何特殊语义：不同 tenant/principal 的同 idempotency_key 天然不撞。验收测试用 `pg_constraint` 钉住这组主键列（加入 `projection_version` 或去掉 `tenant_id` 即红）。

## 验收 gate
`native_mcp_one_process_serves_three_stream_pairs_per_request`（`--ignored`）：一个进程（bootstrap 写流 = pair A），三个 provisioned pair —— A 与 B **同一 tenant** 两个 workspace（本卡真正新开的配置：enumerate 候选谓词是 tenant 级，隔离只靠请求 workspace 这一半身份，tenant 级回归掩盖不了 workspace 级回归）、C 另一 tenant —— 共享同一个 Qdrant collection；四条读路由对每个 pair 各返回且仅返回自己的 memory；18 个交错并发任务（6 轮 × 3 pair × 4 路由）零串扰；A 凭据指名 B / C 的 workspace 四路由皆 403 `FORBIDDEN`；A 凭据在自己 workspace 指名 B / C 的 memory ⇒ `NOT_FOUND`；同 tenant、有成员资格但无 serving 行的 pair D 四路由皆 tool error `DEPENDENCY_UNAVAILABLE`（serving 闸的负对照）；非成员 workspace 四路由 403；body 带 `tenant_id` 四路由 400 `INVALID_INPUT`（closed schema，tenant 永远来自 principal）；receipt 主键列钉住。
故障注入实测（2026-09-06，日志 `scratchpad/gates/fi_card10_fix_{cache,nogate}.log`）：进程级缓存 StreamKey ⇒ pair B `memory.get` 403（同 tenant，仅 workspace 半边判出）红；去掉 serving 读 ⇒ pair D `memory.get` 200 带 `projection.expected=0` 的「complete」账本，红。既有的 foreign-workspace 拒绝测试（`assert_request_boundary_denials`、memory.get 矩阵、semantic recall 的 foreign_workspace 分支）继续通过。

## 速度
推导本身是纯函数；get/enumerate/context 各多一次 serving 读（一个只读短事务，卡片允许的既有 serving/placement 读），recall 不变。实测 p50（n=5，同机同库，前 → 后）：memory.get 25.2ms → 26.1ms，memory.enumerate 24.5ms → 24.4ms，context 27.6ms → 31.9ms，recall 1.177s → 1.177s（被 embedding worker 主导）。

## 否决
- **0156 `control.stream_registry (tenant_id, workspace_id) → (domain, projection_kind, version)`**（计划稿）：多一次 PG 往返、多一张要进 §6.2.2/rls_check 的表、多一条 provisioning 路径；而进程配置的三元组本来就是 process-wide 的，pair 只由 `(principal tenant, requested workspace)` 决定。Q9 裁决否决。
- **进程级 StreamKey 缓存**：纯函数无需缓存；缓存是唯一的串扰来源。
- **只靠 §15.4 账本读判 pair 存在**（`stream_checkpoints` 无行 ⇒ 全 0）：那是「合成空流」，不是失败关闭；而且 `stream_repo` / `context_repo` 不在本卡允许改动面内。serving 读是既有、卡片点名、且 recall 已在用的闸。
- **把 REMEMBER_TENANT_ID / WORKSPACE_ID 立刻改成可选**：写路由仍需要它们；缺省值会让写路由在启动后才失败，违反 fail-closed bootstrap。卡 11 一并处理。
