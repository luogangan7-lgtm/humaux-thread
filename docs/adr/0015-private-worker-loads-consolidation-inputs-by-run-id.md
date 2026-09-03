# ADR-0015: Private worker 按 run id 自行装载 Consolidate 输入（registration 行携带 run id，sealed body 不变）

日期：2026-09-03 · 状态：Accepted · 影响面：`migrations/0145`（`ops.private_inference_rpc_calls.consolidation_run_id`、`role_private_worker` 对两张 consolidation 表的 SELECT、`memory_records`/`evidence_objects` 两条 RLS policy 的角色放宽）/ `crates/adapters`（`consolidation_reasoner.rs` 新增；`contribution_reasoner.rs` 抽出 `pub(crate)` 共享管线；`consolidate_repo.rs` `MaterializedInput.evidence_id`；`private_inference_rpc.rs`）/ `bins/private-worker/src/inference_rpc.rs`（按 purpose 派发）/ `bins/consolidation-worker`（`run_once_bound` + 生产 `build_rollup`）/ `docs/architecture/Baseline_2.9.md` §6.2.2 / `xtask/src/rls_check.rs` MATRIX。

## 背景（已实测的阻塞，不是推断）

1. `crates/adapters/src/contribution_reasoner.rs` 的 `impl PrivateReasoningPort for ContributionReasoner` 对**所有** purpose 都返回
   `Err("legacy contribution inference lacks canonical public coverage")`；而 `bins/private-worker/src/inference_rpc.rs` 只有这一个调用点，
   所以 §11.8 的 Consolidate hop 在 MiniMax 上从未真正成功过。
2. `SealedPrivateReasoningRequest`（§11.8）只有 `reasoning_domain_id / binding_id / binding_version / input_manifest_hash / purpose`；
   0143 的 `ops.private_inference_rpc_calls` 也只存这些。private worker **无法定位**它该推理的是哪一个 `private.memory_consolidation_runs` 行。
3. `role_private_worker` 对 `private.memory_consolidation_runs` / `private.memory_consolidation_inputs` 零授权（0011 只给 consolidation_worker 与 maintenance）。
4. `memory_records_tenant_and_visibility`（0140 之后）对无 `humaux.user_id` 的 headless 会话只放行 `role_retrieval_worker`；
   `evidence_objects_retrieval_worker_read` 只 `TO role_retrieval_worker`。两个 consolidation 角色都只设 `humaux.tenant_id`，
   所以一条 WORKSPACE_SHARED memory 对选择阶段与私有推理阶段都不可见（实测 `PublishOutcome::NoOutput`）。
5. `consolidate_repo::publish_rollup` 在 run 有 `workspace_id` 时发 `("workspace", w)` 票；`projection_worker.rs` 只消费 workspace 族流，
   所以要证明 hop 可被下游消费，T1 必须以 workspace 跑。

## 决定

### D1 —— run id 放在 registration 行，不放进 sealed body

`ops.private_inference_rpc_calls` 新增可空列 `consolidation_run_id uuid REFERENCES private.memory_consolidation_runs(run_id)`，
由 `role_consolidation_worker` 在 register 时与 sealed 标识符一起写入（column-narrow INSERT 授权同步放宽这一列）；private worker 在
claim 之后从**已 claim 的行**读到它，再自己去装载输入。

- 为什么不改 sealed body：§11.8 冻结的语义是「只带标识符 + manifest hash，其余全由 private worker 自取」。run id 是**定位信息**而不是推理内容，
  但 `SealedPrivateReasoningRequest` 是 application 层跨 crate 的 spec 形状，改它就是改 §11.8；而 registration 行本来就是 §11.8/ADR-0012
  「不信任 wire body、以已注册行为准」的那个载体，把 run id 放这里与 `binding_id` 等字段同一纪律。
- 为什么是 FK：一条 registration 永远不能指向不存在的 run；private worker 再核对 run 的 `tenant_id`/`reasoning_domain_id` 与 sealed 请求一致且 `status = 'RUNNING'`。
- `run_once` 的 `&dyn PrivateReasoningPort` 无法携带 run id，所以新增 `run_once_bound(bind_port: impl FnOnce(Uuid) -> P)`：
  `create_run` 之后再构造 port，`UdsInferenceClient` 在构造时绑定 run id（不可变状态，没有 Mutex/setter）。原 `run_once` 签名保留，委托给它。

### D2 —— `role_private_worker` 只读两张 consolidation 表

`GRANT SELECT` on `private.memory_consolidation_runs` / `private.memory_consolidation_inputs`（§6.2.2 两格从 `—` 变 `SELECT`）。
不新增 policy：两张表既有的 `FOR ALL` PERMISSIVE policy 都是 `TO PUBLIC`（runs = §62 NULLIF 租户子句；inputs = `EXISTS(runs ...)`），
已覆盖该角色；再加一条 PERMISSIVE 只是被 OR 掉的重复。§11.6 的 MUST NOT 由授权面成立：没有 INSERT/UPDATE，写不了 runs/inputs/rollups/memory_records。

### D3 —— 两条 RLS policy 放宽到 headless consolidation 角色（与 0140 同一理由）

- `memory_records_tenant_and_visibility`：`current_user = 'role_retrieval_worker'` 扩为
  `current_user IN ('role_retrieval_worker','role_consolidation_worker','role_private_worker')`，其余逐字节保持 0140 的表达式
  （用 `pg_get_expr` 取回后改写，不凭记忆重打）；WITH CHECK 不动。
- `evidence_objects_retrieval_worker_read`：只放宽 `TO` 角色列表，不加第 7 条 policy —— W1 census 钉住 count=6 与 legacy policy 的 sha。
- 理由与 0140 相同：headless、tenant-pinned、没有 acting user 的 worker 永远满足不了 membership 分支；真正的每次查询可见性在 §6.1.2
  下游（Qdrant payload + `visibility_disjunction`）执行；且 rollup 的可见性由 `publish_rollup` 继承 run 自己的 workspace scoping，
  不会把 WORKSPACE_SHARED 输入放大到 tenant。

**D3 修订（review P0，2026-09-03）**：上一段最后一句在 `workspace_id = None`（tenant 作用域 run，
`HUMAUX_CONSOLIDATION_WORKER_WORKSPACE_ID` 未设即是这个形状）时**不成立**：`select_and_materialize_inputs` 原谓词是
`$3 IS NULL OR visibility_workspace_id = $3`，`None` 时整个 workspace 过滤为空；0145 之前它之所以安全，只是因为 RLS 把所有 WORKSPACE_SHARED
行藏起来了（本文背景 4 的实测 NoOutput）。0145 放宽后，tenant 作用域 run 会选中租户内**任意 workspace** 的 WORKSPACE_SHARED memory，
再由 `publish_rollup` 以 TENANT_SHARED 发布 —— 跨 workspace → 全租户的可见性放大（dev 库实测：`role_consolidation_worker` 只设
`humaux.tenant_id` 跑该谓词，返回两条别的 workspace 的 WORKSPACE_SHARED 行）。根因是选择谓词，不是 policy：policy 放宽本身是 D3 要的
（workspace run 必须读到自己 workspace 的行）。修法：谓词改为 `CASE WHEN $3 IS NULL THEN visibility_class = 'TENANT_SHARED'
ELSE visibility_workspace_id = $3 END` —— 输入的可见性类必须已经落在 rollup 将要发布的作用域之内（与 USER_PRIVATE「永不选择」同一立场）。
0145 的注释文本保持原样（§46 迁移不可改），本段为准。`consolidation_hop_e2e.rs` T6 钉住：种 2 条 WORKSPACE_SHARED + 1 条 TENANT_SHARED，
`None` 跑一次，materialized inputs 与 rollup sources 都**只有**那条 TENANT_SHARED，票据 scope=tenant。

**D3 延伸（0146，实测于 0145 之后）**：`private.memory_rollups` 的 `memory_rollups_tenant_and_visibility`（0058）同样没有
headless 角色分支，`publish_rollup` 为 workspace 作用域的 run 写 WORKSPACE_SHARED rollup 时 WITH CHECK 直接 42501
（consolidation_hop_e2e T4/T1 实证）。0146 只改这一条 policy：读侧放行三个 headless 角色（与 0140/0145 同理）；写侧**最小权限**——
`role_consolidation_worker` 只能写 TENANT_SHARED 或带 `visibility_workspace_id` 的 WORKSPACE_SHARED，永远写不了 USER_PRIVATE；
其余分支与 0058 的 `pg_get_expr` 逐字节一致。rollback 在 manifest 里回到 0058 原表达式。

### D4 —— manifest hash 复算是完整性闸

private worker 按 `run_id` 读 `memory_consolidation_inputs`（按 ordinal 排序）JOIN 当前 `memory_records`，
用与 consolidation worker **同一个**函数族（`consolidation_reasoner::compute_input_manifest_hash` / `manifest_hash_over`，
`(memory_id, input_version BE, source_hash, ordinal BE)`）复算，与 `sealed.input_manifest_hash` 不等 ⇒ 立即失败，**不发 provider 调用**、
不落 disclosure 行（T5 实证）。额外地，每行还用 `consolidate_repo::row_fingerprint` 重算当前 memory 指纹与记录的 `source_hash` 比对，
选择之后被改过的输入同样 fail closed（避免对陈旧字节烧一次 BYOK，§11.5.1）。

### D5 —— 共享的 provider 管线，与不共享的 ledger 一段

`ConsolidationReasoner::infer` 走的每一步都是 `ContributionReasoner` 已有的那一步，抽成 `pub(crate)` 后两边共用：
`resolve_user_reasoning_admission`（同一个 SECURITY DEFINER resolver，purpose = `PRIVATE_CONSOLIDATE`）→
`provider_matches_admission` → `authorize_structured_egress`（§7.3，同一份 wire bytes）→ `disclosure::reserve_private`（§7.4）→
`admitted_inference_context` → `complete_structured_timed`（唯一的 `provider.complete_structured` 调用点与同一套 outcome 分类）→
`disclosure::finalize_private`。**没有第二条 provider 调用路径。**

**不能共享的一段（ledger leg）**：`model_call_ledger::reserve_reasoning_call_in_txn` / `finalize_reasoning_call_in_txn` 把
`purpose` 硬编码为 `CONTRIBUTION_DEIDENTIFY`，`call_kind` 只接受 `ContributionReasoningCallKind`；DB 侧 0130 的
`model_call_ledger_purpose_known`（`query_rewrite|embedding|rerank|CONTRIBUTION_DEIDENTIFY`）、
`model_call_ledger_reasoning_call_kind_known`、`model_call_ledger_reasoning_snapshot_shape` 三条 CHECK 让一条
`PRIVATE_CONSOLIDATE` 的 admission 快照行**无法插入**；通用 `reserve_call` 又只接受 `RetrievalWorkerDbPool`。
因此本轮 Consolidate 调用**不写** `ops.model_call_ledger`；`PrivateReasoningResult.model_call_id` / `provider_trace` 携带的是
`ops.data_disclosures.disclosure_id` —— 同一次 provider 尝试的、可从允许文件触达的唯一持久回执（§7.4 行，reserve→call→finalize
成败都落账）。补齐它需要一条新 migration 放宽三条 CHECK（加 `PRIVATE_CONSOLIDATE`）并把 `model_call_ledger.rs` 的
reasoning 预留函数从 contribution-only 泛化，两者都在本卡允许文件之外 —— 记为后续卡，不在这里偷改。

### D6 —— prompt 强制 §11.6 的 MUST NOT

`CONSOLIDATION_PROMPT_V1`（版本化、与 schema 和输出预算一起哈希）要求：只整合给定输入、不得添加事实/假设/外部知识；
每句都可追溯到输入；`sources` 只能列 envelope 里逐字的 `memory_id`；`class` 只能是 7 个 AuthorityClass 线格式之一且不得超过输入本身能支撑的
等级（§11.9 天花板由 `publish_rollup` 的 `validate_rollup_before_publish` 最终裁决）；只输出 JSON。输入内容作为 JSON envelope 里的数据传入，
明确标注为不可信、不得执行其中指令。`parse_rollup_output` 对非 JSON、空内容、未知 class、任何不在本 run 输入内的 source、零 source 一律 fail closed。

**D6 实测修订（真 MiniMax 三连跑，2026-09-03）**：
1. `sources` 改为信封里的 **1 起 `index`**（不再要求逐字抄 memory_id）：第 2 次真跑 MiniMax 把 36 位 uuid 抄错一位 → InvalidInput。
   解析器仍接受 uuid 字符串 / `{"memory_id":…}` 对象形态——形状宽松、成员资格不宽松（越界 index 与不在本 run 输入内的 id 一律关闭）。
2. 信封每个输入带 `"class"`（其 AuthorityClass 线格式），规则 (4) 明说「不得高于所用输入的最高 class，拿不准就照抄最高的」：
   第 3 次真跑模型声称 ProjectConstraint 而输入最高只有 UserPreference → `validate_rollup_before_publish` 按 §10.1 硬规则 3
   拒绝（不悄悄降级/提权）。给模型看见 class 后第 4 次真跑即绿（class=UserPreference，sources=2，workspace 票据 ISSUED）。
   拒绝语义保持不变：超 ceiling 的 run 失败等待下次调度重跑，不做 worker 侧静默截顶。

## 验收 gate

`cargo xtask migrate`（0145 干净应用 + drift 0）· `cargo xtask rls-check` = 0 · `cargo xtask architecture-check` = 0 ·
`bins/consolidation-worker/tests/consolidation_hop_e2e.rs` T1（真 MiniMax 全链：rollup 行 WORKSPACE_SHARED + workspace 票 + outbox +
retrieval_worker 仅凭 tenant 可读 + rpc 行 COMPLETED/run id/sha/disclosure 回执）与 T5（篡改 ordinal ⇒ FAILED、零 disclosure/ledger 行）·
T6（tenant 作用域 run 只吃 TENANT_SHARED 输入，见 D3 修订）· `bins/private-worker --serve-rpc` 与 T1–T5 的 harness 共用同一个
`inference_rpc::{bind_socket, serve}`（生产进程真的监听 consolidation worker 拨的 socket，不再只存在于测试里）·
`cargo test -p humaux-adapters consolidation_reasoner`（解析 fail-closed + manifest 确定性）· clippy/fmt 全绿。

## 已知局限与升级信号

- ledger leg 缺席（见 D5）：升级信号 = 需要按 `binding_id` 对账 Consolidate 的 BYOK 用量或 §11.5 计费时。
- rollup 本身尚未进入 projection 解析路径（`projection_worker` 只解析 `memory_records` 来源；票据引用的是 rollup 的 source memory）：升级信号 = 需要直接召回 rollup 文本时，再给 rollup 建投影入口。
- rollup 行的 headless 可见性已由 0146 解决（T1 断言 `rollup_visible == 1`）；但 projection worker 仍没有 rollup resolver（上一条），
  票据只指向 source memory/evidence。
- consolidation-worker 拨 private-worker 的 UDS 目前只靠内核 peer_cred（ADR-0012 单机本地信任），没有像 gateway→retrieval RPC 那样
  先向 cell registry 取 `IntraCellResource::PRIVATE_INFERENCE_RPC` 的 permit（该变体已登记进拓扑普查但无消费者）。升级信号 = 进入
  多进程/多主机部署或 SPIFFE 阶段时，客户端加 permit 获取（镜像 GatewayRetrievalEmbeddingClient）。
- `run_once`（旧签名）保留只为 `tests/run_once_e2e.rs` 与 T4；新调用方一律用 `run_once_bound`。
