# ADR-0016: Private worker 自己拥有 Distill hop（Evidence → 0..N MemoryRecord，无 RPC）

> **环境契约已被 ADR-0036（card 14，migration 0164）取代**：本文提到的 `HUMAUX_PRIVATE_WORKER_DISTILL_TENANT_ID` / `_REASONING_DOMAIN_ID` 已从二进制删除；distill worker 现在通过 `ops.claim_derived_work` 跨租户认领 job，env 里不再有任何租户/域 id（必填键见 bins/private-worker/src/main.rs 与 ADR-0036）。以下正文保留为历史决策轨迹。

日期：2026-09-03 · 状态：Accepted · 影响面：`migrations/0147`（`memory_records` WITH CHECK 的 headless 臂 + 窄函数
`control.current_reasoning_route_binding`）/ `crates/adapters`（`distill_reasoner.rs`、`distill_repo.rs` 新增；
`consolidation_reasoner.rs` 两个 helper 改 `pub(crate)`；`projection_worker.rs` D6）/ `bins/private-worker`
（`distill.rs` 新增；`main.rs` 抽出 `bootstrap()`，新增 `--distill-once`/`--distill-serve`；`inference_rpc::clone_config` 改 `pub`）/
`xtask`（`rls_check.rs` R3 function gate 钉第二个函数；`e2e_seed.rs` 种 `PRIVATE_DISTILL_TEXT` binding 与 teardown）/
`docs/architecture/Baseline_2.9.md` §6.2.2 / `docs/ops/e2e-seed.md`。

## 背景（主线 rehearsal 2026-09-03 实测的缺口）

`remember.put` 写 `private.evidence_objects` + `private.events` + `ops.outbox(EVIDENCE_ACCEPTED, PENDING)` +
`projection.stream_log` 票据，然后**没有任何生产代码**把它变成 `private.memory_records`/`private.memory_evidence`
（仓内唯一非测试 `INSERT INTO private.memory_records` 是 `#[cfg(test)]` 的 seed helper）。`projection_worker::resolve_memory`
按 `ops.outbox JOIN memory_evidence JOIN memory_records` 解析票据，没有 memory 行就永远解析不到，recall 为空；
之前所有 e2e 都是用 fixture 直接种 memory。Consolidate hop（ADR-0015）是对的，但生产上没有输入。

## 决定

### D1 —— 为什么是 private worker，为什么不是 RPC

§6.2.2 里 `role_private_worker` 已经是这一跳的持有者：`memory_records`/`memory_evidence` 的 `SELECT, INSERT`、
`processing_runs` 的 `INSERT/SELECT/UPDATE`、`ops.outbox` 的 `SELECT/UPDATE`、`private.events` 的 `SELECT`；
consolidation worker 对 memory 表只有 `SELECT`。§4.2：只有这一个进程同时持有 provider（BYOK）与 DB 写能力，所以
Distill 不需要 ADR-0015 那样的 UDS RPC —— 同一进程内直接 `--distill-once`（一遍）/`--distill-serve`（常驻轮询，
`HUMAUX_PRIVATE_WORKER_DISTILL_POLL_INTERVAL_SECS`）。与 `--serve-rpc` 共用同一个 `bootstrap()`（provider/config/resolver/DNS pins），
新 env：`HUMAUX_PRIVATE_WORKER_DISTILL_TENANT_ID` / `_REASONING_DOMAIN_ID` / `_BATCH` / `_LEASE_SECS`。

**binding 按 purpose 解析，env 里不放 binding id。** `control.resolve_user_reasoning_admission`（0130）要 binding id，
而 `control.reasoning_route_bindings` 是 §6.2.2 零授权表（原文：「禁止先给通用 runtime SELECT 再补 owner gate」）。
所以 0147 加第二个窄 SECURITY DEFINER 函数 `control.current_reasoning_route_binding(reasoning_domain_id, purpose)`：
租户来自 session GUC（与 resolver 同一 `session_scope` 配方）、只返回 `(binding_id, binding_version)`、owner
`role_migration_owner`、`search_path=pg_catalog`、仅 `role_private_worker` 有 EXECUTE。拿到 id 后仍走**同一个**
`resolve_user_reasoning_admission(purpose = PRIVATE_DISTILL_TEXT)`，没有第二条 admission 路径。`rls-check` 的 R3 function
gate 改为遍历 `R3_PRIVATE_WORKER_FUNCTIONS`（resolver + 本函数）。

### D2 —— 事务形状与身份

每条 claimed 行两段事务，中间夹一次 provider 调用：

1. **读 + 指纹**（`begin_read_context`）：`domain_owner` → binding → admission → `load_evidence` →
   `context_snapshot_seq`（该租户 `ops.outbox` 最大 `commit_seq`）→ `source_hash` →
   `INSERT private.processing_runs`（`started_at` = now，`completed_at` NULL）→ COMMIT。指纹**先于**任何字节出境落库。
2. provider 调用（D3）。
3. **写**（`begin_write_context`）：逐候选 `authorize`（D4）→ `memory_records` + `memory_evidence(PRIMARY, ordinal 0)` →
   `finish_processing_run(output_digest, output_count, provider_request_id = disclosure_id)` →
   `complete_outbox(DONE)`，以 `lease_owner` 为围栏 → COMMIT。

`private.events` 的 RLS 没有 headless 旁路（`EXISTS(evidence_objects …)` 内联可见性析取），所以读 payload 前必须装
acting user：USER_PRIVATE 用 Evidence 自己的 `visibility_user_id`，否则用 reasoning domain owner（ACTIVE 成员）。
§11.1 context 的 `user_id`：origin 是用户类（DirectUserInput/UserConfirmed/TenantAdmin）且 `origin_principal_id` 非空
则用它，否则 domain owner。

### D3 —— prompt 契约 v1 与共享 provider 管线

`distill_prompt_contract()`：版本化 prompt + JSON schema + 输出预算一起哈希（`humaux.distill-prompt-contract\0` 命名空间），
其 sha256 就是 `processing_runs.prompt_hash`，`DISTILL_PARSER_VERSION` 就是 `parser_version`。信封
`{"evidence":{"index":1,"origin_class":<wire>,"max_class":<§10.1 ceiling>,"occurred_at":…,"payload":<events.payload 作为数据>}}`，
prompt 明说 payload 是不可信数据。输出 `{"memories":[{content, memory_type∈{Fact,Preference,Decision,Rejection,State,Issue},
class∈AuthorityClass 线格式, confidence∈[0,1]}]}`，**`memories` 可以为空**（§15.5「0/1/N」：没什么值得记 = 合法答案）。
`parse_distill_output` 对非 JSON、任一层多余 key、超过 8 条、空/超长 content、未知枚举、confidence 越界一律 fail closed。

管线与 Consolidate 逐步相同（`resolve_user_reasoning_admission` → `provider_matches_admission` →
`authorize_structured_egress` → `disclosure::reserve_private(sources=[Evidence])` → `admitted_inference_context` →
`complete_structured_timed` → `disclosure::finalize_private`），purpose = Distill。**ledger leg 同 ADR-0015 D5 的已知局限**，
本卡不修：`processing_runs.provider_request_id` 存的是 `ops.data_disclosures.disclosure_id`。

### D4 —— §10.1 ceiling：拒绝，不降级；可见性逐字继承

每个候选 `OriginBoundAuthorityPolicy.authorize(requested, memory_type, basis=[evidence.origin_class], scope)`；
`Ok` 才写行，`Err(reason)` 不写行、计一次 `memory_candidate_rejections_total{reason}`（reason 用 §80 metrics registry 的
四个值），其余候选照常继续。仓内没有 metrics facility（grep 无消费者），先以同名结构化 stderr 行 + `DistillPassReport.rejected`
计数（`// ponytail:` 已标）。每条 memory 的 `visibility_class/visibility_user_id/visibility_workspace_id` 逐字 = Evidence 的三列，
永不放宽；0147 的 WITH CHECK headless 臂也只放行这三种形状。`content` 形状 `{"title": 前 80 字, "key_claim": 全文}` ——
`projection_worker::card_input` 读这两个字段（`build_card` 至少要 `key_claim`/`evidence_excerpt` 之一），consolidation 把它当不透明 JSON 转发。
`memory_evidence.grounding_mode = 'IMMUTABLE'`，`recorded_version = payload_sha256 hex`：EVENT payload 内容寻址、不改写（§8.1），
按 §8.8 IMMUTABLE 边不进 recheck 派生。

### D5 —— 幂等键

- DONE 行不再被 claim（claim 谓词只取 PENDING 或 lease 过期的 PROCESSING）。
- memory 行、run 完成、outbox DONE 在**同一事务**提交，`complete_outbox` 以 `status='PROCESSING' AND lease_owner=$me` 围栏：
  lease 被别的 worker 回收后本 worker 的 DONE 翻转更新 0 行 → 整个事务回滚，memory 行不落地（`lost_lease` 计数）。
  crash 于 provider 调用中：processing_runs 行留着（`completed_at` NULL），lease 过期后重试，重试是新 run 行、新 disclosure；
  memory 行只在 DONE 事务里出现一次。
- **失败分两类，只有输入绑定的才 FAILED。** FAILED 是终态：claim 谓词不再取它，票据按 D6 变 `FAILED/distill_failed`，
  §15.7 连续前缀从此停在它前面 —— 所以只给「这条 Evidence 本身不可用」（不存在 / 非 EVENT / 未知 origin）与 parser fail-closed（D4）。
  所有 reasoning 侧失败（binding 未种 / 未 admitted / 健康观测过期 / provider 429·5xx / disclosure 台账）都是环境态：
  `distill_repo::release_outbox` 以 lease 为围栏把行放回 PENDING（`DistillPassReport.deferred`），下一趟重新 claim；
  run 行留 `completed_at` NULL 作尝试标记。没有 attempts 列，所以是无界重试（`// ponytail:` 已标：出现毒行时加
  attempts + DEAD 终态的 forward-fix 迁移）。
- `lease_owner` 每进程一个 uuid 后缀（`humaux-private-worker/<uuidv7>`）。

### D6 —— 0-memory 的票据结算（`projection_worker`）

`resolve_memory == None` 之前直接 FAILED `no_visible_memory_record`，会把一条合法的「没什么可记」Evidence 变成 §15.7 永久缺口。
现在按该 `(tenant, commit_seq)` 的 `ops.outbox.status` 决定：`DONE` → `SKIPPED_BY_POLICY/no_memory_distilled`
（计入连续前缀，与其它 policy 排除同类）；`PENDING|PROCESSING` → 留 `ISSUED` 不结算（`RunOnceOutcome.pending`），下一趟再读；
`FAILED` → `FAILED/distill_failed`；无 outbox 行 → 原样 `FAILED/no_visible_memory_record`。

### D7 / D8 —— 种子与 e2e

`xtask e2e-seed` 在同一 profile 上再种一条 `PRIVATE_DISTILL_TEXT` policy/candidate/binding（`seed_route` 共用），只打印
`distill_binding_id` 与 `HUMAUX_PRIVATE_WORKER_DISTILL_{TENANT_ID,REASONING_DOMAIN_ID}`；teardown 连带删 Distill 输出
（disclosures、memory、processing_runs、outbox、events、evidence）。`bins/private-worker/tests/distill_hop_e2e.rs`：
D1 真 MiniMax 全链 + `projection_worker::run_once` 解析票据；D2 假 provider 越 ceiling → 0 行/计数/DONE/output_count 0；
D3 `{"memories":[]}` → DONE + 票据 SKIPPED_BY_POLICY；D4 解析 fail-closed → FAILED、无行、run `completed_at` NULL；
D5 两遍不重复 + 过期 lease 重试不重复（并断言 `source_hash` 能从 run 行的轴列 + `events.payload` 规范摘要重算、
`evidence_payload_sha256[]` = Evidence 自己的 `payload_sha256`，seed 用非规范原始字节使两摘要确实不同）；
D6 未 admitted / provider 429 → 行回 PENDING、无 lease、不 FAILED，下一趟健康 pass 正好蒸馏一次。

## 验收 gate

`cargo xtask migrate`（0147 干净应用 + drift 0）· `cargo xtask rls-check` = 0 · `cargo xtask architecture-check` = 0 ·
`grep -rn ConsolidationDbPool bins/private-worker/src` = 0 · `cargo test -p humaux-private-worker --test distill_hop_e2e`
（`HUMAUX_REQUIRE_MINIMAX=1 HUMAUX_REQUIRE_DB=1`，D1–D5 全跑）· `cargo test -p humaux-consolidation-worker --test
consolidation_hop_e2e` 回归 · `cargo test -p humaux-adapters --lib distill`（解析 fail-closed + 契约稳定 + source_hash 轴敏感）·
`cargo test -p xtask rls_check` · clippy/fmt 全绿。

## 已知局限与升级信号

- **ledger leg 缺席**（同 ADR-0015 D5）：升级信号 = 需要按 binding 对账 Distill 的 BYOK 用量。
- **`source_hash` 的 evidence 轴用的是 `events.payload` 的规范 jsonb 渲染的 `payload_sha256`**，不是 remember 时的原始字节
  （原始字节不保留；`EvidencePayloadSha256` 唯一构造点是 `payload_sha256(bytes)`，不能从存好的 bytea 还原）。
  `processing_runs.evidence_payload_sha256[]` 存的仍是 `evidence_objects.payload_sha256` 原样 —— 这与 0064 列注释
  「the set source_hash hashes」**不一致**：指纹不能只从 run 行重算，还要读 `events.payload`。e2e D1/D5 的
  `assert_fingerprint_recomputes` 钉住两半各自的现状（任一侧漂移即红）。根因修法在 domain：给 `EvidencePayloadSha256`
  一条持久化摘要的读回路径（非哈希器，G80-22 闸相应改口径），evidence 轴改用 anchor —— 超出本卡文件范围，未做。
  升级信号 = §68 replay 需要用原始字节对账时。
- **stream family 字面量**：票据解析只覆盖 `projection_worker` 现有的 workspace 族（`private_memory`/`PRIVATE_MEMORY`/`v1`）；
  tenant 作用域流仍不在 projection worker 的处理面内。
- **单租户 pinned target**：与 consolidation worker 同理（RLS 会话按租户 pin），没有跨租户「谁有 pending」枚举；升级信号 = 多租户部署。
- 瞬时错误重试无界、无退避（行回 PENDING，下一趟即重试；`--distill-serve` 的 poll interval 是唯一间隔）；
  升级信号 = 观测到同一行反复 deferred（毒行）→ 加 attempts/DEAD。
- `EvidenceOriginClass` 的读侧映射（`distill_repo::origin_class_from_db_str`）是 `remember::origin_class_db_str` 的镜像，
  两边各自被同一份 0004 CHECK 列表的契约测试钉住；升级信号 = 第三个消费者出现时把它移到 domain 类型上。
- （审查 P2，未改）`projection_worker::terminal_for_missing_memory` 无法区分「蒸馏出 0 条」与「memory 行存在但对本 worker 不可见/不可解析」：两者 `resolve_memory` 都是 `Ok(None)`，outbox DONE 时一律 SKIPPED_BY_POLICY。升级信号 = 出现可见性/解析类缺口需要独立告警时，给 resolve 返回三态。
- （审查 P2，未改）`processing_runs.completed_at IS NULL` 同时表示「解析/provider 失败」与「指纹事务后进程崩溃」，孤儿 run 行会累积且无对账。升级信号 = 需要按 run 对账成本时加 `outcome` 列（0148）。
