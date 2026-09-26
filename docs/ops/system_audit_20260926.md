# 系统体检报告（humaux-thread @ 72bdfa7，2026-09-26）

## 1. 结论

1. **架构基线：是按最新基线开发的。** 代码依据的是仓库内的 `docs/architecture/Baseline_2.9.md`（17,160 行，最后一次修改是 card 24 的提交 3cad38f，2026-09-26）和 `docs/adr/` 下的 46 个 ADR。外部的 Baseline 2.10 在 §11.2.2（约第 2445 行）中被写明为"只是上游设计输入，不是第二个真源"，并按 ADR-0008 的规则逐条合并进 2.9。这和你 2026-08-28 的决定一致（Humaux 记忆：唯一架构真源是 Baseline_2.9.md）。仓库的 `PHASE` 标记为 9（§57 共 Phase 0–17），Phase 10 由 ADR-0009 明确关闭。
2. **语言和栈：** Rust edition 2024 的 workspace，含 15 个 crate、8 个 bin 和 xtask。但仓库没有固定工具链版本，构建机是 rustc 1.97.1，比基线冻结的 1.98 低一个版本。PostgreSQL 18.6、Qdrant v1.19.0（走 REST）、axum 0.8.9、tokio 1.53.1、sqlx 0.8.6 都符合 §70。
3. **健康度：** 数据库层这次用在线开发库核过，没有问题：9 个登录角色、130/130 张 RLS 表都开了 FORCE、83 个 SECURITY DEFINER 函数全部属于 role_migration_owner 并固定了 search_path、143 个迁移的校验和漂移为 0。请求路径上的租户隔离也没有发现漏洞，**P0 为 0 项**。但有两个硬问题：
   - 按 runbook 部署时，交付链路走不通：记忆写入后没有进程把它投影进 Qdrant。
   - §67.2 标为"必需"的四项都没实现：BYOK/OpenBao、Prometheus/OTel、§44 备份与重建、§48.1 数据保留。交付报告 §6 也没把它们列为已知限制。
4. **生产前必须处理：去重后 17 项 P1。** 其中 9 项按 runbook 部署就会碰到，4 项在接入第二个真实租户或用户时触发，3 项随数据量增长触发，1 项需要你裁决（本地 reranker 和规范冲突）。另有约 28 项 P2。
5. **你 2026-08-30 定的验收目标（Humaux 记忆 9a7c56a7）里，有两项还没交付：** "人物文档记忆"（artifact.* 五个操作都没实现）和"恢复"（没有备份，也不能重建）。
6. 把各项修法的量级相加，工作量粗估 **25–40 人日**，不含复核和首跑变红后的修复。

## 2. 架构与技术栈

| 项 | 实际情况 | 依据 |
|---|---|---|
| 架构基线 | `Baseline_2.9.md`，17,160 行。第 11 行写"文档权威规则（2.9 Canonical）"，但第 3 行版本头还是"Architecture Baseline 2.8 … 日期 2026-08-25"，没更新 | `docs/architecture/Baseline_2.9.md:3,11` |
| 2.10 的处理 | 只按条外科合并，禁止整体覆盖。有 6 个编号冲突，已改号为 DOD-099/100、G80-47/48/49、G11-5 | ADR-0008 决定 1–3 与附录 A；Baseline:2445 |
| ADR | 46 个文件，编号 0001–0049。0008 有两个文件；0013、0021–0023 缺号 | `ls docs/adr` |
| 阶段 | `PHASE`=9。Phase 10 常驻演化进程已关闭（ADR-0009）。Agent Capture（§34.3，自标 phase=15）仓库里没有实现（ADR-0008:62） | `PHASE`；ADR-0008/0009 |
| 语言与工具链 | Rust edition 2024；没有 `rust-toolchain.toml`，也没有 `rust-version`；构建机 rustc/cargo 1.97.1。基线要求 1.98 | `Cargo.toml:8`；Baseline:5、:13111 |
| 主要依赖 | axum 0.8.9、tokio 1.53.1、sqlx 0.8.6、hyper 1.11.0、reqwest 0.12.28。reqwest 只在 `crates/infra-network` 里用（这是 §83 要求的出网收口点） | `Cargo.lock` |
| 常驻进程 | gateway（HTTP 上的 MCP JSON-RPC）、private-worker（`--serve-rpc`/`--distill-serve`）、retrieval-worker（`--serve-rpc`，只做查询向量）、consolidation-worker（`--serve`）。进程间走 UDS，服务端校验对端凭据 | supervision.md §1 |
| 一次性或占位的进程 | retrieval-worker `--run-once`（投影）、public-worker `--run-once`、admin；`bins/maintenance` 只打印"not wired yet (Phase 0 scaffold)" | `bins/maintenance/src/main.rs:5` |
| 存储 | PostgreSQL 18.6，143 个迁移（止于 0175），ops.schema_migrations 共 143 行。登录角色 9 个（§6.2.0：5 个运行时 + 4 个非运行时；任务说明写的"8 个"少算了 role_migration_owner）。Qdrant v1.19.0 | 只读 SQL |
| 外部模型 | 向量：DashScope（环境变量 `DASHSCOPE_API_KEY`）；推理：MiniMax（OpenAI 兼容）；召回路径上只有情绪（affect）重排 | ADR-0030、recall.rs:546 |
| MCP 工具面 | 已实现 20 个操作键（`mcp_application.rs:77-98`）。合约里声明的 artifact 5 个、code 9 个、coordinate 13 个，共 27 个操作，调用时都拒绝（fail closed）。ADR-0044 D-A 接受了这一点，但交付报告 §6 没有列 | inventory；ADR-0044 |

**与基线的已知偏差**（第 3、4 节逐条展开）：
- 工具链 1.97.1 对基线的 1.98。
- §67.2 标为必需的 OpenBao/BYOK、OTel+Prometheus 没实现。
- §44 备份与重建、§48.1 分区与保留没实现。
- §22.4/§52.2 的投影滞后（projection lag）信号没实现。
- §23.3 要求 `candidate_count == cand_k`，没做到。
- §67.2 的网关准入（并发 16 / 队列 64 / 等待 5 s）没实现。
- §15.5 的 consistency token 没有签名。
- 已登记的偏差有两项：classify() 参数个数（ADR-0047）、Tenant Fair Scheduler 延后（§1.14 第 32 行，DENOMINATOR_GATED）。

## 3. 必须处理（P1，共 17 项；P0 为 0）

**总览**（"复核"一栏是两位独立复核者的结论，合并项取票数之和）

| # | 问题 | 触发时机 | 复核 | 工作量 |
|---|---|---|---|---|
| 1 | 投影环节没有常驻进程驱动，也不跨租户 | 按 runbook 部署即触发 | 10/10 票维持 P1 | 数天 |
| 2 | BYOK 未实现，所有租户共用一把环境变量里的 key | 第二个真实租户 | 2/2 P1（一旦有外部租户即 P0） | 短期数小时，正式数天 |
| 3 | 8 个 LOGIN 角色的口令写死在仓库 | 部署即触发 | 2/2 P1 | 数小时 |
| 4 | 同租户有多个推理域时，合法 Evidence 被永久判 FAILED | 第一个多用户租户 | 2/2 P1 | 数小时 |
| 5 | A2 完整性核对两边单位不同，治理操作后报假丢失 | 任何一次治理操作后 | 2/2 P1 | 数天 |
| 6 | recall 只向 Qdrant 取 5 个候选，却宣称 cand_k=25 | 出现归档或被取代的记忆时 | 2/2 P1 | 1 天 |
| 7 | 查询里出现常用词就返回 INVALID_INPUT | 部署即触发 | 1 票 P1，1 票 P2 | 数小时 |
| 8 | 投影滞后不产生任何降级信号 | worker 停摆时 | 1 票 P1，1 票 P2（下文补了新证据） | 1 天 |
| 9 | 任何进程都不导出指标 | 部署即触发 | 4/4 P1 | 数天 |
| 10 | 没有备份，Qdrant 也不能从 PG 重建 | 磁盘或卷故障时 | 2/2 P1 | 数天 |
| 11 | 增长型表没有分区、没有保留，也没有清理进程 | 随数据增长 | 2/2 P1 | 数天 |
| 12 | gate 链里 35 个 PG 测试静默跳过，却计为通过 | 已经发生 | 2/2 P1 | 1 天 |
| 13 | 连接池和超时全用默认值，缺少 §67.2 准入控制 | 多租户并发时 | 2/2 P1 | 数天 |
| 14 | memory.enumerate 首页取全量 id，并逐行 INSERT | 大 workspace 时 | 2/2 P1 | 1 天 |
| 15 | 热路径上的连接键没有索引 | 随数据增长 | 2 票 P1，2 票 P2 | 数小时–1 天 |
| 16 | distill 跨租户严格先进先出，没有公平调度 | 多租户时 | 1 票 P1，1 票 P2 | 数天 |
| 17 | 本地 reranker：你的需求和规范冲突，代码两边都没实现 | 需你裁决 | 2 票 P2（按规范判） | 裁决数小时，实现数天 |

### P1-1 投影环节没有常驻或跨租户的驱动（合并 C6、DM-4、OPS-1、PERF-2、RQ-4，5 个 lens 各自独立发现）

- **现象和影响：** 按 runbook §5 启动四个常驻进程后，remember→distill 能跑通，但投影票据（`projection.stream_log` 里的 ticket）一直停在 ISSUED，新记忆永远进不了 Qdrant。
  - 不带 consistency_token 的 recall 看不到新记忆。带 token 的只在 TTL 内靠 PG overlay 兜底。
  - 新的 (tenant, workspace) 在有人手工跑 `projection-serve` 之前，recall 返回 DEPENDENCY_UNAVAILABLE（`bins/gateway/src/recall.rs:343-347`，no_serving_projection）。
- **证据：**
  - `bins/retrieval-worker/src/main.rs:117-119` 只有三种模式：`--readyz`、`--run-once`、`--serve-rpc`。
  - `:183-185` 从环境变量读单个 `HUMAUX_RETRIEVAL_WORKER_TENANT_ID/SCOPE_KIND/SCOPE_ID`。
  - `:220-234` 的 placement 由环境变量拼出 SharedFallback；那里的 ponytail 注释写明"没有读 projection.tenant_placements"。
  - `projection_worker::run_once` 在非测试代码里只有 `main.rs:256` 一个调用方。
  - `docs/ops/supervision.md:13` 写明 `--run-once no`（不常驻），§4 的启动顺序图（:106-115）里没有投影进程。
  - `docs/ops/runbook.md:82` 写"After the projection worker catches up"，但没有任何东西让它 catch up。`:85` 给的是不带参数的 `cargo xtask projection-serve`，而 `xtask/src/projection_serve.rs:66-72` 要求 `--tenant/--workspace/--domain/--projection-kind/--version`。
  - 唯一的驱动是 `docs/ops/rehearse.sh:829-853`：一个 shell 循环，每 5 秒对两对写死的 tenant/workspace 跑一次。
  - 开发库里 ISSUED 状态的 ticket 有 138 行，分属 104 个租户（大多是测试残留）。
  - 交付报告 §6.1–6.9 没有登记。
- **根因：** ADR-0036 只给 distill 和 consolidation 做了跨租户认领（迁移 0164 的 `ops.claim_derived_work`），投影没有做同样的改造。
- **修法：**
  - 给 retrieval-worker 加常驻 `--serve` 模式：用 owner 身份的 SECURITY DEFINER 跨租户认领（照 0164 的形状：SKIP LOCKED + 租约 + 每个 family 一把 advisory lock），placement 读 `projection.tenant_placements`，并负责退役 FAILED 票据。
  - 首次激活仍按 ADR-0017 做成显式运维动作。RQ-4 说"ADR-0017 允许自动激活"，这是错的，ADR-0017 否决了自动切 serving。
  - 过渡期：在 supervision.md 和 runbook 里列出按 (tenant, workspace) 的定时任务，改正 runbook.md:85 的参数，并在交付报告 §6 登记。
- **工作量：** 数天（一张卡）。
- **复核：** 10 票全部维持 P1。有两位指出：首次激活靠手工是 ADR-0017 接受的设计，不算缺陷；缺陷在于已激活的 family 之后的持续投影没有进程驱动，并且没有登记。

### P1-2 BYOK 未实现，所有租户共用一把进程级的环境变量 key（ARCH-1）

- **现象：** 租户 B 在准入时绑定的 credential_ref 和 provider_account 被忽略。B 的私有 Evidence 用运营者的账户发给 MiniMax，但 `ops.model_call_ledger` 记的却是 B 的 credential_ref 和计费账户。AccountHealth 也会归错：运营者的 key 被限流（429）时，会把 B 标成不健康。
- **证据：**
  - `bins/private-worker/src/main.rs:178-199` 的 ponytail 注释写着"env-held key until `adapters::openbao` stops being a placeholder"，`resolve(_credential_ref)` 直接返回 `self.0.clone()`。
  - `crates/adapters/src/byok.rs:997` 把租户的 credential_ref 传进去，然后被丢弃。
  - `crates/adapters/src/contribution_reasoner.rs:1030-1045` 的 `provider_matches_admission` 不比较 credential_ref 和 provider_account_id。
  - 规范：Baseline §11.2.4（2588-2627）规定准确的权威身份是 `(tenant_id, credential_ref, provider_account_id)`；§67.2（12405）写"OpenBao | 必需，dev stub 仍 forbidden"；§70（13118）写"Private reasoning: User BYOK only"。
  - 代码注释援引"§4.2 one process, one credential"，但 §4.2（808-826）里没有这个说法。
  - 自 ADR-0036 起 distill 已跨租户（`main.rs:23`）。
  - 开发库 ledger 有 6,509 行，涉及 1,166 个 credential_ref、1,982 个 tenant，全部由同一把 key 服务。ADR 和交付报告 §6 都没有登记。
- **根因：** OpenBao 解密器没实现，用环境变量 stub 顶替，而且没有把这个 stub 限定到单个凭据。
- **修法：**
  - 短期：把进程里这把 key 绑定到唯一一组 `(tenant, credential_ref, provider_account_id)`，其他准入按"未就绪"退回（不消耗重试次数），并在 §6 登记。
  - 正式：实现 OpenBao Transit 解密器，再加一道 gate：任何 CredentialDecryptor 实现忽略 credential_ref 参数就判红。
- **工作量：** 短期数小时，正式数天。
- **复核：** 两位都维持 P1。两位都指出：现在只有运营者自己的开发节点和自己的 key，不构成第三方泄露；一旦同一个 worker 接入第二个真实租户，就升为 P0。

### P1-3 8 个冻结的 LOGIN 角色，口令写死在仓库里（DM-3）

- **现象：** 按 runbook 部署后，能连到 PG 端口的人可以用仓库里公开的口令登录 role_migration_owner。这个角色是所有表和所有 definer 函数的 owner，登录后可以 `ALTER TABLE … NO FORCE` 或 `DROP POLICY`，§6 的租户隔离全部失效。
- **证据：**
  - `migrations/0011_roles_and_grants.sql:21-52` 共 8 条 `CREATE ROLE … LOGIN PASSWORD`（口令值不在此引用）。
  - 同文件头注释 `:9-15` 要求真实部署时从 secrets manager 执行 ALTER ROLE。
  - `docs/ops/runbook.md:30-37`（§2）和交付报告 §7.7 只谈了 role_admin。
  - xtask 里没有轮换检查，`xtask/src/rls_check.rs:4423` 本身就用占位口令登录。
  - 在线库 pg_authid 里 9 行 role_* 都可登录，且带 SCRAM 口令。
  - 开发容器只绑定了 `127.0.0.1:54329`。
- **根因：** 迁移注释里的轮换要求没有写进部署文档，也没有 gate 检查。
- **修法：**
  - runbook §2 加一步：逐个 ALTER ROLE 轮换口令。
  - role_migration_owner 在迁移窗口以外设为 NOLOGIN。
  - 加部署期 gate：用每个仓库占位口令尝试登录，任何一个成功就判红。
  - 在 §7.7 登记。0011 已经应用过，不能改。
- **工作量：** 数小时。
- **复核：** 两位维持 P1。不升 P0，因为利用它需要生产环境的 PG 端口网络可达。

### P1-4 同一租户有多个推理域时，合法 Evidence 被永久判 FAILED（C1）

- **现象：** 同一租户里两个用户各有一个推理域 D1、D2（`crates/adapters/src/remember.rs:223` 按用户分配）。D1 的 DERIVED_DISTILL 任务会按租户整批认领 outbox 行，把 D2 的 E2 也领走。然后按 D1 查 E2 查不到，返回 `Ok(None)`，走到 `settle_failed`。投影侧的票据随之变成 FAILED，卡住 §15.4 的连续前缀。唯一的出口是退役，退役后 E2 永远不会被 recall 返回。
- **证据：**
  - `crates/adapters/src/distill_repo.rs:179-186`：认领的 WHERE 条件里没有推理域。
  - `:292`：load_evidence 按 `reasoning_domain_id = $3` 过滤。
  - `bins/private-worker/src/distill.rs:302-317`：从任务 payload 里取单一推理域。
  - `:496-502`：`Ok(None)` 走 `settle_failed`。
  - `migrations/0164_derived_work_dispatch.sql:81-93`：每个 Evidence 一个任务，带着它自己的推理域和 evidence_id，但 worker 没用这个 evidence_id。
  - 开发库：已有 12 个租户的 evidence_objects 分布在 2 个推理域，但目前没有对应的 outbox 行，所以问题还在潜伏。
- **修法：**
  - 认领按任务的 evidence_id 过滤，或者 join evidence_objects 按推理域过滤。
  - 推理域对不上时把行放回 PENDING，不要判 FAILED。
  - 补一个"一个租户两个推理域"的 e2e 测试。
- **工作量：** 数小时。
- **复核：** 两位维持 P1。第一个多用户租户就会触发；不会泄露数据。

### P1-5 A2 完整性核对两边单位不同，治理操作后 recall 报假丢失（C3）

- **现象：**
  - `done` 数的是已结算的票据。每次 supersede、restore、archive、correct 都会多发一张 MEMORY_LIFECYCLE 票据。
  - `visible` 数的是 Qdrant 里的点。
  - 做过一次治理操作后，`visible + deleted + skipped < done`，于是 recall 放弃作答并报 PROJECTION_INVISIBLE_LOSS，`current=false`。
- **证据：**
  - `crates/adapters/src/stream_repo.rs:149`：done 是 SETTLED_OK 状态行的 count(*)。
  - `crates/adapters/src/retrieve.rs:1168/1231`：visible 计 Qdrant 点。
  - `crates/retrieval/src/envelope.rs:120-133`：judge_a2 比较两者。
  - `crates/adapters/src/memory_governance_repo.rs:276/305/829/937/1505` 发 lifecycle 票据。
  - `retrieve.rs:731-738` 的注释自己也写明：一个 Evidence 对应一张 EVIDENCE_ACCEPTED，再加每次治理一张 MEMORY_LIFECYCLE。
  - 演练证据：rehearsal5 的 run1–run5，`recall_after_restore.json` 都是 done=6、visible=4、比例 0.667，并带 PROJECTION_INVISIBLE_LOSS；rehearsal6 的 `recall_after_kill9` 是 done=8、visible=4。
  - ADR-0042 D-H 修过同一类问题（把 RETIRED_FAILED 归入 skipped），但 lifecycle 票据没处理。ADR-0040 和交付报告 §6 都没有列。
  - 演练 gate 把 `recall_hits_after_restore=1` 判为绿，没有检查这个信号。
- **影响：** 在产品主打的治理流程（纠错、撤销、恢复）里，完整性契约是失真的。
- **修法：** 让两边数同一种东西，例如只数 EVIDENCE_ACCEPTED 票据，另一边数不同的 source_stream_seq；可见性过滤要么两边都做，要么都不做。correct 时再给旧记忆 M1 发一张退役票据。
  - 两个子论点两位复核者都没有独立验证：(b) 一个 Evidence 产出 N 条记忆会导致判为 Inconsistent；correct 之后 M1 的点不会被退役。
- **工作量：** 数天。
- **复核：** 两位维持 P1（主论点 (a) 确认）。

### P1-6 recall 只向 Qdrant 取 top_k=5 个候选，却对外宣称 cand_k=25（RQ-1）

- **证据：**
  - `bins/gateway/src/recall.rs:349-361`：`DenseQuery::new(…, retrieval.top_k(), Vec::new(), …)`。cand_k 只在 `:549-552` 被写进 provenance。
  - `crates/projection/src/dense.rs:191-201`：Qdrant 过滤条件里没有 status 和 archived 子句。
  - memory.archive 只设置 archived_at，status 仍是 active。
  - `crates/adapters/src/read_materialize.rs:126-162`：PG 层按 archived、superseded、secret、tombstoned 排除。
  - Baseline §23.3（约第 6067 行）要求 `candidate_count == cand_k == min(top_k*5,200)`。`crates/retrieval/src/envelope.rs:681-688` 和 `candidate.rs:288` 都实现了这条不变量，但网关的实际路径没有走它们。
- **影响：** 归档 4 条近似重复的记忆后，查询只剩 1 条结果，排在第 6–25 名的有效记忆从来没被取出。情绪重排也只能在 ≤5 条里排序。开发库里没有归档或被取代的行，所以 soak 没有触发这个问题。
- **修法：**
  - 向 Qdrant 取 cand_k 个候选，经过 PG 过滤和情绪重排之后再截到 top_k。
  - payload 加 archived 标志，过滤条件加 `status=='active'`。
  - 把 TOMBSTONED 的 seq 传给 overlay。
  - 加一个 fixture：归档 5 条近似重复后，仍返回 top_k 条。
- **工作量：** 1 天。
- **复核：** 两位维持 P1。

### P1-7 查询里出现常用中文词、引号或 UUID，recall.search 直接返回 INVALID_INPUT（RQ-3）

- **证据：**
  - `bins/gateway/src/recall.rs:252-261`：planner 判定不是 Semantic 就返回 InvalidInput，日志记 `query_not_semantic`。
  - `crates/retrieval/src/planner.rs:203-273` 做子串匹配，触发词包括：最近、上周、相关、关系、现在、目前、进度、状态、继续、接着、代码、函数、`()`、`::`、`.py`，以及任何引号字符。
  - `:145-156`：形如 UUID 或 sha256 的 token 会被当成 DirectGet。
  - `contracts/mcp/recall.schema.json` 的 `query` 字段没有任何说明；也没有测试覆盖 `query_not_semantic`。
- **影响：** "目前项目进度""客户张三最近的情绪怎么样""和支付相关的决定"都会被判成调用方输入错误，agent 不会重试。而唯一存在的 dense 通道其实能回答这些查询。
- **已接受的部分：** Baseline §33（8640-8641）和 ADR-0044 D-A 都明确锚定了 `recall.rs:255-261` 返回 INVALID_INPUT。所以用哪个错误码是规范内的决定；问题在于规范和交付报告 §6 都没写明，日常用词就会触发。
- **修法：**
  - 在 dense 是唯一通道期间，planner 判出的任何类别都走 dense，并在 envelope 里记录 planner 的类别和 `lane_substituted` 降级码。只对显式的 `mode` 参数保留拒绝。同时修订 §33 并写 ADR。
  - 最低限度：给 schema 加说明、在 §6 登记、补 fixture。
- **工作量：** 数小时，外加规范修订。
- **复核：** 一位认为这是规范接受的设计，判 P2；一位认为已接受的限制被低估了，判 P1。本报告取 P1：用户一定会碰到，而且 §6 没列。

### P1-8 投影滞后不产生任何降级信号（ARCH-3）

- **证据：**
  - `crates/retrieval/src/completeness.rs:99-104` 的注释自己承认，lag 没有输入路径。
  - `classify()`（`:676-716`）没有 lag 这个输入。
  - `ProjectionLag` 只在测试桩里出现（`crates/testkit/tests/pair/projection_lag_*.rs`、`fault/projection_lag.rs`，都用本地的 `simulate_projection_lag`），所以 G52-5 这组测试并不覆盖真实行为。
  - `crates/contracts/src/config_registry.rs` 里没有 lag 相关的配置键。
  - 规范要求：Baseline §22.4（5705）、§52.2（10795）、DOD-014（12770）。
- **本次综合补充的证据：** 判 P2 的那位复核者的理由是"ISSUED 超过 15 分钟会被巡检改成 LOST，current 就会翻成 false"。但这个巡检函数 `stream_repo::sweep_lost`（`crates/adapters/src/stream_repo.rs:409`）在非测试代码里没有调用方，ADR-0043:329 也自己写了"has had no caller since §15.2 landed"。
  - 所以在交付的系统里，worker 停摆时 `current` 可以一直是 true，`degradations=[]`，唯一的变化是 completeness_ratio 下降。
  - §42 的 lag 告警又因为没有指标导出（P1-9）不会触发。
- **修法：**
  - 把 lag 阈值注册为 §78 的配置项。
  - 把 (expected−done) 或最老一条 pending 的年龄作为 classify() 的第六个输入，映射到 `CannotEstablish{ProjectionLag}` 或 §52.2 规定的降级。
  - 把 sweep_lost 放进常驻维护进程（见 P1-11）。
  - 写 ADR，和 ADR-0047 欠下的 classify() 参数个数变更放在同一张卡里。
- **工作量：** 1 天，不含维护进程。
- **复核：** 一位判 P2，一位判 P1。本报告依据上面 sweep_lost 没有调用方这一事实，取 P1。

### P1-9 没有任何进程导出指标，所有告警规则永远不会触发（ARCH-2 + OPS-2）

- **证据：**
  - `crates/telemetry/src/metrics.rs:1` 是占位模块。
  - `bins/gateway/src/main.rs:58-73` 只注册了 `/livez` 和 `/readyz`。
  - `GuardMetrics::exposition()`（`bins/gateway/src/guard.rs:112`）没有调用方。
  - 所有 Cargo.toml 都没有引用 opentelemetry 或 prometheus。`crates/retrieval-provider/src/metrics.rs:4` 的 ponytail 注释自己也承认了。
  - `deploy/prometheus/invariants.rules.yml` 的 INV-1/2/4 以 `sum(rate(degrade_total))>0` 为前提；没人导出这个指标族，空向量永远不会 >0，规则永远不触发。
  - 规范：Baseline §67.2（12407）写"OTel Collector + Prometheus | 必需"，§53.5 在 10941-10960。
  - ADR-0016:73-74 只是把"没有指标设施"作为局部实现说明，没当作交付限制；交付报告 §6 也没有列。
- **影响：** 以下情况都没有任何信号：distill 积压超过 0.229 Evidence/s 的容量、MiniMax 或 DashScope 故障、DEAD 任务堆积、投影滞后。runbook §8 给的容量规则无法检测是否被违反。
- **修法：**
  - 每个进程（或网关在 loopback 端口上）暴露 `/metrics`，导出进程内已经在计数的 §41.2 指标族。
  - 加由 SQL 派生的 gauge：最老一条 PENDING 的 EVIDENCE_ACCEPTED 的年龄、各任务类型的 DEAD 数、最老一条 ISSUED 票据的年龄、FAILED 票据数。
  - 过渡期：用 cron 定时跑 SQL 检查查询，并在 §6 登记。
- **工作量：** 数天。
- **复核：** 4 票全部维持 P1。

### P1-10 没有备份和 PITR，Qdrant 也不能从 PG 重建（OPS-4 + ARCH-8）

- **证据：**
  - `crates/adapters/src/projection_worker.rs:845` 只处理 `state='ISSUED'` 的票据。
  - `migrations/0011_roles_and_grants.sql:406-458` 的状态迁移触发器没有 DONE→ISSUED 这条边。
  - 非测试代码里没有重发或回填票据的逻辑。`crates/retrieval-provider/src/failover.rs:93` 把重建工作流明确标为超出范围。
  - 发布新的投影版本也走不通，会被交付报告 §6.1/§6.2 所述的限制拒绝。
  - `deploy/` 下只有 compose/dev.yml、一个空的 helm/ 和 prometheus/。grep pgBackRest、PITR、pg_basebackup 都没有结果。`ops.restore_drills` 是 0 行。
  - 规范：Baseline §44（9836-9880）要求 PITR、"guaranteed rebuild from PostgreSQL"和恢复演练；§41.2/§42（9686-9687、9778）定义了演练的指标和告警。
  - 交付报告 §6 和 runbook 都没有备份或灾备条目。你的 2026-08-30 验收目标明确列了"recovery"。
- **影响：** Qdrant 卷丢了，所有已有记忆都 recall 不到，而且没有恢复路径。PG 盘坏了，所有数据丢失。
- **修法：**
  - runbook 加：pgBackRest 或 pg_basebackup、WAL 归档、异机副本、Qdrant snapshot。
  - 代码加 `xtask projection-rebuild`：用 owner 身份的 definer 函数重建 collection 和 payload 索引，把该 family 的 DONE 票据重置为 ISSUED，让现有的 run_once 重新生成向量。
  - 在临时库上做一次恢复演练，并写入 `ops.restore_drills`。
- **工作量：** 数天。
- **复核：** 两位维持 P1。§44 中 PG 部分的措辞是"推荐 pgBackRest"；Qdrant 重建部分是硬要求。

### P1-11 增长型表没有分区、没有保留，也没有清理进程（DM-2）

- **证据：**
  - Baseline §48.1（10425-10436）："从第一版就要支持按时间 range partition / retention"。
  - Baseline:1181：物理删行的唯一出口是 migration owner 执行分区 drop。
  - `pg_partitioned_table` 为 0 行。
  - `bins/maintenance/src/main.rs:5` 只是脚手架。
  - 生产代码对以下表都没有 DELETE：ops.outbox、ops.jobs、stream_log、audit_events、model_call_ledger、retrieval_embedding_rpc_calls、selection_snapshot_items。
  - `control.retention_policies`（`migrations/0003_control_core.sql:120`）没有任何代码读取。
  - ADR-0043:328 承认"no maintenance daemon"，但只是针对那两个 sweep 说的。交付报告 §6 没有列。
  - 开发库现状：audit_events 17 MB；ops.jobs 13 MB（6,989 行里 6,350 行是 DEAD）；retrieval_embedding_rpc_calls 12 MB / 6,272 行。
- **修法：**
  - 加一个以 role_maintenance 身份运行的常驻维护进程（形状按 ADR-0043 的描述），承担：confirm token 清理、sweep_lost、DONE/DEAD 任务和已越过水位线的 outbox 行的清理（用 owner 身份的 definer 函数实现）、rpc_calls 和快照的保留。
  - 趁表还小，决定 events、audit_events、model_call_ledger 要不要分区；或者写 ADR 明确豁免 §48.1。
  - 不要给运行时角色 DELETE 权限。
- **工作量：** 数天。
- **复核：** 两位维持 P1。这是未满足的硬约束，今天还不会引发故障。

### P1-12 交付 gate 链里有 35 个 PG 集成测试静默跳过，却计为通过（TH-1）

- **证据：**
  - `crates/adapters/tests/g80_31_handoff.rs:176-185` 的 `setup()` 仍然要求端口 61719。同样写死的还有 `provider_budget.rs:44-56`、`mandatory_context_lane.rs:54/142`、`retrieval_query_sources.rs:91`。
  - `crates/testkit/src/lib.rs:163-172` 的 `skip_or_fail` 只在声明了 `HUMAUX_REQUIRE_*` 时才 panic。
  - gate 链的 DSN 指向 54329 端口，而 `live_env.sh` 只导出了 `HUMAUX_REQUIRE_DASHSCOPE`。
  - `gates_card24_final3.log` 的记录：
    - g80_31_handoff：11 个通过，耗时 0.00 s（:1926）
    - mandatory_context_lane：6 个通过，0.00 s（:1966）
    - provider_budget：15 个通过，0.00 s（:2235）
    - retrieval_query_sources：3 个通过（:2425）
  - 同一次运行里，真正连库的 jobs_claim 用了 5.39 s。
  - ADR-0047 D-D 说 g80_31 已经修好，但实际只改了 `gateway_dsn()`。ADR-0045:334 自己也写了 mandatory_context_lane "Not runnable here"。
- **影响：** 以下三块如果回归，会带着 `adapters_tests EXIT 0` 发货：G80-31 handoff；provider 预算（并发预留、跨租户 scope、RPM、幂等重放）；§25 mandatory lane。
- **修法：**
  - 四个文件按 ADR-0047 D-D 的规则改成接受 `HUMAUX_TEST_PG_DSN` 指定的任何库。
  - gate 链导出 `HUMAUX_REQUIRE_DB/QDRANT/MINIMAX=1`，并加进环境检查循环。
  - 再加一道检查：已知需要数据库的 crate 出现"N 个通过、耗时 0.00 s"就判红。
  - 预计首次真跑会有测试变红。
- **工作量：** 1 天，不含修复首跑的红项。
- **复核：** 两位维持 P1。

### P1-13 连接池、超时全用默认值，缺少 §67.2 的准入控制（PERF-4）

- **证据：**
  - `crates/adapters/src/postgres.rs:91` 只写了 `PgPoolOptions::new().connect(dsn)`，用的是 sqlx 0.8.6 的默认值：每个池 10 个连接、取连接超时 30 s。
  - `pg_db_role_setting` 为 0 行；`statement_timeout`、`idle_in_transaction_session_timeout` 都是 0。整个代码里唯一的超时是 `quota_repo.rs:501` 的 `SET LOCAL lock_timeout='2s'`。
  - `bins/gateway/src/guard.rs:245-259、320-425`：每个请求要跑 1 个预检和 4 个认证后的限流桶，各自一个事务，各自拿一把 advisory lock（`quota_repo.rs:505`）。
  - `bins/gateway/src` 里没有信号量或并发限制层，唯一的 503 是 draining 时返回的（`main.rs:69`）。
  - 规范：Baseline §67.2（约 12426-12440）要求网关入站并发 16、队列 64、最长等待 5 s，溢出返回 503 + RATE_LIMITED 并计入 `admission_rejected_total`。第 345 行把连接池大小列为待实测的值。
- **影响：**
  - 单个租户或单个 NAT 出口 IP 突发时，排在自己锁上的事务会占住池里的连接，其他租户取连接最长要等 30 s。
  - handler 的 tokio 超时丢弃 future 后，语句还在服务端继续跑。
  - 每个进程约 170–280 req/s 的上限是估算，没有实测。
- **修法：**
  - 连接池大小、取连接超时、每个角色的 `statement_timeout` 和 `idle_in_transaction_session_timeout` 都做成 §78 配置。
  - 实现 §67.2 的入站准入。
  - 4 个认证后的限流桶合并成一个事务，按固定顺序加锁。
  - 准入用单独的小连接池。
  - 按 IP 的限流桶加 TTL 清理。
- **工作量：** 数天。
- **复核：** 两位维持 P1。两位都指出"会把别的租户饿死"说得过重，锁的临界区只有毫秒级；定 P1 的依据是 §67.2 的准入控制缺失，而且这些参数不可配置。

### P1-14 memory.enumerate 首页取全量 id 并逐行 INSERT，快照永不清理（PERF-3）

- **证据：**
  - `crates/adapters/src/context_repo.rs:1672-1686` 用 `fetch_all` 取数据，没有 LIMIT。
  - `crates/adapters/src/selection_repo.rs:395-399`（`:200-213` 也一样）逐个 id 单行 INSERT，而且是在网关的 REPEATABLE READ 事务里。
  - 生产代码里没有删除快照的语句。迁移 0165 的 manifest 第 43 行断言 role_gateway 没有 DELETE 权限。
  - `ENUMERATION_TTL` 为 15 分钟（`bins/gateway/src/memory.rs:317`），到期只会让游标失效，不会删行。
  - 开发库：1,406 个快照全部过期，全部还在；items 表 22,345 行，4.9 MB。
  - 交付报告 §4.4 里 enumerate 的 p50 为 45 ms，是在记忆条数不超过 100 的租户上测的。
- **影响：** 一个有 5 万条记忆的 workspace，每次取首页要 5 万次往返（≥10 s，这是外推，没实测），期间占住一个池连接，可能超过 handler 超时后返回 DEPENDENCY_UNAVAILABLE。每次成功调用还会留下约 5 万行。
- **修法：**
  - 改成单条 `INSERT … SELECT unnest($ids) WITH ORDINALITY`。
  - 给 manifest 设上限，改用 keyset 游标。
  - 过期快照的清理放进 P1-11 的维护进程。
- **工作量：** 1 天。
- **复核：** 两位维持 P1。O(n) 次往返和无限增长是确定的；超时的量级是外推。

### P1-15 热路径上的连接键没有索引，每次都扫全体租户的历史（DM-1 + PERF-1 + PERF-6）

- **证据：**
  - `crates/adapters/src/retrieve.rs:678-685` 的 overlay 按 `ops.outbox (tenant_id, commit_seq)` 连接，并按 `private.memory_evidence.evidence_id` 连接。
  - 同类查询还有：`read_materialize.rs:155/291`、`continuity_read.rs:332-335`、`projection_worker.rs:334`、`distill_repo.rs:180-186`，以及迁移 `0164:166-176` 的任务认领（每次轮询全表扫 ops.jobs，现有 6,989 行）。
  - 在线 pg_indexes：
    - outbox 只有主键和 4 个部分唯一索引。
    - memory_evidence 只有主键 (memory_id, evidence_id, role)，evidence_id 不是前导列。
    - ops.jobs 没有认领查询要用的索引。
  - outbox 没有生产代码删除。
  - 今天的规模：outbox 2,334 行，一个 100 行的 overlay 用时 7.5 ms。
- **影响：** 今天感觉不到。成本随所有租户的历史线性增长；再叠加 P1-11（从不清理），一定会退化。
- **修法：** 写前向迁移：
  - outbox `(tenant_id, commit_seq)` 唯一索引，条件 `WHERE commit_seq IS NOT NULL`（在线数据里没有重复）；
  - outbox `(tenant_id, evidence_id)` 部分索引，以及给认领用的部分索引；
  - memory_evidence `(evidence_id)`；
  - ops.jobs 认领用的部分索引；
  - memory_records `(superseded_by)` 部分索引。
  - 注意：`migrate.rs` 把每个文件当作一个隐式事务，所以 `CONCURRENTLY` 只能放在单语句文件里。加完后重跑速度基线。
- **工作量：** 数小时到 1 天。
- **复核：** PERF-1 两位判 P1，其中一位说是 P1 的下沿；DM-1 两位降为 P2，其中一位判为不成立，理由是今天只要毫秒级、规范也没强制要这些索引。本报告仍放在"必须处理"，因为修法只要数小时，而且和 P1-11 叠加后必然退化。如果只看今天，可以视为 P2。

### P1-16 distill 跨租户严格先进先出，一个租户的积压会挡住所有租户（PERF-5，附 C7）

- **证据：**
  - `migrations/0164_derived_work_dispatch.sql:174` 的排序是 `ORDER BY priority DESC, next_retry_at, created_at`。
  - 每个 Evidence 一个任务（0164:126-132）。
  - `bins/private-worker/src/distill.rs:414-420`：每个任务的 run_once 按租户整批认领 50 行。
  - 规范：Baseline 4862-4873、7199、12252（"不要让 oldest-only claim 成为唯一仲裁"）、DOD-058（12831）。
- **场景：** 租户 A 导入 1,000 条，租户 B 写 1 条。B 要等大约 1000 / 0.229 ≈ 73 分钟，再加约 6 分钟（估算）。
- **附带问题（C7）：**
  - outbox 行的租约只在认领时设一次（`crates/adapters/src/distill_repo.rs:189-190`），心跳只续 ops.jobs 的租约（`distill.rs:441-449`）。
  - 按演练参数（BATCH=50、LEASE=120 s），一批里大约第 17–27 行之后的行，会被第二个 distill 进程重新认领，导致 provider 调用花两次钱。`complete_outbox` 的围栏能防重复写入，防不了重复花费。
  - 单进程部署不会触发。
- **已接受的部分：** Baseline §1.14 第 32 行和延后表（13036-13037）把 Tenant Fair Scheduler 标为 DENOMINATOR_GATED，激活条件是"≥2 租户同时有在跑 job"。多租户生产环境一上来就满足这个条件。交付报告 §6 和 ADR-0036 都没有登记。
- **修法：**
  - 任务按 (tenant, reasoning_domain) 合并。
  - 认领时按租户轮转（`DISTINCT ON tenant_id`，或者每个租户每轮设上限）。
  - outbox 行的租约随任务心跳一起续期，或者改成逐行认领。
  - 本轮有活干时跳过轮询间隔的 sleep。
- **工作量：** 数天。
- **复核：** 一位判 P1（Baseline 要求公平调度）；一位判为不成立、降 P2（规范写了延后）。本报告取 P1，理由是规范自己定的激活条件在多租户部署下成立。

### P1-17 重排：你要求用本地 reranker，规范写"不提供内置神经 reranker"，代码两边都没实现（RQ-2）

- **证据：**
  - `crates/retrieval/src/rerank.rs` 和 `fusion.rs` 各只有一行占位。
  - `bins/gateway/src/recall.rs:546` 的 `rerank_model_id` 为 NotApplicable，`:554` 的 lanes 为 `["dense"]`。
  - 唯一的重排是情绪排列（`crates/application/src/affect.rs:62`，ADR-0030 D-D）。
  - 规范：Baseline:1492 写"标准部署不提供内置神经 reranker/embedding 模型"；:1451 和 :1479 把 rerank 定为外部托管 provider（默认 DashScope）；:1494-1496 允许 rerank 不可用时降级。
  - ADR-0044 D-A 把 Tool 2 收窄为 semantic 通道，但没提 rerank。交付报告里"rerank"出现 0 次。
  - 你的偏好（Humaux 记忆 06907823，2026-09-01 再次确认；另见 b9e3c2b0）："重排序：使用本地 rerank 模型"，验收阈值必须按本地模型重新校准，不得沿用 qwen3-rerank 的阈值。
- **影响：** 召回少了第二阶段排序。你已确认的路由和冻结的规范互相冲突，没有 ADR 裁决过。
- **修法：**
  - 先在 §6 登记"召回只有 dense，没有 rerank，也没有融合通道"。
  - 你裁决后写 ADR 取代 Baseline:1492。例如在 retrieval-worker 内部经 UDS 跑本地 cross-encoder（不算出网），放在 cand_k 候选、PG 过滤之后执行，并按本地模型重新定阈值。
- **工作量：** 裁决和写 ADR 数小时，实现数天。
- **复核：** 两位都按规范降为 P2，其中一位判为不成立，并称"无法从仓库核实业主要求"。本次综合从你的记忆库里核实了这个要求，所以作为"你已确认但未交付、且与规范冲突"的需求列在这里，严重度由你定。

## 4. 建议处理（P2）

以下各项未经第二轮复核，只有 OPS-3 和 RQ-5 这两项例外（它们由 P1 复核后降级而来）。

| 项 | 问题 | 证据 | 修法 / 工作量 |
|---|---|---|---|
| OPS-3（由 P1 降级） | 两个问题：① 每次认领都给 attempt 加 1，包括"未就绪"和"推迟"的认领，所以连续推迟后遇到第一次真实错误就直接 DEAD，ADR-0036 D5 说的"不花重试预算"不成立；② DEAD 从不写 `last_error_class`，也没有重新投递的工具 | `migrations/0164_derived_work_dispatch.sql:182`；`bins/consolidation-worker/src/lib.rs:404`；`bins/private-worker/src/distill.rs:354`；`crates/adapters/src/jobs.rs:421-430`；开发库 DEAD 行 attempt 最高到 33 | 失败次数单独计数；写入错误类别；加 `xtask requeue-dead`；约 0.5–1 天 |
| RQ-5（由 P1 降级） | 交付报告把 recall p50 约 1.8 s 归因于向量往返，但 ledger 里短查询向量的 p50 只有 228.5 ms（n=368），约 1.5 s 没有解释；soak 跑的是 debug 构建 | `docs/ops/delivery_point_report.md:257-260、285-286`；rehearse_v2.sh:166,187 | 给 recall 加分阶段耗时；用 release 构建重测；数小时 |
| §6 缺项 | 以下限制在 ADR 或规范里已接受，但交付报告 §6 没有写：治理操作只能用于 bootstrap 的 (tenant, workspace)（ADR-0031/0032，即 C5）；投影遇到一次瞬时错误就判 FAILED，退役后永久不可见（§15.2.1、ADR-0042，即 C4）；artifact/code/coordinate 共 27 个操作只有声明（ADR-0044） | ADR-0031:17-18；ADR-0032:18,48；Baseline:3884 | 补 §6.10 及之后的条目；约 1 小时 |
| RQ-7 | 你的验收目标里的"人物文档记忆"未交付：tools/list 里列出了 artifact，但每次调用都返回 DEPENDENCY_UNAVAILABLE | `crates/protocol/src/mcp.rs:49-58、150-155`；`mcp_application.rs:77-98` | §6 登记；把只有声明的工具从 tools/list 过滤掉或在描述里标明；文档接入单独立卡 |
| ARCH-4 | 规范要求 `ledger::close` 自己读三次数据；实际实现接受调用方传入的计数结构体，而且没有 gate 限定谁能调用 | `crates/retrieval/src/completeness.rs:448-455`；Baseline:5741-5746 | 在 architecture-check 里钉死调用方集合，或把结构体改成 pub(crate)；数小时 |
| ARCH-5 + SEC-3 | coord.canvas_elements 和 private.code_edges 只靠单列外键归属租户，没开 RLS，运行时角色有写权限；rls_check 只枚举带 tenant_id 列的表，看不到这两张 | `xtask/src/rls_check.rs:2578、2722`；`migrations/0009_coord.sql:41`；`migrations/0006_private_skeleton.sql:74`；两表今天都是 0 行 | 加 tenant_id、复合外键、FORCE RLS；rls_check 改成白名单制；1 天 |
| ARCH-6 + SEC-5 + OPS-5 | UDS 对端 uid 等于自身 uid（或为 0）时进程不拒绝启动；所有交付演练都在同一个 uid 下运行，所以对端凭据校验在交付证据里等于没测，§6.9 对此说轻了 | `bins/retrieval-worker/src/rpc.rs:109`；`bins/private-worker/src/main.rs:302`；rehearse.sh:148,158,254,729,743 | 启动时比较 geteuid()，只有显式的 `HUMAUX_*_ALLOW_SAME_UID=1` 才放行；修订 §6.9；数小时 |
| ARCH-7 | 工具链没固定（1.97.1 对 1.98）；Baseline 头部版本还写着 2.8 | `Cargo.toml`；Baseline:3、:13111 | 加 `rust-toolchain.toml` 和 `rust-version`，或写 ADR 把基线改为 1.97；数小时 |
| ARCH-9 | consistency token 是没有签名的 hex，调用方可以改 expires_at 绕过过期策略（仍受 RLS 约束，不会泄露数据）。加签名与已否决的方案 710a2548 不冲突：那条否决的是"用签名代替授权" | `crates/adapters/src/retrieve.rs:292-323、429`；Baseline:3964-3972 | 加 HMAC-SHA256；数小时 |
| SEC-1（由 P1 降级，见第 6 节） | §74 身份表（password_hash、token_hash 等）对 6 个运行时角色可读；这是规范 `control.*=R` 的默认值，表目前为空 | `migrations/0035` 第 29-42 行；Baseline 约第 1162 行 | 在 §74 写入路径上线前，把凭据类表从默认授权里单独拿出来；数小时 |
| SEC-2 | 运行时角色可以 INSERT/UPDATE `ops.schema_migrations` 和 email 相关表：伪造一行迁移记录可以让加固迁移被跳过；也可以从本域名发信 | `migrations/0011_roles_and_grants.sql:163`；`xtask/src/migrate.rs:112-137` | 收回写权限，只留给 role_migration_owner；数小时 |
| SEC-4 | UDS 只有单向认证：客户端不校验服务端 uid，socket 没有目录和权限约束；抢占 socket 路径的本地进程可以伪造推理结果，写进 memory_rollups | `bins/consolidation-worker/src/inference_client.rs:96、196-216`；`bins/private-worker/src/inference_rpc.rs:131-134` | 客户端校验 `peer_cred`；socket 目录设 0750、文件设 0660；数小时 |
| SEC-6 | 未认证限流按完整 IP（IPv6 是 /128）做键，而且 rate_buckets 从不清理 | `crates/adapters/src/quota_repo.rs:460、514` | IPv6 按 /64 做键；加清理；数小时 |
| SEC-7 | `live_env.sh` 用 `set -a` 导入了约 25 个无关的第三方密钥；/Volumes/data 以 noowners 挂载，0600 权限不起作用 | `/Volumes/data/output/humaux-thread-stable-system-20260828/delivery-cards-20260903/live_env.sh:34` | 只按名字导出本项目用到的 2 个 key，放在强制属主的卷上；数小时 |
| DM-5 | 迁移 manifest 里的 pre/postcheck 从来没被执行过，其中 3 条根本不是合法 SQL（0068、0174、0175），而交付报告 §7.2 宣称 0174 的 precheck 会拦住违规数据 | `xtask/src/migrate.rs:67-68`；`xtask/src/migration_rehearsal.rs:46-49`；`migrations/0174_validate_evidence_reasoning_domain_fk.manifest.toml:24` | 修好这 3 条；让 migrate 真正执行检查；更正 §7.2；数小时 |
| DM-6 + OPS-9 | migrate 执行迁移文件和写入迁移记录分在两个事务里；不加 advisory lock，也不设 lock_timeout；runbook 没说迁移前要停 worker | `xtask/src/migrate.rs:142-166`；`docs/ops/runbook.md:18-27` | 合并为一个批次并加锁；数小时 |
| DM-7 | context_bindings 上有 3 个 NOT VALID 的 CHECK 一直没验证，而现有数据全部满足 | `migrations/0102_context_bindings_mode_scope.sql:33` | 写 0176 迁移做 VALIDATE，再加 SET NOT NULL；数小时 |
| OPS-6 | 例行运维动作只能通过 `cargo xtask` 执行（生产机上没有工具链），其中两项要用 `HUMAUX_TEST_PG_DSN`；confirm token 清理不在 runbook 里 | `xtask/src/confirm_sweep.rs:3-15、71`；`xtask/src/migrate.rs:18` | 移进 admin/maintenance 二进制；runbook 加定时任务表；数天 |
| OPS-7 | supervision.md 只写了约 125 个环境变量里的 4 个；两个 worker 的 DSN 变量名没有 HUMAUX_ 前缀；private-worker 不带参数启动时退出码为 0，Restart=always 会无限重启 | `docs/ops/supervision.md:137-138、169-177`；`bins/private-worker/src/main.rs:69-72、102` | 生成环境变量表；接受带前缀的别名；无参数时打印用法并退出 2；数小时 |
| OPS-8 | 网关的 `/readyz` 在 bootstrap 成功后恒为 200，不反映 retrieval socket、PG、Qdrant 的状态 | `bins/gateway/src/main.rs:58-70`；`bins/gateway/src/bootstrap.rs:231-236` | 缓存依赖状态，并做 `/status` 端点；数小时 |
| OPS-10 | DNS pin 是写死在环境变量里的静态 IP；供应商换 IP 后，distill 会一直处于"推迟"状态，没有告警 | `bins/private-worker/src/main.rs:243-249` | 写刷新流程；为"推迟"单独分类计数；数小时 |
| TH-2 | 一个 lane(c) 的退役原因是从旁边的测试抄来的，这个测试本身根本不涉及关系锁等待；结果 ADR-0032 依赖的唯一同键并发证人被停跑 | `crates/adapters/tests/operation_receipts.rs:748`；ADR-0032:29 | 恢复这个测试，或改成 lane(a)；数小时 |
| TH-3 | d5 号称验证"恰好一次"，但从没触发迟到 worker 的租约围栏；全仓库没有任何测试断言 `lost_lease` | `bins/private-worker/tests/distill_hop_e2e.rs:1561` | 加 d5b：用阻塞的 FakeProvider 构造迟到 worker；数小时 |
| TH-4 | soak 的 probes_green 看不见被 chaos 杀掉的进程；负载期间的失败率不评分；`ps` 失败时 RSS 记为 0 并判为通过 | `xtask/src/soak.rs:641-647、746-786、1197-1211` | 加失败率断言；按 RSS 斜率评分；1 天 |
| TH-5 | 演练脚本里跨租户和归档的断言只检查"没有出现"，调用失败时也会判通过；也从没试过恶意写法（B 用 A 的 workspace） | `docs/ops/rehearse.sh:390-423、505-514、680-682` | 先断言调用成功且非空；加恶意用例；数小时 |
| TH-6 | 交付报告 §1 有若干锚点指错：archived 断言挂错了测试；d4/d5/d6 的行号偏了 14–93 行；n=67 其实是票据数，不是 jobs 数 | `docs/ops/delivery_point_report.md:31`；`bins/gateway/tests/mcp_gateway.rs:6973` | 报告出稿时用 grep 重新生成锚点；数小时 |
| RQ-6 | embedding_version 和模型没有绑定，网关和 worker 各自读自己的环境变量；同维度换模型会把两个向量空间悄悄混在一起（违反你的约束 06907823 第 2 条） | `bins/gateway/src/recall.rs:291-300`；`bins/gateway/src/bootstrap.rs:471-473`；`bins/retrieval-worker/src/main.rs:198、360` | 在 worker 内用 (provider, model, revision, dim) 算指纹，网关比对不一致就拒绝；数小时 |

## 5. 检查过且没问题的

- **架构符合性：**
  - 角色集合正好是 §6.2.0 规定的 9 个，没有 superuser，也没有 bypassrls。
  - 运行时角色、batch_issuer、maintenance、admin 都没有 DELETE、TRUNCATE、TRIGGER、REFERENCES 权限；role_admin 只有 SELECT。
  - 83 个 SECURITY DEFINER 函数全部属于 role_migration_owner 并固定了 search_path。有 PUBLIC EXECUTE 的只有两个触发器函数和一个只返回计数的扫描函数。
  - 所有 `USING true` 策略都只作用于 role_migration_owner。
  - 迁移不可变：143 个文件对应 143 行记录，校验和漂移为 0（`xtask/src/migrate.rs:107-131`）。
  - ErrorCode 正好是 §52.1 的 18 个。
  - reqwest 只在 `crates/infra-network` 里使用；DNS pin 不能绕过禁止地址段检查。
  - A1 公式只有一处实现（`humaux_domain::ledger::a1_holds`）。
  - 4 条读路由都经过 serving selector。
  - RYW token 会和真实的 stream_log 行核对。
  - continuity 恒为 CANNOT_ESTABLISH，这是规范允许的。
  - PostgreSQL 18.6、Qdrant 1.19.0、MCP 协议 2026-07-28 都与 §70 一致。
  - Qdrant 的副本和一致性参数已配置（`crates/adapters/src/qdrant.rs:145-158`）。
- **安全：**
  - 所有 `set_config('humaux.*')` 都是事务内（is_local=true），没有 SET SESSION。
  - Bearer 用 `Hmac::verify_slice` 做常数时间比较；可信代理 CIDR 的处理正确。
  - workspace scope 只取交集（live ∩ bound ∩ requested）。
  - confirm token：32 字节 CSPRNG 随机数，只存 sha256；消费时把 tenant、user、op、target、successor、有效期全部绑定。
  - continuity_publish GUC 只对 role_migration_owner 生效。
  - UDS 服务端在解析请求体之前先检查对端凭据。
  - distill 的 prompt 把用户内容包进 JSON 信封，并附带"不可信数据"指令。
  - Qdrant 查询的唯一构造器会注入租户和可见性过滤。
  - 日志里不打印请求体、prompt 或 key。
  - `.env.local` 在 .gitignore 里，没有被跟踪；文档和证据目录里没有密钥字面量。
- **交付路径的正确性：**
  - remember 在单个事务里完成所有写入，token 在最后一次写入之后签发。
  - stream_seq 由行锁保证无间隙。
  - redeem 用 SKIP LOCKED，失败时整个事务回滚。
  - distill 的幂等由 lease_owner 围栏保证；release/fail 都带围栏。
  - `claim_derived_work` 在 UPDATE 里重述了资格条件，EvalPlanQual 下是安全的。
  - supersede/restore/correct 由仲裁 UPDATE 作为唯一判定者。
  - 投影状态最终收敛。
  - 读路径会在 PG 里复核 status、superseded_by 和正文哈希。
  - overlay 的可见性两侧都应用了。
  - 配额使用数据库时钟和 GREATEST 保护。
  - `advance_prefix` 单调递增。
  - 伪造 token 只会扩大 overlay 范围，结果仍经可见性过滤，不会泄露。
- **数据模型与迁移：**
  - 迁移集合完整，fnv1a 重算与记录一致。
  - 全新部署只依赖 plpgsql；uuidv7 是 PG18 内置的。
  - 0159 的外键和 0175 的 CHECK 已经 VALIDATE。
  - outbox 的 (tenant, commit_seq) 在线上没有重复。
  - 6,350 个 DEAD 任务是 `derived_dispatch_e2e` 测试的残留。
  - rate_buckets 按键有界。
  - confirm_tokens 有保留机制（迁移 0169）。
  - facet 生成列（0172）的设计合理。
  - disclosures、ledger、stream_log 等表的索引覆盖了各自的查询条件。
- **运维：**
  - 日志带 request_id 和错误类别，不带内容。
  - private-worker 的 key 通过间接变量名读取，Debug 输出是指纹。
  - SIGTERM/SIGINT 在第一次处理前就装好，排空窗口与 supervision.md 一致。
  - PG 重启时 distill 循环不会退出。
  - provider 故障走"推迟"路径，不直接把任务打成 DEAD。
  - 跨租户认领是原子的。
  - worker 的 `--readyz` 会说出缺了哪个对象。
  - 网关要求所有声明的环境变量键都存在，拼错会在启动时失败。
- **测试诚实度：**
  - testkit 的 `skip_or_fail` 在声明依赖时会 panic（`crates/testkit/src/lib.rs:163`）。
  - commit ack 丢失测试用了真实代理丢弃 CommandComplete。
  - membership scope 测试有并集回归的对照组。
  - memory.get 的授权与生命周期矩阵完整。
  - affect 的衰减算术断言精确（8200→2050）。
  - 3 个被标为 ignored 的 recall 测试在 serial lane 下带 `HUMAUX_REQUIRE_DB=1` 真的跑过（`gates_card24_housekeeping2.log:2905、3330-3332`）。
  - d4/d4b/d6 的终态断言正确；live d1 断言了 token 数 > 0。
  - soak 的 `ryw_*` 不会在 n=0 时空转通过。
  - kill-9 的解析失败按失败计。
  - Phase 9 的 4 个 lane(c) 退役原因是准确的。
  - rehearse.sh 有失败时 exit 1。
- **检索质量：**
  - dense 通道强制带租户和可见性过滤（`crates/projection/src/dense.rs:195`），placement 每次请求解析、不回退到其他租户。
  - PG 过滤层是所有排除条件的权威。
  - subject 和 affect 轴：Qdrant 预过滤是超集，PG 复核，两者一致。
  - 向量维度不匹配时明确失败（`bins/gateway/src/recall.rs:307-322`）。
  - 被取代或撤销的点会被删除。
  - 情绪重排是有界、稳定的排列（`recall.rs:482-508`）。
  - card 和 query 在同一进程内使用同一个模型和维度。
- **性能与规模：**
  - recall 在调用 DashScope 或 Qdrant 期间不占用 PG 事务或连接（`recall.rs:295-387`）。
  - Qdrant 的租户和 subject payload 索引已建（`xtask/src/e2e_seed.rs:529-547`）。
  - consolidation 的输入有上限。
  - 限流临界区用 2 s lock_timeout 串行化。
  - 墓碑的 NOT EXISTS 目前 0.36 ms。
  - GuardMetrics 的标签取自封闭枚举。

## 6. 提出后被否决的发现

- **SEC-1**（§74 身份表没有 RLS，6 个运行时角色可读）：两位复核者都否决了 P1。理由：这是规范 §6.2.1 `control.*=R` 的默认授权，§74 没要求更严；表是空的；没有写入授权，所以 §74 登录功能在现有授权下根本上不了线；要利用它得先攻破一个 worker。剩下的是 P2 加固项，已列在第 4 节。
- **C2**（load_evidence 用推理域 owner 的身份读，看不到其他用户的 Evidence）：前提不成立。迁移 0145 第 78 行把 permissive 策略 `evidence_objects_retrieval_worker_read`（只按租户判断）授给了 role_private_worker，多个 permissive 策略之间是 OR，所以能读到。
- **C4**（投影遇到瞬时错误就判 FAILED，而且没有重试边）：这是规范 §15.2.1（Baseline:3884）和 ADR-0042 D-K 已接受的设计，D-K 还明写了"退役后 recall 永久不可见"。剩下的是交付报告 §6 写得太简，已并入第 4 节"§6 缺项"。
- **C5**（治理操作只能用于 bootstrap 的 (tenant, workspace)）：ADR-0031 D-B 和 ADR-0032 D-A 明确接受，标为"下一张卡"。剩下的同样是 §6 缺项。
- **部分否决或降级：**
  - OPS-3：提前进入 DEAD 这一点是 ADR-0036 已接受的终态设计。降为 P2。
  - RQ-5：这是报告写错归因，不是运行时缺陷。降为 P2。
  - RQ-2：复核者按规范降为 P2，本报告按你的偏好保留在第 3 节。
  - DM-1：一位复核者判为不成立（今天只要毫秒级）。
  - RQ-3：一位复核者判为不成立（§33 明确接受该错误码）。
  - PERF-5：一位复核者判为不成立（Tenant Fair Scheduler 被规范延后）。
  - ARCH-3：一位复核者降为 P2，其依据的 LOST 巡检在交付系统里不运行，见 P1-8。
- **其他被纠正的子论点：**
  - RQ-4 说"ADR-0017 允许自动首次激活"：错。ADR-0017 否决了自动切 serving。
  - C3 的子论点 (b) 和"correct 后 M1 的点不退役"：没有得到验证。
  - PERF-4 的"饿死其他租户"：说重了。
  - inventory 说"artifact/code/coordinate 共 27 个操作未实现是 P1"：ADR-0044 D-A 已接受。
  - 任务说明写的"8 个冻结角色"：按 §6.2.0 实际是 9 个。

## 7. 本次审计的边界

- **全程只读：** 没有改文件；没有跑 cargo build、test 或 clippy；没有启停进程，也没有动容器。只对 humaux_thread_dev 执行了 SELECT 和 EXPLAIN。没有做负载实验，所以所有容量、延迟外推（例如 170–280 req/s、5 万条时 ≥10 s、73 分钟）都是估算。
- **数据来源：** 开发库里混有大量测试残留（2,245 个租户有记忆），DEAD、ISSUED 等计数只能说明机制存在，不代表生产中的发生率。
- **复核覆盖：** 第 3 节每一项都经过两位独立复核。第 4 节除了 OPS-3 和 RQ-5，都只有单个 lens 的证据，没做第二轮复核。
- **抽样的部分：**
  - 测试诚实度只抽了交付报告 §1.1–1.9 里的 16 行。
  - 878 处 `.unwrap/.expect` 只按 crate 计数，并检查了测试模块边界，没有逐一追调用图。
  - 105 条 `ponytail:` 注释和 148 条 not_applicable gate 分支只做了清点，没有逐条评估。例如 `distill_repo.rs:508` 自己注明的"毒行无限重试"没有复核。
  - 日志泄露只用同一行里的变量名做了 grep。
  - 环境变量文档的检查只到"是否写出变量名"为止。
- **没有覆盖的面：** public-worker 和 Phase 9 公共平面、§74 Email/控制台认证、admin plane、K8s 部署档位、§30 协同状态模型（全仓零引用，按 ADR-0044 属于只有声明的部分）。
- **本次综合自己核过的事实：** HEAD 72bdfa7 和工作区状态；ADR 列表；Baseline 头部和 §11.2.2；ADR-0008 的正文；`PHASE`=9；rustc 1.97.1；主要依赖的版本；SECURITY DEFINER 数（83）、LOGIN 角色数（9）、RLS 表数（130/130 FORCE）、迁移数（143，止于 0175）；`sweep_lost` 没有调用方（ADR-0043:329）。
- **引用的 Humaux 记忆：** 06907823、b9e3c2b0（本地 reranker 的偏好）；9a7c56a7（2026-08-30 验收目标）；710a2548（已否决"用 token 签名代替授权"，与 ARCH-9 的修法不冲突）。

（本报告摘要已存入 Humaux 记忆：「[research] Humaux Thread 系统体检 @72bdfa7（2026-09-26）：0 P0 / 17 P1 / 约28 P2」）
