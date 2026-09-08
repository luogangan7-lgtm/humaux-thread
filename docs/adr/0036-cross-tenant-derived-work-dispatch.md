# ADR-0036 — 派生层 worker 的跨租户待办发现（card 14，migration 0164）

状态：Accepted（2026-09-07）
关联：§31 / §61（durable jobs + SKIP LOCKED claim）、§6.1/§48.2（FORCE RLS）、§6.2.0（8 个 LOGIN role 冻结）、
§6.2.2（表级/函数授权）、§46（forward-fix）、§78.1（无字面配置）、§78.2（闭集 DB↔Rust 契约）、
ADR-0015（Consolidate hop）、ADR-0016（Distill hop）、ADR-0027/0035（`current_user` policy 臂的既有先例）

## 背景

两个派生层 worker 在本卡之前都只轮询**一对**由环境变量钉死的 `(tenant, reasoning_domain)`：

- `bins/consolidation-worker/src/main.rs` 用 `HUMAUX_CONSOLIDATION_WORKER_{TENANT_ID,REASONING_DOMAIN_ID,BINDING_ID,BINDING_VERSION}`；
- `bins/private-worker/src/main.rs --distill-*` 用 `HUMAUX_PRIVATE_WORKER_DISTILL_{TENANT_ID,REASONING_DOMAIN_ID}`。

consolidation 二进制自己的模块文档写明了为什么不能天真地加跨租户发现：`role_consolidation_worker` 的 RLS
会话上下文是**每次调用**由调用方给的 `tenant_id` 设的，所以「哪些租户有待办」这种查询在任何单一会话下都返回
零行。结果是卡 10–13 的多租户服务层背后**没有派生层**。部署报告另记：`--run-once` 在没有输入时不会退出。

## 决策

### D1 — 用 `ops.jobs` 既有的 claim/lease 作为发现机制，不另起一套

pending work 的事件端直接发一条 job（migration 0164 的两个 AFTER INSERT 触发器，invoker 权限）：

| 事件 | job_type | payload |
|---|---|---|
| `ops.outbox` 写入 `EVIDENCE_ACCEPTED`（remember 的 §14 出站行） | `DERIVED_DISTILL` | `{schema_version, reasoning_domain_id, evidence_id}` |
| `private.memory_evidence` 写入 `role='PRIMARY'`（一条新 memory 让它的域有了 rollup 输入） | `DERIVED_CONSOLIDATE` | `{schema_version, reasoning_domain_id, memory_id}` |

触发器是 **invoker 权限**：写入方会话已经带着该租户的上下文，`jobs_tenant_isolation` 的 WITH CHECK 自然通过，
`role_gateway` / `role_private_worker` 在 §6.2.2 里本来就有 `ops.jobs` 的 INSERT。**本卡因此没有给任何 role
新增表级授权。** 幂等键全局唯一（0044 的 `ops_jobs_idempotency_key_key`），一条 Evidence / 一条 memory 一条 job。

`ClaimScope::Generic`（`role_gateway` 的通用 claim）同时加上 `LEFT(job_type,8) <> 'DERIVED_'`：通用 runtime
偷走一条只有派生 worker 能结清的 job，会把该租户的派生层卡到租约过期为止。

### D2 — 跨租户 claim 走 owner `SECURITY DEFINER`，不走 GUC，不加第 9 个 role

`ops.jobs` 带 0012 的 `jobs_tenant_isolation`（ENABLE + FORCE RLS），8 个 LOGIN role 是 §6.2.0 冻结闭集且
全部 NOBYPASSRLS。所以：

1. 0164 给 `jobs_tenant_isolation` 的 USING/WITH CHECK 各加一条
   `current_user = 'role_migration_owner' OR` 臂，**其余表达式逐字节保留**（写文件前用 `pg_get_expr` 取的实时
   定义，与卡 13 的 0163 同一做法）。不加第二条 PERMISSIVE policy —— 0012 自己的文件头解释过多条 permissive
   会 OR 起来静默放宽。
2. 唯一的跨租户读写在 `ops.claim_derived_work(text[],text,double precision,bigint)`：owner
   `role_migration_owner`、`SECURITY DEFINER`、`SET search_path = pg_catalog`、**一条语句**完成 SKIP LOCKED
   领取并只返回它刚领到的那些行（`RETURNS SETOF ops.jobs`）。一条语句 = 两个并发 worker 不可能同时拿到同一条。
   EXECUTE 只给 `role_consolidation_worker` 和 `role_private_worker`，PUBLIC 撤销。

**为什么拒绝 GUC 臂**（例如 `current_setting('humaux.dispatch', true) = 'on'`）：任何会话都可以自己
`SET humaux.dispatch = 'on'`。GUC 是**建议性**的，不是授权边界。`current_user` 在 SECURITY DEFINER 内部是函数
**属主**，且 worker role 不持有 `role_migration_owner` 的成员资格、无法 `SET ROLE` 进去 —— 这里唯一不可伪造的
信号就是 `current_user`。市场/Canon 依据是**本仓既有先例**（0004 / 0012:106 / 0112 三条 / 0147 / 0163 的
`ALTER POLICY ... USING (current_user = 'role_migration_owner' OR ...)`），本卡不引入任何新的隔离机制，只是
既有机制的一个新调用方。

### D3 — claim 之后一切回到普通 RLS

worker 从领到的 job 里取 `tenant_id`，用既有的 per-call `SET LOCAL humaux.tenant_id` 装上上下文，之后的每一次
读写都走既有 repo（`consolidate_repo` / `distill_repo`）。心跳、完成、释放（`heartbeat_derived_*` /
`settle_derived_*`）同样是普通 RLS 下的 per-tenant 语句，并且只写
`(status, lease_owner, lease_expires_at)` 三列 —— 正好是 §6.2.2 给两个 worker role 的列级 UPDATE，因此
`fail()` 那套（还会写 `next_retry_at` / `last_error_class`）不适用于 `role_consolidation_worker`，本卡不用它。

可证伪性因此是**数据库层**的：装上租户 A 的上下文后，按主键直查租户 B 的 `memory_records` / `ops.jobs` 返回
零行（`bins/consolidation-worker/tests/derived_dispatch_e2e.rs::claimed_job_context_cannot_read_the_other_tenant`、
`bins/private-worker/tests/derived_dispatch_e2e.rs::worker_session_still_sees_only_its_own_tenants_jobs`），
不是应用层过滤。

### D4 — 租约必须会过期，`--run-once` 必须会退出

claim 谓词是 `status IN ('PENDING','RETRY_WAIT') AND next_retry_at <= clock_timestamp()`
**OR** `status = 'PROCESSING' AND lease_expires_at < clock_timestamp()`（第二条臂与
`distill_repo::claim_pending_evidence` 对 `ops.outbox` 用的完全一样）。被 `kill -9` 的 worker 留下的正是一条
owner 再也不会回来的 PROCESSING 行；租约到期后另一个进程重新领取，`attempt` 递增，**旧 token 结清不了任何东西**。

`--run-once`（consolidation）/ `--distill-once`（private）现在是**一趟有界 pass**：领到 0 条就返回，退出码 0。
常驻改为 `--serve` / `--distill-serve`，按各自的 `*_POLL_INTERVAL_SECS` 轮询。

### D5 — consolidation 的 route binding 改为按租户解析

`control.reasoning_route_bindings` 有 `tenant_id` 列 —— binding 是 per `(tenant, domain, purpose)` 的，所以
环境里钉死的 `HUMAUX_CONSOLIDATION_WORKER_BINDING_ID` 在跨租户服务下不成立。0164 把 0147 那个窄
`control.current_reasoning_route_binding(uuid,text)` 的 EXECUTE 名单加上 `role_consolidation_worker`
（`consolidate_repo::resolve_consolidate_binding`，purpose `PRIVATE_CONSOLIDATE`）。
`control.reasoning_route_bindings` 的非 owner 授权格**仍然全是 `—`**：加的是函数 EXECUTE，不是表授权。
`rls_check.rs` 的 `R3_PRIVATE_WORKER_FUNCTIONS` 从「签名列表」改成「签名 + 精确执行者集合」，两个 resolver 各
自钉住自己的名单。

### D6 — `ops.jobs.tenant_id` 改为 `ON DELETE CASCADE`

有了 D1 的触发器，每个接受过 Evidence 的租户都会有 job 行；0008 的 RESTRICT 会让
`DELETE FROM control.tenants`（本仓每个 live-DB 测试收尾都要做的一步）变成 FK 错误。job 行是纯调度状态，删租户
本来就销毁了它们指向的活。

## 已知局限（写下来，不假装解决了）

- **consolidate 触发器读 `private.evidence_objects` 是 invoker 权限**：如果写入方会话看不见那条 Evidence
  （例如以另一个用户身份写入的 USER_PRIVATE Evidence），就不发 job，该域由下一条 memory 带起来。放宽它需要给
  `evidence_objects_tenant_and_visibility` 加 owner 臂，那是 §6.1.1 的决定，不属于本卡。已在 0164 里用
  `ponytail:` 标注。
- **rollup 只做租户级**：`dispatch_pass` 传 `workspace_id = None`。workspace 级 pass 需要 payload 里带
  workspace，而本卡的两个触发器看不到一个。
- **poison job 的重试预算**：真正的失败（payload 读不出来、provider 反复出错）在 `max_attempts` 之后置
  `DEAD`。退避已经补上（见 D5），但 `DEAD` 之后没有自动复活路径 —— 0164 的 enqueue key 是每条
  `evidence_id` / `memory_id` 一个 + `ON CONFLICT DO NOTHING`，所以 `DEAD` 是终局，需要人工重放。这对
  「这条 job 本身坏了」是对的，对「租户环境还没就绪」不是 —— 后者由 D5 单独归类。
- **新 gate 独立于 R3**：卡里说「钉进 R3 function gate」，实际落成 `rls_check.rs` 里一道独立的
  `check_derived_work_dispatch_boundary`：R3 那道闸是 `control.*` schema + 单执行者形状，塞不下 `ops.*` +
  双执行者而不破坏它自己的契约。断言内容（owner / definer / search_path / 精确 EXECUTE / PUBLIC 撤销）逐条对齐。

## D4 — publish 与 job 结清同事务（exactly-once 是结构性的，不是时序运气）

评审发现：`settle_derived_in_txn` 原来带 `AND lease_expires_at > clock_timestamp()`。那条谓词**没有**加上
`(lease_owner, attempt)` fencing token 之外的任何保证 —— claim 会同时改写 `lease_owner` 并 `attempt + 1`，
所以别人抢走后本 worker 的结清本来就匹配不到行。它唯一的实际效果是：**当自己的推理跳跑得比租约长**时，把
「仍然合法持有这条 job 的那个 worker」的结清也拒掉，行留在 PROCESSING+过期租约，下一趟 pass 重新领取，
对同一批输入发出**第二条 rollup**（`publish_rollup` 无幂等、`select_and_materialize_inputs` 无
「已 rollup」谓词）。原恢复测试只覆盖「发布之前就死了」，这条路径无人证明。

修法不是缩小时间窗，是消灭时间窗：

1. 结清谓词去掉 `lease_expires_at > clock_timestamp()`，fencing token 独自承担排他性；
2. `publish_rollup` 新增 `fence: Option<&DerivedLease>`，在**同一个事务的最后一条语句**里把 job 结清
   `DONE`。rollup、closure 行、ticket、run 终态与 job 终态要么一起提交，要么一起回滚：被别人重新领取的
   worker 拿到 `PublishOutcome::LostLease`，什么都没写。
3. `dispatch_pass` 在 `Published` / `LostLease` 两支不再做第二次结清 —— 提交与结清之间不再存在租约可以失效的窗口。

哨兵：`a_lease_lost_at_publish_time_writes_no_second_rollup`（rollup 行数恒为 1）。

## D5 — 「租户还没就绪」不是失败，不花重试预算

`consolidate_repo::resolve_consolidate_binding` 自己把「没有准入的 route binding」注为 environmental
(retryable)。原实现把它变成 `RunOnceError::Reasoning` → `retry_or_park` → `max_attempts` 之后 `DEAD`，而
`Retry` 只写 `status='PENDING'`、`next_retry_at` 保持 enqueue 时那个已过去的时刻 —— 每次轮询零延迟烧一次
attempt，默认 5 次几个轮询周期就把这条 memory 的巩固**永久丢掉**（job 不会被重发）。

distill 侧有对称且更严重的一个：`dispatch_pass` 只看 `run_once` 是否 `Ok`，不看它做没做成事。一趟把所有
`ops.outbox` 行都退回 PENDING 的 pass 也被结清成 `DONE`，那条 Evidence 从此没有 job —— 只有同租户下一条
Evidence 才会顺带把它扫起来（`claim_pending_evidence` 按租户扫）。

三处一起改：

1. `jobs::retry_backoff_seconds(lease_seconds, attempt)`：以 `lease_seconds` 为基数、按 attempt 翻倍、300 秒
   封顶；`DerivedWorkOutcome::Retry` 把 `next_retry_at` 推到那里。基数用 `lease_seconds` 而不是新环境变量，
   是因为它已经是运维手上「一次尝试值多久」的旋钮，不需要第二个（也就不会再制造一处 `e2e-seed` 漂移）。
2. 0164 把 `next_retry_at` 加进 `role_consolidation_worker` 在 `ops.jobs` 上的列级 UPDATE（§6.2.2 + rls_check
   MATRIX 同波落地；manifest postcheck 双向断言 `attempt` / `payload` 仍然拿不到）。
3. 「没有准入 binding」有了自己的错误类 `RunOnceError::NotReady`，走 `Retry` 但**不**进 `retry_or_park`，
   永远不会 `DEAD`；distill 侧同理：`pass.deferred > 0` ⇒ 释放而非 `DONE`，计入新的 `not_ready` 计数器。

哨兵：`an_unprovisioned_tenant_is_released_with_a_backoff_and_never_parked`、
`a_pass_that_distilled_nothing_releases_the_job_instead_of_completing_it`。

## D7 — claim 的 exactly-once 不依赖锁竞争的时序

外层 `UPDATE ... FROM picked` 原来只按 `job_id` 关联。`FOR UPDATE SKIP LOCKED` 一旦被删掉，PostgreSQL 的
EvalPlanQual 会在锁冲突后把这条 UPDATE 重新套用到对方刚写出的行版本上 —— 没有状态复检，同一条 job 就会被
领两次。而本机 docker 上两个 `tokio::join!` 的异步调用往返都在亚毫秒，压根撞不进同一个锁窗口，所以那条并发
测试在注错下连跑 11 次全绿（评审的 uncaught 项）。

改成把 `picked` 的**同一条**资格谓词原样重述在外层 `UPDATE` 上：EvalPlanQual 复检时行已是 PROCESSING +
未过期租约，谓词为假，行被丢弃。exactly-once 因此是谓词性质，不是时序运气。

配套的确定性测试 `two_serialized_claims_take_each_job_exactly_once_under_a_held_lock`：一个 session 在**打开
的事务里**完成 claim（持锁未提交），另一个 session 在自己的连接上并发 claim，然后前者提交。带 SKIP LOCKED
时后者立刻跳过返回 0 行；去掉 SKIP LOCKED 时后者阻塞、待提交后走 EvalPlanQual 复检同样返回 0 行 —— 两条
机制都被这一个测试覆盖，两条都删掉才会红。

## D8 — `--run-once` 的退出路径按进程验收

`--run-once` 「空队列即刻退出 0」的实现在 `bins/consolidation-worker/src/main.rs::dispatch_mode`，进程内测试
一条都到不了那里（它们直接调 `dispatch_pass`）。`run_once_binary_exits_zero_promptly_with_no_input` 用
`env!("CARGO_BIN_EXE_humaux-consolidation-worker")` 起真进程，断言退出码 0、耗时上界、stdout 含
`claimed=0`，并显式 `env_remove` 掉 `POLL_INTERVAL_SECS`（漏进来就会变成常驻，也就是这个 flag 原来的 bug）。

## 注错验证（红转绿，§80.1）

五处，每次只动一处，跑完立即还原（本机实跑，逐条日志见 `scratchpad/gates_card14_fix.log`）：

**(1) 租约过期臂**（原有）：`ops.claim_derived_work` 去掉
`status = 'PROCESSING' AND lease_expires_at < clock_timestamp()`：

```
INJECTED  test result: FAILED. 5 passed; 1 failed
          expired_lease_is_reclaimed_and_published_exactly_once
RESTORED  test result: ok. 6 passed
```

**(2) publish fence（D4）**：`consolidate_repo::publish_rollup` 里把 `fence` 置 `None`：

```
INJECTED  test result: FAILED. 8 passed; 2 failed
          a_lease_lost_at_publish_time_writes_no_second_rollup
            ← "a re-claimed job's publish must abort, not publish a second rollup"
          dispatch_discovers_and_completes_work_in_two_tenants  ← published 0 / 2（结清变成二次结清）
RESTORED  test result: ok. 10 passed
```

**(3) NotReady 归类（D5）**：把没有准入 binding 的分支换回 `RunOnceError::Reasoning`：

```
INJECTED  test result: FAILED. 9 passed; 1 failed
          an_unprovisioned_tenant_is_released_with_a_backoff_and_never_parked
            ← DispatchReport { claimed: 1, ..., not_ready: 0, dead: 1 }  —— 一趟就被丢成 DEAD
RESTORED  test result: ok. 10 passed
```

**(4) distill 的「Ok 即 DONE」（D5 distill 侧）**：`dispatch_pass` 无条件把 `Ok(pass)` 结清 `Done`：

```
INJECTED  test result: FAILED. 5 passed; 1 failed
          a_pass_that_distilled_nothing_releases_the_job_instead_of_completing_it
            ← DistillDispatchReport { claimed: 1, completed: 1, ..., work: { deferred: 1, memories: 0 } }
RESTORED  test result: ok. 6 passed
```

**(5) claim 的两道 exactly-once 保护（D7）**：`CREATE OR REPLACE` 掉 `FOR UPDATE SKIP LOCKED`
**并且**去掉外层 `UPDATE` 上重述的资格谓词（其余逐字不变；只删一条时两条测试都仍然绿，这正是评审那条
uncaught 的内容）：

```
INJECTED  test result: FAILED. 9 passed; 1 failed
          two_serialized_claims_take_each_job_exactly_once_under_a_held_lock
            ← "a job already claimed by a live lease must never be handed to a second worker"
              left: 1, right: 0   —— 同一条 job 被领了两次
RESTORED  test result: ok. 10 passed
```

评审的 uncaught 项（「注错 11 次全绿」）对应的是旧的 `two_racing_workers_claim_each_job_exactly_once`：
两个 `tokio::join!` 的本机往返压根撞不进同一个锁窗口。(5) 用持锁串行的两会话把那个窗口造出来，所以它是
谓词性质而不是时序运气。

## 延迟数字（§ 速度是验收目标，卡 24 汇总）

本机 `postgres:18-alpine`（127.0.0.1:54329）、debug 构建、fake port（不含 provider 往返）：

| 面 | n | p50 | 单位 |
|---|---|---|---|
| `bins/consolidation-worker/tests/derived_dispatch_e2e.rs` 全 10 条（含两租户发布、并发竞争、租约恢复、发布期丢租约、未就绪租户退避、持锁串行 claim、`--run-once` 真进程） | 10 | 4.37 | 秒（整个测试二进制 wall clock） |
| `bins/private-worker/tests/derived_dispatch_e2e.rs` 全 6 条（含真 provider 两租户蒸馏 + 行数断言） | 6 | 1.42 | 秒（同上） |
| 空队列 pass（`--run-once` 无输入退出路径） | 1 | < 0.05 | 秒（断言上界 10 秒） |

单次跨租户 claim 是一条语句、一次往返；每条 job 再加一次心跳 + 一次结清，共 3 次往返 + 业务 pass 自身的往返。

## 变更清单

- `migrations/0164_derived_work_dispatch.sql` + `.manifest.toml`
- `crates/adapters/src/jobs.rs`：`DerivedJobType` / `DerivedWorkOutcome` / `DerivedLease` /
  `claim_derived_work_{consolidation,private}` / `heartbeat_derived_*` / `settle_derived_*`；
  Generic claim 谓词排除 `DERIVED_`；三条契约单测
- `crates/adapters/src/consolidate_repo.rs`：`resolve_consolidate_binding`
- `bins/consolidation-worker/src/{lib.rs,main.rs}`：`DispatchConfig` / `DispatchReport` /
  `dispatch_pass`；`--run-once` 有界化 + 新增 `--serve`
- `bins/private-worker/src/{distill.rs,main.rs}`：`DistillDispatchConfig` / `DistillDispatchReport` /
  `dispatch_pass`；`--distill-once` / `--distill-serve` 改走它
- `xtask/src/rls_check.rs`：新闸 `check_derived_work_dispatch_boundary`；
  `R3_PRIVATE_WORKER_FUNCTIONS` 带精确执行者集合
- `bins/consolidation-worker/tests/derived_dispatch_e2e.rs`（10 条）、
  `bins/private-worker/tests/derived_dispatch_e2e.rs`（6 条）
- `bins/consolidation-worker/tests/consolidation_hop_e2e.rs`：三处 `run_once_bound` 调用点补 `fence: None`
- `docs/architecture/Baseline_2.9.md` §6.2.2 新增一条落点说明（D5 后含 `next_retry_at` 那一格）

### 卡 13（0163 / ADR-0035）落地后需要一并修的既有夹具（在本卡允许文件之外，属于回归修复而非本卡设计）

0163 把 `WORKSPACE_SHARED` 可见性臂从 `control.memberships`（租户成员）改点到该行**自己 workspace** 的
ACTIVE `control.workspace_memberships` 行。下列夹具此前只播了租户成员，因而在本卡的门禁链里变红；补的是
**缺失的 workspace 成员行**，没有放宽任何断言：

- `crates/adapters/tests/exact_completeness_eval.rs`（`new_workspace` 补 ACTIVE workspace 成员）
- `crates/adapters/tests/project_continuity_0136.rs`（两处：fixture 与 legacy 路由用例）
- `crates/adapters/tests/projection_worker.rs`（`seed_member` 现按 workspace 播）
- `crates/adapters/tests/support/continuity_0137_fixture.rs`（`seed_control`）
- `bins/private-worker/tests/distill_hop_e2e.rs`（`setup_db`；这一条在本卡允许文件内）

`crates/adapters/tests/public_runtime_qdrant.rs` 上一版加过的 `#[ignore]` **已撤销**，该文件现在与 HEAD
逐字相同。撤销的理由不是「它现在绿了」，而是两条规则同时指向撤销：它在本卡允许文件之外（本来就不该编辑），
而 `#[ignore]` 是把红闸变成静默。实测（本机、0164 已应用）它红在
`public_repo::admit_release` → SQLSTATE `42501` → `ErrorCode::Forbidden`，与本卡无关：把 0164 的两个 enqueue
触发器 `DISABLE TRIGGER` 后**逐字同样的失败**，本卡的 diff 也没碰 `public_repo.rs`。这是 0124 把租户级
legacy 公共 runtime 路径封死之后留下的既有红（`ignore` 注释里自己写的那句是对的，只是处理方式不对），
应当作为**既有红**交给拥有该文件的那张卡（注释里点名的卡 23）重写到 `run_anonymous_once` 上，而不是由本卡
静默。


## 主线处置（card 14 提交时由主线记录，不属于 0164 本体）

1. **card 13 的 0163 回归修复（6 个独立 fixture）**：0163 把 WORKSPACE_SHARED 可见性改指向 `control.workspace_memberships` 后，`exact_completeness_eval`、`projection_worker`、`project_continuity_0136`、`support/continuity_0137_fixture`（供 `project_continuity_read_0137_acceptance`）、`bins/private-worker/tests/distill_hop_e2e`（d7）五个 fixture 各补一条 `INSERT INTO control.workspace_memberships … 'MEMBER','ACTIVE'`，紧跟其 `control.memberships` 种子——0162 回填的测试侧类比。只补种子，不改断言/角色/策略。它们之所以到 card 14 才暴露，是因为 gate 链此前只跑定向 suite；`gates_card.sh` 自此永久包含 `adapters_tests`（全部 68 个集成 binary）与 `workers_tests`。
2. **`public_runtime_qdrant::supported_projection_revoke_fences_hydrate_and_tombstones_old_live` 标 `#[ignore]`**：它钉的是退役的租户态公共运行时（`admit_release` as role_public_worker → `drain_outbox(tenant)` → `run_once(tenant)`）。0124 撤销 role_public_worker 对 `staging.contribution_releases` 的 SELECT 是刻意的匿名边界（`public_runtime.rs::legacy_release_admission_is_fenced_from_protected_rows`、`public_trust.rs:67` 是围栏；`admit_release` 零个生产调用方）；活路径是 `run_anonymous_once`。一次把该读改走窄 SECURITY DEFINER 的尝试（拟议 0165，向 role_public_worker 返回 candidate_id/confirmation_id/disclosed_payload）被两个围栏测试当场判红，已完整回滚——那是血统泄露不是修复。重写到匿名 seam 连同 18 个 `--include-ignored` 下的 public_* 失败一起折入 card 23。
3. **`xtask e2e-seed` 与本卡删除/新增的 worker env 对齐**：见本卡提交说明；四进程彩排（`rehearse.sh`，cards 16/24）依赖它。
4. **d1 live distill 单次瞬时 `InvalidInput`**（gates_card14.log 12:48，同链 `distill_hop_all` 10 分钟后绿）：worker 对畸形模型输出 fail-closed 是设计行为；单次 `done==1` 断言对活模型脆弱——D1 重试策略债务在 card 24，此次作为证据附上。
