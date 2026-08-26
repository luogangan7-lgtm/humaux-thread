# ADR-0005 — CI 里「跳过」不再等于「通过」：声明式 fail-closed

- 状态：Accepted
- 日期：2026-08-27
- 关联：§79.2（三态 DB fixture 契约）、§57.1（三态 pass/fail/not_applicable）、§80.1（准入条件）
- 触发：相位闸族审计（49 个 agent，43 条原始发现经对抗验证后 4 条站住，本条是其中最严重的一条）

## 事实

`.github/workflows/ci.yml` 从建仓起就没有配过任何 `services:`，也没有设过
`HUMAUX_TEST_PG_DSN`。于是每一次 CI 运行里：

```
无 DSN 跑全 workspace → 99 个测试打印可见 SKIP → cargo test --workspace 报绿
仅 crates/adapters 一个包就占 92 个
```

被跳掉的是这些：RLS 租户隔离、`SELECT ... FOR UPDATE SKIP LOCKED` 并发认领、
fencing token、outbox 原子性、跨租户不可见性、read-your-writes、consolidation snapshot
隔离、Qdrant 契约往返。**恰好是「只有真库能证明」的那一类**（§79.2 自己列的清单）。

`cargo test --workspace` 一路绿，绿得毫无信息量。§79.2 白纸黑字写着「跳过不等于通过」，
而 CI 正好把跳过当成了通过。

## 为什么补 `services:` 不算修好

补上 Postgres/Qdrant service 只解决了**这一次**。SKIP 分支本身没有错——本机没起依赖时
不该红——但它有一个致命副作用：

> **没有被测对象时，它长得和通过一模一样。**

所以哪天有人为了让 CI 快一点删掉那段 YAML，CI 会**悄悄**退回静默跳过，不会有任何东西红。
三个月后才有人发现 RLS 从来没被测过。这正是 §80.1 说的「永远不会红的闸不算闸」，只是发生
在测试侧而非闸侧。

## 决定

**由「声明」兜底，而不是由「配置存在」兜底。**

`crates/testkit/src/lib.rs` 新增 `skip_or_fail(test_name, missing_object, required_when)`，
它是全 workspace **跳过与失败分界的唯一判定点**：

- `required_when` 命名的环境变量置为 `1` ⇒ 缺席即 **panic**；
- 否则打印可见 SKIP 后正常返回（本机行为逐字不变）。

CI 在 job 级设 `HUMAUX_REQUIRE_DB=1` / `HUMAUX_REQUIRE_QDRANT=1`。含义是「本次运行**声明**
自己有这两个依赖」。此后任何一个 fixture 拿不到连接，都不是「这条测试不适用」，而是**环境
坏了**，必须响。删 services 段的那次提交当场变红。

每个外部依赖用**各自**的变量，不合成一个开关：它们是独立服务，合并会让「只起了 Postgres」
的环境被迫在 Qdrant 测试上变红。同理 `HUMAUX_REQUIRE_DASHSCOPE` 覆盖 live egress 冒烟——
本会话吃过亏，那条测试到底跑没跑过只能靠人翻输出确认。

### 唯一判定点，不是一条约定

原先每个测试文件各自手写 `eprintln!("SKIP …"); return;`。散落的跳过点没法统一声明，也就
没法统一兜底——**这正是它能烂三个月没人发现的结构原因**。因此本次把它们全部改为调用
`skip_or_fail`：`adapters/src/postgres.rs`（角色缺失 ×2、表缺失 ×1）、
`adapters/tests/tenant_placements_migration.rs`、`adapters/tests/recall_envelope_g23.rs`
（PG + Qdrant 两处）、`adapters/tests/qdrant_live.rs`、
`retrieval-provider/tests/dashscope_live_smoke.rs`、以及 `run_db_fixture` 自身。

留一处未改：`projection/tests/no_handwritten_filter_scan.rs` 跳的是「找不到 workspace 根」
这个**内部不变量**（非外部依赖），且在任何 cargo 调用下都不可达。

## 实测（三态，逐条跑过）

| 条件 | 结果 |
|---|---|
| 有 DSN + `HUMAUX_REQUIRE_DB=1` | 绿，`1 passed` |
| 无 DSN + `HUMAUX_REQUIRE_DB=1` | **红**：`HUMAUX_REQUIRE_DB is set, so a skip here is a failure — no database URL configured` |
| 无 DSN、无声明 | SKIP，与从前逐字相同 |

CI 可行性也实测过：真·全新 `postgres:18` 集群（`docker run` 起的干净容器，非本机开发库）
68 个迁移干净应用——8 个 DB 角色由 `migrations/0011_roles_and_grants.sql` 的
`CREATE ROLE IF-NOT-EXISTS` 块自建，无需预置——随后 `rls-check` 8/8 pass，
`auth_scope_rls` 实跑通过而非 SKIP。

## 同批附带：六道闸接进 CI

`metrics-registry` / `gate-registry` / `threshold-shape` / `direction-table` /
`card-version` / `rls-check` 此前一道都没接进 CI。它们本地全绿，但**本地绿不是判据**——
没在 CI 跑过的闸等于没有人在看。

`dod-check` 与 `benchset-declaration` **刻意没接**，理由写在 `ci.yml` 里：两道都带相位参数
且默认 0，默认值下豁免一切；按真实相位跑当前是红的（40 条 DoD 无验证器登记 / 4 个 benchmark
集合已到期仍未声明）。接的前提是先补缺口，**不是把相位调低到让它们变绿**——那正是 §80.1.2
批评已退休 G80-21 时说的「调松定义」。

## 可推广的判据

**一个「不适用」分支，如果它在被测对象缺席时的外观与「通过」无法区分，它就会腐烂。**
对策不是靠人记得配环境，而是让**调用方声明自己的环境**，声明与现实不符时立刻响。

## 后续：让测试真跑起来之后立刻浮出的第一个 flake

本 ADR 落地当天，全量跑（声明模式）就红了一次：`disclosure_ledger::
source_row_accepts_matching_single_id` 报 PostgreSQL 死锁 `40P01`，两个 relation 正是
`ops.data_disclosures` 与 `ops.data_disclosure_sources`。

根因链：
1. §7.4/§77 的 append-only 守卫要用 `TRUNCATE … CASCADE` 才触发得到——不带 CASCADE 时
   PostgreSQL 更早地以「cannot truncate a table referenced in a foreign key constraint」
   拒绝，守卫触发器根本不会 fire，测试就断言不到 §7.4（两种形式都实跑核对过）。
2. **PostgreSQL 先拿 `AccessExclusiveLock`，再触发 trigger。** 即便 TRUNCATE 最终被拒，
   锁已经拿到手。
3. CASCADE 按自己的顺序锁上述两张表，与并发读者的加锁顺序交叉即成环。碰这两张表的测试
   跨了两个 binary（`adapters/tests/disclosure_ledger.rs` 与
   `retrieval-provider/tests/dashscope_live_smoke.rs`），而 cargo 并行跑 binary。

**这个 flake 一直都在，只是以前无害**——CI 从不跑这些测试。本 ADR 让它们真跑之后，它就成了
一个会随机红的 CI。这本身是本 ADR 有效的一个侧证：**看不见的东西不会自己变好，只是不被看见。**

修法（最小序列化，不是全局串行）：`humaux_testkit::DISCLOSURE_LEDGER_ADVISORY_LOCK` 定义
唯一锁键，碰这两张表的 fixture 建连后取 `pg_advisory_lock_shared`（读者之间照旧并发），
TRUNCATE 那条先 `pg_advisory_unlock_shared` 再 `pg_advisory_lock` 升排他。顺序不能反——
同一 session 持共享时申请排他会自己阻塞自己。session 级锁，连接 drop 即释放，测试 panic
也不漏锁。

锁键定义在 testkit 一处而不是各文件各写一个数字：两个 binary 跨 crate，写两处迟早改漏，
那时序列化静默失效、只剩偶发红——又是一个「失效时与正常态不可区分」的形状。
