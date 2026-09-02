# ADR-0008: Baseline 2.10 必须外科手术式合并，禁止整体覆盖 2.9

日期：2026-09-01 · 状态：Accepted · 影响面：`docs/architecture/Baseline_2.9.md`（仓库 canonical）/ §6.2.2 / §70.5 / §11.2.x / §12.6 / §72.2.1 / §14 / DOD 与 G80 编号空间

## 背景

用户提供了 `Humaux_Thread_Architecture_Baseline_2.10_Reasoning_Routing_Agent_Capture_Canonical.md`
（16430 行），带来真实增量：Reasoning Provider Plane（§11.1–11.11）、Provider Control
Foundation（§19.3–19.7）、Credential/Payer 三轴隔离（§7.1.1–7.1.3）、Agent Capture
Integrity Contract（§34.3.x）、Phase 9 实现边界（§12.6）。

**但 2.10 不是从仓库当前 canonical（2.9，16348 行）扩写的**——它源自一个更早的分支。
差量分析（2026-09-01）逐章节做正文相似度比对后确认：若按「新版本覆盖旧版本」的直觉
合入，会**删除已经落地并被代码引用的规范内容**。

## 决定

### 1. 禁止整体覆盖；只做增量式外科合并

以仓库 2.9 为底，**逐条**把 2.10 的新增内容合进去。任何一次合入若会让 2.9 的既有段落
变短或消失，必须先证明该内容已被 2.10 的其它位置吸收（给出行号），否则保留 2.9 原文。

### 2. 三处必须保留 2.9 原文（2.10 版本是过期分支）

| § | 保留理由（实测证据） |
|---|---|
| **6.2.2** 表级授权 GRANT 矩阵 | 2.9 含约 60 张表（含全部 `control.reasoning_route_*` / `control.provider_*` / Phase 9 匿名公共表）；2.10 只剩 14 张的旧矩阵。覆盖 = 删掉几十张**已实现**表的授权声明。做法：以 2.9 为底，仅追加本轮 9 张新表的逐角色 GRANT。 |
| **70.5** Public Source 核心 Schema | 2.9 含 0120 迁移的物理表映射、`G70-1 Active Anonymous Safe Topology` 门禁全文、0135 legacy receipt sealing ACL；2.10 只有一段简化 DDL，缺 `anonymous_source_id` / `lineage_mode`，与线上 schema 不符。 |
| **14** Transactional Outbox | 2.9 含 `ops.outbox` 四类互斥事件行的判据段；2.10 该段被裁剪。与本轮主题无关的意外丢失，不采用。 |

### 3. 编号冲突：2.10 的新内容一律另起编号，不得顶替旧含义

2.10 复用了 2.9 已被**代码和迁移引用**的 ID。已实测的引用点：

- `DOD-054`：`crates/testkit/src/dod.rs:244` 有真实 verifier（assessed anonymous
  lifecycle / revocation fails closed），语义 = 2.9 的「匿名 direct-provenance root」，
  **不是** 2.10 的「source closure 回到 PublicSource」。
- `G80-45` / `G70-1`：`migrations/0134_phase9_anonymous_trust_boundary.sql:1` 与
  `migrations/0135_phase9_legacy_trust_trigger_acl.sql:1` 的文件头部直接引用。
- `DOD-095`（2.9 = Project Continuity authority/completeness，phase=8）、
  `G80-44`（2.9 = R4 fault manifest closure）、`G80-46`（2.9 = Project Continuity
  authority closure）同理。

**规则**：2.10 的 Reasoning route isolation、Agent capture integrity、Phase 9 vertical
closure 等新判据，分配**全新编号**（DOD-099+ / G80-47+），旧编号原样留给现有含义。
理由：编号是审计链的锚，迁移文件头部的注释与 testkit 的 verifier 登记都靠它；改含义
= 审计链静默漂移（本仓已有「注错只活在注释里」「改名绕过闸」两类同型教训）。

### 4. 2.9 独有、2.10 已删除的段落：默认保留，逐条确认

`11.2.5.1`(R4)、`25.3.1`(Project Continuity Authority)、`12.1.1`、`17.6.1`、`34.0.1`、
`55.3.2`(ADR-0007 分段声明)、`73.5.1` 在 2.10 中全文消失且未在别处找到继承。**默认保留
2.9 原文**——尤其 `55.3.2` 是本仓 ADR-0007 刚冻结的制度，`25.3.1` 挂着 DOD-095/G25-2。

## 合入优先级

**阻塞 Phase 9（先做）**：① 编号冲突消解 ② §6.2.2 增量追加 9 张表 ③ §1.3 给
`private.processing_runs` 补 6 个版本化列（route_policy_id/version、route_decision_id、
credential_authority、billing_responsibility、billing_instrument_snapshot_id——当前零实现）
④ §19.3/19.5 的 9 张新表建迁移 ⑤ §12.6 + DOD-096 + G12-1（Phase 9 垂直闭合，重编号后）。

**可后置**：§34.3 全套 Agent Capture（DoD 自标 phase=15，仓库零实现，不阻塞 Phase 9）、
§7.1.2 TENANT_REASONING（自标 Phase 17 feature-gated）、DOD-098（phase=14）、
§19.0 定价细化、§19.6 除 admission 外的子模块。

## 后果

- 合入是一系列小 PR，不是一次性替换；每次合入都要能回答「2.9 有什么被删了、为什么允许」。
- 9 张新表 + 6 个新列是 Phase 9 Router/成本预留/账单对账的实现前提，属最大空白。
- `ReasoningExecutionProfile`（2.10 命名）与代码中的 `UserReasoningProfile` 尚未统一，
  实现前须定名，避免两套并行命名。

## 附录 A（2026-09-02）：① 编号冲突实测清单与映射

对两份文档做全量 ID 盘点（scratch 产物：`baseline_2_10_work/id_remap.tsv`）：共享 186 个
（95 DOD + 91 G*），其中 **6 个语义冲突**，代码/迁移/xtask 钉住的均为 2.9 含义：

| 2.10 旧 ID | 新 ID | 2.9 含义（保留） | 钉住它的位置 |
|---|---|---|---|
| DOD-054 | DOD-099 | 匿名 direct-provenance root | `crates/testkit/src/dod.rs:244` |
| DOD-095 | DOD-100 | Project Continuity authority/completeness (phase 8) | `xtask/src/contract_impact.rs` |
| G80-44 | G80-47 | R4 fault manifest closure | `xtask/src/gate_registry.rs:1374-1391` |
| G80-45 | G80-48 | Phase 9 anonymous trust boundary | `migrations/0134_*.sql:1` |
| G80-46 | G80-49 | Project Continuity authority closure | `migrations/0135_*.sql:1` |
| G11-3 | G11-5 | R4 fault manifest closure 的家章锚 | 同 G80-44 行；**本 ADR 原文未列** |

2.9 独有且必须保留：`G25-2`、`G70-1`。2.10 独有（合入时按新号进入）：`DOD-096/097/098`、
`G11-4`、`G12-1`、`G34-1`、`G80-3A`。无真实悬空引用。
