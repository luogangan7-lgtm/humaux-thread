# ADR-0007: 分段（stage-scoped）benchmark 声明制度 + 量具单位统一（Hamming spread / 局部扰动 resolution）

日期：2026-08-27 · 状态：Accepted · 影响面：§45.2 / §55.3 / §69（声明表 + Continuation Gate）/ `xtask benchset-declaration`（G80-16）

## 背景与问题

两个评测工程问题在 Phase 8 清账中暴露，经外部调研（用户指定的 GPT 深度调研会话，
2026-08-27；引用依据：OWASP AI Exchange agentic/RAG 测试指南、NIST AI RMF MEASURE 1.1 +
TEVV、AgentPoison 的 ASR-r/ASR-a/ASR-t 分层、PoisonedRAG、Sakai bootstrap sensitivity /
IR discriminative power、NIST small-n range 统计）后裁决如下。

### 问题一：memory_security_lifecycle 的 owner-phase 矛盾

spec 把该集合 owner 定在 Phase 4+（早已到期，恒红），但四段里 Action 段的 SUT
（Context Renderer 分流 / privileged action probe / consolidate disposition taint）由
DOD-036 排在 phase=14 才实现——集合到期时四分之一被测对象不存在。这是 spec 的
capability-gating 错误（benchmark owner 写早了），不是测试逾期。

### 问题二：continuation_198_v2 的量具单位与第二系统选轴

- `resolution`（翻转题数）与 `spread_tol`（此前定义 = 每层 pass_items 极差）单位不一致：
  总分极差会被正负翻转互相抵消骗成假 0（两次同分但不同题翻转）。本仓已存档 baseline
  数据实测本批恰好无抵消（Hamming ≡ 极差），但作为定义 Hamming 是无损收紧。
- 第二系统扰动轴：换 embedding 量的是「两个产品的差距」（同时动几何/近邻/候选池）；
  收 top_k 改的是判定契约本身（改尺子刻度）；去 rerank 是粗粒度 feature ablation。
  都不是「量仪器分辨率」。

## 决定

### 1. 分段声明制度（scoped stage declaration），仅适用于显式声明了 stage 结构的集合

`memory_security_lifecycle` 的声明单位从「整集合」改为「stage」：

```text
stage_status ∈ { DECLARED, DUE_UNDECLARED, BLOCKED_ON_SUT }

BLOCKED_ON_SUT 的合法性条件（缺一即视同 DUE_UNDECLARED）：
  - 必须点名缺失的 sut_capability（可探测对象，ADR-0006 纪律）
  - 必须点名 unblock_phase（= 该 SUT 的 DoD phase）
  - 禁止携带任何七字段实测值（不许填 0 / N/A / 估值）
  - current_phase >= unblock_phase 时自动转 DUE_UNDECLARED（checker 强制，红）

汇总禁令（invariant）：
  任一 required stage != DECLARED
    ⇒ lifecycle_complete = false
    ⇒ 该集合不得表述为「已通过安全评测」，aggregate 安全判定 = UNAVAILABLE
```

矛盾的化解：owner Phase 4+ 的义务由 Write/Recall/Repair 三段的分段声明满足
（三段各自完整实测七字段）；DOD-036（phase=14）保持不变，它验收的是
lifecycle_complete（四段全 DECLARED），Action 段实测在 SUT 落地后补。

**防挑软样本条款**：Action 段虽 BLOCKED_ON_SUT，其攻击样本清单（attack IDs、
benign 近邻设计、§45.2 四向量覆盖、未来分母 inventory）必须与三段语料**同一提交冻结**
——禁止 Phase 14 看到系统表现后再挑样本。

依据：OWASP AI Exchange（未测 threat category 显式报告、coverage gap 即 finding、
retrieval 与 downstream action 分开测、corpus-to-action 前提 "where the system can
trigger actions"）；NIST AI RMF MEASURE 1.1（当前不能测的风险记录理由）+ TEVV 允许
component-level 验证；AgentPoison 将攻击链拆 ASR-r/ASR-a/ASR-t 分层计量；PoisonedRAG
将 claim 截止在真实存在的 SUT endpoint。

**否决的替代方案**：A) 把 Action 段实现从 phase 14 提前——benchmark 笔误反向支配产品
roadmap，治理反转；C) 整集恒红到 Phase 14——把「SUT 不存在」混同「治理违规」，CI 失去
信号价值（G80-21 死法的变体）。

### 2. 量具单位统一：spread 用逐题 Hamming，resolution 用稳定翻转

`continuation_198_v2`（及未来所有逐题二值判定的集合）：

```text
每次 run 输出逐题 verdict 向量 V_r ∈ {0,1}^n（按层）
spread_tol_observed_n3 := max_{i<j} Hamming(V_i, V_j)   # 单位 = 题
resolution 的 stable_flip(q) :=
    baseline 三次 verdict 一致 ∧ mutant 三次 verdict 一致 ∧ 两者不同
resolution := 最小预冻结局部扰动产生的 stable_flip 计数    # 单位 = 题
可分辨性门槛：between-system disagreement > within-system disagreement
```

- 字段语义如实叫「n=3 观测极差」，**不做假统计修正**（NIST：n=3 时真 σ 的 95% 单侧
  上界 ≈ 4.42s，观测极差对总体约束极弱；要真上界只能加 repeats，登记 §56 参数实验）。
- 第二系统扰动轴冻结为 **recency 权重局部扰动族**：预冻结邻域 {λ, λ−5%, λ−10%, λ−20%, …, 0}，其余全部冻结（embedding / 候选池 / RRF / rerank / top_k / 语料 / 判定），取第一个
  产生稳定非零翻转的**局部** mutant。λ→0 整轴消融不算（那是 feature ablation）。
- **否决的扰动轴**：换 embedding（产品差距不是仪器分辨率）；收 top_k（改判定契约 =
  改尺子刻度，绝对禁止）；去 rerank 仅留作粗消融 sanity check，不进 resolution。

### 3. checker（G80-16）同步升级

- 识别 `BLOCKED_ON_SUT→phase N` 标注：N 到期即红（注错：把 N 改小于当前 phase ⇒ 红）。
- BLOCKED_ON_SUT 段携带实测读数 ⇒ 红（禁止一边挡箭一边填值）。
- 分段声明行的七字段校验按 declared_scope 内的段逐段核（每段各自带数）。

## 后果

- memory_security_lifecycle 的 Write/Recall/Repair 三段可立即开工（语料 + harness +
  实测），Action 段冻结清单不填值；benchset 红名单该行在三段实测后翻绿（scoped）。
- continuation_198_v2 返工时按新单位取数：重取 baseline（只收 exit=0 无降级 run，
  见 rejected 记忆的返工清单）+ recency 扰动族实测 state 层 resolution。
- 既往已声明集合不受影响：planner_predicate / exact_completeness 的判定本就逐题一致
  比较（无 cancellation 空间），其 spread=0 读数在两种定义下同值。
