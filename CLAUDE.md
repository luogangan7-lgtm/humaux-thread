# Humaux Thread — 仓库开发规范（约束所有 agent）

> 规范唯一真源：`docs/architecture/Baseline_2.8.md`（下称「spec」）。本文件只是入口索引，与 spec 冲突时以 spec 家章为准。判据正文不得复制到任何第二处——引用 § 号。

## 硬边界（违反 = CI 红，不靠 review）

- **Domain 永不 import** HTTP / SQLx / Qdrant / Provider SDK / ENV（spec §3、§78.3）。
- **唯一构造点模式**：`Authority::new`、`evidence::payload_sha256`、`retrieval::build_request`、`abstain()`、`ledger::close`、`classify()`——全 workspace 各恰 1 处，断言写 `== 1` 不写 `<= 1`。
- **两个错误枚举**：`ErrorCode`（终止）与 `DegradeCode`（降级），互斥（§52）；禁止新开第三个错误枚举。
- 禁止硬编码业务配置：模型名/价格/plan 判断/quota 数/TTL/限流阈值（§78.1）。
- 禁止 stringly-typed domain；DB enum 与 Rust enum 走 contract test 对账（§78.2）。
- 所有闸三态 `pass/fail/not_applicable`；`not_applicable` 必须打印缺失对象名（§57.1）。
- 一道闸没有「注错红转绿」记录就不算存在（§80.1 准入条件）。

## 注释规范（可维护性要求）

- 每个 `pub` 项必须有 rustdoc `///`；每个模块必须有 `//!` 说明职责边界。
- **注释只写代码自己表达不了的约束**，并**引用 spec § 号**（例：`// §53.2: label 值 = PascalCase 变体名逐字，与线格式不可互换`）。
- 不写「这行做了什么」的复述注释；不写「本次改动为什么正确」的评审话术。
- 有意留下的天花板用 `// ponytail: <天花板>, <升级路径>` 标注。
- 注释语言：英文为主（项目规划 Apache-2.0 开源），spec § 引用必须保留。

## 任务工作流（每张任务卡）

1. 实现 → 2. 独立审查（不同 agent）→ 3. 注错验证（红转绿）→ 4. `cargo fmt/clippy/test` 全绿 → 5. 才可报完成。
- 实现 agent 不跑 `git commit`（避免并行索引竞争）；由主线按 wave 统一提交。
- agent 档位：机械体力活=轻档；标准实现/审查/测试=中档；契约裁决/根因=重档。**模型名不写在本文件**，落在调用方 config/参数。
- 并行 agent 各自只动分配的文件；共享文件（lib.rs 模块声明等）已由骨架预建，不许改别人的模块。

## 测试规范（spec §79）

- 不用 mock PostgreSQL 证明事务正确；DB 集成测试走隔离 schema。
- 注错测试是一等公民：目录 `crates/testkit/tests/fault/`（§53.4）、`tests/pair/`（G52-5）、`sentinels/`（§53.3 规则3）、`tests/metrics/`（G80-6 Witness）。
- 变异测试目标优先：Contract Kernel / security / billing / retrieval。

## Git

- main 不允许红（§46，三态语义下）。提交信息：`<area>: <what> (§ref / T-card)`。
- 架构语义变更必须 ADR（`docs/adr/`，§78.6）。
