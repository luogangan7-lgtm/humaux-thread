# tests/metrics/ — Metric Witness（G80-6）

## 归属闸

**G80-6 / D1–D6**：`metrics-registry-check` 三方比对——R（§41.2 registry 声明）
/ C（architecture-check 对代码的静态解析）/ W（本目录的 witness 测试）。
D2 要求每个 family 恰好 1 个 witness、`sample_count>0`、`scanned_targets>0`；
D6 正哨兵要求 `|R|>0 且 |W|==|R|`。本目录就是 W 的物理落点。

## 命名规则

```text
tests/metrics/<family>.rs
```

`<family>` 是 metric family 名（不含 `_bucket`/`_sum`/`_count`/`le` 等
histogram 后缀——D4 比对时三方都按去后缀的 family 名对齐）。文件名从
family 名**确定性派生**，禁止再手抄一张「family → 文件名」映射表
（该映射表本身会漂移，§80.2 决策）。

## 每个 witness 测试必须做什么

1. 用 `#[metric_witness]`（或等效标记）声明该测试对应哪个 family。
2. 合成 fixture 主动触发至少一次真实 emit（稀有 family 不得用
   `NOT_APPLICABLE_YET`/章级继承/`never_observed` 豁免——§80.2 决策）。
3. 断言 label 集合与 §41.2 registry 声明的一致（D4）。
4. 断言 `actual_emit_callsite_count(C) == declared_emit_count(R)` 所需的
   证据能被 witness 观测到（D5 的一侧）。
