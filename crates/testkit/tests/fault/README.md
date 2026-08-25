# tests/fault/ — 注错测试（§53.4）

## 归属闸

**§53.4**「每个 reason 必须有一条注错测试」+ **§53.3 规则2**：
`|DegradeCode 变体| == |DEGRADE_TOTAL 标签基数| == |注错测试数|`，三个数字不
等即红。这个目录就是等式右边「注错测试数」的计数对象。

## 命名规则

```text
tests/fault/<variant_snake_case>.rs
```

文件名 = `lower(fold(变体名))`，与 `DegradeCode` 变体一一对应。当前 10 个
变体 ⇒ 10 个文件。两条注入方式不能从变体名直接读出，登记于 §53.4：

```text
projection_invisible_loss.rs   绕过 retention::tombstone 直接从 Qdrant 删 10 个 point
rerank_provider_timeout.rs     rerank provider 注入读超时
```

其余文件照变体语义注入。**从未被触发过的 reason 不许合并**——加了变体没加
测试，§53.3 规则2 的等式立刻不等，CI 红。

## 每条测试必须断言什么

对注入的故障，断言：

1. `degrade_total{code="<变体名>"}` 恰好 `+1`
2. 响应体 `completeness.degradations[]` 含该变体的**线格式**值（三种映射
   见 §53.2）

用 `humaux_testkit::assert_fault_observed`（见 `crates/testkit/src/lib.rs`）
统一这两条断言，避免每个文件各写一套判定逻辑。
