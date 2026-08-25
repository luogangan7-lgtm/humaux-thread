# tests/pair/ — 成对终态测试（G52-5）

## 归属闸

**G52-5**：每个 `ErrorCode`/`DegradeCode` 语义在系统中都存在「终止」与
「降级」两条可达路径时，必须各有一条成对测试证明二者互斥、且各自能被
真实触发（而不是永远只有一条路径被测到，另一条靠人工推断「应该也会走」）。

## 命名规则

```text
tests/pair/<code>_terminal.rs
tests/pair/<code>_degraded.rs
```

`<code>` 为 `ErrorCode`/`DegradeCode` 变体的 snake_case 名，两个文件必须
成对出现——只有一半（例如只有 `_terminal.rs` 没有 `_degraded.rs`）视为
该 code 的成对测试未完成。

## 每对测试必须证明什么

- `<code>_terminal.rs`：触发条件下系统走终止路径（`ErrorCode`），响应/
  状态不得同时出现该 code 的降级形态。
- `<code>_degraded.rs`：触发条件下系统走降级路径（`DegradeCode`），服务
  仍返回可用结果（非终止），并带 `completeness.degradations[]` 标记。

两个文件的注入条件必须是**同一根因的两种触发方式**（例如「完全不可用」
vs「部分可用」），而不是两个无关场景各自随便断言一次。
