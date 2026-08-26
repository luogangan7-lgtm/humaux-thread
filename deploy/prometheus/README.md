# Prometheus 运行期不变量（INV-1..4）—— T0.5

规则文件：`invariants.rules.yml`。规范唯一真源见
`docs/architecture/Baseline_2.9.md` §53.5（四条不变量原文 + 注错块）、
§41.2（指标注册表，逐字对账）、§42（冻结：逐字复制进 rule 文件，
禁止任何名字替换；部署前置 Prometheus >= 2.17）。

**本文件是 §53.5 六组注错记录的执行清单，不是注错本身跑过的证据。**
实际注错（往一次性 fixture/测试环境注入故障、观察 firing/不 firing）
在 Phase 14 e2e 环境跑，此处只登记「注入什么 ⇒ 哪条 firing ⇒ 反证怎么
验证判据没被削弱」，供 G80-18 取记录时核对。

## 六组注错记录（§53.5 原文块，逐条登记）

| # | 注入 | 期望结果 | 反证（改回病灶写法应不 firing） |
|---|---|---|---|
| INV-1 | 停掉 planner，但让 `abstain()` 继续跳 | `humaux_retrieval_requests_total` 5m 速率为 0，`degrade_total` 5m 速率 > 0 ⇒ 5m 内 firing | 把分母名字改回 `queries_total`（§41.2 不存在此名）⇒ 同一注入下右侧对空 family 求值恒空，永不 firing |
| INV-1' | 把整个 retrieval 进程杀掉（不是只停 planner），5m 内该 family 一个样本都没有 | 走 `absent_over_time` 分支，仍 firing | 去掉 `or absent_over_time(...)` ⇒ 同一注入下 `sum(rate(...))` 是空向量，`空 == 0` 也是空 ⇒ `and` 配不出结果，最该响的时候反而不响 |
| INV-2 | 注入 `EgressDenied`（PascalCase，§53.2 冻结）并屏蔽 §7.4 `ops.data_disclosures` 的 finalize 写入 | firing | 去掉左右任一侧 `sum()`（左带 `code` label、右带 `outcome` label）⇒ 同一注入下按标签集配对失败，不 firing |
| INV-2' | 停掉 §7.4 那个 exporter，`data_disclosures_finalized_total` 彻底没有样本 | 走 `absent_over_time` 分支，仍 firing | 同 INV-1'：去掉 `or absent_over_time(...)` ⇒ `and` 配不出结果，不 firing |
| INV-3 | 造一条 `reserved_at` 非空、`finalized_at` 空且超 60s 的记录 | `data_disclosures_reserved_unfinalized{age_bucket="gt_60s"}` 0 → 1 ⇒ firing | 把 matcher 改回原文病灶写法 `{age>60s}`（不是合法 label matcher）⇒ rule 文件加载即报错，这条闸不存在，谈不上 firing |
| INV-4 | 让单个 code 24h 占比从 ~10% 压到 > 40% | firing（WARN） | 把中文散文式判据换回去（无可求值形式）⇒ 落不进 Alertmanager，无从 firing |

补充反证（§53.5 INV-2 专属，label 格式）：
把 `degrade_total` 的 `code` label 从 PascalCase 改成 SCREAMING_SNAKE
线格式（如 `EGRESS_DENIED`）⇒ INV-2 的 `code=~"Egress.*"` matcher
匹配 0 条，同一次 EgressDenied 注入不再 firing —— 这正是 §53.2 冻结
「label 值 = PascalCase 变体名逐字，与线格式不可互换」的注错点。

## §41.2 逐名对账（本文件用到的每个 metric / label）

| 本文件用到的名字 | §41.2 是否登记 | §41.2 行内标注 |
|---|---|---|
| `degrade_total{code}` | 是 | 消费方含"§53.5 INV-1/2/4" |
| `humaux_retrieval_requests_total` | 是 | 消费方标注"§53.5 INV-1 分母" |
| `data_disclosures_finalized_total{outcome}` | 是 | 消费方标注"§53.5 INV-2 分母" |
| `data_disclosures_reserved_unfinalized{age_bucket}` | 是 | 消费方标注"§53.5 INV-3" |
| `age_bucket="gt_60s"` | 是 | §41.2 冻结的 label 取值集 `le_10s \| le_60s \| gt_60s` 之一 |
| `code=~"Egress.*"` | 是 | §41.2 冻结：`degrade_total.code` = §53.2 `DegradeCode` 全部变体的 PascalCase 变体名逐字 |

未使用 §41.2 表外任何名字；未对任何表达式做"读作"改写（§42 冻结）。

## 校验：promtool check rules

```
not_applicable（缺失对象：promtool）
```

本机未安装 `promtool`（`command -v promtool` 无输出，`brew list
prometheus` 无结果），按仓库闸三态（`pass/fail/not_applicable`，
§57.1）打印缺失对象名，不伪造通过。

**CI 中由 G80-18 承接**：G80-18 的静态校验必须在 >= 2.17 的
`promtool` 上跑 `promtool check rules invariants.rules.yml`，非零
退出直接判红（§42 冻结）。本地无 promtool 时的等价手工检查（YAML
可解析、`groups[].rules[].expr` 非空、四条 `alert` 名与 §53.5 一一
对应）不能替代 `promtool check rules`，因为 §42 明确指出的两类失败
（非法 label matcher 导致整份规则加载失败、Prometheus < 2.17 时
`absent_over_time` 导致整份规则加载失败）只有 promtool 的 PromQL
解析器能捕获。
