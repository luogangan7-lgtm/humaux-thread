# Prometheus 运行期不变量（INV-1..4）—— T0.5

规则文件：`invariants.rules.yml`。规范唯一真源见
`docs/architecture/Baseline_2.9.md` §53.5（四条不变量原文 + 注错块）、
§41.2（指标注册表，逐字对账）、§42（冻结：逐字复制进 rule 文件，
禁止任何名字替换；部署前置 Prometheus >= 2.17）。

**本节是 §53.5 六组注错记录的执行清单。** 规则层的可执行红绿记录自卡 34 起是
`mutations.sh`（合成序列上的 promtool 单测必须抓住每一处变异，见下方 Files）；
进程层的端到端注错（真实网关 + 真实降级 ⇒ INV-1 到达告警路由）是 rehearsal 的
`alert_drill` 步。此处登记「注入什么 ⇒ 哪条 firing ⇒ 反证怎么验证判据没被削弱」，
供 G80-18 取记录时核对。

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

## Files (card 34, ADR-0061 D-E / D-G / D-I)

| file | what it is |
|---|---|
| `invariants.rules.yml` | §53.5 INV-1..4, byte-identical since `6b6981b` (gate `invariants_rules_unchanged`) |
| `alerts.rules.yml` | the §42 rows loaded now: CoreMetricAbsent, ProjectionLagExceedsSLO, QueueDeadLetterIncrease, BackupFailure (silent until card 37 produces the family), HealthGaugesAbsent (§42 row added by ruling E8), Watchdog (§42.1) |
| `tests/*.test.yml` | promtool unit tests: every alert has a firing and a silent case (Watchdog: two firing checks) |
| `prometheus.yml` | scrape (file_sd `targets/*.json` + collector telemetry), both rule files, Alertmanager on loopback, `external_labels.git_sha` |
| `alertmanager.yml` | root route `group_wait 10s / group_interval 1m / repeat_interval 4h` to the log sink; Watchdog alone to its dead-man receiver every 5 min; URLs only via `url_file` |
| `otel-collector.yml` | OTLP receivers on `127.0.0.1:4317/4318`, `debug` exporter, own telemetry on `127.0.0.1:8888` |
| `../compose/observability.yml` | the three bundle services for card 39 (Linux, `network_mode: host`), images pinned by digest |
| `pinned-tool.sh` | runs a pinned binary after checking its sha256 and version; never PATH |
| `test-rules.sh` | `promtool test rules` + coverage grep (each alert ≥ 2 `alertname:` checks) |
| `mutations.sh` | the §80.1 red record: 14 single-token mutations, each must be caught |
| `check-compose.sh` | static gate for the compose fragment (digests, pinned tags, loopback flags, no write ingress) |
| `selftest.sh` | proves the scripts above go red for the right reason (T-E1..T-E7) |

## Pins (research addendum W1-W3, verified by the main line; values for the test host live in TW `live_env.sh`)

Executables live under `$HOME/.humaux-tools/<tool>-<version>/` and are named by
`HUMAUX_TEST_{PROMTOOL,PROMETHEUS,ALERTMANAGER,AMTOOL,OTELCOL}_{BIN,SHA256}` plus
`HUMAUX_TEST_{PROMTOOL,PROMETHEUS,ALERTMANAGER,OTELCOL}_VERSION` (amtool is checked against the
Alertmanager version: same tarball).

| tool | version | darwin-arm64 executable sha256 (test) | linux-arm64 tarball sha256 | image (deploy) |
|---|---|---|---|---|
| promtool | 3.15.0 | `51a8798ea299906d5f7adeef6783f6eaf2d5c2d247721bb71403e384300ec5f0` | `f1f90ec08e849d494ca66c611470afc50192f0355f1a61c33f2cbde02d067823` | — |
| prometheus | 3.15.0 | `edfbcf257fff3694e345be7648ce8ee05aa0cf602662097c649f4923116d7145` | (same tarball) | `prom/prometheus:v3.15.0@sha256:6b41f7a45cfbd1d259a78701ee5e14fc2ad9383c9aa5d0427345a18539bc3c91` |
| alertmanager | 0.34.1 | `c09fe5d0e479e44a39e8501e5ab6b6a16b19370bf51ce8433b92406ba6368cac` | `d98d6cbaf52151c7e76e24355fec88b11cebcb9875d4cdd8b76ddce7a7e5535c` | `prom/alertmanager:v0.34.1@sha256:47a1dc7e74f1e755e29f74d392262f8d1da41f2ada5653911199bf07219e41d9` |
| amtool | 0.34.1 | `6c3af8b29d7514150aabd42e9d719caf801571830d36dc547aa021a6e096a53d` | (same tarball) | — |
| otelcol (core) | 0.162.0 | `72ea1f0bca7ed32039381b95c8a093dfd2247d630ea145d79bc63dbee499173a` | not downloaded (card 39) | `otel/opentelemetry-collector:0.162.0@sha256:ef772ad07ca455ad83fabbf3da792f0f298a3d41a08e9099367364c0d1349801` |

## Run the gates (from the repo root, after sourcing the TW test env)

```sh
sh deploy/prometheus/pinned-tool.sh promtool check rules deploy/prometheus/invariants.rules.yml deploy/prometheus/alerts.rules.yml
sh deploy/prometheus/pinned-tool.sh promtool check config deploy/prometheus/prometheus.yml
sh deploy/prometheus/test-rules.sh
sh deploy/prometheus/mutations.sh      # prints 14 `mutation=<id> red` lines
sh deploy/prometheus/selftest.sh
sh deploy/prometheus/check-compose.sh
sh deploy/prometheus/pinned-tool.sh amtool check-config deploy/prometheus/alertmanager.yml
sh deploy/prometheus/pinned-tool.sh otelcol validate --config=deploy/prometheus/otel-collector.yml
```

Every script exits 2 (not_applicable, naming the variable) when a pin is unset, and 1 on a sha256 or
version mismatch; none falls back to a binary on PATH. promtool semantics the tests rely on were
measured with the pinned 3.15.0 (W6): an assertion failure and a rule-load error both exit 1, so
`mutations.sh` counts a red only when the output also names `alertname: <expected>, time:`.

## Deployer rendering contract (rehearse.sh now, card-39 packaging later)

- `prometheus.yml`: replace `__HUMAUX_GIT_SHA__` with the `value` of `humaux-admin q deploy.binary`
  (the Watchdog payload must carry it, §42.1 / §67.4); write `targets/*.json` from the seven
  `*_METRICS_ADDR` values with labels `{job: humaux-<process>, mode: <mode>}`; start Prometheus with
  `--web.listen-address=127.0.0.1:<port> --storage.tsdb.retention.time=30d`.
- `alertmanager.yml`: replace `__HUMAUX_ALERTMANAGER_LOG_SINK_URL_FILE__` and
  `__HUMAUX_ALERTMANAGER_WATCHDOG_URL_FILE__` with paths of URL files kept outside the repo; start it with
  `--web.listen-address=127.0.0.1:<port> --cluster.listen-address=` (empty: no gossip listener).
- `observability.yml` needs `HUMAUX_OBSERVABILITY_RENDERED_DIR` (the rendered configs) and
  `HUMAUX_ALERTMANAGER_URL_DIR` (the URL files); both are required interpolations with no default.
- §42.1 injection "stop Alertmanager 5 min ⇒ the external endpoint reports the missing ping" is a
  manual §69 step (runbook §7): the endpoint is external to this repo.
