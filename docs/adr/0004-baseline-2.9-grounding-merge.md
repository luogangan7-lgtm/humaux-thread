# ADR-0004 — Baseline 2.9 Grounding：按增量合并，不整文件替换

- 状态：Accepted
- 日期：2026-08-27
- 关联：ADR-0001（spec 编辑性自相矛盾修正）、ADR-0003（network vs egress choke point）
- 上游：`Humaux_Thread_Architecture_Baseline_2.9_Grounding_Integrated_Canonical.md`（15201 行）

## 背景

2.9 从**原始 2.8** 分叉，而本仓的 `docs/architecture/Baseline_2.8.md` 已经带着 ADR-0001 与
ADR-0003 的修正走了一段。两份文档因此是**两条分支**，不是新旧两版：整文件替换会静默回退
已落地并已有注错记录的架构。

## 决定

**逐节合并 2.9 的增量，保留本仓已有的修正。** 具体裁决如下。

### 1. §83.4 —— 保留本仓版，不取 2.9 版

2.9 的九块承重表把 `NETWORK_BOUNDARY §83.4` 列了进来，说明两条分支在这一点上**独立收敛到了
同一个想法**。但 2.9 的 §83.4 正文只有约 60 行，是同一想法的上一代：

| | 2.9 | 本仓（ADR-0003） |
|---|---|---|
| `RecipientClass` / `NetworkRouteClass` 正交枚举 | 无 | 有 |
| 披露记账依据 | 未区分 | 法律接收方，而非网络路径 |
| §83.4 正文 | ~60 行 | 528 行 |
| private-data purpose 的凭证 | 表格里写 `EgressPermit` | 枚举变体**直接持有** `EgressPermit`（类型层面无法构造无凭证的私有出境） |
| G80-3 子闸 | 只列 3A/3B/3C 名字 | 三子闸 + 注错记录 |

2.9 列的九条 intra-cell AND 准入，本仓实现**全部满足**（逐条实测，见下）。因此本仓 §83.4 在
判据强度上严格覆盖 2.9 版，取 2.9 版是净损失。

### 2. 2.9 九条准入的逐条实测结论

上一代只有六条，2.9 多出的四条中有两条是具体绕过手法，本次逐条打了实弹：

- `automatic redirect disabled` —— **已满足**：`infra-network/src/http.rs` 的
  `redirect(Policy::none())`，且 3xx 在 `transport.rs` 被显式转成 `UnexpectedRedirect`。
- `IPv4-mapped IPv6 规范化` —— **已满足但当时无闸**：`is_metadata_or_link_local` 与
  `CellCidr::contains` 各自开头的 `.to_canonical()` 把 `::ffff:a.b.c.d` 展平回 V4 再判。
  本次补了 `mainline_ssrf_probe.rs` 的攻击 4/5 并完成红转绿（见下节）。
- `A/AAAA 全部规范化后属于显式 endpoint set`、`destination service identity verified
  （SPIFFE 优先）` —— 前者由 `ValidatingResolver` 承担；后者是部署级事实，已记在 README
  的 intra-cell deploy gate（criterion 5），本仓代码不再使其**不可满足**。

### 3. 假绿修正：断言必须钉具体判据，不能写成 OR

补探针首版写的是 `err.contains("Metadata") || err.contains("AddressNotInCell")`，两道闸互相
顶包：拆掉 `is_metadata_or_link_local` 的 `.to_canonical()` 后，地址原封不动漏到下游被
`address_in_cell` 接住，断言照样绿。收紧到逐字命中 metadata 判据本身后，注错立刻红：
`AddressNotInCell(::ffff:169.254.169.254)` —— 正是要拦的形态。

**推广为硬规则**：一条注错闸若断言写成「拒了就行」的析取，就等于没有区分能力；被测判据必须
是断言里唯一能让它绿的那一条。

相对地，`CellCidr::contains` 的 `.to_canonical()` **不配注错闸**：去掉它会让 `(V4 net, V6 ip)`
落进 `_ => false`，判定只会更严不会更松（fail-closed），那是可用性回归而非安全回归，没有对应
的攻击可复现。为一个不存在的攻击造闸是 §80.1 说的那种「永远不会红的闸」。

### 4. 迁移文件里的 `Baseline_2.8.md` 引用保持原样

spec 文件随本次合并更名为 `Baseline_2.9.md`，但 `migrations/*.sql` 里的引用**不改**：
`ops.schema_migrations.checksum` 对已应用迁移做漂移检测，改动任何一个字节（注释也算）都会让
`xtask migrate` 硬失败。迁移里的文件名是**写入当时**的规范名，属于历史引用，保持原样才正确。

### 5. §21 保持「五类检索质量信号」

2.9 没有把 Grounding 变成第六类信号——§21 标题仍是五类，变的是 §21.5 正文那句禁止压分的清单
多了 `Grounding State`，共六个不可压缩的上报维度。这与 §8.8「Temporal Freshness 与 Grounding
Validity 正交、禁止压回一个 stale 字段」一致。落到代码：`QualitySignals` 加**独立字段**
`grounding`，不并入 `freshness`。

## 后果

- `§80.3` 承重块 7 → 9（新增 `GROUNDING_CONTRACT`、`NETWORK_BOUNDARY`）。`xtask/src/
  contract_impact.rs` 的正哨兵把 7 钉在三处：`BLOCK_LIST_ANCHOR` 锚点串、`actual.len() != 7`
  判定、fixture 的 `assert_eq!(blocks.len(), 7)`。三处必须同改，漏一处合并当天即红。
- DoD 091 → 094；benchmark 集合 9 → 10（新增 `grounding_evolution`）；新增闸 G80-43
  （判据 §11.10#G11-2，§57.1 Phase 4 必过），八夹具 A–H + 四条注错。
- `§21.5 Freshness` → `§21.5 Temporal Freshness`。

## 未取的替代方案

- **整文件替换成 2.9**：会回退 ADR-0001 的四处编辑性修正（含 §2.5/§2.6/§2.7 三份记录，2.9
  完全没有）与 ADR-0003 的全部内容。
- **把本仓 §83.4 降级成 2.9 版以「对齐上游」**：这是 §80.1.2 批评已退休 G80-21 时说的「调松
  定义」——用降低判据强度换取表面一致。
