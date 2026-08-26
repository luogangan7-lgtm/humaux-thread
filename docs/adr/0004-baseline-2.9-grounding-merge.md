# ADR-0004 — Baseline 2.9 Grounding：按增量合并，不整文件替换

- 状态：Accepted
- 日期：2026-08-27
- 关联：ADR-0001（spec 编辑性自相矛盾修正）、ADR-0003（network vs egress choke point）
- 上游：`Humaux_Thread_Architecture_Baseline_2.9_Grounding_Integrated_Canonical.md`（15201 行）

## 背景

2.9 从**原始 2.8** 分叉，而本仓的 spec（合并前叫 `Baseline_2.8.md`）已经带着 ADR-0001 与
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

### 4. 按哈希冻结的产物不参与改名

spec 文件随本次合并更名为 `Baseline_2.9.md`，但 `migrations/*.sql` 里指向旧名的引用**不改**：
`ops.schema_migrations.checksum` 对已应用迁移做漂移检测，改动任何一个字节（注释也算）都会让
`xtask migrate` 硬失败。迁移里的文件名是**写入当时**的规范名，属于历史引用，保持原样才正确。

**同一条规则适用于 `evals/`**，这一点是踩出来的：改名波把 `evals/planner_predicate/dataset.tsv`
里一行注释的旧文件名也换掉了，而 §55.3 用 `manifest.toml` 的 `fixture_sha256` 冻结的是**整个
文件**——数据行一字未动，哈希照样从 `d7d788ea…` 变成 `6a25d5cb…`，
`planner_predicate_eval::manifest_matches_real_dataset` 当场红。已回退该文件。

**更值得记的是它为什么没被当场发现**：改名后八道 xtask 闸全绿，于是被当成干净了——但冻结夹具的
哈希不在任何一道闸的判据里，只有 `cargo test` 看得见。**全局改名之后必须跑全量测试；闸全绿只
说明各闸自己声明的判据面没破，不说明没破坏别的东西。**

第三种形态出在本文件自己身上：全局 sed 把 ADR 正文里那些**把旧文件名当论述对象**的句子
（「迁移里的引用保持为 `Baseline_2.8.md`」）也一并替换，直接改反了结论。**全局改名要区分
「引用」与「论述对象」**——论述某个名字的文本，不是指向它的引用。

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

### 6. 夹具 G 按相位延后，走 `not_applicable` 而不是伪装通过

§11.10#G11-2 的八条夹具里，G（`RECHECK_REQUIRED` 的 ProjectConstraint 不得进入 Mandatory
behavior context）的被测对象是 §25 Mandatory Context Lane，而 `domain::context` 在本仓仍是
占位模块——对应的 DOD-093 自己标着 `phase=8`。

因此 A–F、H 七条是真断言，G 在 `crates/domain/src/grounding.rs` 的测试里打印
`NOT_APPLICABLE … missing object: §25 Mandatory Context Lane 准入判定`（§57.1 第2条），
并只断言本波**能**证明的那一半：判据本身已经通过
`GroundingState::revokes_current_truth_assumption` 给出「撤销当前真值假设」的答案，Phase 8
的 Mandatory lane 只需要读它，不需要再推一次。

同理，§11.10 点名的四条注错里前三条（删 version compare → B 红、把 resolver error 当
Missing → D 红、去掉 finalization CAS → F 红）本波已逐条实测红转绿；第四条（Context 忽略
GroundingState → G 红）随 Phase 8 补。

### 7. G80-43 闸的三条断言各钉一个不同的失效面

单看「夹具都在」是不够的：`grounding.rs` 可以被清空成只剩八条空测试的壳。所以
`g80_43_grounding_validity` 同时钉三处——八条夹具逐名在场、`derive_grounding_state` 恰好
定义一次（活哨兵，带左括号做词边界，防 `derive_grounding_state_v2` 这类改名绕过）、
全 workspace 无可手改的 stale 赋值（DOD-092）。

配套注错族用 **fixture 树**而不是在真仓上变异：真仓改名会连带打断依赖 crate 的编译，
`xtask` 自己都构建不起来，空的 grep 输出极易被误读成「闸没红」（先前波次已踩过）。
另有一条反向对照 `g80_43_reading_a_stale_field_is_not_a_violation`——**读** stale 是合法的，
只有赋值才违反 DOD-092；没有这条，判据很容易被写成粗暴匹配 `stale` 而误报合法读路径。
