# ADR-0030: 用户情感记忆 = Memory 的 affect 注解轴（`private.memory_affects`；Card E1）

日期：2026-09-06 · 状态：Accepted · 影响面：`migrations/0156_memory_affects.{sql,manifest.toml}` / `migrations/0157_evidence_affects.{sql,manifest.toml}`（写侧载体 + memory_evidence PRIMARY 复制触发器）/ `crates/domain/src/affect.rs`（`AffectKind` / `EmotionLabel` / `AffectTargetScopeKind` / `BasisPoints` / `MoodHalfLife` / `AffectAnnotation` / `AffectFilter` / `MoodPoint` / `effective_intensity` / `mood_congruence` / `AffectWriteOp`）/ `crates/application/src/affect.rs`（`memories_matching` / `rerank_by_mood` / `effective_intensity_at`）/ `crates/adapters/src/affect_repo.rs`（唯一行签发点 `resolve_targets_in_txn` + `insert_in_txn`、`annotate`、唯一集合读 `AFFECTS_FOR_MEMORIES_SQL`、wire 解析）/ `crates/adapters/src/remember.rs`（`RememberCommand.affects`）/ `crates/adapters/src/memory_governance_repo.rs`（`CorrectRequest.affects` 在 `correct_atomically` 事务内）/ `crates/projection/src/dense.rs`（`Condition::Range`、`AFFECT_*_FIELD`、`DenseQueryFilter::with_affect`）/ `crates/adapters/src/qdrant.rs`（`IndexablePayload::with_affects`、`Range` 线格式、`DenseQuery::with_affect_filter`）/ `projection_worker.rs`（同事务读 affects → payload）/ `read_materialize.rs` + `retrieve.rs`（hydrate gate 的 affect 复核）/ `bins/gateway/src/{memory.rs,recall.rs,mcp_application.rs,bootstrap.rs}` / `contracts/mcp/{memory,memory.output,context.output,recall}.schema.json` / `xtask/src/rls_check.rs` / Baseline §8.5.1、§6.2.2、§59 / 测试：`crates/adapters/tests/memory_affects.rs`、`bins/gateway/tests/mcp_gateway.rs`。前置：卡 1–9 在树上（HEAD 319ff36，迁移至 0155）。研究档案：WebGPT 调研「用户情感记忆 = Memory 的 affect 注解轴」（2026-09-05，引用 Mem0 / Zep-Graphiti / Letta / LangMem / Character.AI 与 Russell circumplex / Mehrabian PAD / Scherer appraisal）。

## 背景
验收新增项「用户情感记忆」。2.10 的 `MemoryType` 没有情感类型；市面公开实现（Mem0 metadata sentiment、Zep 自定义 entity 属性、Letta block、LangMem 自定义 schema、Character.AI facts）都没有形成统一的「agent emotional memory schema」，一致的信号是：**情感是 memory 的附加维度，不是第四种长期记忆本体**。情感常「关于某个 subject」，所以依赖卡 7–9 的 subject 轴先落。

## 决定

### D-A 数据模型：一张 authority 表，正交于 MemoryType
- **不新增 `MemoryType::Emotion`。** 「客户拒绝续约，用户对此非常沮丧」的本体仍是 `Rejection`/`Outcome`/…，情感是它的一个 measurement：`Affect{valence=-0.8, arousal=0.5, dominance=-0.4, label=FRUSTRATION}`。否则类型轴会污染成 `EmotionFact` / `EmotionDecision` / …。
- `private.memory_affects`（0156，0..N 行 / memory）：`affect_kind EMOTION|MOOD`；VAD `valence_bp / arousal_bp / dominance_bp smallint` 可空、CHECK `-10000..10000`（**PG 权威存整数 basis points，永不 float**：无 NaN、无 rounding、等值边界确定）；`intensity_bp / confidence_bp` 必填 `0..10000`（intensity **单独存**：高强度悲伤不要求高 arousal，`abs(arousal)` 不能替代）；`label` 可空、闭集 12 值；`evidence_id`（tenant-leg FK → `evidence_objects`，观察的 provenance = memory 的 PRIMARY Evidence）；`target_subject_id`（tenant-leg FK → `private.subjects` **ON DELETE CASCADE**：subject ERASE 对 affect 终态）；`target_scope_kind/id`（typed ScopeRef，闭集 = §59 Scope 五层）；`observed_at`；`half_life_seconds`（`CHECK ((affect_kind='MOOD') = (half_life_seconds IS NOT NULL))`）。`(tenant_id, memory_id)` FK ON DELETE CASCADE，§37 purge 终态。
- 行 **immutable**：无 runtime 角色持有 UPDATE/DELETE；owner 触发器 `memory_affects_immutable`（0148 `confirm_tokens` 同形）拒绝一切 UPDATE。DELETE 只靠 grant 拒绝——不加 DELETE 触发器，否则 memory/subject 的级联删除会被挡住。
- RLS：逐字 tenant 子句 AND `EXISTS(memory_records)`（0154 `memory_subjects` 同形）。§6.2.2：gateway / private_worker `SELECT, INSERT`；retrieval_worker / maintenance `SELECT`；`rls_check` `ADDITIVE_SEAM_MATRIX` 同一变更加行。`memory_records` 的策略未动——0155 的 hash pin 不需要重 pin。

### D-B Emotion 与 Mood 分开；衰减是读时派生，不是 lifecycle mutation
- `EMOTION` 是关于某事件的历史观察：五年后也不能说「你昨天其实只生气 0.01」，raw intensity 永不衰减。`MOOD` 是弥散的状态：`effective_intensity(now) = raw · 2^(−Δt/half_life)`，`humaux_domain::affect::effective_intensity` 纯函数（饱和、永不为负、永不高于 raw；EMOTION 的 `half_life = None` 原样返回）。
- **没有文献支持「人类 mood 固定半衰期 = N 小时」**，所以 half-life 是冻结的 product/calibration policy：`HUMAUX_GATEWAY_MOOD_HALF_LIFE_SECONDS`（§78.1，必填 env，无字面量），**写入时**取值并按行存 `half_life_seconds`——之后改 policy 不会重写历史。原始行永不 UPDATE；`effective_intensity` 从不落库、从不发 lifecycle 事件或票。
- §78.2 契约：`AffectKind` / `EmotionLabel` / `AffectTargetScopeKind` 与 0156 的 CHECK 字面集合逐字对账（`tests/memory_affects.rs`）；`BasisPoints::signed/unit` 构造器是 Rust 侧的 range CHECK，越界 = `INVALID_INPUT`，**永不 clamp**。

### D-C 写入：一个行签发点，三条入口各在自己的事务里，没有事后第二事务
- 行签发点 `affect_repo::resolve_targets_in_txn`（`target_subject` 经 `subject_repo::resolve_declaration_in_txn`，rule 1/2，未知 ⇒ `INVALID_INPUT`，永不自动注册，与卡 8 一致；**永远在第一条写之前**）+ `affect_repo::insert_in_txn`（`AffectParent::Memory(id)` → `memory_affects`，`AffectParent::Evidence` → `evidence_affects`；同一份绑定代码，MOOD 行盖上冻结 half-life）。
- `memory.annotate_affect {memory_id, affects:[{kind,label?,valence?,arousal?,dominance?,intensity,confidence,target_subject_id?|target_subject_key?,target_scope?,observed_at?}]}`（`AffectWriteOp::Annotate` 闭集，guard `run_local_write` 普通 admitted write，**无 confirm gate**——什么都没删没藏，§33.10 只管破坏性操作）。`affect_repo::annotate` 一个事务：RLS 下核对 memory 可见且为 active head（不可见 ⇒ `NOT_FOUND`；superseded ⇒ `CONFLICT`，去注解 head）→ 解析 → INSERT（`evidence_id` = memory 的 PRIMARY Evidence）→ 发一张 `MEMORY_LIFECYCLE` 票（§60 `next_commit_seq + issue_stream_log_row + insert_outbox` 逐字，与 `memory_governance_repo::issue_lifecycle_ticket` 同一序列）让 worker 以同一确定性 point id 重投影带 affect payload 的点。
- `memory.correct {affects:[...]}`：更正即「新版本重新提供 affects」。`CorrectRequest.affects` 进入 `correct_atomically`：目标 subject 在 confirm token 消费**之前**解析（未知 ⇒ `INVALID_INPUT`，token 完好、零写入，可用同一 token 重试）；arbiter UPDATE 之后、COMMIT 之前 INSERT 到 M2（provenance = E2）；**只有那一张** `MEMORY_LIFECYCLE` 票，结果的 `affect_ids` 来自同一事务（replay 分支从 M2 的行读回）。gateway 在提交后不再做任何 affect 写。
- `remember.put {affects:[...]}`：memory 行在 Distill hop 异步出生、remember 时没有 memory_id，所以与 `evidence_subjects`（ADR-0028）同形：`RememberCommand.affects` 在 `remember_in_txn` 第一条写之前解析目标 subject（未知 ⇒ `INVALID_INPUT`，零 Evidence、零计量），与 Evidence 同事务落 **`private.evidence_affects`**（0157 写侧载体：列与 CHECK 与 `memory_affects` 逐字相同、减 `memory_id`；同一 0156 immutability 触发器函数；gateway `SELECT, INSERT`，private_worker / maintenance `SELECT`）。0157 在 `private.memory_evidence` 上的 `AFTER INSERT ... WHEN (role='PRIMARY')` SECURITY INVOKER 触发器 `memory_affects_inherit_from_evidence` 在 memory 出生的事务里把该 Evidence 的 affect 行逐字复制到 `memory_affects`（provenance / target / observed_at / half_life 原样，不重算）。Distill hop、`memory.confirm`、`memory.correct` 全经 `distill_repo::insert_memory` 这唯一 memory INSERT，所以没有 memory 写者能漏掉它；投影由 hop 自己的票带出，没有事后写、没有与 hop 的竞争。
- **未做 Distill hop 直出 affects（主线裁决 3：MAY，本卡跳过）**：`distill_reasoner` / `distill_repo` 不在允许清单。闭集与 `BasisPoints` 构造器已就位；升级信号：当 reasoner 的 candidate envelope 需要带模型观察到的情感时，在 envelope 解析处调 `affect_repo::parse_affects`，沿 `insert_memory` 事务经 `insert_in_txn(AffectParent::Memory)` 落行（越界整条 candidate 拒绝，永不 clamp）。

### D-D 读：get/enumerate 带 affects；recall 结构化预过滤 + PG 复核；mood 一致性是有界 late rerank；**没有情感向量**
- `memory.get` / `memory.enumerate` 的 item 带 `affects:[...]`——每行原值 + 读时算的 `effective_intensity`（`context.output.schema.json` items，`additionalProperties:false` 不变）。一次集合读（`AFFECTS_FOR_MEMORIES_SQL`，`memory_id = ANY($2)`）。
- Qdrant payload：**六个平铺数组字段** `affect_kinds / affect_labels / affect_valence_bp / affect_arousal_bp / affect_dominance_bp / affect_intensity_bp`（`projection::dense::AFFECT_*_FIELD`，写读同一拼写），一条注解在每个数组同一下标占一格，未记录的轴写 `null`。projection worker 在 `resolve_memory` 同事务读 `memory_affects`（`role_retrieval_worker` 自己的 SELECT），`finish_row` 经 `IndexablePayload::with_affects` 写入。**不建情感 embedding**：继续用既有语义向量，affect 只是 structured payload + rerank 特征。
- `recall.search {affect:{kinds?,labels_any?,valence:[lo,hi]?,arousal?,dominance?,min_effective_intensity?}}`：`DenseQueryFilter::with_affect` 把 `Or(Eq …)`（kinds/labels）与新增的 `Condition::Range`（VAD 区间；`min_effective_intensity` 映射到 **raw** intensity 的下界——effective ≤ raw，raw 界是超集）AND 到 tenant + visibility + subject 子句之后（同一个 Qdrant filter，§17.1 单构造点不变，`no_handwritten_filter_scan` 仍只放行 `dense.rs`）。**Qdrant 只是预过滤**：数组字段的「任一元素匹配」在多子句下是跨注解的过近似，而且算不了衰减；PG hydrate gate（`read_materialize::final_memory_ids_about_in_txn`，与 `include_archived` / subject 复核同一处）用**一次**集合读拿到候选集的全部 affect 行，经 `application::affect::memories_matching` 按「一条注解满足全部子句、以 effective intensity 判 min」复核。带 affect 过滤时 overlay 的裸 Evidence 项被丢弃（Evidence 没有 affect 行，不可能满足过滤；它链接的 memory 仍进候选集过同一道门）。
- `recall.search {mood_congruence:{valence,arousal}}`：hydrate 之后对**已可见集合**按 `memory_congruence`（`10000 − (|Δv|+|Δa|)/4`，注解取最大；无数据取中点 5000）稳定降序重排（`application::affect::rerank_by_mood`）。是置换：不增不减，平局保持 dense 序；信封 `reranked_count` = 被重排的 memory 数。

### D-E 治理
- affect 行永不 mutate。「我其实没生气」= `memory.correct`（新版本重新提供 affects，旧 affect 行随被 superseded 的版本）。restore 恢复历史版本连同它的 affects；archive/unarchive 不碰 affects。subject ERASE 级联删 `target_subject_id` 指向它的 affect 行；memory purge 级联删全部。衰减 ≠ 删除，从不发 lifecycle 事件或票。

## 否决
- **`MemoryType::Emotion` / 第四种记忆本体**：类型轴被污染，所有公开实现都把情感当附加维度。
- **float 存 VAD**：NaN / rounding / 等值边界；basis points 整数 + CHECK。
- **用 `abs(arousal)` 代替 intensity**：高强度悲伤是低 arousal。
- **情感向量 / 第二条 embedding**：既有语义向量足够，affect 走 structured payload + rerank。
- **Qdrant nested object 数组 `affects:[{…}]`**：需要 `nested` filter 变体 + 每子句一层嵌套；平铺数组用现有 `Eq/Or` + 一个 `Range` 变体即可，PG 复核弥补跨注解过近似。
- **在 SQL 里复算衰减做复核**：会出现第二份衰减公式（§78 无第二字面）；改为一次集合读 + domain 纯函数。
- **mood 衰减写回 / lifecycle 事件**：衰减是读时派生值。
- **remember.put / memory.correct 的事后第二事务 affects**：与 Distill hop 竞争、提交后失败留下无 affects 的版本、两张票（卡 8 P0 同型；主线裁决 1/2 判为 P1）。改为 evidence 载体 + 出生事务内复制、`correct_atomically` 事务内 INSERT。
- **给 affect 建独立 confirm gate**：注解不删不藏，§33.10 不适用。

## 验收（真 DB / 真 Qdrant，非 unit-only）
- `crates/adapters/tests/memory_affects.rs`：CHECK 字面集合 ↔ `AffectKind::ALL` / `EmotionLabel::ALL` / `AffectTargetScopeKind::ALL`（§78.2，`memory_affects` 与 `evidence_affects` 两表逐字相同）；`evidence_affects` 在 `role_private_worker` 的 PRIMARY `memory_evidence` INSERT 上复制到 `memory_affects`（非 PRIMARY 不复制）、UPDATE 被同一触发器拒绝；`role_gateway` 下 UPDATE 被触发器拒绝（`23514`）而 DELETE 无权限（`42501`）；subject 删除级联删 affect 行；**一次 round trip**：50 条候选一次 `AFFECTS_FOR_MEMORIES_SQL`，`pg_stat_xact_user_tables` 上 `memory_affects` 的 scan 增量恰为 1（逐行查询会是 50）。
- `bins/gateway/tests/mcp_gateway.rs`：`native_mcp_affect_annotation_governance_acceptance`（annotate EMOTION(FRUSTRATION, valence<0, target Person) + MOOD(observed_at 提前两个半衰期) → get 的 `effective_intensity` < raw 且原值不变、enumerate 同 → 未知 subject / 未知 label INVALID_INPUT 零写 → correct{affects} 带未知 target key ⇒ INVALID_INPUT、M1 仍 active、同一 token 随后成功 → 新版本带新 affect（同一事务、一张票）、旧行随 M1（`memory_records.superseded_by` ⇔ status，G59-4 CHECK 恒真）→ owner 删 Person，指向它的 affect 行级联消失）；真 Qdrant 套件 `native_gateway_semantic_recall_real_qdrant_pg_and_ryw_acceptance` 的 affect A/B 矩阵：按 label / valence 区间只召回带该情感的点、payload 声称 FRUSTRATION 但 PG 无行的点被 hydrate gate 丢弃、`min_effective_intensity` 排掉衰减后的 MOOD 但保留 EMOTION、`mood_congruence` 把最贴近的排第一；记录 recall p50 with/without affect filter（只记数字，基线由卡 24 定）。

## 已知局限
- Distill hop 直出 affects 未落地（主线裁决 3 接受；升级信号见 D-C 末条）。
- `evidence_affects` 与 `memory_affects` 的 CHECK 字面集合在 SQL 里是两份（SQL 无法共享 CHECK）；`tests/memory_affects.rs` 钉两表逐字相等，Rust 侧仍只有 `AffectKind::ALL` / `EmotionLabel::ALL` / `AffectTargetScopeKind::ALL` 一份。
- `min_effective_intensity` 的 Qdrant 预过滤按 raw intensity；衰减很深的 MOOD 会进候选集再被 PG 丢掉——正确但多占一个候选名额（`cand_k` 内）。
- `mood_congruence` 只用 valence/arousal（circumplex）；dominance 不参与一致性。

## 主线裁定与补正（2026-09-06，opus 审查后）
- **P1 ×2（已由 fix 阶段按裁定落地）**：`memory.correct{affects}` 的替换版本 affect 行改在 `correct_atomically` 事务内写入（一张 MEMORY_LIFECYCLE 票，不再是提交后的第二事务）；`remember.put affects: [...]` 进入 `RememberCommand` 的原子路径（remember.schema.json + remember.rs + 五个测试构造点），未知 target_subject_key 在证据被接受**之前**以 §52.1 协议级 INVALID_INPUT 拒绝。
- **P2 未来 `observed_at`（主线补）**：MOOD 的 observed_at 若在未来，elapsed 饱和为 0 → 永不衰减，客户端可借此打穿 Emotion/Mood 区分。DB 无法用 CHECK 表达 now() 边界，故在写入路径（`affect_repo` 解析 `observed_at`）拒绝 > now+5min 时钟偏差容限的值（InvalidInput），并加单测（未来 1h 拒、过去 1min 允）。
- **P2 票签发重复**（affect_repo::annotate 复制了 memory_governance_repo::issue_lifecycle_ticket）→ 归卡 20 ledger 泛化时并回唯一签发点（同时解决 PRIMARY-first LIMIT 1）。
- **P2 restore/archive 两条治理子句无断言** → 归卡 24 全链演练见证（对已注解记忆做 supersede→restore 与 archive→unarchive，affect 行不变、读取时重算 effective_intensity）。
- 故障注入 M1–M6（Emotion 衰减 / 持久化 effective_intensity / 信任 Qdrant 不复检 / mood_congruence 扩集 / 未知 key 自动建 subject / 给 gateway UPDATE 授权）全部被对应测试或 rls-check 抓住。
