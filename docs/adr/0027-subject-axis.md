# ADR-0027: Subject / Aboutness 轴基础 —— `private.visibility_allowed` 纯函数 + subject 注册表（研究问题 Q7/Q8）

日期：2026-09-05 · 状态：Accepted · 影响面：`migrations/0153_subject_registry_and_visibility_fn.{sql,manifest.toml}`（新纯函数 `private.visibility_allowed` + `ALTER POLICY` 重指三条 0012 可见性策略 + 新表 `private.subjects`/`subject_keys`/`subject_roles`）/ `crates/domain/src/subject.rs`（新，`SubjectId`/`SubjectKind`/`SubjectRole`/`SubjectKeyKind`/`SubjectKey`）+ `lib.rs`（模块注册）/ `xtask/src/rls_check.rs`（冻结策略 hash 重指 + §6.2.2 `ADDITIVE_SEAM_MATRIX` 三表 cell）/ `docs/architecture/Baseline_2.9.md`（新 §6.1.3 + §6.2.2 矩阵三列）/ `crates/adapters/tests/subject_registry_rls.rs`（新，注错/隔离/解析/契约）+ `auth_scope_rls.rs`。前置：卡 1–6 已在树上（HEAD d5709e7，迁移至 0152）。

## 背景
用户的「客户记忆 / 用户跟人记忆」需求在任何层都没有表示：迁移里没有 subject/person/customer/aboutness 概念，Rust 树里唯一的 `subject_*` 命中是 `control.rate_buckets` 的限流 `subject_kind`（`migrations/0113`），Baseline 里没有一个 aboutness 章节。没有 aboutness 轴，「我知道关于这个客户的什么」退化成对自由文本的语义猜测。整套设计所依赖的 SECURITY DEFINER 谓词需要一个此前不存在的 SQL 函数 `private.visibility_allowed(...)`——今天这个析取在 `migrations/0012_rls.sql` 里内联了三遍，并在 Rust `crates/domain/src/identity.rs::can_read` 里镜像了一遍。本卡只落**基础**。

## 研究问题 Q8（可见性谓词的单一 SQL 家）
**决定：新迁移建纯 `private.visibility_allowed(p_class text, p_tenant_ok bool, p_user_ok bool, p_workspace_ok bool) RETURNS boolean IMMUTABLE PARALLEL SAFE`；一个前向迁移用 `ALTER POLICY` 把三条策略重指到它，把原 tenant/user/workspace 表达式作为三个布尔传入；同一 PR 重指 `rls_check` 冻结策略 hash。**

- 函数体是纯查表：`TENANT_SHARED→p_tenant_ok`、`USER_PRIVATE→p_user_ok`、`WORKSPACE_SHARED→p_workspace_ok`、其余 `false`——与它替换的 OR 链代数等价。IMMUTABLE PARALLEL SAFE，不读任何表，可被 planner 内联。
- **饿汉求值守卫（本卡的核心非平凡取舍）**：把析取抽成函数使其参数变为**饿汉求值**（SQL 在调用前求所有参数），而原 OR 链是**短路**的。原先被跳过的 `current_setting('humaux.user_id', true)::uuid` 在一个 user_id GUC 未设（空串）的会话上会新抛 `invalid input syntax for type uuid: ""`。故传入函数的每个 user_id cast 都包 `NULLIF(current_setting(...), '')::uuid`——空→NULL→该臂 false，绝不报错。这把一个「本会报错」转成「fail-closed 的 miss」，**不改变任何合法读**（合法 uuid GUC 不受 NULLIF 影响；只有未设那一种——原本会报错的——变成干净的 false）。tenant cast 保持与 live 策略逐字一致（它们本就在顶层 AND，一向饿汉求值；NULLIF 只包 user_id，不包 tenant_id——三条策略在这一点上保持一致）。回归测试 `unset_user_id_session_reads_tenant_shared_rows`：`humaux.user_id = ''` 的 gateway 会话经三条策略读 TENANT_SHARED 行必须各得 1 行且不报错——删掉任一 NULLIF 即红。
- 函数 `OWNER TO role_migration_owner`（与 0104 的 guard 函数同法；0011 的批量 re-own 循环已跑过，触不到之后建的函数）——三张 private 表的可见性谓词是安全代码，不能留在迁移登录角色名下。manifest postcheck 断言 owner。
- **重指用 `ALTER POLICY`**（不 DROP/CREATE，与 0140/0145/0146/0147 同一手法），保留每条策略的 tenant 边界与每个 headless-role 臂逐字不变；只有 user-facing 三臂析取变成 `visibility_allowed(...)` 调用。`evidence_objects_tenant_and_visibility` 的冻结 hash 从 `c77b3b83…`（内联 OR）移到 `d1d8ed74…`（函数形），USING == WITH CHECK 同 hash。0012 字节不变（前向修复，§46）。

## 研究问题 Q7（subject 注册表在哪、什么形状、谁授权）
**决定：新 §6.1.3「Subject / Aboutness 轴」章节（本卡规范插入）+ 三张 tenant-scoped 表。**

- `private.subjects(subject_id uuidv7 PK, tenant_id FK CASCADE, kind CHECK PERSON|ORGANISATION, display_name, created_at, merged_into 自引用 SET NULL (merged_into))`——aboutness 锚，`merged_into` 支持去重/合并（NULL=当前 head）。**所有指向 subjects 的 FK 都带 tenant leg**：`UNIQUE(tenant_id, subject_id)` 作为受约束冗余（0104 `evidence_objects_tenant_id_unique` 同法），`subject_keys`/`subject_roles`/`merged_into` 都是复合 `FOREIGN KEY (tenant_id, subject_id)`。RI 检查绕过 RLS，单列 FK 会让租户 B 把行挂到租户 A 的 subject 上并探测其存在（23503 vs 成功 = 存在性 oracle）；测试 `subject_fks_reject_foreign_tenant_subjects` 断言外租户 id 与不存在 id 得到同一个 23503/同一约束名。
- `private.subject_keys(subject_key_id uuidv7 PK, tenant_id FK, subject_id FK CASCADE, key_kind CHECK CRM, key_value, created_at, UNIQUE(tenant_id,key_kind,key_value))`——外部身份去重键，**永不全局唯一**（唯一约束带 tenant_id：同一 CRM id 在两租户下是两条 key；测试 `subject_keys_unique_per_tenant_never_globally` 断言同租户重复 → 23505、异租户同键 → 成功）。
- `private.subject_roles(tenant_id FK, subject_id FK CASCADE, role CHECK CUSTOMER, created_at, PK(tenant_id,subject_id,role))`——subject 扮演的角色，**与 ScopeKind 正交**（Customer 是 subject *所是*，不是记忆*归档于*的 scope）。
- 三表 §62 tenant 隔离（ENABLE+FORCE RLS，tenant policy），owner `role_migration_owner`。**NAMED §6.2.2 授权**：`role_gateway`/`role_private_worker` `SELECT,INSERT`（注册是用户动作；卡 8 的确定性 resolve 由 private worker 写）；`role_retrieval_worker`/`role_maintenance` `SELECT`；其余非 owner `—`。`rls_check` 的 `ADDITIVE_SEAM_MATRIX` + 矩阵三列 + §48.2 T 集合 68→71 同一变更落地。

## 研究问题 Q7（域类型 D-C）
`SubjectId`（uuidv7 newtype，**刻意不在 `ids.rs`**——那模块冻结在七个 `Scope` id；subject 是 aboutness 不是 scope）/ `SubjectKind {Person, Organisation}` / `SubjectRole {Customer}` / `SubjectKeyKind {Crm}` / `SubjectKey {kind, value}`——闭集，`as_str`/`parse`/`ALL`，§78.2 DB↔Rust 契约测试对 live CHECK 逐一比对。

## 越界 / 刻意收窄（需主线知会）
- **D-E（MCP subject 注册 op + e2e）未落地**：其必然触及 `contracts/mcp/manifest.json`、`bins/gateway/**`、`crates/gateway/tests/mcp_gateway`——**都不在本卡允许改动文件清单内**（清单显式「Anything else → report, don't edit」）。MCP 接线与 subject.register/link_key 属卡 8 的写路径（卡片自己提到「deterministic resolve hook in card 8」）。本卡按边界只落基础（迁移/域/xtask/文档/ADR/测试），并把 `mcp_gateway` 套件作为回归项跑绿（未加新 gateway 测试）。
- **`SubjectRef` / `ByteSpan` / `SubjectRefSource` 未落地**：卡片 Scope 叙述点名了它们，但其后备表 `memory_subject_mentions` 与唯一消费者（卡 8 resolve hook）本卡都不建。落它们=为将来搭脚手架且变体无任何 DB CHECK 可锚（§78.2 无从测），违背 ponytail「no scaffolding for later」。随 mentions 表一并落地。
- **`subject_keys` 授权**：Scope 叙述有一句「no runtime role gets direct SELECT — only the SECURITY DEFINER predicate reads it」，与 D-B 显式授权（gateway/private_worker SELECT,INSERT）相冲。采信 D-B（Decisions 是权威裁决，且一个读不回的注册表无法支撑 e2e 的「按租户读回」）；SECURITY DEFINER 收窄留给卡 8 的 resolve hook 硬化。已在本节标注供复核。

## 验收
Live-DB：抽出的谓词行为等价——0012 策略改调函数后既有 RLS 隔离测试全过，外加一个 parity 测试断言 `private.visibility_allowed` 在 scope×class 穷举矩阵上与 `identity.rs::can_read` 一致；`private.subjects`/`subject_keys` 在非 superuser 角色 `SET LOCAL` 下跨租户隔离；`cargo xtask rls-check` 绿（含 T 集合派生检查——矩阵行缺失即红）；注错：把抽出析取的一个臂改成永真，parity 测试与一个隔离测试必红（`parity_is_fault_sensitive`：同一回滚事务内，USER_PRIVATE 臂永真前 gateway/U2 经三条策略读 U1 的 USER_PRIVATE 行 = 0，永真后 = 1，且 parity 矩阵出现分歧）。门链：`migrate`（drift 0）/ `rls-check` / `architecture-check` / `cargo test -p humaux-gateway --test mcp_gateway`（回归）/ 单测（domain subject）/ clippy / fmt / secret grep + opus review。

## 主线裁定与证据（2026-09-05，opus 审查后）
- **P0 外键无租户腿**（fix 已落 0153：`subjects` 加 `UNIQUE(tenant_id, subject_id)`；`subject_keys`/`subject_roles`/`subjects.merged_into` 均改为复合 `FOREIGN KEY (tenant_id, …) REFERENCES private.subjects(tenant_id, subject_id)`；新测 `subject_fks_reject_foreign_tenant_subjects`：租户 B 引用租户 A 的 subject_id 与引用随机 uuid 得到相同 (SQLSTATE 23503, constraint)——无存在性 oracle）。0153 未提交、仅本地已应用，故直接修文件并重放同步账本 checksum（00dc7492e0febb0e），新库重放同态；live 核实 conkey=2 ×3、UNIQUE 在、函数 owner=role_migration_owner。
- **P1 tenant 转型**：evidence_objects/memory_records 四处 tenant 转型恢复为 0012 原样 `current_setting('humaux.tenant_id', true)::uuid`（NULLIF 仅包 `humaux.user_id`）；三条策略一致；rls_check 冻结哈希 re-pin e877f26c…。新测 `unset_user_id_session_reads_tenant_shared_rows` 钉住 user_id NULLIF 守卫（去掉守卫→22P02）。`parity_is_fault_sensitive` 改为在故障下穿过三条真实策略断言隔离变红（7/7）。
- **P2 短路丢失（热路径逐行 EXISTS）**：以 role_gateway 对 `private.memory_records` 做 `EXPLAIN (VERBOSE)` 核实——PG 18 **内联**了 `private.visibility_allowed`（计划中无函数调用，CASE 直接出现在 Filter），memberships 子查询成为 **hashed SubPlan**、仅在 `WHEN 'WORKSPACE_SHARED'` 臂惰性求值：短路保留，成员表每查询一次索引扫描而非逐行。裁定：不改代码、不加脆弱的计划断言；本段 EXPLAIN 摘录即证据。若日后改函数使其不可内联（引入 STRICT/多次引用参数/非 SQL 语言），需重新核实。
  ```
  Filter: (... CASE memory_records.visibility_class WHEN 'TENANT_SHARED' THEN true
           WHEN 'USER_PRIVATE' THEN (visibility_user_id = NULLIF(current_setting('humaux.user_id',true),'')::uuid)
           WHEN 'WORKSPACE_SHARED' THEN (ANY (tenant_id = (hashed SubPlan 2).col1)) ELSE false END)
  SubPlan 2 -> Index Scan using memberships_tenant_id_user_id_key on control.memberships m
  ```
- **未捕获故障「重复 subject_key」**：新测 `subject_keys_unique_per_tenant_never_globally`（同租户重复 23505；跨租户同 key 允许）。
- **D-E**（MCP subject.register/link_key + 网关级跨租户 e2e）移入卡 8；SubjectRef/ByteSpan 随 memory_subject_mentions 一起在卡 8 落地。
