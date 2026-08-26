# ADR-0006 — 一道闸的 `not_applicable` 条件，不得与它要抓的违规状态重合

- 状态：Accepted
- 日期：2026-08-27
- 关联：§16.2（换代期读路由）、§57.1（三态）、§80.1（准入条件）、§6.2.2/§6.2.3（角色矩阵与 typed pool 闭集）
- 触发：相位闸族审计里对抗验证后站住的 4 条之一

## 事实

`G80-4` 的判据是 §16.2：「检索侧禁止把 `projection_version` 当常量读，只能经
`serving_version(stream_family)` 取；architecture-check 断言全 workspace 该调用点只允许存在
一处」。而它先前的实现是：

```rust
if sites.is_empty() {
    return Verdict::NotApplicable("…adapters::retrieve 尚未接入换代期读路由…尚未交付");
}
if count == 1 { Pass } else { Fail(...) }
```

**「一个调用点都没有」恰恰就是 §16.2 被违反的状态本身。** 于是这道闸在违规最严重的时候
最安静：读路径完全不走读路由 ⇒ NA ⇒ 不计入退出码 ⇒ CI 绿。§57.1 表里 G80-4 自 Phase 5
起必过，实际到 Phase 7 它一次都没有真正判过。

被它掩盖的是一个有真实后果的缺陷：`crates/adapters/src/retrieve.rs` 的 `projection_version`
取自**客户端提交的 consistency token**，随后直接当查询条件用。一个指向已退役版本的 token
会让读路径去读那个版本的行，而不是路由到 `serving` 行——正是 §16.2 那句「读路由只打
`serving` 行」要禁的。

## 根因

`not_applicable` 的合法理由（§57.1 第2条）是**被测对象尚未交付**。这道闸把「被测对象」错认
成了「调用点」，而调用点为零正是违规。真正的被测对象是 `serving_version` 这个函数本身——
它一直都在（`serving_repo.rs:118`，rustdoc 完整），缺的是**调用它**。

`serving_repo.rs` 的模块 doc 还把这层豁免写成了自述：「as of this task the retrieval read
path does not yet route through `serving_version` … so G80-4 是 not_applicable」。
**一道闸不该在自己的被测代码里给自己开豁免条。**

## 决定

### 1. NA 的主语改成函数本身

```rust
if !files.iter().any(|(_, s)| s.contains("pub async fn serving_version(")) {
    return Verdict::NotApplicable("missing object: §16.2 读路由入口 … 尚未交付");
}
match count { 1 => Pass, 0 => Fail("消费侧调用点为 0 …"), n => Fail(...) }
```

零调用点从 NA 变成 **Fail**，并在信息里明说「被测对象就在那儿，缺的是调用它」。

### 2. 排除按精确路径，不按子串

原实现用 `disp.contains("serving_repo")` 排除定义点，这会连带排除任何路径含该子串的文件
（`serving_repo_client.rs`、`tests/serving_repo_wiring.rs`……）——真调用点藏进那种文件就
永远数不到。改为精确路径列表，并配注错 `g80_4_substring_named_file_does_not_escape_the_count`。
与 G80-22.5 踩过的「改名绕过」同型：**任何按名字匹配的判据，都要立刻问一句「改个名是不是
就躲过去了」**。

### 3. 同一批把违规修掉（否则闸落地即红）

- `serving_repo::serving_version` 的 pool 从 `RetrievalWorkerDbPool` 换成 `RuntimeDbPool`。
  §6.2.3 的 typed pool 是闭集、无任何转换路径，而读路由是**请求路径上的纯读**，调用方是
  `recall_with_overlay`；要么在这里换边，要么让请求路径凭空多拿一个 pool。授权侧支持：
  `migrations/0011_roles_and_grants.sql:257` 给 `role_gateway` 的是表级无列限定
  `GRANT SELECT ON projection.stream_checkpoints`（迁移与运行库双向核实过），写面不变。
- `recall_with_overlay` 增加 §16.2 的**唯一消费侧调用点**：拿 stream key 去掉 version 的
  五列组成 family 去问 serving 版本——版本本身正是要问出来的东西，不能拿 token 里那个去问。
- `serving_projection_highwater` 的 SQL 加 `AND serving`。**这是修根因的那一行**：token 指向
  退役/shadow 版本时无行 ⇒ 水位 0 ⇒ 整条流 fail-closed 走 overlay（从 PG 直读），而不是
  相信一个非 serving 版本的水位。
- `RecallEnvelope` 增加 `serving_version: Option<String>`，作为检索面唯一合法版本入参。
  `None` = 该 family 尚无 serving 行（正常引导态），此时不得构建任何 visible count filter：
  没有 serving 版本时「可见计数」没有分母，按 §57.1 那是 `cannot_establish` 而不是 0。

### 4. 不再另设「token.version == serving version」等值守卫

`ux_serving_one`（migration `0065`）保证每 family 至多一行 `serving = true`，因此
「六列命中且 serving」与那道等值判定**在所有输入下同结果**。留两道就必有一道永远观察不到
自己失败——按 §80.1 那种判据不算判据。

### 5. 未采纳：常量扫描臂

设计阶段有方案提出再加一条「扫 `projection_version: "` 字面量」的臂。不采纳，两个理由：
§16.2 自己指定的执行机制就是调用点计数（「断言全 workspace 该调用点只允许存在一处」），
常量扫描是方案外加的；且综合评审自己算出它**落地即红**——`crates/retrieval/src/envelope.rs`
的测试夹具里有一处合法的 `projection_version: "dense-v3"`，而现有的
`strip_cfg_test_module` 是裸字节深度计数、不认识字符串字面量（其自身 rustdoc 已声明这个
天花板），剥不干净。为一条会误报的臂改一个通用 helper，代价与收益不成比例。

// ponytail: 常量扫描留作升级路径；真要做，前提是先把 `matching_brace_end` 换成 `syn`，
// 而那是一次独立改动，不该搭在本 ADR 上。

## 实测

| | 改前 | 改后 |
|---|---|---|
| `architecture-check` G80-4 | `not_applicable` | **`pass`**（恰好 1 处消费侧调用点） |
| 零调用点 | `not_applicable`（假绿） | `Fail` 并点名「消费侧调用点为 0」 |
| 函数本身不存在 | 与零调用点不可区分 | `not_applicable` 并打印缺失对象名 |
| 子串同名文件里的调用点 | 被静默排除 | 被计入并点名 |

六条注错测试（fixture 树，不在真仓变异）：green / zero-is-fail / na-only-when-absent /
second-call-site / substring-escape / real-repo-passes。

## 可推广的判据

**写完一道闸的 `not_applicable` 分支，问一句：这个条件成立的时候，被测规则是不是正好被
违反着？** 是，就说明 NA 的主语选错了——它应该是「被测对象存不存在」，不是「有没有观测到
符合的东西」。后者在违规时同样为空，两种状态因此不可区分。
