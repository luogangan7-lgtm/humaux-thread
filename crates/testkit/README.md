# humaux-testkit

测试基建 crate（§79）。不接真 DB，只立类型与契约——见 `src/lib.rs` 的
`DbIntegrationFixture` / `TenantPair` / `assert_fault_observed`。

## 目录

```text
src/lib.rs        DB fixture 契约 · A/B 租户身份夹具 · 注错断言辅助
sentinels/         §53.3 规则3 正哨兵样本（*.rs.sample，不参与编译）
tests/fault/       §53.4 每个 DegradeCode 变体一条注错测试
tests/pair/        G52-5 ErrorCode/DegradeCode 成对终态测试
tests/metrics/     G80-6 / D1–D6 Metric Witness
```

## proptest / loom / cargo-fuzz 挂点（Advanced Rust Testing / Architecture Tests，§79.8 之后未编号章节）

- **proptest**（`dev-dependencies`，见 `Cargo.toml`；spec Property-based
  小节）：quota reservation invariants、scope normalization、cache keys、
  ID generation、completeness aggregation、public provenance closure。挂在
  各自逻辑所在 crate 的 `#[cfg(test)]` 内，`humaux-testkit` 只声明依赖版本，
  不代跑。
- **loom**：lease/fencing、quota reserve/finalize、refresh token rotation、
  job claim、scheduler leader 这类并发逻辑，验证 race 而非只压测「没撞出
  来」。尚未引入依赖——待对应逻辑落地的 crate 自行加 `loom` dev-dependency。
- **cargo-fuzz**：MCP input schema/decoder、URL/SSRF parser、artifact
  metadata、JWT/OAuth edge parser、public contribution input。同上，待
  对应 crate 落地后再建 `fuzz/` 目录。

## Mutation targets 优先级（§79.7）

对整仓跑 mutation 很贵；按此优先级选目标，不追求全仓覆盖：

1. **Contract Kernel**（唯一构造点、`Outcome`/`abstain()`、`ErrorCode`/
   `DegradeCode` 互斥判定）
2. **security**（cross-tenant 隔离谓词、RLS、鉴权矩阵）
3. **billing**（webhook 幂等、配额并发预留、referral 防重复发放）
4. **retrieval**（completeness 聚合、candidate packing、degraded fallback）

关键不变量的 mutation target 示例（§79.7）：

```text
remove tenant predicate           -> red
change !=1 to >1                  -> red
remove wait visibility            -> red
fail-open metric removal          -> red
skip quota atomic predicate       -> red
bypass confirmation               -> red
```
