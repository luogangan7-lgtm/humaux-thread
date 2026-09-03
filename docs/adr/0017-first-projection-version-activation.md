# ADR-0017: 首个投影版本的激活（§16.2 蓝绿切换的「无 serving 版本」情形）

日期：2026-09-03 · 状态：Accepted · 影响面：`crates/projection/src/serving.rs`（`SwitchCriteria.first_activation`、`evaluate_switch`）/ `crates/adapters/src/serving_repo.rs`（由 checkpoint 行推导）/ `xtask projection-serve`（运维子命令）

## 背景（部署点演练实测）
`recall` 只读 `projection.stream_checkpoints` 里 `serving = true` 的版本（`serving_repo::serving_version_in_txn`）。`serving` 默认 false；投影 worker 只推进水位，从不切 serving；`switch_projection_version` 是 §16.2 的蓝绿切换，`evaluate_switch` 要求 **shadow 与 serving 两个版本的可见读回计数相等且版本不同**。一个全新租户没有 serving 版本，于是任何切换都被 `VisibleUnavailable` 拒绝，`recall` 永远 `DEPENDENCY_UNAVAILABLE`——生产里也没有任何调用者执行这一步。

## 决定
1. `SwitchCriteria.first_activation`：当该流族在 DB 里 **没有** serving 行时（`current_serving_version.is_none()`，由 DB 层推导，调用方不能声明），`evaluate_switch` 只要求：shadow 版本有可见读回（`visible_shadow = Some`）、`shadow_open_gaps == 0`、continuation 为 Pass。同版本/计数不等的比较不适用于首次激活。一旦存在 serving 版本，规则回到原 §16.2（两个版本比较）。
2. 新运维子命令 `cargo xtask projection-serve --tenant --workspace --domain --projection-kind --version --visible-shadow <n> [--continuation]`，以 `HUMAUX_MAINTENANCE_PG_DSN` 调 `switch_projection_version`；部署流程里「首次投影追平后切 serving」是显式的一步，演练 runbook 与 rehearse.sh 已加入。
3. 拒绝的替代方案：种子/夹具预设 `serving = true`（绕过 §16.2 的读回校验，且 checkpoint 行由 remember 创建，种子拿不到）；让投影 worker 自动切 serving（把发布决策藏进后台循环，违背 §16.2「切换是显式运维动作」）。

## 验收
`crates/projection` 单测：首次激活有读回即通过、无读回仍 VisibleUnavailable、有开放缺口仍 OpenGaps、非首次且无 serving 仍 VisibleUnavailable；演练：`projection-serve` 后 `serving = true`，recall 返回真实命中。
