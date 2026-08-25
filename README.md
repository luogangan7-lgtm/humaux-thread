# Humaux Thread

**Humaux Thread — Persistent context infrastructure for AI agents.**
（Humaux 母品牌 · Thread 产品名 · AI Agent 持久上下文与知识记忆基础设施）

- Spec（唯一规范真源）: `docs/architecture/Baseline_2.8.md` — Architecture Frozen, GO for Phase 0.
- 开发计划: Humaux 记忆库 `[decision] Humaux Thread 0→1 开发任务计划 v1 定稿`（118 任务卡，Phase 0–17）。
- 开发规范: `CLAUDE.md`（本仓所有 agent 必读）。

## Workspace 布局（spec §58 contract）

`crates/*` 每个顶层目录一个 crate（§58 树的子目录 = crate 内模块）；`bins/*` 七进程 + `admin`（§4.4）；
`xtask` 工程门禁（architecture-check / mechanism-registry / metrics-registry / contract-impact / …）。

## 快速验证

```bash
cargo check --all-targets
cargo xtask architecture-check
```
