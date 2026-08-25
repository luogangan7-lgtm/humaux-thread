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

## Email deploy gate

§74.6: "生产域名配置 SPF/DKIM/DMARC 属于部署 Gate" — before pointing a production sending
domain at the live `EmailProvider` adapter, verify SPF/DKIM/DMARC DNS records for that domain
and record the result in `ops.email_domains` (`spf_verified_at` / `dkim_verified_at` /
`dmarc_verified_at`, one row per domain, `is_production = true` for anything actually sending
production mail). A production domain with any of the three `*_verified_at` columns still
`NULL` fails this gate.

This is a manual deploy-process check today, not a CI gate — there is no G-numbered automated
check for it (unlike `cargo xtask architecture-check` above). ponytail: wiring an automated
check against `ops.email_domains` is a later add, add it when a deploy has actually shipped
with an unverified production domain.
