# Humaux Thread

**Humaux Thread — Persistent context infrastructure for AI agents.**
（Humaux 母品牌 · Thread 产品名 · AI Agent 持久上下文与知识记忆基础设施）

- Spec（唯一规范真源）: `docs/architecture/Baseline_2.9.md` — Architecture Frozen；含 ADR-0001/0003/0004 的修正与 Baseline 2.9 Grounding 增量。
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

## Intra-cell network deploy gate

§83.4 / ADR-0003 (`docs/adr/0003-network-vs-egress-choke-point.md`): `IntraCellHttpTransport`'s
six AND criteria for same-Cell resource access (`IntraCellResource`, e.g. `QDRANT_REST`) split
code-level from deploy-level — criteria 1/2/3/6 are enforced by `crates/infra-cell` and checked
by `cargo xtask architecture-check`'s G80-3; criteria 4/5 are **not** CI-checkable (they are
facts about network topology and certificate provisioning, not about this repo's source) and
must be verified once per Cell before that Cell's `IntraCellResourceRegistry` is populated with
real endpoints:

- **No Internet/NAT route** (criterion 4): the Cell's private subnet(s) that
  `IntraCellResourceRegistry`'s `allowed_cidrs` name for each resource must have no route to
  the public internet and no NAT gateway attached — verify the VPC/subnet route table directly
  (`aws ec2 describe-route-tables` / equivalent) shows only intra-VPC and Cell-internal routes.
- **Destination identity** (criterion 5): the resource's real endpoint (e.g. the Qdrant
  cluster's REST listener) must present either an mTLS client-verified certificate or a TLS
  certificate whose SAN matches the exact internal DNS name `IntraCellResourceRegistry` is
  configured with, and any required API key (Qdrant: `api-key` header) must be provisioned and
  rotated through the Cell's own secret store, never a literal in `IntraCellResourceRegistry`'s
  construction call. Code-level support for this now exists — `ResourceEntry::new`'s `tls`
  parameter selects `https`/`http` (`HttpIntraCellTransport::execute` builds the URL scheme
  from it), and `IntraCellRequest.headers` carries any per-call header, including Qdrant's
  `api-key`. What remains deploy-level: provisioning the actual certificate/SAN and sourcing
  the API key's value from the Cell's own secret store at the call site — this repo's code no
  longer makes criterion 5 *impossible* to satisfy, only the credential material itself is
  still an operator responsibility.

Record both checks' results the same way the Email deploy gate above does — per-Cell, dated,
outside this repo's test suite. ponytail: no `ops.*` table for this yet (unlike
`ops.email_domains`); add one if a second Cell ships before this gate has a durable record.
