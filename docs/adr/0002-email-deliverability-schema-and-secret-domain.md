# ADR-0002: Email Deliverability Plane — schema落 ops.* 而非 control.*，新增 SMTP provider secret domain

日期：2026-08-26 · 状态：Accepted · 触发：H3（§74.6）落地 migrations/0036_email_deliverability.sql 时新增 5 张核心存储表 + 1 个新 provider secret domain（`SmtpConfig.username`/`password`），均命中 §78.6 冻结的 ADR 强制触发条件（"增加核心存储"、"增加新的 provider secret domain"）。此前该决策只写在 SQL 注释里，未走 docs/adr/ 正式记录——本 ADR 把推理原样迁出，登记为决策记录，不改变已落地的技术结论。

## 决策 1：五张新表放 `ops.*`，不放 `control.*`

§82 canonical 清单显式把 `control.email_suppressions` / `control.email_delivery_events` 列在 `control.*` 下，且对 `email_domains`/`email_provider_health` 完全沉默（grep §82 全文确认两个名字都不出现在其 control.\* 或 ops.\* 清单里——这是清单本身的缺口，不是本迁移可以从清单读出的归属）。

放弃 `control.*` 的具体原因：§6.2.1 定义 `control.*` 域默认对每个运行时角色是 SELECT-only；sibling migration `0034_email_auth_identity.sql` 对其八张 `control.*` auth 表已经踩过同一堵墙，并记录为一个已接受的、尚未关闭的 gap（"该域的每张表都拿到 §6.2.1 的域默认——SELECT-only，MATRIX 没有为它加写权限单元格，因为 spec 正文的任何 ```sql INSERT/UPDATE 块或 role+verb+table 断言都没点名这些表"）。该 gap 对 `control.user_emails`/`sessions` 等可以接受，因为有一个**尚未落地**的独立网关接线任务负责写它们。

对本迁移的五张表，这个 gap **不可接受**：H3 任务本身的验收 gate 要求同一 wave 内有一个真实 worker，在 `role_private_worker` 今天的真实授权下，对 `ops.email_delivery_events` 执行 INSERT、对 `ops.email_outbox.state` 执行 UPDATE（§74.6 任务简报："provider 失败转 FAILED+事件记录"，由本 wave 的真实 DB 集成测试直接验证，不是留给未来）。`ops.*` 域默认已经对 `role_gateway`/`role_private_worker`/`role_public_worker`/`role_retrieval_worker` 授予 SELECT+INSERT+UPDATE（§6.2.1），复用已装好的 0011 `ALTER DEFAULT PRIVILEGES` 机制，零新增 GRANT 语句——即让这个子系统今天就能跑通，而不是等一个未来的 grants 迁移落地之后。

**已知局限 / 升级信号**：这五张表与 §82 canonical 清单存在登记偏差（3 张表 schema 归属不同，2 张表清单里根本没有）。升级信号：canonical-name 校验（mechanism-registry 一类）如果未来扩展到 DB 层做逐表核对，这里会打红——那时的正确动作是回填 §82 清单，而不是搬表。本 ADR 是那次回填之前的显式登记，避免"红了才第一次知道有偏差"。

Tenant 归属：五张表均不带 `tenant_id`（同 0034 已建立的先例：User 在本 schema 不是 tenant-scoped 实体，见 0003 对 `control.users` 的注释；tenant-scoped 的是 join 表 `control.memberships`）。`email_outbox` 直接指向 `control.users`，其余四张 Deliverability Plane 表是平台级运营/信誉数据（一次硬退信或一个发送域的 SPF/DKIM/DMARC 状态不是按租户变化的事实），与 `ops.consistency_reports`/`coord.locks` 已有先例一致。`xtask/src/rls_check.rs` 的 RLS 四项枚举只扫描带 `tenant_id` 的表（§48.2/§62），五张表均不满足，因此不适用 ENABLE/FORCE ROW LEVEL SECURITY。

## 决策 2：新增 provider secret domain — `SmtpConfig.username`/`password`

H3 引入 `SmtpConfig`（`crates/adapters/src/email/smtp.rs`）承载 SMTP 认证凭据。这是本仓库第一个 "email provider" 形状的 secret domain。落地约束：

- 配置来自调用方传入的结构体字段，本模块任何函数都不读 `std::env`（任务简报"配置来自参数不读 env"），实际的 host/port/凭据来源是部署层的既有 typed config 机制，`smtp.rs` 只负责消费。
- 凭据不落库、不落 `ops.*` 任何表——`ops.email_outbox.provider`/`ops.email_delivery_events.provider` 记录的是 adapter 身份字符串（如 `"smtp"`），不是凭据本身。
- 密钥仍然遵守 CLAUDE.md 硬边界"密钥走环境变量，禁止硬编码、禁止打日志"——具体读取点在部署层，不在本 ADR 范围内新开例外。

## 不选的方案（store as `rejected`）

- **把五张表拆进 `control.*` 并为其新增 MATRIX 写权限单元格**：技术上可行，但需要额外一次 grants 迁移，且会把"这批表能不能今天跑通"绑死在一个未落地的网关接线任务上，与本 wave 的验收 gate（真实 worker 今天必须能写）直接冲突。放弃。
- **为五张表新增 `tenant_id` 并接入 RLS**：数据本质是平台级运营信号，不是按租户变化的事实（见上），加 `tenant_id` 是引入一个从不被读的列，纯粹的推测性设计。放弃（YAGNI）。
