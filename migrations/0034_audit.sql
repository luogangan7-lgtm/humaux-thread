-- §77 审计体系: control.audit_events (Operational Audit, Security Audit Events 最小字段集
-- 补全) + ops.audit_batches (Immutable Audit Sink 的 hash-chained batch 元数据)。
--
-- control.audit_events 已在 0003 建表（P1 只落 5 列：audit_event_id/tenant_id/
-- actor_user_id/action/occurred_at，够 P1 自身 FK/GRANT 矩阵占位用）；0003 是已应用迁移，
-- checksum 闸不许改，本迁移只 ADD COLUMN 补齐 §77 "AuditEvent 最少" 字段列表剩余项，
-- RLS（ENABLE+FORCE+NULLIF policy）已由 0012 的 information_schema.columns 扫描 +
-- 0031 的 NULLIF 硬化在 audit_events 建表时自动覆盖（该表当时已有 tenant_id 列），本迁移
-- 不重复声明。GRANT 同理：0011 的 `ALTER DEFAULT PRIVILEGES FOR ROLE <runner> IN SCHEMA
-- control` 已在 0003 建表时按 §6.2.1 域默认自动套用（control.* 全 runtime role 只有 R，
-- 无一个角色有 INSERT —— 与 control.quota_windows 同型的显式空缺：本表未被 §6.2.2 逐条
-- 点名，谁来写 audit_event 行本文档未冻结，留给日后接线的任务，不在本迁移里补 GRANT）。
--
-- ops.audit_batches 全新建表，同样不在 §6.2.2 矩阵内 -> §6.2.1 ops.* 域默认经 0011 的
-- ALTER DEFAULT PRIVILEGES 在本迁移 CREATE TABLE 时自动套用，本文件不重复写 GRANT。
-- 不带 tenant_id：一个 batch 覆盖的是跨租户的整条 Operational Audit 序列窗口
-- （seq_start..seq_end），batch 本身不属于任何单一租户，因此没有、也不应该有 RLS 策略
-- （§62 RLS 只覆盖 tenant_id 列存在的表；这张表故意不落那一列）。

ALTER TABLE control.audit_events
  ADD COLUMN actor_type        text,
  ADD COLUMN actor_id          text,
  ADD COLUMN resource_type     text,
  ADD COLUMN resource_id       text,
  ADD COLUMN result            text,
  ADD COLUMN request_id        text,
  ADD COLUMN trace_id          text,
  ADD COLUMN client_ip         inet,
  ADD COLUMN user_agent_hash   text,
  ADD COLUMN risk_tags         text[] NOT NULL DEFAULT '{}',
  ADD COLUMN before_fingerprint text,
  ADD COLUMN after_fingerprint  text,
  -- allowlist 本身是 Rust 类型层的事（`humaux_domain::audit::AuditMetadata::insert`
  -- 拒绝含 password/token/key/secret/otp/byok/pkce 字样的键，§77 "metadata (allowlisted)"
  -- + "禁止写" 列表）；这一列只负责落盘，不重复用 DB CHECK 再判一遍键名
  -- (ponytail: 若日后需要防"绕过 Rust 层直接裸 SQL 写入"，在此加 CHECK 用
  -- jsonb_object_keys 扫键名，本迁移暂不加)。
  ADD COLUMN metadata          jsonb NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN control.audit_events.actor_id IS
  '§77 AuditEvent.actor_id — 通用主体标识（配合 actor_type 判读；不是 actor_user_id 的替代，后者仍是 control.users 场景下的强类型 FK）。';
COMMENT ON COLUMN control.audit_events.metadata IS
  '§77 "metadata (allowlisted)" — 写入前必须先通过 humaux_domain::audit::AuditMetadata::insert 的键名 allowlist 检查。';

-- §77 高风险动作/MCP_* 事件的常见查询路径：按租户+时间倒序翻页、按 action 过滤、按
-- request_id 做请求级溯源。
CREATE INDEX audit_events_tenant_occurred_at_idx
  ON control.audit_events (tenant_id, occurred_at DESC);
CREATE INDEX audit_events_action_idx
  ON control.audit_events (action);
CREATE INDEX audit_events_request_id_idx
  ON control.audit_events (request_id)
  WHERE request_id IS NOT NULL;

-- §77 Audit Immutability: "普通 Audit DB 仍可被高权限管理员修改" 是两层设计的前提
-- （因此才需要 Immutable Audit Sink 做 tamper-evidence），但 Operational Audit 自身仍应
-- 是 "PG append-only application contract" —— 同 0032 control.credit_ledger_reject_
-- mutation 的形状：BEFORE UPDATE OR DELETE 无条件拒绝，不分角色（包括 owner 自己的连接；
-- owner 仍可用 ALTER TABLE DISABLE TRIGGER 绕过，这正是 Immutable Sink 存在的理由，不是
-- 本触发器要堵的口子）。runtime role 域默认本来就没有 UPDATE/DELETE（见上），这道触发器
-- 是第二层防线，不是唯一防线。
CREATE FUNCTION control.audit_events_reject_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'control.audit_events is append-only (§77 Audit Immutability) — % not permitted', TG_OP
    USING ERRCODE = 'insufficient_privilege';
END;
$$;

CREATE TRIGGER audit_events_reject_mutation
BEFORE UPDATE OR DELETE ON control.audit_events
FOR EACH ROW EXECUTE FUNCTION control.audit_events_reject_mutation();

ALTER FUNCTION control.audit_events_reject_mutation() OWNER TO role_migration_owner;

-- §77 "Audit Batch" 字段列表逐字: seq_start / seq_end / previous_batch_hash / payload_hash
-- / exported_object / created_at。hash-chain 的构建（上一批 hash 链入）与校验函数是纯逻辑，
-- 落在 `humaux_domain::audit`（batch_hash / verify_chain，无 IO，符合 §3/§78.3 Domain 依赖
-- 边界）；这张表只负责持久化每个 batch 的元数据行，真正把 payload 字节写到 WORM/immutable
-- object storage 的 `ObjectStore` 端口占位在 `adapters::audit_sink`（Phase 13 接真实
-- S3/WORM 实现）。
CREATE TABLE ops.audit_batches (
  audit_batch_id       uuid        PRIMARY KEY DEFAULT uuidv7(),
  seq_start            bigint      NOT NULL,
  seq_end              bigint      NOT NULL,
  previous_batch_hash  bytea       NOT NULL,
  payload_hash         bytea       NOT NULL,
  exported_object      text        NOT NULL,
  created_at           timestamptz NOT NULL DEFAULT now(),
  CHECK (seq_end >= seq_start),
  UNIQUE (seq_start)
);

COMMENT ON TABLE ops.audit_batches IS
  '§77 Audit Batch: control.audit_events 导出到 Immutable Audit Sink 的 hash-chained 批次元数据，不带 tenant_id（一批覆盖跨租户的 seq 区间，不属于单一租户）。';

CREATE INDEX audit_batches_created_at_idx ON ops.audit_batches (created_at DESC);

ALTER TABLE ops.audit_batches OWNER TO role_migration_owner;

-- 与 control.audit_events 同型：这也是"不再可变"的账本行，同一套无条件拒绝形状。
CREATE FUNCTION ops.audit_batches_reject_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'ops.audit_batches is append-only (§77 Audit Batch) — % not permitted', TG_OP
    USING ERRCODE = 'insufficient_privilege';
END;
$$;

CREATE TRIGGER audit_batches_reject_mutation
BEFORE UPDATE OR DELETE ON ops.audit_batches
FOR EACH ROW EXECUTE FUNCTION ops.audit_batches_reject_mutation();

ALTER FUNCTION ops.audit_batches_reject_mutation() OWNER TO role_migration_owner;
