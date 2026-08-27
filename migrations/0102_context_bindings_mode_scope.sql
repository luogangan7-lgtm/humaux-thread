-- §25.4 Mandatory Context Lane：给 private.context_bindings 补 mode / scope / 撤销维度。
-- 0006 建的骨架只有 (context_binding_id, tenant_id, memory_id, created_at)——连「这条
-- binding 是 MANDATORY 还是 SUPPLEMENTAL」都表达不了，§25.4 的车道无从谈起。
--
-- 前提（已实测）：**本表当前零行，且全仓没有任何 INSERT 路径**
--   grep 'INSERT INTO private.context_bindings' crates/ xtask/ migrations/  -> 0
--   select count(*) from private.context_bindings                          -> 0
-- 这一条同时解掉「DEFAULT 该选哪个」的两难：没有存量行，任何默认值都不解除既有保护，
-- 于是选**对未来一条绕过代码层的裸 INSERT 最安全**的那个 —— SUPPLEMENTAL（无害档），
-- 而不是 MANDATORY。postcheck 断言 MANDATORY 行数为 0，把这个前提钉住。
--
-- 列名保留 context_binding_id，不按 §25.4 字段清单里的 binding_id 改名：那段是字段清单
-- 不是 DDL，rename 要动一张已冻结表的 PK 加两处已上线 SQL，零行为收益。spec 的用词保留
-- 在 Rust 类型名上。
--
-- **本迁移一条 DML 都没有**，这是刻意的：0012_rls.sql 的 blanket DO 给本表上了
-- ENABLE + FORCE RLS，FORCE 对表 owner 也生效；而 xtask/src/migrate.rs 直连 DSN、
-- 从不 SET humaux.tenant_id。任何回填语句会命中 0 行，随后的 SET NOT NULL 就会顶爆。
-- 因此新增约束一律 NOT VALID：只约束新行，不回验存量、不阻塞迁移。

ALTER TABLE private.context_bindings
  ADD COLUMN scope_kind text NOT NULL DEFAULT 'TENANT'
    CHECK (scope_kind IN ('TENANT','USER','WORKSPACE','REPOSITORY','TASK','RUN','AGENT')),
  ADD COLUMN scope_id   uuid,
  ADD COLUMN mode       text NOT NULL DEFAULT 'SUPPLEMENTAL'
    CHECK (mode IN ('MANDATORY','PINNED','SUPPLEMENTAL')),
  ADD COLUMN created_by uuid,
  ADD COLUMN revoked_at timestamptz;

-- memory_id 在 0006 里是可空的（骨架期）。§25.4 的 binding 必须指向一条 memory，
-- created_by 必须可追溯到创建者——但两者都只对新行成立，故 NOT VALID。
ALTER TABLE private.context_bindings
  ADD CONSTRAINT context_bindings_memory_id_present  CHECK (memory_id  IS NOT NULL) NOT VALID,
  ADD CONSTRAINT context_bindings_created_by_present CHECK (created_by IS NOT NULL) NOT VALID,
  ADD CONSTRAINT context_bindings_scope_id_present
    CHECK (scope_kind = 'TENANT' OR scope_id IS NOT NULL) NOT VALID;

-- 同一 (租户, scope, memory, mode) 只能有一条**未撤销**的 binding。
-- COALESCE(scope_id, tenant_id) 而不是裸 scope_id：TENANT 档的 scope_id 是 NULL，
-- 而 NULL 在唯一索引里彼此不相等，裸写会让同一租户同一 memory 能建无数条 TENANT binding。
CREATE UNIQUE INDEX ux_context_bindings_active
  ON private.context_bindings
     (tenant_id, scope_kind, COALESCE(scope_id, tenant_id), memory_id, mode)
  WHERE revoked_at IS NULL;

-- §25.4 步骤 2/5 的确定性取数路径：按 (租户, scope, mode) 取未撤销的 binding。
CREATE INDEX ix_context_bindings_active_scope
  ON private.context_bindings (tenant_id, scope_kind, COALESCE(scope_id, tenant_id), mode)
  WHERE revoked_at IS NULL;

COMMENT ON COLUMN private.context_bindings.mode IS
  '§25.4 MANDATORY | PINNED | SUPPLEMENTAL。前两者不参与 semantic 淘汰（步骤 2-5 不经 RRF/reranker）；SUPPLEMENTAL 是普通补充位。';
COMMENT ON COLUMN private.context_bindings.revoked_at IS
  '§25.4 撤销时间。非 NULL ⇒ 这条 binding 不再进入任何 lane，且 §11.8 的 auto-mutate 冻结随之解除。软删除而非物理删：binding 的历史是审计对象。';
