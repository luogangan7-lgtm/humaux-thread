-- §75.2 / §19.1 append-only：给两个账本补上 TRUNCATE 守卫。
--
-- 病因与 0047→0057 逐字同型，只是发生在另外两张表上：`FOR EACH ROW` 触发器**永远不会被
-- TRUNCATE 触发**，所以「BEFORE UPDATE OR DELETE FOR EACH ROW」这种写法看起来把
-- append-only 钉死了，实际留着一条整表清空的路。
--
-- 实测（以表 owner `role_migration_owner` 身份，事务内回滚）：
--   TRUNCATE control.credit_ledger CASCADE   -> 成功，剩 0 行
--   TRUNCATE ops.model_call_ledger CASCADE   -> 成功，剩 0 行
--   TRUNCATE ops.data_disclosures CASCADE    -> 被拒（0057 已补）
--
-- 为什么 owner 身份是相关威胁面而不是"反正没人会那么干"：0032 自己的注释写着这道防线的
-- 存在理由就是「append-only 必须在 migration-owner 认证的连接下也成立，不只在运行时角色的
-- grant 之下」——runtime 角色本来就只有 SELECT。防线的自述目标与它的实际覆盖面对不上。
--
-- 顺带说明为什么此前没被发现：这两张表的 append-only 守卫**零测试覆盖**（全仓 tests/ 里
-- 一次都没出现过 credit_ledger）。一个从写下起没被执行过的守卫，按 §80.1 的准入条件不算数。
-- 本迁移配套的 DB 集成测试同批交付。

-- ---------------------------------------------------------------------------
-- control.credit_ledger (§75.2)
-- ---------------------------------------------------------------------------
-- 它的守卫函数是无条件 RAISE、不访问 NEW/OLD，因此可以直接挂到 STATEMENT 级
-- TRUNCATE 上，函数本身一字不用改（消息里的 % 会插值成 'TRUNCATE'）。
CREATE TRIGGER credit_ledger_reject_truncate
BEFORE TRUNCATE ON control.credit_ledger
FOR EACH STATEMENT EXECUTE FUNCTION control.credit_ledger_reject_mutation();

-- ---------------------------------------------------------------------------
-- ops.model_call_ledger (§19.1)
-- ---------------------------------------------------------------------------
-- 这个守卫在 DELETE 分支之后会比较 OLD/NEW 的身份列，而 TRUNCATE 下两者都是 NULL。
-- 所以先把 TG_OP 判定扩到 TRUNCATE（照 0057 对 data_disclosures 的同款改法），
-- 让它在触到 OLD/NEW 之前就抛出——否则挂上去只会得到一个语义不明的 NULL 记录错误。
CREATE OR REPLACE FUNCTION ops.model_call_ledger_guard_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
    RAISE EXCEPTION 'ops.model_call_ledger is append-only (§19.1) — % not permitted', TG_OP
      USING ERRCODE = 'insufficient_privilege';
  END IF;

  IF OLD.request_id      IS DISTINCT FROM NEW.request_id
     OR OLD.tenant_id     IS DISTINCT FROM NEW.tenant_id
     OR OLD.workspace_id  IS DISTINCT FROM NEW.workspace_id
     OR OLD.purpose       IS DISTINCT FROM NEW.purpose
     OR OLD.provider      IS DISTINCT FROM NEW.provider
     OR OLD.model         IS DISTINCT FROM NEW.model
     OR OLD.model_revision IS DISTINCT FROM NEW.model_revision
     OR OLD.called_at     IS DISTINCT FROM NEW.called_at
     OR OLD.estimated_cost IS DISTINCT FROM NEW.estimated_cost
  THEN
    RAISE EXCEPTION 'ops.model_call_ledger identity/reservation columns are immutable after INSERT (§19.1)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER model_call_ledger_reject_truncate
BEFORE TRUNCATE ON ops.model_call_ledger
FOR EACH STATEMENT EXECUTE FUNCTION ops.model_call_ledger_guard_mutation();
