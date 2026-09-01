-- §12.1/§12.5 + §6.2.2 修复：`role_public_worker` 走 0104 的 contribution guard 触发器时，
-- 合法的 USER_CONTRIBUTION 准入被 `permission denied for table contribution_releases`（42501）
-- 拒死。
--
-- 病因（0104 实测）：`public.guard_contribution_source()` / `guard_contribution_provenance()`
-- 是 SECURITY INVOKER，它们调用的 `staging.assert_active_contribution_release(uuid)` 也是
-- INVOKER，最终以**写入者身份**执行 `SELECT ... FROM staging.contribution_releases`。0104 已
-- 把该函数的 EXECUTE 授予 role_public_worker，却没有——**也不允许有**——这张表的 SELECT。
--
-- 修法必须遵守 §6.2.2 的 Phase 9 anonymous boundary（spec 逐字）：
--   「`role_public_worker` 对 `ops.outbox`、`ops.jobs` 与 `staging.contribution_releases`
--     均为 `—`；其 dispatch、projection 与 protected release checks 只经 owner 的
--     narrow SECURITY DEFINER functions」
-- 因此本迁移新增一个**窄** SECURITY DEFINER 函数：owner 身份查表，只返回准入校验所需的
-- 4 个 rights 字段（publisher / source_license / rights_basis / redistribution_policy——
-- 它们本来就要逐字写进公共可见的 `public.sources`），不返回整行。
--
-- 关键：DEFINER 绕过 RLS，所以租户隔离**必须由函数体自己显式重建**，不能再指望
-- `contribution_releases_tenant_isolation` 策略自动裁剪可见行。函数因此显式比对
-- `release_row.tenant_id` 与会话的 `humaux.tenant_id`，无上下文（NULL）即视为不可见。
--
-- 被否决的两个方案（都实测过，记录在此避免重犯）：
--   (A) `GRANT SELECT ON staging.contribution_releases TO role_public_worker`
--       —— 违反 §6.2.2 anonymous boundary（该单元格必须是 `—`）。实测被 `xtask rls-check`
--       的 §6.2.2 矩阵一致性断言拒绝："table-level expected {}, actual {SELECT}"。
--   (B) 直接把现有 `assert_active_contribution_release` 改 DEFINER 而不加显式租户校验
--       —— DEFINER 绕过 RLS 后，「没有租户上下文的 public worker 不得准入」这条性质当场
--       失效，实测把安全测试 public_worker_without_tenant_context_cannot_admit_user_source
--       由绿打红。提权必须同时接管被提权绕过的那道检查。

CREATE FUNCTION staging.assert_active_release_rights(
  release_id uuid,
  OUT publisher text,
  OUT source_license text,
  OUT rights_basis text,
  OUT redistribution_policy text)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
  release_row staging.contribution_releases;
  session_tenant uuid;
BEGIN
  -- 与 assert_active_contribution_release 同款：先取锁再在锁后快照读，避免与并发 revoke 竞态。
  PERFORM staging.lock_contribution_release(release_id, false);
  SELECT r.* INTO release_row FROM staging.contribution_releases r
    WHERE r.contribution_release_id = release_id;
  -- DEFINER 身份不受 RLS 裁剪，租户隔离在这里显式重建（见文件头）。
  session_tenant := NULLIF(current_setting('humaux.tenant_id', true), '')::uuid;
  IF NOT FOUND OR session_tenant IS NULL OR release_row.tenant_id <> session_tenant THEN
    RAISE EXCEPTION 'release is not visible in job tenant context' USING ERRCODE = '42501';
  END IF;
  IF release_row.state <> 'ACTIVE'
      OR release_row.privacy_scan_outcome <> 'PASSED'
      OR release_row.secret_scan_outcome <> 'PASSED'
      OR btrim(release_row.rights_basis) = '' THEN
    RAISE EXCEPTION 'release is not eligible for public admission' USING ERRCODE = '23514';
  END IF;
  publisher := release_row.publisher;
  source_license := release_row.source_license;
  rights_basis := release_row.rights_basis;
  redistribution_policy := release_row.redistribution_policy;
END;
$$;

ALTER FUNCTION staging.assert_active_release_rights(uuid) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION staging.assert_active_release_rights(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION staging.assert_active_release_rights(uuid)
  TO role_private_worker, role_public_worker;

-- 两个 public.* 触发器改用窄函数。触发器本体保持 SECURITY INVOKER：只有「查 staging」这一
-- 步提权，不可变性比对与 rights 一致性仍在调用者身份下运行。
CREATE OR REPLACE FUNCTION public.guard_contribution_source()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path = pg_catalog AS $$
DECLARE
  release_rights record;
BEGIN
  IF TG_OP = 'UPDATE' THEN
    IF ROW(NEW.source_id, NEW.source_type, NEW.contribution_release_id, NEW.content_hash,
        NEW.source_url, NEW.publisher, NEW.source_license, NEW.rights_basis,
        NEW.redistribution_policy, NEW.created_at)
      IS DISTINCT FROM
       ROW(OLD.source_id, OLD.source_type, OLD.contribution_release_id, OLD.content_hash,
        OLD.source_url, OLD.publisher, OLD.source_license, OLD.rights_basis,
        OLD.redistribution_policy, OLD.created_at) THEN
      RAISE EXCEPTION 'public source identity and rights are immutable' USING ERRCODE = '23514';
    END IF;
  ELSIF NEW.source_type = 'USER_CONTRIBUTION' THEN
    release_rights := staging.assert_active_release_rights(NEW.contribution_release_id);
    IF ROW(NEW.publisher, NEW.source_license, NEW.rights_basis, NEW.redistribution_policy)
      IS DISTINCT FROM ROW(release_rights.publisher, release_rights.source_license,
        release_rights.rights_basis, release_rights.redistribution_policy) THEN
      RAISE EXCEPTION 'public source rights must match release snapshot' USING ERRCODE = '23514';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.guard_contribution_provenance()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path = pg_catalog AS $$
DECLARE
  release_id uuid;
BEGIN
  SELECT s.contribution_release_id INTO release_id FROM public.sources s
    WHERE s.source_id = NEW.source_id;
  IF release_id IS NOT NULL THEN
    -- 只要校验（release 仍 ACTIVE、扫描通过、租户可见），返回值不使用。
    PERFORM staging.assert_active_release_rights(release_id);
  END IF;
  RETURN NEW;
END;
$$;
