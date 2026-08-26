-- §7.2 / §7.0 T4.1: tenant-level switch for external (Managed Dense Embedding / Rerank)
-- retrieval egress. §7.2: "只有 TenantDataPolicy.private_retrieval_external_allowed = true 且
-- EgressPolicy 授权时才允许发送"; §7.0's frozen判定线: "未经 EgressPermit 且未记账的出境 =
-- 违规" — this table is the tenant-facing knob `domain::egress::authorize`'s caller consults
-- before minting an `EgressPermit` for a `RETRIEVAL_EMBEDDING`/`RETRIEVAL_RERANK` purpose
-- (§83.4 registry). One row per tenant; a tenant with no row defaults to disallowed (safer
-- default — a missing policy row must never silently read as "external egress permitted").
--
-- Column name is verbatim §7.2's own field name (`private_retrieval_external_allowed`), not a
-- renamed/abbreviated one, per repo CLAUDE.md 判据正文唯一真源 discipline (traceable by name,
-- not by a second paraphrase).
--
-- §6.2.1 domain-default GRANTs apply automatically via 0011's `ALTER DEFAULT PRIVILEGES` (this
-- table is not named in the §6.2.2 explicit-GRANT matrix, same precedent as 0036's three
-- notification tables) — no explicit GRANT statement belongs in this file.

CREATE TABLE control.egress_policies (
  tenant_id                            uuid        PRIMARY KEY
                                                     REFERENCES control.tenants(tenant_id),
  private_retrieval_external_allowed   boolean     NOT NULL DEFAULT false,
  updated_at                           timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE control.egress_policies IS
  '§7.2: per-tenant switch for external Managed Dense Embedding/Rerank egress. Default false '
  '(missing/unset reads as disallowed, never as permitted) — §7.0 判定线 "未经 EgressPermit '
  '且未记账的出境 = 违规" requires the safer default on the row that gates it.';

-- §62 tenant RLS, NULLIF form (0031 hardening note applies to every table created after it).
ALTER TABLE control.egress_policies ENABLE ROW LEVEL SECURITY;
ALTER TABLE control.egress_policies FORCE ROW LEVEL SECURITY;
CREATE POLICY egress_policies_tenant_isolation ON control.egress_policies
USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

-- §6.2.1 ownership: role_migration_owner owns every table in every schema.
ALTER TABLE control.egress_policies OWNER TO role_migration_owner;
