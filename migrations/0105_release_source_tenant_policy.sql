-- §12.1 / §48.2: 0104 introduced a constrained tenant_id on release sources. Use the
-- canonical direct tenant policy rather than 0103's former parent-only EXISTS shape.
-- The composite release FK still proves that this tenant is the parent's tenant.
ALTER POLICY contribution_release_sources_tenant ON staging.contribution_release_sources
  USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);
