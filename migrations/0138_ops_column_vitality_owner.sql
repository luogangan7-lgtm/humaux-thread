-- §6.2 single-owner topology repair. `ops.column_vitality` is created by 0030, which runs
-- AFTER 0011's blanket `ALTER TABLE ... OWNER TO role_migration_owner` loop — that loop only
-- covers tables existing at 0011's execution time, so this table kept the migration runner's
-- own role (`postgres`) as owner. Every other table reaches role_migration_owner either
-- through 0011's loop (pre-0011 tables) or an explicit ALTER in its own migration.
--
-- Why a new migration instead of editing 0030: §46 freezes an applied migration's bytes
-- (`ops.schema_migrations.checksum` drift detection fires otherwise). The fix lands forward.
ALTER TABLE ops.column_vitality OWNER TO role_migration_owner;
