-- §5.1: 7-schema physical partition. `ops` already exists (0001); `public` is PostgreSQL's
-- built-in schema reused as the 7th logical schema (§5.1: "不要把 public 继续建模成魔法
-- tenant" — it stays a plain schema like the other six, no special-cased tenant semantics).
CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS private;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS projection;
CREATE SCHEMA IF NOT EXISTS coord;
CREATE SCHEMA IF NOT EXISTS ops;

COMMENT ON SCHEMA control IS '§5.1 control.* — Identity/SaaS data model (§6).';
COMMENT ON SCHEMA private IS '§5.1 private.* — Evidence-first tenant private data (§8).';
COMMENT ON SCHEMA staging IS '§5.1 staging.* — Contribution release staging pipeline.';
COMMENT ON SCHEMA public IS '§5.1 public.* — Public Knowledge pool (§10); not a tenant schema.';
COMMENT ON SCHEMA projection IS '§5.1 projection.* — Retrieval projection + stream watermark (§15).';
COMMENT ON SCHEMA coord IS '§5.1 coord.* — Task/canvas coordination.';
COMMENT ON SCHEMA ops IS '§5.1 ops.* — Operational/maintenance (outbox, jobs, mechanism observations).';
