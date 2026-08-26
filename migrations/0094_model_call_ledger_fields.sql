-- §19.1 ModelCallLedger — Phase 7 delivery (T7.4). 0008 created `ops.model_call_ledger` as a
-- skeleton (model_call_id/tenant_id/provider/called_at); 0080/0081 added `purpose` with a
-- CHECK closed to the single literal `'query_rewrite'` G20-2/G80-39's e2e half needed, and
-- both migrations' own comments flagged the full §19.1 field list + full purpose enum as
-- "a Phase 7 delivery that widens this same CHECK" — this migration is that delivery. Cannot
-- edit 0008/0080/0081 (rule ③, already applied) — pure ADD COLUMN + one ALTER on the existing
-- CHECK, same EXPAND-only shape 0061 used to widen `reasoning_domain_grants.purposes`.
--
-- Two-phase reserve()->finalize() ledger, same shape as `ops.data_disclosures`
-- (migrations/0047_data_disclosure_ledger.sql, §7.4): `reserve()` inserts identity columns +
-- an `estimated_cost` computed from a pre-call usage estimate, `status='RESERVED'`; the token/
-- latency/`actual_cost`/`error_class`/`provider_request_id` columns start NULL and are filled
-- exactly once by `finalize()`, which also flips `status` to 'SUCCEEDED'/'FAILED'. This is
-- deliberate, not an oversight: "every external call produces exactly one row, from before the
-- call to after" is the same structural guarantee §7.4's doc explains for disclosures — a
-- caller that skips reserve() for "just this one call" produces no ledger row at all, which
-- `ops.model_call_ledger_guard_mutation` below cannot detect after the fact (nothing to guard
-- if there is no row); the fault-injection coverage for that gap lives in
-- crates/adapters/tests/model_call_ledger.rs, not in this migration.
--
-- `request_id` is the caller's idempotency key (§7757/§11684's "request_id" convention,
-- distinct from `model_call_id` — that PK identifies the *row*, `request_id` identifies the
-- *logical call* a retrying caller can safely repeat). `DEFAULT uuidv7()` so the pre-existing
-- `crates/adapters/tests/retrieve_no_hidden_generative_recall.rs` fault-injection INSERT
-- (`INSERT INTO ops.model_call_ledger (tenant_id, provider, purpose) VALUES (...)`, no
-- `request_id` column named) keeps compiling against this migration unchanged — that test is
-- not this task's file to touch (hard rule ①), and it predates `request_id` entirely.
ALTER TABLE ops.model_call_ledger
  ADD COLUMN request_id          uuid    NOT NULL DEFAULT uuidv7(),
  ADD COLUMN workspace_id        uuid    REFERENCES control.workspaces(workspace_id),
  ADD COLUMN model                text,
  ADD COLUMN model_revision       text,
  ADD COLUMN input_tokens         bigint  CHECK (input_tokens IS NULL OR input_tokens >= 0),
  ADD COLUMN billable_tokens      bigint  CHECK (billable_tokens IS NULL OR billable_tokens >= 0),
  ADD COLUMN candidate_count      integer CHECK (candidate_count IS NULL OR candidate_count >= 0),
  ADD COLUMN candidate_tokens     bigint  CHECK (candidate_tokens IS NULL OR candidate_tokens >= 0),
  ADD COLUMN cache_hit            boolean,
  ADD COLUMN latency_ms           integer CHECK (latency_ms IS NULL OR latency_ms >= 0),
  -- §19/§78.1: cost is never a Rust literal — these columns hold what
  -- `humaux_retrieval_provider::cost::compute_cost` returned for a given usage snapshot +
  -- `control.provider_pricing_versions` row (§19 "费用计算：ModelCallLedger + PricingVersion"),
  -- never a value the ledger itself derives. `double precision`, not `numeric`: spec's own
  -- words for this table ("无需第一版实现精确财务会计") — ponytail: exact-decimal invoicing
  -- (rust_decimal + a `numeric` column) is the upgrade path if real billing statements ever
  -- read this column directly instead of a downstream rollup.
  ADD COLUMN estimated_cost       double precision CHECK (estimated_cost IS NULL OR estimated_cost >= 0),
  ADD COLUMN actual_cost          double precision CHECK (actual_cost IS NULL OR actual_cost >= 0),
  ADD COLUMN status               text    NOT NULL DEFAULT 'RESERVED'
                                     CHECK (status IN ('RESERVED', 'SUCCEEDED', 'FAILED')),
  ADD COLUMN error_class          text,
  ADD COLUMN provider_request_id  text;

-- §19.1 field list is otherwise unordered prose — `request_id` is scoped unique per tenant
-- (not globally) matching every other tenant-scoped uniqueness constraint in this schema
-- (e.g. `ops.jobs`'s `(tenant_id, idempotency_key)`, 0008), and is what `reserve()`'s
-- `ON CONFLICT (tenant_id, request_id) DO NOTHING` idempotent-retry path targets
-- (`crates/adapters/src/model_call_ledger.rs`).
ALTER TABLE ops.model_call_ledger
  ADD CONSTRAINT model_call_ledger_request_id_unique UNIQUE (tenant_id, request_id);

CREATE INDEX idx_model_call_ledger_tenant ON ops.model_call_ledger (tenant_id);

-- 0081 closed `purpose` to the one literal G20-2/G80-39's e2e half needed
-- (`'query_rewrite'`); this is that migration's own documented widening point. Cannot ALTER a
-- CHECK in place — DROP + re-ADD, same as 0061's `reasoning_domain_grants` widening. Closed to
-- what T7.4's Retrieval Provider Plane actually calls the model for today (§19: "职责只针对
-- 外部 managed neural retrieval: dense embedding, rerank") plus the pre-existing
-- `'query_rewrite'` literal — not the far larger general-purpose-LLM-gateway purpose set
-- `domain::egress::OutboundPurpose` enumerates (§19 also: "不扩展成通用聊天 LLM Gateway"),
-- and not yet `'user_reasoning'` (§11's separate egress path) since no writer in this
-- codebase produces that value on this table yet — widen again, the same way, once one does.
ALTER TABLE ops.model_call_ledger
  DROP CONSTRAINT model_call_ledger_purpose_known;
ALTER TABLE ops.model_call_ledger
  ADD CONSTRAINT model_call_ledger_purpose_known
    CHECK (purpose IS NULL OR purpose = ANY (ARRAY['query_rewrite', 'embedding', 'rerank']::text[]));

COMMENT ON COLUMN ops.model_call_ledger.purpose IS
  '§19.1 ModelCallLedger field table. Closed to the three values this codebase actually '
  'produces today (query_rewrite / embedding / rerank) — widen the CHECK again, the way this '
  'migration widened 0081''s single-value set, the next time a real writer needs a new one.';
COMMENT ON COLUMN ops.model_call_ledger.request_id IS
  'Caller-supplied idempotency key for the logical call (distinct from model_call_id, the '
  'row''s own identity) — a retry that reuses the same request_id hits '
  'model_call_ledger_request_id_unique instead of producing a second reservation.';
COMMENT ON COLUMN ops.model_call_ledger.estimated_cost IS
  '§19 Pricing Registry: computed by humaux_retrieval_provider::cost::compute_cost from a '
  'pre-call usage estimate + the control.provider_pricing_versions row covering called_at '
  '(humaux_retrieval_provider::pricing::resolve) — never a literal.';
COMMENT ON COLUMN ops.model_call_ledger.actual_cost IS
  '§19 Pricing Registry: same computation as estimated_cost, fed the provider''s actually '
  'reported usage at finalize() time. §19 "历史调用永远按当时 pricing snapshot 归因": both '
  'costs are resolved against the pricing row covering called_at, not "whatever price is '
  'current when finalize() runs" — a later control.provider_pricing_versions row never '
  'changes what an already-finalized row reads back as.';

-- §62/§48.2 RLS four-item: correction after checking the live DB (`\d ops.model_call_ledger`)
-- rather than trusting a literal-string grep of the migration files — `ops.model_call_ledger`
-- already carries `tenant_id` since 0008, which ran *before* 0012's RLS migration. 0012's own
-- policy-creation step is a dynamic `information_schema.columns` sweep over every tenant_id
-- table (`migrations/0012_rls.sql`'s `DO $$ ... FOR rec IN SELECT ... WHERE column_name =
-- 'tenant_id' ...`), not a per-table literal list — so it already caught this table on its
-- first run (`ENABLE`/`FORCE ROW LEVEL SECURITY` + a policy literally named
-- `model_call_ledger_tenant_isolation`, confirmed live: identical NULLIF-form USING/WITH CHECK
-- to the template below). A `grep model_call_ledger migrations/0012_rls.sql` finds nothing
-- because the table name never appears as a string in that file — the absence of a name match
-- is not the same as the absence of coverage. No RLS statements needed here; re-issuing
-- `ENABLE`/`FORCE`/`CREATE POLICY` against an already-covered table is what produced this
-- migration's original "policy ... already exists" failure during development.
--
-- Not one of §6.2.2's named tables ⇒ §6.2.1 domain default already covers the two writers
-- that need it (`role_gateway`/`role_private_worker`/`role_public_worker`/
-- `role_retrieval_worker` all get `ops.* = R + W`) — no new GRANT statement (same reasoning
-- 0083/0084 documented for `ops.selection_snapshots`/`ops.selection_snapshot_items`).
-- Ownership: already `role_migration_owner` since 0008 (0011's retroactive sweep), unchanged
-- by ADD COLUMN/ENABLE ROW LEVEL SECURITY.

-- Append-only + one-shot finalize, same guard shape as
-- `ops.data_disclosures_guard_mutation` (0047): DELETE always rejected; identity columns
-- (everything reserve() sets) immutable for the row's whole life; once `status` has left
-- 'RESERVED' (finalize() already ran), no further UPDATE of any kind is accepted — including a
-- second finalize() attempting to overwrite actual_cost with a value computed against a since-
-- updated PricingVersion, which is exactly the invariant this task's core acceptance test
-- (crates/adapters/tests/model_call_ledger.rs) exercises.
CREATE FUNCTION ops.model_call_ledger_guard_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'ops.model_call_ledger is append-only (§19.1) — DELETE not permitted'
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

  IF OLD.status <> 'RESERVED' AND NEW.status IS DISTINCT FROM OLD.status THEN
    RAISE EXCEPTION 'ops.model_call_ledger already finalized — status cannot change again (§19.1)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF OLD.status <> 'RESERVED' AND (
       OLD.input_tokens        IS DISTINCT FROM NEW.input_tokens
    OR OLD.billable_tokens     IS DISTINCT FROM NEW.billable_tokens
    OR OLD.candidate_count     IS DISTINCT FROM NEW.candidate_count
    OR OLD.candidate_tokens    IS DISTINCT FROM NEW.candidate_tokens
    OR OLD.cache_hit           IS DISTINCT FROM NEW.cache_hit
    OR OLD.latency_ms          IS DISTINCT FROM NEW.latency_ms
    OR OLD.actual_cost         IS DISTINCT FROM NEW.actual_cost
    OR OLD.error_class         IS DISTINCT FROM NEW.error_class
    OR OLD.provider_request_id IS DISTINCT FROM NEW.provider_request_id
  ) THEN
    RAISE EXCEPTION 'ops.model_call_ledger already finalized — outcome columns cannot change again (§19.1)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;

  RETURN NEW;
END;
$$;

ALTER FUNCTION ops.model_call_ledger_guard_mutation() OWNER TO role_migration_owner;

CREATE TRIGGER model_call_ledger_guard_mutation
BEFORE UPDATE OR DELETE ON ops.model_call_ledger
FOR EACH ROW EXECUTE FUNCTION ops.model_call_ledger_guard_mutation();
