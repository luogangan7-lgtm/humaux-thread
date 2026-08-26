-- §7.4 Disclosure Ledger (T4.2): `ops.data_disclosures` is the唯一权威出境账本
-- (append-only, every EgressPermit-backed call recorded, never sampled) plus its
-- normalized source relation `ops.data_disclosure_sources`.
--
-- Column definitions transcribed verbatim from §7.4's table (this is that table's ONLY
-- copy — §37's own text says its old nine-column disclosure list "作废", the real
-- definition lives here and §37's retention/deletion-propagation code reads these same
-- three columns: deletion_capability / deletion_requested_at / deletion_confirmed_at).
--
-- `processor_id` is `uuid` (not `text`): it mirrors `domain::egress::ProcessorId(Uuid)`
-- (T4.1, `crates/domain/src/egress.rs`) as landed by the sibling K1 task. No FK to
-- `control.processors` yet — the Processor Registry (§7 末, `control.processors` et al.)
-- is out of this migration's scope (not point-named by any T4.x task in this wave); add
-- the FK once that table exists. Same reasoning for `grant_id`: it is `EgressPermit`'s
-- own minted correlation id (`domain::egress::EgressPermit::grant_id`), not a row in a
-- separate persisted grants table — there isn't one, `EgressPermit` is intentionally
-- ephemeral (§7.3).
--
-- `purpose` CHECK is restricted to the three §83.4 `external-egress-registry` rows tagged
-- `private-data` (the only `OutboundPurpose` variants that carry an `EgressPermit` at all,
-- per T4.1's `PrivateDataPurpose`) — this ledger only ever records `EgressPermit`-backed
-- disclosures (§7.0 判定线), so no other purpose can ever produce a row here.
--
-- Append-only shape differs from `control.audit_events`/`ops.audit_batches` (0034): those
-- reject UPDATE unconditionally, but this ledger's whole point is a two-phase
-- reserve()->finalize() write (§7.4/T4.2) — the row must be updatable exactly once, to
-- attach the outcome, and its already-set identity columns must never be rewritten
-- afterward. `data_disclosures_guard_mutation` below enforces that precisely: DELETE is
-- always rejected; UPDATE is rejected if any pre-finalize identity column changes, or if
-- the row was already finalized and `finalized_at`/`outcome` would change again.
-- `deletion_capability`/`deletion_requested_at`/`deletion_confirmed_at` remain writable
-- after finalization — §37 deletion propagation appends to those independently of when
-- the disclosure itself was finalized.
CREATE TABLE ops.data_disclosures (
  disclosure_id           uuid        PRIMARY KEY DEFAULT uuidv7(),
  grant_id                uuid        NOT NULL,
  tenant_id               uuid        NOT NULL REFERENCES control.tenants(tenant_id),
  -- "谁的数据" narrower than tenant, same (scope_kind, scope_id) shape as
  -- projection.stream_checkpoints (0007) — both NULL (tenant-wide disclosure) or both set.
  scope_kind              text,
  scope_id                uuid,
  processor_id            uuid        NOT NULL,
  region                  text        NOT NULL,
  data_class              text        NOT NULL CHECK (data_class IN
                            ('PUBLIC','INTERNAL','PRIVATE','SENSITIVE','SECRET_MATERIAL')),
  purpose                 text        NOT NULL CHECK (purpose IN
                            ('USER_REASONING','RETRIEVAL_EMBEDDING','RETRIEVAL_RERANK')),
  payload_sha256          bytea       NOT NULL CHECK (octet_length(payload_sha256) = 32),
  payload_bytes           bigint      NOT NULL CHECK (payload_bytes >= 0),
  reserved_at             timestamptz NOT NULL DEFAULT now(),
  finalized_at            timestamptz,
  outcome                 text        CHECK (outcome IS NULL OR outcome IN ('SUCCESS','FAILED','DENIED')),
  deletion_capability     text        NOT NULL DEFAULT 'UNKNOWN' CHECK (deletion_capability IN
                            ('SUPPORTED','UNSUPPORTED','UNKNOWN')),
  deletion_requested_at   timestamptz,
  deletion_confirmed_at   timestamptz,
  CONSTRAINT data_disclosures_scope_pair CHECK (
    (scope_kind IS NULL) = (scope_id IS NULL)
  ),
  -- reserve() inserts with both NULL; finalize() sets both together (§7.4 "预留->完成").
  CONSTRAINT data_disclosures_finalized_outcome_pair CHECK (
    (finalized_at IS NULL) = (outcome IS NULL)
  ),
  CONSTRAINT data_disclosures_deletion_order CHECK (
    deletion_confirmed_at IS NULL OR deletion_requested_at IS NOT NULL
  )
);

COMMENT ON TABLE ops.data_disclosures IS
  '§7.4: the唯一权威 outbound-disclosure ledger, append-only, one row per EgressPermit-backed call — no sampling. Column definitions here are the sole source; §37 references deletion_capability/deletion_requested_at/deletion_confirmed_at from this table, does not duplicate them.';
COMMENT ON COLUMN ops.data_disclosures.grant_id IS
  '§7.4: EgressPermit.grant_id — same id the permit itself carries (domain::egress::EgressPermit::grant_id), not a second identity.';
COMMENT ON COLUMN ops.data_disclosures.finalized_at IS
  '§53 INV-3: reserved_at set with finalized_at still NULL past 60s is an immediate fail — see ops.data_disclosures_open_reservations below for the observing query shape.';

CREATE INDEX idx_data_disclosures_tenant ON ops.data_disclosures (tenant_id);
-- §53 INV-3's own observation query shape: only unfinalized rows are ever scanned by it,
-- so a partial index keeps that scan cheap regardless of ledger size.
CREATE INDEX idx_data_disclosures_open_reservations
  ON ops.data_disclosures (reserved_at)
  WHERE finalized_at IS NULL;

ALTER TABLE ops.data_disclosures ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.data_disclosures FORCE ROW LEVEL SECURITY;

CREATE POLICY data_disclosures_tenant_isolation ON ops.data_disclosures
USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

ALTER TABLE ops.data_disclosures OWNER TO role_migration_owner;

CREATE FUNCTION ops.data_disclosures_guard_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'ops.data_disclosures is append-only (§7.4) — DELETE not permitted'
      USING ERRCODE = 'insufficient_privilege';
  END IF;

  -- Identity/reservation columns are fixed for the row's whole lifetime — only
  -- finalize()'s own columns and the §37 deletion-propagation columns may ever change.
  IF OLD.grant_id            IS DISTINCT FROM NEW.grant_id
     OR OLD.tenant_id        IS DISTINCT FROM NEW.tenant_id
     OR OLD.scope_kind       IS DISTINCT FROM NEW.scope_kind
     OR OLD.scope_id         IS DISTINCT FROM NEW.scope_id
     OR OLD.processor_id     IS DISTINCT FROM NEW.processor_id
     OR OLD.region           IS DISTINCT FROM NEW.region
     OR OLD.data_class       IS DISTINCT FROM NEW.data_class
     OR OLD.purpose          IS DISTINCT FROM NEW.purpose
     OR OLD.payload_sha256   IS DISTINCT FROM NEW.payload_sha256
     OR OLD.payload_bytes    IS DISTINCT FROM NEW.payload_bytes
     OR OLD.reserved_at      IS DISTINCT FROM NEW.reserved_at
  THEN
    RAISE EXCEPTION 'ops.data_disclosures identity/reservation columns are immutable after INSERT (§7.4)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;

  -- finalize() may run exactly once: reject a second attempt to change finalized_at/outcome
  -- once already set (no re-finalizing, no backdating).
  IF OLD.finalized_at IS NOT NULL
     AND (OLD.finalized_at IS DISTINCT FROM NEW.finalized_at
          OR OLD.outcome IS DISTINCT FROM NEW.outcome)
  THEN
    RAISE EXCEPTION 'ops.data_disclosures already finalized — finalized_at/outcome cannot change again (§7.4)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER data_disclosures_guard_mutation
BEFORE UPDATE OR DELETE ON ops.data_disclosures
FOR EACH ROW EXECUTE FUNCTION ops.data_disclosures_guard_mutation();

ALTER FUNCTION ops.data_disclosures_guard_mutation() OWNER TO role_migration_owner;

-- §7.4 "来源关系规范化" — deletion/revocation propagation only ever queries this table,
-- never a `uuid[]` column on data_disclosures itself.
--
-- **This table already exists** — `migrations/0008_ops_core.sql` created it as one of P1's
-- "§48 canonical tables... skeleton" placeholders, three columns
-- (`data_disclosure_source_id` PK / `tenant_id` / `created_at`), long before §7.4's real
-- shape was written. 0008 is an already-applied migration (checksum-locked); this migration
-- extends it the same way 0034 extended `control.audit_events` — `ALTER TABLE ADD COLUMN`,
-- not a second `CREATE TABLE`. The table has zero rows in every environment this migration
-- has been rehearsed against (P1's skeleton was never wired to a write path), so every new
-- column below can go straight to `NOT NULL` with no backfill step.
--
-- `tenant_id` (0008) is kept rather than dropped: §7.4's literal column list for this
-- relation doesn't name it, but 0008 already carries `ENABLE+FORCE ROW LEVEL SECURITY` plus
-- a `NULLIF` tenant policy on it (`data_disclosure_sources_tenant_isolation`) — removing the
-- column would mean tearing down that RLS guard on a table two schemas' worth of private
-- identifiers (`evidence_id`/`memory_id`) flow through, for zero benefit (a source row's
-- tenant is always redundant with `data_disclosures.tenant_id` via `disclosure_id` — this
-- is defense in depth, not a second source of truth). A future inserter (the seal_card/
-- seal_query wiring landing in Phase 5/6) sets it to the same tenant as the disclosure row
-- it's attached to.
ALTER TABLE ops.data_disclosure_sources
  ADD COLUMN disclosure_id uuid NOT NULL REFERENCES ops.data_disclosures(disclosure_id),
  ADD COLUMN source_kind   text NOT NULL CHECK (source_kind IN ('EVIDENCE','MEMORY','ROLLUP','PUBLIC_RELEASE')),
  ADD COLUMN evidence_id   uuid REFERENCES private.evidence_objects(evidence_id),
  ADD COLUMN memory_id     uuid REFERENCES private.memory_records(memory_id),
  ADD COLUMN rollup_id     uuid REFERENCES private.memory_rollups(rollup_id),
  ADD COLUMN release_id    uuid REFERENCES staging.contribution_releases(contribution_release_id),
  ADD COLUMN ordinal       int  NOT NULL,
  -- §7.4: "四个具体 ID 恰好一个非 NULL".
  ADD CONSTRAINT data_disclosure_sources_exactly_one_id CHECK (
    num_nonnulls(evidence_id, memory_id, rollup_id, release_id) = 1
  ),
  -- §7.4: "source_kind 与非 NULL 列必须一致".
  ADD CONSTRAINT data_disclosure_sources_kind_matches_id CHECK (
    (source_kind = 'EVIDENCE'        AND evidence_id IS NOT NULL)
    OR (source_kind = 'MEMORY'         AND memory_id   IS NOT NULL)
    OR (source_kind = 'ROLLUP'         AND rollup_id   IS NOT NULL)
    OR (source_kind = 'PUBLIC_RELEASE' AND release_id  IS NOT NULL)
  ),
  ADD CONSTRAINT data_disclosure_sources_disclosure_ordinal_unique UNIQUE (disclosure_id, ordinal);

COMMENT ON TABLE ops.data_disclosure_sources IS
  '§7.4: normalized "what a disclosure disclosed" relation — deletion/revocation propagation queries this table only, never a uuid[] column. Base table from 0008 (P1 skeleton); this migration (0047) adds the real §7.4 columns.';

CREATE INDEX idx_data_disclosure_sources_disclosure ON ops.data_disclosure_sources (disclosure_id);
CREATE INDEX idx_data_disclosure_sources_evidence ON ops.data_disclosure_sources (evidence_id) WHERE evidence_id IS NOT NULL;
CREATE INDEX idx_data_disclosure_sources_memory   ON ops.data_disclosure_sources (memory_id)   WHERE memory_id   IS NOT NULL;
CREATE INDEX idx_data_disclosure_sources_rollup    ON ops.data_disclosure_sources (rollup_id)   WHERE rollup_id   IS NOT NULL;
CREATE INDEX idx_data_disclosure_sources_release   ON ops.data_disclosure_sources (release_id)  WHERE release_id  IS NOT NULL;

-- Same append-only shape as control.audit_events/ops.audit_batches (0034): a source row
-- records what already happened and is never edited, only ever inserted. (0008 already set
-- `OWNER TO role_migration_owner` and the §6.2.1 ops domain-default GRANTs — unchanged here.)
CREATE FUNCTION ops.data_disclosure_sources_reject_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'ops.data_disclosure_sources is append-only (§7.4) — % not permitted', TG_OP
    USING ERRCODE = 'insufficient_privilege';
END;
$$;

CREATE TRIGGER data_disclosure_sources_reject_mutation
BEFORE UPDATE OR DELETE ON ops.data_disclosure_sources
FOR EACH ROW EXECUTE FUNCTION ops.data_disclosure_sources_reject_mutation();

ALTER FUNCTION ops.data_disclosure_sources_reject_mutation() OWNER TO role_migration_owner;
