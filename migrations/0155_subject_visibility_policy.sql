-- §6.1.3 (Subject / Aboutness Axis — subject-scoped visibility) / §6.1.1 / §46 forward-fix /
-- ADR-0029 (Card 9).
--
-- Cards 7/8 (0153/0154) made a memory *about* subjects but nothing on the read side asks "may this
-- caller see every subject this memory is about?". This migration lands that guard as a RESTRICTIVE
-- policy on private.memory_records (research Q8 ruling: "subject policy AS RESTRICTIVE ANDed on
-- top"), so the row is readable only when the existing permissive owner/visibility policy passes
-- AND private.memory_subject_visibility_ok(...) passes. Nothing about 0012/0153's permissive policy
-- changes (its deparsed text, and therefore any pinned hash, is untouched — see the manifest).
--
-- Why a SECURITY DEFINER predicate and not an inline EXISTS: memory_subjects' own policy (0154)
-- contains `EXISTS (SELECT 1 FROM private.memory_records ...)`; an inline subquery from a
-- memory_records policy back into memory_subjects would re-enter memory_records' policies and
-- PostgreSQL raises "infinite recursion detected in policy". A SECURITY DEFINER function owned by
-- role_migration_owner breaks the cycle: inside it memory_subjects' EXISTS re-enters memory_records
-- as the owner, whose permissive arm (`current_user = 'role_migration_owner'`, §59.1 I4) is a plain
-- boolean and to whom the RESTRICTIVE policy below does not apply (it is not in the TO list).
-- STABLE, search_path pinned to pg_catalog (0137 shape), and it takes the row's own
-- (tenant_id, memory_id) — it can never be pointed at a different tenant's row than the policy is
-- evaluating.
--
-- Headless-role exemption (main-line amendment A1): the TO list names role_gateway and
-- role_maintenance only. role_retrieval_worker / role_consolidation_worker / role_private_worker
-- keep the 0140/0145/0147 arms untouched — the projection worker must index every row it is handed
-- (a subject-hidden row that FAILED would freeze the tenant under §15.7's watermark rule); actual
-- per-reader subject enforcement happens at Qdrant query time (`DenseQuery::with_subject_ids`)
-- and again at the PG hydrate gate (`final_memory_ids_about_in_txn`), both under the reader's own
-- role. Verbatim §62 tenant clause (A2) so the §48.2 four-item enumeration recognises this policy.
--
-- Subject visibility today = same tenant (the subject row is visible under private.subjects'
-- tenant policy, evaluated through private.visibility_allowed with class TENANT_SHARED). This is
-- the per-subject ACL hook: a later card that adds a subject-level visibility class changes the
-- three booleans passed to visibility_allowed inside this ONE function and nothing else.
--
-- ACL (0137/0147/0149 definer-function convention): EXECUTE is REVOKEd from PUBLIC and GRANTed
-- to exactly the two roles the policy below names. A definer predicate is a read of
-- private.memory_subjects with the owner's privileges; any role that merely has USAGE on schema
-- private (role_batch_issuer first of all) must not be able to call it and learn, one bit at a
-- time, whether a memory carries a subject link — the fact memory_subjects' own RLS withholds.
-- rls_check::check_subject_visibility_policy pins the ACL.
--
-- §46 forward-fix (EXPAND_CONTRACT, next free number after 0154). One grant (the new function's
-- own EXECUTE); one DML block (C., the projection-ticket backfill); no existing policy text
-- touched.

-- ============================================================================
-- A. The predicate. NOT EXISTS an attached subject the caller may not see. A memory with no
--    subject links is trivially visible (aboutness never *widens* a read, never *hides* an
--    unlinked row). LEFT JOIN, not INNER: a subject row the session cannot see under
--    private.subjects' RLS comes back NULL and counts as NOT visible (fail closed).
-- ============================================================================
CREATE FUNCTION private.memory_subject_visibility_ok(
  p_tenant_id uuid,
  p_memory_id uuid
) RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
  SELECT NOT EXISTS (
    SELECT 1
    FROM private.memory_subjects ms
    LEFT JOIN private.subjects s
      ON s.tenant_id = ms.tenant_id AND s.subject_id = ms.subject_id
    WHERE ms.tenant_id = p_tenant_id
      AND ms.memory_id = p_memory_id
      AND NOT coalesce(
        private.visibility_allowed(
          'TENANT_SHARED',
          s.tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid,
          false,
          false),
        false)
  );
$$;

ALTER FUNCTION private.memory_subject_visibility_ok(uuid, uuid) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION private.memory_subject_visibility_ok(uuid, uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION private.memory_subject_visibility_ok(uuid, uuid)
  TO role_gateway, role_maintenance;

COMMENT ON FUNCTION private.memory_subject_visibility_ok(uuid, uuid) IS
  '§6.1.3 subject-scoped visibility predicate (ADR-0029, migration 0155): true iff every subject '
  'linked to (p_tenant_id, p_memory_id) in private.memory_subjects is visible to the session '
  '(today: same tenant, via private.visibility_allowed TENANT_SHARED arm; the per-subject ACL '
  'hook lives here). SECURITY DEFINER to break the memory_records<->memory_subjects policy cycle; '
  'STABLE; search_path pinned. Called only by the RESTRICTIVE policy '
  'memory_records_subject_visibility.';

-- ============================================================================
-- B. The RESTRICTIVE policy. FOR SELECT (UPDATE/DELETE row fetches also run SELECT policies, so
--    an invisible target stays NOT_FOUND for the arbiter ops too). TO the two user-facing roles
--    only (A1). Verbatim tenant clause first (A2), then the predicate.
-- ============================================================================
CREATE POLICY memory_records_subject_visibility ON private.memory_records
  AS RESTRICTIVE
  FOR SELECT
  TO role_gateway, role_maintenance
  USING (
    tenant_id = current_setting('humaux.tenant_id', true)::uuid
    AND private.memory_subject_visibility_ok(tenant_id, memory_id)
  );

COMMENT ON POLICY memory_records_subject_visibility ON private.memory_records IS
  '§6.1.3 / ADR-0029 (migration 0155): AS RESTRICTIVE subject-visibility guard ANDed on top of '
  'memory_records_tenant_and_visibility for role_gateway/role_maintenance. Headless roles '
  '(retrieval/consolidation/private worker) are exempt so projection never skips a row (§15.7).';

-- ============================================================================
-- C. Projection-ticket backfill (ADR-0029 D-A, §15.1). The `subject_ids` payload field is
--    written only when the projection worker (re)projects a row. Points projected before this
--    change carry no such field, so the Qdrant any-of prefilter can never match them and a
--    subject-scoped recall would silently report "nothing is about A" for every memory linked
--    before this migration. The repo's one re-projection mechanism is a new stream ticket on the
--    memory's own stream (the MEMORY_LIFECYCLE ticket memory.supersede/restore/archive already
--    issue): the worker resolves it through ops.outbox -> memory_evidence -> memory_records,
--    re-reads private.memory_subjects in the same transaction and upserts the SAME deterministic
--    point id (registration is keyed by memory_id + updated_at + body_sha256, neither of which a
--    link changes) with the current payload shape. This is the §60 issue_stream_log_row +
--    insert_outbox sequence (remember.rs) verbatim, set-based, run once by the migration owner:
--    one ticket per (stream key, bound evidence) for every live-registered, active memory that
--    has at least one memory_subjects row. Unlinked memories need nothing — an absent array
--    field never matches the any-of prefilter, which is exactly "not about anyone". Until the
--    worker settles these tickets a subject-scoped recall is ordinary projection lag (§15.5
--    read-your-writes covers callers holding a token), not a permanent hole.
--    Each ticket resolves the way every MEMORY_LIFECYCLE ticket resolves (worker
--    resolve_memory: PRIMARY-first memory of the bound evidence, LIMIT 1) — the same binding
--    memory_governance_repo::issue_lifecycle_ticket uses.
-- ============================================================================
WITH linked AS (
  SELECT DISTINCT p.tenant_id, p.scope_kind, p.scope_id, p.domain, p.projection_kind,
         p.projection_version, bound.evidence_id
  FROM projection.private_memory_points p
  JOIN private.memory_records m
    ON m.tenant_id = p.tenant_id AND m.memory_id = p.memory_id
  JOIN LATERAL (
    SELECT me.evidence_id FROM private.memory_evidence me
    WHERE me.memory_id = m.memory_id
    ORDER BY (me.role = 'PRIMARY') DESC, me.ordinal ASC
    LIMIT 1
  ) bound ON true
  WHERE p.projection_live
    AND m.status = 'active' AND m.superseded_by IS NULL
    AND EXISTS (SELECT 1 FROM private.memory_subjects ms
                 WHERE ms.tenant_id = m.tenant_id AND ms.memory_id = m.memory_id)
),
ticketed AS (
  SELECT l.*, nextval('ops.commit_seq_seq') AS commit_seq,
         row_number() OVER (PARTITION BY l.tenant_id, l.scope_kind, l.scope_id, l.domain,
                                         l.projection_kind, l.projection_version
                            ORDER BY l.evidence_id) AS n
  FROM linked l
),
per_stream AS (
  SELECT tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version,
         count(*) AS cnt
  FROM linked
  GROUP BY 1, 2, 3, 4, 5, 6
),
bumped AS (
  UPDATE projection.stream_checkpoints c
     SET issued_highwater = c.issued_highwater + s.cnt
    FROM per_stream s
   WHERE c.tenant_id = s.tenant_id AND c.scope_kind = s.scope_kind AND c.scope_id = s.scope_id
     AND c.domain = s.domain AND c.projection_kind = s.projection_kind
     AND c.projection_version = s.projection_version
  RETURNING c.tenant_id, c.scope_kind, c.scope_id, c.domain, c.projection_kind,
            c.projection_version, c.issued_highwater - s.cnt AS base
),
logged AS (
  INSERT INTO projection.stream_log
    (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version,
     stream_seq, commit_seq)
  SELECT t.tenant_id, t.scope_kind, t.scope_id, t.domain, t.projection_kind,
         t.projection_version, b.base + t.n, t.commit_seq
  FROM ticketed t
  JOIN bumped b
    ON b.tenant_id = t.tenant_id AND b.scope_kind = t.scope_kind AND b.scope_id = t.scope_id
   AND b.domain = t.domain AND b.projection_kind = t.projection_kind
   AND b.projection_version = t.projection_version
  RETURNING tenant_id, commit_seq, stream_seq
)
INSERT INTO ops.outbox (tenant_id, commit_seq, stream_seq, event_type, evidence_id)
SELECT lg.tenant_id, lg.commit_seq, lg.stream_seq, 'MEMORY_LIFECYCLE', t.evidence_id
FROM logged lg
JOIN ticketed t ON t.tenant_id = lg.tenant_id AND t.commit_seq = lg.commit_seq;
