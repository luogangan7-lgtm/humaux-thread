-- §36 (governance op set) / §10.1 / ADR-0026 (Card 6): the distill candidate queue that makes
-- `memory.confirm` reachable. Research question 5 (no spec section defined a candidate registry,
-- its owner, or its TTL) is resolved here: `private.distill_candidates` is a per-tenant queue the
-- private worker writes during the Distill hop for outputs the §10.1 origin-bound ceiling REJECTS
-- (CandidateRejection), so a user can later promote one into `UserConfirmed` Evidence.
--
-- Producer / consumer split (§6.2.2):
--   * role_private_worker  — SELECT, INSERT (writes a PENDING candidate in the same write-leg
--                            transaction as the admitted memories; it never UPDATEs a candidate).
--   * role_gateway         — SELECT, UPDATE(state, confirmed_memory_id) (memory.confirm marks a
--                            candidate CONFIRMED naming the new memory; memory.reject marks it
--                            REJECTED — state only).
--   * role_maintenance     — SELECT, UPDATE(state) (the expiry sweep flips PENDING→EXPIRED;
--                            confirm already fails closed on `expires_at <= now()` independently,
--                            so the sweep is housekeeping, not a correctness gate — see ADR-0026).
--   * every other role     — nothing (a NAMED §6.2.2 table overrides the private-schema RW
--                            domain default entirely; absence here means `—`, not fall-back).
--
-- The rejected requested class + memory_type + the closed CandidateRejection reason ride on the
-- row (a user confirms the exact thing distill parsed); visibility / reasoning domain / data_class
-- are copied verbatim from the source Evidence so `memory.confirm` can materialize the memory
-- without re-reading it. `candidate_sha256` binds the confirm to the exact body the user reviewed.
--
-- §46 forward-fix (EXPAND_CONTRACT, next free number after 0151). New table + its grants only;
-- no DML, no existing object touched.

CREATE TABLE private.distill_candidates (
  candidate_id       uuid PRIMARY KEY DEFAULT uuidv7(),
  tenant_id          uuid NOT NULL REFERENCES control.tenants(tenant_id) ON DELETE CASCADE,
  -- The accepted Evidence the distill output was derived from (the confirm's provenance anchor:
  -- source_evidence_id -> candidate -> confirmed_memory_id -> memory_evidence -> UserConfirmed E2).
  -- ON DELETE CASCADE: a candidate is derived state — if its source Evidence is purged (§37) the
  -- queue entry is meaningless, so it goes too (never blocks the purge with RESTRICT).
  source_evidence_id uuid NOT NULL REFERENCES private.evidence_objects(evidence_id) ON DELETE CASCADE,
  -- The parsed memory content, verbatim as `bins/private-worker::distill::memory_content` shapes
  -- it ({title, key_claim}); memory.confirm passes it straight to the version-insert.
  candidate_body     jsonb NOT NULL,
  -- sha256 over the canonical candidate_body bytes; confirm locks by (candidate_id, this).
  candidate_sha256   bytea NOT NULL,
  -- Closed set mirrors humaux_domain::authority::CandidateRejection::as_db_str (§78.2 DB<->Rust).
  rejection_reason   text NOT NULL CHECK (rejection_reason IN
                     ('origin_authority_ceiling','untrusted_instruction',
                      'cross_tenant_evidence','missing_confirmation')),
  -- The AuthorityClass distill requested and the §10.1 ceiling refused for the source origin;
  -- confirm re-runs OriginBoundAuthorityPolicy for it under a UserConfirmed basis.
  requested_class    text NOT NULL CHECK (requested_class IN
                     ('PublicKnowledge','PrivateKnowledge','UserPreference','ProjectDecision',
                      'UserCorrection','ProjectConstraint','ExplicitTaskContext')),
  memory_type        text NOT NULL CHECK (memory_type IN
                     ('FACT','PREFERENCE','DECISION','REJECTION','STATE','ISSUE',
                      'LESSON','CONSTRAINT','PROCEDURE','OUTCOME','REFERENCE','NOTE')),
  confidence         real NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
  data_class         text NOT NULL CHECK (data_class IN
                     ('PUBLIC','INTERNAL','PRIVATE','SENSITIVE','SECRET_MATERIAL')),
  visibility_class   text NOT NULL CHECK (visibility_class IN
                     ('USER_PRIVATE','WORKSPACE_SHARED','TENANT_SHARED')),
  visibility_user_id uuid,
  visibility_workspace_id uuid,
  reasoning_domain_id uuid NOT NULL
                     REFERENCES control.private_reasoning_domains(reasoning_domain_id),
  occurred_at        timestamptz,
  state              text NOT NULL DEFAULT 'PENDING' CHECK (state IN
                     ('PENDING','CONFIRMED','REJECTED','EXPIRED')),
  -- The UserConfirmed memory a CONFIRMED candidate produced. NO FK — same §37 purge-survival
  -- rationale the CANDIDATES/LIFECYCLE rulings apply to ops.memory_lifecycle_events.memory_id: a
  -- RESTRICT FK would block the memory's own §37 purge, and SET NULL would violate the CONFIRMED
  -- CHECK below. The id is still recorded (traceability); the CHECK enforces it is present.
  confirmed_memory_id uuid,
  created_at         timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(created_at)),
  -- created_at + HUMAUX_PRIVATE_WORKER_CANDIDATE_TTL_SECONDS, computed by the producer (§78.1: no
  -- literal TTL in the schema).
  expires_at         timestamptz NOT NULL CHECK (isfinite(expires_at)),
  CONSTRAINT distill_candidates_visibility_matches_class CHECK (
       (visibility_class='USER_PRIVATE' AND visibility_user_id IS NOT NULL AND visibility_workspace_id IS NULL)
    OR (visibility_class='WORKSPACE_SHARED' AND visibility_user_id IS NULL AND visibility_workspace_id IS NOT NULL)
    OR (visibility_class='TENANT_SHARED' AND visibility_user_id IS NULL AND visibility_workspace_id IS NULL)
  ),
  CONSTRAINT distill_candidates_confirmed_has_memory
    CHECK (state <> 'CONFIRMED' OR confirmed_memory_id IS NOT NULL)
);
ALTER TABLE private.distill_candidates OWNER TO role_migration_owner;
ALTER TABLE private.distill_candidates ENABLE ROW LEVEL SECURITY;
ALTER TABLE private.distill_candidates FORCE ROW LEVEL SECURITY;

-- D-A: tenant-scoped RLS (a queue, not a per-row visibility surface — visibility is carried as
-- data for the confirm materialize, not enforced by this policy). Cross-tenant confirm is refused
-- here (0 rows -> NOT_FOUND), never in application code.
CREATE POLICY distill_candidates_tenant ON private.distill_candidates
  USING (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid)
  WITH CHECK (tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid);

-- Sweep + PENDING enumeration both scan (tenant, expires_at) over PENDING only.
CREATE INDEX idx_distill_candidates_pending
  ON private.distill_candidates (tenant_id, expires_at)
  WHERE state = 'PENDING';
CREATE INDEX idx_distill_candidates_source
  ON private.distill_candidates (tenant_id, source_evidence_id);

REVOKE ALL ON private.distill_candidates FROM PUBLIC, role_gateway, role_private_worker,
  role_consolidation_worker, role_public_worker, role_retrieval_worker, role_batch_issuer,
  role_maintenance, role_admin;
GRANT SELECT, INSERT ON private.distill_candidates TO role_private_worker;
GRANT SELECT ON private.distill_candidates TO role_gateway;
GRANT UPDATE (state, confirmed_memory_id) ON private.distill_candidates TO role_gateway;
GRANT SELECT ON private.distill_candidates TO role_maintenance;
GRANT UPDATE (state) ON private.distill_candidates TO role_maintenance;

COMMENT ON TABLE private.distill_candidates IS
  '§10.1/ADR-0026 (Card 6): per-tenant queue of distill outputs the origin-bound ceiling '
  'rejected, so a user can promote one into UserConfirmed Evidence via memory.confirm. '
  'Permissions: §6.2.2 only (private_worker SELECT,INSERT; gateway SELECT + UPDATE(state,'
  'confirmed_memory_id); maintenance SELECT + UPDATE(state); every other role —). Tenant-scoped '
  'RLS; confirm binds by (candidate_id, candidate_sha256) and fails closed on expires_at.';
