-- §12/§13/Phase 9 (spec:9829-9846) public contribution hardening over 0010's skeleton.
-- Everything here is DDL on provably-zero-row tables (manifest precheck) — no DML, no
-- backfill defaults invented for rows that do not exist.

-- =============================================================================
-- §12.1 + §12.5 + §13: staging.contribution_releases. The table is zero rows (manifest
-- precheck), so NOT NULL columns are added without fabricated DEFAULTs — a future row
-- must state its policy snapshot / scan outcomes / rights basis explicitly.
-- =============================================================================

ALTER TABLE staging.contribution_releases
  -- §12.1 "必须保存 policy snapshot + consent/grant version": snapshot shape (consent
  -- version, grant version) is application-managed jsonb; the DB freezes what §12.1
  -- freezes — the policy field must exist and sit in the closed set. The IS NOT NULL
  -- conjunct is load-bearing: `->>' yields NULL for a missing key (or a scalar jsonb),
  -- and CHECK passes UNKNOWN — a bare `IN (...)` silently admits '{}' (review blocker,
  -- reproduced live before this fix). DISABLED is deliberately NOT in the release
  -- snapshot's set: a ContributionRelease row records a release that happened, and §12.1
  -- forbids releasing under DISABLED — a DISABLED snapshot row is a contradiction, kept
  -- unrepresentable (domain::public::ContributionRelease::release rejects it type-side;
  -- ContributionPolicy itself keeps all three values — the policy CONFIG space is three,
  -- the legal RELEASE-snapshot space is two).
  ADD COLUMN policy_snapshot jsonb NOT NULL,
  ADD CONSTRAINT contribution_releases_policy_closed CHECK (
    (policy_snapshot->>'policy') IS NOT NULL
    AND (policy_snapshot->>'policy') IN ('MANUAL','AUTO_AFTER_USER_DISTILLATION')
  ),
  -- §13 "ACTIVE -> REVOKED". Biconditional is the G59-4 status/superseded_by shape:
  -- REVOKED iff revoked_at is set, no half-revoked rows in either direction.
  ADD COLUMN state text NOT NULL DEFAULT 'ACTIVE' CHECK (state IN ('ACTIVE','REVOKED')),
  ADD COLUMN revoked_at timestamptz,
  ADD CONSTRAINT contribution_releases_revoked_iff_timestamped CHECK (
    (state = 'REVOKED') = (revoked_at IS NOT NULL)
  ),
  -- §12 pipeline: De-identification / Secret Scan / Policy sit BEFORE ContributionRelease,
  -- so a Release row must carry both scan verdicts. The PASSED/BLOCKED value set is
  -- derived from that flow (a gate either passed or blocked the release) — §12 does not
  -- freeze the spelling verbatim.
  ADD COLUMN privacy_scan_outcome text NOT NULL CHECK (privacy_scan_outcome IN ('PASSED','BLOCKED')),
  ADD COLUMN secret_scan_outcome  text NOT NULL CHECK (secret_scan_outcome  IN ('PASSED','BLOCKED')),
  -- §12.5 rights provenance, five fields verbatim. Only rights_basis is NOT NULL —
  -- §12.5 lists the other four without a nullability ruling, and a user contribution
  -- legitimately has no publisher/source_license.
  ADD COLUMN rights_basis            text NOT NULL,
  ADD COLUMN source_license          text,
  ADD COLUMN publisher               text,
  ADD COLUMN contributor_attestation text,
  ADD COLUMN redistribution_policy   text;

-- =============================================================================
-- §12.1 (spec:2694-2703): staging.contribution_release_sources rebuilt to the spec's
-- shape — "release_id / evidence_id? / memory_id? / ordinal", exactly one source id per
-- row, "Public provenance DAG 从这张表开始闭包". 0010's skeleton had memory_id only and
-- PK (release_id, memory_id); the table is zero rows (manifest precheck) so DROP/CREATE
-- is a pure shape swap, no data motion. Column name stays `contribution_release_id`
-- (0010/0047 canonical naming; spec's `release_id` is shorthand for the same key).
-- =============================================================================

DROP TABLE staging.contribution_release_sources;

CREATE TABLE staging.contribution_release_sources (
  contribution_release_id uuid NOT NULL REFERENCES staging.contribution_releases(contribution_release_id),
  evidence_id             uuid REFERENCES private.evidence_objects(evidence_id),
  memory_id               uuid REFERENCES private.memory_records(memory_id),
  ordinal                 integer NOT NULL,
  -- §12.1 "CHECK：evidence_id / memory_id 恰好一个非 NULL".
  CONSTRAINT contribution_release_sources_exactly_one_source
    CHECK ((evidence_id IS NULL) <> (memory_id IS NULL)),
  PRIMARY KEY (contribution_release_id, ordinal)
);
-- ponytail: no reverse-lookup index on evidence_id/memory_id yet — §13 revocation walks
-- release -> sources (PK prefix covers it); add the reverse indexes when a "which
-- releases cite memory X" query actually ships.

-- Recreate 0012's derived-tenant policy (destroyed with the old table): this table has
-- no tenant_id of its own — tenant comes from the parent release, in the 0031
-- NULLIF-hardened GUC form so a no-context session sees NULL (never an ERROR, never a
-- cross-tenant row).
CREATE POLICY contribution_release_sources_tenant ON staging.contribution_release_sources
USING (EXISTS (
  SELECT 1 FROM staging.contribution_releases cr
  WHERE cr.contribution_release_id = contribution_release_sources.contribution_release_id
    AND cr.tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
))
WITH CHECK (EXISTS (
  SELECT 1 FROM staging.contribution_releases cr
  WHERE cr.contribution_release_id = contribution_release_sources.contribution_release_id
    AND cr.tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid
));

ALTER TABLE staging.contribution_release_sources ENABLE ROW LEVEL SECURITY;
ALTER TABLE staging.contribution_release_sources FORCE ROW LEVEL SECURITY;

-- =============================================================================
-- §12.4 (spec:2757-2763): PublicSource.source_type closed 5-value set. trust_class gets
-- NO CHECK: its only spec occurrence is §70.5's DDL line `trust_class text NOT NULL` —
-- no section freezes a value set, so a DB enum here would be invented, not transcribed.
-- =============================================================================

ALTER TABLE public.sources
  ADD CONSTRAINT sources_source_type_closed CHECK (
    source_type IN ('USER_CONTRIBUTION','OFFICIAL_DOCUMENT','PUBLIC_WEB','OPEN_SOURCE_DOCUMENT','ADMIN_IMPORT')
  );

-- =============================================================================
-- §12 Public Claim Trust Model (spec:2806-2814): the six per-claim trust columns,
-- verbatim names. moderation_state's closed set is DERIVED from the Quarantine/Promotion
-- flow (spec:2846-2852 "PUBLIC_STAGING -> trust evaluation -> ... -> supported claim",
-- 审核/较低权威层) plus §13 revocation — the spec names the states in prose, not as a
-- frozen enum: PUBLIC_STAGING (entry), UNDER_REVIEW (审核), SUPPORTED, QUARANTINED,
-- REVOKED (§13). Default is the flow's entry state: 「一贡献即成为 public truth」被禁止.
-- The four class/score columns stay open text/numeric — §12 explicitly refuses a
-- "神奇总分" and keeps dimensions policy-combinable, so no DB-side value freeze.
-- =============================================================================

ALTER TABLE public.claims
  ADD COLUMN moderation_state text NOT NULL DEFAULT 'PUBLIC_STAGING' CHECK (
    moderation_state IN ('PUBLIC_STAGING','UNDER_REVIEW','SUPPORTED','QUARANTINED','REVOKED')
  ),
  ADD COLUMN source_quality_class            text,
  ADD COLUMN contributor_independence_class  text,
  ADD COLUMN support_weight                  numeric,
  ADD COLUMN anomaly_score                   numeric,
  ADD COLUMN poisoning_flags                 jsonb NOT NULL DEFAULT '[]'::jsonb;

-- =============================================================================
-- §12.3 递归演化: "Claim A + Claim B -> S1; S1 + Claim C -> S2" with
-- source_closure(S2) = {A,B,C} — the closure is only computable if each synthesis
-- records its direct inputs, claim or prior synthesis, exactly one per row (same
-- exactly-one CHECK shape as contribution_release_sources). public.* is tenant-agnostic
-- (0010 header, §5.1) — no tenant_id, no RLS, matching every other public.* table.
-- =============================================================================

CREATE TABLE public.synthesis_inputs (
  synthesis_id       uuid NOT NULL REFERENCES public.syntheses(synthesis_id),
  claim_id           uuid REFERENCES public.claims(claim_id),
  input_synthesis_id uuid REFERENCES public.syntheses(synthesis_id),
  ordinal            integer NOT NULL,
  CONSTRAINT synthesis_inputs_exactly_one_input
    CHECK ((claim_id IS NULL) <> (input_synthesis_id IS NULL)),
  PRIMARY KEY (synthesis_id, ordinal)
);
-- ponytail: acyclicity of synthesis -> input_synthesis edges is not enforceable in a row
-- CHECK — §12.3's closure worker is where a cycle guard lands if recursive synthesis ships.

-- =============================================================================
-- §12.4 (spec:2750-2755): ops.source_acquisition_jobs — gap-driven acquisition,
-- "公共知识补全只能通过新增 Evidence". 0008 already created the skeleton
-- (source_acquisition_job_id PK / source_url / created_at), so this is an ALTER, not a
-- CREATE; the PK keeps 0008's <table_singular>_id canonical name. knowledge_gap_id gets
-- NOT NULL with no DEFAULT — zero rows (manifest precheck), and §12.4's flow starts from
-- a detected gap, so a gap-less job is meaningless. Cluster-level like the
-- public.knowledge_gaps it FKs (no tenant_id ⇒ outside §62's RLS enumeration domain,
-- deliberately). The 4-state set is the plain job lifecycle the section implies, not a
-- spec-frozen enum. updated_at is writer-maintained (no trigger) — same convention as
-- other ops.* job tables.
-- =============================================================================

ALTER TABLE ops.source_acquisition_jobs
  ADD COLUMN knowledge_gap_id uuid        NOT NULL REFERENCES public.knowledge_gaps(knowledge_gap_id),
  ADD COLUMN state            text        NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING','RUNNING','DONE','FAILED')),
  ADD COLUMN updated_at       timestamptz NOT NULL DEFAULT now();

-- =============================================================================
-- Grants & ownership. No explicit GRANT statements: 0011's ALTER DEFAULT PRIVILEGES
-- (declared FOR the migration runner role) already hands every table this file creates
-- its §6.2.1 domain defaults — staging: role_private_worker I/U + role_public_worker
-- SELECT; public: role_public_worker S/I/U + role_gateway/role_retrieval_worker SELECT;
-- ops: worker roles S/I/U; role_maintenance SELECT everywhere (verified live on
-- 0096's ops.tenant_cost_events). No role gets DELETE — §13 revocation is
-- `UPDATE ... SET state='REVOKED'`, never row deletion. Ownership goes to
-- role_migration_owner (§6.2.1 "发票权唯一"), same as 0096; new columns on existing
-- tables inherit their table's owner/grants and need nothing here.
-- =============================================================================

ALTER TABLE staging.contribution_release_sources OWNER TO role_migration_owner;
ALTER TABLE public.synthesis_inputs             OWNER TO role_migration_owner;
-- ops.source_acquisition_jobs keeps its 0011-assigned owner/grants (pre-existing table).
