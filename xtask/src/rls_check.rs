//! xtask `rls-check` — G80-26 (§48.2 Runtime DB Role Invariant CI enumeration gate),
//! folding in §62's tenant RLS policy template. §48.2's own text: "上面四项只覆盖 RLS
//! 面。表级授权进同一次枚举" — so the RLS four-item enumeration and the five §48.2
//! grant checks live in one module and one `run()`.
//!
//! Five §48.2 checks, referenced **by name** per its own instruction ("按名引用，不按
//! 序号"): role set equality · table-set derivation · per-cell grant equality ·
//! forbidden-verb sweep · invoice-privilege uniqueness. Plus the §62/§48.2 RLS
//! four-item enumeration: RLS enabled + FORCE + tenant policy + non-owner, over every
//! table carrying a `tenant_id` column.
//!
//! DB-backed checks connect via `HUMAUX_TEST_PG_DSN` (sync `postgres` crate, `NoTls`).
//! Unset env var, unreachable DB, or a queried role/table/schema that doesn't exist yet
//! ⇒ `not_applicable` naming the missing object (§57.1 rule 2: a `not_applicable` that
//! can't point at the object is "扫不到当通过", CLAUDE.md 坑 5). The table-set-derivation
//! check needs no DB — it's a pure scan of [`SPEC_PATH`] — and always runs pass/fail.

use postgres::{Client, GenericClient, NoTls};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;

/// The seven §5.1 logical schemas — shared by the backtick table-name scanner and the
/// §6.2.1 domain-default matrix's column order.
const SCHEMAS: &[&str] = &[
    "control",
    "private",
    "staging",
    "public",
    "projection",
    "coord",
    "ops",
];

const SPEC_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../docs/architecture/Baseline_2.9.md"
);

/// §57.1: every gate/check is three-state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateStatus {
    Pass,
    Fail,
    NotApplicable,
}

/// Uniform three-state outcome for one named §48.2 check.
#[derive(Debug, Clone)]
pub struct GateResult {
    pub check: String,
    pub status: GateStatus,
    pub detail: String,
}

fn pass(check: impl Into<String>, detail: impl Into<String>) -> GateResult {
    GateResult {
        check: check.into(),
        status: GateStatus::Pass,
        detail: detail.into(),
    }
}
fn fail(check: impl Into<String>, detail: impl Into<String>) -> GateResult {
    GateResult {
        check: check.into(),
        status: GateStatus::Fail,
        detail: detail.into(),
    }
}
fn not_applicable(check: impl Into<String>, detail: impl Into<String>) -> GateResult {
    GateResult {
        check: check.into(),
        status: GateStatus::NotApplicable,
        detail: detail.into(),
    }
}

// ============================================================================
// §6.2.0 — frozen role enumeration. Definition lives only here; every check below
// reads these two slices, none re-derives the split from a "looks like runtime" guess
// (§6.2.0: "判据只是设计意图的说明；两者冲突时以枚举为准").
// ============================================================================

pub const RUNTIME_ROLES: &[&str] = &[
    "role_gateway",
    "role_private_worker",
    "role_consolidation_worker",
    "role_public_worker",
    "role_retrieval_worker",
];
pub const NON_RUNTIME_ROLES: &[&str] = &[
    "role_batch_issuer",
    "role_maintenance",
    "role_admin",
    "role_migration_owner",
];

/// The one role §6.2.2 gives `owner` on every table — excluded from grant-equality and
/// invoice-uniqueness queries (§48.2 "发票权唯一": owner implicitly holds every
/// privilege; not excluding it makes the query true on a clean DB, i.e. permanently red).
const OWNER_ROLE: &str = "role_migration_owner";

/// Roles that must own nothing and hold no DELETE/TRUNCATE/DDL anywhere (§6.2.1 "全域硬
/// 约束，无例外": runtime role + role_batch_issuer + role_maintenance; `role_migration_owner`
/// is the one role meant to own tables, so it is not in this set).
const NON_OWNER_ROLES: &[&str] = &[
    "role_gateway",
    "role_private_worker",
    "role_consolidation_worker",
    "role_public_worker",
    "role_retrieval_worker",
    "role_batch_issuer",
    "role_maintenance",
    "role_admin",
];

fn frozen_role_set() -> BTreeSet<&'static str> {
    RUNTIME_ROLES
        .iter()
        .chain(NON_RUNTIME_ROLES.iter())
        .copied()
        .collect()
}

// ============================================================================
// §6.2.2 — table-level/column-level grant matrix (the Phase9 point-named tables). One
// `Cell` per non-empty (table, role) entry; a (table, role) pair absent from MATRIX
// means the matrix cell is `—` (explicitly empty, §6.2.2 overrides §6.2.1's domain
// default entirely for a listed table — absence is not "fall back to default").
// `role_migration_owner`'s `owner` cells are not modeled here — checked via table
// ownership instead (`check_grant_equality` / `check_forbidden_verbs`).
// ============================================================================

/// One §6.2.2 grant cell. `table_verbs` compares against
/// `information_schema.role_table_grants`; `col_verbs` (verb, columns) compares against
/// `information_schema.column_privileges` — spec's own split ("带括号的是 column-level
/// GRANT... 不带括号的是表级").
struct Cell {
    table: &'static str,
    role: &'static str,
    table_verbs: &'static [&'static str],
    col_verbs: &'static [(&'static str, &'static [&'static str])],
}

macro_rules! cell {
    ($table:expr, $role:expr, [$($tv:expr),* $(,)?]) => {
        Cell { table: $table, role: $role, table_verbs: &[$($tv),*], col_verbs: &[] }
    };
    ($table:expr, $role:expr, [$($tv:expr),*], [$(($cv:expr, [$($col:expr),* $(,)?])),* $(,)?]) => {
        Cell { table: $table, role: $role, table_verbs: &[$($tv),*], col_verbs: &[$(($cv, &[$($col),*])),*] }
    };
}

/// §6.2.2's matrix, transcribed cell by cell (spec §6.2.2 is the single source of truth
/// for the prose; this is the runnable form of the same table, not a second copy of its
/// reasoning).
const MATRIX: &[Cell] = &[
    cell!("ops.mechanism_observations", "role_gateway", ["SELECT"]),
    cell!(
        "ops.mechanism_observations",
        "role_private_worker",
        ["SELECT"]
    ),
    cell!(
        "ops.mechanism_observations",
        "role_consolidation_worker",
        ["SELECT"]
    ),
    cell!(
        "ops.mechanism_observations",
        "role_public_worker",
        ["SELECT"]
    ),
    cell!(
        "ops.mechanism_observations",
        "role_retrieval_worker",
        ["SELECT"]
    ),
    cell!(
        "ops.mechanism_observations",
        "role_maintenance",
        ["SELECT", "INSERT"]
    ),
    cell!("ops.mechanism_observations", "role_admin", ["SELECT"]),
    cell!("ops.mechanism_e2e_runs", "role_gateway", ["SELECT"]),
    cell!("ops.mechanism_e2e_runs", "role_private_worker", ["SELECT"]),
    cell!(
        "ops.mechanism_e2e_runs",
        "role_consolidation_worker",
        ["SELECT"]
    ),
    cell!("ops.mechanism_e2e_runs", "role_public_worker", ["SELECT"]),
    cell!(
        "ops.mechanism_e2e_runs",
        "role_retrieval_worker",
        ["SELECT"]
    ),
    cell!(
        "ops.mechanism_e2e_runs",
        "role_maintenance",
        ["SELECT", "INSERT"]
    ),
    cell!("ops.mechanism_e2e_runs", "role_admin", ["SELECT"]),
    // private.ingest_tickets
    cell!(
        "private.ingest_tickets",
        "role_gateway",
        ["SELECT", "UPDATE"]
    ),
    cell!("private.ingest_tickets", "role_private_worker", ["SELECT"]),
    cell!(
        "private.ingest_tickets",
        "role_batch_issuer",
        ["INSERT", "SELECT"]
    ),
    cell!(
        "private.ingest_tickets",
        "role_maintenance",
        ["SELECT", "UPDATE"]
    ),
    // private.events
    cell!("private.events", "role_gateway", ["SELECT", "INSERT"]),
    cell!("private.events", "role_private_worker", ["SELECT"]),
    cell!("private.events", "role_maintenance", ["SELECT"]),
    // projection.stream_log
    cell!(
        "projection.stream_log",
        "role_gateway",
        ["SELECT", "INSERT"]
    ),
    cell!(
        "projection.stream_log",
        "role_private_worker",
        ["SELECT"],
        [("UPDATE", ["state", "error_class"])]
    ),
    cell!(
        "projection.stream_log",
        "role_retrieval_worker",
        ["SELECT", "UPDATE"]
    ),
    cell!(
        "projection.stream_log",
        "role_maintenance",
        ["SELECT", "UPDATE"]
    ),
    // projection.stream_checkpoints
    cell!(
        "projection.stream_checkpoints",
        "role_gateway",
        ["SELECT"],
        [
            (
                "INSERT",
                [
                    "tenant_id",
                    "scope_kind",
                    "scope_id",
                    "domain",
                    "projection_kind",
                    "projection_version"
                ]
            ),
            ("UPDATE", ["issued_highwater"]),
        ]
    ),
    cell!(
        "projection.stream_checkpoints",
        "role_private_worker",
        ["SELECT"]
    ),
    cell!(
        "projection.stream_checkpoints",
        "role_retrieval_worker",
        ["SELECT"],
        [(
            "UPDATE",
            [
                "evidence_highwater",
                "knowledge_highwater",
                "projection_highwater"
            ]
        )]
    ),
    cell!(
        "projection.stream_checkpoints",
        "role_maintenance",
        ["SELECT"],
        [("UPDATE", ["serving", "shadow"])]
    ),
    // ops.outbox (canonical name; spec §6.2.2 tail note: `outbox_event` is the retired alias)
    // SELECT added by T3.8 (§15.5, migrations/0046_stream_log_evidence_id_via_outbox.sql):
    // the read-your-writes overlay joins projection.stream_log to this table on
    // (tenant_id, commit_seq) to recover the evidence_id a stream_log row does not itself
    // carry — see that migration's header for why commit_seq, not stream_seq, is the join key.
    cell!(
        "ops.outbox",
        "role_gateway",
        ["SELECT"],
        [(
            "INSERT",
            [
                "tenant_id",
                "commit_seq",
                "stream_seq",
                "event_type",
                "evidence_id"
            ]
        )]
    ),
    cell!(
        "ops.outbox",
        "role_private_worker",
        ["SELECT", "UPDATE"],
        [(
            "INSERT",
            [
                "tenant_id",
                "commit_seq",
                "event_type",
                "contribution_release_id",
                "anonymous_source_id",
                "candidate_envelope_sha256",
                "anonymous_source_revision"
            ]
        )]
    ),
    cell!("ops.outbox", "role_public_worker", []),
    cell!("ops.outbox", "role_retrieval_worker", ["SELECT", "UPDATE"]),
    cell!("ops.outbox", "role_maintenance", ["SELECT"]),
    // §12/§13, ADR-0008: release IO is separate from gateway Evidence production.
    cell!(
        "staging.contribution_releases",
        "role_private_worker",
        ["SELECT", "INSERT"],
        [("UPDATE", ["state", "revoked_at"])]
    ),
    cell!("staging.contribution_releases", "role_public_worker", []),
    cell!(
        "staging.contribution_releases",
        "role_maintenance",
        ["SELECT"]
    ),
    cell!(
        "staging.contribution_release_sources",
        "role_private_worker",
        ["SELECT", "INSERT"]
    ),
    cell!(
        "staging.contribution_release_sources",
        "role_public_worker",
        ["SELECT"]
    ),
    cell!(
        "staging.contribution_release_sources",
        "role_maintenance",
        ["SELECT"]
    ),
    // ops.jobs
    cell!("ops.jobs", "role_gateway", ["SELECT", "INSERT", "UPDATE"]),
    cell!(
        "ops.jobs",
        "role_private_worker",
        ["SELECT", "INSERT", "UPDATE"]
    ),
    cell!(
        "ops.jobs",
        "role_consolidation_worker",
        ["SELECT"],
        [("UPDATE", ["status", "lease_owner", "lease_expires_at"])]
    ),
    cell!("ops.jobs", "role_public_worker", []),
    cell!(
        "ops.jobs",
        "role_retrieval_worker",
        ["SELECT", "INSERT", "UPDATE"]
    ),
    cell!(
        "ops.jobs",
        "role_maintenance",
        ["SELECT"],
        [("UPDATE", ["status", "lease_owner", "lease_expires_at"])]
    ),
    // control.quota_windows
    cell!(
        "control.quota_windows",
        "role_gateway",
        ["SELECT"],
        [("UPDATE", ["reserved", "consumed"])]
    ),
    cell!("control.quota_windows", "role_private_worker", ["SELECT"]),
    cell!("control.quota_windows", "role_maintenance", ["SELECT"]),
    // §72.2.1 / §6.2.2: durable request accounting and rate limiting.
    cell!(
        "control.usage_reservations",
        "role_gateway",
        ["SELECT", "INSERT"],
        [("UPDATE", ["status", "finished_at"])]
    ),
    cell!("control.usage_reservations", "role_maintenance", ["SELECT"]),
    cell!(
        "control.rate_buckets",
        "role_gateway",
        ["SELECT", "INSERT"],
        [(
            "UPDATE",
            [
                "capacity",
                "tokens",
                "refill_per_second",
                "updated_at",
                "version"
            ]
        )]
    ),
    cell!("control.rate_buckets", "role_maintenance", ["SELECT"]),
    cell!(
        "control.operation_receipts",
        "role_gateway",
        ["SELECT", "INSERT"]
    ),
    cell!("control.operation_receipts", "role_maintenance", ["SELECT"]),
    // §19 native retrieval query disclosure source: only the retrieval worker may create it;
    // maintenance may revoke it without rewriting the immutable identity.
    cell!(
        "private.retrieval_query_sources",
        "role_retrieval_worker",
        ["SELECT", "INSERT"]
    ),
    cell!(
        "private.retrieval_query_sources",
        "role_maintenance",
        ["SELECT"],
        [("UPDATE", ["revoked_at", "revocation_reason"])]
    ),
    // private.evidence_objects
    cell!(
        "private.evidence_objects",
        "role_gateway",
        ["SELECT", "INSERT"]
    ),
    cell!(
        "private.evidence_objects",
        "role_private_worker",
        ["SELECT"]
    ),
    cell!(
        "private.evidence_objects",
        "role_consolidation_worker",
        ["SELECT"]
    ),
    cell!(
        "private.evidence_objects",
        "role_retrieval_worker",
        ["SELECT"]
    ),
    cell!("private.evidence_objects", "role_maintenance", ["SELECT"]),
    // private.memory_records
    cell!(
        "private.memory_records",
        "role_gateway",
        ["SELECT", "INSERT"],
        [("UPDATE", ["status", "superseded_by"])]
    ),
    cell!(
        "private.memory_records",
        "role_private_worker",
        ["SELECT", "INSERT"]
    ),
    cell!(
        "private.memory_records",
        "role_consolidation_worker",
        ["SELECT"]
    ),
    cell!(
        "private.memory_records",
        "role_retrieval_worker",
        ["SELECT"]
    ),
    cell!("private.memory_records", "role_maintenance", ["SELECT"]),
    // private.memory_evidence
    cell!(
        "private.memory_evidence",
        "role_gateway",
        ["SELECT", "INSERT"]
    ),
    cell!(
        "private.memory_evidence",
        "role_private_worker",
        ["SELECT", "INSERT"]
    ),
    cell!(
        "private.memory_evidence",
        "role_consolidation_worker",
        ["SELECT"]
    ),
    cell!(
        "private.memory_evidence",
        "role_retrieval_worker",
        ["SELECT"]
    ),
    cell!("private.memory_evidence", "role_maintenance", ["SELECT"]),
    // private.memory_consolidation_runs
    cell!(
        "private.memory_consolidation_runs",
        "role_consolidation_worker",
        ["SELECT", "INSERT"],
        [(
            "UPDATE",
            [
                "status",
                "input_snapshot_seq",
                "manifest_hash",
                "output_digest",
                "finished_at",
                "error_class"
            ]
        )]
    ),
    cell!(
        "private.memory_consolidation_runs",
        "role_maintenance",
        ["SELECT"]
    ),
    // private.memory_consolidation_inputs
    cell!(
        "private.memory_consolidation_inputs",
        "role_consolidation_worker",
        ["SELECT", "INSERT"]
    ),
    cell!(
        "private.memory_consolidation_inputs",
        "role_maintenance",
        ["SELECT"]
    ),
    // private.memory_rollups
    cell!("private.memory_rollups", "role_gateway", ["SELECT"]),
    cell!(
        "private.memory_rollups",
        "role_consolidation_worker",
        ["SELECT", "INSERT"]
    ),
    cell!(
        "private.memory_rollups",
        "role_retrieval_worker",
        ["SELECT"]
    ),
    cell!("private.memory_rollups", "role_maintenance", ["SELECT"]),
    // private.memory_rollup_sources
    cell!("private.memory_rollup_sources", "role_gateway", ["SELECT"]),
    cell!(
        "private.memory_rollup_sources",
        "role_consolidation_worker",
        ["SELECT", "INSERT"]
    ),
    cell!(
        "private.memory_rollup_sources",
        "role_retrieval_worker",
        ["SELECT"]
    ),
    cell!(
        "private.memory_rollup_sources",
        "role_maintenance",
        ["SELECT"]
    ),
    // ops.deletion_plan_steps (T4.8 follow-up, migrations/0056): named-table override
    // closing a §6.2.1 domain-default over-grant a review found — the four runtime roles
    // held INSERT/UPDATE via the `ops` schema's domain default (meant for ops.jobs/
    // ops.outbox's own named cells below), letting role_gateway forge a completed purge
    // step (§41.2) for any deletion_request_id in its own tenant. Writes go through
    // `ops.record_deletion_plan_step` (0054/0055, SECURITY DEFINER, owned by
    // role_migration_owner) — no caller-side table grant is needed for that path.
    // role_consolidation_worker/role_maintenance keep exactly their pre-existing
    // domain-default SELECT (reaffirmed explicitly, not silently dropped, now that this
    // table is named); role_maintenance additionally holds a direct INSERT (0056).
    cell!("ops.deletion_plan_steps", "role_gateway", ["SELECT"]),
    cell!("ops.deletion_plan_steps", "role_private_worker", ["SELECT"]),
    cell!(
        "ops.deletion_plan_steps",
        "role_consolidation_worker",
        ["SELECT"]
    ),
    cell!("ops.deletion_plan_steps", "role_public_worker", ["SELECT"]),
    cell!(
        "ops.deletion_plan_steps",
        "role_retrieval_worker",
        ["SELECT"]
    ),
    cell!(
        "ops.deletion_plan_steps",
        "role_maintenance",
        ["SELECT", "INSERT"]
    ),
    // Phase9 public runtime and contribution staging tables (0106/0107).
    cell!(
        "control.public_moderator_grants",
        "role_public_worker",
        ["SELECT"]
    ),
    cell!(
        "control.public_moderator_grants",
        "role_maintenance",
        ["SELECT"]
    ),
    cell!(
        "ops.public_release_revocations",
        "role_private_worker",
        ["INSERT"]
    ),
    cell!("ops.public_release_revocations", "role_gateway", ["SELECT"]),
    cell!(
        "ops.public_release_revocations",
        "role_public_worker",
        ["SELECT"]
    ),
    cell!(
        "ops.public_release_revocations",
        "role_retrieval_worker",
        ["SELECT"]
    ),
    cell!(
        "ops.public_release_revocations",
        "role_maintenance",
        ["SELECT"]
    ),
    cell!(
        "public.claim_trust_evaluations",
        "role_public_worker",
        [],
        [(
            "INSERT",
            [
                "evaluation_id",
                "claim_id",
                "synthesis_id",
                "object_revision",
                "body_sha256",
                "policy_version",
                "moderation_state",
                "evaluator_user_id",
                "evaluator_grant_version",
                "rationale",
                "support_count",
                "independent_support_count",
                "trusted_source_count",
                "identity_incomplete",
                "contradiction_count",
                "checks_complete"
            ]
        )]
    ),
    cell!(
        "public.claim_trust_evaluations",
        "role_maintenance",
        ["SELECT"]
    ),
    cell!(
        "public.claim_trust_evaluation_sources",
        "role_public_worker",
        [],
        [(
            "INSERT",
            [
                "evaluation_id",
                "root_source_id",
                "source_content_hash",
                "contribution_release_id"
            ]
        )]
    ),
    cell!(
        "public.claim_trust_evaluation_sources",
        "role_maintenance",
        ["SELECT"]
    ),
    cell!(
        "public.poisoning_signals",
        "role_public_worker",
        [],
        [(
            "INSERT",
            ["evaluation_id", "signal_code", "check_version", "detected"]
        )]
    ),
    cell!("public.poisoning_signals", "role_maintenance", ["SELECT"]),
    cell!(
        "staging.contribution_candidates",
        "role_private_worker",
        ["SELECT", "INSERT"]
    ),
    cell!(
        "staging.contribution_candidates",
        "role_maintenance",
        ["SELECT"]
    ),
    cell!(
        "staging.contribution_candidate_sources",
        "role_private_worker",
        ["SELECT", "INSERT"]
    ),
    cell!(
        "staging.contribution_candidate_sources",
        "role_maintenance",
        ["SELECT"]
    ),
    cell!(
        "control.contribution_confirmations",
        "role_gateway",
        ["SELECT", "INSERT"]
    ),
    cell!(
        "control.contribution_confirmations",
        "role_private_worker",
        ["SELECT"]
    ),
    cell!(
        "control.contribution_confirmations",
        "role_maintenance",
        ["SELECT"]
    ),
    // 0124: aggregate receipt is public-safe, but its protected write path is definer-only.
    cell!(
        "public.claim_independence_attestations",
        "role_gateway",
        ["SELECT"]
    ),
    cell!(
        "public.claim_independence_attestations",
        "role_public_worker",
        ["SELECT"]
    ),
    cell!(
        "public.claim_independence_attestations",
        "role_retrieval_worker",
        ["SELECT"]
    ),
    cell!(
        "public.claim_independence_attestations",
        "role_maintenance",
        ["SELECT"]
    ),
    // 0135 owner-rights hydration view: runtime reads the outer safe projection only.
    cell!("public.eligible_objects", "role_gateway", ["SELECT"]),
    cell!("public.eligible_objects", "role_public_worker", ["SELECT"]),
    cell!(
        "public.eligible_objects",
        "role_retrieval_worker",
        ["SELECT"]
    ),
    cell!("public.eligible_objects", "role_maintenance", ["SELECT"]),
];

// 0120 is an additive observation-window seam, not a rewrite of the frozen §6.2.2 header.
// Keep its narrow grants executable and bidirectionally checked while the later contract
// phase decides whether to merge these columns into the Canonical matrix.
const ADDITIVE_SEAM_MATRIX: &[Cell] = &[
    // 0134: moderator identity/rationale remain owner-only. The private worker may invoke
    // one narrow definer but receives no direct protected-table privilege.
    cell!(
        "control.anonymous_claim_trust_authorities",
        "role_private_worker",
        []
    ),
    cell!(
        "public.anonymous_claim_trust_receipts",
        "role_gateway",
        ["SELECT"]
    ),
    cell!(
        "public.anonymous_claim_trust_receipts",
        "role_public_worker",
        ["SELECT"]
    ),
    cell!(
        "public.anonymous_claim_trust_receipts",
        "role_retrieval_worker",
        ["SELECT"]
    ),
    cell!(
        "public.anonymous_claim_trust_receipts",
        "role_maintenance",
        ["SELECT"]
    ),
    // Phase9 anonymous public seam (0120): protected release resolution stays private;
    // only the public lifecycle facts are readable by gateway/retrieval roles.
    cell!(
        "control.anonymous_source_lineage",
        "role_private_worker",
        ["SELECT"],
        [("INSERT", ["contribution_release_id", "tenant_id"])]
    ),
    cell!(
        "control.anonymous_source_lineage",
        "role_maintenance",
        ["SELECT"]
    ),
    cell!(
        "staging.sanitized_public_candidates",
        "role_private_worker",
        ["SELECT"],
        [(
            "INSERT",
            [
                "tenant_id",
                "anonymous_source_id",
                "sanitized_content",
                "content_sha256",
                "policy_version",
                "policy_digest",
                "assessment_outcome",
                "assessment_digest",
                "envelope_sha256"
            ]
        )]
    ),
    cell!(
        "staging.sanitized_public_candidates",
        "role_public_worker",
        []
    ),
    cell!(
        "staging.sanitized_public_candidates",
        "role_maintenance",
        ["SELECT"]
    ),
    // Phase9 0121: only private USER_REASONING storage retains probe/coverage/assessment
    // bindings. Public workers get neither table access nor a column-level escape hatch.
    cell!(
        "staging.contribution_candidate_phase9_assessments",
        "role_private_worker",
        ["SELECT", "INSERT"]
    ),
    cell!(
        "staging.contribution_candidate_phase9_assessments",
        "role_public_worker",
        []
    ),
    cell!(
        "public.anonymous_source_lifecycle_events",
        "role_gateway",
        ["SELECT"]
    ),
    cell!(
        "public.anonymous_source_lifecycle_events",
        "role_public_worker",
        ["SELECT"]
    ),
    cell!(
        "public.anonymous_source_lifecycle_events",
        "role_retrieval_worker",
        ["SELECT"]
    ),
    cell!(
        "public.anonymous_source_lifecycle_events",
        "role_maintenance",
        ["SELECT"]
    ),
    cell!(
        "public.current_anonymous_source_objects",
        "role_gateway",
        ["SELECT"]
    ),
    cell!(
        "public.current_anonymous_source_objects",
        "role_public_worker",
        ["SELECT"]
    ),
    cell!(
        "public.current_anonymous_source_objects",
        "role_retrieval_worker",
        ["SELECT"]
    ),
    cell!(
        "public.current_anonymous_source_objects",
        "role_maintenance",
        ["SELECT"]
    ),
    // 0122: an immutable random-pair revoke fence is readable by public-serving paths but
    // carries neither tenant, release nor contributor identity. The authority journal itself
    // is definer-only and explicitly suppresses schema-default grants below.
    cell!(
        "ops.anonymous_public_revocations",
        "role_gateway",
        ["SELECT"]
    ),
    cell!(
        "ops.anonymous_public_revocations",
        "role_private_worker",
        []
    ),
    cell!(
        "ops.anonymous_public_revocations",
        "role_consolidation_worker",
        []
    ),
    cell!(
        "ops.anonymous_public_revocations",
        "role_public_worker",
        ["SELECT"]
    ),
    cell!(
        "ops.anonymous_public_revocations",
        "role_retrieval_worker",
        ["SELECT"]
    ),
    cell!(
        "ops.anonymous_public_revocations",
        "role_maintenance",
        ["SELECT"]
    ),
    // 0131 R4 execution authority: direct reads only for the private worker; every
    // other non-owner cell is deliberately empty and writes stay typed-definer-only.
    cell!(
        "private.contribution_executions",
        "role_private_worker",
        ["SELECT"]
    ),
    cell!(
        "private.contribution_execution_sources",
        "role_private_worker",
        ["SELECT"]
    ),
    cell!(
        "ops.contribution_execution_job_links",
        "role_private_worker",
        ["SELECT"]
    ),
];

/// Named §6.2.2 tables whose non-owner cells are all deliberately empty. Their only public
/// mutation surface is a narrowly granted SECURITY DEFINER function, so they still must bypass
/// the wider `ops.*` domain default even though there is no non-empty [`Cell`] to carry the name.
const NAMED_NO_NON_OWNER_GRANTS: &[&str] = &[
    // 0130 R3 health truth is append-only and resolver-only.  These entries must override
    // the `ops.*` domain default completely: no worker, admin, or maintenance pool may read
    // or write observations directly.
    "ops.reasoning_provider_health_observations",
    "ops.reasoning_account_health_observations",
    "ops.retrieval_provider_budget_reservations",
    "ops.retrieval_provider_budget_allocations",
    "public.anonymous_source_authority_events",
    "ops.public_anonymous_dispatches",
    // 0128 R1 route foundation is intentionally inert: only role_migration_owner may
    // access the control plane until the later R2/R3 activation gates grant a narrow read path.
    "control.processor_models",
    "control.provider_accounts",
    "control.provider_endpoints",
    "control.provider_billing_accounts",
    "control.provider_billing_instruments",
    "control.reasoning_profiles",
    "control.reasoning_route_policies",
    "control.reasoning_route_candidates",
    "control.reasoning_route_bindings",
    "control.reasoning_credential_bindings",
    // 0129 R2 receipts remain migration-owner-only.  The explicit bootstrap procedure
    // has no runtime grant, so these rows cannot become a second routing authority.
    "control.reasoning_route_profile_receipts",
    "control.reasoning_route_domain_receipts",
    // 0135 receipt predicate SSOT is readable only through owner-rights consumers.
    "public._legacy_receipt_match_basis",
];

/// The §6.2.2 column set — "T" in the §48.2 "表集合派生" check's `S \ T == ∅`. Single
/// source shared by [`check_grant_equality`] (which tables get matrix treatment vs.
/// domain-default treatment) and [`check_table_set_derivation`] (what S is compared
/// against) — never hand-recounted a second time (§6.2.2: "S 不再手抄张数").
fn named_tables() -> BTreeSet<&'static str> {
    MATRIX
        .iter()
        .chain(ADDITIVE_SEAM_MATRIX.iter())
        .map(|cell| cell.table)
        .chain(NAMED_NO_NON_OWNER_GRANTS.iter().copied())
        .collect()
}

fn spec_named_tables() -> BTreeSet<&'static str> {
    named_tables()
}

// ============================================================================
// §6.2.1 — domain-default matrix (the "未逐表列出的表，一律按本表的域默认执行" half of
// §48.2's grant enumeration — see [`check_domain_default_grants`]). `R` = SELECT,
// `W` = INSERT + UPDATE, `—` = nothing. `role_migration_owner`'s `owner` row is not
// modeled here — same reasoning as [`MATRIX`], checked via table ownership instead.
// ============================================================================

const DEFAULT_R: &[&str] = &["SELECT"];
const DEFAULT_W: &[&str] = &["INSERT", "UPDATE"];
const DEFAULT_RW: &[&str] = &["SELECT", "INSERT", "UPDATE"];
const DEFAULT_NONE: &[&str] = &[];

/// One row of §6.2.1's table, verb sets in [`SCHEMAS`] column order
/// (control · private · staging · public · projection · coord · ops).
const DOMAIN_DEFAULT: &[(&str, [&[&str]; 7])] = &[
    ("role_admin", [DEFAULT_NONE; 7]),
    (
        "role_gateway",
        [
            DEFAULT_R,
            DEFAULT_RW,
            DEFAULT_NONE,
            DEFAULT_R,
            DEFAULT_R,
            DEFAULT_RW,
            DEFAULT_RW,
        ],
    ),
    (
        "role_private_worker",
        [
            DEFAULT_R,
            DEFAULT_RW,
            DEFAULT_W,
            DEFAULT_NONE,
            DEFAULT_NONE,
            DEFAULT_R,
            DEFAULT_RW,
        ],
    ),
    (
        "role_consolidation_worker",
        [
            DEFAULT_R,
            DEFAULT_R,
            DEFAULT_NONE,
            DEFAULT_NONE,
            DEFAULT_NONE,
            DEFAULT_R,
            DEFAULT_R,
        ],
    ),
    (
        "role_public_worker",
        [
            DEFAULT_R,
            DEFAULT_NONE,
            DEFAULT_R,
            DEFAULT_RW,
            DEFAULT_NONE,
            DEFAULT_R,
            DEFAULT_RW,
        ],
    ),
    (
        "role_retrieval_worker",
        [
            DEFAULT_R,
            DEFAULT_R,
            DEFAULT_NONE,
            DEFAULT_R,
            DEFAULT_RW,
            DEFAULT_R,
            DEFAULT_RW,
        ],
    ),
    (
        "role_batch_issuer",
        [
            DEFAULT_NONE,
            DEFAULT_NONE,
            DEFAULT_NONE,
            DEFAULT_NONE,
            DEFAULT_NONE,
            DEFAULT_NONE,
            DEFAULT_NONE,
        ],
    ),
    (
        "role_maintenance",
        [
            DEFAULT_R, DEFAULT_R, DEFAULT_R, DEFAULT_R, DEFAULT_R, DEFAULT_R, DEFAULT_R,
        ],
    ),
];

// ============================================================================
// Item 2 — §48.2 "表集合派生" (S \ T == ∅). Pure spec-text scan, no DB required; this
// is the check's own "承重条" framing — it must not depend on live-DB reachability to
// still catch a doc-only regression (new prose/SQL naming a table that never made it
// into §6.2.2).
// ============================================================================

/// §48.2 S_write: "全文任一 SQL 代码块里作为 INSERT INTO / UPDATE / DELETE FROM 目标
/// 出现的 `<schema.table>`；未带 schema 限定的按 §48 canonical 名归一（当前唯一一处：
/// outbox_event -> ops.outbox）". Scans every ```sql fenced block for those three verbs'
/// targets.
fn extract_s_write(spec_text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut rest = spec_text;
    while let Some(start) = rest.find("```sql\n") {
        let body_start = start + "```sql\n".len();
        let Some(end_rel) = rest[body_start..].find("```") else {
            break;
        };
        let block = &rest[body_start..body_start + end_rel];
        for target in extract_write_targets(block) {
            out.insert(canonicalize_table_name(&target));
        }
        rest = &rest[body_start + end_rel + 3..];
    }
    out
}

/// One fenced block's INSERT INTO / UPDATE / DELETE FROM targets. Skips `FOR UPDATE`
/// (as in `SELECT ... FOR UPDATE SKIP LOCKED`, §61) — that's a row lock clause, not a
/// write target, and a naive `UPDATE\s+(\w+)` scan would otherwise capture `SKIP` as a
/// fake table name.
fn extract_write_targets(sql_block: &str) -> Vec<String> {
    let upper = sql_block.to_ascii_uppercase();
    let bytes = upper.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut out = Vec::new();
    for verb in ["INSERT INTO", "DELETE FROM"] {
        let mut from = 0usize;
        while let Some(rel) = upper[from..].find(verb) {
            let idx = from + rel;
            let boundary_ok = idx == 0 || !is_ident(bytes[idx - 1]);
            if boundary_ok && let Some(name) = next_identifier(sql_block, idx + verb.len()) {
                out.push(name);
            }
            from = idx + verb.len();
        }
    }
    // `UPDATE <target>`, excluding both a leading-boundary false match (e.g.
    // `updated_at` containing `UPDATE` as a prefix) and a `FOR [NO KEY] UPDATE` row-lock
    // clause (`SELECT ... FOR UPDATE SKIP LOCKED`, §61 — a lock hint, not a target).
    // `FOR UPDATE OF <table>` is also excluded here since `UPDATE` there is still
    // directly preceded by `FOR `; the `OF <table>` tail is never itself an `UPDATE`
    // match. ponytail: single-space-only lookback, widen to a real token walk if a
    // multi-space/newline `FOR\n  UPDATE` variant ever shows up in the spec.
    let mut from = 0usize;
    while let Some(rel) = upper[from..].find("UPDATE") {
        let idx = from + rel;
        let boundary_before_ok = idx == 0 || !is_ident(bytes[idx - 1]);
        // Byte-slice comparison, not `&str` slicing: `upper` is an ASCII-uppercased copy
        // that leaves non-ASCII bytes (e.g. Chinese prose sharing the same SQL block)
        // untouched, so a fixed-width lookback can land mid-character. Slicing `&str` at
        // a non-boundary panics; slicing `&[u8]` never does.
        let preceded_by_for = (idx >= 4 && &bytes[idx - 4..idx] == b"FOR ")
            || (idx >= 11 && &bytes[idx - 11..idx] == b"FOR NO KEY ");
        let boundary_after_ok = idx + 6 >= bytes.len() || !is_ident(bytes[idx + 6]);
        if boundary_before_ok
            && boundary_after_ok
            && !preceded_by_for
            && let Some(name) = next_identifier(sql_block, idx + "UPDATE".len())
        {
            out.push(name);
        }
        from = idx + "UPDATE".len();
    }
    out
}

/// First `schema.table` or bare identifier token after `start` (skipping whitespace).
fn next_identifier(source: &str, start: usize) -> Option<String> {
    let bytes = source.as_bytes();
    let mut i = start;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let begin = i;
    while i < bytes.len()
        && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'.')
    {
        i += 1;
    }
    if i == begin {
        return None;
    }
    Some(source[begin..i].to_string())
}

/// §48 canonical-name normalization. The spec names exactly one live case
/// (`outbox_event -> ops.outbox`); anything already schema-qualified passes through.
fn canonicalize_table_name(name: &str) -> String {
    if name == "outbox_event" {
        "ops.outbox".to_string()
    } else {
        name.to_string()
    }
}

/// §48.2 S_grant: "全文任一处「<某角色 或 runtime role> 对 <schema.table> 有 / 无 /
/// 只有 <SQL 动词>」形式的断言所点名的表". Prose parsing over ~15k lines can't recover
/// every phrasing losslessly (tried; see below), so this scans conservatively: a
/// backtick-quoted `schema.table`, a role name (or "runtime role"), and an explicit
/// SQL-privilege verb co-occurring in the same paragraph/table-row.
///
/// §6.2.1/§6.2.2 themselves are excluded from the scan — they *are* T, so scanning them
/// as a source of S would make `S \ T` vacuously empty and blind to the very drift this
/// check exists to catch (§6.2.2's own worked example: deleting `stream_checkpoints`'s
/// column there must still leave it derivable from elsewhere in the doc).
///
/// ponytail: this under-approximates recall (see `check_table_set_derivation`'s doc for
/// which of the Phase9 named tables it currently can't independently re-derive) but has zero
/// false positives against the current frozen spec — a heuristic with lower recall and
/// no false positives is the safe failure mode for a gate (misses some drift now; never
/// cries wolf on a clean spec). Upgrade path: widen the verb/role vocabulary or add
/// sentence-boundary parsing if a real regression in one of the currently-missed tables
/// slips through undetected.
fn extract_s_grant(spec_text: &str) -> BTreeSet<String> {
    const ROLE_TOKENS: &[&str] = &[
        "role_gateway",
        "role_private_worker",
        "role_consolidation_worker",
        "role_public_worker",
        "role_retrieval_worker",
        "role_batch_issuer",
        "role_maintenance",
        "role_migration_owner",
        "role_admin",
        "runtime role",
    ];
    const VERB_TOKENS: &[&str] = &[
        "DELETE",
        "TRUNCATE",
        "INSERT",
        "UPDATE",
        "SELECT",
        "GRANT",
        "REVOKE",
        "BYPASSRLS",
    ];

    let scan_text = exclude_section(spec_text, "### 6.2.1", "### 6.2.3");

    let mut out = BTreeSet::new();
    for unit in scan_units(&scan_text) {
        let prose = strip_fenced_code(&unit);
        let has_role = ROLE_TOKENS.iter().any(|r| prose.contains(r));
        let has_verb = VERB_TOKENS.iter().any(|v| prose.contains(v));
        if has_role && has_verb {
            for table in find_backtick_tables(&prose) {
                out.insert(table);
            }
        }
    }
    out
}

/// Cuts `[start_marker, end_marker)` out of `text` (used to drop §6.2.1..§6.2.2 — the
/// domain-default table and the grant matrix itself — from the S_grant source scan).
fn exclude_section(text: &str, start_marker: &str, end_marker: &str) -> String {
    let Some(start) = text.find(start_marker) else {
        return text.to_string();
    };
    let Some(end) = text.find(end_marker) else {
        return text.to_string();
    };
    format!("{}{}", &text[..start], &text[end..])
}

/// Splits text into scan units: each markdown table row (`| ... |`) is its own unit —
/// merging table rows into the surrounding blank-line-delimited paragraph mixes
/// unrelated rows' role/table/verb mentions together (verified empirically: without
/// this split, adjacent unrelated rows in the same markdown table produce false
/// positives). Everything else is a normal blank-line-delimited paragraph. Fenced code
/// blocks are kept intact as their own unit's text (stripped later by
/// [`strip_fenced_code`]) so a ``` inside one doesn't desync fence-tracking.
fn scan_units(text: &str) -> Vec<String> {
    let mut units = Vec::new();
    let mut cur = Vec::new();
    let mut in_fence = false;
    for line in text.lines() {
        if line.trim_start().starts_with("```") {
            in_fence = !in_fence;
            cur.push(line);
            continue;
        }
        if in_fence {
            cur.push(line);
            continue;
        }
        if line.trim_start().starts_with('|') {
            if !cur.is_empty() {
                units.push(cur.join("\n"));
                cur.clear();
            }
            units.push(line.to_string());
            continue;
        }
        if line.trim().is_empty() {
            if !cur.is_empty() {
                units.push(cur.join("\n"));
                cur.clear();
            }
        } else {
            cur.push(line);
        }
    }
    if !cur.is_empty() {
        units.push(cur.join("\n"));
    }
    units
}

fn strip_fenced_code(unit: &str) -> String {
    let mut out = String::new();
    let mut in_fence = false;
    for line in unit.lines() {
        if line.trim_start().starts_with("```") {
            in_fence = !in_fence;
            continue;
        }
        if !in_fence {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

/// Every `schema.table` substring inside `text` (not required to be the whole string),
/// schema restricted to [`SCHEMAS`] so plain code identifiers (`payload_sha256`, etc.)
/// never match. Shares [`next_identifier`]'s boundary logic with [`extract_write_targets`]
/// so `projection.stream_checkpoints` is captured out of a full statement like
/// `` `GRANT UPDATE ON projection.stream_checkpoints TO role_gateway` `` — §48.2's own
/// worked example (spec L8685) puts the whole GRANT clause in one backtick span, not just
/// the table name.
fn scan_qualified_tables(text: &str) -> Vec<String> {
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    for schema in SCHEMAS {
        let needle = format!("{schema}.");
        let mut from = 0usize;
        while let Some(rel) = text[from..].find(&needle) {
            let idx = from + rel;
            let boundary_ok = idx == 0 || !is_ident(bytes[idx - 1]);
            if boundary_ok && let Some(name) = next_identifier(text, idx) {
                let suffix = &text[idx + name.len()..];
                let is_function_token = suffix.trim_start().starts_with('(');
                if let Some((_, table)) = name.split_once('.')
                    && !table.is_empty()
                    && !is_function_token
                {
                    out.push(name);
                }
            }
            from = idx + needle.len();
        }
    }
    out
}

/// Every backtick-quoted table reference in `text`: a schema-qualified substring found by
/// [`scan_qualified_tables`], or a bare short table name (e.g. `` `stream_log` ``,
/// `` `ingest_tickets` `` — §48.2's own text uses both forms) completed against
/// [`named_tables`]'s short-name → qualified-name map. The completion is restricted to
/// the Phase9 §6.2.2-named tables' own (unambiguous) short names, not a free-text guess — a
/// bare identifier that isn't one of those 14 names is left alone.
fn find_backtick_tables(text: &str) -> Vec<String> {
    let short_names: BTreeMap<&str, &str> = named_tables()
        .into_iter()
        .map(|full| (full.rsplit('.').next().unwrap_or(full), full))
        .collect();
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'`'
            && let Some(close_rel) = text[i + 1..].find('`')
        {
            let inner = &text[i + 1..i + 1 + close_rel];
            out.extend(scan_qualified_tables(inner));
            if let Some(&full) = short_names.get(inner.trim()) {
                out.push(full.to_string());
            }
            i += 1 + close_rel + 1;
            continue;
        }
        i += 1;
    }
    out
}

/// The §6.2.2 markdown header row's own backtick-quoted table names — the real T that
/// [`check_table_set_derivation`] diffs S against (§48.2 blocker: T must come from the
/// spec text itself, not from re-reading the hand-transcribed [`MATRIX`], or deleting a
/// column from the doc has zero effect on the gate — spec's own "反向" 注错).
fn parse_matrix_header_tables(spec_text: &str) -> BTreeSet<String> {
    let Some(start) = spec_text.find("### 6.2.2") else {
        return BTreeSet::new();
    };
    let Some(header_line) = spec_text[start..]
        .lines()
        .find(|l| l.trim_start().starts_with("| role |"))
    else {
        return BTreeSet::new();
    };
    scan_qualified_tables(header_line).into_iter().collect()
}

/// Returns the first malformed §6.2.2 Markdown row.  The grant derivation only consumes
/// the header, but a short separator/role row makes the canonical matrix ambiguous to
/// readers and must therefore fail the same production gate.
fn matrix_row_width_error(spec_text: &str) -> Option<String> {
    let Some(start) = spec_text.find("### 6.2.2") else {
        return Some("§6.2.2 matrix heading is missing".to_string());
    };
    let matrix = &spec_text[start..];
    let Some(end) = matrix.find("### 6.2.3") else {
        return Some("§6.2.3 heading must follow the §6.2.2 matrix".to_string());
    };
    let rows: Vec<&str> = matrix[..end]
        .lines()
        .filter(|line| line.trim_start().starts_with('|'))
        .collect();
    let Some(header) = rows.first() else {
        return Some("§6.2.2 matrix header is missing".to_string());
    };
    let header_width = header.split('|').count();

    rows.iter().find_map(|row| {
        let width = row.split('|').count();
        (width != header_width).then(|| {
            format!("§6.2.2 matrix row has {width} cells; header has {header_width}: {row}")
        })
    })
}

/// The `S \ T == ∅` half of [`check_table_set_derivation`], factored out so tests can
/// exercise the S_write/S_grant scan against a synthetic one-table spec fixture without
/// also tripping the real-[`MATRIX`]-drift assertion (which always reads the real,
/// Phase9 [`MATRIX`] and would spuriously fail against a deliberately tiny fixture).
fn derivation_check_from_text(spec_text: &str) -> GateResult {
    let s_write = extract_s_write(spec_text);
    let s_grant = extract_s_grant(spec_text);
    let s: BTreeSet<String> = s_write.union(&s_grant).cloned().collect();
    let t = parse_matrix_header_tables(spec_text);
    derivation_result(&s, &t.iter().map(String::as_str).collect())
}

/// §48.2 "表集合派生": `S \ T == ∅` where `S = S_grant ∪ S_write` and `T` is parsed live
/// from the §6.2.2 markdown header row ([`parse_matrix_header_tables`]) — not
/// [`named_tables`]'s hardcoded [`MATRIX`], so a column dropped from the doc itself moves
/// T and can turn this red (spec's own "反向" 注错; a Rust-only `T` can never observe a
/// doc-only edit). A second assertion below catches the opposite drift: [`MATRIX`]'s own
/// table set silently diverging from the header it's supposed to transcribe. Needs no
/// DB — pure text — so it always reports `pass`/`fail`, never `not_applicable`.
pub fn check_table_set_derivation(spec_text: &str) -> GateResult {
    if let Some(detail) = matrix_row_width_error(spec_text) {
        return fail("表集合派生", detail);
    }

    let derivation = derivation_check_from_text(spec_text);
    if derivation.status != GateStatus::Pass {
        return derivation;
    }

    let t = parse_matrix_header_tables(spec_text);
    let matrix_t = spec_named_tables();
    let doc_t: BTreeSet<&str> = t.iter().map(String::as_str).collect();
    if matrix_t != doc_t {
        return fail(
            "表集合派生",
            format!(
                "MATRIX transcription {matrix_t:?} != §6.2.2 header row {doc_t:?} — the \
                 Rust matrix has drifted from the spec table it's supposed to copy"
            ),
        );
    }
    derivation
}

/// Pure `S \ T` verdict — factored out of [`check_table_set_derivation`] so tests can
/// exercise the diff logic itself against a synthetic `t`, without needing to mutate
/// the real hardcoded [`MATRIX`] (which [`named_tables`] always reads from) to simulate
/// §48.2's "反向" 注错 (a table dropped from §6.2.2 while still named elsewhere).
fn derivation_result(s: &BTreeSet<String>, t: &BTreeSet<&str>) -> GateResult {
    let diff: Vec<&String> = s
        .iter()
        .filter(|t_name| !t.contains(t_name.as_str()))
        .collect();
    if diff.is_empty() {
        pass(
            "表集合派生",
            format!(
                "S (S_grant ∪ S_write, {} tables) ⊆ §6.2.2 T ({} tables)",
                s.len(),
                t.len()
            ),
        )
    } else {
        fail(
            "表集合派生",
            format!(
                "S \\ T ≠ ∅: {:?} named by prose/SQL but absent from §6.2.2's grant matrix",
                diff
            ),
        )
    }
}

// ============================================================================
// DB connection
// ============================================================================

const DSN_ENV: &str = "HUMAUX_TEST_PG_DSN";

/// G80-26 (§48.2) is a Phase-1 must-pass gate (spec L9817) — §57.1 rule 2 reserves
/// `not_applicable` for "the tested object hasn't been delivered yet", and an unset env
/// var or an unreachable DB is neither of those, it's the gate failing to run at all.
/// The prior version reported this as `not_applicable`, which `report()` doesn't count
/// toward exit status — CI reads that as green, matching the `config-check` fix for the
/// identical SPEC_PATH-missing failure mode.
fn connect() -> Result<Client, GateResult> {
    let dsn = std::env::var(DSN_ENV)
        .map_err(|_| fail("db-connection", format!("env var {DSN_ENV} not set")))?;
    let client = Client::connect(&dsn, NoTls).map_err(|e| {
        fail(
            "db-connection",
            format!("cannot reach Postgres at ${DSN_ENV}: {e}"),
        )
    })?;
    check_superuser(client)
}

/// information_schema's grant/column views (`role_table_grants`, `column_privileges`,
/// `columns`) are themselves filtered by `pg_has_role(current_user, grantee/grantor,
/// 'USAGE')` — connected as a non-superuser, they can silently return an empty or
/// partial result set instead of an error, which every check above would read as "no
/// grants exist" (clean) rather than "I can't see the grants". Failing loud here, once,
/// beats every downstream check separately proving a false pass.
fn check_superuser(mut client: Client) -> Result<Client, GateResult> {
    match client.query_one(
        "SELECT rolsuper FROM pg_roles WHERE rolname = current_user",
        &[],
    ) {
        Ok(row) if row.get::<_, bool>(0) => Ok(client),
        Ok(_) => Err(fail(
            "db-connection",
            "connected user is not a superuser — information_schema grant views are \
             pg_has_role-filtered and would silently under-report",
        )),
        Err(e) => Err(fail(
            "db-connection",
            format!("query pg_roles for current_user failed: {e}"),
        )),
    }
}

/// Replays a `connect()` failure under every check's own name, so each of the six
/// §48.2/§62 checks still names itself while pointing at the same underlying cause.
fn fail_for(check: &str, conn_err: &GateResult) -> GateResult {
    fail(check, conn_err.detail.clone())
}

// ============================================================================
// Item 1 — §6.2.0 角色全集相等
// ============================================================================

struct RoleRow {
    rolname: String,
    rolsuper: bool,
    rolcanlogin: bool,
    rolbypassrls: bool,
}

fn fetch_roles(client: &mut impl GenericClient) -> Result<Vec<RoleRow>, postgres::Error> {
    client
        .query(
            "SELECT rolname, rolsuper, rolcanlogin, rolbypassrls FROM pg_roles",
            &[],
        )
        .map(|rows| {
            rows.iter()
                .map(|r| RoleRow {
                    rolname: r.get(0),
                    rolsuper: r.get(1),
                    rolcanlogin: r.get(2),
                    rolbypassrls: r.get(3),
                })
                .collect()
        })
}

/// §6.2.0: `pg_roles` restricted to `rolcanlogin AND NOT rolsuper` must equal the frozen
/// declared-name set exactly (extra/missing/renamed all red), and per §48.2's opening block
/// every role must independently be `NOT SUPERUSER` / `NOT BYPASSRLS`.
pub fn check_role_set_equality(client: &mut impl GenericClient) -> GateResult {
    let roles = match fetch_roles(client) {
        Ok(r) => r,
        Err(e) => return fail("角色全集相等", format!("query pg_roles failed: {e}")),
    };
    let by_name: std::collections::BTreeMap<&str, &RoleRow> =
        roles.iter().map(|r| (r.rolname.as_str(), r)).collect();

    let expected = frozen_role_set();
    let mut problems = Vec::new();

    for name in &expected {
        match by_name.get(name) {
            None => problems.push(format!("missing object: role {name} does not exist")),
            Some(r) => {
                if r.rolsuper {
                    problems.push(format!("{name} is SUPERUSER"));
                }
                if r.rolbypassrls {
                    problems.push(format!("{name} has BYPASSRLS"));
                }
                if !r.rolcanlogin {
                    problems.push(format!("{name} has rolcanlogin=false (§6.2.0 requires it)"));
                }
            }
        }
    }

    let actual: BTreeSet<&str> = roles
        .iter()
        .filter(|r| r.rolcanlogin && !r.rolsuper)
        .map(|r| r.rolname.as_str())
        .collect();
    let extra: Vec<&&str> = actual.iter().filter(|n| !expected.contains(*n)).collect();
    if !extra.is_empty() {
        problems.push(format!(
            "extra login role(s) beyond the frozen role set: {extra:?}"
        ));
    }

    if problems.is_empty() {
        pass(
            "角色全集相等",
            "pg_roles login∧¬superuser set == §6.2.0 frozen role set, all NOT SUPERUSER/NOBYPASSRLS",
        )
    } else {
        fail("角色全集相等", problems.join("; "))
    }
}

// ============================================================================
// Item 3 — §6.2.2 授权逐条相等
// ============================================================================

struct GrantRow {
    grantee: String,
    table_schema: String,
    table_name: String,
    privilege_type: String,
}

fn fetch_table_grants(client: &mut impl GenericClient) -> Result<Vec<GrantRow>, postgres::Error> {
    client
        .query(
            "SELECT grantee, table_schema, table_name, privilege_type FROM information_schema.role_table_grants",
            &[],
        )
        .map(|rows| {
            rows.iter()
                .map(|r| GrantRow {
                    grantee: r.get(0),
                    table_schema: r.get(1),
                    table_name: r.get(2),
                    privilege_type: r.get(3),
                })
                .collect()
        })
}

struct ColGrantRow {
    grantee: String,
    table_schema: String,
    table_name: String,
    column_name: String,
    privilege_type: String,
}

fn fetch_column_grants(
    client: &mut impl GenericClient,
) -> Result<Vec<ColGrantRow>, postgres::Error> {
    client
        .query(
            "SELECT grantee, table_schema, table_name, column_name, privilege_type FROM information_schema.column_privileges",
            &[],
        )
        .map(|rows| {
            rows.iter()
                .map(|r| ColGrantRow {
                    grantee: r.get(0),
                    table_schema: r.get(1),
                    table_name: r.get(2),
                    column_name: r.get(3),
                    privilege_type: r.get(4),
                })
                .collect()
        })
}

/// All §6.2.2 mismatches for one named table: ownership plus, per non-owner role, the
/// table-level and column-level verb comparison. Factored out of
/// [`check_grant_equality`] purely to keep that function short — same logic, same data.
fn grant_equality_mismatches_for_table(
    table: &str,
    table_grants: &[GrantRow],
    col_grants: &[ColGrantRow],
    owners: &BTreeMap<(String, String), String>,
    non_owner_roles: &[&str],
) -> Vec<String> {
    let (schema, tname) = table
        .split_once('.')
        .expect("named_tables() is schema-qualified");
    let mut mismatches = Vec::new();

    // §6.2.2's own row for role_migration_owner: `owner` on all named tables and views. Not a
    // MATRIX cell (owner isn't modeled as a grant), so check the shared pg_class owner instead.
    let actual_owner = owners.get(&(schema.to_string(), tname.to_string()));
    if actual_owner.map(String::as_str) != Some(OWNER_ROLE) {
        mismatches.push(format!(
            "{table}: expected owner {OWNER_ROLE:?}, actual {actual_owner:?}"
        ));
    }

    for &role in non_owner_roles {
        let cell = MATRIX
            .iter()
            .chain(ADDITIVE_SEAM_MATRIX.iter())
            .find(|c| c.table == table && c.role == role);
        let expected_table_verbs: BTreeSet<&str> = cell
            .map(|c| c.table_verbs.iter().copied().collect())
            .unwrap_or_default();
        let actual_table_verbs: BTreeSet<&str> = table_grants
            .iter()
            .filter(|g| g.grantee == role && g.table_schema == schema && g.table_name == tname)
            .map(|g| g.privilege_type.as_str())
            .collect();
        if expected_table_verbs != actual_table_verbs {
            mismatches.push(format!(
                "{table}/{role}: table-level expected {expected_table_verbs:?}, actual {actual_table_verbs:?}"
            ));
        }

        let expected_col_verbs: BTreeSet<(&str, &str)> = cell
            .map(|c| {
                c.col_verbs
                    .iter()
                    .flat_map(|(verb, cols)| cols.iter().map(move |c| (*verb, *c)))
                    .collect()
            })
            .unwrap_or_default();
        // information_schema.column_privileges expands a *table-level* GRANT to one row
        // per column too (PostgreSQL's own documented behavior, not a bug in the view) —
        // so a plain `actual == col_grants filtered by (grantee, table)` compare makes
        // every table-level verb look like it's also a column-level grant on every
        // column, and the check is permanently red the moment any table-level verb
        // exists (which is every non-empty cell). Filtering out rows whose
        // privilege_type is already accounted for at the table level recovers the *true*
        // column-level grants: the ones a whole-table GRANT didn't already imply.
        let actual_col_verbs: BTreeSet<(&str, &str)> = col_grants
            .iter()
            .filter(|g| g.grantee == role && g.table_schema == schema && g.table_name == tname)
            .filter(|g| !actual_table_verbs.contains(g.privilege_type.as_str()))
            .map(|g| (g.privilege_type.as_str(), g.column_name.as_str()))
            .collect();
        if expected_col_verbs != actual_col_verbs {
            mismatches.push(format!(
                "{table}/{role}: column-level expected {expected_col_verbs:?}, actual {actual_col_verbs:?}"
            ));
        }
    }
    mismatches
}

/// §6.2.2 "授权逐条相等": every MATRIX cell compared against live grants, in both
/// directions (missing == red per spec's explicit "少授也是偏离，不是更安全所以放行").
/// A table from the 14 §6.2.2-named ones (`T`) that doesn't exist yet is reported as
/// `not_applicable` naming it — but §57.1 rule 2 confines that to the missing object
/// itself: the other 13 (already-migrated) tables still get compared and can still fail.
/// A missing table used to make the whole check `not_applicable` (silently skipping 13
/// real comparisons because a 14th table was renamed or not yet migrated).
pub fn check_grant_equality(client: &mut impl GenericClient) -> GateResult {
    let t = named_tables();
    let existing_tables: BTreeSet<String> = match client.query(
        "SELECT table_schema || '.' || table_name FROM information_schema.tables \
         WHERE table_type IN ('BASE TABLE','VIEW') \
           AND table_schema || '.' || table_name = ANY($1)",
        &[&t.iter().map(|s| s.to_string()).collect::<Vec<_>>()],
    ) {
        Ok(rows) => rows.iter().map(|r| r.get::<_, String>(0)).collect(),
        Err(e) => {
            return fail(
                "授权逐条相等",
                format!("query information_schema.tables failed: {e}"),
            );
        }
    };
    let missing_tables: Vec<&&str> = t
        .iter()
        .filter(|name| !existing_tables.contains(**name))
        .collect();

    let table_grants = match fetch_table_grants(client) {
        Ok(g) => g,
        Err(e) => {
            return fail(
                "授权逐条相等",
                format!("query role_table_grants failed: {e}"),
            );
        }
    };
    let col_grants = match fetch_column_grants(client) {
        Ok(g) => g,
        Err(e) => {
            return fail(
                "授权逐条相等",
                format!("query column_privileges failed: {e}"),
            );
        }
    };
    let owners: BTreeMap<(String, String), String> = match client.query(
        "SELECT n.nspname,c.relname,r.rolname \
         FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace \
         JOIN pg_roles r ON r.oid=c.relowner WHERE c.relkind IN ('r','p','v','m','f')",
        &[],
    ) {
        Ok(rows) => rows
            .iter()
            .map(|r| ((r.get(0), r.get(1)), r.get(2)))
            .collect(),
        Err(e) => return fail("授权逐条相等", format!("query pg_tables failed: {e}")),
    };

    let non_owner_roles: Vec<&str> = RUNTIME_ROLES
        .iter()
        .chain(["role_batch_issuer", "role_maintenance", "role_admin"].iter())
        .copied()
        .collect();
    let mismatches: Vec<String> = t
        .iter()
        .filter(|table| existing_tables.contains(**table))
        .flat_map(|&table| {
            grant_equality_mismatches_for_table(
                table,
                &table_grants,
                &col_grants,
                &owners,
                &non_owner_roles,
            )
        })
        .collect();

    if !mismatches.is_empty() {
        return fail("授权逐条相等", mismatches.join("; "));
    }
    if !missing_tables.is_empty() {
        return not_applicable(
            "授权逐条相等",
            format!(
                "missing object: relation(s) {missing_tables:?} from §6.2.2 do not exist yet \
                 ({} of {} named relations compared clean)",
                t.len() - missing_tables.len(),
                t.len()
            ),
        );
    }
    pass(
        "授权逐条相等",
        format!(
            "{} named relations × {} non-owner roles all match §6.2.2, all owned by {OWNER_ROLE}",
            t.len(),
            non_owner_roles.len()
        ),
    )
}

// ============================================================================
// §6.2.1 域级默认授权 — tables §6.2.2 doesn't name (§48.2: "枚举面 = 全部 schema 的全部
// 表", not just the Phase9 point-named ones).
// ============================================================================

/// §6.2.1: every non-owner role's table-level grants on every BASE TABLE **not** named
/// by §6.2.2 must equal that role's domain default for the table's schema
/// ([`DOMAIN_DEFAULT`]). Without this, an over-grant or under-grant on any of the tables
/// outside the Phase9 matrix — the majority of the schema — has no check at all;
/// [`check_grant_equality`] only ever looks at `T`.
pub fn check_domain_default_grants(client: &mut impl GenericClient) -> GateResult {
    let t = named_tables();
    let all_tables: Vec<(String, String, String)> = match client.query(
        "SELECT schemaname, tablename, tableowner FROM pg_tables \
         WHERE schemaname = ANY($1)",
        &[&SCHEMAS.to_vec()],
    ) {
        Ok(rows) => rows
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect(),
        Err(e) => {
            return fail("域默认授权", format!("query pg_tables failed: {e}"));
        }
    };
    let table_grants = match fetch_table_grants(client) {
        Ok(g) => g,
        Err(e) => return fail("域默认授权", format!("query role_table_grants failed: {e}")),
    };

    let mut checked = 0usize;
    let mut problems = Vec::new();
    for (schema, tname, owner) in &all_tables {
        let full = format!("{schema}.{tname}");
        if owner != OWNER_ROLE {
            problems.push(format!(
                "{full}: Canonical owner expected {OWNER_ROLE:?}, actual {owner:?}"
            ));
        }
        if matches!(
            full.as_str(),
            "private.continuity_projects"
                | "private.continuity_facet_versions"
                | "private.continuity_facet_memory_links"
                | "private.continuity_facet_evidence_links"
                | "private.continuity_facet_slots"
        ) {
            // §25.3.1 W1 freezes zero direct runtime privileges; the focused W1 catalog
            // check below owns this explicit exception to §6.2.1's generic domain default.
            continue;
        }
        if t.contains(full.as_str()) {
            continue; // §6.2.2 overrides the domain default for a named table entirely.
        }
        let Some(schema_idx) = SCHEMAS.iter().position(|s| s == schema) else {
            continue;
        };
        checked += 1;
        for (role, verbs_by_schema) in DOMAIN_DEFAULT {
            let expected: BTreeSet<&str> = verbs_by_schema[schema_idx].iter().copied().collect();
            let actual: BTreeSet<&str> = table_grants
                .iter()
                .filter(|g| {
                    g.grantee == *role && &g.table_schema == schema && &g.table_name == tname
                })
                .map(|g| g.privilege_type.as_str())
                .collect();
            if expected != actual {
                problems.push(format!(
                    "{full}/{role}: §6.2.1 domain default expected {expected:?}, actual {actual:?}"
                ));
            }
        }
    }

    if checked == 0 {
        return not_applicable(
            "域默认授权",
            "missing object: no BASE TABLE outside §6.2.2's named tables exists yet",
        );
    }
    if problems.is_empty() {
        pass(
            "域默认授权",
            format!(
                "{checked} non-named table(s) × 7 non-owner roles match §6.2.1; all {} in-scope table(s) owned by {OWNER_ROLE}",
                all_tables.len()
            ),
        )
    } else {
        fail("域默认授权", problems.join("; "))
    }
}

// ============================================================================
// Item 4 — §6.2.1 全域禁动词
// ============================================================================

/// §6.2.1 "全域硬约束，无例外": the non-owner roles ([`NON_OWNER_ROLES`]) get no
/// DELETE/TRUNCATE anywhere (`role_table_grants`), no CREATE on any schema
/// (`has_schema_privilege`), and own nothing (`pg_tables.tableowner`).
pub fn check_forbidden_verbs(client: &mut impl GenericClient) -> GateResult {
    let table_grants = match fetch_table_grants(client) {
        Ok(g) => g,
        Err(e) => return fail("全域禁动词", format!("query role_table_grants failed: {e}")),
    };
    let mut problems = Vec::new();

    for grant in &table_grants {
        if NON_OWNER_ROLES.contains(&grant.grantee.as_str())
            && matches!(grant.privilege_type.as_str(), "DELETE" | "TRUNCATE")
        {
            problems.push(format!(
                "{}.{}: {} holds {} (forbidden for non-owner roles)",
                grant.table_schema, grant.table_name, grant.grantee, grant.privilege_type
            ));
        }
    }

    let schemas: Vec<String> =
        match client.query("SELECT nspname FROM pg_namespace WHERE nspname !~ '^pg_' AND nspname != 'information_schema'", &[]) {
            Ok(rows) => rows.iter().map(|r| r.get(0)).collect(),
            Err(e) => return fail("全域禁动词", format!("query pg_namespace failed: {e}")),
        };
    for role in NON_OWNER_ROLES {
        for schema in &schemas {
            match client.query_one(
                "SELECT has_schema_privilege($1, $2, 'CREATE')",
                &[role, schema],
            ) {
                Ok(row) => {
                    let can_create: bool = row.get(0);
                    if can_create {
                        problems.push(format!(
                            "{role} has CREATE on schema {schema} (DDL forbidden)"
                        ));
                    }
                }
                // Role doesn't exist — role-set-equality already reports this; don't double-count here.
                Err(_) => continue,
            }
        }
    }

    let owned: Vec<(String, String, String)> = match client.query(
        "SELECT schemaname, tablename, tableowner FROM pg_tables",
        &[],
    ) {
        Ok(rows) => rows
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect(),
        Err(e) => return fail("全域禁动词", format!("query pg_tables failed: {e}")),
    };
    for (schema, table, owner) in &owned {
        if NON_OWNER_ROLES.contains(&owner.as_str()) {
            problems.push(format!(
                "{schema}.{table} is owned by non-owner role {owner}"
            ));
        }
    }

    if problems.is_empty() {
        pass(
            "全域禁动词",
            "no DELETE/TRUNCATE/CREATE/ownership held by any of the 7 non-owner roles",
        )
    } else {
        fail("全域禁动词", problems.join("; "))
    }
}

// ============================================================================
// Item 5 — §48.2 发票权唯一
// ============================================================================

/// §48.2 "发票权唯一": `grantee`s holding `INSERT` on `private.ingest_tickets`,
/// excluding [`OWNER_ROLE`] (owner implicitly holds every privilege — not excluding it
/// makes this query true on a clean DB, spec's own "旧写法...一道恒红闸"), must be
/// exactly `{role_batch_issuer}`.
/// §48.2 "发票权唯一": table missing entirely is the only legitimate `not_applicable`
/// (§57.1 rule 2 — object not yet delivered). Once `private.ingest_tickets` exists, an
/// empty non-owner grantee set means the invoice privilege was revoked from everyone —
/// spec's own "少授也是偏离，不是更安全所以放行" (§48.2) — and must fail, not disappear
/// as `not_applicable`. The prior version couldn't tell these two empty-set causes apart.
pub fn check_invoice_privilege_unique(client: &mut impl GenericClient) -> GateResult {
    let table_exists: bool = match client.query_one(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
         WHERE table_type = 'BASE TABLE' AND table_schema = 'private' AND table_name = 'ingest_tickets')",
        &[],
    ) {
        Ok(row) => row.get(0),
        Err(e) => {
            return fail(
                "发票权唯一",
                format!("query information_schema.tables failed: {e}"),
            );
        }
    };
    if !table_exists {
        return not_applicable(
            "发票权唯一",
            "missing object: private.ingest_tickets does not exist yet",
        );
    }

    let rows = match client.query(
        "SELECT grantee FROM information_schema.role_table_grants \
         WHERE table_schema = 'private' AND table_name = 'ingest_tickets' \
           AND privilege_type = 'INSERT' AND grantee <> $1",
        &[&OWNER_ROLE],
    ) {
        Ok(r) => r,
        Err(e) => return fail("发票权唯一", format!("query role_table_grants failed: {e}")),
    };
    let grantees: BTreeSet<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    if grantees == BTreeSet::from(["role_batch_issuer".to_string()]) {
        pass(
            "发票权唯一",
            "INSERT ON private.ingest_tickets (excl. owner) == {role_batch_issuer}",
        )
    } else {
        fail(
            "发票权唯一",
            format!(
                "INSERT ON private.ingest_tickets (excl. owner) grantees == {grantees:?}, want exactly {{role_batch_issuer}}"
            ),
        )
    }
}

// ============================================================================
// Item 6 — §62 / §48.2 RLS four-item enumeration
// ============================================================================

struct TenantTable {
    schema: String,
    table: String,
    rowsecurity: bool,
    forcerowsecurity: bool,
    owner: String,
}

/// Every table (any schema) carrying a `tenant_id` column, with its RLS flags and
/// owner — the enumeration domain §48.2/§62 both point at ("枚举所有带 tenant_id 的
/// tenant-scoped 表"). `relkind IN ('r','p')` also picks up range-partitioned parents
/// (§48.1 candidates control.audit_events / private.events / private.messages / etc.) —
/// a parent's `relrowsecurity` does not propagate to its partitions (PostgreSQL does not
/// inherit RLS across a partition boundary), so a partitioned table needs its own row in
/// this enumeration precisely because leaf partitions can't be trusted to inherit it.
fn fetch_tenant_tables(
    client: &mut impl GenericClient,
) -> Result<Vec<TenantTable>, postgres::Error> {
    let rows = client.query(
        "SELECT c.relnamespace::regnamespace::text, c.relname, c.relrowsecurity, c.relforcerowsecurity, pg_get_userbyid(c.relowner) \
         FROM pg_class c \
         WHERE c.relkind IN ('r', 'p') \
           AND EXISTS ( \
             SELECT 1 FROM information_schema.columns col \
             WHERE col.table_schema = c.relnamespace::regnamespace::text \
               AND col.table_name = c.relname AND col.column_name = 'tenant_id' \
           )",
        &[],
    )?;
    Ok(rows
        .iter()
        .map(|r| TenantTable {
            schema: r.get(0),
            table: r.get(1),
            rowsecurity: r.get(2),
            forcerowsecurity: r.get(3),
            owner: r.get(4),
        })
        .collect())
}

/// §62's tenant-equality clause, in the exact form `pg_policies.qual`/`with_check`
/// deparses it — verified against migration 0012's literal SQL: PostgreSQL's ruleutils
/// adds the `::text` disambiguation cast to `current_setting`'s string-literal argument
/// (it's overloaded on arity) and wraps the comparison in parens. This is that canonical
/// deparsed form, not the raw source SQL — matching against the raw source would silently
/// never match anything live.
const TENANT_CLAUSE: &str = "(tenant_id = (current_setting('humaux.tenant_id'::text, true))::uuid)";

/// Migration 0031's hardened form of the same clause — the GUC read wrapped in
/// `NULLIF(.., '')` so a reverted-`SET LOCAL` session (empty-string GUC placeholder,
/// PG 18.6 observed) reads deterministically 0 rows instead of raising 22P02. Deparsed
/// form verified live against `pg_policies.qual` after 0031. A policy matches if it
/// carries either form: pre-0031 databases mid-migration keep the plain form, every
/// fully-migrated database carries this one.
const TENANT_CLAUSE_NULLIF: &str =
    "(tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid)";

/// Either accepted canonical tenant clause (see the two constants above).
fn contains_tenant_clause(expr: &str) -> bool {
    expr.contains(TENANT_CLAUSE) || expr.contains(TENANT_CLAUSE_NULLIF)
}

/// §48.2/§62's "matching tenant policy exists": a policy covering all four commands
/// (`cmd = 'ALL'`) whose USING clause **and** WITH CHECK clause both literally contain
/// [`TENANT_CLAUSE`] — not just a `%tenant_id%` substring test, which passes
/// `USING (true) WITH CHECK (tenant_id IS NOT NULL)` (full SELECT leak across tenants)
/// just as readily as the real template. Requiring the clause in *both* qual and
/// with_check (not either/or) is what catches a `USING(true)` SELECT leak specifically —
/// a table can compose the tenant clause with a further AND (private.evidence_objects /
/// private.memory_records' §6.1.1 visibility disjunction, migration 0012) and still match
/// via substring containment.
fn table_has_tenant_policy(
    client: &mut impl GenericClient,
    schema: &str,
    table: &str,
) -> Result<bool, postgres::Error> {
    let rows = client.query(
        "SELECT cmd, qual, with_check FROM pg_policies WHERE schemaname = $1 AND tablename = $2",
        &[&schema, &table],
    )?;
    Ok(rows.iter().any(|row| {
        let cmd: String = row.get(0);
        let qual: Option<String> = row.get(1);
        let with_check: Option<String> = row.get(2);
        cmd == "ALL"
            && qual.as_deref().is_some_and(contains_tenant_clause)
            && with_check.as_deref().is_some_and(contains_tenant_clause)
    }))
}

/// §48.2/§62: for every table carrying `tenant_id`, assert RLS enabled + FORCE RLS +
/// a `tenant_id`-referencing policy exists + no runtime role owns it. Missing the
/// object entirely (no tenant-scoped table exists yet) ⇒ `not_applicable`.
pub fn check_rls_four_item(client: &mut impl GenericClient) -> GateResult {
    let tables = match fetch_tenant_tables(client) {
        Ok(t) => t,
        Err(e) => {
            return fail(
                "RLS 四项",
                format!("query pg_class/information_schema.columns failed: {e}"),
            );
        }
    };
    if tables.is_empty() {
        return not_applicable(
            "RLS 四项",
            "missing object: no table with a tenant_id column exists yet",
        );
    }

    let mut problems = Vec::new();
    for t in &tables {
        let full = format!("{}.{}", t.schema, t.table);
        // R3 health observations are intentionally owner-quarantined histories, not tenant
        // visibility tables. Their `USING/WITH CHECK (true)` owner policy is checked exactly
        // by check_r3_health_observation_boundary below; applying the tenant-template here
        // would reject the frozen boundary merely because account health carries tenant_id.
        if matches!(
            full.as_str(),
            "ops.reasoning_provider_health_observations"
                | "ops.reasoning_account_health_observations"
        ) {
            continue;
        }
        if !t.rowsecurity {
            problems.push(format!("{full}: RLS not enabled"));
        }
        if !t.forcerowsecurity {
            problems.push(format!("{full}: FORCE RLS not enabled"));
        }
        match table_has_tenant_policy(client, &t.schema, &t.table) {
            Ok(true) => {}
            Ok(false) => problems.push(format!("{full}: no tenant_id policy found")),
            Err(e) => problems.push(format!("{full}: query pg_policies failed: {e}")),
        }
        if RUNTIME_ROLES.contains(&t.owner.as_str()) {
            problems.push(format!("{full}: owned by runtime role {}", t.owner));
        }
    }

    if problems.is_empty() {
        pass(
            "RLS 四项",
            format!(
                "{} tenant_id table(s): RLS+FORCE+policy+non-owner all hold",
                tables.len()
            ),
        )
    } else {
        fail("RLS 四项", problems.join("; "))
    }
}

// ============================================================================
// Entry point
// ============================================================================

/// §73 Admin-Plane leak scan (added after the P2 review found `control.support_access_requests`
/// leaking cross-tenant rows to `role_gateway`): any base table in `control`/`ops`/`private`
/// carrying a column whose name matches `%tenant_id%` — the owning `tenant_id` OR a reference
/// like `target_tenant_id` — that a runtime role can SELECT and that has row-security *disabled*
/// is a cross-tenant read waiting to happen. `check_rls_four_item` only enumerates the literal
/// `tenant_id` column, so a renamed reference column slipped past it (green gate, open leak);
/// this check closes that class. A table is clean if RLS is enabled+forced, OR no runtime role
/// can SELECT it. NULLIF-policy content is not inspected here — this is the coarse "is it even
/// guarded" gate; per-tenant correctness stays with `check_rls_four_item`.
pub fn check_admin_plane_no_leak(client: &mut impl GenericClient) -> GateResult {
    let rows = match client.query(
        "SELECT DISTINCT c.relnamespace::regnamespace::text AS sch, c.relname,                 c.relrowsecurity, c.relforcerowsecurity          FROM pg_class c          JOIN information_schema.columns col            ON col.table_schema = c.relnamespace::regnamespace::text           AND col.table_name = c.relname          WHERE c.relkind IN ('r','p')            AND c.relnamespace::regnamespace::text IN ('control','ops','private','staging','projection','coord')            AND col.column_name LIKE '%tenant_id%'",
        &[],
    ) {
        Ok(r) => r,
        Err(e) => return fail("Admin-Plane 防泄漏", format!("enumeration query failed: {e}")),
    };

    let mut problems = Vec::new();
    for row in &rows {
        let sch: String = row.get(0);
        let table: String = row.get(1);
        let rls: bool = row.get(2);
        let forced: bool = row.get(3);
        if rls && forced {
            continue; // guarded — per-tenant correctness is check_rls_four_item's job
        }
        // RLS off (or not forced): only a leak if a runtime role can actually SELECT it.
        let full = format!("{sch}.{table}");
        let can_select: bool = match client.query_one(
            "SELECT bool_or(has_table_privilege(r, $1, 'SELECT'))              FROM unnest($2::text[]) AS r",
            &[&full, &RUNTIME_ROLES.to_vec()],
        ) {
            Ok(r) => r.get(0),
            Err(e) => {
                problems.push(format!("{full}: privilege probe failed: {e}"));
                continue;
            }
        };
        if can_select {
            problems.push(format!(
                "{full}: has %tenant_id% column, SELECT-able by a runtime role, RLS not enabled+forced (rls={rls} forced={forced}) — cross-tenant leak class (§73 Admin/User plane 分离)"
            ));
        }
    }
    if problems.is_empty() {
        pass(
            "Admin-Plane 防泄漏",
            "no unguarded runtime-role-readable %tenant_id% table".to_string(),
        )
    } else {
        fail("Admin-Plane 防泄漏", problems.join("; "))
    }
}

/// R3's two health histories are deliberately outside the normal `ops.*` default grant.  The
/// provider table has no tenant_id, so §62's tenant-only enumeration cannot prove FORCE RLS for
/// it; keep this named-object check beside the matrix and resolver ACL checks instead.
pub fn check_r3_health_observation_boundary(client: &mut impl GenericClient) -> GateResult {
    const TABLES: [&str; 2] = [
        "reasoning_provider_health_observations",
        "reasoning_account_health_observations",
    ];
    let rows = match client.query(
        "SELECT c.relname, pg_get_userbyid(c.relowner), c.relrowsecurity, c.relforcerowsecurity \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 'ops' AND c.relkind IN ('r', 'p') AND c.relname = ANY($1)\
         ORDER BY c.relname",
        &[&&TABLES[..]],
    ) {
        Ok(rows) => rows,
        Err(e) => {
            return fail(
                "R3 health boundary",
                format!("query health tables failed: {e}"),
            );
        }
    };
    let mut problems = Vec::new();
    if rows.len() != TABLES.len() {
        problems.push(format!(
            "missing R3 health table(s): expected {TABLES:?}, found {}",
            rows.len()
        ));
    }
    for row in rows {
        let table: String = row.get(0);
        let owner: String = row.get(1);
        let rls: bool = row.get(2);
        let force: bool = row.get(3);
        if owner != OWNER_ROLE || !rls || !force {
            problems.push(format!(
                "ops.{table}: expected owner={OWNER_ROLE}, ENABLE+FORCE RLS; actual owner={owner}, rls={rls}, force={force}"
            ));
        }
    }
    for table in TABLES {
        let policies = match client.query(
            "SELECT cmd, roles, qual, with_check FROM pg_policies \
             WHERE schemaname = 'ops' AND tablename = $1",
            &[&table],
        ) {
            Ok(rows) => rows,
            Err(e) => {
                problems.push(format!(
                    "ops.{table}: query owner quarantine policy failed: {e}"
                ));
                continue;
            }
        };
        let owner_only = policies.iter().any(|row| {
            let cmd: String = row.get(0);
            let roles: Vec<String> = row.get(1);
            let qual: Option<String> = row.get(2);
            let with_check: Option<String> = row.get(3);
            cmd == "ALL"
                && roles.len() == 1
                && roles.first().is_some_and(|role| role == OWNER_ROLE)
                && qual.as_deref() == Some("true")
                && with_check.as_deref() == Some("true")
        });
        if !owner_only {
            problems.push(format!(
                "ops.{table}: requires owner-only ALL USING (true) WITH CHECK (true) policy"
            ));
        }
    }

    if let Err(result) = check_r3_health_resolver_contract(client, &mut problems) {
        return result;
    }
    if problems.is_empty() {
        pass(
            "R3 health boundary",
            "two owner-only FORCE RLS health tables; resolver is private-worker-only SECURITY DEFINER".to_string(),
        )
    } else {
        fail("R3 health boundary", problems.join("; "))
    }
}

fn check_r3_health_resolver_contract(
    client: &mut impl GenericClient,
    problems: &mut Vec<String>,
) -> Result<(), GateResult> {
    let function = match client.query_opt(
        "SELECT pg_get_userbyid(p.proowner), p.prosecdef, coalesce(p.proconfig, ARRAY[]::text[]) \
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
         WHERE n.nspname = 'control' \
           AND p.oid = to_regprocedure('control.resolve_user_reasoning_admission(uuid,bigint,uuid,text)')",
        &[],
    ) {
        Ok(Some(row)) => row,
        Ok(None) => return Err(fail("R3 health boundary", "missing control.resolve_user_reasoning_admission(uuid,bigint,uuid,text)")),
        Err(e) => return Err(fail("R3 health boundary", format!("query resolver failed: {e}"))),
    };
    let owner: String = function.get(0);
    let security_definer: bool = function.get(1);
    let config: Vec<String> = function.get(2);
    if owner != OWNER_ROLE
        || !security_definer
        || !config.iter().any(|v| v == "search_path=pg_catalog")
    {
        problems.push(format!(
            "resolver expected owner={OWNER_ROLE}, SECURITY DEFINER, search_path=pg_catalog; actual owner={owner}, security_definer={security_definer}, config={config:?}"
        ));
    }
    for role in NON_OWNER_ROLES {
        let allowed: bool = match client.query_one(
            "SELECT has_function_privilege($1::text, \
             'control.resolve_user_reasoning_admission(uuid,bigint,uuid,text)', 'EXECUTE')",
            &[role],
        ) {
            Ok(row) => row.get(0),
            Err(e) => {
                problems.push(format!("resolver privilege probe for {role} failed: {e}"));
                continue;
            }
        };
        let expected = *role == "role_private_worker";
        if allowed != expected {
            problems.push(format!(
                "resolver/{role}: expected EXECUTE={expected}, actual {allowed}"
            ));
        }
    }
    let public_execute_absent: bool = match client.query_one(
        "SELECT NOT EXISTS ( \
           SELECT 1 \
           FROM aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) x \
           WHERE x.grantee = 0 AND x.privilege_type = 'EXECUTE' \
         ) \
         FROM pg_proc p \
         JOIN pg_namespace n ON n.oid = p.pronamespace \
         WHERE n.nspname = 'control' \
           AND p.oid = to_regprocedure('control.resolve_user_reasoning_admission(uuid,bigint,uuid,text)')",
        &[],
    ) {
        Ok(row) => row.get(0),
        Err(e) => {
            problems.push(format!("resolver PUBLIC privilege probe failed: {e}"));
            false
        }
    };
    if !public_execute_absent {
        problems.push("resolver/PUBLIC: EXECUTE must be revoked".to_string());
    }
    Ok(())
}

/// R4-D0/0133: the worker surface is an exact, overload-safe allow-list. Table grants alone
/// cannot prove this boundary because mutations intentionally route through SECURITY DEFINER.
const R4_TYPED_FUNCTIONS: &[&str] = &[
    "private.enqueue_contribution_execution(uuid,uuid,uuid,uuid,uuid,uuid,text,bytea,uuid,uuid,bytea,uuid,bigint,jsonb,text,text,text,text,text,uuid,bigint,bigint,bigint,bytea,bytea,text[],uuid[],bytea[])",
    "private.reserve_contribution_a(uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint,jsonb)",
    "private.reserve_contribution_b(uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint,jsonb)",
    "private.complete_contribution_a_exact(uuid,uuid,uuid,uuid,bytea,uuid,text,bytea,uuid,integer,bytea,bytea,jsonb,bytea,text,text,text,uuid,text,integer)",
    "private.complete_contribution_b_exact(uuid,uuid,uuid,uuid,bytea,uuid,text,bytea,bytea,text,text,text,text,bytea,bytea,jsonb,bytea,text,text,text,uuid,text,integer)",
    "private.commit_contribution_candidate(uuid,uuid,uuid,text,integer)",
    "private.settle_contribution_terminal_job(uuid,uuid,uuid,text,integer)",
    "private.mark_contribution_reconciliation_required(uuid,uuid,uuid,text,integer)",
];

const R4_INTERNAL_FUNCTIONS: &[&str] = &[
    "private.require_contribution_execution_lease(uuid,uuid,uuid,text,integer)",
    "private.reserve_contribution_execution_call(uuid,uuid,uuid,text,integer,text,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint)",
    "private.settle_contribution_job_if_live(uuid,uuid,uuid,text,integer,text,text)",
    "private.reserve_contribution_a(uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint)",
    "private.reserve_contribution_b(uuid,uuid,uuid,text,integer,uuid,uuid,bytea,uuid,text,uuid,text,bytea,bigint)",
    "private.compute_contribution_source_backing_closure_v1(uuid,uuid,uuid,text[],uuid[],bytea[])",
    "private.contribution_execution_closure_seal_immutable()",
    "private.require_contribution_source_manifest()",
    "private.assert_contribution_prepared_route_shape(jsonb)",
    "private.assert_current_contribution_reservation_authority(ops.model_call_ledger)",
    "ops.contribution_reservation_authority_validate()",
];

const R4_MIGRATION_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../migrations/0131_contribution_execution.sql"
);

fn r4_create_tables(sql: &str) -> BTreeSet<String> {
    sql.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.starts_with("--") {
                return None;
            }
            let mut words = line.split_whitespace();
            if !words.next()?.eq_ignore_ascii_case("CREATE")
                || !words.next()?.eq_ignore_ascii_case("TABLE")
            {
                return None;
            }
            let next = words.next()?;
            let name = if next.eq_ignore_ascii_case("IF") {
                if !words.next()?.eq_ignore_ascii_case("NOT")
                    || !words.next()?.eq_ignore_ascii_case("EXISTS")
                {
                    return None;
                }
                words.next()?
            } else {
                next
            };
            (name.contains('.')).then(|| name.trim_matches('"').to_string())
        })
        .collect()
}

fn r4_registry_coverage(created: &BTreeSet<String>, named: &BTreeSet<&str>) -> Vec<String> {
    if created.is_empty() {
        return vec!["0131 CREATE TABLE set is empty".to_string()];
    }
    created
        .iter()
        .filter(|relation| !named.contains(relation.as_str()))
        .map(|relation| {
            format!("{relation}: 0131-created relation missing exact named ACL registry")
        })
        .collect()
}

pub fn check_r4_execution_registry(client: &mut impl GenericClient) -> GateResult {
    let named = named_tables();
    let mut problems = Vec::new();
    let sql = match fs::read_to_string(R4_MIGRATION_PATH) {
        Ok(sql) => sql,
        Err(e) => {
            return fail(
                "R4 execution registry",
                format!("cannot read {R4_MIGRATION_PATH}: {e}"),
            );
        }
    };
    let created = r4_create_tables(&sql);
    problems.extend(r4_registry_coverage(&created, &named));
    for relation in &created {
        match client.query_one("SELECT to_regclass($1) IS NOT NULL", &[relation]) {
            Ok(row) if row.get::<_, bool>(0) => {}
            Ok(_) => problems.push(format!("{relation}: missing required R4 relation")),
            Err(e) => problems.push(format!("{relation}: catalog probe failed: {e}")),
        }
    }
    if problems.is_empty() {
        pass(
            "R4 execution registry",
            "all current 0131-created relations are present and registered",
        )
    } else {
        fail("R4 execution registry", problems.join("; "))
    }
}

pub fn check_r4_execution_function_boundary(client: &mut impl GenericClient) -> GateResult {
    let mut problems = Vec::new();
    let expected: BTreeSet<String> = R4_TYPED_FUNCTIONS
        .iter()
        .chain(R4_INTERNAL_FUNCTIONS)
        .map(|signature| (*signature).to_string())
        .collect();
    match client.query(
        "SELECT p.oid::regprocedure::text \
         FROM pg_proc p \
         JOIN pg_namespace n ON n.oid = p.pronamespace \
         WHERE p.prosecdef \
           AND n.nspname IN ('private', 'ops') \
           AND (p.proname LIKE '%contribution%' \
                OR pg_get_functiondef(p.oid) LIKE '%private.contribution_executions%') \
         ORDER BY 1",
        &[],
    ) {
        Ok(rows) => {
            let actual: BTreeSet<String> = rows
                .into_iter()
                .map(|row| row.get::<_, String>(0))
                .collect();
            for signature in actual.difference(&expected) {
                problems.push(format!(
                    "{signature}: unregistered R4 contribution SECURITY DEFINER function"
                ));
            }
            for signature in expected.difference(&actual) {
                problems.push(format!(
                    "{signature}: absent from exact R4 contribution SECURITY DEFINER catalog census"
                ));
            }
        }
        Err(e) => problems.push(format!(
            "exact R4 contribution SECURITY DEFINER catalog census failed: {e}"
        )),
    }
    for (signature, worker_execute) in R4_TYPED_FUNCTIONS
        .iter()
        .map(|signature| (*signature, true))
        .chain(
            R4_INTERNAL_FUNCTIONS
                .iter()
                .map(|signature| (*signature, false)),
        )
    {
        let row = match client.query_opt(
            "SELECT pg_get_userbyid(p.proowner), p.prosecdef, coalesce(p.proconfig, ARRAY[]::text[]) \
             FROM pg_proc p WHERE p.oid = to_regprocedure($1)",
            &[&signature],
        ) {
            Ok(Some(row)) => row,
            Ok(None) => { problems.push(format!("missing R4 typed function {signature}")); continue; }
            Err(e) => { problems.push(format!("{signature}: catalog query failed: {e}")); continue; }
        };
        let owner: String = row.get(0);
        let definer: bool = row.get(1);
        let config: Vec<String> = row.get(2);
        if owner != OWNER_ROLE || !definer || config.as_slice() != ["search_path=pg_catalog"] {
            problems.push(format!("{signature}: expected owner={OWNER_ROLE}, SECURITY DEFINER, search_path=pg_catalog; actual owner={owner}, definer={definer}, config={config:?}"));
        }
        for role in NON_OWNER_ROLES {
            let allowed = match client.query_one(
                "SELECT has_function_privilege($1::text, $2::text, 'EXECUTE')",
                &[role, &signature],
            ) {
                Ok(row) => row.get::<_, bool>(0),
                Err(e) => {
                    problems.push(format!("{signature}/{role}: privilege probe failed: {e}"));
                    continue;
                }
            };
            let expected = worker_execute && *role == "role_private_worker";
            if allowed != expected {
                problems.push(format!(
                    "{signature}/{role}: expected EXECUTE={expected}, actual {allowed}"
                ));
            }
        }
        let public_absent = match client.query_one("SELECT NOT EXISTS (SELECT 1 FROM pg_proc p, aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) x WHERE p.oid=to_regprocedure($1) AND x.grantee=0 AND x.privilege_type='EXECUTE')", &[&signature]) { Ok(row) => row.get::<_, bool>(0), Err(e) => { problems.push(format!("{signature}/PUBLIC: privilege probe failed: {e}")); false } };
        if !public_absent {
            problems.push(format!("{signature}/PUBLIC: EXECUTE must be revoked"));
        }
    }
    if problems.is_empty() {
        pass(
            "R4 execution function boundary",
            format!(
                "{} exposed private-worker APIs and {} internal no-runtime APIs are exact",
                R4_TYPED_FUNCTIONS.len(),
                R4_INTERNAL_FUNCTIONS.len()
            ),
        )
    } else {
        fail("R4 execution function boundary", problems.join("; "))
    }
}

/// §25.3.1 W1: five owner/FORCE tables, Gateway-only commands, and the marker-scoped A''
/// Evidence SELECT/locking policy pairs. The query is deliberately catalog-backed rather than trusting SQL
/// source text; architecture-check owns the companion call-site/protocol scan.
#[allow(clippy::too_many_lines)]
pub fn check_w1_continuity_boundary(client: &mut impl GenericClient) -> GateResult {
    let check = "W1 Project Continuity boundary";
    let exists: bool = match client.query_one(
        "SELECT to_regclass('private.continuity_projects') IS NOT NULL",
        &[],
    ) {
        Ok(row) => row.get(0),
        Err(error) => return fail(check, format!("catalog probe failed: {error}")),
    };
    if !exists {
        return not_applicable(check, "missing object: private.continuity_projects");
    }
    let sql = r#"
WITH continuity_tables(name) AS (VALUES
 ('continuity_projects'),('continuity_facet_versions'),
 ('continuity_facet_memory_links'),('continuity_facet_evidence_links'),
 ('continuity_facet_slots')),
runtime_roles(name) AS (VALUES
 ('role_admin'),('role_gateway'),('role_private_worker'),
 ('role_consolidation_worker'),('role_public_worker'),('role_retrieval_worker'),
 ('role_batch_issuer'),('role_maintenance')),
continuity_functions(signature) AS (VALUES
 ('private.register_continuity_project(uuid,uuid,uuid,uuid,uuid,text)'),
 ('private.publish_continuity_facet(uuid,uuid,uuid,uuid,uuid,text,bigint,text,jsonb,uuid[],bytea[],uuid[],bytea[])'))
SELECT
 (SELECT count(*)=5 FROM continuity_tables expected
  JOIN pg_class c ON c.relname=expected.name
  JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname='private'
  WHERE c.relkind='r' AND c.relrowsecurity AND c.relforcerowsecurity
    AND pg_get_userbyid(c.relowner)='role_migration_owner')
 AND NOT EXISTS(SELECT 1 FROM continuity_tables t CROSS JOIN runtime_roles r
  WHERE has_table_privilege(r.name,'private.'||t.name,
   'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND NOT EXISTS(SELECT 1 FROM continuity_functions f CROSS JOIN runtime_roles r
  WHERE has_function_privilege(r.name,f.signature,'EXECUTE')
   IS DISTINCT FROM (r.name='role_gateway'))
 AND NOT EXISTS(SELECT 1 FROM continuity_functions f
  WHERE has_function_privilege('public',f.signature,'EXECUTE'))
 AND (SELECT count(*)=1 FROM pg_policy p WHERE
  p.polrelid='private.evidence_objects'::regclass
  AND p.polname='continuity_evidence_owner_exact_allow' AND p.polcmd='r'
  AND p.polpermissive AND p.polwithcheck IS NULL
  AND p.polroles=ARRAY[(SELECT oid FROM pg_roles WHERE rolname='role_migration_owner')]
  AND strpos(pg_get_expr(p.polqual,p.polrelid),'humaux.continuity_publish')>0
  AND strpos(pg_get_expr(p.polqual,p.polrelid),'humaux.workspace_id')>0
  AND strpos(pg_get_expr(p.polqual,p.polrelid),'pg_input_is_valid')>0)
 AND (SELECT count(*)=1 FROM pg_policy p WHERE
  p.polrelid='private.evidence_objects'::regclass
  AND p.polname='continuity_evidence_owner_lock_allow' AND p.polcmd='w'
  AND p.polpermissive
  AND p.polroles=ARRAY[(SELECT oid FROM pg_roles WHERE rolname='role_migration_owner')]
  AND strpos(pg_get_expr(p.polqual,p.polrelid),'humaux.continuity_publish')>0
  AND strpos(pg_get_expr(p.polqual,p.polrelid),'humaux.workspace_id')>0
  AND strpos(pg_get_expr(p.polqual,p.polrelid),'pg_input_is_valid')>0
  AND pg_get_expr(p.polwithcheck,p.polrelid)='false')
 AND (SELECT count(*)=1 FROM pg_policy p WHERE
  p.polrelid='private.evidence_objects'::regclass
  AND p.polname='continuity_evidence_owner_lock_guard' AND p.polcmd='w'
  AND NOT p.polpermissive
  AND p.polroles=ARRAY[(SELECT oid FROM pg_roles WHERE rolname='role_migration_owner')]
  AND strpos(pg_get_expr(p.polqual,p.polrelid),'humaux.continuity_publish')>0
  AND strpos(pg_get_expr(p.polqual,p.polrelid),'humaux.workspace_id')>0
  AND strpos(pg_get_expr(p.polqual,p.polrelid),'pg_input_is_valid')>0
  AND strpos(pg_get_expr(p.polwithcheck,p.polrelid),'humaux.continuity_publish')>0
  AND strpos(pg_get_expr(p.polwithcheck,p.polrelid),'IS DISTINCT FROM')>0)
 AND (SELECT polcmd='*' AND polpermissive AND polroles=ARRAY[0::oid]
  AND encode(sha256(convert_to(pg_get_expr(polqual,polrelid),'UTF8')),'hex')
   ='c77b3b830c7abec4ad46704fc7e88cded9a75b028c77ffd1946d2f288d7a045f'
  AND encode(sha256(convert_to(pg_get_expr(polwithcheck,polrelid),'UTF8')),'hex')
   ='c77b3b830c7abec4ad46704fc7e88cded9a75b028c77ffd1946d2f288d7a045f'
 FROM pg_policy WHERE polrelid='private.evidence_objects'::regclass
   AND polname='evidence_objects_tenant_and_visibility')
 AND (SELECT count(*)=5 FROM pg_policy
  WHERE polrelid='private.evidence_objects'::regclass)
 AND (SELECT count(*)=4 FROM (VALUES
   ('continuity_evidence_owner_exact_allow','r',true,
    'a0094a5c18098d94a48082666c19502cb123edafc20bff09abb46eae279e466c',NULL::text),
   ('continuity_evidence_owner_exact_guard','r',false,
    'c4c9a222db017fad89980cab140029fc8fa63c169d491652902e328ed5ae338d',NULL::text),
   ('continuity_evidence_owner_lock_allow','w',true,
    'a0094a5c18098d94a48082666c19502cb123edafc20bff09abb46eae279e466c',
    'fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa'),
   ('continuity_evidence_owner_lock_guard','w',false,
    'c4c9a222db017fad89980cab140029fc8fa63c169d491652902e328ed5ae338d',
    '8d750b95910aa4cec8bc44aef62926b4c361e16a6850d659848de4a7b08b328a')
  ) expected(name,cmd,permissive,qual_sha,check_sha)
  JOIN pg_policy p ON p.polrelid='private.evidence_objects'::regclass
   AND p.polname=expected.name AND p.polcmd=expected.cmd
   AND p.polpermissive=expected.permissive
   AND p.polroles=ARRAY[(SELECT oid FROM pg_roles WHERE rolname='role_migration_owner')]
   AND encode(sha256(convert_to(pg_get_expr(p.polqual,p.polrelid),'UTF8')),'hex')
    =expected.qual_sha
   AND coalesce(encode(sha256(convert_to(pg_get_expr(p.polwithcheck,p.polrelid),'UTF8')),'hex'),'')
    =coalesce(expected.check_sha,''))
 AND (SELECT count(*)=1 FROM pg_policy p WHERE
  p.polrelid='private.evidence_objects'::regclass
  AND p.polname='continuity_evidence_owner_exact_guard' AND p.polcmd='r'
  AND NOT p.polpermissive AND p.polwithcheck IS NULL
  AND p.polroles=ARRAY[(SELECT oid FROM pg_roles WHERE rolname='role_migration_owner')]
  AND strpos(pg_get_expr(p.polqual,p.polrelid),'humaux.continuity_publish')>0
  AND strpos(pg_get_expr(p.polqual,p.polrelid),'humaux.workspace_id')>0
  AND strpos(pg_get_expr(p.polqual,p.polrelid),'pg_input_is_valid')>0)
 AND (SELECT count(*)=5 FROM pg_trigger WHERE NOT tgisinternal AND tgname IN
  ('continuity_project_mutation_guard','continuity_facet_versions_append_only',
   'continuity_facet_memory_links_append_only','continuity_facet_evidence_links_append_only',
   'continuity_facet_version_source_required'))
 AND (SELECT count(*)=3 FROM pg_constraint WHERE convalidated AND conname IN
  ('continuity_facet_versions_kind_closed','continuity_facet_slots_kind_closed',
   'continuity_facet_slots_current_fkey'))
 AND (SELECT proconfig @> ARRAY['search_path=pg_catalog','humaux.continuity_publish=1']::text[]
  FROM pg_proc WHERE oid=
   'private.publish_continuity_facet(uuid,uuid,uuid,uuid,uuid,text,bigint,text,jsonb,uuid[],bytea[],uuid[],bytea[])'::regprocedure)
 AND (SELECT NOT (coalesce(proconfig,'{}') @> ARRAY['humaux.continuity_publish=1']::text[])
  FROM pg_proc WHERE oid=
   'private.register_continuity_project(uuid,uuid,uuid,uuid,uuid,text)'::regprocedure)
"#;
    match client.query_one(sql, &[]) {
        Ok(row) if row.get::<_, bool>(0) => pass(check, "catalog/ACL/RLS/A'' marker census exact"),
        Ok(_) => fail(check, "catalog/ACL/RLS/A'' marker census mismatch"),
        Err(error) => fail(check, format!("catalog census failed: {error}")),
    }
}

/// §25.3.1 W2: the single raw reader is migration-owner/STABLE/definer, PUBLIC is
/// absent, Gateway is the sole runtime EXECUTE principal, and W1 tables remain opaque.
pub fn check_w2_continuity_boundary(client: &mut impl GenericClient) -> GateResult {
    let check = "W2 Project Continuity read boundary";
    let signature = "private.read_continuity_project_storage_v1(uuid,uuid,uuid,uuid,uuid,uuid[])";
    let exists: bool =
        match client.query_one("SELECT to_regprocedure($1) IS NOT NULL", &[&signature]) {
            Ok(row) => row.get(0),
            Err(error) => return fail(check, format!("catalog probe failed: {error}")),
        };
    if !exists {
        return not_applicable(
            check,
            "missing object: private.read_continuity_project_storage_v1",
        );
    }
    let sql = r#"
WITH runtime_roles(name) AS (VALUES
 ('role_admin'),('role_gateway'),('role_private_worker'),
 ('role_consolidation_worker'),('role_public_worker'),('role_retrieval_worker'),
 ('role_batch_issuer'),('role_maintenance')),
continuity_tables(name) AS (VALUES
 ('continuity_projects'),('continuity_facet_versions'),
 ('continuity_facet_memory_links'),('continuity_facet_evidence_links'),
 ('continuity_facet_slots')),
reader AS (
 SELECT p.oid,p.prosecdef,p.provolatile,p.proconfig,p.proowner,
        pg_get_functiondef(p.oid) definition
 FROM pg_proc p WHERE p.oid=to_regprocedure(
  'private.read_continuity_project_storage_v1(uuid,uuid,uuid,uuid,uuid,uuid[])'))
SELECT
 (SELECT count(*)=1 FROM reader WHERE prosecdef AND provolatile='s'
  AND proconfig=ARRAY['search_path=pg_catalog']::text[]
  AND pg_get_userbyid(proowner)='role_migration_owner')
 AND NOT has_function_privilege('public',
  'private.read_continuity_project_storage_v1(uuid,uuid,uuid,uuid,uuid,uuid[])','EXECUTE')
 AND NOT EXISTS(SELECT 1 FROM runtime_roles r WHERE has_function_privilege(
  r.name,'private.read_continuity_project_storage_v1(uuid,uuid,uuid,uuid,uuid,uuid[])','EXECUTE')
  IS DISTINCT FROM (r.name='role_gateway'))
 AND NOT EXISTS(SELECT 1 FROM continuity_tables t CROSS JOIN runtime_roles r
  WHERE has_table_privilege(r.name,'private.'||t.name,
   'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
 AND (SELECT strpos(definition,'project.workspace_id=ANY(p_authorized_workspace_ids)')>0
  AND strpos(definition,'project.lifecycle_state=''ACTIVE''')>0
  AND strpos(definition,'p_requested_workspace_id IS NULL')>0
  AND strpos(definition,'humaux.workspace_id')>0
  AND strpos(definition,'pg_input_is_valid')>0
  AND strpos(definition,'memory_links_exact')>0
  AND strpos(definition,'evidence_links_exact')>0
  AND strpos(definition,'stored_body_sha256')>0
  AND strpos(definition,'ORDER BY slot.facet_kind NULLS FIRST')>0
  AND strpos(definition,'INSERT INTO')=0
  AND strpos(definition,'UPDATE private.')=0
  AND strpos(definition,'DELETE FROM')=0 FROM reader)
"#;
    match client.query_one(sql, &[]) {
        Ok(row) if row.get::<_, bool>(0) => {
            pass(check, "exact owner/config/ACL and zero W1 direct grants")
        }
        Ok(_) => fail(check, "owner/config/ACL or W1 direct-grant census mismatch"),
        Err(error) => fail(check, format!("catalog census failed: {error}")),
    }
}

fn report(results: &[GateResult]) -> i32 {
    let mut failed = false;
    for r in results {
        let tag = match r.status {
            GateStatus::Pass => "pass",
            GateStatus::Fail => {
                failed = true;
                "fail"
            }
            GateStatus::NotApplicable => "not_applicable",
        };
        eprintln!("rls-check {}: {tag} — {}", r.check, r.detail);
    }
    i32::from(failed)
}

pub fn run(_args: &[String]) -> i32 {
    let mut results = Vec::new();

    let spec_text = match fs::read_to_string(SPEC_PATH) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("rls-check: fail — cannot read spec at {SPEC_PATH}: {e}");
            return 1;
        }
    };
    results.push(check_table_set_derivation(&spec_text));

    match connect() {
        Ok(mut client) => {
            results.push(check_role_set_equality(&mut client));
            results.push(check_grant_equality(&mut client));
            results.push(check_domain_default_grants(&mut client));
            results.push(check_forbidden_verbs(&mut client));
            results.push(check_invoice_privilege_unique(&mut client));
            results.push(check_rls_four_item(&mut client));
            results.push(check_admin_plane_no_leak(&mut client));
            results.push(check_r3_health_observation_boundary(&mut client));
            results.push(check_r4_execution_function_boundary(&mut client));
            results.push(check_r4_execution_registry(&mut client));
            results.push(check_w1_continuity_boundary(&mut client));
            results.push(check_w2_continuity_boundary(&mut client));
        }
        Err(conn_err) => {
            for name in [
                "角色全集相等",
                "授权逐条相等",
                "域默认授权",
                "全域禁动词",
                "发票权唯一",
                "RLS 四项",
                "Admin-Plane 防泄漏",
                "R3 health boundary",
                "R4 execution function boundary",
                "R4 execution registry",
                "W1 Project Continuity boundary",
                "W2 Project Continuity read boundary",
            ] {
                results.push(fail_for(name, &conn_err));
            }
        }
    }

    report(&results)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------------
    // Item 2 — pure text, no DB. Fixture spec strings exercise both the
    // §48.2 "派生闸正向" and "派生闸反向" 注错 scenarios directly.
    // ------------------------------------------------------------------

    fn matrix_block() -> &'static str {
        "### 6.2.1 域级默认授权\n\nsome domain default prose\n\n### 6.2.2 表级授权（GRANT 级，逐条可静态查）\n\n| role | `private.ingest_tickets` |\n|---|---|\n| role_gateway | SELECT, UPDATE |\n\n### 6.2.3 Typed DB Pool\n\nsome pool prose\n"
    }

    #[test]
    fn r4_execution_function_classification_is_exact() {
        let exposed: BTreeSet<_> = R4_TYPED_FUNCTIONS.iter().copied().collect();
        let internal: BTreeSet<_> = R4_INTERNAL_FUNCTIONS.iter().copied().collect();
        assert_eq!(R4_TYPED_FUNCTIONS.len(), 8, "8 exposed APIs are frozen");
        assert_eq!(
            R4_INTERNAL_FUNCTIONS.len(),
            11,
            "11 retired/internal authority functions are frozen"
        );
        assert_eq!(
            exposed.len(),
            8,
            "exposed list must not duplicate signatures"
        );
        assert_eq!(
            internal.len(),
            11,
            "internal list must not duplicate signatures"
        );
        assert!(
            exposed.is_disjoint(&internal),
            "a signature cannot be both exposed and internal"
        );
    }

    /// `HUMAUX_REQUIRE_DB=1` makes a missing disposable database a hard failure.
    fn skip_is_a_failure() -> bool {
        std::env::var("HUMAUX_REQUIRE_DB").is_ok_and(|v| v == "1")
    }

    macro_rules! txn_or_skip {
        ($client:ident, $txn:ident) => {
            let Ok(dsn) = std::env::var(DSN_ENV) else {
                assert!(
                    !skip_is_a_failure(),
                    "HUMAUX_REQUIRE_DB is set, so a skip here is a failure — missing object: {DSN_ENV}"
                );
                eprintln!("rls-check test: not_applicable — {DSN_ENV} unset, skipping");
                return;
            };
            let Ok(mut $client) = Client::connect(&dsn, NoTls) else {
                assert!(
                    !skip_is_a_failure(),
                    "HUMAUX_REQUIRE_DB is set, so a skip here is a failure — missing object: reachable Postgres at {DSN_ENV}"
                );
                eprintln!("rls-check test: not_applicable — cannot reach Postgres, skipping");
                return;
            };
            let mut $txn = $client
                .transaction()
                .expect("open fault-injection transaction");
        };
    }

    fn assert_r4_function_boundary_fault(
        txn: &mut impl GenericClient,
        signature: &str,
        expected_detail: &str,
    ) {
        let result = check_r4_execution_function_boundary(txn);
        assert_eq!(result.status, GateStatus::Fail, "{}", result.detail);
        assert!(result.detail.contains(signature), "{}", result.detail);
        assert!(
            result.detail.contains(expected_detail),
            "expected {expected_detail:?} in {}",
            result.detail
        );
    }

    fn apply_r4_function_fault(
        txn: &mut impl GenericClient,
        signature: &str,
        fault_sql: &str,
        restore_sql: &str,
        expected_detail: &str,
    ) {
        txn.batch_execute(fault_sql)
            .expect("apply R4 function fault");
        assert_r4_function_boundary_fault(txn, signature, expected_detail);
        txn.batch_execute(restore_sql)
            .expect("restore R4 function boundary");
        let restored = check_r4_execution_function_boundary(txn);
        assert_eq!(restored.status, GateStatus::Pass, "{}", restored.detail);
    }

    fn apply_r4_public_execute_fault(txn: &mut impl GenericClient, signature: &str) {
        txn.batch_execute(&format!("GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC"))
            .expect("grant R4 function to PUBLIC");
        let public_execute: bool = txn
            .query_one(
                "SELECT has_function_privilege(0, $1::text, 'EXECUTE')",
                &[&signature],
            )
            .expect("verify PUBLIC grant")
            .get(0);
        assert!(
            public_execute,
            "PUBLIC grant did not take effect for {signature}"
        );
        assert_r4_function_boundary_fault(txn, signature, "PUBLIC");
        txn.batch_execute(&format!(
            "REVOKE EXECUTE ON FUNCTION {signature} FROM PUBLIC"
        ))
        .expect("revoke R4 function from PUBLIC");
        let restored = check_r4_execution_function_boundary(txn);
        assert_eq!(restored.status, GateStatus::Pass, "{}", restored.detail);
    }

    /// The real R4 typed-function gate must observe every boundary mutation and
    /// recover after its inverse. This is deliberately one uncommitted transaction:
    /// no ACL, owner, or `proconfig` fault escapes to a shared test database.
    #[test]
    fn r4_function_boundary_fault_matrix() {
        txn_or_skip!(client, txn);

        let clean = check_r4_execution_function_boundary(&mut txn);
        assert_eq!(clean.status, GateStatus::Pass, "{}", clean.detail);

        txn.batch_execute(
            "CREATE FUNCTION private.contribution_unregistered_fixture() RETURNS void \
             LANGUAGE sql SECURITY DEFINER SET search_path TO pg_catalog AS 'SELECT 1'",
        )
        .expect("create unregistered R4 SECURITY DEFINER fixture");
        assert_r4_function_boundary_fault(
            &mut txn,
            "private.contribution_unregistered_fixture()",
            "unregistered R4 contribution SECURITY DEFINER function",
        );
        txn.batch_execute("DROP FUNCTION private.contribution_unregistered_fixture()")
            .expect("drop unregistered R4 SECURITY DEFINER fixture");
        let census_restored = check_r4_execution_function_boundary(&mut txn);
        assert_eq!(
            census_restored.status,
            GateStatus::Pass,
            "{}",
            census_restored.detail
        );

        let backend_pid: i32 = txn
            .query_one("SELECT pg_backend_pid()", &[])
            .expect("read backend pid")
            .get(0);
        let wrong_owner = format!("xtask_fx_r4_wrong_owner_{backend_pid}");
        txn.batch_execute(&format!("CREATE ROLE {wrong_owner} NOLOGIN"))
            .expect("create transaction-local wrong-owner role");
        txn.batch_execute(&format!("GRANT CREATE ON SCHEMA private TO {wrong_owner}"))
            .expect("allow temporary function owner to own private functions");
        txn.batch_execute(&format!("GRANT CREATE ON SCHEMA ops TO {wrong_owner}"))
            .expect("allow temporary function owner to own ops trigger functions");

        for signature in R4_TYPED_FUNCTIONS.iter().chain(R4_INTERNAL_FUNCTIONS) {
            apply_r4_function_fault(
                &mut txn,
                signature,
                &format!("ALTER FUNCTION {signature} OWNER TO {wrong_owner}"),
                &format!("ALTER FUNCTION {signature} OWNER TO {OWNER_ROLE}"),
                &wrong_owner,
            );
            apply_r4_function_fault(
                &mut txn,
                signature,
                &format!("ALTER FUNCTION {signature} SET search_path TO pg_catalog, private"),
                &format!("ALTER FUNCTION {signature} SET search_path TO pg_catalog"),
                "private",
            );
            apply_r4_function_fault(
                &mut txn,
                signature,
                &format!("ALTER FUNCTION {signature} SET work_mem TO '1MB'"),
                &format!("ALTER FUNCTION {signature} RESET work_mem"),
                "work_mem",
            );
        }

        for signature in R4_TYPED_FUNCTIONS {
            apply_r4_public_execute_fault(&mut txn, signature);
            apply_r4_function_fault(
                &mut txn,
                signature,
                &format!("GRANT EXECUTE ON FUNCTION {signature} TO role_gateway"),
                &format!("REVOKE EXECUTE ON FUNCTION {signature} FROM role_gateway"),
                "role_gateway",
            );
            apply_r4_function_fault(
                &mut txn,
                signature,
                &format!("REVOKE EXECUTE ON FUNCTION {signature} FROM role_private_worker"),
                &format!("GRANT EXECUTE ON FUNCTION {signature} TO role_private_worker"),
                "role_private_worker",
            );
        }

        for signature in R4_INTERNAL_FUNCTIONS {
            for role in ["role_private_worker", "PUBLIC", "role_gateway"] {
                if role == "PUBLIC" {
                    apply_r4_public_execute_fault(&mut txn, signature);
                } else {
                    apply_r4_function_fault(
                        &mut txn,
                        signature,
                        &format!("GRANT EXECUTE ON FUNCTION {signature} TO {role}"),
                        &format!("REVOKE EXECUTE ON FUNCTION {signature} FROM {role}"),
                        role,
                    );
                }
            }
        }
    }

    #[test]
    fn r4_execution_relation_classification_is_exact() {
        let created = r4_create_tables(include_str!(
            "../../migrations/0131_contribution_execution.sql"
        ));
        let expected: BTreeSet<String> = [
            "private.contribution_executions",
            "private.contribution_execution_sources",
            "ops.contribution_execution_job_links",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        assert_eq!(created, expected, "test-only S1 identity evidence");
        assert!(r4_registry_coverage(&created, &named_tables()).is_empty());
    }

    #[test]
    fn r4_migration_origin_table_set_is_exact_and_fail_closed() {
        let clean = "CREATE TABLE private.contribution_executions (x int);\nCREATE TABLE private.contribution_execution_sources (x int);\nCREATE TABLE ops.contribution_execution_job_links (x int);";
        let created = r4_create_tables(clean);
        let partial_named: BTreeSet<&str> = [
            "private.contribution_executions",
            "private.contribution_execution_sources",
        ]
        .into_iter()
        .collect();
        assert!(
            !r4_registry_coverage(&created, &partial_named).is_empty(),
            "N1"
        );
        let full_named: BTreeSet<&str> = [
            "private.contribution_executions",
            "private.contribution_execution_sources",
            "ops.contribution_execution_job_links",
        ]
        .into_iter()
        .collect();
        assert!(
            r4_registry_coverage(&created, &full_named).is_empty(),
            "P current coverage"
        );
        let future = format!("{clean}\nCREATE TABLE private.any_fourth_relation (x int);");
        let n2 = r4_registry_coverage(&r4_create_tables(&future), &full_named);
        assert_eq!(
            n2,
            vec![
                "private.any_fourth_relation: 0131-created relation missing exact named ACL registry"
            ]
        );
        let ordinary = "private.contribution_execution_future_probe";
        assert!(
            r4_registry_coverage(&created, &full_named).is_empty(),
            "P non-origin {ordinary} is irrelevant"
        );
        let parser = "-- CREATE TABLE private.comment_only (x int)\n  create   table if not exists private.lower (x int);";
        assert!(r4_create_tables(parser).contains("private.lower"));
        assert!(!r4_create_tables(parser).contains("private.comment_only"));
    }

    #[test]
    fn table_set_derivation_passes_on_clean_matrix_only_spec() {
        let spec = matrix_block();
        let r = derivation_check_from_text(spec);
        assert_eq!(r.status, GateStatus::Pass, "{}", r.detail);
    }

    /// T must come from the doc's own header row, not the hardcoded [`MATRIX`]: parsing
    /// the fixture's one-table header must yield exactly that table, independent of the
    /// real Phase9 MATRIX constant.
    #[test]
    fn parse_matrix_header_tables_reads_the_doc_not_the_rust_matrix() {
        let t = parse_matrix_header_tables(matrix_block());
        assert_eq!(
            t,
            BTreeSet::from(["private.ingest_tickets".to_string()]),
            "{t:?}"
        );
    }

    /// The canonical Markdown table must keep every row aligned with its header.  The
    /// derivation parser only needs the header, so this catches a malformed separator
    /// or role row before a rendered §6.2.2 matrix becomes misleading.
    #[test]
    fn real_matrix_rows_match_header_width() {
        let spec = fs::read_to_string(SPEC_PATH).expect("spec file must exist");
        assert_eq!(matrix_row_width_error(&spec), None);
    }

    /// The production derivation gate must reject malformed matrix rows before it can
    /// accept their header-derived table set.
    #[test]
    fn table_set_derivation_fault_short_matrix_separator_turns_red() {
        let malformed = matrix_block().replacen("|---|---|", "|---|", 1);
        let r = check_table_set_derivation(&malformed);
        assert_eq!(r.status, GateStatus::Fail, "{}", r.detail);
        assert!(r.detail.contains("matrix row has"), "{}", r.detail);
    }

    /// 派生闸正向（§48.2 注错①）: a fresh SQL block writing to a table absent from
    /// §6.2.2 must turn the gate red without touching any GRANT.
    #[test]
    fn table_set_derivation_fault_new_sql_write_target_turns_red() {
        let spec = format!(
            "{}\n\n```sql\nUPDATE private.some_untracked_table SET x = 1;\n```\n",
            matrix_block()
        );
        let r = derivation_check_from_text(&spec);
        assert_eq!(r.status, GateStatus::Fail);
        assert!(
            r.detail.contains("private.some_untracked_table"),
            "{}",
            r.detail
        );
    }

    /// 同 注错①，走 S_grant 路径而非 S_write：一条新的角色断言散文（带反引号表名 +
    /// 角色名 + SQL 动词），不在任何 SQL 代码块里。
    #[test]
    fn table_set_derivation_fault_new_prose_grant_assertion_turns_red() {
        let spec = format!(
            "{}\n\nrole_gateway 对 `private.some_new_table` 无 DELETE 权限。\n",
            matrix_block()
        );
        let r = derivation_check_from_text(&spec);
        assert_eq!(r.status, GateStatus::Fail);
        assert!(r.detail.contains("private.some_new_table"), "{}", r.detail);
    }

    #[test]
    fn derivation_result_unit_diff() {
        let s: BTreeSet<String> = ["private.ingest_tickets", "projection.stream_checkpoints"]
            .map(str::to_string)
            .into();
        let t_without_checkpoints: BTreeSet<&str> = ["private.ingest_tickets"].into();
        let r = derivation_result(&s, &t_without_checkpoints);
        assert_eq!(r.status, GateStatus::Fail);
        assert!(
            r.detail.contains("projection.stream_checkpoints"),
            "{}",
            r.detail
        );

        let t_with_checkpoints: BTreeSet<&str> =
            ["private.ingest_tickets", "projection.stream_checkpoints"].into();
        assert_eq!(
            derivation_result(&s, &t_with_checkpoints).status,
            GateStatus::Pass
        );
    }

    /// 派生闸反向（§48.2 注错②）, on the **real** spec: deleting
    /// `projection.stream_checkpoints` from §6.2.2's actual markdown header row must turn
    /// [`check_table_set_derivation`] red naming that table, with zero edits to
    /// [`MATRIX`] or any SQL block. This is a pure-doc edit — proof that T is read live
    /// from the spec text ([`parse_matrix_header_tables`]), not re-derived from the Rust
    /// constant it's supposed to be checking (the blocker this test replaces: the old
    /// version fed a synthetic `t` to [`derivation_result`] and never touched the real
    /// doc, so it passed even while T was still hardcoded).
    #[test]
    fn table_set_derivation_fault_removed_matrix_column_turns_red_on_real_spec() {
        let spec = fs::read_to_string(SPEC_PATH).expect("spec file must exist");
        let start = spec.find("### 6.2.2").expect("§6.2.2 heading must exist");
        let header_line = spec[start..]
            .lines()
            .find(|l| l.trim_start().starts_with("| role |"))
            .expect("§6.2.2 header row must exist")
            .to_string();
        assert!(
            header_line.contains("`projection.stream_checkpoints`"),
            "fixture assumption broken — header row no longer names this table: {header_line}"
        );
        let doctored_header = header_line.replace("`projection.stream_checkpoints`", "");
        let doctored_spec = spec.replacen(&header_line, &doctored_header, 1);

        let r = check_table_set_derivation(&doctored_spec);
        assert_eq!(r.status, GateStatus::Fail, "{}", r.detail);
        assert!(
            r.detail.contains("projection.stream_checkpoints"),
            "{}",
            r.detail
        );
    }

    #[test]
    fn legacy_receipt_matrix_columns_are_independently_required() {
        let spec = fs::read_to_string(SPEC_PATH).expect("spec file must exist");
        for column in [
            "`public._legacy_receipt_match_basis`",
            "`public.eligible_objects`",
        ] {
            let mutated = spec.replacen(column, "", 1);
            let result = check_table_set_derivation(&mutated);
            assert_eq!(
                result.status,
                GateStatus::Fail,
                "{column}: {}",
                result.detail
            );
        }
    }

    #[test]
    fn table_set_derivation_real_spec_is_clean() {
        let spec = fs::read_to_string(SPEC_PATH).expect("spec file must exist");
        let r = check_table_set_derivation(&spec);
        assert_eq!(r.status, GateStatus::Pass, "{}", r.detail);
    }

    #[test]
    fn extract_write_targets_ignores_for_update_skip_locked() {
        let sql =
            "SELECT * FROM ops.jobs FOR UPDATE SKIP LOCKED;\nUPDATE ops.jobs SET state = 'X';";
        let targets = extract_write_targets(sql);
        assert!(!targets.iter().any(|t| t == "SKIP"), "{targets:?}");
        assert!(targets.iter().any(|t| t == "ops.jobs"), "{targets:?}");
    }

    /// `FOR NO KEY UPDATE` is also a row-lock clause, not a write target — the `UPDATE`
    /// there is preceded by `KEY `, not `FOR `, which the plain 4-byte lookback missed.
    #[test]
    fn extract_write_targets_ignores_for_no_key_update() {
        let sql = "SELECT * FROM ops.jobs FOR NO KEY UPDATE;\nUPDATE ops.outbox SET x = 1;";
        let targets = extract_write_targets(sql);
        assert!(!targets.iter().any(|t| t == "ops.jobs"), "{targets:?}");
        assert!(targets.iter().any(|t| t == "ops.outbox"), "{targets:?}");
    }

    #[test]
    fn canonicalize_maps_outbox_event_alias() {
        assert_eq!(canonicalize_table_name("outbox_event"), "ops.outbox");
        assert_eq!(canonicalize_table_name("private.events"), "private.events");
    }

    /// §48.2's own worked example (spec L8685) puts a whole GRANT statement in one
    /// backtick span, not just the bare table name — `find_backtick_tables` must scan
    /// inside the span, not require the whole span to equal `schema.table`.
    #[test]
    fn find_backtick_tables_scans_inside_a_full_statement_span() {
        let text = "- **列限定**: `GRANT UPDATE ON projection.stream_checkpoints TO role_gateway`（整表）必须红。";
        let found = find_backtick_tables(text);
        assert!(
            found.iter().any(|t| t == "projection.stream_checkpoints"),
            "{found:?}"
        );
    }

    #[test]
    fn function_tokens_do_not_hide_adjacent_legacy_receipt_relations() {
        let text = "runtime role has no SELECT on `public._legacy_receipt_match_basis` or \
            `public.eligible_objects`; it has no EXECUTE on \
            `public.public_receipt_matches(...)`, `public.require_trust_root_seal()`, or \
            `public.guard_evaluated_source_identity()`";
        let found: BTreeSet<String> = find_backtick_tables(text).into_iter().collect();
        assert_eq!(
            found,
            BTreeSet::from([
                "public._legacy_receipt_match_basis".to_string(),
                "public.eligible_objects".to_string(),
            ])
        );
    }

    /// A bare short name (no schema prefix) inside backticks, as §48.2's prose actually
    /// writes it (`` `stream_log` ``, `` `ingest_tickets` ``), completes to its qualified
    /// §6.2.2 name.
    #[test]
    fn find_backtick_tables_completes_bare_short_names() {
        let text = "runtime role 对 `stream_log` 只有 `SELECT` / `UPDATE`。";
        let found = find_backtick_tables(text);
        assert!(
            found.iter().any(|t| t == "projection.stream_log"),
            "{found:?}"
        );
    }

    // ------------------------------------------------------------------
    // Items 1/3/4/5/6 — live-DB fault injection, calling the actual public check_*
    // functions (not a hand-rolled query that merely resembles what they do — the
    // whole point of a fault-injection test is proving the real gate observes the
    // red→green transition). Every test runs its fault DDL inside one open, never-
    // committed transaction: PostgreSQL DDL (GRANT/REVOKE/CREATE ROLE/CREATE POLICY
    // included) is fully transactional, so dropping the `Transaction` without calling
    // `.commit()` rolls it back automatically — no other connection ever sees the fault,
    // and a panicking assertion mid-test can't leave the shared dev DB mutated (repo
    // CLAUDE.md 硬边界: "验证脚本本身就是写操作"). `Transaction` implements the same
    // `GenericClient` trait `Client` does, so the check functions run unmodified.
    // Skip (not a silent pass — just doesn't run) when HUMAUX_TEST_PG_DSN is unset or
    // unreachable.
    //
    // Fault tables/roles are named under the real §6.2.2 tables and real §6.2.0 role
    // names rather than an isolated fixture schema — check_grant_equality/
    // check_invoice_privilege_unique/check_rls_four_item all key off real schema-
    // qualified names ('private.ingest_tickets' etc.), so a same-named table in a
    // separate fixture schema is invisible to them; only the real object is.
    // ------------------------------------------------------------------

    #[test]
    fn w1_continuity_catalog_faults_drive_actual_gate_red_then_restore() {
        txn_or_skip!(client, txn);
        let clean = check_w1_continuity_boundary(&mut txn);
        assert_eq!(clean.status, GateStatus::Pass, "{}", clean.detail);

        for (label, fault) in [
            (
                "drop exact SELECT guard",
                "DROP POLICY continuity_evidence_owner_exact_guard ON private.evidence_objects",
            ),
            (
                "change UPDATE guard permissiveness and checks",
                "DROP POLICY continuity_evidence_owner_lock_guard ON private.evidence_objects; \
                 CREATE POLICY continuity_evidence_owner_lock_guard ON private.evidence_objects \
                 AS PERMISSIVE FOR UPDATE TO role_migration_owner USING (true) WITH CHECK (true)",
            ),
            (
                "broaden exact workspace predicate",
                "DROP POLICY continuity_evidence_owner_exact_allow ON private.evidence_objects; \
                 CREATE POLICY continuity_evidence_owner_exact_allow ON private.evidence_objects \
                 AS PERMISSIVE FOR SELECT TO role_migration_owner USING ( \
                   current_setting('humaux.continuity_publish',true)='1' \
                   AND visibility_class='WORKSPACE_SHARED')",
            ),
            (
                "add fifth W1 owner policy",
                "CREATE POLICY continuity_evidence_owner_extra ON private.evidence_objects \
                 AS PERMISSIVE FOR SELECT TO role_migration_owner USING (false)",
            ),
            (
                "remove FORCE RLS",
                "ALTER TABLE private.continuity_projects NO FORCE ROW LEVEL SECURITY",
            ),
            (
                "over-grant direct runtime table access",
                "GRANT SELECT ON private.continuity_projects TO role_private_worker",
            ),
            (
                "drop composite pointer foreign key",
                "ALTER TABLE private.continuity_facet_slots \
                 DROP CONSTRAINT continuity_facet_slots_current_fkey",
            ),
            (
                "drop closed facet check",
                "ALTER TABLE private.continuity_facet_slots \
                 DROP CONSTRAINT continuity_facet_slots_kind_closed",
            ),
            (
                "drop append-only guard",
                "DROP TRIGGER continuity_facet_versions_append_only \
                 ON private.continuity_facet_versions",
            ),
        ] {
            txn.batch_execute("SAVEPOINT w1_policy_fault")
                .expect("fault savepoint");
            txn.batch_execute(fault).expect(label);
            let red = check_w1_continuity_boundary(&mut txn);
            assert_eq!(red.status, GateStatus::Fail, "{label}: {}", red.detail);
            txn.batch_execute(
                "ROLLBACK TO SAVEPOINT w1_policy_fault; RELEASE SAVEPOINT w1_policy_fault",
            )
            .expect("restore exact catalog");
        }

        let restored = check_w1_continuity_boundary(&mut txn);
        assert_eq!(restored.status, GateStatus::Pass, "{}", restored.detail);
    }

    #[test]
    fn w2_continuity_catalog_faults_drive_actual_gate_red_then_restore() {
        txn_or_skip!(client, txn);
        let clean = check_w2_continuity_boundary(&mut txn);
        assert_eq!(clean.status, GateStatus::Pass, "{}", clean.detail);
        let signature =
            "private.read_continuity_project_storage_v1(uuid,uuid,uuid,uuid,uuid,uuid[])";
        for (label, fault) in [
            (
                "PUBLIC execute",
                format!("GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC"),
            ),
            (
                "second runtime executor",
                format!("GRANT EXECUTE ON FUNCTION {signature} TO role_private_worker"),
            ),
            (
                "security invoker",
                format!("ALTER FUNCTION {signature} SECURITY INVOKER"),
            ),
            ("volatile", format!("ALTER FUNCTION {signature} VOLATILE")),
            (
                "missing gateway execute",
                format!("REVOKE EXECUTE ON FUNCTION {signature} FROM role_gateway"),
            ),
            (
                "unsafe definer search path",
                format!("ALTER FUNCTION {signature} SET search_path TO public"),
            ),
            (
                "direct W1 grant",
                "GRANT SELECT ON private.continuity_facet_slots TO role_gateway".to_string(),
            ),
        ] {
            txn.batch_execute("SAVEPOINT w2_reader_fault")
                .expect("savepoint");
            txn.batch_execute(&fault).expect(label);
            let red = check_w2_continuity_boundary(&mut txn);
            assert_eq!(red.status, GateStatus::Fail, "{label}: {}", red.detail);
            txn.batch_execute(
                "ROLLBACK TO SAVEPOINT w2_reader_fault; RELEASE SAVEPOINT w2_reader_fault",
            )
            .expect("restore exact catalog");
        }
        let restored = check_w2_continuity_boundary(&mut txn);
        assert_eq!(restored.status, GateStatus::Pass, "{}", restored.detail);
    }

    /// The catalog census is not a substitute for a real runtime login: Gateway cannot
    /// directly inspect W1 storage, and PostgreSQL itself rejects a write after READ ONLY.
    #[test]
    fn w2_gateway_runtime_role_direct_storage_denial_and_read_only_write_are_real() {
        let Ok(admin_dsn) = std::env::var(DSN_ENV) else {
            assert!(
                !skip_is_a_failure(),
                "HUMAUX_REQUIRE_DB is set, so a skip here is a failure — missing object: {DSN_ENV}"
            );
            eprintln!("rls-check test: not_applicable — {DSN_ENV} unset, skipping");
            return;
        };
        let Some((_, authority)) = admin_dsn.split_once('@') else {
            panic!("{DSN_ENV} must be a PostgreSQL URL with an authority");
        };
        let gateway_dsn = format!("postgres://role_gateway:devlocal_role_gateway@{authority}");
        let mut gateway = Client::connect(&gateway_dsn, NoTls).unwrap_or_else(|error| {
            panic!("role_gateway must be reachable for W2 runtime boundary proof: {error}")
        });
        let direct_read = gateway
            .query_one("SELECT count(*) FROM private.continuity_projects", &[])
            .expect_err("role_gateway must not directly read W1 continuity storage");
        assert_eq!(
            direct_read.code().expect("SQLSTATE").code(),
            "42501",
            "{direct_read}"
        );

        gateway
            .batch_execute("BEGIN TRANSACTION READ ONLY")
            .expect("open actual read-only runtime transaction");
        let read_only_write = gateway
            .execute(
                "UPDATE private.ingest_tickets SET state=state WHERE false",
                &[],
            )
            .expect_err("READ ONLY runtime transaction must reject a write attempt");
        assert_eq!(
            read_only_write.code().expect("SQLSTATE").code(),
            "25006",
            "{read_only_write}"
        );
        gateway
            .batch_execute("ROLLBACK")
            .expect("rollback read-only runtime transaction");
    }

    /// §48.2 注错「多授」/「少授」/「列限定」, exercised directly against
    /// [`check_grant_equality`] on the real `control.quota_windows`/`role_gateway`
    /// cell (table-level `SELECT` + column-level `UPDATE(reserved, consumed)`).
    #[test]
    fn grant_equality_fault_injection_red_to_green() {
        txn_or_skip!(client, txn);

        let clean = check_grant_equality(&mut txn);
        assert_eq!(
            clean.status,
            GateStatus::Pass,
            "dev DB must start §6.2.2-clean: {}",
            clean.detail
        );

        // 列限定 (spec L8685's own worked example): a whole-table GRANT where the
        // matrix cell is column-limited must turn this red — proof column_privileges'
        // table-level expansion is actually being subtracted out, not just re-summed.
        txn.batch_execute("GRANT UPDATE ON control.quota_windows TO role_gateway")
            .expect("fault injection");
        let over_grant = check_grant_equality(&mut txn);
        assert_eq!(over_grant.status, GateStatus::Fail, "{}", over_grant.detail);
        assert!(
            over_grant.detail.contains("control.quota_windows"),
            "{}",
            over_grant.detail
        );
    }

    /// 少授 on a table-level cell: revoking a verb the matrix requires must also fail,
    /// independent of the column-level path above — spec's own "少授也是偏离，不是更
    /// 安全所以放行".
    #[test]
    fn grant_equality_fault_injection_under_grant() {
        txn_or_skip!(client, txn);

        txn.batch_execute("REVOKE SELECT ON control.quota_windows FROM role_private_worker")
            .expect("fault injection");
        let under_grant = check_grant_equality(&mut txn);
        assert_eq!(
            under_grant.status,
            GateStatus::Fail,
            "{}",
            under_grant.detail
        );
        assert!(
            under_grant.detail.contains("role_private_worker"),
            "{}",
            under_grant.detail
        );
    }

    #[test]
    fn legacy_receipt_views_over_and_under_grants_are_red() {
        txn_or_skip!(client, txn);

        assert_eq!(check_grant_equality(&mut txn).status, GateStatus::Pass);
        txn.batch_execute("GRANT SELECT ON public._legacy_receipt_match_basis TO role_gateway")
            .expect("helper over-grant fault");
        let over = check_grant_equality(&mut txn);
        assert_eq!(over.status, GateStatus::Fail, "{}", over.detail);
        assert!(over.detail.contains("_legacy_receipt_match_basis"));

        txn.batch_execute(
            "REVOKE SELECT ON public._legacy_receipt_match_basis FROM role_gateway; \
             REVOKE SELECT ON public.eligible_objects FROM role_gateway",
        )
        .expect("outer-view under-grant fault");
        let under = check_grant_equality(&mut txn);
        assert_eq!(under.status, GateStatus::Fail, "{}", under.detail);
        assert!(under.detail.contains("eligible_objects"));
    }

    /// §48.2 注错「发票权唯一」「多授」: owner is excluded, and a second non-owner
    /// grantee turns the check red.
    #[test]
    fn invoice_privilege_unique_fault_injection_over_grant() {
        txn_or_skip!(client, txn);

        let clean = check_invoice_privilege_unique(&mut txn);
        assert_eq!(clean.status, GateStatus::Pass, "{}", clean.detail);

        txn.batch_execute("GRANT INSERT ON private.ingest_tickets TO role_gateway")
            .expect("fault injection");
        let over_grant = check_invoice_privilege_unique(&mut txn);
        assert_eq!(over_grant.status, GateStatus::Fail, "{}", over_grant.detail);
        assert!(
            over_grant.detail.contains("role_gateway"),
            "{}",
            over_grant.detail
        );
    }

    /// 少授「发票权唯一」: revoking the invoice privilege from everyone must fail, not
    /// disappear as `not_applicable` — the prior version couldn't distinguish "table
    /// missing" from "grantee set legitimately empty" (both looked like an empty query
    /// result) and treated both as `not_applicable`.
    #[test]
    fn invoice_privilege_unique_fault_injection_under_grant_is_fail_not_not_applicable() {
        txn_or_skip!(client, txn);

        txn.batch_execute("REVOKE INSERT ON private.ingest_tickets FROM role_batch_issuer")
            .expect("fault injection");
        let r = check_invoice_privilege_unique(&mut txn);
        assert_eq!(
            r.status,
            GateStatus::Fail,
            "empty grantee set on an *existing* table must fail, not not_applicable: {}",
            r.detail
        );
    }

    /// §48.2 RLS 四项 注错, exercised against [`check_rls_four_item`] directly: a new
    /// `tenant_id` table with no policy turns the overall (all-tables) check red naming
    /// it; adding `ENABLE`+`FORCE` RLS and the exact §62 template policy turns it green
    /// again. Also proves [`table_has_tenant_policy`]'s tightened match accepts the real
    /// deparsed §62 clause (not just an ILIKE `%tenant_id%` substring).
    #[test]
    fn rls_four_item_fault_injection_red_to_green() {
        txn_or_skip!(client, txn);

        txn.batch_execute(
            "CREATE SCHEMA xtask_fx_rls; \
             CREATE TABLE xtask_fx_rls.tenant_scoped (tenant_id uuid NOT NULL, v int)",
        )
        .expect("fixture DDL");

        let before = check_rls_four_item(&mut txn);
        assert_eq!(before.status, GateStatus::Fail, "{}", before.detail);
        assert!(
            before.detail.contains("xtask_fx_rls.tenant_scoped"),
            "{}",
            before.detail
        );

        txn.batch_execute(
            "ALTER TABLE xtask_fx_rls.tenant_scoped ENABLE ROW LEVEL SECURITY; \
             ALTER TABLE xtask_fx_rls.tenant_scoped FORCE ROW LEVEL SECURITY; \
             CREATE POLICY tenant_scoped_isolation ON xtask_fx_rls.tenant_scoped \
               USING (tenant_id = current_setting('humaux.tenant_id', true)::uuid) \
               WITH CHECK (tenant_id = current_setting('humaux.tenant_id', true)::uuid)",
        )
        .expect("apply §62 policy template");

        let after = check_rls_four_item(&mut txn);
        assert_eq!(after.status, GateStatus::Pass, "{}", after.detail);
    }

    /// §62's weak-USING vulnerability: `USING (true)` with a real `WITH CHECK` must
    /// still fail — a substring-only match on `%tenant_id%` would have passed this
    /// (`with_check` alone contains "tenant_id"), silently leaking every tenant's rows
    /// on SELECT.
    #[test]
    fn rls_four_item_fault_injection_weak_using_clause_stays_red() {
        txn_or_skip!(client, txn);

        txn.batch_execute(
            "CREATE SCHEMA xtask_fx_rls_weak; \
             CREATE TABLE xtask_fx_rls_weak.tenant_scoped (tenant_id uuid NOT NULL); \
             ALTER TABLE xtask_fx_rls_weak.tenant_scoped ENABLE ROW LEVEL SECURITY; \
             ALTER TABLE xtask_fx_rls_weak.tenant_scoped FORCE ROW LEVEL SECURITY; \
             CREATE POLICY weak ON xtask_fx_rls_weak.tenant_scoped \
               USING (true) \
               WITH CHECK (tenant_id = current_setting('humaux.tenant_id', true)::uuid)",
        )
        .expect("fixture DDL");

        assert!(
            !table_has_tenant_policy(&mut txn, "xtask_fx_rls_weak", "tenant_scoped").unwrap(),
            "USING (true) must not pass even though with_check mentions tenant_id"
        );
        let r = check_rls_four_item(&mut txn);
        assert_eq!(r.status, GateStatus::Fail, "{}", r.detail);
    }

    /// §6.2.1 forbidden-verb sweep, red→green against [`check_forbidden_verbs`]
    /// directly: granting DELETE to a non-owner role on a fresh table must be caught.
    #[test]
    fn forbidden_verbs_fault_injection() {
        txn_or_skip!(client, txn);

        txn.batch_execute(
            "CREATE SCHEMA xtask_fx_verbs; CREATE TABLE xtask_fx_verbs.t (id bigint)",
        )
        .expect("fixture DDL");
        let clean = check_forbidden_verbs(&mut txn);
        assert_eq!(clean.status, GateStatus::Pass, "{}", clean.detail);

        txn.batch_execute("GRANT DELETE ON xtask_fx_verbs.t TO role_gateway")
            .expect("fault injection");
        let after = check_forbidden_verbs(&mut txn);
        assert_eq!(after.status, GateStatus::Fail, "{}", after.detail);
        assert!(after.detail.contains("role_gateway"), "{}", after.detail);
    }

    /// §6.2.0 角色全集相等, red→green against [`check_role_set_equality`] directly on a
    /// temp login role.
    #[test]
    fn role_set_equality_predicate_fault_injection() {
        txn_or_skip!(client, txn);

        let clean = check_role_set_equality(&mut txn);
        assert_eq!(clean.status, GateStatus::Pass, "{}", clean.detail);

        txn.batch_execute("CREATE ROLE xtask_fx_extra_login_role LOGIN")
            .expect("fault injection");
        let after = check_role_set_equality(&mut txn);
        assert_eq!(after.status, GateStatus::Fail, "{}", after.detail);
        assert!(
            after.detail.contains("xtask_fx_extra_login_role"),
            "{}",
            after.detail
        );
    }

    /// Every 0131-created relation stays in the one existing named ACL/RLS authority.
    /// Each DDL fault is inverted before the next one so the test proves red→green,
    /// without committing any mutation to the disposable database.
    #[test]
    fn r4_relation_boundary_fault_matrix() {
        txn_or_skip!(client, txn);

        assert_eq!(check_grant_equality(&mut txn).status, GateStatus::Pass);
        assert_eq!(check_rls_four_item(&mut txn).status, GateStatus::Pass);
        assert_eq!(
            check_r4_execution_registry(&mut txn).status,
            GateStatus::Pass
        );

        let backend_pid: i32 = txn
            .query_one("SELECT pg_backend_pid()", &[])
            .expect("read backend pid")
            .get(0);
        let wrong_owner = format!("xtask_fx_r4_relation_owner_{backend_pid}");
        txn.batch_execute(&format!(
            "CREATE ROLE {wrong_owner} NOLOGIN; \
             GRANT CREATE ON SCHEMA private TO {wrong_owner}; \
             GRANT CREATE ON SCHEMA ops TO {wrong_owner}"
        ))
        .expect("create transaction-local wrong relation owner");

        let relations = [
            "private.contribution_executions",
            "private.contribution_execution_sources",
            "ops.contribution_execution_job_links",
        ];
        for relation in relations {
            for verb in ["INSERT", "UPDATE", "DELETE"] {
                txn.batch_execute(&format!(
                    "GRANT {verb} ON TABLE {relation} TO role_private_worker"
                ))
                .expect("grant forbidden R4 relation DML");
                let result = check_grant_equality(&mut txn);
                assert_eq!(result.status, GateStatus::Fail, "{}", result.detail);
                assert!(result.detail.contains(relation), "{}", result.detail);
                assert!(
                    result.detail.contains("role_private_worker"),
                    "{}",
                    result.detail
                );
                txn.batch_execute(&format!(
                    "REVOKE {verb} ON TABLE {relation} FROM role_private_worker"
                ))
                .expect("revoke forbidden R4 relation DML");
                assert_eq!(check_grant_equality(&mut txn).status, GateStatus::Pass);
            }

            txn.batch_execute(&format!(
                "ALTER TABLE {relation} DISABLE ROW LEVEL SECURITY"
            ))
            .expect("disable R4 relation RLS");
            let disabled = check_rls_four_item(&mut txn);
            assert_eq!(disabled.status, GateStatus::Fail, "{}", disabled.detail);
            assert!(disabled.detail.contains(relation), "{}", disabled.detail);
            txn.batch_execute(&format!("ALTER TABLE {relation} ENABLE ROW LEVEL SECURITY"))
                .expect("restore R4 relation RLS");
            assert_eq!(check_rls_four_item(&mut txn).status, GateStatus::Pass);

            txn.batch_execute(&format!(
                "ALTER TABLE {relation} NO FORCE ROW LEVEL SECURITY"
            ))
            .expect("disable R4 relation FORCE RLS");
            let no_force = check_rls_four_item(&mut txn);
            assert_eq!(no_force.status, GateStatus::Fail, "{}", no_force.detail);
            assert!(no_force.detail.contains(relation), "{}", no_force.detail);
            txn.batch_execute(&format!("ALTER TABLE {relation} FORCE ROW LEVEL SECURITY"))
                .expect("restore R4 relation FORCE RLS");
            assert_eq!(check_rls_four_item(&mut txn).status, GateStatus::Pass);

            txn.batch_execute(&format!("ALTER TABLE {relation} OWNER TO {wrong_owner}"))
                .expect("set wrong R4 relation owner");
            let wrong_owner_result = check_grant_equality(&mut txn);
            assert_eq!(
                wrong_owner_result.status,
                GateStatus::Fail,
                "{}",
                wrong_owner_result.detail
            );
            assert!(
                wrong_owner_result.detail.contains(relation),
                "{}",
                wrong_owner_result.detail
            );
            assert!(
                wrong_owner_result.detail.contains(&wrong_owner),
                "{}",
                wrong_owner_result.detail
            );
            txn.batch_execute(&format!("ALTER TABLE {relation} OWNER TO {OWNER_ROLE}"))
                .expect("restore R4 relation owner");
            assert_eq!(check_grant_equality(&mut txn).status, GateStatus::Pass);

            let (schema, name) = relation.split_once('.').expect("qualified R4 relation");
            let renamed = format!("xtask_fx_r4_missing_{backend_pid}_{name}");
            txn.batch_execute(&format!("ALTER TABLE {relation} RENAME TO {renamed}"))
                .expect("rename R4 relation away from migration-origin identity");
            let missing = check_r4_execution_registry(&mut txn);
            assert_eq!(missing.status, GateStatus::Fail, "{}", missing.detail);
            assert!(missing.detail.contains(relation), "{}", missing.detail);
            txn.batch_execute(&format!("ALTER TABLE {schema}.{renamed} RENAME TO {name}"))
                .expect("restore R4 relation migration-origin identity");
            assert_eq!(
                check_r4_execution_registry(&mut txn).status,
                GateStatus::Pass
            );
        }
    }

    fn pre_r4_verdicts(client: &mut impl GenericClient) -> Vec<GateStatus> {
        [
            check_role_set_equality(client),
            check_grant_equality(client),
            check_domain_default_grants(client),
            check_forbidden_verbs(client),
            check_invoice_privilege_unique(client),
            check_rls_four_item(client),
            check_admin_plane_no_leak(client),
            check_r3_health_observation_boundary(client),
        ]
        .into_iter()
        .map(|result| result.status)
        .collect()
    }

    /// R4 migration-origin coverage must not turn into a prefix deny-list or alter the
    /// eight pre-R4 verdicts. A non-0131 private table with an R4-looking name follows
    /// only the Canonical private domain defaults.
    #[test]
    fn r4_non_r4_baseline_regression() {
        txn_or_skip!(client, txn);

        let before = pre_r4_verdicts(&mut txn);
        assert!(before.iter().all(|status| *status == GateStatus::Pass));

        txn.batch_execute(
            "CREATE TABLE private.contribution_execution_future_probe (id bigint); \
             ALTER TABLE private.contribution_execution_future_probe OWNER TO role_migration_owner; \
             GRANT SELECT, INSERT, UPDATE ON private.contribution_execution_future_probe \
               TO role_gateway, role_private_worker; \
             GRANT SELECT ON private.contribution_execution_future_probe \
               TO role_consolidation_worker, role_retrieval_worker, role_maintenance;",
        )
        .expect("create ordinary private domain-default fixture");
        let registry = check_r4_execution_registry(&mut txn);
        assert_eq!(registry.status, GateStatus::Pass, "{}", registry.detail);
        let domain = check_domain_default_grants(&mut txn);
        assert_eq!(domain.status, GateStatus::Pass, "{}", domain.detail);
        let after = pre_r4_verdicts(&mut txn);
        assert_eq!(
            after, before,
            "ordinary non-0131 relation changed a pre-R4 verdict"
        );
    }

    /// §6.2.1 域默认授权, red→green against [`check_domain_default_grants`] directly:
    /// a non-named table over-granted beyond its schema's domain default must be caught.
    #[test]
    fn domain_default_grants_fault_injection() {
        txn_or_skip!(client, txn);

        // A fresh table starts with zero grants — the real dev DB's tables are clean
        // only because migration 0009's blanket `GRANT ... ON ALL TABLES IN SCHEMA`
        // ran when they were created (per-repo memory). Reproduce that step by hand for
        // this one fixture table so the "clean" baseline below is actually clean,
        // instead of failing on the fixture's own bare-metal zero grants. §6.2.1's
        // `ops.*` row: role_gateway/private_worker/public_worker/retrieval_worker get
        // R+W, consolidation_worker/maintenance get R, batch_issuer gets nothing.
        txn.batch_execute(
            "CREATE TABLE ops.xtask_fx_domain_table (id bigint); \
             ALTER TABLE ops.xtask_fx_domain_table OWNER TO role_migration_owner; \
             GRANT SELECT, INSERT, UPDATE ON ops.xtask_fx_domain_table \
               TO role_gateway, role_private_worker, role_public_worker, role_retrieval_worker; \
             GRANT SELECT ON ops.xtask_fx_domain_table \
               TO role_consolidation_worker, role_maintenance;",
        )
        .expect("fixture DDL");

        let clean = check_domain_default_grants(&mut txn);
        assert_eq!(clean.status, GateStatus::Pass, "{}", clean.detail);

        // role_batch_issuer's domain default on ops.* is `—` (nothing) — granting it
        // SELECT is a pure over-grant with no §6.2.2 cell to mask it.
        txn.batch_execute("GRANT SELECT ON ops.xtask_fx_domain_table TO role_batch_issuer")
            .expect("fault injection");
        let after = check_domain_default_grants(&mut txn);
        assert_eq!(after.status, GateStatus::Fail, "{}", after.detail);
        assert!(
            after.detail.contains("role_batch_issuer"),
            "{}",
            after.detail
        );
    }

    /// §6.2.1 Canonical owner is global, including tables that fall back to domain defaults.
    /// A deployment runner retaining ownership must turn the same executable gate red.
    #[test]
    fn domain_default_owner_fault_injection() {
        txn_or_skip!(client, txn);

        txn.batch_execute(
            "CREATE TABLE ops.xtask_fx_domain_owner (id bigint); \
             ALTER TABLE ops.xtask_fx_domain_owner OWNER TO role_migration_owner; \
             GRANT SELECT, INSERT, UPDATE ON ops.xtask_fx_domain_owner \
               TO role_gateway, role_private_worker, role_public_worker, role_retrieval_worker; \
             GRANT SELECT ON ops.xtask_fx_domain_owner \
               TO role_consolidation_worker, role_maintenance;",
        )
        .expect("fixture DDL");

        let clean = check_domain_default_grants(&mut txn);
        assert_eq!(clean.status, GateStatus::Pass, "{}", clean.detail);

        txn.batch_execute("ALTER TABLE ops.xtask_fx_domain_owner OWNER TO role_gateway")
            .expect("fault injection");
        let after = check_domain_default_grants(&mut txn);
        assert_eq!(after.status, GateStatus::Fail, "{}", after.detail);
        assert!(
            after.detail.contains("Canonical owner expected"),
            "{}",
            after.detail
        );
    }
}
