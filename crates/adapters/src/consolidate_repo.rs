//! `adapters::consolidate_repo` — §11.6/§11.7 Private Memory Consolidation SQL, through
//! [`ConsolidationDbPool`] only. The invariants themselves (`ConsolidationRunState`,
//! `AutoMutableMemoryId`/`classify`, the rollup-authority ceiling) are pure Rust in
//! `humaux_domain::consolidate` (§3/§78.3 — no I/O there); this module fetches rows, applies
//! them, and writes results.
//!
//! **§11.7 snapshot-bound selection.** [`select_and_materialize_inputs`] opens ONE transaction
//! at `REPEATABLE READ READ WRITE` (not `READ ONLY` — PostgreSQL rejects
//! `INSERT`/`UPDATE`/`DELETE` on a `READ ONLY` transaction with SQLSTATE `25006`, so the
//! spec's frozen recipe is `READ WRITE`, §11.7), runs exactly one `SELECT` against
//! `private.memory_records`, and materializes the chosen ids into
//! `private.memory_consolidation_inputs` — all inside that same transaction, so every
//! statement in it observes the identical `REPEATABLE READ` snapshot. There is no second
//! transaction, no `OFFSET`, and therefore no window in which a concurrently-inserted
//! higher-ranked row can shift what a later page would have seen (§11.7: "禁止跨事务
//! LIMIT/OFFSET + live mutable ranking"; `tests/consolidate_snapshot.rs`'s G80-29/G11-1 proves
//! it with concurrent inserts, and its doc comment records the red-then-green fault injection
//! that swaps this in for a cross-transaction `OFFSET` page-loop).
//!
//! `role_consolidation_worker` has database-level `SELECT`-only on
//! `private.evidence_objects`/`private.memory_records`/`private.memory_evidence`
//! (`migrations/0011_roles_and_grants.sql`) — the `READ WRITE` isolation level only ever lets
//! this module write the four derived tables named below; a stray `UPDATE`/`DELETE` against a
//! true-source table fails at the database, not by code review discipline
//! (`crates/adapters/src/postgres.rs`'s `g6_db2_consolidation_pool` test proves the negative
//! side of this already).

use sha2::{Digest, Sha256};
use sqlx::Row;
use sqlx::types::Uuid;
use sqlx::types::time::OffsetDateTime;

use humaux_application::consolidate::{
    RollupAuthorityViolation, SourceAuthority, validate_rollup_before_publish,
};
use humaux_domain::authority::{AuthorityClass, EvidenceId, MemoryId};
use humaux_domain::consolidate::{AutoMutableMemoryId, ClassifiedMemoryId, classify};

use crate::postgres::ConsolidationDbPool;

/// `private.memory_records.authority_class` / `private.memory_rollups.authority_class`'s text
/// encoding, reversed. `AuthorityClass` carries no `from_db_str` of its own
/// (`crates/domain/src/authority.rs` is outside this task's file scope) — this mirrors
/// `migrations/0004_private_evidence_memory.sql`'s CHECK list verbatim, same technique
/// `domain::consolidate::ConsolidationRunState::from_db_str` uses for its own DB string.
fn authority_class_from_db_str(s: &str) -> Option<AuthorityClass> {
    Some(match s {
        "PublicKnowledge" => AuthorityClass::PublicKnowledge,
        "PrivateKnowledge" => AuthorityClass::PrivateKnowledge,
        "UserPreference" => AuthorityClass::UserPreference,
        "ProjectDecision" => AuthorityClass::ProjectDecision,
        "UserCorrection" => AuthorityClass::UserCorrection,
        "ProjectConstraint" => AuthorityClass::ProjectConstraint,
        "ExplicitTaskContext" => AuthorityClass::ExplicitTaskContext,
        _ => return None,
    })
}

/// `AuthorityClass`'s DB text encoding — the write-side inverse of
/// `authority_class_from_db_str` above, needed because `private.memory_rollups.authority_class`
/// (added by `migrations/0058_memory_rollups_authority_visibility.sql`) is `text`, not a
/// dedicated Postgres enum.
const fn authority_class_to_db_str(class: AuthorityClass) -> &'static str {
    match class {
        AuthorityClass::PublicKnowledge => "PublicKnowledge",
        AuthorityClass::PrivateKnowledge => "PrivateKnowledge",
        AuthorityClass::UserPreference => "UserPreference",
        AuthorityClass::ProjectDecision => "ProjectDecision",
        AuthorityClass::UserCorrection => "UserCorrection",
        AuthorityClass::ProjectConstraint => "ProjectConstraint",
        AuthorityClass::ExplicitTaskContext => "ExplicitTaskContext",
    }
}

/// DB-layer failure. Adapter-local, not one of the workspace's two frozen domain error enums
/// (§52) — same reasoning as `email::OutboxError` / `postgres::PoolInitError`.
#[derive(Debug)]
pub enum ConsolidateRepoError {
    Db(sqlx::Error),
    /// A status-transition `UPDATE ... WHERE run_id = $1 AND status = <expected>` matched zero
    /// rows: either `run_id` does not exist, or another caller already moved this run past the
    /// expected state (a second `select_and_materialize_inputs` racing the first, or a caller
    /// re-driving an already-terminal run). Fail loudly rather than silently resetting a
    /// SUCCEEDED/FAILED run back to RUNNING.
    LostRace {
        run_id: Uuid,
        expected_status: &'static str,
    },
    /// A `publish_rollup` source `(AutoMutableMemoryId, EvidenceId)` has no matching row in
    /// this `run_id`'s own `private.memory_consolidation_inputs` — the type-level narrowing
    /// (§11.8's `AutoMutableMemoryId`) proves a source came from *some* run's selection, not
    /// necessarily *this* one; this closes the gap explicitly rather than trusting the type
    /// alone (see this module's `publish_rollup` doc comment).
    UnknownSource(Uuid),
    /// §11.9: the proposed rollup's `AuthorityClass` outranks its own source closure.
    AuthorityCeiling(RollupAuthorityViolation),
}

impl From<sqlx::Error> for ConsolidateRepoError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

impl std::fmt::Display for ConsolidateRepoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(e) => write!(f, "consolidate_repo DB error: {e}"),
            Self::LostRace {
                run_id,
                expected_status,
            } => write!(
                f,
                "consolidate_repo: run {run_id} was not in status '{expected_status}' when \
                 expected — another caller already transitioned it (lost race or re-driven \
                 terminal run)"
            ),
            Self::UnknownSource(memory_id) => write!(
                f,
                "consolidate_repo: publish_rollup source {memory_id} is not a recorded input \
                 of this run"
            ),
            Self::AuthorityCeiling(violation) => {
                write!(
                    f,
                    "consolidate_repo: §11.9 authority ceiling violated: {violation:?}"
                )
            }
        }
    }
}

impl std::error::Error for ConsolidateRepoError {}

async fn set_tenant_local(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
) -> Result<(), ConsolidateRepoError> {
    // Same technique as `jobs::set_tenant_local` / `stream_repo`'s equivalents: `tenant_id` is
    // a `Uuid`, never attacker-controlled text, so `format!` interpolation is not a SQL
    // injection surface here.
    sqlx::query(&format!("SET LOCAL humaux.tenant_id = '{tenant_id}'"))
        .execute(&mut **txn)
        .await?;
    Ok(())
}

/// §11.7's per-row change-detection fingerprint: everything about a `memory_records` row that
/// "correct/supersede/revoke" (the three staleness triggers §11.7 names) can change. Deliberately
/// excludes `updated_at` itself — that already becomes `input_version` (see call sites) — and
/// the immutable identity columns (`memory_id`/`tenant_id`/`memory_type`), which correct/
/// supersede/revoke never touch.
fn row_fingerprint(
    content: &serde_json::Value,
    authority_class: &str,
    confidence: f32,
    status: &str,
    superseded_by: Option<Uuid>,
) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(content.to_string().as_bytes());
    hasher.update(b"\0");
    hasher.update(authority_class.as_bytes());
    hasher.update(b"\0");
    hasher.update(confidence.to_bits().to_be_bytes());
    hasher.update(b"\0");
    hasher.update(status.as_bytes());
    hasher.update(b"\0");
    hasher.update(
        superseded_by
            .map(|u| u.to_string())
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.finalize().to_vec()
}

/// One row read out of `private.memory_records` during selection, before classification.
struct Candidate {
    memory_id: Uuid,
    input_version: i64,
    fingerprint: Vec<u8>,
    has_active_binding: bool,
}

/// One row [`select_and_materialize_inputs`] actually wrote to
/// `private.memory_consolidation_inputs` — the tuple §11.8's `input_manifest_hash` must be
/// computed over (memory_id, input_version, source_hash, ordinal), so the caller never has to
/// re-derive it from a bare id list.
#[derive(Debug, Clone)]
pub struct MaterializedInput {
    pub memory_id: AutoMutableMemoryId,
    pub input_version: i64,
    pub source_hash: Vec<u8>,
    pub ordinal: i32,
}

fn row_to_candidate(row: &sqlx::postgres::PgRow) -> Result<Candidate, ConsolidateRepoError> {
    let memory_id: Uuid = row.try_get("memory_id")?;
    let updated_at: OffsetDateTime = row.try_get("updated_at")?;
    let content: serde_json::Value = row.try_get("content")?;
    let authority_class: String = row.try_get("authority_class")?;
    let confidence: f32 = row.try_get("confidence")?;
    let status: String = row.try_get("status")?;
    let superseded_by: Option<Uuid> = row.try_get("superseded_by")?;
    let has_active_binding: bool = row.try_get("has_binding")?;
    Ok(Candidate {
        memory_id,
        // Microseconds since epoch — `OffsetDateTime`'s native precision, plenty fine-grained
        // to distinguish "untouched since selection" from "touched since selection" without a
        // dedicated version counter on `memory_records` (none exists yet, see
        // `migrations/0005_private_pipeline.sql`'s `memory_consolidation_inputs.input_version`
        // doc comment).
        input_version: (updated_at.unix_timestamp_nanos() / 1_000) as i64,
        fingerprint: row_fingerprint(
            &content,
            &authority_class,
            confidence,
            &status,
            superseded_by,
        ),
        has_active_binding,
    })
}

/// §11.6/§11.7: create the run row (`PENDING`) that [`select_and_materialize_inputs`]'s
/// transaction will attach inputs to via FK. A separate, short statement — not part of the
/// snapshot transaction — because §11.7's scope-key lease ("同一 scope 同时最多一个 RUNNING")
/// is `ops.jobs`'s existing lease/fencing (§31), reused rather than re-derived here; this
/// function only mints the row a caller's job-claim protects.
pub async fn create_run(
    pool: &ConsolidationDbPool,
    tenant_id: Uuid,
    reasoning_domain_id: Uuid,
    workspace_id: Option<Uuid>,
) -> Result<Uuid, ConsolidateRepoError> {
    // A transaction (not a bare `acquire()`) specifically so `set_tenant_local`'s `SET LOCAL`
    // is scoped to this statement alone — a plain session-level `SET` on a pooled connection
    // would leak `humaux.tenant_id` into whatever tenant's request reuses this connection
    // next (same reasoning `jobs`/`stream_repo`/every other repo module in this crate already
    // follows: every RLS-scoped statement runs inside its own transaction, never on a bare
    // acquired connection).
    let mut txn = pool.pool().begin().await?;
    set_tenant_local(&mut txn, tenant_id).await?;
    // `started_at` is set here, at INSERT time (unrestricted by column-level GRANT), rather
    // than by a later UPDATE from this role — §6.2.2's `role_consolidation_worker` UPDATE
    // grant on this table is column-restricted to exactly `(status, input_snapshot_seq,
    // manifest_hash, output_digest, finished_at, error_class)` (`migrations/
    // 0011_roles_and_grants.sql`); `started_at` is deliberately not in that list, so any
    // later `UPDATE ... SET started_at = ...` from this role fails with `42501` no matter
    // what else the statement sets.
    let run_id: Uuid = sqlx::query_scalar(
        "INSERT INTO private.memory_consolidation_runs \
           (tenant_id, reasoning_domain_id, workspace_id, status, started_at) \
         VALUES ($1, $2, $3, 'PENDING', now()) \
         RETURNING run_id",
    )
    .bind(tenant_id)
    .bind(reasoning_domain_id)
    .bind(workspace_id)
    .fetch_one(&mut *txn)
    .await?;
    txn.commit().await?;
    Ok(run_id)
}

// §11.8「active no-auto-mutate binding」的唯一判据。同一条查询里的投影列
// （`has_binding`）与 WHERE 过滤（`NOT EXISTS`）**必须同源**：先前两处各写一遍，
// 都只判「存在任意一行 binding」。migration 0102 加上 mode/revoked_at 之后，那种写法
// 会同时造出两个 bug——已撤销的 binding 永久冻结一条 memory、SUPPLEMENTAL binding
// 误挡 consolidation——而且**现有测试一条都不会红**（本表当时零行）。所以 0102 与
// 本处收敛必须同波落地，拆开就是伪修复。
//
// §25.4：只有 MANDATORY / PINNED 两档不参与 semantic 淘汰、也因此不许被自动
// consolidation 就地改写；SUPPLEMENTAL 是普通补充位，不构成冻结。
pub(crate) const ACTIVE_NO_AUTO_MUTATE_BINDING: &str = "SELECT 1 FROM private.context_bindings cb \
     WHERE cb.memory_id = m.memory_id \
       AND cb.revoked_at IS NULL \
       AND cb.mode IN ('MANDATORY','PINNED')";

/// §11.7 frozen recipe, verbatim: one `REPEATABLE READ READ WRITE` transaction resolves the
/// snapshot, selects eligible memories, and materializes them into
/// `memory_consolidation_inputs` — nothing here spans a second transaction or an `OFFSET`.
///
/// Memories carrying an active `private.context_bindings` row are read (so a future ranking
/// pass can see them exist) but never materialized as inputs — `domain::consolidate::classify`
/// routes them to `ClassifiedMemoryId::Bound`, which this function simply skips, mirroring
/// §11.8's "Pinned/Mandatory 输入: 可以读取, 不可自动改写" at the one place a memory could
/// otherwise become eligible for a future governance suggestion.
///
/// Returns the run's materialized inputs in the exact order recorded (`ordinal`).
///
/// `reasoning_domain_id` must be the same value [`create_run`] bound this `run_id` to (§11.8:
/// "一次 LLM consolidation 的全部输入 reasoning_domain 必须相同") — `memory_records` itself
/// carries no `reasoning_domain_id` column (that lives on `evidence_objects`, verified live),
/// so the domain match is a join through `memory_evidence`, not a bare column filter.
pub async fn select_and_materialize_inputs(
    pool: &ConsolidationDbPool,
    run_id: Uuid,
    tenant_id: Uuid,
    reasoning_domain_id: Uuid,
    workspace_id: Option<Uuid>,
    max_inputs: i64,
) -> Result<Vec<MaterializedInput>, ConsolidateRepoError> {
    let mut txn = pool.pool().begin().await?;
    // §11.7 "READ WRITE 是硬修正，不是风格选择": `READ ONLY` rejects the INSERT below with
    // SQLSTATE 25006. Set isolation before any query or data modification can establish
    // the transaction snapshot.
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ WRITE")
        .execute(&mut *txn)
        .await?;
    set_tenant_local(&mut txn, tenant_id).await?;

    let selecting = sqlx::query(
        "UPDATE private.memory_consolidation_runs SET status = 'SELECTING' \
         WHERE run_id = $1 AND status = 'PENDING'",
    )
    .bind(run_id)
    .execute(&mut *txn)
    .await?;
    if selecting.rows_affected() == 0 {
        return Err(ConsolidateRepoError::LostRace {
            run_id,
            expected_status: "PENDING",
        });
    }

    // This source SELECT uses the `REPEATABLE READ` snapshot already established by the
    // run UPDATE above. The later INSERTs use that same transaction view.
    // `ORDER BY memory_id DESC` is the "stable_tuple" §11.7 asks for: no dedicated
    // ranking score exists yet on `memory_records` (a later task's concern), so this uses the
    // primary key as today's deterministic tie-break — DESC specifically because `memory_id`
    // is UUIDv7 (time-ordered), so newest-first is the closest available proxy for "most
    // recently flagged, rank it first" without inventing a scoring column. This also happens
    // to be the ordering `tests/consolidate_snapshot.rs`'s G11-1/G80-29 fault injection needs:
    // a memory inserted *during* selection is always the newest, so it always sorts first
    // under `DESC` — exactly the "并发插入 60 个更高排名 rows" scenario the spec's own Codex
    // war story describes, without a separate test-only ranking heuristic diverging from what
    // production actually runs.
    //
    // Every eligibility predicate lives in this one WHERE clause, not split across SQL +
    // Rust — `LIMIT $4` must count only rows that will actually become inputs, or a tenant
    // whose newest `max_inputs` candidates are all domain-mismatched/USER_PRIVATE/context-bound
    // materializes fewer inputs than exist, or zero, purely from limit-then-filter ordering:
    //   * `eo.reasoning_domain_id = $2` (§11.8 domain-homogeneous-by-construction, see above)
    //   * `m.visibility_class <> 'USER_PRIVATE'` (§11.6/§11.9: a USER_PRIVATE memory body must
    //     never be summarized into `memory_rollups`, a tenant-scoped-by-default artifact — no
    //     `visibility_user_id` scoping exists on the rollup path this run publishes through
    //     yet, so the only safe stance is "never select it", not "select it and hope the
    //     publish path narrows later")
    //   * `NOT EXISTS ... context_bindings` (§11.8 Pinned/Mandatory) — still fed to `classify`
    //     below as `has_binding` too (always `false` for rows this filter lets through), so
    //     `classify` stays the type-level gate the domain module's rustdoc promises rather
    //     than a dead parameter once the SQL side also excludes it.

    let rows = sqlx::query(&format!(
        "SELECT m.memory_id, m.updated_at, m.content, m.authority_class, m.confidence, \
                m.status, m.superseded_by, \
                EXISTS ( {ACTIVE_NO_AUTO_MUTATE_BINDING} ) AS has_binding \
         FROM private.memory_records m \
         WHERE m.tenant_id = $1 \
           AND m.status = 'active' \
           AND m.visibility_class <> 'USER_PRIVATE' \
           AND ($3::uuid IS NULL OR m.visibility_workspace_id = $3) \
           AND EXISTS ( \
             SELECT 1 FROM private.memory_evidence me \
             JOIN private.evidence_objects eo ON eo.evidence_id = me.evidence_id \
             WHERE me.memory_id = m.memory_id AND eo.reasoning_domain_id = $2 \
           ) \
           AND NOT EXISTS ( {ACTIVE_NO_AUTO_MUTATE_BINDING} ) \
         ORDER BY m.memory_id DESC \
         LIMIT $4"
    ))
    .bind(tenant_id)
    .bind(reasoning_domain_id)
    .bind(workspace_id)
    .bind(max_inputs)
    .fetch_all(&mut *txn)
    .await?;

    let mut accepted = Vec::with_capacity(rows.len());
    let mut ordinal: i32 = 0;
    for row in &rows {
        let candidate = row_to_candidate(row)?;
        let classified = classify(MemoryId(candidate.memory_id), candidate.has_active_binding);
        let ClassifiedMemoryId::Unbound(unbound) = classified else {
            continue; // §11.8: Bound (Pinned/Mandatory-equivalent) never becomes an input.
        };
        sqlx::query(
            "INSERT INTO private.memory_consolidation_inputs \
               (run_id, memory_id, input_version, source_hash, ordinal) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(run_id)
        .bind(candidate.memory_id)
        .bind(candidate.input_version)
        .bind(&candidate.fingerprint)
        .bind(ordinal)
        .execute(&mut *txn)
        .await?;
        accepted.push(MaterializedInput {
            memory_id: AutoMutableMemoryId::from(unbound),
            input_version: candidate.input_version,
            source_hash: candidate.fingerprint,
            ordinal,
        });
        ordinal += 1;
    }

    // §11.7's frozen recipe step "resolve input_snapshot_seq": the xmin of this transaction's
    // own REPEATABLE READ snapshot — taken from inside the snapshot, so it names exactly the
    // point-in-time the SELECT above actually read, not a value resolved before or after it.
    // `pg_snapshot_xmin` returns `xid8`; cast through `text` rather than binding it as a
    // dedicated sqlx type, since this column's Rust/DB shape is already `bigint` (no `xid8`
    // wrapper exists anywhere else in this workspace worth introducing for one column).
    let snapshot_seq: i64 =
        sqlx::query_scalar("SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint")
            .fetch_one(&mut *txn)
            .await?;

    // §11.7 Run State: nothing selected is `SUCCEEDED_NO_OUTPUT`, not `FAILED` — but that
    // terminal transition belongs to `publish_rollup` (it needs to happen only once inference
    // has actually run and produced nothing usable, not merely because selection was empty:
    // an empty *input* set is a valid, if unlikely, `RUNNING` state; skipping straight past
    // `RUNNING` here would let a caller believe inference ran when it never was invoked).
    let running = sqlx::query(
        "UPDATE private.memory_consolidation_runs \
         SET status = 'RUNNING', input_snapshot_seq = $2 \
         WHERE run_id = $1 AND status = 'SELECTING'",
    )
    .bind(run_id)
    .bind(snapshot_seq)
    .execute(&mut *txn)
    .await?;
    if running.rows_affected() == 0 {
        return Err(ConsolidateRepoError::LostRace {
            run_id,
            expected_status: "SELECTING",
        });
    }

    txn.commit().await?;
    Ok(accepted)
}

/// Outcome of [`publish_rollup`] — mirrors the three terminal `ConsolidationRunState`s it can
/// reach (`SUCCEEDED` / `SUCCEEDED_NO_OUTPUT` / `STALE_INPUT`; `FAILED` is the caller's to set
/// on an inference error, before ever calling this function).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishOutcome {
    Published { rollup_id: Uuid },
    NoOutput,
    StaleInput,
}

/// §11.7: "运行完成准备 publish rollup 时，再验证 input memory version/source_hash. 如果任一
/// 输入在期间被 correct/supersede/revoke: STALE_INPUT -> discard unpublished rollup -> enqueue
/// next run." Re-checks every `memory_consolidation_inputs` row for this run against the
/// current `memory_records` state inside one `REPEATABLE READ` transaction before writing
/// anything to `memory_rollups`/`memory_rollup_sources` — a stale run publishes nothing.
/// `REPEATABLE READ` (not the pool's default `READ COMMITTED`) makes the N per-input
/// re-checks below mutually consistent — every `SELECT ... FROM memory_records` in this
/// transaction observes the same snapshot, so a correction landing between input #3's check
/// and input #40's check cannot make #40 look fresh relative to a state #3 already moved past.
/// This does not close the residual commit-window race (a correction landing between this
/// transaction's snapshot and its `COMMIT`) — `role_consolidation_worker` has no `SELECT ...
/// FOR SHARE` grant on `private.memory_records` to hold those rows locked for that long
/// (verified live: `SET ROLE role_consolidation_worker; SELECT ... FOR SHARE` →
/// `permission denied for table memory_records`), so closing it needs a §6.2.2 role-grant
/// decision this task does not own; flagged, not silently treated as solved.
///
/// `sources` must each be a `(memory_id, evidence_id)` pair already recorded as one of this
/// run's own `memory_consolidation_inputs` rows — checked explicitly
/// ([`ConsolidateRepoError::UnknownSource`]), not merely inferred from `AutoMutableMemoryId`'s
/// typestate (that only proves a source came from *some* run's selection, §11.8).
///
/// `sources` empty (no useful rollup, e.g. the LLM found nothing worth grouping) publishes
/// nothing and marks `SUCCEEDED_NO_OUTPUT` — a healthy terminal state (§11.7), never `FAILED`.
///
/// `rollup_class` is checked against every source's *current* `AuthorityClass` (§11.9 "禁止
/// Rollup 自己成为比 source Memory 更高的 Authority") inside this same transaction, using the
/// authority rows this function already fetched for staleness — not the caller's possibly
/// stale view of them. `workspace_id` must be the same value the run was selected with; it
/// decides the published rollup's own visibility (`WORKSPACE_SHARED` when `Some`, matching
/// that workspace; `TENANT_SHARED` when `None` — selection already excludes `USER_PRIVATE`
/// inputs, so no third branch is reachable here, see `select_and_materialize_inputs`).
/// `manifest_hash` (§11.8, `None` on the `SkipToNoOutput` path) is persisted on every terminal
/// transition this function reaches, healthy or not, so a completed run always records what it
/// was bound to (§11.7's frozen recipe step, previously never written at all).
#[allow(clippy::too_many_arguments)]
/// The staleness re-check loop, split out of [`publish_rollup`] purely to keep that function
/// under the workspace's `too_many_lines` lint — see its doc comment for the actual semantics
/// (§11.7 re-validation, `REPEATABLE READ` consistency). Returns `None` the moment any input is
/// found stale (matching the original early-return-on-first-stale behavior exactly); `Some` map
/// otherwise, keyed by every recorded input whose current row was found and whose
/// `authority_class` parsed — used by the §11.9 ceiling check below.
async fn resolve_current_authority_or_stale(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    recorded_inputs: &[sqlx::postgres::PgRow],
) -> Result<Option<std::collections::HashMap<Uuid, AuthorityClass>>, ConsolidateRepoError> {
    let mut current_authority = std::collections::HashMap::with_capacity(recorded_inputs.len());

    for recorded in recorded_inputs {
        let memory_id: Uuid = recorded.try_get("memory_id")?;
        let recorded_version: i64 = recorded.try_get("input_version")?;
        let recorded_hash: Vec<u8> = recorded.try_get("source_hash")?;

        let current = sqlx::query(
            "SELECT updated_at, content, authority_class, confidence, status, superseded_by \
             FROM private.memory_records WHERE memory_id = $1",
        )
        .bind(memory_id)
        .fetch_optional(&mut **txn)
        .await?;

        let is_stale = match &current {
            None => true, // Should not happen (memory_records rows are never deleted, only
            // superseded/revoked in place), but "vanished" is stale by definition either way.
            Some(row) => {
                let updated_at: OffsetDateTime = row.try_get("updated_at")?;
                let content: serde_json::Value = row.try_get("content")?;
                let authority_class: String = row.try_get("authority_class")?;
                let confidence: f32 = row.try_get("confidence")?;
                let status: String = row.try_get("status")?;
                let superseded_by: Option<Uuid> = row.try_get("superseded_by")?;
                let current_version = (updated_at.unix_timestamp_nanos() / 1_000) as i64;
                let current_hash = row_fingerprint(
                    &content,
                    &authority_class,
                    confidence,
                    &status,
                    superseded_by,
                );
                if let Some(class) = authority_class_from_db_str(&authority_class) {
                    current_authority.insert(memory_id, class);
                }
                current_version != recorded_version || current_hash != recorded_hash
            }
        };

        if is_stale {
            return Ok(None);
        }
    }

    Ok(Some(current_authority))
}

/// §11.8 "sources must be this run's own recorded inputs" + §11.9 authority ceiling data prep —
/// pure (no I/O), split out of [`publish_rollup`] to keep that function under the workspace's
/// `too_many_lines` lint.
fn resolve_source_authorities(
    sources: &[(AutoMutableMemoryId, EvidenceId)],
    current_authority: &std::collections::HashMap<Uuid, AuthorityClass>,
) -> Result<Vec<SourceAuthority>, ConsolidateRepoError> {
    let mut source_authorities = Vec::with_capacity(sources.len());
    for (memory_id, evidence_id) in sources {
        let raw_memory_id = memory_id.into_inner().0;
        let Some(class) = current_authority.get(&raw_memory_id).copied() else {
            return Err(ConsolidateRepoError::UnknownSource(raw_memory_id));
        };
        source_authorities.push(SourceAuthority {
            memory_id: memory_id.into_inner(),
            evidence_id: *evidence_id,
            class,
        });
    }
    Ok(source_authorities)
}

#[allow(clippy::too_many_arguments)]
pub async fn publish_rollup(
    pool: &ConsolidationDbPool,
    run_id: Uuid,
    tenant_id: Uuid,
    workspace_id: Option<Uuid>,
    content: serde_json::Value,
    rollup_class: AuthorityClass,
    manifest_hash: Option<&[u8]>,
    sources: &[(AutoMutableMemoryId, EvidenceId)],
) -> Result<PublishOutcome, ConsolidateRepoError> {
    let mut txn = pool.pool().begin().await?;
    // §11.7-style consistency guard for the re-validation loop below — see doc comment.
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ WRITE")
        .execute(&mut *txn)
        .await?;
    set_tenant_local(&mut txn, tenant_id).await?;

    let recorded_inputs = sqlx::query(
        "SELECT memory_id, input_version, source_hash FROM private.memory_consolidation_inputs WHERE run_id = $1",
    )
    .bind(run_id)
    .fetch_all(&mut *txn)
    .await?;

    let Some(current_authority) =
        resolve_current_authority_or_stale(&mut txn, &recorded_inputs).await?
    else {
        sqlx::query(
            "UPDATE private.memory_consolidation_runs \
             SET status = 'STALE_INPUT', manifest_hash = $2, finished_at = now() \
             WHERE run_id = $1",
        )
        .bind(run_id)
        .bind(manifest_hash)
        .execute(&mut *txn)
        .await?;
        txn.commit().await?;
        return Ok(PublishOutcome::StaleInput);
    };

    if sources.is_empty() {
        sqlx::query(
            "UPDATE private.memory_consolidation_runs \
             SET status = 'SUCCEEDED_NO_OUTPUT', manifest_hash = $2, finished_at = now() \
             WHERE run_id = $1",
        )
        .bind(run_id)
        .bind(manifest_hash)
        .execute(&mut *txn)
        .await?;
        txn.commit().await?;
        return Ok(PublishOutcome::NoOutput);
    }

    let source_authorities = resolve_source_authorities(sources, &current_authority)?;
    validate_rollup_before_publish(rollup_class, &source_authorities)
        .map_err(ConsolidateRepoError::AuthorityCeiling)?;

    let output_digest = {
        let mut hasher = Sha256::new();
        hasher.update(content.to_string().as_bytes());
        hasher.finalize().to_vec()
    };

    // §11.6/§11.8 rollup visibility mirrors the run's own input scoping — `workspace_id` is
    // the same value `select_and_materialize_inputs` filtered on, so a rollup over
    // WORKSPACE_SHARED inputs stays WORKSPACE_SHARED, never widening to the whole tenant.
    // `USER_PRIVATE` is not reachable here: selection excludes it entirely (see that
    // function's doc comment), so this table's three-branch CHECK never sees that branch from
    // this call site.
    let (visibility_class, visibility_workspace_id): (&str, Option<Uuid>) = match workspace_id {
        Some(w) => ("WORKSPACE_SHARED", Some(w)),
        None => ("TENANT_SHARED", None),
    };

    let rollup_id: Uuid = sqlx::query_scalar(
        "INSERT INTO private.memory_rollups \
           (tenant_id, run_id, content, authority_class, visibility_class, visibility_workspace_id) \
         VALUES ($1, $2, $3, $4, $5, $6) RETURNING rollup_id",
    )
    .bind(tenant_id)
    .bind(run_id)
    .bind(&content)
    .bind(authority_class_to_db_str(rollup_class))
    .bind(visibility_class)
    .bind(visibility_workspace_id)
    .fetch_one(&mut *txn)
    .await?;

    // §11.6: "Rollup 必须能展开回全部 source memory_id / evidence_id" — every `(memory_id,
    // evidence_id)` pair the caller vouches for becomes one closure row; the table's own FKs
    // (`migrations/0005_private_pipeline.sql`) reject anything not already a real Memory/
    // Evidence, so a bogus source fails this INSERT rather than publishing silently.
    for source in &source_authorities {
        sqlx::query(
            "INSERT INTO private.memory_rollup_sources (rollup_id, memory_id, evidence_id) VALUES ($1, $2, $3)",
        )
        .bind(rollup_id)
        .bind(source.memory_id.0)
        .bind(source.evidence_id.0)
        .execute(&mut *txn)
        .await?;
    }

    sqlx::query(
        "UPDATE private.memory_consolidation_runs \
         SET status = 'SUCCEEDED', output_digest = $2, manifest_hash = $3, finished_at = now() \
         WHERE run_id = $1",
    )
    .bind(run_id)
    .bind(&output_digest)
    .bind(manifest_hash)
    .execute(&mut *txn)
    .await?;

    txn.commit().await?;
    Ok(PublishOutcome::Published { rollup_id })
}

#[cfg(test)]
mod binding_predicate_tests {
    //! §11.8 × §25.4：`ACTIVE_NO_AUTO_MUTATE_BINDING` 的真值表。
    //!
    //! **这组测试存在的理由是它此前不存在。** migration 0102 之前，两处判据各自写着
    //! 「存在任意一行 binding 即冻结」，而 `private.context_bindings` 恰好零行，所以
    //! 任何断言都恒真——两个 bug（已撤销的 binding 永久冻结、SUPPLEMENTAL 误挡
    //! consolidation）会在第一条真 binding 落库那天同时上线，且现有测试一条都不会红。
    //!
    //! 打的是**真实谓词文本本身**（`ACTIVE_NO_AUTO_MUTATE_BINDING`），不复制第二份 SQL：
    //! 复制一份就等于把刚收敛掉的漂移又放回来。

    use super::ACTIVE_NO_AUTO_MUTATE_BINDING;
    use humaux_testkit::{ExternalDep, skip_or_fail};
    use postgres::{Client, NoTls};
    use sqlx::types::Uuid;

    /// 播一个租户 + 一条**带 evidence 链**的 memory，返回 (tenant_id, memory_id)。
    ///
    /// §8.6 有一条延迟到 COMMIT 的约束触发器 `check_memory_has_evidence`：任何 Memory 在
    /// 提交时必须已有 `private.memory_evidence` 链接，否则报 orphan Memory。`postgres`
    /// 的 `Client::execute` 是自动提交，单独插一条 memory 会当场被它顶回来——所以
    /// memory 与它的链接必须在**同一个显式事务**里。
    fn seed(admin: &mut Client) -> (Uuid, Uuid) {
        let tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"binding_predicate_tests throwaway tenant"],
            )
            .expect("insert tenant")
            .get(0);
        let reasoning_domain_id: Uuid = admin
            .query_one(
                "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
                 VALUES ($1, 'binding_predicate_tests domain') RETURNING reasoning_domain_id",
                &[&tenant_id],
            )
            .expect("insert reasoning domain")
            .get(0);
        // EVENT-kind evidence 的最小配方，与 `tests/processing_runs_fingerprint_rerun.rs`
        // 逐字同源。
        let evidence_id: Uuid = admin
            .query_one(
                "INSERT INTO private.evidence_objects \
                   (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                    visibility_class, reasoning_domain_id) \
                 VALUES ($1, 'EVENT', $2, 'INTERNAL', 'DirectUserInput', 'TENANT_SHARED', $3) \
                 RETURNING evidence_id",
                &[&tenant_id, &vec![0u8; 32], &reasoning_domain_id],
            )
            .expect("insert evidence")
            .get(0);
        admin
            .execute(
                "INSERT INTO private.events (event_id, event_kind, payload) \
                 VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)",
                &[&evidence_id],
            )
            .expect("insert event");

        let confidence: f32 = 0.9;
        let mut txn = admin.transaction().expect("begin seed txn");
        let memory_id: Uuid = txn
            .query_one(
                "INSERT INTO private.memory_records \
                   (tenant_id, memory_type, content, visibility_class, \
                    authority_class, confidence, status, asserted_at) \
                 VALUES ($1, 'NOTE', $2, 'TENANT_SHARED', 'PrivateKnowledge', $3, 'active', now()) \
                 RETURNING memory_id",
                &[
                    &tenant_id,
                    &serde_json::json!({"fixture": "binding_predicate_tests"}),
                    &confidence,
                ],
            )
            .expect("insert memory")
            .get(0);
        txn.execute(
            "INSERT INTO private.memory_evidence (memory_id, evidence_id, role) \
             VALUES ($1, $2, 'PRIMARY')",
            &[&memory_id, &evidence_id],
        )
        .expect("link memory to evidence (§8.6)");
        txn.commit().expect("commit seed txn");

        (tenant_id, memory_id)
    }

    /// 对一条 memory 跑真实谓词。
    fn predicate_holds(admin: &mut Client, memory_id: &Uuid) -> bool {
        admin
            .query_one(
                &format!(
                    "SELECT EXISTS ( {ACTIVE_NO_AUTO_MUTATE_BINDING} ) \
                     FROM private.memory_records m WHERE m.memory_id = $1"
                ),
                &[memory_id],
            )
            .expect("run the real §11.8 predicate")
            .get(0)
    }

    #[test]
    fn only_active_mandatory_or_pinned_bindings_freeze_a_memory() {
        let Ok(dsn) = std::env::var("HUMAUX_TEST_PG_DSN") else {
            skip_or_fail(
                "only_active_mandatory_or_pinned_bindings_freeze_a_memory",
                "missing object: Postgres DSN (HUMAUX_TEST_PG_DSN not set)",
                ExternalDep::Postgres,
            );
            return;
        };
        let Ok(mut admin) = Client::connect(&dsn, NoTls) else {
            skip_or_fail(
                "only_active_mandatory_or_pinned_bindings_freeze_a_memory",
                "missing object: live Postgres",
                ExternalDep::Postgres,
            );
            return;
        };
        // 0102 未应用时列不存在，谓词会语法错——那不是「不适用」，是环境没建好。
        let migrated: bool = admin
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema='private' AND table_name='context_bindings'
                     AND column_name='mode')",
                &[],
            )
            .expect("probe 0102")
            .get(0);
        if !migrated {
            skip_or_fail(
                "only_active_mandatory_or_pinned_bindings_freeze_a_memory",
                "missing object: private.context_bindings.mode — run `cargo xtask migrate` \
                 (migrations/0102_context_bindings_mode_scope.sql)",
                ExternalDep::Postgres,
            );
            return;
        }

        let (tenant_id, memory_id) = seed(&mut admin);

        // 无 binding：不冻结。
        assert!(
            !predicate_holds(&mut admin, &memory_id),
            "没有任何 binding 的 memory 不该被判为冻结"
        );

        // 逐档验证。每次只留一条 binding，避免「某一档漏判但被另一档掩盖」。
        for (mode, revoked, expect_frozen, why) in [
            ("MANDATORY", false, true, "MANDATORY 未撤销 ⇒ 冻结（§11.8）"),
            ("PINNED", false, true, "PINNED 未撤销 ⇒ 冻结（§11.8）"),
            (
                "SUPPLEMENTAL",
                false,
                false,
                "SUPPLEMENTAL 是普通补充位，**不**构成冻结——判据写成「存在即冻结」时这条会红",
            ),
            (
                "MANDATORY",
                true,
                false,
                "已撤销的 binding 不再冻结——判据漏掉 revoked_at IS NULL 时这条会红",
            ),
        ] {
            admin
                .execute(
                    "DELETE FROM private.context_bindings WHERE memory_id = $1",
                    &[&memory_id],
                )
                .expect("clear bindings between cases");
            admin
                .execute(
                    "INSERT INTO private.context_bindings
                       (tenant_id, memory_id, mode, created_by, revoked_at)
                     VALUES ($1, $2, $3, $1, CASE WHEN $4 THEN now() ELSE NULL END)",
                    &[&tenant_id, &memory_id, &mode, &revoked],
                )
                .expect("insert binding case");
            assert_eq!(
                predicate_holds(&mut admin, &memory_id),
                expect_frozen,
                "{why}（mode={mode} revoked={revoked}）"
            );
        }

        // 清理：binding 不是 append-only，可以删。
        let _ = admin.execute(
            "DELETE FROM private.context_bindings WHERE memory_id = $1",
            &[&memory_id],
        );
        let _ = admin.execute(
            "DELETE FROM private.memory_records WHERE memory_id = $1",
            &[&memory_id],
        );
        let _ = admin.execute(
            "DELETE FROM control.tenants WHERE tenant_id = $1",
            &[&tenant_id],
        );
    }
}
