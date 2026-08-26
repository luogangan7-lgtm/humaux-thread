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
    "/../docs/architecture/Baseline_2.8.md"
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
];

fn frozen_role_set() -> BTreeSet<&'static str> {
    RUNTIME_ROLES
        .iter()
        .chain(NON_RUNTIME_ROLES.iter())
        .copied()
        .collect()
}

// ============================================================================
// §6.2.2 — table-level/column-level grant matrix (the 14 point-named tables). One
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
    cell!("ops.outbox", "role_gateway", ["INSERT", "SELECT"]),
    cell!("ops.outbox", "role_private_worker", ["SELECT", "UPDATE"]),
    cell!("ops.outbox", "role_public_worker", ["SELECT", "UPDATE"]),
    cell!("ops.outbox", "role_retrieval_worker", ["SELECT", "UPDATE"]),
    cell!("ops.outbox", "role_maintenance", ["SELECT"]),
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
    cell!(
        "ops.jobs",
        "role_public_worker",
        ["SELECT", "INSERT", "UPDATE"]
    ),
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
];

/// The §6.2.2 column set — "T" in the §48.2 "表集合派生" check's `S \ T == ∅`. Single
/// source shared by [`check_grant_equality`] (which tables get matrix treatment vs.
/// domain-default treatment) and [`check_table_set_derivation`] (what S is compared
/// against) — never hand-recounted a second time (§6.2.2: "S 不再手抄张数").
fn named_tables() -> BTreeSet<&'static str> {
    MATRIX.iter().map(|c| c.table).collect()
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
/// which of the 14 named tables it currently can't independently re-derive) but has zero
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
            if boundary_ok
                && let Some(name) = next_identifier(text, idx)
                && let Some((_, table)) = name.split_once('.')
                && !table.is_empty()
            {
                out.push(name);
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
/// the 14 §6.2.2-named tables' own (unambiguous) short names, not a free-text guess — a
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

/// The `S \ T == ∅` half of [`check_table_set_derivation`], factored out so tests can
/// exercise the S_write/S_grant scan against a synthetic one-table spec fixture without
/// also tripping the real-[`MATRIX`]-drift assertion (which always reads the real,
/// 14-table [`MATRIX`] and would spuriously fail against a deliberately tiny fixture).
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
    let derivation = derivation_check_from_text(spec_text);
    if derivation.status != GateStatus::Pass {
        return derivation;
    }

    let t = parse_matrix_header_tables(spec_text);
    let matrix_t = named_tables();
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
/// eight-name set exactly (extra/missing/renamed all red), and per §48.2's opening block
/// each of the eight must independently be `NOT SUPERUSER` / `NOT BYPASSRLS`.
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
            "extra login role(s) beyond the frozen eight: {extra:?}"
        ));
    }

    if problems.is_empty() {
        pass(
            "角色全集相等",
            "pg_roles login∧¬superuser set == §6.2.0 frozen eight, all NOT SUPERUSER/NOBYPASSRLS",
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

    // §6.2.2's own row for role_migration_owner: `owner` on all 14 tables. Not a MATRIX
    // cell (owner isn't modeled as a grant) — checked against pg_tables directly, the
    // only place ownership shows up.
    let actual_owner = owners.get(&(schema.to_string(), tname.to_string()));
    if actual_owner.map(String::as_str) != Some(OWNER_ROLE) {
        mismatches.push(format!(
            "{table}: expected owner {OWNER_ROLE:?}, actual {actual_owner:?}"
        ));
    }

    for &role in non_owner_roles {
        let cell = MATRIX.iter().find(|c| c.table == table && c.role == role);
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
         WHERE table_type = 'BASE TABLE' AND table_schema || '.' || table_name = ANY($1)",
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
        "SELECT schemaname, tablename, tableowner FROM pg_tables",
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
        .chain(["role_batch_issuer", "role_maintenance"].iter())
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
                "missing object: table(s) {missing_tables:?} from §6.2.2 do not exist yet \
                 ({} of {} named tables compared clean)",
                t.len() - missing_tables.len(),
                t.len()
            ),
        );
    }
    pass(
        "授权逐条相等",
        format!(
            "{} named tables × {} non-owner roles all match §6.2.2, all owned by {OWNER_ROLE}",
            t.len(),
            non_owner_roles.len()
        ),
    )
}

// ============================================================================
// §6.2.1 域级默认授权 — tables §6.2.2 doesn't name (§48.2: "枚举面 = 全部 schema 的全部
// 表", not just the 14 point-named ones).
// ============================================================================

/// §6.2.1: every non-owner role's table-level grants on every BASE TABLE **not** named
/// by §6.2.2 must equal that role's domain default for the table's schema
/// ([`DOMAIN_DEFAULT`]). Without this, an over-grant or under-grant on any of the tables
/// outside the 14-table matrix — the majority of the schema — has no check at all;
/// [`check_grant_equality`] only ever looks at `T`.
pub fn check_domain_default_grants(client: &mut impl GenericClient) -> GateResult {
    let t = named_tables();
    let all_tables: Vec<(String, String)> = match client.query(
        "SELECT table_schema, table_name FROM information_schema.tables \
         WHERE table_type = 'BASE TABLE' AND table_schema = ANY($1)",
        &[&SCHEMAS.to_vec()],
    ) {
        Ok(rows) => rows.iter().map(|r| (r.get(0), r.get(1))).collect(),
        Err(e) => {
            return fail(
                "域默认授权",
                format!("query information_schema.tables failed: {e}"),
            );
        }
    };
    let table_grants = match fetch_table_grants(client) {
        Ok(g) => g,
        Err(e) => return fail("域默认授权", format!("query role_table_grants failed: {e}")),
    };

    let mut checked = 0usize;
    let mut problems = Vec::new();
    for (schema, tname) in &all_tables {
        let full = format!("{schema}.{tname}");
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
            "missing object: no BASE TABLE outside §6.2.2's 14 named tables exists yet",
        );
    }
    if problems.is_empty() {
        pass(
            "域默认授权",
            format!("{checked} non-named table(s) × 7 non-owner roles all match §6.2.1"),
        )
    } else {
        fail("域默认授权", problems.join("; "))
    }
}

// ============================================================================
// Item 4 — §6.2.1 全域禁动词
// ============================================================================

/// §6.2.1 "全域硬约束，无例外": the seven non-owner roles ([`NON_OWNER_ROLES`]) get no
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
    fn table_set_derivation_passes_on_clean_matrix_only_spec() {
        let spec = matrix_block();
        let r = derivation_check_from_text(spec);
        assert_eq!(r.status, GateStatus::Pass, "{}", r.detail);
    }

    /// T must come from the doc's own header row, not the hardcoded [`MATRIX`]: parsing
    /// the fixture's one-table header must yield exactly that table, independent of the
    /// real (14-table) MATRIX constant.
    #[test]
    fn parse_matrix_header_tables_reads_the_doc_not_the_rust_matrix() {
        let t = parse_matrix_header_tables(matrix_block());
        assert_eq!(
            t,
            BTreeSet::from(["private.ingest_tickets".to_string()]),
            "{t:?}"
        );
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

    macro_rules! txn_or_skip {
        ($client:ident, $txn:ident) => {
            let Ok(dsn) = std::env::var(DSN_ENV) else {
                eprintln!("rls-check test: not_applicable — {DSN_ENV} unset, skipping");
                return;
            };
            let Ok(mut $client) = Client::connect(&dsn, NoTls) else {
                eprintln!("rls-check test: not_applicable — cannot reach Postgres, skipping");
                return;
            };
            let mut $txn = $client
                .transaction()
                .expect("open fault-injection transaction");
        };
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
}
