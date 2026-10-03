//! `adapters::tests::exact_completeness_eval` — exact_completeness eval harness (§55.3 benchset declaration for the
//!   `exact_completeness` set; owner=Retrieval, Phase 6+).
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-projection, humaux-retrieval, humaux-testkit, postgres,
//!   serde_json, sha2, tokio, uuid]; services=[PostgreSQL(any) r=[control.retrieval_predicates, ops.commit_seq_seq, public.pool]
//!   w=[control.memberships, control.private_reasoning_domains, control.tenants, control.users,
//!   control.workspace_memberships, control.workspaces, ops.jobs, ops.outbox, private.events,
//!   private.evidence_objects, private.memory_evidence, private.memory_records, projection.stream_log],
//!   PostgreSQL(role_gateway),
//!   PostgreSQL(role_maintenance)]; env=[CARGO_MANIFEST_DIR, HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_TEST_PG_DSN];
//!   modules=[adapters::exact_census, adapters::forget_repo, adapters::postgres, domain::error, domain::identity,
//!   domain::ids, humaux-testkit, projection::stream, retrieval::completeness, retrieval::envelope,
//!   retrieval::planner, retrieval::predicate_registry]
//! Called-by: [cargo-test]
//! Invariants: [runs the real probe -> decide -> census -> classify chain per fixture case with exact-equality
//!   readouts and the §55.3 spread/resolution measurements; a missing dependency goes through skip_or_fail]
//! Spec: Baseline §55.3; §23.1; §23.4; §79.2; ADR-0005
//!
//! Runs the real chain per fixture case — [`humaux_adapters::exact_census::
//! probe_predicate_inputs`] → `planner::decide` → [`humaux_adapters::exact_census::
//! exact_enumerate`] → the component-only `classify_for_witness` label path — against
//! `evals/exact_completeness/dataset.tsv`, comparing census/classifier readouts by exact equality.
//! This harness deliberately does not produce a final Envelope or emit a completeness metric:
//! its closed fixture ledger has no request-bound provenance, pipeline/A2, or mandatory Context.
//! (`decision_depth = exact_equality`).
//!
//! Three §55.3 measurements live here, same shape as `planner_predicate_eval.rs`:
//! - judgment: [`all_cases_match_expected_readouts`] (the battery itself);
//! - `spread_tol`: [`repeated_runs_have_zero_spread`] — the battery runs 3 times against
//!   fresh workspaces, per-layer pass counts compared; 0 is *observed*, not asserted a
//!   priori (the real path holds a transaction snapshot + the §23.1② tombstone overlay);
//! - `resolution`: [`resolution_is_measured_via_a_real_second_system_diff`] — a real,
//!   runnable second system (the census with its overlay mis-scoped to a decoy stream, i.e.
//!   effectively no overlay — the regression §23.4 words as "把 overlay 退回成「只在
//!   `count()` 里减」" / skipping it entirely) diffed case-by-case on the same fixture set.
//!
//! Three-state discipline (§79.2): `testkit::skip_or_fail`; CI declares
//! `HUMAUX_REQUIRE_DB=1` so a skip there fails (ADR-0005).
//!
//! Sync `postgres::Client` calls stay strictly outside `block_on` sections (this crate's own
//! G5/G80-31 nested-runtime lesson) — the battery is therefore phased: sync seed → async
//! cases through the tombstone boundary → sync authority purge → async purge-boundary case.

use std::collections::BTreeMap;
use std::path::PathBuf;

use humaux_adapters::exact_census::{exact_enumerate, probe_predicate_inputs};
use humaux_adapters::forget_repo;
use humaux_adapters::postgres::{MaintenanceDbPool, RuntimeDbPool};
use humaux_domain::identity::{AuthorizationScope, BoundedSet, PrincipalId};
use humaux_domain::ids::{TenantId, UserId, WorkspaceId};
use humaux_projection::stream::StreamKey;
use humaux_retrieval::completeness::{
    CensusResult, ExactEnumeration, LedgerClosure, classify_for_witness, ledger,
    retrieval_completeness_total_count,
};
use humaux_retrieval::envelope::LaneStatus;
use humaux_retrieval::planner::{PlannerDecision, decide};
use humaux_retrieval::predicate_registry::{PredicateEntry, PredicateRow, load_registry};
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};
use uuid::Uuid;

const NAME: &str = "exact_completeness_eval";

fn dsn_as_role(dsn: &str, role: &str) -> String {
    // ADR-0059 D-D: a real login as `role`, its password from HUMAUX_ROLE_PASSWORD_<SUFFIX>.
    humaux_testkit::role_login_dsn(dsn, role, |name| std::env::var(name).ok())
        .unwrap_or_else(|missing| panic!("missing object: {missing} (ADR-0059 D-D)"))
}

fn verified_maintenance_dsn() -> Option<String> {
    let Ok(dsn) = std::env::var("HUMAUX_MAINTENANCE_PG_DSN") else {
        skip_or_fail(
            NAME,
            "missing object: role_maintenance PostgreSQL DSN",
            ExternalDep::Postgres,
        );
        return None;
    };
    // dep: PostgreSQL(any) — opens the role-scoped connection for `verified_maintenance_dsn`
    let Ok(mut probe) = Client::connect(&dsn, NoTls) else {
        skip_or_fail(
            NAME,
            "missing object: role_maintenance PostgreSQL login",
            ExternalDep::Postgres,
        );
        return None;
    };
    let role_ok: bool = probe
        .query_one(
            "SELECT current_user='role_maintenance' AND session_user='role_maintenance' \
             AND NOT (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user)",
            &[],
        )
        .expect("probe role_maintenance session identity")
        .get(0);
    if !role_ok {
        skip_or_fail(
            NAME,
            "invalid object: HUMAUX_MAINTENANCE_PG_DSN must be a non-bypass role_maintenance LOGIN",
            ExternalDep::Postgres,
        );
        return None;
    }
    Some(dsn)
}

// ============================================================================
// dataset
// ============================================================================

fn dataset_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../evals/exact_completeness/dataset.tsv")
}

fn manifest_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../evals/exact_completeness/manifest.toml")
}

#[derive(Debug, Clone)]
struct DatasetRow {
    case_id: String,
    layer: String,
    expect_class: String,
    expect_reason: Option<String>,
    expect_predicate_id: Option<String>,
    expect_total: Option<u64>,
    expect_returned: Option<u64>,
    expect_excluded_secret: Option<u64>,
    expect_truncated: Option<bool>,
    expect_lower_bound: Option<u64>,
}

fn opt(s: &str) -> Option<String> {
    (s != "-").then(|| s.to_string())
}

fn read_dataset_rows() -> Vec<DatasetRow> {
    let text = std::fs::read_to_string(dataset_path()).expect("dataset.tsv must exist");
    let mut rows = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with("case_id\t") {
            continue;
        }
        let f: Vec<&str> = line.split('\t').collect();
        assert_eq!(f.len(), 10, "dataset row must have 10 columns: {line}");
        rows.push(DatasetRow {
            case_id: f[0].to_string(),
            layer: f[1].to_string(),
            expect_class: f[2].to_string(),
            expect_reason: opt(f[3]),
            expect_predicate_id: opt(f[4]),
            expect_total: opt(f[5]).map(|v| v.parse().unwrap()),
            expect_returned: opt(f[6]).map(|v| v.parse().unwrap()),
            expect_excluded_secret: opt(f[7]).map(|v| v.parse().unwrap()),
            expect_truncated: opt(f[8]).map(|v| v.parse().unwrap()),
            expect_lower_bound: opt(f[9]).map(|v| v.parse().unwrap()),
        });
    }
    rows
}

// ============================================================================
// fixture
// ============================================================================

struct Fixture {
    admin: Client,
    tenant_id: Uuid,
    /// The requesting user (ACTIVE membership) — 0012's WORKSPACE_SHARED branch admits rows
    /// only for a session carrying `humaux.user_id` with an ACTIVE membership; a census
    /// without it sees a different (narrower) authorized universe.
    user_id: Uuid,
    other_user_id: Uuid,
    reasoning_domain_id: Uuid,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Card-31 pattern (card 33 leak fix): each seeded EVIDENCE_ACCEPTED row or PRIMARY link
        // enqueues a DERIVED_* job (0164 trigger). Jobs go first, in one batch with the data rows, and
        // a failure is printed; the tenant row goes in a separate best-effort batch, so a refused tenant
        // delete (append-only audit rows, a missed child table) can no longer roll the job delete back.
        if let Err(error) = self.admin.batch_execute(&format!(
            "DELETE FROM ops.jobs WHERE tenant_id = '{0}'; \
             DELETE FROM projection.stream_log WHERE tenant_id = '{0}'; \
             DELETE FROM ops.outbox WHERE tenant_id = '{0}'; \
             DELETE FROM private.memory_evidence WHERE memory_id IN \
               (SELECT memory_id FROM private.memory_records WHERE tenant_id = '{0}'); \
             DELETE FROM private.memory_records WHERE tenant_id = '{0}'; \
             DELETE FROM private.events WHERE event_id IN \
               (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id = '{0}'); \
             DELETE FROM private.evidence_objects WHERE tenant_id = '{0}'; \
             DELETE FROM control.workspaces WHERE tenant_id = '{0}'; \
             DELETE FROM control.memberships WHERE tenant_id = '{0}'; \
             DELETE FROM control.private_reasoning_domains WHERE tenant_id = '{0}';",
            self.tenant_id,
        )) {
            eprintln!(
                "exact_completeness_eval cleanup failed for tenant {}: {error}",
                self.tenant_id
            );
        }
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM control.tenants WHERE tenant_id = '{0}'; \
             DELETE FROM control.users WHERE user_id IN ('{1}', '{2}');",
            self.tenant_id, self.user_id, self.other_user_id,
        ));
    }
}

fn setup() -> Option<(Fixture, String, String)> {
    let Ok(dsn) = std::env::var("HUMAUX_TEST_PG_DSN") else {
        skip_or_fail(NAME, "missing object: Postgres DSN", ExternalDep::Postgres);
        return None;
    };
    // dep: PostgreSQL(any) — opens the role-scoped connection for `setup`
    let Ok(mut admin) = Client::connect(&dsn, NoTls) else {
        skip_or_fail(NAME, "missing object: live Postgres", ExternalDep::Postgres);
        return None;
    };
    let maintenance_dsn = verified_maintenance_dsn()?;
    // The registry row is the structure the whole gauge sits on — without migration 0077
    // there is no set to measure at all (legal NA per ADR-0006's carve-out for the table the
    // tested judgment lives in, not the judgment itself).
    let migrated: bool = admin
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM control.retrieval_predicates \
             WHERE predicate_id = 'rejected_decisions_v1')",
            &[],
        )
        .expect("query retrieval predicate migration")
        .get(0);
    if !migrated {
        skip_or_fail(
            NAME,
            "missing object: control.retrieval_predicates row rejected_decisions_v1 — run \
             `cargo xtask migrate` (migrations/0077)",
            ExternalDep::Postgres,
        );
        return None;
    }

    let tenant_id: Uuid = admin
        .query_one(
            "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
            &[&"exact_completeness_eval throwaway tenant"],
        )
        .expect("insert throwaway tenant")
        .get(0);
    let reasoning_domain_id: Uuid = admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
             VALUES ($1, 'exact_completeness_eval domain') RETURNING reasoning_domain_id",
            &[&tenant_id],
        )
        .expect("insert private reasoning domain")
        .get(0);
    let user_id: Uuid = admin
        .query_one(
            "INSERT INTO control.users (state) VALUES ('ACTIVE') RETURNING user_id",
            &[],
        )
        .expect("insert requesting user")
        .get(0);
    admin
        .execute(
            "INSERT INTO control.memberships (tenant_id, user_id, role, state) \
             VALUES ($1, $2, 'MEMBER', 'ACTIVE')",
            &[&tenant_id, &user_id],
        )
        .expect("insert requesting membership");
    let other_user_id: Uuid = admin
        .query_one(
            "INSERT INTO control.users (state) VALUES ('ACTIVE') RETURNING user_id",
            &[],
        )
        .expect("insert hidden-source owner")
        .get(0);

    Some((
        Fixture {
            admin,
            tenant_id,
            user_id,
            other_user_id,
            reasoning_domain_id,
        },
        dsn,
        maintenance_dsn,
    ))
}

fn new_workspace(f: &mut Fixture, name: &str) -> Uuid {
    let workspace_id: Uuid = f
        .admin
        .query_one(
            "INSERT INTO control.workspaces (tenant_id, name) VALUES ($1, $2) \
             RETURNING workspace_id",
            &[&f.tenant_id, &name.to_string()],
        )
        .expect("insert workspace")
        .get(0);
    // ADR-0035 (card 13): 0163 re-points the WORKSPACE_SHARED visibility arm from
    // control.memberships (tenant membership) to an ACTIVE control.workspace_memberships row
    // for the row's OWN workspace. Every workspace this harness creates is later read as
    // `f.user_id` (see `authorization_for`), so it is made an ACTIVE MEMBER here.
    f.admin
        .execute(
            "INSERT INTO control.workspace_memberships (tenant_id, workspace_id, user_id, role, state) \
             VALUES ($1, $2, $3, 'MEMBER', 'ACTIVE')",
            &[&f.tenant_id, &workspace_id, &f.user_id],
        )
        .expect("insert workspace membership");
    workspace_id
}

/// One REJECTION memory with its §8.6 evidence chain, workspace-visible (the registry
/// predicate's scope face). Returns `(memory_id, evidence_id)`.
fn seed_rejection(
    f: &mut Fixture,
    ws: Uuid,
    secret: bool,
    superseded_by: Option<Uuid>,
) -> (Uuid, Uuid) {
    let data_class = if secret {
        "SECRET_MATERIAL"
    } else {
        "INTERNAL"
    };
    let status = if superseded_by.is_some() {
        "superseded"
    } else {
        "active"
    };
    let mut txn = f.admin.transaction().expect("begin");
    let evidence_id: Uuid = txn
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                visibility_class, reasoning_domain_id) \
             VALUES ($1, 'EVENT', $2, $3, 'DirectUserInput', 'TENANT_SHARED', $4) \
             RETURNING evidence_id",
            &[
                &f.tenant_id,
                &{
                    // Unique payload hash per evidence row (real evidence never shares one).
                    let mut h = vec![0u8; 32];
                    h[..16].copy_from_slice(Uuid::now_v7().as_bytes());
                    h
                },
                &data_class,
                &f.reasoning_domain_id,
            ],
        )
        .expect("insert evidence")
        .get(0);
    txn.execute(
        "INSERT INTO private.events (event_id, event_kind, payload) \
         VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)",
        &[&evidence_id],
    )
    .expect("insert event");
    let memory_id: Uuid = txn
        .query_one(
            "INSERT INTO private.memory_records \
               (tenant_id, memory_type, content, visibility_class, visibility_workspace_id, \
                authority_class, confidence, status, asserted_at, superseded_by, superseded_at) \
             VALUES ($1, 'REJECTION', $2, 'WORKSPACE_SHARED', $3, 'ProjectDecision', 0.9, \
                     $4, now(), $5, CASE WHEN $5::uuid IS NULL THEN NULL ELSE now() END) \
             RETURNING memory_id",
            &[
                &f.tenant_id,
                &serde_json::json!({"fixture": "exact_completeness_eval"}),
                &ws,
                &status,
                &superseded_by,
            ],
        )
        .expect("insert memory")
        .get(0);
    txn.execute(
        "INSERT INTO private.memory_evidence (memory_id, evidence_id, role, grounding_mode) \
         VALUES ($1, $2, 'PRIMARY', 'SNAPSHOT')",
        &[&memory_id, &evidence_id],
    )
    .expect("link evidence (§8.6)");
    txn.commit().expect("commit");
    (memory_id, evidence_id)
}

fn add_hidden_backing_source(f: &mut Fixture, memory_id: Uuid) -> Uuid {
    let mut txn = f.admin.transaction().expect("begin hidden source");
    let mut payload_sha256 = vec![0_u8; 32];
    payload_sha256[..16].copy_from_slice(Uuid::now_v7().as_bytes());
    let evidence_id: Uuid = txn
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                visibility_class, visibility_user_id, reasoning_domain_id) \
             VALUES ($1, 'EVENT', $2, 'INTERNAL', 'DirectUserInput', 'USER_PRIVATE', $3, $4) \
             RETURNING evidence_id",
            &[
                &f.tenant_id,
                &payload_sha256,
                &f.other_user_id,
                &f.reasoning_domain_id,
            ],
        )
        .expect("insert hidden evidence")
        .get(0);
    txn.execute(
        "INSERT INTO private.events (event_id, event_kind, payload) \
         VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)",
        &[&evidence_id],
    )
    .expect("insert hidden event");
    txn.execute(
        "INSERT INTO private.memory_evidence (memory_id, evidence_id, role, grounding_mode) \
         VALUES ($1, $2, 'SUPPORTING', 'SNAPSHOT')",
        &[&memory_id, &evidence_id],
    )
    .expect("link hidden source");
    txn.commit().expect("commit hidden source");
    evidence_id
}

/// §23.4 tour seeding: evidence + memory + the stream identity the overlay joins through —
/// `ops.outbox(evidence_id, stream_seq)` (§60's same-transaction mapping) and a settled
/// `projection.stream_log` row.
fn seed_tour_memory(
    f: &mut Fixture,
    ws: Uuid,
    key: &StreamKey,
    stream_log_seq: i64,
    outbox_stream_seq: i64,
    commit_seq: i64,
) -> (Uuid, Uuid) {
    let (memory_id, evidence_id) = seed_rejection(f, ws, false, None);
    f.admin
        .execute(
            "INSERT INTO ops.outbox (tenant_id, commit_seq, stream_seq, event_type, evidence_id) \
             VALUES ($1, $2, $3, 'EVIDENCE_ACCEPTED', $4)",
            &[&f.tenant_id, &commit_seq, &outbox_stream_seq, &evidence_id],
        )
        .expect("insert outbox");
    f.admin
        .execute(
            "INSERT INTO projection.stream_log \
               (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, \
                stream_seq, commit_seq, state, settled_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'DONE', now())",
            &[
                &f.tenant_id,
                &key.scope_kind,
                &key.scope_id,
                &key.domain,
                &key.projection_kind,
                &key.projection_version,
                &stream_log_seq,
                &commit_seq,
            ],
        )
        .expect("insert stream_log");
    (memory_id, evidence_id)
}

// ============================================================================
// running one case
// ============================================================================

/// Reads the real registry row from the DB and validates it through `load_registry` — the
/// same §50 fail-loud door production takes; no `PredicateEntry` literal (its fields are
/// private for exactly this reason).
fn real_registry_entry(f: &mut Fixture) -> PredicateEntry {
    let row = f
        .admin
        .query_one(
            "SELECT predicate_id, sql_predicate, required_columns, enumerable_scope, \
                    surface_patterns, owner_module \
             FROM control.retrieval_predicates WHERE predicate_id = 'rejected_decisions_v1'",
            &[],
        )
        .expect("registry row");
    let raw = PredicateRow {
        predicate_id: row.get(0),
        sql_predicate: row.get(1),
        required_columns: row.get(2),
        enumerable_scope: row.get(3),
        surface_patterns: row.get(4),
        owner_module: row.get(5),
    };
    load_registry(vec![raw]).expect("seeded registry row must pass §50 validation")[0].clone()
}

fn closed_ledger() -> LedgerClosure {
    // classify() reads A1 only; the A2 point reading is irrelevant here (ADR-0057 D-A).
    ledger::close(
        ledger::LedgerReads {
            expected: 1,
            done: 1,
            deleted: 0,
            skipped: 0,
            open_gaps: 0,
            pending: 0,
        },
        ledger::ProjectionReads::default(),
    )
}

/// The quantifier query matching migration 0077's seeded surface pattern.
const REAL_QUERY: &str = "我们之前否掉过哪些方案";

fn authorization_for(tenant_id: Uuid, user_id: Uuid, workspace_id: Uuid) -> AuthorizationScope {
    AuthorizationScope::new(
        TenantId(tenant_id),
        PrincipalId(user_id),
        Some(UserId(user_id)),
        BoundedSet::new([WorkspaceId(workspace_id)]).expect("one authorized workspace"),
    )
}

#[derive(Debug, Clone)]
struct ComponentExactReport {
    predicate_id: String,
    total: u64,
    returned: u64,
    coverage: f64,
    truncated: bool,
    excluded_secret: u64,
}

/// Classifier/census result for this component harness, never a final Envelope outcome.
#[derive(Debug, Clone)]
struct ComponentExactOutcome {
    class_label: &'static str,
    reason_label: Option<&'static str>,
    exact: Option<ComponentExactReport>,
    known_lower_bound: Option<u64>,
}

fn component_exact_outcome(
    decision: &PlannerDecision,
    census: &CensusResult,
) -> Result<ComponentExactOutcome, String> {
    let (class_label, reason_label) = classify_for_witness(
        decision,
        LaneStatus::Ok,
        census,
        &closed_ledger(),
        0,
        std::time::Duration::from_secs(60),
    );
    let exact = match (class_label, census.enumeration()) {
        ("exact", Some(enumeration)) => Some(component_exact_report(enumeration)),
        ("exact", None) => {
            return Err(
                "component classifier produced exact without census enumeration".to_string(),
            );
        }
        _ => None,
    };
    Ok(ComponentExactOutcome {
        class_label,
        reason_label: (reason_label != "none").then_some(reason_label),
        exact,
        known_lower_bound: (class_label == "cannot_establish")
            .then(|| census.enumeration().map(ExactEnumeration::returned))
            .flatten(),
    })
}

fn component_exact_report(enumeration: &ExactEnumeration) -> ComponentExactReport {
    ComponentExactReport {
        predicate_id: enumeration.predicate_id().to_string(),
        total: enumeration.total(),
        returned: enumeration.returned(),
        coverage: enumeration.coverage(),
        truncated: enumeration.truncated(),
        excluded_secret: enumeration.excluded_secret(),
    }
}

/// One full real census/classifier component run; returns no final Envelope outcome or metric.
async fn run_exact_case(
    pool: &RuntimeDbPool,
    entry: &PredicateEntry,
    authorization: &AuthorizationScope,
    ws: Uuid,
    key: &StreamKey,
) -> (ComponentExactOutcome, Vec<Uuid>) {
    let (indexed, scopes) = probe_predicate_inputs(pool, entry)
        .await
        .expect("probe must run");
    let decision = decide(REAL_QUERY, std::slice::from_ref(entry), &indexed, &scopes);
    assert!(
        matches!(decision, PlannerDecision::Enumerate { .. }),
        "real registry predicate must be established (probe: indexed={indexed:?}, \
         scopes={scopes:?})"
    );
    let outcome = exact_enumerate(pool, entry, authorization, WorkspaceId(ws), key)
        .await
        .expect("census transaction must open");
    let block = component_exact_outcome(&decision, &outcome.census)
        .expect("§22.0 component pair must hold on the real census path");
    (block, outcome.returned_ids)
}

#[derive(Debug, Clone)]
struct CaseResult {
    case_id: String,
    layer: String,
    pass: bool,
    detail: String,
}

fn check(row: &DatasetRow, out: &ComponentExactOutcome) -> (bool, String) {
    let class_label = out.class_label;
    let mut fails: Vec<String> = Vec::new();
    if class_label != row.expect_class {
        fails.push(format!("class {class_label} != {}", row.expect_class));
    }
    if out.reason_label != row.expect_reason.as_deref() {
        fails.push(format!(
            "reason {:?} != {:?}",
            out.reason_label, row.expect_reason
        ));
    }
    match (&out.exact, &row.expect_predicate_id) {
        (Some(e), Some(pid)) => {
            if e.predicate_id != *pid {
                fails.push(format!("predicate_id {} != {pid}", e.predicate_id));
            }
            if Some(e.total) != row.expect_total {
                fails.push(format!("total {} != {:?}", e.total, row.expect_total));
            }
            if Some(e.returned) != row.expect_returned {
                fails.push(format!(
                    "returned {} != {:?}",
                    e.returned, row.expect_returned
                ));
            }
            if Some(e.excluded_secret) != row.expect_excluded_secret {
                fails.push(format!(
                    "excluded_secret {} != {:?}",
                    e.excluded_secret, row.expect_excluded_secret
                ));
            }
            if Some(e.truncated) != row.expect_truncated {
                fails.push(format!(
                    "truncated {} != {:?}",
                    e.truncated, row.expect_truncated
                ));
            }
            // §22.1 coverage is derived — cross-check the derivation rather than a stored
            // expectation column (same-source discipline; a stored column could drift).
            #[allow(clippy::cast_precision_loss)]
            let want_cov = if e.total == 0 {
                1.0
            } else {
                e.returned as f64 / e.total as f64
            };
            if (e.coverage - want_cov).abs() > f64::EPSILON {
                fails.push(format!("coverage {} != derived {want_cov}", e.coverage));
            }
        }
        (None, None) => {}
        (Some(e), None) => fails.push(format!("unexpected exact block: {e:?}")),
        (None, Some(pid)) => fails.push(format!("missing exact block (expected {pid})")),
    }
    if out.known_lower_bound != row.expect_lower_bound {
        fails.push(format!(
            "lower_bound {:?} != {:?}",
            out.known_lower_bound, row.expect_lower_bound
        ));
    }
    if fails.is_empty() {
        (true, String::new())
    } else {
        (false, fails.join("; "))
    }
}

// ============================================================================
// the battery — one full pass over all 11 cases against fresh workspaces
// ============================================================================

fn tour_key(tenant_id: Uuid, ws: Uuid) -> StreamKey {
    StreamKey::new(
        TenantId(tenant_id),
        "workspace",
        ws,
        "private_memory",
        "retrieval_cards",
        "v1",
    )
}

/// Runs every dataset case against freshly seeded workspaces (`run_tag` keeps repeat runs'
/// worlds disjoint). `decoy_overlay` selects the *second system* for the resolution
/// measurement: the census consults a same-workspace but wrong-version [`StreamKey`], which silently
/// no-ops the §23.1② overlay — a real, runnable regression shape, not a fixture-constant
/// edit (§23.4's own discipline for named faults).
// The battery is one linear scenario script (seed → cases → purge → cases): splitting it
// into per-case functions would scatter the shared world (workspaces/keys/tombstone list)
// into parameters without making any single step clearer.
#[allow(clippy::too_many_lines)]
fn run_battery(
    f: &mut Fixture,
    dsn: &str,
    maintenance_dsn: &str,
    rows: &[DatasetRow],
    run_tag: &str,
    decoy_overlay: bool,
) -> Vec<CaseResult> {
    let entry = real_registry_entry(f);
    let tenant_id = f.tenant_id;
    let user_id = f.user_id;

    // --- sync phase 1: seed everything -----------------------------------------------------
    let ws_full = new_workspace(f, &format!("full-{run_tag}"));
    for _ in 0..4 {
        seed_rejection(f, ws_full, false, None);
    }
    let ws_secret = new_workspace(f, &format!("secret-{run_tag}"));
    for _ in 0..4 {
        seed_rejection(f, ws_secret, false, None);
    }
    seed_rejection(f, ws_secret, true, None);
    let ws_empty = new_workspace(f, &format!("empty-{run_tag}"));
    let ws_iso_a = new_workspace(f, &format!("iso-a-{run_tag}"));
    let ws_iso_b = new_workspace(f, &format!("iso-b-{run_tag}"));
    for _ in 0..3 {
        seed_rejection(f, ws_iso_a, false, None);
    }
    for _ in 0..2 {
        seed_rejection(f, ws_iso_b, false, None);
    }
    let ws_sup = new_workspace(f, &format!("sup-{run_tag}"));
    let mut sup_target = None;
    for _ in 0..3 {
        let (m, _) = seed_rejection(f, ws_sup, false, None);
        sup_target = Some(m);
    }
    seed_rejection(f, ws_sup, false, sup_target);

    // Card 27 (0178): `ops.outbox (tenant_id, commit_seq)` is UNIQUE, as the global
    // `ops.commit_seq_seq` always made it in production. Every seeding of this fixture in one
    // tenant (the second-system and repeat-run tests seed it more than once) therefore draws its
    // own commit identities from one fresh sequence value instead of reusing 1..=12 / 700.
    let commit_base: i64 = f
        .admin
        .query_one("SELECT nextval('ops.commit_seq_seq') * 1000", &[])
        .expect("commit identity base")
        .get(0);
    let ws_tour = new_workspace(f, &format!("tour-{run_tag}"));
    let real_key = tour_key(tenant_id, ws_tour);
    let mut tour: Vec<(Uuid, i64)> = Vec::new();
    for seq in 1..=12i64 {
        let (m, _) = seed_tour_memory(f, ws_tour, &real_key, seq, seq, commit_base + seq);
        tour.push((m, seq));
    }
    let tombstoned: Vec<(Uuid, i64)> = tour[..3].to_vec();

    let ws_hidden = new_workspace(f, &format!("hidden-{run_tag}"));
    let (hidden_memory, _) = seed_rejection(f, ws_hidden, false, None);
    let _hidden_evidence = add_hidden_backing_source(f, hidden_memory);

    // The outbox's stream position intentionally differs from its commit identity. A v2
    // tombstone with the same stream position must not affect the v1 key; the v1 tombstone
    // later must affect the row through commit_seq, not the mismatching stream_seq.
    let ws_commit = new_workspace(f, &format!("commit-{run_tag}"));
    let commit_key = tour_key(tenant_id, ws_commit);
    let (commit_memory, _) = seed_tour_memory(f, ws_commit, &commit_key, 8, 9, commit_base + 700);
    let version_decoy = StreamKey::new(
        TenantId(tenant_id),
        "workspace",
        ws_commit,
        "private_memory",
        "retrieval_cards",
        "v2",
    );
    f.admin
        .execute(
            "INSERT INTO projection.stream_log \
               (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, \
                stream_seq, commit_seq, state, settled_at) \
             VALUES ($1, $2, $3, $4, $5, $6, 8, $7, 'TOMBSTONED', now())",
            &[
                &f.tenant_id,
                &version_decoy.scope_kind,
                &version_decoy.scope_id,
                &version_decoy.domain,
                &version_decoy.projection_kind,
                &version_decoy.projection_version,
                &(commit_base + 700),
            ],
        )
        .expect("insert cross-version tombstone decoy");

    // The census key: the real stream, or (second system) a decoy whose overlay join can
    // never match — same code path, overlay silently ineffective.
    let census_key = if decoy_overlay {
        StreamKey::new(
            TenantId(tenant_id),
            "workspace",
            ws_tour,
            "private_memory",
            "retrieval_cards",
            "v2",
        )
    } else {
        real_key.clone()
    };

    // --- synthetic negative-trigger registry rows (§50-validated, never stored) ------------
    let missing_col_entry = load_registry(vec![PredicateRow {
        predicate_id: "ghost_entries_v1".to_string(),
        sql_predicate: "memory_type='REJECTION'".to_string(),
        required_columns: vec!["task_id".to_string()],
        enumerable_scope: entry.enumerable_scope().to_string(),
        surface_patterns: vec!["所有幽灵条目".to_string()],
        owner_module: "eval::fixture".to_string(),
    }])
    .expect("synthetic row passes §50 validation")
    .remove(0);
    let mixed_scope_entry = load_registry(vec![PredicateRow {
        predicate_id: "mixed_scope_v1".to_string(),
        sql_predicate: "memory_type='REJECTION'".to_string(),
        required_columns: vec!["memory_type".to_string()],
        enumerable_scope: "private.memory_records JOIN public.pool USING (memory_id) \
                           WHERE tenant_id = $1 AND visibility_workspace_id = $2"
            .to_string(),
        surface_patterns: vec!["所有混合域条目".to_string()],
        owner_module: "eval::fixture".to_string(),
    }])
    .expect("synthetic row passes §50 validation")
    .remove(0);
    // Established (real columns + real scope) but the predicate itself references a column
    // the snapshot does not have — §22.4 trigger 4's staging.
    let bad_predicate_entry = load_registry(vec![PredicateRow {
        predicate_id: "bad_predicate_v1".to_string(),
        sql_predicate: "nonexistent_column = 'X'".to_string(),
        required_columns: entry.required_columns().to_vec(),
        enumerable_scope: entry.enumerable_scope().to_string(),
        surface_patterns: vec!["所有坏谓词条目".to_string()],
        owner_module: "eval::fixture".to_string(),
    }])
    .expect("synthetic row passes §50 validation")
    .remove(0);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let metric_before = [
        ("exact", "none"),
        ("cannot_establish", "ledger_not_closed"),
        ("cannot_establish", "predicate_not_enumerable"),
        ("cannot_establish", "census_failed"),
    ]
    .into_iter()
    .map(|(class, reason)| {
        (
            (class, reason),
            retrieval_completeness_total_count(class, reason),
        )
    })
    .collect::<BTreeMap<_, _>>();
    let mut results: Vec<CaseResult> = Vec::new();

    // --- async phase 2: every case up to and including the tombstone boundary --------------
    rt.block_on(async {
        // dep: PostgreSQL(role_gateway) — opens the role-scoped connection for `run_battery`
        let pool = RuntimeDbPool::connect(&dsn_as_role(dsn, "role_gateway"))
            .await
            .expect("runtime pool");
        // dep: PostgreSQL(role_maintenance) — opens the role-scoped connection for `run_battery`
        let maint = MaintenanceDbPool::connect(maintenance_dsn)
            .await
            .expect("maintenance pool");

        let own_scope = authorization_for(tenant_id, user_id, ws_iso_a);
        assert!(matches!(
            exact_enumerate(
                &pool,
                &entry,
                &own_scope,
                WorkspaceId(ws_iso_b),
                &tour_key(tenant_id, ws_iso_b)
            )
            .await,
            Err(humaux_domain::error::ErrorCode::Forbidden)
        ));
        let wrong_workspace_key = tour_key(tenant_id, ws_iso_b);
        assert!(matches!(
            exact_enumerate(
                &pool,
                &entry,
                &own_scope,
                WorkspaceId(ws_iso_a),
                &wrong_workspace_key
            )
            .await,
            Err(humaux_domain::error::ErrorCode::Forbidden)
        ));
        let foreign_key = tour_key(Uuid::now_v7(), ws_iso_a);
        assert!(matches!(
            exact_enumerate(
                &pool,
                &entry,
                &own_scope,
                WorkspaceId(ws_iso_a),
                &foreign_key
            )
            .await,
            Err(humaux_domain::error::ErrorCode::Forbidden)
        ));
        let wrong_scope_kind = StreamKey::new(
            TenantId(tenant_id),
            "tenant",
            tenant_id,
            "private_memory",
            "retrieval_cards",
            "v1",
        );
        assert!(matches!(
            exact_enumerate(
                &pool,
                &entry,
                &own_scope,
                WorkspaceId(ws_iso_a),
                &wrong_scope_kind
            )
            .await,
            Err(humaux_domain::error::ErrorCode::Forbidden)
        ));

        let hidden = exact_enumerate(
            &pool,
            &entry,
            &authorization_for(tenant_id, user_id, ws_hidden),
            WorkspaceId(ws_hidden),
            &tour_key(tenant_id, ws_hidden),
        )
        .await
        .expect("hidden-source census");
        let hidden_counts = hidden
            .census
            .enumeration()
            .expect("hidden-source enumeration");
        assert_eq!(
            hidden_counts.total(),
            0,
            "hidden backing source must leave the denominator"
        );
        assert_eq!(hidden_counts.returned(), 0);
        assert_eq!(hidden_counts.excluded_secret(), 0);
        assert!(!hidden.returned_ids.contains(&hidden_memory));

        let before_commit_tombstone = exact_enumerate(
            &pool,
            &entry,
            &authorization_for(tenant_id, user_id, ws_commit),
            WorkspaceId(ws_commit),
            &commit_key,
        )
        .await
        .expect("cross-version census");
        assert_eq!(before_commit_tombstone.returned_ids, vec![commit_memory]);
        forget_repo::tombstone(&maint, &commit_key, 8)
            .await
            .expect("tombstone matching commit identity");
        let after_commit_tombstone = exact_enumerate(
            &pool,
            &entry,
            &authorization_for(tenant_id, user_id, ws_commit),
            WorkspaceId(ws_commit),
            &commit_key,
        )
        .await
        .expect("commit-seq tombstone census");
        let after_counts = after_commit_tombstone
            .census
            .enumeration()
            .expect("commit-seq enumeration");
        assert_eq!(after_counts.total(), 0);
        assert!(after_commit_tombstone.returned_ids.is_empty());

        for row in rows {
            let (out, extra) = match row.case_id.as_str() {
                "exact_full" => {
                    let (out, _) = run_exact_case(
                        &pool,
                        &entry,
                        &authorization_for(tenant_id, user_id, ws_full),
                        ws_full,
                        &tour_key(tenant_id, ws_full),
                    )
                    .await;
                    (out, None)
                }
                "exact_secret_deducted" => {
                    let (out, ids) = run_exact_case(
                        &pool,
                        &entry,
                        &authorization_for(tenant_id, user_id, ws_secret),
                        ws_secret,
                        &tour_key(tenant_id, ws_secret),
                    )
                    .await;
                    // §22.1: excluded rows are *named*, never returned.
                    let extra =
                        (ids.len() != 4).then(|| format!("returned_ids len {} != 4", ids.len()));
                    (out, extra)
                }
                "exact_empty" => {
                    let (out, _) = run_exact_case(
                        &pool,
                        &entry,
                        &authorization_for(tenant_id, user_id, ws_empty),
                        ws_empty,
                        &tour_key(tenant_id, ws_empty),
                    )
                    .await;
                    (out, None)
                }
                "exact_scope_isolated" => {
                    let (out, _) = run_exact_case(
                        &pool,
                        &entry,
                        &authorization_for(tenant_id, user_id, ws_iso_a),
                        ws_iso_a,
                        &tour_key(tenant_id, ws_iso_a),
                    )
                    .await;
                    (out, None)
                }
                "exact_supersede_out" => {
                    let (out, _) = run_exact_case(
                        &pool,
                        &entry,
                        &authorization_for(tenant_id, user_id, ws_sup),
                        ws_sup,
                        &tour_key(tenant_id, ws_sup),
                    )
                    .await;
                    (out, None)
                }
                "ne_missing_column" => {
                    let (indexed, scopes) = probe_predicate_inputs(&pool, &missing_col_entry)
                        .await
                        .expect("probe");
                    let d = decide(
                        "所有幽灵条目有哪些",
                        std::slice::from_ref(&missing_col_entry),
                        &indexed,
                        &scopes,
                    );
                    let out = component_exact_outcome(&d, &CensusResult::ok_without_enumeration())
                        .expect("pair");
                    (out, None)
                }
                "ne_mixed_scope" => {
                    let (indexed, scopes) = probe_predicate_inputs(&pool, &mixed_scope_entry)
                        .await
                        .expect("probe");
                    let d = decide(
                        "所有混合域条目有哪些",
                        std::slice::from_ref(&mixed_scope_entry),
                        &indexed,
                        &scopes,
                    );
                    let out = component_exact_outcome(&d, &CensusResult::ok_without_enumeration())
                        .expect("pair");
                    (out, None)
                }
                "census_failed" => {
                    let (indexed, scopes) = probe_predicate_inputs(&pool, &bad_predicate_entry)
                        .await
                        .expect("probe");
                    let d = decide(
                        "所有坏谓词条目有哪些",
                        std::slice::from_ref(&bad_predicate_entry),
                        &indexed,
                        &scopes,
                    );
                    assert!(
                        matches!(d, PlannerDecision::Enumerate { .. }),
                        "trigger-4 staging requires an established predicate; got {d:?}"
                    );
                    let outcome = exact_enumerate(
                        &pool,
                        &bad_predicate_entry,
                        &authorization_for(tenant_id, user_id, ws_empty),
                        WorkspaceId(ws_empty),
                        &tour_key(tenant_id, ws_empty),
                    )
                    .await
                    .expect("census txn opens");
                    let out = component_exact_outcome(&d, &outcome.census).expect("pair");
                    (out, None)
                }
                "tour_before" => {
                    let (out, ids) = run_exact_case(
                        &pool,
                        &entry,
                        &authorization_for(tenant_id, user_id, ws_tour),
                        ws_tour,
                        &census_key,
                    )
                    .await;
                    let extra =
                        (ids.len() != 12).then(|| format!("returned_ids len {} != 12", ids.len()));
                    (out, extra)
                }
                "tour_after_tombstone" => {
                    // §37.2 sole exit — the real production tombstone, never a bare UPDATE.
                    for (_, seq) in &tombstoned {
                        forget_repo::tombstone(&maint, &real_key, *seq as u64)
                            .await
                            .expect("tombstone");
                    }
                    let (out, ids) = run_exact_case(
                        &pool,
                        &entry,
                        &authorization_for(tenant_id, user_id, ws_tour),
                        ws_tour,
                        &census_key,
                    )
                    .await;
                    // §23.4: the tombstoned rows must not appear — *including* now, before
                    // the physical purge has run.
                    let leaked: Vec<_> = tombstoned
                        .iter()
                        .filter(|(m, _)| ids.contains(m))
                        .map(|(m, _)| *m)
                        .collect();
                    let extra = (!leaked.is_empty())
                        .then(|| format!("tombstoned ids leaked into EXACT items: {leaked:?}"));
                    (out, extra)
                }
                // Runs in phase 4, after the sync purge below.
                "tour_after_purge" => continue,
                other => panic!("dataset names a case this harness does not stage: {other}"),
            };
            let (mut pass, mut detail) = check(row, &out);
            if let Some(x) = extra {
                pass = false;
                detail = if detail.is_empty() {
                    x
                } else {
                    format!("{detail}; {x}")
                };
            }
            results.push(CaseResult {
                case_id: row.case_id.clone(),
                layer: row.layer.clone(),
                pass,
                detail,
            });
        }
    });

    // --- sync phase 3: DeletionPlan step 2 (authority rows) — physical purge ---------------
    for (memory_id, _) in &tombstoned {
        f.admin
            .batch_execute(&format!(
                "DELETE FROM private.memory_evidence WHERE memory_id = '{memory_id}'; \
                 DELETE FROM private.memory_records WHERE memory_id = '{memory_id}';"
            ))
            .expect("authority purge");
    }

    // --- async phase 4: the purge-boundary sample (§37: purge must not move any readout) ---
    rt.block_on(async {
        // dep: PostgreSQL(role_gateway) — opens the role-scoped connection
        let pool = RuntimeDbPool::connect(&dsn_as_role(dsn, "role_gateway"))
            .await
            .expect("runtime pool");
        for row in rows {
            if row.case_id != "tour_after_purge" {
                continue;
            }
            let (out, ids) = run_exact_case(
                &pool,
                &entry,
                &authorization_for(tenant_id, user_id, ws_tour),
                ws_tour,
                &census_key,
            )
            .await;
            let leaked: Vec<_> = tombstoned
                .iter()
                .filter(|(m, _)| ids.contains(m))
                .map(|(m, _)| *m)
                .collect();
            let (mut pass, mut detail) = check(row, &out);
            if !leaked.is_empty() {
                pass = false;
                let x = format!("purged ids still in EXACT items: {leaked:?}");
                detail = if detail.is_empty() {
                    x
                } else {
                    format!("{detail}; {x}")
                };
            }
            results.push(CaseResult {
                case_id: row.case_id.clone(),
                layer: row.layer.clone(),
                pass,
                detail,
            });
        }
    });

    for ((class, reason), before) in metric_before {
        assert_eq!(
            retrieval_completeness_total_count(class, reason),
            before,
            "component census evaluator must not emit final completeness metric {class}/{reason}"
        );
    }
    results
}

// ============================================================================
// §55.3 measurements
// ============================================================================

#[test]
fn all_cases_match_expected_readouts() {
    let Some((mut f, dsn, maintenance_dsn)) = setup() else {
        return;
    };
    let rows = read_dataset_rows();
    assert_eq!(rows.len(), 11, "frozen fixed_denominator = 11");
    let results = run_battery(&mut f, &dsn, &maintenance_dsn, &rows, "judgment", false);
    assert_eq!(results.len(), 11, "every case must produce a result");
    let failed: Vec<_> = results.iter().filter(|r| !r.pass).collect();
    assert!(
        failed.is_empty(),
        "{} case(s) mismatched:\n{}",
        failed.len(),
        failed
            .iter()
            .map(|r| format!("  {} [{}]: {}", r.case_id, r.layer, r.detail))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// §55.3 `spread_tol`: same system, same profile, 3 runs, per-layer pass-count range. The
/// zero is *observed* — the battery genuinely runs 3 times over fresh workspaces.
#[test]
fn repeated_runs_have_zero_spread() {
    let Some((mut f, dsn, maintenance_dsn)) = setup() else {
        return;
    };
    let rows = read_dataset_rows();
    let mut per_run: Vec<BTreeMap<String, usize>> = Vec::new();
    for run in 0..3 {
        let results = run_battery(
            &mut f,
            &dsn,
            &maintenance_dsn,
            &rows,
            &format!("spread{run}"),
            false,
        );
        let mut layer_pass: BTreeMap<String, usize> = BTreeMap::new();
        for r in &results {
            *layer_pass.entry(r.layer.clone()).or_default() += usize::from(r.pass);
        }
        per_run.push(layer_pass);
    }
    for layer in ["exact", "non_enumerable", "deletion_tour"] {
        let counts: Vec<usize> = per_run
            .iter()
            .map(|m| *m.get(layer).unwrap_or(&0))
            .collect();
        let spread = counts.iter().max().unwrap() - counts.iter().min().unwrap();
        assert_eq!(
            spread, 0,
            "layer {layer} pass_items across 3 runs: {counts:?} — spread_tol measured \
             non-zero; the §55.3 declaration must be updated if this is real"
        );
    }
}

/// §55.3 `resolution`: a real second system (overlay mis-scoped ⇒ effectively absent — the
/// spec-named broken shape) diffed case-by-case against the real system on the same fixture
/// set. The measured value is the *observed* number of flipped cases, and the flip must be
/// exactly where the two systems genuinely differ: the tombstone boundary sample.
#[test]
fn resolution_is_measured_via_a_real_second_system_diff() {
    let Some((mut f, dsn, maintenance_dsn)) = setup() else {
        return;
    };
    let rows = read_dataset_rows();
    let real = run_battery(&mut f, &dsn, &maintenance_dsn, &rows, "res-real", false);
    let second = run_battery(&mut f, &dsn, &maintenance_dsn, &rows, "res-second", true);
    assert_eq!(real.len(), second.len());
    let flipped: Vec<&str> = real
        .iter()
        .zip(second.iter())
        .filter(|(a, b)| {
            assert_eq!(a.case_id, b.case_id, "same battery order");
            a.pass != b.pass
        })
        .map(|(a, _)| a.case_id.as_str())
        .collect();
    assert_eq!(
        flipped,
        vec!["tour_after_tombstone"],
        "resolution: the two systems must differ on exactly the tombstone-boundary case \
         (observed flips: {flipped:?})"
    );
}

/// §55.3.1: the manifest's frozen numbers must match the dataset on disk — recomputed here,
/// not trusted (same shape as `planner_predicate_eval.rs::manifest_matches_real_dataset`).
#[test]
fn manifest_matches_real_dataset() {
    let manifest = std::fs::read_to_string(manifest_path()).expect("manifest.toml must exist");
    let value = |key: &str| -> String {
        manifest
            .lines()
            .find_map(|l| {
                let l = l.trim();
                l.strip_prefix(key)
                    .and_then(|r| r.trim_start().strip_prefix('='))
                    .map(|v| v.trim().trim_matches('"').to_string())
            })
            .unwrap_or_else(|| panic!("manifest missing key {key}"))
    };

    let rows = read_dataset_rows();
    assert_eq!(
        value("fixed_denominator").parse::<usize>().unwrap(),
        rows.len(),
        "manifest fixed_denominator != dataset row count"
    );

    let bytes = std::fs::read(dataset_path()).expect("dataset bytes");
    let digest = {
        use sha2::{Digest, Sha256};
        let out: [u8; 32] = Sha256::digest(&bytes).into();
        out.iter().map(|b| format!("{b:02x}")).collect::<String>()
    };
    assert_eq!(
        value("fixture_sha256"),
        digest,
        "manifest fixture_sha256 != recomputed sha256 of dataset.tsv"
    );

    let mut layers: BTreeMap<String, usize> = BTreeMap::new();
    for r in &rows {
        *layers.entry(r.layer.clone()).or_default() += 1;
    }
    assert_eq!(
        layers,
        BTreeMap::from([
            ("exact".to_string(), 5),
            ("non_enumerable".to_string(), 3),
            ("deletion_tour".to_string(), 3),
        ]),
        "layer split drifted from the declared 5+3+3"
    );
}
