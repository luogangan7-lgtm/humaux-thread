//! `adapters::tests::hot_path_indexes` — pins the seven card-27 hot-path indexes (audit P1-15 + the ADR-0052 claim
//!   index) in the live catalog: present, valid, ready, and shaped exactly as their migrations create them.
//! Depends-on: crates=[humaux-testkit, postgres]; services=[PostgreSQL(any) r=[ops.jobs, ops.outbox,
//!   private.memory_evidence, private.memory_records, projection.stream_log]]; env=[CARGO_MANIFEST_DIR,
//!   HUMAUX_TEST_PG_DSN]; modules=[humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [read-only (catalog SELECTs only); no DSN is a visible SKIP and HUMAUX_REQUIRE_DB=1 turns it into a
//!   failure; every problem is collected and reported by index name in one assertion]
//! Spec: Baseline §46; ADR-0052
use humaux_testkit::{ExternalDep, skip_or_fail};
use postgres::{Client, NoTls};

/// `(migration file, index name, pg_get_indexdef as the server prints it)` — the audit P1-15 list plus
/// the ADR-0052 claim index, one CONCURRENTLY file each (0177–0183).
const PINNED: [(&str, &str, &str); 7] = [
    (
        "0177_idx_stream_log_issued_claim.sql",
        "stream_log_issued_claim_idx",
        "CREATE INDEX stream_log_issued_claim_idx ON projection.stream_log USING btree (domain, projection_kind, projection_version, tenant_id, scope_kind, scope_id, stream_seq) WHERE (state = 'ISSUED'::text)",
    ),
    (
        "0178_idx_outbox_tenant_commit_seq.sql",
        "outbox_tenant_commit_seq_uidx",
        "CREATE UNIQUE INDEX outbox_tenant_commit_seq_uidx ON ops.outbox USING btree (tenant_id, commit_seq) WHERE (commit_seq IS NOT NULL)",
    ),
    (
        "0179_idx_outbox_tenant_evidence.sql",
        "outbox_tenant_evidence_idx",
        "CREATE INDEX outbox_tenant_evidence_idx ON ops.outbox USING btree (tenant_id, evidence_id) WHERE (evidence_id IS NOT NULL)",
    ),
    (
        "0180_idx_outbox_evidence_claim.sql",
        "outbox_evidence_claim_idx",
        "CREATE INDEX outbox_evidence_claim_idx ON ops.outbox USING btree (tenant_id, event_type, commit_seq) WHERE ((status = ANY (ARRAY['PENDING'::text, 'PROCESSING'::text])) AND (evidence_id IS NOT NULL))",
    ),
    (
        "0181_idx_memory_evidence_evidence.sql",
        "memory_evidence_evidence_idx",
        "CREATE INDEX memory_evidence_evidence_idx ON private.memory_evidence USING btree (evidence_id)",
    ),
    (
        "0182_idx_jobs_derived_claim.sql",
        "jobs_claim_active_idx",
        "CREATE INDEX jobs_claim_active_idx ON ops.jobs USING btree (job_type, priority DESC, next_retry_at, created_at) WHERE (status = ANY (ARRAY['PENDING'::text, 'RETRY_WAIT'::text, 'PROCESSING'::text]))",
    ),
    (
        "0183_idx_memory_records_superseded_by.sql",
        "memory_records_superseded_by_idx",
        "CREATE INDEX memory_records_superseded_by_idx ON private.memory_records USING btree (superseded_by) WHERE (superseded_by IS NOT NULL)",
    ),
];

/// Uncaught fault `f_delete_p1_15_index` (review 2026-09-29): the rehearsal's EXPLAIN gate went
/// red for only 2 of the 6 P1-15 indexes when each was dropped — at dev data sizes the planner
/// has another path for the other four, so a plan cannot pin an index. This pins them in the
/// catalog instead: every index exists under its name, is valid and ready (a failed
/// `CREATE INDEX CONCURRENTLY` leaves an INVALID one), and has exactly the definition its
/// migration file creates. Fault injection: drop any one of the seven (throwaway schema copy) ⇒
/// red naming it.
#[test]
fn p1_15_hot_path_indexes_exist_valid_and_shaped() {
    const TEST: &str = "p1_15_hot_path_indexes_exist_valid_and_shaped";
    let Ok(dsn) = std::env::var("HUMAUX_TEST_PG_DSN") else {
        skip_or_fail(
            TEST,
            "missing object: HUMAUX_TEST_PG_DSN",
            ExternalDep::Postgres,
        );
        return;
    };
    // dep: PostgreSQL(any) — catalog read of the pinned indexes
    let mut client = match Client::connect(&dsn, NoTls) {
        Ok(client) => client,
        Err(e) => {
            skip_or_fail(
                TEST,
                &format!("missing object: live Postgres ({e})"),
                ExternalDep::Postgres,
            );
            return;
        }
    };
    let migrations = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut problems = Vec::new();
    for (file, name, def) in PINNED {
        let sql = std::fs::read_to_string(migrations.join(file)).expect("pinned migration file");
        assert!(
            sql.contains(&format!("CONCURRENTLY {name}")),
            "{file} no longer creates {name}: re-pin"
        );
        let row = client
            .query_opt(
                "SELECT i.indisvalid, i.indisready, pg_get_indexdef(i.indexrelid) \
                 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
                 WHERE c.relname = $1",
                &[&name],
            )
            .expect("catalog read");
        match row {
            None => problems.push(format!("{name}: missing")),
            Some(row) => {
                let (valid, ready, live): (bool, bool, String) =
                    (row.get(0), row.get(1), row.get(2));
                if !(valid && ready) {
                    problems.push(format!("{name}: indisvalid={valid} indisready={ready}"));
                }
                if live != def {
                    problems.push(format!("{name}: definition drifted: {live}"));
                }
            }
        }
    }
    assert!(problems.is_empty(), "P1-15 index pin: {problems:?}");
}
