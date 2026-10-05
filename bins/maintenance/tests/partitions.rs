//! `maintenance::tests::partitions` — the §48.1 conversions (0225 stage_runs, 0226 messages, 0227
//!   maintenance_receipts, 0228 events, 0229 audit_events, 0230 model_call_ledger) against throwaway PostgreSQL
//!   databases migrated from the files: every converted table is a RANGE parent with sealed monthly leaves, the
//!   horizon is the current UTC month plus 3, month bounds are UTC whatever the session TimeZone, seeded history
//!   survives a conversion, the events, audit and ledger identity tables carry global uniqueness and the re-pointed
//!   FKs, the ledger dedupe holds for raw inserts and for the `reserve_call` writer, audit_seq continues above the
//!   legacy maximum, the §77 / §19.1 guards and the audit insert definer keep working, and a conversion refuses
//!   orphan data and catalog drift (ADR-0063 D-A, D-B, D-D, D-E, D-F); the registry surviving a logical dump/restore
//!   (ADR-0063 "Registry by name", 0231); plus the D-N insert-latency measurement.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-testkit, postgres, serde_json, tokio, uuid];
//!   services=[PostgreSQL(owner) r=[control.audit_event_identity, control.audit_events_hist,
//!   control.partition_registry, control.retention_policies, ops.maintenance_receipts_hist, ops.model_call_identity,
//!   ops.model_call_ledger_hist, ops.stage_runs_hist, private.event_identity, private.events_hist]
//!   w=[control.audit_events, control.operation_receipts, control.partition_registry,
//!   control.private_reasoning_domains, control.quota_windows, control.tenants, control.usage_reservations,
//!   control.users, ops.maintenance_receipts, ops.model_call_ledger, ops.retrieval_provider_budget_reservations,
//!   ops.stage_runs, private.conversations, private.events, private.evidence_objects, private.ingest_tickets,
//!   private.messages]
//!   x=[control.audit_event_insert, control.partition_create_month], PostgreSQL(role_gateway),
//!   PostgreSQL(role_maintenance), PostgreSQL(role_migration_owner), PostgreSQL(role_retrieval_worker),
//!   subprocess(docker), subprocess(humaux-maintenance)];
//!   env=[CARGO_MANIFEST_DIR, CARGO_TARGET_TMPDIR, HUMAUX_C36_LATENCY_DB, HUMAUX_MIGRATOR_PG_DSN,
//!   HUMAUX_RETRIEVAL_WORKER_PG_DSN];
//!   modules=[adapters::maintenance_repo, adapters::model_call_ledger, adapters::postgres, domain::ledger,
//!   humaux-testkit, maintenance::tests::support::throwaway]
//! Called-by: [cargo-test]
//! Invariants: [each test owns humaux_thread_c36_parts_<pid>_<n>, dropped WITH (FORCE) by the fixture's Drop even on
//!   panic; runtime roles are only entered through SET LOCAL ROLE inside it, or through the retrieval worker's
//!   checked pool on it; orphan rows are planted only inside it under session_replication_role = replica; nothing
//!   here touches the shared dev database (ADR-0063 D-M); `pg_dump` / `pg_restore` run only inside the
//!   humaux-thread-pg container, from one throwaway into another empty one this test created; the executor runs with
//!   HUMAUX_MIGRATOR_PG_DSN set per spawned run to a throwaway's owner DSN; the ignored D-N `partition_insert_latency`
//!   alone targets an existing database, and only one named humaux_thread_c36_* (a restored throwaway), checked
//!   before any write]
//! Spec: Baseline §19.1; Baseline §48.1; ADR-0063 D-A; ADR-0063 D-B; ADR-0063 D-D; ADR-0063 D-E; ADR-0063 D-F;
//!   ADR-0063 D-J; ADR-0063 D-L; ADR-0063 D-M; ADR-0063 D-N

#[path = "support/throwaway.rs"]
#[allow(dead_code)]
mod throwaway;
use std::path::PathBuf;

use postgres::Client;
use throwaway::Db;

/// The §48.1 tables converted by 0225..0230 `(table_key, parent, key column)`.
const CONVERTED: [(&str, &str, &str); 6] = [
    ("STAGE_RUNS", "ops.stage_runs", "started_at"),
    ("MESSAGES", "private.messages", "created_at"),
    ("MAINTENANCE_RECEIPTS", "ops.maintenance_receipts", "ran_at"),
    ("EVENTS", "private.events", "recorded_at"),
    ("AUDIT_EVENTS", "control.audit_events", "occurred_at"),
    ("MODEL_CALL_LEDGER", "ops.model_call_ledger", "called_at"),
];

/// §6.2.0 runtime roles (the set 0224 revokes from).
const RUNTIME_ROLES: [&str; 9] = [
    "role_gateway",
    "role_private_worker",
    "role_consolidation_worker",
    "role_public_worker",
    "role_retrieval_worker",
    "role_batch_issuer",
    "role_maintenance",
    "role_admin",
    "role_health_reader",
];

/// UTC month start `months` after the current one, as SQL (D-D step 5 arithmetic).
fn month(months: i32) -> String {
    format!("date_add(date_trunc('month', now(), 'UTC'), make_interval(months => {months}), 'UTC')")
}

fn head(test: &str) -> Option<Db> {
    // dep: PostgreSQL(owner) — a throwaway migrated 0001→head
    throwaway::db(test, "c36_parts")
}

/// A tenant and one of its conversations, as text ids.
fn tenant(c: &mut Client) -> (String, String) {
    let row = c
        .query_one(
            "WITH t AS (INSERT INTO control.tenants (name) VALUES ('c36-parts') RETURNING tenant_id),
                  v AS (INSERT INTO private.conversations (tenant_id) SELECT tenant_id FROM t
                        RETURNING tenant_id, conversation_id)
             SELECT tenant_id::text, conversation_id::text FROM v",
            &[],
        )
        .expect("tenant fixture");
    (row.get(0), row.get(1))
}

/// One row of `table_key` with its key at `key_sql`, inserted through the parent; the leaf it landed in.
fn insert_at(
    c: &mut impl postgres::GenericClient,
    table_key: &str,
    key_sql: &str,
    ids: &(String, String),
) -> Result<String, postgres::Error> {
    let sql = match table_key {
        "STAGE_RUNS" => format!(
            "INSERT INTO ops.stage_runs (tenant_id, stage_name, started_at) VALUES ($1::text::uuid, 'c36', {key_sql})
             RETURNING tableoid::regclass::text, $2::text"
        ),
        "MESSAGES" => format!(
            "INSERT INTO private.messages (tenant_id, conversation_id, created_at)
             VALUES ($1::text::uuid, $2::text::uuid, {key_sql}) RETURNING tableoid::regclass::text"
        ),
        "MAINTENANCE_RECEIPTS" => format!(
            "INSERT INTO ops.maintenance_receipts (tenant_id, task, cutoff, row_limit, affected, ran_at)
             VALUES ($1::text::uuid, 'rate_buckets', now(), 1, 1, {key_sql}) RETURNING tableoid::regclass::text, $2::text"
        ),
        // An event subtypes its evidence row (1:1, events.event_id = evidence_id), so each one gets its own.
        "EVENTS" => format!(
            "{} INSERT INTO private.events (event_id, event_kind, payload, recorded_at)
             SELECT evidence_id, 'MANUAL_NOTE', '{{}}'::jsonb, {key_sql} FROM eo RETURNING tableoid::regclass::text",
            evidence_cte("now()")
        ),
        "AUDIT_EVENTS" => format!(
            "INSERT INTO control.audit_events (tenant_id, action, occurred_at)
             VALUES ($1::text::uuid, 'C36_FIXTURE', {key_sql}) RETURNING tableoid::regclass::text, $2::text"
        ),
        // A retrieval-plane row (no reasoning column, 0206 CHECK arm B) with a fresh request_id.
        "MODEL_CALL_LEDGER" => format!(
            "INSERT INTO ops.model_call_ledger (tenant_id, provider, purpose, called_at)
             VALUES ($1::text::uuid, 'c36', 'embedding', {key_sql}) RETURNING tableoid::regclass::text, $2::text"
        ),
        other => panic!("no fixture row for {other}"),
    };
    c.query_one(&sql, &[&ids.0, &ids.1]).map(|r| r.get(0))
}

/// `WITH d AS (…), eo AS (…)`: one reasoning domain of tenant `$1` and one EVENT-kind evidence row in it (the
/// `remember` recipe's NOT NULL columns) created at `created_at_sql`; `$2` only names the domain. The caller appends
/// the statement that reads `eo(evidence_id)`.
fn evidence_cte(created_at_sql: &str) -> String {
    format!(
        "WITH d AS (INSERT INTO control.private_reasoning_domains (tenant_id, name)
                    VALUES ($1::text::uuid, 'c36-' || $2::text || '-' || gen_random_uuid())
                    RETURNING tenant_id, reasoning_domain_id),
              eo AS (INSERT INTO private.evidence_objects
                       (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, visibility_class,
                        reasoning_domain_id, created_at)
                     SELECT tenant_id, 'EVENT', sha256('c36'::bytea), 'INTERNAL', 'DirectUserInput', 'TENANT_SHARED',
                            reasoning_domain_id, {created_at_sql}
                       FROM d RETURNING evidence_id) "
    )
}

/// One evidence row of the tenant created at `created_at_sql`, as text id.
fn evidence(c: &mut Client, ids: &(String, String), created_at_sql: &str) -> String {
    c.query_one(
        &format!(
            "{} SELECT evidence_id::text FROM eo",
            evidence_cte(created_at_sql)
        ),
        &[&ids.0, &ids.1],
    )
    .expect("evidence fixture")
    .get(0)
}

/// `parent_pYYYYMM` of the UTC month `months` after the current one.
fn leaf_of(c: &mut Client, parent: &str, months: i32) -> String {
    c.query_one(
        &format!(
            "SELECT $1::text || '_p' || to_char({} AT TIME ZONE 'UTC', 'YYYYMM')",
            month(months)
        ),
        &[&parent],
    )
    .expect("leaf name")
    .get(0)
}

/// `(sqlstate, message)` of a statement that must fail.
fn refused(e: &postgres::Error) -> (String, String) {
    let db = e
        .as_db_error()
        .unwrap_or_else(|| panic!("not a server error: {e}"));
    (db.code().code().to_owned(), db.message().to_owned())
}

/// Every leaf of every RANGE parent, as `(parent, leaf)` regclass text.
fn leaves(c: &mut Client) -> Vec<(String, String)> {
    c.query(
        "SELECT i.inhparent::regclass::text, i.inhrelid::regclass::text
           FROM pg_inherits i JOIN pg_partitioned_table p ON p.partrelid = i.inhparent ORDER BY 1, 2",
        &[],
    )
    .expect("leaves")
    .iter()
    .map(|r| (r.get(0), r.get(1)))
    .collect()
}

/// T-C1 (ADR-0063 D-A, D-F): every converted table is a RANGE parent on its key with no DEFAULT partition, whose
/// leaves are exactly the registered UTC months current … current+3 (an empty table's heap became the current
/// month). Fault: a DEFAULT partition in 0225 ⇒ partdefid ≠ 0 and a fifth leaf ⇒ red.
#[test]
fn every_growth_table_is_partitioned_with_monthly_leaves_and_no_default() {
    let Some(mut db) = head("every_growth_table_is_partitioned_with_monthly_leaves_and_no_default")
    else {
        return;
    };
    let c = db.client();
    for (key, parent, column) in CONVERTED {
        let row = c
            .query_one(
                "SELECT c.relkind::text, p.partstrat::text, p.partdefid <> 0, a.attname::text,
                        (SELECT count(*) FROM pg_inherits i WHERE i.inhparent = c.oid)
                   FROM pg_class c JOIN pg_partitioned_table p ON p.partrelid = c.oid
                   JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = p.partattrs[0]
                  WHERE c.oid = to_regclass($1)",
                &[&parent],
            )
            .unwrap_or_else(|e| panic!("{parent} is not a partitioned table: {e}"));
        let shape: (String, String, bool, String, i64) =
            (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
        assert_eq!(
            shape,
            ("p".into(), "r".into(), false, column.into(), 4),
            "{parent}: (relkind, strategy, DEFAULT partition oid, key, leaves)"
        );
        let actual: Vec<String> = c
            .query(
                "SELECT format('%s %s %s', r.leaf_name,
                               coalesce(to_char(r.lower_bound AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'), 'MINVALUE'),
                               to_char(r.upper_bound AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'))
                   FROM control.partition_registry r
                   JOIN pg_inherits i ON i.inhrelid = to_regclass(r.leaf_name) AND i.inhparent = to_regclass($2)
                  WHERE r.table_key = $1 AND r.state = 'ATTACHED' ORDER BY r.upper_bound",
                &[&key, &parent],
            )
            .expect("registered leaves")
            .iter()
            .map(|r| r.get(0))
            .collect();
        let expected: Vec<String> = c
            .query(
                "SELECT format('%s_p%s %s %s', $1::text, to_char(m AT TIME ZONE 'UTC', 'YYYYMM'),
                               to_char(m AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'),
                               to_char(date_add(m, interval '1 month', 'UTC') AT TIME ZONE 'UTC',
                                       'YYYY-MM-DD HH24:MI:SS'))
                   FROM generate_series(0, 3) g,
                        LATERAL (SELECT date_add(date_trunc('month', now(), 'UTC'), make_interval(months => g), 'UTC')
                                   AS m) x
                  ORDER BY g",
                &[&parent],
            )
            .expect("expected months")
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(
            actual, expected,
            "{parent}: the current UTC month plus 3, registered"
        );
    }
}

/// T-C2 (ADR-0063 D-E): every leaf of every parent has RLS enabled and forced and the parent's policy set verbatim;
/// FORCE blinds the owner itself on a leaf without the tenant GUC. Fault: drop the FORCE line from
/// `control.partition_adopt_leaf` (0224) ⇒ the adopted and created leaves are not forced ⇒ red.
#[test]
fn every_leaf_forces_rls_with_the_parents_policies() {
    let Some(mut db) = head("every_leaf_forces_rls_with_the_parents_policies") else {
        return;
    };
    let c = db.client();
    let all = leaves(c);
    assert!(
        all.len() >= 4 * CONVERTED.len(),
        "leaves of the converted parents: {all:?}"
    );
    // Both sides are deparsed against the parent ($2): events' policy names its own table (0228), which deparses
    // under the leaf's name otherwise.
    let policies = "SELECT coalesce(array_agg(format('%s %s %s %s %s %s', p.polname, p.polcmd, p.polpermissive,
                                                     p.polroles, pg_get_expr(p.polqual, to_regclass($2)),
                                                     pg_get_expr(p.polwithcheck, to_regclass($2)))
                                              ORDER BY p.polname), '{}')
                      FROM pg_policy p WHERE p.polrelid = to_regclass($1)";
    for (parent, leaf) in &all {
        let flags: (bool, bool) = c
            .query_one(
                "SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE oid = to_regclass($1)",
                &[leaf],
            )
            .map(|r| (r.get(0), r.get(1)))
            .expect("leaf flags");
        assert_eq!(flags, (true, true), "{leaf}: RLS (enabled, forced)");
        let mine: Vec<String> = c
            .query_one(policies, &[leaf, parent])
            .expect("leaf policies")
            .get(0);
        let theirs: Vec<String> = c
            .query_one(policies, &[parent, parent])
            .expect("parent policies")
            .get(0);
        assert!(!theirs.is_empty(), "{parent} has a tenant policy");
        assert_eq!(mine, theirs, "{leaf}: the policy set of {parent}");
    }
    let ids = tenant(c);
    let leaf = insert_at(c, "STAGE_RUNS", "now()", &ids).expect("current-month row");
    let mut tx = c.transaction().expect("begin");
    // dep: PostgreSQL(role_migration_owner) — role switch: the owner's own view of a FORCE-RLS leaf
    tx.batch_execute("SET LOCAL ROLE role_migration_owner")
        .expect("owner role");
    let blind: i64 = tx
        .query_one(&format!("SELECT count(*) FROM {leaf}"), &[])
        .expect("owner reads its own leaf")
        .get(0);
    assert_eq!(
        blind, 0,
        "{leaf}: FORCE RLS hides the row from the owner without the tenant GUC"
    );
    tx.execute("SELECT set_config('humaux.tenant_id', $1, true)", &[&ids.0])
        .expect("tenant GUC");
    let seen: i64 = tx
        .query_one(&format!("SELECT count(*) FROM {leaf}"), &[])
        .expect("owner reads with the GUC")
        .get(0);
    assert_eq!(
        seen, 1,
        "{leaf}: the parent's tenant policy admits the tenant's row"
    );
}

/// T-C3 (ADR-0063 D-E): no runtime role holds any privilege on any leaf, and no ACL entry on a leaf (table or
/// column) names anyone but the owner, while the same role still writes through the parent. Fault: skip the REVOKE
/// loop in `control.partition_adopt_leaf` (0224) ⇒ the adopted heap keeps its legacy grants ⇒ red.
#[test]
fn runtime_roles_have_no_grant_on_any_leaf() {
    let Some(mut db) = head("runtime_roles_have_no_grant_on_any_leaf") else {
        return;
    };
    let c = db.client();
    let all = leaves(c);
    assert!(
        all.len() >= 4 * CONVERTED.len(),
        "leaves of the converted parents: {all:?}"
    );
    for (_, leaf) in &all {
        for role in RUNTIME_ROLES {
            let any: bool = c
                .query_one(
                    "SELECT has_table_privilege($1, to_regclass($2),
                              'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER, MAINTAIN')
                         OR has_any_column_privilege($1, to_regclass($2), 'SELECT, INSERT, UPDATE, REFERENCES')",
                    &[&role, leaf],
                )
                .expect("privilege probe")
                .get(0);
            assert!(!any, "{role} holds a privilege on leaf {leaf}");
        }
        let foreign: Vec<String> = c
            .query(
                "SELECT x.grantee::regrole::text || ' ' || x.privilege_type
                   FROM pg_class l, aclexplode(coalesce(l.relacl, acldefault('r', l.relowner))) x
                  WHERE l.oid = to_regclass($1) AND x.grantee <> l.relowner
                 UNION ALL
                 SELECT a.attname || ' ' || x.grantee::regrole::text || ' ' || x.privilege_type
                   FROM pg_attribute a, aclexplode(a.attacl) x WHERE a.attrelid = to_regclass($1)",
                &[leaf],
            )
            .expect("leaf ACL")
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert!(foreign.is_empty(), "{leaf}: ACL entries {foreign:?}");
    }
    let ids = tenant(c);
    let leaf = leaf_of(c, "ops.stage_runs", 0);
    let mut tx = c.transaction().expect("begin");
    // dep: PostgreSQL(role_gateway) — role switch: a runtime role writes through the parent, never a leaf
    tx.batch_execute("SET LOCAL ROLE role_gateway")
        .expect("gateway role");
    tx.execute("SELECT set_config('humaux.tenant_id', $1, true)", &[&ids.0])
        .expect("tenant GUC");
    let landed = insert_at(&mut tx, "STAGE_RUNS", "now()", &ids)
        .expect("role_gateway writes through the parent");
    assert_eq!(landed, leaf, "routed to the current month");
    let e = tx
        .query(&format!("SELECT 1 FROM {leaf}"), &[])
        .expect_err("a runtime role cannot read a leaf directly");
    assert_eq!(refused(&e).0, "42501", "{e}");
}

/// T-C4 (ADR-0063 D-E, D-F): a row whose key is in the next UTC month lands in that month's pre-created leaf, for
/// every converted table, with that leaf registered as exactly that month. Fault: an off-by-one month in
/// `control.partition_create_month` (0224) ⇒ a different leaf name or bound ⇒ red.
#[test]
fn a_next_month_row_lands_in_the_precreated_leaf() {
    let Some(mut db) = head("a_next_month_row_lands_in_the_precreated_leaf") else {
        return;
    };
    let c = db.client();
    let ids = tenant(c);
    let key_sql = format!("{} + interval '1 day 2 hours'", month(1));
    for (key, parent, _) in CONVERTED {
        let expected = leaf_of(c, parent, 1);
        let landed = insert_at(c, key, &key_sql, &ids).expect("next-month row");
        assert_eq!(landed, expected, "{parent}: the next month's leaf");
        let exact: bool = c
            .query_one(
                &format!(
                    "SELECT lower_bound = {} AND upper_bound = {} FROM control.partition_registry
                      WHERE table_key = $1 AND leaf_name = $2 AND state = 'ATTACHED'",
                    month(1),
                    month(2)
                ),
                &[&key, &landed],
            )
            .expect("registry row of the next-month leaf")
            .get(0);
        assert!(exact, "{landed}: registered as exactly the next UTC month");
    }
}

/// T-C5 (ADR-0063 D-F): with no DEFAULT partition, a row in the UTC month four months ahead has no leaf and is
/// refused with 23514 for every converted table, while the last instant of month +3 is still accepted. Fault: 0225
/// pre-creates 4 future months, or attaches a DEFAULT partition ⇒ the row is accepted ⇒ red.
#[test]
fn a_row_four_months_ahead_is_refused() {
    let Some(mut db) = head("a_row_four_months_ahead_is_refused") else {
        return;
    };
    let c = db.client();
    let ids = tenant(c);
    for (key, parent, _) in CONVERTED {
        let last = insert_at(
            c,
            key,
            &format!("{} - interval '1 microsecond'", month(4)),
            &ids,
        )
        .expect("the last instant of month +3");
        assert_eq!(
            last,
            leaf_of(c, parent, 3),
            "{parent}: month +3 is pre-created"
        );
        let e = insert_at(c, key, &month(4), &ids).expect_err("month +4 has no leaf");
        let (code, message) = refused(&e);
        assert_eq!(code, "23514", "{parent}: {e}");
        assert!(
            message.starts_with(&format!(
                "no partition of relation \"{}\" found for row",
                parent.split_once('.').map_or(parent, |(_, rel)| rel)
            )),
            "{parent}: {message}"
        );
    }
}

/// T-C6 (R-36(a), ADR-0063 D-D step 4): a runtime role with UPDATE on stage_runs / messages cannot move a row's
/// partition key, while other columns stay updatable; maintenance_receipts needs no trigger because no runtime role
/// holds UPDATE on it. Fault: drop the key-freeze trigger from 0225 ⇒ the row moves to next month's leaf ⇒ red.
#[test]
fn a_partition_key_update_is_refused() {
    let Some(mut db) = head("a_partition_key_update_is_refused") else {
        return;
    };
    let c = db.client();
    let ids = tenant(c);
    for (key, parent, column, other) in [
        (
            "STAGE_RUNS",
            "ops.stage_runs",
            "started_at",
            "finished_at = now()",
        ),
        (
            "MESSAGES",
            "private.messages",
            "created_at",
            "event_id = NULL",
        ),
    ] {
        let mut tx = c.transaction().expect("begin");
        // dep: PostgreSQL(role_gateway) — role switch: a runtime role holding UPDATE on the parent
        tx.batch_execute("SET LOCAL ROLE role_gateway")
            .expect("gateway role");
        tx.execute("SELECT set_config('humaux.tenant_id', $1, true)", &[&ids.0])
            .expect("tenant GUC");
        insert_at(&mut tx, key, "now()", &ids).expect("current-month row");
        let moved = tx.execute(
            &format!(
                "UPDATE {parent} SET {column} = {} WHERE tenant_id = $1::text::uuid",
                month(1)
            ),
            &[&ids.0],
        );
        let e = moved.expect_err("the partition key is frozen");
        let (code, message) = refused(&e);
        assert_eq!(code, "P0001", "{parent}: {e}");
        // The row trigger is cloned onto the leaf, so TG_TABLE_NAME names the leaf the row lives in.
        assert!(
            message.starts_with(&format!("partition key of {parent}_p"))
                && message.ends_with(" is frozen (ADR-0063 D-D)"),
            "{parent}: {message}"
        );
        tx.rollback().expect("rollback");
        let mut tx = c.transaction().expect("begin");
        // dep: PostgreSQL(role_gateway) — role switch: a runtime role holding UPDATE on the parent
        tx.batch_execute("SET LOCAL ROLE role_gateway")
            .expect("gateway role");
        tx.execute("SELECT set_config('humaux.tenant_id', $1, true)", &[&ids.0])
            .expect("tenant GUC");
        insert_at(&mut tx, key, "now()", &ids).expect("current-month row");
        let updated = tx
            .execute(
                &format!("UPDATE {parent} SET {other} WHERE tenant_id = $1::text::uuid"),
                &[&ids.0],
            )
            .expect("a non-key column stays updatable");
        assert_eq!(updated, 1, "{parent}");
        tx.rollback().expect("rollback");
    }
    for role in RUNTIME_ROLES {
        let update: bool = c
            .query_one(
                "SELECT has_table_privilege($1, 'ops.maintenance_receipts', 'UPDATE')
                     OR has_any_column_privilege($1, 'ops.maintenance_receipts', 'UPDATE')",
                &[&role],
            )
            .expect("privilege probe")
            .get(0);
        assert!(
            !update,
            "{role} may UPDATE ops.maintenance_receipts: it would need a key-freeze trigger"
        );
    }
}

/// Applies every migration body after `after` (4-digit stem) in file order, on this session.
fn apply_after(c: &mut Client, after: &str) {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("migrations dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .filter(|p| {
            p.file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.get(..after.len()))
                .is_some_and(|n| n > after)
        })
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no migration after {after}");
    for file in files {
        let sql = std::fs::read_to_string(&file).expect("migration body");
        c.batch_execute(&sql)
            .unwrap_or_else(|e| panic!("apply {}: {e:?}", file.display()));
    }
}

/// T-D5 (ADR-0063 D-D step 5, D-E): the conversions and the creator run in a session whose TimeZone is
/// Asia/Shanghai and still produce UTC month bounds: seeded history from two UTC months ago becomes the transitional
/// `_hist` leaf `[MINVALUE, next UTC month)` with its rows in it, an empty table's heap becomes the current UTC month,
/// every registered bound is a UTC month start, and the creator refuses a local (non-UTC) month start. Fault: the
/// 2-argument `date_trunc` in 0225..0227 ⇒ a +08:00 month start ⇒ the creator refuses it or a bound drifts ⇒ red.
#[test]
fn bounds_are_utc_month_starts_under_any_session_timezone() {
    let Some(mut db) = throwaway::db_through(
        "bounds_are_utc_month_starts_under_any_session_timezone",
        "c36_parts",
        Some("0224"),
    ) else {
        return;
    };
    let c = db.client();
    let ids = tenant(c);
    let history = format!("{} + interval '3 days'", month(-2));
    for key in ["STAGE_RUNS", "MAINTENANCE_RECEIPTS"] {
        insert_at(c, key, &history, &ids).expect("seed history into the heap");
    }
    c.batch_execute("SET TimeZone = 'Asia/Shanghai'")
        .expect("non-UTC session");
    apply_after(c, "0224");
    let not_utc: Option<String> = c
        .query_one(
            "SELECT string_agg(leaf_name, ', ') FROM control.partition_registry
              WHERE upper_bound <> date_trunc('month', upper_bound, 'UTC')
                 OR lower_bound <> date_trunc('month', lower_bound, 'UTC')",
            &[],
        )
        .expect("registry bounds")
        .get(0);
    assert_eq!(
        not_utc, None,
        "leaves whose bounds are not UTC month starts"
    );
    for (key, parent, first, rows) in [
        (
            "STAGE_RUNS",
            "ops.stage_runs",
            "ops.stage_runs_hist".to_owned(),
            1_i64,
        ),
        (
            "MAINTENANCE_RECEIPTS",
            "ops.maintenance_receipts",
            "ops.maintenance_receipts_hist".to_owned(),
            1,
        ),
        (
            "MESSAGES",
            "private.messages",
            leaf_of(c, "private.messages", 0),
            0,
        ),
    ] {
        let row = c
            .query_one(
                &format!(
                    "SELECT r.leaf_name, r.lower_bound IS NULL, r.upper_bound = {},
                            (SELECT count(*) FROM pg_inherits i WHERE i.inhparent = to_regclass($2))
                       FROM control.partition_registry r
                      WHERE r.table_key = $1 AND r.state = 'ATTACHED' ORDER BY r.upper_bound LIMIT 1",
                    month(1)
                ),
                &[&key, &parent],
            )
            .expect("first leaf");
        let shape: (String, bool, bool, i64) = (row.get(0), row.get(1), row.get(2), row.get(3));
        let hist = first.ends_with("_hist");
        assert_eq!(
            shape,
            (first.clone(), hist, true, 4),
            "{parent}: (first leaf, MINVALUE, upper, leaves)"
        );
        let held: i64 = c
            .query_one(&format!("SELECT count(*) FROM {first}"), &[])
            .expect("rows of the first leaf")
            .get(0);
        assert_eq!(
            held, rows,
            "{first}: the heap's rows stay in the adopted leaf"
        );
    }
    let next: String = c
        .query_one(
            "SELECT control.partition_create_month('MESSAGES', max(upper_bound))
               FROM control.partition_registry WHERE table_key = 'MESSAGES' AND state = 'ATTACHED'",
            &[],
        )
        .expect("the creator under Asia/Shanghai")
        .get(0);
    assert_eq!(next, "created");
    let e = c
        .query_one(
            &format!(
                "SELECT control.partition_create_month('MESSAGES', date_trunc('month', {}))",
                month(5)
            ),
            &[],
        )
        .expect_err("a +08:00 month start is not a UTC month start");
    assert_eq!(refused(&e).0, "22023", "{e}");
    let leaf = leaf_of(c, "private.messages", 4);
    let landed = insert_at(c, "MESSAGES", &month(4), &ids).expect("month +4 after the creator ran");
    assert_eq!(landed, leaf);
}

/// The body of the one migration whose file name starts with `stem`.
fn body(stem: &str) -> String {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let file = std::fs::read_dir(&dir)
        .expect("migrations dir")
        .map(|e| e.expect("entry").path())
        .find(|p| {
            p.extension().is_some_and(|x| x == "sql")
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(stem))
        })
        .unwrap_or_else(|| panic!("no migration {stem}"));
    std::fs::read_to_string(file).expect("migration body")
}

/// One ingest ticket of the tenant: REDEEMED to `event` when given, else ISSUED.
fn ticket(
    c: &mut impl postgres::GenericClient,
    ids: &(String, String),
    event: Option<&str>,
) -> Result<u64, postgres::Error> {
    c.execute(
        "INSERT INTO private.ingest_tickets
           (tenant_id, scope_kind, scope_id, batch_id, client_batch_id, ordinal, expires_at, state, redeemed_event_id)
         VALUES ($1::text::uuid, 'tenant', $1::text::uuid, $1::text::uuid, 'c36',
                 (SELECT count(*)::int FROM private.ingest_tickets WHERE tenant_id = $1::text::uuid),
                 now() + interval '1 day', CASE WHEN $2::text IS NULL THEN 'ISSUED' ELSE 'REDEEMED' END,
                 $2::text::uuid)",
        &[&ids.0, &event],
    )
}

/// `(sqlstate, constraint)` of a refused statement.
fn violated(e: &postgres::Error) -> (String, Option<String>) {
    let db = e
        .as_db_error()
        .unwrap_or_else(|| panic!("not a server error: {e}"));
    (
        db.code().code().to_owned(),
        db.constraint().map(str::to_owned),
    )
}

/// T-B2 (ADR-0063 D-B): with PK (event_id, recorded_at) on the parent, global event_id uniqueness lives in
/// `private.event_identity`: a second event for one evidence row is refused with 23505 on `event_identity_pkey`, in
/// another month and in the same one, also for role_gateway (the claim trigger is the owner's definer). Fault: drop
/// the `events_identity_claim` trigger from 0228 ⇒ the next-month duplicate lands ⇒ red.
#[test]
fn a_second_event_for_one_evidence_is_refused() {
    let Some(mut db) = head("a_second_event_for_one_evidence_is_refused") else {
        return;
    };
    let c = db.client();
    let ids = tenant(c);
    let id = evidence(c, &ids, "now()");
    let first: String = c
        .query_one(
            "INSERT INTO private.events (event_id, event_kind, payload)
             VALUES ($1::text::uuid, 'MANUAL_NOTE', '{}') RETURNING tableoid::regclass::text",
            &[&id],
        )
        .expect("the first event")
        .get(0);
    assert_eq!(
        first,
        leaf_of(c, "private.events", 0),
        "recorded_at DEFAULT now()"
    );
    for recorded_at in [month(1), "now()".to_owned()] {
        let mut tx = c.transaction().expect("begin");
        // dep: PostgreSQL(role_gateway) — role switch: the runtime writer of private.events
        tx.batch_execute("SET LOCAL ROLE role_gateway")
            .expect("gateway role");
        tx.execute("SELECT set_config('humaux.tenant_id', $1, true)", &[&ids.0])
            .expect("tenant GUC");
        let e = tx
            .execute(
                &format!(
                    "INSERT INTO private.events (event_id, event_kind, payload, recorded_at)
                     VALUES ($1::text::uuid, 'MANUAL_NOTE', '{{}}', {recorded_at})"
                ),
                &[&id],
            )
            .expect_err("one event per evidence row");
        assert_eq!(
            violated(&e),
            ("23505".to_owned(), Some("event_identity_pkey".to_owned())),
            "recorded_at = {recorded_at}: {e}"
        );
        tx.rollback().expect("rollback");
    }
    let counts: (i64, i64) = c
        .query_one(
            "SELECT (SELECT count(*) FROM private.events WHERE event_id = $1::text::uuid),
                    (SELECT count(*) FROM private.event_identity WHERE event_id = $1::text::uuid)",
            &[&id],
        )
        .map(|r| (r.get(0), r.get(1)))
        .expect("counts");
    assert_eq!(counts, (1, 1), "(events, identity rows) of {id}");
}

/// ADR-0063 D-B: the ticket and message FKs, re-pointed by 0228 to `private.event_identity`, still refuse an id with
/// no event (23503) and accept one with an event; a referenced event cannot be deleted (23503 through its identity
/// row), while an unreferenced event and then its evidence delete as before 0228 (the test-teardown path: the
/// AFTER DELETE release trigger). Fault: remove the re-created ticket FK from 0228 ⇒ the unknown id is accepted ⇒
/// red.
#[test]
fn event_references_resolve_through_the_identity_table() {
    let Some(mut db) = head("event_references_resolve_through_the_identity_table") else {
        return;
    };
    let c = db.client();
    let ids = tenant(c);
    let id = evidence(c, &ids, "now()");
    let e = ticket(c, &ids, Some(&id)).expect_err("a ticket redeemed to an id with no event");
    assert_eq!(
        violated(&e),
        (
            "23503".to_owned(),
            Some("ingest_tickets_redeemed_event_id_fkey".to_owned())
        ),
        "{e}"
    );
    let message = "INSERT INTO private.messages (tenant_id, conversation_id, event_id)
                   VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid)";
    let e = c
        .execute(message, &[&ids.0, &ids.1, &id])
        .expect_err("a message naming an id with no event");
    assert_eq!(
        violated(&e),
        (
            "23503".to_owned(),
            Some("messages_event_id_fkey".to_owned())
        ),
        "{e}"
    );
    c.execute(
        "INSERT INTO private.events (event_id, event_kind, payload) VALUES ($1::text::uuid, 'USER_MESSAGE', '{}')",
        &[&id],
    )
    .expect("the event");
    ticket(c, &ids, Some(&id)).expect("a ticket redeemed to an existing event");
    c.execute(message, &[&ids.0, &ids.1, &id])
        .expect("a message naming an existing event");
    let e = c
        .execute(
            "DELETE FROM private.events WHERE event_id = $1::text::uuid",
            &[&id],
        )
        .expect_err("a referenced event cannot be deleted");
    assert_eq!(violated(&e).0, "23503", "{e}");
    let free = evidence(c, &ids, "now()");
    c.execute(
        "INSERT INTO private.events (event_id, event_kind, payload) VALUES ($1::text::uuid, 'USER_MESSAGE', '{}')",
        &[&free],
    )
    .expect("an unreferenced event");
    c.execute(
        "DELETE FROM private.events WHERE event_id = $1::text::uuid",
        &[&free],
    )
    .expect("an unreferenced event deletes");
    c.execute(
        "DELETE FROM private.evidence_objects WHERE evidence_id = $1::text::uuid",
        &[&free],
    )
    .expect("its evidence then deletes: the identity row went with the event");
}

/// Both re-pointed FKs reference `private.event_identity`, and the one seeded ticket and message each resolve through
/// it to their event.
fn assert_references_resolve_through_identity(c: &mut Client) {
    let targets: Vec<String> = c
        .query_one(
            "SELECT array_agg(format('%s %s -> %s', conrelid::regclass, conname, confrelid::regclass) ORDER BY conname)
               FROM pg_constraint
              WHERE contype = 'f' AND conparentid = 0
                AND conname IN ('ingest_tickets_redeemed_event_id_fkey', 'messages_event_id_fkey')",
            &[],
        )
        .expect("FK targets")
        .get(0);
    assert_eq!(
        targets,
        [
            "private.ingest_tickets ingest_tickets_redeemed_event_id_fkey -> private.event_identity",
            "private.messages messages_event_id_fkey -> private.event_identity",
        ]
    );
    let resolved: (i64, i64) = c
        .query_one(
            "SELECT (SELECT count(*) FROM private.ingest_tickets t
                       JOIN private.event_identity i ON i.event_id = t.redeemed_event_id
                       JOIN private.events e ON e.event_id = i.event_id),
                    (SELECT count(*) FROM private.messages m
                       JOIN private.event_identity i ON i.event_id = m.event_id
                       JOIN private.events e ON e.event_id = i.event_id)",
            &[],
        )
        .map(|r| (r.get(0), r.get(1)))
        .expect("referencing rows");
    assert_eq!(
        resolved,
        (1, 1),
        "(tickets, messages) resolving to their event"
    );
}

/// role_gateway's `remember` statements, verbatim shape (`remember.rs`: evidence row, event subtype, ticket redeem),
/// with the evidence's reasoning domain taken from `template`; the leaf the event landed in.
fn remember_as_gateway(c: &mut Client, ids: &(String, String), template: &str) -> String {
    let mut tx = c.transaction().expect("begin");
    // dep: PostgreSQL(role_gateway) — role switch: remember's evidence + event + ticket statements, verbatim shape
    tx.batch_execute("SET LOCAL ROLE role_gateway")
        .expect("gateway role");
    tx.execute("SELECT set_config('humaux.tenant_id', $1, true)", &[&ids.0])
        .expect("tenant GUC");
    let new: String = tx
        .query_one(
            "INSERT INTO private.evidence_objects
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, visibility_class,
                reasoning_domain_id)
             SELECT $1::text::uuid, 'EVENT', sha256('c36'::bytea), 'INTERNAL', 'DirectUserInput', 'TENANT_SHARED',
                    eo.reasoning_domain_id
               FROM private.evidence_objects eo WHERE eo.evidence_id = $2::text::uuid
             RETURNING evidence_id::text",
            &[&ids.0, &template],
        )
        .expect("role_gateway writes evidence")
        .get(0);
    let landed: String = tx
        .query_one(
            "INSERT INTO private.events (event_id, event_kind, payload) VALUES ($1::text::uuid, $2, $3::text::jsonb)
             RETURNING tableoid::regclass::text",
            &[&new, &"MANUAL_NOTE", &"{}"],
        )
        .expect("role_gateway writes an event through the parent")
        .get(0);
    // L4: `_hist` is [MINVALUE, B) with B the month after now(), so the conversion month's rows land in it.
    assert_eq!(
        landed, "private.events_hist",
        "routed to the leaf holding the current month"
    );
    let redeemed = tx
        .execute(
            "UPDATE private.ingest_tickets SET redeemed_event_id = $2::text::uuid, state = 'REDEEMED'
              WHERE ticket_id = (SELECT ticket_id FROM private.ingest_tickets
                                  WHERE batch_id = $1::text::uuid AND state = 'ISSUED'
                                  ORDER BY ordinal LIMIT 1 FOR UPDATE SKIP LOCKED)",
            &[&ids.0, &new],
        )
        .expect("role_gateway redeems a ticket to the new event");
    assert_eq!(redeemed, 1);
    tx.commit().expect("commit");
    landed
}

/// T-C7, events part (ADR-0063 D-D): two UTC months of history seeded into the heap before 0228 (with a redeemed
/// ticket and a message pointing at it) survive the conversion: equal counts, one identity row per event, every row
/// in the transitional `_hist` leaf `[MINVALUE, next month)` with `recorded_at` = its evidence's `created_at`, both
/// referencing FKs on `private.event_identity` and every referencing row resolving through it to its event; then
/// role_gateway's `remember` statements (evidence, event, ticket redeem) still land through the parent, in the leaf
/// holding the current month. Faults: delete the identity backfill INSERT from 0228 ⇒ the 7a block raises `c36 identity drift`
/// ⇒ the apply panics ⇒ red; or the recorded_at backfill ⇒ SET NOT NULL fails ⇒ red.
#[test]
fn seeded_rows_survive_conversion_with_counts_identity_and_leaf_placement() {
    let Some(mut db) = throwaway::db_through(
        "seeded_rows_survive_conversion_with_counts_identity_and_leaf_placement",
        "c36_parts",
        Some("0227"),
    ) else {
        return;
    };
    let c = db.client();
    let ids = tenant(c);
    let old: Vec<String> = [-2, -1]
        .iter()
        .map(|m| evidence(c, &ids, &format!("{} + interval '3 days'", month(*m))))
        .collect();
    for id in &old {
        c.execute(
            "INSERT INTO private.events (event_id, event_kind, payload) VALUES ($1::text::uuid, 'USER_MESSAGE', '{}')",
            &[id],
        )
        .expect("seed an event into the heap");
    }
    ticket(c, &ids, Some(&old[0])).expect("a redeemed ticket");
    ticket(c, &ids, None).expect("an issued ticket");
    c.execute(
        "INSERT INTO private.messages (tenant_id, conversation_id, event_id)
         VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid)",
        &[&ids.0, &ids.1, &old[1]],
    )
    .expect("a message naming an event");
    let before: i64 = c
        .query_one("SELECT count(*) FROM private.events", &[])
        .expect("count before")
        .get(0);
    apply_after(c, "0227");

    let row = c
        .query_one(
            "SELECT (SELECT count(*) FROM private.events), (SELECT count(*) FROM private.event_identity),
                    (SELECT count(*) FROM private.events WHERE tableoid = to_regclass('private.events_hist')),
                    (SELECT count(*) FROM private.events e JOIN private.evidence_objects eo ON eo.evidence_id = e.event_id
                      WHERE e.recorded_at = eo.created_at)",
            &[],
        )
        .expect("counts after");
    let counts: (i64, i64, i64, i64) = (row.get(0), row.get(1), row.get(2), row.get(3));
    assert_eq!(
        counts,
        (before, before, before, before),
        "(events, identity rows, rows in events_hist, recorded_at = evidence created_at)"
    );
    let hist: bool = c
        .query_one(
            &format!(
                "SELECT lower_bound IS NULL AND upper_bound = {} FROM control.partition_registry
                  WHERE table_key = 'EVENTS' AND leaf_name = 'private.events_hist' AND state = 'ATTACHED'",
                month(1)
            ),
            &[],
        )
        .expect("the _hist registry row")
        .get(0);
    assert!(hist, "private.events_hist is [MINVALUE, next UTC month)");
    assert_references_resolve_through_identity(c);

    let landed = remember_as_gateway(c, &ids, &old[0]);
    // L4: `_hist` is [MINVALUE, B) with B the month after now(), so the conversion month's rows land in it.
    assert_eq!(
        landed, "private.events_hist",
        "routed to the leaf holding the current month"
    );
    let identity: i64 = c
        .query_one("SELECT count(*) FROM private.event_identity", &[])
        .expect("identity rows")
        .get(0);
    assert_eq!(
        identity,
        before + 1,
        "the claim trigger recorded the new event"
    );
}

/// Main-line dev orphan note (2026-10-05): rows written under `session_replication_role = replica` can violate a
/// validated FK. 0228's precheck refuses with one count per FK it re-creates before touching anything, and the heap
/// stays a heap; once the data is repaired the same body converts. Fault: delete the orphan count block from 0228 ⇒
/// the refusal is a mid-way FK error without the counts ⇒ the message assertion reds.
#[test]
fn a_conversion_with_orphan_event_references_is_refused() {
    let Some(mut db) = throwaway::db_through(
        "a_conversion_with_orphan_event_references_is_refused",
        "c36_parts",
        Some("0227"),
    ) else {
        return;
    };
    let c = db.client();
    let ids = tenant(c);
    let (a, b) = (evidence(c, &ids, "now()"), evidence(c, &ids, "now()"));
    for id in [&a, &b] {
        c.execute(
            "INSERT INTO private.events (event_id, event_kind, payload) VALUES ($1::text::uuid, 'USER_MESSAGE', '{}')",
            &[id],
        )
        .expect("seed an event");
    }
    ticket(c, &ids, Some(&b)).expect("a redeemed ticket");
    // dep: PostgreSQL(owner) — replica mode: plant one orphan event and one orphan ticket, as the dev teardowns did
    // replica-mode: throwaway database only (humaux_thread_c36_parts_<pid>_<n>, created by throwaway::db_through, dropped WITH (FORCE) by Db::drop)
    c.batch_execute(&format!(
        "SET session_replication_role = replica;
         DELETE FROM private.evidence_objects WHERE evidence_id = '{a}';
         DELETE FROM private.events WHERE event_id = '{b}';
         RESET session_replication_role;"
    ))
    .expect("plant orphans");
    let e = c
        .batch_execute(&body("0228"))
        .expect_err("orphan rows refuse the conversion");
    let message = e
        .as_db_error()
        .map(|d| d.message().to_owned())
        .unwrap_or_default();
    assert_eq!(
        message,
        "c36 precheck: events_event_id_fkey: 1 orphan rows - repair data first; \
         ingest_tickets_redeemed_event_id_fkey: 1 orphan rows - repair data first",
        "{e}"
    );
    let kind: String = c
        .query_one(
            "SELECT relkind::text FROM pg_class WHERE oid = 'private.events'::regclass",
            &[],
        )
        .expect("relkind")
        .get(0);
    assert_eq!(kind, "r", "private.events is still the heap");
    // replica-mode: throwaway database only (humaux_thread_c36_parts_<pid>_<n>, created by throwaway::db_through, dropped WITH (FORCE) by Db::drop)
    c.batch_execute(&format!(
        "SET session_replication_role = replica;
         DELETE FROM private.events WHERE event_id = '{a}';
         DELETE FROM private.ingest_tickets WHERE redeemed_event_id = '{b}';
         RESET session_replication_role;"
    ))
    .expect("repair the orphans");
    c.batch_execute(&body("0228"))
        .expect("repaired data converts");
}

/// One §77 audit row written the production way: `control.audit_event_insert` (the owner definer, the only writer)
/// called by role_gateway with the tenant GUC; `(audit_event_id, audit_seq, leaf)` of the row it wrote.
fn audit_as_gateway(
    c: &mut Client,
    tenant: &str,
    id: Option<&str>,
) -> Result<(String, i64, String), postgres::Error> {
    let mut tx = c.transaction()?;
    // dep: PostgreSQL(role_gateway) — role switch: the runtime caller of the audit insert definer
    tx.batch_execute("SET LOCAL ROLE role_gateway")?;
    tx.execute(
        "SELECT set_config('humaux.tenant_id', $1, true)",
        &[&tenant],
    )?;
    // dep: PostgreSQL(role_gateway) — control.audit_event_insert, the owner definer and only audit writer (0041)
    let id: String = tx
        .query_one(
            "SELECT control.audit_event_insert(coalesce($1::text::uuid, uuidv7()), now(), $2::text::uuid,
                      'user', 'c36', 'C36_AUDIT', 'resource', 'r1', 'OK', 'req', 'trace', NULL, '',
                      ARRAY[]::text[], NULL, NULL, '{}'::jsonb)::text",
            &[&id, &tenant],
        )?
        .get(0);
    let row = tx.query_one(
        "SELECT audit_seq, tableoid::regclass::text FROM control.audit_events
          WHERE audit_event_id = $1::text::uuid",
        &[&id],
    )?;
    let (seq, leaf) = (row.get(0), row.get(1));
    tx.commit()?;
    Ok((id, seq, leaf))
}

/// A bare tenant (no conversation), as text id.
fn bare_tenant(c: &mut Client) -> String {
    c.query_one(
        "INSERT INTO control.tenants (name) VALUES ('c36-audit') RETURNING tenant_id::text",
        &[],
    )
    .expect("tenant fixture")
    .get(0)
}

/// One operation receipt of `tenant` naming `audit_event_id`, with a real consumed reservation and quota window
/// behind it (every FK target real). Only the business-integrity trigger `operation_receipt_integrity` (evidence,
/// outbox and stream rows this file does not build) is disabled, inside the fixture's own transaction.
fn receipt(c: &mut Client, tenant: &str, audit_event_id: &str) {
    let mut tx = c.transaction().expect("begin");
    tx.batch_execute(
        "ALTER TABLE control.operation_receipts DISABLE TRIGGER operation_receipt_integrity",
    )
    .expect("disable the integrity trigger in this transaction");
    tx.execute(
        "WITH w AS (INSERT INTO control.quota_windows (tenant_id, entitlement_key, window_start, window_end, hard_limit)
                    VALUES ($1::text::uuid, 'c36', date_trunc('month', now(), 'UTC'),
                            date_add(date_trunc('month', now(), 'UTC'), interval '1 month', 'UTC'), 10)
                    RETURNING tenant_id, entitlement_key, window_start),
              r AS (INSERT INTO control.usage_reservations
                      (request_id, tenant_id, principal_id, entitlement_key, window_start, operation,
                       request_fingerprint, units, status, finished_at, expires_at)
                    SELECT uuidv7(), tenant_id, uuidv7(), entitlement_key, window_start, 'c36.audit', repeat('a', 64),
                           1, 'CONSUMED', clock_timestamp(), now() + interval '1 hour' FROM w
                    RETURNING reservation_id, request_id, tenant_id, principal_id, request_fingerprint)
         INSERT INTO control.operation_receipts
           (tenant_id, principal_id, operation, idempotency_key, request_fingerprint, request_id, reservation_id,
            scope_kind, scope_id, domain, projection_kind, projection_version, stream_seq, commit_seq,
            audit_event_id, replay_expires_at)
         SELECT tenant_id, principal_id, 'c36.audit', 'c36', request_fingerprint, request_id, reservation_id,
                'tenant', tenant_id, 'd', 'k', 'v', 1, 1, $2::text::uuid, now() + interval '1 day' FROM r",
        &[&tenant, &audit_event_id],
    )
    .expect("an operation receipt naming the audit row");
    tx.batch_execute(
        "ALTER TABLE control.operation_receipts ENABLE TRIGGER operation_receipt_integrity",
    )
    .expect("re-enable the integrity trigger");
    tx.commit().expect("commit");
}

/// T-D4 (ADR-0063 D-D step 4, F7): the identity added to the new parent starts at 1, so 0229 restarts it above the
/// legacy maximum and the legacy sequence position: with history in two past months and the legacy sequence moved
/// past its maximum (rolled-back inserts), the first audit row written through `control.audit_event_insert` after
/// the conversion takes exactly the value the legacy sequence would have handed out, and every audit_seq stays
/// unique. Fault: delete the RESTART block from 0229 ⇒ the next value is 1 ⇒ the body's 7a block raises `c36
/// audit_seq drift` ⇒ the apply panics ⇒ red.
#[test]
fn audit_seq_continues_above_the_legacy_maximum() {
    let Some(mut db) = throwaway::db_through(
        "audit_seq_continues_above_the_legacy_maximum",
        "c36_parts",
        Some("0228"),
    ) else {
        return;
    };
    let c = db.client();
    let tenant = bare_tenant(c);
    for m in [-2, -1, -1] {
        insert_at(
            c,
            "AUDIT_EVENTS",
            &format!("{} + interval '3 days'", month(m)),
            &(tenant.clone(), String::new()),
        )
        .expect("seed audit history into the heap");
    }
    let row = c
        .query_one(
            "SELECT max(audit_seq),
                    (SELECT max(nextval(pg_get_serial_sequence('control.audit_events', 'audit_seq')))
                       FROM generate_series(1, 5)) + 1
               FROM control.audit_events",
            &[],
        )
        .expect("legacy maximum and sequence position");
    let (max, legacy_next): (i64, i64) = (row.get(0), row.get(1));
    assert!(
        legacy_next > max + 1,
        "the legacy sequence is past its maximum"
    );
    apply_after(c, "0228");
    let identity: String = c
        .query_one(
            "SELECT a.attidentity::text FROM pg_attribute a
              WHERE a.attrelid = 'control.audit_events'::regclass AND a.attname = 'audit_seq'",
            &[],
        )
        .expect("audit_seq attribute")
        .get(0);
    assert_eq!(
        identity, "a",
        "audit_seq is GENERATED ALWAYS AS IDENTITY on the parent"
    );
    let (_, seq, leaf) = audit_as_gateway(c, &tenant, None).expect("an audit row after 0229");
    assert_eq!(
        seq, legacy_next,
        "the first audit_seq after the conversion continues the legacy sequence (legacy max {max})"
    );
    assert_eq!(
        leaf, "control.audit_events_hist",
        "routed to the leaf holding the current month"
    );
    let dupes: i64 = c
        .query_one(
            "SELECT count(*) - count(DISTINCT audit_seq) FROM control.audit_events",
            &[],
        )
        .expect("audit_seq uniqueness")
        .get(0);
    assert_eq!(
        dupes, 0,
        "audit_seq is unique across the legacy leaf and the new rows"
    );
}

/// T-B3 (ADR-0063 D-B): with PK (audit_event_id, occurred_at) and UNIQUE (audit_seq, occurred_at) on the parent,
/// global uniqueness lives in `control.audit_event_identity`: a second row with an existing audit_event_id is
/// refused with 23505 on `audit_event_identity_pkey` with another occurred_at (another month's leaf) and through the
/// production definer path, and a forced duplicate audit_seq in another month is refused on
/// `audit_event_identity_audit_seq_key`. Fault: drop the `audit_events_identity_claim` trigger from 0229 ⇒ the
/// next-month duplicate lands ⇒ red.
#[test]
fn a_duplicate_audit_event_id_with_another_ts_is_refused() {
    let Some(mut db) = head("a_duplicate_audit_event_id_with_another_ts_is_refused") else {
        return;
    };
    let c = db.client();
    let tenant = bare_tenant(c);
    let (id, seq, leaf) = audit_as_gateway(c, &tenant, None).expect("the first audit row");
    assert_eq!(leaf, leaf_of(c, "control.audit_events", 0));
    let e = c
        .execute(
            &format!(
                "INSERT INTO control.audit_events (audit_event_id, tenant_id, action, occurred_at)
                 VALUES ($1::text::uuid, $2::text::uuid, 'C36_DUP', {})",
                month(1)
            ),
            &[&id, &tenant],
        )
        .expect_err("the same audit_event_id in next month's leaf");
    assert_eq!(
        violated(&e),
        ("23505".into(), Some("audit_event_identity_pkey".into())),
        "{e}"
    );
    let e = audit_as_gateway(c, &tenant, Some(&id)).expect_err("the same id through the definer");
    assert_eq!(
        violated(&e),
        ("23505".into(), Some("audit_event_identity_pkey".into())),
        "{e}"
    );
    let e = c
        .execute(
            &format!(
                "INSERT INTO control.audit_events (tenant_id, action, occurred_at, audit_seq)
                 OVERRIDING SYSTEM VALUE VALUES ($1::text::uuid, 'C36_DUP', {}, $2)",
                month(2)
            ),
            &[&tenant, &seq],
        )
        .expect_err("the same audit_seq in another month's leaf");
    assert_eq!(
        violated(&e),
        (
            "23505".into(),
            Some("audit_event_identity_audit_seq_key".into())
        ),
        "{e}"
    );
    let rows: (i64, i64) = c
        .query_one(
            "SELECT (SELECT count(*) FROM control.audit_events), (SELECT count(*) FROM control.audit_event_identity)",
            &[],
        )
        .map(|r| (r.get(0), r.get(1)))
        .expect("counts");
    assert_eq!(rows.0, rows.1, "one identity row per audit row");
}

/// T-C7, audit part (ADR-0063 D-D): audit history in two past UTC months, one row named by an operation receipt,
/// survives 0229: equal counts, one identity row per audit row carrying its seq, time and tenant, every row in the
/// transitional `_hist` leaf `[MINVALUE, next month)`, the receipt FK on `control.audit_event_identity` resolving to
/// its audit row, and the receipt-referenced row still undeletable (23503 through the identity row) while an
/// unreferenced row deletes with its identity row (the teardown path, append-only guard disabled). Fault: delete the
/// identity backfill INSERT from 0229 ⇒ the receipt FK re-point fails to validate (or the 7a block raises `c36
/// identity drift`) ⇒ the apply panics ⇒ red.
#[test]
fn seeded_audit_rows_survive_conversion_with_identity_and_receipt_target() {
    let Some(mut db) = throwaway::db_through(
        "seeded_audit_rows_survive_conversion_with_identity_and_receipt_target",
        "c36_parts",
        Some("0228"),
    ) else {
        return;
    };
    let c = db.client();
    let tenant = bare_tenant(c);
    let ids = (tenant.clone(), String::new());
    for m in [-2, -2, -1] {
        insert_at(
            c,
            "AUDIT_EVENTS",
            &format!("{} + interval '3 days'", month(m)),
            &ids,
        )
        .expect("seed audit history into the heap");
    }
    let (named, _, _) = audit_as_gateway(c, &tenant, None).expect("an audit row for the receipt");
    receipt(c, &tenant, &named);
    let before: i64 = c
        .query_one("SELECT count(*) FROM control.audit_events", &[])
        .expect("count before")
        .get(0);
    apply_after(c, "0228");

    let row = c
        .query_one(
            "SELECT (SELECT count(*) FROM control.audit_events), (SELECT count(*) FROM control.audit_event_identity),
                    (SELECT count(*) FROM control.audit_events
                      WHERE tableoid = to_regclass('control.audit_events_hist')),
                    (SELECT count(*) FROM control.audit_events a JOIN control.audit_event_identity i
                        ON i.audit_event_id = a.audit_event_id AND i.audit_seq = a.audit_seq
                       AND i.occurred_at = a.occurred_at AND i.tenant_id = a.tenant_id)",
            &[],
        )
        .expect("counts after");
    let counts: (i64, i64, i64, i64) = (row.get(0), row.get(1), row.get(2), row.get(3));
    assert_eq!(
        counts,
        (before, before, before, before),
        "(audit rows, identity rows, rows in audit_events_hist, identity rows equal to their audit row)"
    );
    let hist: bool = c
        .query_one(
            &format!(
                "SELECT lower_bound IS NULL AND upper_bound = {} FROM control.partition_registry
                  WHERE table_key = 'AUDIT_EVENTS' AND leaf_name = 'control.audit_events_hist'
                    AND state = 'ATTACHED'",
                month(1)
            ),
            &[],
        )
        .expect("the _hist registry row")
        .get(0);
    assert!(
        hist,
        "control.audit_events_hist is [MINVALUE, next UTC month)"
    );
    let target: (String, String) = c
        .query_one(
            "SELECT co.confrelid::regclass::text,
                    (SELECT a.action FROM control.operation_receipts r
                       JOIN control.audit_event_identity i ON i.audit_event_id = r.audit_event_id
                       JOIN control.audit_events a ON a.audit_event_id = i.audit_event_id
                      WHERE r.tenant_id = $1::text::uuid)
               FROM pg_constraint co WHERE co.conname = 'operation_receipts_audit_event_id_fkey'",
            &[&tenant],
        )
        .map(|r| (r.get(0), r.get(1)))
        .expect("the receipt FK target");
    assert_eq!(
        target,
        ("control.audit_event_identity".into(), "C36_AUDIT".into()),
        "(FK target, the receipt's audit row through it)"
    );
    assert_audit_delete_follows_its_identity_row(c, &tenant, &named, before);
}

/// The teardown path after 0229 (append-only guard disabled inside one rolled-back transaction): the
/// receipt-referenced audit row `named` refuses deletion with 23503 through its identity row, and the three
/// unreferenced `C36_FIXTURE` rows delete together with their identity rows (the release trigger), out of `before`.
fn assert_audit_delete_follows_its_identity_row(
    c: &mut Client,
    tenant: &str,
    named: &str,
    before: i64,
) {
    let mut tx = c.transaction().expect("begin");
    // dep: PostgreSQL(owner) — the teardown path: append-only guard disabled inside this transaction only
    tx.batch_execute(
        "ALTER TABLE control.audit_events DISABLE TRIGGER audit_events_reject_mutation",
    )
    .expect("disable the guard in this transaction");
    let mut sp = tx.transaction().expect("savepoint");
    let e = sp
        .execute(
            "DELETE FROM control.audit_events WHERE audit_event_id = $1::text::uuid",
            &[&named],
        )
        .expect_err("a receipt-referenced audit row");
    assert_eq!(
        violated(&e),
        (
            "23503".into(),
            Some("operation_receipts_audit_event_id_fkey".into())
        ),
        "{e}"
    );
    sp.rollback().expect("rollback to savepoint");
    let gone = tx
        .execute(
            "DELETE FROM control.audit_events WHERE tenant_id = $1::text::uuid AND action = 'C36_FIXTURE'",
            &[&tenant],
        )
        .expect("unreferenced audit rows delete");
    let left: i64 = tx
        .query_one("SELECT count(*) FROM control.audit_event_identity", &[])
        .expect("identity rows")
        .get(0);
    assert_eq!(
        (gone, left),
        (3, before - 3),
        "(deleted audit rows, identity rows left): the release trigger follows a row DELETE"
    );
    tx.rollback().expect("rollback");
}

/// §77 guards and the card-33 write path after 0229: role_gateway writes through `control.audit_event_insert` with
/// the tenant GUC (the claim runs as the owner under FORCE RLS on the identity table, so the GUC is still required:
/// without it the insert is refused 42501 and nothing is written); UPDATE and DELETE are refused on the parent and on
/// a leaf directly (the row guard is cloned), and TRUNCATE on the parent and on every leaf (the statement guard is
/// copied by `partition_adopt_leaf`), for the superuser too. Fault: drop the TRUNCATE guard copy from
/// `control.partition_adopt_leaf` (0224) ⇒ the leaf truncates ⇒ red.
#[test]
fn audit_guards_and_the_tenant_guc_insert_definer_survive_the_conversion() {
    let Some(mut db) =
        head("audit_guards_and_the_tenant_guc_insert_definer_survive_the_conversion")
    else {
        return;
    };
    let c = db.client();
    let tenant = bare_tenant(c);
    let (id, _, leaf) =
        audit_as_gateway(c, &tenant, None).expect("the definer path with the tenant GUC");
    assert_eq!(leaf, leaf_of(c, "control.audit_events", 0));
    let mut tx = c.transaction().expect("begin");
    // dep: PostgreSQL(role_gateway) — role switch: the definer called without the tenant GUC
    tx.batch_execute("SET LOCAL ROLE role_gateway")
        .expect("gateway role");
    let e = tx
        .query_one(
            "SELECT control.audit_event_insert(uuidv7(), now(), $1::text::uuid, 'user', 'c36', 'C36_AUDIT',
                      'resource', 'r1', 'OK', 'req', 'trace', NULL, '', ARRAY[]::text[], NULL, NULL, '{}'::jsonb)",
            &[&tenant],
        )
        .expect_err("FORCE RLS applies to the owner definer");
    assert_eq!(refused(&e).0, "42501", "{e}");
    tx.rollback().expect("rollback");
    let written: i64 = c
        .query_one("SELECT count(*) FROM control.audit_event_identity", &[])
        .expect("identity rows")
        .get(0);
    assert_eq!(written, 1, "the refused insert wrote no identity row");
    let guard = "control.audit_events is append-only (§77 Audit Immutability) — ";
    for target in ["control.audit_events".to_owned(), leaf.clone()] {
        for (op, sql) in [
            (
                "UPDATE",
                format!("UPDATE {target} SET action = 'X' WHERE audit_event_id = $1::text::uuid"),
            ),
            (
                "DELETE",
                format!("DELETE FROM {target} WHERE audit_event_id = $1::text::uuid"),
            ),
        ] {
            let e = c.execute(&sql, &[&id]).expect_err("append-only");
            assert_eq!(
                refused(&e),
                ("42501".into(), format!("{guard}{op} not permitted")),
                "{target}"
            );
        }
    }
    let mut targets = vec!["control.audit_events".to_owned()];
    targets.extend(
        leaves(c)
            .into_iter()
            .filter(|(p, _)| p == "control.audit_events")
            .map(|(_, l)| l),
    );
    assert_eq!(
        targets.len(),
        5,
        "the parent and its four leaves: {targets:?}"
    );
    for target in targets {
        let e = c
            .batch_execute(&format!("TRUNCATE {target}"))
            .expect_err("TRUNCATE is refused");
        assert_eq!(
            refused(&e),
            ("42501".into(), format!("{guard}TRUNCATE not permitted")),
            "{target}"
        );
    }
}

/// Main-line dev orphan note (2026-10-05): 0229's precheck refuses with one count per FK it re-creates (actor user,
/// tenant, operation receipt) before touching anything, and the heap stays a heap; once the data is repaired the same
/// body converts. Fault: delete the orphan count block from 0229 ⇒ the refusal is a mid-way FK error without the
/// counts ⇒ the message assertion reds.
#[test]
fn a_conversion_with_orphan_audit_references_is_refused() {
    let Some(mut db) = throwaway::db_through(
        "a_conversion_with_orphan_audit_references_is_refused",
        "c36_parts",
        Some("0228"),
    ) else {
        return;
    };
    let c = db.client();
    let (gone, kept) = (bare_tenant(c), bare_tenant(c));
    let user: String = c
        .query_one(
            "INSERT INTO control.users (user_id) VALUES (gen_random_uuid()) RETURNING user_id::text",
            &[],
        )
        .expect("user fixture")
        .get(0);
    let orphaned =
        |c: &mut Client, tenant: &str| audit_as_gateway(c, tenant, None).expect("audit row").0;
    let (a, b) = (orphaned(c, &gone), orphaned(c, &kept));
    let d = orphaned(c, &kept);
    c.execute(
        "INSERT INTO control.audit_events (tenant_id, actor_user_id, action) VALUES ($1::text::uuid, $2::text::uuid, 'C36_ACTOR')",
        &[&kept, &user],
    )
    .expect("an audit row naming a user");
    receipt(c, &kept, &b);
    // dep: PostgreSQL(owner) — replica mode: plant one orphan per FK, as the dev teardowns did
    // replica-mode: throwaway database only (humaux_thread_c36_parts_<pid>_<n>, created by throwaway::db_through, dropped WITH (FORCE) by Db::drop)
    c.batch_execute(&format!(
        "SET session_replication_role = replica;
         DELETE FROM control.audit_events WHERE audit_event_id IN ('{a}', '{b}');
         INSERT INTO control.audit_events (tenant_id, action) VALUES ('{gone}', 'C36_ORPHAN');
         DELETE FROM control.tenants WHERE tenant_id = '{gone}';
         DELETE FROM control.users WHERE user_id = '{user}';
         RESET session_replication_role;"
    ))
    .expect("plant orphans");
    let e = c
        .batch_execute(&body("0229"))
        .expect_err("orphan rows refuse the conversion");
    let message = e
        .as_db_error()
        .map(|d| d.message().to_owned())
        .unwrap_or_default();
    assert_eq!(
        message,
        "c36 precheck: audit_events_actor_user_id_fkey: 1 orphan rows - repair data first; \
         audit_events_tenant_id_fkey: 1 orphan rows - repair data first; \
         operation_receipts_audit_event_id_fkey: 1 orphan rows - repair data first",
        "{e}"
    );
    let kind: String = c
        .query_one(
            "SELECT relkind::text FROM pg_class WHERE oid = 'control.audit_events'::regclass",
            &[],
        )
        .expect("relkind")
        .get(0);
    assert_eq!(kind, "r", "control.audit_events is still the heap");
    // replica-mode: throwaway database only (humaux_thread_c36_parts_<pid>_<n>, created by throwaway::db_through, dropped WITH (FORCE) by Db::drop)
    c.batch_execute(&format!(
        "SET session_replication_role = replica;
         DELETE FROM control.audit_events WHERE tenant_id = '{gone}' OR actor_user_id = '{user}';
         DELETE FROM control.operation_receipts WHERE audit_event_id = '{b}';
         RESET session_replication_role;"
    ))
    .expect("repair the orphans");
    c.batch_execute(&body("0229"))
        .expect("repaired data converts");
    let kept_row: i64 = c
        .query_one(
            "SELECT count(*) FROM control.audit_events WHERE audit_event_id = $1::text::uuid",
            &[&d],
        )
        .expect("the untouched row")
        .get(0);
    assert_eq!(kept_row, 1, "rows that were never orphans survive");
}

/// One ledger row of `tenant` (a retrieval-plane `embedding` call of provider and model `c36`) at `called_at_sql`
/// with `status`, as text id.
fn ledger_row(c: &mut Client, tenant: &str, called_at_sql: &str, status: &str) -> String {
    c.query_one(
        &format!(
            "INSERT INTO ops.model_call_ledger (tenant_id, provider, model, purpose, status, called_at)
             VALUES ($1::text::uuid, 'c36', 'c36', 'embedding', $2, {called_at_sql}) RETURNING model_call_id::text"
        ),
        &[&tenant, &status],
    )
    .expect("ledger fixture")
    .get(0)
}

/// One budget reservation of `tenant` naming the RESERVED [`ledger_row`] `model_call_id`
/// (`ops.retrieval_provider_budget_reservations`, the first of the five referencers; its guard wants that match).
fn budget_reservation(
    c: &mut impl postgres::GenericClient,
    tenant: &str,
    model_call_id: &str,
) -> Result<u64, postgres::Error> {
    c.execute(
        "INSERT INTO ops.retrieval_provider_budget_reservations
           (tenant_id, model_call_id, provider_id, model_id, region, purpose, requested_tokens, ttl_micros,
            reserved_at, expires_at, status)
         VALUES ($1::text::uuid, $2::text::uuid, 'c36', 'c36', 'c36', 'embedding', 1, 1, now(),
                 now() + interval '1 hour', 'RESERVED')",
        &[&tenant, &model_call_id],
    )
}

/// `INSERT … RETURNING` of one retrieval-plane ledger row with `request_id` at `called_at_sql`, as
/// role_retrieval_worker with the tenant GUC (a runtime writer's session): the rows it returned.
fn insert_request(
    tx: &mut postgres::Transaction<'_>,
    tenant: &str,
    request_id: &str,
    called_at_sql: &str,
) -> Result<usize, postgres::Error> {
    // dep: PostgreSQL(role_retrieval_worker) — role switch: the retrieval writer's session
    tx.batch_execute("SET LOCAL ROLE role_retrieval_worker")?;
    tx.execute(
        "SELECT set_config('humaux.tenant_id', $1, true)",
        &[&tenant],
    )?;
    tx.query(
        &format!(
            "INSERT INTO ops.model_call_ledger (request_id, tenant_id, provider, purpose, called_at)
             VALUES ($1::text::uuid, $2::text::uuid, 'c36', 'embedding', {called_at_sql}) RETURNING model_call_id"
        ),
        &[&request_id, &tenant],
    )
    .map(|rows| rows.len())
}

/// Waits until a backend of this database other than `except` waits on a lock (the concurrent duplicate).
fn await_lock_waiter(c: &mut Client, except: &[i32]) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let waiting: i64 = c
            .query_one(
                "SELECT count(*) FROM pg_stat_activity
                  WHERE datname = current_database() AND wait_event_type = 'Lock' AND NOT pid = ANY($1)",
                &[&except],
            )
            .expect("pg_stat_activity")
            .get(0);
        if waiting > 0 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the duplicate never waited on the uncommitted reservation"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// T-B1 (ADR-0063 D-B, ruling E3-b): with PK (model_call_id, called_at) on the parent, the former (tenant_id,
/// request_id) UNIQUE lives on `ops.model_call_identity` and the claim trigger enforces it: a duplicate request_id
/// from a runtime writer, in the same month or another one, is refused 23505 on
/// `model_call_identity_request_id_unique` (no skip, so a plain duplicate INSERT keeps erring); a concurrent duplicate
/// waits for the uncommitted first one, then gets the same 23505. The writer's return of the existing row is
/// `a_duplicate_reserve_returns_the_existing_row_never_a_second_one`. Fault: the claim back to RETURN NULL ⇒ the
/// duplicate returns 0 rows without an error ⇒ red.
#[test]
fn a_duplicate_request_id_is_refused_by_the_identity_claim() {
    let Some(mut db) = head("a_duplicate_request_id_is_refused_by_the_identity_claim") else {
        return;
    };
    let dsn = throwaway::with_db(&db.owner_dsn, &db.name);
    let c = db.client();
    let tenant = bare_tenant(c);
    let request: String = c
        .query_one("SELECT gen_random_uuid()::text", &[])
        .expect("request id")
        .get(0);
    let refused = (
        "23505".to_owned(),
        Some("model_call_identity_request_id_unique".to_owned()),
    );
    let mut tx = c.transaction().expect("begin");
    assert_eq!(
        insert_request(&mut tx, &tenant, &request, "now()").expect("first reservation"),
        1
    );
    tx.commit().expect("commit");
    for called_at in ["now()".to_owned(), month(1)] {
        let mut tx = c.transaction().expect("begin");
        let e = insert_request(&mut tx, &tenant, &request, &called_at)
            .expect_err("a duplicate request_id is refused, never skipped");
        assert_eq!(violated(&e), refused, "called_at = {called_at}: {e}");
        tx.rollback().expect("rollback");
    }

    // Concurrent: the second writer waits on the first's uncommitted claim, then gets the same 23505.
    let racing: String = c
        .query_one("SELECT gen_random_uuid()::text", &[])
        .expect("request id")
        .get(0);
    // dep: PostgreSQL(owner) — the first writer's own session on the throwaway
    let mut a = Client::connect(&dsn, postgres::NoTls).expect("first writer");
    let a_pid: i32 = a
        .query_one("SELECT pg_backend_pid()", &[])
        .expect("pid")
        .get(0);
    let mut a_tx = a.transaction().expect("begin");
    assert_eq!(
        insert_request(&mut a_tx, &tenant, &racing, "now()").expect("uncommitted first"),
        1
    );
    let (b_dsn, b_tenant, b_request) = (dsn.clone(), tenant.clone(), racing.clone());
    let second = std::thread::spawn(move || {
        // dep: PostgreSQL(owner) — the concurrent duplicate's own session on the throwaway
        let mut b = Client::connect(&b_dsn, postgres::NoTls).expect("second writer");
        let mut b_tx = b.transaction().expect("begin");
        let got =
            insert_request(&mut b_tx, &b_tenant, &b_request, "now()").map_err(|e| violated(&e));
        b_tx.rollback().expect("rollback");
        got
    });
    await_lock_waiter(c, &[a_pid]);
    a_tx.commit().expect("commit the first");
    assert_eq!(
        second.join().expect("second writer"),
        Err(refused),
        "the waiting duplicate is refused after the first commits"
    );

    let counts: (i64, i64) = c
        .query_one(
            "SELECT (SELECT count(*) FROM ops.model_call_ledger WHERE tenant_id = $1::text::uuid),
                    (SELECT count(*) FROM ops.model_call_identity WHERE tenant_id = $1::text::uuid)",
            &[&tenant],
        )
        .map(|r| (r.get(0), r.get(1)))
        .expect("counts");
    assert_eq!(
        counts,
        (2, 2),
        "(ledger rows, identity rows): one per request_id"
    );
}

/// ADR-0063 E3 condition (main-line ruling): the writer keeps the idempotency its `ON CONFLICT` gave: a duplicate
/// `reserve_call` returns the existing reservation (same model_call_id, `already_reserved`) and never a second row,
/// when the first row lives in another month's leaf, and when the first is still uncommitted under the writers'
/// `model-call-request:` advisory lock (it waits, ruling E3-b). Fault: drop the writer's lookup ⇒ the retry's INSERT
/// is refused 23505 by the identity claim ⇒ red.
#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one writer, three retry shapes (same leaf, another leaf, concurrent) against one throwaway"
)]
fn a_duplicate_reserve_returns_the_existing_row_never_a_second_one() {
    let Ok(worker_dsn) = std::env::var("HUMAUX_RETRIEVAL_WORKER_PG_DSN") else {
        humaux_testkit::skip_or_fail(
            "a_duplicate_reserve_returns_the_existing_row_never_a_second_one",
            "missing object: HUMAUX_RETRIEVAL_WORKER_PG_DSN",
            humaux_testkit::ExternalDep::Postgres,
        );
        return;
    };
    let Some(mut db) = head("a_duplicate_reserve_returns_the_existing_row_never_a_second_one")
    else {
        return;
    };
    let dsn = throwaway::with_db(&db.owner_dsn, &db.name);
    let worker_dsn = throwaway::with_db(&worker_dsn, &db.name);
    let c = db.client();
    let tenant = bare_tenant(c);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    // dep: PostgreSQL(role_retrieval_worker) — the writer's checked pool on the throwaway
    let pool = rt
        .block_on(humaux_adapters::postgres::RetrievalWorkerDbPool::connect(
            &worker_dsn,
        ))
        .expect("retrieval worker pool");
    let reserve = |request: uuid::Uuid| {
        let input = humaux_adapters::model_call_ledger::ReserveCall {
            request_id: Some(request),
            tenant_id: tenant.parse().expect("tenant uuid"),
            workspace_id: None,
            purpose: Some(humaux_domain::ledger::ModelCallPurpose::Rerank),
            provider: "c36".to_owned(),
            model: Some("c36".to_owned()),
            model_revision: None,
            estimated_cost: Some(0.01),
        };
        rt.block_on(humaux_adapters::model_call_ledger::reserve_call(
            &pool, &input,
        ))
        .expect("reserve")
    };

    let request = uuid::Uuid::now_v7();
    let (first, again) = (reserve(request), reserve(request));
    assert_eq!(
        (
            first.already_reserved,
            again.already_reserved,
            again.model_call_id
        ),
        (false, true, first.model_call_id),
        "a retried reserve returns the original reservation"
    );

    // The first reservation in next month's leaf (written by the owner), the retry now: still the existing row.
    let earlier = uuid::Uuid::now_v7();
    let existing: String = c
        .query_one(
            &format!(
                "INSERT INTO ops.model_call_ledger (request_id, tenant_id, provider, purpose, called_at)
                 VALUES ($1::text::uuid, $2::text::uuid, 'c36', 'rerank', {}) RETURNING model_call_id::text",
                month(1)
            ),
            &[&earlier.to_string(), &tenant],
        )
        .expect("a reservation in another leaf")
        .get(0);
    let retried = reserve(earlier);
    assert_eq!(
        (retried.already_reserved, retried.model_call_id.to_string()),
        (true, existing),
        "a retry across leaves returns the existing reservation"
    );

    // Concurrent: the first writer holds the writers' advisory lock (ruling E3-b) with its row uncommitted; the retry
    // waits on that lock, then returns the committed reservation.
    let racing = uuid::Uuid::now_v7();
    // dep: PostgreSQL(owner) — the first writer's own session on the throwaway
    let mut a = Client::connect(&dsn, postgres::NoTls).expect("first writer");
    let a_pid: i32 = a
        .query_one("SELECT pg_backend_pid()", &[])
        .expect("pid")
        .get(0);
    let mut a_tx = a.transaction().expect("begin");
    a_tx.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended('model-call-request:' || $1 || ':' || $2, 0))",
        &[&tenant, &racing.to_string()],
    )
    .expect("the writers' request lock");
    let held: String = a_tx
        .query_one(
            "INSERT INTO ops.model_call_ledger (request_id, tenant_id, provider, purpose)
             VALUES ($1::text::uuid, $2::text::uuid, 'c36', 'rerank') RETURNING model_call_id::text",
            &[&racing.to_string(), &tenant],
        )
        .expect("uncommitted first reservation")
        .get(0);
    std::thread::scope(|scope| {
        let waiter = scope.spawn(|| reserve(racing));
        await_lock_waiter(c, &[a_pid]);
        a_tx.commit().expect("commit the first");
        let got = waiter.join().expect("waiting reserve");
        assert_eq!(
            (got.already_reserved, got.model_call_id.to_string()),
            (true, held),
            "the waiting retry returns the committed reservation"
        );
    });

    let rows: Vec<(String, i64)> = c
        .query(
            "SELECT request_id::text, count(*) FROM ops.model_call_ledger WHERE tenant_id = $1::text::uuid
              GROUP BY 1 ORDER BY 1",
            &[&tenant],
        )
        .expect("rows per request")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(rows.len(), 3, "three logical calls: {rows:?}");
    assert!(
        rows.iter().all(|(_, n)| *n == 1),
        "never a second row: {rows:?}"
    );
}

/// T-C7, ledger part (ADR-0063 D-B, D-D): a heap seeded across two past UTC months (RESERVED and SUCCEEDED rows, a
/// budget reservation naming one) converts losslessly: every row and every RESERVED row survives in
/// `ops.model_call_ledger_hist [MINVALUE, next UTC month)`, each has its identical identity row, the five referencer
/// FKs are validated on `ops.model_call_identity` with their columns unchanged, the reservation still resolves, and a
/// reservation naming an unknown call is refused 23503. Fault: delete the identity backfill INSERT from 0230 ⇒ the
/// re-pointed FK's VALIDATE or the 7a identity count refuses ⇒ the apply panics ⇒ red.
#[test]
#[allow(
    clippy::too_many_lines,
    reason = "seed, convert, then assert counts, placement, the five FK targets and their enforcement on one copy"
)]
fn seeded_ledger_rows_survive_conversion_with_identity_and_referencer_targets() {
    let Some(mut db) = throwaway::db_through(
        "seeded_ledger_rows_survive_conversion_with_identity_and_referencer_targets",
        "c36_parts",
        Some("0229"),
    ) else {
        return;
    };
    let c = db.client();
    let tenant = bare_tenant(c);
    let mut named = String::new();
    for (m, status) in [
        (-2, "RESERVED"),
        (-2, "SUCCEEDED"),
        (-1, "FAILED"),
        (-1, "RESERVED"),
    ] {
        named = ledger_row(
            c,
            &tenant,
            &format!("{} + interval '3 days'", month(m)),
            status,
        );
    }
    budget_reservation(c, &tenant, &named).expect("a reservation naming a ledger row");
    let before: (i64, i64) = c
        .query_one(
            "SELECT count(*), count(*) FILTER (WHERE status = 'RESERVED') FROM ops.model_call_ledger",
            &[],
        )
        .map(|r| (r.get(0), r.get(1)))
        .expect("counts before");
    apply_after(c, "0229");

    let row = c
        .query_one(
            "SELECT (SELECT count(*) FROM ops.model_call_ledger),
                    (SELECT count(*) FROM ops.model_call_ledger WHERE status = 'RESERVED'),
                    (SELECT count(*) FROM ops.model_call_identity),
                    (SELECT count(*) FROM ops.model_call_ledger
                      WHERE tableoid = to_regclass('ops.model_call_ledger_hist')),
                    (SELECT count(*) FROM ops.model_call_ledger l JOIN ops.model_call_identity i
                        ON i.model_call_id = l.model_call_id AND i.tenant_id = l.tenant_id
                       AND i.request_id = l.request_id AND i.called_at = l.called_at
                       AND i.egress_processor_id IS NOT DISTINCT FROM l.egress_processor_id
                       AND i.binding_id IS NOT DISTINCT FROM l.binding_id
                       AND i.binding_version IS NOT DISTINCT FROM l.binding_version
                       AND i.reasoning_domain_id IS NOT DISTINCT FROM l.reasoning_domain_id
                       AND i.profile_version IS NOT DISTINCT FROM l.profile_version)",
            &[],
        )
        .expect("counts after");
    let after: (i64, i64, i64, i64, i64) =
        (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
    assert_eq!(
        after,
        (before.0, before.1, before.0, before.0, before.0),
        "(rows, RESERVED rows, identity rows, rows in model_call_ledger_hist, identity rows equal to their row)"
    );
    let hist: bool = c
        .query_one(
            &format!(
                "SELECT lower_bound IS NULL AND upper_bound = {} FROM control.partition_registry
                  WHERE table_key = 'MODEL_CALL_LEDGER' AND leaf_name = 'ops.model_call_ledger_hist'
                    AND state = 'ATTACHED'",
                month(1)
            ),
            &[],
        )
        .expect("the _hist registry row")
        .get(0);
    assert!(
        hist,
        "ops.model_call_ledger_hist is [MINVALUE, next UTC month)"
    );
    let fks: Vec<String> = c
        .query(
            "SELECT format('%s %s %s (%s) -> (%s)', co.conname, co.convalidated::text, co.confrelid::regclass,
                           (SELECT string_agg(a.attname, ', ' ORDER BY k.n)
                              FROM unnest(co.conkey) WITH ORDINALITY k(att, n)
                              JOIN pg_attribute a ON a.attrelid = co.conrelid AND a.attnum = k.att),
                           (SELECT string_agg(a.attname, ', ' ORDER BY k.n)
                              FROM unnest(co.confkey) WITH ORDINALITY k(att, n)
                              JOIN pg_attribute a ON a.attrelid = co.confrelid AND a.attnum = k.att))
               FROM pg_constraint co
              WHERE co.contype = 'f' AND co.conparentid = 0
                AND co.confrelid IN (to_regclass('ops.model_call_identity'), to_regclass('ops.model_call_ledger'))
                AND co.conrelid <> to_regclass('ops.model_call_ledger')
              ORDER BY 1",
            &[],
        )
        .expect("referencer FKs")
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(
        fks,
        [
            "contribution_candidates_reasoning_model_call_exact_fk true ops.model_call_identity (tenant_id, \
             model_call_id, binding_id, binding_version, reasoning_domain_id, profile_version) -> (tenant_id, \
             model_call_id, binding_id, binding_version, reasoning_domain_id, profile_version)",
            "contribution_executions_assessment_model_call_fk true ops.model_call_identity (tenant_id, \
             assessment_model_call_id) -> (tenant_id, model_call_id)",
            "contribution_executions_coverage_model_call_fk true ops.model_call_identity (tenant_id, \
             coverage_model_call_id) -> (tenant_id, model_call_id)",
            "data_disclosures_reasoning_model_call_fk true ops.model_call_identity (tenant_id, model_call_id, \
             processor_id) -> (tenant_id, model_call_id, egress_processor_id)",
            "retrieval_provider_budget_reservations_model_call_fk true ops.model_call_identity (tenant_id, \
             model_call_id) -> (tenant_id, model_call_id)",
        ],
        "the five referencer FKs, validated, columns unchanged, on the identity table"
    );
    let resolved: String = c
        .query_one(
            "SELECT l.status FROM ops.retrieval_provider_budget_reservations r
               JOIN ops.model_call_ledger l ON l.tenant_id = r.tenant_id AND l.model_call_id = r.model_call_id
              WHERE r.tenant_id = $1::text::uuid",
            &[&tenant],
        )
        .expect("the reservation's ledger row")
        .get(0);
    assert_eq!(resolved, "RESERVED");
    let mut tx = c.transaction().expect("begin");
    // dep: PostgreSQL(owner) — the reservation guard (which refuses an unknown call first) off in this transaction
    tx.batch_execute(
        "ALTER TABLE ops.retrieval_provider_budget_reservations
           DISABLE TRIGGER retrieval_provider_budget_reservation_guard",
    )
    .expect("disable the guard in this transaction");
    let e = budget_reservation(&mut tx, &tenant, "00000000-0000-0000-0000-0000000000c3")
        .expect_err("a reservation naming no call");
    assert_eq!(
        violated(&e),
        (
            "23503".into(),
            Some("retrieval_provider_budget_reservations_model_call_fk".into())
        ),
        "{e}"
    );
    tx.rollback().expect("rollback");
}

/// 0230's rowtype rebind (F10 gap): 0133's `private.assert_current_contribution_reservation_authority` took the heap's
/// rowtype, which the RENAME turns into one leaf's; re-created on the parent's rowtype, the
/// `contribution_reservation_authority_validate` trigger reaches it from a row routed to any leaf (a partition's row
/// converts to its parent's rowtype), here next month's, and the insert goes on to the next validator. Fault: delete
/// the rebind block from 0230 ⇒ the function keeps the current-month leaf's rowtype ⇒ a next-month row raises 42883
/// ⇒ red.
#[test]
fn the_contribution_authority_check_takes_rows_of_every_leaf() {
    let Some(mut db) = head("the_contribution_authority_check_takes_rows_of_every_leaf") else {
        return;
    };
    let c = db.client();
    let signatures: Vec<String> = c
        .query(
            "SELECT array_to_string(p.proargtypes::regtype[], ',') FROM pg_proc p
              WHERE p.proname = 'assert_current_contribution_reservation_authority'",
            &[],
        )
        .expect("signatures")
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(
        signatures,
        ["ops.model_call_ledger"],
        "one authority check, on the parent's rowtype"
    );
    let tenant = bare_tenant(c);
    for months in [0, 1, 3] {
        let e = c
            .execute(
                &format!(
                    "INSERT INTO ops.model_call_ledger (tenant_id, provider, purpose, request_id, called_at)
                     VALUES ($1::text::uuid, 'c36', 'CONTRIBUTION_DEIDENTIFY', gen_random_uuid(), {} + interval '1 day')",
                    month(months)
                ),
                &[&tenant],
            )
            .expect_err("no admission behind the row");
        assert_eq!(
            refused(&e),
            (
                "23514".into(),
                "reasoning model call snapshot must equal one current exact admission".into()
            ),
            "month +{months}: the authority check ran and passed it on to reasoning_model_call_validate"
        );
    }
}

/// T-C8 (ADR-0063 D-E, F9): the §19.1 TRUNCATE guard is a statement trigger, which PostgreSQL does not clone onto
/// partitions, so `control.partition_adopt_leaf` copies it: TRUNCATE of the parent and of every ledger leaf is
/// refused 42501 for the superuser. Fault: skip the statement-trigger copy in `control.partition_adopt_leaf` (0224)
/// ⇒ a leaf truncates ⇒ red.
#[test]
fn a_leaf_truncate_is_refused_like_the_parent() {
    let Some(mut db) = head("a_leaf_truncate_is_refused_like_the_parent") else {
        return;
    };
    let c = db.client();
    let tenant = bare_tenant(c);
    ledger_row(c, &tenant, "now()", "RESERVED");
    let mut targets = vec!["ops.model_call_ledger".to_owned()];
    targets.extend(
        leaves(c)
            .into_iter()
            .filter(|(p, _)| p == "ops.model_call_ledger")
            .map(|(_, l)| l),
    );
    assert_eq!(
        targets.len(),
        5,
        "the parent and its four leaves: {targets:?}"
    );
    for target in targets {
        let e = c
            .batch_execute(&format!("TRUNCATE {target}"))
            .expect_err("TRUNCATE is refused");
        assert_eq!(
            refused(&e),
            (
                "42501".into(),
                "ops.model_call_ledger is append-only (§19.1) — TRUNCATE not permitted".into()
            ),
            "{target}"
        );
    }
    let left: i64 = c
        .query_one("SELECT count(*) FROM ops.model_call_ledger", &[])
        .expect("rows")
        .get(0);
    assert_eq!(left, 1, "the row survives");
}

/// T-D6 (ADR-0063 D-D 7a, review finding 10): the body's fingerprint assertion refuses a conversion whose parent
/// lacks a legacy trigger. A seeded heap gets the 0230 body with the statement between one pair of mandatory markers
/// removed (the deferred disclosure constraint trigger; then one BEFORE INSERT validator), applied in one
/// transaction: refused with `c36 fingerprint drift: ops.model_call_ledger` naming the trigger, the heap untouched;
/// then the unmutated body converts. Fault: delete the fingerprint comparison from 0230's 7a block ⇒ the mutated body
/// commits ⇒ red.
#[test]
fn a_conversion_that_drops_a_trigger_is_refused_by_its_fingerprint_assert() {
    let Some(mut db) = throwaway::db_through(
        "a_conversion_that_drops_a_trigger_is_refused_by_its_fingerprint_assert",
        "c36_parts",
        Some("0229"),
    ) else {
        return;
    };
    let c = db.client();
    let tenant = bare_tenant(c);
    for m in [-2, -1] {
        ledger_row(c, &tenant, &month(m), "SUCCEEDED");
    }
    let text = body("0230");
    for (marker, trigger) in [
        (
            "c36:deferred-disclosure-trigger",
            "model_call_ledger_private_disclosure_present",
        ),
        (
            "c36:insert-validator",
            "contribution_reservation_authority_validate",
        ),
    ] {
        let (begin, end) = (format!("-- {marker} begin\n"), format!("-- {marker} end\n"));
        let (Some(from), Some(to)) = (text.find(&begin), text.find(&end)) else {
            panic!("0230 lacks the mandatory markers {begin:?} / {end:?}");
        };
        assert!(
            text[from..to].contains(&format!(" TRIGGER {trigger} ")),
            "the {marker} markers enclose {trigger}"
        );
        let mutated = format!("{}{}", &text[..from], &text[to + end.len()..]);
        let e = c
            .batch_execute(&mutated)
            .expect_err("a parent without a legacy trigger");
        let message = e
            .as_db_error()
            .map(|d| d.message().to_owned())
            .unwrap_or_default();
        assert!(
            message.starts_with("c36 fingerprint drift: ops.model_call_ledger missing trigger: ")
                && message.contains(trigger),
            "{marker}: {message}"
        );
        let heap: (String, i64) = c
            .query_one(
                "SELECT (SELECT relkind::text FROM pg_class WHERE oid = 'ops.model_call_ledger'::regclass),
                        (SELECT count(*) FROM ops.model_call_ledger)",
                &[],
            )
            .map(|r| (r.get(0), r.get(1)))
            .expect("the heap");
        assert_eq!(heap, ("r".into(), 2), "{marker}: the heap is untouched");
    }
    c.batch_execute(&text).expect("the unmutated body converts");
}

/// Main-line dev orphan note (2026-10-05): 0230's precheck counts, for every FK it re-creates (12 outbound, 5
/// referencers), the rows without a target and refuses before touching anything, one `<fk>: <n> orphan rows`
/// clause each; once the data is repaired the same body converts. Fault: delete the orphan loop from 0230 ⇒ the
/// refusal is a mid-way FK error without the counts ⇒ the message assertion reds.
#[test]
fn a_conversion_with_orphan_ledger_references_is_refused() {
    let Some(mut db) = throwaway::db_through(
        "a_conversion_with_orphan_ledger_references_is_refused",
        "c36_parts",
        Some("0229"),
    ) else {
        return;
    };
    let c = db.client();
    let (gone, kept) = (bare_tenant(c), bare_tenant(c));
    ledger_row(c, &gone, &month(-1), "SUCCEEDED");
    let referenced = ledger_row(c, &kept, &month(-1), "RESERVED");
    let untouched = ledger_row(c, &kept, &month(-1), "RESERVED");
    budget_reservation(c, &kept, &referenced).expect("a reservation naming a ledger row");
    // dep: PostgreSQL(owner) — replica mode: plant one orphan per FK side, as the dev teardowns did
    // replica-mode: throwaway database only (humaux_thread_c36_parts_<pid>_<n>, created by throwaway::db_through, dropped WITH (FORCE) by Db::drop)
    c.batch_execute(&format!(
        "SET session_replication_role = replica;
         DELETE FROM ops.model_call_ledger WHERE model_call_id = '{referenced}';
         DELETE FROM control.tenants WHERE tenant_id = '{gone}';
         RESET session_replication_role;"
    ))
    .expect("plant orphans");
    let e = c
        .batch_execute(&body("0230"))
        .expect_err("orphan rows refuse the conversion");
    let message = e
        .as_db_error()
        .map(|d| d.message().to_owned())
        .unwrap_or_default();
    assert_eq!(
        message,
        "c36 precheck: model_call_ledger_tenant_id_fkey: 1 orphan rows - repair data first; \
         retrieval_provider_budget_reservations_model_call_fk: 1 orphan rows - repair data first",
        "{e}"
    );
    let kind: String = c
        .query_one(
            "SELECT relkind::text FROM pg_class WHERE oid = 'ops.model_call_ledger'::regclass",
            &[],
        )
        .expect("relkind")
        .get(0);
    assert_eq!(kind, "r", "ops.model_call_ledger is still the heap");
    // replica-mode: throwaway database only (humaux_thread_c36_parts_<pid>_<n>, created by throwaway::db_through, dropped WITH (FORCE) by Db::drop)
    c.batch_execute(&format!(
        "SET session_replication_role = replica;
         DELETE FROM ops.model_call_ledger WHERE tenant_id = '{gone}';
         DELETE FROM ops.retrieval_provider_budget_reservations WHERE model_call_id = '{referenced}';
         RESET session_replication_role;"
    ))
    .expect("repair the orphans");
    c.batch_execute(&body("0230"))
        .expect("repaired data converts");
    let kept_row: i64 = c
        .query_one(
            "SELECT count(*) FROM ops.model_call_ledger WHERE model_call_id = $1::text::uuid",
            &[&untouched],
        )
        .expect("the untouched row")
        .get(0);
    assert_eq!(kept_row, 1, "rows that were never orphans survive");
}

/// The shared PostgreSQL container of this node (the c36 scripts' `docker exec` target): `pg_dump` / `pg_restore`
/// run inside it, so their version is the server's.
const PG_CONTAINER: &str = "humaux-thread-pg";

/// The §77 fields every writing `retention` run requires.
const ADMIN: [&str; 8] = [
    "--actor",
    "ops@example.test",
    "--reason",
    "card 36 registry by name",
    "--ticket",
    "T-C36",
    "--step-up-auth",
    "test-step-up",
];

/// The rls-check partition arm's registry-vs-catalog drift SQL, the very text the gate runs (ADR-0063 D-L, 0231).
const REGISTRY_DRIFT_SQL: &str =
    include_str!("../../../crates/testkit/sql/partition_registry_drift.sql");

/// The arm's drift rows `leaf: problem` over every schema of `c` (empty = the registry describes the catalog).
fn registry_drift(c: &mut Client) -> Vec<String> {
    let schemas: Vec<String> = c
        .query("SELECT nspname::text FROM pg_namespace", &[])
        .expect("schemas")
        .iter()
        .map(|r| r.get(0))
        .collect();
    c.query(REGISTRY_DRIFT_SQL, &[&schemas])
        .expect("the rls-check partition arm's drift SQL")
        .iter()
        .map(|r| format!("{}: {}", r.get::<_, String>(0), r.get::<_, String>(1)))
        .collect()
}

/// One `humaux-maintenance retention <args> + ADMIN` run against `db` as its superuser owner:
/// `(exit code, receipt = the last stdout line as JSON, stderr)`.
fn retention(db: &Db, args: &[&str]) -> (Option<i32>, serde_json::Value, String) {
    let mut all = vec!["retention"];
    all.extend(args);
    all.extend(ADMIN);
    // dep: subprocess(humaux-maintenance) — one one-shot executor run against this throwaway
    let out = throwaway::run(
        &all,
        &[(
            "HUMAUX_MIGRATOR_PG_DSN",
            throwaway::with_db(&db.owner_dsn, &db.name),
        )],
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let receipt = stdout
        .lines()
        .last()
        .and_then(|l| serde_json::from_str(l).ok())
        .unwrap_or(serde_json::Value::Null);
    (
        out.status.code(),
        receipt,
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// The daemon's PARTITIONS run (`maintenance_repo::propose_partitions`) as role_maintenance on `db`.
fn partitions_run(db: &Db) -> humaux_adapters::maintenance_repo::PartitionsRun {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        // dep: PostgreSQL(role_maintenance) — the daemon's checked pool on this throwaway
        let pool = humaux_adapters::postgres::MaintenanceDbPool::connect(&db.maintenance_dsn)
            .await
            .expect("maintenance pool");
        humaux_adapters::maintenance_repo::propose_partitions(&pool)
            .await
            .expect("propose")
    })
}

/// `(leaf_name, state, lower, upper, leaf exists, rows)` of every registry row of `db`, by leaf name; rows are
/// counted for STAGE_RUNS leaves only (`-1` otherwise: a drop's §77 row lands in the current audit leaf).
fn registry_rows(c: &mut Client) -> Vec<(String, String, String, String, bool, i64)> {
    let rows = c
        .query(
            "SELECT leaf_name, state, coalesce(lower_bound::text, 'MINVALUE'), upper_bound::text,
                    to_regclass(leaf_name) IS NOT NULL, table_key = 'STAGE_RUNS'
               FROM control.partition_registry ORDER BY leaf_name",
            &[],
        )
        .expect("registry rows");
    rows.iter()
        .map(|r| {
            let (leaf, exists, counted): (String, bool, bool) = (r.get(0), r.get(4), r.get(5));
            let n: i64 = if exists && counted {
                c.query_one(&format!("SELECT count(*) FROM {leaf}"), &[])
                    .expect("leaf count")
                    .get(0)
            } else {
                -1
            };
            (leaf, r.get(1), r.get(2), r.get(3), exists, n)
        })
        .collect()
}

/// `pg_dump -Fc` of `from` into `file`, then `pg_restore` of it into the empty database `into`, both inside
/// [`PG_CONTAINER`] as its superuser; `Err` names the step and carries its stderr.
fn dump_and_restore(from: &str, into: &str, file: &std::path::Path) -> Result<(), String> {
    use std::process::{Command, Stdio};
    // dep: subprocess(docker) — pg_dump inside the PostgreSQL container, the custom-format archive to `file`
    let dump = Command::new("docker")
        .args([
            "exec",
            PG_CONTAINER,
            "pg_dump",
            "-U",
            "postgres",
            "-Fc",
            from,
        ])
        .stdout(std::fs::File::create(file).map_err(|e| format!("dump file: {e}"))?)
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| format!("spawn pg_dump: {e}"))?;
    if !dump.status.success() {
        return Err(format!(
            "pg_dump: {}",
            String::from_utf8_lossy(&dump.stderr)
        ));
    }
    // dep: subprocess(docker) — pg_restore inside the PostgreSQL container from the archive (a seekable stdin)
    let restore = Command::new("docker")
        .args([
            "exec",
            "-i",
            PG_CONTAINER,
            "pg_restore",
            "-U",
            "postgres",
            "-d",
            into,
        ])
        .stdin(std::fs::File::open(file).map_err(|e| format!("dump file: {e}"))?)
        .output()
        .map_err(|e| format!("spawn pg_restore: {e}"))?;
    if !restore.status.success() || !restore.stderr.is_empty() {
        return Err(format!(
            "pg_restore exit {:?}: {}",
            restore.status.code(),
            String::from_utf8_lossy(&restore.stderr)
        ));
    }
    Ok(())
}

/// ADR-0063 "Registry by name" (0231), the witness of the D-M rollback: restoring a `pg_dump -Fc` into a fresh
/// database renumbers every relation, and the registry must still describe the copy. A throwaway at head gets rows in
/// all six converted tables and two expired STAGE_RUNS months (finished rows) under an effective 1-month policy; it is
/// dumped and restored into a second throwaway. On the restored copy: (a) the rls-check partition arm's own drift SQL
/// is empty; (b) the daemon's PARTITIONS run (proposals and horizons) and the `retention execute --dry-run` listing
/// (leaves, bounds, counts, verdicts) equal the original's; (c) after `retention approve` of a new 1-month revision, a
/// registry row whose upper bound was tampered is refused `registry_catalog_mismatch <leaf>` with nothing dropped,
/// and the real drop of the oldest month removes exactly that leaf: its row DROPPED with the receipt columns and a
/// `PARTITION_DROPPED` §77 row, every sibling row and leaf unchanged, the drift SQL still empty.
/// Faults (0231 rewritten, then restored): (i) a stored OID again — adopt writes it and `partition_drop_check`
/// matches `c.oid = reg.leaf_oid` ⇒ the restored listing says `registry_catalog_mismatch …` where the original says
/// `ok` ⇒ red; (ii) the bound comparison removed from `partition_drop_check` ⇒ the tampered row is dropped instead
/// of refused ⇒ red.
#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one dump/restore, then the gate SQL, the proposer, the listing and one drop on both sides"
)]
fn registry_survives_a_logical_dump_and_restore() {
    const TEST: &str = "registry_survives_a_logical_dump_and_restore";
    // dep: subprocess(docker) — probe for pg_dump inside the PostgreSQL container
    let probe = std::process::Command::new("docker")
        .args(["exec", PG_CONTAINER, "pg_dump", "--version"])
        .output();
    if !probe.is_ok_and(|o| o.status.success()) {
        humaux_testkit::skip_or_fail(
            TEST,
            &format!("missing object: docker container {PG_CONTAINER} with pg_dump / pg_restore"),
            humaux_testkit::ExternalDep::Postgres,
        );
        return;
    }
    let Some(mut original) = head(TEST) else {
        return;
    };
    let c = original.client();
    let ids = tenant(c);
    for (key, _, _) in CONVERTED {
        insert_at(c, key, &format!("{} + interval '1 day'", month(0)), &ids)
            .unwrap_or_else(|e| panic!("{key} row: {e}"));
    }
    let (oldest_id, oldest) = throwaway::past_leaf(c, "STAGE_RUNS", "ops.stage_runs", -3);
    let (sibling_id, sibling) = throwaway::past_leaf(c, "STAGE_RUNS", "ops.stage_runs", -2);
    for (months, n) in [(-3, 3), (-2, 2)] {
        c.execute(
            &format!(
                "INSERT INTO ops.stage_runs (tenant_id, stage_name, started_at, finished_at)
                 SELECT $1::text::uuid, 'c36 restore', {m} + make_interval(days => g),
                        {m} + make_interval(days => g, hours => 1)
                   FROM generate_series(1, $2) g",
                m = month(months)
            ),
            &[&ids.0, &n],
        )
        .expect("finished stage_runs rows in a past month");
    }
    let (policy, _) = throwaway::effective_policy(c, "STAGE_RUNS", Some(1));

    let Some(mut restored) = throwaway::empty(TEST, "c36_parts") else {
        return;
    };
    // This test's own scratch directory (the archive and every export dir), emptied first.
    let scratch = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("c36_registry_restore");
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).expect("scratch dir");
    let archive = scratch.join("original.dump");
    dump_and_restore(&original.name, &restored.name, &archive).expect("pg_dump | pg_restore");
    let renumbered: i64 = restored
        .client()
        .query_one(
            "SELECT count(*) FROM pg_inherits i JOIN pg_class l ON l.oid = i.inhrelid
              WHERE l.oid::text <> ALL($1::text[])",
            &[&original
                .client()
                .query("SELECT inhrelid::text FROM pg_inherits", &[])
                .expect("original leaf oids")
                .iter()
                .map(|r| r.get::<_, String>(0))
                .collect::<Vec<_>>()],
        )
        .expect("renumbered leaves")
        .get(0);
    assert!(
        renumbered > 0,
        "the restore must renumber leaves for this witness to mean anything"
    );

    // (a) the gate's own comparison, on both sides.
    assert_eq!(
        registry_drift(original.client()),
        Vec::<String>::new(),
        "original"
    );
    assert_eq!(
        registry_drift(restored.client()),
        Vec::<String>::new(),
        "restored copy"
    );

    // (b) the proposer and the dry-run listing see the same registry on both sides.
    let run = partitions_run(&original);
    assert_eq!(
        run.proposed, 2,
        "both expired months proposed on the original"
    );
    assert_eq!(
        partitions_run(&restored),
        run,
        "the restored copy's PARTITIONS run"
    );
    let listing = |db: &Db| {
        let dir = scratch.join(&db.name);
        std::fs::create_dir_all(&dir).expect("export dir");
        let dir = dir.display().to_string();
        let (code, receipt, stderr) = retention(
            db,
            &[
                "execute",
                "--policy",
                &policy,
                "--export-dir",
                &dir,
                "--lock-timeout-ms",
                "5000",
                "--dry-run",
            ],
        );
        assert_eq!(code, Some(0), "{receipt} {stderr}");
        receipt["due"].clone()
    };
    let due = listing(&original);
    let verdicts: Vec<&str> = due
        .as_array()
        .expect("due leaves")
        .iter()
        .map(|d| d["verdict"].as_str().expect("verdict"))
        .collect();
    assert_eq!(verdicts, ["ok", "ok"], "{due}");
    assert_eq!(
        listing(&restored),
        due,
        "the restored copy's dry-run listing"
    );

    // (c) a new revision approved on the restored copy, proposed by the daemon, then one refusal and one drop.
    let effective_at: String = restored
        .client()
        .query_one(
            "SELECT to_char((clock_timestamp() + interval '3 seconds') AT TIME ZONE 'UTC',
                            'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')",
            &[],
        )
        .expect("effective_at")
        .get(0);
    let (code, approved, stderr) = retention(
        &restored,
        &[
            "approve",
            "--table",
            "STAGE_RUNS",
            "--months",
            "1",
            "--effective-at",
            &effective_at,
            "--lock-timeout-ms",
            "5000",
        ],
    );
    assert_eq!(code, Some(0), "{approved} {stderr}");
    assert_eq!(approved["policy_revision"], 2, "{approved}");
    let revision = approved["policy_id"]
        .as_str()
        .expect("policy_id")
        .to_owned();
    while restored
        .client()
        .query_one(
            "SELECT clock_timestamp() < effective_at FROM control.retention_policies
              WHERE policy_id = $1::text::uuid",
            &[&revision],
        )
        .expect("effective yet")
        .get::<_, bool>(0)
    {
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    assert_eq!(
        partitions_run(&restored).proposed,
        2,
        "re-proposed under revision 2"
    );

    let dir = scratch.join("drop");
    std::fs::create_dir_all(&dir).expect("export dir");
    let dir = dir.display().to_string();
    let execute = |db: &Db, registry_id: &str| {
        retention(
            db,
            &[
                "execute",
                "--policy",
                &revision,
                "--registry-id",
                registry_id,
                "--export-dir",
                &dir,
                "--lock-timeout-ms",
                "5000",
            ],
        )
    };
    let before = registry_rows(restored.client());
    restored.sql(&format!(
        "UPDATE control.partition_registry SET upper_bound = upper_bound - interval '24 hours'
          WHERE registry_id = '{sibling_id}'"
    ));
    let (code, refused, stderr) = execute(&restored, &sibling_id);
    assert_eq!(
        (
            code,
            refused["outcome"].as_str(),
            refused["reason"].as_str()
        ),
        (
            Some(3),
            Some("refused"),
            Some(format!("registry_catalog_mismatch {sibling}").as_str())
        ),
        "a registry row whose bound drifted from the catalog is refused: {refused} {stderr}"
    );
    restored.sql(&format!(
        "UPDATE control.partition_registry SET upper_bound = upper_bound + interval '24 hours'
          WHERE registry_id = '{sibling_id}'"
    ));
    assert_eq!(
        registry_rows(restored.client()),
        before,
        "the refusal changed nothing"
    );

    let (code, dropped, stderr) = execute(&restored, &oldest_id);
    assert_eq!(code, Some(0), "{dropped} {stderr}");
    assert_eq!(dropped["outcome"], "dropped", "{dropped}");
    assert_eq!(dropped["leaf"], oldest.as_str(), "{dropped}");
    let after = registry_rows(restored.client());
    let unchanged = |rows: &[(String, String, String, String, bool, i64)]| -> Vec<_> {
        rows.iter().filter(|r| r.0 != oldest).cloned().collect()
    };
    assert_eq!(
        unchanged(&after),
        unchanged(&before),
        "every sibling row and leaf"
    );
    let receipt = restored
        .client()
        .query_one(
            "SELECT state, to_regclass(leaf_name) IS NULL, rows_dropped, export_sha256 = $2, drop_policy_revision,
                    (SELECT count(*) FROM control.audit_events
                      WHERE action = 'PARTITION_DROPPED' AND resource_id = $1)
               FROM control.partition_registry WHERE registry_id = $1::text::uuid",
            &[&oldest_id, &dropped["export_sha256"].as_str().unwrap_or_default()],
        )
        .expect("the dropped row's receipt");
    let receipt: (String, bool, i64, bool, i32, i64) = (
        receipt.get(0),
        receipt.get(1),
        receipt.get(2),
        receipt.get(3),
        receipt.get(4),
        receipt.get(5),
    );
    assert_eq!(
        receipt,
        ("DROPPED".to_owned(), true, 3, true, 2, 1),
        "(state, leaf gone, rows_dropped, export sha256, policy revision, PARTITION_DROPPED rows) of {oldest}"
    );
    assert!(
        after
            .iter()
            .any(|r| r.0 == sibling && r.1 == "ATTACHED" && r.4 && r.5 == 2),
        "the sibling month {sibling} stays attached with its rows: {after:?}"
    );
    assert_eq!(
        registry_drift(restored.client()),
        Vec::<String>::new(),
        "after the drop"
    );
}

/// Inserts per table for the D-N latency lines (the design's n = 200, nearest-rank percentiles).
const LATENCY_N: usize = 200;

/// `(p50, p95)` in microseconds, nearest rank over the sorted samples.
fn p50_p95(mut us: Vec<u128>) -> (u128, u128) {
    us.sort_unstable();
    let rank = |p: usize| us[(p * us.len()).div_ceil(100).max(1) - 1];
    (rank(50), rank(95))
}

/// D-N (ADR-0063): insert latency through the real write paths on a database restored from a dev dump, printed as
/// `LAT table=<t> phase=<before|after> n=200 p50_us=… p95_us=…` (phase derived: `private.events` still a heap ⇒
/// before, partitioned ⇒ after), plus the by-id settle that probes every ledger leaf (L2). `c36_devcopy.sh` runs it
/// on two throwaways restored from the same dump, one left at card 35's head and one migrated to head. Ignored: it
/// measures, it asserts no behaviour, and it targets an existing database named by `HUMAUX_C36_LATENCY_DB`, which
/// must be a `humaux_thread_c36_*` throwaway — the shared dev database is refused before any write.
#[test]
#[ignore = "lane(c) D-N measurement, not a test the lane can run: c36_devcopy.sh runs it against a humaux_thread_c36_* restore of a dev dump named by HUMAUX_C36_LATENCY_DB, which no lane resource provisions; never in the chain"]
#[allow(
    clippy::too_many_lines,
    reason = "four write paths and the settle probe timed against one restored database"
)]
fn partition_insert_latency() {
    let (Ok(target), Ok(owner_dsn), Ok(worker_dsn)) = (
        std::env::var("HUMAUX_C36_LATENCY_DB"),
        std::env::var(throwaway::OWNER_DSN),
        std::env::var("HUMAUX_RETRIEVAL_WORKER_PG_DSN"),
    ) else {
        humaux_testkit::skip_or_fail(
            "partition_insert_latency",
            "missing object: HUMAUX_C36_LATENCY_DB, HUMAUX_TEST_PG_DSN or HUMAUX_RETRIEVAL_WORKER_PG_DSN",
            humaux_testkit::ExternalDep::Postgres,
        );
        return;
    };
    assert!(
        target.starts_with("humaux_thread_c36_"),
        "partition_insert_latency refuses database {target}: only a humaux_thread_c36_* throwaway (ADR-0063 D-M)"
    );
    // dep: PostgreSQL(owner) — the superuser test principal on the restored throwaway
    let mut c = Client::connect(&throwaway::with_db(&owner_dsn, &target), postgres::NoTls)
        .expect("connect the latency target");
    let facts = c
        .query_one(
            "SELECT current_database()::text, relkind::text FROM pg_class WHERE oid = 'private.events'::regclass",
            &[],
        )
        .expect("target facts");
    let (db, events_kind): (String, String) = (facts.get(0), facts.get(1));
    assert!(
        db.starts_with("humaux_thread_c36_"),
        "connected to {db}, not a humaux_thread_c36_* throwaway"
    );
    let phase = if events_kind == "p" {
        "after"
    } else {
        "before"
    };
    let ids = tenant(&mut c);
    let tenant_id = ids.0.clone();
    let domain: String = c
        .query_one(
            "INSERT INTO control.private_reasoning_domains (tenant_id, name)
             VALUES ($1::text::uuid, 'c36-latency') RETURNING reasoning_domain_id::text",
            &[&tenant_id],
        )
        .expect("reasoning domain")
        .get(0);
    let timed = |c: &mut Client, f: &dyn Fn(&mut Client)| {
        let t = std::time::Instant::now();
        f(c);
        t.elapsed().as_micros()
    };
    let line = |table: &str, op: &str, us: Vec<u128>| {
        let (p50, p95) = p50_p95(us);
        println!("LAT table={table} phase={phase}{op} n={LATENCY_N} p50_us={p50} p95_us={p95}");
    };

    // control.audit_events: the one writer, the owner definer, as the gateway under the tenant GUC.
    let audit = (0..LATENCY_N)
        .map(|_| {
            timed(&mut c, &|c| {
                let mut tx = c.transaction().expect("begin");
                // dep: PostgreSQL(role_gateway) — role switch: the runtime caller of the audit insert definer
                tx.batch_execute("SET LOCAL ROLE role_gateway").expect("role");
                tx.execute("SELECT set_config('humaux.tenant_id', $1, true)", &[&tenant_id])
                    .expect("tenant guc");
                // dep: PostgreSQL(role_gateway) — control.audit_event_insert (0041 owner definer)
                tx.execute(
                    "SELECT control.audit_event_insert(uuidv7(), now(), $1::text::uuid, 'user', 'c36', 'C36_LATENCY',
                              'resource', 'r1', 'OK', 'req', 'trace', NULL, '', ARRAY[]::text[], NULL, NULL,
                              '{}'::jsonb)",
                    &[&tenant_id],
                )
                .expect("audit insert");
                tx.commit().expect("commit");
            })
        })
        .collect();
    line("audit_events", "", audit);

    // private.events: an evidence row plus its event in one statement (the remember recipe's NOT NULL columns).
    let events = (0..LATENCY_N)
        .map(|_| {
            timed(&mut c, &|c| {
                c.execute(
                    "WITH eo AS (INSERT INTO private.evidence_objects
                                   (tenant_id, evidence_kind, payload_sha256, data_class, origin_class,
                                    visibility_class, reasoning_domain_id)
                                 VALUES ($1::text::uuid, 'EVENT', sha256(gen_random_uuid()::text::bytea), 'INTERNAL',
                                         'DirectUserInput', 'TENANT_SHARED', $2::text::uuid)
                                 RETURNING evidence_id)
                     INSERT INTO private.events (event_id, event_kind, payload)
                     SELECT evidence_id, 'MANUAL_NOTE', '{}'::jsonb FROM eo",
                    &[&tenant_id, &domain],
                )
                .expect("evidence + event");
            })
        })
        .collect();
    line("events", "", events);

    // ops.stage_runs: a plain insert through the parent.
    let stages = (0..LATENCY_N)
        .map(|_| {
            timed(&mut c, &|c| {
                c.execute(
                    "INSERT INTO ops.stage_runs (tenant_id, stage_name) VALUES ($1::text::uuid, 'c36-latency')",
                    &[&tenant_id],
                )
                .expect("stage run");
            })
        })
        .collect();
    line("stage_runs", "", stages);

    // ops.model_call_ledger: the retrieval-plane writer (`reserve_call`, fresh request ids), then its by-id settle.
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    // dep: PostgreSQL(role_retrieval_worker) — the writer's checked pool on the throwaway
    let pool = rt
        .block_on(humaux_adapters::postgres::RetrievalWorkerDbPool::connect(
            &throwaway::with_db(&worker_dsn, &target),
        ))
        .expect("retrieval worker pool");
    let tenant_uuid: uuid::Uuid = tenant_id.parse().expect("tenant uuid");
    let mut calls = Vec::with_capacity(LATENCY_N);
    let reserve = (0..LATENCY_N)
        .map(|_| {
            let input = humaux_adapters::model_call_ledger::ReserveCall {
                request_id: Some(uuid::Uuid::now_v7()),
                tenant_id: tenant_uuid,
                workspace_id: None,
                purpose: Some(humaux_domain::ledger::ModelCallPurpose::Rerank),
                provider: "c36".to_owned(),
                model: Some("c36".to_owned()),
                model_revision: None,
                estimated_cost: Some(0.01),
            };
            let t = std::time::Instant::now();
            // dep: PostgreSQL(role_retrieval_worker) — adapters::model_call_ledger::reserve_call
            let reserved = rt
                .block_on(humaux_adapters::model_call_ledger::reserve_call(
                    &pool, &input,
                ))
                .expect("reserve");
            let us = t.elapsed().as_micros();
            calls.push(reserved.model_call_id);
            us
        })
        .collect();
    line("model_call_ledger", "", reserve);
    let settle = calls
        .iter()
        .map(|id| {
            let t = std::time::Instant::now();
            // dep: PostgreSQL(role_retrieval_worker) — adapters::model_call_ledger::finalize_call (UPDATE by id, no key)
            let changed = rt
                .block_on(humaux_adapters::model_call_ledger::finalize_call(
                    &pool,
                    tenant_uuid,
                    *id,
                    humaux_adapters::model_call_ledger::ModelCallOutcome::Succeeded,
                    &humaux_adapters::model_call_ledger::FinalizeCall {
                        actual_cost: Some(0.01),
                        ..Default::default()
                    },
                ))
                .expect("settle");
            assert!(changed, "the reservation settles once");
            t.elapsed().as_micros()
        })
        .collect();
    line("model_call_ledger", " op=settle", settle);
}
