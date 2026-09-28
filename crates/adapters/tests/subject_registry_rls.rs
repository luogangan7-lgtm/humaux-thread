//! `adapters::tests::subject_registry_rls` — Card 7 integration tests — §6.1.3 subject registry + §6.1.1
//!   `private.visibility_allowed` (migration 0153, ADR-0027).
//! Depends-on: crates=[humaux-domain, humaux-testkit, postgres, uuid]; services=[PostgreSQL(owner)
//!   w=[control.private_reasoning_domains, control.tenants, control.users, private.evidence_objects,
//!   private.memory_consolidation_runs, private.memory_records, private.memory_rollups, private.subject_keys,
//!   private.subject_roles, private.subjects] x=[private.visibility_allowed], PostgreSQL(role_gateway)];
//!   env=[HUMAUX_TEST_PG_DSN]; modules=[domain::identity, domain::ids, domain::subject, humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [the SQL visibility predicate agrees with domain::identity::can_read over the whole scope x class
//!   matrix, and reverting the USER_PRIVATE arm must turn both the parity and the policy read red]
//! Spec: Baseline §6.1.1; §6.1.3; §78.2; ADR-0027
//!
//! Everything here runs against the *real* `private` objects the
//! migration created (not a scratch schema like `auth_scope_rls.rs` — this card's acceptance is
//! specifically that the extracted predicate and the real registry behave correctly):
//!
//!  1. `visibility_allowed_matches_can_read_exhaustive` — the SQL predicate agrees with
//!     `humaux_domain::identity::can_read` across the whole scope×class matrix (the parity the
//!     card's acceptance names, mirroring the property style of `projection::dense`).
//!  2. `parity_is_fault_sensitive` — reverting the USER_PRIVATE arm to always-true makes BOTH the
//!     parity comparison and a policy-level isolation read go red: a gateway session for user U2
//!     reads user U1's USER_PRIVATE rows through all three re-pointed policies (0 before the fault,
//!     1 after). Self-checking inside a rolled-back transaction so the fault can never leak.
//!  3. `unset_user_id_session_reads_tenant_shared_rows` — the eager-evaluation NULLIF guard
//!     (ADR-0027's central tradeoff) has a regression test: a gateway session whose
//!     `humaux.user_id` GUC is the empty string reads TENANT_SHARED rows through all three policies
//!     without `invalid input syntax for type uuid: ""`.
//!  4. `subject_checks_match_domain_enums` — the three `text` CHECK columns' literal sets equal the
//!     closed domain enums (`SubjectKind`/`SubjectKeyKind`/`SubjectRole`), the §78.2 DB↔Rust contract.
//!  5. `subjects_cross_tenant_isolation_via_set_local_role` — a non-superuser role (`role_gateway`)
//!     registers a Person and an Organisation with a CRM key under tenant A and reads them back;
//!     tenant B's session sees zero. Cross-tenant invisibility under RLS, the registry's whole point.
//!  6. `subject_fks_reject_foreign_tenant_subjects` — every FK into `private.subjects` carries the
//!     tenant leg: tenant B cannot attach a key / role / merge pointer to tenant A's subject, and the
//!     failure is byte-identical to a nonexistent id (no existence oracle through RI, which bypasses RLS).
//!  7. `subject_keys_unique_per_tenant_never_globally` — the same (key_kind, key_value) twice in one
//!     tenant is 23505; the same key under a second tenant succeeds.
//!
//! Skip contract (`humaux_testkit`, §79.2): no DSN / unreachable / migration 0153 not applied each
//! print a visible SKIP and return. Tests 2/3/6/7 run entirely inside one rolled-back transaction;
//! test 5's committed rows are torn down in `Drop` (`DELETE FROM control.tenants` cascades to
//! subjects→keys/roles), so a passing run leaves the dev DB clean (repo `CLAUDE.md` hard rule ④).

use humaux_domain::identity::{
    AuthorizationScope, BoundedSet, PrincipalId, VisibilityClass, VisibilityDescriptor, can_read,
};
use humaux_domain::ids::{TenantId, UserId, WorkspaceId};
use humaux_domain::subject::{SubjectKeyKind, SubjectKind, SubjectRole};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::error::SqlState;
use postgres::{Client, GenericClient, NoTls};
use uuid::Uuid;

struct SubjectHandle {
    client: Client,
    /// Test tenants to purge on teardown (CASCADE removes their subjects/keys/roles).
    tenants: Vec<uuid::Uuid>,
}

impl Drop for SubjectHandle {
    fn drop(&mut self) {
        for t in &self.tenants {
            let _ = self
                .client
                .execute("DELETE FROM control.tenants WHERE tenant_id = $1", &[t]);
        }
    }
}

struct SubjectFixture;

impl DbIntegrationFixture for SubjectFixture {
    type Handle = SubjectHandle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
        let mut client = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
        // Migration 0153 must be on the tree; if not, skip rather than fail spuriously.
        let ready: bool = client
            .query_one(
                "SELECT to_regprocedure('private.visibility_allowed(text,boolean,boolean,boolean)') \
                 IS NOT NULL AND to_regclass('private.subjects') IS NOT NULL",
                &[],
            )
            .map(|r| r.get(0))
            .unwrap_or(false);
        if !ready {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "migration 0153_subject_registry_and_visibility_fn not applied".to_string(),
            ));
        }
        Ok(SubjectHandle {
            client,
            tenants: Vec::new(),
        })
    }
}

/// Wire form of a visibility class (mirrors the `text` CHECK the policies read).
fn class_wire(c: VisibilityClass) -> &'static str {
    match c {
        VisibilityClass::TenantShared => "TENANT_SHARED",
        VisibilityClass::UserPrivate => "USER_PRIVATE",
        VisibilityClass::WorkspaceShared => "WORKSPACE_SHARED",
    }
}

/// The exhaustive scope×class matrix: for each class and each (user_ok, ws_ok) pair, build a
/// scope/descriptor that yields those booleans and return (class, user_ok, ws_ok, can_read).
fn parity_matrix() -> Vec<(VisibilityClass, bool, bool, bool)> {
    let uid = UserId::new();
    let wid = WorkspaceId::new();
    let mut out = Vec::new();
    for class in [
        VisibilityClass::TenantShared,
        VisibilityClass::UserPrivate,
        VisibilityClass::WorkspaceShared,
    ] {
        for user_ok in [true, false] {
            for ws_ok in [true, false] {
                let scope = AuthorizationScope::new(
                    TenantId::new(),
                    PrincipalId::new(),
                    Some(uid),
                    BoundedSet::new([wid]).unwrap(),
                );
                let desc = VisibilityDescriptor {
                    class,
                    user_id: Some(if user_ok { uid } else { UserId::new() }),
                    workspace_id: Some(if ws_ok { wid } else { WorkspaceId::new() }),
                };
                out.push((class, user_ok, ws_ok, can_read(&scope, &desc)));
            }
        }
    }
    out
}

/// The three tables whose 0012/0058 policies migration 0153 re-pointed at `visibility_allowed`.
const POLICED_TABLES: [&str; 3] = [
    "private.evidence_objects",
    "private.memory_records",
    "private.memory_rollups",
];

/// Superuser seed (inside the caller's transaction): a tenant, its reasoning domain, and one row
/// of the given visibility in each of the three policed tables. Returns nothing the caller needs —
/// the rows are found again through the policies, which is the whole point.
fn seed_policed_rows<C: GenericClient>(
    c: &mut C,
    tenant: Uuid,
    visibility_class: &str,
    visibility_user_id: Option<Uuid>,
) {
    c.execute(
        "INSERT INTO control.tenants (tenant_id, name) VALUES ($1, 'card7-policy-test')",
        &[&tenant],
    )
    .expect("seed tenant");
    if let Some(u) = visibility_user_id {
        c.execute("INSERT INTO control.users (user_id) VALUES ($1)", &[&u])
            .expect("seed row owner user");
    }
    let domain: Uuid = c
        .query_one(
            "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
             VALUES ($1, 'card7') RETURNING reasoning_domain_id",
            &[&tenant],
        )
        .expect("seed reasoning domain")
        .get(0);
    c.execute(
        "INSERT INTO private.evidence_objects \
           (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
            visibility_class, visibility_user_id, reasoning_domain_id) \
         VALUES ($1, 'EVENT', '\\x00'::bytea, 'INTERNAL', 'DirectUserInput', $2, $3, $4)",
        &[&tenant, &visibility_class, &visibility_user_id, &domain],
    )
    .expect("seed evidence row");
    c.execute(
        "INSERT INTO private.memory_records \
           (tenant_id, memory_type, content, visibility_class, visibility_user_id, \
            authority_class, confidence, status, asserted_at) \
         VALUES ($1, 'NOTE', '{}'::jsonb, $2, $3, 'PrivateKnowledge', 0.9, 'active', now())",
        &[&tenant, &visibility_class, &visibility_user_id],
    )
    .expect("seed memory row");
    let run: Uuid = c
        .query_one(
            "INSERT INTO private.memory_consolidation_runs (tenant_id, reasoning_domain_id) \
             VALUES ($1, $2) RETURNING run_id",
            &[&tenant, &domain],
        )
        .expect("seed consolidation run")
        .get(0);
    c.execute(
        "INSERT INTO private.memory_rollups \
           (tenant_id, run_id, content, authority_class, visibility_class, visibility_user_id) \
         VALUES ($1, $2, '{}'::jsonb, 'PrivateKnowledge', $3, $4)",
        &[&tenant, &run, &visibility_class, &visibility_user_id],
    )
    .expect("seed rollup row");
}

/// `SELECT count(*)` on each policed table under whatever role/GUC context the transaction
/// currently carries. Returns the raw `Result` per table so callers can assert "no error" as well
/// as the count.
fn count_policed<C: GenericClient>(c: &mut C) -> Vec<(&'static str, Result<i64, String>)> {
    POLICED_TABLES
        .iter()
        .map(|tbl| {
            let r = c
                .query_one(&format!("SELECT count(*) FROM {tbl}"), &[])
                .map(|row| row.get::<_, i64>(0))
                // Keep SQLSTATE + message so a red run names the fault (e.g. 22P02 on ''::uuid).
                .map_err(|e| {
                    e.as_db_error().map_or_else(
                        || e.to_string(),
                        |d| format!("{} {}", d.code().code(), d.message()),
                    )
                });
            (*tbl, r)
        })
        .collect()
}

/// (SQLSTATE, constraint name) of a failed statement — the only two things a caller could use to
/// tell "foreign tenant's id" apart from "no such id".
fn failure_signature(e: &postgres::Error) -> (Option<SqlState>, Option<String>) {
    (
        e.code().cloned(),
        e.as_db_error()
            .and_then(|d| d.constraint())
            .map(str::to_string),
    )
}

#[test]
fn visibility_allowed_matches_can_read_exhaustive() {
    run_db_fixture::<SubjectFixture, _>("visibility_allowed_matches_can_read", |mut handle| {
        for (class, user_ok, ws_ok, expected) in parity_matrix() {
            let db: bool = handle
                .client
                .query_one(
                    "SELECT private.visibility_allowed($1, true, $2, $3)",
                    &[&class_wire(class), &user_ok, &ws_ok],
                )
                .expect("visibility_allowed query")
                .get(0);
            assert_eq!(
                db,
                expected,
                "parity mismatch at class={} user_ok={user_ok} ws_ok={ws_ok}",
                class_wire(class)
            );
        }
    });
}

#[test]
fn parity_is_fault_sensitive() {
    // Fault injection (card acceptance): flip the USER_PRIVATE arm to always-true and prove that
    // (a) the parity comparison finds a disagreement and (b) a policy-level isolation read widens —
    // user U2 newly sees user U1's USER_PRIVATE row in every policed table. Done inside a
    // rolled-back transaction so the real function is never actually mutated.
    run_db_fixture::<SubjectFixture, _>("parity_is_fault_sensitive", |mut handle| {
        let tenant = TenantId::new().0;
        let owner = UserId::new().0;
        let intruder = UserId::new().0;
        let mut txn = handle.client.transaction().expect("begin");
        seed_policed_rows(&mut txn, tenant, "USER_PRIVATE", Some(owner));
        txn.execute(
            "INSERT INTO control.users (user_id) VALUES ($1)",
            &[&intruder],
        )
        .expect("seed intruder user");

        // Baseline: with the real predicate, U2 sees none of U1's private rows.
        txn.batch_execute(&format!(
            // dep: PostgreSQL(role_gateway) — test switches PG role to exercise RLS
            "SET LOCAL ROLE role_gateway; SET LOCAL humaux.tenant_id = '{tenant}'; \
             SET LOCAL humaux.user_id = '{intruder}';"
        ))
        .expect("gateway/U2 context");
        for (tbl, r) in count_policed(&mut txn) {
            assert_eq!(
                r,
                Ok(0),
                "{tbl}: U2 must not see U1's USER_PRIVATE row before the fault"
            );
        }
        txn.batch_execute("RESET ROLE").expect("back to superuser");

        // Inject: USER_PRIVATE arm → always-true.
        txn.batch_execute(
            "CREATE OR REPLACE FUNCTION private.visibility_allowed(\
               p_class text, p_tenant_ok boolean, p_user_ok boolean, p_workspace_ok boolean) \
             RETURNS boolean LANGUAGE sql IMMUTABLE PARALLEL SAFE \
             RETURN CASE p_class \
               WHEN 'TENANT_SHARED' THEN p_tenant_ok \
               WHEN 'USER_PRIVATE' THEN true \
               WHEN 'WORKSPACE_SHARED' THEN p_workspace_ok ELSE false END;",
        )
        .expect("inject fault");

        // (a) parity goes red.
        let mut disagreements = 0;
        for (class, user_ok, ws_ok, expected) in parity_matrix() {
            let db: bool = txn
                .query_one(
                    "SELECT private.visibility_allowed($1, true, $2, $3)",
                    &[&class_wire(class), &user_ok, &ws_ok],
                )
                .expect("q")
                .get(0);
            if db != expected {
                disagreements += 1;
            }
        }

        // (b) the isolation read goes red: the widened arm leaks U1's row to U2 through every
        // re-pointed policy (the GUCs set above are transaction-scoped and still in force).
        // dep: PostgreSQL(role_gateway) — test switches PG role to exercise RLS
        txn.batch_execute("SET LOCAL ROLE role_gateway")
            .expect("gateway/U2 context under fault");
        let leaked = count_policed(&mut txn);
        txn.rollback().expect("rollback fault");

        assert!(
            disagreements > 0,
            "reverting the USER_PRIVATE arm to always-true must make the parity check go red"
        );
        for (tbl, r) in leaked {
            assert_eq!(
                r,
                Ok(1),
                "{tbl}: the always-true USER_PRIVATE arm must widen the policy (isolation goes red)"
            );
        }
    });
}

#[test]
fn unset_user_id_session_reads_tenant_shared_rows() {
    // Regression for the eager-evaluation guard (ADR-0027): a pooled connection whose
    // `humaux.user_id` placeholder GUC exists but is '' (the state SET LOCAL leaves behind after
    // its transaction ends) must still read TENANT_SHARED rows through the three re-pointed
    // policies. Without the NULLIF around every user_id cast passed into visibility_allowed, the
    // now-eagerly-evaluated `''::uuid` raises 22P02 instead of the arm evaluating to false.
    run_db_fixture::<SubjectFixture, _>("unset_user_id_reads_tenant_shared", |mut handle| {
        let tenant = TenantId::new().0;
        let mut txn = handle.client.transaction().expect("begin");
        seed_policed_rows(&mut txn, tenant, "TENANT_SHARED", None);
        txn.batch_execute(&format!(
            // dep: PostgreSQL(role_gateway) — test switches PG role to exercise RLS
            "SET LOCAL ROLE role_gateway; SET LOCAL humaux.tenant_id = '{tenant}'; \
             SET LOCAL humaux.user_id = '';"
        ))
        .expect("gateway context with empty user_id");
        let counts = count_policed(&mut txn);
        txn.rollback().expect("rollback");
        for (tbl, r) in counts {
            assert_eq!(
                r,
                Ok(1),
                "{tbl}: a session with humaux.user_id='' must read TENANT_SHARED rows, not error"
            );
        }
    });
}

#[test]
fn subject_checks_match_domain_enums() {
    run_db_fixture::<SubjectFixture, _>("subject_checks_match_domain_enums", |mut handle| {
        let cases: [(&str, &str, Vec<&str>); 3] = [
            (
                "private.subjects",
                "kind",
                SubjectKind::ALL.iter().map(|k| k.as_str()).collect(),
            ),
            (
                "private.subject_keys",
                "key_kind",
                SubjectKeyKind::ALL.iter().map(|k| k.as_str()).collect(),
            ),
            (
                "private.subject_roles",
                "role",
                SubjectRole::ALL.iter().map(|r| r.as_str()).collect(),
            ),
        ];
        for (table, column, wire) in cases {
            // The single CHECK constraint mentioning this column.
            // table/column are trusted test constants (not user input) — inline them so the driver
            // does not try to serialize a &str into the `regclass` its `$1::regclass` inference wants.
            let def: String = handle
                .client
                .query_one(
                    &format!(
                        "SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c \
                         WHERE c.conrelid = '{table}'::regclass AND c.contype = 'c' \
                           AND pg_get_constraintdef(c.oid) LIKE '%{column}%'"
                    ),
                    &[],
                )
                .unwrap_or_else(|e| panic!("no CHECK on {table}.{column}: {e}"))
                .get(0);
            // Every enum wire value must appear, and the number of single-quoted literals in the
            // CHECK must equal the enum size — no extra DB value the Rust set does not know.
            for w in &wire {
                assert!(
                    def.contains(&format!("'{w}'")),
                    "{table}.{column} CHECK missing enum value {w}: {def}"
                );
            }
            let literal_count = def.matches('\'').count() / 2;
            assert_eq!(
                literal_count,
                wire.len(),
                "{table}.{column} CHECK has {literal_count} literals; domain enum has {}: {def}",
                wire.len()
            );
        }
    });
}

#[test]
fn subjects_cross_tenant_isolation_via_set_local_role() {
    run_db_fixture::<SubjectFixture, _>("subjects_cross_tenant_isolation", |mut handle| {
        let tenant_a = TenantId::new().0;
        let tenant_b = TenantId::new().0;
        handle.tenants.push(tenant_a);
        handle.tenants.push(tenant_b);
        // Seed both tenants (superuser; FK target for subjects.tenant_id).
        handle
            .client
            .batch_execute(&format!(
                "INSERT INTO control.tenants (tenant_id, name) \
                 VALUES ('{tenant_a}', 'card7-test-a'), ('{tenant_b}', 'card7-test-b');"
            ))
            .expect("seed tenants");

        // Register a Person + an Organisation with a CRM key under tenant A, as role_gateway.
        {
            let mut txn = handle.client.transaction().expect("begin register");
            txn.batch_execute(&format!(
                // dep: PostgreSQL(role_gateway) — test switches PG role to exercise RLS
                "SET LOCAL ROLE role_gateway; SET LOCAL humaux.tenant_id = '{tenant_a}';"
            ))
            .expect("set gateway/tenant-A context");
            let person: uuid::Uuid = txn
                .query_one(
                    "INSERT INTO private.subjects (tenant_id, kind, display_name) \
                     VALUES ($1, 'PERSON', 'Ada Lovelace') RETURNING subject_id",
                    &[&tenant_a],
                )
                .expect("insert person")
                .get(0);
            let org: uuid::Uuid = txn
                .query_one(
                    "INSERT INTO private.subjects (tenant_id, kind, display_name) \
                     VALUES ($1, 'ORGANISATION', 'Analytical Engines Ltd') RETURNING subject_id",
                    &[&tenant_a],
                )
                .expect("insert org")
                .get(0);
            txn.execute(
                "INSERT INTO private.subject_keys (tenant_id, subject_id, key_kind, key_value) \
                 VALUES ($1, $2, 'CRM', 'CRM-1001')",
                &[&tenant_a, &org],
            )
            .expect("insert crm key");
            txn.execute(
                "INSERT INTO private.subject_roles (tenant_id, subject_id, role) \
                 VALUES ($1, $2, 'CUSTOMER')",
                &[&tenant_a, &org],
            )
            .expect("insert role");
            let _ = person;
            txn.commit().expect("commit register");
        }

        // Tenant A reads its two subjects + the CRM key back.
        {
            let mut txn = handle.client.transaction().expect("begin read A");
            txn.batch_execute(&format!(
                // dep: PostgreSQL(role_gateway) — test switches PG role to exercise RLS
                "SET LOCAL ROLE role_gateway; SET LOCAL humaux.tenant_id = '{tenant_a}';"
            ))
            .expect("ctx A");
            let n: i64 = txn
                .query_one("SELECT count(*) FROM private.subjects", &[])
                .unwrap()
                .get(0);
            assert_eq!(n, 2, "tenant A sees its two subjects");
            let key: String = txn
                .query_one(
                    "SELECT key_value FROM private.subject_keys WHERE key_kind = 'CRM'",
                    &[],
                )
                .unwrap()
                .get(0);
            assert_eq!(key, "CRM-1001");
            txn.rollback().expect("rollback read A");
        }

        // Tenant B sees zero of tenant A's subjects/keys/roles.
        {
            let mut txn = handle.client.transaction().expect("begin read B");
            txn.batch_execute(&format!(
                // dep: PostgreSQL(role_gateway) — test switches PG role to exercise RLS
                "SET LOCAL ROLE role_gateway; SET LOCAL humaux.tenant_id = '{tenant_b}';"
            ))
            .expect("ctx B");
            for tbl in [
                "private.subjects",
                "private.subject_keys",
                "private.subject_roles",
            ] {
                let n: i64 = txn
                    .query_one(&format!("SELECT count(*) FROM {tbl}"), &[])
                    .unwrap()
                    .get(0);
                assert_eq!(n, 0, "tenant B must see 0 rows of {tbl}");
            }
            txn.rollback().expect("rollback read B");
        }
    });
}

/// Superuser seed of two tenants and one subject under the first (inside the caller's txn).
fn seed_two_tenants_one_subject<C: GenericClient>(c: &mut C, a: Uuid, b: Uuid) -> Uuid {
    c.execute(
        "INSERT INTO control.tenants (tenant_id, name) VALUES ($1, 'card7-fk-a'), ($2, 'card7-fk-b')",
        &[&a, &b],
    )
    .expect("seed tenants");
    c.query_one(
        "INSERT INTO private.subjects (tenant_id, kind, display_name) \
         VALUES ($1, 'PERSON', 'Tenant A subject') RETURNING subject_id",
        &[&a],
    )
    .expect("seed tenant A subject")
    .get(0)
}

#[test]
fn subject_fks_reject_foreign_tenant_subjects() {
    // RI checks bypass RLS, so a single-column FK on subject_id would let tenant B attach rows to
    // tenant A's subject and learn (via 23503 vs success) which ids exist. Every FK into
    // private.subjects carries the tenant leg: B's insert against A's id must fail exactly like
    // an insert against a random id — same SQLSTATE, same constraint.
    run_db_fixture::<SubjectFixture, _>("subject_fks_reject_foreign_tenant", |mut handle| {
        let tenant_a = TenantId::new().0;
        let tenant_b = TenantId::new().0;
        let mut txn = handle.client.transaction().expect("begin");
        let a_subject = seed_two_tenants_one_subject(&mut txn, tenant_a, tenant_b);
        txn.batch_execute(&format!(
            // dep: PostgreSQL(role_gateway) — test switches PG role to exercise RLS
            "SET LOCAL ROLE role_gateway; SET LOCAL humaux.tenant_id = '{tenant_b}';"
        ))
        .expect("gateway/tenant-B context");
        let visible: i64 = txn
            .query_one("SELECT count(*) FROM private.subjects", &[])
            .expect("count")
            .get(0);
        assert_eq!(visible, 0, "tenant B cannot see tenant A's subject");

        let attempts: [(&str, &str); 3] = [
            (
                "subject_keys",
                "INSERT INTO private.subject_keys (tenant_id, subject_id, key_kind, key_value) \
                 VALUES ($1, $2, 'CRM', 'CRM-FK')",
            ),
            (
                "subject_roles",
                "INSERT INTO private.subject_roles (tenant_id, subject_id, role) \
                 VALUES ($1, $2, 'CUSTOMER')",
            ),
            (
                "subjects.merged_into",
                "INSERT INTO private.subjects (tenant_id, kind, display_name, merged_into) \
                 VALUES ($1, 'PERSON', 'B merging into A', $2)",
            ),
        ];
        for (what, sql) in attempts {
            let mut signatures = Vec::new();
            for target in [a_subject, Uuid::new_v4()] {
                let mut sp = txn.savepoint("fk_probe").expect("savepoint");
                let err = sp.execute(sql, &[&tenant_b, &target]).expect_err(&format!(
                    "{what}: tenant B must not reference subject {target}"
                ));
                signatures.push(failure_signature(&err));
                sp.rollback().expect("rollback savepoint");
            }
            assert_eq!(
                signatures[0].0.as_ref(),
                Some(&SqlState::FOREIGN_KEY_VIOLATION),
                "{what}: foreign-tenant subject must be a FK violation, got {:?}",
                signatures[0]
            );
            assert_eq!(
                signatures[0], signatures[1],
                "{what}: foreign-tenant id and nonexistent id must fail identically (no oracle)"
            );
        }
        txn.rollback().expect("rollback");
    });
}

#[test]
fn subject_keys_unique_per_tenant_never_globally() {
    run_db_fixture::<SubjectFixture, _>("subject_keys_unique_per_tenant", |mut handle| {
        let tenant_a = TenantId::new().0;
        let tenant_b = TenantId::new().0;
        let mut txn = handle.client.transaction().expect("begin");
        let a_subject = seed_two_tenants_one_subject(&mut txn, tenant_a, tenant_b);
        const KEY: &str = "INSERT INTO private.subject_keys (tenant_id, subject_id, key_kind, key_value) \
                           VALUES ($1, $2, 'CRM', 'CRM-DUP')";

        txn.batch_execute(&format!(
            // dep: PostgreSQL(role_gateway) — test switches PG role to exercise RLS
            "SET LOCAL ROLE role_gateway; SET LOCAL humaux.tenant_id = '{tenant_a}';"
        ))
        .expect("gateway/tenant-A context");
        txn.execute(KEY, &[&tenant_a, &a_subject])
            .expect("first key registers");
        let dup = {
            let mut sp = txn.savepoint("dup").expect("savepoint");
            let err = sp
                .execute(KEY, &[&tenant_a, &a_subject])
                .expect_err("same (tenant, key_kind, key_value) twice must be rejected");
            sp.rollback().expect("rollback savepoint");
            failure_signature(&err)
        };
        assert_eq!(
            dup.0.as_ref(),
            Some(&SqlState::UNIQUE_VIOLATION),
            "duplicate key must be 23505, got {dup:?}"
        );

        // The same key under tenant B is a distinct key (never globally unique).
        txn.batch_execute(&format!("SET LOCAL humaux.tenant_id = '{tenant_b}';"))
            .expect("tenant-B context");
        let b_subject: Uuid = txn
            .query_one(
                "INSERT INTO private.subjects (tenant_id, kind, display_name) \
                 VALUES ($1, 'PERSON', 'Tenant B subject') RETURNING subject_id",
                &[&tenant_b],
            )
            .expect("tenant B registers its own subject")
            .get(0);
        txn.execute(KEY, &[&tenant_b, &b_subject])
            .expect("same CRM key under a second tenant must succeed");
        txn.rollback().expect("rollback");
    });
}
