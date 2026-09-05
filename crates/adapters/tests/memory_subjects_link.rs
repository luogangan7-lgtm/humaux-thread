//! Card 8 integration tests — §6.1.3 memory↔subject linkage + the deterministic resolve hook
//! (migration 0154, ADR-0028). Everything runs against the *real* `private` objects the migration
//! created, under the real runtime roles via `SET LOCAL ROLE` (never a superuser shortcut for the
//! assertion under test):
//!
//!  1. `link_checks_match_domain_enums` — `memory_subjects.relation` / `source_kind` CHECK literal
//!     sets equal the closed `SubjectLinkRelation` / `SubjectLinkSource` enums (§78.2).
//!  2. `declared_and_key_links_carry_source_kind_and_mention_spans` — under `role_private_worker`
//!     the hook links an explicit DECLARED subject and, as INHERITED (§6.1.3 rule 3), one declared
//!     by external key on the PRIMARY Evidence; both ABOUT / 10000 bp, and every mention span
//!     indexes back into the exact stored revision (`convert_to(content::text)`, sha256-bound).
//!  3. `cross_tenant_subject_is_unlinkable` — tenant B cannot link tenant A's subject: the tenant-leg
//!     FK fails byte-identically to a nonexistent id (no existence oracle), and B's RLS resolve of
//!     A's id is empty.
//!  4. `correction_inherits_but_explicit_supersede_does_not` — the 0154 trigger: a USER_CORRECTION
//!     successor inherits the predecessor's links (INHERITED) under `role_gateway`; an explicit
//!     supersede onto a non-correction successor inherits nothing.
//!  5. `rollup_inherits_source_subjects_under_consolidation_role` — `link_rollup_subjects` under
//!     `role_consolidation_worker` (SELECT on memory_subjects, INSERT on memory_rollup_subjects).
//!
//! Skip contract (`humaux_testkit`, §79.2): no DSN / unreachable / 0154 not applied ⇒ visible SKIP.
//! Every test runs inside one rolled-back transaction, so a passing run leaves the dev DB clean.

use humaux_domain::ids::{TenantId, UserId};
use humaux_domain::subject::{SubjectLinkRelation, SubjectLinkSource};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::error::SqlState;
use postgres::{Client, GenericClient, NoTls};
use uuid::Uuid;

struct LinkHandle {
    client: Client,
}

struct LinkFixture;

impl DbIntegrationFixture for LinkFixture {
    type Handle = LinkHandle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut client = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
        let ready: bool = client
            .query_one(
                "SELECT to_regprocedure('private.link_memory_subjects(uuid,uuid,uuid[],text[])') \
                 IS NOT NULL AND to_regclass('private.memory_subjects') IS NOT NULL",
                &[],
            )
            .map(|r| r.get(0))
            .unwrap_or(false);
        if !ready {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "migration 0154_memory_subjects not applied".to_string(),
            ));
        }
        Ok(LinkHandle { client })
    }
}

/// Superuser seed inside the caller's transaction: tenant + owner user (ACTIVE member) + reasoning
/// domain. Returns `(user_id, reasoning_domain_id)`.
fn seed_tenant<C: GenericClient>(c: &mut C, tenant: Uuid, tag: &str) -> (Uuid, Uuid) {
    c.execute(
        "INSERT INTO control.tenants (tenant_id, name) VALUES ($1, $2)",
        &[&tenant, &format!("card8-{tag}")],
    )
    .expect("seed tenant");
    let user = UserId::new().0;
    c.execute("INSERT INTO control.users (user_id) VALUES ($1)", &[&user])
        .expect("seed user");
    c.execute(
        "INSERT INTO control.memberships (tenant_id, user_id, role, state) \
         VALUES ($1, $2, 'OWNER', 'ACTIVE')",
        &[&tenant, &user],
    )
    .expect("seed membership");
    let domain: Uuid = c
        .query_one(
            "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
             VALUES ($1, 'card8') RETURNING reasoning_domain_id",
            &[&tenant],
        )
        .expect("seed reasoning domain")
        .get(0);
    (user, domain)
}

fn seed_subject<C: GenericClient>(c: &mut C, tenant: Uuid, kind: &str, name: &str) -> Uuid {
    c.query_one(
        "INSERT INTO private.subjects (tenant_id, kind, display_name) \
         VALUES ($1, $2, $3) RETURNING subject_id",
        &[&tenant, &kind, &name],
    )
    .expect("seed subject")
    .get(0)
}

/// One TENANT_SHARED EVENT Evidence (+ events row) and a memory PRIMARY-linked to it, exactly the
/// shape `distill_repo::insert_memory` writes. Returns `(evidence_id, memory_id)`.
fn seed_memory<C: GenericClient>(
    c: &mut C,
    tenant: Uuid,
    domain: Uuid,
    event_kind: &str,
    content: &serde_json::Value,
) -> (Uuid, Uuid) {
    let evidence: Uuid = c
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                visibility_class, reasoning_domain_id) \
             VALUES ($1, 'EVENT', sha256(convert_to(gen_random_uuid()::text, 'UTF8')), 'INTERNAL', 'DirectUserInput', \
                     'TENANT_SHARED', $2) RETURNING evidence_id",
            &[&tenant, &domain],
        )
        .expect("seed evidence")
        .get(0);
    c.execute(
        "INSERT INTO private.events (event_id, event_kind, payload) VALUES ($1, $2, '{}'::jsonb)",
        &[&evidence, &event_kind],
    )
    .expect("seed event");
    let memory: Uuid = c
        .query_one(
            "INSERT INTO private.memory_records \
               (tenant_id, memory_type, content, visibility_class, authority_class, confidence, \
                status, asserted_at) \
             VALUES ($1, 'NOTE', $2, 'TENANT_SHARED', 'PrivateKnowledge', 0.9, 'active', now()) \
             RETURNING memory_id",
            &[&tenant, content],
        )
        .expect("seed memory")
        .get(0);
    c.execute(
        "INSERT INTO private.memory_evidence (memory_id, evidence_id, role, ordinal) \
         VALUES ($1, $2, 'PRIMARY', 0)",
        &[&memory, &evidence],
    )
    .expect("seed PRIMARY link");
    (evidence, memory)
}

/// `(subject_id, relation, source_kind, confidence_bp)` rows for one memory, superuser view.
fn links<C: GenericClient>(c: &mut C, memory: Uuid) -> Vec<(Uuid, String, String, i16)> {
    c.query(
        "SELECT subject_id, relation, source_kind, confidence_bp FROM private.memory_subjects \
         WHERE memory_id = $1 ORDER BY subject_id",
        &[&memory],
    )
    .expect("read links")
    .into_iter()
    .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
    .collect()
}

fn failure_signature(e: &postgres::Error) -> (Option<SqlState>, Option<String>) {
    (
        e.code().cloned(),
        e.as_db_error()
            .and_then(|d| d.constraint())
            .map(str::to_string),
    )
}

#[test]
fn link_checks_match_domain_enums() {
    run_db_fixture::<LinkFixture, _>("link_checks_match_domain_enums", |mut handle| {
        let cases: [(&str, Vec<&str>); 2] = [
            (
                "relation",
                SubjectLinkRelation::ALL
                    .iter()
                    .map(|r| r.as_str())
                    .collect(),
            ),
            (
                "source_kind",
                SubjectLinkSource::ALL.iter().map(|s| s.as_str()).collect(),
            ),
        ];
        for (column, wire) in cases {
            let def: String = handle
                .client
                .query_one(
                    &format!(
                        "SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c \
                         WHERE c.conrelid = 'private.memory_subjects'::regclass AND c.contype = 'c' \
                           AND pg_get_constraintdef(c.oid) LIKE '%{column} %'"
                    ),
                    &[],
                )
                .unwrap_or_else(|e| panic!("no CHECK on memory_subjects.{column}: {e}"))
                .get(0);
            for w in &wire {
                assert!(
                    def.contains(&format!("'{w}'")),
                    "memory_subjects.{column} CHECK missing {w}: {def}"
                );
            }
            assert_eq!(
                def.matches('\'').count() / 2,
                wire.len(),
                "memory_subjects.{column} CHECK literal count != enum size: {def}"
            );
        }
    });
}

#[test]
fn declared_and_key_links_carry_source_kind_and_mention_spans() {
    run_db_fixture::<LinkFixture, _>("declared_and_key_links", |mut handle| {
        let tenant = TenantId::new().0;
        let mut txn = handle.client.transaction().expect("begin");
        let (user, domain) = seed_tenant(&mut txn, tenant, "links");
        let person = seed_subject(&mut txn, tenant, "PERSON", "Ada Lovelace");
        let org = seed_subject(&mut txn, tenant, "ORGANISATION", "Analytical Engines Ltd");
        txn.execute(
            "INSERT INTO private.subject_keys (tenant_id, subject_id, key_kind, key_value) \
             VALUES ($1, $2, 'CRM', 'CRM-1001')",
            &[&tenant, &org],
        )
        .expect("crm key");
        let content = serde_json::json!({
            "title": "Renewal call",
            "key_claim": "Ada Lovelace (account CRM-1001) wants the renewal moved to Q4.",
        });
        let (evidence, memory) = seed_memory(&mut txn, tenant, domain, "USER_MESSAGE", &content);
        // The org was declared on the Evidence by exact external key (what remember.put writes).
        txn.execute(
            "INSERT INTO private.evidence_subjects (tenant_id, evidence_id, subject_id, source_kind) \
             VALUES ($1, $2, $3, 'EXTERNAL_KEY')",
            &[&tenant, &evidence, &org],
        )
        .expect("evidence declaration");

        // The hook, as the Distill hop runs it: role_private_worker, tenant + acting user GUCs.
        txn.batch_execute(&format!(
            "SET LOCAL ROLE role_private_worker; SET LOCAL humaux.tenant_id = '{tenant}'; \
             SET LOCAL humaux.user_id = '{user}';"
        ))
        .expect("private worker context");
        let linked: i32 = txn
            .query_one(
                "SELECT private.link_memory_subjects($1, $2, ARRAY[$3]::uuid[], ARRAY['DECLARED']::text[])",
                &[&tenant, &memory, &person],
            )
            .expect("hook runs under role_private_worker")
            .get(0);
        assert_eq!(
            linked, 2,
            "one DECLARED + one INHERITED (Evidence-declared) link"
        );
        // Idempotent: a second run adds nothing.
        let again: i32 = txn
            .query_one(
                "SELECT private.link_memory_subjects($1, $2, ARRAY[$3]::uuid[], ARRAY['DECLARED']::text[])",
                &[&tenant, &memory, &person],
            )
            .expect("second run")
            .get(0);
        assert_eq!(again, 0, "hook is idempotent");
        txn.batch_execute("RESET ROLE").expect("superuser");

        let mut rows = links(&mut txn, memory);
        rows.sort_by(|a, b| a.2.cmp(&b.2));
        assert_eq!(
            rows,
            vec![
                (person, "ABOUT".to_owned(), "DECLARED".to_owned(), 10_000),
                (org, "ABOUT".to_owned(), "INHERITED".to_owned(), 10_000),
            ],
            "source_kind + confidence per resolve rule (Evidence declaration → INHERITED)"
        );

        // Mention spans index back into the exact stored revision and are sha256-bound to it.
        let mentions = txn
            .query(
                "SELECT ms.subject_id, \
                        convert_from(substring(rev.body FROM ms.span_start + 1 FOR ms.span_end - ms.span_start), 'UTF8'), \
                        ms.revision_sha256 = sha256(rev.body) \
                 FROM private.memory_subject_mentions ms, \
                      (SELECT convert_to(content::text, 'UTF8') AS body FROM private.memory_records \
                        WHERE memory_id = $1) rev \
                 WHERE ms.memory_id = $1 ORDER BY ms.span_start",
                &[&memory],
            )
            .expect("read mentions")
            .into_iter()
            .map(|r| (r.get::<_, Uuid>(0), r.get::<_, String>(1), r.get::<_, bool>(2)))
            .collect::<Vec<_>>();
        assert_eq!(
            mentions.len(),
            2,
            "one span per mentioned subject: {mentions:?}"
        );
        assert!(
            mentions
                .iter()
                .any(|(id, text, bound)| *id == person && text == "Ada Lovelace" && *bound),
            "person display_name span indexes the stored revision: {mentions:?}"
        );
        assert!(
            mentions
                .iter()
                .any(|(id, text, bound)| *id == org && text == "CRM-1001" && *bound),
            "org key value span indexes the stored revision: {mentions:?}"
        );
        txn.rollback().expect("rollback");
    });
}

#[test]
fn cross_tenant_subject_is_unlinkable() {
    run_db_fixture::<LinkFixture, _>("cross_tenant_subject_is_unlinkable", |mut handle| {
        let tenant_a = TenantId::new().0;
        let tenant_b = TenantId::new().0;
        let mut txn = handle.client.transaction().expect("begin");
        let (_ua, _da) = seed_tenant(&mut txn, tenant_a, "a");
        let (user_b, domain_b) = seed_tenant(&mut txn, tenant_b, "b");
        let a_subject = seed_subject(&mut txn, tenant_a, "PERSON", "Tenant A person");
        let (_e, b_memory) = seed_memory(
            &mut txn,
            tenant_b,
            domain_b,
            "USER_MESSAGE",
            &serde_json::json!({"key_claim": "tenant B memory"}),
        );

        txn.batch_execute(&format!(
            "SET LOCAL ROLE role_private_worker; SET LOCAL humaux.tenant_id = '{tenant_b}'; \
             SET LOCAL humaux.user_id = '{user_b}';"
        ))
        .expect("tenant B worker context");
        // Resolution (what subject_repo::resolve_declaration_in_txn runs) is empty under B's RLS.
        let visible: i64 = txn
            .query_one(
                "SELECT count(*) FROM private.subjects WHERE tenant_id = $1 AND subject_id = $2",
                &[&tenant_b, &a_subject],
            )
            .expect("count")
            .get(0);
        assert_eq!(visible, 0, "tenant B cannot resolve tenant A's subject");
        // Forcing the link anyway (bypassing resolution) hits the tenant-leg FK exactly as a
        // nonexistent id does — no existence oracle through RI (which bypasses RLS).
        let mut signatures = Vec::new();
        for target in [a_subject, Uuid::new_v4()] {
            let mut sp = txn.savepoint("probe").expect("savepoint");
            let err = sp
                .execute(
                    "SELECT private.link_memory_subjects($1, $2, ARRAY[$3]::uuid[], ARRAY['DECLARED']::text[])",
                    &[&tenant_b, &b_memory, &target],
                )
                .expect_err("foreign / unknown subject must not link");
            signatures.push(failure_signature(&err));
            sp.rollback().expect("rollback savepoint");
        }
        assert_eq!(
            signatures[0].0.as_ref(),
            Some(&SqlState::FOREIGN_KEY_VIOLATION),
            "foreign-tenant subject is a FK violation: {:?}",
            signatures[0]
        );
        assert_eq!(
            signatures[0], signatures[1],
            "foreign-tenant id and nonexistent id fail identically"
        );
        txn.rollback().expect("rollback");
    });
}

#[test]
fn correction_inherits_but_explicit_supersede_does_not() {
    run_db_fixture::<LinkFixture, _>("correction_inherits", |mut handle| {
        let tenant = TenantId::new().0;
        let mut txn = handle.client.transaction().expect("begin");
        let (user, domain) = seed_tenant(&mut txn, tenant, "correct");
        let person = seed_subject(&mut txn, tenant, "PERSON", "Grace Hopper");
        let body =
            serde_json::json!({"key_claim": "Grace Hopper prefers COBOL reviews on Fridays."});
        let (_e1, m1) = seed_memory(&mut txn, tenant, domain, "USER_MESSAGE", &body);
        txn.execute(
            "SELECT private.link_memory_subjects($1, $2, ARRAY[$3]::uuid[], ARRAY['DECLARED']::text[])",
            &[&tenant, &m1, &person],
        )
        .expect("link M1");
        // M2: a USER_CORRECTION version (what correct_atomically materialises before its UPDATE).
        let corrected =
            serde_json::json!({"key_claim": "Grace Hopper prefers COBOL reviews on Mondays."});
        let (_e2, m2) = seed_memory(&mut txn, tenant, domain, "USER_CORRECTION", &corrected);
        assert!(
            links(&mut txn, m2).is_empty(),
            "M2 has no links before the arbiter UPDATE"
        );

        // The arbiter UPDATE, as role_gateway runs it (column-scoped UPDATE grant, RLS with the
        // correcting user installed) — the 0154 trigger fires here.
        txn.batch_execute(&format!(
            "SET LOCAL ROLE role_gateway; SET LOCAL humaux.tenant_id = '{tenant}'; \
             SET LOCAL humaux.user_id = '{user}';"
        ))
        .expect("gateway context");
        let updated = txn
            .execute(
                "UPDATE private.memory_records \
                    SET status = 'superseded', superseded_by = $2, superseded_at = clock_timestamp() \
                  WHERE tenant_id = $3 AND memory_id = $1 AND status = 'active'",
                &[&m1, &m2, &tenant],
            )
            .expect("gateway supersedes M1 by its correction");
        assert_eq!(updated, 1);
        txn.batch_execute("RESET ROLE").expect("superuser");
        let m2_links = links(&mut txn, m2);
        assert_eq!(
            m2_links,
            vec![(person, "ABOUT".to_owned(), "INHERITED".to_owned(), 10_000)],
            "the correction inherits M1's link as INHERITED"
        );
        let mention_text: String = txn
            .query_one(
                "SELECT convert_from(substring(convert_to(m.content::text,'UTF8') \
                           FROM ms.span_start + 1 FOR ms.span_end - ms.span_start), 'UTF8') \
                 FROM private.memory_subject_mentions ms JOIN private.memory_records m USING (memory_id) \
                 WHERE ms.memory_id = $1",
                &[&m2],
            )
            .expect("M2 mention")
            .get(0);
        assert_eq!(
            mention_text, "Grace Hopper",
            "mention re-located in M2's own revision"
        );

        // An explicit supersede onto a non-correction successor inherits nothing.
        let (_e3, m3) = seed_memory(
            &mut txn,
            tenant,
            domain,
            "USER_MESSAGE",
            &serde_json::json!({"key_claim": "unrelated replacement"}),
        );
        txn.execute(
            "UPDATE private.memory_records SET status = 'superseded', superseded_by = $2 \
             WHERE memory_id = $1",
            &[&m2, &m3],
        )
        .expect("explicit supersede");
        assert!(
            links(&mut txn, m3).is_empty(),
            "explicit supersede must not copy subjects onto an arbitrary successor"
        );
        txn.rollback().expect("rollback");
    });
}

#[test]
fn rollup_inherits_source_subjects_under_consolidation_role() {
    run_db_fixture::<LinkFixture, _>("rollup_inherits_source_subjects", |mut handle| {
        let tenant = TenantId::new().0;
        let mut txn = handle.client.transaction().expect("begin");
        let (_user, domain) = seed_tenant(&mut txn, tenant, "rollup");
        let org = seed_subject(&mut txn, tenant, "ORGANISATION", "Babbage & Co");
        let (evidence, memory) = seed_memory(
            &mut txn,
            tenant,
            domain,
            "USER_MESSAGE",
            &serde_json::json!({"key_claim": "Babbage & Co renewed."}),
        );
        txn.execute(
            "SELECT private.link_memory_subjects($1, $2, ARRAY[$3]::uuid[], ARRAY['DECLARED']::text[])",
            &[&tenant, &memory, &org],
        )
        .expect("link source memory");
        let run: Uuid = txn
            .query_one(
                "INSERT INTO private.memory_consolidation_runs (tenant_id, reasoning_domain_id) \
                 VALUES ($1, $2) RETURNING run_id",
                &[&tenant, &domain],
            )
            .expect("run")
            .get(0);
        let rollup: Uuid = txn
            .query_one(
                "INSERT INTO private.memory_rollups \
                   (tenant_id, run_id, content, authority_class, visibility_class) \
                 VALUES ($1, $2, '{}'::jsonb, 'PrivateKnowledge', 'TENANT_SHARED') RETURNING rollup_id",
                &[&tenant, &run],
            )
            .expect("rollup")
            .get(0);
        txn.execute(
            "INSERT INTO private.memory_rollup_sources (rollup_id, memory_id, evidence_id) \
             VALUES ($1, $2, $3)",
            &[&rollup, &memory, &evidence],
        )
        .expect("rollup source");

        txn.batch_execute(&format!(
            "SET LOCAL ROLE role_consolidation_worker; SET LOCAL humaux.tenant_id = '{tenant}';"
        ))
        .expect("consolidation context");
        let linked: i32 = txn
            .query_one(
                "SELECT private.link_rollup_subjects($1, $2)",
                &[&tenant, &rollup],
            )
            .expect("rollup hook under role_consolidation_worker")
            .get(0);
        assert_eq!(linked, 1);
        let rows: Vec<(Uuid, String)> = txn
            .query(
                "SELECT subject_id, source_kind FROM private.memory_rollup_subjects WHERE rollup_id = $1",
                &[&rollup],
            )
            .expect("consolidation worker reads its own rows")
            .into_iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
        assert_eq!(rows, vec![(org, "INHERITED".to_owned())]);
        txn.rollback().expect("rollback");
    });
}
