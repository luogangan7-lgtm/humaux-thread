//! Card E1 integration tests — §8.5.1 affect annotation axis (migration 0156, ADR-0030) against
//! the *real* `private.memory_affects` under the real runtime roles (`SET LOCAL ROLE`):
//!
//!  1. `affect_checks_match_domain_enums` — the `affect_kind` / `label` / `target_scope_kind`
//!     CHECK literal sets equal the closed `AffectKind` / `EmotionLabel` /
//!     `AffectTargetScopeKind` enums, and the basis-point range CHECKs reject the first value
//!     outside `BasisPoints::signed` / `unit` (§78.2, D-B: fail closed, never clamped).
//!  2. `rows_are_immutable_and_cascade_with_memory_and_subject` — `role_gateway` can INSERT but
//!     an UPDATE is rejected by the owner trigger (`23514`) and DELETE has no grant (`42501`);
//!     the MOOD⇔half-life CHECK holds both ways; deleting the target subject (§37 ERASE) cascades
//!     the affect row while the memory survives; deleting the memory cascades everything (D-E).
//!  3. `affect_recheck_is_one_round_trip_for_the_whole_candidate_set` — the ONE set-based read
//!     every reader shares (`affect_repo::AFFECTS_FOR_MEMORIES_SQL`) fetches 50 candidates'
//!     rows with exactly one scan of `memory_affects` (`pg_stat_xact_user_tables`, the
//!     transaction's own counters) — a per-row loop would show 50 (card E1 speed goal).
//!  4. `evidence_affects_copy_onto_the_primary_memory_under_private_worker` — the 0157 write-side
//!     carrier: `role_gateway` INSERTs `evidence_affects` (remember.put's transaction), the rows
//!     are immutable like `memory_affects`, and the PRIMARY `memory_evidence` INSERT performed by
//!     `role_private_worker` (the Distill hop's role, its own grants + RLS) copies them verbatim
//!     onto the newborn memory — a SUPPORTING link copies nothing (D-C, main-line ruling 2).
//!
//! Skip contract (`humaux_testkit`, §79.2): no DSN / unreachable / 0156+0157 not applied ⇒ visible SKIP.
//! Every test runs inside one rolled-back transaction, so a passing run leaves the dev DB clean.

use std::time::Instant;

use humaux_adapters::affect_repo::AFFECTS_FOR_MEMORIES_SQL;
use humaux_domain::affect::{AffectKind, AffectTargetScopeKind, BasisPoints, EmotionLabel};
use humaux_domain::ids::{TenantId, UserId};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::error::SqlState;
use postgres::{Client, GenericClient, NoTls};
use uuid::Uuid;

struct AffectHandle {
    client: Client,
}

struct AffectFixture;

impl DbIntegrationFixture for AffectFixture {
    type Handle = AffectHandle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut client = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
        let ready: bool = client
            .query_one(
                "SELECT to_regclass('private.memory_affects') IS NOT NULL \
                 AND to_regprocedure('private.reject_memory_affect_update()') IS NOT NULL \
                 AND to_regclass('private.evidence_affects') IS NOT NULL \
                 AND to_regprocedure('private.memory_affects_inherit_from_evidence()') IS NOT NULL",
                &[],
            )
            .map(|r| r.get(0))
            .unwrap_or(false);
        if !ready {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "migrations 0156_memory_affects / 0157_evidence_affects not applied".to_string(),
            ));
        }
        Ok(AffectHandle { client })
    }
}

/// Superuser seed inside the caller's transaction: tenant + owner user (ACTIVE member) + reasoning
/// domain. Returns `(user_id, reasoning_domain_id)`.
fn seed_tenant<C: GenericClient>(c: &mut C, tenant: Uuid, tag: &str) -> (Uuid, Uuid) {
    c.execute(
        "INSERT INTO control.tenants (tenant_id, name) VALUES ($1, $2)",
        &[&tenant, &format!("cardE1-{tag}")],
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
             VALUES ($1, 'cardE1') RETURNING reasoning_domain_id",
            &[&tenant],
        )
        .expect("seed reasoning domain")
        .get(0);
    (user, domain)
}

/// One TENANT_SHARED EVENT Evidence (+ events row); core PG only: the digest is
/// `sha256(convert_to(gen_random_uuid()::text,'UTF8'))`.
fn seed_evidence<C: GenericClient>(c: &mut C, tenant: Uuid, domain: Uuid) -> Uuid {
    let evidence: Uuid = c
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                visibility_class, reasoning_domain_id) \
             VALUES ($1, 'EVENT', sha256(convert_to(gen_random_uuid()::text, 'UTF8')), 'INTERNAL', \
                     'DirectUserInput', 'TENANT_SHARED', $2) RETURNING evidence_id",
            &[&tenant, &domain],
        )
        .expect("seed evidence")
        .get(0);
    c.execute(
        "INSERT INTO private.events (event_id, event_kind, payload) VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)",
        &[&evidence],
    )
    .expect("seed event");
    evidence
}

/// One TENANT_SHARED memory record (no evidence link yet).
fn seed_memory_record<C: GenericClient>(c: &mut C, tenant: Uuid) -> Uuid {
    c.query_one(
        "INSERT INTO private.memory_records \
           (tenant_id, memory_type, content, visibility_class, authority_class, confidence, \
            status, asserted_at) \
         VALUES ($1, 'NOTE', '{\"title\":\"renewal\"}'::jsonb, 'TENANT_SHARED', 'PrivateKnowledge', 0.9, 'active', now()) \
         RETURNING memory_id",
        &[&tenant],
    )
    .expect("seed memory")
    .get(0)
}

/// [`seed_evidence`] + a memory PRIMARY-linked to it. Returns `(evidence_id, memory_id)`.
fn seed_memory<C: GenericClient>(c: &mut C, tenant: Uuid, domain: Uuid) -> (Uuid, Uuid) {
    let evidence = seed_evidence(c, tenant, domain);
    let memory = seed_memory_record(c, tenant);
    c.execute(
        "INSERT INTO private.memory_evidence (memory_id, evidence_id, role, ordinal) \
         VALUES ($1, $2, 'PRIMARY', 0)",
        &[&memory, &evidence],
    )
    .expect("seed PRIMARY link");
    (evidence, memory)
}

const INSERT_EVIDENCE_AFFECT: &str = "INSERT INTO private.evidence_affects \
       (tenant_id, affect_kind, label, valence_bp, arousal_bp, dominance_bp, \
        intensity_bp, confidence_bp, evidence_id, target_subject_id, observed_at, half_life_seconds) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now(), $11) RETURNING affect_id";

/// `(kind, label, valence, intensity, evidence, target, half_life)` of one table's rows for one
/// parent, in write order — the verbatim-copy witness.
#[allow(clippy::type_complexity)] // the full copied tuple is the assertion; an alias would only rename it.
fn affect_tuples<C: GenericClient>(
    c: &mut C,
    table: &str,
    parent_column: &str,
    parent: Uuid,
) -> Vec<(
    String,
    Option<String>,
    Option<i16>,
    i16,
    Uuid,
    Option<Uuid>,
    Option<i32>,
)> {
    c.query(
        &format!(
            "SELECT affect_kind, label, valence_bp, intensity_bp, evidence_id, target_subject_id, \
                    half_life_seconds FROM private.{table} WHERE {parent_column} = $1 \
             ORDER BY created_at, affect_id"
        ),
        &[&parent],
    )
    .expect("affect tuples")
    .into_iter()
    .map(|r| {
        (
            r.get(0),
            r.get(1),
            r.get(2),
            r.get(3),
            r.get(4),
            r.get(5),
            r.get(6),
        )
    })
    .collect()
}

const INSERT_AFFECT: &str = "INSERT INTO private.memory_affects \
       (tenant_id, memory_id, affect_kind, label, valence_bp, arousal_bp, dominance_bp, \
        intensity_bp, confidence_bp, evidence_id, target_subject_id, observed_at, half_life_seconds) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now(), $12) RETURNING affect_id";

fn sqlstate(e: &postgres::Error) -> Option<SqlState> {
    e.code().cloned()
}

fn affects_of<C: GenericClient>(c: &mut C, memory: Uuid) -> i64 {
    c.query_one(
        "SELECT count(*) FROM private.memory_affects WHERE memory_id = $1",
        &[&memory],
    )
    .expect("count affects")
    .get(0)
}

/// `seq_scan + idx_scan` of `private.memory_affects` in the CURRENT transaction.
fn affect_table_scans<C: GenericClient>(c: &mut C) -> i64 {
    c.query_one(
        "SELECT coalesce(seq_scan, 0) + coalesce(idx_scan, 0) FROM pg_stat_xact_user_tables \
         WHERE schemaname = 'private' AND relname = 'memory_affects'",
        &[],
    )
    .expect("xact table stats")
    .get(0)
}

#[test]
fn affect_checks_match_domain_enums() {
    run_db_fixture::<AffectFixture, _>("affect_checks_match_domain_enums", |mut handle| {
        let cases: [(&str, Vec<&str>); 3] = [
            (
                "affect_kind",
                AffectKind::ALL.iter().map(|k| k.as_str()).collect(),
            ),
            (
                "label",
                EmotionLabel::ALL.iter().map(|l| l.as_str()).collect(),
            ),
            (
                "target_scope_kind",
                AffectTargetScopeKind::ALL
                    .iter()
                    .map(|s| s.as_str())
                    .collect(),
            ),
        ];
        // 0156 memory_affects and the 0157 evidence_affects carrier carry the SAME closed sets
        // (SQL cannot share a CHECK; the Rust side still has one list per enum).
        for table in ["memory_affects", "evidence_affects"] {
            for (column, wire) in &cases {
                let def: String = handle
                    .client
                    .query_one(
                        &format!(
                            "SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c \
                             WHERE c.conrelid = 'private.{table}'::regclass AND c.contype = 'c' \
                               AND pg_get_constraintdef(c.oid) LIKE '%{column} %' \
                               AND (pg_get_constraintdef(c.oid) LIKE '%IN (%' \
                                    OR pg_get_constraintdef(c.oid) LIKE '%= ANY (ARRAY[%')"
                        ),
                        &[],
                    )
                    .unwrap_or_else(|e| panic!("no closed-set CHECK on {table}.{column}: {e}"))
                    .get(0);
                for w in wire {
                    assert!(
                        def.contains(&format!("'{w}'")),
                        "{table}.{column} CHECK missing {w}: {def}"
                    );
                }
                assert_eq!(
                    def.matches('\'').count() / 2,
                    wire.len(),
                    "{table}.{column} CHECK literal count != enum size: {def}"
                );
            }
        }
        // Basis-point range CHECKs = the Rust constructors' ranges, first value outside each.
        assert!(BasisPoints::signed(10_001).is_err() && BasisPoints::unit(10_001).is_err());
        let tenant = TenantId::new().0;
        let mut txn = handle.client.transaction().expect("begin");
        let (_user, domain) = seed_tenant(&mut txn, tenant, "checks");
        let (evidence, memory) = seed_memory(&mut txn, tenant, domain);
        for (column, value) in [
            ("valence_bp", 10_001i16),
            ("valence_bp", -10_001),
            ("intensity_bp", -1),
            ("intensity_bp", 10_001),
            ("confidence_bp", 10_001),
        ] {
            let mut sp = txn.savepoint("range").expect("savepoint");
            let (v, i, c) = match column {
                "valence_bp" => (Some(value), 5_000i16, 5_000i16),
                "intensity_bp" => (None, value, 5_000),
                _ => (None, 5_000, value),
            };
            let err = sp
                .query_one(
                    INSERT_AFFECT,
                    &[
                        &tenant,
                        &memory,
                        &"EMOTION",
                        &None::<&str>,
                        &v,
                        &None::<i16>,
                        &None::<i16>,
                        &i,
                        &c,
                        &evidence,
                        &None::<Uuid>,
                        &None::<i32>,
                    ],
                )
                .expect_err("out-of-range basis points must be rejected, never clamped");
            assert_eq!(
                sqlstate(&err),
                Some(SqlState::CHECK_VIOLATION),
                "{column}={value}: {err}"
            );
            drop(sp);
        }
        txn.rollback().expect("rollback");
    });
}

#[test]
#[allow(clippy::too_many_lines)] // One live-DB oracle keeps the immutability trigger, MOOD<->half_life CHECKs, subject/memory cascade and the closed-set contract causally ordered in a single rolled-back transaction (same precedent as card 9's S1/S3).
fn rows_are_immutable_and_cascade_with_memory_and_subject() {
    run_db_fixture::<AffectFixture, _>("rows_are_immutable_and_cascade", |mut handle| {
        let tenant = TenantId::new().0;
        let mut txn = handle.client.transaction().expect("begin");
        let (user, domain) = seed_tenant(&mut txn, tenant, "immutable");
        let (evidence, memory) = seed_memory(&mut txn, tenant, domain);
        let person: Uuid = txn
            .query_one(
                "INSERT INTO private.subjects (tenant_id, kind, display_name) \
                 VALUES ($1, 'PERSON', 'Ada Lovelace') RETURNING subject_id",
                &[&tenant],
            )
            .expect("seed subject")
            .get(0);

        // role_gateway annotates (INSERT grant + RLS WITH CHECK through the parent memory).
        txn.batch_execute(&format!(
            "SET LOCAL ROLE role_gateway; SET LOCAL humaux.tenant_id = '{tenant}'; \
             SET LOCAL humaux.user_id = '{user}';"
        ))
        .expect("gateway context");
        let emotion: Uuid = txn
            .query_one(
                INSERT_AFFECT,
                &[
                    &tenant,
                    &memory,
                    &"EMOTION",
                    &Some("FRUSTRATION"),
                    &Some(-8_000i16),
                    &Some(5_000i16),
                    &Some(-4_000i16),
                    &9_000i16,
                    &10_000i16,
                    &evidence,
                    &Some(person),
                    &None::<i32>,
                ],
            )
            .expect("gateway inserts an EMOTION about the person")
            .get(0);
        let _mood: Uuid = txn
            .query_one(
                INSERT_AFFECT,
                &[
                    &tenant,
                    &memory,
                    &"MOOD",
                    &Some("ANXIETY"),
                    &Some(-3_000i16),
                    &Some(2_000i16),
                    &None::<i16>,
                    &8_200i16,
                    &7_000i16,
                    &evidence,
                    &None::<Uuid>,
                    &Some(21_600i32),
                ],
            )
            .expect("gateway inserts a MOOD with the write-time half-life")
            .get(0);
        // MOOD ⇔ half-life, both directions (D-B).
        for (kind, half_life) in [("MOOD", None::<i32>), ("EMOTION", Some(3_600))] {
            let mut sp = txn.savepoint("mood_hl").expect("savepoint");
            let err = sp
                .query_one(
                    INSERT_AFFECT,
                    &[
                        &tenant,
                        &memory,
                        &kind,
                        &None::<&str>,
                        &None::<i16>,
                        &None::<i16>,
                        &None::<i16>,
                        &5_000i16,
                        &5_000i16,
                        &evidence,
                        &None::<Uuid>,
                        &half_life,
                    ],
                )
                .expect_err("MOOD without / EMOTION with a half-life is rejected");
            assert_eq!(
                sqlstate(&err),
                Some(SqlState::CHECK_VIOLATION),
                "{kind}: {err}"
            );
            drop(sp);
        }
        // Immutable: UPDATE is rejected by the owner trigger even for a role holding SELECT+INSERT.
        {
            let mut sp = txn.savepoint("update").expect("savepoint");
            let err = sp
                .execute(
                    "UPDATE private.memory_affects SET intensity_bp = 1 WHERE affect_id = $1",
                    &[&emotion],
                )
                .expect_err("affect rows never mutate");
            // role_gateway has no UPDATE grant at all (42501); the owner trigger (23514) is the
            // second line for any role that ever gets one. Either code proves immutability.
            assert!(
                matches!(
                    sqlstate(&err),
                    Some(SqlState::INSUFFICIENT_PRIVILEGE | SqlState::CHECK_VIOLATION)
                ),
                "{err}"
            );
            drop(sp);
        }
        {
            let mut sp = txn.savepoint("delete").expect("savepoint");
            let err = sp
                .execute(
                    "DELETE FROM private.memory_affects WHERE affect_id = $1",
                    &[&emotion],
                )
                .expect_err("no runtime role deletes affect rows");
            assert_eq!(
                sqlstate(&err),
                Some(SqlState::INSUFFICIENT_PRIVILEGE),
                "{err}"
            );
            drop(sp);
        }
        txn.batch_execute("RESET ROLE").expect("superuser");
        // The owner trigger itself: an owner-side UPDATE (the only role with the privilege) is
        // still refused — immutability is a mechanism, not a grant accident.
        {
            let mut sp = txn.savepoint("owner_update").expect("savepoint");
            let err = sp
                .execute(
                    "UPDATE private.memory_affects SET intensity_bp = 1 WHERE affect_id = $1",
                    &[&emotion],
                )
                .expect_err("owner trigger rejects UPDATE");
            assert_eq!(sqlstate(&err), Some(SqlState::CHECK_VIOLATION), "{err}");
            drop(sp);
        }
        assert_eq!(affects_of(&mut txn, memory), 2);

        // §37 subject ERASE: the affect about the person is terminal, the memory (and the
        // MOOD row, which targets nobody) survive.
        txn.execute(
            "DELETE FROM private.subjects WHERE subject_id = $1",
            &[&person],
        )
        .expect("erase subject");
        assert_eq!(
            affects_of(&mut txn, memory),
            1,
            "EMOTION about the person cascaded"
        );
        let memory_alive: i64 = txn
            .query_one(
                "SELECT count(*) FROM private.memory_records WHERE memory_id = $1",
                &[&memory],
            )
            .expect("memory")
            .get(0);
        assert_eq!(memory_alive, 1);
        // Memory purge cascades the rest (the cascade is the owner's DELETE, not a role's).
        txn.execute(
            "DELETE FROM private.memory_records WHERE memory_id = $1",
            &[&memory],
        )
        .expect("purge memory");
        assert_eq!(affects_of(&mut txn, memory), 0);
        txn.rollback().expect("rollback");
    });
}

#[test]
fn affect_recheck_is_one_round_trip_for_the_whole_candidate_set() {
    run_db_fixture::<AffectFixture, _>("affect_recheck_is_one_round_trip", |mut handle| {
        let tenant = TenantId::new().0;
        let mut txn = handle.client.transaction().expect("begin");
        let (user, domain) = seed_tenant(&mut txn, tenant, "roundtrip");
        let mut memory_ids = Vec::with_capacity(50);
        for _ in 0..50 {
            let (evidence, memory) = seed_memory(&mut txn, tenant, domain);
            txn.query_one(
                INSERT_AFFECT,
                &[
                    &tenant,
                    &memory,
                    &"EMOTION",
                    &Some("JOY"),
                    &Some(6_000i16),
                    &Some(3_000i16),
                    &None::<i16>,
                    &7_000i16,
                    &9_000i16,
                    &evidence,
                    &None::<Uuid>,
                    &None::<i32>,
                ],
            )
            .expect("seed affect");
            memory_ids.push(memory);
        }
        // The read exactly as the gateway's hydrate gate issues it: role_gateway + tenant GUCs.
        txn.batch_execute(&format!(
            "SET LOCAL ROLE role_gateway; SET LOCAL humaux.tenant_id = '{tenant}'; \
             SET LOCAL humaux.user_id = '{user}';"
        ))
        .expect("gateway context");
        // Warm the statement/planner once so the measured pass is the steady-state shape.
        let before = affect_table_scans(&mut txn);
        let started = Instant::now();
        let rows = txn
            .query(AFFECTS_FOR_MEMORIES_SQL, &[&tenant, &memory_ids])
            .expect("one set-based read");
        let elapsed = started.elapsed();
        let after = affect_table_scans(&mut txn);
        assert_eq!(rows.len(), 50, "every candidate's affect row came back");
        assert_eq!(
            after - before,
            1,
            "the affect re-check scans memory_affects exactly once for 50 candidates (a per-row loop would show 50)"
        );
        eprintln!(
            "memory_affects: 50-candidate affect re-check = 1 scan, {} µs",
            elapsed.as_micros()
        );
        txn.batch_execute("RESET ROLE").expect("superuser");
        txn.rollback().expect("rollback");
    });
}

#[test]
#[allow(clippy::too_many_lines)] // One live-DB oracle keeps the gateway INSERT, immutability, the private_worker PRIMARY copy and the SUPPORTING no-copy causally ordered in a single rolled-back transaction.
fn evidence_affects_copy_onto_the_primary_memory_under_private_worker() {
    run_db_fixture::<AffectFixture, _>("evidence_affects_copy_onto_primary", |mut handle| {
        let tenant = TenantId::new().0;
        let mut txn = handle.client.transaction().expect("begin");
        let (user, domain) = seed_tenant(&mut txn, tenant, "carrier");
        let evidence = seed_evidence(&mut txn, tenant, domain);
        let person: Uuid = txn
            .query_one(
                "INSERT INTO private.subjects (tenant_id, kind, display_name) \
                 VALUES ($1, 'PERSON', 'Ada Lovelace') RETURNING subject_id",
                &[&tenant],
            )
            .expect("seed subject")
            .get(0);

        // remember.put's transaction: role_gateway declares the affects on the Evidence.
        txn.batch_execute(&format!(
            "SET LOCAL ROLE role_gateway; SET LOCAL humaux.tenant_id = '{tenant}'; \
             SET LOCAL humaux.user_id = '{user}';"
        ))
        .expect("gateway context");
        let emotion: Uuid = txn
            .query_one(
                INSERT_EVIDENCE_AFFECT,
                &[
                    &tenant,
                    &"EMOTION",
                    &Some("FRUSTRATION"),
                    &Some(-8_000i16),
                    &Some(5_000i16),
                    &Some(-4_000i16),
                    &9_000i16,
                    &10_000i16,
                    &evidence,
                    &Some(person),
                    &None::<i32>,
                ],
            )
            .expect("gateway declares an EMOTION about the person on the Evidence")
            .get(0);
        txn.query_one(
            INSERT_EVIDENCE_AFFECT,
            &[
                &tenant,
                &"MOOD",
                &Some("ANXIETY"),
                &Some(-3_000i16),
                &Some(2_000i16),
                &None::<i16>,
                &8_200i16,
                &7_000i16,
                &evidence,
                &None::<Uuid>,
                &Some(21_600i32),
            ],
        )
        .expect("gateway declares a MOOD with the write-time half-life");
        // Immutable like memory_affects: UPDATE refused, DELETE has no grant.
        {
            let mut sp = txn.savepoint("update").expect("savepoint");
            let err = sp
                .execute(
                    "UPDATE private.evidence_affects SET intensity_bp = 1 WHERE affect_id = $1",
                    &[&emotion],
                )
                .expect_err("evidence affect rows never mutate");
            assert!(
                matches!(
                    sqlstate(&err),
                    Some(SqlState::INSUFFICIENT_PRIVILEGE | SqlState::CHECK_VIOLATION)
                ),
                "{err}"
            );
            drop(sp);
        }
        {
            let mut sp = txn.savepoint("delete").expect("savepoint");
            let err = sp
                .execute(
                    "DELETE FROM private.evidence_affects WHERE affect_id = $1",
                    &[&emotion],
                )
                .expect_err("no runtime role deletes evidence affect rows");
            assert_eq!(
                sqlstate(&err),
                Some(SqlState::INSUFFICIENT_PRIVILEGE),
                "{err}"
            );
            drop(sp);
        }
        txn.batch_execute("RESET ROLE").expect("superuser");
        {
            let mut sp = txn.savepoint("owner_update").expect("savepoint");
            let err = sp
                .execute(
                    "UPDATE private.evidence_affects SET intensity_bp = 1 WHERE affect_id = $1",
                    &[&emotion],
                )
                .expect_err("owner trigger rejects UPDATE");
            assert_eq!(sqlstate(&err), Some(SqlState::CHECK_VIOLATION), "{err}");
            drop(sp);
        }
        let declared = affect_tuples(&mut txn, "evidence_affects", "evidence_id", evidence);
        assert_eq!(declared.len(), 2);

        // The Distill hop's transaction: role_private_worker links the newborn memory to its
        // PRIMARY Evidence — the 0157 trigger copies the declaration under the worker's own
        // SELECT(evidence_affects) + INSERT(memory_affects) grants and RLS.
        let born = seed_memory_record(&mut txn, tenant);
        let supporting = seed_memory_record(&mut txn, tenant);
        txn.batch_execute(&format!(
            "SET LOCAL ROLE role_private_worker; SET LOCAL humaux.tenant_id = '{tenant}'; \
             SET LOCAL humaux.user_id = '{}';",
            Uuid::nil()
        ))
        .expect("private worker context");
        txn.execute(
            "INSERT INTO private.memory_evidence (memory_id, evidence_id, role, ordinal) \
             VALUES ($1, $2, 'PRIMARY', 0)",
            &[&born, &evidence],
        )
        .expect("private_worker links the PRIMARY evidence");
        txn.execute(
            "INSERT INTO private.memory_evidence (memory_id, evidence_id, role, ordinal) \
             VALUES ($1, $2, 'SUPPORTING', 0)",
            &[&supporting, &evidence],
        )
        .expect("private_worker links a SUPPORTING evidence");
        txn.batch_execute("RESET ROLE").expect("superuser");
        let inherited = affect_tuples(&mut txn, "memory_affects", "memory_id", born);
        assert_eq!(
            inherited, declared,
            "the PRIMARY-linked memory carries the Evidence's affects verbatim (kind, label, VAD, intensity, provenance, target, half-life)"
        );
        assert_eq!(
            affects_of(&mut txn, supporting),
            0,
            "a SUPPORTING link inherits nothing"
        );
        // Provenance of the copy is the Evidence itself, and the target subject survived the copy.
        assert!(inherited.iter().all(|t| t.4 == evidence));
        assert_eq!(inherited[0].5, Some(person));
        txn.rollback().expect("rollback");
    });
}
