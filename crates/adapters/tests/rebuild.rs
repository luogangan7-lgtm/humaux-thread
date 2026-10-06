//! `adapters::tests::rebuild` — card 37 (ADR-0064): the rebuild generation side table, its definers and triggers,
//!   the embedding-fingerprint binding, the DR receipt tables, and the projection worker's stored vectors, against a
//!   real PostgreSQL throwaway database per test (and a real Qdrant collection for the worker tests). S1 holds the
//!   DB-only tests (module `c37_s1`); S2 the top-level worker tests; S3 appends the rebuild tests.
//! Depends-on: crates=[async-trait, humaux-adapters, humaux-domain, humaux-infra-cell, humaux-local-secret-scan,
//!   humaux-projection, humaux-testkit, postgres, serde_json, sha2, sqlx, tokio];
//!   services=[PostgreSQL(owner) w=[control.private_reasoning_domains, control.tenants,
//!   ops.outbox, private.events, private.evidence_objects, private.memory_evidence, private.memory_records,
//!   projection.embedding_fingerprints, projection.memory_vectors, projection.private_memory_points,
//!   projection.stream_checkpoints, projection.stream_log, projection.tenant_placements]
//!   r=[ops.commit_seq_seq, projection.rebuild_runs, projection.rebuild_tickets],
//!   PostgreSQL(role_maintenance) w=[ops.backup_receipts,
//!   ops.backup_sets, ops.restore_drills] x=[projection.issue_rebuild_tickets, projection.rebuild_close,
//!   projection.rebuild_open], PostgreSQL(role_retrieval_worker) w=[projection.embedding_fingerprints,
//!   projection.stream_log], Qdrant(*)];
//!   env=[HUMAUX_TEST_PG_DSN]; modules=[adapters::forget_repo, adapters::postgres,
//!   adapters::private_projection_registry, adapters::projection_worker, adapters::provisioning, adapters::qdrant,
//!   adapters::rebuild, adapters::retrieve, adapters::stream_repo, adapters::tests::support::a2_fixture,
//!   adapters::tests::support::governance_ops, adapters::tests::support::scratch_qdrant,
//!   adapters::tests::support::throwaway_db, domain::authority, domain::confirm, domain::egress, domain::error,
//!   domain::ids, humaux-local-secret-scan, humaux-testkit, infra-cell::permit, infra-cell::resource,
//!   infra-cell::transport, projection::embedding_fingerprint, projection::serving]
//! Called-by: [cargo-test]
//! Invariants: [every test owns its throwaway database humaux_thread_c37_s1_<pid>_<n> / humaux_thread_c37_s2_<pid>_<n>,
//!   created and migrated by the fixture and dropped WITH (FORCE) by its Drop even on panic, so nothing here touches
//!   the shared dev database (ruling E7); the S2 fixture's Qdrant collection is a throwaway dropped by its Drop; the S3
//!   tests own a scratch Qdrant container humaux-c37-qdrant-<pid>-<n> each, one at a time, removed by its guard; every
//!   write runs as the role that will make it in production (role_maintenance for the definers and receipts,
//!   role_retrieval_worker for the fingerprint binding, the settles and the worker); every refusal is asserted by
//!   SQLSTATE or typed error, never by message text alone]
//! Spec: Baseline §15.2; §37.2; §44; §6.2.2; ADR-0064 D-A; ADR-0064 D-B; ADR-0064 D-C; ADR-0064 D-D; ADR-0064 D-E;
//!   ADR-0064 D-J; ADR-0064 D-O

#[path = "support/throwaway_db.rs"]
#[allow(dead_code)]
mod throwaway_db;

mod c37_s1 {
    use std::time::Duration;

    use humaux_adapters::postgres::{MaintenanceDbPool, RetrievalWorkerDbPool};
    use humaux_adapters::private_projection_registry::{
        PrivateProjectionRegistryError, bind_embedding_fingerprint, worker_fingerprint_inputs,
    };
    use humaux_adapters::stream_repo;
    use humaux_projection::embedding_fingerprint::{
        DISTANCE, DTYPE, EmbeddingFingerprint, EmbeddingFingerprintInputs, NORMALIZATION,
    };
    use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
    use postgres::error::SqlState;
    use postgres::{Client, NoTls};
    use sqlx::types::Uuid;

    use super::throwaway_db::{self, ThrowawayDb};

    const SCOPE_KIND: &str = "workspace";
    const DOMAIN: &str = "humaux_private_memory";
    const KIND: &str = "PRIVATE_MEMORY";
    const VERSION: &str = "v1";
    const LABEL: &str = "c37-embed@2026-10";

    fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
        let sep = if admin_dsn.contains('?') { '&' } else { '?' };
        format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
    }

    fn fingerprint(revision: &str) -> [u8; 32] {
        EmbeddingFingerprint::compute(&EmbeddingFingerprintInputs {
            provider: "c37-test",
            model_id: "c37-model",
            model_revision: revision,
            dimension: 4,
            task_type: "document",
            preprocessing_version: "v1-test",
            projection_contract_version: VERSION,
        })
        .expect("test fingerprint")
        .0
    }

    /// Fields drop in order: the role connections close before `_db` drops the database.
    struct Handle {
        admin: Client,
        maintenance: Client,
        retrieval: Client,
        dsn: String,
        tenant: Uuid,
        scope_id: Uuid,
        fingerprint: Vec<u8>,
        _db: ThrowawayDb,
    }

    struct S1Fixture;

    impl DbIntegrationFixture for S1Fixture {
        type Handle = Handle;

        fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
            let setup = |e: &dyn std::fmt::Display| {
                DbFixtureSkipReason::IsolationSetupFailed(e.to_string())
            };
            let db = throwaway_db::create("c37_s1")?;
            let dsn = db.dsn();
            // dep: PostgreSQL(owner) — seeds this test's throwaway database
            let mut admin = Client::connect(&dsn, NoTls)
                .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
            let tenant: Uuid = admin
                .query_one(
                    "INSERT INTO control.tenants (name) VALUES ('c37 s1') RETURNING tenant_id",
                    &[],
                )
                .map_err(|e| setup(&e))?
                .get(0);
            let scope_id = Uuid::new_v4();
            admin
                .execute(
                    "INSERT INTO projection.stream_checkpoints (tenant_id, scope_kind, scope_id, domain, \
                       projection_kind, projection_version, issued_highwater) VALUES ($1,$2,$3,$4,$5,$6,0)",
                    &[&tenant, &SCOPE_KIND, &scope_id, &DOMAIN, &KIND, &VERSION],
                )
                .map_err(|e| setup(&e))?;
            // ADR-0064 D-C: the closed sets of the 0232 CHECKs come from the Rust constants (contract reconcile).
            let fingerprint = fingerprint("2026-10").to_vec();
            admin
                .execute(
                    "INSERT INTO projection.embedding_fingerprints (fingerprint_sha256, embedding_version, provider, \
                       model_id, model_revision, dimension, task_type, preprocessing_version, \
                       projection_contract_version, dtype, normalization, distance) \
                     VALUES ($1, $2, 'c37-test', 'c37-model', '2026-10', 4, 'document', 'v1-test', $3, $4, $5, $6)",
                    &[&fingerprint, &format!("{LABEL}-run"), &VERSION, &DTYPE, &NORMALIZATION, &DISTANCE],
                )
                .map_err(|e| setup(&e))?;
            let role = |name: &str| -> Result<Client, DbFixtureSkipReason> {
                // dep: PostgreSQL(any) — role_maintenance or role_retrieval_worker, the production writer roles
                let mut c = Client::connect(&dsn_as_role(&dsn, name), NoTls)
                    .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
                c.batch_execute(&format!("SET humaux.tenant_id = '{tenant}'"))
                    .map_err(|e| setup(&e))?;
                Ok(c)
            };
            Ok(Handle {
                maintenance: role("role_maintenance")?,
                retrieval: role("role_retrieval_worker")?,
                admin,
                dsn,
                tenant,
                scope_id,
                fingerprint,
                _db: db,
            })
        }
    }

    impl Handle {
        /// One Evidence with one active memory and one ticket per `commit_seqs` entry on the stream (the first is its
        /// EVIDENCE_ACCEPTED carrier, the rest MEMORY_LIFECYCLE carriers), each in `state`. Returns the evidence id.
        fn seed_evidence(&mut self, commit_seqs: &[i64], state: &str) -> Uuid {
            let mut txn = self.admin.transaction().expect("seed transaction");
            let evidence: Uuid = txn
                .query_one(
                    "WITH d AS (INSERT INTO control.private_reasoning_domains (tenant_id, name) \
                                VALUES ($1, 'c37 ' || gen_random_uuid()) RETURNING reasoning_domain_id) \
                     INSERT INTO private.evidence_objects (tenant_id, evidence_kind, payload_sha256, data_class, \
                       origin_class, visibility_class, reasoning_domain_id) \
                     SELECT $1, 'EVENT', sha256(gen_random_uuid()::text::bytea), 'INTERNAL', 'DirectUserInput', \
                       'TENANT_SHARED', reasoning_domain_id FROM d RETURNING evidence_id",
                    &[&self.tenant],
                )
                .expect("seed evidence")
                .get(0);
            txn.execute(
                "INSERT INTO private.events (event_id, event_kind, payload) VALUES ($1, 'USER_MESSAGE', '{}')",
                &[&evidence],
            )
            .expect("seed event");
            txn.execute(
                "WITH m AS (INSERT INTO private.memory_records (tenant_id, memory_type, content, visibility_class, \
                              authority_class, confidence, status, asserted_at) \
                            VALUES ($1, 'NOTE', '{\"title\":\"c37\"}', 'TENANT_SHARED', 'PrivateKnowledge', 0.9, \
                              'active', now()) RETURNING memory_id) \
                 INSERT INTO private.memory_evidence (memory_id, evidence_id, role, ordinal) \
                 SELECT m.memory_id, $2, 'PRIMARY', 0 FROM m",
                &[&self.tenant, &evidence],
            )
            .expect("seed memory");
            for (i, commit) in commit_seqs.iter().enumerate() {
                let event_type = if i == 0 {
                    "EVIDENCE_ACCEPTED"
                } else {
                    "MEMORY_LIFECYCLE"
                };
                let seq: i64 = txn
                    .query_one(
                        "UPDATE projection.stream_checkpoints SET issued_highwater = issued_highwater + 1 \
                          WHERE tenant_id = $1 AND scope_id = $2 RETURNING issued_highwater",
                        &[&self.tenant, &self.scope_id],
                    )
                    .expect("bump issued_highwater")
                    .get(0);
                txn.execute(
                    "INSERT INTO ops.outbox (tenant_id, commit_seq, stream_seq, event_type, evidence_id, status) \
                     VALUES ($1, $2, $3, $4, $5, 'DONE')",
                    &[&self.tenant, commit, &seq, &event_type, &evidence],
                )
                .expect("seed outbox");
                txn.execute(
                    "INSERT INTO projection.stream_log (tenant_id, scope_kind, scope_id, domain, projection_kind, \
                       projection_version, stream_seq, commit_seq, state, settled_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, \
                             CASE WHEN $9 IN ('DONE', 'SKIPPED_BY_POLICY', 'FAILED', 'TOMBSTONED') THEN now() END)",
                    &[&self.tenant, &SCOPE_KIND, &self.scope_id, &DOMAIN, &KIND, &VERSION, &seq, commit, &state],
                )
                .expect("seed ticket");
            }
            txn.commit().expect("seed commit");
            evidence
        }

        /// A generation-1 issuer's ticket for an existing commit: bumps the counter, inserts ISSUED. Returns its seq.
        fn issue_plain_ticket(&mut self, commit: i64) -> i64 {
            let mut txn = self.admin.transaction().expect("issue transaction");
            let seq: i64 = txn
                .query_one(
                    "UPDATE projection.stream_checkpoints SET issued_highwater = issued_highwater + 1 \
                      WHERE tenant_id = $1 AND scope_id = $2 RETURNING issued_highwater",
                    &[&self.tenant, &self.scope_id],
                )
                .expect("bump")
                .get(0);
            txn.execute(
                "INSERT INTO projection.stream_log (tenant_id, scope_kind, scope_id, domain, projection_kind, \
                   projection_version, stream_seq, commit_seq) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                &[&self.tenant, &SCOPE_KIND, &self.scope_id, &DOMAIN, &KIND, &VERSION, &seq, &commit],
            )
            .expect("plain ticket");
            txn.commit().expect("issue commit");
            seq
        }

        fn open(&mut self) -> (Uuid, i32, i64, bool) {
            let row = self
                .maintenance
                .query_one(
                    "SELECT run_id, generation, boundary_seq, resumed \
                       FROM projection.rebuild_open($1, $2, $3, $4, $5, $6, $7)",
                    &[
                        &self.tenant,
                        &SCOPE_KIND,
                        &self.scope_id,
                        &DOMAIN,
                        &KIND,
                        &VERSION,
                        &self.fingerprint,
                    ],
                )
                .expect("rebuild_open");
            (row.get(0), row.get(1), row.get(2), row.get(3))
        }

        fn issue(&mut self, run: Uuid, limit: i32) -> Result<i64, postgres::Error> {
            self.maintenance
                .query_one(
                    "SELECT projection.issue_rebuild_tickets($1, $2, false)",
                    &[&run, &limit],
                )
                .map(|r| r.get(0))
        }

        fn close(&mut self, run: Uuid, boundary: i64) -> Result<(), postgres::Error> {
            self.maintenance
                .execute(
                    "SELECT projection.rebuild_close($1, $2, 'equivalent', 0, NULL, '{}'::jsonb)",
                    &[&run, &boundary],
                )
                .map(|_| ())
        }

        fn high(&mut self) -> i64 {
            self.admin
                .query_one(
                    "SELECT issued_highwater FROM projection.stream_checkpoints WHERE tenant_id = $1 AND scope_id = $2",
                    &[&self.tenant, &self.scope_id],
                )
                .expect("issued_highwater")
                .get(0)
        }

        /// The generation tickets of `run` as `(stream_seq, commit_seq, state)`, by seq.
        fn generation(&mut self, run: Uuid) -> Vec<(i64, i64, String)> {
            self.admin
                .query(
                    "SELECT sl.stream_seq, sl.commit_seq, sl.state FROM projection.rebuild_tickets t \
                       JOIN projection.stream_log sl USING (tenant_id, scope_kind, scope_id, domain, projection_kind, \
                                                            projection_version, stream_seq) \
                      WHERE t.run_id = $1 ORDER BY 1",
                    &[&run],
                )
                .expect("generation tickets")
                .iter()
                .map(|r| (r.get(0), r.get(1), r.get(2)))
                .collect()
        }

        /// Settles ISSUED tickets DONE the way the projection runner does (role_retrieval_worker, 0167 edge).
        fn settle(&mut self, seqs: &[i64]) {
            for seq in seqs {
                let n = self
                    .retrieval
                    .execute(
                        "UPDATE projection.stream_log SET state = 'DONE' \
                          WHERE tenant_id = $1 AND scope_id = $2 AND stream_seq = $3 AND state = 'ISSUED'",
                        &[&self.tenant, &self.scope_id, seq],
                    )
                    .expect("settle DONE");
                assert_eq!(n, 1, "ticket {seq} settles");
            }
        }

        fn state(&mut self, seq: i64) -> String {
            self.admin
                .query_one(
                    "SELECT state FROM projection.stream_log WHERE tenant_id = $1 AND scope_id = $2 AND stream_seq = $3",
                    &[&self.tenant, &self.scope_id, &seq],
                )
                .expect("ticket state")
                .get(0)
        }
    }

    fn sqlstate(result: Result<impl std::fmt::Debug, postgres::Error>) -> SqlState {
        match result {
            Ok(v) => panic!("expected a refusal, got Ok({v:?})"),
            Err(e) => e
                .code()
                .cloned()
                .unwrap_or_else(|| panic!("no SQLSTATE: {e:?}")),
        }
    }

    /// T-C1 (ADR-0064 D-C): a label already bound to a fingerprint cannot be bound to another one — the raw binding
    /// INSERT gets 23505 (S1; fault: drop `UNIQUE (embedding_version)` from 0232) and the adapter
    /// `bind_embedding_fingerprint` returns `FingerprintMismatch` (S2; fault: compare by label only).
    #[test]
    fn a_label_bound_to_another_fingerprint_is_refused() {
        run_db_fixture::<S1Fixture, _>(
            "a_label_bound_to_another_fingerprint_is_refused",
            |mut h| {
                let bind = |c: &mut Client, fp: &[u8]| {
                    c.execute(
                    "INSERT INTO projection.embedding_fingerprints (fingerprint_sha256, embedding_version, provider, \
                       model_id, model_revision, dimension, task_type, preprocessing_version, \
                       projection_contract_version, dtype, normalization, distance) \
                     VALUES ($1, $2, 'c37-test', 'c37-model', 'r', 4, 'document', 'v1-test', 'v1', $3, $4, $5)",
                    &[&fp, &LABEL, &DTYPE, &NORMALIZATION, &DISTANCE],
                )
                };
                let first = fingerprint("r1");
                let other = fingerprint("r2");
                assert_ne!(first, other);
                assert_eq!(bind(&mut h.retrieval, &first).expect("first binding"), 1);
                assert_eq!(
                    sqlstate(bind(&mut h.retrieval, &other)),
                    SqlState::UNIQUE_VIOLATION,
                    "a second fingerprint under the same label must be refused"
                );
                // The closed sets of 0232 hold: a dtype or distance outside the Rust constants is a CHECK violation.
                let rogue = h.retrieval.execute(
                "INSERT INTO projection.embedding_fingerprints (fingerprint_sha256, embedding_version, provider, \
                   model_id, model_revision, dimension, task_type, preprocessing_version, projection_contract_version, \
                   dtype, normalization, distance) \
                 VALUES ($1, 'c37-other', 'p', 'm', 'r', 4, 't', 'v', 'v1', 'float16', 'n', 'Dot')",
                &[&other.to_vec()],
            );
                assert_eq!(sqlstate(rogue), SqlState::CHECK_VIOLATION);

                // S2 half: the adapter the retrieval worker boots through binds once, re-binds the same
                // fingerprint idempotently and refuses another one by fingerprint, not by label.
                let rt = tokio::runtime::Runtime::new().expect("runtime");
                // dep: PostgreSQL(role_retrieval_worker) — the boot binding's own role
                let pool = rt
                    .block_on(RetrievalWorkerDbPool::connect(&dsn_as_role(
                        &h.dsn,
                        "role_retrieval_worker",
                    )))
                    .expect("role_retrieval_worker pool");
                let label = format!("{LABEL}-adapter");
                let bind = |revision: &str| {
                    rt.block_on(bind_embedding_fingerprint(
                        &pool,
                        &label,
                        &worker_fingerprint_inputs("c37-test", "c37-model", revision, 4, VERSION),
                    ))
                };
                let bound = bind("r1").expect("first adapter binding");
                assert_eq!(bind("r1").expect("same fingerprint again"), bound);
                assert!(
                    matches!(
                        bind("r2"),
                        Err(PrivateProjectionRegistryError::FingerprintMismatch)
                    ),
                    "a second model under the same label must be refused by the adapter"
                );
            },
        );
    }

    /// T-A1 (ADR-0064 D-A, card fault): a stream with 3 Evidences and 5 generation-1 tickets; g2 issues one ticket per
    /// Evidence on its home commit, an immediate re-run issues 0, g2 closes, g3 issues 3 again. Fault: drop
    /// `generation` from `rebuild_tickets_generation_unique` → g3 errors (23505) → red.
    #[test]
    fn generation_tickets_are_unique_per_input_and_generation() {
        run_db_fixture::<S1Fixture, _>(
            "generation_tickets_are_unique_per_input_and_generation",
            |mut h| {
                h.seed_evidence(&[101, 104], "DONE");
                h.seed_evidence(&[102, 105], "DONE");
                h.seed_evidence(&[103], "DONE");
                assert_eq!(h.high(), 5);

                let (g2, generation, boundary, resumed) = h.open();
                assert_eq!((generation, boundary, resumed), (2, 5, false));
                assert_eq!(h.issue(g2, 10).expect("g2 issue"), 3);
                assert_eq!(
                    h.issue(g2, 10).expect("g2 re-run"),
                    0,
                    "an input is ticketed once per generation"
                );
                let tickets = h.generation(g2);
                assert_eq!(
                    tickets.iter().map(|t| t.1).collect::<Vec<_>>(),
                    vec![101, 102, 103],
                    "each ticket carries its Evidence's home commit"
                );
                assert_eq!(
                    tickets.iter().map(|t| t.0).collect::<Vec<_>>(),
                    vec![6, 7, 8]
                );
                let (again, _, _, resumed) = h.open();
                assert_eq!(
                    (again, resumed),
                    (g2, true),
                    "the open run is resumed, not duplicated"
                );
                h.settle(&[6, 7, 8]);
                h.close(g2, 8).expect("g2 closes");

                let (g3, generation, boundary, resumed) = h.open();
                assert_eq!((generation, boundary, resumed), (3, 8, false));
                assert_eq!(
                    h.issue(g3, 10).expect("g3 issue"),
                    3,
                    "a later generation re-tickets every input"
                );
                assert_eq!(
                    h.generation(g3).iter().map(|t| t.1).collect::<Vec<_>>(),
                    vec![101, 102, 103]
                );
            },
        );
    }

    /// T-A2 (ADR-0064 D-A completion terms): `rebuild_close` refuses while a generation ticket is in flight, when
    /// the boundary moved after the verifier's read, and while a write that arrived during the run is in flight;
    /// it closes once all three hold. Fault: delete the in-flight check from `rebuild_close` → the first close
    /// succeeds → red.
    #[test]
    fn close_refuses_while_a_generation_ticket_is_in_flight_or_the_boundary_moved() {
        run_db_fixture::<S1Fixture, _>(
            "close_refuses_while_a_generation_ticket_is_in_flight_or_the_boundary_moved",
            |mut h| {
                h.seed_evidence(&[201], "DONE");
                h.seed_evidence(&[202], "DONE");
                let (run, _, boundary, _) = h.open();
                assert_eq!(boundary, 2);
                assert_eq!(h.issue(run, 10).expect("issue"), 2);
                assert_eq!(
                    sqlstate(h.close(run, 4)),
                    SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE,
                    "generation_in_flight"
                );
                h.settle(&[3, 4]);
                // A generation-1 write arrives during the run.
                let late = h.issue_plain_ticket(201);
                assert_eq!(late, 5);
                assert_eq!(
                    sqlstate(h.close(run, 4)),
                    SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE,
                    "boundary_moved"
                );
                assert_eq!(
                    sqlstate(h.close(run, 5)),
                    SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE,
                    "catch_up_in_flight"
                );
                let open: bool = h
                    .admin
                    .query_one(
                        "SELECT closed_at IS NULL FROM projection.rebuild_runs WHERE run_id = $1",
                        &[&run],
                    )
                    .expect("run row")
                    .get(0);
                assert!(open, "every refusal leaves the run open");
                h.settle(&[late]);
                h.close(run, 5)
                    .expect("closes once the generation and the catch-up are settled");
                let verdict: String = h
                    .admin
                    .query_one(
                        "SELECT verdict FROM projection.rebuild_runs WHERE run_id = $1",
                        &[&run],
                    )
                    .expect("closed run")
                    .get(0);
                assert_eq!(verdict, "equivalent");
            },
        );
    }

    /// T-A4 (ADR-0064 D-A, finding 16): card 35's `sweep_lost` with `sla = 0` moves the stale generation-1 ticket to
    /// LOST and leaves every generation ticket ISSUED. Fault: drop the `stream_log_generation_never_lost` trigger.
    #[test]
    fn a_generation_backlog_older_than_lost_after_is_never_swept() {
        run_db_fixture::<S1Fixture, _>(
            "a_generation_backlog_older_than_lost_after_is_never_swept",
            |mut h| {
                h.seed_evidence(&[301], "ISSUED");
                h.seed_evidence(&[302], "DONE");
                let (run, _, _, _) = h.open();
                assert_eq!(h.issue(run, 10).expect("issue"), 2);
                // Strictly older than the sweep's now(): the sweep compares issued_at < now() - sla.
                h.admin
                .execute(
                    "UPDATE projection.stream_log SET issued_at = issued_at - interval '1 hour' \
                      WHERE tenant_id = $1 AND scope_id = $2",
                    &[&h.tenant, &h.scope_id],
                )
                .expect("backdate");
                let rt = tokio::runtime::Runtime::new().expect("runtime");
                let swept = rt.block_on(async {
                    // dep: PostgreSQL(role_maintenance) — card 35's sweep, the production caller
                    let pool = MaintenanceDbPool::connect(&dsn_as_role(&h.dsn, "role_maintenance"))
                        .await
                        .expect("maintenance pool");
                    stream_repo::sweep_lost(&pool, h.tenant, Duration::ZERO, 100)
                        .await
                        .expect("sweep_lost")
                });
                assert_eq!(swept, 1, "only the stale generation-1 ticket is swept");
                assert_eq!(h.state(1), "LOST");
                let generation = h.generation(run);
                assert_eq!(generation.len(), 2);
                assert!(
                    generation.iter().all(|t| t.2 == "ISSUED"),
                    "a generation backlog is never moved to LOST: {generation:?}"
                );
            },
        );
    }

    /// T-J1 (ADR-0064 D-J, 10.11 C/I): the table refuses a VERIFIED receipt without a clean verify (23514) or with a
    /// manifest other than the label's first-verified one (23503), a VERIFIED row missing its label, manifest or verify
    /// exit and a FAILED row without its failure (23514), and accepts a budget refusal row with a NULL label. Faults: drop
    /// `backup_receipts_verified_derived`, the `backup_sets` FK, or `backup_receipts_shape`.
    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one table of receipt shapes, each inserted and judged by SQLSTATE in place"
    )]
    fn a_verified_receipt_without_a_clean_verify_or_with_another_manifest_is_refused_by_the_table()
    {
        run_db_fixture::<S1Fixture, _>(
            "a_verified_receipt_without_a_clean_verify_or_with_another_manifest_is_refused_by_the_table",
            |mut h| {
                let first = vec![1_u8; 32];
                let other = vec![2_u8; 32];
                h.maintenance
                    .execute(
                        "INSERT INTO ops.backup_sets (backup_label, manifest_sha256) VALUES ('20261006-003000F', $1)",
                        &[&first],
                    )
                    .expect("first verification of the label");
                let receipt = |c: &mut Client,
                               label: Option<&str>,
                               manifest: Option<&Vec<u8>>,
                               verify_exit: Option<i32>,
                               verified: Option<&Vec<u8>>,
                               outcome: &str,
                               failure: Option<&str>| {
                    c.execute(
                        "INSERT INTO ops.backup_receipts (backup_label, backup_type, backup_started_at, \
                           backup_stopped_at, manifest_sha256, verify_exit, verified_manifest_sha256, outcome, failure, \
                           repo_bytes, repo_free_bytes, set_repo_bytes) \
                         VALUES ($1, 'full', CASE WHEN $1::text IS NULL THEN NULL ELSE now() - interval '5 min' END, \
                                 CASE WHEN $1::text IS NULL THEN NULL ELSE now() END, $2, $3, $4, $5, $6, 10, 20, 5)",
                        &[&label, &manifest, &verify_exit, &verified, &outcome, &failure],
                    )
                };
                let label = Some("20261006-003000F");
                let m = &mut h.maintenance;
                // verify exited 1, yet VERIFIED.
                assert_eq!(
                    sqlstate(receipt(
                        m,
                        label,
                        Some(&first),
                        Some(1),
                        Some(&first),
                        "VERIFIED",
                        None
                    )),
                    SqlState::CHECK_VIOLATION
                );
                // A failure named, yet VERIFIED.
                assert_eq!(
                    sqlstate(receipt(
                        m,
                        label,
                        Some(&first),
                        Some(0),
                        Some(&first),
                        "VERIFIED",
                        Some("x")
                    )),
                    SqlState::CHECK_VIOLATION
                );
                // The pulled-back manifest is not the one first verified under the label.
                assert_eq!(
                    sqlstate(receipt(
                        m,
                        label,
                        Some(&other),
                        Some(0),
                        Some(&other),
                        "VERIFIED",
                        None
                    )),
                    SqlState::FOREIGN_KEY_VIOLATION
                );
                // 10.11 I: VERIFIED with a NULL manifest, a NULL label or a NULL verify exit (the derived CHECK alone
                // reads TRUE = NULL there and passes); FAILED without its failure.
                assert_eq!(
                    sqlstate(receipt(
                        m,
                        label,
                        Some(&first),
                        None,
                        Some(&first),
                        "VERIFIED",
                        None
                    )),
                    SqlState::CHECK_VIOLATION
                );
                assert_eq!(
                    sqlstate(receipt(
                        m,
                        label,
                        None,
                        Some(0),
                        Some(&first),
                        "VERIFIED",
                        None
                    )),
                    SqlState::CHECK_VIOLATION
                );
                assert_eq!(
                    sqlstate(receipt(
                        m,
                        None,
                        Some(&first),
                        Some(0),
                        Some(&first),
                        "VERIFIED",
                        None
                    )),
                    SqlState::CHECK_VIOLATION
                );
                assert_eq!(
                    sqlstate(receipt(
                        m,
                        label,
                        Some(&first),
                        Some(1),
                        None,
                        "FAILED",
                        None
                    )),
                    SqlState::CHECK_VIOLATION
                );
                // Accepted: a budget refusal (no label, no manifest, no times) and a clean verification.
                receipt(
                    m,
                    None,
                    None,
                    None,
                    None,
                    "FAILED",
                    Some("budget_free_floor:1"),
                )
                .expect("refusal row");
                receipt(
                    m,
                    label,
                    Some(&first),
                    Some(0),
                    Some(&first),
                    "VERIFIED",
                    None,
                )
                .expect("clean VERIFIED");
                receipt(
                    m,
                    label,
                    Some(&first),
                    Some(1),
                    None,
                    "FAILED",
                    Some("verify_exit:1"),
                )
                .expect("FAILED");
                let rows: i64 = h
                    .admin
                    .query_one("SELECT count(*) FROM ops.backup_receipts", &[])
                    .expect("count")
                    .get(0);
                assert_eq!(rows, 3);
            },
        );
    }

    /// T-O1 (ADR-0064 D-O, finding 5, 10.11 H): `succeeded` is derived by `restore_drills_succeeded_derived`; a row
    /// claiming success with any failing or unreached check is refused (23514), and a passing row must say so.
    /// Fault: drop one term of the CHECK (`repo_intact` included).
    #[test]
    fn a_drill_receipt_claiming_success_with_a_failed_check_is_refused() {
        run_db_fixture::<S1Fixture, _>(
            "a_drill_receipt_claiming_success_with_a_failed_check_is_refused",
            |mut h| {
                const PASSING: &[(&str, &str)] = &[
                    ("manifest_matches", "true"),
                    ("witness_a_present", "true"),
                    ("witness_b_absent", "true"),
                    ("server_version_matches", "true"),
                    ("rebuild_equivalent", "true"),
                    ("repo_intact", "true"),
                    ("migrations_drift", "0"),
                    ("rls_unforced", "0"),
                    ("isolation_violations", "0"),
                    ("isolation_pairs", "2"),
                    ("payload_digest_mismatches", "0"),
                    ("provider_calls", "0"),
                    ("drill_archiver_attempts", "0"),
                    ("legacy_points_without_vector", "0"),
                    ("residue", "0"),
                    ("rebuild_points", "42"),
                ];
                let insert = |c: &mut Client,
                              succeeded: bool,
                              column: Option<(&str, &str)>,
                              failure: Option<&str>| {
                    let values: Vec<(&str, &str)> = PASSING
                        .iter()
                        .map(|&(k, v)| column.filter(|(ck, _)| *ck == k).unwrap_or((k, v)))
                        .collect();
                    let names = values
                        .iter()
                        .map(|(k, _)| *k)
                        .collect::<Vec<_>>()
                        .join(", ");
                    let literals = values
                        .iter()
                        .map(|(_, v)| *v)
                        .collect::<Vec<_>>()
                        .join(", ");
                    c.execute(
                    &format!(
                        "INSERT INTO ops.restore_drills (succeeded, failure, {names}) VALUES ($1, $2, {literals})"
                    ),
                    &[&succeeded, &failure],
                )
                };
                let m = &mut h.maintenance;
                insert(m, true, None, None)
                    .expect("a fully passing drill is recorded as succeeded");
                assert_eq!(
                    sqlstate(insert(m, false, None, None)),
                    SqlState::CHECK_VIOLATION,
                    "a passing drill cannot be recorded as failed either"
                );
                assert_eq!(
                    sqlstate(insert(m, true, None, Some("x"))),
                    SqlState::CHECK_VIOLATION,
                    "failure named"
                );
                for failing in [
                    ("isolation_pairs", "0"),
                    ("server_version_matches", "false"),
                    ("drill_archiver_attempts", "1"),
                    ("legacy_points_without_vector", "1"),
                    ("rebuild_points", "NULL"),
                    ("repo_intact", "false"),
                    ("repo_intact", "NULL"),
                    ("manifest_matches", "false"),
                    ("witness_b_absent", "false"),
                    ("migrations_drift", "1"),
                    ("rls_unforced", "1"),
                    ("isolation_violations", "1"),
                    ("payload_digest_mismatches", "1"),
                    ("provider_calls", "1"),
                    ("residue", "1"),
                ] {
                    assert_eq!(
                        sqlstate(insert(m, true, Some(failing), None)),
                        SqlState::CHECK_VIOLATION,
                        "succeeded = true with {failing:?}"
                    );
                    insert(m, false, Some(failing), None)
                        .unwrap_or_else(|e| panic!("failed drill {failing:?}: {e}"));
                }
            },
        );
    }
}

// ---- S2 (ADR-0064 D-B, D-D): the worker stores the vector it embedded, reuses it, and purges it with the point.
// Top-level, so the chain's N lines grep `test <name> ... ok`. The a2 fixture runs in its own throwaway database
// (humaux_thread_c37_s2_<pid>_<n>) with a real Qdrant collection and the real projection worker.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use a2_fixture::{Handle, TENANT_SHARED};
use async_trait::async_trait;
use governance_ops::stream;
use humaux_adapters::private_projection_registry::{
    ProjectionPointId, bind_embedding_fingerprint, retire_points_for_memory,
    retire_private_memory_point, worker_fingerprint_inputs,
};
use humaux_adapters::projection_worker::{CardEmbedder, run_once};
use humaux_domain::authority::MemoryId;
use humaux_domain::confirm::DestructiveOp;
use humaux_domain::error::ErrorCode;
use humaux_domain::ids::TenantId;
use humaux_local_secret_scan::SealedRetrievalCard;
use humaux_projection::serving::StreamFamily;
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use sha2::{Digest, Sha256};
use sqlx::types::Uuid;

#[path = "support/a2_fixture.rs"]
mod a2_fixture;
#[path = "support/governance_ops.rs"]
#[allow(dead_code)]
mod governance_ops;

/// The label every a2 fixture deps instance projects under.
const A2_LABEL: &str = "embed-v1";

struct S2Fixture;

impl DbIntegrationFixture for S2Fixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let db = throwaway_db::create("c37_s2")?;
        Handle::in_throwaway(db.dsn(), Box::new(db))
    }
}

/// A provider stand-in that counts its calls and records, per memory, the sha256 of the sealed text it embedded.
/// Its vector is a pure function of that text, so a stored vector can be compared with what it returned.
#[derive(Default)]
struct CountingEmbedder {
    calls: AtomicUsize,
    seen: Mutex<HashMap<Uuid, [u8; 32]>>,
}

fn vector_of(sha: &[u8; 32], dimension: u32) -> Vec<f32> {
    sha.iter()
        .take(dimension as usize)
        .map(|b| f32::from(*b) + 1.0)
        .collect()
}

#[async_trait]
impl CardEmbedder for CountingEmbedder {
    async fn embed_cards(
        &self,
        _tenant_id: TenantId,
        dimension: u32,
        cards: &[SealedRetrievalCard],
        memory_ids: &[Uuid],
    ) -> Result<Vec<Vec<f32>>, ErrorCode> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut seen = self.seen.lock().expect("seen");
        Ok(cards
            .iter()
            .zip(memory_ids)
            .map(|(card, memory)| {
                let sha: [u8; 32] = Sha256::digest(card.as_str().as_bytes()).into();
                seen.insert(*memory, sha);
                vector_of(&sha, dimension)
            })
            .collect())
    }
}

/// Binds the fixture label to a test model fingerprint, as the retrieval worker does at boot.
fn bind(h: &Handle) -> [u8; 32] {
    h.rt.block_on(bind_embedding_fingerprint(
        &h.retrieval,
        A2_LABEL,
        &worker_fingerprint_inputs("c37-test", "c37-model", "r1", 4, "v1"),
    ))
    .expect("label binding")
    .0
}

/// The real worker on `ws` with `embedder` until nothing is ISSUED; no ticket may fail.
fn drain(h: &Handle, ws: Uuid, embedder: &Arc<CountingEmbedder>) {
    let mut deps = h.deps(ws, h.transport.clone());
    deps.embedder = embedder.clone();
    for _ in 0..20 {
        // dep: Qdrant(*) — the worker upserts into the fixture's throwaway collection
        let outcome = h.rt.block_on(run_once(&deps, 50)).expect("run_once");
        assert_eq!(outcome.failed, 0, "{outcome:?}");
        if outcome.done + outcome.skipped_by_policy + outcome.retried == 0 {
            return;
        }
    }
    panic!("the stream of {ws} did not drain in 20 passes");
}

/// `(memory_id, fingerprint, input_sha256, vector)` of every live registry row of the fixture tenant.
type LiveRow = (Uuid, Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<f32>>);

fn live_rows(h: &mut Handle) -> Vec<LiveRow> {
    h.admin
        .query(
            "SELECT p.memory_id, p.fingerprint_sha256, p.input_sha256, v.vector \
               FROM projection.private_memory_points p \
               LEFT JOIN projection.memory_vectors v \
                 USING (tenant_id, memory_id, fingerprint_sha256, input_sha256) \
              WHERE p.tenant_id = $1 AND p.projection_live ORDER BY p.memory_id",
            &[&h.tenant_id],
        )
        .expect("live registry rows")
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect()
}

/// `(vector IS NOT NULL, purged_at IS NOT NULL)` of every vector row of `memory`.
fn vector_state(h: &mut Handle, memory: Uuid) -> Vec<(bool, bool)> {
    h.admin
        .query(
            "SELECT vector IS NOT NULL, purged_at IS NOT NULL FROM projection.memory_vectors \
              WHERE tenant_id = $1 AND memory_id = $2",
            &[&h.tenant_id, &memory],
        )
        .expect("vector rows")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// T-B1 (ADR-0064 D-B): after a worker pass every live registry row carries the worker label's fingerprint, the
/// sha256 of the sealed text the provider embedded, and that exact vector. Fault: delete the vector upsert from
/// `register_private_memory_point_with_vector` → the registry insert fails 23503 → the ticket is not DONE → red.
#[test]
fn a_registered_point_always_has_its_vector() {
    run_db_fixture::<S2Fixture, _>("a_registered_point_always_has_its_vector", |mut h| {
        let fingerprint = bind(&h);
        let ws = h.workspace();
        let memories = h.fan_out(ws, "c37 b1", 2);
        let embedder = Arc::new(CountingEmbedder::default());
        drain(&h, ws, &embedder);
        assert_eq!(
            embedder.calls.load(Ordering::SeqCst),
            1,
            "one batch per Evidence"
        );
        let seen = embedder.seen.lock().expect("seen").clone();
        let rows = live_rows(&mut h);
        assert_eq!(rows.len(), memories.len(), "{rows:?}");
        for (memory, fp, input, vector) in rows {
            let sha = seen
                .get(&memory)
                .expect("the provider embedded this memory");
            assert_eq!(
                fp.as_deref(),
                Some(&fingerprint[..]),
                "{memory}: fingerprint"
            );
            assert_eq!(input.as_deref(), Some(&sha[..]), "{memory}: input sha");
            assert_eq!(vector, Some(vector_of(sha, 4)), "{memory}: stored vector");
        }
    });
}

/// T-B2 (ADR-0064 D-B stored-first): a governance re-ticket (archive) of an unchanged memory projects DONE with zero
/// provider calls, because its vector is stored under the same fingerprint and input bytes. Fault: remove the
/// stored-vector lookup → one embed call → red.
#[test]
fn a_reprojection_of_an_unchanged_card_calls_no_provider() {
    run_db_fixture::<S2Fixture, _>(
        "a_reprojection_of_an_unchanged_card_calls_no_provider",
        |mut h| {
            bind(&h);
            let ws = h.workspace();
            let evidence = h.evidence(ws, "c37 b2");
            let memory = h.memory(evidence, "c37 b2", TENANT_SHARED);
            let first = Arc::new(CountingEmbedder::default());
            drain(&h, ws, &first);
            assert_eq!(first.calls.load(Ordering::SeqCst), 1);
            governance_ops::archive(
                &h.rt,
                &h.gateway,
                &h.scope(ws),
                &stream(h.tenant_id, ws),
                memory,
                DestructiveOp::MemoryArchive,
            )
            .expect("archive issues a lifecycle ticket");
            let issued: i64 = h
                .admin
                .query_one(
                    "SELECT count(*) FROM projection.stream_log WHERE tenant_id = $1 AND state = 'ISSUED'",
                    &[&h.tenant_id],
                )
                .expect("issued tickets")
                .get(0);
            assert_eq!(issued, 1, "the archive re-ticketed the memory");
            let second = Arc::new(CountingEmbedder::default());
            drain(&h, ws, &second);
            assert_eq!(
                second.calls.load(Ordering::SeqCst),
                0,
                "a stored vector for the same card bytes must not reach the provider"
            );
            let open: i64 = h
                .admin
                .query_one(
                    "SELECT count(*) FROM projection.stream_log WHERE tenant_id = $1 AND state <> 'DONE'",
                    &[&h.tenant_id],
                )
                .expect("open tickets")
                .get(0);
            assert_eq!(open, 0, "the re-ticket settled DONE from the stored vector");
            assert_eq!(vector_state(&mut h, memory), vec![(true, false)]);
        },
    );
}

/// T-B3 (ADR-0064 D-D): retiring the last live binding of a memory purges its vector bytes by UPDATE (the key row
/// stays), a revival re-stores them, and both retire paths purge; another memory's vector is untouched. Fault:
/// delete the purge UPDATE from `retire_points_for_memory` or from `retire_private_memory_point` → red.
#[test]
fn retiring_the_last_live_point_purges_its_vector_and_revival_restores_it() {
    run_db_fixture::<S2Fixture, _>(
        "retiring_the_last_live_point_purges_its_vector_and_revival_restores_it",
        |mut h| {
            bind(&h);
            let ws = h.workspace();
            let e1 = h.evidence(ws, "c37 b3 one");
            let memory = h.memory(e1, "c37 b3 one", TENANT_SHARED);
            let e2 = h.evidence(ws, "c37 b3 two");
            let other = h.memory(e2, "c37 b3 two", TENANT_SHARED);
            let embedder = Arc::new(CountingEmbedder::default());
            drain(&h, ws, &embedder);
            assert_eq!(vector_state(&mut h, memory), vec![(true, false)]);

            let family = StreamFamily::new(
                TenantId(h.tenant_id),
                "workspace",
                ws,
                "private_memory",
                "PRIVATE_MEMORY",
            );
            let scope = h.scope(ws);
            let retired =
                h.rt.block_on(retire_points_for_memory(
                    &h.retrieval,
                    &scope,
                    &family,
                    "v1",
                    A2_LABEL,
                    MemoryId(memory),
                ))
                .expect("retire_points_for_memory");
            assert_eq!(retired.len(), 1);
            assert_eq!(
                vector_state(&mut h, memory),
                vec![(false, true)],
                "retire_points_for_memory purges the last live binding's vector"
            );
            assert_eq!(vector_state(&mut h, other), vec![(true, false)]);

            // Revival: the memory is still active, so its next ticket registers it again and re-stores the bytes.
            governance_ops::archive(
                &h.rt,
                &h.gateway,
                &scope,
                &stream(h.tenant_id, ws),
                memory,
                DestructiveOp::MemoryArchive,
            )
            .expect("archive re-tickets the memory");
            drain(&h, ws, &embedder);
            assert_eq!(
                vector_state(&mut h, memory),
                vec![(true, false)],
                "a revival re-stores the purged vector"
            );

            let live: Vec<Uuid> = h
                .admin
                .query(
                    "SELECT point_id FROM projection.private_memory_points \
                      WHERE tenant_id = $1 AND memory_id = $2 AND projection_live",
                    &[&h.tenant_id, &memory],
                )
                .expect("live points")
                .iter()
                .map(|r| r.get(0))
                .collect();
            assert_eq!(live.len(), 1, "{live:?}");
            let flipped =
                h.rt.block_on(retire_private_memory_point(
                    &h.retrieval,
                    &scope,
                    &family,
                    "v1",
                    A2_LABEL,
                    ProjectionPointId::new(live[0]),
                ))
                .expect("retire_private_memory_point");
            assert!(flipped);
            assert_eq!(
                vector_state(&mut h, memory),
                vec![(false, true)],
                "retire_private_memory_point purges too"
            );
            assert_eq!(vector_state(&mut h, other), vec![(true, false)]);
        },
    );
}

// ---- S3 (ADR-0064 D-A, D-E, D-F, D-G, D-M): the rebuild as generation g+1, its verifier, the overlay predicate and
// the closed no-provider deps. Top-level like S2. Every test owns a throwaway database AND a scratch Qdrant
// container humaux-c37-qdrant-<pid>-<n> (support/scratch_qdrant.rs), and the S3 tests run one at a time
// (SCRATCH) so at most one scratch Qdrant exists per process.

use std::future::Future;
use std::pin::Pin;
use std::sync::{MutexGuard, PoisonError};

use humaux_adapters::forget_repo;
use humaux_adapters::postgres::RetrievalWorkerDbPool;
use humaux_adapters::projection_worker::{
    PassConfig, SharedProjectionDeps, run_claimed_pass, run_claimed_pass_for_run,
};
use humaux_adapters::provisioning::{self as provisioning_api, QdrantFace};
use humaux_adapters::qdrant::RetrievalFamily;
use humaux_adapters::rebuild::{
    self, ClosedDeps, NoProviderEmbedder, OrphanStep, RebuildDeps, RebuildOptions, Stream,
    StreamOutcome, Verdict, WorkerLabel,
};
use humaux_adapters::retrieve;
use humaux_adapters::stream_repo::{Backoff, ClaimFamily, OnlyRun};
use humaux_domain::egress::ProcessorId;
use humaux_infra_cell::{
    IntraCellHttpTransport, IntraCellMethod, IntraCellRequest, IntraCellResource,
    authorize_cell_access,
};
use serde_json::{Value, json};

#[path = "support/scratch_qdrant.rs"]
mod scratch_qdrant;

use scratch_qdrant::ScratchQdrant;

/// One scratch Qdrant at a time in this process (memory caps, ruling E7).
static SCRATCH: Mutex<()> = Mutex::new(());

/// Fields drop in order: the handle (its pools, then its throwaway database and scratch Qdrant), then the lock.
struct S3 {
    h: Handle,
    port: u16,
    _serial: MutexGuard<'static, ()>,
}

struct S3Fixture;

impl DbIntegrationFixture for S3Fixture {
    type Handle = S3;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let serial = SCRATCH.lock().unwrap_or_else(PoisonError::into_inner);
        let qdrant =
            ScratchQdrant::start("qdrant").map_err(DbFixtureSkipReason::IsolationSetupFailed)?;
        let port = qdrant.port;
        let db = throwaway_db::create("c37_s3")?;
        let dsn = db.dsn();
        // (qdrant, db): the container goes first, the database last.
        let h = Handle::in_throwaway_at(dsn, Box::new((qdrant, db)), port)?;
        Ok(S3 {
            h,
            port,
            _serial: serial,
        })
    }
}

/// The tenant's §17.3 placement row (the claim joins it) and the label binding of the retrieval worker's boot.
fn prepare(h: &mut Handle) -> [u8; 32] {
    h.admin
        .execute(
            "INSERT INTO projection.tenant_placements \
               (tenant_id, projection_family, collection_name, placement_class) \
             VALUES ($1, 'private_memory_v1', $2, 'SHARED_FALLBACK')",
            &[&h.tenant_id, &h.collection],
        )
        .expect("placement row");
    bind(h)
}

/// Settles every open EVIDENCE_ACCEPTED row of the tenant DONE, as the distill worker would once it has written
/// the memories (the fixture runs no distill worker; the claim skips an Evidence still being distilled).
fn settle_distill(h: &mut Handle) {
    h.admin
        .execute(
            "UPDATE ops.outbox SET status = 'DONE' WHERE tenant_id = $1 \
               AND event_type = 'EVIDENCE_ACCEPTED' AND status IN ('PENDING', 'PROCESSING')",
            &[&h.tenant_id],
        )
        .expect("distill settled");
}

/// Marks `ws`'s stream serving (D-E: a rebuild walks the serving checkpoint rows) and settles distill.
fn serve(h: &mut Handle, ws: Uuid) {
    settle_distill(h);
    let n = h
        .admin
        .execute(
            "UPDATE projection.stream_checkpoints SET serving = true WHERE tenant_id = $1 AND scope_id = $2",
            &[&h.tenant_id, &ws],
        )
        .expect("serving flag");
    assert_eq!(n, 1, "one checkpoint row for {ws}");
}

fn face(port: u16) -> QdrantFace {
    QdrantFace::new("127.0.0.1", port, "127.0.0.1/32").expect("scratch Qdrant face")
}

/// Owned pools for the rebuild steps, so a test can keep writing through `h` while it holds them.
struct Rb {
    maintenance: humaux_adapters::postgres::MaintenanceDbPool,
    reader: RetrievalWorkerDbPool,
    face: QdrantFace,
    worker: WorkerLabel,
}

impl Rb {
    fn new(h: &Handle, port: u16) -> Self {
        let sep = if h.dsn.contains('?') { '&' } else { '?' };
        let maintenance = format!("{}{sep}options=-c%20role%3Drole_maintenance", h.dsn);
        Self {
            // dep: PostgreSQL(role_maintenance) — the definer pool of the rebuild steps
            maintenance: h
                .rt
                .block_on(humaux_adapters::postgres::MaintenanceDbPool::connect(
                    &maintenance,
                ))
                .expect("role_maintenance connects"),
            reader: retrieval_pool(h),
            face: face(port),
            worker: worker(h),
        }
    }

    fn deps(&self) -> RebuildDeps<'_> {
        RebuildDeps {
            maintenance: &self.maintenance,
            reader: &self.reader,
            qdrant: &self.face,
            worker: &self.worker,
        }
    }
}

fn worker(h: &Handle) -> WorkerLabel {
    h.rt.block_on(rebuild::worker_label(&h.maintenance, A2_LABEL))
        .expect("label bound")
}

fn stream_of(h: &Handle, ws: Uuid) -> Stream {
    Stream {
        key: stream(h.tenant_id, ws),
        collection: h.collection.clone(),
    }
}

fn retrieval_pool(h: &Handle) -> RetrievalWorkerDbPool {
    let sep = if h.dsn.contains('?') { '&' } else { '?' };
    let dsn = format!("{}{sep}options=-c%20role%3Drole_retrieval_worker", h.dsn);
    // dep: PostgreSQL(role_retrieval_worker) — the pass's own pool
    h.rt.block_on(RetrievalWorkerDbPool::connect(&dsn))
        .expect("role_retrieval_worker connects")
}

fn mint(h: &Handle) -> Arc<dyn Fn() -> Option<humaux_infra_cell::CellAccessPermit> + Send + Sync> {
    let registry = h.registry.clone();
    Arc::new(move || {
        authorize_cell_access(
            &registry,
            IntraCellResource::QDRANT_REST,
            std::time::Duration::from_secs(300),
        )
        .ok()
    })
}

fn sleeper()
-> Arc<dyn Fn(std::time::Duration) -> humaux_adapters::projection_worker::Sleep + Send + Sync> {
    Arc::new(|period| Box::pin(tokio::time::sleep(period)))
}

/// D-M: the closed deps (NoProviderEmbedder, no embedder parameter) the drill and T-E1/T-E9 project with.
fn closed(h: &Handle) -> (SharedProjectionDeps, Arc<NoProviderEmbedder>) {
    rebuild::drill_projection_deps(ClosedDeps {
        pool: retrieval_pool(h),
        scanner: h.scanner.clone(),
        transport: h.transport.clone(),
        mint_permit: mint(h),
        embedding_version: A2_LABEL.to_owned(),
        dimension: 4,
        processor_id: ProcessorId(Uuid::from_u128(0x0c37_0003)),
        sleep: sleeper(),
    })
}

/// The same shared deps with a counting provider stand-in (T-E8's consented re-embed).
fn counting(h: &Handle, embedder: Arc<CountingEmbedder>) -> SharedProjectionDeps {
    let (mut shared, _) = closed(h);
    shared.embedder = embedder;
    shared
}

fn pass_cfg() -> PassConfig {
    PassConfig {
        claim: ClaimFamily::of(RetrievalFamily::PrivateMemoryV1).expect("ticket family"),
        lease_owner: format!("c37-s3/{}", Uuid::now_v7()),
        lease_secs: 60.0,
        batch: 50,
        per_tenant_cap: 50,
        max_attempts: 3,
        backoff: Backoff {
            base_secs: 1.0,
            max_secs: 2.0,
        },
    }
}

type PumpFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// The in-process projector the rebuild waits on: one resident-path pass, then a short pause.
fn pump<'a>(
    shared: &'a SharedProjectionDeps,
    cfg: &'a PassConfig,
) -> impl Fn() -> PumpFuture<'a> + Sync + 'a {
    move || {
        Box::pin(async move {
            run_claimed_pass(shared, cfg).await.expect("pass");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        })
    }
}

/// One `projection rebuild` of `stream`, pumped by `shared`.
fn rebuild_with(
    h: &Handle,
    port: u16,
    stream: &Stream,
    shared: &SharedProjectionDeps,
    allow_reembed: Option<u64>,
) -> StreamOutcome {
    let rb = Rb::new(h, port);
    let deps = rb.deps();
    let cfg = pass_cfg();
    let pump = pump(shared, &cfg);
    let opts = RebuildOptions {
        batch: 10,
        wait: std::time::Duration::from_secs(20),
        allow_reembed,
        require_stored_vector: false,
        pump: &pump,
    };
    h.rt.block_on(rebuild::rebuild_stream(&deps, stream, &opts))
        .expect("rebuild_stream")
}

/// The worker on `ws` until nothing is ISSUED, failures allowed (a card_unbuildable / distill_failed g1 ticket).
fn drain_any(h: &Handle, ws: Uuid, embedder: &Arc<CountingEmbedder>) {
    let mut deps = h.deps(ws, h.transport.clone());
    deps.embedder = embedder.clone();
    for _ in 0..20 {
        // dep: Qdrant(*) — the worker upserts into the scratch collection
        let outcome = h.rt.block_on(run_once(&deps, 50)).expect("run_once");
        if outcome.done + outcome.skipped_by_policy + outcome.retried + outcome.failed == 0 {
            return;
        }
    }
    panic!("the stream of {ws} did not drain in 20 passes");
}

/// One raw Qdrant call on the scratch collection.
fn qdrant_call(h: &Handle, method: IntraCellMethod, path: String, body: Option<Value>) -> Value {
    let permit = h.permit();
    // dep: Qdrant(*) — test-side tamper / teardown of the scratch collection
    let response =
        h.rt.block_on(h.transport.execute(
            &permit,
            IntraCellRequest {
                method,
                path,
                json_body: body,
                headers: Vec::new(),
            },
        ))
        .expect("qdrant call");
    assert!(
        (200..300).contains(&response.status),
        "{}: {:?}",
        response.status,
        response.json_body
    );
    response.json_body.unwrap_or(Value::Null)
}

fn drop_collection(h: &Handle) {
    qdrant_call(
        h,
        IntraCellMethod::Delete,
        format!("/collections/{}", h.collection),
        None,
    );
}

/// Upserts one point (id, vector, payload) into the scratch collection.
fn put_point(h: &Handle, id: Uuid, vector: &[f32], payload: &serde_json::Map<String, Value>) {
    qdrant_call(
        h,
        IntraCellMethod::Put,
        format!("/collections/{}/points?wait=true", h.collection),
        Some(json!({ "points": [{ "id": id.to_string(), "vector": vector, "payload": payload }] })),
    );
}

fn scrolled(h: &Handle, port: u16, stream: &Stream) -> rebuild::Scrolled {
    let rb = Rb::new(h, port);
    let deps = rb.deps();
    h.rt.block_on(rebuild::scroll_stream(&deps, stream))
        .expect("scroll")
}

fn verify(h: &Handle, port: u16, stream: &Stream, run: Option<Uuid>) -> rebuild::Report {
    let rb = Rb::new(h, port);
    let deps = rb.deps();
    h.rt.block_on(rebuild::verify_stream(&deps, stream, run, false))
        .expect("verify")
}

fn count(
    admin: &mut postgres::Client,
    sql: &str,
    args: &[&(dyn postgres::types::ToSql + Sync)],
) -> i64 {
    admin.query_one(sql, args).expect(sql).get(0)
}

fn rebuild_rows(h: &mut Handle) -> (i64, i64) {
    let t = h.tenant_id;
    (
        count(
            &mut h.admin,
            "SELECT count(*) FROM projection.rebuild_runs WHERE tenant_id = $1",
            &[&t],
        ),
        count(
            &mut h.admin,
            "SELECT count(*) FROM projection.rebuild_tickets WHERE tenant_id = $1",
            &[&t],
        ),
    )
}

/// The commit_seqs of `evidence`'s outbox rows.
fn commits_of(h: &mut Handle, evidence: Uuid) -> Vec<i64> {
    h.admin
        .query(
            "SELECT commit_seq FROM ops.outbox WHERE evidence_id = $1 ORDER BY commit_seq",
            &[&evidence],
        )
        .expect("outbox rows")
        .iter()
        .map(|r| r.get(0))
        .collect()
}

/// Generation tickets of `evidence` (rebuild_tickets rows on its home commits).
fn generation_tickets_of(h: &mut Handle, evidence: Uuid) -> Vec<(i64, String)> {
    h.admin
        .query(
            "SELECT rt.stream_seq, sl.state FROM projection.rebuild_tickets rt \
               JOIN projection.stream_log sl USING (tenant_id, scope_kind, scope_id, domain, projection_kind, \
                                                    projection_version, stream_seq) \
              WHERE rt.commit_seq IN (SELECT commit_seq FROM ops.outbox WHERE evidence_id = $1)",
            &[&evidence],
        )
        .expect("generation tickets")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// Tombstones every stream_log row of `evidence` as role_maintenance (forget_repo::tombstone, §37 step 1).
fn forget(h: &mut Handle, ws: Uuid, evidence: Uuid) {
    let seqs: Vec<i64> = h
        .admin
        .query(
            "SELECT sl.stream_seq FROM projection.stream_log sl JOIN ops.outbox o \
                 ON o.tenant_id = sl.tenant_id AND o.commit_seq = sl.commit_seq \
              WHERE o.evidence_id = $1 AND NOT EXISTS (SELECT 1 FROM projection.rebuild_tickets rt \
                     WHERE rt.tenant_id = sl.tenant_id AND rt.scope_id = sl.scope_id \
                       AND rt.stream_seq = sl.stream_seq)",
            &[&evidence],
        )
        .expect("stream rows of the evidence")
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert!(!seqs.is_empty());
    for seq in seqs {
        h.rt.block_on(forget_repo::tombstone(
            &h.maintenance,
            &stream(h.tenant_id, ws),
            seq as u64,
        ))
        .expect("tombstone");
    }
}

/// T-E1 (ADR-0064 D-A, D-B, D-E, D-F): two tenants are projected, both collections are deleted, and each stream
/// is rebuilt by generation tickets projected through `run_claimed_pass` with the closed NoProviderEmbedder:
/// `equivalent`, points = live registry rows, zero provider attempts, completeness below 1 while the generation is
/// pending and closed after. Fault: the in-process projector without the stored-vector lookup → the
/// NoProviderEmbedder is called (> 0), its tickets never settle → red.
#[test]
fn a_rebuild_into_an_empty_collection_is_equivalent_without_a_provider_call() {
    run_db_fixture::<S3Fixture, _>(
        "a_rebuild_into_an_empty_collection_is_equivalent_without_a_provider_call",
        |mut s| {
            let port = s.port;
            let mut b = Handle::in_throwaway_at(s.h.dsn.clone(), Box::new(()), port)
                .expect("second tenant");
            let embedder = Arc::new(CountingEmbedder::default());
            let mut workspaces = Vec::new();
            for h in [&mut s.h, &mut b] {
                prepare(h);
                let ws = h.workspace();
                h.fan_out(ws, "c37 e1 fan", 2);
                let e = h.evidence(ws, "c37 e1 single");
                h.memory(e, "c37 e1 single", TENANT_SHARED);
                drain(h, ws, &embedder);
                serve(h, ws);
                workspaces.push(ws);
            }
            let provider_calls = embedder.calls.load(Ordering::SeqCst);
            drop_collection(&s.h);
            drop_collection(&b);

            // Tenant A: the D-E steps one by one, with the completeness reading between issue and close.
            let (ws, h) = (workspaces[0], &mut s.h);
            let stream_a = stream_of(h, ws);
            let live = count(
                &mut h.admin,
                "SELECT count(*) FROM projection.private_memory_points WHERE tenant_id = $1 AND projection_live",
                &[&h.tenant_id],
            );
            let (shared, no_provider) = closed(h);
            let cfg = pass_cfg();
            let rb = Rb::new(h, port);
            let deps = rb.deps();
            assert_eq!(
                h.rt.block_on(rebuild::precheck(&deps, &stream_a, None))
                    .expect("precheck"),
                Ok(0)
            );
            let run =
                h.rt.block_on(rebuild::open_run(&deps, &stream_a))
                    .expect("open");
            assert_eq!(run.generation, 2);
            h.rt.block_on(provisioning_api::ensure_collection(
                &rb.face,
                &h.collection,
                4,
            ))
            .expect("collection recreated");
            let issued =
                h.rt.block_on(rebuild::issue_tickets(&deps, &stream_a, &run, 1, false))
                    .expect("issue");
            assert_eq!(issued, 2, "one generation ticket per input Evidence");
            let pending = h.reading("c37 e1 pending", ws, &h.scope(ws));
            assert!(
                pending.value.points_in_flight > 0,
                "completeness < 1 while the generation is pending"
            );
            let pump_a = pump(&shared, &cfg);
            assert!(
                h.rt.block_on(rebuild::wait_generation(
                    &deps,
                    &stream_a,
                    &run,
                    std::time::Duration::from_secs(20),
                    &pump_a,
                ))
                .expect("wait"),
                "the generation settled; NoProviderEmbedder attempts={}",
                no_provider.attempts()
            );
            let (report, orphans) =
                h.rt.block_on(rebuild::verify_run(&deps, &stream_a, &run, false))
                    .expect("verify");
            assert_eq!(report.verdict, Verdict::Equivalent, "{}", report.json);
            assert_eq!(orphans, 0);
            assert_eq!(report.points, live, "points = live registry rows");
            h.rt.block_on(rebuild::close_run(&deps, &stream_a, &run, &report))
                .expect("close");
            h.assert_closed("c37 e1 closed", ws, &h.scope(ws));
            assert_eq!(
                no_provider.attempts(),
                0,
                "no provider attempt in the rebuild"
            );

            // Tenant B: the one-call orchestration.
            let stream_b = stream_of(&b, workspaces[1]);
            let outcome = rebuild_with(&b, port, &stream_b, &shared, None);
            assert_eq!(outcome.verdict, Verdict::Equivalent, "{}", outcome.receipt);
            assert_eq!(outcome.receipt["closed"], json!(true));
            assert_eq!(outcome.receipt["points"], json!(3));
            assert_eq!(no_provider.attempts(), 0);
            assert_eq!(embedder.calls.load(Ordering::SeqCst), provider_calls);
        },
    );
}

/// T-F1 (ADR-0064 D-F E3/E4/E5): `verify` names the id of a tampered payload field, of a vector moved by 1e-3, and
/// of an extra point with a foreign id; then `rebuild` deletes the extra point and closes `equivalent`. Fault: skip
/// E4 or E5 → that case reads `equivalent` → red.
#[test]
fn a_tampered_payload_vector_or_extra_point_is_not_equivalent() {
    run_db_fixture::<S3Fixture, _>(
        "a_tampered_payload_vector_or_extra_point_is_not_equivalent",
        |mut s| {
            let port = s.port;
            let h = &mut s.h;
            prepare(h);
            let ws = h.workspace();
            h.fan_out(ws, "c37 f1", 2);
            drain(h, ws, &Arc::new(CountingEmbedder::default()));
            serve(h, ws);
            let stream = stream_of(h, ws);
            assert_eq!(verify(h, port, &stream, None).verdict, Verdict::Equivalent);
            let points = scrolled(h, port, &stream);
            let (&id, point) = points.label.iter().next().expect("a point");
            let vector = point.vector.clone().expect("vector");

            // (a) one payload field overwritten
            let original = point.payload["status"].clone();
            let set_status = |status: &Value| {
                qdrant_call(
                    h,
                    IntraCellMethod::Post,
                    format!("/collections/{}/points/payload?wait=true", h.collection),
                    Some(json!({ "payload": { "status": status }, "points": [id.to_string()] })),
                );
            };
            set_status(&json!("superseded"));
            let report = verify(h, port, &stream, None);
            assert_eq!(report.verdict, Verdict::NotEquivalent, "{}", report.json);
            assert_eq!(
                report.json["differing_ids"]["payload"],
                json!([id]),
                "{}",
                report.json
            );
            set_status(&original);

            // (b) one vector perturbed by 1e-3
            let mut moved = vector.clone();
            moved[0] += 1e-3;
            put_point(h, id, &moved, &point.payload);
            let report = verify(h, port, &stream, None);
            assert_eq!(report.verdict, Verdict::NotEquivalent, "{}", report.json);
            assert_eq!(
                report.json["differing_ids"]["vector"],
                json!([id]),
                "{}",
                report.json
            );
            put_point(h, id, &vector, &point.payload);
            assert_eq!(verify(h, port, &stream, None).verdict, Verdict::Equivalent);

            // (c) one extra point with a foreign id
            let foreign = Uuid::new_v4();
            put_point(h, foreign, &vector, &point.payload);
            let report = verify(h, port, &stream, None);
            assert_eq!(report.verdict, Verdict::NotEquivalent, "{}", report.json);
            assert_eq!(
                report.json["differing_ids"]["qdrant_not_registered"],
                json!([foreign]),
                "{}",
                report.json
            );
            let (shared, _) = closed(h);
            let outcome = rebuild_with(h, port, &stream, &shared, None);
            assert_eq!(outcome.verdict, Verdict::Equivalent, "{}", outcome.receipt);
            assert_eq!(outcome.receipt["orphans_deleted"], json!(1));
            assert!(!scrolled(h, port, &stream).label.contains_key(&foreign));
        },
    );
}

/// T-F2 (ADR-0064 D-G, card fault): registry rows of the label rewritten to a second fingerprint F2 (the tamper
/// case) are refused `re_embed_required` (exit 3): no rebuild_runs row, no ticket, no provider attempt. Fault: skip
/// the precheck → a run opens → red.
#[test]
fn a_changed_fingerprint_is_refused_as_re_embed_required() {
    run_db_fixture::<S3Fixture, _>(
        "a_changed_fingerprint_is_refused_as_re_embed_required",
        |mut s| {
            let port = s.port;
            let h = &mut s.h;
            prepare(h);
            let ws = h.workspace();
            h.fan_out(ws, "c37 f2", 2);
            drain(h, ws, &Arc::new(CountingEmbedder::default()));
            serve(h, ws);
            let f2 = humaux_projection::embedding_fingerprint::EmbeddingFingerprint::compute(
                &worker_fingerprint_inputs("c37-test", "c37-model", "r2-tamper", 4, "v1"),
            )
            .expect("F2")
            .0
            .to_vec();
            h.admin.batch_execute("SELECT 1").expect("admin alive");
            let mut txn = h.admin.transaction().expect("tamper txn");
            txn.execute(
                "INSERT INTO projection.embedding_fingerprints (fingerprint_sha256, embedding_version, provider, \
                   model_id, model_revision, dimension, task_type, preprocessing_version, \
                   projection_contract_version, dtype, normalization, distance) \
                 SELECT $1, embedding_version || '-tamper', provider, model_id, 'r2-tamper', dimension, task_type, \
                        preprocessing_version, projection_contract_version, dtype, normalization, distance \
                   FROM projection.embedding_fingerprints WHERE embedding_version = $2",
                &[&f2, &A2_LABEL],
            )
            .expect("F2 row");
            txn.execute(
                "INSERT INTO projection.memory_vectors (tenant_id, memory_id, fingerprint_sha256, input_sha256, \
                   dimension, vector) \
                 SELECT v.tenant_id, v.memory_id, $1, v.input_sha256, v.dimension, v.vector \
                   FROM projection.memory_vectors v WHERE v.tenant_id = $2",
                &[&f2, &h.tenant_id],
            )
            .expect("F2 vectors");
            let rewritten = txn
                .execute(
                    "UPDATE projection.private_memory_points SET fingerprint_sha256 = $1 \
                      WHERE tenant_id = $2 AND projection_live",
                    &[&f2, &h.tenant_id],
                )
                .expect("rewrite the registry rows");
            txn.commit().expect("tamper commit");
            assert_eq!(rewritten, 2);
            let (shared, no_provider) = closed(h);
            let outcome = rebuild_with(h, port, &stream_of(h, ws), &shared, Some(1_000));
            assert!(outcome.refused, "{}", outcome.receipt);
            assert_eq!(outcome.verdict, Verdict::ReEmbedRequired);
            assert_eq!(
                outcome.receipt["points_other_fingerprint"],
                json!(2),
                "{}",
                outcome.receipt
            );
            assert_eq!(rebuild_rows(h), (0, 0), "nothing opened or issued");
            assert_eq!(no_provider.attempts(), 0);
        },
    );
}

/// T-F3 (ADR-0064 D-F): `verify` writes nothing — a stream with an orphan reads `not_equivalent` and afterwards the
/// exact Qdrant count and a digest of every PG table the rebuild touches are unchanged. Fault: let verify delete
/// orphans → the count drops → red.
#[test]
fn verify_is_read_only() {
    run_db_fixture::<S3Fixture, _>("verify_is_read_only", |mut s| {
        let port = s.port;
        let h = &mut s.h;
        prepare(h);
        let ws = h.workspace();
        h.fan_out(ws, "c37 f3", 2);
        drain(h, ws, &Arc::new(CountingEmbedder::default()));
        serve(h, ws);
        let stream = stream_of(h, ws);
        let points = scrolled(h, port, &stream);
        let (_, point) = points.label.iter().next().expect("a point");
        put_point(
            h,
            Uuid::new_v4(),
            point.vector.as_ref().expect("vector"),
            &point.payload,
        );
        let digest = |h: &mut Handle| -> String {
            h.admin
                .query_one(
                    "SELECT md5(concat_ws('|', \
                       (SELECT string_agg(x::text, ',' ORDER BY x::text) FROM projection.stream_log x WHERE tenant_id = $1), \
                       (SELECT string_agg(x::text, ',' ORDER BY x::text) FROM projection.stream_checkpoints x WHERE tenant_id = $1), \
                       (SELECT string_agg(x::text, ',' ORDER BY x::text) FROM projection.rebuild_runs x WHERE tenant_id = $1), \
                       (SELECT string_agg(x::text, ',' ORDER BY x::text) FROM projection.rebuild_tickets x WHERE tenant_id = $1), \
                       (SELECT string_agg(x::text, ',' ORDER BY x::text) FROM projection.private_memory_points x WHERE tenant_id = $1), \
                       (SELECT string_agg(x::text, ',' ORDER BY x::text) FROM projection.memory_vectors x WHERE tenant_id = $1)))",
                    &[&h.tenant_id],
                )
                .expect("digest")
                .get(0)
        };
        let before = (scrolled(h, port, &stream).count_exact, digest(h));
        let report = verify(h, port, &stream, None);
        assert_eq!(report.verdict, Verdict::NotEquivalent, "{}", report.json);
        assert_eq!(report.orphans.len(), 1);
        let after = (scrolled(h, port, &stream).count_exact, digest(h));
        assert_eq!(before, after, "verify wrote nothing");
    });
}

/// T-E2 (ADR-0064 D-F E2): an Evidence whose card is unbuildable fails its generation ticket `card_unbuildable`;
/// that is an exclusion by outcome (its buildable sibling memory is in X and absent from R), not a mismatch: the
/// run closes `equivalent` with `excluded_by_outcome.card_unbuildable = 1`. Fault: count deterministic exclusions as
/// missing → `not_equivalent` → red.
#[test]
fn a_generation_ticket_failing_card_unbuildable_is_an_exclusion_not_a_mismatch() {
    run_db_fixture::<S3Fixture, _>(
        "a_generation_ticket_failing_card_unbuildable_is_an_exclusion_not_a_mismatch",
        |mut s| {
            let port = s.port;
            let h = &mut s.h;
            prepare(h);
            let ws = h.workspace();
            h.fan_out(ws, "c37 e2 fine", 1);
            let bad = h.evidence(ws, "c37 e2 bad");
            h.memory(bad, "c37 e2 buildable sibling", TENANT_SHARED);
            h.admin
                .execute(
                    "WITH m AS (INSERT INTO private.memory_records (tenant_id, memory_type, content, \
                                  visibility_class, authority_class, confidence, status, asserted_at) \
                                VALUES ($1, 'NOTE', '{\"title\":\"no claim, no excerpt\"}', 'TENANT_SHARED', \
                                  'PrivateKnowledge', 0.9, 'active', now()) RETURNING memory_id) \
                     INSERT INTO private.memory_evidence (memory_id, evidence_id, role, ordinal) \
                     SELECT memory_id, $2, 'SUPPORTING', 1 FROM m",
                    &[&h.tenant_id, &bad],
                )
                .expect("an unbuildable memory");
            drain_any(h, ws, &Arc::new(CountingEmbedder::default()));
            serve(h, ws);
            let (shared, _) = closed(h);
            let outcome = rebuild_with(h, port, &stream_of(h, ws), &shared, None);
            assert_eq!(outcome.verdict, Verdict::Equivalent, "{}", outcome.receipt);
            assert_eq!(
                outcome.receipt["excluded_by_outcome"]["card_unbuildable"],
                json!(1),
                "{}",
                outcome.receipt
            );
        },
    );
}

/// T-E3 (ADR-0064 D-F E2, finding 17): an Evidence whose EVIDENCE_ACCEPTED row FAILED and one whose
/// EVIDENCE_ACCEPTED row is gone close `equivalent` with `excluded_by_outcome: distill_failed=1,
/// no_visible_memory_record=1`; a generation ticket forced to `transient_exhausted` fails E2. Fault: E2 accepting
/// only card_unbuildable / secret_scan_rejected → red.
#[test]
fn a_failed_distill_evidence_still_closes_equivalent() {
    run_db_fixture::<S3Fixture, _>(
        "a_failed_distill_evidence_still_closes_equivalent",
        |mut s| {
            let port = s.port;
            let h = &mut s.h;
            prepare(h);
            let ws = h.workspace();
            h.fan_out(ws, "c37 e3 fine", 1);
            let failed = h.evidence(ws, "c37 e3 distill failed");
            h.admin
                .execute(
                    "UPDATE ops.outbox SET status = 'FAILED' \
                  WHERE evidence_id = $1 AND event_type = 'EVIDENCE_ACCEPTED'",
                    &[&failed],
                )
                .expect("distill failed");
            let gone = evidence_without_accepted_row(h, ws);
            assert_eq!(commits_of(h, gone).len(), 1);
            drain_any(h, ws, &Arc::new(CountingEmbedder::default()));
            serve(h, ws);
            let stream = stream_of(h, ws);
            let (shared, _) = closed(h);
            let outcome = rebuild_with(h, port, &stream, &shared, None);
            assert_eq!(outcome.verdict, Verdict::Equivalent, "{}", outcome.receipt);
            assert_eq!(
                outcome.receipt["excluded_by_outcome"],
                json!({ "distill_failed": 1, "no_visible_memory_record": 1 }),
                "{}",
                outcome.receipt
            );

            // A generation ticket forced to transient_exhausted (as the retrieval worker, ISSUED -> FAILED) fails E2.
            let rb = Rb::new(h, port);
            let deps = rb.deps();
            let run =
                h.rt.block_on(rebuild::open_run(&deps, &stream))
                    .expect("open g3");
            h.rt.block_on(rebuild::issue_tickets(&deps, &stream, &run, 10, false))
                .expect("issue g3");
            let forced = force_transient_exhausted(h, run.run_id);
            assert_eq!(forced, 1);
            let cfg = pass_cfg();
            let p = pump(&shared, &cfg);
            assert!(
                h.rt.block_on(rebuild::wait_generation(
                    &deps,
                    &stream,
                    &run,
                    std::time::Duration::from_secs(20),
                    &p,
                ))
                .expect("wait")
            );
            let report = verify(h, port, &stream, Some(run.run_id));
            assert_eq!(report.verdict, Verdict::NotEquivalent, "{}", report.json);
            assert!(
                report.json["reasons"]
                    .as_array()
                    .expect("reasons")
                    .contains(&json!("rebuild_tickets_failed:transient_exhausted=1")),
                "{}",
                report.json
            );
        },
    );
}

/// T-E4 (ADR-0064 D-A/D-E, P0, finding 14): a tombstoned Evidence gets no generation ticket (`excluded.tombstoned
/// = 1`), never shows in the RYW overlay, and no point or vector of its memory is revived; a forget while a run is
/// open tombstones that Evidence's generation ticket (the follow trigger). Faults: drop the tombstone predicate from
/// `issue_rebuild_tickets`; drop the follow trigger.
#[test]
fn a_tombstoned_evidence_gets_no_generation_ticket_and_no_overlay() {
    run_db_fixture::<S3Fixture, _>(
        "a_tombstoned_evidence_gets_no_generation_ticket_and_no_overlay",
        |mut s| {
            let port = s.port;
            let h = &mut s.h;
            prepare(h);
            let ws = h.workspace();
            let gone = h.evidence(ws, "c37 e4 forgotten");
            let gone_memory = h.memory(gone, "c37 e4 forgotten", TENANT_SHARED);
            let kept = h.evidence(ws, "c37 e4 kept");
            h.memory(kept, "c37 e4 kept", TENANT_SHARED);
            let later = h.evidence(ws, "c37 e4 forgotten during the run");
            h.memory(later, "c37 e4 forgotten during the run", TENANT_SHARED);
            drain(h, ws, &Arc::new(CountingEmbedder::default()));
            serve(h, ws);
            forget(h, ws, gone);
            let registry_before = count(
                &mut h.admin,
                "SELECT count(*) FROM projection.private_memory_points WHERE memory_id = $1 AND projection_live",
                &[&gone_memory],
            );
            let vectors_before = vector_state(h, gone_memory);

            let stream = stream_of(h, ws);
            let rb = Rb::new(h, port);
            let deps = rb.deps();
            let run =
                h.rt.block_on(rebuild::open_run(&deps, &stream))
                    .expect("open");
            h.rt.block_on(rebuild::issue_tickets(&deps, &stream, &run, 10, false))
                .expect("issue");
            assert!(
                generation_tickets_of(h, gone).is_empty(),
                "no generation ticket for a tombstoned Evidence"
            );
            let pending = generation_tickets_of(h, later);
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].1, "ISSUED");
            // A write during the run: its token's overlay holds that write only.
            let fresh = h.evidence(ws, "c37 e4 fresh");
            let up_to = count(
                &mut h.admin,
                "SELECT issued_highwater FROM projection.stream_checkpoints WHERE tenant_id = $1 AND scope_id = $2",
                &[&h.tenant_id, &ws],
            );
            let overlay =
                h.rt.block_on(retrieve::pg_delta_overlay(
                    &h.gateway,
                    &h.scope(ws),
                    &stream.key,
                    run.boundary_seq,
                    up_to,
                ))
                .expect("overlay");
            let evidences: Vec<Uuid> = overlay.iter().map(|c| c.evidence_id).collect();
            assert_eq!(evidences, vec![fresh], "{overlay:?}");
            // Forget during the run: the generation ticket follows to TOMBSTONED.
            forget(h, ws, later);
            assert_eq!(generation_tickets_of(h, later)[0].1, "TOMBSTONED");
            settle_distill(h);

            let (shared, _) = closed(h);
            let cfg = pass_cfg();
            let p = pump(&shared, &cfg);
            assert!(
                h.rt.block_on(rebuild::wait_generation(
                    &deps,
                    &stream,
                    &run,
                    std::time::Duration::from_secs(20),
                    &p,
                ))
                .expect("wait")
            );
            h.rt.block_on(run_claimed_pass(&shared, &cfg))
                .expect("catch-up pass");
            let (report, _) =
                h.rt.block_on(rebuild::verify_run(&deps, &stream, &run, false))
                    .expect("verify");
            assert_eq!(report.verdict, Verdict::Equivalent, "{}", report.json);
            assert_eq!(
                report.json["excluded"]["tombstoned"],
                json!(1),
                "{}",
                report.json
            );
            assert!(generation_tickets_of(h, gone).is_empty());
            assert_eq!(
                count(
                    &mut h.admin,
                    "SELECT count(*) FROM projection.private_memory_points WHERE memory_id = $1 AND projection_live",
                    &[&gone_memory],
                ),
                registry_before,
                "no point revived for the forgotten memory"
            );
            assert_eq!(
                vector_state(h, gone_memory),
                vectors_before,
                "no vector revived"
            );
        },
    );
}

/// T-E5 (ADR-0064 D-A, E13): with a run open and its generation tickets pending, the RYW overlay of a token for a
/// new write holds that write only — no generation row double-serves. Fault: drop the overlay predicate in
/// `retrieve::pg_delta_overlay_in_txn` → the pending generation rows join the overlay → red.
#[test]
fn recall_during_an_open_rebuild_serves_no_generation_row_in_the_overlay() {
    run_db_fixture::<S3Fixture, _>(
        "recall_during_an_open_rebuild_serves_no_generation_row_in_the_overlay",
        |mut s| {
            let port = s.port;
            let h = &mut s.h;
            prepare(h);
            let ws = h.workspace();
            h.fan_out(ws, "c37 e5 one", 1);
            h.fan_out(ws, "c37 e5 two", 1);
            drain(h, ws, &Arc::new(CountingEmbedder::default()));
            serve(h, ws);
            let stream = stream_of(h, ws);
            let rb = Rb::new(h, port);
            let deps = rb.deps();
            let run =
                h.rt.block_on(rebuild::open_run(&deps, &stream))
                    .expect("open");
            let issued =
                h.rt.block_on(rebuild::issue_tickets(&deps, &stream, &run, 10, false))
                    .expect("issue");
            assert_eq!(issued, 2);
            let fresh = h.evidence(ws, "c37 e5 fresh");
            let up_to = count(
                &mut h.admin,
                "SELECT issued_highwater FROM projection.stream_checkpoints WHERE tenant_id = $1 AND scope_id = $2",
                &[&h.tenant_id, &ws],
            );
            let overlay =
                h.rt.block_on(retrieve::pg_delta_overlay(
                    &h.gateway,
                    &h.scope(ws),
                    &stream.key,
                    run.boundary_seq,
                    up_to,
                ))
                .expect("overlay");
            let evidences: Vec<Uuid> = overlay.iter().map(|c| c.evidence_id).collect();
            assert_eq!(evidences, vec![fresh], "{overlay:?}");
        },
    );
}

/// T-E6 (ADR-0064 D-F, finding 15): a point upserted by a ticket still in flight (seq ≤ H2, not yet registered) is
/// in the scroll but is not an orphan: the orphan step deletes nothing and the verdict is `cannot_establish:
/// generation_in_flight`; once the ticket settles, a re-run deletes the real orphan and closes `equivalent`. Fault:
/// compute Q \ R with R read before the scroll and no in-flight check → the point is deleted → red.
#[test]
fn orphan_deletion_spares_a_point_registered_after_the_scroll() {
    run_db_fixture::<S3Fixture, _>(
        "orphan_deletion_spares_a_point_registered_after_the_scroll",
        |mut s| {
            let port = s.port;
            let h = &mut s.h;
            prepare(h);
            let ws = h.workspace();
            h.fan_out(ws, "c37 e6", 1);
            let embedder = Arc::new(CountingEmbedder::default());
            drain(h, ws, &embedder);
            serve(h, ws);
            let stream = stream_of(h, ws);
            // The in-flight ticket: a new write, ISSUED, whose worker has upserted p but not registered it yet.
            h.fan_out(ws, "c37 e6 in flight", 1);
            let points = scrolled(h, port, &stream);
            let (_, template) = points.label.iter().next().expect("a point");
            let p = Uuid::new_v4();
            put_point(
                h,
                p,
                template.vector.as_ref().expect("vector"),
                &template.payload,
            );

            let rb = Rb::new(h, port);

            let deps = rb.deps();
            let h2 = count(
                &mut h.admin,
                "SELECT issued_highwater FROM projection.stream_checkpoints WHERE tenant_id = $1 AND scope_id = $2",
                &[&h.tenant_id, &ws],
            );
            let q =
                h.rt.block_on(rebuild::scroll_stream(&deps, &stream))
                    .expect("scroll");
            assert!(q.label.contains_key(&p));
            let step =
                h.rt.block_on(rebuild::delete_orphans(&deps, &stream, h2, &q))
                    .expect("orphan step");
            assert_eq!(step, OrphanStep::InFlight);
            assert!(
                scrolled(h, port, &stream).label.contains_key(&p),
                "p survives"
            );
            let report = verify(h, port, &stream, None);
            assert_eq!(report.verdict, Verdict::CannotEstablish, "{}", report.json);
            assert_eq!(report.json["reasons"][0], json!("generation_in_flight"));

            drain(h, ws, &embedder);
            settle_distill(h);
            let (shared, _) = closed(h);
            let outcome = rebuild_with(h, port, &stream, &shared, None);
            assert_eq!(outcome.verdict, Verdict::Equivalent, "{}", outcome.receipt);
            assert_eq!(outcome.receipt["orphans_deleted"], json!(1));
        },
    );
}

/// T-E7 (ADR-0064 D-E, finding 16): an input whose ticket failed `card_unbuildable` and was retired by the operator
/// stays retired: no generation ticket, `excluded.retired_by_operator = 1`, completeness 1 after the equivalent
/// close. Fault: drop the RETIRED_FAILED exclusion → it gets a generation ticket → red.
#[test]
fn an_operator_retired_input_stays_retired_after_an_equivalent_rebuild() {
    run_db_fixture::<S3Fixture, _>(
        "an_operator_retired_input_stays_retired_after_an_equivalent_rebuild",
        |mut s| {
            let port = s.port;
            let h = &mut s.h;
            prepare(h);
            let ws = h.workspace();
            h.fan_out(ws, "c37 e7 fine", 1);
            let retired = h.evidence(ws, "c37 e7 retired");
            h.admin
                .execute(
                    "WITH m AS (INSERT INTO private.memory_records (tenant_id, memory_type, content, \
                                  visibility_class, authority_class, confidence, status, asserted_at) \
                                VALUES ($1, 'NOTE', '{\"title\":\"no claim, no excerpt\"}', 'TENANT_SHARED', \
                                  'PrivateKnowledge', 0.9, 'active', now()) RETURNING memory_id) \
                     INSERT INTO private.memory_evidence (memory_id, evidence_id, role, ordinal) \
                     SELECT memory_id, $2, 'PRIMARY', 0 FROM m",
                    &[&h.tenant_id, &retired],
                )
                .expect("an unbuildable memory");
            drain_any(h, ws, &Arc::new(CountingEmbedder::default()));
            h.retire(ws, "card_unbuildable");
            serve(h, ws);
            let (shared, _) = closed(h);
            let outcome = rebuild_with(h, port, &stream_of(h, ws), &shared, None);
            assert_eq!(outcome.verdict, Verdict::Equivalent, "{}", outcome.receipt);
            assert_eq!(
                outcome.receipt["excluded"]["retired_by_operator"],
                json!(1),
                "{}",
                outcome.receipt
            );
            assert!(generation_tickets_of(h, retired).is_empty());
            h.assert_closed("c37 e7 closed", ws, &h.scope(ws));
        },
    );
}

/// T-E8 (ADR-0064 D-G, finding 19): registry rows projected before the label was bound (legacy: no fingerprint)
/// are refused without consent (remedy printed); with `--allow-reembed n` the provider is called once per legacy
/// card, the rows gain fingerprint, input hash and vector, and a second rebuild makes no call. Fault: skip the
/// backfill-on-touch → the rows stay legacy → red.
#[test]
fn allow_reembed_backfills_legacy_vectors_under_the_bound_fingerprint() {
    run_db_fixture::<S3Fixture, _>(
        "allow_reembed_backfills_legacy_vectors_under_the_bound_fingerprint",
        |mut s| {
            let port = s.port;
            let h = &mut s.h;
            h.admin
                .execute(
                    "INSERT INTO projection.tenant_placements \
                       (tenant_id, projection_family, collection_name, placement_class) \
                     VALUES ($1, 'private_memory_v1', $2, 'SHARED_FALLBACK')",
                    &[&h.tenant_id, &h.collection],
                )
                .expect("placement row");
            let ws = h.workspace();
            h.fan_out(ws, "c37 e8 a", 1);
            h.fan_out(ws, "c37 e8 b", 1);
            // Projected while the label is unbound: legacy registry rows, no stored vector.
            drain(h, ws, &Arc::new(CountingEmbedder::default()));
            bind(h);
            serve(h, ws);
            let stream = stream_of(h, ws);
            let embedder = Arc::new(CountingEmbedder::default());
            let shared = counting(h, embedder.clone());
            let refused = rebuild_with(h, port, &stream, &shared, None);
            assert!(refused.refused, "{}", refused.receipt);
            assert_eq!(refused.receipt["points_without_vector"], json!(2));
            assert!(
                refused.receipt["run"]
                    .as_str()
                    .is_some_and(|r| r.contains("--allow-reembed 2"))
            );
            assert_eq!(rebuild_rows(h), (0, 0));

            let outcome = rebuild_with(h, port, &stream, &shared, Some(2));
            assert_eq!(outcome.verdict, Verdict::Equivalent, "{}", outcome.receipt);
            assert_eq!(
                embedder.calls.load(Ordering::SeqCst),
                2,
                "one call per legacy card"
            );
            for (_, fp, input, vector) in live_rows(h) {
                assert!(fp.is_some() && input.is_some() && vector.is_some());
            }
            let again = rebuild_with(h, port, &stream, &shared, None);
            assert_eq!(again.verdict, Verdict::Equivalent, "{}", again.receipt);
            assert_eq!(
                embedder.calls.load(Ordering::SeqCst),
                2,
                "a second rebuild calls no provider"
            );
        },
    );
}

/// T-E9 (ADR-0064 D-N(g), finding 18): an ISSUED generation-1 ticket without a vector (restored state) beside a
/// run's generation tickets: the run-scoped pass claims only the generation, and the NoProviderEmbedder counts 0
/// attempts; the g1 ticket stays ISSUED and unclaimed. Fault: ignore `only_run` → the g1 ticket is claimed and the
/// provider is attempted → red.
#[test]
fn drill_scoped_claim_leaves_restored_g1_tickets_alone() {
    run_db_fixture::<S3Fixture, _>(
        "drill_scoped_claim_leaves_restored_g1_tickets_alone",
        |mut s| {
            let port = s.port;
            let h = &mut s.h;
            prepare(h);
            let ws = h.workspace();
            h.fan_out(ws, "c37 e9 projected", 1);
            drain(h, ws, &Arc::new(CountingEmbedder::default()));
            serve(h, ws);
            let stream = stream_of(h, ws);
            let rb = Rb::new(h, port);
            let deps = rb.deps();
            let run =
                h.rt.block_on(rebuild::open_run(&deps, &stream))
                    .expect("open");
            h.rt.block_on(rebuild::issue_tickets(&deps, &stream, &run, 10, true))
                .expect("issue");
            let restored = h.evidence(ws, "c37 e9 restored g1");
            h.memory(restored, "c37 e9 restored g1", TENANT_SHARED);
            settle_distill(h);
            let (shared, no_provider) = closed(h);
            let outcome =
                h.rt.block_on(run_claimed_pass_for_run(
                    &shared,
                    &pass_cfg(),
                    OnlyRun {
                        tenant_id: h.tenant_id,
                        run_id: run.run_id,
                    },
                ))
                .expect("scoped pass");
            assert_eq!(outcome.claimed, 1, "{outcome:?}");
            assert_eq!(outcome.done, 1, "{outcome:?}");
            assert_eq!(no_provider.attempts(), 0);
            let g1: (String, i32) = {
                let row = h
                .admin
                .query_one(
                    "SELECT sl.state, sl.attempts FROM projection.stream_log sl JOIN ops.outbox o \
                       ON o.tenant_id = sl.tenant_id AND o.commit_seq = sl.commit_seq WHERE o.evidence_id = $1",
                    &[&restored],
                )
                .expect("restored g1 ticket");
                (row.get(0), row.get(1))
            };
            assert_eq!(
                g1,
                ("ISSUED".to_owned(), 0),
                "the restored g1 ticket was never claimed"
            );
        },
    );
}

/// Contract (ADR-0064 D-E, §78.2): `rebuild::Verdict` is exactly the closed set of `projection.rebuild_runs`'
/// verdict CHECK. Fault: a Rust variant or a CHECK value the other side lacks → red.
#[test]
fn rebuild_verdicts_match_the_runs_check() {
    run_db_fixture::<S1Contract, _>("rebuild_verdicts_match_the_runs_check", |mut c| {
        let def: String = c
            .client
            .query_one(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
                  WHERE conrelid = 'projection.rebuild_runs'::regclass AND contype = 'c' \
                    AND pg_get_constraintdef(oid) LIKE '%verdict%' \
                    AND pg_get_constraintdef(oid) LIKE '%equivalent%'",
                &[],
            )
            .expect("verdict CHECK")
            .get(0);
        let in_db: std::collections::BTreeSet<String> = def
            .split('\'')
            .skip(1)
            .step_by(2)
            .map(str::to_owned)
            .collect();
        let in_rust: std::collections::BTreeSet<String> = Verdict::ALL
            .iter()
            .map(|v| v.as_db_str().to_owned())
            .collect();
        assert_eq!(in_db, in_rust, "{def}");
    });
}

/// A bare migrated throwaway database (no Qdrant), for the contract test.
struct ContractDb {
    client: postgres::Client,
    _db: throwaway_db::ThrowawayDb,
}

struct S1Contract;

impl DbIntegrationFixture for S1Contract {
    type Handle = ContractDb;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let db = throwaway_db::create("c37_s3c")?;
        // dep: PostgreSQL(owner) — read the catalog of this test's throwaway database
        let client = postgres::Client::connect(&db.dsn(), postgres::NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
        Ok(ContractDb { client, _db: db })
    }
}

/// An Evidence whose only carrier is a MEMORY_LIFECYCLE row and one ticket: its EVIDENCE_ACCEPTED row is gone
/// (outbox identity is immutable, 0104, so the row is never written rather than rewritten).
fn evidence_without_accepted_row(h: &mut Handle, ws: Uuid) -> Uuid {
    let gone: Uuid = h
            .admin
            .query_one(
                "WITH e AS (INSERT INTO private.evidence_objects (tenant_id, evidence_kind, payload_sha256, \
                              data_class, origin_class, visibility_class, reasoning_domain_id) \
                            VALUES ($1, 'EVENT', sha256(gen_random_uuid()::text::bytea), 'INTERNAL', \
                              'DirectUserInput', 'TENANT_SHARED', $2) RETURNING evidence_id), \
                      ev AS (INSERT INTO private.events (event_id, event_kind, payload) \
                             SELECT evidence_id, 'USER_MESSAGE', '{}' FROM e), \
                      k AS (UPDATE projection.stream_checkpoints SET issued_highwater = issued_highwater + 1 \
                             WHERE tenant_id = $1 AND scope_id = $3 RETURNING issued_highwater), \
                      c AS (SELECT nextval('ops.commit_seq_seq') AS commit_seq), \
                      o AS (INSERT INTO ops.outbox (tenant_id, commit_seq, stream_seq, event_type, evidence_id, \
                              status) \
                            SELECT $1, c.commit_seq, k.issued_highwater, 'MEMORY_LIFECYCLE', e.evidence_id, 'DONE' \
                              FROM e, k, c RETURNING commit_seq, stream_seq), \
                      t AS (INSERT INTO projection.stream_log (tenant_id, scope_kind, scope_id, domain, \
                              projection_kind, projection_version, stream_seq, commit_seq) \
                            SELECT $1, 'workspace', $3, 'private_memory', 'PRIVATE_MEMORY', 'v1', o.stream_seq, \
                                   o.commit_seq FROM o) \
                 SELECT evidence_id FROM e",
                &[&h.tenant_id, &h.reasoning_domain_id, &ws],
            )
            .expect("an Evidence without an EVIDENCE_ACCEPTED row")
            .get(0);
    gone
}

/// Forces the generation ticket on the run's lowest home commit to FAILED `transient_exhausted`, as the retrieval
/// worker (its ISSUED -> FAILED edge); returns the rows moved.
fn force_transient_exhausted(h: &Handle, run_id: Uuid) -> u64 {
    // dep: PostgreSQL(role_retrieval_worker) — the worker's own ISSUED -> FAILED edge for the forced ticket
    let mut worker_client = postgres::Client::connect(
        &format!(
            "{}{}options=-c%20role%3Drole_retrieval_worker",
            h.dsn,
            if h.dsn.contains('?') { '&' } else { '?' }
        ),
        postgres::NoTls,
    )
    .expect("retrieval worker connects");
    let mut txn = worker_client.transaction().expect("txn");
    txn.batch_execute(&format!("SET LOCAL humaux.tenant_id = '{}'", h.tenant_id))
        .expect("guc");
    let forced = txn
            .execute(
                "UPDATE projection.stream_log sl SET state = 'FAILED', error_class = 'transient_exhausted' \
                   FROM projection.rebuild_tickets rt \
                  WHERE rt.run_id = $1 AND sl.tenant_id = rt.tenant_id AND sl.scope_id = rt.scope_id \
                    AND sl.stream_seq = rt.stream_seq \
                    AND rt.commit_seq = (SELECT min(commit_seq) FROM projection.rebuild_tickets WHERE run_id = $1)",
                &[&run_id],
            )
            .expect("force transient_exhausted");
    txn.commit().expect("commit");
    drop(worker_client);
    forced
}
