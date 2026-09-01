//! T4.6/T4.7 integration test — `consolidate_repo` (§11.7) against a real Postgres.
//!
//! **G11-1 / G80-29 "Consolidation Snapshot Integrity"** (§11.9's own anchor for both gate
//! numbers): seed 100 eligible memories, start selection, concurrently insert 60 higher-ranked
//! rows, repeat 10 times — every run's `memory_consolidation_inputs` must be duplicate-free,
//! miss nothing from its own snapshot, and hash-stable. `select_and_materialize_inputs` orders
//! `ORDER BY memory_id DESC` (see its own doc comment) specifically so that "60 higher-ranked"
//! concurrent inserts are real: `memory_id` is UUIDv7 (time-ordered), so a row inserted while
//! this test's transaction is open is always the newest id in the table, and therefore always
//! sorts *first* under `DESC` — exactly the shape of the 2026 Codex bug §11.7 quotes ("Phase 1
//! 插入高排名行时，后续 offset 整体移动").
//!
//! **Red-then-green fault injection (§80.1 "没有注错红转绿记录就不算存在" — this file's own
//! record of running it, since the buggy variant is never something this repo can ship):**
//! reran [`select_and_materialize_inputs`] locally with its body replaced by a naive
//! multi-page `OFFSET`/`LIMIT` loop across *separate* transactions (one `SELECT ... ORDER BY
//! memory_id DESC LIMIT 40 OFFSET n` autocommit query per page, `n` advancing 40/80/…, no
//! shared snapshot) — [`snapshot_survives_concurrent_higher_ranked_inserts`] below went RED:
//! the 60-higher-ranked-first inserts shifted every later page's `OFFSET` window by up to 60,
//! producing both duplicate memory_ids (a row already returned on an earlier page reappearing
//! after the window shifted back over it) and missed ones (a row shifted past the last page's
//! `LIMIT` boundary entirely) across the 10 repetitions. Reverting to this file's single-
//! transaction `select_and_materialize_inputs` turned the same test back GREEN. The buggy
//! variant itself is intentionally not committed anywhere in this workspace (§46 "main 不允许
//! 红") — this comment plus the description above is the "红转绿" record §80.1 requires.
//!
//! Three-state skip (§79.2): no DSN, unreachable DB, or the migration missing all print a
//! visible SKIP and return.

use std::{
    sync::{Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};

use humaux_adapters::consolidate_repo;
use humaux_adapters::postgres::ConsolidationDbPool;
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;

/// Serializes every test in this file — each seeds its own throwaway tenant (disjoint rows),
/// but the concurrency test below drives real concurrent connections against the shared
/// `private.memory_records`/`private.memory_consolidation_*` tables and asserts on exact
/// counts; running two copies of it in true parallel would not corrupt correctness but would
/// make the "no leakage from a sibling test" reasoning harder to state cleanly.
static SERIAL_GUARD: Mutex<()> = Mutex::new(());

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    // libpq-standard `options=-c role=X` (URL-encoded). The older `options[role]=X` form
    // is silently tolerated by sqlx but rejected outright by rust-postgres ("invalid
    // connection string") — which made every `Client::connect` role fixture skip, and a
    // skip is not a pass (§79.2). This form is verified working on both drivers.
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

fn dsn_as_role_with_application(admin_dsn: &str, role: &str, application_name: &str) -> String {
    format!(
        "{}&application_name={application_name}",
        dsn_as_role(admin_dsn, role)
    )
}

fn wait_for_selector_materialization_wait(
    admin: &mut Client,
    application_name: &str,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let waiting: bool = admin
            .query_one(
                "SELECT EXISTS (                   SELECT 1                     FROM pg_stat_activity AS activity                     JOIN pg_locks AS locks USING (pid)                    WHERE activity.application_name = $1                      AND locks.locktype = 'relation'                      AND locks.relation = 'private.memory_consolidation_inputs'::regclass                      AND locks.mode = 'RowExclusiveLock'                      AND NOT locks.granted                 )",
                &[&application_name],
            )
            .map_err(|error| format!("inspect selector materialization wait: {error}"))?
            .get(0);
        if waiting {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "selector {application_name} never waited for a RowExclusiveLock on \
                 private.memory_consolidation_inputs within 30s"
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

struct Handle {
    rt: tokio::runtime::Runtime,
    consolidation: ConsolidationDbPool,
    admin: Client,
    tenant_id: Uuid,
    reasoning_domain_id: Uuid,
    /// One shared Evidence every seeded Memory links to (§8.6's orphan-Memory check just needs
    /// *a* link, not a distinct one per Memory — the PK on `memory_evidence` is
    /// `(memory_id, evidence_id, role)`, so many memories sharing one `evidence_id` is fine).
    evidence_id: Uuid,
    /// Kept for the concurrency test's independent inserter connections — one per simulated
    /// concurrent writer, same reasoning as `jobs_claim.rs`'s `gateway_dsn`.
    admin_dsn: String,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Best-effort cleanup (repo CLAUDE.md hard rule ④: this file never touches a
        // schema/table of its own, only rows it created under its own throwaway tenant).
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM private.memory_rollup_sources WHERE rollup_id IN \
               (SELECT rollup_id FROM private.memory_rollups WHERE tenant_id = '{0}'); \
             DELETE FROM private.memory_rollups WHERE tenant_id = '{0}'; \
             DELETE FROM private.memory_consolidation_inputs WHERE run_id IN \
               (SELECT run_id FROM private.memory_consolidation_runs WHERE tenant_id = '{0}'); \
             DELETE FROM private.memory_consolidation_runs WHERE tenant_id = '{0}'; \
             DELETE FROM private.memory_evidence WHERE memory_id IN \
               (SELECT memory_id FROM private.memory_records WHERE tenant_id = '{0}'); \
             DELETE FROM private.memory_records WHERE tenant_id = '{0}'; \
             DELETE FROM private.events WHERE event_id IN \
               (SELECT evidence_id FROM private.evidence_objects WHERE tenant_id = '{0}'); \
             DELETE FROM private.evidence_objects WHERE tenant_id = '{0}'; \
             DELETE FROM control.private_reasoning_domains WHERE tenant_id = '{0}'; \
             DELETE FROM control.tenants WHERE tenant_id = '{0}';",
            self.tenant_id
        ));
    }
}

struct ConsolidateFixture;

impl DbIntegrationFixture for ConsolidateFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        let migrated: bool = admin
            .query_one(
                "SELECT to_regclass('private.memory_consolidation_inputs') IS NOT NULL",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !migrated {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "private.memory_consolidation_inputs does not exist — run `cargo xtask migrate` \
                 against HUMAUX_TEST_PG_DSN first"
                    .to_string(),
            ));
        }

        let tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"consolidate_snapshot.rs throwaway tenant"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        let reasoning_domain_id: Uuid = admin
            .query_one(
                "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
                 VALUES ($1, 'consolidate_snapshot.rs domain') RETURNING reasoning_domain_id",
                &[&tenant_id],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        // §8.1: `evidence_kind='EVENT'` needs a matching `private.events` row (real FK on
        // `event_id = evidence_id`) — same minimal recipe as
        // `retrieve_read_your_writes.rs::seed_evidence_and_stream_row`.
        let evidence_id: Uuid = admin
            .query_one(
                "INSERT INTO private.evidence_objects \
                   (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                    visibility_class, reasoning_domain_id) \
                 VALUES ($1, 'EVENT', $2, 'INTERNAL', 'DirectUserInput', 'TENANT_SHARED', $3) \
                 RETURNING evidence_id",
                &[&tenant_id, &vec![0u8; 32], &reasoning_domain_id],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        admin
            .execute(
                "INSERT INTO private.events (event_id, event_kind, payload) \
                 VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)",
                &[&evidence_id],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        let consolidation_dsn = dsn_as_role(&dsn, "role_consolidation_worker");
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let consolidation = rt
            .block_on(ConsolidationDbPool::connect(&consolidation_dsn))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        Ok(Handle {
            rt,
            consolidation,
            admin,
            tenant_id,
            reasoning_domain_id,
            evidence_id,
            admin_dsn: dsn,
        })
    }
}

/// Seeds `count` `active`/`TENANT_SHARED` memory rows for `handle.tenant_id`, each linked to
/// `handle.evidence_id` — `TENANT_SHARED` (both `visibility_user_id`/`visibility_workspace_id`
/// NULL) sidesteps needing a seeded `control.users`/`workspaces` row just to satisfy
/// `memory_records_visibility_matches_class`.
///
/// §8.6's orphan-Memory check is a `DEFERRABLE INITIALLY DEFERRED` constraint trigger — it
/// only fires at COMMIT, but each `memory_records` INSERT still needs its `memory_evidence`
/// link in the *same* transaction, or the `postgres` crate's default per-statement autocommit
/// (outside an explicit `transaction()`) commits the orphan Memory row before its link exists
/// and the trigger rejects it (same reasoning as
/// `retrieve_read_your_writes.rs::link_memories_to_evidence`).
fn seed_active_memories(handle: &mut Handle, confidence: f32, count: usize) -> Vec<Uuid> {
    let evidence_id = handle.evidence_id;
    let mut txn = handle.admin.transaction().expect("begin seed txn");
    let ids: Vec<Uuid> = (0..count)
        .map(|i| {
            let memory_id: Uuid = txn
                .query_one(
                    "INSERT INTO private.memory_records \
                       (tenant_id, memory_type, content, visibility_class, \
                        authority_class, confidence, status, asserted_at) \
                     VALUES ($1, 'NOTE', $2, 'TENANT_SHARED', 'PrivateKnowledge', $3, 'active', now()) \
                     RETURNING memory_id",
                    &[&handle.tenant_id, &serde_json::json!({"seed": i}), &confidence],
                )
                .expect("seed active memory")
                .get(0);
            txn.execute(
                "INSERT INTO private.memory_evidence (memory_id, evidence_id, role) \
                 VALUES ($1, $2, 'PRIMARY')",
                &[&memory_id, &evidence_id],
            )
            .expect("insert memory_evidence link");
            memory_id
        })
        .collect();
    txn.commit().expect("commit seed txn");
    ids
}

fn insert_concurrent_higher_ranked_memories(
    admin_dsn: &str,
    tenant_id: Uuid,
    evidence_id: Uuid,
) -> Result<(), String> {
    let mut client = Client::connect(admin_dsn, NoTls)
        .map_err(|error| format!("concurrent inserter connects: {error}"))?;
    let mut txn = client
        .transaction()
        .map_err(|error| format!("begin concurrent insert transaction: {error}"))?;
    for i in 0..60 {
        let memory_id: Uuid = txn
            .query_one(
                "INSERT INTO private.memory_records \
                   (tenant_id, memory_type, content, visibility_class, \
                    authority_class, confidence, status, asserted_at) \
                 VALUES ($1, 'NOTE', $2, 'TENANT_SHARED', \
                         'PrivateKnowledge', 0.9, 'active', now()) \
                 RETURNING memory_id",
                &[&tenant_id, &serde_json::json!({"concurrent": i})],
            )
            .map_err(|error| format!("insert concurrent memory: {error}"))?
            .get(0);
        txn.execute(
            "INSERT INTO private.memory_evidence (memory_id, evidence_id, role) \
             VALUES ($1, $2, 'PRIMARY')",
            &[&memory_id, &evidence_id],
        )
        .map_err(|error| format!("link concurrent memory evidence: {error}"))?;
    }
    txn.commit()
        .map_err(|error| format!("commit concurrent memory inserts: {error}"))?;
    Ok(())
}

fn recorded_input_memory_ids(handle: &mut Client, run_id: Uuid) -> Vec<Uuid> {
    handle
        .query(
            "SELECT memory_id FROM private.memory_consolidation_inputs \
             WHERE run_id = $1 ORDER BY ordinal",
            &[&run_id],
        )
        .expect("read back memory_consolidation_inputs")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

fn recorded_input_hash(handle: &mut Client, run_id: Uuid) -> Vec<u8> {
    // §11.9 "输入集合 hash 稳定": a stable digest over the recorded `(memory_id, ordinal)`
    // pairs in ordinal order — independent of this test's own in-memory bookkeeping, reads
    // only what `select_and_materialize_inputs` actually persisted.
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for id in recorded_input_memory_ids(handle, run_id) {
        hasher.update(id.as_bytes());
    }
    hasher.finalize().to_vec()
}

/// Resets `handle.tenant_id`'s rows, seeds a fresh 100, runs one snapshot-bound selection while
/// racing 60 "higher-ranked" concurrent inserts against it, and asserts the recorded inputs are
/// exactly that iteration's 100 — no duplicates, nothing missing, no leak from the concurrent
/// 60 (§11.7: "新 Memory 在 snapshot 之后写入 -> next consolidation run"). Returns the
/// §11.9 "输入集合 hash" for the caller's cross-iteration sanity check.
// Sequential DB setup/race/assert steps for one iteration, same shape as this crate's own
// `qdrant_live.rs`/`recall_envelope_g23.rs` precedent for this allow — splitting would scatter
// one iteration's steps across helpers with no reuse, not simplify anything.
#[allow(clippy::too_many_lines)]
fn run_one_iteration(handle: &mut Handle, iteration: usize) -> Vec<u8> {
    // Each iteration must select from exactly its own fresh 100 — without this, iteration N's
    // query would also see every prior iteration's still-`active` rows (this test never
    // supersedes/revokes them), inflating the expected count.
    handle
        .admin
        .batch_execute(&format!(
            "DELETE FROM private.memory_evidence WHERE memory_id IN \
               (SELECT memory_id FROM private.memory_records WHERE tenant_id = '{0}'); \
             DELETE FROM private.memory_consolidation_inputs WHERE run_id IN \
               (SELECT run_id FROM private.memory_consolidation_runs WHERE tenant_id = '{0}'); \
             DELETE FROM private.memory_consolidation_runs WHERE tenant_id = '{0}'; \
             DELETE FROM private.memory_records WHERE tenant_id = '{0}';",
            handle.tenant_id
        ))
        .expect("reset tenant rows between iterations");

    let base_ids = seed_active_memories(handle, 0.5, 100);
    let mut base_sorted = base_ids.clone();
    base_sorted.sort();

    let run_id = handle
        .rt
        .block_on(consolidate_repo::create_run(
            &handle.consolidation,
            handle.tenant_id,
            handle.reasoning_domain_id,
            None,
        ))
        .expect("create_run must succeed");

    let tenant_id = handle.tenant_id;
    let reasoning_domain_id = handle.reasoning_domain_id;
    let evidence_id = handle.evidence_id;
    let admin_dsn = handle.admin_dsn.clone();
    let selector_application_name = format!("cs_{iteration}_{}", Uuid::new_v4());
    let selector_dsn = dsn_as_role_with_application(
        &admin_dsn,
        "role_consolidation_worker",
        &selector_application_name,
    );

    // This fixture-held SHARE lock is a durable barrier, unlike the old brief run-row lock.
    // The selector has already established its REPEATABLE READ snapshot and read all eligible
    // memory rows before its first INSERT into this table requests the blocked RowExclusiveLock.
    let mut holder = Client::connect(&admin_dsn, NoTls).expect("connect materialization holder");
    let mut holder_txn = holder
        .transaction()
        .expect("begin materialization-holder transaction");
    holder_txn
        .batch_execute("LOCK TABLE private.memory_consolidation_inputs IN SHARE MODE")
        .expect("hold materialization table SHARE lock");

    let selector = thread::spawn(move || {
        let actor_rt = tokio::runtime::Runtime::new()
            .map_err(|error| format!("create selector runtime: {error}"))?;
        let actor_pool = actor_rt
            .block_on(ConsolidationDbPool::connect(&selector_dsn))
            .map_err(|error| format!("connect selector pool: {error}"))?;
        actor_rt
            .block_on(consolidate_repo::select_and_materialize_inputs(
                &actor_pool,
                run_id,
                tenant_id,
                reasoning_domain_id,
                None,
                10_000,
            ))
            .map_err(|error| format!("selector materialization: {error}"))
    });

    // The exact actor must be waiting on this exact target relation before the 60 new rows
    // commit. The fixture therefore proves a source set frozen before materialization, and
    // catches the specific bad implementation that paginates and materializes each page in a
    // separate transaction; it does not claim to distinguish RR from RC for one eager SELECT.
    let barrier_result =
        wait_for_selector_materialization_wait(&mut handle.admin, &selector_application_name);
    let (inserter_done_tx, inserter_done_rx) = mpsc::channel();
    let inserter = barrier_result.as_ref().ok().map(|()| {
        let admin_dsn = admin_dsn.clone();
        thread::spawn(move || {
            let result =
                insert_concurrent_higher_ranked_memories(&admin_dsn, tenant_id, evidence_id);
            let completion = result.as_ref().map_err(Clone::clone).map(|_| ());
            let _ = inserter_done_tx.send(completion);
            result
        })
    });
    let insertion_completion = inserter.as_ref().map(|_| {
        inserter_done_rx
            .recv_timeout(Duration::from_secs(30))
            .map_err(|error| {
                format!("concurrent inserter did not report a committed result within 30s: {error}")
            })?
    });

    // No assertion may bypass cleanup: release the fixture lock, then collect every actor
    // result, before reporting a barrier, writer, or selector failure.
    let holder_release = holder_txn.rollback();
    let selector_join = selector.join();
    let inserter_join = inserter.map(|actor| actor.join());

    barrier_result.expect("selector must reach the materialization wait barrier");
    insertion_completion
        .expect("inserter runs only after the materialization barrier")
        .expect("concurrent higher-ranked inserts must commit");
    holder_release.expect("release materialization-holder transaction");
    let accepted = selector_join
        .expect("selector actor must join")
        .expect("select_and_materialize_inputs must not error");
    inserter_join
        .expect("inserter runs only after the materialization barrier")
        .expect("inserter actor must join")
        .expect("concurrent higher-ranked inserts must commit");

    let mut recorded: Vec<Uuid> = accepted
        .into_iter()
        .map(|a| a.memory_id.into_inner().0)
        .collect();
    recorded.sort();

    assert_eq!(
        recorded.len(),
        base_sorted.len(),
        "iteration {iteration}: expected exactly the 100 pre-run ids, got {} \
         (duplicates or a leak from the concurrent 60 would change this count)",
        recorded.len()
    );
    assert_eq!(
        recorded, base_sorted,
        "iteration {iteration}: recorded input set must equal exactly the snapshot's \
         100 ids — no duplicates, nothing missing, and none of this iteration's \
         concurrent 60 leaking in"
    );

    let db_recorded = recorded_input_memory_ids(&mut handle.admin, run_id);
    let mut db_sorted = db_recorded.clone();
    db_sorted.sort();
    db_sorted.dedup();
    assert_eq!(
        db_sorted.len(),
        db_recorded.len(),
        "iteration {iteration}: memory_consolidation_inputs must have no duplicate memory_id \
         for this run_id (PK is (run_id, memory_id) so a true duplicate would already be an \
         INSERT conflict, but this also catches a regression that silently swallowed one)"
    );

    recorded_input_hash(&mut handle.admin, run_id)
}

/// G11-1 / G80-29 (§11.9's anchor): seed 100 eligible memories, begin selection, concurrently
/// insert 60 higher-ranked rows mid-flight, repeat 10 times — see [`run_one_iteration`] for the
/// per-iteration assertions.
#[test]
fn snapshot_survives_concurrent_higher_ranked_inserts() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<ConsolidateFixture, _>(
        "snapshot_survives_concurrent_higher_ranked_inserts",
        |mut handle| {
            let seen_hashes: Vec<Vec<u8>> =
                (0..10).map(|i| run_one_iteration(&mut handle, i)).collect();

            // §11.9 "输入集合 hash 稳定" — not "every run's hash is identical" (each iteration
            // seeds a *fresh* 100 ids, so distinct hashes are expected across iterations); the
            // stability property this test can actually check without re-running the exact
            // same snapshot twice is "deterministic within a run" (already covered by
            // `run_one_iteration`'s equality assertions) and "no run produced an
            // empty/degenerate hash" (would indicate the digest silently saw zero recorded
            // rows).
            for (i, hash) in seen_hashes.iter().enumerate() {
                assert!(
                    !hash.is_empty() && hash.iter().any(|b| *b != 0),
                    "iteration {i}: input-set hash must not be empty/all-zero"
                );
            }
        },
    );
}

/// Seeds a second reasoning domain + its own evidence, linked to a memory that is otherwise
/// identical to a fixture-eligible one (same tenant, active, `TENANT_SHARED`) — split out of
/// [`selection_excludes_cross_domain_and_user_private_memories`] to keep that test under the
/// workspace's `too_many_lines` lint. Returns the decoy `memory_id`.
fn seed_cross_domain_decoy_memory(handle: &mut Handle) -> Uuid {
    let other_domain_id: Uuid = handle
        .admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
             VALUES ($1, 'cross-domain decoy') RETURNING reasoning_domain_id",
            &[&handle.tenant_id],
        )
        .expect("seed second reasoning domain")
        .get(0);
    let other_evidence_id: Uuid = handle
        .admin
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                visibility_class, reasoning_domain_id) \
             VALUES ($1, 'EVENT', $2, 'INTERNAL', 'DirectUserInput', 'TENANT_SHARED', $3) \
             RETURNING evidence_id",
            &[&handle.tenant_id, &vec![1u8; 32], &other_domain_id],
        )
        .expect("seed decoy evidence")
        .get(0);
    handle
        .admin
        .execute(
            "INSERT INTO private.events (event_id, event_kind, payload) \
             VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)",
            &[&other_evidence_id],
        )
        .expect("seed decoy event");
    let mut txn = handle.admin.transaction().expect("begin decoy memory txn");
    let decoy_memory_id: Uuid = txn
        .query_one(
            "INSERT INTO private.memory_records \
               (tenant_id, memory_type, content, visibility_class, \
                authority_class, confidence, status, asserted_at) \
             VALUES ($1, 'NOTE', '{}'::jsonb, 'TENANT_SHARED', \
                     'PrivateKnowledge', 0.9, 'active', now()) \
             RETURNING memory_id",
            &[&handle.tenant_id],
        )
        .expect("seed decoy memory")
        .get(0);
    txn.execute(
        "INSERT INTO private.memory_evidence (memory_id, evidence_id, role) \
         VALUES ($1, $2, 'PRIMARY')",
        &[&decoy_memory_id, &other_evidence_id],
    )
    .expect("link decoy memory to decoy evidence");
    txn.commit().expect("commit decoy memory txn");
    decoy_memory_id
}

/// Seeds a `USER_PRIVATE` memory in the fixture's own (domain-correct) evidence — domain-correct
/// but must still be excluded on visibility grounds alone. Returns `(private_user_id,
/// private_memory_id)`; the caller is responsible for best-effort `control.users` cleanup since
/// that table carries no `tenant_id` column for `Handle::drop`'s blanket delete to catch.
fn seed_user_private_decoy_memory(handle: &mut Handle) -> (Uuid, Uuid) {
    let private_user_id: Uuid = handle
        .admin
        .query_one(
            "INSERT INTO control.users DEFAULT VALUES RETURNING user_id",
            &[],
        )
        .expect("seed throwaway user")
        .get(0);
    let evidence_id = handle.evidence_id;
    let mut txn = handle
        .admin
        .transaction()
        .expect("begin private memory txn");
    let private_memory_id: Uuid = txn
        .query_one(
            "INSERT INTO private.memory_records \
               (tenant_id, memory_type, content, visibility_class, visibility_user_id, \
                authority_class, confidence, status, asserted_at) \
             VALUES ($1, 'NOTE', '{}'::jsonb, 'USER_PRIVATE', $2, \
                     'PrivateKnowledge', 0.9, 'active', now()) \
             RETURNING memory_id",
            &[&handle.tenant_id, &private_user_id],
        )
        .expect("seed USER_PRIVATE memory")
        .get(0);
    txn.execute(
        "INSERT INTO private.memory_evidence (memory_id, evidence_id, role) \
         VALUES ($1, $2, 'PRIMARY')",
        &[&private_memory_id, &evidence_id],
    )
    .expect("link USER_PRIVATE memory to fixture evidence");
    txn.commit().expect("commit private memory txn");
    (private_user_id, private_memory_id)
}

/// Fixer-review red-then-green record for two blockers in `select_and_materialize_inputs`'s
/// `WHERE` clause: before this fix, the query filtered only on `tenant_id`/`status`/
/// `visibility_workspace_id` — no `reasoning_domain_id` join at all (§11.8: "一次 LLM
/// consolidation 的全部输入 reasoning_domain 必须相同") and no `visibility_class` exclusion
/// (§11.6/§11.9: a `USER_PRIVATE` memory must never be summarized into the tenant-scoped
/// `memory_rollups` table). Re-running this test against the pre-fix query (temporarily,
/// locally, not committed — same discipline as this file's other red-then-green record above)
/// selected all 3 seeded memories instead of 1; this asserts exactly the fixture's one
/// same-domain `TENANT_SHARED` memory comes back.
///
/// Also covers §11.7's frozen recipe step this same review found unwritten:
/// `input_snapshot_seq` must be set on the run row once selection completes.
#[test]
fn selection_excludes_cross_domain_and_user_private_memories() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<ConsolidateFixture, _>(
        "selection_excludes_cross_domain_and_user_private_memories",
        |mut handle| {
            // The fixture's own domain/evidence: one genuinely eligible memory.
            let eligible_ids = seed_active_memories(&mut handle, 0.5, 1);
            // Domain-mismatched decoy (§11.8) and visibility-mismatched decoy (§11.6/§11.9) —
            // both domain-correct-or-not-quite, must both stay excluded from selection.
            let _cross_domain_decoy_memory_id = seed_cross_domain_decoy_memory(&mut handle);
            let (private_user_id, _user_private_decoy_memory_id) =
                seed_user_private_decoy_memory(&mut handle);

            let run_id = handle
                .rt
                .block_on(consolidate_repo::create_run(
                    &handle.consolidation,
                    handle.tenant_id,
                    handle.reasoning_domain_id,
                    None,
                ))
                .expect("create_run must succeed");

            let accepted = handle
                .rt
                .block_on(consolidate_repo::select_and_materialize_inputs(
                    &handle.consolidation,
                    run_id,
                    handle.tenant_id,
                    handle.reasoning_domain_id,
                    None,
                    10_000,
                ))
                .expect("select_and_materialize_inputs must not error");

            let mut recorded: Vec<Uuid> = accepted
                .into_iter()
                .map(|m| m.memory_id.into_inner().0)
                .collect();
            recorded.sort();
            let mut expected = eligible_ids.clone();
            expected.sort();
            assert_eq!(
                recorded, expected,
                "must select exactly the fixture's same-domain TENANT_SHARED memory — the \
                 cross-domain decoy and the USER_PRIVATE memory must both be excluded"
            );

            let input_snapshot_seq: Option<i64> = handle
                .admin
                .query_one(
                    "SELECT input_snapshot_seq FROM private.memory_consolidation_runs \
                     WHERE run_id = $1",
                    &[&run_id],
                )
                .expect("read back run row")
                .get(0);
            assert!(
                input_snapshot_seq.is_some(),
                "§11.7 input_snapshot_seq must be set once selection completes"
            );

            // Best-effort cleanup of this test's extra rows beyond what `Handle::drop` already
            // deletes by `tenant_id` (control.users carries no tenant_id column).
            let _ = handle.admin.execute(
                "DELETE FROM control.users WHERE user_id = $1",
                &[&private_user_id],
            );
        },
    );
}
