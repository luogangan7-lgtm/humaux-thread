//! `adapters::tests::forget_repo` — T4.8 integration test — `forget_repo` (§37/§37.1/§37.2/§65) against a real
//!   Postgres.
//! Depends-on: crates=[humaux-adapters, humaux-application, humaux-domain, humaux-projection, humaux-testkit,
//!   postgres, sqlx, tokio]; services=[PostgreSQL(any) w=[control.deletion_requests, control.tenants,
//!   ops.deletion_plan_steps, projection.stream_checkpoints, projection.stream_log], PostgreSQL(role_maintenance)];
//!   env=[HUMAUX_TEST_PG_DSN]; modules=[adapters::forget_repo, adapters::postgres, application::forget, domain::ids,
//!   humaux-testkit, projection::stream]
//! Called-by: [cargo-test]
//! Invariants: [rows are scoped to a throwaway tenant cleaned up on Drop; the TOMBSTONED edge and plan steps run on
//!   role_maintenance; no DSN, unreachable DB or missing table/function is a visible SKIP]
//! Spec: Baseline §79.2
//!
//! Same
//! convention as `stream_repo.rs`: shared tables, not a scratch schema, every test scopes
//! rows to a throwaway `control.tenants` row this file owns and cleans up on `Drop`.
//!
//! Three-state skip (§79.2): no DSN, unreachable DB, or a missing table/function prints a
//! visible SKIP and returns.

use std::time::{Duration, SystemTime};

use humaux_adapters::forget_repo;
use humaux_adapters::postgres::MaintenanceDbPool;
use humaux_application::forget::{DeletionPlan, DeletionStep};
use humaux_domain::ids::TenantId;
use humaux_projection::stream::StreamKey;
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    // libpq-standard `options=-c role=X` (URL-encoded). The older `options[role]=X` form
    // is silently tolerated by sqlx but rejected outright by rust-postgres ("invalid
    // connection string") — which made every `Client::connect` role fixture skip, and a
    // skip is not a pass (§79.2). This form is verified working on both drivers.
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

struct Handle {
    rt: tokio::runtime::Runtime,
    maintenance: MaintenanceDbPool,
    gateway: Client,
    admin: Client,
    tenant_id: Uuid,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Best-effort cleanup (repo CLAUDE.md hard rule ④: this file never touches a
        // schema/table of its own, only rows it created under its own throwaway tenant).
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM ops.deletion_plan_steps WHERE tenant_id = '{0}'; \
             DELETE FROM control.deletion_requests WHERE tenant_id = '{0}'; \
             DELETE FROM projection.stream_log WHERE tenant_id = '{0}'; \
             DELETE FROM projection.stream_checkpoints WHERE tenant_id = '{0}'; \
             DELETE FROM control.tenants WHERE tenant_id = '{0}';",
            self.tenant_id
        ));
    }
}

struct ForgetFixture;

impl DbIntegrationFixture for ForgetFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        // dep: PostgreSQL(any) — opens the role-scoped connection for `isolate`
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        for table in [
            "projection.stream_log",
            "control.deletion_requests",
            "ops.deletion_plan_steps",
        ] {
            let exists: bool = admin
                .query_one("SELECT to_regclass($1) IS NOT NULL", &[&table])
                .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
                .get(0);
            if !exists {
                return Err(DbFixtureSkipReason::IsolationSetupFailed(format!(
                    "{table} does not exist — run `cargo xtask migrate` first"
                )));
            }
        }

        let tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"forget_repo.rs throwaway tenant"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        // dep: PostgreSQL(role_maintenance) — opens the role-scoped connection for `isolate`
        let maintenance = rt
            .block_on(MaintenanceDbPool::connect(&dsn_as_role(
                &dsn,
                "role_maintenance",
            )))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        // dep: PostgreSQL(role_maintenance) — opens the role-scoped connection for `isolate`
        let gateway = Client::connect(&dsn_as_role(&dsn, "role_gateway"), NoTls)
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        Ok(Handle {
            rt,
            maintenance,
            gateway,
            admin,
            tenant_id,
        })
    }
}

fn key(handle: &Handle) -> StreamKey {
    StreamKey::new(
        TenantId(handle.tenant_id),
        "workspace",
        Uuid::new_v4(),
        "code",
        "retrieval_card",
        "v1",
    )
}

fn seed_checkpoint(admin: &mut Client, key: &StreamKey, issued_highwater: i64) {
    admin
        .execute(
            "INSERT INTO projection.stream_checkpoints \
               (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, \
                issued_highwater) \
             VALUES ($1,$2,$3,$4,$5,$6,$7)",
            &[
                &key.tenant_id.0,
                &key.scope_kind,
                &key.scope_id,
                &key.domain,
                &key.projection_kind,
                &key.projection_version,
                &issued_highwater,
            ],
        )
        .expect("seed stream_checkpoints row");
}

/// Seeds `count` `DONE` `stream_log` rows, `stream_seq` 1..=count.
fn seed_done_rows(admin: &mut Client, key: &StreamKey, count: i64) {
    let now = SystemTime::now();
    for seq in 1..=count {
        admin
            .execute(
                "INSERT INTO projection.stream_log \
                   (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, \
                    stream_seq, commit_seq, state, issued_at, settled_at) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$7,'DONE',$8,$8)",
                &[
                    &key.tenant_id.0,
                    &key.scope_kind,
                    &key.scope_id,
                    &key.domain,
                    &key.projection_kind,
                    &key.projection_version,
                    &seq,
                    &now,
                ],
            )
            .expect("seed stream_log DONE row");
    }
}

fn seed_deletion_request(admin: &mut Client, key: &StreamKey, seq: i64) -> Uuid {
    admin
        .query_one(
            "INSERT INTO control.deletion_requests \
               (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, \
                stream_seq) \
             VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING deletion_request_id",
            &[
                &key.tenant_id.0,
                &key.scope_kind,
                &key.scope_id,
                &key.domain,
                &key.projection_kind,
                &key.projection_version,
                &seq,
            ],
        )
        .expect("seed control.deletion_requests row")
        .get(0)
}

fn envelope_counts(admin: &mut Client, key: &StreamKey) -> (i64, i64, i64) {
    // (done, deleted, open_gaps) — §15.2/§37.1's three independent counts, read directly
    // (this test's own scope is forget_repo, not stream_repo's fuller snapshot query).
    let row = admin
        .query_one(
            "SELECT \
               count(*) FILTER (WHERE state IN ('DONE','SKIPPED_BY_POLICY','TOMBSTONED')) AS done, \
               count(*) FILTER (WHERE state = 'TOMBSTONED') AS deleted, \
               count(*) FILTER (WHERE state IN ('FAILED','LOST')) AS open_gaps \
             FROM projection.stream_log \
             WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND domain=$4 \
               AND projection_kind=$5 AND projection_version=$6",
            &[
                &key.tenant_id.0,
                &key.scope_kind,
                &key.scope_id,
                &key.domain,
                &key.projection_kind,
                &key.projection_version,
            ],
        )
        .expect("envelope aggregate query");
    (row.get(0), row.get(1), row.get(2))
}

/// §23.4 G23-2 "合法删除对照": tombstone 10 of 100 `DONE` rows through the sole legal path
/// (`forget_repo::tombstone`, never a raw `UPDATE ... SET state`) and confirm the ledger's
/// worked-example row 1 (§37.1): `done` stays 100 (`TOMBSTONED` counts as done, watermark
/// never blocks), `deleted` becomes 10 (`count(state='TOMBSTONED')`, current-computed),
/// `open_gaps` stays 0 (`TOMBSTONED` is not `FAILED`/`LOST`) — i.e. the legitimate-deletion
/// side of the same read the fault-injection variant (bypass tombstone, delete a point
/// directly) turns red on.
#[test]
fn legal_deletion_leaves_done_unchanged_and_deleted_equals_ten() {
    run_db_fixture::<ForgetFixture, _>(
        "legal_deletion_leaves_done_unchanged_and_deleted_equals_ten",
        |mut handle| {
            let k = key(&handle);
            seed_checkpoint(&mut handle.admin, &k, 100);
            seed_done_rows(&mut handle.admin, &k, 100);

            let (done_before, deleted_before, gaps_before) = envelope_counts(&mut handle.admin, &k);
            assert_eq!((done_before, deleted_before, gaps_before), (100, 0, 0));

            for seq in 1..=10u64 {
                let tombstoned = handle
                    .rt
                    .block_on(forget_repo::tombstone(&handle.maintenance, &k, seq))
                    .expect("tombstone succeeds");
                assert!(tombstoned, "seq {seq} must transition on first call");
            }

            let (done_after, deleted_after, gaps_after) = envelope_counts(&mut handle.admin, &k);
            assert_eq!(done_after, 100, "§37.1: TOMBSTONED still counts as done");
            assert_eq!(deleted_after, 10, "deleted = count(state='TOMBSTONED')");
            assert_eq!(gaps_after, 0, "tombstone is not a gap state");
            // visible = expected(100) - deleted(10) = 90 (§37.1 worked example row 1,
            // 90/90 = 1.0) — this test's scope is the ledger side; the retrieval-side
            // TOMBSTONED-exclusion overlay is a different T-card's read path.

            // Idempotent replay: tombstoning an already-TOMBSTONED seq is a no-op, not a
            // second transition or an error (§65 "幂等：中断后重放不重复删").
            let replay = handle
                .rt
                .block_on(forget_repo::tombstone(&handle.maintenance, &k, 1))
                .expect("replay does not error");
            assert!(!replay, "already-TOMBSTONED seq must report no-op");
            let (done_replay, deleted_replay, _) = envelope_counts(&mut handle.admin, &k);
            assert_eq!(
                (done_replay, deleted_replay),
                (100, 10),
                "replay must not double-count"
            );
        },
    );
}

/// §37.2 GRANT-layer rejection: `role_gateway` holds `SELECT, INSERT` on `projection.stream_log`
/// (0011) — no `UPDATE` at all — so a raw `UPDATE ... SET state = 'TOMBSTONED'` issued as that
/// role must be refused by Postgres itself (`42501 insufficient_privilege`), independent of
/// the `stream_log_guard_state_transition` trigger's own legality check (which never even
/// runs — the GRANT check happens first).
#[test]
fn runtime_role_direct_update_is_rejected_by_grant_not_just_trigger() {
    run_db_fixture::<ForgetFixture, _>(
        "runtime_role_direct_update_is_rejected_by_grant_not_just_trigger",
        |mut handle| {
            let k = key(&handle);
            seed_checkpoint(&mut handle.admin, &k, 1);
            seed_done_rows(&mut handle.admin, &k, 1);

            let err = handle
                .gateway
                .execute(
                    "UPDATE projection.stream_log SET state = 'TOMBSTONED' \
                     WHERE tenant_id = $1 AND stream_seq = 1",
                    &[&handle.tenant_id],
                )
                .expect_err("role_gateway has no UPDATE grant on projection.stream_log");

            // Assert on SQLSTATE, not the Display string: rust-postgres renders a DbError
            // as the bare "db error", so a message match here passes for *any* failure —
            // including the trigger firing, which is precisely what this test must rule out.
            // 42501 insufficient_privilege is raised by aclcheck_error before any trigger runs.
            assert_eq!(
                err.code(),
                Some(&postgres::error::SqlState::INSUFFICIENT_PRIVILEGE),
                "expected a GRANT-layer rejection (42501), got: {err:?}"
            );
        },
    );
}

/// §65 purge-replay idempotency: a plan interrupted after some steps are recorded must, on
/// replay, execute only the remaining steps — never re-execute (and never duplicate the
/// audit row for) a step already marked done.
#[test]
fn purge_replay_resumes_without_repeating_completed_steps() {
    run_db_fixture::<ForgetFixture, _>(
        "purge_replay_resumes_without_repeating_completed_steps",
        |mut handle| {
            let k = key(&handle);
            seed_checkpoint(&mut handle.admin, &k, 1);
            seed_done_rows(&mut handle.admin, &k, 1);
            let request_id = seed_deletion_request(&mut handle.admin, &k, 1);
            let tenant_id = handle.tenant_id;

            // Simulate a crash right after step 1 (tombstone) by recording it directly,
            // without going through the other seven steps.
            handle
                .rt
                .block_on(forget_repo::record_step(
                    &handle.maintenance,
                    tenant_id,
                    request_id,
                    DeletionStep::StreamLogTombstone,
                    "DONE",
                    None,
                ))
                .expect("record step 1");

            let plan = DeletionPlan::new();
            let mut calls: Vec<DeletionStep> = Vec::new();
            let executed_first = handle
                .rt
                .block_on(forget_repo::replay_pending_steps(
                    &handle.maintenance,
                    tenant_id,
                    request_id,
                    &plan,
                    |step| {
                        calls.push(step);
                        ("DONE", None)
                    },
                ))
                .expect("first replay executes remaining steps");
            assert_eq!(
                executed_first.len(),
                7,
                "steps 2..8, step 1 already recorded"
            );
            assert!(!executed_first.contains(&DeletionStep::StreamLogTombstone));
            assert_eq!(
                calls.len(),
                7,
                "execute() called once per remaining step, not per replay"
            );

            // Replay after "completion": everything already recorded ⇒ no-op, no duplicate
            // rows, `execute` never invoked again.
            let mut calls_second: Vec<DeletionStep> = Vec::new();
            let executed_second = handle
                .rt
                .block_on(forget_repo::replay_pending_steps(
                    &handle.maintenance,
                    tenant_id,
                    request_id,
                    &plan,
                    |step| {
                        calls_second.push(step);
                        ("DONE", None)
                    },
                ))
                .expect("second replay is a no-op");
            assert!(
                executed_second.is_empty(),
                "fully-completed plan replays to nothing"
            );
            assert!(
                calls_second.is_empty(),
                "execute() must not run for already-done steps"
            );

            let row_count: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM ops.deletion_plan_steps WHERE deletion_request_id = $1",
                    &[&request_id],
                )
                .expect("count rows")
                .get(0);
            assert_eq!(
                row_count, 8,
                "exactly one row per step, no duplicates across replays"
            );
        },
    );
}

/// §41.2 `tombstoned_unpurged_over_sla`: current-computed (no materialized column), flips
/// from >0 to 0 the instant the missing `QDRANT_POINTS` (physical purge, step 5) row is
/// recorded — the "红转绿" this gauge exists to make observable (§37: "它必须有自己的出口").
#[test]
fn tombstoned_unpurged_over_sla_goes_red_then_green_on_purge_step() {
    run_db_fixture::<ForgetFixture, _>(
        "tombstoned_unpurged_over_sla_goes_red_then_green_on_purge_step",
        |mut handle| {
            let k = key(&handle);
            seed_checkpoint(&mut handle.admin, &k, 1);
            seed_done_rows(&mut handle.admin, &k, 1);
            let request_id = seed_deletion_request(&mut handle.admin, &k, 1);
            let tenant_id = handle.tenant_id;

            handle
                .rt
                .block_on(forget_repo::tombstone(&handle.maintenance, &k, 1))
                .expect("tombstone seq 1");

            let sla = Duration::from_secs(0);
            let before = handle
                .rt
                .block_on(forget_repo::tombstoned_unpurged_over_sla(
                    &handle.maintenance,
                    tenant_id,
                    sla,
                ))
                .expect("metric query");
            assert_eq!(before, 1, "tombstoned + no QDRANT_POINTS step yet ⇒ red");

            handle
                .rt
                .block_on(forget_repo::record_step(
                    &handle.maintenance,
                    tenant_id,
                    request_id,
                    DeletionStep::QdrantPoints,
                    "DONE",
                    None,
                ))
                .expect("record purge step");

            let after = handle
                .rt
                .block_on(forget_repo::tombstoned_unpurged_over_sla(
                    &handle.maintenance,
                    tenant_id,
                    sla,
                ))
                .expect("metric query");
            assert_eq!(after, 0, "purge step recorded ⇒ green");
        },
    );
}

/// §37.2 architecture assertion (own-file verification — the workspace-wide
/// architecture-check(§37.2) grep gate itself is not wired into `xtask` by this ticket, see
/// the T4.8 report): `projection.stream_log` is exactly the columns §15.1's DDL names — the
/// original 12 plus the two §15.2.1 audit columns 0167 added (`retired_at`, `retired_by`; facts
/// about a transition, not counters derivable from `state`, which is what §37.2's freeze
/// guards against) — no `deleted_count` and no `status` (`state` is the only status-like
/// column). The set is asserted verbatim so a 15th column is red by name, not by count alone.
#[test]
fn stream_log_has_exactly_the_frozen_columns() {
    run_db_fixture::<ForgetFixture, _>(
        "stream_log_has_exactly_the_frozen_columns",
        |mut handle| {
            let cols: Vec<String> = handle
                .admin
                .query(
                    "SELECT column_name FROM information_schema.columns \
                 WHERE table_schema = 'projection' AND table_name = 'stream_log'",
                    &[],
                )
                .expect("query columns")
                .iter()
                .map(|r| r.get(0))
                .collect();
            let mut got = cols.clone();
            got.sort();
            let mut want = vec![
                "tenant_id",
                "scope_kind",
                "scope_id",
                "domain",
                "projection_kind",
                "projection_version",
                "stream_seq",
                "commit_seq",
                "state",
                "error_class",
                "issued_at",
                "settled_at",
                "retired_at",
                "retired_by",
            ];
            want.sort();
            assert_eq!(
                got, want,
                "§37.2/§15.1: the frozen column set (12 + the two 0167 audit columns), got {cols:?}"
            );
            assert!(
                !cols.iter().any(|c| c == "deleted_count"),
                "no materialized deleted_count column"
            );
            assert!(
                !cols.iter().any(|c| c == "status"),
                "column is `state`, never `status`"
            );
        },
    );
}
