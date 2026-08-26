//! T3.3+T3.4 integration test — `stream_repo` (§15) against a real Postgres. Same convention
//! as `jobs_claim.rs`/`email_outbox.rs`: the tables are shared (`0007_projection.sql`,
//! `0008_ops_core.sql`), not a scratch schema, so every test scopes rows to a throwaway
//! `control.tenants` row this file owns and cleans up on `Drop`.
//!
//! Three-state skip (§79.2): no DSN, unreachable DB, or `projection.stream_log` missing all
//! print a visible SKIP and return.

use std::time::{Duration, SystemTime};

use humaux_adapters::postgres::{MaintenanceDbPool, RetrievalWorkerDbPool};
use humaux_adapters::stream_repo::{self, AdvanceError};
use humaux_domain::ids::TenantId;
use humaux_projection::stream::StreamKey;
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;

// `sweep_lost` is scoped to one `tenant_id` (RLS has no cross-tenant carve-out for
// `role_maintenance` — see `stream_repo`'s module doc), and every test below seeds its own
// throwaway tenant, so no serialization guard is needed here: two tests' sweeps can never
// touch each other's rows even running concurrently.

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options[role]={role}")
}

struct Handle {
    rt: tokio::runtime::Runtime,
    retrieval: RetrievalWorkerDbPool,
    maintenance: MaintenanceDbPool,
    admin: Client,
    tenant_id: Uuid,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Best-effort cleanup (repo CLAUDE.md hard rule ④: this file never touches a
        // schema/table of its own, only rows it created under its own throwaway tenant).
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM ops.jobs WHERE tenant_id = '{0}'; \
             DELETE FROM projection.stream_log WHERE tenant_id = '{0}'; \
             DELETE FROM projection.stream_checkpoints WHERE tenant_id = '{0}'; \
             DELETE FROM control.tenants WHERE tenant_id = '{0}';",
            self.tenant_id
        ));
    }
}

struct StreamFixture;

impl DbIntegrationFixture for StreamFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        for table in [
            "projection.stream_log",
            "projection.stream_checkpoints",
            "projection.processing_gaps",
            "ops.jobs",
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
                &[&"stream_repo.rs throwaway tenant"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let retrieval = rt
            .block_on(RetrievalWorkerDbPool::connect(&dsn_as_role(
                &dsn,
                "role_retrieval_worker",
            )))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let maintenance = rt
            .block_on(MaintenanceDbPool::connect(&dsn_as_role(
                &dsn,
                "role_maintenance",
            )))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        Ok(Handle {
            rt,
            retrieval,
            maintenance,
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

/// TERMINAL states (§15.2) need `settled_at` (0007's CHECK); every other state needs it NULL.
fn seed_log_row(admin: &mut Client, key: &StreamKey, seq: i64, state: &str, issued_at: SystemTime) {
    let settled_at: Option<SystemTime> = matches!(
        state,
        "DONE" | "SKIPPED_BY_POLICY" | "FAILED" | "TOMBSTONED"
    )
    .then(SystemTime::now);
    admin
        .execute(
            "INSERT INTO projection.stream_log \
               (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, \
                stream_seq, commit_seq, state, issued_at, settled_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$7,$8,$9,$10)",
            &[
                &key.tenant_id.0,
                &key.scope_kind,
                &key.scope_id,
                &key.domain,
                &key.projection_kind,
                &key.projection_version,
                &seq,
                &state,
                &issued_at,
                &settled_at,
            ],
        )
        .expect("seed stream_log row");
}

fn seed_job(
    admin: &mut Client,
    tenant_id: Uuid,
    stream_key_text: &str,
    stream_seq: i64,
    status: &str,
) {
    admin
        .execute(
            "INSERT INTO ops.jobs (tenant_id, job_type, idempotency_key, status, stream_key, stream_seq) \
             VALUES ($1, 'test.stream_patrol', $2, $3, $4, $5)",
            &[
                &tenant_id,
                &format!("stream-patrol-{}", Uuid::new_v4()),
                &status,
                &stream_key_text,
                &stream_seq,
            ],
        )
        .expect("seed ops.jobs row");
}

fn log_state(admin: &mut Client, key: &StreamKey, seq: i64) -> String {
    admin
        .query_one(
            "SELECT state FROM projection.stream_log \
             WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND domain=$4 \
               AND projection_kind=$5 AND projection_version=$6 AND stream_seq=$7",
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
        .expect("row must exist")
        .get(0)
}

/// §15.4 worked example: seq 1..99 `DONE`, seq 100 `FAILED`, seq 101 `DONE` — the watermark
/// can only advance to 99, never past the gap at 100 even though 101 already settled OK
/// (§15.7 "禁 watermark 跨未知 gap"). Pins both `advance_prefix`'s return value and the actual
/// `stream_checkpoints.projection_highwater` row it writes.
#[test]
fn advance_prefix_stops_before_the_gap_100_failed_101_done() {
    run_db_fixture::<StreamFixture, _>(
        "advance_prefix_stops_before_the_gap_100_failed_101_done",
        |mut handle| {
            let k = key(&handle);
            let now = SystemTime::now();
            for seq in 1..=99i64 {
                seed_log_row(&mut handle.admin, &k, seq, "DONE", now);
            }
            seed_log_row(&mut handle.admin, &k, 100, "FAILED", now);
            seed_log_row(&mut handle.admin, &k, 101, "DONE", now);
            seed_checkpoint(&mut handle.admin, &k, 101);

            let n = handle
                .rt
                .block_on(stream_repo::advance_prefix(&handle.retrieval, &k))
                .expect("consistent ledger must advance");
            assert_eq!(
                n, 99,
                "highwater must stop at 99, not skip past seq 100's gap"
            );

            let written: i64 = handle
                .admin
                .query_one(
                    "SELECT projection_highwater FROM projection.stream_checkpoints \
                     WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND domain=$4 \
                       AND projection_kind=$5 AND projection_version=$6",
                    &[
                        &k.tenant_id.0,
                        &k.scope_kind,
                        &k.scope_id,
                        &k.domain,
                        &k.projection_kind,
                        &k.projection_version,
                    ],
                )
                .expect("checkpoint row must exist")
                .get(0);
            assert_eq!(written, 99);
        },
    );
}

/// Fault injection, identity side (a): `expected == done + open_gaps + pending`. A
/// stream_log row for seq 100 is never written at all (simulates the class of bug §15's
/// intro names — an `INSERT` silently skipped/lost — not representable as any `state`, so it
/// can't be caught by a state-based check) while the checkpoint still claims
/// `issued_highwater = 101` and `MAX(stream_seq) = 101` (seq 101 does exist) — so side (b)
/// (`expected == max_stream_seq`) holds while side (a) alone breaks (`done=100 + gaps=0 +
/// pending=0 = 100 != 101`). Without the identity check this would silently compute some
/// `contiguous_done_prefix` and write it (red); with it, `advance_prefix` refuses (green).
#[test]
fn advance_prefix_inconsistent_when_sum_identity_breaks() {
    run_db_fixture::<StreamFixture, _>(
        "advance_prefix_inconsistent_when_sum_identity_breaks",
        |mut handle| {
            let k = key(&handle);
            let now = SystemTime::now();
            for seq in 1..=99i64 {
                seed_log_row(&mut handle.admin, &k, seq, "DONE", now);
            }
            // seq 100 deliberately never inserted.
            seed_log_row(&mut handle.admin, &k, 101, "DONE", now);
            seed_checkpoint(&mut handle.admin, &k, 101);

            let err = handle
                .rt
                .block_on(stream_repo::advance_prefix(&handle.retrieval, &k))
                .expect_err("expected==max_stream_seq holds but expected!=done+gaps+pending");
            assert!(matches!(err, AdvanceError::Inconsistent));
        },
    );
}

/// Fault injection, identity side (b): `expected == max_stream_seq`. 101 rows exist and are
/// all `DONE` (so side (a) holds: `done=101 == expected`), but one of them sits at seq 200
/// instead of being contiguous — `MAX(stream_seq) = 200 != expected = 101`. Same "without the
/// check this silently proceeds" logic as the sibling test above, isolating the other half of
/// the identity.
#[test]
fn advance_prefix_inconsistent_when_max_seq_disagrees_with_expected() {
    run_db_fixture::<StreamFixture, _>(
        "advance_prefix_inconsistent_when_max_seq_disagrees_with_expected",
        |mut handle| {
            let k = key(&handle);
            let now = SystemTime::now();
            for seq in 1..=100i64 {
                seed_log_row(&mut handle.admin, &k, seq, "DONE", now);
            }
            seed_log_row(&mut handle.admin, &k, 200, "DONE", now); // 101st row, out-of-range seq
            seed_checkpoint(&mut handle.admin, &k, 101);

            let err = handle
                .rt
                .block_on(stream_repo::advance_prefix(&handle.retrieval, &k))
                .expect_err("done+gaps+pending==expected holds but max_stream_seq!=expected");
            assert!(matches!(err, AdvanceError::Inconsistent));
        },
    );
}

/// §15.2: `WAITING_KEY` "可以持续数小时/数天而仍是已知 blocked，不是 LOST" — the patrol's
/// `WHERE s.state = 'ISSUED'` guard must exclude it structurally, not just by SLA math. Seeds
/// a `WAITING_KEY` row issued 6 hours ago (far past the 15-minute SLA) and asserts
/// `sweep_lost` leaves it untouched.
#[test]
fn sweep_lost_never_touches_waiting_key_regardless_of_age() {
    run_db_fixture::<StreamFixture, _>(
        "sweep_lost_never_touches_waiting_key_regardless_of_age",
        |mut handle| {
            let k = key(&handle);
            let six_hours_ago = SystemTime::now() - Duration::from_secs(6 * 3600);
            seed_log_row(&mut handle.admin, &k, 1, "WAITING_KEY", six_hours_ago);
            seed_checkpoint(&mut handle.admin, &k, 1);

            let tenant_id = handle.tenant_id;
            handle
                .rt
                .block_on(stream_repo::sweep_lost(
                    &handle.maintenance,
                    tenant_id,
                    Duration::from_secs(900),
                ))
                .expect("sweep must run");

            assert_eq!(log_state(&mut handle.admin, &k, 1), "WAITING_KEY");
        },
    );
}

/// §15.2 orphan patrol, positive and negative together: an `ISSUED` row past SLA with no
/// matching in-flight `ops.jobs` row (simulating a worker that died before it could write its
/// own gap — "kill before writing a gap") becomes `LOST` with `error_class =
/// 'ORPHANED_PIPELINE_ITEM'`; a same-age `ISSUED` row that *does* have a matching `PROCESSING`
/// job survives untouched (its owner is still alive by the patrol's own definition); a
/// same-key `ISSUED` row younger than the SLA survives regardless of ownership (not yet
/// eligible). The owned row's `ops.jobs.stream_key` is built from
/// `StreamKey::stream_key_text()` — this is also the round-trip proof that `sweep_lost`'s
/// in-SQL text concatenation matches that Rust encoding exactly.
#[test]
fn sweep_lost_sweeps_orphan_but_spares_owned_and_fresh_issued() {
    run_db_fixture::<StreamFixture, _>(
        "sweep_lost_sweeps_orphan_but_spares_owned_and_fresh_issued",
        |mut handle| {
            let k = key(&handle);
            let old = SystemTime::now() - Duration::from_secs(20 * 60); // 20 min > 15 min SLA
            let fresh = SystemTime::now() - Duration::from_secs(30); // well under SLA

            seed_log_row(&mut handle.admin, &k, 1, "ISSUED", old); // orphan -> LOST
            seed_log_row(&mut handle.admin, &k, 2, "ISSUED", old); // owned -> stays ISSUED
            seed_log_row(&mut handle.admin, &k, 3, "ISSUED", fresh); // too young -> stays ISSUED
            seed_checkpoint(&mut handle.admin, &k, 3);

            seed_job(
                &mut handle.admin,
                handle.tenant_id,
                &k.stream_key_text(),
                2,
                "PROCESSING",
            );

            let tenant_id = handle.tenant_id;
            handle
                .rt
                .block_on(stream_repo::sweep_lost(
                    &handle.maintenance,
                    tenant_id,
                    Duration::from_secs(900),
                ))
                .expect("sweep must run");

            assert_eq!(log_state(&mut handle.admin, &k, 1), "LOST");
            assert_eq!(log_state(&mut handle.admin, &k, 2), "ISSUED");
            assert_eq!(log_state(&mut handle.admin, &k, 3), "ISSUED");

            let error_class: Option<String> = handle
                .admin
                .query_one(
                    "SELECT error_class FROM projection.stream_log \
                     WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND domain=$4 \
                       AND projection_kind=$5 AND projection_version=$6 AND stream_seq=1",
                    &[
                        &k.tenant_id.0,
                        &k.scope_kind,
                        &k.scope_id,
                        &k.domain,
                        &k.projection_kind,
                        &k.projection_version,
                    ],
                )
                .expect("row must exist")
                .get(0);
            assert_eq!(error_class.as_deref(), Some("ORPHANED_PIPELINE_ITEM"));
        },
    );
}
