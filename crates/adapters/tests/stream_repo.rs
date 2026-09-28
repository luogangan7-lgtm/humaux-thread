//! `adapters::tests::stream_repo` — T3.3+T3.4 integration test — `stream_repo` (§15) against a real Postgres.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-projection, humaux-retrieval, humaux-testkit, postgres,
//!   sqlx, tokio]; services=[PostgreSQL(owner) r=[projection.processing_gaps] w=[control.tenants, ops.jobs,
//!   projection.stream_checkpoints, projection.stream_log], PostgreSQL(role_maintenance),
//!   PostgreSQL(role_retrieval_worker)]; env=[HUMAUX_TEST_PG_DSN]; modules=[adapters::postgres, adapters::retrieve,
//!   adapters::stream_repo, domain::egress, domain::ids, humaux-testkit, projection::stream, retrieval::completeness,
//!   retrieval::envelope]
//! Called-by: [cargo-test]
//! Invariants: [advance_prefix/sweep_lost run per tenant under FORCE RLS on real projection tables scoped to a
//!   throwaway tenant; no DSN, unreachable DB or projection.stream_log missing is a visible SKIP]
//! Spec: Baseline §79.2
//!
//! Same convention
//! as `jobs_claim.rs`/`email_outbox.rs`: the tables are shared (`0007_projection.sql`,
//! `0008_ops_core.sql`), not a scratch schema, so every test scopes rows to a throwaway
//! `control.tenants` row this file owns and cleans up on `Drop`.
//!
//! Three-state skip (§79.2): no DSN, unreachable DB, or `projection.stream_log` missing all
//! print a visible SKIP and return.

use std::time::{Duration, SystemTime};

use humaux_adapters::postgres::{MaintenanceDbPool, RetrievalWorkerDbPool};
use humaux_adapters::stream_repo::{self, AdvanceError};
use humaux_domain::egress::ProcessorId;
use humaux_domain::ids::TenantId;
use humaux_projection::stream::StreamKey;
use humaux_retrieval::completeness::LedgerClosure;
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;

// `sweep_lost` is scoped to one `tenant_id` (RLS has no cross-tenant carve-out for
// `role_maintenance` — see `stream_repo`'s module doc), and every test below seeds its own
// throwaway tenant, so no serialization guard is needed here: two tests' sweeps can never
// touch each other's rows even running concurrently.

/// Card 21 fix pass: two §7.4 worker identities. `advance_prefix` writes the caller's into
/// `projection.stream_checkpoints.projection_processor_id` (migration 0171) — the column that
/// makes "a checkpoint written by one worker is attributed to it" expressible at all.
const WORKER_A: Uuid = Uuid::from_u128(0x0171_00a0);
const WORKER_B: Uuid = Uuid::from_u128(0x0171_00b0);

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
        // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
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
            // dep: PostgreSQL(role_retrieval_worker) — test opens a direct PG connection for setup/verification
            .block_on(RetrievalWorkerDbPool::connect(&dsn_as_role(
                &dsn,
                "role_retrieval_worker",
            )))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let maintenance = rt
            // dep: PostgreSQL(role_maintenance) — test opens a direct PG connection for setup/verification
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
                .block_on(stream_repo::advance_prefix(
                    &handle.retrieval,
                    &k,
                    ProcessorId(WORKER_A),
                ))
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

/// Card 21 fix pass (reviewer P1): the card's own acceptance asks for "a checkpoint written by
/// one worker is attributed to it". Before migration 0171 the table had no column that could
/// carry the answer and the assertion was quietly replaced by an `ops.data_disclosures` one;
/// this is the assertion the card actually asked for. Two workers advance the SAME checkpoint
/// in turn, and each time the row names the one that moved it — not the first, not NULL.
///
/// Fault injection, measured (2026-09-16): dropping `projection_processor_id = $8` from
/// `advance_prefix`'s UPDATE makes the first `expect("attributed")` panic on a NULL. Writing it
/// in a second statement outside the monotonic `WHERE` is the other failure this pins — then the
/// second assertion (B, not A) is what catches a row whose name and number disagree.
#[test]
fn a_checkpoint_carries_the_processor_id_of_the_worker_that_advanced_it() {
    run_db_fixture::<StreamFixture, _>(
        "a_checkpoint_carries_the_processor_id_of_the_worker_that_advanced_it",
        |mut handle| {
            let k = key(&handle);
            let now = SystemTime::now();
            for seq in 1..=3i64 {
                seed_log_row(&mut handle.admin, &k, seq, "DONE", now);
            }
            seed_checkpoint(&mut handle.admin, &k, 3);

            // Nothing has advanced this checkpoint yet: attribution is NULL, never a
            // placeholder identity (0171 deliberately back-fills nothing).
            assert_eq!(
                attribution(&mut handle.admin, &k),
                None,
                "a checkpoint nobody advanced must not name a processor"
            );

            let n = handle
                .rt
                .block_on(stream_repo::advance_prefix(
                    &handle.retrieval,
                    &k,
                    ProcessorId(WORKER_A),
                ))
                .expect("worker A advances");
            assert_eq!(n, 3);
            assert_eq!(
                attribution(&mut handle.admin, &k).expect("attributed"),
                WORKER_A,
                "the checkpoint must name the worker that wrote it"
            );

            // A second, distinct worker moves the same checkpoint further.
            for seq in 4..=5i64 {
                seed_log_row(&mut handle.admin, &k, seq, "DONE", now);
            }
            handle
                .admin
                .execute(
                    "UPDATE projection.stream_checkpoints SET issued_highwater=5 \
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
                .expect("bump issued_highwater");

            let n = handle
                .rt
                .block_on(stream_repo::advance_prefix(
                    &handle.retrieval,
                    &k,
                    ProcessorId(WORKER_B),
                ))
                .expect("worker B advances");
            assert_eq!(n, 5);
            assert_eq!(
                attribution(&mut handle.admin, &k).expect("attributed"),
                WORKER_B,
                "the checkpoint must name the LAST worker that moved it, not the first"
            );
            assert_ne!(WORKER_A, WORKER_B, "the two identities must be distinct");
        },
    );
}

/// `projection.stream_checkpoints.projection_processor_id` for one key (migration 0171).
fn attribution(admin: &mut Client, key: &StreamKey) -> Option<Uuid> {
    admin
        .query_one(
            "SELECT projection_processor_id FROM projection.stream_checkpoints \
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
        .expect("checkpoint row must exist")
        .get(0)
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
                .block_on(stream_repo::advance_prefix(
                    &handle.retrieval,
                    &k,
                    ProcessorId(WORKER_A),
                ))
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
                .block_on(stream_repo::advance_prefix(
                    &handle.retrieval,
                    &k,
                    ProcessorId(WORKER_A),
                ))
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

// =============================================================================================
// §15.2.1 / §15.4 — the audited FAILED -> RETIRED_FAILED retirement (migration 0167, ADR-0042).
// =============================================================================================

/// Seeds the §15.7 worked example in miniature — seq 1 `DONE`, seq 2 `FAILED` with a named
/// `error_class`, seq 3 `DONE` — plus the checkpoint that claims all three were issued.
fn seed_one_failed_between_two_done(handle: &mut Handle, k: &StreamKey, failure_class: &str) {
    let now = SystemTime::now();
    seed_log_row(&mut handle.admin, k, 1, "DONE", now);
    seed_log_row(&mut handle.admin, k, 2, "FAILED", now);
    seed_log_row(&mut handle.admin, k, 3, "DONE", now);
    // `settle_row` writes the class alongside the state; the seed helper only writes the state,
    // so the class is set here. Not a transition (state is unchanged), so the 0011/0167 guard
    // returns early and this stays a plain owner UPDATE.
    handle
        .admin
        .execute(
            "UPDATE projection.stream_log SET error_class = $8 \
             WHERE tenant_id=$1 AND scope_kind=$2 AND scope_id=$3 AND domain=$4 \
               AND projection_kind=$5 AND projection_version=$6 AND stream_seq=$7",
            &[
                &k.tenant_id.0,
                &k.scope_kind,
                &k.scope_id,
                &k.domain,
                &k.projection_kind,
                &k.projection_version,
                &2i64,
                &failure_class,
            ],
        )
        .expect("tag the failed ticket with its error_class");
    seed_checkpoint(&mut handle.admin, k, 3);
}

/// The whole point of the state (card 18's debt 1, card 20's migration 0167): a settled `FAILED`
/// ticket pins §15.4's contiguous prefix at 1 no matter how many later seqs settle OK, and the
/// audited retirement releases it to 3.
///
/// This asserts the WRITE path's copy (`stream_repo::fetch_ledger_snapshot`'s four-number
/// cross-check). The read path's copy moves with it by construction — both interpolate
/// `retrieve::SETTLED_OK_SQL_LIST`, pinned against the enum by
/// `retrieve::contract_tests::settled_ok_sql_list_matches_the_enum` — and is asserted live in
/// `retrieve_read_your_writes.rs::retired_seq_leaves_the_overlay_and_enters_the_contiguous_prefix`,
/// which needs the gateway pool this fixture does not carry. Card 18 stored "fix only the
/// reader's copy" as a `rejected` decision; this is the other half of honouring it.
///
/// This test is its own fault control: take the `retire_failed` call away and the post-retirement
/// assertions read the pre-retirement numbers (prefix 1, `open_gaps` 1) and go red.
#[test]
fn retirement_unpins_the_contiguous_done_prefix_in_both_copies() {
    run_db_fixture::<StreamFixture, _>(
        "retirement_unpins_the_contiguous_done_prefix_in_both_copies",
        |mut handle| {
            let k = key(&handle);
            seed_one_failed_between_two_done(&mut handle, &k, "distill_failed");

            let before = handle
                .rt
                .block_on(stream_repo::fetch_ledger_snapshot(&handle.retrieval, &k))
                .expect("snapshot must read");
            assert_eq!(
                before.contiguous_done_prefix, 1,
                "§15.7: the FAILED row at seq 2 pins the prefix at 1, even though seq 3 settled OK"
            );
            assert_eq!(
                before.open_gaps, 1,
                "the FAILED row is a processing_gaps row"
            );
            assert_eq!(before.done, 2);
            assert_eq!(
                handle
                    .rt
                    .block_on(stream_repo::advance_prefix(
                        &handle.retrieval,
                        &k,
                        ProcessorId(WORKER_A)
                    ))
                    .expect("consistent ledger"),
                1
            );

            let retired = handle
                .rt
                .block_on(stream_repo::retire_failed(
                    &handle.maintenance,
                    &k,
                    "distill_failed",
                ))
                .expect("role_maintenance may retire through the 0167 definer");
            assert_eq!(retired, vec![2], "exactly the exhausted ticket");

            let after = handle
                .rt
                .block_on(stream_repo::fetch_ledger_snapshot(&handle.retrieval, &k))
                .expect("snapshot must read");
            assert_eq!(
                after.contiguous_done_prefix, 3,
                "RETIRED_FAILED is SETTLED_OK, so the prefix runs to the end of the stream"
            );
            assert_eq!(
                after.open_gaps, 0,
                "the retired row leaves processing_gaps because its state changed (§15.2.1) — \
                 the view itself is untouched"
            );
            assert_eq!(after.done, 3, "settled, so it counts toward done");
            assert_eq!(
                handle
                    .rt
                    .block_on(stream_repo::advance_prefix(
                        &handle.retrieval,
                        &k,
                        ProcessorId(WORKER_A)
                    ))
                    .expect("identity still holds after retirement"),
                3,
                "§15.4's four-number identity must still close after a retirement"
            );
        },
    );
}

/// §23.1② A2 after a retirement — the read-side half of 0167 that the prefix tests do not see.
///
/// A2 is `visible + deleted + skipped == done`, so every state inside `done` needs a home on the
/// left. A1 (`done + open_gaps + pending == expected`) leaves `RETIRED_FAILED` nowhere but
/// `done`, and the retired record is by construction never in the index, so without a left-hand
/// term claiming it `lhs` sits one BELOW `done` forever ⇒ `A2Closure::InvisibleLoss` ⇒
/// `abstain(ProjectionInvisibleLoss)` on every later recall of that stream. §23.1② (amended with
/// 0167's citation) puts it on `skipped`: settled, never indexable, but not a §37 deletion.
///
/// Its own fault control: drop `RETIRED_FAILED` from `close_ledger_in_txn`'s `skipped` filter and
/// the post-retirement A2 assertion below goes red (`skipped` reads 0, `lhs` 2 vs `done` 3).
#[test]
fn a_retired_ticket_keeps_the_a2_closure_shut() {
    run_db_fixture::<StreamFixture, _>(
        "a_retired_ticket_keeps_the_a2_closure_shut",
        |mut handle| {
            let k = key(&handle);
            seed_one_failed_between_two_done(&mut handle, &k, "distill_failed");
            handle
                .rt
                .block_on(stream_repo::retire_failed(
                    &handle.maintenance,
                    &k,
                    "distill_failed",
                ))
                .expect("retire");

            let closure = handle
                .rt
                .block_on(stream_repo::fetch_ledger_closure(&handle.retrieval, &k))
                .expect("the real envelope-side counting query, not a fixture copy");
            let counts = match &closure {
                LedgerClosure::Closed(counts) => counts,
                LedgerClosure::Broken(_) => panic!("A1 must still close after a retirement"),
            };
            assert_eq!(counts.done(), 3);
            assert_eq!(counts.deleted(), 0, "a retirement is not a §37 deletion");
            assert_eq!(
                counts.skipped(),
                1,
                "the retired row is the `skipped` term's second member (§23.1②/0167)"
            );

            // `visible` = the two DONE rows: the retired record was never indexed (0167 header).
            let block = humaux_retrieval::envelope::build_projection_block(&closure, Some(2));
            assert!(
                block.degradations.is_empty(),
                "a retirement must not read as ProjectionInvisibleLoss: {:?}",
                block.degradations
            );
            assert!(
                block.value.current,
                "open_gaps == 0 and A2 closed ⇒ current"
            );
            assert_eq!(
                block.value.completeness_ratio,
                Some(2.0 / 3.0),
                "§23.1②'s recorded cost: the retired record stays in the denominator, so the \
                 ratio is honestly below 1.0 while `current` stays true"
            );
        },
    );
}

/// Audit (§15.2.1): who / when, with the failure class preserved rather than overwritten. The
/// row is settled, and `settled_at` is the ORIGINAL settlement time — retirement records a second
/// decision, it does not rewrite the first.
#[test]
fn a_retired_ticket_records_who_when_and_keeps_its_failure_class() {
    run_db_fixture::<StreamFixture, _>(
        "a_retired_ticket_records_who_when_and_keeps_its_failure_class",
        |mut handle| {
            let k = key(&handle);
            seed_one_failed_between_two_done(&mut handle, &k, "qdrant_upsert_failed");
            let settled_before: SystemTime = handle
                .admin
                .query_one(
                    "SELECT settled_at FROM projection.stream_log \
                     WHERE tenant_id=$1 AND stream_seq=2",
                    &[&k.tenant_id.0],
                )
                .expect("row must exist")
                .get(0);

            handle
                .rt
                .block_on(stream_repo::retire_failed(
                    &handle.maintenance,
                    &k,
                    "qdrant_upsert_failed",
                ))
                .expect("retire");

            let row = handle
                .admin
                .query_one(
                    "SELECT state, error_class, retired_by, retired_at, settled_at \
                     FROM projection.stream_log WHERE tenant_id=$1 AND stream_seq=2",
                    &[&k.tenant_id.0],
                )
                .expect("row must exist");
            assert_eq!(row.get::<_, String>(0), "RETIRED_FAILED");
            assert_eq!(
                row.get::<_, Option<String>>(1).as_deref(),
                Some("qdrant_upsert_failed"),
                "the failure class is the audit's third field, never overwritten"
            );
            // `retired_by` is `session_user`, taken inside the definer. It must be the CALLER's
            // authenticated principal, never `role_migration_owner`: inside a SECURITY DEFINER
            // function `current_user` IS the owner, so recording that would make every retirement
            // look self-authorised. (In production the maintenance process logs in as
            // `role_maintenance`; this fixture reaches that role with libpq's `options=-c role=`,
            // which changes `current_user` and leaves `session_user` at the login role — so the
            // assertion is on the property that matters, not on one deployment's login name.)
            let retired_by = row.get::<_, Option<String>>(2);
            let retired_by = retired_by.as_deref().expect("retired_by must be recorded");
            assert_ne!(
                retired_by, "role_migration_owner",
                "retired_by must be the caller, not the definer's owner"
            );
            assert!(!retired_by.is_empty());
            assert!(row.get::<_, Option<SystemTime>>(3).is_some(), "retired_at");
            assert_eq!(
                row.get::<_, SystemTime>(4),
                settled_before,
                "retirement must not rewrite the original settlement time"
            );
        },
    );
}

/// The audit is a precondition, not a label: naming a class the ticket does not carry retires
/// nothing at all. "Retire whatever failed" is the blanket action §15.2.1 rejects — it would let
/// a transient `qdrant_upsert_failed` disappear behind a policy written for `distill_failed`.
#[test]
fn retirement_refuses_a_ticket_whose_failure_class_was_not_named() {
    run_db_fixture::<StreamFixture, _>(
        "retirement_refuses_a_ticket_whose_failure_class_was_not_named",
        |mut handle| {
            let k = key(&handle);
            seed_one_failed_between_two_done(&mut handle, &k, "qdrant_upsert_failed");

            let retired = handle
                .rt
                .block_on(stream_repo::retire_failed(
                    &handle.maintenance,
                    &k,
                    "distill_failed",
                ))
                .expect("a class mismatch is 0 rows, not an error");
            assert!(retired.is_empty());
            assert_eq!(log_state(&mut handle.admin, &k, 2), "FAILED");
            assert_eq!(
                handle
                    .rt
                    .block_on(stream_repo::fetch_ledger_snapshot(&handle.retrieval, &k))
                    .expect("snapshot")
                    .contiguous_done_prefix,
                1,
                "nothing was retired, so nothing moved"
            );
        },
    );
}

/// §6.2.2 / §15.2.1: `role_retrieval_worker` holds table-level UPDATE on `projection.stream_log`
/// and is the role that WRITES `FAILED`, so the wall that keeps it from settling its own failure
/// OK is the 0011/0167 transition trigger, not a missing grant. Direct `UPDATE ... SET state =
/// 'RETIRED_FAILED'` must raise `check_violation` for it and for `role_maintenance` alike — the
/// definer function is the only door, for everybody.
#[test]
fn only_the_definer_function_can_reach_retired_failed() {
    run_db_fixture::<StreamFixture, _>(
        "only_the_definer_function_can_reach_retired_failed",
        |mut handle| {
            let k = key(&handle);
            seed_one_failed_between_two_done(&mut handle, &k, "distill_failed");

            for role in ["role_retrieval_worker", "role_maintenance"] {
                let error = handle
                    .admin
                    .batch_execute(&format!(
                        // dep: PostgreSQL(owner) — test switches PG role to exercise RLS
                        "BEGIN; \
                         SET LOCAL ROLE {role}; \
                         SET LOCAL humaux.tenant_id = '{tenant}'; \
                         UPDATE projection.stream_log SET state = 'RETIRED_FAILED' \
                          WHERE tenant_id = '{tenant}' AND stream_seq = 2; \
                         COMMIT;",
                        tenant = k.tenant_id.0,
                    ))
                    .expect_err("the transition guard must refuse a direct write");
                assert_eq!(
                    error.code(),
                    Some(&postgres::error::SqlState::CHECK_VIOLATION),
                    "{role} must be refused by stream_log_guard_state_transition, got: {error}"
                );
                handle.admin.batch_execute("ROLLBACK").ok();
            }
            assert_eq!(log_state(&mut handle.admin, &k, 2), "FAILED");
        },
    );
}

/// §78.2 DB enum <-> Rust enum contract, against the DEPLOYED constraint rather than a migration
/// file: `ProcessingState::ALL` must be exactly `projection.stream_log`'s `state` CHECK, both
/// directions. Card note (e): accept both deparse forms — PostgreSQL renders `IN (…)` and
/// `= ANY (ARRAY[…])` identically, so the parser keys on the quoted literals, not the spelling.
#[test]
fn deployed_state_check_mirrors_processing_state_all() {
    run_db_fixture::<StreamFixture, _>(
        "deployed_state_check_mirrors_processing_state_all",
        |mut handle| {
            let def: String = handle
                .admin
                .query_one(
                    "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
                     WHERE conrelid = 'projection.stream_log'::regclass \
                       AND conname = 'stream_log_state_check'",
                    &[],
                )
                .expect("stream_log_state_check must exist")
                .get(0);
            assert!(
                def.contains("IN (") || def.contains("= ANY (ARRAY["),
                "state CHECK must still be a closed set, got: {def}"
            );

            let mut in_db: Vec<String> = def
                .split('\'')
                .skip(1)
                .step_by(2)
                .map(str::to_string)
                .collect();
            in_db.sort_unstable();
            in_db.dedup();

            let mut in_rust: Vec<String> = humaux_adapters::retrieve::ProcessingState::ALL
                .into_iter()
                .map(|s| s.as_db_str().to_string())
                .collect();
            in_rust.sort_unstable();

            assert_eq!(
                in_db, in_rust,
                "projection.stream_log's state CHECK and ProcessingState::ALL have drifted — \
                 widen/narrow both in one migration (§78.2)"
            );
        },
    );
}
