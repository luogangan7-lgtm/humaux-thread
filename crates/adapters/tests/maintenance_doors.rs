//! `adapters::tests::maintenance_doors` — the four owner purge doors of migration 0218 (ADR-0062 D-E..D-J) and the
//!   single-statement manifest page read (D-H), against a real PostgreSQL throwaway database per test.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-testkit, postgres, sqlx, tokio];
//!   services=[PostgreSQL(owner) r=[ops.commit_seq_seq, ops.maintenance_receipts] w=[control.confirm_tokens,
//!   control.private_reasoning_domains, control.rate_buckets, control.tenants, control.users, control.workspaces,
//!   ops.contribution_execution_job_links, ops.distill_calls, ops.jobs, ops.outbox,
//!   ops.selection_snapshot_items, ops.selection_snapshots, private.evidence_objects] x=[ops.admit_distill_budget,
//!   ops.requeue_dead_distill], PostgreSQL(role_gateway), PostgreSQL(role_maintenance)
//!   x=[control.purge_idle_rate_buckets, control.sweep_confirm_tokens, ops.purge_expired_selection_snapshots,
//!   ops.purge_terminal_jobs], PostgreSQL(role_retrieval_worker)];
//!   env=[HUMAUX_TEST_PG_DSN]; modules=[adapters::confirm_token_repo, adapters::maintenance_repo,
//!   adapters::postgres, adapters::quota_repo, adapters::selection_repo, adapters::tests::support::throwaway_db,
//!   domain::error, domain::selection, humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [every test owns its throwaway database humaux_thread_c35_doors_<pid>_<n>, created and migrated by
//!   the fixture and dropped WITH (FORCE) by its Drop even on panic, so no purge ever runs on the shared dev
//!   database (ruling E8); every door is called the way the daemon calls it: role_maintenance, one transaction,
//!   the tenant GUC, one statement; no DSN or unreachable DB is a visible §79.2 skip]
//! Spec: Baseline §6.2.1; §20.4; §79.2; ADR-0058 R4; ADR-0062 D-E; ADR-0062 D-F; ADR-0062 D-G; ADR-0062 D-H;
//!   ADR-0062 D-I; ADR-0062 D-J

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use humaux_adapters::confirm_token_repo;
use humaux_adapters::maintenance_repo::{self, JobRetention};
use humaux_adapters::postgres::{MaintenanceDbPool, RetrievalWorkerDbPool, RuntimeDbPool};
use humaux_adapters::quota_repo::{self, RatePolicy, RateSubject};
use humaux_adapters::selection_repo::{self, SelectionRepoError};
use humaux_domain::error::ErrorCode;
use humaux_domain::selection::{Cursor, query_fingerprint};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, IsolationLevel, NoTls};
use sqlx::types::Uuid;

#[path = "support/throwaway_db.rs"]
#[allow(dead_code)]
mod throwaway_db;
use throwaway_db::ThrowawayDb;

const MAC_KEY: &[u8] = b"c35 maintenance doors test mac key";
const HOUR: Duration = Duration::from_secs(3600);

fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

/// Fields drop in order: the pools and the owner connection close before `_db` drops the database.
struct Handle {
    rt: tokio::runtime::Runtime,
    maintenance: MaintenanceDbPool,
    retrieval: RetrievalWorkerDbPool,
    runtime: RuntimeDbPool,
    admin: Client,
    dsn: String,
    tenant: Uuid,
    other: Uuid,
    _db: ThrowawayDb,
}

struct DoorsFixture;

impl DbIntegrationFixture for DoorsFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let setup = |e: String| DbFixtureSkipReason::IsolationSetupFailed(e);
        let db = throwaway_db::create("c35_doors")?;
        let dsn = db.dsn();
        // dep: PostgreSQL(owner) — fixture connection to this test's throwaway database
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
        let mut tenant = |name: &str| -> Result<Uuid, DbFixtureSkipReason> {
            Ok(admin
                .query_one(
                    "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                    &[&name],
                )
                .map_err(|e| setup(e.to_string()))?
                .get(0))
        };
        let (tenant, other) = (tenant("c35 doors tenant")?, tenant("c35 doors bystander")?);
        let rt = tokio::runtime::Runtime::new().map_err(|e| setup(e.to_string()))?;
        // dep: PostgreSQL(role_maintenance) — the doors' only caller role
        let maintenance = rt
            .block_on(MaintenanceDbPool::connect(&dsn_as_role(
                &dsn,
                "role_maintenance",
            )))
            .map_err(|e| setup(e.to_string()))?;
        // dep: PostgreSQL(role_retrieval_worker) — the manifest page reader
        let retrieval = rt
            .block_on(RetrievalWorkerDbPool::connect(&dsn_as_role(
                &dsn,
                "role_retrieval_worker",
            )))
            .map_err(|e| setup(e.to_string()))?;
        // dep: PostgreSQL(role_gateway) — the rate-bucket consumer
        let runtime = rt
            .block_on(RuntimeDbPool::connect(&dsn_as_role(&dsn, "role_gateway")))
            .map_err(|e| setup(e.to_string()))?;
        Ok(Handle {
            rt,
            maintenance,
            retrieval,
            runtime,
            admin,
            dsn,
            tenant,
            other,
            _db: db,
        })
    }
}

impl Handle {
    fn count(&mut self, sql: &str, tenant: Uuid) -> i64 {
        self.admin
            .query_one(sql, &[&tenant])
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .get(0)
    }

    /// `(affected, row_limit)` of every receipt of `task` for `tenant`, oldest first.
    fn receipts(&mut self, tenant: Uuid, task: &str) -> Vec<(i64, i32)> {
        self.admin
            .query(
                "SELECT affected, row_limit FROM ops.maintenance_receipts \
                 WHERE tenant_id = $1 AND task = $2 ORDER BY ran_at",
                &[&tenant, &task],
            )
            .expect("read receipts")
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect()
    }

    fn purge_snapshots(&self, tenant: Uuid, limit: i32) -> i64 {
        self.rt
            .block_on(maintenance_repo::purge_expired_selection_snapshots(
                &self.maintenance,
                tenant,
                limit,
            ))
            .expect("snapshot door")
    }

    fn purge_buckets(&self, tenant: Uuid, idle: Duration, limit: i32) -> i64 {
        self.rt
            .block_on(maintenance_repo::purge_idle_rate_buckets(
                &self.maintenance,
                tenant,
                idle,
                limit,
            ))
            .expect("rate-bucket door")
    }

    fn purge_jobs(&self, retention: JobRetention, limit: i32) -> Result<i64, sqlx::Error> {
        self.rt.block_on(maintenance_repo::purge_terminal_jobs(
            &self.maintenance,
            self.tenant,
            retention,
            limit,
        ))
    }

    /// One confirm token of `tenant` labelled `operation`, expiring `expires_in` seconds from now (negative: past).
    fn seed_token(&mut self, tenant: Uuid, operation: &str, expires_in: i32) {
        self.admin
            .execute(
                "WITH u AS (INSERT INTO control.users (user_id) VALUES (gen_random_uuid()) RETURNING user_id), \
                      w AS (INSERT INTO control.workspaces (tenant_id, name) VALUES ($1, 'c35 doors') \
                            RETURNING workspace_id) \
                 INSERT INTO control.confirm_tokens (tenant_id, user_id, operation, target_id, nonce_sha256, \
                   issued_at, expires_at, workspace_id) \
                 SELECT $1, u.user_id, $2, gen_random_uuid(), sha256(convert_to(gen_random_uuid()::text, 'UTF8')), \
                        now() - interval '1 day', now() + make_interval(secs => $3), w.workspace_id FROM u, w",
                &[&tenant, &operation, &f64::from(expires_in)],
            )
            .expect("seed confirm token");
    }

    fn tokens(&mut self, tenant: Uuid) -> Vec<String> {
        self.admin
            .query(
                "SELECT operation FROM control.confirm_tokens WHERE tenant_id = $1 ORDER BY operation",
                &[&tenant],
            )
            .expect("tokens")
            .iter()
            .map(|r| r.get(0))
            .collect()
    }

    /// One snapshot of `tenant` with `items` items, expiring `expires_in` seconds from now; the fingerprint is the
    /// enumerate predicate's, so a cursor signed for it validates.
    fn seed_snapshot(&mut self, tenant: Uuid, expires_in: i32, items: i32) -> Uuid {
        let fingerprint =
            query_fingerprint(selection_repo::ENUMERATE_ACTIVE_MEMORY_RECORDS_V1, tenant);
        let id: Uuid = self
            .admin
            .query_one(
                "INSERT INTO ops.selection_snapshots (tenant_id, query_fingerprint, expires_at) \
                 VALUES ($1, $2, now() + make_interval(secs => $3)) RETURNING selection_snapshot_id",
                &[&tenant, &fingerprint, &f64::from(expires_in)],
            )
            .expect("seed snapshot")
            .get(0);
        self.admin
            .execute(
                "INSERT INTO ops.selection_snapshot_items (selection_snapshot_id, tenant_id, item_id, ordinal) \
                 SELECT $1, $2, gen_random_uuid(), g - 1 FROM generate_series(1, $3) g",
                &[&id, &tenant, &items],
            )
            .expect("seed items");
        id
    }

    fn cursor(&self, snapshot: Uuid) -> Cursor {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_secs();
        Cursor::sign(
            snapshot,
            self.tenant,
            query_fingerprint(
                selection_repo::ENUMERATE_ACTIVE_MEMORY_RECORDS_V1,
                self.tenant,
            ),
            -1,
            i64::try_from(now).expect("unix time") + 600,
            MAC_KEY,
        )
    }

    fn page(&self, snapshot: Uuid) -> Result<Vec<Uuid>, SelectionRepoError> {
        self.rt
            .block_on(selection_repo::fetch_enumeration_page(
                &self.retrieval,
                self.tenant,
                &self.cursor(snapshot),
                10,
                MAC_KEY,
            ))
            .map(|page| page.items)
    }

    /// One `user` bucket of `tenant` (capacity, tokens, refill/s) last touched `idle_secs` ago.
    fn seed_bucket(
        &mut self,
        tenant: Uuid,
        kind: &str,
        subject: &str,
        bucket: (i64, i64, i64),
        idle_secs: i32,
    ) {
        let (capacity, tokens, refill) = bucket;
        self.admin
            .execute(
                "INSERT INTO control.rate_buckets (tenant_id, subject_kind, subject_id, operation, bucket_key, \
                   capacity, tokens, refill_per_second, updated_at) \
                 VALUES ($1, $2, $3, 'mcp.read', 'default', $4, $5::bigint, $6, \
                         now() - make_interval(secs => $7))",
                &[
                    &tenant,
                    &kind,
                    &subject,
                    &capacity,
                    &tokens,
                    &refill,
                    &f64::from(idle_secs),
                ],
            )
            .expect("seed rate bucket");
    }

    fn buckets(&mut self, tenant: Uuid) -> Vec<String> {
        self.admin
            .query(
                "SELECT subject_id FROM control.rate_buckets WHERE tenant_id = $1 ORDER BY subject_id",
                &[&tenant],
            )
            .expect("buckets")
            .iter()
            .map(|r| r.get(0))
            .collect()
    }

    /// One job of this tenant created `age_secs` ago.
    fn seed_job(&mut self, job_type: &str, status: &str, age_secs: i32) -> Uuid {
        self.admin
            .query_one(
                "INSERT INTO ops.jobs (tenant_id, job_type, status, idempotency_key, created_at) \
                 VALUES ($1, $2, $3, gen_random_uuid()::text, now() - make_interval(secs => $4)) \
                 RETURNING job_id",
                &[&self.tenant, &job_type, &status, &f64::from(age_secs)],
            )
            .expect("seed job")
            .get(0)
    }

    /// One provider call of `job`, begun `age_secs` ago.
    fn seed_call(&mut self, job: Uuid, age_secs: i32) {
        self.admin
            .execute(
                "INSERT INTO ops.distill_calls (model_call_id, tenant_id, job_id, claim_generation, attempt, \
                   begun_at) VALUES (gen_random_uuid(), $1, $2, 0, 1, now() - make_interval(secs => $3))",
                &[&self.tenant, &job, &f64::from(age_secs)],
            )
            .expect("seed distill call");
    }

    fn job_exists(&mut self, job: Uuid) -> bool {
        self.admin
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM ops.jobs WHERE job_id = $1)",
                &[&job],
            )
            .expect("job probe")
            .get(0)
    }

    /// One accepted Evidence whose EVIDENCE_ACCEPTED row has `outbox_status`, its 0164-enqueued distill job settled
    /// DEAD FAILED_OUTPUT_SCHEMA (the bins/maintenance onboarding seeder's shape). Returns the job.
    fn dead_distill_job(&mut self, outbox_status: &str) -> Uuid {
        let tenant = self.tenant;
        let mut txn = self.admin.transaction().expect("txn");
        let evidence: Uuid = txn
            .query_one(
                "WITH d AS (INSERT INTO control.private_reasoning_domains (tenant_id, name) \
                            VALUES ($1, 'c35 doors ' || gen_random_uuid()) RETURNING reasoning_domain_id) \
                 INSERT INTO private.evidence_objects (tenant_id, evidence_kind, payload_sha256, data_class, \
                   origin_class, visibility_class, reasoning_domain_id) \
                 SELECT $1, 'EVENT', sha256(convert_to(gen_random_uuid()::text, 'UTF8')), 'INTERNAL', \
                        'DirectUserInput', 'TENANT_SHARED', d.reasoning_domain_id FROM d \
                 RETURNING evidence_id",
                &[&tenant],
            )
            .expect("evidence")
            .get(0);
        txn.execute(
            "INSERT INTO ops.outbox (tenant_id, commit_seq, stream_seq, event_type, evidence_id) \
             VALUES ($1, nextval('ops.commit_seq_seq'), 1, 'EVIDENCE_ACCEPTED', $2)",
            &[&tenant, &evidence],
        )
        .expect("outbox");
        txn.execute(
            "UPDATE ops.outbox SET status = $2, processed_at = now() WHERE evidence_id = $1",
            &[&evidence, &outbox_status],
        )
        .expect("outbox status");
        let job: Uuid = txn
            .query_one(
                "UPDATE ops.jobs SET status = 'DEAD', attempt = 2, last_error_class = 'FAILED_OUTPUT_SCHEMA' \
                 WHERE job_type = 'DERIVED_DISTILL' AND payload ->> 'evidence_id' = $1::uuid::text \
                 RETURNING job_id",
                &[&evidence],
            )
            .expect("job DEAD")
            .get(0);
        txn.commit().expect("commit");
        job
    }
}

/// Retentions zero, window one hour: only the window and the keep-predicates hold a job back.
const ZERO_RETENTION: JobRetention = JobRetention {
    done: Duration::ZERO,
    dead: Duration::ZERO,
    budget_window: HOUR,
};

/// T-G1 (D-G): five expired, unconsumed tokens; a call with LIMIT 2 deletes the two oldest-expiring and writes one
/// receipt `(2, 2)`; the bystander tenant's expired token stays. Fault: drop the LIMIT ⇒ 5 deleted ⇒ red.
#[test]
fn confirm_sweep_deletes_at_most_limit_and_writes_one_receipt() {
    run_db_fixture::<DoorsFixture, _>(
        "confirm_sweep_deletes_at_most_limit_and_writes_one_receipt",
        |mut h| {
            let (tenant, other) = (h.tenant, h.other);
            for (label, expired_secs_ago) in [
                ("t.a", 3000),
                ("t.b", 2400),
                ("t.c", 1800),
                ("t.d", 1200),
                ("t.e", 600),
            ] {
                h.seed_token(tenant, label, -expired_secs_ago);
            }
            h.seed_token(other, "t.other", -3000);
            let deleted =
                h.rt.block_on(confirm_token_repo::sweep_expired(
                    &h.maintenance,
                    tenant,
                    HOUR,
                    2,
                ))
                .expect("confirm door");
            assert_eq!(deleted, 2, "at most LIMIT rows per call");
            assert_eq!(
                h.tokens(tenant),
                ["t.c", "t.d", "t.e"],
                "oldest expiry first"
            );
            assert_eq!(h.receipts(tenant, "confirm_tokens"), [(2, 2)]);
            assert_eq!(
                h.tokens(other),
                ["t.other"],
                "per tenant under the caller's GUC"
            );
            assert!(h.receipts(other, "confirm_tokens").is_empty());
        },
    );
}

/// T-G2 (D-F): a tenant with nothing purgeable gets 0 from every door and no receipt row. Fault: drop the
/// `affected > 0` guard ⇒ a zero receipt is attempted (and refused by the CHECK) ⇒ red.
#[test]
fn a_call_that_deletes_nothing_writes_no_receipt() {
    run_db_fixture::<DoorsFixture, _>("a_call_that_deletes_nothing_writes_no_receipt", |mut h| {
        let tenant = h.tenant;
        h.seed_token(tenant, "t.live", 600);
        h.seed_snapshot(tenant, 600, 2);
        h.seed_bucket(tenant, "user", "recent", (10, 10, 1), 0);
        h.seed_job("c35.fixture", "PENDING", 7200);
        let confirm =
            h.rt.block_on(confirm_token_repo::sweep_expired(
                &h.maintenance,
                tenant,
                HOUR,
                10,
            ))
            .expect("confirm door");
        let snapshots = h.purge_snapshots(tenant, 10);
        let buckets = h.purge_buckets(tenant, HOUR, 10);
        let jobs = h.purge_jobs(ZERO_RETENTION, 10).expect("jobs door");
        assert_eq!((confirm, snapshots, buckets, jobs), (0, 0, 0, 0));
        assert_eq!(
            h.count(
                "SELECT count(*) FROM ops.maintenance_receipts WHERE tenant_id = $1",
                tenant
            ),
            0,
            "idle tenants never grow the receipt table"
        );
    });
}

/// T-H1 (D-H): two expired snapshots go with all their items in one statement (the NO ACTION FK is checked at the
/// end of it); the live snapshot and the bystander's expired one stay; one receipt counts snapshots. Fault: drop
/// the `expires_at` predicate ⇒ the live snapshot goes too ⇒ red.
#[test]
fn an_expired_snapshot_and_its_items_go_in_one_statement_live_ones_stay() {
    run_db_fixture::<DoorsFixture, _>(
        "an_expired_snapshot_and_its_items_go_in_one_statement_live_ones_stay",
        |mut h| {
            let (tenant, other) = (h.tenant, h.other);
            h.seed_snapshot(tenant, -120, 3);
            h.seed_snapshot(tenant, -60, 3);
            let live = h.seed_snapshot(tenant, 600, 2);
            h.seed_snapshot(other, -120, 4);
            assert_eq!(h.purge_snapshots(tenant, 10), 2);
            let left: Vec<Uuid> = h
                .admin
                .query(
                    "SELECT selection_snapshot_id FROM ops.selection_snapshots WHERE tenant_id = $1",
                    &[&tenant],
                )
                .expect("snapshots")
                .iter()
                .map(|r| r.get(0))
                .collect();
            assert_eq!(left, [live], "only the live snapshot stays");
            let items = "SELECT count(*) FROM ops.selection_snapshot_items WHERE tenant_id = $1";
            assert_eq!(
                h.count(items, tenant),
                2,
                "the expired manifests went with them"
            );
            assert_eq!(
                h.count(items, other),
                4,
                "the bystander's rows are untouched"
            );
            assert_eq!(h.receipts(tenant, "selection_snapshots"), [(2, 10)]);
        },
    );
}

/// T-H3 (D-H): reader A's REPEATABLE READ snapshot is taken, B purges the expired snapshot and commits, A runs the
/// production page statement and still sees the snapshot with its whole manifest; a fresh read then finds no
/// snapshot. Neither ever sees a found snapshot with zero items. (The interleave inside one READ COMMITTED
/// statement cannot happen: one statement reads one MVCC snapshot; gate c35_one_page_statement pins the one
/// statement.)
#[test]
fn a_purge_after_the_page_read_began_cannot_empty_it() {
    run_db_fixture::<DoorsFixture, _>(
        "a_purge_after_the_page_read_began_cannot_empty_it",
        |mut h| {
            let tenant = h.tenant;
            let snapshot = h.seed_snapshot(tenant, -60, 3);
            // dep: PostgreSQL(role_retrieval_worker) — reader A on its own connection
            let mut reader = Client::connect(&dsn_as_role(&h.dsn, "role_retrieval_worker"), NoTls)
                .expect("reader connects");
            let mut a = reader
                .build_transaction()
                .isolation_level(IsolationLevel::RepeatableRead)
                .start()
                .expect("A begins");
            a.batch_execute(&format!("SET LOCAL humaux.tenant_id = '{tenant}'"))
                .expect("A tenant");
            a.query_one("SELECT 1", &[]).expect("A takes its snapshot");

            assert_eq!(h.purge_snapshots(tenant, 10), 1, "B purges and commits");

            let rows = a
                .query(
                    selection_repo::MANIFEST_PAGE_SQL,
                    &[&snapshot, &-1_i64, &10_i64],
                )
                .expect("A's page statement");
            let items: Vec<Option<Uuid>> = rows.iter().map(|r| r.get("item_id")).collect();
            assert_eq!(items.len(), 3, "A sees the whole manifest: {items:?}");
            assert!(items.iter().all(Option::is_some), "{items:?}");
            a.commit().expect("A commits");

            let fresh = h.page(snapshot);
            assert!(
                matches!(fresh, Err(SelectionRepoError::SnapshotNotFound)),
                "{fresh:?}"
            );
        },
    );
}

/// T-I1 (D-I): with idle = 1 h, a full idle bucket and an idle one that has refilled go; an idle bucket that is
/// still draining and a full but recent one stay, and so does the bystander's. Fault: drop the refill predicate ⇒
/// the draining bucket goes ⇒ red.
#[test]
fn only_buckets_that_would_be_full_now_are_purged() {
    run_db_fixture::<DoorsFixture, _>("only_buckets_that_would_be_full_now_are_purged", |mut h| {
        let (tenant, other) = (h.tenant, h.other);
        h.seed_bucket(tenant, "user", "full.idle", (10, 10, 1), 7200);
        h.seed_bucket(tenant, "user", "refilled.idle", (10, 0, 1), 7200);
        h.seed_bucket(tenant, "user", "draining.idle", (1_000_000, 0, 1), 7200);
        h.seed_bucket(tenant, "user", "full.recent", (10, 10, 1), 0);
        h.seed_bucket(other, "user", "other.full.idle", (10, 10, 1), 7200);
        assert_eq!(h.purge_buckets(tenant, HOUR, 10), 2);
        assert_eq!(h.buckets(tenant), ["draining.idle", "full.recent"]);
        assert_eq!(h.buckets(other), ["other.full.idle"]);
        assert_eq!(h.receipts(tenant, "rate_buckets"), [(2, 10)]);
    });
}

/// T-I2 (D-I): a purged pre-auth bucket (system tenant) answers the next two requests exactly as an identical
/// bucket that was never purged: allowed, then RATE_LIMITED. Fault: the consumer recreates a bucket with 0
/// tokens ⇒ the purged one answers RATE_LIMITED first ⇒ red.
#[test]
fn a_purged_bucket_answers_the_same_rate_decision() {
    run_db_fixture::<DoorsFixture, _>("a_purged_bucket_answers_the_same_rate_decision", |mut h| {
        let system = Uuid::nil();
        let (purged, kept) = ("203.0.113.7", "203.0.113.8");
        h.seed_bucket(system, "ip", purged, (1, 1, 1), 7200);
        assert_eq!(h.purge_buckets(system, HOUR, 10), 1);
        assert_eq!(h.buckets(system), Vec::<String>::new());
        h.seed_bucket(system, "ip", kept, (1, 1, 1), 7200);
        let policy = RatePolicy::new(1, 1).expect("policy");
        let decide = |ip: &str| -> Vec<Result<(), ErrorCode>> {
            (0..2)
                .map(|_| {
                    h.rt.block_on(quota_repo::consume_rate(
                        &h.runtime,
                        RateSubject::PreauthIp(ip.parse().expect("ip")),
                        "mcp.read",
                        "default",
                        policy,
                    ))
                })
                .collect()
        };
        let (after_purge, never_purged) = (decide(purged), decide(kept));
        println!("purged {after_purge:?} / never purged {never_purged:?}");
        assert_eq!(after_purge, never_purged);
        assert_eq!(after_purge, [Ok(()), Err(ErrorCode::RateLimited)]);
    });
}

/// SEC-6 x D-I: the consumer keys an IPv6 client by its /64 (ADR-0062 E5), and that one bucket is what the purge
/// door removes once it would be full; the next address of the /64 starts from a full bucket again. Fault: key by
/// the full address ⇒ two buckets, the second call is allowed ⇒ red.
#[test]
fn an_ipv6_64_preauth_bucket_goes_through_the_purge_door() {
    run_db_fixture::<DoorsFixture, _>(
        "an_ipv6_64_preauth_bucket_goes_through_the_purge_door",
        |mut h| {
            let system = Uuid::nil();
            let policy = RatePolicy::new(1, 1).expect("policy");
            let consume = |h: &Handle, ip: &str| {
                h.rt.block_on(quota_repo::consume_rate(
                    &h.runtime,
                    RateSubject::PreauthIp(ip.parse().expect("ip")),
                    "mcp.read",
                    "default",
                    policy,
                ))
            };
            assert_eq!(consume(&h, "2001:db8:5:6::1"), Ok(()));
            assert_eq!(consume(&h, "2001:db8:5:6::2"), Err(ErrorCode::RateLimited));
            assert_eq!(h.buckets(system), ["2001:db8:5:6::/64"]);
            h.admin
                .execute(
                    "UPDATE control.rate_buckets SET updated_at = now() - interval '2 hours' \
                     WHERE tenant_id = $1",
                    &[&system],
                )
                .expect("age the /64 bucket");
            assert_eq!(h.purge_buckets(system, HOUR, 10), 1);
            assert_eq!(h.buckets(system), Vec::<String>::new());
            assert_eq!(consume(&h, "2001:db8:5:6::3"), Ok(()));
        },
    );
}

/// T-J3 (D-J): at retention 0 with a one-hour budget window, a DONE job whose provider call began 10 minutes ago
/// stays with its call (and `admit_distill_budget` still counts it); one whose call is two hours old goes with
/// its call. Fault: compare `begun_at` with the done cutoff instead of the window ⇒ the first job and its call go
/// ⇒ red.
#[test]
fn a_done_job_with_a_call_inside_the_budget_window_is_kept_at_retention_zero() {
    run_db_fixture::<DoorsFixture, _>(
        "a_done_job_with_a_call_inside_the_budget_window_is_kept_at_retention_zero",
        |mut h| {
            let tenant = h.tenant;
            let inside = h.seed_job("c35.fixture", "DONE", 7200);
            h.seed_call(inside, 600);
            let outside = h.seed_job("c35.fixture", "DONE", 7200);
            h.seed_call(outside, 7200);
            assert_eq!(h.purge_jobs(ZERO_RETENTION, 10).expect("jobs door"), 1);
            assert!(h.job_exists(inside), "the window keeps the job");
            assert!(!h.job_exists(outside));
            assert_eq!(
                h.count(
                    "SELECT count(*) FROM ops.distill_calls WHERE tenant_id = $1",
                    tenant
                ),
                1,
                "the CASCADE took only the call outside the window"
            );
            let admit = |h: &mut Handle, max: i32| -> bool {
                h.admin
                    .query_one(
                        "SELECT ops.admit_distill_budget($1, 3600, $2)",
                        &[&tenant, &max],
                    )
                    .expect("admit_distill_budget")
                    .get(0)
            };
            assert!(
                !admit(&mut h, 1),
                "the kept call still counts against a budget of 1"
            );
            assert!(admit(&mut h, 2));
            assert_eq!(h.receipts(tenant, "terminal_jobs"), [(1, 10)]);
        },
    );
}

/// T-J4 (D-J): DONE and DEAD jobs each wait out their own retention. Fault: one retention for both ⇒ the DONE job
/// goes in the first call ⇒ red.
#[test]
fn dead_jobs_use_their_own_retention() {
    run_db_fixture::<DoorsFixture, _>("dead_jobs_use_their_own_retention", |mut h| {
        let done = h.seed_job("c35.fixture", "DONE", 7200);
        let dead = h.seed_job("c35.fixture", "DEAD", 7200);
        let day = Duration::from_secs(86_400);
        let dead_short = JobRetention {
            done: day,
            dead: HOUR,
            budget_window: HOUR,
        };
        assert_eq!(h.purge_jobs(dead_short, 10).expect("jobs door"), 1);
        assert!(h.job_exists(done) && !h.job_exists(dead));
        let failed = h.seed_job("c35.fixture", "FAILED", 7200);
        let done_short = JobRetention {
            done: HOUR,
            dead: day,
            budget_window: HOUR,
        };
        assert_eq!(h.purge_jobs(done_short, 10).expect("jobs door"), 1);
        assert!(!h.job_exists(done) && h.job_exists(failed));
    });
}

/// T-J5 (D-J, ADR-0058 R4): a DEAD distill job whose Evidence row is FAILED survives dead retention 0 and
/// `requeue_dead_distill` still re-arms it; the same job shape with a DONE Evidence row (which that door would
/// refuse) is purged. Fault: drop the R4 predicate ⇒ requeue answers `job_not_found` ⇒ red.
#[test]
fn a_redrivable_dead_distill_job_is_kept_until_its_outbox_settles() {
    run_db_fixture::<DoorsFixture, _>(
        "a_redrivable_dead_distill_job_is_kept_until_its_outbox_settles",
        |mut h| {
            let tenant = h.tenant;
            let redrivable = h.dead_distill_job("FAILED");
            let settled = h.dead_distill_job("DONE");
            assert_eq!(h.purge_jobs(ZERO_RETENTION, 10).expect("jobs door"), 1);
            assert!(h.job_exists(redrivable) && !h.job_exists(settled));
            let mut txn = h.admin.transaction().expect("txn");
            txn.batch_execute(&format!("SET LOCAL humaux.tenant_id = '{tenant}'"))
                .expect("tenant");
            let rearmed = txn
                .query(
                    "SELECT job_id, skipped FROM ops.requeue_dead_distill($1, $2, NULL)",
                    &[&tenant, &redrivable],
                )
                .map(|rows| {
                    rows.iter()
                        .map(|r| (r.get::<_, Uuid>(0), r.get::<_, Option<String>>(1)))
                        .collect::<Vec<_>>()
                });
            println!("requeue after the purge => {rearmed:?}");
            assert_eq!(rearmed.expect("R4 still re-arms"), [(redrivable, None)]);
            txn.commit().expect("commit");
        },
    );
}

/// T-J6 (D-J): a job a contribution execution links is kept (its NO ACTION FK would abort the whole statement),
/// and the unlinked one beside it still goes. Fault: drop the links NOT EXISTS ⇒ the call fails on the FK ⇒ red.
#[test]
fn a_job_linked_by_a_contribution_execution_is_kept() {
    run_db_fixture::<DoorsFixture, _>(
        "a_job_linked_by_a_contribution_execution_is_kept",
        |mut h| {
            let tenant = h.tenant;
            let linked = h.seed_job("c35.fixture", "DONE", 7200);
            let free = h.seed_job("c35.fixture", "DONE", 7200);
            // Seeding only: a real link needs a whole contribution execution; replica mode skips the link's FK
            // and validate triggers for this one insert, while the jobs -> links FK the purge meets stays live.
            let mut txn = h.admin.transaction().expect("txn");
            txn.batch_execute("SET LOCAL session_replication_role = replica")
                .expect("replica");
            txn.execute(
                "INSERT INTO ops.contribution_execution_job_links (job_id, tenant_id, execution_id, created_at) \
                 VALUES ($1, $2, gen_random_uuid(), now())",
                &[&linked, &tenant],
            )
            .expect("seed link");
            txn.commit().expect("commit");
            let purged = h.purge_jobs(ZERO_RETENTION, 10);
            println!("linked + free => {purged:?}");
            assert_eq!(purged.expect("the statement must not abort"), 1);
            assert!(h.job_exists(linked) && !h.job_exists(free));
        },
    );
}

/// T-E1 (D-E): no door takes a table name — a text argument has no signature to land in (42883) — and a call
/// without the tenant GUC is refused before it reads a row (42704). The rls-check closed set (T-R1) is this
/// shape's fault gate.
#[test]
fn a_door_takes_no_table_name() {
    run_db_fixture::<DoorsFixture, _>("a_door_takes_no_table_name", |h| {
        // dep: PostgreSQL(role_maintenance) — raw calls as the daemon's role
        let mut maintenance = Client::connect(&dsn_as_role(&h.dsn, "role_maintenance"), NoTls)
            .expect("role_maintenance connects");
        for call in [
            "SELECT control.sweep_confirm_tokens('control.users'::text)",
            "SELECT ops.purge_expired_selection_snapshots('ops.outbox'::text)",
            "SELECT control.purge_idle_rate_buckets('control.rate_buckets'::text)",
            "SELECT ops.purge_terminal_jobs('ops.outbox'::text)",
        ] {
            let error = maintenance.query(call, &[]).expect_err(call);
            assert_eq!(
                error.code().map(|c| c.code()),
                Some("42883"),
                "{call}: {error}"
            );
        }
        let error = maintenance
            .query("SELECT ops.purge_expired_selection_snapshots(10)", &[])
            .expect_err("no tenant GUC");
        assert_eq!(error.code().map(|c| c.code()), Some("42704"), "{error}");
    });
}
