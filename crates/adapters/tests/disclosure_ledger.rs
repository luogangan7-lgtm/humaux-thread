//! T4.2 integration test — `disclosure` (§7.4) against a real Postgres, on
//! `migrations/0047_data_disclosure_ledger.sql`'s real `ops.data_disclosures` /
//! `ops.data_disclosure_sources` tables (same convention as `jobs_claim.rs`: shared tables,
//! each test scopes rows to its own throwaway `control.tenants` row cleaned up on drop).
//!
//! Three-state skip (§79.2): no DSN, unreachable DB, or the migration not yet applied all
//! print a visible SKIP and return.

use std::time::{Duration, Instant};

use humaux_adapters::disclosure::{self, DeletionCapability, DisclosureOutcome, DisclosureSource};
use humaux_adapters::postgres::{MaintenanceDbPool, PrivateWorkerDbPool};
use humaux_domain::dataclass::DataClass;
use humaux_domain::egress::{self, AuthorizedEgressPayload, PrivateDataPurpose, ProcessorId};
use humaux_domain::ids::TenantId;
use humaux_testkit::{
    DISCLOSURE_LEDGER_ADVISORY_LOCK, DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture,
};
use postgres::error::SqlState;
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
    private_worker: PrivateWorkerDbPool,
    maintenance: MaintenanceDbPool,
    admin: Client,
    tenant_id: Uuid,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Best-effort cleanup (repo CLAUDE.md hard rule ④). Deliberately does NOT attempt to
        // delete the ops.data_disclosures / ops.data_disclosure_sources rows this test wrote —
        // both reject DELETE unconditionally (§7.4 append-only, migrations/0047+0057), even as
        // `postgres` superuser (BYPASSRLS bypasses row security, not triggers) — nor this
        // throwaway tenant's own control.tenants row, which those disclosure rows FK-reference
        // and can therefore never be deleted either. All of it persists in the dev DB across
        // every test run, same as a real ledger row and its tenant would.
        //
        // What CAN and must still be cleaned up — evidence_objects / private_reasoning_domains
        // — is done as two independent statements, not one `batch_execute`: a single
        // `batch_execute` call is one implicit transaction, so the (expected) failure on a
        // since-removed `DELETE FROM control.tenants` used to abort the whole batch and roll
        // back these two DELETEs right along with it, silently leaking both every test run
        // (confirmed: 40 throwaway tenants / 16 evidence / 16 private_reasoning_domains rows
        // had accumulated in the dev DB from this file alone).
        let _ = self.admin.execute(
            "DELETE FROM private.evidence_objects WHERE tenant_id = $1",
            &[&self.tenant_id],
        );
        let _ = self.admin.execute(
            "DELETE FROM control.private_reasoning_domains WHERE tenant_id = $1",
            &[&self.tenant_id],
        );
    }
}

struct DisclosureFixture;

impl DbIntegrationFixture for DisclosureFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        // 与 `guard_trigger_rejects_truncate` 的 `TRUNCATE … CASCADE` 互斥（见
        // `DISCLOSURE_LEDGER_ADVISORY_LOCK` 的 doc：那条测试必须拿两张表的
        // AccessExclusiveLock，与并发读者交叉即成环，实测过一次真 40P01）。
        // 取**共享**锁：读者之间照旧并发，只在那一条测试跑时才让路。
        // session 级锁，连接 drop 即释放，测试 panic 也不会漏锁。
        admin
            .execute(
                "SELECT pg_advisory_lock_shared($1)",
                &[&DISCLOSURE_LEDGER_ADVISORY_LOCK],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        let migrated: bool = admin
            .query_one(
                "SELECT to_regclass('ops.data_disclosures') IS NOT NULL",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !migrated {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "ops.data_disclosures does not exist — run `cargo xtask migrate` \
                 (migrations/0047_data_disclosure_ledger.sql) against HUMAUX_TEST_PG_DSN first"
                    .to_string(),
            ));
        }

        let tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&"disclosure_ledger.rs throwaway tenant"],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        let private_dsn = dsn_as_role(&dsn, "role_private_worker");
        let maintenance_dsn = dsn_as_role(&dsn, "role_maintenance");
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let private_worker = rt
            .block_on(PrivateWorkerDbPool::connect(&private_dsn))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let maintenance = rt
            .block_on(MaintenanceDbPool::connect(&maintenance_dsn))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        Ok(Handle {
            rt,
            private_worker,
            maintenance,
            admin,
            tenant_id,
        })
    }
}

fn is_finalized(handle: &mut Handle, disclosure_id: Uuid) -> bool {
    handle
        .admin
        .query_one(
            "SELECT finalized_at IS NOT NULL FROM ops.data_disclosures WHERE disclosure_id = $1",
            &[&disclosure_id],
        )
        .expect("disclosure row must exist")
        .get(0)
}

fn ledger_data_class_and_bytes(handle: &mut Handle, disclosure_id: Uuid) -> (String, i64) {
    let row = handle
        .admin
        .query_one(
            "SELECT data_class, payload_bytes FROM ops.data_disclosures \
             WHERE disclosure_id = $1",
            &[&disclosure_id],
        )
        .expect("disclosure row must exist");
    (row.get(0), row.get(1))
}

/// §7.4 reserve()->finalize() round trip: reserve() must leave `finalized_at` NULL, and
/// finalize() must set it and return `true`.
#[test]
fn reserve_then_finalize_round_trip() {
    run_db_fixture::<DisclosureFixture, _>("reserve_then_finalize_round_trip", |mut handle| {
        let tenant_id = TenantId(handle.tenant_id);
        let payload = AuthorizedEgressPayload::new(b"hello disclosure ledger".to_vec());
        let permit = egress::authorize(
            tenant_id,
            ProcessorId(Uuid::now_v7()),
            PrivateDataPurpose::UserReasoning,
            DataClass::Private,
            &payload,
            Duration::from_secs(300),
        )
        .expect("authorize must not refuse a Private USER_REASONING permit");
        let evidence_id = seed_evidence(&mut handle);

        let disclosure_id = handle
            .rt
            .block_on(disclosure::reserve_private(
                &handle.private_worker,
                &permit,
                "cn-hangzhou",
                &payload,
                None,
                &[DisclosureSource::Evidence(evidence_id)],
            ))
            .expect("reserve must succeed");

        assert!(
            !is_finalized(&mut handle, disclosure_id),
            "reserve() must leave finalized_at NULL"
        );

        // The whole point of reading `data_class`/`payload_bytes` off `permit`/`payload`
        // instead of trusting free-standing caller arguments (§7.3/§7.4): the ledger row must
        // record what `authorize()` actually approved and what `payload` actually contains,
        // not something a caller could separately mis-state.
        let (recorded_class, recorded_bytes) =
            ledger_data_class_and_bytes(&mut handle, disclosure_id);
        assert_eq!(
            recorded_class,
            permit.data_class().as_str(),
            "ledger data_class must come from the permit, not a free-standing argument"
        );
        assert_eq!(
            recorded_bytes,
            payload.bytes().len() as i64,
            "ledger payload_bytes must come from the payload's own length"
        );

        let changed = handle
            .rt
            .block_on(disclosure::finalize_private(
                &handle.private_worker,
                handle.tenant_id,
                disclosure_id,
                DisclosureOutcome::Success,
                DeletionCapability::Unknown,
            ))
            .expect("finalize must not error");
        assert!(changed, "finalize() must report a real state change");
        assert!(
            is_finalized(&mut handle, disclosure_id),
            "finalize() must set finalized_at"
        );

        // §7.4 guard trigger: a second finalize() must be a no-op (Ok(false)), never a second
        // write — the DB trigger in migrations/0047 would reject it outright if this call
        // somehow tried anyway (WHERE finalized_at IS NULL already prevents the attempt).
        let changed_again = handle
            .rt
            .block_on(disclosure::finalize_private(
                &handle.private_worker,
                handle.tenant_id,
                disclosure_id,
                DisclosureOutcome::Failed,
                DeletionCapability::Unsupported,
            ))
            .expect("re-finalize must not error, just report no change");
        assert!(
            !changed_again,
            "re-finalize on an already-finalized row must be a no-op"
        );
    });
}

/// §7.3 "Permit 不能被拿去发送另一份正文": `reserve` must refuse a payload whose digest does
/// not match the permit it was authorized under, before writing any row.
#[test]
fn reserve_rejects_payload_digest_mismatch() {
    run_db_fixture::<DisclosureFixture, _>(
        "reserve_rejects_payload_digest_mismatch",
        |mut handle| {
            let tenant_id = TenantId(handle.tenant_id);
            let authorized_payload = AuthorizedEgressPayload::new(b"authorized body".to_vec());
            let permit = egress::authorize(
                tenant_id,
                ProcessorId(Uuid::now_v7()),
                PrivateDataPurpose::UserReasoning,
                DataClass::Private,
                &authorized_payload,
                Duration::from_secs(300),
            )
            .expect("authorize must not refuse a Private USER_REASONING permit");
            let evidence_id = seed_evidence(&mut handle);

            let different_payload =
                AuthorizedEgressPayload::new(b"a different body entirely".to_vec());
            let err = handle
                .rt
                .block_on(disclosure::reserve_private(
                    &handle.private_worker,
                    &permit,
                    "cn-hangzhou",
                    &different_payload,
                    None,
                    &[DisclosureSource::Evidence(evidence_id)],
                ))
                .expect_err("reserve must reject a payload whose digest does not match the permit");
            assert!(matches!(err, disclosure::DisclosureError::PayloadMismatch));
        },
    );
}

/// §7.4 "来源关系规范化": `reserve` must refuse an empty `sources` slice rather than silently
/// writing an unattributed disclosure row.
#[test]
fn reserve_rejects_empty_sources() {
    run_db_fixture::<DisclosureFixture, _>("reserve_rejects_empty_sources", |handle| {
        let tenant_id = TenantId(handle.tenant_id);
        let payload = AuthorizedEgressPayload::new(b"no sources probe".to_vec());
        let permit = egress::authorize(
            tenant_id,
            ProcessorId(Uuid::now_v7()),
            PrivateDataPurpose::UserReasoning,
            DataClass::Private,
            &payload,
            Duration::from_secs(300),
        )
        .expect("authorize must not refuse a Private USER_REASONING permit");

        let err = handle
            .rt
            .block_on(disclosure::reserve_private(
                &handle.private_worker,
                &permit,
                "cn-hangzhou",
                &payload,
                None,
                &[],
            ))
            .expect_err("reserve must reject an empty sources slice");
        assert!(matches!(err, disclosure::DisclosureError::NoSources));
    });
}

/// §80.1 fault-injection: `reserve_in_txn`'s `RecipientClass` guard (ADR-0003 second round) is
/// unreachable via any *production* `PrivateDataPurpose` — all three real variants classify as
/// `ExternalProcessor`. `NonRecipientForTest` (the `test-support`-feature-only variant,
/// `domain::egress`'s own doc) reaches the real call site anyway, proving the guard is wired
/// into `reserve_in_txn` itself, not merely exercised by `domain::boundary`'s own unit tests
/// of the pure `requires_disclosure_record` decision function in isolation. Verified this test
/// goes red on the mutation the guard exists to catch: deleting the `if
/// !requires_disclosure_record(...)` block from `reserve_in_txn` makes this call fall through
/// to a real INSERT, and the row-count assertion below fails.
#[cfg(feature = "test-support")]
#[test]
fn reserve_rejects_a_non_recipient_classified_purpose_before_any_write() {
    run_db_fixture::<DisclosureFixture, _>(
        "reserve_rejects_a_non_recipient_classified_purpose_before_any_write",
        |mut handle| {
            let tenant_id = TenantId(handle.tenant_id);
            let payload = AuthorizedEgressPayload::new(b"non-recipient probe".to_vec());
            let permit = egress::authorize(
                tenant_id,
                ProcessorId(Uuid::now_v7()),
                PrivateDataPurpose::NonRecipientForTest,
                DataClass::Private,
                &payload,
                Duration::from_secs(300),
            )
            .expect("authorize() does not gate on recipient classification");
            let evidence_id = seed_evidence(&mut handle);

            let err = handle
                .rt
                .block_on(disclosure::reserve_private(
                    &handle.private_worker,
                    &permit,
                    "cn-hangzhou",
                    &payload,
                    None,
                    &[DisclosureSource::Evidence(evidence_id)],
                ))
                .expect_err("reserve must refuse a non-recipient-classified purpose");
            assert!(matches!(
                err,
                disclosure::DisclosureError::NotADisclosureRecipient
            ));

            let row_count: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM ops.data_disclosures WHERE tenant_id = $1",
                    &[&handle.tenant_id],
                )
                .expect("count query must succeed")
                .get(0);
            assert_eq!(
                row_count, 0,
                "reserve must not write any ops.data_disclosures row for a rejected purpose"
            );
        },
    );
}

/// §53 INV-3: a reservation left unfinalized past the staleness window must be observable via
/// `open_reservations_older_than`. Passing `staleness_seconds = 0` lets this test observe the
/// "immediate red" case without a real 60s wait.
#[test]
fn open_reservations_older_than_finds_unfinalized_row() {
    run_db_fixture::<DisclosureFixture, _>(
        "open_reservations_older_than_finds_unfinalized_row",
        |mut handle| {
            let tenant_id = TenantId(handle.tenant_id);
            let payload = AuthorizedEgressPayload::new(b"stale reservation probe".to_vec());
            let permit = egress::authorize(
                tenant_id,
                ProcessorId(Uuid::now_v7()),
                PrivateDataPurpose::RetrievalEmbedding,
                DataClass::Sensitive,
                &payload,
                Duration::from_secs(300),
            )
            .expect("authorize must not refuse a Sensitive RetrievalEmbedding permit");
            let evidence_id = seed_evidence(&mut handle);

            let disclosure_id = handle
                .rt
                .block_on(disclosure::reserve_private(
                    &handle.private_worker,
                    &permit,
                    "cn-hangzhou",
                    &payload,
                    None,
                    &[DisclosureSource::Evidence(evidence_id)],
                ))
                .expect("reserve must succeed");

            let start = Instant::now();
            let stale = handle
                .rt
                .block_on(disclosure::open_reservations_older_than(
                    &handle.maintenance,
                    handle.tenant_id,
                    0.0,
                ))
                .expect("query must not error");
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "INV-3 observation query must not block waiting for real time to pass"
            );
            assert!(
                stale.iter().any(|r| r.disclosure_id == disclosure_id),
                "unfinalized reservation must appear in the stale set at staleness=0"
            );

            // Finalizing must remove it from the stale set.
            handle
                .rt
                .block_on(disclosure::finalize_private(
                    &handle.private_worker,
                    handle.tenant_id,
                    disclosure_id,
                    DisclosureOutcome::Success,
                    DeletionCapability::Unknown,
                ))
                .expect("finalize must not error");
            let stale_after = handle
                .rt
                .block_on(disclosure::open_reservations_older_than(
                    &handle.maintenance,
                    handle.tenant_id,
                    0.0,
                ))
                .expect("query must not error");
            assert!(
                !stale_after.iter().any(|r| r.disclosure_id == disclosure_id),
                "a finalized disclosure must not appear in the stale set"
            );
        },
    );
}

/// §7.4: `ops.data_disclosure_sources`'s "四个具体 ID 恰好一个非 NULL" CHECK — zero IDs set
/// must be rejected (注错: inserting with every id column NULL must fail, not silently pass).
#[test]
fn source_row_rejects_zero_ids_set() {
    run_db_fixture::<DisclosureFixture, _>("source_row_rejects_zero_ids_set", |mut handle| {
        let disclosure_id = seed_finalized_disclosure(&mut handle);
        let err = handle
            .admin
            .execute(
                "INSERT INTO ops.data_disclosure_sources \
                   (tenant_id, disclosure_id, source_kind, ordinal) \
                 VALUES ($1, $2, 'EVIDENCE', 0)",
                &[&handle.tenant_id, &disclosure_id],
            )
            .expect_err("zero non-NULL source ids must be rejected by the CHECK constraint");
        assert_eq!(
            err.code(),
            Some(&postgres::error::SqlState::CHECK_VIOLATION),
            "expected a CHECK-constraint violation, got: {err:?}"
        );
    });
}

/// §7.4: "source_kind 与非 NULL 列必须一致" — `source_kind = 'MEMORY'` with `evidence_id` set
/// (instead of `memory_id`) must be rejected.
#[test]
fn source_row_rejects_kind_mismatch() {
    run_db_fixture::<DisclosureFixture, _>("source_row_rejects_kind_mismatch", |mut handle| {
        let disclosure_id = seed_finalized_disclosure(&mut handle);
        let evidence_id = seed_evidence(&mut handle);
        let err = handle
            .admin
            .execute(
                "INSERT INTO ops.data_disclosure_sources \
                   (tenant_id, disclosure_id, source_kind, evidence_id, ordinal) \
                 VALUES ($1, $2, 'MEMORY', $3, 0)",
                &[&handle.tenant_id, &disclosure_id, &evidence_id],
            )
            .expect_err("source_kind='MEMORY' with evidence_id set must be rejected");
        assert_eq!(
            err.code(),
            Some(&postgres::error::SqlState::CHECK_VIOLATION),
            "expected a CHECK-constraint violation, got: {err:?}"
        );
    });
}

/// A correctly-shaped source row (exactly one id, matching its kind) must be accepted — the
/// positive control proving the two rejection tests above are catching a real constraint, not
/// an unrelated error.
#[test]
fn source_row_accepts_matching_single_id() {
    run_db_fixture::<DisclosureFixture, _>(
        "source_row_accepts_matching_single_id",
        |mut handle| {
            let disclosure_id = seed_finalized_disclosure(&mut handle);
            let evidence_id = seed_evidence(&mut handle);
            handle
                .admin
                .execute(
                    "INSERT INTO ops.data_disclosure_sources \
                   (tenant_id, disclosure_id, source_kind, evidence_id, ordinal) \
                 VALUES ($1, $2, 'EVIDENCE', $3, 0)",
                    &[&handle.tenant_id, &disclosure_id, &evidence_id],
                )
                .expect("a correctly-shaped source row must be accepted");
        },
    );
}

/// §80.1 "没有『注错红转绿』记录就不算存在" for `data_disclosures_guard_mutation`'s UPDATE-side
/// branches, proven directly against the trigger rather than inferred from
/// `reserve_then_finalize_round_trip`'s `WHERE finalized_at IS NULL` never reaching it.
#[test]
fn guard_trigger_rejects_identity_rewrite_refinalize_and_delete() {
    run_db_fixture::<DisclosureFixture, _>(
        "guard_trigger_rejects_identity_rewrite_refinalize_and_delete",
        |mut handle| {
            let disclosure_id = seed_finalized_disclosure(&mut handle);

            let region_err = handle
                .admin
                .execute(
                    "UPDATE ops.data_disclosures SET region = 'us-east-1' WHERE disclosure_id = $1",
                    &[&disclosure_id],
                )
                .expect_err("rewriting an identity column after INSERT must be rejected");
            assert_eq!(
                region_err.code(),
                Some(&SqlState::INSUFFICIENT_PRIVILEGE),
                "expected insufficient_privilege, got: {region_err:?}"
            );

            let data_class_err = handle
                .admin
                .execute(
                    "UPDATE ops.data_disclosures SET data_class = 'PUBLIC' WHERE disclosure_id = $1",
                    &[&disclosure_id],
                )
                .expect_err("rewriting data_class after INSERT must be rejected");
            assert_eq!(
                data_class_err.code(),
                Some(&SqlState::INSUFFICIENT_PRIVILEGE),
                "expected insufficient_privilege, got: {data_class_err:?}"
            );

            let refinalize_err = handle
                .admin
                .execute(
                    "UPDATE ops.data_disclosures SET finalized_at = NULL, outcome = NULL \
                     WHERE disclosure_id = $1",
                    &[&disclosure_id],
                )
                .expect_err("un-finalizing an already-finalized row must be rejected");
            assert_eq!(
                refinalize_err.code(),
                Some(&SqlState::INSUFFICIENT_PRIVILEGE),
                "expected insufficient_privilege, got: {refinalize_err:?}"
            );

            let delete_err = handle
                .admin
                .execute(
                    "DELETE FROM ops.data_disclosures WHERE disclosure_id = $1",
                    &[&disclosure_id],
                )
                .expect_err("DELETE must be rejected (§7.4 append-only)");
            assert_eq!(
                delete_err.code(),
                Some(&SqlState::INSUFFICIENT_PRIVILEGE),
                "expected insufficient_privilege, got: {delete_err:?}"
            );
        },
    );
}

/// §37 / migrations/0057: `deletion_requested_at` may transition NULL -> value exactly once —
/// a second write, even to a different timestamp, must be rejected (0047 originally left this
/// column, and `deletion_confirmed_at`, writable indefinitely).
#[test]
fn guard_trigger_rejects_second_deletion_requested_at_write() {
    run_db_fixture::<DisclosureFixture, _>(
        "guard_trigger_rejects_second_deletion_requested_at_write",
        |mut handle| {
            let disclosure_id = seed_finalized_disclosure(&mut handle);
            handle
                .admin
                .execute(
                    "UPDATE ops.data_disclosures SET deletion_requested_at = now() \
                     WHERE disclosure_id = $1",
                    &[&disclosure_id],
                )
                .expect("first deletion_requested_at write (NULL -> value) must succeed");

            let err = handle
                .admin
                .execute(
                    "UPDATE ops.data_disclosures \
                     SET deletion_requested_at = now() + interval '1 second' \
                     WHERE disclosure_id = $1",
                    &[&disclosure_id],
                )
                .expect_err("a second deletion_requested_at write must be rejected (§37)");
            assert_eq!(
                err.code(),
                Some(&SqlState::INSUFFICIENT_PRIVILEGE),
                "expected insufficient_privilege, got: {err:?}"
            );
        },
    );
}

/// §7.4/§77 append-only: TRUNCATE must be rejected on both tables (0047 originally only
/// installed `FOR EACH ROW` triggers, which TRUNCATE never fires — closed by migrations/0057).
/// Runs inside an explicit, never-committed transaction: even if the guard regressed and
/// TRUNCATE actually succeeded, the rollback on drop still discards it rather than truncating
/// the shared dev-DB table for real.
#[test]
fn guard_trigger_rejects_truncate() {
    run_db_fixture::<DisclosureFixture, _>("guard_trigger_rejects_truncate", |mut handle| {
        let _ = seed_finalized_disclosure(&mut handle);

        // 升级为排他：先放掉 fixture 取的共享锁，再等所有并发读者放手。
        // 顺序不能反——同一 session 持共享时申请排他会自己阻塞自己。
        handle
            .admin
            .execute(
                "SELECT pg_advisory_unlock_shared($1)",
                &[&DISCLOSURE_LEDGER_ADVISORY_LOCK],
            )
            .expect("release shared advisory lock");
        handle
            .admin
            .execute(
                "SELECT pg_advisory_lock($1)",
                &[&DISCLOSURE_LEDGER_ADVISORY_LOCK],
            )
            .expect("take exclusive advisory lock before TRUNCATE");

        let mut txn = handle.admin.transaction().expect("begin txn");
        let err = txn
            .execute("TRUNCATE ops.data_disclosures CASCADE", &[])
            .expect_err("TRUNCATE must be rejected (§7.4/§77 append-only)");
        assert_eq!(
            err.code(),
            Some(&SqlState::INSUFFICIENT_PRIVILEGE),
            "expected insufficient_privilege, got: {err:?}"
        );
        drop(txn);

        let mut txn = handle.admin.transaction().expect("begin txn");
        let err = txn
            .execute("TRUNCATE ops.data_disclosure_sources", &[])
            .expect_err("TRUNCATE must be rejected (§7.4/§77 append-only)");
        assert_eq!(
            err.code(),
            Some(&SqlState::INSUFFICIENT_PRIVILEGE),
            "expected insufficient_privilege, got: {err:?}"
        );
    });
}

/// §79.3 cross-tenant A/B: tenant B must not be able to finalize tenant A's disclosure, and
/// must not observe it via its own stale-reservation scan. RLS
/// (`data_disclosures_tenant_isolation`) is expected to scope both by `humaux.tenant_id` down
/// to zero matching rows; this pins that actual behavior instead of leaving it to inference
/// from rls-check's structural policy-existence coverage.
#[test]
fn cross_tenant_cannot_finalize_or_observe_another_tenants_disclosure() {
    run_db_fixture::<DisclosureFixture, _>(
        "cross_tenant_cannot_finalize_or_observe_another_tenants_disclosure",
        |mut handle| {
            let tenant_b: Uuid = handle
                .admin
                .query_one(
                    "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                    &[&"disclosure_ledger.rs throwaway tenant B"],
                )
                .expect("seed tenant B")
                .get(0);

            let tenant_id_a = TenantId(handle.tenant_id);
            let payload = AuthorizedEgressPayload::new(b"cross-tenant probe".to_vec());
            let permit = egress::authorize(
                tenant_id_a,
                ProcessorId(Uuid::now_v7()),
                PrivateDataPurpose::UserReasoning,
                DataClass::Private,
                &payload,
                Duration::from_secs(300),
            )
            .expect("authorize must not refuse a Private USER_REASONING permit");
            let evidence_id = seed_evidence(&mut handle);

            let disclosure_id = handle
                .rt
                .block_on(disclosure::reserve_private(
                    &handle.private_worker,
                    &permit,
                    "cn-hangzhou",
                    &payload,
                    None,
                    &[DisclosureSource::Evidence(evidence_id)],
                ))
                .expect("reserve must succeed");

            // Tenant B attempts to finalize A's disclosure_id under its own RLS scope.
            let changed = handle
                .rt
                .block_on(disclosure::finalize_private(
                    &handle.private_worker,
                    tenant_b,
                    disclosure_id,
                    DisclosureOutcome::Success,
                    DeletionCapability::Unknown,
                ))
                .expect("finalize must not error even cross-tenant");
            assert!(
                !changed,
                "tenant B must not be able to finalize tenant A's disclosure"
            );
            assert!(
                !is_finalized(&mut handle, disclosure_id),
                "tenant A's disclosure must remain unfinalized after tenant B's attempt"
            );

            // Tenant B's own stale-reservation scan must not see tenant A's row.
            let stale_for_b = handle
                .rt
                .block_on(disclosure::open_reservations_older_than(
                    &handle.maintenance,
                    tenant_b,
                    0.0,
                ))
                .expect("query must not error");
            assert!(
                !stale_for_b.iter().any(|r| r.disclosure_id == disclosure_id),
                "tenant B must not observe tenant A's open reservation"
            );

            let _ = handle.admin.execute(
                "DELETE FROM control.tenants WHERE tenant_id = $1",
                &[&tenant_b],
            );
        },
    );
}

/// §78.2 DB-vs-Rust contract test — the real one. `dataclass.rs`'s `as_str_literals_are_stable`
/// and this module's `*_wire_strings_are_stable` unit tests compare compiled Rust against
/// compiled Rust and therefore cannot see a migration drifting a CHECK constraint out from
/// under them; this test has the live Postgres connection they don't. Extracts every quoted
/// literal from each CHECK's own `pg_get_constraintdef` text and compares the sorted set
/// against each Rust side's own wire strings, sorted the same way (order-independent — the
/// CHECK clause's literal order is not itself a frozen contract, only the *set* is).
#[test]
fn wire_strings_match_live_db_check_constraints() {
    run_db_fixture::<DisclosureFixture, _>(
        "wire_strings_match_live_db_check_constraints",
        |mut handle| {
            let data_class_wire: Vec<&str> = DataClass::ALL.iter().map(|c| c.as_str()).collect();
            let cases: [(&str, &str, Vec<&str>); 5] = [
                (
                    "private.evidence_objects",
                    "evidence_objects_data_class_check",
                    data_class_wire.clone(),
                ),
                (
                    "ops.data_disclosures",
                    "data_disclosures_data_class_check",
                    data_class_wire,
                ),
                (
                    "ops.data_disclosures",
                    "data_disclosures_purpose_check",
                    vec!["USER_REASONING", "RETRIEVAL_EMBEDDING", "RETRIEVAL_RERANK"],
                ),
                (
                    "ops.data_disclosures",
                    "data_disclosures_outcome_check",
                    vec!["SUCCESS", "FAILED", "DENIED"],
                ),
                (
                    "ops.data_disclosures",
                    "data_disclosures_deletion_capability_check",
                    vec!["SUPPORTED", "UNSUPPORTED", "UNKNOWN"],
                ),
            ];
            for (table, constraint, mut expected) in cases {
                expected.sort_unstable();
                let def: String = handle
                    .admin
                    .query_one(
                        "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
                         WHERE conrelid = to_regclass($1) AND conname = $2",
                        &[&table, &constraint],
                    )
                    .unwrap_or_else(|e| panic!("constraint {constraint} on {table} not found: {e}"))
                    .get(0);
                // Quoted literals sit at the odd positions of a split on `'` (the definition
                // always opens with non-literal SQL text before the first literal).
                let mut actual: Vec<&str> = def.split('\'').skip(1).step_by(2).collect();
                actual.sort_unstable();
                assert_eq!(
                    actual, expected,
                    "{constraint} literals drifted from the Rust wire strings — db def: {def}"
                );
            }
        },
    );
}

fn seed_finalized_disclosure(handle: &mut Handle) -> Uuid {
    handle
        .admin
        .query_one(
            "INSERT INTO ops.data_disclosures \
               (grant_id, tenant_id, processor_id, region, data_class, purpose, \
                payload_sha256, payload_bytes, finalized_at, outcome) \
             VALUES ($1, $2, $3, 'cn-hangzhou', 'PRIVATE', 'USER_REASONING', \
                     $4, 11, now(), 'SUCCESS') \
             RETURNING disclosure_id",
            &[
                &Uuid::now_v7(),
                &handle.tenant_id,
                &Uuid::now_v7(),
                &vec![0u8; 32],
            ],
        )
        .expect("seed disclosure row")
        .get(0)
}

fn seed_evidence(handle: &mut Handle) -> Uuid {
    let reasoning_domain_id: Uuid = handle
        .admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains (tenant_id, name) \
             VALUES ($1, 'disclosure_ledger.rs throwaway domain') \
             RETURNING reasoning_domain_id",
            &[&handle.tenant_id],
        )
        .expect("seed reasoning domain row")
        .get(0);

    handle
        .admin
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                visibility_class, reasoning_domain_id) \
             VALUES ($1, 'EVENT', $2, 'PRIVATE', 'DirectUserInput', 'TENANT_SHARED', $3) \
             RETURNING evidence_id",
            &[&handle.tenant_id, &vec![1u8; 32], &reasoning_domain_id],
        )
        .expect("seed evidence row")
        .get(0)
}
