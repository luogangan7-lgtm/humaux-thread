//! `adapters::tests::phase9_assessed_contract` — Real-PG negative gates for the additive Phase 9 assessed-candidate
//!   seam.
//! Depends-on: crates=[postgres, uuid]; services=[PostgreSQL(any) r=[ops.jobs, ops.outbox,
//!   staging.sanitized_public_candidates] w=[public.sources] x=[ops.claim_global_anonymous_public_dispatches,
//!   ops.enqueue_public_projection_from_anonymous_dispatch, public.admit_anonymous_dispatch,
//!   public.admit_anonymous_source, public.phase9_public_coverage_for_probe, public.revoke_anonymous_source,
//!   staging.read_sanitized_public_candidate], PostgreSQL(role_private_worker), PostgreSQL(role_public_worker)];
//!   env=[HUMAUX_TEST_PG_DSN]; modules=[]
//! Called-by: [cargo-test]
//! Invariants: [the assessed release contract is checked on role_private_worker/role_public_worker pools against a
//!   PG18 fixture migrated through 0127; the tests are #[ignore] lane tests]
//! Spec: Baseline §12; §79.2
//!
//! Run after applying migrations through 0127 to an isolated PG18 fixture.

use postgres::{Client, NoTls, error::SqlState};
use uuid::Uuid;

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL migrated through 0127"]
#[allow(
    clippy::too_many_lines,
    reason = "single integration scenario covers the complete assessed contract"
)]
fn public_worker_cannot_scan_sanitized_envelopes_or_forge_anonymous_admission() {
    let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PostgreSQL DSN");
    // dep: PostgreSQL(role_public_worker) — open a role-scoped PG connection/pool for this test
    let mut client = Client::connect(&dsn, NoTls).expect("isolated PostgreSQL");
    let mut txn = client.transaction().expect("transaction");
    txn.batch_execute("SET LOCAL ROLE role_public_worker")
        .expect("non-superuser public worker role");

    txn.batch_execute("SAVEPOINT deny_column_scan")
        .expect("savepoint");
    assert!(
        txn.query(
            "SELECT sanitized_content FROM staging.sanitized_public_candidates LIMIT 1",
            &[]
        )
        .is_err(),
        "column-level direct scans must be denied"
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT deny_column_scan")
        .expect("rollback denied read");
    txn.batch_execute("SAVEPOINT deny_table_scan")
        .expect("savepoint");
    assert!(
        txn.query(
            "SELECT * FROM staging.sanitized_public_candidates LIMIT 1",
            &[]
        )
        .is_err(),
        "table-level direct scans must be denied"
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT deny_table_scan")
        .expect("rollback denied read");
    txn.batch_execute("SAVEPOINT deny_identity_join")
        .expect("savepoint");
    assert!(
        txn.query(
            "SELECT source.source_id,outbox.tenant_id FROM public.sources source \
             JOIN ops.outbox outbox ON outbox.anonymous_source_id=source.source_id LIMIT 1",
            &[],
        )
        .is_err(),
        "public worker must not join an anonymous source to a tenant-bearing outbox row"
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT deny_identity_join")
        .expect("rollback denied join");
    txn.batch_execute("SAVEPOINT deny_job_payload_scan")
        .expect("savepoint");
    assert!(
        txn.query("SELECT tenant_id,payload FROM ops.jobs LIMIT 1", &[])
            .is_err(),
        "public worker must not scan tenant-bearing job payloads"
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT deny_job_payload_scan")
        .expect("rollback denied job scan");

    txn.batch_execute("SAVEPOINT global_claim_validation")
        .expect("savepoint");
    let opaque_claim = txn
        .query(
            "SELECT dispatch_id,job_type,attempt,payload \
             FROM ops.claim_global_anonymous_public_dispatches('phase9-contract',60.0,0)",
            &[],
        )
        .expect_err("global anonymous dispatcher validates a tenant-free claim before scanning");
    assert_eq!(
        opaque_claim.code(),
        Some(&SqlState::INVALID_PARAMETER_VALUE),
        "the no-tenant claim must execute its boundary validation, not fail for missing EXECUTE"
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT global_claim_validation")
        .expect("rollback global claim validation");
    txn.batch_execute("SAVEPOINT deny_unleased_projection_enqueue")
        .expect("savepoint");
    let forged_projection = txn
        .query(
            "SELECT ops.enqueue_public_projection_from_anonymous_dispatch($1,'forged',1,$2,NULL,1)",
            &[&Uuid::new_v4(), &Uuid::new_v4()],
        )
        .expect_err("arbitrary dispatch id must not authorize a projection enqueue");
    assert_eq!(
        forged_projection.code(),
        Some(&SqlState::INSUFFICIENT_PRIVILEGE),
        "projection enqueue requires the exact live anonymous source lease"
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT deny_unleased_projection_enqueue")
        .expect("rollback denied projection enqueue");

    let source_id = Uuid::new_v4();
    let envelope = vec![7_u8; 32];
    txn.batch_execute("SAVEPOINT deny_anonymous_insert")
        .expect("savepoint");
    // dep: PostgreSQL(any) — pool/txn query execution
    assert!(txn.execute(
        "INSERT INTO public.sources(source_id,source_type,content_hash,rights_basis,trust_class,lineage_mode,anonymous_envelope_sha256) \
         VALUES($1,'ANONYMOUS_USER_CONTRIBUTION','forged','forged','forged','ANONYMOUS_RELEASE',$2)",
        &[&source_id, &envelope],
    ).is_err(), "direct anonymous source creation must be rejected before lifecycle admission");
    txn.batch_execute("ROLLBACK TO SAVEPOINT deny_anonymous_insert")
        .expect("rollback denied insert");
    txn.batch_execute("SAVEPOINT deny_anonymous_admit_without_authority")
        .expect("savepoint");
    let direct_admit = txn
        .query_one(
            "SELECT public.admit_anonymous_source($1,$2,1)",
            &[&source_id, &envelope],
        )
        .expect_err("the old pair-only admit entry point is not executable by the public worker");
    assert_eq!(direct_admit.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE));
    txn.batch_execute("ROLLBACK TO SAVEPOINT deny_anonymous_admit_without_authority")
        .expect("rollback denied admission");
    txn.batch_execute("SAVEPOINT deny_anonymous_revoke_without_authority")
        .expect("savepoint");
    let direct_revoke = txn
        .query(
            "SELECT * FROM public.revoke_anonymous_source($1,$2,2)",
            &[&source_id, &envelope],
        )
        .expect_err("the old pair-only revoke entry point is not executable by the public worker");
    assert_eq!(
        direct_revoke.code(),
        Some(&SqlState::INSUFFICIENT_PRIVILEGE)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT deny_anonymous_revoke_without_authority")
        .expect("rollback denied revocation");
    txn.batch_execute("SAVEPOINT deny_wrong_dispatch_bridge")
        .expect("savepoint");
    let wrong_dispatch = txn
        .query(
            "SELECT * FROM public.admit_anonymous_dispatch($1,'forged',1)",
            &[&Uuid::new_v4()],
        )
        .expect_err("an unleased dispatch cannot bridge anonymous admission");
    assert_eq!(
        wrong_dispatch.code(),
        Some(&SqlState::INSUFFICIENT_PRIVILEGE)
    );
    txn.batch_execute("ROLLBACK TO SAVEPOINT deny_wrong_dispatch_bridge")
        .expect("rollback denied bridge");
    assert!(
        txn.query(
            "SELECT * FROM staging.read_sanitized_public_candidate($1,$2)",
            &[&source_id, &envelope]
        )
        .is_err(),
        "public worker must not call the protected staging lookup function"
    );
}

#[test]
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL migrated through 0127"]
fn private_coverage_contract_returns_a_single_empty_snapshot_in_an_empty_pool() {
    let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("isolated PostgreSQL DSN");
    // dep: PostgreSQL(role_private_worker) — open a role-scoped PG connection/pool for this test
    let mut client = Client::connect(&dsn, NoTls).expect("isolated PostgreSQL");
    let mut txn = client.transaction().expect("transaction");
    txn.batch_execute("SET LOCAL ROLE role_private_worker")
        .expect("private worker role");
    let rows = txn
        .query(
            "SELECT snapshot_id,coverage_version,summary FROM public.phase9_public_coverage_for_probe(convert_to('no-match','UTF8'),32)",
            &[],
        )
        .expect("bounded coverage function");
    assert_eq!(
        rows.len(),
        1,
        "coverage shape is stable even when no object matches"
    );
    assert_eq!(rows[0].get::<_, i32>("coverage_version"), 1);
}
