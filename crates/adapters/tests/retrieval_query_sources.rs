//! `adapters::tests::retrieval_query_sources` — Real-role acceptance for migration 0115's metadata-only
//!   retrieval-query source.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-local-secret-scan, humaux-retrieval, humaux-testkit,
//!   postgres, sqlx, tokio]; services=[PostgreSQL(owner) r=[ops.commit_seq_seq] w=[control.memberships,
//!   control.private_reasoning_domains, control.tenants, control.users, control.workspaces,
//!   ops.data_disclosure_sources, ops.data_disclosures, ops.outbox, private.events, private.evidence_objects,
//!   private.memory_consolidation_runs, private.memory_evidence, private.memory_records, private.memory_rollups,
//!   private.retrieval_query_sources, staging.contribution_release_sources, staging.contribution_releases]
//!   x=[ops.attach_retrieval_query_source], PostgreSQL(role_maintenance), PostgreSQL(role_migration_owner),
//!   PostgreSQL(role_retrieval_worker)]; env=[HUMAUX_MAINTENANCE_PG_DSN, HUMAUX_RETRIEVAL_WORKER_PG_DSN,
//!   HUMAUX_TEST_GITLEAKS_BIN, HUMAUX_TEST_GITLEAKS_SHA256, HUMAUX_TEST_GITLEAKS_VERSION, HUMAUX_TEST_PG_DSN];
//!   modules=[adapters::disclosure, adapters::postgres, adapters::retrieval_query_source, domain::dataclass,
//!   domain::egress, domain::identity, domain::ids, humaux-local-secret-scan, humaux-testkit, retrieval::request]
//! Called-by: [cargo-test]
//! Invariants: [fixture rows live under a throwaway tenant; attaching a query source with the wrong permit is
//!   WrongPermit; skips route through run_db_fixture so HUMAUX_REQUIRE_DB=1 makes an unreachable DB red]
//! Spec: ADR-0047; ADR-0050
//!
//! depends-on: Postgres at `HUMAUX_TEST_PG_DSN` (owner, any loopback port/database — ADR-0047
//! D-D, ADR-0050 D-B) plus `HUMAUX_{RETRIEVAL_WORKER,MAINTENANCE}_PG_DSN` on the same target
//! (or the legacy 61719 pair); retrieval-query source functions from migration 0115.
//! called-by: `cargo test -p humaux-adapters --test retrieval_query_sources` (chain `adapters_tests`).
//! invariants: fixture rows live under a throwaway tenant; skips route through `run_db_fixture`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use humaux_adapters::{
    disclosure::{self, DisclosureSource},
    postgres::{MaintenanceDbPool, RetrievalWorkerDbPool},
    retrieval_query_source::{
        RetrievalQueryCallContext, RetrievalQuerySourceError, SerializedRetrievalQueryBatch,
        get_retrieval_query_source, reserve_retrieval_query_batch, revoke_retrieval_query_source,
    },
};
use humaux_domain::{
    dataclass::DataClass,
    egress::{self, AuthorizedEgressPayload, PrivateDataPurpose, ProcessorId},
    identity::{AuthorizationScope, BoundedSet, PrincipalId},
    ids::{TenantId, UserId, WorkspaceId},
};
use humaux_local_secret_scan::{
    LocalSecretScanner, LocalSecretScannerConfig, SealedRetrievalQuery,
};
use humaux_retrieval::request::{RetrievalIntent, build_request};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::error::SqlState;
use postgres::{Client, NoTls};
use sqlx::postgres::PgConnectOptions;
use sqlx::types::Uuid;

const FIXTURE_DB: &str = "humaux_thread_request_guard_20260828";

struct Handle {
    rt: tokio::runtime::Runtime,
    retrieval: RetrievalWorkerDbPool,
    maintenance: MaintenanceDbPool,
    admin: Client,
    tenant_id: Uuid,
    workspace_id: Uuid,
    evidence_id: Uuid,
    memory_id: Uuid,
    rollup_id: Uuid,
    release_id: Uuid,
    auth: AuthorizationScope,
    retrieval_dsn: String,
}

struct Fixture;

fn setup_error(stage: &str) -> DbFixtureSkipReason {
    DbFixtureSkipReason::IsolationSetupFailed(format!(
        "retrieval query source fixture setup failed at {stage}"
    ))
}

fn seed_memory_and_link(
    admin: &mut Client,
    tenant_id: Uuid,
    evidence_id: Uuid,
) -> Result<Uuid, DbFixtureSkipReason> {
    let mut txn = admin
        .transaction()
        .map_err(|_| setup_error("begin memory/evidence seed"))?;
    let memory_id: Uuid = txn
        .query_one(
            "INSERT INTO private.memory_records \
               (tenant_id,memory_type,content,visibility_class,authority_class,confidence,status,asserted_at) \
             VALUES($1,'NOTE','{}'::jsonb,'TENANT_SHARED','PrivateKnowledge',0.9,'active',clock_timestamp()) \
             RETURNING memory_id",
            &[&tenant_id],
        )
        .map_err(|_| setup_error("seed memory"))?
        .get(0);
    txn.execute(
        "INSERT INTO private.memory_evidence(memory_id,evidence_id,role,grounding_mode) \
         VALUES($1,$2,'PRIMARY','SNAPSHOT')",
        &[&memory_id, &evidence_id],
    )
    .map_err(|_| setup_error("seed memory evidence"))?;
    txn.commit()
        .map_err(|_| setup_error("commit memory/evidence seed"))?;
    Ok(memory_id)
}

/// The owner (`HUMAUX_TEST_PG_DSN`, `expected_role = None`) defines the fixture target; a role
/// DSN must name the same port and database as the owner, or the legacy
/// `61719 / FIXTURE_DB` pair. This is a read of ADR-0047 D-D (ADR-0050 D-B), the same
/// predicate as `request_guard.rs::same_target`, not a second rule. Loopback and a
/// query-free DSN stay enforced for every DSN.
fn checked_dsn(name: &str, expected_role: Option<&str>) -> Result<String, DbFixtureSkipReason> {
    let dsn = std::env::var(name).map_err(|_| setup_error("required DSN"))?;
    let options = PgConnectOptions::from_str(&dsn).map_err(|_| setup_error("DSN parse"))?;
    let same_target = expected_role.is_none()
        || (options.get_port() == 61719 && options.get_database() == Some(FIXTURE_DB))
        || std::env::var("HUMAUX_TEST_PG_DSN")
            .ok()
            .and_then(|owner| PgConnectOptions::from_str(&owner).ok())
            .is_some_and(|owner| {
                options.get_port() == owner.get_port()
                    && options.get_database() == owner.get_database()
            });
    if expected_role.is_some_and(|role| options.get_username() != role)
        || options.get_host() != "127.0.0.1"
        || !same_target
        || dsn.contains(['?', '#'])
    {
        return Err(setup_error("DSN boundary validation"));
    }
    Ok(dsn)
}

fn role_login_ok(dsn: &str, role: &str) -> Result<(), DbFixtureSkipReason> {
    // dep: PostgreSQL(owner) — legacy tag rewritten to convention grammar
    // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
    let mut client = Client::connect(dsn, NoTls)
        .map_err(|_| setup_error(&format!("actual role login for {role}")))?;
    let ok: bool = client
        .query_one(
            "SELECT current_user=$1 AND session_user=$1 \
             AND NOT (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user)",
            &[&role],
        )
        .map_err(|_| setup_error(&format!("actual role identity probe for {role}")))?
        .get(0);
    ok.then_some(())
        .ok_or_else(|| setup_error(&format!("actual role identity mismatch for {role}")))
}

struct SeedContext {
    tenant_id: Uuid,
    user_id: Uuid,
    principal_id: Uuid,
    workspace_id: Uuid,
    evidence_id: Uuid,
    memory_id: Uuid,
    rollup_id: Uuid,
    release_id: Uuid,
}

fn seed_contribution_release(
    admin: &mut Client,
    tenant_id: Uuid,
    evidence_id: Uuid,
) -> Result<Uuid, DbFixtureSkipReason> {
    let mut tx = admin
        .transaction()
        .map_err(|_| setup_error("begin contribution release"))?;
    let release_id = tx
        .query_one(
            "INSERT INTO staging.contribution_releases \
               (tenant_id,policy_snapshot,state,privacy_scan_outcome,secret_scan_outcome,rights_basis) \
             VALUES($1,'{\"policy\":\"MANUAL\",\"consent_version\":\"fixture-v1\"}'::jsonb, \
                    'ACTIVE','PASSED','PASSED','fixture') \
             RETURNING contribution_release_id",
            &[&tenant_id],
        )
        .map_err(|_| setup_error("seed contribution release"))?
        .get(0);
    tx.execute(
        "INSERT INTO staging.contribution_release_sources \
           (tenant_id,contribution_release_id,evidence_id,ordinal) VALUES($1,$2,$3,0)",
        &[&tenant_id, &release_id, &evidence_id],
    )
    .map_err(|_| setup_error("seed contribution release source"))?;
    tx.execute(
        "INSERT INTO ops.outbox(tenant_id,commit_seq,event_type,contribution_release_id) \
         VALUES($1,nextval('ops.commit_seq_seq'),'PUBLIC_RELEASE',$2)",
        &[&tenant_id, &release_id],
    )
    .map_err(|_| setup_error("seed contribution release outbox"))?;
    tx.commit()
        .map_err(|_| setup_error("commit contribution release"))?;
    Ok(release_id)
}

fn seed_authorization_context(admin: &mut Client) -> Result<SeedContext, DbFixtureSkipReason> {
    let tenant_id: Uuid = admin
        .query_one(
            "INSERT INTO control.tenants(name) VALUES('query-source fixture') RETURNING tenant_id",
            &[],
        )
        .map_err(|_| setup_error("seed tenant"))?
        .get(0);
    let user_id = Uuid::now_v7();
    let principal_id = Uuid::now_v7();
    admin
        .execute(
            "INSERT INTO control.users(user_id,state) VALUES($1,'ACTIVE')",
            &[&user_id],
        )
        .map_err(|_| setup_error("seed user"))?;
    let workspace_id: Uuid = admin
        .query_one(
            "INSERT INTO control.workspaces(tenant_id,name) VALUES($1,'query-source workspace') \
             RETURNING workspace_id",
            &[&tenant_id],
        )
        .map_err(|_| setup_error("seed workspace"))?
        .get(0);
    admin
        .execute(
            "INSERT INTO control.memberships(tenant_id,user_id,role,state) VALUES($1,$2,'member','ACTIVE')",
            &[&tenant_id, &user_id],
        )
        .map_err(|_| setup_error("seed membership"))?;
    let domain_id: Uuid = admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains(tenant_id,name) VALUES($1,'query-source domain') RETURNING reasoning_domain_id",
            &[&tenant_id],
        )
        .map_err(|_| setup_error("seed reasoning domain"))?
        .get(0);
    let evidence_id: Uuid = admin
        .query_one(
            "INSERT INTO private.evidence_objects(tenant_id,evidence_kind,payload_sha256,data_class,origin_class,visibility_class,reasoning_domain_id) \
             VALUES($1,'EVENT',$2,'PRIVATE','AuthenticatedAgent','TENANT_SHARED',$3) RETURNING evidence_id",
            &[&tenant_id, &vec![7_u8; 32], &domain_id],
        )
        .map_err(|_| setup_error("seed evidence"))?
        .get(0);
    admin
        .execute(
            "INSERT INTO private.events(event_id,event_kind,payload) VALUES($1,'TOOL_CALL','{}'::jsonb)",
            &[&evidence_id],
        )
        .map_err(|_| setup_error("seed event"))?;
    let memory_id = seed_memory_and_link(admin, tenant_id, evidence_id)?;
    let run_id: Uuid = admin
        .query_one(
            "INSERT INTO private.memory_consolidation_runs \
               (tenant_id,reasoning_domain_id,workspace_id,status) \
             VALUES($1,$2,$3,'PENDING') RETURNING run_id",
            &[&tenant_id, &domain_id, &workspace_id],
        )
        .map_err(|_| setup_error("seed consolidation run"))?
        .get(0);
    let rollup_id: Uuid = admin
        .query_one(
            "INSERT INTO private.memory_rollups \
               (tenant_id,run_id,content,authority_class,visibility_class) \
             VALUES($1,$2,'{}'::jsonb,'PrivateKnowledge','TENANT_SHARED') RETURNING rollup_id",
            &[&tenant_id, &run_id],
        )
        .map_err(|_| setup_error("seed rollup"))?
        .get(0);
    let release_id = seed_contribution_release(admin, tenant_id, evidence_id)?;
    Ok(SeedContext {
        tenant_id,
        user_id,
        principal_id,
        workspace_id,
        evidence_id,
        memory_id,
        rollup_id,
        release_id,
    })
}

impl DbIntegrationFixture for Fixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let owner_dsn = checked_dsn("HUMAUX_TEST_PG_DSN", None)?;
        let retrieval_dsn = checked_dsn(
            "HUMAUX_RETRIEVAL_WORKER_PG_DSN",
            Some("role_retrieval_worker"),
        )?;
        let maintenance_dsn = checked_dsn("HUMAUX_MAINTENANCE_PG_DSN", Some("role_maintenance"))?;
        role_login_ok(&retrieval_dsn, "role_retrieval_worker")?;
        role_login_ok(&maintenance_dsn, "role_maintenance")?;
        // dep: PostgreSQL(owner) — legacy tag rewritten to convention grammar
        let mut admin =
            // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
            Client::connect(&owner_dsn, NoTls).map_err(|_| setup_error("owner login"))?;
        let ready: bool = admin
            .query_one(
                "SELECT to_regclass('private.retrieval_query_sources') IS NOT NULL \
                 AND to_regprocedure('ops.attach_retrieval_query_source(uuid,uuid,uuid,integer)') IS NOT NULL",
                &[],
            )
            .map_err(|_| setup_error("migration readiness probe"))?
            .get(0);
        if !ready {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "migration 0115 is not applied".into(),
            ));
        }
        let seed = seed_authorization_context(&mut admin)?;
        let rt = tokio::runtime::Runtime::new().map_err(|_| setup_error("test runtime"))?;
        // dep: PostgreSQL(owner) — legacy tag rewritten to convention grammar
        let retrieval = rt
            // dep: PostgreSQL(role_retrieval_worker) — test opens a direct PG connection for setup/verification
            .block_on(RetrievalWorkerDbPool::connect(&retrieval_dsn))
            .map_err(|_| setup_error("retrieval worker pool"))?;
        // dep: PostgreSQL(owner) — legacy tag rewritten to convention grammar
        let maintenance = rt
            // dep: PostgreSQL(role_maintenance) — test opens a direct PG connection for setup/verification
            .block_on(MaintenanceDbPool::connect(&maintenance_dsn))
            .map_err(|_| setup_error("maintenance pool"))?;
        let auth = AuthorizationScope::new(
            TenantId(seed.tenant_id),
            PrincipalId(seed.principal_id),
            Some(UserId(seed.user_id)),
            BoundedSet::new([WorkspaceId(seed.workspace_id)])
                .map_err(|_| setup_error("authorization workspace set"))?,
        );
        Ok(Handle {
            rt,
            retrieval,
            maintenance,
            admin,
            tenant_id: seed.tenant_id,
            workspace_id: seed.workspace_id,
            evidence_id: seed.evidence_id,
            memory_id: seed.memory_id,
            rollup_id: seed.rollup_id,
            release_id: seed.release_id,
            auth,
            retrieval_dsn,
        })
    }
}

fn call_context<'a>(
    authorization: &'a AuthorizationScope,
    workspace_id: Uuid,
) -> RetrievalQueryCallContext<'a> {
    RetrievalQueryCallContext::new(
        authorization,
        WorkspaceId(workspace_id),
        Uuid::now_v7(),
        Uuid::now_v7(),
        1,
    )
    .expect("trusted query call context")
}

fn sealed_query(text: &str) -> SealedRetrievalQuery {
    sealed_query_with_top_k(text, None)
}

fn sealed_query_with_top_k(text: &str, top_k: Option<&str>) -> SealedRetrievalQuery {
    let scanner = LocalSecretScanner::new(LocalSecretScannerConfig {
        executable: PathBuf::from(
            std::env::var("HUMAUX_TEST_GITLEAKS_BIN")
                .expect("query source fixture requires HUMAUX_TEST_GITLEAKS_BIN"),
        ),
        expected_version: std::env::var("HUMAUX_TEST_GITLEAKS_VERSION")
            .expect("query source fixture requires HUMAUX_TEST_GITLEAKS_VERSION"),
        expected_executable_sha256: std::env::var("HUMAUX_TEST_GITLEAKS_SHA256")
            .expect("query source fixture requires HUMAUX_TEST_GITLEAKS_SHA256"),
        timeout: Duration::from_secs(5),
        max_payload_bytes: 64 * 1024,
        finding_exit_code: 1,
    })
    .expect("pinned scanner fixture");
    let mut raw = BTreeMap::new();
    if let Some(top_k) = top_k {
        raw.insert("retrieval.profile.top_k".to_string(), top_k.to_string());
    }
    let profile = humaux_retrieval::request::resolve_registered_retrieval_profile(&raw)
        .expect("registered profile");
    let request = build_request(
        RetrievalIntent::new(text.to_owned(), vec![], BTreeSet::new(), BTreeSet::new())
            .expect("text intent"),
        &profile,
    )
    .expect("trusted request");
    scanner
        .seal_query(&request.trusted_query().expect("text query"))
        .expect("clean sealed query")
}

fn permit_and_payload(
    handle: &Handle,
) -> (humaux_domain::egress::EgressPermit, AuthorizedEgressPayload) {
    permit_and_payload_for_class(
        handle,
        DataClass::Private,
        b"{\"inputs\":[\"sealed\"]}".to_vec(),
    )
}

fn permit_and_payload_for_class(
    handle: &Handle,
    data_class: DataClass,
    bytes: Vec<u8>,
) -> (humaux_domain::egress::EgressPermit, AuthorizedEgressPayload) {
    let payload = AuthorizedEgressPayload::new(bytes);
    let permit = egress::authorize(
        TenantId(handle.tenant_id),
        ProcessorId(Uuid::now_v7()),
        PrivateDataPurpose::RetrievalEmbedding,
        data_class,
        &payload,
        Duration::from_secs(120),
    )
    .expect("non-secret embedding wire payload is permitted");
    (permit, payload)
}

fn query_wire(queries: &[SealedRetrievalQuery]) -> SerializedRetrievalQueryBatch<'_> {
    SerializedRetrievalQueryBatch::new("text-embedding-v4", 2, queries)
        .expect("valid deterministic query wire")
}

fn permit_for_query_wire(
    handle: &Handle,
    data_class: DataClass,
    wire: &SerializedRetrievalQueryBatch<'_>,
) -> humaux_domain::egress::EgressPermit {
    egress::authorize(
        TenantId(handle.tenant_id),
        ProcessorId(Uuid::now_v7()),
        PrivateDataPurpose::RetrievalEmbedding,
        data_class,
        wire.payload(),
        Duration::from_secs(120),
    )
    .expect("query wire permit")
}

fn source_and_disclosure_counts(admin: &mut Client, tenant_id: Uuid) -> (i64, i64, i64) {
    let row = admin
        .query_one(
            "SELECT \
               (SELECT count(*) FROM private.retrieval_query_sources WHERE tenant_id=$1), \
               (SELECT count(*) FROM ops.data_disclosures WHERE tenant_id=$1), \
               (SELECT count(*) FROM ops.data_disclosure_sources WHERE tenant_id=$1)",
            &[&tenant_id],
        )
        .expect("read source and disclosure counts");
    (row.get(0), row.get(1), row.get(2))
}

fn assert_rejected_before_any_write(
    rt: &tokio::runtime::Runtime,
    retrieval: &RetrievalWorkerDbPool,
    admin: &mut Client,
    tenant_id: Uuid,
    permit: &humaux_domain::egress::EgressPermit,
    context: &RetrievalQueryCallContext<'_>,
    wire: &SerializedRetrievalQueryBatch<'_>,
) {
    let before = source_and_disclosure_counts(admin, tenant_id);
    let result = rt.block_on(reserve_retrieval_query_batch(
        retrieval,
        context,
        wire,
        permit,
        "cn-hangzhou",
    ));
    assert!(
        result.is_err(),
        "invalid sealed-query metadata must be denied"
    );
    assert_eq!(source_and_disclosure_counts(admin, tenant_id), before);
}

fn assert_query_reserve_negative_gates(
    handle: &mut Handle,
    context: &RetrievalQueryCallContext<'_>,
    query: &SealedRetrievalQuery,
) {
    let one_query = [query.clone()];
    let one_wire = query_wire(&one_query);
    let public_permit = permit_for_query_wire(handle, DataClass::Public, &one_wire);
    assert_rejected_before_any_write(
        &handle.rt,
        &handle.retrieval,
        &mut handle.admin,
        handle.tenant_id,
        &public_permit,
        context,
        &one_wire,
    );
    let before_empty = source_and_disclosure_counts(&mut handle.admin, handle.tenant_id);
    assert!(
        SerializedRetrievalQueryBatch::new("text-embedding-v4", 2, &[]).is_err(),
        "empty query batches must fail before a reserve transaction exists"
    );
    assert_eq!(
        source_and_disclosure_counts(&mut handle.admin, handle.tenant_id),
        before_empty
    );
    let other_queries = [sealed_query("different wire query")];
    let other_wire = query_wire(&other_queries);
    let mismatched_permit = permit_for_query_wire(handle, DataClass::Private, &other_wire);
    let before_mismatch = source_and_disclosure_counts(&mut handle.admin, handle.tenant_id);
    let mismatch = handle.rt.block_on(reserve_retrieval_query_batch(
        &handle.retrieval,
        context,
        &one_wire,
        &mismatched_permit,
        "cn-hangzhou",
    ));
    assert!(
        matches!(mismatch, Err(RetrievalQuerySourceError::WrongPermit)),
        "a permit for wire B must not authorize sealed query wire A"
    );
    assert_eq!(
        source_and_disclosure_counts(&mut handle.admin, handle.tenant_id),
        before_mismatch,
        "wire mismatch must fail before source/disclosure writes"
    );
    let mixed_profiles = [
        sealed_query("default profile query"),
        sealed_query_with_top_k("deeper profile query", Some("10")),
    ];
    let mixed_wire = query_wire(&mixed_profiles);
    let mixed_permit = permit_for_query_wire(handle, DataClass::Private, &mixed_wire);
    assert_rejected_before_any_write(
        &handle.rt,
        &handle.retrieval,
        &mut handle.admin,
        handle.tenant_id,
        &mixed_permit,
        context,
        &mixed_wire,
    );
}

fn assert_negative_query_ordinal_rejected(handle: &mut Handle, query_source_id: Uuid) {
    let before = source_and_disclosure_counts(&mut handle.admin, handle.tenant_id);
    // dep: PostgreSQL(owner) — legacy tag rewritten to convention grammar
    // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
    let mut retrieval = Client::connect(&handle.retrieval_dsn, NoTls)
        .expect("real retrieval-worker login for negative ordinal probe");
    let mut probe = retrieval
        .transaction()
        .expect("begin negative ordinal probe");
    let user_id = handle.auth.user_id().expect("fixture user").0;
    probe
        .execute(
            "SELECT set_config('humaux.tenant_id',$1,true), \
                    set_config('humaux.user_id',$2,true), \
                    set_config('humaux.principal_id',$3,true)",
            &[
                &handle.tenant_id.to_string(),
                &user_id.to_string(),
                &handle.auth.principal().0.to_string(),
            ],
        )
        .expect("scope real retrieval worker");
    let insert = probe.execute(
        "INSERT INTO private.retrieval_query_sources \
           (tenant_id,principal_id,user_id,workspace_id,request_id,logical_call_id,attempt_no, \
            profile_fingerprint,classifier_revision,data_class,purpose,query_sha256,query_bytes, \
            wire_payload_sha256,wire_payload_bytes,created_at,expires_at,revoked_at, \
            revocation_reason,query_ordinal) \
         SELECT tenant_id,principal_id,user_id,workspace_id,request_id,$2,attempt_no, \
                profile_fingerprint,classifier_revision,data_class,purpose,query_sha256, \
                query_bytes,wire_payload_sha256,wire_payload_bytes,created_at,expires_at, \
                revoked_at,revocation_reason,-1 \
         FROM private.retrieval_query_sources WHERE query_source_id=$1",
        &[&query_source_id, &Uuid::now_v7()],
    );
    assert_eq!(
        insert
            .expect_err("negative query ordinal must be rejected")
            .code(),
        Some(&SqlState::CHECK_VIOLATION)
    );
    probe.rollback().expect("rollback negative ordinal probe");
    assert_eq!(
        source_and_disclosure_counts(&mut handle.admin, handle.tenant_id),
        before,
        "negative ordinal rejection must not mutate durable provenance"
    );
}

#[test]
fn typed_query_source_reserve_is_atomic_and_generic_paths_cannot_forge_it() {
    run_db_fixture::<Fixture, _>("typed_query_source_reserve", |mut handle| {
        let authorization = handle.auth.clone();
        let context = call_context(&authorization, handle.workspace_id);
        let query = sealed_query("typed query source");
        assert_query_reserve_negative_gates(&mut handle, &context, &query);
        let queries = [query, sealed_query("second typed query source")];
        let wire = query_wire(&queries);
        let permit = permit_for_query_wire(&handle, DataClass::Private, &wire);
        let reservation = handle
            .rt
            .block_on(reserve_retrieval_query_batch(
                &handle.retrieval,
                &context,
                &wire,
                &permit,
                "cn-hangzhou",
            ))
            .expect("typed path reserves source and disclosure atomically");
        let rows = handle
            .admin
            .query(
                "SELECT s.source_kind, q.query_ordinal, s.ordinal, \
                        q.query_sha256, q.query_bytes, q.wire_payload_sha256, \
                        q.wire_payload_bytes, d.payload_sha256, d.payload_bytes \
                 FROM ops.data_disclosure_sources s \
                 JOIN private.retrieval_query_sources q ON q.query_source_id=s.query_source_id \
                 JOIN ops.data_disclosures d ON d.disclosure_id=s.disclosure_id \
                 WHERE s.disclosure_id=$1 ORDER BY s.ordinal",
                &[&reservation.disclosure_id],
            )
            .expect("typed reservation relations exist");
        assert_eq!(rows.len(), queries.len());
        assert_eq!(reservation.query_source_ids.len(), queries.len());
        for (ordinal, (row, query)) in rows.iter().zip(&queries).enumerate() {
            assert_eq!(row.get::<_, String>(0), "RETRIEVAL_QUERY");
            assert_eq!(row.get::<_, i32>(1), ordinal as i32);
            assert_eq!(row.get::<_, i32>(2), ordinal as i32);
            assert_eq!(row.get::<_, Vec<u8>>(3), query.payload_sha256_bytes());
            assert_eq!(row.get::<_, i64>(4), query.payload_bytes() as i64);
            assert_eq!(row.get::<_, Vec<u8>>(5), wire.payload().sha256());
            assert_eq!(row.get::<_, i64>(6), wire.payload().bytes().len() as i64);
            assert_eq!(row.get::<_, Vec<u8>>(7), wire.payload().sha256());
            assert_eq!(row.get::<_, i64>(8), wire.payload().bytes().len() as i64);
        }
        assert_negative_query_ordinal_rejected(&mut handle, reservation.query_source_ids[0]);
        let generic = handle.rt.block_on(disclosure::reserve_retrieval(
            &handle.retrieval,
            &permit,
            "cn-hangzhou",
            wire.payload(),
            None,
            &[DisclosureSource::RetrievalQuery(
                reservation.query_source_ids[0],
            )],
        ));
        assert!(
            generic.is_err(),
            "broad reserve must reject the fifth source kind"
        );
    });
}

#[test]
fn established_four_source_kinds_remain_positive_under_rewritten_constraints() {
    run_db_fixture::<Fixture, _>("established_four_source_positive", |mut handle| {
        let cases = [
            (
                DisclosureSource::Evidence(handle.evidence_id),
                "EVIDENCE",
                handle.evidence_id,
            ),
            (
                DisclosureSource::Memory(handle.memory_id),
                "MEMORY",
                handle.memory_id,
            ),
            (
                DisclosureSource::Rollup(handle.rollup_id),
                "ROLLUP",
                handle.rollup_id,
            ),
            (
                DisclosureSource::Release(handle.release_id),
                "PUBLIC_RELEASE",
                handle.release_id,
            ),
        ];

        for (source, expected_kind, expected_id) in cases {
            let (permit, payload) = permit_and_payload(&handle);
            let disclosure_id = handle
                .rt
                .block_on(disclosure::reserve_retrieval(
                    &handle.retrieval,
                    &permit,
                    "cn-hangzhou",
                    &payload,
                    None,
                    std::slice::from_ref(&source),
                ))
                .expect("established disclosure source remains legal");
            let row = handle
                .admin
                .query_one(
                    "SELECT source_kind, \
                            num_nonnulls(evidence_id,memory_id,rollup_id,release_id,query_source_id), \
                            coalesce(evidence_id,memory_id,rollup_id,release_id) \
                     FROM ops.data_disclosure_sources WHERE disclosure_id=$1",
                    &[&disclosure_id],
                )
                .expect("one established source relation");
            assert_eq!(row.get::<_, String>(0), expected_kind);
            assert_eq!(row.get::<_, i32>(1), 1);
            assert_eq!(row.get::<_, Uuid>(2), expected_id);
        }
    });
}

fn assert_query_source_acl(admin: &mut Client) {
    let grants = admin
        .query_one(
            "SELECT has_table_privilege('role_retrieval_worker','private.retrieval_query_sources','INSERT'), \
                    NOT has_table_privilege('role_gateway','private.retrieval_query_sources','SELECT'), \
                    NOT has_function_privilege('role_gateway','ops.attach_retrieval_query_source(uuid,uuid,uuid,integer)','EXECUTE'), \
                    has_function_privilege('role_retrieval_worker','ops.attach_retrieval_query_source(uuid,uuid,uuid,integer)','EXECUTE'), \
                    p.prosecdef AND r.rolname='role_migration_owner' \
                      AND NOT r.rolsuper AND NOT r.rolcreaterole AND NOT r.rolbypassrls \
                      AND p.proconfig @> ARRAY['search_path=pg_catalog']::text[] \
             FROM pg_proc p JOIN pg_roles r ON r.oid=p.proowner \
             WHERE p.oid='ops.attach_retrieval_query_source(uuid,uuid,uuid,integer)'::regprocedure",
            &[],
        )
        .expect("grant inspection");
    assert!(grants.get::<_, bool>(0) && grants.get::<_, bool>(1));
    assert!(grants.get::<_, bool>(2) && grants.get::<_, bool>(3));
    assert!(
        grants.get::<_, bool>(4),
        "typed attach stays owned by the restricted migration role with pg_catalog-only search path"
    );
}

fn assert_cross_tenant_source_relation_rejected(handle: &mut Handle, query_source_id: Uuid) {
    let tenant_b: Uuid = handle
        .admin
        .query_one(
            "INSERT INTO control.tenants(name) VALUES('query-source foreign tenant') RETURNING tenant_id",
            &[],
        )
        .expect("seed foreign tenant")
        .get(0);
    let foreign_disclosure: Uuid = handle
        .admin
        .query_one(
            "INSERT INTO ops.data_disclosures \
               (grant_id,tenant_id,processor_id,region,data_class,purpose,payload_sha256,payload_bytes) \
             VALUES($1,$2,$3,'test','PRIVATE','RETRIEVAL_EMBEDDING',$4,1) \
             RETURNING disclosure_id",
            &[&Uuid::now_v7(), &tenant_b, &Uuid::now_v7(), &vec![9_u8; 32]],
        )
        .expect("seed foreign disclosure")
        .get(0);
    let mut cross_tenant = handle
        .admin
        .transaction()
        .expect("begin cross-tenant injection");
    cross_tenant
        .execute(
            "SELECT set_config('humaux.tenant_id',$1,true)",
            &[&handle.tenant_id.to_string()],
        )
        .expect("scope injection to source tenant");
    cross_tenant
        // dep: PostgreSQL(role_migration_owner) — test switches PG role to exercise RLS
        .batch_execute("SET LOCAL ROLE role_migration_owner")
        .expect("exercise FK beneath query-source insert guard");
    let injection = cross_tenant.execute(
        "INSERT INTO ops.data_disclosure_sources \
           (tenant_id,disclosure_id,source_kind,query_source_id,ordinal) \
         VALUES($1,$2,'RETRIEVAL_QUERY',$3,99)",
        &[&handle.tenant_id, &foreign_disclosure, &query_source_id],
    );
    assert_eq!(
        injection
            .expect_err("A source cannot attach to a B disclosure")
            .code(),
        Some(&SqlState::FOREIGN_KEY_VIOLATION),
        "composite disclosure FK must reject cross-tenant relation injection"
    );
    cross_tenant.rollback().expect("rollback failed injection");
}

fn assert_revoked_source_retention(
    handle: &mut Handle,
    query_source_id: Uuid,
    permit: &humaux_domain::egress::EgressPermit,
    payload: &AuthorizedEgressPayload,
) {
    assert!(
        handle
            .rt
            .block_on(revoke_retrieval_query_source(
                &handle.maintenance,
                handle.tenant_id,
                query_source_id,
                "credential revoked",
            ))
            .expect("maintenance revocation")
    );
    assert_revoked_source_cannot_attach(handle, query_source_id, payload);
    let source = handle
        .rt
        .block_on(get_retrieval_query_source(
            &handle.retrieval,
            &handle.auth,
            query_source_id,
        ))
        .expect("source read")
        .expect("source still retained");
    assert!(
        source.revoked_at.is_some(),
        "revocation retains metadata instead of deleting it"
    );
    assert_source_is_immutable_and_bodyless(&mut handle.admin, query_source_id);
    let old_source = handle.rt.block_on(disclosure::reserve_retrieval(
        &handle.retrieval,
        permit,
        "cn-hangzhou",
        payload,
        None,
        &[
            DisclosureSource::Evidence(handle.evidence_id),
            DisclosureSource::Memory(handle.memory_id),
        ],
    ));
    assert!(
        old_source.is_ok(),
        "existing evidence and memory disclosure reserves remain legal"
    );
}

fn assert_revoked_source_cannot_attach(
    handle: &mut Handle,
    query_source_id: Uuid,
    payload: &AuthorizedEgressPayload,
) {
    // dep: PostgreSQL(owner) — legacy tag rewritten to convention grammar
    // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
    let mut retrieval_client = Client::connect(&handle.retrieval_dsn, NoTls)
        .expect("real retrieval-worker login for revoked-source probe");
    let mut revoked_probe = retrieval_client
        .transaction()
        .expect("begin revoked-source probe");
    revoked_probe
        .execute(
            "SELECT set_config('humaux.tenant_id',$1,true)",
            &[&handle.tenant_id.to_string()],
        )
        .expect("scope real retrieval worker");
    let later_disclosure: Uuid = revoked_probe
        .query_one(
            "INSERT INTO ops.data_disclosures \
               (grant_id,tenant_id,processor_id,region,data_class,purpose,payload_sha256,payload_bytes) \
             VALUES($1,$2,$3,'test','PRIVATE','RETRIEVAL_EMBEDDING',$4,$5) \
             RETURNING disclosure_id",
            &[
                &Uuid::now_v7(),
                &handle.tenant_id,
                &Uuid::now_v7(),
                &payload.sha256().to_vec(),
                &(payload.bytes().len() as i64),
            ],
        )
        .expect("reserve probe disclosure")
        .get(0);
    let revoked_attach = revoked_probe.query_one(
        "SELECT ops.attach_retrieval_query_source($1,$2,$3,0)",
        &[&handle.tenant_id, &later_disclosure, &query_source_id],
    );
    assert_eq!(
        revoked_attach
            .expect_err("revoked source must not attach")
            .code(),
        Some(&SqlState::CHECK_VIOLATION),
        "source lifecycle is checked at attach, not only during creation"
    );
    revoked_probe
        .rollback()
        .expect("rollback revoked-source probe");
}

fn assert_source_is_immutable_and_bodyless(admin: &mut Client, query_source_id: Uuid) {
    let immutable = admin.execute(
        "UPDATE private.retrieval_query_sources SET profile_fingerprint='mutated' WHERE query_source_id=$1",
        &[&query_source_id],
    );
    assert!(immutable.is_err(), "source metadata must remain immutable");
    let no_body: bool = admin
        .query_one(
            "SELECT NOT EXISTS (SELECT 1 FROM information_schema.columns \
             WHERE table_schema='private' AND table_name='retrieval_query_sources' \
               AND column_name IN ('query','query_text','payload','wire_payload'))",
            &[],
        )
        .expect("metadata schema inspection")
        .get(0);
    assert!(
        no_body,
        "query source table must not persist query or provider wire bodies"
    );
}

#[test]
fn query_source_grants_tenant_fk_and_one_way_revocation_hold_under_real_roles() {
    run_db_fixture::<Fixture, _>("query_source_grants_and_revocation", |mut handle| {
        let query = sealed_query("revocation fixture query");
        let queries = [query];
        let wire = query_wire(&queries);
        let permit = permit_for_query_wire(&handle, DataClass::Private, &wire);
        let context = call_context(&handle.auth, handle.workspace_id);
        let reservation = handle
            .rt
            .block_on(reserve_retrieval_query_batch(
                &handle.retrieval,
                &context,
                &wire,
                &permit,
                "cn-hangzhou",
            ))
            .expect("seed typed query source");
        assert_query_source_acl(&mut handle.admin);
        assert_cross_tenant_source_relation_rejected(&mut handle, reservation.query_source_ids[0]);
        assert_revoked_source_retention(
            &mut handle,
            reservation.query_source_ids[0],
            &permit,
            wire.payload(),
        );
    });
}
