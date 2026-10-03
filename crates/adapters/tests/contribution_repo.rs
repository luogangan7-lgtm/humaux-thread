//! `adapters::tests::contribution_repo` — §12/§13 real-Postgres integration coverage for contribution repository IO.
//! Depends-on: crates=[humaux-adapters, humaux-domain, humaux-testkit, postgres, serde_json, sqlx, tokio];
//!   services=[PostgreSQL(any) r=[ops.card_c_outbox_fault_, ops.commit_seq_seq] w=[control.private_reasoning_domains,
//!   control.tenants, ops.jobs, ops.outbox, ops.public_release_revocations, private.events, private.evidence_objects,
//!   private.memory_evidence, private.memory_records, public.claims, public.provenance_edges, public.source_closure,
//!   public.sources, public.syntheses, public.synthesis_inputs, staging.contribution_release_sources,
//!   staging.contribution_releases], PostgreSQL(role_private_worker), PostgreSQL(role_public_worker)];
//!   env=[HUMAUX_TEST_PG_DSN]; modules=[adapters::contribution_repo, adapters::postgres, domain::authority,
//!   domain::error, domain::ids, domain::public, humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [exercises a checked PublicWorkerDbPool (schema setup/cleanup is admin-only); tenant and visibility
//!   violations must be TenantBoundary/Forbidden; an isolation setup failure is a fixture error]
//! Spec: none
//!
//! These tests
//! deliberately exercise a checked `PublicWorkerDbPool`; schema setup/cleanup is admin-only.

use std::{
    sync::{Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};

use humaux_adapters::{
    contribution_repo,
    postgres::{PrivateWorkerDbPool, PublicWorkerDbPool},
};
use humaux_domain::{
    authority::{EvidenceId, MemoryId},
    ids::TenantId,
    public::{
        ContributionPolicy, ContributionRelease, ModerationState, ReleaseSource, RightsProvenance,
        ScanOutcome,
    },
};
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use sqlx::types::Uuid;

static SERIAL: Mutex<()> = Mutex::new(());

fn dsn_as_role(dsn: &str, role: &str) -> String {
    let sep = if dsn.contains('?') { '&' } else { '?' };
    format!("{dsn}{sep}options=-c%20role%3D{role}")
}

fn dsn_as_role_with_application(dsn: &str, role: &str, application_name: &str) -> String {
    format!(
        "{}&application_name={application_name}",
        dsn_as_role(dsn, role)
    )
}

struct Handle {
    rt: tokio::runtime::Runtime,
    public: PublicWorkerDbPool,
    private: PrivateWorkerDbPool,
    admin: Client,
    tenant_id: Uuid,
    reasoning_domain_id: Uuid,
    evidence_id: Uuid,
    memory_id: Uuid,
    source_ids: Vec<Uuid>,
    claim_ids: Vec<Uuid>,
    synthesis_ids: Vec<Uuid>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Admin-only fixture cleanup; production repository functions contain no DELETE.
        let ids = |values: &[Uuid]| {
            values
                .iter()
                .map(Uuid::to_string)
                .collect::<Vec<_>>()
                .join("','")
        };
        let sources = ids(&self.source_ids);
        let claims = ids(&self.claim_ids);
        let syntheses = ids(&self.synthesis_ids);
        // Card-31 pattern (card 33 leak fix): the seeded PRIMARY memory_evidence link enqueues a
        // DERIVED_CONSOLIDATE job (0164 trigger). Jobs go first, in one batch with the data rows,
        // and a failure is printed; the tenant row goes in a separate best-effort batch, so a
        // refused tenant delete can no longer roll the job delete back.
        if let Err(error) = self.admin.batch_execute(&format!(
            "DELETE FROM ops.jobs WHERE tenant_id = '{}'; \
             DELETE FROM public.source_closure WHERE root_source_id IN ('{sources}') OR claim_id IN ('{claims}') OR synthesis_id IN ('{syntheses}'); \
             DELETE FROM public.provenance_edges WHERE source_id IN ('{sources}') OR claim_id IN ('{claims}'); \
             DELETE FROM public.synthesis_inputs WHERE synthesis_id IN ('{syntheses}') OR claim_id IN ('{claims}') OR input_synthesis_id IN ('{syntheses}'); \
             DELETE FROM public.syntheses WHERE synthesis_id IN ('{syntheses}'); \
             DELETE FROM public.claims WHERE claim_id IN ('{claims}'); \
             DELETE FROM public.sources WHERE source_id IN ('{sources}'); \
             DELETE FROM ops.public_release_revocations WHERE release_id IN (SELECT contribution_release_id FROM staging.contribution_releases WHERE tenant_id = '{}'); \
             DELETE FROM ops.outbox WHERE tenant_id = '{}'; \
             DELETE FROM staging.contribution_release_sources WHERE tenant_id = '{}'; \
             DELETE FROM staging.contribution_releases WHERE tenant_id = '{}'; \
             DELETE FROM private.events WHERE event_id = '{}'; \
             DELETE FROM private.memory_evidence WHERE memory_id = '{}'; \
             DELETE FROM private.memory_records WHERE memory_id = '{}'; \
             DELETE FROM private.evidence_objects WHERE evidence_id = '{}'; \
             DELETE FROM control.private_reasoning_domains WHERE reasoning_domain_id = '{}';",
            self.tenant_id, self.tenant_id, self.tenant_id, self.tenant_id, self.tenant_id, self.evidence_id,
            self.memory_id, self.memory_id, self.evidence_id, self.reasoning_domain_id,
        )) {
            eprintln!(
                "contribution_repo cleanup failed for tenant {}: {error}",
                self.tenant_id
            );
        }
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM control.tenants WHERE tenant_id = '{}';",
            self.tenant_id
        ));
    }
}

struct Fixture;

fn seed_memory_with_evidence(
    admin: &mut Client,
    tenant_id: Uuid,
    evidence_id: Uuid,
) -> Result<Uuid, DbFixtureSkipReason> {
    let mut memory_tx = admin
        .transaction()
        .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
    let memory_id: Uuid = memory_tx
        .query_one(
            "INSERT INTO private.memory_records \
             (tenant_id, memory_type, content, visibility_class, authority_class, confidence, status, asserted_at) \
             VALUES ($1, 'FACT', '{}'::jsonb, 'TENANT_SHARED', 'PrivateKnowledge', 1, 'active', now()) \
             RETURNING memory_id",
            &[&tenant_id],
        )
        .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
        .get(0);
    memory_tx
        .execute(
            "INSERT INTO private.memory_evidence (memory_id, evidence_id, role) VALUES ($1, $2, 'SUPPORTING')",
            &[&memory_id, &evidence_id],
        )
        .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
    memory_tx
        .commit()
        .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
    Ok(memory_id)
}

impl DbIntegrationFixture for Fixture {
    type Handle = Handle;

    fn isolate() -> Result<Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        // dep: PostgreSQL(any) — opens the role-scoped connection for `isolate`
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
        let ready: bool = admin.query_one(
            "SELECT to_regclass('public.synthesis_inputs') IS NOT NULL AND \
                    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'source_closure' AND column_name = 'synthesis_id')",
            &[],
        ).map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?.get(0);
        if !ready {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "migration 0104 is not applied".to_owned(),
            ));
        }
        let tenant_id: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
                &[&format!("contribution_repo_test_{}", Uuid::new_v4())],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        let reasoning_domain_id: Uuid = admin.query_one(
            "INSERT INTO control.private_reasoning_domains (tenant_id, name) VALUES ($1, $2) RETURNING reasoning_domain_id",
            &[&tenant_id, &format!("contribution_repo_domain_{}", Uuid::new_v4())],
        ).map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?.get(0);
        let evidence_id: Uuid = admin.query_one(
            "INSERT INTO private.evidence_objects \
             (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, visibility_class, reasoning_domain_id) \
             VALUES ($1, 'EVENT', $2, 'INTERNAL', 'DirectUserInput', 'TENANT_SHARED', $3) RETURNING evidence_id",
            &[&tenant_id, &vec![0_u8; 32], &reasoning_domain_id],
        ).map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?.get(0);
        admin.execute("INSERT INTO private.events (event_id, event_kind, payload) VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)", &[&evidence_id])
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let memory_id = seed_memory_with_evidence(&mut admin, tenant_id, evidence_id)?;
        let source_ids = (0..3)
            .map(|_| {
                admin.query_one(
                "INSERT INTO public.sources (source_type, content_hash, rights_basis, trust_class) \
                 VALUES ('OFFICIAL_DOCUMENT', $1, 'fixture rights', 'fixture') RETURNING source_id",
                &[&Uuid::new_v4().to_string()],
            ).map(|row| row.get(0))
            })
            .collect::<Result<Vec<Uuid>, _>>()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let claim_ids = (0..3)
            .map(|_| {
                admin
                    .query_one(
                        "INSERT INTO public.claims (content) VALUES ('{}') RETURNING claim_id",
                        &[],
                    )
                    .map(|row| row.get(0))
            })
            .collect::<Result<Vec<Uuid>, _>>()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        for (claim_id, source_id) in claim_ids.iter().zip(source_ids.iter()) {
            admin
                .execute(
                    "INSERT INTO public.provenance_edges (claim_id, source_id) VALUES ($1, $2)",
                    &[claim_id, source_id],
                )
                .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        }
        let synthesis_ids = (0..3).map(|_| {
            admin.query_one("INSERT INTO public.syntheses (content) VALUES ('{}') RETURNING synthesis_id", &[]).map(|row| row.get(0))
        }).collect::<Result<Vec<Uuid>, _>>().map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let public_dsn = dsn_as_role(&dsn, "role_public_worker");
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        // dep: PostgreSQL(role_public_worker) — opens the role-scoped connection for `isolate`
        let public = rt
            .block_on(PublicWorkerDbPool::connect(&public_dsn))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        // dep: PostgreSQL(role_private_worker) — opens the role-scoped connection for `isolate`
        let private = rt
            .block_on(PrivateWorkerDbPool::connect(&dsn_as_role(
                &dsn,
                "role_private_worker",
            )))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        Ok(Handle {
            rt,
            public,
            private,
            admin,
            tenant_id,
            reasoning_domain_id,
            evidence_id,
            memory_id,
            source_ids,
            claim_ids,
            synthesis_ids,
        })
    }
}

fn release_with_sources(sources: Vec<ReleaseSource>) -> ContributionRelease {
    ContributionRelease::release(
        ContributionPolicy::Manual,
        "fixture-consent-v1".to_owned(),
        RightsProvenance::new("fixture rights".to_owned(), None, None, None, None)
            .expect("fixture rights"),
        ScanOutcome::Passed,
        ScanOutcome::Passed,
        sources,
    )
    .expect("release shape")
}

fn admit_contribution_source(dsn: &str, tenant_id: Uuid, release_id: Uuid) -> Uuid {
    // dep: PostgreSQL(any) — opens the role-scoped connection for `admit_contribution_source`
    let mut source_client = Client::connect(&dsn_as_role(dsn, "role_public_worker"), NoTls)
        .expect("public role source client");
    let mut source_tx = source_client.transaction().expect("source transaction");
    source_tx
        .batch_execute(&format!("SET LOCAL humaux.tenant_id = '{tenant_id}'"))
        .expect("tenant context");
    let source_id: Uuid = source_tx.query_one(
        "INSERT INTO public.sources (source_type, contribution_release_id, content_hash, rights_basis, trust_class) \
         VALUES ('USER_CONTRIBUTION', $1, $2, 'fixture rights', 'fixture') RETURNING source_id",
        &[&release_id, &Uuid::new_v4().to_string()],
    ).expect("admit source").get(0);
    source_tx.commit().expect("commit source");
    source_id
}

fn wait_for_advisory_waiter(
    admin: &mut Client,
    application_name: &str,
    timeout: Duration,
) -> Result<(), String> {
    let until = Instant::now() + timeout;
    loop {
        let waiters: i64 = admin
            .query_one(
                "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a USING (pid) \
                 WHERE a.application_name = $1 AND l.locktype = 'advisory' AND NOT l.granted",
                &[&application_name],
            )
            .expect("advisory lock state")
            .get(0);
        if waiters > 0 {
            return Ok(());
        }
        if Instant::now() >= until {
            return Err(format!("{application_name} never waited on advisory lock"));
        }
        thread::yield_now();
    }
}

fn wait_for_granted_advisory_lock(
    admin: &mut Client,
    application_name: &str,
    timeout: Duration,
) -> Result<(), String> {
    let until = Instant::now() + timeout;
    loop {
        let granted: i64 = admin
            .query_one(
                "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a USING (pid) \
                 WHERE a.application_name = $1 AND l.locktype = 'advisory' AND l.granted",
                &[&application_name],
            )
            .expect("advisory lock state")
            .get(0);
        if granted > 0 {
            return Ok(());
        }
        if Instant::now() >= until {
            return Err(format!("{application_name} never acquired advisory lock"));
        }
        thread::yield_now();
    }
}

fn wait_for_transactionid_waiter(
    admin: &mut Client,
    application_name: &str,
    timeout: Duration,
) -> Result<(), String> {
    let until = Instant::now() + timeout;
    loop {
        let waiters: i64 = admin
            .query_one(
                "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a USING (pid) \
                 WHERE a.application_name = $1 AND l.locktype = 'transactionid' AND NOT l.granted \
                   AND a.wait_event_type = 'Lock' AND a.wait_event = 'transactionid'",
                &[&application_name],
            )
            .expect("database lock state")
            .get(0);
        if waiters > 0 {
            return Ok(());
        }
        if Instant::now() >= until {
            return Err(format!(
                "{application_name} never waited on source transaction"
            ));
        }
        thread::yield_now();
    }
}

fn wait_for_synthesis_relation_waiter(
    admin: &mut Client,
    application_name: &str,
    timeout: Duration,
) -> Result<(), String> {
    let until = Instant::now() + timeout;
    loop {
        let waiters: i64 = admin
            .query_one(
                "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a USING (pid) \
                 WHERE a.application_name = $1 AND l.locktype = 'relation' AND NOT l.granted \
                   AND l.relation = 'public.synthesis_inputs'::regclass",
                &[&application_name],
            )
            .expect("relation lock state")
            .get(0);
        if waiters > 0 {
            return Ok(());
        }
        if Instant::now() >= until {
            return Err(format!(
                "{application_name} never waited on public.synthesis_inputs"
            ));
        }
        thread::yield_now();
    }
}

fn wait_for_source_closure_relation_waiter(
    admin: &mut Client,
    application_name: &str,
    timeout: Duration,
) -> Result<(), String> {
    let until = Instant::now() + timeout;
    loop {
        let waiters: i64 = admin
            .query_one(
                "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a USING (pid) \
                 WHERE a.application_name = $1 AND l.locktype = 'relation' AND NOT l.granted \
                   AND l.relation = 'public.source_closure'::regclass",
                &[&application_name],
            )
            .expect("source closure lock state")
            .get(0);
        if waiters > 0 {
            return Ok(());
        }
        if Instant::now() >= until {
            return Err(format!(
                "{application_name} never waited on public.source_closure"
            ));
        }
        thread::yield_now();
    }
}

#[test]
fn release_and_repeat_revoke_write_exactly_one_event_each() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    run_db_fixture::<Fixture, _>("contribution_repo", |mut h| {
        let release = ContributionRelease::release(
            ContributionPolicy::Manual,
            "fixture-consent-v1".to_owned(),
            RightsProvenance::new("fixture rights".to_owned(), None, None, None, None)
                .expect("rights"),
            ScanOutcome::Passed,
            ScanOutcome::Passed,
            vec![humaux_domain::public::ReleaseSource::Evidence(EvidenceId(
                h.evidence_id,
            ))],
        )
        .expect("release shape");
        let release_id =
            h.rt.block_on(contribution_repo::create_release(
                &h.private,
                TenantId(h.tenant_id),
                &release,
            ))
            .expect("create release");
        assert!(
            h.rt.block_on(contribution_repo::revoke_release(
                &h.private,
                TenantId(h.tenant_id),
                release_id
            ))
            .expect("first revoke")
        );
        assert!(
            !h.rt
                .block_on(contribution_repo::revoke_release(
                    &h.private,
                    TenantId(h.tenant_id),
                    release_id
                ))
                .expect("repeat revoke")
        );
        let events: i64 = h
            .admin
            .query_one(
                "SELECT count(*) FROM ops.outbox WHERE contribution_release_id = $1",
                &[&release_id],
            )
            .expect("outbox query")
            .get(0);
        assert_eq!(events, 2);
    });
}

#[test]
fn cross_tenant_evidence_is_rejected_without_durable_release() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    run_db_fixture::<Fixture, _>("contribution_repo", |mut h| {
        let release = ContributionRelease::release(
            ContributionPolicy::Manual,
            "fixture-consent-v1".to_owned(),
            RightsProvenance::new("fixture rights".to_owned(), None, None, None, None)
                .expect("rights"),
            ScanOutcome::Passed,
            ScanOutcome::Passed,
            vec![humaux_domain::public::ReleaseSource::Evidence(EvidenceId(
                h.evidence_id,
            ))],
        )
        .expect("release shape");
        let other_tenant = Uuid::new_v4();
        assert_eq!(
            h.rt.block_on(contribution_repo::create_release(
                &h.private,
                TenantId(other_tenant),
                &release,
            )),
            Err(humaux_domain::error::ErrorCode::TenantBoundary)
        );
        let releases: i64 = h
            .admin
            .query_one(
                "SELECT count(*) FROM staging.contribution_releases WHERE tenant_id = $1",
                &[&other_tenant],
            )
            .expect("release query")
            .get(0);
        assert_eq!(releases, 0);
    });
}

#[test]
fn memory_sources_reject_cross_tenant_api_and_composite_fk_writes() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    run_db_fixture::<Fixture, _>("contribution_repo", |mut h| {
        let release = release_with_sources(vec![ReleaseSource::Memory(MemoryId(h.memory_id))]);
        let foreign_tenant = Uuid::new_v4();
        assert_eq!(
            h.rt.block_on(contribution_repo::create_release(
                &h.private,
                TenantId(foreign_tenant),
                &release,
            )),
            Err(humaux_domain::error::ErrorCode::TenantBoundary)
        );

        // The private API cannot see a foreign memory.  Admin proves separately that the
        // composite FK also rejects a blind cross-tenant UUID even when the memory is valid.
        let mut direct_tx = h.admin.transaction().expect("admin direct FK transaction");
        direct_tx
            .execute(
                "INSERT INTO control.tenants (tenant_id, name) VALUES ($1, $2)",
                &[
                    &foreign_tenant,
                    &format!("foreign_memory_tenant_{foreign_tenant}"),
                ],
            )
            .expect("foreign tenant fixture");
        let foreign_release: Uuid = direct_tx
            .query_one(
                "INSERT INTO staging.contribution_releases \
                 (tenant_id, policy_snapshot, privacy_scan_outcome, secret_scan_outcome, rights_basis) \
                 VALUES ($1, '{\"policy\":\"MANUAL\",\"consent_version\":\"fixture\"}'::jsonb, 'PASSED', 'PASSED', 'fixture rights') \
                 RETURNING contribution_release_id",
                &[&foreign_tenant],
            )
            .expect("admin foreign release fixture")
            .get(0);
        let error = direct_tx
            .execute(
                "INSERT INTO staging.contribution_release_sources \
                 (tenant_id, contribution_release_id, memory_id, ordinal) VALUES ($1, $2, $3, 0)",
                &[&foreign_tenant, &foreign_release, &h.memory_id],
            )
            .expect_err("composite memory FK rejects tenant mismatch");
        assert_eq!(error.code().map(|code| code.code()), Some("23503"));
        assert_eq!(
            error.as_db_error().and_then(|db| db.constraint()),
            Some("release_sources_tenant_memory_fk")
        );
        direct_tx.rollback().expect("rollback direct FK fixture");
    });
}

#[test]
fn private_worker_without_tenant_context_cannot_read_fixture_release() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    run_db_fixture::<Fixture, _>("contribution_repo", |mut h| {
        let release = ContributionRelease::release(
            ContributionPolicy::Manual,
            "fixture-consent-v1".to_owned(),
            RightsProvenance::new("fixture rights".to_owned(), None, None, None, None)
                .expect("rights"),
            ScanOutcome::Passed,
            ScanOutcome::Passed,
            vec![humaux_domain::public::ReleaseSource::Evidence(EvidenceId(
                h.evidence_id,
            ))],
        )
        .expect("release");
        let release_id =
            h.rt.block_on(contribution_repo::create_release(
                &h.private,
                TenantId(h.tenant_id),
                &release,
            ))
            .expect("create");
        let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("test dsn");
        // dep: PostgreSQL(any) — opens the role-scoped connection for `private_worker_without_tenant_context_cannot_read_fixture_release`
        let mut worker = Client::connect(&dsn_as_role(&dsn, "role_private_worker"), NoTls)
            .expect("private role client");
        let visible: i64 = worker
            .query_one(
                "SELECT count(*) FROM staging.contribution_releases WHERE tenant_id = $1",
                &[&h.tenant_id],
            )
            .expect("no-context query")
            .get(0);
        assert_eq!(visible, 0);
        assert!(h.admin.query_one("SELECT count(*) FROM staging.contribution_releases WHERE contribution_release_id = $1", &[&release_id]).expect("admin release").get::<_, i64>(0) == 1);
    });
}

#[test]
fn public_worker_without_tenant_context_cannot_admit_user_source() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    run_db_fixture::<Fixture, _>("contribution_repo", |mut h| {
        let release_id =
            h.rt.block_on(contribution_repo::create_release(
                &h.private,
                TenantId(h.tenant_id),
                &release_with_sources(vec![ReleaseSource::Evidence(EvidenceId(h.evidence_id))]),
            ))
            .expect("create release");
        let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("test dsn");
        // dep: PostgreSQL(any) — opens the role-scoped connection for `public_worker_without_tenant_context_cannot_admit_user_source`
        let mut worker = Client::connect(&dsn_as_role(&dsn, "role_public_worker"), NoTls)
            .expect("public role client");
        let error = worker
            .execute(
                "INSERT INTO public.sources \
                 (source_type, contribution_release_id, content_hash, rights_basis, trust_class) \
                 VALUES ('USER_CONTRIBUTION', $1, $2, 'fixture rights', 'fixture')",
                &[&release_id, &Uuid::new_v4().to_string()],
            )
            .expect_err("public source admission requires tenant context");
        assert_eq!(error.code().map(|code| code.code()), Some("42501"));
        let admitted: i64 = h
            .admin
            .query_one(
                "SELECT count(*) FROM public.sources WHERE contribution_release_id = $1",
                &[&release_id],
            )
            .expect("source count")
            .get(0);
        assert_eq!(admitted, 0);
    });
}

#[test]
fn user_source_guard_allows_active_then_rejects_revoked_release() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    run_db_fixture::<Fixture, _>("contribution_repo", |mut h| {
        let release = ContributionRelease::release(
            ContributionPolicy::Manual,
            "fixture-consent-v1".to_owned(),
            RightsProvenance::new("fixture rights".to_owned(), None, None, None, None)
                .expect("rights"),
            ScanOutcome::Passed,
            ScanOutcome::Passed,
            vec![humaux_domain::public::ReleaseSource::Evidence(EvidenceId(
                h.evidence_id,
            ))],
        )
        .expect("release");
        let release_id =
            h.rt.block_on(contribution_repo::create_release(
                &h.private,
                TenantId(h.tenant_id),
                &release,
            ))
            .expect("create");
        let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("test dsn");
        // dep: PostgreSQL(any) — opens the role-scoped connection for `user_source_guard_allows_active_then_rejects_revoked_release`
        let mut worker = Client::connect(&dsn_as_role(&dsn, "role_public_worker"), NoTls)
            .expect("public role client");
        let mut tx = worker.transaction().expect("source transaction");
        tx.batch_execute(&format!("SET LOCAL humaux.tenant_id = '{}'", h.tenant_id))
            .expect("tenant context");
        let source_id: Uuid = tx.query_one("INSERT INTO public.sources (source_type, contribution_release_id, content_hash, rights_basis, trust_class) VALUES ('USER_CONTRIBUTION', $1, $2, 'fixture rights', 'fixture') RETURNING source_id", &[&release_id, &Uuid::new_v4().to_string()]).expect("active source").get(0);
        tx.commit().expect("source commit");
        h.source_ids.push(source_id);
        h.rt.block_on(contribution_repo::promote_claim(
            &h.public,
            TenantId(h.tenant_id),
            &serde_json::json!({"user": true}),
            &[source_id],
        ))
        .expect("active promotion");
        assert!(
            h.rt.block_on(contribution_repo::revoke_release(
                &h.private,
                TenantId(h.tenant_id),
                release_id
            ))
            .expect("revoke")
        );
        assert_eq!(
            h.rt.block_on(contribution_repo::promote_claim(
                &h.public,
                TenantId(h.tenant_id),
                &serde_json::json!({"late": true}),
                &[source_id]
            )),
            Err(humaux_domain::error::ErrorCode::InvalidInput)
        );
    });
}

#[test]
fn promotion_and_closure_preserve_minimum_roots() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    run_db_fixture::<Fixture, _>("contribution_repo", |mut h| {
        // A+B -> S1; S1+C -> S2, plus A directly into S2 creates a diamond.  Roots remain
        // distinct and the direct A path has the minimum depth of two.
        h.admin.batch_execute(&format!(
            "INSERT INTO public.synthesis_inputs (synthesis_id, claim_id, ordinal) VALUES \
                ('{}', '{}', 1), ('{}', '{}', 2); \
             INSERT INTO public.synthesis_inputs (synthesis_id, input_synthesis_id, ordinal) VALUES ('{}', '{}', 1); \
             INSERT INTO public.synthesis_inputs (synthesis_id, claim_id, ordinal) VALUES ('{}', '{}', 2), ('{}', '{}', 3);",
            h.synthesis_ids[0], h.claim_ids[0], h.synthesis_ids[0], h.claim_ids[1],
            h.synthesis_ids[1], h.synthesis_ids[0], h.synthesis_ids[1], h.claim_ids[2], h.synthesis_ids[1], h.claim_ids[0],
        )).expect("seed synthesis graph");
        let count =
            h.rt.block_on(contribution_repo::recompute_source_closure(
                &h.public,
                h.synthesis_ids[1],
            ))
            .expect("closure recompute");
        assert_eq!(count, 3);
        assert_eq!(
            h.rt.block_on(contribution_repo::recompute_source_closure(
                &h.public,
                h.synthesis_ids[1],
            ))
            .expect("repeat closure recompute"),
            3
        );
        let closure_rows: i64 = h
            .admin
            .query_one(
                "SELECT count(*) FROM public.source_closure WHERE synthesis_id = $1",
                &[&h.synthesis_ids[1]],
            )
            .expect("no duplicate closure pairs")
            .get(0);
        assert_eq!(closure_rows, 3);
        let min_depth: i32 = h.admin.query_one(
            "SELECT depth FROM public.source_closure WHERE synthesis_id = $1 AND root_source_id = $2 AND is_current",
            &[&h.synthesis_ids[1], &h.source_ids[0]],
        ).expect("closure row").get(0);
        assert_eq!(min_depth, 2);
        let indirect_depth: i32 = h.admin.query_one(
            "SELECT depth FROM public.source_closure WHERE synthesis_id = $1 AND root_source_id = $2 AND is_current",
            &[&h.synthesis_ids[1], &h.source_ids[1]],
        ).expect("indirect closure row").get(0);
        assert_eq!(indirect_depth, 3);
        let promoted =
            h.rt.block_on(contribution_repo::promote_claim(
                &h.public,
                TenantId(h.tenant_id),
                &serde_json::json!({"fixture": true}),
                &h.source_ids,
            ))
            .expect("promotion");
        h.claim_ids.push(promoted);
        assert_eq!(
            h.rt.block_on(contribution_repo::set_moderation_state(
                &h.public,
                promoted,
                ModerationState::UnderReview,
            )),
            Err(humaux_domain::error::ErrorCode::Forbidden)
        );
        let state: String = h
            .admin
            .query_one(
                "SELECT moderation_state FROM public.claims WHERE claim_id = $1",
                &[&promoted],
            )
            .expect("promoted claim")
            .get(0);
        assert_eq!(
            state, "PUBLIC_STAGING",
            "forbidden legacy facade must not mutate state"
        );
    });
}

#[test]
fn invalid_cycle_preserves_previous_closure() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    run_db_fixture::<Fixture, _>("contribution_repo", |mut h| {
        h.admin.batch_execute(&format!(
            "INSERT INTO public.synthesis_inputs (synthesis_id, claim_id, ordinal) VALUES ('{}', '{}', 1);",
            h.synthesis_ids[0], h.claim_ids[0],
        )).expect("seed valid graph");
        h.rt.block_on(contribution_repo::recompute_source_closure(
            &h.public,
            h.synthesis_ids[0],
        ))
        .expect("initial closure");
        h.admin.batch_execute(&format!(
            "INSERT INTO public.synthesis_inputs (synthesis_id, input_synthesis_id, ordinal) VALUES ('{}', '{}', 2); \
             INSERT INTO public.synthesis_inputs (synthesis_id, input_synthesis_id, ordinal) VALUES ('{}', '{}', 1);",
            h.synthesis_ids[0], h.synthesis_ids[1], h.synthesis_ids[1], h.synthesis_ids[0],
        )).expect("seed cycle");
        assert_eq!(
            h.rt.block_on(contribution_repo::recompute_source_closure(
                &h.public,
                h.synthesis_ids[0]
            )),
            Err(humaux_domain::error::ErrorCode::InvalidInput)
        );
        let current: i64 = h
            .admin
            .query_one(
                "SELECT count(*) FROM public.source_closure WHERE synthesis_id = $1 AND is_current",
                &[&h.synthesis_ids[0]],
            )
            .expect("old closure survives")
            .get(0);
        assert_eq!(current, 1);
    });
}

#[test]
fn outbox_fault_after_release_writes_rolls_back_every_contribution_row() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    run_db_fixture::<Fixture, _>("contribution_repo", |mut h| {
        let suffix = Uuid::new_v4().simple().to_string();
        let function_name = format!("ops.card_c_outbox_fault_{suffix}");
        let trigger_name = format!("card_c_outbox_fault_{suffix}");
        h.admin
            .batch_execute(&format!(
                "CREATE FUNCTION {function_name}() RETURNS trigger LANGUAGE plpgsql AS $$ \
                 BEGIN IF NEW.tenant_id = '{}'::uuid THEN RAISE EXCEPTION 'card c injected outbox failure' USING ERRCODE = '23514'; END IF; RETURN NEW; END; $$; \
                 CREATE TRIGGER {trigger_name} BEFORE INSERT ON ops.outbox FOR EACH ROW EXECUTE FUNCTION {function_name}();",
                h.tenant_id
            ))
            .expect("install tenant-scoped outbox fault");
        let result = h.rt.block_on(contribution_repo::create_release(
            &h.private,
            TenantId(h.tenant_id),
            &release_with_sources(vec![ReleaseSource::Evidence(EvidenceId(h.evidence_id))]),
        ));
        h.admin
            .batch_execute(&format!(
                "DROP TRIGGER {trigger_name} ON ops.outbox; DROP FUNCTION {function_name}();"
            ))
            .expect("remove tenant-scoped outbox fault");
        assert_eq!(result, Err(humaux_domain::error::ErrorCode::InvalidInput));
        for (table, filter) in [
            ("staging.contribution_releases", "tenant_id"),
            ("staging.contribution_release_sources", "tenant_id"),
            ("ops.outbox", "tenant_id"),
        ] {
            let sql = format!("SELECT count(*) FROM {table} WHERE {filter} = $1");
            let count: i64 = h
                .admin
                .query_one(&sql, &[&h.tenant_id])
                .expect("rollback count")
                .get(0);
            assert_eq!(count, 0, "{table} must not survive injected outbox fault");
        }
    });
}

#[test]
fn concurrent_revokes_use_two_runtime_sessions_and_emit_one_revoke() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    run_db_fixture::<Fixture, _>("contribution_repo", |mut h| {
        let release_id =
            h.rt.block_on(contribution_repo::create_release(
                &h.private,
                TenantId(h.tenant_id),
                &release_with_sources(vec![ReleaseSource::Evidence(EvidenceId(h.evidence_id))]),
            ))
            .expect("create release");
        let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("test dsn");
        let private_dsn = dsn_as_role(&dsn, "role_private_worker");
        let tenant_id = TenantId(h.tenant_id);
        let (left, right) = h.rt.block_on(async {
            // dep: PostgreSQL(role_private_worker) — opens the role-scoped connection for `concurrent_revokes_use_two_runtime_sessions_and_emit_one_revoke`
            let left = PrivateWorkerDbPool::connect(&private_dsn)
                .await
                .expect("left role pool");
            // dep: PostgreSQL(role_private_worker) — opens the role-scoped connection for `concurrent_revokes_use_two_runtime_sessions_and_emit_one_revoke`
            let right = PrivateWorkerDbPool::connect(&private_dsn)
                .await
                .expect("right role pool");
            tokio::join!(
                contribution_repo::revoke_release(&left, tenant_id, release_id),
                contribution_repo::revoke_release(&right, tenant_id, release_id)
            )
        });
        let outcomes = [left.expect("left revoke"), right.expect("right revoke")];
        assert_eq!(outcomes.into_iter().filter(|changed| *changed).count(), 1);
        let revoked: i64 = h.admin.query_one(
            "SELECT count(*) FROM ops.outbox WHERE contribution_release_id = $1 AND event_type = 'PUBLIC_REVOKE'",
            &[&release_id],
        ).expect("revoke outbox count").get(0);
        assert_eq!(revoked, 1);
    });
}

#[test]
fn promotion_completes_before_queued_revoke_under_release_lock() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    run_db_fixture::<Fixture, _>("contribution_repo", |mut h| {
        let release_id =
            h.rt.block_on(contribution_repo::create_release(
                &h.private,
                TenantId(h.tenant_id),
                &release_with_sources(vec![ReleaseSource::Evidence(EvidenceId(h.evidence_id))]),
            ))
            .expect("create release");
        let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("test dsn");
        let source_id = admit_contribution_source(&dsn, h.tenant_id, release_id);
        h.source_ids.push(source_id);

        // dep: PostgreSQL(any) — opens the role-scoped connection for `promotion_completes_before_queued_revoke_under_release_lock`
        let mut lock_client = Client::connect(&dsn_as_role(&dsn, "role_public_worker"), NoTls)
            .expect("public lock client");
        let mut source_lock_tx = lock_client.transaction().expect("source lock transaction");
        source_lock_tx
            .batch_execute(&format!("SET LOCAL humaux.tenant_id = '{}'", h.tenant_id))
            .expect("lock tenant context");
        source_lock_tx
            .query_one(
                "SELECT source_id FROM public.sources WHERE source_id = $1 FOR UPDATE",
                &[&source_id],
            )
            .expect("hold public source row lock");

        let suffix = Uuid::new_v4().simple().to_string();
        let promotion_application = format!("card_c_promotion_{suffix}");
        let public_dsn =
            dsn_as_role_with_application(&dsn, "role_public_worker", &promotion_application);
        let promotion_tenant = TenantId(h.tenant_id);
        let promotion = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("promotion runtime");
            // dep: PostgreSQL(role_public_worker) — opens the role-scoped connection for `promotion_completes_before_queued_revoke_under_release_lock`
            let pool = rt
                .block_on(PublicWorkerDbPool::connect(&public_dsn))
                .expect("promotion pool");
            rt.block_on(contribution_repo::promote_claim(
                &pool,
                promotion_tenant,
                &serde_json::json!({"ordered": true}),
                &[source_id],
            ))
        });
        // The API has now acquired its real shared release lock and is blocked only on the
        // source row.  A later exclusive revoke must queue behind that shared holder.
        let promotion_barrier = wait_for_granted_advisory_lock(
            &mut h.admin,
            &promotion_application,
            Duration::from_secs(3),
        )
        .and_then(|_| {
            wait_for_transactionid_waiter(
                &mut h.admin,
                &promotion_application,
                Duration::from_secs(3),
            )
        });
        let revoke_application = format!("card_c_revoke_{suffix}");
        let revoke = promotion_barrier.as_ref().ok().map(|_| {
            let private_dsn =
                dsn_as_role_with_application(&dsn, "role_private_worker", &revoke_application);
            let tenant_id = TenantId(h.tenant_id);
            thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().expect("revoke runtime");
                // dep: PostgreSQL(role_private_worker) — opens the role-scoped connection for `promotion_completes_before_queued_revoke_under_release_lock`
                let pool = rt
                    .block_on(PrivateWorkerDbPool::connect(&private_dsn))
                    .expect("revoke pool");
                rt.block_on(contribution_repo::revoke_release(
                    &pool, tenant_id, release_id,
                ))
            })
        });
        let revoke_barrier = if revoke.is_some() {
            wait_for_advisory_waiter(&mut h.admin, &revoke_application, Duration::from_secs(3))
        } else {
            Ok(())
        };
        let unlock_result = source_lock_tx.commit();
        let promotion_join = promotion.join();
        let revoke_join = revoke.map(thread::JoinHandle::join);
        unlock_result.expect("release source row lock");
        promotion_barrier.expect("promotion lock barrier");
        revoke_barrier.expect("revoke lock barrier");
        let promotion_result = promotion_join.expect("promotion thread");
        let revoke_result = revoke_join.map(|result| result.expect("revoke thread"));
        let promoted = promotion_result.expect("promotion owns shared lock before revoke");
        h.claim_ids.push(promoted);
        assert!(
            revoke_result
                .expect("revoke thread started")
                .expect("revoke result")
        );
        let state: String = h.admin.query_one(
            "SELECT state FROM staging.contribution_releases WHERE contribution_release_id = $1",
            &[&release_id],
        ).expect("release state").get(0);
        assert_eq!(state, "REVOKED");
    });
}

#[test]
fn closure_marks_removed_root_stale_and_repeat_refresh_keeps_one_pair() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    run_db_fixture::<Fixture, _>("contribution_repo", |mut h| {
        let synthesis_id = h.synthesis_ids[0];
        h.admin.execute(
            "INSERT INTO public.synthesis_inputs (synthesis_id, claim_id, ordinal) VALUES ($1, $2, 1)",
            &[&synthesis_id, &h.claim_ids[0]],
        ).expect("seed first root");
        assert_eq!(
            h.rt.block_on(contribution_repo::recompute_source_closure(
                &h.public,
                synthesis_id
            )),
            Ok(1)
        );
        h.admin
            .execute(
                "DELETE FROM public.synthesis_inputs WHERE synthesis_id = $1",
                &[&synthesis_id],
            )
            .expect("replace graph input");
        h.admin.execute(
            "INSERT INTO public.synthesis_inputs (synthesis_id, claim_id, ordinal) VALUES ($1, $2, 1)",
            &[&synthesis_id, &h.claim_ids[1]],
        ).expect("seed replacement root");
        assert_eq!(
            h.rt.block_on(contribution_repo::recompute_source_closure(
                &h.public,
                synthesis_id
            )),
            Ok(1)
        );
        let removed_root_is_current: bool = h.admin.query_one(
            "SELECT is_current FROM public.source_closure WHERE synthesis_id = $1 AND root_source_id = $2",
            &[&synthesis_id, &h.source_ids[0]],
        ).expect("stale root row").get(0);
        assert!(!removed_root_is_current);
        assert_eq!(
            h.rt.block_on(contribution_repo::recompute_source_closure(
                &h.public,
                synthesis_id
            )),
            Ok(1)
        );
        let replacement_rows: i64 = h.admin.query_one(
            "SELECT count(*) FROM public.source_closure WHERE synthesis_id = $1 AND root_source_id = $2",
            &[&synthesis_id, &h.source_ids[1]],
        ).expect("replacement pair count").get(0);
        assert_eq!(replacement_rows, 1);
    });
}

#[test]
fn reachable_unrooted_branch_keeps_previous_closure_current() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    run_db_fixture::<Fixture, _>("contribution_repo", |mut h| {
        let target = h.synthesis_ids[0];
        let unrooted = h.synthesis_ids[1];
        h.admin.execute(
            "INSERT INTO public.synthesis_inputs (synthesis_id, claim_id, ordinal) VALUES ($1, $2, 1)",
            &[&target, &h.claim_ids[0]],
        ).expect("seed valid root");
        h.rt.block_on(contribution_repo::recompute_source_closure(
            &h.public, target,
        ))
        .expect("initial closure");
        h.admin.execute(
            "INSERT INTO public.synthesis_inputs (synthesis_id, input_synthesis_id, ordinal) VALUES ($1, $2, 2)",
            &[&target, &unrooted],
        ).expect("attach reachable unrooted branch");
        assert_eq!(
            h.rt.block_on(contribution_repo::recompute_source_closure(
                &h.public, target
            )),
            Err(humaux_domain::error::ErrorCode::InvalidInput)
        );
        let current: i64 = h.admin.query_one(
            "SELECT count(*) FROM public.source_closure WHERE synthesis_id = $1 AND root_source_id = $2 AND is_current",
            &[&target, &h.source_ids[0]],
        ).expect("old closure stays current").get(0);
        assert_eq!(current, 1);
    });
}

#[test]
fn refresh_waits_for_graph_update_then_has_stable_distinct_pairs() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    run_db_fixture::<Fixture, _>("contribution_repo", |mut h| {
        let target = h.synthesis_ids[0];
        h.admin.execute(
            "INSERT INTO public.synthesis_inputs (synthesis_id, claim_id, ordinal) VALUES ($1, $2, 1)",
            &[&target, &h.claim_ids[0]],
        ).expect("seed initial graph");
        let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("test dsn");
        let suffix = Uuid::new_v4().simple().to_string();
        let writer_application = format!("card_c_graph_writer_{suffix}");
        let refresher_application = format!("card_c_graph_refresh_{suffix}");
        let (locked_tx, locked_rx) = mpsc::channel();
        let (commit_tx, commit_rx) = mpsc::channel();
        let writer_dsn =
            dsn_as_role_with_application(&dsn, "role_public_worker", &writer_application);
        let second_claim = h.claim_ids[1];
        let writer = thread::spawn(move || {
            // dep: PostgreSQL(any) — opens the role-scoped connection for `refresh_waits_for_graph_update_then_has_stable_distinct_pairs`
            let mut writer = Client::connect(&writer_dsn, NoTls).expect("public graph writer");
            let mut tx = writer.transaction().expect("graph writer transaction");
            tx.batch_execute("LOCK TABLE public.synthesis_inputs IN ROW EXCLUSIVE MODE")
                .expect("hold graph update lock");
            locked_tx.send(()).expect("graph lock acquired");
            commit_rx
                .recv_timeout(Duration::from_secs(3))
                .expect("graph writer release");
            tx.execute(
                "INSERT INTO public.synthesis_inputs (synthesis_id, claim_id, ordinal) VALUES ($1, $2, 2)",
                &[&target, &second_claim],
            ).expect("write graph update");
            tx.commit().expect("commit graph update");
        });
        if let Err(error) = locked_rx.recv_timeout(Duration::from_secs(3)) {
            drop(commit_tx);
            let _ = writer.join();
            panic!("graph writer lock barrier: {error}");
        }
        let (result_tx, result_rx) = mpsc::channel();
        let public_dsn =
            dsn_as_role_with_application(&dsn, "role_public_worker", &refresher_application);
        let refresher = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("refresh runtime");
            // dep: PostgreSQL(role_public_worker) — opens the role-scoped connection for `refresh_waits_for_graph_update_then_has_stable_distinct_pairs`
            let pool = rt
                .block_on(PublicWorkerDbPool::connect(&public_dsn))
                .expect("refresh pool");
            result_tx
                .send(rt.block_on(contribution_repo::recompute_source_closure(&pool, target)))
                .expect("refresh result");
        });
        let refresh_barrier = wait_for_synthesis_relation_waiter(
            &mut h.admin,
            &refresher_application,
            Duration::from_secs(3),
        );
        let release_signal = commit_tx.send(());
        let writer_join = writer.join();
        let refresher_join = refresher.join();
        release_signal.expect("allow graph update");
        let refresh_result = result_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("refresh completes");
        writer_join.expect("graph writer thread");
        refresher_join.expect("refresh thread");
        refresh_barrier.expect("refresh lock barrier");
        assert_eq!(refresh_result, Ok(2));
        assert_eq!(
            h.rt.block_on(contribution_repo::recompute_source_closure(
                &h.public, target
            )),
            Ok(2)
        );
        let pairs: i64 = h
            .admin
            .query_one(
                "SELECT count(*) FROM public.source_closure WHERE synthesis_id = $1",
                &[&target],
            )
            .expect("distinct pair count")
            .get(0);
        assert_eq!(pairs, 2);
    });
}

#[test]
fn concurrent_refreshes_serialize_and_preserve_distinct_root_depth_pairs() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    run_db_fixture::<Fixture, _>("contribution_repo", |mut h| {
        let target = h.synthesis_ids[0];
        h.admin
            .batch_execute(&format!(
                "INSERT INTO public.synthesis_inputs (synthesis_id, claim_id, ordinal) VALUES \
                 ('{}', '{}', 1), ('{}', '{}', 2);",
                target, h.claim_ids[0], target, h.claim_ids[1],
            ))
            .expect("seed two-root synthesis");
        let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("test dsn");
        let suffix = Uuid::new_v4().simple().to_string();
        let first_application = format!("card_c_refresh_one_{suffix}");
        let second_application = format!("card_c_refresh_two_{suffix}");
        // dep: PostgreSQL(any) — opens the role-scoped connection for `concurrent_refreshes_serialize_and_preserve_distinct_root_depth_pairs`
        let mut holder = Client::connect(
            &dsn_as_role_with_application(
                &dsn,
                "role_public_worker",
                &format!("card_c_refresh_holder_{suffix}"),
            ),
            NoTls,
        )
        .expect("public closure-lock holder");
        let mut holder_tx = holder.transaction().expect("closure-lock transaction");
        holder_tx
            .batch_execute("LOCK TABLE public.source_closure IN SHARE ROW EXCLUSIVE MODE")
            .expect("prehold closure serialization lock");

        let first_dsn =
            dsn_as_role_with_application(&dsn, "role_public_worker", &first_application);
        let first = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("first refresh runtime");
            // dep: PostgreSQL(role_public_worker) — opens the role-scoped connection for `concurrent_refreshes_serialize_and_preserve_distinct_root_depth_pairs`
            let pool = rt
                .block_on(PublicWorkerDbPool::connect(&first_dsn))
                .expect("first refresh pool");
            rt.block_on(contribution_repo::recompute_source_closure(&pool, target))
        });
        let second_dsn =
            dsn_as_role_with_application(&dsn, "role_public_worker", &second_application);
        let second = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("second refresh runtime");
            // dep: PostgreSQL(role_public_worker) — opens the role-scoped connection for `concurrent_refreshes_serialize_and_preserve_distinct_root_depth_pairs`
            let pool = rt
                .block_on(PublicWorkerDbPool::connect(&second_dsn))
                .expect("second refresh pool");
            rt.block_on(contribution_repo::recompute_source_closure(&pool, target))
        });

        let first_wait = wait_for_source_closure_relation_waiter(
            &mut h.admin,
            &first_application,
            Duration::from_secs(3),
        );
        let second_wait = wait_for_source_closure_relation_waiter(
            &mut h.admin,
            &second_application,
            Duration::from_secs(3),
        );
        let unlock_result = holder_tx.commit();
        let first_join = first.join();
        let second_join = second.join();
        unlock_result.expect("release closure serialization lock");
        first_wait.expect("first refresh lock barrier");
        second_wait.expect("second refresh lock barrier");
        let first_result = first_join.expect("first refresh thread");
        let second_result = second_join.expect("second refresh thread");
        assert_eq!(first_result, Ok(2));
        assert_eq!(second_result, Ok(2));
        let pairs = h
            .admin
            .query(
                "SELECT root_source_id, depth FROM public.source_closure \
                 WHERE synthesis_id = $1 AND is_current ORDER BY root_source_id",
                &[&target],
            )
            .expect("current closure pairs")
            .into_iter()
            .map(|row| (row.get::<_, Uuid>(0), row.get::<_, i32>(1)))
            .collect::<Vec<_>>();
        let mut expected = vec![(h.source_ids[0], 2), (h.source_ids[1], 2)];
        expected.sort_unstable();
        assert_eq!(pairs, expected);
        let stored_pairs: i64 = h
            .admin
            .query_one(
                "SELECT count(*) FROM public.source_closure WHERE synthesis_id = $1",
                &[&target],
            )
            .expect("no accumulated closure pairs")
            .get(0);
        assert_eq!(stored_pairs, 2);
    });
}

#[test]
fn direct_sql_rejects_invalid_outbox_class_and_identity_rewrite() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    run_db_fixture::<Fixture, _>("contribution_repo", |mut h| {
        let commit_seq: i64 = h
            .admin
            .query_one("SELECT nextval('ops.commit_seq_seq')", &[])
            .expect("reserve commit sequence")
            .get(0);
        let row_class_error = h.admin.execute(
            "INSERT INTO ops.outbox (tenant_id, commit_seq, event_type) VALUES ($1, $2, 'PUBLIC_RELEASE')",
            &[&h.tenant_id, &commit_seq],
        ).expect_err("release event cannot omit release identity");
        assert_eq!(
            row_class_error.code().map(|code| code.code()),
            Some("23514")
        );
        let release_id =
            h.rt.block_on(contribution_repo::create_release(
                &h.private,
                TenantId(h.tenant_id),
                &release_with_sources(vec![ReleaseSource::Evidence(EvidenceId(h.evidence_id))]),
            ))
            .expect("create release");
        let identity_error = h
            .admin
            .execute(
                "UPDATE ops.outbox SET event_type = 'PUBLIC_REVOKE' \
             WHERE contribution_release_id = $1 AND event_type = 'PUBLIC_RELEASE'",
                &[&release_id],
            )
            .expect_err("outbox identity is immutable");
        assert_eq!(identity_error.code().map(|code| code.code()), Some("23514"));
    });
}
