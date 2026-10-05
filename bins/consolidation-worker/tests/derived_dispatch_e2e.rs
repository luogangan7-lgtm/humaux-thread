//! `consolidation-worker::tests::derived_dispatch_e2e` — ADR-0036 (card 14) acceptance: cross-tenant pending-work
//!   discovery for the consolidation worker.
//! Depends-on: crates=[async-trait, humaux-adapters, humaux-application, humaux-testkit, postgres, tokio, uuid];
//!   services=[PostgreSQL(role_consolidation_worker) r=[ops.claim_derived_work, private.memory_rollups] w=[control.credentials, control.memberships, control.private_reasoning_domains,
//!   control.processor_models, control.provider_accounts, control.provider_endpoints,
//!   control.reasoning_credential_bindings, control.reasoning_profiles, control.reasoning_route_bindings,
//!   control.reasoning_route_candidates, control.reasoning_route_policies, control.tenants, control.users, ops.jobs,
//!   private.events, private.evidence_objects, private.memory_evidence,
//!   private.memory_records] x=[ops.claim_derived_work], UDS(serve),
//!   subprocess(humaux-consolidation-worker), subprocess(kill)]; env=[CARGO_BIN_EXE_humaux-consolidation-worker,
//!   CONSOLIDATION_WORKER_PG_DSN, HUMAUX_CONSOLIDATION_WORKER_BATCH, HUMAUX_CONSOLIDATION_WORKER_CALL_TTL_SECS,
//!   HUMAUX_CONSOLIDATION_WORKER_DIAL_TIMEOUT_SECS, HUMAUX_CONSOLIDATION_WORKER_LEASE_SECS,
//!   HUMAUX_CONSOLIDATION_WORKER_MAX_ATTEMPTS, HUMAUX_CONSOLIDATION_WORKER_MAX_INPUTS,
//!   HUMAUX_CONSOLIDATION_WORKER_POLL_INTERVAL_SECS, HUMAUX_CONSOLIDATION_WORKER_RPC_SOCKET_PATH,
//!   HUMAUX_CONSOLIDATION_WORKER_SERVE_METRICS_ADDR, HUMAUX_TEST_PG_DSN]; modules=[adapters::consolidate_repo, adapters::jobs, adapters::postgres,
//!   application::consolidate, humaux-consolidation-worker, humaux-testkit,
//!   testkit::fixture_purge, testkit::reaped]
//! Called-by: [cargo-test]
//! Invariants: [one tenant-less pass completes both tenants' work, claims each job exactly once under RLS, and a
//!   killed worker's expired lease is re-claimed with the result written once; an isolation-setup failure is a
//!   fixture error, not a pass]
//! Spec: ADR-0036; §79.2
//!
//! Every test here drives [`dispatch_pass`] with NO tenant id, reasoning domain, or route
//! binding of its own — exactly what the binary now has in its environment (none of the three) —
//! and proves the five properties the card's acceptance gate names:
//!
//! 1. two provisioned tenants' work is discovered and completed by ONE worker pass;
//! 2. no cross-tenant leakage in what a claimed job can read, asserted UNDER RLS (a direct SELECT
//!    for tenant B's rows with tenant A's context installed returns zero rows) rather than by
//!    application filtering;
//! 3. two workers racing on the same pending work claim it exactly once — asserted on row counts
//!    (`private.memory_rollups`), not on logs;
//! 4. a worker killed mid-run leaves a lease that expires, after which another process completes
//!    the job and the result is written exactly once;
//! 5. a pass with no input claims nothing and returns promptly (what `--run-once` exits on;
//!    before this card `--run-once` looped forever on an empty tenant).
//!
//! The fault injection for (4) is recorded in ADR-0036: `CREATE OR REPLACE` the 0164 claim
//! function without its `status = 'PROCESSING' AND lease_expires_at < clock_timestamp()` arm and
//! `expired_lease_is_reclaimed_and_published_exactly_once` goes red while nothing else moves.
//!
//! Three-state skip (§79.2): no DSN, unreachable DB, or the 0164 objects missing all print a
//! visible SKIP and return — same fixture shape as `tests/run_once_e2e.rs`.

use humaux_adapters::consolidate_repo::PublishOutcome;
use humaux_adapters::jobs::{self, DerivedJobType};
use humaux_adapters::postgres::ConsolidationDbPool;
use humaux_application::consolidate::{
    ContentSha256, PrivateReasoningError, PrivateReasoningPort, PrivateReasoningResult,
    ProviderTraceRef, ReasoningRouteBindingId, ReasoningRouteBindingVersion,
    SealedPrivateReasoningRequest,
};
use humaux_consolidation_worker::{DispatchConfig, build_rollup, dispatch_pass, run_once_bound};
use humaux_testkit::fixture_purge::purge_tenant_fixture_sql;
use humaux_testkit::reaped::SpawnReaped;
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::{Client, NoTls};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

/// Serializes this file's tests. Each seeds its own throwaway tenants, but the claim these tests
/// exercise is deliberately CROSS-tenant: two of this file's tests running at once would see each
/// other's `DERIVED_CONSOLIDATE` jobs, which is the one thing a per-tenant fixture cannot isolate.
static SERIAL_GUARD: Mutex<()> = Mutex::new(());

// Route-graph fixture values, mirrored from `tests/consolidation_hop_e2e.rs` so the two files
// share one catalog row in the global append-only `control.processor_models`.
const PROVIDER_ID: &str = "minimax";
const MODEL_ID: &str = "MiniMax-M3";
const ENDPOINT_REF: &str = "https://api.minimaxi.com/v1/chat/completions";
const REGION: &str = "cn-shanghai";
const SERVICE_TIER: &str = "standard";
const EGRESS_PROCESSOR_ID: Uuid = Uuid::from_u128(0x2001);

/// Same rewrite `tests/consolidation_hop_e2e.rs` uses: the typed pools authenticate AS the role
/// (§6.2.3 assertion E checks `current_user`), so the DSN's credentials are swapped, never a
/// `SET ROLE` on the admin connection.
fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    // ADR-0059 D-D: a real login as `role`, its password from HUMAUX_ROLE_PASSWORD_<SUFFIX>.
    humaux_testkit::role_login_dsn(admin_dsn, role, |name| std::env::var(name).ok())
        .unwrap_or_else(|missing| panic!("missing object: {missing} (ADR-0059 D-D)"))
}

/// Returns the §11.8 rollup contract shape (`content` / `class` / `sources`), with `sources` as
/// the 1-based envelope index so the reply never has to name a uuid the fixture would have to
/// thread through — `parse_rollup_output` maps the index back to this run's own materialized
/// `(memory_id, evidence_id)` pair, which is the §11.6 guard this fake must not bypass.
struct FakePort {
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl PrivateReasoningPort for FakePort {
    async fn infer(
        &self,
        _req: SealedPrivateReasoningRequest,
    ) -> Result<PrivateReasoningResult, PrivateReasoningError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(PrivateReasoningResult {
            output_bytes: br#"{"content":"one rollup","class":"PrivateKnowledge","sources":[1]}"#
                .to_vec(),
            output_sha256: ContentSha256([7u8; 32]),
            provider_trace: ProviderTraceRef("derived-dispatch-e2e".into()),
            model_call_id: Uuid::from_u128(0x3001),
            binding_id: ReasoningRouteBindingId(Uuid::from_u128(0x3002)),
            binding_version: ReasoningRouteBindingVersion(1),
        })
    }
}

/// A borrowed [`FakePort`] as an owned port — `dispatch_pass` builds a NEW port per claimed job
/// (each one is sealed to that job's tenant and run), so the factory must hand back a value.
struct PortRef<'a>(&'a FakePort);

#[async_trait::async_trait]
impl PrivateReasoningPort for PortRef<'_> {
    async fn infer(
        &self,
        req: SealedPrivateReasoningRequest,
    ) -> Result<PrivateReasoningResult, PrivateReasoningError> {
        self.0.infer(req).await
    }
}

/// One provisioned tenant: an admitted `PRIVATE_CONSOLIDATE` route binding (the worker resolves
/// it per claimed tenant — there is no binding id in the process environment any more) and one
/// Evidence its memories hang off.
struct SeededTenant {
    tenant_id: Uuid,
    reasoning_domain_id: Uuid,
    evidence_id: Uuid,
}

struct Handle {
    rt: tokio::runtime::Runtime,
    consolidation: ConsolidationDbPool,
    admin: Client,
    dsn: String,
    tenants: Vec<SeededTenant>,
    user_id: Uuid,
}

impl Drop for Handle {
    /// Throwaway-tenant cleanup through the one fixture purge (`humaux_testkit::fixture_purge`, ADR-0063 "Dev
    /// integrity finding"): every `tenant_id` row, everything that references one through any FK, the identity
    /// rows, then the tenant — the route graph's 0128 append-only rows and the jobs 0164's triggers emitted
    /// included. A failure is printed (the fixture tenant stays, nothing is half-deleted). The shared user is not a
    /// tenant row: deleted last with constraints enforced, best effort.
    fn drop(&mut self) {
        for tenant in &self.tenants {
            let tenant = tenant.tenant_id;
            let purged = purge_tenant_fixture_sql(&tenant.to_string())
                .and_then(|sql| self.admin.batch_execute(&sql).map_err(|e| db_detail(&e)));
            if let Err(error) = purged {
                eprintln!("derived_dispatch_e2e teardown ({tenant}): {error}");
            }
        }
        let _ = self.admin.execute(
            "DELETE FROM control.users WHERE user_id = $1",
            &[&self.user_id],
        );
    }
}

struct DispatchFixture;

impl DbIntegrationFixture for DispatchFixture {
    type Handle = Handle;

    fn isolate() -> Result<Self::Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        // dep: PostgreSQL(role_consolidation_worker) — role-scoped pool call
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;

        let migrated: bool = admin
            .query_one(
                "SELECT to_regprocedure('ops.claim_derived_work(text[],text,double precision,bigint)') \
                 IS NOT NULL",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !migrated {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "ops.claim_derived_work does not exist — run `cargo xtask migrate` against \
                 HUMAUX_TEST_PG_DSN first"
                    .to_string(),
            ));
        }

        // This file's claim is deliberately CROSS-tenant, so it starts from a quiet queue: park
        // any `DERIVED_CONSOLIDATE` row another test's throwaway tenant left claimable. On a
        // fresh database this updates nothing.
        admin
            .execute(
                "UPDATE ops.jobs SET status = 'DEAD', lease_owner = NULL, lease_expires_at = NULL \
                 WHERE job_type = 'DERIVED_CONSOLIDATE' AND status <> 'DEAD'",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(db_detail(&e)))?;

        let user_id: Uuid = admin
            .query_one(
                "INSERT INTO control.users (state) VALUES ('ACTIVE') RETURNING user_id",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);

        // A and B are provisioned; C deliberately has NO admitted PRIVATE_CONSOLIDATE binding —
        // the "tenant onboarded before its route was admitted" case, which must be released with
        // a backoff and never parked DEAD.
        let mut tenants = Vec::new();
        for (label, with_binding) in [
            ("e2e-fixture derived_dispatch_e2e tenant A", true),
            ("e2e-fixture derived_dispatch_e2e tenant B", true),
            (
                "e2e-fixture derived_dispatch_e2e tenant C (no admitted route)",
                false,
            ),
        ] {
            tenants.push(
                seed_tenant(&mut admin, label, user_id, with_binding)
                    .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(db_detail(&e)))?,
            );
        }

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let consolidation = rt
            // dep: PostgreSQL(role_consolidation_worker) — role-scoped pool call
            .block_on(ConsolidationDbPool::connect(&dsn_as_role(
                &dsn,
                "role_consolidation_worker",
            )))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;

        Ok(Handle {
            rt,
            consolidation,
            admin,
            dsn,
            tenants,
            user_id,
        })
    }
}

/// `postgres::Error`'s own `Display` is the bare string "db error" — useless in a SKIP line,
/// which §79.2 requires to name what was missing.
fn db_detail(error: &postgres::Error) -> String {
    match error.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{error}"),
    }
}

/// The R3 route graph tail `control.reasoning_route_candidates` needs, mirrored from
/// `tests/consolidation_hop_e2e.rs::setup_db` — none of these values reach a network: this file's
/// port is a fake, and the only thing under test is that the WORKER resolves the binding per
/// claimed tenant instead of reading one out of its environment.
fn seed_reasoning_profile(
    admin: &mut Client,
    tenant_id: Uuid,
    user_id: Uuid,
    label: &str,
) -> Result<Uuid, postgres::Error> {
    let credential: Uuid = admin
        .query_one(
            "INSERT INTO control.credentials (tenant_id, purpose, openbao_ref) \
             VALUES ($1, 'USER_REASONING', $2) RETURNING credential_id",
            &[&tenant_id, &format!("openbao://derived-dispatch/{label}")],
        )?
        .get(0);
    // `control.processor_models` is global and append-only (0128 trigger): one catalog row per
    // (processor, model, revision) across every run of every test in this workspace.
    admin.execute(
        "INSERT INTO control.processor_models \
           (processor_id, provider_model_id, model_revision, capabilities, status, \
            catalog_observed_at) \
         VALUES ($1, $2, NULL, ARRAY['TEXT','STRUCTURED_OUTPUT'], 'ACTIVE', clock_timestamp()) \
         ON CONFLICT DO NOTHING",
        &[&PROVIDER_ID, &MODEL_ID],
    )?;
    let processor_model_id: Uuid = admin
        .query_one(
            "SELECT processor_model_id FROM control.processor_models \
             WHERE processor_id = $1 AND provider_model_id = $2 AND model_revision IS NULL \
               AND status = 'ACTIVE'",
            &[&PROVIDER_ID, &MODEL_ID],
        )?
        .get(0);
    let account: Uuid = admin
        .query_one(
            "INSERT INTO control.provider_accounts \
               (tenant_id, owner_user_id, processor_id, external_account_ref_hash) \
             VALUES ($1, $2, $3, sha256(convert_to(gen_random_uuid()::text, 'UTF8'))) \
             RETURNING provider_account_id",
            &[&tenant_id, &user_id, &PROVIDER_ID],
        )?
        .get(0);
    admin.execute(
        "INSERT INTO control.reasoning_credential_bindings \
           (credential_ref, tenant_id, owner_user_id, provider_account_id, processor_id) \
         VALUES ($1, $2, $3, $4, $5)",
        &[&credential, &tenant_id, &user_id, &account, &PROVIDER_ID],
    )?;
    let endpoint: Uuid = admin
        .query_one(
            "INSERT INTO control.provider_endpoints \
               (tenant_id, provider_account_id, region, service_tier, endpoint_ref, \
                egress_processor_id) \
             VALUES ($1, $2, $3, $4, $5, $6) RETURNING endpoint_id",
            &[
                &tenant_id,
                &account,
                &REGION,
                &SERVICE_TIER,
                &ENDPOINT_REF,
                &EGRESS_PROCESSOR_ID,
            ],
        )?
        .get(0);
    Ok(admin
        .query_one(
            "INSERT INTO control.reasoning_profiles \
               (tenant_id, owner_user_id, provider_account_id, endpoint_id, processor_model_id, \
                credential_ref, billing_account_id, default_billing_instrument_id, capabilities, \
                processing_region) \
             VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL, ARRAY['TEXT'], $7) RETURNING profile_id",
            &[
                &tenant_id,
                &user_id,
                &account,
                &endpoint,
                &processor_model_id,
                &credential,
                &REGION,
            ],
        )?
        .get(0))
}

fn seed_tenant(
    admin: &mut Client,
    label: &str,
    user_id: Uuid,
    with_binding: bool,
) -> Result<SeededTenant, postgres::Error> {
    let tenant_id: Uuid = admin
        .query_one(
            "INSERT INTO control.tenants (name) VALUES ($1) RETURNING tenant_id",
            &[&label],
        )?
        .get(0);
    // `control.reasoning_route_policies_check_owner` refuses a policy whose owner has no ACTIVE
    // membership in the policy's own tenant (§6.3); the binding trigger additionally requires the
    // domain to name that same owner.
    admin.execute(
        "INSERT INTO control.memberships (tenant_id, user_id, role, state) \
         VALUES ($1, $2, 'OWNER', 'ACTIVE')",
        &[&tenant_id, &user_id],
    )?;
    let reasoning_domain_id: Uuid = admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains \
               (tenant_id, name, owner_user_id, status) \
             VALUES ($1, $2, $3, 'ACTIVE') RETURNING reasoning_domain_id",
            &[&tenant_id, &label, &user_id],
        )?
        .get(0);
    if with_binding {
        seed_route_binding(admin, tenant_id, user_id, reasoning_domain_id, label)?;
    }
    let evidence_id: Uuid = admin
        .query_one(
            "INSERT INTO private.evidence_objects \
               (tenant_id, evidence_kind, payload_sha256, data_class, origin_class, \
                visibility_class, reasoning_domain_id) \
             VALUES ($1, 'EVENT', sha256(convert_to(gen_random_uuid()::text, 'UTF8')), \
                     'INTERNAL', 'DirectUserInput', 'TENANT_SHARED', $2) \
             RETURNING evidence_id",
            &[&tenant_id, &reasoning_domain_id],
        )?
        .get(0);
    admin.execute(
        "INSERT INTO private.events (event_id, event_kind, payload) \
         VALUES ($1, 'USER_MESSAGE', '{}'::jsonb)",
        &[&evidence_id],
    )?;
    Ok(SeededTenant {
        tenant_id,
        reasoning_domain_id,
        evidence_id,
    })
}

/// The admitted `PRIVATE_CONSOLIDATE` binding the worker resolves per claimed tenant.
fn seed_route_binding(
    admin: &mut Client,
    tenant_id: Uuid,
    user_id: Uuid,
    reasoning_domain_id: Uuid,
    label: &str,
) -> Result<(), postgres::Error> {
    let profile_id = seed_reasoning_profile(admin, tenant_id, user_id, label)?;
    let route_policy_id: Uuid = admin
        .query_one(
            // Left at the DRAFT default: `control.reasoning_route_policies_check_owner` refuses
            // any other opening state, and `current_reasoning_route_binding` (the resolver the
            // worker calls) keys on the BINDING's effective window, not the policy lifecycle.
            "INSERT INTO control.reasoning_route_policies \
               (tenant_id, policy_owner_user_id, purpose) \
             VALUES ($1, $2, 'PRIVATE_CONSOLIDATE') RETURNING route_policy_id",
            &[&tenant_id, &user_id],
        )?
        .get(0);
    // A PINNED policy needs exactly one priority-0 candidate before it may leave DRAFT.
    admin.execute(
        "INSERT INTO control.reasoning_route_candidates \
           (tenant_id, route_policy_id, route_policy_version, profile_id, profile_version, \
            priority) \
         VALUES ($1, $2, 1, $3, 1, 0)",
        &[&tenant_id, &route_policy_id, &profile_id],
    )?;
    // DRAFT -> SHADOW -> SERVING, the only order `reasoning_route_policies_check_owner` accepts;
    // a binding may only reference an open SHADOW/SERVING policy version.
    for state in ["SHADOW", "SERVING"] {
        admin.execute(
            "UPDATE control.reasoning_route_policies SET lifecycle_state = $2 \
             WHERE route_policy_id = $1 AND policy_version = 1",
            &[&route_policy_id, &state],
        )?;
    }
    admin.execute(
        "INSERT INTO control.reasoning_route_bindings \
           (tenant_id, reasoning_domain_id, purpose, route_policy_id, route_policy_version) \
         VALUES ($1, $2, 'PRIVATE_CONSOLIDATE', $3, 1)",
        &[&tenant_id, &reasoning_domain_id, &route_policy_id],
    )?;
    Ok(())
}

/// Inserting the PRIMARY `memory_evidence` link is what 0164's `derived_consolidate_work_enqueue`
/// trigger fires on — the test never writes an `ops.jobs` row itself, exactly like production.
fn seed_memory(handle: &mut Handle, tenant: usize) -> Uuid {
    let tenant_id = handle.tenants[tenant].tenant_id;
    let evidence_id = handle.tenants[tenant].evidence_id;
    let mut txn = handle.admin.transaction().expect("begin seed txn");
    let memory_id: Uuid = txn
        .query_one(
            "INSERT INTO private.memory_records \
               (tenant_id, memory_type, content, visibility_class, authority_class, confidence, \
                status, asserted_at) \
             VALUES ($1, 'NOTE', '{}'::jsonb, 'TENANT_SHARED', 'PrivateKnowledge', 0.7, 'active', \
                     now()) \
             RETURNING memory_id",
            &[&tenant_id],
        )
        .expect("seed active memory")
        .get(0);
    txn.execute(
        "INSERT INTO private.memory_evidence (memory_id, evidence_id, role) \
         VALUES ($1, $2, 'PRIMARY')",
        &[&memory_id, &evidence_id],
    )
    .expect("insert memory_evidence link");
    txn.commit().expect("commit seed txn");
    memory_id
}

fn config(owner: &str, lease_seconds: f64) -> DispatchConfig {
    DispatchConfig {
        lease_owner: owner.to_string(),
        lease_seconds,
        batch: 16,
        max_inputs: 1_000,
        max_attempts: 5,
    }
}

fn rollup_count(handle: &mut Handle, tenant: usize) -> i64 {
    let tenant_id = handle.tenants[tenant].tenant_id;
    handle
        .admin
        .query_one(
            "SELECT count(*) FROM private.memory_rollups WHERE tenant_id = $1",
            &[&tenant_id],
        )
        .expect("count rollups")
        .get(0)
}

fn job_ids(handle: &mut Handle, tenant: usize) -> Vec<Uuid> {
    let tenant_id = handle.tenants[tenant].tenant_id;
    handle
        .admin
        .query(
            "SELECT job_id FROM ops.jobs \
             WHERE tenant_id = $1 AND job_type = 'DERIVED_CONSOLIDATE' ORDER BY created_at",
            &[&tenant_id],
        )
        .expect("read jobs")
        .into_iter()
        .map(|r| r.get(0))
        .collect()
}

/// (1) One worker pass, no tenant id anywhere in its inputs, completes work written to BOTH
/// tenants — the whole point of the card.
#[test]
fn dispatch_discovers_and_completes_work_in_two_tenants() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(
        "dispatch_discovers_and_completes_work_in_two_tenants",
        |mut handle| {
            seed_memory(&mut handle, 0);
            seed_memory(&mut handle, 1);
            assert_eq!(
                job_ids(&mut handle, 0).len(),
                1,
                "trigger must enqueue for A"
            );
            assert_eq!(
                job_ids(&mut handle, 1).len(),
                1,
                "trigger must enqueue for B"
            );

            let port = FakePort {
                calls: AtomicUsize::new(0),
            };
            let report = handle
                .rt
                .block_on(dispatch_pass(
                    &handle.consolidation,
                    |_tenant, _run| PortRef(&port),
                    &config("worker-one", 60.0),
                ))
                .expect("dispatch pass must succeed");

            assert_eq!(
                report.claimed, 2,
                "one pass must find BOTH tenants: {report:?}"
            );
            assert_eq!(report.published, 2, "{report:?}");
            assert_eq!(port.calls.load(Ordering::SeqCst), 2);
            assert_eq!(rollup_count(&mut handle, 0), 1);
            assert_eq!(rollup_count(&mut handle, 1), 1);

            let done: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM ops.jobs \
                     WHERE tenant_id = ANY($1) AND job_type = 'DERIVED_CONSOLIDATE' \
                       AND status = 'DONE'",
                    &[&vec![
                        handle.tenants[0].tenant_id,
                        handle.tenants[1].tenant_id,
                    ]],
                )
                .expect("count done jobs")
                .get(0);
            assert_eq!(done, 2);
        },
    );
}

/// (2) The isolation half, asserted UNDER RLS: with tenant A's context installed — the context a
/// claimed job runs its whole body under — a direct SELECT for tenant B's memories returns zero
/// rows. This is the database refusing, not an application filter.
#[test]
fn claimed_job_context_cannot_read_the_other_tenant() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(
        "claimed_job_context_cannot_read_the_other_tenant",
        |mut handle| {
            let a_memory = seed_memory(&mut handle, 0);
            let b_memory = seed_memory(&mut handle, 1);
            let (a_tenant, b_tenant) = (handle.tenants[0].tenant_id, handle.tenants[1].tenant_id);

            let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("dsn");
            let mut worker =
                // dep: PostgreSQL(role_consolidation_worker) — role-scoped pool call
                Client::connect(&dsn_as_role(&dsn, "role_consolidation_worker"), NoTls)
                    .expect("connect as role_consolidation_worker");
            let mut txn = worker.transaction().expect("begin");
            txn.batch_execute(&format!("SET LOCAL humaux.tenant_id = '{a_tenant}'"))
                .expect("install tenant A context");

            let own: i64 = txn
                .query_one(
                    "SELECT count(*) FROM private.memory_records WHERE memory_id = $1",
                    &[&a_memory],
                )
                .expect("read own row")
                .get(0);
            assert_eq!(own, 1, "tenant A must still see its own memory");

            // Named by primary key, so nothing but RLS can be filtering it out.
            let other: i64 = txn
                .query_one(
                    "SELECT count(*) FROM private.memory_records WHERE memory_id = $1",
                    &[&b_memory],
                )
                .expect("read cross-tenant row")
                .get(0);
            assert_eq!(
                other, 0,
                "tenant A's context must not see tenant B's memory"
            );

            let other_jobs: i64 = txn
                .query_one(
                    "SELECT count(*) FROM ops.jobs WHERE tenant_id = $1",
                    &[&b_tenant],
                )
                .expect("read cross-tenant jobs")
                .get(0);
            assert_eq!(
                other_jobs, 0,
                "the owner arm 0164 added must NOT make ops.jobs cross-tenant readable from a \
                 worker session — only from inside the SECURITY DEFINER claim"
            );
            txn.rollback().expect("rollback probe txn");
        },
    );
}

/// (3) Two workers racing: each job is claimed exactly once, and the row counts (not the logs)
/// say so — one rollup per tenant, never two.
#[test]
fn two_racing_workers_claim_each_job_exactly_once() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(
        "two_racing_workers_claim_each_job_exactly_once",
        |mut handle| {
            seed_memory(&mut handle, 0);
            seed_memory(&mut handle, 1);

            let one = FakePort {
                calls: AtomicUsize::new(0),
            };
            let two = FakePort {
                calls: AtomicUsize::new(0),
            };
            let (one_config, two_config) = (config("racer-one", 60.0), config("racer-two", 60.0));
            let (first, second) = handle.rt.block_on(async {
                tokio::join!(
                    dispatch_pass(&handle.consolidation, |_t, _r| PortRef(&one), &one_config),
                    dispatch_pass(&handle.consolidation, |_t, _r| PortRef(&two), &two_config),
                )
            });
            let first = first.expect("racer one");
            let second = second.expect("racer two");

            assert_eq!(
                first.claimed + second.claimed,
                2,
                "each job may be claimed by exactly one racer: {first:?} / {second:?}"
            );
            assert_eq!(first.published + second.published, 2);
            assert_eq!(
                one.calls.load(Ordering::SeqCst) + two.calls.load(Ordering::SeqCst),
                2,
                "a double claim would spend a second inference call on the same job"
            );
            assert_eq!(rollup_count(&mut handle, 0), 1, "no double rollup for A");
            assert_eq!(rollup_count(&mut handle, 1), 1, "no double rollup for B");
        },
    );
}

/// (4) Recovery: a worker that died holding a lease (simulated with a lease that has already
/// expired — `kill -9` leaves exactly this row state, a PROCESSING job whose owner is gone) must
/// have its job re-claimed by another process, and the result written exactly once.
///
/// Fault injection (ADR-0036): drop the `status = 'PROCESSING' AND lease_expires_at <
/// clock_timestamp()` arm from `ops.claim_derived_work` and this test goes red — the second
/// worker claims nothing and no rollup is ever written.
#[test]
fn expired_lease_is_reclaimed_and_published_exactly_once() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(
        "expired_lease_is_reclaimed_and_published_exactly_once",
        |mut handle| {
            seed_memory(&mut handle, 0);
            let jobs_before = job_ids(&mut handle, 0);
            assert_eq!(jobs_before.len(), 1);

            // The dead worker's claim. A sub-second lease is the observable equivalent of a
            // process that was killed before it could heartbeat: the row is PROCESSING under an
            // owner that will never come back.
            let dead = handle
                .rt
                .block_on(jobs::claim_derived_work_consolidation(
                    &handle.consolidation,
                    &[DerivedJobType::Consolidate],
                    "killed-worker",
                    0.05,
                    16,
                ))
                .expect("dead worker claim");
            assert_eq!(dead.len(), 1, "the dead worker must have held the job");
            assert_eq!(dead[0].job_id, jobs_before[0]);
            assert_eq!(rollup_count(&mut handle, 0), 0, "it died before publishing");

            std::thread::sleep(std::time::Duration::from_millis(200));

            let port = FakePort {
                calls: AtomicUsize::new(0),
            };
            let report = handle
                .rt
                .block_on(dispatch_pass(
                    &handle.consolidation,
                    |_t, _r| PortRef(&port),
                    &config("survivor", 60.0),
                ))
                .expect("survivor pass");
            assert_eq!(
                report.claimed, 1,
                "the expired lease must be re-claimable: {report:?}"
            );
            assert_eq!(report.published, 1, "{report:?}");
            assert_eq!(
                rollup_count(&mut handle, 0),
                1,
                "exactly once: the dead worker published nothing and the survivor published once"
            );

            // The dead worker's stale fencing token must settle nothing, so a straggler coming
            // back from the dead cannot flip the job the survivor already finished.
            let straggler = handle
                .rt
                .block_on(jobs::settle_derived_consolidation(
                    &handle.consolidation,
                    &jobs::DerivedLease::of(&dead[0], "killed-worker"),
                    jobs::DerivedWorkOutcome::Done,
                    60.0,
                ))
                .expect("straggler settle");
            assert!(!straggler, "a stale lease must settle nothing");
        },
    );
}

/// (5) `--run-once` with no input: the pass claims nothing and returns, which is what makes the
/// flag exit zero promptly instead of looping (the deployment report's finding).
#[test]
fn dispatch_pass_with_no_pending_work_claims_nothing() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(
        "dispatch_pass_with_no_pending_work_claims_nothing",
        |handle| {
            // No memory seeded for either tenant: the enqueue triggers never fired.
            let port = FakePort {
                calls: AtomicUsize::new(0),
            };
            let started = std::time::Instant::now();
            let report = handle
                .rt
                .block_on(dispatch_pass(
                    &handle.consolidation,
                    |_t, _r| PortRef(&port),
                    &config("idle-worker", 60.0),
                ))
                .expect("idle pass must succeed, not hang");
            assert_eq!(report.claimed, 0, "{report:?}");
            assert_eq!(report.published, 0);
            assert_eq!(port.calls.load(Ordering::SeqCst), 0);
            assert!(
                started.elapsed() < std::time::Duration::from_secs(10),
                "an empty pass must return promptly, not poll"
            );
        },
    );
}

/// A `DERIVED_CONSOLIDATE` job whose payload the worker cannot read is a permanent defect of that
/// row: it must be retried a bounded number of times and then parked, never spun on forever.
#[test]
fn unreadable_payload_is_deferred_then_parked() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(
        "unreadable_payload_is_deferred_then_parked",
        |mut handle| {
            seed_memory(&mut handle, 0);
            let job = job_ids(&mut handle, 0)[0];
            handle
                .admin
                .execute(
                    "UPDATE ops.jobs SET payload = '{}'::jsonb WHERE job_id = $1",
                    &[&job],
                )
                .expect("blank the payload");

            let port = FakePort {
                calls: AtomicUsize::new(0),
            };
            let mut config = config("parker", 60.0);
            config.max_attempts = 1;
            let report = handle
                .rt
                .block_on(dispatch_pass(
                    &handle.consolidation,
                    |_t, _r| PortRef(&port),
                    &config,
                ))
                .expect("pass must not fail on one bad row");
            assert_eq!(report.claimed, 1, "{report:?}");
            assert_eq!(report.dead, 1, "{report:?}");
            assert_eq!(port.calls.load(Ordering::SeqCst), 0);

            let status: String = handle
                .admin
                .query_one("SELECT status FROM ops.jobs WHERE job_id = $1", &[&job])
                .expect("read status")
                .get(0);
            assert_eq!(status, "DEAD");
        },
    );
}

/// (6) The publish-then-lose-the-lease path, which is where "the result is written exactly once"
/// actually used to break: the recovery test above only covers a worker that died BEFORE
/// publishing. A worker whose own run outlives its lease is re-claimed by the next pass, and
/// before ADR-0036 D4 it went on to publish a SECOND rollup over the same inputs —
/// `publish_rollup` had no idempotency and `select_and_materialize_inputs` no "already rolled
/// up" predicate, so nothing downstream would have caught it either.
///
/// The fix is structural, not a timing improvement: `publish_rollup` settles the job `DONE`
/// inside its OWN transaction, so a lost fence rolls the rollup back with it.
///
/// Fault injection: put `AND lease_expires_at > clock_timestamp()` back into
/// `jobs::settle_derived_in_txn`'s WHERE clause, or drop the `fence` argument from
/// `publish_rollup`'s call site, and this test goes red with two rollups.
#[test]
fn a_lease_lost_at_publish_time_writes_no_second_rollup() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(
        "a_lease_lost_at_publish_time_writes_no_second_rollup",
        |mut handle| {
            seed_memory(&mut handle, 0);

            // The slow worker claims with a lease its own inference hop will outlive.
            let slow = handle
                .rt
                .block_on(jobs::claim_derived_work_consolidation(
                    &handle.consolidation,
                    &[DerivedJobType::Consolidate],
                    "slow-worker",
                    0.05,
                    16,
                ))
                .expect("slow worker claim");
            assert_eq!(slow.len(), 1);
            std::thread::sleep(std::time::Duration::from_millis(200));

            // While it is still "thinking", the next pass re-claims the expired lease and
            // publishes the one legitimate rollup.
            let survivor_port = FakePort {
                calls: AtomicUsize::new(0),
            };
            let survivor = handle
                .rt
                .block_on(dispatch_pass(
                    &handle.consolidation,
                    |_t, _r| PortRef(&survivor_port),
                    &config("survivor", 60.0),
                ))
                .expect("survivor pass");
            assert_eq!(survivor.published, 1, "{survivor:?}");
            assert_eq!(rollup_count(&mut handle, 0), 1);

            // NOW the slow worker comes back and runs its publish leg under its stale fence.
            let slow_port = FakePort {
                calls: AtomicUsize::new(0),
            };
            let lease = jobs::DerivedLease::of(&slow[0], "slow-worker");
            let (tenant_id, reasoning_domain_id) = (
                handle.tenants[0].tenant_id,
                handle.tenants[0].reasoning_domain_id,
            );
            let outcome = handle
                .rt
                .block_on(run_once_bound(
                    &handle.consolidation,
                    |_run_id| PortRef(&slow_port),
                    tenant_id,
                    reasoning_domain_id,
                    ReasoningRouteBindingId(Uuid::from_u128(0x3002)),
                    ReasoningRouteBindingVersion(1),
                    None,
                    1_000,
                    build_rollup,
                    Some(&lease),
                ))
                .expect("slow worker publish leg");
            assert!(
                matches!(outcome, PublishOutcome::LostLease),
                "a re-claimed job's publish must abort, not publish a second rollup"
            );
            assert_eq!(
                rollup_count(&mut handle, 0),
                1,
                "exactly once: the slow worker's whole transaction rolled back"
            );
        },
    );
}

/// (7) A tenant whose `PRIVATE_CONSOLIDATE` route binding has not been admitted yet is NOT a
/// failing job. `consolidate_repo::resolve_consolidate_binding` calls the condition
/// "environmental (retryable)" itself, and the 0164 enqueue trigger's idempotency key is per
/// `memory_id` with `ON CONFLICT DO NOTHING`, so a job parked `DEAD` here is that memory's
/// consolidation dropped permanently. It must be released with a backoff and never parked.
///
/// Fault injection: route the missing binding back through `retry_or_park` (i.e. return
/// `RunOnceError::Reasoning` instead of `NotReady`) and the `dead == 0` assertion goes red;
/// remove the `next_retry_at` write from `jobs::settle_derived_in_txn` and the "second pass
/// claims nothing" assertion goes red.
#[test]
fn an_unprovisioned_tenant_is_released_with_a_backoff_and_never_parked() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(
        "an_unprovisioned_tenant_is_released_with_a_backoff_and_never_parked",
        |mut handle| {
            seed_memory(&mut handle, 2); // tenant C: no admitted route binding
            let job = job_ids(&mut handle, 2)[0];

            let port = FakePort {
                calls: AtomicUsize::new(0),
            };
            // `max_attempts = 1` is the harshest budget there is: under the pre-ADR-0036 D5
            // shape this single pass parked the job DEAD.
            let mut cfg = config("not-ready-worker", 60.0);
            cfg.max_attempts = 1;
            let report = handle
                .rt
                .block_on(dispatch_pass(
                    &handle.consolidation,
                    |_t, _r| PortRef(&port),
                    &cfg,
                ))
                .expect("pass must not fail on an unprovisioned tenant");
            assert_eq!(report.claimed, 1, "{report:?}");
            assert_eq!(report.not_ready, 1, "{report:?}");
            assert_eq!(
                report.dead, 0,
                "an unprovisioned tenant must never lose its consolidation job: {report:?}"
            );
            assert_eq!(port.calls.load(Ordering::SeqCst), 0);

            let row = handle
                .admin
                .query_one(
                    "SELECT status, next_retry_at > clock_timestamp() FROM ops.jobs \
                     WHERE job_id = $1",
                    &[&job],
                )
                .expect("read released job");
            let status: String = row.get(0);
            let backed_off: bool = row.get(1);
            assert_eq!(status, "PENDING");
            assert!(
                backed_off,
                "the release must push next_retry_at out — otherwise every --serve poll burns \
                 one attempt with zero delay"
            );

            // The backoff is what makes it a backoff: an immediately-following pass claims
            // nothing rather than spending another attempt.
            let again = handle
                .rt
                .block_on(dispatch_pass(
                    &handle.consolidation,
                    |_t, _r| PortRef(&port),
                    &cfg,
                ))
                .expect("second pass");
            assert_eq!(
                again.claimed, 0,
                "a backed-off job must not be re-claimable on the very next poll: {again:?}"
            );
        },
    );
}

/// (8) Exactly-once for the claim itself, DETERMINISTICALLY — no `tokio::join!` of two futures
/// whose statements may never land in the same lock window. One session claims inside an OPEN
/// transaction; a second session on its own connection claims while that lock is held, and the
/// holder commits underneath it.
///
/// Both admitted mechanisms are exercised: with `FOR UPDATE SKIP LOCKED` the racer skips the
/// locked row immediately; without it, the racer BLOCKS and PostgreSQL's EvalPlanQual re-check
/// re-applies the outer `UPDATE`'s predicate to the row version the holder just committed —
/// which is why that predicate is restated on the `UPDATE` and not only in the `picked` CTE.
/// Delete BOTH guards from `ops.claim_derived_work` and the racer claims the same job twice.
#[test]
fn two_serialized_claims_take_each_job_exactly_once_under_a_held_lock() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(
        "two_serialized_claims_take_each_job_exactly_once_under_a_held_lock",
        |mut handle| {
            seed_memory(&mut handle, 0);
            assert_eq!(job_ids(&mut handle, 0).len(), 1);

            let worker_dsn = dsn_as_role(&handle.dsn, "role_consolidation_worker");
            let kinds = vec!["DERIVED_CONSOLIDATE".to_owned()];
            let mut holder =
                // dep: PostgreSQL(role_consolidation_worker) — role-scoped pool call
                Client::connect(&worker_dsn, NoTls).expect("connect the holding session");
            let mut txn = holder.transaction().expect("begin holder txn");
            let held = txn
                .query(
                    "SELECT job_id FROM ops.claim_derived_work($1, $2, $3, $4)",
                    &[&kinds, &"holder", &60.0_f64, &16_i64],
                )
                .expect("holder claim");
            assert_eq!(held.len(), 1, "the holder took the only job");

            let racer_dsn = worker_dsn.clone();
            let racer_kinds = kinds.clone();
            let racer = std::thread::spawn(move || {
                let mut client =
                    // dep: PostgreSQL(role_consolidation_worker) — role-scoped pool call
                    Client::connect(&racer_dsn, NoTls).expect("connect the racing session");
                client
                    .query(
                        "SELECT job_id FROM ops.claim_derived_work($1, $2, $3, $4)",
                        &[&racer_kinds, &"racer", &60.0_f64, &16_i64],
                    )
                    .expect("racer claim")
                    .len()
            });

            // Give the racer's statement time to reach the server (and, under the fault
            // injection, to block on the holder's row lock) before the holder commits.
            std::thread::sleep(std::time::Duration::from_millis(300));
            txn.commit().expect("holder commits its claim");

            let racer_rows = racer.join().expect("racer thread");
            assert_eq!(
                racer_rows, 0,
                "a job already claimed by a live lease must never be handed to a second worker"
            );
        },
    );
}

/// (9) The acceptance item "`--run-once` with no input exits zero promptly" asserted against the
/// BINARY, not against an in-process `report.claimed == 0`: the flag's exit path lives in
/// `src/main.rs`'s `dispatch_mode`, which no in-process test reaches.
#[test]
fn run_once_binary_exits_zero_promptly_with_no_input() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(
        "run_once_binary_exits_zero_promptly_with_no_input",
        |handle| {
            // Nothing seeded: the enqueue triggers never fired, so the queue is empty.
            let started = std::time::Instant::now();
            let output =
                // dep: subprocess(humaux-consolidation-worker) — spawns the consolidation-worker binary under test
                std::process::Command::new(env!("CARGO_BIN_EXE_humaux-consolidation-worker"))
                    .arg("--run-once")
                    .env(
                        "CONSOLIDATION_WORKER_PG_DSN",
                        dsn_as_role(&handle.dsn, "role_consolidation_worker"),
                    )
                    // Never dialled: an empty pass claims no job, so no inference hop happens. A path
                    // that cannot exist is the point — if the pass ever tried to dial, this test would
                    // fail instead of silently passing.
                    .env(
                        "HUMAUX_CONSOLIDATION_WORKER_RPC_SOCKET_PATH",
                        "/nonexistent/humaux-consolidation-worker-run-once.sock",
                    )
                    .env("HUMAUX_CONSOLIDATION_WORKER_CALL_TTL_SECS", "30")
                    .env("HUMAUX_CONSOLIDATION_WORKER_DIAL_TIMEOUT_SECS", "5")
                    .env("HUMAUX_CONSOLIDATION_WORKER_LEASE_SECS", "60")
                    .env("HUMAUX_CONSOLIDATION_WORKER_BATCH", "16")
                    .env("HUMAUX_CONSOLIDATION_WORKER_MAX_INPUTS", "1000")
                    .env("HUMAUX_CONSOLIDATION_WORKER_MAX_ATTEMPTS", "5")
                    // `--serve`'s poll interval must be absent, or a leaked one would make the binary
                    // resident and this test hang — which is exactly the bug the flag had.
                    .env_remove("HUMAUX_CONSOLIDATION_WORKER_POLL_INTERVAL_SECS")
                    .output()
                    .expect("spawn humaux-consolidation-worker --run-once");
            let elapsed = started.elapsed();
            assert!(
                output.status.success(),
                "--run-once must exit zero on an empty queue: status={:?} stderr={}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                elapsed < std::time::Duration::from_secs(30),
                "--run-once must exit promptly, not poll: took {elapsed:?}"
            );
            assert!(
                String::from_utf8_lossy(&output.stdout).contains("claimed=0"),
                "the pass must have run and found nothing: stdout={}",
                String::from_utf8_lossy(&output.stdout)
            );
        },
    );
}

// ---------------------------------------------------------------------------
// Card 15 / ADR-0037 — readiness and graceful shutdown, asserted against the BINARY
// ---------------------------------------------------------------------------

/// macOS XProtect assesses a freshly linked binary on its first exec (~1 min, sometimes much
/// longer under load). Every test below spawns `humaux-consolidation-worker` under a deadline,
/// so pay that cost once, up front, on a run that measures nothing.
fn warm_binary() {
    // dep: subprocess(humaux-consolidation-worker) — spawns the consolidation-worker binary under test
    let _ = std::process::Command::new(env!("CARGO_BIN_EXE_humaux-consolidation-worker"))
        .arg("--warm-up-not-a-mode")
        .env_clear()
        .output();
}

/// Short `/tmp` socket path — the same convention `tests/consolidation_hop_e2e.rs` uses, and
/// for the same reason: macOS's `std::env::temp_dir()` (`/var/folders/…/T/`) plus a uuid
/// overruns `sockaddr_un.sun_path` (SUN_LEN, 104 bytes) and `bind` fails with InvalidInput.
fn socket_path(tag: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("/tmp/hc15-{tag}-{}.sock", Uuid::now_v7().simple()))
}

/// The `--serve` environment card 14's `run_once_binary_exits_zero_promptly_with_no_input`
/// established, plus the two keys resident mode adds. `socket_path` is the ONLY thing the two
/// card-15 tests vary.
fn serve_command(dsn: &str, socket_path: &str) -> std::process::Command {
    // dep: subprocess(humaux-consolidation-worker) — spawns the consolidation-worker binary under test
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_humaux-consolidation-worker"));
    cmd.env("CONSOLIDATION_WORKER_PG_DSN", dsn)
        .env("HUMAUX_CONSOLIDATION_WORKER_RPC_SOCKET_PATH", socket_path)
        .env("HUMAUX_CONSOLIDATION_WORKER_CALL_TTL_SECS", "5")
        .env("HUMAUX_CONSOLIDATION_WORKER_DIAL_TIMEOUT_SECS", "5")
        .env("HUMAUX_CONSOLIDATION_WORKER_LEASE_SECS", "120")
        .env("HUMAUX_CONSOLIDATION_WORKER_BATCH", "16")
        .env("HUMAUX_CONSOLIDATION_WORKER_MAX_INPUTS", "1000")
        .env("HUMAUX_CONSOLIDATION_WORKER_MAX_ATTEMPTS", "5")
        .env("HUMAUX_CONSOLIDATION_WORKER_POLL_INTERVAL_SECS", "1")
        // ADR-0061 D-B: `--serve`'s own ops listener, a free loopback port bound and released here.
        .env(
            "HUMAUX_CONSOLIDATION_WORKER_SERVE_METRICS_ADDR",
            std::net::TcpListener::bind("127.0.0.1:0")
                .and_then(|l| l.local_addr())
                .expect("reserve a loopback port")
                .to_string(),
        );
    cmd
}

/// (10) ADR-0037 D1: `--readyz` probes its dependencies LIVE and, when one is down, exits
/// non-zero having NAMED it — never a bare failure and never a healthy-looking zero (§4.4 坑5
/// applied to readiness). Both directions are asserted in one test so a probe that always
/// passes and a probe that always fails are equally red.
///
/// 注错: drop the `UnixStream::connect` arm from `readyz()` (leave only the DB connect) ⇒ the
/// first half goes green when it must be red, and this test names the socket that was not
/// probed.
#[test]
fn readyz_binary_probes_its_uds_peer_and_names_it_when_down() {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(
        "readyz_binary_probes_its_uds_peer_and_names_it_when_down",
        |handle| {
            warm_binary();
            let dsn = dsn_as_role(&handle.dsn, "role_consolidation_worker");

            // (a) peer down: named, non-zero.
            let down = serve_command(&dsn, "/nonexistent/humaux-card15-readyz.sock")
                .arg("--readyz")
                .output()
                .expect("spawn --readyz with a dead peer");
            let stderr = String::from_utf8_lossy(&down.stderr).to_string();
            assert!(
                !down.status.success(),
                "--readyz must fail when its UDS peer is down: stderr={stderr}"
            );
            assert!(
                stderr.contains("missing object") && stderr.contains("inference RPC socket"),
                "a failed readiness probe must NAME the object that is down: stderr={stderr}"
            );

            // (b) peer up: exit zero. A bare listener is enough — readiness dials and drops,
            // it never sends a request.
            let socket_path = socket_path("readyz");
            let _ = std::fs::remove_file(&socket_path);
            // dep: UDS(serve) — unix-socket RPC
            let listener = std::os::unix::net::UnixListener::bind(&socket_path)
                .expect("bind the stand-in private-worker socket");
            let accepting = std::thread::spawn(move || {
                // One accept is all readiness makes; the thread ends with the test.
                let _ = listener.accept();
            });
            let up = serve_command(&dsn, &socket_path.to_string_lossy())
                .arg("--readyz")
                .output()
                .expect("spawn --readyz with a live peer");
            let _ = accepting.join();
            let _ = std::fs::remove_file(&socket_path);
            assert!(
                up.status.success(),
                "--readyz must exit zero when every dependency answers: stderr={}",
                String::from_utf8_lossy(&up.stderr)
            );
            assert!(
                String::from_utf8_lossy(&up.stdout).contains("ready"),
                "stdout={}",
                String::from_utf8_lossy(&up.stdout)
            );
        },
    );
}

/// (10b) ADR-0037 D2, the dependency the down-path never exercised: PostgreSQL itself. The DSN
/// points at a loopback port that was bound and released, so the connect is genuinely refused —
/// a real absence, not a mock, and not the shared fixture container every other suite on this
/// machine needs left running. The DB is probed FIRST, so naming it here also proves the arm at
/// `readyz()`'s `ConsolidationDbPool::connect` actually runs.
///
/// Needs no database, which is the point: there is nothing listening on that port either way.
///
/// 注错: drop the `?` on `ConsolidationDbPool::connect` in `readyz()` ⇒ the probe exits zero
/// against a dead database and this goes red.
#[test]
fn readyz_binary_names_postgresql_when_the_database_is_down() {
    warm_binary();
    let dead = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a port");
        let port = listener.local_addr().expect("reserved port").port();
        drop(listener);
        port
    };
    let dsn = format!("postgres://role_consolidation_worker@127.0.0.1:{dead}/humaux_thread_dev");
    let output = serve_command(&dsn, "/nonexistent/humaux-card15-readyz.sock")
        .arg("--readyz")
        .output()
        .expect("spawn --readyz against a dead database");
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(
        !output.status.success(),
        "--readyz must fail when PostgreSQL is down: stderr={stderr}"
    );
    assert!(
        stderr.contains("missing object")
            && stderr.contains("PostgreSQL as role_consolidation_worker"),
        "a failed readiness probe must NAME the object that is down: stderr={stderr}"
    );
}

/// (11) ADR-0037 D3, the card's acceptance item asserted against the BINARY and then against
/// SQL: SIGTERM delivered while a pass is genuinely in flight must drain and exit ZERO, leaving
/// no job `PROCESSING` with a live lease that only expiry could free.
///
/// "In flight" is made deterministic rather than hoped for: the stand-in private-worker socket
/// ACCEPTS the connection and then never answers, so the claimed job sits in the §11.8 inference
/// hop until `HUMAUX_CONSOLIDATION_WORKER_CALL_TTL_SECS` elapses. The test waits until
/// `ops.jobs` actually shows `PROCESSING` — i.e. the claim has happened — and only then signals.
///
/// 注错: move the `shutdown.recv()` arm from the poll wait INTO the pass (cancel `dispatch_pass`
/// mid-flight) ⇒ the claimed job is left `PROCESSING` with a live lease and the final assertion
/// goes red, naming the job id.
#[test]
fn sigterm_mid_pass_drains_and_leaves_no_job_processing_with_a_live_lease() {
    drain_mid_pass_leaves_no_live_lease(
        "TERM",
        "sigterm_mid_pass_drains_and_leaves_no_job_processing_with_a_live_lease",
    );
}

/// (11b) The SAME invariant for Ctrl-C, which `Shutdown`'s doc claims is latched before the
/// first pass. It was not: `tokio::signal::ctrl_c()` is an `async fn` that registers SIGINT on
/// its FIRST POLL, and the first poll happens in the `select!` AFTER a pass returns — so a
/// Ctrl-C during the first pass hit SIGINT's default disposition and killed the worker holding a
/// claimed job, which is exactly the state card 15 forbids.
///
/// 注错: put `_ = tokio::signal::ctrl_c() => {}` back in place of the eagerly installed
/// `SignalKind::interrupt()` stream in `Shutdown` ⇒ the worker is killed mid-pass, exits
/// non-zero, and leaves the claimed job `PROCESSING` with a live lease: two assertions red.
#[test]
fn sigint_mid_pass_drains_and_leaves_no_job_processing_with_a_live_lease() {
    drain_mid_pass_leaves_no_live_lease(
        "INT",
        "sigint_mid_pass_drains_and_leaves_no_job_processing_with_a_live_lease",
    );
}

fn drain_mid_pass_leaves_no_live_lease(signal: &str, test_name: &'static str) {
    let _guard = SERIAL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    run_db_fixture::<DispatchFixture, _>(test_name, |mut handle| {
        warm_binary();
        seed_memory(&mut handle, 0);
        assert_eq!(job_ids(&mut handle, 0).len(), 1, "one job to claim");
        let tenant_id = handle.tenants[0].tenant_id;

        let socket_path = socket_path("sigterm");
        let _ = std::fs::remove_file(&socket_path);
        // dep: UDS(serve) — unix-socket RPC
        let listener = std::os::unix::net::UnixListener::bind(&socket_path)
            .expect("bind the stand-in private-worker socket");
        // Accept and hold: the worker's inference hop blocks here until its call TTL, which
        // is the window this test needs the signal to land in.
        let stalling = std::thread::spawn(move || {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept() {
                held.push(stream);
                if held.len() > 8 {
                    break;
                }
            }
        });

        let dsn = dsn_as_role(&handle.dsn, "role_consolidation_worker");
        let mut child = serve_command(&dsn, &socket_path.to_string_lossy())
            .arg("--serve")
            .spawn_reaped("spawn humaux-consolidation-worker --serve");

        // Wait for the claim to actually have happened (bounded, monotonic).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        let mut claimed = false;
        while std::time::Instant::now() < deadline {
            let processing: i64 = handle
                .admin
                .query_one(
                    "SELECT count(*) FROM ops.jobs \
                         WHERE tenant_id = $1 AND status = 'PROCESSING'",
                    &[&tenant_id],
                )
                .expect("read job status")
                .get(0);
            if processing > 0 {
                claimed = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        assert!(
            claimed,
            "the worker never claimed the seeded job — this test would assert nothing"
        );

        // The signal, mid-pass by construction.
        // dep: subprocess(kill) — spawns external process
        let signalled = std::process::Command::new("kill")
            .arg(format!("-{signal}"))
            .arg(child.id().to_string())
            .status()
            .expect("send the termination signal");
        assert!(signalled.success(), "kill -{signal} failed");

        // Drain must complete well inside the call TTL plus one poll interval; the deadline
        // is generous but bounded, so a worker that ignores SIGTERM fails rather than hangs.
        let exit_deadline = std::time::Instant::now() + std::time::Duration::from_secs(90);
        let status = loop {
            match child.try_wait().expect("poll the worker") {
                Some(status) => break status,
                None if std::time::Instant::now() >= exit_deadline => {
                    panic!("the worker did not exit within 90s of SIG{signal}");
                }
                None => std::thread::sleep(std::time::Duration::from_millis(100)),
            }
        };
        drop(stalling);
        let _ = std::fs::remove_file(&socket_path);

        assert!(
            status.success(),
            "a worker drained by SIG{signal} must exit zero, got {status:?} — a non-zero \
                 status here means the signal hit its default disposition instead of a handler"
        );

        // The invariant this card exists for, read from the database and not from a log.
        let stuck = handle
            .admin
            .query(
                "SELECT job_id, status, lease_owner FROM ops.jobs \
                     WHERE tenant_id = $1 AND status = 'PROCESSING' \
                       AND lease_expires_at > clock_timestamp()",
                &[&tenant_id],
            )
            .expect("read leases");
        let stuck_ids: Vec<Uuid> = stuck.iter().map(|r| r.get(0)).collect();
        assert!(
            stuck.is_empty(),
            "a drained worker left {} job(s) PROCESSING with a live lease only expiry could \
                 free: {stuck_ids:?}",
            stuck.len()
        );
    });
}
