//! `adapters::tests::reasoning_route_runtime` — ADR-0060 (card 33b slice 1, migration 0206) against the real PostgreSQL:
//!   the tenant-scoped capabilities definer, the locator's Profile capabilities, the private ledger row equal to the
//!   admitted route (all-or-none, no role exemption), the one-transaction reserve with its matching disclosure, the
//!   processing-run Profile@version, the credential-accounts definer, request_extras, and worker-observed health.
//! Depends-on: crates=[humaux-adapters, humaux-application, humaux-domain, humaux-testkit, postgres, tokio, uuid];
//!   services=[PostgreSQL(owner) r=[control.reasoning_profiles, ops.data_disclosures, ops.model_call_ledger,
//!   ops.reasoning_account_health_observations, ops.reasoning_provider_health_observations]
//!   w=[control.reasoning_profiles, control.tenants, ops.jobs, ops.model_call_ledger, private.processing_runs],
//!   PostgreSQL(role_private_worker) w=[ops.data_disclosures, ops.model_call_ledger]
//!   x=[control.observe_reasoning_route_health, control.reasoning_credential_accounts,
//!   control.reasoning_profile_capabilities, control.reasoning_route_health_state], PostgreSQL(role_gateway)]; env=[HUMAUX_TEST_PG_DSN];
//!   modules=[adapters::byok, adapters::disclosure, adapters::model_call_ledger, adapters::postgres,
//!   adapters::reasoning_route_admission, adapters::tests::support::private_route, application::consolidate,
//!   domain::dataclass, domain::egress, domain::ids, domain::ledger, humaux-testkit]
//! Called-by: [cargo-test]
//! Invariants: [every test seeds its own tenants and catalog rows; ledger, disclosure, profile and health rows are
//!   append-only and stay (forensics), the fixture deletes its tenants' jobs and processing runs in one printed batch
//!   and tries the tenant rows in a separate best-effort batch; a missing DB is a fixture error under
//!   HUMAUX_REQUIRE_DB=1]
//! Spec: Baseline §11.2; §11.2.4; §11.2.5; §11.5; §19.1; ADR-0060 D-A, D-I, D-J, D-K, D-N, ruling E3
//!
//! Each test names, in its doc, the fault that turns it red.

#[path = "support/private_route.rs"]
mod private_route;

use std::time::Duration;

use humaux_adapters::byok::{ADAPTER_OWNED_REQUEST_KEYS, ReasoningCapability};
use humaux_adapters::disclosure::DisclosureSource;
use humaux_adapters::model_call_ledger::{self, FinalizeCall, ModelCallOutcome, ReservedCall};
use humaux_adapters::postgres::PrivateWorkerDbPool;
use humaux_adapters::reasoning_route_admission::ReasoningAdmissionLocator;
use humaux_application::consolidate::PrivateReasoningPurpose;
use humaux_domain::dataclass::DataClass;
use humaux_domain::egress::{self, AuthorizedEgressPayload, PrivateDataPurpose};
use humaux_domain::ids::TenantId;
use humaux_domain::ledger::ModelCallPurpose;
use humaux_testkit::{DbFixtureSkipReason, DbIntegrationFixture, run_db_fixture};
use postgres::error::SqlState;
use postgres::{Client, NoTls};
use private_route::{Binding, Owner, Profile, dsn_as_role};
use uuid::Uuid;

const DISTILL: &str = "PRIVATE_DISTILL_TEXT";
const BASE_CAPS: [&str; 2] = ["TEXT", "STRUCTURED_OUTPUT"];

struct Handle {
    rt: tokio::runtime::Runtime,
    admin: Client,
    private: PrivateWorkerDbPool,
    dsn: String,
    tenants: Vec<Uuid>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        let ids = self
            .tenants
            .iter()
            .map(|t| format!("'{t}'"))
            .collect::<Vec<_>>()
            .join(",");
        if ids.is_empty() {
            return;
        }
        // Card-31 lesson: jobs and data rows in ONE printed batch; the tenant rows separately and
        // best-effort (profiles, ledger and disclosure rows are append-only and keep them alive).
        if let Err(e) = self.admin.batch_execute(&format!(
            "DELETE FROM ops.jobs WHERE tenant_id IN ({ids}); \
             DELETE FROM private.processing_runs WHERE tenant_id IN ({ids});"
        )) {
            eprintln!("reasoning_route_runtime cleanup: jobs/data batch failed: {e}");
        }
        let _ = self.admin.batch_execute(&format!(
            "DELETE FROM control.tenants WHERE tenant_id IN ({ids});"
        ));
    }
}

struct Fixture;

impl DbIntegrationFixture for Fixture {
    type Handle = Handle;

    fn isolate() -> Result<Handle, DbFixtureSkipReason> {
        let dsn =
            std::env::var("HUMAUX_TEST_PG_DSN").map_err(|_| DbFixtureSkipReason::NoDatabaseUrl)?;
        // dep: PostgreSQL(owner) — seeding, inspection and cleanup
        let mut admin = Client::connect(&dsn, NoTls)
            .map_err(|e| DbFixtureSkipReason::ConnectFailed(e.to_string()))?;
        let migrated: bool = admin
            .query_one(
                "SELECT to_regprocedure('control.reasoning_profile_capabilities(uuid,bigint)') IS NOT NULL",
                &[],
            )
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?
            .get(0);
        if !migrated {
            return Err(DbFixtureSkipReason::IsolationSetupFailed(
                "the 0206 definers are missing — run `cargo xtask migrate`".into(),
            ));
        }
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        let private = rt
            // dep: PostgreSQL(role_private_worker) — the worker's own pool
            .block_on(PrivateWorkerDbPool::connect(&dsn_as_role(
                &dsn,
                "role_private_worker",
            )))
            .map_err(|e| DbFixtureSkipReason::IsolationSetupFailed(e.to_string()))?;
        Ok(Handle {
            rt,
            admin,
            private,
            dsn,
            tenants: Vec::new(),
        })
    }
}

impl Handle {
    fn owner(&mut self) -> Owner {
        let owner = private_route::seed_owner(&mut self.admin, "reasoning_route_runtime.rs");
        self.tenants.push(owner.tenant);
        owner
    }

    /// An owner with one Profile bound for PRIVATE_DISTILL_TEXT, healthy for `health_secs`.
    fn routed(&mut self, health_secs: f64) -> (Owner, Profile, Binding) {
        let owner = self.owner();
        let profile = private_route::seed_profile(
            &mut self.admin,
            owner,
            &BASE_CAPS,
            &BASE_CAPS,
            health_secs,
        );
        let binding = private_route::bind(&mut self.admin, owner, DISTILL, &profile);
        (owner, profile, binding)
    }

    fn admit(&self, owner: Owner, binding: Binding) -> Option<ReasoningAdmissionLocator> {
        private_route::admit(
            &self.rt,
            &self.private,
            owner,
            binding,
            PrivateReasoningPurpose::Distill,
        )
    }

    fn reserve(
        &mut self,
        owner: Owner,
        locator: &ReasoningAdmissionLocator,
    ) -> Result<(ReservedCall, Uuid), model_call_ledger::ModelCallLedgerError> {
        let evidence = private_route::seed_evidence(&mut self.admin, owner);
        let payload = AuthorizedEgressPayload::new(b"reasoning_route_runtime probe".to_vec());
        let permit = egress::authorize(
            TenantId(locator.tenant_id),
            locator.egress_processor_id,
            PrivateDataPurpose::UserReasoning,
            DataClass::Private,
            &payload,
            Duration::from_secs(300),
        )
        .expect("a Private USER_REASONING permit");
        self.rt
            .block_on(model_call_ledger::reserve_private_call_with_disclosure(
                &self.private,
                ModelCallPurpose::PrivateDistillText,
                locator,
                &permit,
                &payload,
                &[DisclosureSource::Evidence(evidence)],
            ))
    }

    fn finalize(&self, tenant: Uuid, call: Uuid, outcome: ModelCallOutcome) {
        let changed = self
            .rt
            .block_on(model_call_ledger::finalize_private_call(
                &self.private,
                tenant,
                call,
                outcome,
                &FinalizeCall {
                    latency_ms: Some(5),
                    ..Default::default()
                },
            ))
            .expect("finalize");
        assert!(changed, "the reservation finalizes once");
    }

    /// A direct connection as `role`, for statements the adapter never sends.
    fn as_role(&self, role: &str) -> Client {
        // dep: PostgreSQL(any) — role-scoped connection (role_private_worker / role_gateway) for direct statements
        Client::connect(&dsn_as_role(&self.dsn, role), NoTls).expect("role connection")
    }
}

fn run(name: &str, body: impl FnOnce(Handle)) {
    run_db_fixture::<Fixture, _>(name, body);
}

/// The 0130 column list of a private row, valued from `l` with `profile` / `binding_version`
/// substituted (the forgery under test).
fn insert_private_row(
    client: &mut impl postgres::GenericClient,
    l: &ReasoningAdmissionLocator,
    profile: (Uuid, i64),
    binding_version: i64,
) -> Result<Uuid, postgres::Error> {
    client
        .query_one(
            "INSERT INTO ops.model_call_ledger(tenant_id,purpose,provider,model,model_revision,reasoning_domain_id,binding_id,binding_version,route_policy_id,route_policy_version,profile_id,profile_version,provider_account_id,provider_endpoint_id,egress_processor_id,credential_ref,billing_account_id,billing_instrument_id,provider_health_observation_id,account_health_observation_id,billing_responsibility,admitted_at) \
             VALUES($1,'PRIVATE_DISTILL_TEXT',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,'USER','epoch'::timestamptz + $20::bigint * interval '1 microsecond') RETURNING model_call_id",
            &[
                &l.tenant_id,
                &l.processor_id,
                &l.provider_model_id,
                &l.model_revision,
                &l.reasoning_domain_id.0,
                &l.binding_id.0,
                &binding_version,
                &l.route_policy_id,
                &l.route_policy_version,
                &profile.0,
                &profile.1,
                &l.provider_account_id,
                &l.provider_endpoint_id,
                &l.egress_processor_id.0,
                &l.credential_ref,
                &l.billing_account_id,
                &l.billing_instrument_id,
                &l.provider_health_observation_id,
                &l.account_health_observation_id,
                &micros(l),
            ],
        )
        .map(|row| row.get(0))
}

/// `admitted_at` in microseconds since the epoch (PostgreSQL's own resolution).
fn micros(l: &ReasoningAdmissionLocator) -> i64 {
    i64::try_from(l.admitted_at.unix_timestamp_nanos() / 1000).expect("in range")
}

fn set_tenant(client: &mut impl postgres::GenericClient, tenant: Uuid) {
    client
        .batch_execute(&format!("SET LOCAL humaux.tenant_id = '{tenant}'"))
        .expect("tenant GUC");
}

fn code(error: &postgres::Error) -> Option<&SqlState> {
    error.code()
}

/// T1 — ADR-0060 D-A: under A's GUC the definer answers A's Profile@version and NULL for B's.
/// Fault: drop the `tenant_id = GUC` predicate AND the FORCE RLS on control.reasoning_profiles (the
/// predicate alone is a second fence: FORCE RLS already scopes the owner) → B's caps returned.
#[test]
fn profile_capabilities_definer_is_tenant_scoped() {
    run("profile_capabilities_definer_is_tenant_scoped", |mut h| {
        let (a, pa, _) = h.routed(3600.0);
        let (_, pb, _) = h.routed(3600.0);
        let mut worker = h.as_role("role_private_worker");
        let mut txn = worker.transaction().expect("txn");
        set_tenant(&mut txn, a.tenant);
        let caps = |txn: &mut postgres::Transaction<'_>, p: &Profile| -> Option<Vec<String>> {
            txn.query_one(
                "SELECT control.reasoning_profile_capabilities($1,$2)",
                &[&p.profile_id, &p.profile_version],
            )
            .expect("definer")
            .get(0)
        };
        assert_eq!(
            caps(&mut txn, &pa),
            Some(BASE_CAPS.map(str::to_owned).to_vec())
        );
        assert_eq!(
            caps(&mut txn, &pb),
            None,
            "another tenant's profile is invisible"
        );
    });
}

/// T2 — §11.2 / ADR-0060 D-A: the locator carries what the PROFILE declares, not the catalog upper
/// bound. Fault: the definer selects `processor_models.capabilities` → TOOL_CALLS appears.
#[test]
fn locator_carries_profile_not_catalog_capabilities() {
    run(
        "locator_carries_profile_not_catalog_capabilities",
        |mut h| {
            let owner = h.owner();
            let profile = private_route::seed_profile(
                &mut h.admin,
                owner,
                &["TEXT", "STRUCTURED_OUTPUT", "TOOL_CALLS"],
                &BASE_CAPS,
                3600.0,
            );
            let binding = private_route::bind(&mut h.admin, owner, DISTILL, &profile);
            let locator = h.admit(owner, binding).expect("admitted");
            assert_eq!(
                locator.capabilities,
                vec![
                    ReasoningCapability::Text,
                    ReasoningCapability::StructuredOutput
                ]
            );
        },
    );
}

/// T3 — ADR-0060 D-I: the reserved row equals the admitted route on every axis, USER-paid, and is
/// the tenant's only row. Fault: drop `credential_ref` from the INSERT column list → 23514.
#[test]
fn private_reservation_row_equals_admitted_route() {
    run("private_reservation_row_equals_admitted_route", |mut h| {
        let (owner, _, binding) = h.routed(3600.0);
        let l = h.admit(owner, binding).expect("admitted");
        let (reserved, _) = h.reserve(owner, &l).expect("reserve");
        let row = h
            .admin
            .query_one(
                "SELECT purpose, provider, model, model_revision, reasoning_domain_id, binding_id, \
                        binding_version, route_policy_id, route_policy_version, profile_id, \
                        profile_version, provider_account_id, provider_endpoint_id, \
                        egress_processor_id, credential_ref, billing_account_id, \
                        billing_instrument_id, provider_health_observation_id, \
                        account_health_observation_id, \
                        (extract(epoch FROM admitted_at) * 1000000)::bigint AS admitted_us, \
                        billing_responsibility, \
                        estimated_cost IS NULL AND actual_cost IS NULL AS no_cost, status \
                 FROM ops.model_call_ledger WHERE model_call_id = $1",
                &[&reserved.model_call_id],
            )
            .expect("row");
        assert_eq!(row.get::<_, String>("purpose"), DISTILL);
        assert_eq!(row.get::<_, String>("provider"), l.processor_id);
        assert_eq!(row.get::<_, String>("model"), l.provider_model_id);
        assert_eq!(
            row.get::<_, Option<String>>("model_revision"),
            l.model_revision
        );
        assert_eq!(
            row.get::<_, Uuid>("reasoning_domain_id"),
            l.reasoning_domain_id.0
        );
        assert_eq!(row.get::<_, Uuid>("binding_id"), l.binding_id.0);
        assert_eq!(row.get::<_, i64>("binding_version"), l.binding_version.0);
        assert_eq!(row.get::<_, Uuid>("route_policy_id"), l.route_policy_id);
        assert_eq!(
            row.get::<_, i64>("route_policy_version"),
            l.route_policy_version
        );
        assert_eq!(row.get::<_, Uuid>("profile_id"), l.profile_id);
        assert_eq!(row.get::<_, i64>("profile_version"), l.profile_version);
        assert_eq!(
            row.get::<_, Uuid>("provider_account_id"),
            l.provider_account_id
        );
        assert_eq!(
            row.get::<_, Uuid>("provider_endpoint_id"),
            l.provider_endpoint_id
        );
        assert_eq!(
            row.get::<_, Uuid>("egress_processor_id"),
            l.egress_processor_id.0
        );
        assert_eq!(row.get::<_, Uuid>("credential_ref"), l.credential_ref);
        assert_eq!(
            row.get::<_, Option<Uuid>>("billing_account_id"),
            l.billing_account_id
        );
        assert_eq!(
            row.get::<_, Option<Uuid>>("billing_instrument_id"),
            l.billing_instrument_id
        );
        assert_eq!(
            row.get::<_, i64>("provider_health_observation_id"),
            l.provider_health_observation_id
        );
        assert_eq!(
            row.get::<_, i64>("account_health_observation_id"),
            l.account_health_observation_id
        );
        assert_eq!(row.get::<_, i64>("admitted_us"), micros(&l));
        assert_eq!(row.get::<_, String>("billing_responsibility"), "USER");
        assert!(row.get::<_, bool>("no_cost"));
        assert_eq!(row.get::<_, String>("status"), "RESERVED");
        let rows: i64 = h
            .admin
            .query_one(
                "SELECT count(*) FROM ops.model_call_ledger WHERE tenant_id = $1",
                &[&owner.tenant],
            )
            .expect("count")
            .get(0);
        assert_eq!(rows, 1, "exactly one row per reservation");
    });
}

/// T4 — ADR-0060 D-I: a private INSERT must equal one CURRENT exact admission. (i) a row naming the
/// tenant's second profile under a binding to the first; (ii) a row under a closed binding version.
/// Fault: remove the private branch from `ops.reasoning_model_call_validate` → both accepted.
#[test]
fn private_insert_refused_unless_it_equals_a_current_admission() {
    run(
        "private_insert_refused_unless_it_equals_a_current_admission",
        |mut h| {
            let (owner, _, binding) = h.routed(3600.0);
            let second =
                private_route::seed_profile(&mut h.admin, owner, &BASE_CAPS, &BASE_CAPS, 3600.0);
            let l = h.admit(owner, binding).expect("admitted");
            let mut worker = h.as_role("role_private_worker");

            let mut txn = worker.transaction().expect("txn");
            set_tenant(&mut txn, owner.tenant);
            let forged = insert_private_row(
                &mut txn,
                &l,
                (second.profile_id, second.profile_version),
                l.binding_version.0,
            )
            .expect_err("(i) another profile than the binding admits");
            assert_eq!(code(&forged), Some(&SqlState::CHECK_VIOLATION), "{forged}");
            drop(txn);

            let next = private_route::rebind(&mut h.admin, owner, DISTILL, binding, &second);
            assert_eq!(next.binding_version, 2);
            let mut txn = worker.transaction().expect("txn");
            set_tenant(&mut txn, owner.tenant);
            let stale = insert_private_row(&mut txn, &l, (l.profile_id, l.profile_version), 1)
                .expect_err("(ii) the closed v1 is no current admission");
            assert_eq!(code(&stale), Some(&SqlState::CHECK_VIOLATION), "{stale}");
        },
    );
}

/// T5 — ADR-0060 D-I: the 14 route columns of a private row are all-or-none, and after 0206 only the
/// all-present form can be inserted, by every role. (i) the CHECK alone (triggers off): one column
/// set → 23514; (ii) owner all-NULL INSERT → 23514; (iii) role_private_worker all-NULL → 23514.
/// Faults: (i) arm C without the all-or-none clause; (ii)/(iii) a `current_user` exemption or an
/// "all NULL ⇒ RETURN NEW" shortcut in the private branch → accepted.
#[test]
fn private_route_columns_all_or_none() {
    run("private_route_columns_all_or_none", |mut h| {
        let (owner, _, binding) = h.routed(3600.0);
        {
            let mut txn = h.admin.transaction().expect("txn");
            txn.batch_execute("SET LOCAL session_replication_role = replica")
                .expect("triggers off: the CHECK alone");
            let partial = txn
                .execute(
                    "INSERT INTO ops.model_call_ledger(tenant_id,provider,purpose,binding_id) \
                     VALUES($1,'route-fixture','PRIVATE_DISTILL_TEXT',$2)",
                    &[&owner.tenant, &binding.binding_id],
                )
                .expect_err("(i) one route column without the rest");
            assert_eq!(
                code(&partial),
                Some(&SqlState::CHECK_VIOLATION),
                "{partial}"
            );
        }
        let all_null = "INSERT INTO ops.model_call_ledger(tenant_id,provider,purpose) VALUES($1,'route-fixture','PRIVATE_DISTILL_TEXT')";
        let owner_insert = h
            .admin
            .execute(all_null, &[&owner.tenant])
            .expect_err("(ii) the owner gets no exemption");
        assert_eq!(
            code(&owner_insert),
            Some(&SqlState::CHECK_VIOLATION),
            "{owner_insert}"
        );
        let mut worker = h.as_role("role_private_worker");
        let mut txn = worker.transaction().expect("txn");
        set_tenant(&mut txn, owner.tenant);
        let worker_insert = txn
            .execute(all_null, &[&owner.tenant])
            .expect_err("(iii) nor does the worker");
        assert_eq!(
            code(&worker_insert),
            Some(&SqlState::CHECK_VIOLATION),
            "{worker_insert}"
        );
    });
}

/// T6 — §11.2.5 / ADR-0060 D-I: health ids are recorded, not compared — a newer attestation between
/// admission and reservation does not refuse the row. Fault: require the recorded ids to equal the
/// resolver's latest → refused.
#[test]
fn newer_attestation_does_not_refuse_reservation() {
    run("newer_attestation_does_not_refuse_reservation", |mut h| {
        let (owner, profile, binding) = h.routed(3600.0);
        let l = h.admit(owner, binding).expect("admitted");
        let (newer, _) = private_route::attest(&mut h.admin, owner, &profile, 3600.0);
        assert_ne!(newer, l.provider_health_observation_id);
        h.reserve(owner, &l)
            .expect("the admitted observation still covered admitted_at");
    });
}

/// T7 — ADR-0060 D-K: processing runs name a whole, existing Profile@version or none.
/// Fault: drop `processing_runs_profile_pair` → the half pair is accepted.
#[test]
fn processing_run_profile_pair_and_fk() {
    run("processing_run_profile_pair_and_fk", |mut h| {
        let (owner, profile, _) = h.routed(3600.0);
        let evidence = private_route::seed_evidence(&mut h.admin, owner);
        let insert = |admin: &mut Client, id: Option<Uuid>, version: Option<i64>| {
            admin.execute(
                "INSERT INTO private.processing_runs(tenant_id,evidence_id,processor_kind,model_id,prompt_version,source_hash,context_snapshot_seq,processor_version,model_provider,model_revision,prompt_hash,parser_version,evidence_payload_sha256,profile_id,profile_version) \
                 VALUES($1,$2,'distill','m','1',$3::bytea,0,'1','p','','h','1',ARRAY[$3::bytea],$4::uuid,$5::bigint)",
                &[&owner.tenant, &evidence, &vec![7_u8; 32], &id, &version],
            )
        };
        let half = insert(&mut h.admin, Some(profile.profile_id), None)
            .expect_err("profile_id without its version");
        assert_eq!(code(&half), Some(&SqlState::CHECK_VIOLATION), "{half}");
        let unknown = insert(&mut h.admin, Some(Uuid::new_v4()), Some(1))
            .expect_err("no such Profile@version");
        assert_eq!(
            code(&unknown),
            Some(&SqlState::FOREIGN_KEY_VIOLATION),
            "{unknown}"
        );
        insert(
            &mut h.admin,
            Some(profile.profile_id),
            Some(profile.profile_version),
        )
        .expect("a real Profile@version");
        insert(&mut h.admin, None, None).expect("a run with no recorded route (pre-0206 shape)");
    });
}

/// T37 — ADR-0060 D-J: the credential-accounts definer answers vendor identity only, across
/// tenants, restores the caller's tenant GUC, and is private-worker-only.
/// Fault: grant EXECUTE to role_gateway → no 42501.
#[test]
fn credential_accounts_definer_returns_vendor_identity_only() {
    run(
        "credential_accounts_definer_returns_vendor_identity_only",
        |mut h| {
            let (a, pa, _) = h.routed(3600.0);
            let (_, pb, _) = h.routed(3600.0);
            let mut worker = h.as_role("role_private_worker");
            let mut txn = worker.transaction().expect("txn");
            set_tenant(&mut txn, a.tenant);
            let rows = txn
                .query(
                    "SELECT * FROM control.reasoning_credential_accounts($1)",
                    &[&vec![pa.credential, pb.credential, Uuid::new_v4()]],
                )
                .expect("definer");
            let columns: Vec<&str> = rows[0].columns().iter().map(|c| c.name()).collect();
            assert_eq!(
                columns,
                [
                    "credential_ref",
                    "processor_id",
                    "external_account_ref_hash"
                ],
                "vendor identity only: no tenant, no secret, no openbao_ref"
            );
            let mut got: Vec<(Uuid, String)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
            got.sort();
            let mut want = vec![
                (pa.credential, pa.processor.clone()),
                (pb.credential, pb.processor.clone()),
            ];
            want.sort();
            assert_eq!(got, want, "both tenants' refs, the unknown one absent");
            let guc: String = txn
                .query_one("SELECT current_setting('humaux.tenant_id')", &[])
                .expect("guc")
                .get(0);
            assert_eq!(
                guc,
                a.tenant.to_string(),
                "the caller's tenant GUC is restored"
            );
            drop(txn);
            let mut gateway = h.as_role("role_gateway");
            let denied = gateway
                .query(
                    "SELECT * FROM control.reasoning_credential_accounts($1)",
                    &[&vec![pa.credential]],
                )
                .expect_err("not the gateway's door");
            assert_eq!(
                code(&denied),
                Some(&SqlState::INSUFFICIENT_PRIVILEGE),
                "{denied}"
            );
        },
    );
}

/// T39 — ADR-0060 D-N / §11.2.5: a private reservation is one transaction with its matching
/// disclosure. (i) a valid row committed alone → 23514 at COMMIT, 0 rows; (ii) a disclosure naming
/// another processor → 23514, both rolled back; (iii) the adapter's one call → 1 row + 1 disclosure,
/// linked, processor = egress id = the locator's. Faults: (i) drop the deferred constraint trigger
/// → commits; (ii)/(iii) keep 0130's CONTRIBUTION-only disclosure purpose list → (iii) refused.
#[test]
fn private_reserve_is_one_transaction_with_matching_disclosure() {
    run(
        "private_reserve_is_one_transaction_with_matching_disclosure",
        |mut h| {
            let (owner, _, binding) = h.routed(3600.0);
            let l = h.admit(owner, binding).expect("admitted");
            let mut worker = h.as_role("role_private_worker");

            let mut txn = worker.transaction().expect("txn");
            set_tenant(&mut txn, owner.tenant);
            insert_private_row(&mut txn, &l, (l.profile_id, l.profile_version), 1)
                .expect("the row itself equals the admission");
            let alone = txn.commit().expect_err("(i) no disclosure at COMMIT");
            assert_eq!(code(&alone), Some(&SqlState::CHECK_VIOLATION), "{alone}");

            let mut txn = worker.transaction().expect("txn");
            set_tenant(&mut txn, owner.tenant);
            let call = insert_private_row(&mut txn, &l, (l.profile_id, l.profile_version), 1)
                .expect("row");
            let mismatch = txn
                .execute(
                    "INSERT INTO ops.data_disclosures(grant_id,tenant_id,processor_id,region,data_class,purpose,payload_sha256,payload_bytes,model_call_id) \
                     VALUES(gen_random_uuid(),$1,$2,$3,'PRIVATE','USER_REASONING',sha256('x'::bytea),1,$4)",
                    &[&owner.tenant, &Uuid::new_v4(), &l.region, &call],
                )
                .expect_err("(ii) another processor than the row's egress id");
            assert_eq!(
                code(&mismatch),
                Some(&SqlState::CHECK_VIOLATION),
                "{mismatch}"
            );
            drop(txn);
            let none: i64 = h
                .admin
                .query_one(
                    "SELECT count(*) FROM ops.model_call_ledger WHERE tenant_id = $1",
                    &[&owner.tenant],
                )
                .expect("count")
                .get(0);
            assert_eq!(none, 0, "(i) and (ii) left no ledger row");

            let (reserved, disclosure) = h.reserve(owner, &l).expect("(iii) one transaction");
            let row = h
                .admin
                .query_one(
                    "SELECT d.model_call_id, d.processor_id, c.egress_processor_id, \
                            (SELECT count(*) FROM ops.model_call_ledger WHERE tenant_id = $2), \
                            (SELECT count(*) FROM ops.data_disclosures WHERE tenant_id = $2) \
                     FROM ops.data_disclosures d JOIN ops.model_call_ledger c \
                       ON c.tenant_id = d.tenant_id AND c.model_call_id = d.model_call_id \
                     WHERE d.disclosure_id = $1",
                    &[&disclosure, &owner.tenant],
                )
                .expect("linked pair");
            assert_eq!(row.get::<_, Uuid>(0), reserved.model_call_id);
            assert_eq!(row.get::<_, Uuid>(1), l.egress_processor_id.0);
            assert_eq!(row.get::<_, Uuid>(2), l.egress_processor_id.0);
            assert_eq!((row.get::<_, i64>(3), row.get::<_, i64>(4)), (1, 1));
        },
    );
}

/// ADR-0060 research amendment 1: `request_extras` is an object without adapter-owned keys, and the
/// live CHECK lists exactly `ADAPTER_OWNED_REQUEST_KEYS`. Fault: drop the key list from the CHECK →
/// `{"tools": []}` accepted.
#[test]
fn request_extras_check_refuses_adapter_owned_keys() {
    run(
        "request_extras_check_refuses_adapter_owned_keys",
        |mut h| {
            let (owner, p, _) = h.routed(3600.0);
            let def: String = h
                .admin
                .query_one(
                    "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
                 WHERE conname = 'reasoning_profiles_request_extras_check'",
                    &[],
                )
                .expect("constraint")
                .get(0);
            let listed = def.split("?| ARRAY[").nth(1).expect("key list");
            let keys: Vec<&str> = listed
                .split(']')
                .next()
                .expect("list end")
                .split('\'')
                .skip(1)
                .step_by(2)
                .collect();
            assert_eq!(keys, ADAPTER_OWNED_REQUEST_KEYS, "{def}");
            let caps = BASE_CAPS.map(str::to_owned).to_vec();
            let mut insert = |extras: &str| {
                h.admin.execute(
                "INSERT INTO control.reasoning_profiles(tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,capabilities,processing_region,request_extras) \
                 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::text::jsonb)",
                &[&owner.tenant, &owner.user, &p.account, &p.endpoint, &p.processor_model, &p.credential, &caps, &p.region, &extras],
            )
            };
            for owned in ["{\"tools\": []}", "{\"max_completion_tokens\": 9}", "[1]"] {
                let refused = insert(owned).expect_err(owned);
                assert_eq!(
                    code(&refused),
                    Some(&SqlState::CHECK_VIOLATION),
                    "{owned}: {refused}"
                );
            }
            insert("{\"enable_thinking\": false}").expect("a vendor field is profile data");
        },
    );
}

/// ADR-0060 ruling E3 (b): a successful call near expiry renews the route (one HEALTHY provider row
/// and one HEALTHY/VALID account row, WORKER_OBSERVED), so the next admission after the old
/// `valid_until` succeeds. Fault: no renewal (the definer returns before its INSERTs) → NOT_READY.
#[test]
fn successful_call_near_expiry_renews_route_health() {
    run(
        "successful_call_near_expiry_renews_route_health",
        |mut h| {
            let (owner, _, binding) = h.routed(3.0);
            let l = h.admit(owner, binding).expect("admitted");
            let (reserved, _) = h.reserve(owner, &l).expect("reserve");
            h.finalize(
                owner.tenant,
                reserved.model_call_id,
                ModelCallOutcome::Succeeded,
            );
            let mut worker = h.as_role("role_private_worker");
            let mut txn = worker.transaction().expect("txn");
            set_tenant(&mut txn, owner.tenant);
            let row = txn
                .query_one(
                    "SELECT * FROM control.observe_reasoning_route_health($1,false,600)",
                    &[&reserved.model_call_id],
                )
                .expect("observe");
            let (provider, account): (Option<i64>, Option<i64>) = (row.get(0), row.get(1));
            txn.commit().expect("commit");
            let (provider, account) = (provider.expect("renewed"), account.expect("renewed"));
            let kinds: i64 = h
            .admin
            .query_one(
                "SELECT (SELECT count(*) FROM ops.reasoning_provider_health_observations \
                          WHERE observation_id = $1 AND source_kind = 'WORKER_OBSERVED' AND verdict = 'HEALTHY') \
                      + (SELECT count(*) FROM ops.reasoning_account_health_observations \
                          WHERE observation_id = $2 AND source_kind = 'WORKER_OBSERVED' \
                            AND account_verdict = 'HEALTHY' AND credential_verdict = 'VALID')",
                &[&provider, &account],
            )
            .expect("kinds")
            .get(0);
            assert_eq!(kinds, 2);
            std::thread::sleep(Duration::from_secs(4));
            let next = h
                .admit(owner, binding)
                .expect("admitted on the worker-observed rows after the seed expired");
            assert_eq!(
                (
                    next.provider_health_observation_id,
                    next.account_health_observation_id
                ),
                (provider, account)
            );
        },
    );
}

/// ADR-0060 ruling E3 (b): a rejected credential appends one INVALID account row and the next
/// admission is refused although the attestation is still valid; a call reported with the wrong
/// outcome is refused (55000). Fault: no negative row → still admitted.
#[test]
fn rejected_credential_denies_the_next_admission() {
    run("rejected_credential_denies_the_next_admission", |mut h| {
        let (owner, _, binding) = h.routed(3600.0);
        let l = h.admit(owner, binding).expect("admitted");
        let (reserved, _) = h.reserve(owner, &l).expect("reserve");
        h.finalize(
            owner.tenant,
            reserved.model_call_id,
            ModelCallOutcome::Failed,
        );
        let mut worker = h.as_role("role_private_worker");
        let mut txn = worker.transaction().expect("txn");
        set_tenant(&mut txn, owner.tenant);
        let wrong = txn
            .query_one(
                "SELECT * FROM control.observe_reasoning_route_health($1,false,600)",
                &[&reserved.model_call_id],
            )
            .expect_err("a FAILED call is no success");
        assert_eq!(
            code(&wrong),
            Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE),
            "{wrong}"
        );
        drop(txn);
        let mut txn = worker.transaction().expect("txn");
        set_tenant(&mut txn, owner.tenant);
        let row = txn
            .query_one(
                "SELECT * FROM control.observe_reasoning_route_health($1,true,600)",
                &[&reserved.model_call_id],
            )
            .expect("observe");
        assert!(
            row.get::<_, Option<i64>>(0).is_none(),
            "no provider row on a key rejection"
        );
        assert!(
            row.get::<_, Option<i64>>(1).is_some(),
            "one INVALID account row"
        );
        txn.commit().expect("commit");
        assert!(
            h.admit(owner, binding).is_none(),
            "a revoked key denies the next admission at once"
        );
    });
}

/// ADR-0060 ruling E3 (b): renewal is rate-limited — two successful calls far from expiry append
/// nothing. Fault: drop the remaining-validity test → two pairs appended.
#[test]
fn renewal_far_from_expiry_appends_nothing() {
    run("renewal_far_from_expiry_appends_nothing", |mut h| {
        let (owner, profile, binding) = h.routed(3600.0);
        let mut worker = h.as_role("role_private_worker");
        for _ in 0..2 {
            let l = h.admit(owner, binding).expect("admitted");
            let (reserved, _) = h.reserve(owner, &l).expect("reserve");
            h.finalize(
                owner.tenant,
                reserved.model_call_id,
                ModelCallOutcome::Succeeded,
            );
            let mut txn = worker.transaction().expect("txn");
            set_tenant(&mut txn, owner.tenant);
            let row = txn
                .query_one(
                    "SELECT * FROM control.observe_reasoning_route_health($1,false,600)",
                    &[&reserved.model_call_id],
                )
                .expect("observe");
            assert_eq!(
                (row.get::<_, Option<i64>>(0), row.get::<_, Option<i64>>(1)),
                (None, None)
            );
            txn.commit().expect("commit");
        }
        let observations: i64 = h
            .admin
            .query_one(
                "SELECT count(*) FROM ops.reasoning_account_health_observations \
                 WHERE tenant_id = $1 AND provider_account_id = $2",
                &[&owner.tenant, &profile.account],
            )
            .expect("count")
            .get(0);
        assert_eq!(observations, 1, "only the seed attestation");
    });
}

/// ADR-0060 ruling E3 (b), 0209: two seats admitted under one account observation; the first
/// succeeds near expiry and renews, the second then meets a revoked key. Its INVALID row is still
/// appended and the next admission is refused. Fault: the 0206 guard (any newer account row skips
/// a rejection) → nothing appended, the next admission rides the renewal.
#[test]
fn rejection_after_a_newer_renewal_still_denies_the_next_admission() {
    run(
        "rejection_after_a_newer_renewal_still_denies_the_next_admission",
        |mut h| {
            let (owner, _, binding) = h.routed(3.0);
            let first = h.admit(owner, binding).expect("seat 1 admitted");
            let second = h.admit(owner, binding).expect("seat 2 admitted");
            assert_eq!(
                first.account_health_observation_id,
                second.account_health_observation_id
            );
            let (renewing, _) = h.reserve(owner, &first).expect("reserve seat 1");
            let (rejected, _) = h.reserve(owner, &second).expect("reserve seat 2");
            h.finalize(
                owner.tenant,
                renewing.model_call_id,
                ModelCallOutcome::Succeeded,
            );
            h.finalize(
                owner.tenant,
                rejected.model_call_id,
                ModelCallOutcome::Failed,
            );
            let mut worker = h.as_role("role_private_worker");
            let mut observe = |call: Uuid, credential_rejected: bool| {
                let mut txn = worker.transaction().expect("txn");
                set_tenant(&mut txn, owner.tenant);
                let row = txn
                    .query_one(
                        "SELECT * FROM control.observe_reasoning_route_health($1,$2,600)",
                        &[&call, &credential_rejected],
                    )
                    .expect("observe");
                txn.commit().expect("commit");
                row.get::<_, Option<i64>>(1)
            };
            assert!(
                observe(renewing.model_call_id, false).is_some(),
                "seat 1 renews"
            );
            assert!(
                observe(rejected.model_call_id, true).is_some(),
                "seat 2's rejection is recorded although a newer renewal exists"
            );
            assert!(
                h.admit(owner, binding).is_none(),
                "the revoked key denies the next admission at once"
            );
        },
    );
}

/// ADR-0060 ruling E3 (c), 0209: a disabled profile whose attestation also expired is reported as
/// no route (the generic "not admitted"), never as stale health. Fault: the 0207 body (no
/// administrative conditions) → one row with health_stale = true.
#[test]
fn disabled_route_with_stale_health_is_not_reported_stale() {
    run(
        "disabled_route_with_stale_health_is_not_reported_stale",
        |mut h| {
            let (owner, profile, binding) = h.routed(1.0);
            std::thread::sleep(Duration::from_millis(1500));
            let mut worker = h.as_role("role_private_worker");
            let mut state = || {
                let mut txn = worker.transaction().expect("txn");
                set_tenant(&mut txn, owner.tenant);
                txn.query_opt(
                    "SELECT health_stale FROM control.reasoning_route_health_state($1,$2)",
                    &[&binding.binding_id, &binding.binding_version],
                )
                .expect("health state")
                .map(|row| row.get::<_, bool>(0))
            };
            assert_eq!(
                state(),
                Some(true),
                "an enabled route with expired health is stale"
            );
            h.admin
                .execute(
                    "UPDATE control.reasoning_profiles SET enabled = false \
                 WHERE tenant_id = $1 AND profile_id = $2 AND profile_version = $3",
                    &[&owner.tenant, &profile.profile_id, &profile.profile_version],
                )
                .expect("disable the profile");
            assert_eq!(state(), None, "a disabled route is not admitted, not stale");
        },
    );
}
