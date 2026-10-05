//! `adapters::tests::support::private_route` — seeds one complete USER_REASONING route graph (tenant, owner, domain,
//!   catalog model, account, credential binding, endpoint, Profile@version, PINNED policy, candidate, binding, health)
//!   for the private purposes, and admits it through the real resolver as role_private_worker.
//! Depends-on: crates=[humaux-adapters, humaux-application, postgres, tokio, uuid];
//!   services=[PostgreSQL(owner) w=[control.credentials, control.memberships, control.private_reasoning_domains,
//!   control.processor_models, control.provider_accounts, control.provider_endpoints,
//!   control.reasoning_credential_bindings, control.reasoning_profiles, control.reasoning_route_bindings,
//!   control.reasoning_route_candidates, control.reasoning_route_policies, control.tenants, control.users,
//!   ops.reasoning_account_health_observations, ops.reasoning_provider_health_observations,
//!   private.evidence_objects]]; env=[]; modules=[adapters::distill_repo, adapters::postgres,
//!   adapters::reasoning_route_admission, application::consolidate]
//! Called-by: [adapters::tests::model_call_ledger, adapters::tests::reasoning_route_runtime]
//! Invariants: [every call mints its own processor id, so catalog rows never collide across tests or runs; routes
//!   are owner-seeded exactly as the 0128 triggers require (READ COMMITTED, DRAFT -> SHADOW -> SERVING); health rows
//!   carry source_kind TEST]
//! Spec: Baseline §11.2.3; §11.2.5; ADR-0060
#![allow(dead_code)]

use humaux_adapters::distill_repo;
use humaux_adapters::postgres::PrivateWorkerDbPool;
use humaux_adapters::reasoning_route_admission::{
    ReasoningAdmissionLocator, resolve_user_reasoning_admission,
};
use humaux_application::consolidate::{
    PrivateReasoningDomainId, PrivateReasoningPurpose, ReasoningRouteBindingId,
    ReasoningRouteBindingVersion,
};
use postgres::Client;
use uuid::Uuid;

/// libpq `options=-c role=X` on the owner DSN (the repo-wide test convention).
pub fn dsn_as_role(admin_dsn: &str, role: &str) -> String {
    let sep = if admin_dsn.contains('?') { '&' } else { '?' };
    format!("{admin_dsn}{sep}options=-c%20role%3D{role}")
}

/// One tenant with its ACTIVE owner and that owner's ACTIVE reasoning domain.
#[derive(Debug, Clone, Copy)]
pub struct Owner {
    pub tenant: Uuid,
    pub user: Uuid,
    pub domain: Uuid,
}

/// One Profile@1 with everything it points at.
#[derive(Debug, Clone)]
pub struct Profile {
    pub profile_id: Uuid,
    pub profile_version: i64,
    pub processor: String,
    pub model: String,
    pub processor_model: Uuid,
    pub account: Uuid,
    pub credential: Uuid,
    pub endpoint: Uuid,
    pub endpoint_ref: String,
    pub egress: Uuid,
    pub region: String,
    pub service_tier: String,
}

/// One current binding of `(domain, purpose)`.
#[derive(Debug, Clone, Copy)]
pub struct Binding {
    pub binding_id: Uuid,
    pub binding_version: i64,
    pub policy: Uuid,
}

pub fn seed_owner(admin: &mut Client, label: &str) -> Owner {
    let tenant: Uuid = admin
        .query_one(
            "INSERT INTO control.tenants(name,state) VALUES($1,'ACTIVE') RETURNING tenant_id",
            &[&format!(
                "e2e-fixture {label} throwaway tenant {}",
                Uuid::new_v4()
            )],
        )
        .expect("tenant")
        .get(0);
    let user: Uuid = admin
        .query_one(
            "INSERT INTO control.users(state) VALUES('ACTIVE') RETURNING user_id",
            &[],
        )
        .expect("user")
        .get(0);
    admin
        .execute(
            "INSERT INTO control.memberships(tenant_id,user_id,role,state) VALUES($1,$2,'owner','ACTIVE')",
            &[&tenant, &user],
        )
        .expect("membership");
    let domain: Uuid = admin
        .query_one(
            "INSERT INTO control.private_reasoning_domains(tenant_id,name,owner_user_id,status) \
             VALUES($1,'route-fixture',$2,'ACTIVE') RETURNING reasoning_domain_id",
            &[&tenant, &user],
        )
        .expect("reasoning domain")
        .get(0);
    Owner {
        tenant,
        user,
        domain,
    }
}

/// A Profile@1 of `owner` on a fresh catalog row declaring `catalog_caps`; the profile declares
/// `profile_caps` (must be a subset, 0128). Healthy for `health_secs` from now.
pub fn seed_profile(
    admin: &mut Client,
    owner: Owner,
    catalog_caps: &[&str],
    profile_caps: &[&str],
    health_secs: f64,
) -> Profile {
    let processor = format!("route-fixture-{}", Uuid::new_v4());
    let model = "route-fixture-model".to_owned();
    let endpoint_ref = "https://route-fixture.invalid/v1/chat/completions".to_owned();
    let (region, service_tier) = ("test-region".to_owned(), "fixture".to_owned());
    let egress = Uuid::new_v4();
    let catalog: Vec<String> = catalog_caps.iter().map(|c| (*c).to_owned()).collect();
    let caps: Vec<String> = profile_caps.iter().map(|c| (*c).to_owned()).collect();
    let processor_model: Uuid = admin
        .query_one(
            "INSERT INTO control.processor_models(processor_id,provider_model_id,model_revision,capabilities,status,catalog_observed_at) \
             VALUES($1,$2,NULL,$3,'ACTIVE',clock_timestamp()) RETURNING processor_model_id",
            &[&processor, &model, &catalog],
        )
        .expect("processor model")
        .get(0);
    let credential: Uuid = admin
        .query_one(
            "INSERT INTO control.credentials(tenant_id,purpose,openbao_ref) VALUES($1,'USER_REASONING','route-fixture-ref') RETURNING credential_id",
            &[&owner.tenant],
        )
        .expect("credential")
        .get(0);
    let account_hash = Uuid::new_v4().as_bytes().repeat(2);
    let account: Uuid = admin
        .query_one(
            "INSERT INTO control.provider_accounts(tenant_id,owner_user_id,processor_id,external_account_ref_hash) VALUES($1,$2,$3,$4) RETURNING provider_account_id",
            &[&owner.tenant, &owner.user, &processor, &account_hash],
        )
        .expect("provider account")
        .get(0);
    admin
        .execute(
            "INSERT INTO control.reasoning_credential_bindings(credential_ref,tenant_id,owner_user_id,provider_account_id,processor_id) VALUES($1,$2,$3,$4,$5)",
            &[&credential, &owner.tenant, &owner.user, &account, &processor],
        )
        .expect("credential binding");
    let endpoint: Uuid = admin
        .query_one(
            "INSERT INTO control.provider_endpoints(tenant_id,provider_account_id,region,service_tier,endpoint_ref,egress_processor_id) VALUES($1,$2,$3,$4,$5,$6) RETURNING endpoint_id",
            &[&owner.tenant, &account, &region, &service_tier, &endpoint_ref, &egress],
        )
        .expect("provider endpoint")
        .get(0);
    let profile_id: Uuid = admin
        .query_one(
            "INSERT INTO control.reasoning_profiles(tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,capabilities,processing_region) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING profile_id",
            &[&owner.tenant, &owner.user, &account, &endpoint, &processor_model, &credential, &caps, &region],
        )
        .expect("reasoning profile")
        .get(0);
    let profile = Profile {
        profile_id,
        profile_version: 1,
        processor,
        model,
        processor_model,
        account,
        credential,
        endpoint,
        endpoint_ref,
        egress,
        region,
        service_tier,
    };
    attest(admin, owner, &profile, health_secs);
    profile
}

/// One HEALTHY provider + HEALTHY/VALID account observation of `profile`, valid `secs` from now.
/// Returns `(provider_observation_id, account_observation_id)`.
pub fn attest(admin: &mut Client, owner: Owner, profile: &Profile, secs: f64) -> (i64, i64) {
    let provider: i64 = admin
        .query_one(
            "INSERT INTO ops.reasoning_provider_health_observations(tenant_id,processor_id,processor_model_id,provider_model_id,model_revision,provider_endpoint_id,endpoint_ref,region,service_tier,source_kind,verdict,observed_at,valid_until) \
             VALUES($1,$2,$3,$4,NULL,$5,$6,$7,$8,'TEST','HEALTHY',clock_timestamp()-interval '1 second',clock_timestamp()+make_interval(secs=>$9)) RETURNING observation_id",
            &[&owner.tenant, &profile.processor, &profile.processor_model, &profile.model, &profile.endpoint, &profile.endpoint_ref, &profile.region, &profile.service_tier, &secs],
        )
        .expect("provider health")
        .get(0);
    let account: i64 = admin
        .query_one(
            "INSERT INTO ops.reasoning_account_health_observations(tenant_id,provider_account_id,credential_ref,source_kind,account_verdict,credential_verdict,observed_at,valid_until) \
             VALUES($1,$2,$3,'TEST','HEALTHY','VALID',clock_timestamp()-interval '1 second',clock_timestamp()+make_interval(secs=>$4)) RETURNING observation_id",
            &[&owner.tenant, &profile.account, &profile.credential, &secs],
        )
        .expect("account health")
        .get(0);
    (provider, account)
}

/// A SERVING PINNED policy with exactly one candidate (`profile`) for `purpose`.
fn seed_policy(admin: &mut Client, owner: Owner, purpose: &str, profile: &Profile) -> Uuid {
    let policy: Uuid = admin
        .query_one(
            "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose) VALUES($1,$2,$3) RETURNING route_policy_id",
            &[&owner.tenant, &owner.user, &purpose],
        )
        .expect("route policy")
        .get(0);
    admin
        .execute(
            "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,1,$3,$4,0)",
            &[&owner.tenant, &policy, &profile.profile_id, &profile.profile_version],
        )
        .expect("route candidate");
    for state in ["SHADOW", "SERVING"] {
        admin
            .execute(
                "UPDATE control.reasoning_route_policies SET lifecycle_state=$2 WHERE route_policy_id=$1 AND policy_version=1",
                &[&policy, &state],
            )
            .expect("promote route policy");
    }
    policy
}

/// The first binding of `(owner.domain, purpose)` to `profile`.
pub fn bind(admin: &mut Client, owner: Owner, purpose: &str, profile: &Profile) -> Binding {
    let policy = seed_policy(admin, owner, purpose, profile);
    let binding_id: Uuid = admin
        .query_one(
            "INSERT INTO control.reasoning_route_bindings(tenant_id,reasoning_domain_id,purpose,route_policy_id,route_policy_version) VALUES($1,$2,$3,$4,1) RETURNING binding_id",
            &[&owner.tenant, &owner.domain, &purpose, &policy],
        )
        .expect("route binding")
        .get(0);
    Binding {
        binding_id,
        binding_version: 1,
        policy,
    }
}

/// Closes `current` and inserts its successor version over a new policy naming `profile`.
pub fn rebind(
    admin: &mut Client,
    owner: Owner,
    purpose: &str,
    current: Binding,
    profile: &Profile,
) -> Binding {
    let policy = seed_policy(admin, owner, purpose, profile);
    admin
        .execute(
            "UPDATE control.reasoning_route_bindings SET effective_to=clock_timestamp() WHERE binding_id=$1 AND binding_version=$2",
            &[&current.binding_id, &current.binding_version],
        )
        .expect("close binding");
    let version = current.binding_version + 1;
    admin
        .execute(
            "INSERT INTO control.reasoning_route_bindings(binding_id,binding_version,tenant_id,reasoning_domain_id,purpose,route_policy_id,route_policy_version,effective_from) \
             VALUES($1,$2,$3,$4,$5,$6,1,clock_timestamp())",
            &[&current.binding_id, &version, &owner.tenant, &owner.domain, &purpose, &policy],
        )
        .expect("successor binding");
    Binding {
        binding_id: current.binding_id,
        binding_version: version,
        policy,
    }
}

/// One Evidence row of the owner's domain (a disclosure source).
pub fn seed_evidence(admin: &mut Client, owner: Owner) -> Uuid {
    admin
        .query_one(
            "INSERT INTO private.evidence_objects(tenant_id,evidence_kind,payload_sha256,data_class,origin_class,visibility_class,reasoning_domain_id) \
             VALUES($1,'EVENT',sha256(convert_to(gen_random_uuid()::text,'UTF8')),'PRIVATE','DirectUserInput','TENANT_SHARED',$2) RETURNING evidence_id",
            &[&owner.tenant, &owner.domain],
        )
        .expect("evidence")
        .get(0)
}

/// The resolver's answer for `binding`, as role_private_worker under the tenant's GUC.
pub fn admit(
    rt: &tokio::runtime::Runtime,
    pool: &PrivateWorkerDbPool,
    owner: Owner,
    binding: Binding,
    purpose: PrivateReasoningPurpose,
) -> Option<ReasoningAdmissionLocator> {
    rt.block_on(async {
        let mut txn = distill_repo::begin_read_context(pool, owner.tenant)
            .await
            .expect("private worker read txn");
        let locator = resolve_user_reasoning_admission(
            &mut txn,
            ReasoningRouteBindingId(binding.binding_id),
            ReasoningRouteBindingVersion(binding.binding_version),
            PrivateReasoningDomainId(owner.domain),
            purpose,
        )
        .await
        .expect("resolver reachable");
        txn.rollback().await.expect("rollback read txn");
        locator
    })
}
