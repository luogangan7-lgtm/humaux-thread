//! `adapters::tests::support::contribution_fixture` — Shared isolated PostgreSQL/Gitleaks fixture for authenticated
//!   contribution entry tests.
//! Depends-on: crates=[async-trait, humaux-adapters, humaux-application, humaux-domain, postgres, sha2, tokio, uuid];
//!   services=[PostgreSQL(owner) r=[ops.outbox, staging.contribution_candidates, staging.contribution_releases]
//!   w=[control.contribution_policies, control.credentials, control.memberships, control.private_reasoning_domains,
//!   control.processor_models, control.provider_accounts, control.provider_endpoints,
//!   control.reasoning_credential_bindings, control.reasoning_domain_grants, control.reasoning_profiles,
//!   control.reasoning_route_bindings, control.reasoning_route_candidates, control.reasoning_route_policies,
//!   control.tenants, control.users, ops.data_disclosures, ops.model_call_ledger,
//!   ops.reasoning_account_health_observations, ops.reasoning_provider_health_observations, private.events,
//!   private.evidence_objects, private.memory_evidence, private.memory_records], PostgreSQL(role_gateway),
//!   PostgreSQL(role_private_worker)]; env=[HUMAUX_TEST_GITLEAKS_BIN, HUMAUX_TEST_GITLEAKS_SHA256,
//!   HUMAUX_TEST_GITLEAKS_VERSION, HUMAUX_TEST_PG_DSN]; modules=[adapters::contribution_entry_repo,
//!   adapters::contribution_scan, adapters::postgres, application::consolidate, application::contribute,
//!   domain::authority, domain::identity, domain::ids, domain::public]
//! Called-by: [adapters::tests::contribution_authorization, adapters::tests::contribution_execution_0131, adapters::tests::contribution_execution_disclosure_sources_0131, adapters::tests::contribution_execution_ingress_0131, adapters::tests::contribution_execution_repo_0131, adapters::tests::contribution_pipeline, adapters::tests::contribution_policy_lifecycle_0132, adapters::tests::contribution_reasoner, adapters::tests::contribution_self_principal_authority_0133, adapters::tests::mechanism_observation, adapters::tests::phase9_exact_assessed_storage_binding, adapters::tests::phase9_independence_attestation, adapters::tests::project_continuity_0136, adapters::tests::public_provenance, adapters::tests::public_provenance_revocation_eval, adapters::tests::public_runtime, adapters::tests::public_runtime_qdrant, adapters::tests::public_trust, adapters::tests::support::public_anonymous_seam, private-worker::tests::contribution_execution_runner, private-worker::tests::start_manual_contribution_command_0131]
//! Invariants: [seeds the contribution graph as owner and exercises it through the role_gateway/role_private_worker
//!   pools; fixtures stay in the disposable isolated database for post-failure forensics]
//! Spec: none
//!
//! Fixtures remain in the disposable isolated database for post-failure forensics.
#![allow(dead_code)]

use std::{path::PathBuf, time::Duration};

use async_trait::async_trait;
use humaux_adapters::{
    contribution_entry_repo::{self, ContributionEntryRepo},
    contribution_scan::{ContributionScanner, ContributionScannerConfig},
    postgres::{PrivateWorkerDbPool, RuntimeDbPool},
};
use humaux_application::{
    consolidate::{
        ContentSha256, LogicalReasoningCallId, PrivateReasoningDomainId, PrivateReasoningError,
        PrivateReasoningPort, PrivateReasoningPurpose, PrivateReasoningResult, ProviderTraceRef,
        ReasoningRouteBindingId, ReasoningRouteBindingVersion, SealedPrivateReasoningRequest,
    },
    contribute::{self, ConfirmContribution, ContributionCandidateId, PrepareContribution},
};
use humaux_domain::{
    authority::MemoryId,
    identity::{AuthorizationScope, BoundedSet, PrincipalId},
    ids::{TenantId, UserId, WorkspaceId},
    public::ReleaseSource,
};
use postgres::{Client, NoTls};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub struct ContributionFixture {
    pub rt: tokio::runtime::Runtime,
    pub admin: Client,
    pub private: PrivateWorkerDbPool,
    pub gateway: RuntimeDbPool,
    pub auth: AuthorizationScope,
    pub domain: Uuid,
    pub memory: Uuid,
    pub binding: Uuid,
    pub binding_version: i64,
    pub egress_processor: Uuid,
    pub coverage_probe_call_id: Uuid,
    pub assessment_call_id: Uuid,
}

impl ContributionFixture {
    #[allow(clippy::too_many_lines)] // One real-PG setup must atomically expose the full authenticated release path to integration tests.
    pub fn new() -> Self {
        let dsn = std::env::var("HUMAUX_TEST_PG_DSN").expect("explicit isolated PG fixture");
        // dep: PostgreSQL(owner) — test opens a direct PG connection for setup/verification
        let mut admin = Client::connect(&dsn, NoTls).expect("isolated PG");
        let tenant: Uuid = admin
            .query_one(
                "INSERT INTO control.tenants(name,state) VALUES($1,'ACTIVE') RETURNING tenant_id",
                &[&format!("contribution-fixture-{}", Uuid::new_v4())],
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
        let credential: Uuid = admin
            .query_one(
                "INSERT INTO control.credentials(tenant_id,purpose,openbao_ref) VALUES($1,'USER_REASONING','fixture-ref-only') RETURNING credential_id",
                &[&tenant],
            )
            .expect("credential")
            .get(0);
        let domain: Uuid = admin
            .query_one(
                "INSERT INTO control.private_reasoning_domains(tenant_id,name,owner_user_id,status) \
                 VALUES($1,'offline-fixture',$2,'ACTIVE') RETURNING reasoning_domain_id",
                &[&tenant, &user],
            )
            .expect("reasoning domain")
            .get(0);
        let processor = "offline-fixture";
        let provider_model = "offline-fixture";
        let processor_model: Uuid = admin.query_one(
            "INSERT INTO control.processor_models(processor_id,provider_model_id,model_revision,capabilities,status,catalog_observed_at) \
             VALUES($1,$2,NULL,ARRAY['TEXT','STRUCTURED_OUTPUT'],'ACTIVE',clock_timestamp()) \
             ON CONFLICT (processor_id,provider_model_id,model_revision) DO UPDATE \
             SET processor_id=EXCLUDED.processor_id \
             RETURNING processor_model_id",
            &[&processor, &provider_model],
        ).expect("processor model").get(0);
        let account_hash = vec![7_u8; 32];
        let provider_account: Uuid = admin.query_one(
            "INSERT INTO control.provider_accounts(tenant_id,owner_user_id,processor_id,external_account_ref_hash) VALUES($1,$2,$3,$4) RETURNING provider_account_id",
            &[&tenant, &user, &processor, &account_hash],
        ).expect("provider account").get(0);
        admin.execute(
            "INSERT INTO control.reasoning_credential_bindings(credential_ref,tenant_id,owner_user_id,provider_account_id,processor_id) VALUES($1,$2,$3,$4,$5)",
            &[&credential, &tenant, &user, &provider_account, &processor],
        ).expect("credential binding");
        let endpoint_ref = "https://reasoning.invalid/v1/chat/completions";
        let region = "test-region";
        let service_tier = "fixture";
        let egress_processor = Uuid::new_v4();
        let provider_endpoint: Uuid = admin.query_one(
            "INSERT INTO control.provider_endpoints(tenant_id,provider_account_id,region,service_tier,endpoint_ref,egress_processor_id) VALUES($1,$2,$3,$4,$5,$6) RETURNING endpoint_id",
            &[&tenant, &provider_account, &region, &service_tier, &endpoint_ref, &egress_processor],
        ).expect("provider endpoint").get(0);
        let profile: Uuid = admin.query_one(
            "INSERT INTO control.reasoning_profiles(tenant_id,owner_user_id,provider_account_id,endpoint_id,processor_model_id,credential_ref,capabilities,processing_region) VALUES($1,$2,$3,$4,$5,$6,ARRAY['TEXT','STRUCTURED_OUTPUT'],$7) RETURNING profile_id",
            &[&tenant, &user, &provider_account, &provider_endpoint, &processor_model, &credential, &region],
        ).expect("reasoning profile").get(0);
        let route_policy: Uuid = admin.query_one(
            "INSERT INTO control.reasoning_route_policies(tenant_id,policy_owner_user_id,purpose) VALUES($1,$2,'CONTRIBUTION_DEIDENTIFY') RETURNING route_policy_id",
            &[&tenant, &user],
        ).expect("route policy").get(0);
        admin.execute(
            "INSERT INTO control.reasoning_route_candidates(tenant_id,route_policy_id,route_policy_version,profile_id,profile_version,priority) VALUES($1,$2,1,$3,1,0)",
            &[&tenant, &route_policy, &profile],
        ).expect("route candidate");
        admin.execute(
            "UPDATE control.reasoning_route_policies SET lifecycle_state='SHADOW' WHERE route_policy_id=$1 AND policy_version=1",
            &[&route_policy],
        ).expect("shadow route policy");
        admin.execute(
            "UPDATE control.reasoning_route_policies SET lifecycle_state='SERVING' WHERE route_policy_id=$1 AND policy_version=1",
            &[&route_policy],
        ).expect("serving route policy");
        let binding: Uuid = admin.query_one(
            "INSERT INTO control.reasoning_route_bindings(tenant_id,reasoning_domain_id,purpose,route_policy_id,route_policy_version) VALUES($1,$2,'CONTRIBUTION_DEIDENTIFY',$3,1) RETURNING binding_id",
            &[&tenant, &domain, &route_policy],
        ).expect("route binding").get(0);
        let provider_health: i64 = admin.query_one(
            "INSERT INTO ops.reasoning_provider_health_observations(tenant_id,processor_id,processor_model_id,provider_model_id,model_revision,provider_endpoint_id,endpoint_ref,region,service_tier,source_kind,verdict,observed_at,valid_until) VALUES($1,$2,$3,$4,NULL,$5,$6,$7,$8,'TEST','HEALTHY',clock_timestamp()-interval '1 second',clock_timestamp()+interval '1 hour') RETURNING observation_id",
            &[&tenant, &processor, &processor_model, &provider_model, &provider_endpoint, &endpoint_ref, &region, &service_tier],
        ).expect("provider health").get(0);
        let account_health: i64 = admin.query_one(
            "INSERT INTO ops.reasoning_account_health_observations(tenant_id,provider_account_id,credential_ref,source_kind,account_verdict,credential_verdict,observed_at,valid_until) VALUES($1,$2,$3,'TEST','HEALTHY','VALID',clock_timestamp()-interval '1 second',clock_timestamp()+interval '1 hour') RETURNING observation_id",
            &[&tenant, &provider_account, &credential],
        ).expect("account health").get(0);
        let principal = Uuid::new_v4();
        admin
            .execute(
                "INSERT INTO control.reasoning_domain_grants(reasoning_domain_id,principal_id,purposes,expires_at) \
                 VALUES($1,$2,ARRAY['USER_REASONING'],clock_timestamp()+interval '1 hour')",
                &[&domain, &principal],
            )
            .expect("reasoning grant");
        admin
            .execute(
                "INSERT INTO control.contribution_policies(tenant_id,allow_public_contribution,rights_basis) \
                 VALUES($1,true,'explicit fixture redistribution rights')",
                &[&tenant],
            )
            .expect("policy");
        let evidence: Uuid = admin
            .query_one(
                "INSERT INTO private.evidence_objects(tenant_id,evidence_kind,payload_sha256,data_class,origin_class,visibility_class,reasoning_domain_id) \
                 VALUES($1,'EVENT',sha256(convert_to('{}','UTF8')),'INTERNAL','DirectUserInput','TENANT_SHARED',$2) RETURNING evidence_id",
                &[&tenant, &domain],
            )
            .expect("evidence")
            .get(0);
        admin
            .execute(
                "INSERT INTO private.events(event_id,event_kind,payload) VALUES($1,'USER_MESSAGE','{}')",
                &[&evidence],
            )
            .expect("event");
        let mut txn = admin.transaction().expect("memory transaction");
        let memory: Uuid = txn
            .query_one(
                "INSERT INTO private.memory_records(tenant_id,memory_type,content,visibility_class,authority_class,confidence,status,asserted_at) \
                 VALUES($1,'FACT','{\"text\":\"private fixture detail\"}','TENANT_SHARED','PrivateKnowledge',1,'active',now()) RETURNING memory_id",
                &[&tenant],
            )
            .expect("memory")
            .get(0);
        txn.execute(
            "INSERT INTO private.memory_evidence(memory_id,evidence_id,role) VALUES($1,$2,'SUPPORTING')",
            &[&memory, &evidence],
        )
        .expect("memory evidence");
        txn.commit().expect("memory commit");
        let coverage_probe_call_id = Uuid::new_v4();
        let assessment_call_id = Uuid::new_v4();
        admin
            .query_one(
                "SELECT set_config('humaux.tenant_id',$1,false)",
                &[&tenant.to_string()],
            )
            .expect("tenant GUC");
        admin.execute(
            "INSERT INTO ops.model_call_ledger(model_call_id,request_id,tenant_id,purpose,provider,model,model_revision,call_kind,intent_sha256,reasoning_domain_id,binding_id,binding_version,route_policy_id,route_policy_version,profile_id,profile_version,provider_account_id,provider_endpoint_id,egress_processor_id,credential_ref,provider_health_observation_id,account_health_observation_id,billing_responsibility,admitted_at) VALUES($1,$1,$2,'CONTRIBUTION_DEIDENTIFY',$3,$4,NULL,'TYPED_ASSESSMENT',$5,$6,$7,1,$8,1,$9,1,$10,$11,$12,$13,$14,$15,'USER',clock_timestamp())",
            &[&assessment_call_id, &tenant, &processor, &provider_model, &vec![9_u8; 32], &domain, &binding, &route_policy, &profile, &provider_account, &provider_endpoint, &egress_processor, &credential, &provider_health, &account_health],
        ).expect("offline reasoning reservation");
        admin.execute(
            "INSERT INTO ops.data_disclosures(grant_id,tenant_id,processor_id,region,data_class,purpose,payload_sha256,payload_bytes,model_call_id,finalized_at,outcome) VALUES($1,$2,$3,$4,'PRIVATE','USER_REASONING',$5,0,$6,clock_timestamp(),'SUCCESS')",
            &[&Uuid::new_v4(), &tenant, &egress_processor, &region, &vec![0_u8; 32], &assessment_call_id],
        ).expect("offline reasoning disclosure");
        admin.execute(
            "UPDATE ops.model_call_ledger SET status='SUCCEEDED' WHERE tenant_id=$1 AND model_call_id=$2 AND status='RESERVED'",
            &[&tenant, &assessment_call_id],
        ).expect("offline reasoning finalization");
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let role = |name: &str| {
            format!(
                "{dsn}{}options=-c%20role%3D{name}",
                if dsn.contains('?') { '&' } else { '?' }
            )
        };
        let private = rt
            // dep: PostgreSQL(role_private_worker) — test opens a direct PG connection for setup/verification
            .block_on(PrivateWorkerDbPool::connect(&role("role_private_worker")))
            .expect("private role pool");
        let gateway = rt
            // dep: PostgreSQL(role_gateway) — test opens a direct PG connection for setup/verification
            .block_on(RuntimeDbPool::connect(&role("role_gateway")))
            .expect("gateway role pool");
        let auth = AuthorizationScope::new(
            TenantId(tenant),
            PrincipalId(user),
            Some(UserId(user)),
            BoundedSet::<WorkspaceId>::new([]).expect("empty workspace scope"),
        );
        Self {
            rt,
            admin,
            private,
            gateway,
            auth,
            domain,
            memory,
            binding,
            binding_version: 1,
            egress_processor,
            coverage_probe_call_id,
            assessment_call_id,
        }
    }

    pub fn request(&self) -> PrepareContribution {
        PrepareContribution {
            authorization: self.auth.clone(),
            requested_sources: vec![ReleaseSource::Memory(MemoryId(self.memory))],
            reasoning_domain_id: PrivateReasoningDomainId(self.domain),
            binding_id: ReasoningRouteBindingId(self.binding),
            binding_version: ReasoningRouteBindingVersion(self.binding_version),
            coverage_probe_call_id: LogicalReasoningCallId(self.coverage_probe_call_id),
            assessment_call_id: LogicalReasoningCallId(self.assessment_call_id),
        }
    }

    pub fn scanner(&self) -> ContributionScanner {
        ContributionScanner::new(ContributionScannerConfig {
            executable: PathBuf::from(
                std::env::var("HUMAUX_TEST_GITLEAKS_BIN").expect("pinned Gitleaks fixture"),
            ),
            expected_executable_sha256: std::env::var("HUMAUX_TEST_GITLEAKS_SHA256")
                .expect("pinned executable digest"),
            expected_version: std::env::var("HUMAUX_TEST_GITLEAKS_VERSION")
                .expect("pinned Gitleaks version"),
            timeout: Duration::from_secs(5),
            max_payload_bytes: 65_536,
            finding_exit_code: 1,
        })
        .expect("scanner config")
    }

    #[allow(deprecated)] // This fixture intentionally retains the legacy-path acceptance helper.
    pub fn prepare(&self) -> ContributionCandidateId {
        self.rt
            .block_on(contribute::prepare(
                self.request(),
                &OfflineReasoner(b"General knowledge without identifying details.".to_vec()),
                &self.scanner(),
                &ContributionEntryRepo::new(&self.private),
            ))
            .expect("authenticated prepare")
    }

    pub fn confirm(&self, id: ContributionCandidateId) -> ConfirmContribution {
        let preview = self
            .rt
            .block_on(contribution_entry_repo::preview(
                &self.private,
                &self.auth,
                id,
            ))
            .expect("private preview");
        let confirmation_id = self
            .rt
            .block_on(contribution_entry_repo::record_confirmation(
                &self.gateway,
                &self.auth,
                id,
                preview.payload_sha256,
                preview.policy_version,
                Duration::from_secs(300),
            ))
            .expect("authenticated confirmation");
        ConfirmContribution {
            authorization: self.auth.clone(),
            candidate_id: id,
            candidate_payload_sha256: preview.payload_sha256,
            confirmation_id,
        }
    }

    pub fn finalize_release(&self) -> Uuid {
        let candidate = self.prepare();
        let confirmation = self.confirm(candidate);
        self.rt
            .block_on(contribute::finalize(
                confirmation,
                &ContributionEntryRepo::new(&self.private),
            ))
            .expect("authenticated finalize")
            .0
    }

    pub fn counts(&mut self) -> (i64, i64, i64) {
        let row = self
            .admin
            .query_one(
                "SELECT \
                 (SELECT count(*) FROM staging.contribution_candidates WHERE tenant_id=$1), \
                 (SELECT count(*) FROM staging.contribution_releases WHERE tenant_id=$1), \
                 (SELECT count(*) FROM ops.outbox WHERE tenant_id=$1)",
                &[&self.auth.tenant_id().0],
            )
            .expect("counts");
        (row.get(0), row.get(1), row.get(2))
    }
}

pub struct OfflineReasoner(pub Vec<u8>);

#[async_trait]
impl PrivateReasoningPort for OfflineReasoner {
    async fn infer(
        &self,
        request: SealedPrivateReasoningRequest,
    ) -> Result<PrivateReasoningResult, PrivateReasoningError> {
        assert_eq!(
            request.purpose,
            PrivateReasoningPurpose::ContributionDeidentify
        );
        let model_call_id = request
            .contribution_attempt
            .expect("caller-carried fixture attempt")
            .logical_call_id
            .0;
        Ok(PrivateReasoningResult {
            output_bytes: self.0.clone(),
            output_sha256: ContentSha256(Sha256::digest(&self.0).into()),
            provider_trace: ProviderTraceRef(model_call_id.to_string()),
            model_call_id,
            binding_id: request.binding_id,
            binding_version: request.binding_version,
        })
    }
}
