use humaux_adapters::contribution_repo::{create_release, promote_claim};
use humaux_adapters::postgres::{PrivateWorkerDbPool, PublicWorkerDbPool};
use humaux_domain::ids::TenantId;
use humaux_domain::public::ContributionRelease;
use serde_json::Value;
use uuid::Uuid;

fn main() {
    let public: PublicWorkerDbPool = unimplemented!();
    let private: PrivateWorkerDbPool = unimplemented!();
    let tenant = TenantId(Uuid::nil());
    let release: ContributionRelease = unimplemented!();
    let content = Value::Null;
    let source_ids: Vec<Uuid> = Vec::new();

    let _ = create_release(&public, tenant, &release);
    let _ = promote_claim(&private, tenant, &content, &source_ids);
}
