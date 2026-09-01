//! `humaux-public-worker` explicit bounded Phase 9 runner (§14 / §31).
//!
//! Phase10 resident evolution is not enabled.

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    process::ExitCode,
    time::Duration,
};

use humaux_adapters::{
    postgres::PublicWorkerDbPool, public_projection::PublicProjectionAdapter, public_repo,
};
use humaux_domain::ids::TenantId;
use humaux_infra_cell::{
    CallerId, CellId, HttpIntraCellTransport, IntraCellResource, IntraCellResourceRegistry,
    ResourceEntry, authorize_cell_access,
};
use uuid::Uuid;

fn required(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("missing required configuration: {name}"))
}

fn parse<T: std::str::FromStr>(name: &str) -> Result<T, String> {
    required(name)?
        .parse()
        .map_err(|_| format!("invalid configuration: {name}"))
}

fn usage() -> &'static str {
    "usage: humaux-public-worker --run-once | --run-anonymous-once"
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("humaux-public-worker: {error}");
            ExitCode::from(2)
        }
    }
}

async fn run() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 1 || !matches!(args[0].as_str(), "--run-once" | "--run-anonymous-once") {
        return Err(usage().to_owned());
    }
    let tenant_id = if args[0] == "--run-once" {
        Some(TenantId(parse::<Uuid>("HUMAUX_PUBLIC_WORKER_TENANT_ID")?))
    } else {
        None
    };
    let lease_owner = required("HUMAUX_PUBLIC_WORKER_LEASE_OWNER")?;
    if lease_owner.trim().is_empty() {
        return Err("invalid configuration: HUMAUX_PUBLIC_WORKER_LEASE_OWNER".to_owned());
    }
    let limit = parse::<i64>("HUMAUX_PUBLIC_WORKER_LIMIT")?;
    if limit <= 0 {
        return Err("invalid configuration: HUMAUX_PUBLIC_WORKER_LIMIT".to_owned());
    }
    let cell_id = CellId(parse::<Uuid>("HUMAUX_PUBLIC_WORKER_CELL_ID")?);
    let caller = CallerId(required("HUMAUX_PUBLIC_WORKER_CALLER")?);
    let host = required("HUMAUX_PUBLIC_WORKER_QDRANT_HOST")?;
    let port = parse::<u16>("HUMAUX_PUBLIC_WORKER_QDRANT_PORT")?;
    if port == 0 {
        return Err("invalid configuration: HUMAUX_PUBLIC_WORKER_QDRANT_PORT".to_owned());
    }
    let cidr = required("HUMAUX_PUBLIC_WORKER_QDRANT_CIDR")?
        .parse()
        .map_err(|_| "invalid configuration: HUMAUX_PUBLIC_WORKER_QDRANT_CIDR".to_owned())?;
    let collection = required("HUMAUX_PUBLIC_WORKER_QDRANT_COLLECTION")?;
    let tls = parse::<bool>("HUMAUX_PUBLIC_WORKER_QDRANT_TLS")?;

    let mut entries = BTreeMap::new();
    entries.insert(
        IntraCellResource::QDRANT_REST,
        ResourceEntry::new(
            host,
            port,
            cell_id,
            vec![cidr],
            BTreeSet::from([caller.clone()]),
            tls,
        )
        .map_err(|_| "invalid Qdrant cell resource configuration".to_owned())?,
    );
    let registry = IntraCellResourceRegistry::new(entries, cell_id, caller);
    let permit = authorize_cell_access(
        &registry,
        IntraCellResource::QDRANT_REST,
        Duration::from_secs(60),
    )
    .map_err(|_| "public worker is not authorized for the Qdrant cell".to_owned())?;
    let transport = HttpIntraCellTransport::new(
        registry,
        Duration::from_secs(10),
        humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES,
    )
    .map_err(|_| "could not construct Qdrant cell transport".to_owned())?;
    let projection = PublicProjectionAdapter::new(&transport, &permit, &collection)
        .map_err(|_| "invalid Qdrant collection configuration".to_owned())?;
    let dsn = required("HUMAUX_PUBLIC_WORKER_PG_DSN")?;
    let pool = PublicWorkerDbPool::connect(&dsn)
        .await
        .map_err(|_| "public worker database role connection failed".to_owned())?;

    if let Some(tenant_id) = tenant_id {
        public_repo::drain_outbox(&pool, tenant_id, limit)
            .await
            .map_err(|_| "public outbox drain failed".to_owned())?;
        public_repo::run_once(&pool, tenant_id, &lease_owner, limit, &projection)
            .await
            .map_err(|_| "public run-once failed".to_owned())?;
    } else {
        public_repo::run_anonymous_once(&pool, &lease_owner, limit, &projection)
            .await
            .map_err(|_| "anonymous public run-once failed".to_owned())?;
    }
    Ok(())
}
