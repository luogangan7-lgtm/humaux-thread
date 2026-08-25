// §6.2.3 assertion D (`fail_consolidation_with_private_pool`): a `consolidate`-shaped port
// typed for `&ConsolidationDbPool` must reject a `PrivateWorkerDbPool`.
use humaux_adapters::postgres::{ConsolidationDbPool, PrivateWorkerDbPool};

fn consolidate(_pool: &ConsolidationDbPool) {}

fn make_private_worker_pool() -> PrivateWorkerDbPool {
    unimplemented!()
}

fn main() {
    let pool = make_private_worker_pool();
    consolidate(&pool);
}
