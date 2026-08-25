// §6.2.3 assertion D (`fail_begin_batch_with_runtime_pool`): a `begin_batch`-shaped port
// typed for `&BatchIssuerDbPool` must reject a `RuntimeDbPool` — request-path connections
// must never be able to self-issue an ingest ticket (§60.1 / G23-1c).
use humaux_adapters::postgres::{BatchIssuerDbPool, RuntimeDbPool};

fn begin_batch(_pool: &BatchIssuerDbPool) {}

fn make_runtime_pool() -> RuntimeDbPool {
    unimplemented!()
}

fn main() {
    let pool = make_runtime_pool();
    begin_batch(&pool);
}
