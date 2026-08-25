// §6.2.3 assertion D (`fail_remember_with_batch_pool`): a `remember`-shaped port typed for
// `&RuntimeDbPool` must reject a `BatchIssuerDbPool` — the closed set is per-role, not
// "any pool will do".
use humaux_adapters::postgres::{BatchIssuerDbPool, RuntimeDbPool};

fn remember(_pool: &RuntimeDbPool) {}

fn make_batch_pool() -> BatchIssuerDbPool {
    unimplemented!()
}

fn main() {
    let pool = make_batch_pool();
    remember(&pool);
}
