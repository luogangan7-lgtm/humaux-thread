// §6.2.3 assertion D (`fail_pool_cross_from`): the four wrappers must not implement
// `From`/`Into` of one another — one leaked cross-conversion would let a request-path pool
// masquerade as any other role's pool.
use humaux_adapters::postgres::{BatchIssuerDbPool, RuntimeDbPool};

fn make_batch_pool() -> BatchIssuerDbPool {
    unimplemented!()
}

fn main() {
    let batch = make_batch_pool();
    let _runtime: RuntimeDbPool = batch.into();
}
