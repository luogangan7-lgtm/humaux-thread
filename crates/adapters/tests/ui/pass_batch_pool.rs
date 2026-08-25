// §6.2.3 assertion C: a `begin_batch`-shaped port that takes `&BatchIssuerDbPool`
// type-checks when given a `BatchIssuerDbPool`. See `pass_runtime_pool.rs` for why the
// check lives in an uncalled function instead of `main`.
use humaux_adapters::postgres::BatchIssuerDbPool;

fn begin_batch(_pool: &BatchIssuerDbPool) {}

fn _typecheck_only(pool: &BatchIssuerDbPool) {
    begin_batch(pool);
}

fn main() {}
