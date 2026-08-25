// §6.2.3 assertion C: a `private_worker_step`-shaped port that takes `&PrivateWorkerDbPool`
// type-checks when given a `PrivateWorkerDbPool`. See `pass_runtime_pool.rs` for why the
// check lives in an uncalled function instead of `main`.
use humaux_adapters::postgres::PrivateWorkerDbPool;

fn private_worker_step(_pool: &PrivateWorkerDbPool) {}

fn _typecheck_only(pool: &PrivateWorkerDbPool) {
    private_worker_step(pool);
}

fn main() {}
