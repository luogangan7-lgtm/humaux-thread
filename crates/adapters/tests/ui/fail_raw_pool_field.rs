// §6.2.3 (`fail_raw_pool_field`): `.0` is private to the `postgres` module — code outside
// this crate's `postgres.rs` (this fixture is compiled as a wholly separate crate) must not
// be able to reach the raw `sqlx::PgPool` through the tuple field.
use humaux_adapters::postgres::RuntimeDbPool;

fn make_runtime_pool() -> RuntimeDbPool {
    unimplemented!()
}

fn main() {
    let pool = make_runtime_pool();
    let _inner = pool.0;
}
