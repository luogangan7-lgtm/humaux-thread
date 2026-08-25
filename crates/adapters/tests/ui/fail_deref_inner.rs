// §6.2.3 (`fail_deref_inner`): no wrapper implements `Deref<Target = PgPool>` — dereferencing
// one must not type-check, regardless of what the target type would be.
use humaux_adapters::postgres::RuntimeDbPool;

fn make_runtime_pool() -> RuntimeDbPool {
    unimplemented!()
}

fn main() {
    let pool = make_runtime_pool();
    let _inner = &*pool;
}
