// §6.2.3 assertion C: a `consolidate`-shaped port that takes `&ConsolidationDbPool`
// type-checks when given a `ConsolidationDbPool`. See `pass_runtime_pool.rs` for why the
// check lives in an uncalled function instead of `main`.
use humaux_adapters::postgres::ConsolidationDbPool;

fn consolidate(_pool: &ConsolidationDbPool) {}

fn _typecheck_only(pool: &ConsolidationDbPool) {
    consolidate(pool);
}

fn main() {}
