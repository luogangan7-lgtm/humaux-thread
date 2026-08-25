// §6.2.3 assertion C: a `remember`-shaped port that takes `&RuntimeDbPool` type-checks when
// given a `RuntimeDbPool`. `trybuild::TestCases::pass` also *runs* the compiled binary, so
// the check lives in an uncalled function — Rust still type-checks a function body it never
// executes, and `main` staying empty means there is nothing to construct or run at runtime.
use humaux_adapters::postgres::RuntimeDbPool;

fn remember(_pool: &RuntimeDbPool) {}

fn _typecheck_only(pool: &RuntimeDbPool) {
    remember(pool);
}

fn main() {}
