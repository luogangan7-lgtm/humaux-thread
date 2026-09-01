use humaux_adapters::postgres::PublicWorkerDbPool;

fn accepts_public_worker_pool(_: &PublicWorkerDbPool) {}

fn main() {
    let _ = accepts_public_worker_pool;
}
