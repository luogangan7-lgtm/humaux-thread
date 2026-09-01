//! Read-only operational mechanism status. No default target and no bootstrap fallback.

use humaux_adapters::{
    mechanism_observation::{RuntimeObservations, read_target},
    postgres::AdminDbPool,
};
use humaux_contracts::mechanism_registry::{MechanismSpec, parse_registry, parse_target_args};

pub const SPEC_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docs/architecture/Baseline_2.9.md"
);

pub fn read(args: &[String]) -> Result<(Vec<MechanismSpec>, RuntimeObservations), String> {
    let target = parse_target_args(args)?;
    let text = std::fs::read_to_string(SPEC_PATH)
        .map_err(|_| "cannot read canonical mechanism registry")?;
    let specs = parse_registry(&text)?;
    let dsn = std::env::var("HUMAUX_ADMIN_PG_DSN").map_err(|_| "missing HUMAUX_ADMIN_PG_DSN")?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|_| "cannot start database runtime")?;
    let observations = runtime.block_on(async {
        let pool = AdminDbPool::connect(&dsn)
            .await
            .map_err(|_| "cannot connect as read-only role_admin")?;
        read_target(&pool, &target)
            .await
            .map_err(|_| "cannot read runtime observation evidence")
    })?;
    Ok((specs, observations))
}

pub fn status(args: &[String]) -> i32 {
    match read(args) {
        Ok((specs, observations)) => {
            println!("{}", observations.render_json(&specs));
            i32::from(observations.cannot_establish(&specs))
        }
        Err(error) => {
            eprintln!("mechanism status: cannot_establish — {error}");
            1
        }
    }
}
