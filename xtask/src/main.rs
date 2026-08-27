//! `cargo xtask` — 工程门禁统一入口（§78：复杂 gate 不散落 bash，走 xtask）。
//!
//! 子命令与闸的对应：architecture-check(G80-1/2/3/22…) · mechanism-registry(G80-10)
//! · migration-rehearsal(G80-37) · metrics-registry(G80-6) · contract-impact(G80-42)
//! · config-check(G80-41) · rls-check(§48.2)。判据正文以家章为唯一真源。

mod architecture_check;
mod benchset_declaration;
mod card_version;
mod config_check;
mod contract_impact;
mod direction_table;
mod dod_check;
mod gate_registry;
mod mechanism_registry;
mod metrics_registry;
mod migrate;
mod migration_rehearsal;
mod phase;
mod rls_check;
mod threshold_shape;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let code = match args.get(1).map(String::as_str) {
        Some("architecture-check") => architecture_check::run(&args[2..]),
        Some("mechanism-registry") => mechanism_registry::run(&args[2..]),
        Some("migration-rehearsal") => migration_rehearsal::run(&args[2..]),
        Some("metrics-registry") => metrics_registry::run(&args[2..]),
        Some("contract-impact") => contract_impact::run(&args[2..]),
        Some("config-check") => config_check::run(&args[2..]),
        Some("rls-check") => rls_check::run(&args[2..]),
        Some("threshold-shape") => threshold_shape::run(&args[2..]),
        Some("benchset-declaration") => benchset_declaration::run(&args[2..]),
        Some("gate-registry") => gate_registry::run(&args[2..]),
        Some("dod-check") => dod_check::run(&args[2..]),
        Some("phase-check") => phase::run(&args[2..]),
        Some("direction-table") => direction_table::run(&args[2..]),
        Some("card-version") => card_version::run(&args[2..]),
        Some("migrate") => migrate::run(&args[2..]),
        _ => {
            eprintln!(
                "usage: cargo xtask <architecture-check|mechanism-registry|migration-rehearsal|metrics-registry|contract-impact|config-check|rls-check|threshold-shape|benchset-declaration|gate-registry|dod-check|phase-check|direction-table|card-version>"
            );
            2
        }
    };
    std::process::exit(code);
}
