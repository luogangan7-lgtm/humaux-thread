//! `xtask::main` — cargo xtask entry point: dispatches every gate subcommand.
//! Depends-on: crates=[]; services=[]; env=[]; modules=[xtask::architecture_check, xtask::benchset_declaration, xtask::card_version, xtask::config_check, xtask::confirm_sweep, xtask::contract_impact, xtask::dep_map, xtask::direction_table, xtask::dod_check, xtask::e2e_onboard, xtask::e2e_seed, xtask::gate_registry, xtask::gate_truth, xtask::mechanism_registry, xtask::member, xtask::metrics_registry, xtask::migrate, xtask::migration_rehearsal, xtask::phase, xtask::projection_serve, xtask::r4_fault_manifest, xtask::rls_check, xtask::serial_lane, xtask::soak, xtask::threshold_shape]
//! Called-by: [process(cargo-xtask)]
//! Invariants: [unknown subcommand exits 2 with the usage string; every subcommand module is registered in both mod list and dispatch match]
//! Spec: Baseline §78; ADR-0051
//!
//! `cargo xtask` — 工程门禁统一入口（§78：复杂 gate 不散落 bash，走 xtask）。
//!
//! 子命令与闸的对应：architecture-check(G80-1/2/3/22…) · mechanism-registry(G80-10)
//! · migration-rehearsal(G80-37) · metrics-registry(G80-6) · contract-impact(G80-42)
//! · config-check(G80-41) · rls-check(§48.2) · gate-truth(§79.2, ADR-0050 D-C) · dep-map(ADR-0051)。判据正文以家章为唯一真源。

mod architecture_check;
mod benchset_declaration;
mod card_version;
mod config_check;
mod confirm_sweep;
mod contract_impact;
mod dep_map;
mod direction_table;
mod dod_check;
mod e2e_onboard;
mod e2e_seed;
mod gate_registry;
mod gate_truth;
mod mechanism_registry;
mod member;
mod metrics_registry;
mod migrate;
mod migration_rehearsal;
mod phase;
mod projection_serve;
mod r4_fault_manifest;
mod rls_check;
mod serial_lane;
mod soak;
mod switch_visible;
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
        Some("r4-fault-manifest") => r4_fault_manifest::run(&args[2..]),
        Some("direction-table") => direction_table::run(&args[2..]),
        Some("card-version") => card_version::run(&args[2..]),
        Some("migrate") => migrate::run(&args[2..]),
        Some("e2e-seed") => e2e_seed::run(&args[2..]),
        Some("e2e-onboard") => e2e_onboard::run(&args[2..]),
        Some("member") => member::run(&args[2..]),
        Some("projection-serve") => projection_serve::run(&args[2..]),
        Some("sweep-confirm-tokens") => confirm_sweep::run(&args[2..]),
        Some("serial-lane") => serial_lane::run(&args[2..]),
        Some("soak") => soak::run(&args[2..]),
        Some("gate-truth") => gate_truth::run(&args[2..]),
        Some("dep-map") => dep_map::run(&args[2..]),
        _ => {
            eprintln!(
                "usage: cargo xtask <architecture-check|mechanism-registry|migration-rehearsal|metrics-registry|contract-impact|config-check|rls-check|threshold-shape|benchset-declaration|gate-registry|dod-check|phase-check|r4-fault-manifest|direction-table|card-version|migrate|e2e-seed|e2e-onboard|member|projection-serve|sweep-confirm-tokens|serial-lane|soak|gate-truth|dep-map>"
            );
            2
        }
    };
    std::process::exit(code);
}
