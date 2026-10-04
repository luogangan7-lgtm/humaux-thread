//! `admin::main` — `humaux-admin` 进程入口（最小必要进程集见 §4.2；探针契约见 §4.4）。
//! Depends-on: crates=[]; services=[]; env=[]; modules=[admin::mechanism, admin::probe, admin::render]
//! Called-by: [process(humaux-admin)]
//! Invariants: []
//! Spec: Baseline §1.14; §4.2; §4.4; ADR-0003; ADR-0061 D-J
//!
//! 子命令：
//! - `render mechanism-registry` —— 读 §1.14 canonical md 的 `mechanism-registry` 围栏并渲染
//!   成人读表格。**这是与 `q` 平级的另一个子命令，不是探针**：不进 §4.4 冻结的 10 条探针目录，
//!   不产出 §4.4 统一契约的 `{value, scanned_n, scope_hash, checked_at, probe_version}` JSON
//!   （§1.14：「render 非探针」，此前 `humaux-admin q mechanism.registry --render` 的写法作废）。
//! - `q <name>` — the §4.4 instant probe catalog (ADR-0061 D-J): 8 probes answer the unified envelope and exit 0;
//!   `public.corroborated`, `public.consensus_ready` and `parse.poison` exit non-zero naming the column or state the
//!   schema does not have (§4.4 line 883). See `probe`.

mod cell_resources;
mod mechanism;
mod ops_status;
mod probe;
mod render;
mod tls_expiry;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let code = match args.get(1).map(String::as_str) {
        Some("mechanism") => match args.get(2).map(String::as_str) {
            Some("status") => mechanism::status(&args[3..]),
            _ => usage(),
        },
        Some("render") => match args.get(2).map(String::as_str) {
            Some("mechanism-registry") => render::mechanism_registry(&args[3..]),
            _ => usage(),
        },
        Some("q") => match args.get(2) {
            Some(name) => probe::run(name, &args[3..]),
            None => usage(),
        },
        _ => usage(),
    };
    std::process::exit(code);
}

fn usage() -> i32 {
    eprintln!(
        "usage: humaux-admin <render mechanism-registry [--deployment ID --cell ID] | mechanism status --deployment ID --cell ID | q <name>>"
    );
    2
}
