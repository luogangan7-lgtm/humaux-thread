//! `humaux-admin render mechanism-registry` —— §1.14 canonical md 围栏的人读渲染。
//!
//! **不是探针**（见 main.rs 模块文档）：不产出 §4.4 的 `{value, scanned_n, scope_hash,
//! checked_at, probe_version}` JSON 契约，只把 static spec 的 `mechanism-registry` 围栏
//! 渲染成表格。渲染结果禁止回写进 canonical md（§1.14）。
//!
//! With an explicit target, join actual runtime observations through role_admin.
//! Without a target, render only the static fence and do not assert runtime status.

use humaux_contracts::mechanism_registry::{extract_fences, parse_registry};

/// spec 唯一真源，相对本 crate manifest 目录解析（§1.14 冻结：不得另建镜像文件）。
const SPEC_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docs/architecture/Baseline_2.9.md"
);

/// §1.14 固定列序。
const COLUMNS: [&str; 8] = [
    "ch",
    "mechanism",
    "activation_kind",
    "min_denominator",
    "probe",
    "bootstrap_value",
    "bootstrap_measured_at",
    "note",
];

/// 渲染 canonical md 的 `mechanism-registry` 围栏为人读表格；返回进程退出码。
pub fn mechanism_registry(args: &[String]) -> i32 {
    if !args.is_empty() {
        return match crate::mechanism::read(args) {
            Ok((specs, observations)) => {
                println!("mechanism_id | runtime_status | reason | value | scanned_n");
                for spec in &specs {
                    let derived = observations.status(spec);
                    let observation = observations.latest.get(&spec.id());
                    println!(
                        "{} | {} | {} | {:?} | {:?}",
                        spec.id(),
                        derived.status.map_or("-", |s| s.as_str()),
                        derived.reason,
                        observation.map(|o| o.value),
                        observation.and_then(|o| o.scanned_n)
                    );
                }
                i32::from(observations.cannot_establish(&specs))
            }
            Err(error) => {
                eprintln!("render mechanism-registry: cannot_establish — {error}");
                1
            }
        };
    }
    let text = match std::fs::read_to_string(SPEC_PATH) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("render mechanism-registry: fail — cannot read spec at {SPEC_PATH}: {e}");
            return 1;
        }
    };
    if let Err(error) = parse_registry(&text) {
        eprintln!("render mechanism-registry: invalid spec — {error}");
        return 1;
    }
    let fences = extract_fences(&text);
    let Some(body) = fences.first() else {
        eprintln!(
            "render mechanism-registry: fail — missing object: mechanism-registry fence in {SPEC_PATH}"
        );
        return 1;
    };
    println!("{}", COLUMNS.join(" | "));
    for line in body.iter().map(|l| l.trim()).filter(|l| !l.is_empty()) {
        println!("{line}");
    }
    eprintln!(
        "# note: render is not a probe — outside the §4.4 probe catalog, no {{value, scanned_n, scope_hash, checked_at, probe_version}} envelope (§1.14)."
    );
    0
}
