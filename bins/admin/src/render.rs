//! `humaux-admin render mechanism-registry` —— §1.14 canonical md 围栏的人读渲染。
//!
//! **不是探针**（见 main.rs 模块文档）：不产出 §4.4 的 `{value, scanned_n, scope_hash,
//! checked_at, probe_version}` JSON 契约，只把 static spec 的 `mechanism-registry` 围栏
//! 渲染成表格。渲染结果禁止回写进 canonical md（§1.14）。
//!
//! `--deployment` / `--cell` 是 §1.14.1 描述的目标接口（static spec JOIN 目标 Observation），
//! 但 `ops.mechanism_observations` 本轮未部署（见 xtask `mechanism-registry` G3/G4/G5），
//! 因此本轮渲染只输出 static spec 一侧，忽略这两个参数。

/// spec 唯一真源，相对本 crate manifest 目录解析（§1.14 冻结：不得另建镜像文件）。
const SPEC_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docs/architecture/Baseline_2.8.md"
);

const FENCE_OPEN: &str = "```mechanism-registry";
const FENCE_CLOSE: &str = "```";

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
pub fn mechanism_registry(_args: &[String]) -> i32 {
    let text = match std::fs::read_to_string(SPEC_PATH) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("render mechanism-registry: fail — cannot read spec at {SPEC_PATH}: {e}");
            return 1;
        }
    };
    let Some(body) = extract_fence(&text) else {
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

/// 找到第一个（唯一一个，见 xtask G0）`mechanism-registry` 围栏并返回其正文行。
fn extract_fence(text: &str) -> Option<Vec<&str>> {
    let mut lines = text.lines();
    while let Some(line) = lines.next() {
        if line.trim() == FENCE_OPEN {
            let mut body = Vec::new();
            for inner in lines.by_ref() {
                if inner.trim() == FENCE_CLOSE {
                    break;
                }
                body.push(inner);
            }
            return Some(body);
        }
    }
    None
}
