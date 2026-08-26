//! xtask `config-check` — G50-1 / G80-41: `config/features.toml` 契约闸（§50.1）。
//! G50-1/G80-41 登记在 Phase 0 must-pass 列（line 9837），只有 pass/fail 两态，永远不允许
//! not_applicable（§57.1 第2条：not_applicable 仅在被测对象本 Phase 未上线时合法，本闸的被测
//! 对象 `config/features.toml` 始终存在）。

use humaux_contracts::feature_registry::{FeatureActivationKind, parse_features_toml};
use std::fs;
use std::path::Path;

const FEATURES_TOML_PATH: &str = "config/features.toml";
const SPEC_PATH: &str = "docs/architecture/Baseline_2.9.md";
/// §58 workspace tree 里登记 `config/features.toml` 的那一行，逐字命中即视为在树中
/// （G50-1 第0条）。
const SPEC_TREE_MARK: &str =
    "features.toml           # §50.1 / G80-41 唯一 Enterprise Feature Registry";

/// 单条 G50-1 校验失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation(pub String);

/// 解析 §1.14 `mechanism-registry` 围栏块（\`\`\`mechanism-registry ... \`\`\`），
/// 返回 (ch, mechanism) 逐字对（只取前两列，供 G50-1 第3条比对；围栏块本体的其余 6 列/
/// G0–G5 由另一张任务卡的 `mechanism_registry.rs` 负责，不在本 checker 职责内）。
pub fn parse_mechanism_ch_mechanism(spec_md: &str) -> Vec<(u8, String)> {
    let mut out = Vec::new();
    let mut in_block = false;
    for line in spec_md.lines() {
        let trimmed = line.trim();
        if trimmed == "```mechanism-registry" {
            in_block = true;
            continue;
        }
        if in_block && trimmed == "```" {
            break;
        }
        if in_block {
            let cols: Vec<&str> = trimmed.split('|').map(str::trim).collect();
            if cols.len() >= 2
                && let Ok(ch) = cols[0].parse::<u8>()
            {
                out.push((ch, cols[1].to_string()));
            }
        }
    }
    out
}

/// 判断一个 Rust 源文件是否携带真实生效的 `#[ignore]` attribute（G50-1 第5条）。
/// 只看非注释行的 attribute 语句，避免文档注释里提到 `` `#[ignore]` `` 这几个字符
/// 被误判为真的忽略标记。
pub fn source_has_active_ignore_attr(source: &str) -> bool {
    source
        .lines()
        .map(str::trim_start)
        .filter(|line| !line.starts_with("//"))
        .any(|line| line.starts_with("#[ignore]") || line.starts_with("#[ignore("))
}

/// G50-1 第0条：文件必须存在，且必须在 §58 workspace tree 中登记。纯函数，供 [`run`] 与单测共用。
pub fn check_exists_and_in_tree(exists: bool, in_tree: bool) -> Result<(), Violation> {
    if !exists {
        return Err(Violation(format!("missing object: {FEATURES_TOML_PATH}")));
    }
    if !in_tree {
        return Err(Violation(format!(
            "{FEATURES_TOML_PATH} 未在 §58 workspace tree 中登记"
        )));
    }
    Ok(())
}

/// G50-1 全部子条款校验（1/2/3/4/5），纯函数——不接触文件系统，靠参数注入便于注错单测。
///
/// 第 0 条（文件存在 + 在 §58 树中）与第 6 条（无第二份 feature list）由调用方 [`run`] 在
/// 进入本函数前后分别处理，因为它们分别是「先决条件」与「repo 级扫描」，不属于对已解析行的
/// 逐条校验。
pub fn check_parsed(
    features_toml_raw: &str,
    spec_md: &str,
    contract_test_exists: impl Fn(&str) -> bool,
    contract_test_has_ignore: impl Fn(&str) -> bool,
) -> Result<(), Vec<Violation>> {
    let records = match parse_features_toml(features_toml_raw) {
        Ok(r) => r,
        Err(e) => {
            return Err(vec![Violation(format!(
                "G50-1.1/3/4: features.toml 解析失败: {e}"
            ))]);
        }
    };

    let mut v = Vec::new();

    // 2. feature_id 唯一，owner_phase ∈ 0..=17。
    let mut seen = std::collections::BTreeSet::new();
    for r in &records {
        if !seen.insert(r.feature_id.clone()) {
            v.push(Violation(format!(
                "G50-1.2: feature_id 重复: {}",
                r.feature_id
            )));
        }
        if r.owner_phase > 17 {
            v.push(Violation(format!(
                "G50-1.2: owner_phase 越界(0..=17): {} = {}",
                r.feature_id, r.owner_phase
            )));
        }
    }

    // 3. DenominatorGated.mechanism_ref 必须精确命中 §1.14 (ch, mechanism)。
    let registry = parse_mechanism_ch_mechanism(spec_md);
    for r in &records {
        if let FeatureActivationKind::DenominatorGated {
            mechanism_ch,
            mechanism,
        } = &r.activation_kind
        {
            let hit = registry
                .iter()
                .any(|(ch, m)| ch == mechanism_ch && m == mechanism);
            if !hit {
                v.push(Violation(format!(
                    "G50-1.3: {} mechanism_ref 未精确命中 §1.14: ch={} mechanism={:?}",
                    r.feature_id, mechanism_ch, mechanism
                )));
            }
        }
    }

    // 5. contract_test 文件存在，且未 #[ignore]。
    for r in &records {
        if !contract_test_exists(&r.contract_test) {
            v.push(Violation(format!(
                "G50-1.5: {} contract_test 不存在: {}",
                r.feature_id, r.contract_test
            )));
        } else if contract_test_has_ignore(&r.contract_test) {
            v.push(Violation(format!(
                "G50-1.5: {} contract_test 被 #[ignore]: {}",
                r.feature_id, r.contract_test
            )));
        }
    }

    if v.is_empty() { Ok(()) } else { Err(v) }
}

/// G50-1 第6条：全 repo 扫描其它 `*.toml` 是否含 `feature_id` 字段（Phase 17 不维护第二份
/// feature list）。跳过 VCS/构建产物目录与 `config/features.toml` 本身。
fn scan_other_feature_lists(root: &Path) -> Vec<String> {
    let mut hits = Vec::new();
    walk(root, &mut hits);
    hits
}

fn walk(dir: &Path, hits: &mut Vec<String>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if matches!(name, "target" | ".git" | "node_modules") {
                continue;
            }
            walk(&path, hits);
        } else if path.extension().and_then(|e| e.to_str()) == Some("toml") {
            let normalized = path.to_string_lossy().trim_start_matches("./").to_string();
            if normalized == FEATURES_TOML_PATH {
                continue;
            }
            if let Ok(content) = fs::read_to_string(&path)
                && content.contains("feature_id")
            {
                hits.push(normalized);
            }
        }
    }
}

pub fn run(_args: &[String]) -> i32 {
    // 0. exists(config/features.toml) && §58 workspace tree contains exactly that path.
    let exists = Path::new(FEATURES_TOML_PATH).exists();
    // G50-1/G80-41 是 Phase 0 must-pass 闸，永远没有 not_applicable 态（§57.1 第2条：
    // not_applicable 仅在「被测对象本 Phase 未上线」时合法——被测对象是 config/features.toml，
    // 它确实存在；spec 文档读不到是环境/checkout 故障，必须 fail，不能当 0 退出码放过。
    let spec_md = match fs::read_to_string(SPEC_PATH) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("config-check: fail — cannot read {SPEC_PATH}: {e}");
            return 1;
        }
    };
    let in_tree = spec_md.contains(SPEC_TREE_MARK);
    if let Err(v) = check_exists_and_in_tree(exists, in_tree) {
        eprintln!("config-check: fail — G50-1.0: {}", v.0);
        return 1;
    }
    let features_toml_raw = match fs::read_to_string(FEATURES_TOML_PATH) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("config-check: fail — G50-1.0: cannot read {FEATURES_TOML_PATH}: {e}");
            return 1;
        }
    };

    let mut violations = Vec::new();
    if let Err(v) = check_parsed(
        &features_toml_raw,
        &spec_md,
        |p: &str| Path::new(p).exists(),
        |p: &str| {
            fs::read_to_string(p)
                .map(|s| source_has_active_ignore_attr(&s))
                .unwrap_or(false)
        },
    ) {
        violations.extend(v);
    }

    // 6. Phase 17 不维护第二份 feature list。
    for hit in scan_other_feature_lists(Path::new(".")) {
        violations.push(Violation(format!(
            "G50-1.6: 发现第二份 feature list: {hit}"
        )));
    }

    if violations.is_empty() {
        println!("config-check: pass");
        0
    } else {
        for v in &violations {
            eprintln!("config-check: fail — {}", v.0);
        }
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn real_spec_md() -> String {
        let path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/architecture/Baseline_2.9.md");
        fs::read_to_string(path).expect("spec must be readable in test env")
    }

    fn real_features_toml() -> String {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../config/features.toml");
        fs::read_to_string(path).expect("config/features.toml must be readable in test env")
    }

    fn ok_exists(_p: &str) -> bool {
        true
    }
    fn ok_no_ignore(_p: &str) -> bool {
        false
    }

    #[test]
    fn baseline_real_features_toml_passes() {
        let raw = real_features_toml();
        let spec = real_spec_md();
        assert!(check_parsed(&raw, &spec, ok_exists, ok_no_ignore).is_ok());
    }

    /// 注错 1：删 `config/features.toml` → `run()` 的第0条早退必须红。
    #[test]
    fn fault_missing_toml_is_red() {
        assert!(check_exists_and_in_tree(false, true).is_err());
    }

    /// 注错 2：`oidc`（ConfigOnly）填 mechanism_ref → 红。
    #[test]
    fn fault_oidc_mechanism_ref_is_red() {
        let raw = real_features_toml();
        let bad = raw.replacen(
            "entitlement_key = \"feature.oidc\"",
            "mechanism_ref = \"1:X\"\nentitlement_key = \"feature.oidc\"",
            1,
        );
        assert!(bad != raw, "fixture must actually mutate the oidc row");
        let spec = real_spec_md();
        assert!(check_parsed(&bad, &spec, ok_exists, ok_no_ignore).is_err());
    }

    /// 注错 3：`multi_cell` 的 ch 从 67 改成 66 → 未精确命中 §1.14 → 红。
    #[test]
    fn fault_multi_cell_wrong_ch_is_red() {
        let raw = real_features_toml();
        let bad = raw.replacen("67:Cell Routing", "66:Cell Routing", 1);
        assert!(bad != raw);
        let spec = real_spec_md();
        let result = check_parsed(&bad, &spec, ok_exists, ok_no_ignore);
        assert!(result.is_err());
        let violations = result.unwrap_err();
        assert!(violations.iter().any(|v| v.0.contains("G50-1.3")));
    }

    /// 注错 4：新增第三个 enum 变体字符串 `Manual` → 红。
    #[test]
    fn fault_third_variant_manual_is_red() {
        let raw = real_features_toml();
        let bad = raw.replacen(
            "activation_kind = \"ConfigOnly\"",
            "activation_kind = \"Manual\"",
            1,
        );
        assert!(bad != raw);
        let spec = real_spec_md();
        assert!(check_parsed(&bad, &spec, ok_exists, ok_no_ignore).is_err());
    }

    /// 注错 5：删 `scim` 的 contract_test（改指向不存在的文件）→ 红。
    #[test]
    fn fault_scim_missing_contract_test_is_red() {
        let raw = real_features_toml();
        let spec = real_spec_md();
        let exists = |p: &str| p != "crates/contracts/tests/does_not_exist.rs";
        // scim 是唯一 feature_id 精确字面量，定位其行块后把 contract_test 换成不存在路径。
        let scim_start = raw
            .find("feature_id = \"scim\"")
            .expect("scim row must exist");
        let after = &raw[scim_start..];
        let ct_rel = after
            .find("contract_test")
            .expect("scim row must have contract_test");
        let ct_abs = scim_start + ct_rel;
        let line_end = raw[ct_abs..]
            .find('\n')
            .map(|i| ct_abs + i)
            .unwrap_or(raw.len());
        let mut bad = String::new();
        bad.push_str(&raw[..ct_abs]);
        bad.push_str("contract_test = \"crates/contracts/tests/does_not_exist.rs\"");
        bad.push_str(&raw[line_end..]);
        assert!(bad != raw);
        let result = check_parsed(&bad, &spec, exists, ok_no_ignore);
        assert!(result.is_err());
        let violations = result.unwrap_err();
        assert!(
            violations
                .iter()
                .any(|v| v.0.contains("G50-1.5") && v.0.contains("scim"))
        );
    }
}
