//! `contracts::feature_registry` — `FeatureActivationKind` 闭集 + `config/features.toml` 解析
//! （§50.1）。
//!
//! 唯一静态真源：`config/features.toml`。本模块只做纯文本 → 类型解析与结构校验，不接触文件
//! 系统、也不比对 §1.14 mechanism-registry 围栏块本体——那一步需要读取
//! `docs/architecture/Baseline_2.9.md`，属于 fs 编排，留给 `xtask config-check`（G50-1/G80-41）。

use serde::Deserialize;
use std::fmt;

/// Feature 激活方式冻结闭集（§50.1 字面 Rust 定义）。
///
/// 禁止新增第三个变体：注错「新增 `Manual` 变体字符串」必须在 [`parse_features_toml`] 阶段就被
/// `activation_kind` 的显式字符串匹配拒绝，见该函数文档。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeatureActivationKind {
    /// contract/e2e PASS + entitlement/policy allows 即可激活（§50.1 语义段）。
    ConfigOnly,
    /// 额外要求 `mechanism_ref` 精确命中 §1.14 且目标 deployment/cell 的
    /// `MechanismObservation.derived_status == ACTIVE`（§50.1 语义段）。
    DenominatorGated {
        /// §1.14 mechanism-registry 围栏块第 1 列（章号）。
        mechanism_ch: u8,
        /// §1.14 mechanism-registry 围栏块第 2 列（mechanism 名，逐字）。
        /// 冻结 Rust 片段（§50.1 line 8860-8868）字面写的是 `&'static str`；这里改成 `String`
        /// 是必要偏离，不是誊抄误差——该值是运行时从 `config/features.toml` 解析出来的，
        /// 没有 leak/硬编码就拿不到 `&'static str`。无 G50-1 gate 检查字面字段类型。
        mechanism: String,
    },
}

/// 单条 feature registry 记录（`config/features.toml` 一行，§50.1 字段表）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureRecord {
    /// Feature 唯一标识（G50-1 第 2 条：全表内必须唯一）。
    pub feature_id: String,
    /// 所属 Phase（G50-1 第 2 条：owner_phase ∈ 0..=17）。
    pub owner_phase: u8,
    /// 激活方式（闭集，见 [`FeatureActivationKind`]）。
    pub activation_kind: FeatureActivationKind,
    /// entitlement/policy 判定用键名。
    pub entitlement_key: String,
    /// 该 feature 的 contract test 文件相对路径（G50-1 第 5 条：必须存在、未 `#[ignore]`）。
    pub contract_test: String,
}

/// `config/features.toml` 解析/校验失败（§50: 垃圾值 fail-loud，禁止 `Default` 兜底掩盖坏行）。
///
/// 这是解析期的局部错误类型，不是 §52 的运行期 `ErrorCode`/`DegradeCode` 二元分类
/// （CLAUDE.md「两个错误枚举互斥」约束的是运行期请求分类，不含 build-time 配置解析）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureRegistryError(pub String);

impl fmt::Display for FeatureRegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for FeatureRegistryError {}

#[derive(Deserialize)]
struct RawFile {
    feature: Vec<RawFeatureRow>,
}

#[derive(Deserialize)]
struct RawFeatureRow {
    feature_id: String,
    owner_phase: u8,
    activation_kind: String,
    #[serde(default)]
    mechanism_ref: Option<String>,
    entitlement_key: String,
    contract_test: String,
}

/// 解析 `config/features.toml` 原始文本为 [`FeatureRecord`] 列表。
///
/// fail-loud（§50 / §50.1 G50-1）：
/// - `activation_kind` 不在 `{"ConfigOnly", "DenominatorGated"}` 闭集内 → `Err`（G50-1 第 1 条）。
/// - `DenominatorGated` 行缺 `mechanism_ref`，或其格式不是 `"<ch>:<mechanism>"` → `Err`
///   （G50-1 第 3 条；`<ch>` 段必须是合法 `u8`）。
/// - `ConfigOnly` 行携带非空 `mechanism_ref` → `Err`（G50-1 第 4 条）。
///
/// 与 §1.14 mechanism-registry 的逐字命中比对（G50-1 第 3 条剩余部分）不在本函数职责内，
/// 需要调用方（`xtask config-check`）拿解析出的 `mechanism_ch`/`mechanism` 去比对围栏块。
pub fn parse_features_toml(raw: &str) -> Result<Vec<FeatureRecord>, FeatureRegistryError> {
    let file: RawFile = toml::from_str(raw).map_err(|e| FeatureRegistryError(e.to_string()))?;
    file.feature.into_iter().map(row_to_record).collect()
}

fn row_to_record(row: RawFeatureRow) -> Result<FeatureRecord, FeatureRegistryError> {
    let activation_kind = match row.activation_kind.as_str() {
        "ConfigOnly" => {
            if row.mechanism_ref.is_some() {
                return Err(FeatureRegistryError(format!(
                    "{}: ConfigOnly 行 mechanism_ref 必须为空（§50.1 G50-1 第4条）",
                    row.feature_id
                )));
            }
            FeatureActivationKind::ConfigOnly
        }
        "DenominatorGated" => {
            let reference = row.mechanism_ref.ok_or_else(|| {
                FeatureRegistryError(format!(
                    "{}: DenominatorGated 行缺 mechanism_ref（§50.1 G50-1 第3条）",
                    row.feature_id
                ))
            })?;
            let (ch_str, mechanism) = reference.split_once(':').ok_or_else(|| {
                FeatureRegistryError(format!(
                    "{}: mechanism_ref 格式必须是 \"<ch>:<mechanism>\"，得到 {reference:?}",
                    row.feature_id
                ))
            })?;
            let mechanism_ch: u8 = ch_str.trim().parse().map_err(|_| {
                FeatureRegistryError(format!(
                    "{}: mechanism_ref 的 ch 段不是合法 u8: {ch_str:?}",
                    row.feature_id
                ))
            })?;
            FeatureActivationKind::DenominatorGated {
                mechanism_ch,
                mechanism: mechanism.trim().to_string(),
            }
        }
        other => {
            return Err(FeatureRegistryError(format!(
                "{}: activation_kind={other:?} 不在闭集 {{ConfigOnly, DenominatorGated}} 内（§50.1）",
                row.feature_id
            )));
        }
    };
    Ok(FeatureRecord {
        feature_id: row.feature_id,
        owner_phase: row.owner_phase,
        activation_kind,
        entitlement_key: row.entitlement_key,
        contract_test: row.contract_test,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = r#"
[[feature]]
feature_id = "oidc"
owner_phase = 17
activation_kind = "ConfigOnly"
entitlement_key = "feature.oidc"
contract_test = "crates/contracts/tests/feature_registry_contract.rs"

[[feature]]
feature_id = "multi_cell"
owner_phase = 17
activation_kind = "DenominatorGated"
mechanism_ref = "67:Cell Routing"
entitlement_key = "feature.multi_cell"
contract_test = "crates/contracts/tests/feature_registry_contract.rs"
"#;

    #[test]
    fn parses_config_only_and_denominator_gated() {
        let records = parse_features_toml(BASE).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0].activation_kind,
            FeatureActivationKind::ConfigOnly
        );
        assert_eq!(
            records[1].activation_kind,
            FeatureActivationKind::DenominatorGated {
                mechanism_ch: 67,
                mechanism: "Cell Routing".to_string(),
            }
        );
    }

    /// 注错 1：`oidc`（ConfigOnly）填 mechanism_ref → 必须红（G50-1 第4条）。
    #[test]
    fn fault_config_only_with_mechanism_ref_is_err() {
        let bad = BASE.replacen(
            "entitlement_key = \"feature.oidc\"",
            "mechanism_ref = \"1:X\"\nentitlement_key = \"feature.oidc\"",
            1,
        );
        assert!(parse_features_toml(&bad).is_err());
    }

    /// 注错 2：新增第三个 enum 变体字符串 `Manual` → 必须红（G50-1 第1条）。
    #[test]
    fn fault_third_variant_manual_is_err() {
        let bad = BASE.replacen("ConfigOnly", "Manual", 1);
        assert!(parse_features_toml(&bad).is_err());
    }

    #[test]
    fn fault_denominator_gated_missing_mechanism_ref_is_err() {
        let bad = BASE.replacen("mechanism_ref = \"67:Cell Routing\"\n", "", 1);
        assert!(parse_features_toml(&bad).is_err());
    }
}
