//! Contract test for `config/features.toml`（§50.1 每条 feature 的 `contract_test` 字段指向此
//! 文件；G50-1 第 5 条要求该文件存在且没有 `#[ignore]`）。
//!
//! 校验对象是仓库里真实的 `config/features.toml`：它必须能被
//! `humaux_contracts::feature_registry::parse_features_toml` 解析成功，且 Phase 17 初始闭集
//! 恰为 §50.1 正文列出的 9 条 feature_id。

use humaux_contracts::feature_registry::parse_features_toml;
use std::fs;
use std::path::PathBuf;

fn features_toml_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config/features.toml")
}

#[test]
fn features_toml_parses_and_matches_phase_17_closed_set() {
    let raw = fs::read_to_string(features_toml_path()).expect("config/features.toml must exist");
    let records = parse_features_toml(&raw).expect("config/features.toml must parse (§50.1)");

    let mut ids: Vec<&str> = records
        .iter()
        .filter(|r| r.owner_phase == 17)
        .map(|r| r.feature_id.as_str())
        .collect();
    ids.sort_unstable();

    let mut expected = vec![
        "byoc",
        "cmk",
        "customer_retrieval_endpoint",
        "dedicated_enterprise_cell",
        "multi_cell",
        "oidc",
        "passkey",
        "saml",
        "scim",
    ];
    expected.sort_unstable();

    assert_eq!(ids, expected, "§50.1 Phase 17 闭集必须恰为文档列出的 9 条");
}
