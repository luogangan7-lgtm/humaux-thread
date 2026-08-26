//! `contracts::config_registry` — Typed Config Registry + effective config fingerprint (§50).
//!
//! §50: 所有配置集中 typed registry，字段 name/type/default/scope/secret?/reloadability/owner
//! module；垃圾值 fail-loud；生产运行必须记录 effective config fingerprint，而不是只看 `.env`。

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;

/// The one sanctioned raw `std::env::var` read site outside process bootstrap (`bins/*`) and
/// CI-gate tooling (`xtask/`) — module doc/§50.1: "humaux-contracts owns the one legitimate
/// config-read point". Every other crate that needs a single named environment variable's
/// value (rather than the full typed [`resolve_effective_config`] flow, which expects the raw
/// key/value map already assembled) calls this function instead of naming `std::env::var`
/// itself; `xtask`'s §78 boundary lint (`architecture_check::env_var_scan`) is a hard CI gate
/// that fails on any other file containing that call. Returns `None` for both "unset" and
/// "present but empty" alike — callers that must tell those two apart read
/// `std::env::var(name)`'s own `Result` directly instead of going through this helper, but no
/// caller in this workspace has needed that distinction yet.
pub fn read_env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// 单条 typed config registry entry（§50 字段表逐字对应）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    /// 配置键名。
    pub name: String,
    /// 声明类型名（如 "bool"/"u64"/"string"），供上层做进一步类型化解析。
    pub type_name: String,
    /// 缺省值；`None` 表示该条无默认值，运行时必须提供。
    pub default: Option<String>,
    /// 生效范围（如 "process"/"tenant"/"deployment"），自由文本——§50 未冻结取值闭集。
    pub scope: String,
    /// 是否敏感值；敏感值不得出现在日志/fingerprint 原文中（由调用方在记录前脱敏）。
    pub secret: bool,
    /// 是否可热加载（如 "static"/"hot"），自由文本——§50 未冻结取值闭集。
    pub reloadability: String,
    /// 拥有该配置项语义的模块名，用于追责与变更评审路由。
    pub owner_module: String,
}

/// Effective config 解析失败（§50: 垃圾值 fail-loud，禁止用 `Default` 掩盖缺口）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigResolveError {
    /// 出错的配置键名。
    pub entry_name: String,
    /// 失败原因（人读文本，供日志/CI 输出）。
    pub reason: String,
}

impl fmt::Display for ConfigResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "config entry {:?}: {}", self.entry_name, self.reason)
    }
}

impl std::error::Error for ConfigResolveError {}

/// 由 registry 声明与运行时已解析的原始键值对构造 effective config。
///
/// fail-loud（§50）：任一声明项既无运行时值又无 `default`，或 `raw` 携带 registry 未声明的键
/// （垃圾值），一律返回 `Err`——不得静默丢弃、不得用 `Default::default()` 兜底吞掉缺口。
pub fn resolve_effective_config(
    registry: &[ConfigEntry],
    raw: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, ConfigResolveError> {
    let mut effective = BTreeMap::new();
    for entry in registry {
        let value = raw.get(&entry.name).or(entry.default.as_ref());
        match value {
            Some(v) => {
                effective.insert(entry.name.clone(), v.clone());
            }
            None => {
                return Err(ConfigResolveError {
                    entry_name: entry.name.clone(),
                    reason: "missing runtime value and no default".to_string(),
                });
            }
        }
    }
    for key in raw.keys() {
        if !registry.iter().any(|entry| &entry.name == key) {
            return Err(ConfigResolveError {
                entry_name: key.clone(),
                reason: "value present for key not declared in registry".to_string(),
            });
        }
    }
    Ok(effective)
}

/// Effective config fingerprint（§50: 生产运行必须记录 effective config fingerprint）。
///
/// 规范序列化：按 key 升序拼接 `"key=value\n"`（`effective` 已是 `BTreeMap`，迭代天然有序），
/// 避免 hash 结果随 map 内部实现的迭代顺序漂移；secret 项的值应由调用方在传入前脱敏/占位。
pub fn effective_config_fingerprint(effective: &BTreeMap<String, String>) -> String {
    let mut canonical = String::new();
    for (key, value) in effective {
        canonical.push_str(key);
        canonical.push('=');
        canonical.push_str(value);
        canonical.push('\n');
    }
    let digest = Sha256::digest(canonical.as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str, default: Option<&str>) -> ConfigEntry {
        ConfigEntry {
            name: name.to_string(),
            type_name: "string".to_string(),
            default: default.map(str::to_string),
            scope: "process".to_string(),
            secret: false,
            reloadability: "static".to_string(),
            owner_module: "test".to_string(),
        }
    }

    #[test]
    fn missing_value_and_no_default_is_fail_loud_err() {
        let registry = vec![entry("a", None)];
        let raw = BTreeMap::new();
        let err = resolve_effective_config(&registry, &raw).unwrap_err();
        assert_eq!(err.entry_name, "a");
    }

    #[test]
    fn undeclared_key_is_fail_loud_err() {
        let registry = vec![entry("a", Some("1"))];
        let mut raw = BTreeMap::new();
        raw.insert("garbage".to_string(), "x".to_string());
        let err = resolve_effective_config(&registry, &raw).unwrap_err();
        assert_eq!(err.entry_name, "garbage");
    }

    #[test]
    fn fingerprint_is_deterministic_and_order_independent() {
        let mut a = BTreeMap::new();
        a.insert("b".to_string(), "2".to_string());
        a.insert("a".to_string(), "1".to_string());
        let mut b = BTreeMap::new();
        b.insert("a".to_string(), "1".to_string());
        b.insert("b".to_string(), "2".to_string());
        assert_eq!(
            effective_config_fingerprint(&a),
            effective_config_fingerprint(&b)
        );
    }

    #[test]
    fn fingerprint_changes_when_value_changes() {
        let mut a = BTreeMap::new();
        a.insert("a".to_string(), "1".to_string());
        let mut b = BTreeMap::new();
        b.insert("a".to_string(), "2".to_string());
        assert_ne!(
            effective_config_fingerprint(&a),
            effective_config_fingerprint(&b)
        );
    }
}
