//! `contracts::retrieval_config` — Typed retrieval-profile configuration (§50 / §55.6).
//! Depends-on: crates=[]; services=[]; env=[]; modules=[contracts::config_registry]
//! Called-by: [gateway::bootstrap, retrieval::request, tests]
//! Invariants: []
//! Spec: Baseline §50; §55.1; §55.6
//!
//! This is the sole bootstrap boundary for retrieval depth. A request caller receives an
//! already-resolved [`RegisteredRetrievalProfile`] and therefore cannot turn a request-local
//! `top_k` into a second tuning surface.

use std::collections::BTreeMap;
use std::fmt;

use crate::config_registry::{
    ConfigEntry, ConfigResolveError, effective_config_fingerprint, resolve_effective_config,
};

const PROFILE_ID: &str = "retrieval.profile.id";
const QUERY_TRANSFORM: &str = "retrieval.profile.query_transform";
const TOP_K: &str = "retrieval.profile.top_k";
const PRODUCTION_ENABLED: &str = "retrieval.profile.production_enabled";
/// §55.6's registered-profile candidate-depth multiplier. Request construction imports this
/// constant so typed validation and the derived `cand_k` formula cannot drift.
pub const RETRIEVAL_CANDIDATE_MULTIPLIER: u32 = 5;

/// The transform that §55.1 places on a request after profile resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryTransform {
    Deterministic,
}

/// A profile accepted by the typed §50 bootstrap registry.
///
/// Fields are private and this type has no public constructor: callers can select neither
/// request depth nor transform. Add a production-capable profile by extending
/// [`retrieval_profile_registry`], never by constructing this value at a request call site.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredRetrievalProfile {
    profile_id: String,
    query_transform: QueryTransform,
    top_k: u32,
    effective_config_fingerprint: String,
}

impl RegisteredRetrievalProfile {
    #[must_use]
    pub fn profile_id(&self) -> &str {
        &self.profile_id
    }

    #[must_use]
    pub fn query_transform(&self) -> QueryTransform {
        self.query_transform
    }

    #[must_use]
    pub fn top_k(&self) -> u32 {
        self.top_k
    }

    #[must_use]
    pub fn effective_config_fingerprint(&self) -> &str {
        &self.effective_config_fingerprint
    }
}

/// Parsing errors at the retrieval-specific typed boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetrievalConfigError {
    Registry(ConfigResolveError),
    BlankProfileId,
    UnsupportedQueryTransform(String),
    GenerativeContractUnavailable,
    InvalidTopK(String),
    InvalidProductionEnabled(String),
    ProductionDisabled,
}

impl fmt::Display for RetrievalConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Registry(error) => error.fmt(f),
            Self::BlankProfileId => write!(f, "retrieval profile id must not be blank"),
            Self::UnsupportedQueryTransform(transform) => {
                write!(f, "unsupported retrieval query transform {transform:?}")
            }
            Self::GenerativeContractUnavailable => write!(
                f,
                "generative retrieval profiles require provider, purpose, budget, timeout, fallback, and manifest contracts"
            ),
            Self::InvalidTopK(value) => write!(f, "invalid retrieval profile top_k {value:?}"),
            Self::InvalidProductionEnabled(value) => {
                write!(f, "invalid retrieval profile production_enabled {value:?}")
            }
            Self::ProductionDisabled => {
                write!(f, "retrieval profile must be production_enabled=true")
            }
        }
    }
}

impl std::error::Error for RetrievalConfigError {}

/// The one §50 registry declaration for the currently shipped deterministic profile.
#[must_use]
pub fn retrieval_profile_registry() -> Vec<ConfigEntry> {
    vec![
        entry(PROFILE_ID, "string", "default_deterministic"),
        entry(
            QUERY_TRANSFORM,
            "enum:deterministic|generative",
            "deterministic",
        ),
        entry(TOP_K, "u32", "5"),
        entry(PRODUCTION_ENABLED, "bool", "true"),
    ]
}

/// Resolves process bootstrap configuration into the only type request construction accepts.
///
/// `raw` is intentionally accepted here, rather than at the request API: it belongs to a
/// process/config bootstrapper, and `resolve_effective_config` rejects undeclared values before
/// request handling can begin.
pub fn resolve_registered_retrieval_profile(
    raw: &BTreeMap<String, String>,
) -> Result<RegisteredRetrievalProfile, RetrievalConfigError> {
    let effective = resolve_effective_config(&retrieval_profile_registry(), raw)
        .map_err(RetrievalConfigError::Registry)?;
    let profile_id = required(&effective, PROFILE_ID).to_string();
    if profile_id.trim().is_empty() {
        return Err(RetrievalConfigError::BlankProfileId);
    }
    let query_transform = match required(&effective, QUERY_TRANSFORM) {
        "deterministic" => QueryTransform::Deterministic,
        "generative" => return Err(RetrievalConfigError::GenerativeContractUnavailable),
        other => {
            return Err(RetrievalConfigError::UnsupportedQueryTransform(
                other.to_string(),
            ));
        }
    };
    let top_k_text = required(&effective, TOP_K);
    let top_k = top_k_text
        .parse::<u32>()
        .ok()
        .filter(|top_k| *top_k > 0 && top_k.checked_mul(RETRIEVAL_CANDIDATE_MULTIPLIER).is_some())
        .ok_or_else(|| RetrievalConfigError::InvalidTopK(top_k_text.to_string()))?;
    let production_enabled = required(&effective, PRODUCTION_ENABLED)
        .parse::<bool>()
        .map_err(|_| {
            RetrievalConfigError::InvalidProductionEnabled(
                required(&effective, PRODUCTION_ENABLED).to_string(),
            )
        })?;
    if !production_enabled {
        return Err(RetrievalConfigError::ProductionDisabled);
    }
    Ok(RegisteredRetrievalProfile {
        profile_id,
        query_transform,
        top_k,
        effective_config_fingerprint: effective_config_fingerprint(&effective),
    })
}

fn entry(name: &str, type_name: &str, default: &str) -> ConfigEntry {
    ConfigEntry {
        name: name.to_string(),
        type_name: type_name.to_string(),
        default: Some(default.to_string()),
        scope: "process".to_string(),
        secret: false,
        reloadability: "static".to_string(),
        owner_module: "retrieval".to_string(),
    }
}

fn required<'a>(effective: &'a BTreeMap<String, String>, key: &str) -> &'a str {
    effective
        .get(key)
        .expect("retrieval profile registry resolves every declared key")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_defaults_resolve_to_the_registered_production_profile() {
        let profile = resolve_registered_retrieval_profile(&BTreeMap::new()).expect("defaults");
        assert_eq!(profile.profile_id(), "default_deterministic");
        assert_eq!(profile.query_transform(), QueryTransform::Deterministic);
        assert_eq!(profile.top_k(), 5);
        assert_eq!(profile.effective_config_fingerprint().len(), 64);
    }

    #[test]
    fn rejects_garbage_disabled_and_invalid_depth_values() {
        let mut raw = BTreeMap::new();
        raw.insert("not.registered".to_string(), "x".to_string());
        assert!(matches!(
            resolve_registered_retrieval_profile(&raw),
            Err(RetrievalConfigError::Registry(_))
        ));

        let mut raw = BTreeMap::new();
        raw.insert(TOP_K.to_string(), "0".to_string());
        assert!(matches!(
            resolve_registered_retrieval_profile(&raw),
            Err(RetrievalConfigError::InvalidTopK(_))
        ));

        let mut raw = BTreeMap::new();
        raw.insert(PRODUCTION_ENABLED.to_string(), "false".to_string());
        assert_eq!(
            resolve_registered_retrieval_profile(&raw),
            Err(RetrievalConfigError::ProductionDisabled)
        );

        let mut raw = BTreeMap::new();
        raw.insert(PRODUCTION_ENABLED.to_string(), "yes".to_string());
        assert!(matches!(
            resolve_registered_retrieval_profile(&raw),
            Err(RetrievalConfigError::InvalidProductionEnabled(_))
        ));
    }

    #[test]
    fn generative_profile_is_fail_closed_until_its_complete_contract_exists() {
        let mut raw = BTreeMap::new();
        raw.insert(QUERY_TRANSFORM.to_string(), "generative".to_string());
        assert_eq!(
            resolve_registered_retrieval_profile(&raw),
            Err(RetrievalConfigError::GenerativeContractUnavailable)
        );
    }

    #[test]
    fn effective_fingerprint_changes_with_registered_depth() {
        let default = resolve_registered_retrieval_profile(&BTreeMap::new()).expect("defaults");
        let mut raw = BTreeMap::new();
        raw.insert(TOP_K.to_string(), "10".to_string());
        let deeper = resolve_registered_retrieval_profile(&raw).expect("registered depth");
        assert_eq!(deeper.top_k(), 10);
        assert_ne!(
            default.effective_config_fingerprint(),
            deeper.effective_config_fingerprint()
        );
    }
}
