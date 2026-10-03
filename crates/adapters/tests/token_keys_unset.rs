//! `adapters::tests::token_keys_unset` — a process that never installed a token key set can neither issue nor verify a
//!   consistency token.
//! Depends-on: crates=[humaux-adapters, sqlx, time]; services=[]; env=[]; modules=[adapters::retrieve]
//! Called-by: [cargo-test]
//! Invariants: [own test binary that never calls install_token_keys; there is no default key, so both production
//!   entry points return TokenKeysUnset instead of an unsigned token or an unverified parse]
//! Spec: Baseline §15.5; ADR-0059 D-G

use humaux_adapters::retrieve::{
    RetrieveError, TokenClaims, decode_consistency_token, issue_consistency_token,
};
use sqlx::types::Uuid;
use time::OffsetDateTime;

fn claims() -> TokenClaims {
    TokenClaims {
        tenant_id: Uuid::nil(),
        workspace_id: None,
        scope_kind: "tenant".to_string(),
        scope_id: Uuid::nil(),
        domain: "knowledge".to_string(),
        projection_kind: "ingest".to_string(),
        projection_version: "v1".to_string(),
        stream_seq: 1,
        commit_seq: 1,
        issued_at: OffsetDateTime::UNIX_EPOCH,
        expires_at: OffsetDateTime::UNIX_EPOCH,
    }
}

#[test]
fn issue_without_installed_keys_is_token_keys_unset() {
    assert!(matches!(
        issue_consistency_token(&claims()),
        Err(RetrieveError::TokenKeysUnset)
    ));
}

#[test]
fn decode_without_installed_keys_is_token_keys_unset() {
    // An unsigned v0 token (the pre-ADR-0059 wire form): 11 hex-encoded fields.
    let plain = [
        Uuid::nil().to_string(),
        String::new(),
        "tenant".to_string(),
        Uuid::nil().to_string(),
        "knowledge".to_string(),
        "ingest".to_string(),
        "v1".to_string(),
        "1".to_string(),
        "1".to_string(),
        "0".to_string(),
        "0".to_string(),
    ]
    .join("\u{1}");
    let token: String = plain.bytes().map(|b| format!("{b:02x}")).collect();
    assert!(matches!(
        decode_consistency_token(&token),
        Err(RetrieveError::TokenKeysUnset)
    ));
}
