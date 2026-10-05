//! `domain::audit` — §77 双轨审计: `AuditEvent` minimal field set, the `AuditMetadata` allowlist type,
//!   `SensitiveAdminAction`, the closed `McpAuditAction` set, and the `AuditBatch` hash-chain (construction +
//!   verification only — no IO, no ObjectStore; the adapter-layer export port lives in `adapters::audit_sink`,
//!   §3/§78.3 keeps this crate free of it).
//! Depends-on: crates=[sha2, uuid]; services=[]; env=[]; modules=[domain::error, domain::ids]
//! Called-by: [adapters::audit_sink, adapters::confirm_token_repo, adapters::context_repo, adapters::distill_repo, adapters::maintenance_repo, adapters::memory_governance_repo, adapters::operation_receipt, adapters::provisioning, adapters::quota_repo, adapters::request_guard_repo, application::auth, gateway::guard, tests]
//! Invariants: []
//! Spec: Baseline §77
//!
//! §77 draws three log kinds apart (Application Logs / Security Audit Events / Financial
//! Ledger) and, within Security Audit, two further layers: **Operational Audit** (PostgreSQL
//! append-only application contract — `control.audit_events`, `migrations/0034_audit.sql`'s
//! `BEFORE UPDATE OR DELETE` trigger) and the **Immutable Audit Sink** (periodic
//! hash-chained batch export to WORM/immutable object storage — this module's [`AuditBatch`]
//! / [`batch_hash`] / [`verify_chain`]). The two-layer split exists because §77 states
//! plainly that the operational table "仍可被高权限管理员修改" — the Rust-level append-only
//! contract and the DB trigger are both defense in depth, not the tamper-evidence guarantee
//! itself; that guarantee is the exported hash chain.

use crate::error::ErrorCode;
use crate::ids::TenantId;
use uuid::Uuid;

/// Audit event id (§77 `event_id`).
///
/// Minted locally rather than folded into `domain::ids`'s frozen seven (§59): this task's
/// assigned file is `crates/domain/src/audit.rs` only, and `AuditEventId` is not among that
/// fixed list. Mint/parse shape hand-matches `ids::uuid_newtype!` (private to `ids.rs`,
/// exporting it is outside this task's file scope) — same deviation already taken by
/// `domain::identity::PrincipalId` and `domain::authority`'s `MemoryId`/`EvidenceId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AuditEventId(pub Uuid);

#[allow(clippy::new_without_default)] // see ids::uuid_newtype!'s identical note
impl AuditEventId {
    /// Mints a new id (UUIDv7, §49).
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Parses an existing UUID string; a malformed input is `INVALID_INPUT` (§52.1).
    pub fn parse(s: &str) -> Result<Self, ErrorCode> {
        Uuid::parse_str(s)
            .map(Self)
            .map_err(|_| ErrorCode::InvalidInput)
    }
}

/// §77 metadata allowlist: the closed, frozen set of keys an [`AuditEvent`]/
/// [`SensitiveAdminAction`] metadata entry may use (§78.2 no-stringly-typed applied to a map
/// key: membership in a fixed table is the property checked, not something inferred from the
/// string's shape). A key outside this set is rejected regardless of what it looks like —
/// this is the **primary** control, so a key like `authorization_code` (§77 MCP "不记录"
/// list) or `note` carrying a pasted secret in its *value* (values are never inspected; the
/// call site must never construct such a value at all) is refused simply for not being here,
/// with no substring heuristic to evade.
///
/// No caller outside this module's own tests exists yet (grep-verified when this list was
/// written) — the entries below are exactly what those tests exercise, not a set invented
/// ahead of a real need (YAGNI). A caller needing a new key extends this array, which is the
/// point: every key this type can ever hold has been looked at once, in review, against
/// §77's "禁止写" list before it exists — growth is a deliberate code change, not a runtime
/// decision made by a string-shape heuristic.
const ALLOWED_METADATA_KEYS: &[&str] = &["plan", "previous_role", "role"];

/// §77 metadata allowlist: an [`AuditEvent`]/[`SensitiveAdminAction`] metadata map that
/// **rejects a forbidden key at insertion time**, so no write path can construct one holding
/// a secret in the first place — the type is the enforcement point, not a filter run later
/// over an already-built map.
///
/// [`ALLOWED_METADATA_KEYS`] closed-set membership is checked first (primary control, see its
/// doc). A key is *additionally* rejected when its lowercase form contains any of `password` /
/// `token` / `key` / `secret` / `otp` / `byok` / `pkce` (case-insensitive substring, matching
/// §77's "禁止写" list and the MCP section's access/refresh token/PKCE verifier/TOTP secret) —
/// kept as a **second** line so a future addition to the closed set that happens to collide
/// with a named secret category is still caught, not because the substring check alone is
/// trusted to gate admission (it previously was, and false-positived on benign names like
/// `keyboard_layout`/`monkey_patch` while still admitting anything not spelled with one of
/// the seven substrings — the failure mode a denylist-shaped check always has).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuditMetadata {
    entries: Vec<(String, String)>,
}

impl AuditMetadata {
    /// Empty metadata map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts one allowlisted key/value pair. Rejects — without mutating `self` — a key
    /// outside [`ALLOWED_METADATA_KEYS`], a key whose lowercase form contains a forbidden
    /// substring (see type doc), or a key already present (§50 fail-loud: a rejected insert
    /// must never look like a silently-dropped duplicate once the map round-trips through
    /// the `jsonb` column, where duplicate keys collapse to last-wins with no error). A
    /// malformed insert attempt is `INVALID_INPUT` (§52.1).
    pub fn insert(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), ErrorCode> {
        let key = key.into();
        if !ALLOWED_METADATA_KEYS.contains(&key.as_str()) {
            return Err(ErrorCode::InvalidInput);
        }
        if Self::is_forbidden_key(&key) {
            return Err(ErrorCode::InvalidInput);
        }
        if self.entries.iter().any(|(k, _)| k == &key) {
            return Err(ErrorCode::InvalidInput);
        }
        self.entries.push((key, value.into()));
        Ok(())
    }

    /// Case-insensitive substring check against the forbidden-key list (type doc); second
    /// line of defense behind [`ALLOWED_METADATA_KEYS`] membership.
    fn is_forbidden_key(key: &str) -> bool {
        let lower = key.to_ascii_lowercase();
        ["password", "token", "key", "secret", "otp", "byok", "pkce"]
            .iter()
            .any(|forbidden| lower.contains(forbidden))
    }

    /// Read-only view over the accepted entries, insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Number of accepted entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no entry was accepted.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Reserved system tenant for a §77 security event that cannot be resolved to a real tenant
/// (e.g. a failed `MCP_AUTH_LOGIN` — the ten listed events include failed authentications,
/// which by definition have no established tenant identity). `control.audit_events.tenant_id`
/// is `NOT NULL` with an FK to `control.tenants`, so such an event needs *some* row to point
/// at; this is the nil UUID, matching the reserved `control.tenants` row the audit-hardening
/// migration seeds for it (`migrations/0041_audit_hardening.sql`). Single canonical constant
/// so no call site re-invents the sentinel (repo `CLAUDE.md` "唯一构造点模式" spirit); a
/// protocol-constant-shaped value, not hardcoded business config (§78.1 allows this class).
pub const SYSTEM_TENANT_ID: TenantId = TenantId(Uuid::nil());

/// §77 "AuditEvent 最少" field set, verbatim. No field beyond `before_fingerprint` /
/// `after_fingerprint` carries `?` in the spec listing — those two are the only `Option`
/// fields here; every other field is mandatory at construction (a caller with no real
/// `client_ip`/`request_id` etc. for a background/system-originated action supplies its own
/// sentinel string, this type does not invent one).
///
/// `actor_type`/`actor_id`/`action`/`resource_type`/`resource_id`/`result` stay `String`
/// rather than closed Rust enums (§78.2): unlike `EvidenceOriginClass`'s 9 variants or
/// the closed `McpAuditAction` set, §77 never freezes an enumerated set for these — inventing
/// one here would be structure the spec does not license, not a §78.2 compliance win.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEvent {
    pub event_id: AuditEventId,
    pub ts: std::time::SystemTime,
    pub tenant_id: TenantId,
    pub actor_type: String,
    pub actor_id: String,
    pub action: String,
    pub resource_type: String,
    pub resource_id: String,
    pub result: String,
    pub request_id: String,
    pub trace_id: String,
    pub client_ip: String,
    pub user_agent_hash: String,
    pub risk_tags: Vec<String>,
    /// §77 `before_fingerprint?` — absent when the action has no prior-state comparison
    /// (e.g. a create).
    pub before_fingerprint: Option<String>,
    /// §77 `after_fingerprint?` — absent when the action has no resulting-state comparison
    /// (e.g. a delete/revoke read back as "gone").
    pub after_fingerprint: Option<String>,
    pub metadata: AuditMetadata,
}

/// §77 Audit Immutability §"Sensitive Admin Action" field list, verbatim: actor / subject
/// tenant·user / reason / request·ticket / before·after high-level metadata / trace_id /
/// step_up_auth_context — all seven are "必须包含", so all seven are plain (non-`Option`)
/// fields here; a caller missing one cannot construct the type at all.
///
/// Covers both the §73 Support Access impersonation record ("actor_user/impersonated_user·
/// tenant/reason/action/request_id 必须不可省略") and Break-glass ("strong auth/explicit
/// reason/short TTL/immutable audit/post-incident review") — `subject_user: None` is the
/// non-impersonation admin-action shape, `Some` is impersonation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SensitiveAdminAction {
    /// The admin/support employee performing the action.
    pub actor: String,
    /// Tenant the action targets.
    pub subject_tenant: TenantId,
    /// User the action targets, if any (impersonation/support-access target; §73).
    pub subject_user: Option<String>,
    pub reason: String,
    /// Request/ticket reference (§73 `SupportAccessRequest.ticket`).
    pub ticket: String,
    pub before_metadata: AuditMetadata,
    pub after_metadata: AuditMetadata,
    pub trace_id: String,
    /// Evidence that step-up authentication (strong auth) gated this action (§73 MFA/
    /// step-up, Break-glass "strong auth"). Opaque here — no closed shape is frozen by §77
    /// for its contents.
    pub step_up_auth_context: String,
}

/// §77 "MCP Security Audit Events" — the events MCP integration "至少记录" (closed set,
/// unlike the general `AuditEvent.action` string field above).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpAuditAction {
    McpGrantCreated,
    McpGrantRefreshed,
    McpGrantRevoked,
    McpScopeDenied,
    McpTenantBoundaryDenied,
    McpQuotaReserved,
    McpQuotaConsumed,
    McpQuotaReleased,
    McpClientRegistered,
    McpAuthLogin,
    McpRequestDenied,
    McpRequestFinished,
    McpRequestOutcomeUnknown,
}

impl McpAuditAction {
    /// All variants, §77 listing order.
    pub const ALL: [McpAuditAction; 13] = [
        McpAuditAction::McpGrantCreated,
        McpAuditAction::McpGrantRefreshed,
        McpAuditAction::McpGrantRevoked,
        McpAuditAction::McpScopeDenied,
        McpAuditAction::McpTenantBoundaryDenied,
        McpAuditAction::McpQuotaReserved,
        McpAuditAction::McpQuotaConsumed,
        McpAuditAction::McpQuotaReleased,
        McpAuditAction::McpClientRegistered,
        McpAuditAction::McpAuthLogin,
        McpAuditAction::McpRequestDenied,
        McpAuditAction::McpRequestFinished,
        McpAuditAction::McpRequestOutcomeUnknown,
    ];

    /// SCREAMING_SNAKE wire form — the value this variant is written into
    /// `AuditEvent.action` as (§77 lists these as literal event names).
    pub const fn as_str(self) -> &'static str {
        match self {
            McpAuditAction::McpGrantCreated => "MCP_GRANT_CREATED",
            McpAuditAction::McpGrantRefreshed => "MCP_GRANT_REFRESHED",
            McpAuditAction::McpGrantRevoked => "MCP_GRANT_REVOKED",
            McpAuditAction::McpScopeDenied => "MCP_SCOPE_DENIED",
            McpAuditAction::McpTenantBoundaryDenied => "MCP_TENANT_BOUNDARY_DENIED",
            McpAuditAction::McpQuotaReserved => "MCP_QUOTA_RESERVED",
            McpAuditAction::McpQuotaConsumed => "MCP_QUOTA_CONSUMED",
            McpAuditAction::McpQuotaReleased => "MCP_QUOTA_RELEASED",
            McpAuditAction::McpClientRegistered => "MCP_CLIENT_REGISTERED",
            McpAuditAction::McpAuthLogin => "MCP_AUTH_LOGIN",
            McpAuditAction::McpRequestDenied => "MCP_REQUEST_DENIED",
            McpAuditAction::McpRequestFinished => "MCP_REQUEST_FINISHED",
            McpAuditAction::McpRequestOutcomeUnknown => "MCP_REQUEST_OUTCOME_UNKNOWN",
        }
    }
}

/// Genesis previous-batch hash — the fixed all-zero sentinel the first [`AuditBatch`] in a
/// lineage carries as `previous_batch_hash`. Not itself the hash of anything; distinguished
/// from a real [`batch_hash`] output only by being the fixed starting point every lineage's
/// index-0 batch must equal ([`verify_chain`] checks it explicitly at index 0).
pub const GENESIS_BATCH_HASH: [u8; 32] = [0u8; 32];

/// One exported §77 "Audit Batch" (verbatim field list): the tamper-evident unit
/// periodically exported from the append-only Operational Audit table to the Immutable
/// Audit Sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditBatch {
    /// First covered Operational Audit sequence number (inclusive).
    pub seq_start: i64,
    /// Last covered sequence number (inclusive).
    pub seq_end: i64,
    /// [`batch_hash`] of the immediately preceding batch in this lineage, or
    /// [`GENESIS_BATCH_HASH`] for the first batch — the chain link.
    pub previous_batch_hash: [u8; 32],
    /// `sha256` over this batch's exported payload bytes (raw-byte hash, no normalization —
    /// same contract as `evidence::payload_sha256`, §8.1).
    pub payload_hash: [u8; 32],
    /// Where the exported bytes landed (WORM/immutable object storage reference) — written
    /// through the adapter-layer `ObjectStore` port (`adapters::audit_sink`, Phase 13).
    pub exported_object: String,
    pub created_at: std::time::SystemTime,
}

/// Raw-byte `sha256` over exported batch payload bytes — same no-normalization contract as
/// `evidence::payload_sha256` (§8.1), separate function because it hashes Immutable-Sink
/// export bytes, not an Evidence payload; the two are unrelated content domains that happen
/// to share an algorithm.
pub fn payload_hash(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).into()
}

/// This batch's own content hash — the value the *next* batch in the lineage must carry as
/// its `previous_batch_hash` (§77 "tamper-evident chain"). Deliberately excludes
/// `exported_object`/`created_at`: those describe *where*/*when* the batch landed, not *what*
/// audit rows it covers — chaining on them would make the link fragile to a re-export at a
/// new path/time with no audit content actually changing.
pub fn batch_hash(batch: &AuditBatch) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(batch.seq_start.to_be_bytes());
    hasher.update(batch.seq_end.to_be_bytes());
    hasher.update(batch.previous_batch_hash);
    hasher.update(batch.payload_hash);
    hasher.finalize().into()
}

/// Walks a batch lineage in order and checks every link (§77 "tamper-evident chain"): both
/// the hash link (a single mutated field anywhere in `batches[i]` changes
/// `batch_hash(&batches[i])`, which no longer matches `batches[i + 1].previous_batch_hash`)
/// **and** `seq` continuity (`batches[i].seq_start == batches[i - 1].seq_end + 1`, head batch
/// `seq_start == 1`). Hash-only checking would pass a lineage missing a whole exported range
/// outright — e.g. `[seq 1..100] -> [seq 500..600]` — which is exactly the "高权限管理员删了
/// 审计行" tamper the two-layer §77 design exists to catch; a gap between batches is
/// indistinguishable from a deleted batch unless coverage itself is checked, not just the
/// hashes of the batches that do exist. Returns `ErrorCode` (§52: the sole terminal-error
/// enum — no bespoke `ChainVerificationError` here); which batch broke is available
/// separately from [`first_broken_link`] for callers that want to report *where*. Does not
/// re-verify `payload_hash` against raw export bytes; that comparison happens at export time,
/// adapter layer (Phase 13).
pub fn verify_chain(batches: &[AuditBatch]) -> Result<(), ErrorCode> {
    if batches.is_empty() {
        return Err(ErrorCode::InvalidInput);
    }
    match first_broken_link(batches) {
        Some(_) => Err(ErrorCode::Conflict),
        None => Ok(()),
    }
}

/// Index of the first batch in `batches` whose incoming hash link or `seq` continuity is
/// broken (see [`verify_chain`]), or `None` for an empty slice or a fully-verified chain.
/// Kept separate from `verify_chain`'s `Result<(), ErrorCode>` because §52 confines the
/// terminal-error taxonomy to `ErrorCode`/`DegradeCode` (repo `CLAUDE.md` 硬边界: 禁止新开
/// 第三个错误枚举) — failure *location* is diagnostic detail, not a new error kind, so it
/// travels out-of-band instead of through a bespoke enum variant.
pub fn first_broken_link(batches: &[AuditBatch]) -> Option<usize> {
    let first = batches.first()?;
    if first.previous_batch_hash != GENESIS_BATCH_HASH || first.seq_start != 1 {
        return Some(0);
    }
    (1..batches.len()).find(|&i| {
        batches[i].previous_batch_hash != batch_hash(&batches[i - 1])
            || batches[i].seq_start != batches[i - 1].seq_end + 1
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn sample_event(action: &str) -> AuditEvent {
        AuditEvent {
            event_id: AuditEventId::new(),
            ts: SystemTime::UNIX_EPOCH,
            tenant_id: TenantId::new(),
            actor_type: "user".to_string(),
            actor_id: "actor-1".to_string(),
            action: action.to_string(),
            resource_type: "mcp_grant".to_string(),
            resource_id: "grant-1".to_string(),
            result: "SUCCESS".to_string(),
            request_id: "req-1".to_string(),
            trace_id: "trace-1".to_string(),
            client_ip: "203.0.113.7".to_string(),
            user_agent_hash: "deadbeef".to_string(),
            risk_tags: vec![],
            before_fingerprint: None,
            after_fingerprint: None,
            metadata: AuditMetadata::new(),
        }
    }

    #[test]
    fn system_tenant_id_is_the_nil_uuid() {
        assert_eq!(SYSTEM_TENANT_ID.0, Uuid::nil());
    }

    #[test]
    fn audit_event_id_round_trips_and_is_v7() {
        let id = AuditEventId::new();
        assert_eq!(id.0.get_version_num(), 7);
        let parsed = AuditEventId::parse(&id.0.to_string()).expect("valid uuid string parses");
        assert_eq!(id, parsed);
    }

    #[test]
    fn audit_event_id_parse_rejects_garbage() {
        assert_eq!(
            AuditEventId::parse("not-a-uuid").unwrap_err(),
            ErrorCode::InvalidInput
        );
    }

    /// Acceptance test named in the T2.8 task card: "写入含 password/token 字段的 metadata
    /// 被 allowlist 拒".
    #[test]
    fn metadata_rejects_forbidden_key_substrings() {
        for key in [
            "password",
            "user_password",
            "session_token",
            "reset_token",
            "api_key",
            "totp_secret",
            "otp_code",
            "byok_credential",
            "pkce_verifier",
        ] {
            let mut metadata = AuditMetadata::new();
            assert_eq!(
                metadata.insert(key, "x").unwrap_err(),
                ErrorCode::InvalidInput,
                "key {key} should have been rejected"
            );
            assert!(
                metadata.is_empty(),
                "a rejected insert must not mutate the map"
            );
        }
    }

    #[test]
    fn metadata_accepts_benign_keys() {
        let mut metadata = AuditMetadata::new();
        metadata.insert("plan", "PRO").unwrap();
        metadata.insert("previous_role", "member").unwrap();
        assert_eq!(metadata.len(), 2);
        let collected: Vec<_> = metadata.iter().collect();
        assert_eq!(
            collected,
            vec![("plan", "PRO"), ("previous_role", "member")]
        );
    }

    #[test]
    fn metadata_check_is_case_insensitive() {
        let mut metadata = AuditMetadata::new();
        assert!(metadata.insert("PASSWORD_HASH", "x").is_err());
        assert!(metadata.insert("Api-Key".replace('-', "_"), "x").is_err());
    }

    /// Red test for the blocker: under the pre-fix denylist, every key below was `Ok` —
    /// including `authorization_code`, which is on §77's own MCP "不记录" list verbatim, and
    /// `note`, which admitted any secret because only the *key* was ever checked. Closed-set
    /// [`ALLOWED_METADATA_KEYS`] membership rejects all of them for not being in the set, with
    /// no substring to evade.
    #[test]
    fn metadata_rejects_keys_outside_the_allowlist() {
        for key in [
            "authorization_code",
            "refresh",
            "bearer",
            "credential",
            "passwd",
            "private_memory_body",
            "note",
        ] {
            let mut metadata = AuditMetadata::new();
            assert_eq!(
                metadata.insert(key, "x").unwrap_err(),
                ErrorCode::InvalidInput,
                "key {key} should have been rejected (not allowlisted)"
            );
        }
    }

    /// A value is never inspected — the call site must never construct a secret-bearing value
    /// in the first place (type doc) — but a key outside the allowlist is still rejected
    /// regardless of what its value contains.
    #[test]
    fn metadata_rejects_disallowed_key_even_with_innocuous_looking_value() {
        let mut metadata = AuditMetadata::new();
        assert!(
            metadata
                .insert("note", "user pasted their api_key sk-live-1234")
                .is_err()
        );
    }

    /// §50 fail-loud: a duplicate key must be rejected, not silently accepted and later
    /// collapsed to last-wins by the `jsonb` column's own key-uniqueness semantics.
    #[test]
    fn metadata_rejects_duplicate_key() {
        let mut metadata = AuditMetadata::new();
        metadata.insert("role", "member").unwrap();
        assert_eq!(
            metadata.insert("role", "admin").unwrap_err(),
            ErrorCode::InvalidInput
        );
        assert_eq!(metadata.len(), 1);
    }

    #[test]
    fn mcp_audit_action_has_the_complete_closed_set() {
        assert_eq!(McpAuditAction::ALL.len(), 13);

        fn assert_exhaustive(a: McpAuditAction) {
            match a {
                McpAuditAction::McpGrantCreated
                | McpAuditAction::McpGrantRefreshed
                | McpAuditAction::McpGrantRevoked
                | McpAuditAction::McpScopeDenied
                | McpAuditAction::McpTenantBoundaryDenied
                | McpAuditAction::McpQuotaReserved
                | McpAuditAction::McpQuotaConsumed
                | McpAuditAction::McpQuotaReleased
                | McpAuditAction::McpClientRegistered
                | McpAuditAction::McpAuthLogin
                | McpAuditAction::McpRequestDenied
                | McpAuditAction::McpRequestFinished
                | McpAuditAction::McpRequestOutcomeUnknown => {}
            }
        }
        for a in McpAuditAction::ALL {
            assert_exhaustive(a);
        }
    }

    #[test]
    fn mcp_audit_action_wire_forms_are_screaming_snake_and_unique() {
        let mut seen = std::collections::HashSet::new();
        for a in McpAuditAction::ALL {
            let s = a.as_str();
            assert!(
                s.chars().all(|c| c.is_ascii_uppercase() || c == '_'),
                "{s} is not SCREAMING_SNAKE"
            );
            assert!(seen.insert(s), "duplicate wire form {s}");
        }
        assert_eq!(seen.len(), McpAuditAction::ALL.len());
    }

    /// Acceptance test named in the T2.8 task card: "每类 MCP_* 事件构造往返" — build an
    /// `AuditEvent` from every `McpAuditAction` variant and confirm the action string round
    /// trips.
    #[test]
    fn every_mcp_audit_action_round_trips_through_an_audit_event() {
        for action in McpAuditAction::ALL {
            let event = sample_event(action.as_str());
            assert_eq!(event.action, action.as_str());
        }
    }

    fn sample_batch(
        seq_start: i64,
        seq_end: i64,
        previous_batch_hash: [u8; 32],
        payload: &[u8],
    ) -> AuditBatch {
        AuditBatch {
            seq_start,
            seq_end,
            previous_batch_hash,
            payload_hash: payload_hash(payload),
            exported_object: format!("s3://audit/batch-{seq_start}-{seq_end}"),
            created_at: SystemTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn genesis_batch_chains_from_genesis_hash() {
        let genesis = sample_batch(1, 100, GENESIS_BATCH_HASH, b"batch-1-payload");
        assert!(verify_chain(&[genesis]).is_ok());
    }

    #[test]
    fn three_batch_chain_verifies() {
        let b1 = sample_batch(1, 100, GENESIS_BATCH_HASH, b"payload-1");
        let b2 = sample_batch(101, 200, batch_hash(&b1), b"payload-2");
        let b3 = sample_batch(201, 300, batch_hash(&b2), b"payload-3");
        assert!(verify_chain(&[b1, b2, b3]).is_ok());
    }

    /// Acceptance test named in the T2.8 task card: "篡改单条后链校验红". A tail-batch tamper
    /// with no following batch to link against it is a real, documented limitation of
    /// hash-only chaining (nothing yet references the tampered tail), but is deliberately not
    /// asserted here as "still verifies" — encoding that gap as an expected-`Ok` assertion is
    /// exactly the tautological-test shape (green regardless of what the implementation does)
    /// this file's own review flagged; the mid-chain injection below is what this test proves.
    #[test]
    fn tampering_a_single_batch_breaks_the_chain() {
        let b1 = sample_batch(1, 100, GENESIS_BATCH_HASH, b"payload-1");
        let b2 = sample_batch(101, 200, batch_hash(&b1), b"payload-2");
        let b3 = sample_batch(201, 300, batch_hash(&b2), b"payload-3");
        assert!(verify_chain(&[b1.clone(), b2.clone(), b3.clone()]).is_ok());

        // Tamper b2 in place (e.g. someone edited its seq_end after the fact) — b3's stored
        // previous_batch_hash no longer matches the (now different) batch_hash(&b2), and its
        // seq_start no longer follows tampered_b2's (now different) seq_end.
        let mut tampered_b2 = b2.clone();
        tampered_b2.seq_end = 9_999;
        assert_eq!(
            verify_chain(&[b1, tampered_b2, b3]),
            Err(ErrorCode::Conflict)
        );
    }

    #[test]
    fn first_broken_link_reports_the_tampered_index() {
        let b1 = sample_batch(1, 100, GENESIS_BATCH_HASH, b"payload-1");
        let b2 = sample_batch(101, 200, batch_hash(&b1), b"payload-2");
        let b3 = sample_batch(201, 300, batch_hash(&b2), b"payload-3");
        assert_eq!(
            first_broken_link(&[b1.clone(), b2.clone(), b3.clone()]),
            None
        );

        let mut tampered_b2 = b2.clone();
        tampered_b2.seq_end = 9_999;
        assert_eq!(first_broken_link(&[b1, tampered_b2, b3]), Some(2));
    }

    /// Major finding: hash-only checking passes a lineage with a whole exported range
    /// missing — indistinguishable from a deleted batch unless `seq` coverage itself is
    /// checked. Hash links are all individually valid here; only the gap is wrong.
    #[test]
    fn a_gap_between_batches_breaks_the_chain_even_with_valid_hash_links() {
        let b1 = sample_batch(1, 100, GENESIS_BATCH_HASH, b"payload-1");
        let b2 = sample_batch(500, 600, batch_hash(&b1), b"payload-2");
        assert_eq!(verify_chain(&[b1, b2]), Err(ErrorCode::Conflict));
    }

    /// A first batch whose `seq_start` isn't 1 (e.g. a lineage's head silently starts mid-way
    /// through the operational sequence) must be rejected, not just checked for the genesis
    /// hash sentinel.
    #[test]
    fn a_head_batch_not_starting_at_seq_1_is_rejected() {
        let b1 = sample_batch(5_000, 5_100, GENESIS_BATCH_HASH, b"payload-1");
        assert_eq!(verify_chain(&[b1]), Err(ErrorCode::Conflict));
    }

    #[test]
    fn empty_chain_is_rejected() {
        assert_eq!(verify_chain(&[]), Err(ErrorCode::InvalidInput));
        assert_eq!(first_broken_link(&[]), None);
    }

    #[test]
    fn chain_not_starting_from_genesis_is_rejected() {
        let not_genesis = [1u8; 32];
        let b1 = sample_batch(1, 100, not_genesis, b"payload-1");
        assert_eq!(verify_chain(&[b1]), Err(ErrorCode::Conflict));
    }

    #[test]
    fn sensitive_admin_action_carries_all_seven_required_fields() {
        let mut before = AuditMetadata::new();
        before.insert("role", "member").unwrap();
        let mut after = AuditMetadata::new();
        after.insert("role", "admin").unwrap();

        let action = SensitiveAdminAction {
            actor: "support-employee-1".to_string(),
            subject_tenant: TenantId::new(),
            subject_user: Some("user-1".to_string()),
            reason: "customer-requested role escalation".to_string(),
            ticket: "TICKET-42".to_string(),
            before_metadata: before,
            after_metadata: after,
            trace_id: "trace-9".to_string(),
            step_up_auth_context: "webauthn:2026-08-26T00:00:00Z".to_string(),
        };
        assert_eq!(action.ticket, "TICKET-42");
        assert_eq!(action.before_metadata.len(), 1);
        assert_eq!(action.after_metadata.len(), 1);
    }
}
