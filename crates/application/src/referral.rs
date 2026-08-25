//! H5: §75 Invitation/Referral 双轨（Phase 2 wave 实现；判据出处见 spec 家章）.
//!
//! §75 frozen: Team Invitation (security object, §75.1) and Referral (marketing/value
//! object, §75.2) never share a table or a code path — this module keeps them as two
//! independent sections rather than one generic "invite" abstraction, on purpose.
//!
//! This module is pure state-machine/policy logic — no SQLx, no HTTP (§3/§78.3 Domain
//! layer discipline extended to this crate's Phase 2 modules). Row persistence (`control.
//! team_invitations` / `control.referral_*`, `migrations/0032_invitation_referral.sql`)
//! is an adapter concern outside this file's scope; every function here takes the caller's
//! already-fetched row state as plain arguments and returns either an updated state or an
//! [`ErrorCode`] — it never reaches for a database itself.

use humaux_domain::error::ErrorCode;
use humaux_domain::ids::TenantId;
use sha2::{Digest, Sha256};
use std::time::SystemTime;

// =============================================================================
// §75.1 Team Invitation (security object)
// =============================================================================

/// §75.1 `control.team_invitations.status`: `PENDING -> ACCEPTED | REVOKED | EXPIRED`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvitationStatus {
    Pending,
    Accepted,
    Revoked,
    Expired,
}

impl InvitationStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Accepted => "ACCEPTED",
            Self::Revoked => "REVOKED",
            Self::Expired => "EXPIRED",
        }
    }
}

/// Generates a bearer invitation token: `(raw_token, token_hash)`. The raw token is shown
/// to the invitee exactly once by the caller (email body); only `token_hash` — SHA-256 hex
/// of the raw token — is ever persisted (repo CLAUDE.md 安全红线: token 永不入日志/DB 明
/// 文, DB 只存 hash). 32 random bytes (256 bits) via the OS CSPRNG, hex-encoded.
pub fn generate_invitation_token() -> (String, String) {
    let mut raw_bytes = [0u8; 32];
    rand::fill(&mut raw_bytes);
    let raw_token = hex::encode(raw_bytes);
    (raw_token.clone(), hash_token(&raw_token))
}

/// SHA-256 hex digest of a raw bearer token — the sole hash construction point this module
/// uses, so `accept_invitation`'s comparison and whatever persists `token_hash` at
/// invitation-creation time can never drift onto two different hash functions.
pub fn hash_token(raw_token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_token.as_bytes());
    hex::encode(hasher.finalize())
}

/// Constant-time byte comparison — `token_hash` is derived from a secret, and even though
/// it is itself only a hash (not the bearer secret), comparing it with `==` leaks timing
/// information proportional to the common prefix length. Defense in depth, not the only
/// thing standing between an attacker and a hit (a single guess still needs 2^256 tries).
fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Server-held ground truth for one `control.team_invitations` row — everything
/// [`accept_invitation`] re-derives from, never from the client's request.
#[derive(Debug, Clone)]
pub struct InvitationRecord {
    pub tenant_id: TenantId,
    pub invited_email: String,
    pub role: String,
    pub token_hash: String,
    pub expires_at: SystemTime,
    pub status: InvitationStatus,
}

/// What an "accept invitation" HTTP/MCP handler received from the caller — untrusted input,
/// each field independently re-checked against [`InvitationRecord`] by
/// [`accept_invitation`].
#[derive(Clone)]
pub struct AcceptInvitationRequest<'a> {
    pub raw_token: &'a str,
    pub claimed_tenant_id: TenantId,
    pub claimed_role: &'a str,
    pub claimed_email: &'a str,
}

/// Hand-written `Debug` — `raw_token` is the plaintext bearer secret (repo CLAUDE.md 安全红
/// 线: token 永不入日志/DB 明文). A derived `Debug` would print it verbatim into any `{:?}`
/// log of a rejected/failed accept request; this prints its SHA-256 hash instead, matching
/// what `token_hash` persistence already protects.
impl std::fmt::Debug for AcceptInvitationRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcceptInvitationRequest")
            .field("raw_token_hash", &hash_token(self.raw_token))
            .field("claimed_tenant_id", &self.claimed_tenant_id)
            .field("claimed_role", &self.claimed_role)
            .field("claimed_email", &self.claimed_email)
            .finish()
    }
}

/// §75.1: "接受时必须服务器再次检查 tenant、role、email、token、邀请状态" — five
/// independent checks, every one against [`InvitationRecord`] (the row), never against
/// anything the request itself asserts about them. A request whose token is correct but
/// whose `claimed_role`/`claimed_tenant_id`/`claimed_email` disagrees with the row is
/// rejected exactly the same as a wrong token — tampering with any one of the five fails
/// closed (`Forbidden`), not just a wrong token.
///
/// Email comparison is case-insensitive (ASCII-lowercased) — §74.1's own scope ("只规范化
/// domain case") is a different module's concern (H2/§74), but a bare `==` here would make
/// `Bob@Example.com` fail against a stored `bob@example.com` for reasons that have nothing
/// to do with tampering; case sensitivity is not a security property of an email address.
pub fn accept_invitation(
    record: &InvitationRecord,
    req: &AcceptInvitationRequest<'_>,
    now: SystemTime,
) -> Result<(), ErrorCode> {
    if record.status != InvitationStatus::Pending {
        return Err(ErrorCode::Conflict);
    }
    if now >= record.expires_at {
        return Err(ErrorCode::Forbidden);
    }
    if !constant_time_eq(&hash_token(req.raw_token), &record.token_hash) {
        return Err(ErrorCode::Forbidden);
    }
    if req.claimed_tenant_id != record.tenant_id {
        return Err(ErrorCode::Forbidden);
    }
    if req.claimed_role != record.role {
        return Err(ErrorCode::Forbidden);
    }
    if !req
        .claimed_email
        .eq_ignore_ascii_case(&record.invited_email)
    {
        return Err(ErrorCode::Forbidden);
    }
    Ok(())
}

// =============================================================================
// §75.2 Referral (marketing/value object)
// =============================================================================

/// §75.2 `control.referral_rewards.status`: `ATTRIBUTED -> QUALIFIED -> MATURING -> GRANTED
/// -> REVOKED`. `REVOKED` is reachable from `QUALIFIED`/`MATURING`/`GRANTED` (a chargeback
/// or fraud signal discovered after qualification/grant must still be able to revoke it),
/// every other edge is the single forward chain — no skipping a state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewardStatus {
    Attributed,
    Qualified,
    Maturing,
    Granted,
    Revoked,
}

impl RewardStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Attributed => "ATTRIBUTED",
            Self::Qualified => "QUALIFIED",
            Self::Maturing => "MATURING",
            Self::Granted => "GRANTED",
            Self::Revoked => "REVOKED",
        }
    }
}

/// The §75.2 legal-transition set, verbatim. Not derivable from ordinal comparison
/// (`REVOKED` is reachable from three different predecessors, not just the state before
/// it) — this table is the one place that fact is encoded; nothing else in this module
/// re-derives it.
fn transition_allowed(from: RewardStatus, to: RewardStatus) -> bool {
    use RewardStatus::{Attributed, Granted, Maturing, Qualified, Revoked};
    matches!(
        (from, to),
        (Attributed, Qualified)
            | (Qualified, Maturing)
            | (Maturing, Granted)
            | (Qualified, Revoked)
            | (Maturing, Revoked)
            | (Granted, Revoked)
    )
}

/// One `control.credit_ledger` row-to-be-inserted — append-only by construction (§75.2:
/// "撤销通过反向 entry，不原地改余额"). Nothing in this module ever constructs one that
/// edits or replaces an existing entry; [`revoke_reward`] only ever produces a *new* one
/// with `reverses_entry_id` pointing back at the grant it cancels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerEntry {
    pub tenant_id: TenantId,
    pub delta: i64,
    pub reason: &'static str,
    pub reference_type: &'static str,
    pub reference_id: String,
    pub reverses_entry_id: Option<String>,
}

/// Outcome of a state-machine transition that also produces a ledger entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewardTransitionOutcome {
    pub new_status: RewardStatus,
    pub ledger_entry: LedgerEntry,
}

/// §75.2: `ATTRIBUTED -> QUALIFIED` step of the qualification pipeline (email verified +
/// paid/qualified event, per spec's example list) — a plain status transition, no ledger
/// entry (money moves only at [`grant_reward`]).
pub fn qualify(current: RewardStatus) -> Result<RewardStatus, ErrorCode> {
    if transition_allowed(current, RewardStatus::Qualified) {
        Ok(RewardStatus::Qualified)
    } else {
        Err(ErrorCode::Conflict)
    }
}

/// §75.2: `QUALIFIED -> MATURING` — the refund/chargeback observation window opens; still
/// no ledger entry.
pub fn mature(current: RewardStatus) -> Result<RewardStatus, ErrorCode> {
    if transition_allowed(current, RewardStatus::Maturing) {
        Ok(RewardStatus::Maturing)
    } else {
        Err(ErrorCode::Conflict)
    }
}

/// §75.2: `MATURING -> GRANTED`, the only edge that mints a [`LedgerEntry`]. Calling this
/// from any state other than `MATURING` — most importantly `ATTRIBUTED` (fresh
/// registration) — is rejected outright: "绝不能：注册成功 -> 立即送钱/送无限额度" is
/// enforced structurally here, not by a caller convention that could be forgotten.
/// `credits` must be positive; a non-positive reward amount is a caller bug
/// (`INVALID_INPUT`), not a legal zero-value grant.
pub fn grant_reward(
    current: RewardStatus,
    tenant_id: TenantId,
    reward_id: &str,
    credits: i64,
) -> Result<RewardTransitionOutcome, ErrorCode> {
    if !transition_allowed(current, RewardStatus::Granted) {
        return Err(ErrorCode::Conflict);
    }
    if credits <= 0 {
        return Err(ErrorCode::InvalidInput);
    }
    Ok(RewardTransitionOutcome {
        new_status: RewardStatus::Granted,
        ledger_entry: LedgerEntry {
            tenant_id,
            delta: credits,
            reason: "REFERRAL_REWARD",
            reference_type: "referral_reward",
            reference_id: reward_id.to_string(),
            reverses_entry_id: None,
        },
    })
}

/// §75.2: `{QUALIFIED,MATURING,GRANTED} -> REVOKED`. `original_ledger_entry_id`/
/// `original_delta` describe the grant entry being cancelled (only meaningful — and only
/// present in the DB — when revoking from `GRANTED`; revoking from `QUALIFIED`/`MATURING`
/// has no prior grant entry to reverse, so callers pass `None`/`0`). The produced entry's
/// delta is always the exact negation of the original — never a caller-supplied amount —
/// so a reversal cannot under- or over-correct the balance it is cancelling.
pub fn revoke_reward(
    current: RewardStatus,
    tenant_id: TenantId,
    reward_id: &str,
    original_grant: Option<(&str, i64)>,
) -> Result<RewardTransitionOutcome, ErrorCode> {
    if !transition_allowed(current, RewardStatus::Revoked) {
        return Err(ErrorCode::Conflict);
    }
    let (delta, reverses_entry_id) = match original_grant {
        Some((entry_id, original_delta)) => (-original_delta, Some(entry_id.to_string())),
        None => (0, None),
    };
    Ok(RewardTransitionOutcome {
        new_status: RewardStatus::Revoked,
        ledger_entry: LedgerEntry {
            tenant_id,
            delta,
            reason: "REFERRAL_REVOKE",
            reference_type: "referral_reward",
            reference_id: reward_id.to_string(),
            reverses_entry_id,
        },
    })
}

// =============================================================================
// §75.3 Anti-Abuse
// =============================================================================

/// §75.3 cap configuration — external config, not hardcoded constants (repo CLAUDE.md 硬
/// 边界: 禁止硬编码 quota 数/TTL/限流阈值). The caller (adapter layer) supplies these from
/// `config/features.toml` or similar; this module only evaluates them.
#[derive(Debug, Clone, Copy)]
pub struct AntiAbuseCaps {
    pub per_account_cap: u32,
    pub per_ip_cap: u32,
    pub per_device_cap: u32,
    pub lifetime_cap: u32,
    pub cooldown: std::time::Duration,
}

/// Everything [`check_anti_abuse`] needs about the attempted attribution — counts the
/// caller has already looked up (from `control.referral_attributions`), not something this
/// pure function queries itself.
#[derive(Debug, Clone, Copy)]
pub struct AntiAbuseContext {
    pub referrer_tenant_id: TenantId,
    pub referred_tenant_id: TenantId,
    pub attributions_for_referrer_account: u32,
    pub attributions_for_ip: u32,
    pub attributions_for_device: u32,
    pub referrer_lifetime_rewards: u32,
    pub last_attribution_at: Option<SystemTime>,
    pub now: SystemTime,
}

/// §75.3: "邀请/奖励属于 OWASP Sensitive Business Flows，需要独立风控" — self-referral
/// first (structural, cannot be raised by any cap tuning), then each of the five caps
/// independently. First violation wins; callers that want every violated cap reported
/// should call this once per cap category instead — the function is capped, not
/// exhaustive-report, so a single retry after fixing one cap does not silently skip a
/// second one still in force.
pub fn check_anti_abuse(caps: &AntiAbuseCaps, ctx: &AntiAbuseContext) -> Result<(), ErrorCode> {
    // §75.2 "no self-referral" — checked here as the primary enforcement point (the
    // migration's `CHECK (tenant_id <> referred_tenant_id)` on referral_attributions is
    // only a same-direct-tenant backstop, see migrations/0032's own comment on that table).
    if ctx.referrer_tenant_id == ctx.referred_tenant_id {
        return Err(ErrorCode::Forbidden);
    }
    if let Some(last) = ctx.last_attribution_at
        && let Ok(elapsed) = ctx.now.duration_since(last)
        && elapsed < caps.cooldown
    {
        return Err(ErrorCode::RateLimited);
    }
    if ctx.attributions_for_referrer_account >= caps.per_account_cap
        || ctx.attributions_for_ip >= caps.per_ip_cap
        || ctx.attributions_for_device >= caps.per_device_cap
    {
        return Err(ErrorCode::Forbidden);
    }
    if ctx.referrer_lifetime_rewards >= caps.lifetime_cap {
        return Err(ErrorCode::QuotaExhausted);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn invitation(
        status: InvitationStatus,
        expires_in: Duration,
        now: SystemTime,
    ) -> (InvitationRecord, String) {
        let (raw, hash) = generate_invitation_token();
        let record = InvitationRecord {
            tenant_id: TenantId::new(),
            invited_email: "invitee@example.com".to_string(),
            role: "member".to_string(),
            token_hash: hash,
            expires_at: now + expires_in,
            status,
        };
        (record, raw)
    }

    // ---- §75.1 accept_invitation: happy path + every field independently tampered ----

    #[test]
    fn accept_succeeds_when_every_field_matches() {
        let now = SystemTime::now();
        let (record, raw) = invitation(InvitationStatus::Pending, Duration::from_secs(3600), now);
        let req = AcceptInvitationRequest {
            raw_token: &raw,
            claimed_tenant_id: record.tenant_id,
            claimed_role: "member",
            claimed_email: "invitee@example.com",
        };
        assert!(accept_invitation(&record, &req, now).is_ok());
    }

    #[test]
    fn accept_rejects_tampered_role() {
        let now = SystemTime::now();
        let (record, raw) = invitation(InvitationStatus::Pending, Duration::from_secs(3600), now);
        let req = AcceptInvitationRequest {
            raw_token: &raw,
            claimed_tenant_id: record.tenant_id,
            claimed_role: "owner", // tampered: row says "member"
            claimed_email: "invitee@example.com",
        };
        assert_eq!(
            accept_invitation(&record, &req, now).unwrap_err(),
            ErrorCode::Forbidden
        );
    }

    #[test]
    fn accept_rejects_tampered_tenant() {
        let now = SystemTime::now();
        let (record, raw) = invitation(InvitationStatus::Pending, Duration::from_secs(3600), now);
        let req = AcceptInvitationRequest {
            raw_token: &raw,
            claimed_tenant_id: TenantId::new(), // tampered: a different tenant
            claimed_role: "member",
            claimed_email: "invitee@example.com",
        };
        assert_eq!(
            accept_invitation(&record, &req, now).unwrap_err(),
            ErrorCode::Forbidden
        );
    }

    #[test]
    fn accept_rejects_tampered_email() {
        let now = SystemTime::now();
        let (record, raw) = invitation(InvitationStatus::Pending, Duration::from_secs(3600), now);
        let req = AcceptInvitationRequest {
            raw_token: &raw,
            claimed_tenant_id: record.tenant_id,
            claimed_role: "member",
            claimed_email: "attacker@example.com", // tampered
        };
        assert_eq!(
            accept_invitation(&record, &req, now).unwrap_err(),
            ErrorCode::Forbidden
        );
    }

    #[test]
    fn accept_rejects_wrong_token() {
        let now = SystemTime::now();
        let (record, _raw) = invitation(InvitationStatus::Pending, Duration::from_secs(3600), now);
        let (_other_raw_owner, forged_raw) = generate_invitation_token();
        let req = AcceptInvitationRequest {
            raw_token: &forged_raw,
            claimed_tenant_id: record.tenant_id,
            claimed_role: "member",
            claimed_email: "invitee@example.com",
        };
        assert_eq!(
            accept_invitation(&record, &req, now).unwrap_err(),
            ErrorCode::Forbidden
        );
    }

    #[test]
    fn accept_rejects_already_accepted_status() {
        let now = SystemTime::now();
        let (record, raw) = invitation(InvitationStatus::Accepted, Duration::from_secs(3600), now);
        let req = AcceptInvitationRequest {
            raw_token: &raw,
            claimed_tenant_id: record.tenant_id,
            claimed_role: "member",
            claimed_email: "invitee@example.com",
        };
        assert_eq!(
            accept_invitation(&record, &req, now).unwrap_err(),
            ErrorCode::Conflict
        );
    }

    #[test]
    fn accept_rejects_expired_invitation() {
        let now = SystemTime::now();
        let (record, raw) = invitation(
            InvitationStatus::Pending,
            Duration::from_secs(0),
            now - Duration::from_secs(1),
        );
        let req = AcceptInvitationRequest {
            raw_token: &raw,
            claimed_tenant_id: record.tenant_id,
            claimed_role: "member",
            claimed_email: "invitee@example.com",
        };
        assert_eq!(
            accept_invitation(&record, &req, now).unwrap_err(),
            ErrorCode::Forbidden
        );
    }

    // ---- §75.2 reward state machine: registration-time reward is structurally rejected ----

    #[test]
    fn grant_reward_from_attributed_is_rejected() {
        // "注册即领奖被拒（未过 QUALIFIED）": ATTRIBUTED is registration's own starting
        // state — MATURING is the only legal predecessor of GRANTED.
        let err =
            grant_reward(RewardStatus::Attributed, TenantId::new(), "reward-1", 500).unwrap_err();
        assert_eq!(err, ErrorCode::Conflict);
    }

    #[test]
    fn grant_reward_from_qualified_is_rejected() {
        // one hop closer than ATTRIBUTED, still not MATURING — still rejected.
        let err =
            grant_reward(RewardStatus::Qualified, TenantId::new(), "reward-1", 500).unwrap_err();
        assert_eq!(err, ErrorCode::Conflict);
    }

    #[test]
    fn grant_reward_from_maturing_succeeds_and_mints_positive_ledger_entry() {
        let tenant = TenantId::new();
        let outcome = grant_reward(RewardStatus::Maturing, tenant, "reward-1", 500).unwrap();
        assert_eq!(outcome.new_status, RewardStatus::Granted);
        assert_eq!(outcome.ledger_entry.delta, 500);
        assert_eq!(outcome.ledger_entry.reverses_entry_id, None);
        assert_eq!(outcome.ledger_entry.tenant_id, tenant);
    }

    #[test]
    fn grant_reward_rejects_non_positive_credits() {
        let err = grant_reward(RewardStatus::Maturing, TenantId::new(), "reward-1", 0).unwrap_err();
        assert_eq!(err, ErrorCode::InvalidInput);
    }

    // ---- revoke produces a reversal entry, never an edited balance ----

    #[test]
    fn revoke_from_granted_produces_exact_negation_reversal_entry() {
        let tenant = TenantId::new();
        let grant = grant_reward(RewardStatus::Maturing, tenant, "reward-1", 500).unwrap();
        let original_entry_id = "ledger-entry-original";

        let outcome = revoke_reward(
            RewardStatus::Granted,
            tenant,
            "reward-1",
            Some((original_entry_id, grant.ledger_entry.delta)),
        )
        .unwrap();

        assert_eq!(outcome.new_status, RewardStatus::Revoked);
        // exact negation, not a re-derived/rounded amount.
        assert_eq!(outcome.ledger_entry.delta, -500);
        assert_eq!(
            outcome.ledger_entry.reverses_entry_id,
            Some(original_entry_id.to_string())
        );
        // the two entries are distinct values (a fresh row), not the same struct mutated.
        assert_ne!(outcome.ledger_entry, grant.ledger_entry);
    }

    #[test]
    fn revoke_from_attributed_is_rejected() {
        let err =
            revoke_reward(RewardStatus::Attributed, TenantId::new(), "reward-1", None).unwrap_err();
        assert_eq!(err, ErrorCode::Conflict);
    }

    // ---- §75.3 anti-abuse ----

    fn abuse_caps() -> AntiAbuseCaps {
        AntiAbuseCaps {
            per_account_cap: 10,
            per_ip_cap: 5,
            per_device_cap: 5,
            lifetime_cap: 20,
            cooldown: Duration::from_secs(60),
        }
    }

    #[test]
    fn self_referral_is_rejected_even_with_zero_prior_activity() {
        let same_tenant = TenantId::new();
        let ctx = AntiAbuseContext {
            referrer_tenant_id: same_tenant,
            referred_tenant_id: same_tenant,
            attributions_for_referrer_account: 0,
            attributions_for_ip: 0,
            attributions_for_device: 0,
            referrer_lifetime_rewards: 0,
            last_attribution_at: None,
            now: SystemTime::now(),
        };
        assert_eq!(
            check_anti_abuse(&abuse_caps(), &ctx).unwrap_err(),
            ErrorCode::Forbidden
        );
    }

    #[test]
    fn distinct_tenants_under_every_cap_pass() {
        let ctx = AntiAbuseContext {
            referrer_tenant_id: TenantId::new(),
            referred_tenant_id: TenantId::new(),
            attributions_for_referrer_account: 1,
            attributions_for_ip: 1,
            attributions_for_device: 1,
            referrer_lifetime_rewards: 1,
            last_attribution_at: None,
            now: SystemTime::now(),
        };
        assert!(check_anti_abuse(&abuse_caps(), &ctx).is_ok());
    }

    #[test]
    fn cooldown_still_running_is_rate_limited() {
        let now = SystemTime::now();
        let ctx = AntiAbuseContext {
            referrer_tenant_id: TenantId::new(),
            referred_tenant_id: TenantId::new(),
            attributions_for_referrer_account: 0,
            attributions_for_ip: 0,
            attributions_for_device: 0,
            referrer_lifetime_rewards: 0,
            last_attribution_at: Some(now - Duration::from_secs(5)),
            now,
        };
        assert_eq!(
            check_anti_abuse(&abuse_caps(), &ctx).unwrap_err(),
            ErrorCode::RateLimited
        );
    }

    #[test]
    fn per_ip_cap_reached_is_forbidden() {
        let ctx = AntiAbuseContext {
            referrer_tenant_id: TenantId::new(),
            referred_tenant_id: TenantId::new(),
            attributions_for_referrer_account: 0,
            attributions_for_ip: 5, // == per_ip_cap
            attributions_for_device: 0,
            referrer_lifetime_rewards: 0,
            last_attribution_at: None,
            now: SystemTime::now(),
        };
        assert_eq!(
            check_anti_abuse(&abuse_caps(), &ctx).unwrap_err(),
            ErrorCode::Forbidden
        );
    }

    #[test]
    fn lifetime_cap_reached_is_quota_exhausted() {
        let ctx = AntiAbuseContext {
            referrer_tenant_id: TenantId::new(),
            referred_tenant_id: TenantId::new(),
            attributions_for_referrer_account: 0,
            attributions_for_ip: 0,
            attributions_for_device: 0,
            referrer_lifetime_rewards: 20, // == lifetime_cap
            last_attribution_at: None,
            now: SystemTime::now(),
        };
        assert_eq!(
            check_anti_abuse(&abuse_caps(), &ctx).unwrap_err(),
            ErrorCode::QuotaExhausted
        );
    }

    // ---- token hashing sanity ----

    #[test]
    fn generated_token_hash_matches_hash_token() {
        let (raw, hash) = generate_invitation_token();
        assert_eq!(hash_token(&raw), hash);
    }

    #[test]
    fn accept_invitation_request_debug_redacts_raw_token() {
        let (raw, _) = generate_invitation_token();
        let req = AcceptInvitationRequest {
            raw_token: &raw,
            claimed_tenant_id: TenantId::new(),
            claimed_role: "member",
            claimed_email: "invitee@example.com",
        };
        let debug_output = format!("{req:?}");
        assert!(
            !debug_output.contains(&raw),
            "Debug output must not contain the plaintext bearer token"
        );
        assert!(debug_output.contains(&hash_token(&raw)));
    }

    #[test]
    fn two_generated_tokens_are_distinct() {
        let (raw_a, _) = generate_invitation_token();
        let (raw_b, _) = generate_invitation_token();
        assert_ne!(raw_a, raw_b);
    }
}
