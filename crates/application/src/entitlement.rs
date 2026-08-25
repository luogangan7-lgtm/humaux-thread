//! H5: §76 Entitlement Projector（Phase 2 wave 实现；判据出处见 spec 家章）.
//!
//! §76 pipeline: Payment/Subscription feeds Base Entitlements, joined by Promotion/Coupon,
//! Referral Reward, and Manual Admin Grant; all four flow into one Effective Entitlement
//! Snapshot, which alone feeds Quota Window / Request Authorization.
//!
//! **Frozen (§76, rustdoc-enforced):** "运行时不读取散落的 coupon/referral 表计算权限" —
//! the *only* legitimate output of this module for an authorization/quota decision is
//! [`EntitlementSnapshot`], produced by [`project`]. There is no function here that answers
//! "what does grant X say" or "what is tenant Y's raw referral-reward-derived grant" in
//! isolation — [`EntitlementGrant`] exists only as `project`'s input type, never as
//! something a request-authorization code path queries directly. Any future authz/quota
//! module that imports `EntitlementGrant` (or reaches past this module into
//! `control.referral_rewards`/`control.credit_ledger`/a coupon table) to decide a live
//! request is violating this freeze — the fix is always "call [`project`] and read the
//! snapshot", never "add a second read path". Enforced at compile time only within this
//! crate: `crates/application` has no SQLx dependency at all (see this crate's `Cargo.toml`
//! — Phase 2 identity/entitlement modules are pure logic, not database-bound), so no
//! runtime code that could touch those raw tables exists in this crate to begin with; the
//! rule this comment freezes is for whichever adapter crate wires persistence later.

use humaux_domain::ids::TenantId;
use std::collections::BTreeMap;
use std::time::SystemTime;

/// §76: `source = PLAN | PROMOTION | REFERRAL | ADMIN | TRIAL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantSource {
    Plan,
    Promotion,
    Referral,
    Admin,
    Trial,
}

impl GrantSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Plan => "PLAN",
            Self::Promotion => "PROMOTION",
            Self::Referral => "REFERRAL",
            Self::Admin => "ADMIN",
            Self::Trial => "TRIAL",
        }
    }
}

/// One `control.entitlement_grants` row — §76's raw per-source fact, [`project`]'s only
/// input type. Deliberately not `pub` outside constructing a `Vec<EntitlementGrant>` to
/// hand to `project`: nothing in this module reads a single grant's `value` directly as an
/// authorization answer (see the module freeze note above).
#[derive(Debug, Clone)]
pub struct EntitlementGrant {
    pub grant_id: String,
    pub tenant_id: TenantId,
    pub source: GrantSource,
    pub feature: String,
    pub value: serde_json::Value,
    pub valid_from: SystemTime,
    pub valid_until: Option<SystemTime>,
    pub priority: i32,
    pub revoked_at: Option<SystemTime>,
}

impl EntitlementGrant {
    /// A grant is active exactly when: not revoked, `now` is within
    /// `[valid_from, valid_until)` (`valid_until = None` means no expiry). Half-open on the
    /// upper bound so a grant expiring at exactly `now` has already lapsed — matches
    /// `[valid_from, valid_until)` interval-membership convention used elsewhere in this
    /// workspace (temporal filtering, §9).
    fn is_active_at(&self, now: SystemTime) -> bool {
        self.revoked_at.is_none()
            && self.valid_from <= now
            && self.valid_until.is_none_or(|until| now < until)
    }
}

/// §76 Effective Entitlement Snapshot — the sole read surface for Quota Window / Request
/// Authorization (`migrations/0033_entitlement_projector.sql`'s `control.
/// entitlement_snapshots`, one row per tenant). `source_grant_ids` is audit/debug metadata
/// for the Projector itself, not a second way to look a feature up — `effective` is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntitlementSnapshot {
    pub tenant_id: TenantId,
    pub effective: BTreeMap<String, serde_json::Value>,
    pub source_grant_ids: Vec<String>,
    pub computed_at: SystemTime,
}

/// §76 Entitlement Projector — the sole constructor of [`EntitlementSnapshot`] (this
/// crate's "唯一构造点" convention, repo CLAUDE.md). For each `feature` name across every
/// grant active at `now`, keeps the single winning grant: highest `priority` first, ties
/// broken by the later `valid_from` (the more-recently-granted entitlement wins a same-
/// priority tie — deterministic and documented here, not left to iteration order).
///
/// Grants belonging to a different tenant than `tenant_id` are silently skipped rather than
/// trusted from the input slice — defense in depth matching §6.1.1's "调用方永远自己 AND
/// tenant filter" discipline, even though this function's own contract already puts that
/// filtering burden on the caller.
pub fn project(
    tenant_id: TenantId,
    grants: &[EntitlementGrant],
    now: SystemTime,
) -> EntitlementSnapshot {
    let mut winners: BTreeMap<&str, &EntitlementGrant> = BTreeMap::new();

    for grant in grants {
        if grant.tenant_id != tenant_id || !grant.is_active_at(now) {
            continue;
        }
        match winners.get(grant.feature.as_str()) {
            None => {
                winners.insert(&grant.feature, grant);
            }
            Some(current) => {
                let grant_wins = grant.priority > current.priority
                    || (grant.priority == current.priority
                        && grant.valid_from > current.valid_from);
                if grant_wins {
                    winners.insert(&grant.feature, grant);
                }
            }
        }
    }

    let effective = winners
        .iter()
        .map(|(feature, grant)| (feature.to_string(), grant.value.clone()))
        .collect();
    let mut source_grant_ids: Vec<String> = winners.values().map(|g| g.grant_id.clone()).collect();
    source_grant_ids.sort();

    EntitlementSnapshot {
        tenant_id,
        effective,
        source_grant_ids,
        computed_at: now,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Duration;

    fn grant(
        tenant_id: TenantId,
        feature: &str,
        priority: i32,
        source: GrantSource,
        valid_from: SystemTime,
        valid_until: Option<SystemTime>,
    ) -> EntitlementGrant {
        EntitlementGrant {
            grant_id: format!("{feature}-{priority}-{source:?}"),
            tenant_id,
            source,
            feature: feature.to_string(),
            value: json!(true),
            valid_from,
            valid_until,
            priority,
            revoked_at: None,
        }
    }

    #[test]
    fn active_grant_appears_in_snapshot() {
        let tenant = TenantId::new();
        let now = SystemTime::now();
        let grants = vec![grant(
            tenant,
            "advanced_search",
            0,
            GrantSource::Plan,
            now - Duration::from_secs(60),
            None,
        )];
        let snapshot = project(tenant, &grants, now);
        assert_eq!(
            snapshot.effective.get("advanced_search"),
            Some(&json!(true))
        );
    }

    #[test]
    fn expired_grant_does_not_enter_snapshot() {
        let tenant = TenantId::new();
        let now = SystemTime::now();
        let grants = vec![grant(
            tenant,
            "advanced_search",
            0,
            GrantSource::Trial,
            now - Duration::from_secs(3600),
            Some(now - Duration::from_secs(60)), // valid_until already in the past
        )];
        let snapshot = project(tenant, &grants, now);
        assert!(snapshot.effective.is_empty());
    }

    #[test]
    fn not_yet_valid_grant_does_not_enter_snapshot() {
        let tenant = TenantId::new();
        let now = SystemTime::now();
        let grants = vec![grant(
            tenant,
            "beta_feature",
            0,
            GrantSource::Promotion,
            now + Duration::from_secs(60), // starts in the future
            None,
        )];
        let snapshot = project(tenant, &grants, now);
        assert!(snapshot.effective.is_empty());
    }

    #[test]
    fn revoked_grant_does_not_enter_snapshot_even_within_valid_window() {
        let tenant = TenantId::new();
        let now = SystemTime::now();
        let mut g = grant(
            tenant,
            "advanced_search",
            0,
            GrantSource::Admin,
            now - Duration::from_secs(60),
            None,
        );
        g.revoked_at = Some(now - Duration::from_secs(1));
        let snapshot = project(tenant, &[g], now);
        assert!(snapshot.effective.is_empty());
    }

    #[test]
    fn grant_expiring_exactly_at_now_is_excluded_half_open_interval() {
        let tenant = TenantId::new();
        let now = SystemTime::now();
        let grants = vec![grant(
            tenant,
            "advanced_search",
            0,
            GrantSource::Trial,
            now - Duration::from_secs(60),
            Some(now), // valid_until == now
        )];
        let snapshot = project(tenant, &grants, now);
        assert!(snapshot.effective.is_empty());
    }

    #[test]
    fn higher_priority_grant_wins_conflict() {
        let tenant = TenantId::new();
        let now = SystemTime::now();
        let base = now - Duration::from_secs(60);
        let plan_grant = grant(tenant, "seats", 0, GrantSource::Plan, base, None);
        let mut admin_override = grant(tenant, "seats", 10, GrantSource::Admin, base, None);
        admin_override.value = json!(50);
        let snapshot = project(tenant, &[plan_grant, admin_override], now);
        assert_eq!(snapshot.effective.get("seats"), Some(&json!(50)));
    }

    #[test]
    fn equal_priority_tie_broken_by_later_valid_from() {
        let tenant = TenantId::new();
        let now = SystemTime::now();
        let mut older = grant(
            tenant,
            "seats",
            0,
            GrantSource::Promotion,
            now - Duration::from_secs(120),
            None,
        );
        older.value = json!(10);
        let mut newer = grant(
            tenant,
            "seats",
            0,
            GrantSource::Promotion,
            now - Duration::from_secs(30),
            None,
        );
        newer.value = json!(20);
        let snapshot = project(tenant, &[older, newer], now);
        assert_eq!(snapshot.effective.get("seats"), Some(&json!(20)));
    }

    #[test]
    fn grant_belonging_to_a_different_tenant_is_never_projected() {
        let tenant = TenantId::new();
        let other_tenant = TenantId::new();
        let now = SystemTime::now();
        let grants = vec![grant(
            other_tenant,
            "advanced_search",
            0,
            GrantSource::Plan,
            now - Duration::from_secs(60),
            None,
        )];
        let snapshot = project(tenant, &grants, now);
        assert!(snapshot.effective.is_empty());
        assert_eq!(snapshot.tenant_id, tenant);
    }

    #[test]
    fn source_grant_ids_records_exactly_the_winning_grants() {
        let tenant = TenantId::new();
        let now = SystemTime::now();
        let base = now - Duration::from_secs(60);
        let g1 = grant(tenant, "seats", 0, GrantSource::Plan, base, None);
        let g2 = grant(tenant, "advanced_search", 0, GrantSource::Trial, base, None);
        let expected_ids = {
            let mut v = vec![g1.grant_id.clone(), g2.grant_id.clone()];
            v.sort();
            v
        };
        let snapshot = project(tenant, &[g1, g2], now);
        assert_eq!(snapshot.source_grant_ids, expected_ids);
    }
}
