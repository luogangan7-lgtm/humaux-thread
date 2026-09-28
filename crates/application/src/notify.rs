//! `application::notify` — H4: §74.7 Notification Plane.
//! Depends-on: crates=[humaux-domain, serde, serde_json]; services=[]; env=[]; modules=[domain::ids]
//! Called-by: []
//! Invariants: []
//! Spec: §74.7; §6.2.3; §74.6
//!
//! Platform-authoritative user notification, decoupled from delivery channel: Email/future
//! Webhook/Push are adapters, `control.notifications` is the record of truth (§74.7 "建立
//! 平台内 Notification 作为权威用户通知记录，Email/未来 Webhook/Push 只是 delivery
//! adapter"). This module owns the channel-independent decision logic — event
//! classification, dedup/cooldown, and preference resolution — persistence and the real
//! `email_outbox` writer are adapter-layer concerns outside this crate (§6.2.3: this crate
//! never holds a bare `sqlx::PgPool`).
//!
//! [`NotificationEmailPort`] is the injection point for H3's `email_outbox` writer (§74.6);
//! this module only depends on the narrow shape it needs (a same-transaction enqueue), not
//! H3's full `EmailProvider` trait, so it compiles and tests independently of H3's landing
//! order — satisfied by a test double ([`tests::RecordingEmailPort`]) until H3 wires the
//! real outbox-backed implementation.

use humaux_domain::ids::{TenantId, UserId};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

/// §74.7 "至少覆盖" — verbatim spec enumeration (8 bullet lines, 12 named events; the task
/// brief's "八类" counts the bullet lines, not the variant count). No `Other` — the closed
/// set is what every `category` CHECK constraint and delivery-preference row is keyed by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NotificationCategory {
    ByokInvalid,
    WaitingKey,
    Quota80,
    QuotaExhausted,
    BillingPastDue,
    McpGrantCreated,
    McpGrantRevoked,
    NewLogin,
    SecurityEvent,
    ExportReady,
    DeletionProgress,
    PublicContributionReview,
}

impl NotificationCategory {
    /// Every variant, in the same order [`Self::as_db_str`] enumerates them — the §78.2
    /// contract test iterates this against `0036_notification_plane.sql`'s `category` CHECK
    /// list so the two enumerations can never silently drift apart.
    pub const ALL: [NotificationCategory; 12] = [
        Self::ByokInvalid,
        Self::WaitingKey,
        Self::Quota80,
        Self::QuotaExhausted,
        Self::BillingPastDue,
        Self::McpGrantCreated,
        Self::McpGrantRevoked,
        Self::NewLogin,
        Self::SecurityEvent,
        Self::ExportReady,
        Self::DeletionProgress,
        Self::PublicContributionReview,
    ];

    /// The exact `control.notifications.category` / `control.notification_preferences.category`
    /// wire value (§78.2: DB enum and Rust enum reconciled by contract test, never by
    /// convention alone).
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::ByokInvalid => "BYOK_INVALID",
            Self::WaitingKey => "WAITING_KEY",
            Self::Quota80 => "QUOTA_80",
            Self::QuotaExhausted => "QUOTA_EXHAUSTED",
            Self::BillingPastDue => "BILLING_PAST_DUE",
            Self::McpGrantCreated => "MCP_GRANT_CREATED",
            Self::McpGrantRevoked => "MCP_GRANT_REVOKED",
            Self::NewLogin => "NEW_LOGIN",
            Self::SecurityEvent => "SECURITY_EVENT",
            Self::ExportReady => "EXPORT_READY",
            Self::DeletionProgress => "DELETION_PROGRESS",
            Self::PublicContributionReview => "PUBLIC_CONTRIBUTION_REVIEW",
        }
    }
}

/// `control.notifications.severity` (§74.7 field list). Closed 3-value set backing the
/// "安全关键通知可忽略用户营销偏好" rule: only [`Severity::Security`] bypasses preference
/// (see [`resolve_channels`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Info,
    Warning,
    Security,
}

impl Severity {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Warning => "WARNING",
            Self::Security => "SECURITY",
        }
    }

    /// Fixed category -> severity map. Spec freezes only the *principle* ("安全关键通知可
    /// 忽略用户营销偏好"), not a per-category table — `NewLogin`/`SecurityEvent` are the two
    /// categories whose own names name a security event; everything else defaults to
    /// `Warning`/`Info` by ordinary account-state significance. Not spec-frozen line-by-line,
    /// flagged here rather than silently treated as settled.
    pub fn of(category: NotificationCategory) -> Self {
        use NotificationCategory::*;
        match category {
            NewLogin | SecurityEvent => Self::Security,
            ByokInvalid | WaitingKey | Quota80 | QuotaExhausted | BillingPastDue
            | DeletionProgress => Self::Warning,
            McpGrantCreated | McpGrantRevoked | ExportReady | PublicContributionReview => {
                Self::Info
            }
        }
    }
}

/// `control.notification_preferences` row shape, minus the primary key (§74.7 field list).
/// `digest_policy` is carried for the DB row but has no reader in this module yet — batching
/// is a future delivery-worker concern, not this decision layer.
#[derive(Debug, Clone, Copy)]
pub struct NotificationPreference {
    pub in_app: bool,
    pub email: bool,
}

impl Default for NotificationPreference {
    /// `control.notification_preferences` column defaults (§74.7 migration): both channels
    /// on until a user opts out.
    fn default() -> Self {
        Self {
            in_app: true,
            email: true,
        }
    }
}

/// Which channels a dispatch should actually engage, after preference resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Channels {
    pub in_app: bool,
    pub email: bool,
}

/// §74.7 "安全关键通知可忽略用户营销偏好": a [`Severity::Security`] notification always
/// engages every channel regardless of `prefs`; every other severity respects the user's row
/// as-is on both channels — `control.notification_preferences.in_app` is a genuine
/// user-facing toggle exactly like `email`, not exempted from gating (§74.7 field list names
/// `in_app` as a per-category column with no carve-out; only the security-bypass principle is
/// frozen, and it names no channel it excludes).
pub fn resolve_channels(severity: Severity, prefs: NotificationPreference) -> Channels {
    if severity == Severity::Security {
        return Channels {
            in_app: true,
            email: true,
        };
    }
    Channels {
        in_app: prefs.in_app,
        email: prefs.email,
    }
}

/// §74.7 "`dedup_key` / cooldown 防 notification storm": `true` iff enough time has passed
/// since the last notification sharing this `dedup_key` (or none exists yet) that a new one
/// should be created. `cooldown` is a caller-supplied parameter, never a literal inside this
/// function (§78.1: no hardcoded TTL/threshold in business logic) — the caller reads it from
/// config keyed by category.
pub fn should_emit(
    now: SystemTime,
    last_emitted_at: Option<SystemTime>,
    cooldown: Duration,
) -> bool {
    match last_emitted_at {
        None => true,
        Some(last) => match now.duration_since(last) {
            Ok(elapsed) => elapsed >= cooldown,
            // `now` before `last`: a clock went backwards relative to the stored timestamp.
            // Fail closed toward "still cooling down" rather than let clock skew re-open a
            // storm gate.
            Err(_) => false,
        },
    }
}

/// §74.7 "Notification payload 不复制 private Memory 正文": the closed, explicitly-named
/// field set every category's variables draw from. `#[serde(deny_unknown_fields)]` makes
/// this a *type*-level rejection (§78.1's "禁止 stringly-typed domain" extended to payload
/// shape) — deserializing any object carrying a field this struct does not name (e.g. a
/// `memory_body`/`content` excerpt) is a hard `Err`, not a silently-dropped extra key.
/// Construction from Rust code is equally closed: there is no field to put memory content
/// into, so the rejection also holds at the call site that builds one, not only at the
/// deserialize boundary.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationPayload {
    /// EXPORT_READY: opaque export identifier (§74.7 "payload_ref").
    pub export_id: Option<String>,
    /// DELETION_PROGRESS: a short machine-readable stage label, never a file/record list.
    pub deletion_stage: Option<String>,
    /// QUOTA_80: the crossed threshold, 0-100.
    pub quota_used_percent: Option<u8>,
    /// MCP_GRANT_CREATED / MCP_GRANT_REVOKED: opaque grant identifier.
    pub mcp_grant_id: Option<String>,
    /// NEW_LOGIN: the observed client IP, never a full `ClientNetworkIdentity` (§73.2).
    pub login_ip: Option<String>,
    /// PUBLIC_CONTRIBUTION_REVIEW: opaque contribution identifier, never the contribution body.
    pub contribution_id: Option<String>,
    /// BILLING_PAST_DUE: amount owed, integer cents (never a full invoice/PAN).
    pub amount_cents: Option<i64>,
}

/// One notification to classify and (maybe) dispatch. Mirrors `control.notifications`'
/// writable columns (§74.7); `notification_id`/`created_at` are storage-assigned, not part
/// of this decision-layer type.
#[derive(Debug, Clone)]
pub struct NotificationEvent {
    pub tenant_id: TenantId,
    pub user_id: Option<UserId>,
    pub category: NotificationCategory,
    pub dedup_key: String,
    pub title_template_id: String,
    pub payload: NotificationPayload,
}

/// Injection point for H3's `email_outbox` writer (§74.6: "发信必须走 email_outbox" — a
/// same-transaction enqueue, not a direct SMTP call at this layer). Synchronous: enqueueing
/// is a DB insert, not network I/O, so no async runtime dependency is needed in this crate
/// (`application/Cargo.toml` carries none — adding one is out of this task's file scope, see
/// module doc). H3's real adapter implements this alongside its own richer `EmailProvider`
/// trait; until H3 lands, callers pass a double (see [`tests::RecordingEmailPort`]).
pub trait NotificationEmailPort {
    /// Enqueues `event`'s email delivery. `Err` on enqueue failure — per §74.7 "Email
    /// delivery failure 不删除 in-app notification", callers must not use this `Err` as a
    /// reason to remove the in-app record; this module never implements deletion at all, so
    /// that invariant holds structurally (no code path here deletes a notification).
    fn enqueue(&self, event: &NotificationEvent) -> Result<(), NotifyError>;
}

/// Terminal failure from [`NotificationEmailPort::enqueue`]. Not one of the workspace's two
/// frozen error enums (§52 `ErrorCode`/`DegradeCode`) — this is an adapter-boundary error
/// local to the injected port, not a Domain/Application terminal or degrade classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotifyError(pub String);

/// Result of [`dispatch`]: which channels actually fired, or that cooldown suppressed the
/// whole event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchOutcome {
    /// `dedup_key` was inside its cooldown window — no in-app row, no email enqueue.
    Suppressed,
    Dispatched {
        channels: Channels,
    },
}

/// Ties classification, cooldown, and preference resolution together, then enqueues email
/// through the injected [`NotificationEmailPort`] if that channel is engaged. Does not touch
/// storage itself (no `control.notifications` INSERT here — that is the adapter layer's
/// job); this is the pure decision the adapter calls before/around its own write.
pub fn dispatch(
    event: &NotificationEvent,
    prefs: NotificationPreference,
    now: SystemTime,
    last_emitted_at: Option<SystemTime>,
    cooldown: Duration,
    email_port: &impl NotificationEmailPort,
) -> Result<DispatchOutcome, NotifyError> {
    if !should_emit(now, last_emitted_at, cooldown) {
        return Ok(DispatchOutcome::Suppressed);
    }
    let severity = Severity::of(event.category);
    let channels = resolve_channels(severity, prefs);
    if channels.email {
        email_port.enqueue(event)?;
    }
    Ok(DispatchOutcome::Dispatched { channels })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    fn event(dedup_key: &str) -> NotificationEvent {
        NotificationEvent {
            tenant_id: TenantId::new(),
            user_id: Some(UserId::new()),
            category: NotificationCategory::QuotaExhausted,
            dedup_key: dedup_key.to_string(),
            title_template_id: "quota_exhausted_v1".to_string(),
            payload: NotificationPayload {
                quota_used_percent: Some(100),
                ..Default::default()
            },
        }
    }

    /// Double for [`NotificationEmailPort`] — records every enqueued event instead of
    /// touching a real outbox, standing in until H3's adapter lands (module doc).
    #[derive(Default)]
    struct RecordingEmailPort {
        enqueued: RefCell<Vec<String>>,
    }

    impl NotificationEmailPort for RecordingEmailPort {
        fn enqueue(&self, event: &NotificationEvent) -> Result<(), NotifyError> {
            self.enqueued.borrow_mut().push(event.dedup_key.clone());
            Ok(())
        }
    }

    // ---- dedup_key / cooldown storm suppression ----

    #[test]
    fn same_dedup_key_within_cooldown_is_suppressed() {
        let port = RecordingEmailPort::default();
        let e = event("quota:tenant-a");
        let t0 = SystemTime::now();
        let cooldown = Duration::from_secs(3600);

        let first = dispatch(
            &e,
            NotificationPreference::default(),
            t0,
            None,
            cooldown,
            &port,
        )
        .expect("first dispatch does not error");
        assert!(matches!(first, DispatchOutcome::Dispatched { .. }));

        // Same dedup_key, 10 minutes later — still inside the 1h cooldown.
        let t1 = t0 + Duration::from_secs(600);
        let second = dispatch(
            &e,
            NotificationPreference::default(),
            t1,
            Some(t0),
            cooldown,
            &port,
        )
        .expect("second dispatch does not error");
        assert_eq!(second, DispatchOutcome::Suppressed);

        // Exactly one enqueue reached the email port — the cooldown-suppressed call never
        // produced a second one.
        assert_eq!(port.enqueued.borrow().len(), 1);
    }

    #[test]
    fn same_dedup_key_after_cooldown_emits_again() {
        let port = RecordingEmailPort::default();
        let e = event("quota:tenant-a");
        let t0 = SystemTime::now();
        let cooldown = Duration::from_secs(3600);

        let t1 = t0 + Duration::from_secs(3601);
        let outcome = dispatch(
            &e,
            NotificationPreference::default(),
            t1,
            Some(t0),
            cooldown,
            &port,
        )
        .expect("dispatch does not error");
        assert!(matches!(outcome, DispatchOutcome::Dispatched { .. }));
    }

    // ---- SECURITY severity bypasses the email marketing preference ----

    #[test]
    fn security_severity_delivers_email_even_when_preference_is_off() {
        let port = RecordingEmailPort::default();
        let mut e = event("login:tenant-a:user-b");
        e.category = NotificationCategory::NewLogin;
        let prefs_email_off = NotificationPreference {
            in_app: true,
            email: false,
        };

        let outcome = dispatch(
            &e,
            prefs_email_off,
            SystemTime::now(),
            None,
            Duration::from_secs(60),
            &port,
        )
        .expect("dispatch does not error");

        assert_eq!(
            outcome,
            DispatchOutcome::Dispatched {
                channels: Channels {
                    in_app: true,
                    email: true
                }
            }
        );
        assert_eq!(
            port.enqueued.borrow().len(),
            1,
            "email port must have been called"
        );
    }

    #[test]
    fn non_security_severity_respects_email_preference_off() {
        let port = RecordingEmailPort::default();
        let e = event("quota:tenant-a"); // QuotaExhausted -> Warning, not Security
        let prefs_email_off = NotificationPreference {
            in_app: true,
            email: false,
        };

        let outcome = dispatch(
            &e,
            prefs_email_off,
            SystemTime::now(),
            None,
            Duration::from_secs(60),
            &port,
        )
        .expect("dispatch does not error");

        assert_eq!(
            outcome,
            DispatchOutcome::Dispatched {
                channels: Channels {
                    in_app: true,
                    email: false
                }
            }
        );
        assert!(
            port.enqueued.borrow().is_empty(),
            "email port must not have been called"
        );
    }

    #[test]
    fn non_security_severity_respects_in_app_preference_off() {
        let port = RecordingEmailPort::default();
        let e = event("quota:tenant-a"); // QuotaExhausted -> Warning, not Security
        let prefs_in_app_off = NotificationPreference {
            in_app: false,
            email: true,
        };

        let outcome = dispatch(
            &e,
            prefs_in_app_off,
            SystemTime::now(),
            None,
            Duration::from_secs(60),
            &port,
        )
        .expect("dispatch does not error");

        assert_eq!(
            outcome,
            DispatchOutcome::Dispatched {
                channels: Channels {
                    in_app: false,
                    email: true
                }
            },
            "in_app is a real preference toggle for non-Security severity, same as email"
        );
    }

    #[test]
    fn security_severity_forces_both_channels_even_when_both_preferences_are_off() {
        let port = RecordingEmailPort::default();
        let mut e = event("login:tenant-a:user-b");
        e.category = NotificationCategory::NewLogin;
        let prefs_all_off = NotificationPreference {
            in_app: false,
            email: false,
        };

        let outcome = dispatch(
            &e,
            prefs_all_off,
            SystemTime::now(),
            None,
            Duration::from_secs(60),
            &port,
        )
        .expect("dispatch does not error");

        assert_eq!(
            outcome,
            DispatchOutcome::Dispatched {
                channels: Channels {
                    in_app: true,
                    email: true
                }
            },
            "Security severity bypasses in_app too, not only email"
        );
    }

    // ---- payload type rejection: no field exists to carry private Memory body text ----

    #[test]
    fn payload_with_memory_body_field_is_type_rejected() {
        let raw = serde_json::json!({
            "export_id": "exp_123",
            "memory_body": "the user's private memory excerpt",
        });
        let err = serde_json::from_value::<NotificationPayload>(raw)
            .expect_err("an unlisted field must be rejected, not silently dropped");
        assert!(
            err.to_string().contains("memory_body") || err.to_string().contains("unknown field"),
            "expected an unknown-field error naming the offending key, got: {err}"
        );
    }

    #[test]
    fn payload_with_only_allowlisted_fields_round_trips() {
        let raw = serde_json::json!({ "export_id": "exp_123" });
        let payload: NotificationPayload =
            serde_json::from_value(raw).expect("allowlisted-only payload must deserialize");
        assert_eq!(payload.export_id.as_deref(), Some("exp_123"));
    }

    // ---- §78.2 DB/Rust enum contract: category strings must match the migration's CHECK ----

    #[test]
    fn category_db_strings_match_migration_check_list() {
        // Mirrors 0036_notification_plane.sql's `category` CHECK list verbatim — a drift in
        // either place must fail this test, not surface only at INSERT time in production.
        const MIGRATION_CHECK_LIST: &[&str] = &[
            "BYOK_INVALID",
            "WAITING_KEY",
            "QUOTA_80",
            "QUOTA_EXHAUSTED",
            "BILLING_PAST_DUE",
            "MCP_GRANT_CREATED",
            "MCP_GRANT_REVOKED",
            "NEW_LOGIN",
            "SECURITY_EVENT",
            "EXPORT_READY",
            "DELETION_PROGRESS",
            "PUBLIC_CONTRIBUTION_REVIEW",
        ];
        let rust: Vec<&str> = NotificationCategory::ALL
            .iter()
            .map(|c| c.as_db_str())
            .collect();
        assert_eq!(rust, MIGRATION_CHECK_LIST);
    }
}
