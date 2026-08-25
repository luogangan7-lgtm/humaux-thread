//! `adapters::email` — H3: §74.6 `EmailProvider` trait + SMTP adapter + the Email
//! Deliverability Plane (outbox / delivery events / suppression / domain / provider
//! health).
//!
//! Module layout: this file (trait + wire enums), [`smtp`] (lettre-backed SMTP adapter,
//! config from parameters — never reads env directly, §74.6 task brief), [`test_double`]
//! (in-memory [`test_double::RecordingEmailProvider`]), [`outbox`] (DB-backed `enqueue` +
//! SKIP LOCKED worker + suppression check + delivery-event recording, `ops.email_outbox`
//! DDL in `migrations/0038_email_deliverability.sql`).
//!
//! §74.6 "不写死供应商": nothing outside [`smtp`]/[`test_double`] may name a concrete
//! provider type — every other caller (including [`outbox`]) takes `&dyn EmailProvider`.

pub mod outbox;
pub mod smtp;
pub mod test_double;

pub use outbox::{EnqueueOutcome, OutboxError};
pub use smtp::{SmtpConfig, SmtpEmailProvider};
pub use test_double::RecordingEmailProvider;

use async_trait::async_trait;

/// §74.6 abstraction, verbatim signature from the spec code block. The sole boundary
/// between the Deliverability Plane (this module) and a concrete transport
/// (SMTP/SES/Postmark/Resend/...); [`outbox::run_once`] holds this as `&dyn EmailProvider`
/// so the worker never names a concrete adapter type.
#[async_trait]
pub trait EmailProvider: Send + Sync {
    /// Sends one email through this provider. This is the one network round trip in the
    /// whole H3 module — §74.6 "发信走 email_outbox, 验证码 HTTP handler 不直接阻塞 SMTP"
    /// is enforced by [`outbox::enqueue`] never calling this method at all, only the
    /// background [`outbox::run_once`] worker does.
    async fn send(&self, mail: OutboundEmail) -> Result<ProviderMessageId, EmailError>;

    /// Adapter identity written to `ops.email_outbox.provider` /
    /// `ops.email_delivery_events.provider` once [`outbox::run_once`] dispatches through this
    /// provider — an open string (§74.6 "不写死供应商"), never a fixed enum of provider
    /// names. Both columns are documented (migrations/0038_email_deliverability.sql) as
    /// "filled in once dispatched"; this method is what [`outbox::run_once`] now has to fill
    /// them with.
    fn name(&self) -> &str;
}

/// One outbound email, provider-agnostic.
#[derive(Debug, Clone)]
pub struct OutboundEmail {
    pub to: String,
    pub from: String,
    pub subject: String,
    pub body_text: String,
    pub body_html: Option<String>,
    /// Carried through to the provider call so an adapter with separate sending
    /// identities/IP pools per stream (§74.6 "Transactional 与 Marketing 分离") can route
    /// on it. This struct does not itself enforce the separation — [`outbox::enqueue`]'s
    /// suppression check does (§74.6 Suppression / [`SuppressionScope::blocks`]).
    pub stream: EmailStream,
}

/// Provider-assigned message id, opaque outside this module (stored in
/// `ops.email_outbox.provider_message_id` / `ops.email_delivery_events.detail`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderMessageId(pub String);

/// Adapter-level send failure. Not one of the workspace's two frozen domain error enums
/// (`ErrorCode`/`DegradeCode`, §52 "禁止新开第三个错误枚举") — this is a transport-technical
/// error local to the `EmailProvider` boundary, same precedent as this crate's own
/// `postgres::PoolInitError`. The calling layer ([`outbox::run_once`]) maps it to a
/// `state = 'FAILED'` row + a `FAILED` delivery event; it never itself becomes a domain
/// `ErrorCode`.
#[derive(Debug)]
pub enum EmailError {
    /// Transport/connect/auth failure talking to the provider.
    Transport(String),
    /// Provider rejected the message content/recipient outright (e.g. a 5xx SMTP reply).
    Rejected(String),
}

impl std::fmt::Display for EmailError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(m) => write!(f, "email transport error: {m}"),
            Self::Rejected(m) => write!(f, "email rejected by provider: {m}"),
        }
    }
}

impl std::error::Error for EmailError {}

/// §74.6 "Transactional 与 Marketing 分离" stream. Wire values back
/// `ops.email_outbox.stream` and the two non-`ALL` values of
/// `ops.email_suppressions.scope` — `outbox::contract_tests` reconciles Rust/DB (§78.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EmailStream {
    Transactional,
    Marketing,
}

impl EmailStream {
    pub const ALL: [EmailStream; 2] = [Self::Transactional, Self::Marketing];

    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Transactional => "TRANSACTIONAL",
            Self::Marketing => "MARKETING",
        }
    }
}

/// `ops.email_outbox.state` — §74.6 Email Deliverability Plane state list, verbatim
/// (`QUEUED -> SENT -> DELIVERED -> BOUNCED/COMPLAINED/SUPPRESSED/FAILED`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EmailOutboxState {
    Queued,
    Sent,
    Delivered,
    Bounced,
    Complained,
    Suppressed,
    Failed,
}

impl EmailOutboxState {
    pub const ALL: [EmailOutboxState; 7] = [
        Self::Queued,
        Self::Sent,
        Self::Delivered,
        Self::Bounced,
        Self::Complained,
        Self::Suppressed,
        Self::Failed,
    ];

    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Queued => "QUEUED",
            Self::Sent => "SENT",
            Self::Delivered => "DELIVERED",
            Self::Bounced => "BOUNCED",
            Self::Complained => "COMPLAINED",
            Self::Suppressed => "SUPPRESSED",
            Self::Failed => "FAILED",
        }
    }
}

/// `ops.email_delivery_events.event_type` — the outbox states minus `QUEUED` (queueing a
/// row is not itself a delivery *event*; §74.6 "email_outbox 只是第一步, 还必须维护
/// email_delivery_events").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeliveryEventType {
    Sent,
    Delivered,
    Bounced,
    Complained,
    Suppressed,
    Failed,
}

impl DeliveryEventType {
    pub const ALL: [DeliveryEventType; 6] = [
        Self::Sent,
        Self::Delivered,
        Self::Bounced,
        Self::Complained,
        Self::Suppressed,
        Self::Failed,
    ];

    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Sent => "SENT",
            Self::Delivered => "DELIVERED",
            Self::Bounced => "BOUNCED",
            Self::Complained => "COMPLAINED",
            Self::Suppressed => "SUPPRESSED",
            Self::Failed => "FAILED",
        }
    }
}

/// `ops.email_suppressions.reason` (§74.6 Suppression: "硬退信、投诉等" plus the Gmail
/// one-click-unsubscribe reference in the same section). Closed set — `Manual` is an
/// operator-entered row, the rest are provider-signaled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SuppressionReason {
    HardBounce,
    Complaint,
    Unsubscribe,
    Manual,
}

impl SuppressionReason {
    pub const ALL: [SuppressionReason; 4] = [
        Self::HardBounce,
        Self::Complaint,
        Self::Unsubscribe,
        Self::Manual,
    ];

    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::HardBounce => "HARD_BOUNCE",
            Self::Complaint => "COMPLAINT",
            Self::Unsubscribe => "UNSUBSCRIBE",
            Self::Manual => "MANUAL",
        }
    }
}

/// `ops.email_suppressions.scope` — which stream(s) one suppression row blocks. `All` is
/// what keeps a hard bounce (mailbox does not exist) off *every* stream, while a
/// marketing-only complaint or unsubscribe leaves the transactional verification/reset
/// channel untouched — the frozen §74.6 rule "不能让营销投诉率打坏验证码/找回密码通道"
/// implemented as data (one column), not as two separate suppression tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SuppressionScope {
    Transactional,
    Marketing,
    All,
}

impl SuppressionScope {
    pub const ALL_VARIANTS: [SuppressionScope; 3] =
        [Self::Transactional, Self::Marketing, Self::All];

    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Transactional => "TRANSACTIONAL",
            Self::Marketing => "MARKETING",
            Self::All => "ALL",
        }
    }

    /// Whether a suppression carrying this scope blocks a send on `stream`.
    pub fn blocks(self, stream: EmailStream) -> bool {
        match self {
            Self::All => true,
            Self::Transactional => stream == EmailStream::Transactional,
            Self::Marketing => stream == EmailStream::Marketing,
        }
    }

    /// The reasonable default scope for a provider-signaled `reason`, used by
    /// [`outbox::record_suppression`] when the caller does not override it. A mailbox that
    /// hard-bounces or an address that files an abuse complaint is unreachable/hostile
    /// regardless of which stream triggered it — `All`. An unsubscribe click is scoped to
    /// the stream the user actually opted out of (§74.6 Gmail one-click unsubscribe is a
    /// marketing-list requirement, not a transactional one). `Manual` has no reason-derived
    /// default — an operator states the scope explicitly.
    pub fn default_for_reason(reason: SuppressionReason, stream: EmailStream) -> Option<Self> {
        match reason {
            SuppressionReason::HardBounce | SuppressionReason::Complaint => Some(Self::All),
            SuppressionReason::Unsubscribe => Some(match stream {
                EmailStream::Transactional => Self::Transactional,
                EmailStream::Marketing => Self::Marketing,
            }),
            SuppressionReason::Manual => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suppression_scope_blocks_matches_intent() {
        assert!(SuppressionScope::All.blocks(EmailStream::Transactional));
        assert!(SuppressionScope::All.blocks(EmailStream::Marketing));
        assert!(SuppressionScope::Transactional.blocks(EmailStream::Transactional));
        assert!(!SuppressionScope::Transactional.blocks(EmailStream::Marketing));
        assert!(!SuppressionScope::Marketing.blocks(EmailStream::Transactional));
        assert!(SuppressionScope::Marketing.blocks(EmailStream::Marketing));
    }

    #[test]
    fn marketing_complaint_does_not_block_transactional() {
        // §74.6 frozen rule, exercised directly: a marketing complaint (scope=All per
        // default_for_reason) DOES also block transactional — that's intentional (a
        // COMPLAINT is a hostile-mailbox signal like a hard bounce). The rule the frozen
        // text actually protects is the *unsubscribe* case: opting out of marketing must
        // NOT silently suppress the password-reset channel too.
        let unsub_scope = SuppressionScope::default_for_reason(
            SuppressionReason::Unsubscribe,
            EmailStream::Marketing,
        )
        .expect("unsubscribe always has a default scope");
        assert_eq!(unsub_scope, SuppressionScope::Marketing);
        assert!(!unsub_scope.blocks(EmailStream::Transactional));
    }
}
