//! `adapters::email::smtp` — the SMTP reference adapter for [`super::EmailProvider`] (§74.6: "Reference adapters:
//!   SMTP / SES / Postmark / Resend 等, 不写死供应商" — this is the SMTP one; SES/Postmark/Resend are HTTP-API adapters and
//!   out of this task's scope).
//! Depends-on: crates=[async-trait, lettre]; services=[]; env=[]; modules=[adapters::email]
//! Called-by: []
//! Invariants: [configuration comes only from the SmtpConfig parameter, never from env; an SMTP refusal is
//!   EmailError::Rejected and a transport failure is surfaced, never retried silently here]
//! Spec: none
//!
//! Built on
//! `lettre`'s async tokio transport so `send` is one non-blocking network round trip.
//!
//! [`SmtpConfig`] is a plain struct filled in by the caller — this module never reads an
//! environment variable itself (task brief item 1: "配置来自参数不读 env"); production wiring
//! reads the real host/port/credentials from whatever typed config source the deployment
//! layer uses and constructs a [`SmtpConfig`] from that.

use async_trait::async_trait;
use lettre::message::{Mailbox, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

use super::{EmailError, EmailProvider, OutboundEmail, ProviderMessageId};

/// SMTP connection parameters, supplied by the caller — never read from `std::env` inside
/// this module (task brief: "配置来自参数不读 env").
#[derive(Debug, Clone)]
pub struct SmtpConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    /// STARTTLS on the given port (587/25) vs implicit TLS (465, `smtps`). Not inferred
    /// from the port number — an explicit field so a deployment's actual transport choice
    /// is never a guess (§78.1: no hardcoded business/infra assumption baked into code that
    /// a config value should carry instead).
    pub implicit_tls: bool,
}

/// lettre-backed [`EmailProvider`]. Construction (`new`) is fallible only on malformed
/// config (bad hostname string); the actual network connection happens lazily per `send`
/// call via `lettre`'s pooled `AsyncSmtpTransport`, so `new` itself never blocks on the
/// network.
pub struct SmtpEmailProvider {
    transport: AsyncSmtpTransport<Tokio1Executor>,
}

impl SmtpEmailProvider {
    pub fn new(config: &SmtpConfig) -> Result<Self, EmailError> {
        let creds = Credentials::new(config.username.clone(), config.password.clone());
        let builder = if config.implicit_tls {
            AsyncSmtpTransport::<Tokio1Executor>::relay(&config.host)
        } else {
            AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&config.host)
        }
        .map_err(|e| EmailError::Transport(format!("invalid SMTP host {}: {e}", config.host)))?;
        let transport = builder.port(config.port).credentials(creds).build();
        Ok(Self { transport })
    }
}

#[async_trait]
impl EmailProvider for SmtpEmailProvider {
    fn name(&self) -> &str {
        "smtp"
    }

    async fn send(&self, mail: OutboundEmail) -> Result<ProviderMessageId, EmailError> {
        let to: Mailbox = mail
            .to
            .parse()
            .map_err(|e| EmailError::Rejected(format!("invalid recipient {}: {e}", mail.to)))?;
        let from: Mailbox = mail.from.parse().map_err(|e| {
            EmailError::Transport(format!("invalid from address {}: {e}", mail.from))
        })?;

        let builder = Message::builder().to(to).from(from).subject(mail.subject);
        let message = match mail.body_html {
            Some(html) => builder
                .multipart(MultiPart::alternative_plain_html(mail.body_text, html))
                .map_err(|e| EmailError::Rejected(format!("message build failed: {e}")))?,
            None => builder
                .singlepart(SinglePart::plain(mail.body_text))
                .map_err(|e| EmailError::Rejected(format!("message build failed: {e}")))?,
        };

        let response = self
            .transport
            .send(message)
            .await
            .map_err(|e| EmailError::Transport(e.to_string()))?;
        if !response.is_positive() {
            return Err(EmailError::Rejected(format!(
                "SMTP server rejected message: {:?}",
                response.message().collect::<Vec<_>>()
            )));
        }
        // SMTP itself has no provider-message-id concept (unlike an HTTP-API provider) —
        // the queue-id line most MTAs echo in the response is the closest analogue and is
        // what downstream delivery-event correlation actually has to work with.
        let id = response
            .message()
            .next()
            .map(str::to_string)
            .unwrap_or_else(|| format!("{:?}", response.code()));
        Ok(ProviderMessageId(id))
    }
}
