//! `adapters::email::test_double` — an in-memory [`EmailProvider`] for tests (task brief item 1: "一个 test double").
//! Depends-on: crates=[async-trait]; services=[]; env=[]; modules=[adapters::email]
//! Called-by: [tests]
//! Invariants: [never dials out; each send is recorded and answered from the configured outcome queue, so provider
//!   failures propagate to the caller exactly as scripted]
//! Spec: none
//! dep-map: allow table-undeclared — panic message text names ops.email_outbox for diagnostics only, no DB access here
//!
//! Never dials out; every `send` call is recorded and answered from a
//! caller-configured outcome queue, so a test can assert both what was sent and how a
//! provider failure propagates without a real SMTP server.

use std::sync::Mutex;

use async_trait::async_trait;

use super::{EmailError, EmailProvider, OutboundEmail, ProviderMessageId};

/// One scripted response for the next [`RecordingEmailProvider::send`] call.
#[derive(Debug, Clone)]
pub enum ScriptedOutcome {
    Ok(String),
    Err(EmailErrorKind, String),
}

/// [`EmailError`] has no `Clone`/`PartialEq` (it wraps `String` reasons only, no need for
/// either outside this test double) — this mirrors its two variants so a script can name
/// which one to raise without cloning an `EmailError` itself.
#[derive(Debug, Clone, Copy)]
pub enum EmailErrorKind {
    Transport,
    Rejected,
}

/// Records every [`OutboundEmail`] passed to `send`, in call order. Defaults to answering
/// every call with `Ok("test-<n>")`; call [`Self::push_outcome`] to script a specific
/// (e.g. failing) response for the next call.
#[derive(Default)]
pub struct RecordingEmailProvider {
    sent: Mutex<Vec<OutboundEmail>>,
    scripted: Mutex<Vec<ScriptedOutcome>>,
}

impl RecordingEmailProvider {
    pub fn new() -> Self {
        Self::default()
    }

    /// Queues one outcome (FIFO) for a future `send` call to return instead of the default
    /// `Ok`.
    pub fn push_outcome(&self, outcome: ScriptedOutcome) {
        self.scripted
            .lock()
            .expect("scripted outcomes lock")
            .push(outcome);
    }

    /// Every email passed to `send` so far, in call order.
    pub fn sent(&self) -> Vec<OutboundEmail> {
        self.sent.lock().expect("sent emails lock").clone()
    }
}

#[async_trait]
impl EmailProvider for RecordingEmailProvider {
    fn name(&self) -> &str {
        "recording-test-double"
    }

    async fn send(&self, mail: OutboundEmail) -> Result<ProviderMessageId, EmailError> {
        let outcome = {
            let mut scripted = self.scripted.lock().expect("scripted outcomes lock");
            if scripted.is_empty() {
                None
            } else {
                Some(scripted.remove(0))
            }
        };
        let call_index = {
            let mut sent = self.sent.lock().expect("sent emails lock");
            sent.push(mail);
            sent.len()
        };
        match outcome {
            None => Ok(ProviderMessageId(format!("test-{call_index}"))),
            Some(ScriptedOutcome::Ok(id)) => Ok(ProviderMessageId(id)),
            Some(ScriptedOutcome::Err(EmailErrorKind::Transport, msg)) => {
                Err(EmailError::Transport(msg))
            }
            Some(ScriptedOutcome::Err(EmailErrorKind::Rejected, msg)) => {
                Err(EmailError::Rejected(msg))
            }
        }
    }
}

/// An [`EmailProvider`] that panics if `send` is ever called — used to *prove*
/// non-blocking enqueue (task brief: "handler 发验证码不阻塞 SMTP" / "入队即返回"), not just
/// assert it ran fast. `outbox::enqueue` never touches its `provider` argument at all (it
/// doesn't take one), so this double exists for the handler-layer test that wires an
/// enqueue call alongside a provider it must never reach.
#[derive(Default)]
pub struct UnreachableEmailProvider;

#[async_trait]
impl EmailProvider for UnreachableEmailProvider {
    fn name(&self) -> &str {
        "unreachable-test-double"
    }

    async fn send(&self, _mail: OutboundEmail) -> Result<ProviderMessageId, EmailError> {
        panic!(
            "UnreachableEmailProvider::send called — a QUEUED-path caller reached the \
             network transport directly instead of going through ops.email_outbox (§74.6)"
        );
    }
}
