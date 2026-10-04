//! `admin::tls_expiry` — §4.4 `tls.expiry`: how many of the certificate files in `HUMAUX_ADMIN_TLS_CERT_PATHS` expire
//!   within the frozen 21 d WARN window (ADR-0061 D-J).
//! Depends-on: crates=[serde_json, x509-cert]; services=[fs(PEM certificate files)]; env=[HUMAUX_ADMIN_TLS_CERT_PATHS];
//!   modules=[admin::probe]
//! Called-by: [admin::probe, tests]
//! Invariants: [an unreadable or unparsable file is a MissingObject naming its path, never a skipped file]
//! Spec: Baseline §4.4; §42; §68; ADR-0061 D-J

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::json;
use x509_cert::Certificate;

use crate::probe::{ProbeOutcome, reading};

const CERT_PATHS: &str = "HUMAUX_ADMIN_TLS_CERT_PATHS";

/// Baseline §42 cert-expiry row / §68: frozen at 21 d WARN (1814400 s); the Prometheus rule and this probe share it,
/// so it is a spec constant, not deploy config.
pub(crate) const WARN_REMAINING_SECONDS: i64 = 1_814_400;

/// `tls.expiry` scan predicate; the file list is appended per call. Pinned by `probe::PINNED`.
pub(crate) const SCOPE: &str =
    "HUMAUX_ADMIN_TLS_CERT_PATHS#min(notAfter) per file|notAfter - now < 1814400 s";

/// §4.4 `tls.expiry` at the current wall clock.
pub(crate) fn run() -> ProbeOutcome {
    let Ok(raw) = std::env::var(CERT_PATHS) else {
        return ProbeOutcome::MissingObject(format!(
            "{CERT_PATHS} (comma list of PEM certificate paths; required for this probe)"
        ));
    };
    let paths: Vec<&str> = raw
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .collect();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    expiring(&paths, now)
}

/// value = files whose earliest notAfter is less than [`WARN_REMAINING_SECONDS`] after `now`; `scanned_n` = files.
pub(crate) fn expiring(paths: &[&str], now: Duration) -> ProbeOutcome {
    let mut value = 0_i64;
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let not_after = match not_after(path) {
            Ok(t) => t,
            Err(e) => {
                return ProbeOutcome::MissingObject(format!(
                    "a readable PEM certificate at {path} ({CERT_PATHS}): {e}"
                ));
            }
        };
        let remaining = secs(not_after) - secs(now);
        let warn = remaining < WARN_REMAINING_SECONDS;
        value += i64::from(warn);
        files.push(json!({
            "path": path,
            "not_after_unix": secs(not_after),
            "remaining_seconds": remaining,
            "warn": warn,
        }));
    }
    reading(
        value,
        paths.len(),
        format!("{SCOPE}|files={}", paths.join(",")),
        "tls.expiry@1",
        json!({ "files": files }),
        CERT_PATHS,
    )
}

fn secs(d: Duration) -> i64 {
    i64::try_from(d.as_secs()).unwrap_or(i64::MAX)
}

/// The earliest notAfter in the file: a served chain is valid only until its first-expiring link.
fn not_after(path: &str) -> Result<Duration, String> {
    // dep: fs(PEM certificate files) — reads one operator-listed certificate file
    let pem = std::fs::read(path).map_err(|e| e.to_string())?;
    let chain = Certificate::load_pem_chain(&pem).map_err(|e| e.to_string())?;
    chain
        .iter()
        .map(|c| c.tbs_certificate.validity.not_after.to_unix_duration())
        .min()
        .ok_or_else(|| "no CERTIFICATE block".to_owned())
}
