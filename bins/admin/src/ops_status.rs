//! `admin::ops_status` — §4.4 `degrade.counters` and `flags.effective`: reads the loopback `/status` document of every
//!   process listed in `HUMAUX_ADMIN_OPS_ADDRS` (ADR-0061 D-B, D-J).
//! Depends-on: crates=[humaux-infra-cell, humaux-telemetry, serde_json]; services=[HTTP(loopback)]; env=[HUMAUX_ADMIN_OPS_ADDRS];
//!   modules=[admin::probe, infra-cell::transport, telemetry::degrade, telemetry::metrics]
//! Called-by: [admin::probe, tests]
//! Invariants: [a listed process whose `/status` cannot be read is a MissingObject naming it, never a partial sum;
//!   a document missing a `DegradeCode` or carrying an unknown config source fails closed naming it]
//! Spec: Baseline §4.4; §53; §78; ADR-0061 D-B; ADR-0061 D-J
//!
//! `degrade_total{code}` is an in-process counter (ADR-0061 D-A), so the only process-external store of it is each
//! process's own `/status`. The effective config likewise exists only inside the gateway that resolved it.

use std::io::{ErrorKind, Read as _, Write as _};
use std::net::{SocketAddr, TcpStream};
use std::time::Instant;

use humaux_telemetry::degrade::DegradeCode;
use humaux_telemetry::metrics::IO_TIMEOUT;
use serde_json::{Map, Value, json};

use crate::probe::{ProbeOutcome, reading};

const OPS_ADDRS: &str = "HUMAUX_ADMIN_OPS_ADDRS";
/// The largest `/status` answer read: the admin's existing cap for an intra-Cell HTTP answer (ADR-0061 review-fix 3,
/// F4), not a second threshold.
const MAX_STATUS_BYTES: usize = humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES;

/// `degrade.counters` scan predicate; the listed process names are appended per call. Pinned by `probe::PINNED`.
pub(crate) const DEGRADE_SCOPE: &str = "GET /status#degrade|sum(count) over DegradeCode::ALL";
/// `flags.effective` scan predicate; the gateway's listed name is appended per call. Pinned by `probe::PINNED`.
pub(crate) const FLAGS_SCOPE: &str =
    "GET /status#effective_config|source = env|process = humaux-gateway";

/// `name=host:port` entries, comma separated. The name is the operator's label for the entry (two resident modes
/// of one binary share a `process` value), and is what a refusal or `by_process` names.
fn targets(raw: &str) -> Result<Vec<(String, SocketAddr)>, String> {
    raw.split(',')
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .map(|e| {
            let (name, addr) = e
                .split_once('=')
                .ok_or_else(|| format!("{OPS_ADDRS} entry {e:?} is not name=host:port"))?;
            let addr = addr.parse().map_err(|_| {
                format!("{OPS_ADDRS} entry {e:?}: {addr:?} is not a socket address")
            })?;
            Ok((name.to_owned(), addr))
        })
        .collect()
}

fn get_status(addr: SocketAddr) -> Result<Value, String> {
    get_status_capped(addr, MAX_STATUS_BYTES)
}

fn get_status_capped(addr: SocketAddr, max_bytes: usize) -> Result<Value, String> {
    // ADR-0061 review-fix 3 (F4): one total deadline for connect, request write and response read (the listener's
    // own budget), and a size cap. Per-read timeouts alone let a peer that trickles bytes hold the probe forever.
    let deadline = Instant::now() + IO_TIMEOUT;
    let left = || {
        deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
            .ok_or_else(|| format!("no complete answer within {IO_TIMEOUT:?}"))
    };
    // dep: HTTP(loopback) — GET /status of a process ops listener (ADR-0061 D-B)
    let mut stream = TcpStream::connect_timeout(&addr, left()?).map_err(|e| e.to_string())?;
    stream
        .set_write_timeout(Some(left()?))
        .map_err(|e| e.to_string())?;
    write!(
        stream,
        "GET /status HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    )
    .map_err(|e| e.to_string())?;
    let mut response = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        // macOS refuses setsockopt (EINVAL) once the peer has closed, which is the normal end of a
        // `Connection: close` answer: the read then cannot block, and the previous timeout still bounds it.
        let _ = stream.set_read_timeout(Some(left()?));
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => response.extend_from_slice(&chunk[..n]),
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                return Err(format!("no complete answer within {IO_TIMEOUT:?}"));
            }
            Err(e) => return Err(e.to_string()),
        }
        if response.len() > max_bytes {
            return Err(format!("the answer exceeds {max_bytes} bytes"));
        }
    }
    let response = String::from_utf8(response).map_err(|_| "the answer is not UTF-8")?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or("the response has no HTTP head")?;
    let status_line = head.lines().next().unwrap_or("");
    if !status_line.starts_with("HTTP/1.1 200 ") {
        return Err(format!("answered {status_line:?}"));
    }
    serde_json::from_str(body).map_err(|e| format!("/status is not JSON: {e}"))
}

/// Every listed document, or the first listed process that cannot be read, named.
fn statuses() -> Result<Vec<(String, Value)>, String> {
    let raw = std::env::var(OPS_ADDRS).map_err(|_| {
        format!("{OPS_ADDRS} (comma list of name=127.0.0.1:port; required for this probe)")
    })?;
    let targets = targets(&raw)?;
    if targets.is_empty() {
        return Err(format!("{OPS_ADDRS} lists no process"));
    }
    targets
        .into_iter()
        .map(|(name, addr)| match get_status(addr) {
            Ok(doc) => Ok((name, doc)),
            Err(e) => Err(format!("/status of {name}={addr} ({OPS_ADDRS}): {e}")),
        })
        .collect()
}

/// §4.4 `degrade.counters` over every listed process.
pub(crate) fn degrade_counters() -> ProbeOutcome {
    statuses().map_or_else(ProbeOutcome::MissingObject, |docs| degrade_from(&docs))
}

/// §4.4 `flags.effective` from the one listed gateway.
pub(crate) fn flags_effective() -> ProbeOutcome {
    statuses().map_or_else(ProbeOutcome::MissingObject, |docs| flags_from(&docs))
}

/// value = Σ count over codes over processes; `scanned_n` = |`DegradeCode::ALL`| (ADR-0061 D-J).
pub(crate) fn degrade_from(docs: &[(String, Value)]) -> ProbeOutcome {
    let mut total: i64 = 0;
    let mut detail = Map::new();
    for code in DegradeCode::ALL {
        let (mut count, mut last, mut by_process) = (0_i64, None::<u64>, Map::new());
        for (name, doc) in docs {
            let entry = &doc["degrade"][code.as_str()];
            let Some(n) = entry["count"].as_i64() else {
                return ProbeOutcome::MissingObject(format!(
                    "degrade.{}.count in /status of {name}",
                    code.as_str()
                ));
            };
            count += n;
            last = last.max(entry["last_fired_at"].as_u64());
            by_process.insert(name.clone(), Value::from(n));
        }
        total += count;
        detail.insert(
            code.as_str().to_owned(),
            json!({"count": count, "last_fired_at": last, "by_process": by_process}),
        );
    }
    let names: Vec<&str> = docs.iter().map(|(n, _)| n.as_str()).collect();
    reading(
        total,
        DegradeCode::ALL.len(),
        format!("{DEGRADE_SCOPE}|processes={}", names.join(",")),
        "degrade.counters@1",
        Value::Object(detail),
        "DegradeCode::ALL",
    )
}

/// value = `effective_config` rows whose source is `env`; `scanned_n` = all rows (= the gateway `registry()` count).
pub(crate) fn flags_from(docs: &[(String, Value)]) -> ProbeOutcome {
    let gateways: Vec<&(String, Value)> = docs
        .iter()
        .filter(|(_, doc)| doc["process"] == "humaux-gateway")
        .collect();
    let [(name, doc)] = gateways.as_slice() else {
        return ProbeOutcome::MissingObject(format!(
            "exactly one humaux-gateway /status among {OPS_ADDRS} (found {})",
            gateways.len()
        ));
    };
    let Some(rows) = doc["effective_config"].as_array() else {
        return ProbeOutcome::MissingObject(format!("effective_config in /status of {name}"));
    };
    let mut from_env = 0_i64;
    for row in rows {
        match row["source"].as_str() {
            Some("env") => from_env += 1,
            Some("default") => {}
            other => {
                return ProbeOutcome::MissingObject(format!(
                    "a known source (env|default) for effective_config entry {} of {name}, got {other:?}",
                    row["name"]
                ));
            }
        }
    }
    reading(
        from_env,
        rows.len(),
        format!("{FLAGS_SCOPE}|{name}"),
        "flags.effective@1",
        json!({"process": name, "entries": rows}),
        &format!("effective_config of {name}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn targets_name_a_malformed_entry() {
        let ok = targets("gateway=127.0.0.1:19101, mh=127.0.0.1:19107").unwrap();
        assert_eq!(ok.len(), 2);
        assert_eq!(ok[1].0, "mh");
        assert!(
            targets("127.0.0.1:1")
                .unwrap_err()
                .contains("name=host:port")
        );
        assert!(targets("g=nowhere").unwrap_err().contains("\"nowhere\""));
    }

    /// A loopback server that answers its head and then trickles one body byte per second.
    fn trickling_server(bytes: usize) -> SocketAddr {
        // dep: HTTP(loopback) — a stalling /status peer for the deadline test
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut buf = [0u8; 1024];
            let _ = s.read(&mut buf);
            let _ = s.write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n{");
            for _ in 0..bytes {
                std::thread::sleep(std::time::Duration::from_secs(1));
                if s.write_all(b" ").is_err() {
                    return;
                }
            }
            let _ = s.write_all(b"}");
        });
        addr
    }

    /// ADR-0061 review-fix 3 (F4): a peer that stalls mid-answer is cut at the total deadline and named, never
    /// waited for. Fault: no total deadline (a per-read `IO_TIMEOUT` only; each byte arrives inside it) ⇒ the fetch
    /// waits the full ten seconds and parses `{}` ⇒ red.
    #[test]
    fn a_stalling_status_peer_is_cut_at_the_deadline() {
        let started = Instant::now();
        let err = get_status(trickling_server(10)).expect_err("a stalled answer is not a document");
        let waited = started.elapsed();
        assert!(
            waited < IO_TIMEOUT + std::time::Duration::from_secs(1),
            "waited {waited:?} for a stalled /status (deadline {IO_TIMEOUT:?})"
        );
        assert!(err.contains("no complete answer within"), "{err}");
    }

    /// ADR-0061 review-fix 3 (F4): an answer larger than the cap is refused naming the cap (a 1 KiB cap here, so
    /// the case does not race the deadline).
    #[test]
    fn an_oversized_status_answer_is_refused() {
        const CAP: usize = 1024;
        // dep: HTTP(loopback) — a /status peer answering past the cap
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut buf = [0u8; 1024];
            let _ = s.read(&mut buf);
            let _ = s.write_all(b"HTTP/1.1 200 OK\r\n\r\n");
            let _ = s.write_all(&[b' '; 2 * CAP]);
            // Half-close and drain until the client hangs up: closing with unread bytes would send a reset that the
            // client could read before the body.
            let _ = s.shutdown(std::net::Shutdown::Write);
            let _ = s.read_to_end(&mut Vec::new());
        });
        let err = get_status_capped(addr, CAP).expect_err("an oversized answer is refused");
        assert!(err.contains("exceeds 1024 bytes"), "{err}");
    }

    /// A process whose document lacks one code fails closed naming the code and the process.
    #[test]
    fn a_missing_code_is_named_not_summed_as_zero() {
        let doc = json!({"degrade": {"ProjectionLag": {"count": 1, "last_fired_at": 5}}});
        let ProbeOutcome::MissingObject(m) = degrade_from(&[("gw".to_owned(), doc)]) else {
            panic!("a partial document must not produce a reading");
        };
        assert!(m.contains("of gw"), "{m}");
    }
}
