//! `projection::stream` — §15 stream identity + the pure (no-IO) half of T3.3's
//! `advance_prefix` three-way ledger consistency check.
//!
//! This module deliberately holds no SQL and no async: the three/four independent reads
//! §15.4 requires (`stream_log_agg`, `count_open_gaps` via the `processing_gaps` view,
//! `contiguous_done_prefix`, `max_stream_seq`) are each a real round trip to PostgreSQL and
//! live in `humaux_adapters::stream_repo` against `&RetrievalWorkerDbPool` /
//! `&MaintenanceDbPool` (§6.2.3) — reusing one query result for two of the checks would
//! defeat the entire point of "three numbers independently taken and cross-proved" (spec
//! prose at the top of §15: "单边取数的检查永远观察不到自己漏了"). Keeping the arithmetic
//! here means it is unit-testable without a database and cannot itself perform the query
//! reuse the design forbids — the caller physically cannot pass this function anything but
//! already-independent numbers.

use std::time::Duration;

use humaux_domain::ids::TenantId;
use uuid::Uuid;

/// §15.2 patrol's wall-clock threshold, as a named/overridable config value rather than a
/// literal buried in SQL (§78.1 bans hardcoded TTLs). Spec's own comment on the SQL block:
/// "实测同一次 memory_store 落 L0 后 40s 蒸馏出 L1，留 20× 余量" — 15 minutes is that
/// measured-then-margined value, not an arbitrary round number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamSlaConfig {
    /// An `ISSUED` row older than this with no matching in-flight `ops.jobs` row is orphaned
    /// (§15.2).
    pub stream_sla: Duration,
}

impl Default for StreamSlaConfig {
    fn default() -> Self {
        Self {
            stream_sla: Duration::from_secs(15 * 60),
        }
    }
}

/// §15.1 DDL's first six `stream_log` primary-key columns (everything but `stream_seq`
/// itself) — the identity of one dense per-stream sequence ledger.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamKey {
    pub tenant_id: TenantId,
    pub scope_kind: String,
    pub scope_id: Uuid,
    pub domain: String,
    pub projection_kind: String,
    pub projection_version: String,
}

impl StreamKey {
    /// Builds a key from its six DDL-column values.
    pub fn new(
        tenant_id: TenantId,
        scope_kind: impl Into<String>,
        scope_id: Uuid,
        domain: impl Into<String>,
        projection_kind: impl Into<String>,
        projection_version: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id,
            scope_kind: scope_kind.into(),
            scope_id,
            domain: domain.into(),
            projection_kind: projection_kind.into(),
            projection_version: projection_version.into(),
        }
    }

    /// Canonical text encoding for `ops.jobs.stream_key` (§15.2's patrol SQL, `j.stream_key =
    /// s.<stream_key>`; §31's comment "typed locator ... 不得把 stream identity 只塞 JSON
    /// payload 后靠扫描字符串做 orphan 判断"). Every writer of a pipeline `ops.jobs` row for
    /// this stream (T3.1 `remember`, T3.5 job enqueue) and the T3.4 LOST patrol below must
    /// produce/match this exact string — it is the one place that encoding is defined.
    // ponytail: `:`-joined, unescaped. All six components are internal system identifiers
    // (a UUID and closed-set-ish enum-like strings: scope_kind/domain/projection_kind/
    // projection_version), never user-controlled free text, so a literal `:` inside one of
    // them isn't reachable today. Upgrade path if that ever stops being true: a length-
    // prefixed or percent-escaped encoding instead of a bare delimiter.
    pub fn stream_key_text(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}:{}",
            self.tenant_id.0,
            self.scope_kind,
            self.scope_id,
            self.domain,
            self.projection_kind,
            self.projection_version
        )
    }
}

/// §15.4's four independently-fetched numbers for one stream. `expected` and
/// `max_stream_seq` are two genuinely separate queries against two different tables
/// (`stream_checkpoints.issued_highwater` vs. `MAX(stream_log.stream_seq)`) even though they
/// are expected to agree — that agreement is itself one of the invariants [`advance_prefix`]
/// checks, so folding them into one query would make the check unable to observe its own
/// drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamLedgerSnapshot {
    /// `stream_checkpoints.issued_highwater` (§15.4: `expected = issued_highwater`).
    pub expected: u64,
    /// `count(state IN SETTLED_OK)` — `DONE | SKIPPED_BY_POLICY | TOMBSTONED | RETIRED_FAILED`
    /// (§15.2, widened by migration 0167's audited retirement).
    pub done: u64,
    /// `count(state IN PENDING)` — `ISSUED | PROCESSING | WAITING_KEY | RETRY_WAIT` (§15.2).
    pub pending: u64,
    /// `count(*)` from the `processing_gaps` view (`FAILED | LOST`, §15.1 comment).
    pub open_gaps: u64,
    /// `MAX(stream_seq)` read directly off `stream_log`, independent of `expected` above.
    pub max_stream_seq: u64,
    /// `min{s | stream_log[s].state NOT IN SETTLED_OK} - 1`, or `max(stream_seq)` when no
    /// such `s` exists (§15.4's `contiguous_done_prefix` formula).
    pub contiguous_done_prefix: u64,
}

/// §15.4 / §23.1② ledger-closure violation. Not one of the workspace's two frozen domain
/// error enums (§52 `ErrorCode`/`DegradeCode`) — like `postgres::PoolInitError` and
/// `email::OutboxError`, this is a narrow adapter/projection-boundary signal, not a Domain
/// terminal or degrade classification. The caller that owns completeness classification
/// (`ledger::close`/`classify()`, out of this task's scope — see `retrieval::completeness`'s
/// module doc) is the one that turns this into `CompletenessClass::CannotEstablish` /
/// `Outcome.degradations` (§53); this type only carries the fact that the identity broke.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Inconsistent;

/// §15.4 `advance_prefix`: given one stream's four independently-fetched numbers, validates
/// the three-way identity and returns the `stream_checkpoints.projection_highwater` value the
/// caller may now write (never higher than `contiguous_done_prefix`, so a `FAILED`/`LOST` row
/// blocks the watermark at the gap rather than skipping over it — §15.7 "禁 watermark 跨未知
/// gap"). No IO here; the caller (`humaux_adapters::stream_repo::advance_prefix`) is
/// responsible for the four independent queries and for the write. Never panics — every
/// invalid state returns `Err(Inconsistent)`.
pub fn advance_prefix(snapshot: StreamLedgerSnapshot) -> Result<u64, Inconsistent> {
    let StreamLedgerSnapshot {
        expected,
        done,
        pending,
        open_gaps,
        max_stream_seq,
        contiguous_done_prefix,
    } = snapshot;

    // §15.4 frozen identity, evaluated by §22.5's sole A1 implementation
    // (`humaux_domain::ledger::a1_holds`) rather than a second copy of the expression here —
    // §22.5: 「A1 的算式全库只此一处，不会两边各写一遍再漂移」.
    if !humaux_domain::ledger::a1_holds(expected, done, open_gaps, pending) {
        return Err(Inconsistent);
    }
    // The two independent "how far has this stream issued" numbers must agree — a drift here
    // is exactly the class of bug §15.4's three-way design exists to surface (an aggregate
    // counter and a directly-counted MAX() disagreeing).
    if expected != max_stream_seq {
        return Err(Inconsistent);
    }
    // The prefix can never run ahead of what was ever issued.
    if contiguous_done_prefix > expected {
        return Err(Inconsistent);
    }

    Ok(contiguous_done_prefix)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> StreamKey {
        StreamKey::new(
            TenantId::new(),
            "workspace",
            Uuid::now_v7(),
            "code",
            "retrieval_card",
            "v1",
        )
    }

    #[test]
    fn stream_sla_default_is_fifteen_minutes() {
        assert_eq!(
            StreamSlaConfig::default().stream_sla,
            Duration::from_secs(900)
        );
    }

    #[test]
    fn stream_key_text_joins_all_six_components() {
        let k = key();
        let text = k.stream_key_text();
        assert_eq!(
            text.matches(':').count(),
            5,
            "six components joined by five colons"
        );
        assert!(text.starts_with(&k.tenant_id.0.to_string()));
        assert!(text.ends_with("v1"));
    }

    #[test]
    fn stream_key_text_is_stable_for_equal_keys() {
        let k1 = key();
        let k2 = k1.clone();
        assert_eq!(k1.stream_key_text(), k2.stream_key_text());
    }

    /// §15.4 happy path: identity holds, prefix within bound ⇒ `Ok(contiguous_done_prefix)`.
    #[test]
    fn advance_prefix_ok_when_identity_holds() {
        let snap = StreamLedgerSnapshot {
            expected: 10,
            done: 7,
            pending: 2,
            open_gaps: 1,
            max_stream_seq: 10,
            contiguous_done_prefix: 6,
        };
        assert_eq!(advance_prefix(snap), Ok(6));
    }

    /// §15.4 worked example (100 `FAILED`, 101 `DONE`): the watermark stops at 99, one seq
    /// short of the highest issued — this pins the *bound*, not the SQL computing it (that
    /// lives in `stream_repo` and is proved by the DB-backed fault-injection test).
    #[test]
    fn advance_prefix_stops_at_the_gap_not_past_it() {
        let snap = StreamLedgerSnapshot {
            expected: 101,
            done: 99, // 99 DONE below the gap + this stream's other SETTLED_OK rows
            pending: 0,
            open_gaps: 2, // seq 100 (FAILED) is a gap; seq 101 (DONE) is not counted as done
            // here because the caller passes the already-computed prefix, not
            // this module recomputing `done` from raw rows.
            max_stream_seq: 101,
            contiguous_done_prefix: 99,
        };
        assert_eq!(advance_prefix(snap), Ok(99));
    }

    /// Violating `expected == done + open_gaps + pending` ⇒ `Inconsistent`, never a silent
    /// clamp or a panic (§15.4 / §23.1②).
    #[test]
    fn advance_prefix_inconsistent_when_sum_identity_breaks() {
        let snap = StreamLedgerSnapshot {
            expected: 10,
            done: 7,
            pending: 2,
            open_gaps: 2, // 7+2+2=11 != 10
            max_stream_seq: 10,
            contiguous_done_prefix: 6,
        };
        assert_eq!(advance_prefix(snap), Err(Inconsistent));
    }

    /// Violating `expected == max_stream_seq` ⇒ `Inconsistent` — the two independently-taken
    /// "how far issued" numbers disagree.
    #[test]
    fn advance_prefix_inconsistent_when_expected_disagrees_with_max_seq() {
        let snap = StreamLedgerSnapshot {
            expected: 10,
            done: 8,
            pending: 1,
            open_gaps: 1,
            max_stream_seq: 9, // stream_log only actually goes up to 9
            contiguous_done_prefix: 6,
        };
        assert_eq!(advance_prefix(snap), Err(Inconsistent));
    }

    /// `contiguous_done_prefix > expected` can only happen if the prefix computation itself
    /// is broken — still must surface as `Inconsistent`, not be trusted.
    #[test]
    fn advance_prefix_inconsistent_when_prefix_exceeds_expected() {
        let snap = StreamLedgerSnapshot {
            expected: 5,
            done: 5,
            pending: 0,
            open_gaps: 0,
            max_stream_seq: 5,
            contiguous_done_prefix: 6,
        };
        assert_eq!(advance_prefix(snap), Err(Inconsistent));
    }
}
