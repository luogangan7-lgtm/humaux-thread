# ADR-0009: Complete the Phase 9 contribution boundary

Status: accepted for isolated implementation; production rollout requires separate approval.

## Decision and rationale

The normative contracts remain in Baseline_2.9.md §12–14 and §31, with grants solely in
§6.2.2. This ADR records why those contracts changed, not a second schema specification.

Card C proved release IO and graph closure but did not prove private de-identification,
actual secret scanning, authorized exact-byte confirmation, public trust evaluation,
revoke-time retrieval exclusion, or durable consumption. These are separate acceptance
obligations. They use the existing PostgreSQL authority, source closure, outbox, typed pools,
and jobs rather than another message broker, mutable graph authority, or lease counter.

A minimal append-only revoke fact bridges the private/public read boundary without giving
private workers public mutation rights or retrieval workers access to private staging. It
closes the asynchronous revoke window. Immutable body/root receipts prevent stale support
or surviving roots from silently validating old synthesis prose. Human authorization is
required until an automatic promotion policy has evidence for its calibration.

The Phase 9 executor is explicitly on demand. Deferred synthesis rebuilds remain visible
work, not fake success, and do not enable the Phase 10 resident evolution process.

Remote Qdrant delivery is not part of a PostgreSQL rollback. The existing attempt fence
protects PostgreSQL state and acknowledgment; revision-specific permanent tombstones
protect late remote delivery. The precise boundary and expiry-during-I/O acceptance live
in Baseline §31 and §17.6.1, rather than an unattainable cross-system atomicity claim.

## Verification and limits

Real PostgreSQL 18.6 and Qdrant tests cover role boundaries, rollback, replay, same-owner
lease fencing, immediate revoke reads and revision projection safety. SUT mutations must
turn the corresponding tests red before restoration. Gitleaks uses pinned executable bytes
and clean/leak/error cases; offline model fixtures are not reported as live provider tests.
Production policy calibration, production migration approval, and resident evolution are
outside this change. Concrete results are recorded in the acceptance artifact, not here.
