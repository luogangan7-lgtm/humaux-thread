# ADR-0008: Tenant-safe release IO, typed outbox, and derived public closure

Date: 2026-08-28. Status: Accepted for Card C implementation; acceptance evidence remains separate.

## Problem and decision

Phase9's domain types and migration0103 existed, but Card C had only a placeholder. The
old task shape could neither read staging through the private worker nor write a durable
release event. UUID-only source FKs did not prove source/release tenancy, and claim-only
closure could not represent recursive syntheses. §1.11 also repeated a different closure
shape from the live schema.

Canonical contracts live only in Baseline_2.9: §6.2.2 grants, §6.2.3 typed pools, §12.1
release/admission, §12.3 closure, §14 outbox, §70.5 public-source identity. This ADR records
the reasons for those changes, not a second field or permission specification.

- Keep the existing outbox. Separate evidence and release row classes with real FKs and
  CHECKs; narrow producer columns rather than inventing an Evidence ID or stream sequence.
  Reuse the sole commit-sequence helper. Deferred constraints reject a release without
  sources and its event, while event guards reject pre-announced revoke events and identity
  rewrites. Release admission requires passed scans, consent and rights before persistence.
- Constrain the source table's derived tenant with composite FKs. RLS visibility checks
  complement referential integrity; neither one substitutes for the other. Public sources
  link to a release only for user contributions and retain its immutable rights snapshot.
  Migration0105 aligns the new tenant column's policy with the canonical tenant template;
  it preserves the applied0104 checksum and keeps the parent composite FK authoritative.
- Use invoker triggers and one transaction advisory-lock helper for release IO. Public
  workers keep no private SELECT or staging UPDATE. READ COMMITTED with a separate
  VOLATILE post-lock read avoids a stale snapshot after a concurrent revoke. A caller
  choosing another isolation level fails explicitly rather than bypassing this protocol.
- Extend the existing closure projection for claim or synthesis targets. Stable pair
  identity plus is_current avoids physical deletion and duplicate historical pairs.
  The closure is topological provenance, not the current rights/support verdict; revocation
  consumers still need the original roots. Never union a synthesis's roots into child claims.
- For closure only, acquire the input table locks and serialization lock before the first
  REPEATABLE READ query. This is a deliberate short-lock baseline, not an unmeasured
  scalability claim. Detect every reachable cycle or unrooted branch before replacing rows.
  Read only the target's reachable subgraph and memoize each synthesis's relative roots;
  enumerating every path through layered diamonds or loading unrelated graphs is unnecessary.

## Rejected alternatives

Direct jobs writes bypass §14; invented stream_seq=0 corrupts stream meaning; runtime DELETE
violates §6.2.1. Granting public workers private reads or staging UPDATE would collapse the
role boundary. A unique index limited to is_current permits repeated inactive duplicates.
Copying the downloaded architecture over the canonical file would discard accepted ADRs.

## Acceptance

Use a disposable PostgreSQL18 instance and actual runtime role connections, with
HUMAUX_REQUIRE_DB=1. Required cases: release roundtrip and transactional rollback; exactly
one event under repeated/concurrent revoke; tenant and visibility negatives; source eligibility
and promotion/revoke ordering; recursive diamond/minimum-depth/stale roots; cycle/unrooted
failure preserving the prior closure; concurrent refresh. Also require independent review,
mutation red/green, fmt/clippy/adapters regression, migration pre/post checks, the RLS matrix,
and typed-pool/architecture checks. Missing dependencies cannot count as pass.
The contract-impact dispatcher must recognize the existing G80-26/G80-40 implementations;
the omission fixture uses that real dispatch rather than a test-only substitute mapping.

This card does not implement the downstream revoke consumer or all Phase9 application
ports, does not enable the Phase10 resident worker, and does not close existing benchmark
or DoD debt. No production migration is authorized by this development acceptance.

## Limits and upgrade signals

Release-bound admission validates USER_CONTRIBUTION sources in the job's tenant context;
already-public claims can compose across tenants in syntheses. Closure table locks may
limit writer concurrency; replace them only after measured contention threatens an actual
SLO, with equivalent snapshot and cycle guarantees. A source or rights correction creates
a new identity rather than rewriting lineage. Production rollout needs a backup, restore
rehearsal, and explicit approval; orphan legacy user sources require true lineage recovery.

## Research provenance

User-provided research/architecture chats were read and compared with current canon:
[security evaluation](https://chatgpt.com/share/6a90850d-88a8-83e8-8d91-18a368024a05),
[architecture evolution](https://chatgpt.com/share/6a908559-fc0c-83e8-9eea-e8716e75ab84).
Ambiguous Card C contracts were separately reviewed in
[web ChatGPT](https://chatgpt.com/c/6a908500-5530-83e8-9dcf-c3d629bbe4f9), then checked against
PostgreSQL18 [LOCK TABLE](https://www.postgresql.org/docs/18/sql-lock.html),
[function volatility](https://www.postgresql.org/docs/18/xfunc-volatility.html), and
[constraints](https://www.postgresql.org/docs/18/ddl-constraints.html). Chat suggestions are
design input, not authority over the repository contracts or executable acceptance.
