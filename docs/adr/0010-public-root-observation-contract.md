# ADR-0010: Reconcile the Phase9 root probe with typed public sources

Status: accepted for isolated implementation; production rollout requires separate approval.

## Decision

The source-kind contract stays in Baseline_2.9.md §12, the structural phase probe in
§57.1, and full closure acceptance in §69 DOD-054. The phase probe previously required
every public root to be a private contribution release, contradicting the explicitly
allowed official/document/web/import source types. It now refers to the canonical
typed roots. A user contribution still requires its authorized release; a different
source type must not fabricate one merelyto satisfy a probe.

The runtime provenance probe independently compares actual graph roots with closure
root/depth pairs. It checks typed-root shape without reading private evidence. A
withdrawn but traceable root is structurally distinct from current serving eligibility.

## Rejected alternatives and verification

We do not copy the entire closure contract into the phase-presence probe, weaken the
USER_CONTRIBUTION release boundary, or introduce a speculative parent trigger merely
to turn a gate green. Production admission already writes a claim and its source edge
atomically; any newly demonstrated orphan creation path must be fixed at that path.

Real PostgreSQL fixtures must include mixed typed roots, shortest-depth drift, cycles,
unrooted branches, a corrupt user root and withdrawal. A source-copy predicate removal
must turn the unchanged bad-root test red, with constraints and bytes restored afterward.
Fixture support for an official source is not a claim that its acquisition ingress exists.
