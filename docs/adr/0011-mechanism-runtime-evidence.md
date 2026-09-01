# ADR-0011: Read actual mechanism observations and retain execution witnesses

Status: accepted for isolated implementation; production rollout requires separate approval.

## Decision and rationale

The normative homes are Baseline_2.9.md §1.14.1 (runtime evidence), §4.4 (probe
envelopes), §6.2 (roles and grants), and §48 (schema inventory). This ADR does not
provide another registry, threshold table or activation policy.

The observation table already existed, but the registry checker returned a fixed
“not deployed” result without querying it. Targeted reads and recomputation replace
that stub. A stored status or two adjacent values cannot establish that an E2E run
actually bracketed the observations. A narrow immutable run link records the pair;
the measuring runner binds the scope, probe version, build and execution interval.
Legacy observations are retained without invented scan hashes or run receipts.

The admin executable needs a real read-only role. Reusing the maintenance identity
would keep repair rights even if a particular transaction happened to be read-only.
The dedicated identity receives only the two observation-related SELECT grants.
The writer remains an operational maintenance path, never a request-path pool.

The public recorder freezes deployment/build at bootstrap, takes the cell from the
existing resource registries, and checks both pools' actual database/server identity.
No target/build override is accepted per review. This uses the existing configuration
trust boundary instead of adding a deployment registry table. The reader independently
checks the run's own scope hash against both observations, even when the insert guard
already performed the same check. Business review and observation receipt commits are
separate: a receipt failure is not a rollback guarantee for an already committed review.

We reject bootstrap-derived status, self-reported ACTIVE, unlinked counter changes,
and a generic SQL/shell execution API presented as an E2E recorder. The record proves
the controlled runner's execution and measurements, not a causal benchmark or model
quality claim. A compromised privileged operator is outside this evidence boundary.

## Verification and limits

The gates cover target isolation, empty/stale data, forged cached status, zero delta,
scope/version/build mismatch, role denial, immutable receipts and SUT mutations.
Evidence must come from an isolated PostgreSQL cluster: adding a role to a cluster
shared with an older frozen schema would contaminate its role-set acceptance.
Actual collector coverage and target activation are reported separately; implementing
the reader does not activate Phase10 or establish a complete system delivery.
