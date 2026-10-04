# ADR-0037 — Liveness, readiness, and the supervision contract for the five processes

- Status: accepted
- Date: 2026-09-09
- Card: 15
- Spec: §4.2 (minimal process set), §4.4 (probe contract and its 坑5), §6.2.2/§6.2.3 (role
  grants and assertion E), §57.1 (three-state verdicts must name the missing object), §83.4
  (Layer 1B intra-Cell resources), §78.1 (no hardcoded business config)
- Supersedes nothing. Composes with ADR-0016 (distill leases), ADR-0036 (tenant-free derived
  dispatch and its `ops.jobs` leases).

## Context

Before this card nothing in the system could answer "is it up and healthy". `humaux-admin q`
documented itself as a §4.4 placeholder that exited non-zero for every name except
`cell.resources`, with one blanket reason ("probe backend not wired yet") that told an operator
nothing about *what* was missing. Only the gateway had graceful shutdown. Stable operation is
one of the delivery acceptance terms, and a supervisor cannot restart what it cannot probe —
nor can the soak harness assert liveness during a run.

## Decision

### D1 — Readiness is a probe of live dependencies, not a self-report

Each of the four workers gains a `--readyz` mode that performs **one live round trip per
dependency it cannot work without** and exits: `0` = ready, non-zero with the missing object
**named** on stderr. The gateway gains `GET /livez` and `GET /readyz`.

Probed dependencies, and what is deliberately excluded, are tabulated in
`docs/ops/supervision.md` §1. The two exclusions that are decisions rather than omissions:

- **No probe performs a claim.** `ops.claim_derived_work` (migration 0164) and the `ops.outbox`
  claim are SECURITY DEFINER *writes*. A readiness probe that called one would take real work
  off the queue and hand it to a process that is about to exit. Readiness therefore asserts the
  readable half — the role connects and `current_user` matches (§6.2.3 assertion E) — and lets
  the first real pass prove the callable half.
- **No probe spends a provider call.** A readiness probe that costs money is a probe nobody
  polls, which is worse than no probe.

Both derived workers' readiness is **tenant-free** (ADR-0036 left no tenant/domain/binding id in
their environment): "can I claim at all", never "is tenant X's pair healthy". No readiness path
reads a configuration key its process does not already need for its run modes.

### D2 — A missing object is never a zero

The §4.4 discipline is now carried by the type system rather than by convention.
`bins/admin/src/probe.rs`'s `ProbeOutcome` is a two-valued enum: `Reading { value, scanned_n, … }`
or `MissingObject(String)`. The `MissingObject` arm **has no `value` field**, so "could not reach
the object, reported 0" cannot be written in that module; and `Reading`'s `scanned_n > 0` is
asserted for every catalog entry by `no_probe_reports_a_reading_it_did_not_scan`.

The same shape is applied to readiness: a worker whose dependency is down prints
`not ready — missing object: <the thing>` and exits non-zero. It never prints a healthy envelope
with a zero in it.

### D3 — Graceful shutdown composes with leases by observing the signal BETWEEN passes

The two resident derived workers (`humaux-consolidation-worker --serve`,
`humaux-private-worker --distill-serve`) latch SIGTERM/Ctrl-C at process start and act on it in
the poll wait, not inside a pass.

**Both** handlers are installed with `tokio::signal::unix::signal(...)` at start —
`tokio::signal::ctrl_c()` is not used on unix, in any of the five processes. It is an `async fn`
whose body registers the SIGINT handler on the future's *first poll*, and in every one of these
loops the first poll happens only after a pass has already returned. Until card 15's review that
made the "latched before the first pass" claim true for SIGTERM and false for Ctrl-C: a Ctrl-C
during the first pass hit SIGINT's default disposition and killed a worker holding a claimed job
— the exact state this decision exists to prevent. `sigint_mid_pass_…` /
`sigint_mid_serve_…` are the tests that hold it. A pass settles every job it claimed before it returns (card 14
/ ADR-0016 leases), so exiting between passes can never leave a job `PROCESSING` with a live
lease that only expiry could free. Cancelling a pass mid-flight could — which is precisely the
failure this card's acceptance gate forbids.

The consequence is stated rather than hidden: **shutdown latency is bounded by one pass, not by
the signal.** A supervisor grace period shorter than one pass degrades a graceful shutdown into
a SIGKILL. That is still safe (the lease expires and another worker reclaims the job) but slow,
so `docs/ops/supervision.md` §3 makes the grace-period sizing an explicit deployment
requirement.

The two RPC listeners (`--serve-rpc` on the private and retrieval workers) hold no lease of
their own, so their drain is the ordinary one: stop accepting, let in-flight calls finish
(retrieval, via axum's `with_graceful_shutdown`) or fail the one call in flight (private, via a
`select!` on the accept loop) — a failed inference hop is settled by the *caller's* lease on the
consolidation side, not left dangling here.

`humaux-public-worker` has no loop; its whole lifetime is one bounded pass holding an
`ops.outbox` lease. A signal during that pass is **latched and logged, never acted on**: for a
bounded one-shot, "drain" means "let the pass settle its lease and exit zero". The latch exists
so that decision is visible in the log instead of inferred from silence.

### D4 — The gateway's `/readyz` flips to 503 and then keeps accepting for a drain window

Readiness goes false the moment the signal is observed, and the process **keeps accepting new
connections for `DRAIN_ANNOUNCE_WINDOW` (5 s, `bins/gateway/src/main.rs`) afterwards**, so a
supervisor polling `/readyz` during the drain sees `503 draining` rather than a refused
connection it cannot tell apart from a crash.

The window is the decision, not an implementation detail. Returning from the future passed to
`with_graceful_shutdown` is what stops axum accepting, so flipping the flag and returning as
adjacent statements makes the 503 branch reachable **only to an already-established keep-alive
connection**. The consumer this route is written for — a k8s `readinessProbe.httpGet`, or the
`curl` loop in `docs/ops/supervision.md` §7 — opens a fresh TCP connection per poll and would
have received `ECONNREFUSED` every time. That was the state card 15's review found: the 503
contract asserted in three places and unreachable for every supervisor that dials.

Consequences, stated rather than hidden: a graceful stop of the gateway now takes at least the
drain window, so `terminationGracePeriodSeconds` (and systemd's `TimeoutStopSec`) must exceed
the window **plus** the longest in-flight request; and the readiness poll period must be shorter
than the window, or a supervisor can still step over the whole draining state.

`/livez` and `/readyz` are merged onto the router **outside** `native_request_boundary`. They
carry no tenant data, no config fingerprint, and no state mutation — a constant token and a
status code — so the boundary's Host/Origin allowlist (DNS-rebinding protection for the MCP
surface) has nothing to protect, while a supervisor probing over the pod IP has no Host header
the allowlist could be configured to accept. Anything that would report *what* this process is
belongs behind the boundary or in `humaux-admin q`.

### D5 — `deploy.binary` refuses to answer rather than answering "unknown"

`humaux-admin q deploy.binary` reports the git sha / build time burned in at **compile** time
(`HUMAUX_BUILD_GIT_SHA`, `HUMAUX_BUILD_TIME`) plus the crate version. Read at runtime instead, an
environment variable would let a process claim to be any revision, which is the very "名与实对
不上" §4.4 坑4 exists to catch. When the sha was not burned in, the probe emits `MissingObject`
rather than `"unknown"`: a `deploy.binary` that always answers is worse than none, because it
keeps liveness green while three arms run an old image.

**Something has to burn it in.** Card 15's review found that nothing in the repository ever set
`HUMAUX_BUILD_GIT_SHA`, so the one probe Baseline §4.4 and the runbook both list as *live*
answered `missing object` in every build the workspace produced — a probe documented as wired and
observed as unwired. `bins/admin/build.rs` now supplies it from `git rev-parse HEAD` when the
environment did not already carry one; an explicit release value still wins, and a build with no
checkout to read (a vendored tarball) emits nothing and the probe keeps refusing, which is the
correct answer there. The refusal arm and the reading arm are both covered by
`deploy_binary_reads_the_burned_in_sha_and_refuses_without_one`, which drives the compile-time
facts as data — they were previously unreachable from a test, because `deploy.binary` sits in
`WIRED_PROBES` and both catalog-wide tests skip it.

### D6 — The four-OS-user requirement is part of the supervision contract

Both UDS hops authenticate the calling *process* through `UnixStream::peer_cred()`
(`bins/retrieval-worker/src/rpc.rs` checks `expected_gateway_uid`;
`bins/private-worker/src/inference_rpc.rs` checks `expected_consolidation_uid`). If any two of
{gateway, retrieval-worker, private-worker, consolidation-worker} share a uid, that check
compares a uid against itself and admits anything that user can run. It does not fail loudly; it
silently stops being a check. The four therefore MUST run as four distinct OS users, and
`docs/ops/supervision.md` §5 carries the per-host verification.

## What is NOT wired, and the unlock condition for each

When card 15 closed, nine of the eleven §4.4 probes reported `MissingObject` (ADR-0061 D-J has since unlocked six; see the table). That is the spec-conformant answer,
not a placeholder: each names the specific object this process lacks. What none of them can do
is *read the database*, and that is a structural boundary, not an omission:

- `role_admin` holds only the §6.2.2 observation SELECT grants — verified against the live
  schema: `ops.mechanism_observations` and `ops.mechanism_e2e_runs`, nothing else. Every DB probe
  needs a grant it does not have.
- G80-40 (`xtask architecture-check`) confines raw `sqlx::PgPool` to
  `crates/adapters/src/postgres.rs`, and every typed pool's `pool()` accessor is `pub(crate)`.
  `humaux-admin` therefore has no query surface at all, and cannot acquire one without either a
  new read function in `humaux-adapters` or a direct `sqlx` dependency that the gate would red.
- Four of the six DB-backed probes read RLS-scoped tables (`ops.jobs`, `ops.outbox`,
  `projection.stream_checkpoints`, `private.artifacts`). Read cross-tenant with no tenant
  context they return zero rows — which is exactly the `scanned_n = 0` §4.4 forbids a probe from
  reporting as a fact. They need `--arg tenant=<uuid>` in the `q` contract's argument slot plus a
  session-context setter, not just a grant.

| probe | unlock condition |
|---|---|
| `public.corroborated` / `public.consensus_ready` | the §7.6 GA columns `public.claims.corroboration` / `.contributor_set` must exist, **and** a read path must exist for this process. **Still refused** after ADR-0061: the columns do not exist, so the probe exits non-zero naming them (§4.4 line 883). |
| `stream.watermark`, `outbox.backlog`, `jobs.stuck` | **Unlocked by ADR-0061 D-J** as an aggregate definer instead of table grants plus a tenant argument: `ops.admin_probe_snapshot()` (migration 0210, owner the NOLOGIN `role_health_reader`, EXECUTE `role_admin` only) returns cross-tenant counts and no tenant id; read through `adapters::health::read_admin_probe_snapshot`. |
| `parse.poison` | **Still refused** after ADR-0061: no POISON state or `limit_hit` column exists anywhere; the probe names `limit_hit`. |
| `degrade.counters` | **Unlocked by ADR-0061 D-J**: each process serves its counters on its loopback `/status` (ADR-0061 D-B); the probe sums every `HUMAUX_ADMIN_OPS_ADDRS` entry and refuses, naming it, when one is unreachable. |
| `flags.effective` | **Unlocked by ADR-0061 D-J**: the gateway publishes its resolved `effective_config` (source per entry, no secret value) on its loopback `/status`. |
| `tls.expiry` | **Unlocked by ADR-0061 D-J**: certificate paths in `HUMAUX_ADMIN_TLS_CERT_PATHS`, parsed with `x509-cert`. |

The table above was written for card 15, whose allowed files excluded `migrations/`, `crates/adapters/` and
`bins/admin/Cargo.toml`; card 34 (ADR-0061) opened them. After it the catalog is 8 Readings and 3 typed refusals.

## Acceptance evidence

- `bins/admin/src/probe.rs` unit tests, including the card's required fault injection:
  `every_unwired_probe_names_its_missing_object` (flip any unwired probe to a `Reading` — a
  `value = 0` one included — without adding it to `WIRED_PROBES` ⇒ red, naming that probe) and
  `no_probe_reports_a_reading_it_did_not_scan` (`scanned_n == 0` ⇒ red).
- `bins/admin/src/probe.rs`: `deploy_binary_reads_the_burned_in_sha_and_refuses_without_one`
  (both arms of the one probe `WIRED_PROBES` makes the catalog-wide tests skip) and
  `this_build_burned_in_a_sha_when_it_had_a_checkout_to_read` (the `build.rs` ↔ probe wiring —
  delete `bins/admin/build.rs` in a git checkout ⇒ red).
- `bins/consolidation-worker/tests/derived_dispatch_e2e.rs`:
  `readyz_binary_probes_its_uds_peer_and_names_it_when_down`,
  `readyz_binary_names_postgresql_when_the_database_is_down`,
  `sigterm_mid_pass_drains_and_leaves_no_job_processing_with_a_live_lease` and its
  `sigint_…` twin — all spawn the real binary (the mode's exit path lives in `src/main.rs`,
  which no in-process test reaches), the last two asserting with SQL on `ops.jobs` after the
  process has exited.
- `bins/private-worker/tests/derived_dispatch_e2e.rs`:
  `sigterm_mid_serve_drains_and_leaves_no_job_processing_with_a_live_lease` and its `sigint_…`
  twin — the same invariant for the OTHER lease-holding resident loop, which settles both an
  `ops.jobs` lease and a per-tenant `ops.outbox` lease per pass (ADR-0016 D5); both leases are
  asserted clear from SQL after exit.
- `bins/public-worker/tests/readyz_probe.rs` and `bins/retrieval-worker/tests/readyz_probe.rs`:
  `readyz_names_postgresql_when_the_database_is_down`,
  `readyz_names_the_qdrant_cell_resource_when_qdrant_is_down`, and the up-direction
  `readyz_exits_zero_when_postgresql_and_qdrant_both_answer` — the Qdrant arm of the readiness
  table had no test of any kind before.
- `bins/gateway/tests/mcp_gateway.rs`: the binary supervision block now sends SIGTERM and then
  polls `/readyz` on a **fresh TCP connection**, asserting `503` rather than a refused
  connection — the D4 claim, previously asserted in prose only (the existing assertion covered
  the 200 path).

Every "down" in the list above is a real absence produced by pointing the probe at a loopback
port that was bound and released, never a mock — and never by stopping the shared PostgreSQL or
Qdrant container, which is production-shared state for every other suite on the machine.

Still asserted only by construction, and recorded here rather than left implicit:
`humaux-retrieval-worker --serve-rpc`'s drain. Reaching its accept loop requires a live
embedding provider (`build_embedding_provider`), so a spawn-and-signal test would assert the
provider bootstrap, not the drain. Its shutdown path is axum's own `with_graceful_shutdown` —
the identical construct the gateway's drain test does exercise end to end — and it holds no
lease, so there is no SQL invariant to read afterwards. `humaux-public-worker` likewise has no
loop to interrupt: its whole lifetime is one bounded pass, and the latch is only read after that
pass has settled.

## Latency recorded by this card (card 24 delivery baseline)

Measured on the card's own rehearsal environment (macOS aarch64, PostgreSQL 18 and Qdrant 1.19
in local containers, debug build — so these are ceilings, not release numbers). Every binary was
warmed once first, so no sample carries the macOS XProtect first-exec assessment.

| what | n | p50 | p95 | notes |
|---|---:|---:|---:|---|
| `humaux-consolidation-worker --readyz` (process spawn → exit) | 30 | 46 ms | 49 ms | one `role_consolidation_worker` connect + `current_user` check + one UDS peer dial |
| `humaux-admin q <probe>` on a `MissingObject` path | 30 | 3.6 ms | 5.7 ms | no I/O by construction — this is the process's own start-to-exit floor |
| `humaux-admin q deploy.binary` (now a `Reading`, sha burned in by `build.rs`) | 30 | 3.0 ms | 4.9 ms | still no I/O: the sha is a compile-time constant, so wiring the probe cost nothing at runtime |
| `humaux-gateway` SIGTERM → exit | — | ≥ 5 s by construction | — | **a deliberate floor, not a measurement**: `DRAIN_ANNOUNCE_WINDOW` (5 s) is announced-draining time, not work. It is a sizing input for `terminationGracePeriodSeconds`, not a regression to optimise |
| `native_gateway_semantic_recall_real_qdrant_pg_and_ryw_acceptance` | 1 | 40.9 s | — | unchanged hop; recorded because this card touches the gateway's router and the retrieval worker's RPC listener |

Two readiness numbers an operator has to size probes against, both observed:

- **A down Qdrant fails fast**: the intra-Cell HTTP call returns a connect error immediately
  (well under a second).
- **A down PostgreSQL fails SLOWLY**: `*DbPool::connect` waits out the sqlx pool acquire timeout
  (~30 s observed) before reporting. A readiness probe's own timeout must therefore exceed that,
  or the supervisor will read "probe timed out" instead of the named missing object the probe was
  about to print. This is the one place where the supervisor's configuration can hide the whole
  point of naming the object.

## Known ceilings

1. Shutdown latency is one pass (D3), by choice. Upgrade path: pass a cancellation token into
   `dispatch_pass` so it stops *claiming* new jobs mid-pass while still settling the ones it
   holds. That is a `crates/`-level change.
2. `--readyz` proves connectivity and role identity, not that the next claim will succeed. Only
   a write could prove that, and a probe must not write (D1).
3. The `Shutdown` latch is duplicated across four `main.rs` files (~15 lines each). There is no
   shared crate all five binaries depend on that carries a tokio dependency it would be
   appropriate to extend; deduplicating means creating one. Not worth it for 60 lines —
   revisit when a fifth process needs the same shape.
