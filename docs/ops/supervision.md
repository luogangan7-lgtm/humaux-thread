# Supervision runbook — the five processes

> Scope: §4.2's minimal process set, §4.4's probe contract, ADR-0037. This file is the
> operational half of card 15; the architectural decisions and their limits are in
> `docs/adr/0037-liveness-readiness-and-supervision.md`. Spec § numbers are references, never
> copies — judgement text lives in `docs/architecture/Baseline_2.9.md`.

**Environment variables.** This runbook does not list them. The one complete list is
[`docs/architecture/env_vars.md`](../architecture/env_vars.md), generated from the code by
`cargo xtask dep-map --write`. It covers every variable, which process and module reads it, and
whether the typed registry declares it. `cargo xtask dep-map --check` fails when the list and the
code drift apart. Look a variable up there rather than copying it here. One gateway fact that
list cannot show: since card 29 (ADR-0054) the gateway needs **no default write pair** — every
write derives (tenant, workspace) per request, and the two `REMEMBER_TENANT_ID` /
`REMEMBER_WORKSPACE_ID` keys are accepted-and-ignored (e2e tooling only).

## 1. The five processes and how each answers "are you up"

| process | liveness | readiness | resident? |
|---|---|---|---|
| `humaux-gateway` | `GET /livez` → 200 | `GET /readyz` → 200 / 503 | yes (HTTP accept loop) |
| `humaux-retrieval-worker` | process alive | `--readyz` exit 0 | `--serve-rpc` yes; `--serve` yes (the projection runner, ADR-0052); `--run-once` no |
| `humaux-private-worker` | process alive | `--readyz` exit 0 | `--serve-rpc` / `--distill-serve` yes |
| `humaux-consolidation-worker` | process alive | `--readyz` exit 0 | `--serve` yes |
| `humaux-public-worker` | process alive | `--readyz` exit 0 | no — one bounded pass per invocation |
| `humaux-maintenance` | `health serve`: process alive | `health serve`: `GET /metrics` on its ops address → 200 / 503 | `health serve` yes (card 34, ADR-0061 D-D); every other subcommand no (card 28); the resident `--serve` job is card 35 |

`humaux-maintenance health serve` (ADR-0061 D-D) **is** a supervised unit: the one process that samples
the §41.2 SQL-derived health gauges (`jobs_*`, `oldest_pending_age_seconds`, `projection_lag_events`,
`processing_gap_count`, `data_disclosures_*`) through `ops.health_snapshot()` as `role_maintenance`,
every `HUMAUX_MAINTENANCE_HEALTH_SAMPLE_SECONDS`, and serves the last sample on the loopback
`HUMAUX_MAINTENANCE_HEALTH_SERVE_METRICS_ADDR` (`/metrics`, `/status`). Both keys are required, with no
default. The first sample is taken before the port binds, so a boot that cannot sample exits non-zero
naming the failure. A scrape never runs SQL. A failed sample, or one older than 2 × the interval, makes
`/metrics` answer 503 with the reason and the age — never the last good values. Run exactly one
instance: two would export every gauge twice.

### Ops listeners: `/metrics` and `/status` (card 34, ADR-0061 D-B)

Every resident mode opens one **loopback** ops listener on its own required key — one key per
mode, never per binary, because two modes of one binary run at the same time from one environment.
It serves `GET /metrics` (Prometheus text format 0.0.4, every label value pre-seeded at 0 so the
first scrape already has a sample per family) and `GET /status` (JSON: `process`, `mode`,
`crate_version`, `git_sha`, `started_at`, `uptime_seconds`, the 11 `degrade` codes with their count
and `last_fired_at`; the gateway adds `accepting`, `readiness` and `effective_config`, where a
secret entry carries `secret: true` and never a value). A non-loopback or missing address is
boot-fatal naming the key; there is no default port and no fallback to `:0`. One-shot modes
(`--readyz`, `--run-once`, `--distill-once`, every other maintenance subcommand) open nothing.

| process / mode | ops key | families on `/metrics` |
|---|---|---|
| gateway | `HUMAUX_GATEWAY_METRICS_ADDR` | 9: `degrade_total`, `humaux_retrieval_requests_total`, `retrieval_completeness_total`, the six guard families |
| retrieval-worker `--serve-rpc` | `HUMAUX_RETRIEVAL_WORKER_SERVE_RPC_METRICS_ADDR` | 4 `retrieval_provider_*` |
| retrieval-worker `--serve` | `HUMAUX_RETRIEVAL_WORKER_SERVE_METRICS_ADDR` | 4 `retrieval_provider_*` |
| private-worker `--serve-rpc` | `HUMAUX_PRIVATE_WORKER_SERVE_RPC_METRICS_ADDR` | none yet (card 34b) — `up` is its liveness |
| private-worker `--distill-serve` | `HUMAUX_PRIVATE_WORKER_DISTILL_SERVE_METRICS_ADDR` | none yet (card 34b) |
| consolidation-worker `--serve` | `HUMAUX_CONSOLIDATION_WORKER_SERVE_METRICS_ADDR` | none yet (card 34b) |
| maintenance `health serve` | `HUMAUX_MAINTENANCE_HEALTH_SERVE_METRICS_ADDR` | 9 SQL-derived gauges / counters |

`/metrics` and `/status` are **never** on the gateway's `BIND_ADDR` (the MCP surface behind the
reverse proxy): anything mounted there is reachable through the proxy. `BIND_ADDR` keeps `/livez`
and `/readyz`, whose body is a status word only. Each binary prints its zero-state exposition with
`--metrics-families` before reading any configuration; `cargo xtask metrics-registry --check` uses
exactly that to prove every exported family is a §41.2 row.

### The gateway's `/readyz` is dependency-truthful (ADR-0061 D-F, ruling E15)

A background task refreshes a readiness snapshot every `HUMAUX_GATEWAY_READINESS_REFRESH_SECONDS`
(required, > 0); the first snapshot is taken before the listener accepts. Each check is bounded by
that interval: `pg` (`SELECT 1` as `role_gateway`), `retrieval_rpc` (a real
`GET /internal/v1/retrieval/readyz` round trip over the recall socket; the worker answers from one
fresh `role_retrieval_worker` connection and never calls the embedding provider) and `qdrant`
(the gateway's own Qdrant cell resource). With semantic recall disabled the last two are
`not_applicable`. `/readyz` answers 200 `{"status":"ready"}` only while the gateway accepts, every
dependency is `pass` or `not_applicable` and the snapshot is at most 2 × the interval old;
otherwise 503 with `{"status":"not_ready"}`, `{"status":"stale"}` or `{"status":"draining"}`. The
body never names a dependency, a path or an error — those are in the loopback `/status`
(`readiness`: per dependency `state`, `checked_at`, `age_seconds`, `missing_object`). Probe
`/readyz` no faster than the refresh interval buys you nothing: a probe reads the cached snapshot.

Every other `humaux-maintenance` subcommand (§4.2, the operator-write process) is not supervised: every subcommand
(`deploy-init`, `onboard tenant|workspace|user`, `apikey issue|revoke`, `placement ensure`,
`collection ensure`, `activate`, `status`) is one-shot, idempotent and prints one JSON receipt;
exit `0` created/existing, `3` refused with a named reason, `2` usage, `1` infrastructure (the
database or Qdrant it names is down — retry once it is up; a re-run never duplicates a row).
It runs as its own OS user with `role_maintenance`'s DSN and the credential pepper, which no
resident process except the gateway holds. Operating it: `docs/ops/runbook.md` §3 and §6.

The four workers have no HTTP surface, so their readiness is an **exec probe**: run the binary
with `--readyz`, exit 0 = ready. It performs one live round trip per dependency and exits; it
claims nothing, writes nothing, and spends no provider call.

### What each `--readyz` actually touches

| process | dependencies probed | deliberately NOT probed |
|---|---|---|
| retrieval-worker | `role_retrieval_worker` connect + `current_user` (§6.2.3 assertion E); Qdrant cell resource answers a real HTTP GET through the same registry/permit/transport the projection path uses | its own RPC socket (it is that socket's server) |
| private-worker | `role_private_worker` connect + `current_user` | the BYOK provider endpoint — a readiness poll must not spend an inference call; its own RPC socket, same reason as above |
| consolidation-worker | `role_consolidation_worker` connect + `current_user`; the private worker's UDS peer accepts a connection | `ops.claim_derived_work` — it is a SECURITY DEFINER **write** (migration 0164); calling it as a probe would take a real job off the queue and hand it to a process about to exit |
| public-worker | `role_public_worker` connect + `current_user`; Qdrant cell resource answers a real HTTP GET | `ops.outbox` claim — same reason |

A `--readyz` failure prints `not ready — missing object: <the thing that is down>` on stderr and
exits non-zero. **The object is always named.** A probe that cannot reach its dependency must
never report a healthy-but-empty reading — §4.4 坑5, "没有" ≠ "没扫到".

## 2. What a failed probe means, and what to do

| symptom | means | action |
|---|---|---|
| `/livez` connection refused | the gateway process is gone or never bound | restart; check bootstrap stderr for the missing configuration key. A default write pair is **not** one of them since card 29 (ADR-0054): `HUMAUX_GATEWAY_REMEMBER_TENANT_ID` / `_WORKSPACE_ID` are optional and ignored (a present value must still be a UUID — a malformed one is the only way they fail boot) |
| `/readyz` → 503 `draining` | SIGTERM was received; the process is finishing in-flight requests | take it out of rotation; do **not** restart it, it will exit on its own |
| `/readyz` → 503 `not_ready` | a dependency is down: PostgreSQL as `role_gateway`, the retrieval worker's RPC round trip, or Qdrant | **do not restart the gateway** — it is healthy and will turn ready by itself on the next refresh after the dependency returns. On a single node (§67.2) **do not route on it either**: nothing else can take the traffic, and recall already degrades to `LaneSubstituted` while remember / memory.get keep working. Read `curl -fsS 127.0.0.1:<HUMAUX_GATEWAY_METRICS_ADDR port>/status` — `readiness.<dep>.missing_object` names what is down — and fix that. Liveness (`/livez`) is the restart signal, never `/readyz` |
| `/readyz` → 503 `stale` | the readiness refresh task has not produced a snapshot for more than 2 × `HUMAUX_GATEWAY_READINESS_REFRESH_SECONDS` | a stuck refresh is a gateway fault: capture `/status`, then restart the gateway |
| an ops `/metrics` connection refused | the process is down or never bound its ops key (boot stderr names the key) | Prometheus shows `up{job,mode} == 0` for it; restart per §3 |
| `/readyz` connection refused, `/livez` too | the drain window already elapsed, or the process died | treat as gone; restart per §3 |
| `--readyz` names `PostgreSQL as role_*` | the DB is down, the DSN is wrong, or the role does not exist / does not match `current_user` | do not restart the worker in a loop — it will fail identically. Fix PostgreSQL or the DSN first |
| `--readyz` names `the Qdrant cell resource` | Qdrant is down, or its address no longer resolves inside the registered Cell CIDR (§83.4) | check Qdrant, then `humaux-admin q cell.resources` for the declaration-vs-reality breakdown |
| `--readyz` names `the private worker's inference RPC socket` | the private worker is not running, or is running as a different OS user / with a different socket path | start the private worker first (§4 start order); check the socket path and its owner |
| `health serve` `/metrics` → 503 `health sample failed: …` | the last `ops.health_snapshot()` call failed: PostgreSQL is down, the DSN is wrong, or `role_maintenance` lost EXECUTE on the function (the body names it) | do not restart in a loop: the process keeps sampling and returns to 200 by itself on the next good sample. Fix PostgreSQL or the grant. Meanwhile Prometheus marks the gauges stale, and `HealthGaugesAbsent` fires after 2 min |
| `health serve` `/metrics` → 503 `health sample stale` | no good sample for more than 2 × the interval (each sample is bounded by the interval) | as above; if it persists while PostgreSQL is healthy, restart the unit |
| `humaux-admin q <name>` exits non-zero with `missing object` | the probe could not reach its object **at all** | that is data, not a bug: the named object is what has to exist before the probe can answer. See §5 |

### `humaux-maintenance deploy-check` (ADR-0059 D-F; run before traffic and after every rotation)

Read-only; one JSON `{checks:[{name, status, detail}]}`, where `detail` holds role or variable
**names** only. Exit `0` every check passes · `3` a check fails or is `not_applicable` (it names the
missing object) · `1` infrastructure (connect refused, timeout, TLS).

| check | red means | action |
|---|---|---|
| `placeholder_login` | a named role accepted a repository placeholder (`Accepted`), or answered SQLSTATE 28000 (`unverified`: pg_hba rejected the path, or a NOLOGIN role still holds a verifier) | `roles rotate` that role (runbook §10.1); for 28000, add a password-auth pg_hba line for that role from this host |
| `probe_valid` | a random password was not refused with 28P01 — that path does not check passwords (trust/peer) or is unverified, so `placeholder_login` would be vacuous | fix pg_hba for the named role; never mark the check optional |
| `owner_nologin` | `role_migration_owner` can log in | `ALTER ROLE role_migration_owner NOLOGIN PASSWORD NULL` via the migrator principal; find who changed it |
| `schema_migrations_write` | a non-owner role holds INSERT/UPDATE/DELETE/TRUNCATE on `ops.schema_migrations` | revoke it; a forged ledger row would make `migrate` skip a migration (SEC-2) |
| `env_dsn_placeholders` | a `*_PG_DSN` / `DATABASE_URL` in this environment still carries a placeholder password (the variable is named) | rotate (§10.1) and update the named variable from the secrets store |

### Private worker: credential map (ADR-0059 D-I)

| symptom | means | action |
|---|---|---|
| boot exits naming `HUMAUX_PRIVATE_WORKER_CREDENTIALS` | the map is unset, malformed, repeats a reference, or names an unset/empty variable | fix the map; do not restart in a loop. An explicitly empty value boots (every route parks) |
| boot exits naming `HUMAUX_PRIVATE_WORKER_KEY_ENV ... removed` | stale card-32 configuration | delete the variable; put the key in the map |
| boot exits naming `HUMAUX_PRIVATE_WORKER_{PROVIDER_ID,MODEL_ID,MODEL_REVISION,CHAT_URL,CAPABILITIES,EGRESS_PROCESSOR_ID,REGION} ... removed by ADR-0060 D-C` | stale card-33 configuration: provider, model, endpoint, capabilities, recipient and region now come from each call's admitted route | delete the variable; register / bind the route (runbook §3); a recipient goes in `HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS`, a region in `HUMAUX_PRIVATE_WORKER_REGIONS` |
| boot exits naming `HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS`, `_REGIONS` or `_HEALTH_RENEW_SECS` | required since ADR-0060 (no default); malformed, repeated or blank entry | set it; an explicitly empty recipient or region list boots (every route parks with its class) |
| distill jobs `WAITING_KEY`, `last_error_class = CREDENTIAL_NOT_MAPPED` | the route's credential reference is not in this worker's map; no provider call, no ledger row, no attempt was spent | add `<credential_ref>=<ENV_NAME>` to the map and restart (runbook §10.4); the jobs are re-checked every `HUMAUX_PRIVATE_WORKER_DISTILL_NOT_READY_PARK_SECS` |

**Never treat a `missing object` as a zero reading.** If a dashboard shows `0` where a probe
actually failed, the dashboard is broken, not the system: the probes emit no envelope at all on
that path, precisely so a `0` can never be manufactured downstream.

## 3. Restart policy

| process | policy | grace period (SIGTERM → SIGKILL) |
|---|---|---|
| `humaux-gateway` | always restart | ≥ **5 s drain window** + the longest request timeout |
| `humaux-retrieval-worker --serve-rpc` | always restart | ≥ one embedding call |
| `humaux-retrieval-worker --serve` | always restart | **≥ one projection pass** = `HUMAUX_RETRIEVAL_WORKER_BATCH` × worst-case ticket time (embed timeout + 3 × the 10 s Qdrant timeout + PG) |
| `humaux-private-worker --serve-rpc` | always restart | ≥ one inference call |
| `humaux-private-worker --distill-serve` | always restart | **≥ one distill job** = `HTTP_TIMEOUT_SECS` + one `DISTILL_LEASE_SECS` (ADR-0058) |
| `humaux-consolidation-worker --serve` | always restart | **≥ one dispatch pass** |
| `humaux-maintenance health serve` | always restart | ≥ one sample = `HUMAUX_MAINTENANCE_HEALTH_SAMPLE_SECONDS` (each sample is bounded by it); SIGTERM / Ctrl-C exit 0 after the sample in hand |
| `humaux-public-worker --run-once` | on-failure only; it is a scheduled one-shot, exit 0 is success | ≥ one outbox pass |

The bolded rows are the ones that matter. The resident derived-layer workers and the projection
runner observe the termination signal **only between passes**, on purpose: a pass settles every job it claimed
before it returns (card 14 / ADR-0016 leases), so exiting between passes can never leave a job
`PROCESSING` with a live lease that only expiry could free. Cancelling a pass mid-flight could.
The cost is that shutdown latency is bounded by one pass, not by the signal —
**a grace period shorter than one pass converts a graceful shutdown into a SIGKILL, and a
SIGKILL is exactly the case the leases exist to survive** (the job stays `PROCESSING` until its
lease expires, then another worker reclaims it). That is safe but slow; sizing the grace period
correctly is what makes it fast.

`--distill-serve` (ADR-0058) has no pass: IN_FLIGHT seats each claim one job (one Evidence),
work it and claim again. On the signal every seat finishes the job it holds and claims no more,
so the grace period is one job — the HTTP window plus the post-call legs — not a batch. A SIGKILL
mid-call is survived differently from a lease: a job whose request was admitted
(`DISPATCH_INTENT`) is NOT re-sent when its lease expires; it turns `EXECUTION_UNCERTAIN` and
keeps its provider slot until `HUMAUX_PRIVATE_WORKER_DISTILL_HARD_DEADLINE_SECS` (≥ 2 × (HTTP +
lease)), then is re-queued with backoff, classed and counted. Four SIGKILLs mid-call therefore
hold all four slots of the deployment for at most one hard deadline. A job killed before its
request was admitted (`CLAIMED`) is re-queued one lease after the kill.

The projection runner (`--serve`, card 27 / ADR-0052) follows the same rule for its
`projection.stream_log` ticket leases: a pass settles, retries (backoff) or releases every ticket
it claimed before it returns. A SIGKILL leaves the batch leased for `HUMAUX_RETRIEVAL_WORKER_LEASE_SECS`,
after which the restarted runner re-claims it (the claim's expired-lease arm). The lease is a
liveness signal, not a time budget: while a pass runs, a background heartbeat renews EVERY family
it claimed each `LEASE_SECS / 3` (and each ticket's family once more before it is processed), so
the lease only has to outlast `LEASE_SECS / 3` plus one renew round — it is NOT tied to `BATCH` or
to a ticket's worst case. (Before the 2026-09-29 review fix only the family in hand was renewed, so
a batch slower than one lease lost its tail; "above one ticket's worst case" was the wrong rule.)
`LEASE_SECS` is also how long a SIGKILLed batch waits before it is re-claimed. A database outage
is not an exit: the runner logs `projection pass failed` and polls again, so the supervisor never
crash-loops it against an unhealthy PostgreSQL.

`humaux-public-worker` has no loop at all: its whole lifetime is one bounded pass. A signal
during that pass is latched and logged, and the pass is allowed to settle its `ops.outbox` lease
before the process exits zero.

**The gateway's drain window.** On SIGTERM the gateway flips `/readyz` to `503 draining` and
then **keeps accepting for 5 s** (`DRAIN_ANNOUNCE_WINDOW`, `bins/gateway/src/main.rs`) before it
stops the accept loop. That window is the only reason a supervisor can observe the 503 at all:
`readinessProbe.httpGet` opens a new TCP connection per poll, so without it every poll after the
signal would be `ECONNREFUSED` — indistinguishable from the crash this table tells you to restart.
Two consequences:

- set the readiness poll period **below** 5 s, or a supervisor can step straight over the
  draining state;
- size `terminationGracePeriodSeconds` / `TimeoutStopSec` **above** 5 s plus the longest
  in-flight request, or the SIGKILL lands during the announced drain.

**Probe timeouts.** A down Qdrant fails the readiness probe in well under a second, and so does
a PostgreSQL whose port actively refuses the connection. A PostgreSQL that is *unreachable*
rather than refusing (host down, packets dropped) fails SLOWLY — `connect` waits out the pool
acquire timeout (~30 s observed) before it can report. Set the supervisor's probe timeout above
that, or you will read "probe timed out" instead of the named missing object the probe was about
to print.

**Ctrl-C is handled exactly like SIGTERM** in all five processes, and both handlers are installed
before any work starts (`SignalKind::interrupt()`, not `tokio::signal::ctrl_c()` — the latter
registers on first poll, which in these loops is after the first pass has already returned). An
operator's Ctrl-C on a foreground worker therefore drains rather than killing a pass that holds a
lease.

Restart backoff: exponential, and **do not restart faster than the readiness probe can fail**.
A worker whose DB is down will fail `--readyz` in well under a second; a tight restart loop then
becomes a connection-storm against the database that is already unhealthy.

## 4. Dependency start order

```
PostgreSQL ──┬─> humaux-retrieval-worker --serve-rpc ──> humaux-retrieval-worker --serve ──> humaux-gateway
             │        (binds the query-embedding UDS)     (projection runner, ADR-0052)   (dials the UDS)
Qdrant ──────┘
             ├─> humaux-private-worker --serve-rpc ─────> humaux-consolidation-worker --serve
             │        (binds the inference UDS)             (dials it)
             ├─> humaux-private-worker --distill-serve
             ├─> humaux-public-worker --run-once   (scheduled; needs Qdrant + PostgreSQL only)
             └─> humaux-maintenance health serve   (PostgreSQL only; migration 0210 applied)
```

Rules:

1. **PostgreSQL and Qdrant before anything.** Every process's readiness probe fails without them.
2. **A UDS server starts before its client.** The gateway's semantic-recall path dials the
   retrieval worker's socket; the consolidation worker dials the private worker's socket. Start
   the server, wait for its `--readyz` to pass, then start the client.
3. **The projection runner starts after `--serve-rpc` and before the gateway**, gated on its own
   readiness: the process is alive AND `--readyz` (same PostgreSQL + Qdrant round trips) exits 0.
   It holds no start-time tenant binding (no tenant, workspace or collection in its
   environment); first activation of a (tenant, workspace) serving projection stays an operator
   act (`cargo xtask projection-serve`, runbook §6) until card 28.
4. Order within the derived layer is otherwise free: the two derived workers discover work
   cross-tenant through `ops.claim_derived_work` and hold no start-time tenant binding
   (ADR-0036).
5. A supervisor that cannot express ordering can start everything at once and rely on restart:
   the client's first calls fail, the job's lease releases it, and the next pass succeeds. That
   is correct but noisy — prefer readiness-gated ordering.

## 5. The four OS users (non-negotiable for UDS peer-credential validity)

Both UDS hops authenticate the **calling process**, not the request body, through the kernel
peer credential (`UnixStream::peer_cred()`):

| socket | server | accepts only uid | checked in |
|---|---|---|---|
| query-embedding RPC | `humaux-retrieval-worker` | the gateway's uid (its `*_GATEWAY_UID` row in env_vars.md) | `bins/retrieval-worker/src/rpc.rs` |
| private inference RPC | `humaux-private-worker` | the consolidation worker's uid (its `*_CONSOLIDATION_UID` row in env_vars.md) | `bins/private-worker/src/inference_rpc.rs` |

**Therefore the gateway, the retrieval worker, the private worker and the consolidation worker
MUST each run as a distinct OS user.** If any two of the four share a uid, the peer-credential
check they perform on each other becomes vacuous — it compares a uid against itself and admits
any process that user can run. The check does not fail loudly in that configuration; it silently
stops being a check, which is the worst shape a control can take.

`humaux-public-worker` is the fifth process and dials no UDS, so it is not part of this
requirement; give it its own user anyway for the ordinary database-role reason (§6.2.0: no
runtime role shares a pool or a credential).

Deployment check, per host: `ps -o user,comm` over the four must print four distinct users.

## 6. `humaux-admin q <name>` — the §4.4 probe catalog

11 frozen probe names (§4.4). Every one either returns the frozen envelope

```json
{ "value": …, "scanned_n": …, "scope_hash": "sha256:…", "checked_at": "…", "probe_version": "name@n" }
```

or exits non-zero having **named the object it could not reach**. There is no third answer, and
`value = 0` is never how a probe reports "I could not look".

Since card 34 (ADR-0061 D-J, ruling E2): **8 Readings and 3 refusals that name their object.**

| probe | today | needs |
|---|---|---|
| `stream.watermark` | streams whose projection highwater is behind the issued one, of all streams | `HUMAUX_ADMIN_PG_DSN` (`role_admin`; one call of the aggregate definer `ops.admin_probe_snapshot()`) |
| `outbox.backlog` | undelivered rows, of the whole outbox (detail: oldest undelivered age) | the same |
| `jobs.stuck` | `PROCESSING` jobs with an expired lease, of all jobs (detail: in-lease count) | the same |
| `degrade.counters` | Σ `degrade_total` over every listed process (detail: per code and process, `last_fired_at`) | `HUMAUX_ADMIN_OPS_ADDRS` = `name=127.0.0.1:port,…`; one unreachable process refuses the probe, never a partial sum |
| `flags.effective` | gateway `effective_config` entries whose source is `env`, of all entries | the same, with exactly one gateway entry |
| `deploy.binary` | the git sha / build time burned in at compile time, plus the crate version | — |
| `tls.expiry` | certificate files inside the 21-day WARN window, of the files listed | `HUMAUX_ADMIN_TLS_CERT_PATHS` |
| `cell.resources` | healthy resources of all three `IntraCellResource`s: `QDRANT_REST` by DNS + a real intra-Cell HTTP call cross-checked against the §83.4 registry declaration; `RETRIEVAL_EMBEDDING_RPC` / `PRIVATE_INFERENCE_RPC` by a connect to their Unix socket (2 s; the listener accepting is "reachable" — the peer-uid check after accept is not probed). `detail.unhealthy` names the rest | `HUMAUX_CELL_*`, `HUMAUX_QDRANT_*`, `HUMAUX_ADMIN_RETRIEVAL_RPC_SOCKET_PATH`, `HUMAUX_ADMIN_PRIVATE_INFERENCE_RPC_SOCKET_PATH` (the paths given to the gateway / the private worker; a missing key refuses, naming it) |
| `public.corroborated` / `public.consensus_ready` | exit non-zero naming `public.claims.corroboration` / `public.claims.contributor_set` (§4.4 freeze: those columns do not exist) | — |
| `parse.poison` | exit non-zero naming the `limit_hit` column (no POISON state exists anywhere) | — |

An empty denominator table is a refusal naming the table (`… has no rows — nothing scanned`), never
`0/0`.

`deploy.binary` answers only when the build burned in the git sha (build-time variables of
`admin::build`, see env_vars.md). `bins/admin/build.rs` fills it from `git rev-parse HEAD` when the build
environment did not supply one, so an ordinary `cargo build` inside a checkout already answers.
**Release builds should still set it explicitly** — the build then does not depend on a `.git`
directory being present, and an explicit value always wins:

```sh
HUMAUX_BUILD_GIT_SHA="$(git rev-parse HEAD)" \
HUMAUX_BUILD_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  cargo build --release -p humaux-admin
```

Without it the probe refuses to answer rather than printing `"unknown"` — §4.4 坑4 is precisely
the failure where three arms all ran an old image while liveness stayed green. A probe that
cannot name the revision it is running must say so.

## 7. Minimal supervisor wiring

systemd (one unit per process; the ordering of §4 expressed as `After=`/`ExecStartPre=`):

```ini
[Service]
User=humaux-consolidation
ExecStartPre=/usr/local/bin/humaux-consolidation-worker --readyz
ExecStart=/usr/local/bin/humaux-consolidation-worker --serve
Restart=always
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=<longer than one dispatch pass>
```

Kubernetes: `/livez` as `livenessProbe.httpGet`, `/readyz` as `readinessProbe.httpGet` for the
gateway (`periodSeconds` below the 5 s drain window, so the draining state is actually observed);
`exec: ["…-worker", "--readyz"]` as `readinessProbe` for the workers.
`terminationGracePeriodSeconds` must exceed one pass for the two resident derived workers, and
the drain window plus the longest request for the gateway.

## 8. Observability units (§67.2, ADR-0061 D-G)

Three more supervised units on the same host, every listener on loopback by an **explicit** flag:

| unit | listen | notes |
|---|---|---|
| Prometheus | `--web.listen-address=127.0.0.1:9090`, `--storage.tsdb.retention.time=30d` | scrapes the seven ops listeners above (`targets/*.json`, written by the deployer from the seven `*_METRICS_ADDR` values, labels `{job: humaux-<process>, mode: <mode>}`) and the collector; loads `invariants.rules.yml` + `alerts.rules.yml`; **no** remote-write / OTLP receiver, admin or lifecycle flag (`deploy/prometheus/check-compose.sh` refuses them); reload = SIGHUP |
| Alertmanager | `--web.listen-address=127.0.0.1:9093 --cluster.listen-address=` (empty: no gossip listener) | default route → the log sink, `Watchdog` → its own receiver every 5 min; both URLs come from `url_file`s outside the repo |
| OTel Collector (core) | OTLP `127.0.0.1:4317/4318`, own telemetry `127.0.0.1:8888` | terminates OTLP at `debug`; carries no application traffic until an SDK producer exists; `up{job="otelcol"}` is its liveness |

Restart policy: always restart, each. Start them after PostgreSQL and before traffic; they depend on
nothing in the app. Prometheus's `prometheus.yml` carries `external_labels.git_sha:
"__HUMAUX_GIT_SHA__"`: the deployer **must** replace it with the `git_sha` of
`humaux-admin q deploy.binary`, so the Watchdog ping names the running revision (§42.1, §67.4).
Verification is runbook §7. The rehearsal (`docs/ops/rehearse.sh` steps `observability`,
`metrics_scrape`, `alert_drill`) runs the same configs from the pinned host binaries.
