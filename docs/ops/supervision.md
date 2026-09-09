# Supervision runbook — the five processes

> Scope: §4.2's minimal process set, §4.4's probe contract, ADR-0037. This file is the
> operational half of card 15; the architectural decisions and their limits are in
> `docs/adr/0037-liveness-readiness-and-supervision.md`. Spec § numbers are references, never
> copies — judgement text lives in `docs/architecture/Baseline_2.9.md`.

## 1. The five processes and how each answers "are you up"

| process | liveness | readiness | resident? |
|---|---|---|---|
| `humaux-gateway` | `GET /livez` → 200 | `GET /readyz` → 200 / 503 | yes (HTTP accept loop) |
| `humaux-retrieval-worker` | process alive | `--readyz` exit 0 | `--serve-rpc` yes; `--run-once` no |
| `humaux-private-worker` | process alive | `--readyz` exit 0 | `--serve-rpc` / `--distill-serve` yes |
| `humaux-consolidation-worker` | process alive | `--readyz` exit 0 | `--serve` yes |
| `humaux-public-worker` | process alive | `--readyz` exit 0 | no — one bounded pass per invocation |

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
| `/livez` connection refused | the gateway process is gone or never bound | restart; check bootstrap stderr for the missing configuration key |
| `/readyz` → 503 `draining` | SIGTERM was received; the process is finishing in-flight requests | take it out of rotation; do **not** restart it, it will exit on its own |
| `/readyz` connection refused, `/livez` too | the drain window already elapsed, or the process died | treat as gone; restart per §3 |
| `--readyz` names `PostgreSQL as role_*` | the DB is down, the DSN is wrong, or the role does not exist / does not match `current_user` | do not restart the worker in a loop — it will fail identically. Fix PostgreSQL or the DSN first |
| `--readyz` names `the Qdrant cell resource` | Qdrant is down, or its address no longer resolves inside the registered Cell CIDR (§83.4) | check Qdrant, then `humaux-admin q cell.resources` for the declaration-vs-reality breakdown |
| `--readyz` names `the private worker's inference RPC socket` | the private worker is not running, or is running as a different OS user / with a different socket path | start the private worker first (§4 start order); check the socket path and its owner |
| `humaux-admin q <name>` exits non-zero with `missing object` | the probe could not reach its object **at all** | that is data, not a bug: the named object is what has to exist before the probe can answer. See §5 |

**Never treat a `missing object` as a zero reading.** If a dashboard shows `0` where a probe
actually failed, the dashboard is broken, not the system: the probes emit no envelope at all on
that path, precisely so a `0` can never be manufactured downstream.

## 3. Restart policy

| process | policy | grace period (SIGTERM → SIGKILL) |
|---|---|---|
| `humaux-gateway` | always restart | ≥ **5 s drain window** + the longest request timeout |
| `humaux-retrieval-worker --serve-rpc` | always restart | ≥ one embedding call |
| `humaux-private-worker --serve-rpc` | always restart | ≥ one inference call |
| `humaux-private-worker --distill-serve` | always restart | **≥ one distill pass** |
| `humaux-consolidation-worker --serve` | always restart | **≥ one dispatch pass** |
| `humaux-public-worker --run-once` | on-failure only; it is a scheduled one-shot, exit 0 is success | ≥ one outbox pass |

The two bolded rows are the ones that matter. Both resident derived-layer workers observe the
termination signal **only between passes**, on purpose: a pass settles every job it claimed
before it returns (card 14 / ADR-0016 leases), so exiting between passes can never leave a job
`PROCESSING` with a live lease that only expiry could free. Cancelling a pass mid-flight could.
The cost is that shutdown latency is bounded by one pass, not by the signal —
**a grace period shorter than one pass converts a graceful shutdown into a SIGKILL, and a
SIGKILL is exactly the case the leases exist to survive** (the job stays `PROCESSING` until its
lease expires, then another worker reclaims it). That is safe but slow; sizing the grace period
correctly is what makes it fast.

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
PostgreSQL ──┬─> humaux-retrieval-worker --serve-rpc ──> humaux-gateway
             │        (binds the query-embedding UDS)     (dials it)
Qdrant ──────┘
             ├─> humaux-private-worker --serve-rpc ─────> humaux-consolidation-worker --serve
             │        (binds the inference UDS)             (dials it)
             ├─> humaux-private-worker --distill-serve
             └─> humaux-public-worker --run-once   (scheduled; needs Qdrant + PostgreSQL only)
```

Rules:

1. **PostgreSQL and Qdrant before anything.** Every process's readiness probe fails without them.
2. **A UDS server starts before its client.** The gateway's semantic-recall path dials the
   retrieval worker's socket; the consolidation worker dials the private worker's socket. Start
   the server, wait for its `--readyz` to pass, then start the client.
3. Order within the derived layer is otherwise free: the two derived workers discover work
   cross-tenant through `ops.claim_derived_work` and hold no start-time tenant binding
   (ADR-0036).
4. A supervisor that cannot express ordering can start everything at once and rely on restart:
   the client's first calls fail, the job's lease releases it, and the next pass succeeds. That
   is correct but noisy — prefer readiness-gated ordering.

## 5. The four OS users (non-negotiable for UDS peer-credential validity)

Both UDS hops authenticate the **calling process**, not the request body, through the kernel
peer credential (`UnixStream::peer_cred()`):

| socket | server | accepts only uid | checked in |
|---|---|---|---|
| query-embedding RPC | `humaux-retrieval-worker` | `HUMAUX_RETRIEVAL_WORKER_GATEWAY_UID` | `bins/retrieval-worker/src/rpc.rs` |
| private inference RPC | `humaux-private-worker` | `HUMAUX_PRIVATE_WORKER_CONSOLIDATION_UID` | `bins/private-worker/src/inference_rpc.rs` |

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

| probe | today |
|---|---|
| `cell.resources` | live: DNS + a real intra-Cell HTTP call, cross-checked against the §83.4 registry declaration |
| `deploy.binary` | live: the git sha / build time burned in at compile time, plus the crate version |
| the other 9 | each names the specific object it lacks (a column that is GA-建 and not yet migrated, a read grant this process does not hold, a counter store that does not exist outside the emitting process). ADR-0037 §"What is not wired" lists the unlock condition for each |

`deploy.binary` answers only when the build burned in `HUMAUX_BUILD_GIT_SHA` (and, optionally,
`HUMAUX_BUILD_TIME`). `bins/admin/build.rs` fills it from `git rev-parse HEAD` when the build
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
