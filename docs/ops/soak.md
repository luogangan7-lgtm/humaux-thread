# Soak runbook — `cargo xtask soak`

> Scope: the endurance + crash-recovery harness (card 16, ADR-0038). The process-lifecycle half
> — start order, probes, restart policy, drain windows — is `docs/ops/supervision.md`
> (ADR-0037) and this file assumes it. Spec § numbers are references, never copies.

## 1. What a soak run is

Continuous concurrent load against the **real** four-process deployment, for a stated duration,
while a chaos hook kills and restarts a worker — then a drain, then one verdict per assertion.

The harness **starts nothing**. Bring the deployment up first (supervision runbook §4 start
order, every process gated on its own probe), then point `soak` at it. It needs:

- the gateway's MCP endpoint on loopback, and one bearer per tenant **in an environment
  variable** (the flag names the variable; the value never appears in argv, the log or the
  report);
- `HUMAUX_MAINTENANCE_PG_DSN` — every table it reads is a `role_maintenance` SELECT in §6.2.2,
  and RLS keys off `humaux.tenant_id`, which the harness installs per lane;
- **at least two tenants.** `cargo xtask e2e-seed` provisions one tenant per invocation, so run
  it twice and keep both tenant ids in the report. Every invocation must repeat the SAME
  deployment-shaped lane flags — `--processor-id`, `--region`, `--endpoint-ref`, `--provider-id`,
  `--provider-model-id`, `--model-revision` — and the same `--pepper-hex`. `--processor-id` is the
  egress recipient of the deployment (§7.3), not a per-tenant value: the private worker holds a
  recipient LIST (`HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS`, `<uuid>=<host>[|<host>…][,…]`,
  ADR-0060 D-C / D-L), and a tenant seeded under a recipient that is not in the list parks every
  Distill job `WAITING_KEY` with `last_error_class = EGRESS_PROCESSOR_NOT_ALLOWED` — no provider
  call, no ledger row (runbook §3). Card 16's first soak ran a mismatched tenant for 8 minutes and
  it produced zero memories; the class now names the cause, and `cargo test -p xtask e2e_seed`
  pins the property that two seeded tenants share one egress processor.

## 2. Invocation

```sh
HUMAUX_MAINTENANCE_PG_DSN='postgres://role_maintenance:…@127.0.0.1:54329/humaux_thread_dev' \
cargo run -q -p xtask -- soak \
  --gateway-url http://127.0.0.1:8080/mcp \
  --tenant "$TENANT_A:$WS_A:BEARER_A" \
  --tenant "$TENANT_B:$WS_B:BEARER_B" \
  --sessions-per-tenant 4 \
  --duration-secs 900 --drain-secs 240 --think-ms 250 \
  --probe-every-secs 15 \
  --probe-cmd 'curl -fsS -o /dev/null http://127.0.0.1:8080/livez' \
  --probe-cmd './target/debug/humaux-retrieval-worker --readyz' \
  --probe-cmd "curl -fsS 127.0.0.1:19101/metrics | grep -q '^degrade_total{'" \
  --watch-pidfile gateway=$S/gw.pid \
  --watch-pidfile retrieval-worker=$S/rw.pid \
  --watch-pidfile distill-worker=$S/ds.pid \
  --watch-pidfile consolidation-worker=$S/cw.pid \
  --watch-pidfile projection-runner=$S/rp.pid \
  --chaos-every-secs 180 --chaos-grace-secs 60 \
  --chaos-cmd ./chaos-retrieval-worker.sh \
  --chaos-cmd ./chaos-distill-worker.sh \
  --chaos-cmd ./chaos-consolidation-worker.sh \
  --chaos-cmd ./chaos-projection-runner.sh \
  --lease-secs 120 --max-rss-mib 2048 --max-db-connections 80 \
  --max-op-failure-rate 0.05 \
  --report ./soak-report.json
```

Card 34 / ADR-0061 D-F (ruling E15): the gateway is graded on **`/livez`**, not `/readyz`. Since
card 34 `/readyz` is dependency-truthful, and the chaos hook kill -9's the retrieval worker the
gateway depends on, so a correct `/readyz` is 503 `not_ready` for that window — counting it would
grade the chaos, not the gateway. The old `/readyz` was only the accepting flag, which `/livez`
carries at the same strength. Probes that need a number read the loopback `/metrics` of the
process instead of shelling out (the third `--probe-cmd` above: the gateway's ops listener answers
and exports `degrade_total`; port = `HUMAUX_GATEWAY_METRICS_ADDR`). The xtask soak harness itself is
unchanged.

Card 27 / ADR-0052: the rotation includes the resident projection runner
(`humaux-retrieval-worker --serve`). It replaced the per-tenant `--run-once` loop the rehearsal
used to run beside the soak; there is no projection loop any more, and its kill -9 is what
`no_live_ticket_lease_after_drain` grades. Its chaos script is the retrieval worker's with the
`--serve` env block (the seven pass keys) and `rp.pid` — both processes are the
`humaux-retrieval-worker` binary, so the pidfile, not the name, says which one dies.
`docs/ops/rehearse.sh` lists the runner's hook first, so a short soak (`SOAK_SECS=120`, 90 s chaos
period) fires exactly that hook; the card-27 gate ran it with `SOAK_SESSIONS=1 SOAK_THINK_MS=10000
SOAK_DRAIN=300` (card 24's shape) — with the default load, one resident distiller cannot drain three
tenants' writes inside a 150 s drain and `backlog_drained` grades distill throughput, not the soak.

Card 35 / ADR-0062 D-T: the rotation also includes the resident maintenance daemon
(`humaux-maintenance --serve`, hook `soak_chaos_md.sh`, pidfile `md.pid`, watched as `md`). It runs only
against the rehearsal's **throwaway** database `humaux_thread_c35_rh_<pid>` (created, migrated with `cargo xtask
migrate --dsn`, seeded with eight tenants of purgeable work and one orphan ticket each, dropped on exit; ruling
E8: never the shared dev database), with retentions 0, so its kill -9 lands while the purge doors are working.
Its hook is listed **second**, right after the runner's, so the gate's `SOAK_SECS=1800 SOAK_CHAOS_SECS=400`
(four hooks fire) reaches it, and the seed is sized from `SOAK_SECS` so the purging outlasts that round. After
the soak, step `maintenance_drain` grades it, not the soak harness: per purge door `seeded − remaining =
Σ ops.maintenance_receipts.affected` (`maintenance_soak_receipts_balance`; a multi-statement or receipt-less
purge cut by kill -9 breaks the equality), every orphan LOST with exactly one reissue, and the restarted daemon
finished a cycle and answers 200. `cargo test -p xtask rotation_includes_the_maintenance_daemon` reds when the
hook leaves the rotation.

Each chaos script kills **one PID the launcher recorded at spawn**, and never a pattern or a
port — see §6. `pgrep -f "…"` in a chaos command is the shape this runbook used to publish and
must not: it matches any process on the host whose argv happens to contain that string.

Exit codes: `0` all assertions pass · `1` at least one assertion failed (read the report) ·
`2` bad flags or a missing object (the message names it) · `3` the run could not complete.

### Size the load to the distill hop's capacity, or three assertions are unwinnable

`tickets_settled_after_drain`, `backlog_drained` and `ryw_replay_answered` all assume the
pipeline can consume what the load generator produces. It usually cannot: the Distill hop makes
**one real provider round trip per Evidence, serially**, so its throughput is the provider's, not
the database's. Measured on this deployment (MiniMax `MiniMax-M3`, one resident
`--distill-serve`, 2026-09-10): **0.229 Evidence/s** (n = 140 `private.processing_runs` over
611 s). A run at `--sessions-per-tenant 2 --think-ms 500` across two tenants ingested **0.992
tickets/s** (n = 211 over 213 s) — 4.3x capacity — and left 93 tickets in flight and 296 rows
queued no matter how correct the code was. Those verdicts were measuring MiniMax's TPS.

So: pick `--sessions-per-tenant` and `--think-ms` such that
`tenants x sessions / (think_secs + round_trip_secs)` stays **under** the measured Evidence/s,
and set `--drain-secs` above `peak_backlog / capacity`.

**Sizing a latency measurement (card 30).** `docs/ops/rehearse.sh` runs three tenants, so a run
needs `recalls = 3 x SOAK_SESSIONS x SOAK_SECS / (think_secs + round_trip_secs)` for its `n`,
under the same ceiling `3 x SOAK_SESSIONS / (think_secs + round_trip_secs) < 0.229`. With
`SOAK_SESSIONS=1 SOAK_THINK_MS=14000` the round trip (remember + recall + enumerate + get, ~1 s
in release, ~2.5 s in debug) gives ~0.2 loops/s: `SOAK_SECS=1800` ⇒ ≥ 330 recalls at ~0.2
Evidence/s, `SOAK_DRAIN=300`, and `SOAK_CHAOS_SECS=400` so four of the five chaos hooks fire
about once (the runner's, the maintenance daemon's, the retrieval worker's and the distiller's; card 35) (every retrieval-worker kill fails the recalls in its restart window, and the default
90 s period over a 30-minute run would spend the 1 % `op_failure_rate` budget on chaos alone). Measure latency on `REHEARSE_PROFILE=release` (the rehearsal
builds and runs every binary from `$CARGO_TARGET_DIR/release`); a debug build inflates exactly
the CPU-bound stages (`scan`) the stage table exists to attribute, so a debug number is a
build-profile artifact, not a deployment baseline. Re-measure the capacity line above
whenever the provider or the model changes — it is a property of the deployment, not a constant.
**Card 32 (ADR-0058): a distill kill holds its slot until the hard deadline, so the drain must
cover it.** `--distill-serve` now runs `HUMAUX_PRIVATE_WORKER_DISTILL_IN_FLIGHT` seats (4, the
`ops.provider_slots` count, §67.2) instead of one serial pass, and a `kill -9` that lands while a
request is admitted leaves that job `EXECUTION_UNCERTAIN` with its slot kept until
`HARD_DEADLINE_SECS` (rehearsal: 300 = 2 × (HTTP 120 + lease 30)), then re-queues it with backoff
and one resend. With `SOAK_SECS=1800 SOAK_CHAOS_SECS=400` the last chaos step fires at 1600 s, its
reconcile is due by 1900 s and its resend ends by ~2050 s — so the soak's drain is
`SOAK_DRAIN=420`, not 300, or `backlog_drained` grades the hard deadline instead of the code.
(`--drain-secs > --lease-secs` still holds: the lease is 30.) The capacity line above was measured
with one serial distiller; ADR-0058 §Measurements has the 1 vs 4 in-flight numbers that replace it.

Over-subscription also shows up on the recall side: the embedding provider is on the same
budget, so an over-subscribed run's recalls start coming back `DEPENDENCY_UNAVAILABLE` with a
`query_embedding_*` line in the gateway log. Read the line, not the code — since card 17 the
gateway distinguishes `query_embedding_skipped` (the provider declined),
`query_embedding_unavailable reason=<tag>` (the worker could not be reached or failed) and
`query_embedding_dimension_mismatch` (the vector came back the wrong shape). Only the first two
are ever a capacity story, and none of them is what a `limit` mismatch looks like — that is
`limit_not_profile_top_k`, see below.

### Every flag is required, on purpose

§78.1: the subcommand has **no defaults**. A threshold baked into the binary moves with the
code it grades, so the duration, the drain, the think time, the poll period, the lease, and both
leak ceilings all come from the invocation — and all of them land in the report's `config`
block, so a number in a report can always be traced to the run that produced it.

Two flag rules are enforced at parse time rather than discovered at verdict time:

- **`--drain-secs` must exceed `--lease-secs`.** A lease held by a killed worker is still live
  until it expires; a shorter drain cannot tell a wedged worker from a crashed one (ADR-0038 D5).
- **`--chaos-every-secs` and `--chaos-cmd` come together or not at all.** Half a chaos hook is
  a run that silently tests nothing.
- **`--chaos-grace-secs` is required when, and only when, chaos is configured** (card 25,
  ADR-0050 D-I). It is the window after each chaos step's start in which a watched process may
  be absent. Size it to cover kill plus restart plus the restarted binary's first-exec cost.
- **`--watch-pidfile <name>=<path>` is required, at least once** (card 25). Name every process
  the launcher started, using the pidfiles the launcher and its chaos scripts already keep
  current. The harness still takes no PIDs as arguments and reads nothing from the workers'
  env.
- **`--max-op-failure-rate <0..1>` is required** (card 25). It is the ceiling on any one
  operation's `failed / n`.

`--probe-cmd` and `--chaos-cmd` may each be repeated; probes all run every poll, chaos commands
are used round-robin.

### Never send `limit`

§55.1 reserves candidate depth to the registered retrieval profile: *"callers cannot supply
`top_k`, `cand_k`"*. Since card 30 (ADR-0055 D-D) a `limit` in `1..=top_k` is accepted and only
shortens the returned list; anything above `top_k` is `INVALID_INPUT`, refused before the request
reaches the embedding step. The harness still sends none — it measures the default depth. Card 16's
post-drain replay sent `"limit": <live point count>` to widen its result page; every replay was
refused, `ryw_replay_answered` was red in every run, and because the refusal was **silent** the
failure was read for a day as an embedding fault (the gateway's `query_embedding_unusable` line
two steps further down, which was itself firing for an unrelated chaos-window recall). The
gateway now logs `limit_not_profile_top_k limit=<n> profile_top_k=<n>`, and the harness sends
no `limit` at all.

The same silence hid a second refusal behind it: an expired `consistency_token` is also a bare
`INVALID_INPUT`. The gateway now logs `consistency_token_refused reason=token_expired |
token_malformed | token_not_issued` (the class only, never the token).

## 3. The assertions, and what a red one means

| id | red means | unit |
|---|---|---|
| `watermark_no_stall` | a stream's issued highwater grew while the projection worker's **applied** position — `max(stream_seq)` over its settled `projection.stream_log` rows — never moved: the worker is alive but no longer consuming (§15.3). Deliberately **not** measured on `projection_highwater`; see `projection_promoted` | streams |
| `projection_promoted` | the §15.4 contiguous DONE prefix never advanced, so the §16.2 serving switch had nothing it could legally promote. **REQUIRED since card 20** — the `expected_red_until` marker is gone and a red here fails the run. What made it red before was a settled `FAILED` ticket pinning its stream's prefix for the rest of the run no matter how healthy the worker was; migration `0167` adds the audited `FAILED -> RETIRED_FAILED` retirement (§15.2.1) and the ops path calls it with `xtask projection-serve --retire-failed <error_class>`, so the prefix can move past a retired seq. **The rehearsal harness is that operator for `distill_failed` (ADR-0058 R11):** a DEAD distill settles its ticket `FAILED distill_failed`; `docs/ops/rehearse.sh` retires that class (only that class) for every soak family once before the timed window and every `SOAK_RETIRE_SECS` (60) during it, logging each retirement to `soak-operator.log`, and grades the death itself as `distill_dead`. Chain run 2 (2026-10-03) went red here because a DEAD from the pst step, before the soak, was never retired. Any other FAILED class is a projection-path failure and still pins the prefix. `detail.reject_reasons` is **whatever `projection::serving::evaluate_switch` returned** for each pending candidate the database described, keyed by the rejection variant's own name and counted per candidate — not a fixed list and not a row count wearing a rejection's name (which is what this field held until it was fixed: `count(*) FROM stream_checkpoints` relabelled `VisibleUnavailable`, reported even on streams whose promotion had succeeded). The candidate population and its denominator now live on `promote_candidates_admitted` below. | streams |
| `promote_candidates_admitted` | a **pending promotion candidate** — a `projection_version` that is NOT its family's serving row — would be refused by §16.3. `n` is the number of `projection.stream_checkpoints` rows scanned for candidacy, so `n = 0` is `FAIL-VACUOUS` (ADR-0038: the run never looked), while `detail.candidates_pending` states separately how many of those rows were actually offerable. On a deployment where every family holds exactly one `projection_version` (card 21: derived from `domain::ticket_family::TicketFamily`, a single value for the whole build — the three `HUMAUX_RETRIEVAL_WORKER_{DOMAIN,PROJECTION_KIND,PROJECTION_VERSION}` env values are gone) the honest answer is `candidates_pending: 0` — §16.3 was not exercised — and that is now said out loud instead of arriving as an empty `reject_reasons` map. Card 20 fixed the loop that used to offer each family's serving version back to itself every 5 s: `promote_rejections` filters `WHERE NOT c.serving`, and `rehearse.sh`'s serve loop skips a family that is already serving the version it would offer, so `VisibleSameVersionDeclared` (a count compared with itself — the 恒真闸 shape §16.3 rejects on the version tag alone) is gone from the tally. Both `visible_*` counts are taken live against Qdrant through the read routes' own producer (`adapters::retrieve::visible_count_of_version`, via `xtask::switch_visible`): `visible_shadow` counts the **candidate** row's `projection_version`, `visible_serving` the family's `serving` one, so `VisibleUnavailable` appears only when a count genuinely could not be taken — no §17.3 placement row, an unreachable Qdrant, or the `USER_PRIVATE` blind-spot refusal ADR-0040 D-J describes. `BenchmarkNotPass` no longer appears on a **first** activation: §16.3 criterion ③ compares `benchmark(shadow)` against `benchmark(serving)`, and a family with no serving version has no second operand — the same ADR-0017 exemption criterion ① already had (a proven §69 `FAIL` still refuses, which is what keeps that branch observable). Off the first-activation path the gate still requires `Pass`, so a real second-version promotion is still refused while `continuation_198_v2` is `NOT_DECLARED` (§69), and `xtask projection-serve --continuation` now defaults to `cannot_establish` rather than `pass` so the CLI and this tally can no longer disagree about the same switch. | candidates |
| `watermark_monotonic` | a highwater went **backwards** — a projection rebuilt from a stale checkpoint, or two writers racing one row | regressions |
| `tickets_not_lost` | a gap in the §15.1 dense range `1..=max(stream_seq)` **of one stream**, or a `LOST` row. The census is grouped by the full `(scope_kind, scope_id, domain, projection_kind, projection_version)` key and the gap is computed per stream, then summed: a tenant-wide subtraction masks a lost seq behind a sibling stream's row count (two streams of 10 rows give `total = 20` against `max_seq = 10`, so `(10-20).max(0)` is 0 however many are missing). **A `SKIPPED_BY_POLICY` ticket is settled, not lost** | tickets |
| `tickets_settled_after_drain` | a ticket was still in flight after the drain — the chain did not converge | tickets |
| `tickets_applied_once` | two live `projection.private_memory_points` for one memory in one family: a double-apply (ADR-0038 D4) | memories |
| `no_live_lease_after_drain` | an `ops.jobs` row is `PROCESSING` with a lease still live more than one `--lease-secs` after the last kill: a worker is wedged, not crashed (§31/§61) | ops.jobs rows |
| `no_live_ticket_lease_after_drain` | a `projection.stream_log` ticket of a run tenant still holds a live `--serve` lease (`lease_owner IS NOT NULL AND lease_expires_at > now()`) after the drain: the runner's kill -9 left leases the restarted runner never re-claimed and settled, or a runner is wedged (ADR-0052). The sibling of `no_live_lease_after_drain` for ticket leases | stream_log tickets |
| `backlog_drained` | `ops.jobs` + `ops.outbox` still queued after the drain | queued rows |
| `db_connections_bounded` | connection count exceeded `--max-db-connections` at some poll: a leak, or pool sizing that does not survive this concurrency | connections |
| `rss_bounded` | some `humaux-*` process exceeded `--max-rss-mib`. Only real readings are scored, and `n` counts them. An observation whose `ps` failed contributes no reading, never `0 MiB`, so a run with no reading at all is `FAIL-VACUOUS` | MiB |
| `probes_green` | an ADR-0037 probe exited non-zero during the run, **or** a watched process was absent outside every chaos grace window (card 25, ADR-0050 D-I). Read `docs/ops/supervision.md` §2 for the probe's meaning. `--probe-cmd '<worker> --readyz'` starts a fresh process and stays green while the resident worker is dead, so each observation also runs one `ps -axo pid=,rss=,comm=` and looks up every `--watch-pidfile`'s current pid. A process counts as present only if that pid is listed with a `humaux-` command; a reused pid running something else counts as absent. An absence inside `[chaos_start, chaos_start + --chaos-grace-secs]` is an **expected** absence: it is reported in `detail.expected_absent_in_chaos_window` and the timeline, and is not a failure. Any other absence is **unexpected** and is a failure. `n` = probe runs + process look-ups | failed probe runs + unexpected absences |
| `ps_observed` | `ps` could not run, exited non-zero, or returned an empty table at some observation (card 25). A `ps` failure is an assertion failure. It is never RSS 0 and never "everything present" | observations whose ps failed |
| `op_failure_rate` | some operation's `failed / n` exceeded `--max-op-failure-rate` (card 25). `value` is the worst operation's rate, so a healthy operation cannot dilute a failing one. `detail` lists every op's `{op, n, failed, rate}`, so a red names its op. `n` = all samples, which makes a run with no load `FAIL-VACUOUS`. Failures inside chaos windows **are** counted: excusing them needs chaos-to-op attribution, which card 54 owns | fraction of calls failed (worst op) |
| `ryw_token_honoured` | a `consistency_token` this run minted was refused (§15.5) | rejections |
| `ryw_replay_answered` | a lane's post-drain replay produced no answer — the write did not land, its `stream_seq` could not be resolved, or the recall itself errored. `n` = replays **attempted**, one per lane unconditionally, so a lane that bails out early lands in the denominator instead of shrinking it to zero. Kept separate from the row below on purpose: a replay that never happened tells you nothing about visibility, and reporting a transport failure as a consistency violation is a lie the harness would be telling about the system. Which half failed is one lookup away: `latency[]` for `remember.ryw_replay` (write) and `recall.ryw_replay` (read) | unanswered replays |
| `ryw_settled_write_visible` | a `stream_seq` the replay's own token entitled it to see did not come back. The replay writes once more after the drain and uses **that** token: a `consistency_token` lives `HUMAUX_GATEWAY_REMEMBER_TOKEN_TTL_SECONDS` (60 s here) and `--drain-secs` must exceed `--lease-secs` (120 s), so a token minted during the load is always expired by replay time. The witness is the §15.5 PG delta overlay's seq coverage — every `projection.stream_log` row in `(serving_highwater, token stream_seq]` must arrive as an item carrying that `stream_seq`; the token's seq is resolved from the accept envelope's `evidence_id` through `ops.outbox`. Never a marker in the content (the distill hop rewrites it through the model) and never "every live projection point" (`recall` returns top-k) | stale reads |
| `no_cross_tenant_row` | one lane's MCP response carried another lane's sentinel, tenant id or workspace id (§6.1) | responses |

## 4. Reading the report

`--report` is the artifact; the console prints the same verdicts. Its shape:

- `config` — every threshold the run used, so no number is orphaned from its run;
- `timeline` — one row per poll: `issued_highwater_sum`, `applied_highwater_sum`,
  `projection_highwater_sum`, `backlog_rows`, `db_connections`, `max_process_rss_mib`,
  `probe_failures`. This is the evidence behind `watermark_no_stall` (the applied sum) and
  `projection_promoted` (the projection sum): a reader can watch the two diverge rather than
  taking the split on trust;
- `assertions` — `{id, value, unit, n, threshold_at_most, verdict, expected_red_until, detail}`.
  §78.2: no bare numbers. `verdict` is `PASS` / `FAIL` / `FAIL-VACUOUS` / `EXPECTED-RED`; an
  `EXPECTED-RED` row names the card that owns the gap, is listed again under the report's
  `expected_red`, and does **not** fail the run or change its exit code;
- **`n = 0` is never a pass.** Every assertion here counts a bad thing that did not happen, so a
  run in which the thing was never attempted reports `0 <= 0` and is indistinguishable from a
  run that attempted it a thousand times and survived. Card 16's own final run shipped
  `ryw_settled_write_visible PASS value 0.0 n 0` that way. A zero denominator is now
  `FAIL-VACUOUS` and fails the run — the harness says "I did not measure this" instead of "this
  is fine". Spelled differently from `FAIL` on purpose: "0 stale reads out of 0" and "1 stale
  read out of 28" are different defects, and a reader should not have to check `n` by eye;
- `latency` — per MCP operation `{p50, p95, n, unit: "ms", failed_calls}`, nearest-rank
  percentiles over successful calls only. An operation whose calls **all** failed is still
  listed, with `n: 0` and its `failed_calls` — a total failure must not be the one shape that
  disappears from the report. Buckets (card 30, ADR-0055 D-E): `remember`, `recall`,
  `memory.enumerate` (reported as plain `memory` before card 30 — delivery_point_report §4.1
  misread it as `memory.get`), `memory.get` (new: the session's first recalled `memory_id`;
  **skipped, not failed**, when its recall returned no memory item), the post-drain
  `remember.ryw_replay` / `recall.ryw_replay`, and two **derived** families read from each
  successful recall's `provenance.stage_ms`: `recall.stage.<route|planner|scan|embed|qdrant|
  hydrate|rerank|assemble>` and `recall.stage_sum` (the eight summed). A failed recall adds no
  stage sample (a missing stage is not a zero), and `op_failure_rate` ignores both derived
  families — they are not calls. `recall.stage_sum` p50 against `recall` p50 is how much of the
  wire latency `search()` itself accounts for; `docs/ops/rehearse.sh` asserts ≥ 90 %
  (`recall_stage_sum_covers_90pct_of_recall_p50`);
- `tenants` — per lane: ticket census, plus two rates that are metrics rather than assertions.

### The two rates that are metrics, not failures

`skipped_by_policy_rate` and `recall_dependency_unavailable_rate` carry the known live-model
non-determinism (card 24 debt D1): MiniMax sometimes classifies ordinary content above the
origin's authority ceiling, the candidate is rejected `origin_authority_ceiling`, the ticket
settles `SKIPPED_BY_POLICY`, projection-serve reports `rejected:[OpenGaps]`, and that item's
recall answers `DEPENDENCY_UNAVAILABLE`. **That chain is settled work, not lost work.** Counting
it as lost would make every honest run red for a reason this harness does not own; hiding it
would make a real regression invisible. So it is counted, per tenant, with `n`, and the
exactly-once assertions stay strict. A rate that climbs run over run is a model or prompt
regression — take it to the distill hop, not to this harness.

## 5. Before the timed window: warm every binary

macOS assesses each freshly linked executable on first exec (XProtect / `syspolicyd`), **~98 s
each, strictly serial**, regardless of how many you launch in parallel. A binary first executed
inside the timed window turns that assessment into apparent latency and can blow a start
deadline that is correctly sized in production.

So: build, then warm **every binary the run will launch or probe**, before starting the clock —

```sh
cargo build -p humaux-gateway -p humaux-retrieval-worker \
            -p humaux-private-worker -p humaux-consolidation-worker -p xtask
for b in humaux-gateway humaux-retrieval-worker humaux-private-worker humaux-consolidation-worker; do
  env -i ./target/debug/$b --help >/dev/null 2>&1
done
```

and say in the report that you did. **Never widen a production timeout to absorb this** — the
timeout would then be wrong on every host that does not have XProtect.

## 6. Chaos: what to kill, and what must survive it

The chaos hook is a shell command precisely so the process owner keeps process ownership
(ADR-0038 D1). Ownership is the whole point, so it has one absolute rule:

> **Kill only a PID your launcher recorded at spawn, and only after `ps -o comm= -p $PID`
> confirms the binary.** Never `pkill -f` / `pgrep -f` (matches any process whose argv contains
> the string) and never `lsof -ti :PORT` / `fuser -k` (matches whoever holds a port — on
> 2026-09-09 that killed a user's chat client, which happened to hold 8080). If a port you need
> is busy, take another one and report the conflict; never free it by force.

Which means a chaos hook always has three parts: read the pidfile, confirm the binary, kill —
then restart and **write the new PID back into the pidfile**, or the process it just created is
unreachable by anything except another pattern-kill:

```sh
#!/bin/sh
# chaos-retrieval-worker.sh — one worker, one pidfile, no patterns.
p=$(cat "$PIDFILE") || exit 1
case "$p" in ''|*[!0-9]*) echo "bad pid"; exit 1;; esac
[ "$(ps -o comm= -p "$p" | sed 's#.*/##')" = humaux-retrieval-worker ] || exit 1
kill -9 "$p"
while kill -0 "$p" 2>/dev/null; do sleep 1; done
( <the same env the launcher used>; exec ./target/debug/humaux-retrieval-worker --serve-rpc \
    >> worker.log 2>&1 ) &
echo $! > "$PIDFILE"
```

Use the *same* env block for the restart as for the original spawn — a chaos hook that brings
back a differently-configured worker grades a deployment nobody ran.

What matters beyond that is not which worker you pick but that the run still finishes with
**exactly-once** results:

- `kill -9` is the interesting signal. `SIGTERM` drains (supervision runbook §3): both resident
  derived workers observe it only *between* passes, so a graceful stop can never strand a lease.
  `kill -9` mid-pass **can**, and the lease is what makes that survivable — the row stays
  `PROCESSING` until the lease expires, then another worker reclaims it.
- Therefore the assertion that grades a kill is `no_live_lease_after_drain`, measured more than
  one `--lease-secs` after the last kill. A `PROCESSING` row with a live lease at that point is
  a worker renewing a lease it can no longer make progress on.
- Kill **each** worker in turn across a run (round-robin `--chaos-cmd`s), not the same one
  repeatedly: the restart path differs per process (supervision runbook §3 and §4's start order
  — a UDS server must come back before its client can succeed). Card 16's runs shipped with
  `"chaos_cmds": 1` for a while — only the retrieval worker — so `no_live_lease_after_drain` had
  never been exercised against either process that actually holds an `ops.jobs` lease. The
  rotation is now the retrieval worker, the `--distill-serve` private worker
  (`DERIVED_DISTILL` lease) and the `--serve` consolidation worker (`DERIVED_CONSOLIDATION`
  lease); check `config.chaos_cmds` in the report before believing a run exercised restarts.
- Two processes are deliberately **out** of the rotation, and the reason belongs in the report,
  not in silence: the **gateway**, whose `GET /livez` is the only probe bound to a live process
  (the worker probes are one-shot binaries checking PG/Qdrant), so killing it makes
  `probes_green` grade the harness's own outage window; and the private worker's
  **`--serve-rpc`** listener, which holds no `ops.jobs` lease and is the UDS server the
  consolidation worker dials, so a restart races §11.8's bind rather than the lease path this
  step exists to exercise. Both are supervision-runbook (ADR-0037) restart cases, not lease
  cases.

## 7. Negative controls — proving the harness can go red

A harness nobody has seen fail is a harness nobody should trust. Three controls are hermetic
and run in the ordinary gate chain (`cargo test -p xtask`, ADR-0038 "Negative controls"):
stalled projection worker → `watermark_no_stall`; double-apply → `tickets_applied_once`;
leaked connection → `db_connections_bounded`. Each also asserts its neighbour stays green, so a
control cannot pass by turning the whole report red. A fourth,
`a_pinned_promotion_prefix_is_not_reported_as_a_stalled_worker`, pins the other direction: a
worker consuming every ticket behind a pinned §15.4 prefix must leave `watermark_no_stall`
green and show up only as `projection_promoted` — which, since card 20 dropped that assertion's
`expected_red_until`, now also fails the run.

Four more hermetic tests pin the checks that a review found could not observe their own failure
— every one of them was green against a defect before it was fixed:

| test (`cargo test -p xtask soak`) | pins |
|---|---|
| `a_lost_seq_in_one_stream_is_not_masked_by_a_sibling_streams_rows` | the §15.1 gap is per stream; the tenant-wide subtraction it replaced scored the same census as clean |
| `an_assertion_with_no_witness_is_vacuous_not_a_pass` | `n = 0` is `FAIL-VACUOUS`, not `PASS`, and it fails the run |
| `promote_reject_reasons_are_taken_from_evaluate_switch` | `detail.reject_reasons` comes from the real §16.3 evaluator, not from a row count |
| `a_live_visible_count_removes_visible_unavailable_from_the_promote_tally` | a §23.1② count that **was** taken stops being reported as an unavailable one, and one that was not still refuses (ADR-0040 D-H..D-K) |
| `promote_candidate_set_is_graded_over_the_rows_it_scanned` | `promote_candidates_admitted` PASSes with `candidates_pending: 0` stated explicitly, FAILs on a real refused candidate, and is `FAIL-VACUOUS` when no checkpoint row was scanned at all |
| `a_gap_in_the_dense_ledger_is_a_lost_ticket_but_skipped_by_policy_is_not` | the census itself, through `fold_census` rather than hand-set fields |

Three more are available live at zero injection cost, because every threshold is a flag:

```sh
… --max-db-connections 1   # db_connections_bounded goes red against a healthy system
… --max-rss-mib 1          # rss_bounded goes red
# start everything EXCEPT the retrieval worker → watermark_no_stall goes red under real traffic
```

Run one of these whenever the harness itself has been edited. A green soak that cannot be made
red is not evidence.

## Failed calls

`latency[]` carries `failed_calls` per operation — a call that returned non-200, `isError:true`
or a transport error. No verdict is keyed to it, so it is easy to read past; since card 24
(2026-09-26) every failed call also prints one stderr line the moment it happens:

```
soak: remember failed on lane soak-sentinel-<tenant> after 42ms: RATE_LIMITED
```

The head of the reply (240 chars) is what makes a failed call investigable. That line is how
rehearsal4 found that the request guard answered `RATE_LIMITED` on advisory-lock contention
between the two lanes' shared pre-auth `ip` bucket (fixed in `quota_repo::consume_rate` — the
bucket now waits for its holder). A soak whose `failed_calls` are not zero owes an explanation
per line in its evidence, not a threshold.
