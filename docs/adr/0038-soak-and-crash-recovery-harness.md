# ADR-0038 — The soak and crash-recovery harness (`cargo xtask soak`)

- Status: accepted
- Date: 2026-09-09
- Card: 16
- Spec: §15.1 (per-stream dense ledger = the sole "expected set"), §15.2 (settled vs in-flight
  states), §15.3 (four highwaters), §15.5 (read-your-writes / `consistency_token`), §31/§61
  (durable jobs, SKIP LOCKED claim, leases), §6.1 (tenant isolation), §6.2.2 (`role_maintenance`
  SELECT grants — this card adds none), §33.10 (confirm gate on destructive verbs only),
  §78.1 (no hardcoded thresholds), §78.2 (no bare numbers), §83.4 (raw HTTP client rule)
- Supersedes nothing. Composes with ADR-0016 (distill leases), ADR-0036 (tenant-free derived
  dispatch), ADR-0037 (liveness/readiness probes and the supervision contract).

## Context

Every correctness claim on this tree up to card 15 rests on **single-shot** tests: one write,
one read, one assertion, one process. A repo-wide grep for `soak`/`endurance` returned zero
hits before this card, and no test anywhere exercised two agents writing concurrently through
MCP. 稳定运行 and 并发 are both named acceptance terms, and neither had a first-class harness.

The failure modes that turn a working demo into an unusable deployment are exactly the ones a
single-shot test structurally cannot see:

- a **slow leak** — RSS or PostgreSQL connections climbing over minutes;
- a **stalled watermark** — the projection worker alive (liveness green) but no longer
  advancing `projection.stream_checkpoints`;
- a **lease that never expires** — a `kill -9` mid-pass leaves an `ops.jobs` row `PROCESSING`,
  and if the lease is renewed by a wedged owner instead of expiring, that job is gone forever;
- a **ticket lost across a restart** — a gap in the §15.1 dense sequence;
- a **ticket applied twice** — a second live projection point for one memory.

## Decision

### D1 — One xtask subcommand, driving the *real* four-process deployment

`cargo xtask soak` is a load driver and a judge; it is **not** a process supervisor and it
starts nothing. It dials the running gateway over loopback MCP and reads the live database as
`role_maintenance`. The processes are started by whoever owns their environment
(`scratchpad/rehearse.sh`, or a systemd/K8s stack per `docs/ops/supervision.md`).

Consequence: the harness never needs the workers' PIDs and never touches their environment,
which is where the BYOK key material lives. Chaos and probing are shell hooks
(`--chaos-cmd`, `--probe-cmd`), so the process owner keeps process ownership.

**Why not a supervisor too.** A harness that also started the five processes would have to
carry a second copy of card 14's env contract, and that copy would drift from the one the
rehearsal and the runbook use. One env contract, one owner.

**The obligation that comes with the hook.** Handing the process owner a `--chaos-cmd` moves
the kill out of Rust and into a shell script, and a shell script is where "kill the worker"
degrades into `pkill -9 -f 'humaux-retrieval-worker --serve-rpc'` — a match against the argv of
every process on the host, selecting by coincidence rather than by ownership. That is the same
class of action as the `lsof -ti :PORT | xargs kill` that killed a user's chat client on
2026-09-09 (it happened to hold 8080), and this card's own chaos hook shipped it. So the rule
is stated here rather than left to the caller's judgement, and `docs/ops/soak.md` §6 publishes
the three-part shape every chaos hook must have:

1. the launcher writes the PID it got from `$!` into a pidfile **at spawn**;
2. the hook reads that pidfile and refuses unless `ps -o comm= -p $PID` names the expected
   binary — never `pkill`/`pgrep`/`lsof`/`fuser`;
3. the restart writes the **new** PID back into the pidfile, or the process the hook just
   created is unreachable by anything except another pattern-kill (the old hook backgrounded
   its restart without capturing `$!`, so nothing — including the rehearsal's own teardown —
   could ever reap it).

Same env block for the restart as for the original spawn: a hook that brings back a
differently-configured worker grades a deployment nobody ran.

### D2 — The probes are *called*, never re-implemented

`--probe-cmd` runs the real ADR-0037 probes (`curl -fsS .../readyz`, `<worker> --readyz`) and
counts a non-zero exit as a failure. The harness does not open its own database connection to
"check readiness" — a second readiness implementation would be green on days the real one is
red, which is the worst shape a control can take (§4.4 坑5).

### D3 — A ticket is lost only when it is neither settled nor in flight

The §15.1 expected set is the dense range `1..=max(stream_seq)` **of one stream**, so the census
is grouped by the full `(scope_kind, scope_id, domain, projection_kind, projection_version)`
key and the gap is computed per stream, then summed. Aggregating per *tenant* and subtracting
once is unsound and silently so: two streams of 10 rows give `total = 20` against
`max_seq = 10`, and `(10 - 20).max(0)` is `0` however many seqs are missing from either. It
happened to be sound in this card's runs only because each tenant had exactly one stream; a
second workspace, or card 18's `projection_version` bump, makes it wrong. `fold_census` is a
pure function precisely so that case is a hermetic test
(`a_lost_seq_in_one_stream_is_not_masked_by_a_sibling_streams_rows`) rather than a two-workspace
live run nobody will stage. Therefore:

| observation | verdict |
|---|---|
| `DONE` / `FAILED` / `TOMBSTONED` / `SKIPPED_BY_POLICY` | **settled** |
| `ISSUED` / `PROCESSING` / `WAITING_KEY` / `RETRY_WAIT` | in flight (must be 0 after the drain) |
| `LOST`, or a seq missing from the dense range | **lost** — always red |

`SKIPPED_BY_POLICY` is settled, **not** lost. The live-model non-determinism carried as debt D1
on card 24 (MiniMax occasionally classifies ordinary content above the origin's authority
ceiling → candidate rejected `origin_authority_ceiling` → ticket `SKIPPED_BY_POLICY` →
projection-serve `rejected:[OpenGaps]` → `DEPENDENCY_UNAVAILABLE` for that item) would, if
absorbed into "lost", make every run red for a reason this card does not own. So the report
carries `skipped_by_policy_rate` and `recall_dependency_unavailable_rate` **per tenant, with n**,
as first-class metrics — and the exactly-once assertions stay strict.

### D4 — Exactly-once is asserted where a double-apply is actually observable

`projection.private_memory_points` already has a UNIQUE constraint on the exact identity
(migration 0119), so a byte-identical replay is refused by the database and proves nothing. The
observable double-apply is a **second live point for the same `memory_id` in the same
(scope, domain, projection kind, version, embedding version)** — that is what the assertion
counts, and what negative control (b) injects.

### D5 — A live lease surviving the drain is the crash-recovery assertion

`--drain-secs` is **required to exceed `--lease-secs`** (refused at parse time, with the
reason). After a drain longer than one lease, every lease a killed worker held has expired, so
`ops.jobs` rows still `PROCESSING` with `lease_expires_at > now()` mean a worker is *wedged*,
not crashed. This is the SQL form of the supervision contract's claim that a `SIGKILL` is the
case the leases exist to survive.

### D6 — Read-your-writes is asserted twice, and neither form guesses

1. **During load**: a `consistency_token` this run minted is never refused for a token reason
   (§15.5 `TokenMalformed` / `TokenExpired` / `TokenNotIssued` / cross-tenant / cross-workspace).
2. **After the drain**: each lane writes once more, and is replayed with *that write's own,
   still-valid token*. The extra write is deliberate. A `consistency_token` has a §15.5 lifetime
   (`HUMAUX_GATEWAY_REMEMBER_TOKEN_TTL_SECONDS`, 60 s on this deployment) and D5 requires
   `--drain-secs > --lease-secs` (120 s), so a token minted during the load is **always** expired
   by replay time: the gateway refuses it `TokenExpired → INVALID_INPUT`, which is correct
   behaviour and an assertion no correct system could ever pass. The replay runs *after* the
   ticket census so its own write is never counted as an unsettled ticket. The witness is the
   **§15.5 PG delta overlay's own seq coverage**. With a token in hand the
   read is served by the projection up to the `serving` row's `projection_highwater` and by the
   overlay above it — one item per `projection.stream_log` row in
   `(serving_highwater, token stream_seq]`, each carrying its `stream_seq`. So the expected set
   is that range, read from the database, and a seq in it the response does not carry is a
   stale read. `n` is the size of the range, so a lane with an empty overlay range reports
   `n = 0` — and `n = 0` is not a pass; see D10.

   The token's own `stream_seq` is resolved from the accept envelope's `evidence_id` through
   `ops.outbox` (`evidence_id → stream_seq`, readable by `role_maintenance`) — the harness never
   assumes the token points at `max(stream_seq)`.

   **Three witnesses were tried and rejected before this one**, all because they were silently
   vacuous or silently false — the failure mode this card exists to prevent:

   - a **nonce planted in the write's content**: the distill hop rewrites content through the
     model, so nothing planted in a body survives into the memory it becomes;
   - the **`private.memory_evidence` → `private.memory_records` join** on the write's returned
     `evidence_id`: correct in principle, but §6.1.3's RESTRICTIVE visibility policy needs an
     identity context `role_maintenance` does not carry, so both tables return **zero rows for
     every tenant** under this harness's role. Measured, not assumed:
     `SET humaux.tenant_id=<A>; SELECT count(*) FROM private.memory_records` returns `0` as
     `role_maintenance` while the same query as the superuser returns `30`;
   - **replaying the load's freshest token after the drain** — always expired, as above. Both
     this and the `limit` mismatch below were refused as `INVALID_INPUT` with **nothing in the
     gateway log**, which is why this card also added the operator lines
     `consistency_token_refused reason=<class>` and
     `limit_not_profile_top_k limit=<n> profile_top_k=<n>`;
   - **"every live `projection.private_memory_points` row must come back"**, widened by sending
     `"limit": min(live points, 100)`. This is the one this card removed, and it failed twice
     over. §55.1 reserves candidate depth to the registered profile — *"callers cannot supply
     `limit`, `top_k`, `cand_k`"* — so the only value the gateway accepts is the profile's own
     `top_k` echoed back, and every soak replay was answered `INVALID_INPUT` before the request
     ever reached the embedding step. The assertion `ryw_replay_answered` was red in every run
     and `ryw_settled_write_visible` passed vacuously at `n = 0` behind it. Even had the field
     been accepted, the claim was false: `recall` returns top-k, and demanding a whole live set
     out of a top-k page manufactures stale reads rather than finding them.

   A replay whose recall does not answer at all is counted by a **separate** assertion,
   `ryw_replay_answered`, whose `n` is the number of replays **attempted** — incremented once
   per lane at the top of the loop, unconditionally. It used to be incremented just before the
   recall, three `continue`s later, so a lane whose post-drain write failed or whose seq lookup
   came back empty left the counter untouched: with every lane bailing out, `n` reached `0` and
   both RYW assertions reported `0 <= 0` as a pass. A lane that never got as far as the read is
   now an unanswered replay, and `latency[]` says which half failed —
   `remember.ryw_replay`'s `failed_calls` for the write, `recall.ryw_replay`'s for the read. The
   in-load half's detector was also fixed in
this card: it required the response to echo `consistency_token`, which recall never does, so it
could not observe its own failure (§80.1) and stayed green through every expired-token refusal.
The detector is now a bare `INVALID_INPUT` on a token-carrying recall, which is sound because
that request shape has exactly one refusable caller-supplied field left. Folding a transport
failure into
   `ryw_settled_write_visible` would report a dead socket as a consistency violation — the
   harness lying about the system, which is worse than the harness being silent.

### D7 — Every threshold is a flag, every number carries `n` and a unit

§78.1: there is no default anywhere in the subcommand — not for the duration, the ceilings, the
lease, the endpoint, or the think time. A soak whose thresholds were baked into the binary
would move with the code it grades. §78.2: every assertion in the JSON report is
`{id, value, unit, n, threshold_at_most, verdict, expected_red_until, detail}`, and every rate is
`{value, unit, n}`. `verdict` is `PASS` / `FAIL` / `FAIL-VACUOUS` (D10) / `EXPECTED-RED` (D9).

### D8 — Loopback HTTP is the standard library's, not a new dependency

§83.4 restricts `reqwest`/`hyper` client construction to `humaux-infra-network`, and the
manifest gate would trip on an `xtask` that declared either. The driver is ~50 lines of
HTTP/1.1 over `std::net::TcpStream` against 127.0.0.1, with the response parser unit-tested for
both `content-length` and `chunked` framing. Bearers stay in process memory: `--tenant` names
the *environment variable*, never the value, `Config` deliberately derives no `Debug`, and the
request template holds a `{BEARER}` placeholder that is substituted at send time.

### D9 — Consumption and promotion are two watermarks, not one

`projection.stream_checkpoints` carries `projection_highwater`, and this card's first
implementation graded the projection worker with it. It is the wrong number for that question. §15.4 defines it as the
**contiguous DONE prefix**, so a single `FAILED` ticket pins it for the rest of the run while
the worker keeps consuming normally. Card 16's own two-tenant run: the worker settled every one
of 22 / 20 tickets, and `projection_highwater` sat at 3 / 4 behind `FAILED` rows at seq 4 and 5.
`watermark_no_stall = 1 stream` was therefore not measuring the projection worker at all.

So the harness now reads three positions per stream and grades two of them separately:

| position | source | assertion |
|---|---|---|
| `issued` | `stream_checkpoints.issued_highwater` | (context for both) |
| `applied` | `max(stream_seq)` over the stream's **settled** `projection.stream_log` rows | `watermark_no_stall` — strict |
| `projected` | `stream_checkpoints.projection_highwater` (§15.4 prefix) | `projection_promoted` — reported, `expected_red_until: card 18` |

`applied` uses `settled_at IS NOT NULL`, which is the §15.1 CHECK's own definition of the four
terminal states, so the harness cannot drift from that closed set. Terminal is terminal: a
`FAILED` or `SKIPPED_BY_POLICY` ticket is a *consumed* ticket, so `applied` advances on every
ticket the worker finishes and is immune to the gap a declined one leaves behind.

`projection_promoted` carries its reject reasons as counts per candidate, and they are
**whatever `projection::serving::evaluate_switch` returned** — the §16.3 "无裁量口", called
here rather than paraphrased. `promote_rejections` reads one `PromoteCandidate` per
`projection.stream_checkpoints` row (`first_activation` = the family has no `serving` row;
`open_gaps` = `count(*)` from `projection.processing_gaps` on that candidate's own full stream
key, the same filter `adapters::serving_repo` uses), builds the `SwitchCriteria`, and counts the
variants the evaluator hands back, keyed by variant name.

**This is a correction of the first implementation, and the reason it matters is that the first
one was a fabricated measurement.** It returned `count(*) FROM projection.stream_checkpoints`
under the label `VisibleUnavailable`, unconditionally, and never called `evaluate_switch` at
all. Three consequences, all observed: the run reported `VisibleUnavailable: 2` beside its own
`projection_promoted PASS value 0.0 n 2` — a refusal claimed on two streams whose prefix had
demonstrably advanced; the four other `SwitchRejection` variants could never appear however the
data looked; and the field would have kept printing the stream count after card 18 landed,
under a name that then meant nothing. A number the code did not measure is worse than a number
the code does not have.

What the evaluator returns for these inputs, on this deployment:

- **`VisibleUnavailable`** — §23.1② wants the live Qdrant `visible` count on the shadow **and**
  the serving side at one instant. Nothing here supplies the serving side, so both are passed as
  `None`, which is §23.1's own rule for an uncountable index ("索引 count 取不到时输出
  `visible: null`", never backfilled from another number). **Card 18 owns it**, and this is why
  the assertion is marked `expected_red_until: card 18` rather than deleted or weakened: a known
  gap that is silently dropped is a gap nobody re-checks.
- **`BenchmarkNotPass`** — §69's `baseline_min` / `frozen_by` are `NOT_DECLARED`, so the honest
  `ContinuationVerdict` is `CannotEstablish`, and §69 forbids reading "not established" as a
  pass. This reason was invisible under the old fabrication and is not card 18's: it clears when
  the §69 benchmark declaration lands.
- **`OpenGaps`** — the candidate has `projection.processing_gaps` rows (`FAILED` / `LOST`), so
  the prefix genuinely cannot move past them. A real, permanent property of the run, not a
  missing feature. Counted once per candidate, not once per gap row.

The map is not a fixed list: each reason disappears from the report on its own when the thing it
names is wired, because nothing here decides what the reasons are.

### D10 — An assertion with a zero denominator fails

Every assertion in this harness counts a bad thing that did not happen, so `value <= threshold`
alone cannot distinguish "attempted a thousand times, survived" from "never attempted". Both
print `0 <= 0`. That is not a hypothetical: this card's own final run shipped
`ryw_settled_write_visible PASS value 0.0 n 0`, and the two vacuous read-your-writes witnesses
in "What the live runs found" below are the same failure twice.

So `at_most` requires `n > 0` as well, for **every** assertion — not a per-assertion opt-in,
because the next vacuous denominator will be on whichever one nobody thought to mark. The
verdict is spelled `FAIL-VACUOUS`, distinct from `FAIL`, and it fails the run: "0 stale reads
out of 0" and "1 stale read out of 28" are different defects, and a reader must not have to
compare `n` by eye to tell them apart. The report's `verdict` vocabulary is therefore
`PASS` / `FAIL` / `FAIL-VACUOUS` / `EXPECTED-RED`.

An `EXPECTED-RED` assertion prints, lands in the report with its `expected_red_until` and its
`detail.reject_reasons`, and appears in the report's `expected_red` list — but does not fail the
run or change its exit code. **When card 18 lands, delete the `.expected_red_until("card 18")`
call in `promotion_assertion` and the assertion goes strict; nothing else about it changes.**

## Ceilings (deliberate, with upgrade paths)

- **`ponytail:` the load drives no destructive governance verb.** Sessions run
  `remember.put` → `recall.search` → `memory.enumerate`. §33.10's confirm gate is card 8's, and
  a soak that pinned and unpinned under load would be grading that gate rather than this card's
  endurance claim. Upgrade path: add a `--governance destructive` lane that does
  pin → confirm → unpin once the confirm-token TTL under concurrency is itself a question.
- **`ponytail:` RSS is read with `ps`, per binary name, not per PID.** It is the ceiling that
  keeps the harness out of the process-ownership business (D1). Upgrade path: `--rss-pid`
  flags if a deployment ever runs two builds of one binary side by side.
- **`ponytail:` the monitor polls; it does not subscribe.** Nothing between two polls is
  observed, so `--probe-every-secs` bounds the resolution of every timeline number. Upgrade
  path: shrink the period; a stall shorter than one period is by construction invisible.

## Negative controls

The acceptance gate names three. All three are injected at the **evaluator boundary**, in
`xtask/src/soak.rs`'s hermetic tests — that is where an injection is repeatable and costs no
provider call:

| control | test | asserts red |
|---|---|---|
| (a) stalled projection worker | `negative_control_a_stalled_projection_worker_fails_watermark_advance` | `watermark_no_stall` (on `applied`) |
| (a') pinned promotion prefix is **not** a stalled worker | `a_pinned_promotion_prefix_is_not_reported_as_a_stalled_worker` | `projection_promoted` only; `watermark_no_stall` stays green |
| (b) double-apply in the projection path | `negative_control_b_double_apply_fails_exactly_once` | `tickets_applied_once` |
| (c) leaked connection | `negative_control_c_leaked_connections_fails_bounded_connections` | `db_connections_bounded` |

Each also asserts the *neighbouring* verdict stays green, so a control cannot pass by turning
everything red: (a) leaves `watermark_monotonic` green (a stall is not a regression), (b) leaves
`tickets_not_lost` green (a double-apply is not a lost ticket), (c) leaves `rss_bounded` green.

Two further controls are available live at zero injection cost, because every threshold is a
flag (D7): running with `--max-db-connections 1` or `--max-rss-mib 1` turns the corresponding
assertion red against a healthy system, and running with the projection worker not started
turns `watermark_no_stall` red against real traffic.

Both halves of D9 were injected at the source and observed red before the split was accepted
(§80.1 — a gate with no red-to-green record does not exist):

| injection in `stalled_streams` | red test |
|---|---|
| `pick = w.projected` (card 16's measurement) | `a_pinned_promotion_prefix_is_not_reported_as_a_stalled_worker` |
| `pick = w.issued` (a detector that can never fire) | `negative_control_a_stalled_projection_worker_fails_watermark_advance` |

The product half is injected the same way, live: the same recall carrying the same token, sent
once with `"limit": <n>` and once without, is `INVALID_INPUT` and then answered.

## What the live runs found

Five runs against the real four-process deployment (real MiniMax + real DashScope). Three
defects in the deployment and two in the harness itself — which is the whole argument for the
card, and for running a harness against reality before trusting its green:

1. **Two resident modes refused to start** for want of a config key the one-shot modes do not
   need (`HUMAUX_PRIVATE_WORKER_DISTILL_POLL_INTERVAL_SECS`,
   `HUMAUX_CONSOLIDATION_WORKER_POLL_INTERVAL_SECS`). The rehearsal drove `--distill-once` and
   `--run-once` only, so nothing had ever exercised `--distill-serve` / `--serve`. Fixed in the
   rehearsal's env contract; the soak's `watermark_no_stall` is what surfaced it.
2. **A SIGTERM witness can be vacuous.** With (1) in place the drain worker exited in 1 s having
   claimed nothing, so "no stranded lease" was true for the wrong reason. The rehearsal now
   fails the witness loudly when the worker died on a missing key. After the fix the same step
   took 44 s (one real pass) and still stranded zero leases — a witness that means something.
3. **The SECOND tenant's distill hop never completed** — the finding that justifies the card,
   and the one that forced this card outside its own file list (see "Changes outside card 16's
   file list" below). With two tenants live in one deployment, the first tenant's evidence
   distilled normally and the second tenant's never did: its `DERIVED_DISTILL` job was claimed on
   every pass and came back `not_ready` — `claimed=1 completed=0 not_ready=1
   evidence_claimed=10 memories=0`, eighteen times in one run — because the pass deferred every
   claimed row (`DistillError::Reasoning`, the silent `settle_deferred` arm in
   `bins/private-worker/src/distill.rs`). The job was released, retried, and deferred again
   forever.

   Reproduced on a **freshly migrated database holding exactly three tenants**, which ruled out
   the first hypothesis (a poisoned shared backlog on the dev database, which does hold 3124
   accumulated throwaway tenants): tenant A settled 13 `DONE` + 1 `FAILED`, tenant B's 10
   tickets stayed `ISSUED` for the whole run and drain. Reported as
   `tickets_settled_after_drain = 10 (n = 24)`, `backlog_drained = 20` and
   `watermark_no_stall = 2` while `tickets_not_lost = 0`, `tickets_applied_once = 0` and
   `no_live_lease_after_drain = 0` — nothing was lost or double-applied, the pipeline simply
   never served the second tenant. Exactly the "works in the demo, unusable in deployment" shape
   the card exists to catch.

   **Root cause: this harness's own seeding, not the ADR-0016 dispatch path.** `--processor-id`
   is the *deployment's* egress processor (§7.3), not a per-tenant value: one private worker
   holds one `HUMAUX_PRIVATE_WORKER_EGRESS_PROCESSOR_ID`, and `provider_matches_admission`
   compares the two. The rehearsal minted a fresh UUID for its second `e2e-seed` invocation, so
   tenant B was admitted by no running worker and every one of its rows deferred, forever and
   without a word. Both invocations now pass the same `$EGRESS_PROC`
   (`scratchpad/rehearse.sh`), and `xtask/src/e2e_seed.rs` pins the property with
   `seeding_two_tenants_yields_two_independently_admitted_distill_routes`.

   **What made it cost a day is the silence, and that is fixed too.** `PrivateReasoningError`'s
   `Display` is redacted by construction, so the deferral arm printed nothing at all: an
   admission mismatch and a provider outage were indistinguishable from the log.
   `bins/private-worker/src/distill.rs` now carries the static error *class* onto every deferral
   line (`… deferred: configured provider does not match admitted route`) while the payload
   still never appears, and
   `a_tenant_the_deployment_cannot_admit_is_named_and_does_not_starve_the_ready_one` pins both
   halves: the unadmittable tenant is named, and the admittable one is still served in the same
   pass.

   The final run is green on both tenants — `soak-report-soak23.json`: `verdict PASS`, tenant A
   25/25 and tenant B 22/22 tickets settled, `in_flight: 0` on both, `tickets_not_lost = 0`.

Two defects were in **this harness**, both found by running it rather than by reading it, and
both fixed here (each with a hermetic test):

4. **A vacuous read-your-writes witness** — twice. See D6: the content nonce, then the
   `private.memory_evidence` join. Both reported `n = 0` while looking green. The lesson is the
   one §4.4 坑5 states for probes and this card restates for assertions: an assertion whose
   denominator can silently reach zero is not an assertion.
5. **A totally-failing operation disappeared from the report.** `report_json` only listed
   operations with at least one *successful* call, so a replay that failed on every attempt had
   no `latency` row at all — and the replay's failure was then mis-attributed as 12 stale reads.
   Fixed on both sides: the operation is listed with `n: 0` and its `failed_calls`, and an
   unanswerable replay is counted by its own assertion (`ryw_replay_answered`) instead of being
   folded into staleness. The corrected run reports `ryw_replay_answered = 1 (n = 1)` and
   `ryw_settled_write_visible = 0 (n = 0)` — the same reality, no longer mislabelled.

Five more were found by **reviewing** the harness after those runs, and all five are the same
species: a check that could not observe its own failure. Each is fixed here with a hermetic
test, and each is described at its decision above.

6. **A chaos hook that killed by pattern.** `pkill -9 -f 'humaux-retrieval-worker --serve-rpc'`
   — see D1. Also: it never captured the restarted worker's PID, so the process it created
   could only ever be reaped by another pattern-kill.
7. **A fabricated promotion measurement.** `promote_rejections` returned the stream count as
   `VisibleUnavailable` and never called `evaluate_switch` — see D9. The same run's own PASS
   verdict contradicted the refusal it reported.
8. **The dense-ledger check aggregated per tenant, not per stream** — see D3. Sound only
   because every tenant in these runs had exactly one stream.
9. **Both post-drain read-your-writes assertions could reach `n = 0` and report PASS** — see
   D6 and D10. Observed in this card's own final run.
10. **Chaos exercised one process, not each in turn.** Every run shipped `"chaos_cmds": 1`
    against the retrieval worker, so `no_live_lease_after_drain` had never been exercised
    against either worker that actually holds an `ops.jobs` lease. The rotation is now retrieval
    worker → `--distill-serve` → `--serve` consolidation worker; the gateway and the private
    worker's `--serve-rpc` listener are excluded for reasons the runbook §6 states rather than
    omits.

## Changes outside card 16's file list

Card 16's `Allowed files` are `xtask/src/`, `crates/testkit/{src,tests}/`, `docs/ops/`,
`scratchpad/rehearse.sh`, `docs/adr/00xx-*.md` and the named `Baseline_2.9.md` sections. Six
files outside that list are modified on this tree, all of them consequences of live finding (3)
above — the second tenant that never distilled — and they are listed here rather than left for
a diff reader to discover:

| file | change | pinned by |
|---|---|---|
| `bins/private-worker/src/distill.rs` | the deferral arm names the static error class; payload still never printed | `a_tenant_the_deployment_cannot_admit_is_named_and_does_not_starve_the_ready_one` |
| `crates/application/src/consolidate.rs` | `PrivateReasoningError::classified` / `class()` — the accessor that lets a redacted error contribute its class without its payload | same |
| `crates/adapters/src/contribution_reasoner.rs` | constructs the classified variant at the admission-mismatch site | same |
| `bins/private-worker/tests/derived_dispatch_e2e.rs` | the test above | — |
| `bins/gateway/src/recall.rs` | operator lines `consistency_token_refused reason=<class>` and `limit_not_profile_top_k limit=<n> profile_top_k=<n>` (D6: both refusals were bare `INVALID_INPUT` with nothing in the log, which is what cost a day) | `recall_with_a_consistency_token_answers_and_a_caller_chosen_limit_is_refused` |
| `bins/gateway/tests/mcp_gateway.rs` | the test above | — |

They are product-behaviour changes made under a harness card, which is a real scope violation
and is recorded as one. The alternative was to ship a harness whose two headline findings were
undiagnosable from the logs the product emits.

## Consequences

- `docs/ops/soak.md` is the runbook; `docs/ops/supervision.md` (ADR-0037) stays the
  process-lifecycle half and is unchanged by this card.
- No new table, column, grant, migration, MCP operation or `ErrorCode` variant: the §6.2.2
  matrix, `xtask/src/rls_check.rs`'s `MATRIX`, `SUPPORTED_OPERATION_KEYS` and the catalog
  output union are all untouched, deliberately.
- `cargo test -p xtask` gains 24 hermetic soak tests (no database, no socket) plus the
  `e2e-seed` admission-route test.
- Two live integration tests are added. `a_tenant_the_deployment_cannot_admit_is_named_and_does_not_starve_the_ready_one`
  (`bins/private-worker/tests/derived_dispatch_e2e.rs`) runs in the ordinary chain's
  `workers_tests` gate. `recall_with_a_consistency_token_answers_and_a_caller_chosen_limit_is_refused`
  (`bins/gateway/tests/mcp_gateway.rs`) needs the isolated request-guard PostgreSQL fixture, the
  pinned scanner and a disposable Qdrant, so it carries `#[ignore]` like its sibling
  `native_gateway_semantic_recall_real_qdrant_pg_and_ryw_acceptance` and must be **run** by name
  in the live gate: `cargo test -p humaux-gateway --test mcp_gateway
  recall_with_a_consistency_token_answers_and_a_caller_chosen_limit_is_refused -- --ignored
  --exact` with `HUMAUX_REQUIRE_DB=1`. An `#[ignore]` that no gate log shows *passing* is an
  untested claim — this one shipped that way once, recorded only as `… ignored`. The run that
  closes this card must quote the passing line.
- The report's `verdict` vocabulary gains `FAIL-VACUOUS` (D10). Anything parsing soak reports
  must treat it as a failure.

## Latency (speed is an acceptance goal — card 24 turns these into the baseline)

Every figure below is a **measurement with provenance**, taken after the XProtect warm-up (§5 of
the runbook) on the run named in its last column. Recording them here is not the §78.1 literal
this ADR argues against: §78.1 binds *thresholds inside the binary*, which still come only from
flags and still land in each report's `config` block. A measurement nobody wrote down is a
baseline card 24 cannot inherit, which is the failure this table exists to prevent — the first
version of it said "see run report" in every cell.

Run of record: `delivery-cards-20260903/card16_soak_evidence/soak-report-soak23.json` —
2 tenants x 1 session, `--duration-secs 300 --drain-secs 300 --think-ms 12000`,
`verdict PASS`, database `humaux_thread_soak23`, real MiniMax `MiniMax-M3` + real DashScope
`text-embedding-v4@2026-08`, macOS on loopback.

| hop | p50 | p95 | n | unit | source |
|---|---|---|---|---|---|
| `remember.put` (MCP round trip) | 70.5 | 539.6 | 43 | ms | soak23 `latency[].operation = "remember"` |
| `recall.search` (in-load, token-carrying) | 1778.3 | 2285.4 | 41 | ms | soak23 `latency[].operation = "recall"` |
| `recall` RYW replay (post-drain, token **hit**) | 1468.1 | 1564.3 | 2 | ms | soak23 `latency[].operation = "recall.ryw_replay"` |
| `remember.put` RYW replay (post-drain) | 36.4 | 40.3 | 2 | ms | soak23 `latency[].operation = "remember.ryw_replay"` |
| `memory.enumerate` | 36.5 | 54.7 | 41 | ms | soak23 `latency[].operation = "memory"` |
| distill pass (one Evidence, one real provider round trip) | 3609.9 | 7209.8 | 43 | ms | soak23 `private.processing_runs`, `completed_at - started_at` |
| consolidation pass (§11.8 private-inference RPC round trip) | 6428.3 | 8071.9 | 7 | ms | `ops.private_inference_rpc_calls`, `finished_at - claimed_at`, `outcome = COMPLETED` — **`humaux_thread_dev`, rehearsal of 2026-09-03**, not soak23 |
| projection resolve (§15.1 ticket `issued_at` → `settled_at`) | 11888.3 | 23101.3 | 47 | ms | soak23 `projection.stream_log` |

Four things a reader of these numbers has to know, or they will be misread:

- **The consolidation row is from a different run.** soak23's load drives
  `remember` → `recall` → `memory.enumerate` only, so its 36 `DERIVED_CONSOLIDATE` jobs finished
  with nothing eligible to consolidate and made zero provider calls
  (`private.memory_rollups = 0`, `private_inference_rpc_calls = 0` for both tenants). The row is
  the most recent real consolidation round trip on this deployment instead, and it is labelled
  as such rather than left blank or quietly borrowed. Card 24 should re-take it under load.
- **"projection resolve" is the whole derived chain**, not the projection worker's own step: it
  spans distill, consolidation dispatch and projection, and it includes the rehearsal's 5 s
  projection-loop period, which is a harness artefact and not a property of the system.
- **`recall` is dominated by the embedding provider**, on the same budget as the distill hop, so
  an over-subscribed run inflates it and starts returning `DEPENDENCY_UNAVAILABLE` — see the
  runbook's capacity section before comparing two runs' `recall` numbers.
- **Throughput, not latency, is what sizes a run**: the distill hop's measured capacity is
  **0.229 Evidence/s** (n = 140 `private.processing_runs` over 611 s, same deployment,
  2026-09-10). `docs/ops/soak.md` §2 turns that into the rule for picking
  `--sessions-per-tenant` and `--think-ms`.

Every soak run emits the first five rows' shape into its own report's `latency` array, so a
future run is compared against this table by reading the same field, not by re-deriving it.
