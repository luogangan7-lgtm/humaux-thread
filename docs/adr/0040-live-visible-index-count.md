# ADR-0040 — The completeness denominator gets its numerator: a live Qdrant `visible` count on every read route

- Status: accepted
- Date: 2026-09-12
- Card: 18
- Spec: §23.1② (A1/A2, `visible`, `completeness_ratio`, `PROJECTION_INVISIBLE_LOSS`), §22.4/§22.5
  (`cannot_establish` reasons; `ledger::close` never judges A2), §16.2 (blue/green: the read
  routes see only the `serving = true` version), §17.1 (tenant + visibility injection is not
  optional — the filter has exactly one builder), §37 (tombstone first, physical purge later),
  §57.1 / §4.4 坑5 (a missing measurement is `not_applicable`, never `0`), §78.1 (no literals at
  a contract boundary)
- Composes with ADR-0017 (first-activation), ADR-0029 (subject axis), ADR-0030 (affect axis),
  ADR-0031/0032 (per-request stream identity), ADR-0035 (workspace membership scope).
  Supersedes nothing.

## Context

§22/§23.1 exist so an agent can tell "here is the answer" from "here is part of an answer".
Every piece of that contract was implemented and unit-tested — and **none of it was reachable in
production**.

All three read routes called `build_projection_block(&ledger, None)`:

| route | call site |
|---|---|
| `recall.search` | `bins/gateway/src/recall.rs` |
| `memory.get` / `memory.enumerate` | `bins/gateway/src/memory.rs` |
| `context.assemble` | `bins/gateway/src/context.rs` |

`None` is not a neutral placeholder there. `envelope::build_projection_block` returns a block
with `completeness_ratio: None` for a missing `visible`, and `final_completeness_class` then
overrides whatever `classify()` decided with `CannotEstablish { IndexCountUnavailable }`. So
**every successful read on every deployment reported `cannot_establish` /
`index_count_unavailable`**, regardless of how healthy the projection actually was. It was
observed in the deployment rehearsal, and it is mechanically guaranteed by those three `None`s.

The pieces to fix it were already in the tree — `qdrant::VisibleCountFilter`, `qdrant::count`,
`qdrant::overlay_filter`, and a doc comment on `retrieve::RecallEnvelope::serving_version` that
states the rule in full. Nothing had ever called them from a read path.

## Decision

**D-A. One producer, `retrieve::visible_index_count`, and every route goes through it.**
It takes the ledger key, the family's serving version and the already-closed `LedgerClosure`,
and returns `Option<u64>`. There is no second place in the workspace that turns a Qdrant count
into a `visible` number. `recall.search` calls it directly (it has already resolved the tenant
placement and minted a Qdrant permit for its own dense query); `memory.*` and `context.assemble`
reach it through `ContextBootstrap::visible_index_count`, which borrows the configured
`SemanticRecallRuntime` purely as a count face. On a deployment with no semantic lane, that is
`None` — and `None` is the honest answer: there is no index to count.

**D-B. No serving version ⇒ `None`, never `Some(0)` — and a serving version that is not the
ledger's version is the same "no measurement".**
§16.2 says a read may only see the family's `serving = true` version. A family with no serving
row has no retrieval face, so "how many points are visible on it" has no denominator. That is
`cannot_establish` (§57.1), and `0` would be the *different*, false claim that the index is
empty. This is why `provisioned_request_stream` now returns the serving version instead of
discarding it: the ledger key carries this process's configured `projection_version` (ADR-0031's
Q9 ruling), which is a different string from the serving one mid-switch, and counting the wrong
face is exactly the §16.2 failure the rule forbids.

That has a second half the first version of this card missed, and a review caught: A2 is
`visible + deleted + skipped == done`, and `done` comes from a `LedgerClosure` closed at
`key.projection_version`. Counting the *serving* face while the ledger describes a different
version compares two different faces and fabricates a `PROJECTION_INVISIBLE_LOSS` — or an A2
overshoot, i.e. `cannot_establish` — on a perfectly healthy projection. The divergence is
structural, not hypothetical, on `memory.*` / `context.assemble`: `provisioned_request_stream`
derives the key from this process's configured version and reads the family's `serving = true`
row separately, and `context_repo` closes the ledger at the configured key. So
`visible_index_count` returns `None` unless `serving_version == key.projection_version`, and
everything below that guard — the filter, the §37 tombstone overlay, the ledger — uses one
string. `recall.search` was already coherent (it builds its key from the serving version it just
read); the guard is what makes that a property of the shared producer instead of an accident of
one call site. Leg 6 of the acceptance test is the mid-switch shape, with a control call at the
matching version that still returns a number.

**D-C. The filter comes from the shared builder and narrows by `projection_version` only.**
`VisibleCountFilter::new` routes through `projection::dense::build_dense_filter`, which
unconditionally ANDs the tenant clause and the §6.1.2 visibility disjunction — so the count sees
exactly the point set the caller may see, and the hand-written second filter
`crates/projection/tests/no_handwritten_filter_scan.rs` pins cannot arise. It does **not** carry
the request's `subject_ids` / `affect` / `embedding_version` clauses.

*Rejected alternative: make the count subject- and affect-aware, i.e. count exactly what the
dense query counted.* It is the intuitive reading of "scope the count the same way the query is
scoped", and it is wrong here, for a reason that is only visible once you look at what `visible`
is compared against. A2 is `visible + deleted + skipped == done`, and `done` is a whole-stream
number read from `projection.stream_log` — no `recall.search` argument narrows it. Narrowing
only the left-hand side would make every subject- or affect-scoped recall report
`PROJECTION_INVISIBLE_LOSS` with a ratio proportional to how narrow the filter was. The block is
called `pipeline.projection` because it grades the projection pipeline, not the result set; the
result set is already graded by `returned` / `candidate_count`.

**D-D. Tombstones are excluded by the overlay, inside the same count request.**
§37's step 1 (`state -> TOMBSTONED`) and step 5 (physical purge) are separated in time, and on
this deployment step 5 is not wired at all — so a tombstoned row's point is normally still in
the index and still matches the filter. Counting it inflates A2's left side by exactly
`ledger.deleted`. The exclusion is a `must_not` clause folded into the same `points/count` body
(`qdrant::count_visible_excluding_seqs`), never an arithmetic subtraction afterwards, so the
answer is the same on either side of the purge — a seq whose point was never indexed, or was
already purged, is simply not there to exclude and is never subtracted twice.

*Rejected alternative: `visible_count(raw, ledger.deleted)`, the arithmetic form that already
exists.* It double-subtracts a tombstoned seq whose point was never indexed, producing a false
`PROJECTION_INVISIBLE_LOSS`. It is kept for the callers that hold only two numbers and no
transport.

**D-E. The overlay is keyed by `source_stream_seq`, not by point id.**
§37 names both keys ("point id 由它派生亦可，二选一"). A `TOMBSTONED` row is identified by
`stream_seq`; resolving each one to its opaque projection point id would mean a second
`private.projection_registry` round trip on every read for an answer the payload field already
gives. `qdrant::tombstoned_seq_overlay_filter` is the second builder, sharing `overlay_filter`'s
single `must_not` fold. The read is skipped entirely when `ledger.deleted == 0`, which is the
common case.

## Consequences

- `recall.search`, `memory.get`, `memory.enumerate` and `context.assemble` now report a real
  `pipeline.projection.visible`, a real `completeness_ratio`, a real `current`, and
  `PROJECTION_INVISIBLE_LOSS` when the index has actually lost something.
- **`cannot_establish` has not disappeared from those responses, and this card does not claim it
  did.** `final_completeness_class` has two projection-side triggers and one pipeline-count
  trigger. The projection-side ones (`index_count_unavailable`, `a2_overshoot_beyond_pending`)
  are what this card fixes, and the live witness asserts `index_count_unavailable` is gone. The
  remaining one is `count_unknown` from `PipelineBlock::count_inconsistency_reason`, which
  requires `evidence.persisted` and all four `knowledge.*` counts *in `CountScope::StreamLedger`*
  — no read route reads them, each declaring `AuthorizedView` with `None`s on purpose (a page
  length is not an independent pipeline census; `accept_memory_envelope` carries that note). So
  the observable change is `reason: index_count_unavailable` → `reason: count_unknown` with a
  fully established `pipeline.projection` block underneath it. Making the evidence/knowledge
  census real is a separate card; faking it would be the §23.3 substitution mistake the type
  already refuses.

  **That residual is what §23.3④ mandates, not a shortfall against it.** Verbatim
  (`Baseline_2.9.md:5757-5759`): 「已知同口径但数值矛盾为 `pipeline_count_mismatch`，缺必要读数
  为 `count_unknown`，跨口径为 `count_scope_mismatch`；三者都使完整 envelope 为
  `cannot_establish`」; and the same section's DirectGet paragraph: 「没有独立 census 的对象读取
  不生成 `exact` 或 `known_lower_bound`，未读 pipeline 数值依本节保留未知」. The same section
  also closes the one derivation that would flip the class without new reads: 「禁止填 `0`、
  返回条数、`issued_highwater` 或其他块的值充数」 — so `evidence.persisted := projection.expected`
  is explicitly forbidden. Card 18's gate line asks for "a completeness class other than
  `cannot_establish`/`index_count_unavailable`": the `reason` half is met and asserted on the
  live witness; the `class` half cannot be met without a real evidence/knowledge census, which
  needs the retrieval worker to advance `stream_checkpoints.evidence_highwater` /
  `knowledge_highwater` (nothing in the workspace writes them today — the only hits are the
  GRANT in `0011_roles_and_grants.sql` and two test fixtures) plus a knowledge-state census with
  no table behind it yet. Both are outside this card's allowed files and outside its scope
  sentence. Reported with the citation rather than closed by relaxing
  `PipelineBlock::count_inconsistency_reason`, which would be the §80.1 "a check that can never
  observe its own failure" mistake.
- Cost per read: one exact Qdrant `points/count`, plus one PG `SELECT stream_seq` only when the
  stream has tombstones, plus (on the two PG-only routes) one `tenant_placement` lookup.
- `ContextBootstrap` now carries an optional `Arc<SemanticRecallRuntime>`; the single attach
  point is `GatewayMcpApplication::with_semantic_recall`, the one place holding both values.
  **`bins/gateway/src/mcp_application.rs` is not on card 18's allowed-files list, and this is a
  declared exception rather than an oversight.** The card's scope sentence is "thread `Some(v)`
  into `build_projection_block` at all three call sites"; two of those three routes hold no
  Qdrant transport of their own, and `with_semantic_recall` is the only place in the binary
  holding both the bootstrap and the runtime. The only alternative inside the list is to leave
  `memory.*` and `context.assemble` on `None` — i.e. not to do two thirds of the card. The edit
  is three lines plus a comment, adds no branch and no state, and is recorded here so it is read
  as a decision instead of being found in `git status`. (The two debts folded into this card are
  a different case: the card's own preface declares them in scope, so their files are not
  "exceptions" — see "Out of scope" for why they are nonetheless unimplemented.)
- `ContextBootstrap::provisioned_request_stream` and `memory::read_scope` return one more
  element (the serving version). Callers that do not need it bind it as `_serving`.

## Latency

Measured against real Qdrant 1.19 on 127.0.0.1:6333 and real PostgreSQL, `cargo test -p
humaux-adapters --test visible_index_count`, 100 indexed points, 10 of them tombstoned (so every
sample pays both the extra PG read and the exact count):

| measurement | p50 | p95 | n |
|---|---|---|---|
| `retrieve::visible_index_count` end to end (PG tombstone read + exact Qdrant count) | 2.252 ms | 2.723 ms | 40 |

That 2.4 ms is the whole before/after delta on a read: nothing else on the path changed. For
scale, the same run's `recall.search` end to end through the real gateway, real embedding RPC and
real Qdrant measured p50 = 1169 ms unfiltered / 1179 ms with an affect filter (n = 5 each,
`native_gateway_semantic_recall_real_qdrant_pg_and_ryw_acceptance`) — the count is ~0.2 % of it;
the query-embedding RPC dominates.

The exact count dominates and it stays exact: §23.1②'s ratio needs a true denominator, and
Qdrant's approximate fast-count path would make `completeness_ratio` a number with no defined
error bar. At this corpus size the cost is ~2.3 ms; `projection_version` carries no payload
index today (only `tenant_id` and `subject_ids` do), so this is a filtered scan and will grow
with the collection. If it becomes the dominant term on a large tenant, the fix is a keyword
payload index on `projection_version` at collection provisioning time — not an approximate
count. Card 24 turns these into the delivery baseline.

## Acceptance

- `crates/adapters/tests/visible_index_count.rs` (new, real PG + real Qdrant), six legs:
  healthy ⇒ `visible = 100` / ratio `1.0` / `current`; 10 legal tombstones with the points still
  indexed ⇒ `visible = 90`, ratio still `1.0`, no degradation; **fault injection** — the same
  filter through raw `qdrant::count` reads 100 and `completeness_ratio` collapses to `None`, so
  the previous assertion is falsifiable; 5 points deleted straight out of Qdrant ⇒ ratio `< 1.0`
  and **not** null, `current` cleared, `PROJECTION_INVISIBLE_LOSS`; no serving version ⇒ `None`,
  asserted `!= Some(0)`; and a serving version that is **not** the ledger key's version ⇒
  `None`, with a control call at the matching version that still returns `Some(100)` so the leg
  is about the mismatch and not about some unrelated fixture failure.
- `bins/gateway/tests/mcp_gateway.rs`, live witness inside
  `native_gateway_semantic_recall_real_qdrant_pg_and_ryw_acceptance`: the real MCP response
  carries `pipeline.projection` = `{visible: 3, expected: 3, done: 3, completeness_ratio: 1.0,
  current: true}`, `reason != "index_count_unavailable"`, and no degradations; then one extra
  settled ledger row with no point behind it forces `visible: 3 / done: 4`, `ratio = 0.75` and
  `degradations = ["PROJECTION_INVISIBLE_LOSS"]`, after which the fixture is restored. The
  residual `reason == "count_unknown"` is asserted too, so the next card that moves it has to
  come through this line. The fixture gained `seed_semantic_projection_ledger`: it hand-seeds
  Qdrant points, and before this card it seeded no ledger rows behind them (harmless while
  `visible` was always `None`; a genuine A2 overshoot once it is not).
- `bins/gateway/tests/mcp_gateway.rs`, same fixture, the other two read routes: `memory.get`,
  `memory.enumerate` and `context.assemble` are called against the *same* running gateway and
  must each report `pipeline.projection` = `{visible: 3, completeness_ratio: 1.0, current: true}`
  with `reason != "index_count_unavailable"`. This is the only fixture in the suite built
  `with_semantic_recall`, so it is the only place those two routes have an index face at all —
  without this leg, deleting `ContextBootstrap::attach_index_face` or either
  `Some(serving_version.as_str())` argument leaves the whole suite green (a review caught exactly
  that). **Fault injection, run 2026-09-12:** replacing
  `self.context_bootstrap.attach_index_face(&runtime)` with a no-op makes `memory.get` answer
  `visible: null`, `completeness_ratio: null`, `reason: index_count_unavailable` and the leg goes
  red (`EXIT=101`); restoring it is green again — so the leg observes its own failure (§80.1).
- `crates/testkit/tests/metrics/retrieval_completeness_total.rs` still covers every class/reason
  pair (unchanged by this card). **The invocation is `cargo xtask metrics-registry`, not
  `cargo test -p humaux-testkit --test <...>`** — `humaux-testkit` declares no `[[test]]` entry
  for `tests/metrics/`, deliberately: `xtask::metrics_registry::run_witness_probe`
  (`xtask/src/metrics_registry.rs:462`) copies each witness file into its own one-off probe
  package and runs it as a real `cargo test`, which is what gives each metric family a private
  process and therefore a private global counter. Recorded run: D2/D6 report
  `tests_passed=13 tests_total=13` over `|W|=6` witness files, exit 0. Pulling the six files into
  one shared `tests/metrics_main.rs` binary instead was tried and rejected during this fix: the
  three `retrieval_provider_*` witnesses then share one process's counters and three of them go
  red on interference (`left: 2, right: 1`), which would have been a second, worse runner for a
  gate that already exists.

## The two folded-in debts — decided here, landable only outside this card

Card 18's preface declares both debts **in scope**. Both are decided below. Neither is
implemented, and in both cases the blocker is named with the exact line that blocks it rather
than with "allowed files" alone — an earlier revision of this ADR filed them under "out of
scope", which a review correctly called a non-answer.

### D-F. Debt 1 — a settled `FAILED` ticket pinning the §15.4 contiguous-DONE prefix

**Decision: reject the formula change; the poison ticket must leave the unsettled set by an
explicit, audited state transition. Both halves of that decision are blocked inside this card,
for different reasons.**

The card offered two shapes: *(a)* treat `FAILED` as settled in the prefix filter, like
`SKIPPED_BY_POLICY`, or *(b)* tombstone/compact the ticket. **(a) is rejected**, on three
independent grounds:

1. **§15.4 freezes the opposite, with a worked example.** `Baseline_2.9.md:3970`: 「`101` 之前
   每一个 seq 在 stream_log 里都有行，`100` 处于 `FAILED` ∉ `SETTLED_OK`，
   `contiguous_done_prefix = 99`，`advance_prefix` 只能写 99」. `crates/adapters/src/stream_repo.rs`
   carries that example verbatim in the comment above the query. Card 18 may edit
   `Baseline_2.9.md` only in "the sections the card names" — §46, §6.2.2/§48.2, §52, §78.1/§78.2,
   §33.10. §15.2 and §15.4 are not among them, so (a) is not a change this card is permitted to
   make even setting correctness aside.
2. **The prefix is not only a promotion input.** §15.4 (`Baseline_2.9.md:3884`) makes
   `contiguous_done_prefix` the *lower bound of the read-your-writes overlay*: everything above it
   is served from PG rather than from the index. Advancing the prefix past a `FAILED` seq declares
   that seq "covered by the projection" — its content then stops being served by the overlay while
   no point for it was ever indexed, i.e. it silently disappears from reads. That is the exact
   「刚记住的内容不能立即失忆」 failure the overlay exists to prevent, and it would be introduced
   by a change whose stated purpose is to *unblock* reads.
3. **It is not what ordered-log systems do.** Kafka Connect's answer to a poison record is
   `errors.tolerance=all` plus an explicit `errors.deadletterqueue.topic.name`: the record is
   routed out of the stream, with headers recording exception class, stack trace, source topic,
   partition and offset, and *then* the consumer advances. The offset semantics are never
   redefined so that "failed" reads as "done" — the record is moved, and the move is auditable.
   Debezium, Flink and Kafka Streams all take the same shape. (a) is the redefinition; (b) is the
   DLQ.

**Accepted shape (b), not implemented:** a maintenance-role retirement path that moves an
exhausted `FAILED` row to a state inside `SETTLED_OK`, carrying the failure reason, only once
§15.2's retry budget is spent — the DLQ-headers analogue. It needs, at minimum:

- a **migration**: `migrations/0011_roles_and_grants.sql:429` legalizes exactly
  `OLD.state = 'ISSUED' AND NEW.state IN ('DONE','SKIPPED_BY_POLICY','FAILED')`, so
  `FAILED -> <settled>` is refused by the database today, by design;
- a **§6.2.2 row + `rls_check` MATRIX entry** for whichever role gets that transition;
- a **§15.2 amendment**, because the open sub-decision is which settled state it lands in:
  reusing `SKIPPED_BY_POLICY` overloads §18.2's `SecretMaterial`/`ExcludedSecret` meaning onto a
  distillation failure, and a new terminal state changes §15.2's frozen state set and the
  `ProcessingState::ALL` contract test that pins it against the CHECK constraint.

None of those three files or sections is editable from this card, and the sub-decision is a
spec-owner call, not an implementer's. What this card can and does record: the formula stays as
it is (`crates/adapters/src/retrieve.rs` and `crates/adapters/src/stream_repo.rs` are byte-for-byte
the same query in two places and remain in agreement), and soak21's terminal state
(27× `no_serving_projection`, `recall_dependency_unavailable_rate = 1.0`) reproduces unchanged.

*Rejected alternative, recorded so the next card does not re-derive it:* changing only
`retrieve::contiguous_done_prefix_in_txn` (the one copy that IS in an allowed file) and leaving
`stream_repo::fetch_snapshot_in_txn` alone. That puts the reader and the writer's cross-proof
into disagreement over one formula — §15.4's whole point is that the three numbers cross-prove
each other — and it would make `advance_prefix` and the request path answer differently for the
same stream. Strictly worse than the defect.

### D-G. Debt 2 — `cargo xtask soak`'s `projection_promoted` assertion becoming required

**Decision: the assertion cannot become required in this card, and `expected_red_until("card 18")`
is now the wrong marker — but the correct marker names files this card may not edit.**

The review is mechanically right that nothing this card added can lower the `VisibleUnavailable`
tally, and the earlier draft of this ADR claimed otherwise. Verified in the tree:

- `xtask/src/soak.rs` builds every `SwitchCriteria` with `visible_shadow: None,
  visible_serving: None` and `continuation: ContinuationVerdict::CannotEstablish`, so
  `projection::serving::evaluate_switch` takes its `_ =>` arm and pushes `VisibleUnavailable`
  plus `BenchmarkNotPass` for **every** candidate, unconditionally. That tally is a readout of the
  harness's own inputs, not of the deployment.
- `retrieve::visible_index_count` is wired to the **read** face only. The switch path still takes
  a hand-typed number: `xtask/src/projection_serve.rs` parses `--visible-shadow` and passes
  `None` for the serving side.
- Even with both counts supplied, `OpenGaps` still refuses any stream carrying a `FAILED` ticket,
  which is D-F — and `BenchmarkNotPass` still refuses everything while §69's `baseline_min` /
  `frozen_by` are `NOT_DECLARED`.

So three independent refusals gate that assertion, and card 18 owns none of the three. Flipping
`expected_red_until` to required would convert a reported red into a blocking red without
removing any of them; deleting the marker without a re-run would be worse still. The honest
change is to re-point the marker at the card that owns the switch-path count and at D-F, and to
re-run the soak once *that* card lands — both `xtask/src/soak.rs` and `docs/ops/soak.md` are
outside this card's allowed files, so the re-point is **reported here, not applied**. No soak run
is recorded for card 18: a run whose verdict is decided in advance by three refusals this card
cannot touch is not evidence, and `docs/ops/soak.md`'s capacity budget is better spent by the
card that can move one of them.

---

## Card 19 addendum (2026-09-12) — the serve-side half of D-A

D-G above recorded, correctly, that card 18 could not lower the `VisibleUnavailable` tally and
that the marker was pointing at the wrong card. This addendum is that follow-up: the §16.2
switch now takes its `visible_*` inputs from the **same producer** the read routes use.

### D-H. Diagnosis — where the serve-side `visible` came from, and why it was unavailable

`projection::serving::evaluate_switch` has exactly two callers, and neither had ever measured
anything:

| caller | `visible_shadow` before | `visible_serving` before |
|---|---|---|
| `adapters::serving_repo::switch_projection_version`, via `cargo xtask projection-serve` | the operator's `--visible-shadow <n>` command-line argument (the rehearsal filled it with `count(*) FROM projection.private_memory_points`, a PostgreSQL row count never compared against the index it claimed to describe) | hard-coded `None` |
| `xtask::soak::switch_rejections` (the `projection_promoted` report) | literal `None` | literal `None` |

`None` on either side is `SwitchRejection::VisibleUnavailable`, so the soak's tally was a readout
of the harness's own inputs. It is the same shape card 16 removed from this very function once
already (a `count(*)` relabelled as a rejection) surviving one layer up: card 18 wired the live
count into `build_projection_block` on the three read routes and nothing on the switch path
changed, because nothing on the switch path had ever called a counter.

### D-I. One producer, and which version each side counts

`retrieve::visible_index_count`'s body is split: the filter construction and the counted request
become `retrieve::visible_count_of_version(index, scope, projection_version, tombstoned_seqs)`,
and `visible_index_count` keeps its two ledger guards and delegates. `xtask::switch_visible`
calls that same function. There is still exactly one place in the workspace that turns a Qdrant
count into a `visible` number, and still exactly one `VisibleCountFilter` builder — so
`crates/projection/tests/no_handwritten_filter_scan.rs` covers the switch path by construction
(the ops caller never names a `Condition`).

**The version decision.** `visible_index_count` refuses when `serving_version !=
key.projection_version` (D-B above). That guard is **not** reproduced on the switch path, and the
reason is the comparison each side makes:

- the read route compares its count against a `LedgerClosure` closed at `key.projection_version`
  (A2: `visible + deleted + skipped == done`). Two different faces there is a manufactured
  `PROJECTION_INVISIBLE_LOSS`, hence the guard;
- §16.3 criterion ① compares a count against **another count**, and requires the two to differ in
  exactly the `projection_version` filter and in nothing else. There is no ledger in the
  comparison at all.

So the switch counts the **candidate** version for `visible_shadow` (that is the version
`evaluate_switch` is judging — not the one already serving) and the family's current `serving`
version for `visible_serving`, `None` there meaning ADR-0017 first activation. Reproducing the
read route's guard would have returned `None` for every genuine (candidate ≠ serving) promotion:
`VisibleUnavailable` forever, by construction.

### D-J. The ops `AuthorizationScope`, and its one refusal

A switch has no end user, so `switch_visible::ops_scope` builds `AuthorizationScope::new(tenant,
<fresh principal>, user_id = None, {workspace})`. `projection::dense`'s §6.1.2 disjunction then
carries no `USER_PRIVATE` arm, so points of that class are counted on **neither** side — and two
undercounts that agree by absence are precisely the 恒真闸 §16.3 warns about (a shadow face
missing every user-private point would compare equal to the serving face and promote). The count
therefore **refuses** rather than undercounts: `USER_PRIVATE_PROBE_SQL` asks whether this tenant
has any live projected point whose memory is `USER_PRIVATE`, and both sides return `None`
(`VisibleUnavailable`, the honest refusal) if it does. Over-refusal, never a false pass.

`None` also stands — unchanged and deliberately — for: no §17.3 placement row (no collection to
count against), an unreachable or erroring Qdrant, and an empty version string. §23.1②'s rule
holds on the switch path exactly as it does on the read path: an uncountable index is
`visible: null`, never a backfill from another number.

### D-K. Before / after — `projection_promoted.detail.reject_reasons`

Same driver, same config (`SOAK_SECS=380 SOAK_SESSIONS=1 SOAK_THINK_MS=10000 SOAK_DRAIN=200
SOAK_CHAOS_SECS=90`), fresh database each time, 2 tenant lanes, **n = 2 candidates** (one
`projection.stream_checkpoints` row per lane, both `serving = true` at report time):

| run | `VisibleUnavailable` | `VisibleSameVersionDeclared` | `OpenGaps` | `BenchmarkNotPass` |
|---|---|---|---|---|
| `humaux_thread_soak26` (card 18, before) | **2** | 0 | 2 | 2 |
| `humaux_thread_soak27` (this card, after) | **0** | 2 | 2 | 2 |

Reports: `delivery-cards-20260903/card18_soak_evidence/soak-report-humaux_thread_soak26.json` and
`…-soak27.json`. Both runs: `projection_promoted = 1 streams (n = 2, at most 0)`, verdict
`EXPECTED-RED`, soak exit 0, every other assertion PASS.

`VisibleSameVersionDeclared` is new, and it is not a rename of the reason it replaced — it is
what the evaluator genuinely returns once the counts are real, and it says something true that
`VisibleUnavailable` was hiding. Both of this deployment's candidates *are* their family's
`serving` row (one `projection_version`, `v1`, per family), and the rehearsal's promote loop
re-runs `projection-serve --version v1` every five seconds after the first activation has
already switched it. Promoting a version to itself compares a count with itself — §16.3's 恒真闸
shape, rejected on the version tag before the numbers are even looked at. The live ops path says
the same thing: against soak26, `projection-serve` now prints
`visible shadow=Some(("v1", 25)) serving=Some(("v1", 25))` and rejects
`[VisibleSameVersionDeclared, OpenGaps]`. Whether the promote loop should stop re-offering the
serving version (or the harness stop grading it as a candidate) is a question for the card that
owns the loop; it is recorded here, not fixed here, because changing the candidate set would move
the `OpenGaps` / `BenchmarkNotPass` tallies this card is required to leave alone.

soak27's first activation is the other half of the evidence — the one case where a real count
decides a real switch: `projection-serve: visible shadow=Some(("v1", 3)) serving=None` ⇒
`switched v1 to serving`. Before this card that `3` was `--visible-shadow $(SELECT count(*) FROM
projection.private_memory_points)`, a number no one had compared against the index.

### D-L. `expected_red_until` moves to card 20

`projection_promoted` grades §15.4 prefix advance, not criterion ①, so clearing
`VisibleUnavailable` does not by itself make it green: `OpenGaps` (D-F's `FAILED` ticket is
terminal, so the prefix never clears it) and `BenchmarkNotPass` (§69's `baseline_min` /
`frozen_by` are `NOT_DECLARED`) both remain, and both are card 20's. The marker in
`xtask/src/soak.rs::promotion_assertion` is therefore re-pointed from `card 18` to `card 20`
rather than deleted — D-G's own instruction, now applied rather than only reported. soak27
confirms it is still the right call: `projection_promoted = 1 streams (n = 2, at most 0)`,
`EXPECTED-RED`, `expected_red { id: projection_promoted, until: card 20 }`.
