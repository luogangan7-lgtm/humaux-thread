# ADR-0042 — The model-call ledger covers the private reasoning hops, not just the cheap ones

- Status: Accepted
- Date: 2026-09-13
- Card: 20
- Spec: §19 / §19.1 (ModelCallLedger, reserve→finalize, cost arithmetic lives in the pricing
  registry), §11.6 / §11.7 / §11.8 (private reasoning hops), §7.4 (disclosure ledger — what
  left the boundary), §46 (forward-fix migration 0166), §6.2.1 / §6.2.2 (grant surface —
  unchanged by this card), §78.1 / §78.2 (no literals, closed enums), §78.3 (where a shared
  vocabulary lives), §23.3④ (禁止填 `0` 充数).
- Supersedes nothing. Retires one line of ADR-0015 ("§ledger leg": the distill and
  consolidation hops have no `ops.model_call_ledger` row) and the `model_call_ledger` entry in
  the 2026-09-03 technical-debt list.

## Context

Two ledgers exist for an external model call and they answer different questions.
`ops.data_disclosures` (§7.4) answers **what left the boundary** — processor, region, data
class, payload digest, sources, outcome. `ops.model_call_ledger` (§19.1) answers **what it
cost** — provider, model, tokens, latency, actual cost, error class.

Before this card the §11.6 distill hop and the §11.7 consolidation hop produced only the first.
`crates/adapters/src/distill_reasoner.rs` and `crates/adapters/src/consolidation_reasoner.rs`
both ran the shared provider pipeline (`authorize_structured_egress` →
`disclosure::reserve_private` → `complete_structured_timed` → `disclosure::finalize_private`)
and both threw away the two values that pipeline already computes:

```rust
let (response, disclosure_outcome, _model_outcome, _finalize) =
    complete_structured_timed(self.provider, &context, request).await;
```

`_finalize` is a fully populated `FinalizeCall` — input tokens, latency, error class. It was
discarded because `migrations/0130`'s `model_call_ledger_purpose_known` CHECK closed `purpose`
to `['query_rewrite','embedding','rerank','CONTRIBUTION_DEIDENTIFY']`, so there was no legal
row to put it in. Both module docs recorded the gap honestly and neither hop could close it
from inside its own file ownership.

The consequence is the one a reviewer preparing for a paying customer calls P1: the two most
expensive paths in the system are the two with no cost row anywhere, and card 16's soak run
would burn real provider budget with nothing to show for it. "Cost is visible for the cheap
calls and invisible for the expensive ones" is worse than no ledger — it reads as coverage.

## Decisions

**D-A — Widen the CHECK, do not build a second receipt mechanism.**
`migrations/0166_model_call_ledger_private_purposes.sql` (§46 forward-fix, EXPAND-only) adds
`PRIVATE_DISTILL_TEXT`, `PRIVATE_DISTILL_VISION`, `PRIVATE_CONSOLIDATE` to the existing CHECK.
The three literals are exactly the §7.4 disclosure purposes
`adapters::contribution_execution_repo` already maps `PrivateReasoningPurpose` to, so one
provider call's ledger row and disclosure row read as the same purpose and no second lookup
table exists. A new `ops.private_model_calls`-shaped table was rejected: the existing ledger
already carries every column these hops need, already has the append-only + one-shot-finalize
guard (0094/0130), already has RLS, and a parallel table would make "total spend" a UNION that
someone eventually forgets to update.

**D-B — The purpose column becomes a closed Rust enum.**
`humaux_domain::ledger::ModelCallPurpose` (7 variants, `ALL`, `as_db_str`, `from_db_str`) is the
single Rust-side truth, and `ReserveCall::purpose` changed from `Option<String>` to
`Option<ModelCallPurpose>`. With only three retrieval values the stringly-typed field was
survivable; once the private hops write the same table, a free string is a second unchecked
vocabulary for the column that decides **whose budget was burned** (§78.2). It lives in
`humaux-domain` rather than `humaux-adapters` for the reason `domain::ledger`'s own module doc
gives about `a1_holds`: the retrieval plane and the private reasoning plane are sibling crates
that do not depend on each other, so putting the vocabulary in their common dependency makes
"only one copy" a property of the dependency graph instead of a discipline.

**D-C — One registration point, two pool wrappers.**
`model_call_ledger::reserve_private_call` / `finalize_private_call` run the **same**
`reserve_in_txn` / `finalize_in_txn` statements as `reserve_call` / `finalize_call`; they exist
only because the `PgPool` accessors on `postgres::*DbPool` are `pub(crate)` (G6-DB1 keeps the
role a type, not a convention), so a per-role wrapper is the only way to share one write path
across two roles. This is exactly the split `disclosure::reserve_private` /
`reserve_retrieval` already uses for `ops.data_disclosures`. `private_reserve_call(purpose,
locator)` is the one place a resolved route becomes a ledger reservation, so distill and
consolidation cannot drift on which columns a private row carries.

**D-D — Private rows are platform-paid, so they take 0130's plain arm.**
`model_call_ledger_reasoning_snapshot_shape`'s second arm already says "purpose IS DISTINCT
FROM 'CONTRIBUTION_DEIDENTIFY' ⇒ every route/billing column NULL", which is the correct shape
for these rows. The private hops resolve the same
`control.resolve_user_reasoning_admission` route, but §11.8's USER-paid snapshot (credential
authority, billing instrument, health observations, `billing_responsibility='USER'`) asserts a
billing relationship distill and consolidation do not have. Claiming it would be a lie in the
one ledger that exists to stop lies about spend. So 0166 changes no trigger and no other CHECK.

**D-E — `estimated_cost` and `actual_cost` stay NULL, on purpose; BOTH token dimensions do not.**
§19/§78.1 put cost arithmetic in `humaux_retrieval_provider::cost` against a
`control.provider_pricing_versions` row, and the private reasoning models have no pricing row.
A fabricated `0.0` would be worse than the honest NULL (§23.3④ "禁止填 `0` … 充数") — it would
make unpriced spend look like free spend, which is the exact failure this card exists to end.
Token usage and latency are real and land at finalize; the money column becomes real the day a
pricing row exists, with no code change.

That last clause was **not true as first delivered** (review finding, `contribution_reasoner.rs`
`complete_structured_timed`) and migration **0168** is what makes it true. A generative call
bills two dimensions — `prompt_tokens` at `input_token_price`, `completion_tokens` at
`output_token_price` — and `cost::compute_cost` has always taken both
(`UsageSnapshot { billable_tokens, output_tokens }`), with `output_token_price` on the pricing
table since 0095. But §19.1's column set was shaped for the retrieval plane, which has ONE
billed dimension: there was nowhere to put `completion_tokens`, so `byok` parsed it and
`complete_structured_timed` dropped it, and `billable_tokens` — the column cost is computed
from — was simply never written. A pricing row plus `input_tokens` cannot compute a generative
cost, and for these two hops the output leg usually dominates it. So:

* 0168 adds `ops.model_call_ledger.output_tokens` (nullable bigint, same `>= 0` CHECK as its
  three siblings), and adds it by name to the two guards that enumerate the outcome columns
  (`ops.model_call_ledger_guard_mutation`'s set-once list, `ops.reasoning_model_call_validate`'s
  `num_nonnulls` reservation-purity count) — a column no guard names would be the one outcome
  column a second UPDATE could rewrite;
* `contribution_reasoner::finalize_from_usage` (extracted for exactly this reason: it is now a
  pure function with its own runnable check) writes `input_tokens`, `billable_tokens` **and**
  `output_tokens` for every hop that goes through `complete_structured_timed` — contribution,
  distill and consolidation alike. `billable_tokens` stays the input-priced dimension (§19's
  rerank formula is why it is a separate column from `input_tokens` at all; for a chat
  completion the two coincide). An absent usage block still leaves all three NULL, never 0.

§19.1's field list is amended with `output_tokens` and a paragraph stating the two-dimension
rule, so the column set and the spec do not disagree.

**D-F — A failed provider call is a FAILED ledger row, never a missing one.**
Both hops finalize **before** the `response.map_err(…)?` that turns a provider error into a
`PrivateReasoningError`. That early `?` is what would have swallowed the cost leg, and a
never-finalized RESERVED row is indistinguishable from a worker that died mid-call. Failure
records `status='FAILED'` + `error_class='PROVIDER_ERROR'` + latency, with no token columns
(the provider reported none) — the shape `complete_structured_timed` already decides for every
dispatch path.

**D-G — The consolidation hop's `model_call_id` still carries the disclosure id.**
`PrivateReasoningResult::model_call_id` flows out over the ADR-0012 UDS RPC into
`ops.private_inference_rpc_calls.response_model_call_id`, and that column is read as a
**disclosure** reference (`bins/consolidation-worker/tests/consolidation_hop_e2e.rs` joins it
to `ops.data_disclosures`). Card 20 asks for the ledger row *alongside* the §7.4 receipt, not
for the receipt to be re-pointed, so the ledger row is written and the RPC surface is left
alone. Re-pointing that column is a separate, cross-file change with its own e2e blast radius.

## Acceptance gates

- `crates/adapters/tests/model_call_ledger.rs::db_purpose_check_mirrors_the_rust_closed_set` —
  reads `pg_get_constraintdef` and compares the literal set against `ModelCallPurpose::ALL` in
  **both** directions, so drift on either side is red. It extracts the single-quoted literals
  rather than grepping for a syntax form, because PostgreSQL deparses `CHECK (col IN ('A','B'))`
  as `(col = ANY (ARRAY['A'::text, 'B'::text]))` — a test pinned to the authored form passes on
  the file and fails on the catalog. The form assertion accepts both.
- `…::purpose_outside_the_closed_set_is_rejected_by_the_database` — the DB is the enforcing
  side, not just the documenting one; without this the mirror test would still pass against a
  CHECK that had been dropped entirely.
- `…::private_purposes_reserve_and_finalize_on_the_private_worker_pool` — all three private
  purposes, on `role_private_worker`, one row each, purpose/model/tenant/tokens persisted, and
  every 0130 route column NULL (D-D).
- `…::a_failed_private_call_is_ledgered_as_failed_not_absent` — D-F.
- `humaux_domain::ledger::purpose_tests` — literals unique and round-tripping, unknown literal
  fails closed.
- The live `distill_hop_e2e` and `consolidation_hop_e2e` suites exercise the real hops with the
  real provider; the existing ledger, `ledger_append_only` and contribution suites pin that the
  0094/0130 guards are unchanged.

### Fault injection (红转绿)

Adding a purpose literal to 0166's CHECK without a `ModelCallPurpose` variant, or a variant
without the CHECK value, makes `db_purpose_check_mirrors_the_rust_closed_set` fail with the
set difference. Run live on 2026-09-14: with `'PRIVATE_NOT_IN_RUST'` added to the deployed
CHECK the test failed naming the exact difference (`left` had the extra literal, `right` did
not); restoring 0166's CHECK turned it green again (11/11). The deployed catalog text is
`CHECK (((purpose IS NULL) OR (purpose = ANY (ARRAY['query_rewrite'::text, …]))))` — the
`= ANY (ARRAY[` form, which is why the test must not pin `IN (`.

**Correction (review finding).** This ADR previously claimed "deleting either hop's
`finalize_private_call` call makes the corresponding assertion red". That was FALSE of every
assertion in the repo: `crates/adapters/tests/model_call_ledger.rs` calls the ledger API
directly and proves the API works, not that either hop calls it, and the only hop-level ledger
assertion was the NEGATIVE one on the manifest-mismatch path
(`consolidation_hop_e2e.rs::ledger_rows == 0`). Both ledger calls could be deleted from both
reasoners with the whole suite green. The positive assertions now exist, in the same shape
`contribution_reasoner.rs` already used (count the rows on the purpose):

* `distill_hop_e2e.rs` — `observe()` reads `ops.model_call_ledger` for this tenant on the
  distill purposes; the D1 assertions pin `rows == 1`, `status = SUCCEEDED`,
  `purpose = PRIVATE_DISTILL_TEXT`, a non-empty `model`, and `input_tokens`/`output_tokens > 0`,
  **alongside** the existing `disclosure_outcomes == ["SUCCESS"]`.
* `consolidation_hop_e2e.rs` — the same, on `purpose = 'PRIVATE_CONSOLIDATE'`, immediately after
  the disclosure-receipt block.

Deleting `reserve_private_call` from either reasoner fails to compile (the `reserved` binding
feeds finalize); deleting `finalize_private_call` leaves the row `RESERVED` and the
`status = SUCCEEDED` assertion red; deleting BOTH makes `rows == 1` read 0 and go red.

**D-G — One ticket projects EVERY memory its Evidence carries (card 9's folded debt).**
A `projection.stream_log` ticket binds an EVIDENCE, not a memory: `ops.outbox.evidence_id` is
the only link it has. One Evidence routinely carries N memories — `distill_repo::persist` writes
one `private.memory_evidence` row per extracted memory (`role='PRIMARY', ordinal 0`) against the
single Evidence `remember` issued the ticket for. `projection_worker::resolve_memory` resolved
`ORDER BY (role='PRIMARY') DESC, ordinal ASC LIMIT 1`, so **memories 2..N of every multi-output
distill were never indexed**, and a `MEMORY_LIFECYCLE` ticket (or a 0155 backfill ticket) aimed
at memory #2 silently re-projected memory #1 instead. Card 9 reported the lifecycle half; the
remember-time half is the same defect and is larger.

The fix is at the single point all three issuers already route through, not at each issuer and
not by inventing a memory-bound ticket shape (which would need an `ops.outbox` column and a
second ticket vocabulary): `resolve_memories` drops the `LIMIT 1` and returns every memory of
the bound Evidence, `resolve_and_embed` builds/seals a card per memory and embeds them in ONE
batched `embed_cards` call (N memories, one provider round trip), and `process_row` folds the
per-memory outcomes into the row's terminal:

* the first failure short-circuits the row to `FAILED` — §15.7 already requires one failed seq
  to block the prefix, and the retry is safe because every `finish_row` step is idempotent on a
  deterministic `point_id` (upsert / `AlreadyRegistered` / verify);
* §18.2 `ExcludedSecret` becomes **per memory**: an Evidence carrying one secret memory and one
  ordinary memory still indexes the ordinary one, and only an Evidence where nothing is
  indexable settles `SKIPPED_BY_POLICY`. `Unbuildable` stays a whole-row failure (§18.4: the
  stored record itself is malformed);
* one indexed memory makes the row `DONE`. Counts are unchanged — N memories is still one
  settled row, so A1/§15.4 arithmetic is untouched.

A short embedding batch (`vectors.len() != cards.len()`) is now itself a failure; before, a
provider returning fewer vectors than cards would have silently dropped the tail memories.
Pinned by `crates/adapters/tests/projection_worker.rs::
one_ticket_projects_every_memory_its_evidence_carries` (both halves on one chain: the
remember-time ticket indexes both memories, then a lifecycle ticket aimed at the sibling
re-projects the sibling in place). Restoring the `LIMIT 1` makes it red at the sibling's point
lookup — there is no registered point for it.

**D-H — `RETIRED_FAILED` joins A2's `skipped` term (§23.1②, amended).**
0167's read-side analysis covered the §15.4 prefix and the §15.5 overlay and stopped there. It
missed §23.1② A2 (`visible + deleted + skipped == done`): A1
(`done + open_gaps + pending == expected`) leaves a retired row nowhere but `done`, and the
retired record is by construction never in the index, so with no left-hand term claiming it
`lhs` sat one BELOW `done` **forever** — every later recall on a stream that ever had one
retirement abstained with `ProjectionInvisibleLoss`, i.e. an audited retirement read as a silent
loss, the one confusion §37.1's three-way table exists to prevent.

It goes on `skipped`, not `deleted` and not a seventh `LedgerCounts` field:
* `skipped` already means "settled, never indexable, not a deletion" — retirement is that,
  literally;
* `deleted` is also §37's deletion-propagation counter: it leaves the denominator and §41.2's
  `tombstoned_unpurged_over_sla` chases it to a physical purge. A retired row has no bytes to
  purge, and §37.2 freezes `deleted = count(state = 'TOMBSTONED')` as the single definition;
* a seventh field is frozen out by §23.1②'s own architecture-check (`LedgerCounts` field set is
  exactly 6).

Recorded cost: the retired record stays in the denominator, so that stream's
`completeness_ratio` is permanently below 1.0 while `current` stays true — "there is one I can
never serve, and I know which, who retired it and what failed". That is the same reading
`SKIPPED_BY_POLICY` has always had. One SQL filter changed (`stream_repo::close_ledger_in_txn`);
`stream_repo::fetch_ledger_closure` is a new public read-only entry point so the test exercises
the REAL counting query instead of a fixture's copy of it — a fixture copy is how this got past
every existing envelope test. Pinned by `crates/adapters/tests/stream_repo.rs::
a_retired_ticket_keeps_the_a2_closure_shut`.

**D-I — §16.3 is amended, not just ADR'd.**
The first-activation exemption (criteria ① and ③ have no second operand when the family has no
serving row) was implemented in `projection::serving::evaluate_switch` and recorded only in
ADRs, while Baseline §16.3 still read "三条全真才允许切换…不存在『人工判断可以上』的分支". §16.3
now carries the exemption with its reasoning, its limit (a proven `FAIL` still refuses a first
activation — a criterion no input can falsify is not a criterion, §80.1), its non-applicability
off the first-activation path, and the measured cost of not having it (soak25/26/27: every fresh
tenant's first promotion refused forever ⇒ `no_serving_projection` ⇒ `DEPENDENCY_UNAVAILABLE`).
`xtask projection-serve --continuation`'s default (`cannot_establish`, not `pass`) is consistent
with that text: it refuses a non-first promotion, and it stops the CLI attesting to a §69 gate
run that did not happen.

## Speed (measured)

Measured against the dev PostgreSQL 18 (`humaux-thread-pg`) as `role_private_worker`, after 20
discarded warm-up iterations, n = 100:

| operation | p50 | p95 | n | unit |
|---|---|---|---|---|
| ledger reserve (INSERT, statement time) | 0.072 | 0.112 | 100 | ms |
| ledger finalize (UPDATE, statement time) | 0.063 | 0.068 | 100 | ms |
| full bracket: reserve txn + finalize txn, both commits included | 0.475 | 0.763 | 100 | ms |

The bracket is what a distill or consolidation call now pays on top of a provider round trip
measured in seconds — sub-millisecond, two extra commits. Cost visibility is not on the
latency budget.

The review pass (D-G/D-H, 2026-09-15) adds **no round trip anywhere**: `output_tokens` rides the
existing finalize UPDATE, `resolve_memories` is the same single query with the `LIMIT 1` removed
(N rows instead of 1), the N cards go out in ONE `embed_cards` call rather than N, and the A2
`skipped` term is the same aggregate with a two-value `IN` instead of `=`. Live, warm, on this
host: one distill hop end to end (seed → live MiniMax → memories → projection → ticket DONE)
7.16 s wall for the whole test, of which the ledger bracket is the 0.475 ms p50 above; the
consolidation hop suite (7 tests, one live provider call) 3.76 s wall. The whole
`humaux-adapters` integration set (71 binaries, 440 tests, PostgreSQL + Qdrant live) runs in
~40 s once the binaries are XProtect-assessed — the multi-memory fan-out is not measurable
against it.

## Known limits / not in this change

- All three folded debts listed on card 20 have landed (the audited `FAILED → RETIRED_FAILED`
  retirement, the soak promote-loop candidate set together with the §16.3 benchmark criterion at
  a first activation, and — in the review pass, see D-G — the card-9 one-Evidence→one-memory
  ticket resolution).
- `bins/private-worker/tests/distill_hop_e2e.rs::d1_live_distill_writes_memories_and_projection_resolves_ticket`
  is live-MiniMax and is not deterministic: it failed twice (21:55, 22:12) with the strict
  `parse_distill_output` rejecting the model's reply (`InvalidInput`), then passed at 23:32 on
  the identical binary. The seven fake-provider distill tests exercise the whole
  `DistillReasoner::infer` path including the new ledger bracket and were green throughout, so
  the flake is the model's output, not the cost leg. A card that wants this test to be a gate
  needs a retry policy or a recorded reply; naming it here so the next red is read correctly.
- `ops.tenant_cost_events` gets no row for a private call. `record_external_model_cost_event`
  takes a `cost` it does not compute, and D-E is why there is none to pass. When a pricing row
  for the private reasoning models exists, the cost event follows from the same finalize.


---

# Addendum — card 20's folded debts (2026-09-14)

Three debts card 18 and card 20 named but could not reach from their own allowed file sets. All
three are one subject: the §16.2 promote loop could not promote, and the soak said so with numbers
that were partly about the harness.

## D-K. `RETIRED_FAILED` — the audited retirement of an exhausted `FAILED` ticket

**Decision (migration `0167_retired_failed_ticket`).** A settled `FAILED` row is terminal and
∉ `SETTLED_OK`, so §15.4's contiguous prefix — which is also §15.5's RYW overlay lower bound —
stops there for the life of the stream. The tenant never satisfies §16.3 criterion ②, never gets a
serving projection, and answers `no_serving_projection` for every later recall (soak21: 27/27).

Option (a), "count `FAILED` as settled", stays **rejected** (stored as `rejected`, card 18):
`Baseline_2.9.md`'s §15.7 worked example freezes the opposite, and skipping the seq silently drops
a never-indexed record out of reads while the envelope still claims completeness. Option (b),
taken here and checked against Kafka Connect / Debezium `errors.tolerance=all` + DLQ: move the
record out into a NEW terminal state, leave the offset arithmetic alone.

* New state `RETIRED_FAILED` ∈ `TERMINAL` ∩ `SETTLED_OK` (§15.2 / §15.2.1 amendment). Not a reuse
  of `SKIPPED_BY_POLICY`, which means §18.2 policy exclusion and is separately reported in §15.6 /
  §23.1② / the soak's `skipped_by_policy_rate`.
* One door: `projection.retire_failed_ticket(uuid, text, uuid, text, text, text, bigint, text)`,
  owner SECURITY DEFINER, `search_path=pg_catalog`, EXECUTE to `role_maintenance` only, PUBLIC
  revoked. The 0011 transition trigger gains exactly one arm — `FAILED -> RETIRED_FAILED` when
  `current_user` is the owner, which inside a definer function it is and nowhere else can be
  (0164's non-GUC argument, verbatim).
* **Who may call it, with the evidence.** `role_retrieval_worker` writes `FAILED` on the FIRST
  error of a pass with no retry budget spent — sixteen `error_class` values reach
  `projection_worker::settle_row` and most are transient infrastructure (`qdrant_upsert_failed`,
  `embedding_failed`, `db_commit_failed`, `registry_failed`, …), so `FAILED` is not evidence that
  retries are exhausted and the role that writes it must not be the role that settles it OK. The
  worker that really exhausts retries is `humaux-private-worker`
  (`distill.rs`: `job.attempt >= dispatch.max_attempts ⇒ DerivedWorkOutcome::Dead`), but at park
  time the ticket is still `ISSUED` — there is nothing to retire. So EXECUTE goes to
  `role_maintenance` alone, the §6.3 admin path that already owns the §15.2 `ISSUED -> LOST`
  patrol. A grant no caller can use is attack surface, not a caller.
* **Audit**: `retired_by` (`session_user`, written inside the function), `retired_at`, and the
  preserved `error_class`, bound by an all-or-nothing CHECK. The caller must NAME the class it is
  retiring; a mismatch retires zero rows.
* `projection.processing_gaps` is untouched: §15.2's frozen view text is
  `state IN ('FAILED','LOST')` and a retired row's state is no longer `FAILED`. Adding a second
  place that excludes the new state would be a second definition to keep in sync.
* **Read-side cost, recorded rather than glossed**: the retired seq enters the contiguous prefix,
  the overlay's lower bound moves past it, and the record — never indexed — is permanently
  invisible to recall. That is what retirement means; it is why the door is EXECUTE-gated,
  class-named and audited. Asserted by
  `retrieve_read_your_writes.rs::retired_seq_leaves_the_overlay_and_enters_the_contiguous_prefix`.
* Both copies of the §15.4 formula move together and cannot drift again: they now interpolate one
  `retrieve::SETTLED_OK_SQL_LIST`, pinned against `ProcessingState::is_settled_ok` by a unit test.

## D-L. §16.3 criterion ③ at a first activation

`evaluate_switch` required `ContinuationVerdict::Pass`. On this deployment `continuation_198_v2`
is `NOT_DECLARED` (§69) and the gate can only output `cannot_establish` until `frozen_by` is
written, so a fresh tenant's FIRST promotion was unreachable by construction — and criterion ③ is
a comparison against `benchmark(serving)`, which a family with no serving version does not have.
That is the same missing second operand ADR-0017 already exempted criterion ① for. **Fixed at the
first activation only**: `Fail` still refuses there (the injection that keeps the branch
observable — a criterion no input can fail is not a criterion, §80.1), and off that path the gate
still requires `Pass`, so `Inconclusive` / `CannotEstablish` keep refusing exactly as §69 and the
card that pinned them required.

**The contradiction card 20 asked to reconcile** — soak25/26/27 tallying `BenchmarkNotPass` 2 at a
first promotion that had in fact succeeded — was two different inputs to one evaluator:
`xtask::soak::switch_rejections` graded every candidate with the honest `CannotEstablish`, while
`xtask projection-serve` defaulted `--continuation` to `Pass`. The default is now
`cannot_establish`; an operator who really ran the gate says so on the command line.

## D-M. The promote loop's candidate set

`promote_rejections` offered every `stream_checkpoints` row to §16.3, including each family's own
serving row, and `rehearse.sh`'s loop re-ran `projection-serve --version v1` every 5 s after
activation — so `VisibleSameVersionDeclared` (comparing a count with itself, the 恒真闸 shape
§16.3 rejects on the version tag alone) was a permanent entry describing the harness. Fixed in
both places: the query filters `WHERE NOT c.serving`, and `projection-serve` itself now
short-circuits when the requested version is already serving.

**Which of the card's two options, and why.** Manufacturing a real second projection version for
the soak to grade would measure a synthetic backfill, not the deployment: criterion ① would
compare `visible(v2) = 0` against a populated `visible(v1)` unless the whole corpus were
re-projected under v2, and criterion ③ cannot admit a non-first activation at all while §69's set
is `NOT_DECLARED` — an "admitted v2" would have to be bought with a `--continuation pass` nobody
can honestly declare. So "no candidate pending" is graded explicitly instead, as its own
assertion `promote_candidates_admitted`, whose denominator is the checkpoint rows **scanned**
(`n = 0` ⇒ `FAIL-VACUOUS` per ADR-0038: the run never looked) and whose
`detail.candidates_pending` states the candidate count out loud rather than leaving it to be
inferred from an empty map. `projection_promoted` keeps its own real witness — streams whose
§15.4 prefix moved, `n` = streams — and its `expected_red_until` marker is gone: it is REQUIRED,
and D-K is what makes it reachable.
