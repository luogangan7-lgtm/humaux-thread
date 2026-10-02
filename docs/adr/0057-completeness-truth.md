# ADR-0057 — Completeness truth: A2 counts memory points on both sides, correct retires M1, lifecycle tickets go home, one count filter per audience

- Status: Accepted (card 31, 2026-10-01/02, on HEAD `2c8eba7` = card 30b). Slice 1 implements D-A,
  D-B, D-C, D-J, D-L, D-M; slice 2 implements D-D, D-E, D-F, D-K (ProjectionLag as a `classify()`
  input; closes ADR-0047's `classify()` parameter-count debt); slice 3 implements D-I (the Qdrant
  write fence), records D-G (PINNED floor, ruling C) and D-H (debt 3 filed to card 35), lands the
  seven Baseline sentence-level amendments of escalation E2 (§17.1, §16.3 ①, §22.4, §22.5, §23.1②,
  §23.3, §59 I3) and the rehearsal assertions.
- Amends: ADR-0040 D-J (the switch's USER_PRIVATE refusal is superseded, D-C); ADR-0040's "one
  producer" rule (now one producer per audience over one private count body, D-C); ADR-0054 D-A's
  sentence "the lifecycle ticket lands on the stream whose ledger that pair's reads consult" (D-M,
  amendment note added to ADR-0054). Records, does not change: Baseline §25.4.B(6) (D-G).
- Spec: Baseline §23.1② (A1/A2), §22.4, §22.5, §52.2, §78.1, §16.2/§16.3, §17.1, §17.4, §6.1,
  §25.4.B(6), §59 I3; ADR-0047 (classify arity); ADR-0049 (retire path); ADR-0052 D-D (lease fence);
  design `card_31_design.md` §1 (the proof sketch and the per-transition table live there).

## Context

Facts read on `2c8eba7` before any change (file:line at that commit):

- `done` counts settled tickets of every kind (`stream_repo.rs:149`, the `SETTLED_OK` aggregate);
  `visible` counts Qdrant points (`retrieve.rs:1168/1231`); `judge_a2` (`envelope.rs:120-133`)
  compared `visible + deleted + skipped` with `done`. One EVIDENCE_ACCEPTED ticket projects N memories
  (`projection_worker.rs:494-506`), and every supersede / restore / archive / correct issues one more
  MEMORY_LIFECYCLE ticket (`memory_governance_repo.rs:276/305/829/937/1505`) that moves 0, +1 or −1
  points. Rehearsal 5 `recall_after_restore` read `done=6 visible=4` (0.667, `PROJECTION_INVISIBLE_LOSS`,
  `current=false`) in all five runs; rehearsal 6 `recall_after_kill9` read `done=8 visible=4`. The
  rehearsal gate never checked either signal.
- `visible` was counted tenant-wide (`retrieve::visible_count_of_version` → `VisibleCountFilter::new`,
  `qdrant.rs:596-640`) against a one-workspace ledger: the card-30 two-workspace fixture read
  `visible=29` against `done=6` (`a2_overshoot_beyond_pending`).
- `correct_atomically` issued one ticket on E2 (`memory_governance_repo.rs:1516-1546`), so M1's point
  stayed in Qdrant (ADR-0055 known limit); undoing a correction deactivated M2 with no ticket
  (`:758-771`); a lifecycle ticket requested from another workspace landed on the request's stream,
  whose family never held the point (gateway `memory.rs:327-330`, family-scoped ids at
  `projection_worker.rs:755/940/974-981`).
- Lag had no input: `completeness.rs:99-104` said so, `classify()` (`:676-716`) took five inputs, and
  `ProjectionLag` existed only in test stubs. With the runner stalled, `current` stayed `true` and
  `degradations=[]`.
- The §16.2 switch refused (`VisibleUnavailable`) any tenant with a live USER_PRIVATE point because the
  ops `AuthorizationScope` cannot express "every USER_PRIVATE point of a tenant"
  (`switch_visible.rs:60-235`).
- `fetch_pinned_in_txn` borrows the `ProjectConstraint` floor from `explicit_mandatory_bindings_v1`
  (`context_repo.rs:2131-2150`) while `project_active_constraints_v1` claims every active
  `ProjectConstraint` row, so every legal PINNED row lands in `pinned_excluded` — the outcome Baseline
  §25.4.B(6) (:6578-6595) already records.
- The Qdrant upsert (`projection_worker.rs:984`) and the retire delete (`:1093`) carried no fence;
  only the PG settle was fenced on `(lease_owner, attempts)` (ADR-0052 D-D). A reclaimed worker could
  overwrite or delete a point after a later ticket of the same memory had settled.

## Decisions

**D-A — A2 in the point unit.** `LedgerCounts` keeps its six ticket fields (A1:
`done + open_gaps + pending == expected`). `LedgerClosure` additionally carries
`ProjectionReads { points_expected U, points_settled L, points_in_flight F, points_unsettled Q }`,
read in the ledger's snapshot. Judge (`envelope::judge_a2`, private, one caller):
`visible < L` → loss (`PROJECTION_INVISIBLE_LOSS`, ratio kept); `L ≤ visible ≤ L+Q` → closed;
`≤ L+Q+F` → in flight; beyond → cannot_establish (label `a2_overshoot_beyond_pending` kept for wire
stability). `completeness_ratio = visible / U` (1.0 when U = 0). The four readings are on the wire
(`projection` block, `context.output.schema.json`). Q is the price of audited retirements and
failures (0 or 1 point per memory, W5); card 35's reissue drains it.

**D-L — Migration 0189 `projection.stream_point_ledger`.** Owner SECURITY DEFINER, STABLE,
`search_path=pg_catalog`, EXECUTE `role_gateway` + `role_retrieval_worker`. It asserts the tenant GUC,
applies the caller's §6.1 visibility (user from the GUC only; the workspace array only narrows,
membership re-checked) — the same predicate the Qdrant count applies — and skips only the 0155
subject term, which Qdrant cannot mirror. Counts only.

**D-B — `correct` retires M1.** `correct_atomically` issues a second lifecycle ticket on E1 (M1's
PRIMARY) before its arbiter UPDATE, so the ADR-0049 retire path deletes M1's point.
`restore_atomically`'s UserCorrection branch tickets M2 before deactivating it. Every
`SET status =` writer in `memory_governance_repo` is paired with a ticket (gate
`w3_status_writers_ticketed`).

**D-B addendum — a corrected memory is projectable (found by the slice-3 rehearsal).**
`memory.correct` stores the user's text verbatim as a JSON string (`mcp_application.rs:917`,
`Value::String(text)`), but `projection_worker::card_input` read `key_claim` / `evidence_excerpt`
only from an object, so `build_card` answered `Unbuildable` and **every correction made through the
gateway** settled its E2 ticket `FAILED card_unbuildable`: the corrected memory never reached the
index, the stream gained an open gap (`current=false` for every later read) and Q = 1. First
rehearsal on the isolated DB: `a2 correct: visible=3 L=3 Q=1 U=4 current=False`, ticket 9
`FAILED|card_unbuildable`. Pre-existing since ADR-0025; the integration tests missed it because the
test helper stored an object. Fix at the one mapping: a bare JSON string is the whole statement, so
it is the claim and the title (`card_input`); the test helper now stores the gateway's exact shape,
so the five correct tests exercise production's content.

**D-M — Lifecycle tickets go to the target's home stream.** `home_stream` derives the workspace
stream of the target Evidence's first ticket (keeping the request's tenant / domain / kind /
version); a home other than the request stream must already be provisioned, else
`DependencyUnavailable`. `correct` homes E2 and M2 with M1. Consistency tokens name the stream the
ticket landed on. The request workspace still authorizes the write (ADR-0054). Every ticket bound to
an existing memory's Evidence is routed this way, not only the governance ops: `memory.annotate_affect`
(`affect_repo::annotate`, whose wire `stream_seq` is therefore a seq of the home stream) and the
consolidation rollup's `MEMORY_PUBLISHED` ticket (`consolidate_repo::publish_rollup`, a source memory's
PRIMARY Evidence, previously the run's scope — possibly `('tenant', tenant_id)`) both call the one
`memory_governance_repo::home_stream` (gate `one_home_stream`). A ticket on a fresh Evidence
(`remember`, distill confirm) creates the home and needs no routing. Without it (review P0) a W2
annotate wrote a duplicate point into W2's family that a later home-stream supersede never retires:
W2 then reads overshoot for good, and a real W2 loss cancels against it to Closed.

**D-C — Two count audiences.** Read routes: `VisibleCountFilter::family_probe` (§17.1 tenant +
caller visibility, narrowed by `workspace_id` and `projection_version`); the tenant-wide
`VisibleCountFilter::new` is deleted. Ops (serve switch, soak, ADR-0053 provisioning probe):
`projection::dense::StreamCountFilter` from `build_stream_count_filter(tenant, workspace, version)` —
every visibility class, no conversion into `DenseQueryFilter` (compile-fail test), reachable only
from `adapters::{qdrant, retrieve, provisioning}` (gate `stream_count_off_request_path`). It returns a
number, never ids or bodies; this is §17.1's count-only exception. `switch_visible::ops_scope`, the
USER_PRIVATE probe and `user_private_points` are deleted.

**D-J — One test per transition through the real ops.** `projection_worker.rs` drives supersede,
restore, archive, unarchive and correct through `memory_governance_repo`; the raw-SQL flag matrix is
gone. `a2_point_identity.rs` checks the identity after each op, a 1→3 fan-out, the two-workspace
case, cross-workspace governance, other users' private points, subject-restricted memories and three
retired fixtures, both sides read by the production producers.

**D-D — Projection-lag reading.** `close_ledger_in_txn`'s deleted/skipped query gains one column:
the age in whole seconds (`now()` = the snapshot's transaction start, so DB clock and same instant as
the counts) of the stream's oldest `ISSUED`/`PROCESSING`/`RETRY_WAIT` ticket, `NULL` when nothing is
pending. `WAITING_KEY` is excluded (it waits for a key, not for the runner; DOD-014 names it as its
own signal); backing-off `ISSUED` rows are included (a write retrying past the threshold is not
reflected). No extra round trip, no cache. It rides in `ProjectionReads::oldest_pending_age_secs`;
`LedgerClosure::lagging(threshold)` (strict `>`) is the only comparison. Unit: seconds — `pending`
already carries the event count, and §42's `projection_lag_events` stays the deployment-wide gauge.

**D-E — `classify()` sixth input.** `classify(planner, lane_status, census, ledger,
mandatory_missing, lag_threshold)`; arm order A1 → lag → census → lane → mandatory → planner; new
reason `CannotEstablishReason::ProjectionLag` (label `projection_lag`, the 13th, also in
`context.output.schema.json`). `build_projection_block(ledger, visible, lag_threshold)` degrades
`PROJECTION_LAG` (the existing `DegradeCode`, none added) on every path while lagging, composed after
the A2 loss with `Outcome::also` (telemetry::degrade, which routes each extra code through
`abstain()`), so a stalled runner plus a lost point reports `[PROJECTION_INVISIBLE_LOSS,
PROJECTION_LAG]`. `current` keeps its frozen formula. `CompletenessInputs.lag_threshold` carries the
threshold to the one real `classify()` call; `classify_for_witness` takes it as a parameter. This
closes ADR-0047's debt by making Baseline §22.5's block equal the code (six inputs).

**D-F — Config key.** `HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS`, declared in the gateway's
`bootstrap::registry()` as `("PROJECTION_LAG_SECONDS", "u64", false)` with no default (absent ⇒
boot-fatal naming the key; zero refused), parsed into `ContextBootstrap::projection_lag`, which every
read route (recall, context, memory get/enumerate) already receives. Card 35's sweep SLA must stay a
separate, larger key, or stalled tickets turn LOST before they read as lag.

**D-K — Metrics registry: no change.** §41.2 already reserves `projection_lag_events`; card 34
owns export (emit + witness together), so this card emits nothing new.

**D-G — PINNED floor: ruling C (the spec as written).** Baseline §25.4.B(6) already records that
the `ProjectConstraint` floor leaves no PINNED row outside Mandatory, accepts it as "not a capability
reduction", and sends deterministic delivery of a below-floor memory to the v2 task-authorization
path (audit, content version, revocation); :6623 names the threat a lower floor reopens (one induced
`memory.pin` would inject low-origin content into every Context, bypassing §10.1). So: no floor
change, no fixture flips (`g80_31_handoff.rs:684`, `mcp_gateway.rs:8848` stay as item 6 left them),
no `MandatoryRow::from_pinned`. A pin at or above the floor is delivered through Mandatory and counted
in `pinned_excluded`; a below-floor pin is counted there and not returned. The card's line
"memory.pin then context.assemble delivers the pinned memory" holds for the first and is struck for
the second (main-line ruling E1, 2026-10-01). `context_repo.rs:2148` carries the citation.

**D-H — Debt 3 (an unarchive ticket that ends FAILED leaves the point `archived=true`): filed to card
35.** D-A counts existence, not payload flags, and D-B only retires dead memories, so nothing here
repairs it. Card 35's sweep reissues one fresh MEMORY_LIFECYCLE ticket on the PRIMARY evidence, in
the home stream (D-M), for every memory whose latest ticket is FAILED, LOST or RETIRED_FAILED (every
memory in `points_unsettled`, any ticket kind) — never a state rewind (the guard admits only ISSUED →
{DONE, SKIPPED_BY_POLICY, FAILED}). Classes that reach FAILED (ADR-0052 D-E): `transient_exhausted`,
`qdrant_upsert_rejected`, `qdrant_delete_rejected`, `registry_conflict`, `registry_failed`,
`embedding_rejected`, `embedding_dimension_mismatch`, `card_unbuildable`, `secret_scan_rejected`; the
three deterministic ones are reissued at most once per retirement. Acceptance for card 35:
`points_unsettled == 0` after one sweep on the three retired fixtures of `a2_point_identity`. A later
lifecycle ticket for the same memory already repairs the flag (it re-reads PG), so the exposure is
"FAILED and no later ticket".

**D-I — The worker's Qdrant writes are fenced on `source_stream_seq`.** The settle fence does not
suffice: worker A resolves a memory, stalls past its lease, worker B settles a later ticket of the
same memory, and A's write then lands (its settle is rejected, its write is not). The payload already
carries a monotone per-stream version, `source_stream_seq`; seqs are allocated under the checkpoint
row lock (`remember::issue_stream_log_row`) and every ticket of a memory goes to its home stream
(D-M), so a higher stored seq is always a later ticket of the same memory.
- Upsert: `qdrant::upsert_fenced` (one point, the worker's only write call, `projection_worker.rs:987`)
  sends Qdrant's `update_filter` = `should [is_empty(source_stream_seq), source_stream_seq lte mine]`.
  `lte`, not `lt`: a retry of the same ticket re-applies idempotently. An absent point is inserted.
  Measured by the main line on the dev server 1.19.0: absent ⇒ inserted; stored 5 vs incoming 3 ⇒ not
  written; stored 5 vs incoming 9 ⇒ written; without the filter the stale write overwrites. The
  refusal is silent (`status: completed`), so the test reads the point back.
- Delete: the retire delete (`retire_row`) and the registry-refusal compensation delete in
  `finish_row` both go through `qdrant::delete_points`, which now deletes by filter `must [has_id,
  should [is_empty, lte fence_seq]]` with the deleting ticket's seq. The retire delete is a separate
  call site from the upsert, but the same adapter function serves both worker deletes, so the mirror
  fence is in place rather than left to A2. The compensation delete needed it most: if the upsert was
  silently refused because a later ticket owns the point, an unfenced compensation would delete that
  later ticket's live point.
- The unfenced `qdrant::upsert` stays for test fixtures; gate `worker_writes_fenced` pins that the
  worker calls only `upsert_fenced`.
- Deploy requirement: **Qdrant server ≥ 1.19.0** (runbook §4); card 39 pins the image.

## Per-transition identity (numbers)

After the worker settles (F = 0), on a healthy stream (Q = 0) the identity is `visible == L`; the
ticket count `done` moves independently (A1's unit). Numbers are the `a2 …` lines that
`crates/adapters/tests/a2_point_identity.rs` prints (real PG + Qdrant, real ops, real worker,
2026-10-02, 19/19 pass); the fixture seeds 2 memories on 2 tickets unless stated.

| transition | tickets (A1) | L | visible | F | Q | U | before ADR-0057 (ticket unit) | measured after |
|---|---|---|---|---|---|---|---|---|
| seed (2 memories) | 2 | 2 | 2 | 0 | 0 | 2 | closed | `visible=2 L=2 done=2` |
| supersede M1→M2 | +1 | −1 | −1 (retire_row) | 0 | 0 | −1 | `1 < 3` ⇒ loss | `visible=1 L=1 U=1 done=3` |
| restore M1 | +1 | +1 | +1 (same id revived) | 0 | 0 | +1 | `2 < 4` ⇒ loss | `visible=2 L=2 done=4` |
| archive | +1 | 0 | 0 (re-upsert same id) | 0 | 0 | 0 | `2 < 3` ⇒ loss | `visible=2 L=2 done=3` |
| unarchive | +1 | 0 | 0 | 0 | 0 | 0 | `2 < 4` ⇒ loss | `visible=2 L=2 done=4` |
| correct M1→M2 (two tickets, D-B) | +2 | +1 −1 | +1 −1 | 0 | 0 | 0 | `2 < 4` ⇒ loss | `visible=2 L=2 done=4` |
| restore after correct (undo, D-B) | +2 | +1 −1 | +1 −1 | 0 | 0 | 0 | `2 < 6` ⇒ loss | `visible=2 L=2 done=6` |
| 1 Evidence → 3 memories | 1 | 3 | 3 | 0 | 0 | 3 | `3 > 1` beyond pending ⇒ cannot_establish | `visible=3 L=3 done=1` |
| mid-flight ticket (1 of 3 claimed, lease expired) | 1 pending | 0 | 3 | 3 | 0 | 3 | — | in flight `visible=3 L=0 F=3`, then `visible=3 L=3 done=2` |
| two workspaces W1 (6) / W2 (23) | 1 / 1 | 6 / 23 | 6 / 23 | 0 | 0 | 6 / 23 | W1 read `visible=29` ⇒ cannot_establish | `w1 visible=6 L=6`, `w2 visible=23 L=23` |
| supersede + correct from W2 on W1 memories (D-M) | +4 in W1, 0 in W2 | W1 2 | W1 2, W2 0 | 0 | 0 | 2 / 0 | W1 overshoot | `home_w1 visible=2 L=2 done=6`, `request_w2 visible=0 L=0 done=0` |
| retired fan-out (1 of 3 points written) | 1 | 0 | 1 | 0 | 3 | 3 | — | `visible=1 L=0 Q=3` ⇒ closed |
| retired supersede, delete rejected | 3 | 1 | 2 | 0 | 1 | 1 | — | `visible=2 L=1 Q=1` ⇒ closed |
| retired restore after DONE, upsert rejected | 4 | 1 | 1 | 0 | 1 | 2 | — | `visible=1 L=1 Q=1` ⇒ closed |
| point deleted behind the ledger (G23-2 injection 1) | 1 | 3 | 2 | 0 | 0 | 3 | — | `visible=2 L=3` ⇒ `PROJECTION_INVISIBLE_LOSS` |
| kill -9 mid-ticket (rehearsal) | ticket stays ISSUED until re-claim | — | — | memories of the evidence | — | — | rehearsal 6 read `done=8 visible=4` ⇒ loss | rehearsal line `a2_after_kill9_drained` (§Rehearsal) |

## Tests (each with the single fault that turns it red)

| test | fault that reds it |
|---|---|
| retrieval `a2_closes_in_points_after_lifecycle_tickets` | judge `visible + deleted + skipped` against `done` again |
| retrieval `a2_fan_out_one_evidence_three_memories_is_closed` | the same ticket-unit judge (⇒ Inconsistent) |
| retrieval `a2_visible_below_points_settled_is_loss_with_ratio` | route `<` to cannot_establish |
| retrieval `a2_overshoot_within_points_in_flight_is_in_flight` | use ticket `pending` as the slack |
| retrieval `a2_overshoot_beyond_points_in_flight_is_cannot_establish` | treat every overshoot as in flight |
| retrieval `a2_points_unsettled_is_closed_slack_not_in_flight` | drop Q from the judge |
| retrieval `completeness_ratio_is_visible_over_points_expected` | denominator `expected - deleted` |
| retrieval `classify_lag_beyond_threshold_is_cannot_establish_projection_lag` | delete the lag arm |
| retrieval `classify_ledger_not_closed_outranks_lag` | lag arm before A1 |
| retrieval `classify_lag_at_or_below_threshold_is_unchanged` | `>=` instead of `>` |
| retrieval `build_projection_block_degrades_projection_lag_via_abstain` | remove `.also(ProjectionLag)` |
| retrieval `build_projection_block_carries_loss_and_lag_together` | early return after either code |
| retrieval reason/cell exhaustiveness (`REASONS` = 14 labels incl. `projection_lag`) | drop `projection_lag` from `REASONS` |
| projection `stream_count_filter_counts_every_visibility_class_of_the_stream` | AND the visibility disjunction into the builder |
| projection `stream_count_filter_drops_other_workspace_and_other_version` | drop the workspace term |
| projection `stream_count_filter_refuses_an_empty_version` | remove the empty-string check |
| telemetry `also_counts_every_code_through_abstain` | `also` pushes without `abstain()` |
| adapters `a2_point_identity::identity_holds_after_{supersede,restore,archive,unarchive,correct}` | `build_projection_block` judges on ticket `done` |
| adapters `a2_point_identity::one_evidence_three_memories_is_closed` | ticket-unit judge |
| adapters `a2_point_identity::correct_retires_the_corrected_versions_point` | remove correct's E1 `issue_lifecycle_ticket` |
| adapters `a2_point_identity::identity_holds_after_correct_then_restore` | remove restore's M2 ticket |
| adapters `a2_point_identity::mid_flight_ticket_reads_in_flight_then_closed` | definer drops the pending filter |
| adapters `a2_point_identity::two_workspaces_count_only_their_own_stream` | drop the workspace term from `family_probe` |
| adapters `a2_point_identity::supersede_from_another_workspace_keeps_both_streams_closed` | `home_stream` returns the request stream |
| adapters `a2_point_identity::archive_from_another_workspace_adds_no_point_to_it` | same HEAD routing |
| adapters `a2_point_identity::annotate_from_another_workspace_writes_no_point_into_it` | `affect_repo::annotate` issues on the request stream (W2 stream count 1) |
| consolidation-worker `consolidation_hop_e2e::t8_rollup_ticket_goes_to_the_source_memorys_home_stream` | `publish_rollup` issues on the run's key (ticket on `('tenant', tenant_id)`) |
| adapters `a2_point_identity::other_users_private_points_close_the_identity` | definer drops `vis(m)` |
| adapters `a2_point_identity::subject_restricted_memory_closes_the_identity` | definer `SECURITY INVOKER` |
| adapters `a2_point_identity::definer_ignores_a_foreign_user_and_a_non_member_workspace` | drop the membership `EXISTS` |
| adapters `a2_point_identity::a_point_deleted_behind_the_ledger_is_invisible_loss` | take `visible` from the registry |
| adapters `a2_point_identity::retired_{fan_out_partial,supersede_delete_rejected,restore_failed_after_done}_is_closed` | (a)(b) definer leaves RETIRED_FAILED out of Q; (c) L uses "some DONE" |
| adapters `projection_lag::a_pending_ticket_older_than_the_threshold_lags` | remove the age column |
| adapters `projection_lag::a_waiting_key_ticket_never_lags` | add WAITING_KEY to the age filter |
| adapters `projection_lag::settling_the_ticket_clears_lag` | age over every state |
| adapters `switch_user_private::a_tenant_with_user_private_memories_can_switch` | the shared ops producer (`retrieve::stream_count_of_version`) returns `None` whenever a USER_PRIVATE point exists |
| xtask `switch_visible::tests::projection_serve_promotes_a_stream_holding_user_private_memories` (the real `projection-serve` entry: `visible_inputs` → `read_candidate_facts` → `visible_pair` → `switch_projection_version`, first activation; another user's live USER_PRIVATE memory + registry row + point) | re-add the USER_PRIVATE refusal on the ops path — measured red: the ADR-0040 D-J `user_id=None` scope count in `VisibleFace::count` (pair `(Some(v1, 1), None)`), and a probe in `read_candidate_facts` for a live registry row whose memory `role_maintenance` cannot see (`Ok(None)` ⇒ no pair). Measured green, and why: the pre-card `USER_PRIVATE_PROBE_SQL` itself, re-added verbatim, never fires — under `role_maintenance` with only `humaux.tenant_id` set, 0155's visibility policy hides every USER_PRIVATE `memory_records` row, so the D-J refusal was dead and its live effect was the `user_id=None` undercount |
| adapters `switch_user_private::a_shadow_missing_a_user_private_point_is_refused` | count under a `user_id=None` scope |
| adapters `pool_typestate` ui `fail_stream_count_filter_in_dense_search` | `impl From<StreamCountFilter> for DenseQueryFilter` |
| adapters `projection_worker::{supersede,restore,archive,unarchive,correct}_through_the_real_op_*` | remove that op's `issue_lifecycle_ticket` |
| adapters `projection_worker::stale_worker_upsert_is_refused_by_seq_fence` | upsert without `update_filter` (measured red: stored seq 2, want 3) |
| adapters `projection_worker::stale_worker_delete_is_refused_by_seq_fence` | delete without the seq filter (measured red: the lower seq deleted the point) |
| adapters `stream_repo` closure tests | definer not called (all zeros) |
| adapters `projection_worker::correct_through_the_real_op_*`, `a2_point_identity::{identity_holds_after_correct, identity_holds_after_correct_then_restore, correct_retires_the_corrected_versions_point}` (helper stores the gateway's string body) | `card_input` reads only objects (measured red: 1 + 3 tests, ticket `failed: 1`) |
| gateway lib `bootstrap_without_projection_lag_seconds_fails_naming_the_key` | give the key a default |
| rehearsal `a2_after_*`, `correct_retired_m1_point`, `pinned_*`, `stall_*`, `resume_*` | see §Rehearsal |

Not built (design tests 33b and 34): the PINNED shape is witnessed live by the rehearsal's two
`pinned_*` lines and by `handoff.rs`'s overlap unit test; the four point fields are `required` in
`context.output.schema.json`, which every existing schema-validated `context.assemble` test already
enforces.

## Rehearsal (no soak, 2026-10-02)

`c31_rehearse.sh impl` with `SOAK_SECS=0` on an isolated database (`humaux_thread_c31`; the shared dev
database starves ready tenants' distill jobs behind 195 not-ready ones from leftover test tenants, see
delivery_point_report §6.17): `REHEARSAL VERDICT: 92 passed, 1 failed`.

| line | numbers |
|---|---|
| `a2_after_supersede` | `visible=3 L=3 F=0 Q=0 U=3 done=5 current=True` |
| `a2_after_restore` (recall_after_restore) | `visible=4 L=4 F=0 Q=0 U=4 done=6 current=True` (rehearsal 5: `done=6 visible=4`, loss) |
| `a2_after_archive` / `_unarchive` | `visible=4 L=4 … done=7` / `done=8`, `current=True` |
| `a2_after_correct` / `_correct_undo` | `visible=4 L=4 … done=10` / `done=12`, `current=True` |
| `a2_after_kill9_drained` | `visible=4 L=4 F=0 Q=0 U=4 done=17 current=True` (rehearsal 6: `done=8 visible=4`) |
| `correct_retired_m1_point` | registry live 0, Qdrant points 0, 1 registry row |
| `pinned_constraint_delivered_by_context_assemble` | mandatory 1, content 1, pinned lane 0, 2 live PINNED bindings |
| `pinned_below_floor_named_excluded` | `PrivateKnowledge`: mandatory 0, content 0, pinned lane 0; counts expected 2 / returned 0 / excluded 2 |
| `stall_yields_projection_lag_within_threshold` | threshold 20 s, observed 21 s, ticket `ISSUED`, no lease while the runner was stopped |
| `resume_clears_projection_lag` | cleared 0 s after SIGCONT, `current=True` |

The one red is `no_seq_scan_on_outbox_jobs_memory_evidence` (`4|1`). The planner chose a seq scan on
`ops.outbox` (227 rows in the fresh database) in the claim plan; the index catalog check passed.
The earlier run on the same database went red on the correct-path lines (`a2_after_correct`
`visible=3 L=3 Q=1 current=False`, ticket `FAILED card_unbuildable`), which is how the D-B addendum
was found.

## Known limits

1. A2 proves completeness of what the caller may see, not of the stream (§17.1); stream-wide
   completeness is the ops switch's job.
2. The definer is O(tickets of the stream) per read.
   `// ponytail` in 0189: upgrade to per-memory projection state on the registry when card-30 stage
   timing shows the ledger stage over budget.
3. Q's slack can absorb a real loss one-for-one on a stream with failed or retired tickets; Q is on
   the wire and card 35's reissue drains it.
4. Points written into another workspace's family by pre-D-M routing read as overshoot until a
   ticket in that family retires them.
5. Lag includes backoff: one poison ticket retrying past the threshold makes its whole family lag
   until it settles FAILED (`// ponytail` in `stream_repo`); tune with the key.
6. A stall shows as class + degradation, never as `current = false` (frozen formula).
7. The read-your-write terminal `ErrorCode::ProjectionLag` is not wired; no card owns it yet.
8. Spec/code arity drift for `classify()` has no automatic check; upgrade if it drifts again: an
   architecture-check rule comparing §22.5's block with `fn classify(`.
9. The seq fence has no tombstone: a stale upsert that arrives after a retire delete finds no point
   and inserts one (`is_empty` arm). A2 reads it as overshoot (`visible > L + Q + F` ⇒
   cannot_establish, never a false close) and the PG gate never serves the dead memory. Upgrade: a
   soft-delete payload (write `retired=true` at the retire seq instead of deleting) or card 37's
   generation fencing.
10. The registry write is not seq-fenced: a stale retire can still mark the binding
    `projection_live=false` while the fenced delete leaves the point. Upgrade: card 37's generation
    fence on `projection.private_memory_points`.
11. The fence relies on `update_filter` semantics measured on Qdrant 1.19.0; an older server may
    ignore the field and drop the fence silently. Control: the deploy requirement above; upgrade: a
    startup probe of the server version in the runner's `--readyz`.
12. Below-floor deterministic PINNED delivery does not exist (D-G); upgrade path: the v2
    task-authorization path of §25.4.B(6).
13. An unarchive ticket that ends FAILED leaves `archived=true` on the point until a later ticket or
    card 35's reissue (D-H).
14. The definer runs before the index count (the reverse of §23.1②'s read order); a tombstone
    committed between them reads as a one-off loss. Upgrade: read the overlay seq list inside the
    snapshot.
15. The worker takes `data_class` from the ticket's evidence, the definer from the memory's PRIMARY
    evidence (`projection_worker.rs:494-506`); a mismatch reads as loss, never as a false close.
    Main line: verify the worker side (possible §18.2 egress path) and file it.

## Rejected

- Card option (i), `done` = distinct EVIDENCE_ACCEPTED seqs: breaks A1 (shares `done`), cannot see
  fan-out, and the Qdrant side cannot be grouped by origin ticket (`source_stream_seq` is the last
  writer).
- Card option (ii), count lifecycle outcomes on the visible side: Qdrant has no record of a
  ticket's effect.
- Registry-only right side (`projection_live` count): the worker writes it, so "settled DONE without
  any upsert" is invisible to it.
- PG side through plain `role_gateway` RLS: the 0155 subject policy has no Qdrant mirror and would
  read subject-restricted rows as overshoot.
- A visibility-free stream count on the request path: crosses §17.1 for every request (kept only for
  ops, D-C).
- A 7th `LedgerCounts` field: frozen at 6 by §22.5 / §23.1② and an architecture-check.
- Lowering the PINNED floor (E1 alternate branch, `MandatoryRow::from_pinned`): reopens the §25.4.B
  threat :6623 names; the main line confirmed ruling C.
- A PG lease check right before the Qdrant upsert (D-I): narrows the window, does not close it, and
  costs one more query per ticket.
- Retiring a FAILED lifecycle ticket by deleting its evidence's points (debt 3, option a): would
  delete a live point on a retired archive/restore ticket and needs Qdrant in the maintenance path.
