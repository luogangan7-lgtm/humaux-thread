# Humaux Thread — delivery point report

- Date: 2026-09-26 · Card 24 · tree = the card-23 commit (`1bc38e8`) plus this card's change.
- Rule this document obeys: **every claim carries a `file:line`, a test name, or a log name.**
  A sentence with no anchor is not a claim in this report; it is prose, and it has been cut.
- Status of the claim: this is a **delivery-point report, not a "全链路完成" declaration.** §6
  (Limits) and §8 (What this run did NOT establish) are load-bearing parts of it.

---

## 1. Acceptance chain — what is proven, by which named test

Each row names the test that would go red if the capability broke. `n` is that test's own
sample size where it has one; a witness with a single assertion carries `n=1` honestly rather
than borrowing a soak's number.

### 1.1 Write (remember)

| Claim | Anchor | n |
|---|---|---|
| `remember.put` accepts, commits, and replays the committed receipt on ack loss | `bins/gateway/tests/mcp_gateway.rs:4281` `native_mcp_gateway_commit_ack_loss_is_unknown_and_replays_the_committed_receipt` | 1 |
| Per-call visibility and per-request write stream (cards 10/11) | `bins/gateway/tests/mcp_gateway.rs:10853` `native_mcp_remember_put_per_call_visibility_and_per_request_stream` | 1 |
| One process serves three stream pairs per request (card 11) | `bins/gateway/tests/mcp_gateway.rs:10342` `native_mcp_one_process_serves_three_stream_pairs_per_request` | 1 |
| Write latency under soak | `soak-report-humaux_thread_soak28.json` `latency[remember]` | 63 |

### 1.2 Retrieve (recall / memory / context)

| Claim | Anchor | n |
|---|---|---|
| Real-Qdrant semantic recall + read-your-write acceptance | `bins/gateway/tests/mcp_gateway.rs:1764` `native_gateway_semantic_recall_real_qdrant_pg_and_ryw_acceptance` (`--ignored`, `HUMAUX_REQUIRE_DB=1`) | 1 |
| `memory.get` authorization + lifecycle matrix (incl. `archived:true` still returned) | `bins/gateway/tests/mcp_gateway.rs:3748` `native_mcp_memory_get_authorization_and_lifecycle_matrix` | matrix |
| `memory.enumerate` freezes a snapshot and binds the cursor | `bins/gateway/tests/mcp_gateway.rs:5064` `native_mcp_memory_enumeration_freezes_snapshot_and_binds_cursor` | 1 |
| `memory.enumerate` counts inside the page snapshot (EXACT census, card 19) | `bins/gateway/tests/mcp_gateway.rs:5230` `native_mcp_memory_enumeration_counts_in_the_page_snapshot` | 1 |
| A revoked page fails whole and restarts (never a partial page) | `bins/gateway/tests/mcp_gateway.rs:5323` `native_mcp_memory_enumeration_fails_whole_revoked_page_and_restarts` | 1 |
| `context.assemble` preserves governance on overflow, refuses unimplemented routes | `bins/gateway/tests/mcp_gateway.rs:3398` `native_mcp_context_preserves_governance_on_overflow_and_rejects_unimplemented_routes` | 1 |

**Completeness classes — the three answers, each with its pin** (§59 four-class vocabulary,
§22.4 scoping, ADR-0041 D-I):

| Operation | Class | Why | Pin |
|---|---|---|---|
| `memory.enumerate` | `exact` | a frozen denominator exists (card 19 `exact_census`) | `native_mcp_memory_enumeration_counts_in_the_page_snapshot` |
| `memory.get` | `cannot_establish` / `count_unknown` | DirectGet has no enumerable universe; a universe-of-one census is a future decision (ADR-0041 D-D) | `native_mcp_memory_get_authorization_and_lifecycle_matrix` |
| `recall.search` | `semantic_bounded`, `reason: null` | §22.4 scopes `reason`/`known_lower_bound` to `cannot_establish` only | ADR-0044 (card 22 narrowed Tool 2 to the semantic lane) |
| `context.assemble` | `cannot_establish` / `lane_failed`, and since card 22c also `mandatory_not_satisfied` | the §25 selector set is wired but the PINNED lane has no deliverable class — see §6.3 | ADR-0047 |

### 1.3 Share (workspace / tenant scope)

| Claim | Anchor | n |
|---|---|---|
| Workspace membership defines scope (card 13) | `bins/gateway/tests/mcp_gateway.rs:11770` `native_mcp_workspace_membership_scope` | 1 |
| Suspending a membership rejects the bearer on the NEXT request; last owner protected (card 12) | `bins/gateway/tests/mcp_gateway.rs:11431` `native_mcp_membership_suspend_rejects_bearer_on_next_request_and_last_owner_protected` | 1 |
| No response ever carries another tenant's sentinel, tenant id or workspace id | soak assertion `no_cross_tenant_row` (`xtask/src/soak.rs:855`), soak26 / soak27 / soak28 | **196** responses each, value 0 |

### 1.4 Undo (confirm gate, supersede, pin, lifecycle restore)

| Claim | Anchor | n |
|---|---|---|
| `memory.supersede` behind the confirm gate (card 1) | `bins/gateway/tests/mcp_gateway.rs:5527` `native_mcp_memory_supersede_confirm_gate_acceptance` | 1 |
| `memory.pin` / `unpin` behind the confirm gate (card 2) | `bins/gateway/tests/mcp_gateway.rs:7630` `native_mcp_memory_pin_unpin_confirm_gate_acceptance` | 1 |
| `memory.archive` behind the confirm gate (card 3) | `bins/gateway/tests/mcp_gateway.rs:6920` `native_mcp_memory_archive_confirm_gate_acceptance` | 1 |
| `memory.restore` behind the confirm gate (card 3) | `bins/gateway/tests/mcp_gateway.rs:7164` `native_mcp_memory_restore_confirm_gate_acceptance` | 1 |
| `unarchive` undoes the archive and NOT an intervening supersede | `bins/gateway/tests/mcp_gateway.rs:7082` `native_mcp_memory_unarchive_undoes_archive_not_intervening_supersede` | 1 |
| `memory.correct` writes a NEW version and never edits Evidence | `bins/gateway/tests/mcp_gateway.rs:6075` `native_mcp_memory_correct_writes_new_version_and_never_edits_evidence` | 1 |
| `memory.confirm` promotes a candidate to `UserConfirmed` Evidence | `bins/gateway/tests/mcp_gateway.rs:6464` `native_mcp_memory_confirm_promotes_candidate_to_user_confirmed_evidence` | 1 |
| `memory.reject` + candidate enumeration | `bins/gateway/tests/mcp_gateway.rs:6698` `native_mcp_memory_reject_and_enumerate_candidates` | 1 |
| A supersede's MEMORY_LIFECYCLE ticket settles `DONE`: the dead memory's binding is retired and its point leaves the index (ADR-0049 D-A; was `registry_failed` every time, §5.8) | `crates/adapters/tests/projection_worker.rs` `a_superseded_memory_ticket_retires_its_point_and_settles_done` | 1 |
| `memory.archive` excludes the row from `recall.search` while `memory.get` still answers it with the flag, and `unarchive` serves it again — live, four processes, real providers (card 4 / ADR-0024 folded debt) | `docs/ops/rehearse.sh` step `archive_exclusion`: `archived_row_is_excluded_from_recall`, `archived_row_still_answers_memory_get_with_the_flag`, `unarchive_makes_the_row_servable_again` (rehearsal6 2/2) | 2 |
| A token-carrying recall after a supersede/restore is served — the Evidence is one overlay object described by its newest row (ADR-0049 D-B; was `ConflictingOverlayEvidence` on every such recall, §5.8) | `crates/adapters/tests/retrieve_read_your_writes.rs` `an_evidence_with_a_lifecycle_row_surfaces_once_as_its_newest_row` | 1 |

§33.10 holds: the confirm gate exists **only** on the destructive ops above.

### 1.5 Recovery (lease expiry, kill -9, SIGTERM drain, --run-once, probes)

| Claim | Anchor | n |
|---|---|---|
| Two passes + an expired lease never duplicate memories (exactly-once) | `bins/private-worker/tests/distill_hop_e2e.rs:1468` `d5_two_passes_and_expired_lease_never_duplicate_memories` | 1 |
| A retryable failure hands the row back for a later pass (never FAILED) | `bins/private-worker/tests/distill_hop_e2e.rs:1564` `d6_retryable_failures_hand_the_row_back_for_a_later_pass` | 1 |
| A parser failure marks the outbox FAILED and writes no memory (fail-closed) | `bins/private-worker/tests/distill_hop_e2e.rs:1444` `d4_parser_fail_closed_marks_outbox_failed` | 1 |
| No live lease survives the drain after chaos kills | soak `no_live_lease_after_drain`, soak28 | 67 jobs, value 0 |
| Every ticket settles after the drain; none lost; each applied once | soak `tickets_settled_after_drain` / `tickets_not_lost` / `tickets_applied_once`, soak28 | 67 each, value 0 |
| ADR-0037 probes stayed green across the whole run | soak `probes_green`, soak28 | 120 probe runs, value 0 failures |
| Watermarks never stalled and never regressed | soak `watermark_no_stall` (n=2 streams) / `watermark_monotonic` (n=80) | 0 |

### 1.6 Customer / user-person memory (subject axis, cards 7–9)

| Claim | Anchor |
|---|---|
| Subject registry declaration + linkage | `bins/gateway/tests/mcp_gateway.rs:9323` `native_mcp_subject_registry_declaration_and_linkage_acceptance` |
| Subject-scoped recall (dense filter on `subject_ids`) | ADR-0029; the §9 `AS RESTRICTIVE` visibility policy is `private.memory_subject_visibility_ok` |

### 1.7 Emotional memory (affect axis, card E1)

| Claim | Anchor |
|---|---|
| VAD annotation, decay, correction-not-mutation, all governed | `bins/gateway/tests/mcp_gateway.rs:9811` `native_mcp_affect_annotation_governance_acceptance`; ADR-0030 |

### 1.8 Project / memory integrity

| Claim | Anchor | n |
|---|---|---|
| Read-your-write token honoured; never refused | soak `ryw_token_honoured`, soak28 | 63, value 0 |
| A settled write is visible to the token that entitles it | soak `ryw_settled_write_visible`, soak28 | 18, value 0 |
| Every post-drain replay was answered | soak `ryw_replay_answered`, soak28 | 2, value 0 unanswered |
| DB connections and RSS bounded under chaos | soak `db_connections_bounded` 19/120, `rss_bounded` 73/2048 MiB, soak28 | 40 polls each |

### 1.9 Governance / ledger (cards 20, 21)

| Claim | Anchor |
|---|---|
| `ops.model_call_ledger` covers the private reasoning hops | ADR-0042; `d1_…`'s ledger block (`bins/private-worker/tests/distill_hop_e2e.rs`, purpose `PRIVATE_DISTILL_TEXT`, real `input_tokens`/`output_tokens`) |
| Derived values are derived (ticket family, ProcessorId, recomputable `source_hash`, lease heartbeats, confirm-token retention) | ADR-0043 |
| §16.3 promotion candidate set + FAILED retirement + BenchmarkNotPass | ADR-0042 / card 20; soak28 `promote_candidates_admitted` = 0 refused, `candidates_pending: 0` |

---

## 2. This card's own change — ADR-0048

The D1 live-distill flake was the last open P0-class item on the delivery path: it failed once
in each of four consecutive main-line chains (2026-09-08, 09-09, 09-19, 09-20) and passed on
retry every time. A 1-in-4 flake on the write→memory hop is not shippable as a limit.

Root cause and fix: **the class menu the model reads is now the ceiling it may assert.**
See `docs/adr/0048-the-class-menu-is-the-ceiling.md`. In short — the v1 contract showed all
seven `AuthorityClass` values and asked the model to apply a negative constraint to its own
answer; a model that judged the evidence to deserve a forbidden class had two escapes and took
both (over-ceiling → rejected; empty answer → silent 0). v2 renders the prompt and schema from
`admissible_classes(ceiling)`, so nothing illegal is on the menu.

- `crates/adapters/src/distill_reasoner.rs` — contract v1 → v2, rendered per ceiling; ceiling
  folded into the contract sha256 so `processing_runs.prompt_hash` moves with it.
- `bins/private-worker/src/distill.rs` — the ceiling is resolved ONCE and feeds both the
  contract and the envelope's `max_class`; a clean parse that yields zero candidates is re-asked
  once (`DISTILL_EMPTY_RETRY_BUDGET = 1`) and the retry is counted in
  `DistillPassReport::empty_retries`.
- New pins: `distill_menu_never_offers_a_class_above_the_origin_ceiling`,
  `distill_menu_never_offers_explicit_task_context`,
  `distill_contract_hash_moves_with_the_ceiling`; `d3_zero_memories_settles_outbox_and_ticket`
  now asserts `empty_retries == 1` and `provider.calls() == 2`.

### 2.1 Acceptance evidence — five consecutive live runs

`d1_live_distill_writes_memories_and_projection_resolves_ticket`, real MiniMax-M3, five
independent runs (`/tmp/c24_d1x3.log`, `/tmp/c24_d1_45.log`, 2026-09-26 03:57–04:01), plus the
chain's own run in `gates_card24_final.log` `distill_hop_all`. **5/5 green, plus the chain = 6.**

Under load, after the ADR-0048 addendum (§5.8 finding 3): rehearsal5's five soaks ran **324 live
MiniMax distills** with `failed=0`, `malformed_retries=0`, `empty_retries=0` and
`origin_authority_ceiling` rejections `0` (`card24_rehearsal5_summary.txt`; the retry counts
are the per-evidence `malformed reply` / `returned zero candidates` log lines, see §5.8
finding 10) — against 8 of 29 failed on the first soak of the rewritten script that morning.

| run | memories | classes | rejected | empty_retries | ledger tokens in/out |
|---|---|---|---|---|---|
| 1 | 1 | `["PrivateKnowledge"]` | 0 | 0 | 569 / 221 |
| 2 | 1 | `["PrivateKnowledge"]` | 0 | 0 | 569 / 159 |
| 3 | 1 | `["PrivateKnowledge"]` | 0 | 0 | 569 / 153 |
| 4 | 1 | `["PrivateKnowledge"]` | 0 | 0 | 569 / 193 |
| 5 | 1 | `["PrivateKnowledge"]` | 0 | 0 | 569 / 211 |

Every run: `outbox=DONE`, `run(completed=true, output_count=1, source_hash_recomputes=true)`,
`disclosures=["SUCCESS"]`, one `PRIVATE_DISTILL_TEXT` ledger row `SUCCEEDED`,
`projection(done=1, failed=0)`, `ticket=(DONE, None)`.

What the numbers say, and what they do not:

- `classes` is `PrivateKnowledge` on all five — the `AuthenticatedAgent` ceiling — where the
  pre-fix model answered `ProjectConstraint` (over-ceiling, rejected) or nothing.
- `rejected: 0` on all five: no candidate reached the store gate and bounced.
- `empty_retries: 0` on all five: **the retry backstop never fired.** The menu fix alone carried
  every run. D-C is insurance that has not yet been needed against the live model, and its
  behaviour is proven by `d3` (deterministic fixture), not by these runs.
- The varying `output_tokens` (153–221) confirm five distinct generations, not a cached reply.
- Five runs bound the flake rate loosely, not tightly. The pre-fix rate was roughly 1-in-4 across
  four chains; 5/5 is consistent with that being fixed and also with a rate low enough to hide in
  five samples. The structural argument — the illegal options are no longer on the menu — is what
  carries the claim; the runs corroborate it.

**Not reversed:** `d2_over_ceiling_candidate_rejected_not_downgraded` still pins §10.1 rule 3
(reject, never downgrade). Option (a) CLAMP was rejected for exactly that reason and is recorded
as the rejected alternative in ADR-0048.

---

## 3. Multi-tenant and concurrency evidence

- Two tenants on one gateway process, chaos rotating through the three workers by pidfile:
  `soak-report-humaux_thread_soak26.json`, `soak27`, `soak28` (`config.tenants` = 2,
  `config.chaos_cmds` = 3, `config.chaos_every_secs` = 90).
- `no_cross_tenant_row`: **n = 196 responses, value 0** on soak26, soak27 and soak28
  independently. This is the number the acceptance gate asks for.
- Rehearsal5 (2026-09-26, five runs): two tenants seeded on the main path in every run,
  `no_cross_tenant_row` value 0 (n = 196 responses) on each of the five soaks, and the
  gateway-level witnesses `tenant_b_cannot_recall_tenant_a` / `tenant_a_cannot_recall_tenant_b`
  / `tenant_b_enumerates_no_foreign_memory` 5/5 (`card24_rehearsal5_evidence/run*/rehearsal.log`).
- Isolation is enforced below the application: `cargo xtask rls-check` (gate, EXIT 0 on the
  card-23 chain, `gates_card23_chain3.log`) checks the §6.2.2 matrix row-for-row.

---

## 4. Speed baseline (运行速度快)

All figures are from real runs against the real providers, **after warm-up**, first-exec
assessment excluded. Unit: **milliseconds**. Source: the `latency[]` block of each soak report.

#### 4.1 Gateway operations measured under soak

`failed` is the soak report's own `failed_calls` for that operation in that run. It is in the
table because a p50 computed over the calls that succeeded says nothing about the ones that did
not, and quoting the percentile alone would hide them.

| Operation | soak23 p50 / p95 (n, failed) | soak26 p50 / p95 (n, failed) | soak27 p50 / p95 (n, failed) | soak28 p50 / p95 (n, failed) |
|---|---|---|---|---|
| `remember` | 70.5 / 539.6 (43, 1) | 67.8 / 618.7 (60, 4) | 68.9 / 515.0 (63, 1) | 70.5 / 556.5 (63, 1) |
| `memory.enumerate` (reported as `memory`; relabelled by card 30) | 36.5 / 54.7 (41, 3) | 41.8 / 64.7 (63, 1) | 39.0 / 60.2 (62, 2) | 49.4 / 71.4 (61, 3) |
| `recall` | 1778.3 / 2285.4 (41, 3) | 1636.7 / 2289.8 (62, 2) | 1671.0 / 1841.5 (62, 2) | 1805.3 / 2048.1 (60, 4) |
| `recall.ryw_replay` | 1468.1 / 1564.3 (2, 0) | 1397.3 / 1664.6 (2, 0) | 1416.4 / 1639.7 (2, 0) | 1448.4 / 1646.1 (2, 0) |
| `remember.ryw_replay` | 36.4 / 40.3 (2, 0) | 35.0 / 47.6 (2, 0) | 22.3 / 32.8 (2, 0) | 27.9 / 40.0 (2, 0) |

#### 4.2 Derived-hop operations measured from their own tables

These are not gateway calls, so they carry no soak `latency[]` row; the figures come from the
rows the hops themselves write. Cited, not re-measured in this pass.

| Operation | p50 | p95 | n | Source |
|---|---|---|---|---|
| distill pass (one Evidence, one real provider round trip) | 3609.9 | 7209.8 | 43 | `docs/adr/0038-*.md:486` — soak23 `private.processing_runs`, `completed_at - started_at` |
| consolidation pass (§11.8 private-inference RPC) | 6428.3 | 8071.9 | 7 | `docs/adr/0038-*.md:487` — `ops.private_inference_rpc_calls`, **`humaux_thread_dev` 2026-09-03**, not soak23 |
| projection resolve (§15.1 `issued_at` → `settled_at`, whole derived chain) | 11888.3 | 23101.3 | 47 | `docs/adr/0038-*.md:488` — soak23 `projection.stream_log` |

#### 4.3 Operations the card names that this baseline does **not** cover

The card's LATENCY BASELINE lists ten operations. Five are in §4.1, three in §4.2. The
remaining ones have **no measurement anywhere on disk**, and are listed here (and in §8.2)
rather than quietly dropped:

| Operation | Status | What would produce a number |
|---|---|---|
| `memory.get` | **not measured before card 30** | the soak driver's `"memory"` bucket was `memory.enumerate` all along (`xtask/src/soak.rs` sent `{"action":"enumerate"}`); card 30 renamed it and added a real `memory.get` bucket — see §4.5 |
| `context.assemble` | **not measured** | same — and see §6's completeness entry: the route answers `cannot_establish/lane_failed` until the two §25 selectors land, so a p50 today would time a degraded lane |
| `--readyz` (each of the four probes) | **not measured** | the soak runs them as `--probe-cmd` for liveness only; the runner records pass/fail, not duration |
| admin probe | **not measured** | no timed harness exists for `humaux-admin` |

**Folded-in card 9 P2 (speed) is likewise unsettled and is a LIMIT, not a finding:** the card
asks for gateway `recall`/`memory.get`/`memory.enumerate` p50/p95 **with vs without subject
links**. No run on disk splits the corpus that way — soak23/25/26/27/28 all wrote
subject-less memories, so every number in §4.1 is the *without* column and the *with* column is
empty. The cost being looked for is real and named in the card: the `AS RESTRICTIVE`
subject-visibility policy calls the non-inlinable `SECURITY DEFINER`
`private.memory_subject_visibility_ok` (`migrations/0155_subject_visibility_policy.sql:52`)
once per candidate `memory_records` row on every gateway read. Producing the split needs one
soak whose corpus is half subject-linked; if the gap shows, the named fix is a STABLE
SQL-language predicate or a per-query CTE — **not** dropping the guard.

Reading the table:

- `recall` is ~1.7 s p50. **Correction (card 30):** this bullet used to read the `memory` row as
  `memory.get` and attribute the ~1.6 s gap to the embedding round trip. Both halves were wrong:
  the row is `memory.enumerate`, and the ledger's query-embedding p50 on the same deployment is
  228.5 ms (n = 368), so at most ~0.23 s of the gap is the provider. The per-stage attribution
  that replaces the guess is §4.5 (ADR-0055 D-E).
- Regression check vs the previous card's numbers: soak28 `memory.enumerate` p50 49.4 ms vs soak27's
  39.0 ms is **+27%**, above the 20% bar this card sets. n = 61/62, and p95 moved 60.2 → 71.4
  (+19%), so it is a whole-distribution shift, not one outlier. It is **not** explained in this
  report and is carried as an open item (§8).
- `remember` p95 is 8× its p50 on every run. The p50 is the common path; the p95 tail is the
  write that lands while a chaos kill is in flight. Both numbers are reported because quoting
  only the p50 would be the more flattering lie.
- `remember.ryw_replay` n = 2 per run (one replay per lane, by design). Two samples is a
  sighting, not a distribution — it is listed so the number is not mistaken for one.

#### 4.4 Rehearsal5 soak measurements (2026-09-26, five runs, 1 session per tenant, think 10 s)

Per run `n` ≈ 64 calls per operation and lane pair; p50 is the median of the five per-run p50s,
p95 the maximum of the five per-run p95s (`card24_rehearsal5_summary.txt`).

| Operation | n (5 runs) | p50 ms | p95 ms (max) | failed calls |
|---|---|---|---|---|
| `remember.put` | 320 | 64 | 555 | 0 |
| `recall.search` (semantic, real DashScope query embedding) | 312 | 1778 | 2149 | 8 — all `DEPENDENCY_UNAVAILABLE` at chaos step 4 |
| `memory.enumerate` | 320 | 45 | 67 | 0 |
| `recall` read-your-writes replay after drain | 10 | 1421 | 2611 | 0 |
| `remember` read-your-writes replay after drain | 10 | 24 | 35 | 0 |

The `recall.search` p50 is dominated by the provider round trip for the query embedding; the
PG/Qdrant work behind it is the `memory.enumerate`-class tens of milliseconds. This is the
first soak series on the tree after §5.8's fixes; it is not directly comparable with
soak23/26/27/28 above (different tree, different tenants), and is presented as its own baseline.

#### 4.5 Card 30 release rehearsal: recall per-stage attribution (2026-09-30, n = 351)

One release-profile rehearsal (`REHEARSE_PROFILE=release SOAK_SECS=1800 SOAK_SESSIONS=1
SOAK_THINK_MS=14000 SOAK_DRAIN=300 SOAK_CHAOS_SECS=400`, three tenants, evidence
`card30_rehearsal_evidence/measure_run3_n351/`). Stage rows are each recall's own
`provenance.stage_ms`; `failed_calls = 0` on every row.

| Operation / stage | n | p50 ms | p95 ms |
|---|---|---|---|
| `recall` (over the wire) | 351 | 934.6 | 2430.7 |
| `recall.stage_sum` (8 stages of `search()`) | 351 | 892.9 | 2279.3 |
| `recall.stage.scan` (gateway gitleaks seal) | 351 | 247.5 | 577.1 |
| `recall.stage.embed` (worker seal + provider + ledger) | 351 | 604.2 | 1490.1 |
| `recall.stage.hydrate` / `qdrant` / `route` / `assemble` | 351 | 15.2 / 4.8 / 2.7 / 2.2 | 76.6 / 23.1 / 51.5 / 7.5 |
| `recall.stage.planner` / `rerank` | 351 | 0.0 / 0.0 | 0.0 / 0.0 |
| provider query embedding (ledger, same window) | 342 | 245.5 | 506.8 |
| `memory.get` (never timed before) | 351 | 34.5 | 271.5 |
| `memory.enumerate` | 351 | 92.5 | 595.8 |
| `remember` | 351 | 86.8 | 380.2 |

The stage sum covers 95.5 % of the wire p50 (rehearsal gate ≥ 90 %: PASS). A second run with the
same parameters (`card30_rehearsal_evidence/chain/`, `79 passed, 0 failed`) gave recall p50 1103.2 /
p95 2384.1 ms (n = 345), stage_sum 95.4 %, scan 270.7, embed 716.1, provider 225.0 (n = 339). The old "~1.6 s is
the embedding" reading is replaced by: debug build (1778 → 935 ms p50 in release), then two local
secret scans per recall (≈ 2 × 248 ms: a 21.3 MB SHA-256 twice plus one `gitleaks` spawn each),
then the provider (≈ 245 ms). Fixes are filed in ADR-0055 §Measurements (outside card 30's files).

Verify-2 release rehearsal (`card30_rehearsal_evidence/verify2/`, 2026-09-30 22:27-23:10, `79 passed, 0 failed`):
recall p50 1010.4 / p95 1731.9 ms (n = 351, failed 0); stage_sum 950.2 / 1607.4 (94.0 %, gate PASS);
scan 251.8 / 389.6, embed 643.4 / 1161.7, hydrate 17.1 / 58.9, qdrant 5.3 / 21.2, route 3.0 / 250.1,
assemble 2.3 / 6.0; all 351 recalls carried all eight stage keys; `memory.get` 37.2 / 121.1,
`memory.enumerate` 103.0 / 316.2, `remember` 82.7 / 327.3. Closes system_audit P1-6, P1-7 and RQ-5.

#### 4.6 Card 30b release rehearsal: one query seal, gitleaks only, hash once (2026-10-01, n = 368)

Same harness and parameters as §4.5 (`c30b_rehearse.sh measure`, evidence
`card30b_rehearsal_evidence/measure/`, 06:09–06:52). ADR-0056 §Measurements carries the reading.

| Operation / stage | card 30 p50 / p95 (n = 351) | card 30b p50 / p95 (n = 368) |
|---|---|---|
| `recall` (over the wire) | 934.6 / 2430.7 | **486.6 / 604.6** (failed 1: one `TRANSPORT` in a worker chaos restart) |
| `recall.stage_sum` | 892.9 / 2279.3 | 462.5 / 584.7 (95.0 % of the wire p50) |
| `recall.stage.scan` | 247.5 / 577.1 | **0.0 / 0.0** (no gateway seal; `trusted_query()` only) |
| `recall.stage.embed` (worker seal + provider + ledger) | 604.2 / 1490.1 | 435.5 / 564.2 |
| provider query embedding (ledger ⋈ `ops.retrieval_embedding_rpc_calls`, same window) | 245.5 / 506.8 (n = 342) | 176.0 / 326.6 (n = 362) |
| `recall.stage.hydrate` / `qdrant` / `route` / `assemble` | 15.2 / 4.8 / 2.7 / 2.2 | 17.9 / 5.3 / 2.0 / 2.2 |
| `memory.get` | 34.5 / 271.5 | 25.5 / 34.2 |
| `memory.enumerate` | 92.5 / 595.8 | 74.6 / 113.7 |
| `remember` | 86.8 / 380.2 | 48.2 / 55.9 |

Recall p50 −448 ms against card 30. What remains in `embed` beyond the provider (≈ 260 ms p50) is
the worker's one gitleaks spawn, the UDS RPC and the ledger reserve/settle; a resident scanner is
the next lever (ADR-0056 Known limits). The rehearsal's first run of the new pst item 7 was
`83 passed, 1 failed`: the credential leg demanded a terminal ticket, but live MiniMax distilled
the credential put into a clean memory ("contains the credential used by the release bot"), which
correctly projected `DONE`. The assertion now grades "no live projected card carries the token"
and the terminal ticket only when the distilled card carries it (ADR-0056 §Measurements).

### Capacity: ingest vs distill

| Quantity | Value | n | Source |
|---|---|---|---|
| Distill throughput (one resident `--distill-serve`, MiniMax) | **0.229 Evidence/s** | 140 `private.processing_runs` over 611 s | `docs/ops/soak.md:60-74`, measured 2026-09-10 |
| Ingest at `--sessions-per-tenant 2 --think-ms 500`, 2 tenants | **0.992 tickets/s** | 211 over 213 s | same |
| Over-subscription factor of that config | **4.3×** | — | same |

**Consequence, stated as an operating rule, not a footnote:** the Distill hop makes one serial
provider round trip per Evidence, so its throughput is the provider's TPS, not Postgres's. Any
load above 0.229 Evidence/s makes `backlog_drained`, `tickets_settled_after_drain` and
`ryw_replay_answered` unwinnable no matter how correct the code is — those verdicts then measure
MiniMax, not Humaux. Size every run with
`tenants × sessions / (think_secs + round_trip_secs) < 0.229`, and re-measure the capacity line
whenever the provider or model changes: it is a property of the deployment, not a constant.

---

## 5. Incidents

### 5.1 2026-09-09 — a subagent killed the user's WeChat

During card 16's soak work, the harness agent ran `lsof -ti :8080 | xargs -r kill -9` between
runs to free a port. Port 8080 was held at that moment by the user's **WeChat**, which was
terminated. No data was touched and nothing was lost, but this was real damage to the user's
machine caused by this project's automation.

**Root cause:** killing by *port* kills "whoever holds this port", not "the process I started".
Any application can hold any port. The command was not wrong about the port; it was wrong about
what it was addressing.

**Rule, added to every card and every agent prompt from card 16 onward:**

1. Never kill a process you did not spawn. `lsof -ti … | xargs kill` and `fuser -k` are banned
   outright.
2. Kill only PIDs you started and recorded in a variable or pidfile, and only after
   `ps -o comm= -p $PID` confirms the expected binary name.
3. If a port you need is busy, use a different port (every harness takes a port flag or env) and
   report the conflict. Never free a port by force.
4. Never stop or restart the shared `humaux-thread-pg` / `humaux-thread-qdrant` containers. To
   make a dependency absent, point at a closed loopback port or an absent socket.

### 5.2 2026-09-17 — a DELETE-class smoke run against the shared database

`cargo xtask sweep-confirm-tokens --all-tenants --consumed-retention-secs 2592000` was
smoke-run against the **shared dev database** (4180 tenants). It deleted 2
`control.confirm_tokens` rows that were already expired and either never consumed or consumed
more than 30 days earlier — the function's intended retention behaviour on unusable rows. The
behaviour was correct; the decision was not. It was a write to shared state taken without
asking.

**Rule reinforced:** DELETE-class smoke runs happen on a throwaway database only.

---

### 5.3 2026-09-18 — the chain's env file went missing and `source` failed silently

The scratchpad copy of `live_env.sh` disappeared (cause unknown; the twin in the delivery directory survived). `gates_card.sh` sourced it at line 6, the failure went to the script's own stderr, and the chain continued on its self-exported DSNs — every DB gate green, every live gate keyless and unpinned. With the host's proxy in fake-IP mode `api.minimaxi.com` resolved to `198.18.0.42` and the SSRF choke point correctly refused it: two gates red for an environmental reason. Fix: the chain refuses to run without the env file and writes `### GATE env EXIT 0 … pins=…` as the first line of every log (v4.1); the working copies moved to the persistent delivery directory.

### 5.4 2026-09-19 — host reboot into macOS 27.0 during a chain

The reboot wiped `/private/tmp` (the session scratchpad and the pinned gitleaks binary), stopped the shared containers and killed the running chain. Everything persisted under `/Volumes/data/output/…/delivery-cards-20260903/` survived; gitleaks 8.30.1 was restored from the phase-9 environment copy (sha256 = pin) and `live_env.sh` now self-heals it.

### 5.5 2026-09-20 → 22 — the implementer hit the model's weekly usage limit

Card 22c's first implementer died after 3.4 h on `weekly limit · resets Sep 22 4am`; the tree kept its partial work; a RESUME NOTE on the card let the successor inventory and finish rather than redo. Four further implementer runs died on API 529/500 before the card completed; the main line fixed the remaining reds itself.

### 5.6 2026-09-24 — a chain and the serial lane shared the cargo lock for ~24 h

They alternated on the build-directory lock; the lane's log did not advance for 42 minutes and one chain gate took 67 minutes. The superseded pre-fix chain (own PIDs, verified) was terminated to free the lock. Rule: the lane and the chain run serially, never concurrently.

### 5.7 2026-09-26 — root cause of every multi-hour warm-up: the build directory on the external volume

`target/` lived on `/Volumes/data` (APFS, mounted `noowners`, 91% full). Every freshly linked executable and proc-macro dylib there paid a full Gatekeeper assessment inside dyld: rustc blocked 25 minutes at 0% CPU in `dlopen` of a proc-macro while `syspolicyd` held the CPU. With `CARGO_TARGET_DIR` on the boot volume the lane's warm-up fell from 12,425 s to 49 s, its clock from 3,647 s to 298 s, clippy from 61 min to 21 s, and a full chain from 21 h to 19 min. This is the mechanism behind the cards 8/13/17 "host transient" debts and every XProtect note in the earlier ADRs; `live_env.sh` now exports the boot-volume target dir for every cargo consumer. The 237 GB external `target/` is now dead weight (housekeeping §7.6).

### 5.8 2026-09-26 — the rewritten rehearsal went red twice, identically, and found three defects

The 36-assertion rehearsal with the soak (`gates_card24_rehearsal2.log`, runs 1 and 2, 05:40 and
05:51) ended `33 passed, 5 failed` both times: `derived_jobs_not_done = 1`,
`the_only_undrained_write_is_the_ryw_probes_own = 2`, `projection_tickets_unresolved = 1`,
`soak_exit_code = 1`, `soak_report_carries_latency_rows_with_n = 1`; soak verdict FAIL with
`recall` 31 of 33 calls failed, `projection_promoted` red for one of two streams. The 33 green
assertions are the non-soak acceptance items (two tenants isolated, subject-scoped recall,
supersede → restore, exact completeness, kill -9 rotation with `recovery_failures=0`,
`stranded_leases=0`, `duplicate_live_points=0`). The five reds had three causes, none of them
the soak's:

1. **Every `memory.supersede` projection ticket failed `registry_failed`** (dev DB, tenant A:
   seq 5 `FAILED registry_failed MEMORY_LIFECYCLE`). ADR-0018 §4 re-upserts the superseded
   memory's point; the registry added by card 18 refuses a non-live source
   (`current_source_matches` → `SourceNotLive`). The FAILED row wedged the §15.4 prefix at 4,
   so the stream never promoted (`projection_promoted`, `derived_jobs_not_done`,
   `projection_tickets_unresolved`, the second undrained write). Present since card 18; no test
   had run a real supersede through the worker. Fix: ADR-0049 D-A.
2. **Every token-carrying recall on that stream failed** `materialize_failed
   class=read-your-writes overlay repeats an Evidence with conflicting state` — the
   rehearsal's own read-your-writes probe (leg B) and all 33 soak recalls. The overlay range
   carried the same Evidence once per lifecycle row and the serving seam called that a
   conflict. Fix: ADR-0049 D-B.
3. **8 of 29 live distills failed `InvalidInput`** with no reason in the log. A 12-call live
   probe through the real worker path showed 4 of 12 replies carrying
   `"memory_type":"Requirement"` — a type the prompt's rule (1) primed and rule (3) never
   offered. Fix: ADR-0048 addendum (prompt mapping, `DistillParseError` in the log, one bounded
   re-ask). The 1 `origin_authority_ceiling` rejection in the same soak was a persisted
   candidate, not a failure — ADR-0048 D-A working as designed.

The first rehearsal on those three fixes (`gates_card24_rehearsal3.log` run 1, 06:26–06:38)
ended `33 passed, 5 failed` again — a different five. Recall and the RYW probe were green
(soak `recall` 0 of 64 failed, probe leg B `items=4 overlay_seqs=[7]`); what remained:

4. **`restored_memory_is_servable_again = 0`.** `memory.restore` flips status back without
   touching `updated_at`, so the restore ticket computed the same point id and found the
   binding fix 1 had just retired — `AlreadyRegistered` on a retired row, resolvable by
   nobody. Fix: an identical registration of a retired binding revives it (ADR-0049 D-A,
   `RegistrationOutcome::Revived`).
5. **Lifecycle outbox rows never settle.** MEMORY_LIFECYCLE / MEMORY_PUBLISHED rows are ticket
   carriers no distiller claims, so they sat `PENDING` forever (dev DB: 9 + 47 rows) and every
   "drained" measure counted them (`the_only_undrained_write_is_the_ryw_probes_own = 2`, soak
   `backlog_drained = 2`, `derived_jobs_not_done = 1`). Fix: the projection worker marks the
   carrier `DONE` when it settles the ticket (ADR-0049 D-C).
6. **The soak's own helpers ran stale binaries.** `rehearse.sh`'s generated chaos / probe /
   projection helpers launched `./target/debug/…` — the external-volume build directory §5.7
   retired — so after the first chaos restart the private and retrieval workers were the
   pre-fix binaries (6 bare `failed: InvalidInput`, no reason, no retry). Fix: every launch in
   both scripts goes through `${CARGO_TARGET_DIR:-target}/debug/`.

7. **`derived_jobs_not_done = 1` was the probe's own job, mis-excluded.** The read-your-writes
   probe deliberately leaves one write un-drained; the assertion excluded its `ops.jobs` row
   by `idempotency_key LIKE '%:<evidence>'`, but derived-job keys carry the job's own id, not
   the evidence's (`derived-work:DERIVED_DISTILL:<job>`). The exclusion never matched, so the
   assertion was red on every run of the rewritten script. Fix (script): exclude by
   `payload->>'evidence_id'`, which is where the distill job actually names its Evidence.

8. **`drain_all` drained one hop of two.** With finding 7 fixed, `derived_jobs_not_done` was
   still 1 (rehearsal4 run 2): the last pre-probe distill lands memories, which creates a
   `DERIVED_CONSOLIDATE` job, and the script's only consolidation pass had already run. A
   drain that stops after the first hop is not a drain; `drain_all` now runs
   `humaux-consolidation-worker --run-once` after `--distill-once` (script fix; the product
   was doing exactly what it should).

9. **`RATE_LIMITED` on lock contention, not on rate** (product). Once the soak harness
   printed the shape of its failed calls, every one read `RATE_LIMITED` after 3–42 ms while
   the buckets held 99 of 100 tokens. Both soak lanes share the pre-auth `ip` bucket
   (127.0.0.1), and `quota_repo::consume_rate` serialized a bucket with
   `pg_try_advisory_xact_lock` — the request that lost the lock race was answered
   `RATE_LIMITED`, the code §72.3 reserves for "秒/分钟级速率超限". Any two clients behind one
   NAT would see the same. Fix: the bucket's lock is waited for (`pg_advisory_xact_lock`
   under a 2 s `lock_timeout`, a timeout failing closed as a DB error, never as a rate
   decision); the critical section is one row update. Rehearsal4 runs 1–3 (before the fix):
   1, 3 and 3 failed calls per soak, of which one (run 3, recall, 1307 ms) was
   `DEPENDENCY_UNAVAILABLE` — the retrieval worker was SIG9'd by chaos step 4 while that
   recall's RPC was in flight, which is the designed fail-closed answer, not a defect. Run 4
   (after the fix, every binary rebuilt 07:24): `38 passed, 0 failed`, soak 16/16,
   `remember` 0 of 64 failed, `recall` 1 of 63 — again `DEPENDENCY_UNAVAILABLE` at chaos
   step 4 (retrieval worker SIG9'd mid-recall), zero `RATE_LIMITED`. Run 5 (07:35–07:47):
   `38 passed, 0 failed`, soak 16/16, `remember` 0 of 64, `recall` 2 of 62 — both
   `DEPENDENCY_UNAVAILABLE` at chaos step 4, zero `RATE_LIMITED`. Rehearsal4's tally is
   therefore 2 clean of 5, with runs 1–3 red on script-side findings 7 and 8 only; the five
   consecutive clean runs the acceptance gate asks for are rehearsal5 (§8.2).

10. **The fresh opus review of the day's work (workflow final stage, 09:04–09:20) found three
    more code defects in the fixes above, all fixed before the commit:** `DistillPassReport::add`
    never folded `malformed_retries`, so the dispatch line's field was structurally 0 in
    production (the "0 malformed retries in 324 distills" claim in §2.1 rests on the
    per-evidence `malformed reply` log lines, which were unaffected — 0 of them in every
    rehearsal5 soak log — not on that field); `d1`'s expected attempt count ignored a
    malformed-reply retry, so a live retry would have turned the fixed hop's own test red; and
    a FAILED lifecycle ticket's carrier outbox row was marked DONE — it now mirrors the ticket
    (`FAILED`). Two documentation P1s (this §7.2 list, the card-4 archive witness in the
    rehearsal, now step `archive_exclusion` with three assertions) and three P2s (a runbook
    anchor, an ADR-0049 open-debt sentence, an ADR-0048 open-debt admission) were fixed with
    them. The chain and the rehearsal were re-run on the corrected tree (§8.1, §8.2).

Also fixed in the script: the soak summary and its assertion read `count/p50_ms/p95_ms` where
the report writes `n/p50/p95` (`soak_report_carries_latency_rows_with_n`), and the soak
harness now prints the shape of every failed call (`soak: <op> failed on lane … : <reply
head>`) — rehearsal4 run 1 had `remember failed_calls = 1` with nothing anywhere saying what
the reply was.

Evidence for the fixes: `gates_card24_rehearsal4.log` and `card24_rehearsal4_evidence/`
(§8.2 states the outcome).

## 6. Limits and ship-as-limits

Each limit states the exact refusal and the condition that unlocks it. None of these is a
"probably fine".

### 6.1 USER_PRIVATE points make the §16.2 promotion switch refuse (ADR-0040 D-J)

The ops `AuthorizationScope` cannot express "all USER_PRIVATE points of a tenant". Rather than
counting two sides that agree only by absence, the §16.2 switch **refuses with
`VisibleUnavailable`** whenever a tenant has any live projected USER_PRIVATE point.

- False on the rehearsal corpus (everything is WORKSPACE_SHARED), so it has never fired here.
- **A real deployment with private memories cannot promote a projection version until a
  system-scope arm exists in `projection::dense`** (§17.1 / §6.1.2 — a change no card owns).
- Unlock: that system-scope arm.

### 6.2 Non-first projection promotions refuse on §16.3 criterion ③ (card 20)

While §69 is `NOT_DECLARED`, criterion ③ cannot be satisfied, so non-first promotions are
refused. Unlock: a §69 unfreeze at ≥800 items with resolution ≤ 2.

Since card 27 (ADR-0052) a transient projection failure no longer lands in `FAILED` — it is a
bounded retry, and `FAILED` means a permanent class or `transient_exhausted` (§6.12) — so a
`FAILED` pin on the §15.4 prefix now always names something an operator should look at.

Related, and operator-visible: **RETIRED_FAILED tickets are permanently invisible to recall**
(§15.2.1 — they were never indexed). This is a consequence of retirement, not loss of anything
that was ever served.

### 6.3 The PINNED lane has no deliverable class (discovered by card 22c, pre-existing)

`fetch_pinned_in_txn` borrows the `ProjectConstraint` floor from `explicit_mandatory_bindings_v1`
while `project_active_constraints_v1` claims every active `ProjectConstraint` row, so
`PinnedLane::excluding_mandatory` is **always empty for legally storable memories**. Two
fixtures had to manufacture a stored `ExplicitTaskContext` to escape it.

This needs a ruling on the PINNED floor before any code change. Until then the affected
assertions are listed here rather than counted as green.

### 6.4 `memory.get` completeness is `cannot_establish` by construction (ADR-0041 D-D)

Not a bug and not fixable by trying harder: DirectGet has no enumerable universe. A
universe-of-one census is a future decision.

### 6.5 Two closed sets have no producer (ADR-0046 open debt)

- `coord.tasks.authorization_epoch` has no writer — no task lifecycle op exists.
- `issuer_kind = TENANT_POLICY` has no producer.

Both are declared shapes with nothing filling them. They are inert, not broken, but a reader of
the schema would otherwise assume they are live.

### 6.6 `AuthorizedAuthority` cannot emit `BEHAVIOR_ELIGIBLE` (§45 G45-2 case E)

`crates/domain/src/policy.rs:17-24` states it in the source: `max_disposition` is the §10.1
table, but `authorize` never consults it and `AuthorizedAuthority` carries only the
`AuthorityClass`. Nothing in the workspace can currently produce `BEHAVIOR_ELIGIBLE`.

### 6.7 The distill class menu is an instruction, not a grammar (ADR-0048 open debt)

`byok::structured_request_body` does not put `json_schema` on the wire
(`crates/adapters/src/byok.rs:1090`). The rendered **prompt** is what constrains the model
today. A sufficiently contrary model can still answer outside the menu, at which point the
parser or `StoredAuthority::authorize` fails closed — the pre-card-24 behaviour, not a new risk.
Measured on 2026-09-26 (§5.8): the live model went outside the `memory_type` list in 8 of 29
soak distills and above the class ceiling in 1 of 29. The prompt now names the mapping and the
worker re-asks once (ADR-0048 addendum); a run that still fails after that is a `FAILED` ticket
`role_maintenance` retires, exactly as before.

### 6.8 The consolidation hop still has the shape ADR-0048 just removed from distill

Found while running this card's chain, and **not fixed here** —
`crates/adapters/src/consolidation_reasoner.rs` is outside card 24's allowed files.

`CONSOLIDATION_PROMPT_V1` rule (4) is the same construction the D1 flake was traced to: it lists
all seven `AuthorityClass` values and then asks the model to apply a negative constraint to its
own answer — "must NOT rank above the highest `class` among the inputs you used". The schema's
`class` enum likewise carries all seven. That is verbatim the v1 distill shape.

What is actually observed, stated no more strongly than the evidence allows:

- In this card's chain, `workers_tests` went **EXIT 101** on
  `bins/consolidation-worker/tests/consolidation_hop_e2e.rs:642`
  `t1_full_inference_hop_publishes_ticket`, with a redacted `PrivateReasoningError`
  (`fingerprint=336e4630`, `rpc_outcome=FAILED`, `rollups=0`).
- The same test **passed on an immediate rerun** (`rollup_id=01a0da25-07e9…`,
  `class=UserPreference`, `sources=2`, real `MiniMax-M3` ledger row, 530 in / 189 out tokens).
- The error is redacted by construction, so **the failure cannot be attributed to the class
  menu**. What can be said is that a live consolidation call failed once and succeeded on
  retry, and that the hop carries the structural shape that produced exactly that behaviour in
  distill across four chains.

The truncation is available here too and is sound: no legal output can rank above the highest
class among the **supplied** inputs, and that maximum is known before the call — so the menu can
be cut to it regardless of which subset the model ends up using. That is a one-function change
of the same shape as `admissible_classes`, for whichever card owns that file.

### 6.9 Provider and host dependence

- The delivery path depends on **real DashScope** (embeddings) and **real MiniMax** (distill,
  consolidation). Both are single points of failure for the hops they serve.
- **This node requires DNS pins.** The dev box's resolver fake-IPs `api.minimaxi.com` into
  `198.18.0.0/15`, which §11.4 forbids; the checked resolver correctly refuses it. That refusal
  is the card-17 negative control, not a defect. Every live run must source
  `live_env.sh`, which unsets the proxy and sets the pins.
- Four processes must run as **four different OS users** — UDS peer-credential checking is by
  uid (ADR-0012/0014). Running all four as one user silently degrades the check to nothing.
  See `docs/ops/supervision.md`.

---

### 6.10 What the post-delivery system audit found (2026-09-26)

An independent read-only audit of HEAD `72bdfa7` (3 inventory agents, 8 lenses, 2 adversarial
refuters per P1 finding, one synthesis; report at `docs/ops/system_audit_20260926.md`) found
**0 P0, 17 P1, about 28 P2**. The P1s that §6.1–6.9 had NOT listed and that a deployment by
the runbook would hit first: the projection hop has no resident or multi-tenant runner
(`humaux-retrieval-worker` offers only `--run-once` pinned to one tenant/workspace by env, so
under the runbook new memories never reach Qdrant unless something external drives it, as the
rehearsal's shell loop did); BYOK is not implemented (one env-held key serves every tenant);
the eight LOGIN roles' passwords are literals in migration 0011 and the runbook rotates only
`role_admin`; no process exports metrics; no backup/PITR and no Qdrant rebuild from
PostgreSQL; no retention or maintenance process; 35 PostgreSQL integration tests skip
silently inside the gate chain; recall fetches only `top_k` (5) candidates while its
provenance claims `cand_k` (25); the query planner answers `INVALID_INPUT` to everyday words.
The ranked list, evidence and fix sketches are in the audit report. **This report's "MET"
verdicts stand for what the rehearsal exercised; the audit is the record of what it did
not, and it withdraws the "ready for production" reading of §5.**

### 6.11 Folded debts closed by card 26 (ADR-0051 D-L)

- **ARCH-7, second half.** `Baseline_2.9.md` line 3 now reads "Architecture Baseline 2.9" (was still "2.8" after
  card 25 pinned the toolchain); every member `Cargo.toml` carries `rust-version.workspace = true`.
- **Gate-truth breadth (review P2, card 25).** The DB-bound set is now computed from the dependency map itself
  (`dep_map::test_target_services`, ADR-0051 D-K) instead of the 4-marker allowlist: 28 raw-DSN/raw-Qdrant
  integration binaries that were invisible to `HUMAUX_REQUIRE_DB` are now covered, and the 4 ad-hoc `SKIP:` paths
  were converted to `skip_or_fail` calls.
- **`migration_rehearsal.rs:344` and `soak.rs:1366` (review P2, card 25).** Fixed under ADR-0051 D-L: manifest
  checks containing `;` outside comments/literals are refused at parse time and EXPLAIN goes over the extended
  protocol; the reused-pid defence matches the executable basename, not the full `ps` path.

### 6.12 Closed by card 27 (ADR-0052)

- **P1-1 is closed by card 27.** The projection hop now has a resident, tenant-free runner:
  `humaux-retrieval-worker --serve` (runbook §5 step 3, supervision.md §4 rule 3) claims ISSUED
  tickets across every placed tenant through the owner definer `projection.claim_issued_tickets`
  (migration 0176). The rehearsal's per-tenant `--run-once` shell loop is deleted; the rehearsal
  and the soak run the same single `--serve` process, with no tenant in its environment. First
  activation of a (tenant, workspace) serving projection stayed an operator act until card 28 (§6.13).
- **C4 is closed: a transient projection failure is now a bounded retry.** Qdrant transport/5xx,
  provider 429/5xx, PG connection/serialization/lock errors, a lost registry race and an
  unconfirmed visibility probe return the ticket to `ISSUED` with exponential backoff
  (`attempts`, `next_attempt_at`); after `HUMAUX_RETRIEVAL_WORKER_MAX_ATTEMPTS` it settles
  `FAILED` `transient_exhausted`. `FAILED` now means **permanent** (`qdrant_upsert_rejected`,
  `embedding_rejected`, `embedding_dimension_mismatch`, `card_unbuildable`, `secret_scan_rejected`, `registry_conflict`,
  `distill_failed`, `no_visible_memory_record`, …) or `transient_exhausted` — it no longer means
  "one Qdrant blip". A dashboard that counted `FAILED` as blips will see fewer rows and four new
  classes (`qdrant_upsert_rejected`, `embedding_rejected`, `secret_scan_rejected`, `transient_exhausted`).
  A **dependency outage** (Qdrant, the embedding provider including a quota/budget window, the
  scanner, a PG connection) spends at most one attempt per ticket however long it lasts: while the
  runner has seen no `DONE` since the first outage failure, further outage failures are uncharged
  retries with backoff (`refunded=` on the pass line), so an outage longer than the backoff series
  no longer exhausts every ticket written during it (review 2026-09-29).
- **P1-15 and DM-7 are closed**: six hot-path indexes (0178–0183, built `CONCURRENTLY` through
  `xtask migrate`'s new `transaction = "none"` manifest key) plus the claim's own (0177); the three
  `private.context_bindings` presence CHECKs are validated (0184). The seven indexes are pinned in
  the catalog (present, valid, exact definition) by `crates/adapters/tests/hot_path_indexes.rs` —
  the EXPLAIN assertion alone caught only 2 of 6 dropped indexes at dev data sizes.
- **Open for the main line:** the card's literal gate "all tickets DONE within 120 s" of the puts
  is NOT met (put→DONE p95 143.9 s, max 146.1 s, n=60 — bounded by live distill, not projection);
  the rehearsal grades `projection_lag_within_120s` and prints the literal as a `GATE-LITERAL` line.
- **120 s line, main-line reading (ADR-0052 Measurements):** all tickets settled (60/60) and projection lag
  p95 < 120 s (p95 3.0 s, n=60). Put->DONE p95 143.9 s / 169.6 s is owned by card 32 (distill throughput),
  not by projection.
- **Measurements** (n and unit per row, before/after plans): ADR-0052 §Measurements, from the
  rehearsal step `projection_serve_multi_tenant`.

### 6.13 Closed by card 28 (ADR-0053)

- **"There is no production onboarding path" is closed.** `humaux-maintenance` (the §4.2
  operator-write process) is a real CLI: `deploy-init` (GLOBAL/REGION §0117 tiers), `onboard
  tenant|workspace|user`, `apikey issue|revoke`, `placement ensure`, `collection ensure`, `activate`,
  `status` — each one-shot, idempotent (a re-run writes nothing and answers `existing`), one JSON
  receipt, exit 0/3/2/1, the API key printed once and nowhere else. Every write goes through eight
  owner SECURITY DEFINER functions (migration 0186) executable by `role_maintenance` only; the
  provisioning code moved out of `xtask e2e-seed` into `crates/adapters/src/provisioning.rs`, which the
  seed now wraps (its 127.0.0.1 guard kept). Runbook §3.
- **"Reads are DEPENDENCY_UNAVAILABLE until a manual projection-serve" is closed.** Onboarding
  performs the first activation itself with VerifiedEmpty evidence (a new `ActivationEvidence` form,
  ADR-0017 amended; the family's DB-derived head is 0 and the Qdrant family probe counts 0), and the
  workspace is `READY` before its first write; while `PROVISIONING`, every stream-issuing write is
  refused `CONFLICT` by an invoker trigger on the `issued_highwater` increment (migration 0185).
  `memory.get` / `memory.enumerate` no longer need a serving version at all (PostgreSQL is the
  authority; the visible count is then `null` and the census never claims `exact`), and
  `recall.search` / `context.assemble` on an initialised but unserved family answer the explicit
  B-shaped read (`cannot_establish / no_serving_projection`, `current = false`) instead of
  `DEPENDENCY_UNAVAILABLE`; recall does it before paying for a query embedding. An uninitialised pair
  (no checkpoint row) is still `DEPENDENCY_UNAVAILABLE`. Runbook §6.
- **Acceptance** (`cargo xtask e2e-onboard`, fresh database 0001→head, release binaries, live DashScope
  and MiniMax): the onboard receipt carries every field of the gate; immediately after it the four read
  routes answer NOT_FOUND / exact total 0 / 0 items `current=true` / empty context, none
  `DEPENDENCY_UNAVAILABLE`; 5 `remember.put` were recalled 35 s later with no operator action and no
  `projection-serve`; a re-onboard wrote nothing; the five faults (non-empty family, missing placement,
  write while PROVISIONING, concurrent onboard of one name, e2e-seed on a hostname DSN) were each
  refused as specified. Measurements: ADR-0053 §Measurements.
- **Known limits (ADR-0053):** the collection generation is a config digest (card 37 adds a real
  generation column); `LEGACY` workspaces (every pre-0185 row) are never gated and are still activated
  by `projection-serve`; the entitlement period and quota window are issued at onboarding only (renewal
  is card 35's `--serve` job, until then a tenant past `--period-end` gets QUOTA/ENTITLEMENT); the owner
  email is stored unverified (§74 is post-go-live); the BYOK distill lane is not part of onboarding
  (card 52).

### 6.14 Closed by card 29 (ADR-0054)

- **Audit C5 ("governance ops only work for the bootstrap (tenant, workspace)") is closed.** The 14
  governance / subject / affect ops — `memory.supersede/restore/correct/confirm/reject/archive/
  unarchive/pin/unpin/bind/unbind` (confirm-gated) and `subject_register/subject_link_key/
  annotate_affect` — derive their write stream per request exactly like the reads (ADR-0031 D-A):
  the credential's tenant + the requested or bound workspace, narrowed by live membership, admitted
  only when the pair's ledger key was provisioned. The bootstrap comparison is deleted and the
  gateway holds no (tenant, workspace) at all; `HUMAUX_GATEWAY_REMEMBER_TENANT_ID / _WORKSPACE_ID`
  are optional and ignored (runbook §4). ADR-0031 D-B is retired.
- **In-transaction recheck.** Every one of those write transactions (both confirm legs) calls the
  owner definer `control.assert_write_scope(api_key_id, workspace_id)` (migration 0188, EXECUTE
  `role_gateway` only): credential unrevoked, tenant and user ACTIVE at their snapshotted epochs,
  tenant and workspace membership ACTIVE and held `FOR SHARE` — a suspend, removal or epoch bump
  that commits between authentication and the write's COMMIT now stops the write (`FORBIDDEN` /
  `UNAUTHORIZED`). Ceiling: the tenants / api_keys rows are rechecked but not locked.
- **Confirm tokens bind the workspace** (migration 0187, expand only: nullable
  `control.confirm_tokens.workspace_id` + composite FK to `control.workspaces`). A token minted in W1
  and presented in W2 of the same tenant answers `CONFLICT` and stays usable in W1; an unbound PAT has
  no route for a confirm-gated op (`DEPENDENCY_UNAVAILABLE`, no token row).
- **Candidates are scoped.** `memory.confirm` / `memory.reject` lock a distill candidate only inside
  the narrowed write scope (TENANT_SHARED, WORKSPACE_SHARED of the routed workspace, or the caller's
  USER_PRIVATE — the `memory.enumerate {candidates:true}` predicate); another workspace's candidate
  is `NOT_FOUND` with nothing written (ADR-0054 D-B′, three-pair W6b).
- **Deploy note (§46.1 expand).** Order: migrate 0187/0188, then roll the gateway binary; the old
  binary runs unchanged on the expanded schema (NULL `workspace_id`, nothing CHECKs it). Rollback:
  binary first, then drop the FK and the column. NULL rows (pre-0187, or minted by an old binary
  during the rollout) can never be consumed by the new binary — a client in the middle of a two-step
  call sees one `CONFLICT` and re-mints (TTL 300 s). The CONTRACT migration
  (`CHECK (workspace_id IS NOT NULL) NOT VALID`, then `VALIDATE` once `control.sweep_confirm_tokens`
  has removed every NULL row) is a follow-up after the observation window; its rollback drops the
  CHECK before the binary.
- **Acceptance:** `native_mcp_one_process_serves_three_stream_pairs_per_request` (one in-process
  gateway without a default pair: A/B same tenant + user, C other tenant; W1–W11 + W6b) and the rehearsal's
  governance steps for both seeded tenants (`governance_tenants_exercised=2`,
  `gateway_boots_without_a_default_write_pair=0`, `cross_workspace_token_replay_is_conflict=CONFLICT`).
  Rehearsal 2026-09-29: `REHEARSAL VERDICT: 74 passed, 0 failed` (the governance legs ran for both
  seeded tenants). Measurements: ADR-0054 §Measurements.
- **Open:** `memory.confirm` on a PROVISIONING workspace answers `INTERNAL` instead of `CONFLICT`
  (`distill_repo`'s error map lacks the ADR-0053 `55000` arm, outside this card's files; the gate
  itself holds — nothing is written); `xtask e2e-onboard` still passes the two ignored keys.

### 6.15 Closed by card 30 (ADR-0055)

| Change | Before | After (ADR-0055) | Witness |
|---|---|---|---|
| Candidate depth | Qdrant asked for `top_k` (5); `cand_k` (25) only printed | Qdrant asked for `cand_k`; PG gate → cut to `top_k` → mood rerank (a permutation of that visible set, ADR-0030 D-D kept) → cut to `limit`; `candidate_count` is the real Qdrant count | `recall_archive_fixture_fills_top_k_from_cand_k_over_fetch`, `recall_mood_rerank_permutes_only_the_visible_top_k` |
| Candidate scope | tenant-wide: another workspace's tenant-shared points took candidate slots (the registry never resolves them for this stream) and collided with this stream's per-workspace tombstoned seqs | `workspace_id == request workspace` ANDed with the seq overlay in one builder (`DenseQuery::in_workspace_stream`) | `recall_candidate_set_is_scoped_to_the_request_workspace_stream` |
| Lifecycle prefilter | none — archived/superseded points took candidate slots, the PG gate dropped them, recall under-filled | payload `status == "active"` AND NOT `archived == true` (nested `must_not`, so a flagless legacy point passes); `archived` derived from `archived_at` on every ticket; TOMBSTONED seqs reach the overlay | `dense_query_never_returns_an_archived_or_non_active_point`, the five `projection_worker` flag tests, `recall_tombstoned_seq_never_reaches_the_candidate_set` |
| Planner lane substitution (**LANE_SUBSTITUTED**) | any non-SEMANTIC class (最近/相关/目前/quotes/UUID …) was `INVALID_INPUT` | dense answers every class; `provenance.planner_class` recorded; `completeness.degradations ∋ LANE_SUBSTITUTED` when no `mode` was sent (new `DegradeCode::LaneSubstituted`, §53.4 fault file); an explicit undelivered `mode` stays `DEPENDENCY_UNAVAILABLE` | `recall_everyday_queries_are_answered_by_dense_with_lane_substituted`, `recall_explicit_undelivered_mode_is_refused_and_explicit_semantic_is_not_substituted`, rehearsal `recall_everyday_queries_answered_with_lane_substituted` |
| `limit` | must equal `top_k` | `1..=top_k` accepted, only shortens the returned list; `> top_k` stays `INVALID_INPUT` | `recall_with_a_consistency_token_answers_and_a_caller_chosen_limit_is_refused` leg 4 |
| Flagless legacy points | — | no backfill, no `projection_version` bump, no migration: every pre-card-30 point lacks `archived` and still serves; a memory archived before card 30 keeps a flagless point until its next ticket (the PG gate excludes it, the 5× over-fetch absorbs the fill loss) | live count 2026-09-30: 3447/3447 e2e points flagless |
| Per-stage timing | none; the §4.1 bucket was mislabelled | `provenance.stage_ms` (8 stages + total, reported only when every lap followed the fixed lap sequence) and soak buckets `recall.stage.*`, `recall.stage_sum`, `memory.get`, `memory.enumerate` | `recall_stage_ms_cover_at_least_ninety_percent_of_in_process_search_time`, `recall::tests::stage_clock_reports_every_stage_only_for_the_full_lap_sequence`, §4.5 |

- **Spec text amended by the main line (2026-09-30):** Baseline §53.2 (variant list), §53.4 (fault
  file list, `lane_substituted.rs` registered) and §41 (`degrade_total.code` cardinality) now read
  11 variants, matching `crates/telemetry/src/degrade.rs`; the xtask self-test
  `parse_degrade_variant_names_matches_real_degrade_rs` pins 11 and asserts `LaneSubstituted`.
- **Open:** `correct`'s M1 keeps its point until card 31's retire ticket; the tombstone overlay keys
  on `source_stream_seq`, which a later lifecycle re-upsert rewrites (the PG gate stays exact).
- **Behaviour change to know:** `memory.unarchive` is now visible to recall only after its lifecycle
  ticket re-projects the point (projection lag; the prefilter still sees `archived=true` until
  then). The rehearsal's card-4 witness polls for it and reports `unarchive_to_served_s`.
- **Finding (pre-existing, outside card 30):** the §23.1② `visible` count
  (`retrieve::visible_count_of_version` → `VisibleCountFilter::new`, shared with the §16.2 serve
  switch in `xtask/src/switch_visible.rs`) is tenant-wide while the ledger it is compared with is
  one workspace stream, so another workspace's tenant-shared points inflate it and the seq overlay
  subtracts their colliding seqs: the card-30 two-workspace fixture reads `visible = 29` against
  `done = 6` ⇒ `a2_overshoot_beyond_pending` / `cannot_establish`. Fix belongs with the switch
  (`VisibleCountFilter::family_probe` on both sides). ADR-0055 Known limits.
- **Finding (pre-existing, outside card 30):** a recall query holding ≥ 7 digits (a random UUID such
  as `…-79869668a78d`, a date `2026-09-30`, an order number) is refused `FORBIDDEN` by the gateway's
  `seal_query` — `local-secret-scan`'s contribution-privacy phone heuristic
  (`crates/local-secret-scan/src/lib.rs:479-492`) runs on recall queries. ADR-0055 Known limits.
  **Closed by card 30b (§6.16).**

### 6.16 Closed by card 30b (ADR-0056)

| Change | Before | After (ADR-0056) | Witness |
|---|---|---|---|
| Retrieval seal rule set (§7.5 B/C) | `seal_query`/`seal_card` ran the §12 contribution-privacy e-mail/phone rules: a date, e-mail, phone, 7+ digit run or random UUID failed a card `secret_scan_rejected` and a query `FORBIDDEN` | size/DataClass checks + pinned gitleaks only; seal receipts carry `retrieval-seal-secrets-v1`; the contribution path (`scan`/`scan_outcome`) is unchanged | `seal_path_never_runs_contribution_privacy_rules`, `seal_rules.rs` lane(b), `cards_with_date_email_phone_number_and_uuid_project_done`, `worker_embeds_queries_with_date_email_phone_number_and_uuid`, `recall_answers_identifier_bearing_queries_and_refuses_a_credential_query`, rehearsal pst item 7 |
| Seals per recall | two (gateway + worker), each 2 hashes + 1 spawn | one, in the worker (the egress process); the gateway has no scanner, no `HUMAUX_GATEWAY_GITLEAKS_*` keys and only a dev-dependency on the crate | gate `gateway_no_scanner_dep`; soak `recall.stage.scan` p50 0.0 ms |
| Credential query | gateway `FORBIDDEN` | worker `SCAN_REJECTED` → gateway `FORBIDDEN` (operator line `query_scan_rejected`); a broken scanner is `SCANNER_UNAVAILABLE` → `DEPENDENCY_UNAVAILABLE` (it used to read `SCAN_REJECTED`) | `worker_refuses_a_credential_query_as_scan_rejected_and_the_gateway_client_surfaces_it`, `worker_scanner_outage_is_scanner_unavailable_not_scan_rejected` |
| Pinned-binary check | `fs::read` + SHA-256 of 21.3 MB twice per scan | hashed at `new()`, re-hashed only when `(dev, ino, len, mtime, ctime)` changes; a mismatch stays `DependencyUnavailable` | `swapped_binary_by_content_is_detected`, `swapped_binary_by_path_is_detected`, `touched_binary_with_identical_bytes_still_scans` |
| Blocking scan on the async path | gateway `seal_query` inside `async fn search` | worker seal under `spawn_blocking` | §4.6 |

- **Operator-visible:** `HUMAUX_GATEWAY_GITLEAKS_*` removed; a deployment env still setting them fails
  gateway boot (unknown key). Runbook §4.
- **What now leaves for private retrieval:** e-mail addresses, phone numbers, dates and long numbers in
  private memories and queries reach the retrieval provider (the disclosed ExternalProcessor). The
  main line should confirm the tenant disclosure text covers PII in private retrieval.
- **Known limits:** the contribution path still rejects dates / UUID tails / long numbers (fail-closed
  by design); the stat check misses a rewrite that preserves all five fields (root, shared `mmap`,
  coarse-timestamp FS) — the control is a root-owned 0555 binary on a read-only path;
  `egress_chars_total` has no production emit (G80-6), so "one seal per recall" is pinned
  structurally, not by the metric.

## 7. Housekeeping — done on 2026-09-26 with the user's approval

The user approved the whole list on 2026-09-26 ("需要清理删除的进行清理删除，其他的你看着办"). Every
destructive step below was preceded by a backup, is logged line by line in
`delivery-cards-20260903/housekeeping_20260926.log`, and touched only what the list named.

### 7.1 Databases on the shared PG container — 96 archived and dropped, 5 kept

`hk_db_backup_drop.sh`: for every database not in the keep set, `pg_dump -Fc` to
`/Volumes/data/output/humaux-thread-stable-system-20260828/db_backups_20260926/<db>.dump`,
`pg_restore -l` to prove the archive reads back, then `DROP DATABASE` (refused if any live
connection existed — none did). **dumped=96 dropped=96 kept=5 failed=0.** Kept, by name:
`humaux_thread_dev` (the shared dev database), `humaux_thread_ci`, `ci_sim`,
`humaux_thread_stable_observations` (the serial lane's fixed-name fixture, re-created on demand)
and `postgres`. Dropped: `humaux_thread_soak16…28`, `humaux_thread_fix16`, `fix22b_*`,
`fix22c_*` and every `humaux_thread_{request_guard,qdrant,disposable,pre0132,prov_fault}_*`
the lane had provisioned. Re-derive the current list with:

```
docker exec humaux-thread-pg psql -U postgres -d postgres -Atc \
  "select datname from pg_database where datname not in ('postgres','template0','template1') order by 1"
```

**So it does not regrow:** `cargo xtask serial-lane --drop-provisioned` (new) drops, after the
tally, exactly the databases that run created — never one it found, never the fixed-name
fixture — one reported line per drop, `WITH (FORCE)`. The chain's `serial_lane` extra now
passes the flag (`card24_extra_gates.env`); `docs/ops/serial-lane.md` documents it.

### 7.2 The two orphan `private.evidence_objects` rows — deleted, FK validated (0174)

Backed up with every dependent row to `db_backups_20260926/dev_rows_before_delete.json`, then
deleted in one transaction, children first: 2 `control.operation_receipts`, 2
`private.events`, 2 `private.evidence_objects` (`01a0658c-8b4d-7145-8757-f872b1e505bc`,
`01a0658c-8b80-7c69-b52a-2f7ea4c09e8a`, tenant `01a0658c-6e00…`, whose tenant row no longer
existed). Then migration `0174_validate_evidence_reasoning_domain_fk` (`VALIDATE CONSTRAINT`,
FORWARD_ONLY) applied to the dev database: `evidence_objects_reasoning_domain_tenant_fk` is
now `convalidated = true`. Correction (system audit 2026-09-26, DM-5): an earlier revision of
this paragraph said the manifest's precheck "refuses to run while a violating row exists".
That was false — `xtask migrate` executes only the `.sql` and records its checksum; nothing
executes manifest pre/postchecks, and this manifest's postcheck was not even valid SQL until
fixed on 2026-09-26. What refuses a violating row is PostgreSQL's `VALIDATE CONSTRAINT`
itself, which fails the migration. All four checks of 0174/0175 now execute read-only and
return `t`.
Pointer (card 25): manifest checks execute in `xtask migrate` from ADR-0050 D-D on (docs/adr/0050-gate-chain-truth.md).

### 7.3 The four legacy fixture rows — deleted, stored-authority CHECK validated (0175)

The four `{"fixture": "operation receipt scoped context"}` rows carrying
`authority_class = 'ExplicitTaskContext'` (memory ids `01a06b29-48d5…`, `01a06b29-48e6…`,
`01a07aa2-a11f…`, `01a07aa2-a123…`) were backed up with their 4 `memory_evidence` links and 4
`context_bindings` (0 `task_binding_grants`), then deleted in the same transaction as §7.2.
Migration `0175_validate_memory_records_stored_authority_v2` applied (same correction as
§7.2: its manifest checks are documentation; `VALIDATE CONSTRAINT` is the enforcing step):
`memory_records_stored_authority_v2_check` is now `convalidated = true` — the I-STORE rule of
ADR-0046 is proven for every row, not only refused for new ones. The contract test that had
pinned the NOT VALID state as "a card-24 step still owed"
(`task_grant_contract::stored_authority_check_refuses_explicit_task_context_in_the_database`)
now pins `convalidated = true`, so a database migrated only through 0173 fails it by design.

### 7.4 Closed without action — the rehearsal's own probe

Unchanged: the read-your-writes probe's own undrained write is excluded by name (now by
`payload->>'evidence_id'`, §5.8 finding 7) and pinned as the ONLY undrained row.

### 7.5 The stray build directories — deleted

`target-clippy/` at the repo root and the superseded external-volume `target/` (237 GB, §5.7)
were removed after confirming no cargo or rustc process was running; the boot-volume target
directory (`$HOME/humaux-target-boot`) is the only one every chain and rehearsal uses. Free
space on `/Volumes/data` went from 48 GB to over 140 GB.

### 7.6 Plaintext secrets in the Humaux memory library — redacted and deleted; keys to rotate

The note that "memory `d38f590e` holds a plaintext Alibaba key" could not be resolved to an id
by that prefix; a literal search of the library for key-shaped tokens found **five** entries
holding real secrets, none from this project's own flows: a DashScope key (`sk-670638…`, old
35-character format), two DeepSeek keys (`sk-32d11b…`, `sk-742fea…`), the same DashScope key
again inside a Stagehand state document, and an Alibaba Cloud RAM AccessKey ID + Secret
(`LTAI5t9Q…`) for VIAPI in a 2026-07-05 user message. Each was re-stored with its knowledge
intact and the secret replaced by a prefix plus a "rotate" instruction
(`memory_store(supersedes=…)`), and the original was then deleted (`memory_delete`, five
entries) — supersession alone would have left the plaintext readable. **The user must rotate
those four credentials**; this project's own DashScope key (`sk-ws-…`, in `.env.local`) and
MiniMax key were never in the library and are unaffected.

### 7.7 Still the user's — `role_admin`'s development password

Unchanged: migration 0110 creates `role_admin` with LOGIN and no password ("provisioned
externally"); the main line set a development password out of band so the mechanism group
could run (`HUMAUX_ADMIN_PG_DSN`). A production deployment needs a real source for it
(`docs/ops/runbook.md` §2). Not changed by this pass.

### 7.8 Migration manifests brought onto the rehearsal gate's closed class set

`cargo xtask migration-rehearsal` had been red since 0169 — five manifests (0169–0173) lacked
the required `rollback_or_forward_fix` / `backup_restore_requirement` fields and used classes
(`EXPAND_ONLY`, `CONTRACT_ONLY`) outside the gate's closed set `{REVERSIBLE, EXPAND_CONTRACT,
FORWARD_ONLY}`. The fields were added (documentation only; the applied SQL is untouched, so
checksums are unchanged) and the classes set to `FORWARD_ONLY`. The gate's static half is
green again and is now a chain extra; its dynamic half (up/down/up with schema digests) is
still `not_applicable` by its own admission — the executor is unimplemented (DOD-089). Note
that "static pass" parses the TOML only: the gate does not execute `precheck`/`postcheck` SQL,
so an invalid check passes it (0174/0175 did, until the audit caught them).

## 8. What this pass ran, and what it did not

### 8.1 The gate chain — first chain one red (rerun green); final chain all green

`gates_card24_final.log`, 2026-09-26 03:44:27 → 04:02:58 (**18 min 31 s**, warm tree).
**27 of the 28 gates exited 0 in the chain. One did not.**

`gates_card24_final.log:2789` reads `### GATE workers_tests EXIT 101`. The code tree was
already final when the chain started (`distill.rs` 02:54, `distill_hop_e2e.rs` 03:36,
`distill_reasoner.rs` 03:37, chain start 03:44:27), so **the red is on the final tree** — a
green rerun of one gate is a different artefact from a chain at zero, and this section does not
present it as the same thing.

EXIT 0 in the chain:

```
env · migrate · rls_check · arch_check · domain_lib · application_lib · adapters_lib
mcp_gateway_build · gateway_warmup · mcp_gateway · gateway_lib · gateway_wiring
xtask_rls_tests · xtask_all_tests · adapters_bins_warmup · adapters_tests
workers_bins_warmup · testkit_tests · admin_tests
dashscope_live_smoke · distill_hop_all · realqdrant_ryw · serial_lane_audit · serial_lane
clippy · fmt · secret_scan hits: 0
```

EXIT 101 in the chain: `workers_tests` — the consolidation hop's live `t1`
(redacted `PrivateReasoningError`, fingerprint `336e4630`). A full rerun of that same gate was
**EXIT 0** (`gates_card24_workers_rerun.log`, 14 binaries, 0 failed). It is a live-provider
transient in a hop this card did not touch — see §6.8, which records what is and is not known
about it rather than filing it as fixed.

**Against the card's acceptance gate ("full gate chain at zero on the final tree"): NOT MET by
this chain.** What is owed is one chain in which `workers_tests` is green in the chain itself.

**Review-fix pass, 2026-09-26 05:11:28 → 05:19:28 (`gates_card24_review_fix.log`).** After the
seven P1s from the opus review were fixed, every gate in this set exited 0, in one chain, on the
final tree:

```
migrate (0 applied, 141 already-applied, drift 0) · rls_check · arch_check
workers_distill_hop (8/8, incl. live d1 and the new d3 processing-runs sentinel)
workers_all · xtask_all (incl. the new e2e_seed::a_seeded_collection_gets_both_payload_indexes)
adapters_tests · gateway_tests · clippy · fmt · secret_scan hits: 0
```

This chain does **not** include `dashscope_live_smoke`, `realqdrant_ryw`, `serial_lane` or the
soak, so it is narrower than `gates_card24_final.log` — it establishes that the review fixes
are green, not that the card's full chain is. The `workers_tests` red above is still owed a
green in-chain run.

Live suites that really ran with the key: `dashscope_live_smoke`, `distill_hop_all`
(`--include-ignored`, so the live D1), `realqdrant_ryw` (`HUMAUX_REQUIRE_DB=1`).

**Final chain on the final tree, 2026-09-26 08:44:18 → 09:03:45 (`gates_card24_final2.log`,
19 min 27 s, boot-volume target, launched automatically after rehearsal5): 27 gates EXIT 0 in
ONE chain** —

```
env · migrate · rls_check · arch_check · domain_lib · application_lib · adapters_lib
mcp_gateway_build · gateway_warmup · mcp_gateway · gateway_lib · gateway_wiring
xtask_rls_tests · xtask_all_tests · adapters_bins_warmup · adapters_tests
workers_bins_warmup · workers_tests · testkit_tests · admin_tests
dashscope_live_smoke · distill_hop_all · realqdrant_ryw · serial_lane_audit · serial_lane
clippy · fmt · secret_scan hits: 0
```

`workers_tests` is green **in the chain itself** (the run owed above), `serial_lane` reads
`n=108 run-set, passed=108, failed=0, not_run=0, retired=7 (inventory=115), lane clock 294.8s`
— the lane now includes the lane-tagged regressions added today (`private_projection_registry`
revive, `quota_and_rate` contention). **Against the card's acceptance gate ("full gate chain at
zero on the final tree"): MET by this chain**, on the tree that also produced rehearsal5's 5/5.

**Chain 3 after the final review's fixes (§5.8 finding 10), 2026-09-26 09:51:25 → 10:11:01
(`gates_card24_final3.log`, run automatically after rehearsal6): the same 27 gates EXIT 0 in
one chain, `secret_scan hits: 0`.** This is the chain on the committed tree.

### 8.2 What ran after the draft, and what still did not

Named plainly, because the alternative is a report that reads as complete and is not:

- **The five-run rehearsal on the rewritten 38-assertion script WITH the soak ran (main line,
  2026-09-26 07:47–08:44, `gates_card24_rehearsal5.log`, evidence
  `card24_rehearsal5_evidence/run1…5`, per-run summary `card24_rehearsal5_summary.txt`):**
  **5/5, each `REHEARSAL VERDICT: 38 passed, 0 failed`** (36 assertions + 2 soak-lane), each run
  ≈ 11.3 min: two tenants seeded on the main path and isolated (`tenant_b_cannot_recall_tenant_a`,
  `tenant_a_cannot_recall_tenant_b`, `tenant_b_enumerates_no_foreign_memory`), subject-scoped
  recall authorized vs unauthorized, `lifecycle: supersede_events=1 restore_undoes=1
  recall_hits_after_restore=1`, `enumerate_completeness_is_exact`, `kill9 rotation:
  recovery_failures=0 stranded_leases=0 duplicate_live_points=0`, the read-your-writes probe
  served with its overlay (`leg B isError=False items=5 overlay_seqs=[7]`), and a 380 s soak
  (16/16 thresholds with `n` and units) — see the next bullet. 324 live MiniMax distills across
  the five runs: `failed=0 malformed_retries=0 empty_retries=0 ceiling_rejections=0`. Three
  earlier series (rehearsal2/3/4, `gates_card24_rehearsal{2,3,4}.log`) and the nine findings of
  §5.8 are what it took to get here; the 04:15 "5/5" of the pre-fix 20-assertion script is
  superseded and is not evidence for the acceptance gate.
  **After the final opus review (§5.8 finding 10) the script gained the card-4 archive witness
  (step `archive_exclusion`, 3 assertions → 41 with the soak) and the tree its three code fixes;
  rehearsal6 (`gates_card24_rehearsal6.log`, `card24_rehearsal6_evidence/run1…2`, 09:28–09:51)
  ran that final script on that final tree twice: `41 passed, 0 failed` both times, soak 16/16,
  `archive: recall_hits_while_archived=0 get_reports_archived=true recall_hits_after_unarchive=1`
  in both, 130 live distills `failed=0 malformed_retries=0` (per-evidence lines). Rehearsal5's
  five runs stand as the acceptance-gate witnesses for the 38-assertion superset they cover;
  rehearsal6 is the witness that the review fixes and the archive step changed nothing else.**

- **Acceptance item (5), the soak, is witnessed by every run of rehearsal5** (`SOAK_SECS=380
  SOAK_SESSIONS=1 SOAK_THINK_MS=10000 SOAK_DRAIN=200 SOAK_CHAOS_SECS=90`, chaos SIG9 rotating
  retrieval → private → consolidation → retrieval worker every 90 s): `soak PASS 16/16` on all
  five, `remember` 0 of 320 calls failed, `memory` (enumerate) 0 of 320, `recall` 8 of 312 —
  every one `DEPENDENCY_UNAVAILABLE` during chaos step 4 (the retrieval worker SIG9'd
  mid-call, the designed fail-closed answer; §5.8 finding 9 is what the *other* shape,
  `RATE_LIMITED`, turned out to be, and it is gone since run 4 of rehearsal4). The script prints
  `NOTE: SOAK_SECS unset — acceptance item (5) NOT witnessed by this run` when the lane is
  skipped, so a run can no longer be silent about it.
- **Soak numbers:** §3 and §4 cite soak23/26/27/28 by file as before; §4.4 adds the five
  rehearsal5 soaks measured today (same config shape: 2 tenants, 3 chaos commands, 90 s).
- **Three of the ten operations in the card's latency baseline have no measurement**
  (`context.assemble`, `--readyz`, admin probe) and the card 9 P2 subject with/without split has
  none either; `memory.enumerate` is now measured by the rehearsal5 soak's `memory` lane
  (§4.4). §4.3 names each remaining one and what would produce a number.
- **The `subject_ids` uuid payload index is created by the seed tool and exercised by every
  rehearsal run**: each run seeds fresh collections (`xtask e2e-seed` PUTs `subject_index_body()`
  next to `tenant_index_body()`) and `subject_recall_returns_the_subjects_memories` passed 5/5
  against them, so the deploy step at `docs/ops/runbook.md:45-47` is *verified*, not merely
  implemented. Collections seeded before 2026-09-26 still lack the index (the seed tool
  short-circuits on an existing collection) and must be re-created to pick it up.
- **`promote_candidates_admitted > 0` has still never been observed.** soak28 records
  `candidates_pending: 0` with the note "§16.3 was not exercised this run". Freezing a §69 set
  (≥800 items, resolution ≤ 2, `frozen_by` written) and running with a second version is what
  would exercise it. **The delivery ships without an observed real promotion**, stated here
  rather than implied by an absent row.
- **The soak28 `memory.get` p50 regression (+27% vs soak27) is unexplained** — see §4.
- **The serial ignored-tier lane passes as a gate** (`serial_lane` EXIT 0 in this chain), but
  card 23's last full inventory run — 110 tests, 75 passed, 34 failed, 1 not run — has not been
  triaged to completion. The failures cluster in the Phase-9 public/anonymous and contribution
  families (`Forbidden`, `42501`). `card23_lane_failures.txt` has the list.

## 9. How to run it

One command per gate. Every one of them sources the same env file; none of them takes a key on
the command line.

```sh
# The env for EVERY live suite and the gate chain. Sources both key files by path; the values
# never appear in a log, a file, or git.
source /Volumes/data/output/humaux-thread-stable-system-20260828/delivery-cards-20260903/live_env.sh

# The three structural gates
cargo xtask migrate          # 0 = applied, drift 0
cargo xtask rls-check        # §6.2.2 matrix, row for row
cargo xtask architecture-check

# The rehearsal (docs/ops/rehearse.sh, 36 assertions; SOAK_SECS enables the soak lane, acceptance
# item (5)). It builds the five binaries itself into $CARGO_TARGET_DIR, seeds two tenants on the
# dev database, and prints `REHEARSAL VERDICT: N passed, M failed` (exit 1 on any failure).
#   SOAK_SECS=380 SOAK_SESSIONS=1 SOAK_THINK_MS=10000 SOAK_DRAIN=200 SOAK_CHAOS_SECS=90 \
#     zsh docs/ops/rehearse.sh
# The full chain (sources live_env.sh itself; ~20 min on a warm tree, hours on a cold one)
CARD_EXTRA_GATES="$(cat .../card24_extra_gates.env)" \
  zsh .../gates_card.sh /path/to/gates_card24.log

# The ignored tier, serially, on isolated databases
cargo xtask serial-lane --audit-only    # disposition audit, no DB
cargo xtask serial-lane                 # the run

# The live witnesses
HUMAUX_REQUIRE_DB=1 cargo test -p humaux-gateway --test mcp_gateway \
  native_gateway_semantic_recall_real_qdrant_pg_and_ryw_acceptance -- --ignored --exact
cargo test -p humaux-private-worker --test distill_hop_e2e -- --include-ignored

# The secret gate — this exact command, nothing else counts
(git diff; git ls-files --others --exclude-standard | xargs cat 2>/dev/null) \
  | grep -cEi "sk-[a-zA-Z0-9_.-]{16,}|api[_-]?key\s*[:=]\s*['\"][^'\"]{12,}"   # must print 0
```

**Budget the wall clock by the state of `target/`, not by the number of gates.** Measured on
this host, the same chain took **19 minutes** on a warm tree (`gates_card23_chain3.log`,
2026-09-26 02:19→02:38, every gate EXIT 0) and **~21 hours** on a cold one
(`gates_card23_consolidated_v2.log`, 21:56→18:50, of which `adapters_tests` alone was 6 h 16 m).
The difference is not compilation: macOS assesses every freshly linked executable and every
freshly built proc-macro dylib on first exec, serially, through `syspolicyd` — card 23 measured
**8278 s of warm-up for 29 test binaries**. Plan a chain after a dependency-graph change as a
multi-hour job, and do not read a slow gate as a hung test.

**Ports.** The harnesses take a port flag or env. If a port is busy, pass another one and report
the conflict. Never free a port by force — see §5.1.

**Processes.** Four processes, four OS users: `humaux-gateway`, `humaux-retrieval-worker`,
`humaux-consolidation-worker`, `humaux-private-worker`. `docs/ops/supervision.md` has the unit
files and the probe meanings; `docs/ops/soak.md` has the sizing rules; `docs/ops/e2e-seed.md`
has tenant provisioning.

**Activation.** After the projection catches up, the first serving version must be activated
explicitly: `cargo xtask projection-serve` (ADR-0017 — the no-serving-version rule). Later
version upgrades go through the §16.2 two-version comparison, subject to §6.1 above.
