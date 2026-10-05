# ADR-0058 — Distill dispatch v2: tenant-fair claim through four provider slots, generation fencing, counted attempts, honest DEAD

- Status: Accepted (card 32, 2026-10-02, on HEAD `5ff4aff` = card 31). Implemented in four serial slices:
  **slice 1: the SQL core (0190–0192), the `adapters::jobs` API, the rls-check boundary and the T-tests**;
  **slice 2: worker rewrite + cutover (0193) — the private worker claims distill only through the v2 slots**;
  **slice 3: tool-call output, provider error logging, consolidation menu, affect inference (0194)**; **slice 4:
  live gate, rehearsal twins, measurements**.
- Amends: ADR-0036 (0164) for `DERIVED_DISTILL` only — the v1 claim stays for consolidation (L1) and, from 0193
  on, refuses distill. Keeps ADR-0036 D4 (no lease predicate on settle) and ADR-0048 D-D (re-ask budget 1).
- Spec: Baseline §31, §61, §32, §67.2 (private reasoning in-flight 4), §11 (WAITING_KEY), §10.1, §72.3, §78.1,
  §78.2, §6.2.2; research ruling R-32 (`research_gpt_batch1_20260926.md` §Q2); design `card_32_design.md`
  revision 2 + main-line rulings (2026-10-02). The per-transition table and the in-flight argument live in the
  design (§2); this ADR records the decisions and their evidence.

## Context

Read on `5ff4aff` (file:line at that commit):

- P1-4: the v1 distill claim has no reasoning domain; `load_evidence` filters by the job's domain and
  `Ok(None)` settles FAILED (`distill_repo.rs:303`, `distill.rs:502-508`) — a tenant with two domains loses
  valid Evidence.
- P1-16: one global-FIFO statement `ORDER BY priority DESC, next_retry_at, created_at` (0164:174); one tenant's
  backlog delays every other tenant; card-31 rehearsal: 250 never-ready jobs of 142 leaked tenants headed
  every pass (`claimed=8 not_ready=8`, `memory_records=0`).
- C7 / debt 1: a pass works its batch serially and renews only the job in hand (`distill.rs:279-373,
  411-470`); with the provider timeout equal to the lease, the siblings of one hung call lost their leases
  (card 30b: `claimed=139 completed=88 lost_lease=44`).
- v1 counts claims as attempts (`attempt + 1` at claim, 0164:186) — dev DEAD rows reach `attempt=31`.

## Decisions

- **D-A** `ops.jobs` stays the only job table and `status` keeps its seven frozen values. New columns:
  `claim_generation` (fencing token, +1 per claim), `hard_deadline` (end of a claim), `dispatch_state`
  (`CLAIMED | DISPATCH_INTENT | EXECUTION_UNCERTAIN`, only while `PROCESSING`, CHECK added NOT VALID by 0190
  and validated by 0192), `dispatch_model_call_id`, `not_ready_since`, `abandoned_claims`. Rejected: a second
  job table (`ops.distill_jobs`) and new `status` values.
- **D-B** Scheduling per R-32: `ops.provider_arbiters` (one row, `FOR UPDATE` by every claim, `next_turn`
  numbers claims), `ops.provider_slots` (exactly four rows, created by the migration — §67.2),
  `ops.distill_tenant_scheduler` (`last_served_turn` per tenant, admitted by an owner trigger on enqueue),
  one job per claim: arbiter → sweep → free slot `SKIP LOCKED` → least-recently-served tenant with READY work
  (forward pointer: the four slots are shared across every provider, profile and tenant; since 0206 the tenant
  order is fewest held slots, then least recently served — ADR-0060 D-F)
  (`FOR UPDATE OF t SKIP LOCKED`) → that tenant's oldest READY job (`SKIP LOCKED`, eligibility restated) →
  job `PROCESSING/CLAIMED`, slot bound to `(job, generation, hard_deadline)` with a `job_id IS NULL` recheck,
  turn advanced. No attempt at claim. Rejected (card list): global FIFO, row_number-only pick, process-only
  semaphore, count<4-then-insert.
- **D-C/D-D** (slice 2) job identity = one Evidence: the job takes exactly its own Evidence's outbox row in a
  transaction that first locks the job row under its generation (`distill_repo::take_outbox_row`); the
  tenant-batch outbox claim (`claim_pending_evidence`, `release_outbox`, `fail_outbox`) is deleted; the Evidence's
  own domain is compared with the payload's before any binding is resolved, and a mismatch returns the row to
  PENDING as `NOT_READY DOMAIN_MISMATCH`, never FAILED.
- **D-E** Fencing: `ops.renew_lease` (owner + generation + live lease + `hard_deadline`; the new lease is
  `LEAST(now + lease, hard_deadline)`), `ops.begin_call` (same fence + a slot bound to `(job, generation)` +
  `hard_deadline − now ≥ min_remaining`; sets `DISPATCH_INTENT`, `attempt + 1`, writes one `ops.distill_calls`
  row; NULL = refused, no HTTP), `ops.finish_derived_work_v2` (owner + generation + `PROCESSING`, no lease
  predicate per ADR-0036 D4; frees the slot). `ops.distill_calls` is the attempt ledger (R-32 "attempt
  ledger"): every admitted request names its job, generation and attempt.
- **D-F** An attempt is a provider request: counted at `begin_call`, never at claim. A `WAITING_KEY` finish
  (provider 401) reverts its own call's increment (§11: no retry count, never DEAD). DEAD after
  `max_attempts` with the error class (worker config, slice 2); pre-dispatch crashes are counted in
  `abandoned_claims`. Deviation: no `max_attempts` column (a value at enqueue time would be a literal in the
  0164 trigger, §78.1).
- **D-G** The claim's sweep, under the arbiter lock, over bound slots (slot `SKIP LOCKED`, job read by MVCC
  only, every transition probes the job row `SKIP LOCKED` and NOT FOUND means busy): T4 `CLAIMED` with an
  expired lease → READY, `abandoned_claims + 1`, slot freed; T5 `DISPATCH_INTENT` with an expired lease and
  `hard_deadline` ahead → `EXECUTION_UNCERTAIN`, **slot kept** (lease expiry is not the end of execution); T6
  `DISPATCH_INTENT | EXECUTION_UNCERTAIN` past `hard_deadline` → PENDING after one lease, class
  `EXECUTION_UNCERTAIN`, attempt stays counted, slot freed; T9 a slot whose job is gone or no longer
  `PROCESSING` under that generation is healed only after `bound_until`.
  **Main-line ruling E1 (accepted as designed):** T6 re-queues automatically. R-32's "no automatic resend" is
  read as "never at lease expiry and never silently": T6 fires only after `hard_deadline`, which config
  validation keeps `≥ 2 × (http_timeout + lease)` so no request of that claim can still be open, and the
  transition is classed, counted and attributed. Operator-only reconcile is rejected because four kill -9s
  would stop distill for every tenant of an unattended deployment. Price: at most one extra billed call per
  uncertain claim, bounded by `max_attempts`, reported as "unknown outcome", never as a duplicate after a
  SUCCEEDED call. Rejected: lease-expiry-frees-slot.
- **D-H** `NOT_READY` (anything before the first admitted call) backs off and parks as `WAITING_KEY` with its
  class once not ready for the park age; a parked job is re-checked by the claim after the park interval and
  never spends an attempt.
- **D-I** Function owner stays `role_migration_owner` (deviation from R-32's dedicated NOLOGIN owner): the
  owner arm on `jobs_tenant_isolation` (0164) and the new owner/tenant policies meet R-32's FORCE-RLS concern;
  a NOLOGIN owner would need a new rls-check role gate and has no precedent. Contained by
  `search_path=pg_catalog`, schema-qualified names, `DERIVED_DISTILL`-only bodies with explicit job/tenant
  filters, EXECUTE `role_private_worker` only.
- **D-J/D-K** (slice 2) `IN_FLIGHT` seat futures polled by one task (`distill::join_all`, `// ponytail:` L6); each
  seat claims one job, runs it, claims again at once and sleeps the poll interval only after an empty claim
  (`--distill-serve`) or stops there (`--distill-once`). Per job, the work and its own heartbeat (`renew_lease`
  every lease/3) are polled by one `select!`; a lost lease never drops the work (its call is still ledgered, its
  settle is refused by the generation fence). The HTTP cutoff (`local_deadline − lease`, the local deadline taken
  before the claim) is passed into `DistillReasoner::call` and cuts only the provider future; `prepare` reserves
  the ledger and disclosure rows, `begin_call(min_remaining = http_timeout + lease)` admits the request, `abandon`
  finalizes a refused one as `FAILED DISPATCH_REFUSED`. Config keys `HUMAUX_PRIVATE_WORKER_DISTILL_IN_FLIGHT`,
  `_HARD_DEADLINE_SECS`, `_NOT_READY_PARK_SECS` (all required, no code default); `_DISTILL_BATCH` and
  `_DISTILL_JOB_BATCH` removed; `validate()` requires `hard_deadline ≥ 2 × (http_timeout + lease)` and (ruling R2)
  `1 ≤ in_flight ≤ 4` (`distill::PROVIDER_SLOTS`, the seeded slot rows the pool is sized for); rehearsal values
  lease 30, in-flight 4, hard deadline 300, park 600. Ruling R3: `lost_lease` counts generation-fence refusals
  only; an error that escapes a job (DB, reasoner, membership) is `errors=<n>` on the dispatch line and
  `outcome=ERROR error_class=<class>` on the job line, so "healthy = `lost_lease=0`" stays true.
- **D-F settles (slice 2)**: anything before the first admitted request is NOT_READY; a provider 401
  (`ReasoningProviderError::WAITING_KEY_CLASS`) parks WAITING_KEY and reverts its attempt; any other failed call is
  RETRY with `jobs::retry_backoff_seconds(lease, attempt)` or, at `max_attempts`, DEAD with the class and the outbox
  FAILED in the same transaction (forward pointer: so a provider outage settles RETRY → DEAD, not WAITING_KEY,
  and is recovered with `jobs requeue-dead` once the provider is back — ADR-0060 D-G); a refused malformed re-ask
  is DEAD `FAILED_OUTPUT_SCHEMA`, a refused empty retry
  accepts the empty answer; a claim with `attempt` or `abandoned_claims ≥ max_attempts` is DEAD at once
  (`ATTEMPTS_EXHAUSTED` / `PRE_DISPATCH_ABANDONED`). Every settle runs `finish_derived_work_v2` as the first
  statement of the transaction that also flips the outbox row (and, for DONE, writes the memories). A T6 re-claim
  prints one line naming the uncertain `dispatch_model_call_id` (ruling E1). `ReasoningProviderError::class()`
  (one static class per variant) lands here because the worker needs it for the class and the 401 test; the
  provider failure log line itself is slice 3 (D-N).
- **D-M/D-N/D-O/D-P** (slice 3) `emit_distillation` tool path, provider failure line, consolidation class
  menu, affect inference (0194) — see "Slice 3" below.
- **D-R** No `claim_request_id` (deviation): a claim is one autocommit statement; a lost response leaks one
  slot for one lease (T4 frees it). Limit L11.
- **D-S** `jobs_distill_ready_idx (tenant_id, next_retry_at) WHERE DERIVED_DISTILL AND status IN
  (PENDING, RETRY_WAIT, WAITING_KEY)` (0191, CONCURRENTLY).
- **D-T** (card-32 review, guard (d) of ruling E1) §72.3 tenant distill budget. The research addendum assumed
  `begin_call` already verified a §72.3 reservation; it did not (0190's `begin_call` checks owner, generation,
  lease, slot and deadline; the ledger reservation is a plain INSERT; the product had no private-reasoning
  provider budget at all — `control.memory_automation_policies` (0050) is read by no code). 0196 adds
  `ops.admit_distill_budget(tenant, window_seconds, max_calls)`: a sliding window over the attempt ledger
  (`ops.distill_calls.begun_at`, index `distill_calls_tenant_begun_idx`), no new state, the tenant's advisory lock
  held to the end of the caller's transaction. `jobs::begin_distill_call` runs it and `ops.begin_call` in ONE
  transaction, so every request — first call, re-ask and the T6 resend after `EXECUTION_UNCERTAIN` — is admitted
  only while the tenant budget admits it (`CallAdmission::{Admitted, Refused, OverBudget}`); an over-budget
  admission counts nothing and writes nothing. Worker: a refused FIRST call settles NOT_READY with class
  `PROVIDER_BUDGET` (backs off, never an attempt); a refused re-ask fails closed exactly like a fence refusal
  (ADR-0048 D-D). Config `HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS` / `_MAX_CALLS` (required, no default,
  `validate()` rejects 0); rehearsal 60 s / 120 calls. No breaker (card 38) and no token weighting (L18).
- **D-U** (main-line ruling R4, plan v2 OPS-3) Operator re-drive of a DEAD job: 0197
  `ops.requeue_dead_distill(tenant, job, error_class)`, one owner definer (`search_path=pg_catalog`, EXECUTE
  `role_maintenance` only, tenant GUC asserted first, the 0186 door pattern). In the caller's one transaction it
  re-arms either one job or every DEAD job of the tenant whose `last_error_class` equals the given class
  exactly: job `PENDING`, `attempt 0`, `abandoned_claims 0`, ready now, `last_error_class` **kept**; the
  Evidence's outbox row `FAILED → PENDING` (an open v1 leftover is taken as it is); the tenant's scheduler row
  admitted (the 0190 trigger fires on INSERT only). Refusals (55000, nothing written but the §77 DENIED row):
  `job_not_found` (absent / another tenant's), `job_not_dead`, `evidence_gone` (the payload names no Evidence or
  the Evidence has no outbox row — the outbox FK pins `evidence_objects`, and the outbox is readable under the
  tenant GUC alone, while the evidence/event rows are visibility-scoped), `outbox_settled`, `no_dead_job`.
  Door: `humaux-maintenance jobs requeue-dead --tenant <id> (--job <id> | --error-class <class>)` through
  `adapters::provisioning::requeue_dead_distill` (receipt lists every re-armed job; audit risk tag
  `distill_redrive`). Deviations: (1) the binary is `humaux-maintenance`, not `humaux-admin` as OPS-3 wrote it —
  `humaux-admin` is the read-only §4.4 probe process (`AdminDbPool`; `admin::probe` "never writes
  ops.jobs/ops.outbox") and holds no write command, `humaux-maintenance` is the §4.2 operator-write process whose
  card-28 doors this follows; (2) `--error-class` is an exact match against the class the worker stored, not a
  Rust enum: the classes a DEAD settle can store are not a closed Rust set (`DistillError::class` passes through
  `PrivateReasoningError` labels raised at ~40 sites), so a hand-written list would go stale with the next raise
  site; exact equality, DEAD-only and tenant-scoped is what "closed" protects against (no pattern, no
  cross-class sweep). Runbook §7.1 carries the operator procedure for DEAD, WAITING_KEY, EXECUTION_UNCERTAIN
  and a `RESERVED` ledger row.
  **Amendment (0198, card-32 review P0):** re-arming the job and its outbox row does not re-arm the Evidence's
  projection ticket. On a running system that ticket is already terminal — FAILED `distill_failed` (the
  projection worker read the FAILED outbox row), RETIRED_FAILED, or LOST — and none of those has an edge back to
  ISSUED (0167 guard; the 0176 lease claim takes ISSUED only), so the re-distilled memories would reach PG and
  never the index. 0198 replaces the definer body: when the remember ticket (the `stream_log` row carrying the
  outbox row's `commit_seq`) is FAILED / RETIRED_FAILED / LOST, the same transaction issues a successor ticket
  on that ticket's own stream — `ops.commit_seq_seq`, `issued_highwater + 1`, a `stream_log` row and a
  `MEMORY_LIFECYCLE` carrier bound to the Evidence, the §60 sequence 0155 and `memory_governance_repo` already
  use. An open ticket needs nothing (it reads the PENDING outbox row). The projection worker now decides a
  memory-less ticket from the distill row of the Evidence its carrier binds (not the carrier's own
  `commit_seq`), so the successor waits `distill_pending` while the job is open instead of failing
  `no_visible_memory_record`; for a remember ticket the two lookups are the same row. The old FAILED row is not
  touched: it stays an open gap until the operator's audited `--retire-failed distill_failed` (0167), which no
  longer drops the Evidence. Rejected: moving the old row back to ISSUED (a new owner edge into the §15.2
  machine and a re-used `stream_seq`); retiring it inside the re-drive (retirement is the operator's audited
  act per error class, 0167). Limit L20.
- **D-V** (main-line ruling R5) `--distill-once` says why it stopped: after a seat's empty claim it asks the
  owner definer `ops.distill_slots_all_bound()` (0199, MVCC read, no lock — a locking probe would make a
  concurrent claim's `SKIP LOCKED` slot pick miss a free slot; EXECUTE `role_private_worker` only, the worker
  holds no grant on `ops.provider_slots`). All four bound ⇒ `DrainStop::NoSlot` (READY work may be left behind
  for the next pass), else `NoWork`. The report carries `stopped: Option<DrainStop>` (the worse of the seats,
  `None` in serve mode) and the summary line ends `stopped=no_work|no_slot|-`. The claim's own return shape is
  unchanged (changing `SETOF ops.jobs` would need a second definition of the claim).
- **D-W** (main-line ruling R8, 2026-10-02 20:30) U+0000 is not storable (PostgreSQL `text`/`jsonb`) and is refused
  at both trust boundaries by ONE helper, `adapters::byok::json_has_nul` (every string, object keys included, at
  any depth). (a) Model output: every reply-parsing site calls it before any write — distill
  `parse_distill_output_detailed` (new `DistillParseError::NulCharacter`, detail `nul_character`: the worker
  spends its malformed re-ask budget, then DEAD `FAILED_OUTPUT_SCHEMA`), consolidation `parse_rollup_output`
  (`InvalidInput` → `ROLLUP_OUTPUT_REJECTED`), contribution `parse_json_string` (every key and value of
  `parse_coverage_probe` / `parse_assessment`) and the two `UserContributionAssessmentPort` paths
  (`derive_coverage_probe`, `assess`). Nothing is stripped (ADR-0048). Sites grepped and not changed: `byok`
  `complete_structured` (JSON-validity only, writes nothing; the parsers above are the refusal point),
  `analyze_vision` (no production caller), `inference_rpc` (hex-encodes the bytes). Evidence for the ruling:
  `distill_fairness_live` M3 red `double_spend duplicates=1` — a live reply with `\u0000` passed the parser,
  the write failed `unsupported Unicode escape sequence`, the job settled RETRY `WORKER_DB_ERROR` and the billed
  call was made again. (b) Ingress: `GatewayMcpApplication::validated`, the step every operation key of
  `SUPPORTED_OPERATION_KEYS` passes before its handler (and before any transaction), returns `INVALID_INPUT` for
  any NUL in the decoded arguments. It covers every operation, read routes included (a NUL in a read argument
  was the same INTERNAL); the text-storing writes are `remember.put` (content, subject keys, affect target
  keys), `memory.correct` (text, keys), `memory.confirm` (subject keys), `memory.subject_register` (kind,
  display name, roles), `memory.subject_link_key` (kind, value), `memory.annotate_affect` (target keys).
  Before the fix every one of them answered `INTERNAL` (the database refused the write), and five left rows
  behind (`rows_changed=true`: the confirm-gated pair minted and consumed a token; register, link_key and
  annotate changed a counted row) — the red recorded below. (c) Known limit L21.
- **D-U amendment 2** (ruling 2026-10-02 20:30, P2) 0200: in class mode `requeue-dead` reports every matching
  DEAD job it skipped. The definer returns one more column, `skipped` (NULL on a re-armed row,
  `evidence_gone` / `outbox_settled` on a skipped one — the reasons job mode refuses with), and raises
  `no_dead_job` only when no DEAD job of the class exists; a call whose every match was skipped returns the skip
  rows, re-arms nothing, admits no scheduler row. The receipt gains `skipped: [{job_id, evidence_id,
  last_error_class, attempt_spent, reason}]` (`RequeueSkipReason`, closed), the SUCCESS audit row carries the same
  list under `skipped`, and `outcome` is `requeued` or `nothing_requeued` (exit 0 both). The return shape
  changes, which `CREATE OR REPLACE` cannot do, so 0200 drops and re-creates the function in one transaction:
  still one definer, same `(uuid, uuid, text)` signature, owner, `SECURITY DEFINER`, pinned `search_path` and
  grants (rls-check unchanged).

Main-line rulings E2–E8 (2026-10-02): **E2** the card fault "remove the arbiter lock → >4 in-flight" cannot go
red alone (see T3/T4 below); **E3** the fairness gate is the live cargo test `distill_fairness_live`; **E4**
`contribution_reasoner.rs` joins the allowed files for the failure line only; **E5** 0194 + the
explicit-shadows-inferred affect read; **E6** `xtask/src/e2e_onboard.rs` drops the removed env keys; **E7**
`ops.distill_calls` is approved as a new owner-only table; **E8** 0193 re-arms every Evidence v1 stranded.
**W1** (`tool_choice`) is measured with one live request in slice 3; **W3** (provider request-id lookup)
stays open, non-blocking (L5).

## Migrations (slice 1)

| # | File | Class | Content |
|---|---|---|---|
| 0190 | `0190_distill_dispatch_v2.sql` | EXPAND_CONTRACT, txn | six `ops.jobs` columns + NOT VALID CHECK; four owner-only tables (scheduler and distill_calls ENABLE+FORCE RLS with one owner/tenant policy each); 1 arbiter row + 4 slot rows; scheduler backfill; admit trigger; four definers, EXECUTE `role_private_worker` only |
| 0191 | `0191_idx_jobs_distill_ready.sql` | REVERSIBLE, `transaction = "none"` | `jobs_distill_ready_idx` CONCURRENTLY |
| 0192 | `0192_validate_jobs_dispatch_state.sql` | FORWARD_ONLY, txn | `VALIDATE CONSTRAINT jobs_dispatch_state_check` |
| 0195 | `0195_reasoning_capabilities_tool_calls.sql` | EXPAND_CONTRACT, txn | (review pass) the three §11.2 capability CHECKs (0048, 0128 x2) gain `TOOL_CALLS`, `REASONING_SPLIT`, same names (D-M) |
| 0196 | `0196_distill_call_budget.sql` | EXPAND_CONTRACT, txn | (review pass) `ops.admit_distill_budget` owner definer, EXECUTE `role_private_worker` only; `distill_calls_tenant_begun_idx` (D-T) |
| 0197 | `0197_requeue_dead_distill.sql` | EXPAND_CONTRACT, txn | (ruling R4) `ops.requeue_dead_distill` owner definer, EXECUTE `role_maintenance` only (D-U); applied to `humaux_thread_dev` (`1 applied, 164 already-applied, 165 total`) and `c32_fault_scratch` |
| 0198 | `0198_requeue_dead_distill_reissues_ticket.sql` | EXPAND_CONTRACT, txn | (review P0, D-U amendment) `CREATE OR REPLACE` of 0197's definer: same signature, owner, grants; adds the successor ticket |
| 0200 | `0200_requeue_dead_distill_reports_skips.sql` | EXPAND_CONTRACT, txn | (ruling 2026-10-02 20:30, D-U amendment 2) `DROP` + re-create of the definer with the `skipped` column; same signature, owner, grants. Applied to `humaux_thread_dev` and `c32_fault_scratch` (`1 applied, 167 already-applied, 168 total` on each) |
| 0199 | `0199_distill_slots_all_bound.sql` | EXPAND_CONTRACT, txn | (ruling R5, D-V) `ops.distill_slots_all_bound()` owner definer, EXECUTE `role_private_worker` only. 0198 + 0199 applied to `humaux_thread_dev` and `c32_fault_scratch` (`2 applied, 165 already-applied, 167 total` on each) |

Applied to `humaux_thread_dev` 2026-10-02 (`migrate: pass (3 applied, 157 already-applied, 160 total)`) and to a
fresh scratch database 0001→head (`160 applied`). 0195/0196 (review pass): `migrate: pass (2 applied, 162
already-applied, 164 total)` on dev; 0195 was then edited in a comment only (uncommitted), so its three CHECKs and
its `ops.schema_migrations` row were rolled back by hand and it was re-applied (`1 applied, 163 already-applied`);
`c32_fault_scratch` 0194→0196 after the T23 fault run. §6.2.2 gains the four tables as `—` columns for every
non-owner role (they must be named, or the ops-domain default would expect `arw` grants) and one bullet; rls-check
`check_distill_dispatch_v2_boundary` pins the four definers, the admit trigger, both policies, four slots and one
arbiter.

## Tests and faults (slice 1; red runs on the scratch database `c32_fault_scratch`, never the shared dev DB)

`crates/adapters/tests/distill_dispatch_v2.rs`, real PostgreSQL, role_private_worker through `adapters::jobs`;
every scenario ends with I-SLOT. Foreign tenants' scheduler rows are held `FOR UPDATE` by a fence connection, so
the cross-tenant claim serves only the test's own tenants. 17/17 green on dev and on the scratch DB.

| Test | Fault | Result |
|---|---|---|
| T1 `claim_takes_one_job_binds_one_slot_and_leaves_attempt_alone` | `attempt = j.attempt + 1` in the claim | red |
| T2 `claims_rotate_across_tenants_not_global_fifo` | tenant pick replaced by the globally oldest READY job | red (A,A,A… instead of A,B,C,A,B,C) |
| T3 `at_most_four_slots_are_bound_under_twelve_concurrent_claimers` | slot `FOR UPDATE SKIP LOCKED` + `job_id IS NULL` recheck dropped | **green** — the arbiter lock alone serializes every claim (E2) |
| T3 | the same + the arbiter `FOR UPDATE` dropped | red (more than four claims) |
| T4 `each_successful_claim_advances_the_arbiter_turn_exactly_once` | arbiter `FOR UPDATE` dropped | red (turn delta < claims) |
| T5 `begin_call_counts_one_attempt_writes_one_call_row_and_refuses_a_stale_generation` | `claim_generation = p_claim_generation` dropped | **green** — the slot EXISTS is generation-bound too |
| T5 | the same + the slot EXISTS dropped | red (gen 1's late begin_call admitted) |
| T6 `renew_lease_is_fenced_and_never_passes_hard_deadline` | `LEAST(…, hard_deadline)` → plain lease | red |
| T7 `an_expired_dispatch_intent_becomes_uncertain_and_keeps_its_slot` | T5 branch frees the slot | red |
| T8 `uncertain_reconciles_only_after_hard_deadline_with_its_class` | T6 branch tests `lease_expires_at` | red (reconciled before the deadline) |
| T9 `an_expired_claimed_job_is_ready_again_and_counts_an_abandoned_claim` | `abandoned_claims + 1` dropped | red |
| T10 `finish_frees_the_slot_and_rejects_a_stale_generation` | generation predicate dropped from finish | red |
| T11 `not_ready_past_the_park_age_parks_waiting_key_and_is_rechecked` | park branch removed | red |
| T12 `finish_outcomes_and_dispatch_states_match_the_rust_closed_sets` | Rust variant `PARKED` added to `DistillFinish` | red (SQL 5 values vs Rust 6) |
| T13 `the_enqueue_admits_one_scheduler_row_per_tenant` | `DROP TRIGGER derived_distill_scheduler_admit` | red |
| T14 `runtime_roles_cannot_read_or_write_the_scheduling_tables` | `GRANT SELECT ON ops.provider_slots TO role_private_worker` | red |
| T15 `a_row_locked_in_flight_job_keeps_its_slot_through_a_claim` | heal the slot when the SKIP LOCKED probe finds nothing | red (the fifth job took the in-flight job's slot) |
| T16 `begin_call_refuses_when_the_slot_is_not_bound_to_its_generation` | slot EXISTS dropped from begin_call | red |
| T18 `a_waiting_key_finish_reverts_its_attempt` | WAITING_KEY revert dropped | red |
| rls-check `check_distill_dispatch_v2_boundary` | `GRANT EXECUTE ON ops.begin_call(…) TO role_gateway` | red (`expected EXECUTE=false, actual true`) |
| T21 `the_t6_resend_is_admitted_only_within_the_tenant_budget` (review pass, D-T) | `admit_distill_budget` returns true (`… < p_max_calls OR true`) | red (`left: Admitted(2) right: OverBudget` — the T6 resend admitted) |
| T21b `a_concurrent_admission_of_one_tenant_sees_the_other_ones_call` | tenant advisory lock dropped from `admit_distill_budget` | red (job 2's admission did not wait for the open one) |
| T22 `a_claim_skips_a_slot_row_another_transaction_holds` (review pass, uncaught fault M2) | slot pick `FOR UPDATE SKIP LOCKED` → plain `LIMIT 1` (M2) | red (the claim waited on the held slot row) |
| T22 | M2 + `job_id IS NULL` recheck dropped (M3) | red (same) |
| T22 | M3 alone | **green** — see below |
| T23 `reasoning_capability_checks_match_the_rust_closed_set` (review pass, D-M test 4) | 0195 not applied (`c32_fault_scratch` at 0194) | red (live CHECK 4 values vs Rust 6) |
| rls-check (review pass) | `GRANT EXECUTE ON ops.admit_distill_budget(…) TO role_gateway` | red (`expected EXECUTE=false, actual true`) |

The **green** rows are defence in depth, not dead fences: each guard is redundant with another one that stays in
place, and the combined fault is red. E2's ruling replaces the card's single "arbiter lock" fault with these
pairs. Review pass on the slot guards: the arbiter `FOR UPDATE` serializes every claim, so M2 and M3 were
unreachable by any claim (T3/T4/E8 green under them). T22 reaches the slot pick without the arbiter's help — a
free slot row held by another transaction (a heal or an operator repair) — so M2 is now red on its own. M3 (the
`job_id IS NULL` recheck on the bind) stays green alone **by construction**: the pick already holds the slot row
`FOR UPDATE`, so no transaction can bind it between pick and bind; the recheck is the assertion behind the `40001`
raise, red together with M2 (T22). Kept, not deleted: removing it would need a third full copy of the claim body in
a new migration for no behaviour change.

## Slice 2 — cutover 0193 and the worker

### 0193 `0193_private_worker_off_claim_v1.sql` (EXPAND_CONTRACT, txn)

`REVOKE EXECUTE ON ops.claim_derived_work … FROM role_private_worker`; `CREATE OR REPLACE` of the v1 claim with a
23514 refusal of any `p_job_types` containing `DERIVED_DISTILL` (both arms covered); `CREATE OR REPLACE` of
`ops.claim_derived_work_v2` whose T6 re-queue now backs off on `min(lease · 2^(attempt−1), 300) × (0.75 … 1.25)`
(main-line ruling on E1, guard b: the same capped schedule as every retry, with jitter, never `now`; cap pinned to
`jobs::RETRY_BACKOFF_CAP_SECONDS` by contract test); the `adr0058-cutover` block (legacy PROCESSING reset,
DEAD-with-open-outbox re-arm, backfill of job-less open outbox rows in 0164's key/payload shape, counter reset,
PENDING vs WAITING_KEY by binding presence, scheduler admit). Guard (d) of the E1 ruling (corrected by the review
pass): a T6 resend needs a new claim and a new admission, and since 0196 the admission is the §72.3 tenant budget
check plus `begin_call` in one transaction (D-T, T21); the slice-2 text here claimed the slot/lease/deadline check
was a budget, which it is not. No separate breaker is built here (card 38).

Applied to `humaux_thread_dev` on 2026-10-02 (`migrate: pass (1 applied, 160 already-applied, 161 total)`, pre- and
postcheck ok) with no private-worker running. Row counts (read-only queries before/after):

| Figure | Before | After |
|---|---|---|
| legacy `PROCESSING` distill rows without `dispatch_state` | 0 | 0 |
| DEAD distill jobs with an open outbox row, tenant has a binding | 103 | 0 (re-armed `PENDING`) |
| DEAD distill jobs with an open outbox row, tenant without binding | 435 | 0 (re-armed `WAITING_KEY`) |
| open `EVIDENCE_ACCEPTED` rows with no job | 83 | 0 (backfilled, all `WAITING_KEY`) |
| open distill jobs by status | — | `PENDING 103`, `WAITING_KEY 518` |
| open distill jobs with `attempt > 0` or `abandoned_claims > 0` | 0 | 0 |
| `ops.distill_tenant_scheduler` rows | 3 | 326 |
| DEAD distill jobs | 8 603 | 8 065 |
| stranded Evidence (gate `no_stranded_evidence`) | 621 (red) | 0 (green) |
| PENDING distill jobs on throwaway tenants (gate `no_leaked_distill_jobs`) | 0 | 0 |

### Tests and faults (slice 2; every red run on the scratch database `c32_fault_scratch`, faults restored after)

| Test | Fault | Result |
|---|---|---|
| T17 `v1_claim_refuses_distill_for_every_role` | 0164's body restored in place of 0193's | red |
| T19 `the_cutover_block_rearms_stranded_evidence_with_a_full_budget` | backfill statement removed from the block | red (job-less row has no job) |
| T19 | counter reset removed from the block | red (attempt 12 kept) |
| T20 `t6_requeue_backs_off_on_the_retry_schedule` | 0190's T6 (`now + lease`) | red (30 s, not 120 s ± 25 %) |
| `jobs` contract `v1_claim_refuses_distill_after_the_cutover`, `t6_requeue_uses_the_retry_backoff_cap` | — | read 0193's text |
| E1 `one_hung_provider_call_does_not_cost_siblings_their_leases_or_stall_another_tenant` | heartbeat future removed from the per-job select | red (a concurrent sweep turns the hung job EXECUTION_UNCERTAIN; nothing else breaks because D-G keeps the slot and accepts the same-generation settle) |
| E1 | seats forced to 1 | red (a B row waits behind the hang) |
| E2 `twenty_never_ready_tenants_cannot_delay_the_ready_tenant` | global-FIFO job pick (T2 fault) | red (B claimed past turn 21) |
| E3 `one_tenant_two_reasoning_domains_distills_both_and_fails_none` | take the tenant's oldest open row instead of the job's own | red (`not_ready=1`, one Evidence undistilled) |
| E4 `a_job_whose_domain_does_not_match_its_evidence_returns_to_pending_never_failed` | `evidence_domain` comparison removed | red (outbox FAILED) |
| E5 `a_failing_provider_backs_off_and_dies_after_max_attempts_with_its_class` | `attempt ≥ max_attempts ⇒ DEAD` removed | red |
| E6 `not_ready_never_spends_an_attempt_and_parks_after_the_age` | `begin_call` before binding resolution | red (attempt 1) |
| E7 `a_401_parks_waiting_key_and_never_dies` | WAITING_KEY revert removed from finish | red |
| E8 `two_dispatchers_over_one_backlog_call_each_evidence_once` | job pick without `SKIP LOCKED` and without restated eligibility | **green** |
| E8 | the same + arbiter `FOR UPDATE` dropped | **green** — the tenant row `FOR UPDATE OF t SKIP LOCKED` still serializes claims of one tenant and the slot lock separates claimers; defence in depth like T3/T5 |
| E8 (review pass) | arbiter `FOR UPDATE` + tenant `FOR UPDATE OF t SKIP LOCKED` + job `FOR UPDATE SKIP LOCKED` dropped, eligibility kept | **green** — the restated eligibility in the claim UPDATE re-checks the job row after the lock wait |
| E8 (review pass) | every claim-exclusion layer dropped (the three locks + the restated eligibility) | **red** — two dispatchers claim one job; dispatcher one fails with `23505 provider_slots_job_id_key` on the second bind. E8's "each Evidence once" holds as long as any one layer holds; each layer alone is red in T2/T3/T4/T22 |
| E13 `a_db_error_after_a_counted_call_settles_one_fenced_retry` (review pass, D-E) | the call loop's `Err` propagated with `?` (no settle) | red (job left `PROCESSING`/`DISPATCH_INTENT` holding its slot, `lost_lease=1`) |
| E9 `abandoned_pre_dispatch_claims_reach_dead_at_the_cap` | worker ignores `abandoned_claims` | red |
| E10 `two_resident_workers_never_overlap_calls_for_one_evidence` | T5 branch → READY + slot freed (lease-expiry-frees-slot) | red (`overlaps=3`) |
| E11 `a_success_at_the_end_of_the_http_window_is_kept_not_resent` | deadline around the whole job future | red (resent in gen 2) |
| E12 `a_malformed_reply_whose_reask_cannot_fit_fails_closed_once` | refused re-ask mapped to NOT_READY | red |
| d5b `d5b_a_late_worker_loses_its_lease_writes_nothing_and_its_cost_is_ledgered` | generation predicate dropped in finish | red (W1 `lost_lease=0`, its write landed) |
| d5b | generation + hard-deadline predicates dropped in `renew_lease` | red (W1 `heartbeat_lost=0`) |
| unit `config_rejects_a_hard_deadline_that_cannot_hold_a_call_and_its_reask` | bound weakened to `http_timeout + lease` | red |
| unit `in_flight_is_bounded_by_the_seeded_slot_rows` (ruling R2) | `in_flight = 5` accepted | red (`in_flight=5` accepted) |
| E14 `an_escaped_job_error_is_counted_as_an_error_not_a_lost_lease` (ruling R3) | an escaped error reported as `LeaseLost` | red (`left: (1, 0, 1) right: (1, 1, 0)` — the escaped `WORKER_DB_ERROR` counted `lost_lease`) |
| support `a_reserved_call_overlaps_a_later_generation_until_the_claims_hard_deadline` (ruling R6, `tests/support/double_spend.rs`) | a `RESERVED` row's interval ends at `called_at` | red (`overlaps=0`, expected 1: the killed generation-1 call is invisible to the generation-2 call 30 s later) |
| R4-a `a_requeued_dead_job_is_distilled_by_the_next_pass` (ruling R4, D-U) | the definer leaves the outbox row FAILED (`AND false` on its outbox UPDATE) | red (`left: "FAILED" right: "PENDING"` — the re-armed job has no open outbox row; its claim would settle DONE with no memory) |
| R4-b `requeue_dead_refuses_a_job_that_is_not_dead` (ruling R4) | the `status <> 'DEAD'` refusal dropped | red (`expected a refusal, got Ok(RequeueReceipt { outcome: "requeued", … last_error_class: None …`) — a PENDING job re-armed) |
| R4-c `requeue_dead_refuses_another_tenants_job` (ruling R4) | the `j.tenant_id = p_tenant_id` job filter dropped | red (`left: "evidence_gone" right: "job_not_found"` — the foreign job was selected; only the tenant-scoped outbox lookup stopped the re-arm) |
| R4-d `requeue_dead_refuses_a_job_whose_evidence_is_gone` (ruling R4) | the `v_outbox IS NULL` refusal dropped | red (`expected a refusal, got Ok(RequeueReceipt { outcome: "requeued", …`) |
| maintenance `jobs_requeue_dead_prints_what_it_rearmed_and_refuses_a_rerun` (ruling R4, the CLI) | the CLI takes `--job` when `--error-class` is also given | red (`both selectors: … left: Some(0) right: Some(2)`) |
| d11 `d11_a_requeued_dead_evidence_is_projected_by_a_successor_ticket` (review P0, D-U amendment) | the definer issues no successor (0197's body restored) | red (`one successor ticket: [] left: 0 right: 1`) |
| d11 | the projection worker reads the distill state by the ticket's own `commit_seq` (pre-fix lookup) | red (`left: (0, 1) right: (1, 0)` — the successor failed `no_visible_memory_record` while the re-armed job was open) |
| R5 `a_drain_names_why_it_stopped` (ruling R5, D-V) | `ops.distill_slots_all_bound()` answers `false` | red (`left: (0, Some(NoWork)) right: (0, Some(NoSlot))` — the pass blocked by four bound slots, its job still PENDING, reports `no_work`) |
| unit `add_folds_every_counter` (ruling R5 extension) | `add` does not fold `stopped` | red (`the worse seat reason wins: … stopped=-`) |
| rls-check `check_distill_dispatch_v2_boundary` (ruling R5) | `GRANT EXECUTE ON ops.distill_slots_all_bound() TO role_gateway` | red (`ops.distill_slots_all_bound()/role_gateway: expected EXECUTE=false, actual true`) |
| rls-check `check_distill_dispatch_v2_boundary` (ruling R4) | `GRANT EXECUTE ON ops.requeue_dead_distill(…) TO role_private_worker` | red (`ops.requeue_dead_distill(uuid,uuid,text)/role_private_worker: expected EXECUTE=false, actual true`) |
| byok `every_provider_error_variant_has_its_own_class` | — | pins one class per variant |
| E15 `a_reply_carrying_u0000_is_malformed_never_written_and_never_resent` (ruling R8 a) | the distill parser's `json_has_nul` refusal dropped | red (`write failed: … unsupported Unicode escape sequence`, `outcome=RETRY … error_class=WORKER_DB_ERROR` twice, `double_spend duplicates=1`, `left: 1 right: 0`); green: `malformed reply (nul_character), retry 1/1`, DEAD `FAILED_OUTPUT_SCHEMA` attempt 2, `duplicates=0` |
| unit `a_reply_with_u0000_in_any_string_is_refused_as_nul_character` (ruling R8 a, distill) | the parser's refusal dropped | red (memory text accepted) |
| unit `parse_rollup_output_refuses_u0000_in_any_string` (ruling R8 a, consolidation) | the parser's refusal dropped | red (rollup accepted) |
| unit `contribution_parsers_refuse_u0000_in_any_string` (ruling R8 a, contribution) | the refusal in `parse_json_string` dropped | red (probe and assessment accepted) |
| unit `json_has_nul_finds_u0000_in_any_string_or_key_at_any_depth` (ruling R8, the helper) | `Value::String` answers `false` | red (and the three parser tests above with it) |
| gateway `every_text_storing_write_refuses_u0000_at_ingress` (ruling R8 b, `--include-ignored` file `mcp_gateway`) | no NUL check in `validated` (today's code, run before the fix) | red: `remember.put -> INTERNAL`, `remember.put (subject_keys) -> INTERNAL`, `memory.correct -> INTERNAL rows_changed=true`, `memory.confirm (subject_keys) -> INTERNAL rows_changed=true`, `memory.subject_register -> INTERNAL rows_changed=true`, `memory.subject_link_key -> INTERNAL rows_changed=true`, `memory.annotate_affect -> INTERNAL rows_changed=true`; green: all seven `INVALID_INPUT rows_changed=false` |
| R4-e `requeue_dead_by_class_reports_the_dead_jobs_it_skipped` (ruling 20:30, 0200) | the definer's skip branch `CONTINUE`s without `RETURN NEXT` (0198's behaviour), on `c32_fault_scratch` | red (`left: [] right: [SkippedJob { … reason: EvidenceGone }, SkippedJob { … reason: OutboxSettled }]`) |
| maintenance `jobs_requeue_dead_prints_and_audits_the_dead_jobs_it_skipped` (ruling 20:30, the CLI) | the adapter leaves `skipped` out of the audit metadata | red (`left: Null right: Array [ … "reason": "evidence_gone" …`) |
| rls-check `check_derived_work_dispatch_boundary` (`DERIVED_CLAIM_EXECUTORS = [role_consolidation_worker]`) | `GRANT EXECUTE ON ops.claim_derived_work(…) TO role_private_worker` | red (`expected EXECUTE=false, actual true`) |

Green runs on dev: `distill_dispatch_v2` 20/20, `derived_dispatch_e2e` 22/22 (E10: two subprocesses, 12 Evidence,
`samples=321 max_bound=4 uncertain_seen=true overlaps=0 duplicates=0 unattributed_succeeded=0`, all DEAD
`RETRY_WAIT`, 35 s), `distill_hop_e2e` 10/10 incl. d1 live (one MiniMax call, `completed=1
attempts=1 memories=1`). d1 stays a plain `#[test]` (live whenever `MINIMAX_API_KEY` is set, as before this card): `serial_lane` admits no live-provider lane disposition for an `#[ignore]`.

D-E exits (review pass): before the fix, four error exits after a counted call — the zero-candidate re-ask's
run-close transaction, `active_member` and `begin_distill_call` on a re-ask iteration, `abandon` — returned with
`?`; `run_claimed` mapped that to `LeaseLost` with no settle, so the job kept its slot until `hard_deadline` and a
known outcome came back as `EXECUTION_UNCERTAIN`. `work_job` now runs the loop (`call_loop`) and settles every `Err`
through `settle_after_error` (NOT_READY before any admitted call, `settle_failed_call` after one) in a fresh
transaction; the per-site `settle_after_error` calls inside the loop are gone (one place). E13 drives it with a
`lock_timeout` on the run row (`WORKER_DB_ERROR`, RETRY, slot freed, attempt 1).

Additional limit found in slice 2:
- L16 The T6 log line is printed at the re-claim, not at the sweep (the sweep is SQL inside the claim). A job that
  T6 re-queues and that is never claimed again (parked by readiness) has no line; its `last_error_class` and
  `dispatch_model_call_id` still name it. Upgrade: return the swept rows from the claim.

## Slice 3 — tool-call output, provider failure line, consolidation menu, affect inference (0194)

### W1 — `tool_choice`, measured live (2026-10-02, `api.minimaxi.com`, MiniMax-M3, the existing DNS pin, no proxy)

Four requests, each with `tools=[emit_distillation]` and `"reasoning_split":true`:

| Request | `tool_choice` | HTTP | `finish_reason` | `message` keys | Tool calls |
|---|---|---|---|---|---|
| distill prompt (contract v3 shape) | `{"type":"function","function":{"name":"emit_distillation"}}` | 200, `base_resp.status_code=0` | `tool_calls` | `role, tool_calls, reasoning_content, reasoning_details` — **no `content` key** | 1 × `emit_distillation`, `arguments` = a JSON string of `{"memories":[…]}` (187 chars) |
| distill prompt | none | 200, `base_resp=0` | `tool_calls` | same | 1 × `emit_distillation` (377 chars) |
| neutral prompt "Reply with the single word hello." | named function (as above) | 200, `base_resp=0` | `stop` | `role, content, …` | **none** — `content = "hello"` |
| neutral prompt | none | 200, `base_resp=0` | `stop` | `role, content, …` | none |

Reading: a named-function `tool_choice` is accepted (no refusal, no error text) but **not honoured** — with a
prompt that does not ask for the tool, the forced request answered in `content` exactly like the unforced one.
Decision: **no `tool_choice` is sent**; the contract keeps "call `emit_distillation` exactly once" in the prompt
and the provider accepts exactly one call (zero, two, another name, non-string arguments or `finish_reason =
"length"` ⇒ `FailedOutputSchema`). The reasoning (`reasoning_content`) never reaches the arguments.

### Decisions as built

- **D-M** `StructuredReasoningRequest.output: OutputChannel` = `Content` (v1 body byte for byte — contribution
  and consolidation) | `Tool(name)`. **Provider-neutral (main-line ruling 2026-10-02 10:35, implemented in the
  review pass):** the distill channel is `distill_reasoner::distill_output_channel(descriptor)` — `Tool` only when
  the descriptor declares `TOOL_CALLS`, otherwise `Content` with the v1-shaped prompt ("output JSON only");
  `"reasoning_split":true` is appended to the Tool body only when the descriptor declares `REASONING_SPLIT`;
  `complete_structured` refuses a Tool request without `TOOL_CALLS` before any byte is sent. Both capabilities
  join the §11.2 closed set (`ReasoningCapability`, 0195, Baseline §11.2, contract tests byok unit + T23). The
  worker's descriptor capabilities are the required key `HUMAUX_PRIVATE_WORKER_CAPABILITIES` (comma list, no
  default) and `HUMAUX_PRIVATE_WORKER_KEY_ENV` is required (its `MINIMAX_API_KEY` default was a provider name in
  code, §78.1; `_KEY_ENV` is superseded by ADR-0059 D-I, the credential map `HUMAUX_PRIVATE_WORKER_CREDENTIALS`); profile-driven resolution per binding replaces both in card 33b. The rehearsal profile declares
  `STRUCTURED_OUTPUT,TOOL_CALLS,REASONING_SPLIT` because MiniMax documents `tools` and `reasoning_split` but not
  `response_format: json_schema` (R-32 a, research batch 5) — that is the reason for the rehearsal's values, never
  a branch in code; no non-test comment in `byok.rs` or the reasoners names a vendor. The channel is folded into
  the prompt hash. The envelope parse reads the one call's `arguments` (a tool-call reply has
  no `content`). The ADR-0048 parser stays the validator (the tool call is a transport, never the validation).
  A tool-shape failure spends the worker's malformed re-ask budget exactly like a parser refusal
  (`DistillCallOutcome::Failed(FAILED_OUTPUT_SCHEMA)` → the same branch), then DEAD `FAILED_OUTPUT_SCHEMA`.
  `DISTILL_PROMPT_CONTRACT_VERSION` 2 → 3, `DISTILL_PARSER_VERSION` "1" → "2". No SDK, no implicit retry: one
  `complete_structured` = one transport send (unit test). **Amended by main-line rulings R9 and R10
  (2026-10-03):** the tool channel also accepts the answer object from `content` when the reply carries no
  tool call (R9), and the rehearsal profile's channel is chosen by a live A/B measurement, not by
  documentation (R10) — see "Fifth fix pass" below.
- **D-N** `contribution_reasoner::provider_failure_line` is the one operator line, printed at the three sites
  (contribution, distill `call`, consolidation) before the error is mapped to its static class (the literal
  "user reasoning provider failed" is gone):
  `humaux-reasoning: provider call failed purpose=PRIVATE_DISTILL_TEXT tenant=<uuid> model_call_id=<uuid>
  error_class=RETRY_WAIT latency_ms=120004`. Never the provider message. Ledger `error_class` stays
  `PROVIDER_ERROR`.
- **D-O** `consolidation_prompt_contract(ceiling)` renders rule (4) and the schema enum from
  `distill_reasoner::admissible_classes(ceiling)`; the ceiling is the highest stored class among the run's
  inputs (read before the request); the hash folds the ceiling; `CONSOLIDATION_PROMPT_CONTRACT_VERSION` 1 → 2.
  The v1 seven-class list and its "must NOT rank above" constraint are gone; `validate_rollup_before_publish`
  still rejects over-ceiling (never clamps).
- **D-P** 0194: `private.memory_affects.origin` (`EXPLICIT` default | `DISTILL`) +
  `memory_affects_inferred_confidence_ceiling (origin = 'EXPLICIT' OR confidence_bp <= 5000)`;
  `domain::affect::AffectOrigin` and `INFERRED_CONFIDENCE_CEILING_BP = 5000` are pinned to both CHECKs by a
  contract test. `affect_repo::insert_rows` is the one row issuer; `insert_in_txn` (declared, unchanged
  signature) and `insert_inferred_in_txn` (EMOTION only, no target, no half-life, confidence ≤ ceiling) are its
  two entries. The affect menu is offered only to `DirectUserInput | UserConfirmed` Evidence that declared no
  affect itself (`distill_repo::evidence_has_declared_affects`; a declared one reaches the memory as EXPLICIT
  through the 0157 trigger); the menu flag is folded into the prompt hash. **Amended by the review pass:** the menu
  is also offered to `AuthenticatedAgent` Evidence — the origin the gateway stamps on every `remember.put`
  (`bins/gateway/src/remember.rs`), i.e. the product's only distill ingress; without it scope 6 was unreachable
  in the product (rehearsal 2026-10-02: 0 inferred rows). An agent writing under the user's own credential reports
  the user's session; connector, tool, artifact and external content stay without a menu. Low authority is
  unchanged (confidence ≤ 5 000 bp, shadowed by any EXPLICIT row). Values are basis points, the same wire
  shape `affect_repo::parse_affects` already validates (the menu's "confidence ≤ 0.5" is `confidence ≤ 5000`);
  any key outside `{kind,label,valence,arousal,dominance,intensity,confidence}`, a MOOD, an out-of-range value,
  a target, a wrong shape or a confidence over the ceiling makes the entry invalid; nothing is clamped.
  **Amended by main-line ruling R1 (2026-10-02 18:20):** an invalid entry no longer refuses the reply. The
  `affects` arrays are validated per memory only after the ADR-0048 parser accepted the reply; one invalid entry
  discards every inferred affect of that reply (`DistillReply::affects_dropped`), the memories are written with
  zero DISTILL rows, the re-ask budget is not spent, the dispatch line counts it (`affects_dropped=<n>`) and the
  job line names the class (`outcome=DONE error_class=affect_invalid`); `DistillParseError::AffectInvalid` is
  gone and `DISTILL_PARSER_VERSION` is `3`. Measured cause: with the menu on every `remember.put`, 15 malformed
  replies in ~520 calls and 2 Evidence DEAD on `affect_invalid` twice in a row. Not CLAMP: ADR-0048 rejected
  altering a value the model wrote; here no value is altered — the optional, low-authority annotation is dropped
  whole and the memory, the reason for the call, is kept. `AFFECTS_FOR_MEMORIES_SQL` drops a memory's DISTILL rows when it has an EXPLICIT row, by a window
  `bool_or` over the same single scan (the one-scan test still holds). Inferred rows are written in the distill
  write transaction, after `insert_memory`; a rejected (PENDING) candidate's affects are dropped with it.

### Tests and faults (slice 3; code faults restored after each run, the store fault on `c32_fault_scratch`)

| Test | Fault | Result |
|---|---|---|
| byok `tool_channel_body_carries_one_tool_and_reasoning_split` (descriptor declares both capabilities) | `reasoning_split` omitted | red |
| byok `tool_channel_without_reasoning_split_capability_sends_no_reasoning_split` (review pass, ruling test 2) | field sent unconditionally | red |
| byok `a_tool_request_without_tool_calls_capability_fails_before_the_network` (review pass) | `TOOL_CALLS` gate removed from `complete_structured` | red |
| distill_reasoner `the_output_channel_follows_the_declared_capabilities` (review pass, ruling test 1) | always `Tool` | red |
| e2e `the_worker_refuses_to_boot_without_capabilities_or_key_env` (review pass, ruling test 3) | literal default for `_CAPABILITIES` / old `MINIMAX_API_KEY` default for `_KEY_ENV` | red / red |
| byok `capability_wire_form_matches_migration_check_constraint` (reads 0195's three CHECKs) + T23 (live CHECKs) | 0195 skipped | red (T23) |
| byok `content_channel_body_is_byte_identical_to_v1` | `tools` emitted on Content | red |
| byok `zero_two_or_misnamed_tool_calls_are_a_schema_failure` | `tool_calls[0]` taken without the count check | red |
| byok `finish_reason_length_is_a_schema_failure` | length check dropped | red |
| byok `complete_structured_sends_exactly_once_on_retry_wait` | `send_once` retried once on RetryWait | red |
| byok `every_provider_error_variant_has_its_own_class` | two variants mapped to one class | red |
| contribution_reasoner `provider_failure_line_names_class_latency_and_model_call_id` | `latency_ms=` dropped | red |
| distill_reasoner `affect_menu_is_offered_only_to_user_and_agent_origins` (renamed, review pass) | menu offered to `ExternalContent` / menu without `AuthenticatedAgent` | red / red |
| distill_reasoner `an_out_of_range_inferred_affect_drops_the_affects_keeps_the_memory_never_clamps` (renamed by ruling R1) | ceiling check dropped (a clamp's effect) / invalid entry refuses the reply | red (`confidence over the inferred ceiling` kept its affect) / red (`an invalid affect refused the reply (item_shape)`) |
| consolidation_reasoner `consolidation_menu_is_the_ceiling` | all seven classes rendered | red |
| memory_affects `affect_origin_closed_set_and_inferred_ceiling_match_the_checks` | Rust ceiling 6000 / Rust wire `INFERRED` not in SQL | red / red |
| memory_affects `an_inferred_affect_over_the_ceiling_is_rejected_by_the_store` | `memory_affects_inferred_confidence_ceiling` dropped (scratch DB) | red |
| memory_affects `explicit_annotation_shadows_inferred_rows_in_the_one_read` | `has_explicit` predicate dropped | red |
| d8 `d8_tool_call_reply_goes_through_the_fail_closed_parser` (real provider over a scripted transport) | first of two tool calls accepted | red (a memory written) |
| d9 `d9_inferred_affect_rows_carry_origin_distill_and_explicit_affects_suppress_them` | inferred rows written as EXPLICIT | red |
| d9 (review pass: `AuthenticatedAgent` Evidence, then the recall gate's affect read under role_gateway + `memories_matching(labels_any=[RELIEF])` selects the memory) | menu without `AuthenticatedAgent` | red (the reply's `affects` refused, re-ask) |
| d10 `d10_an_invalid_inferred_affect_drops_the_affects_and_keeps_the_memory` (ruling R1) | dropped affects treated as a malformed reply | red (`malformed_retries=1 attempts=2`, job DEAD, 0 memories) |

## Slice 4 — live gate, rehearsal twins, measurements

### Live gate `distill_fairness_live` (D-Q, ruling E3)

`bins/private-worker/tests/derived_dispatch_e2e.rs::distill_fairness_live` (`#[ignore = "lane(a:shared_db) …"]`,
`HUMAUX_REQUIRE_MINIMAX=1`, `--include-ignored --nocapture`) seeds and tears down its own tenants inside the file's
fence (`dispatch_fence`), drives the real `OpenAiCompatibleProvider` on MiniMax-M3 in process (`dispatch_serve`
with a 100 ms slot/I-SLOT sampler) and, for M3/M5, the real `--distill-serve` binary as subprocesses. The live
MiniMax helpers moved (not copied) from `distill_hop_e2e.rs` to `tests/support/live_minimax.rs`, shared by both
files. Its faults are carried by T2/T3/E1/E2/E3/E10/d5b; this test is the measurement and asserts the card's
bounds. M6 seeds the poison domain's own lane on a model id MiniMax does not serve (`c32-no-such-model`) and runs a
second in-process dispatcher whose provider names that model: route admission compares the worker's descriptor
with the admitted route, so a binding to an unserved model is NOT_READY for the deployment's own provider and only
a provider configured for it produces the real refusal (design §7 M6 assumed otherwise; the measured shape is
recorded here).

### Live gate `distill_poison_live` (M6, moved out of `distill_fairness_live`)

M6 used to run inside `distill_fairness_live` with the poison-lane and the good-lane dispatchers concurrent over
one queue. Each can only release the other's jobs NOT_READY (`configured provider does not match admitted route`),
so which one claims the poison job after a backoff is a race; the main-line chain lost it
(`gates_card32_mainline.red_2202_d1_and_fairness.log`, gate `distill_fairness_live` EXIT 101, `M6: the tenant's
work did not settle`): `M6 poison status=PENDING attempt=2 class=Some("configured provider does not match admitted
route") calls=2 ... good_lane: claimed=34 completed=2 not_ready=32`. A defect of the test construction, not of the
dispatcher (a process serves one provider until card 33b). `serve_until` now runs one dispatcher only.

`distill_poison_live` (`#[ignore = "lane(a:shared_db) …"]`, gate `distill_poison_live`) seeds one tenant with the
poison Evidence (its domain bound to `c32-no-such-model`) and two good Evidence (the tenant's MiniMax-M3 domain),
then runs two phases, never two dispatchers at once (lease 5 s, `max_attempts` 3, in-flight 3):

- **A** only the poison-lane dispatcher until the poison job is DEAD; bound = backoffs between attempts
  (`retry_backoff_seconds(5, 1..3)` = 5 + 10 s) + 3 × (HTTP 60 s + lease 5 s) + one lease = 215 s. Asserts DEAD,
  attempt 3, class `PROVIDER_PERMANENT`, 3 `distill_calls` rows = 3 ledger rows, all `FAILED`, outbox FAILED, and
  the good jobs it claimed and released NOT_READY at attempt 0.
- **B** the good jobs made due, only the good-lane dispatcher; bound = 3 (per-claim call budget) × HTTP 60 s + one
  lease = 185 s. Asserts both good jobs DONE at attempt 1 and the poison job's `claim_generation` unchanged.

| Run (2026-10-02, live MiniMax-M3, debug) | Result |
|---|---|
| green | `M6 poison status=DEAD attempt=3 class=Some("PROVIDER_PERMANENT") calls=3 ledger_rows=3 ledger_failed=3 outbox=FAILED good_done=2/2 good_attempts_after_poison_phase=[0, 0] good_final=[("DONE", 1), ("DONE", 1)] poison_gen_after_a=3 poison_gen_after_b=3 wall_s=34.6 phase_a_s=18.5/215 phase_b_s=16.0/185`, EXIT 0 (`c32_poison_live.green.log`) |
| fault: `attempt >= max_attempts ⇒ DEAD` dropped from `settle_failed_call` (restored after) | red: the claim-side cap (D-F) still kills the job at gen 4, but with `ATTEMPTS_EXHAUSTED` instead of the provider's class — `left: Some("ATTEMPTS_EXHAUSTED") right: Some("PROVIDER_PERMANENT")`, EXIT 101 (`c32_poison_live.red_max_attempts.log`, gate `distill_poison_live_red_recorded`) |

### Measurements (2026-10-02, live MiniMax-M3, tool channel, debug build, run 1 512 s, EXIT 0)

| # | Result |
|---|---|
| M1 | in_flight=1: 50/50 DONE, **0.078 Evidence/s** (641.5 s); in_flight=4: 50/50 DONE, **0.269 Evidence/s** (186.5 s); ratio **3.44**. Both: `lost_lease=0 heartbeat_lost=0`, 1 malformed re-ask per 50. Per-call latency on this day ≈ 7–18 s (tool channel + reasoning), so the old serial capacity line (0.229/s, 2026-09-10, content channel ≈ 1.5 s/call) does not transfer; re-measure per provider/model. |
| M2 | tenant A 200 queued, tenant B 1 row: **B put→DONE 7.7 s** (bound 60 s); A still had 199 open rows at B's DONE. |
| M3 | two `--distill-serve` subprocesses, 100 Evidence over two tenants: 100 DONE / 0 FAILED in 441.1 s; `duplicates=0 overlaps=0 per_gen_over_budget=0 unattributed_succeeded=0 unattributed=[]`; calls per Evidence `{1: 90, 2: 5, 3: 4, 4: 1}` (retries of failed calls in later generations, none after a SUCCEEDED/RESERVED call); 0 Evidence with two completed processing runs. |
| M4 | max bound slots 4 in every sample (M2: 74 samples, M3: 3 927 samples), I-SLOT breaks 0; max overlapping attributed ledger intervals 4 (M2 and M3). |
| M5 | `kill -9` of the worker holding the only job in `DISPATCH_INTENT` (HTTP 20, lease 5, hard deadline 50): the survivor's sweep turned it `EXECUTION_UNCERTAIN` with its slot kept; T6 reconcile 50.1 s after the kill with `last_error_class=EXECUTION_UNCERTAIN`, attempt 1; the killed call's ledger row stays `RESERVED`; the resend's `distill_calls.begun_at` is after the T6 time; final DONE with 2 calls. |
| M6 | poison Evidence: DEAD, attempt 3/3, class `PROVIDER_PERMANENT`, 3 `distill_calls` rows == 3 ledger rows, outbox FAILED; the tenant's other domain 2/2 DONE (65.7 s). Since the main-line race (see `distill_poison_live` above) M6 is measured by `distill_poison_live`; `distill_fairness_live` runs M1–M5, M7, M8. |
| M7 | 5 user-origin emotional Evidence: 5 inferred affect rows (origin DISTILL) on 5 memories, 0 over the 5 000 bp ceiling. |
| M8 | one tenant, two reasoning domains: 6/6 DONE (3 per domain), FAILED 0. |

Review pass (2026-10-02 afternoon, after D-M/D-T/D-E/D-P changes, live MiniMax-M3): run 1 red at M3 only — 99/100
DONE when the 900 s drain bound expired; one Evidence timed out at the 60 s HTTP window four times in a row
(`RETRY_WAIT latency_ms=60003`, backoff 30/60/120 s), every invariant held (`duplicates=0 overlaps=0
per_gen_over_budget=0`, max bound 4, I-SLOT 0). The live Evidence is `DirectUserInput`, so the D-P amendment did not
change its prompt; read as provider latency. Run 2 green (1 149 s): M1 0.083 / 0.379 Evidence/s (ratio 4.55), M2
11.6 s, M3 100/100 in 234.4 s with one call each and all double-spend figures 0, M5 T6 after 49.8 s, M6 DEAD
`PROVIDER_PERMANENT` 3/3, M7 5 inferred rows / 0 over ceiling, M8 6/6.

### Rehearsal twins (`docs/ops/rehearse.sh` step `projection_serve_multi_tenant`, block 2b)

- M2 twin `tenantB_single_row_done_within_60s_while_A_has_queue` (+ `tenantA_has_queue_at_tenantB_put` so a
  drained queue cannot make it vacuous): tenant A queues 40 puts, tenant B puts one.
- M8 twin `one_tenant_two_domains_all_distilled_none_failed`: `xtask e2e-seed --second-domain` gives tenant C a
  second user (through `provisioning::onboard_user`) who owns a second reasoning domain with its own lane and key;
  remember.put resolves the on-behalf-of user's own domain (§11.2.1), so three puts per user land in two domains.
- M7 twin: the first rehearsal run (2026-10-02 impl) read 5 emotional puts, origin `AuthenticatedAgent`, 0
  inferred rows, recall hit 0 — D-P then offered the menu to user origins only. With the review-pass amendment
  (menu for `AuthenticatedAgent`) the twin is graded in the verdict: `rehearsal_inferred_affect_rows > 0`,
  `rehearsal_inferred_affect_row_recalled_by_affect_filter` (recall with `affect.kinds=[EMOTION]` returns a memory
  that carries an inferred row) and `inferred_affect_within_ceiling`. The GATE-LITERAL line is gone.
- The card-27 EXPLAIN gate probed the v1 claim with `DERIVED_DISTILL` (refused by 0193 since slice 2 ⇒ psql
  failed, `n=0 plans`) and the deleted tenant-batch outbox claim; it now probes the v1 claim for consolidation
  only, the job's own outbox take, and the v2 claim's tenant and job picks (6 plans, 0 seq scans on the hot
  tables, run against dev: tenant pick = Index Only Scan on `ops.jobs`, job pick = Index Scan).
- The soak drain is `SOAK_DRAIN=420` (D-K: last chaos step 1 600 s + hard deadline 300 s + resend).
- `distill_once` (the one-shot pass behind step `distill` and `drain_all`) assumed one pass settles every
  seeded Evidence. Under D-F a provider transient settles RETRY (`PENDING`, `RETRY_WAIT`, backoff 30/60 s), so
  it does not: the chain run of 2026-10-02 15:50 got two fast `RETRY_WAIT`s (571/615 ms) for tenant B's only two
  Evidence in the first burst, B had no corpus at `cross_tenant`, and 9 assertions went red
  (`tenant_b_has_a_corpus`, the seven `gov_B_*`, `governance_tenants_exercised`); B's memory landed 12 s later
  at gen 2. `distill_once` now runs up to three further passes while a seeded tenant has a `RETRY_WAIT` retry due
  within 70 s (never for NOT_READY, parked or DEAD jobs). The same run's two other reds
  (`no_ticket_failed_during_outage`, `other_tickets_unaffected`) are one honest DEAD: a tenant-B Evidence whose
  reply and re-ask both carried an invalid inferred affect (`affect_invalid`, ADR-0048 D-D / D-P whole-reply
  refusal) → outbox FAILED → its ticket `FAILED distill_failed`, which those projection witnesses count
  (superseded by ruling R1, D-P: such a reply now keeps its memory and drops its affects). Left as
  is (not narrowed): it is the designed fail-closed path, now reachable on every `remember.put` since the D-P
  amendment; that run saw 15 malformed replies in ~520 calls (impl run before the amendment: 1 in ~180) and
  2 DEAD.

### Test fix found by this slice

`d1_live_distill_writes_memories_and_projection_resolves_ticket` went red once in seven live runs (output not
captured). Its dispatch config was the fake-provider one (`http_timeout_seconds = 5`), so the HTTP cutoff was
`hard_deadline − lease` = 40 s while live calls take 7–18 s and a malformed re-ask doubles that: a slow re-ask is
cut and the job ends UNKNOWN instead of DONE. d1 now sizes the window from the provider's own 120 s timeout (D-K).
Green in the following runs; a provider 429 during d1 would still be red (it is a live test).

Main line, 2026-10-02 (chain `gates_card32_mainline.log`, gate `private_worker_tests` EXIT 101): that last
sentence came true — one live call answered `RETRY_WAIT` after 3.9 s, the job settled `RETRY` (D-F) and the
single-pass assertion `completed == 1` was red with `deferred: 1`. d1 asserted a provider with no transients,
which D-F no longer promises. It now runs up to three passes: each pass must claim the job and leave it DONE or
deferred by a transient (nothing else), a deferred job is made due and claimed again inside its own attempt
budget, and the job must be DONE within the three passes. What d1 gates is unchanged: live memories, PRIMARY
links, the authority ceiling, visibility, the outbox row and the projection ticket.

## Fifth fix pass — main-line rulings R9, R10, R11 (2026-10-03)

Cause (chain run 2, `card32_rehearsal_evidence/chain_run2_red_distill_dead_and_fork_eagain/`): two Evidence went
DEAD `FAILED_OUTPUT_SCHEMA` on the tool channel — `01a0fd6d-8387-…` (tenant B, pst step, `tool_call_shape`
twice) and `01a0fd82-a5f4-…` (tenant B, soak, `nul_character` twice). Each left its remember ticket `FAILED
distill_failed` on tenant B's first workspace stream (seq 20 at 16:23 UTC, seq 68 at 16:46 UTC). The first was
counted by `no_ticket_failed_during_outage` and `other_tickets_unaffected`; it also pinned that stream's §15.4
prefix at 19 from before the soak's first observation to its last, which is the one stream `projection_promoted`
reported (`stalled_by`: first and last `projected` equal while `issued` grew). No `--retire-failed
distill_failed` step ran after the pst step: the harness retired that class only at first activation and in
`drain_all`, never in the soak phase. `xtask/src/soak.rs`'s `digit_free` comment (line 188) records the same
shape once before (a FAILED ticket from a harness artefact pinning a prefix); `projection_promoted` was right
both times and keeps its meaning.

- **D-M amendment (R9)** On `OutputChannel::Tool`, a reply with NO tool call (no `tool_calls` key, `null` or
  `[]`) whose `content`, `<think>` blocks stripped, is a JSON object is returned as the answer with
  `StructuredReasoningResponse::channel_fallback = true` (`byok::tool_arguments`). It goes through the same
  ADR-0048 parser as every other reply; one validator, two places the payload may arrive (not
  tool-call-as-validation, nothing altered). Two or more calls, another function name, non-string arguments, a
  truncated reply, or zero calls with content that is not a JSON object stay `FailedOutputSchema`
  (`tool_call_shape`). The worker counts an accepted, written fallback reply on the dispatch line
  (`channel_fallback=<n>`) and names it on the job line (`channel_fallback=<0|1>`). Contract and parser
  versions are unchanged: the request is byte-identical, and the parser's acceptance rules did not move.
- **D-M amendment (R10)** The rehearsal profile's channel is chosen by measurement. Probe
  `derived_dispatch_e2e::distill_channel_ab_live` (`#[ignore]`, lane `a:shared_db`): the SAME 100 distinct notes
  (facts, preferences, decisions, dates, identifiers, emotional content) through each channel, MiniMax-M3,
  `in_flight` 4, one tenant per channel; first replies classified by the ADR-0048 parser (or `tool_call_shape`),
  latency per provider call. Run 2026-10-03 01:26–01:35 (debug build, 510.6 s, EXIT 0, log
  `c32_r10_channel_ab_live.log`):

  ```
  AB channel=tool n=100 done=100 dead=0 first_reply_malformed=1 by_class=memories_missing:1 channel_fallback=0 affects_dropped=2 calls=101 latency_p50_ms=9407 latency_p95_ms=19244
  AB channel=content n=100 done=99 dead=1 first_reply_malformed=6 by_class=class_unknown:1,confidence_invalid:5 channel_fallback=0 affects_dropped=0 calls=106 latency_p50_ms=7721 latency_p95_ms=18166
  ```

  Rule, applied mechanically: the profile declares `TOOL_CALLS` only if the tool channel's DEAD count and
  first-reply malformed rate are not higher than the content channel's. Tool 0 DEAD / 1 % vs content 1 DEAD /
  6 % (n = 100 each) → **the rehearsal profile keeps `STRUCTURED_OUTPUT,TOOL_CALLS,REASONING_SPLIT`**
  (`docs/ops/rehearse.sh` `PW_CAPABILITIES`, one definition for its five workers; `xtask e2e_onboard`;
  `docs/ops/e2e-seed.md`; the live tests' `REHEARSAL_CAPABILITIES`). The cards 30b/31 content-channel
  `malformed_retries=0` were rehearsal-sized samples; on the same 100 texts the content channel measured
  worse. Both channels keep their tests. Run 2's tool-channel rate (6 malformed first replies in ~450 calls,
  2 DEAD) is the same order as this probe's 1/100; R9 did not fire in the probe (`channel_fallback=0`), so its
  value is the class of reply it rescues, not a measured rate yet.
- **R11 (rehearsal grading)** `no_ticket_failed_during_outage` and `other_tickets_unaffected` count tickets
  FAILED by the projection path (`error_class` other than `distill_failed`). A new graded line
  `distill_dead(n=<Evidence of the seeded tenants>, dead=<DEAD DERIVED_DISTILL jobs>, classes=<last_error_class
  [/parser reason]:count,...>)` asserts `dead == 0` for the whole run (after the soak), so a distill death is red
  under its own name. `dead` is the database's `count(*)`, never the number of DEAD rows a query printed: a query
  that fails (docker exec, psql, the fork `EAGAIN` run 2 hit at 834 s) leaves `dead` empty and the line red, where
  counting printed rows read it as `dead=0` and passed (review P1, fail-open).
  The soak phase now has the operator act the runbook prescribes for a DEAD distill whose
  cause the run cannot fix: the audited `projection-serve --retire-failed distill_failed` for each soak family,
  once before the timed window and every `SOAK_RETIRE_SECS` (60) during it (`soak-operator.log`). Only
  `distill_failed` is retired; any other FAILED class still pins the prefix and `projection_promoted` grades it.

### Tests and faults (fifth fix pass; code faults restored after each run)

| Test | Fault | Result |
|---|---|---|
| byok `a_tool_reply_without_a_tool_call_answers_from_its_content_object` (R9) | no fallback (zero calls always `FailedOutputSchema`) | red (`c32_r9_unit.red_no_fallback.log`, EXIT 101); green |
| byok `zero_two_or_misnamed_tool_calls_are_a_schema_failure` (R9 cases added: two calls / other name with a valid `content` object, zero calls with non-JSON / array / no content) | take the first of two tool calls | red at `two calls` (`c32_r9_two_calls.red_take_first.log`, EXIT 101); green |
| d12 `d12_a_tool_channel_reply_in_content_is_parsed_and_accepted_once` (R9, real provider over a scripted transport) | no fallback | red (`sends=2 status=DEAD attempt=2 class=FAILED_OUTPUT_SCHEMA memories=0 … malformed_retries=1 … channel_fallback=0`, `left: 2 right: 1`, `c32_r9_e2e.red_no_fallback.log`); green: `sends=1 status=DONE attempt=1 memories=1 … channel_fallback=1` |
| d8 `d8_tool_call_reply_goes_through_the_fail_closed_parser` (unchanged) | — | green with R9 (two calls still DEAD after one re-ask) |
| `add_folds_every_counter` (18 counters) | `channel_fallback` not folded in `add` | red (`channel_fallback=0`, `left: 17 right: 18`, `c32_r9_fold.red_not_folded.log`); green |
| live `distill_channel_ab_live` (R10) | measurement, no fault | both runs finished; the two `AB` lines above |
| harness `c32_r11_distill_dead_fail_closed.sh` (R11, review P1): cuts the `distill_dead` block and `assert_eq` out of `rehearse.sh`, stubs `PGQ` as down / 2 DEAD / 0 DEAD | `dead` counted from the printed DEAD rows (`grep -c .`, the pre-fix line) | red (`case=down WRONG: got 'ASSERTION PASS distill_dead(n=, dead=0, classes=-): 0'`, EXIT 1, `c32_r11_distill_dead.red_rows_counted.log`); green: `case=down ok: ASSERTION FAIL distill_dead(n=, dead=, …)`, `dead2` FAIL `dead=2`, `clean` PASS, EXIT 0 (`c32_r11_distill_dead.green.log`). The `count(*)` read-only on `humaux_thread_dev` for run 2's tenant B answers `2` |

#### R11 amendment (main line, 2026-10-03 02:20): `distill_dead` is a bound, not zero

The first wording of R11 asserted `dead == 0`. Measured on the live rehearsal model: 0 dead in the no-soak
rehearsal after R9 (n = 147), 2 in main-line chain run 2 before R9 (about 450 calls), 0 and 1 per 100 in the two
arms of the A/B probe. A hard zero would make the chain red on model variance and teach operators to re-run
until green. The assertion is therefore `dead <= n / 100` (the soak's own 1 % op-failure bound), fail-closed
when the database does not answer, and it prints `n`, `dead`, the bound and every class
(`c32_r11_distill_dead_fail_closed.sh`: five cases, down / 2 of 5 / 0 of 5 / 3 of 300 / 4 of 300).
Known limit **L23**: an Evidence whose reply is refused twice is DEAD until an operator runs
`humaux-maintenance jobs requeue-dead`; nothing re-drives it automatically. Upgrade path (filed to card 35):
the maintenance daemon re-drives a `FAILED_OUTPUT_SCHEMA` death once after a cool-down, audited, and the
re-ask of a profile that declares both channels may use the other channel (the A/B probe shows the two channels
fail on different classes).

**Addendum (card 35 S6, 2026-10-04; ADR-0062 D-P / D-J).** L23 is closed for the same channel: the daemon's task
`redrive` calls `ops.auto_redrive_schema_failed(uuid, interval, integer)` (0221), which re-arms a DEAD
`FAILED_OUTPUT_SCHEMA` distill job once ever (`ops.jobs.auto_redrives`), after a cool-down measured from its last
counted provider call, through this ADR's R4 body (`ops.requeue_dead_distill`, unchanged), with one §77
`DISTILL_AUTO_REDRIVE` row; `PROVIDER_PERMANENT`, `ATTEMPTS_EXHAUSTED`, `PRE_DISPATCH_ABANDONED` and
`EXECUTION_UNCERTAIN` are never selected, and an always-refusing provider costs exactly 2 + 2 calls. The re-ask keeps
the route's channel (ADR-0060 D-D; the other-channel option stays an upgrade, ADR-0062 L8). The terminal-jobs purge
door keeps every R4 handle: a DEAD distill job is purgeable iff `requeue_dead_distill` would refuse it (its
`EVIDENCE_ACCEPTED` row is DONE or gone), so neither the operator's door nor the daemon can lose a job to retention.

## Known limits

- L1 Consolidation stays on v1 (global FIFO, attempt at claim, no slot): private-reasoning in-flight across both
  hops can be 4 + 1. Upgrade: consolidation claims through the same slots.
- L2 Fairness by claim count, not cost (§32 asks cost-weighted DRR). Upgrade: deficit counter on the scheduler
  row charged with ledger tokens.
- L3 One tenant can hold all four slots. Upgrade: per-tenant slot cap inside the claim.
- L4 The tenant pick scans the scheduler in turn order with one EXISTS probe per row (`// ponytail:` in 0190).
  Upgrade: an `eligible_at` column maintained by finish and the admit trigger.
- L5 T6 may resend a call whose provider-side cost was already incurred (bounded, classed). Upgrade: a provider
  request-id lookup before reconcile (W3).
- L11 No `claim_request_id`: a lost claim response leaks one slot for one lease.
- L13 (slice 2) ledger reservation and `begin_call` are two transactions.
- L15 `ops.distill_calls` grows one row per admitted call and is deleted only with its job (CASCADE).
- L9 (slice 3) The ADR-0048 parser reads replies through `serde_json::Value`, which keeps the last of duplicate
  keys (`// ponytail:` at the parser). Upgrade: a duplicate-rejecting visitor.
- L18 (review pass, D-T) The §72.3 distill budget counts admitted requests, not tokens or cost, and is one
  deployment-wide window/limit for every tenant. Upgrade: per-plan limits from the entitlement snapshot and
  token weighting from the ledger, with the card-38 breaker (`// ponytail:` on `jobs::DistillCallBudget`).
- L19 (ruling R4) `requeue-dead`'s `evidence_gone` test is the Evidence's outbox row (RLS-readable under the tenant
  GUC); an Evidence whose `private.events` row vanished while its outbox row stayed would be re-armed and die
  again (no code path deletes an events row today). Upgrade: the definer installs the Evidence's visibility
  context and probes `private.events` when erasure lands.
- L20 (D-U amendment) The successor ticket goes to the remember ticket's own `projection_version`; a version
  retired since then is not followed (`// ponytail:` in 0198). Upgrade: the door takes the served version from
  the operator's process configuration (PG does not record it).
- L21 (ruling R8 c) A transient database error in the write leg AFTER a succeeded provider call settles RETRY
  (`settle_after_error` → `settle_failed_call`) and the next claim calls the provider again: one extra billed
  call, counted in `attempt` and visible as a `double_spend` duplicate (`// ponytail:` at the settle site in
  `bins/private-worker/src/distill.rs`). R8 removes the one deterministic cause found live (U+0000); others
  (lock timeout, connection loss) stay. Upgrade: retry the write leg in place with the parsed reply, inside
  the claim's hard deadline. Not built in card 32.
- L22 (ruling R10) The channel choice rests on one n = 100 run per channel on one model; it is re-measured, not
  assumed, when the model, the prompt contract or the provider changes. Upgrade: per-binding capability
  profiles (card 33b) carry their own measured channel.
- L17 (slice 3) The model's emotion inference is not validated beyond the menu (closed sets, ranges, ceiling);
  a wrong-but-in-range inference is stored as a low-authority DISTILL row that any explicit annotation of the
  memory shadows. Upgrade: an explicit-feedback loop that retires contradicted inferences.

## Rejected

CLAMP; pure retry; global FIFO; row_number-only batch pick; process-only semaphore; count<4-then-insert;
lease-expiry-frees-slot; tool-call-as-validation; implicit provider switch; a second job table; new
`ops.jobs.status` values; a NOLOGIN function owner (D-I); a `max_attempts` column (D-F).
