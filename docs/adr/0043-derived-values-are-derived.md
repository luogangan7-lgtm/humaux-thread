# ADR-0043 — A value that must be equal in three places is derived in one

- Status: Accepted
- Date: 2026-09-15
- Card: 21
- Spec: §15.1 (stream ticket identity), §16.1 / §16.1.1 (processing input fingerprint), §17 /
  §17.3 (retrieval families and placement), §7.3 / §7.4 (egress permit and disclosure ledger),
  §48.0① / G80-22 (payload digest construction point), §16.2 / G80-4 (serving-version read
  route), §6.2.2 / §48.2 (grant matrix — `control.confirm_tokens` DELETE), §52 D-B
  (`ConflictReason`), §33.10 rule 9 (confirm gate), §46 (forward-fix migration 0169),
  §78.1 / §78.2 (no literals, closed enums), §57.1 / ADR-0006 (`not_applicable` discipline).
- Supersedes nothing. Closes ADR-0016's registered 已知局限 ("指纹不能只从 run 行重算 …
  根因修法在 domain … 超出本卡文件范围，未做"), card 18's `--visible-shadow` debt and its G80-4
  needle note, card 16's `lost_lease=6` soak finding and its consolidation-side error-class
  twin, card 1's review P2 (`control.confirm_tokens` has no retention), and card 12's untyped
  membership conflict reason.

## Context

Three reviewer-P1 findings from the deployment report, one defect class: **a value that should
be derived from a source of truth was instead hardcoded, nil, or recomputed from a re-rendered
representation.**

1. **The ticket family triple.** `projection.stream_log`'s `(domain, projection_kind,
   projection_version)` is how an issuer and a resolver find each other. It existed in three
   hand-aligned copies — three literals in `consolidate_repo::publish_rollup`, three
   `HUMAUX_RETRIEVAL_WORKER_{DOMAIN,PROJECTION_KIND,PROJECTION_VERSION}` env values, and a
   `DOMAIN=…; PKIND=…; PVER=…` line in `rehearse.sh`. Mis-set one and **nothing fails**: the
   worker polls a stream nobody writes, forever, with no error and no metric. There is no
   runtime signal for this failure mode, which is why it has to be a compile-time one.
2. **`ProcessorId(Uuid::nil())`.** The retrieval worker built its embedding provider with the
   nil UUID, so every `ops.data_disclosures` row it wrote carried `processor_id` all-zeros.
   §7.4's ledger exists to answer "which processor received this private data"; a column that
   is the same constant on every row answers nothing, and §7.3's deletion/revocation
   propagation — which queries by processor — had nothing to find.
3. **`source_hash` was not recomputable from storage.** The §16.1 distill fingerprint hashed
   `payload_sha256(canonical jsonb of events.payload)` — a digest of a *re-rendering* — while
   the same `private.processing_runs` row stored `evidence_objects.payload_sha256`, the
   Evidence's real §8.1 anchor, in `evidence_payload_sha256[]`. Two different values. Migration
   0064's column comment ("the set `source_hash` hashes") was therefore false, and the one
   property §16.1.1 asks of an audit fingerprint — reproduce it from the persisted row — did
   not hold. The e2e's `assert_fingerprint_recomputes` had to reach back into `private.events`
   to reproduce the hash, and said so in its own rustdoc.

The root cause of (3) is worth naming because it is a gate that mis-fired: `EvidencePayloadSha256`
had exactly one construction point (§48.0① G80-22), a **hasher**. So the only way to obtain an
anchor was to hash something, and the fingerprint hashed the only bytes it still had. G80-22
exists to stop a second crate deciding the §8.1 *encoding*; it was written as "exactly one
construction site", which also outlawed **reading back** a digest that is already persisted —
an operation that decides no encoding at all. A correctly-motivated gate, stated one notch too
strong, produced a broken fingerprint.

## Decisions

### D1 — `domain::ticket_family::TicketFamily` is the only place the triple is spelled

A closed enum in Domain (§78.2, one variant today) with `domain()` / `projection_kind()` /
`projection_version()`, plus `collection_name()` **derived** as `{domain}_{version}`.
`adapters::qdrant::RetrievalFamily::ticket_family()` maps §17's retrieval family onto it, and
`retrieval_family_matches_ticket_family` asserts §17's own collection name equals the derived
one — so the ticket triple and the Qdrant collection are provably one identity, not two
spellings that happen to agree.

Consumers:

- `consolidate_repo::publish_rollup`: three literals → `TicketFamily::PrivateMemory`.
- `bins/retrieval-worker`: the three `required(...)` env reads are **gone**. The triple comes
  from `PROJECTION_FAMILY` (`RetrievalFamily::PrivateMemoryV1`), the same constant that already
  fed the §17.3 placement five lines below — so the family this process indexes into and the
  family it polls tickets for cannot differ.
- The gateway's `HUMAUX_GATEWAY_REMEMBER_*` keys stay (its write policy is deployment
  configuration, §78.1), but `xtask e2e-seed` now **emits** them from `TicketFamily`, and
  `rehearse.sh` takes `DOMAIN/PKIND/PVER` from that output instead of typing them. The value is
  generated from the source of truth rather than hand-aligned with it.

**Gate:** `architecture-check`'s new `§78.1/§15.1 (ticket-family triple spelled once)` scans
production source (`…/src/…`, `#[cfg(test)]` stripped) for the literals `"private_memory"` /
`"PRIVATE_MEMORY"` outside the home module. Test files may still spell them — a fixture that
writes the literal and asserts production derives the same value is a *positive control*, and
`qdrant.rs`, `bins/retrieval-worker` and `distill_hop_e2e` all carry exactly that assertion.
`"v1"` is deliberately **not** scanned (a dozen unrelated versions spell it, and a scan that
matched it would be noise); the version cannot drift alone because `collection_name()` derives
from it and is pinned against §17.

**Fault injection (runs in CI, fixture tree, never a mutation of the real repo):**
`ticket_family_gate_red_when_a_literal_comes_back` puts `"private_memory".to_owned()` back into
a fake `consolidate_repo.rs` ⇒ red and named; removing it ⇒ green again (a red that cannot go
green is not a gate). `ticket_family_gate_is_not_applicable_without_the_closed_set` pins
ADR-0006: NA names the missing object and does not coincide with the violation.

### D2 — the retrieval worker has its own §7 egress identity

`HUMAUX_RETRIEVAL_WORKER_EGRESS_PROCESSOR_ID`, read the way the private worker already reads
`HUMAUX_PRIVATE_WORKER_EGRESS_PROCESSOR_ID` (deployment identity, never tenant data), and
**emitted by `xtask e2e-seed` from the same `--processor-id`** so the deployment sets one value,
not two. `Uuid::nil()` is refused explicitly rather than accepted as "unset" — nil *is* the
unattributed state this fixes, and §78.1 gives it no default.

`two_workers_get_distinct_non_nil_processor_ids` pins the parser; the rehearsal pins the rows
(`disclosure_processor_never_nil` + `disclosure_processor_is_the_seeded_identity`, asserted over
this tenant's whole `ops.data_disclosures` table, not one row) — **and that rehearsal is now
actually run; see D6, which is the correction of a sentence this ADR previously wrote in the
past tense about a script nobody had executed.**

The card's second half — "a checkpoint written by one worker is attributed to it" — was *not*
delivered by the disclosure rows, and this ADR originally substituted one for the other without
saying so. `projection.stream_checkpoints` had no processor/worker/lease-owner column at all, so
the assertion was unwritable rather than merely unwritten. Migration **0171** adds
`projection_processor_id`; D6 has the shape.

The nil `DisclosureSource::Memory` fallback next to it is left in place and re-documented: it is
reachable only from `embed_cards` (no memory ids), and this binary's projection path calls
`embed_cards_for_memories`, which builds one real source per card.

### D3 — `source_hash` reads the persisted digest back

`EvidencePayloadSha256::from_stored_digest(&[u8]) -> Option<Self>` in Domain: validates width,
adopts the bytes, **hashes nothing**. `distill::infer_claimed`'s evidence axis now reads
`evidence.payload_sha256` (the column the run row records) instead of re-rendering
`events.payload`. `source_hash` is now a pure function of columns that are all on the run row.

**G80-22's 口径 is widened in the same change** (§48.0① and the §80.1 registry table updated):
not "exactly one construction site" but "**every** construction site is in
`crates/domain/src/evidence.rs`, and there are exactly two — one hasher, one read-back — and the
read-back must not hash". The last clause is a structural probe on the function body
(`Sha256` / `digest(` ⇒ red), because the moment the read-back hashes it becomes a second
encoding and the premise of the widening is gone. Two fixture-tree 注错 tests cover a foreign
construction site and a re-hashing read-back.

`assert_fingerprint_recomputes` no longer touches `private.events`: it recomputes from the run
row alone, and then — every run, not in a comment — perturbs one persisted axis byte and asserts
the fingerprint disagrees. A check that only ever sees the matching case cannot distinguish
"recomputable" from "always equal".

### D4 — folded debts

- **`--visible-shadow` is a hard error** (card 18). It was warned-about-and-ignored while
  `rehearse.sh` kept passing it a PostgreSQL row count; an ignored flag is indistinguishable,
  from the operator's side, from an honoured one. Both `rehearse.sh` copies drop it (and the
  `POINTS` query that existed only to feed it). Three parser tests, plus a rehearsal assertion
  that the flag has not come back — written so the assertion line cannot match itself.
- **G80-4's needle is a real call-site match.** It counted the bare substring
  `serving_version(`, and avoided a false red only because `walk_workspace_rs` strips
  `#[cfg(test)]` modules — something G80-4 does not control, and which does not apply to
  `crates/*/tests/*.rs` at all. A judgement that survives only because of an unrelated filter is
  not a judgement. `count_call_sites` now requires a left word boundary and excludes `fn NAME(`
  definitions in any file; two self-tests cover a definition-shaped test function and a
  lookalike identifier.
- **`lost_lease=6` (card 16 soak).** `dispatch_pass` heartbeat the `ops.jobs` lease once per
  job, before the first row; `run_once` then worked N evidence rows serially, each with a real
  provider round trip. Lease was `LEASE_SECS`; work was `rows × provider_latency`; the two were
  never related. `run_once` now takes the lease and **renews it per row**, so pass duration
  stops being a function of batch size. The alternative the debt offered — sizing
  `DISTILL_BATCH × DISTILL_JOB_BATCH` from `LEASE_SECS` and a *measured* per-row latency — was
  rejected: it would be a fourth derived value to keep hand-aligned, against a provider whose
  latency is not ours to fix. `DistillPassReport.heartbeats` counts the renewals, because "the
  heartbeat happened" is the only observable difference between this and the shape that burned
  the budget. Two DB tests (`adapters::jobs_claim`): 1.6 s of work on a 1 s lease settles with
  `lost_lease = 0`, and the same pass without per-row heartbeats becomes re-claimable, gets
  re-claimed, and settles nothing. The negative control is what makes the first test mean
  anything. **Those two tests drive `jobs::heartbeat_derived_private` in a hand-rolled loop:
  they prove `ops.claim_derived_work`'s predicate, not the call site in `run_once`** — deleting
  `Some(&lease)` from `dispatch_pass` left every test in the tree green, and `heartbeats` was
  read by nothing. D6 adds the test that fails when the fix is reverted.
- **The consolidation worker logs an error class.** `bins/consolidation-worker`'s catch-all arm
  printed `RunOnceError`'s `Display`, which for the `Reasoning` arm delegates to
  `PrivateReasoningError`'s redacted one ("…fingerprint=abcd1234"). That was the end of the road
  for the only information an operator can act on — the same defect card 16 fixed on the distill
  side. `error_class_for_log` uses `PrivateReasoningError::class()`; other arms keep their own
  `Display`.
- **`control.confirm_tokens` retention** (migration 0169, EXPAND_ONLY). 0148 gave the table no
  way to shrink: no DELETE to anybody, no `expires_at` index, no sweep — unconditional growth on
  a table whose rows are worthless the moment they expire. 0169 adds two partial indexes, a
  DELETE grant to `role_maintenance` **alone** (the minter still cannot erase its own audit
  trail, §37.2), and `control.sweep_confirm_tokens(interval)` — owner SECURITY DEFINER,
  `search_path` pinned, EXECUTE to `role_maintenance` only. The retention rule is stated once,
  in that function: expired **and** (never consumed, or consumed longer ago than the caller's
  audit-retention interval — consumed-recently is kept because §9 must still answer "which token
  authorized this destructive call"). The interval is a parameter, not a literal (§78.1:
  retention is deployment policy). The function is subject to `confirm_tokens_tenant` like every
  other access, so it is per-tenant by construction and deletes nothing without a tenant context.
  §6.2.2 row and `xtask/src/rls_check.rs` MATRIX updated in the same change (§48.2).

  **0169 also granted `role_maintenance` a table-level `DELETE`, and migration 0170 revokes
  it.** §6.2.1's 全域禁动词 is a *global* hard constraint with no exceptions — no non-owner role
  holds DELETE or TRUNCATE on any table — and `rls-check`'s `check_forbidden_verbs` enumerates
  `role_table_grants` for exactly that and went red. The grant was also unnecessary: the DELETE
  inside an owner SECURITY DEFINER function executes as the owner, so EXECUTE on the function is
  the whole permission surface. §46 makes an applied migration immutable, hence a second number
  rather than an edit. Recording it because the lesson generalizes: **a §6.2.2 cell cannot
  license a verb §6.2.1 bans globally** — the per-table matrix is a narrowing of the global
  rule, never an exemption from it, and the two gates are ordered that way on purpose.
- **Typed `ConflictReason` for membership** (card 12). `MembershipConflict` carried three
  SCREAMING_SNAKE strings of its own and no numeric reason, while `ConflictReason` is the closed
  set that owns exactly that taxonomy — two registries for one wire contract, the second of
  which a client cannot switch on. `MembershipConflict::reason()` maps the three refusals onto
  `ALREADY_IN_STATE` (1201, reused rather than twinned), `TRANSITION_NOT_ALLOWED` (1203) and
  `LAST_OWNER` (1204); `as_str()` is now **derived** from that reason's label instead of being a
  second `match` with the same three strings. Same-card theme: the labels agreed, which is the
  only reason nobody noticed there were two tables.

### D6 — fix pass: the four claims that had no witness (reviewer P1s, 2026-09-16)

Card 21's first delivery wrote four things down as done that nothing observed. Each is fixed the
same way — by making the claim true and giving it a witness that fails when the fix is reverted —
not by softening the claim.

**(1) The rehearsal was never run.** Every card-21 acceptance assertion on the live deployment
(`disclosure_rows_written`, `disclosure_processor_never_nil`,
`disclosure_processor_is_the_seeded_identity`, `ticket_family_agrees_across_processes`,
`no_visible_shadow_flag_left_in_this_script`) existed only as lines in `rehearse.sh`; no log in
the evidence tree had ever executed one, and the newest evidence directory belonged to card 20.
The startup contract the card introduced — `rehearse.sh` hard-exiting 2 when
`HUMAUX_RETRIEVAL_WORKER_EGRESS_PROCESSOR_ID` is absent, and DOMAIN/PKIND/PVER coming from the
seed rather than the script — was equally unvalidated. The rehearsal is run in this pass and its
log is the evidence. Two things changed in the script itself: the new checkpoint-attribution
assertions (below), and **the script now exits non-zero when any assertion fails**. Without that
last line, "the rehearsal ran" and "the rehearsal passed" were indistinguishable from outside —
which is precisely how a table of unrun assertions came to be reported as delivered.

**(2) The per-row lease heartbeat had no test.** `distill::run_once`'s heartbeat is gated on
`job_lease` being `Some`; the only production caller passed it and the only test caller passed
`None`. `derived_dispatch_e2e::a_pass_longer_than_its_lease_heartbeats_per_row_and_settles_without_losing_it`
drives the real `dispatch_pass` against a real `ops.jobs` lease: three evidence rows in one
tenant (`job_batch: 1`, so one lease covers the whole pass), a 700 ms provider round trip, a 1 s
lease — 5 s of work on a 3 s lease, **with a second claimer running concurrently** at t≈3.5 s.

The competitor is the point, not decoration. `jobs::settle_derived_private`'s predicate is
`(job_id, tenant_id, lease_owner, attempt, status='PROCESSING')` and never reads
`lease_expires_at`, so a pass that overruns its lease with an empty queue still settles cleanly —
a test without a competitor would have measured the counter and nothing else. What the soak hit
was another dispatcher reclaiming the expired lease and both workers paying the provider for the
same rows; that is what this reproduces. The test asserts `heartbeats == 5`, `lost_lease == 0` on
both report levels, that the job settled `DONE`, and that the thief's claim does not contain the
in-flight job.

Measured revert (`Some(&lease)` → `None`, 2026-09-16): `heartbeats` 0 instead of 5 and
`DistillDispatchReport.lost_lease` 1 instead of 0 — the thief took the job, five rows were
distilled by a worker that could no longer commit them. `FakeProvider` grew one field (`delay`)
to make a pass take real time without a network; that is the whole production-side surface of
this test.

**(3) The confirm-token sweep had no named test and no caller.**
`control.sweep_confirm_tokens` shipped with its predicate verified by the migration manifests
only — they assert the function *exists* with the right owner, `search_path` and EXECUTE set, and
say nothing about what it deletes — and with no caller anywhere, so the unbounded-growth defect it
was written for was still unfixed on any running deployment. Both halves now exist:

  * `adapters::confirm_token_retention::the_sweep_deletes_only_expired_tokens_no_longer_wanted_as_audit`
    covers the predicate's whole truth table in one pass — expired-unconsumed deleted,
    expired-consumed-long-ago deleted, expired-but-consumed-recently kept (the §9 audit answer
    outlives the call), unexpired kept, another tenant's expired row untouched (FORCE RLS scopes
    the definer's own DELETE). It then re-sweeps to prove convergence, and sweeps again at zero
    retention to prove the interval is the caller's policy and not a literal inside the function.
  * `the_maintenance_role_cannot_delete_a_confirm_token_directly` pins 0170's retraction: an
    operator who skips the function and types the predicate by hand gets `42501`, from the
    database, not from convention.
  * `confirm_token_repo::sweep_expired` + `cargo xtask sweep-confirm-tokens
    --consumed-retention-secs <n> (--tenant <uuid> … | --all-tenants)` is the runnable door, the
    same one the test goes through. `--consumed-retention-secs` has no default (§78.1: retention
    is deployment policy). Cross-tenant enumeration reads `control.tenants` through the
    admin DSN, because `role_maintenance` cannot see across tenants — the same loop-the-tenants
    shape `stream_repo::sweep_lost` documents. Smoke-run on this development database
    (2026-09-17, `--all-tenants --consumed-retention-secs 2592000`): 4180 tenants swept, **2 rows
    deleted** — the first time the table has ever shrunk, which is the defect card 1's P2 raised
    and card 21 had until now only described. Both argument refusals were exercised in the same
    run (no retention ⇒ exit 2; neither `--all-tenants` nor `--tenant` ⇒ exit 2).

**(4) "A checkpoint is attributed to the worker that wrote it" was unwritable.**
`projection.stream_checkpoints` (0007) carried tenant/scope/family, four highwaters, the two
read-routing flags and `updated_at` — nothing that could name a process. The delivery claimed the
attribution and this ADR quietly substituted `ops.data_disclosures` rows for it. Migration
**0171** (EXPAND_ONLY) adds `projection_processor_id uuid`, nullable and un-backfilled (a row
advanced before 0171 reads NULL, which is its truth — inventing attribution would be worse than
admitting none), with the column-level UPDATE granted to `role_retrieval_worker` alone, extending
the cell it already holds for the three highwaters. `stream_repo::advance_prefix` takes a
`ProcessorId` and writes it **in the same statement as the watermark**, under the same monotonic
`WHERE`: the process that may write the number is the process that signs it, and the two can
never be written at different times by different callers. `ProjectionWorkerDeps` carries the id;
`bins/retrieval-worker` passes `egress_processor_id()` — the *same* §7.4 identity its embedding
provider discloses under, not a fourth env var to hand-align.

Witnesses: `adapters::stream_repo::a_checkpoint_carries_the_processor_id_of_the_worker_that_advanced_it`
(worker A advances, the row names A; worker B advances further, the row names B; before either,
the row names nobody) and, live, the rehearsal's `checkpoints_advanced` +
`checkpoint_attributed_to_the_worker_that_advanced_it`, which assert over this tenant's whole
checkpoint table that every advanced row carries the seeded identity.

Why this is not the second source of truth §48.0④ bans: "which process last advanced this
watermark" cannot be recomputed from anything else in the schema, unlike `open_gap_count` or
`deleted_count`. It is the same shape as 0167's `retired_at` / `retired_by` on
`projection.stream_log` — an audit fact about a write, recorded where the write happens.

## Known limits and upgrade signals

- **`TicketFamily` has one variant.** `ALL` and `from_triple` exist so a second family cannot be
  added without every enumerating consumer seeing it; `projection_kind` is stored per-variant
  rather than derived from `domain.to_ascii_uppercase()` precisely because a second family need
  not follow that pattern. Upgrade signal: a tenant-scoped or second-domain ticket stream.
- **The gateway still reads `HUMAUX_GATEWAY_REMEMBER_{DOMAIN,PROJECTION_KIND,PROJECTION_VERSION}`.**
  It is a multi-family writer by design (its default write pair is deployment policy), so the
  keys stay and the seed fills them. The hand-alignment is gone; the key is not. Upgrade signal:
  the gateway becoming a single-family writer, at which point the keys should go too.
- **Not done, and why: `0159` is still `NOT VALID`.** `evidence_objects_reasoning_domain_tenant_fk`
  cannot be `VALIDATE`d while orphan rows exist, and the development database currently holds 2
  (test residue: `private.evidence_objects` rows whose `(tenant_id, reasoning_domain_id)` pair
  has no `control.private_reasoning_domains` row). Repairing them is a destructive write to
  shared state, which this card is not authorized to perform. The constraint already refuses
  every new write; what is missing is only the historical proof and the planner's use of it.
  Upgrade signal: a clean database, or an owner-approved repair of the two rows, followed by a
  one-line `VALIDATE CONSTRAINT` migration.
- **Not done, and why: the membership `reason` is not on the wire yet.** `MembershipConflict::
  reason()` gives every refusal a typed code and is consumed by `as_str()`, but surfacing it as
  `structuredContent.reason` requires `bins/gateway/src/mcp_application.rs` and
  `crates/adapters/src/membership_repo.rs`, neither of which is in this card's file set.
  Upgrade signal: the next card that owns the membership MCP surface.
- **The checkpoint attribution names the LAST advancer, not a history.** One column, overwritten
  by whichever worker moved the watermark most recently — enough to answer "which deployment
  advanced this stream", not "which worker advanced seq 41..47". A per-advance ledger is a table,
  not a column, and nothing asks for one today. Upgrade signal: more than one retrieval worker
  per stream in a deployment anyone has to debug.
- **`projection_processor_id` is NULL for every row advanced before 0171**, including on this
  development database, because nothing was back-filled. The rehearsal asserts over rows whose
  `projection_highwater > 0` *in the run's own tenant*, which is the only set whose attribution
  this deployment can vouch for. Upgrade signal: none — this is the correct reading of "we do not
  know who advanced that watermark".
- **The sweep has a door, not a schedule.** `cargo xtask sweep-confirm-tokens` is invocable from
  cron/launchd; this system has no maintenance daemon to hang it on (the same is true of
  `stream_repo::sweep_lost`, which has had no caller since §15.2 landed). Upgrade signal: the
  first periodic-maintenance process, at which point both sweeps move into it and the CLI becomes
  the manual override.
- **`DisclosureSource::Memory(Uuid::nil())`** remains as the `embed_cards` batch-level fallback
  (see D2). Upgrade signal: a caller that reaches `embed_cards` for real; today the projection
  path does not.
- **`distill_hop_e2e::d1_live_distill_writes_memories_and_projection_resolves_ticket` is
  flaky against the live model, and was before this card.** In this card's run it failed once
  (`failed: InvalidInput` — the live MiniMax response did not satisfy the strict distill parser,
  which then correctly settles the row FAILED) and passed on 2 of 3 immediate retries. The
  identical failure, with the identical message, is in card 20's own gate log
  (`gates_card20b.log`); card 19's log has it green. It is a live-model output flake in the
  first ticket window, the same class card 24 D1 names — not a card-21 regression, and the
  passing runs exercise `assert_fingerprint_recomputes` end to end against real data. Upgrade
  signal: a bounded retry (or a recorded-response mode) for the live distill acceptance test.

## Latency (measured after warm-up, 2026-09-16)

Recorded per the card-14-onward rule. Only two operations are *new* per-unit work; both were
measured in-database on the dev instance after warm-up, and each number below names the loop it
came from rather than being quoted from a report.

| operation | n | unit | p50 | p95 |
|---|---|---|---|---|
| `ops.jobs` per-row lease heartbeat — the exact `UPDATE … WHERE job_id AND tenant_id AND lease_owner AND attempt AND status='PROCESSING' AND lease_expires_at > clock_timestamp()` that `jobs::heartbeat_derived_private` issues, now once per evidence row in `run_once` | 200 | ms | 0.015 | 0.032 |
| `control.sweep_confirm_tokens(interval '30 days')` — the 0169 retention sweep, with the two partial indexes in place | 50 | ms | 0.005 | 0.027 |
| §15.4 `advance_prefix`'s checkpoint write with 0171's attribution column — the exact `UPDATE projection.stream_checkpoints SET projection_highwater = …, projection_processor_id = … WHERE <6-column key> AND projection_highwater <= …` (fix pass, 2026-09-16, one PK row under a tenant RLS context) | 200 | ms | 0.005 | 0.015 |

Both loops ran under a tenant RLS context, against the live `humaux_thread_dev` instance, with
the row set the dev database actually holds (`control.confirm_tokens` = 3 rows at measurement
time — a small-table number, and honest about it: the p95 is an index-plan cost, not a
throughput claim about a full table).

**Why the heartbeat is affordable.** At 0.015 ms p50 it is ~5 orders of magnitude below the
provider round trip it precedes (`embedding` p50 282 ms / p95 600 ms, n=102, from
`ops.model_call_ledger`). Renewing per row costs nothing measurable and removes an entire class
of double-spend; sizing the batch from a measured latency instead would have cost a derived
value to maintain and still failed whenever the provider was slower than its own history.

**Unchanged by this card.** No hop was added or removed. The distill fingerprint leg does
strictly *less* work than before — one `serde_json::to_vec` of the event payload plus one SHA-256
over it, per evidence row, are gone, replaced by a 32-byte `try_from` — so the distill hop's own
latency can only have improved; the effect is below the noise floor of a single live provider
call and is not claimed as a measured improvement.

**Fix pass, added column.** Writing `projection_processor_id` in the same statement as the
watermark costs nothing measurable: the statement is a single-row PK update either way, and at
0.005 ms p50 it is four orders of magnitude below the Qdrant upsert + visibility verification that
precedes it in `run_once`'s step (j). There was never a second statement to save — the attribution
was added *to* the existing one, precisely so it cannot drift from the number it signs.

**Live acceptance timings from this card's run** (for the record, not as a per-operation
baseline): `native_gateway_semantic_recall_real_qdrant_pg_and_ryw_acceptance` 80.81 s wall
(n=1, real Qdrant + PG + read-your-writes); `consolidation_hop_e2e` 7 tests in 3.89 s;
`distill_hop_e2e` 8 tests in 3.31 s.

## Addendum (card 35, 2026-10-04; ADR-0062 D-G / D-Q)

The confirm-token sweep now has a schedule. Card 35 supersedes "this system has no maintenance daemon": the resident
`humaux-maintenance --serve` (ADR-0062 D-A) calls the bounded door `control.sweep_confirm_tokens(interval, integer)`
(migration 0218, which dropped the 1-arg version: one door) per tenant with `ORDER BY expires_at LIMIT … FOR UPDATE
SKIP LOCKED` and one `ops.maintenance_receipts` row per call that removed rows. Its predicate is this ADR's,
verbatim. `cargo xtask sweep-confirm-tokens` and `xtask/src/confirm_sweep.rs` are retired; the manual override is
`humaux-maintenance sweep once` with the §77 fields, which needs no test DSN (OPS-6). "Derived values are derived"
is also why card 35 rejected mark-only purge columns (ADR-0062 D-E, ruling E2): an `expired_at` mark is derivable
from `expires_at` and nothing would ever delete the marked rows.
