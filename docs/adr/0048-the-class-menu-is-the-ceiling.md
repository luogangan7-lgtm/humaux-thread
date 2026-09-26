# ADR-0048 — The class menu the model reads is the ceiling it may assert

- Status: accepted (card 24, 2026-09-26)
- Supersedes nothing. Does NOT reverse ADR-0016 D3/D4 or the §10.1 rule-3 decision that an
  over-ceiling candidate is rejected and never downgraded — `distill_hop_e2e::
  d2_over_ceiling_candidate_rejected_not_downgraded` still pins that, and still passes.
- Spec: §10.1 (origin-bound authority ceiling), §15.5 (a Distill pass answers 0/1/N),
  §16.1 G16-4 (`prompt_hash` is a fingerprint axis), §78.1 (no literals — the menu is derived
  from the ceiling table, not written out a second time).

## Context

`d1_live_distill_writes_memories_and_projection_resolves_ticket` has failed once in each of the
last four main-line chains and passed on retry every time:

| date | chain log | shape |
|---|---|---|
| 2026-09-08 | `gates_card14.log` | `claimed:1, done:0, failed:1` — `InvalidInput` on the model's reply |
| 2026-09-09 | `gates_card14c.log` 00:12 | `done:1, memories:0`, candidate rejected `origin_authority_ceiling` — live MiniMax classified the fixture sentence as `ProjectConstraint` for an `AuthenticatedAgent` origin |
| 2026-09-19 07:34 | `gates_card22_final.log` | `done:1, memories:0` — the model returned `{"memories":[]}` |
| 2026-09-20 12:39 | `gates_card22b_final.log` | `done:1, memories:0` — same |

Three of the four look like two different bugs. They are one.

The v1 contract showed the model all seven `AuthorityClass` values and then, in prose, asked it
to apply a NEGATIVE constraint to its own answer: "must NOT rank above the envelope's
`max_class`". The fixture sentence — "New backend services must expose a health endpoint before
any traffic is routed to them." — reads as a project constraint to any competent reader, and
`ProjectConstraint` was on the menu. A model that judges the evidence to deserve a class it is
forbidden to use has exactly two escapes, and across four chains it took both: answer over the
ceiling (rejected, `memories: 0`), or answer nothing (`{"memories":[]}`, also `memories: 0`).

Retry only changes the odds. A 1-in-4 flake on the delivery path is not shippable as a limit.

## Decision

### D-A — Render the contract per ceiling; the menu IS the constraint

`distill_prompt_contract()` becomes `distill_prompt_contract(ceiling: AuthorityClass)`
(`crates/adapters/src/distill_reasoner.rs`). Contract version 1 → 2. The system prompt's rule
(4) and the JSON schema's `class` enum are both rendered from `admissible_classes(ceiling)` —
the §10 ladder truncated at the origin's §10.1 ceiling. Nothing above the ceiling appears
anywhere the model can read it.

Rule (4)'s wording changed from a prohibition to a closure: the listed classes are "ALREADY the
complete set of classes this evidence's origin is permitted to assert … a statement that feels
like it deserves more authority than the highest listed class is still recorded at that highest
listed class rather than dropped." The second half is what removes the "answer nothing" escape:
the model is told what to do with a statement that outranks the menu, instead of being left to
invent a disposal route.

This is option (c) of the card-14c/22b debt. Option (a) CLAMP (store at the highest permitted
type and record the requested one) was **rejected**: it reverses §10.1 rule 3 and the pinned
`d2_over_ceiling_candidate_rejected_not_downgraded` — a downgrade at the STORE gate is exactly
the "总结后洗白" §10.1 exists to prevent. Option (b) REJECT+RETRY was **rejected** as the primary
fix: it is the status quo plus a coin-flip budget, and it leaves the model's dilemma in place.
(c) removes the dilemma and keeps (b)'s fail-closed rejection as the backstop, which is what
d2 pins.

`ExplicitTaskContext` is excluded from the menu at EVERY ceiling, not merely because no origin's
ceiling reaches it, but because `domain::policy::StoredAuthority::authorize` refuses it
unconditionally (card 22c I-STORE: 6 is a task-binding authorization, not a content property).
Pinned by `distill_menu_never_offers_explicit_task_context`.

### D-B — `prompt_hash` moves with the ceiling

The ceiling's wire name is folded into the contract's sha256 alongside the rendered prompt and
schema, so two Evidence rows distilled under different ceilings can never share a `prompt_hash`
and therefore never share a §16.1 `source_hash`. `bins/private-worker/src/distill.rs` resolves
the ceiling ONCE (`evidence.origin_class.authority_ceiling(MemoryType::Fact)`) and feeds both
the contract and the envelope's `max_class` from that one binding — before card 24 the contract
was ceiling-blind and only the envelope knew, which is how prompt and policy could disagree.
Pinned by `distill_contract_hash_moves_with_the_ceiling`.

Consequence, stated plainly: every Distill run's `source_hash` moves with this change. That is
the documented behaviour of the `prompt_hash` axis (`distill_reasoner`'s module doc), not a
regression, and the contract version bump to 2 is the record of it.

### D-C — An empty answer is re-asked once, and the retry is counted

`process_claimed` re-runs the provider round trip ONCE when a clean parse yields zero
candidates, then accepts the empty answer. `DistillPassReport::empty_retries` counts it.

The parser is deliberately NOT changed to reject `{"memories":[]}`. §15.5 makes 0 a legitimate
answer of 0/1/N and `distill_parser_accepts_the_contract_shape_including_zero_memories` pins it;
turning every greeting into a fail-closed parse error would be a worse bug than the one being
fixed. What changes is that the worker stops treating an empty answer as unremarkable: there is
a second attempt, and the attempt is visible in the pass report, in a second
`private.processing_runs` row, in a second §7.4 disclosure and in a second §19.1 ledger row.

**Correction, card 24 review pass (two ways the first draft of this decision was false):**

1. *"visible in the pass report"* was true of the struct and false of the deployment. The only
   operational emitter, `bins/private-worker/src/main.rs:393`, did not print
   `report.work.empty_retries`, so in production an empty-answer retry was silent — the exact
   property the 2026-09-19 / 2026-09-20 chains lacked. The field is now on that line.
2. The abandoned first attempt was left with `completed_at IS NULL`, which
   `crates/adapters/src/distill_repo.rs:386-388` defines verbatim as the ADR-0016 D4 **failure**
   marker. A provider call that succeeded and returned nothing was therefore recorded as one
   that failed, and an empty-retry produced `(2 rows, 1 completed)` — byte-identical to d6's
   deferred-provider-failure shape. `process_claimed` now closes the abandoned run with its own
   output digest and `output_count = 0` before re-asking, so an empty-retry reads
   `(2 rows, 2 completed)`. `d3` asserts that pair (`distill_hop_e2e.rs`), which is what makes a
   regression here visible; before, nothing asserted the state at all.

D1's disclosure and ledger assertions are now derived from `report.empty_retries` rather than
hard-coded at 1, so the "every provider call leaves a receipt" invariant is still what they
test — a retry that left no receipt still goes red.

## Numbers

- Contract version: 1 → 2. Menu size by ceiling: `PrivateKnowledge` (the `AuthenticatedAgent`,
  `TrustedConnector`, `ToolResult`, `UploadedArtifact`, `ExternalContent`, `SystemMigration`
  rows of §10.1) → 2 classes; `UserCorrection` (`DirectUserInput`, `UserConfirmed`) → 5;
  `ProjectConstraint` (`TenantAdmin`) → 6. Seven classes were offered at every ceiling before.
- Empty-retry budget: 1 (`DISTILL_EMPTY_RETRY_BUDGET`), flat, no backoff.
- `private.processing_runs` rows per Evidence after one empty-retry: 2, **both completed**,
  `output_count` 0 then N.

## Tests

- `crates/adapters/src/distill_reasoner.rs`
  - `distill_menu_never_offers_a_class_above_the_origin_ceiling` (new) — for every ceiling on
    the ladder, every class in the schema enum is `<= ceiling` AND named in the prompt, and no
    class above the ceiling appears in either.
  - `distill_menu_never_offers_explicit_task_context` (new)
  - `distill_contract_hash_moves_with_the_ceiling` (new)
  - `distill_contract_memory_types_exclude_the_constraint_ceiling_case` (now loops all ceilings)
  - `distill_contract_is_stable_and_hashes_prompt_schema_and_budget` (rendered, not literal)
- `bins/private-worker/tests/distill_hop_e2e.rs`
  - `d3_zero_memories_settles_outbox_and_ticket` — asserts `report.empty_retries == 1` and
    `provider.calls() == 2`; the retry is a real second round trip, not a counter bump.
  - `d1_…` — disclosure/ledger counts derived from `report.empty_retries`.
  - `d2_over_ceiling_candidate_rejected_not_downgraded` — unchanged, still pins §10.1 rule 3.

## Acceptance evidence

Five independent live runs of `d1_live_distill_writes_memories_and_projection_resolves_ticket`
against real MiniMax-M3 (2026-09-26 03:57–04:01), plus the chain's own run in
`gates_card24_final.log` `distill_hop_all` — **5/5 + chain green**, where the pre-fix test failed
once in each of four consecutive chains.

Every run: `memories: 1`, `classes: ["PrivateKnowledge"]` (the `AuthenticatedAgent` ceiling),
`rejected: 0`, `empty_retries: 0`, one `SUCCEEDED` `PRIVATE_DISTILL_TEXT` ledger row,
`projection(done=1, failed=0)`, `ticket=(DONE, None)`. `output_tokens` varied 153–221 across the
five, so these are five distinct generations, not a cached reply.

`empty_retries: 0` on all five is worth stating explicitly: **D-C never fired.** The menu fix
alone carried every run. D-C's behaviour is proven by `d3` against a deterministic fixture, not
by these runs; against the live model it is so-far-unneeded insurance.

Five runs bound the flake rate loosely, not tightly. The structural argument — the illegal
options are not on the menu the model reads — is what carries the claim; the runs corroborate it.

Full chain: `gates_card24_final.log`, 18 min 31 s. **27 of 28 gates EXIT 0 in the chain;
`workers_tests` was EXIT 101 at `gates_card24_final.log:2789`** (the consolidation hop's live
`t1`), EXIT 0 on a full rerun of that gate. The chain is not a chain at zero — see the delivery
report §8.1, which no longer states otherwise.

## Open debt

- `byok::structured_request_body` still does not put `json_schema` on the wire (its own
  `ponytail:` note). The rendered PROMPT is what constrains the model today; the schema is
  rendered from the same list so the two cannot disagree once the wire body does carry it.
  Until then the menu is an instruction, not a grammar, and a sufficiently contrary model can
  still answer outside it — at which point the parser (`authority_class_from_db_str`) or
  `StoredAuthority::authorize` fails closed, which is the pre-card-24 behaviour, not a new risk.
- Rule (4)'s "when in doubt" fallback asks the model to record an over-ceiling-worthy statement
  at the highest listed class. That is a model-side clamp with no record of the class the model
  would have wanted — option (a)'s behaviour without option (a)'s audit row. A candidate the
  model still writes above the menu IS persisted PENDING with its requested class
  (`memory_candidate_rejections_total`), so only the silent clamp is unaudited. Accepted for
  card 24 (review P2); a `wanted_class` field on the wire would close it.
- **The consolidation hop still carries the v1 shape.** `CONSOLIDATION_PROMPT_V1` rule (4) lists
  all seven classes and applies the same negative constraint ("must NOT rank above the highest
  `class` among the inputs you used"), and its schema enum carries all seven. The truncation is
  sound there too — no legal output can rank above the highest class among the **supplied**
  inputs, and that maximum is known before the call. Not fixed here:
  `crates/adapters/src/consolidation_reasoner.rs` is outside card 24's allowed files. Delivery
  report §6.8 records it with what this chain did and did not observe.

## Acceptance evidence, extended (rehearsal5, 2026-09-26 07:47–08:44)

Five consecutive full rehearsals with the soak (`gates_card24_rehearsal5.log`), 324 live
MiniMax distills in total: `failed=0`, `malformed_retries=0`, `empty_retries=0`,
`origin_authority_ceiling` rejections `0` — the D1 shapes of 2026-09-18/19/20 and the
`memory_type: "Requirement"` shape of the same morning's first soak (8 of 29) did not recur.

## Addendum (2026-09-26, card 24 soak) — the same defect on the `memory_type` axis, and D-D

The rewritten rehearsal's soak ran 29 live distills; 8 failed as a bare `failed: InvalidInput`
and each became a permanent `FAILED` ticket. A 12-call live probe through the real worker path
(a logging transport around `EgressHttpTransport`, temporary, removed) showed the whole shape:
MiniMax-M3 answers `<think>…</think>` followed by the JSON (already stripped by
`byok::strip_think_blocks` — not the cause), and **4 of 12 replies carried
`"memory_type":"Requirement"`**. Rule (1) tells the model to capture "a rule, requirement,
constraint, policy"; rule (3) offered `Fact, Preference, Decision, Rejection, State, Issue` and
no type for any of those words (`Constraint` is excluded on purpose — ADR-0016 D3/D4, one
`Fact` ceiling per Evidence). The model's own reasoning even noticed ("the allowed types are…
wait") and still invented the type a third of the time. Same defect as D-A: a rule the model
must obey with no legal way to obey it.

- **Prompt.** Rule (3) now says the list is closed ("there is no other memory_type — no
  Requirement, Rule, Constraint, Policy, Lesson or Note") and names the mapping: a rule,
  requirement, constraint, or policy is `Decision`; a durable fact about the user, the project,
  or the system is `Fact`. `prompt_hash` moves with it (D-B); `DISTILL_PARSER_VERSION` does not
  — the accepted set is unchanged.
- **Parser.** Stays closed. `parse_distill_output_detailed` names the rule it refused on
  (`DistillParseError`: `not_json`, `top_level_shape`, `memories_missing`, `too_many_memories`,
  `item_shape`, `content_empty_or_too_long`, `memory_type_unknown`, `class_unknown`,
  `confidence_invalid`); `parse_distill_output` keeps the wire class `InvalidInput`. The worker
  prints the reason — a structural label, never payload text — so the next
  `failed: InvalidInput` is diagnosable from the log alone.
- **D-D — a refused reply is re-asked once, and the retry is counted.** Mirrors D-C with one
  difference: the abandoned attempt keeps ADR-0016 D4's failure marker (`completed_at IS NULL`)
  because a reply the parser refused *is* a failed attempt, unlike D-C's succeeded-but-empty
  one. `DistillPassReport.malformed_retries` is printed on the dispatch line next to
  `empty_retries`. After the budget the row fails exactly as before (d4 still pins fail-closed).

Tests: `distill_reasoner::distill_parser_names_the_rule_it_refused_and_the_prompt_maps_requirements`;
`distill_hop_e2e::d4_parser_fail_closed_marks_outbox_failed` (now four replies,
`malformed_retries = 2`, both rows still FAILED) and the new
`d4b_a_malformed_reply_is_re_asked_once_then_settles` (`(runs, completed) = (2, 1)`, one memory,
`DONE`).
