# ADR-0044 — The document must not claim a surface the binary does not have

- Status: Accepted
- Date: 2026-09-17
- Card: 22
- Spec: §33 Canonical MCP Tool Details (Tool 2, 工具面交付实况), §33.1 (Canonical Tool
  Contract), §52 / §52.1 (18 frozen ErrorCode variants; `DEPENDENCY_UNAVAILABLE` vs
  `INVALID_INPUT`), §52 D-B (`ConflictReason`), §55.1 / §55.4 (profile-owned candidate depth;
  「不劣于」unassertable), §16.3 (无裁量切换判据), §69 (Continuation Gate, benchmark 分母声明),
  §22.0 / §22.4 (exact class needs a census; lane trigger precedes the planner), §25.4
  (Mandatory Context Lane selectors), §77 (Sensitive Admin Action audit), §78.2 (closed enums,
  spelled once), §80.1 (a criterion no input can falsify is not a criterion), §57.1 / ADR-0006
  (`not_applicable` names its missing object).
- Supersedes nothing. Closes card 16's `recall.schema.json` `limit` debt, card 19's ADR-0041
  D-D (memory.get counts) debt, card 20's §16.3 criterion ③ SPEC-OWNER item, and card 21's
  typed-but-invisible membership `ConflictReason` debt.
- **Does NOT close card 19's ADR-0041 D-I (§25 selectors).** This card re-diagnoses it — the
  recorded root cause was wrong (D-G) — and leaves the behaviour debt open, because the real
  unlock is a migration plus a `crates/domain` mapping clause and both are outside this card's
  allowed files. The debt is carried forward with its budget in D-G and pinned live by
  `bins/gateway/tests/mcp_gateway.rs`'s `unavailable_selectors` assertion, which turns red the
  day either column lands. Closing the diagnosis is not closing the debt.

## Context

Four debts, one defect class: **the document claims a capability the binary does not have, so
every honest fail-closed refusal reads as a bug.** An auditor filing those refusals as defects
costs more than the narrowing does.

1. **Tool 2.** §33 advertised semantic/literal/state/temporal/association/public-private.
   `bins/gateway/src/recall.rs:224-227` supports `mode == "semantic"` only and refuses
   `completeness_request == "required"` outright; `SemanticRecallRuntime`'s own doc calls
   itself the one native dense lane.
2. **The tool surface.** Four of eight tools (`artifact`, `code`, `coordinate`, plus
   `remember`'s three batch actions) are declared-only. 30 of the catalog's 48 operation keys
   have no implementation and answer `DEPENDENCY_UNAVAILABLE`.
3. **`recall.schema.json` `limit`.** Advertised as a caller-chosen `1..=100`, while §55.1
   (`crates/retrieval/src/request.rs:203`) freezes that a caller may not supply
   limit/top_k/cand_k. Card 16 paid a day for this: the soak's post-drain replay sent
   `limit = <live point count>`, got a silent `INVALID_INPUT`, and the failure was read as an
   embedding fault two steps further down.
4. **`SUPPORTED_OPERATION_KEYS` had already drifted.** Nothing reads it — three string pins
   only compare its declared *length* — so it sat at 15 while the `invoke` dispatch had been
   routing `memory.correct` / `memory.confirm` / `memory.reject` since ADR-0025 / ADR-0026.
   The card's own acceptance gate calls that array "the truth of what is wired"; it was not.

## Decisions

### D-A — Tool 2's v1 scope is the semantic lane; the other lanes are named future scope

§33 now states the target shape *and* the delivery point, per value, each anchored to the line
that implements it (`recall.rs:224` / `:225` / `:255-261` / `:269-278`). The five-value `mode`
enum **stays** in `contracts/mcp/recall.schema.json`. Deleting the unsupported values would
make the guard unreachable and its refusal unobservable — the §80.1 shape this codebase keeps
closing elsewhere. Instead each property carries `x-humaux-v1-supported` plus a description
naming the anchor and the error code, so the schema is honest without going silent. That
declaration is gate-checked against the guard, field by field — see the Known limitations entry
for the arm and its fault witness.

`DEPENDENCY_UNAVAILABLE` (not `INVALID_INPUT`) stays the answer for an unwired lane: §52's
reading is "the contract admits it, this deployment has no implementation", which is exactly
true and is what a client needs to distinguish from a bad value.

### D-B — `limit` keeps its type and bounds; the truth goes in the description

JSON Schema cannot express "equals a runtime profile's `top_k`", and the field cannot be
removed: `recall_with_a_consistency_token_answers_and_a_caller_chosen_limit_is_refused`'s third
leg sends `limit == profile top_k` and must still answer 200 (removing the property would make
it a schema violation under `additionalProperties:false`, moving the error away from the
gateway's own typed refusal). Input validation at a trust boundary is not deleted for tidiness.
The gateway line stays the authority; the schema now says so.

### D-C — `SUPPORTED_OPERATION_KEYS` is corrected to 18 and made load-bearing by a gate

Added `memory.correct` / `memory.confirm` / `memory.reject`; the three length pins
(`xtask/src/architecture_check.rs` ×2, `bins/gateway/tests/continuity_get.rs`) moved with it.

New `architecture-check` arm **tool surface truth** closes the loop in four directions:

```text
SUPPORTED_OPERATION_KEYS  ==  the invoke dispatch's arms
SUPPORTED_OPERATION_KEYS  ==  §33's <!-- humaux:wired-operation-keys --> block
wired ∪ declared-only     ==  every x-humaux-operation.operation_key in contracts/mcp
wired ∩ declared-only     ==  ∅
```

Array entries that are `DestructiveOp::X.operation_key()` resolve through the domain's own
`ALL` tables (§78.2 — the key is spelled once, in `domain`; the gate does not keep a second
list). Six mutation tests cover each single-place drift, including the length pin and the
document markers themselves.

The catalog leg fails closed on a malformed contract. §57.1's `not_applicable` is for an object
that is ABSENT; a contract that is present and shaped differently is not absent, and treating it
as such made this arm switchable off by a contract edit (renaming `oneOf` to `anyOf` stopped the
`wired ∪ declared-only == catalog` leg from judging anything while the run still exited 0). The
reader now separates the two (`CatalogDefect::Missing` ⇒ `not_applicable` naming the file,
`CatalogDefect::Malformed` ⇒ red), pinned by
`mcp_tool_surface_truth_faults_a_present_but_malformed_contract` — a tempdir copy of the real
tree that is green before the mutation, red on `anyOf`, red on non-JSON, and `not_applicable`
only when the file is actually deleted.

### D-D — `memory.get`'s absent counts are documented as intentional

`classify()` maps `DirectGet` to `QueryClass::Exact`; §22.0 makes an `exact` class without a
`predicate_id`/census a hard 5xx. Filling the five counts without a census converts a correct
200 into a 500. §33 and `memory.output.schema.json`'s top-level description now say this, name
the unlock (a universe-of-one census for `DirectGet`, a separate decision), and point at the
three-route assertion in `bins/gateway/tests/mcp_gateway.rs` that turns red if someone
"finishes the job".

### D-E — §16.3 criterion ③: neither the code nor the criterion moves; the benchmark declaration is what is missing

Card 20's SPEC-OWNER item asked whether to relax ③ to §69's FAIL side for non-first promotions,
because §69 is `NOT_DECLARED` on this deployment ⇒ `CannotEstablish` ⇒ every non-first
promotion is refused.

**Decided: keep both.** §16.3's own text already froze this side after ADR-0017
(「非首次激活的一切照旧 … `CANNOT_ESTABLISH` / `INCONCLUSIVE` … 继续拒绝」) and
`crates/projection/src/serving.rs::evaluate_switch` matches it verbatim. §16.3 gains a
delivery-point subsection stating the consequence and the unlock.

**Rejected alternative:** apply the FAIL-side-only form to non-first promotions too
(`CannotEstablish` passes ③ when §69 is `NOT_DECLARED`, reason recorded on the switch).
Rejected because on a deployment that never declares a benchmark, ③ degenerates into a
criterion no input can falsify — the 恒真闸 §16.3 and §80.1 both forbid. First activation is
exempt for a different reason: it is **missing a second operand** (no serving row, nothing to
be worse than). A non-first promotion has both operands and is missing only the *instrument
declaration* — a deployment gap, not a criterion-shape error. The web check corroborates the
same convention: baseline-gated promotion tools (Argo Rollouts' AnalysisRun, baseline-gate CLIs)
treat "no comparable baseline" as a distinct *gate-not-evaluated* state, never as a pass.

Both branches already have witnesses, so no test was added:
`evaluate_switch_rejects_when_benchmark_cannot_establish`,
`first_activation_needs_no_benchmark_because_there_is_no_baseline`,
`first_activation_still_refuses_a_proven_degradation`,
`a_real_promotion_still_requires_the_benchmark_to_pass`.

Consequence recorded honestly: the soak's `promote_candidates_admitted` can only be observed as
0 on a deployment with a second version until the §69 set is frozen (≥800 items, resolution ≤ 2,
`frozen_by` written). That is the metric reporting the deployment's state, not a broken soak.
Delivery status (review P2, card 22): the acceptance clause — a soak pass that observes a *real* promotion
(`promote_candidates_admitted > 0`) — is **not delivered here**; it is card 24's rehearsal item (freeze the §69 set,
run the rehearsal with a second version, record the counter). Until then D-E is a decided-but-open debt, not a closed item.

### D-F — The membership `ConflictReason` code lands on the §77 audit row, because there is no wire to put it on

Card 21 typed `MembershipConflict::reason()` (ALREADY_IN_STATE 1201 / TRANSITION_NOT_ALLOWED
1203 / LAST_OWNER 1204) and card 21's debt asked for it as `structuredContent.reason` in
`bins/gateway/src/mcp_application.rs`.

**That route does not exist.** Membership mutation is an admin-plane path (`xtask member` →
`adapters::membership_repo::apply`); no membership key is in `SUPPORTED_OPERATION_KEYS`, and
`contracts/mcp/manifest.json` has no membership tool. Inventing an MCP route to carry a reason
code would be the opposite of this card.

The observable surface of a refused membership mutation is its §77 `DENIED` audit row, so the
code goes there: `metadata.refusal_code`, derived from the same `ConflictReason` the label is
derived from (`refusal_str` is already `reason().label()`), so a row can never carry a label
and a code that disagree. `MembershipRepoError::Display` prints both. `NotFound` is
`ErrorCode::NotFound`, not a `CONFLICT`, and correctly has no code.

Witness (live, gateway suite, `membership_lifecycle_*` fixture): three live refusals produce
1203 / 1201 / 1204 on their own DENIED rows, plus a distinctness assertion — a mapping replaced
by a constant, by `None`, or by a second hand-typed table cannot satisfy it.

### D-G — §25.4's two unavailable selectors: the recorded root cause was wrong, and the real one is out of this card's reach

Card 19's ADR-0041 D-I recorded that `context.assemble` is `cannot_establish/lane_failed` on
every deployment because `context_repo::fetch_frozen_in_txn`'s `other =>` arm has no WHERE
clause for `TaskExplicitContextV1` / `RequiredCurrentStateFacetsV1`.

Measured (2026-09-17, `humaux_thread_dev`): `private.memory_records` has 21 columns and
**neither `task_id` nor `facet`**, which are precisely those two selectors'
`required_columns` (`crates/domain/src/context.rs:140` / `:180`). The column probe at
`crates/adapters/src/context_repo.rs:523` runs *before* the WHERE dispatch, so both selectors
are `Unavailable` at the probe and the `other =>` arm (at `:573` after this card's comment block; ADR-0041 D-I recorded it at `:558`) is **unreachable code today**.
Writing the two WHERE clauses changes no output.

The real unlock is two columns (migration + §6.2.2 grant row + `rls_check` MATRIX) plus, for
`required_current_state_facets_v1`, a §25.2-five-facets ↔ §24-nine-variants alignment clause
that §25.4 does not contain. `crates/domain/src/context.rs:178` already refuses to guess it
(「不猜映射」) because a guessed mapping makes G25-1 accidentally green on a small fixture —
a gate that passes without judging anything (§80.1).

Migrations and `crates/domain` are outside this card's allowed files, so the behaviour is not
shipped here. What ships is the corrected diagnosis in §25.4, anchored line by line, so the
next card budgets a schema change rather than a WHERE clause.

**The debt stays open and is now pinned by its cause, not by its symptom.** Asserting only
`context.assemble = cannot_establish/lane_failed` would let this rot: the day the columns land
the class moves and the suite fails with "expected cannot_establish" and no hint of why. So the
live RYW acceptance additionally reads the lane back —

```text
handoff.unavailable_selectors == [
  ["task_explicit_context_v1",            "private.memory_records.task_id"],
  ["required_current_state_facets_v1",    "private.memory_records.facet"]
]
```

— in `bins/gateway/tests/mcp_gateway.rs::native_gateway_semantic_recall_real_qdrant_pg_and_ryw_acceptance`.
That is a live readback of this diagnosis rather than a repeat of its prose, and it forces the
next card's order of work: adding either column without writing that selector's predicate is red.

Budget for the card that closes D-I (all four, one change): (1) a migration adding
`private.memory_records.task_id` and `.facet` with the §6.2.2 matrix rows and the `rls_check`
MATRIX entry; (2) a §25.4 amendment carrying the §25.2-five-facets ↔ §24-nine-variants
alignment clause, stored with its rejected alternative; (3) the two WHERE clauses in
`context_repo.rs`, one predicate each; (4) the live witness flipped — `context.assemble`
answers a class other than `cannot_establish` with `reason = null` and real `stream_ledger`
counts, plus the fault that makes one selector `Unavailable` again and turns `lane_failed` red.

## Known limitations / upgrade signals

- `x-humaux-v1-supported` is gate-checked field by field against the `recall.rs` guard by
  `architecture-check`'s 「§33 card 22 (Tool 2 semantic lane …)」 arm
  (`xtask/src/architecture_check.rs::recall_v1_supported_lanes`; fault witness
  `recall_v1_supported_lanes_faults_every_single_place_drift`, eight single-place mutations
  including deleting `"semantic"` from the declaration, plus a positive control: declaring a
  value supported and deleting its refusal in the same change is the one green way out, so the
  arm cannot be read as "the guard must always refuse"). It stays advisory to clients — the
  runtime authority is the guard — but it is no longer an inert annotation: before that arm
  existed nothing in the tree read the key and any edit to it kept every gate green, which is
  the same unbacked-claim defect this ADR exists to remove (§80.1).
- `context.assemble` stays `lane_failed` until the two columns land (D-G) — an OPEN debt, not
  one this card closes.
- Non-first promotions stay refused until §69's benchmark set is frozen (D-E).
- `memory.get` stays `count_unknown` until a `DirectGet` census is decided (D-D).

## Latency (measured after warm-up, 2026-09-18)

Recorded per the card-14-onward rule. Every number below names the loop it came from; none is
quoted from a report.

| operation | n | unit | p50 | p95 |
|---|---|---|---|---|
| `cargo xtask architecture-check` — the whole run, i.e. every arm including this card's two new ones (`./target/debug/xtask architecture-check`, warm binary, 2 discarded warm-up runs, wall time) | 20 | ms | 3306 | 3324 |
| `recall.search` end to end over the live gateway (real Qdrant + PG), the timed loop inside `native_gateway_semantic_recall_real_qdrant_pg_and_ryw_acceptance` | 30 | ms | 1173 | 1181 |
| `context.assemble` end to end over the same gateway, same loop, same fixture | 30 | ms | 33 | 43 |
| `recall.search` without an affect filter — the D-D/§8.5.1 A/B leg of the same suite | 5 | ms | 1173 | n/a (n too small for a p95) |
| `recall.search` with an affect filter (Qdrant structured-payload prefilter + PG re-check) | 5 | ms | 1176 | n/a (n too small for a p95) |

The two request-path rows are this card's *unchanged* baseline, not a claimed improvement: no
code on the `recall.search`, `memory.get`, `context.assemble` or membership admin paths changed
except the audit row's one extra JSON field (D-F). They are recorded because a later card cannot
tell "unchanged" from "never measured" — the numbers come from the suite's own print line
(`ADR-0041 D-I p50/p95 (n=30, ms): …`), reproducible with

```text
HUMAUX_REQUIRE_DB=1 cargo test -p humaux-gateway --test mcp_gateway \
  native_gateway_semantic_recall_real_qdrant_pg_and_ryw_acceptance -- --ignored --exact --nocapture
```

`recall.search`'s ~1.17 s is dominated by the live embedding call, not by anything on this
card's diff; `context.assemble` runs no provider hop, which is the whole 35× gap between the two
rows. The two new `architecture-check` arms read three files and parse two JSON documents each
run — no DB, no network — and the full-run p50 above is the only figure that could hide them.
