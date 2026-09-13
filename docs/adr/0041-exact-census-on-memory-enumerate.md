# ADR-0041 — `memory.enumerate` answers with a real census, and the pipeline counts that let it

- Status: Accepted
- Date: 2026-09-13
- Card: 19
- Spec: §22.0 / §22.1 / §22.4 / §22.5 (EXACT completeness), §23.1② / §23.1④, §23.3③ / §23.3④
  (pipeline count scopes — amended in this change, text only), §20.1 / §20.2 / §20.3
  (predicate registry, and why this predicate is deliberately **not** in it), §20.4 (EXACT
  pagination is not best-effort), §46 (forward-fix migration 0165), §15.1 / §15.2 / §15.3 /
  §60 (stream ledger), §78.1 / §78.2.
- Supersedes nothing. Pays down ADR-0040's (card 18) named census debt in two passes: the
  original change moved `memory.enumerate` only; the follow-up recorded in D-H and D-I threads
  the same `stream_ledger` pipeline counts through `memory.recall` and `context.assemble`.
  All three read routes now take every §23.3④ reading they are entitled to. `count_unknown` is
  gone from every route except `memory.get`, where D-D shows it is the only legal answer. D-H
  states each route's class truthfully, including the one route whose class still does not
  move and why that blocker is not this debt.

## Context

The EXACT census machinery has been built and unit-tested since the §22.1 wave
(`crates/adapters/src/exact_census.rs`: `probe_predicate_inputs`, `exact_enumerate`, a real
`SELECT count(*)` in a `REPEATABLE READ` transaction with the §23.1② tombstone overlay and the
§18 secret deduction), and it had **no non-test caller**. Every read route passed
`CensusResult::ok_without_enumeration` (`bins/gateway/src/memory.rs`, `recall.rs`,
`context.rs`), so `memory.enumerate` — whose `PlannerDecision` is already
`Enumerate { AUTHORIZED_MEMORY_ENUMERATION_V1 }` (`crates/retrieval/src/request.rs`) — published
`exact: null`, `total: null`, `coverage: null` on every call. Production machinery with no
production caller is P1, and the completeness contract it exists to keep was a slogan.

The second half is ADR-0040's debt. Card 18 wired a live Qdrant `visible` count onto all three
read routes, so §23.1②'s ratio became real — and the envelope still said
`cannot_establish/count_unknown`, because §23.3④ (`Baseline_2.9.md:5750-5759`) requires
`evidence.persisted` and the four `knowledge.*` counts in the **`stream_ledger`** universe
before any envelope may leave `cannot_establish`, and forbids the shortcuts ("禁止填 `0`、返回
条数、`issued_highwater` 或其他块的值充数"). Nothing in the workspace writes
`projection.stream_checkpoints.evidence_highwater` / `knowledge_highwater` (grep: migration
0011's GRANT and two fixtures), and the knowledge three-state has no table.

## Decisions

### D-A. The census runs inside `materialize_memory_enumeration`'s own transaction

`exact_census::exact_enumerate` opened its own transaction, which cannot satisfy §22.1's "`total`
… 与返回项取**同一事务快照**" when the items come from somebody else's. It is now a thin
pool-level wrapper over a new `census_in_txn`, which the caller invokes inside the transaction
that also mints the manifest, materializes the bodies, and closes the ledger. Every census
statement runs inside a `SAVEPOINT`: §22.4 trigger 4 must degrade the envelope, and a
statement-level failure that aborted the caller's transaction would instead lose the page.

`exact_enumerate`'s public signature is unchanged, so `crates/adapters/tests/
exact_completeness_eval.rs` (a §55.3-declared frozen benchset, `fixed_denominator = 11`) keeps
measuring the same thing.

### D-B. One predicate, two faces — and it is deliberately not a registry row

`ENUMERATION_SCOPE` / `ENUMERATION_PREDICATE` are two consts in `context_repo`, and both the
manifest's candidate query **and** the census's three `count(*)`/id statements run them. A
second, hand-written filter is exactly how a `coverage` comes to describe a set the page never
held. On top of the shared text the mint cross-checks the two faces: the census's id list must
equal the manifest's, and disagreement is reported as a failed census (§22.4 trigger 4) rather
than as a ratio for a set that was never enumerated.

Why not a `control.retrieval_predicates` row, when §20.1 calls that registry "the sole entry
point into §22 EXACT completeness": that registry is the entry point for §20.2's
**surface-pattern** path, and §20.3 obliges every row in it to ship ≥5 natural-language
questions with ≥2 near-miss negatives (`crates/retrieval/tests/planner_predicate_eval.rs`
enforces it against a frozen 21-row eval). This route never reaches the planner —
`RetrievalIntent::trusted_memory_enumerate` hands `build_request` a frozen
`PlannerDecision::Enumerate` whose id lives in `domain::selection::
AUTHORIZED_MEMORY_ENUMERATION_V1`, documented there since it was added as "Authorized Gateway
enumeration, **distinct from the worker's tenant-shared predicate**". Registering an NL surface
for a trusted-intent predicate would make it reachable from query text, which is the opposite of
what it is; and re-declaring a frozen §55.3 benchset to accommodate a predicate with no NL
surface would be paperwork bought with a measurement.

The scope's `visibility_workspace_id IS NULL OR = $2` is `domain::identity::can_read`'s own
shape in SQL: migration 0004's `memory_records_visibility_matches_class` forces that column NOT
NULL exactly for `WORKSPACE_SHARED` rows, so another workspace's shared row leaves the
denominator while `USER_PRIVATE` / `TENANT_SHARED` rows (always NULL) stay in it — then
`readable_memory_ids` applies the per-row authority (cards 9 and 13). The subject filter
(§6.1.3 / ADR-0028 D-D) rides inside the census WHERE, not as a post-filter, so a
subject-scoped page never reports an unscoped denominator.

### D-C. The readout is frozen with the manifest (migration 0165)

Page 1 counts in the minting snapshot. Page 2 arrives in a younger transaction: re-counting
there pairs a fresh denominator with frozen items (the drift §22.1 forbids), and counting the
frozen item rows is 分母内生 ("禁止用召回条数冒充 `total`"). §20.4 rules out best-effort for
EXACT pagination, so the only honest option left is to freeze the readout with the manifest.
Migration 0165 adds three nullable columns to `ops.selection_snapshots`
(`census_predicate_id`, `census_total`, `census_excluded_secret`) under one all-or-nothing
CHECK. Three NULLs — a pre-0165 snapshot, or a mint whose census failed — read back as "no
census" ⇒ `CensusResult::failed` ⇒ `cannot_establish/census_failed`, never a total of 0.

`returned` stays per page (it is what this envelope returned); `total` and `excluded_secret`
describe the universe the manifest was minted from. So an unpaginated read reports
`coverage = 1.0, truncated = false`, and page 1 of 3 reports `2/3, truncated = true`.

### D-D. `memory.get` keeps `ok_without_enumeration` — and therefore keeps unknown counts

A DirectGet has no enumerable universe (§23.3④: "没有独立 census 的对象读取不生成 `exact`"), so
its census carries no enumeration. That is not free: `classify()` maps **both** `DirectGet` and
`Enumerate` to `Exact`, and §22.0 makes `class = exact` without a `predicate_id` an invariant
violation — a hard 5xx, not a downgrade. Today `memory.get` is kept off that path only by its
unknown pipeline counts. So the two halves must move together, and they do: one `Option` on
`MaterializedMemoryPage` carries the census **and** the pipeline counts, and `memory.get`
passes `None`. `memory.get` therefore stays `cannot_establish/count_unknown`, which is the true
answer for an object read, not a leftover.

This is a correction to ADR-0040's Consequences, which listed "`memory.get` with a class other
than `cannot_establish`" as part of this card's target. Given §22.0 + `classify()`'s frozen
mapping and this card's own decision that `memory.get` keeps `ok_without_enumeration`, that
outcome is unreachable by construction: the only way to move `memory.get` off
`cannot_establish` is to give DirectGet a census (a universe of one), which is a different
decision on a different card.

### D-E. `evidence.persisted` and `knowledge.*` are derived from the stream ledger

Read in the same transaction, for the request's full six-column `StreamKey`, from
`projection.stream_log`:

- `evidence.persisted` = `count(*)` of that stream's rows. §15.1/§60 issue a `stream_seq` in the
  same transaction that persists its Evidence and its `ops.outbox` row, so a row's existence
  *is* the "Evidence 已完整持久化" reading.
- `knowledge.eligible` = `count(*)`; `processed` = `DONE/SKIPPED_BY_POLICY/TOMBSTONED`;
  `waiting_key` = `WAITING_KEY`; `failed` = `FAILED/LOST`.
- `ISSUED/PROCESSING/RETRY_WAIT` are in **none** of the three buckets. Work in flight is not
  processed, and counting it as processed is the "填数充数" §23.3④ forbids; the sum then
  legitimately falls short of `eligible` and the envelope says `pipeline_count_mismatch`.

Both blocks declare `CountScope::StreamLedger`. §23.3④'s equations therefore discriminate on
two real axes — the ledger rows against `stream_checkpoints.issued_highwater` (a separately
written watermark: a hole in the dense sequence, or a watermark ahead of its rows, fails it),
and the knowledge partition against its own base. The authorized EXACT census is a different
universe and never fills these, exactly as §23.3④'s closing paragraph requires ("授权 EXACT
census 即使独立证明了自己的 total，也不能让全链闭合跨全集变成 true").

Marked ceiling (`ponytail:` in `context_repo`): this is a read-time derivation because nothing
advances `evidence_highwater` / `knowledge_highwater`. When the distill/projection workers start
advancing them, read those two watermarks here instead; the equations and the classification do
not change. §23.3④ gained a paragraph recording this 口径 (spec change is text only — no
classifier, Envelope, statistics eligibility, or A1/A2 algorithm change).

### D-F. No wire-shape change was needed

Checked for consumer compatibility before writing anything: `contracts/mcp/
context.output.schema.json` — which `memory.output.schema.json` `$ref`s as its `Envelope`
branch — already describes `completeness.exact` (six required fields), `known_lower_bound`,
`reason: pipeline_count_mismatch`, and the nullable `pipeline.evidence.persisted` /
`pipeline.knowledge.*` with both `count_scope` values. So no output branch was added, the
`oneOf` union stays 15 long, and the `mcp_catalog` pin
(`memory_output_accepts_bare_and_snapshot_page_shapes`) is untouched. No MCP op was added, so
the three `SUPPORTED_OPERATION_KEYS` pins are untouched. No new table, role or grant, so no
§6.2.2 matrix row and no `xtask/src/rls_check.rs` MATRIX change (0165 adds columns to a table
covered by §6.2.1's domain default, and a table-level grant extends to later columns).

### D-G. The same-snapshot claim needs an insert committed INSIDE the mint's window

D-A's claim — `total` counted in the snapshot the page came from — had no failing witness, and
the §23.1④ control that looked like one is not one. Both of its inserts land **after** the
minting call has already returned, so they cannot tell "counted in the page's snapshot" apart
from "counted in a fresh transaction opened the instant that one committed": either way the
census answers with the pre-insert number and every later page reads D-C's frozen readout
regardless. That control kills "re-count per page" — a real but different mutant.

The only insert that separates the two is one **committed while the mint is still open**, which
requires the mint to hold still for one. `context_repo::arm_census_mint_barrier` is that seam:
armed with a PostgreSQL advisory key a test already holds on a second connection, the mint parks
on `pg_advisory_xact_lock` after `final_memory_ids_in_txn` and before `mint_census_in_txn`; the
test polls `pg_locks` until the transaction is demonstrably parked, commits a fourth memory,
releases, and then asserts the frozen readout is still `3`. Disarmed — every production process,
and every other test — it is one relaxed atomic load and emits no statement. `pg_advisory_xact_lock`
(not the session form) releases with the transaction, so no failure path can hand a pooled
connection back still holding a lock.

The fault this card was asked to inject, performed and reverted
(`native_mcp_memory_enumeration_counts_in_the_page_snapshot`):

| injected fault | old §23.1④ control | new control |
| --- | --- | --- |
| `mint_census_in_txn` moved to its own transaction (`REPEATABLE READ READ ONLY`, its own `set_authorization_local`) opened after the id list | **green** | **red** — `cannot_establish/census_failed`, `known_lower_bound: null` |
| the same, plus D-B's `returned_ids == universe` cross-check removed | **green** | **red** — frozen readout `(…, 4, 0)` against the expected `(…, 3, 0)` |

The first row is also the first witness that D-B's cross-check is load-bearing rather than
defensive: with the census in a younger snapshot the two faces of the predicate disagree by
exactly the raced row, and §22.4 trigger 4 fires instead of a `coverage` for a set the page
never held.

### D-H. All three read routes, stated truthfully

`stream_pipeline_counts_in_txn` — already written, already `stream_ledger`-scoped — is now
called on all three read paths, each inside the transaction that path already closes its ledger
in, with the counts threaded out on the materialized value the way `MaterializedMemoryPage::
census` threads them for `memory.enumerate`:

| route | reads §23.3④ counts | class | reason |
| --- | --- | --- | --- |
| `memory.enumerate` | yes (with the census, D-D) | `exact` | `null` |
| `memory.recall` | yes | `semantic_bounded` | `null` |
| `context.assemble` | yes | `cannot_establish` | `lane_failed` |
| `memory.get` | no, by construction (D-D) | `cannot_establish` | `count_unknown` |

`semantic_bounded` is what §59's four-class vocabulary and §22.5's frozen degrade table leave
for a bounded semantic read: `classify()` maps `PlannerDecision::Class(_)` to `SemanticBounded`
(never `Exact` — a dense-recall answer has no enumerable universe, so neither route needs a
`control.retrieval_predicates` row and D-B's argument does not have to be re-run for them), and
§22.4 scopes both `reason` and `known_lower_bound` to `cannot_establish`, so both stay `null`.
That also means §22.0's exact-without-a-`predicate_id` 5xx is not on these two paths, which is
why their counts travel without a census while `memory.enumerate`'s cannot.

The count wiring is one shared mint, not three hand-written copies:
`StreamPipelineCounts::blocks()` produces the `(EvidenceBlock, KnowledgeBlock)` pair for all
three routes. §23.3④ freezes the scope label together with the numbers ("同一块的计数必须来自
同一实际全集、同一授权与快照"), and a route assembling the pair by hand could pair a
`stream_ledger` reading with an `authorized_view` label — a `count_scope_mismatch` no type would
catch. Same discipline as D-B's one-predicate-two-faces, one layer up.

**`context.assemble`'s class does not move, and the blocker is not this debt.** §22.4's lane
trigger is checked inside `classify()` *before* `planner_output` is read at all, and this
route's mandatory lane is `failed` on every deployment: `context_repo::fetch_frozen_in_txn`
returns `SelectorOutcome::Unavailable` for two of §25's five selectors
(`TaskExplicitContextV1`, `RequiredCurrentStateFacetsV1`) because neither has a WHERE clause
yet (`other =>` arm, "adapters::context_repo 尚未实现 … 的 WHERE 子句"), so
`handoff.unavailable_selectors` is never empty and `lane_status` is never `Ok`. This was
already this route's answer before the wiring — which corrects the original D-H's claim that
both routes were held at `count_unknown`: only `memory.recall` was. What the wiring changes for
`context.assemble` is real and observable, just not in `class`: its two blocks carry real
`stream_ledger` readings where they carried `null`s in an `authorized_view` label, and
`count_unknown` is out of its reason chain. Implement those two selectors and this route
becomes `semantic_bounded` with no further envelope change; that is a §25 decision on a
different card, not census debt.

`memory.get` is finished as it stands: D-D argues it keeps `ok_without_enumeration` and
therefore `count_unknown`, because a DirectGet has no universe and `classify()` maps DirectGet
to `Exact` — filling its five counts is what would put it on §22.0's 5xx path. The live
acceptance pins that by name so the next "finish the job" pass turns a test red instead of
shipping a 500.

### D-I. Recall / context adoption: witnesses and speed

Live witnesses added to `native_gateway_semantic_recall_real_qdrant_pg_and_ryw_acceptance` (the
one fixture where a live `visible` count and a closing ledger exist at once):

- `recall.search` on the healthy fixture: `class = semantic_bounded`, `reason = null`,
  `exact = null`, `known_lower_bound = null`, both blocks `stream_ledger` with
  `persisted = eligible = processed = projection.expected = 3`, `waiting_key = failed = 0`,
  `evidence.expected = null` (§23.1①, no batch).
- The three PG-only routes' loop now pins each route's `(class, reason)` pair and its count
  shape: `memory.enumerate` `exact`/`null` with `3`s, `context.assemble`
  `cannot_establish`/`lane_failed` with `3`s, `memory.get`
  `cannot_establish`/`count_unknown` with `null`s in `authorized_view`.
- §23.3④ fault re-injected (one `ISSUED` seq 4 + `issued_highwater = 4`, exactly as the
  enumerate leg does it): `recall.search` returns to `cannot_establish` with
  `pipeline_count_mismatch` — §23.3④'s own verdict for a *known but contradictory* chain, a
  different answer from `count_unknown` — while `completeness_ratio` stays `0.75` (A1/A2 hold;
  the fault is in the knowledge layer). `context.assemble` keeps `lane_failed`, which is the
  honest outcome given the lane trigger's priority, and its counts move to
  `eligible = 4, processed = 3` — the only thing that *can* observe the fault on that route,
  and therefore what the assertion checks.

Fault injection on the wiring itself (applied, observed, reverted):

| injected fault | observed |
| --- | --- |
| `recall.rs` passes the old four-`None` `KnowledgeBlock` / `no_batch(None, AuthorizedView)` again | **red** — `completeness.class` `cannot_establish` against the expected `semantic_bounded` |
| `context.rs` passes the same four `None`s again | **red** — `pipeline.evidence.persisted` `null` against the expected `3` (its class is lane-pinned, so the counts are the observable) |
| both restored | **green** |

Speed, live fixture, n = 30 each, ms, end-to-end MCP call:

| route | before p50 | before p95 | after p50 | after p95 |
| --- | --- | --- | --- | --- |
| `recall.search` | 1176 | 1188 | 1177 | 1186 |
| `context.assemble` | 33 | 40 | 34 | 40 |

"Before" is the same binary with exactly the two statements this change adds removed
(`stream_pipeline_counts_in_txn` short-circuited to constants, no SQL) so the delta is the two
`projection.stream_log` aggregates and nothing else. Both routes are within run-to-run noise
(a second "after" run read 1179/1192 and 34/39): `recall.search`'s p50 is dominated by the
embedding provider round trip (~1.17 s, unchanged since card 18), and on `context.assemble`
— the route where the aggregates would be a visible share of a 34 ms call — the delta is 1 ms
at p50 and 0 at p95, i.e. at this measurement's own 1 ms resolution. Both statements are
indexed aggregates over the request's own stream, issued in a transaction that was already
open, so there is no extra round trip to pay for.

## Rejected

- **Relax §23.3④'s `count_inconsistency_reason`** so a known census alone can close the chain
  (carried over as rejected from card 18, §80.1). It would make the one reading that proves the
  pipeline unknown into a decoration.
- **Fill `evidence.persisted` / `knowledge.eligible` from `projection.expected`
  (`issued_highwater`)** — named verbatim in §23.3④'s prohibition. The equation would become
  tautological and the density fault below would be undetectable.
- **Re-count the census on every cursor page.** Cheaper than migration 0165 and wrong: a
  younger snapshot's denominator over frozen items is the drift §22.1 exists to forbid.
- **Derive `total` from the frozen manifest's row count.** 分母内生; §22.1 names it.
- **Add `authorized_memory_enumeration_v1` to `control.retrieval_predicates`** — see D-B: it
  would declare an NL trigger that must not exist and force a frozen benchset re-declaration.
- **Give `memory.get` a one-row census to move it off `cannot_establish`.** Out of this card's
  scope (its Scope section decides the opposite) and a real decision in its own right.

## Acceptance

Live, real-Qdrant (`native_gateway_semantic_recall_real_qdrant_pg_and_ryw_acceptance`, the one
fixture where a live `visible` count and a closing ledger exist at once — without card 18's
count §23.1②'s ratio is null and the envelope is correctly
`cannot_establish/index_count_unavailable` whatever the census says):

- unpaginated: `class = exact`, `reason = null`, `exact.predicate_id =
  authorized_memory_enumeration_v1`, `total = 3 = returned`, `coverage = 1.0`,
  `truncated = false`, `excluded_secret = 0`, `known_lower_bound = null` (§22.4 scopes it to
  `cannot_establish`), and both pipeline blocks `stream_ledger` with
  `persisted = eligible = projection.expected = 3`, `processed = 3`, `waiting_key = failed = 0`,
  `evidence.expected = null` (§23.1①, no batch).
- §23.1④ same-snapshot control: page 1 of 2 reports `total = 3, returned = 2, truncated = true`;
  a memory inserted before the continuation does **not** move the frozen `total`; a fresh
  manifest reports `4`.
- §22.4 trigger 4 injected: NULL out a frozen manifest's readout ⇒
  `cannot_establish/census_failed`, `exact = null`, `known_lower_bound = null`.
- §23.3④ census fault injected: one `ISSUED` seq 4 + watermark 4 keeps A1/A2 closed
  (`completeness_ratio = 0.75`) and breaks the knowledge partition ⇒
  `cannot_establish/pipeline_count_mismatch`, `exact = null`, `known_lower_bound = 4`.

Plain gateway suite (`native_mcp_memory_enumeration_freezes_snapshot_and_binds_cursor`, no
semantic runtime ⇒ `visible` legitimately unavailable): `known_lower_bound = 1` where it was
`null`, `exact = null`, all five pipeline counts real `0`s in `stream_ledger` scope where they
were `null`s, and the frozen readout read straight from `ops.selection_snapshots` —
`(authorized_memory_enumeration_v1, 5, 0)` before and after 20 concurrent inserts, `25` on the
next fresh manifest.

- **"Another user's private memories never raise the authorized count"** has a witness in that
  same test: a sixth row in the fixture workspace, owned by a peer user and `USER_PRIVATE`, is
  asserted present in `private.memory_records` (`count(*) = 6` over the active universe) and
  absent from both the page and the denominator (`5`). Honest limit, carried from the review's
  uncaught-fault note: this one is **not independently invertible inside this card's scope**.
  Migration 0012's `memory_records_tenant_and_visibility` RLS policy (`FORCE ROW LEVEL
  SECURITY`, bound by `set_authorization_local`'s GUCs in this very transaction) already removes
  the row at the database layer, so `authorized_candidate_ids` and `census_readout` see the same
  filtered set with or without the `AUTHORIZED_CANDIDATE` (`= ANY($8)`) conjunct — removing that
  conjunct is a no-op and the test stays green. The conjunct is kept as defence in depth on an
  authorization path, not deleted; inverting the guarantee itself means defeating RLS (an
  unrestricted role, or skipping `set_authorization_local`), which is cards 9/12/13's
  cross-cutting mechanism and outside this card's allowed files.
- **§22.1 same-snapshot, the card's named fault injection**
  (`native_mcp_memory_enumeration_counts_in_the_page_snapshot`): see D-G for the seam, the two
  injected variants and the red/green table. The fault was applied, observed red, and reverted;
  the pre-existing §23.1④ control stayed green under both variants, which is why the new one
  exists.

## Speed

`memory.enumerate`, n = 30 each, ms, 25-row authorized universe, local PG, plain gateway suite
(printed by the test above):

| shape | p50 | p95 |
| --- | --- | --- |
| minting page (census counted + pipeline counts read) | 41 | 56 |
| continuation page (frozen readout, no count) | 25 | 38 |

The delta between the two rows is the census's own cost on this fixture: the minting call adds
four statements (candidate ids, `total`, id list, `excluded_secret`) plus two
`projection.stream_log` aggregates over the request's stream; the continuation adds one indexed
single-row read of the frozen readout.
