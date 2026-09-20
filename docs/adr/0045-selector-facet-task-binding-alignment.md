# ADR-0045 — §25 selector schema pass: one generated column, one existing table, one written alignment clause

- Status: Accepted
- Date: 2026-09-19
- Card: 22b (delivery cards 20260903)
- Supersedes the diagnosis half of ADR-0041 D-I; completes ADR-0044 D-G.
- Spec landing: Baseline_2.9 §25.4.A (verbatim, 11 points), §25.2, §24, §6.2.2, §33.10.
- Migration: `migrations/0172_memory_records_mandatory_facet.sql` (+ manifest).

## Context

`context.assemble` answered `completeness.class = cannot_establish / reason = lane_failed` on
**every** deployment. Card 22 measured the cause and corrected ADR-0041 D-I: it was not a
missing WHERE clause. Two of the five §25.4 selectors declared `required_columns` that
`private.memory_records` does not have — `task_id` and `facet` — so the column probe marked
them `Unavailable` before the WHERE dispatch ran, `handoff.unavailable_selectors` was never
empty, and §22.4's lane trigger fires inside `classify()` before `planner_output` is read at
all. The `other =>` arm in `context_repo.rs` was unreachable code.

Card 22 also froze a rule: **no guessing the facet mapping before the alignment clause is
written**, because a guessed mapping makes G25-1 accidentally green on a small fixture (§80.1).

## Ruling consumed

Web-GPT Pro, 2026-09-18, archived at
`/Volumes/data/output/humaux-thread-stable-system-20260828/delivery-cards-20260903/research/card22b_facet_task_ruling.md`
(conversation "Rust记忆系统规范裁决"). Its §25.4.A clause — 11 numbered points — is copied
**verbatim** into Baseline §25.4.A; this ADR records the decisions, the alternatives rejected
and the measurements, and does not paraphrase the clause.

## Decisions

**D-A — Task dimension = the binding table, not a column.** `task_explicit_context_v1` reads
`private.context_bindings` (`scope_kind='TASK'`, `scope_id` = the request's authenticated
TaskId, `mode='MANDATORY'`, `revoked_at IS NULL`). No `task_id` on `evidence_objects` or
`memory_records`, no change to `remember.put`. Rationale (ruling §三.1): `remember.put(task_id=T)`
expresses **provenance** ("this input was ingested in task T"), while the selector needs
**authority** ("this memory version is authorized as T's context"). They are different
relations, and `memory_evidence` is many-to-many, so an Evidence-carried task would immediately
need a rule for "which task does a Memory inherit when its sources disagree".

**D-B — Facet = a generated column.** `private.memory_records.facet text GENERATED ALWAYS AS
(CASE memory_type …) STORED` + `CHECK (facet IS NULL OR facet IN ('state','constraints',
'decisions','issues'))`. The mapping is a pure function of one column of the same row, so the
engine — not a review rule — makes it un-writable: PostgreSQL rejects INSERT/UPDATE of a
generated column outright. No grant is needed and none is given (§6.2.2 bullet).

**D-C — `REJECTION -> decisions` is a NEW explicit product contract**, not a derivation from
§24's reserve names. An explicitly rejected route must stay deterministically reachable rather
than depending on similarity recall. It does not rewrite `memory_type` and does not merge
Continuity's separate Decision / Rejection slots.

**D-D — `memory.bind` / `memory.unbind`.** The ruling (§三.2) is explicit that a table is not a
relation: "表结构表达关系，受控写入建立关系". `domain::context::authorize_mandatory` had no
production caller and `ElevatedActor` had no minting path at all, so a qualified TASK MANDATORY
binding could not be created by any means. The two verbs mirror `memory.pin` / `memory.unpin`
exactly: the same §33.10 rule-9 confirm gate, the same envelope
(`context_repo::write_binding_confirmed`), the same two sole binding writers. What differs is
the authorization step — the envelope re-reads the memory's own `authority_class`,
`memory_type` and Evidence origin basis **in the same transaction** and hands them to
`authorize_mandatory`; the request may not supply them. `ElevatedActor::from_consumed_confirmation`
accepts only `MemoryBind` / `MemoryUnbind` and carries the memory id, and `authorize_mandatory`
now refuses an actor minted against a different memory (X's confirmation cannot bind Y) — the
same gate `authorize_pinned` already had.

**D-E — A->B replacement is `memory.bind {replaces_binding_id}`, not a third verb.** §25.4.A(9)
requires the revoke and the create to be one transaction; a separate unbind+bind cannot be.
The revoke uses the existing sole revoke site and treats zero rows affected as `Conflict`.

**D-F — One output branch, not four.** `memory.output.schema.json`'s `BindingWritten` already
carried `memory_id / binding_id / mode / state / scope`. The four ops differ only in three
closed values, so `mode` gained `MANDATORY`, `scope.kind` gained `TASK`, and `state` gained
`bound` / `unbound`. The output union stays at 15 branches, so the `mcp_catalog.rs`
`memory_output_accepts_bare_and_snapshot_page_shapes` length/order pin is unchanged. A fifth
`$defs` entry would have been a copy of the same five fields.

**D-H — The bind confirmation binds the PAIR (memory, task), not the memory alone.** §33.10
rule 9's token carries `(tenant, user, operation, target, successor)`; `memory.supersede` already
uses the successor leg for its second argument. `memory.bind`'s second argument is the TaskId, so
it rides the same leg. Without it, a confirmation minted for "bind S to task T" would execute
"bind S to task U" — the user confirmed one obligation and got another. The first call's
`target` therefore reports `{memory_id, task_id}` (a new `task_id` property on
`ConfirmationRequired.target`, with an `allOf` arm requiring both for the MANDATORY pair), and
`context_repo::validate_binding_write` checks the pair itself rather than calling
`application::pin::check_claim` — that function *requires* `successor_id` to be absent, because a
pin has no second argument, so the two rules are different assertions about different tuples and
cannot share one implementation. Witnessed: the live suite presents a (S, T) token against a
(S, U) request and asserts `CONFLICT`.

**D-I — `canonical_scope` carries `task_id` through.** It previously refused any scope with
`task_id` set and zeroed the field, so `task_explicit_context_v1` could never receive a TaskId
and would have been structurally empty even with a perfect predicate. A TASK is a selector
association range, not an authorization axis (§25.4.A(7)/(8)): it grants nothing on its own, and
the binding's target still runs the full authority/origin floor. `repository_id`, `run_id` and
`agent_id` stay refused, because no selector consumes them and a scope axis nothing reads is a
silent widening. The MCP wire is **unchanged**: `context.assemble {task_id}` is still a
declared-only route answering `DEPENDENCY_UNAVAILABLE` (`bins/gateway/src/context.rs` still
builds its scope with `task_id: None`). Plumbing the wire argument is a separate card; this card
only makes the in-process selector path able to receive a task at all.

**D-G — No index.** The facets predicate is `tenant_id = $1 AND facet = ANY($2)` over a
6,736-row table on dev, inside an assembly transaction that already runs four other scans of
the same table. **The decision rests on the dev-database plan in the Measurements table below —
`Index Scan` on `idx_memory_records_live`, p50 0.036 / p95 0.042 ms, n = 30 — not on the first
`EXPLAIN` taken while the work was still on a throwaway database.** That first plan read
`Seq Scan on memory_records`, cost 187.20..204.04, actual 1.0 ms, 9 rows, and it says nothing
about the shipped deployment: the planner takes a sequential scan on a 9-row table no matter
what indexes exist, because reading one page beats descending a btree. Two databases, two
plans, no contradiction — but only the 6,736-row one is evidence about production shape, and
that one is an Index Scan. `migrations/0172_memory_records_mandatory_facet.sql` lines ~33-35
still carry the throwaway `EXPLAIN`'s wording ("a sequential scan of that table is
sub-millisecond"); the migration is applied and therefore immutable (§46), so **this ADR is the
corrected record** and the migration comment is to be read as the earlier, smaller measurement.
The ruling's optional
`(tenant_id, facet, memory_id) WHERE facet IS NOT NULL` index is deferred — adding it later is
a `CONCURRENTLY` forward-fix with no contract change. The ruling's optional bindings index is
**not** added either: `ix_context_bindings_active_scope (tenant_id, scope_kind,
COALESCE(scope_id, tenant_id), mode) WHERE revoked_at IS NULL` already serves the seed query.

## Alternatives rejected

- **WHERE-only fix (ADR-0041 D-I's plan).** Rejected as measured dead code: the probe answers
  before the dispatch, so the arm is unreachable. Recorded so the next reader does not retry it.
- **`task_id` on `memory_records` / `evidence_objects`.** Rejected per D-A: provenance is not
  authority, and a scalar task on a many-to-many provenance graph needs an inheritance rule
  nobody has written.
- **A writable `facet` column set by the distiller / a backfill.** Rejected: that is the second
  source of truth §25.4.A(3) forbids, and it would make the probe's existence check meaningless.
- **Binding-table-only for task, with no write verb.** Rejected per D-D: the table existed
  before this card and the selector still had nothing to read, because nothing could create a
  qualified row.
- **A third verb `memory.rebind`.** Rejected per D-E.
- **A new `MandatoryBindingWritten` output branch.** Rejected per D-F.

## Measurements

Measured 2026-09-19 on the shared dev database (PostgreSQL 18.6, `private.memory_records` =
6,736 rows), `EXPLAIN (ANALYZE)` execution time of the two selector predicates this card adds,
warm, n = 30 each, unit = **ms**:

| predicate | plan | p50 | p95 | n |
| --- | --- | --- | --- | --- |
| `required_current_state_facets_v1` rows | Index Scan on `idx_memory_records_live`, filter on `facet = ANY(...)` | 0.036 | 0.042 | 30 |
| `task_explicit_context_v1` rows (binding EXISTS) | Index Scan + `ix_context_bindings_active_scope` semi-join | 0.060 | 0.076 | 30 |

`context.assemble` end to end over the live gateway, the card's required before/after, both
from the same suite's own print line
(`ADR-0041 D-I p50/p95 (n=30, ms): recall.search …/… · context.assemble …/…`, printed by
`native_gateway_semantic_recall_real_qdrant_pg_and_ryw_acceptance`):

| operation | when | n | unit | p50 | p95 |
| --- | --- | --- | --- | --- | --- |
| `context.assemble` | **before** (ADR-0044's table, 2026-09-18, pre-0172) | 30 | ms | 33 | 43 |
| `context.assemble` | **after** (this card's tree, 2026-09-20) | 30 | ms | 33 | 41 |
| `recall.search` | before (ADR-0044) | 30 | ms | 1173 | 1181 |
| `recall.search` | after (this card's tree) | 30 | ms | 1148 | 1161 |

So the two selectors leaving `Unavailable` and actually running cost `context.assemble` nothing
measurable: p50 unchanged at 33 ms, p95 inside the run-to-run spread of a loop whose own
`recall.search` leg moved 25 ms in the other direction on the same run. **The run that prints
this line currently exits 101**, on an assertion unrelated to latency and outside this card's
allowed files: `bins/gateway/tests/mcp_gateway.rs:2605` still pins the in-flight-fault leg of
the same suite to `context.assemble = cannot_establish/lane_failed`, the literal this card
reversed (the three-route loop at :2136 was updated, this second site was not; it now reads
`pipeline_count_mismatch`). The timed loop runs and prints before that assertion, so the
numbers above are measured, not estimated — but the suite is red until that second site is
corrected, which is a separate edit.

The per-predicate numbers above remain the honest before/after for the two *selectors*:
**before**, both selectors were
`Unavailable` and issued no query at all, so their "before" cost is exactly the probe round-trip
they already paid; **after**, each pays the probe plus the two enumerations above. What cannot
be produced is a *same-build* A/B: migration 0172 is `EXPAND_ONLY` and §46 forbids reversing it
on the shared database to re-measure. The end-to-end pair in the table above is the closest
real substitute and is a genuine pair — same suite, same loop, same fixture, same print line,
one card apart (ADR-0044's row, 2026-09-18, is pre-0172). The older pre-0172 number in
ADR-0041 D-I is **not** used: it was taken on a different fixture. What is additionally
recorded is the added work: two probe queries (`pg_attribute` instead of the slower
`information_schema.columns` view, for all five selectors) plus the four queries above, all
inside the existing single REPEATABLE READ assembly transaction.

The facets plan is an **Index Scan, not a Seq Scan**, which is what settles D-G: the `facet`
predicate rides `idx_memory_records_live`'s tenant leg and filters in place at 0.04 ms, so the
optional `(tenant_id, facet, memory_id)` index would buy nothing today.

Table rewrite for the generated column on dev: 6,736 rows, completed inside the single
`cargo xtask migrate` step; every pre-existing row's facet matches the CASE of its own
`memory_type` (migration postcheck, and `facet_contract.rs`'s stored-value leg).

## §80.1 fault injection — red turns green

Run on a **throwaway** database (`fix22b_<ts>`), never on the shared dev DB, because it mutates
the shipped generated expression.

```
ALTER TABLE private.memory_records
  ALTER COLUMN facet SET EXPRESSION AS (
    CASE memory_type
      WHEN 'CONSTRAINT' THEN 'constraints'
      WHEN 'DECISION'   THEN 'decisions'
      WHEN 'REJECTION'  THEN 'decisions'
      WHEN 'ISSUE'      THEN 'issues'
      ELSE NULL END) STORED;   -- the STATE arm removed; PG18 recomputes STORED values
```

Note the exact spelling: PostgreSQL 18 rejects a trailing `STORED` on `SET EXPRESSION`
(`syntax error at or near "STORED"`); the clause is `ALTER COLUMN facet SET EXPRESSION AS (...)`
and the column stays STORED. Measured red tail (`crates/adapters/tests/facet_contract.rs`
against the throwaway DSN, 2026-09-19):

```text
running 3 tests
test memory_type_labels_round_trip_between_db_and_rust ... ok
test facet_column_is_stored_generated_and_matches_the_golden ... FAILED
test registry_required_columns_name_real_relations ... ok

---- facet_column_is_stored_generated_and_matches_the_golden stdout ----
panicked at crates/adapters/tests/facet_contract.rs:169:9:
assertion `left == right` failed: live generated expression disagrees with the §25.4.A(2) GOLDEN for STATE
  left: Some(None)
 right: Some(Some("state"))

test result: FAILED. 2 passed; 1 failed; ...
```

The green control on the same throwaway database immediately before the mutation was
`3 passed; 0 failed`, so this is a red-turns-green record and not a broken fixture. The DDL
succeeded, the two unrelated legs still passed — the mutant reached the target execution path
and failed only on the business assertion.

Second mutant on the same throwaway database — **replace the generated column with a writable
one of the same name and type** (`DROP CONSTRAINT ... ; DROP COLUMN facet; ADD COLUMN facet
text`), which is precisely the §25.4.A(3) violation an existence-only probe cannot see:

```text
---- registry_required_columns_name_real_relations stdout ----
panicked at crates/adapters/tests/facet_contract.rs:299:17:
assertion `left == right` failed: private.memory_records.facet must be STORED generated (§25.4.A(11))
  left: ""
 right: "s"
```

The throwaway database was created for these two runs and dropped immediately after; the shared
dev database was never mutated. The **empty-database full migration path** (ruling §五.6) is
covered by the same throwaway: `xtask migrate --dsn <throwaway>` applied all 140 migrations from
nothing, `pass (140 applied, 0 already-applied, 140 total)`.

Mutant matrix, each confirmed to reach its target path:

| mutant | expected red |
| --- | --- |
| drop the STATE arm from the generated expression | facets exact set loses the State rows; Task lane unchanged |
| drop `cb.scope_id = $2` from the task predicate | task selector returns another task's target |
| drop `cb.mode = 'MANDATORY'` | PINNED/SUPPLEMENTAL rows admitted |
| drop `cb.revoked_at IS NULL` | a revoked binding comes back to life |
| GOLDEN `Rejection -> NULL` | `facet_contract.rs` both halves red |
| probe checks existence only (drop `attgenerated='s'`) | a writable `facet` passes as available |

## Open debt this card did NOT close

`task_explicit_context_v1`'s `min_authority` is `ExplicitTaskContext` (§25.4's frozen
mechanical rule, kept). §10.1's origin ceiling table has **no origin that reaches that class** —
the maximum is `ProjectConstraint` (`TenantAdmin`, or `DirectUserInput`/`UserConfirmed` on a
Constraint memory). §10.1 hard rule 2 says the class "only comes from the current authenticated
Task Request / Tenant Policy", and `domain::policy::OriginBoundAuthorityPolicy` implements only
the ceiling table, so it cannot express rule 2 at all.

Measured consequence: `memory.bind` can create a TASK/MANDATORY binding for a
`ProjectConstraint` memory (and `explicit_mandatory_bindings_v1` reads it back at the TASK
scope), but `task_explicit_context_v1` admits nothing, because nothing can legitimately hold
`ExplicitTaskContext` today. The selector therefore **runs and returns an empty admitted set**
instead of failing the lane — which is this card's unlock — but it is not yet load-bearing.
DOD-035 is still `[phase=14]`. The producer for §10.1 rule 2 belongs in
`crates/domain/src/policy.rs` / §10.1, outside card 22b's allowed files.

## Files touched outside card 22b's allowed list (reported, not hidden)

Four edits were forced by this change and could not be made inside the allowed list. All are
named here rather than left as a red gate:

1. `crates/adapters/src/postgres.rs` (test-only module). Its `insert_probe_ok` /
   `insert_probe_denied` helpers ran `INSERT INTO t SELECT * FROM t WHERE false`, which stops
   being schema-agnostic the moment a table gains a `GENERATED ALWAYS ... STORED` column: `*`
   expands to include `facet` and PostgreSQL answers "cannot insert a non-DEFAULT value into
   column", a shape error that has nothing to do with the permission the probe measures
   (`postgres::tests::g6_db2_private_worker_pool` went red on exactly that). The probes now name
   the non-generated columns (`pg_attribute.attgenerated = ''`), which restores the property the
   helper's own doc comment claims.
2. `crates/protocol/src/mcp_catalog.rs`'s `memory_output_accepts_bare_and_snapshot_page_shapes`.
   It pinned `BindingWritten.scope.kind` to `const: "WORKSPACE"`; with the branch reused by the
   MANDATORY pair, that const becomes a closed two-value enum. The pin was widened in the same
   shape, and `mode` / `state` gained the same closed-set assertions so the widening is itself
   pinned. The union length stayed 15, so the length/order assertions are untouched.

3. `crates/domain/src/confirm.rs`. `DestructiveOp` gains `MemoryBind` / `MemoryUnbind` and its
   `ALL` goes 9 -> 11. The card's allowed list named `context.rs` / `memory.rs` / `error.rs` on
   the assumption that the gated-op enum lived there; it does not — `confirm.rs` is the single
   spelling of the confirm-token op set, and a new gated op cannot be added anywhere else.
4. `crates/adapters/tests/support/operation_receipt_fixture.rs`. Teardown gained
   `DELETE FROM coord.tasks`: `coord.tasks.tenant_id` is an FK to `control.tenants`, so once the
   fixture seeds a task for the TASK-scoped binding, dropping the tenant first fails the
   constraint and leaves the fixture undeletable. Added by the fix stage, not the build stage.

`crates/adapters/tests/mandatory_context_lane.rs`'s
`probe_reports_the_columns_that_are_actually_missing` was rewritten in place (same directory the
card allows for contract/fault tests): it hard-coded `memory_records.task_id` / `.facet` as the
two columns the probe reports, which is exactly the claim this card reverses. It now derives the
expected set from `REGISTRY.required_columns` itself and additionally asserts §25.4.A(11)'s
"never probe `memory_records.task_id` again".

## Gate record (2026-09-19, this machine)

```text
cargo xtask migrate            -> 0   (0 applied, 140 already-applied, 140 total; drift 0)
cargo xtask rls-check          -> 0
cargo xtask architecture-check -> 0   (incl. §25.4 A1/A2/A3 and §33 tool-surface truth)
cargo test -p humaux-domain   --lib                        -> 0   215 passed
cargo test -p humaux-protocol --lib                        -> 0    42 passed
cargo test -p humaux-adapters --lib --test facet_contract  -> 0   137 + 4 passed
cargo test -p humaux-gateway  --test mcp_gateway --test continuity_get
                                                           -> 0    28 passed / 3 ignored, 2 passed
cargo clippy --workspace --all-targets -- -D warnings      -> 0
cargo fmt --all --check                                    -> 0
secret grep                                                -> 0
```

New tests, by name:

- `humaux_domain::memory::tests::{memory_type_wire_round_trips, mandatory_context_facet_wire_round_trips, facet_golden_twelve_entries}`
- `humaux_domain::context::tests::elevated_actor_is_bound_to_one_op_and_one_memory`
- `crates/adapters/tests/facet_contract.rs::{facet_column_is_stored_generated_and_matches_the_golden, facet_expression_and_stored_values_match_the_golden, memory_type_labels_round_trip_between_db_and_rust, registry_required_columns_name_real_relations}`
- `bins/gateway/tests/mcp_gateway.rs::native_mcp_memory_bind_task_and_facet_selector_exact_sets_acceptance`

Not runnable here: `crates/adapters/tests/mandatory_context_lane.rs` (5 tests) demands the
isolated request-guard fixture at `127.0.0.1:61719/humaux_thread_request_guard_20260828`, which
this node does not run; under `HUMAUX_REQUIRE_DB=1` its skip is a failure by design. It compiles,
and its `probe_reports_the_columns_that_are_actually_missing` was rewritten to derive its
expectations from `REGISTRY` rather than from the two hard-coded column names this card removed.

## Post-review corrections (2026-09-20, Opus review of card 22b)

Six findings, five fixed here at the root, one left open with its blocker named.

**P0 — `replaces_binding_id` revoked any active binding in the tenant.** `apply_binding_write`
passed the raw wire uuid to `revoke_binding_in_txn`, which filters only on
`(context_binding_id, tenant_id, revoked_at IS NULL)` — no `mode`, no `scope_kind`, no
`scope_id`. The confirm token binds (tenant, user, op, memory, task) and never covered this
argument (`guard.rs`, `validate_binding_write`), so one legitimately minted bind confirmation
could revoke another user's PINNED row or another task's MANDATORY row and still answer 200
`state:"bound"`. Root cause: the only caller-supplied binding id in the file reused the writer
built for *derived* ids (`active_pinned_binding_in_txn` / `active_task_mandatory_binding_in_txn`
both derive theirs, which is why the pin path was never exposed). Fix: a second writer,
`revoke_task_mandatory_binding_in_txn`, whose `WHERE` carries the dimension this operation is
authorized for — `scope_kind='TASK' AND scope_id=<this task> AND mode='MANDATORY'`. Anything
outside it matches zero rows ⇒ `Conflict`, per ruling §四.4 ("必须断言返回恰好一行；零行按并发
冲突或幂等协议处理"). Self-replacement (`replaces_binding_id` == the existing (memory, task)
binding) is refused before the UPDATE: it would revoke the very row the idempotent
`PinAction::ReturnExisting` arm reports back as bound.

**P1 — migration 0172's §46.1 postcheck was a false assertion.** `has_column_privilege(...,
'facet','INSERT')` is TRUE for `role_gateway` and `role_private_worker` (measured on
humaux_thread_dev, PostgreSQL 18.6): a table-level grant covers every column in the ACL, and
ACLs know nothing about `attgenerated`. `pg_column_is_updatable` is no better — measured TRUE
for this column, it answers for views. The security property itself holds (`attgenerated='s'`,
asserted at the top of the same postcheck); only the evidence was wrong. The four ACL legs are
replaced with the assertion this migration can honestly make — it granted nothing of its own
(`attacl IS NULL`) — and §6.2.2's claim about that evidence is rewritten to match.
Red-turns-green, both runs extracting the manifest's `postcheck` block verbatim and executing
it on the dev DB: **before `postcheck_ok = f`, after `postcheck_ok = t`**. (Note the postcheck
is still never executed by `cargo xtask migrate`, which reads only `.sql`; the manifest edit
changes no checksum and is not a re-application of 0172.)

**P1 — `context.assemble` hard-coded `task_id: None` on the wire.** The MCP route listed
`task_id` among the fail-closed "valid schema field, no semantics" arguments, so the task
dimension was reachable only through the adapter's `selector_outcomes` test entry point. D-I
said plumbing it was "a separate card"; the review is right that this card's allowed-files list
names `bins/gateway/src/context.rs` for exactly this. `assemble` now takes the requested task
and puts it in the Scope; `mcp_application::context_assemble` parses it off the wire.

**P1 — §25.4.A(7)'s "本次已认证解析的 TaskId" was asserted but not implemented.** The TaskId was
caller-supplied on both paths with no check of any kind. It is now RESOLVED in-transaction
against `coord.tasks` (tenant-scoped, RLS-isolated, §48) on both the read path
(`run_selectors_in_txn`, before any selector) and the write path (`write_binding_confirmed`,
before a binding is written) — an id naming no task of this tenant is `NOT_FOUND`. Ceiling,
stated rather than implied, in the code and in §25.4.A's new boundary list: `coord.tasks` has no
owner/participant column, so "the caller belongs to this task" is not checkable in this schema
and is NOT asserted; what bounds it is `readable_memory_ids`/`can_read` on every selector row
and the §33.10 confirm gate on every binding write.

**P1 — `replaces_binding_id` had zero test coverage.** New:
`bins/gateway/tests/mcp_gateway.rs::native_mcp_memory_bind_replacement_is_scoped_to_the_authorized_binding`,
through the real two-call confirm flow, no raw INSERT: the authorized A→B replacement (A
revoked, B created, **exactly one** active MANDATORY row left on the task), the replay (A
already revoked ⇒ zero rows ⇒ `Conflict`, and C is not bound), and the three blast-radius legs
that were the P0 — another task's MANDATORY binding, a PINNED/WORKSPACE row, a uuid that is no
binding at all — each `Conflict` with the named row asserted still active afterwards. Plus
self-replacement ⇒ `Conflict`, and an unresolvable task ⇒ `NOT_FOUND` with zero rows written
under the invented scope. Each of the three P0 legs is red on the pre-fix code (it revoked the
named row and returned `state:"bound"`).

**P1 — "Task(T) == {S} … unresolved empty" is still not asserted.** Unchanged and still open;
see "Open debt this card did NOT close" above. It is not a defect inside this card's diff: the
selector's frozen `min_authority` is `ExplicitTaskContext` and §10.1's ceiling table has no
origin that reaches it, while §10.1 hard rule 2 says the class comes from "当前经过认证的 Task
Request / Tenant Policy" — a producer `EvidenceOriginClass` has no variant for. Closing it means
editing `crates/domain/src/evidence.rs`, `crates/domain/src/policy.rs` and §10.1 (a frozen
section), all three outside this card's allowed files, and it first needs a ruling on the
contradiction between §10.1's table (nothing reaches class 6) and §10.1 rule 2 (two named
sources do). The witness continues to assert what is true today — admitted set empty, the
nomination reported as an unresolved obligation — with the comment that says to assert the real
set here rather than loosen this once the producer lands.

### §80.1 fault injection for the review fixes — red turns green

| mutant | expected red | observed |
| --- | --- | --- |
| `replaces_binding_id` back through the unscoped `revoke_binding_in_txn` (the pre-fix code) | the cross-task leg admits the replacement | RED: request 41 returned `isError:false`, `state:"bound"`, `inserted:true` — task U's MANDATORY binding revoked by a bind authorized for task T |
| drop both `resolve_task_in_txn` calls | the ghost-task legs stop refusing | RED ×3: write path (request 53) bound `c` under a uuid that is no task; read path (requests 42 and 77) assembled normally instead of `NOT_FOUND` |
| 0172 postcheck, extracted verbatim and executed on humaux_thread_dev | the ACL legs are false assertions | RED before (`postcheck_ok = f`), GREEN after the predicate swap (`postcheck_ok = t`) |

### Gate record for the review fixes (2026-09-20, this machine)

```text
cargo xtask migrate            -> 0   (0 applied, 140 already-applied, 140 total; drift 0)
cargo xtask rls-check          -> 0
cargo xtask architecture-check -> 0   (incl. §25.4 A1/A2/A3)
HUMAUX_REQUIRE_DB=1 cargo test -p humaux-gateway --test mcp_gateway --test continuity_get
                               -> 0   29 passed / 3 ignored (pre-existing), 2 passed
HUMAUX_REQUIRE_DB=1 cargo test -p humaux-adapters --lib --test facet_contract
                               -> 0   137 + 4 passed
cargo clippy --workspace --all-targets -- -D warnings      -> 0
cargo fmt --all --check                                    -> 0
secret grep                                                -> 0
```

New test, by name:
`bins/gateway/tests/mcp_gateway.rs::native_mcp_memory_bind_replacement_is_scoped_to_the_authorized_binding`.
Two existing tests gained assertions rather than losing them:
`native_mcp_memory_bind_task_and_facet_selector_exact_sets_acceptance` (its two tasks are real
`coord.tasks` rows now, and it asserts the task on the wire plus the unresolvable-task refusal)
and `native_mcp_context_preserves_governance_on_overflow_and_rejects_unimplemented_routes`
(`task_id` moved off the fail-closed list and onto a `NOT_FOUND` resolution assertion).

One more file outside card 22b's allowed list, named rather than hidden:
`crates/adapters/tests/support/operation_receipt_fixture.rs` — its teardown deletes the tenant,
and `coord.tasks.tenant_id` is an FK, so a fixture that now seeds a task could not drop its
tenant. One `DELETE FROM coord.tasks WHERE tenant_id=$1` in the existing teardown list; without
it every task-seeding test fails in cleanup (observed) rather than in its assertions.
