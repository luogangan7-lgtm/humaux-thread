# ADR-0046 — ExplicitTaskContext is a verified, non-inheritable task authorization, not a content property

- Status: Accepted
- Date: 2026-09-20
- Card: 22c (delivery cards 20260903)
- Closes ADR-0045's "Open debt"; retires the `task_explicit_context_v1` registry record.
- Spec landing: Baseline_2.9 §10.1 (stored authority / task authorization split), §25.4.B, §6.2.2, §33.10.
- Migration: `migrations/0173_task_binding_grants.sql` (+ manifest).

## Context

ADR-0045 unlocked `task_explicit_context_v1` — the selector ran instead of being reported
column-unavailable — and left a debt it measured rather than guessed: the selector's frozen
`min_authority` is `ExplicitTaskContext` (6), and §10.1's origin ceiling table lets **no**
origin reach 6. §10.1's own hard rule 2 names a producer ("当前经过认证的 Task Request /
Tenant Policy") that `EvidenceOriginClass` does not model at all. So:

```text
∀m: authority_stored(m) ≤ ceiling(origin(m)) ≤ ProjectConstraint(5) < ExplicitTaskContext(6)
```

The selector's admitted set was therefore **structurally** empty — not "empty today", empty by
arithmetic, on every deployment, forever. Card 22b asserted exactly that rather than relaxing
the assertion (`native_mcp_memory_bind_task_and_facet_selector_exact_sets_acceptance`), and
`domain::policy`'s `no_origin_ceiling_reaches_explicit_task_context` pinned the other half.

## Ruling consumed

Web-GPT Pro, new conversation, 2026-09-20 12:24→12:45 (20m41s of reasoning, 4 tool calls),
archived VERBATIM at
`/Volumes/data/output/humaux-thread-stable-system-20260828/delivery-cards-20260903/research/card22c_task_authority_ruling.md`.

Verdict: **Option B, tightened**. Stored authority stays ≤ 5; 6 becomes the USE authority of a
"current task context instance", proven by a grant row. No new `EvidenceOriginClass`, no change
to the 9-value origin CHECK, no lowering of the threshold. Its §3 contract table is copied
verbatim into Baseline §10.1; its three invariants are copied verbatim below. This ADR records
decisions, deviations, measurements and the fault record — it does not paraphrase the ruling.

## The three invariants (verbatim from the ruling)

```text
I-STORE:      所有 MemoryRecord 的 authority ≤ 原 authority-basis ceiling ≤ 5。
I-TASK:       有效上下文 authority=6 ⇒ 存在当前任务、精确目标版本、明确用途的有效授权证明。
I-NONINHERIT: 派生、复制、检索、重排、迁移、换任务、替换版本都不能创建或继承上述授权证明。
最终裁决：把 6 从内容属性改为受验证的、不可继承的当前任务授权。
```

## Decisions

**D-A — Stored authority refuses 6 directly, not as a side effect of the ceiling table.**
`humaux_domain::policy::StoredAuthority::authorize` returns `Err(OriginAuthorityCeiling)` for
`ExplicitTaskContext` **before** any ceiling comparison; `OriginBoundAuthorityPolicy` is now a
one-line delegation to it, so there is exactly one place a fault injection can open. The
ceiling table already made 6 unreachable *indirectly* — an indirect guard fails silently the
day the table moves. Pinned by `stored_authority_refuses_explicit_task_context`, which asserts
both the nine-origin sweep and (separately) that 5 still passes, so the gate cannot go green by
rejecting everything. DB half: `memory_records_stored_authority_v2_check` (0173).

**D-B — The authorization object is a row, and it binds everything the ruling's §二.1 lists.**
`private.task_binding_grants` carries tenant + task + `task_epoch`, the exact
`context_binding_id` (composite FK back to the binding's whole shape, so a grant cannot outlive
a change of task/memory/mode), `memory_id` + `payload_sha256`, `purpose`, `issuer_kind`,
`issued_by_principal_id`, `authorization_evidence_id` (FK ON DELETE RESTRICT), `operation_id`
(UNIQUE per tenant), `policy_version`, `issued_at`/`expires_at`/`revoked_at`. `grant_authority`
is CHECK-pinned to 6: this table exists to carry one authority and must not become a second,
unreviewed ladder.

**D-C — Deviations from the ruling's SQL template, and why.** The card's "Facts measured"
section wins where it contradicts the template; each deviation is recorded here per the card.

| Ruling template | This tree | Why |
|---|---|---|
| `memory_revision bigint >= 1` column | dropped; exact version = `memory_id` + `payload_sha256` | Memory rows here are immutable and `supersede` mints a NEW `memory_id`. There is no revision column to reference, and inventing one would create a second version axis nothing writes. |
| `CHECK (authority_class BETWEEN 0 AND 5)` then `VALIDATE` | `CHECK (authority_class <> 'ExplicitTaskContext') NOT VALID`; manifest postcheck reports (and bounds) the offender count | `authority_class` is TEXT (PascalCase labels), not a smallint. And the dev database already holds four legacy rows at 6 (below) — `VALIDATE` would fail the migration, and mass-downgrading them is forbidden by ruling §五.3. VALIDATE is a card-24 step after the four rows are dispositioned. |
| `UNIQUE (tenant_id, binding_id, ...)` "reuse an equivalent key if present" | added `context_bindings_task_grant_identity_v2_uq` | `private.context_bindings`' PK is `context_binding_id` alone; there was no composite key the FK could reference. The new UNIQUE is trivially satisfied (its first two columns are already unique), so it is additive with no rejection risk. |
| task epoch assumed to exist | `coord.tasks.authorization_epoch bigint NOT NULL DEFAULT 0` added | The task table modelled only existence + tenant. No task lifecycle operation exists in this tree, so the column has exactly one future writer: whoever adds that operation. Card 22c writes it nowhere. |
| `expires_at` NOT NULL-ish (`CHECK expires_at > issued_at`) | nullable, with the same CHECK when present | A NULL expiry means "the task's own epoch and the revoke path are the lifetime", which is the honest default here; a wall-clock expiry nobody sets would be a number invented to fill a column. |

**D-D — `purpose` is on the wire, is required, and is covered by the confirm token.**
`memory.bind` gains `purpose ∈ {REFERENCE_ONLY, ADOPT_TASK_INSTRUCTION}` with **no default**:
"reference or instruction" is the whole question the field exists to answer, and a defaulted
answer is one nobody gave. Only `ADOPT_TASK_INSTRUCTION` writes a grant.

Card 22b's review lesson was "count the parameters the confirm token covers one by one; an
uncovered parameter must not carry permission". `control.confirm_tokens` binds
(tenant, user, operation, target, successor) and has no room for a third argument, so `purpose`
rides the **successor** leg: for the MANDATORY pair the successor id is now
`sha256("humaux.memory.binding.intent.v2\0" || task_id || "\0" || purpose)[..16]`, i.e. the
whole confirmed intent, instead of the bare task id. A token minted for `REFERENCE_ONLY` simply
is not the token an `ADOPT_TASK_INSTRUCTION` call presents; the consume matches zero rows and
the call is `CONFLICT`. The pair binding card 22b added (a confirmation for (S,T) cannot execute
(S,U)) survives unchanged because the task is still inside the digest.

*Rejected alternative:* adding a `purpose` column to `control.confirm_tokens`. It is a
migration-immutable table outside this card's file set, and the successor leg already exists
for exactly this purpose (§33.10 rule 9 reserves it for "an operation's second argument").

**D-E — The authorization Evidence is a new row, and it is deliberately NOT linked to the
memory.** Each `ADOPT_TASK_INSTRUCTION` bind inserts one `UserConfirmed` EVENT whose
`payload_sha256` digests the complete intent
(`policy_version|tenant|task|epoch|binding|memory|content-digest|purpose`), and the grant's FK
points at it. It is **never** inserted into `private.memory_evidence`: a task-authorization
receipt is not a general-purpose authority basis, and linking it would let the next
`AuthorityPolicy::authorize` read a `UserConfirmed` origin that nobody asserted about the
*content* (ruling §五, last line: 授权材料不洗白). `reasoning_domain_id` is copied from the
target memory's own Evidence in the same transaction — the domain is a property of where the
memory lives, and deriving it from anywhere else would be a second source of truth.

**D-F — Nominated is enumerated from obligations, never from admissions.** The read path is
`WITH nominated AS MATERIALIZED (bindings ...) ... LEFT JOIN grants ... LEFT JOIN
memory_records`, with **no** authority filter, **no** `g.revoked_at IS NULL`, **no**
`m.status='active'` and **no** INNER JOIN. `MATERIALIZED` is not a performance hint: it stops
the planner from pushing the outer filters back into the obligation enumeration, i.e. from
re-doing the thing we just refused to do. `candidate_ids` (the lane's `expected` oracle) is the
full obligation set — **including obligations whose target is unreadable**, unlike the other
four selectors, whose candidates are "rows matching a predicate". Admission is
`domain::context::authorize_task_item`, in the ruling's §四.2 order; its per-obligation reject
reason is readable through `context_repo::task_obligation_report`.

*Rejected alternative:* adding a `rejected` field to `SelectorOutcome::Ran`. That enum is also
constructed by `crates/retrieval`, which is outside this card's file set, and the honest
denominator (N) is already carried by `candidate_ids`. The report is a separate read that
shares the **same** function as the lane, so the two cannot drift.

**D-G — `AuthorityRequirement` in the registry, with `min_authority` kept as a derived field.**
`SelectorSpec` gains `authority: AuthorityRequirement::{StoredAtLeast, VerifiedCurrentTaskBinding}`.
`min_authority` stays as an `AuthorityClass` **field** because `crates/retrieval`'s tests read
it as one and that crate is outside this card's file set; it is now defined as
`authority.stored_floor()` and a registry test (`registry_min_authority_is_derived_from_requirement`)
turns red the moment the two drift. The v2 stored floor is `PrivateKnowledge`, not
`ProjectConstraint`: ruling §二.4 rejects option C precisely because a 5-floor excludes the
legitimate `UserConfirmed(4)` target the positive control uses. It excludes only
`PublicKnowledge` — public-pool content is not a tenant's task instruction source.

**D-H — v1 is retired, not silently re-pointed.** `SelectorId::TaskExplicitContextV1` stays as
the Rust *slot* identifier (it is referenced by variant name across `retrieval`, the gateway and
the tests), but `SelectorSpec::registered_name` is now `task_explicit_context_v2`, and
`RETIRED_SELECTORS` keeps v1's record with its retirement reason and successor. Ruling §三:
同名 selector 不得偷换准入对象.
**Measured correction (review, 2026-09-22): this landed in the registry only, not on the wire.**
`SelectorSpec::registered_name` has zero non-test readers; the name a client actually sees —
`Handoff.mandatory[].selector`, `needs_verification[].selector`, `unavailable_selectors` — comes
from `crates/retrieval/src/handoff.rs::selector_wire`, which still maps
`SelectorId::TaskExplicitContextV1 => "task_explicit_context_v1"`. So today the admission object
was swapped under the old observable name, which is the substitution §三 forbids. `retrieval` is
outside this card's allowed files, so it is recorded in Open debt below rather than fixed here.

**D-I — Two sole-site pins, both mechanical.** architecture-check gains arm **A3b**:
`INSERT INTO private.task_binding_grants` / `UPDATE private.task_binding_grants` may appear only
in `crates/adapters/src/context_repo.rs`, and the literals `VerifiedTaskGrant {` /
`AuthorizedTaskItem {` only in `crates/domain/src/context.rs`. Field privacy does not stop a
sibling module in the same crate from writing the literal, so `_priv: ()` is a convention until
a needle pins it.

**D-J — The MANDATORY write floor moves to the v2 stored floor.** `authorize_mandatory`'s
extra floor was `ProjectConstraint` (card 22b): a stand-in for "this memory deserves Mandatory",
written when a TASK/MANDATORY binding **was** the admission. It no longer is. The ruling (§二.4)
rejects a 5-floor by name, because it excludes the legitimate `UserConfirmed(4)` target the card
requires as the positive control — so the card's own acceptance is unreachable without this
change. The floor is now `AuthorityRequirement::VerifiedCurrentTaskBinding.stored_floor()`
(`PrivateKnowledge`: not public-pool content). What did **not** move:
`explicit_mandatory_bindings_v1`'s SQL still admits only `ProjectConstraint`, so a lower-authority
TASK binding cannot enter Mandatory through that selector, and v2 admits it only with a grant.
§25.4's attack path (one induced write pinning low-origin content into Context forever) is closed
by the authorization, not by a number. Pinned by
`mandatory_write_floor_is_the_v2_stored_floor_not_project_constraint`, which asserts both ends —
`PublicKnowledge` still refused, `UserCorrection` now admitted — so it cannot go green by
admitting everything.

**D-K — `require_behavior_eligible_target` runs on the WRITE path too, and D-J is why it had to
be added** (post-review fix, 2026-09-22). The ruling §四.4 freezes the bind write order as
`… → authorize_mandatory → require_behavior_eligible_target → insert_binding_and_task_grant →
audit`, and Baseline §25.4.B(4) already carried it verbatim (「复核 §10.1 → 行为资格 → 写绑定 +
授权」) — the spec was right, the code skipped it. `apply_binding_write` went straight from
`authorize_mandatory` to
the insert, and `StoredAuthority::authorize` does not supply the check either — it refuses only
`requested == ExplicitTaskContext` or `requested > ceiling`. D-J made the gap **reachable**: at
the old `ProjectConstraint` floor, a `ToolResult`/`ExternalContent`/`UploadedArtifact`/
`SystemMigration` origin (ceiling `PrivateKnowledge`, `max_disposition` = `DataOnly`) could never
clear the floor; at the v2 floor it sits exactly on its ceiling and clears it. The read side does
fail closed (`TaskContextReject::UntrustedInstruction`), so no authority ever leaked — but the
transaction still **committed** a `task_binding_grants` row with `purpose =
ADOPT_TASK_INSTRUCTION` and a `UserConfirmed` authorization Evidence, i.e. a durable record
asserting that the user approved, as an instruction, content §10.1 row 4/5 says may never be one.
The fix is one domain function, `humaux_domain::context::require_behavior_eligible_target(basis)`,
returning the same `UntrustedInstruction` reason as the read side (one §10.1 judgement, not two
error surfaces), called from the ADOPT_TASK_INSTRUCTION branch of `apply_binding_write` between
`authorize_mandatory` and the insert. REFERENCE_ONLY is deliberately **not** gated: it mints no
authorization, asserts nothing about behaviour eligibility, and its obligation is rejected by name
on the read side regardless — gating it would be a new restriction with no ruling behind it.
Fault: F-R8. Witness: the live acceptance test's DATA_ONLY control (a `ToolResult` origin stored
at `PrivateKnowledge`), which asserts `FORBIDDEN`, zero bindings **and** zero grant rows, and then
that the same target still binds REFERENCE_ONLY.

## Measurements on this tree / DB (2026-09-20, humaux_thread_dev)

**The four legacy `ExplicitTaskContext` rows** (fixture leftovers; content
`{"fixture": "operation receipt scoped context"}`):

| memory_id | tenant_id | created_at | memory_type | status |
|---|---|---|---|---|
| `01a06b29-48d5-708e-b85f-94088bdf2126` | `d0a7e258-4873-4630-b011-a09cc7664eca` | 2026-09-04 06:44:22.862506+00 | NOTE | active |
| `01a06b29-48e6-730f-bf34-e024f1b6777e` | `d0a7e258-4873-4630-b011-a09cc7664eca` | 2026-09-04 06:44:22.884661+00 | NOTE | active |
| `01a07aa2-a11f-721f-9b13-207cc21f8a2e` | `5c853c6f-b8d4-422a-8818-d3a3390e7112` | 2026-09-07 06:51:13.565741+00 | NOTE | active |
| `01a07aa2-a123-7182-9d02-ef72d4bdb014` | `5c853c6f-b8d4-422a-8818-d3a3390e7112` | 2026-09-07 06:51:13.570124+00 | NOTE | active |

They are left in place. Card 24 dispositions them one by one and then runs
`ALTER TABLE private.memory_records VALIDATE CONSTRAINT memory_records_stored_authority_v2_check`.

**Other measured facts that shaped the migration**: `authority_class` is TEXT with a 7-label
CHECK (0004); `private.context_bindings`' PK is `context_binding_id` with no composite unique;
`coord.tasks` has no epoch; `private.evidence_objects` already carries
`UNIQUE (tenant_id, evidence_id)`; the role set is the frozen 8 + `role_migration_owner`.

**A structural consequence, measured rather than assumed.** Once `ExplicitTaskContext` cannot be
stored, the only class at or above `explicit_mandatory_bindings_v1`'s floor
(`ProjectConstraint`) is `ProjectConstraint` itself — and `project_active_constraints_v1`
selects every active `ProjectConstraint` row in the tenant unconditionally. So a row that only
an explicit MANDATORY binding can deliver no longer exists, and a PINNED row that clears its
floor is always also a Mandatory row (counted in `pinned_excluded` by the existing
`excluding_mandatory` rule). This is not a capability loss: delivering a memory **below**
`ProjectConstraint` deterministically into Mandatory is exactly what the v2 authorization path
does, with an audit trail, a content version and a revoke. The fixtures that used a stored 6 to
manufacture "above the floor but not a project constraint" were rewritten to the new truth, not
loosened — see the fault record.

## Fault record (red → green)

Each gate below was reached by a directed mutation and observed red before the fix was in
place. Layer matters (ruling §七 last paragraph): a Rust check deleted while the DB still
refuses is **not** evidence that the Rust gate works, so the Rust and DB halves are recorded
separately. The mutation was reverted after every arm and the green-back re-run is quoted.

Raw tails: `delivery-cards-20260903/gates_card22c_faults_rust.log` (Rust layer) and
`gates_card22c_faults_db.log` (DB layer). The DB arms ran on a **throwaway** database,
`fix22c_20260922100216` (`createdb` + all 141 migrations), never on `humaux_thread_dev`.

### Rust layer

| # | Mutation | Test turned red | Observed |
|---|---|---|---|
| F-R1 | `StoredAuthority::authorize`: delete the three-line direct `ExplicitTaskContext` refusal | `humaux-domain --lib stored_authority_refuses_explicit_task_context` | `FAILED … left: Err(UntrustedInstruction) right: Err(OriginAuthorityCeiling)` (policy.rs:267) |
| F-R2 | `authorize_task_item` step 3: a TASK/MANDATORY binding mints its own grant from the obligation | `task_explicit_context_v2_negative_control_missing_authorization` | `left: Ok(AuthorizedTaskItem { … grant_authority: 6, purpose: Some(AdoptTaskInstruction) … }) right: Err(MissingTaskAuthorization)`; the **positive control stayed ok** |
| F-R3 | admission becomes an authority *property* again: require `stored_authority >= ExplicitTaskContext`, drop the stored-6 read-side refusal | `task_explicit_context_v2_positive_control` | `FAILED` at context.rs:1921 (`正对照必须准入`); the **negative control stayed ok** |
| F-R4 | delete `grant.task_epoch != task.authorization_epoch()` | `task_authorization_rejects_each_broken_dimension` | `FAILED` at context.rs:2002 — an epoch-1 read admitted an epoch-0 grant (`left: Ok(… task_epoch: 0 …) right: Err(TaskAuthorizationInactive)`) |
| F-R5 | delete the `target.payload_sha256 != grant.payload_sha256` arm | same test | `FAILED` at context.rs:2061 (`right: Err(TargetRevisionChanged)`) — admission followed latest |
| F-R6 | `task_nominated_sql()`: `LEFT JOIN private.task_binding_grants` → `JOIN` | live `native_mcp_memory_bind_task_and_facet_selector_exact_sets_acceptance` | `N must carry every TASK/MANDATORY obligation of this task` — `left` 2 ids, `right` 3; the unauthorized obligation stopped existing instead of being reported rejected (ruling §六.3) |
| F-R8 | `apply_binding_write`, ADOPT branch: drop the `require_behavior_eligible_target` call (`let _ = &behavior_eligible;`) — the state the review found the tree in | live `native_mcp_memory_bind_task_and_facet_selector_exact_sets_acceptance` (DATA_ONLY control) | `left: 200 right: 403` at mcp_gateway.rs:8683 with the body `{"purpose":"ADOPT_TASK_INSTRUCTION", … "task_authorization":"granted"}` — a `ToolResult` origin stored at `PrivateKnowledge` was minted a live grant. Green-back: `2 passed; 0 failed … 6.93s` |
| F-R7 | `context_repo::check_claim`: compare `request.claim.successor_id` against the raw `task.0` instead of `binding_confirmation_successor(task, purpose)` (main-line fix 1 reverted) | both live bind witnesses | `{"code":"CONFLICT"}` on request id 11 in `…_exact_sets_acceptance` **and** `…_replacement_is_scoped_to_the_authorized_binding` |

Green-backs with the tree restored: `humaux-domain --lib` **222 passed / 0 failed**;
`humaux-gateway --test mcp_gateway native_mcp_memory_bind` **2 passed / 0 failed, 6.63 s**
(`HUMAUX_REQUIRE_DB=1`). F-R8 was added by the post-review fix (D-K) and its tails are in
`gates_card22c_fix_witness.log`; the rest of the chain is in `gates_card22c_fix_chain.log`.

F-R8 is the one arm where the mutation *is* the bug as it was found, not a synthetic one: the
row it left behind (`task_authorization: "granted"` on a `ToolResult` target) is what the tree
was writing before D-K. Note which assertion caught it — the HTTP refusal, then the
zero-rows-written readback. An assertion that only checked the *read* side would have stayed
green throughout, because the read side was already refusing that item; the durable grant is
invisible from there.

F-R1's tail is worth reading twice: with the direct refusal deleted the request for 6 was still
refused — by the ceiling table — but for `TrustedConnector`/`ToolResult` it came back as
`UntrustedInstruction` instead of `OriginAuthorityCeiling`. That is exactly the failure mode D-A
names: an indirect guard that looks like it is holding while the reason it gives has already
drifted. The half of the test that actually pins the guard is the second one (refusal *precedes*
the ceiling comparison), and the positive half (`ProjectConstraint` still storable) is what stops
the gate from passing by refusing everything.

F-R2 and F-R3 are each other's control: the first makes the negative control red while the
positive stays green, the second makes the positive red while the negative stays green. A single
mutation that reddened both would mean the two controls were testing one thing.

### DB layer (throwaway `fix22c_20260922100216`)

**F-DB1 — revoke-only is a mechanism.** With a legitimately seeded grant (tenant → reasoning
domain → memory + basis Evidence link → authorization Evidence → TASK/MANDATORY binding → grant):

```
---- red arm 1/3: UPDATE a non-revoked_at column (task_epoch)
ERROR:  task binding grants are revoke-only and otherwise immutable
---- red arm 2/3: extend expires_at            ERROR: … revoke-only and otherwise immutable
---- red arm 3/3: re-point to another memory   ERROR: … revoke-only and otherwise immutable
---- positive control: the ONE legal UPDATE (revoked_at NULL -> now())      UPDATE 1
---- single-shot: revoke an already-revoked grant   UPDATE 1 / ERROR: … immutable
---- clear a revocation                             UPDATE 1 / ERROR: … immutable
---- FAULT: drop the trigger, repeat red arm 1/3    DROP TRIGGER / UPDATE 1
---- RESTORED (rollback): red arm 1/3 refused again ERROR: … immutable
```

All six SQLSTATEs are 23514. With the trigger dropped, `UPDATE 1` — a live grant was re-pointed
to another task generation — so the refusal is the trigger, not the column ACL. The same four
arms now also run as Rust, inside an always-rolled-back transaction that seeds its own
authorization chain: `crates/adapters/tests/task_grant_contract.rs::`
`task_grant_revoke_only_is_attempted_not_read_off_the_catalog` (added by this run; the suite's
other four arms only read `pg_trigger`, which stays green against a trigger PostgreSQL would
never run). Writing it surfaced one real detail: the revoke must use `statement_timestamp()`,
not `now()` — `now()` is the *transaction* start and sorts before the grant's own
`clock_timestamp()` `issued_at`, so a same-transaction revoke trips
`CHECK (revoked_at >= issued_at)`. The production revoke (`context_repo.rs`) already uses
`statement_timestamp()`; the probe was wrong, not the schema.

**F-DB2 — I-NONINHERIT.** Copying a live grant onto a superseded memory's successor, keeping the
binding, is refused by the composite FK:

```
ERROR:  insert or update on table "task_binding_grants" violates foreign key constraint
        "task_binding_grants_binding_fk"
DETAIL:  Key (tenant_id, context_binding_id, scope_kind, task_id, memory_id, mode)=(…) is not
         present in table "context_bindings".
```

And after a real supersede (`status='superseded'` + `superseded_by`), the census is
`grants_on_successor = 0, grants_left_on_dead_row = 1`: the authorization stays on the version
that was approved, and the new version starts with none. The read side refuses the same thing a
second way — F-R5 above is the `payload_sha256` arm.

**F-DB3 — I-STORE, DB half.**

```
---- green: refuse a stored ExplicitTaskContext
ERROR:  new row for relation "memory_records" violates check constraint
        "memory_records_stored_authority_v2_check"
---- positive control: the same insert at a legal class is accepted    INSERT 0 1
---- FAULT: drop the CHECK, repeat                    ALTER TABLE / INSERT 0 1
```

**F-DB4 — the low-privilege boundary, and a finding.** `role_private_worker` (the background
distillation role §10.1 rule 2 names) keeps its ordinary read and cannot mint an authorization:
`ERROR: permission denied for table task_binding_grants` (42501). Opening the layers one at a
time shows there are three, not one:

| Fault | Result |
|---|---|
| `GRANT INSERT` only | `ERROR: new row violates row-level security policy for table "task_binding_grants"` |
| `GRANT INSERT` + a permissive INSERT policy | `ERROR: … violates foreign key constraint "task_binding_grants_binding_fk"` |
| both, aimed at a **real** binding | `INSERT 0 0` — under FORCE RLS the role reads zero rows from `private.context_bindings`, so it cannot even source the binding facts a grant must agree with |

The honest consequence for the assertion: `task_explicit_context_v2_negative_control_authorization_cannot_be_forged`
pins SQLSTATE 42501, and **RLS raises 42501 too**, so opening the ACL alone leaves that test
green. It goes red at the second fault (23503 ≠ 42501). The assertion therefore pins "a
background role is refused when it tries to mint", not "the ACL is what refuses" — stated here
rather than papered over. Restored: `ERROR: permission denied for table task_binding_grants`.

## Main-line fixes during the card

Three root fixes landed while this card was being finished; they are part of the change, not
incidental:

1. **The confirm claim's successor leg was minted and checked by two different rules.** The
   gateway minted `binding_confirmation_successor(task, purpose)` (D-D's intent digest) while
   `context_repo::check_claim` still compared the raw task id, so *every* confirmed bind answered
   `CONFLICT`. The digest is now one domain function,
   `humaux_domain::context::binding_confirmation_successor`, called by both sides — the mint in
   `bins/gateway/src/mcp_application.rs` and the check in `crates/adapters/src/context_repo.rs`.
   Fault F-R7 above is the revert of this fix.
2. **`seed_task_instruction_memory` (the blocking admin client) was called from inside the async
   body** at two sites in the live witness; wrapped in `tokio::task::block_in_place` like every
   other seed there, or the sync `postgres` driver starts a runtime inside the runtime.
3. **"bind without `purpose`" is a §52.1 protocol refusal, not a business one.** The contract
   makes `purpose` required on the bind branch, so request 82 is `400 / -32602` and the assertion
   uses `assert_protocol_invalid_input`. The business arm in `parse_binding_arguments` stays as
   defence in depth.

## Open debt

- **The v2 registered name does not reach the wire (review finding, 2026-09-22).**
  `selector_wire` (`crates/retrieval/src/handoff.rs:92-99`) is the only producer of the selector
  name in `Handoff.mandatory[].selector` (:183, :190), `needs_verification[].selector` (:114) and
  `unavailable_selectors` (:142), and it still emits `task_explicit_context_v1` while the
  admission object behind it is now `VerifiedCurrentTaskBinding`. `SelectorSpec::registered_name`
  (`crates/domain/src/context.rs:155`) is read by nothing but the two registry assertions and
  `task_grant_contract.rs:343`. The fix is one line — `selector_wire` returning
  `spec(id).registered_name` (and the retired record for the retired id) — but
  `crates/retrieval` is outside this card's allowed files, so it is reported, not edited.
  Until it lands, D-H and §25.4.B(1) are true of the registry and false of the wire; both now
  say so.
- **The negative control's `completeness` half was never implementable (review finding,
  2026-09-22).** Card 22c scope 6 and ruling §六 ask the negative control to report
  `completeness cannot_establish/mandatory_not_satisfied`. That reason does not exist:
  `CannotEstablishReason` (`crates/retrieval/src/completeness.rs:120-143`, labels at :165-177)
  carries `MandatoryContextOverflow`, `LaneFailed` and seven unrelated reasons, and an unmet
  obligation only increments `mandatory_missing` (`handoff.rs:55,213`) without changing the
  completeness class. So with a live rejected REFERENCE_ONLY obligation the assembled context can
  still report a non-`cannot_establish` completeness, and the live witness can only assert
  `reason != "lane_failed"` (`mcp_gateway.rs:8685-8688`, with the "deliberately NOT asserting
  completeness == complete" note). Adding the reason means classifying in `retrieval`, outside
  this card's allowed files. Whoever adds it owns ruling §六's last sentence too: 存在未履行
  Mandatory 时不能构造 ExecutableContext — DiagnosticContext and ExecutableContext are not
  distinguished in this tree today.
- The four legacy rows and `VALIDATE CONSTRAINT` (card 24).
- `coord.tasks.authorization_epoch` has no writer: there is no task lifecycle operation in this
  tree. Until one exists, "closing and reopening a task invalidates its grants" is a mechanism
  with no caller — the mechanism is tested, the lifecycle event is not, because it does not
  exist. Whoever adds the operation bumps the epoch in the same change.
- `issuer_kind = 'TENANT_POLICY'` is representable and admitted but has no producer: the only
  write path today is the authenticated-task-request one. A tenant-policy issuer needs a policy
  object this tree does not have.
- **Discovered, pre-existing: the PINNED lane has no deliverable class.** `fetch_pinned_in_txn`
  borrows its authority floor from `explicit_mandatory_bindings_v1` (`ProjectConstraint`), and
  `project_active_constraints_v1` claims every active `ProjectConstraint` row in the tenant
  unconditionally, so `PinnedLane::excluding_mandatory` empties the lane for every legally
  storable memory. The only class that ever escaped this was a stored `ExplicitTaskContext` —
  which §10.1 rule 2 already said no origin can produce, so the production write path could
  never create one. Two fixtures were manufacturing it with a raw owner UPDATE
  (`native_mcp_memory_pin_unpin_confirm_gate`, `g80_31_handoff`'s pinned-count judgment); I-STORE
  refuses that write and makes the defect visible. **This card does not fix it**: changing the
  PINNED floor is a policy decision with no ruling behind it, and §25.4.A's own rule is 不许猜.
  The affected assertions now state the measured truth and point here; `memory.pin`'s own
  guarantee (the row, active, scoped, idempotent, confirm-gated) is unchanged and still asserted.
- `authorize_task_item` verifies the *shape* of the authorization evidence (present, FK-intact)
  and the intent digest is recomputable from the grant row, but nothing re-derives the digest at
  read time — an auditor can, the selector does not. Doing it in the hot path would mean
  re-rendering the intent string on every assembly; it is listed here rather than claimed.
