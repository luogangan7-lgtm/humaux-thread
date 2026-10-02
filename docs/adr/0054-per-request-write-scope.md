# ADR-0054 — Per-request (tenant, workspace) write scope for the 14 governance / subject / affect ops; confirm tokens bound to the workspace; ADR-0031 D-B retired

- Status: Accepted (card 29 implemented 2026-09-29 on HEAD `403cb43`; design pass same day). Retires ADR-0031 D-B.
  Amends ADR-0018 D-A/D-B (the token binding gains `workspace_id`). ADR-0032 D-A (remember.put) is
  kept; its "process default pair" fallback is removed (D-D).
- Spec: §33.10 rule 9 (two-step confirm), §34.0.1 (receipt key, Q9), §6.1.1 / §6.3 (workspace
  membership, security_epoch), §6.2.1–§6.2.2 (grants), §52.1 (closed ErrorCode set), §15.1 (ticket
  issuance); ADR-0018/0019 (confirm gate), ADR-0031 D-A (per-request stream identity), ADR-0032,
  ADR-0033 (epoch bump on suspend/remove/set-role), ADR-0035 (live ∩ bound ∩ requested), ADR-0046 D-D
  (purpose rides the successor leg), ADR-0050 (executed single-statement migration checks), ADR-0053
  D-B (PROVISIONING write gate, 55000 → CONFLICT), ADR-0053 D-E (initialised ledger key).
- Closes: `docs/ops/system_audit_20260926.md` C5 ("governance ops only work for the bootstrap
  (tenant, workspace)"); delivery_point_report §6 gains §6.14 "Closed by card 29".
- Amended by ADR-0057 D-M (card 31, 2026-10-02): the request's (tenant, workspace) still
  authorizes every write, but a `MEMORY_LIFECYCLE` ticket no longer lands on the request stream. It
  goes to the target memory's **home stream** — the workspace stream of its PRIMARY Evidence's first
  ticket, the family that holds its point — through `memory_governance_repo::home_stream`; `correct`
  homes E2 and M2 with M1. A home other than the request stream must already be provisioned, else
  `DEPENDENCY_UNAVAILABLE`. Consistency tokens name the stream the ticket landed on. D-A's sentence
  "the ticket lands on the derived stream" is superseded for lifecycle tickets; it still holds for
  `remember.put` and for confirm-token binding.

## Context (read on `403cb43`)

- `bins/gateway/src/memory.rs` has two write-side scope rules. `write_scope` (:826-840, used by
  `register_subject` :572, `link_subject_key` :589, `annotate_affect` :629) narrows, then refuses any
  (tenant, workspace) other than `bootstrap.stream`'s with `DependencyUnavailable`. The seven
  confirm-gated handlers (`supersede` :336, `restore` :371, `correct` :423, `confirm` :468,
  `reject` :646, `archive` :679, `write_binding` :725 — 11 ops, archive/unarchive and
  pin/unpin/bind/unbind share handlers) repeat the same comparison inline against the `stream`
  argument, which `mcp_application.rs` fills from `self.context_bootstrap.stream.clone()` at :851,
  :947, :1032, :1121, :1206, :1335, :1477. `annotate_affect` issues its ticket on `&bootstrap.stream`
  (:633). The read routes already derive per request (`read_scope` :785 →
  `ContextBootstrap::provisioned_request_stream`, context.rs :135).
- The 11 confirm-gated input schemas (`contracts/mcp/memory.schema.json` oneOf 2–12) carry **no**
  `workspace_id`; `run_confirmed_write` is called with `requested_workspace = None`, so the guard
  routes to `credential.bound_workspace_id()` (`guard.rs::admit_with_workspace_policy` :983). The three
  non-destructive ops (oneOf 13–15) carry `workspace_id`.
- Authentication already re-derives, per request, the live ACTIVE workspace-membership ceiling and
  compares both security epochs (`auth.rs::authenticated_binding` :202-251, `derive_workspace_scope`
  :138): a suspended/removed member or an advanced epoch is refused on the **next** request. Nothing
  re-checks it **inside** the write transaction, so a suspend committing between authentication and
  the write's COMMIT does not stop that write.
- Every write transaction installs its RLS GUCs through `confirm_token_repo::set_authorization_local`
  (:80): `consume_in_txn` (:222, called by all 11 confirm-gated adapters), `subject_repo::register_subject`
  (:328) / `link_key` (:372), `affect_repo::annotate` (:364). It is the one chokepoint the 14 ops share.
- `control.confirm_tokens` (0148) binds (tenant, user, operation, target, successor) — no workspace.
  RLS is FORCE with a tenant-GUC policy; the owner-invoker trigger `confirm_token_single_use`
  refuses any UPDATE that changes a column other than `consumed_at` (`to_jsonb(NEW) - 'consumed_at'
  <> to_jsonb(OLD) - 'consumed_at'`). `role_gateway` holds table-level `SELECT, INSERT` and
  `UPDATE(consumed_at)`.
- `HUMAUX_GATEWAY_REMEMBER_TENANT_ID / WORKSPACE_ID` feed `RememberPolicy::stream`
  (`bootstrap.rs::parse_remember_policy` :393), which `RememberPolicy::new` refuses when tenant or
  scope is nil (`remember.rs` :191). Readers after this card would be: `ContextBootstrap::new`
  (copies the key; only its family fields are used by `request_stream`) and
  `mcp_application.rs` :148/:278 (remember.put's default workspace). Outside the gateway:
  `xtask/src/e2e_onboard.rs` :842-843 and `docs/ops/rehearse.sh` :217 pass them. Unknown
  `HUMAUX_GATEWAY_*` keys are a boot error (`bootstrap.rs` test :1009).
- Receipts: `control.operation_receipts` PK is `(tenant_id, principal_id, scope_kind, scope_id,
  operation, idempotency_key)` since 0158, pinned by the three-pair test. Only `remember.put` writes
  receipts; the confirm-gated ops write none (memory_governance_repo header), `restore`'s idempotency
  is `ops.memory_lifecycle_events UNIQUE(tenant, actor, key)` with key = hex(sha256(nonce)), and
  `nonce_sha256` is globally UNIQUE.

## Decisions

### D-A One write-scope derivation = the read routes' derivation; bootstrap comparison deleted

`memory.rs::write_scope` becomes

```rust
async fn write_scope(pool, authorization, requested_workspace, bootstrap)
    -> Result<(AuthorizationScope, StreamKey), ErrorCode>
{
    let (authorization, _scope, _family, stream, _serving) =
        read_scope(pool, authorization, requested_workspace, bootstrap).await?;
    Ok((authorization, stream))
}
```

i.e. exactly ADR-0031 D-A: `narrow(requested)` (`Forbidden` outside `live ∩ bound ∩ requested`) →
`ContextBootstrap::request_stream(principal tenant, workspace)` → admitted only if the ledger key is
initialised (`provisioned_request_stream`; uninitialised ⇒ `DEPENDENCY_UNAVAILABLE`, the same code
the four read routes answer for that pair). All 14 call sites use it; every
`stream.tenant_id != … || stream.scope_id != …` comparison and the bootstrap comparison are deleted.
The seven confirm-gated handlers take `bootstrap: ContextBootstrap` instead of `stream: StreamKey`
and derive `(authorization, stream)` from `write.request.authorization()` + `write.request.workspace_id()`
(the guard-routed workspace: requested, else the credential's binding). `annotate_affect` issues its
ticket on the derived stream; `write_binding` passes the narrowed workspace into `BindingWriteRequest`.
`register_subject` / `link_subject_key` discard the stream (tenant-level registry rows) but still pass
through the same gate, so an unprovisioned or foreign workspace is refused identically.

Why `provisioned_request_stream` and not the pure `request_stream` remember.put uses: ticket issuance
(`remember::issue_stream_log_row`) INSERTs the checkpoint row when absent, so a pure derivation would
let a governance write *initialise* a pair no one provisioned — after which reads of that pair stop
answering `DEPENDENCY_UNAVAILABLE`. remember.put keeps its pure derivation (ADR-0032: it is the
pair's first writer). The PROVISIONING gate (0185 trigger on `issued_highwater`, 55000 → `CONFLICT`)
is reached automatically because the ticket now lands on the per-request stream; the test asserts it.

Cost: one extra read-only round trip (`family_read_state`) per governance write, the one the reads
already pay. Record p50 before/after (D-I).

### D-B The in-transaction recheck: `control.assert_write_scope(api_key_id, workspace_id)`

A new owner SECURITY DEFINER function (migration 0188), `VOLATILE`, `search_path=pg_catalog`, EXECUTE
`role_gateway` only, returns `true` or raises:

1. `control.api_keys` row for `p_api_key_id` exists, `tenant_id = humaux.tenant_id` GUC,
   `revoked_at IS NULL`; `control.tenants` row `state='ACTIVE'` and `security_epoch =
   k.tenant_security_epoch` — else SQLSTATE `28000` (→ `UNAUTHORIZED`).
2. Machine credential (`k.user_id IS NULL`): `k.workspace_id = p_workspace_id` — else `42501`
   (→ `FORBIDDEN`).
3. User credential: `k.user_id = humaux.user_id` GUC; `control.users` `state='ACTIVE'` and
   `security_epoch = k.user_security_epoch`; `PERFORM 1 FROM control.memberships WHERE (tenant,user)
   AND state='ACTIVE' FOR SHARE` found — else `28000`. `k.workspace_id IS NULL OR = p_workspace_id`
   and `PERFORM 1 FROM control.workspace_memberships WHERE (tenant, p_workspace_id, user) AND
   state='ACTIVE' FOR SHARE` found — else `42501`.

The codes mirror what authentication answers for the same facts (`authenticated_binding` →
`Unauthorized`; `derive_workspace_scope` → `Forbidden`). `FOR SHARE` on the two membership rows
linearises the write with a concurrent suspend/remove/set-role (ADR-0033's `membership_repo::apply`
UPDATEs `control.memberships` in the same transaction as the epoch bump; `set_workspace_membership`
UPDATEs the workspace row): the suspend waits for the write to commit, or the write sees the
suspended row and aborts. Share locks do not conflict with each other, so concurrent writes of one
user do not serialise. The `api_keys` row is **not** locked (`mark_used` UPDATEs it on every request
and would serialise one credential's requests). Owner RLS: `api_keys`/`tenants` have the 0112 owner
SELECT policies; `memberships` (0012 `_tenant_isolation`, ALL) and `workspace_memberships` (0162
tenant ALL + restrictive self-read) are satisfied by the GUCs the caller already set.

Called from ONE adapter helper, `confirm_token_repo::set_write_authorization_local(txn, auth)`:
`sole_workspace(auth)?` (the scope must be narrowed to exactly one workspace, else `FORBIDDEN`) →
`set_authorization_local` → `SELECT control.assert_write_scope($principal, $workspace)`. Callers:
`consume_in_txn` and `mint_with_audit` (all 11 confirm-gated ops, both legs), the three
non-destructive writers swap their `set_authorization_local` call for it
(`subject_repo::register_subject`, `subject_repo::link_key`, `affect_repo::annotate`), and so do
`distill_repo::{confirm,reject}_candidate_atomically`, which then lock the candidate only inside the
scope (D-B′ below). Reads keep
`set_authorization_local` and pay nothing. One extra statement inside each write transaction.

### D-B′ Candidate writes are scoped too (review fix)

The recheck proves the principal belongs to the routed workspace, not that the *target* does. For
memories that half is `context_repo::readable_memory_ids` (the workspace's `can_read`). For distill
candidates it was missing: `private.distill_candidates` RLS is tenant-only (0152: visibility is
carried as data), and the confirm / reject locks filtered by `(tenant, candidate_id[, sha])` only, so
a member routed to W2 could reject a W1 WORKSPACE_SHARED (or another user's USER_PRIVATE) candidate
by id, and confirm it with id + sha — materialising Evidence and Memory with
`visibility_workspace_id = W1`. Both locks now add the same visibility disjunction
`memory.enumerate {candidates:true}` lists by (`distill_repo::candidate_in_scope`: TENANT_SHARED, or
WORKSPACE_SHARED in the scope's one workspace, or USER_PRIVATE of the scope's user); outside it the
row is absent ⇒ `NOT_FOUND` before the token is consumed (no oracle, token intact). Witness: W6b.

### D-C `confirm_tokens.workspace_id`: nullable column + FK now (EXPAND), CHECK later (CONTRACT), no backfill

Migration 0187 (the EXPAND half):

```sql
ALTER TABLE control.confirm_tokens
  ADD COLUMN workspace_id uuid,
  ADD CONSTRAINT confirm_tokens_tenant_workspace_fk FOREIGN KEY (tenant_id, workspace_id)
    REFERENCES control.workspaces (tenant_id, workspace_id) ON DELETE CASCADE;
```

- §46.1 EXPAND_CONTRACT: the old and the new gateway binary must both run against the expanded
  schema. The pre-card-29 binary INSERTs without `workspace_id` (NULL; the FK is `MATCH SIMPLE`, so
  unchecked) and consumes without matching it; the ADR-0054 binary always INSERTs the narrowed
  workspace and consumes only on equality. Deploy order: migrate 0187/0188, then roll the binary
  (the new binary cannot go first — it names a column and a function that do not exist yet).
- **No CHECK in 0187.** A `CHECK (workspace_id IS NOT NULL) NOT VALID` is still enforced on every
  INSERT, so shipping it with the column (the first implementation did) made every old-binary mint
  fail with `23514` → `CONFLICT` for all 11 confirm-gated first legs during the rollout window, and
  made the manifest's binary-first rollback impossible (verified on a throwaway DB as
  `role_gateway`). The binding's safety does not rest on the CHECK: a NULL row can never be consumed
  by an ADR-0054 gateway (`NULL = x` is never true). The CHECK is defence in depth and belongs to the
  CONTRACT migration — a later card, after the §46.1 observation window with only ADR-0054 binaries
  serving: `ADD CONSTRAINT confirm_tokens_workspace_bound CHECK (workspace_id IS NOT NULL) NOT VALID`,
  then `VALIDATE` once the 0169 sweep has removed every NULL row. Its rollback order is the reverse of
  0187's: DROP the CHECK first (the new binary keeps working), then roll the binary back.
- During the rollout window a token minted by an old binary (NULL) and presented to a new one answers
  `CONFLICT`, byte-identical to an expired token; a token minted by a new binary and presented to an
  old one is consumed without the workspace match (the old binary serves only its bootstrap pair).
  Both last ≤ the 300 s TTL.
- **No backfill, deliberately.** (1) A legacy row is never consumable after deploy: the consume
  predicate gains `AND workspace_id = $7`, and `NULL = x` is never true — an in-flight token minted
  before the deploy answers `CONFLICT`, byte-identical to an expired token, and the client re-mints
  (the TTL is 300 s in every env file). (2) A backfill cannot be derived generically: `target_id`
  is a candidate id for `memory.confirm/reject`, and a TENANT_SHARED / USER_PRIVATE target has no
  `visibility_workspace_id`; guessing a workspace would *grant* a token to a workspace the minting
  request may never have used. (3) It would have to defeat two guards: the single-use trigger
  refuses any UPDATE that changes `workspace_id`, and FORCE RLS makes an owner UPDATE without a
  tenant GUC match zero rows (a per-tenant loop with the trigger disabled — both of which this
  design avoids).
- The CONTRACT migration (CHECK NOT VALID, then `VALIDATE CONSTRAINT`) comes after the observation
  window and once the 0169 sweep (`control.sweep_confirm_tokens`) has removed every NULL row
  (unconsumed rows are deletable the moment they expire; consumed ones after the deployment's
  retention). Not this card.
- A time-conditioned CHECK ("only unconsumed and unexpired rows must be non-null") is rejected: a
  CHECK is evaluated only when the row is written, so `expires_at > now()` inside it is meaningless.
- Grants unchanged: table-level `INSERT` covers the new column; `UPDATE` stays `consumed_at`-only, and
  the single-use trigger already makes `workspace_id` immutable. rls-check MATRIX: no cell changes.
- The FK is composite so a token can only name a workspace **of its own tenant** (`MATCH SIMPLE`: the
  NULL legacy rows are not checked).

Mint and consume bind the workspace:

- `mint_with_audit` INSERTs `workspace_id = sole_workspace(auth)`. `guard.rs::mint_confirmation`
  first requires `admitted.request.workspace_id()` (`None` — an unbound PAT on a confirm-gated op —
  is refused before any row is written, `DEPENDENCY_UNAVAILABLE`, the same code its second leg
  already answered) and passes `authorization().narrow(workspace)?`.
- `CONSUME_SQL` = the existing full binding `AND workspace_id = $7`, `$7 = sole_workspace(auth)`.
  `ConfirmationClaim` is unchanged: the workspace is not client input, it is the narrowed scope the
  adapter already receives, so there is exactly one source for it.
- Replay in another workspace of the same tenant: admission succeeds (the caller is a member of W2),
  the consume matches zero rows ⇒ **`CONFLICT` (§52.1)** — the ADR-0018 D-B "one indistinguishable
  CONFLICT, never an existence oracle" rule; the token stays unconsumed and still works in W1.
  Replay by another tenant's credential: FORCE RLS hides the row ⇒ `CONFLICT`. A caller whose
  membership does not cover W2 never reaches the consume: `FORBIDDEN` at admission.
- ADR-0046 D-D is preserved: the successor leg (`binding_confirmation_successor(task, purpose)` for
  bind/unbind, `replacement_memory_id` for supersede) is untouched; the workspace is an additional
  equality column, not folded into the successor digest.
- `DestructiveOp::ALL` (11) and the token/digest shape in `crates/domain/src/confirm.rs` do not change.

### D-D Bootstrap: the default write pair is gone; the two env keys are accepted and ignored

- New `ProcessFamily { scope_kind, domain, projection_kind, projection_version }` (gateway
  `remember.rs`, validated non-empty, `scope_kind == "workspace"`). `RememberPolicy` and
  `ContextBootstrap` hold it instead of a full `StreamKey`; `RememberPolicy::workspace_id()`,
  `stream_key()` and the in-process `remember::put` convenience (no production caller) are deleted.
  `ContextBootstrap::request_stream` builds the key from `ProcessFamily` + (principal tenant,
  workspace) — still the single derivation point. Nothing in the process can name a tenant or a
  workspace any more, so reintroducing a bootstrap comparison has nothing to compare against.
- `remember.put` without `workspace_id` lands on the guard-routed workspace (the credential's
  binding) instead of the process default; the `request.workspace_id() != Some(workspace)` check
  becomes `request.workspace_id().ok_or(DependencyUnavailable)?`. For every bound credential
  (all e2e-seed / onboarding keys) the result is identical to today.
- `HUMAUX_GATEWAY_REMEMBER_TENANT_ID / WORKSPACE_ID` stay in the registry as optional
  (`entry_with_default(…, "")`): a non-empty value must parse as a UUID (fail closed on garbage), is
  otherwise unused, and the gateway prints one startup line `ignored since ADR-0054`. They stay
  accepted only because `xtask e2e-onboard` (outside this card's files) still passes them; the
  follow-up that removes them there also deletes the two registry entries, making their presence a
  boot error.
- Env files: `docs/ops/rehearse.sh` `start_gw` and the TW `rehearse_v2.sh` twin drop both keys (the
  rehearsal proves boot without them); `bins/gateway/tests/mcp_gateway.rs`'s binary env map drops them;
  `docs/architecture/env_vars.md` is regenerated (`dep-map --write`); `docs/ops/supervision.md` and
  runbook §4 say "not required; ignored; e2e tooling only".

### D-E Receipts (§34.0.1) unchanged; the key space cannot collide across workspaces

The PK already carries `scope_kind, scope_id` (0158). The only receipt writer is remember.put; the
lifecycle idempotency key is the token digest, globally UNIQUE and now per workspace by construction.
The extended test proves it for ONE principal (an unbound PAT of a user who is a member of W_A and
W_B): the same `idempotency_key` in W_A and W_B commits twice (distinct `evidence_id`), each replay
returns its own, and `count(*)` over `(tenant, principal, key)` is 2 with two distinct `scope_id`.

### D-F Tests (exact names)

- `bins/gateway/tests/mcp_gateway.rs::native_mcp_one_process_serves_three_stream_pairs_per_request`
  (existing `#[ignore]` lane, run with `--include-ignored`) gains a write phase against the SAME
  in-process gateway, built with **no default pair** (`application_with_budget` now constructs
  `ProcessFamily` only — every fixture test runs without it). A and B: same tenant, same user U, two
  workspace-bound PATs (`bind_credential_to_user`); C: other tenant, own user. Assertions:
  - W1–W4 A, B and C each run `pin`, `supersede` (second seeded memory as successor), `bind`
    (`REFERENCE_ONLY`, one `coord.tasks` row per tenant) and `annotate_affect` — all executed. Each
    MEMORY_LIFECYCLE / affect ticket lands on `(pair tenant, 'workspace', pair workspace)` in
    `projection.stream_log`, and the other two pairs' `stream_log` counts do not move across the call.
    The PINNED binding's `scope_id` is the pair workspace.
  - W5 cross-workspace replay: U mints `pin` on a USER_PRIVATE memory of U (readable from both W_A and
    W_B) with bearer A; presenting that token with bearer B ⇒ tool error `CONFLICT`, row still
    unconsumed with `workspace_id = W_A`; the same token with bearer A then executes.
  - W6 cross-tenant: bearer C presents A's token ⇒ `CONFLICT`; `annotate_affect` with bearer A naming
    W_C ⇒ 403 `FORBIDDEN`; naming W_B ⇒ 403 `FORBIDDEN` (bound credential).
  - W6b candidate scope (D-B′): a WORKSPACE_SHARED PENDING candidate of W_A; bearer B (same tenant +
    user, routed to W_B) rejects it by id and confirms it by id + sha ⇒ `NOT_FOUND`, the candidate
    stays PENDING and both B-bound tokens stay unconsumed; bearer A then rejects it (executed).
  - W7 unbound PAT of U: confirm-gated `pin` ⇒ `DEPENDENCY_UNAVAILABLE` at the mint with zero new
    `confirm_tokens` rows; receipts per D-E.
  - W8 pair D (membership, no checkpoint row): `annotate_affect` and a `pin` second leg ⇒
    `DEPENDENCY_UNAVAILABLE`, and D still has no checkpoint row afterwards.
  - W9 pair F onboarded through `control.onboard_workspace` (card 28 owner definer, called on the
    fixture owner connection) and not activated (PROVISIONING): `annotate_affect` ⇒ `CONFLICT`, zero
    `stream_log` rows for F.
  - W10 in-transaction membership recheck, auth bypassed: owner sets U's W_B workspace membership
    `SUSPENDED` (`control.set_workspace_membership`); `humaux_adapters::subject_repo::register_subject`
    and `confirm_token_repo::mint_with_audit` called directly with the B-narrowed scope ⇒ `Forbidden`;
    over HTTP bearer B's next write ⇒ 403; bearer A still writes (no over-refusal).
  - W11 epoch: owner `SELECT control.bump_user_security_epoch(U)`; a direct adapter write with the
    A-narrowed scope ⇒ `Unauthorized`; bearer A's next write ⇒ 401; bearer C still 200.
  - The existing receipt-PK pin and every read assertion stay.
- `bins/gateway/src/bootstrap.rs::tests::bootstrap_starts_without_the_default_write_pair` (new):
  `raw()` minus the two keys boots; a present-but-malformed value is refused.
- `crates/adapters/src/confirm_token_repo.rs::tests::consume_predicate_is_the_full_binding` (updated:
  pins `AND workspace_id = $7`) and `…::tests::sole_workspace_requires_a_singleton_scope` (new).
- Unchanged suites that must stay green: the supersede / restore / archive / unarchive / pin_unpin /
  bind (both) / confirm / subject_registry / membership_suspend / workspace_membership_scope /
  remember_put_per_call_visibility acceptance tests in `mcp_gateway.rs`; `humaux-domain --lib`.

### D-G Faults (each must go red)

| # | Fault | Red assertion |
|---|---|---|
| a | drop `AND workspace_id = $7` from `CONSUME_SQL` | W5 (bearer B executes A's token) + `consume_predicate_is_the_full_binding` |
| b | drop the `workspace_memberships` clause from `control.assert_write_scope` | W10 direct `register_subject` / mint succeed |
| c | reintroduce a bootstrap comparison in `write_scope` (compare the request to a configured pair) | W1–W4: B and C writes `DEPENDENCY_UNAVAILABLE` |
| d | drop the user-epoch clause from `control.assert_write_scope` | W11 direct write succeeds (the HTTP 401 half guards `authenticated_binding`) |
| e | `candidate_in_scope` returns `true` (the pre-review tenant-only candidate locks) | W6b: bearer B's reject / confirm of the W_A candidate are not `NOT_FOUND` |

Plus two design witnesses: replace `provisioned_request_stream` with `request_stream` in `write_scope`
⇒ W8 (D gains a checkpoint row); issue annotate on `bootstrap`'s family key ⇒ W1–W4 ticket placement.

### D-H Migrations (0187, 0188; forward; executed single-statement boolean checks, ADR-0050)

- `0187_confirm_tokens_workspace_binding` (EXPAND_CONTRACT, the expand half) — D-C DDL + column
  COMMENT. precheck: table present, no `workspace_id` attribute, `workspaces_tenant_workspace_key`
  UNIQUE present. postcheck: nullable `uuid` column; no CHECK constraint mentions `workspace_id` (the
  old binary's INSERT must pass); the composite FK to
  `control.workspaces`; gateway column INSERT yes / UPDATE no on `workspace_id`; gateway
  `UPDATE(consumed_at)` still yes; no DELETE for gateway; `confirm_token_single_use` trigger present.
- `0188_assert_write_scope` (EXPAND_CONTRACT) — D-B function, owner `role_migration_owner`, REVOKE
  PUBLIC, GRANT EXECUTE `role_gateway`. postcheck: one row with prosecdef, `provolatile='v'`,
  `search_path=pg_catalog`, gateway EXECUTE, no EXECUTE for the seven other runtime roles, no PUBLIC
  EXECUTE.
- `xtask/src/rls_check.rs`: `("control.assert_write_scope(uuid,uuid)", &["role_gateway"])` joins the
  exact-executor list checked by `check_r3_private_worker_function` (doc comment widened to "narrow
  `control.*` SECURITY DEFINER functions"). Table MATRIX unchanged.
- Baseline §6.2.2: the `control.confirm_tokens` prose row gains the 0187 workspace binding and the
  0188 in-transaction recheck door (EXECUTE `role_gateway`, reads `api_keys/tenants/users/memberships/
  workspace_memberships` as owner, no table grant changes).

### D-I Rehearsal (`docs/ops/rehearse.sh` + TW `rehearse_v2.sh`)

- `start_gw` loses the two keys; `assert_eq "gateway_boots_without_a_default_write_pair"` greps `$0`
  for `HUMAUX_GATEWAY_REMEMBER_(TENANT|WORKSPACE)_ID=` with a pattern that cannot match its own line
  (the `--visible-shadow[ $]` technique) ⇒ 0.
- The lifecycle + archive steps become one function `governance_leg <label> <bearer> <tenant> <ws>`
  run for `A` (`$BEARER`, `$TENANT`, `$WS`) and `B` (`$BEARER_B`, `$TENANT_B`, `$WS_B`): supersede →
  restore → archive → unarchive → pin → bind (`REFERENCE_ONLY`, one `coord.tasks` row per tenant
  inserted via `PGQ` — no product path creates tasks, ADR-0044) → annotate_affect (`workspace_id` =
  the leg's workspace). Per tenant: `gov_<L>_supersede_in_lifecycle_log`=1,
  `gov_<L>_restore_undoes_supersede`=1, `gov_<L>_archive_and_unarchive_events`=2,
  `gov_<L>_pin_binding_row`=1, `gov_<L>_bind_binding_row`=1, `gov_<L>_affect_rows`>0,
  `gov_<L>_tickets_on_own_stream`>0 and `gov_<L>_tickets_on_foreign_streams`=0; the existing recall
  witnesses stay for A. Across both: `governance_tenants_exercised`=2 (distinct seeded tenants with a
  SUPERSEDE event this run).
- Live replay witness: mint `pin` with `$BEARER` (WS), present with `$BEARER_A2` (WS_A2, same user) ⇒
  `cross_workspace_token_replay_is_conflict` = `CONFLICT`.

### D-J Speed

Record p50 (n=9, same box) of the `pin` second leg and `annotate_affect`, before (on `403cb43`, bootstrap
pair) vs after. Expected: +1 short read-only round trip (`family_read_state`) and +1 in-transaction
statement.

## Rejected

- **Backfill `confirm_tokens.workspace_id` from the target memory** (the card's first sketch): no
  generic source (candidate targets, TENANT_SHARED/USER_PRIVATE memories), would invent a workspace
  authorization, and must disable the single-use trigger and loop tenants under FORCE RLS. Legacy
  tokens are simply unconsumable; they expire in ≤300 s.
- **CHECK "unconsumed and unexpired ⇒ non-null"**: time-dependent CHECK, evaluated only at write time.
- **`workspace_id NOT NULL` via DELETE of legacy rows**: §6.2.1 bans DELETE for every non-owner role
  and the owner sweep deliberately keeps consumed rows for the §9 audit answer.
- **Fold the workspace into the successor digest**: overloads ADR-0046 D-D's leg, NULL successor ops
  would need a new encoding, and the binding stops being readable per column.
- **Carry the workspace in `ConfirmationClaim`**: a second source for a value the narrowed scope
  already carries; changes four construction sites and the fault-sentinel test for no gain.
- **Pure `request_stream` for the governance writes (remember.put's derivation)**: a governance write
  would initialise an unprovisioned pair through `issue_stream_log_row`'s INSERT.
- **Rely on the authentication-time checks only**: leaves the auth → COMMIT window open to a
  concurrent suspend/epoch bump; the card requires the check inside the write transaction.
- **Recheck with the tables role_gateway can already read (no definer)**: `SELECT … FOR SHARE` needs
  UPDATE privilege, which role_gateway must not hold on membership tables; without the lock the
  window only shrinks. The api_keys/users epochs are not readable by role_gateway outside
  `api_key_lookup(prefix)`, and the scope does not carry the prefix.
- **Lock `api_keys` / `tenants` FOR SHARE too**: `mark_used` updates `api_keys` on every request (it
  would serialise one credential's requests); `tenants` is one hot row per tenant (multixact churn).
- **Add `workspace_id` to the 11 confirm-gated schemas**: contracts are outside this card; the bound
  route already is the per-request workspace. An unbound PAT keeps refusing these ops (as today).
- **Delete the two env keys now**: `xtask e2e-onboard` still passes them and unknown keys are a boot
  error. **Keep using them as remember.put's default**: that is the process-bound pair being retired.
- **A separate registry of writable pairs**: ADR-0031's Q9 ruling (no registry table) applies.

## Known limits / upgrade signals

- Tenant-epoch bump and credential revocation are rechecked in-transaction but not locked: a tenant
  suspend committing in the few ms between the recheck and COMMIT is not linearised (ceiling noted in
  the function comment; upgrade: `FOR SHARE` on `tenants` if a tenant-suspend race is ever observed).
  Credential `status` / `expires_at` stay authentication-time only.
- A TENANT_SHARED / USER_PRIVATE target is readable from several workspaces; its lifecycle ticket
  lands on the caller's workspace stream. The worker resolves the ticket by memory (outbox →
  memory_evidence → memory_records, ADR-0049 D-A), not by stream, so the point is still retired; the
  ticket is simply accounted on that stream's ledger.
- `DEPENDENCY_UNAVAILABLE` from `write_scope` on a confirmed second leg is recorded by the guard as an
  UNKNOWN observation (pre-existing for the old bootstrap mismatch, ADR-0053 context). Unchanged here.
- Legacy NULL `confirm_tokens` rows remain until swept; the CONTRACT migration (NOT NULL CHECK, then
  `VALIDATE`) is a follow-up after the §46.1 observation window (D-C).
- Allowed-file note for the implement pass: D-B touches `crates/adapters/src/{subject_repo.rs,
  affect_repo.rs}` (one call swapped each) and D-D touches `bins/gateway/src/{remember.rs, context.rs}`
  — outside the card's list; the main line must approve. Without them D-B covers only the 11
  confirm-gated ops, and D-D cannot make the two keys optional (`RememberPolicy::new` refuses a nil
  tenant/workspace, `remember.rs` :191), which would leave the card's acceptance gate unmet.
- `memory.confirm` on a PROVISIONING workspace answers `INTERNAL` rather than `CONFLICT`:
  `distill_repo`'s error map lacks the ADR-0053 `55000` arm (outside this card's files). The gate
  itself holds (the transaction rolls back, nothing is written); only the code is wrong.

## Implementation notes (card 29 implement pass, kept in sync with the code)

- Implemented as designed (D-A…D-I). `memory.rs::write_scope` is `read_scope` minus the serving
  version; the seven confirm-gated handlers go through one `confirmed_write_scope(pool, write,
  bootstrap)` helper (the guard-routed workspace of the admitted request), so the rule exists once.
- `remember::put` (no production caller) and its private `map_remember_error` are deleted together
  (the mapper had no other caller and would be dead code).
- **`affect_repo::db_error` gains the `55000 → CONFLICT` arm** that `memory_governance_repo` and
  `operation_receipt` got in card 28: before this card `annotate_affect` could only ticket the
  bootstrap pair, so the ADR-0053 PROVISIONING gate was unreachable from it; now it is (W9), and
  without the arm the refusal surfaced as `INTERNAL`. `distill_repo` (memory.confirm) has the same
  gap and is outside this card's files — open, see Known limits.
- `bins/gateway/tests/mcp_gateway.rs::native_mcp_memory_pin_unpin_confirm_gate_acceptance` calls
  `context_repo::unpin_confirmed` directly; its scope/audit now carry the bearer's real credential
  row as principal (the fixture's `handle.auth` principal is not an `api_keys` row, and the
  in-transaction recheck resolves the credential — exactly what D-B is for).
- `crates/adapters/tests/confirm_token_retention.rs` (outside the card's list) seeded token rows
  without a workspace; its seed now creates one workspace per token (and its teardown deletes them),
  the shape an ADR-0054 gateway writes. (Found red by the first card-29 gate chain while 0187 still
  carried the CHECK; kept after the CHECK moved to the CONTRACT migration.)
- Review fixes (second pass): D-B′ touches `crates/adapters/src/distill_repo.rs` (outside the card's
  list; the finding was the root cause, not fixable from `memory.rs` without a second candidate
  read): both candidate locks and the enumerate listing share `candidate_in_scope`, and confirm /
  reject open with `set_write_authorization_local`. 0187 lost its CHECK (D-C) and its manifest's
  rollback text names the expand-only order. The dev database (`humaux_thread_dev`), which had the
  first 0187 applied, was reconciled to the edited file (CHECK dropped, ledger checksum updated) so
  `cargo xtask migrate` stays drift-free; no row changed.
- Convention-forced header edits outside the card's files (comments only, `dep-map --check` red
  otherwise): `Called-by` of `crates/adapters/src/postgres.rs` (− `gateway::remember`),
  `crates/domain/src/ids.rs` (+ `adapters::confirm_token_repo`, − `gateway::bootstrap`),
  `crates/projection/src/stream.rs` (− `gateway::bootstrap`); `bins/gateway/Cargo.toml` `# why`
  used-by lists of `humaux-projection` and `sqlx`.
- Fault injections (D-G), each run against real PG on the extended three-pair test: (a) consume
  predicate without the workspace → red at W5 (bearer B executed A's token) and
  `consume_predicate_is_the_full_binding` red; (b) definer without the workspace-membership clause
  (throwaway DB) → red at W10 (direct `register_subject` not `Forbidden`); (c) a first-pair-wins
  bootstrap comparison in `write_scope` → red at W1–W4 (pair B pin `DEPENDENCY_UNAVAILABLE`);
  (d) definer without the user-epoch clause (throwaway DB) → red at W11; witness 1 (`request_stream`
  instead of `provisioned_request_stream`) → red at W8 (pair D annotated, its ledger key created);
  witness 2 (annotate on one process-wide stream) → red at W1–W4 ("no ticket on another pair's
  stream").

## Measurements (D-J)

p50 of 9 uncontended calls of pair A inside the three-pair test, same host, fresh throwaway
databases (before: tree `403cb43` at migration 0186; after: this card at 0188), ms. The host is
noisy — the unchanged `memory.get` p50 of the same runs spans 35–74 ms — so every run is listed.

| run | pin second leg | annotate_affect | memory.get (control) |
|---|---|---|---|
| before 1 | 28.8 | 29.2 | 43.5 |
| before 2 | 29.5 | 29.9 | 38.2 |
| before 3 | 35.5 | 38.2 | 37.0 |
| before 4 | 62.7 | 65.2 | 74.4 |
| after 1 | 49.7 | 46.5 | 52.3 |
| after 2 | 30.3 | 33.0 | 37.6 |
| after 3 | 43.3 | 47.4 | 35.3 |

Median of the per-run p50s: pin 32.5 → 43.3 ms, annotate 34.1 → 46.5 ms (n = 4 / 3 runs × 9
calls). Expected cost: the `family_read_state` REPEATABLE READ round trip (BEGIN, two GUCs, one
read, COMMIT) plus one in-transaction statement that takes two `FOR SHARE` row locks. Upgrade
signal: if a governance write p50 above ~60 ms on a quiet host is ever attributed to it, fold the
provisioning check into the write transaction (one round trip instead of two).

## Rehearsal (D-I)

`docs/ops/rehearse.sh` (identical to the TW `rehearse_v2.sh` twin but for the work-dir line), run
2026-09-29 22:08–22:17 on live MiniMax + DashScope + real Qdrant, gateway started without the two
keys: `REHEARSAL VERDICT: 74 passed, 0 failed`; card-24 table `ASSERTIONS 58 passed, 0 failed`,
including `gateway_boots_without_a_default_write_pair: 0`, `gov_{A,B}_supersede_in_lifecycle_log: 1`,
`gov_{A,B}_restore_undoes_supersede: 1`, `gov_{A,B}_archive_and_unarchive_events: 2`,
`gov_{A,B}_pin_binding_row: 1`, `gov_{A,B}_bind_binding_row: 1`, `gov_{A,B}_affect_rows: 1 > 0`,
`gov_{A,B}_tickets_on_own_stream: 5 > 0`, `gov_{A,B}_tickets_on_foreign_streams: 0`,
`governance_tenants_exercised: 2`, `cross_workspace_token_replay_is_conflict: CONFLICT`. The run
also fixed a card-28 regression in the script itself: the serve-switch guard counted serving
checkpoints tenant-wide, which is 2 since card 28's seed activates both seeded workspaces; it is
now scoped to lane A's workspace. Evidence: `delivery-cards-20260903/card29_rehearsal_evidence/run2`.
Re-run after the review fixes (D-B′, 0187 expand-only) 2026-09-30 07:28–07:36: `REHEARSAL VERDICT: 74
passed, 0 failed`, the same governance assertions for both tenants (`run4`). Fault (e) on the
three-pair test: red at W6b (bearer B's reject of the W_A candidate answered `rejected`).

