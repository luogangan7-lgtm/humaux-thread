# ADR-0047 — The ignored tier runs somewhere

- Status: accepted (card 23, 2026-09-23; review-fix pass 2026-09-26 — D-G, the D-C addendum,
  lane runs 3 and 4, the executed fault injections, and the mount-point finding under "Numbers")
- Supersedes nothing. Closes ADR-0046's two open debts on the wire side (the v2 registered
  name, and the `mandatory_not_satisfied` completeness reason that card 22c's negative control
  was asked to assert before it existed).
- Spec: §22.4 (a fifth `CANNOT_ESTABLISH` trigger), §23.3 (the reason enum), §25.4.B(1)
  (registered name on the wire), §79.2 (skip is not pass), §80.1 (a gate with no fault
  injection is not a gate).

## Context

The deployment report counted ~108 `#[ignore]`d tests needing a serial run against an isolated
database. Today's walk finds 114. None of them ran anywhere. That is worse than missing
coverage: every acceptance claim built on those files reads as green, and stays green while the
code underneath them moves. Cards 1–21 kept adding to this tier — live-DB and live-Qdrant
witnesses are exactly the tests that carry an `#[ignore]` — so the rot compounds.

Two smaller debts land in the same change because they are the same failure in miniature: a
name and a number that were true in one place and false on the wire.

## Decision

### D-A — The disposition lives next to the test, not in a table

Every `#[ignore = "..."]` in `crates/` and `bins/` must carry its disposition inside the reason
string: `lane(a:<resource>)`, `lane(b)` or `lane(c)`, followed by the written reason.
`cargo xtask serial-lane` walks the tree and exits 1, naming file and line, for any ignore that
does not.

The obvious alternative — a `const REGISTRY: &[(file, test, class)]` inside
`xtask/src/serial_lane.rs` — was rejected. It is a second list of the same 114 tests, and the
only thing keeping two lists in step is whoever remembers to update both. The failure this card
exists to fix *is* a list nobody kept in step. With the disposition in the attribute, an ignored
test added tomorrow in a file nobody thought about reds the gate the first time it runs, in the
file where it was added.

Resources are a closed set (§78.2): `shared_db`, `request_guard`, `qdrant`, `disposable`,
`pre_0132`, `post_0132`, `mechanism_fixture`, `mechanism_admin_bin`, `provenance_mutation`. An
unknown resource name is a red, never a skip.

`pre_0132` is why `cargo xtask migrate` gained `--through <id>`.
`contribution_policy_lifecycle_0132.rs::pre_0132_unresolved_triples_hard_stop` re-runs 0132's
own SQL and requires its `55000` hard stop, which only exists against the *pre-0132* schema —
on head the objects are already there and the migration fails for an unrelated reason. Without
a partial-apply mode the only honest disposition for that test was "cannot be provisioned",
i.e. a permanent red. Ten lines in `migrate.rs` turn it into a test that runs. `--through` with
an id matching no file is a fail, never a silent full apply.

`lane(c)` is *retired*, not *deferred*: the free text is the recorded reason and the lane never
runs the test. Anything under twenty characters of reason reds
`retired_dispositions_carry_a_written_reason` — a retirement with no reason is the silent ignore
this card is about.

### D-B — A declared dependency that is missing fails, it does not skip

The lane exports `HUMAUX_REQUIRE_DB=1` for every group it runs, so `testkit::skip_or_fail`
turns a missing dependency into a failure instead of a printed SKIP (§79.2). A resource the lane
cannot provision is counted `NOT RUN` **by name, with the missing object**, and reds the lane.
There is no arm that runs a test without its resource and hopes.

The naming obligation is the whole rule, and lane run 1 found it half-kept: the mechanism
fixture arm provisioned its database and said nothing about `HUMAUX_ADMIN_PG_DSN`, which the
suite's own reader `expect`s — so four tests died inside a panic message instead of being
reported as a named missing object (§57.1). An arm must name **every** object its group needs,
not only the one it provisions. That arm now returns
`missing object: HUMAUX_ADMIN_PG_DSN (role_admin login)` when the login is not exported.

### D-C — Warm the binary, never widen the deadline

Three folded debts (cards 8, 13, 17) are the same host effect: macOS pays a Gatekeeper/XProtect
provenance assessment on a freshly linked executable's first exec — 0.4 s on a quiet box, past
two minutes during an assessment storm. Tests with their own short deadlines (the scanner
fixture's 2 s probe, `GATEWAY_PROCESS_START_TIMEOUT`'s 5 s) then go red for a reason that is not
in the code.

The lane exec's the pinned scanner (`<gitleaks> version`) and every freshly linked test binary
(`--list`) once, with `env_clear()`, **before** any timed step, and reports that warm-up clock
separately from the lane clock. The production timeouts are unchanged: widening a real deadline
to hide a host effect turns a measured budget into a decoration.

### D-D — `g80_31_handoff` stops pinning a database that does not exist

`crates/adapters/tests/g80_31_handoff.rs::gateway_dsn` required
`127.0.0.1:61719 / humaux_thread_request_guard_20260828` exactly. Nothing on this node (or any
standard one) provisions that, so all 11 tests in the file took the `skip_or_fail` branch and
printed SKIP — a whole file of false green, and card 19's recorded debt.

The fix reuses the rule that already landed one file over: `support/operation_receipt_fixture.rs`
was changed on 2026-09-03 to accept whatever `HUMAUX_TEST_PG_DSN` names, keeping the two legacy
machine-local targets. `gateway_dsn` now reads the same way. This is a *read of* that rule, not
a second one — the fixture and this file cannot disagree about what "the isolated database" is.

### D-E — The registered name reaches the wire (closes ADR-0046 open debt 1)

`retrieval::handoff::selector_wire` held a hand-written `SelectorId -> &str` table that still
emitted `task_explicit_context_v1` on all four handoff surfaces (`mandatory[].selector`,
`pinned[].selector`, `needs_verification[].selector`, `unavailable_selectors[0]`) while
ADR-0046 had retired that registration in favour of `task_explicit_context_v2`. §25.4.B(1)'s own
prohibition — "同名 selector 不得偷换准入对象" — was therefore violated *by the wire* while the
registry was correct.

`selector_wire` is now `spec(id).registered_name`: one read of
`humaux_domain::context::REGISTRY`, and the second table is deleted. That is the only reliable
form of the prohibition — with two tables of the same names, nothing but discipline keeps them
equal, which is the same shape as D-A one layer down.

`handoff.rs::retired_selector_v1_never_reaches_the_wire` pins it from both sides: no selector may
emit the retired literal, and every emitted name must be a registered one.
`contracts/mcp/context.output.schema.json` tightens the four selector positions from
`{"type":"string","minLength":1}` to the closed enum of the five registered names (§78.2: a
closed set must not be a free string).

### D-F — An unmet Mandatory obligation moves `completeness` (closes ADR-0046 open debt 2)

Card 22c's ruling §六 asked its negative control to assert
`completeness cannot_establish/mandatory_not_satisfied`. The reason did not exist, so the witness
could only assert `reason != "lane_failed"`. Meanwhile an unmet obligation incremented
`handoff.counts.mandatory_missing` and changed nothing else: the envelope could report a
`semantic_bounded` answer over a context it *knew* was short.

`CannotEstablishReason::MandatoryNotSatisfied` (`mandatory_not_satisfied`) now exists,
`classify()` takes `mandatory_missing` as a fifth input, and returns
`cannot_establish/mandatory_not_satisfied` whenever it is non-zero — after the ledger/census/lane
triggers and **before the planner leg**. The ordering is the decision: the planner judges the
predicate, and a clean predicate over a short Mandatory context is still an answer nobody can
establish.

`MandatoryContextOverflow` (§25.5) cannot express this. Overflow is "it does not fit"; this is
"it fit and came back short". §25.5 already refuses to silently truncate Mandatory and still
claim complete; this is the same refusal on the other side.

`CompletenessInputs` grew a `mandatory_missing` field rather than a new `classify()` caller, so
there is still exactly one `fn classify(` in `retrieval` (the sole-construction-point rule, §11.8/§22.5, pinned by `architecture-check`). The production
supplier is `bins/gateway/src/context.rs`, which passes `handoff.counts.mandatory_missing` —
the same number the handoff already reported, now with somewhere to go.

Also in this change: `CompletenessTotal`'s hand-written 44-cell `AtomicU64` array became
`[const { AtomicU64::new(0) }; Self::CELLS]`. The old expansion had to be grown by hand every
time `REASONS` gained a variant, and the only thing enforcing that was a comment saying so.
The inline `const` block sidesteps the `AtomicU64: !Copy` problem that forced the expansion and
does not trip `clippy::declare_interior_mutable_const`, which a `const ZERO` item would.

### D-G — The Qdrant tombstone oracle moves onto the anonymous seam

`public_runtime_qdrant.rs::supported_projection_revoke_fences_hydrate_and_tombstones_old_live`
drove the whole oracle through `public_repo::admit_release` as `role_public_worker`, then
`drain_outbox(tenant)` and `run_once(tenant)`. Migration
`0124_phase9_independence_attestation:233` REVOKEd that role's `SELECT` on
`staging.contribution_releases` on purpose — the anonymous role must never link a claim to a
release or a contributor — and
`public_runtime.rs::legacy_release_admission_is_fenced_from_protected_rows` pins the fence. The
test was therefore an oracle for a path the product had retired, which is why it was
`#[ignore]`d and why the Qdrant tombstone ended up with no witness at all.

It is rewritten, not retired. Admission and evaluation now run on the live seam:
`admit_assessed_release` → `run_anonymous_once` → a `control.anonymous_source_lineage`
read-back, then `evaluate_anonymous_claim` via `evaluate_anonymous_supported`. The revoke half
is `contribution_repo::revoke_release` followed by two `run_anonymous_once` calls carrying the
**real** `PublicProjectionAdapter` — the first applies `PUBLIC_ANONYMOUS_REVOKE_APPLY`, the
second the `PUBLIC_PROJECT` dispatch that revoke enqueues, and that second one is what writes
the tombstone. Only the *projection* step stays tenant-scoped (`seed_project_job` +
`run_once`): projection reads `public.eligible_objects`, which carries no contributor or
release link, so 0124 does not fence it and the file's own projection tests already use that
shape. The disposition moves from `lane(c)` to `lane(a:qdrant)` — it runs.

The shared half lives in `crates/adapters/tests/support/public_anonymous_seam.rs`:
`dsn_as_role`, `NoopProjector`, the offline assessment/coverage ports, `coverage_for_probe`,
`prepare_assessed_candidate`, `finalize_assessed_release`, `drain_anonymous_queue`,
`admit_assessed_release`, `grant_moderator`, `evaluate_anonymous_supported`, `seed_project_job`
and `job_status` — all *moved* out of `public_runtime.rs`, not copied. Copying them would have
been a second definition of the one seam this fence leaves open, which is D-A's rejected second
table one layer down.

### D-C addendum — the fake scanner stops manufacturing its own Gatekeeper cost

The folded card-8 / card-17 debt was a *fixture*, not a lane resource.
`crates/adapters/src/retrieval_query_source.rs`'s `FakeScanner::new` wrote a brand-new
executable into `temp_dir()` per instance (two per test) and then probed it under the
production 2 s `LocalSecretScannerConfig.timeout`, with a `Drop` that deleted the file so the
next run had to create — and have macOS re-assess — another one. That is the pre-`main` stall
measured below, aimed at a 2 s budget.

The fixture now keeps one file per marker beside the test binary under `target/`, rewrites it
only when its bytes change, publishes it with a rename so a concurrent process never execs a
half-written script, and pays whatever assessment it still owes with an unbounded warm `exec`
before the timed probe. There is no `Drop`, because deleting the file is exactly what forced
the re-assessment. The production timeout is untouched (§79.2), and
`mixed_scanner_attestations_are_rejected_before_reserve` keeps running in the **default**
`cargo test -p humaux-adapters --lib` suite: putting it behind an `#[ignore]` to hand it to the
lane would have taken a passing test out of every ordinary run to work around a fixture defect.

## Consequences

- One command runs the ignored tier, and one command reds when a test joins it without a
  disposition. Both halves are gates: `card23_extra_gates.env` carries
  `serial_lane_audit|cargo xtask serial-lane --audit-only` (seconds, fails fast on a missing
  disposition) **and** `serial_lane|cargo xtask serial-lane` (the full run), and
  `card24_extra_gates.env` carries the same pair so the delivery rehearsal quotes the tally
  rather than the audit. The subcommand name is therefore fixed. The audit alone is not the
  gate this card exists for: it proves every ignore has a disposition, not that any test ran.
- `lane(c)` retirements are visible in the lane's own output every run, with their reasons.
  They are not hidden, and they are not counted as passes.
- §23.3's `reason` enum is now eleven values plus `null`; §41.2's counter grid is 4 × 12.
- A `context.assemble` whose Mandatory lane comes back short now answers `cannot_establish`
  where it previously answered `semantic_bounded`. That is a visible behaviour change on a
  route that was previously wrong, not a regression.

## The dispositions as landed (2026-09-23 walk)

| Disposition | At the walk | After lane run 1 |
|---|---|---|
| `lane(a:disposable)` | 2 | 5 |
| `lane(a:mechanism_admin_bin)` | 1 | 1 |
| `lane(a:mechanism_fixture)` | 6 | 6 |
| `lane(a:post_0132)` | 4 | 4 |
| `lane(a:pre_0132)` | 1 | 1 |
| `lane(a:provenance_mutation)` | 1 | 1 |
| `lane(a:qdrant)` | 1 | 1 |
| `lane(a:request_guard)` | 13 | 13 |
| `lane(a:shared_db)` | 73 | 68 |
| `lane(b)` | 8 | 8 |
| `lane(c)` | 4 | 8 |
| **total** | **114** | **114** |

The second column is after the triage below: three `shared_db` tests whose oracles read a
*global* table moved to `disposable` (which now provisions a per-run database), and two moved to
`c`. The inventory is unchanged — no test was deleted, and none lost its disposition.

The six `lane(c)` retirements, with their recorded reasons:

- `crates/adapters/tests/operation_receipts.rs` ×3 — the relation-lock wait observation
  (`pg_stat_activity.wait_event = 'relation'`) was only ever exercised on the 61719 side
  container and does not reproduce on the standard `HUMAUX_TEST_PG_DSN` database. There is no
  target this lane can provision on which the observation is true, so the tests are retired
  rather than carried as a permanent `NOT RUN`.
- `crates/adapters/tests/public_runtime_qdrant.rs` ×1 — **withdrawn from the retired set.** It
  was retired at run 1 for pinning the tenant-scoped path; D-G then rewrote it onto the live
  anonymous seam, so it is `lane(a:qdrant)` and runs. It appears in this list only because the
  run-1 and run-2 tallies above were taken while it was still `lane(c)`.
- `crates/adapters/tests/public_provenance.rs` ×1 — added after lane run 1. Same 0124 fence,
  reached from the other side: the test admits an identity-bearing User root through
  `role_public_worker` `admit_release`, `public_trust.rs` pins that the same call must answer
  `Forbidden` (and passes), and the 0165 attempt to re-open it was reverted as a lineage leak.
  The typed-root / closure-depth / revocation oracle has to be rebuilt on
  `admit_assessed_release` + `run_anonymous_once`; the reason in the attribute carries that
  recipe.
- `crates/adapters/tests/public_provenance_revocation_eval.rs` ×1 — added after lane run 1. Its
  `admit()` helper takes the same fenced path, so the three-run counterfactual measurement is a
  rewrite onto the assessed/anonymous seam, not a fix. Retired with the recipe rather than
  carried as a permanent red — which is exactly the `lane(c)` contract in D-A.

## Lane run 1 -> fixes (2026-09-23)

The first full run is the measurement this card was missing:
`serial-lane: n=110 passed=75 failed=34 not_run=1 retired=4`. Triage of the 34: **R = 2,
P = 32, D = 0** — not one of them is the product violating the current spec. Eight root-cause
families, and what each got:

| # | n | Class | Family | Root cause | Fix |
|---|---|---|---|---|---|
| 1 | 18 | P | the `61719` fixture pin | D-D landed on `g80_31_handoff` only. `service_credentials.rs`, `request_guard.rs` and `quota_and_rate.rs` still required `port == 61719` **and** a fixed database name, so every one answered `IsolationSetupFailed` — a fail in 0.01 s under `HUMAUX_REQUIRE_DB=1` | D-D applied verbatim to all three: accept whatever `HUMAUX_TEST_PG_DSN` names, with `61719 / FIXTURE_DB` kept as a legacy alternative |
| 2 | 2 | **R** | the 0124 fence | `0124_phase9_independence_attestation:233` REVOKEs `SELECT ON staging.contribution_releases` from `role_public_worker` on purpose; both tests admit through that exact path, and `public_trust.rs` pins that the same call must answer `Forbidden` (and passes) | retired `lane(c)`, each reason carrying the rebuild recipe (the assessed / anonymous seam) |
| 3 | 4 | P | the 0134 fence, two seams mixed | `public.guard_trust_evaluation` raises `42501` for a claim whose root is `lineage_mode='ANONYMOUS_RELEASE'`; four `public_runtime` tests admit anonymously and then evaluate through the **legacy** writer, which 0134 keeps for legacy roots only | the file's own `evaluate_anonymous_supported` at all five call sites; the now-dead legacy helper deleted |
| 4 | 2 | P | global-queue residue | the oracles claim from, and count, the **global** `ops.public_anonymous_dispatches`; the shared database still holds a row a 2026-09-08 run left `PROCESSING` | moved to `lane(a:disposable)`, which now provisions a per-run database |
| 5 | 1 | P | global-profile residue | `control.bootstrap_contribution_deidentify_shadow` walks **every** `control.user_reasoning_profiles` row, so one legacy profile from 2026-09-01 is a `23514` | same |
| 6 | 2 | P | §6.1.1 / 0163 | the 0133 workspace fixture shares a memory into a workspace but never admits its owner to it; 0163's arm wants an ACTIVE `control.workspace_memberships` row, and an ACTIVE tenant membership is no longer enough | the fixture inserts the ACTIVE `MEMBER` row for the reasoning domain's `owner_user_id` |
| 7 | 4 + the 1 `NOT RUN` | P | a login nobody provisioned | `mechanism_observation.rs::admin_pool` `expect`s `HUMAUX_ADMIN_PG_DSN`, and the suite asserts `session_user = 'role_admin'` — a real LOGIN, which `?options=-c role=…` on the superuser DSN cannot fake. `role_admin` has LOGIN and **no password**: migration 0110 says so in words ("Authentication is provisioned externally"), so this is a node that was never finished, not drift | the `mechanism_fixture` arm now *connects* as role_admin and returns `missing object: HUMAUX_ADMIN_PG_DSN (role_admin login)` when it cannot — one named `NOT RUN` per §79.2 / §57.1 instead of five panics. It stays `NOT RUN` on this node: provisioning a role credential on the shared cluster is a human decision, and `live_env.sh` now carries the one-line recipe and the evidence. The old `NOT RUN` was `mechanism_admin_bin`: this workspace *does* build `humaux-admin`, so the lane locates — and if needed builds — `target/debug/humaux-admin` rather than waiting for a human to export a path |
| 8 | 1 | P | serving-projection residue | the registry oracle met `ServingProjectionChanged` from an earlier run's projections, on a resource whose ignore reason already promised "an isolated PostgreSQL fixture" | `qdrant` provisions a per-run database too |

Two lane-side changes fall out of the triage as a whole rather than out of any one family:

- **`Resource::RequestGuard` became a real database.** It was a synonym for `SharedDb`, so the
  "dedicated request-guard fixture" that thirteen ignore reasons promise existed nowhere — the
  fixtures were pinning a name for isolation nothing provided. It is now a per-run
  `humaux_thread_request_guard_<unix>`, created and migrated, with every role DSN repointed:
  the same `ensure_database` + `role_dsns_pointed_at` shape `ProvenanceMutation` already used.
  Family 1's fixtures can accept it precisely *because* D-D stopped pinning a name. Two halves
  of one fix: a promise in an ignore reason that the provisioner does not keep is the same
  false green as an `#[ignore]` with no disposition, one layer down.
- **A failing group prints its whole output.** The tail was twelve lines, which for a group
  that fails eight tests at once is libtest's summary with every panic message already scrolled
  past. Families 1 and 7 both had to be re-run by hand to be diagnosed at all. A gate whose log
  names failures it cannot explain has moved the work, not done it.

`D = 0` is worth stating plainly. The ignored tier was not hiding a product defect. It was
hiding thirty-two provisioning lies and two fences the tests were standing on the wrong side of
— which is the same finding as the card's, one level down: what nobody runs, nobody maintains,
and the fixtures rot before the code does.

## Two traps in the secret-grep gate

1. **A build directory outside `/target` reds the secret grep.** The card's exact command
   (`grep -cEi "sk-[a-zA-Z0-9_.-]{16,}|…"`) counted 14 on this tree, every one of them a path
   inside an untracked `target-clippy/` directory at the repo root: cargo dep-info files and
   `.fingerprint/*.json` for the `xtask` and `futures-task` crates, plus one `query-cache.bin`.
   A cargo artifact named `<crate>-<16-hex-hash>` ends in a run that the `sk-` alternative
   matches once the crate name ends in `sk`. None is a secret.

   The directory came from a driver line, `CARGO_TARGET_DIR=target-clippy cargo clippy …`,
   added so a clippy run would not fight the chain for the main target lock. `.gitignore` has
   `/target`, so a *sibling* of it is untracked and reaches `git ls-files --others`. The fix is
   the alternate target directory, not the ignore file: it is now `target/clippy`, already
   covered by `/target`, and the existing directory was moved there rather than deleted, so no
   build cache was lost. Adding `/target-clippy` to `.gitignore` would have been a second entry
   for the same rule — and `.gitignore` is outside this card's allowed files, so making a stop
   condition green by editing it would have been the exact failure this card is about, one
   layer up. After the move the card's exact grep prints **0** with `.gitignore` untouched.
2. **Editing a long-named ADR whose slug ends in `sk` reds the same gate by itself.** The
   first version of this change added a "closed by card 23" note to ADR-0046; the diff header
   `+++ b/docs/adr/0046-…` alone matches `sk-[a-zA-Z0-9_.-]{16,}`, because that ADR's slug
   begins `ta` + `sk-` and runs well past sixteen characters. The edit was withdrawn and the
   closure is recorded here and in §25.4.B(1) instead — the gate is not worth failing for a
   cross-reference. The same trap catches this ADR if it spells such a path out, so it does
   not. Worth knowing before the next card touches one of those paths.

## Fault injection (§80.1)

| Mutation | Expected |
|---|---|
| Strip `lane(...)` from any one ignore reason | `cargo xtask serial-lane` exit 1, naming that file:line; `every_ignored_test_has_a_disposition` red |
| Give a `lane(c)` an empty reason | `retired_dispositions_carry_a_written_reason` red |
| `lane(a:not_a_resource)` | `unknown disposition` red (closed set) |
| Put `"task_explicit_context_v1"` back in `selector_wire` | `retired_selector_v1_never_reaches_the_wire` red |
| Delete the `mandatory_missing > 0` branch in `classify()` | `unmet_mandatory_obligation_yields_cannot_establish_ahead_of_the_planner_leg` red on both halves; the live gateway witness red whenever the live lane is short |
| Make the branch fire on a full lane | the same tests' second half red (the `else` arm of the live witness, and the LaneFailed precedence assertion) |
| Point `public_runtime_qdrant`'s admission back at `admit_release` as `role_public_worker` | the 0124 fence answers `Forbidden` — the failure that retired the test in the first place |
| Drop the second post-revoke `run_anonymous_once` (the `PUBLIC_PROJECT` dispatch) | `projection_live` reads back `true` and `query_live` is non-empty: no tombstone |
| Restore `Drop for FakeScanner`, or give it a per-instance `temp_dir()` path again | every run creates an unassessed executable and probes it under the 2 s timeout — `mixed_scanner_attestations_are_rejected_before_reserve` reds with `DependencyUnavailable` under load, the card-8 / 13 / 17 flake |
| Remove the `serial_lane` line from `card23_extra_gates.env`, keeping only `serial_lane_audit` | the chain stays green while **no** ignored test runs. Not a unit test — a chain-shape check, and the reason the audit alone cannot be this card's gate |


### Lane run 2 → the mechanism group (2026-09-24, main line)

Run 2 after the fixes: `n=108 passed=101 failed=0 not_run=7 retired=6` — the seven NOT RUN were the
`mechanism_observation` group, named as `missing object: HUMAUX_ADMIN_PG_DSN (role_admin login)`.
Root cause: migration 0110 creates `role_admin` with LOGIN and **no password** (deployments set it from
the secrets manager, outside migrations — 0011's own rule), and host-side connections reach the
container through the Docker bridge, i.e. the `scram-sha-256` pg_hba rule. The dev operator step
(`ALTER ROLE role_admin PASSWORD …`, runbook) plus the exported DSN made the group runnable; the main
line ran it with the lane's exact environment (`gates_card23_mechanism.log`): 5 passed, 2 failed —
`real_public_review_records_before_after_and_quarantine_remains_visible` and
`admin_cli_reports_real_supported_then_quarantined_review` both admit their fixture release through
`role_public_worker` `public_repo::admit_release` (mechanism_observation.rs ~:413) and answer
`Forbidden` at the 0124 fence — the same class as the two `public_provenance` retirements. Both are
now `lane(c)` with written reasons; `lane(c)` = 8, `a:mechanism_fixture` = 5,
`a:mechanism_admin_bin` = 0 (the lane's own walk on this tree prints `lane(a:mechanism_fixture): 5`,
and `mechanism_observation.rs` carries five such attributes at :161, :218, :252, :304, :511).

### Lane run 3 — the first green full lane (2026-09-25)

`serial-lane: n=106 run-set, passed=106, failed=0, not_run=0, retired=8 (inventory=114), lane
clock 3646.6s` (`gates_card23_lane3.log:1803`, warm-up 12424.7 s reported separately and
excluded). This is the acceptance measurement the card asks for: **n = 106, all passed, nothing
left ignored-and-unrun, 8 retired with written reasons**, and it is the number card 24 quotes —
not run 2's seven `NOT RUN`, which the operator step below cleared.

| | run 1 (2026-09-23) | run 2 (2026-09-24) | run 3 (2026-09-25) |
|---|---|---|---|
| run set `n` | 110 | 108 | 106 |
| passed | 75 | 101 | 106 |
| failed | 34 | 0 | 0 |
| `NOT RUN` | 1 | 7 | 0 |
| retired `lane(c)` | 4 | 6 | 8 |
| inventory | 114 | 114 | 114 |

Run 3's dispositions, from the lane's own walk: `a:shared_db` 68, `a:request_guard` 13,
`a:disposable` 5, `a:mechanism_fixture` 5, `a:post_0132` 4, `a:pre_0132` 1, `a:provenance_mutation`
1, `a:qdrant` 1, `b` 8, `c` 8 — 114, none without a disposition.

### Lane run 4 — after the review-fix pass (2026-09-26)

`serial-lane: n=107 run-set, passed=107, failed=0, not_run=0, retired=7 (inventory=114), lane
clock 298.1s`, gate exit 0, warm-up 49.1 s with 29 of 29 binaries finished inside the budget
(`gates_card23_lane4.log`). The run set grew by one and the retired set shrank by one because
D-G moved the Qdrant tombstone oracle from `lane(c)` to `lane(a:qdrant)`, and
`public_runtime_qdrant::supported_projection_revoke_fences_hydrate_and_tombstones_old_live`
**passes** on the anonymous seam — the first time that oracle has run since it was written. The
eleven `public_runtime` tests pass on the moved helpers, and the five `mechanism_observation`
tests pass with the provisioned `role_admin` login.

| | run 1 | run 2 | run 3 | run 4 |
|---|---|---|---|---|
| run set `n` | 110 | 108 | 106 | **107** |
| passed | 75 | 101 | 106 | **107** |
| failed | 34 | 0 | 0 | **0** |
| `NOT RUN` | 1 | 7 | 0 | **0** |
| retired `lane(c)` | 4 | 6 | 8 | **7** |
| lane clock (s) | 665.9 | 27067.7 | 3646.6 | **298.1** |
| warm-up (s, excluded) | 8278.2 | 10214.1 | 12424.7 | **49.1** |
| target directory | `/Volumes/data` | `/Volumes/data` | `/Volumes/data` | **boot volume** |

Run 4's last two rows are the mount-point finding below, not a code change: nothing in this
pass made the lane twelve times faster, and nothing in it made the warm-up 250 times cheaper.

## Open debt (named, not hidden)

- **The 19 `--include-ignored` public-runtime failures are still red.** Card 23 folded in the
  Phase 9 public-runtime list found by card 14 (`public_runtime.rs` :362 `evaluate_claim`
  Forbidden ×4; `public_trust.rs` :67 ×5; `public_provenance_revocation_eval` 42501 on
  `public.claim_trust_evaluations`; `mechanism_observation` ×7; `public_provenance`'s
  mutation-DB test) plus the `public_runtime_qdrant` tombstone-oracle rewrite. This change gives
  every one of them a disposition, a provisioned resource and a lane that runs them — which is
  what makes them *visible* — but it does not fix them. `admission_include_ignored`
  (`card14_extra_gates.env:6`) stays red until a follow-up does. Lane run 1 closed most of that
  list as provisioning faults (see *Lane run 1 -> fixes*), but the part that is a real
  rewrite — the public tier standing on the wrong side of the 0124 fence: the two `lane(c)`
  retirements added after run 1, and the two added after run 2 — is **not** a lane-side fix and
  does not belong to card 23. `admission_include_ignored` stays red until
  the public-tier rewrite gets its own card, and this is the named debt that says so: five
  retired tests whose recorded reasons already carry the recipe (rebuild on
  `admit_assessed_release` + `run_anonymous_once`), waiting for a card that owns the seam
  (`public_provenance.rs:417`, `public_provenance_revocation_eval.rs:337`,
  `mechanism_observation.rs:499` and `:505`, plus the three `operation_receipts.rs` relation-lock
  observations, which are a different family). The card's own rule for that list is "each must
  become a live witness **or** be retired with a written reason — no silent ignores left", and
  each of these is the second. The one thing the rewrite must not do is re-open `admit_release`
  for `role_public_worker` (the 0165 attempt that returned `candidate_id` / `confirmation_id` /
  `disclosed_payload` to that role was reverted as a lineage leak).

  **Why those four are retired and the Qdrant oracle was not.** The Qdrant oracle's subject is
  the *projection* lifecycle — live write, revoke fence, tombstone — and it was only reaching it
  through the fenced admission path. Swap the admission and the oracle is intact, which is D-G.
  The other four are about **identity-bearing roots**: typed User roots, closure depth, their
  revocation, and a public review linked to the contributor who made it. The anonymous seam does
  not produce such a root — that is what 0124 fences, not an implementation detail — so there is
  nothing to swap. Rebuilding them means deciding what the oracle *is* once the root is
  anonymous, which is a design question for the card that owns the seam, and is why each carries
  the recipe rather than a mechanical rewrite.

  **The `public_runtime_qdrant` tombstone oracle is no longer on that list — it was rewritten,
  see D-G.** `admission_include_ignored` is a gate line in `card14_extra_gates.env:6`, not in
  `card23_extra_gates.env`; card 23 added no such key.
- **`role_admin` has no authentication on a fresh node.** Migration 0110 creates it with `LOGIN`
  and deliberately no password, delegating authentication to external provisioning. Where nothing
  has provisioned it, the `lane(a:mechanism_fixture)` group and the one
  `lane(a:mechanism_admin_bin)` test are a named `NOT RUN` and the lane is red until an
  operator sets it (`live_env.sh` carries the one-line recipe). Two of those six do not need the
  reader at all and are lost with the group — resource-level provisioning has no finer grain, and
  inventing a second mechanism resource to split them would be a closed-set entry that exists to
  work around an unfinished environment.

  **Closed on this node (2026-09-24), by an operator, out of band.** `ALTER ROLE role_admin
  PASSWORD …` was run against `humaux-thread-pg` as the human decision it is, `live_env.sh`
  exports `HUMAUX_ADMIN_PG_DSN`, and lane run 3 reports `not_run=0`. The debt stands for any
  *fresh* node: the recipe is in `live_env.sh` and `docs/ops/serial-lane.md`, and a node that
  has not run it still gets one named `NOT RUN` per §79.2, never a silent skip.

- **§22.5 still freezes the four-parameter `classify()` signature.**
  `docs/architecture/Baseline_2.9.md:5719-5727` is a normative code block
  `pub(crate) fn classify(planner_output, lane_status, census_result, ledger) -> CompletenessClass;`
  carrying the comment "第四个入参是本轮新增" at :5720, and :5745 states
  "`classify` 保留上述四参签名及 A1 优先分支".
  D-F makes it five (`crates/retrieval/src/completeness.rs:676-682`, the `mandatory_missing`
  input). The module docs that repeated the freeze were corrected (`completeness.rs:7-10`,
  `:101-103`, `envelope.rs:242`, `:246`), but §22.5 is **not** one of the Baseline sections this
  card is allowed to touch — the card names §22.4 / §23.3 / §25.4 only — so the frozen wording
  is still standing and is now false on the tree. The sole-construction-point rule it states
  (exactly one `fn classify(` in `retrieval`) still holds and `architecture-check` still pins
  it; only the arity is wrong. The next card that opens §22.5 must change the block to the five
  inputs and keep the A1-priority branch wording. Recorded here rather than edited, because a
  spec edit outside a card's scope is how a normative clause acquires an author nobody agreed
  to.

- **`DiagnosticContext` vs `ExecutableContext`** (card 22c ruling §六's last sentence) is still
  undelivered. D-F makes the *completeness class* honest about an unmet obligation; it does not
  make it impossible to construct an executable context while one is outstanding. That is a
  type split in `retrieval::compiler`, not a completeness reason.

## Numbers (this node, 2026-09-23 → 2026-09-26)

**The warm-up is not a precaution, it is the largest cost in the lane on this host.** First
measured attempt warmed every test binary of every package the lane touches, serially: one
`--list` on a freshly linked binary sat for **2m53s**, and with dozens of binaries that phase
alone would have run for hours. Two fixes, both in this change: warm only the binaries whose
targets the lane actually runs (it was paying the assessment for targets with no ignored test
in them), and spawn them all and then wait instead of one at a time. Even then, three
still-unassessed binaries were observed at **32m15s** wall clock in the parallel phase, and
`ps` reports `0:00.00` CPU for each: `sample 33342` shows the whole process as

```text
1583 Thread_5395595: Main Thread   DispatchQueue_<multiple>
  1583 _dyld_start  (in dyld) + 0  [0x101868bf0]
```

— blocked in `dyld` before a single instruction of `main`, which is the Gatekeeper assessment
itself and not anything the test does. `syspolicyd` appears to serialize system-wide, so
spawning the warm-ups together bounds the damage without removing it.

That is the measurement behind D-C, and it is worse than card 8's recorded 30 s / 2 min: a 2 s
scanner probe or a 5 s process-start budget cannot absorb a 32-minute pre-`main` stall, and the
answer is to pay it outside the timed step, never to widen the budget.

**The same assessment also stalls `rustc`, which no warm-up reaches.** Measured 2026-09-26 on
this node: `cargo test -p humaux-retrieval --lib` sat 16 minutes on one crate with 19 s of CPU.
`sample <rustc pid>` puts the whole compile inside

```text
rustc_metadata::creader::CStore::dlsym_proc_macros
  dyld4::APIs::dlopen
    dyld4::Loader::mapSegments
      dyld4::SyscallDelegate::fcntl   ← blocked here
```

— `dlopen` of a **proc-macro dylib**, blocked in the `fcntl` that registers its code signature.
Beside it, `syspolicyd` held 85–87 % CPU continuously while `rustc` held 0 % and its accumulated
CPU time did not move for minutes: the compiler is not slow, it is queued behind the assessor. So it is not only freshly linked *executables* that pay:
every freshly built proc-macro `.dylib` pays too, inside the compiler, before any test exists to
warm. That is the term behind "this node type-checks the whole workspace in 61m42s", and it is
why the lane's warm-up budget has to cover the `cargo test --no-run` builds rather than start
after them. The blocked file was named by `lsof` on the stalled compiler:
`target/debug/deps/libasync_trait-<hash>.dylib`, 3.7 MB — the cost is not proportional to size.

**Measured 2026-09-26: the host-transient family is a mount point.** This tree does not live on
the boot volume. `/` is `/dev/disk3s1s1` (sealed, read-only) and `$HOME` is `/dev/disk3s5`;
`/Volumes/data` is `/dev/disk7s1`, a secondary APFS volume mounted `nodev,nosuid,**noowners**`
and 91 % full. On a `noowners` volume the system does not treat a locally built file as a
trusted-ownership artifact, so every freshly linked executable **and every freshly built
proc-macro dylib** under `/Volumes/data/humaux-thread/target/` pays a full assessment on first
use.

The controlled comparison, same commit, same command, only the target directory moved:

| target directory | volume | result |
|---|---|---|
| `/Volumes/data/humaux-thread/target` | `disk7s1`, `noowners` | `cargo test -p humaux-retrieval --lib -- --list` had **not finished after 25 minutes**; 19 s of CPU in that time, `rustc` at 0 %, blocked in `dlopen` of `libasync_trait-<hash>.dylib`, `syspolicyd` at 73–87 % throughout |
| `$HOME/humaux-target-boot` (new, empty) | `disk3s5`, boot | the **whole dependency graph from scratch**, then the same list, in **under two minutes** |

Every later command in this change ran against the boot-volume target directory: the retrieval
lib suite in **1–2 s** (124 tests), `cargo test -p xtask serial_lane` cold in **26 s**,
`cargo xtask migrate` / `rls-check` / `architecture-check` in **1–5 s each**, the whole
`humaux-adapters` integration set compiled in **24 s**, and
`cargo clippy --workspace --all-targets -- -D warnings` in **21 s** against the 61m42s
`cargo check` recorded above. Against `target/` on `/Volumes/data`, a single crate had not
compiled in 25 minutes.

The single clearest number is the lane's own warm-up, which exists only to pay these
assessments, measured by the lane itself on both volumes:

| target directory | warm-up |
|---|---|
| `/Volumes/data/…/target` | 8278.2 s (run 1) · 10214.1 s (run 2) · 12424.7 s (run 3), 26–27 of 28–29 binaries finishing inside the 180 s budget |
| `$HOME/humaux-target-boot` | **49.1 s**, 29 of 29 binaries finishing inside the budget |

That reframes cards 8, 13, 17 and this one. "The 2 s scanner probe is flaky", "the 5 s gateway
start deadline is flaky", "the warm-up costs 8 000–12 000 s", "the workspace type-checks in
61m42s" are one environmental fact, not four code defects — which is consistent with every one
of them having been triaged as a host transient and none as a product bug (`D = 0` above).

The fixes in this change stand on their own merits and are not conditional on the mount: D-C's
warm-up and the D-C addendum's reused fake scanner are correct on any host, and the production
timeouts stay untouched either way (§79.2). But the operational answer is a target directory on
the boot volume — `CARGO_TARGET_DIR` under `$HOME`, or moving the checkout — and no figure in
this section should be read as a property of this repository. **Whoever runs the lane next
should set `CARGO_TARGET_DIR` to a boot-volume path before measuring anything.** Both halves of
the lane honour it: `serial_lane.rs:411` reads `CARGO_TARGET_DIR` and falls back to
`<root>/target`.

Inventory, from the lane's own walk:

- 114 `#[ignore]` attributes in `crates/` + `bins/`, 0 without a disposition.
- At the walk: 110 in the run set (`lane(a:*)` + `lane(b)`), 4 retired (`lane(c)`).
- After lane run 1's triage: 108 in the run set, 6 retired.
- After the mechanism group ran for real (run 2 → run 3): 106 in the run set, 8 retired. Run 3
  passed all 106 with `not_run=0`.

**Full-lane wall clock, n = 3 runs, warm-up excluded and stated separately** (the card's
measurement; the lane prints both lines itself):

| run | date | target dir | run set `n` | lane clock (s) | warm-up (s, excluded) | outcome |
|---|---|---|---|---|---|---|
| 1 | 2026-09-23 | `/Volumes/data` | 110 | 665.9 | 8278.2 | 75 passed / 34 failed / 1 NOT RUN |
| 2 | 2026-09-24 | `/Volumes/data` | 108 | 27067.7 | 10214.1 | 101 passed / 0 failed / 7 NOT RUN |
| 3 | 2026-09-25 | `/Volumes/data` | 106 | 3646.6 | 12424.7 | 106 passed / 0 failed / 0 NOT RUN |
| 4 | 2026-09-26 | boot volume | 107 | **298.1** | **49.1** | **107 passed / 0 failed / 0 NOT RUN** |

These are not repeats of one measurement and must not be averaged. Run 1 stopped 34 tests
early, so its 665.9 s is a *short* lane, not a fast one. Run 2's 27067.7 s (7h31m) is the only
honest figure for "every group runs to completion on a contended host": it shared the machine
with the main gate chain, and on an I/O-bound volume that is the dominant term. Run 3 is the
lane alone on the same tree: 3646.6 s (1h01m) for 106 tests, warm-up excluded. Run 4 is the
same lane, one more test, with `CARGO_TARGET_DIR` on the boot volume: **298.1 s for 107 tests,
after a 49.1 s warm-up**. Budget the gate at run 4 on a boot-volume target directory, and at
run 3 plus hours of warm-up if it is left on `/Volumes/data` — the warm-up there is the larger
half and is bounded, not completed (see `HUMAUX_SERIAL_LANE_WARM_SECS` below). The 12× lane
clock and 250× warm-up are the mount point, not this change.

Audit-half wall clock (`--audit-only`), n = 3 runs — this is the cheap pre-gate, not the lane:

| run | s |
|---|---|
| 1 | 789.95 |
| 2 | 1.12 |
| 3 | 0.86 |

Run 1 is not an audit measurement: `cargo xtask` is `cargo run -p xtask`, and the preceding
gate had built xtask in the `test` profile, so run 1 paid a full `dev`-profile rebuild of the
binary and its dependency graph (this node type-checks the whole workspace in 61m42s — it is
I/O-bound, ~3.5 MB/s and ~4 % CPU per `rustc`). Runs 2 and 3 are the audit: **~1 s** for a
114-attribute walk of `crates/` + `bins/`, which is what makes it cheap enough to be a gate.

Gate chain on this tree (each run by this card, tails in the delivery log):

| gate | result |
|---|---|
| `cargo check --workspace --all-targets` | 0 (61m42s) |
| `cargo test -p xtask` | 0 — 477 passed, 0 failed (790s) |
| `cargo xtask serial-lane --audit-only` | 0 ×3 |
| `cargo xtask migrate` | 0 — 0 applied, 141 already-applied, drift 0 |
| `cargo xtask rls-check` | 0 |
| `cargo xtask architecture-check` | 0 |
| `cargo test -p humaux-retrieval -p humaux-protocol -p humaux-domain --lib` | 0 — 222 + 42 + 124 passed |
| `cargo clippy --workspace --all-targets -- -D warnings` | 0 (after fixing one `collapsible_if` in this card's own new file) |
| `cargo fmt --all --check` | 0 |
| secret grep (exact card command) | 0 across `git diff` and every untracked file outside `target-clippy/`; the remaining matches are cargo build artifacts in that un-ignored directory (see above) |

Re-run after the review-fix pass (2026-09-26), `CARGO_TARGET_DIR` on the boot volume:

| gate | result |
|---|---|
| `cargo test -p xtask serial_lane` | 0 — 5 passed (1 s) |
| `cargo xtask serial-lane --audit-only` | 0 — 114 inventoried, every one dispositioned (1 s) |
| `cargo xtask migrate` | 0 — 0 applied, 141 already-applied, drift 0 (1 s) |
| `cargo xtask rls-check` | 0 (1 s) |
| `cargo xtask architecture-check` | 0 (5 s) |
| `cargo test -p humaux-retrieval --lib` | 0 — 124 passed (2 s) |
| `cargo test -p humaux-adapters --lib` | 0 — 137 passed, including `mixed_scanner_attestations_are_rejected_before_reserve` on the reused fake scanner (16 s) |
| `cargo test -p humaux-adapters --tests --no-run` | 0 — every integration target links, `public_runtime.rs` and `public_runtime_qdrant.rs` included (24 s) |
| `cargo test -p humaux-adapters --tests --no-fail-fast` | 0 — 458 passed, 0 failed, 101 ignored (140 s) |
| `cargo clippy --workspace --all-targets -- -D warnings` | 0 — no findings (21 s) |
| `cargo xtask serial-lane` (the full lane) | 0 — `n=107 passed=107 failed=0 not_run=0 retired=7 (inventory=114)`, lane clock 298.1 s, warm-up 49.1 s (349 s wall) |
| `HUMAUX_REQUIRE_DB=1 … mcp_gateway native_gateway_semantic_recall_real_qdrant_pg_and_ryw_acceptance -- --ignored --exact` | 0 — 1 passed, 79.32 s (live PG + real Qdrant) |
| `cargo test -p humaux-testkit --tests --no-fail-fast` | 0 — 20 passed (4 s) |
| `cargo fmt --all --check` | 0 |
| secret grep (exact card command) | **0, with `.gitignore` at `HEAD`** — the alternate clippy target directory is now `target/clippy`, inside the already-ignored `/target` |

Fault injection actually executed, each applied and then reverted with a SHA-256 check
(§80.1 requires the red, not a claim about it):

| mutation | test | result |
|---|---|---|
| delete the `mandatory_missing > 0` arm in `classify()` | `completeness::tests::unmet_mandatory_obligation_yields_cannot_establish_ahead_of_the_planner_leg` | **red** — `left: Exact`, `right: CannotEstablish { MandatoryNotSatisfied }`; 123 passed / 1 failed |
| hardcode `"task_explicit_context_v1"` in `selector_wire()` | `handoff::tests::retired_selector_v1_never_reaches_the_wire` | **red** — `TaskExplicitContextV1 still emits the retired v1 registration`; 123 passed / 1 failed |
| strip `lane(a:shared_db) ` from one ignore reason (`contribution_execution_repo_0131.rs:303`) | `serial_lane::tests::every_ignored_test_has_a_disposition` | **red**, naming `…:303` and the missing prefix; 4 passed / 1 failed. `cargo xtask serial-lane --audit-only` exits **1** on the same mutation |
