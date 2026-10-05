# `cargo xtask serial-lane` — the serial isolated lane for the ignored tests

Card 23. Runbook for the one command that runs the `#[ignore]`d tier.

## What it is

Roughly 114 tests in this tree are `#[ignore]`d because they need a serial run against an
isolated database (and, for some, an isolated Qdrant, a pinned scanner, or a dedicated fixture
database). Until this lane existed, nothing ran them: the tier was green because nobody looked.

The lane does four things, in order:

1. **Audit.** Walks `crates/` and `bins/` and requires every `#[ignore = "..."]` to carry its
   disposition in the reason string. No disposition ⇒ exit 1, naming file and line.
2. **Warm up.** Exec's the pinned scanner and every freshly linked test binary once with a
   scrubbed environment, so macOS pays its Gatekeeper/XProtect first-exec assessment *there*
   rather than inside a test's own 2 s or 5 s deadline. Reported separately from the lane clock.
3. **Provision.** Creates and migrates what the `lane(a:…)` dispositions name; a resource it
   cannot provision is reported by name and its tests are counted `NOT RUN`, never skipped.
4. **Run.** `--test-threads=1`, `--ignored --exact`, one libtest invocation per
   (package, target, resource) group, with a pass/fail line per test and a final `n=` tally.

## The disposition grammar

The disposition lives **next to the test**, in the ignore reason, not in a table inside the
xtask. A table would be a second list of the same tests, and nothing keeps two lists in step.

```rust
#[ignore = "lane(a:shared_db) requires isolated PostgreSQL 18 migrated through 0131"]
#[ignore = "lane(b) requires pinned real Gitleaks fixture"]
#[ignore = "lane(c) pins the retired tenant-scoped public runtime; 0124 fenced that path"]
```

| Tag | Meaning | Lane behaviour |
|-----|---------|----------------|
| `lane(a:<resource>)` | needs a dedicated resource | provisions it, then runs the test |
| `lane(b)` | timing-sensitive | runs it serially after the warm-up |
| `lane(c)` | retired: pins a path that no longer exists | **not run**; the free text is the recorded reason |

Resources are a closed set (`xtask/src/serial_lane.rs::Resource`): `shared_db`,
`request_guard`, `qdrant`, `disposable`, `pre_0132`, `post_0132`, `mechanism_fixture`,
`mechanism_admin_bin`, `provenance_mutation`. An unknown resource name is a red, not a skip.

`lane(c)` needs more than twenty characters of written reason — a retirement with no reason is
the silent ignore this gate exists to stop.

## What each resource actually provisions

A resource name is a promise about isolation, so each one has to *be* that isolation — after
lane run 1, `request_guard` was a synonym for `shared_db` and the "dedicated request-guard
fixture" its ignore reasons named did not exist anywhere.

| Resource | Provisioned | Environment the group runs under |
|---|---|---|
| `shared_db` | `cargo xtask migrate` against `HUMAUX_TEST_PG_DSN` | unchanged |
| `request_guard` | per-run `humaux_thread_request_guard_<unix>`, migrated | every role DSN repointed |
| `qdrant` | TCP probe of `HUMAUX_TEST_QDRANT_PORT` + per-run `humaux_thread_qdrant_<unix>`, migrated | every role DSN repointed |
| `disposable` | per-**target** `humaux_thread_disposable_<unix>`, migrated | every role DSN repointed |
| `pre_0132` | per-run `humaux_thread_pre0132_<unix>`, migrated `--through 0131` | + `HUMAUX_0132_GATE_MODE=pre0132` |
| `post_0132` | `cargo xtask migrate` against `HUMAUX_TEST_PG_DSN` | + `HUMAUX_0132_GATE_MODE=post0132` |
| `mechanism_fixture` | `humaux_thread_stable_observations`, migrated; **requires `HUMAUX_ADMIN_PG_DSN`** | + `HUMAUX_MECHANISM_FIXTURE`, role DSNs repointed |
| `mechanism_admin_bin` | the `mechanism_fixture` half + `target/debug/humaux-admin` (built if absent) | + `HUMAUX_MECHANISM_ADMIN_BIN` |
| `provenance_mutation` | per-run `humaux_thread_prov_fault_<unix>`, migrated | + `HUMAUX_PUBLIC_PROVENANCE_FAULT_DB=1`, role DSNs repointed |

"Per-run" means a new database every run, **never dropped** (see Process discipline): the lane
prints each one as it provisions it, and removing them is a human decision. `disposable` is also
where an oracle that reads a *global* queue or a global profile table belongs — those cannot be
isolated by a fixture, only by a database nobody else has written — and it is the one resource
provisioned once per **target** rather than once per group, because a global scan is not isolated
from the target that ran before it in its own group.

## Running it

```sh
source <delivery-cards>/live_env.sh     # DSNs, Qdrant port, pinned scanner — never echo these
cargo xtask serial-lane                 # audit + provision + warm up + run
cargo xtask serial-lane --audit-only    # the cheap half: disposition audit only, no DB, no cargo
```

`--audit-only` is a pre-flight, **not the gate**: it proves every ignore carries a disposition,
not that any test ran. Both halves are wired, in this order, in `card23_extra_gates.env` and
`card24_extra_gates.env`:

```
serial_lane_audit|cargo xtask serial-lane --audit-only
serial_lane|cargo xtask serial-lane
```

`gates_card.sh` runs those through `$CARD_EXTRA_GATES`, so each appears in the chain log as its
own `### GATE <name> EXIT <rc>` line and card 24 quotes the `serial-lane: n=…` tally from the
second. The subcommand name is fixed by those lines: do not rename it.

**Set `CARGO_TARGET_DIR` to a boot-volume path before you measure anything.** On this node the
checkout lives on `/Volumes/data` (`/dev/disk7s1`, APFS, mounted `noowners`), and a build
directory there makes macOS assess every freshly linked executable *and every freshly built
proc-macro dylib* on first use, inside `dyld` before `main`. Measured 2026-09-26, same commit,
same command, only the target directory moved: against `target/` on that volume
`cargo test -p humaux-retrieval --lib -- --list` had not finished after **25 minutes** (`rustc`
at 0 % CPU blocked in `dlopen`, `syspolicyd` at 73–87 %); against a fresh empty
`$HOME/humaux-target-boot` the whole dependency graph compiled from scratch in **under two
minutes**, and every later run of that suite took 1–2 s. The lane honours the variable
(`serial_lane.rs:411`), so:

```sh
export CARGO_TARGET_DIR="$HOME/humaux-target-boot"   # boot volume, not /Volumes/data
cargo xtask serial-lane
```

The warm-up below is still correct and still needed — it is right on any host — but on a
boot-volume target directory it has far less to pay for. See ADR-0047, "Numbers".

**Budget.** Measured on this node with the target directory on `/Volumes/data`
(ADR-0047, "Numbers"), full lane, n = 3 runs:

| run | target dir | run set `n` | lane clock | warm-up (excluded) |
|---|---|---|---|---|
| 1 (2026-09-23) | `/Volumes/data` | 110 | 665.9 s — stopped 34 tests early, a short lane not a fast one | 8278.2 s |
| 2 (2026-09-24) | `/Volumes/data` | 108 | 27067.7 s — sharing the host with the main chain | 10214.1 s |
| 3 (2026-09-25) | `/Volumes/data` | 106 | 3646.6 s — the lane alone | 12424.7 s |
| 4 (2026-09-26) | boot volume | 107 | **298.1 s** | **49.1 s** |

On a boot-volume target directory, budget run 4: about five minutes of lane plus a minute of
warm-up. Left on `/Volumes/data`, budget run 3's hour plus hours of warm-up. The warm-up is
bounded by `HUMAUX_SERIAL_LANE_WARM_SECS` (default 180 s **after the last child is spawned**),
so the lane always terminates; the wall clock above includes the `cargo test --no-run` builds
that precede it.

The lane sets `HUMAUX_REQUIRE_DB=1` for every group it runs. That is deliberate — under that
flag `testkit::skip_or_fail` turns a missing declared dependency into a failure instead of a
printed SKIP, which is the whole point of running these tests somewhere.

`live_env.sh` must export `HUMAUX_ADMIN_PG_DSN` (a real `role_admin` **login**) as well as the
four role DSNs: `mechanism_observation.rs::admin_pool` reads it and the suite asserts
`session_user = 'role_admin'`, so `?options=-c role=role_admin` on the superuser DSN is not a
substitute. The lane connects as that role while provisioning, so a variable pointing at a login
nobody provisioned is reported as `missing object: HUMAUX_ADMIN_PG_DSN (role_admin login): …`
(§79.2, §57.1) rather than as five panicking tests.

Migration 0110 creates `role_admin` with `LOGIN` and no password on purpose — "Authentication is
provisioned externally" — so a fresh node has to be finished by an operator before this group can
run. That is a credential change on a shared cluster, so the lane never makes it; `live_env.sh`
carries the recipe.

When any test in a group fails, the lane prints that group's **whole** libtest output, not a
tail. A group can fail eight tests at once, and a twelve-line tail is then libtest's summary
with every panic message already scrolled past — a log that names failures it cannot explain
sends the next reader back to re-run the group by hand.

## The operation-receipt witnesses run here (card 25, ADR-0050 D-J)

The three same-key concurrency witnesses in `crates/adapters/tests/operation_receipts.rs` were
changed from `lane(c)` (retired, not run) to `lane(a:request_guard)` on 2026-09-26:

- `receipt_insert_lock_past_token_deadline_rolls_back_business_bmo_audit_and_receipt`
- `receipt_insert_lock_past_reservation_deadline_rolls_back_while_token_is_valid`
- `concurrent_same_key_never_commits_two_business_or_bmo_rows`

`Resource::RequestGuard` provisions a per-run database with every role DSN repointed, and
`support/operation_receipt_fixture.rs` already accepts it (ADR-0047 D-D). The old reason said
the relation-lock wait observation "does not reproduce" on the standard database. That was
never tried on a quiet per-run database. The third test does not observe a relation lock at
all; it is the only same-key witness ADR-0032 relies on. The disposition lives in the ignore
reason, not in a second table. The lane's `request_guard` group reports each test by name.

## Known flake source: `g80_31_handoff` (card 25 review P2, recorded card 26)

`crates/adapters/tests/g80_31_handoff.rs` stays in the default chain (`adapters_tests`), not in
this lane. Its byte-identity precondition is **cluster-wide**: two assemblies of one snapshot are
compared only after asserting their `pg_current_snapshot()` tokens are equal. The `QUIET` mutex
in that file serialises fixtures **inside the one test binary only**. Another test binary, lane
group or agent committing on the shared `HUMAUX_TEST_PG_DSN` cluster between the two reads makes
the tokens differ, and the test fails on its precondition. It does not fail on a byte regression.

- Diagnosis: a failure whose message is the snapshot-token precondition, not a byte diff, is
  this flake. A byte diff with equal tokens is a real regression.
- Disposition: while it has not flaked in a chain, it stays where it is. If it flakes in a card's
  chain, move the affected tests to `lane(b)` with the reason "cluster-wide snapshot precondition;
  QUIET is binary-local". Do not widen the lock or retry the test.
- Status on card 26: it did not flake. The card-25 main-line chain passed with 11 tests in 3.22 s.

## `enumerate_first_page_scales_with_a_capped_manifest` (card 35, ADR-0062 D-K M-1)

`crates/adapters/tests/enumerate_scale.rs` carries `lane(b)`: it is timing-sensitive (n = 30 timed first and later
pages after 3 warm-ups, p95 bars per ruling E12) and seeds 50 000 memories, so it runs serially after the warm-up,
never beside the chain. It needs no lane resource: it creates, migrates and drops its own throwaway database
`humaux_thread_c35_enum_<pid>_<n>` (the shared database never sees the seed). Its bars are the E12 regression bars
read against the checked-in baseline `crates/adapters/tests/data/enumerate_scale_baseline.txt`; the 300 ms
first-page target is not met on this host and moves to card 35b. The card-35 chain also runs it by name (gate
`c35_enumerate_scale_live`). The S7 reason text lacked the `lane(` prefix and the audit was red on it ("1 ignored
test(s) with no disposition"); card 35 S8 gave it `lane(b)`.

## Process discipline

The lane never kills a process it did not spawn and never frees a port by force (a `lsof -ti
:PORT | xargs kill` once killed a user application that happened to hold 8080). It never stops
the shared `humaux-thread-pg` / `humaux-thread-qdrant` containers, and it never drops a
database — `ensure_database` creates and migrates, additively, and leaves removal to a human.

## Self-test

`cargo test -p xtask serial_lane` runs the gate's own tests (xtask is a binary crate — there is no `--lib` target):

- `every_ignored_test_has_a_disposition` — the audit, over the real tree.
- `retired_dispositions_carry_a_written_reason`
- `disposition_grammar_is_closed`
- `dsn_database_swap_keeps_the_authority`
- `prose_about_ignore_is_not_an_attribute` — three files in the tree *discuss* `#[ignore]` in
  doc comments; demanding a disposition for a sentence would make the audit unfixable.

Fault injection: strip the `lane(...)` prefix from any one ignore reason and the first test
goes red naming that file and line.

## `lane(a:mechanism_fixture)` needs a LOGIN `role_admin`

Migration 0110 creates `role_admin` with LOGIN but **no password** — deployments set it from the secrets
manager outside migrations (the same rule 0011 states for every role). On a dev instance that is one
operator step, after which `live_env.sh` can export `HUMAUX_ADMIN_PG_DSN`:

```sh
# ADR-0059 D-E: a client-side SCRAM verifier, value printed once (or read from HUMAUX_ROLE_PASSWORD_ADMIN);
# store it in $HOME/.config/humaux/dev_role_passwords.env, which live_env.sh sources.
humaux-maintenance roles rotate --roles-sql migrations/0011_roles_and_grants.sql --role role_admin --actor <you> --reason <why> --ticket <id> --step-up-auth <proof>
```

Without it the lane reports that group as `NOT RUN (missing object: HUMAUX_ADMIN_PG_DSN
(role_admin login))` — a named dependency (§79.2), never a silent skip. The first full run after
card 23's fixes (2026-09-24) ended exactly there: `n=108 passed=101 failed=0 not_run=7 retired=6`.
With the login provisioned the group runs 5/7, and the two that admit through the fenced
`role_public_worker` path are retired as `lane(c)` (see ADR-0047).

The run that followed, on the same tree, is the current expected shape:

```
serial-lane: n=106 run-set, passed=106, failed=0, not_run=0, retired=8 (inventory=114), lane clock 3646.6s
```

`not_run=0` is the line to check. A `NOT RUN` is a red lane, not a caveat.

That tally was taken while `public_runtime_qdrant.rs`'s tombstone oracle was still `lane(c)`.
It has since been rewritten onto the anonymous seam (ADR-0047 D-G) and is `lane(a:qdrant)`, so
the **current** expected shape, measured 2026-09-26 (`gates_card23_lane4.log`), is:

```
serial-lane: n=107 run-set, passed=107, failed=0, not_run=0, retired=7 (inventory=114), lane clock 298.1s
```

with `lane(a:qdrant): 2` and `lane(c): 7`.


## Reaping the databases a run provisions

Each run provisions its throwaway databases (`humaux_thread_{request_guard,qdrant,disposable,
pre0132,prov_fault}_<unix stamp>`) and, by default, never drops them — dropping is a decision
the gate does not own. `cargo xtask serial-lane --drop-provisioned` makes that decision for the
run that carries the flag: after the tally it drops exactly the databases this process
created (never one it found, never the fixed-name `humaux_thread_stable_observations`), one
reported line per drop, `WITH (FORCE)` so a leaked pooled connection cannot keep a throwaway
alive. The gate chain's `serial_lane` extra passes the flag (`card24_extra_gates.env`); an
operator who wants to inspect a run's databases afterwards omits it.
