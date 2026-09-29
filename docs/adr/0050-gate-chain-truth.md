# ADR-0050 — A green gate chain means the thing ran: declared deps, executed manifest checks, one migrator at a time, a pinned toolchain

- Status: **accepted** (card 25, 2026-09-26). The draft was written in the design pass; the
  implementation corrections are marked inline.
- Closes: audit `docs/ops/system_audit_20260926.md` §3 P1-12 (TH-1), §4 DM-5, DM-6 (+ the migrate
  half of OPS-9), ARCH-7, TH-2, TH-4. Plan: `docs/ops/delivery_plan_v2.md` §4 "Card 25".
- Spec: Baseline 2.9 §46 / §46.1 (migrations immutable; every migration declares
  `precheck`/`postcheck`; G46-1), §79.2 (「跳过不等于通过」, real Postgres only), §69 DoD,
  §70 (`Rust 1.98 / Edition 2024`, Baseline:13111). ADR-0047 D-D (fixture DSN rule) and its
  lane dispositions. ADR-0038 (a soak assertion with `n = 0` is `FAIL-VACUOUS`).

## Context (verified before this draft; do not re-derive)

1. **35 DB tests passed without a database.** `g80_31_handoff` (11), `provider_budget` (15),
   `mandatory_context_lane` (6), `retrieval_query_sources` (3) pin `127.0.0.1:61719 /
   humaux_thread_request_guard_20260828`; the chain runs on `54329 / humaux_thread_dev`. Card 24's
   final chain (`gates_card24_final3.log`) shows all four as `ok. N passed … finished in 0.00s`.
   `skip_or_fail` already panics when a dependency is **declared**, but the chain declares only
   `HUMAUX_REQUIRE_DASHSCOPE`, so nothing else is.
2. **The chain cannot go red as a whole.** `gates_card.sh` ends with an `echo`, so its exit status is
   0 whatever any gate returned; and it hard-exports `HUMAUX_TEST_PG_DSN` after sourcing
   `live_env.sh`, so a caller cannot point it anywhere else. The plan's first fault ("DSN at a
   closed port → the chain exits non-zero") is unobservable on the current script.
3. **Manifest checks never executed.** `xtask migrate` sends the `.sql` and, in a *second*
   statement outside that implicit transaction, inserts the checksum row. Nothing runs
   `precheck`/`postcheck`. A read-only probe of all 143 manifests (286 checks) against
   `humaux_thread_dev` at head on 2026-09-26: **285 valid, every one a single boolean column;
   1 invalid** — `0068_tenant_placements_qdrant_fields` postcheck,
   `operator does not exist: name[] = text[]` (`array_agg(a.attname …) = array['tenant_id', …]`).
   0174/0175 were already repaired on 2026-09-26.
4. **No migrate lock.** Two `xtask migrate` runs interleave; `CREATE TABLE IF NOT EXISTS` in the
   bootstrap can itself race.
5. **Toolchain.** Host: `rustup 1.29.0`, only `stable-aarch64-apple-darwin` = `rustc 1.97.1
   (8bab26f4f 2026-07-14)`. `https://static.rust-lang.org/dist/channel-rust-1.98.toml` → HTTP 200,
   `[pkg.rustc] version = "1.98.1 (48a229cea 2026-09-01)"`, `aarch64-apple-darwin available = true`
   for `rust` and `clippy`. `channel-rust-1.99.0.toml` → 404. No `rust-toolchain.toml`; no
   `rust-version` anywhere. Every rehearsal and soak so far ran `target/debug` binaries; there is
   no `release/` directory under `CARGO_TARGET_DIR` yet.
6. **Soak blind spots (TH-4).** `probes_green` counts only `--probe-cmd` exit codes; a probe such as
   `<worker> --readyz` is a fresh process and is green while the resident worker is dead. Per-op
   sample `ok=false` is reported but never scored. `max_rss_mib()` returns `0` when `ps` cannot
   run, and `0 <= max_rss` passes.
7. **TH-2.** `operation_receipts.rs` `receipt_insert_lock_past_token_deadline_…` (:714),
   `receipt_insert_lock_past_reservation_deadline_…` (:731) and
   `concurrent_same_key_never_commits_two_business_or_bmo_rows` (:748) carry the same `lane(c)`
   reason about a relation-lock wait observation. The third test does not observe a relation lock
   at all. It is the only same-key concurrency witness ADR-0032 relies on.

## Decisions

### D-A — The chain declares what it has, and exits with what its gates said

`gates_card.sh`, TW copy, outside the repo:

- After sourcing `live_env.sh`: `export HUMAUX_REQUIRE_DB=1 HUMAUX_REQUIRE_QDRANT=1
  HUMAUX_REQUIRE_MINIMAX=1` (DASHSCOPE is already exported). All four are added to the
  env-presence loop, so an empty one is `### GATE env EXIT 98`.
- `gate()` records any non-zero rc in a `FAIL` accumulator. `secret_scan hits > 0` sets it too. The
  last line is `exit $FAIL`. The chain's exit code is the conjunction of its gates, and the
  per-gate lines stay as they are.
- A caller's `HUMAUX_TEST_PG_DSN` (captured *before* `live_env.sh` is sourced) overrides the
  chain default. The env line logs it as `host:port/db` only, never with the password. This is
  what makes the closed-port fault injectable without editing the script.
- The chain logs its total wall clock (`GATES_DONE … total=<s>`), so "before/after" is read
  from the log rather than subtracted by hand.
- The script is zsh (`${(P)v}`, and a `echo | while` loop whose body must run in the current
  shell), but the card's acceptance gate invokes it as `bash gates_card.sh`. Under bash the
  extra-gates loop is a subshell, so every `CARD_EXTRA_GATES` failure was lost from `FAIL`, and
  `${(P)v}` was a "bad substitution" that skipped the env guard (review finding). Line 2 now
  re-execs under zsh when started by any other shell: `[ -n "${ZSH_VERSION-}" ] || exec zsh "$0"
  "$@"`. Fault test (fake `cargo` on PATH, `CARD_EXTRA_GATES` with one `false` gate, run with
  `bash`): before, rc 0, `fail=0`, one "bad substitution"; after, rc 1, `fail=1`, none. The
  same run with only green extra gates: rc 0.

### D-B — The four fixtures read ADR-0047 D-D; they do not restate it

The owner DSN (`HUMAUX_TEST_PG_DSN`) **defines** the fixture target. The owner check keeps only
`host == 127.0.0.1` and `!dsn.contains(['?', '#'])`. Every role DSN (gateway, maintenance,
retrieval) must name the **same port and database as the owner**, or the legacy
`61719 / humaux_thread_request_guard_20260828` pair. This is the predicate that
`request_guard.rs::same_target` and `g80_31_handoff.rs::gateway_dsn` already implement. Each file
gets the same few lines inline, with the existing "this is a read of that rule, not a second
one" comment, which follows the house pattern. Assertions do not change.

The loopback host is deliberately kept, although the card's shorthand says "any host". Two of these
fixtures run DDL as the owner (`g80_31_handoff` installs a tenant-scoped RESTRICTIVE policy, and
`mandatory_context_lane::FacetShadow` renames `private.memory_records.facet`). "Loopback only"
is the one guard that stops a mis-exported DSN from reaching a remote database.

**The consequence has to be named.** In the chain the target is `humaux_thread_dev`, which is
at head, so `facet` has `attgenerated='s'` (verified). `FacetShadow` therefore **will arm on the
dev database**. `Drop` restores it, but a SIGKILL or a tool timeout mid-test would leave dev with
a writable `facet` plus a `facet__probe_shadow` column, which is schema drift that the
checksum-only migrate cannot see. `setup()` therefore first checks for the `facet__probe_shadow`
residue and, if it is present, runs the same restore SQL as `Drop` and prints one line. This
handles the Drop that did not run. It changes no assertion.

*Implementation correction.* The draft put this repair in `FacetShadow::arm`. Fault-testing it
on a throwaway database with an injected residue showed that the repair was never reached. With
a residue present, `facet` reads as `attgenerated=''`, so the shadow test does not arm at all,
and the residue survived the run. The repair now runs in `setup()`, before any test reads the
catalog. Re-tested: the injected residue came back as `facet attgenerated='s'` with no shadow
column.

**What the first real run exposed.** These are fixture-side preconditions, not assertion
changes. They were invisible because these files had never run on a shared database.

- Both `g80_31_handoff` and `mandatory_context_lane` assumed their tests were alone on the
  target. `g80`'s snapshot token is the cluster-wide `pg_current_snapshot()`, so any sibling
  commit between the two assemblies breaks the "same snapshot" precondition. This reproduced
  3/3 in parallel and 0/2 with `--test-threads=1`. `FacetShadow` rewrites the catalog for
  every session, which made sibling probes flaky (1/3). `g80_31_handoff` holds a process-wide
  `QUIET` mutex for its fixture's lifetime (the `Fixture::_quiet` guard). On 61719 the
  dedicated container, which never actually ran these tests, provided that isolation.
- *Review correction (mandatory_context_lane).* A process-wide mutex does not cover the shared
  `humaux_thread_dev`: a second chain or agent running the same binary could see A's live shadow
  as "residue" and restore it mid-assertion. `mandatory_context_lane` therefore replaces `QUIET`
  with a **database** advisory lock, `FACET_SHADOW_ADVISORY_LOCK = 0x4858_4641_4345_5453`
  ("HXFACETS"). Every fixture takes it on its `admin` session right after connecting (under
  `lock_timeout = 120s`; the binary runs in ~1.5 s, so a timeout means a stuck holder and is a
  red 55P03 panic, not a hang) and holds it until that session closes, which also releases it
  when the process is killed. The shadow only exists inside a fixture, and the residue repair
  runs only after the lock is taken, so the repair can never see a live shadow in this process
  or any other. `FacetShadow::arm` sets `lock_timeout = 10s` before its ACCESS EXCLUSIVE DDL,
  so a busy `memory_records` on the dev database yields a red test instead of a queue that
  blocks every later session.
- `mandatory_context_lane::context_lanes_filter_visibility_lifecycle_and_binding_scope`
  failed deterministically, including serially. Since migration 0163 (ADR-0035), a
  `WORKSPACE_SHARED` row requires an ACTIVE `control.workspace_memberships` row. The fixture
  predates 0163 and seeded only the tenant membership, so its "allowed" workspace was not
  allowed. The fixture now seeds that one membership, as `auth_scope_rls.rs` already does. The
  assertion is unchanged.

### D-C — `cargo xtask gate-truth <chain.log>`

- **Parse.** A `Running tests/<stem>.rs (…)` line followed by its
  `test result: ok. N passed; … finished in X s` line is one integration binary result. Unit-test
  (`Running unittests`) and doc-test results are ignored. A result line that has no preceding
  `Running tests/` line is ignored, which covers the serial lane's own nested output.
- **DB-bound set, derived and never listed by hand.** An integration test target is every
  `{crates,bins}/*/tests/*.rs` and `xtask/tests/*.rs` file (autodiscovered targets;
  `tests/support/**` and `tests/<dir>/*.rs` modules are not targets). It is DB-bound when its source,
  or any file it pulls in with `#[path = "…"] mod`, contains one of `run_db_fixture`,
  `DbIntegrationFixture`, `ExternalDep::Postgres` or `ExternalDep::Qdrant`. DashScope/MiniMax-only
  files are live-egress smokes, not DB-bound. The same stem in two packages (e.g.
  `derived_dispatch_e2e`) is DB-bound if **either** is, which is the conservative choice. On
  today's tree this derives **55** stems, and replaying card 24's log flags exactly the four files
  in context §1 and no others.
- **Offender.** A DB-bound binary with `N > 0` passed and `X < 0.05` s. Output is one line per
  offender, then `gate-truth: fail (k offenders)` with exit 1.
- **Vacuous guards.** Zero parsed integration results, or an empty DB-bound set, is exit 1. A
  gate that reads the wrong file must not print `pass`.
- **Pass line.** `gate-truth: pass (<parsed> binaries, <db-bound seen> DB-bound, 0 offenders)`,
  followed by each DB-bound binary's `N` and duration. That list is where the ADR's fixture
  durations are read from.
- **Wiring.** This is the **last** gate in `gates_card.sh`, after `fmt` and the secret scan, and not
  in `CARD_EXTRA_GATES` (extras run before clippy). It reads the chain's own `$LOG`.
- **Two layers, two faults.** Under `HUMAUX_REQUIRE_DB=1` a re-pinned fixture panics and the
  `adapters_tests` gate goes red. gate-truth is the backstop for a run that forgot to declare. The
  "fixture re-pinned to 61719 → gate-truth red" fault is therefore run **without** the
  declaration (`cargo test -p humaux-adapters --test g80_31_handoff > f.log; cargo xtask
  gate-truth f.log` → exit 1), so it proves the backstop and not the panic.

### D-D — `xtask migrate` executes the checks of pending migrations, in the migration's transaction

For each migration **not yet recorded** in `ops.schema_migrations`, in one explicit transaction:

```
BEGIN
  precheck   -> must return exactly 1 row x 1 column of type bool, value true
  <file>.sql -> batch_execute (unchanged bytes; checksum unchanged)
  postcheck  -> same shape rule, value true
  INSERT ops.schema_migrations (migration_id, checksum)
COMMIT
```

- A false value, a non-boolean or multi-column/multi-row result, or a server error refuses the
  migration. The run stops at the first refusal, the transaction rolls back (drift 0: no object,
  no record), and the message names the check and carries the server's `DbError` message (or
  `returned false` / `returned <type>`).
- One log line per check: `migrate: precheck <id> ok` / `migrate: postcheck <id> ok`.
- A pending migration **without** a manifest is refused. `migration-rehearsal` already requires
  one, and migrate cannot execute a check it does not have. Manifests are parsed with the existing
  `migration_rehearsal::parse_manifest`, so there is no second parser.
- **Already-applied migrations are not re-checked.** A precheck asserts the *pre*-state, so it is
  false after apply by construction. An old postcheck may be legitimately invalidated by a later
  migration (for example, 0068's postcheck pins `cell_id` nullable and the PK shape). Re-running
  them would turn history into false reds. Drift on applied files stays the checksum's job.
- This also closes the other half of DM-6: apply and record are now one transaction. No file in
  `migrations/` contains `BEGIN`/`COMMIT`/`CONCURRENTLY`/`VACUUM` (grepped), so wrapping them in
  an explicit transaction changes nothing a simple-query batch did not already do.
- `--through` and serial-lane's `ensure_database_through` inherit all of this, because every
  caller routes through `apply_all`.

### D-E — `migration-rehearsal` gains a check-SQL pass over every manifest

After the static pass, when `HUMAUX_TEST_PG_DSN`/`DATABASE_URL` is set, rehearsal opens one
`BEGIN READ ONLY` transaction against it. For each of the 2×N checks it runs `EXPLAIN <check>`
(syntax plus semantic analysis, no execution) and `prepare(<check>)` (the result shape: exactly one
column of type `bool`), then `ROLLBACK`. Any error or shape mismatch is a `fail` naming
`<manifest> <field>: <server message>`. The output is one summary line,
`check-sql pass (286 checks, 0 invalid)`. With no DSN the result is `not_applicable (missing object:
HUMAUX_TEST_PG_DSN)`, as today. A DSN that is set but unreachable is a `fail`: a configured
database that cannot be reached is an environment fault, not an absence.

This pass validates against the **head** schema. It proves syntax and shape for all manifests. It
cannot prove that a precheck is true in its migration's pre-state, and that is what the throwaway
0001→head test in D-D proves.

**0068 manifest fix (manifest only):** `array_agg(a.attname::text order by a.attnum)`. The `.sql`
file and its checksum are untouched.

**The throwaway 0001→head run found six more manifest defects** that only show in the
pre-state or post-state. EXPLAIN at head cannot see them. All six fixes are manifest-only;
no `.sql` changed and every checksum is intact:

| manifest | check | defect | fix |
|---|---|---|---|
| 0041_audit_hardening | postcheck | `audit_seq … is_nullable = 'YES'`, but an IDENTITY column is always NOT NULL (never true) | `is_identity = 'YES' and is_nullable = 'NO'` |
| 0125_phase9_anonymous_lifecycle_revision | postcheck | `has_function_privilege(…, 'control.anonymous_source_lineage', …)` is called on a **table**, which is a runtime error | `has_table_privilege` |
| 0141_retrieval_embedding_rpc_calls | postcheck | `has_table_privilege(role_gateway, …, 'INSERT')`, but the grant is column-level `INSERT (cols)` (false) | `has_any_column_privilege` |
| 0143_private_inference_rpc_calls | postcheck | same column-level INSERT shape for role_consolidation_worker | `has_any_column_privilege` |
| 0150_memory_records_archive | precheck | `has_column_privilege(…, 'archived_at', …)` on a column that does not exist yet (a runtime error in the pre-state) | dropped; the `not exists(column)` conjunct already covers it |
| 0162_workspace_memberships | precheck | `conkey = array_agg(attnum order by attnum)`, but `workspaces_tenant_workspace_key` is `{2,1}` (false) | compare `conkey` sorted |

After these fixes, all 286 checks pass on 0001→head (`migrate_throwaway_…` test and
`check-sql pass (286 checks, 0 invalid)` at head).

### D-F — One migrator per database: advisory lock `0x4858_4D49_4752_4154` ("HXMIGRAT")

- `const MIGRATE_ADVISORY_LOCK: i64 = 0x4858_4D49_4752_4154;` is the ASCII `HXMIGRAT`. It is a fixed
  bigint with no meaning, in the same style as `testkit::DISCLOSURE_LEDGER_ADVISORY_LOCK`
  (`0x0074_4C45_4447_5231`). No other literal advisory key exists in the repo. The runtime keys
  are `hashtextextended(…)` values, so a collision is about 2⁻⁶⁴.
- `apply_all` runs `SET lock_timeout = '30s'` and then `SELECT pg_advisory_lock($KEY)` **before**
  the bootstrap DDL. It is a session-level lock because each migration commits separately, and it
  is released explicitly at the end, or on disconnect. `lock_timeout` stays set for the session, so
  each migration's DDL is bounded by the same 30 s. A DDL that queues behind a live worker lock is
  refused with drift 0 instead of stalling traffic behind an `ACCESS EXCLUSIVE` request. That is
  OPS-9's point, and runbook §1 says "stop the workers first".
- A second migrate waits up to 30 s. If the first one finishes in time, the second applies 0 and
  passes. Otherwise it fails with `55P03 lock_not_available` →
  `migrate: fail (another migrate holds HXMIGRAT; refused, 0 applied)`. Both outcomes leave drift 0.
- Advisory locks are **per database**, because the lock tag carries the database OID. The serial
  lane's per-run databases and the unit tests' throwaways never contend with `humaux_thread_dev`.

### D-G — Toolchain: pin 1.98.1, no re-baseline

1.98 is published for this host (context §5), so the Baseline stands and no re-baseline ADR is
written.

- `rust-toolchain.toml`: `[toolchain] channel = "1.98.1"`, `components = ["clippy",
  "rustfmt"]`, `profile = "minimal"`. The exact patch version is pinned so that "`rustc --version`
  matches rust-toolchain.toml" is a string comparison, not an interpretation.
- `Cargo.toml [workspace.package] rust-version = "1.98"`. Cargo applies it to a member only if the
  member declares `rust-version.workspace = true`, and member manifests are outside card 25's
  allowed files. Until they inherit it, the enforced pin is `rust-toolchain.toml`, and
  `rust-version` is the declared MSRV. See Open items.
- The implementer runs `rustup toolchain install 1.98.1 --profile minimal -c clippy -c rustfmt`
  **explicitly** and records the output, rather than relying on rustup's auto-install on the first
  `cargo` call. **Done (2026-09-26):** exit 0 in 53 s, output
  `1.98.1-aarch64-apple-darwin installed - rustc 1.98.1 (48a229cea 2026-09-01)`. As a side
  effect, rustup self-updated from 1.29.0 to 1.29.1. `rustc --version` in the repo now prints
  `rustc 1.98.1 (48a229cea 2026-09-01)`.
- The stale 1.97.1 debug artifacts were removed first (`cargo +1.97.1 clean --profile dev`:
  637 895 files, 90.9 GiB). This took the boot volume from 25 GiB free to 59 GiB free.
- 1.98's clippy adds `chunks_exact_to_as_chunks`. It fired once, in
  `crates/contracts/src/mechanism_registry.rs::parse_target_args`, which is outside card 25's
  allowed files. The fix was one mechanical line (`args.as_chunks::<2>().0`, same semantics,
  `humaux-contracts --lib` 16/16), made because the pin this card mandates caused it. It is
  flagged to the main line as an allowed-files extension.
- **Fallback path** (only if that install fails on this host, with the error recorded): drop the
  pin to `1.97.1`, rewrite this decision as "re-baseline §70 to Rust 1.97.1", and fix the Baseline
  line-3 header in the same card.

### D-H — `release_build` is a chain gate

`gate release_build cargo build --release --workspace` goes in `card25_extra_gates.env`. It uses the
same `CARGO_TARGET_DIR` (the release artifacts land in `…/release`, with no collision with
`debug`). Release is what ships, and every rehearsal so far graded debug binaries. Measured wall
clock is below.

### D-I — Soak probes see processes, score failure rate, and treat `ps` failure as red (TH-4)

- **Observed set.** New flag `--watch-pidfile <name>=<path>` (repeat, ≥ 1, required, §78.1 no
  defaults). The launcher already keeps pidfiles that chaos steps rewrite (`rehearse_v2.sh`:
  `$S/ds.pid`, `$S/cw.pid`, …), so the harness still never takes PIDs as arguments and never
  reads the workers' env. Each observation runs **one** `ps -axo pid=,rss=,comm=` and looks up
  each pidfile's current pid. It is present only if the pid is listed and `comm` contains `humaux-`
  (a reused pid is absent).
- **Expected vs unexpected absence.** Each chaos step stamps its start. An absence inside
  `[chaos_start, chaos_start + --chaos-grace-secs]` counts as `expected_absent` and is reported.
  Any other absence is a probe failure. `--chaos-grace-secs` is required whenever
  `--chaos-every-secs` is given, paired the same way as `--chaos-cmd`.
- **`ps` failure.** A spawn error, a non-zero exit or an empty table is `ps_ok = false` for that
  observation. The new assertion `ps_observed` is `at_most(value = failed observations, n =
  observations, threshold 0)`. RSS becomes `Option<u64>` and is never `0` by default.
  `rss_bounded` scores only real readings, and `n = 0` is `FAIL-VACUOUS` (ADR-0038).
- **Failure rate.** The new assertion `op_failure_rate` has value = max over ops of `failed/n` and
  threshold `--max-op-failure-rate <0..1>` (required). The `detail` field carries every op's
  `{op, n, failed, rate}`, so a red names its op. Chaos-window failures are counted. Excusing
  them is the plan's later "0 errors outside chaos windows" (card 54), not this card.
- The logic lives in pure functions (`parse_ps_table`, `classify_presence`, `op_failure_rate`),
  and each unit test targets one of them.

### D-J — TH-2: the three receipt witnesses run in the serial lane

The three ignore reasons become `lane(a:request_guard) …`. `Resource::RequestGuard` provisions a
per-run database with every role DSN repointed, which `support/operation_receipt_fixture.rs`
already accepts. Disposition lives in the ignore reason, as ADR-0047 requires, and not in a
second table.

- **Outcome (lane run 2026-09-26, `n=111 run-set, passed=111, failed=0`):** all three ran on
  the provisioned request-guard database and passed. The relation-lock observation *does*
  reproduce on a quiet per-run database, so nothing is retired.
- `concurrent_same_key_never_commits_two_business_or_bmo_rows` (:748) **must** run and pass.
  Nothing in it observes a relation lock, and the copied `lane(c)` reason was wrong.
- :714 and :731 observe `pg_locks … locktype='relation' AND NOT granted` joined on
  `application_name`. If they fail in the lane, the implementer diagnoses the cause, looking first
  at whether the actor's `?application_name=` survives `RuntimeDbPool::connect`, before calling it
  "does not reproduce". Only a reproduced, explained non-reproduction may go back to `lane(c)`,
  with that explanation as the reason, and this ADR then says so.

## Measurements (n = 1 each)

| What | Before | After |
|---|---|---|
| Chain wall clock | 19 m 36 s (`gates_card24_final3.log`, 09:51:25 → 10:11:01, debug, warm) | 18 m 54 s (`gates_card25_run1.log`, `GATES_DONE … total=1134s fail=0`, 1.98.1, debug mostly warm from the implementation builds; it includes the new `release_build`, `migrate_throwaway` and `gate_truth` gates) |
| `g80_31_handoff` | 11 passed, 0.00 s (skipped) | 11 passed, 3.38 s |
| `provider_budget` | 15 passed, 0.00 s (skipped) | 15 passed, 1.70 s |
| `mandatory_context_lane` | 6 passed, 0.00 s (skipped) | 6 passed, 1.47 s |
| `retrieval_query_sources` | 3 passed, 0.00 s (skipped) | 3 passed, 3.49 s |
| `cargo build --release --workspace`, cold | no release dir exists | 98 s (after `cargo clean --profile release`, 1.98.1, same `CARGO_TARGET_DIR`) |
| `cargo build --release --workspace`, warm | — | 0 s (no-op); 45 s as a chain gate after the xtask edits |
| Throwaway 0001→head with all checks | — | 143 migrations, 286 checks, 1.2 s |
| `rustc --version` | 1.97.1 (8bab26f4f 2026-07-14) | `rustc 1.98.1 (48a229cea 2026-09-01)` |

The smallest duration of a genuinely DB-backed binary in card 24's log is `facet_contract`,
0.06 s, which is 10 ms above the 0.05 s line. In the card-25 chain it was 0.08 s. See
Consequences. gate-truth on the card-25 chain printed `pass (88 binaries, 55 DB-bound, 0
offenders)`, and replaying card 24's log flags exactly the four fixtures.

Fault runs, each observed red:
1. The chain with `HUMAUX_TEST_PG_DSN` at closed port 127.0.0.1:59999 → `CHAIN_EXIT` non-zero
   (`gates_card25_f1.log`).
2. `g80_31_handoff` re-pinned to 61719 and run without `HUMAUX_REQUIRE_DB` gave `11 passed … 0.01s`.
   `gate-truth` then printed `offender g80_31_handoff`, `fail (1 offenders)` and exited 1. The
   re-pin was reverted.
3. A false postcheck, invalid-SQL precheck, `select 1` check and missing manifest are each
   refused with drift 0 (`migrate_refuses_*` tests).
4. Two concurrent `xtask migrate --dsn <fresh throwaway>` runs: one applied 143, the other waited
   on HXMIGRAT and then applied 0 with 143 already applied; the ledger held 143 rows, 143 distinct.
   The 1 s-timeout test variant refuses with `55P03` and 0 applied.
5. `rustc --version` equals the pin.


## Amendment (card 27, 2026-09-29) — per-test floor

The fixed 0.05 s floor flagged `facet_contract` (4 genuine DB tests, 0.04 s on a warm run) in
card 27's verify pass — the flake this ADR's Consequences predicted. `gate-truth` now uses the
SMALLER of the fixed floor and 3 ms × passed tests (`PER_TEST_FLOOR_SECS`): a skipped test costs
microseconds, a real one at least one loopback PostgreSQL round trip, so a binary whose passed
tests average under 3 ms each cannot have reached its database, while the fixed floor still
bounds large binaries. Replaying `gates_card27_rerun.log` now passes with 0 offenders; a 4-test
binary at 0.00 s and a 40-test binary at 0.04 s are still offenders (unit test
`gate_truth_per_test_floor_keeps_a_fast_genuine_small_binary_green`). The skip ledger remains the
upgrade path for a real single-test binary under 3 ms.

## Consequences and limits

- gate-truth sees **whole-binary** skips only. A binary where some tests skip and others do real
  work stays above 0.05 s. The declaration layer (D-A) is what catches partial skips. Upgrade
  signal: if gate-truth ever flags a genuinely DB-backed binary (for example, a warm release run
  pushes `facet_contract` below 0.05 s), replace timing with a skip ledger that `skip_or_fail`
  appends to. Do not raise the threshold.
- Four more binaries now write to `humaux_thread_dev` in the chain, including two DDL fixtures
  (D-B). The residue repair covers a killed `FacetShadow`, and HXFACETS serialises every
  `mandatory_context_lane` fixture across processes. Known ceiling: `facet_contract.rs` reads
  the same catalog without taking HXFACETS (outside card 25's files). `cargo test` runs binaries
  one at a time, so a single chain is safe; a concurrent second chain or a parallel runner
  (nextest) needs `facet_contract` to take `pg_advisory_lock_shared` on that key (named in the
  `ponytail:` note on the constant).
- Every external call site in the files this card touched carries a `// dep: <service> — why`
  line (card 26's convention): the five fixture files, `migrate.rs` (including its test helpers),
  `migration_rehearsal.rs` and `soak.rs`.
- Migrate DDL is now bounded by `lock_timeout = 30s`. A deploy that forgets to stop the workers
  gets a refusal with drift 0 instead of a stall.
- The first build on 1.98.1 invalidates every fingerprint in `CARGO_TARGET_DIR`. See Risks in
  the card-25 return: disk space and XProtect first-exec cost.

## Rejected

- **Hand-maintained DB-bound list.** It is a second list of the same tests, and the thing it guards
  against is someone forgetting to update it. It is derived by scanning instead.
- **Re-checking applied migrations' pre/postchecks.** Prechecks are false after apply by
  construction. Old postchecks can be invalidated by later migrations.
- **`pg_advisory_xact_lock` inside one transaction for the whole run.** This would force all 143
  migrations into one transaction (one failure rolls back hours of DDL on a fresh node), and it
  would hold every lock to the end.
- **`pg_try_advisory_lock` polling loop.** This reimplements `lock_timeout` in Rust.
- **Re-baselining to 1.97.1.** 1.98.1 is published for this host. Re-baselining would move the
  spec to fit the builder.
- **Excusing chaos-window request failures now.** That needs the chaos-to-op attribution that card
  54 owns. This card scores the raw rate against an explicit threshold.
- **Pointing the four fixtures at a per-run database inside the main chain.** That is a
  serial-lane redesign (out of scope). The fixtures already run as ordinary tests on the chain
  database, as `request_guard.rs`/`quota_and_rate.rs` do today.
- **Relying on `SKIP …` lines in the chain log instead of timing.** libtest captures the stderr
  of a passing test, so `skip_or_fail`'s `SKIP …` line never reaches the chain log for a test
  that reports as passed.
- **Executing the rehearsal check pass against dev instead of EXPLAIN/prepare.** Execution on a
  head-state database gives wrong answers for prechecks, which are false after apply by
  construction. `EXPLAIN` plus `prepare` in a `READ ONLY` transaction proves syntax and shape
  without executing; the throwaway 0001→head test is what proves pre-state semantics.
- **A hand-rolled `HUMAUX_REQUIRE_DB` check in xtask's own tests, instead of a `humaux-testkit`
  dev-dependency.** That would create a second skip/fail decision point, which testkit's own doc
  forbids (one place in the workspace), and it trips the §79.2 declared-dependency philosophy.
  testkit has zero dependencies, so the dev-dependency costs nothing.
- **A shared `support/fixture_target.rs` module for the four fixtures' target check.** The house
  pattern is the inline `same_target` with a "this is a read of D-D, not a second rule" comment
  (`request_guard.rs`, the g80 `gateway_dsn`). The other copies of that pattern live in files
  outside this card's allowed set, so a shared module would still leave several copies standing;
  adding one more file removes none of them.
- **Accepting any fixture host, dropping the `127.0.0.1` check, per the card's shorthand.** Two
  of these fixtures run DDL as the owner. Loopback-only is the one guard against a mis-exported
  remote DSN reaching a real database, and ADR-0047's implementation keeps it.
- **Soak discovering processes by `ps` name match (`humaux-*`) instead of pidfiles.** A name
  match also catches other sessions' test binaries and gateways on the same host, which confounds
  RSS and presence. Pidfiles already exist in the launcher and are rewritten by the chaos steps.
- **Putting `gate-truth` in `card25_extra_gates.env`.** Extras run before clippy and fmt. The
  card requires gate-truth to be the last gate, reading the chain's own finished log.

## Files outside the allowed list (main-line decision needed)

- `crates/contracts/src/mechanism_registry.rs`: the one-line `as_chunks::<2>()` change (D-G).
  It is caused by this card's toolchain pin, and reverting it makes `clippy -D warnings` red on
  1.98.1. Approve it as an extension, or move it to a separate commit.
- `docs/ops/delivery_plan_v2.md`: the working-tree edits (R-28 ruling, `code_index` removal,
  §5 step-1 wording, research-status paragraph) are plan edits unrelated to card 25. They are
  left untouched here (no checkout/revert); commit them separately from card 25.
- `docs/ops/delivery_point_report.md` §7.2: trimmed to the one-line pointer the card allows.

## Open items (not decided here)

1. Member manifests inheriting `rust-version.workspace = true` (24 `Cargo.toml`s, outside card 25's
   allowed files).
2. Baseline_2.9.md line 3 still reads "Architecture Baseline 2.8" (ARCH-7's second half). Under
   D-G this card does not touch the Baseline, so the header fix needs an allowed-file extension or
   goes to card 26.
3. `rehearse_v2.sh` (TW) must pass the new required soak flags (`--watch-pidfile`,
   `--chaos-grace-secs`, `--max-op-failure-rate`) before the next rehearsal.
