# ADR-0051 — Every module says what it depends on and who calls it, and a gate proves it

- Status: **Accepted** (card 26, 2026-09-27). Design 2026-09-26; implement and review passes 2026-09-27;
  corrections marked inline below.
- Closes: the user's maintainability requirement (「什么地方调用了什么依赖需要都写好注释……防止屎山代码」), audit
  `docs/ops/system_audit_20260926.md` OPS-7 (env vars undocumented), the dependency-map item of delivery_plan_v2 §4
  "Card 26"; folds card 25's open items (ADR-0050: member `rust-version`, Baseline line-3 header, gate-truth breadth,
  migration_rehearsal `;`, soak comm match, g80_31 QUIET lane disposition).
- Spec: delivery_plan_v2 §6 (binding convention text — refined here, not redesigned), §5 (process); Baseline 2.9 §58
  (workspace layout), §6.2.2 (role × table matrix), §50 / §78.1 (typed config registry, no hard-coded config),
  §78.3 (dependency rule), §79.2 (「跳过不等于通过」). ADR-0050 D-C (gate-truth), D-E (manifest check pass), D-I (soak).
- Convention text: `docs/architecture/maintainability.md` (the rules and rule ids live there; this ADR records why).

## Context (measured 2026-09-26 on c459837)

1. 384 `.rs` files, 214,845 lines: crates/ 314, bins/ 45, xtask/ 25 (the card's 383 predates `gate_truth.rs`).
   24 are trybuild fixtures under `*/tests/ui/` (311 lines) with line-pinned `.stderr` snapshots → **360 files in
   scope**. First lines of those 360: 344 start with `//!` (139 already in the form ``//! `path` — sentence``),
   6 with a plain `//` (5 are testkit `// witness:` markers), 1 with `#![…]`, 9 with code.
2. `env::var(` 283 call sites + `var_os(` 4, but only 30 distinct `HUMAUX_*` names are direct literal arguments;
   153 distinct `HUMAUX_*` literals exist because most reads go through helpers (`required("HUMAUX_…")`) or
   consts. The gateway builds most of its names at runtime: `format!("{PREFIX}{suffix}")` over tuple tables,
   `format!("HUMAUX_GATEWAY_RATE_{name}_CAPACITY")` over a loop, and `retrieval_env_key("retrieval.profile.id")`;
   the `HUMAUX_GATEWAY_RATE_*` names never appear as a literal anywhere. Baseline 2.9 names 6 `HUMAUX_` variables
   in prose and has no env registry table; the only typed registry is code (`gateway::bootstrap::registry()` +
   `contracts::retrieval_config`), and the workers read raw variables.
3. 24 member `Cargo.toml`, 213 dependency lines, **0** trailing comments; prose blocks above many lines
   (e.g. `crates/adapters/Cargo.toml` sqlx: 10 lines). No member declares `rust-version.workspace`.
4. 55 `// dep:` tags already exist (card 25), 51 spelled `// dep: Postgres (role_x, DSN) — …`, plus `sh`, `ps`,
   `humaux-gateway`, `Qdrant`.
5. External-call surface (fixed-string counts, all code): `Client::connect(` 207 · `DbPool::connect(` 172 ·
   `.begin()` 162 · `IntraCellRequest {` 53 · `Command::new(` 40 · `SET [LOCAL] ROLE` literals ≈ 84 ·
   `TcpStream::connect` 14 · `UnixStream::connect` 11 · `UnixListener::bind` 11 · egress `.call(…permit…)` 10 ·
   reqwest `.send()` 9 · `PgPoolOptions` 8. Versus per-statement surfaces: `.execute(` 1,189, `.query_one(` 1,152,
   `batch_execute(` 544.
6. gate-truth's DB-bound set (`DB_MARKERS`, 4 testkit markers) misses **28** test sources that reach PG/Qdrant
   with a raw `HUMAUX_TEST_PG_DSN` / `HUMAUX_TEST_QDRANT_PORT`; 4 of them print an ad-hoc `SKIP: HUMAUX_TEST_PG_DSN
   is not set` and return.
7. 7 of 286 manifest checks contain `;` — **all inside `--` SQL comments** (0155, 0161, 0164, 0165, 0167 ×2, 0170).
8. xtask declares `toml`, `postgres`, `serde_json`; `regex` is in Cargo.lock only transitively.

## Decisions

### D-A — Header: five fields first, prose kept below
The first `//!` run of every in-scope file carries, in order: ``//! `<canonical path>` — <sentence>``,
`Depends-on:`, `Called-by:`, `Invariants:`, `Spec:`; continuation lines start `//!` + ≥2 spaces; a blank `//!`
separates the fields from the existing prose, which is never edited. Only blank and plain `//` lines may precede
the header (the testkit `// witness:` markers stay on line 1). Canonical path = package name minus `humaux-`
(dashes kept) + module path; crate roots use the package name; `main.rs` → `<short>::main`; `tests/x.rs` →
`<short>::tests::x`. This matches the 139 existing ``path —`` first lines (e.g. `retrieval-provider::router`,
`humaux-retrieval`) so those lines become field 1 unchanged. `Invariants: []` is legal only for a file with no
services and no env (it has nothing to fail closed about). `Spec:` refs are checked to exist (ADR file, Baseline
heading) — a dangling citation is worse than none.

### D-B — `Depends-on:` refines §6's `r/w schema.table` into `PostgreSQL(role) r=[…] w=[…] x=[…]`
Per role, because one file can speak as two roles (tests: `owner` seeds, `role_gateway` under test) and the
reverse index needs readers vs writers vs EXECUTE callers separately. Service vocabulary is closed
(PostgreSQL, Qdrant, MiniMax, DashScope, UDS, subprocess, HTTP, fs) so the reverse index can group. `crates=` uses
package names, never Rust paths (the architecture gate scans text for `sqlx::PgPool`-like tokens).

### D-C — Call-site tags: anchor on where the role/peer is fixed, not on every statement
`// dep: <Service>[(detail)] — <why>` within 3 lines above the match or above the start of its statement.
Patterns (maintainability.md §2): connection/pool/transaction open, pool-direct execution, role switch literals,
`IntraCellRequest {`, reqwest `.send()`, egress `.call(…permit…)`, UDS connect/bind, `TcpStream::connect`,
`Command::new(`. Estimated ≈ 800 tags (context 5) against ≈ 2,900 per-statement sites — the role is decided at
`.begin()` / `connect`, so the tag there answers "which role, which service" once. Existing tags are rewritten to
the one grammar (no alias `Postgres`), so the checker has one spelling to parse.
Rules apply to `#[cfg(test)]` and `tests/` too — those are where raw DSNs live and what D-K needs.

### D-D — Cargo: `# why: <reason>; used-by: [..]` as the line directly above each dependency
Picked over the trailing form: dependency lines are inline tables up to ~150 chars, and 100+ lines already carry a
prose block *above* them; appending one line to that block keeps the prose, keeps diffs one line, and keeps the
field next to the reason it summarises. `used-by` ⊆ modules that actually name the crate's Rust ident (a claim
nobody backs is red) and non-empty (a dependency nobody names is red); the full computed set lives in
dependency_map.md, so the comment stays short. The line scan is cross-validated against a `toml` parse of the same
file (`cargo-parse`) so a multi-line table cannot hide a dependency from the grammar check.

### D-E — `Called-by` is the static import graph, computed by the checker
Edges come from `use` trees, inline paths, `crate::`/`super::`/`self::`, child-module first segments and
`#[path] mod`; a path resolves to the longest module-file prefix; `mod x;` is not an edge. Set equality both ways.
- **Bins (processes):** `main.rs` is the root: `Called-by: [process(<package>)]`, `process(cargo-xtask)` for xtask.
  Process → module reachability is computed from the roots over non-test edges for dependency_map.md; `process(…)`
  is illegal elsewhere, so Called-by lists stay "direct importers" and do not duplicate the map.
- **Crate roots (`lib.rs`):** `crate(<package>)` per Cargo reverse dependent + in-package importers. Most roots are
  `pub mod` lists whose items resolve to leaves, so the import graph alone would leave them `[]`; Cargo is the
  truthful "who uses this crate".
- **`pub use` re-exports:** one hop of resolution to the leaf (named or `*` with a `pub` item of that name); the
  facade line is not an edge. Otherwise every re-exported leaf would show only its `lib.rs`, which answers nothing.
- **`#[cfg(test)]`:** importers in test context (a `#[cfg(test)]` item, or any file under `tests/`) of non-test code
  collapse to `tests`; test-to-test edges (`#[path]` support files) stay individual because "which test binaries
  include this fixture" is exactly what gate-truth and a flaky-test diagnosis need.
- **Macro-generated modules:** none exist (7 `macro_rules!`, none emits `mod`); macro-invocation arguments are
  scanned as code; the proc-macro crate is an ordinary crate root (`Called-by: [crate(humaux-telemetry)]`) and
  `compile_fail.rs` naming it inside a generated-crate string is not an edge.

### D-F — Env vars: literals found ⊆ declared; runtime-built names are declared with brace expansion
Found = literal argument of the env readers (incl. `var_os`, `env!`, `option_env!`, `Command::env`) ∪ any code
literal that is exactly a `HUMAUX_…` name (catches helpers and consts). Declared-only names are allowed and shown
as `declared-only`, because the gateway's registry names are built at runtime (context 2) and a static evaluator of
`format!` tables would be a second implementation of `registry()` that drifts. "Registry" in env_vars.md means the
§50 typed registry in code; `env-unregistered` is a gateway-only rule (the gateway resolves config only through
it). Worker raw reads are listed and counted as §50 debt — making them errors would need code changes this card
forbids and would be a ratchet in disguise.

### D-G — Tables from string literals only; writes from the verb
`schema.name` tokens in string literals (comments and docs ignored), function when followed by `(`. A literal
with `INSERT INTO|UPDATE|DELETE FROM|MERGE INTO|TRUNCATE|COPY … FROM t` makes `t` a write, so the reverse index's
writer column is computed, not just claimed. Dynamic SQL (`format!("{schema}.{table}")`) is invisible — the header
must still declare it (reviewer), documented as a limit.

### D-H — Exemptions are per-line comments with a rule id and a reason, listed in the map
`// dep-map: allow <rule> — <reason>` (line), `//! dep-map: allow …` (file, in the header), `# dep-map: allow …`
(Cargo). Unused exemptions are red. No allowlist file, no ratchet (R1).

### D-I — Three generated docs, byte-compared; db_objects from the catalog, read-only
dependency_map.md / db_objects.md / env_vars.md, deterministic (sorted, repo-relative, no timestamps). db_objects
reads `pg_class`/`pg_proc`/`pg_namespace`/`aclexplode(relacl|proacl|attacl)` in a `READ ONLY` transaction on
`HUMAUX_TEST_PG_DSN`, reusing `rls_check`'s schema list and connection style; it lists only objects some
`migrations/*.sql` creates (first creating migration by number), so fixture objects in the shared DB never make
the doc flap; it refuses unless `ops.schema_migrations` equals the repo's migration set (`db-not-at-head`).
§6.2.2 agreement is inherited from `rls-check` on the same catalog; not re-parsed here.

### D-J — `cargo xtask dep-map [--check|--write|--suggest <files>]`
No flag = `--check`. Exit 0 pass, 1 violations or drift, 2 usage/IO/missing DSN (skip ≠ pass, ADR-0050).
`--write` regenerates and still reports (exit 1 on violations). `--suggest` prints the computed header parts so the
backfill agents copy instead of grepping callers (the token-discipline requirement of card 26); the purpose,
Invariants, Spec and the r/w split stay human. No new xtask dependency: a ~300-line masking lexer (line/block/
nested comments, `"…"` with escapes, `r#"…"#`, `b"…"`, char literals vs lifetimes) plus string scanning covers every
rule; `toml` (present) validates Cargo, `postgres` (present) reads the catalog. Budget ≤ 30 s (expected < 2 s:
215 k lines in memory + two catalog queries).

### D-K — gate-truth's DB-bound set comes from the same map (one source of truth)
`gate_truth::db_bound_stems` is replaced by `dep_map::test_target_services(root)`: an integration target is
DB-bound when its **default run** (`cargo test`, no `--ignored` — what the chain runs) reaches PostgreSQL or
Qdrant: a call-site match (D-C patterns), a `HUMAUX_TEST_PG_DSN`/`HUMAUX_TEST_QDRANT_PORT` literal or a testkit
fixture marker inside the non-`#[ignore]` `#[test]` functions or anything they name (local `fn`/`const`/`static`/
`macro_rules!` items by name — a method name matches every item of that name, over-inclusion never under-; a
`#[path]` module by its name or an ident `use`d from it). A target with no `#[ignore]` is judged whole. The floor
then applies to every fast DB-bound binary, whatever its `ignored` count (review P1, card 26: an earlier pass
excused any fast binary with ignored > 0, which let "one lane test + one silently skipping DB test" through). Computed facts, not headers, so gate-truth stays correct while the backfill is in flight. The
same function renders dependency_map.md §Test binaries. The 28 raw-DSN targets of context 6 become DB-bound. The 4
ad-hoc SKIP paths become `humaux_testkit::skip_or_fail(…, ExternalDep::Postgres)` (testkit is already their
dev-dependency). Not converted: `public-worker`/`retrieval-worker` `readyz_probe.rs` (their local `skip()` already
honours `HUMAUX_REQUIRE_DB`; converting needs a testkit dev-dependency = a non-comment Cargo change), and
`contribution_policy_lifecycle_0132.rs` (skips on `HUMAUX_0132_GATE_MODE`, a serial-lane mode, not a missing dependency).

### D-L — Folded card-25 debts
- `rust-version.workspace = true` in all 24 member manifests (lockfile unchanged; future `cargo update` becomes
  MSRV-aware, which is the point). Baseline_2.9.md line 3: `Architecture Baseline 2.8` → `2.9`, that line only.
- `migration_rehearsal`: `parse_manifest` refuses a check with a `;` **outside** SQL comments and literals (a
  naive `contains(';')` would reject the 7 valid manifests of context 7; manifests of applied migrations are not
  edited); the EXPLAIN goes through the extended protocol (`sp.query(&format!("EXPLAIN {sql}"), &[])`), where the
  server itself rejects a second command — the parse-time rule is the early, named error; the protocol is the guarantee.
- `soak::classify_presence`: alive iff the `comm` **basename** starts with `humaux-` (macOS prints the full path, and
  `…/humaux-target-boot/…` made any binary built there "alive"; Linux truncates comm to 15 chars, which still starts with `humaux-`).
- TW `rehearse_v2.sh` and `docs/ops/rehearse.sh` (done in the review pass; the first pass described it but had not
  edited either file): the soak call passes `--watch-pidfile` for the five pidfiles the script already maintains
  (`gw rw pw ds cw`), `--chaos-grace-secs ${SOAK_CHAOS_GRACE:-60}` and `--max-op-failure-rate
  ${SOAK_MAX_OP_FAIL:-0.01}`; the two files still differ only on line 4 (work dir). Test
  `soak::tests::rehearse_script_soak_invocation_parses` feeds the script's own soak words through `parse_config`,
  so a missing required flag is a red unit test, not card 27's rehearsal dying at argument parsing (fault-checked:
  deleting the `--max-op-failure-rate` line turns it red).
- `g80_31_handoff` QUIET mutex: recorded in `docs/ops/serial-lane.md` §"Known flake source" (review pass; the first
  pass described it but had not edited the file): binary-local lock, cluster-wide `pg_current_snapshot()`
  precondition, diagnosis (a token-precondition failure is the flake, a byte diff with equal tokens is a
  regression) and disposition (move to `lane(b)` with that reason if it flakes; it did not on card 26).
- `docs/ops/supervision.md` (review pass): its inline env names are replaced by one pointer to the generated
  `env_vars.md`; only the release-build shell command keeps its two build variables.

## Measurements (to fill in the implement / verify passes)

| Quantity | Before backfill | After |
|----------|-----------------|-------|
| files checked / Cargo.toml checked | 360 / 24 (+ `dep_map.rs` itself = 361) | |
| violations (`--check` on c459837 + checker only) | **6,360** | 0 |
| — `table-undeclared` | 1,612 (154 files) | |
| — `header-field` | 1,440 (360 files × 4 missing fields) | |
| — `callsite-untagged` | 774 (171 files) | |
| — `table-write` | 639 | |
| — `env-undeclared` | 459 (142 files) | |
| — `calledby-missing` | 326 | |
| — `modules-drift` | 278 | |
| — `crates-undeclared` | 277 | |
| — `header-path` | 236 | |
| — `cargo-why` | 213 | |
| — `tag-grammar` | 55 (every pre-card-26 tag) | |
| — `env-unregistered` | 47 | |
| — `doc-drift` | 3 (docs not yet generated) | |
| — `calledby-phantom` | 1 (`dep_map.rs` names `gate_truth` before gate_truth imports it) | |
| exemptions | 3 (all in `dep_map.rs`: its own test fixtures) | n (listed) |
| `// dep:` tags | 55 | |
| gate-truth DB-bound set | 55 stems from 4 markers | whole-binary: 83 stems (+28: 24 raw-DSN/raw-Qdrant + 4 converted SKIPs); default run (review pass, D-K): 58 |
| `--check` wall clock | 1.34 s (binary), 1.95 s via `cargo xtask` | ≤ 30 s |

## Implementation corrections (implement pass, 2026-09-27)

- **gate-truth vs `#[ignore]`d lane tests.** With the whole-binary set, 7 binaries on the card-25 chain log
  (`contribution_execution_0131`, `contribution_reasoner`, `contribution_self_principal_authority_0133`,
  `public_provenance`, `public_provenance_revocation_eval`, `public_trust`, `contribution_execution_runner`) are
  DB-bound but every DB test is `#[ignore = "lane(a…)"]`; the one pure test passes in 0.00 s. The implement pass
  excused them with a "not judged" branch for any ignored > 0 — **withdrawn in the review pass** (it weakened the
  floor beyond the card). The fix is at the root: the DB-bound set is the default-run closure (D-K), so those 7 are
  not DB-bound (their default run touches no database) and the floor judges every binary that is. Card-25 log
  replay: 88 binaries, 55 DB-bound (default run), 0 offenders; the card-24 replay still flags exactly the four
  fixtures. Tests: `gate_truth_ignored_count_does_not_excuse_a_fast_db_bound_binary`,
  `gate_truth_db_bound_set_is_the_default_run_closure` (pure + lane → not bound; helper, `#[path]` module or DSN
  literal reached from a non-ignored test + lane → bound).
- **`modules=` of test files.** A file under `tests/` is test code throughout, so its `modules=` lists all its
  outgoing edges (otherwise every integration test would declare `modules=[]`); src files list non-test edges only.
- **`test_target_services` facts.** Call-site classes imply a service (`tcp` implies none — it is ambiguous;
  `http-send`/`egress-call` imply `HTTP`); testkit markers are matched on masked code, so a marker named only in a
  comment no longer makes a binary DB-bound.
- **Exemption validity.** An exemption naming an unexemptable or unknown rule, or with an empty/`TODO` reason, is an
  `exemption-grammar` violation and is neither applied nor reported again as `exemption-unused`.

## Review pass (2026-09-27) — the checker verified the header's shape, not its truth

The review found that a header could over-claim or mis-claim and still pass, and that one parser rule inverted
reads/writes/executes. Every item below is a checker change plus the header fixes it forced; none is an exemption.

### D-M — P0: a column list is not a call
`schema_tokens` called `schema.t(` a function whatever preceded it, so `INSERT INTO t(cols)` counted as an EXECUTE
of `t`, `table-write` never fired for it (the write pass only accepted non-function tokens), and the maps showed
`adapters::contribution_entry_repo` with `w=[]` and tables under `x=` (461 such literals). Now `(` after `INTO`,
`REFERENCES`, `TABLE`, `EXISTS`, `ON`, `COPY`, `UPDATE` or `ONLY` opens a column list, and the write pass takes the
token after a write verb as the written relation, column list or not. The two parts were fixed separately: the unit test
`table_insert_with_column_list_is_a_write_not_a_function` caught that the write pass still dropped `INSERT INTO t(`
(it re-scanned the suffix, where the verb is not visible) after the first fix alone had passed `--check`. Effect: 137
`table-undeclared` + 177 `table-write` on the tree, all fixed by moving the tables into `w=` of the role that holds them.

### D-N — Over-declaration is an error (witness rules)
`crates-unwitnessed` (listed crate the code never names, including non-dependencies), `table-unwitnessed` (`r=`
entry no literal names, `w=` entry no literal writes, `x=` entry no literal calls), `service-unwitnessed` (a service
item with no witness in the file: a tag of that service and detail; for PostgreSQL a typed pool naming the role or a
schema token its lists name; a dependency env literal or testkit marker). Transitive dependencies belong to the
callee's header. Examples fixed: `infra-egress::http` declared `Qdrant(*), DashScope` with only HTTP tags (Qdrant is
same-Cell), and 49 unwitnessed or malformed service items were dropped across the tree (e.g. `projection`, `infra-cell`, `infra-network` and `testkit` declaring `Qdrant(*)`/`DashScope`/`MiniMax` they never touch).
`fs(…)` is witnessed by a `// dep: fs(…)` tag (one: `gate_truth`).

### D-O — Closed detail vocabulary and code-fixed details (`service-vocab`, `tag-grammar`, `tag-mismatch`)
Details are required (except MiniMax/DashScope, which take none) and closed: PostgreSQL = a frozen role from
`rls_check`, `owner` or `any`; UDS = `serve` or a workspace binary's short name; subprocess = the program (generic
words rejected); HTTP = a short peer name (never `humaux-…`). A tag must match a declared item exactly (only
`PostgreSQL(any)` matches any role). Where the code fixes the detail, the nearest tag must say it: typed pool
`<X>DbPool::connect(` → its role, `SET [LOCAL] ROLE r` → `r`, `UnixListener::bind` → `serve`, `UnixStream::connect` →
not `serve`, `Command::new("…/p")` → `p`. `Command::new(` preceded by an identifier character
(`StartManualContributionCommand::new(`) is no longer a subprocess site. On the tree this found 147 tags naming the
wrong role (e.g. `retrieval-worker::main` readyz tagged `role_private_worker` while connecting `RetrievalWorkerDbPool`),
the retrieval worker declared as a UDS client of itself, and `local-secret-scan`'s gitleaks spawns tagged `proc`.

### D-P — Header content, not just shape (`header-purpose`, `spec-ref`, `header-invariants`)
- Field 1 may continue on `//!  ` lines and must end its sentence (`.`/`。`); 40 headers had split a sentence.
- `Spec: none` while the prose cites resolvable `§`/`ADR` refs, or a `Spec:` sharing none of them, is `spec-ref`
  (131 files; filled from the prose's own refs, in order, at most eight). 25 `xtask` files carried `ADR-0051` (this
  ADR) as their spec; kept only on `dep_map`, `gate_truth` and `main`.
- `Invariants:` may not point elsewhere (`see prose`, `see body`, …) and no text may be shared verbatim by ≥ 3 files
  (`INVARIANT_COPIES`): 180 headers rewritten with their own invariant and failure behaviour.

### Prose restoration (fault the review did not list)
The backfill had **deleted** header prose in 48 files (`application`, `protocol`, `retrieval`, `retrieval-provider`,
…) and duplicated the first prose line under the fields in 95. Every file was rebuilt from `HEAD`'s header block:
field 1 = the old first sentence, the rest verbatim below the fields. Proof: for every changed non-xtask file the
whitespace-normalised old header text equals the new field 1 + prose (2 files differ by one character: a sentence
that ended in `:` now ends in `.`).

| Quantity (review pass) | Count |
|------|------|
| violations on the backfilled tree when the new rules landed | 1,393 (`tag-grammar` 347, `header-invariants` 256, `tag-mismatch` 175, `service-undeclared` 145, `table-undeclared` 137, `spec-ref` 114, `table-unwitnessed` 62, `service-unwitnessed` 42, `crates-unwitnessed` 41, `header-purpose` 40, `service-vocab` 34), then 177 `table-write` from D-M's second half and 17 `spec-ref` from the Spec-vs-prose rule |
| after | 0 violations, 23 exemptions (unchanged), docs in sync |
| `// dep:` tags | 858, every one with a detail |
| gate-truth DB-bound (default run) | 58 of the repo's test targets; 55 of the 88 binaries on the card-25 log |
| `--check` wall clock | 1.65 s in-process, 2.4 s via `cargo xtask` |

## Consequences and limits

- Every later card is checked against this convention; a new import, env var, table or external call without the
  header/tag change is red in the chain. Maintenance cost moves to the author of the change, where it is cheapest.
- `Called-by` is static imports; trait-object/DI calls surface at the wiring site only.
- Runtime-built env names and dynamic SQL are declared, not proven (`declared-only` column; reviewer).
- Which role writes a table is the header's claim. The witness rules prove the table is written in the file, not
  under which of the file's roles; `tag-mismatch` proves the role only at typed-pool and `SET ROLE` sites, not at a
  raw `Client::connect` whose DSN comes from config (two such `any` tags were corrected by hand: `projection_serve`,
  `soak`).
- The default-run closure matches names, not types: a method call matches every item of that name
  (over-inclusion), and a helper reached only through a trait object or a function pointer stored in a static is
  not followed (under-inclusion). The floor stays the backstop; a skip ledger written by `skip_or_fail` is the upgrade.
- `Invariants:` content is reviewed, not proven: the checker rejects pointers and copies, not wrong claims.
- Filesystem access outside the repo is declared (`fs(…)`), not pattern-checked.
- `[a::b]` single-item lists in `//!` could trip rustdoc's `broken_intra_doc_links` if `cargo doc -D warnings` is
  ever gated; it is not today.
- Workers have no typed config registry; env_vars.md shows each raw read — the upgrade signal for a §50 card.

## Rejected

- **R1 — Ratchet / legacy allowlist file** (plan A26 `xtask/dep_map_baseline.txt`, B26 `dep_map_legacy.txt`):
  rejected by the main line — it leaves ~187 modules undocumented indefinitely and moves the burden to whoever
  touches a file next. Full backfill now; per-line visible exemptions instead.
- **R2 — Humaux `code_index`/`code_query` for callers:** the server cannot read this host's filesystem; pushing
  384 files through MCP costs more tokens than the checker's own `use` scan (main line, verified 2026-09-26).
- **R3 — `syn` or `regex` as a new xtask dependency:** `syn` gives exact use-trees but adds a parser crate and
  compile time for a surface a masking lexer covers; `regex` cannot mask comments/strings (the rules are about
  *where* a token is), and it is not a declared dependency.
- **R4 — Trailing `# why:` on the dependency line:** see D-D.
- **R5 — Tagging every SQL statement:** ≈ 2,900 sites of noise; the role is fixed at the transaction/connection.
- **R6 — Pattern-checking filesystem calls:** 300 sites; repo-relative, tempdir and outside-repo paths are
  indistinguishable statically.
- **R7 — Fully generated headers:** would make the check tautological and drop the human fields (purpose,
  Invariants, Spec) that make a header diagnostic; computed parts are checked, and `--suggest` only proposes them.
- **R8 — Static evaluator of the gateway registry's `format!` tables:** a second, brittle copy of `registry()`.
- **R9 — db_objects from the migrations scan alone:** owner/RLS/grants are the cumulative effect of 170+
  ALTER/GRANT/REVOKE migrations; replaying them is a SQL interpreter. The catalog is the truth; migrations give provenance.
- **R10 — `--check` passing with a warning when no DSN:** a skip is not a pass (ADR-0050, §79.2).
- **R11 — Excluding `tests/` from the convention:** tests hold the raw DSNs gate-truth missed; D-K needs them in the map.
- **R12 — Headers on trybuild fixtures:** shifts the line numbers their `.stderr` snapshots pin.
- **R13 — Refusing any `;` in manifest checks:** rejects 7 valid, already-applied manifests whose `;` is in comments.
- **R14 — Accepting the legacy `// dep: Postgres (role, DSN)` spelling as an alias:** two spellings mean two
  parsers and an inconsistent map; the 55 legacy tags (context 4) are rewritten once to the one grammar (D-C).

## Files outside the card's allowed list (main-line decision needed)

The folded debts need code changes the "comment lines only" rule forbids: `xtask/src/gate_truth.rs`,
`xtask/src/migration_rehearsal.rs`, `xtask/src/soak.rs` (+ tests), `xtask/src/rls_check.rs` (`SCHEMAS` made
`pub(crate)` so dep-map reads the one schema list), the 4 skip_or_fail test files, `docs/ops/supervision.md`, `docs/ops/serial-lane.md`,
`docs/architecture/Baseline_2.9.md` (line 3), `docs/ops/rehearse.sh` + TW `rehearse_v2.sh`, and one
non-comment line (`rust-version.workspace = true`) in each member Cargo.toml. The comment-only proof command must
exclude exactly these paths (`':!xtask/src/gate_truth.rs' …`) and they get their own named tests.

## Final gate replay (checker + reconcile passes, 2026-09-27)

Two independent replays after the review pass landed, each starting from its own before-state and finishing on
the same acceptance gate (`dep-map --check` → 0, three docs byte-identical, full chain green).

### Checker gate

| Check | Result |
|---|---|
| `cargo test -p xtask` (`HUMAUX_REQUIRE_DB=1`, live DSN) | EXIT=0: 540 passed, 0 failed (38 `dep_map` tests incl. `real_tree_sample_headers_pass` + 2 DB tests; 9 `gate_truth` tests; 3 `migration_rehearsal` tests; `soak::tests::presence_matches_executable_basename_not_path`) |
| Mutation red-check, then restored (`cmp`) | Each reversion failed its test: EXPLAIN back to `batch_execute` → `explain_uses_extended_protocol_multi_statement_rejected`; `comm.contains` back → `presence_matches_executable_basename_not_path`; disabling the re-export hop → `called_by_pub_use_reexport_resolves_to_leaf_one_hop_incl_glob`; lifetimes treated as char literals → `lexer_masks_*`. All green again after restore |
| `dep-map --check`, before (c459837 + checker only, `git archive` to scratch) | EXIT=1: 361 files, 24 Cargo.toml, **6,360** violations, 3 exemptions, 1.34 s |
| — by rule | table-undeclared 1,612 (154 files) · header-field 1,440 (360 files) · callsite-untagged 774 (171 files) · table-write 639 · env-undeclared 459 (142 files) · calledby-missing 326 · modules-drift 278 · crates-undeclared 277 · header-path 236 · cargo-why 213 · tag-grammar 55 · env-unregistered 47 · doc-drift 3 · calledby-phantom 1 (`dep_map.rs` names `gate_truth` before `gate_truth` imports it) |
| `dep-map --check`, after `--write` | EXIT=1: 361 files, 24 Cargo.toml, **6,346** violations, 3 exemptions, docs in sync, 1.26 s (1.95 s via `cargo xtask`); 0 violations in the two sample-header files (`xtask/src/dep_map.rs`, `xtask/src/gate_truth.rs`) |
| `--write` determinism | Two `--write` runs; `shasum -c` on all three docs: OK OK OK (919 / 367 / 187 lines). `db_objects.md`: 359 migration-created objects, rendered from a `READ ONLY` transaction that is rolled back |
| Exit codes | `--bogus` → 2 (usage); `HUMAUX_TEST_PG_DSN` unset → 2 ("db_objects.md cannot be checked"); violations → 1 |
| gate-truth replay | card-25 mainline log: pass (88 binaries, 81 DB-bound, 0 offenders, 7 "not judged"). card-24 final3 log: still exactly the 4 fixture offenders (`g80_31_handoff`, `mandatory_context_lane`, `provider_budget`, `retrieval_query_sources`), EXIT=1. DB-bound set grows from 55 stems to 83 (+28) |
| `skip_or_fail` conversion | `contribution_execution_repo_0131` `--ignored` with DSN unset: under `HUMAUX_REQUIRE_DB=1` it panics ("HUMAUX_REQUIRE_DB is set, so a skip here is a failure"); unset, it prints "SKIP … (§79.2)" |
| `cargo build --workspace` | EXIT=0 |
| `cargo clippy --workspace --all-targets -- -D warnings` | EXIT=0 |
| `cargo fmt --all --check` | EXIT=0 |
| Comment-only proof (`.rs`, excluding the 9 debt/checker files) | 0 |
| `Cargo.toml` member diff | 24× `+rust-version.workspace = true` and nothing else (root `Cargo.toml` change is comment-only) |
| Secret grep (card-25 exact command) | 0 |

Note: the "before"/"after `--write`" pair above (6,360 → 6,346) reflects the checker binary alone replayed on a
scratch export mid-review, ahead of the backfill; it is a fixture for the rule-set change, not the card's overall
before/after (that is the Measurements table above: 6,360 → 0 once the backfill and the D-M…D-P rule corrections
both landed). The Reconcile gate below is the actual final-tree replay.

### Reconcile gate (final tree)

| Check | Result |
|---|---|
| `dep_map --check`, before | 361 files, 24 Cargo.toml, 822 violations, 6 exemptions, docs drift × 3 (143 files). By rule: callsite-untagged 201, table-undeclared 169, table-write 127, modules-drift 65, cargo-why 62, service-undeclared 53, calledby-phantom 41, calledby-missing 31, tag-grammar 30, crates-undeclared 22, service-vocab 8, cargo-usedby 6, env-undeclared 4, doc-drift 3 |
| `dep_map --check`, after | `dep-map: 361 files, 24 Cargo.toml, 0 violations, 23 exemptions, docs in sync, 1.34s`; exit 0 (wall clock 2.03 s) |
| `dep_map --write` | 0 violations, 23 exemptions, docs written; the following `--check` reports docs in sync (the three docs are untracked, so `git diff --exit-code` does not apply; `--check` does the byte comparison) |
| `cargo build --workspace` | exit 0, 0 warnings (22.8 s) |
| `cargo check --workspace --tests` | exit 0, 0 warnings (46.1 s) |
| `cargo test -p xtask -- dep_map` | ok, 38 passed, 0 failed |
| `cargo fmt --all --check` | exit 0 |
| Comment-only proof, all `.rs` | 357 — all inside the pre-existing Checker/folded-debt files: `migration_rehearsal.rs` 145, `gate_truth.rs` 124, `soak.rs` 26, the 4 `skip_or_fail` conversions at 14 each (`crates/adapters/tests/contribution_execution_repo_0131.rs`, `crates/adapters/tests/contribution_execution_ingress_0131.rs`, `bins/private-worker/tests/start_manual_contribution_command_0131.rs`, `bins/private-worker/tests/contribution_execution_runner.rs`), `xtask/src/main.rs` 4 (dep-map dispatch + usage), `xtask/src/rls_check.rs` 2 (`SCHEMAS` made `pub(crate)`) |
| Comment-only proof, excluding those allowed files | 0 |
| `Cargo.toml` non-comment diff | only 24× `+rust-version.workspace = true` (folded debt) |
| Card 26 extra gates env | `dep_map`/`cargo xtask dep-map --check` (+ `xtask_dep_map_tests`, `migration_rehearsal`, `soak_unit`) already present in the TW dir; left as is |

Both replays land on the same acceptance state: `dep-map --check` exits 0 on the final tree, the three generated
docs are byte-identical to the committed ones, and the full chain (including `card26_extra_gates.env`) is green.

## Open items

- A typed §50 registry for the four workers (env_vars.md `raw` rows are the list).
- `cargo doc -D warnings` as a gate would need the list syntax revisited (see limits).
