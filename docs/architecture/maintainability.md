# Maintainability convention — checked headers, call-site tags, Cargo `# why`, generated maps

> Status: **binding from card 26 on** (delivery_plan_v2 §6, ADR-0051; review-pass rules D-M…D-P added 2026-09-27).
> Why this exists (the user's rule): 「为了方便整个项目后期升级和维护……什么地方调用了什么依赖需要都写好注释，
> 这样方便有问题的时候诊断分析，防止屎山代码」. A hop must be diagnosable from the module headers and the three
> generated maps alone, without reading code, and no module may grow a silent dependency.
> Nothing here is advisory: every rule below has a rule id, `cargo xtask dep-map --check` enforces it in the
> gate chain, and the only escape hatch is a visible, counted, per-line exemption (§6).

## 0. Scope

- Every `.rs` file under `crates/`, `bins/`, `xtask/` — src, `tests/`, `tests/support/`, `build.rs`.
- **Excluded by path, not by exemption:** `*/tests/ui/*.rs` (24 trybuild fixtures). They are not modules of
  any crate (trybuild compiles each standalone) and their `.stderr` snapshots pin line numbers, so a header
  would break the snapshot. The generated map lists them under "excluded fixtures" with their count.
- Every `Cargo.toml` of a workspace member (24) — `[dependencies]`, `[dev-dependencies]`,
  `[build-dependencies]`, and `[target.'…'.dependencies]` tables.

## 1. Module header (every file, rule ids `header-*`)

The header is the **first run of `//!` lines** in the file. Only blank lines and plain `//` comments
(e.g. the `// witness:` markers `metrics-registry` reads) may precede it; `#![…]` inner attributes and all
items come after it (an inner doc comment after an item is a compile error — `cargo check -p` after editing).

Illustrative (the backfill derives the real values):

```rust
//! `adapters::qdrant` — Qdrant multitenant placement adapter: collection/index shaping and wire calls.
//! Depends-on: crates=[humaux-domain, humaux-infra-cell, humaux-projection, serde_json]; services=[Qdrant(*)];
//!   env=[]; modules=[adapters::postgres, projection::dense]
//! Called-by: [adapters::projection_worker, adapters::retrieve, gateway::recall, xtask::e2e_seed, tests]
//! Invariants: [every wire call carries a CellAccessPermit; Qdrant down → QdrantTransportError to the caller,
//!   no fallback search here; <the degrade code the caller emits>]
//! Spec: Baseline §17; §23.4; ADR-0003
//!
//! (existing prose, kept verbatim — it cites the §s and the reasons)
```

Five fields, **in this order, each present, never omitted** (`header-field`):

| # | Field | Grammar | Checked against |
|---|-------|---------|-----------------|
| 1 | purpose | ``//! `<path>` — <one sentence>.`` | `<path>` must equal the file's canonical path (`header-path`); the sentence is whole: after joining its continuation lines it ends in `.` or `。` (`header-purpose`) |
| 2 | `Depends-on:` | `crates=[…]; services=[…]; env=[…]; modules=[…]` — all four sub-lists, in this order, each may be `[]` | §1.2 |
| 3 | `Called-by:` | `[entry, …]` | computed importers, set-equal (§4) |
| 4 | `Invariants:` | `[free text]` — may be `[]` only when `services=[]` and `env=[]` (`header-invariants`) | not a pointer (`see prose`, `see body`, …) and not text shared verbatim by ≥ 3 files (`header-invariants`); the content itself is reviewed |
| 5 | `Spec:` | `Baseline §x.y; ADR-00nn; …` or `none` | each `ADR-00nn` exists in `docs/adr/`, each `§x.y` is a Baseline heading; when the prose below the fields cites resolvable refs, `Spec:` is not `none` and names at least one of them (`spec-ref`) |

**Continuation:** a field — field 1 included — may continue on following lines that start with `//!` + at least
two spaces. The parser joins them with one space. A blank `//!` line or the next field ends a field. So field 1 is
the file's first prose **sentence**, whole, even when it spans three lines; it never stops mid-sentence with the
rest of the sentence parked below the fields.

**Existing prose:** the old header's first sentence *becomes* field 1 (its ``` `path` — ``` prefix fixed to the
canonical path, e.g. `infra-cell` → `humaux-infra-cell`, or added when the old line had none), continued on
`//!  ` lines until the sentence ends; the four field lines go directly under it, then one blank `//!`, then the
rest of the old prose unchanged (a line split by the sentence end keeps its remainder as the first prose line).
Never delete or reword prose.
Lowercase prose lines such as card 25's `//! depends-on: …` are prose, not fields; keep them.

### 1.1 Canonical paths (used in field 1, `modules=`, `Called-by:`, Cargo `used-by:`)

`<short>` = package name with the leading `humaux-` removed, dashes kept (`retrieval-provider`, `infra-cell`);
`xtask` stays `xtask`.

| File | Canonical path |
|------|----------------|
| `src/lib.rs` | package name, e.g. `humaux-adapters` (the crate root) |
| `src/main.rs` | `<short>::main` |
| `src/a.rs`, `src/a/mod.rs` | `<short>::a` |
| `src/a/b.rs` | `<short>::a::b` |
| `tests/x.rs` | `<short>::tests::x` |
| `tests/<dir>/y.rs`, `tests/<dir>/mod.rs` (support, fault, pair, metrics) | `<short>::tests::<dir>::y`, `<short>::tests::<dir>` |
| `build.rs` | `<short>::build` |

### 1.2 `Depends-on:` sub-lists

- `crates=[…]` — every crate the file names in code (`humaux_x::…`, `sqlx::…`, `use tokio::…`), written as the
  **package name** (`humaux-domain`, `sqlx`, `serde_json`). `std`/`core`/`alloc` are never listed.
  Must equal the computed set: a crate named in code but not listed is `crates-undeclared`, a listed crate the
  code never names (or that is not even a dependency of the package) is `crates-unwitnessed`. Never write a Rust
  path here (the architecture gate scans text for `sqlx::PgPool`-style tokens).
- `services=[…]` — closed vocabulary (`service-vocab`):

  | Item | Meaning |
  |------|---------|
  | `PostgreSQL(<role>) r=[…] w=[…] x=[…]` | one item per role the file talks as: a frozen role (`role_gateway`, …, `role_migration_owner`, from `xtask::rls_check`), `owner` (migration owner / test admin DSN), `any` (helper generic over pools). Anything else (`PostgreSQL(PRIMARY)`, bare `PostgreSQL`) is `service-vocab`. `r`/`w` list `schema.table`, `x` lists `schema.function` it EXECUTEs. Omit an empty `r=`/`w=`/`x=`. |
  | `Qdrant(<collection or *>)` | same-Cell Qdrant REST; the detail is required |
  | `MiniMax`, `DashScope` | provider HTTP through the egress transport; no detail |
  | `UDS(<peer>)` | Unix-socket RPC; a client names the other process by its short name (`retrieval-worker`, `private-worker` — a workspace binary); a server writes `UDS(serve)`. `UDS(peer)` or bare `UDS` is `service-vocab` |
  | `subprocess(<program>)` | `Command::new` (`gitleaks`, `ps`, `sh`, `humaux-gateway`, `psql`); a generic word (`proc`, `process`, `binary`, `cmd`, `command`, `exec`, `program`, `child`) is `service-vocab` |
  | `HTTP(<peer>)` | any other HTTP (`loopback` test servers, `provider` for the egress transport itself, `gateway` from `xtask soak`); the peer is a short name, never `humaux-…` |
  | `fs(<what>)` | filesystem outside the repo; not pattern-checked (ADR-0051 R6), but witnessed like any service by a `// dep: fs(<what>)` tag above one access |

  **Every item needs a witness in the file's own code** (`service-unwitnessed`): a `// dep:` tag of that service and
  detail (a `PostgreSQL(any)` tag or item matches any role), or — PostgreSQL only — a typed pool naming the role
  (`RuntimeDbPool` → `role_gateway`, …) or a schema token its lists name (with no lists: any schema token), or a
  dependency env literal (`HUMAUX_TEST_PG_DSN`, `HUMAUX_TEST_QDRANT_PORT`, `DASHSCOPE_API_KEY`, `MINIMAX_API_KEY`) or
  testkit marker for that service (`any`/`owner` only for PostgreSQL). A header cannot claim a dependency the code
  does not have; transitive dependencies belong to the callee's header.

- `env=[…]` — every environment variable the file names (§5.1), comma-separated. Brace expansion is allowed
  for names the code builds at runtime: `HUMAUX_GATEWAY_RATE_{PREAUTH_IP,CREDENTIAL,USER,TENANT,OPERATION}_{CAPACITY,REFILL_PER_SECOND}`
  expands to ten names.
- `modules=[…]` — every workspace module this file names in non-test code (canonical paths); set-equal to the
  computed outgoing edges (`modules-drift`). Test-only references are not listed here.

## 2. Call-site tags `// dep:` (rule ids `callsite-untagged`, `tag-grammar`, `service-undeclared`)

Grammar (one line, exactly): `// dep: <Service>[(<detail>)] — <why here>`
`<Service>` ∈ `PostgreSQL | Qdrant | MiniMax | DashScope | UDS | subprocess | HTTP | fs`, detail as in §1.2
(`PostgreSQL(role_gateway)`, `subprocess(gitleaks)`). The separator is ` — ` (em dash). The 55 pre-card-26 tags
spelled `Postgres (role_x, DSN)` are rewritten to this form by the backfill.

Every line of **code** (comments and string contents masked) that matches a pattern below needs a tag whose
service is compatible **and** declared in the header with the **same detail** (`service-undeclared`; only a
`PostgreSQL(any)` tag matches any declared role). The detail is required and follows §1.2's vocabulary
(`tag-grammar`). Where the code itself fixes the detail, the nearest compatible tag must say it (`tag-mismatch`):

| Site | Tag detail must be |
|------|--------------------|
| `<Type>DbPool::connect(` | the pool's role: `Runtime`→`role_gateway`, `BatchIssuer`→`role_batch_issuer`, `Consolidation`→`role_consolidation_worker`, `PrivateWorker`→`role_private_worker`, `RetrievalWorker`→`role_retrieval_worker`, `Maintenance`→`role_maintenance`, `PublicWorker`→`role_public_worker`, `Admin`→`role_admin` |
| a literal `SET [LOCAL] ROLE role_x` | `role_x` |
| `UnixListener::bind` | `serve` |
| `UnixStream::connect` | anything but `serve` (the peer) |
| `Command::new("…/prog")` | `prog` (basename of the literal) | The tag must sit within the **3 lines above the matched
line**, or within the 3 lines above the **first line of the statement** containing the match (walk up while the
previous code line does not end in `;`, `{` or `}`) — so one tag above `let r = transport` covers a
`.execute(` / `IntraCellRequest {` chain that starts three lines later.

| Rule class | Pattern (masked code) | Compatible services |
|------------|-----------------------|---------------------|
| `pg-connect` | `Client::connect(` · `DbPool::connect(` · `PgPoolOptions::new(` | PostgreSQL |
| `pg-txn` | `.begin()` · `.build_transaction()` | PostgreSQL |
| `pg-pool-exec` | `.execute(` `.fetch_one(` `.fetch_all(` `.fetch_optional(` `.fetch(` whose first argument text contains `pool` | PostgreSQL |
| `pg-role-switch` | a string literal containing `SET ROLE` or `SET LOCAL ROLE` (case-insensitive); anchor = the literal's first line | PostgreSQL |
| `qdrant-request` | `IntraCellRequest {` | Qdrant |
| `http-send` | `.send()` (reqwest dispatch; `send(x)` with an argument is a channel and does not match) | HTTP, Qdrant, MiniMax, DashScope |
| `egress-call` | `.call(` whose first argument text contains `permit` | MiniMax, DashScope, HTTP |
| `uds` | `UnixStream::connect` · `UnixListener::bind` | UDS |
| `tcp` | `TcpStream::connect` | PostgreSQL, Qdrant, HTTP |
| `subprocess` | `Command::new(` not preceded by an identifier character (`XCommand::new(` is a constructor, not a spawn) | subprocess |

Deliberately **not** patterns: every `.execute(txn)` / `.query_one(` on an open transaction (the role is fixed where
the transaction or connection is opened — tag that), `.transaction()` savepoints on a `postgres::Client`, and
filesystem calls. The rules apply to all code including `#[cfg(test)]` modules and `tests/`: a test's real
dependencies are exactly what gate-truth needs (§7).

## 3. Cargo.toml dependency comments (rule ids `cargo-why`, `cargo-usedby`)

**One grammar:** the line **directly above** every dependency entry is

```toml
# §6.2.3 Typed DB Pool: sole encapsulation point is postgres.rs; …   (existing prose, kept)
# why: typed per-role PG pools and runtime queries; used-by: [adapters::postgres, adapters::remember, tests]
sqlx = { version = "0.8.6", default-features = false, features = ["postgres", "runtime-tokio", "uuid", "time", "json"] }
```

- It starts with `# why: `, has a non-empty reason, then `; used-by: [` a non-empty list `]`, all on one line.
- `used-by` entries are canonical module paths of **this package**, or `tests` (≥1 test-context file of this
  package names the crate), or `build` (build.rs). Each entry must actually name the dependency's Rust ident
  (package name with `-`→`_`, or the rename key when `package = "…"` is used) — `cargo-usedby`. The list may be
  a subset; the full computed set is in `dependency_map.md` §Cargo.
- Existing prose blocks stay; the `# why:` line is appended as their last line.
- The checker line-scans the file and cross-validates the set of dependency keys it found against a `toml`
  parse of the same file; a key only one of them sees is a `cargo-parse` violation (no grammar evasion via
  multi-line tables).

## 4. `Called-by:` — how importers are computed (rule ids `calledby-phantom`, `calledby-missing`)

`Called-by` = who **names this module in code** (static import graph), not the runtime call graph. DI edges
(an application port implemented in adapters) show up at the wiring site (the bin's bootstrap). Set-equality is
required: an entry nobody imports is `calledby-phantom`, an importer not listed is `calledby-missing`. The
violation message prints the computed list — copy it.

An edge A → B exists when A's masked code contains a path that resolves to B:
`use` trees (grouped `{…}`, `self`, globs), inline paths `humaux_x::a::f(…)`, `crate::`, `super::`, `self::`, and a
bare first segment equal to a child module declared in A (`mod b;` then `b::run(…)`). A path resolves to the
**longest** prefix that is a module file. `mod b;` declarations themselves are not edges.

| Case | Rule |
|------|------|
| Process roots | `src/main.rs` → `Called-by: [process(<package>)]`; `xtask/src/main.rs` → `[process(cargo-xtask)]`. `process(…)` is legal nowhere else. |
| Crate roots | `src/lib.rs` → `crate(<package>)` for every workspace package that depends on it in Cargo (normal/dev/build), plus in-package importers (`<short>::main`, `tests`). |
| Integration tests | `tests/x.rs` → `[cargo-test]`; `build.rs` → `[cargo-build]`. |
| `#[path = "support/x.rs"] mod x;` | an edge (it is the only way the file is used); support files list the individual test files that include them. |
| `#[cfg(test)]` and `tests/` importers of non-test code | collapse to the single entry `tests`. The `#[cfg(test)]` item (a `mod tests {…}`, a `use`, a `fn`) is test context up to its closing `;`/`}`. |
| `pub use` re-exports | a consumer that imports `humaux_x::Name` through a `pub use a::Name` (or `pub use a::*` when `a` defines `pub … Name`) is an importer of the **leaf** `a`; the `pub use` line itself is not an edge. One hop. |
| Macro modules | none exist (7 `macro_rules!`, none emits `mod`). Paths inside macro-invocation arguments are ordinary code and are scanned. The one proc-macro crate (`humaux-fail-closed-macro`) is a normal crate root: `Called-by: [crate(humaux-telemetry)]`; `compile_fail.rs` names it only inside a string (a generated crate) and is not an edge. |
| Self references | `use super::*` inside the file's own `mod tests` is not an edge. |

## 5. What else `--check` computes per file

### 5.1 Environment variables (`env-undeclared`, `env-unregistered`)
A name is **found** in a file when (a) it is the string-literal argument of `env::var(`, `env::var_os(`,
`std::env::var(`, `var_os(`, `env::set_var(`, `env::remove_var(`, `env!(`, `option_env!(`, `.env(`; or (b) any
string literal of code is exactly `HUMAUX_[A-Z0-9_]*[A-Z0-9]` (the project namespace — this catches helper
indirection such as `required("HUMAUX_…")` and `const X: &str = "HUMAUX_…"`). Found ⊆ declared, per file.
Literals inside comments, docs and larger strings (fixtures) are not names. A declared name that no literal
witnesses (built with `format!`) is allowed and shown as `declared-only` in env_vars.md.
Kind: `env!`/`option_env!` → build-time; `.env(` on a `Command` → passed to a child; everything else → runtime.

**§50/§78 typed registry.** Today the typed Config Registry (`ConfigEntry`) lives in `gateway::bootstrap`
(+ `contracts::retrieval_config`); the workers read raw variables. env_vars.md shows, per name, `registry: gateway`
or `raw`. `env-unregistered` fires when a module reachable from `process(humaux-gateway)` names a
`HUMAUX_GATEWAY_*` variable that `gateway::bootstrap`'s header does not declare (the gateway resolves its config
only through that registry). Raw reads in the workers are listed and counted, not errors (ADR-0051 D-F).

### 5.2 Database objects (`table-undeclared`, `table-write`)
Tokens `(control|coord|ops|private|projection|public|staging)\.[a-z][a-z0-9_]*` inside **string literals only**
(comments and `///`/`//!` docs ignored). A token followed by `(` is a function (must be in some `x=[…]`) **unless**
the word before it is `INTO`, `REFERENCES`, `TABLE`, `EXISTS`, `ON`, `COPY`, `UPDATE` or `ONLY` — then the `(`
opens a column list (`INSERT INTO t(cols)`, `REFERENCES t(id)`, `CREATE INDEX … ON t(col)`) and the token is a
relation (must be in some `r=[…]` or `w=[…]`). If the literal has `INSERT INTO t`, `UPDATE t`, `DELETE FROM t`,
`MERGE INTO t`, `TRUNCATE t` or `COPY t FROM`, then `t` must be in a `w=[…]`, column list or not.

The other direction holds too (`table-unwitnessed`): every `r=` entry is named by some literal, every `w=` entry is
written by one, every `x=` entry is called by one. A table the file reaches only through a callee belongs to the
callee's header.

### 5.3 Crates (`crates-undeclared`, `crates-unwitnessed`) — computed from the package's Cargo deps named in code (§1.2); set-equal.

## 6. Exemptions — the only escape hatch (`exemption-grammar`, `exemption-unused`)

There is **no allowlist or ratchet file**. An exemption is a comment that must name the rule and give a reason:

- line-scoped, on the line directly above the offending line: `// dep-map: allow <rule-id> — <reason>`
- file-scoped, as a `//!` line inside the header block (after field 5): `//! dep-map: allow <rule-id> — <reason>`
- Cargo, directly above the dependency's `# why:` line: `# dep-map: allow <rule-id> — <reason>`

The rule id must be one of §9's ids except `doc-drift`, `exemption-*` and `header-field`; the reason must be
non-empty and not `TODO`. An exemption that suppresses nothing is itself a violation. Every exemption is listed
(file:line, rule, reason) in `dependency_map.md` §Exemptions and counted in the summary line, so they are
visible, countable and reviewed. Expected users: `xtask::rls_check` / `xtask::architecture_check` (they transcribe
§6.2.2 and SQL fixtures in strings — `table-undeclared` file-scoped).

## 7. The three generated maps (`doc-drift`)

All three are written by `cargo xtask dep-map --write` and compared **byte-for-byte** by `--check` (like
`cargo fmt --check`). Deterministic: sorted (BTreeMap) everywhere, repo-relative paths, no timestamps, no host
names, no counts that depend on DB traffic. Hand edits are a `doc-drift` violation.

| File | Contents | Source |
|------|----------|--------|
| `docs/architecture/dependency_map.md` | summary line; **process → module** (modules reachable from each `main.rs` over non-test edges) with tables r/w/x, env, services, UDS peers; **reverse index** table → readers/writers/executors by process; service → modules (UDS server ↔ clients); Cargo dep → why + every module naming it; **test binaries → external deps** (PostgreSQL/Qdrant/MiniMax/DashScope/subprocess): everything the target reaches, what its **default run** reaches (the closure of its non-`#[ignore]` tests over local fns/consts/statics/macros and `#[path]` modules), and `db-bound (default run)` — gate-truth's DB-bound set; excluded fixtures; exemptions | headers + computed facts |
| `docs/architecture/db_objects.md` | every `schema.object` created by a migration: kind, owner role, RLS / FORCE, grants per runtime role (`SELECT, INSERT, UPDATE(col,…)`, `EXECUTE`), first migration that created it, modules/processes that read / write / execute it | `pg_catalog` on `HUMAUX_TEST_PG_DSN` (**read-only transaction**) + `migrations/*.sql` scan |
| `docs/architecture/env_vars.md` | every variable: kind, processes and modules that name it, witness (`literal` / `declared-only`), registry (`gateway` / `raw`) | headers + computed facts |

`db_objects.md` refuses to generate (violation `db-not-at-head`) unless `ops.schema_migrations` holds exactly the
repo's migrations; objects present in the catalog but created by no migration (test fixtures, barrier policies)
are ignored. The grants agree with Baseline §6.2.2 by construction for every table §6.2.2 names: `xtask rls-check` (G80-26,
same database, earlier in the chain) already fails on any catalog ≠ §6.2.2 cell, and this file is rendered from that
catalog. dep-map does not re-parse §6.2.2 (one checker per fact).

How to read them when something breaks: start from the symptom's table or env var in the reverse index /
env_vars.md → the processes and modules touching it → that module's header (`Invariants:` says what it does when
the dependency is down, `Spec:` says where the rule lives) → its `// dep:` tags locate the exact call.

## 8. Running it

```sh
set -a; source <live_env.sh>; set +a      # needs HUMAUX_TEST_PG_DSN (read-only use) for db_objects
cargo xtask dep-map --check               # default; exit 0 = no violation and all three docs byte-identical
cargo xtask dep-map --write               # regenerate the three docs; still prints violations, exit 1 if any
cargo xtask dep-map --suggest <file.rs>…  # print the computed parts of a header (path, crates, env, tables,
                                          # modules, Called-by) for pasting; purpose/Invariants/Spec/r-w split are yours
```

Output: one line per violation, `path:line: [rule-id] message (computed: …)`, then
`dep-map: <n> files, <m> Cargo.toml, <v> violations, <e> exemptions, docs <in sync|drift: …>, <t>s`.
Exit codes: 0 pass · 1 violations or drift · 2 usage / IO / missing `HUMAUX_TEST_PG_DSN` (a skip is not a pass,
ADR-0050). Gate: `dep_map|cargo xtask dep-map --check` (card26_extra_gates.env). Budget ≤ 30 s wall clock.

Workflow for any change: edit code → `cargo xtask dep-map --check` → fix what it names (usually: add the new import
to `modules=`/`Called-by:`, the new env/table to `Depends-on:`, a `// dep:` tag above a new call) →
`cargo xtask dep-map --write` → commit the regenerated maps with the change.

## 9. Rule ids

`header-field` · `header-path` · `header-purpose` · `header-invariants` · `spec-ref` · `service-vocab` ·
`crates-undeclared` · `crates-unwitnessed` · `modules-drift` · `calledby-phantom` · `calledby-missing` ·
`env-undeclared` · `env-unregistered` · `table-undeclared` · `table-write` · `table-unwitnessed` ·
`callsite-untagged` · `tag-grammar` · `tag-mismatch` · `service-undeclared` · `service-unwitnessed` · `cargo-why` ·
`cargo-usedby` · `cargo-parse` · `exemption-grammar` · `exemption-unused` · `db-not-at-head` · `doc-drift`
