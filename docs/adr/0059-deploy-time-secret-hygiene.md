# ADR-0059 — Deploy-time secret hygiene: NOLOGIN migration owner, owner-only migration ledger, verifier-only role rotation, deploy-check

- Status: Accepted (card 33, 2026-10-03, on HEAD `af22a38` = card 32). Implemented in four serial slices:
  **slice S1: roles, grants, deploy-check, rotate, fresh-cluster guard (D-A..D-F, migration 0201)**; slice S2:
  every real-login placeholder site and the dev environment (D-D tail, D-J); **slice S3: consistency-token MAC and
  pepper window (D-G, D-H, migrations 0202 + 0203)**; **slice S4: provider credential map, runbook, delivery report
  (D-I; the D-D and D-J sections are written with it)**.
- Spec: Baseline §6.2.0, §6.2.2, §15.5, §45, §48.2, §57.1, §73.5, §77; audit `docs/ops/system_audit_20260926.md` P1-3, SEC-2;
  design `card_33_design.md` and its main-line rulings (2026-10-02 21:05: E1, E2, E5 approved; E4 — rotating the
  shared dev cluster — is a main-line step, not part of any slice).
- Rejected memory 710a2548 (signature is not authorization) applies to D-G and is cited there.

## Context

- `migrations/0011_roles_and_grants.sql:21-52` creates eight roles `LOGIN PASSWORD '<literal>'`; the literals are
  published in the repository and every one follows `P_<role>` for one dev prefix `P`. Nothing proves a deployment
  ever replaced them.
- SEC-2: the `ops.*` domain default gave four runtime roles INSERT/UPDATE on `ops.schema_migrations` (0011:163) —
  one forged row makes `migrate` skip a hardening migration — and on the five `ops.email_*` tables.
- `migrate` never logs in as `role_migration_owner`: it creates objects as its own principal and hands them over
  with `ALTER … OWNER TO` (85 migrations); tests that act as the owner use `SET ROLE`, which needs no LOGIN.

## Decisions

### D-A  The migration owner never connects
Migration 0201 runs `ALTER ROLE role_migration_owner NOLOGIN PASSWORD NULL`; `migrate` is unchanged. The 0011
manifest postcheck (manifest only, the `.sql` is byte-identical, E1) accepts the owner NOLOGIN, because roles are
cluster-global and every fresh database on the same cluster replays 0011's postcheck. `migrate` refuses, before
applying anything, a run where 0011 is pending and one of its roles is missing, unless `--dev-placeholder-roles` is
passed (`placeholder_role_guard`): a fresh production cluster is pre-provisioned with
`humaux-maintenance roles rotate --create-missing`, so no role is ever created with a repository password.
Rejected: a per-run LOGIN window (opens a door nobody uses and races across lanes, roles being cluster-global); a
separate `humaux_migrator` LOGIN role (a tenth LOGIN role against §6.2.0, and objects it creates would miss 0011's
default privileges keyed to the original runner); keeping LOGIN with only `PASSWORD NULL` (trust/peer/cert paths
would still log in).

### D-B  rls-check role-set equality
`pg_roles(rolcanlogin ∧ ¬rolsuper)` equals the §6.2.0 rows minus `role_migration_owner`, and the owner exists
NOLOGIN. Baseline §6.2.0 (last paragraph) and §48.2 say so (E2).

### D-C  SEC-2 grants
0201 revokes every non-owner privilege on `ops.schema_migrations` and on the five `ops.email_*` tables, then grants
exactly the verbs `adapters::email::outbox` issues: role_gateway INSERT on `ops.email_outbox` and SELECT on
`ops.email_suppressions`; role_private_worker SELECT, UPDATE on `ops.email_outbox`, INSERT on
`ops.email_delivery_events`, SELECT, INSERT, UPDATE on `ops.email_suppressions`. `ops.email_domains` and
`ops.email_provider_health` have no code writer and no non-owner grant. §6.2.2 carries the six columns; rls-check's
MATRIX and `NAMED_NO_NON_OWNER_GRANTS` carry the same cells. The email cells are a module-derived interim matrix;
§74's identity matrix belongs to card 55.

### D-D  No test or tool logs in with a repository password
Every real-login site reads its password from the environment: `rls_check`'s W2 probe uses `HUMAUX_GATEWAY_PG_DSN` and
asserts `current_user = session_user = role_gateway` (a superuser DSN cannot make it vacuous); the readyz probes use
their own worker DSN; every other real-login fixture builds its DSN through `humaux_testkit::role_login_dsn`, which
takes the password from `HUMAUX_ROLE_PASSWORD_<SUFFIX>` (the names `roles rotate` reads) and fails naming the variable
when it is unset. Sites that assert `session_user` keep a real login. Rejected: `SET ROLE` / `options=-c role=X` for
those sites (it makes their `session_user` and live-LOGIN assertions vacuous); a per-file copy of the helper.

### D-E  `humaux-maintenance roles rotate`
Runs through `HUMAUX_MIGRATOR_PG_DSN` (`MigratorDbPool`: refuses every §6.2.0 role and any principal without
SUPERUSER or CREATEROLE). Targets: the eight password roles (or `--role` narrowing; `--create-missing` = the seven
LOGIN roles of 0011, the owner created NOLOGIN without a password). New values come from
`HUMAUX_ROLE_PASSWORD_<SUFFIX>` (at least 32 URL-unreserved characters, and refused naming the variable when the
value contains the 0011 placeholder prefix `P` — so it can equal no published or derived placeholder; `rotate`
therefore takes the same `--roles-sql <0011>` as deploy-check) or are generated (32 random bytes, hex).
The server only ever receives a client-side `SCRAM-SHA-256$4096:…` verifier (one builder,
`alter_role_password_sql`), so no plaintext reaches the server log, `pg_stat_statements` or an error. All changes
run in one transaction; a missing role, a LOGIN owner, or the connected principal as a target refuses the whole
rotation. Generated values are printed once, after COMMIT, as `HUMAUX_ROLE_PASSWORD_<SUFFIX>=<value>` lines before
the JSON receipt; the receipt names roles and sources only. The durable record is the receipt only,
because `control.audit_events` needs a tenant and roles are cluster-level (upgrade: a tenant-less ops audit stream).
Rejected: plaintext `ALTER ROLE … PASSWORD` with `log_statement` off (superuser-only, does nothing for
`pg_stat_statements`); per-role autocommit (a crash leaves a mixed cluster); passwords on argv.

### D-F  `humaux-maintenance deploy-check --roles-sql <0011>`
Read-only, names only, exit 0 all pass / 3 any fail or not_applicable / 1 infra. Five checks:
`placeholder_login` (the eight values parsed from 0011 at run time — never copied — plus the derived `P_<role>`
and `P`, tried against the nine `role_*` roles and the migrator user; only SQLSTATE 28P01 counts as refused, 28000
is unverified and fails), `probe_valid` (a random password must be refused 28P01, else the path does not check
passwords), `owner_nologin`, `schema_migrations_write`, `env_dsn_placeholders` (every `*_PG_DSN` / `DATABASE_URL`
password compared with the placeholders; a finding names the variable). deploy-check must run from a host whose
pg_hba path applies password authentication to every probed role; a per-service-subnet-only hba is red by design.

### D-G  Consistency-token MAC (ARCH-9), key id, gateway-process key set
Wire v1 = `hex(plain11 ‖ U+0001 ‖ kid ‖ U+0001 ‖ mac)`: `plain11` is the unchanged 11-field list, `kid` the first
8 hex of `SHA-256("humaux.consistency_token.kid.v1" ‖ 0x00 ‖ key)`, `mac` = `HMAC-SHA256(key,
"humaux.consistency_token.v1" ‖ 0x00 ‖ plain11 ‖ U+0001 ‖ kid)`. `verify_with` picks the key by kid (current or
previous), checks the MAC with `verify_slice` (constant time), and only then parses the 11 fields; the field
parser is private with one call site (architecture-check). Every refusal is `TokenMalformed`, so the gateway's
`INVALID_INPUT` / `token_malformed` mapping is unchanged. Keys: `HUMAUX_GATEWAY_TOKEN_HMAC_KEY` (hex, ≥ 32 bytes,
required, no default) and `HUMAUX_GATEWAY_TOKEN_HMAC_KEY_PREVIOUS` (optional, empty = closed; ≥ 32 bytes and ≠
current when set), boot-fatal naming the key. `retrieve::install_token_keys` fills a process-wide `OnceLock`; gateway
bootstrap is its one production caller (architecture-check), before the pool and the listener. With no key set
installed both `issue_consistency_token` and `decode_consistency_token` return `RetrieveError::TokenKeysUnset`
(issuers map it to `DEPENDENCY_UNAVAILABLE`, recall logs `token_keys_unset`); there is no unverified parse path.
Every controlled gateway spawn (rehearse.sh, e2e-onboard, the mcp_gateway process tests) gets a per-run generated key.
**Not authorization (rejected memory 710a2548, "signature is not authorization").** The MAC proves only that a
gateway holding this key issued these 11 fields unchanged; `AuthorizationScope`, `validate_scope` and RLS remain the
boundary and still run on every verified token. Tokens issued before this deploy are unsigned and are refused for at
most one token TTL. Rejected: try-current-then-previous without a kid (no observable key use); passing `&TokenKeys`
through ~25 command constructions (puts a secret into `Debug`/`Clone` structs); signing at the gateway edge (a
second constructor); deriving the key from the pepper (ties two rotations together).
`// ponytail:` one key set per process; a key change is a restart (L5).

### D-H  API-key pepper dual-verify window and epoch-gated rehash on use
`validate_api_key(record, key, current, previous, ip, now) -> Result<PepperMatch, _>` verifies against the current
pepper and, only if that fails and the window is open (`HUMAUX_GATEWAY_CREDENTIAL_PEPPER_PREVIOUS_HEX` set, ≠
current), against the previous one; `compute_api_key_hash` always uses current. On `PepperMatch::Previous` the
gateway calls `control.api_key_rehash(id, old, new)` after the request validated; the call is never fatal
(`pepper_rehash_skipped` / `pepper_rehash_failed` logged by class) and `mark_used` still runs. role_gateway can read
every `(id, key_hash)` through `api_key_lookup`, so a compare-and-set alone would be a standing cross-tenant verifier
overwrite; migration 0202 bounds it with a pepper epoch: `control.credential_pepper_state` (singleton, owner-only),
`control.api_keys.pepper_epoch` (default = the current epoch), and the door rewrites only a key behind the epoch,
moving it to the current epoch (at most once per epoch) and writing one `api_key.pepper_rehash` audit row in the key's
tenant. Only role_maintenance moves the epoch (`control.credential_pepper_epoch_advance()`, `humaux-maintenance apikey
pepper-epoch advance`, receipt-only audit like D-E). The epoch alone never closed: a key stays behind it for good
(unused during the window, or minted under the new pepper before the advance), so it stayed rewritable with no
rotation open. Migration 0204 makes the window explicit state: `credential_pepper_state.rehash_open` (default false),
opened by `advance`, closed by `control.credential_pepper_epoch_close()` (`apikey pepper-epoch close`, runbook pepper
phase 4, role_maintenance only); the door changes nothing while it is closed, whatever a key's epoch. Migration 0205
makes `advance` refuse (SQLSTATE 55000, reason `rehash_window_open`) while the window is open: before it, a second
`apikey pepper-epoch advance` (an operator retry, a replayed runbook step) moved the epoch again inside one window, so
every key already rehashed fell behind once more and became rewritable a second time. The only way to the next epoch
is `close`, then `advance`; `close` re-runs as a no-op; the CLI maps the refusal to exit 3 with that reason. Migration 0203 is the forward-fix that sets the key's tenant GUC
around the audit insert (`control.audit_events` is FORCE RLS on `humaux.tenant_id`; 0202's body failed 42501). The
rls-check gate "ADR-0059 API-key rehash boundary" pins the four EXECUTE sets ({role_gateway}, {role_maintenance} for
advance and close, none) and that the advance body refuses an open window (0205). Enumerate cursors and email codes stay keyed by the current pepper only and die at rotation (short-lived).
Rejected: `apikey rehash` on demand (the raw key is never stored, so only the request path can rehash); an
un-gated compare-and-set (standing overwrite); a raw-key proof in the database (it holds no pepper); re-issuing every
key per rotation (contradicts card scope 4).
Rejected for the close: bumping every key still behind to the current epoch at close (a bulk write over every
tenant's keys that also makes `pepper_epoch` lie about which pepper wrote the verifier).
Residual (Baseline §45, L12): between `advance` and `close` a holder of the role_gateway DB credential can rewrite each
not-yet-rehashed key's verifier once; every rewrite is audited in the victim tenant.

### D-I  Private-worker credential map (card-33 amendment of scope 5)
`HUMAUX_PRIVATE_WORKER_CREDENTIALS = <credential_ref uuid>=<ENV_NAME>[,…]` replaces card 32's single
`HUMAUX_PRIVATE_WORKER_KEY_ENV`. The map holds variable names only; each provider key stays in its own variable.
Required with no default (an unset map refuses to boot); an explicitly empty value boots with no references (main-line
amendment 2026-10-02 22:30: a node with no credentials yet must not crash-loop under supervision; every route then
parks). Boot refuses an entry without exactly one `=`, a non-UUID or nil reference, a name outside `[A-Z0-9_]+`, a
duplicate reference, a named variable that is unset or empty, and a still-set `HUMAUX_PRIVATE_WORKER_KEY_ENV`; errors
name variables, never values. Two references may share one variable. A right-hand side is echoed only when that
variable exists in the environment (set but empty: it is a variable name); an unset one may be a pasted secret that
happens to match `[A-Z0-9_]+`, so its refusal names the entry number, the credential_ref and the pattern only, as
`<unset variable>` (card 33 review P2).
`EnvCredentialMap` (the worker's only `CredentialDecryptor`) resolves a reference to exactly its own key and never
falls back to another reference's key; a miss is `WaitingKey` (defence in depth). The distill dispatcher receives the
map's reference set from the same parse (`DistillDispatchConfig.credential_refs`) and checks it in `read_leg` right
after the route is admitted: a reference not in the map is NOT_READY `CREDENTIAL_NOT_MAPPED` before any ledger row,
`ops.begin_call` or provider call exists, so the job backs off and parks `WAITING_KEY` with that class past
`not_ready_park_seconds` without spending an attempt (ADR-0058 D-H). Provider, model and endpoint still come from
the process-level descriptor; resolving them per binding is card 33b, and OpenBao replaces the env keys in card 54
(the §67.2 deviation is declared in `docs/ops/delivery_point_report.md` §6).
Rejected: the single-reference binding `…_REASONING_CREDENTIAL_REF` (replaced by the amendment; card 33b would tear it
out); a pre-check inside `DistillReasoner::admit` (a new trait method on `UserReasoningProvider` /
`CredentialDecryptor` and ~13 test impls, and the shared predicate yields no named class); relying on the
post-call `WaitingKey` (a ledger row and a `begin_call` row already exist); a default `MINIMAX_API_KEY` (a provider
name in code, §78.1).

### D-J  Dev environment: values outside every tracked file
The dev role passwords and the dev superuser password live only in `$HOME/.config/humaux/dev_role_passwords.env`
(mode 0600, on the boot volume where ownership is enforced) under the `HUMAUX_ROLE_PASSWORD_<SUFFIX>` names and
`HUMAUX_DEV_PG_SUPERUSER_PASSWORD`. The chain's env script sources it and composes every DSN under the name its reader
uses; it pulls only the two provider keys it needs from their files, each through a subshell (SEC-7). No tracked file
holds a placeholder or a rotated value (`placeholder_files`, `rotated_secret_files` gates). Rotating the shared dev
cluster is a main-line step (E4), not part of any slice. The superuser password never reaches argv either:
`deploy/compose/dev.yml` interpolates it into the container config through the compose API, and its single-container
equivalent passes `-e POSTGRES_PASSWORD` by name only (docker reads the value from its own environment), never
`-e NAME=value`, which `ps` would show.

## Consequences and limits

- L1 No dedicated migrator role; upgrade path: a §6.2.0 row with re-keyed default privileges (deploy-topology card).
- L2 One password per role: each service has a reconnect gap during rotation (OpenBao dynamic credentials, card 54).
- L3 Rotation and the pepper-epoch advance are audited by their receipts only.
- L4 Unsigned tokens outstanding at the first deploy are refused for at most one token TTL.
- L5 The token key set is a per-process `OnceLock`: one set per process, a key change is a restart.
- L6 No `apikey pepper-status` count of keys still behind the epoch; closing the window is time-based.
- L7 Consolidation and contribution RPC calls meet an unmapped credential reference only after their reservation
  (a ledger row with a WAITING_KEY outcome); upgrade: card 33b moves the check into `admitted_inference_context`.
- L8 A credential miss parks only after `not_ready_park_seconds` (the ADR-0058 D-H backoff first), not immediately.
- L9 deploy-check makes up to ~110 failed logins per run (visible in the PG log).
- L10 The secret greps scan tracked and untracked files, not the chain log.
- L11 The email grants are module-derived interim cells; card 55 owns the §74 matrix.
- L12 While the rehash window is open (advance → close), the role_gateway DB credential can rewrite each not-yet-rehashed key's verifier
  once (audited per tenant); upgrade: OpenBao-issued short-lived gateway DB credentials (card 54).
- L13 deploy-check needs a password-auth pg_hba path to every probed role.
- L14 (accepted debt) deploy-check's `schema_migrations_write` uses `has_table_privilege`, which does not see a
  column-level INSERT/UPDATE grant on `ops.schema_migrations` (rls-check's grant equality does). Upgrade path (L-next):
  add a `has_any_column_privilege` / `information_schema.column_privileges` check to deploy-check.
- L15 (accepted debt) `env_dsn_placeholders` recognises only the placeholder prefix P and the eight exact 0011 values
  as DSN passwords; a password built on P with a suffix, or an older placeholder convention, passes it. Upgrade path:
  reuse D-E's `taints` rule (contains P) for DSN passwords.
- Known S1 residual for the main line: 0201's unconditional `ALTER ROLE` updates the cluster-wide `pg_authid` row,
  so two processes applying 0201 to fresh databases on one cluster at the same instant can fail with
  "tuple concurrently updated"; the bodies-only fixture in `bins/maintenance/tests/onboarding.rs` serialises its
  applies for that reason.

## Fix pass (review findings, 2026-10-03)

| Finding | Root cause | Fix |
|---|---|---|
| `roles rotate` accepted a placeholder as the new value | `password_for` checked length and charset only | values containing `P` are refused by variable name (D-E); `rotate --roles-sql` required |
| pepper epoch never closed (0202) | rehash gated on `pepper_epoch < epoch` only | 0204: `rehash_open` window, `credential_pepper_epoch_close()`, `apikey pepper-epoch close` (D-H) |
| rehash tests ratcheted the shared epoch, in-process mutex only | T21/T32 called `advance` on the shared DB and never closed | cross-process advisory lock; T32 in one rolled-back transaction; T21 closes the window on drop |
| kid not proven MAC-covered | no test computed the wire MAC independently | wire-v1 known-answer test |
| metrics-registry D3 `token_keys` | `TOKEN_KEYS.set(` reads as a gauge emit under §80.2's convention | the `OnceLock` is filled through `get_or_init` |
| rehearsal `derived_jobs_not_done` red (`claimed=8 not_ready=8`) | test fixtures left PENDING DERIVED_* jobs: one cleanup batch ending in the tenant delete was rolled back whole by a still-referencing row (selection_snapshot, run_once_e2e, binding_predicate_tests), no cleanup at all (retrieval_query_sources), or a `replica`-mode delete skipped the tenant cascade (continuity 0137) | card-31 pattern in every fixture that seeds an EVIDENCE_ACCEPTED row or a PRIMARY link: `ops.jobs` first, failure printed; tenant row in a separate best-effort batch |
| fault 12(a) not covered | both guard tests exercised `placeholder_role_guard` / `check_placeholder_roles` directly, never through `apply_all` | wiring test through `apply_all` on a throwaway DB |
| credential map echoed an unset right-hand side | the refusal formatted the name whether or not it was a variable | echo only an existing (empty) variable; unset = `<unset variable>` + credential_ref (D-I) |
| `apikey pepper-epoch advance` re-opened an open window | 0204's advance had no open-window guard | 0205: 55000 `rehash_window_open` while open (D-H) |

Tests and faults (red runs on `c32_fault_scratch` for SQL faults, never the shared dev DB):

| Test | Fault | Result |
|---|---|---|
| `roles::tests::env_value_built_on_a_placeholder_is_refused` (maintenance unit) | skip the `taints` refusal in `password_for` | red (1 failed), green restored |
| `api_key_rehash_is_epoch_gated_once_and_audited` (T32, gateway `service_credentials`) | 0204 `api_key_rehash` without the `rehash_open` check (scratch DB) | red at the closed-window assertion, green restored |
| `retrieve::contract_tests::token_mac_covers_label_fields_and_kid` (adapters unit) | `token_mac` drops `mac.update(kid)` | red (1 failed), green restored |
| rls-check "ADR-0059 API-key rehash boundary" (T22) | — (gate extended to `credential_pepper_epoch_close()` = {role_maintenance}) | pass on dev |
| `apikey_pepper_epoch_advance_refuses_while_the_window_is_open` (maintenance `onboarding`, own throwaway DB) | migration 0205 absent (0204 advance body) | red (second advance exit 0, expected 3), green restored |
| `rls_check::tests::advance_body_probe_tells_0204_from_0205` (xtask unit) | the advance-body probe accepts any body | red (1 failed), green restored |
| `migrate::tests::migrate_refuses_a_pending_roles_migration_whose_role_is_missing` (xtask, throwaway DB; fault 12(a)) | `check_placeholder_roles` drops its `placeholder_role_guard` call (migrate.rs:321); the two older guard tests stay green | red (1 failed), green restored |
| `tests::credential_map_unset_rhs_is_never_echoed` (private-worker unit) | an unset right-hand side echoed verbatim (pre-fix message) | red (2 failed, with `credential_map_boot_refusals`), green restored |
| gate `no_leaked_distill_jobs` (`c31_leak_check.sh`) after the adapters, private-worker and consolidation-worker suites | the pre-fix fixtures (selection_snapshot, retrieval_query_sources, binding_predicate_tests, continuity 0137 cleanup, run_once_e2e) | red (1+ PENDING DERIVED_CONSOLIDATE on a throwaway tenant, 10 tenant-less PENDING DERIVED_DISTILL), green after the fix |
| gate `fixture_jobs_cleanup` (every fixed fixture deletes `ops.jobs`) | the pre-fix `selection_snapshot.rs` | red (EXIT 1), green |
| gate `dev_compose_no_argv_password` | the pre-fix `deploy/compose/dev.yml` (`-e POSTGRES_PASSWORD=<value>`) | red (EXIT 1), green |
| gates `runbook_rotate_rollback`, `adr59_accepted_debt` | the runbook without the 10.2/10.3 rollback paragraphs; the ADR without L14/L15 | red (EXIT 1), green |

