# Deploy runbook — first deployment and every one after

> Scope: the **ordered** steps to stand this system up and prove it is up. It does not repeat
> what already has a home: process/probe semantics are `docs/ops/supervision.md`, load sizing is
> `docs/ops/soak.md`, tenant provisioning detail is `docs/ops/e2e-seed.md`, and the evidence
> behind the delivery claim is `docs/ops/delivery_point_report.md`. Each step below says what
> must be true before the next one runs.

## 0. Before anything — the four OS users

`humaux-gateway`, `humaux-retrieval-worker`, `humaux-consolidation-worker` and
`humaux-private-worker` must run as **four distinct OS users**. UDS peer-credential checking is
by uid (ADR-0012/0014): running two of them as the same user does not fail, it silently degrades
the check to nothing. This is a deployment property no test on one machine can catch, so it is
step 0, not a footnote.

`humaux-public-worker` is a scheduled one-shot and may share a user with none of the above.

## 1. Migrations

```sh
cargo xtask migrate
```

Exit 0 with drift 0. §46's drift gate refuses any already-applied migration whose file was
edited — if it fires, the fix is a NEW migration, never an edit to the old one. Every migration
ships as a pair `NNNN_name.sql` + `NNNN_name.manifest.toml`; a missing manifest is a hard
refusal, not a warning.

Since card 25 (ADR-0050 D-D/D-F), migrate also enforces the following:

- **The checks run.** Each *pending* migration runs in one transaction: manifest `precheck` →
  the `.sql` → manifest `postcheck` → the `ops.schema_migrations` row → COMMIT. Each check must
  return one row with one `bool` column whose value is `true`. The log prints one line per check
  (`migrate: precheck <id> ok`). A false, non-boolean or invalid check refuses the migration with
  the check's name and the server's text, and the transaction rolls back, leaving drift 0 (no
  object and no record). Already-applied migrations are not re-checked.
- **One migrator at a time.** Migrate holds the advisory lock `HXMIGRAT`
  (`0x4858_4D49_4752_4154`) for the whole run and sets `lock_timeout = 30s`. A second migrate
  waits up to 30 s and then refuses with `55P03`, having applied 0.
- **Stop the workers before migrating.** The same 30 s `lock_timeout` bounds every migration's
  DDL. A migration that queues behind a live worker's lock is refused, leaving drift 0, rather
  than stalling traffic behind an `ACCESS EXCLUSIVE` request. Stop the workers, migrate, then
  start them in §5 order.

### 1.1 The §48.1 partition conversions (0224–0230, card 36, ADR-0063 D-D / D-M)

Migrations 0224–0230 turn `ops.stage_runs`, `private.messages`, `ops.maintenance_receipts`, `private.events`,
`control.audit_events` and `ops.model_call_ledger` into monthly RANGE parents under their own names (the heap
becomes one leaf, the current month plus three future months are pre-created, no DEFAULT partition). All seven are
`FORWARD_ONLY` with `backup_restore_requirement = "pg_dump before apply"`: there is no down migration. Each holds
ACCESS EXCLUSIVE on its table and SHARE ROW EXCLUSIVE on every FK neighbour for its whole transaction, so **every
writer is stopped first** (milliseconds on an empty production database; measured on a dev copy in ADR-0063).
A conversion refuses `c36 precheck: <fk>: <n> orphan rows - repair data first` when rows violate an FK it re-creates.

On a database that already holds data (the shared dev database), the order is fixed and run by the main line only:

1. stop every writer; `SELECT count(*) FROM pg_stat_activity WHERE datname = '<db>' AND usename <> 'postgres'` = 0;
2. `c36_devcopy.sh` (task-work directory): a fresh `pg_dump -Fc` into `db_backups_<YYYYMMDD>_c36/`, checked with
   `pg_restore -l` and sha256, restored into two throwaways, one migrated; counts compared, `rls-check`, latency;
   it must end `C36 DEVCOPY COUNTS EQUAL` with every migration under 30 s;
3. `MAINLINE_GO=1 c36_dev_convert.sh`: refuses unless step 2 is EQUAL, the dump's sha256 still matches and dev
   has not changed since the dump; migrates, compares counts and ends `C36 DEV COUNTS EQUAL`;
4. restart the services in §5 order.

**Rollback** (no down migration): restore the step-2 dump into a fresh database
(`createdb <db>_restore && pg_restore -d <db>_restore <dump>`), stop every service, swap the names
(`ALTER DATABASE <db> RENAME TO <db>_c36_failed; ALTER DATABASE <db>_restore RENAME TO <db>`), start the services.
The restored database passes `cargo xtask rls-check` (partition arm included) and runs retention **without any
rebind step**: since 0231 a leaf is identified by its name and bounds, verified against the catalog, never by a
stored OID (a logical restore renumbers every relation; ADR-0063 "Registry by name", proven by
`partitions.rs::registry_survives_a_logical_dump_and_restore`). A dump taken before 0231 restores the same way: 0231
re-checks every registry row by name and bounds when the restored copy is migrated, and clears the old OIDs.

## 2. Roles and grants

ADR-0059. The eight password roles — the seven LOGIN roles of `migrations/0011_roles_and_grants.sql`
(gateway, private_worker, consolidation_worker, public_worker, retrieval_worker, batch_issuer,
maintenance) and `role_admin` — get their passwords **only** from
`humaux-maintenance roles rotate`. The passwords written in 0011 are development placeholders,
published in the repository, and must never survive a deploy. `role_migration_owner` is NOLOGIN
with no password (D-A, migration 0201): nothing logs in as it; `migrate`'s own principal acts on
its behalf (superuser, or CREATEROLE with ADMIN OPTION on the eight roles plus membership in
`role_migration_owner`). Before any traffic, **both** must exit 0:

- `cargo xtask rls-check` — walks the §6.2.2 matrix row for row; a new table or grant that is not
  in both the matrix and `xtask/src/rls_check.rs::MATRIX` reds it; the role set must be the
  §6.2.0 rows with the owner NOLOGIN.
- `humaux-maintenance deploy-check --roles-sql migrations/0011_roles_and_grants.sql` — read-only,
  prints names only, exit 0 all pass / 3 any check fails or is `not_applicable` / 1 infrastructure.
  Its five checks and what each red means are in `docs/ops/supervision.md` §2. It must run from a
  host whose pg_hba path applies password authentication (`scram-sha-256`) to every probed role:
  SQLSTATE 28000 means *unverified* and is red, by design.

**First deploy on a fresh cluster** (D-A; `migrate` refuses step 2 with
`missing object: role <name>` when step 1 was skipped):

1. `humaux-maintenance roles rotate --create-missing --roles-sql migrations/0011_roles_and_grants.sql
   --actor … --reason … --ticket … --step-up-auth …` with `HUMAUX_MIGRATOR_PG_DSN` set. Creates the seven 0011 LOGIN roles with a client-side SCRAM
   verifier and the owner NOLOGIN. Generated values print once, as `HUMAUX_ROLE_PASSWORD_<SUFFIX>=…`
   lines before the JSON receipt; pipe `grep '^HUMAUX_ROLE_PASSWORD_'` straight into the secrets
   store.
2. `cargo xtask migrate` (§1).
3. `humaux-maintenance roles rotate --roles-sql migrations/0011_roles_and_grants.sql --role role_admin …` (0110 creates `role_admin` without a
   password).
4. `humaux-maintenance deploy-check --roles-sql migrations/0011_roles_and_grants.sql` → exit 0.

An interrupted migrate after step 1 leaves no placeholder login, because none was ever created. A
brand-new **development** container may instead pass `migrate --dev-placeholder-roles` once.

**Migration 0210 and a non-superuser migrator** (ADR-0061 review-fix 3, F2). 0210 makes the NOLOGIN
`role_health_reader` the owner of `ops.health_snapshot(timestamptz)` and `ops.admin_probe_snapshot()` with
`ALTER FUNCTION … OWNER TO`. A superuser executor needs nothing more (the dev cluster's migrator is the superuser).
Any other executor needs, for the duration of 0210 only: membership in `role_health_reader` with the SET option
(`GRANT role_health_reader TO <migrator> WITH INHERIT FALSE, SET TRUE`) and `GRANT CREATE ON SCHEMA ops TO
role_health_reader`. Revoke both right after 0210 (`REVOKE role_health_reader FROM <migrator>`, `REVOKE CREATE ON
SCHEMA ops FROM role_health_reader`), then run `cargo xtask rls-check`: it is red while the reader has a member or
the extra grant.

## 3. Tenant provisioning

Card 28 / ADR-0053: onboarding is `humaux-maintenance` (the §4.2 operator-write process),
**never raw SQL and never `xtask e2e-seed`** (the seed refuses any host but `127.0.0.1`). Every
subcommand is one-shot and idempotent, prints ONE JSON receipt on stdout, and exits `0`
created/existing, `3` refused (a named `reason`, nothing written), `2` usage, `1` infrastructure.
Environment (no defaults): `HUMAUX_MAINTENANCE_PG_DSN` (role_maintenance),
`HUMAUX_MAINTENANCE_CREDENTIAL_PEPPER_HEX` (= the gateway's `HUMAUX_GATEWAY_CREDENTIAL_PEPPER_HEX`),
`HUMAUX_MAINTENANCE_QDRANT_{HOST,PORT,CIDR}`, `HUMAUX_MAINTENANCE_EMBEDDING_DIMENSION`,
`HUMAUX_MAINTENANCE_PRIVATE_MEMORY_COLLECTION`. Every writing subcommand needs the §77 fields
`--actor --reason --ticket --step-up-auth` (`--trace-id` optional).

Once per deployment and embedding provider — the GLOBAL and REGION §0117 tiers (onboarding
refuses with `deployment_admission_missing` until they exist):

```sh
humaux-maintenance deploy-init --provider dashscope --region <region> --tpm <n> --rpm <n> \
  --actor <who> --reason <why> --ticket <ref> --step-up-auth <evidence>
```

Per tenant — one command does it all:

```sh
humaux-maintenance onboard tenant --name <unique name> --owner-email <email> \
  --plan-limit <n> --period-end <RFC 3339> --scopes memory:write,context:read --key-name <name> \
  --provider dashscope --region <region> --tenant-tpm <n> --tenant-rpm <n> [--workspace <name>] \
  --actor <who> --reason <why> --ticket <ref> --step-up-auth <evidence>
```

It writes, in ONE transaction, the owner user (email stored unverified, §74 is post-go-live),
the tenant (`onboarding_name` = `--name`, the idempotency key), the OWNER membership, the
reasoning domain, the entitlement snapshot, the TENANT + two PURPOSE tiers, the workspace in
`PROVISIONING` with its OWNER workspace membership and zeroed checkpoint rows, the placement row
and — only when the tenant was created — the API key; then issues the quota window, ensures the
Qdrant collection with **both** payload indexes (`tenant_id`, `subject_ids`), and performs the
VerifiedEmpty first activation (§6), which flips the workspace to `READY`. The receipt lists
`tenant_id`, `workspace_id`, `owner_user_id`, `reasoning_domain_id`, `api_key.fingerprint` (prefix +
first four hash bytes), `admission_tiers` (GLOBAL, REGION, TENANT, PURPOSE ×2), `placements`,
`collection` (+ `payload_indexes`, `generation`), `activations[]` (`evidence: "VerifiedEmpty"`,
probe latency) and `lifecycle`. **The API key is printed once**, as the line
`Authorization: Bearer <prefix>.<secret>` before the JSON, only when it was created. A re-run with
the same `--name` writes nothing, prints `"outcome":"existing"` and no key: a lost key is revoked
(`apikey revoke --tenant <id> --key-name <name>`) and a new one issued
(`apikey issue --tenant --user --workspace --scopes --key-name <new name>`).

More workspaces and users: `onboard workspace --tenant <id> --name <n> --owner <user id>`,
`onboard user --tenant <id> --email <e> --role OWNER|ADMIN|MEMBER [--workspace <id>]`. Repair
steps if a run was interrupted: `placement ensure --tenant <id>`, `collection ensure`,
`activate --tenant <id> --workspace <id>`; `status --tenant <id>` shows workspaces + lifecycle,
families (serving, highwaters), activation receipts, placement, key prefixes and tiers.

A collection created before card 24 lacks the `subject_ids` index; `collection ensure` PUTs both
indexes idempotently, so running it once fixes such a collection.

### 3.1 Reasoning routes — which LLM a domain uses (ADR-0060 D-H)

A freshly onboarded tenant's reasoning domain has **no route**: its distill and consolidation jobs park
`NO_DISTILL_BINDING` until an operator binds it. Every step is a `humaux-maintenance reasoning …`
subcommand (same exit codes, §77 flags and one-JSON-receipt rule as onboarding; a refusal names its
`reason` and writes only its DENIED audit row). Never raw SQL, never `e2e-seed`. `<A>` below means
`--actor <who> --reason <why> --ticket <ref> --step-up-auth <evidence>`.

**Register** one Profile@version (provider, model, endpoint, capabilities, vendor request fields, vendor
account, credential reference):

```sh
humaux-maintenance reasoning register --tenant <id> --owner-user <domain owner user id> \
  --provider-id <processor id> --provider-model-id <model> [--model-revision <catalog label>] \
  --capabilities TEXT,STRUCTURED_OUTPUT[,TOOL_CALLS|JSON_OBJECT…] --request-extras '<json object>' \
  --account-ref <vendor account reference text> --endpoint-ref https://<host>/<path> \
  --region <region> --service-tier <tier> --egress-processor-id <recipient uuid of this vendor/region> \
  [--credential-ref <existing ref bound to this account>] [--successor-of <profile id>] <A>
```

- `--request-extras` is required (`'{}'` for none): vendor fields such as a thinking switch are profile
  data; an adapter-owned key (`model`, `messages`, `tools`, `response_format`, `max_tokens`, …) is refused
  `request_extras_invalid`.
- The catalog row is append-only: declaring more capabilities than an existing (provider, model,
  revision) row is refused `capabilities_exceed_catalog` — use a new `--model-revision` label
  (convention `caps-<sorted capabilities joined by .>`; never sent on the wire).
- `--account-ref` is hashed (sha256) before it leaves the CLI. Profiles whose references will share one
  key variable **must** name the same account reference, or the worker refuses to boot (ADR-0060 D-J).
- The receipt prints `credential_map_entry: "<credential_ref>=<ENV_NAME>"`: add it to
  `HUMAUX_PRIVATE_WORKER_CREDENTIALS` with the variable that holds the key (§10.4). The key never passes
  through the CLI.
- A re-run with the same values answers `existing`.

**Bind** a domain to it (both derived purposes are bound separately):

```sh
humaux-maintenance reasoning bind --tenant <id> --domain <reasoning domain id> \
  --purpose PRIVATE_DISTILL_TEXT|PRIVATE_CONSOLIDATE --profile <profile id> --profile-version <n> <A>
```

One PINNED policy, exactly one candidate, one current binding. Refused: `purpose_not_bindable`
(contribution keeps its own bootstrap), `owner_mismatch` (the profile's owner must own the domain),
`profile_disabled`, `profile_lacks_structured_output`.

**Attest health** — a bound route is admitted only with valid provider and account observations:

```sh
humaux-maintenance reasoning attest-health --tenant <id> --profile <id> --profile-version <n> \
  --valid-for-secs <n> <A>
```

`--valid-for-secs` has no default. Traffic renews a route on its own (`HUMAUX_PRIVATE_WORKER_HEALTH_RENEW_SECS`,
§10.4), so the value only has to cover the time until the first successful call and any quiet period you
accept; the rehearsal uses the run length plus 90 minutes. A route idle for longer parks
`ROUTE_HEALTH_STALE` (no spend) until re-attested.

**Status** — `humaux-maintenance reasoning status --tenant <id>` lists every ACTIVE domain × purpose with
its binding, profile, provider/model, recipient, credential reference, the latest verdicts, `valid_until`
and `health`: `UNBOUND`, `MISSING`, `STALE`, `DENIED` or `ADMISSIBLE`.

**Switch model** (no restart, history kept): `register --successor-of <profile id>` with the new model /
revision / endpoint / capabilities, then `bind` the new Profile@version (the current binding is closed and
a successor Binding@version takes over), then `attest-health`. Old rows stay; every earlier ledger row
and processing run still names the route it ran under. If the new profile has a new credential
reference, add its map entry and restart the worker **before** `bind` (L15).

**Disable** one route at once: `reasoning profile-state --tenant <id> --profile <id> --profile-version <n>
--enabled false <A>`; the next admission refuses it (`reasoning route not admitted`), nothing is sent.
`--enabled true` re-enables it.

**Add a provider** (any OpenAI-compatible chat endpoint): mint one recipient uuid for that vendor/region
(reused by every tenant's endpoint for it), add `<uuid>=<host>[|<host>]` to
`HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS` and its region to `HUMAUX_PRIVATE_WORKER_REGIONS` (and a DNS pin
if this node's resolver is not trusted for the host, §10.5), export its key under a variable name,
`register` with `--egress-processor-id <uuid>`, add the map entry, restart the worker, `bind`,
`attest-health`. Two profiles on one vendor account share that account's rate limit at the provider
(research amendment 6).

What to do when a job line or the status says:
- `NO_DISTILL_BINDING` / `UNBOUND` — the domain has no route: `register` (if needed) and `bind`.
- `CREDENTIAL_NOT_MAPPED` — the route's credential reference is not in the worker's map: add the
  register receipt's `credential_map_entry` with the right variable and restart the worker.
- `ROUTE_HEALTH_STALE` / `STALE` / `MISSING` — no valid observation: check the provider, then
  `attest-health`. `ROUTE_HEALTH_DENIED` / `DENIED` after a 401: rotate the key (§10.4), then
  `attest-health`.
- `EGRESS_PROCESSOR_NOT_ALLOWED` / `EGRESS_HOST_NOT_ALLOWED` / `REGION_NOT_ALLOWED` — the deployment's
  lists do not cover the route: fix the lists (never the route) and restart.
- `ENDPOINT_REJECTED` — the route's endpoint failed the §11.4 check before any request: it is not a
  bare `https` host, or the host resolves into a forbidden range. On a node whose resolver answers the
  host with a fake IP (198.18.0.0/15 and similar), add a pin for it to `HUMAUX_PRIVATE_WORKER_DNS_PINS`
  (§10.5) and restart; a wrong endpoint is fixed with a successor profile (**Switch model**), never by
  weakening the check. Nothing was sent.
- `TRANSPORT` when the job parks — the worker could not build the egress client for the route (TLS /
  client setup on this node); nothing was sent. Fix the node and restart. A `TRANSPORT` failure of a
  call that was sent is an outage (next bullet).
- `ROUTE_PROVIDER_MISMATCH` — the instance the worker holds is not the one the admitted route names (a
  seam or cache defect, never a configuration choice). Restart the worker (instances are rebuilt from
  the routes), keep the job line, and file a defect; do not edit the route to match.
- `PROFILE_LACKS_STRUCTURED_OUTPUT` — the bound Profile@version does not declare `STRUCTURED_OUTPUT`:
  `register --successor-of` with the capability (it must be in the catalog row), then `bind` and
  `attest-health`.
- A provider outage is **not** parked: jobs retry with backoff and go DEAD at `max_attempts` (ADR-0060
  D-G). Once it is back: `jobs requeue-dead --tenant <id> --error-class <class>`.

## 4. Environment

Per-process env is listed in `docs/ops/supervision.md` and the deployment's own secrets manager.
Two rules that are not obvious from the variable names:

- **Secrets go in env, never in a file that is read into a log.** The repo's own live env file
  sources the two key files by path precisely so the values never appear in it.
- **If the node's resolver lies about a provider host, pin it.** This dev box fake-IPs
  `api.minimaxi.com` into `198.18.0.0/15`; §11.4 forbids that range and the checked resolver
  correctly refuses the connection. The refusal is the resolver working, not a defect
  (ADR-0039). Pin the host, and run proxy-free — a proxy makes the client CONNECT by hostname
  and the resolver is never consulted at all.
- **The gateway has no default write pair** (card 29 / ADR-0054).
  `HUMAUX_GATEWAY_REMEMBER_TENANT_ID` / `HUMAUX_GATEWAY_REMEMBER_WORKSPACE_ID` are not required and
  are ignored (a present value must still be a UUID; the gateway prints one
  `ignored since ADR-0054` line). They remain accepted only because `xtask e2e-onboard` still
  passes them. Every write — `remember.put` and the 14 governance / subject / affect ops
  (supersede, restore, correct, confirm, reject, archive, unarchive, pin, unpin, bind, unbind,
  subject_register, subject_link_key, annotate_affect) — lands on the caller's own
  (tenant, workspace): the credential's tenant plus the requested or bound workspace, provisioned
  pairs only. A confirm token is bound to the workspace it was minted in; presented in another
  workspace it answers `CONFLICT` and stays usable where it was minted.
- **The gateway holds no secret scanner** (card 30b / ADR-0056). `HUMAUX_GATEWAY_GITLEAKS_BIN`,
  `HUMAUX_GATEWAY_GITLEAKS_VERSION` and `HUMAUX_GATEWAY_GITLEAKS_SHA256` are removed. The gateway
  refuses unknown `HUMAUX_GATEWAY_*` keys, so a deployment env (`.env`, compose/helm values, an
  operator shell) that still sets any of the three fails gateway boot until the lines are
  deleted. The retrieval worker's `HUMAUX_RETRIEVAL_WORKER_GITLEAKS_*` stay: its `seal_query` /
  `seal_card` is the one egress seal. A query carrying a gitleaks finding still answers
  `FORBIDDEN` (the worker returns `SCAN_REJECTED`; operator line `query_scan_rejected`); a scanner
  that cannot run is `SCANNER_UNAVAILABLE` → `DEPENDENCY_UNAVAILABLE`.
- **The pinned gitleaks binary is root-owned, mode 0555, on a read-only path.** Since ADR-0056 D-D
  the scanner hashes it once at start and re-hashes only when its `(dev, ino, len, mtime, ctime)`
  changes, so a rewrite that preserves all five fields (root, a shared `mmap` write, or a
  coarse-timestamp filesystem) is no longer caught per scan. File ownership is the control; the
  stat check detects tampering, it does not prevent it.
- **`HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS` is required, no default** (card 31 / ADR-0057 D-F). A
  gateway without it, or with `0`, fails boot naming the key. It is the age (seconds, DB clock) of
  the oldest `ISSUED` / `PROCESSING` / `RETRY_WAIT` projection ticket of the stream a read touches
  beyond which recall / context / memory reads answer `cannot_establish` with reason
  `projection_lag` and the degradation `PROJECTION_LAG`. `WAITING_KEY` does not count; a ticket in
  retry backoff does. Set it above the normal put → `DONE` time of the deployment (distill is
  usually the long hop, §8) and below `HUMAUX_MAINTENANCE_SERVE_LOST_AFTER_SECONDS` (the LOST sweep of
  "Scheduled maintenance" below; the daemon refuses to boot unless LOST_AFTER is greater), or stuck tickets
  would turn `LOST` before they ever read as lag. The rehearsal uses 20.
- **Qdrant server ≥ 1.19.0** (ADR-0057 D-I). The projection runner fences every point upsert with
  Qdrant's `update_filter` and every delete with a `source_stream_seq` filter, so a reclaimed
  runner's late write cannot undo a later ticket's. 1.19.0 is the version this behaviour was
  measured on; an older server may ignore `update_filter` and silently drop the fence. Card 39
  pins the image.
- **Private retrieval egress runs gitleaks only** (ADR-0056 D-A). E-mail addresses, phone numbers,
  dates and long numbers in private memories and recall queries are sent to the retrieval provider
  (the disclosed ExternalProcessor, recorded in `ops.data_disclosures`). The e-mail/phone rules
  still apply to the §12 contribution path.

## 5. Start order

1. `humaux-private-worker --serve-rpc` and `--distill-serve`
2. `humaux-retrieval-worker --serve-rpc`
3. `humaux-retrieval-worker --serve` — the fifth resident process, card 27 / ADR-0052: ONE
   tenant-free projection runner for every placed tenant (same OS user as `--serve-rpc`). Its
   keys beyond the `--serve-rpc` set (DSN, embedding descriptor, Qdrant cell, gitleaks pin,
   egress processor id) are the seven pass keys, all required, no defaults:
   `HUMAUX_RETRIEVAL_WORKER_BATCH`, `_POLL_INTERVAL_SECS`, `_LEASE_SECS`, `_PER_TENANT_CAP`,
   `_MAX_ATTEMPTS`, `_BACKOFF_BASE_SECS`, `_BACKOFF_MAX_SECS` (meaning and readers:
   `docs/architecture/env_vars.md`). There is no tenant, workspace or collection key: the claim
   hands each ticket its tenant's `projection.tenant_placements` row, and a tenant without a
   placement is never claimed (the runner logs `placement_missing tenants=<n> tickets=<m>`).
   Readiness: the process is alive AND `--readyz` exits 0 (`docs/ops/supervision.md` §4 rule 3).
4. `humaux-consolidation-worker --serve`
5. `humaux-maintenance health serve` (ADR-0061 D-D), then `humaux-maintenance --serve` (card 35, ADR-0062 D-A) —
   after migrations 0214-0222 and `health serve`, before traffic. PostgreSQL is its only dependency; it is ready
   when `GET /metrics` on `HUMAUX_MAINTENANCE_SERVE_METRICS_ADDR` answers 200 (one clean cycle). What it runs:
   "Scheduled maintenance" below.
6. `humaux-gateway`

Before card 27 nothing in this runbook drove projection at all: tickets stayed `ISSUED` and recall
never saw a new memory (audit P1-1). One line per pass shows the runner working:
`projection pass claimed= done= skipped= failed= retried= refunded= pending= lost_lease= placement_missing= placement_invalid= claim_ms=`.
`refunded` counts retries that spent no attempt because the dependency (Qdrant, the embedding
provider incl. its quota/budget, the scanner, PostgreSQL) was already known down — an outage
parks tickets, it does not exhaust them. `placement_invalid` counts tickets parked because their
tenant's placement row carries a value this build does not know (deploy skew): fix the build or
the row, and they are claimed again after the backoff.

The order is not a preference: the consolidation worker's `--readyz` probes the private worker's
UDS peer, and the gateway's semantic recall needs the retrieval worker's socket. Starting a
consumer before its socket exists produces a readiness failure that names the missing socket
(`docs/ops/supervision.md` §2) — correct behaviour, but an avoidable page.

## 5.1 Scheduled maintenance (`humaux-maintenance --serve`, card 35, ADR-0062)

One resident unit, one instance, one env file shared with the gateway and the private worker (it reads
`HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS` and `HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS` under their own
names; two env files could disagree — ADR-0062 L4). Every `HUMAUX_MAINTENANCE_SERVE_CYCLE_SECONDS` it runs each
**due** task (its `<T>_EVERY_SECONDS` has passed, ≥ CYCLE) over one page of `HUMAUX_MAINTENANCE_SERVE_TENANTS_PER_RUN`
tenants (`control.maintenance_tenant_page`, ids only), one transaction and **one statement** per tenant with LIMIT
`<T>_LIMIT`. Every key below is required with no default (prefix `HUMAUX_MAINTENANCE_SERVE_`, full list and readers
in `docs/architecture/env_vars.md`); every age is sent as an interval and compared with the **DB clock** inside the
door. A full rotation takes `ceil(tenants / TENANTS_PER_RUN) × EVERY` (ADR-0062 L1).

| task (`task` label) | door (one statement per tenant) | cadence / limit keys | age key | never touches | evidence |
|---|---|---|---|---|---|
| `lost` | `stream_repo::sweep_lost`: ISSUED → LOST, oldest first | `LOST_EVERY_SECONDS`, `LOST_LIMIT` | `LOST_AFTER_SECONDS` (> `HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS`) | a ticket under a live runner lease, a ticket in retry backoff, a ticket with an in-flight job, a ticket whose Evidence is still being distilled (`EVIDENCE_ACCEPTED` row PENDING / PROCESSING) | the row itself: `state = LOST`, `error_class = ORPHANED_PIPELINE_ITEM`, `lost_at` (the reissue cool-down counts from it) |
| `quota_reservations` | `control.reap_quota_reservations` (0113) | `QUOTA_RESERVATIONS_*` | — (reservation TTL) | live reservations | the reaped reservations' state |
| `provider_budgets` | `ops.reap_expired_retrieval_provider_budget` (0117) | `PROVIDER_BUDGETS_*` | — (reservation TTL) | live reservations | the reaped reservations' state |
| `confirm_tokens` | `control.sweep_confirm_tokens(interval, integer)` (0218) | `CONFIRM_TOKENS_*` | `CONFIRM_TOKENS_CONSUMED_RETENTION_SECONDS` | an unexpired token; a consumed token younger than the retention (§9 audit) | `ops.maintenance_receipts` task `confirm_tokens` |
| `snapshots` | `ops.purge_expired_selection_snapshots(integer)` (0218): snapshot + items in one statement | `SNAPSHOTS_*` | — (DB `expires_at`) | an unexpired snapshot; a page read already running (one-statement read, ADR-0062 D-H) | receipts task `selection_snapshots` (counted in snapshots) |
| `rate_buckets` | `control.purge_idle_rate_buckets(interval, integer)` (0218) | `RATE_BUCKETS_*` | `RATE_BUCKETS_IDLE_SECONDS` | a bucket that would not be full now; a bucket a consumer holds (same advisory key) | receipts task `rate_buckets` |
| `jobs` | `ops.purge_terminal_jobs(interval, interval, interval, integer)` (0218) | `JOBS_*` | `JOBS_DONE_RETENTION_SECONDS`, `JOBS_DEAD_RETENTION_SECONDS`, budget window = `HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS` | a job with a provider call inside the budget window; a contribution-linked job; a DEAD distill job `jobs requeue-dead` could still re-arm (its Evidence row is not DONE); every outbox row (no outbox door, ruling E11) | receipts task `terminal_jobs` |
| `reissue` | `projection.reissue_unsettled_tickets(uuid, interval, integer)` (0220; cool-down clock incl. `lost_at`, 0223) | `REISSUE_*` | `REISSUE_COOLDOWN_SECONDS` (> 0) | a tombstoned or non-indexable memory; a memory with a ticket in flight; a deterministic failure already reissued once | `projection.ticket_reissues` (one row per fresh ticket) |
| `redrive` | `ops.auto_redrive_schema_failed(uuid, interval, integer)` (0221) + one §77 row | `REDRIVE_*` | `REDRIVE_COOLDOWN_SECONDS` (> 0) | any class but `FAILED_OUTPUT_SCHEMA`; a job already re-driven once (`auto_redrives = 1`) | `control.audit_events` action `DISTILL_AUTO_REDRIVE` |
| `partitions` (cluster-level, once per run, ADR-0063 D-K) | one `UPDATE control.partition_registry` of the two proposal columns (expired leaves of each table's latest effective policy, never the newest leaf) + one horizon read | `PARTITIONS_EVERY_SECONDS` (no LIMIT, no tenant page) | the policy's `retention_months` (`control.retention_policies`) | every DDL, every DROP, every hold (holds are re-derived by the executor only) | `proposed_at` / `proposed_policy_revision` on the registry row; `partition_horizon_months{table}` |
| `dr_evidence` (cluster-level, once per run, card 37, ADR-0064 D-K) | one statement over `ops.backup_receipts`, `ops.restore_drills` and `pg_stat_archiver` (inserting one `ops.wal_archive_failures` row per archive-failure incident), then two `df -Pk` children on `HUMAUX_MAINTENANCE_DR_REPO_FS_PATH` / `HUMAUX_MAINTENANCE_DR_PGDATA_FS_PATH` | `DR_EVIDENCE_EVERY_SECONDS` (no LIMIT, no tenant page) | — | every backup and every repository file (it reads receipts, never pgBackRest; no budget key) | the six DR families (`backup_last_success_timestamp_seconds{target="local"}`, `restore_drill_last_success_timestamp_seconds{target="local"}`, `backup_repo_bytes`, `backup_disk_free_bytes{volume}`, `backup_budget_headroom_bytes{limit}`, `wal_archive_failing`), §11 |
| `backup run` / `restore drill` — **HOST CRONTAB**, not the daemon (card 37, ADR-0064 D-I) | `humaux-dr.sh backup run` (nightly 00:30 UTC) and `humaux-dr.sh restore drill --evidence …` (Wednesdays 03:00 UTC) from `deploy/pgbackrest/humaux-backup.crontab`; `backup verify` / `backup status` on demand | the crontab | — | the production data volume (the drill restores into its own `humaux-drill-<id>` project and destroys it) | `ops.backup_receipts`, `ops.restore_drills`, the drill evidence file; §11 |
| `create-partitions` — **MANUAL monthly operator step**, not the daemon | `humaux-maintenance retention create-partitions --months-ahead 3` (§5.3), from the operator shell with the superuser `HUMAUX_MIGRATOR_PG_DSN` exported for that one command | monthly, first week of the month | — | existing leaves (idempotent: an existing month returns `exists`) | new `control.partition_registry` rows; `partition_horizon_months` back to 3; one §77 row `PARTITIONS_CREATED` |

- **What an operator reads**, as `role_maintenance` with the tenant GUC: `SELECT task, sum(affected), max(ran_at)
  FROM ops.maintenance_receipts WHERE tenant_id = $1 AND ran_at > now() - interval '1 day' GROUP BY task`.
  Fleet-wide: `maintenance_task_rows_total{task}` and `maintenance_task_runs_total{task,outcome}` on the daemon's
  `/metrics`; `MaintenanceCountersAbsent` (WARNING) fires while the daemon answers 503 or is not scraped, and
  `MaintenanceTaskFailing` (WARNING) records any failed call of the last hour once a clean scrape reads it. A call that deletes
  nothing writes no receipt.
- **Pause one task**: raise its `<T>_EVERY_SECONDS` and restart the unit (there is no per-task off switch: a
  disabled purge is unbounded growth).
- **The jobs door frees `idempotency_key`** (ADR-0062 L5): safe while every producer's key derives from a row
  created once (outbox row, PRIMARY evidence, `(schedule_id, planned_at)`, `(outbox row, consumer)`). A new
  producer that re-presents an old key must keep its jobs out of the door first.
- **Manual one-shot**: `humaux-maintenance sweep once --actor … --reason … --ticket … --step-up-auth …` reads the
  same keys except `METRICS_ADDR` and the `*_EVERY_SECONDS`, runs every task for one page, prints one receipt with
  per-task counts and exits 1 if any call failed (ADR-0062 D-Q).

### 5.2 Running the daemon against a database that already holds residue (ruling E8)

The first `--serve` (or `sweep once`) against a long-lived database deletes shared rows across every tenant and
makes irreversible ISSUED → LOST transitions, permanent reissue tickets and live re-drive model calls. That is a
production-class destructive step: the shared dev database (`humaux_thread_dev`) is **never** pointed at by a
test, a gate or the rehearsal (they use throwaway databases). When an operator decides to run it:

1. **Backup first**, as the owner, of exactly what it can change:
   `pg_dump -Fc -t control.confirm_tokens -t ops.selection_snapshots -t ops.selection_snapshot_items
   -t control.rate_buckets -t ops.jobs -t ops.distill_calls -t projection.stream_log -t projection.stream_checkpoints
   -t ops.outbox -t control.audit_events -t projection.ticket_reissues -t ops.maintenance_receipts <db> > pre_maintenance.dump`,
   and keep the file until the run is reviewed.
2. **State the impact** before running: the counts each door would take now (e.g. expired snapshots, DEAD/DONE jobs
   past the retentions, ISSUED tickets older than `LOST_AFTER` with no live lease, Q memories, schema-failed deaths).
3. **Run once with explicit retentions**: `humaux-maintenance sweep once` with the §77 fields (actor, reason,
   ticket, step-up) and the keys chosen for this run; read its receipt, then the receipts table.
4. Only then start the resident unit.

### 5.3 Partitions: the monthly create-partitions step and the horizon alerts (ADR-0063 D-F)

The §48.1 tables have **no DEFAULT partition**. The first insert whose key is at or past the last pre-created month
fails with SQLSTATE 23514 (`no partition of relation … found for row`) for **every tenant at the same instant**
(00:00 UTC on the 1st): audited admin operations abort, every model call is refused before dispatch, `remember`
fails, the maintenance doors answer 503. No data is lost (every failure is a refused write), but most repositories
map 23514 to **409 / 400**, not 5xx (card 36b fixes the mapping), so the outage does not page through 5xx
monitoring. The alarm is the three horizon alerts, one to two months earlier.

**Monthly step** (first week of each month; nothing stores the DSN — no cron, no supervised env file):

```sh
export HUMAUX_MIGRATOR_PG_DSN=…   # the superuser migrator principal, this shell only
humaux-maintenance retention create-partitions --months-ahead 3 --lock-timeout-ms 5000 \
  --actor <you> --reason "monthly partitions" --ticket <ticket> --step-up-auth <ref>
unset HUMAUX_MIGRATOR_PG_DSN
```

It prints one JSON receipt (`outcome` `created` or `existing`), writes one §77 `PARTITIONS_CREATED` row when it
created a leaf, and the next `--serve` PARTITIONS run shows `partition_horizon_months{table} = 3` for every table.
New leaves serve the next insert at once; nothing restarts. Exit 1 = busy (another `retention` command holds
`HXRETAIN`) or a lock wait past `--lock-timeout-ms`: re-run.

**When an alert fires** (`partition_horizon_months{table}`: 3 after the step, 2 the month after, ≤ 1 after one
missed step, -1 = a table with no leaf):

| alert | meaning | do |
|---|---|---|
| `PartitionHorizonShort` (WARNING, ≤ 1 for 1h) | one monthly step was missed; the outage is two months away | run the monthly step now |
| `PartitionHorizonExhausted` (CRITICAL, ≤ 0 for 10m) | the current month's leaf is the last one (or the table has none): inserts fail from the next 1st | run the monthly step now; check its receipt lists the table; confirm the gauge reads 3 on the next PARTITIONS run |
| `PartitionHorizonAbsent` (CRITICAL, absent for 2h) | the daemon's PARTITIONS run has not succeeded (a failed run removes the family, it never keeps the last value) or the daemon is not scraped | read `maintenance_task_runs_total{task="partitions",outcome="failed"}` and the daemon's log (`partitions: …`): usually a revoked grant on `control.partition_registry` / `control.retention_policies` or an unreachable database; fix it, the next run renders the family again. Until then nobody sees the horizon: run the monthly step by hand |

**Never attach a DEFAULT partition** as a stop-gap: it hides the miss, makes every later month creation scan and
move rows, and forbids `DETACH … CONCURRENTLY`.

### 5.4 Retention execute (ADR-0063 D-C, D-G, D-I, D-J)

Retention drops one whole month of one table for **all tenants at once** (per-tenant deletion stays §37
tombstones). `EVENTS` and `AUDIT_EVENTS` cannot have a policy (a CHECK; card 37 / a separate approval line). Every
command below runs from the operator shell with the superuser `HUMAUX_MIGRATOR_PG_DSN` exported for that command
only, never from the supervised environment (both resident modes refuse to boot when it is set), and takes the §77
fields `--actor --reason --ticket --step-up-auth` plus `--lock-timeout-ms`. Exit 0 = done / no-op, 1 = infra or busy
(re-run), 2 = configuration, 3 = refused (`{"outcome":"refused","reason":"<code>"}`).

1. **Precondition — backup.** `pg_dump -Fc -t <parent> <db> > pre_retention_<table>_<date>.dump` (the leaf rows are
   in it), checked with `pg_restore -l`, kept under §44 protection until the drop is reviewed. Card 37's PITR proof
   replaces this; until then it is required.
2. **Approve** a policy (a row IS the approval; the newest revision of a table is the current one; `forever`
   withdraws): `retention approve --table MODEL_CALL_LEDGER --months 12 --effective-at <rfc3339>`.
3. **Wait for the proposal**: the daemon's next PARTITIONS run (after `effective_at`) marks each expired leaf
   (`proposed_policy_revision`, `proposed_at`). The proposal corroborates; it authorises nothing.
4. **List**: `retention execute --policy <id> --export-dir <dir> --dry-run` — every due leaf with `registry_id`,
   bounds, row count and the verdict of `control.partition_drop_check` (`ok` or the refusal, e.g.
   `hold:unsettled_ledger_calls`, `hold:open_budget_reservations`, `hold:open_contribution_executions`,
   `hold:running_stage`, `not_due`, `newest_leaf`, `not_proposed`, `registry_catalog_mismatch <leaf>` — the registry
   row's name or bounds no longer describe the catalog's leaf; investigate, never edit the row to make it pass).
   Nothing is written.
5. **Review one leaf**: `retention execute --policy <id> --registry-id <id> --export-dir <dir> --dry-run` prints the
   exact `DETACH PARTITION` / `DROP TABLE … RESTRICT` statements the database will execute.
6. **Run it**: the same command without `--dry-run`. In one transaction the database re-derives every predicate,
   the leaf is exported (`COPY` to `<dir>/<leaf>__<registry_id>.copy`, mode 0600, fsync, rename, directory fsync,
   row count checked, sha256; the file holds every tenant's rows of the month, so `<dir>` is owner-only, e.g.
   `install -d -m 0700 <dir>`), detached,
   dropped RESTRICT (never CASCADE), and the registry row becomes the receipt (`rows_dropped`, `export_path`,
   `export_sha256`, `dropped_by`) with one §77 `PARTITION_DROPPED` row. One leaf per run; a backlog is one reviewed
   run per leaf.
7. **After exit 1** (lost acknowledgement, lock timeout): re-run the same command. A dropped leaf answers
   `already_dropped` with its stored receipt; the next month is never taken.
8. **Restore from the export** (to read it, or to hand it over): `CREATE TABLE <scratch> (LIKE <parent>)` in a
   scratch database, `COPY <scratch> FROM '<file>'`, then compare `count(*)` with `rows_dropped` and the file's
   sha256 with `export_sha256`. The parent cannot take the rows back through this door: a dropped month has no
   leaf, `control.partition_create_month` only extends the newest bound (it refuses a gap), and re-attaching a past
   month is owner DDL that needs its own reviewed decision.

## 6. First activation — performed by onboarding (ADR-0053)

A family (tenant, workspace, `private_memory`/`PRIVATE_MEMORY`/`v1`) is first activated by
`onboard tenant` / `onboard workspace` themselves, with **VerifiedEmpty** evidence: the family's
checkpoint row, stream log, outbox, point registry and gaps are all zero (derived by the DB, never
declared), a Qdrant count of the family in the collection's current generation is zero, and the
workspace is still `PROVISIONING` — which also means no write could have landed: while a workspace
is `PROVISIONING`, every stream-issuing write (`remember.put`, the governance tickets, distill,
consolidation) is refused with `CONFLICT` (SQLSTATE 55000 `workspace_provisioning`). The receipt
lands in `projection.family_activations`; when the last family of the workspace serves, it becomes
`READY`. From that moment `memory.get` answers `NOT_FOUND`, `memory.enumerate` an exact empty
census, `recall.search` an empty result with `current = true`, `context.assemble` an empty
envelope — no `DEPENDENCY_UNAVAILABLE` — and the resident runner's projections are read as they
land, with no further operator action. `humaux-maintenance activate --tenant <id> --workspace <id>`
repeats it idempotently (`existing` once done) or finishes a run that crashed between the
PostgreSQL transaction and the activation (the workspace then stays `PROVISIONING`: writes
closed, `memory.get`/`enumerate` PG-served, recall/context the explicit B read with reason
`no_serving_projection`).

`cargo xtask projection-serve` is now only for **version upgrades** (and for `LEGACY` pairs made
before card 28, which are never gated and never auto-activated) — for the version onboarding
activated it prints "already serving" and exits 0:

```sh
cargo xtask projection-serve --tenant <tenant_id> --workspace <workspace_id> \
  --domain private_memory --projection-kind PRIVATE_MEMORY --version <v> \
  [--retire-failed distill_failed,no_visible_memory_record]
```

`--retire-failed <classes>` first moves that family's `FAILED` tickets of exactly those classes
to `RETIRED_FAILED` (0167, audited), because one `FAILED` ticket pins the §15.4 prefix. Since
card 27 a transient failure is a bounded retry, not `FAILED`; what still lands in `FAILED` is a
permanent class (`qdrant_upsert_rejected`, `embedding_rejected`, `embedding_dimension_mismatch`,
`card_unbuildable`, `secret_scan_rejected`, `registry_conflict`, `distill_failed`, …) or `transient_exhausted`.
Version upgrades go through the §16.2 two-version comparison — see
`docs/ops/delivery_point_report.md` §6.1 before assuming it will succeed: a tenant with any live
projected `USER_PRIVATE` point makes the switch refuse with `VisibleUnavailable`.

## 7. Verify it is up

```sh
curl -fsS http://<gateway>/livez      # 200
curl -fsS http://<gateway>/readyz     # 200; 503 with `draining` means SIGTERM, do not restart
humaux-retrieval-worker    --readyz   # exit 0
humaux-private-worker      --readyz   # exit 0
humaux-consolidation-worker --readyz  # exit 0
```

A `--readyz` failure always **names the object that is down** (§4.4 坑5). Never read a probe
failure as a zero: if a dashboard shows `0` where a probe failed, the dashboard is wrong. The
probes emit no envelope at all on that path so a `0` cannot be manufactured downstream.

Then one real round trip: `remember.put` → distill pass → projection resolve → `recall.search`
returns the memory. This is the chain the acceptance suites exercise; a deployment that answers
`/readyz` but cannot complete it is not up.

`/readyz` is dependency-truthful since card 34 (ADR-0061 D-F): 503 `not_ready` means PostgreSQL,
the retrieval RPC round trip or Qdrant is down — do **not** restart the gateway for it; the
loopback `/status` names the dependency (supervision.md §2).

**Metrics and alerts (card 34, ADR-0061).** Every resident mode serves `/metrics` and `/status`
on its own loopback ops key (supervision.md §1 lists the seven keys). Prometheus, Alertmanager and
the collector are units of their own (supervision.md §8).

```sh
curl -fsS 127.0.0.1:<ops port>/metrics | head          # one per resident mode: 200, `# HELP` / `# TYPE` lines
curl -fsS 127.0.0.1:<ops port>/status                  # JSON naming `process`, `mode`, `git_sha`, the 11 degrade codes
sh deploy/prometheus/pinned-tool.sh promtool check rules deploy/prometheus/invariants.rules.yml deploy/prometheus/alerts.rules.yml
sh deploy/prometheus/test-rules.sh                     # every loaded alert fires and stays silent on synthetic series
curl -fsS 127.0.0.1:9090/api/v1/targets                # every humaux-* target and otelcol: health "up"
curl -fsS -G 127.0.0.1:9090/api/v1/query --data-urlencode 'query=up{job=~"humaux-.*"}'   # exactly the seven (job, mode) pairs, all 1
curl -fsS 127.0.0.1:9090/api/v1/alerts                 # Watchdog firing (always); nothing else firing on a quiet system
humaux-admin q deploy.binary                           # detail.git_sha = the sha rendered into prometheus.yml
```

- The Watchdog receipt (the sink log or the external healthchecks service) carries
  `labels.git_sha` equal to `humaux-admin q deploy.binary`'s `detail.git_sha`. A receipt that
  says `__HUMAUX_GIT_SHA__` means the deployer skipped the render: fix it before trusting any
  alert (§42.1, §67.4).
- `HealthGaugesAbsent` firing means `humaux-maintenance health serve` is down or its sample fails
  (its `/metrics` answers 503 naming why); INV-3, projection lag and dead-letter are blind until it
  is back (supervision.md §2).
- `BackupFailure` is silent until card 37 produces `backup_last_success_timestamp_seconds`;
  `metrics-registry --check` names it not_applicable with its producer card.
- `DistillNoOutput` (CRITICAL, §42 no-output stage, card 34b) means the private worker finished distill runs
  in the last hour and wrote no memory record: the stage is alive but produces nothing. Read the
  `--distill-serve` scrape (`private_distill_runs_total` rising, `private_distill_outputs_total` flat), then, in
  this order: the distill parser (the dispatch line's `memories=` / `rejected=` counts, §7.1 — replies the
  fail-closed parser turns into an empty array are exactly this alert); the provider replies (the route's
  `humaux-maintenance reasoning status`, the `error_class` of recent `ops.model_call_ledger` rows); then
  `humaux-admin q jobs.stuck`. Do not restart the worker for it — a restart resets the counters and silences
  the alert without fixing the stage. `private_reasoning_usage_total` on the same scrape is the provider-reported
  token count (input + output) the §35 quota reads.

**Manual §69 step — the external dead-man (§42.1), once per deployment and after every change to
the Watchdog route.** It is not a gate, because the endpoint is outside this repo: stop
Alertmanager for 5 minutes and confirm that the external healthchecks endpoint reports the missing
ping (its own alert or e-mail); start Alertmanager again and confirm the next ping clears it.
Record the date, the endpoint and its alert in the deployment log. A Watchdog that nobody would
miss is not a dead-man.

### 7.1 Reading the private-worker dispatch line

`humaux-private-worker: distill dispatch claimed=N completed=N failed=N not_ready=N parked=N deferred=N dead=N lost_lease=N errors=N heartbeat_lost=N attempts=N unknown=N memories=N rejected=N empty_retries=N malformed_retries=N affects_dropped=N channel_fallback=N stopped=no_work|no_slot|-`

One line per pass (`--distill-once`) or per resident run (`--distill-serve`, printed at exit).
ADR-0058: `claimed` = jobs claimed (one job = one Evidence), `attempts` = admitted provider
requests (`ops.begin_call`), `deferred` = failed calls backed off for a retry, `parked` = jobs
parked `WAITING_KEY`, `unknown` = calls cut at the HTTP window whose outcome is unknown,
`lost_lease` / `heartbeat_lost` = a job's generation was superseded (its late result is discarded,
its cost still ledgered) — fence refusals only; `errors` = jobs an error escaped (a database, reasoner or
membership failure the job could not settle; the claim is reconciled by the sweep), each named on its job
line. `affects_dropped` = written replies whose inferred affects were discarded as invalid (the memories
were kept). `channel_fallback` (ADR-0058 R9) = written replies that, on the tool channel, came back
with no tool call and the answer object in `content`; the same parser accepted them, so they are not
malformed — a steady rate says the model often ignores the tool and the content channel may measure
better (R10). `stopped` (ADR-0058 R5) = why a `--distill-once` pass ended: `no_work` = a provider slot was
free and no READY job was left for it; `no_slot` = all four slots were bound (another dispatcher holds
them), so READY work may be left for the next pass; `-` for `--distill-serve`, which never stops on an
empty claim. A healthy run reads `lost_lease=0 errors=0 heartbeat_lost=0`.

Every job also prints one line:
`humaux-private-worker: distill job=<uuid> tenant=<uuid> evidence=<uuid> gen=<n> outcome=<DONE|RETRY|NOT_READY|PARKED|DEAD|LEASE_LOST|ERROR|UNKNOWN> attempt=<n>/<max> error_class=<class|-> next_retry_s=<n|-> channel_fallback=<0|1>`
(`channel_fallback=1`: the written reply arrived in `content` on the tool channel, ADR-0058 R9)

- `outcome=NOT_READY error_class=<reason>` — nothing was sent (no admitted binding,
  `CREDENTIAL_NOT_MAPPED`, `EGRESS_PROCESSOR_NOT_ALLOWED` / `EGRESS_HOST_NOT_ALLOWED` /
  `REGION_NOT_ALLOWED` — the route's recipient, host or region is not in this deployment's lists —,
  `ROUTE_HEALTH_STALE` — the route's latest health observation ran out —, `ROUTE_HEALTH_DENIED` — it
  is valid but not HEALTHY/VALID, e.g. a key the provider rejected —, or `DOMAIN_MISMATCH`); no
  attempt is spent. Past `HUMAUX_PRIVATE_WORKER_DISTILL_NOT_READY_PARK_SECS` the job is `PARKED`
  (`ops.jobs.status = 'WAITING_KEY'`, re-checked once per park interval): fix the tenant's
  binding; the job resumes by itself.
- `outcome=NOT_READY error_class=PROVIDER_BUDGET` — the tenant spent its §72.3 distill budget
  (`HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_MAX_CALLS` admitted requests in the last
  `_BUDGET_WINDOW_SECS`, counted in `ops.distill_calls`, ADR-0058 D-T); nothing was sent and no
  attempt spent. Every request passes it, including the resend after `EXECUTION_UNCERTAIN`.
- `outcome=RETRY error_class=WORKER_DB_ERROR` after a counted call — a database error after the
  provider answered; the job was settled at once in a fresh transaction (slot freed), never left
  to the hard deadline (ADR-0058 D-E).
- `outcome=ERROR error_class=<class>` — an error escaped the job before it could settle (e.g.
  `WORKER_DB_ERROR` on taking the outbox row); the job stays claimed until the sweep reconciles it (an
  unexpired lease first). Never counted as a lost lease (ADR-0058 R3).
- `outcome=DONE error_class=affect_invalid` — the reply's memories were written, but an inferred affect
  was outside the menu (unknown key, MOOD, target, out of range, confidence over 5 000 bp), so every
  inferred affect of that reply was dropped (ADR-0058 R1). Not a failure; a steady rate is a prompt/model
  issue.
- `outcome=PARKED error_class=WAITING_KEY` — the provider answered 401: the tenant's key is
  invalid. Never DEAD, the attempt is not counted (§11); rotate the key.
- `outcome=DEAD` — `attempt` reached `HUMAUX_PRIVATE_WORKER_DISTILL_MAX_ATTEMPTS` real provider
  requests (or `ATTEMPTS_EXHAUSTED` / `PRE_DISPATCH_ABANDONED` / `FAILED_OUTPUT_SCHEMA`); the
  outbox row is `FAILED` in the same transaction and the ticket ends `distill_failed`. The
  tenant's other Evidence keeps flowing.
- `re-claimed after EXECUTION_UNCERTAIN (T6) uncertain_model_call_id=<uuid>` — a worker died (or
  stalled past `HARD_DEADLINE_SECS`) mid-call; the slot was held until the hard deadline, then
  the job was re-queued with backoff and this call counted. The named ledger row stays
  `RESERVED` (its outcome is unknown, ADR-0058 L5).
- `humaux-reasoning: provider call failed purpose=PRIVATE_DISTILL_TEXT tenant=<uuid>
  model_call_id=<uuid> error_class=<class> latency_ms=<n>` — `RETRY_WAIT` with latency ≈
  `HTTP_TIMEOUT_SECS` is a timeout, with a short latency a 429/5xx; `PROVIDER_PERMANENT` is a
  refusal.
- In-flight is bounded by the four rows of `ops.provider_slots` across every process (§67.2):
  `select slot_no, job_id, bound_until from ops.provider_slots` shows who holds them.
- `distill evidence=<id> failed: InvalidInput (<reason>)` — the parser refused the model's
  reply. `<reason>` is structural, never payload text: `not_json`, `top_level_shape`,
  `memories_missing`, `too_many_memories`, `item_shape`, `content_empty_or_too_long`,
  `memory_type_unknown`, `class_unknown`, `confidence_invalid` (ADR-0048 addendum), `nul_character`
  (ADR-0058 R8), `tool_call_shape` (tool channel: two calls, another name, a truncated reply, or no
  call and no JSON object in `content`, ADR-0058 D-M/R9). The worker
  re-asks once on its own (`malformed retry 1/1`, counted in `malformed_retries`); a row that
  still fails is a `FAILED` outbox row and a `FAILED` ticket, which blocks the §15.4 prefix
  until `role_maintenance` retires it (`projection.retire_failed_ticket`, ADR-0042). A steady
  `memory_type_unknown` rate above a few percent means the model is ignoring rule (3) of the
  distill prompt — check the model, not the parser.
- `memory_candidate_rejections_total{reason="origin_authority_ceiling"} 1 …` — an over-ceiling
  candidate persisted as PENDING (`memory.confirm` can promote it). Not a failure; the row is
  still `done=1`.
- A `MEMORY_LIFECYCLE` ticket for a superseded / revoked / expired memory settles `DONE` by
  retiring its registry binding and deleting its point (ADR-0049). `registry_failed` on such a
  ticket means the retrieval worker binary predates ADR-0049.

#### Operator procedure for a terminal or stuck distill job (ADR-0058 R4)

Read the state first (database owner session; no runtime role reads these tables):
`select job_id, status, dispatch_state, attempt, abandoned_claims, last_error_class, next_retry_at,
hard_deadline from ops.jobs where tenant_id = '<tenant>' and job_type = 'DERIVED_DISTILL' and status
in ('DEAD', 'WAITING_KEY', 'PROCESSING') order by status, last_error_class`.

- **DEAD** — terminal; nothing retries it. 1) Inspect the class (`last_error_class`, the job line's
  `error_class`, and the `humaux-reasoning: provider call failed` lines of the same `tenant=`):
  `RETRY_WAIT` / `TRANSPORT` = provider outage or timeout, `PROVIDER_PERMANENT` = refusal,
  `FAILED_OUTPUT_SCHEMA` = the model kept breaking the output contract, `ATTEMPTS_EXHAUSTED` /
  `PRE_DISPATCH_ABANDONED` = a crash loop, `WORKER_DB_ERROR` = the database. 2) Fix the cause
  (provider back, model/prompt fixed, worker stable) — re-driving an unfixed cause spends
  `MAX_ATTEMPTS` more billed requests and dies again. 3) Re-drive with the §77 fields:

  ```
  humaux-maintenance jobs requeue-dead --tenant <tenant> --job <job_id> \
    --actor <you> --reason "<why>" --ticket <T-n> --step-up-auth <ctx>
  humaux-maintenance jobs requeue-dead --tenant <tenant> --error-class RETRY_WAIT \
    --actor <you> --reason "<why>" --ticket <T-n> --step-up-auth <ctx>
  ```

  Exactly one of `--job` / `--error-class` (exit 2 otherwise); `--error-class` matches the stored
  class exactly and re-arms only that tenant's DEAD jobs. One transaction (`ops.requeue_dead_distill`,
  0197): job `PENDING`, `attempt 0`, `last_error_class` kept as the record, the Evidence's outbox row
  `FAILED -> PENDING`, the tenant's scheduler row admitted; the next pass claims it. When the
  Evidence's projection ticket had already settled `FAILED distill_failed` (or was retired / `LOST`),
  the same transaction issues a successor ticket on that stream (0198): it waits while the job is
  open and indexes the re-distilled memories once it is DONE. The old `FAILED` ticket stays an open
  gap until you retire it — `projection-serve ... --retire-failed distill_failed` — which is safe now,
  since the successor carries the Evidence. The receipt lists
  every re-armed `job_id` / `evidence_id` / `last_error_class` / `attempt_spent` under `requeued`
  and the §77 audit id. With `--error-class` it also lists, under `skipped`, every matching DEAD job
  it left DEAD (0200), each with `reason`: `evidence_gone` (the Evidence has no outbox row: a
  re-armed job could only die again) or `outbox_settled` (the Evidence is already DONE — nothing to
  re-drive); the SUCCESS audit row carries the same `skipped` list. `outcome` is `requeued`, or
  `nothing_requeued` when every match was skipped (exit 0 both; read `skipped` to see why):

  ```
  {"outcome":"requeued","tenant_id":"…","requeued":[{"job_id":"…","evidence_id":"…",
   "last_error_class":"FAILED_OUTPUT_SCHEMA","attempt_spent":3}],
   "skipped":[{"job_id":"…","evidence_id":"…","last_error_class":"FAILED_OUTPUT_SCHEMA",
   "attempt_spent":3,"reason":"evidence_gone"}],"audit_event_id":"…"}
  ```

  Exit 3 = refused, nothing written but a DENIED audit row: `job_not_found` (wrong tenant or id),
  `job_not_dead`, `evidence_gone` / `outbox_settled` (`--job` only — the same reasons as above),
  `no_dead_job` (no DEAD job with that class at all). A re-run is refused `job_not_dead` (`--job`)
  or answers only what is still DEAD (`--error-class`), never a second re-arm.
- **WAITING_KEY** (`outcome=PARKED`) — never DEAD and never re-driven by hand: rotate the tenant's
  provider key (`error_class=WAITING_KEY`, a 401) or bind/admit its `PRIVATE_DISTILL_TEXT` route
  profile (any other NOT_READY class). The claim re-checks it once per
  `HUMAUX_PRIVATE_WORKER_DISTILL_NOT_READY_PARK_SECS` and it resumes by itself; no attempt is spent.
- **EXECUTION_UNCERTAIN** (`status = 'PROCESSING'`, `dispatch_state = 'EXECUTION_UNCERTAIN'`) — a worker
  died or stalled mid-call; its slot stays bound on purpose. Wait for `hard_deadline`: the next claim
  re-queues it with backoff (`last_error_class = 'EXECUTION_UNCERTAIN'`, the uncertain call counted)
  and the resend is automatic, admitted by the tenant budget, and printed as `re-claimed after
  EXECUTION_UNCERTAIN (T6) uncertain_model_call_id=<uuid>`. Do not edit the row or free the slot.
- **A ledger row left `RESERVED`** (ADR-0058 L5) — a provider request whose outcome is unknown (the
  worker died before finalizing it). For billing it means: the provider may or may not have charged the
  tenant's account for it; Humaux's ledger holds only the pre-call `estimated_cost`, never an
  `actual_cost`, and nothing finalizes it later. The resend after `EXECUTION_UNCERTAIN` is a separate,
  counted, ledgered request — so one Evidence can show one `RESERVED` row plus one `SUCCEEDED` row (at
  most one extra billed call per uncertain claim, bounded by `MAX_ATTEMPTS`). List them (owner session):
  `select l.model_call_id, l.tenant_id, l.called_at, l.estimated_cost, c.job_id, c.attempt from
  ops.model_call_ledger l left join ops.distill_calls c using (model_call_id) where l.purpose =
  'PRIVATE_DISTILL_TEXT' and l.status = 'RESERVED' and l.called_at < now() - interval '<hard deadline>'
  order by l.called_at`. Reconcile against the provider's usage console by time window when a tenant
  disputes a charge; a `RESERVED` row with no `distill_calls` row was reserved but never admitted by
  `ops.begin_call` (a crash between the two, ADR-0058 L13): no request was sent.

### 7.2 What `PROJECTION_LAG` means and what to do (ADR-0057 D-E)

A read answers `completeness.class = cannot_establish`, `reason = projection_lag`, with
`PROJECTION_LAG` in `degradations`, when the stream it read has a pending projection ticket older
than `HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS`. The items are still served; the envelope says the
index is behind the ledger, so a recent write may be missing. `projection.current` does not
change (its formula is frozen), so watch the class, not `current`. With `PROJECTION_INVISIBLE_LOSS`
at the same time both codes are reported, loss first.

1. Is the runner alive and claiming? `humaux-retrieval-worker --serve`'s pass line
   (`projection pass claimed= done= … pending= lost_lease=`) every poll interval. No line ⇒ the
   process is stopped or hung: restart it through its supervisor (never by port or pattern, §9).
2. Is the oldest pending ticket waiting on distill? An `EVIDENCE_ACCEPTED` ticket stays `ISSUED`
   until its distill job closes; a growing private-worker backlog (§8) reads as lag. Fix the
   distill hop, not the runner.
3. Is one ticket retrying? `attempts` / `next_attempt_at` / `error_class` on the oldest `ISSUED`
   row of the stream. A poison ticket makes its whole family lag until it settles `FAILED`; that is
   intended (its write is not reflected). `FAILED` then shows as `open_gaps`, which `role_maintenance`
   retires (ADR-0042).
4. It clears by itself once the oldest pending ticket settles; no restart of the gateway is needed.

## 8. Load sizing — read before the first real traffic

The Distill hop makes **one serial provider round trip per Evidence**, so its throughput is the
provider's, not the database's. Measured on this deployment: **0.229 Evidence/s** (n = 140
`processing_runs` over 611 s, MiniMax, one resident `--distill-serve`). Ingest above that number
builds a backlog no amount of correct code will drain.

Size for `tenants × sessions / (think_secs + round_trip_secs) < 0.229`, and **re-measure that
line whenever the provider or model changes** — it is a property of the deployment, not a
constant. `docs/ops/soak.md` §"Size the load to the distill hop's capacity" has the full rule and
what it looks like when it is violated.

## 9. Operating rules that outrank convenience

- **Never kill by port.** `lsof -ti :PORT | xargs kill` and `fuser -k` are banned. On 2026-09-09
  that exact command terminated the user's WeChat, which happened to hold port 8080. Kill only
  PIDs you started and recorded, and only after `ps -o comm= -p $PID` confirms the binary name.
  If a port is busy, use another one and report the conflict.
- **Never run a DELETE-class command against shared state as a smoke test.** Throwaway database
  only. See `docs/ops/delivery_point_report.md` §5.2 for the time this rule was learned.
- **Never stop or restart shared infrastructure containers to simulate an outage.** Point at a
  closed loopback port or an absent socket instead.

## 10. Rotate — role passwords, API-key pepper, token key, provider keys, DNS pins

ADR-0059. Every value below lives in the secrets store and reaches a process as an environment
variable; none is ever on argv, in a tracked file or in a log. Rotation never needs a migration.

### 10.1 PostgreSQL role passwords

Preconditions: `HUMAUX_MIGRATOR_PG_DSN` set; new values in the store as
`HUMAUX_ROLE_PASSWORD_<SUFFIX>` (at least 32 characters of `[A-Za-z0-9._~-]`, not built on the
0011 placeholder prefix), or let `rotate` generate them. `rotate` refuses, naming the variable, any
value that equals or contains a repository placeholder; an environment that still exports the
current placeholders as `HUMAUX_ROLE_PASSWORD_*` (the dev env before its first rotation) must
**unset every one of them** (or replace it with a new value) before step 2, or `rotate` stops at the
first such variable with exit 2.

1. `deploy-check` — record the current state.
2. `humaux-maintenance roles rotate --roles-sql migrations/0011_roles_and_grants.sql --actor … --reason …
   --ticket … --step-up-auth … [--role role_x]`.
   One transaction; only SCRAM verifiers reach the server; generated values print once after
   COMMIT (pipe `grep '^HUMAUX_ROLE_PASSWORD_'` into the 0600 secrets file). Crash or unknown
   outcome: re-run with the variables set — idempotent (same passwords, fresh salts). The rotating
   principal is never a target, so a rotation cannot lock the operator out.
3. Update each service's DSN from the store and restart **one service at a time**. Open sessions
   survive; only new connections need the new value (one password per role: each service has a
   reconnect gap, ADR-0059 L2).
4. `deploy-check` → exit 0.
5. The migrator/superuser itself: `psql \password <user>` from a session of that user (libpq sends
   a client-side verifier), update `HUMAUX_MIGRATOR_PG_DSN` in the store, then `deploy-check`.

Development: the values live in `$HOME/.config/humaux/dev_role_passwords.env` (0600), including
`HUMAUX_DEV_PG_SUPERUSER_PASSWORD`; the chain's env script sources it.

### 10.2 API-key pepper — rolling phases, never one restart (gateway replicas ≥ 2, §67.5)

Generate a new 32-byte hex value N; O is the current one.

1. **Verify both.** Every gateway replica: `HUMAUX_GATEWAY_CREDENTIAL_PEPPER_HEX=O`,
   `HUMAUX_GATEWAY_CREDENTIAL_PEPPER_PREVIOUS_HEX=N`; rolling restart until **all** replicas run it.
   Maintenance stays on O. Nothing observable changes; every replica can now verify N.
2. **Switch current.** Maintenance `HUMAUX_MAINTENANCE_CREDENTIAL_PEPPER_HEX=N`, restart; then every
   replica `…_PEPPER_HEX=N`, `…_PEPPER_PREVIOUS_HEX=O`, rolling restart. A replica still in phase 1
   that sees an N-hashed key matches it as previous and asks to rehash; the closed rehash window
   refuses that (non-fatal), so keys never ping-pong.
3. **Open rehash.** Only after all replicas are in phase 2:
   `humaux-maintenance apikey pepper-epoch advance --actor … --reason … --ticket … --step-up-auth …`
   (next epoch, rehash window open). A key used from now on is rehashed to N once (migrations
   0202–0204, one `api_key.pepper_rehash` audit row in its tenant). Run it once: while the window is
   open a second `advance` is refused, exit 3, `"reason": "rehash_window_open"`, epoch unchanged
   (migration 0205) — a retry after an unknown outcome is safe and tells you the window is open.
4. **Close.** After the window you chose (at least the longest expected key idle time):
   `humaux-maintenance apikey pepper-epoch close --actor … --reason … --ticket … --step-up-auth …`
   (from then on no verifier can be rewritten, whatever a key's epoch — migration 0204), then unset
   `…_PEPPER_PREVIOUS_HEX` with a rolling restart. A key not used in the window is refused from then
   on: re-issue it (`apikey issue` + `apikey revoke`). There is no count of keys still behind the
   epoch yet (ADR-0059 L6).

Side effects: outstanding enumerate cursors and email verification codes die when a replica's
current pepper changes (phase 2); both are short-lived. Between `advance` and `close` (and only
then), the role_gateway database credential can rewrite each not-yet-rehashed key's verifier once,
audited (L12), so keep the window no longer than it must be.

Rollback. Before phase 3, undo the phases in reverse: maintenance back to O, then every replica
`…_PEPPER_HEX=O`, `…_PEPPER_PREVIOUS_HEX=N`, rolling restart; unset `…_PREVIOUS_HEX` only once no key
was issued under N (re-issue any that were). After `advance`, keys already rehashed verify only under
N: `close` first, then make O current and N previous (rolling restart) — every key still verifies —
and never unset N while a key is hashed under it; moving those keys back is a full rotation N → O
(phases 3–4 again). Never roll back by unsetting `…_PREVIOUS_HEX` mid-rotation: every key on the
other pepper is refused at once.

Card-33 rollout: a pre-card-33 gateway reads no `…_PEPPER_PREVIOUS_HEX` and never calls the rehash
door, so do not start phase 1 until every replica runs the card-33 build (whose own rollout order is
in §10.3).

### 10.3 Consistency-token key — same shape

1. Every replica `HUMAUX_GATEWAY_TOKEN_HMAC_KEY=O`, `HUMAUX_GATEWAY_TOKEN_HMAC_KEY_PREVIOUS=N`
   (hex, ≥ 32 bytes, N ≠ O); rolling restart until all run it — all verify N's key id, all still
   sign with O.
2. Every replica `…_TOKEN_HMAC_KEY=N`, `…_PREVIOUS=O`; rolling restart. Any replica verifies
   anything any other replica signed.
3. After one token TTL (`HUMAUX_GATEWAY_REMEMBER_TOKEN_TTL_SECONDS`), unset `…_PREVIOUS` with a
   rolling restart.

The token MAC proves only that a gateway issued those fields unchanged; it is not authorization
(rejected memory 710a2548). First deploy of card 33 only: outstanding unsigned tokens are refused
(`token_malformed`) for at most one TTL (L4).

Rollback. Before step 3, swap back: every replica `…_TOKEN_HMAC_KEY=O`, `…_PREVIOUS=N`, rolling
restart — all still verify both kids. After step 3 (O gone), going back to O is the same three steps
with O as the new key; unsetting N earlier refuses every outstanding token N signed (`token_malformed`)
until it expires.

Card-33 rollout (mixed versions): a pre-card-33 gateway issues unsigned tokens, which a card-33
gateway refuses, and refuses the card-33 wire form (kid and MAC appended). A replica-by-replica roll
behind one load balancer therefore refuses tokens in both directions for as long as both versions
serve. Avoid it: start the full set of card-33 replicas (all with the same
`HUMAUX_GATEWAY_TOKEN_HMAC_KEY`, `…_PREVIOUS` unset), switch all traffic to them at once, then stop
the old set. Only tokens issued by the old set before the switch are refused, for at most one TTL
(L4). Rolling back across that boundary is the same switch in reverse, with the same one-TTL cost.

### 10.4 Provider keys (private worker)

`HUMAUX_PRIVATE_WORKER_CREDENTIALS = <credential_ref>=<ENV_NAME>[,…]` names, per credential
reference, the variable holding its key (ADR-0059 D-I). To rotate: put the new key under a **new**
variable name, point the map entry at it, restart the private worker (`--distill-serve` and
`--serve-rpc`), then revoke the old key at the provider and unset the old variable. A reference
missing from the map parks its distill jobs `WAITING_KEY` with class `CREDENTIAL_NOT_MAPPED` — no
ledger row, no provider call, no attempt spent — and they are re-checked every
`HUMAUX_PRIVATE_WORKER_DISTILL_NOT_READY_PARK_SECS`. Consolidation and contribution calls are refused
the same way before any reservation (ADR-0060 D-B). An explicitly empty map is legal: the worker boots
and every route parks `CREDENTIAL_NOT_MAPPED`. References that share one key (by variable name or by
value) must all be bound to one vendor account, or the worker refuses to start naming them
(ADR-0060 D-J); boot prints one `credential ref=<uuid> env=<NAME> account_hash=<8 hex|unregistered>`
line per entry.

The provider, model, endpoint and capabilities of every call come from its admitted route
(ADR-0060 D-B); the worker env names none of them. It names only the deny-only lists
`HUMAUX_PRIVATE_WORKER_EGRESS_RECIPIENTS = <egress_processor_id>=<host>[|<host>…][,…]` (each
recipient with the hosts its endpoints may dial; a subdomain of a listed host is allowed) and
`HUMAUX_PRIVATE_WORKER_REGIONS = <region>[,…]`. Both are required; an explicitly empty value allows
nothing (every route parks with its class). The removed keys `HUMAUX_PRIVATE_WORKER_KEY_ENV`,
`_PROVIDER_ID`, `_MODEL_ID`, `_MODEL_REVISION`, `_CHAT_URL`, `_CAPABILITIES`, `_EGRESS_PROCESSOR_ID`
and `_REGION` are refused at boot when set. Onboarding order for a new route: start with the lists
as they are, register the profile, add its `<credential_ref>=<ENV_NAME>` entry and its recipient,
restart the worker, bind, attest health.

`HUMAUX_PRIVATE_WORKER_HEALTH_RENEW_SECS` (required, no default; ruling E3): after a SUCCEEDED call
the worker appends a `WORKER_OBSERVED` HEALTHY provider and account observation valid for this many
seconds, but only when the admitted observation has less than half of it left, so a route with
traffic never expires and a busy route costs one definer write per half window. After a provider
401 it appends one account observation with the credential verdict INVALID, so the next admission
of that route is refused `ROUTE_HEALTH_DENIED` at once. Trade-off: a larger value keeps a quiet
route admissible longer after its last success but also keeps a provider that degraded without a
401 admissible longer. A route with no traffic for longer than its attestation parks
`ROUTE_HEALTH_STALE` until it is re-attested (ADR-0060 L3; a prober is a later card). The rehearsal
and `e2e-onboard` use 1800. Two profiles that share one vendor account share that account's rate
limit at the provider (research amendment 6); nothing in the worker separates them.

### 10.5 DNS pins (OPS-10)

Refresh `HUMAUX_PRIVATE_WORKER_DNS_PINS` (and `HUMAUX_MINIMAX_DNS_PINS` in the chain) from an
authoritative resolver, check each IP is outside the §11.4 forbidden ranges (the worker re-checks at
boot and refuses a forbidden pin), restart, and confirm with one live call.

## 11. Disaster recovery (card 37, ADR-0064)

What card 37 ships is the go-live class **`local_only`**: one encrypted pgBackRest repository on its own fixed-size
filesystem on this server, a nightly checked full backup, continuous WAL archiving, a weekly restore drill and a
real restore (`restore pitr`). Qdrant is never backed up: it is rebuilt from PostgreSQL (`projection rebuild`), which
holds every point's vector. Every arm runs through `deploy/pgbackrest/humaux-dr.sh`, which refuses unless
`$HOME/.config/humaux/dr.env` is mode 0600 and yours.

**NOT OFFSITE.** All backups are on the same server and disk as the database. **host loss = total loss.** The
system says so on every backup arm, in `backup status`, in every drill and restore evidence file, and with the
`BackupNotOffsite` warning, which repeats once a week (it is never silenced) until card 37d's NAS copy works.
中文：**目前所有备份与数据库在同一台服务器、同一块盘；服务器或磁盘丢失 = 全部丢失。** 在卡 37d 的 NAS 副本生效之前，
`BackupNotOffsite` 每周提醒一次（不会被静音）。

### 11.1 Before go-live (once, in this order)

1. **Give the backups their own fixed-size filesystem** (uid:gid of `postgres` in the pinned image is **999:999**,
   ADR-0064 SP-13), outside Docker's data root:
   ```sh
   sudo fallocate -l 12G /srv/humaux/pgbackrest.img && sudo mkfs.ext4 -q -m 0 /srv/humaux/pgbackrest.img && sudo install -d /srv/humaux/pgbackrest
   # /etc/fstab:
   # /srv/humaux/pgbackrest.img /srv/humaux/pgbackrest ext4 loop,nodev,nosuid,noexec 0 2
   sudo mount /srv/humaux/pgbackrest && sudo chown 999:999 /srv/humaux/pgbackrest && sudo chmod 0750 /srv/humaux/pgbackrest
   ```
   and set `HUMAUX_PG_REPO_DIR=/srv/humaux/pgbackrest`. This takes 12 GiB of the disk at once and never more; when
   it is full, backups stop, the database does not. Deleting Docker volumes or `docker compose down -v` can never
   delete it (it is a host bind with `create_host_path: false`, never a Docker volume).
2. **Write `$HOME/.config/humaux/dr.env`** (mode 0600, owned by the user whose crontab runs the arms; names only
   here, values are yours). `humaux-dr.sh` refuses (exit 3) to run any arm, `compose` included, until this file
   exists with that mode and owner, so this step comes before anything below:
   `PATH` (cron's `PATH` is `/usr/bin:/bin`; it must reach `humaux-maintenance` and `docker`, e.g.
   `/usr/local/bin:/usr/bin:/bin` — `humaux-dr.sh` refuses, exit 3, naming the binary it cannot find),
   `HUMAUX_MAINTENANCE_PG_DSN` (role_maintenance on the production database), `PGBACKREST_REPO1_CIPHER_PASS`,
   `HUMAUX_MAINTENANCE_BACKUP_COMPOSE_FILE`, `HUMAUX_MAINTENANCE_BACKUP_PROJECT`,
   `HUMAUX_MAINTENANCE_BACKUP_REPO_MAX_BYTES` = **8 GiB** (8589934592), `HUMAUX_MAINTENANCE_BACKUP_MIN_FREE_BYTES` =
   **2 GiB** (2147483648, the free space that must remain on the backup filesystem after the new set: the WAL
   margin), `HUMAUX_MAINTENANCE_DRILL_COMPOSE_FILE`, `HUMAUX_MAINTENANCE_DRILL_MIN_FREE_MEMORY_BYTES`,
   `HUMAUX_MAINTENANCE_DRILL_RESTORE_TIMEOUT_SECONDS`, `HUMAUX_MAINTENANCE_RESTORE_MIN_FREE_DISK_BYTES` = **7.5 GiB**
   (8053063680), `HUMAUX_PG_REPO_DIR`, `HUMAUX_PG_IMAGE`, `HUMAUX_DRILL_PG_MEM_LIMIT`, `HUMAUX_DRILL_QDRANT_MEM_LIMIT`,
   `HUMAUX_DRILL_CPUS`, the retrieval worker's `HUMAUX_RETRIEVAL_WORKER_GITLEAKS_{BIN,VERSION,SHA256}` (the drill's
   scanner pin, read under the worker's names), and the compose keys `HUMAUX_PG_CONTAINER`, `HUMAUX_PG_PORT`,
   `HUMAUX_PG_ARCHIVE_TIMEOUT_SECONDS` (= 3600) and `HUMAUX_PG_LISTEN_ADDRESSES`. Every key is required with no
   default; a missing key is exit 2. The full list with readers is `docs/architecture/env_vars.md`.
3. **Start the production database through the wrapper, then create the stanza and check it:**
   ```sh
   humaux-dr.sh compose up -d pg
   humaux-dr.sh compose exec -T --user postgres pg pgbackrest --stanza=humaux stanza-create
   humaux-dr.sh backup check
   ```
   Always start or recreate `pg` with `humaux-dr.sh compose …`, never with a plain `docker compose up`: `backup.yml`
   passes `PGBACKREST_REPO1_CIPHER_PASS` by name only, so a container started without `dr.env` cannot encrypt, every
   WAL archive push fails and `WalArchiveFailing` latches. `backup check` must print `repo_cipher=aes-256-cbc` and
   `weak_subkeys=0` and exit 0. It also refuses (`repo_shares_pgdata_fs`) a backup directory on the database's own
   filesystem — that is what an unmounted image looks like. The encryption keys inside the backup store are made
   once, now; the future NAS copy will be this same store.
4. **Write the backup password down** (`PGBACKREST_REPO1_CIPHER_PASS`) on paper or in your password manager, away
   from this server. Without it no backup can ever be read (ADR-0064 L14). When the NAS is added, card 37d asks you to
   prove this copy with one command before anything is called "offsite".
5. **Import the production data and run the E12 re-embed** (`projection rebuild --all --allow-reembed <legacy count>`,
   provider cost = legacy points × the embedding price, under the retrieval worker's budget) **before** the first
   `backup run`, so their burst of changes leaves the backup store with the first rotation.
6. **Create the drill evidence directory and install `deploy/pgbackrest/humaux-backup.crontab`.** The crontab
   assumes the checkout at `$HOME/humaux` and writes the weekly drill evidence into `$HOME/humaux/dr-evidence`:
   ```sh
   install -d -m 0700 "$HOME/humaux/dr-evidence"
   crontab -l 2>/dev/null | cat - "$HOME/humaux/deploy/pgbackrest/humaux-backup.crontab" | crontab -
   ```
   It holds exactly two lines (UTC): `30 0 * * *` `humaux-dr.sh backup run` (nightly full) and `0 3 * * 3`
   `humaux-dr.sh restore drill --evidence <dir>/drill-<date>.json` (weekly drill). A drill whose evidence directory
   is missing refuses before it starts (exit 2, no receipt), so it can never record a success whose evidence is
   lost. No gate proves the crontab is installed: `BackupFailure` (26 h) and `RestoreDrillFailure` (8 d) are the
   proof that it runs (L6).
7. **In the daemon's supervised environment** (`humaux-maintenance --serve`, §5.1) set
   `HUMAUX_MAINTENANCE_DR_REPO_FS_PATH=/srv/humaux/pgbackrest`, `HUMAUX_MAINTENANCE_DR_PGDATA_FS_PATH=/` and
   `HUMAUX_MAINTENANCE_SERVE_DR_EVIDENCE_EVERY_SECONDS`; as root confirm that `df -P /var/lib/docker` and `df -P /`
   print the same device and that `df -P /srv/humaux/pgbackrest` prints a `/dev/loop…` device. These are not
   secrets; the budget keys are **not** daemon keys (each receipt records the limits the arm used).

### 11.2 What is protected today (`local_only`)

- Every night at 00:30 UTC: a full backup of PostgreSQL, a read-back check of every file (`pgbackrest verify --set`
  plus the manifest identity), and only then removal of backups older than the newest two (`repo1-retention-full=2`,
  expire only after VERIFIED). A failed check removes nothing.
- Between backups, changes are copied into the backup store at least once an hour (`archive_timeout` = 3600 s).
- Every Wednesday a drill restores the latest backup into a throw-away copy, checks it, rebuilds search from it with
  no provider call, deletes the copy and checks that the backup directory was not touched (`repo_intact`).
- You can undo a mistake from up to 24–48 hours ago, **as long as `backup status` shows `pitr_window_unbroken_since`
  that far back**. If archiving broke, it says from when the window is whole again.

### 11.3 What is NOT protected today

- **All backups are on the same server and disk as the database. If the server or its disk is lost, everything is
  lost** (host loss = total loss, L37). The backup filesystem is separate from the root filesystem (it survives Docker
  cleanup and a full root filesystem), not from disk or host loss (L25).
- 中文：**目前所有备份与数据库在同一台服务器、同一块盘；服务器或磁盘丢失 = 全部丢失。** 在卡 37d 的 NAS 副本生效之前，
  `BackupNotOffsite` 每周提醒一次。
- A compromised server (root or docker access) can wipe or corrupt the backup store and forge receipts (L21).
- `dev` is not backed up (ruling E7): `BackupFailure` and `RestoreDrillFailure` fire on the dev observability stack,
  truthfully.

### 11.4 Disk: what uses it, its cap, what happens at the cap, how to check

| what | cap | at the cap | check |
|---|---|---|---|
| backup store (2 fulls + WAL since the older one) | its own 12 GiB filesystem; `REPO_MAX_BYTES` (8 GiB) and `MIN_FREE_BYTES` (2 GiB left on that filesystem) checked **before** each backup | the next backup is refused before writing, existing backups stay, `BackupBudgetLow` warns; if WAL then fills the filesystem, archiving stops and `WalArchiveFailing` (critical) fires — the database keeps running. Remedy in 11.5 | `humaux-dr.sh backup status`; `backup_repo_bytes`, `backup_budget_headroom_bytes`, `backup_disk_free_bytes{volume="repo"}` |
| free space on the main disk | `DiskFreeLow` (critical) below 7.5 GiB, whatever the cause; drills and restores refuse to go below 7.5 GiB | you are paged before PostgreSQL stops | `backup_disk_free_bytes{volume="pgdata"}` |
| unarchived WAL (`pg_wal`) when archiving breaks | 2 GiB (`archive-push-queue-max`) | after about 5 days at light load the oldest unarchived changes are dropped from the backup (the database keeps running); this also happens when the backup filesystem is full. `WalArchiveFailing` (critical) fires within 15 minutes and **stays on until a new nightly backup has been checked** | `wal_archive_failing`; fix the cause, then `humaux-dr.sh backup run` |
| weekly drill copy | refuses below its memory floor or when the copy would leave less than 7.5 GiB free, naming the bytes (`drill_free_bytes:<need>/<have> (restore= vectors= repo_copy= floor=)`) | drill skipped; `RestoreDrillFailure` after 8 days | the drill evidence file |
| Docker images and build cache | not capped by this card | on the old host they took 46 GB; pruning is your decision. **Never needed for backups:** the backup directory is not a Docker volume | `docker system df` |

Measured on the card-36 dev dump (ADR-0064 "S7 measurements"): one compressed, encrypted full = 149 MB for a
770 MB cluster; peak with retention 2 and a generous WAL bound ≈ 0.74 GB, far inside 8 GiB.

### 11.5 After a refused backup

`backup run` exit 3 means it refused before writing anything (`budget_repo_max:<bytes>`,
`budget_free_floor:<bytes>` or `repo_shares_pgdata_fs`) and wrote a FAILED receipt; the existing backups stay. If
`backup status` shows the backup filesystem still has room, raise `REPO_MAX_BYTES` in `dr.env` and run
`humaux-dr.sh backup run`. If it does not, grow the filesystem (stop `pg`, `umount`, `truncate -s +4G` the image,
`e2fsck -f`, `resize2fs`, mount, start `pg`) or free the main disk (`docker image prune` / build-cache prune, your
decision). **Never delete a verified backup to make room.** A set whose check FAILED stays until you remove it
(`pgbackrest expire --set=<label>`, your deletion decision, L29); it counts toward retention and budget meanwhile.

### 11.6 When something must be restored on this server ("PG lost")

Keep the damaged data. Default target: **end of archive**; `--target-time <rfc3339>` only to undo a logical mistake
inside the window `backup status` shows.

1. `docker compose -f <backup.yml> -p <old project> down` — **never with `-v`**: the container goes, the damaged data
   volume stays.
2. `humaux-dr.sh restore pitr --compose-file <backup.yml> --project <new project> --target end --evidence <file>` (or
   `--target-time <ts>`), with the same `HUMAUX_PG_CONTAINER` and `HUMAUX_PG_PORT` as before. It refuses (exit 3)
   while the old project still runs (`production_pg_running:<container>` — only one cluster may ever archive into the
   stanza), when the new project's data volume is not empty, when the disk would drop below 7.5 GiB
   (`restore_free_bytes:<need>/<have>`), and when the backup does not verify (`repo_set_unverifiable:<label>`). It
   restores, boots socket-only, sets **NOLOGIN** on role_gateway and the private, consolidation and public workers,
   records the restored in-flight state in the evidence, then boots with TCP. The restored cluster archives on its new
   timeline: it is the new production.
3. Set `HUMAUX_MAINTENANCE_BACKUP_PROJECT=<new project>` in `dr.env`.
4. Run `humaux-dr.sh backup run` at once, so a verified full exists on the new timeline.
5. `humaux-maintenance projection rebuild --all …` (Qdrant from the restored PostgreSQL), reconcile the restored job
   state (RESERVED ledger rows, in-flight jobs; L8), re-run card 36's `retention execute` (drops are re-derived from
   the approved policy), then `ALTER ROLE … LOGIN` the locked roles. Deletions committed after the target are lost
   with every other write of that window: re-submit them from the requester's side (D-T).

Never start the old project again; to read its data, ask first. A restore needs free disk of about one database.

### 11.7 Qdrant lost

`humaux-maintenance projection rebuild --all --batch <n> --wait-seconds <n>` before traffic (plus
`--allow-reembed <legacy count>` while pre-card-37 points without a stored vector remain, D-G). The rebuild issues a
new generation of tickets on the same streams, the resident retrieval worker re-projects them from
`projection.memory_vectors` (no provider call), and `projection verify` proves the collection equals PostgreSQL
(count, id set, payload Merkle, per-point vectors). A changed embedding fingerprint is refused `re_embed_required`
(card 37b re-embeds).

### 11.8 Measured RPO and RTO (measured, not promised)

Numbers from ADR-0064 "S7 measurements" (dev-sized data, this Mac, n = 3 medians):

| quantity | value | from |
|---|---|---|
| full backup | 3.9 s | M1 `full_backup_s` (770 MB cluster) |
| verify of one set | 0.75 s | M1 `verify_full_s` |
| restore to promotion (`restore pitr`) | 8.5 s | M1 `pitr_restore_s` |
| rebuild of 17,435 points (issue + project + verify) | 147 s (0.15 + 126 + 21) | M2 |
| **RTO (dev size)** = restore + drill checks + rebuild + verify | ≈ 156 s (2.6 min) | M1 + drill `checks_s` 0.43 s + M2 |
| **RPO with the WAL archive intact** | ≤ 1 h at light load (`archive_timeout` 3600 s; busier writes fill segments sooner) | D-V |
| **RPO if the WAL archive is lost** | the age of the last verified full (≤ 24 h) | D-V |

Production scales roughly linearly in database bytes and points; re-measure at the first production drill (L13).
§67.2's RTO/RPO bound is 24 h.

### 11.9 Other routine

- **After a killed drill:** `humaux-maintenance restore drill --destroy-stale` (removes only resources labelled
  `humaux.drill`; the next drill refuses `stale_drill` until then).
- **Reading a drill evidence file:** `succeeded`, `repo_intact`, `repo_class` (always `local_only`), the notice, and
  each check (`witness_a_present`, `witness_b_absent`, `migrations_drift`, `rls_unforced`, `isolation_violations`,
  `payload_digest_mismatches`, `provider_calls`, `rebuild.equivalent`, `residue`) are plain fields; the timings are
  `restore_s`, `checks_s`, `rebuild_s`, `destroy_s`, `rto_seconds`.
- **Cipher pass custody:** readers are the postgres OS user in `pg` (hence every PostgreSQL superuser), docker users
  on the host and the owner of `dr.env` — all of whom already read the live database. Losing it loses every backup.
- **Deletion bound** (§37 DeletionGraph "Backups"): data deleted from the database leaves the backups **3 days**
  after the last set holding it expires ((2 + 1) × 1 d, D-T).

### 11.10 When the NAS arrives

Tell us four things: the NAS brand and model and its operating-system version; whether it can run a scheduled shell
script with `ssh`, `rsync`, `sha256sum`, `awk` and `find`, or can run Docker; whether your home internet has a fixed
IP address (so the server's firewall can admit only your home); and how much space the share has (it needs room for
about two full backups plus a day of changes, plus the snapshots you choose to keep). Also confirm that the backup
password from go-live step 3 is written down away from the server. Card 37d then wires the NAS copy — about one and a
half days of work — and until it is done nothing on the server will call any copy "offsite".
