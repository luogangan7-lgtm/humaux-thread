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
  usually the long hop, §8) and below the sweep SLA card 35 introduces, or stuck tickets turn
  `LOST` before they ever read as lag. The rehearsal uses 20.
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
5. `humaux-gateway`

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
