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

`cargo xtask rls-check` must exit 0. It walks the §6.2.2 matrix row for row; a new table or a
new grant that is not in both the matrix and `xtask/src/rls_check.rs::MATRIX` reds it.

`role_admin` is created **without a password** (`migrations/0110_mechanism_runtime_evidence.sql:70`
— deployments set it from their secrets manager, which is outside migrations). Provision it
before the mechanism-observation surface is used.

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

### 7.1 Reading the private-worker dispatch line

`humaux-private-worker: distill dispatch … done=N failed=N … rejected=N empty_retries=N malformed_retries=N`

- `distill evidence=<id> failed: InvalidInput (<reason>)` — the parser refused the model's
  reply. `<reason>` is structural, never payload text: `not_json`, `top_level_shape`,
  `memories_missing`, `too_many_memories`, `item_shape`, `content_empty_or_too_long`,
  `memory_type_unknown`, `class_unknown`, `confidence_invalid` (ADR-0048 addendum). The worker
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
