# ADR-0052 — One resident, tenant-free projection runner claims tickets by lease; a transient failure is a retry, not a verdict

- Status: Accepted (card 27, 2026-09-29, on HEAD `bfbd101`; design pass + implement pass). The
  implement pass's deviations from the design text are listed under "Implementation notes"; the
  measurements are in "Measurements".
- Spec: Baseline §4.2 (process set), §6.2.2 / §48.2 (grant matrix), §15.1–§15.5 (stream ledger, states,
  prefix, overlay), §15.2.1 (audited retirement), §16.2 / ADR-0017 (serving switch stays an ops act —
  not this ADR), §17.3 (placement), §17.4 (upsert → verify → checkpoint), §31 / §61 (leases), §42
  (projection lag), §46 / §46.1 (forward-only migrations, manifests), §78 (config keys);
  ADR-0036 (0164 cross-tenant claim shape), ADR-0037 (probes, SIGTERM drain), ADR-0042 (0167
  retirement), ADR-0043 (per-row lease heartbeat), ADR-0048 (no unbounded retry), ADR-0049 (dead
  memory → retire its point), ADR-0050 (migrate executes checks in one transaction).
- Closes: audit P1-1 (nothing drives projection), C4 (one transient error = permanent FAILED),
  P1-15 (hot-path indexes), DM-7 (three NOT VALID CHECKs).

## Context (read on `bfbd101`, dev DB `humaux_thread_dev`, 2026-09-29, read-only queries)

- `bins/retrieval-worker/src/main.rs:117-119` has three modes; `--run-once` pins ONE tenant from env
  (`:204-206`) and a `SharedFallback` placement from env (`:243-252`). Its only driver is
  `docs/ops/rehearse.sh:829-853`, a zsh loop over two hard-coded pairs every 5 s. Under the runbook
  nothing drives projection: dev has `ISSUED 145` tickets right now.
- `projection_worker::run_once` (`crates/adapters/src/projection_worker.rs:960`) reads ISSUED rows of one
  `StreamKey` with a plain SELECT (`fetch_issued_rows`, `:848`, doc: "a second concurrent run_once against
  the same key would race it"), processes each row end to end and writes a terminal state. Every
  failure site maps to `RowTerminal::Failed` (`:568-785`): sixteen `error_class` values, most of them
  infrastructure (`db_begin_failed`, `qdrant_upsert_failed`, `embedding_failed`, …). 0167's header
  already records that FAILED is "NOT evidence that retries are exhausted".
- `projection.stream_log` has no lease or attempt column. The 0011/0167 transition guard lets
  `role_retrieval_worker` do exactly `ISSUED -> {DONE,SKIPPED_BY_POLICY,FAILED}`, and returns early
  when `OLD.state = NEW.state` — so any non-state column write by that role is already legal.
  `role_retrieval_worker` holds **table-level** `SELECT, UPDATE` on `stream_log` (0011:253) and `SELECT`
  on `tenant_placements`.
- RLS: `stream_log_tenant_isolation` and `tenant_placements_tenant_isolation` are the bare
  `tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid` form, FORCE on, no owner arm.
  `ops.outbox` already has `outbox_phase9_owner_dispatch_read` (SELECT TO role_migration_owner).
- `projection.retire_failed_ticket` (0167) takes `p_tenant_id` and **relies on FORCE RLS** to confine
  the owner to the caller's installed tenant ("Setting it from p_tenant_id here would hand any executor a
  cross-tenant write").
- `tenant_placements` PK is `(tenant_id, projection_family)`; dev has 49 `private_memory_v1` rows, all
  `SHARED_FALLBACK` → `humaux_private_memory_v1_e2e`.
- Ticket families on dev: `workspace/private_memory/PRIVATE_MEMORY/v1` 1,473 rows over 147 tenants;
  `tenant/private_memory/PRIVATE_MEMORY/v1` 58 rows (run_once refuses non-workspace scope with
  `InvalidInput`).
- `xtask/src/migrate.rs:281-296` runs precheck, body, postcheck and the ledger row in ONE explicit
  transaction, so `CREATE INDEX CONCURRENTLY` cannot run through it today.
- `authorize_cell_access(.., Duration::from_secs(60))` in `build_cell_access`: a permit expires 60 s
  after it is minted, which a resident process outlives.
- Distill throughput, one resident `--distill-serve` on MiniMax: 0.229 Evidence/s
  (`docs/ops/delivery_point_report.md:293`).

## Decisions

### D-A — Ticket order inside a family need not be serial; ticket processing inside a family must be exclusive

Evidence, per row, from `process_row`/`resolve_and_embed`/`finish_row`/`retire_row`:

1. Each ticket re-reads the memory's **current** state (`resolve_memories`, status partition at
   `:610-612`); it never replays a snapshot taken at issue time. Running ticket N+1 (supersede M) before
   N (put M) ends the same: N+1 retires M's registered points and indexes M'; N later resolves M as dead
   and retires nothing. Sequential out-of-order processing converges, so the family is **not**
   head-of-line blocked by a ticket in backoff.
2. Concurrent processing of one family does not converge. `finish_row` upserts to Qdrant **before**
   `register_private_memory_point` (`:732-763`); `retire_row` retires only **registered** points
   (`retire_points_for_memory`). Interleave worker A (reads M live, upserts M's point) with worker B
   (supersede: retires M's registered points = none, deletes nothing), then A's registration fails
   `SourceNotLive` → the point A wrote is live in Qdrant and bound to nothing, so no later retirement can
   find it. The registry also turns two concurrent writes of one identity into
   `RegistryRaceLost`/`IdentityAlreadyBound`, and `advance_prefix` is written for one writer per key.
3. So the requirement is **mutual exclusion per family**, not ordering. Claimed tickets of one family go
   to one worker and are processed sequentially in `stream_seq` order. Different families may run
   concurrently.

Mechanism: one transaction-scoped advisory lock, `pg_advisory_xact_lock(5213004883044813901)`
(ASCII "HXPRJCLM"), taken as the claim function's first statement, then the claim as a second statement.
Inside the lock the claim excludes any family that still has an `ISSUED` row whose lease is live. The
claim function's statements get fresh snapshots (READ COMMITTED, VOLATILE plpgsql). Any claimer that
committed before we got the lock is therefore visible to the in-flight predicate. The function refuses
any other isolation level (`25000`).
`# ponytail: one global claim lock; the claim holds it for milliseconds and 1–3 workers are expected. If claim p95 grows under contention, move to per-family locks taken in a plpgsql loop, where each family is re-checked in its own statement.`

A per-family `pg_try_advisory_xact_lock` inside ONE statement is rejected (see Rejected): the statement's
snapshot predates the lock, so a family whose previous claimer committed between our snapshot and our
lock still looks idle.

### D-B — Lease and attempt columns live on `projection.stream_log`; the state stays `ISSUED`

Migration 0176 adds `lease_owner text`, `lease_expires_at timestamptz`,
`attempts integer NOT NULL DEFAULT 0`, `next_attempt_at timestamptz`, plus
`CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL))` and `CHECK (attempts >= 0)`. Adding a
constant-default column is a metadata-only change in PostgreSQL 11+.

Why not a sibling claim table: stream_log is updated in place already (state, error_class, settled_at,
retired_*). A sibling table would still need the same owner arm on `stream_log` to find ISSUED rows
across tenants. It would also add a second row per ticket that must settle atomically with the first, a
second RLS policy, and a second grant row.

Why the state stays ISSUED (no `PROCESSING`/`RETRY_WAIT` for this role): the lease and the backoff are
column writes with `OLD.state = NEW.state`. The guard trigger therefore returns early, and the §6.2.2
verbatim triple for `role_retrieval_worker`, pinned byte-for-byte by
`rls_check.rs:3567` ("OLD.state = 'ISSUED' AND NEW.state IN (...)"), does not change. "RETRY" is a
derived predicate, named once in `stream_repo` and used by the harness:
`state = 'ISSUED' AND attempts >= 1 AND next_attempt_at > now() AND lease_owner IS NULL`.
Carried debt (card 35): `stream_repo::sweep_lost` sweeps every ISSUED row older than its SLA that has
no in-flight `ops.jobs` row. Projection tickets never have one, so its first caller must exclude
`attempts > 0` and live-leased rows.

Cross-tenant visibility: 0176 gives `stream_log_tenant_isolation` and
`tenant_placements_tenant_isolation` the same `current_user = 'role_migration_owner' OR` arm on both
legs. It is the shape used in 0004/0012/0112/0147/0163/0164, and the tenant arm is reproduced from the
live `pg_get_expr` byte for byte. **Consequence handled in the same migration:** with that arm,
`projection.retire_failed_ticket` would silently become cross-tenant. 0176 `CREATE OR REPLACE`s it with
one extra predicate, `AND tenant_id = NULLIF(current_setting('humaux.tenant_id', true), '')::uuid`, so
its documented contract ("caller must have installed the tenant GUC") becomes a predicate instead of an
RLS side effect. A test pins it (`retire_failed_ticket_still_refuses_a_tenant_other_than_the_installed_one`).

### D-C — `projection.claim_issued_tickets`: 0164's shape plus fairness, family exclusivity, placement and distill predicates

Signature: `projection.claim_issued_tickets(p_domain text, p_projection_kind text,
p_projection_version text, p_projection_family text, p_lease_owner text,
p_lease_seconds double precision, p_limit bigint, p_per_tenant_cap bigint)
RETURNS TABLE (<7 stream key columns>, commit_seq bigint, attempts integer, <the 7 placement columns
placement_repo::from_row reads>)`, SECURITY DEFINER, owner `role_migration_owner`,
`SET search_path = pg_catalog`, `#variable_conflict use_column`. EXECUTE goes to
`role_retrieval_worker` **only**, and PUBLIC plus the other seven runtime roles are revoked. The
family parameters come from `RetrievalFamily::PrivateMemoryV1` / `ticket_family()` in the binary, which
keeps card 21's single source and adds no literal.

Body, in two statements:
1. validate the arguments (`22023`) and require `transaction_isolation = 'read committed'`, then
   `PERFORM pg_advisory_xact_lock(5213004883044813901)`.
2. `RETURN QUERY WITH cand AS (…) , capped AS (…) , picked AS (… FOR UPDATE OF s SKIP LOCKED) UPDATE … RETURNING …`, where
   - **eligible** (E): `state='ISSUED' AND scope_kind='workspace' AND` the family triple `AND (lease_expires_at IS NULL OR lease_expires_at < clock_timestamp()) AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp())`.
     The expired-lease arm is how a killed worker's tickets come back, as in 0164.
   - **placed**: `JOIN projection.tenant_placements p ON p.tenant_id = s.tenant_id AND p.projection_family = p_projection_family`.
     A tenant without a placement row is never claimed.
   - **family idle**: `NOT EXISTS (SELECT 1 FROM projection.stream_log f WHERE <same 6 family columns> AND f.state='ISSUED' AND f.lease_expires_at >= clock_timestamp())` (D-A).
   - **distill closed**: `NOT EXISTS (SELECT 1 FROM ops.outbox o WHERE o.tenant_id=s.tenant_id AND o.commit_seq=s.commit_seq AND o.event_type='EVIDENCE_ACCEPTED' AND o.status IN ('PENDING','PROCESSING'))`.
     This is the ADR-0016 D6 `Pending` condition moved into the claim. Without it, each poll claims and
     releases tickets whose distill is still open. At 0.229 Evidence/s those can outnumber `p_limit`
     and starve ready tickets behind them. `RowTerminal::Pending` stays as the race fallback.
   - **fairness**: `row_number() OVER (PARTITION BY s.tenant_id ORDER BY s.scope_id, s.stream_seq) AS rn`,
     keep `rn <= p_per_tenant_cap`, `ORDER BY rn, issued_at LIMIT p_limit`. Tenants are served round
     robin; within a tenant the order is (family, stream_seq).
   - the UPDATE sets `lease_owner`, `lease_expires_at = clock_timestamp() + make_interval(secs => p_lease_seconds)`,
     `attempts = attempts + 1`, and **restates E** (0164's EvalPlanQual argument). `RETURNING` joins
     the placement row, so the worker needs **no placement lookup and no placement cache**. The
     placement is read fresh at every claim, at no extra cost (see Rejected: cache).

A second definer, `projection.unplaced_issued_tickets(p_domain, p_projection_kind,
p_projection_version, p_projection_family) RETURNS TABLE (tenant_id uuid, tickets bigint)`, with the
same ownership, search_path and EXECUTE set. It exists only to feed the worker's `placement_missing`
log line, `projection_worker: placement_missing tenants=<n> tickets=<m> first_tenant=<uuid>`, emitted
once per pass when the result is non-empty (one line, not one per tenant: at a 1 s poll a per-tenant
line would flood the log with every unplaced dev tenant).

Index for the claim, in its own CONCURRENTLY file (0177):
`projection.stream_log (domain, projection_kind, projection_version, tenant_id, scope_kind, scope_id, stream_seq) WHERE state = 'ISSUED'`.
It serves E, the family-idle probe and the unplaced count. Without it every poll scans every tenant's
settled history, which is the P1-15 pathology introduced fresh.

### D-D — Heartbeat and settle are fenced UPDATEs by the role itself (ADR-0043), not new functions

`role_retrieval_worker` already holds table-level UPDATE on `stream_log`, so no definer and no new
grant are needed. All writes below run under the claimed ticket's own tenant GUC
(`set_worker_rls_context`).

- **heartbeat**, per family:
  `UPDATE projection.stream_log SET lease_expires_at = clock_timestamp() + make_interval(secs => $lease) WHERE <family key> AND state='ISSUED' AND lease_owner=$owner AND lease_expires_at > clock_timestamp()`.
  It runs (a) once per row before `process_row` for that row's family — if the current ticket was not
  renewed, the lease was lost: the worker stops that family (`lost_lease += 1`) and does not settle —
  and (b) **for every family the pass claimed, every `lease_secs / 3`, in the background for the whole
  pass** (review 2026-09-29 P0, see "Review fixes"). With (a) alone, a family processed after
  `lease_secs` into the pass had already expired. The lease is therefore a liveness signal, not a time
  budget, and is not tied to `BATCH`.
- **settle** (DONE / SKIPPED_BY_POLICY / FAILED): today's `settle_row` UPDATE plus
  `SET lease_owner = NULL, lease_expires_at = NULL` plus fence
  `AND lease_owner IS NOT DISTINCT FROM $owner AND attempts = $attempts`. There is no
  `lease_expires_at > now()` in the fence, per the `jobs.rs:417-423` lesson (ADR-0036 D4). 0 rows is
  `lost_lease`, not an error.
- **retry** (transient):
  `SET lease_owner=NULL, lease_expires_at=NULL, error_class=$class, next_attempt_at = clock_timestamp() + make_interval(secs => least($base * power(2, attempts - 1), $max))`
  with the same fence. `error_class` on an ISSUED row carries the last transient class, and the next
  DONE settle writes NULL over it.
- **pending release** (distill race): the same as retry but `attempts = attempts - 1, next_attempt_at = NULL`,
  so a distill delay costs no attempt.

### D-E — Error taxonomy: bounded transient retry, immediate permanent failure

`RowTerminal` gains `Retry(&'static str)`. The mapping per existing failure site is below; new class
names are in bold.

| site / cause | class | error_class |
|---|---|---|
| sqlx `Io`, `PoolTimedOut`, `PoolClosed`, SQLSTATE `08*`, `40001`, `40P01`, `55P03`, `57P*`, `53*` | transient | existing `db_*_failed` |
| other sqlx errors (data / constraint) | permanent | existing `db_*_failed` |
| embed `ProviderRateLimited`, `ProviderTransient`, `DependencyUnavailable`, `RateLimited`, `QuotaExhausted`, `CostBudgetExceeded`, `WaitingKey`, `Internal` | transient | `embedding_failed` |
| embed `ProviderPermanent`, `InvalidInput`, `Forbidden`, `Unauthorized`, `EntitlementRequired`, `TenantBoundary` | permanent | **`embedding_rejected`** |
| short embedding batch | transient | `embedding_batch_empty` |
| vector length ≠ configured dimension | permanent | `embedding_dimension_mismatch` |
| `CardBuildOutcome::Unbuildable` | permanent | `card_unbuildable` |
| secret scanner process failure (`DependencyUnavailable`) | transient | `secret_scan_failed` |
| secret scanner verdict: gitleaks finding (`Forbidden`) or unsealable card (`InvalidInput`) | permanent | **`secret_scan_rejected`** |
| Qdrant `Transport(_)` (connect/DNS/timeout/**ExpiredPermit**), HTTP 404/408/409/429/5xx, `UnexpectedResponseShape`, cell-registry refusals | transient | `qdrant_upsert_failed` / `qdrant_delete_failed` |
| Qdrant HTTP 400/422 (payload / schema / vector dimension) | permanent | **`qdrant_upsert_rejected`** / **`qdrant_delete_rejected`** |
| Qdrant `InvalidCollectionName` | permanent | **`qdrant_upsert_rejected`** |
| registry `SourceNotLive`, `SourceChanged`, `RegistryRaceLost`, `Db(transient)` | transient | `registry_failed` |
| registry `PointIdCollision`, `IdentityAlreadyBound`, `CrossTenant`, `CrossWorkspace`, `InvalidInput`, `MissingAuthenticatedUser`, `Db(permanent)` | permanent | `registry_conflict` / `registry_failed` |
| visibility not confirmed | transient | `visibility_not_confirmed` |
| outbox FAILED / no visible memory | permanent (unchanged, 0167's retirable classes) | `distill_failed` / `no_visible_memory_record` |

The Qdrant rule lives in a helper, `QdrantTransportError::is_transient(&self) -> bool`, in `qdrant.rs`.
The sqlx and ErrorCode rules live next to their only callers in `projection_worker.rs`.
The scanner row was split by the implement pass: the 2026-09-29 rehearsal (run #3) retried a
gitleaks finding six times to `transient_exhausted` under the design's single "scanner failure =
transient" row; a finding is the same verdict on every retry.

**Dependency outage does not spend attempts (review 2026-09-29 P1).** The transient classes that say
"a dependency every ticket needs is unavailable" — `qdrant_upsert_failed` / `qdrant_delete_failed`
(transport, 5xx, 429, a refused permit), `embedding_failed` (429/5xx, `QuotaExhausted`,
`CostBudgetExceeded`, key pending), `secret_scan_failed`, `db_*` (lost connection) — are outage
classes. The process keeps one flag, `dependency_down`: the first outage failure after a `DONE` is
charged like any transient and sets it; while it is set, further outage failures are released with
the attempt given back (`attempts = greatest(attempts - 1, 1)`, still backing off, never
`transient_exhausted`); the next `DONE` clears it. A ticket-specific transient
(`registry_failed`, `visibility_not_confirmed`, `embedding_batch_empty`) always spends. So an outage
or a budget window of any length parks tickets instead of FAILing them, and a ticket that fails
while others succeed is still bounded by `MAX_ATTEMPTS` (ADR-0048). Floor 1 keeps every failed
ticket visibly a retry (`RETRY_PREDICATE`, `attempts >= 1`) and its backoff at least `BASE`.

Transient with `attempts >= MAX_ATTEMPTS` (and not an uncharged outage retry) becomes FAILED with **`transient_exhausted`**. A claimed ticket
with `attempts > MAX_ATTEMPTS` (a worker that dies on this ticket every time) is settled
`transient_exhausted` before processing. Retry is bounded, because pure retry was rejected in
ADR-0048.

**What FAILED retires (deviation from the card text, with evidence).** "FAILED → retire the point" is
implemented as a compensation for the failing memory, not as "retire every memory of the ticket". A
ticket binds an **Evidence**, and `resolve_memories` returns ALL memories of that Evidence
(`projection_worker.rs:325-337`). Retiring all of them on one memory's failure would delete valid live
points of sibling memories, with recall losing memories that nothing was wrong with. The rules are:
(a) ADR-0049 retirement of dead memories runs first and is unchanged; (b) when `finish_row` fails
**after** its upsert landed (the registry refused, as in D-A's race), it deletes that deterministic point
id before returning (`delete_points` with `CorrectionDeleteSupersede`), and if the delete fails the row
is transient; (c) nothing else is deleted. The failing memory is therefore left with no point that this
attempt wrote.

### D-F — `humaux-retrieval-worker --serve` (and `--run-once` = one pass of the same code)

The shape is `bins/consolidation-worker/src/main.rs:127-199`: parse → validate → connect →
`Shutdown::install()` (both signal handlers before the first pass; ~30 lines copied, because a bin
cannot import another bin) → `loop { pass; select!(sleep(poll), shutdown) }`. The signal is observed
**between** passes only; a pass settles or releases everything it claimed. The supervisor's grace must
exceed one pass: BATCH × worst-case ticket time (embed timeout + 3 × Qdrant 10 s + PG).

One pass is `projection_worker::run_claimed_pass(&shared, &pass_cfg)`:
unplaced count → claim → group by `StreamKey` (claim order kept) → per family: build `ProjectionWorkerDeps`
from the ticket's key and placement → per ticket: exhaustion check, heartbeat, **fresh permit**
(`authorize_cell_access` per ticket, which is local and cheap; the 60 s TTL literal is otherwise outlived
by any resident process), `process_row`, then settle/retry/release → `advance_prefix` once per family.
Output is one log line per pass:
`claimed= done= skipped= failed= retried= refunded= pending= lost_lease= placement_missing= placement_invalid= claim_ms=`.
`claim_ms` is the ADR's claim latency sample.

- New §78 keys, required, no defaults (bin `required`/`parse`, the ADR-0036 precedent; the typed
  registry has no retrieval-worker table): `HUMAUX_RETRIEVAL_WORKER_POLL_INTERVAL_SECS`,
  `_LEASE_SECS`, `_PER_TENANT_CAP`, `_MAX_ATTEMPTS`, `_BACKOFF_BASE_SECS`, `_BACKOFF_MAX_SECS`.
  `_BATCH` is reused as `p_limit`. Validation: all > 0 and `BACKOFF_MAX_SECS >= BACKOFF_BASE_SECS`.
- Deleted: `HUMAUX_RETRIEVAL_WORKER_TENANT_ID`, `_SCOPE_KIND`, `_SCOPE_ID`, `_QDRANT_COLLECTION`,
  and the `SharedFallback` block. `--run-once` with nothing claimable exits 0.
- `lease_owner = "humaux-retrieval-worker/<uuid v7 per process>"`.
- `--readyz` and `--serve-rpc` are unchanged. The library `run_once(&deps, batch)` stays for the
  existing adapter tests and `bins/private-worker/tests/distill_hop_e2e.rs` (outside the allowed files).
  Its read adds E and returns `attempts`. It settles with fence `lease_owner IS NULL`, and its retry
  writes `attempts + 1`, because it has no claim.

### D-G — P1-15 indexes: one CONCURRENTLY statement per file, run outside a transaction via a manifest key

`migrate` gains the manifest key `transaction = "none"`, read by `migrate.rs` itself from the manifest
text (`toml`), so `migration_rehearsal::Manifest` is untouched. The rules:

- The only accepted value is `"none"`; anything else is refused.
- A `"none"` file must contain `CONCURRENTLY`, so it is not a way to escape atomicity for ordinary DDL.
- Sequence: precheck (autocommit) → body (`batch_execute`, autocommit, simple protocol) → postcheck →
  ledger INSERT. PostgreSQL itself refuses `CONCURRENTLY` in a multi-statement body, because a
  simple-query string runs as an implicit transaction block. The one-statement rule is therefore
  enforced by the server, not re-parsed.
- The files are not atomic with the ledger. A failed build leaves an INVALID index. The rerun's precheck
  (`to_regclass(<name>) IS NULL`) refuses and names it, and the manifest's `rollback_or_forward_fix`
  gives the exact `DROP INDEX CONCURRENTLY IF EXISTS <name>`. Drift is visible, never silent.
- The advisory lock HXMIGRAT and `lock_timeout` still apply. CIC's waits honour `lock_timeout`.

Files (exact P1-15 list, audit lines 327-348, plus the claim index of D-C):
0178 `ops.outbox (tenant_id, commit_seq) UNIQUE WHERE commit_seq IS NOT NULL`;
0179 `ops.outbox (tenant_id, evidence_id) WHERE evidence_id IS NOT NULL`;
0180 `ops.outbox (tenant_id, event_type, commit_seq) WHERE status IN ('PENDING','PROCESSING') AND evidence_id IS NOT NULL` (the distill claim, `distill_repo.rs:189-196`);
0181 `private.memory_evidence (evidence_id)`;
0182 `ops.jobs (job_type, priority DESC, next_retry_at, created_at) WHERE status IN ('PENDING','RETRY_WAIT','PROCESSING')` (the 0164 claim);
0183 `private.memory_records (superseded_by) WHERE superseded_by IS NOT NULL`.

### D-H — DM-7: one transactional FORWARD_ONLY file

0184 runs `ALTER TABLE private.context_bindings VALIDATE CONSTRAINT` for
`context_bindings_created_by_present`, `context_bindings_memory_id_present` and
`context_bindings_scope_id_present`. Dev has 47 rows and 0 violators (read 2026-09-29). The precheck is
"the three exist AND no row violates their exact negation". The postcheck is "all three convalidated".
`SET NOT NULL` (the audit's second half) is not part of card 27.

### D-I — The e2e harness is a rehearse.sh v3 step, not a new xtask subcommand

The gate needs the full deployment: gateway, both private-worker modes, retrieval `--serve-rpc`,
consolidation `--serve`, the new `--serve`, real MiniMax distill and real DashScope/Qdrant.
`rehearse.sh` already owns seeding, the `start_*` definitions, `own_signal` / pidfile process ownership
(the 2026-09-09 incident rule), the MCP helpers and the `assert_eq` verdict. An xtask subcommand would
re-implement about 250 lines of env wiring and spawn/kill discipline in Rust. The soak already splits
the work this way: the launcher owns processes, xtask grades. The step is named
`projection_serve_multi_tenant` so its assertion lines are greppable.

## Implementation notes (card 27 implement pass)

Where the code differs from the design text above, and why:

- **The claim takes a `ClaimFamily`**, not a `RetrievalFamily`. `stream_repo::ClaimFamily::of(family)`
  derives the triple and the placement family from the one `RetrievalFamily` (card 21's single
  source, `PassConfig.claim`); its fields are open so the real-PG tests claim a throwaway
  `projection_version` (`card27-test-<uuid>`) that no dev ticket carries — the claim is cross-tenant,
  so tests can only be isolated by family.
- **Per-ticket permits come from a closure** (`SharedProjectionDeps::mint_permit`), which the binary
  closes over its cell registry and the pre-existing 60 s TTL. Holding the registry in the adapter
  would have made `adapters::projection_worker` an importer of `infra-cell::resource`, a header
  outside the card's files.
- **The per-row code reads a borrowed `RowCtx`**, built from `ProjectionWorkerDeps` by the legacy
  `run_once` and from `SharedProjectionDeps` + the claimed ticket by `run_claimed_pass`.
  `RetrievalWorkerDbPool` is not `Clone` (G6-DB1) and `ProjectionWorkerDeps` must keep its shape
  (`bins/private-worker/tests/distill_hop_e2e.rs` builds it literally), so a per-family owned deps
  value was not an option.
- **Legacy `run_once` applies no backoff and no `max_attempts`.** Its read takes only unleased
  rows whose backoff has elapsed; a transient row is returned with `attempts + 1` and
  `next_attempt_at = NULL`. It has no driver loop left (the binary's `--run-once` is one
  `run_claimed_pass`), so a policy of its own would be dead configuration.
- **The compensating delete skips `PointIdCollision`.** In that case the deterministic id IS
  registered — to another source — and deleting it would remove that source's live point.
- **The claim's `tenant_id` is read once.** `RETURNS TABLE` has one `tenant_id` column (ticket =
  placement by the join); `placement_repo::from_row` (now `pub(crate)`) parses the placement and the
  stream key takes its typed tenant from it.
- **`--serve` connects lazily.** The pool and the embedder's disclosure pool are connected inside
  the loop and kept; a database that is down is a logged `projection pass failed` and the next poll
  retries (`bins/retrieval-worker/tests/serve_drain.rs`). Configuration errors still exit non-zero
  before any claim.
- **Removed with the env pins:** `humaux-retrieval-worker` no longer depends on
  `humaux-projection` (its only use was the `StreamFamily` of the deleted `SharedFallback` block;
  `Cargo.toml`/`Cargo.lock` lose one line, and the two `projection` headers lose the importer), and
  `xtask e2e-seed` no longer prints `HUMAUX_RETRIEVAL_WORKER_{TENANT_ID,SCOPE_KIND,SCOPE_ID,QDRANT_COLLECTION}`.
- **The 120 s assertion is `projection_lag_within_120s` — the card's literal gate is reported, not
  graded.** Put → DONE is dominated by live MiniMax distill (one resident distiller), which card 27
  does not change; the assertion requires every one of the 60 tickets settled and the per-ticket
  projection lag (distill complete → ticket DONE, §42) ≤ 120 s. It was first named
  `all_tickets_done_within_120s`, which read as the verbatim gate while grading something else
  (review 2026-09-29 P1); renamed. The literal "all tickets DONE within 120 s" of the puts is
  printed as `GATE-LITERAL all_tickets_done_within_120s_of_put: MET|NOT MET (put->DONE n p95 max)`,
  outside the verdict: it is NOT MET on this deployment (Measurements) and is a main-line decision
  (waive, or fund distill throughput), not something this ADR redefines.

- **Fallout fixed in the same change.** (a) `xtask soak` labelled a stream by
  `{tenant}|domain|kind|version` — no scope — so with two workspaces per tenant it compared one
  workspace's watermark with the other's (53 false `watermark_monotonic` regressions in the first
  3 × 2 soak); the label now carries `(scope_kind, scope_id)`. (b) 0178's UNIQUE
  `(tenant_id, commit_seq)` refused a test fixture (`exact_completeness_eval`) that hand-picked
  commit identities 1..=12 / 700 twice in one tenant; production draws them from the global
  `ops.commit_seq_seq` and never collided, so the fixture now does too. (c) `forget_repo`'s
  frozen-column pin gains the four 0176 columns. (d) `e2e-seed --teardown` deleted tenants under
  `session_replication_role = replica`, which skips 0164's `ON DELETE CASCADE`, and left every
  torn-down tenant's `ops.jobs` rows orphaned; it now deletes them explicitly. (e) review-fix chain
  2026-09-29: the soak's content marker was a random hex nonce; the live distiller copied one into
  a key claim, its `3831887` tripped the scanner's deterministic phone rule, the ticket settled
  `secret_scan_rejected` (correctly, permanent) and pinned its stream's prefix, so
  `projection_promoted` went red. Soak markers (content nonce and lane sentinel) now spell hex
  digits as letters (`soak::digit_free`, unit-tested).
- **Rehearsal lessons written into the harness.** A derived-work backlog left by one run sits,
  FIFO, in front of the next run's jobs (audit P1-16); the step therefore runs a resident
  consolidation worker and asserts `pst_leaves_no_derived_backlog`. Harness contents carry no uuid
  (gitleaks flags one inside a distilled memory). The short soak runs the runner's chaos hook
  first and uses card 24's load shape (`SOAK_SESSIONS=1 SOAK_THINK_MS=10000 SOAK_DRAIN=300`).

## Review fixes (2026-09-29, same card)

| finding | root cause | fix | pinned by |
|---|---|---|---|
| P0 `projection_worker.rs` run_family: the tail of a slow batch loses its leases and is exhausted unattempted | the heartbeat renewed only the family in hand; families claimed in the same pass waited un-renewed, expired, were re-claimed (+1 attempt each time) and after `MAX_ATTEMPTS` were settled `transient_exhausted` by the pre-processing check; `supervision.md` sized the lease "above one ticket's worst case" | `run_claimed_pass` runs a background heartbeat (`lease_secs / 3`) over every claimed family for the whole pass, concurrently with the work (`while_running`, std-only; the timer is the bin's `tokio::time::sleep`, injected as `SharedProjectionDeps::sleep` like `mint_permit`); supervision.md rule rewritten | `projection_claim::a_family_waiting_behind_a_slow_one_keeps_its_lease` (1.5 s lease, 3 s first family; heartbeat removed ⇒ `lost_lease=1`, red) |
| P1 `stream_repo.rs` claim_issued: one unparsable placement row poisons every claim that includes it | the claim commits leases (and `attempts+1`) before the rows are parsed, and the parse collected with `?`, so one bad row failed the whole committed batch before any settle or exhaustion check | rows parsed one by one (`parse_claim_rows`); an unparsable placement becomes an `UnplaceableTicket`, parked by `release_for_retry(spend = false)` with backoff and class `placement_invalid` (counted `placement_invalid=`); co-claimed tickets are processed; a row whose stream key does not parse (definer drift) is logged and left to lease expiry | `stream_repo::claim_parse_tests::one_unparsable_placement_row_costs_only_itself` (claim-shaped literal rows, one unknown `promotion_state`; collect-with-`?` ⇒ red) |
| P1 `projection_worker.rs` classification: a long outage / budget window FAILs every ticket written during it | every transient spent an attempt, including a dependency outage that says nothing about the ticket | dependency-outage refund (D-E) via `SharedProjectionDeps::dependency_down`; `release_for_retry` takes `spend` | `projection_claim::outage_failures_after_the_first_spend_no_attempt` (Qdrant closed port; second ticket claimed at `MAX_ATTEMPTS` stays ISSUED attempts 5; refund removed ⇒ `transient_exhausted`, red); `transient_at_max_attempts_settles_failed_transient_exhausted` still pins the charged path |
| P1 `rehearse.sh` the 120 s assertion was a redefined gate under the verbatim name | naming | renamed `projection_lag_within_120s`; literal printed as `GATE-LITERAL …`, outside the verdict | rehearsal log |
| uncaught `f_delete_p1_15_index`: dropping 4 of 6 P1-15 indexes left the EXPLAIN gate green | at dev data sizes the planner has another path; a plan cannot pin an index | catalog pin: `crates/adapters/tests/hot_path_indexes.rs` (all 7 of 0177–0183 present, `indisvalid`, `indisready`, exact `pg_get_indexdef`, and each migration file still creates its name) + rehearsal assertion `p1_15_indexes_present_valid_ready(n=7)` | fault run on a throwaway schema copy: each of the 7 drops ⇒ red naming the index; `indisvalid=false` ⇒ red |

Review-fix chain (2026-09-29 12:32, `gates_card27_reviewfix.log`, fail=0; rehearsal VERDICT 57/0,
soak exit 0): claim wall time n=309 passes p50 2 ms / p95 10 ms / max 160 ms; projection lag
(distill done → DONE) n=60 p50 2.1 s / p95 2.8 s / max 2.9 s; put → DONE n=60 p50 81.2 s / p95
160.2 s / max 163.4 s — `GATE-LITERAL all_tickets_done_within_120s_of_put: NOT MET`; recall without
token 104/104 memories; outage tickets 4/4 at attempts 1, 0 FAILED, all DONE after restore;
`p1_15_indexes_present_valid_ready` 7/7.

## Consequences

- P1-1: one tenant-free resident process projects every placed tenant. The runbook's fifth process
  replaces the rehearsal loop.
- C4: a transient infrastructure blip no longer makes a memory permanently invisible, and neither
  does a dependency outage of any length (uncharged outage retries). FAILED means permanent or
  `transient_exhausted`.
- Unchanged, deliberately: the §15.4 prefix and the FAILED pin on it; 0167 retirement as the ops act
  that moves it; the serving switch and first activation as operator acts until card 28.

## Rejected

Each entry: alternative, then why not.

- **RETRY_WAIT (or PROCESSING) state for projection tickets** - It needs a CREATE OR REPLACE of the 0011/0167 guard trigger (new edges ISSUED->RETRY_WAIT for role_retrieval_worker and RETRY_WAIT->ISSUED for the owner). It also changes the §6.2.2 verbatim triple and the byte-pinned rls_check.rs:3567 literal. Keeping ISSUED and writing only columns passes the guard's OLD.state=NEW.state early return with zero trigger change. RETRY is a derived predicate.
- **Sibling claim table (projection.ticket_claims)** - Finding ISSUED rows across tenants still needs the owner arm on stream_log. It adds a second row per ticket that must settle atomically with the first, plus a new RLS policy, a new grant row and a new tenant FK. stream_log is already updated in place (state, error_class, settled_at, retired_*).
- **Reuse ops.jobs + ops.claim_derived_work with an AFTER INSERT enqueue trigger on stream_log (the §31 stream_key/stream_seq locator)** - It doubles the bookkeeping: the job and the ticket must settle together. The invoker trigger fails for role_consolidation_worker, a stream_log issuer per 0144 whose INSERT on ops.jobs the 0164 postcheck forbids, so it would need a definer trigger or a new grant. It needs a backfill of 145+ ISSUED tickets. Family exclusivity, the per-tenant cap and the placement predicate would still need a new claim function.
- **Per-family pg_try_advisory_xact_lock inside the single claim statement (plan v2 wording)** - The statement's snapshot predates the lock. A family whose previous claimer committed and released between our snapshot and our try-lock still looks idle, so two workers can process one family (D-A's orphan-point race). The correct per-family version needs a plpgsql loop with one statement per family. That is kept as the ponytail upgrade path.
- **families_in_flight NOT EXISTS predicate with no lock** - Under READ COMMITTED, two concurrent claimers both miss each other's uncommitted leases, and SKIP LOCKED only stops them taking the same row, not the same family.
- **Definer heartbeat function projection.renew_ticket_lease(...)** - role_retrieval_worker already holds table-level UPDATE on stream_log (0011:253). ADR-0043's precedent is the worker's own fenced per-row UPDATE; a definer would add a grant and a function for nothing.
- **Placement cache per tenant x family for the poll interval** - The claim already JOINs tenant_placements (the no-placement exclusion) and RETURNING hands the row back. Placement is then fresh at every claim with zero extra queries, and there is no staleness to reason about.
- **On FAILED, retire every memory of the ticket's Evidence (literal reading of "FAILED -> retire point")** - A ticket binds an Evidence and resolve_memories returns ALL of its memories (projection_worker.rs:325-337). One memory's permanent failure would then delete valid live points of the others.
- Also rejected (reasons in D-G / D-I): an xtask e2e that owns processes; migrate auto-detecting CONCURRENTLY; a filename flag; a GUC policy arm.

Added by the 2026-09-29 review fixes:
- **Tying `LEASE_SECS` to `BATCH × worst-case ticket` in config** — the worst case is unbounded in
  practice (PG pool acquire, provider timeouts, retries inside Qdrant HA profiles); the background
  heartbeat makes the lease independent of both.
- **Renewing all families only at each ticket start** (no timer) — still loses a lease whenever a
  single ticket outlives `LEASE_SECS`; the concurrent heartbeat does not.
- **Charging every outage failure** (the card's literal "transient → attempts+1") — a Qdrant or
  provider outage, or a daily budget window, longer than the backoff series (30+60+120+240+300 s =
  12.5 min at the rehearsal config) FAILed every ticket written during it: the invisibility C4 exists
  to remove.
- **Stopping the pass at the first outage failure / pausing claims** — one tenant whose placement
  collection 404s would, as the first ticket of every pass, starve every other tenant; uncharged
  retries with backoff give the same protection without the starvation.
- **FAILED `placement_invalid` for an unparsable placement row** — the only way to get one is
  deploy skew (every parsed column is CHECK-constrained); FAILED would pin the §15.4 prefix and make
  every ticket of the tenant permanently invisible over a rolling deploy. Parked instead.
- **An EXPLAIN-only index gate** — see "Review fixes" (uncaught fault).

## Measurements (implement pass, 2026-09-29, dev DB `humaux_thread_dev`, live DashScope + MiniMax, real Qdrant)

Source: rehearsal run 5 (`delivery-cards-20260903/card27_rehearsal_evidence/run5/`, REHEARSAL VERDICT
56 passed / 0 failed, step `projection_serve_multi_tenant`), one `--serve` process with no tenant
env, `BATCH=16 PER_TENANT_CAP=8 LEASE_SECS=60 POLL_INTERVAL_SECS=1 MAX_ATTEMPTS=6
BACKOFF_BASE_SECS=30 BACKOFF_MAX_SECS=300`, 3 tenants × 2 workspaces, 60 `remember.put` in 2.1 s.
Run 2 (same step, no soak) is given beside it where it was measured.

| measure | n | unit | p50 | p95 | max | source |
|---|---|---|---|---|---|---|
| `claim_issued_tickets` wall time (worker `claim_ms`, one sample per pass) | 316 passes (run 2: 331) | ms | 3 (2) | 8 (11) | 48 | `projection-runner.log` pass lines |
| distill completion → ticket DONE (projection lag, §42; the runner's share) | 60 tickets | s | 2.1 (1.9) | 3.0 (2.8) | 3.4 | `memory_records.created_at` / outbox `processed_at` → `stream_log.settled_at` |
| remember.put → ticket DONE | 60 tickets | s | 70.5 (69.5) | 143.9 (133.9) | 146.1 | put time (client) → `settled_at` |
| remember.put → visible in recall, no `consistency_token` | 100 memories of the 60 puts (run 2: 103) | s | 70.8 (67.6) | 143.9 (134.0) | 146.9 | recall poller, each memory queried by its own key claim, 8 in flight |
| SIGKILL mid-batch → the killed batch DONE again | 10 tickets | s | — | — | 56 after restart | lease expiry (60 s) + re-claim |
| Qdrant unreachable (closed port) → DONE after restore | 4 tickets | s | — | — | 55 after restore | backoff 30 s → 60 s |

Put → DONE and put → visible are dominated by live MiniMax distill with ONE resident distiller
(≈0.43 Evidence/s here): the runner settles a ticket 2–3 s after its distill completes. The card's
literal "all tickets DONE within 120 s" of the puts is therefore NOT met by the deployment
(p95 143.9 s, max 146.1 s, n=60); the assertion (then `all_tickets_done_within_120s`) graded what card 27
owns — every ticket settled and the per-ticket projection lag ≤ 120 s (max 3.4 s) — and is named
`projection_lag_within_120s` since the review fix; the literal gate is a `GATE-LITERAL` line. Meeting the
literal number needs distill throughput (a second distiller, or audit P1-16), not projection.

EXPLAIN plans (`auto_explain` nested plans for the two definers, `EXPLAIN (FORMAT JSON, VERBOSE)`
for the overlay and the distill claim; hot = a `Seq Scan` on `ops.outbox`, `ops.jobs` or
`private.memory_evidence`):

| plan | before 0177–0183 | after | evidence |
|---|---|---|---|
| `projection.claim_issued_tickets` | Seq Scan stream_log ×4, Seq Scan **ops.outbox** (throwaway DB built to head, 0177 dropped — the function does not exist before 0176, so there is no dev before-plan) | Index Scan `stream_log_issued_claim_idx` ×3 + `stream_log_pkey`, Index Scan `outbox_tenant_commit_seq_uidx`; only `tenant_placements` (49 rows) seq-scanned | `card27_evidence/claim_before_plans_throwaway_no_0177/`, run 5 `pst/plans/` |
| `ops.claim_derived_work` (0164) | Seq Scan **ops.jobs** (7,986 rows) | Bitmap Heap Scan via `jobs_claim_active_idx` | `card27_evidence/before_plans/`, `after_plans/` |
| RYW overlay (`retrieve.rs`) | Seq Scan **ops.outbox** | Index Scan `outbox_tenant_evidence_idx`, Index Scan `memory_evidence_evidence_idx` (both captures) | same |
| distill claim (`distill_repo.rs`) | Seq Scan **ops.outbox** | Index Scan `outbox_evidence_claim_idx` | same |

Hot seq scans: 3 before (dev, pre-`migrate`), 0 after (n = 4 plans, asserted by
`no_seq_scan_on_outbox_jobs_memory_evidence`), with `ANALYZE` only — never `enable_seqscan=off`.

### Main-line reading of the 120 s line

The card's "all tickets DONE within 120 s" is read on the main line as two conditions that card 27 owns:
(1) all tickets settled (60/60) and (2) projection lag p95 < 120 s (measured p95 3.0 s, max 3.4 s, n=60).
The put->DONE p95 of 143.9 s (run 2: 133.9 s; max 146.1 s) is bounded by live distill throughput with one
resident distiller and is owned by card 32 (distill throughput), not by this ADR. The literal figure stays
visible as the `GATE-LITERAL` line.
