# ADR-0062 — Resident maintenance daemon (`humaux-maintenance --serve`) and its bounded doors

- Status: Accepted — card 35, eight serial slices plus slice 7b and a review fix pass, all implemented (2026-10-04 /
  2026-10-05, uncommitted; the main line commits and runs the chain, the serial lane and the 30-minute rehearsal).
  - S1: the resident skeleton shared with `health serve`, the `--serve` arm with its own ops listener, migration 0214
    (receipt table and tenant page), `sweep_lost` fixed at the root and wired, the two existing reapers wired.
  - S2: migrations 0215–0218 (three CONCURRENTLY indexes and the four purge doors), the single-statement manifest
    page read, `sweep once`, the rls-check closed set, `cargo xtask sweep-confirm-tokens` retired.
  - S3: the IPv6 /64 pre-auth rate key. S4: migration 0219, `projection.stream_point_ledger` v2.
  - S5: migration 0220 (the Q drain: `projection.ticket_reissues` and the reissue door), the daemon's `reissue` task.
  - S6: migration 0221 (`ops.jobs.auto_redrives` and the schema-failed re-drive door), the daemon's `redrive` task
    with its §77 row.
  - S7 / 7b: migration 0222 (`ops.selection_snapshots.continues_before`), the one-statement capped manifest with
    keyset segments, the required gateway keys `HUMAUX_GATEWAY_ENUMERATION_TTL_SECONDS` / `_MANIFEST_CAP`; the
    300 ms first-page bar is not met and moves to card 35b under ruling E12 (§Known limits).
  - S8: the D-S counters through the card-34 exposition, the eighth ops pair, the docs of D-T, the rehearsal's
    throwaway maintenance database with its receipts balance, the daemon in the soak's kill rotation.
  - Fix pass (2026-10-05, review findings): `sweep_lost` spares a ticket whose Evidence is still being distilled;
    migration 0223 (`stream_log.lost_at`, the reissue cool-down counted from the LOST transition); the gateway's
    page read goes through the one page statement; `--serve` answers 503 until its first clean cycle; the §42
    `MaintenanceCountersAbsent` row; SECURITY DEFINER trigger functions join the closed-set scan; the rehearsal
    reads the daemon's database through the daemon's own DSN; T-K5's red witness re-run with the real fault.
- Spec: Baseline §4.2, §6.2.1, §6.2.2, §15.2, §22.1, §41.2, §42, §48.1, §73.2, §78.1; ADR-0037 (drain), ADR-0043
  (sweeps), ADR-0052 (ticket claims), ADR-0057 D-F (lag threshold), ADR-0061 D-B/D-D (ops listeners); design
  `card_35_design.md` with its main-line rulings (2026-10-04: E1–E13).

## Context

`stream_repo::sweep_lost` had no caller and was stale against ADR-0052; the quota and provider-budget reapers had no
caller; the confirm-token sweep was an xtask that needed a test DSN (OPS-6); confirm tokens, enumerate snapshots,
rate buckets and terminal jobs were never deleted; `memory.enumerate` materialized every id with one INSERT each
(P1-14); the pre-auth rate key was the full IPv6 address (SEC-6). Nothing ran on a schedule.

## Decisions

### D-A  A second resident arm with its own listener; `health serve` unchanged
`humaux-maintenance --serve` is a resident mode of the same binary. `bins/maintenance/src/resident.rs` holds what
both resident modes share: the SIGTERM/SIGINT latch (installed before any work), the per-statement timeout on the
pool DSN, the last-outcome record behind the 200/503 verdict, the `/status` identity document and the loopback
listener wiring. The daemon never samples health and `health serve` never purges. Readiness is `GET /metrics` on
`HUMAUX_MAINTENANCE_SERVE_METRICS_ADDR`: 503 with the reason and age until the first cycle has finished clean
(`Last::pending()`: the listener binds before the first cycle, so "booted" must not read as ready), when the last
finished cycle had a failed call, or when no clean cycle finished within 3 × `CYCLE_SECONDS`; 200 means one clean
cycle, never stale or zero counters. Only the first connect is boot-fatal; every later failure is logged, counted
and answered 503, and the loop goes on.

### D-B  Loop, cadence and keys (all required, no code default)
One cycle per `CYCLE_SECONDS` tick (`MissedTickBehavior::Delay`), tasks in D-C order from a rotating start. A due
task (its `<TASK>_EVERY_SECONDS`, ≥ `CYCLE_SECONDS`, has passed) processes one tenant page: one transaction and one
door call per tenant. Every statement is bounded by `CYCLE_SECONDS` on the server (`statement_timeout`) and the
client (tokio timeout); a cycle stops issuing calls once `CYCLE_SECONDS` have passed, and the latch is checked
between calls. Keys (prefix `HUMAUX_MAINTENANCE_SERVE_`): `METRICS_ADDR`, `CYCLE_SECONDS`, `TENANTS_PER_RUN`,
`<TASK>_EVERY_SECONDS` / `<TASK>_LIMIT` per task, `LOST_AFTER_SECONDS`, the ages of D-G..D-J
(`CONFIRM_TOKENS_CONSUMED_RETENTION_SECONDS`, `RATE_BUCKETS_IDLE_SECONDS`, `JOBS_DONE_RETENTION_SECONDS`,
`JOBS_DEAD_RETENTION_SECONDS`), `REISSUE_COOLDOWN_SECONDS`, `REDRIVE_COOLDOWN_SECONDS`, plus
`HUMAUX_MAINTENANCE_PG_DSN`. Boot refuses unless `LOST_AFTER_SECONDS` is greater than the peer key
`HUMAUX_GATEWAY_PROJECTION_LAG_SECONDS`, read under its own name (ADR-0057 D-F); the peer key
`HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS` is passed to the jobs door (D-J). Every age is sent as an
`interval` and compared with the DB clock inside the door.

### D-C  The task set (the closed `MaintenanceTask` list, in cycle order)
`lost` (`stream_repo::sweep_lost`, a direct UPDATE as `role_maintenance`, which the 0011/0167 guard requires),
`quota_reservations` (0113 definer), `provider_budgets` (0117 definer), `confirm_tokens`, `selection_snapshots`,
`rate_buckets`, `terminal_jobs` (the D-E doors), `reissue` (D-N), `redrive` (D-P). The system tenant (nil id) is
skipped by the two reapers: both producers and both reapers refuse it, so it can hold no reservation. Adding a task
is one variant, one door and its two keys (coord-lease recipe: §Known limits L18).

### D-D  Tenant enumeration: one narrow owner definer page (0214)
`control.maintenance_tenant_page(uuid, integer)`: owner `role_migration_owner`, SECURITY DEFINER,
`search_path=pg_catalog`, EXECUTE `role_maintenance` only, keyset page of tenant ids strictly after the cursor,
`p_limit <= 0` refused (22023). Every sweep and purge stays per tenant under the caller's GUC. rls-check arm
"ADR-0062 maintenance doors" pins it. Rejected: tenant-less doors on new owner policy arms (every existing owner
definer on those tables would silently turn cross-tenant); a NOLOGIN purge-owner role with table DELETE; enumerating
through `HUMAUX_TEST_PG_DSN`.

### D-E  Purge path: bounded owner DELETE doors, a closed set (S2, rulings E1/E2)
Expiring operational state leaves through four owner SECURITY DEFINER doors in migration 0218, not through "mark"
columns: a mark writes a value already derivable from `expires_at` / `status` / timestamps (ADR-0043), and §48.1's
partition drops never reach these tables, so marked rows would never go. Every door:
- is owned by `role_migration_owner`, `SECURITY DEFINER`, `SET search_path = pg_catalog`, `STRICT`;
- takes `interval` and `integer` arguments only, so no table name or SQL text has a signature to land in;
- asserts the tenant GUC: it reads `current_setting('humaux.tenant_id')` without `missing_ok` and casts it to uuid
  (unset 42704, empty 22P02); the final SELECT reads that value, so the check runs on every call, and every victim
  predicate names the tenant (`ops.jobs` has an owner policy arm, so RLS alone would not hold the definer in);
- refuses a non-positive LIMIT or a negative age with PostgreSQL's own negative-LIMIT error (2201W), raised before
  any row is read;
- is `LANGUAGE sql`, one statement: victim (`ORDER BY age LIMIT … FOR UPDATE SKIP LOCKED`), delete, receipt and
  count are sibling CTEs, so kill -9 commits the delete with its receipt or neither;
- compares every age with the DB clock inside the body; the daemon sends intervals, never timestamps;
- has EXECUTE for `role_maintenance` only, PUBLIC revoked.

Rejected: mark-only columns (above); an independent short-lived owner executor (it needs an owner credential in a
running process, which R-36 forbids, and buys nothing for single-statement deletes); a table DELETE grant (§6.2.1
forbids it, and 0170 already reverted one).

### D-F  Receipts table (0214)
`ops.maintenance_receipts`: FORCE RLS with the tenant clause, `role_maintenance` SELECT only, every other runtime
cell `—` (§6.2.2 column). Written only by the purge doors, in the same statement as the delete, and only when the
delete removed rows (idle tenants do not grow it). `control.operation_receipts` is not reused (it is the gateway's
§34.0.1 replay state). Sweeps, reissues and re-drives are not receipt rows: a LOST ticket carries its own audit
(`state`, `error_class = ORPHANED_PIPELINE_ITEM`, `lost_at`), reissues are `projection.ticket_reissues` rows (D-N),
re-drives are §77 `control.audit_events` rows (D-P).

### D-G  Confirm tokens: one bounded door; the xtask retires
`control.sweep_confirm_tokens(p_consumed_retention interval, p_limit integer)` keeps 0169's predicate verbatim
(`expires_at < now() AND (consumed_at IS NULL OR consumed_at < now() - retention)`), oldest expiry first, with a
receipt. 0218 drops the 1-argument door, leaving one. `confirm_token_repo::sweep_expired` gains `limit`.
`cargo xtask sweep-confirm-tokens` is deleted (`xtask/src/confirm_sweep.rs`, its arm and usage word): its manual
role passes to `humaux-maintenance sweep once`, which needs no test DSN (OPS-6 closed). A thin wrapper was rejected:
two manual doors, one still needing `HUMAUX_TEST_PG_DSN`.

### D-H  Enumerate snapshots and the one-statement page read
`ops.purge_expired_selection_snapshots(p_limit integer)` deletes DB-expired snapshots of the calling tenant with
their items in sibling CTEs. The items → snapshots FK is NO ACTION and is checked at the end of the statement, so no
CASCADE is needed. LIMIT counts snapshots, so one call deletes at most LIMIT × manifest-size items. Index 0215
`(tenant_id, expires_at)`.

Every page read — the gateway's `memory.enumerate` (`authorized_page_in_txn`, every page) and the worker API
(`fetch_page_from_manifest`) — goes through the one caller of `MANIFEST_PAGE_SQL`, `read_manifest_page`: the
snapshot row, its DB-clock liveness `expires_at > now()`, its `continues_before` and the page of items via
`LEFT JOIN LATERAL`, in one statement. No row is `SnapshotNotFound` (worker) / `NotFound` (gateway); `live = false`
is `Cursor(Expired)` / `NotFound`; a fingerprint mismatch is `QueryMismatch` / `NotFound`. One statement reads one
MVCC snapshot, and the purge deletes a snapshot with its items in one statement, so a reader sees the whole
manifest or no snapshot, never an empty page under a promised census. The continuation probe
(`continuation_bound_in_txn`) also requires `expires_at > now()`, so a DB-expired snapshot is refused rather than
continued into a fresh segment. The host-clock cursor check stays as the early refusal; the DB check is the
authority and uses the purge's clock, so no grace key exists. (Fix pass: S2 had moved only the worker path, which
has test callers only; the gateway still ran two statements with a host-clock check.)

### D-I  Rate buckets
`control.purge_idle_rate_buckets(p_idle interval, p_limit integer)` deletes only a bucket idle for longer than
`p_idle` that would be full right now (`tokens + elapsed × refill ≥ capacity`) and whose `consume_rate` advisory key
it wins (`pg_try_advisory_xact_lock` on the same `rate:<tenant>:<kind>:<id>:<op>:<bucket>` text). The consumer
recreates a missing bucket with `tokens = capacity`, which equals the deleted row's refilled value, so a purge never
changes a rate decision. The pre-auth `ip` buckets live under the system tenant, which the tenant page returns like
any other. Index 0216 `(tenant_id, updated_at)`.

**SEC-6, the pre-auth IP rate key (ruling E5; Baseline §73.2).** `quota_repo::preauth_ip_subject` is the only
producer of an `ip` bucket's `subject_id`: the address is canonicalised first (`::ffff:a.b.c.d` is the IPv4
client), IPv4 keys by its full address and IPv6 by its `/64` network, rendered `<net>/64` (e.g.
`2001:db8:1:2::/64`). One IPv6 end site owns a whole /64, so a /128 key gave it 2^64 independent buckets.
Authenticated keys (credential, user, tenant) are unchanged. Existing `/128` buckets are not migrated: they stop
being read and leave through the D-I door once full and idle.

### D-J  Terminal jobs; no outbox door
`ops.purge_terminal_jobs(p_done_retention, p_dead_retention, p_budget_window, p_limit)` deletes `DONE` jobs past
the done retention and `DEAD` / `FAILED` jobs past the dead retention, measured from the latest timestamp the row
carries (`GREATEST(created_at, next_retry_at, lease_expires_at, not_ready_since)`). It always keeps:
- a job with an `ops.distill_calls` row begun inside `p_budget_window`, the exact `ops.admit_distill_budget`
  predicate. The daemon passes the private worker's own `HUMAUX_PRIVATE_WORKER_DISTILL_BUDGET_WINDOW_SECS`, so the
  CASCADE never removes a call the budget still counts, at any retention, zero included;
- a job a `ops.contribution_execution_job_links` row names (that FK is NO ACTION and would abort the statement);
- a DEAD distill job whose Evidence's `EVIDENCE_ACCEPTED` row exists and is not DONE. A DEAD distill job is purgeable
  iff `ops.requeue_dead_distill` would refuse it (ADR-0058 R4), so no re-drive handle is ever lost.

Purging a job frees its `idempotency_key`. That is safe while every producer's key derives from a row created once
(L5). Index 0217 `(tenant_id, created_at) WHERE status IN ('DONE','DEAD','FAILED')`.

**No outbox door (ruling E11).** Every outbox row is read as authority while it exists: private rows through the
stream ledger (stream_log is never deleted, §37.2), the five `PUBLIC_*` types by live guards. The remaining victim
set is empty and has no producer, so the fifth door is not built. Rejected: a purge with the authority types denied
(silently wrong the day a sixth type ships); a consumer-watermark table (the authority readers are triggers).

### D-K  P1-14: a bounded first page (0222; Baseline §22.1 E4 sentence)
- **One INSERT.** `selection_repo::insert_manifest_in_txn` writes a manifest with one
  `INSERT … SELECT … FROM unnest($3::uuid[]) WITH ORDINALITY`; both the gateway mint and the worker's
  `begin_enumeration_snapshot` call it. The per-id loop is gone (gate `c35_one_manifest_insert`).
- **Cap.** `HUMAUX_GATEWAY_ENUMERATION_MANIFEST_CAP` (> 0, required, no default) rides in
  `MemoryEnumerationParams.manifest_cap`. A manifest stores the first `min(cap, |ids|)` ids in `memory_id DESC`
  order; when it truncates, `ops.selection_snapshots.continues_before` records the last stored id.
- **Continuation.** A page that reaches the end of a capped manifest still answers a cursor. On the next call one
  statement (`continuation_bound_in_txn`, under the caller's RLS GUCs, after the cursor's MAC / tenant / expiry /
  fingerprint checks) reads `continues_before` when the snapshot is DB-live and no item lies past the cursor; that
  call then runs the page-1 flow in a fresh `REPEATABLE READ READ WRITE` transaction with
  `memory_records.memory_id < bound` in both faces of the predicate (manifest candidates and
  `CensusInputs.before_id`, bound like `subject_id`). Each segment's census is exact for its own predicate and
  snapshot: page 1's total is the whole universe, a later segment's is its remainder (L12).
- **Fingerprint.** The segment bound does not enter `query_fingerprint`: the fingerprint is the caller-identity
  check on every later page, the bound is read server-side from a MAC-bound snapshot row and never from caller input,
  and recomputing a bound-carrying fingerprint would need each segment's own bound stored (a second column).
- **TTL.** `ENUMERATION_TTL` (`15 * 60` in `gateway::memory`) is the required key
  `HUMAUX_GATEWAY_ENUMERATION_TTL_SECONDS`; `GatewayMcpApplication::with_enumeration(ttl, cap)` carries both, and
  `memory.enumerate` fails closed through `reject_unsupported` until it is set.
- **Rejected.** A count-only census (drops ADR-0041 D-G's set-equality cross-check); a continuation reusing page
  1's frozen total (items from a younger snapshot would make returned versus total lie); refusing above the cap.

### D-L  `sweep_lost` fixed at the root
One UPDATE as `role_maintenance` over a victim subquery `ORDER BY issued_at LIMIT $3 FOR UPDATE SKIP LOCKED`. A
ticket is an orphan only when no pipeline item is in flight for it (§15.2): it excludes a ticket under a live runner
lease, a backing-off ticket (`RETRY_PREDICATE IS NOT TRUE`), a ticket with a legacy in-flight `ops.jobs` row (by
`stream_key` / `stream_seq`), and — fix pass — a ticket whose Evidence's `EVIDENCE_ACCEPTED` outbox row (same
`commit_seq`) is still `PENDING` / `PROCESSING`. That last predicate is 0176's claim hold-back verbatim: the claim
keeps such a ticket back on purpose while the distill job runs, and the 0164 trigger writes DERIVED_DISTILL jobs
without `stream_key` / `stream_seq`, so the legacy jobs test can never see them. LOST carries no `settled_at` (the
0007/0167 CHECK pairs `settled_at` with the terminal states only, and LOST is not one); the sweep records the
transition time in `stream_log.lost_at` (0223), the clock of the reissue cool-down (D-N).

**LOST in card 31's recall (ruling E13).** An orphan first reads as `PROJECTION_LAG`; after the sweep the lag
clears, `points_unsettled` rises by one and `completeness_ratio < 1` until a reissue settles. No degrade code is
emitted for it.

### D-M  Ledger v2 (0219; card 31 debt 2)
`CREATE OR REPLACE FUNCTION projection.stream_point_ledger(...)` with the identical 8-argument signature and the 0189
body plus `AND r.indexable` in the `points_in_flight` (F) and `points_unsettled` (Q) filters; U and L already had it.
0189 is not edited, callers are unchanged, owner / `search_path` / EXECUTE (`role_gateway`, `role_retrieval_worker`)
are re-asserted and the manifest postcheck fails on a body without either fence. `live` stays out of Q: an archived
memory's FAILED unarchive ticket (ADR-0057 limit 13) stays visible as slack until the reissue (D-N). Rejected: a
`_v2` name with a caller switch (two definers, two call sites). `crates/adapters/tests/visible_index_count.rs` now
reads U/L/F/Q through this definer as `role_gateway` (its seeder writes one Evidence, outbox row and PRIMARY memory
per ticket); the hand copy of the ticket terms is gone.

### D-N  Drain Q: `projection.reissue_unsettled_tickets(uuid, interval, integer)` (0220, 0223; card 31 debt 1)
An owner plpgsql SECURITY DEFINER (one call, one transaction), `search_path=pg_catalog`, EXECUTE `role_maintenance`
only, tenant argument checked against the installed GUC, `p_cooldown > 0` and `p_limit > 0` (22023), one caller per
tenant at a time (transaction advisory lock: two concurrent callers would both read the same latest ticket).
- **Candidates.** Per stream of the tenant and per memory its tickets reach (0189's join), the latest ticket `t` is
  FAILED, LOST or RETIRED_FAILED, the memory has no TOMBSTONED and no in-flight ticket there and is indexable (the
  0219 term): exactly 0219's Q, without the caller-visibility narrowing. One ticket per (stream, PRIMARY evidence),
  bound to that evidence, so one ticket covers every memory of an Evidence.
- **Eligibility.** Every class waits out the cool-down, counted from the latest timestamp `t` carries
  (`GREATEST(issued_at, settled_at, lost_at, lease_expires_at, next_attempt_at)`, 0223). For every settled state the
  expression is at least `settled_at`; a LOST ticket has no `settled_at`, and before 0223 its clock was its old
  `issued_at`, so a ticket swept LOST was reissued on the next cycle and, with no runner on the stream, every
  reissued ticket went LOST and was reissued again after `LOST_AFTER` with no cool-down at all (review P1). With
  `lost_at` (written by `sweep_lost`) consecutive reissues of one memory are at least `LOST_AFTER` + cool-down
  apart. LOST rows swept before 0223 have no `lost_at` and keep the old clock. Then RETIRED_FAILED, LOST and the
  transient FAILED classes (`transient_exhausted`, `qdrant_upsert_rejected`, `qdrant_delete_rejected`,
  `registry_conflict`, `registry_failed`) are eligible; every other FAILED class (the deterministic three,
  `embedding_rejected`, any class unknown to the door) only when `t` is not itself a reissue — once, then an operator
  retirement, then once more ("once per retirement").
- **Issue.** 0198's sequence inline on `t`'s own stream: `nextval('ops.commit_seq_seq')`, checkpoint
  `issued_highwater + 1`, `stream_log` ISSUED, `ops.outbox` MEMORY_LIFECYCLE bound to the PRIMARY evidence, one
  `projection.ticket_reissues` row (the marker; `role_maintenance` SELECT, every other cell `—`, FORCE RLS). LIMIT
  counts tickets issued. No old row is touched: the 0167 guard has no edge back to ISSUED. Under ADR-0057 D-M every
  ticket of an Evidence sits on its home stream, so `t`'s stream is the home stream and the gate `one_home_stream`
  keeps one Rust `home_stream`.
- **0223.** `ALTER TABLE projection.stream_log ADD COLUMN lost_at` with CHECK `lost_at IS NULL OR state IN
  ('LOST','TOMBSTONED')`, and `CREATE OR REPLACE` of the door with `lost_at` in its clock (0220's body otherwise
  verbatim; 0220 is not edited). `role_maintenance` writes the column through its existing table-level UPDATE (0011),
  so no §6.2.2 cell changes. The design had rejected a `stream_log` column as the reissue *marker*; `lost_at` is a
  transition clock, not the marker, which stays `projection.ticket_reissues`. Because it is a fact about a transition
  (the class of 0167's `retired_at`/`retired_by` and 0176's lease/attempt columns), not a counter derivable from
  `state`, the §37.2 frozen-column pin (`forget_repo.rs::stream_log_has_exactly_the_frozen_columns`, G80-25) was
  widened to 19 columns by name (Baseline §15.1/§15.2/§37.2, ADR-0052 addendum 2026-10-05).
- **Daemon.** Task `reissue`, keys `REISSUE_EVERY_SECONDS`, `REISSUE_LIMIT` and `REISSUE_COOLDOWN_SECONDS` (> 0),
  all required; `maintenance_repo::reissue_unsettled_tickets` makes the call in one tenant-GUC transaction like
  every door.
- **Rejected.** Calling `memory_governance_repo::home_stream` from the daemon (it is `pub(crate)`, and its write path
  needs INSERT grants `role_maintenance` must not get). A `stream_log` marker column (the marker is the reissue
  table). Reissuing deterministic classes freely (loops forever). Widening `settled_at` to LOST (the CHECK and
  §15.2's frozen four terminal states).

### D-O  Which memories can never hold a point
v2 excludes SECRET_MATERIAL-primary memories (`indexable`) from F and Q. It does not model card-size or gitleaks
refusals (`card_unbuildable`, `secret_scan_rejected`): PostgreSQL cannot recompute them (L7).

### D-P  Auto re-drive of schema-failed distill deaths (0221; card 32 debt, ADR-0058 L23)
An owner plpgsql SECURITY DEFINER `ops.auto_redrive_schema_failed(p_tenant uuid, p_cooldown interval, p_limit
integer) RETURNS TABLE(job_id, evidence_id, skipped)`, `search_path=pg_catalog`, EXECUTE `role_maintenance` only,
tenant argument checked against the installed GUC, `p_cooldown > 0` and `p_limit > 0` (22023).
- **Victims.** `DERIVED_DISTILL` jobs of the tenant that are DEAD with exactly `last_error_class =
  'FAILED_OUTPUT_SCHEMA'` and `auto_redrives = 0`, whose last counted provider call (`max(ops.distill_calls.begun_at)`;
  `ops.jobs` has no dead-at column) is older than the cool-down; `ORDER BY job_id LIMIT p_limit FOR UPDATE SKIP
  LOCKED`. A job with no counted call never qualifies. `PROVIDER_PERMANENT`, `ATTEMPTS_EXHAUSTED`,
  `PRE_DISPATCH_ABANDONED`, `EXECUTION_UNCERTAIN` and every other class are never taken: the predicate is equality.
- **Re-drive.** Each victim is marked `auto_redrives = 1` (new column, `smallint NOT NULL DEFAULT 0`), then re-armed
  by `ops.requeue_dead_distill(p_tenant, job, NULL)`, 0200's body unchanged: one re-arm code path. A refusal
  (55000 `evidence_gone` / `outbox_settled`) is caught in a sub-block and returned as a skip row; the mark stays, so a
  refused job is never taken again.
- **Invariants.** At most one automatic re-drive per Evidence (job identity is one Evidence; the operator's
  `jobs requeue-dead` neither reads nor resets the mark). An always-refusing provider costs 2 + 2 counted calls, then
  the job stays DEAD.
- **Same channel.** The re-ask uses the route's own output channel: ADR-0060 D-D makes the admitted Profile the one
  source of the request shape, and R9 already gives the tool channel its in-call content fallback (L8).
- **Daemon.** Task `redrive`, keys `REDRIVE_EVERY_SECONDS`, `REDRIVE_LIMIT` and `REDRIVE_COOLDOWN_SECONDS` (> 0), all
  required. `maintenance_repo::auto_redrive_schema_failed` runs one transaction: tenant GUC, the door, and, only when
  the door returned rows, one §77 `control.audit_events` row through `provisioning::audit_tagged` (action
  `DISTILL_AUTO_REDRIVE`, risk tag `distill_redrive`, resource `distill_job` with the job ids, metadata
  `{redriven, skipped}`). Under `--serve` the row carries the daemon's system identity (actor
  `system:humaux-maintenance --serve`, reason and ticket `ADR-0062 D-P`, a trace id minted per process); under
  `sweep once` it carries the operator's §77 fields.
- **Rejected.** Letting the re-ask use the Profile's other declared channel (a second shape source; it would need a
  claim flag read at `distill.rs:961`). Re-driving every DEAD class (would re-spend calls on permanent failures). A
  dead-at column (the last counted call is already the clock).

### D-Q  `humaux-maintenance sweep once`
The manual override and test seam. It requires the §77 fields like every writing subcommand, reads the `--serve`
keys except `METRICS_ADDR` and the `*_EVERY_SECONDS`, runs every task over one tenant page under the same cycle
budget, and prints one receipt `{command, outcome, tasks:[{task, ran, tenants, affected, failed, error}]}`. Exit 0,
or 1 when a call failed, with the receipt printed either way.

### D-R  rls-check: the closed set
The arm "ADR-0062 maintenance DELETE doors" collects the `pg_proc` rows `role_maintenance` can reach that delete:
`prosecdef AND (prorettype = trigger OR has_function_privilege('role_maintenance', oid, 'EXECUTE')) AND prosrc ~*
'\mDELETE\M'`, and requires that set to equal exactly six signatures, each with its justification in
`MAINTENANCE_DELETE_DOORS`:
- the four doors of D-E;
- `control.ensure_user(text,text)` (ADR-0053: it deletes only the `control.users` row it inserted in the same call
  when a concurrent onboarding won the email, a self-rollback rather than a purge path; it keeps its shipped 0186
  ACL);
- `staging.require_phase9_exact_assessed_storage_binding()` (0126's phase-9 exact-binding guard trigger: `DELETE`
  appears only in `TG_OP <> 'DELETE'` tests; it reads and raises, it deletes nothing).

Every SECURITY DEFINER trigger function is in scope whatever its ACL: it runs as its owner whenever a write
`role_maintenance` may make (stream_log, stream_checkpoints, ops.jobs …) fires it, and firing checks no EXECUTE, so a
deleting definer trigger would be an unlisted DELETE path (fix pass; S2 had filtered every trigger out). Invoker
functions, trigger or not, run with the caller's table privileges, which `check_forbidden_verbs` pins, so they are
filtered out before the comparison. The four doors must also be owner-owned with a pinned `search_path`, EXECUTE
exactly `{role_maintenance}` and PUBLIC revoked. The arm prints `maintenance DELETE doors exactly {…}`. A `DELETE`
built by dynamic SQL would evade the regex (L15).

### D-S  Exported counters (S8, ruling E3)
Two process-local families, rendered only by `humaux-maintenance --serve` through the card-34 encoder (ADR-0061 D-A;
no metrics crate): `maintenance_task_runs_total{task,outcome}` (one per tenant door call; a tenant page that cannot be
read counts one `failed` run) and `maintenance_task_rows_total{task}` (the affected rows of each committed call).
`task` is the closed D-C enum `maintenance_repo::MaintenanceTask` (one label source for the counters and the
daemon), `outcome` is `TaskOutcome` (ok | failed); every pair is seeded at 0. The one emit of each family is
`maintenance_repo::count_task_call`; its production caller is the daemon's cycle. `/metrics` serves them only while
ready (D-A); `--serve --metrics-families` prints the zero state, and `cargo xtask metrics-registry` reads that as the
process key `maintenance-serve` (D7; the rehearsal's EX leg maps the `humaux-maintenance:serve` scrape to it).

Two §42 rows:
- `MaintenanceTaskFailing`: `increase(maintenance_task_runs_total{outcome="failed"}[1h]) > 0`, WARNING. While a
  failure lasts `/metrics` answers 503 (D-A), so Prometheus records no sample and marks the series stale; this row is
  the after-the-fact record, read on the first clean scrape after recovery.
- `MaintenanceCountersAbsent` (fix pass): `absent(maintenance_task_runs_total)` for 2m, WARNING — the live signal
  while a door keeps failing, the database is unreachable, the daemon is dead, never scraped, or has not finished its
  first cycle. The HealthGaugesAbsent shape for the eighth ops pair; only a §41.2 name, no label key.

The promtool cases feed the shape the daemon really exports (flat, stale at the first 503, a gap, the recovered
counter higher) and the mutation rows `maint_label`, `maint_cmp`, `maint_absent`: `mutations: red=23/23`.

### D-T  Docs, rehearsal, soak (S8)
- `docs/ops/supervision.md`: the sixth resident unit (liveness: process alive; readiness: `GET /metrics` 200/503;
  one instance; one env file), its 503 rows and restart policy, the eighth ops pair and its two families.
- `docs/ops/runbook.md` §5: start order (`--serve` after migrations and `health serve`, before traffic); §5.1
  "Scheduled maintenance" (task, door, cadence / limit / age keys, what it never touches, where the evidence is,
  the operator's receipts query, how to pause a task, the idempotency-key precondition, `sweep once`); §5.2 the E8
  operator procedure (backup list, impact statement, explicit `sweep once`) for a database that holds residue.
- `docs/ops/rehearse.sh`: the daemon never touches `$DB`. The rehearsal creates `humaux_thread_c35_rh_$$` through the
  owner psql path, migrates it with `xtask migrate --dsn`, seeds eight tenants (expired confirm tokens, expired
  snapshots with items, idle full rate buckets, old DONE / DEAD jobs, one orphan ISSUED ticket each whose Evidence
  carries an indexable memory and whose `EVIDENCE_ACCEPTED` carrier is DONE — a PENDING carrier is in flight, D-L),
  starts `--serve` with retentions 0 and `LOST_AFTER = LAG_SECS + 1`, kill -9's it after its first receipt and
  restarts it from the one `$MD_ENV`, puts its hook second in the soak's chaos rotation, and drops the database on
  every exit path (`md_teardown`, called by `obs_stop`). `md_daemon_db` reads `current_database()` through the
  daemon's own DSN (`$MD_ENV` evaluated in a subshell exactly as `start_md` spawns it, so a DSN left out of `$MD_ENV`
  reads the inherited `$DB` one), and `start_md` refuses to spawn unless that is the throwaway database (fix pass:
  the S8 check built its own DSN from `$MD_DB` and could not fail). Step `maintenance_drain` asserts
  `maintenance_daemon_db_is_throwaway` (the same read after the drain, no refused spawn),
  `maintenance_kill9_landed_mid_purge`, `maintenance_soak_receipts_balance` (per purge door
  `seeded − remaining = Σ ops.maintenance_receipts.affected`, snapshots counted in snapshots),
  `maintenance_seeded_work_drained`, `maintenance_every_orphan_lost_and_reissued_once`,
  `maintenance_reissue_waits_cooldown_after_lost` (every reissue of a LOST source at least the cool-down after its
  `lost_at`, consecutive reissues of one Evidence at least the cool-down apart) and
  `maintenance_daemon_cycled_and_ready_after_restart`; `OPS_PORTS` gains `humaux-maintenance:serve 19108` and the
  `up` assertion becomes `prometheus_up_is_exactly_the_eight_ops_pairs`. The FK-orphan count of snapshot items is not
  asserted: the NO ACTION FK makes it 0 in every committed state, so it could never fail.
- `docs/ops/soak.md`: the daemon in the kill rotation; `docs/ops/serial-lane.md`: T-K5's `lane(b)` disposition;
  `docs/ops/delivery_point_report.md` §6.22 (P1-11 daemon part, P1-14, OPS-6, SEC-6, folded debts) and §6.23 (card
  35b, the test-helper debt); `docs/ops/delivery_plan_v2.md` row 35; Baseline §6.2.1 / §6.2.2 / §22.1 / §41.2 / §42 /
  §48.1 / §73.2 rows; addenda to ADR-0043, ADR-0057, ADR-0058 and ADR-0061; `deploy/prometheus/prometheus.yml` (eight
  targets).

## Main-line rulings (2026-10-04 11:30 and 21:50) this record follows
- **E1** Baseline §6.2.1's "物理删行唯一出口" line is amended: physical row delete also leaves through the §6.2.2-listed
  owner SECURITY DEFINER sweep doors (this ADR's closed set; one statement, LIMIT, receipt; `role_maintenance` EXECUTE
  only). 0169 already lived under that clause.
- **E2** R-36 §(c) "bounded DELETE stays rejected" is scoped to §48.1 history retention (partition drops, card 36).
  Expiring operational state (confirm tokens, snapshots + items, idle rate buckets, terminal jobs) leaves through
  D-E's doors. Mark-only columns are rejected.
- **E3** Two §41.2 rows and the §42 rows of D-S; labels from closed sets only.
- **E4** §22.1 gains the manifest cap / keyset segments / per-segment exact census sentence (D-K, L12).
- **E5** §73.2: IPv4 full address, IPv6 /64 prefix, after canonicalisation.
- **E6** `ops.maintenance_receipts` joins the §48.1 append-heavy list for card 36 (L11).
- **E7** (+ addendum) the allowed-file extension the slices used.
- **E8** No cross-tenant purge of the shared `humaux_thread_dev` in this card. Acceptance "after one daemon cycle" is
  proven on throwaway databases (the maintenance tests and the rehearsal's `humaux_thread_c35_rh_<pid>`); the runbook
  carries the operator procedure (backup, impact, `sweep once`, runbook §5.2).
- **E9** Every privilege expansion is enumerated below and pinned by rls-check closed sets.
- **E10** Card text corrected (ADR number, `sweep_lost` line, stale counts, "marks" → D-E).
- **E11** No outbox door: outbox rows are existence authority (private ledger joins, public guards).
- **E12** The 300 ms first-page bar is not met on this host and moves unrelaxed to card 35b; T-K5 asserts regression
  bars instead (§Known limits, E12).
- **E13** LOST reads in recall as `points_unsettled` + 1 and `completeness_ratio < 1` until the reissue settles; no
  degrade code (D-L).

## Privilege expansions (ruling E9), enumerated
- 0214: `control.maintenance_tenant_page(uuid,integer)` (a read of tenant ids); SELECT on `ops.maintenance_receipts`.
- 0218: the four DELETE doors of D-E (EXECUTE `role_maintenance`). No table grant changed; no non-owner role holds
  DELETE or TRUNCATE anywhere.
- 0220: `projection.reissue_unsettled_tickets(uuid,interval,integer)` (writes `stream_log`, `stream_checkpoints`,
  `ops.outbox` and the marker as the owner, EXECUTE `role_maintenance`; no DELETE in its body, so the closed set of
  D-R is unchanged); SELECT on `projection.ticket_reissues`. rls-check "ADR-0062 maintenance doors" pins the door.
- 0221: `ops.auto_redrive_schema_failed(uuid,interval,integer)` (writes `ops.jobs.auto_redrives` and, through 0200's
  `ops.requeue_dead_distill`, re-arms the job, its FAILED outbox row and the scheduler row as the owner; EXECUTE
  `role_maintenance`; no DELETE in its body). No table or column grant: the new column is written by the door only,
  and the 0221 postcheck fails if `role_maintenance` can UPDATE it.
- 0223: none. `stream_log.lost_at` falls under `role_maintenance`'s existing table-level UPDATE (0011, the §6.2.2
  `ISSUED → LOST` cell); the re-created door keeps its owner, `search_path` and EXECUTE (postcheck).
- S8 / rehearsal: none. The counters are process-local; the rehearsal's throwaway database is created and dropped by
  the superuser psql path the rehearsal already uses, never by a runtime role.

## Measurements (D-K M-1; dev host, debug test build, throwaway database `humaux_thread_c35_enum_*`, n = 30 timed runs after 3 warm-ups, 50 000 memories)
- Baseline B (Step 1, the S6 tree, per-id INSERT, no cap; checked in as
  `crates/adapters/tests/data/enumerate_scale_baseline.txt`):
  `ENUM n=50000 runs=30 first_p50_ms=19886.5 first_p95_ms=21562.1 later_p95_ms=37.4 floor_p95_ms=655.0 ratio=32.92 manifest_rows=50000`
- After S7 (one INSERT … unnest WITH ORDINALITY, cap 1000, keyset segments, 0222):
  `ENUM n=50000 runs=30 first_p50_ms=9258.2 first_p95_ms=9379.7 later_p95_ms=31.1 floor_p95_ms=560.7 ratio=16.73 manifest_rows=1000`
  (a second run after the test's helper split: `first_p50_ms=9247.9 first_p95_ms=9603.6 later_p95_ms=31.1
  floor_p95_ms=596.4 ratio=16.10 manifest_rows=1000`). Against the design's original bars `manifest_rows ≤ cap`
  held, `ratio_after × 10 ≤ ratio_B` failed (167.3 > 32.92) and `first_p95_ms < 300` failed (9379.7); ruling E12
  replaced them with regression bars.
- Slice 7b, under the E12 bars (operands (a) 9469.1 < 12937.3, (b) 16.13 × 1.5 = 24.20 ≤ 32.92, (c) 31.4 < 300,
  (d) 1000 == 1000):
  `ENUM n=50000 runs=30 first_p50_ms=9267.5 first_p95_ms=9469.1 later_p95_ms=31.4 floor_p95_ms=586.9 ratio=16.13 manifest_rows=1000 target_300ms=not_met`
- Fix pass, every page through `MANIFEST_PAGE_SQL` (green run, exit 0; operands (a) 9182.7 < 12937.3, (b) 15.94 ×
  1.5 = 23.91 ≤ 32.92, (c) 28.6 < 300, (d) 1000 == 1000):
  `ENUM n=50000 runs=30 first_p50_ms=9044.2 first_p95_ms=9182.7 later_p95_ms=28.6 floor_p95_ms=576.1 ratio=15.94 manifest_rows=1000 target_300ms=not_met`
- Fix pass, T-K5's red witness under its real fault (the per-id INSERT loop over every id, cap removed — the B
  shape; built into its own test binary, the source restored before the run): exit 101, bar (d) fired first
  (`manifest_rows=50000 must be == cap=1000`), and the line shows (a) 18956.1 ≥ 12937.3 and (b) 31.84 × 1.5 = 47.76 >
  32.92 red as well; (c) 26.8 < 300 stays green, as it must (later pages read the frozen manifest):
  `ENUM n=50000 runs=30 first_p50_ms=17932.7 first_p95_ms=18956.1 later_p95_ms=26.8 floor_p95_ms=595.3 ratio=31.84 manifest_rows=50000 target_300ms=not_met`
- Why the 300 ms bar is out of reach (measured with `EXPLAIN ANALYZE` as `role_gateway` on the seeded database):
  every 50k-row read of `private.memory_records` under role_gateway costs about 550 ms because the
  `memory_records_subject_visibility` RLS policy calls `private.memory_subject_visibility_ok(tenant_id, memory_id)`
  per row; the evidence-visibility join of `readable_memory_ids` costs about 900 ms. The floor itself (one such scan,
  560–655 ms p95) is already above 300 ms. The per-id INSERT loop was about half of the baseline (≈ 10 s of ≈ 20 s),
  not the dominant ×10 term the design assumed: the first page still runs the two §22.1 faces (manifest candidates +
  `final_memory_ids`, then the census candidates, its `readable_memory_ids` and its three readout statements), about
  a dozen O(N) reads under RLS.

## Tests and their faults
One row per test; every fault was applied, shown red, then restored and shown green (§80.1). Every database-writing
test owns a throwaway database (`humaux_thread_c35_<purpose>_<pid>_<n>`, created, migrated and dropped by its
fixture) unless the row says otherwise; nothing purges, LOSTs, reissues or re-drives on the shared dev database.

| test (file) | fault that reds it | gate |
|---|---|---|
| T-B1 `serve_refuses_to_boot_without_each_key_naming_it` (`bins/maintenance/tests/serve.rs`) | a default on `CYCLE_SECONDS` | `c35_serve_keys_named` |
| T-B2 `serve_refuses_lost_after_not_above_projection_lag` (serve.rs) | the LOST_AFTER > lag check dropped | `c35_serve_lag_relation_named` |
| T-A1 `one_cycle_answers_200_and_counts_each_due_task` (serve.rs; also reads the counters off `/metrics`: a run per tenant call, the swept orphan as one `lost` row, the revoked door's failed calls counted after the re-grant) | the verdict ignores failed calls | `c35_serve_cycle_named` |
| T-A2 `a_closed_pg_port_fails_cycles_with_503_and_recovers_without_restart` (serve.rs, an in-test TCP proxy) | a cycle failure returns `Err` (process exits) | `c35_serve_pg_restart_named` |
| T-A3 `sigterm_between_calls_exits_0_with_a_stopped_receipt` (serve.rs) | the latch checked only between cycles | `c35_serve_sigterm_named` |
| T-A4 `metrics_answers_503_until_the_first_cycle_finishes` (serve.rs, fix pass; a 5000-tenant first cycle holds the window open) | the record starts as a good outcome (`Last::ok()` in `serve`) ⇒ 200 mid-first-cycle | `c35_serve_first_cycle_named` |
| T-D1 `the_tenant_page_walks_every_tenant_once_per_rotation` (serve.rs) | `>=` for `>` in the page cursor | `c35_tenant_page_named` |
| T-D2 `rls_check::tests::the_tenant_page_is_maintenance_only` (inside a never-committed transaction on dev; also grants the reissue door to `role_gateway` and the re-drive door to `role_private_worker`) | EXECUTE granted to `role_gateway` | `c35_tenant_page_rls_named` |
| T-Q1 `sweep_once_requires_admin_fields_and_prints_per_task_counts` (serve.rs) | the §77 check dropped | `c35_sweep_once_named` |
| T-L1 `sweep_lost_skips_a_leased_or_backing_off_ticket` (`crates/adapters/tests/stream_repo.rs`) | the lease predicate dropped | `c35_sweep_lost_lease_named` |
| T-L2 `sweep_lost_sweeps_at_most_limit_oldest_first` (stream_repo.rs) | the LIMIT dropped | `c35_sweep_lost_limit_named` |
| T-L5 `sweep_lost_spares_a_ticket_whose_evidence_is_still_distilling` (stream_repo.rs, fix pass; real 0164 DERIVED_DISTILL jobs, carriers PENDING / PROCESSING / DONE / FAILED; also asserts `lost_at` only on the swept) | the outbox NOT EXISTS dropped ⇒ `[LOST, LOST, LOST, LOST]` | `c35_sweep_lost_outbox_named` |
| T-L3 `an_orphan_reads_as_lag_then_lost_then_in_flight_after_reissue` (`crates/adapters/tests/projection_lag.rs`; fix pass: the orphan's carrier is DONE, and the door issues 0 right after the sweep, 1 once `lost_at` is past the cool-down) | the sweep leaves the ticket ISSUED | — |
| T-L4 `a_swept_orphan_trades_lag_for_unsettled_slack_and_a_ratio_below_one` (projection_lag.rs) | the sweep leaves the ticket ISSUED | `c35_lost_reads_in_recall_named` |
| T-G1 `confirm_sweep_deletes_at_most_limit_and_writes_one_receipt` (`crates/adapters/tests/maintenance_doors.rs`) | the LIMIT dropped | `c35_doors` |
| T-G2 `a_call_that_deletes_nothing_writes_no_receipt` (maintenance_doors.rs) | the `affected > 0` guard dropped | `c35_doors` |
| T-H1 `an_expired_snapshot_and_its_items_go_in_one_statement_live_ones_stay` (maintenance_doors.rs) | the `expires_at` predicate dropped | `c35_doors` |
| T-H2 `a_db_expired_snapshot_is_refused_even_when_the_cursor_is_host_valid` (`crates/adapters/tests/g80_31_handoff.rs`, fix pass: through the production entry point `materialize_memory_enumeration`; a test-owned snapshot row in a throwaway tenant on dev) | `expires_at > now()` dropped from `MANIFEST_PAGE_SQL` ⇒ the frozen page comes back; dropped from `continuation_bound_in_txn` ⇒ a capped cursor mints a new segment | `c35_snapshot_db_expiry_named` |
| T-H3 `a_purge_after_the_page_read_began_cannot_empty_it` (maintenance_doors.rs; runs the production `MANIFEST_PAGE_SQL` text) | none of its own: the interleave inside one statement cannot happen; splitting the page read into two statements reds the gate | `c35_one_page_statement` (one `query(MANIFEST_PAGE_SQL)`, no standalone fingerprint or items SELECT: red on the pre-fix gateway text) |
| T-I1 `only_buckets_that_would_be_full_now_are_purged` (maintenance_doors.rs) | the refill predicate dropped | `c35_bucket_full_only_named` |
| T-I2 `a_purged_bucket_answers_the_same_rate_decision` (maintenance_doors.rs) | a bucket recreated with tokens 0 | `c35_doors` |
| `an_ipv6_64_preauth_bucket_goes_through_the_purge_door` (maintenance_doors.rs) | the /64 key reverted | `c35_doors` |
| T-J3 `a_done_job_with_a_call_inside_the_budget_window_is_kept_at_retention_zero` (maintenance_doors.rs) | `begun_at` compared with the done cutoff | `c35_jobs_budget_window_named` |
| T-J4 `dead_jobs_use_their_own_retention` (maintenance_doors.rs) | one retention for both states | `c35_doors` |
| T-J5 `a_redrivable_dead_distill_job_is_kept_until_its_outbox_settles` (maintenance_doors.rs) | the R4 predicate dropped | `c35_jobs_keep_r4_named` |
| T-J6 `a_job_linked_by_a_contribution_execution_is_kept` (maintenance_doors.rs) | the links NOT EXISTS dropped | `c35_doors` |
| T-E1 `a_door_takes_no_table_name` (maintenance_doors.rs) | structural (42883); its fault lives in T-R1 | `c35_doors` |
| `confirm_token_retention.rs` (moved to throwaway databases, new signature) | — (regression) | `c35_doors` chain |
| T-R1 `rls_check::tests::maintenance_delete_door_faults_drive_gate_red_then_restore` (never-committed transaction on dev) | (1) a table DELETE grant reds `check_forbidden_verbs`; (2) a fifth (outbox) DELETE door reds the closed set; (3) an invoker DELETE function stays green (drop the `prosecdef` clause and it reds); (4) fix pass: a SECURITY DEFINER trigger function that deletes, with no EXECUTE grant, reds the closed set — restoring the old "non-trigger, EXECUTE-granted" filter turns the whole test red | `c35_rls_door_faults_named`, `c35_delete_doors_closed` |
| T-S1 `two_ipv6_addresses_in_one_64_share_a_preauth_bucket` (`crates/adapters/tests/quota_and_rate.rs`) | `ip.to_string()` restored | `c35_sec6_named` |
| T-S2 `ipv6_addresses_in_different_64s_do_not_share` (quota_and_rate.rs) | mask /48 | — |
| T-S3 `ipv4_keeps_its_full_address_key` (quota_and_rate.rs, incl. the IPv4-mapped form) | IPv4 masked to /24 | — |
| T-M1 `a_non_indexable_memory_with_a_failed_ticket_widens_no_slack` (`crates/adapters/tests/a2_point_identity.rs`) | `AND r.indexable` dropped from 0219's Q filter (Q = 2) | `c35_ledger_v2_named`, `c35_ledger_v2_is_last` |
| `visible_index_count.rs` through the definer | the hand copy of the ticket terms restored | `c35_visible_index_definer` |
| T-N1 `one_sweep_drains_the_three_retired_fixtures_to_zero_unsettled` (a2_point_identity.rs) | the ticket issued on one stream for all three fixtures | `c35_reissue_named` |
| T-N2 `a_deterministic_failure_is_reissued_once_then_left` (a2_point_identity.rs) | the "not a reissue" predicate dropped | `c35_reissue_once_named` |
| T-N3 `every_class_waits_out_the_cooldown` (a2_point_identity.rs) | no cool-down for RETIRED_FAILED | `c35_reissue_cooldown_named` |
| T-N4 `one_ticket_per_stream_and_evidence` (a2_point_identity.rs) | dedup by memory | — |
| T-N5 `the_reissue_stream_is_the_home_stream` (a2_point_identity.rs) | the stream of the Evidence's last ticket of another kind | — |
| T-N6 `a_tombstoned_or_non_indexable_memory_is_never_reissued` (a2_point_identity.rs) | `indexable` dropped | — |
| T-N7 `a_lost_ticket_waits_out_the_cooldown_from_its_sweep` (a2_point_identity.rs, fix pass; production `sweep_lost` then the door: swept now ⇒ 0, aged ⇒ 1, the reissued ticket swept LOST ⇒ 0 again, aged ⇒ 1) | `sweep_lost` records no `lost_at` (0220's clock) ⇒ the first call right after the sweep issues 1 | `c35_reissue_lost_cooldown_named`, `c35_one_reissue_definer` |
| T-P1 `a_schema_failed_death_is_redriven_once_after_cooldown_and_distilled` (`bins/private-worker/tests/derived_dispatch_e2e.rs`, stub provider) | no `PERFORM requeue_dead_distill` (the job stays DEAD) | `c35_redrive_named` |
| T-P2 `an_always_refusing_provider_is_redriven_once_and_never_loops` (derived_dispatch_e2e.rs) | `auto_redrives = 0` dropped (the second call re-drives again) | `c35_redrive_no_loop_named` |
| T-P3 `attempts_exhausted_and_provider_permanent_deaths_are_never_auto_redriven` (derived_dispatch_e2e.rs) | class predicate widened to `IS NOT NULL` (3 re-armed) | `c35_redrive_classes_named` |
| T-P4 `a_death_inside_the_cooldown_is_left_dead` (derived_dispatch_e2e.rs) | the cool-down dropped | — |
| 0221 manifest postcheck | a gateway / PUBLIC EXECUTE grant; a `role_maintenance` UPDATE grant on the column; SECURITY INVOKER; a body without `auto_redrives = 0` | `migrate` |
| T-K1 `the_manifest_is_one_insert_in_ordinal_order` (`crates/adapters/tests/selection_snapshot.rs`; one `cmin` for every row, ordinals 0..N-1 in DESC id order) | reversed ordinality; the per-id loop (any cap) | `c35_one_manifest_insert` (also greps the loop) |
| T-K2 `a_manifest_larger_than_the_cap_continues_in_a_new_segment_with_its_own_exact_census` (g80_31_handoff.rs; cap 3, 7 memories, totals 7, 7, 4, 4, 1 over three segments) | `next_cursor = None` at a capped manifest's end | `c35_enumerate_cap_named` |
| T-K3 `snapshot_rows_never_exceed_the_cap` (g80_31_handoff.rs) | every id stored | — |
| T-K4 `bootstrap_without_enumeration_cap_or_ttl_fails_naming_the_key` (`bins/gateway/src/bootstrap.rs`) | a default for the TTL or cap key | `c35_enumerate_keys_named`, `c35_no_ttl_const` |
| T-K5 `enumerate_first_page_scales_with_a_capped_manifest` (`crates/adapters/tests/enumerate_scale.rs`, ignored, serial lane(b)) | the per-id INSERT loop over every id with the cap removed (the B shape) — red, see §Measurements; the baseline file deleted (fails naming it, never skips). **Not caught here:** the per-id loop with the cap kept (1000 single INSERTs cost < 0.5 s against a 12 937 ms bar; measured `first_p95_ms=9393.7 ratio=16.14 manifest_rows=1000`, test ok) — that fault is caught by T-K1 and `c35_one_manifest_insert`. The S7b record multiplied the timing samples (`first_ms` × 2.3, `later_ms` × 20) instead of applying a fault; it showed the bars can red, not that the named fault reds them | `c35_enumerate_scale_live`, `c35_enum_baseline_checked_in` |
| witness `maintenance_task_runs_total` (`crates/testkit/tests/metrics/`) | the `.inc(` line of `count_task_call` removed | `metrics-registry --check` |
| witness `maintenance_task_rows_total` (`crates/testkit/tests/metrics/`) | the `.inc(` line of `count_task_call` removed | `metrics-registry --check` |
| promtool `MaintenanceTaskFailing` fire / silent (`deploy/prometheus/tests/alerts.test.yml`) | the `outcome` matcher dropped; `> 0` → `< 0` | `c35_promtool_maintenance`, `promtool_mutations` |
| promtool `MaintenanceCountersAbsent` fire (outage gap, never scraped) / silent (fix pass) | `absent(maintenance_task_runs_total)` → `absent(vector(1))` | `c35_promtool_maintenance`, `promtool_mutations` (23/23) |
| `soak::tests::rotation_includes_the_maintenance_daemon` (`xtask/src/soak.rs`; `rehearse_script_soak_invocation_parses` expects the `md` pidfile) | the daemon's hook removed from the chaos rotation | `c35_soak_rotation_named` |
| rehearsal `maintenance_soak_receipts_balance` | a purge that can half-apply under kill -9 (a multi-statement or receipt-less door) | `rehearse_c35` |
| rehearsal `maintenance_daemon_db_is_throwaway` | the daemon pointed at the shared database: `$MD_ENV`'s DSN set to `$DB`, or dropped so the inherited `$DB` DSN applies (witness: `md_daemon_db` reads `humaux_thread_dev` in both cases, so `start_md` refuses and the assertion reds) | `rehearse_c35` |
| rehearsal `maintenance_reissue_waits_cooldown_after_lost` (fix pass) | 0220's cool-down clock (a reissue right after the sweep) | `rehearse_c35` |
| rehearsal `prometheus_up_is_exactly_the_eight_ops_pairs` | the daemon missing from `OPS_PORTS` / not scraped | `rehearse_c35` |
| `health_serve` tests (`bins/maintenance/tests/health_serve.rs`) | — (the `resident.rs` refactor regression) | `c35_serve_tests` chain |

## Known limits
- **E12: the 300 ms first-page bar is not met** (2026-10-04 21:50). Root cause: the RLS policy
  `memory_records_subject_visibility` calls `private.memory_subject_visibility_ok` per row (§Measurements). The bar
  is not relaxed and moves verbatim to **card 35b**: set-based subject-visibility — the RLS policy evaluates
  `memory_subject_visibility_ok` once per statement (a set-based policy or a security-barrier view), or the census
  shares the authorized-id read — an ADR-0041 D-G change with its own design, attackers and fault tests; acceptance
  = this same test with `first_p95_ms < 300` restored. It is not a hot patch inside card 35: the policy is a security
  boundary that card 35's design never analysed. Until then T-K5 asserts four regression bars, each printing its
  operands: (a) `first_p95_ms < 0.6 × B.first_p95_ms` (= 12 937 ms); (b) `ratio_after × 1.5 ≤ ratio_B` (= 21.95);
  (c) `later_p95_ms < 300` (the pages-2..n promise; a page read that re-scans the workspace reds it); (d)
  `manifest_rows == cap`. `FIRST_P95_TARGET_MS = 300` stays as the documented target (`// ponytail:`), printed as
  `target_300ms=met|not_met`. Gates `c35_enumerate_scale_live` and `c35_enum_baseline_checked_in` are unchanged;
  `delivery_point_report.md` §6.23 carries the row.
- L1 `// ponytail:` per-task tenant page rotation; latency = ceil(tenants / `TENANTS_PER_RUN`) × EVERY. Upgrade: a
  "tenants with work" definer per task.
- L2 LIMIT is per tenant call: one run deletes at most `TENANTS_PER_RUN × LIMIT` rows per task. The cycle budget,
  not the LIMIT, bounds wall time.
- L3 No outbox row is purged. Outbox growth follows Evidence, lifecycle and public events, not churn. Upgrade: when
  stream_log rows of purged memories retire and public authority moves off outbox existence, a door can return
  with an allowlist of event types.
- L4 The daemon reads the gateway's lag key and the distill worker's budget window under their own names, so one
  env file gives one value; two units started from different env files could still disagree (the runbook requires
  one env per host). Upgrade: a deployment config-check that loads every unit's env and compares shared keys.
- L5 Purging a job frees its `idempotency_key`. Upgrade: a key tombstone in `ops.job_history` (§48.1, card 36)
  before any producer re-presents old keys.
- L6 `ops.retrieval_embedding_rpc_calls` / `ops.private_inference_rpc_calls` are not purged (no grant to
  `role_maintenance`, not in the card's list). Upgrade: one door each with the same shape plus a §6.2.2 note.
- L7 Memories refused by `seal_card` for size or gitleaks are not modelled as non-indexable: one reissue (D-N
  deterministic rule), then they stay in Q until an operator retirement. Upgrade: a per-memory unindexable marker
  written by the worker and read by the v2 `indexable` term.
- L8 Auto re-drive keeps the route's channel. Upgrade signal: the post-re-drive DEAD rate above R11
  (`dead ≤ n/100`); then a per-claim channel override honoured at `distill.rs:961` under ADR-0060 D-D (a
  Profile-declared second channel only).
- L9 `// ponytail:` the reissue door repeats 0189's "latest ticket per memory" join instead of sharing a terms
  function; T-N1 ties the two (the reissue drains Q to 0). Upgrade: a shared definer if a third consumer appears.
- L10 Transient-class and LOST reissues repeat while the dependency stays down: at most one per memory per
  `LOST_AFTER` + `REISSUE_COOLDOWN_SECONDS` for LOST, one per cool-down for transient FAILED; bounded cost, no cap.
  Upgrade: a per-memory cap from a `ticket_reissues` count.
- L11 Receipts exist only for deletes that removed something; the receipt table itself waits for card 36 (E6).
- L12 Per-segment census: a client that pages across segments gets exact totals per segment, not one global total
  for a snapshot it never had (Baseline §22.1, E4).
- L13 Cursors and cadence clocks are in memory; a restart begins a new rotation and runs every task on its first
  cycle (harmless: idempotent doors).
- L14 (closed in design revision) the jobs door keeps every DEAD distill job whose Evidence row is not DONE, so no
  boot relation between the dead retention and the re-drive cool-down is needed.
- L15 `// ponytail:` the closed-set arm reads `prosrc` with a regex, so a `DELETE` built by dynamic SQL would evade
  it. Every door is `LANGUAGE sql`; upgrade: also flag maintenance-reachable definers whose body runs `EXECUTE`.
- L16 `// ponytail:` the rate-bucket door may take a consumer's advisory lock on a non-victim bucket (the planner
  may test the lock first) and hold it until the statement commits; consumers wait under their 2 s
  `lock_timeout`. Upgrade: a lock-free recheck in a second CTE if a purge ever runs long.
- L17 `// ponytail:` evidence rows are visibility-scoped, so the reissue door (owner, no user GUC) reads a
  secret PRIMARY it cannot see as indexable; that memory gets one ticket, which the worker settles
  SKIPPED_BY_POLICY (it reads the ticket evidence's class), and leaves Q. Upgrade: a tenant-only owner read arm on
  the evidence data class.
- L18 No coord-lease task: cards 44/45 add the coord tables. Recipe: one owner definer
  `<schema>.reap_expired_<x>_leases(tenant, limit)` and one `MaintenanceTask` variant with its two keys; a
  placeholder variant now would be dead code.
- L19 `maintenance_task_*` counters are process-local and reset on restart (ADR-0061 D-A); `increase()` absorbs the
  reset. The rehearsal's receipts balance is therefore read from `ops.maintenance_receipts`, not from the counters.
- L20 `MaintenanceTaskFailing` cannot fire while the failure it names lasts (D-S): the live signal is
  `MaintenanceCountersAbsent`, which does not say which task failed; the 503 body and `/status` name it.
- L21 LOST rows swept before 0223 carry no `lost_at`; their cool-down still counts from their other timestamps.

## Follow-ups
- **Card 35b**: the 300 ms first-page bar (E12, §Known limits).
- **Test-helper debt** (`delivery_point_report.md` §6.23): three copies of the create-and-migrate throwaway-database
  helper — `bins/maintenance/tests/support/throwaway.rs`, `crates/adapters/tests/support/throwaway_db.rs`,
  `crates/adapters/tests/health_snapshot.rs` — each with its own per-binary mutex and different failure semantics.
  Consolidating them gives `crates/testkit` its first runtime dependency and rewires twelve test files, beyond a
  contained change; upgrade: one testkit module with the strictest semantics and a cluster-wide advisory lock.
- **E8 operator step**: the shared dev database's residue waits for runbook §5.2 (backup, impact, `sweep once`).

## Addendum 2026-10-05 (card 36, ADR-0063): the receipts table is partitioned; the daemon gains PARTITIONS

- **L11 / E6 closed.** Migration 0227 (ADR-0063 D-D) turned `ops.maintenance_receipts` into a RANGE (`ran_at`)
  parent under its own name: PK `(receipt_id, ran_at)`, the heap became one leaf, the current UTC month plus three
  future months are pre-created, no DEFAULT partition, every leaf sealed (FORCE RLS with the parent's tenant policy
  verbatim, zero runtime grants). The four purge doors of D-E..D-J are unchanged: each still writes its one receipt
  row in the same statement, through the parent, which routes it to the current month's leaf; no door names a leaf.
  Nothing references the table, so it has no identity table (ADR-0063 L10). Its retention is an ordinary
  `MAINTENANCE_RECEIPTS` policy (whole months for all tenants, no hold; runbook §5.4).
- **What a missed horizon does here.** Once the last pre-created month is past, a purge door's receipt insert fails
  with 23514, so the door's single statement deletes nothing and the daemon answers 503 (D-A) — fail closed, never
  a delete without a receipt. The alarm is ADR-0063's `PartitionHorizonShort / Exhausted / Absent`, one to two
  months earlier.
- **D-C gains one cluster-level task, `partitions`** (ADR-0063 D-K): `HUMAUX_MAINTENANCE_SERVE_PARTITIONS_EVERY_SECONDS`
  (required, no default), no tenant page and no LIMIT key. It runs once per due run, counts
  `maintenance_task_runs_total{task="partitions"}` once per run (not per tenant), and only writes the two proposal
  columns of `control.partition_registry`. The closed DELETE-door set rls-check pins (D-R) is unchanged; the daemon holds no DDL
  and no owner path.
- **Boot refusal.** `--serve` (and `health serve`) exit 2 when `HUMAUX_MIGRATOR_PG_DSN` is in their environment
  (ADR-0063 D-H): the shared env file of this ADR's L4 must not carry the superuser migrator DSN.

## Addendum 2026-10-05 — ruling E12-b: the enumerate regression bar is ratio-only

Card 36's verification showed that the absolute bar `first_p95_ms < 0.6 × B.first_p95_ms` depends on host load, not on
the code: the same tree measured 9.4–9.6 s (card 35 chain), 16.6 s (a verifier overlapping its own reruns; the bare floor
scan doubled to 1.2 s) and 12.9 s on an idle but slower day (floor 748 ms, a 25 ms margin against the bar), while the
ratio `first_p95 / floor_p95` of each run stayed at 13.8–17.3. `enumerate_first_page_scales_with_a_capped_manifest` now
asserts three bars: (a) `ratio_after ≤ 0.6 × ratio_B` (= 19.75 against baseline B's 32.92; it replaces the absolute bar
and subsumes the former `ratio × 1.5 ≤ ratio_B`), (c) `later_p95_ms < 300`, (d) `manifest_rows == cap`. The uncapped
per-id loop still reds (a) at a ratio of about 33. Absolute first-page times and `target_300ms=not_met` stay on the `ENUM`
line; the 300 ms target itself remains card 35b's acceptance.
