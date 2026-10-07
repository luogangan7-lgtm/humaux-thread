# ADR-0065 — Gateway admission control, gateway pool keys, startup-option timeouts, single-transaction rate buckets

- Status: Accepted (2026-10-07, chain run 1 below; design `card_38_design.md`, decisions D-A–D-I, main-line rulings R-1–R-14). Implemented in seven serial
  slices (S1–S7); this file is completed in S6 and its Measurement section in S7.
  - S1 (2026-10-07, uncommitted): the measurement harness `cargo xtask load` (`xtask/src/load.rs`) and the rehearsal's
    `load` step (`docs/ops/rehearse.sh`, behind `LOAD_LEVELS`; the card-37 chain shape is unchanged when it is unset),
    run once against the unchanged gateway (the BEFORE measurement below). No gateway, adapter or schema change.
  - S2 (2026-10-07, uncommitted): `PoolSettings` + `RuntimeDbPool::connect_with` in `crates/adapters/src/postgres.rs`
    (the workspace's one pool-builder site), the seven gateway pool / session-timeout keys and the boot order rule in
    `bins/gateway/src/bootstrap.rs`, the keys in every launcher (gateway test fixtures, `xtask e2e-onboard`,
    `rehearse.sh start_gw`, hence the load step's scratch gateway). No migration, no role GUC. Notes under
    "Pool keys and session timeouts (S2 notes)" below.
  - S3 (2026-10-07, uncommitted): `quota_repo::consume_rate_batch` — one request's post-auth buckets in ONE
    transaction with the tier lock order (D-D, R-1: no dedicated pool), the rate path's 55P03 / 57014 / 40P01 →
    `DependencyUnavailable` arm, and the two keys `HUMAUX_GATEWAY_RATE_LOCK_TIMEOUT_MS` and
    `HUMAUX_GATEWAY_RATE_PREAUTH_IPV6_PREFIX_BITS` (D-E) in every launcher and fixture. No migration. Notes under
    "Single-transaction rate buckets (S3 notes)" below.
  - S4 (2026-10-07, uncommitted): the §67.2 admission layer `bins/gateway/src/admission.rs` (D-C), wrapped once in
    `main` around the MCP router only; the five `HUMAUX_GATEWAY_ADMISSION_*` keys in every launcher and fixture; the
    family `admission_rejected_total{class,reason}` (counter in `crates/telemetry/src/admission.rs`, rendered by the
    gateway, witness, §42 `AdmissionRejected` rule + promtool test + mutation row 33); the R-6 Baseline text. No
    migration. Notes under "Admission layer (S4 notes)" below.
  - S5 (2026-10-07, uncommitted): Debt A ruled fail fast on the server (no gateway code change; the stub-RPC test
    `a_transient_query_embedding_failure_fails_fast_with_one_worker_call` pins it) and one jittered retry in the soak
    client (`xtask/src/soak.rs`, `docs/ops/soak.md` "The retry rule"); Debts B and C ruled deferred, no code. Notes
    under "Debts A / B / C (S5 notes)" below; L12 under "Known limits".
  - S6 (2026-10-07, uncommitted): this file completed — Decisions D-A–D-I, rulings R-1–R-13, the §78.6 spec text,
    the Tests table (28 tests, one fault each), Known limits L1–L15, follow-up 38b and open item O-1 with the card-39
    gate `a5_load_remeasure`; runbook §8.1 "Gateway admission and pool (dev-host)" from the BEFORE numbers;
    `delivery_point_report.md` §6.26; delivery_plan_v2 rows 38 / 38b / 39 and the three lag lines (R-2, R-3, R-9,
    R-11); Baseline:345 (R-9). The AFTER measurement, the C sweep and the load faults are S7's.
  - S7 (2026-10-07, uncommitted): the AFTER measurement, the C sweep and the load faults through the rehearsal's new
    `LOAD_EXTRA_PHASES` runs (`docs/ops/rehearse.sh`; `xtask load --only` / `--rfc7239-lane`, assertions a run does not
    grade print `NOT_APPLICABLE` with the missing phases, tests 29–30); K measured down to C / 2 = 8 (K = C failed
    isolation at 2.5×); launcher values with their citations in `rehearse.sh` and `xtask e2e-onboard`; runbook §8.1
    AFTER column. Section "Measurement" → AFTER.
  - Review fix (2026-10-07, uncommitted): six review findings and one uncaught fault, each changed at the root — the
    admitted request runs in its own task that owns its ticket (D-C; a client disconnect no longer frees the permit
    while rmcp's detached handler runs on; D-A, D-B, L6 and L13 corrected); the rate path's 57014 and 40P01 arms pinned
    by real DB tests; the load step's ASSERTION lines counted into the rehearsal verdict and exit code; assertion 7
    reads the rows the load really writes, and assertions 3 and 7 have recorded reds (faults F-A3, F-TEARDOWN); the
    tenant-lock wait and single-tenant throughput measured (`LOCKWAIT`, D-D) instead of the "≈ 1–2 ms" estimate; the
    inherited `--no-lane --processor-id` count gates raised to 4; `c38_measurements` compares exact counts. AFTER
    re-measured on the fixed tree (run `after-fix`); known limit L16 added. Notes under "Review fixes" below.

## Context

Baseline §67.2 fixes the gateway's inbound admission (concurrency 16, queue 64, wait 5 s, overflow HTTP 503 +
`Retry-After` + `RATE_LIMITED`); Baseline:345 leaves the PostgreSQL pool size "to be measured". Before card 38 the
gateway had no admission layer, its pool ran on sqlx 0.8.6 defaults, no statement or idle-in-transaction timeout was
set, and each request spent its post-auth rate buckets in four separate transactions.

## Decisions

Facts F1–F24 of the design were re-read on HEAD `ed3ab0c`; the ones a decision rests on are cited inline.

### D-A — Gateway pool keys and the sizing bound

`crates/adapters/src/postgres.rs` gains `pub struct PoolSettings` and `RuntimeDbPool::connect_with(dsn, &PoolSettings)`;
`open(dsn, Option<&PoolSettings>)` holds the workspace's one `PgPoolOptions::new()`, and `None` keeps the sqlx 0.8.6
defaults for every other pool (L1, card 38b). The gateway reads five pool keys — `HUMAUX_GATEWAY_PG_POOL_MAX_CONNECTIONS`,
`…_PG_POOL_MIN_CONNECTIONS`, `…_PG_ACQUIRE_TIMEOUT_MS`, `…_PG_IDLE_TIMEOUT_SECONDS`, `…_PG_MAX_LIFETIME_SECONDS` —
each required, `default: None` (§78.1).

Sizing. Admission (D-C) holds a request's permit until its handler returns: the admitted work runs in its own task
that owns the ticket, so a client that disconnects frees nothing early (rmcp runs every handler detached; review fix).
What still outlives a permit is a statement abandoned inside the handler: a handler timeout drops the handler's
future, its error response frees the permit, and the abandoned statement's connection stays checked out until the
server ends it (F19). So checked-out connections ≤ C·k + 1 + D, with D ≈ handler-timeout rate × statement_timeout, and
**P = C·k + 2 + H**: k = peak non-idle `role_gateway` backends / C at 64 clients (1 expected, every path awaits its
statements in sequence), 2 = the readiness refresh and one spare, H ≥ the expected D (0 while every phase measures 0
drops, else ceil(drop_rate × 10 s)). With D > P − C·k − 1 an acquire waits and fails after acquire_timeout as
`DEPENDENCY_UNAVAILABLE` without `Retry-After` (L13). Dev budget: max_connections 100 − 3 reserved = 97; expected
peak ≈ 42 (other processes, soak peak n = 147) + 8 + 18 = 68, asserted ≤ 80 (assertion 6); memory ≈ 80 × 4.40 MB +
160 MiB shared_buffers + 40 × 4 MB work_mem ≈ 0.7 GB against the card-37 low-water of 1282 MB free. Production
(A5, 4 vCPU): more than ≈ 2 × cores = 8 running statements add no throughput, so the dev-host sweep overstates the
knee (L2, O-1).

Rejected: (a) keys for every process in this card — card 38b (R-3); (b) a boot check P ≥ C + 2 — it would block the
F-P fault run, the invariant is asserted from the measurement; (c) `test_before_acquire = false` — unmeasured (W-2);
(d) registry `default: Some(..)` — contradicts "unset key → refuses to start" (R-5).

### D-B — Session timeouts as startup options, not `ALTER ROLE`

`connect_with` sets `PgConnectOptions::options([statement_timeout, idle_in_transaction_session_timeout])` from
`HUMAUX_GATEWAY_PG_STATEMENT_TIMEOUT_MS` and `HUMAUX_GATEWAY_PG_IDLE_IN_TRANSACTION_TIMEOUT_MS` (and
`application_name`), so the server applies them once per connection and every pooled reuse keeps them. Boot
refuses unless `RATE_LOCK_TIMEOUT_MS` < `PG_STATEMENT_TIMEOUT_MS` < handler timeout and idle-in-transaction ≤ handler
timeout, naming the key and the order: the server cancels a statement before the client stops waiting for it.
Visible as `pg_settings.source = client`; `/status` `effective_config` and the config fingerprint carry the keys;
`pg_db_role_setting` stays at 0 rows (gate `c38_no_role_guc`). A cancel is SQLSTATE 57014; an idle transaction past
its timeout loses its session with FATAL 25P03. In the rate path 57014 / 55P03 / 40P01 map to
`DEPENDENCY_UNAVAILABLE`; the other mappers still map 57014 to `INTERNAL` (L4, card 38b). A handler timeout drops the
handler's future and its error response frees the permit; the abandoned server statement runs on until
statement_timeout (L6, L13). A client disconnect frees nothing: the admitted task runs to the end of its handler
(D-C). Exempt: the superuser
migrator and RetentionExecutor, maintenance (its cycle-derived DSN timeout, ADR-0061 D-D), workers and admin (38b),
ad-hoc psql sessions (L5).

Rejected: (a) `ALTER ROLE role_gateway SET …` in a migration 0235 — a literal in SQL (§78.1), a forward-only
migration per change, a write to the cluster-shared catalog that races throwaway migrations (`CLUSTER_DDL`, F4), and a
check of the catalog rather than of the pool the process uses; (b) `SET` in `after_connect` — one round trip per
connection for the same effect; (c) `SET LOCAL` per transaction — dozens of call sites.

### D-C — §67.2 admission layer with a body-read bound and a per-credential limit

`bins/gateway/src/admission.rs`, one `from_fn_with_state` layer on the MCP router only (`wrap` = `mcp.layer(..)
.merge(supervision)`, one caller in `main`). Order per request: (0) the whole body is read under
`HUMAUX_GATEWAY_ADMISSION_BODY_READ_TIMEOUT_MS` (B) and the existing `HUMAUX_GATEWAY_MAX_REQUEST_BODY_BYTES` — 413 /
408 before any admission state; (1) the per-credential slot K (`…_ADMISSION_PER_KEY_LIMIT`, key = first 8 bytes of
SHA-256 of the raw `Authorization` value, unverified, fairness only); (2) the semaphore C (`…_ADMISSION_CONCURRENCY`);
(3) the waiter cap Q (`…_ADMISSION_QUEUE_DEPTH`); (4) the wait W (`…_ADMISSION_MAX_WAIT_MS`). Overflow = 503 +
`Retry-After` (integer seconds, ceil(waiting × EWMA service / C), 1 s floor, 30 s cap) + the pre-parse `RATE_LIMITED`
body; `admission_rejected_total{class="gateway_inbound", reason ∈ {queue_full, wait_timeout, key_limit}}` +1 per 503,
no log line per refusal. `/livez` and `/readyz` are outside the layer, so draining wins: the drain 503 never waits on
admission; the SIGTERM→SIGKILL grace is > 5 s + B + W + handler + finalize (`docs/ops/supervision.md`). The admitted
work runs in its own task, which owns the permit and the key slot until the handler returns, also when the client
disconnects: rmcp's stateless mode spawns every handler detached and nothing reads its cancellation token, so a permit
freed with the request future would leave C bounding connected clients instead of gateway and PG work (review fix).
Why K: with a
global FIFO alone one tenant's 64-client burst keeps Q full and another tenant waits ≈ 64·S/C or gets `queue_full`;
F-K (K = C + Q) must turn `load_tenant_isolation` red, otherwise K is deleted. Notes and tests: "Admission layer (S4
notes)".

Rejected: (a) tower `ConcurrencyLimitLayer` + `LoadShedLayer` — no bounded queue with a wait deadline; (b) a new
`ErrorCode` / error_map row carrying Retry-After — §52 surgery and a second shape for one code; (c) a semaphore inside
`check_access` — after JSON parsing and the preauth bucket, inside the handler timeout; (d) the tenant as the key — not
known before authentication without a DB read; (e) rejecting queued waiters at drain start — already bounded by W;
(f) taking the permit before the body read — 16 slow-body connections would hold every permit (F18); (g) tower-http
`RequestBodyTimeoutLayer` — not a gateway dependency, and after the permit it still holds one per slow client; (h) a
log line per refusal — a 503 flood becomes a log and CPU amplifier.

### D-D — Post-auth buckets in one transaction, tier lock order, no dedicated pool

`quota_repo::consume_rate_batch(pool, &[RateCharge], lock_timeout)`: one transaction binds the tenant once, sets
`lock_timeout` from `HUMAUX_GATEWAY_RATE_LOCK_TIMEOUT_MS` with `set_config(.., true)`, takes one blocking
`pg_advisory_xact_lock(hashtextextended(..))` per charge in tier order tenant > user > credential/mcp >
credential/operation, inserts missing rows with one `unnest` statement, and runs the unchanged refill-and-take per
charge in input order; the first denial commits what was taken before it (parity with the former sequential path).
Deadlock freedom: every transaction takes at most one key per tier in strictly increasing tier order, so no wait
cycle forms; a cross-tier 64-bit hash collision is backstopped by the deadlock detector (40P01, W-5), lock_timeout
(55P03, W-4) and their `DEPENDENCY_UNAVAILABLE` mapping — fail closed, never hang. Cost, measured (`LOCKWAIT`,
"Measurement" → AFTER): the tenant lock is held across ≤ 3 more updates and the commit; at 64 clients of one tenant
the p99 wait over all 8974 takes is 9.7 ms (longest 100.9 ms; L16 15.8 ms), up to 10–11 backends queue at once, and
single-tenant throughput is ≈ 178 calls/s (≈ 180 tenant-bucket takes/s) on this CPU-bound host — far below the
2000 ms lock_timeout and not the cause of the enumerate tail (L16). `tools/call` still spends the shared buckets
twice (E-12, card 38b). Notes and tests: "Single-transaction rate buckets (S3 notes)".

Rejected: (a) a dedicated small admission / rate pool (R-1) — after D-C at most C requests hold ≤ 1 connection each and
P ≥ C + 2, while a small pool adds head-of-line blocking across tenants; (b) all locks in one `unnest … ORDER BY`
statement — target-list evaluation order is not guaranteed; (c) ordering by hash value — correct but unreadable;
(d) batching the preauth bucket — it runs before authentication under the SYSTEM tenant.

### D-E — The IPv6 pre-auth prefix is a registered key

`HUMAUX_GATEWAY_RATE_PREAUTH_IPV6_PREFIX_BITS` (1..=128, start 64), carried as `RateSubject::PreauthIp(IpAddr, u8)`.
`to_canonical()` first, so `::ffff:a.b.c.d` keys as the IPv4 address; per W-10 loopback `::1` and link-local
`fe80::/10` key as themselves (/128); any other IPv6 address keys as `{masked}/{bits}`. The /64 keying itself shipped
in card 35 (ADR-0062 E5); this card lifts the literal (§78.1 限流阈值). No migration: dev rows are already `/64`, and
subjects from another prefix age out through card 35's idle-bucket purge. Rejected: a data migration rewriting
`subject_id` (buckets rebuild full on demand); dropping the key (§78.1).

### D-F — Measurement harness inside the rehearsal

`cargo xtask load` (`xtask/src/load.rs`), run by the rehearsal's `load` step behind `LOAD_LEVELS`: see "Measurement
harness (D-F)" below for tenants, mix, lanes, phases and the nine assertions `load_gateway_ready` (0),
`load_no_pool_or_statement_timeout` (1), `load_overflow_503_within_max_wait` (2), `load_admission_counter_equals_503s`
(3), `load_p95_recorded` (4), `load_tenant_isolation` (5, median of 3 pairs with the `LOAD_ISO_FLOOR_MS` floor),
`load_pg_backends_bounded` (6), `load_tenants_torn_down` (7), `load_forwarded_lanes_keyed` (8). Measurement-only
extras (S7): the C sweep C ∈ {8, 16, 24} with P = C + 2, and the fault runs F-K, F-P, F-FWD.

Rejected: (a) extending soak — a correctness harness, ≥ 2 tenants, and its fresh-socket client caps out near 546
connections/s (F14); (b) a standalone launcher — it would duplicate the rehearsal's gateway env; (c) open-loop
arrival — C / Q / K are about fixed concurrency (L10); (d) a throwaway PG — the gate is about the real dev schema;
(e) `remember.put` in the mix — ≈ 10⁴ paid embedding calls and orphan Qdrant points per run (F21, L15); (f) a recall
lane — ≈ 22 calls per level < n 50, and paid; (g) one bearer per tenant everywhere — K would decide every overload;
(h) refusal reasons from a log line — the `reason` label replaces it.

### D-G, D-H, D-I — Debts A, B, C

Debt A: fail fast on the server, one jittered retry in the soak client (D-G). Debt B: provider-wide distill breaker
deferred (D-H). Debt C: route-health prober deferred (D-I). Rulings, evidence and reopening signals: "Debts A / B / C
(S5 notes)" below; L11, L12.

## Main-line rulings (2026-10-07)

- **R-1** (E-1) No dedicated admission / rate pool: D-C bounds admitted requests, D-D batches the buckets in tier
  order; a second pool only adds head-of-line blocking.
- **R-2** (E-2) Timeouts are startup options, never `ALTER ROLE`; delivery_plan_v2's B46 / A35 wording and the card-38
  acceptance gate now name the `source = client` test and the `pg_db_role_setting` = 0 rows gate.
- **R-3** (E-3) Card 38 sizes the gateway pools only. Plan row **38b**: worker / maintenance / admin pool keys and
  per-worker concurrency caps (Baseline:345 item 3), the 57014 arm in the twelve other `db_error` mappers (E-7), the
  `tools/call` double token spend (E-12). Until then those pools keep the sqlx defaults (L1).
- **R-4** (E-4) Every listed allowed-file extension accepted.
- **R-5** (E-5) Registry keys keep `default: None`; measured values live in the launchers (`docs/ops/rehearse.sh`,
  `xtask/src/e2e_onboard.rs`, later card 39's compose), this ADR and runbook §8.1, each citing its `LOAD` / `SWEEP`
  line and labelled `dev-host start value, A5 re-measure required`.
- **R-6** (E-6) The Baseline text changes listed under "Spec text changed (§78.6)" below; `error_map.rs`'s ponytail
  points here.
- **R-7** (E-7) Deferred to 38b.
- **R-8** (E-8) The load step may seed and tear down LA (16 workspaces) and LB on `humaux_thread_dev` through
  `e2e-seed` / `e2e-seed --teardown` only; no memory / outbox / job / Qdrant writes (assertion 7 counts before
  teardown); at most the two SYSTEM preauth `ip` rows remain, purged as idle by card 35.
- **R-9** (E-9) Baseline:345 takes the dev-host sentence; **open item O-1 blocks card 39 from freezing its compose
  values** until its gate `a5_load_remeasure` is green; no `ops.mechanism_observations` row from the dev host; runbook
  §8.1 stays "(dev-host)" until then.
- **R-10** (E-10) The other product line on this host cannot be quieted and the Docker VM stays 3.8 GiB; every ISO pair
  records `ISO_NOISE`; if noise still reds the isolation assertion, S7 reruns the ISO pairs once after
  `docker stats --no-stream` and records both results.
- **R-11** (E-11) The plan's "lag degradation / lag classification → card 38" lines point at ADR-0057's
  `PROJECTION_LAG_SECONDS` (closed by card 31); the non-existent `xtask soak --burst 128` is replaced by the
  `xtask load` phases.
- **R-12** (E-12) Deferred to 38b.
- **R-13** (E-13) Isolation is the median of 3 interleaved ISO pairs: ratio < 2 OR increase < `LOAD_ISO_FLOOR_MS`
  (= max(2, 2 × the S1 base spread) = 7 ms, frozen in `TW/c38_rehearse.sh`); the plain ratio is printed as
  `ISO_RATIO`, not graded.
- **Web research**: no outside batch. Answered from vendored sources by the slice that needed them: W-1 (sqlx-core
  0.8.6 `pool/connection.rs:199-208, 314`: no CancelRequest, the slot is held through `ping()`), W-3 (sqlx-postgres
  0.8.6 `options/mod.rs:426-441`, `connection/establish.rs:45`), W-6 (tokio `sync/semaphore.rs` FIFO hand-off, a
  dropped `acquire` releases its place), W-11 (`axum::serve` 0.8.9 sets no hyper timer). W-4 / W-5 are answered by
  the S2 / S3 DB tests (55P03, 57014, 25P03, 40P01). W-8: `Retry-After` delta-seconds (RFC 9110 §10.2.3), floor 1 s,
  cap 30 s. W-10: IPv4-mapped → IPv4, loopback and link-local /128. W-2 (`test_before_acquire`), W-7 (tower layers,
  answered by D-C rejected a) and W-9 (open-loop method, L10) stay unmeasured.

- **R-14 (main line, 2026-10-07 13:3x, after the implementation and its review).** The AFTER measurement shows a
  faster median and a heavier tail than BEFORE at the same levels (enumerate L16: p50 114.7 → 87.5 ms, p95 136.8 →
  263.1 ms, p99 160.6 → 355.6 ms; L64: p50 448.6 → 363.2 ms, p95 488.2 → 542.4 ms, p99 514.1 → 652.4 ms; throughput
  up). Accepted for this card as known limit L16 with its reopening signal: the card bounds load and ends hangs, it
  does not promise a lower tail, and these are dev-host numbers (O-1). Follow-up 38b carries the investigation of the
  tenant-lock hold time of the single-transaction batch (D-D) against the per-bucket transactions it replaced, with
  the A5 re-measure (`a5_load_remeasure`) as the arbiter. The inherited harness `c32_r11_distill_dead_fail_closed.sh`
  was taught the `assertion_line` helper that `assert_eq` now prints through (a main-line edit of the TW script; the
  harness stays green against the HEAD rehearse.sh as well).

## Spec text changed (§78.6)

Each sentence below was edited in `docs/architecture/Baseline_2.9.md` in this card and cites ADR-0065:

1. §41.2 metrics row: `admission_rejected_total{class, reason}` — +1 where §67 admission returns 503; `class` =
   `gateway_inbound`; `reason` ∈ {`queue_full`, `wait_timeout`, `key_limit`}; consumers §67, §54 and the §42 admission
   rejected alert (D-C).
2. §67.2, after the inbound admission block: the body is read whole under B and `MAX_REQUEST_BODY_BYTES` (413 / 408)
   before any admission state, so a slow upload holds no slot; then the per-credential limit K (SHA-256 prefix of the
   raw `Authorization`, unverified, fairness only; 1 ≤ K ≤ C + Q; a stricter refusal than the global bound); then C, Q,
   W. Overflow 503 carries an integer `Retry-After` (floor 1, cap 30) and the pre-parse text body `RATE_LIMITED`, no
   JSON-RPC envelope; `class` = `gateway_inbound` plus `reason`; exactly +1 per 503, no log line per refusal;
   `/livez` and `/readyz` bypass admission (D-C).
3. §67.2 single-node table, rate-limit row: one transaction, blocking `pg_advisory_xact_lock(hashtextextended(...))`
   per bucket in tier order tenant > user > credential/mcp > credential/op, `lock_timeout` from
   `HUMAUX_GATEWAY_RATE_LOCK_TIMEOUT_MS` (replaces the stale `pg_try_advisory_xact_lock(hashtext(...))`; D-D).
4. Line 345 (R-9): Postgres pool size (gateway) is a dev-host start value — P = C·k + 2 + H, start C = 16, P = 18, the
   BEFORE measurement cited with n and `measured_at`, the AFTER `LOAD` lines in this ADR — and the A5 same-spec
   re-measure is still required (card 39 gate `a5_load_remeasure`, open item O-1); the item is not closed until that
   gate is green, and no `ops.mechanism_observations` row is written from the dev host; worker pools and concurrency
   caps are card 38b.

`crates/protocol/src/error_map.rs`'s ponytail on the §67.2 503 shape now points here (comment only).

## Measurement harness (D-F)

`cargo xtask load` drives one scratch gateway (`start_gw 127.0.0.1:18080 19109 gw_load`) that the rehearsal starts
with the rehearsal's own environment plus `LOAD_GW_ENV` — only keys the tree under test registers, because the
gateway refuses an unknown `HUMAUX_GATEWAY_*` key. Two load tenants are seeded through `e2e-seed` and torn down after
the phase (ruling R-8): LA with 16 workspaces (16 workspace-bound keys, used round-robin in the levels and BURST),
LB with one. The mix is `memory.enumerate` 70% / `tools/list` 30%, closed loop, zero think time, one keep-alive
connection per client; no op reaches a provider, the outbox or Qdrant. Each lane sends the bare
`Forwarded: 2001:db8:<lane>::<n>` form the edge parser accepts, and the scratch gateway trusts 127.0.0.1/32, so each
lane keys into its own preauth bucket `2001:db8:<lane>::/64`. A 503 pauses its client for `Retry-After` seconds.

Phases: L8/L16/L32/L64 (5 s warm-up + 45 s), BURST (128 clients, 20 s), three interleaved ISO pairs (LB 4 clients
alone, then LA 64 clients on ONE key plus LB 4; 20 s each), BURST-W (scratch gateway restarted with W = 1 ms; needs
the admission keys, so the BEFORE run skips it). Assertions 0–8 are printed as `ASSERTION PASS|FAIL load_*`;
assertion 0 (`/readyz` = 200 before the first phase and after every restart) gates whether a report is written at
all. Every graded line joins the rehearsal's `A_OK` / `A_BAD` (`load_tally`; a non-zero exit with no FAIL line is one
FAIL of its own), so a red load assertion makes `REHEARSAL VERDICT` fail and `rehearse.sh` exit 1. Assertion 7 reads
LA's and LB's outbox / job / memory_evidence / tenant / rate-bucket rows: before the teardown `0/0/0/2/>0` (the load
wrote its buckets and nothing it must not), after it `0/0/0/0/0`, and no tenant left listed. Each phase also prints a
`LOCKWAIT` line: a second read-only session samples every `--lock-sample-ms` (10 ms) the waiting rate advisory locks
of `role_gateway` (`pg_locks` ⋈ `pg_stat_activity`), splits them into LA's tenant lock (key from
`quota_repo::rate_lock_key` and the server's `hashtextextended`) and every other rate lock, and ranks the tenant waits
over ALL tenant-bucket takes of the phase (the bucket's `version` delta), so a take never seen waiting counts as a
wait below the 10 ms resolution. Every `LOAD`, `LOCKWAIT`, `ISO_RATIO` and `ISO_NOISE` line carries `host=` and
`measured_at=`.

## Measurement

### BEFORE (S1, gateway at HEAD `ed3ab0c`; dev-host start values, A5 re-measure required)

Run: `REHEARSE_PROFILE=release SOAK_SECS=0 LOAD_LEVELS=8,16,32,64 LOAD_BURST=128 zsh TW/c38_rehearse.sh before`
(evidence `card38_rehearsal_evidence/before/`, report `load-report.json`). The gateway is HEAD's: no admission layer,
the sqlx 0.8.6 pool default of 10 connections, no statement timeout. BURST-W is skipped (no admission key to restart
with). Assertions 2 and 5 are recorded, not graded, in this run (design S1).

```
LOAD L8 group=LA clients=8 op=enumerate n=4917 p50=60.9ms p95=71.1ms p99=79.0ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:39:04Z
LOAD L8 group=LA clients=8 op=tools_list n=2106 p50=25.7ms p95=30.3ms p99=34.5ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:39:04Z
LOAD L16 group=LA clients=16 op=enumerate n=5076 p50=114.7ms p95=136.8ms p99=160.6ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:39:55Z
LOAD L16 group=LA clients=16 op=tools_list n=2170 p50=57.0ms p95=68.7ms p99=83.6ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:39:55Z
LOAD L32 group=LA clients=32 op=enumerate n=5029 p50=226.3ms p95=258.4ms p99=279.3ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:40:45Z
LOAD L32 group=LA clients=32 op=tools_list n=2159 p50=130.9ms p95=153.3ms p99=171.2ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:40:45Z
LOAD L64 group=LA clients=64 op=enumerate n=5052 p50=448.6ms p95=488.2ms p99=514.1ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:41:35Z
LOAD L64 group=LA clients=64 op=tools_list n=2151 p50=279.8ms p95=316.0ms p99=338.1ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:41:35Z
LOAD BURST group=LA clients=128 op=enumerate n=2136 p50=946.7ms p95=1067.6ms p99=1111.0ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:41:56Z
LOAD BURST group=LA clients=128 op=tools_list n=914 p50=621.7ms p95=744.5ms p99=754.0ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:41:56Z
LOAD ISO-BASE-1 group=LB clients=4 op=enumerate n=1664 p50=39.1ms p95=49.1ms p99=58.3ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:42:18Z
LOAD ISO-BASE-1 group=LB clients=4 op=tools_list n=708 p50=17.2ms p95=22.8ms p99=27.5ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:42:18Z
LOAD ISO-BURST-1 group=LA clients=68 op=enumerate n=2128 p50=477.9ms p95=530.6ms p99=571.5ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:42:38Z
LOAD ISO-BURST-1 group=LA clients=68 op=tools_list n=898 p50=297.4ms p95=347.7ms p99=368.8ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:42:38Z
LOAD ISO-BURST-1 group=LB clients=68 op=enumerate n=135 p50=473.5ms p95=517.9ms p99=545.1ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:42:38Z
LOAD ISO-BURST-1 group=LB clients=68 op=tools_list n=55 p50=292.6ms p95=342.5ms p99=381.9ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:42:38Z
LOAD ISO-BASE-2 group=LB clients=4 op=enumerate n=1678 p50=38.8ms p95=46.9ms p99=58.3ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:43:01Z
LOAD ISO-BASE-2 group=LB clients=4 op=tools_list n=717 p50=17.0ms p95=22.6ms p99=28.6ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:43:01Z
LOAD ISO-BURST-2 group=LA clients=68 op=enumerate n=2107 p50=474.1ms p95=553.0ms p99=594.0ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:43:21Z
LOAD ISO-BURST-2 group=LA clients=68 op=tools_list n=892 p50=299.6ms p95=368.0ms p99=415.5ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:43:21Z
LOAD ISO-BURST-2 group=LB clients=68 op=enumerate n=135 p50=465.8ms p95=536.1ms p99=562.3ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:43:21Z
LOAD ISO-BURST-2 group=LB clients=68 op=tools_list n=54 p50=296.5ms p95=363.9ms p99=451.5ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:43:21Z
LOAD ISO-BASE-3 group=LB clients=4 op=enumerate n=1700 p50=38.8ms p95=46.0ms p99=55.5ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:43:43Z
LOAD ISO-BASE-3 group=LB clients=4 op=tools_list n=723 p50=16.9ms p95=19.6ms p99=22.9ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:43:43Z
LOAD ISO-BURST-3 group=LA clients=68 op=enumerate n=2142 p50=475.3ms p95=516.0ms p99=569.7ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:44:04Z
LOAD ISO-BURST-3 group=LA clients=68 op=tools_list n=906 p50=294.7ms p95=346.4ms p99=363.6ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:44:04Z
LOAD ISO-BURST-3 group=LB clients=68 op=enumerate n=137 p50=469.8ms p95=504.8ms p99=515.9ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:44:04Z
LOAD ISO-BURST-3 group=LB clients=68 op=tools_list n=57 p50=295.4ms p95=341.5ms p99=360.4ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-06T20:44:04Z
LOAD_SKIP BURST-W: no --restart BURST-W hook (the tree under test registers no admission key)
ISO_RATIO pair=1 lb_base_p95=47.1ms lb_burst_p95=502.0ms ratio=10.66 host=10cpu-vm3.8 measured_at=2026-10-06T20:44:04Z
ISO_RATIO pair=2 lb_base_p95=44.8ms lb_burst_p95=527.1ms ratio=11.77 host=10cpu-vm3.8 measured_at=2026-10-06T20:44:04Z
ISO_RATIO pair=3 lb_base_p95=43.8ms lb_burst_p95=499.0ms ratio=11.40 host=10cpu-vm3.8 measured_at=2026-10-06T20:44:04Z
ASSERTION PASS load_gateway_ready: before the first phase and after restarts []
ASSERTION PASS load_no_pool_or_statement_timeout: L64: DEPENDENCY_UNAVAILABLE=0 INTERNAL=0 drops=0
ASSERTION FAIL load_overflow_503_within_max_wait: BURST: 503=0 queue_full=0 key_limit=0 p100_503=0ms (W=5000); BURST-W not run
ASSERTION PASS load_admission_counter_equals_503s: 11 phases
ASSERTION PASS load_p95_recorded: n >= 50 for every level and op
ASSERTION FAIL load_tenant_isolation: pairs=3 median_ratio=11.40 median_increase=455.2ms floor=2ms lb_n_and_no_503=true
ASSERTION PASS load_pg_backends_bounded: client_backends_peak=24 (<= 80) role_gateway_peak=11 (<= 2x10) sampled=true
ASSERTION PASS load_forwarded_lanes_keyed: want ["2001:db8:a::/64", "2001:db8:b::/64"] got ["2001:db8:a::/64", "2001:db8:b::/64"]
ASSERTION PASS load_tenants_torn_down: outbox/jobs/memory_evidence before=0/0/0 after=0/0/0
```

Reading: throughput is flat at about 155 calls/s from L8 to BURST while latency grows linearly with the client count
(enumerate p95 71 → 137 → 258 → 488 → 1068 ms), and LB's p95 rises from about 45 ms alone to about 500 ms next to a
64-client burst on one LA key (ratio 10.7–11.8). The role_gateway backend peak is 11 in every phase (the scratch gateway's 10 + the rehearsal
gateway's 1), consistent with every request queueing for the same 10 pool connections; S7's C sweep with sized pools
separates that from the other shared points (the one preauth /64 bucket per lane, gateway CPU). No request is refused
and nothing separates one tenant's burst from another tenant's calls. No failed call, no drop, no 503.

`pg_stat_activity` peaks per phase (1 Hz, read-only): L8: client backends 23, role_gateway 11, MemAvailable 2214392 kB; L16: client backends 24, role_gateway 11, MemAvailable 2248768 kB; L32: client backends 23, role_gateway 11, MemAvailable 2249372 kB; L64: client backends 23, role_gateway 11, MemAvailable 2258236 kB; BURST: client backends 23, role_gateway 11, MemAvailable 2252780 kB; ISO-BASE-1: client backends 23, role_gateway 11, MemAvailable 2246960 kB; ISO-BURST-1: client backends 23, role_gateway 11, MemAvailable 2211112 kB; ISO-BASE-2: client backends 23, role_gateway 11, MemAvailable 2255392 kB; ISO-BURST-2: client backends 24, role_gateway 11, MemAvailable 2280432 kB; ISO-BASE-3: client backends 23, role_gateway 11, MemAvailable 2271232 kB; ISO-BURST-3: client backends 23, role_gateway 11, MemAvailable 2304488 kB.

`LOAD_ISO_FLOOR_MS` (ruling R-13), frozen in `TW/c38_rehearse.sh` from this run (`ISO_NOISE pair=` printed the phase index, not the pair number, in this run; fixed after it): ISO-BASE p95 (LB, all ops) =
47.110, 44.770, 43.773 ms; spread 3.338 ms; max(2, 2 × spread) = 6.675 ms → **7 ms** (the
flag takes whole milliseconds; rounded up).

### AFTER (review fix re-run on the fixed tree; start values of runbook §8.1; dev-host start values, A5 re-measure required)

Run: `REHEARSE_PROFILE=release SOAK_SECS=0 LOAD_LEVELS=8,16,32,64 LOAD_BURST=128 LOAD_EXTRA_PHASES="sweep-c8 sweep-c16
sweep-c24 fault-k fault-p fault-p1 fault-p-c64 fault-fwd fault-a3 fault-teardown" zsh TW/c38_rehearse.sh after-fix` on
the review-fix tree (release build), 2026-10-07 11:12:44–11:39:42 local, 26 min 58 s wall clock, `REHEARSAL VERDICT: 140
passed, 0 failed` (131 + the nine load lines, now counted), EXIT 0, store epoch unchanged; evidence `card38_rehearsal_evidence/after-fix/` (`load-report.json`,
`evidence/load_main.log`, `evidence/load_sweep.log`, `evidence/load_faults.log` = `TW/c38_load_faults.log`). It
supersedes S7's run (a) (`after/`, 08:14–08:39, same launcher values, numbers within a few percent of these), which
ran before the admitted work held its ticket in its own task and before the load lines reached the verdict. The
scratch gateway runs the launcher values of runbook §8.1: C 16, Q 64, W 5000 ms, **K 8**, B 5000 ms, P 18, acquire
1000 ms, statement / idle-in-transaction 10 s, rate lock 2000 ms, /64. All nine load assertions PASS and are counted
in the verdict:

```
LOAD L8 group=LA clients=8 op=enumerate n=5580 p50=55.1ms p95=65.4ms p99=74.9ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:14:05Z
LOAD L8 group=LA clients=8 op=tools_list n=2389 p50=19.2ms p95=24.2ms p99=28.4ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:14:05Z
LOCKWAIT L8 lock=tenant group=LA acquisitions=8812 observed_waits=810 p50=<10ms p99=3.7ms max=22.4ms other_rate_lock_waits=68 other_max=21.5ms resolution=10ms samples=4613 host=10cpu-vm3.8 measured_at=2026-10-07T03:14:05Z
LOAD L16 group=LA clients=16 op=enumerate n=5658 p50=87.5ms p95=263.1ms p99=355.6ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:14:55Z
LOAD L16 group=LA clients=16 op=tools_list n=2418 p50=21.3ms p95=33.6ms p99=45.2ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:14:55Z
LOCKWAIT L16 lock=tenant group=LA acquisitions=8981 observed_waits=2049 p50=<10ms p99=15.8ms max=45.4ms other_rate_lock_waits=112 other_max=21.9ms resolution=10ms samples=4623 host=10cpu-vm3.8 measured_at=2026-10-07T03:14:55Z
LOAD L32 group=LA clients=32 op=enumerate n=5666 p50=180.1ms p95=353.2ms p99=462.2ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:15:45Z
LOAD L32 group=LA clients=32 op=tools_list n=2422 p50=108.9ms p95=133.5ms p99=149.8ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:15:45Z
LOCKWAIT L32 lock=tenant group=LA acquisitions=8998 observed_waits=1736 p50=<10ms p99=10.8ms max=38.5ms other_rate_lock_waits=97 other_max=24.9ms resolution=10ms samples=4623 host=10cpu-vm3.8 measured_at=2026-10-07T03:15:45Z
LOAD L64 group=LA clients=64 op=enumerate n=5616 p50=363.2ms p95=542.4ms p99=652.4ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:16:35Z
LOAD L64 group=LA clients=64 op=tools_list n=2407 p50=288.0ms p95=332.1ms p99=368.0ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:16:35Z
LOCKWAIT L64 lock=tenant group=LA acquisitions=8974 observed_waits=1582 p50=<10ms p99=9.7ms max=100.9ms other_rate_lock_waits=101 other_max=63.4ms resolution=10ms samples=4629 host=10cpu-vm3.8 measured_at=2026-10-07T03:16:35Z
LOAD BURST group=LA clients=128 op=enumerate n=2572 p50=446.3ms p95=607.1ms p99=702.9ms failed=0 admission_503=960 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:16:56Z
LOAD BURST group=LA clients=128 op=tools_list n=1106 p50=377.0ms p95=406.7ms p99=453.9ms failed=0 admission_503=960 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:16:56Z
LOCKWAIT BURST lock=tenant group=LA acquisitions=3678 observed_waits=734 p50=<10ms p99=12.0ms max=83.2ms other_rate_lock_waits=46 other_max=16.1ms resolution=10ms samples=1874 host=10cpu-vm3.8 measured_at=2026-10-07T03:16:56Z
LOAD ISO-BASE-1 group=LB clients=4 op=enumerate n=1943 p50=34.7ms p95=41.1ms p99=49.2ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:17:18Z
LOAD ISO-BASE-1 group=LB clients=4 op=tools_list n=828 p50=12.8ms p95=16.0ms p99=20.0ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:17:18Z
LOCKWAIT ISO-BASE-1 lock=tenant group=LA acquisitions=0 observed_waits=0 p50=<10ms p99=<10ms max=0.0ms other_rate_lock_waits=65 other_max=5.9ms resolution=10ms samples=1805 host=10cpu-vm3.8 measured_at=2026-10-07T03:17:18Z
LOAD ISO-BURST-1 group=LA clients=68 op=enumerate n=1931 p50=71.4ms p95=85.1ms p99=95.9ms failed=0 admission_503=1120 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:17:38Z
LOAD ISO-BURST-1 group=LA clients=68 op=tools_list n=823 p50=24.1ms p95=30.1ms p99=34.6ms failed=0 admission_503=1120 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:17:38Z
LOAD ISO-BURST-1 group=LB clients=68 op=enumerate n=1089 p50=62.4ms p95=72.6ms p99=80.4ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:17:38Z
LOAD ISO-BURST-1 group=LB clients=68 op=tools_list n=465 p50=22.9ms p95=27.1ms p99=31.2ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:17:38Z
LOCKWAIT ISO-BURST-1 lock=tenant group=LA acquisitions=2754 observed_waits=319 p50=<10ms p99=5.6ms max=34.1ms other_rate_lock_waits=101 other_max=7.0ms resolution=10ms samples=1855 host=10cpu-vm3.8 measured_at=2026-10-07T03:17:38Z
LOAD ISO-BASE-2 group=LB clients=4 op=enumerate n=1921 p50=34.8ms p95=43.6ms p99=52.3ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:18:00Z
LOAD ISO-BASE-2 group=LB clients=4 op=tools_list n=817 p50=12.8ms p95=16.4ms p99=19.1ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:18:00Z
LOCKWAIT ISO-BASE-2 lock=tenant group=LA acquisitions=0 observed_waits=0 p50=<10ms p99=<10ms max=0.0ms other_rate_lock_waits=51 other_max=6.8ms resolution=10ms samples=1825 host=10cpu-vm3.8 measured_at=2026-10-07T03:18:00Z
LOAD ISO-BURST-2 group=LA clients=68 op=enumerate n=1907 p50=72.2ms p95=86.8ms p99=97.4ms failed=0 admission_503=1120 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:18:21Z
LOAD ISO-BURST-2 group=LA clients=68 op=tools_list n=817 p50=24.2ms p95=30.7ms p99=35.9ms failed=0 admission_503=1120 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:18:21Z
LOAD ISO-BURST-2 group=LB clients=68 op=enumerate n=1067 p50=63.9ms p95=75.4ms p99=84.4ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:18:21Z
LOAD ISO-BURST-2 group=LB clients=68 op=tools_list n=455 p50=23.2ms p95=29.2ms p99=34.2ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:18:21Z
LOCKWAIT ISO-BURST-2 lock=tenant group=LA acquisitions=2724 observed_waits=337 p50=<10ms p99=5.1ms max=13.7ms other_rate_lock_waits=109 other_max=21.1ms resolution=10ms samples=1869 host=10cpu-vm3.8 measured_at=2026-10-07T03:18:21Z
LOAD ISO-BASE-3 group=LB clients=4 op=enumerate n=1932 p50=34.6ms p95=42.3ms p99=51.3ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:18:43Z
LOAD ISO-BASE-3 group=LB clients=4 op=tools_list n=825 p50=12.7ms p95=16.3ms p99=19.7ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:18:43Z
LOCKWAIT ISO-BASE-3 lock=tenant group=LA acquisitions=0 observed_waits=0 p50=<10ms p99=<10ms max=0.0ms other_rate_lock_waits=66 other_max=8.4ms resolution=10ms samples=1818 host=10cpu-vm3.8 measured_at=2026-10-07T03:18:43Z
LOAD ISO-BURST-3 group=LA clients=68 op=enumerate n=1919 p50=71.4ms p95=87.5ms p99=102.4ms failed=0 admission_503=1120 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:19:03Z
LOAD ISO-BURST-3 group=LA clients=68 op=tools_list n=818 p50=24.0ms p95=30.9ms p99=40.5ms failed=0 admission_503=1120 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:19:03Z
LOAD ISO-BURST-3 group=LB clients=68 op=enumerate n=1083 p50=62.8ms p95=73.9ms p99=88.9ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:19:03Z
LOAD ISO-BURST-3 group=LB clients=68 op=tools_list n=458 p50=22.7ms p95=28.3ms p99=37.9ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:19:03Z
LOCKWAIT ISO-BURST-3 lock=tenant group=LA acquisitions=2737 observed_waits=326 p50=<10ms p99=6.4ms max=14.0ms other_rate_lock_waits=94 other_max=7.5ms resolution=10ms samples=1859 host=10cpu-vm3.8 measured_at=2026-10-07T03:19:03Z
LOAD BURST-W group=LA clients=128 op=enumerate n=1844 p50=89.3ms p95=268.4ms p99=382.3ms failed=0 admission_503=1680 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:19:24Z
LOAD BURST-W group=LA clients=128 op=tools_list n=785 p50=22.0ms p95=33.6ms p99=73.2ms failed=0 admission_503=1680 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:19:24Z
LOCKWAIT BURST-W lock=tenant group=LA acquisitions=2629 observed_waits=626 p50=<10ms p99=16.4ms max=54.0ms other_rate_lock_waits=45 other_max=76.5ms resolution=10ms samples=1389 host=10cpu-vm3.8 measured_at=2026-10-07T03:19:24Z
ISO_RATIO pair=1 lb_base_p95=39.6ms lb_burst_p95=70.8ms ratio=1.79 host=10cpu-vm3.8 measured_at=2026-10-07T03:19:24Z
ISO_RATIO pair=2 lb_base_p95=41.2ms lb_burst_p95=73.3ms ratio=1.78 host=10cpu-vm3.8 measured_at=2026-10-07T03:19:24Z
ISO_RATIO pair=3 lb_base_p95=40.6ms lb_burst_p95=71.2ms ratio=1.76 host=10cpu-vm3.8 measured_at=2026-10-07T03:19:24Z
ASSERTION PASS load_gateway_ready: before the first phase and after restarts ["BURST-W:0.0s"]
ASSERTION PASS load_no_pool_or_statement_timeout: L64: DEPENDENCY_UNAVAILABLE=0 INTERNAL=0 drops=0
ASSERTION PASS load_overflow_503_within_max_wait: every phase
ASSERTION PASS load_admission_counter_equals_503s: 12 phases
ASSERTION PASS load_p95_recorded: n >= 50 for every level and op
ASSERTION PASS load_tenant_isolation: pairs=3 median_ratio=1.78 median_increase=31.2ms floor=7ms lb_n_and_no_503=true
ASSERTION PASS load_pg_backends_bounded: client_backends_peak=34 (<= 80) role_gateway_peak=20 (<= 2x18) sampled=true
ASSERTION PASS load_forwarded_lanes_keyed: want ["2001:db8:a::/64", "2001:db8:b::/64"] got ["2001:db8:a::/64", "2001:db8:b::/64"]
ASSERTION PASS load_tenants_torn_down(outbox/jobs/memory_evidence/tenants/rate_buckets before=0/0/0/2/55 after=0/0/0/0/0 left=[]): ok
```

`ISO_NOISE` per pair (loadavg; the other product line's containers idle): pair 1 `{ 9.56 6.74 4.29 }`, pair 2
`{ 8.27 6.73 4.40 }`, pair 3 `{ 9.01 7.04 4.62 }` — the load is this run's own. Scraped `admission_rejected_total`
deltas, equal to the client's 503 count in every phase (assertion 3): BURST {queue_full: 960}; ISO-BURST-1/2/3
{key_limit: 1120} each; BURST-W {wait_timeout: 1680}.

`pg_stat_activity` peaks per phase (1 Hz, read-only): L8: client backends 29, role_gateway 16, active role_gateway max
12 ms, wait_event_type Lock peak 3; L16: 34, 20, 510 ms, Lock 11; L32: 33, 20, 305 ms, Lock 10; L64: 33, 20, 329 ms,
Lock 10; BURST: 33, 20, 186 ms, Lock 10; ISO-BASE-1/2/3: 33, 20, ≤ 1 ms, Lock 1; ISO-BURST-1/2/3: 33, 20, 9–28 ms,
Lock 2–3; BURST-W: 33, 20, 234 ms, Lock 10. Idle-in-transaction max ≤ 1 ms in every phase; MemAvailable 2.17–2.31 GB.

Reading, against BEFORE:
- **Throughput** rises from about 155–160 to about 178 calls/s (L64: 7203 → 8023 admitted calls in 45 s) and stays flat
  from L16 up: the host is CPU-bound (loadavg 8.3–9.6 on 10 CPUs during the ISO pairs), so C moves latency, not
  throughput (SWEEP below). **Single-tenant throughput** (D-D): one tenant on 16 keys at 64 clients is LA's L64 rate,
  ≈ 178 calls/s, with 8974 tenant-bucket takes in the phase (≈ 180 /s: each enumerate spends the post-auth buckets
  twice, E-12, tools/list once).
- **Tenant lock** (D-D, `LOCKWAIT`): at L64 the p99 wait over all 8974 takes is 9.7 ms, p50 below the 10 ms resolution,
  the longest 100.9 ms; L16 15.8 ms / 45.4 ms, L32 10.8 ms / 38.5 ms, BURST 12.0 ms / 83.2 ms, BURST-W 16.4 ms /
  54.0 ms. Up to 10–11 backends queue on locks at once (wait_event_type Lock peak), so the lock is a convoy, but a
  short one: it is far below the 2000 ms lock_timeout and far below the enumerate tail. The estimate "≈ 1–2 ms per
  take" is replaced by these numbers (D-D).
- **Enumerate tail at L16–L64 regressed and is not the tenant lock.** Enumerate p95 136.8 → 263.1 ms (L16; p99 160.6 →
  355.6), 258.4 → 353.2 (L32), 488.2 → 542.4 (L64; p99 514.1 → 652.4), while p50 fell (114.7 → 87.5 ms at L16) and
  tools/list improved at every level. The tenant-lock p99 (≤ 16 ms) explains at most a few percent of it; what moved
  with it is the longest single `role_gateway` statement (active max 13 → 510 ms at L16, 50 → 329 ms at L64): with P =
  18 instead of the old 10 and C = 16, up to 16 enumerate statements run at once on a CPU-bound host instead of 10, so
  each one's tail grows. Not attributed further on this host — L16 under "Known limits".
- **BURST** (128 clients) is now bounded: 960 `queue_full` 503s (Retry-After 1–30 s, body `RATE_LIMITED`, p100 within
  W + 200 ms) instead of an unbounded queue; enumerate p95 1067.6 → 607.1 ms for the admitted calls. **BURST-W**
  (W = 1 ms) exercises the timer branch end to end: 1680 `wait_timeout`.
- **Isolation**: tenant B's p95 next to a 64-client burst on ONE tenant-A key went from ≈ 45 → ≈ 500 ms (median ratio
  11.40) to 40–41 → 71–73 ms (median ratio 1.78, increase 31.2 ms). The margin to the 2× bound is thin on this host;
  A5 re-measures it (O-1).
- **Pool**: role_gateway peak 20 = the scratch gateway's 18 + the rehearsal gateway's 2; client backends 34 ≤ 80; no
  acquire, statement or lock timeout, 0 drops (D = 0, so H = 0 in P = C·k + 2 + H, D-A); `active role_gateway max`
  ≤ 510 ms, far below the 10 s statement_timeout.

**K, measured (D-C, ruling R-5).** The design's start value K = C = 16 failed `load_tenant_isolation` (first AFTER run,
evidence `card38_rehearsal_evidence/after-k16/`): one key held all C permits and B's p95 was 2.4–2.5× its baseline. The
`iso-c<C>-k<K>` extra runs of the second AFTER run (`after-k8-run2/`) placed it:

```
K=16 C=16 (after-k16, main run):
ISO_RATIO pair=1 lb_base_p95=39.4ms lb_burst_p95=99.2ms ratio=2.52 host=10cpu-vm3.8 measured_at=2026-10-06T23:03:10Z
ISO_RATIO pair=2 lb_base_p95=40.3ms lb_burst_p95=96.4ms ratio=2.39 host=10cpu-vm3.8 measured_at=2026-10-06T23:03:10Z
ISO_RATIO pair=3 lb_base_p95=38.6ms lb_burst_p95=97.1ms ratio=2.52 host=10cpu-vm3.8 measured_at=2026-10-06T23:03:10Z
ASSERTION FAIL load_tenant_isolation: pairs=3 median_ratio=2.52 median_increase=58.5ms floor=7ms lb_n_and_no_503=true
K=8 C=8 (after-k8-run2, iso-c8-k8):
ISO_RATIO pair=1 lb_base_p95=40.5ms lb_burst_p95=88.6ms ratio=2.19 host=10cpu-vm3.8 measured_at=2026-10-06T23:36:17Z
ISO_RATIO pair=2 lb_base_p95=39.4ms lb_burst_p95=90.2ms ratio=2.29 host=10cpu-vm3.8 measured_at=2026-10-06T23:36:17Z
ISO_RATIO pair=3 lb_base_p95=40.9ms lb_burst_p95=87.5ms ratio=2.14 host=10cpu-vm3.8 measured_at=2026-10-06T23:36:17Z
ASSERTION FAIL load_tenant_isolation: pairs=3 median_ratio=2.19 median_increase=48.1ms floor=7ms lb_n_and_no_503=true
K=12 C=16 (after-k8-run2, iso-c16-k12):
ISO_RATIO pair=1 lb_base_p95=39.3ms lb_burst_p95=76.3ms ratio=1.94 host=10cpu-vm3.8 measured_at=2026-10-06T23:38:30Z
ISO_RATIO pair=2 lb_base_p95=38.8ms lb_burst_p95=76.5ms ratio=1.97 host=10cpu-vm3.8 measured_at=2026-10-06T23:38:30Z
ISO_RATIO pair=3 lb_base_p95=40.2ms lb_burst_p95=75.0ms ratio=1.87 host=10cpu-vm3.8 measured_at=2026-10-06T23:38:30Z
ASSERTION PASS load_tenant_isolation: pairs=3 median_ratio=1.94 median_increase=37.0ms floor=7ms lb_n_and_no_503=true
```

K = C / 2 = 8 is the launcher value: it is the lowest K the load shape allows (128 BURST clients on 16 keys = 8 per
key, and assertion 2 requires `key_limit` = 0 in BURST) and gave 1.85 (run 3) / 1.86 (run 2) / 1.91 (run (a)). C stays
16 (§67.2): C = 8 with K = 8 isolates worse (2.19), because the bursting key then also fills the queue ahead of B.

**C sweep** (64 clients on 16 keys, 45 s, P = C + 2, same boot; K is irrelevant here at 4 clients per key):

```
SWEEP C=8 P=10 L64 group=LA clients=64 op=enumerate n=5590 p50=369.0ms p95=399.5ms p99=424.6ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:20:20Z
SWEEP C=8 P=10 L64 group=LA clients=64 op=tools_list n=2397 p50=333.9ms p95=362.6ms p99=388.1ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:20:20Z
SWEEP_ASSERT C=8 ASSERTION PASS load_no_pool_or_statement_timeout: L64: DEPENDENCY_UNAVAILABLE=0 INTERNAL=0 drops=0
SWEEP_ASSERT C=8 ASSERTION PASS load_pg_backends_bounded: client_backends_peak=26 (<= 80) role_gateway_peak=12 (<= 2x10) sampled=true
SWEEP C=16 P=18 L64 group=LA clients=64 op=enumerate n=5683 p50=358.7ms p95=527.8ms p99=634.1ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:21:16Z
SWEEP C=16 P=18 L64 group=LA clients=64 op=tools_list n=2439 p50=287.3ms p95=319.3ms p99=336.3ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:21:16Z
SWEEP_ASSERT C=16 ASSERTION PASS load_no_pool_or_statement_timeout: L64: DEPENDENCY_UNAVAILABLE=0 INTERNAL=0 drops=0
SWEEP_ASSERT C=16 ASSERTION PASS load_pg_backends_bounded: client_backends_peak=34 (<= 80) role_gateway_peak=20 (<= 2x18) sampled=true
SWEEP C=24 P=26 L64 group=LA clients=64 op=enumerate n=5619 p50=366.8ms p95=670.1ms p99=869.0ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:22:12Z
SWEEP C=24 P=26 L64 group=LA clients=64 op=tools_list n=2414 p50=244.0ms p95=281.6ms p99=300.8ms failed=0 admission_503=0 drops=0 host=10cpu-vm3.8 measured_at=2026-10-07T03:22:12Z
SWEEP_ASSERT C=24 ASSERTION PASS load_no_pool_or_statement_timeout: L64: DEPENDENCY_UNAVAILABLE=0 INTERNAL=0 drops=0
SWEEP_ASSERT C=24 ASSERTION PASS load_pg_backends_bounded: client_backends_peak=42 (<= 80) role_gateway_peak=28 (<= 2x26) sampled=true
```

Throughput is the same at C 8 / 16 / 24 (n ≈ 7990–8120 per 45 s); C only trades enumerate p95 (400 / 528 / 670 ms)
against tools/list p95 (363 / 319 / 282 ms). C = 16 stays (§67.2); nothing measured argues for moving it on this host.

**Load faults** (`TW/c38_load_faults.log`; gate `c38_load_faults_recorded`). Assertions a fault run does not grade are
`NOT_APPLICABLE` with the missing phases named (§57.1), so each `ASSERTION FAIL` line below is the fault's own:

```
### FAULT fault-k env=[HUMAUX_GATEWAY_ADMISSION_PER_KEY_LIMIT=80] args=[--only ISO]
ISO_RATIO pair=1 lb_base_p95=41.4ms lb_burst_p95=368.8ms ratio=8.90 host=10cpu-vm3.8 measured_at=2026-10-07T03:24:26Z
ISO_RATIO pair=2 lb_base_p95=38.6ms lb_burst_p95=364.4ms ratio=9.44 host=10cpu-vm3.8 measured_at=2026-10-07T03:24:26Z
ISO_RATIO pair=3 lb_base_p95=38.1ms lb_burst_p95=381.1ms ratio=10.01 host=10cpu-vm3.8 measured_at=2026-10-07T03:24:26Z
ASSERTION FAIL load_tenant_isolation: pairs=3 median_ratio=9.44 median_increase=327.4ms floor=7ms lb_n_and_no_503=true
### FAULT fault-p env=[HUMAUX_GATEWAY_PG_POOL_MAX_CONNECTIONS=2 HUMAUX_GATEWAY_PG_ACQUIRE_TIMEOUT_MS=100] args=[--only L64 --levels 64 --pool-max 2]
### FAULT fault-p1 env=[HUMAUX_GATEWAY_PG_POOL_MAX_CONNECTIONS=1 HUMAUX_GATEWAY_PG_POOL_MIN_CONNECTIONS=1 HUMAUX_GATEWAY_PG_ACQUIRE_TIMEOUT_MS=100] args=[--only L64 --levels 64 --pool-max 1]
ASSERTION FAIL load_no_pool_or_statement_timeout: L64: DEPENDENCY_UNAVAILABLE=100 INTERNAL=0 drops=0
ASSERTION FAIL load_pg_backends_bounded: client_backends_peak=17 (<= 80) role_gateway_peak=3 (<= 2x1) sampled=true
### FAULT fault-p-c64 env=[HUMAUX_GATEWAY_PG_POOL_MAX_CONNECTIONS=2 HUMAUX_GATEWAY_PG_ACQUIRE_TIMEOUT_MS=100 HUMAUX_GATEWAY_ADMISSION_CONCURRENCY=64] args=[--only L64 --levels 64 --pool-max 2]
ASSERTION FAIL load_no_pool_or_statement_timeout: L64: DEPENDENCY_UNAVAILABLE=5653 INTERNAL=0 drops=0
### FAULT fault-fwd env=[] args=[--only FWD --iso-secs 10 --rfc7239-lane f --lane f=01a11459-d209-7b0c-8b84-4ff5aac041ab:LOAD_A_BEARER_1]
ASSERTION FAIL load_forwarded_lanes_keyed: want ["2001:db8:f::/64"] got []
### FAULT fault-a3 env=[] args=[--only ISO-BURST-1 --ops-url http://127.0.0.1:19101/metrics]
ASSERTION FAIL load_admission_counter_equals_503s: ISO-BURST-1: scraped=0 client_503=1120
### FAULT fault-teardown env=[] args=[] (teardown skipped: assertion 7 graded on the rows before it)
ASSERTION FAIL load_tenants_torn_down(outbox/jobs/memory_evidence/tenants/rate_buckets before=0/0/0/2/55 after=0/0/0/2/55): after_not_0/0/0/0/0
```

- **F-K** (K = C + Q = 80): B's p95 next to the burst is 8.9–10.0× its baseline (median increase 327 ms) → K stays.
- **F-P as designed** (P = 2, acquire 100 ms) stayed GREEN in every run (0 `DEPENDENCY_UNAVAILABLE`, L64 enumerate p95 692 ms in `after-fix`):
  admission caps the pool's waiters at C = 16, and each waits less than 100 ms for one of 2 connections. Recorded, not
  the red. The red comes from the two variants that really exhaust the pool: **F-P1** (P = 1, acquire 100 ms) → 100
  `DEPENDENCY_UNAVAILABLE` (5 in run (a)) (and the backend bound fails too: role_gateway 3 > 2 × 1), and **F-P-C64** (P = 2,
  acquire 100 ms, C = 64, so admission no longer bounds the pool's waiters) → 5653 (1282 in run (a)). A tighter variant, P = 2 with
  acquire 10 ms (second AFTER run), never booted: the gateway refused at boot because its first connect took longer
  than 10 ms (`ASSERTION FAIL load_gateway_ready: 127.0.0.1:18080/readyz not 200 after 90s: connect: Connection refused (os error 61)`).
- **F-FWD** (lane `f` sends `Forwarded: for="[2001:db8:f::<n>]"`): the edge parser refuses the form, no
  `2001:db8:f::/64` preauth row is written, assertion 8 FAILs.
- **F-A3** (review fix; phase ISO-BURST-1 scraped from the rehearsal gateway's ops port 19101, whose counter never
  moves): the client counted 1120 `key_limit` 503s, the scrape 0 → assertion 3 FAILs.
- **F-TEARDOWN** (review fix; no gateway run): assertion 7 graded on LA / LB as if their teardown had not run — the
  rows are really there (2 tenants, 55 rate buckets) → FAIL; the run's own assertion 7, after the real teardown, is
  `before=0/0/0/2/55 after=0/0/0/0/0` PASS.

Runs and wall clock (all `zsh TW/c38_rehearse.sh after`, release, SOAK_SECS = 0, EXIT 0 and 131/0 each; the earlier
evidence directories are kept): `after-k16/` 06:56:21–07:21:22 (25 min, K 16, sweep + F-K / F-P / F-FWD + the R-10
ISO rerun, which repeated 2.38 / 2.50 / 2.49: not noise), `after-k8-run2/` 07:22:20–07:49:02 (27 min, K 8, iso-c8-k8 /
iso-c16-k12, F-P tight), `after-k8-run3/` 07:49:12–08:12:37 (23 min, K 8, F-P1 / F-P-C64 added; its subset-run FAIL
lines predate `NOT_APPLICABLE`), `after/` = S7's run (a), 08:14:18–08:39:36. Those verdicts counted none of the
load lines (review fix). `after-fix/` = the AFTER above, 11:12:44–11:39:42 (27 min), 140/0 with the nine load lines counted.

## Pool keys and session timeouts (S2 notes; D-A, D-B, rulings R-2 / R-3 / R-5)

Keys (each `entry(..)`, `default: None`, boot-fatal when absent or empty; read only by `humaux-gateway`):
`HUMAUX_GATEWAY_PG_POOL_MAX_CONNECTIONS` (≥ 1), `HUMAUX_GATEWAY_PG_POOL_MIN_CONNECTIONS` (≤ max),
`HUMAUX_GATEWAY_PG_ACQUIRE_TIMEOUT_MS` (> 0), `HUMAUX_GATEWAY_PG_IDLE_TIMEOUT_SECONDS` (> 0),
`HUMAUX_GATEWAY_PG_MAX_LIFETIME_SECONDS` (> idle), `HUMAUX_GATEWAY_PG_STATEMENT_TIMEOUT_MS` (> 0, < handler timeout),
`HUMAUX_GATEWAY_PG_IDLE_IN_TRANSACTION_TIMEOUT_MS` (> 0, ≤ handler timeout). Boot refuses a violated order naming
the key and the order `lock_timeout < statement_timeout < handler timeout` (S3 adds the lock_timeout key to the same
check). Launcher values (rehearsal, `e2e-onboard`): 18 / 2 / 1000 ms / 300 s / 1800 s / 10000 ms / 10000 ms against a
20 s handler — dev-host start value, A5 re-measure required (R-5; the S7 `LOAD` lines are cited here once measured).
The gateway test fixtures keep the sqlx 0.8.6 defaults they ran on (10 / 0 / 30 s / 600 s / 1800 s) with both session
timeouts at 4000 ms under their 5 s handler.

Mechanism (W-3): `PgConnectOptions::options` appends `-c statement_timeout=<ms> -c idle_in_transaction_session_timeout=<ms>`
to the startup `options` parameter (sqlx-postgres 0.8.6 `src/options/mod.rs:426-441`, sent at
`src/connection/establish.rs:45`), after any `options` the DSN already carries, so both combine. The session reports
them with `pg_settings.source = client` and keeps them across pooled reuse (sqlx issues no `DISCARD ALL`).
`pg_db_role_setting` stays at 0 rows (gate `c38_no_role_guc`).

Evidence (each test on its own throwaway database `humaux_thread_c38_<purpose>_<pid>`, dropped WITH (FORCE) by a guard;
each shown red once under its named fault, `TW/c38_red_s2.log`):

| Test (`crates/adapters/src/postgres.rs`) | Answers | Fault → red |
|---|---|---|
| `pool_settings_reach_the_session_as_client_settings` | W-3: `application_name`, `statement_timeout`, `idle_in_transaction_session_timeout` all `source = client` | `.options(..)` removed → `("statement_timeout", "0", "default")` |
| `a_dropped_query_runs_on_until_statement_timeout_cancels_it` | W-1: no CancelRequest on drop — the dropped `pg_sleep(5)` is still `active` 50 ms after the 100 ms client timeout; statement_timeout 500 ms ends it before +1000 ms | statement_timeout option removed → still active at +1000 ms |
| `a_dropped_query_holds_its_pool_slot_until_statement_timeout` | W-1 / L13: pool max 1 — an acquire right after the drop fails `PoolTimedOut` (200 ms); at +1300 ms it succeeds | statement_timeout option removed → `PoolTimedOut` at +1300 ms |
| `an_awaited_query_past_statement_timeout_is_57014` | W-4: an awaited statement past statement_timeout is SQLSTATE 57014 | statement_timeout option removed → the statement succeeds |
| `an_idle_transaction_past_its_timeout_loses_its_session` | W-4: the next statement after 800 ms idle in a transaction (timeout 300 ms) fails with FATAL 25P03 "terminating connection due to idle-in-transaction timeout" | that option removed → the statement succeeds |
| `bootstrap::tests::every_card38_key_is_required_and_named` | §78.1 / R-5: each key once, no default, boot without it or with it empty names it | one key moved to `entry_with_default` |
| `bootstrap::tests::timeout_ordering_is_refused_at_boot` | D-B order | the statement-timeout order check removed |

Why the slot is held (F19, W-1): `PoolConnection::drop` spawns `return_to_pool()` (sqlx-core 0.8.6
`src/pool/connection.rs:199-208`), which tests the connection on release with `self.raw.ping().await` (`:314`); the
Postgres ping writes a Sync and waits for every pending ReadyForQuery (sqlx-postgres 0.8.6
`src/connection/mod.rs:176-185`), i.e. until the abandoned server statement ends. Hence checked-out connections ≤
C·k + 1 + D (D-A) and the bound L6 / L13 is statement_timeout.

The workspace has one pool-builder site (`open` in postgres.rs; gate `c38_one_pool_options_site`): the two test-only
raw pools in `crates/adapters/tests/public_runtime.rs` and `outbox_batch_remember.rs` now open through
`Pool::<Postgres>::connect`, and the dep-map fixture string assembles the builder name.

## Single-transaction rate buckets (S3 notes; D-D, D-E, rulings R-1 / W-4 / W-5 / W-10)

`crates/adapters/src/quota_repo.rs::consume_rate_batch(pool, charges, lock_timeout) -> Result<(), (ErrorCode, usize)>`:
all charges resolve to one `(tenant, user)` binding (`InvalidInput` otherwise); one transaction binds the tenant
once, runs `SELECT set_config('lock_timeout', '<ms>ms', true)` from the key, takes one advisory lock per charge in
`lock_order` — tier (tenant 0, user 1, credential/`SHARED_RATE_OPERATION` 2, credential/operation 3, pre-auth ip 4),
then the lock key, a total order — inserts the missing rows with one `unnest` statement, and runs the unchanged
refill-and-take CTE per charge in INPUT order. The first denial commits what was taken before it and returns
`(RateLimited, index)`; the guard counts `rate_limit_rejected_total{scope}` with the denied charge's scope, as before.
`consume_rate` is a batch of one (the pre-auth `ip` bucket, still its own transaction under the SYSTEM tenant —
D-D rejected d). `guard::postauth_rate` makes the one batch call (gate `c38_one_postauth_call`); the workspace keeps
one rate advisory-lock statement (gate `c38_one_rate_lock_site`) and no `lock_timeout` or `/64` literal
(`c38_no_rate_literals`). In the rate path only, 55P03 / 57014 / 40P01 map to `DependencyUnavailable` (503, never
`RATE_LIMITED`, never `Conflict`); the other mappers keep their arms until card 38b (E-7).

Keys: `HUMAUX_GATEWAY_RATE_LOCK_TIMEOUT_MS` (> 0, < `PG_STATEMENT_TIMEOUT_MS`, refused at boot naming the order
`lock_timeout < statement_timeout < handler timeout`) and `HUMAUX_GATEWAY_RATE_PREAUTH_IPV6_PREFIX_BITS` (1..=128;
the repository also refuses 0 / 129). Launcher values 2000 ms (the former literal) and 64 — dev-host start value,
A5 re-measure required; the load step's `LOAD_GW_ENV` pins both because assertion 8 expects `/64`. Keying (W-10):
`to_canonical()` first, so `::ffff:a.b.c.d` keys as the IPv4 address; loopback `::1` and link-local `fe80::/10` key
as themselves (`/128`); any other IPv6 address keys as `{masked}/{bits}`. No migration: the dev rows are already
`/64`, and a subject string from another prefix ages out through card 35's idle-bucket purge.

Evidence (DB tests each on their own throwaway database `humaux_thread_c38_rate_<pid>_<n>`, dropped WITH (FORCE) by the
fixture's Drop; each shown red once under its named fault, `TW/c38_red_s3.log`; structural gate reds in
`TW/c38_gate_reds_s3.log`):

| Test | Answers | Fault → red |
|---|---|---|
| `quota_repo::tests::lock_order_is_tier_order_for_every_input_permutation` | all 24 input orders lock tenant → user → credential/shared → credential/operation | sort removed → `[3, 0, 1, 2]` |
| `c38_reversed_batches_with_a_queued_waiter_do_not_deadlock` (`crates/adapters/tests/quota_and_rate.rs`) | W-5: holder H on the user lock, batch B (user, tenant) and batch A (tenant, user) queued behind it; H commits; both `Ok` inside 3 s | sort removed → `Err((DependencyUnavailable, 1))` (40P01 after `deadlock_timeout` = 1 s) |
| `c38_batch_denial_spends_exactly_what_the_sequential_path_spent` | parity: a denial at index 2 spends credential 1, user 1, tenant (drained) 1, operation 0 — the same as four sequential transactions | evaluation in tier order → only the tenant bucket spent |
| `c38_a_rate_lock_wait_past_lock_timeout_is_dependency_unavailable` | W-4: holder 400 ms, lock_timeout 100 ms → 55P03 → `(DependencyUnavailable, 2)` before the holder lets go | 55P03 arm removed → `Err((Conflict, 2))` |
| `quota_repo::tests::preauth_ipv6_prefix_bits_from_config` | D-E / W-10: 56 shares a /56, 64 splits it, 128 = the address, `::ffff:` → IPv4, `::1` and `fe80::1:2` → `/128` | bits ignored (fixed 64) → `/64` where `/56` was expected |
| `c38_a_rate_statement_timeout_is_dependency_unavailable` (review fix) | D-B: through `connect_with` (statement_timeout 150 ms as a startup option, lock_timeout 5 s) a wait on the held tenant lock is cancelled 57014 → `(DependencyUnavailable, 1)` before the holder lets go | 57014 dropped from the arm → `Err((Internal, 1))` (`TW/c38_red_s8.log`) |
| `c38_a_rate_deadlock_victim_is_dependency_unavailable` (review fix) | W-5: H holds the user lock, the batch holds the tenant lock and queues on user, H then asks for tenant; the batch waited first, so the detector picks it → 40P01 → `(DependencyUnavailable, 0)`; H then gets the tenant lock | 40P01 dropped from the arm → `Err((Conflict, 0))` (`TW/c38_red_s8.log`) |

The shared request-guard `Fixture` of `quota_and_rate.rs` now holds a process-wide mutex from `isolate()` until its
`Drop` has deleted the fixture rows: under the default parallel harness (`--include-ignored`, gate
`c38_quota_and_rate_all`) one test's Drop deleted the tenant another test was using (8 of 14 red with
`TenantBoundary` / missing windows on the first S3 run; 14 of 14 green after). The serial lane already ran them one at
a time.


## Admission layer (S4 notes; D-C, rulings R-6 / W-3 / W-8 / W-11)

`bins/gateway/src/admission.rs`: one `axum::middleware::from_fn_with_state` layer; `admission::wrap(mcp, supervision,
admission)` returns `mcp.layer(..).merge(supervision)` and `main` is its one caller (gate `c38_one_admission_wrap`), so
`/livez` and `/readyz` never pass it and the drain 503 never waits on admission. Per request:

0. the whole body is read under `tokio::time::timeout(B, axum::body::to_bytes(body, MAX_REQUEST_BODY_BYTES))` before
   any admission state is touched — over the limit 413, timeout 408 with `Connection: close`; the request is rebuilt
   with `Body::from(bytes)`, and `native_request_boundary`'s own `to_bytes` returns at once (crates/protocol unchanged);
1. the per-credential slot (key = first 8 bytes of SHA-256 of the raw `Authorization` value, 0 when absent; never
   logged, never an authorization input) — refused `key_limit` at K in flight + waiting;
2. `Semaphore::try_acquire` (tokio hands a released permit to the first queued waiter, so a newcomer never jumps the
   queue — W-3: tokio `sync/semaphore.rs` FIFO acquire);
3. the waiter count, refused `queue_full` at Q (Q = 0 = no waiting);
4. `tokio::time::timeout(W, acquire())`, refused `wait_timeout`.

The permit, the waiter count and the key slot are Drop guards, so a client that disconnects while queued (its future
is dropped) and a handler panic release them. Once admitted, the request runs in a `tokio::spawn`ed task that owns
the ticket and that the layer awaits: a client that disconnects drops the layer's future but not the task, so the
permit and the key slot stay held until the handler returns (rmcp-3.1.4 stateless mode spawns each handler with its
JoinHandle dropped, and hyper drops the request future when it reads the client's EOF); a panic in the task resumes in
the layer after the ticket dropped. Overflow = 503, `Retry-After: N` with N = clamp(ceil(waiting × EWMA
service time / C), 1, 30) whole seconds (W-8, RFC 9110 §10.2.3), `Content-Type: text/plain`, body `RATE_LIMITED` (the
pre-parse refusal shape; no JSON-RPC envelope, no `ErrorCode` row change — `error_map.rs`'s ponytail now points here).
`admission_rejected_total{class="gateway_inbound",reason}` gains exactly 1 per 503; no log line per refusal (D-C
rejected h). The counter and the closed `reason` set (`AdmissionRefusal`, re-exported as `admission::Refusal`) live in
`crates/telemetry/src/admission.rs` so the G80-6 witness — which may depend on library crates only — drives the real
emit and `metrics-registry` D5 counts its one `.inc(` site; `GuardMetrics::render` renders it after the six guard
families (`--metrics-families`, `/metrics`).

Keys (§78.1, `default: None`, boot-fatal when absent or empty, declared once — gate `c38_keys_declared`):
`HUMAUX_GATEWAY_ADMISSION_CONCURRENCY` C ≥ 1, `..._QUEUE_DEPTH` Q ≥ 0, `..._MAX_WAIT_MS` W > 0, `..._PER_KEY_LIMIT`
1 ≤ K ≤ C + Q, `..._BODY_READ_TIMEOUT_MS` 0 < B ≤ the handler timeout. Launcher values C 16 / Q 64 / W 5000 / K 16 /
B 5000 — the §67.2 start values, dev-host start value, A5 re-measure required (S7 measured them and set K = 8, see
"Measurement" → AFTER).
The rehearsal's load step pins them in `LOAD_GW_ENV` and restarts its scratch gateway with W = 1 ms before BURST-W
through a request/ack file handshake (`load_burst_w_listener`; the `sh -c` hook of `xtask load` has neither
`start_gw` nor the launcher's secrets). Supervision: SIGTERM→SIGKILL grace > `DRAIN_ANNOUNCE_WINDOW` + B + W + handler
timeout + finalize timeout (`docs/ops/supervision.md` §3); a connection stalled inside its headers is unbounded (W-11:
`axum::serve` sets no hyper timer) and holds no permit — L14.

§78.6 text (R-6), Baseline edited in the same change: §41.2 row `admission_rejected_total{class, reason}` with the
closed `reason` set; §67.2 gains the gateway-inbound landing paragraph (body read under B first, per-credential K,
`gateway_inbound`, integer `Retry-After` 1..=30, pre-parse `RATE_LIMITED` body, `/livez` `/readyz` outside); the §67.2
rate-limit row now names the blocking `pg_advisory_xact_lock` in tier order with the `lock_timeout` key (D-D).

| Test (`admission::tests`, stub router, no DB) | Answers | Fault → red (`TW/c38_red_s4.log`) |
|---|---|---|
| `queue_zero_rejects_the_seventeenth_at_once` | C 16, Q 0, K 16, 17 distinct credentials: the 17th is 503 in < 100 ms, `Refusal::QueueFull`, `queue_full` +1, `key_limit` +0 | waiter-cap check removed → refused only after W (2.0 s) |
| `a_waiter_past_max_wait_gets_503_retry_after_rate_limited` | C 1, Q 1, W 200 ms: 503, integer `Retry-After` in 1..=30, `text/plain`, body `RATE_LIMITED`, `wait_timeout` +1 | header dropped → "a Retry-After header" |
| `cap_one_two_requests_counts_one_rejection` | §42 injection: cap 1, two concurrent requests → the counter +1 exactly | increment removed → 0 ≠ 1 |
| `a_dropped_waiter_frees_its_queue_slot` | an aborted queued `enter` leaves the queue; the next waiter queues and is admitted; every key slot released | guard replaced by a decrement after the await → waiting stays 1 |
| `one_key_at_its_limit_is_rejected_while_another_key_is_admitted` | K 2: the third request of key a is `KeyLimit` while key b is admitted | per-key check removed → admitted |
| `slow_body_clients_hold_no_permit` | real TCP, C 16: 16 connections stall after 1 body byte; a 17th complete request is 200 in < 500 ms | body read moved after the permit → 503 |
| `a_stalled_body_gets_408_after_the_body_read_timeout` | B 200 ms: 408 + `Connection: close` within 2 s | timeout removed → no response within 2 s |
| `supervision_routes_bypass_admission` | C 1 held, Q 0: `/readyz` and `/livez` still 200 | layer applied after the merge → 503 |
| `a_client_that_disconnects_keeps_its_permit_until_the_handler_ends` (review fix) | real TCP, C 1, Q 0: a client sends a whole request and closes its socket; 300 ms later a second credential is 503 `queue_full` while the first handler still runs; once it ends the permit is back and the next request is 200 | `next.run` awaited in the request future → the second request admitted, no reply within 5 s (`TW/c38_red_s8.log`) |

## Debts A / B / C (S5 notes; D-G, D-H, D-I, rulings R-13 / R-3)

### Debt A — transient query-embedding failure: FAIL FAST on the server, one jittered retry in the client (D-G)

**Ruling.** The gateway does not retry the query embedding. `recall.rs` folds a worker `UNAVAILABLE` (any
`failure_code` but `SCAN_REJECTED`) into `DEPENDENCY_UNAVAILABLE`, and `GatewayRetrievalEmbeddingClient::embed_query`
dials the worker once per recall (`bins/gateway/src/retrieval_embedding_client.rs`, one `self.dial(..)` per call). A
server-side retry would hold one of the C admission permits for 2× the provider timeout (≈ 5 s per attempt in the
card-31 ledger) plus the jitter; during a provider blip recalls would hold every permit and admission would refuse
the PG-only operations too — the coupling D-C exists to prevent.

**Evidence.** Card-31 outage evidence: dashscope 34 failures in 1054 calls, clustered in one blip; uncorrelated single
failures are rare. Test `a_transient_query_embedding_failure_fails_fast_with_one_worker_call`
(`bins/gateway/tests/semantic_recall_wiring.rs`): a stub retrieval-worker RPC on a UDS answers its first call
`UNAVAILABLE / PROVIDER_TRANSIENT` and later calls `EMBEDDED`; through the real `GatewayRetrievalEmbeddingClient`
and `recall::search`, the first recall is `DependencyUnavailable` in < handler timeout / 2 (measured 2026-10-07 on the dev host: 1.75 s, of which the cold `role_gateway` pool's first connects are most — the answering recall right after took 40 ms; the stub answers at once)
after exactly 1 stub call, and the next recall answers with the seeded memory. The soak client retries once on a 503
or a wire code in {`DEPENDENCY_UNAVAILABLE`, `PROVIDER_TRANSIENT`, `RATE_LIMITED`}, after `Retry-After` when present,
else a uniform 500–1500 ms jitter; `op_failure_rate` (1 % bound, unchanged) scores the answer after the retry, the
report adds `latency[].retried_calls` and `first_attempt_failure_rate {value, unit, n}`, and a first-attempt failure
rate above 0.05 prints a WARN line (informational). Nothing loosens a threshold.

**What fail-fast does not fix** (stated as fact, L12): `ProviderHealthRegistry`
(`crates/retrieval-provider/src/health.rs`, Called-by `[retrieval-provider::metrics, tests]`) has no caller under
`bins/` (grep count 0 on 2026-10-07, F24), so nothing cuts a correlated blip short. Each failing recall still holds a
permit for up to one provider timeout (≈ 5 s): at C = 16 about C / 5 s ≈ 3.2 failing recalls/s hold every permit, and
`remember.put` / `memory.enumerate` then get 503. A blip of the card-31 shape (correlated, ≈ 1 h) still fails the soak
1 % recall bound — the client retry 0.5–1.5 s later lands inside the same blip — and that red is correct.

**Reopening signal:** `admission_rejected_total` rising while `ops.model_call_ledger` shows the embedding provider
above 50 % FAILED over 5 min, or a second rehearsal red on recall attributed to a provider blip. Upgrade (card 38b):
wire `ProviderHealthRegistry` on the query-embedding path so recalls fail in milliseconds during a known-down window,
and/or a recall sub-limit (≤ C/2 permits).

| Test | Answers | Fault → red (`TW/c38_red_s5.log`) |
|---|---|---|
| `a_transient_query_embedding_failure_fails_fast_with_one_worker_call` | stub RPC fails once: first recall `DependencyUnavailable` after 1 call, inside the deadline; next recall answers | retry loop in `retrieval_embedding_client` → first recall answers (`left: None`) |
| `soak::tests::a_transient_failure_retried_once_counts_as_one_ok_call` | 503 `RATE_LIMITED` + `Retry-After: 2` then OK → one call, ok, retried, paused 2 s; `op_failure_rate` passes n = 1; `retried_calls` 1 | retry removed → `ok: false` |
| `soak::tests::a_failure_after_the_retry_still_counts_failed` | `DEPENDENCY_UNAVAILABLE` twice → exactly 2 attempts, one 500–1500 ms pause, counted failed; `INVALID_INPUT` not retried | second retry added → 3 attempts |

### Debt B — provider-wide distill breaker: DEFERRED, no code (D-H)

**Ruling.** No code in card 38. §67.2's private reasoning in-flight 4 is already a provider-wide bound: after an
outage all four slots together send at most 4 concurrent calls, so a lockstep retry cannot become a herd larger than
4. Jitter in `retry_backoff_seconds` is left out too (no measured effect under the 4-slot bound).

**Evidence.** Card-31 outage (`card31_rehearsal_evidence/chain_red_0711_provider_outage`): `dead=0` in every distill
dispatch line over a ≈ 1 h provider blip — the per-job backoff carried it (F15). A "known down → no attempt spent"
flag would only matter past the attempt budget's backoff sum, lease·(1+2+4+8) capped at 300 s per step ≈ 16 min at
lease 120 s with max_attempts 5; that case is the operator path `jobs requeue-dead --tenant` (ADR-0058 R4).

**Reopening signal:** any DEAD `DERIVED_DISTILL` job whose `last_error_class` is transient (PROVIDER_TRANSIENT /
PROVIDER_ERROR / timeout), in a rehearsal (`distill_dead` graded assertion) or in production; OR
`ops.model_call_ledger` showing one provider above 50 % FAILED over 15 min while distill jobs reach attempt ≥ 3.
Upgrade: ADR-0060 L1/L2, worker-written negative provider health so a down route parks.

### Debt C — route-health prober: DEFERRED, no code (D-I)

**Ruling.** No prober in card 38.

**Evidence.** (1) A reachability probe (TCP/TLS or `GET /models`) cannot honestly assert the four §11.2.5 account
components HEALTHY — billing and credit verdicts are not observable without a billable call, so HEALTHY rows written
from it would turn fail-closed admission into fail-open. (2) `control.observe_reasoning_route_health(model_call_id,
…)` derives every identity column from one finalized routed call (F16, ADR-0060); a prober needs a new owner definer,
a new `source_kind`, a migration and an rls-check arm — an architecture change outside this card's capacity remit.
Workaround (unchanged): an idle route parks `ROUTE_HEALTH_STALE` with no spend, and the operator runs
`reasoning attest-health --valid-for-secs`.

**Reopening signal:** the first production tenant whose distill traffic interval exceeds
`HUMAUX_PRIVATE_WORKER_HEALTH_RENEW_SECS`, OR any NOT_READY `ROUTE_HEALTH_STALE` settle older than 1 h for a tenant
with PENDING distill work. Upgrade (with §11.2.2 R4, multi-profile routing — ADR-0060): a prober that renews only the provider-endpoint component, with
a separate validity.

## Review fixes (2026-10-07; findings of the review pass, each changed at the root)

- **P1 admission.rs — a disconnect freed the permit while the work ran on.** Root cause: the ticket lived in the
  layer's request future; rmcp-3.1.4's stateless transport spawns each handler detached
  (`streamable_http_server/tower.rs` `tokio::spawn(serve_directly_with_ct(..))`, `service.rs` `spawn_service_task`) and
  nothing in `crates/protocol` or `bins/gateway` reads its cancellation token, while hyper drops the request future on
  the client's EOF. Fix: the admitted work runs in its own task that owns the ticket (D-C); D-A's D term, D-B, L6 and
  L13 now name only the handler timeout. Test 31; red `work_awaited_in_the_request_future`.
- **P1 quota_repo.rs — the 57014 and 40P01 arms were untested.** Two DB tests on their own throwaway databases produce
  the real SQLSTATEs through the real batch: 57014 from a `connect_with` pool's statement_timeout on a held tenant lock,
  40P01 from a wait cycle the server's detector breaks (tests 32, 33; gates `c38_rate_57014_named`,
  `c38_rate_40P01_named`).
- **P1 rehearse.sh — the load seed line broke two inherited count gates.** The load step's `LOAD_SEED_ARGS` is a fourth
  `--no-lane --processor-id` seed site (LA and LB onboard through `e2e-seed --no-lane`, exactly what
  `r10_one_capabilities_definition` and `c33b_rehearsal_onboards_through_doors` protect: seeds carry no lane flags; the
  lanes come through the onboarding doors). Their card-38 copies now expect 4; each was shown red once with the load
  line's `--no-lane` removed (`TW/c38_gate_reds_s8.log`). The card-37 list is unchanged.
- **P1 rehearse.sh — load assertions bypassed the verdict.** Root cause: `A_OK` / `A_BAD` and `assert_eq` were defined
  in step 6b, after the load step, which echoed its lines directly. Fix: the counters and one printer
  (`assertion_line`) are defined before step 2d; xtask load's lines are tallied (`load_tally`), the "not seeded" and
  assertion-7 lines go through `assert_eq`. Gate `c38_load_assertions_counted` (one live `echo "ASSERTION ` line, one
  `load_tally` call, the counters before `step load`), red twice in `TW/c38_gate_reds_s8.log`. The BEFORE run and
  `after-k16/` printed `ASSERTION FAIL load_*` lines and exited 0 with 131/0: that verdict did not count them.
- **P1 ADR — the D-D cost was an estimate.** The `LOCKWAIT` line measures the tenant-lock wait; the single-tenant
  throughput is LA's L64 call rate. Both, and the enumerate tail they explain, are under "Measurement" → AFTER and in
  D-D.
- **P1 load.rs — assertions 3 and 7 were never red; 7 was vacuous.** Assertion 3: unit test 34 and the load fault F-A3
  (phase ISO-BURST-1 scraped from the rehearsal gateway's ops port, whose counter never moves). Assertion 7 now reads
  the tenant and rate-bucket rows the load writes (see "Measurement harness (D-F)"); fault F-TEARDOWN grades it on the
  live load tenants as if their teardown had not run.
- **Uncaught fault — `c38_measurements` had slack.** It compared `-ge 20` / `-ge 3`; it now compares the exact numbers
  of `LOAD`, `SWEEP` and `LOCKWAIT` lines this file carries, so deleting any one is red.

## Tests (one fault per test)

Each test was shown red once under exactly one named fault, mutated and restored by writing the file and touching it;
the log holds `### FAULT <name>`, the mutation, the command and the `test result: FAILED` line (gate
`c38_red_recorded`: 35 faults, 35 reds; rows 29–30 are S7's, rows 31–35 the review fix's). What each test answers is
in the slice notes above and under "Review fixes".

| # | Test | Fault (`### FAULT` name) → red | Log |
|---|---|---|---|
| 1 | `load::tests::keepalive_reads_content_length_and_chunked_bodies` | `chunked_decoding_removed` → "second response on the same connection: read body: Resource temporarily unavailable" | `TW/c38_red_s1.log` |
| 2 | `load::tests::report_carries_n_p50_p95_and_503s_per_level` | `refusals_counted_as_failed_calls` → "503s are not failed_calls" (`RATE_LIMITED: 3` in failed_calls) | s1 |
| 3 | `load::tests::each_lane_sends_its_own_forwarded_for` | `rfc7239_forwarded_form` → `for="[2001:db8:a::1]"`: `FORWARDED_HEADER_UNPARSEABLE` | s1 |
| 4 | `load::tests::no_report_without_a_ready_gateway` | `readiness_check_removed` → "no load-report.json without a ready gateway" | s1 |
| 5 | `load::tests::a_503_pauses_the_client_for_retry_after` | `retry_after_sleep_removed` → next call 0.6 ms after a 503 with `Retry-After: 1` | s1 |
| 6 | `postgres::tests::pool_settings_reach_the_session_as_client_settings` | `options_not_applied` → `("statement_timeout", "0", "default")` | `TW/c38_red_s2.log` |
| 7 | `postgres::tests::a_dropped_query_runs_on_until_statement_timeout_cancels_it` | `statement_timeout_option_removed_cancel` → still `active` at +1000 ms | s2 |
| 8 | `postgres::tests::a_dropped_query_holds_its_pool_slot_until_statement_timeout` | `statement_timeout_option_removed_slot` → `PoolTimedOut` at +1300 ms | s2 |
| 9 | `postgres::tests::an_awaited_query_past_statement_timeout_is_57014` | `statement_timeout_option_removed_57014` → the statement succeeds | s2 |
| 10 | `postgres::tests::an_idle_transaction_past_its_timeout_loses_its_session` | `idle_in_transaction_option_removed` → the next statement succeeds | s2 |
| 11 | `bootstrap::tests::every_card38_key_is_required_and_named` | `card38_key_moved_to_entry_with_default` → "HUMAUX_GATEWAY_PG_IDLE_TIMEOUT_SECONDS must have no default" | s2 |
| 12 | `bootstrap::tests::timeout_ordering_is_refused_at_boot` | `timeout_order_check_removed` → "configuration must be rejected" | s2 |
| 13 | `quota_repo::tests::lock_order_is_tier_order_for_every_input_permutation` | `sort_removed_lock_order` → `[3, 0, 1, 2]` | `TW/c38_red_s3.log` |
| 14 | `c38_reversed_batches_with_a_queued_waiter_do_not_deadlock` | `sort_removed_deadlock` → `Err((DependencyUnavailable, 1))` (40P01) | s3 |
| 15 | `c38_batch_denial_spends_exactly_what_the_sequential_path_spent` | `evaluation_in_tier_order` → only the tenant bucket spent | s3 |
| 16 | `c38_a_rate_lock_wait_past_lock_timeout_is_dependency_unavailable` | `55P03_arm_removed` → `Err((Conflict, 2))` | s3 |
| 17 | `quota_repo::tests::preauth_ipv6_prefix_bits_from_config` | `ipv6_bits_ignored` → `/64` where `/56` was expected | s3 |
| 18 | `admission::tests::queue_zero_rejects_the_seventeenth_at_once` | `waiter_cap_removed` → refused only after W (2.0 s) | `TW/c38_red_s4.log` |
| 19 | `admission::tests::a_waiter_past_max_wait_gets_503_retry_after_rate_limited` | `retry_after_header_dropped` → "a Retry-After header" | s4 |
| 20 | `admission::tests::cap_one_two_requests_counts_one_rejection` | `increment_removed` → 0 ≠ 1 | s4 |
| 21 | `admission::tests::a_dropped_waiter_frees_its_queue_slot` | `waiter_guard_replaced_by_decrement_after_await` → waiting stays 1 | s4 |
| 22 | `admission::tests::one_key_at_its_limit_is_rejected_while_another_key_is_admitted` | `per_key_check_removed` → the third request of key a admitted | s4 |
| 23 | `admission::tests::slow_body_clients_hold_no_permit` | `body_read_after_permit` → the 17th request 503 | s4 |
| 24 | `admission::tests::a_stalled_body_gets_408_after_the_body_read_timeout` | `body_read_timeout_removed` → no response within 2 s | s4 |
| 25 | `admission::tests::supervision_routes_bypass_admission` | `layer_after_merge` → "/readyz while admission is full": 503 | s4 |
| 26 | `a_transient_query_embedding_failure_fails_fast_with_one_worker_call` | `embedding_client_retry_loop` → the first recall answers (`left: None`) | `TW/c38_red_s5.log` |
| 27 | `soak::tests::a_transient_failure_retried_once_counts_as_one_ok_call` | `soak_retry_removed` → `ok: false` | s5 |
| 28 | `soak::tests::a_failure_after_the_retry_still_counts_failed` | `soak_second_retry_added` → 3 attempts | s5 |
| 29 | `load::tests::only_and_rfc7239_lane_shape_the_plan_and_the_graded_lanes` | `only_filter_removed` → `--only FWD` ran `["L1", "BURST", "ISO-BASE-1", "ISO-BURST-1", "BURST-W", "FWD"]` | `TW/c38_red_s7.log` |
| 30 | `load::tests::an_assertion_whose_phases_only_dropped_is_not_applicable` | `only_guard_removed` → without `--only`, `load_tenant_isolation` came back `Some(["ISO-BASE-1", "ISO-BURST-1"])` (a full run would excuse a phase that never ran) | s7 |
| 31 | `admission::tests::a_client_that_disconnects_keeps_its_permit_until_the_handler_ends` | `work_awaited_in_the_request_future` → the second request admitted while the first handler ran: "a reply while the first handler still runs" (no reply in 5 s) | `TW/c38_red_s8.log` |
| 32 | `c38_a_rate_statement_timeout_is_dependency_unavailable` | `rate_57014_arm_removed` → `Err((Internal, 1))` | s8 |
| 33 | `c38_a_rate_deadlock_victim_is_dependency_unavailable` | `rate_40P01_arm_removed` → `Err((Conflict, 0))` | s8 |
| 34 | `load::tests::assertion_3_fails_when_the_scrape_differs_from_the_client_503s` | `a3_compares_scrape_presence_only` → "a scrape that missed every 503 passed: 2 phases" | s8 |
| 35 | `load::tests::lock_wait_quantile_ranks_unobserved_acquisitions_below_the_resolution` | `lock_wait_rank_over_observed_only` → p99 `Some(500.0)` where 11.0 (the 10th longest of 1000) was expected | s8 |

Load-level faults (S7, `TW/c38_load_faults.log`, gate `c38_load_faults_recorded`): F-K (K = C + Q) →
`ASSERTION FAIL load_tenant_isolation`; F-P1 (P = 1, acquire 100 ms) and F-P-C64 (P = 2, acquire 100 ms, C = 64) →
`ASSERTION FAIL load_no_pool_or_statement_timeout` (the design's F-P, P = 2 / acquire 100 ms at C = 16, measured green
and is recorded beside them, "Measurement" → AFTER); F-FWD (lane `f`, RFC 7239 `for="[…]"`) → `ASSERTION FAIL
load_forwarded_lanes_keyed`; F-A3 (ISO-BURST-1 scraped from the wrong ops port) → `ASSERTION FAIL
load_admission_counter_equals_503s`; F-TEARDOWN (assertion 7 graded before the teardown) → `ASSERTION FAIL
load_tenants_torn_down`. Structural gate reds: `TW/c38_gate_reds_s{2,3,4,6,8}.log`.

## Chain run 1 (2026-10-07 13:31–17:07): one red, closed before the commit

- Tree fingerprint `e21bb2ad…` (`TW/c38_run1_tree.txt`), Docker VM 3.8 GiB. Lane 127/127 (14:11); chain 536 green,
  1 red; `stores_not_restarted` unchanged, ceilings 0, stragglers 0; gate-truth 239 binaries / 168 DB-bound /
  0 offenders; `release_build` green; `rehearse_c38` 144/144 including the load phases (BASE, BURST and ISO rounds
  with `admission_503` equal to the rejected counter and 0 drops).
- `maintenance_all_tests` (15:06): `expire_keeps_exactly_the_two_newest_verified_fulls` panicked in `Source::up`
  (`bins/maintenance/tests/support/containers.rs`) because Docker refused `compose up` with "Bind for
  127.0.0.1:55948 failed: port is already allocated". The support module probes a free host port with
  `TcpListener::bind("127.0.0.1:0")` and hands it to compose; Docker keeps its own ledger of published ports, and a
  container of another product line on the shared Docker Desktop had already published that one, which the host
  probe cannot see. An environment collision, not a card-37/38 defect; first seen on this host.
- Fix at the one entry every scratch source goes through: `Source::up` retries `compose up` up to three times,
  re-probing the port and rewriting `HUMAUX_PG_PORT` in the compose env only on that exact refusal
  (`port_collision_is_retried`, a pure predicate with its own test; any other failure still panics with the compose
  output). `Source.env` became a `RefCell` so the retry can rewrite the port behind `&self`; `owner_dsn` and the
  suites already read the port through `env_value` at call time, so a retried port is the port they connect to.
  Witness: fault "the last attempt also retries" → the predicate test red (EXIT 101) → reverted → green,
  `TW/c38_port_retry_fault.log`. The eight gates that read the support module (`maintenance_all_tests`,
  `maintenance_tests`, `c37_backup_suite`, `c37_drill_suite`, `c37_drill_e2e_live`, `c37_drill_corrupt_wal_live`,
  `c37_drill_catalog_faults_live`, `c37_restore_pitr_live`) reran through `gates_subset.sh`
  (`TW/gates_card38_rerun1.log`); fmt, clippy and `dep-map --check` green on the patched tree. Rerun outcome:
  `maintenance_all_tests` green (17:17); the seven others green across `rerun1` / `rerun2` / `rerun3` (17:17–17:33)
  with the store epoch unchanged throughout. Three of them first went red for a second environmental reason: the
  scratch-source floor `free >= 1024 + 768 MiB` (`Source::prepare`, free = Docker `MemTotal` minus the sum of every
  container's usage) was crossed when the other product line on the shared 3.8 GiB Docker VM started two test
  containers in the same minute (`blocked: free=1696 MiB` … `1781 MiB`); the floor is fail-closed under
  `HUMAUX_REQUIRE_DOCKER=1` by design and was not lowered. The reruns waited for free ≥ 2400 MiB by the same measure
  before each attempt; the backup suite passed 15/15 on its third attempt (`rerun3`, free 3158 MiB).
- Not retried: the restore target of `measure.rs` (`restore pitr` with an operator-chosen port) is the product path;
  a collision there surfaces as the compose error, which is the right answer for an operator.

## Known limits

- **L1** Worker, maintenance and admin pools stay on sqlx defaults (10 per pool, lazily grown); their worst-case sum
  can exceed the 97 usable connections, though 42 were observed (soak peak, n = 147). Upgrade: card 38b,
  `PoolSettings` per process.
- **L2** Every number is a dev-host number (10 CPU; PostgreSQL in a 3.8 GiB Docker VM shared with another product
  line). Upgrade: card 39's `a5_load_remeasure` on the A5-spec host (open item O-1).
- **L3** The fairness key is an unverified hash of the `Authorization` value: per credential, not per tenant. A tenant
  that spreads a burst over many credentials, or a forger who rotates tokens, gets K per key, bounded only by the
  global C / Q / W (and, for a forger, by fast authentication failure). Upgrade: a per-tenant limit after
  authentication.
- **L4** Outside the rate path 57014 (statement timeout) still surfaces as `INTERNAL` in the twelve other `db_error`
  mappers. Upgrade: card 38b (R-7).
- **L5** Ad-hoc sessions as `role_gateway` (psql) carry no timeout: the startup options cover the pool only. Upgrade:
  a role GUC, if ever required.
- **L6** A handler timeout abandons its in-flight statement, which runs on for up to statement_timeout after the
  permit is freed (no CancelRequest, F19). The bound is the boot order lock_timeout < statement_timeout < handler
  timeout. A client disconnect abandons nothing: the admitted task runs its handler to the end holding the permit (the
  handler's own timeout bounds it).
- **L7** `Retry-After` comes from an EWMA of admitted service time: coarse, with a 1 s floor and a 30 s cap.
- **L8** `DRAIN_ANNOUNCE_WINDOW` is still a constant (its existing ponytail).
- **L9** Loopback and link-local IPv6 clients key by their full address (/128, W-10): a link-local client that rotates
  addresses gets a fresh pre-auth bucket per address. Only the trusted adapter's client address reaches the keying.
- **L10** The load is closed-loop, so open-loop tail latency (coordinated omission) is not measured (W-9).
- **L11** The provider-wide distill breaker (D-H) and the route-health prober (D-I) are deferred, with their
  reopening signals.
- **L12** `ProviderHealthRegistry` is unwired on the query-embedding path (F24: no caller under `bins/`). During a
  provider blip each failing recall holds an admission permit for one provider timeout (≈ 5 s); about 3.2 failing
  recalls/s fill C = 16 and the PG-only operations get 503. A correlated blip of the card-31 shape still fails the soak
  1 % recall bound. Reopening signal and upgrade: Debt A (D-G).
- **L13** A handler-timed-out request's PG connection stays checked out up to statement_timeout after its permit is
  freed (F19), so while abandoned statements drain an acquire can wait and fail as `DEPENDENCY_UNAVAILABLE` 503 without
  `Retry-After`. Bound: D-A's checked-out ≤ C·k + 1 + D, D ≈ handler-timeout rate × statement_timeout (client
  disconnects add nothing, D-C). Upgrade: raise H in P = C·k + 2 + H from the measured drop rate, or send a
  CancelRequest on drop.
- **L14** A connection stalled inside its request headers is unbounded: `axum::serve` 0.8.9 sets no hyper timer (F18,
  W-11). It costs a task and a socket, never a permit; pre-existing. Upgrade: a hyper-util accept loop with
  `http1().header_read_timeout` and a timer.
- **L15** The load measures reads and the rate-bucket writes only; `remember.put` and recall are not load-measured,
  because both reach a paid provider in the rehearsal (F21). Upgrade: a stub-embedder rehearsal profile with no real
  provider, then both ops join the mix.
- **L16** Enumerate's tail grew against BEFORE at L16–L64 (p95 136.8 → 263.1 ms at L16, 488.2 → 542.4 ms at L64)
  while its p50 fell and tools/list improved. The tenant lock accounts for ≤ 16 ms of it (`LOCKWAIT` p99); the longest
  single `role_gateway` statement grew with it (13 → 510 ms at L16), consistent with 16 instead of 10 concurrent
  enumerate statements on a CPU-bound 10-core host, but not attributed further here. Reopening signal: card 39's
  `a5_load_remeasure` shows enumerate p95 at L16 above 2× BEFORE on the A5 host. Upgrade: sample `wait_event` and the
  statement per backend in `xtask load`, then size C / P against the measured knee.

Ponytail ceilings in code (`bins/gateway/src/admission.rs`): one process-wide semaphore (§67.2 is single node; a
second replica needs shared admission or C / replicas); one `Mutex<HashMap>` for the per-key counts (uncontended at
C + Q ≤ 80); the body buffered before admission, bounded by `MAX_REQUEST_BODY_BYTES` and B per connection (a
connection cap belongs at the listener); the fixed EWMA weight behind `Retry-After`.

## Follow-ups and open items

- **Card 38b** (plan row, R-3): `PoolSettings` keys for every other resident process and per-worker concurrency caps
  (Baseline:345 item 3; L1), the 57014 → `DEPENDENCY_UNAVAILABLE` arm in the other `db_error` mappers or one shared
  classifier (R-7; L4), and a ruling on `tools/call` spending tenant / user / credential/mcp tokens twice (R-12).
- **O-1** (R-9) **blocks card 39 from freezing its compose values.** Card 39's compose carries this card's values
  labelled `dev-host start value, A5 re-measure required`, and its clean-node rehearsal gains the named gate
  **`a5_load_remeasure`**: the same `cargo xtask load` phase set on the 4-vCPU / 24 GiB ARM runner, appending
  `LOAD … host=a5-… measured_at=…` lines to this ADR (at least one per level). Until it is green, runbook §8.1 stays
  titled "(dev-host)", Baseline:345 stays open, and no `ops.mechanism_observations` row is written from the dev host.
- Debt A upgrade (wire `ProviderHealthRegistry` on the query-embedding path and / or a recall sub-limit) and the
  Debt B / C upgrades open only on their reopening signals (see "Debts A / B / C").
