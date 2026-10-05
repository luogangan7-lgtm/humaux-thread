# ADR-0061 — Metrics exposition on every process, SQL-derived health gauges, loaded rules, truthful readiness

- Status: Accepted — implemented (card 34, 2026-10-04, on HEAD `6b6981b`) in ten serial slices: S1 telemetry encoder,
  loopback ops listener, `degrade_total` last-fired time, health publisher and witnesses; S2 the retrieval emit; S3
  migrations 0210-0213 and the aggregate reads; S4 `humaux-maintenance health serve`; S5 the gateway ops listener,
  truthful `/readyz`, `/status`, guard render and the worker's readiness route; S6 worker ops listeners; S7
  `metrics-registry --check` D7/D8; S8 loaded rules, promtool tests, the mutation record and bundle configs; S9 the
  §4.4 probes; S10 the rehearsal steps, docs, rulings, tests and limits recorded below. Addendum card 34b
  (2026-10-04, on HEAD `5c5f8fa`): the private worker's three families and the §42 no-output stage — see "Addendum
  card 34b".
- Spec: Baseline §41.2, §42, §53.5, §67.2, §4.4, §80.2; design `card_34_design.md` with its main-line rulings
  (2026-10-04: E1–E15) and the research addendum (W1–W8).

## Context

No process exported a §41.2 family: `telemetry::metrics` was a placeholder, so the §53.5 invariant rules and the
§42 alerts could never fire.

## Decisions (slice S1)

### D-A  One hand-written text-format encoder, no metrics crate
`crates/telemetry/src/metrics.rs` holds the only family table (`families::*`, one const per exported §41.2 family),
`write_family` / `write_single_label` / `write_histogram`, the label-value and HELP escaping of the 0.0.4 text
format, and `CONTENT_TYPE = text/plain; version=0.0.4; charset=utf-8`. Label values are `&'static str` from closed
enums, and every value of a closed set is rendered from the first scrape (seeding), so an `absent*()` branch means
"process down", never "never incremented". Histograms keep count + sum and render one `le="+Inf"` bucket.
`abstain()` also stores the last-fired unix time per `DegradeCode` (`degrade_last_fired_unix`) for `/status` and the
§4.4 `degrade.counters` probe; it is not a metric family.

### D-B  One std-only loopback listener for every process
`serve_loopback(key, addr, Routes)` refuses a non-loopback address naming the §78 key, binds a `TcpListener` on one
std thread, serves `GET /metrics` (200, `CONTENT_TYPE`) and `GET /status` (200, `application/json`), answers 503 with
the renderer's error text, 404 otherwise. Dropping the `OpsListener` handle wakes the accept with one self-dial,
joins the thread and closes the port. One connection at a time, 8 KiB request head, and one 2 s total deadline per
connection covering the head read and the response write (review-fix 3, F8).

### D-D (emit side)  Health gauges have one emit point
`telemetry::health::publish(&HealthSnapshot)` holds the one `.set(` per health gauge (`jobs_*`,
`oldest_pending_age_seconds`, `projection_lag_events`, `processing_gap_count{stream}`,
`data_disclosures_reserved_unfinalized{age_bucket}`) and the one `.inc(` of
`data_disclosures_finalized_total{outcome}` (a delta since the sampler's watermark). `stream` =
`TicketFamily::domain()`; `outcome` = the 0047 CHECK literals, pinned by a contract test. A scrape renders the last
publish and never runs SQL.

## Decisions (slice S3)

### D-D (SQL side)  One NOLOGIN reader owns two read-only aggregate definers
Migration 0210 creates `role_health_reader` (NOLOGIN NOINHERIT NOBYPASSRLS, no password, no members) and makes it the
owner of `ops.health_snapshot(timestamptz)` (EXECUTE `role_maintenance`) and `ops.admin_probe_snapshot()` (EXECUTE
`role_admin`): `LANGUAGE sql STABLE SECURITY DEFINER`, `search_path = pg_catalog, ops, projection`, PUBLIC revoked,
one row of cross-tenant aggregates, no tenant id. The reader holds SELECT on exactly `ops.jobs`, `ops.data_disclosures`,
`ops.outbox`, `projection.stream_checkpoints` and the view `projection.processing_gaps`; five policies
`<table>_health_reader_read FOR SELECT TO PUBLIC USING (current_user = 'role_health_reader')` (stream_log included,
because the non-invoker view is checked against its owner while `current_user` stays the reader) are false for every
other role. Rejected: an owner-wide `USING (true)` read, which would be ORed into every existing owner definer reading
those tables (0115, 0132) and strip their FORCE-RLS tenant filter. The gap count reads `projection.processing_gaps`
only; 0212's partial-index predicate mirrors the view as a planner hint. 0211-0213 are one CONCURRENTLY index each.

Deviation from the design's "precheck: the role is absent": roles are cluster-global, and `e2e-onboard` / the serial
lane migrate fresh databases on the same cluster, so the 0210 precheck accepts a pre-existing reader only in its
frozen shape (no LOGIN, no superuser, no BYPASSRLS, no password, no membership) and keys "not applied" on the
database-local functions and policies (the 0201 precedent).

`adapters::health::{read_health_snapshot, read_admin_probe_snapshot}` are the only SQL; an unknown
`(domain, projection_kind)` pair or outcome literal fails the read naming it (§78.2). `RuntimeDbPool::ping()` is the
gateway's `pg` readiness round trip (D-F). `rls-check` gains `ADR-0061 health reader boundary` (role flags, empty
membership both ways, owns exactly the two functions and no relation, exact grant set, verbatim policies, no
owner-wide read beyond 0127's `outbox_phase9_owner_dispatch_read`); the owner-arm "one permissive policy" guard skips
only a verbatim reader policy. §6.2.0 / §6.2.2 carry the reader's row (E14) and the `role_admin` read-only-definer
note (E11).

## Decisions (slice S4)

### D-D (process side)  `humaux-maintenance health serve` is the one resident sampler
`health serve` reads two required keys with no code default, `HUMAUX_MAINTENANCE_HEALTH_SERVE_METRICS_ADDR` (its own
ops listener, one key per resident mode, D-B) and `HUMAUX_MAINTENANCE_HEALTH_SAMPLE_SECONDS` (> 0), and logs in as
`role_maintenance`. The first sample is taken before the port binds; a boot that cannot sample exits 1 naming the
error. Then one `ops.health_snapshot(watermark)` per interval, each bounded by the interval twice over: server-side
through the DSN startup option `options[statement_timeout]=<interval ms>` (sqlx's `-c` form), and client-side by a
`tokio::time::timeout` that also covers a pool acquire against an unreachable server. A good sample publishes and
advances the finalized watermark to its `as_of`; a failed one leaves the watermark, so the next good sample counts
what it missed. `/metrics` renders the last publish only while the latest sample succeeded and is at most
2 × interval old; otherwise it answers 503 with the error (`permission denied for function health_snapshot` when
EXECUTE is revoked) or `stale` and the age, never the last values and never a partial exposition. `/status` carries
`process`, `mode`, `crate_version`, `git_sha` (null unless burned in), `started_at`, `uptime_seconds`, the 11
`degrade` codes and the sample state. SIGTERM / SIGINT (handlers installed before any work) are observed between
samples: the port closes and the process prints its receipt and exits 0. `--metrics-families` prints the nine-family
zero-state exposition before any configuration is read (D-C / D-H). supervision.md lists it as a supervised unit.

## Decisions (slice S5)

### D-B / D-C (gateway)  `/metrics` and `/status` on a loopback ops listener; nine families; status-word `/readyz`
Two required `registry()` keys with no default: `HUMAUX_GATEWAY_METRICS_ADDR` (loopback, fixed port; refused at parse
time naming the key) and `HUMAUX_GATEWAY_READINESS_REFRESH_SECONDS` (> 0). Both feed the config fingerprint. The ops
listener (`telemetry::metrics::serve_loopback`) serves `/metrics` and `/status`; BIND_ADDR serves `/livez` and
`/readyz` only (E5). `/metrics` renders nine families through one function shared with `--metrics-families`:
`degrade_total`, `humaux_retrieval_requests_total`, `retrieval_completeness_total` and the six guard families.
`GuardMetrics` now keys its counters by `(family, [&'static str])` instead of pre-rendered strings, and `render`
seeds each family over its closed label sets (`ToolName` × (`ErrorCode::ALL` ∪ `ok`) × `unclassified`; the §41.2
frozen auth `result` set × `service_credential`; `ErrorCode::ALL`; `ok|denied`; `unclassified`;
`ip|credential|user|tenant|operation`). A recorded value outside a seed set still renders. `ToolName`'s list is
private to protocol, so the gateway keeps its own `TOOLS` with an exhaustive-match test. The ops listener is dropped
after `DRAIN_ANNOUNCE_WINDOW`, so the drain is scrapable. `/status` carries `process`, `mode`, `crate_version`,
`git_sha`, `started_at`, `uptime_seconds`, the 11 `degrade` codes with `last_fired_at`, `accepting`, `readiness` and
`effective_config`: one `{name, source: env|default}` row per `registry()` entry, with `value` only when the entry
is not secret (`secret: true` and no `value` key otherwise). The design's `ConfigEntry::public_value()` accessor is
realised as the gateway-local `effective_config_rows`: a `ConfigEntry` holds no value, and that one function reads the
entry's own `secret` flag for rows from both constructors.

### D-F  Truthful `/readyz`
A tokio task refreshes `ReadinessSnapshot` every N s (the first snapshot is taken before the listener accepts); each
check is bounded by N. `pg` = `RuntimeDbPool::ping()`. `retrieval_rpc` = `GET /internal/v1/retrieval/readyz` over the
recall client's own socket, permit and the worker's peer-uid layer; the worker answers from one fresh
`role_retrieval_worker` connection (DSN kept in `RpcState`, never serialized) and 503 `missing object: PostgreSQL as
role_retrieval_worker (...)` otherwise; it never calls the embedding provider. `qdrant` = `GET /` on the gateway's
own read-only Qdrant cell resource: Qdrant's `/readyz` answers plain text, which the cell transport refuses to parse,
so the root (the retrieval worker's `--readyz` request) is used. With semantic recall disabled both are
`not_applicable` naming `HUMAUX_GATEWAY_RETRIEVAL_RPC_SOCKET_PATH`. The verdict is `draining` (signal received),
else `stale` (snapshot older than 2N), else `not_ready` (any fail), else `ready`; `/readyz` answers 200
`{"status":"ready"}` or 503 with the word only. All three dependencies are hard (E15); the soak grades the gateway on
`/livez` (rehearse.sh, soak.rs doc). rehearse.sh carries the seven-entry `OPS_PORTS` table; `start_gw`, e2e-onboard
and the bin tests pass both keys, and the recall helpers read `GW_URL`.


## Decisions (slice S6)

### D-B / D-C (workers)  One ops listener per resident mode, opened first; four provider families on the retrieval worker
Each resident mode reads its own required loopback key, with no code default, before any other configuration:
`HUMAUX_RETRIEVAL_WORKER_SERVE_RPC_METRICS_ADDR` (`--serve-rpc`), `HUMAUX_RETRIEVAL_WORKER_SERVE_METRICS_ADDR`
(`--serve`), `HUMAUX_PRIVATE_WORKER_SERVE_RPC_METRICS_ADDR` (`--serve-rpc`),
`HUMAUX_PRIVATE_WORKER_DISTILL_SERVE_METRICS_ADDR` (`--distill-serve`) and
`HUMAUX_CONSOLIDATION_WORKER_SERVE_METRICS_ADDR` (`--serve`). A missing, unparsable or non-loopback value exits
non-zero naming the key; a mode never reads another mode's key, so two modes of one binary run at once from one
environment (rehearse.sh `$RW_ENV` carries both retrieval keys). The listener is bound at the top of the mode and
dropped when the mode returns, so it answers while the mode is still connecting to its database and closes after
the signal drain. The one-shot modes (`--readyz`, `--run-once`, `--distill-once`, `--probe-connection`) read no
key and open no listener. The three workers share one telemetry helper, `metrics::process_routes`, which serves
the binary's `render` on `/metrics` and the common `/status` document (`process`, `mode`, `crate_version`,
`git_sha`, `started_at`, `uptime_seconds`, the 11 `degrade` codes with `last_fired_at`), hand-encoded as JSON
(no JSON crate in telemetry, D-A). The retrieval worker renders the four `retrieval_provider_*` families from the
`snapshot_*` functions (requests 2 × 2 × 4 × 6 = 96 seeded series, latency as a `+Inf`-only histogram, tokens,
cost); the private and consolidation workers render nothing (E10). Every binary prints the same render under
`--metrics-families` before reading any configuration. Launchers: e2e-onboard takes all its ports from one
`free_ports::<5>()` call (listeners held until all are bound, so no two collide); rehearse.sh derives one
`OPS_*` assignment per mode from `OPS_PORTS` (now defined before the first launch) and every launch line, the
soak env strings and therefore every chaos-restart heredoc carry it; the bin tests take free ports at run time.

## Decisions (slice S8)

### D-E  Loaded rules are tested on synthetic series, and every test is shown to catch a mutation
`deploy/prometheus/alerts.rules.yml` loads the §42 rows that have a producer: CoreMetricAbsent, ProjectionLagExceedsSLO
(`for: 10m`), QueueDeadLetterIncrease, BackupFailure (silent until card 37; D8 allowlist), HealthGaugesAbsent
(`for: 2m`; its §42 row is added by ruling E8, because INV-3, lag and DEAD have no absent branch and a dead sampler
would silence all three) and the §42.1 Watchdog. `invariants.rules.yml` stays byte-identical (§42 freeze ③). Every
alert has a firing and a silent promtool case (`tests/*.test.yml`, rule paths relative to `tests/`), and
`test-rules.sh` adds the coverage grep (≥ 2 `alertname:` checks per alert) that turns a deleted test block red,
which promtool alone does not. `mutations.sh` is the executable §80.1 record: 14 single-occurrence token
substitutions; a row is red only if the token occurs exactly once, the file changed (`cmp`), the pinned
`promtool check rules` still accepts it, and `promtool test rules` exits 1 printing `alertname: <expected>, time:`
— measured (W6): a rule-load error also exits 1, so the exit code alone would fake a red. The unmutated tree must
pass in the same copy layout first, and the pin is checked before any row (exit 2 = not_applicable). Deviation from
the design table: the `dead` row mutates `> 0` to `> 2`, not `> 1`; measured, `delta(jobs_dead[15m])` extrapolates
a 0→1 step at 1m samples to 1.05, so `> 1` still fires on one new DEAD job and is not a behaviour change.
`selftest.sh` (T-E1..T-E7) derives each bad fixture from the real file by one proven substitution: pin unset,
a no-op row, a `)`-deleting row, a wrong `rule_files` path, five bad compose files, a sha256 digit off, a deleted
test block.

### D-G / D-I  Bundle configs, loopback only, pinned binaries
`prometheus.yml` scrapes `targets/*.json` (written by the deployer from the seven `*_METRICS_ADDR` values) and the
collector's own telemetry, loads both rule files, and carries `external_labels.git_sha` as a placeholder the
deployer renders from `humaux-admin q deploy.binary`. `alertmanager.yml` has explicit root timing
(10s / 1m / 4h; the default 5m `group_interval` would gate resolved notifications) and routes Watchdog alone every
5 min; webhook URLs are only `url_file` placeholders the deployer renders from
`HUMAUX_ALERTMANAGER_{LOG_SINK,WATCHDOG}_URL_FILE`. `otel-collector.yml` terminates OTLP (127.0.0.1:4317/4318) at the
`debug` exporter. `deploy/compose/observability.yml` (card 39, Linux, `network_mode: host`) pins every image as
`name:tag@sha256:` with the tags of the test pins; `check-compose.sh` enforces digests, tag == pin,
promtool version == Prometheus version, explicit `127.0.0.1` listen flags, an empty `--cluster.listen-address=`, and
no remote-write / OTLP receiver, admin or lifecycle flag. Every gate runs the binaries through `pinned-tool.sh`
(sha256 + `--version` check, never PATH; amtool is checked against the Alertmanager version, same tarball).

## Decisions (slice S9)

### D-J  The §4.4 catalog: 8 Readings over aggregate reads, 3 typed refusals
`humaux-admin q` answers eight probes with the unified envelope and refuses three. `stream.watermark`,
`outbox.backlog` and `jobs.stuck` are one call of the 0210 definer `ops.admin_probe_snapshot()` as `role_admin`
(`HUMAUX_ADMIN_PG_DSN`): cross-tenant aggregates, no row and no tenant id leave the function, and `role_admin`
stays SELECT-less on every RLS table (the ADR-0037 unlock done as an aggregate definer, not as table grants plus a
tenant argument). The denominators are full-table counts (E2c): "undelivered" or "in lease" alone reads 0/0 on a
healthy system, and an empty denominator is a `MissingObject` naming the table, never a Reading (§4.4: 0/0 means
"not scanned"); one constructor, `probe::reading`, enforces it. `degrade.counters` and `flags.effective` read the
loopback `/status` of every `HUMAUX_ADMIN_OPS_ADDRS` entry (`name=127.0.0.1:port`, comma list): the sum spans every
listed process, and any listed process that cannot be read refuses the whole probe naming it, never a partial sum;
a document missing one of the 11 `DegradeCode`s fails closed naming it; `flags.effective` needs exactly one
`humaux-gateway` document and counts its `effective_config` rows whose source is `env` (E2d). `tls.expiry` parses
each PEM file in `HUMAUX_ADMIN_TLS_CERT_PATHS` with `x509-cert` 0.2 (one crate; `der`/`spki`/`pem-rfc7468` were
already in the tree), takes the earliest notAfter of the file's chain and counts the files under the frozen
§42/§68 WARN window (1814400 s); an unreadable file is named. `public.corroborated`, `public.consensus_ready` and
`parse.poison` exit 1 naming `public.claims.corroboration`, `public.claims.contributor_set` and the `limit_hit`
column (§4.4 line 883 freeze, E2). Every new probe is `@1`; a unit test pins each scan predicate's scope hash and
version, so editing a predicate without a bump is red. The `--arg k=v` slot takes no key in this catalog version:
any argument exits 2. Proven in `bins/admin/tests/probes.rs` through the real binary (a throwaway database and a real
`role_admin` login; real loopback listeners; fixture certificates), each test shown red under its named fault.

## Decisions (slice S10)

### D-K  The rehearsal runs the bundle and proves the alert path end to end
`docs/ops/rehearse.sh` (twin `c34_rehearse.sh`, gate `rehearse_c34`, always the last chain line):
- **`start_mh`** (step `processes`): `humaux-maintenance health serve` as `role_maintenance`, its own ops key
  from the seven-entry `OPS_PORTS` table, a 5 s sample interval; its readiness is its own `/metrics` (200 only while
  the last sample succeeded and is fresh).
- **`observability`** (after `readyz`, before traffic): the pinned host binaries through `pinned-tool.sh` (W5: Docker
  Desktop cannot reach host loopback listeners, so the dev Mac never runs the compose fragment). The deployer
  contract is applied exactly: `targets/humaux.json` from `OPS_PORTS` (seven entries, labels `{job, mode}`), the rule
  paths, the Alertmanager address, `__HUMAUX_GIT_SHA__` → `detail.git_sha` of `humaux-admin q deploy.binary`, and the
  two webhook `url_file`s pointing at a local POST sink (`alert_sink.py`, one JSON line per request in
  `alert_receipts.jsonl`). Prometheus `127.0.0.1:19190`, Alertmanager `127.0.0.1:19193` with an empty
  `--cluster.listen-address=`, the collector with the repo config. Readiness waits on `/-/ready`, the collector's
  telemetry and **a Watchdog receipt on `/watchdog` whose `labels.git_sha` equals the deployed sha** (§42.1, §67.4);
  all of them count in `all_processes_ready_before_traffic`.
- **`metrics_scrape`** (after the traffic, at the end of `projection_serve_multi_tenant`): the resident distiller and
  consolidation worker exist only inside that step and the soak, so this is the one point where all seven modes run.
  Each ops `/metrics` is scraped into `$EV/metrics/` and graded by `cargo xtask metrics-registry --exposition …`
  (family set == the binary's `--metrics-families`, ≥ 1 sample each, R / R6 / kind); each `/status` must be JSON naming
  its process; `up{job=~"humaux-.*"}` is polled every 1 s for at most 2 × scrape_interval until the `(job, mode)` pairs
  at 1 **equal** the seven `OPS_PORTS` pairs (`up n=7`); an empty or short result names the missing pairs.
- **`admin_probes`**: all 11 `humaux-admin q` with a real `role_admin` login, the seven ops addresses and a self-signed
  30-day certificate: 8 five-key envelopes with `scanned_n > 0`, 3 non-zero exits naming `public.claims.corroboration`,
  `public.claims.contributor_set` and `limit_hit`; `jobs.stuck` is a count and `degrade.counters` equals Σ
  `degrade_total` over the seven scrapes.
- **`alert_drill`** (last before `stop`): a scratch gateway G′ (`127.0.0.1:18081`, ops `19111`, both step-local and
  never in `OPS_PORTS`) and a drill Prometheus P′ (`19191`, 5 s scrape/eval, the two rule files unchanged, the same
  Alertmanager, external label `prometheus: drill` so its alerts never merge with the main Prometheus's) that drops
  `humaux_retrieval_requests_total` with a `metric_relabel_configs` rule — INV-1′'s "denominator absent" made at
  ingestion, with no grant, row or shared object touched (E13). Sequence: one sample of G′'s
  `degrade_total{code="LaneSubstituted"}` at 0 → the `lane_substitution` step's queries to G′ until one carries
  `LANE_SUBSTITUTED` → INV-1 firing in P′ → a firing receipt in the sink → P′ reloaded without the drop (SIGHUP, no
  lifecycle API, W8) and one recall → INV-1 inactive → a resolved receipt. G′ and P′ stop through their pidfiles.
- Every observability process is started and stopped only through its own pidfile (`own_signal`, binary name
  checked); an EXIT trap covers every exit path, a previous run's pidfiles are dropped rather than acted on, and the
  twin's safety net signals only the run's own pidfiles. `/readyz`'s flip is a test (T-G2), not a rehearsal step.

### D-L  Docs
supervision.md: the ops listener table (seven keys), the dependency-truthful `/readyz`, the §2 rows for 503
`not_ready` ("do not restart, do not route on a single node; `/status` names it") and `stale`, the 8 + 3 probe table,
the Prometheus / Alertmanager / collector units (§8). runbook §7: verifying metrics, targets, alerts and the
Watchdog sha; the manual §69 "stop Alertmanager 5 min" dead-man step. soak.md: the gateway is graded on `/livez`
(E15), probes read `/metrics`. Baseline: the §41.2 rows of the SQL gauges name `ops.health_snapshot()` and their
consumers (E3); the §42 `health gauges absent` row (E8); the §53.5 INV-1 cross-process sentence (E9); §6.2.0 /
§6.2.2 (E11, E14). Delivery report §6.20.

## Decisions (slice S11, main-line rulings 2)

### D-J (B1)  `cell.resources` probes all three intra-Cell resources
The probe now reads every `IntraCellResource`: `QDRANT_REST` as before (DNS + a permit-gated HTTP call), and
`RETRIEVAL_EMBEDDING_RPC` / `PRIVATE_INFERENCE_RPC` as connect-level reachability of their Unix sockets
(`tokio::net::UnixStream::connect` bounded by the 2 s `telemetry::metrics::IO_TIMEOUT`) at the paths in two new
required §78 admin keys, `HUMAUX_ADMIN_RETRIEVAL_RPC_SOCKET_PATH` and `HUMAUX_ADMIN_PRIVATE_INFERENCE_RPC_SOCKET_PATH`
(no default; a missing key exits 2 naming it). `value` = resources found healthy, `scanned_n` = 3, `detail.unhealthy`
names the rest; the version moved to `cell.resources@2` because `scanned_n` went from 1 to 3. The rehearsal passes the
same socket paths it gives the gateway and the private worker.

### D-D (B2)  An unregistered stream family is fail-closed; fixture residue is removed at the root
`ops.health_snapshot` / `ops.admin_probe_snapshot` keep refusing a `(domain, projection_kind)` that is no
`TicketFamily` (§78.2). The shared dev DB carried 17 fixture checkpoints of `(code, retrieval_card)` /
`(knowledge, ingest)` (and 603 stream_log rows) because three fixtures cleaned up only on their happy path or inside
an all-or-nothing teardown: `visible_index_count` and `recall_envelope_g23` now clean up from a `Drop` guard,
`forget_repo` splits its data batch from the tenant delete, and `support/operation_receipt_fixture` moves its stream
rows into the batch that already deletes its jobs (card-31 pattern: one data batch whose failure is printed, the
tenant row best-effort). The residue was backed up and deleted, scoped to the two pairs; tenant rows stay. The leak
gate `no_leaked_distill_jobs` gained a second count: checkpoints of an unregistered pair on any tenant must be 0.

## Review-fix pass 3 (2026-10-04): the 12 P2 findings of the implement-pass review

Each behavioural fix carries a test shown red under its fault and green without it; the 14 blocks are in TW
`c34_fix3_red.log` and checked by the gate `c34_fix3_red_recorded`.

| # | finding | root-cause fix | red under |
|---|---|---|---|
| F1 | `check-compose.sh` scanned the compose file only | it also reads the two mounted configs: every collector `endpoint:` / `host:` is 127.0.0.1 and there are at least the three D-G names (grpc, http, telemetry), no `0.0.0.0` anywhere; `alertmanager.yml` carries no `listen` / `cluster` / `peer` line. selftest T-E5 gains four fixtures | otlp grpc on `0.0.0.0:4317`; a `cluster_listen_address` line |
| F2 | a non-superuser migrator cannot run 0210's `ALTER FUNCTION … OWNER TO role_health_reader` | recorded, no SQL change: see E14 below | — (prerequisite, not behaviour) |
| F3 | `scope_hash` of the DB probes hashed a Rust description only | `adapters::health::read_admin_probe_snapshot` reads `pg_get_functiondef('ops.admin_probe_snapshot()'::regprocedure)` with the sample, and `probe::db_scope` hashes description + that definition, so a forward migration that changes a predicate changes the hash. Chosen over `include_str!` of 0210: 0210 is applied and immutable, a predicate change arrives in a NEW migration, which a compile-time 0210 text would never see | `db_scope` returns the description only |
| F4 | admin `/status` fetch had per-read timeouts only and no size cap | one total deadline (`telemetry::metrics::IO_TIMEOUT`, the listener's own budget, no new default) over connect + write + read, and a cap equal to the admin's existing intra-Cell answer cap `humaux_infra_cell::DEFAULT_MAX_RESPONSE_BYTES` | per-read `IO_TIMEOUT` only (a peer trickling one byte per second held the probe 10 s) |
| F5 | two `--include-ignored` gates checked the exit code only | `gateway_tests` and `retrieval_worker_tests` capture output and grep every named test `... ok` (gateway: the 19 ignored tests and the 7 ops-listener tests; retrieval-worker: all 9) | — (gate shape) |
| F6 | one firing test per multi-branch absent alert: deleting any other branch stayed green | `alerts.test.yml` has a firing case per `absent(...)` branch of `CoreMetricAbsent` and `HealthGaugesAbsent` with only that family missing; `mutations.sh` has one row per branch (14 → 18 rows) | each of the four branches not covered before, deleted |
| F7 | `/readyz` staleness used `SystemTime` | `ReadinessSnapshot.taken: Instant` is the only staleness input; `taken_at: SystemTime` is reported only | age measured from `taken_at` (an old snapshot with a future wall time read `Ready`) |
| F8 | the ops listener applied a per-read timeout only | one total deadline per connection (`IO_TIMEOUT`, 2 s) over the head read and the response write; each read and write gets the remaining time | per-read timeout only (a client sending one byte per second held the listener 9 s) |
| F9 | worker / maintenance `*_METRICS_ADDR` accepted port 0 | `telemetry::metrics::parse_ops_addr` (loopback, port ≠ 0) is the one parser; gateway, both workers' `ops_listener`, consolidation `ops_listener` and maintenance `health serve` each call it once | `parse_ops_addr` accepts port 0 (telemetry unit test and the consolidation `--serve` leg) |
| F10 | D7 counted a HELP/TYPE-only family as exported | D7's exported set is the families with at least one sample line | every `# TYPE` family counted |
| F11 | the leak sentinel scanned telemetry + bins only, for `Box::leak` only | it scans `crates/telemetry/src`, `crates/retrieval/src`, `crates/retrieval-provider/src` and every `bins/*/src`, for `Box::leak`, `String::leak`, `Vec::leak` and the method form `.leak(` | the narrower scan domain |
| F12 | "index-only" claim for the jobs gauge | measured, claim corrected below; no migration | — (measurement) |

**E14 / F2 — the migrator prerequisite of 0210.** `ALTER FUNCTION … OWNER TO role_health_reader` succeeds for a superuser
unconditionally. For any other executor PostgreSQL requires both (a) that the executor can `SET ROLE
role_health_reader` (membership with the SET option, PostgreSQL 16+) and (b) `CREATE` on schema `ops` for
`role_health_reader`. The dev executor (`HUMAUX_MIGRATOR_PG_DSN`, user `postgres`) is the cluster superuser, so 0210
needs neither and its SQL is unchanged (it is applied; a change would be a new migration). Both prerequisites collide
with this ADR's invariants: (a) gives the reader a member, which `rls-check` forbids, and (b) widens the reader to
creating objects in `ops`. Card 39's production migrator therefore runs 0210 as a superuser; the only non-superuser
path is to grant (a) and (b) immediately before 0210 and revoke both right after it, then run `rls-check`, which
proves "no members" and the exact grant set again. The 0210 postcheck proves ownership either way.
(`docs/ops/runbook.md` §2 and the card-39 note in `docs/ops/delivery_plan_v2.md` carry the same text.)

**F12 — the jobs gauge statement is not index-only; measured on `humaux_thread_dev` (2026-10-04).** The 0012 tenant
policy `jobs_tenant_isolation` is `current_user = 'role_migration_owner' OR tenant_id = <GUC>`, ORed with the reader
policy `current_user = 'role_health_reader'`; `current_user` is not folded at plan time, so the definer's statement
carries a per-row filter that reads `tenant_id`, a column the 0211 index `(status, created_at)` does not hold — an
index-only scan is impossible whatever the data. Measured as `role_health_reader` (`EXPLAIN (ANALYZE, BUFFERS)` of
0210's `jobs` CTE; ops.jobs 36 920 rows, 16 MB; DEAD 29 548, PENDING 304, PROCESSING 293, WAITING_KEY 633 — 83 % of
the table matches the four states):
`Seq Scan on jobs j (cost=0.00..3406.95 rows=307 width=13) (actual time=0.026..15.320 rows=30778.00 loops=1)`,
`Filter: ((status = ANY ('{PENDING,PROCESSING,WAITING_KEY,DEAD}'::text[])) AND ((CURRENT_USER = 'role_migration_owner'::name) OR (tenant_id = …) OR (CURRENT_USER = 'role_health_reader'::name)))`,
`Buffers: shared hit=2022`, execution 17.3 ms. Without RLS (superuser) the planner also seq-scans (2022 buffers, 9.2
ms): at an 83 % match no index wins. With `enable_seqscan = off` the reader gets `Bitmap Index Scan on
jobs_health_status_idx` + `Bitmap Heap Scan` with the RLS filter (cost 4034.78, 19.4 ms, 2048 buffers). Decision
(option b, by the measurement): no 0214. Adding `tenant_id` to the index (`INCLUDE`) would make index-only possible
in principle, but `jobs_dead` counts every DEAD row and DEAD rows stay until an operator requeues them or a retention
path removes them, so the read stays O(DEAD) either way; the index serves the gauge as a bitmap scan when the four states are a small share of the table, and the
planner seq-scans when they are not. The 0210 comment "every statement is index-backed" and 0211's "index-only" are
applied text and stay byte-identical; this paragraph is their correction.
ponytail: O(PENDING+PROCESSING+WAITING_KEY+DEAD) per sample, once per `HUMAUX_MAINTENANCE_HEALTH_SAMPLE_SECONDS`, never
per scrape; upgrade = a DEAD retention/archival path (card 36 retention) or a maintained per-status counter row.

## Main-line rulings applied (2026-10-04, design `card_34_design.md`)

| ruling | decision | where |
|---|---|---|
| E1 | allowed files extended (retrieval emit, adapters, migrations, rls/arch checks, e2e_onboard, launchers, rpc readiness route, soak.rs doc, delivery_plan row) | S2–S7 |
| E2 | 8 Readings + 3 typed refusals = "all 11 implemented"; full-table denominators; "flag" = a §78 registry entry | D-J |
| E3 | the SQL gauges keep their §41.2 names, rows updated with `ops.health_snapshot()` and consumers; `stream` = `TicketFamily::domain()` | D-D, Baseline §41.2 |
| E4 | this ADR is 0061 (the card's 0059 was taken by card 33) | — |
| E5 | `/metrics`, `/status` only on the loopback ops listener; `BIND_ADDR` keeps `/livez` and a status-word `/readyz` | D-B |
| E6 | the `metrics_registry … --check` gate of cards 31–33b ran the default mode (the flag was ignored); the strict parser exists from card 34 | D-H, delivery report |
| E7 | `health serve` is a supervised resident unit | D-D, supervision.md |
| E8 | §42 `health gauges absent` row, injection on a throwaway database only | D-E |
| E9 | INV-1's `sum()` spans every process exporting `degrade_total`; a non-gateway `abstain()` needs its own row first | Baseline §53.5 |
| E10 | private and consolidation workers export zero families; `private_distill_runs_total` / `_outputs_total` are on `NOT_YET_PRODUCED` with producer card 34b — **closed by card 34b** (addendum below); the consolidation worker still exports none | D-C, D-H |
| E11 | `role_admin` may EXECUTE the read-only aggregate definer | Baseline §6.2.2 |
| E12 | pin suffix `_SHA256` | D-I |
| E13 | the drill drops the denominator at ingestion; no shared-DB grant | D-K |
| E14 | `role_health_reader` row in §6.2.0 / §6.2.2; `rls-check` asserts ownership == 2; the dev migrator is the superuser, a non-superuser migrator's prerequisites are in "Review-fix pass 3" (F2) | D-D |
| E15 | PG, the retrieval RPC round trip and Qdrant are hard for `/readyz`; the soak grades `/livez` | D-F |

## Addendum card 34b (2026-10-04): the private worker's families and the §42 no-output stage

### D-M  Three unlabeled counters, each emitted where §41.2 reads it, rendered by the private worker

| family (§41.2) | emit (the one `.inc(`, §41.2 R4) | the one production caller | why there |
|---|---|---|---|
| `private_distill_runs_total` | `adapters::distill_repo::count_committed_distill` | `commit_distill_write`, after `txn.commit()`, with the runs `finish_processing_run` flipped | §42 no-output stage is about runs that *finish* without output; a run counts whatever its `output_count`, a failed attempt never finishes (ADR-0016 D4), an already finished run adds 0 |
| `private_distill_outputs_total` | the same `count_committed_distill` | the same post-commit call, with the `insert_memory` calls of that write | one per memory record the distill write made durable |
| `private_reasoning_usage_total` | `adapters::model_call_ledger::count_private_reasoning_usage` | `finalize_private_call`, after the commit and only when the row was finalized | §35 quota counts input + output tokens as the provider reported them; unreported or negative usage adds 0 and never fails the call; a no-op re-finalize adds nothing |

**Counted after commit (review fix, 2026-10-04).** The first cut counted runs in `finish_processing_run` and outputs
in `insert_memory`, i.e. inside the settle transaction. `write_txn` still has fallible steps after those statements
(the inferred-affect insert, the candidate TTL read, the outbox settle that returns `false` when the outbox lease
moved, the commit itself) and rolls back on each, so a rolled-back attempt counted, and its retry counted again —
outputs could rise while nothing was persisted and `DistillNoOutput` stayed silent. Both distill commits (the
written attempt and the ADR-0048 abandoned empty attempt) now go through `distill_repo::commit_distill_write`,
which commits and only then counts; `finish_processing_run` returns the runs it finished instead of counting.
memory.confirm / memory.correct reach `insert_memory` too but never this commit, so they are no longer counted as
distill outputs anywhere.

- The counters are `humaux_adapters::Counter` statics, not telemetry `Counters`: `humaux-adapters` has no
  production dependency on `humaux-telemetry`, and card 34b adds no dependency. The private worker renders the three
  values through `telemetry::metrics::write_family` (the one encoder, D-A) from adapters accessors, the shape
  `retrieval-provider::metrics` already has. No label (the §41.2 rows carry none).
- The recorder functions are public only so the G80-6 witnesses can drive the emit without a database. Their call
  sites are pinned twice: statically, `metrics-registry` D5 counts each `EMIT_HELPERS` helper's production callers
  and requires the declared §41.2 count (1) — a deleted call is `=1/0`, a second caller `=1/2`; and on the real
  path against PostgreSQL, `distill_hop_e2e` d5 requires exactly (runs, outputs) = (1, 1) for one committed write
  and d5c requires (0, 0) for a write whose outbox settle is refused after its insert, plus exactly the provider's
  input + output tokens once for its finalized call. The rehearsal's `> 0` remains the deployed-binary check.
- D-C: both resident modes (`--serve-rpc`, `--distill-serve`) and `--metrics-families` render the three, seeded at 0.
  The consolidation worker's reasoner also reaches `finalize_private_call` — in a process that does not export the
  family, so nothing is double-counted on a scrape.
- `NOT_YET_PRODUCED` keeps only `backup_last_success_timestamp_seconds` (card 37); D8's stale check would fail on
  either distill entry now that D7 counts them exported. **E10 closed** for the private worker.
- `public_reasoning_usage_total` stays dormant: no PLATFORM_PUBLIC reasoning call exists (no producing card in plan
  v2) and no loaded rule names it, so it is neither emitted nor allowlisted.

### D-N  `DistillNoOutput` loaded verbatim from §42, proven on synthetic series

- `alerts.rules.yml` gains `DistillNoOutput`: `increase(private_distill_runs_total[1h]) > 0 and
  increase(private_distill_outputs_total[1h]) == 0`, CRITICAL, no `for:` (the §42 row gives none).
- promtool cases: fires when runs rise and outputs stay flat; silent when outputs rise too; silent when neither moves.
  `mutations.sh` rows `distill_and` (drop the `and` branch) and `distill_flat` (`== 0` → `> 0`): 20/20 red.
- The §42 injection ("a parser stub that always returns an empty array ⇒ runs rise, outputs do not") is proven by
  the firing promtool case, not by the rehearsal: a stub parser in the deployed binary is a code change, not an
  operator action. The rehearsal's `metrics_scrape` instead asserts the three counters of the `--distill-serve`
  scrape are > 0 after this step's distiller distilled real Evidence — the live proof that the three call sites are
  wired.

## Pins (D-I, research addendum W1–W3; values live only in TW `live_env.sh`)

| tool | version | where |
|---|---|---|
| promtool / prometheus | 3.15.0 | `$HOME/.humaux-tools/prometheus-3.15.0/`; image `prom/prometheus:v3.15.0@sha256:6b41f7a4…bc3c91` |
| alertmanager / amtool | 0.34.1 | `$HOME/.humaux-tools/alertmanager-0.34.1/`; image `prom/alertmanager:v0.34.1@sha256:47a1dc7e…e41d9` |
| otelcol (core) | 0.162.0 | `$HOME/.humaux-tools/otelcol-0.162.0/`; image `otel/opentelemetry-collector:0.162.0@sha256:ef772ad0…49801` |

Every gate verifies the executable's sha256 and `--version` first (`pinned-tool.sh`; exit 2 = a pin variable unset,
named). The full executable and tarball digests are in `deploy/prometheus/README.md`. gates_card.sh requires every
pin variable before the chain starts.

## Rejected alternatives

- A `prometheus` / `prometheus-client` crate or an OTel SDK (one dependency for ~60 lines; a second family table).
- A `crates/contracts` metrics module (a second home for names). Rendering `GuardMetrics`' pre-rendered keys verbatim.
- `/metrics` on the gateway's `BIND_ADDR` (public through the proxy); a UDS per worker (not scrapable); OTLP push;
  a textfile collector for one-shots; port 0 plus discovery; a code default port.
- Collect-on-scrape SQL; serving the last good value after a failed sample; a per-tenant loop; a BYPASSRLS role;
  raw SELECT for `role_maintenance`; owner-wide `USING (true)` policies; re-deriving the gap state set; a GUC-gated
  owner policy; sampling in each worker or in the gateway.
- Loading all 24 §42 rows; editing `invariants.rules.yml`; counting any non-zero promtool exit as a red; proving the
  route with `/api/v1/alerts` only.
- Checking dependencies per `/readyz` request; `connect()`-only socket readiness; dependency names in the public body;
  PG-only hard readiness; a chaos-window exemption in the soak harness.
- The collector as scraper with remote-write; both scraping; a remote-write or OTLP receiver on Prometheus; a file
  exporter.
- A static per-process family list or scraping a running stack in CI for D7; extending the C scan to `bins/`.
- A drill that REVOKEs on the shared dev database (E13).
- Card 34b: a production `humaux-adapters` → `humaux-telemetry` dependency so the emits could call telemetry
  `Counters` (a new dependency edge; the crate-local counter rendered by the process is the retrieval-provider shape
  already); a database-backed witness driving `commit_distill_write` (the probe package links no async runtime —
  the PostgreSQL leg lives in `distill_hop_e2e` d5 / d5c and D5 pins the callers); counting in
  `finish_processing_run` / `insert_memory` inside the transaction (the review P0: a rollback still counted); the §42
  parser-stub injection inside the rehearsal (a code
  change to the deployed binary, not an operator action).

## Tests (each shown red under its named fault while it was written; slice notes carry the failing lines)

| test | fault that reds it |
|---|---|
| telemetry `label_value_escaping_round_trips` | drop the `\n` escape arm |
| telemetry `render_seeds_all_eleven_codes` | seed from `ALL[..10]` |
| telemetry `histogram_renders_inf_bucket_equal_to_count` | omit the `+Inf` bucket line |
| telemetry `health::render_seeds_every_family_at_its_cardinality_bound` | add a label value |
| telemetry `serves_metrics_status_and_404` | a wrong content type |
| telemetry `non_loopback_addr_is_refused_naming_the_key` (T-B2); retrieval-worker `each_resident_mode_needs_its_own_metrics_key` and consolidation-worker `serve_needs_its_own_loopback_metrics_key` (T-W1 non-loopback legs) | delete the `is_loopback` refusal in `serve_loopback` (gateway T-G6 cannot see it: the gateway parser refuses first) |
| telemetry `drop_closes_the_port` | remove the self-dial wake (join hangs, watchdog panics) |
| telemetry `renderer_error_is_503_with_text` | map a renderer `Err` to 200 |
| telemetry `abstain_sets_last_fired_for_its_own_code_only` | store into index 0 always |
| telemetry `health::publish_sets_gauges_and_accumulates_the_finalized_counter` | `set` instead of `inc` |
| retrieval `finish_counts_the_returned_envelope_once_under_its_own_intent` | map every intent to `text`; move the inc out of `finish()` |
| retrieval `render_metrics_parses_with_the_registry_label_keys_and_seeds_every_cell` | rename `completeness_class` → `class` |
| retrieval-provider `snapshots_return_every_recorded_tuple_seeded_over_the_closed_sets` | return an empty snapshot |
| adapters `health_snapshot` (9 tests, T-D3..T-D9, T-J1, T-F1) | drop a `_health_reader_read` policy; `>=` for `>`; swap the lag operands; read `stream_log` LOST only; add an owner-wide `USING (true)` |
| xtask `rls_check` health-reader faults | grant EXECUTE to `role_gateway`; grant the reader to a login role; `ALTER ROLE … LOGIN` |
| maintenance `health_serve` (T-M1..T-M5) | sample once only; serve the last snapshot on error; default the address; ignore SIGTERM |
| gateway `ops_listener` (T-G1..T-G3, T-G5..T-G8) | drop guard seeding; **remove the readiness refresh**; name the dependency in `/readyz`; `connect()`-only readiness; check `accepting` only; serialize a secret `value`; delete the `parse_metrics_addr` loopback refusal (T-G6, also `bootstrap::tests::ops_keys_are_required_and_the_ops_address_is_loopback_only`); drop the ops handle before the drain window |
| gateway `status::tests::an_old_all_pass_snapshot_is_stale` (T-G4) | drop the age check |
| retrieval-worker `rpc_readyz` (T-W5), `ops_listener` (T-W1..T-W4) | answer readiness without connecting; one shared metrics key for two modes; render only recorded tuples |
| private / consolidation worker `ops_listener` | default the metrics address |
| card 34b: testkit witnesses `private_distill_runs_total`, `private_distill_outputs_total`, `private_reasoning_usage_total` (G80-6 probe) | delete the family's `.inc(` line (TW `c34b_red.log`, gate `c34b_red_recorded`, 12 faults) |
| card 34b review fix: private-worker `distill_hop_e2e` d5c | count inside the write transaction before the outbox settle (the reviewed shape) ⇒ (1, 1); delete the `count_private_reasoning_usage` call ⇒ usage 0 |
| card 34b review fix: `distill_hop_e2e` d5 (real path) and `metrics-registry` D5 (`d5_counts_emit_helper_callers_exactly`) | delete the `count_committed_distill` call ⇒ (0, 0); call it twice ⇒ D5 `private_distill_*<-count_committed_distill()=1/2` |
| card 34b review fix: gate `c34b_rehearse_asserts_private_counters` | rehearse.sh `assert_gt … "${PW_V:-absent}" 0` floor → `-1` (a counter at 0 would pass) |
| card 34b: private-worker `ops_listener` `metrics_families_prints_the_three_private_families_without_env` and the resident-mode leg | the card-34 empty `render_metrics` |
| card 34b: `metrics-registry --check` D8 | the private worker renders nothing ⇒ `DistillNoOutput`'s families referenced but not exported |
| xtask `metrics_registry` T-H1..T-H10 | **rename one exported family**; wrong kind; extra label; `queries_total`; `by (stream)`; restore the NA path for an unexported rule family; stop exporting `humaux_mcp_requests_total`; empty rules dir; an unknown flag; a zero-sample family |
| xtask `architecture_check` `adr0061_box_leak_sentinel…` | a `Box::leak` label |
| review-fix 3 (14 faults, TW `c34_fix3_red.log`, gate `c34_fix3_red_recorded`) | see "Review-fix pass 3" below |
| promtool `test-rules.sh` + `mutations.sh` (20 rows since card 34b) + `selftest.sh` (T-E1..T-E7) | **delete one rule test** (coverage grep); each row's token mutation; pin unset / no-op / syntax-breaking mutation / wrong `rule_files` path / bad compose |
| admin `probes` (T-J2..T-J12) | drop `lease_expires_at < now()`; count FAILED as undelivered; a public probe returning a Reading; sum the reachable processes only; compare against `notBefore`; an empty table read as 0/0; edit a predicate without a bump |
| admin `probes::cell_resources_reads_all_three_resources_and_names_the_unreachable_socket` (B1) | ignore the UDS connect result (value 3 instead of 2) |
| TW `c31_leak_check.sh` count 2 (B2) | one `(code, retrieval_card)` checkpoint written on the dev DB; a fixture leg panicking with its cleanup guard removed |
| rehearsal `metrics_scrape` / `alert_drill` | `up` short of seven; the relabel drop removed (INV-1 never fires); the alert not routed; the Watchdog sha missing |

## Known limits and upgrade paths

- One loopback connection at a time per ops listener (2 s total deadline per connection); upgrade to a tokio
  listener if a scrape timeout is ever observed.
- Histograms render a `+Inf` bucket only (no quantiles for `retrieval_provider_latency_seconds`); upgrade = a §78
  bucket-boundary key and real buckets.
- Guard families live in a bin: the C scan and D2 witnesses do not see them; their export is enforced by D7 / D8(c).
- **E10 (closed for the private worker by card 34b):** the consolidation worker still exports zero families (none is
  registered in §41.2); provider slot / dispatch families are not registered either — a §41.2 row first.
- The distill counters count after `txn.commit()` returns: a commit acknowledged by PostgreSQL but lost on the wire
  returns an error and counts nothing although the rows persisted (an under-count of that one write, never an
  over-count).
- `EMIT_HELPERS` is a hand-kept table in `metrics_registry.rs`; a new helper-emitted family must add its row (a
  family whose `.inc(` sits in an unlisted helper is checked at the helper only).
- The other 19 §42 rows are not loaded (each needs a producer and a red record); `processing_gap_count` and
  `oldest_pending_age_seconds` are exported, their rules are cheap follow-ups. No §42 row alerts on `up == 0` for a
  worker or the collector.
- `BackupFailure` is silent until card 37 produces `backup_last_success_timestamp_seconds` (allowlisted, named).
- The collector carries no application traffic (`debug` exporter) until an SDK producer lands.
- Prometheus, Alertmanager and collector APIs are loopback-only but unauthenticated to local users; upgrade =
  `--web.config.file` (card 39) if the host is ever multi-user.
- `retrieval_rpc` readiness never calls the embedding provider; a provider outage shows as `LaneSubstituted` / INV-4.
- The finalized-disclosure watermark can only undercount (a late-committing finalize behind the watermark).
- Worker keys are raw env without a registry or fingerprint (§78 partly met for workers, as before).
- Per-process counters reset on restart; `degrade.counters` sums live processes only.
- `cell.resources` judges a Unix socket reachable when its listener accepts; the worker's peer-uid check runs after
  accept and is not probed (upgrade: a `/readyz` round trip per socket).
- The leak gate mirrors `TicketFamily::ALL` as a literal pair list in TW `c31_leak_check.sh`; a new family is added
  there in the same change (the gate goes red, never silently green, if it is not).
- `tls.expiry`'s dead-man (24 h without a report) is not wired; the probe is operator-invoked.
- The Watchdog `git_sha` is rendered by the deployer; only the rehearsal asserts it, and card 39's packaging must keep
  that assertion. The external dead-man is a manual §69 step (runbook §7), not a gate.
- **Mac compose (W5):** the dev Mac does not run `deploy/compose/observability.yml` (Docker Desktop host networking
  cannot reach host loopback listeners reliably); `check-compose.sh` validates it statically and the rehearsal runs
  the same configs from the pinned host binaries. Linux / card 39 runs the fragment.
- **gitleaks path (W7):** the observability pins moved to `$HOME/.humaux-tools/` (macOS may purge `/private/tmp`);
  gitleaks still lives at `/private/tmp/gitleaks-8.30.1/` until its next bump — a purge turns its gates
  not_applicable / red, never silently green.
- The drill Prometheus loads `alerts.rules.yml` too, so `CoreMetricAbsent` (and, past 2 min, `HealthGaugesAbsent`)
  also fire in P′ during the drill; they carry `prometheus: drill` in the receipts and are not graded.

## Addendum (card 35, 2026-10-04; ADR-0062 D-A / D-S, ruling E3)

D-B's ops-port map gains an **eighth** `(job, mode)` pair: `humaux-maintenance:serve`, key
`HUMAUX_MAINTENANCE_SERVE_METRICS_ADDR` (rehearsal port 19108). It is a second resident mode of the maintenance binary
with its own families, so D-C's zero-state render is `humaux-maintenance --serve --metrics-families`, and
`cargo xtask metrics-registry` reads it as the process key `maintenance-serve` (D7 and the rehearsal's EX leg).
Families (§41.2 rows added by ruling E3): `maintenance_task_runs_total{task,outcome}` and
`maintenance_task_rows_total{task}`, `task` = the closed D-C list, `outcome` ∈ {ok, failed}, seeded at 0, one emit
each in `adapters::maintenance_repo::count_task_call` (witnesses `crates/testkit/tests/metrics/maintenance_*.rs`).
§42 `MaintenanceTaskFailing` (`increase(maintenance_task_runs_total{outcome="failed"}[1h]) > 0`, WARNING) is loaded
with firing / silent promtool cases and two mutations (matcher dropped, `> 0` → `< 0`); the card-35 fix pass adds
`MaintenanceCountersAbsent` (`absent(maintenance_task_runs_total)` for 2m, WARNING), the live signal while the
daemon answers 503, with its own cases and mutation: 23/23 (ADR-0062 D-S).
`prometheus.yml`'s deployer contract, rehearse.sh's `OPS_PORTS` and the `up` assertion now say eight
(`prometheus_up_is_exactly_the_eight_ops_pairs`). `NOT_YET_PRODUCED` is unchanged (backup only). Readiness of the
daemon follows D-D's rule: a failed or stale cycle answers 503, never the last counters.
