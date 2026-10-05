# ADR-0063 — §48.1 growth tables: monthly range partitions, retention through `control.retention_policies`, a short-lived superuser executor

- Status: Accepted (design rulings 2026-10-04, E1–E4 and E6–E8 approved, E3 conditioned, E5 main line only, E9 →
  card 36b). Implemented in seven serial slices (S1–S7, plus S5b for ruling E3-b); complete as of S7.
  - S1 (2026-10-05, uncommitted): migration 0224 (`control.retention_policies` v2, `control.partition_registry`, the
    nine owner functions of the machinery), the PostgreSQL 18 fact spike P1–P10 as tests, the rls-check partition
    arm (D-L) and the db_objects leaf filter, the resident boot refusal of the migrator DSN (D-H, T-H3). No table is
    converted yet.
  - S2 (2026-10-05, uncommitted): conversions 0225 stage_runs, 0226 messages, 0227 maintenance_receipts (D-D), the
    T-C1..C6 and T-D5 tests in `bins/maintenance/tests/partitions.rs`, and a rehearsal on a throwaway restored from
    a fresh `pg_dump -Fc` of dev. Dev itself is not migrated (D-M is a main-line step).
  - S3 (2026-10-05, uncommitted): conversion 0228 events with `private.event_identity` (D-B, D-D), the T-B2 / T-C7
    (events part) / FK re-point / orphan-refusal tests, a policy-deparse fix in `partition_adopt_leaf` (0224) and
    the rls-check arm, and a rehearsal on an FK-consistent copy of dev. Dev itself is not migrated.
  - S4 (2026-10-05, uncommitted): conversion 0229 audit_events with `control.audit_event_identity` (D-B, D-D), the
    T-D4 / T-B3 / T-C7 (audit part) / §77-guard / orphan-refusal tests, and a rehearsal on an FK-consistent copy of
    dev. Dev itself is not migrated.
  - S5 (2026-10-05, uncommitted): conversion 0230 model_call_ledger (the design's 0229, shifted by one because card
    35 took 0223) with `ops.model_call_identity` (D-B, D-D), the writer statement change in
    `crates/adapters/src/model_call_ledger.rs`, the T-B1 / E3 writer-idempotency / T-C7 (ledger part) / T-C8 / T-D6 /
    orphan-refusal / rowtype-rebind tests, and a rehearsal on an FK-consistent copy of dev. Dev itself is not
    migrated. The dedupe contradiction S5 reported is resolved by main-line ruling E3-b (S5b, see "Ledger dedupe:
    ruling E3-b").
  - S6 (2026-10-05, uncommitted): `humaux-maintenance retention approve | create-partitions | execute` over
    `RetentionExecutor` (D-H, D-I, D-J), the daemon's PARTITIONS proposer with the `partition_horizon_months{table}`
    family (D-K), and the T-H1/H2, T-G2..G6, T-J1..J5, T-F1/F2, T-I1, T-K1..K4 tests plus the table_key contract test,
    all on throwaway databases. No retention ran on dev.
  - S7 (2026-10-05, uncommitted): the three §42 horizon rules with promtool firing / silent cases and four
    mutations (27/27), the D-N latency test and the dev-copy measurements below, `c36_devcopy.sh` /
    `c36_dev_convert.sh` / `c36_rehearse.sh`, runbook §1.1 / §5.3 / §5.4, supervision, the delivery report, Baseline
    §6.2.1 / §6.2.2 / §41.2 / §42 / §48.1 (E1 texts verbatim), and the ADR-0062 addendum for the receipts table.
    Dev itself is converted by the main line afterwards (D-M, ruling E5).
  - Registry by name (2026-10-05, uncommitted): migration 0231 (the registry identified by name + bounds, the stored
    OID retired), the rls-check arm and the horizon read without OIDs, `registry_catalog_mismatch <leaf>` replacing
    `stale`, and the logical dump/restore witness; see "Registry by name (logical-restore finding)".
- Spec: Baseline §6.2.1, §6.2.2, §37.2, §41.2, §42, §46, §48.1; research ruling R-36; ADR-0059 (executor principal),
  ADR-0062 (the resident daemon); design `card_36_design.md` with its main-line rulings.

## Context

§48.1 requires monthly range partitions and retention from v1 for the append-heavy tables. PostgreSQL 18 cannot
convert a heap in place (R-36(a)); the only physical delete is the migration owner's partition drop (§6.2.1).
`control.retention_policies` v1 (0003) is per tenant, has no reader and holds 0 rows on dev.

## Decisions

### D-A Per-table scope (every existing §48.1 table now; the two that do not exist are created partitioned)

| table_key | table | key | decision | why |
|---|---|---|---|---|
| `STAGE_RUNS` | `ops.stage_runs` | `started_at` | partitioned now (0225, pilot) | empty: a millisecond DDL now, a long ACCESS EXCLUSIVE later; R-36's deferral is allowed only without a purge promise |
| `MESSAGES` | `private.messages` | `created_at` | partitioned now (0226) | empty, no writer; same cost argument |
| `MAINTENANCE_RECEIPTS` | `ops.maintenance_receipts` | `ran_at` | partitioned now (0227) | card 35 ruling E6 put it on the list |
| `EVENTS` | `private.events` | new `recorded_at` | partitioned now (0228); retention impossible by CHECK until card 37's rebuild baseline | §48.1 body table |
| `AUDIT_EVENTS` | `control.audit_events` | `occurred_at` | partitioned now (0229); retention impossible by CHECK until a separate approval line | largest table |
| `MODEL_CALL_LEDGER` | `ops.model_call_ledger` | `called_at` | partitioned now (0230), with three hold arms | R-36(b) names its hold |
| — | `ops.job_history` | — | **deferred: the table does not exist.** The card that creates it creates it partitioned from birth, adds its table_key to both D-C CHECKs and to `PartitionTable`, and creates its leaves with `control.partition_create_month` | §48.1 "如拆历史表" |
| — | public evolution history | — | **deferred: no table exists**; same trigger and rule | — |

Rejected: deferring the empty tables (saves nothing now, costs a large conversion later); deferring the ledger
(R-36(b) expects it partitioned, the identity table makes it mechanical); a range on the uuidv7 id (v4 ids exist in
dev data and in tests).

### Contract changes (E3), old → new

| object | before | after |
|---|---|---|
| PKs | `stage_runs(stage_run_id)`, `messages(message_id)`, `maintenance_receipts(receipt_id)`, `events(event_id)`, `audit_events(audit_event_id)`, `model_call_ledger(model_call_id)` | each `(id, key)`; global id uniqueness of events / audit / ledger moved to the identity tables |
| UNIQUEs | `audit_events(audit_seq)`; ledger `(tenant_id, request_id)`, `(tenant_id, model_call_id)`, `(tenant_id, model_call_id, egress_processor_id)`, the 6-column binding set | `audit_event_identity(audit_seq)` (+ `audit_events(audit_seq, occurred_at)`); the four ledger UNIQUEs on `model_call_identity`, global again |
| inbound FKs (8) | `ingest_tickets.redeemed_event_id`, `messages.event_id` → events; `operation_receipts.audit_event_id` → audit_events; budget reservations, data disclosures, contribution candidates, contribution executions ×2 → ledger | names and column lists unchanged, re-pointed to `event_identity` / `audit_event_identity` / `model_call_identity`; no FK references a partitioned table (gate `c36_no_fk_into_log_rows`) |
| ledger dedupe | `ON CONFLICT (tenant_id, request_id) DO NOTHING` in two writers | the claim raises 23505 on any duplicate; the writers lock, look up, insert only when absent (ruling E3-b) |
| `private.events` | no time column | `recorded_at timestamptz NOT NULL DEFAULT now()`, legacy rows backfilled from `evidence_objects.created_at` (L12) |
| `audit_seq` | identity sequence of the heap | a new identity on the parent restarted at `greatest(max + 1, the legacy next value)` |
| `control.retention_policies` | v1 per tenant, 0 rows, no reader | dropped and replaced by v2 (per table, all tenants, monthly) |
| EVENTS / AUDIT_EVENTS retention | — | impossible by CHECK (card 37 / separate approval line) |


### D-C `control.retention_policies` v2 and `control.partition_registry` (0224)

- v1 is dropped only when it holds 0 rows and nothing references it (manifest precheck plus a body guard), then v2
  is created. One row is one approval: the latest `policy_revision` of a `table_key` is its policy; NULL
  `retention_months` keeps forever. **Retention granularity: per table, every tenant at once, whole UTC months**;
  a per-tenant TTL is not expressible (§37 tombstones remain the per-tenant path).
- `table_key` excludes `EVENTS` (card 37's rebuild baseline adds it with its coverage hold) and `AUDIT_EVENTS` (a
  separate approval line). The CHECK, not code, makes their policies impossible (T-G1).
- `control.partition_registry` holds one row per leaf: bounds (NULL lower = MINVALUE), `ATTACHED | DROPPED`, the
  daemon's two proposal columns and the drop receipt. "Sealed" is derived (`upper_bound <= month start`), not
  stored. The rls-check arm keeps its ATTACHED rows equal to `pg_inherits`. A leaf is its schema-qualified name plus
  its bounds, verified against the catalog wherever it is acted on; no relation OID is stored or trusted (0231,
  "Registry by name").
- Grants (§6.2.2 columns): role_maintenance SELECT on both and column UPDATE on `proposed_at`,
  `proposed_policy_revision`; every other runtime role nothing (v1's control-domain SELECT is revoked).

### D-E Leaf creation and sealing

`control.partition_create_month` (owner definer) creates the next UTC month only: no gap, no overlap, idempotent
(`exists`), the first leaf of an empty parent is the current month, and never a DEFAULT partition.
`control.partition_adopt_leaf` (owner definer) re-owns the leaf, ENABLEs and FORCEs RLS, replaces its policy set
with the parent's verbatim, copies the parent's statement-level triggers (not cloned by PostgreSQL, P5), revokes
every grant but the owner's, and registers the leaf with the bounds read from `pg_get_expr(relpartbound)`.
`control.partition_parent` is the one table_key → (parent, key column) map; it refuses a parent that is not range
partitioned on that column.

### D-G / D-J The chokepoint

`control.partition_drop_check` and `control.partition_drop` are SECURITY INVOKER, `row_security = off`,
superuser-only by their first statement, EXECUTE revoked from PUBLIC. Every predicate lives there: latest and
effective policy, keep-forever, cutoff (UTC month arithmetic), newest leaf, registry = catalog by name and bounds
(`registry_catalog_mismatch <leaf>`, 0231; it replaced `stale`), proposal corroboration,
SHARE lock on the leaf, the holds (MODEL_CALL_LEDGER: RESERVED calls, RESERVED budget reservations, non-terminal
contribution executions; STAGE_RUNS: unfinished stages; an unknown key raises `no_hold_rule`) and the row count.
`partition_drop` re-runs the check, takes `rows_dropped` from its own count, executes the statements built by
`control.partition_drop_statements` (`DETACH`, then `DROP … RESTRICT`, never CASCADE) and writes the receipt.

### D-H Executor principal and the resident boot refusal

The executor is the existing superuser `HUMAUX_MIGRATOR_PG_DSN`, in the operator shell for one command
(`HUMAUX_MIGRATION_OWNER_PG_DSN` is struck). Both resident modes (`--serve`, `health serve`) call
`resident::refuse_owner_credentials()` first and exit 2 when that key is present at all, naming the key and never
its value (T-H3).

### D-H / D-I / D-J The executor (S6)

- `RetentionExecutor::connect` (`crates/adapters/src/postgres.rs`) is `MigratorDbPool::connect` (frozen §6.2.0 set
  refused) plus `rolsuper` of `session_user`; either mismatch is `RoleMismatch` and the CLI exits 2 before any other
  statement (T-H1: the maintenance DSN and a CREATEROLE non-superuser).
- `bins/maintenance/src/retention.rs` holds flag parsing and receipts only; `adapters::maintenance_repo` calls the
  owner functions. Every arm requires the §77 fields and `--lock-timeout-ms` (no default, §78.1); each transaction
  sets `lock_timeout`, then `pg_try_advisory_xact_lock("HXRETAIN")` (`busy` otherwise, T-H2). The client never sets
  `row_security`; the §77 row (`RETENTION_POLICY_APPROVED`, `PARTITIONS_CREATED`, `PARTITION_DROPPED`, system tenant,
  risk tag `retention`) is written in the effect's transaction right before COMMIT (T-I1, T-J1).
- `execute`: `partition_drop_check` → `COPY <leaf> TO STDOUT` over the same connection into
  `<export-dir>/<leaf>__<registry_id>.copy.tmp`, fsync, rename, newline count = the check's row count
  (`export_mismatch` otherwise), sha256 → `control.partition_drop` (re-derives everything, DETACH, DROP RESTRICT,
  receipt) → §77 row → COMMIT. A DROPPED id answers `already_dropped` with the stored receipt, exit 0 (T-J2). A real
  run without `--registry-id` is refused `registry_id_required` before connecting (exit 2). `--dry-run --registry-id`
  prints `control.partition_drop_statements` (the builder `partition_drop` executes) and rolls back (T-J4);
  `--dry-run` alone lists every attached leaf of the policy's table whose verdict is not `not_due`, each checked under
  its own savepoint, with its row count.
- `create-partitions --months-ahead n` (n ≥ 2) calls `control.partition_create_month` per table_key until the last
  attached upper bound reaches the UTC month start + (n + 1); a second run creates nothing and writes no audit row
  (T-F1).
- Exit codes follow the binary's one table (ADR-0053 D-F): 0 done / no-op / dry run, 3 refused with
  `{"outcome":"refused","reason":<code>}` (every `retention refused: <code>` of the owner functions, `busy`, and a
  policy CHECK refusal `policy_check:<constraint>`), 2 usage or principal mismatch, 1 infrastructure (including a
  `lock_timeout` 55P03). The design's D-I sentence "2 refusal, 1 busy" is superseded here so that 2 keeps meaning
  usage/config for every subcommand of the binary.

### D-K The PARTITIONS proposer and the horizon gauge (S6)

`MaintenanceTask::Partitions` (label `partitions`, key `HUMAUX_MAINTENANCE_SERVE_PARTITIONS_EVERY_SECONDS`, no LIMIT
key, no tenant page) runs `maintenance_repo::propose_partitions` as role_maintenance in one transaction: the
proposal UPDATE (latest revision per table_key, `retention_months IS NOT NULL`, `effective_at <= clock_timestamp()`,
the D-J cutoff and newest-leaf terms, re-proposing a row proposed under another revision or before `effective_at`)
and the horizon read over `control.partition_registry ⋈ pg_inherits` for every closed key (`-1` without a leaf). It
counts `maintenance_task_runs_total{task="partitions"}` once per run. Success sets
`partition_horizon_months{table}` (lowercase table_key); a failure first resets the family, so a later clean cycle's
200 renders the header with no sample (T-K3). Holds are never proposed (tenant-blind under FORCE RLS).
`PartitionTable` is the Rust closed set of `table_key`, reconciled with both DB CHECKs by
`partition_table_keys_match_the_rust_enum`.

### D-F Horizon: three pre-created months, a manual monthly step, a gauge and three alerts (S7)

- Migrations leave the current UTC month plus three; `humaux-maintenance retention create-partitions
  --months-ahead 3` is a **manual monthly operator step** (runbook §5.1 table, §5.3) run from the operator shell with
  the superuser DSN exported for that one command. No scheduler, cron entry or supervised environment stores that
  DSN (D-H; both resident modes refuse to boot with it).
- `partition_horizon_months{table}` = whole months from the current UTC month start to the last ATTACHED upper bound,
  minus 1: 3 right after a run, 2 during the next month, ≤ 1 after a missed run; `-1` when a table has no leaf. A
  failed PARTITIONS run removes the family (never a stale value).
- §42 rules in `deploy/prometheus/alerts.rules.yml` (E1 rows):
  - `PartitionHorizonShort`: `min by (table) (partition_horizon_months) <= 1` for 1h, WARNING;
  - `PartitionHorizonExhausted`: `min by (table) (partition_horizon_months) <= 0` for 10m (spelled `for: 600s` so
    the `lag` row of `mutations.sh` keeps the file's only 10-minute token), CRITICAL;
  - `PartitionHorizonAbsent`: `absent(partition_horizon_months)` for 2h, CRITICAL.
  Promtool cases (`deploy/prometheus/tests/alerts.test.yml`): Short firing at horizon 1 (only that table, labelled
  by `table` alone) and silent at 2 / 3; Exhausted firing at 0 and at -1, silent at 1; Absent firing when only the
  daemon's counters are present for 2h, silent while the family is present and across a 20-minute failure gap;
  `MaintenanceTaskFailing` firing on `{task="partitions",outcome="failed"}` after the gap. Mutations (each red):
  `horizon_le1` (`<= 1` → `< 1`), `horizon_min` (drop the `min by`), `horizon_le0` (`<= 0` → `< 0`),
  `horizon_absent` (the absent rule made never-firing): `mutations: red=27/27`.
- **metrics-registry D7 / D8.** The daemon's zero-state render (`--serve --metrics-families`) declares the family
  with no sample (truthful: no PARTITIONS run yet), and D7 counts a header-only family as unexported (ADR-0061
  review-fix 3, F10), so D8 refused the three rules. `xtask/src/metrics_registry.rs` gains the closed list
  `ABSENT_UNTIL_FIRST_RUN = [partition_horizon_months]` (design E4: "only if D7 needs the family listed"): only a
  family named there counts as exported from its `# TYPE` line alone; every other header-only family stays
  unexported (unit test `header_only_family_is_exported_only_when_absence_is_its_signal`, red when the entry is
  misspelt).
- **What a miss breaks.** No DEFAULT partition: the first insert at or past the last upper bound fails 23514 for
  every tenant at the same instant (00:00 UTC on the 1st): audited admin operations abort, every model call is
  refused before dispatch, `remember` / evidence ingest fail, the maintenance doors answer 503. No data is lost.
  23514 maps to 409/400 in eleven repositories (L15, card 36b), so the outage is invisible to 5xx monitoring: the
  three alerts are the alarm. Margin: after a run in month M the last bound is M+4; Short fires at M+2, Exhausted at
  M+3. **Never** attach a DEFAULT partition as a stop-gap.
- Rejected: "the insert failure is the alarm" (card scope 4; false here); storing the migrator DSN in a scheduler;
  a DEFAULT partition as a safety net; daemon-driven creation through an owner definer granted to role_maintenance
  (a privilege expansion R-36(c) reserves for an ADR; the upgrade path if Short ever fires in production, L9);
  N = 12 (more empty relations, no extra safety beyond the alert margin).

### D-L rls-check partition arm

`check_partition_leaves`: no DEFAULT partition; every leaf owned by role_migration_owner with RLS + FORCE, its
parent's policy set and statement triggers, and no ACL entry for any other role; registry ATTACHED rows equal
`pg_inherits` by name, parent and bounds (no OID, 0231; the SQL is `crates/testkit/sql/partition_registry_drift.sql`,
shared with the restore witness); no FK references a partitioned table; the nine machinery functions executable by their owner only,
and the two hold readers invoker with `row_security=off`. The domain-default and forbidden-verb scans skip
`relispartition` leaves; `db_objects.md` lists parents only (`partitioned (monthly leaves: control.partition_registry)`).

### D-A / D-D Pilot conversions (S2: 0225 stage_runs, 0226 messages, 0227 maintenance_receipts)

| table_key | table | key | PK before → after | FKs | key-freeze trigger |
|---|---|---|---|---|---|
| `STAGE_RUNS` | `ops.stage_runs` | `started_at` | `(stage_run_id)` → `(stage_run_id, started_at)` | outbound tenant FK re-created on the parent; nothing references it | yes (four runtime roles hold UPDATE) |
| `MESSAGES` | `private.messages` | `created_at` | `(message_id)` → `(message_id, created_at)` | outbound tenant, conversation and `private.events(event_id)` FKs re-created unchanged (0228 re-points the last one to `private.event_identity`); nothing references it | yes (two runtime roles hold UPDATE) |
| `MAINTENANCE_RECEIPTS` | `ops.maintenance_receipts` | `ran_at` | `(receipt_id)` → `(receipt_id, ran_at)` | outbound tenant FK re-created; nothing references it | no: no runtime role holds UPDATE (asserted by T-C6) |

- One transactional FORWARD_ONLY migration per table, one text for dev and production: LOCK ACCESS EXCLUSIVE →
  snapshot (count + `partition_catalog_fingerprint`) → RENAME → strip policy, PK, FKs and secondary index → parent
  `LIKE … INCLUDING DEFAULTS/CONSTRAINTS/GENERATED/STORAGE/COMMENTS/COMPRESSION` plus explicit PK, FKs, index,
  policy, ENABLE + FORCE RLS, owner, `REVOKE` of 0011's default grants and the exact legacy grant cells → bounds
  from data in UTC (`B` = month after `greatest(max(key), now())`; `L` = MINVALUE when history predates the current
  month, else the current month) → bounded CHECK NOT VALID → VALIDATE → ATTACH → drop the CHECK → rename to `_hist`
  or `_pYYYYMM` → `partition_adopt_leaf` → `partition_create_month` until the current month + 4 → the 7a body block
  (count equal, fingerprint equal except the declared key-freeze trigger, first differing row in the message).
- Contract changes (E3) so far: the three PKs above gain their key column. No UNIQUE and no inbound FK exists on
  these tables, so nothing else moves; no identity table is needed (L10). No DEFAULT partition; manifest checks are
  catalog-only (`to_regclass`).
- The three tables are empty on dev and in production, so each heap becomes the current month's leaf
  (`_p202610`) and three future months follow. A non-empty heap with history becomes `_hist` `[MINVALUE, B)`
  (L4; proven by T-D5 with seeded rows).

### D-B / D-D Events (S3: 0228)

| table_key | table | key | PK before → after | FKs | key-freeze trigger |
|---|---|---|---|---|---|
| `EVENTS` | `private.events` | new `recorded_at timestamptz NOT NULL DEFAULT now()` | `(event_id)` → `(event_id, recorded_at)` | outbound `events_event_id_fkey` → `private.evidence_objects` re-created on the parent; inbound `ingest_tickets_redeemed_event_id_fkey` and `messages_event_id_fkey` re-pointed, names and columns unchanged, to `private.event_identity` | no: no runtime role holds UPDATE |

- **Identity table.** `private.event_identity (event_id uuid PK REFERENCES private.evidence_objects)`, not
  partitioned, no tenant_id, no RLS, every runtime cell `—` (§6.2.2 column added; rls-check `NAMED_NO_NON_OWNER_GRANTS`).
  Backfilled from the heap in the migration; afterwards filled by `events_identity_claim` (BEFORE INSERT, owner
  SECURITY DEFINER `private.event_identity_claim()`). A second event for one evidence row raises 23505 on
  `event_identity_pkey`, in any month (T-B2). No skip branch (the ledger's claim has none either, ruling E3-b).
- **Release on row DELETE (addition to the design).** `events_identity_release` (AFTER DELETE, owner SECURITY INVOKER
  `private.event_identity_release()`) deletes the identity row of a deleted event. Reason: 29 test files plus
  `xtask/src/{e2e_seed,switch_visible}.rs` tear down with `DELETE FROM private.events …; DELETE FROM
  private.evidence_objects …` (grep, 2026-10-05); with a permanent identity row referencing the evidence, every one
  of those evidence deletes would fail with 23503 once dev is converted. With the trigger, a referenced event still
  refuses deletion with 23503 (now through its identity row) and an unreferenced event and then its evidence delete
  exactly as before (proven by `event_references_resolve_through_the_identity_table`). Invoker, so only a principal
  that may DELETE events (owner, superuser; no runtime role) can release a row. A partition drop fires no row
  trigger, so identity rows of dropped months persist as tombstones (D-B invariant 3 unchanged).
- **Backfill.** `recorded_at` of the 19,832 legacy rows on the dev copy = their evidence's `created_at` (L12); the
  copy's events all landed in `private.events_hist [MINVALUE, 2026-11-01)`, then `_p202611` … `_p202701`.
- **Orphan refusal (main-line dev orphan note).** The first body block counts, per FK the migration re-creates
  (`events_event_id_fkey`, `ingest_tickets_redeemed_event_id_fkey`, `messages_event_id_fkey`), the rows without a
  target and refuses `c36 precheck: <fk>: <n> orphan rows - repair data first` before any DDL. No FK is weakened.
- **Policy deparse (fix to S1).** The events policy names its own table (`events.event_id` inside an EXISTS). Deparsed
  against the parent it does not re-parse on a leaf (`missing FROM-clause entry for table "events"`), so
  `partition_adopt_leaf` deparses the parent's policy against the leaf when the leaf's column numbers equal the
  parent's (always, for these tables) and against the parent otherwise. The rls-check arm, T-C2 and the 0228
  postcheck compare leaf and parent policies both deparsed against the parent.
- **FK re-point.** `NOT VALID` then `VALIDATE` in the same transaction, as designed (PostgreSQL 18 accepts both on the
  partitioned `private.messages`); the inbound-FK precheck excludes a partitioned referrer's per-leaf clones
  (`conparentid = 0`).

### D-B / D-D Audit events (S4: 0229)

| table_key | table | key | PK / UNIQUE before → after | FKs | key-freeze trigger |
|---|---|---|---|---|---|
| `AUDIT_EVENTS` | `control.audit_events` | `occurred_at` (the caller's `p_ts`, L6) | PK `(audit_event_id)` → `(audit_event_id, occurred_at)`; UNIQUE `(audit_seq)` → `(audit_seq, occurred_at)` | outbound `audit_events_tenant_id_fkey` → `control.tenants` and `audit_events_actor_user_id_fkey` → `control.users` re-created on the parent; inbound `operation_receipts_audit_event_id_fkey` re-pointed, name and column unchanged, to `control.audit_event_identity` | no: the §77 row guard already refuses every UPDATE |

- **Identity table.** `control.audit_event_identity (audit_event_id uuid PK, audit_seq bigint NOT NULL UNIQUE,
  occurred_at timestamptz NOT NULL, tenant_id uuid NOT NULL)`, not partitioned, ENABLE + FORCE RLS with the parent's
  tenant policy verbatim, every runtime cell `—` (§6.2.2 column added; rls-check `NAMED_NO_NON_OWNER_GRANTS`). It
  carries the two global uniquenesses the heap had: a second row with an existing `audit_event_id` (any
  `occurred_at`) raises 23505 on `audit_event_identity_pkey`, a forced duplicate `audit_seq` on
  `audit_event_identity_audit_seq_key` (T-B3). No skip branch.
- **Claim trigger is SECURITY INVOKER (deviation from D-B's "owner definer").** The only writer of
  `control.audit_events` is the owner definer `control.audit_event_insert` (runtime roles hold only SELECT), so inside
  it `current_user` is already the owner and the identity insert faces the same FORCE-RLS tenant check as the parent
  insert (card 33 lesson; a call without the tenant GUC is refused 42501 and writes nothing, now naming
  `audit_event_identity`). A definer would additionally refuse a superuser's GUC-less insert that the parent accepts
  (superusers bypass RLS, the definer's owner does not). Invoker therefore changes no outcome on the production path
  and grants nothing extra. rls-check pins both functions owner-only (13 partition functions).
- **Release on row DELETE.** `audit_events_identity_release` (AFTER DELETE, owner SECURITY INVOKER
  `control.audit_event_identity_release()`): audit rows are append-only, so this fires only on the test-teardown path
  that disables `audit_events_reject_mutation` (`crates/adapters/tests/support/operation_receipt_fixture.rs`). A
  receipt-referenced audit row still refuses deletion with 23503 (now through its identity row), as before the
  conversion. In replica mode (the three teardown paths in the dev orphan note) no row trigger fires and identity
  rows stay behind as tombstones, which no FK or uniqueness check is harmed by.
- **audit_seq (F7).** `DROP IDENTITY` on the heap, `ADD GENERATED ALWAYS AS IDENTITY` on the parent, then `RESTART WITH
  greatest(max(audit_seq) + 1, the legacy sequence's next value)`: the first row after the conversion takes exactly
  the value the legacy sequence would have handed out, so §77 Audit Batch order stays monotonic even when rolled-back
  inserts had moved the sequence past the maximum (T-D4). The 7a block refuses `c36 audit_seq drift` otherwise.
- **§77 guards.** `audit_events_reject_mutation` (BEFORE UPDATE OR DELETE, row) is re-created on the parent and cloned
  onto every leaf; `audit_events_reject_truncate` (BEFORE TRUNCATE, statement) is re-created on the parent and copied
  onto every leaf by `partition_adopt_leaf` (P5). UPDATE and DELETE on the parent or a leaf, and TRUNCATE of the
  parent or any leaf, are refused 42501 for the superuser too
  (`audit_guards_and_the_tenant_guc_insert_definer_survive_the_conversion`).
- **Orphan refusal.** The first body block (after the ACCESS EXCLUSIVE lock) counts, per FK the migration re-creates
  (`audit_events_actor_user_id_fkey`, `audit_events_tenant_id_fkey`, `operation_receipts_audit_event_id_fkey`), the
  rows without a target and refuses `c36 precheck: <fk>: <n> orphan rows - repair data first` before any DDL. On dev
  that is 14,824 audit rows whose tenant is gone (dev orphan note): the main line repairs them before D-M.

### D-B / D-D Model call ledger (S5: 0230)

| table_key | table | key | PK / UNIQUE before → after | FKs | key-freeze trigger |
|---|---|---|---|---|---|
| `MODEL_CALL_LEDGER` | `ops.model_call_ledger` | `called_at` (`DEFAULT now()`) | PK `(model_call_id)` → `(model_call_id, called_at)`; the four UNIQUEs `(tenant_id, request_id)`, `(tenant_id, model_call_id)`, `(tenant_id, model_call_id, egress_processor_id)` and the 6-column reasoning binding set move to `ops.model_call_identity` (`model_call_identity_*`); the ledger keeps lookup indexes `(tenant_id, request_id)`, `(tenant_id, model_call_id)` and `idx_model_call_ledger_tenant` | the 12 outbound FKs re-created on the parent; new `model_call_ledger_model_call_id_fkey` → `ops.model_call_identity` (D-B invariant 2); the five inbound FKs (`retrieval_provider_budget_reservations_model_call_fk`, `data_disclosures_reasoning_model_call_fk`, `contribution_candidates_reasoning_model_call_exact_fk`, `contribution_executions_coverage_model_call_fk`, `contribution_executions_assessment_model_call_fk`) re-pointed, names and column lists unchanged, `NOT VALID` then `VALIDATE` | no: the §19.1 guard already freezes `called_at`; 0230 adds `model_call_id` to its immutable columns (with the PK now `(model_call_id, called_at)`, an UPDATE to another call's id in another month would otherwise duplicate an id the old PK kept unique) |

- **Identity table.** `ops.model_call_identity (model_call_id uuid PK, tenant_id, request_id, called_at,
  egress_processor_id, binding_id, binding_version, reasoning_domain_id, profile_version)`, not partitioned, ENABLE +
  FORCE RLS with the parent's tenant policy verbatim, every runtime cell `—` (§6.2.2 column added; rls-check
  `NAMED_NO_NON_OWNER_GRANTS`). The copied columns are exactly the FK-target columns, all frozen by the guard.
- **Claim trigger** `zz_model_call_identity_claim` (BEFORE INSERT, owner SECURITY DEFINER
  `ops.model_call_identity_claim()`; `zz_` makes it fire after both BEFORE INSERT validators, P2). It inserts the
  identity row with no `ON CONFLICT` (ruling E3-b): any duplicate raises 23505 on the identity table's copy of the
  former UNIQUE (`model_call_identity_request_id_unique` for a second `(tenant_id, request_id)`,
  `model_call_identity_pkey` for a second `model_call_id`), after waiting on an uncommitted duplicate as the heap's
  index did, exactly like the events and audit claims. No skip branch, so a plain duplicate INSERT keeps erring
  (T-B1, `a_duplicate_request_id_is_refused_by_the_identity_claim`). FORCE RLS on the identity table applies to the
  owner, so a session subject to RLS is checked against its own tenant GUC before the unique check (another tenant's
  row is refused 42501, as the parent's policy refuses it);
  a session whose login role bypasses RLS (migrate, the test superuser) has the claim checked against the row's own
  tenant, since the parent's policy never applied to it either (deviation from D-B's plain definer, same reason as
  S4's invoker audit claim: superuser inserts without the tenant GUC, e.g. `crates/adapters/tests/model_call_ledger.rs:847` and `retrieve_no_hidden_generative_recall.rs:131`).
- **Writers (ruling E3-b).** `reserve_in_txn` and `reserve_reasoning_call_in_txn` lose `ON CONFLICT (tenant_id,
  request_id) DO NOTHING` and implement it explicitly: take the existing transaction-scoped `model-call-request:`
  advisory lock on `(tenant_id, request_id)` (the reasoning lookup already took it), look the key up, insert only when
  absent, otherwise return the existing row (`already_reserved`; the reasoning writer keeps its RESERVED + snapshot
  check). A retry returns the existing reservation in any month, and a concurrent writer waits on the lock until the
  first commits, then returns its row (`a_duplicate_reserve_returns_the_existing_row_never_a_second_one`;
  `two_concurrent_writers_for_one_key_leave_one_ledger_row` in `crates/adapters/tests/model_call_ledger.rs`, gate
  `c36_ledger_claim_raises_named`). An INSERT outside the writers (no lock) that races a writer gets the claim's
  23505, never a second row.
- **No release trigger.** The guard refuses DELETE and TRUNCATE for every role, the owner included, so a ledger row
  is never deleted except in replica mode (three test teardowns), where no row trigger fires; identity rows then stay
  as tombstones, which no FK or uniqueness check is harmed by.
- **Rowtype rebind (F10 gap, fixed in 0230).** 0133's `private.assert_current_contribution_reservation_authority`
  takes `ops.model_call_ledger` as its parameter type. That is a `pg_depend` from `pg_proc` onto the heap's
  **rowtype**, which F10's precheck (onto the relation) does not see; the RENAME makes it one leaf's rowtype, and the
  `contribution_reservation_authority_validate` trigger then fails 42883 for every row routed to any other leaf (the
  next month on dev). 0230 re-creates it on the parent's rowtype from its own `prosrc` with 0133's attributes (a leaf's
  row converts to its parent's rowtype), drops the leaf-bound one, and its precheck now refuses any other dependent of
  the rowtype. No other §48.1 table has a rowtype dependent (dev catalog, 2026-10-05).
- **Locks (measured, `pg_locks` before COMMIT).** Dropping an FK removes its RI triggers on both tables, so 0230 holds
  ACCESS EXCLUSIVE not only on the ledger, its leaves and the identity table but on all twelve referenced tables
  (`control.tenants`, `control.workspaces`, the eight reasoning-route / billing tables, the two health-observation
  tables) and the four referencing tables, for its whole transaction (≤ 922 ms on the dev copy). The design's "SHARE
  ROW EXCLUSIVE on every table on either end" understates it for every conversion that drops an FK (0228 and 0229
  too); the runbook's "stop every writer, then migrate" already covers it.
- **Orphan refusal.** The first body block counts, for every FK 0230 re-creates (12 outbound, 5 inbound, read from
  `pg_constraint`, MATCH SIMPLE), the rows without a target and refuses `c36 precheck: <fk>: <n> orphan rows - repair
  data first` before any DDL. On dev that is 4 ledger rows whose tenant is gone (dev orphan note).

### Ledger dedupe: ruling E3-b (S5b)

S5 found that D-B's "RETURN NULL so the ledger row is skipped" also turned a plain duplicate INSERT into a silent
0-row no-op, while the inherited chain gate `adapters_route_health_admission`
(`crates/adapters/tests/reasoning_route_health_admission.rs:1003-1010`) asserts that the duplicate errs ("one
tenant/request attempt key cannot fork"), and E3 was approved only with the existing ledger suites green unchanged.
Main-line ruling E3-b (2026-10-05) overrides D-B on this point: option (b), as recorded under "Claim trigger" and
"Writers" above: one mechanism for the three identity tables (raise 23505), the dedupe explicit in the two writers.
Rejected: (a) keeping the skip and amending the inherited assertion, which weakens a contract test and hides a
caller's programming error behind a trigger (Baseline's guard rule likewise forbids a `RETURN NULL` that silently
swallows a mutation). Fault witnesses (S5b): the writer lookup dropped ⇒ the second writer's 23505 escapes to the
caller ⇒ red; the claim back to `RETURN NULL` ⇒ `reasoning_route_health_admission.rs:1007` red.

### D-M Dev conversion protocol (main line only, ruling E5)

The shared dev database is converted by the main line, never by an implementing slice: every writer stopped and
`pg_stat_activity` recorded → `c36_devcopy.sh` (a fresh `pg_dump -Fc` into `db_backups_<YYYYMMDD>_c36/`, verified by
`pg_restore -l` and sha256, `counts_before.json`; two throwaways restored from that one dump, one migrated with every
manifest check executed; counts and identity counts compared; `rls-check`; the D-N latency test on both; ends
`C36 DEVCOPY COUNTS EQUAL`) → `MAINLINE_GO=1 c36_dev_convert.sh` (refuses unless the copy was EQUAL with every
migration under 30 s, the dump's sha256 still matches and dev has not changed since the dump; then migrates dev,
compares counts, `rls-check`; ends `C36 DEV COUNTS EQUAL`) → services restarted → the rehearsal (`c36_rehearse.sh`)
asserts `partitioned_parents_written_by_rehearsal`. Rollback is restoring the dump into a fresh database and
swapping names (runbook §1.1). Dev's FK orphans from test teardown (dev orphan note) are repaired by the main line
before this; `c36_devcopy.sh`'s `C36_COPY_PURGE_ORPHANS=1` exists only to measure on copies of a dev that still has
them, and `c36_dev_convert.sh` refuses a devcopy log produced that way. No retention executes on dev in this card.

**Protocol breach (recorded by the main line, 2026-10-05).** Dev had 0224-0228 applied at 14:38 by the workflow
verifier's inherited `migrate` gate, before this protocol ran (not by `c36_dev_convert.sh`, without stopped writers or
the devcopy verdict). The rollback points are the dumps in `db_backups_20261005_c36/` (the 13:30 / 13:33
pre-conversion dumps of dev at 0223, and every later devcopy dump). Both scripts are therefore baseline-independent:
"this run" is the set of migrations the target's ledger lacks before its migrate, only those are timed (`SLOW` >= 30 s),
and the two bookkeeping tables are compared by expectation (`ops.schema_migrations` = before + migrations applied in
this run, none left pending; `control.partition_registry` = before + leaves created in this run, from the catalog's
leaf count), every other pre-existing table by equality. A restored dump passes `rls-check` and runs retention
without any rebind step ("Registry by name").

**Protocol hardening (review, 2026-10-05 evening).** Every remaining path that could move dev outside this protocol
is closed: the inherited `migrate` gate line is a `# main line:` comment that `gates_card.sh` skips; the serial lane's
`shared_db` / `post_0132` resources check that dev is at head and never run `migrate` (they used to apply any pending
file, a forward fix such as 0232 included); `c36_dev_orphan_repair.sh repair` needs `MAINLINE_GO=1` and takes a fresh
timestamped dump per run; `c36_devcopy.sh` refuses a DSN whose database it cannot swap (keyword form, trailing slash,
encoded name) instead of falling through to dev, and `c36_dev_convert.sh` refuses a `HUMAUX_TEST_PG_DSN` that does not
name dev in URL form. The orphan scan behind `dev_no_fk_orphans` fails closed (`ORPHANS unknown`, exit 2) when it
cannot connect or a scan statement errors. `c36_devcopy.sh` prunes its backup directory on every exit (keeps the newest
EQUAL dump, its own dump and the dump the last conversion ran on; removes older pre_c36 dumps and finished runs' work
files; never a pre_orphan_repair dump), so repeated rehearsals do not fill the disk.

`docs/ops/rehearse.sh` is not unchanged by this card: it gains ONE line, `HUMAUX_MAINTENANCE_SERVE_PARTITIONS_EVERY_SECONDS=3`
in the resident daemon's environment (the PARTITIONS task's required cadence key, D-K; without it the daemon refuses to
boot). `c36_rehearse.sh`'s header says so.

### Registry by name (logical-restore finding, 2026-10-05)

- **Finding (main line, proven).** 0224 stored `leaf_oid` in `control.partition_registry` and every consumer trusted
  it: `partition_adopt_leaf` matched name AND OID on re-adoption, `partition_drop_check` resolved the leaf through the
  OID, the rls-check arm compared the OID, the daemon's horizon read joined `pg_inherits` on it. A logical
  dump/restore renumbers every relation: the D-M dev-copy run restored a dump of dev (at 0228) and `rls-check` failed
  `ADR-0063 partition leaves: fail — <every leaf>: leaf with no matching ATTACHED registry row`
  (`db_backups_20261005_c36/rls_humaux_thread_c36_50952_after.log`). Nothing wrong could be dropped (name and OID
  both had to match: refuse-closed), but after the documented rollback (restore the dump, swap names) rls-check is red
  and every drop refuses: retention could never run again. A P1 against this card's own rollback contract.
- **Ruling.** The registry is keyed by NAME + BOUNDS verified against the catalog; no stored OID is trusted or
  required.
- **0231 `partition_registry_by_name`.** `leaf_oid` is retired, not dropped: the applied manifests of 0225-0230 name
  `r.leaf_oid` in their postchecks, `migration-rehearsal` EXPLAINs every manifest check against HEAD (ADR-0050 D-E),
  and those manifests may not change, so dropping the column would make six frozen checks invalid at HEAD. It becomes
  nullable, is cleared, and `CHECK partition_registry_leaf_oid_retired (leaf_oid IS NULL)` keeps it empty; nothing
  reads it. UNIQUE (`leaf_name`) stays unconditional (also among non-DROPPED rows; the adopter's `ON CONFLICT` arbiter
  needs it whole). `partition_adopt_leaf` registers name + catalog bounds and requires an existing row of that name to
  agree (same table_key, ATTACHED, same bounds). `partition_drop_check` (D-J step 4) resolves `to_regclass(leaf_name)`
  and requires the canonical name, `relkind = 'r'`, a partition of `control.partition_parent(table_key)` and
  `pg_get_expr(relpartbound)` equal to `FOR VALUES FROM (<lower>|MINVALUE) TO (<upper>)` rendered by the same session
  (equal text = equal instants); any disagreement raises `retention refused: registry_catalog_mismatch <leaf>`
  before anything acts on the leaf. Signatures, owners, SECURITY attributes, `search_path` / `row_security` pins and
  EXECUTE sets are unchanged (CREATE OR REPLACE; the manifest postcheck and the rls arm pin them). The body re-checks
  every ATTACHED row by name and bounds before clearing the OIDs (a restored copy passes; a disagreeing registry is
  named and refused). Wall clock on the dev copy (24 leaves, `c36_devcopy.sh`, each to the migrate exit):
  `ms<=28.6` (16:33), `ms<=35.4` (16:43) and `ms<=34.2` (16:44, the final 0231 bytes).
- **Other consumers.** The rls-check arm's drift SQL moved to `crates/testkit/sql/partition_registry_drift.sql`
  (name, parent, bounds; no OID) and is `include_str!`-ed by the arm and the witness; its fault test gained a
  lower-bound drift (the upper-bound one already existed). `maintenance_repo::propose_partitions`' horizon read joins
  a leaf by schema-qualified name over `pg_class` (catalog only, no schema USAGE needed by role_maintenance).
  `crates/testkit/sql/integrity_edges.sql`'s tombstone filter resolves the leaf by name.
- **Witness.** `bins/maintenance/tests/partitions.rs::registry_survives_a_logical_dump_and_restore`: a throwaway at
  head with rows in all six converted tables and two expired STAGE_RUNS months under an effective 1-month policy is
  `pg_dump -Fc`'d and `pg_restore`d (inside `humaux-thread-pg`) into a second, empty throwaway (the test asserts the
  leaves were renumbered). On the restored copy: (a) the arm's drift SQL is empty; (b) the daemon's PARTITIONS run
  (2 proposals, every horizon) and the `retention execute --dry-run` listing (leaves, bounds, counts, verdicts `ok`)
  equal the original's; (c) `retention approve` of revision 2, its proposal, a tampered registry upper bound refused
  `registry_catalog_mismatch <leaf>` with nothing changed, then the real drop of the oldest month: exactly that leaf
  gone, its row DROPPED with `rows_dropped` 3, the export sha256 and revision 2, one `PARTITION_DROPPED` §77 row,
  every sibling row and leaf unchanged, the drift SQL still empty. Faults (0231 rewritten, restored, `cmp` identical):
  (i) a stored OID again (the adopter writes it, `partition_drop_check` matches `c.oid = reg.leaf_oid`) ⇒ the restored
  listing says `registry_catalog_mismatch ops.stage_runs_p202607` where the original says `ok` ⇒ red; (ii) the bound
  comparison removed from `partition_drop_check` ⇒ the tampered row is dropped (`outcome: dropped`) instead of refused
  ⇒ red. Gate `c36_registry_survives_restore_named`.
- **Rollback path, now proven.** The D-M rollback (restore the dump into a fresh database, swap names) yields a
  database that passes rls-check and runs retention with no rebind step: the witness is that rollback in miniature,
  and `c36_devcopy.sh` (2026-10-05 16:44) restored a dump of dev at 0228, migrated it through 0231 (`3 applied`) and
  ended `C36 DEVCOPY COUNTS EQUAL` with `rls-check` EXIT 0 and no `SLOW` line.
- **Rejected.** A rebind step after restore (an operator command re-reading OIDs): a second source of truth to keep
  in step and one more manual step in a rollback; dropping the column outright (six frozen manifest checks invalid at
  HEAD); keeping the OID as a fast path with the name as fallback (two identities, the drift the finding was).

### E8 R-36 refinements, for the record

- The "stable identity tables" of R-36(a) are the three identity tables: the target of every inbound FK and every
  global UNIQUE; partition drops never touch them (tombstones, L1).
- Plain `DETACH` plus `DROP … RESTRICT` in one transaction instead of `DETACH … CONCURRENTLY`: the drop, the
  registry receipt and the §77 row commit together (CONCURRENTLY cannot run in a transaction and leaves a pending
  state on failure).
- Until card 37 proves PITR coverage of a dropped range, every DROP is preceded by an export verified by row count
  (the database's own count) and sha256 (the executor's attestation, L16).
- "Holds" are evaluated only by the superuser chokepoint (`control.partition_drop_check`), never by the tenant-blind
  daemon; the daemon's proposal corroborates and authorises nothing.

### Addenda pointers

- ADR-0059 (executor principal): the retention executor is the existing superuser `HUMAUX_MIGRATOR_PG_DSN` principal
  through `MigratorDbPool` plus a `rolsuper` check (`RetentionExecutor::connect`); no new login role, no LOGIN window
  on role_migration_owner, no membership (D-H). The dedicated NOLOGIN executor role is the card-54 upgrade (L14).
- ADR-0062 (resident daemon): addendum 2026-10-05 in that file — the receipts table is partitioned (E6 / L11
  closed), the `partitions` cluster task joins D-C, and the boot refusal.

## Measurements

### Conversion wall clock (dev copy, S2)

A fresh `pg_dump -Fc humaux_thread_dev` (137,829,273 bytes, 9.7 s, sha256 `ed66f469…e223a316`) restored into
a throwaway, then `cargo xtask migrate --dsn <throwaway>` (every precheck and postcheck executed). Durations are the
gaps between consecutive `ops.schema_migrations.applied_at` (each migration's transaction start); the last one runs
to the migrate process exit, so it is an upper bound.

| migration | rows converted | wall clock |
|---|---|---|
| 0224_partition_machinery | — | 49.8 ms |
| 0225_partition_stage_runs | 0 | 18.1 ms |
| 0226_partition_messages | 0 | 13.9 ms |
| 0227_partition_maintenance_receipts | 0 | ≤ 40.4 ms (to process exit) |

S3 rehearsal (2026-10-05): a fresh read-only `pg_dump -Fc humaux_thread_dev` (137,829,273 bytes, 10.1 s, sha256
`a7af9dd6…7d0b39b`) restored into a throwaway in two steps: pre-data + data, then every row violating one of dev's
378 FKs deleted to a fixpoint (one pass deleted 47,001 rows, a second found none: `control.audit_events` 14,824,
`ops.selection_snapshot_items` 17,145, `control.workspace_memberships` 7,727, `control.usage_reservations` 3,696,
`control.operation_receipts` 952, `private.memory_consolidation_runs` 789, `ops.jobs` 750,
`ops.selection_snapshots` 515, `ops.distill_tenant_scheduler` 290, `control.rate_buckets` 209,
`control.confirm_tokens` 20, `ops.memory_lifecycle_events` 20, `private.memory_consolidation_inputs` 16,
`ops.private_inference_rpc_calls` 11, `private.memory_rollup_sources` 11, `private.memory_rollups` 7,
`private.evidence_subjects` 5, `private.memory_subjects` 5, `private.subjects` 5, `ops.model_call_ledger` 4), then
post-data (all 378 FKs created and validated, as on dev). `cargo xtask migrate --dsn <copy> --through 0228`
(every precheck and postcheck executed): 0224 38.2 ms, 0225 19.3 ms, 0226 14.7 ms, 0227 14.0 ms, **0228 ≤ 244.8 ms**
(to process exit; 19,832 events, identity 19,832, all in `events_hist`, `recorded_at` = evidence `created_at` for
all 19,832; 0 redeemed tickets and 0 messages reference events on dev). Then `cargo xtask rls-check` EXIT 0
(`partition leaves: 16 leaves under 4 parents sealed; 11 partition functions owner-only`) and
`cargo xtask migration-rehearsal` EXIT 0 (392 checks, 0 invalid) against the copy, which was then dropped.

| migration | rows converted | wall clock (S3 copy) |
|---|---|---|
| 0228_partition_events | 19,832 events (+19,832 identity rows) | ≤ 244.8 ms (to process exit) |

S4 rehearsal (2026-10-05): a fresh read-only `pg_dump -Fc humaux_thread_dev` (137,908,318 bytes, 10.4 s, sha256
`7085a780…`) restored into a throwaway as pre-data + data, every row violating one of dev's 378 FKs deleted in
replica mode to a fixpoint (pass 1 deleted 47,085 rows, among them `control.audit_events` 14,824 via
`audit_events_tenant_id_fkey`, `control.operation_receipts` 952 via `operation_receipts_tenant_id_fkey`; pass 2 found
none), then post-data (378 FKs, as on dev). `cargo xtask migrate --dsn <copy> --through 0228`, then
`cargo xtask migrate --dsn <copy>` for 0229 alone (precheck and postcheck executed):

| migration | rows converted | wall clock (S4 copy) |
|---|---|---|
| 0229_partition_audit_events | 231,636 audit rows (+231,636 identity rows), 15,616 receipts re-pointed | ≤ 1,350.7 ms (`applied_at` to process exit) |

All 231,636 rows landed in `control.audit_events_hist [MINVALUE, 2026-11-01)`, then `_p202611` … `_p202701`; the
legacy maximum `audit_seq` was 697,162 with the sequence at 697,169, and the new identity's next value is 697,170.
`cargo xtask rls-check` EXIT 0 (`partition leaves: 20 leaves under 5 parents sealed; 13 partition functions
owner-only`) and `cargo xtask migration-rehearsal` EXIT 0 (394 checks, 0 invalid) against the copy, which was then
dropped.

On the S2 copy afterwards: 3 parents, 12 leaves (`_p202610` … `_p202701` each), no DEFAULT partition, 12 ATTACHED
registry rows, 0 leaves without RLS + FORCE or with a policy set other than the parent's, 0 runtime-role privileges
on any leaf, row counts 0 = 0 for all three; a next-month `ops.stage_runs` row landed in `ops.stage_runs_p202611`
and a month +4 row was refused with 23514 `no partition of relation "stage_runs" found for row`;
`cargo xtask rls-check` (`partition leaves: 12 leaves under 3 parents sealed`) and
`cargo xtask migration-rehearsal` (390 checks, 0 invalid) passed against it. The throwaway was then dropped.

**Dev finding for D-M (S4/S5).** The dev dump does not restore cleanly: 30 validated FKs are violated by rows on dev
itself (for example 14,824 `control.audit_events` rows whose tenant is gone from `control.tenants`, plus orphans in
`ops.model_call_ledger`, `control.operation_receipts`, `ops.jobs` and others), so `pg_restore` skips those 30
constraints. None touches the three S2 tables (0 rows each). It does touch D-D for 0229 audit_events and
0230 model_call_ledger: their parents re-create the tenant/workspace FKs and ATTACH validates them on the `_hist`
leaf, so the conversion would refuse on dev until those orphans are dealt with. The main line decides that before D-M.

S5 rehearsal (2026-10-05): a fresh read-only `pg_dump -Fc humaux_thread_dev` (137,908,318 bytes, 11.1 s, sha256
`5a79e156…`) restored into a throwaway (pg_restore skipped the same 30 FK constraints), every row violating one of
dev's 378 FKs deleted in replica mode to a fixpoint (pass 1 deleted 47,074 rows, pass 2 11 more, pass 3 none; by FK:
`selection_snapshot_items_tenant_id_fkey` 17,145, `audit_events_tenant_id_fkey` 14,824,
`workspace_memberships_membership_fkey` 7,801, `usage_reservations_tenant_id_entitlement_key_window_start_fkey` 3,696,
`operation_receipts_audit_event_id_fkey` 952, `memory_consolidation_runs_reasoning_domain_id_fkey` 789,
`jobs_tenant_id_fkey` 750, `selection_snapshots_tenant_id_fkey` 515, `distill_tenant_scheduler_tenant_id_fkey` 300,
`rate_buckets_tenant_id_fkey` 209, `memory_lifecycle_events_tenant_id_fkey` 20, `confirm_tokens_tenant_id_fkey` 20,
`memory_consolidation_inputs_memory_id_fkey` 16, `memory_rollup_sources_evidence_id_fkey` 11,
`private_inference_rpc_calls_consolidation_run_id_fkey` 11, `memory_rollups_run_id_fkey` 7,
`subjects_tenant_id_fkey` 5, `evidence_subjects_evidence_id_fkey` 5, `memory_subjects_memory_id_fkey` 5,
`model_call_ledger_tenant_id_fkey` 4), then the 30 missing FKs added from dev's own definitions (378 validated).
`cargo xtask migrate --dsn <copy> --through 0229`, then `cargo xtask migrate --dsn <copy>` for 0230 alone (precheck
and postcheck executed):

| migration | rows converted | wall clock (S5 copy) |
|---|---|---|
| 0230_partition_model_call_ledger | 61,031 ledger rows (2,547 RESERVED; +61,031 identity rows); 38,190 budget reservations, 8,089 disclosures, 1,071 candidates, 1,226 executions re-pointed | ≤ 922 ms (`applied_at` to process exit) |

All 61,031 rows landed in `ops.model_call_ledger_hist [MINVALUE, 2026-11-01)`, then `_p202611` … `_p202701`; every
count above was equal before and after. `cargo xtask rls-check` EXIT 0 (`partition leaves: 24 leaves under 6 parents
sealed; 14 partition functions owner-only`) and `cargo xtask migration-rehearsal` EXIT 0 (396 checks, 0 invalid)
against the copy, which was then dropped.

### S7: dev copy, all seven migrations, and insert latency (D-M step 3, D-N)

`c36_devcopy.sh` with `C36_COPY_PURGE_ORPHANS=1` (2026-10-05 13:33, HEAD 1437dae + this card's tree; dev at
`0223_ticket_lost_at`, 0 non-postgres sessions): a fresh read-only `pg_dump -Fc humaux_thread_dev` (139,928,088 bytes,
11.7 s, sha256 `bb0c6346e9d20e45…`, `pg_restore -l` lists TABLE DATA for the six tables and the referencers) restored
into two throwaways; pg_restore skipped dev's 30 orphan-violated FKs in each; in both copies identically one pass
deleted 47,085 orphan rows (a second found none) and the 30 FKs were re-added (378, as on dev). The after copy was
migrated with every manifest check executed: `migrate: pass (7 applied, 191 already-applied, 198 total)`, 6.0 s
including the cargo start. Every pre-existing table's count was equal (`ops.schema_migrations` 191 + 7 = 198),
identity = parent for events (19,988), audit (233,689) and ledger (61,662), 6 parents / 24 leaves / no DEFAULT
partition / no FK into a parent, `rls-check` EXIT 0 (`partition leaves: 24 leaves under 6 parents sealed; 14
partition functions owner-only, 2 hold readers invoker with row_security=off`); the log ends `C36 DEVCOPY COUNTS
EQUAL`, and both copies were dropped. Wall clock per migration = the gap between consecutive `applied_at` (each
migration's transaction start), the last to the migrate exit (an upper bound):

WALL migration=0224_partition_machinery ms=68.2
WALL migration=0225_partition_stage_runs ms=32.7
WALL migration=0226_partition_messages ms=18.5
WALL migration=0227_partition_maintenance_receipts ms=15.1
WALL migration=0228_partition_events ms=327.5
WALL migration=0229_partition_audit_events ms=1563.8
WALL migration=0230_partition_model_call_ledger ms=1275.2 (upper bound: to the migrate exit)

Insert latency (`partitions.rs::partition_insert_latency`, ignored, refuses any database but `humaux_thread_c36_*`;
n = 200 per line, nearest-rank percentiles, debug test build on the dev host, one client): the before copy is the
restored dump at card 35's head (heaps), the after copy the same dump migrated through 0230. Paths: the audit
definer as role_gateway under the tenant GUC; an evidence row plus its event; a stage_runs row; the retrieval-plane
`model_call_ledger::reserve_call` (fresh request ids; after: lock, lookup, insert, identity claim); `op=settle` is
`finalize_call`'s UPDATE by `(model_call_id, tenant_id)` without the key, which probes every ledger leaf (L2):

LAT table=audit_events phase=before n=200 p50_us=1704 p95_us=4409
LAT table=audit_events phase=after n=200 p50_us=1524 p95_us=4838
LAT table=events phase=before n=200 p50_us=836 p95_us=1449
LAT table=events phase=after n=200 p50_us=1275 p95_us=4312
LAT table=stage_runs phase=before n=200 p50_us=528 p95_us=2425
LAT table=stage_runs phase=after n=200 p50_us=480 p95_us=628
LAT table=model_call_ledger phase=before n=200 p50_us=1515 p95_us=2114
LAT table=model_call_ledger phase=after n=200 p50_us=1443 p95_us=2357
LAT table=model_call_ledger phase=before op=settle n=200 p50_us=1037 p95_us=1640
LAT table=model_call_ledger phase=after op=settle n=200 p50_us=1409 p95_us=4416

Reading: every p50 stays within about 0.5 ms of its heap value and every insert stays in the low milliseconds; the
p95s move by up to a few ms in both directions. A first run three minutes earlier on another pair of copies of an
identical dump measured (p50 / p95 µs, before → after) audit 1448/2006 → 1392/2039, events 707/1168 → 643/1262,
stage_runs 524/995 → 658/3841, ledger reserve 1572/2344 → 2329/5658, settle 1385/2795 → 1582/4341 (its wall clock:
0224 84.9, 0225 38.9, 0226 23.4, 0227 19.3, 0228 247.1, 0229 1914.2, 0230 ≤ 993.4 ms) — so run-to-run variance on
this host is of the same size as the before/after difference, and n = 200 on a shared laptop does not resolve a
sub-millisecond effect. The one structural cost is L2 (a by-id statement probes 4 leaf indexes instead of 1). The
main line's D-M run (after dev's orphan repair, without the purge) re-measures and replaces these lines if they
differ materially.

### D-M on dev (main line, 2026-10-05 16:47)

`MAINLINE_GO=1 c36_dev_convert.sh` on `humaux_thread_dev` (writers stopped, devcopy EQUAL on the 16:43:48 dump,
rollback dump `db_backups_20261005_c36/humaux_thread_dev.pre_c36.164348.dump`): wall clock 0229 1330.7 ms, 0230
1029.1 ms; 0231, the last migration of that run, is bounded by the migrate exit and was printed as an epoch then (the
script now prints `ms<=` for it); `C36 DEV COUNTS EQUAL`. 0224-0228 had been applied earlier by the verifier's migrate
gate (protocol breach, D-M) and carry no protocol timing.

## PostgreSQL 18 facts (S1 spike, `bins/maintenance/tests/partition_pg_facts.rs`, PostgreSQL 18.6)

All ten hold; each test went red under a mutated premise once (2026-10-05).

| # | fact |
|---|---|
| P1 | an identity added to the new parent starts at 1; after `DROP IDENTITY` on the legacy heap, ATTACH and `RESTART` above the maximum, the next id continues above it |
| P2 | a BEFORE INSERT row trigger on a partitioned parent returning NULL skips the row and its RETURNING; same-timing triggers fire in name order |
| P3 | a deferred constraint trigger on a partitioned parent fires at COMMIT |
| P4 | FORCE RLS blinds the owner (0 rows); `row_security = off` makes the same read raise 42501 |
| P5 | statement-level triggers are not cloned to partitions |
| P6 | an insert past the last leaf fails with 23514 `no partition of relation …` |
| P7 | ATTACH of a heap with a valid implying CHECK does not scan it (control: without it, it does) |
| P8 | plain DETACH holds ACCESS EXCLUSIVE on the parent |
| P9 | a function-level `SET row_security = off` ends with the call; an owner definer's FORCE-RLS insert then succeeds in the same transaction (control: transaction-level `off` makes it raise 42501) |
| P10 | `LOCK … IN SHARE MODE` needs a table privilege beyond SELECT (42501) — why the executor is a superuser |

## Privilege expansions (E7)

- role_maintenance: SELECT on `control.retention_policies` and `control.partition_registry`; column UPDATE on the
  registry's two proposal columns.
- Nine owner functions with no runtime EXECUTE (PUBLIC revoked), plus (0228) `private.event_identity_claim()`
  (definer) and `private.event_identity_release()` (invoker), plus (0229) `control.audit_event_identity_claim()` and
  `control.audit_event_identity_release()` (both invoker), plus (0230) `ops.model_call_identity_claim()` (definer),
  likewise owner-only; rls-check pins all fourteen. 0230 re-creates 0133's
  `private.assert_current_contribution_reservation_authority` on the parent's rowtype with its owner-only EXECUTE
  unchanged. The maintenance DELETE-door set rls-check pins is unchanged (the six functions of card 35, `ensure_user`
  included): no card-36 function is executable by role_maintenance, and none of them contains a row DELETE.
- The resident daemon gains no privilege beyond the two SELECTs and the column UPDATE above; the executor gains none
  (it is the existing superuser migrator principal, D-H).

## Known limits

- L1 `// ponytail:` identity tables keep one narrow row per log row forever (dedupe and FK targets survive
  retention); prune identity rows older than the oldest attached leaf that no FK references, in a later card, when
  identity size matters.
- L2 a lookup by id without the key probes every leaf's index: the ledger settle UPDATE by `model_call_id` and the
  writers' `(tenant_id, request_id)` lookup (0230 indexes both); measured below (`op=settle`). Upgrade: carry
  `called_at` from the identity row into the settle statement for pruning.
- L3 retention is a whole month for all tenants at once; a per-tenant TTL is not expressible (§37 tombstones remain
  the per-tenant delete path).
- L4 a converted heap with history becomes the transitional `_hist` leaf `[MINVALUE, B)`: not strictly monthly,
  droppable only when `B ≤ cutoff`, then in one unit.
- L5 RESERVED ledger rows in `_hist` (2,547 on the S5 dev copy) hold it (`hold:unsettled_ledger_calls`) until they
  settle; correct, and reported by the executor's refusal.
- L6 `audit_events.occurred_at` is the caller's `p_ts` (application clock): a clock skewed beyond the horizon makes
  audit writes fail closed (23514). Upgrade: the definer clamps or replaces `p_ts` under a spec change.
- L7 one explicitly named leaf per executor run (`--registry-id`); the first approval on a table with N past months
  makes several leaves due at once, each one reviewed dry-run and run. Upgrade: a `--registry-ids` list echoed back by
  the dry-run, if backlogs become routine; never "drop everything due".
- L8 `// ponytail:` the executor's COPY export is the pre-drop safety net until card 37 proves PITR coverage of the
  dropped range; exports hold personal data and follow §44 retention. The superuser COPY bypasses RLS, so a file
  holds every tenant's rows of the leaf: it is created mode 0600 whatever the umask (T-J1 asserts it), fsynced,
  renamed, and its directory fsynced before `control.partition_drop` commits (a crash after the DROP never loses the
  only copy). The operator keeps the export directory owner-only (runbook §5.4).
- L9 leaf creation depends on a monthly **manual** operator step plus the alert margin (two months to the outage
  after `PartitionHorizonShort`). Upgrade: `partition_create_month` granted to role_maintenance (daemon-driven
  creation, no superuser) under an ADR, if the alert ever fires in production.
- L10 stage_runs, messages and maintenance_receipts enforce id uniqueness only per `(id, key)`; ids are server
  `uuidv7()` defaults and nothing references them. Upgrade: an identity table the first time a writer supplies ids
  or an FK targets them.
- L11 a conversion holds its locks (ACCESS EXCLUSIVE on the table, its FK neighbours and, when an FK is dropped, the
  referenced tables) for the whole migration transaction: milliseconds on empty production tables, measured below on
  a dev copy; a large future table would need the split NOT VALID / VALIDATE path across two migrations.
- L12 legacy events carry `recorded_at` = their evidence's `created_at`, not the (never recorded) event insert time;
  only `_hist` holds such rows.
- L13 a new table_key without a hold arm raises `no_hold_rule` and is never dropped by default; adding a hold or a
  table_key is a migration (`CREATE OR REPLACE` of the check).
- L14 `// ponytail:` retention and leaf creation need a superuser for each command; upgrade = a dedicated NOLOGIN
  BYPASSRLS role that owns the leaves and the two drop functions (definers), granted to the migrator principal, with
  an ADR-0059 amendment (card 54, OpenBao), when a managed PostgreSQL without superuser becomes the production target
  (card 39 provisions a superuser-capable operator path or records retention / leaf creation as unavailable:
  fail-closed growth plus alerts).
- L15 a horizon outage presents as 409/400 to clients (23514 → `Conflict` / `InvalidInput` in eleven repositories)
  and is invisible to 5xx monitors until card 36b (one shared mapper: `no partition of relation` → `Internal`); the
  three horizon alerts fire one to two months earlier.
- L16 the export's sha256 and path in the receipt are the executor's attestation; the database counts the rows
  itself but cannot read a client-side file. Card 37's PITR proof replaces the export (L8).
- L17 `create-partitions --months-ahead` is the closed range 2..=24 (T-F2), refused before any connection: each
  month is one leaf per table with its indexes, policies and triggers in one transaction, and the design horizon is 3.
- L18 `reserve_reasoning_call_in_txn`'s own lookup (ruling E3-b) is unreachable with a duplicate on today's only
  caller: `contribution_reasoner` runs `lookup_reasoning_call_in_txn` first under the same `model-call-request:` lock
  in the same transaction (contribution_reasoner.rs:610-623, then :673), and a writer racing between the two
  deadlocks on the reasoner's `humaux-contribution-inputs-v1` lock instead (measured 2026-10-05: the reasoner fails
  `contribution inputs rejected`, Conflict). A mutation of that lookup is therefore equivalent through every public
  entry point; it stays as the writer's self-contained dedupe for a future caller (rustdoc says so).
- L19 `// ponytail:` `cargo xtask serial-lane` drops each lane group's databases when the group ends and on unwind,
  but installs no SIGINT handler: a lane killed by Ctrl-C leaves its current group's databases; the migrate tests'
  throwaway helper drops dead-pid leftovers of its own purpose on the next create.

## Dev integrity finding (2026-10-05)

- **Finding.** The shared `humaux_thread_dev` violates validated FKs: on 2026-10-05 the catalog scan counts 62,314
  violating rows over 31 FKs (per FK, per-leaf clones included; e.g. `ops.selection_snapshot_items` 17,145 and
  `control.audit_events` 14,824 whose tenant is gone, `control.workspace_memberships` 7,875,
  `control.usage_reservations` 3,696, `control.operation_receipts` 952, `ops.jobs` 750), plus 1,767
  `private.event_identity` rows whose `private.events` row is gone (all of them also orphans of their evidence):
  `ORPHANS 64081`. Production cannot have this (tenants are never hard-deleted there), but a conversion that
  re-creates a validated FK refuses on dev (the orphan refusal above) and a `pg_dump` → `pg_restore` of dev loses
  the violated FKs.
- **Root cause.** Test teardowns hard-delete tenants under `SET session_replication_role = replica` — the only way
  past the append-only guards (0128, §77) for test residue — which also skips every RI trigger and every user
  trigger (since 0228 the D-B identity release triggers too), with hand-written DELETE lists that miss any
  dependent table not in the list (every table added after the list was written). The finding named
  `xtask e2e-seed --teardown`, `switch_visible.rs` and `consolidation-worker` `derived_dispatch_e2e.rs`;
  `switch_visible.rs` in fact deleted with constraints enforced (it could leak, not orphan), and 16 further test
  files still used replica mode (`grep -rl session_replication_role --include=*.rs`) — closed below ("The 16
  remaining replica-mode files").
- **Fix: one purge, no table named.** `humaux_testkit::fixture_purge::purge_tenant_fixture_sql` builds one `DO`
  statement (test infrastructure, no migration, no runtime caller): refuse unless the database is
  `humaux_thread_*` and the tenant's name starts `e2e-` (`e2e-seed-<uuid>`, the fixtures' `e2e-fixture …`); `SET
  LOCAL session_replication_role = replica`; mark to a fixpoint every row with the tenant's `tenant_id`, every row
  referencing a marked row through any FK read from `pg_constraint` at run time (any arity, MATCH SIMPLE), and the
  identity row of every marked partitioned-parent row; refuse if a marked row carries another tenant's id; delete
  children first by FK depth, then the identity tables (the rows their skipped release triggers would have
  removed), then the tenant row; restore the caller's replication role. The three paths call it; their DELETE
  lists are gone. Users are not tenant rows: `e2e-seed` and the two fixtures delete theirs afterwards with
  constraints enforced. A lane's `control.processor_models` catalog row stays (append-only,
  `processor_models_identity_immutable`; the old teardown deleted it in replica mode, checking only
  `reasoning_profiles`).
- **Identity tables** are recognised structurally, not by name: a plain table whose primary key is a partitioned
  parent's primary key without the partition key and whose every column is a same-typed column of that parent
  (`private.event_identity`, `control.audit_event_identity`, `ops.model_call_identity`; two other `(event_id)`
  tables fail the column rule). An identity row without a parent row is garbage unless its partition key lies in
  a DROPPED `control.partition_registry` range (a tombstone, D-B invariant 3, L1); `private.event_identity` has no
  key column, which is harmless while EVENTS is not a retention table_key.
- **Scan, repair, gate.** `crates/testkit/sql/integrity_edges.sql` is the one edge generator: the purge's test
  scans its throwaway with it, and the task-work `c36_dev_orphan_repair.sh` reads the same file — `scan` prints one
  `child|parent|constraint|n` line per violated FK and one `identity|<table>|<parent>|<n>` line per identity
  table with garbage, then `ORPHANS <total>`; `repair` keeps its dated `pg_dump -Fc` backup first, deletes FK
  orphans pass by pass and runs the identity sweep only once no FK line is left (a swept identity row may orphan a
  referencer, which the next pass deletes), and exits 0 only when the rescan is clean. Gate `dev_no_fk_orphans`
  (`card36_extra_gates.env`, before `release_build`): the scan's last line must be `ORPHANS 0` — red until the main
  line repairs dev.
- **Witness.** `xtask` `e2e_seed::tests::teardown_leaves_no_fk_orphan_and_no_identity_garbage`: a throwaway at
  head, a tenant seeded through `provision` (two workspaces), `seed_lane` and `seed_second_domain` plus runtime
  residue (evidence + event + identity, memory + link, ledger row + identity, consumed reservation + receipt,
  selection snapshot + item, conversation + message; 35 `tenant_id` tables hold rows), a decoy fixture tenant and
  a non-fixture tenant. After `teardown`: no `tenant_id` row of the tenant, the whole-database scan empty, the
  decoy's rows unchanged, the non-fixture tenant refused and intact. Faults: skip `ops.jobs` in the delete loop ⇒
  `ops.jobs|control.tenants|jobs_tenant_id_fkey|1`; skip the identity tables ⇒
  `private.event_identity|private.evidence_objects|event_identity_event_id_fkey|1`,
  `identity|private.event_identity|private.events|1`, `identity|control.audit_event_identity|control.audit_events|9`,
  `identity|ops.model_call_identity|ops.model_call_ledger|1`.
- **Rule.** A fixture that disables constraint enforcement must prove the data it leaves is consistent: the
  whole-database integrity scan after its teardown, not "teardown returned Ok". "Teardown passes" was never a gate.

### The 16 remaining replica-mode files (2026-10-05)

Each file's replica-mode block was classified by what it does and which database it touches: **(a)** shared-dev
teardown, **(b)** shared-dev fault setup (a row mutated or planted past a guard), **(c)** a throwaway database only.
(a) now goes through `fixture_purge` (no DELETE list left); (b) keeps the one statement the fault needs and the test
ends by purging its fixture tenants (or plants only inside a transaction that is rolled back); (c) is unchanged and
declared. Every fixture tenant a purge reaches is named `e2e-…` (`c31_leak_check.sh`'s `%throwaway tenant%` kept
where it was). Scan = `c36_dev_orphan_repair.sh scan | tail -1` on `humaux_thread_dev` before → after the binary.

| File | Class | What changed | Run (EXIT) | Scan |
|---|---|---|---|---|
| `crates/adapters/tests/support/continuity_0137_cleanup.rs` | (a) dev | replica-mode DELETE list (it never listed `control.workspace_memberships`) → per-tenant fixture purge, last registered first (a decoy's `ops.outbox` row references the fixture tenant's evidence), under the 0137 DDL advisory lock, then the users with constraints enforced; tenants `e2e-fixture w2 …` | `project_continuity_read_0137` 3/3, `…_acceptance` 13/13 ×4 (0) | 0 → 0 |
| `bins/private-worker/tests/derived_dispatch_e2e.rs` | (a) dev (E8 re-drive: throwaway) | replica-mode DELETE list → release the tenants' `ops.provider_slots` (global, no FK), fixture purge per tenant, user; tenants `e2e-fixture …` | 49 passed + live `distill_two_provider_live`, `distill_poison_live`, `distill_fairness_live`, `distill_channel_ab_live` (0) | 0 → 0 each |
| `bins/private-worker/tests/distill_hop_e2e.rs` | (a) dev | same as above; tenant `e2e-fixture distill_hop_e2e throwaway tenant …` | 17/17 incl. live MiniMax (0) | 0 → 0 |
| `bins/private-worker/tests/support/double_spend.rs` | (b) dev | marker only: the planted jobs/calls/ledger rows (no tenant row) live in one transaction that is rolled back | in `derived_dispatch_e2e` (0) | 0 → 0 |
| `crates/adapters/tests/contribution_execution_ingress_0131.rs` | (b) dev | marker; the test ends with `purge()` of its three `ContributionFixture`s (new `ContributionFixture::purge`: fixture purge + user); fixture tenants `e2e-contribution-fixture-…` | 1/1 (0) | 0 → 0 |
| `crates/adapters/tests/distill_dispatch_v2.rs` | (b) dev | marker (T19's shapes and tenants live in a rolled-back transaction); `Drop` best-effort tenant DELETE → slots release + fixture purge | 25/25 (0) | 0 → 0 |
| `crates/adapters/tests/project_continuity_read_0137_acceptance.rs` | (b) dev | marker (the memory-link swap); its fixture is purged by the cleanup owner above; the DDL lock key now shared with it | 13/13 (0) | 0 → 0 |
| `crates/adapters/tests/reasoning_route_health_admission.rs` | (b) dev | 3 markers (each fault in a rolled-back case transaction); new `Purge` guard purges every seeded lane's tenant and user when the test ends, panic included; tenants `e2e-fixture r3-…` (they were never deleted before) | 4/4 `--include-ignored` (0) | 0 → 0 |
| `crates/adapters/tests/reasoning_route_runtime.rs` | (b) dev | marker (T5's insert is refused in a dropped transaction); `Drop` best-effort tenant DELETE → fixture purge; `private_route::seed_owner` tenants `e2e-fixture … throwaway tenant …` | 15/15 (0); `model_call_ledger` (same support) 12/12 (0) | 0 → 0 |
| `bins/admin/tests/probes.rs` | (c) `humaux_thread_c34_ap_*` | marker | 8/8 `--include-ignored` (0) | 0 → 0 |
| `bins/maintenance/tests/partitions.rs` | (c) `humaux_thread_c36_parts_*` | 6 markers | 24 passed, 1 ignored (D-N) (0) | 0 → 0 |
| `bins/maintenance/tests/retention.rs` | (c) `humaux_thread_c36_retention_*` | 3 markers | 15/15 (0) | 0 → 0 |
| `crates/adapters/tests/maintenance_doors.rs` | (c) `humaux_thread_c35_doors_*` | marker | 12/12 (0) | 0 → 0 |
| `crates/adapters/tests/contribution_policy_lifecycle_0132.rs` | (c) `humaux_thread_pre0132_*` (lane a:pre_0132) | 2 markers | post-0132 lane on dev 4/4 (0); `pre_0132_unresolved_triples_hard_stop` on a throwaway migrated through 0131, then dropped: 1/1 (0) | 0 → 0 |
| `crates/adapters/tests/contribution_self_principal_authority_0133.rs` | (c) `humaux_0133_cutover_*` (own, dropped) | marker | 6/6 `--include-ignored` (0) | 0 → 0 |
| `crates/adapters/tests/reasoning_route_shadow_bootstrap.rs` | (c) `humaux_thread_disposable_*` (lane a:disposable) | 7 markers | on a throwaway migrated to head, then dropped: 1/1 (0) | 0 → 0 |

- **Two purge constraints the conversion met** (recorded, not changed here): one purge per transaction (its `ON
  COMMIT DROP` temp tables: a second purge in the same transaction is `42P07`), so every caller runs each purge as its
  own statement; and the purge reads the catalog at run time, so a `tenant_id` table dropped concurrently renders as
  its OID in the generated text (`42601`): the 0137 `BarrierGuard` table did, which is why the 0137 cleanup holds the
  guards' DDL advisory lock. Hardening the purge itself (relation names from `pg_namespace`/`relname`, skipping a
  vanished relation) is a main-line follow-up.
- **Static gate** `replica_mode_is_declared` (`card36_extra_gates.env`, immediately before `dev_no_fk_orphans`): every
  `*.rs` under `bins crates xtask` naming `session_replication_role` must carry `fixture_purge` or one of the markers
  `// replica-mode: throwaway database only (…)`, `// replica-mode: fault setup, fixture purged at the end (…)`,
  `// replica-mode: fault setup, rolled back (…)`. Green on the final tree (15 files); red witness: the marker removed
  from `bins/admin/tests/probes.rs` ⇒ `undeclared replica mode: bins/admin/tests/probes.rs`, EXIT 1 (restored).
- `crates/adapters/tests/support/continuity_0137_cleanup.rs` keeps one `DELETE FROM ops.jobs` ahead of the purges
  (the purge reaches the same rows): gate `fixture_jobs_cleanup` pins that statement in the file.

## Chain stall, 2026-10-05

The first full chain on this tree did not go red where it failed: it stopped. Recorded here because three things in
the repository changed as a result, and because the cause was first misread as "the gate is slow".

**What happened (evidence: Docker VM kernel log, PostgreSQL log, gate log).** The Docker VM had 2 GiB of RAM and 1 GiB
of swap and was shared with another product line's test containers. Between 15:41:48 and 15:48:13 UTC the kernel ran
four global OOM kills (`constraint=CONSTRAINT_NONE`): another project's PostgreSQL backend, the dev Qdrant twice, and
at 15:48:13 a dev PostgreSQL backend running `ops.claim_derived_work_v2`. PostgreSQL terminated every backend and
recovered in 11 s. `distill_fairness_live` was in its M3 sampling loop; `bound_slots()` failed, the test panicked,
and the two resident `--distill-serve` subprocesses it had spawned were bare `std::process::Child` values: dropping
one does not stop the process. The orphans kept the test's inherited stdout open — the pipe the gate captures with
`O=$(cargo test …)` — so the gate never returned. No row was lost (dev at head, orphan scan 0 after recovery).

**Decisions.**

- **Subprocesses of the worker e2e files are kill-on-drop.** `humaux_testkit::reaped::{Reaped, SpawnReaped}` is the
  one construction point; `bins/private-worker/tests/derived_dispatch_e2e.rs` (six sites) and
  `bins/consolidation-worker/tests/derived_dispatch_e2e.rs` (one site, the same shape, found by the independent
  review) spawn through it. Gates: `c36_children_are_reaped` (no subprocess spawn of any spelling on a live line of
  either file, exact site counts), `c36_reaped_child_named` (the unwinding witness in testkit; red when `Drop` does
  not kill). The structural gates read live lines only: a commented-out line does not satisfy them.
- **Every gate runs under a supervisor** (`gate_ceiling.pl`, beside the chain runner): its own process group, stdin
  from `/dev/null`, a ceiling (5400 s; the lane 7200 s) that exits 142, signals forwarded, and members of the group
  that outlive the command signalled and counted on a `### GATE STRAGGLERS` line. Stdin matters independently of the
  stall: the runner pipes the gate list into `while read`, so a gate that read stdin would have swallowed every later
  gate without a trace. The serial lane, which collects each test group through pipes with no timeout, runs under
  the same supervisor.
- **A chain that spans a store restart or a crash recovery is void.** The runner records a store epoch at its start
  and `stores_not_restarted`, just before `gate_truth`, is red when it changed. For PostgreSQL the epoch is the
  postmaster start AND the checkpointer's `backend_start`: a backend killed by the kernel makes the postmaster
  re-initialise without restarting, so `pg_postmaster_start_time()` alone does not move (measured on a throwaway
  18.6 container; the first version of the gate compared only that and would have stayed green on this very
  incident — found by the second review round). For Qdrant it is the container's start instant and restart count.
  No gate restarts the dev containers (closed-port fixtures only).
- **`docs/ops/rehearse.sh` traps TERM and INT** (`exit 143`; not HUP, which a `nohup` run must keep ignoring): zsh runs no EXIT trap when an untrapped TERM kills it,
  which would skip the pidfile teardown and the `FAULT=revoke_claim` re-GRANT on the rehearsal database. Gate:
  `c36_rehearse_traps_term`.

**Measured.** One backend that opened all six partitioned parents holds 4.40 MB of memory contexts against 1.52 MB
fresh and 2.74 MB after one claim: partitioning costs at most about 1.7 MB per backend that touches every growth
table (86 relations: 6 parents, 24 leaves, their indexes), about 170 MB at `max_connections = 100`. That is a minor
term next to the VM's size; it is the number to carry into pool sizing (card 38).

**Not done here.** Four more test files hold a bare child across assertions
(`bins/private-worker/tests/ops_listener.rs`, `bins/retrieval-worker/tests/{ops_listener,serve_drain}.rs`,
`bins/consolidation-worker/tests/ops_listener.rs`). Their children run with stdout closed, so they cannot hold a
gate's pipe; a panic leaves a straggler the supervisor signals and counts. Routing them through `reaped`, and making
a non-zero straggler count red, is plan row 36c. The fixture tenants the stopped run left on dev (twelve
`e2e-fixture …` tenants, their teardown skipped by the recovery and by the stop) were purged with the fixture purge
after a dump; `xtask e2e-seed --teardown` refused ten of them because it also deletes membership users this fixture
shares across tenants — a second tenant-scoped teardown entry point is part of 36c as well.
