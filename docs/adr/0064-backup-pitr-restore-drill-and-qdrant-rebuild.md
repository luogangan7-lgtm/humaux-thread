# ADR-0064 — Backup + WAL/PITR on one local encrypted repository, restore drill, Qdrant rebuild from PostgreSQL as an in-place generation, embedding fingerprint + stored vectors

- Status: Accepted (design `card_37_design.md` with main-line rulings E1–E17; ruling E17: card 37 ships the go-live
  class `local_only` only, the NAS pull mirror is follow-up card 37d). Implemented in seven serial slices (S1–S7);
  complete as of S7 (2026-10-06). Reviewed independently by two read-only reviewers (data loss / false claims / disk
  fill / privilege / drill isolation; operability / measurement / scale fit / maintainability) and fault-injected in
  two halves; the findings, the uncaught faults and their fixes are the section "Review and verifier pass" below.
  The main line migrated dev (E7, after a dated `pg_dump -Fc`), ran the serial lane and the full chain, closed the two
  chain reds (section "Chain run 1") and committed.
  - S1 (2026-10-06, uncommitted): migrations 0232 (fingerprints, stored vectors, registry vector identity), 0233
    (rebuild generations: side tables, two `stream_log` triggers, three definers) and 0234 (DR receipts); the pure
    fingerprint module `humaux_projection::embedding_fingerprint`; the rls-check matrix cells and the rebuild
    boundary arm; Baseline §6.2.2 columns. No runtime behaviour changes. Dev itself is not migrated (ruling E7: the
    main line applies 0232–0234 after a dated `pg_dump -Fc`).
  - S2 (2026-10-06, uncommitted): the retrieval worker stores the vector it embedded in the registry transaction,
    reuses a stored vector before sealing, purges the bytes with the last live binding, and binds its label to its
    fingerprint before the first pass. From S2 on the worker reads `projection.embedding_fingerprints` and
    `projection.memory_vectors` on every ticket, so a worker (or a test suite) pointed at a database without
    0232 fails; dev must be migrated (E7) before the next chain or resident run.
  - S3 (2026-10-06, uncommitted): `adapters::rebuild` (precheck, open / issue / wait / verify / close, the E1–E5
    verifier with the quiescent orphan step, `NoProviderEmbedder` and the closed `drill_projection_deps`), the
    verifier scroll `qdrant::scroll_stream_points`, the run-scoped claim `stream_repo::claim_run_tickets` behind
    `projection_worker::run_claimed_pass_for_run`, the one RYW overlay predicate in `retrieve.rs` (E13), and the
    operator arms `humaux-maintenance projection rebuild | verify`. No migration.
  - S4 (2026-10-06, uncommitted): the backup bundle (`deploy/images/postgres/Dockerfile` stage `pg`,
    `deploy/pgbackrest/{pgbackrest.conf, humaux-dr.sh, humaux-backup.crontab}`, `deploy/compose/backup.yml`, and
    `deploy/compose/drill.yml` for the compose gates), the arms `humaux-maintenance backup check | run | verify |
    status` (`bins/maintenance/src/backup.rs`, receipts through `adapters::maintenance_repo`), `ExternalDep::Docker`,
    the twelve kept spikes and the backup suite. No migration; no metric (the gauges are S6's).
  - S5 (2026-10-06, uncommitted): `humaux-maintenance restore drill | restore drill --destroy-stale | restore pitr`
    (`bins/maintenance/src/drill.rs`, `deploy/compose/drill.yml`), the drill's SQL in `adapters::maintenance_repo`,
    `adapters::rebuild::drill_closed_deps`, the drill suite and the four ignored live legs. No migration.
  - S6 (2026-10-06, uncommitted): the DR_EVIDENCE task of `humaux-maintenance --serve` and the six DR families
    (`telemetry::dr`), the alert rows BackupFailure (`{target="local"}`), BackupNotOffsite, RestoreDrillFailure,
    WalArchiveFailing, BackupBudgetLow and DiskFreeLow with 11 promtool cases and five mutations (red=32/32), the
    weekly route for the standing notice, and the empty metrics-registry allowlist. No migration.
  - S7 (2026-10-06, uncommitted): the measurements (`bins/maintenance/tests/measure.rs`, M1 ×5, M2 ×4, M3 ×2, M5 ×1),
    runbook §11 "Disaster recovery", supervision.md, delivery report §6.25, Baseline §1.9 / §16.2 / §44, this record's
    rulings, scope, findings, tests, limits, E16 table, rejected lists and follow-ups, `card37_extra_gates.env` and
    `c37_rehearse.sh`. No code path changes.

## S1 — schema as built

Migration numbers follow design 10.3 shifted by +2 (card 36 ended at 0231): fingerprints and vectors = 0232,
rebuild generations = 0233, DR receipts = 0234.

- **D-C** `projection.embedding_fingerprints`: one label = one fingerprint (`UNIQUE (embedding_version)`); the
  fingerprint is sha256 over the length-prefixed fields of `embedding_fingerprint::FINGERPRINT_DOMAIN`; dtype
  `float32` and distance `Cosine` are both Rust constants and table CHECKs (reconciled by T-C1).
- **D-B** `projection.memory_vectors` (FORCE RLS on the tenant GUC) and the registry's `fingerprint_sha256` /
  `input_sha256` with a MATCH SIMPLE FK to it: the database refuses a fingerprinted registry row without its
  vector row; legacy rows (both NULL) are exempt until D-G's consented re-embed.
- **D-A / D-E** `projection.rebuild_runs`, `projection.rebuild_tickets` (UNIQUE on stream key + generation +
  commit_seq), `projection.rebuild_open`, `projection.issue_rebuild_tickets`, `projection.rebuild_close`; the
  completion terms `generation_in_flight`, `boundary_moved`, `catch_up_in_flight` are refusals (55000) inside
  `rebuild_close`, not Rust checks.
- **Deviation from D-E's trigger text (S1):** `stream_log_tombstone_follows_to_generation` is SECURITY INVOKER, not
  DEFINER. The 0167 state guard (`stream_log_guard_state_transition`) lets only `role_maintenance` write
  `* -> TOMBSTONED` and gives the owner one edge (`FAILED -> RETIRED_FAILED`), so an owner-run follow UPDATE would
  be refused for every forget. As invoker the follow runs as the role that tombstoned (`forget_repo::tombstone`,
  role_maintenance, which holds SELECT on `ops.outbox` and `rebuild_tickets` and UPDATE on `stream_log`).
  `stream_log_generation_never_lost` stays DEFINER (it only reads). rls-check pins both modes.
- **D-J / D-K / D-O** (10.3 as corrected by 10.11 C): `ops.backup_sets`, `ops.backup_receipts` (no `target` column;
  `backup_receipts_verified_derived` + `backup_receipts_shape`, whose VERIFIED arm also requires `verify_exit IS
  NOT NULL` — otherwise the derived CHECK reads `TRUE = NULL` for a NULL exit and passes (review pass, 2026-10-06);
  nullable label / manifest / start / stop for budget refusals; the budget columns), `ops.restore_witnesses`, `ops.wal_archive_failures`, and `ops.restore_drills`'s
  receipt columns with `repo_intact` inside `restore_drills_succeeded_derived`. Nothing in the schema can name an
  offsite copy (E17).

## Enumerated grants (E9, as built in S1)

| object | grants (every other non-owner cell is `—`) |
|---|---|
| `projection.embedding_fingerprints` | role_retrieval_worker SELECT, INSERT; role_maintenance SELECT |
| `projection.memory_vectors` | role_retrieval_worker SELECT, INSERT, UPDATE (vector, purged_at); role_maintenance SELECT |
| `projection.private_memory_points` | unchanged 0011 domain defaults (role_retrieval_worker's table-level UPDATE covers the two new columns, so design §4's column grant is not added) |
| `projection.rebuild_runs` | role_maintenance SELECT |
| `projection.rebuild_tickets` | role_maintenance, role_retrieval_worker, role_gateway SELECT |
| `ops.backup_sets`, `ops.backup_receipts`, `ops.restore_witnesses`, `ops.wal_archive_failures` | role_maintenance SELECT, INSERT |
| `ops.restore_drills` | role_maintenance SELECT, INSERT; gateway, private, consolidation, public, retrieval workers SELECT (INSERT/UPDATE revoked) |
| the three rebuild definers | EXECUTE role_maintenance only |
| the two trigger functions | no executor |

## S1 measurements (migration lock windows)

On a throwaway migrated to 0231 and seeded with 16,703 registry rows (the dev registry size): 0232 11.2 ms, 0233
18.4 ms, 0234 19.5 ms (2026-10-06, one run each).

## S2 — worker as built

- **D-B write site.** `private_projection_registry::register_private_memory_point_with_vector(pool, auth,
  registration, Option<&StoredVector>)`: in one transaction, the vector row upsert (`ON CONFLICT (pk) DO UPDATE SET
  vector = EXCLUDED.vector, purged_at = NULL WHERE memory_vectors.vector IS NULL`), then the registry row with
  `fingerprint_sha256` / `input_sha256`; on `AlreadyRegistered` / `Revived` a legacy row (both NULL) takes the key
  (backfill-on-touch). A NaN / infinite / empty vector is `InvalidInput` (PostgreSQL has no cheap all-finite CHECK).
  The three-argument `register_private_memory_point` stays and registers without a vector (a legacy row).
- **D-B read site (stored-first).** `resolve_and_embed` keeps its read transaction open through `build_card`, reads
  the label's fingerprint and the non-purged vectors of the kept memories under it, and commits; when every kept
  card has a vector with `input_sha256 = sha256(card_text)` (the text `seal_card` seals, unchanged) and the
  configured width, the row neither seals nor calls the provider. Otherwise the unchanged seal + one batched
  `embed_cards` runs for the whole Evidence.
- **D-D purge.** One statement (`PURGE_UNREFERENCED_VECTORS`) runs in the liveness transaction of both
  `retire_points_for_memory` and `retire_private_memory_point` (the latter now returns the retired row's
  `memory_id`).
- **D-C binding.** `bind_embedding_fingerprint(pool, label, inputs)` → `INSERT … ON CONFLICT DO NOTHING`, then the
  label's row is read back and compared by fingerprint; a mismatch (or this fingerprint under another label) is
  `PrivateProjectionRegistryError::FingerprintMismatch` (a variant of the existing registry error, not a new enum).
  `worker_fingerprint_inputs` fixes the task type (`document`), the preprocessing version (`CARD_TEMPLATE_HASH`)
  and the contract version (the claim family's projection version). `humaux-retrieval-worker --serve / --run-once`
  binds after its pool connects and before the first pass; a mismatch or an empty input exits 2 with `boot
  refused: fingerprint_mismatch: HUMAUX_RETRIEVAL_WORKER_EMBEDDING_VERSION is bound to another model fingerprint
  (ADR-0064 D-C)`; a database error stays a logged failed pass in `--serve`.
- **D-F E4 builder.** `projection_worker::point_payload` is the one payload builder (`finish_row` calls it);
  `ResolvedMemory` and `resolve_memories` are `pub(crate)` for S3's verifier.
- **Deviation (S2): no `fingerprint_sha256` field on the worker deps, no `vector` field on
  `PrivateMemoryPointRegistration`.** Both structs are built by struct literal in suites that must stay unchanged
  (`projection_worker`, `private_projection_registry`, `projection_claim`, the a2 fixture, `private-worker`'s
  `distill_hop_e2e`); a new field would not compile there. The worker reads its fingerprint by label inside the
  ticket's read transaction instead — the same value, since the label is bound 1:1 (UNIQUE + PK) and the boot
  binding proved it equals the configured model's — and passes the vector beside the registration
  (`StoredVector`). An unbound label (only reachable in tests that never boot the binary) stores no vector, as a
  legacy row.

## S3 — rebuild and verify as built

- **D-E steps.** `rebuild::rebuild_stream(deps, stream, opts)` = `precheck` (D-G) → `open_run`
  (`projection.rebuild_open`) → `provisioning::ensure_collection` → `issue_tickets` (`issue_rebuild_tickets` in
  batches of `--batch` until 0) → `wait_generation` (the caller's pump: a 1 s poll inside `--wait-seconds` while the
  resident worker projects; an in-process pass in the drill and the tests) → `verify_run` → `close_run`
  (`projection.rebuild_close`). A generation still in flight after the wait leaves the run open
  (`cannot_establish`); the next call resumes it. Every write is one of the three definers as role_maintenance under
  the tenant GUC. Streams = the serving `stream_checkpoints` rows of the tenant (optionally one workspace) per
  `TicketFamily::ALL` member, joined to the tenant's placement collection; `--all` takes every tenant id from
  card 35's tenant page definer.
- **D-F verifier.** One READ ONLY snapshot (H2, any ticket ≤ H2 in flight, the registry R with its vector rows, the
  inputs classified as D-E step 4 classifies them, T by read_materialize's hydrate-gate predicate, and X =
  `projection_worker::expected_points` per kept input: `resolve_memories` → `card_input` → `build_card` → the
  deterministic point id → `point_payload`, the one builder), then one scroll of the stream (`scroll_stream_points`
  with the typed `StreamCountFilter`, payload and vector) and `count_exact` of the same filter. E1 =
  `collection_generation` (dimension from the fingerprint row) and status `green`; E2 = every generation ticket's
  terminal `(state, class)` is one PG implies (DONE, SKIPPED_BY_POLICY, the three deterministic classes,
  `distill_failed` only with a FAILED EVIDENCE_ACCEPTED row, `no_visible_memory_record` only with that row or the
  Evidence gone, and TOMBSTONED from a forget during the run), anything else is
  `rebuild_tickets_failed:<class>=<n>`; E3 = `Q\T = R\T`, `R\T ⊆ X`, `X\R ⊆` memories of inputs whose latest
  ticket ended in an outcome exclusion; E4 = per-id leaves `sha256(id ‖ sha256(canonical payload))` without
  `source_stream_seq`, root over R\T in id order (the PG-side root is stored); E5 = registry fingerprint = the
  worker's, vector row present, `max |normalize_f32(stored) − qdrant| ≤ VECTOR_TOLERANCE = 1e-6`. Any ticket of the
  stream at or below H2 in flight is `cannot_establish: generation_in_flight`; a moved `issued_highwater` between the
  two PG reads is `cannot_establish: boundary_moved`. The report names the first 10 differing ids per term.
- **Orphan step (rebuild mode).** After the scroll, ONE statement reads `R_after` and "any ticket ≤ H2 in flight";
  in flight → nothing deleted, `cannot_establish`; otherwise the scrolled label points absent from `R_after` are
  deleted fenced at H2, and the stream is verified once more.
- **D-G precheck.** Live registry rows of the stream under the worker label: another fingerprint → refused
  `re_embed_required` with `points_other_fingerprint` (exit 3, nothing opened, issued or called); NULL fingerprint
  (legacy) without `--allow-reembed n ≥ legacy` → refused with `points_without_vector` and the exact command. An
  unbound label → refused `label_unbound:<label>`.
- **D-M closed deps.** `drill_projection_deps(ClosedDeps) -> (SharedProjectionDeps, Arc<NoProviderEmbedder>)`:
  `ClosedDeps` has no embedder field; `NoProviderEmbedder` counts each call and refuses `DependencyUnavailable`.
- **D-N(g) scoped claim.** `run_claimed_pass_for_run(shared, cfg, OnlyRun { tenant_id, run_id })` claims through
  `stream_repo::claim_run_tickets`: the 0176 claim's lease / attempt increment / per-family exclusivity /
  distill-closed predicate / placement join, narrowed by a join on `rebuild_tickets.run_id`, as role_retrieval_worker
  under the run's tenant GUC (its table-level UPDATE and its `rebuild_tickets` SELECT). The resident worker's
  `run_claimed_pass` is unchanged.
- **E13 overlay predicate.** `retrieve::pg_delta_overlay_in_txn` gains exactly `AND NOT EXISTS (SELECT 1 FROM
  projection.rebuild_tickets rt WHERE <rt stream key> = <sl stream key> AND rt.stream_seq = sl.stream_seq)`.
- **ADR-0057 D-C addendum.** A `StreamCountFilter` reaches the count and the rebuild verifier's scroll, never search.

### S3 deviations (each with its reason)

1. **The verifier reads as role_retrieval_worker.** role_maintenance's SELECT on `private.memory_records` is
   narrowed by the 0012 visibility and 0155 subject policies, so X and the E4 payloads (USER_PRIVATE /
   WORKSPACE_SHARED memories) cannot be built from its view; only the projector's 0140 arm sees every row. The
   reads take a `RetrievalWorkerDbPool`, the definers the `MaintenanceDbPool`. The operator CLI therefore reads
   `HUMAUX_RETRIEVAL_WORKER_PG_DSN` (and the label) under the worker's own names. This exceeds E9's "a
   RetrievalWorkerDbPool only against a drill cluster" — **open point for the main line**: accept the peer DSN in the
   operator shell, or add an owner read definer for role_maintenance in a later migration.
2. **`only_run` is a function, not a `PassConfig.claim` field.** `ClaimFamily` and `PassConfig` are built by struct
   literal in `projection_claim.rs` and `bins/retrieval-worker`; a new field would edit both. The scoped claim is a
   Rust statement, not a definer change (0176 is applied and untouchable).
3. **`reembed_allowed` is recorded in the run report**, not the column: 0233's `rebuild_open` takes no consent
   parameter and role_maintenance has SELECT only on `rebuild_runs`; the column stays 0. The report carries
   `reembed_allowed` and `re_embed`.
4. **verify's read-only proof** is structural (READ ONLY transactions, no Qdrant write call on its path) plus T-F3's
   table digests and exact counts, instead of `pg_stat_xact` deltas (cumulative statistics flush asynchronously).
5. **`report.excluded`** counts inputs the definer left out (no generation ticket in the run); a forget after issue
   is the ticket's outcome (`excluded_by_outcome.tombstoned`).
6. **Files outside the S3 list:** `provisioning.rs` (`QdrantFace::wire`, so the CLI holds one Qdrant face),
   `projection/src/dense.rs` (its invariant now names the verifier scroll), `tests/support/a2_fixture.rs`
   (`in_throwaway_at` for the scratch Qdrant port; cleanup of the card-37 tables), new
   `tests/support/scratch_qdrant.rs` (the `humaux-c37-qdrant-<pid>-<n>` guard, shared with the CLI test).

## S4 — backup bundle and arms as built

### Spike measurements (design 10.6; `cargo test -p humaux-maintenance --test spike -- --ignored --nocapture`, pinned image, 2026-10-06)

MEASURE sp1 verify_exit_on_corrupt_file=0 exit_code n=1 pgbackrest=2.59.3 (text output reports status: error=true; checksum invalid: 1=true; bytes_read=whole set=7153152 B)
MEASURE sp2 archive_push_same_name_same_content_exit=0 exit_code n=1 pgbackrest=2.59.3 (other content exit=45)
MEASURE sp3 bundle_files_in_one_full=2 files n=1 pgbackrest=2.59.3 (manifest plaintext on disk=false; repo-get backup/humaux/<label>/backup.manifest first line=[backrest])
MEASURE sp4 pgbackrest_package=2.59.3-1.pgdg13+1 version n=1 pgbackrest=2.59.3 (check exit=0; conf options reported invalid=0; zst files in the set's pg_data=1; 2.59.3-1.pgdg13+1 installed from trixie-pgdg, so O2 pins it)
MEASURE sp5 restore_from_read_only_bind_rows=7626 rows n=1 pgbackrest=2.59.3 (source rows=7626; archive-get completions=4; write probe=rc=1)
MEASURE sp9 repo_bytes_per_forced_switch_segment=692 bytes n=10 pgbackrest=2.59.3 (segments archived=10; 24 switches/day at archive_timeout=3600 s ≈ 16608 bytes/day)
MEASURE sp10 pg_wal_max_bytes=134217728 bytes n=1 pgbackrest=2.59.3 (queue max 64MiB, max_wal_size 64MB, 16 rounds of ~12 MiB; dropped segments=11 (pgBackRest acknowledges a dropped segment, so PostgreSQL counts it archived); postmaster unchanged=true)
MEASURE sp11 info_backup_info_repository_size=7153552 bytes n=1 pgbackrest=2.59.3 (field backup[].info.repository.size; set directory du -sb=7572848; field repo[].cipher="aes-256-cbc")
MEASURE sp12 pg_stat_archiver_select_as_role_maintenance=ok result n=1 pgbackrest=n/a (current_user=role_maintenance; server_version=18.6 (Debian 18.6-1.pgdg13+2); archived_count=0)
MEASURE sp13 postgres_uid_gid=999:999 uid:gid n=1 pgbackrest=2.59.3 (docker run --rm <pinned image> id postgres)
MEASURE sp14 posix_repo_on_host_bind_stanza_backup_verify=ok result n=1 pgbackrest=2.59.3 (repo/ owner:group mode inside pg=postgres:postgres 750; host dir under $TMPDIR, no external volume needed)
MEASURE sp16 amtool_routes_test_exit=0 exit_code n=1 pgbackrest=n/a (placeholders accepted as-is, no temp copy needed; receiver today=humaux-log-sink)

What each measurement decided:
- **SP-1** `verify --set` exits **0** on a set with an invalid file and reports it only in its text output
  (`status: error`, `checksum invalid: 1`). The arms therefore run `verify --set=<label> --output=text --verbose` and
  pass a set only on exit 0 **and** a `status: ok` line **and** `backup: <label>, status: valid`; otherwise the receipt
  is FAILED `verify_invalid` (or `verify_exit:<n>`). T-J3's fault reads "treat the reported invalid status as a
  warning". verify reads the whole set (bytes read = the set's repository size).
- **SP-2** same name + same content → 0 (a WARN), same name + other content → 45 (`ERROR [045]`): the T-H2 contract.
- **SP-3** a full is two bundle files plus large files; `backup.manifest` is ciphertext on disk and `repo-get
  backup/humaux/<label>/backup.manifest` decrypts it inside `pg`; the arms hash it there (only the digest leaves).
- **SP-4** `pgbackrest=2.59.3-1.pgdg13+1` is in trixie-pgdg, so **O2 resolves to the 2.59.3 pin** (the weak-subkey fix
  is in the build that creates the go-live stanza); `backup check` still prints `weak_subkeys=<n>` as the
  precondition. Every conf option is accepted (no `invalid option`), zst is used.
- **SP-5** restore and `archive-get` from a **read-only** bind work and a write probe gets EROFS: the drill restores
  straight from the ro bind; the `cp -a` fallback of D-M is not needed (S5).
- **SP-9** ≈ 690 B of repository per forced-switch segment (zst + aes-256-cbc): at `archive_timeout`=3600 s idle WAL
  costs ≈ 17 kB/day; D-V's 384 MiB/day was the uncompressed bound. Real W is M3 (S7).
- **SP-10** with `archive-push-queue-max=64MiB`, `max_wal_size=64MB` and an unwritable archive, pgBackRest drops
  segments (`dropped WAL file … because archive queue exceeded`), PostgreSQL counts them archived, the postmaster never
  restarts, and `pg_wal` peaks at 128–144 MiB over two runs. The pg_wal bound is therefore **queue max + max_wal_size + 3 × 16 MiB**
  (T-J13 uses 176 MiB); the design's "64 MiB + 3 × 16 MiB" omitted `max_wal_size`. Production: ≤ 2 GiB + max_wal_size.
- **SP-11** set size = `backup[].info.repository.size` (the D-V estimate basis, receipt `set_repo_bytes`); cipher =
  `repo[].cipher` (`backup check` refuses `repo_not_encrypted` on anything but `aes-256-cbc`).
- **SP-12** `pg_stat_archiver` is readable by role_maintenance (PUBLIC): the S6 latch needs no grant.
- **SP-13** `postgres` is uid:gid **999:999** in the pinned image (runbook go-live step 1).
- **SP-14** a posix repository on a Docker Desktop host bind works (stanza-create, backup, verify; `repo/` shows as
  postgres:postgres 0750 inside `pg`): tests use a per-test host directory, no `external: true` volume.
- **SP-16** the pinned amtool 0.34.1 accepts `alertmanager.yml` with its `url_file` placeholders as-is (exit 0):
  `c37_not_offsite_route` needs no temp copy.

### S4 decisions and deviations (each with its reason)

1. **FS identity stats the mount point.** `check` and `run` compare `stat -c %d /var/lib/pgbackrest` (the bind target)
   with `pg1-path`, not `…/repo`: same device, and it also works before `stanza-create` and when the repository mount
   is a volume holding no `repo/` yet (T-J14).
2. **`run` order as 10.11 A**; a refusal (`repo_shares_pgdata_fs`, `budget_repo_max:<repo+estimate>`,
   `budget_free_floor:<free−estimate>`) writes a FAILED receipt with a NULL label (the budget refusals with
   `repo_bytes`, `repo_free_bytes`, `repo_max_bytes`, `min_free_bytes`, `estimate_bytes`) and exits 3. A failed
   `expire` after a VERIFIED set keeps the receipt VERIFIED and exits 1 naming `expire_exit:<n>`.
3. **VERIFIED is a claim the table judges.** The arm writes `outcome = VERIFIED` only when every verify step passed;
   `backup_receipts_verified_derived` / `_shape` / the `backup_sets` FK refuse anything else (T-J2's fault surfaces
   as the table's 23514).
4. **`backup verify` writes a receipt without budget numbers**, so D-K's "newest receipt `WHERE repo_bytes IS NOT
   NULL`" is always a `run` (or refusal) row.
5. **The receipt SQL lives in `adapters::maintenance_repo`** (appended, E5): `insert_backup_receipt`,
   `first_verified_manifest`, `bind_backup_set`, `newest_verified_set_repo_bytes`, `backup_status_facts`; the
   binary holds no SQL.
6. **Secrets by name in compose.** `backup.yml` / `drill.yml` pass `POSTGRES_PASSWORD` (first initdb only) and
   `PGBACKREST_REPO1_CIPHER_PASS` as bare `environment` names; no value, no `:?` key. `listen_addresses` uses
   `${HUMAUX_PG_LISTEN_ADDRESSES?}` (set, may be empty: socket only for D-U).
7. **`docker compose exec` needs no `HUMAUX_PG_*` interpolation** (measured), so `dr.env` carries only the names of
   10.11 J for the arms; `humaux-dr.sh compose …` (`up`) needs the compose keys in `dr.env` too. `humaux-dr.sh`
   resolves `humaux-maintenance` and `docker` through `PATH`, which `dr.env` may set (cron's `PATH` is minimal).
8. **`deploy/compose/drill.yml` is written in S4** because `c37_compose_valid` and `c37_repo_is_bind_mount` parse it;
   its behaviour (and its tests) are S5's.
9. **T-J13 in S4 observes `pg_stat_archiver` directly** (the failure state on the scratch cluster); the DR_EVIDENCE
   pass and its latch assertions ("1, still 1 after the drops") are added in S6 with the task. The filler leaves
   4 MiB free (≤ 16 MiB) because zst halves the md5-text segments; the bound is SP-10's.
10. **Gate-text fixes** in `card37_extra_gates.env` (the design's text could not pass on any input):
    `HUMAUX_PG_CONTAINER=x` → `x1` in `c37_compose_valid` / `c37_repo_is_bind_mount` (compose refuses a one-character
    `container_name`), and `[^$[:space:]]` → `[^[:space:]$]` in `c37_templates_no_secret` (the chain's `/bin/sh` is
    bash, which expands `$[` as arithmetic).

### S4 tests and the fault that turned each red (mutated by writing content + touch, restored, re-run green)

| test (`bins/maintenance/tests/backup.rs`) | fault | failing line |
|---|---|---|
| T-H6 `the_stanza_const_matches_the_conf` | conf section `[humaux]` → `[humaux2]` | `the conf has a [humaux] section` |
| T-I1 `the_dr_wrapper_refuses_a_group_readable_env_file` | mode check → `if false` | `0640 is refused` (left 0, right 3) |
| T-I1 | the `command -v` PATH check deleted | `cron PATH: … exec: humaux-maintenance: not found` (left 127, right 3) |
| T-H1 `the_image_is_pinned_and_carries_pgbackrest` | `"pgbackrest=${PGBACKREST_VERSION}"` → `pgbackrest` | `the install line pins the package to the ARG` |
| T-H2 `archive_push_refuses_a_same_name_segment_with_other_content` | `archive-async=y` | `same name, same content` (non-zero) |
| T-J2 `backup_run_records_a_verified_local_receipt` | VERIFIED claimed without `verify --set` | exit 1: the table refused the claim |
| T-J3 `verify_fails_on_a_corrupted_repo_object_and_records_failed` | the reported invalid status ignored | exit 0, outcome VERIFIED |
| T-J3b `backup::tests::verify_needs_exit_zero_and_a_valid_report` (bin unit test) | `if code != 0` → `if false` (T-J3's corrupt file exits 0 per SP-1, so T-J3 cannot reach this branch) | the first `assert_eq!`: left None, right `verify_exit:1` |
| T-J4 `backup_status_is_read_only` | status writes a receipt | `status wrote nothing` |
| T-J5 `a_manifest_overwritten_after_verification_fails_the_next_verify` | identity compare dropped | failure ≠ `manifest_changed` |
| T-J7 `a_failed_verification_expires_nothing` | expire unconditional | `a failed verification expired a set` |
| T-J8 `a_backup_that_would_break_the_budget_is_refused_before_writing` | estimate × 0 | exit ≠ 3 (`estimate_bytes: 0`, the backup ran into the tmpfs) |
| T-J13 `a_full_repo_filesystem_stops_archiving_not_postgres` | the 64 MiB queue-max override dropped | `pg_wal bounded: 285212672 > 184549376` |
| T-J14 `backup_refuses_a_repo_on_the_pgdata_filesystem` | `st_dev` comparison dropped | exit ≠ 1 / ≠ 3 |
| T-J9 `expire_keeps_exactly_the_two_newest_verified_fulls` | `repo1-retention-full=3` | `exactly the two newest` |
| T-J12 `check_counts_weak_subkeys_without_printing_them` | `repo-get backup.info` printed | `a 64-character base64 run (a subkey?) in the output` |

Gate `c37_templates_no_secret` was shown red by a `PGBACKREST_REPO1_CIPHER_PASS=abc123` line appended to the crontab.

## S5 — restore drill and real restore as built

`humaux-maintenance restore drill --evidence <file> [--drill-id <uuid>]`, `restore drill --destroy-stale` and
`restore pitr --compose-file <f> --project <p> (--target end | --target-time <rfc3339>) --evidence <file>` live in
`bins/maintenance/src/drill.rs` (design 10.4 S5 as corrected by 10.11 B, H, I). The drill's SQL is appended to
`adapters::maintenance_repo` (witnesses, newest VERIFIED set, cluster and per-tenant facts, isolation probes, the
receipt); `adapters::rebuild` gains `drill_closed_deps` (transport and permits that reach only the drill's own Qdrant
on 127.0.0.1, the pinned scanner) and `collection_names` (the fresh-Qdrant check).

- **Step 0, in this order, all before any project exists (exit 3):** `migrator_dsn_present`; `no_verified_backup`
  (newest set whose LATEST receipt is VERIFIED, joined to its `ops.backup_sets` identity); `stale_drill: <kind name>,
  …; run humaux-maintenance restore drill --destroy-stale` on ANY `humaux.drill` resource; then one labelled
  one-shot of the pg image with the repository mounted exactly as `docker compose config` of the drill file declares
  it: the EROFS write probe (`drill_repo_writable` unless the probe reads `Read-only file system`), `MemAvailable`
  (`drill_free_memory:<need>/<have>`) and `df -Pk /` of the Docker disk
  (`drill_free_bytes:<need>/<have> (restore=<a> vectors=<b> repo_copy=0 floor=<d>)`, restore = `info`'s
  `backup[].info.size`, vectors = live registry rows × (4 × dimension + 2,048)). The probe runs before the floors
  because the floors need `info` and the probe needs nothing.
- **Steps 2–9** follow D-L/D-M/D-N/D-O: witnesses A, T = `clock_timestamp()`, B through the source's role_maintenance;
  `pgbackrest check` in production `pg` (its duration is `archive_wait_seconds`); `up -d qdrant`; restore
  `--set=<label> --type=time --target=<T> --target-action=promote --archive-mode=off` by `compose run`; socket-only
  boot and promotion wait (a `pg` that exits is `restore_failed: pg exited: <log tail>`); passwords of postgres,
  role_maintenance, role_retrieval_worker, role_gateway replaced by in-memory random values over stdin, every other
  LOGIN role NOLOGIN; TCP boot; checks (b) first on the pool every later step uses; the rebuild of every serving
  stream of every restored tenant with `require_stored_vector` and a pump that claims only the run's tickets
  (`run_claimed_pass_for_run`); destroy; `repo_intact` digest after destroy; residue; receipt; evidence (0600).
- **Destroy** is a label sweep: containers (`rm -f -v`), then volumes, then networks whose
  `com.docker.compose.project` is exactly `humaux-drill-<id>` or whose `humaux.drill` is exactly `<id>`; never a name
  prefix, never prune. A drop guard runs it on every path after `up`.
- **`restore pitr`** refusals in 10.11 B order: `pgdata_not_empty:<volume>`, `migrator_dsn_present`,
  `production_pg_running:<container>` (any running container of `HUMAUX_MAINTENANCE_BACKUP_PROJECT`),
  `restore_free_bytes:<need>/<have> (restore=<a> floor=<d>)`, `repo_set_unverifiable:<label> (<why>)`. Then restore
  (`--type=default` for `end`), socket-only boot, NOLOGIN for every LOGIN role but postgres, role_maintenance and
  role_retrieval_worker, the in-flight counts, the boot with the operator's `HUMAUX_PG_LISTEN_ADDRESSES`.

### S5 decisions and deviations (each with its reason)

1. **`ops.schema_migrations` is read as the image superuser over each container's socket** (drill `pg` and
   production `pg`, `psql -d <db>`, read-only): 0201 D-C made the table owner-only, so role_maintenance gets
   `permission denied` (measured on the first shared run). No grant was added (it would undo SEC-2).
2. **Stream quiescence in drill mode (rebuild.rs, a defect of S3 found by T-L1):** a restored non-generation ISSUED
   ticket (D-Q's "one ticket unprojected") made every drill stream `cannot_establish`. With `require_stored_vector`
   only the run's generation tickets count as in flight; the restored ticket is frozen quarantine state, its input
   excluded `without_stored_vector`. Red: reverting the predicate fails T-L1 with `checks_failed:rebuild_equivalent`.
3. **The drill's scanner** comes from the retrieval worker's `HUMAUX_RETRIEVAL_WORKER_GITLEAKS_{BIN,VERSION,SHA256}`
   (read under the worker's names, the card-35 peer-key precedent): `SharedProjectionDeps` holds a scanner; the
   stored-vector path never calls it. dr.env therefore also carries these three non-secret names, and the drill and
   `restore pitr` read `HUMAUX_MAINTENANCE_BACKUP_{COMPOSE_FILE,PROJECT}` (production `check` / `info`, the
   production-running refusal). `restore pitr` reuses `HUMAUX_MAINTENANCE_DRILL_RESTORE_TIMEOUT_SECONDS` as its
   promotion bound and takes the database name from `HUMAUX_MAINTENANCE_PG_DSN`. S7 lists them in runbook §11.
4. **`restore pitr` picks its set from `info`** (the newest that stopped at or before the target): production — the
   receipts database — is down by rule, so no receipt can be read; the set is verified first anyway.
5. **In-process pass knobs are fixed consts** (batch 50, lease 60 s, 3 attempts, 1–2 s backoff), marked
   `ponytail:` — the run-scoped pass has no competitor; the generation wait is the restore timeout.
6. **`--archive-mode=off` cannot be shown red by T-L1:** `backup.yml` sets `archive_mode` on the command line, not in
   PGDATA, so a restored cluster boots with `archive_mode=off` either way and `drill_archiver_attempts = 0` holds
   with or without the flag. The flag stays as the defence for a PGDATA that carries archive settings.
7. **T-L1 step 5 (one `DR_EVIDENCE` pass) moves to S6** with the task; T-L2's "no gauge advances" asserts the
   gauge's source instead: no `ops.restore_drills` row with `succeeded = true` exists.
8. **T-W6 folded into T-L1's teardown** compares the `backup/` tree listing, `backup.info` bytes, and that no
   archived file disappeared: a clean shutdown by `down -v` archives one more segment, so `archive/` may only grow.
9. **Test layout:** T-M2, T-M7, T-O2, T-O3 read one shared drill run (one migrated scratch source, one VERIFIED
   backup, a floor refusal, then a full drill that fails `isolation_not_applicable` but runs every step), so the
   Docker suite takes ~25 s. T-M1', T-M3, T-M4, T-M6 need no source (a throwaway receipts database with a forged
   VERIFIED row where step 0 needs one). The live tests seed through the adapters' a2 fixture by `#[path]`, which
   adds seven dev-dependencies to `bins/maintenance/Cargo.toml`.

### S5 measured (T-L1 on the scratch source, dev image, 2026-10-06; n = 1, S7 repeats with n = 3)

`restore_s` 6.27, `checks_s` 0.43, `rebuild_s` 1.31 (6 points, 0 provider calls), `destroy_s` 1.07,
`rto_seconds` 9.33; `restored_in_flight` {issued_g1 2, open_jobs 13, reserved_calls 0}; residue 0, repo_intact true.

### S5 tests and the fault that turned each red (mutated by writing content + touch, restored, re-run green)

| test (`bins/maintenance/tests/drill.rs`) | fault | failing line |
|---|---|---|
| T-M4 `drill_refuses_with_the_migrator_dsn_in_env` | check → `if false &&` | exit 1 `connect HUMAUX_MAINTENANCE_PG_DSN: … pool timed out` (≠ 3) |
| T-M4b `drill_and_pitr_refuse_an_evidence_path_whose_directory_is_missing` | `dir.is_dir()` → `true` | exit 1 `connect HUMAUX_MAINTENANCE_PG_DSN: … pool timed out` (left 1, right 2) |
| T-M3 `drill_refuses_without_a_verified_backup` | `WHERE l.outcome = 'VERIFIED'` → `WHERE true` | exit 2 `missing … BACKUP_COMPOSE_FILE` (the unverified set was taken) |
| T-M1' `drill_refuses_a_writable_repo_mount` | EROFS probe → `if false &&` | exit 2 (≠ 3), the drill went on |
| T-M6 `a_leftover_drill_resource_blocks_the_next_drill` | stale filter → `label=humaux.drill=<own id>` | exit 2 (≠ 3), the stale volume unseen |
| T-M7 `drill_refuses_below_the_free_memory_or_disk_floor` | disk floor → `if false &&` | the drill ran to `isolation_not_applicable` (≠ 3) |
| T-M2 `drill_destroys_only_its_own_project` | `name=humaux-drill-<id>` added to the destroy filters | `decoys_survived` (left ≠ (true, true)) |
| T-O2 `the_drill_receipt_is_written_after_destroy_with_residue` | receipt inserted before destroy | `residue counted after destroy` (left None) |
| T-O3 `the_evidence_file_holds_no_secret` | the source DSN put in the evidence | `a DSN in the evidence` |
| T-L1 `drill_end_to_end_on_scratch_containers` | drill-mode quiescence predicate reverted | `checks_failed:rebuild_equivalent` |
| T-L1 | drill Qdrant face built from `HUMAUX_MAINTENANCE_QDRANT_{HOST,PORT}` | `drill_qdrant_not_fresh` |
| T-L1 | `repo1-retention-full=3` | `retention 2: exactly two fulls` (left 3) |
| T-L1 | `--archive-mode=off` dropped from the drill's restore (the source's `postgresql.auto.conf` carries `archive_mode=on`, review pass U4) | `failure: checks_failed:drill_archiver_attempts` (drill.rs:1075, exit 1 ≠ 0) |
| T-L2 `drill_with_a_corrupted_wal_segment_fails_and_no_gauge_advances` | a `pg` that exits treated as promoted | `neutralise_failed: psql: service "pg" is not running` (failure ≠ `restore_failed`) |
| T-L4 `drill_reports_each_injected_catalog_fault` | (h) hard-coded to 0 | `payload_digest_mismatches >= 1` |
| T-Q1 `restore_pitr_to_end_of_archive_quarantines_worker_roles` | NOLOGIN step skipped | `role_gateway quarantined` |
| T-Q1 | pre-restore verify skipped | exit 1 `restore_failed: … actual checksum` (≠ 3) |
| T-Q1 | `production_pg_running` skipped | exit 0: the restore forked beside the running source (≠ 3) |

## S6 — gauges, alerts, registry and routing as built

- **Producer (D-K as corrected by 10.11 D).** `MaintenanceTask::DrEvidence` (label `dr_evidence`) is a cluster-level
  task of the resident daemon like PARTITIONS: cadence key `HUMAUX_MAINTENANCE_SERVE_DR_EVIDENCE_EVERY_SECONDS`, no
  LIMIT key, one `maintenance_task_runs_total` count per run. Each run is ONE statement
  (`adapters::maintenance_repo::dr_evidence`): CTE `latest` = `DISTINCT ON (backup_label)` over the labelled receipts
  newest first; local = `max(backup_stopped_at)` of `latest` VERIFIED; the newest succeeded drill's `finished_at`;
  `repo_bytes` and the recorded limits from the newest receipt `WHERE repo_bytes IS NOT NULL` (a refusal qualifies);
  the WAL latch as a data-modifying CTE (`v = coalesce(newest VERIFIED start, '-infinity')`, insert when
  `last_failed_time > v` and no row since `v`; `wal_archive_failing` = a row since `v` or failing now). The archiver
  input is `coalesce($1, last_failed_time)`, `coalesce($2, last_archived_time)`; production binds NULL, NULL — the
  seam exists for T-K3' only. Then two `df -Pk` children (tokio `process`, killed on drop, bounded by CYCLE_SECONDS)
  on `HUMAUX_MAINTENANCE_DR_{REPO,PGDATA}_FS_PATH`, the headroom in Rust (`repo_max` = recorded max − repo − estimate;
  `free_floor` = live repo free − recorded min free − estimate; both 0 before the first measuring receipt), and
  `telemetry::dr::publish`, which holds the one `.set(` per family. The daemon reads no budget key.
- **Failure.** A failed run publishes nothing (the last values stay) and counts `outcome="failed"`; `/metrics`
  answers 503 for that cycle (ADR-0062 D-A). No `reset()` exists in `telemetry::dr`.
- **One value for `target`.** `telemetry::dr::TARGET_LOCAL = "local"` is the only `target` the render can emit
  (`write_single_label(…, &[TARGET_LOCAL], …)`); no `"offsite"` literal exists in `crates/telemetry/src`,
  `crates/adapters/src` or `bins/maintenance/src` (gate `c37_no_offsite_claim`).
- **Estimate ratio, one definition.** `maintenance_repo::ESTIMATE_RATIO` (5/4) and `estimate_from_set_bytes` replace
  the backup arm's private const, so the arm's precheck and the daemon's headroom cannot drift apart.
- **Alerts.** BackupFailure → `time() - backup_last_success_timestamp_seconds{target="local"} > 93600` and its
  comment says "26 h without a new VERIFIED local backup set"; appended BackupNotOffsite
  (`label_replace(vector(0), "repo_class", "local_only", "", "")`, WARN, no `for`, the E17 annotation),
  RestoreDrillFailure (`> 691200`, CRITICAL), WalArchiveFailing (`> 0` for 15m, CRITICAL), BackupBudgetLow (`< 0` for
  1h, WARN), DiskFreeLow (`{volume="pgdata"} < 8053063680` for 15m, CRITICAL). Alertmanager: child route
  `alertname="BackupNotOffsite"` → receiver `humaux-log-sink-weekly` (same `url_file` placeholder,
  `send_resolved: false`), `repeat_interval: 168h`; SP-16's amtool resolves it with the placeholder as is.
- **Promtool: 11 cases**: BackupFailure fire/silent (O4: series and `exp_labels` `target: local`), BackupNotOffsite
  (one case, two firing checks, labels and annotation), RestoreDrillFailure, WalArchiveFailing, BackupBudgetLow and
  DiskFreeLow fire/silent. **Mutations: red=32/32** (+ `not_offsite`, `drill`, `wal_failing`, `budget_low`,
  `disk_free_low`, tokens as 10.1 / 10.11 F).
- **Registry.** Baseline §41.2: the two changed rows (取数点 = the DR_EVIDENCE run, `target` = `local` only) and four
  new rows (`backup_repo_bytes`, `backup_disk_free_bytes{volume}`, `backup_budget_headroom_bytes{limit}`,
  `wal_archive_failing`), frozen value sets `target: local`, `volume: repo | pgdata`, `limit: repo_max | free_floor`,
  `task += dr_evidence`; §42: BackupFailure's local expression and the five new rows. `NOT_YET_PRODUCED` is empty;
  `metrics-registry --check` D7 reads `maintenance-serve=9`, D8 passes with 21 expressions; six witnesses in
  `crates/testkit/tests/metrics/` (each one test named after its family).

### S6 decisions and deviations (each with its reason)

1. **RestoreDrillFailure test cases use a negative timestamp** (`-691200` fire, `-604800` silent, both at 1m):
   promtool's `evaluation_interval` is file-wide, so an `8d1m` case evaluates every rule 11,581 times; measured, two
   such cases took `test-rules.sh` from 2.2 s to 25 s and the 32-row mutation run to ~14 min. The negative value is
   "the last drill finished eight days before the test clock's start" (time() = 0 at 0m), the same arithmetic.
2. **BackupNotOffsite has one promtool case with two firing checks** (1m and 2h), like Watchdog: `test-rules.sh`
   requires two `alertname:` checks per alert, and the rule has no silent state in card 37 (10.11 F drops it).
3. **`sweep once` runs DR_EVIDENCE too** (it runs every task over one page, ADR-0062 D-Q), so it requires the two
   `HUMAUX_MAINTENANCE_DR_*_FS_PATH` keys like `--serve`; it reads no cadence key.
4. **The witness gates use the probe harness** (`target/metrics-witness-probe`, as `c34b_witness_probe`): the
   design's `cargo test -p humaux-testkit --test metrics` names no test target (the witness files are compiled only
   by `metrics-registry`'s probe package). Each witness has one test named after its family, so the gate greps
   `test <family> ... ok`.
5. **Copied card-34b gates edited** because the allowlist row is gone: `c34b_metrics_registry` now requires
   `metrics-registry D8: pass` instead of the "not yet exported: backup… (producer: card 37)" text;
   `c34b_not_yet_produced_backup_only` asserts the empty list. The pinned unit test
   `t_h6_d8_allowlist_is_the_only_not_applicable_path` keeps its name and drives `check_d8_with` with a fixture
   entry (`fixture card`), and asserts the real list is empty.
6. **T-J13 and T-L1 run one production-shaped DR_EVIDENCE pass** (`throwaway::dr_evidence_pass`: the real
   `--serve` against the scratch cluster's role_maintenance until its first cycle answers 200, then killed as a
   `Reaped`). T-J13's scratch cluster gains a migrated `humaux_thread` database and TCP (`listen_addresses=*`) for it.
   Measured on T-J13: at the second pass the archiver is usually still failing (`last_failed_time` newer), so the
   latch's own fault is shown by T-K3' (dev seam), not by T-J13.
7. **`docs/ops/rehearse.sh`** gains the three daemon keys (`DR_EVIDENCE_EVERY_SECONDS=3`, both FS paths = the
   evidence directory): without them the rehearsal's daemon refuses to boot. Noted for S7's `c37_rehearse.sh`: the
   rehearsal daemon now exports `backup_last_success_timestamp_seconds{target="local"} 0`, so BackupFailure fires
   there, and BackupNotOffsite fires from Prometheus start (routed to the weekly receiver).

### S6 tests and the fault that turned each red (mutated by writing content + touch, restored, re-run green)

| test | fault | failing line |
|---|---|---|
| T-K1 `dr_evidence_sets_both_gauges_from_the_latest_verified_receipts_only` (serve.rs) | `max` over every VERIFIED receipt | `left: Some(1788310800.0) right: Some(1788224400.0)` (the withdrawn set's stop) |
| T-K2 `a_failed_dr_evidence_run_keeps_the_last_values_and_counts_failed` | publish a zero reading on failure (`reset`) | `kept: … left: Some(0.0) right: Some(1788224400.0)` |
| T-K3' `wal_archive_failing_latches_until_a_later_verified_full` | the `last_failed_time > last_archived_time` comparison alone | `(b) a later archived segment does not clear it` |
| T-K3' | no `-infinity` coalesce on `v` | `(a) one row per incident: left: 0 right: 1` |
| T-K4 `budget_headroom_is_max_minus_repo_minus_estimate` | estimate omitted | `left: Some(4000.0) right: Some(3500.0)` |
| T-K4 | headroom from the newest VERIFIED receipt only | `left: Some(3500.0) right: Some(-1500.0)` |
| witness `backup_repo_bytes` (probe) | its `.set(` removed | `backup_repo_bytes: test result: FAILED. 0 passed; 1 failed` |
| witness `backup_last_success_timestamp_seconds` (probe) | its `.set(` removed | `test result: FAILED. 0 passed; 1 failed` |
| promtool `not_offsite`, `drill`, `wal_failing`, `budget_low`, `disk_free_low` | the 10.1 / 10.11 F tokens | `alertname: <Rule>, time: …` for each (red=32/32) |
| gate `c37_not_offsite_route` | route matcher renamed | exit 1 |
| gate `c37_dr_families` | `TARGET_LOCAL` = `concat!("off", "site")` | exit 1 |

## S7 — rulings, scope, decisions, measurements and the record

### Rulings applied (main line, 2026-10-04 / 05)

| ruling | what it fixed for this card |
|---|---|
| E1 | the §6.2.2 / §41.2 / §44 / §16.2 texts and the ADR-0057 D-C note, edited with the code (reduced to `local_only` by E17: Baseline §44, §41.2 `target = local`, §42 rows, §16.2 note, a §1.9 pointer that P0-2 stays open) |
| E2 | card corrections: ADR 0064; generation g+1 in the side table `projection.rebuild_tickets` (stream_log's columns frozen); vectors written in the registry transaction; host crontab + `humaux-dr.sh` (0600 `dr.env`), no scheduler container; witness-defined T (`--target-time` struck for the drill); the drill's isolation = a closed deps constructor + a counted refusal; in-place generation, no serving switch; recall parity = E3 + E4 + E5; Qdrant snapshot deferred |
| E3 | re-embed generation and the gateway's fingerprint refusal → card 37b (RQ-6 closed on the write side only) |
| E4 / E5 | allowed files incl. `crates/adapters/src/retrieve.rs` (one overlay predicate); cards 35 / 36 files appended only |
| E6 | vectors purge-by-UPDATE with the point (no table_key, no DELETE door); the EVENTS coverage hold and the PITR replacement of card 36's COPY export → card 37c |
| E7 | the main line alone migrates `humaux_thread_dev` (after a dated `pg_dump -Fc`); no test touches the shared containers; scratch containers `humaux-c37-<purpose>-<pid>` with caps; `blocked: free=<n> MiB` |
| E8 | (a) no scheduler container, (b) no §77 rows for the DR arms (receipts are the record), (c) the drill's in-process projector, (d) snapshot deferred, (e) `--workspace` granularity |
| E9 | the enumerated grants (section "Enumerated grants"; 10.11 C corrected receipts) |
| E10 (as amended by 9.6 / 10.11 J) | the only secrets: `PGBACKREST_REPO1_CIPHER_PASS` and `HUMAUX_MAINTENANCE_PG_DSN` in `$HOME/.config/humaux/dr.env` (0600) until card 54 |
| E11 | the deploy-artifact offsite list → card 39 |
| E12 | `projection rebuild --allow-reembed` runs on production and dev by the main line after commit |
| E13 | the one `rebuild_tickets` predicate in `pg_delta_overlay_in_txn` + role_gateway SELECT on `rebuild_tickets` |
| E14 | no OSS / S3 / key pairs; a posix repository in a host directory, encrypted from day one; NAS later by PULL |
| E15 | disk budget: refuse before write, both keys required; WAL cannot fill a disk; visible numbers; a disk table |
| E16 | the owner's intent is the invariant (five invariants, section "E16 table"); mechanism is the design's |
| E17 | card 37 ships `local_only` only; everything only the NAS mirror needs (D-W, inventories, offsite receipts, cipher-pass attestation, `BackupOffsiteStale`, mirror tests / spikes / gates / mutations, NAS runbook steps) moves to card 37d; nothing in card 37 can build, name or report an offsite copy (gate `c37_no_offsite_claim`) |

### Scope under E17 (design section 10)

Built: the KEEP rows of design 10.1 (113 rows) as corrected by 10.11 A–N. Not built (card 37d, design 10.1 MOVE,
60 rows): decision D-W entire (inventory, `humaux-digest`, `sshd_config`, `mirror-gate.sh`, `nas-pull.sh`, `backup
mirror-ingest`, `backup attest-cipher-offline`), the `mirror` image stage and compose service, `ops.backup_inventories`,
`ops.cipher_pass_checks`, `backup_receipts.target` and the offsite columns / CHECKs / FKs, the offsite gauge target,
`BackupOffsiteStale`, SP-6 / 7 / 8 / 15 (numbers reserved), M4, the mirror tests and gates, the NAS runbook steps.
How 37d adds each moved schema element as a plain forward migration on card-37 data is design 10.3's table (the
receipts CHECK is named `backup_receipts_verified_derived` so 37d can swap it in one `ALTER TABLE`).

**Why nothing can report an offsite copy.** No writer (no `mirror-ingest`), no column (`target` does not exist; no
`'offsite'` literal in 0234), one label value (`telemetry::dr::TARGET_LOCAL`), one class const
(`backup::REPO_CLASS = "local_only"`, `// ponytail: one value on purpose (E17)`), no config switch, no compose service,
no `deploy/mirror/`. Gates `c37_no_offsite_claim`, `c37_dr_families` (no rendered `target="offsite"`) and
`c37_rules_loaded` (no offsite matcher).

**The five E16 invariants on card 37 alone.** (1) Never fills a disk or stops the service: refuse-before-write on
both budget keys, the repository on its own fixed 12 GiB filesystem (a full repository stops archiving, not
PostgreSQL: T-J13), `archive-push-queue-max=2GiB`, `DiskFreeLow`, the drill / restore byte floors. (2) Small, bounded,
visible: retention 2 daily fulls, zstd, bundling, no chains; six gauges and `backup status`; measured peak 0.74 GB
(M5). (3) Nothing paid or external: a posix directory on the server's own disk. (4) No false claim: the paragraph
above; VERIFIED and `succeeded` derived by table CHECKs. (5) Always one verified restorable set after the first:
expire only after VERIFIED (T-J7), retention 2, a refusal deletes nothing, failed sets are never auto-deleted (L29),
restorability proven weekly by the drill.

### Decisions D-A … D-V (index; the text as built is in the slice sections above)

| decision | where |
|---|---|
| D-A generation g+1 = new tickets on the same stream in `projection.rebuild_tickets`, rebuilt in place | S1, S3 |
| D-B stored vectors `projection.memory_vectors`, written in the registry transaction, read first | S1, S2 |
| D-C one label = one fingerprint (`projection.embedding_fingerprints`); worker boot binding | S1, S2 |
| D-D vector purge by UPDATE with the last live binding (tombstone row kept) | S2 |
| D-E `projection rebuild` and its three definers | S1, S3 |
| D-F equivalence E1–E5 against Project(PG@H2); quiescent orphan step | S3 |
| D-G another fingerprint refused `re_embed_required`; legacy rows only with `--allow-reembed` | S3 |
| D-H the PG bundle: one image stage `pg` (PG 18.6 arm64 digest + pgbackrest 2.59.3-1.pgdg13+1), `pgbackrest.conf`, `backup.yml` (repo = host bind, `create_host_path: false`) | S4 |
| D-I the host crontab (two lines) and `humaux-dr.sh` (0600 `dr.env` check) | S4 |
| D-J `backup check | run | verify | status`; receipts; label identity | S4 (10.11 A, C, E, G) |
| D-K the DR_EVIDENCE task, six families, the WAL latch | S6 (10.11 D) |
| D-L / D-M / D-N / D-O the restore drill, its isolation, checks and receipt | S5 (10.11 H) |
| D-P test containers (`humaux-c37-*`, caps, per-test host-dir repository) | S4, S5 |
| D-Q the acceptance flow (T-L1) and the measurements | S5, S7 |
| D-R docs | S7 |
| D-S Qdrant snapshot fast path: deferred; trigger = measured `rebuild_project_s` of the largest production tenant > 6 h | S7 (M2) |
| D-T restore and deletion: deletion bound 3 d; PITR replays deletions ≤ T'; partition drops re-derived | S7, runbook §11 |
| D-U `restore pitr` | S5 (10.11 B) |
| D-V disk budget, retention and WAL bound (below) | S4, S7 |

**D-V as built (design 9.3 D-V, corrected by 10.11 A).** Daily full at 00:30 UTC, `repo1-retention-full=2` (count),
`expire-auto=n`, no diff / incr chains; expire only after the new set is VERIFIED; failed sets are never deleted
automatically (L29). The repository lives on its own fixed 12 GiB ext4 image mounted at `HUMAUX_PG_REPO_DIR`
(`loop,nodev,nosuid,noexec`, outside `/var/lib/docker`); `check` and `run` refuse `repo_shares_pgdata_fs` when it
shares a device with `pg1-path`. Refuse before write on live numbers measured in `pg` (`du -sk`, `df -Pk`): estimate =
newest VERIFIED `set_repo_bytes` × 1.25 (`maintenance_repo::ESTIMATE_RATIO`), or `du -sk <pg1-path>/base` for the first
backup; `budget_repo_max:<bytes>` when repo + estimate > `HUMAUX_MAINTENANCE_BACKUP_REPO_MAX_BYTES`;
`budget_free_floor:<bytes>` when free − estimate < `HUMAUX_MAINTENANCE_BACKUP_MIN_FREE_BYTES` (free space on the
repository filesystem: the WAL margin). Both keys are required with no default; runbook values 8 GiB / 2 GiB. A
refusal writes a FAILED receipt with a NULL label and the numbers, and nothing to the repository. Peak = (N+1)·F + N·W
(N = 2); measured 0.74 GB (M5). `archive_timeout` = 3600 s; `archive-push-queue-max=2GiB` caps `pg_wal` growth from a
failing archive (≈ 5 days at light load before drops; SP-10 measured the drop + exit 0 in sync mode), and
`WalArchiveFailing` latches from the first observed failure until a later full is VERIFIED.

**Growth statement (D-B).** Stored vectors cost M2's `vectors_bytes_per_row` per live point (1024 float4 + key +
TOAST); the old system's ≈ 49.6 k memories ⇒ ≈ 290 MB (5,838 B × 49.6 k) in PostgreSQL and inside every full set.

### S7 measurements (`bins/maintenance/tests/measure.rs`, ignored; this Mac, Docker Desktop 8 GiB VM, 2026-10-06)

M1 / M3 / M5 ran on a scratch source (768 MiB, 1 CPU, repository on a host bind) holding the card-36 dev dump
`humaux_thread_dev.pre_c36.164348.dump` restored into a second database (read as a file; the shared container was
never touched): `HUMAUX_REQUIRE_DOCKER=1 HUMAUX_C37_MEASURE_DUMP=<dump> cargo test -p humaux-maintenance --test measure
-- --ignored --exact m1_m3_m5_backup_restore_and_wal_on_the_dev_dump --nocapture` (90.7 s, EXIT 0). M2 ran on a
throwaway database on the dev cluster and a scratch Qdrant (512 MiB): `HUMAUX_REQUIRE_DB=1 HUMAUX_REQUIRE_DOCKER=1 cargo
test -p humaux-maintenance --test measure -- --ignored --exact m2_rebuild_of_dev_sized_stored_vectors --nocapture`
(4326.4 s, of which the production-path seed took 3884 s: one gitleaks scan per sealed card, EXIT 0). Logs: `$TW/c37_measure_m1.log`, `$TW/c37_measure_m2.log`.

MEASURE m1 full_backup_s=3.942 s n=3 (runs=5.284,3.265,3.942; pgbackrest=2.59.3; scratch source 768 MiB / 1 CPU, repo on a host bind; cluster 770569131 B incl. the dev copy 724571839 B from humaux_thread_dev.pre_c36.164348.dump (errors ignored on restore: 0))
MEASURE m1 set_repo_bytes=149467792.000 bytes n=3 (runs=149467792.000,149467792.000,149467792.000; F of D-V: info backup[].info.repository.size, zst + aes-256-cbc)
MEASURE m1 verify_full_s=0.746 s n=3 (runs=0.746,0.623,0.818; verify --set, the arm's pass rule)
MEASURE m1 pitr_restore_s=8.470 s n=3 (runs=10.896,8.470,8.244; restore pitr --target end receipt restore_s: restore + socket boot + quarantine + TCP boot; source stopped)
MEASURE m1 expire_s=0.090 s n=3 (runs=0.075,0.097,0.090; expire after each verified full; run 3 removed the oldest set)
MEASURE m2 rebuild_issue_s=0.150 s n=3 (runs=0.377,0.147,0.150; 17435 points x 1024 dims, two tenants, 350 generation tickets per round (one per Evidence of 50); throwaway DB on the dev cluster, scratch Qdrant 512 MiB / 1 CPU; seed 3884 s)
MEASURE m2 rebuild_project_s=126.217 s n=3 (runs=127.118,126.217,116.679; in-process run-scoped pass, stored vectors, NoProviderEmbedder attempts 0)
MEASURE m2 verify_s=20.648 s n=3 (runs=20.648,20.744,19.951; E1-E5 incl. per-point vector compare, verdict equivalent)
MEASURE m2 vectors_bytes_per_row=5838.004 bytes n=3 (runs=5838.004,5838.004,5838.004; pg_total_relation_size(projection.memory_vectors) / rows, 1024 float4)
MEASURE m3 wal_bytes_per_day=145501248.000 bytes n=1 (runs=145501248.000; upper bound: one full rewrite of the dev copy per day (pg_restore 28.4 s, 42 segments = 704643072 B raw archived as 145484640 B, ratio 0.206) + 24 x SP-9 692 B)
MEASURE m3 drill_check_wait_s=0.208 s n=3 (runs=0.189,0.208,0.220; pgbackrest check in production pg (the drill's archive_wait_seconds))
MEASURE m5 peak_bytes=739405872.000 bytes n=1 (runs=739405872.000; (N+1)F + N W with N=2, F=median set_repo_bytes, W=m3 bound; REPO_MAX_BYTES 8 GiB = 8589934592 B, headroom 7850528720 B)

What they say:
- **F = 149 MB** for a 770 MB cluster (zst + aes-256-cbc, bundling): 19 % of the database bytes. D-V's upper bound
  (F ≤ 0.97 GB) was 6.5 × too pessimistic; the 8 GiB `REPO_MAX_BYTES` holds the peak with 7.3 GB to spare. Re-size
  from M1 on the imported production data at go-live (O6).
- **W** has no production sample yet (this system is not live): m3 is an **upper bound** — every byte of the dev
  copy rewritten once per day (42 segments of real write WAL compress to 21 %) plus the hourly forced switches. The
  idle floor is 16.6 kB / day (SP-9). Re-measure from `pg_stat_archiver` seven days after go-live.
- **RPO**: with the archive intact, ≤ `archive_timeout` = 1 h at light load (a forced switch hourly; busier writes
  fill 16 MiB segments sooner); `drill_check_wait_s` 0.21 s is the time PostgreSQL + pgBackRest take to ship one
  forced segment. With the archive lost: the age of the last verified full (≤ 24 h + the nightly run time).
- **Rebuild**: 17,435 stored 1024-dim vectors re-project in 126 s (≈ 138 points/s, the in-process run-scoped pass,
  no provider call) and verify in 21 s (E1–E5 incl. every vector); issue is 0.15 s (350 tickets). Stored vectors cost
  5,838 B per row in PostgreSQL (D-B's ≈ 4 KB estimate was low by the key, TOAST and index overhead).
- **RTO (dev size)** = `pitr_restore_s` 8.5 s + the drill's `checks_s` 0.57 s (acceptance evidence below) +
  `rebuild_issue_s` + `rebuild_project_s` + `verify_s` (M2) = 8.47 + 0.57 + 0.15 + 126.2 + 20.6 ≈ **156 s** for 17,435 points, against §67.2's 24 h. D-S's snapshot trigger
  (rebuild > 6 h for the largest tenant) is about 170 × the measured 126 s per 17,435 points (≈ 3 M points at this rate, single stream pass) away.

### Acceptance evidence (T-L1 `drill_end_to_end_on_scratch_containers`, `$TW/c37_drill_evidence.json`, 2026-10-06)

```json
{
  "arm": "restore drill",
  "backup": {
    "label": "20261006-064436F",
    "manifest_matches": true,
    "manifest_sha256": "66a73ac7db1c99aff00244a49636207e8f065a23735b698d6d26c8cee5b7396e",
    "stopped_at": 1791269077,
    "type": "full"
  },
  "drill_archiver_attempts": 0,
  "drill_id": "01a10ff5-0a1f-72b0-9065-c8537ac9a03c",
  "failure": null,
  "isolation_pairs": 2,
  "isolation_violations": 0,
  "legacy_points_without_vector": 0,
  "migrations_drift": 0,
  "notice": "NOT OFFSITE: every backup copy lives on this database host; host loss = total loss / 非异地：所有备份副本都在本数据库主机上，主机丢失 = 全部丢失 (ADR-0064)",
  "payload_digest_mismatches": 0,
  "project": "humaux-drill-01a10ff5-0a1f-72b0-9065-c8537ac9a03c",
  "provider_calls": 0,
  "rebuild": {
    "distill_pending_inputs": 1,
    "equivalent": true,
    "orphans_deleted": 0,
    "points": 6,
    "provider_calls": 0,
    "streams": [
      {
        "closed": true,
        "collection": "test_a2_points_f74cbfb986ce4af59ee190d3ef3b5747",
        "differing_ids": {
          "payload": [],
          "pg_projection_not_registered": [],
          "qdrant_not_registered": [],
          "registered_not_in_pg_projection": [],
          "registered_not_in_qdrant": [],
          "vector": []
        },
        "domain": "private_memory",
        "excluded": {
          "distill_pending": 1,
          "without_stored_vector": 1
        },
        "excluded_by_outcome": {},
        "generation": 2,
        "h2": 6,
        "issued": 2,
        "legacy_points_without_vector": 0,
        "merkle_root": "7edaf1b73eb04a416dd08e2132bfbf9baacb7d54b5c1b8e3d8d8080055524366",
        "orphans_deleted": 0,
        "other_label_points": 0,
        "points": 3,
        "projection_kind": "PRIVATE_MEMORY",
        "projection_version": "v1",
        "reasons": [],
        "run_id": "01a10ff5-39c5-7d9c-b52d-29d8299ac281",
        "tenant_id": "01a10ff4-c19a-7fce-a75a-7b95f9ca81dc",
        "tombstoned_unpurged_points": 0,
        "verdict": "equivalent",
        "workspace_id": "01a10ff4-c4b4-7162-849a-df5788fa03a4"
      },
      {
        "closed": true,
        "collection": "test_a2_points_41c7c47098594daaae3c34e5c5dde0ff",
        "differing_ids": {
          "payload": [],
          "pg_projection_not_registered": [],
          "qdrant_not_registered": [],
          "registered_not_in_pg_projection": [],
          "registered_not_in_qdrant": [],
          "vector": []
        },
        "domain": "private_memory",
        "excluded": {},
        "excluded_by_outcome": {},
        "generation": 2,
        "h2": 4,
        "issued": 2,
        "legacy_points_without_vector": 0,
        "merkle_root": "5c7a4872af24c0bf67a60346fafccd62a84f807568010ba422646d65d441866b",
        "orphans_deleted": 0,
        "other_label_points": 0,
        "points": 3,
        "projection_kind": "PRIVATE_MEMORY",
        "projection_version": "v1",
        "reasons": [],
        "run_id": "01a10ff5-3bbb-7ea8-8001-f72ef665a67a",
        "tenant_id": "01a10ff4-cbba-7769-8104-3c12a9dc94c2",
        "tombstoned_unpurged_points": 0,
        "verdict": "equivalent",
        "workspace_id": "01a10ff4-cdc0-73c9-9878-579c7831fef4"
      }
    ],
    "unprojected_at_target": 1
  },
  "rebuild_points": 6,
  "repo_class": "local_only",
  "repo_intact": true,
  "residue": 0,
  "restored_in_flight": {
    "issued_g1": 2,
    "open_jobs": 13,
    "reserved_calls": 0
  },
  "rls_unforced": 0,
  "rpo": {
    "backup_age_s": 16,
    "wal_archive_wait_s": 0.436387208
  },
  "rto_seconds": 10.659982792,
  "server_version_matches": true,
  "source": {
    "server_version_num": 180006
  },
  "succeeded": true,
  "timings": {
    "checks_s": 0.568623459,
    "destroy_s": 1.312218917,
    "rebuild_s": 1.087565042,
    "restore_s": 7.472315,
    "witnesses_s": 0.4783345
  },
  "unprojected_at_target": 1,
  "witness_a_present": true,
  "witness_b_absent": true,
  "witnesses": {
    "a_written_at": 1791269081,
    "archive_wait_seconds": 0.436387208,
    "b_walfile": "00000001000000000000000A",
    "b_written_at": 1791269081,
    "target_time": "2026-10-06 06:44:41.404107+00"
  }
}
```

### Findings and their resolution (design 10.10, finisher pass; 18 findings, all accepted)

| # | sev | finding | resolution as built | where |
|---|---|---|---|---|
| F1 | P0 | archived WAL had no hard cap and shared PGDATA's filesystem; a refusal also stops expiry | the repository on its own fixed 12 GiB filesystem; `repo_shares_pgdata_fs` refusal; `MIN_FREE_BYTES` = free space on that filesystem; T-J13, T-J14 | S4, runbook §11.1 step 1 |
| F2 | P0 | `restore pitr` beside a running production forks the timeline | refusal `production_pg_running:<container>`, disk floor `restore_free_bytes`, runbook cutover steps; T-Q1 (i)–(iii) | S5, runbook §11.6 |
| F3 | P1 | a budget refusal could not be written (NOT NULL label / manifest) | nullable label / manifest / start / stop + `backup_receipts_shape`; set reads filter `backup_label IS NOT NULL` | S1 (0234), S6 |
| F4 | P1 | after a refusal the gauges read 0 and BackupBudgetLow cleared | live `du` / `df` written into the refusal row; DR_EVIDENCE reads the newest receipt `WHERE repo_bytes IS NOT NULL`; T-K4 | S4, S6 |
| F5 | P1 | the PITR window counted expired sets | `status` intersects with `info --output=json`; `none` when no set qualifies; T-J9 | S4 |
| F6 | P1 | BackupNotOffsite tied to a series flapped | `label_replace(vector(0), "repo_class", "local_only", "", "")`, `send_resolved: false`, mutation `label_replace(vector(0),` → `… < 0,`; 11 promtool cases | S6 |
| F7 | P1 | T-K3' could not run on the archive-off dev cluster | parameter seam on the archiver input (production binds NULL); the live leg is T-J13 | S6 |
| F8 | P1 | T-J8's tmpfs filled during seeding | PGDATA on its volume, repository on a 256 MiB tmpfs, seeded with archiving off, test-owned filler | S4 |
| F9 | P1 | queue-max and cipher type could not go red | T-H6 asserts six conf keys; `check` refuses `repo_not_encrypted`; T-J12 asserts `repo_cipher=aes-256-cbc`; T-J13 proves queue-max | S4 |
| F10 | P1 | daemon df paths / budget limits unconfigured or duplicated | receipts carry `repo_max_bytes`, `min_free_bytes`, `estimate_bytes`; the daemon reads no budget key; runbook step 7 + supervision.md | S4, S6, S7 |
| F11 | P2 | `repo_intact` hashed only the two info files | digest of the `backup/` tree listing (names, sizes) + both info files (L38) | S5 |
| F12 | P2 | latch compared with NULL before the first VERIFIED | `coalesce(…, '-infinity')`; guard "any failure since the newest VERIFIED start" | S6 |
| F13 | P2 | pin regex vs `AS pg` | `FROM postgres:18.6@sha256:<64 hex> AS pg`; gate regex `… AS pg$` | S4 |
| F14 | P2 | off-site wording left in the rule comment and Baseline | rule comment rewritten; `c37_no_offsite_claim` greps both quote forms in three source trees and the Baseline phrases | S6 |
| F15 | P2 | the drill floor ignored the production floor and the SP-5 copy | need = restore + vectors + repo copy (0: SP-5 green) + `HUMAUX_MAINTENANCE_RESTORE_MIN_FREE_DISK_BYTES`; the refusal names every term | S5 |
| F16 | P2 | no remedy after a refusal; no exact stanza-create line | runbook §11.1 step 3 and §11.5 | S7 |
| F17 | P2 | Docker tests ran twice; T-W6 duplicated a static gate | suite lines write `$TW/c37_{backup,drill}_suite.log`, per-test lines grep it; T-W6 folded into T-L1's teardown | S4, S5, gates |
| F18 | P2 | `DRILL_ARCHIVE_WAIT_SECONDS` had no reader; `pg_database_size` may be denied | the key is deleted (the wait is pgBackRest's `archive-timeout`, measured as `archive_wait_seconds`); first estimate = `du -sk <pg1-path>/base` | S4, S5 |

Section 9.12's findings kept by 10.1 (#3, #6, #7 part 2, #11, #12, #14–#20, #25, #26) are resolved in D-K (latch),
S5 (`repo_intact`, pre-restore verify), the custody check, the 3 d deletion bound, the repository bind, the
local-only BackupFailure, the weekly route, `DiskFreeLow` and the runbook wording; the moved ones are card 37d's.

### Review and verifier pass (2026-10-06): findings and uncaught faults

| # | sev | finding | resolution | test / gate |
|---|---|---|---|---|
| R1 | P1 | a VERIFIED receipt with a NULL `verify_exit` passed both CHECKs (`TRUE = NULL` is NULL) | 0234 (uncommitted, never applied outside throwaways) `backup_receipts_shape`'s VERIFIED arm requires `verify_exit IS NOT NULL`; the manifest postcheck pins the term | T-J1 NULL-exit case (red `Ok(1)` → green) |
| R2 | P1 | runbook §11.1 ran `humaux-dr.sh` before `dr.env` existed and never started `pg` through the wrapper | §11.1 reordered: filesystem → `dr.env` (incl. `PATH`) → `humaux-dr.sh compose up -d pg` + stanza-create through the wrapper + `backup check` → password → import → crontab → daemon env; a plain `docker compose up` is called out (no cipher pass → WAL archiving fails) | `c37_docs` |
| R3 | P1 | the crontab's evidence directory was never created (the drill recorded success, then lost the evidence with exit 1) and cron's `PATH` cannot reach `humaux-maintenance` (exit 127) | `restore drill` / `restore pitr` refuse an `--evidence` whose directory is missing before any work (exit 2, no receipt); runbook step 6 creates `$HOME/humaux/dr-evidence` and states the `$HOME/humaux` checkout; `humaux-dr.sh` refuses by name (exit 3) when the binary it would exec is off `PATH`; runbook step 2 lists `PATH` in `dr.env` | T-M4b; T-I1 PATH leg |
| U1 | — | `verify_failure`'s `code != 0` branch had no test (T-J3's corrupt file exits 0, SP-1) | bin unit test on the pass rule, each half alone | T-J3b |
| U2 | — | raising `archive-push-queue-max` to 2 PiB is not caught by T-J13 | not a gap: T-J13 proves the mechanism with its own 64 MiB override (its fault: the override dropped); the shipped value is T-H6's static pin (design 10.11, `c37_stanza_named`), which the verifier saw red at `backup.rs:129` | T-H6 |
| U3 | — | T-W6 `the_repo_dir_survives_compose_down_v` and its gate do not exist | not a gap: design 10.11 I withdrew T-W6 and removed `c37_repo_survives_down_v_named`; the `down -v` assertion is T-L1's teardown, and the named-volume fault is red on `c37_repo_is_bind_mount` | T-L1, `c37_repo_is_bind_mount` |
| U4 | — | dropping `--archive-mode=off` left T-L1 green: `backup.yml` sets archiving by `-c` flags, so the restored data directory never archived | T-L1 writes `archive_mode` / `archive_command` into the source's `postgresql.auto.conf` (ALTER SYSTEM; the `-c` flags still win on the source) before the backups, so a restored cluster archives unless the drill turns it off | T-L1 |

Verifier reds outside these rows: `stream_count_off_request_path` listed `crates/adapters/src/rebuild.rs`, whose
`StreamCountFilter` use is the D-F verifier scroll this ADR's ADR-0057 D-C addendum allows — the gate's allow-list
gains `rebuild` (it still names every other file); `replica_mode_is_declared` — T-L4's replica-mode batch runs on the
scratch source and now says so; `c35_serial_lane_disposition` — the 4 live drills, 2 measurements and 12 spikes
carry `lane(c)` with their reason (Docker scratch containers / dev-dump copies no lane resource provisions; the
drills run under their own named gates), and `maintenance_tests` (`--include-ignored`) skips the 2 measurements and
12 spikes as it already skipped card 36's `partition_insert_latency`. The DB-bound reds are the dev database at
0231 (0232–0234 unapplied, `db-not-at-head`): the main line migrates dev (E7).

### Tests and the fault each one names (S1–S3; S4–S6 tables above give the failing lines)

The S1–S3 tests were each shown red under the fault below during their slice (mutated by writing content + touch,
restored); their failing lines are in the slice records.

| test | file | fault |
|---|---|---|
| `embedding_fingerprint::tests::every_field_changes_the_fingerprint` | `crates/projection/src/embedding_fingerprint.rs` | a field dropped from the canonical encoding |
| `stream_log_has_exactly_the_frozen_columns` (T-A3, unchanged) | `crates/adapters/tests/forget_repo.rs` | a `target_generation` column on stream_log |
| T-A1 `c37_s1::generation_tickets_are_unique_per_input_and_generation` | `crates/adapters/tests/rebuild.rs` | generation dropped from the side-table UNIQUE |
| T-A2 `c37_s1::close_refuses_while_a_generation_ticket_is_in_flight_or_the_boundary_moved` | same | in-flight check deleted from `rebuild_close` |
| T-A4 `c37_s1::a_generation_backlog_older_than_lost_after_is_never_swept` | same | the never-lost trigger dropped |
| T-C1 `c37_s1::a_label_bound_to_another_fingerprint_is_refused` | same | `UNIQUE (embedding_version)` dropped / label-only compare |
| T-J1 `c37_s1::a_verified_receipt_without_a_clean_verify_or_with_another_manifest_is_refused_by_the_table` | same | `backup_receipts_verified_derived`, `backup_receipts_shape` or the `backup_sets` FK dropped; `verify_exit IS NOT NULL` dropped from the shape's VERIFIED arm (a VERIFIED row with a NULL exit: `expected a refusal, got Ok(1)`, rebuild.rs:332) |
| T-O1 `c37_s1::a_drill_receipt_claiming_success_with_a_failed_check_is_refused` | same | one term (incl. `repo_intact`) dropped from the derived-success CHECK |
| T-R1 `rls_check::tests::c37_tables_and_definers_drive_gate_red_then_restore` | `xtask/src/rls_check.rs` | a re-granted 0011 default, `NO FORCE`, PUBLIC EXECUTE on a definer |
| T-B1 `a_registered_point_always_has_its_vector` | `crates/adapters/tests/rebuild.rs` | vector upsert removed from the registry transaction |
| T-B2 `a_reprojection_of_an_unchanged_card_calls_no_provider` | same | stored-vector lookup removed |
| T-B3 `retiring_the_last_live_point_purges_its_vector_and_revival_restores_it` | same | purge UPDATE removed |
| T-E1 `a_rebuild_into_an_empty_collection_is_equivalent_without_a_provider_call` | same | the projector reaches the provider during the rebuild |
| T-F1 `a_tampered_payload_vector_or_extra_point_is_not_equivalent` | same | E4 or E5 skipped |
| T-F2 `a_changed_fingerprint_is_refused_as_re_embed_required` | same | the fingerprint precheck skipped |
| T-F3 `verify_is_read_only` | same | verify deletes orphans |
| T-E2 `a_generation_ticket_failing_card_unbuildable_is_an_exclusion_not_a_mismatch` | same | deterministic exclusion counted as missing |
| T-E3 `a_failed_distill_evidence_still_closes_equivalent` | same | E2 accepting only two classes |
| T-E4 `a_tombstoned_evidence_gets_no_generation_ticket_and_no_overlay` | same | tombstone predicate or follow trigger dropped |
| T-E5 `recall_during_an_open_rebuild_serves_no_generation_row_in_the_overlay` | same | the E13 overlay predicate dropped |
| T-E6 `orphan_deletion_spares_a_point_registered_after_the_scroll` | same | orphans computed without the quiescent rule |
| T-E7 `an_operator_retired_input_stays_retired_after_an_equivalent_rebuild` | same | RETIRED_FAILED exclusion dropped |
| T-E8 `allow_reembed_backfills_legacy_vectors_under_the_bound_fingerprint` | same | backfill-on-touch skipped |
| T-E9 `drill_scoped_claim_leaves_restored_g1_tickets_alone` | same | `only_run` ignored |
| `projection_rebuild_requires_every_flag_and_prints_one_object_per_stream` | `bins/maintenance/tests/rebuild_cli.rs` | a default for `--batch` |
| `m1_m3_m5_backup_restore_and_wal_on_the_dev_dump`, `m2_rebuild_of_dev_sized_stored_vectors` (S7, ignored measurements) | `bins/maintenance/tests/measure.rs` | none by design: they print numbers, and each asserts its outcome (VERIFIED sets with retention 2, restored clusters, an equivalent rebuild with `NoProviderEmbedder` attempts 0) so a failed run prints nothing |

### Chain run 1 (2026-10-06 18:12–22:04): two reds, both closed before the commit

- `rehearse_c37`: the card-27 permanent-fault step of `docs/ops/rehearse.sh` started a 512-d `--run-once` worker under
  the production embedding label to provoke `qdrant_upsert_rejected`. D-C refuses exactly that at boot (exit 2,
  nothing claimed), so the ticket stayed ISSUED and the step went red — the refusal is the intended behaviour, the
  rehearsal was out of date. The step now witnesses the refusal first (`same_label_other_dimension_refused_at_boot`,
  exit 2 and the ticket still ISSUED, logged to its own `dc-refusal.log`) and injects the fault under the worker's own
  label `<label>-rehearsal-512d`, which binds its own fingerprint and reaches the Qdrant rejection as before. The
  `c37_rehearse.sh` post-chain assertion keeps its "no fingerprint_mismatch in the worker logs" clause.
- `no_leaked_distill_jobs`: 164 `PENDING` `DERIVED_DISTILL` jobs on `a2_point_identity.rs throwaway tenant` rows, all
  created by the workflow's intermediate test runs (02:00–09:51 UTC, before the fix stage); the final tree's own runs
  in the chain window left none. The rehearsal's resident worker had already parked them `WAITING_KEY`. Retired
  `DEAD` (`C37_WORKFLOW_RESIDUE`, 356 rows incl. older parked ones) with the record in
  `c37_chain_residue_retire.log`; the gate reran green. The a2 fixture's tenants are not `e2e-` named, so the fixture
  purge does not reach them: plan row 36c.
- The rerun of `rehearse_c37` after the first fix passed the permanent-fault step (both new assertions) and then lost
  six assertions to `ENTITLEMENT_REQUIRED` (HTTP 402) from the soak onwards: `xtask e2e-seed` opened the seeded
  tenants' quota window for one hour, the rerun took 67 minutes (the multi-tenant projection step alone 22, as in
  card 36's 19), and every billable call after 23:05:55 was refused by the window, not by the card. Run 1 had
  finished in 51 minutes. The seed window is now 24 hours (the card-53 soak is eight); the plan limit, not the clock,
  is the quota the rehearsal exercises. Both fixes were validated by a second full lane + chain run (run 2).
- Run 2 (2026-10-06 23:18 – 10-07 03:00, tree fingerprint `01a626dd…`): lane 127/127, chain 490 green, rehearsal
  135/135, stores unchanged. Its one red, `no_stranded_evidence`, was the outbox side of the residue retired above:
  the 356 `EVIDENCE_ACCEPTED` rows of the dead a2 jobs were still `PENDING`. Settled `FAILED` with the same record;
  the gate reran green. A residue retirement has two sides, the job and its outbox row.

### Known limits (design 10.8 as corrected by 10.11 M; L1–L4, L6–L10, L12–L14, L17–L19, L22, L24 as design §7)

- **L37.** The offsite copy is not implemented; it is card **37d**. **Until then, losing the host or its disk loses
  every backup: host loss = total loss.** Said by every arm, `backup status`, the drill evidence, `BackupNotOffsite`
  (weekly) and the delivery report.
- **L5.** Single node: arms run `docker compose exec` in `pg`. Upgrade: a dedicated repository host or the provider's
  backup (card 39).
- **L11.** `verify --set` reads from local disk with no egress.
- **L15.** The PITR window is 24–48 h, reported by `pitr_window_unbroken_since`.
- **L16.** There is no object-store class. Adding one is a PUSH class (`repo2-type=s3`, `archive-async=y` with a
  spool, its own schedule, per-repo receipts); trigger: offsite RPO must beat 37d's pull interval, or no NAS can be
  bought.
- **L20.** `verify --set` covers a set and its WAL to consistency; later WAL is proven by the weekly drill (T-L2).
- **L21.** A compromised server (root or docker access) can wipe or corrupt the repository and forge receipts.
  Nothing in card 37 survives that; 37d's NAS pull with NAS-side snapshots is the layer below.
- **L25.** Every copy is on the database host's disk. The backup filesystem is separate from the root filesystem
  (survives Docker cleanup and a full root filesystem), not from disk or host loss.
- **L29.** A verify-FAILED set is kept until the owner removes it (`pgbackrest expire --set=<label>`, an E16 deletion
  decision); it counts toward retention and budget meanwhile.
- **L31.** Dropped WAL is not detected byte by byte; the latch keeps `WalArchiveFailing` on from the first observed
  failure until a later full is VERIFIED, and `status` prints the unbroken window.
- **L33.** `repo_bytes` is as old as the last backup arm run, refusals included (≤ 24 h); growth between runs is
  bounded by the 12 GiB backup filesystem and watched by live `df` (`free_floor` headroom).
- **L34.** A failure is latched by the first `DR_EVIDENCE` cycle that runs after it while `pg_stat_archiver` still
  remembers it; a crash that resets the counters before that cycle loses the observation (no WAL is lost by a failure
  PostgreSQL retried to success). A latch needs the daemon running; a daemon outage is alerted by card 35.
- **L38.** `repo_intact` covers the `backup/` tree (names, sizes) and both info files; the `archive/` tree changes
  during a drill and is not covered — a removed WAL segment fails the next drill's restore (T-L2) instead.
  `// ponytail: a backup arm that rewrites backup.info during the drill reads as a false red, never a false green`.
- **L39.** `BackupNotOffsite` is constant; an Alertmanager restart that loses its notification log sends it once more
  before the 168 h repeat (a repeated true notice).
- **L40.** The backup filesystem is a fixed image; growing it is the offline procedure in runbook §11.5. Upgrade: a
  separate disk or partition mounted at the same path (no code change).
- **Deletion bound (D-T, §44):** 3 d after the last set holding the data expires; no NAS term exists yet.
- L14 in plain words: lose the backup password and every backup is unreadable — hence go-live step 3.
- L13: RTO and RPO are measured on dev-sized data (above); re-measure at the first production drill.

### E16 table: owner said X, design does Y, because Z, cost (rows 1–4, 6–9 of design 9.11, rows 12 and 13 of 10.8 / 10.11 M)

| # | owner said | design does | because | cost |
|---|---|---|---|---|
| 1 | keep only the latest backup | keeps the **two** newest checked daily fulls | with one, the undo window drops to about zero every night at 00:30; with two you can go back 24–48 h | one more full ≈ F (measured 149 MB today) |
| 2 | backups must not fill the disk | hard repo cap + free floor checked **before** writing; WAL queue capped at 2 GiB; `DiskFreeLow` critical at 7.5 GiB; six visible gauges | the service stays up first (E15) | a refused backup leaves you on yesterday's backup until you act |
| 3 | (implied) small footprint | daily **full**, zstd, bundling; no incremental chains | at ~1 GB a full takes seconds (M1: 3.9 s); independent sets are simpler to check | more CPU per night |
| 4 | — | `archive_timeout` 3600 s | forced WAL ≤ 384 MiB/day uncompressed (measured 16.6 kB/day idle, SP-9) instead of 4.5 GiB/day | at light load up to 1 h of changes lost on a total loss |
| 6 | — | backups encrypted from day one | encryption cannot be switched on later; the NAS must hold ciphertext only | one password kept offline; card 37d adds the one-line proof |
| 7 | — | `BackupNotOffsite` is a WARNING repeated **weekly**, never silenced | E14: say so continuously; a page every 4 h would train you to ignore the channel that also carries the disk warnings | one weekly reminder until the NAS works |
| 8 | — | S3 / object store not built | nobody has a bucket; untested code rots | a later card (L16) |
| 9 | — | the backup directory is a plain host mount, not a Docker volume | `docker compose down -v` or volume cleanup on this host can never delete your backups | one root runbook step at go-live |
| 12 | "if backups are needed, a NAS will be bought later as the backup storage" (E14); the main line first promised adding it would be a configuration-only switch | card 37 ships `local_only` only; the NAS pull mirror is deferred to card **37d**, which starts when the NAS exists and its model is known | the NAS model is unknown, so the NAS-side script, scheduler and tools cannot be fixed or tested against the real thing; mirror code would only be exercised against containers and every later card would pay its gate time; the repository is already an encrypted posix store, so 37d pulls existing ciphertext without re-creating it | one follow-up card of about one and a half days when the NAS exists; adding the NAS is a code card, not a configuration change — the earlier promise is withdrawn; until 37d, host loss = total loss (L37) |
| 13 | "backups must not take much disk space; never let backups fill the disk" (E15) | the backup store is its own fixed 12 GiB filesystem | archived WAL has no other hard cap, and a refused or failed backup also stops expiry; with the boundary, a full backup store stops archiving and pages instead of stopping PostgreSQL | 12 GiB of the 146 GB disk reserved at go-live (≈ 13 % of today's 93 GB free) and one root runbook step; WAL beyond the 2 GiB queue is dropped (PITR gap until the next verified full) only after the backup store is full |

### Rejected

- **An `archive_command` wrapper** (finding F1's alternative): it puts custom code in the only WAL path and needs its
  own drop marker read by the daemon; the filesystem boundary is a native feature pgBackRest and PostgreSQL already
  handle (`archive-push-queue-max`).
- **Retention 1** (the undo window collapses nightly); R-37's **weekly full / daily diff / 6 h incr with retention 4**
  (peak 5·F plus chains); **`repo-block=y`** (pays only for diff / incr); **time-based retention**; **expire on
  FAILED**; a **35-day** deletion bound (D-T now 3 d).
- **OSS / S3 / SeaweedFS / a TLS CA / three key pairs / a `pgbackrest` runner service / `--repo-class` / `local_test`
  / an endpoint resolver / a drill S3 key and PUT probe / `restore_drills.target`** (E14, design 9.10): no object
  store exists; posix has no key to isolate; a configured class can never raise a claim.
- **An S3 class kept as optional configuration** (E14 item 3, third bullet): not free to keep — untested code rots
  (L16 is the path).
- **A Docker named volume for the repository** (`down -v` / `volume prune` would delete every backup; T-L1's teardown
  shows the bind survives `down -v`).
- **Qdrant snapshots now** (D-S): snapshots live on the volume that is being lost; shipping them is a second
  artifact pipeline with its own PITR proof, for an RTO the rebuild already meets (M2).
- **A `target_generation` column on stream_log** (its 18 columns are frozen; the side table instead), a **serving
  switch** for the in-place generation, a **dense-query recall parity** (E3 + E4 + E5 instead), and the plan's
  **DONE → ISSUED reset** and `humaux-admin backup run` (E2).
- **The Qdrant-scroll backfill of legacy vectors** (no fingerprint can be proven for `embed-v1`; `--allow-reembed`
  instead, L3).
- **Everything design 10.1 MOVEs to card 37d** (not rejected — deferred; see "Scope under E17").

### Follow-ups

- **37b** — the re-embed generation under a new label and the gateway's fingerprint refusal (RQ-6's gateway half, E3).
- **37c** — `hold:no_verified_backup_covering_leaf` in card 36's `partition_drop_check` (a new migration), widening the
  retention CHECK with `'EVENTS'`, and replacing card 36's COPY export with the PITR coverage proof (E6, two-reviewer
  data-loss pass).
- **37d** — the NAS pull mirror with design section 9 as its blueprint (10.9): starts when the owner has the NAS; its
  first precondition is `backup check` → `weak_subkeys=0` on this card's repository, unchanged.
- **39** — the deploy-artifact offsite list of §67.4 / G80-20 (incl. `deploy/images`, `deploy/pgbackrest`,
  `deploy/compose/{backup,drill}.yml`) (E11).
- Open point for the main line (S3 deviation 1): the operator CLI's verifier reads under the retrieval worker's DSN;
  accept the peer DSN in the operator shell, or add an owner read definer for role_maintenance later.

### Addenda to earlier ADRs

- **ADR-0057 D-C.** A `StreamCountFilter` now reaches the count **and the rebuild verifier's scroll**
  (`qdrant::scroll_stream_points`), never search. The stale-upsert-after-retire point (ADR-0057 limits 9–10) is now
  **detected** by `projection verify` (an orphan, E3) and removed by `projection rebuild` (the quiescent orphan step).
- **ADR-0061.** Six more families come from the maintenance daemon (`telemetry::dr`); the zero state before the first
  receipt (`backup_last_success_timestamp_seconds{target="local"} 0`, headroom 0) is truthful: BackupFailure fires.
- **ADR-0062.** The daemon gains the cluster-level task `DR_EVIDENCE` (label `dr_evidence`, cadence key, no LIMIT).
- **ADR-0063.** Vectors join card 36's retention decision as purge-by-UPDATE with the point (no table_key, no DELETE
  door, tombstone rows kept); L8's "replace the COPY export by a PITR coverage proof" and the EVENTS hold are card 37c.
