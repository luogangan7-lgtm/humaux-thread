# ADR-0053 — Production onboarding in `humaux-maintenance`; explicit VerifiedEmpty first activation; PG-authoritative reads without a serving version

- Status: Accepted (card 28, 2026-09-29, implemented on HEAD `be919a2`). Amends ADR-0017 (first activation
  gains a second evidence form). "Measurements" and "Implementation notes" below record the implement pass.
- Spec: Baseline §4.2 (humaux-admin read-only; operator writes in humaux-maintenance), §6.2.1–§6.2.3 / §48.2
  (grants, closed pool set), §6.3 (tenant/membership lifecycle), §15.1–§15.4 (stream ledger), §16.2–§16.3 +
  ADR-0017 (serving switch), §17.3 (placement), §22.4 (cannot_establish), §23.1② (visible), §52.1–§52.3
  (ErrorCode closed at 18), §77 (sensitive admin action audit), §78.1 (no literal defaults), 0117 (four
  admission tiers); ADR-0031 D-A/D-B, ADR-0032 D-A, ADR-0033 (membership path), ADR-0035 (workspace
  membership definer), ADR-0040 (live visible counts), ADR-0050 (executed migration checks), ADR-0052
  (resident runner claims only placed tenants).
- Binding research: R-28 (Humaux memory `[research] 网页 GPT 调研批次 1（2026-09-26，会话 6ab7c509）`,
  TW `research_gpt_batch1_20260926.md` §Q1): A (explicit EMPTY activation by the onboarding control plane,
  `ActivationEvidence::VerifiedEmpty{initialized_head, target_generation, probe_id}`, persisted PROVISIONING
  write gate, DB-derived expected, family-bound read-back, no re-switch after the first batch) + C (PG
  authoritative get/enumerate); B only as a protocol state for optional lanes.
- Closes (delivery_point_report §6): "no production onboarding path"; "reads DEPENDENCY_UNAVAILABLE until a
  manual projection-serve".

## Context (read on `be919a2`, read-only)

- `bins/maintenance/src/main.rs` is a 6-line stub. The only provisioning code is `xtask/src/e2e_seed.rs`:
  `seed_base` :203 (tenant, user, OWNER membership, workspace, workspace membership, reasoning domain,
  entitlement snapshot, API key via `insert_api_key` :302), `seed_extra_workspaces` :331, `seed_lane` :420
  (BYOK route graph with `TEST` health rows — not production), `collection_setup_puts` :615 /
  `ensure_qdrant_collection` :642 (collection + `tenant_id` and `subject_ids` payload indexes),
  `seed_embedding_admission` :757 (the five 0117 rows), `seed_placement` :790. It runs every INSERT through the
  **owner** DSN (`HUMAUX_TEST_PG_DSN`, `postgres` sync client) and refuses any host but `127.0.0.1` (:103).
- §6.2.3: the raw pool lives only in `crates/adapters/src/postgres.rs`; `MaintenanceDbPool::pool()` is
  `pub(crate)`. Any SQL a production binary runs as `role_maintenance` must therefore live in
  `crates/adapters`. `role_maintenance` holds only `R` on `control.*`/`projection.*` (§6.2.1) plus the §6.2.2
  cells: `UPDATE(serving, shadow)` on `projection.stream_checkpoints`, `SELECT, INSERT, UPDATE(state, role,
  updated_at)` on `control.memberships`, EXECUTE on `control.set_workspace_membership` (0162),
  `control.issue_quota_window` (0113), `control.audit_event_insert` (0161), `control.bump_user_security_epoch`.
- `control.tenants`, `control.workspaces`, `control.api_keys`, `control.private_reasoning_domains`,
  `control.entitlement_snapshots`, `control.retrieval_provider_admission_limits`, `projection.tenant_placements`
  are ENABLE+FORCE RLS; `control.users` / `control.user_emails` are not. `control.tenants` already has the 0112
  `tenants_bootstrap_owner_select` (owner may SELECT any tenant). `control.user_emails` has
  `UNIQUE(canonical_email)`; `control.api_keys` has `UNIQUE(prefix)`; 0093 gives the admission table
  `UNIQUE … NULLS NOT DISTINCT WHERE effective_to IS NULL`.
- Tenant names are NOT unique today (36 fixture INSERTs reuse constant names such as `'quota fixture'`), so
  name-based idempotency needs its own key.
- `ux_serving_one` (0065) = `UNIQUE (tenant_id, scope_kind, scope_id, domain, projection_kind) WHERE serving`
  already is R-28's "one serving version per family" index (scope_id = workspace_id for `scope_kind =
  'workspace'`, the only kind the gateway accepts, `bootstrap.rs:405`). No new index.
- Serving read today: `memory.rs::read_scope` :784 → `ContextBootstrap::provisioned_request_stream`
  (`context.rs` :136) → `private_read_projection_selector` → `DependencyUnavailable` when no serving row; the
  same for `context.assemble`; `recall.rs` :350-358 fails `no_serving_projection` → `DependencyUnavailable`
  **after** it already paid for the query embedding.
- Every stream-issuing write — `remember.put` (`remember.rs` :590), supersede/restore/archive/correct
  (`memory_governance_repo.rs` :303, :1520), annotate_affect (`affect_repo.rs` :406), distill (`distill_repo.rs`
  :1036), consolidation (`consolidate_repo.rs` :828) — goes through `remember::issue_stream_log_row` :386, i.e.
  `INSERT … ON CONFLICT DO NOTHING` + `UPDATE projection.stream_checkpoints SET issued_highwater =
  issued_highwater + 1`. That UPDATE is the only way a family's `expected` can move.
- §52.1 has no `INVALID_STATE`. `DEPENDENCY_UNAVAILABLE` on a write is read by the guard as **outcome
  unknown** (`guard.rs::observe_unknown` writes an `UNKNOWN` audit and keeps the receipt open for a retry) —
  wrong for a definite, rolled-back refusal. `CONFLICT` is terminal, audited with its code, HTTP 409.
- `adapters::retrieve::visible_count_of_version` + `VisibleCountFilter::new(scope, version)` count through
  `build_dense_filter`, whose `TENANT_SHARED` arm is tenant-wide: a probe for workspace B would count
  workspace A's tenant-shared points. A family probe needs one extra `narrow_by` term (`workspace_id`).

## Decisions

### D-A Where the provisioning library lives: `crates/adapters/src/provisioning.rs` (no new crate)
One adapters module is the single home of every onboarding write: it calls the owner definers of D-C through
`&MaintenanceDbPool`, ensures the Qdrant collection (the code moved verbatim from `e2e_seed.rs` :584-708:
`qdrant_registry`, `collection_setup_puts`, `ensure_collection`) and takes the empty probe. `humaux-maintenance`
(CLI) and `xtask e2e-seed` (thin wrapper, keeps its 127.0.0.1 guard and the BYOK `seed_lane`) both call it.
A `crates/provisioning` crate was rejected: it cannot hold SQL (no raw pool outside `postgres.rs`, pools are
`pub(crate)`), so it would be a crate of orchestration forwarding every call back into adapters.

### D-B Workspace lifecycle, write gate, error code
- `control.workspaces.lifecycle text NOT NULL DEFAULT 'LEGACY' CHECK (lifecycle IN ('LEGACY','PROVISIONING',
  'READY'))`. `LEGACY` = created outside the state machine (every pre-0185 row, every owner-DSN fixture); it
  is never gated, so no existing fixture or dev tenant changes behaviour. Only `control.onboard_workspace`
  writes `PROVISIONING`; only `projection.activate_empty_family` writes `READY`. No non-owner role holds
  INSERT/UPDATE on `control.workspaces` (rls-check asserts it), so the two definers are the only doors.
- Gate = owner trigger `stream_checkpoints_workspace_write_gate` on `projection.stream_checkpoints`, `BEFORE
  UPDATE OF issued_highwater FOR EACH ROW WHEN (NEW.scope_kind = 'workspace' AND NEW.issued_highwater >
  OLD.issued_highwater)`: `SELECT lifecycle FROM control.workspaces WHERE tenant_id = NEW.tenant_id AND
  workspace_id = NEW.scope_id`; `PROVISIONING` ⇒ `RAISE EXCEPTION 'workspace_provisioning' USING ERRCODE =
  '55000'` (object_not_in_prerequisite_state), DETAIL carries the workspace id only. SECURITY INVOKER (every
  stream-issuing role already has SELECT on `control.workspaces` and its tenant GUC installed), `search_path =
  pg_catalog`, fully qualified. Absent row or `LEGACY`/`READY` ⇒ pass.
- Why this placement: it is inside the write transaction of **every** stream-issuing write in every process
  (gateway, consolidation, private worker), independent of how the scope was derived — card 29's per-request
  governance scope reaches `issue_stream_log_row` and therefore the gate without touching it. It guards exactly
  the quantity VerifiedEmpty depends on (`issued_highwater`). Non-ticket governance ops (pin/unpin/bind/unbind/
  reject/confirm) need an existing memory or candidate; in a PROVISIONING workspace none can exist (every
  Evidence/Memory birth issues a ticket), so they answer `NOT_FOUND` before any write. `subject_register` /
  `subject_link_key` write the tenant-level subject registry, not workspace projection input — out of the
  gate's scope by design.
- Serialization with activation: the trigger fires after the writer holds the checkpoint row lock; activation
  holds the same row `FOR UPDATE` while it checks `issued_highwater = 0` and flips `serving`/`READY`. A writer
  therefore sees either PROVISIONING (refused) or committed READY; it can never bump the head between the
  check and the flip. REPEATABLE READ writers fail 40001 on the row activation updated (safe direction).
- ErrorCode: `CONFLICT` (§52.3 Q2: `INVALID_STATE` does not exist; `CONFLICT` is the registered "state does not
  permit this" code; `DEPENDENCY_UNAVAILABLE` would be logged as outcome-unknown). No new variant, no
  `ConflictReason` (that is the success-shaped refusal of governance ops, not this). Mapping: add `"55000"` to
  the Conflict arm of `operation_receipt.rs::db_error` (remember.put) and `memory_governance_repo.rs::db_error`.

### D-C Doors: owner SECURITY DEFINER functions, EXECUTE `role_maintenance` only
All owned by `role_migration_owner`, `SET search_path = pg_catalog`, fully qualified, `REVOKE ALL … FROM
PUBLIC`, `GRANT EXECUTE … TO role_maintenance` (same chokepoint discipline as 0161/0162/0167). No new table
grant to `role_maintenance`, so §6.2.1's "maintenance writes only where §6.2.2 lists it" stays true.
The INSERT statements are the ones in `e2e_seed.rs`, moved (not copied) into these bodies.

| function | does | idempotency |
|---|---|---|
| `control.ensure_admission_tier(p_provider_id text, p_region text, p_tenant_id uuid, p_purpose text, p_tpm bigint, p_rpm bigint) → boolean created` | one 0117 tier row; never overwrites | 0093 NULLS-NOT-DISTINCT unique ⇒ `ON CONFLICT DO NOTHING` |
| `control.ensure_user(p_email_original text, p_email_canonical text) → (user_id uuid, created boolean)` | `control.users` ACTIVE + unverified `control.user_emails` (§74 verification stays post-go-live) | `UNIQUE(canonical_email)` |
| `control.onboard_tenant(p_name text, p_owner_user uuid, p_workspace_name text, p_reasoning_domain_name text, p_plan_limit bigint, p_period_start timestamptz, p_period_end timestamptz, p_provider_id text, p_region text, p_tenant_tpm bigint, p_tenant_rpm bigint, p_families text[]) → (tenant_id, workspace_id, reasoning_domain_id, created)` | refuses (`55000`, `deployment_admission_missing`) unless the GLOBAL and REGION tiers for `(provider, region)` exist; then tenant (ACTIVE, `onboarding_name = p_name`), OWNER membership, reasoning domain owned by the owner, entitlement snapshot (the `mcp.billable_operations.per_period` shape from `seed_base`, values from params), the TENANT + two PURPOSE tiers via `ensure_admission_tier`, and `onboard_workspace` | `tenants.onboarding_name` UNIQUE: `INSERT … ON CONFLICT (onboarding_name) DO NOTHING`, then owner SELECT (0112 policy) returns the existing ids with `created = false` and nothing else is written |
| `control.onboard_workspace(p_tenant_id uuid, p_name text, p_owner_user uuid, p_families text[]) → (workspace_id, created)` | workspace `PROVISIONING`, `control.set_workspace_membership(…,'OWNER','ACTIVE')`, one `projection.stream_checkpoints` row per family triple (all highwaters 0, `serving = false`) — "initialise family/version" | partial unique `(tenant_id, name) WHERE lifecycle <> 'LEGACY'` |
| `control.issue_api_key(p_tenant_id uuid, p_user_id uuid, p_workspace_id uuid, p_prefix text, p_key_hash bytea, p_scopes text[]) → (api_key_id, created)` | ACTIVE key, `authorization_version = 1`, current tenant/user security epochs read in-function (not 0 as the seed did); refuses unless the user holds ACTIVE membership + ACTIVE workspace membership | `UNIQUE(prefix)`: same prefix + same binding ⇒ `created = false`; same prefix, other binding ⇒ raise |
| `control.revoke_api_key(p_tenant_id uuid, p_prefix text) → (api_key_id, changed)` | `status = 'REVOKED', revoked_at = now()` | already REVOKED ⇒ `changed = false` |
| `projection.ensure_tenant_placement(p_tenant_id uuid, p_projection_family text, p_collection text) → created` | the `seed_placement` row (`SHARED_FALLBACK`, 0/0, `STABLE`) | PK `(tenant_id, projection_family)`; existing row naming another collection ⇒ raise `placement_conflict` (never a silent move) |
| `projection.activate_empty_family(…)` | D-D | receipt PK |

`p_families` comes from `TicketFamily::ALL` in Rust (§78.2: the triple has one owner; SQL never spells
`private_memory`/`PRIVATE_MEMORY`/`v1`). The required-family set of a workspace IS the set of checkpoint rows
`onboard_workspace` created — no registry table. `onboard_tenant` installs `humaux.tenant_id` with
`set_config(…, true)` for the new tenant (RLS WITH CHECK on the FORCE tables); callers of the other functions
install it first and each function asserts `p_tenant_id = current_setting('humaux.tenant_id')` (0176 lesson:
an owner door must not become cross-tenant). The API key hash is computed in Rust by the real §73.5 primitive
`humaux_protocol::edge::compute_api_key_hash(pepper, wire)` by the caller (see Implementation notes); the receipt carries only
`edge::api_key_log_fingerprint(prefix, key_hash)`.

### D-D Empty activation: `ActivationEvidence::VerifiedEmpty`, probe outside, short re-verifying transaction
Pure half (`crates/projection/src/serving.rs`):
```rust
pub enum ActivationEvidence {
    /// §16.3 criterion ① read-back: the candidate's live §23.1② count, tagged with its version (ADR-0040).
    VisibleThrough(Option<(String, u64)>),
    /// ADR-0053: the family has no input at all and the physical index holds nothing for it.
    VerifiedEmpty(VerifiedEmpty),
}
pub struct VerifiedEmpty {
    pub initialized_head: u64,          // DB-derived expected (issued_highwater), never caller-declared
    pub target_generation: GenerationId, // collection name + sha256 of its config subset
    pub probe_id: Uuid,
    pub probe_visible: u64,              // the probe's own count; a criterion no input can fail is not one
}
```
`SwitchCriteria.visible_shadow` becomes `SwitchCriteria.shadow: ActivationEvidence` (the two forms are
mutually exclusive by type — no `Some(0)` hack is expressible). `evaluate_switch` on `VerifiedEmpty` pushes
`EmptyEvidenceOffFirstActivation` unless `first_activation`, `EmptyEvidenceNotEmpty` unless
`initialized_head == 0 && probe_visible == 0`, keeps `OpenGaps`, requires `visible_serving == None`, and keeps
ADR-0017's first-activation benchmark rule (only a proven `Fail` refuses). `VisibleThrough` is today's
behaviour byte for byte.

Generation: `GenerationId = "<collection_name>@<sha256 hex of canonical JSON {vectors.size, vectors.distance,
payload_schema.tenant_id, payload_schema.subject_ids}>"` from `GET /collections/{c}`.
ponytail: a config digest stands in for a physical generation counter; card 37 (R-37 rebuild g+1) replaces it
with a `tenant_placements.collection_generation` column.

Sequence (`adapters::provisioning::activate_workspace`, run per family of `TicketFamily::ALL`):
1. Outside any transaction: read the placement (maintenance SELECT); missing ⇒ refusal `placement_missing`.
   `GET /collections/{c}` ⇒ `g1` (must have both payload indexes and the configured dimension, else
   `collection_misconfigured`). Probe: `POST /collections/{c}/points/count {exact:true}` through
   `VisibleCountFilter::family_probe(ops_scope(tenant, ws), ws, version)` = `build_dense_filter` with
   `narrow_by = [workspace_id = ws, projection_version = v]` — tenant clause injected as everywhere, the
   workspace term removes the tenant-wide TENANT_SHARED arm's other-workspace points. The USER_PRIVATE blind spot
   of a user-less scope is empty by construction: `ws` is a fresh uuidv7 minted in `onboard_workspace`, and no
   ticket can have been issued for it while PROVISIONING (D-B), so no point anywhere can carry it; the PG side
   is checked in step 2 regardless of visibility.
2. Short maintenance transaction: `SET LOCAL humaux.tenant_id`; `SELECT * FROM projection.activate_empty_family(
   p_tenant, p_workspace, p_domain, p_kind, p_version, p_collection, p_generation, p_probe_id, p_probe_visible,
   p_probed_at)`. The definer, in order: lock the workspace row `FOR UPDATE` (refuse `not_provisioning` unless
   `PROVISIONING`); lock the family checkpoint row `FOR UPDATE` (missing ⇒ `family_not_initialized` — never
   COALESCE 0); placement row `FOR SHARE` must name `p_collection` (`placement_missing` /
   `generation_mismatch`); derive `expected = issued_highwater` and require it, `evidence_highwater`,
   `knowledge_highwater`, `projection_highwater`, `count(stream_log)`, `count(ops.outbox ⋈ stream_log on
   (tenant_id, commit_seq))`, `count(projection.private_memory_points)` for the family+version all = 0
   (`not_empty`), `count(processing_gaps) = 0` (`open_gaps`); `p_probe_visible = 0` (`probe_not_empty`); no
   serving row for the family (`already_serving` — or, when a receipt for this exact version exists, return
   `EXISTING` as the idempotent no-op). Then `UPDATE … SET serving = true, shadow = false WHERE <family+version>
   AND NOT serving` (1 row or raise), insert the `projection.family_activations` receipt, and when no
   checkpoint row of the workspace lacks a serving sibling, `UPDATE control.workspaces SET lifecycle = 'READY'`.
   Returns `(outcome ACTIVATED|EXISTING|REFUSED, reason, initialized_head, open_gaps, first_activation,
   workspace_ready)`; on REFUSED it has written nothing.
3. Same transaction, Rust: build `SwitchCriteria{shadow: VerifiedEmpty{initialized_head, …}, first_activation,
   shadow_open_gaps}` **from the returned DB facts** and run `evaluate_switch` (the §16.3 single judgement
   point; a disagreement with the definer is a bug ⇒ ROLLBACK, receipt `evaluator_veto`). Append the §77 audit
   row with `control.audit_event_insert` — the `membership_repo` row shape (actor/reason/ticket/step-up from
   `membership_repo::AdminAction`, SUCCEEDED or DENIED). `GET /collections/{c}` again ⇒ `g2`; `g2 != g1` ⇒
   ROLLBACK (`generation_changed`), else COMMIT. The transaction spans one HTTP GET, nothing else.

`projection.family_activations` (new, 0185): PK = the six family+version columns (FK to
`projection.stream_checkpoints`), `evidence_kind CHECK = 'VERIFIED_EMPTY'`, `initialized_head CHECK = 0`,
`collection_name`, `collection_generation`, `probe_id UNIQUE`, `probe_visible CHECK = 0`, `probed_at`,
`activated_at`, `activated_by DEFAULT session_user`. ENABLE+FORCE RLS tenant policy; `role_maintenance`
SELECT; every other non-owner role `—` (overrides the `projection.*` domain default, so it gets a §6.2.2
column).

No re-switch after the first batch (R-28 (4)): `serving = true` means "the version reads route to", not
"caught up". The card-27 runner advances highwaters; recall reads the serving v1 rows as they land.
`xtask projection-serve` for the same version already prints "already serving; nothing to promote" and
exits 0 (unchanged); it stays the tool for real version upgrades (non-empty path untouched).

### D-E Reads: PG authority for get/enumerate, B shape for recall/context on an unactivated family
- `memory.get` / `memory.enumerate` / `enumerate{candidates}`: `memory.rs::read_scope` stops calling
  `provisioned_request_stream`. It derives `(family, key)` with the pure `bootstrap.request_stream` and calls
  new `serving_repo::family_read_state(pool, auth, &key) → {initialized: bool, serving: Option<String>}` (one
  REPEATABLE READ READ ONLY transaction under `role_gateway`, two `SELECT`s on `stream_checkpoints`).
  `initialized = false` (no checkpoint row for the ledger key) ⇒ `DEPENDENCY_UNAVAILABLE` exactly as today —
  never a synthetic empty stream. Otherwise the route serves from PostgreSQL; `serving` is passed as the
  `Option` that `visible_index_count` already takes: `None` ⇒ `visible: null` ⇒ completeness
  `cannot_establish / index_count_unavailable` (honest: the index was not counted) — never
  `exact`/"complete" on a ledger nobody can compare. With a serving version (every READY family) nothing
  changes: an empty READY workspace answers get ⇒ `NOT_FOUND`, enumerate ⇒ `exact`, total 0.
- `recall.search` / `context.assemble` on `initialized && serving == None`: the B-shaped success envelope.
  Recall checks `family_read_state` **before** the query embedding (no provider egress, no provider budget
  for a read that cannot use it). Uninitialised pair ⇒ `DEPENDENCY_UNAVAILABLE` (unchanged).
  Wire shape (only the fields that differ from a normal empty result):
  ```json
  { "content": {
      "items": [],
      "pipeline": { "projection": { "expected": <ledger>, "done": <ledger>, "deleted": <ledger>,
          "skipped": <ledger>, "visible": null, "open_gaps": <ledger>, "pending": <ledger>,
          "completeness_ratio": null, "current": false } },
      "completeness": { "class": "cannot_establish", "reason": "no_serving_projection",
          "exact": null, "known_lower_bound": null, "lanes": { "semantic": "failed" },
          "candidate_count": 0, "reranked_count": 0, "returned": 0, "truncated": false, "degradations": [...] },
      "provenance": { "projection_version": { "status": "cannot_establish" },
          "embedding_model_id": { "status": "not_applicable" } },
      "freshness": { "class": "unknown", "latest_evidence_at": null, "state_age_seconds": null } } }
  ```
  Ledger numbers are the real §15.4 reads of the initialised key (0 in PROVISIONING because the gate holds,
  not because a default was invented). Contract change: `completeness.reason` enum gains
  `"no_serving_projection"` in `contracts/mcp/context.output.schema.json` (the one embedded canonical
  Envelope) and `CannotEstablishReason::NoServingProjection` in `crates/retrieval/src/completeness.rs` with a
  `pub` constructor in `envelope.rs`; catalog pins and the `final_completeness_count` reason list in
  `bins/gateway/tests/mcp_gateway.rs` grow by one. No new field. B is never mapped to get ⇒ 404 or
  enumerate ⇒ complete empty set (R-28 (6)).
- Reachability after this card: a READY family always has a serving row (D-D), so B appears only for the
  PROVISIONING window (onboarding crashed between step 1 and the READY flip; re-running `activate` or `onboard`
  finishes it) and for LEGACY pairs that wrote but were never activated.

### D-F `humaux-maintenance` CLI (subcommand mode; `--serve` is card 35)
Env (§78.1, no defaults, `env=[…]` headers feed env_vars.md): `HUMAUX_MAINTENANCE_PG_DSN`,
`HUMAUX_MAINTENANCE_CREDENTIAL_PEPPER_HEX` (secret; must equal the gateway's
`HUMAUX_GATEWAY_CREDENTIAL_PEPPER_HEX`), `HUMAUX_MAINTENANCE_QDRANT_HOST`, `HUMAUX_MAINTENANCE_QDRANT_PORT`,
`HUMAUX_MAINTENANCE_QDRANT_CIDR`,
`HUMAUX_MAINTENANCE_EMBEDDING_DIMENSION`, `HUMAUX_MAINTENANCE_PRIVATE_MEMORY_COLLECTION`. No host restriction
(production); e2e-seed alone keeps the 127.0.0.1 guard. Every writing subcommand requires `--actor --reason
--ticket --step-up-auth` (§77, same as `xtask member`), `--trace-id` optional.

| subcommand | writes | receipt (one JSON object on stdout, `"outcome": "created"|"existing"|"refused"`) |
|---|---|---|
| `deploy-init --provider <id> --region <r> --tpm <n> --rpm <n>` | GLOBAL + REGION tiers | tier ids + created flags |
| `onboard tenant --name <n> --owner-email <e> [--workspace <name>] --plan-limit <n> --period-end <rfc3339> --scopes <a,b> --key-name <k> --provider <id> --region <r> --tenant-tpm <n> --tenant-rpm <n>` | T1 = one transaction: `ensure_user` → `onboard_tenant` (→ `onboard_workspace`) → `ensure_tenant_placement` → `issue_api_key` (only when the tenant was created) → audit; then `quota_repo::issue_window`; then collection ensure; then `activate` per family | tenant/workspace/owner user/reasoning domain ids, `api_key_fingerprint`, 4 tiers, placement, collection + both payload index names, `activations[]` (`evidence: "VerifiedEmpty"`, generation, probe_id, probe latency ms), `lifecycle` |
| `onboard workspace --tenant <id> --name <n> --owner <user id>` | `onboard_workspace` + collection check + activate | as above, workspace part |
| `onboard user --tenant <id> --email <e> --role OWNER|ADMIN|MEMBER [--workspace <id>]` | `ensure_user` + `membership_repo::apply` (invite, activate — reused, audited) + `set_workspace_membership` | user id, membership state |
| `apikey issue --tenant --user --workspace --scopes --key-name <k>` | `issue_api_key`, prefix = `"hx" + hex12(sha256(tenant ‖ key-name))` so a re-run is `existing` | fingerprint only; the wire key is printed ONCE, as the single line `Authorization: Bearer <prefix>.<secret>` on stdout **before** the JSON, and only when `created` |
| `apikey revoke --tenant --key-name <k>` | `revoke_api_key` | changed flag |
| `placement ensure --tenant` / `collection ensure` | placement row / Qdrant PUTs | created flags, index names |
| `activate --tenant --workspace [--domain --projection-kind --version]` | D-D (defaults = `TicketFamily::ALL`, never literals) | per-family outcome/reason, `lifecycle` |
| `status --tenant` | none (maintenance SELECTs) | workspaces+lifecycle, families (serving version, highwaters), receipts, placement, key prefixes+status, tiers |

Exit codes: 0 created/existing, 3 refused (named reason), 2 usage, 1 infrastructure. Idempotent means a re-run
writes no row and prints `existing` (the plaintext key is not recoverable; a lost key is revoked and reissued
under a new `--key-name`). Secrets: the pepper and wire key never reach stderr, logs or the receipt; the
receipt is asserted secret-free by test.

### D-G e2e-seed becomes a thin wrapper
`seed_base`/`seed_extra_workspaces`/`insert_api_key`/`seed_embedding_admission`/`seed_placement`/
`ensure_qdrant_collection`/`collection_setup_puts` are deleted from `e2e_seed.rs`; `run` calls
`provisioning::{deploy_init, onboard_tenant, onboard_workspace, ensure_collection, activate_workspace}` through
`MaintenanceDbPool` (already connected at :1102) with the seed's generous tiers (1e9) and its random `e2e…`
prefix; `seed_lane`/`seed_route` (BYOK TEST lane, not production) and `teardown` stay (teardown also deletes
`projection.family_activations` and `control.user_emails` rows, and the tenant's own `onboarding_name`
row goes with the tenant). Printed lines are unchanged, so `docs/ops/rehearse.sh` keeps parsing; its later
`projection-serve` calls hit "already serving" and exit 0. `collection_setup_puts`'s fault sentinel test moves
with the function.

### D-H Harness: `cargo xtask e2e-onboard` (not a rehearse.sh step)
New `xtask/src/e2e_onboard.rs`. Chosen over a rehearse.sh v3 step because the card's assertions are about the
release `humaux-maintenance` binary's receipts and refusals, which a Rust driver asserts field-by-field; it
reuses `soak::{mcp_request, parse_http}` for MCP calls and `e2e_seed::seed_lane` for the one thing production
onboarding does not create (the BYOK distill route; card 52). Steps, each printing `e2e-onboard: <name> PASS|FAIL`:
1. `CREATE DATABASE humaux_thread_c28_onboard_<pid>`; migrate 0001→head; DSNs rewritten to host `localhost`
   (a hostname, not 127.0.0.1); Qdrant collection `humaux_c28_onboard_<pid>`.
2. `humaux-maintenance deploy-init`; `onboard tenant --name t1 …` ⇒ assert every receipt field of the gate
   (tenant, workspace, owner, fingerprint, reasoning domain, 4 tiers, placement, collection + 2 indexes,
   `VerifiedEmpty` activation, `lifecycle = READY`).
3. Start gateway, private worker, retrieval worker `--serve` (release binaries, live DashScope). Immediately:
   get(unknown) ⇒ NOT_FOUND; enumerate ⇒ exact, total 0; recall ⇒ `current = true`, 0 items; context ⇒ empty
   envelope; none `DEPENDENCY_UNAVAILABLE`.
4. `seed_lane` for t1; 5 × `remember.put` (distinct sentinels) with the issued key; poll recall (bounded
   deadline) until all 5 sentinels return; assert `projection-serve` was never invoked.
5. Idempotency: re-run `onboard tenant --name t1` ⇒ `existing`, row counts of the 12 touched tables unchanged.
6. Faults: (a) owner-DSN setup of a PROVISIONING workspace with one stream row ⇒ `activate` exit 3
   `not_empty`; (b) delete the placement row of a fresh PROVISIONING tenant ⇒ `activate` exit 3
   `placement_missing`; (c) `remember.put` on a PROVISIONING workspace ⇒ tool error `CONFLICT`, zero
   evidence rows; (d) two concurrent `onboard tenant --name t2` ⇒ exactly one tenant row, one API key;
   (e) `xtask e2e-seed` with a `localhost` DSN ⇒ refused.
7. Stop only its own children; `DROP DATABASE`; `DELETE /collections/humaux_c28_onboard_<pid>`; exit 0 iff all
   PASS. Wall clock of step 2 (n=3 runs) and per-family probe latency are printed for "Measurements".

### D-I Migrations (0185, 0186; forward, executed single-statement boolean checks per ADR-0050)
- `0185_workspace_lifecycle_and_family_activation` (EXPAND_CONTRACT): `control.workspaces.lifecycle` +
  `workspaces_lifecycle_known` CHECK + `workspaces_onboarded_name_uniq`; `control.tenants.onboarding_name` +
  `tenants_onboarding_name_uniq`; `projection.family_activations` + RLS + grant; the gate trigger + function.
- `0186_onboarding_definers` (EXPAND_CONTRACT): the eight functions of D-C/D-D and their grants.
Exact pre/postchecks are in the change list returned with this design.

### D-J Grants documentation
§6.2.2: one new matrix column `projection.family_activations` (`role_maintenance` SELECT, every other
non-owner `—`) and one bullet for 0185/0186 naming the eight definers (EXECUTE `role_maintenance` only) and
the invoker gate trigger. `xtask rls-check`: MATRIX cells for the column; new `check_onboarding_boundary`
(each function via `check_owner_definer_function(…, &["role_maintenance"])`; trigger present, owner-owned,
invoker, raises 55000; no non-owner role holds INSERT/UPDATE on `control.workspaces` at table or column level).

## Rejected
- `visible_shadow = Some(0)` for an empty family — R-28: changes the unit of the count and lets a caller
  declare emptiness.
- The projection worker auto-activating — ADR-0017: the switch is an explicit ops act.
- Seeds/fixtures presetting `serving = true` — bypasses every check (ADR-0017).
- Table INSERT grants to `role_maintenance` on eight control/projection tables — opens `lifecycle = 'READY'`
  without activation, key minting without membership checks, and eight new §6.2.2 columns.
- `DEFAULT 'PROVISIONING'` for `lifecycle` — silently gates every owner-DSN fixture and every existing tenant.
- `DEPENDENCY_UNAVAILABLE` / a 19th `INVALID_STATE` for the gate — outcome-unknown audit / §52.1 is closed.
- Gate as an explicit Rust check in each repo — N call sites, card 29 would need to re-add it; the trigger sits
  on the one statement every stream-issuing write executes.
- Gate reading `control.workspaces … FOR SHARE` — needs UPDATE privilege and multixacts on a hot row; the
  checkpoint row lock already serialises writer vs activation.
- A `required_families` registry table — the checkpoint rows created at PROVISIONING are the required set.
- A new `one_serving_version_per_family` index — `ux_serving_one` (0065) is that index.
- `crates/provisioning` — cannot hold SQL under §6.2.3.
- Global `UNIQUE(control.tenants.name)` — 36 fixtures reuse constant names.
- rehearse.sh v3 step as the harness — cannot assert the maintenance receipts; e2e-onboard reuses rehearse
  pieces through soak/e2e_seed functions instead.
- Qdrant probe through the existing `VisibleCountFilter::new` — its TENANT_SHARED arm counts other
  workspaces' points and would refuse every second workspace of an active tenant.
- B shape (`current=false`, empty data) for `memory.get` / `memory.enumerate` — R-28: a 404 or a "complete empty set" would assert something the gateway cannot establish; get/enumerate are PG-authoritative (C), B is for the optional lanes (recall/context) only.

## Known limits / upgrade signals
- Generation = collection name + config digest (card 37 introduces a real generation column).
- `LEGACY` workspaces are never gated; activating them goes through `projection-serve` (non-empty path) as today.
- Entitlement period and quota windows are issued at onboarding only; renewal is card 35's `--serve` job.
- `onboard tenant` spans three steps (PG txn, Qdrant, activation txn); a crash leaves PROVISIONING (writes
  closed, reads PG-served / B) and a re-run completes it.

## Measurements

All on the local deployment point (PostgreSQL 18 + Qdrant 1.19 containers, release binaries), 2026-09-29.

- **Onboarding wall clock** (`humaux-maintenance onboard tenant`, the whole CLI run: T1 + quota window +
  collection ensure + activation), `cargo xtask e2e-onboard`, n=3 separate CLI runs on a fresh database:
  **368 ms / 84 ms / 74 ms** (the first run also creates the Qdrant collection and both payload indexes; the
  other two find it). Re-measured in the full gate chain after the 2026-09-29 host reboot (fresh database, same
  harness): **202 ms / 50 ms / 46 ms**, probe **1 / 1 / 1 ms**, activation transaction **6 ms**.
- **Activation probe latency per family** (`GET /collections/{c}` + the exact family count): **19 / 11 / 15 ms**
  (n=3); the short activation transaction (definer + `evaluate_switch` + audit row + generation re-read):
  **7 ms**.
- **Write-gate cost**: the trigger function per `issued_highwater` increment, `EXPLAIN (ANALYZE)` on a
  throwaway database, **p50 0.005 ms, p95 0.009 ms (n=20)** against **p50 0.018 ms** for the whole UPDATE — one
  PK lookup on `control.workspaces` per issued ticket, three orders of magnitude below a `remember.put`
  round trip. The end-to-end write path is the e2e run's 5 × `remember.put`, all accepted, all recalled 35 s
  after the puts through the card-27 runner with no operator action.

## Implementation notes

- **Where it landed.** `crates/adapters/src/provisioning.rs` (D-A), migrations `0185` / `0186`,
  `bins/maintenance` (D-F), `xtask/src/e2e_seed.rs` as the thin wrapper (D-G), `xtask/src/e2e_onboard.rs` (D-H),
  `serving_repo::family_read_state` + the gateway read paths (D-E), `rls_check::check_onboarding_boundary` +
  the §6.2.2 column (D-J).
- **Pre-existing defect found and fixed in place (0186, no edit to 0162).** `control.set_workspace_membership`
  (0162) had no Rust caller; its `INSERT … ON CONFLICT DO UPDATE` must also pass
  `workspace_memberships_self_read` (RESTRICTIVE, `user_id = humaux.user_id`), so an owner-definer call
  without the target user's GUC fails `new row violates row-level security policy`. `onboard_workspace`,
  `issue_api_key` (its workspace-membership check) and `provisioning::onboard_user` set `humaux.user_id` to the
  target user around the call and restore the caller's value.
- **API key minting stays with the caller.** `humaux-adapters` does not depend on `humaux-protocol` (it would
  pull the MCP stack into every adapter consumer), so the §73.5 hash and the log fingerprint are computed by
  `humaux-maintenance` / `xtask e2e-seed`; `provisioning::onboard_tenant` takes a `FnMut(tenant_id) ->
  NewApiKey` that is called at most once, inside T1, only when the tenant was created. The deterministic prefix
  (`provisioning::api_key_prefix`) lives in adapters so issue and revoke agree.
- **`p_families` is a flat `text[]`** of `(domain, projection_kind, projection_version)` triples (sqlx binds no
  2-D arrays); `family_triples()` builds it from `TicketFamily::ALL`. The placement family of a triple is
  `domain || '_' || projection_version`, the `TicketFamily::collection_name` derivation.
- **Audit.** Every write that changed a row appends one §77 row through `control.audit_event_insert` in the
  same transaction (the `membership_repo` shape: actor/reason/ticket/trace/step-up from `AdminAction`,
  actions `ONBOARD_DEPLOY_INIT` (system tenant), `ONBOARD_TENANT`, `ONBOARD_WORKSPACE`, `APIKEY_ISSUE`,
  `APIKEY_REVOKE`, `PLACEMENT_ENSURE`, `ONBOARD_WORKSPACE_MEMBERSHIP`, `FAMILY_ACTIVATE`). An `existing`
  re-run writes nothing, audit included. A refused activation (definer `REFUSED`, `evaluator_veto`,
  `generation_changed`, `placement_missing`, `collection_*`) rolls its transaction back and commits one
  `DENIED` `FAMILY_ACTIVATE` row in a transaction of its own.
- **Refusal names** (SQLSTATE 55000, message = reason, CLI exit 3): `deployment_admission_missing`,
  `owner_not_active`, `onboarding_name_taken`, `workspace_name_mismatch`, `owner_not_member`,
  `user_not_member`, `user_not_workspace_member`, `api_key_prefix_conflict`, `api_key_not_found`,
  `placement_conflict`; from `activate_empty_family`: `workspace_not_found`, `not_provisioning`,
  `family_not_initialized`, `placement_missing`, `generation_mismatch`, `not_empty`, `open_gaps`,
  `probe_not_empty`, `already_serving`; from the Rust half: `collection_missing`, `collection_misconfigured`,
  `evaluator_veto …`, `generation_changed`. A tenant-context mismatch is 42501, a malformed request 22023.
- **Qdrant face.** `HUMAUX_MAINTENANCE_QDRANT_CIDR` joins the D-F env set (the same host + CIDR pair the
  gateway and retrieval worker take; `ResourceEntry` needs the private CIDR the host resolves into).
  ponytail: plaintext only (`tls = false`, like the seed). `ensure_collection` treats a non-2xx create as a
  concurrent creator and re-reads the collection up to 10 × 100 ms (bounded blocking re-read in a one-shot CLI).
  Index PUTs carry `?wait=true` so the generation read right after sees both indexes.
- **Reads.** `family_read_state` returns `{initialized, serving, unserved}`; `unserved` (only when initialised
  and unserved) carries the key's real `LedgerClosure` and §23.3④ pipeline counts from the same RR snapshot,
  which is what the B envelope reports. `ContextBootstrap::provisioned_request_stream` now returns that state
  (initialisation is the admission; serving is an `Option`) and `memory.rs::read_scope` keeps calling it.
  `retrieval::envelope::no_serving_projection_envelope` is the one constructor of B (lanes
  `{"semantic":"failed"}` on both routes); it bypasses `envelope_outcome_block` because G23-6 refuses a
  `cannot_establish` provenance, and records `cannot_establish / no_serving_projection` on `finish`. Context's
  B keeps the same snapshot's `handoff` (a PROVISIONING workspace has no memory, so it is empty there).
- **Gateway tests.** `bins/gateway/tests/read_decoupling.rs` (8 wire tests, counting embedding port).
  `mcp_gateway.rs`: pair D still has no checkpoint row and stays `DEPENDENCY_UNAVAILABLE` on all four routes;
  a new pair E (checkpoint, `serving = false`) is PG-served for get/enumerate and B for recall/context;
  `final_completeness_count` gains `no_serving_projection`.
- **Fault injections run (red, then restored green):** VerifiedEmpty accepted off first activation
  (`serving.rs`, 2 tests red); `"55000"` dropped from `operation_receipt::db_error` (read_decoupling
  `remember_put…conflict` red: INTERNAL); the initialised check dropped from `provisioned_request_stream`
  (`memory_get_on_uninitialized…` red); on a throwaway database, EXECUTE granted to `role_gateway`,
  `UPDATE(name)` on `control.workspaces` granted, and the gate trigger disabled (`rls-check`
  `check_onboarding_boundary` red, exit 1).
