# `cargo xtask e2e-seed`

Persistent tenant/credential/quota seed for deployment-point rehearsals. This is an ops
tool, not a test fixture: rows outlive the process, teardown is explicit.

## Seed

```sh
export HUMAUX_TEST_PG_DSN='postgres://postgres:devlocal@127.0.0.1:54329/humaux_thread_dev'
export HUMAUX_MAINTENANCE_PG_DSN='postgres://role_maintenance:devlocal_role_maintenance@127.0.0.1:54329/humaux_thread_dev'

cargo run -p xtask -- e2e-seed \
  --pepper-hex <hex> \
  --scopes memory:write,context:read \
  --limit 1000 \
  --processor-id <egress-processor-uuid> \
  --region <region> \
  --service-tier <tier> \
  --endpoint-ref <chat-completions-url> \
  --provider-id <provider text id, e.g. minimax> \
  --provider-model-id <provider model id> \
  --model-revision <model revision, or an explicit placeholder text> \
  --collection <qdrant collection name> \
  --dimension <embedding vector size, e.g. 1024> \
  --qdrant-host 127.0.0.1 \
  --qdrant-port 6333
```

`--qdrant-host`/`--qdrant-port` default to `127.0.0.1:6333` and may name no other host
(same local-only rule as the two Postgres DSNs).

Prints, once, to stdout:
- the six base ids (`tenant_id`, `user_id`, `workspace_id`, `reasoning_domain_id`,
  `api_key_id`, `api_key_prefix`) and the bearer (`Authorization: Bearer <wire>`);
- the PRIVATE_CONSOLIDATE R3 admission lane's own ids (`binding_id`, `binding_version`,
  `credential_id`, `provider_account_id`, `processor_model_id`, `endpoint_id`,
  `profile_id`, `policy_id`) plus `distill_binding_id` — a second policy/candidate/binding
  for purpose `PRIVATE_DISTILL_TEXT` over the same profile (ADR-0016). The private worker
  resolves it by purpose at runtime, so no `*_BINDING_ID` export is printed for it;
- the semantic-recall placement's `collection_name`/`dimension`;
- four paste-ready `export` blocks (`HUMAUX_CONSOLIDATION_WORKER_*`,
  `HUMAUX_PRIVATE_WORKER_*`, `HUMAUX_RETRIEVAL_WORKER_*`,
  `HUMAUX_PRIVATE_WORKER_DISTILL_*`) plus `HUMAUX_GATEWAY_EMBEDDING_DIMENSION`, so every
  rehearsal hop can pick up the exact same seeded values (`provider_matches_admission`).

Nothing is written to a file, logged, or stored — the bearer's secret half only ever
appears in this one stdout line.

Card 28 (ADR-0053): the seed is a thin wrapper over the production onboarding library
(`crates/adapters/src/provisioning.rs`, the same owner-definer doors `humaux-maintenance` uses —
no INSERT of its own for tenant, users, workspaces, keys, tiers or placement). It runs
`deploy-init` (GLOBAL/REGION tiers for `--embedding-provider`/`--embedding-region`, 1e9, never
torn down), onboards a tenant named `e2e-seed-<uuid>` (owner `e2e-seed-<uuid>@e2e.invalid`,
stored unverified), issues the quota window, adds `--workspaces` 2..n with their own keys,
ensures the Qdrant collection with **both** payload indexes (`tenant_id`, `subject_ids`) and then
**activates every workspace empty (VerifiedEmpty)** — a seeded workspace is `READY` and serving
before any write, so the rehearsal's later `projection-serve` calls print "already serving" and
exit 0. Only the BYOK distill lane (`seed_lane`, TEST health rows, not production) is still
inserted by the seed itself (card 52). The printed lines are unchanged. The 127.0.0.1 /
`humaux_thread_*` guard stays on both DSNs: production onboarding is `humaux-maintenance`
(`docs/ops/runbook.md` §3), never this tool.

Teardown additionally removes the tenant's `projection.family_activations`, workspace memberships,
§77 audit rows and the owner's `control.user_emails` row.

## Teardown

```sh
cargo run -p xtask -- e2e-seed --teardown <tenant_id> [--drop-collection <name>]
```

Deletes every row this seed created (lane, then base fixture tables including the
`projection.tenant_placements` row, then the user and tenant), in dependency-reverse
order. `control.processor_models` catalog rows are never deleted — they are global (no
`tenant_id`) and append-only by design; a shared row from an earlier seed run is expected
and harmless. The Qdrant collection itself is **not** dropped by default — it may be
shared with another tenant's placement row — pass `--drop-collection <name>` (with the
same `--qdrant-host`/`--qdrant-port` as the seed run, if non-default) to delete it
explicitly.

## Safety

Refuses to run unless `$HUMAUX_TEST_PG_DSN`'s host is `127.0.0.1` and its database name
starts with `humaux_thread_` — this never touches a production database.

## Distill hop (remember → private-worker → memory_records, ADR-0016)

With the seeded bearer and the `HUMAUX_PRIVATE_WORKER_*` + `HUMAUX_PRIVATE_WORKER_DISTILL_*`
exports in the shell (plus `HUMAUX_PRIVATE_WORKER_DISTILL_BATCH`,
`HUMAUX_PRIVATE_WORKER_DISTILL_LEASE_SECS` and, for `--distill-serve`,
`HUMAUX_PRIVATE_WORKER_DISTILL_POLL_INTERVAL_SECS` — deployment values, no defaults):

```sh
target/debug/humaux-private-worker --distill-once    # one pass over pending EVIDENCE_ACCEPTED rows
target/debug/humaux-private-worker --distill-serve   # resident loop on the poll interval
```

Write Evidence through the gateway's `remember.put` first. One pass claims the tenant's
`ops.outbox` `EVIDENCE_ACCEPTED` rows (PENDING → PROCESSING → DONE, FAILED only for an
unusable Evidence or a fail-closed parse; a not-yet-admitted route or a provider 429/5xx hands
the row back to PENDING for the next pass), leaves one
`private.processing_runs` row per Evidence (fingerprint recorded before the provider call),
and 0..N `private.memory_records` + PRIMARY `memory_evidence` rows whose visibility is the
Evidence's own. The remember-time `projection.stream_log` ticket then resolves in the
retrieval worker (a 0-memory Evidence settles it `SKIPPED_BY_POLICY/no_memory_distilled`).
`--teardown` removes these rows too (they all carry `tenant_id`).

## Second hop (consolidation ⇄ private-worker rehearsal)

With the seeded bearer and the two `export` blocks above in the shell:

```sh
target/debug/humaux-private-worker --serve-rpc   # env: HUMAUX_PRIVATE_WORKER_*
cargo run -p humaux-consolidation-worker -- --run-once   # env: HUMAUX_CONSOLIDATION_WORKER_*
```

Write at least two `WORKSPACE_SHARED` memories through the gateway's `remember.put`
first (using the seeded bearer), then run the two workers above. A successful hop
produces one `private.memory_rollups` row and a `projection.stream_log` row with
`scope_kind='workspace'` and event type `MEMORY_PUBLISHED` for the seeded tenant.
`--teardown` afterward brings every seeded table back to zero rows for that tenant.

- `--embedding-provider <id> --embedding-region <region>` (required): seeds `control.retrieval_provider_admission_limits` rows (purposes embedding + rerank) so projection/query embedding admission succeeds; echoed as `HUMAUX_RETRIEVAL_WORKER_EMBEDDING_PROVIDER/REGION`.
- Admission limits: the seed creates the 0117 canonical tiers for `--embedding-provider` — shared GLOBAL and REGION rows (tenant NULL, created only when missing, never torn down) plus the tenant's TENANT and TENANT+PURPOSE rows (RETRIEVAL_EMBEDDING, RETRIEVAL_RERANK); `--teardown` removes only the tenant-scoped rows.
