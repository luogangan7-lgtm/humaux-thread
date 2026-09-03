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
  `profile_id`, `policy_id`);
- the semantic-recall placement's `collection_name`/`dimension`;
- three paste-ready `export` blocks (`HUMAUX_CONSOLIDATION_WORKER_*`,
  `HUMAUX_PRIVATE_WORKER_*`, `HUMAUX_RETRIEVAL_WORKER_*`) plus
  `HUMAUX_GATEWAY_EMBEDDING_DIMENSION`, so every rehearsal hop can pick up the exact same
  seeded values (`provider_matches_admission`).

Nothing is written to a file, logged, or stored — the bearer's secret half only ever
appears in this one stdout line.

Seeding also provisions the Qdrant collection this tenant recalls against (mirroring
`bins/gateway/tests/semantic_recall_wiring.rs::create_collection` — `GET` first, skip if
already provisioned, else the two `PUT`s: create-collection then the tenant keyword
index) and inserts its `projection.tenant_placements` row
(`placement_class='SHARED_FALLBACK'`). Without both, `tenant_placement(...)` resolves to
`None` and semantic recall degrades closed with `DependencyUnavailable`.

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
