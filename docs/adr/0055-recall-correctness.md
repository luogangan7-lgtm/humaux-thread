# ADR-0055 — Recall correctness: true `cand_k` over-fetch, lifecycle payload pre-filter, planner lane substitution, per-stage timing, `limit ≤ top_k`

- Status: Accepted (card 30 implement pass, 2026-09-30, on HEAD `f23b888` = card 29).
- Amends: Baseline §33 Tool 2 "v1 delivery scope" paragraph (planner anchoring and the `limit`
  paragraph); ADR-0044 D-A (the `INVALID_INPUT` planner anchor is retired; the
  `DEPENDENCY_UNAVAILABLE` explicit-`mode` refusal is **kept verbatim**) and ADR-0044 D-B
  (`limit` must-equal becomes `1..=top_k`). Adds one `DegradeCode` variant under §52.3 Q1 /
  §53.2 / §53.4 (`LaneSubstituted`). Builds on ADR-0049 (retired points), ADR-0052 (resident
  runner), ADR-0053 (serving read before the embedding), ADR-0054 (per-request scope — recall keeps
  reading through `authorization.narrow(workspace_id)` + `bootstrap.request_stream`, unchanged).
  **Does not amend ADR-0030 D-D** (verify-2): the mood rerank stays a permutation of the visible
  `top_k` set — see D-A.
- Spec: §23.3 (`candidate_count == cand_k == min(top_k*5, 200)`, `truncated == candidate_count >
  returned`), §22.4, §33 Tool 2, §33.5 (output budget), §52.3, §53.1–§53.4, §55.1, §16.2/§16.3,
  §17.1, §15.5, §37; ADR-0030 D-D (mood re-rank).
- Closes: `docs/ops/system_audit_20260926.md` P1-6, P1-7, RQ-5 (measurement + filed root cause);
  `docs/ops/delivery_point_report.md` §4.1 mislabel.

## Context (read on `f23b888`; line numbers are this HEAD's, the card's are one commit older)

1. **Under-fill.** `bins/gateway/src/recall.rs:391-399` builds `DenseQuery::new(…, retrieval.top_k(), …)`:
   Qdrant is asked for 5 points. `cand_k` (25) exists only in `provenance.profile` (`:590`).
   The PG hydrate gate (`crates/adapters/src/read_materialize.rs:126-176`) then drops archived,
   superseded, secret-backed and tombstoned rows, so every dropped row is a missing answer, and the
   ADR-0030 mood re-rank (`:474-483`) permutes ≤ 5 rows. `request.cand_k()`
   (`crates/retrieval/src/request.rs:172`) and `CompletenessBlock::profile_consistent`
   (`crates/retrieval/src/envelope.rs:694-700`) already encode §23.3 — the gateway bypasses them.
2. **No lifecycle clause in the Qdrant filter.** `crates/adapters/src/qdrant.rs:1264-1288`
   (`DenseQuery::new`) narrows by tenant + visibility + `projection_version` + `embedding_version`
   only. Measured on the shared Qdrant (read-only counts, 2026-09-30):
   `humaux_private_memory_v1_e2e` 3447 points, `status` missing on 0, `status != "active"` on 0,
   `archived` missing on 3447; every `card18_*` / `gateway_*` collection likewise. On a
   100-point collection `must_not[archived == true]` counts **100**, `must[archived == false]`
   counts **0** — a positive clause would blank every existing point.
3. **Planner refusals.** `recall.rs:262-268` returns `INVALID_INPUT` (`query_not_semantic`) for any
   non-`Class(Semantic)` decision. `crates/retrieval/src/planner.rs:203-279` routes on substrings
   (最近/上周/相关/关系/现在/目前/进度/状态/继续/接着/代码/函数/`()`/`::`/`.py`/any quote) and
   `:145-160` makes a bare UUID/sha256 a `DirectGet`. Recall builds its intent with an **empty**
   predicate registry (`recall.rs:258`), so `Enumerate` / `CannotEstablish` are unreachable here;
   reachable are `DirectGet` and `Class(_)`. A `DirectGet` would be classified `Exact`
   (`completeness.rs:724-726`) and `exact_outcome_from_class` returns `Internal` without a census
   (`envelope.rs:643-649`) — so "just let it through" would turn a 400 into a 500.
4. **The explicit-`mode` refusal is `DEPENDENCY_UNAVAILABLE`, not `INVALID_INPUT`.**
   `recall.rs:232-236`, §33 (Baseline:8630-8660), ADR-0044 D-A:60-62, the schema description, and
   `xtask/src/architecture_check.rs::recall_v1_supported_lanes` (which *requires* a
   `DependencyUnavailable` guard naming exactly `enum − x-humaux-v1-supported`). The card's
   "explicit `mode: literal` → INVALID_INPUT" does not match the code it says it keeps.
5. **`limit`.** `recall.rs:275-287`: `limit != top_k` ⇒ `INVALID_INPUT`.
6. **RQ-5.** Rehearsal5 recall p50 1778 ms / p95 2149 ms (n=312) vs ledger query-embedding p50
   228.5 ms (n=368), soak on a **debug** build (`docs/ops/rehearse.sh:97` builds without
   `--release`; every binary path is `…/debug/…`). There is no per-stage timing. The soak's
   `"memory"` bucket is `memory.enumerate` (`xtask/src/soak.rs:1692-1696`); `memory.get` was never
   timed, and `delivery_point_report.md` §4.1 labels that row "`memory` (get)".
   Leading suspect, found while reading: every recall seals its query twice through
   `LocalSecretScanner::scan` — once in the gateway (`recall.rs:332`), once in the retrieval worker —
   and each scan runs `verify_executable` **twice** (`crates/local-secret-scan/src/lib.rs:236-238`,
   `:415-421`: `fs::read` + SHA-256 of the 21.3 MB gitleaks binary) around a gitleaks spawn
   (`:538-560`, measured 0.232 s wall for one `gitleaks stdin` run on this host). That is 4 × 21 MB
   of SHA-256 in an unoptimised build plus 2 process spawns per recall — the right order of
   magnitude for ~1.5 s. The gateway's call is also synchronous inside `async fn search`, so it
   blocks a Tokio worker for its whole duration. Hypothesis only until D-E measures it.

## Decisions

### D-A — Over-fetch `cand_k`, filter, re-rank, truncate last (reuse, no new builder)

`recall.rs` passes `retrieval.cand_k()` (not `top_k()`) to `DenseQuery::new`. Order after the query:
`materialize_private_read_serving_about` (PG authority: can_read, status, superseded, archived,
secret, tombstone, subject, affect) → **truncate** memory items to the profile `top_k` (the
visible set) → `mood_rerank` on that visible set only (`reranked_count` = its size) → **truncate**
memory items to `k = limit.unwrap_or(top_k)` (overlay items — §15.5 read-your-writes `TemporaryEvidence` /
`ArtifactUnavailable` — are never truncated: the token's range bounds them and dropping one would
break RYW) → `accepted_output`. `completeness.candidate_count` stays `candidates.len()` (the real
Qdrant count), `returned = items.len()`, `truncated = candidate_count > returned` (unchanged
derivation). The §23.3 invariant code is reused as the *oracle*: the fixture asserts the JSON
equivalent of `CompletenessBlock::profile_consistent` + `rerank_degradation_consistent`.
`candidate::build_candidates` is **not** called (see Rejected R-1).

*Verify-2 revision (rerank window).* The implement pass reranked **all** `cand_k` survivors before
the cut. `application::affect::rerank_by_mood` sorts purely by descending congruence (dense order
only breaks ties) and a memory without affect data scores the 5000 midpoint, so an annotated,
mood-congruent memory at dense rank 25 beat an un-annotated rank-1 hit and took a top-5 slot: the
rerank decided *membership*, which ADR-0030 D-D forbids ("是置换：不增不减") and the schema's
`mood_congruence` description denies. Card 30's own motive ("the affect re-rank works on ≤ 5
candidates") was under-fill, which the over-fetch + prefilter already fix: the visible set is now a
full `top_k` of live rows. So the window is the first `top_k` survivors in dense order; `limit`
shortens the reranked list afterwards (§33.5), exactly like a plain recall. Witness with real
affect rows: `recall_mood_rerank_permutes_only_the_visible_top_k` (fault: rerank before the
`top_k` cut ⇒ the congruent rank-25 filler is served ⇒ red, observed).

### D-B — Payload pre-filter: `status == "active"` AND NOT `archived == true`; missing flag = not archived; no backfill, no version bump

- **Payload fields.** `status` (string, lowercase `authority_status_wire`, written on every point
  since the first payload shape `d4f91b6`; unchanged). New `archived` (JSON bool), written on every
  upsert by `IndexablePayload::with_archived(bool)` (default `false`, same builder shape as
  `with_subject_ids` / `with_affects` — no `QdrantPointPayload` field, so its 13 constructors in
  tests are untouched). The worker sets it from `private.memory_records.archived_at IS NOT NULL`
  read in `resolve_memories` (table-level SELECT grant already held, 0011:303 / 0140:62).
- **Filter.** `crates/projection/src/dense.rs` gains `STATUS_FIELD`, `ARCHIVED_FIELD` (one-spelling
  constants like `SUBJECT_IDS_FIELD`), one `Condition` variant `NotTrue { field }` ("field is
  missing, null or false"), and `DenseQueryFilter::servable(self)` which appends
  `Eq{status, "active"}` and `NotTrue{archived}`. `DenseQuery::new` calls it unconditionally (every
  dense query is a serving read). Wire (qdrant.rs `condition_to_wire`):
  `{"must":[…, {"key":"status","match":{"value":"active"}},
  {"must_not":[{"key":"archived","match":{"value":true}}]}]}` — the nested `must_not` is what makes
  a missing flag pass (context item 2). `VisibleCountFilter` is **not** narrowed: an archived point
  is indexed and settled `DONE`, so §23.1② A2 must keep counting it.
- **Honesty about `status`.** Under ADR-0049 D-A a non-active memory is retired, never re-upserted,
  so today the `status` clause excludes 0 of 3447 real points. It is kept (card-decided scope, one
  `Eq`) as defence for pre-ADR-0049 superseded upserts and any future re-upsert path, and is
  falsifiable at filter level by a seeded `status="superseded"` point (test below).
- **No backfill, no `projection_version` bump.** Missing = not archived, so every existing point
  stays servable exactly as today; a memory archived before card 30 keeps a flagless point until its
  next ticket re-projects it, and until then the PG gate drops it (today's behaviour) while the 5×
  over-fetch absorbs the loss. A bump is actively harmful here: §16.3 ③ refuses every non-first
  promotion on this deployment (Baseline:4178-4185), so a new version could never become serving.
  No SQL migration (0189 stays free).
- **TOMBSTONED seqs reach the overlay — over this workspace stream's points only.**
  `retrieve::tombstoned_source_seqs` (`retrieve.rs:1279`, today `pub(crate)`) becomes `pub`;
  recall reads it for the serving key before the query and passes it through
  `DenseQuery::in_workspace_stream(workspace_id, Vec<i64>)`, which ANDs
  `DenseQueryFilter::in_workspace` (`workspace_id == W`, `dense.rs`) into the query filter and has
  `dense_query_body` fold the seqs with the existing `tombstoned_seq_overlay_filter`. One indexed PG
  read per recall, timed inside the `qdrant` stage. *Verify-2 revision:* the implement pass folded
  the seqs into a tenant-wide candidate set, but `stream_seq` counts from 1 in every workspace
  stream (`remember.rs:407-412`), so a TOMBSTONED seq N of W1 also dropped every other workspace's
  tenant-shared point with seq N. The narrowing is exact, not a new restriction: the projection
  worker writes payload `workspace_id` = the family's `scope_id` (`projection_worker.rs:754, 941`)
  and `resolve_private_memory_points_in_txn` only resolves points registered under the request's
  `scope_id`, so another workspace's point could never be served from this stream — it could only
  occupy a candidate slot (measured: 25 foreign tenant-shared points on the query vector ⇒
  `returned = 1` of 5 without the narrowing). One builder takes both arguments so the seq overlay
  can never be applied to an unscoped candidate set. Witness:
  `recall_candidate_set_is_scoped_to_the_request_workspace_stream`.
- **The flag is a prefilter, never an authority.** ADR-0049 rejected "a Qdrant payload flag the
  reader would have to trust"; this ADR does not reverse that. The reader trusts nothing from the
  payload: `final_memory_ids_about_in_txn` still decides. A stale flag can only cost fill, never
  leak an archived row.

#### Lifecycle flag matrix (who flips what, through which ticket)

The flag is **derived from the PG row at resolve time**, never set by a specific op — so *any*
ticket that re-projects a memory writes the current truth, and an out-of-order or unrelated ticket
cannot reset it.

| Transition | PG write | Ticket (existing path) | Point consequence | `archived` | payload `status` |
|---|---|---|---|---|---|
| archive | `archived_at = clock_timestamp()` (`memory_governance_repo.rs:1085-1089`) | `MEMORY_LIFECYCLE` (`:1043`) | same deterministic point id re-upserted (live path) | → `true` | `active` |
| unarchive | `archived_at = NULL` (`:1091-1095`) | `MEMORY_LIFECYCLE` (`:1043`) | re-upserted | → `false` | `active` |
| supersede | `status='superseded'`, `superseded_by` (`:321-364`) | `MEMORY_LIFECYCLE` (`:364`) | ADR-0049 D-A `retire_row`: binding retired + Qdrant delete | n/a (no point) | n/a |
| restore | `status='active'`, `superseded_by=NULL` (`:612-710`) | `MEMORY_LIFECYCLE` (`:710`) | ADR-0049 revive: same point id re-upserted | = `archived_at IS NOT NULL` (archive survives supersede→restore) | `active` |
| correct | M1 superseded by M2; M2 inserted (`:1340-1530`) | one `MEMORY_LIFECYCLE` bound to E2 (`:1516-1529`) | M2 upserted fresh; **M1's point is not touched today** (resolves E2 only) — card 31 adds M1's retire ticket | M2 `false` | M2 `active` |
| affect annotate / subject relink | none on `archived_at` | `MEMORY_LIFECYCLE` | re-upsert | carried from PG (never reset) | `active` |
| retire / tombstone (§37) | `stream_log.state='TOMBSTONED'` (`forget_repo.rs:94`) | none new | recall passes the seqs to the `source_stream_seq` overlay (D-B); purge deletes later | — | — |
| secret (`SECRET_MATERIAL`) | at ingest | `EVIDENCE_ACCEPTED` → `SKIPPED_BY_POLICY` | never indexed (`into_indexable` gate) | — | — |
| revoke / expire (no MCP op) | `status` | whichever ticket re-projects | retired per ADR-0049 | — | — |

### D-C — Planner lane substitution: every class is answered by dense; only an explicit `mode` refuses

- While dense is the only delivered lane (§33 Tool 2 v1), `recall.rs` no longer refuses on the
  planner decision. The decision is **recorded** as `provenance.planner_class` (the §20 wire name,
  `QueryClass::as_wire_name()`, e.g. `STATE`, `TEMPORAL`, `ASSOCIATION`, `LITERAL`, `DIRECT_GET`,
  `SEMANTIC`).
- When the decision is not `Class(Semantic)` **and the caller sent no `mode`**,
  `completeness.degradations` carries `LANE_SUBSTITUTED`. This is a fail-open path (a result is
  returned; §52.3 Q1 "有 -> 走 DegradeCode"), so it is a new `DegradeCode::LaneSubstituted`
  emitted only through `abstain()` (§53.1) — counted in `degrade_total{code="LaneSubstituted"}`,
  paired with `testkit/fault/lane_substituted.rs` (§53.4), direction-table row (§53.2 parity), §53.2
  count 10 → 11. **No new `ErrorCode`.**
- An explicit `mode: "semantic"` names the lane that runs: `planner_class` is recorded, no
  degradation (the caller chose dense; nothing was substituted).
- Classification: the dense lane's answer is `semantic_bounded` whatever the planner said. A new
  `envelope::dense_lane_outcome_block(request, inputs, accept)` shares `envelope_outcome_block`'s
  body but classifies under `PlannerDecision::Class(QueryClass::Semantic)` (fixes context item 3's
  `DirectGet → Exact → Internal`). `envelope::dense_lane_substitution(decision, explicit_mode) ->
  Vec<DegradeCode>` is the single place the abstain fires. Context / memory keep calling
  `envelope_outcome_block` unchanged.
- **Explicit `mode` naming an undelivered lane keeps `DEPENDENCY_UNAVAILABLE`** (`recall.rs:232`,
  unchanged). That is the schema-documented reason, ADR-0044 D-A's §52 reading ("the contract admits
  it, this deployment has no implementation"), and the architecture-check gate's required shape.
  Switching it to `INVALID_INPUT` would contradict §52, make a valid enum value look like a bad
  value, and redden `recall_v1_supported_lanes` (outside this card's files). The card's text
  ("keeps the INVALID_INPUT refusal") mis-states the current code; this ADR keeps the refusal that
  exists.
- **§33 anchoring text (revision of Baseline 8641-8643, the only §33 paragraph touched besides
  `limit`):** "planner 的判定只被记录（`provenance.planner_class`），不再决定拒绝：dense 是唯一已交付
  lane 期间，任何判定都由 dense 作答；调用方未指定 `mode` 且判定不是 `SEMANTIC` 时，
  `completeness.degradations` 追加 `LANE_SUBSTITUTED`（§53.2 `LaneSubstituted`，ADR-0055 D-C）。
  只有显式 `mode` 指名未交付 lane 才拒绝，错误码仍是 `DEPENDENCY_UNAVAILABLE`（本节上表，不变）。
  原锚点 `recall.rs:255-261 ⇒ INVALID_INPUT` 作废。" Schema: `recall.schema.json` `query` gains a
  description saying exactly this; `mode`/`limit`/`completeness_request` descriptions get their
  line anchors refreshed.
- `DirectGet` is substituted, not routed to `memory.get` (Rejected R-4).

### D-D — `limit ∈ 1..=top_k` is accepted; `limit > top_k` stays `INVALID_INPUT`

§55.1 forbids a caller choosing *candidate depth*; D-A keeps depth (`cand_k`) and `top_k` coming only
from the profile, and the profile fingerprint does not include `limit`. A smaller `limit` only
shortens the returned list — exactly §33.5's "Humaux does its own context budget / bounded items".
`limit > top_k` stays refused with the existing `limit_not_profile_top_k` operator line (renamed
semantics: "above profile top_k"), so card 16's replay shape (`limit = <n> > 5`, rehearse.sh:703
leg A = 17) is still refused. §23.3's `returned == top_k` is a fixture equality for the default
request; a `limit < top_k` answer reports `returned = min(limit, survivors)` and
`truncated = candidate_count > returned` honestly. Amends ADR-0044 D-B and the §33 `limit`
paragraph (Baseline 8660-8680).

### D-E — Per-stage timing in the envelope and the soak; RQ-5 plan

- `recall.rs` laps a monotonic clock at fixed boundaries and writes
  `provenance.stage_ms = {route, planner, scan, embed, qdrant, hydrate, rerank, assemble}` (f64 ms,
  0.1 ms resolution) plus `provenance.stage_ms.total` (search entry → assembly). The partition is
  exhaustive — every `.await` in `search()` belongs to exactly one stage:
  `route` = narrow + placement + `family_read_state`; `planner` = intent + `prepare_request` + limit
  check; `scan` = gateway `seal_query` (the RQ-5 suspect, deliberately not folded into `embed`);
  `embed` = `embed_query` RPC (worker scan + provider + ledger); `qdrant` = tombstone-seq read +
  `query_dense`; `hydrate` = `materialize_private_read_serving_about`; `rerank` = `mood_rerank` +
  truncation; `assemble` = `visible_index_count` + envelope build. Two stages beyond the card's six
  (`route`, `scan`) because a 1.5 s gap needs attribution, and folding PG round trips into
  "planner" or the scan into "embed" would mislabel the answer. `planner_class` and `stage_ms` are
  inserted into the serialized envelope object in `recall.rs` only — `ProvenanceBlock` is shared
  with context/memory, whose schema is `additionalProperties:false`; recall is validated against
  the permissive `output.schema.json`, so no struct or contract change is needed (Rejected R-5).
- *Verify-2 revision (lap integrity).* A lap attributes "time since the previous lap", so a
  dropped or moved inner lap folded its stage into the next one and kept the sum whole (a deleted
  `Hydrate` lap passed the 90 % test with `hydrate = 0.0`). `StageClock` now checks every lap
  against the fixed `LAP_SEQUENCE` (route, planner, route, scan, embed, qdrant, hydrate, rerank,
  assemble); a clock that deviated reports `stage_ms = {total}` only (operator line
  `stage_clock_out_of_sequence`), which the wiring test, the realqdrant_ryw test and the soak
  sampler all refuse (missing keys, no fake zeros). Witnesses:
  `recall::tests::stage_clock_reports_every_stage_only_for_the_full_lap_sequence` (drop hydrate /
  first route / assemble lap, double a lap) and the wiring test (drop the hydrate lap ⇒
  `stage_ms.route missing` ⇒ red, observed).
- Soak (`xtask/src/soak.rs`): `timed()` takes an op label separate from the tool; buckets become
  `remember`, `recall`, `memory.enumerate` (was `memory`), and a new `memory.get` (the session gets
  the first `memory_id` from its own recall body; skipped, not failed, when the recall returned no
  memory item). Each successful recall adds one sample per `recall.stage.<name>` and one
  `recall.stage_sum`; failed recalls add none (no fake zeros). `op_failure_rate` ignores
  `recall.stage.*` (derived, not calls).
- `docs/ops/rehearse.sh`: build and run from `${REHEARSE_PROFILE:-debug}` (one `BIN_DIR` variable
  replacing the 23 `…/debug/…` paths; `release` ⇒ `cargo build --release`); new assertions
  `recall_stage_sum_covers_90pct_of_recall_p50` (from the soak report),
  `recall_everyday_queries_answered_with_lane_substituted`,
  `recall_explicit_literal_mode_refused_dependency_unavailable`; the card-4 unarchive witness now
  polls (≤ 60 s) and reports `unarchive_to_served_s`; health observations are re-observed before a
  long soak (Known limits).
- **RQ-5 measurement plan (as run).** One release-profile rehearsal with the soak sized for n ≥ 300
  recalls and inside the distill capacity rule (§4 "Capacity"): `REHEARSE_PROFILE=release
  SOAK_SECS=1800 SOAK_SESSIONS=1 SOAK_THINK_MS=14000 SOAK_DRAIN=300 SOAK_CHAOS_SECS=400 zsh
  c30_rehearse.sh measure` (3 lanes × ~15 s/loop ⇒ 351 recalls, ~0.2 Evidence/s < 0.229); the
  same line is the `rehearse_recall_measure` chain gate (label `chain`). Read `recall.stage.*` p50/p95 against `recall` p50/p95 and against the
  ledger's provider latency for the same window. Decision rule: if `scan` + (`embed` − provider
  ledger latency) carries the gap, the root cause is the per-scan executable re-hash + per-call
  gitleaks spawn (context item 6) — **outside this card's files**; file it as: "hash the pinned
  gitleaks binary once at `LocalSecretScanner::new` and re-verify only on (dev, ino, len, mtime)
  change; run gateway `seal_query` under `spawn_blocking`" (crates/local-secret-scan/src/lib.rs
  :225-240, :415-421; bins/gateway/src/recall.rs:332). A debug-vs-release pair of runs is the
  cheap discriminator (hashing collapses ~10–30× under optimisation; a provider round trip does not).
  Results go into the Measurements section below with n and units.

## Tests (exact names)

`humaux-projection --lib` (dense.rs): `servable_filter_excludes_archived_true_and_non_active_status`;
`servable_filter_passes_a_point_with_no_archived_field`; `servable_filter_keeps_tenant_and_visibility_first`;
`in_workspace_filter_drops_another_workspaces_tenant_shared_point` (verify-2).
`crates/projection/tests/no_handwritten_filter_scan.rs`: `Condition::NotTrue {` added to both pattern lists.

`humaux-adapters --lib` (qdrant.rs): `indexable_payload_writes_archived_false_by_default`;
`indexable_payload_with_archived_true_writes_json_true`;
`dense_query_body_requires_status_active_and_nests_must_not_archived_true`;
`dense_query_body_folds_tombstoned_seq_overlay_next_to_has_id_overlay` (verify-2: also pins the
trailing `workspace_id` term `in_workspace_stream` adds).
`crates/adapters/tests/qdrant_contract.rs::dense_query_body_keeps_scope_version_tombstone_and_shard_in_one_request`
updated (must[4]/must[5]).
`crates/adapters/tests/qdrant_live.rs::dense_query_never_returns_an_archived_or_non_active_point`
(real Qdrant: 10 points, 4 `archived=true`, 2 `status="superseded"`, 4 flagless; limit 10 ⇒ exactly
the 4 flagless ids) — the "no archived id in candidates" witness; fault: drop `servable()` ⇒ red.

`humaux-adapters --test projection_worker` (real PG + Qdrant):
`archive_ticket_reupserts_the_same_point_with_archived_true`;
`unarchive_ticket_reupserts_the_same_point_with_archived_false`;
`restore_of_an_archived_memory_revives_the_point_with_archived_true`;
`annotate_ticket_on_an_archived_memory_keeps_archived_true`;
`correct_ticket_projects_the_successor_with_archived_false_and_status_active`;
existing `a_superseded_memory_ticket_retires_its_point_and_settles_done` (supersede row).

`humaux-retrieval --lib`: planner.rs `everyday_queries_keep_their_planner_class`
(目前项目进度→STATE, 客户张三最近的情绪怎么样→TEMPORAL, 和支付相关的决定→ASSOCIATION,
`the "frozen contract" decision`→LITERAL, bare UUID→DIRECT_GET; the gateway fixture pins
`a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5`, see Known limits);
envelope.rs `dense_lane_substitution_is_empty_for_semantic_or_explicit_mode_and_lane_substituted_otherwise`,
`dense_lane_outcome_classifies_direct_get_as_semantic_bounded_not_exact`.
`humaux-telemetry --lib`: `DegradeCode::ALL.len() == 11`, `direction_table_has_eleven_fail_open_rows_and_one_fail_closed_row`.
`humaux-testkit --test fault_main`: `lane_substituted_increments_counter_and_reports_both_forms`.

`humaux-gateway --test mcp_gateway` (`#[ignore]` lane a, real Qdrant + PG):
- `recall_archive_fixture_fills_top_k_from_cand_k_over_fetch` — 30 near-duplicates (25 archived:
  PG `archived_at` set **and** payload `archived=true`, vectors = the query vector so they outrank
  everything; 5 live: slightly perturbed) + 20 live filler (further away). `mood_congruence` set.
  Asserts, after first requiring a 200 with non-empty items: returned == 5, all 5 are the live
  near-duplicates, no archived id in items, `candidate_count == 25 == provenance.profile.cand_k`,
  `reranked_count == 5` (verify-2: the visible top_k), `truncated == true`. Faults: revert to `top_k()` ⇒ candidate_count 5 ⇒ red;
  drop `servable()` ⇒ the 25 archived fill the candidate set, hydrate drops all ⇒ returned 0 ⇒ red.
- `recall_supersede_chain_of_six_never_returns_a_superseded_row_and_fills_top_k` — M1→…→M6 chain
  (M1..M5 superseded in PG; their points left in place as the pre-retire race, three with payload
  status `superseded`, two still `active`) + 24 live filler: returned 5, no M1..M5 id, M6 present.
- `recall_everyday_queries_are_answered_by_dense_with_lane_substituted` — the five queries above,
  no `mode`: each 200, non-empty items, `degradations ∋ "LANE_SUBSTITUTED"`, expected
  `planner_class`, `completeness.class == "semantic_bounded"`, never `INVALID_INPUT`. Fault: restore
  the planner refusal ⇒ red.
- `recall_explicit_undelivered_mode_is_refused_and_explicit_semantic_is_not_substituted` —
  `mode` ∈ {literal, state, temporal, association} ⇒ `DEPENDENCY_UNAVAILABLE` (schema-documented);
  `mode:"semantic"` + a TEMPORAL query ⇒ 200, `planner_class == "TEMPORAL"`, no `LANE_SUBSTITUTED`.
- `recall_tombstoned_seq_never_reaches_the_candidate_set` — 26 points, the best-scoring one's
  `source_stream_seq` born `TOMBSTONED` in `projection.stream_log` (the §6.2.2 transition guard
  refuses an owner-side DONE → TOMBSTONED rewrite) and with no `ops.outbox` row, so the PG gate
  cannot see the tombstone and only the overlay excludes it; `mood_congruence` set:
  `candidate_count == 25`, `reranked_count == 5`, the tombstoned memory absent. Fault: drop the
  seqs in `in_workspace_stream` ⇒ the tombstoned memory is served first ⇒ red.
- `recall_mood_rerank_permutes_only_the_visible_top_k` (verify-2) — 5 near + 20 filler live
  memories; real `private.memory_affects` FRUSTRATION rows on dense rank 5 and rank 25 matching the
  reader's mood exactly. Plain recall: the 5 near, rank 5 not first. Mood recall: rank 5 first, the
  same 5 ids, rank 25 absent, `reranked_count == 5`, `candidate_count == 25`. Fault: rerank before
  the `top_k` cut ⇒ rank 25 served ⇒ red (observed).
- `recall_candidate_set_is_scoped_to_the_request_workspace_stream` (verify-2) — this workspace:
  seq 1 TOMBSTONED (best-scoring point) + 5 live; another workspace: 25 tenant-shared points on the
  query vector, seqs 1..=25. Returns exactly the 5 live, `candidate_count == 5`. Faults: drop
  `in_workspace` ⇒ returned 1 ⇒ red; drop the seqs ⇒ the tombstoned point served,
  `candidate_count == 6` ⇒ red (both observed).
- `recall_with_a_consistency_token_answers_and_a_caller_chosen_limit_is_refused` (existing name kept,
  ADR-0038/0044 anchor it) gains leg 4: `limit: 1` (no token) ⇒ 200, exactly 1 memory item,
  `candidate_count == 3` (the fixture now indexes three points), `returned == 1`,
  `truncated == true`; leg 2 (`top_k + 1`) still `INVALID_INPUT`. Leg 4 sends no token because the
  RYW overlay item also counts in `returned` and would hide the truncation.
- `native_gateway_semantic_recall_real_qdrant_pg_and_ryw_acceptance` (existing, realqdrant_ryw
  gate) gains: `provenance.stage_ms` has the 8 keys + `total`, `planner_class == "SEMANTIC"`.

`humaux-gateway --test semantic_recall_wiring`:
`recall_stage_ms_cover_at_least_ninety_percent_of_in_process_search_time` — wall clock around the
in-process `search()`; asserts Σ stage_ms ≥ 0.9 × wall and `stage_ms.total ≤ wall`. Fault: move
one `.await` outside every lap ⇒ red when that call is slow (the harness injects a 200 ms delay in
its fake embedding port to make the leg observable).

`xtask` (`cargo test -p xtask soak::`): `soak_buckets_split_memory_get_from_memory_enumerate`;
`recall_stage_samples_come_from_provenance_stage_ms_and_skip_failed_recalls`;
`op_failure_rate_ignores_derived_stage_samples`.

Rehearsal (`docs/ops/rehearse.sh`): `recall_everyday_queries_answered_with_lane_substituted`,
`recall_explicit_literal_mode_refused_dependency_unavailable`,
`recall_stage_sum_covers_90pct_of_recall_p50`.

## Rejected

- **R-1 Route through `candidate::build_candidates`.** It is §24's multi-facet reserve/fusion/token
  packer. With one facet (`Dense`) and Qdrant already returning a sorted, unique, ≤ `cand_k` list,
  it is the identity except for token packing, and token estimates need bodies that only exist
  after hydrate. Reuse = `request.cand_k()` for depth + `profile_consistent` as the test oracle.
  Revisit when a second lane lands (card 41/52).
- **R-2 `must[archived == false]`.** Measured: blanks every existing point (0 of 100) until a
  backfill completes. **R-2b Backfill ticket / re-issue for every archived memory.** Unneeded for
  correctness (PG gate) and fill (5× over-fetch); upgrade signal: soak recalls with
  `returned < top_k` while `candidate_count == cand_k` on a material share of recalls ⇒ run a one-off lifecycle re-issue for
  `archived_at IS NOT NULL` memories.
- **R-3 `projection_version` bump.** §16.3 ③ refuses every non-first promotion here; the new
  version could never serve.
- **R-4 Route a bare UUID to `memory.get`.** Makes recall a second direct-get path with a different
  completeness contract; `memory.get` already exists. Plan v2's "UUID routes to DirectGet" is
  superseded by card 30's decided scope.
- **R-5 Add `planner_class` / `stage_ms` fields to `ProvenanceBlock` / `Envelope`.** Shared with
  context/memory (schema `additionalProperties:false`), 4 constructors outside the allowed files;
  recall-only JSON insertion is the smaller correct change.
- **R-6 `INVALID_INPUT` for an explicit undelivered `mode`.** Contradicts §52 / ADR-0044 D-A and the
  architecture-check gate (context item 4).
- **R-7 A new error code for planner classes.** §52.3 Q1: a result is returned ⇒ `DegradeCode`.
- **R-8 Keep `limit` must-equal.** Refuses a budget-narrowing request §33.5 asks Humaux to honour,
  without protecting §55.1 (depth is untouched).
- **R-9 Payload `status` flip on lifecycle (ADR-0018 §4 style).** ADR-0049 already retires dead
  points; a second liveness notion is what it rejected.
- **R-10 Two generic `Condition` variants (`Not` + bool `Eq`) instead of `NotTrue`.** Only one
  shape is needed ("not true, missing passes"); one variant keeps the §17.1 construction-scan
  surface minimal.
- **R-11 Only the card's six timing stages.** Placement/serving PG reads and the gateway
  gitleaks seal would hide inside `planner` and `embed`; explaining a 1.5 s gap needs `route` and
  `scan` as separate stages, so there are eight.

## Known limits / upgrade signals

- `correct`'s M1 keeps its point until card 31's retire ticket; until then it can occupy one
  candidate slot (PG drops it).
- An archive is visible to the prefilter only after its ticket settles (§15 projection lag); inside
  that window the PG gate is the only exclusion.
- **Unarchive is the costly direction.** Until its MEMORY_LIFECYCLE ticket re-projects the point
  with `archived=false`, the point still carries `archived=true` and the prefilter drops it — the
  memory is live in PG but not recalled (a false negative bounded by projection lag; before card 30
  the PG gate made unarchive instant). The card-30 release rehearsal caught exactly this on the
  card-4 witness: a recall issued immediately after `memory.unarchive` returned 0 hits
  (`unarchive_makes_the_row_servable_again: got '0'`). `docs/ops/rehearse.sh` now polls (≤ 60 s)
  and reports `unarchive_to_served_s`. Upgrade path if the lag matters to a client: have
  `memory.unarchive` return a consistency token and teach the §15.5 overlay to re-admit a memory
  whose own lifecycle ticket is still in flight, or have the gateway treat `archived` as a
  prefilter only for memories whose flag is older than the serving checkpoint.
- Tombstone overlay keys on the point's `source_stream_seq`, which a later lifecycle re-upsert
  rewrites to the lifecycle ticket's seq; the PG gate (keyed by Evidence → outbox → stream_log)
  stays exact, the overlay may miss such a point. Upgrade: key the overlay on point id via the
  registry if recalls with `returned < top_k` while `candidate_count == cand_k` show it.
- **`visible` is tenant-wide (pre-existing, outside card 30's files).** §23.1②'s count
  (`retrieve::visible_count_of_version` → `VisibleCountFilter::new`, shared verbatim with the §16.2
  serve switch in `xtask/src/switch_visible.rs`) has no workspace term while the ledger it is
  compared with is one workspace stream; other workspaces' tenant-shared points inflate it and the
  seq overlay subtracts their colliding seqs. The verify-2 two-workspace fixture reads
  `visible = 29` against `done = 6` ⇒ `a2_overshoot_beyond_pending` / `cannot_establish` (the recall
  itself is correct). Fix: count through `VisibleCountFilter::family_probe` on both the read routes
  and the switch, in one change.
- `planner_class` is a keyword heuristic (planner.rs:196-201 ponytail note); it is recorded, not
  trusted.
- **A bare UUID (or any query) holding a run of ≥ 7 digits is refused `FORBIDDEN` before the
  planner is reached** — pre-existing, outside this card's files. The gateway's `seal_query`
  runs `local-secret-scan`'s deterministic `contains_phone_like` (7+ ASCII digits, where `-`, space,
  `(`, `)`, `.`, `+` do NOT break the run; `crates/local-secret-scan/src/lib.rs:479-492`), and
  `scan` maps any rejection to `Forbidden`. A random v4 such as `f21476e9-…-79869668a78d` trips
  it (observed in the full `mcp_gateway` run, 2026-09-30), and so does a date like `2026-09-30`.
  The card's "bare UUID → dense + LANE_SUBSTITUTED" holds only for UUIDs without such a run; the
  fixture and the rehearsal pin `a1b2c3d4-e5f6-4a7b-8c9d-e0f1a2b3c4d5` (max digit run 2). File:
  recall's query should get a query-specific privacy rule (or the rejection a typed
  `INVALID_INPUT`/degradation), not the contribution-privacy phone heuristic's `FORBIDDEN`.
  **Resolved by ADR-0056 (card 30b):** the retrieval seal runs no contribution-privacy rule, so a
  date, an e-mail address, a phone number, a long number or a random UUID in a query or card is
  sealed; only a gitleaks finding is `FORBIDDEN`.
- **Rehearsal health window.** `xtask e2e-seed` writes TEST provider/account health observations
  valid for 30 minutes; a soak sized for n ≥ 300 outlives them and every later distill defers
  ("reasoning route not admitted"). `docs/ops/rehearse.sh` now re-observes the seeded tenants'
  latest verdicts once before the soak for `SOAK_SECS + drain + 900 s` (append-only rows, the
  shape a health prober writes). Found when an earlier card-30 run was stalled 21 min on the cargo
  lock by a concurrent `clippy` and its pst step fell past the window (run aborted, not scored).
  Side effect worth knowing: an aborted rehearsal leaves its tenants' `DERIVED_DISTILL` jobs
  `PENDING` forever once their health expires (not-ready never consumes an attempt), and the next
  rehearsal's single `--distill-once` pass (`JOB_BATCH=8`, oldest first) can spend its whole batch on
  them — the card-30 `chain` run of 16:38 got `memory_records=0` that way. The 31 such rows card
  30's own aborted runs left were backed up (`c30_stale_jobs_backup.csv`, session scratchpad) and
  set `DEAD` (`last_error_class='abandoned_test_run'`); 4 older fixture rows remain.

## Measurements (D-E)

Source: card-30 release rehearsal `measure` (2026-09-30 15:35–16:21, evidence
`delivery-cards-20260903/card30_rehearsal_evidence/measure_run3_n351/`), `REHEARSE_PROFILE=release
SOAK_SECS=1800 SOAK_SESSIONS=1 SOAK_THINK_MS=14000 SOAK_DRAIN=300 SOAK_CHAOS_SECS=400`, three
tenants, real DashScope query embedding. Unit ms; nearest-rank percentiles over successful calls;
`failed_calls = 0` for every row. Stage rows come from each recall's own `provenance.stage_ms`.

| Stage | p50 ms | p95 ms | n | share of recall p50 |
|---|---|---|---|---|
| route (narrow + placement + `family_read_state`) | 2.7 | 51.5 | 351 | 0.3 % |
| planner (intent + `prepare_request` + limit) | 0.0 | 0.0 | 351 | 0 % |
| scan (gateway `seal_query`: privacy rules + 2× exe SHA-256 + `gitleaks stdin`) | 247.5 | 577.1 | 351 | 26.5 % |
| embed (`embed_query` RPC: worker seal + provider + ledger) | 604.2 | 1490.1 | 351 | 64.6 % |
| qdrant (tombstoned-seq read + `query_dense`, `cand_k` = 25) | 4.8 | 23.1 | 351 | 0.5 % |
| hydrate (PG gate over ≤ 25 candidates) | 15.2 | 76.6 | 351 | 1.6 % |
| rerank (+ truncation) | 0.0 | 0.0 | 351 | 0 % |
| assemble (`visible_index_count` + envelope) | 2.2 | 7.5 | 351 | 0.2 % |
| **stage_sum** | **892.9** | **2279.3** | 351 | **95.5 %** (gate ≥ 90 %: PASS) |
| **recall e2e (soak, over the wire)** | **934.6** | **2430.7** | 351 | — |
| provider query embedding (`ops.model_call_ledger`, 6-token rows, same window) | 245.5 | 506.8 | 342 | 26.3 % |
| `memory.get` (first measurement ever) | 34.5 | 271.5 | 351 | — |
| `memory.enumerate` (the old `memory` bucket) | 92.5 | 595.8 | 351 | — |
| `remember` | 86.8 | 380.2 | 351 | — |

Second run, same parameters (`chain` label = the `rehearse_recall_measure` gate, 17:02–17:46,
`REHEARSAL VERDICT: 79 passed, 0 failed`): recall p50 1103.2 / p95 2384.1 ms (n = 345),
stage_sum p50 1052.9 (95.4 %), scan 270.7 / 733.6, embed 716.1 / 1464.5, hydrate 19.8,
qdrant 6.2, route 4.2, assemble 2.5; provider query embedding p50 225.0 / p95 504.0 (n = 339);
`memory.get` 44.7 / 307.2; `unarchive_to_served_s = 2`. Same shape, ~15 % slower host.

Verify-2 run after the rerank-window, workspace-scope and lap-integrity revisions (label `verify2`,
2026-09-30 22:27–23:10, same parameters, evidence `card30_rehearsal_evidence/verify2/`,
`REHEARSAL VERDICT: 79 passed, 0 failed`): recall p50 1010.4 / p95 1731.9 ms (n = 351,
failed 0); stage_sum p50 950.2 / p95 1607.4 (94.0 % of recall p50; gate PASS); scan 251.8 / 389.6,
embed 643.4 / 1161.7, hydrate 17.1 / 58.9, qdrant 5.3 / 21.2, route 3.0 / 250.1, assemble 2.3 / 6.0,
planner and rerank 0.0; every one of the 351 recalls carried all eight stage keys (the
lap-sequence check never tripped); `memory.get` 37.2 / 121.1, `memory.enumerate` 103.0 / 316.2,
`remember` 82.7 / 327.3; `unarchive_to_served_s = 3`. Same shape as the runs above.

**RQ-5 explained.** Rehearsal5's 1778 ms p50 was a **debug** build; the same path in release is
934.6 ms p50 (n = 351). Of that: the provider round trip is ≈ 245 ms (ledger), and the two
secret scans on the query carry ≈ 495 ms — the gateway's own `scan` stage (247.5 ms) plus the
worker's identical seal inside `embed` (604.2 − 245.5 provider ≈ 359 ms = one more ~248 ms scan +
~110 ms UDS RPC and ledger reserve/settle). Everything PG/Qdrant-side is ≈ 25 ms. The remaining
41.7 ms (4.5 %) sits outside `search()` in the guard/HTTP layer. Debug inflates exactly the scan:
the in-process wiring test (debug) times one gateway scan at 381 ms. Root cause, per scan
(`crates/local-secret-scan/src/lib.rs`): `scan_outcome` calls `verify_executable` twice
(:236, :238), each an `fs::read` + SHA-256 of the 21.3 MB pinned gitleaks binary (≈ 60 ms per
hash measured with `shasum`), around a fresh `gitleaks stdin` process (≈ 240 ms wall measured
standalone). Two scans per recall ⇒ 4 hashes + 2 spawns. **Filed (outside this card's files):**
(1) hash the pinned binary once at `LocalSecretScanner::new` and re-verify only when
(dev, ino, len, mtime) changes — saves ≈ 4 × 60 ms per recall; (2) drop the gateway's duplicate
gitleaks spawn for queries (keep the deterministic privacy rules; the worker seals independently
before any egress) or keep one resident scanner — saves ≈ 240 ms per recall; (3) the gateway calls
`seal_query` synchronously inside `async fn search`, blocking a Tokio worker for the whole scan —
`TrustedRetrievalQuery<'_>` borrows the request, so `spawn_blocking` needs an owned query type
from `humaux-retrieval`/`local-secret-scan`; filed with (2).

**Status of the filed items (card 30b, ADR-0056).** (1) **done** — ADR-0056 D-D: hashed once at
`new()`, re-hashed only when `(dev, ino, len, mtime, ctime)` changes. (2) **done** — ADR-0056 D-C:
the gateway seal is deleted, the worker's is the one query seal per recall; the "keep the
deterministic privacy rules" wording is superseded by ADR-0056 D-A (the seal path runs gitleaks
only). (3) **done in the worker** — the gateway no longer scans; the worker's seal runs under
`tokio::task::spawn_blocking` on the owned request, no new query type needed. From 30b on the
`scan` stage times `trusted_query()` only and reads ≈ 0 ms; the worker's seal stays inside
`embed`.
