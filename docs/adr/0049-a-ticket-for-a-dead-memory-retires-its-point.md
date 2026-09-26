# ADR-0049 — A ticket for a dead memory retires its point; an Evidence is one overlay object

- Status: accepted (card 24, 2026-09-26)
- Supersedes ADR-0018 §4's projection clause ("re-upsert the point's payload with
  `status='superseded'`, settle DONE") for memories that are no longer live. Does not touch
  ADR-0018's confirm gate, its supersede predicate, or ADR-0020's lifecycle log.
- Spec: §7/§15 (every ticket has a row and is settled), §15.4 (contiguous DONE prefix),
  §15.5 (read-your-writes overlay: one Evidence, its live `processing_state`, 0/1/N memories),
  §17.5 (correction/delete/supersede writes use strong ordering), §36 (`memory.supersede`).

## Context

Card 24's rewritten rehearsal (two tenants, subject scope, supersede → restore, kill -9
rotation, then a 380 s soak) ran twice on 2026-09-26 and failed identically:
`33 passed, 5 failed`, soak verdict FAIL with **31 of 33 recalls failed** and
`projection_promoted` red for one of two streams. The dev database showed why, in one row:

```
stream_seq | state  | error_class     | event_type        | evidence
         1 | DONE   |                 | EVIDENCE_ACCEPTED | 01a0da83-61fd…
         5 | FAILED | registry_failed | MEMORY_LIFECYCLE  | 01a0da83-61fd…   ← supersede
         6 | DONE   |                 | MEMORY_LIFECYCLE  | 01a0da83-61fd…   ← restore
```

Two defects, one visible through the other:

1. **The supersede ticket always fails.** ADR-0018 §4 designed the projection consequence of
   a supersede as "resolve the same memory, re-upsert its point with the current
   `status='superseded'`, settle DONE", and honestly recorded that no Qdrant-backed worker
   test had proven it. Migration 0119 (the private projection registry, card 18) later made
   every registration pass `current_source_matches`, which refuses a source whose `status` is
   not `active` or whose `superseded_by` is set (`SourceNotLive`). `finish_row` maps every
   non-collision registry error to `registry_failed`. So the ticket ADR-0018 issues has been
   un-settleable since card 18; the FAILED row wedges the §15.4 prefix (`projection_highwater`
   stays at 4 forever), and nothing before the rehearsal's lifecycle step ever ran a real
   supersede through the worker.
2. **An Evidence with a lifecycle row behind it is rejected as a "conflicting copy".**
   `pg_delta_overlay_in_txn` recovers `evidence_id` by joining `ops.outbox` on `commit_seq`,
   so the overlay range above the (wedged) serving highwater carries the same Evidence once
   per row — 5 and 6 above, later 1 as well. `private_read_serving_candidates` treated a
   second row with a different `stream_seq` as `ConflictingOverlayEvidence`, so **every
   token-carrying recall on that stream failed** with
   `materialize_failed class=read-your-writes overlay repeats an Evidence with conflicting
   state` — the rehearsal's own read-your-writes probe (leg B) and all 33 soak recalls.
   `bins/gateway/tests/mcp_gateway.rs` had documented the scenario as "the worker-less
   limitation the card names" and placed its overlay leg *before* the supersede/restore pair
   to avoid it.

## Decision

### D-A — A memory that is no longer live is retired, not re-projected

`projection_worker::resolve_and_embed` partitions the ticket's memories by
`status == Active`. Live memories take the unchanged path (card → seal → embed → upsert →
register → verify). A dead memory (superseded, revoked, expired) gets no card, no vector and
no registration; its consequence is `retire_row`:

1. `private_projection_registry::retire_points_for_memory` — in one worker transaction,
   `SELECT` every point id this family/version ever bound for the memory (live or already
   retired), then `UPDATE … SET projection_live = false, retired_at = now()` on the live ones.
   Returning *every* id, not just the ones flipped, makes the next step safe to repeat after a
   failure between the two writes.
2. `qdrant::delete_points` — `POST /collections/{name}/points/delete` by id, `wait=true`,
   `ordering=strong` (`QdrantOperation::CorrectionDeleteSupersede`, §17.5's class for exactly
   this write, defined but unused until now).
3. `DONE`. A ticket whose memories are all dead settles `DONE` on the retirements alone; a
   mixed ticket folds retirements into the same "any success is DONE, first failure is FAILED"
   rule `process_row` already applies to index writes.

Why not ADR-0018's "re-upsert with `status='superseded'`": the registry's invariant — a
binding is a *live* PG source — is the newer, stricter decision and the one the reader's PG
re-check (`resolve_private_memory_points`, which "silently omits revoked/superseded rows")
relies on. A superseded memory was therefore never *served* even while its point sat in the
index; what was broken was settlement and hygiene, and retirement fixes both without adding
a second notion of liveness (a Qdrant payload flag the reader would have to trust).

Restore keeps the memory's identity: `restore_atomically` flips `status` back and leaves
`updated_at` alone, so the restore ticket computes the SAME deterministic point id and finds the
binding D-A just retired. `register_private_memory_point` therefore treats an identical
registration of a retired binding as a **revive** (`projection_live = true, retired_at = NULL`,
outcome `RegistrationOutcome::Revived`) — the first rehearsal on D-A alone answered
`AlreadyRegistered` on the retired row and the restored memory was unresolvable
(`restored_memory_is_servable_again = 0`, 2026-09-26 06:38). The point itself is re-upserted by
the unchanged live path before registration, and verified visible after it.

### D-C — The ticket settles its carrier row

A MEMORY_LIFECYCLE / MEMORY_PUBLISHED `ops.outbox` row exists only to bind `evidence_id` to
its ticket (ADR-0018 §4; the distiller claims `EVIDENCE_ACCEPTED` only). Nothing ever moved
it off `PENDING`, so every "backlog drained" measure — the soak's `backlog_drained`, the
rehearsal's `the_only_undrained_write_is_the_ryw_probes_own` — counted each supersede,
restore, archive and rollup as open work forever (dev DB, 2026-09-26: 9 lifecycle + 47
published rows PENDING). `projection_worker::settle_row` now marks the carrier row `DONE`
(`processed_at = now()`) in the same transaction that writes the ticket's terminal state,
keyed by `commit_seq` and filtered by event type — `role_retrieval_worker` already holds
`UPDATE` on `ops.outbox` (rls-check MATRIX), and `EVIDENCE_ACCEPTED` rows stay the
distiller's.

### D-B — The overlay's identity is the Evidence, described by its newest row

`pg_delta_overlay_in_txn` collapses the range to one `OverlayCandidate` per Evidence, keeping
the row with the highest `stream_seq`: its `processing_state` is the settledness of the
newest ticket (an `ISSUED` lifecycle row reads as "a change to this Evidence is not yet
projected" — the honest read-your-writes signal), and `memory_ids` is the same live-memory set
on every row because the SQL reads it from `private.memory_records`, not from the row.
`private_read_serving_candidates` applies the same newest-wins rule for any other producer;
`ConflictingOverlayEvidence` survives only for the case it was written for — the *same* row
arriving twice with different facts.

## Tests

- `crates/adapters/tests/projection_worker.rs::a_superseded_memory_ticket_retires_its_point_and_settles_done`
  — two memories projected, one superseded the way `memory_governance_repo` writes it
  (G59-4: `status` and `superseded_by` flip together), a re-issued lifecycle ticket → `(done,
  failed) = (1, 0)`, ticket `DONE`, `projection_highwater` past it, the binding
  `projection_live = false`, the point gone from the index and the sibling's still there.
  Then the restore leg: status back with the same `updated_at`, a re-issued ticket → the SAME
  point id, binding live again (`Revived`), the point back in the index, both carrier rows
  `DONE` (D-C). Fault control: route the dead memory through `finish_row` again and it reads
  `(0, 1)` / `registry_failed` — the 2026-09-26 shape; drop the revive and the restore leg reads
  `projection_live = false`.
- `crates/adapters/tests/private_projection_registry.rs` (the retirement helper) — retire →
  identical registration → `Revived` → resolvable again → a second one is `AlreadyRegistered`.
- `crates/adapters/tests/retrieve_read_your_writes.rs::an_evidence_with_a_lifecycle_row_surfaces_once_as_its_newest_row`
  — a `DONE` Evidence row plus an `ISSUED` lifecycle row for the same Evidence, a token at the
  lifecycle seq → one candidate, `stream_seq = 2`, `Issued`, and the serving seam accepts it.
- The rehearsal itself (`rehearse.sh` steps `lifecycle` → `ryw_probe` → `soak`): the
  read-your-writes probe's leg B and the soak's `recall` lane are the end-to-end witnesses;
  their evidence is cited in `docs/ops/delivery_point_report.md` §5.8.

## Acceptance evidence

`gates_card24_rehearsal4.log`, runs 4 and 5 (2026-09-26 07:24–07:47, every binary rebuilt at
07:24): `REHEARSAL VERDICT: 38 passed, 0 failed` twice — `lifecycle: supersede_events=1
restore_undoes=1 recall_hits_after_restore=1`, `ryw probe: token seq=7 serving_highwater=6`
with leg B `isError=False items=5 overlay_seqs=[7]`, `kill9 rotation: recovery_failures=0
stranded_leases=0 duplicate_live_points=0`, soak 16/16 with `recall` 0 and 2 failed calls of
63/62 (both `DEPENDENCY_UNAVAILABLE` while the retrieval worker was SIG9'd by chaos step 4).
Before D-A/D-B the same script read `33 passed, 5 failed` with 33 of 33 soak recalls failed
(`gates_card24_rehearsal2.log`, runs 1–2). The five consecutive runs the card's acceptance
gate asks for are `gates_card24_rehearsal5.log` (delivery report §8.2): 5/5 × `38 passed,
0 failed`, 2026-09-26 07:47–08:44. The full gate chain on the same tree,
`gates_card24_final2.log` (08:44–09:03): 27 gates EXIT 0, `serial_lane` 108/108 including the
lane-tagged registry revive assertion, `secret_scan hits: 0`.

## Open debt

- The Qdrant dense pre-filter still has no `status` clause; correctness rests on the PG
  re-check as before. A dead memory's point now leaves the index, so the only stale points
  are those retired *before* this ADR (none in a fresh deployment; in the dev database, the
  rehearsal tenants' — dropped with them).
- A restore revives the retired binding under the same identity (same `updated_at`, same body),
  so the registry holds ONE row for it. Only a restore that also changed the body or
  `updated_at` would register a second identity next to the retired one — the audit trail the
  table was designed to hold, not a leak.
