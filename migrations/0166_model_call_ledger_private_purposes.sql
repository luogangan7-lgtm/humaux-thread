-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0165). ADR-0042 (card 20):
-- widen `ops.model_call_ledger.purpose` so the two most expensive hops in the system —
-- §11.6 distill and §11.7 consolidation — can register their real provider attempts on the
-- SAME ledger the §19/§20 retrieval hops and the §11.8 contribution hop already use.
--
-- Why (deployment report, card 20 "Why"): 0130's `model_call_ledger_purpose_known` CHECK
-- closes `purpose` to ['query_rewrite','embedding','rerank','CONTRIBUTION_DEIDENTIFY'].
-- Every MiniMax call distill and consolidation make therefore exists ONLY as a §7.4
-- disclosure receipt (`ops.data_disclosures`, purpose PRIVATE_DISTILL_TEXT /
-- PRIVATE_DISTILL_VISION / PRIVATE_CONSOLIDATE) — a receipt that deliberately records WHAT
-- left the boundary, not WHAT IT COST. So the two paths that actually burn provider budget
-- are the two paths with no token/latency/cost row anywhere, and card 16's soak run would
-- spend real money with nothing to show a reviewer. Unmetered LLM spend is a P1 for a paying
-- customer.
--
-- The three new values are LITERALLY the §7.4 disclosure purposes
-- (`adapters::contribution_execution_repo` already maps `PrivateReasoningPurpose` to exactly
-- these strings) so one provider call's ledger row and disclosure row read as the same
-- purpose — no second lookup table, no second vocabulary. Rust side: the closed enum
-- `humaux_domain::ledger::ModelCallPurpose` (§78.2), mirrored by
-- `crates/adapters/tests/model_call_ledger.rs::db_purpose_check_mirrors_the_rust_closed_set`,
-- which parses THIS constraint's deparsed text and compares the set both ways.
--
-- EXPAND only: the CHECK is replaced by a strictly larger set, so every existing row stays
-- legal and no row is read, rewritten or deleted.
--
-- What this migration deliberately does NOT touch:
--   * `model_call_ledger_reasoning_snapshot_shape` (0130) — its second arm already covers
--     "purpose IS DISTINCT FROM 'CONTRIBUTION_DEIDENTIFY' ⇒ every reasoning route column
--     NULL", which is exactly the shape a distill/consolidation row has. The private hops
--     resolve the same `control.resolve_user_reasoning_admission` route, but they are
--     PLATFORM-paid (§19: `actual_cost` is the platform number), not USER-paid, so they must
--     NOT claim 0130's USER billing snapshot. They take the plain retrieval-shaped arm.
--   * `ops.reasoning_model_call_validate` (0130) — returns NEW untouched for any purpose
--     other than CONTRIBUTION_DEIDENTIFY, so the private rows pass it by construction.
--   * `ops.data_disclosure_reasoning_model_call_validate` (0130) — only fires when a
--     disclosure row carries `model_call_id`. The distill/consolidation disclosure rows
--     carry NULL there (`adapters::disclosure::reserve_private`) and keep doing so: the card
--     asks for ledger row AND disclosure row side by side, not for a new binding between
--     them. Binding them would force those disclosures to purpose='USER_REASONING' and to
--     0130's USER-paid ledger arm, which is the opposite of what a platform-paid hop is.
--   * `ops.model_call_ledger_guard_mutation` (0094/0130) — append-only + one-shot finalize
--     is purpose-agnostic and applies to the new rows unchanged (card 20 acceptance:
--     "the 0094/0095 guard-mutation constraints still pass").
--
-- No new table, no new column, no new role, no new GRANT, no RLS change, no DML — so no
-- §6.2.2 matrix row and no `xtask/src/rls_check.rs` MATRIX cell. `role_private_worker`
-- already writes this table today (0130's CONTRIBUTION_DEIDENTIFY reservation runs on
-- `PrivateWorkerDbPool`), and the distill/consolidation legs added by card 20 run on that
-- same pool, so the grant surface is unchanged.

ALTER TABLE ops.model_call_ledger
  DROP CONSTRAINT model_call_ledger_purpose_known,
  ADD CONSTRAINT model_call_ledger_purpose_known CHECK (
    purpose IS NULL OR purpose = ANY (
      ARRAY[
        'query_rewrite',
        'embedding',
        'rerank',
        'CONTRIBUTION_DEIDENTIFY',
        'PRIVATE_DISTILL_TEXT',
        'PRIVATE_DISTILL_VISION',
        'PRIVATE_CONSOLIDATE'
      ]::text[]
    )
  );

COMMENT ON COLUMN ops.model_call_ledger.purpose IS
  '§19.1 purpose. Closed set, mirrored by humaux_domain::ledger::ModelCallPurpose (0166). '
  'Retrieval plane: query_rewrite/embedding/rerank. Private reasoning plane: '
  'CONTRIBUTION_DEIDENTIFY (USER-paid, 0130 route snapshot required) and the three '
  'platform-paid hops PRIVATE_DISTILL_TEXT / PRIVATE_DISTILL_VISION / PRIVATE_CONSOLIDATE, '
  'whose rows carry every 0130 reasoning column NULL.';
