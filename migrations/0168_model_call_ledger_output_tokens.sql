-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0167). ADR-0042 (card 20, review
-- finding P1 on `crates/adapters/src/contribution_reasoner.rs:850`): the private reasoning
-- plane's ledger rows could record only the INPUT leg of a generative call.
--
-- Why: §19.1's token columns were shaped for the retrieval plane, where a call has exactly one
-- billed token dimension — `billable_tokens` (§19's rerank formula
-- `query_tokens * document_count + sum(document_tokens)`, or an embedding input's own count),
-- multiplied by `control.provider_pricing_versions.input_token_price`. A generative call has
-- TWO priced dimensions, and the pricing table has carried the second one
-- (`output_token_price`) since 0095; `humaux_retrieval_provider::cost::compute_cost` already
-- takes both (`UsageSnapshot { billable_tokens, output_tokens }`). The provider's
-- `usage.completion_tokens` is parsed by `adapters::byok` (`TokenUsage::output_tokens`) and was
-- then dropped on the floor at `complete_structured_timed`, because there was no column to put
-- it in. For 0166's three PLATFORM-paid purposes (PRIVATE_DISTILL_TEXT / PRIVATE_DISTILL_VISION
-- / PRIVATE_CONSOLIDATE) — and for CONTRIBUTION_DEIDENTIFY, whose cost a USER pays but still
-- wants itemised — output tokens usually DOMINATE the bill, so "a pricing row plus
-- `input_tokens`" cannot compute a cost at all. Card 20 exists to meter exactly these two hops.
--
-- Shape: one nullable outcome column, same CHECK as its three siblings, written exactly once by
-- `finalize()` and immutable afterwards. NULL keeps its existing meaning everywhere else —
-- "this call has no output-token dimension" (embedding/rerank) or "the provider never
-- responded". No existing row is read, rewritten or deleted.
--
-- No new table, no new role, no new GRANT, no RLS change, no DML ⇒ no §6.2.2 matrix row and no
-- `xtask/src/rls_check.rs` MATRIX cell (§6.2.2/§48.2 apply to new tables/grants, and this is a
-- column on a table both writing roles already hold INSERT+UPDATE on).

ALTER TABLE ops.model_call_ledger
  ADD COLUMN output_tokens bigint CHECK (output_tokens IS NULL OR output_tokens >= 0);

COMMENT ON COLUMN ops.model_call_ledger.output_tokens IS
  '§19.1 (0168): generated tokens the provider reported (`usage.completion_tokens`). Feeds '
  'control.provider_pricing_versions.output_token_price; `billable_tokens` stays the '
  'input-priced dimension. NULL for a call with no output dimension (embedding/rerank) or a '
  'provider that never responded. Outcome column: set once by finalize(), immutable after.';

-- ---------------------------------------------------------------------------------------------
-- The two guards that enumerate the outcome columns by name. Both are reproduced from 0130
-- byte-for-byte apart from the added `output_tokens` term — a guard that does not name the new
-- column would leave it the one outcome column a second UPDATE could rewrite, which is the exact
-- hole 0098 finding #6 closed for the other eight.
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ops.model_call_ledger_guard_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
  IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
    RAISE EXCEPTION 'ops.model_call_ledger is append-only (§19.1) — % not permitted', TG_OP
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF OLD.request_id IS DISTINCT FROM NEW.request_id
     OR OLD.tenant_id IS DISTINCT FROM NEW.tenant_id
     OR OLD.workspace_id IS DISTINCT FROM NEW.workspace_id
     OR OLD.purpose IS DISTINCT FROM NEW.purpose
     OR OLD.provider IS DISTINCT FROM NEW.provider
     OR OLD.model IS DISTINCT FROM NEW.model
     OR OLD.model_revision IS DISTINCT FROM NEW.model_revision
     OR OLD.called_at IS DISTINCT FROM NEW.called_at
     OR OLD.estimated_cost IS DISTINCT FROM NEW.estimated_cost
     OR OLD.reasoning_domain_id IS DISTINCT FROM NEW.reasoning_domain_id
     OR OLD.call_kind IS DISTINCT FROM NEW.call_kind
     OR OLD.intent_sha256 IS DISTINCT FROM NEW.intent_sha256
     OR OLD.binding_id IS DISTINCT FROM NEW.binding_id
     OR OLD.binding_version IS DISTINCT FROM NEW.binding_version
     OR OLD.route_policy_id IS DISTINCT FROM NEW.route_policy_id
     OR OLD.route_policy_version IS DISTINCT FROM NEW.route_policy_version
     OR OLD.profile_id IS DISTINCT FROM NEW.profile_id
     OR OLD.profile_version IS DISTINCT FROM NEW.profile_version
     OR OLD.provider_account_id IS DISTINCT FROM NEW.provider_account_id
     OR OLD.provider_endpoint_id IS DISTINCT FROM NEW.provider_endpoint_id
     OR OLD.egress_processor_id IS DISTINCT FROM NEW.egress_processor_id
     OR OLD.credential_ref IS DISTINCT FROM NEW.credential_ref
     OR OLD.billing_account_id IS DISTINCT FROM NEW.billing_account_id
     OR OLD.billing_instrument_id IS DISTINCT FROM NEW.billing_instrument_id
     OR OLD.provider_health_observation_id IS DISTINCT FROM NEW.provider_health_observation_id
     OR OLD.account_health_observation_id IS DISTINCT FROM NEW.account_health_observation_id
     OR OLD.billing_responsibility IS DISTINCT FROM NEW.billing_responsibility
     OR OLD.admitted_at IS DISTINCT FROM NEW.admitted_at THEN
    RAISE EXCEPTION 'ops.model_call_ledger identity/reservation columns are immutable after INSERT (§19.1)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF OLD.status <> 'RESERVED' AND NEW.status IS DISTINCT FROM OLD.status THEN
    RAISE EXCEPTION 'ops.model_call_ledger already finalized — status cannot change again (§19.1)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF (OLD.input_tokens IS NOT NULL AND OLD.input_tokens IS DISTINCT FROM NEW.input_tokens)
  OR (OLD.output_tokens IS NOT NULL AND OLD.output_tokens IS DISTINCT FROM NEW.output_tokens)
  OR (OLD.billable_tokens IS NOT NULL AND OLD.billable_tokens IS DISTINCT FROM NEW.billable_tokens)
  OR (OLD.candidate_count IS NOT NULL AND OLD.candidate_count IS DISTINCT FROM NEW.candidate_count)
  OR (OLD.candidate_tokens IS NOT NULL AND OLD.candidate_tokens IS DISTINCT FROM NEW.candidate_tokens)
  OR (OLD.cache_hit IS NOT NULL AND OLD.cache_hit IS DISTINCT FROM NEW.cache_hit)
  OR (OLD.latency_ms IS NOT NULL AND OLD.latency_ms IS DISTINCT FROM NEW.latency_ms)
  OR (OLD.actual_cost IS NOT NULL AND OLD.actual_cost IS DISTINCT FROM NEW.actual_cost)
  OR (OLD.error_class IS NOT NULL AND OLD.error_class IS DISTINCT FROM NEW.error_class)
  OR (OLD.provider_request_id IS NOT NULL AND OLD.provider_request_id IS DISTINCT FROM NEW.provider_request_id) THEN
    RAISE EXCEPTION 'ops.model_call_ledger outcome columns can only be set once, from NULL (§19.1)'
      USING ERRCODE = 'insufficient_privilege';
  END IF;
  IF OLD.purpose = 'CONTRIBUTION_DEIDENTIFY' AND NEW.actual_cost IS NOT NULL THEN
    RAISE EXCEPTION 'USER-paid reasoning calls never record platform actual cost'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

-- 0130's "a reasoning call must BEGIN as an unfinalized reservation" arm counts the outcome
-- columns that must all be NULL at INSERT. Leaving `output_tokens` out of the count would let a
-- CONTRIBUTION_DEIDENTIFY row be inserted already carrying an output-token number — i.e. a
-- reservation that claims a result before the provider was called.
CREATE OR REPLACE FUNCTION ops.reasoning_model_call_validate()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  IF NEW.purpose IS DISTINCT FROM 'CONTRIBUTION_DEIDENTIFY' THEN
    RETURN NEW;
  END IF;
  IF NEW.status <> 'RESERVED'
     OR num_nonnulls(
          NEW.input_tokens, NEW.output_tokens, NEW.billable_tokens, NEW.candidate_count,
          NEW.candidate_tokens, NEW.cache_hit, NEW.latency_ms, NEW.actual_cost,
          NEW.error_class, NEW.provider_request_id
        ) <> 0
     OR NEW.admitted_at > clock_timestamp() THEN
    RAISE EXCEPTION 'reasoning model call must begin as an unfinalized USER-paid reservation'
      USING ERRCODE = '23514';
  END IF;
  IF NOT EXISTS (
    SELECT 1
      FROM control.resolve_user_reasoning_admission(
        NEW.binding_id,
        NEW.binding_version,
        NEW.reasoning_domain_id,
        'CONTRIBUTION_DEIDENTIFY'
      ) admission
      JOIN ops.reasoning_provider_health_observations provider_health
        ON provider_health.tenant_id = admission.tenant_id
       AND provider_health.observation_id = admission.provider_health_observation_id
      JOIN ops.reasoning_account_health_observations account_health
        ON account_health.tenant_id = admission.tenant_id
       AND account_health.observation_id = admission.account_health_observation_id
     WHERE admission.tenant_id = NEW.tenant_id
       AND admission.binding_id = NEW.binding_id
       AND admission.binding_version = NEW.binding_version
       AND admission.reasoning_domain_id = NEW.reasoning_domain_id
       AND admission.route_policy_id = NEW.route_policy_id
       AND admission.route_policy_version = NEW.route_policy_version
       AND admission.profile_id = NEW.profile_id
       AND admission.profile_version = NEW.profile_version
       AND admission.provider_account_id = NEW.provider_account_id
       AND admission.processor_id = NEW.provider
       AND admission.provider_model_id = NEW.model
       AND admission.model_revision IS NOT DISTINCT FROM NEW.model_revision
       AND admission.provider_endpoint_id = NEW.provider_endpoint_id
       AND admission.egress_processor_id = NEW.egress_processor_id
       AND admission.credential_ref = NEW.credential_ref
       AND admission.billing_account_id IS NOT DISTINCT FROM NEW.billing_account_id
       AND admission.billing_instrument_id IS NOT DISTINCT FROM NEW.billing_instrument_id
       AND admission.provider_health_observation_id = NEW.provider_health_observation_id
       AND admission.account_health_observation_id = NEW.account_health_observation_id
       AND NEW.billing_responsibility = 'USER'
       AND provider_health.observed_at <= NEW.admitted_at
       AND NEW.admitted_at < provider_health.valid_until
       AND provider_health.verdict = 'HEALTHY'
       AND account_health.observed_at <= NEW.admitted_at
       AND NEW.admitted_at < account_health.valid_until
       AND account_health.account_verdict = 'HEALTHY'
       AND account_health.credential_verdict = 'VALID'
       AND account_health.billing_account_verdict IS NOT DISTINCT FROM
         CASE WHEN NEW.billing_account_id IS NULL THEN NULL ELSE 'ENABLED' END
       AND account_health.billing_instrument_verdict IS NOT DISTINCT FROM
         CASE WHEN NEW.billing_instrument_id IS NULL THEN NULL ELSE 'ENABLED' END
  ) THEN
    RAISE EXCEPTION 'reasoning model call snapshot must equal one current exact admission'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;
