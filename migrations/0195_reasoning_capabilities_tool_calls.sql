-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0194). ADR-0058 D-M as amended by the
-- main-line ruling of 2026-10-02 10:35 (provider-neutral output channel): the §11.2 capability
-- closed set ("能力至少") gains
--
--   * TOOL_CALLS      — the endpoint accepts one side-effect-free `tools` function and answers in
--                       `tool_calls`; the distill hop uses the tool channel only when the worker's
--                       descriptor declares it (otherwise the v1 content body, byte-identical);
--   * REASONING_SPLIT — the endpoint accepts `"reasoning_split": true` and returns its reasoning
--                       outside the answer; the request carries the field only when declared.
--
-- The three capability CHECKs (0048 user_reasoning_profiles, 0128 processor_models and
-- reasoning_profiles) are widened in place: DROP + ADD under the same names, so every reader
-- (rls-check, adapters::byok contract test) keeps one name per table. Rust twin:
-- humaux_adapters::byok::ReasoningCapability::ALL (pinned against the live constraints by
-- crates/adapters/tests/distill_dispatch_v2.rs T23 and against this file's text by byok.rs).
--
-- Locks: ACCESS EXCLUSIVE on three small control tables for one CHECK scan each; no row changes,
-- no grant change.
ALTER TABLE control.user_reasoning_profiles
  DROP CONSTRAINT user_reasoning_profiles_capabilities_known,
  ADD CONSTRAINT user_reasoning_profiles_capabilities_known CHECK (
    capabilities <@ ARRAY['TEXT', 'VISION', 'STRUCTURED_OUTPUT', 'TOKEN_USAGE', 'TOOL_CALLS', 'REASONING_SPLIT']::text[]
    AND array_length(capabilities, 1) > 0
  );

ALTER TABLE control.processor_models
  DROP CONSTRAINT processor_models_capabilities_check,
  ADD CONSTRAINT processor_models_capabilities_check CHECK (
    cardinality(capabilities) > 0
    AND capabilities <@ ARRAY['TEXT', 'VISION', 'STRUCTURED_OUTPUT', 'TOKEN_USAGE', 'TOOL_CALLS', 'REASONING_SPLIT']::text[]
  );

ALTER TABLE control.reasoning_profiles
  DROP CONSTRAINT reasoning_profiles_capabilities_check,
  ADD CONSTRAINT reasoning_profiles_capabilities_check CHECK (
    cardinality(capabilities) > 0
    AND capabilities <@ ARRAY['TEXT', 'VISION', 'STRUCTURED_OUTPUT', 'TOKEN_USAGE', 'TOOL_CALLS', 'REASONING_SPLIT']::text[]
  );
