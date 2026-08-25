-- §46.1 migration — fixer-review follow-up to 0034_email_auth_identity. 0034 is already
-- applied and per repo policy applied migrations are additive-only, so these fixes land as a
-- new migration rather than editing 0034 in place.

-- =============================================================================
-- Fix (major, §73.6 / §6.2.1): control.sessions carried no session-token column at all — the
-- `session_id uuid PRIMARY KEY DEFAULT uuidv7()` was the only candidate bearer value for the
-- `__Host-humaux_session` cookie §73.6 requires, which means the bearer value was: plaintext
-- (no hash), time-ordered (uuidv7 leaks issuance time), and SELECT-able by all six non-owner
-- runtime roles via 0011's `ALTER DEFAULT PRIVILEGES` domain default (§6.2.1) — any worker
-- role could enumerate every live session and impersonate any user by replaying session_id.
--
-- `token_hash` is the actual bearer-secret verifier from here on: a keyed hash
-- (`HMAC-SHA256(pepper, raw_session_token)`), the same construction `protocol::edge`'s
-- `compute_api_key_hash` already uses for `control.api_keys.key_hash` (§73.5) — reused, not
-- reinvented, for the identical reason: a random-entropy bearer token doesn't need Argon2's
-- slow KDF, and read access to a keyed hash is not equivalent to possession of the token.
-- `session_id` remains the primary key / internal surrogate a caller uses to reference the
-- row server-side; the fix is that it is never again the value handed to or accepted from a
-- client.
--
-- Precheck requires the table to currently hold zero rows: this crate's own module doc
-- (`crates/application/src/auth.rs`) states session issuance is not wired into any caller yet
-- at Phase 2 wave time, so an empty table is the expected state, not an assumption of
-- convenience. If a future run of this migration hits a non-empty table, the precheck fails
-- loudly rather than silently picking a backfill strategy for live bearer tokens — that
-- decision (rotate every session? force-logout?) is an operational call for whoever is
-- deploying at that point, not one this migration makes on their behalf.
-- =============================================================================

ALTER TABLE control.sessions
  ADD COLUMN token_hash bytea NOT NULL,
  ADD CONSTRAINT sessions_token_hash_key UNIQUE (token_hash);

COMMENT ON COLUMN control.sessions.session_id IS
  'Internal-only surrogate key. Never the value stored in the __Host-humaux_session cookie or '
  'accepted from a client (§73.6) — see token_hash.';

COMMENT ON COLUMN control.sessions.token_hash IS
  '§73.6 keyed-hash bearer-token verifier: HMAC-SHA256(pepper, raw_session_token), same '
  'construction as control.api_keys.key_hash (protocol::edge::compute_api_key_hash, §73.5). '
  'The raw session token is generated at issuance, handed to the client once in the '
  '__Host-humaux_session cookie, and never stored — only this hash is.';

-- =============================================================================
-- Fix (minor, §74.2 per-IP 限流): control.password_reset_challenges already carries
-- `requested_ip inet` as the correlation key its per-IP limiter needs; control.email_challenges
-- (signup verification codes, subject to the same per-email/per-IP/per-device limiting per
-- §74.2) had no such column, so the signup send path had no IP correlation key in the schema
-- at all. Added now, symmetric with password_reset_challenges, while the table is still new
-- enough that this is a plain additive column rather than a second migration later.
-- =============================================================================

ALTER TABLE control.email_challenges
  ADD COLUMN requested_ip inet;

COMMENT ON COLUMN control.email_challenges.requested_ip IS
  '§74.2 per-IP rate-limit correlation key, symmetric with '
  'control.password_reset_challenges.requested_ip. Rate-limit bucket mechanism itself is '
  '§73.3 IP Policy (H1/T2.1+T2.2), not reimplemented here.';
