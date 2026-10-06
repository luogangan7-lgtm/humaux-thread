-- §46 EXPAND_CONTRACT (card 37 S1; ADR-0064 D-A, D-E). Rebuild generation g+1 of a stream = a set of NEW tickets on
-- the SAME stream, recorded in a side table (projection.stream_log keeps its 18 frozen columns, §37.2 / G80-25);
-- the projection is rebuilt in place, nothing switches (§16.2 note, ADR-0064 D-A).
--
--   projection.rebuild_runs      one row per (stream, generation >= 2); at most one open run per stream (partial
--                                UNIQUE); verdict derived at close. role_maintenance SELECT only.
--   projection.rebuild_tickets   one row per generation ticket, keyed by the ticket's stream_log PK (FK);
--                                UNIQUE (stream key, generation, commit_seq) is the ticket uniqueness WITH the
--                                generation predicate (D-A). role_maintenance, role_retrieval_worker (the scoped
--                                claim, D-N(g)) and role_gateway (the §15.5 overlay predicate, E13) SELECT.
--   Both: tenant_id, ENABLE + FORCE RLS on the tenant GUC; the 0011 default privileges are REVOKEd first (F24).
--
-- Triggers on projection.stream_log (functions owned by role_migration_owner, search_path pinned, EXECUTE revoked
-- from PUBLIC, no executor: a trigger fires without EXECUTE):
--   stream_log_generation_never_lost            BEFORE UPDATE OF state WHEN (NEW.state = 'LOST'): returns NULL (the
--     projection.stream_log_generation_never_lost()   row is skipped, no error) for a ticket that has a rebuild_tickets
--                                               row, so card 35's sweep never moves a generation backlog to LOST and
--                                               its reissue never sees one. SECURITY DEFINER (reads only); fires
--                                               before stream_log_guard_state_transition (name order). Caller: any
--                                               UPDATE of state to LOST (only role_maintenance's sweep_lost may).
--   stream_log_tombstone_follows_to_generation  AFTER UPDATE OF state WHEN (NEW.state = 'TOMBSTONED' AND
--     projection.stream_log_tombstone_follows_to_generation()   OLD.state <> 'TOMBSTONED'): tombstones every
--                                               non-tombstoned generation ticket of the same tenant whose Evidence
--                                               (through ops.outbox) is the tombstoned row's Evidence (finding 14).
--                                               SECURITY INVOKER on purpose: the 0167 state guard lets only
--                                               role_maintenance write `* -> TOMBSTONED` and refuses it to the owner
--                                               (0167 owner branch: FAILED -> RETIRED_FAILED only), so the follow
--                                               UPDATE must run as the role that tombstoned (forget_repo::tombstone,
--                                               role_maintenance under the tenant GUC: SELECT ops.outbox and
--                                               rebuild_tickets, UPDATE stream_log).
--
-- Definers (owner role_migration_owner, SECURITY DEFINER, search_path = pg_catalog, tenant-GUC assertion first,
-- EXECUTE role_maintenance only, PUBLIC revoked, no DELETE in any body; caller: humaux-maintenance projection rebuild
-- and the restore drill, card 37 S3/S5, through adapters::rebuild):
--   projection.rebuild_open(uuid, text, uuid, text, text, text, bytea)
--       RETURNS TABLE (run_id uuid, generation integer, boundary_seq bigint, resumed boolean)
--       locks the checkpoint row FOR UPDATE; boundary H = issued_highwater (a closed committed boundary: every issuer
--       bumps it under the same row lock); resumes the stream's open run with the same fingerprint, refuses
--       `rebuild_open_with_other_fingerprint` (55000) for another.
--   projection.issue_rebuild_tickets(uuid, integer, boolean) RETURNS bigint
--       fence: p_limit > 0 (22023), at most p_limit tickets per call, the run open and of the GUC tenant; under the
--       checkpoint row lock, one generation ticket per input Evidence at or below H (home commit = the lowest
--       commit_seq of its generation-1 tickets on this stream) not yet ticketed in this generation, minus four
--       excluded classes: tombstoned (any TOMBSTONED ticket reached through ops.outbox, read_materialize's §37
--       hydrate-gate predicate), retired_by_operator (latest ticket RETIRED_FAILED), distill_pending (no memory and
--       its EVIDENCE_ACCEPTED outbox row PENDING/PROCESSING), without_stored_vector (only when p_require_stored_vector:
--       an active memory with no stored vector under the run's fingerprint). Per input: issued_highwater + 1,
--       stream_log ISSUED, rebuild_tickets; no outbox row (the ticket's commit_seq is the Evidence's home commit).
--       ponytail: each call re-derives the candidate set over the stream's tickets at or below H (O(stream) per
--       batch); upgrade = a keyset cursor on home_commit if M2's rebuild_issue_s grows past the RTO budget.
--   projection.rebuild_close(uuid, bigint, text, bigint, bytea, jsonb) RETURNS void
--       refuses (55000) `generation_in_flight` (a ticket of the run not settled), `boundary_moved` (issued_highwater
--       <> p_boundary) and `catch_up_in_flight` (a ticket outside the generation with H < stream_seq <= p_boundary in
--       flight); otherwise closes the run with its verdict.
--
-- Locks: CREATE TABLE / CREATE FUNCTION lock only the new objects; each CREATE TRIGGER takes SHARE ROW EXCLUSIVE on
-- projection.stream_log for the catalog update only (no scan), which waits for in-flight writers of stream_log and
-- blocks new ones for that moment. The whole file took 18.4 ms on a throwaway migrated to 0232 (2026-10-06).

CREATE TABLE projection.rebuild_runs (
  run_id              uuid        PRIMARY KEY DEFAULT uuidv7(),
  tenant_id           uuid        NOT NULL,
  scope_kind          text        NOT NULL,
  scope_id            uuid        NOT NULL,
  domain              text        NOT NULL,
  projection_kind     text        NOT NULL,
  projection_version  text        NOT NULL,
  generation          integer     NOT NULL CHECK (generation >= 2),
  fingerprint_sha256  bytea       NOT NULL REFERENCES projection.embedding_fingerprints (fingerprint_sha256),
  boundary_seq        bigint      NOT NULL CHECK (boundary_seq >= 0),
  reembed_allowed     bigint      NOT NULL DEFAULT 0 CHECK (reembed_allowed >= 0),
  opened_at           timestamptz NOT NULL DEFAULT clock_timestamp(),
  closed_at           timestamptz,
  closed_boundary_seq bigint,
  verdict             text        CHECK (verdict IN ('equivalent', 'not_equivalent', 're_embed_required',
                                                     'cannot_establish')),
  points              bigint,
  merkle_root         bytea,
  report              jsonb,
  CONSTRAINT rebuild_runs_stream_fk
    FOREIGN KEY (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version)
    REFERENCES projection.stream_checkpoints (tenant_id, scope_kind, scope_id, domain, projection_kind,
                                              projection_version),
  CONSTRAINT rebuild_runs_closed_has_verdict CHECK ((closed_at IS NULL) = (verdict IS NULL)),
  CONSTRAINT rebuild_runs_closed_boundary CHECK ((closed_at IS NULL) = (closed_boundary_seq IS NULL)),
  CONSTRAINT rebuild_runs_generation_unique
    UNIQUE (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, generation),
  CONSTRAINT rebuild_runs_run_generation_unique UNIQUE (run_id, generation)
);
-- ADR-0064 D-A invariant 3: a stream has at most one open run.
CREATE UNIQUE INDEX rebuild_runs_one_open_per_stream
  ON projection.rebuild_runs (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version)
  WHERE closed_at IS NULL;

CREATE TABLE projection.rebuild_tickets (
  tenant_id          uuid    NOT NULL,
  scope_kind         text    NOT NULL,
  scope_id           uuid    NOT NULL,
  domain             text    NOT NULL,
  projection_kind    text    NOT NULL,
  projection_version text    NOT NULL,
  stream_seq         bigint  NOT NULL,
  generation         integer NOT NULL CHECK (generation >= 2),
  commit_seq         bigint  NOT NULL,
  run_id             uuid    NOT NULL,
  PRIMARY KEY (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, stream_seq),
  CONSTRAINT rebuild_tickets_stream_log_fk
    FOREIGN KEY (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, stream_seq)
    REFERENCES projection.stream_log (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version,
                                      stream_seq),
  CONSTRAINT rebuild_tickets_run_fk FOREIGN KEY (run_id, generation)
    REFERENCES projection.rebuild_runs (run_id, generation),
  -- ADR-0064 D-A: the ticket uniqueness with the generation predicate (one ticket per input per generation).
  CONSTRAINT rebuild_tickets_generation_unique
    UNIQUE (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, generation, commit_seq)
);
CREATE INDEX rebuild_tickets_run ON projection.rebuild_tickets (run_id);

ALTER TABLE projection.rebuild_runs OWNER TO role_migration_owner;
ALTER TABLE projection.rebuild_tickets OWNER TO role_migration_owner;
ALTER TABLE projection.rebuild_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection.rebuild_runs FORCE ROW LEVEL SECURITY;
ALTER TABLE projection.rebuild_tickets ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection.rebuild_tickets FORCE ROW LEVEL SECURITY;
CREATE POLICY rebuild_runs_tenant_isolation ON projection.rebuild_runs
  USING (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid)
  WITH CHECK (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid);
CREATE POLICY rebuild_tickets_tenant_isolation ON projection.rebuild_tickets
  USING (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid)
  WITH CHECK (tenant_id = (NULLIF(current_setting('humaux.tenant_id'::text, true), ''::text))::uuid);
REVOKE ALL ON projection.rebuild_runs, projection.rebuild_tickets
FROM PUBLIC, role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin, role_health_reader;
GRANT SELECT ON projection.rebuild_runs TO role_maintenance;
GRANT SELECT ON projection.rebuild_tickets TO role_maintenance, role_retrieval_worker, role_gateway;

-- ---------------------------------------------------------------------------------------------------------------
-- Triggers
-- ---------------------------------------------------------------------------------------------------------------
CREATE FUNCTION projection.stream_log_generation_never_lost() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
  -- ADR-0064 D-A (finding 16): a generation backlog is never an orphan; skip the row, raise nothing.
  IF EXISTS (
       SELECT 1 FROM projection.rebuild_tickets rt
        WHERE rt.tenant_id = OLD.tenant_id AND rt.scope_kind = OLD.scope_kind AND rt.scope_id = OLD.scope_id
          AND rt.domain = OLD.domain AND rt.projection_kind = OLD.projection_kind
          AND rt.projection_version = OLD.projection_version AND rt.stream_seq = OLD.stream_seq) THEN
    RETURN NULL;
  END IF;
  RETURN NEW;
END;
$$;
ALTER FUNCTION projection.stream_log_generation_never_lost() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION projection.stream_log_generation_never_lost() FROM PUBLIC;

CREATE TRIGGER stream_log_generation_never_lost
BEFORE UPDATE OF state ON projection.stream_log
FOR EACH ROW WHEN (NEW.state = 'LOST')
EXECUTE FUNCTION projection.stream_log_generation_never_lost();

CREATE FUNCTION projection.stream_log_tombstone_follows_to_generation() RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $$
BEGIN
  -- ADR-0064 D-A (finding 14): a forget during an open run reaches the generation tickets of the same Evidence.
  UPDATE projection.stream_log g
     SET state = 'TOMBSTONED'
    FROM projection.rebuild_tickets rt
   WHERE g.tenant_id = NEW.tenant_id
     AND rt.tenant_id = g.tenant_id AND rt.scope_kind = g.scope_kind AND rt.scope_id = g.scope_id
     AND rt.domain = g.domain AND rt.projection_kind = g.projection_kind
     AND rt.projection_version = g.projection_version AND rt.stream_seq = g.stream_seq
     AND g.state <> 'TOMBSTONED'
     AND g.commit_seq IN (
           SELECT home.commit_seq
             FROM ops.outbox gone
             JOIN ops.outbox home ON home.tenant_id = gone.tenant_id AND home.evidence_id = gone.evidence_id
            WHERE gone.tenant_id = NEW.tenant_id AND gone.commit_seq = NEW.commit_seq);
  RETURN NULL;
END;
$$;
ALTER FUNCTION projection.stream_log_tombstone_follows_to_generation() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION projection.stream_log_tombstone_follows_to_generation() FROM PUBLIC;

CREATE TRIGGER stream_log_tombstone_follows_to_generation
AFTER UPDATE OF state ON projection.stream_log
FOR EACH ROW WHEN (NEW.state = 'TOMBSTONED' AND OLD.state <> 'TOMBSTONED')
EXECUTE FUNCTION projection.stream_log_tombstone_follows_to_generation();

-- ---------------------------------------------------------------------------------------------------------------
-- Definers
-- ---------------------------------------------------------------------------------------------------------------
CREATE FUNCTION projection.rebuild_open(
  p_tenant uuid, p_scope_kind text, p_scope_id uuid, p_domain text, p_kind text, p_version text,
  p_fingerprint bytea)
RETURNS TABLE (run_id uuid, generation integer, boundary_seq bigint, resumed boolean)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
#variable_conflict use_column
DECLARE
  v_high bigint;
  v_open record;
  v_generation integer;
  v_run uuid;
BEGIN
  IF p_tenant IS NULL OR p_fingerprint IS NULL THEN
    RAISE EXCEPTION 'rebuild_open: tenant and fingerprint are required' USING ERRCODE = '22023';
  END IF;
  IF p_tenant IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'tenant_context_mismatch' USING ERRCODE = '42501';
  END IF;
  -- ADR-0064 D-E step 2: H is read under the checkpoint row lock every issuer takes to bump the counter.
  SELECT k.issued_highwater INTO STRICT v_high
    FROM projection.stream_checkpoints k
   WHERE k.tenant_id = p_tenant AND k.scope_kind = p_scope_kind AND k.scope_id = p_scope_id
     AND k.domain = p_domain AND k.projection_kind = p_kind AND k.projection_version = p_version
   FOR UPDATE;
  SELECT r.run_id, r.generation, r.boundary_seq, r.fingerprint_sha256 INTO v_open
    FROM projection.rebuild_runs r
   WHERE r.tenant_id = p_tenant AND r.scope_kind = p_scope_kind AND r.scope_id = p_scope_id
     AND r.domain = p_domain AND r.projection_kind = p_kind AND r.projection_version = p_version
     AND r.closed_at IS NULL;
  IF FOUND THEN
    IF v_open.fingerprint_sha256 IS DISTINCT FROM p_fingerprint THEN
      RAISE EXCEPTION 'rebuild refused: rebuild_open_with_other_fingerprint' USING ERRCODE = '55000';
    END IF;
    RETURN QUERY SELECT v_open.run_id, v_open.generation, v_open.boundary_seq, true;
    RETURN;
  END IF;
  SELECT greatest(coalesce(max(r.generation), 1), 1) + 1 INTO v_generation
    FROM projection.rebuild_runs r
   WHERE r.tenant_id = p_tenant AND r.scope_kind = p_scope_kind AND r.scope_id = p_scope_id
     AND r.domain = p_domain AND r.projection_kind = p_kind AND r.projection_version = p_version;
  INSERT INTO projection.rebuild_runs
    (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, generation,
     fingerprint_sha256, boundary_seq)
  VALUES (p_tenant, p_scope_kind, p_scope_id, p_domain, p_kind, p_version, v_generation, p_fingerprint, v_high)
  RETURNING projection.rebuild_runs.run_id INTO v_run;
  RETURN QUERY SELECT v_run, v_generation, v_high, false;
END;
$$;
ALTER FUNCTION projection.rebuild_open(uuid, text, uuid, text, text, text, bytea) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION projection.rebuild_open(uuid, text, uuid, text, text, text, bytea) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION projection.rebuild_open(uuid, text, uuid, text, text, text, bytea) TO role_maintenance;

CREATE FUNCTION projection.issue_rebuild_tickets(p_run_id uuid, p_limit integer, p_require_stored_vector boolean)
RETURNS bigint
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  r record;
  c record;
  v_seq bigint;
  v_issued bigint := 0;
BEGIN
  IF p_run_id IS NULL OR p_limit IS NULL OR p_limit <= 0 OR p_require_stored_vector IS NULL THEN
    RAISE EXCEPTION 'issue_rebuild_tickets: p_run_id, p_limit > 0 and p_require_stored_vector are required'
      USING ERRCODE = '22023';
  END IF;
  -- The run table is FORCE RLS on the tenant GUC: another tenant's run reads as absent.
  SELECT * INTO r FROM projection.rebuild_runs x WHERE x.run_id = p_run_id;
  IF NOT FOUND OR r.tenant_id IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'tenant_context_mismatch' USING ERRCODE = '42501';
  END IF;
  IF r.closed_at IS NOT NULL THEN
    RAISE EXCEPTION 'rebuild refused: rebuild_run_closed' USING ERRCODE = '55000';
  END IF;
  -- ADR-0064 D-A: the next stream_seq is taken under the checkpoint row lock, like every other issuer.
  PERFORM 1 FROM projection.stream_checkpoints k
    WHERE k.tenant_id = r.tenant_id AND k.scope_kind = r.scope_kind AND k.scope_id = r.scope_id
      AND k.domain = r.domain AND k.projection_kind = r.projection_kind
      AND k.projection_version = r.projection_version
    FOR UPDATE;

  FOR c IN
    WITH tickets AS (
      SELECT sl.stream_seq, sl.commit_seq, sl.state, o.evidence_id
        FROM projection.stream_log sl
        JOIN ops.outbox o ON o.tenant_id = sl.tenant_id AND o.commit_seq = sl.commit_seq
       WHERE sl.tenant_id = r.tenant_id AND sl.scope_kind = r.scope_kind AND sl.scope_id = r.scope_id
         AND sl.domain = r.domain AND sl.projection_kind = r.projection_kind
         AND sl.projection_version = r.projection_version
         AND sl.stream_seq <= r.boundary_seq
         AND o.evidence_id IS NOT NULL
         AND NOT EXISTS (
               SELECT 1 FROM projection.rebuild_tickets t
                WHERE t.tenant_id = sl.tenant_id AND t.scope_kind = sl.scope_kind AND t.scope_id = sl.scope_id
                  AND t.domain = sl.domain AND t.projection_kind = sl.projection_kind
                  AND t.projection_version = sl.projection_version AND t.stream_seq = sl.stream_seq)
    ), inputs AS (
      SELECT t.evidence_id, min(t.commit_seq) AS home_commit,
             (array_agg(t.state ORDER BY t.stream_seq DESC))[1] AS latest_state
        FROM tickets t
       GROUP BY t.evidence_id
    )
    SELECT i.evidence_id, i.home_commit
      FROM inputs i
     WHERE NOT EXISTS (
             SELECT 1 FROM projection.rebuild_tickets t
              WHERE t.tenant_id = r.tenant_id AND t.scope_kind = r.scope_kind AND t.scope_id = r.scope_id
                AND t.domain = r.domain AND t.projection_kind = r.projection_kind
                AND t.projection_version = r.projection_version
                AND t.generation = r.generation AND t.commit_seq = i.home_commit)
       -- excluded:tombstoned — read_materialize's §37 hydrate-gate predicate (§23.1②: reused, not re-derived).
       AND NOT EXISTS (
             SELECT 1 FROM ops.outbox ob
               JOIN projection.stream_log ts ON ts.tenant_id = ob.tenant_id AND ts.commit_seq = ob.commit_seq
              WHERE ob.tenant_id = r.tenant_id AND ob.evidence_id = i.evidence_id AND ts.state = 'TOMBSTONED')
       -- excluded:retired_by_operator — the operator's retirement is reproduced, not undone (card 35 D-N/D-O).
       AND i.latest_state IS DISTINCT FROM 'RETIRED_FAILED'
       -- excluded:distill_pending — no memory yet and the Evidence is still being distilled (F25).
       AND NOT (
             NOT EXISTS (SELECT 1 FROM private.memory_evidence me WHERE me.evidence_id = i.evidence_id)
             AND EXISTS (
                   SELECT 1 FROM ops.outbox ea
                    WHERE ea.tenant_id = r.tenant_id AND ea.evidence_id = i.evidence_id
                      AND ea.event_type = 'EVIDENCE_ACCEPTED' AND ea.status IN ('PENDING', 'PROCESSING')))
       -- excluded:without_stored_vector — the drill has no provider (D-E step 4, D-G).
       AND NOT (
             p_require_stored_vector
             AND EXISTS (
                   SELECT 1 FROM private.memory_evidence me
                     JOIN private.memory_records m ON m.memory_id = me.memory_id AND m.tenant_id = r.tenant_id
                    WHERE me.evidence_id = i.evidence_id AND m.status = 'active'
                      AND NOT EXISTS (
                            SELECT 1 FROM projection.memory_vectors v
                             WHERE v.tenant_id = r.tenant_id AND v.memory_id = m.memory_id
                               AND v.fingerprint_sha256 = r.fingerprint_sha256 AND v.vector IS NOT NULL)))
     ORDER BY i.home_commit
     LIMIT p_limit
  LOOP
    UPDATE projection.stream_checkpoints k
       SET issued_highwater = k.issued_highwater + 1
     WHERE k.tenant_id = r.tenant_id AND k.scope_kind = r.scope_kind AND k.scope_id = r.scope_id
       AND k.domain = r.domain AND k.projection_kind = r.projection_kind
       AND k.projection_version = r.projection_version
    RETURNING k.issued_highwater INTO STRICT v_seq;
    INSERT INTO projection.stream_log
      (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, stream_seq, commit_seq)
    VALUES (r.tenant_id, r.scope_kind, r.scope_id, r.domain, r.projection_kind, r.projection_version, v_seq,
            c.home_commit);
    INSERT INTO projection.rebuild_tickets
      (tenant_id, scope_kind, scope_id, domain, projection_kind, projection_version, stream_seq, generation,
       commit_seq, run_id)
    VALUES (r.tenant_id, r.scope_kind, r.scope_id, r.domain, r.projection_kind, r.projection_version, v_seq,
            r.generation, c.home_commit, r.run_id);
    v_issued := v_issued + 1;
  END LOOP;
  RETURN v_issued;
END;
$$;
ALTER FUNCTION projection.issue_rebuild_tickets(uuid, integer, boolean) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION projection.issue_rebuild_tickets(uuid, integer, boolean) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION projection.issue_rebuild_tickets(uuid, integer, boolean) TO role_maintenance;

CREATE FUNCTION projection.rebuild_close(
  p_run_id uuid, p_boundary bigint, p_verdict text, p_points bigint, p_merkle_root bytea, p_report jsonb)
RETURNS void
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  r record;
  v_high bigint;
BEGIN
  IF p_run_id IS NULL OR p_boundary IS NULL OR p_verdict IS NULL THEN
    RAISE EXCEPTION 'rebuild_close: p_run_id, p_boundary and p_verdict are required' USING ERRCODE = '22023';
  END IF;
  SELECT * INTO r FROM projection.rebuild_runs x WHERE x.run_id = p_run_id FOR UPDATE;
  IF NOT FOUND OR r.tenant_id IS DISTINCT FROM NULLIF(current_setting('humaux.tenant_id', true), '')::uuid THEN
    RAISE EXCEPTION 'tenant_context_mismatch' USING ERRCODE = '42501';
  END IF;
  IF r.closed_at IS NOT NULL THEN
    RAISE EXCEPTION 'rebuild refused: rebuild_run_closed' USING ERRCODE = '55000';
  END IF;
  -- ADR-0064 D-A completion term 1: every generation ticket has left the in-flight states.
  IF EXISTS (
       SELECT 1 FROM projection.rebuild_tickets t
         JOIN projection.stream_log sl
           ON sl.tenant_id = t.tenant_id AND sl.scope_kind = t.scope_kind AND sl.scope_id = t.scope_id
          AND sl.domain = t.domain AND sl.projection_kind = t.projection_kind
          AND sl.projection_version = t.projection_version AND sl.stream_seq = t.stream_seq
        WHERE t.run_id = r.run_id AND sl.state IN ('ISSUED', 'PROCESSING', 'WAITING_KEY', 'RETRY_WAIT')) THEN
    RAISE EXCEPTION 'rebuild refused: generation_in_flight' USING ERRCODE = '55000';
  END IF;
  -- Completion term 2: nothing was issued after the verifier read H2.
  SELECT k.issued_highwater INTO STRICT v_high
    FROM projection.stream_checkpoints k
   WHERE k.tenant_id = r.tenant_id AND k.scope_kind = r.scope_kind AND k.scope_id = r.scope_id
     AND k.domain = r.domain AND k.projection_kind = r.projection_kind
     AND k.projection_version = r.projection_version
   FOR SHARE;
  IF v_high <> p_boundary OR p_boundary < r.boundary_seq THEN
    RAISE EXCEPTION 'rebuild refused: boundary_moved' USING ERRCODE = '55000';
  END IF;
  -- Completion term 3: the writes that arrived during the run (outside the generation, above H) are settled.
  IF EXISTS (
       SELECT 1 FROM projection.stream_log sl
        WHERE sl.tenant_id = r.tenant_id AND sl.scope_kind = r.scope_kind AND sl.scope_id = r.scope_id
          AND sl.domain = r.domain AND sl.projection_kind = r.projection_kind
          AND sl.projection_version = r.projection_version
          AND sl.stream_seq > r.boundary_seq AND sl.stream_seq <= p_boundary
          AND sl.state IN ('ISSUED', 'PROCESSING', 'WAITING_KEY', 'RETRY_WAIT')
          AND NOT EXISTS (
                SELECT 1 FROM projection.rebuild_tickets t
                 WHERE t.run_id = r.run_id AND t.tenant_id = sl.tenant_id AND t.scope_kind = sl.scope_kind
                   AND t.scope_id = sl.scope_id AND t.domain = sl.domain AND t.projection_kind = sl.projection_kind
                   AND t.projection_version = sl.projection_version AND t.stream_seq = sl.stream_seq)) THEN
    RAISE EXCEPTION 'rebuild refused: catch_up_in_flight' USING ERRCODE = '55000';
  END IF;
  UPDATE projection.rebuild_runs x
     SET closed_at = clock_timestamp(), closed_boundary_seq = p_boundary, verdict = p_verdict,
         points = p_points, merkle_root = p_merkle_root, report = p_report
   WHERE x.run_id = r.run_id;
END;
$$;
ALTER FUNCTION projection.rebuild_close(uuid, bigint, text, bigint, bytea, jsonb) OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION projection.rebuild_close(uuid, bigint, text, bigint, bytea, jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION projection.rebuild_close(uuid, bigint, text, bigint, bytea, jsonb) TO role_maintenance;

COMMENT ON TABLE projection.rebuild_runs IS
  'ADR-0064 D-E: one rebuild generation (>= 2) of one stream; at most one open run per stream; written only by the '
  'owner definers rebuild_open / rebuild_close. role_maintenance SELECT only.';
COMMENT ON TABLE projection.rebuild_tickets IS
  'ADR-0064 D-A: the generation of each rebuild ticket, keyed by its stream_log PK; UNIQUE (stream, generation, '
  'commit_seq). Written only by projection.issue_rebuild_tickets. SELECT: maintenance, retrieval worker, gateway.';
