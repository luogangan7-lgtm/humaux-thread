-- ADR-0011 / §1.14.1. Observation values and executed E2E links are separate facts.
-- Never backfill a scan hash or an E2E receipt for legacy observations.
ALTER TABLE ops.mechanism_observations ADD COLUMN scope_hash text
  CHECK (scope_hash IS NULL OR scope_hash ~ '^sha256:[0-9a-f]{64}$');

CREATE TABLE ops.mechanism_e2e_runs (
  run_id uuid PRIMARY KEY DEFAULT uuidv7(),
  deployment_id uuid NOT NULL,
  cell_id uuid NOT NULL,
  mechanism_id text NOT NULL,
  before_observation_id uuid NOT NULL REFERENCES ops.mechanism_observations(observation_id),
  after_observation_id uuid NOT NULL UNIQUE REFERENCES ops.mechanism_observations(observation_id),
  scope_hash text NOT NULL CHECK (scope_hash ~ '^sha256:[0-9a-f]{64}$'),
  probe_version text NOT NULL CHECK (length(probe_version) > 0),
  binary_build text NOT NULL CHECK (length(binary_build) > 0),
  started_at timestamptz NOT NULL,
  completed_at timestamptz NOT NULL,
  CHECK (before_observation_id <> after_observation_id),
  CHECK (started_at < completed_at)
);
ALTER TABLE ops.mechanism_e2e_runs OWNER TO role_migration_owner;

CREATE FUNCTION ops.guard_mechanism_e2e_binding() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, ops AS $$
DECLARE
  before_row ops.mechanism_observations%ROWTYPE;
  after_row ops.mechanism_observations%ROWTYPE;
BEGIN
  SELECT * INTO STRICT before_row FROM ops.mechanism_observations
    WHERE observation_id=NEW.before_observation_id;
  SELECT * INTO STRICT after_row FROM ops.mechanism_observations
    WHERE observation_id=NEW.after_observation_id;
  IF before_row.deployment_id <> NEW.deployment_id OR after_row.deployment_id <> NEW.deployment_id
    OR before_row.cell_id <> NEW.cell_id OR after_row.cell_id <> NEW.cell_id
    OR before_row.mechanism_id <> NEW.mechanism_id OR after_row.mechanism_id <> NEW.mechanism_id
    OR before_row.scope_hash IS DISTINCT FROM NEW.scope_hash OR after_row.scope_hash IS DISTINCT FROM NEW.scope_hash
    OR before_row.probe_version <> NEW.probe_version OR after_row.probe_version <> NEW.probe_version
    OR before_row.binary_build <> NEW.binary_build OR after_row.binary_build <> NEW.binary_build
    OR NEW.started_at > before_row.measured_at OR before_row.measured_at >= after_row.measured_at
    OR after_row.measured_at > NEW.completed_at OR NEW.completed_at > clock_timestamp()
    OR coalesce(before_row.scanned_n,0) <= 0 OR coalesce(after_row.scanned_n,0) <= 0
    OR before_row.value < 0 OR after_row.value < 0 THEN
    RAISE EXCEPTION 'invalid mechanism E2E observation binding' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END $$;
ALTER FUNCTION ops.guard_mechanism_e2e_binding() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION ops.guard_mechanism_e2e_binding() FROM PUBLIC;
CREATE TRIGGER mechanism_e2e_binding BEFORE INSERT ON ops.mechanism_e2e_runs
  FOR EACH ROW EXECUTE FUNCTION ops.guard_mechanism_e2e_binding();

CREATE FUNCTION ops.reject_mechanism_evidence_mutation() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  RAISE EXCEPTION 'mechanism runtime evidence is append-only' USING ERRCODE='23514';
END $$;
ALTER FUNCTION ops.reject_mechanism_evidence_mutation() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION ops.reject_mechanism_evidence_mutation() FROM PUBLIC;
CREATE TRIGGER mechanism_observation_immutable BEFORE UPDATE OR DELETE ON ops.mechanism_observations
  FOR EACH ROW EXECUTE FUNCTION ops.reject_mechanism_evidence_mutation();
CREATE TRIGGER mechanism_observation_no_truncate BEFORE TRUNCATE ON ops.mechanism_observations
  FOR EACH STATEMENT EXECUTE FUNCTION ops.reject_mechanism_evidence_mutation();
CREATE TRIGGER mechanism_e2e_immutable BEFORE UPDATE OR DELETE ON ops.mechanism_e2e_runs
  FOR EACH ROW EXECUTE FUNCTION ops.reject_mechanism_evidence_mutation();
CREATE TRIGGER mechanism_e2e_no_truncate BEFORE TRUNCATE ON ops.mechanism_e2e_runs
  FOR EACH STATEMENT EXECUTE FUNCTION ops.reject_mechanism_evidence_mutation();

-- §6.2: a dedicated read-only operational identity, not a maintenance connection in
-- a nominally read-only transaction. Authentication is provisioned externally; no
-- password, inherited writer role or application-pool credential in this migration.
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='role_admin') THEN
    CREATE ROLE role_admin LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE
      NOREPLICATION NOBYPASSRLS;
  END IF;
END $$;
GRANT USAGE ON SCHEMA ops TO role_admin;

REVOKE ALL ON ops.mechanism_observations, ops.mechanism_e2e_runs FROM PUBLIC,
  role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_batch_issuer, role_maintenance, role_admin;
GRANT SELECT ON ops.mechanism_observations, ops.mechanism_e2e_runs TO
  role_gateway, role_private_worker, role_consolidation_worker, role_public_worker,
  role_retrieval_worker, role_maintenance, role_admin;
GRANT INSERT ON ops.mechanism_observations, ops.mechanism_e2e_runs TO role_maintenance;

COMMENT ON TABLE ops.mechanism_e2e_runs IS
  '§1.14.1 Immutable before/after links from controlled E2E execution; no copied StaticSpec, '
  'passed boolean or inference from adjacent observations. Runtime status is recomputed.';
