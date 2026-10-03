-- §46 forward-fix (EXPAND_CONTRACT, first free number after 0204). Card 33 / ADR-0059 D-H; §73.5.
--
-- 0204's control.credential_pepper_epoch_advance() moved to the next epoch and opened the rehash
-- window on every call. `humaux-maintenance apikey pepper-epoch advance` is documented one-shot like
-- every subcommand, but a second run (an operator retry after an unknown outcome, a replayed runbook
-- step) moved the epoch again while the window was open: every key rehashed under the first advance
-- fell behind the epoch once more and became rewritable a second time by role_gateway (L12 doubled).
-- The body is replaced: advance refuses with SQLSTATE 55000 and the reason token
-- `rehash_window_open` while control.credential_pepper_state.rehash_open is true, so the only way to
-- the next epoch is close, then advance. Concurrent advances serialise on the singleton row; the
-- loser re-evaluates `NOT rehash_open` and is refused. Owner, SECURITY DEFINER, the pinned
-- search_path and EXECUTE = {role_maintenance} are re-asserted (CREATE OR REPLACE keeps them).

CREATE OR REPLACE FUNCTION control.credential_pepper_epoch_advance()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
  v_epoch integer;
BEGIN
  UPDATE control.credential_pepper_state SET epoch = epoch + 1, rehash_open = true
   WHERE id AND NOT rehash_open
  RETURNING epoch INTO v_epoch;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'rehash_window_open' USING ERRCODE = '55000',
      HINT = 'close the open window first: humaux-maintenance apikey pepper-epoch close (runbook 10.2)';
  END IF;
  RETURN v_epoch;
END;
$$;
ALTER FUNCTION control.credential_pepper_epoch_advance() OWNER TO role_migration_owner;
REVOKE ALL ON FUNCTION control.credential_pepper_epoch_advance() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control.credential_pepper_epoch_advance() TO role_maintenance;

COMMENT ON FUNCTION control.credential_pepper_epoch_advance() IS
  '§73.5 / ADR-0059 D-H: moves to the next pepper epoch and opens the rehash window (runbook Rotate, '
  'pepper phase 3); refused 55000 rehash_window_open while the window is already open (0205). '
  'role_maintenance only; role_gateway can never move the epoch or open the window.';
