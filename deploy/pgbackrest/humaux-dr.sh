#!/bin/sh
# deploy/pgbackrest/humaux-dr.sh <humaux-maintenance arm ...> | compose <docker compose args ...>
# The one entry point of the host crontab and the operator for every DR arm (ADR-0064 D-I, ruling E10 / 10.11 J).
# It loads $HOME/.config/humaux/dr.env — the only carrier of HUMAUX_MAINTENANCE_PG_DSN, PGBACKREST_REPO1_CIPHER_PASS
# and the HUMAUX_MAINTENANCE_{BACKUP,DRILL,RESTORE}_* / HUMAUX_PG_REPO_DIR keys — and refuses (exit 3) unless that
# file is a regular file, mode 600, owned by the caller; cron's stripped environment is never the carrier. It also
# refuses (exit 3) when the binary it would exec is not on the PATH that dr.env leaves.
# `compose ...` runs `docker compose -f $HUMAUX_MAINTENANCE_BACKUP_COMPOSE_FILE -p $HUMAUX_MAINTENANCE_BACKUP_PROJECT ...`
# under the same environment (bringing `pg` up). No value lives in this file. Pinned by T-I1 and c37_dr_script_parses.
set -eu
env_file="${HOME:?}/.config/humaux/dr.env"
if [ ! -f "$env_file" ] || [ -L "$env_file" ]; then
  echo "humaux-dr: refused — $env_file is not a regular file" >&2
  exit 3
fi
mode=$(stat -c %a "$env_file" 2>/dev/null || stat -f %Lp "$env_file")
owner=$(stat -c %u "$env_file" 2>/dev/null || stat -f %u "$env_file")
if [ "$mode" != "600" ] || [ "$owner" != "$(id -u)" ]; then
  echo "humaux-dr: refused — $env_file must be mode 600 and owned by uid $(id -u) (is mode $mode, uid $owner)" >&2
  exit 3
fi
set -a
. "$env_file"
set +a
# Cron's PATH is /usr/bin:/bin; dr.env sets PATH (runbook §11.1 step 2). A binary off PATH is a named refusal, not 127.
need=humaux-maintenance
[ "${1:-}" = compose ] && need=docker
if ! command -v "$need" >/dev/null 2>&1; then
  echo "humaux-dr: refused — $need is not on PATH ($PATH); set PATH in $env_file" >&2
  exit 3
fi
if [ "${1:-}" = compose ]; then
  shift
  exec docker compose -f "$HUMAUX_MAINTENANCE_BACKUP_COMPOSE_FILE" -p "$HUMAUX_MAINTENANCE_BACKUP_PROJECT" "$@"
fi
exec humaux-maintenance "$@"
