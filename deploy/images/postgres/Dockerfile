# deploy/images/postgres/Dockerfile — the PostgreSQL 18 image with pgBackRest installed at build time
# (ADR-0064 D-H, R-37: pgBackRest lives IN the pinned PG18 image; nothing is installed at container start).
# One stage, `pg` (ruling E17: card 37 builds no mirror stage).
#   base: the linux/arm64 manifest of postgres:18.6 (multi-arch index sha256:5a5a84b1…; research addendum).
#   pgbackrest: exact version string from trixie-pgdg (the apt source the base image already carries); 2.59.3
#     carries the weak-subkey fix (pgBackRest NEWS), measured present by spike SP-4 (open point O2).
# deploy/pgbackrest/pgbackrest.conf is bind-mounted read-only by the compose files, never baked in.
# The entrypoint and PGDATA=/var/lib/postgresql/18/docker are the base image's. Built and run as linux/arm64.
# Pinned by T-H1 (`the_image_is_pinned_and_carries_pgbackrest`) and gate c37_image_pinned.
FROM postgres:18.6@sha256:f4fcd2b983483177b15d72d94fb3614736211554f7cac5c2cc54a05c75110b90 AS pg

ARG PGBACKREST_VERSION=2.59.3-1.pgdg13+1

RUN apt-get update \
 && apt-get install -y --no-install-recommends "pgbackrest=${PGBACKREST_VERSION}" ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && pgbackrest version \
 && install -d -o postgres -g postgres -m 0750 /var/log/pgbackrest /var/lib/pgbackrest /tmp/pgbackrest
