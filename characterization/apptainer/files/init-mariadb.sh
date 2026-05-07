#!/bin/bash
# init-mariadb.sh — first-run seeding for the SIF's MariaDB data dir.
#
# The SIF's filesystem is read-only at runtime, so the data dir baked
# in during %post lives at /var/lib/mysql-seed. The runtime data dir
# at /var/lib/mysql is a bind-mounted writable host path. On first run
# the host path is empty; we copy the seed into it. On later runs the
# host path already has data and we leave it alone.
#
# Usage:
#   init-mariadb.sh [command...]
#
# After seeding, exec's the trailing command (or a no-op if none).
# Designed to be the SIF's %runscript entry point.

set -eu

DATA_DIR="${MARIADB_DATA_DIR:-/var/lib/mysql}"
SEED_DIR="/var/lib/mysql-seed"

if [ ! -d "$SEED_DIR" ]; then
    echo "[init-mariadb] FATAL: seed dir $SEED_DIR missing — was the SIF built correctly?" >&2
    exit 1
fi

mkdir -p "$DATA_DIR"

if [ -z "$(ls -A "$DATA_DIR" 2>/dev/null)" ]; then
    echo "[init-mariadb] First run — seeding $DATA_DIR from $SEED_DIR ..."
    cp -a "$SEED_DIR"/. "$DATA_DIR"/
    echo "[init-mariadb] Seed copy complete."
else
    echo "[init-mariadb] $DATA_DIR already populated; skipping seed."
fi

if [ "$#" -gt 0 ]; then
    exec "$@"
fi
