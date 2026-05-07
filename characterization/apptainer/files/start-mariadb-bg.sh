#!/bin/bash
# start-mariadb-bg.sh — start mariadbd in the background as the calling
# user (no fakeroot required). For non-fakeroot invocations.
#
# Assumes init-mariadb.sh has already run (data dir is populated).
#
# Returns once mariadbd is accepting connections on the socket.
# On failure, dumps the log and exits non-zero.

set -eu

DATA_DIR="${MARIADB_DATA_DIR:-/var/lib/mysql}"
SOCK_DIR="${MARIADB_SOCKET_DIR:-/var/run/mysqld}"
SOCK_PATH="${SOCK_DIR}/mysqld.sock"
LOG_FILE="${MARIADB_LOG:-${SOCK_DIR}/mariadb-error.log}"
PID_FILE="${SOCK_DIR}/mariadbd.pid"

mkdir -p "$SOCK_DIR"

# Run as the calling user (override mysql:mysql since we lack fakeroot).
EFFECTIVE_USER="$(id -un)"

/usr/sbin/mariadbd \
    --user="$EFFECTIVE_USER" \
    --datadir="$DATA_DIR" \
    --socket="$SOCK_PATH" \
    --pid-file="$PID_FILE" \
    --log-error="$LOG_FILE" \
    --bind-address=127.0.0.1 \
    --skip-networking=0 \
    "$@" &

# Wait for socket
for i in $(seq 1 60); do
    if [ -S "$SOCK_PATH" ] && \
       /usr/bin/mariadb --socket="$SOCK_PATH" -uroot -e "SELECT 1" >/dev/null 2>&1; then
        echo "[start-mariadb-bg] mariadbd ready (PID $(cat "$PID_FILE" 2>/dev/null))."
        exit 0
    fi
    sleep 1
done

echo "[start-mariadb-bg] mariadbd failed to start within 60s. Log:" >&2
[ -f "$LOG_FILE" ] && tail -100 "$LOG_FILE" >&2 || true
exit 1
