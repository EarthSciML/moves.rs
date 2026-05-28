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

# Wait for port 3306 to be free before starting mariadbd.
# Other MOVES/mariadbd instances on this host share port 3306 (Apptainer uses
# the host network namespace). Starting mariadbd while 3306 is taken wastes
# memory (InnoDB buffer pool allocation) and may trigger the SLURM OOM killer.
# Poll cheaply instead and only start mariadbd once the port is clear.
MAX_WAIT=1200  # 20 minutes total
wait_secs=0
while [ "$wait_secs" -lt "$MAX_WAIT" ]; do
    # TCP check: succeed (port busy) → wait; fail (port free) → proceed.
    if ! bash -c 'exec 3<>/dev/tcp/127.0.0.1/3306' 2>/dev/null; then
        break
    fi
    [ "$wait_secs" -eq 0 ] && echo "[start-mariadb-bg] port 3306 busy, waiting for it to clear..." >&2
    sleep 5
    wait_secs=$((wait_secs + 5))
done

if [ "$wait_secs" -ge "$MAX_WAIT" ]; then
    echo "[start-mariadb-bg] port 3306 still busy after ${MAX_WAIT}s, giving up." >&2
    exit 1
fi

# Start mariadbd. If it fails to bind (another process beat us to port 3306),
# wait for the port to clear again and retry once.
for start_attempt in 1 2; do
    /usr/sbin/mariadbd \
        --user="$EFFECTIVE_USER" \
        --datadir="$DATA_DIR" \
        --socket="$SOCK_PATH" \
        --pid-file="$PID_FILE" \
        --log-error="$LOG_FILE" \
        --bind-address=127.0.0.1 \
        --skip-networking=0 \
        "$@" &
    MARIADBD_BG_PID=$!

    # Wait for socket. In no-root mode MariaDB runs as the calling user, so
    # unix_socket auth for root is unavailable. Use the moves/moves account
    # (provisioned during %post) for the readiness probe instead.
    for i in $(seq 1 60); do
        if [ -S "$SOCK_PATH" ] && \
           /usr/bin/mariadb --socket="$SOCK_PATH" -umoves -pmoves -e "SELECT 1" >/dev/null 2>&1; then
            echo "[start-mariadb-bg] mariadbd ready (PID $(cat "$PID_FILE" 2>/dev/null))."
            exit 0
        fi
        sleep 1
    done

    # Startup failed (likely port race). Kill any lingering mariadbd and wait
    # for port 3306 to clear before the second attempt.
    kill "$MARIADBD_BG_PID" 2>/dev/null || true
    wait "$MARIADBD_BG_PID" 2>/dev/null || true
    if [ "$start_attempt" -lt 2 ]; then
        echo "[start-mariadb-bg] mariadbd failed to start (port race?); waiting for port 3306..." >&2
        while bash -c 'exec 3<>/dev/tcp/127.0.0.1/3306' 2>/dev/null; do sleep 5; done
    fi
done

echo "[start-mariadb-bg] mariadbd failed to start after 2 attempts. Log:" >&2
[ -f "$LOG_FILE" ] && tail -100 "$LOG_FILE" >&2 || true
exit 1
