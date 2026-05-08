#!/bin/bash
# run-moves.sh — convenience wrapper for invoking MOVES inside the SIF.
# Sets up the bind-mount layout, starts MariaDB inside the container,
# runs the requested ant target, then shuts MariaDB down cleanly.
#
# Designed to work BOTH with and without --fakeroot. The non-fakeroot
# path uses start-mariadb-bg.sh which runs mariadbd as the calling
# user. The fakeroot path uses `service mariadb start`, matching the
# canonical validation command in the bead.
#
# Usage:
#   ./run-moves.sh [-f|--fakeroot] [--runspec PATH] [-- <ant-target> ...]
#
# Default: ant target = crun, runspec = testdata/SampleRunSpec.xml.
#
# Environment:
#   SIF                   Path to canonical-moves.sif (default: ./canonical-moves.sif)
#   WORKDIR               Host scratch root (default: /scratch/${USER}/moves-canonical)
#   MARIADB_DATA          Override datadir bind path
#   MARIADB_SOCK_DIR      Override socket-dir bind path
#   MOVES_TEMP            Override MOVESTemporary bind path
#   WORKER_DIR            Override WorkerFolder bind path
#   JAVA_TOOL_OPTIONS     If set on the host, propagated into the container so
#                         every JVM (ant, the forked MOVES JVM) honors it.
#                         Used by run-fixture.sh for Phase 0 Task 8 (mo-d7or)
#                         class-load instrumentation; harmless when unset.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SIF="${SIF:-${HERE}/canonical-moves.sif}"
WORKDIR="${WORKDIR:-/scratch/${USER}/moves-canonical}"
MARIADB_DATA="${MARIADB_DATA:-${WORKDIR}/mariadb-data}"
MARIADB_SOCK_DIR="${MARIADB_SOCK_DIR:-${WORKDIR}/run-mysqld}"
MOVES_TEMP="${MOVES_TEMP:-${WORKDIR}/MOVESTemporary}"
WORKER_DIR="${WORKER_DIR:-${WORKDIR}/WorkerFolder}"

USE_FAKEROOT=0
RUNSPEC="testdata/SampleRunSpec.xml"
ANT_ARGS=( crun )

while [ $# -gt 0 ]; do
    case "$1" in
        -f|--fakeroot)
            USE_FAKEROOT=1; shift ;;
        --runspec)
            RUNSPEC="$2"; shift 2 ;;
        --)
            shift
            ANT_ARGS=( "$@" )
            break ;;
        *)
            echo "Unknown arg: $1" >&2
            echo "Usage: $0 [-f|--fakeroot] [--runspec PATH] [-- <ant-target> ...]" >&2
            exit 2 ;;
    esac
done

if [ ! -f "${SIF}" ]; then
    echo "FATAL: SIF ${SIF} not found. Run build-sif.sh first." >&2
    exit 2
fi

mkdir -p "${MARIADB_DATA}" "${MARIADB_SOCK_DIR}" "${MOVES_TEMP}" "${WORKER_DIR}"

# Apptainer bind args. The SIF stores the seed DB at /var/lib/mysql-seed
# (read-only); init-mariadb.sh copies it into /var/lib/mysql on first run.
BINDS=(
    --bind "${MARIADB_DATA}:/var/lib/mysql"
    --bind "${MARIADB_SOCK_DIR}:/var/run/mysqld"
    --bind "${MOVES_TEMP}:/opt/moves/MOVESTemporary"
    --bind "${WORKER_DIR}:/opt/moves/WorkerFolder"
)

FAKEROOT_FLAG=()
if [ "${USE_FAKEROOT}" = "1" ]; then
    FAKEROOT_FLAG=( --fakeroot )
    START_CMD="service mariadb start"
else
    START_CMD="/opt/moves-bin/start-mariadb-bg.sh"
fi

# Propagate JAVA_TOOL_OPTIONS into the container if the caller set it.
# Apptainer scrubs the host environment by default; an explicit --env
# is required for opt-in passthrough. JAVA_TOOL_OPTIONS is the JVM-spec
# env knob honored by every JVM unconditionally, so anything we set
# here is picked up by ant and the forked MOVES JVM both.
EXTRA_ENV_ARGS=()
if [ -n "${JAVA_TOOL_OPTIONS:-}" ]; then
    EXTRA_ENV_ARGS+=( --env "JAVA_TOOL_OPTIONS=${JAVA_TOOL_OPTIONS}" )
fi

ANT_ARGS_QUOTED="$(printf '%q ' "${ANT_ARGS[@]}")"
RUNSPEC_QUOTED="$(printf '%q' "${RUNSPEC}")"

apptainer exec \
    "${FAKEROOT_FLAG[@]}" \
    "${BINDS[@]}" \
    "${EXTRA_ENV_ARGS[@]}" \
    "${SIF}" \
    bash -c "
        set -eu
        /opt/moves-bin/init-mariadb.sh
        ${START_CMD}
        # Wait for socket if 'service' returned without blocking.
        for i in \$(seq 1 60); do
            [ -S /var/run/mysqld/mysqld.sock ] && break
            sleep 1
        done
        cd /opt/moves
        ant ${ANT_ARGS_QUOTED} -Drunspec=${RUNSPEC_QUOTED}
        STATUS=\$?
        # Stop mariadbd cleanly so the next run sees a consistent datadir.
        if [ '${USE_FAKEROOT}' = '1' ]; then
            service mariadb stop || true
        else
            mariadb-admin --socket=/var/run/mysqld/mysqld.sock -uroot shutdown 2>/dev/null || \
              kill \"\$(cat /var/run/mysqld/mariadbd.pid 2>/dev/null)\" 2>/dev/null || true
        fi
        exit \$STATUS
    "
