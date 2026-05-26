#!/bin/bash
# run-moves.sh — convenience wrapper for invoking MOVES inside the SIF.
# Sets up the bind-mount layout, starts MariaDB inside the container,
# runs the requested ant target, then shuts MariaDB down cleanly.
#
# Designed to work BOTH with and without --fakeroot. Both modes use
# start-mariadb-bg.sh, which runs mariadbd directly as the calling user.
# In fakeroot mode the calling user is root so MariaDB runs with
# --user=root; in no-root mode it runs as the HPC user with moves/moves
# credentials for the readiness probe. Avoids mariadbd-safe-helper, which
# cannot setuid(mysql) in root-mapped-namespace fakeroot environments.
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
#   NONROAD_TEMP          Override NonroadTemporaryData bind path
#   JAVA_TOOL_OPTIONS     If set on the host, propagated into the container so
#                         every JVM (ant, the forked MOVES JVM) honors it.
#                         Used by run-fixture.sh for Phase 0 Task 8 (mo-d7or)
#                         class-load instrumentation; harmless when unset.
#   NRDBG_FILE            If set, propagated into the container so the
#                         instrumented NONROAD.exe writes its intermediate-state
#                         TSV to that path (container-side). Set by
#                         generate-corpus.sh (nonroad-fidelity/) to the
#                         /opt/moves/MOVESTemporary/<fixture>.tsv path that is
#                         bind-mounted back to the host scratch directory.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SIF="${SIF:-${HERE}/canonical-moves.sif}"
WORKDIR="${WORKDIR:-/scratch/${USER}/moves-canonical}"
MARIADB_DATA="${MARIADB_DATA:-${WORKDIR}/mariadb-data}"
MARIADB_SOCK_DIR="${MARIADB_SOCK_DIR:-${WORKDIR}/run-mysqld}"
MOVES_TEMP="${MOVES_TEMP:-${WORKDIR}/MOVESTemporary}"
WORKER_DIR="${WORKER_DIR:-${WORKDIR}/WorkerFolder}"
NONROAD_TEMP="${NONROAD_TEMP:-${WORKDIR}/NonroadTemporaryData}"

USE_FAKEROOT=0
RUNSPEC="testdata/SampleRunSpec.xml"
ANT_ARGS=( main1worker )

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

mkdir -p "${MARIADB_DATA}" "${MARIADB_SOCK_DIR}" "${MOVES_TEMP}" "${WORKER_DIR}" "${NONROAD_TEMP}" \
         "${WORKER_DIR}/sharedwork" "${WORKER_DIR}/worker-temp"

# Copy config overrides to workdir so MOVES can write to them (MOVES rejects read-only configs).
MANYWORKERS_CONF="${WORKDIR}/manyworkers.txt"
MAKETODO_CONF="${WORKDIR}/maketodo.txt"
cp "${HERE}/files/manyworkers.txt" "${MANYWORKERS_CONF}"
cp "${HERE}/files/maketodo.txt" "${MAKETODO_CONF}"

# Apptainer bind args. The SIF stores the seed DB at /var/lib/mysql-seed
# (read-only); init-mariadb.sh copies it into /var/lib/mysql on first run.
# files/start-mariadb-bg.sh is the authoritative source for this script and
# is bind-mounted read-only over the SIF's baked-in copy so changes take
# effect without rebuilding the SIF.
BINDS=(
    --bind "${MARIADB_DATA}:/var/lib/mysql"
    --bind "${MARIADB_SOCK_DIR}:/var/run/mysqld"
    --bind "${MOVES_TEMP}:/opt/moves/MOVESTemporary"
    --bind "${WORKER_DIR}:/opt/moves/WorkerFolder"
    --bind "${NONROAD_TEMP}:/opt/moves/NonroadTemporaryData"
    --bind "${MANYWORKERS_CONF}:/opt/moves/manyworkers.txt"
    --bind "${MAKETODO_CONF}:/opt/moves/maketodo.txt"
    --bind "${HERE}/files/start-mariadb-bg.sh:/opt/moves-bin/start-mariadb-bg.sh:ro"
)

FAKEROOT_FLAG=()
if [ "${USE_FAKEROOT}" = "1" ]; then
    FAKEROOT_FLAG=( --fakeroot )
fi
# Both fakeroot and no-root modes use start-mariadb-bg.sh (direct mariadbd).
# In fakeroot, id -un == root so mariadbd runs with --user=root and root
# auth succeeds. In no-root mode, the SIF's moves/moves account is used.
START_CMD="/opt/moves-bin/start-mariadb-bg.sh"

# Propagate opt-in env vars into the container.
# Apptainer scrubs the host environment by default; an explicit --env
# is required for passthrough.
EXTRA_ENV_ARGS=()
if [ -n "${JAVA_TOOL_OPTIONS:-}" ]; then
    EXTRA_ENV_ARGS+=( --env "JAVA_TOOL_OPTIONS=${JAVA_TOOL_OPTIONS}" )
fi
if [ -n "${NRDBG_FILE:-}" ]; then
    EXTRA_ENV_ARGS+=( --env "NRDBG_FILE=${NRDBG_FILE}" )
fi

ANT_ARGS_QUOTED="$(printf '%q ' "${ANT_ARGS[@]}")"
RUNSPEC_QUOTED="$(printf '%q' "${RUNSPEC}")"

apptainer exec \
    "${FAKEROOT_FLAG[@]}" \
    --writable-tmpfs \
    "${BINDS[@]}" \
    "${EXTRA_ENV_ARGS[@]}" \
    "${SIF}" \
    bash -c "
        set -eu
        /opt/moves-bin/init-mariadb.sh
        ${START_CMD}
        # %environment sets JAVA_HOME to Temurin 17, but the apt-installed 'ant'
        # package pulled in OpenJDK 21 as a dependency, so the classes in the SIF
        # were compiled by javac 21 (class file version 65). Override JAVA_HOME to
        # the OpenJDK 21 present in the SIF so the forked JVM matches.
        export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
        cd /opt/moves
        ant ${ANT_ARGS_QUOTED} -Drunspec=${RUNSPEC_QUOTED}
        STATUS=\$?
        # Stop mariadbd cleanly so the next run sees a consistent datadir.
        mariadb-admin --socket=/var/run/mysqld/mysqld.sock -umoves -pmoves shutdown 2>/dev/null || \
          kill \"\$(cat /var/run/mysqld/mariadbd.pid 2>/dev/null)\" 2>/dev/null || true
        exit \$STATUS
    "
