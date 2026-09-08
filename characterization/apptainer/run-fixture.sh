#!/bin/bash
# run-fixture.sh — fixture-capture orchestrator (Phase 0 Task 4).
#
# Wraps a fixture run end-to-end: sets up bind-mounts, runs the patched
# MOVES inside moves-fixture.sif against the supplied RunSpec, dumps the
# resulting MariaDB databases to TSV, stages MOVESTemporary/ and
# WorkerFolder/ into a captures directory, and invokes
# `moves-fixture-capture` to produce a deterministic snapshot under
# `characterization/snapshots/<fixture-name>/`.
#
# The on-disk snapshot is a function of:
#   * the SIF SHA256 (pinned in characterization/fixture-image.lock)
#   * the RunSpec bytes
# Two runs of this script against the same inputs produce byte-identical
# snapshot files — that's the bead's "deterministic given the same inputs"
# acceptance criterion.
#
# Usage:
#   ./run-fixture.sh [-f|--fakeroot] --runspec PATH [options]
#
# Required:
#   --runspec PATH        RunSpec XML to execute. Path on the host.
#
# Optional:
#   -f, --fakeroot        Pass --fakeroot to apptainer exec. Both modes start
#                         mariadbd via start-mariadb-bg.sh; in fakeroot mode
#                         the effective user is root so mariadbd runs with
#                         --user=root; without it, mariadbd runs as the calling
#                         user. GitHub-hosted runners: do NOT use --fakeroot —
#                         MariaDB startup fails under fakeroot on hosted runners.
#   --sif PATH            moves-fixture.sif path (default: ./moves-fixture.sif).
#   --workdir DIR         Host scratch root (default: /scratch/$USER/moves-fixture/<fixture>).
#   --output-dir DIR      Snapshot output directory
#                         (default: ../snapshots/<fixture-name>/ relative to
#                         this script).
#   --min-tables N        Refuse to publish a snapshot holding fewer than N
#                         tables (default: 8; env MOVES_FIXTURE_MIN_TABLES).
#                         The smallest real snapshot in
#                         characterization/snapshots/ has 316 tables, so this
#                         floor only trips on a catastrophically empty run.
#   --keep-captures       Don't delete the staged captures directory after
#                         the snapshot is built. Useful for forensics.
#   --skip-run            Skip the MOVES execution. Captures and snapshot
#                         only — assumes a previous run's scratch is intact.
#
# Environment:
#   MOVES_FIXTURE_CAPTURE_BIN  Path to the moves-fixture-capture binary
#                              (default: cargo's target/release/ then debug/).
#
# Failure semantics (issue #56):
#   Exit 0 from this script means one thing only: a snapshot holding at
#   least --min-tables tables is on disk at OUTPUT_DIR. Every other outcome
#   exits non-zero, prints a "CAPTURE FAILED — NO SNAPSHOT WRITTEN" banner
#   on stderr, and leaves no snapshot (and no partial snapshot) behind.
#
#   Note that ant invokes MOVES via <java fork="yes"> WITHOUT
#   failonerror="true" (MOVES's own build.xml, target main1worker). A MOVES
#   JVM that dies non-zero is therefore reported only as an
#   "[java] Java Result: N" line while ant, apptainer, run-moves.sh and
#   hence this script all see status 0. The MOVES exit status is not merely
#   swallowed somewhere in the pipeline — it is never produced. Scanning the
#   run log for that marker is consequently the primary MOVES-failure
#   detector, not a nicety; the table-count checks are the second belt.
#
# Determinism notes:
#   * MariaDB dumps use `SELECT col1, col2, ... FROM table ORDER BY 1, 2,
#     ..., N` so on-disk row order is column-lexicographic.
#   * Schemas are dumped from INFORMATION_SCHEMA in ORDINAL_POSITION order.
#   * The Rust capture step's directory walk is sorted lexicographically.
#   * The snapshot crate's parquet output is uncompressed, dictionary-disabled,
#     statistics-disabled, with a fixed `created_by` stamp.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/.." && pwd)"   # characterization/
REPO_ROOT="$(cd "${ROOT}/.." && pwd)"

# ----- Defaults -----
USE_FAKEROOT=0
RUNSPEC=""
SIF="${HERE}/moves-fixture.sif"
WORKDIR=""
OUTPUT_DIR=""
KEEP_CAPTURES=0
SKIP_RUN=0
MIN_TABLES="${MOVES_FIXTURE_MIN_TABLES:-8}"

usage() {
    sed -n '2,/^set -euo pipefail/p' "$0" | sed 's/^#\s\?//' | head -n -1
}

# ----- Arg parsing -----
while [ $# -gt 0 ]; do
    case "$1" in
        -f|--fakeroot)
            USE_FAKEROOT=1; shift ;;
        --runspec)
            RUNSPEC="$2"; shift 2 ;;
        --sif)
            SIF="$2"; shift 2 ;;
        --workdir)
            WORKDIR="$2"; shift 2 ;;
        --output-dir)
            OUTPUT_DIR="$2"; shift 2 ;;
        --min-tables)
            MIN_TABLES="$2"; shift 2 ;;
        --keep-captures)
            KEEP_CAPTURES=1; shift ;;
        --skip-run)
            SKIP_RUN=1; shift ;;
        -h|--help)
            usage
            exit 0 ;;
        *)
            echo "Unknown arg: $1" >&2
            usage >&2
            exit 2 ;;
    esac
done

if [ -z "${RUNSPEC}" ]; then
    echo "FATAL: --runspec is required." >&2
    exit 2
fi
if [ ! -f "${RUNSPEC}" ]; then
    echo "FATAL: RunSpec ${RUNSPEC} not found." >&2
    exit 2
fi
if [ ! -f "${SIF}" ]; then
    echo "FATAL: SIF ${SIF} not found. Build it via build-fixture-sif.sh." >&2
    exit 2
fi
if ! command -v apptainer >/dev/null 2>&1; then
    echo "FATAL: apptainer not found in PATH." >&2
    exit 2
fi
case "${MIN_TABLES}" in
    ''|*[!0-9]*)
        echo "FATAL: --min-tables must be a positive integer (got '${MIN_TABLES}')." >&2
        exit 2 ;;
esac
if [ "${MIN_TABLES}" -lt 1 ]; then
    echo "FATAL: --min-tables must be >= 1 (got '${MIN_TABLES}')." >&2
    exit 2
fi
# A relative --runspec passes the host-side -f test above but is resolved
# INSIDE the container against /opt/moves, where it almost certainly does not
# exist; MOVES then dies with "The specified runspec file does not exist"
# while ant still exits 0. That is the reported repro for issue #56. The
# guards below turn it into a hard failure, but warn early so the operator
# sees the cause and not just the symptom.
case "${RUNSPEC}" in
    /*) : ;;
    *)  echo "[run-fixture] WARNING: --runspec '${RUNSPEC}' is a relative path." >&2
        echo "[run-fixture] WARNING: it is resolved inside the container against" >&2
        echo "[run-fixture] WARNING: /opt/moves, not against your shell's cwd." >&2
        echo "[run-fixture] WARNING: pass an absolute host path unless you really" >&2
        echo "[run-fixture] WARNING: mean a path that exists inside the SIF." >&2
        ;;
esac

# ----- Derive fixture name from RunSpec filename -----
RUNSPEC_BASENAME="$(basename "${RUNSPEC}")"
RUNSPEC_STEM="${RUNSPEC_BASENAME%.*}"
# Sanitize: lowercase, allow [a-z0-9_-], replace anything else with '_'.
FIXTURE_NAME="$(printf '%s' "${RUNSPEC_STEM}" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/_/g')"
[ -n "${FIXTURE_NAME}" ] || FIXTURE_NAME="unnamed"

WORKDIR="${WORKDIR:-/scratch/${USER}/moves-fixture/${FIXTURE_NAME}}"
OUTPUT_DIR="${OUTPUT_DIR:-${ROOT}/snapshots/${FIXTURE_NAME}}"

MARIADB_DATA="${WORKDIR}/mariadb-data"
MARIADB_SOCK_DIR="${WORKDIR}/run-mysqld"
MOVES_TEMP="${WORKDIR}/MOVESTemporary"
WORKER_DIR="${WORKDIR}/WorkerFolder"
CAPTURES_DIR="${WORKDIR}/captures"
MOVES_LOG="${WORKDIR}/moves-run.log"
DUMP_LOG="${WORKDIR}/dump-databases.log"

# Phase 0 Task 8 (mo-d7or): JVM class-load logs land here so the
# moves-fixture-capture trace builder can pick them up alongside the
# worker.sql files. Materialized inside MOVESTemporary so the existing
# bind-mount layout carries it into the container at
# /opt/moves/MOVESTemporary/instrumentation/, and so the existing
# captures-step copy of MOVESTemporary into captures/moves-temporary/
# carries it back out without further wiring.
INSTRUMENTATION_DIR="${MOVES_TEMP}/instrumentation"

# ----- Failure handling (issue #56) -----
# Every abort path funnels through fail(): it tears down any half-written
# staging directory, prints an unambiguous banner on stderr, and exits
# non-zero. Nothing below is allowed to reach "[run-fixture] done." unless a
# snapshot with real tables is on disk.
STAGING_DIR=""
PREV_DIR=""

cleanup_incomplete() {
    # Never leave a partial snapshot behind. Drop the staging tree, and if we
    # had already moved a previous good snapshot aside, put it back.
    if [ -n "${STAGING_DIR}" ] && [ -d "${STAGING_DIR}" ]; then
        rm -rf "${STAGING_DIR}" || true
    fi
    if [ -n "${PREV_DIR}" ] && [ -d "${PREV_DIR}" ]; then
        if [ -d "${OUTPUT_DIR}" ]; then
            rm -rf "${PREV_DIR}" || true
        else
            mv "${PREV_DIR}" "${OUTPUT_DIR}" || true
        fi
    fi
    STAGING_DIR=""
    PREV_DIR=""
    return 0
}

banner() {
    {
        echo
        echo "############################################################"
        echo "[run-fixture] CAPTURE FAILED — NO SNAPSHOT WRITTEN"
        echo "[run-fixture]   fixture : ${FIXTURE_NAME}"
        echo "[run-fixture]   reason  : $*"
        echo "[run-fixture]   snapshot: ${OUTPUT_DIR}"
        if [ -f "${MOVES_LOG}" ]; then
            echo "[run-fixture]   moves log: ${MOVES_LOG}"
        fi
        if [ -f "${DUMP_LOG}" ]; then
            echo "[run-fixture]   dump log : ${DUMP_LOG}"
        fi
        echo "############################################################"
    } >&2
}

fail() {
    cleanup_incomplete
    banner "$@"
    exit 1
}

# An unanticipated non-zero status is exactly as dangerous as a MOVES
# failure, so route set -e trips through the same banner instead of dying
# quietly mid-script.
on_unexpected_error() {
    local rc="$1" line="$2"
    cleanup_incomplete
    banner "unexpected non-zero status ${rc} at line ${line}"
    exit "${rc}"
}
trap 'on_unexpected_error "$?" "${LINENO}"' ERR
trap cleanup_incomplete EXIT

# ant's <java> task for MOVES has no failonerror="true", so these markers in
# the run log are the only evidence that MOVES failed.
MOVES_FAILURE_MARKERS=(
    'BUILD FAILED'
    'The specified runspec file does not exist'
    'ERROR: A runspec was not provided'
)

scan_moves_log() {
    local log="$1"
    if [ ! -s "${log}" ]; then
        fail "MOVES produced no output at all (${log} is empty or missing)"
    fi

    # Any non-zero "Java Result: N" means the forked MOVES JVM died even
    # though ant reported success.
    local codes
    codes="$(sed -nE 's/.*Java Result:[[:space:]]*(-?[0-9]+).*/\1/p' "${log}" \
             | grep -vx '0' | sort -u | tr '\n' ' ' || true)"
    codes="${codes% }"
    if [ -n "${codes}" ]; then
        fail "ant reported non-zero Java Result (${codes}) — MOVES failed while ant exited 0"
    fi

    local marker
    for marker in "${MOVES_FAILURE_MARKERS[@]}"; do
        if grep -Fq -- "${marker}" "${log}"; then
            fail "MOVES run log contains failure marker: ${marker}"
        fi
    done
}

# Number of dumped data tables staged in the captures dir (schema sidecars
# don't count).
count_capture_tables() {
    local n=0
    if [ -d "${CAPTURES_DIR}/databases" ]; then
        n="$(find "${CAPTURES_DIR}/databases" -type f -name '*.tsv' \
             ! -name '*.schema.tsv' | wc -l)"
    fi
    printf '%s' "${n}"
}

# Wipe the per-fixture MariaDB datadir before each run so init-mariadb.sh
# re-seeds from scratch. Otherwise MOVES INSERTs into the OUT tables left
# behind from previous runs, producing duplicate MOVESRunID rows (e.g. a
# second canonical run on the same fixture doubles MOVESOutput row count).
# Override with KEEP_MARIADB_DATA=1 if you intentionally want to reuse the
# datadir (e.g. for a fast-iterate debugging loop).
if [ "${KEEP_MARIADB_DATA:-0}" != "1" ]; then
    rm -rf "${MARIADB_DATA}"
fi
mkdir -p "${WORKDIR}" "${MARIADB_DATA}" "${MARIADB_SOCK_DIR}" "${MOVES_TEMP}" "${WORKER_DIR}" "${INSTRUMENTATION_DIR}"

echo "[run-fixture] fixture_name = ${FIXTURE_NAME}"
echo "[run-fixture] workdir      = ${WORKDIR}"
echo "[run-fixture] output_dir   = ${OUTPUT_DIR}"
echo "[run-fixture] sif          = ${SIF}"

# ----- Step 1: run patched MOVES inside the SIF -----
if [ "${SKIP_RUN}" = "0" ]; then
    echo "[run-fixture] step 1/3 — executing MOVES via run-moves.sh"
    FAKEROOT_ARGS=()
    [ "${USE_FAKEROOT}" = "1" ] && FAKEROOT_ARGS=( -f )

    # Wait for MOVES master port 13131 to be free. MOVES hardcodes this port
    # for master-worker sockets; concurrent instances on the same host conflict
    # and the second MOVES fails at startup. start-mariadb-bg.sh already waits
    # for port 3306; mirror that pattern here for 13131.
    MAX_WAIT_MOVES=1200
    wait_secs_moves=0
    while [ "${wait_secs_moves}" -lt "${MAX_WAIT_MOVES}" ]; do
        bash -c 'exec 3<>/dev/tcp/127.0.0.1/13131' 2>/dev/null || break
        [ "${wait_secs_moves}" -eq 0 ] && echo "[run-fixture] port 13131 busy, waiting for other MOVES instance..." >&2
        sleep 5
        wait_secs_moves=$((wait_secs_moves + 5))
    done
    if [ "${wait_secs_moves}" -ge "${MAX_WAIT_MOVES}" ]; then
        echo "[run-fixture] port 13131 still busy after ${MAX_WAIT_MOVES}s, giving up." >&2
        exit 1
    fi

    # Phase 0 Task 8 (mo-d7or): tell every JVM under this run to log
    # class-load events into a per-PID file under MOVESTemporary/
    # instrumentation/. The %p substitution gives each forked JVM its
    # own filename so ant's own loads don't overwrite the MOVES JVM's.
    # `class+load=info` is the unified-logging tag for the load event;
    # output lines look like
    #   [0.123s][info][class,load] gov.epa.otaq.moves.master...
    # which moves-fixture-capture's trace builder filters down to the
    # `gov.epa.otaq.moves.*` package.
    FIXTURE_JAVA_TOOL_OPTIONS="-Xlog:class+load=info:file=/opt/moves/MOVESTemporary/instrumentation/class-load-%p.log"

    # Tee the run to a log so the "Java Result" scan below has something to
    # read, and take run-moves.sh's own status from PIPESTATUS rather than
    # the pipeline's (tee always succeeds). set +e around the pipeline so we
    # reach the explicit checks instead of dying inside the ERR trap with a
    # less useful message.
    set +e
    SIF="${SIF}" \
    WORKDIR="${WORKDIR}" \
    MARIADB_DATA="${MARIADB_DATA}" \
    MARIADB_SOCK_DIR="${MARIADB_SOCK_DIR}" \
    MOVES_TEMP="${MOVES_TEMP}" \
    WORKER_DIR="${WORKER_DIR}" \
    JAVA_TOOL_OPTIONS="${FIXTURE_JAVA_TOOL_OPTIONS}" \
        "${HERE}/run-moves.sh" "${FAKEROOT_ARGS[@]}" --runspec "${RUNSPEC}" \
        2>&1 | tee "${MOVES_LOG}"
    # Snapshot PIPESTATUS in one shot: reading it into a variable is itself a
    # command, which overwrites PIPESTATUS before a second read can see it.
    MOVES_PIPE_STATUS=( "${PIPESTATUS[@]}" )
    set -e
    MOVES_STATUS="${MOVES_PIPE_STATUS[0]:-0}"
    TEE_STATUS="${MOVES_PIPE_STATUS[1]:-0}"

    if [ "${TEE_STATUS}" -ne 0 ]; then
        fail "could not write the MOVES run log to ${MOVES_LOG} (tee exit ${TEE_STATUS})"
    fi
    if [ "${MOVES_STATUS}" -ne 0 ]; then
        fail "run-moves.sh exited ${MOVES_STATUS}"
    fi
    # ant exits 0 even when the forked MOVES JVM dies; the log is the only
    # witness. See the "Failure semantics" note in this file's header.
    scan_moves_log "${MOVES_LOG}"
    echo "[run-fixture] MOVES run OK (no failure markers in ${MOVES_LOG})"
else
    echo "[run-fixture] step 1/3 — skipped (--skip-run)"
    echo "[run-fixture] NOTE: --skip-run means the MOVES run itself is unverified;" >&2
    echo "[run-fixture] NOTE: the table-count guards below still apply." >&2
fi

# ----- Step 2: dump MariaDB databases to TSV -----
echo "[run-fixture] step 2/3 — dumping MariaDB databases to TSV"

# Kill any mariadbd left over from step 1 so the dump container can start
# MariaDB cleanly on the same port. The container's mariadb-admin shutdown
# may silently fail (fakeroot auth quirks), leaving an orphan on port 3306.
MARIADBD_PID_FILE="${MARIADB_SOCK_DIR}/mariadbd.pid"
if [ -f "${MARIADBD_PID_FILE}" ]; then
    OLD_PID="$(cat "${MARIADBD_PID_FILE}" 2>/dev/null || true)"
    if [ -n "${OLD_PID}" ]; then
        echo "[run-fixture] killing leftover mariadbd PID ${OLD_PID}"
        kill "${OLD_PID}" 2>/dev/null || true
    fi
    rm -f "${MARIADBD_PID_FILE}" "${MARIADB_SOCK_DIR}/mysqld.sock" 2>/dev/null || true
fi
# Also wait for port 3306 to clear (start-mariadb-bg.sh does this too,
# but an explicit wait here avoids a race between the kill and step 2's start).
for _w in $(seq 1 60); do
    bash -c 'exec 3<>/dev/tcp/127.0.0.1/3306' 2>/dev/null || break
    sleep 1
done

rm -rf "${CAPTURES_DIR}"
mkdir -p "${CAPTURES_DIR}/databases"

FAKEROOT_FLAG=()
if [ "${USE_FAKEROOT}" = "1" ]; then
    FAKEROOT_FLAG=( --fakeroot )
fi
# Always use start-mariadb-bg.sh (bind-mounted read-only) for the dump pass.
# "service mariadb start" is unreliable in fakeroot — the init.d script may
# find a stale PID file from step 1 and report "running but not responding"
# even when mariadbd is already stopped. start-mariadb-bg.sh handles port
# contention, retries, and the moves/moves readiness probe correctly.
START_MARIADB="/opt/moves-bin/start-mariadb-bg.sh"

# Bind-mount layout for the dump pass. The dump script and start-mariadb-bg.sh
# live next to this script on the host and are bind-mounted read-only into
# the container so we don't need to rebuild the SIF when they change.
BINDS=(
    --bind "${MARIADB_DATA}:/var/lib/mysql"
    --bind "${MARIADB_SOCK_DIR}:/var/run/mysqld"
    --bind "${CAPTURES_DIR}:/captures"
    --bind "${HERE}/dump-databases.sh:/opt/fixture-tools/dump-databases.sh:ro"
    --bind "${HERE}/files/start-mariadb-bg.sh:/opt/moves-bin/start-mariadb-bg.sh:ro"
)

set +e
apptainer exec \
    "${FAKEROOT_FLAG[@]}" \
    "${BINDS[@]}" \
    --env "START_MARIADB=${START_MARIADB}" \
    --env "CAPTURES_DIR=/captures" \
    --env "MARIADB_SKIP_NETWORKING=1" \
    "${SIF}" \
    bash /opt/fixture-tools/dump-databases.sh \
    2>&1 | tee "${DUMP_LOG}"
DUMP_PIPE_STATUS=( "${PIPESTATUS[@]}" )
set -e
DUMP_STATUS="${DUMP_PIPE_STATUS[0]:-0}"
DUMP_TEE_STATUS="${DUMP_PIPE_STATUS[1]:-0}"

if [ "${DUMP_TEE_STATUS}" -ne 0 ]; then
    fail "could not write the dump log to ${DUMP_LOG} (tee exit ${DUMP_TEE_STATUS})"
fi
if [ "${DUMP_STATUS}" -ne 0 ]; then
    fail "dump-databases.sh exited ${DUMP_STATUS} inside apptainer"
fi

# First belt: a captures dir with no dumped tables cannot produce a real
# snapshot, so refuse before the capture binary manufactures an empty one.
CAPTURED_TABLES="$(count_capture_tables)"
echo "[run-fixture] dumped ${CAPTURED_TABLES} table(s) into ${CAPTURES_DIR}/databases"
if [ "${CAPTURED_TABLES}" -eq 0 ]; then
    fail "captures directory holds no dumped tables — MOVES wrote no output databases"
fi

# ----- Step 3: stage MOVESTemporary and WorkerFolder; build snapshot -----
echo "[run-fixture] step 3/3 — building snapshot"

# Mirror MOVESTemporary/ and WorkerFolder/ into the captures directory.
copy_tree() {
    local src="$1" dst="$2"
    mkdir -p "${dst}"
    if [ -d "${src}" ] && [ -n "$(ls -A "${src}" 2>/dev/null || true)" ]; then
        if command -v rsync >/dev/null 2>&1; then
            rsync -a "${src}/" "${dst}/"
        else
            cp -a "${src}/." "${dst}/"
        fi
    fi
}
copy_tree "${MOVES_TEMP}" "${CAPTURES_DIR}/moves-temporary"
copy_tree "${WORKER_DIR}" "${CAPTURES_DIR}/worker-folder"

# Locate the moves-fixture-capture binary; build it on-demand if missing.
BIN="${MOVES_FIXTURE_CAPTURE_BIN:-}"
if [ -z "${BIN}" ]; then
    for cand in \
        "${REPO_ROOT}/target/release/moves-fixture-capture" \
        "${REPO_ROOT}/target/debug/moves-fixture-capture"; do
        if [ -x "${cand}" ]; then
            BIN="${cand}"
            break
        fi
    done
fi
if [ -z "${BIN}" ] || [ ! -x "${BIN}" ]; then
    echo "[run-fixture] building moves-fixture-capture (release)" >&2
    ( cd "${REPO_ROOT}" && cargo build --release -p moves-fixture-capture )
    BIN="${REPO_ROOT}/target/release/moves-fixture-capture"
fi

# Build into a staging directory next to the real one, verify it, and only
# then swap it into place. Writing straight into OUTPUT_DIR is what let a
# failed run leave a zero-table snapshot (or, worse, a half-written one)
# where a good snapshot used to be.
OUTPUT_PARENT="$(dirname "${OUTPUT_DIR}")"
OUTPUT_BASE="$(basename "${OUTPUT_DIR}")"
mkdir -p "${OUTPUT_PARENT}"
STAGING_DIR="${OUTPUT_PARENT}/.${OUTPUT_BASE}.staging.$$"
rm -rf "${STAGING_DIR}"
mkdir -p "${STAGING_DIR}"

set +e
"${BIN}" \
    --captures-dir "${CAPTURES_DIR}" \
    --runspec "${RUNSPEC}" \
    --sif-lockfile "${ROOT}/fixture-image.lock" \
    --output-dir "${STAGING_DIR}" \
    --fixture-name "${FIXTURE_NAME}"
CAPTURE_STATUS=$?
set -e
if [ "${CAPTURE_STATUS}" -ne 0 ]; then
    fail "moves-fixture-capture exited ${CAPTURE_STATUS}"
fi

# Second belt: the snapshot must actually hold tables, and enough of them.
if [ ! -f "${STAGING_DIR}/manifest.json" ]; then
    fail "moves-fixture-capture wrote no manifest.json"
fi
if [ -d "${STAGING_DIR}/tables" ]; then
    SNAPSHOT_TABLES="$(find "${STAGING_DIR}/tables" -maxdepth 1 -type f \
                       -name '*.parquet' | wc -l)"
else
    SNAPSHOT_TABLES=0
fi
if [ "${SNAPSHOT_TABLES}" -eq 0 ]; then
    fail "built snapshot holds 0 tables"
fi
if [ "${SNAPSHOT_TABLES}" -lt "${MIN_TABLES}" ]; then
    fail "built snapshot holds only ${SNAPSHOT_TABLES} table(s), below the --min-tables floor of ${MIN_TABLES}"
fi

# Publish: move any existing snapshot aside, move the staging tree into
# place, then drop the old one. cleanup_incomplete() restores the previous
# snapshot if anything goes wrong between the two moves.
if [ -d "${OUTPUT_DIR}" ]; then
    PREV_DIR="${OUTPUT_PARENT}/.${OUTPUT_BASE}.prev.$$"
    rm -rf "${PREV_DIR}"
    mv "${OUTPUT_DIR}" "${PREV_DIR}"
fi
mv "${STAGING_DIR}" "${OUTPUT_DIR}"
STAGING_DIR=""
if [ -n "${PREV_DIR}" ]; then
    rm -rf "${PREV_DIR}"
    PREV_DIR=""
fi

if [ "${KEEP_CAPTURES}" = "0" ]; then
    rm -rf "${CAPTURES_DIR}"
fi

echo
echo "[run-fixture] done."
echo "  snapshot: ${OUTPUT_DIR}"
echo "  tables:   ${SNAPSHOT_TABLES} (floor: ${MIN_TABLES})"
