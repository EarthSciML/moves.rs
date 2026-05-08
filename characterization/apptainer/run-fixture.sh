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
#   -f, --fakeroot        Use Apptainer --fakeroot mode (matches the
#                         `service mariadb start` path baked into the SIF).
#                         Without this, mariadbd is started as the calling
#                         user via /opt/moves-bin/start-mariadb-bg.sh.
#   --sif PATH            moves-fixture.sif path (default: ./moves-fixture.sif).
#   --workdir DIR         Host scratch root (default: /scratch/$USER/moves-fixture/<fixture>).
#   --output-dir DIR      Snapshot output directory
#                         (default: ../snapshots/<fixture-name>/ relative to
#                         this script).
#   --keep-captures       Don't delete the staged captures directory after
#                         the snapshot is built. Useful for forensics.
#   --skip-run            Skip the MOVES execution. Captures and snapshot
#                         only — assumes a previous run's scratch is intact.
#
# Environment:
#   MOVES_FIXTURE_CAPTURE_BIN  Path to the moves-fixture-capture binary
#                              (default: cargo's target/release/ then debug/).
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

# Phase 0 Task 8 (mo-d7or): JVM class-load logs land here so the
# moves-fixture-capture trace builder can pick them up alongside the
# worker.sql files. Materialized inside MOVESTemporary so the existing
# bind-mount layout carries it into the container at
# /opt/moves/MOVESTemporary/instrumentation/, and so the existing
# captures-step copy of MOVESTemporary into captures/moves-temporary/
# carries it back out without further wiring.
INSTRUMENTATION_DIR="${MOVES_TEMP}/instrumentation"

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

    SIF="${SIF}" \
    WORKDIR="${WORKDIR}" \
    MARIADB_DATA="${MARIADB_DATA}" \
    MARIADB_SOCK_DIR="${MARIADB_SOCK_DIR}" \
    MOVES_TEMP="${MOVES_TEMP}" \
    WORKER_DIR="${WORKER_DIR}" \
    JAVA_TOOL_OPTIONS="${FIXTURE_JAVA_TOOL_OPTIONS}" \
        "${HERE}/run-moves.sh" "${FAKEROOT_ARGS[@]}" --runspec "${RUNSPEC}"
else
    echo "[run-fixture] step 1/3 — skipped (--skip-run)"
fi

# ----- Step 2: dump MariaDB databases to TSV -----
echo "[run-fixture] step 2/3 — dumping MariaDB databases to TSV"

rm -rf "${CAPTURES_DIR}"
mkdir -p "${CAPTURES_DIR}/databases"

FAKEROOT_FLAG=()
if [ "${USE_FAKEROOT}" = "1" ]; then
    FAKEROOT_FLAG=( --fakeroot )
    START_MARIADB="service mariadb start"
else
    START_MARIADB="/opt/moves-bin/start-mariadb-bg.sh"
fi

# Bind-mount layout for the dump pass. The dump script lives next to this
# script on the host and is bind-mounted read-only into the container so
# we don't need to rebuild the SIF when it changes.
BINDS=(
    --bind "${MARIADB_DATA}:/var/lib/mysql"
    --bind "${MARIADB_SOCK_DIR}:/var/run/mysqld"
    --bind "${CAPTURES_DIR}:/captures"
    --bind "${HERE}/dump-databases.sh:/opt/fixture-tools/dump-databases.sh:ro"
)

apptainer exec \
    "${FAKEROOT_FLAG[@]}" \
    "${BINDS[@]}" \
    --env "START_MARIADB=${START_MARIADB}" \
    --env "CAPTURES_DIR=/captures" \
    "${SIF}" \
    bash /opt/fixture-tools/dump-databases.sh

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

mkdir -p "${OUTPUT_DIR}"
"${BIN}" \
    --captures-dir "${CAPTURES_DIR}" \
    --runspec "${RUNSPEC}" \
    --sif-lockfile "${ROOT}/fixture-image.lock" \
    --output-dir "${OUTPUT_DIR}" \
    --fixture-name "${FIXTURE_NAME}"

if [ "${KEEP_CAPTURES}" = "0" ]; then
    rm -rf "${CAPTURES_DIR}"
fi

echo
echo "[run-fixture] done."
echo "  snapshot: ${OUTPUT_DIR}"
