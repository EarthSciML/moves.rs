#!/bin/bash
# generate-corpus.sh — build the gfortran NONROAD reference corpus (Phase 5 T4).
#
# For each of the ten nr-*.xml fixtures listed in FIXTURES, invokes
# characterization/apptainer/run-fixture.sh with NRDBG_FILE set so the
# instrumented NONROAD writes its intermediate-state TSV. Copies each
# result into baselines/<fixture>.tsv and records per-fixture SHA256,
# line count, and elapsed time in baselines/corpus.sha.
#
# Idempotent: re-running skips fixtures whose TSV already exists and
# matches the recorded SHA. Set FORCE=1 to regenerate unconditionally.
#
# Usage:
#   ./generate-corpus.sh [--dry-run] [-h|--help]
#
# Options:
#   --dry-run   Echo the ten apptainer-exec command lines without executing.
#   -h, --help  Print this help and exit.
#
# Environment:
#   FORCE=1     Regenerate all fixtures even when SHAs match (default: 0).
#   SIF         Path to moves-fixture.sif passed to run-fixture.sh
#               (default: <this-script-dir>/../apptainer/moves-fixture.sif).
#   SCRATCH     Host scratch root for MOVES working directories
#               (default: /scratch/$USER/nonroad-corpus).

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APPTAINER_DIR="${HERE}/../apptainer"
FIXTURES_DIR="${HERE}/../fixtures"
BASELINES_DIR="${HERE}/baselines"
FIXTURES_FILE="${HERE}/FIXTURES"

SIF="${SIF:-${APPTAINER_DIR}/moves-fixture.sif}"
SCRATCH="${SCRATCH:-/scratch/${USER}/nonroad-corpus}"
FORCE="${FORCE:-0}"
DRY_RUN=0

usage() {
    sed -n '2,/^set -euo pipefail/p' "$0" | sed 's/^#\s\?//' | head -n -1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run)  DRY_RUN=1; shift ;;
        -h|--help)  usage; exit 0 ;;
        *)          echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

if [ ! -f "${FIXTURES_FILE}" ]; then
    echo "FATAL: FIXTURES file not found: ${FIXTURES_FILE}" >&2
    exit 2
fi

mapfile -t FIXTURE_NAMES < <(grep -v '^[[:space:]]*\(#\|$\)' "${FIXTURES_FILE}")

CORPUS_SHA="${BASELINES_DIR}/corpus.sha"

_sha256() { sha256sum "$1" | awk '{print $1}'; }

run_one() {
    local fixture="$1"
    local workdir="${SCRATCH}/${fixture}"
    local moves_temp="${workdir}/MOVESTemporary"
    # Container-side path — bound to ${moves_temp} on the host.
    local nrdbg_container="/opt/moves/MOVESTemporary/${fixture}.tsv"
    local nrdbg_host="${moves_temp}/${fixture}.tsv"
    local baseline="${BASELINES_DIR}/${fixture}.tsv"
    local runspec="${FIXTURES_DIR}/${fixture}.xml"

    if [ "${DRY_RUN}" = "1" ]; then
        printf 'apptainer exec --env NRDBG_FILE=%s --bind %s:/opt/moves/MOVESTemporary %s bash -c '"'"'cd /opt/moves && ant crun -Drunspec=%s'"'"'\n' \
            "${nrdbg_container}" "${moves_temp}" "${SIF}" "${runspec}"
        return
    fi

    # Idempotency: skip when baseline exists and SHA matches corpus.sha.
    if [ "${FORCE}" != "1" ] && [ -f "${baseline}" ] && [ -f "${CORPUS_SHA}" ]; then
        local recorded
        recorded=$(awk -v f="${fixture}" 'BEGIN{FS="\t"} $1 == f {print $2}' "${CORPUS_SHA}" || true)
        if [ -n "${recorded}" ] && [ "${recorded}" = "$(_sha256 "${baseline}")" ]; then
            echo "[corpus] ${fixture}: up-to-date (SHA matches) — skipping"
            return
        fi
    fi

    echo "[corpus] ${fixture}: starting MOVES run..."
    mkdir -p "${moves_temp}"

    local t0; t0=$(date +%s)

    WORKDIR="${workdir}" \
    NRDBG_FILE="${nrdbg_container}" \
        "${APPTAINER_DIR}/run-fixture.sh" --runspec "${runspec}"

    local t1; t1=$(date +%s)
    local elapsed=$(( t1 - t0 ))

    if [ ! -f "${nrdbg_host}" ]; then
        echo "FATAL: NRDBG_FILE not produced at ${nrdbg_host}" >&2
        exit 1
    fi

    mkdir -p "${BASELINES_DIR}"
    cp "${nrdbg_host}" "${baseline}"

    local sha; sha=$(_sha256 "${baseline}")
    local lines; lines=$(wc -l < "${baseline}")

    if [ -f "${CORPUS_SHA}" ]; then
        awk -v f="${fixture}" 'BEGIN{FS="\t"} $1 != f' "${CORPUS_SHA}" > "${CORPUS_SHA}.tmp" || true
        mv "${CORPUS_SHA}.tmp" "${CORPUS_SHA}"
    fi
    printf '%s\t%s\t%d\t%d\n' "${fixture}" "${sha}" "${lines}" "${elapsed}" >> "${CORPUS_SHA}"

    echo "[corpus] ${fixture}: done — ${lines} lines, SHA ${sha:0:12}…, elapsed ${elapsed}s"
}

for fixture in "${FIXTURE_NAMES[@]}"; do
    run_one "${fixture}"
done

if [ "${DRY_RUN}" = "0" ]; then
    echo
    echo "[corpus] all ${#FIXTURE_NAMES[@]} fixtures processed. Baselines: ${BASELINES_DIR}"
fi
