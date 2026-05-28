#!/bin/bash
# generate-corpus.sh — build the gfortran NONROAD reference corpus (Phase 5 T4).
#
# For each of the ten nr-*.xml fixtures listed in FIXTURES, invokes
# characterization/apptainer/run-moves.sh with NRDBG_FILE set so the
# instrumented NONROAD writes its intermediate-state TSV. Copies each
# result into baselines/<fixture>.tsv and records per-fixture SHA256,
# line count, and elapsed time in baselines/corpus.sha.
#
# After all fixtures, writes baselines/MANIFEST.toml with SHA256 hashes and
# provenance (sif_sha256). The fidelity harness reads MANIFEST.toml when
# NONROAD_FIDELITY_REFERENCE points at the baselines directory.
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
#   FORCE=1       Regenerate all fixtures even when SHAs match (default: 0).
#   SIF           Path to moves-fixture.sif (default: <this-script-dir>/../apptainer/moves-fixture.sif).
#   SCRATCH       Host scratch root for MOVES working directories
#                 (default: /scratch/$USER/nonroad-corpus).
#   NONROAD_EXE   Path to the instrumented NONROAD.exe binary to inject into
#                 the SIF at runtime (default: $SCRATCH/nonroad-build/NONROAD.exe).
#                 If not found, it is compiled from the SIF's bundled MOVES
#                 source using nonroad-build/build.sh.

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
NONROAD_BUILD_DIR="${SCRATCH}/nonroad-build"
NONROAD_EXE="${NONROAD_EXE:-${NONROAD_BUILD_DIR}/NONROAD.exe}"

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

# ----- Ensure instrumented NONROAD.exe is available -----
# If not found, extract the MOVES source from the SIF and compile it.
if [ "${DRY_RUN}" = "0" ] && [ ! -f "${NONROAD_EXE}" ]; then
    echo "[corpus] Instrumented NONROAD.exe not found; building from SIF source..."
    MOVES_SRC="${NONROAD_BUILD_DIR}/moves-src"
    mkdir -p "${MOVES_SRC}"
    if [ ! -f "${SIF}" ]; then
        echo "FATAL: SIF ${SIF} not found — cannot extract MOVES source to build NONROAD.exe." >&2
        exit 2
    fi
    echo "[corpus] Extracting NONROAD source from ${SIF}..."
    apptainer exec --bind "${MOVES_SRC}:/mnt-out" "${SIF}" bash -c "cp -rp /opt/moves/NONROAD /mnt-out/"
    echo "[corpus] Compiling instrumented NONROAD.exe..."
    OUTPUT="${NONROAD_EXE}" bash "${HERE}/../nonroad-build/build.sh" "${MOVES_SRC}"
    echo "[corpus] Built: ${NONROAD_EXE}"
fi

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

    # Kill any orphaned MariaDB and MOVES JVM processes from prior runs and
    # wait until port 3306 is actually free before launching the next run.
    pkill -f 'gov.epa.otaq.moves' 2>/dev/null || true
    pkill -9 mariadbd 2>/dev/null || true
    for i in $(seq 1 30); do
        ss -tlnp 2>/dev/null | grep -q ':3306' || break
        sleep 1
    done
    if ss -tlnp 2>/dev/null | grep -q ':3306'; then
        echo "[corpus] WARN: port 3306 still in use after 30s; attempting force kill"
        lsof -ti:3306 2>/dev/null | xargs kill -9 2>/dev/null || true
        sleep 2
    fi

    # Stage the runspec XML into the bind-mounted MOVESTemporary directory
    # so MOVES can read it from its container-side path.
    local runspec_staged="${moves_temp}/${fixture}.xml"
    local runspec_container="/opt/moves/MOVESTemporary/${fixture}.xml"
    cp "${runspec}" "${runspec_staged}"

    local t0; t0=$(date +%s)

    SIF="${SIF}" \
    NONROAD_EXE="${NONROAD_EXE}" \
    WORKDIR="${workdir}" \
    NRDBG_FILE="${nrdbg_container}" \
        "${APPTAINER_DIR}/run-moves.sh" --runspec "${runspec_container}"

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

    # ----- Write MANIFEST.toml -----
    # The fidelity harness reads this file when NONROAD_FIDELITY_REFERENCE is
    # set. It contains the SHA256 of each TSV plus optional provenance fields.
    MANIFEST="${BASELINES_DIR}/MANIFEST.toml"
    echo "[corpus] writing ${MANIFEST}"

    SIF_SHA256="$(sha256sum "${SIF}" | awk '{print $1}')"

    {
        printf '# NONROAD gfortran reference corpus manifest.\n'
        printf '# Generated by generate-corpus.sh from moves-fixture.sif.\n'
        printf '# Activate the fidelity gate:\n'
        printf '#   NONROAD_FIDELITY_REFERENCE=characterization/nonroad-fidelity/baselines \\\n'
        printf '#       cargo test -p moves-nonroad --test nonroad_fidelity\n'
        printf '\n'
        printf 'sif_sha256 = "%s"\n' "${SIF_SHA256}"
        printf '\n'

        while IFS=$'\t' read -r name sha rows elapsed; do
            bytes="$(stat -c '%s' "${BASELINES_DIR}/${name}.tsv")"
            printf '[[fixtures]]\n'
            printf 'name         = "%s"\n' "${name}"
            printf 'path         = "%s.tsv"\n' "${name}"
            printf 'sha256       = "%s"\n' "${sha}"
            printf 'bytes        = %d\n' "${bytes}"
            printf 'rows         = %d\n' "${rows}"
            printf 'wall_seconds = %d\n' "${elapsed}"
            printf '\n'
        done < "${CORPUS_SHA}"
    } > "${MANIFEST}"

    echo "[corpus] MANIFEST.toml written (sif_sha256 ${SIF_SHA256:0:12}…)"
fi
