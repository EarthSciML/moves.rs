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
# ----- Failure semantics (issue #60) ---------------------------------------
#
# This script writes the reference corpus the NONROAD fidelity gate diffs
# against, so a failed run that is recorded as a baseline is worse than no
# baseline at all: the corpus passes its own SHA integrity check, because the
# hash genuinely matches the bytes, and nothing downstream can tell.
#
# Three things used to make that reachable. All three are closed here.
#
#   1. run-moves.sh was called bare, so its exit status was never read. It is
#      now teed to a log with the status taken from PIPESTATUS[0].
#   2. There was no log scan at all. MOVES's ant target runs the master JVM
#      without failonerror, so a MOVES process that dies still yields BUILD
#      SUCCESSFUL and exit 0 — the status carries no signal and the log is the
#      only witness. The scan is now the shared one from
#      ../apptainer/lib/moves-log-scan.sh (issues #56, #64).
#   3. The only artifact guard was `[ -f "${nrdbg_host}" ]`, and nothing ever
#      removed that path first — so with FORCE=1 over a populated workdir, a
#      MOVES failure left the PREVIOUS run's TSV in place, the check passed,
#      and the old bytes were copied into baselines/ and re-hashed as fresh.
#      Stale outputs are now removed before the run, so the check means "this
#      run produced it"; the TSV must also be non-empty, carry a recognisable
#      dbgemit phase in field 1, and clear a row floor.
#
# WHAT THE LOG SCAN CANNOT SEE HERE. It is the MOVES-side scan, unchanged,
# and on the NONROAD path it is strictly weaker than it is for run-fixture.sh.
# From MOVES's own source inside moves-fixture.sif:
#
#   * NONROAD.exe's stdout does not reach the ant log at all. worker/framework
#     /RemoteEmissionsCalculator.java:1151-1157 redirects it to a file,
#     NonroadProcessOutput.txt, in the worker folder. Confirmed against a real
#     capture: a full nonroad fixture run log carries only "Time spent on
#     running nonroad.exe" and "Nonroad files are in: ...", no NONROAD output.
#   * That same call site discards ApplicationRunner.runApplication's exit
#     status entirely (:1154-1163 catch and ignore). A NONROAD.exe that dies
#     is invisible to MOVES.
#   * MOVES does re-log NONROAD's own ERROR lines through Logger at
#     :1221 — but the worker runs inside the simulation window, where
#     common/Logger.java:78-80 rewrites both WARNING and ERROR to RUN_ERROR
#     (set at master/framework/MOVESEngine.java:457, cleared at :1245). So
#     they arrive as `RUN_ERROR:` and are textually indistinguishable from
#     benign in-simulation warnings — the issue #64 residual gap, which on
#     this path swallows the entire NONROAD failure class.
#
# Hence the one guard here that run-fixture.sh does not have: nrerrors.txt.
# RemoteEmissionsCalculator.java:1213-1226 writes that file into the worker
# folder if and only if NONROAD's own output contained an `ERROR: ` line. It
# is the single unambiguous NONROAD-side failure signal that survives the
# RUN_ERROR promotion, and it is bind-mounted back to the host under
# MOVESTemporary/. It is also removed before the run, for the same reason the
# TSV is.
#
# The artifact checks are therefore not a belt behind the log scan on this
# path — for NONROAD's own failures they are the primary detector.
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
#   BASELINES_DIR Where to write baselines (default: <this-dir>/baselines).
#   FIXTURES_FILE Fixture-name list to process (default: <this-dir>/FIXTURES).
#   MIN_ROWS      Minimum acceptable row count for a captured TSV (default:
#                 100). The smallest fixture in the shipped corpus is
#                 nr-logging-county at 1266 rows, so 100 is a ~12x margin
#                 against a truncated capture while staying well clear of a
#                 false refusal. Raise it for a known-large corpus.

# -E (errtrace) so the ERR trap below is inherited by run_one(): without it,
# an unexpected failure inside the per-fixture function aborts the script
# SILENTLY, which is the very defect this file is being fixed for. Verified
# by tests/generate-corpus-guards.sh, whose happy-path cases would go red on
# any spurious trip.
set -Eeuo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APPTAINER_DIR="${HERE}/../apptainer"
FIXTURES_DIR="${HERE}/../fixtures"
# Overridable so the guard suite (tests/generate-corpus-guards.sh) can drive
# the real script over one fixture into a throwaway directory. Nothing in
# normal use sets either.
BASELINES_DIR="${BASELINES_DIR:-${HERE}/baselines}"
FIXTURES_FILE="${FIXTURES_FILE:-${HERE}/FIXTURES}"

SIF="${SIF:-${APPTAINER_DIR}/moves-fixture.sif}"
SCRATCH="${SCRATCH:-/scratch/${USER}/nonroad-corpus}"
FORCE="${FORCE:-0}"
MIN_ROWS="${MIN_ROWS:-100}"
DRY_RUN=0
NONROAD_BUILD_DIR="${SCRATCH}/nonroad-build"
NONROAD_EXE="${NONROAD_EXE:-${NONROAD_BUILD_DIR}/NONROAD.exe}"

usage() {
    sed -n '2,/^set -Eeuo pipefail/p' "$0" | sed 's/^#\s\?//' | head -n -1
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

# ----- Failure handling (issue #60) ----------------------------------------
# Every abort funnels through fail(): it removes any half-written staging
# file, prints an unambiguous banner on stderr, and exits non-zero. Nothing is
# allowed to reach baselines/, corpus.sha or MANIFEST.toml unless a TSV this
# run actually produced is on disk and has passed every check below.
STAGING_FILE=""
CURRENT_FIXTURE=""
CURRENT_LOG=""

cleanup_incomplete() {
    if [ -n "${STAGING_FILE}" ] && [ -e "${STAGING_FILE}" ]; then
        rm -f "${STAGING_FILE}" || true
    fi
    STAGING_FILE=""
    return 0
}

banner() {
    {
        echo
        echo "############################################################"
        echo "[corpus] CAPTURE FAILED — NO BASELINE RECORDED"
        echo "[corpus]   fixture : ${CURRENT_FIXTURE:-<none>}"
        echo "[corpus]   reason  : $*"
        if [ -n "${CURRENT_LOG}" ] && [ -f "${CURRENT_LOG}" ]; then
            echo "[corpus]   run log : ${CURRENT_LOG}"
        fi
        echo "[corpus]"
        echo "[corpus] baselines/, corpus.sha and MANIFEST.toml are unchanged"
        echo "[corpus] for this fixture. A stale TSV recorded as a fresh"
        echo "[corpus] baseline would pass the corpus' own integrity check,"
        echo "[corpus] so this refuses rather than records."
        echo "############################################################"
    } >&2
}

fail() {
    cleanup_incomplete
    banner "$@"
    exit 1
}

# An unanticipated non-zero status is exactly as dangerous as a MOVES
# failure, so route set -e trips through the same banner.
on_unexpected_error() {
    local rc="$1" line="$2"
    cleanup_incomplete
    banner "unexpected non-zero status ${rc} at line ${line}"
    exit "${rc}"
}
trap 'on_unexpected_error "$?" "${LINENO}"' ERR
trap cleanup_incomplete EXIT

# The run-log failure rule, shared with run-fixture.sh so neither can drift.
# See this file's header for what it cannot see on the NONROAD path.
# shellcheck source=../apptainer/lib/moves-log-scan.sh
source "${APPTAINER_DIR}/lib/moves-log-scan.sh"

# NONROAD's own error file, written by the MOVES worker if and only if
# NONROAD.exe's output carried an `ERROR: ` line
# (worker/framework/RemoteEmissionsCalculator.java:1213-1226). Located by
# search rather than by a hard-coded path: the worker folder layout comes
# from manyworkers.txt and a bundle-specific WorkerTemp subdirectory.
_find_nrerrors() {
    find "$1" -type f -name 'nrerrors.txt' -size +0c 2>/dev/null | head -n 1 || true
}

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

    CURRENT_FIXTURE="${fixture}"
    local run_log="${workdir}/moves-run.log"
    CURRENT_LOG="${run_log}"

    # Remove every artifact this run is about to be judged by, BEFORE the run.
    # This is the heart of issue #60: without it, "the TSV exists" means "a
    # file is there", not "this run produced it", and a MOVES failure over a
    # populated workdir hands the previous run's bytes to baselines/.
    rm -f "${nrdbg_host}" "${run_log}"
    local stale_nrerrors
    while IFS= read -r stale_nrerrors; do
        [ -n "${stale_nrerrors}" ] && rm -f "${stale_nrerrors}"
    done < <(find "${moves_temp}" -type f -name 'nrerrors.txt' 2>/dev/null)
    if [ -e "${nrdbg_host}" ]; then
        fail "could not remove the previous run's TSV at ${nrdbg_host}"
    fi

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

    # Tee the run so the log scan has something to read, and take
    # run-moves.sh's own status from PIPESTATUS rather than the pipeline's
    # (tee always succeeds). errexit and the ERR trap come off around the
    # pipeline so the explicit checks below are reached with a specific reason
    # instead of aborting the script on the pipeline's status. Written inline
    # rather than in a helper because bash restores the ERR trap when a
    # function returns, which would silently undo a helper's `trap - ERR`.
    trap - ERR
    set +e
    SIF="${SIF}" \
    NONROAD_EXE="${NONROAD_EXE}" \
    WORKDIR="${workdir}" \
    NRDBG_FILE="${nrdbg_container}" \
        "${APPTAINER_DIR}/run-moves.sh" --runspec "${runspec_container}" \
        2>&1 | tee "${run_log}"
    # Snapshot PIPESTATUS in one shot: reading it is itself a command, which
    # overwrites PIPESTATUS before a second read can see it.
    local pipe_status=( "${PIPESTATUS[@]}" )
    set -e
    trap 'on_unexpected_error "$?" "${LINENO}"' ERR

    local moves_status="${pipe_status[0]:-0}"
    local tee_status="${pipe_status[1]:-0}"

    local t1; t1=$(date +%s)
    local elapsed=$(( t1 - t0 ))

    if [ "${tee_status}" -ne 0 ]; then
        fail "could not write the run log to ${run_log} (tee exit ${tee_status})"
    fi
    if [ "${moves_status}" -ne 0 ]; then
        fail "run-moves.sh exited ${moves_status}"
    fi

    # ant exits 0 even when the forked MOVES JVM dies; the log is the only
    # witness. Primary detector for the MOVES-side failure classes.
    local reason
    if ! reason="$(moves_log_failure_reason "${run_log}")"; then
        fail "${reason}"
    fi

    # NONROAD-side failures never reach that scan (see this file's header).
    # nrerrors.txt is the one signal that survives the RUN_ERROR promotion.
    local nrerrors; nrerrors="$(_find_nrerrors "${moves_temp}")"
    if [ -n "${nrerrors}" ]; then
        local first; first="$(head -n 1 "${nrerrors}" | tr -d '\r' | cut -c1-200)"
        fail "NONROAD reported an error (${nrerrors}): ${first}"
    fi

    # ----- Artifact checks: what this run actually produced -----------------
    # ${nrdbg_host} was removed above, so its presence now means this run
    # wrote it.
    if [ ! -f "${nrdbg_host}" ]; then
        fail "NRDBG_FILE not produced at ${nrdbg_host} — the instrumented NONROAD wrote no capture"
    fi
    if [ ! -s "${nrdbg_host}" ]; then
        fail "NRDBG_FILE at ${nrdbg_host} is empty"
    fi

    # Field 1 of every dbgemit record is one of four phase tokens
    # (nonroad-build/src/dbgemit.f). A first line that is not one of them
    # means the file is not a capture, however many bytes it holds.
    local first_phase
    first_phase="$(head -n 1 "${nrdbg_host}" | cut -f1 | tr -d '[:space:]')"
    case "${first_phase}" in
        GETPOP|AGEDIST|GRWFAC|CLCEMS) ;;
        *) fail "${nrdbg_host} does not look like a dbgemit capture (first field '${first_phase}', expected GETPOP/AGEDIST/GRWFAC/CLCEMS)" ;;
    esac

    local lines; lines=$(wc -l < "${nrdbg_host}")
    if [ "${lines}" -lt "${MIN_ROWS}" ]; then
        fail "${nrdbg_host} holds ${lines} row(s), below the MIN_ROWS floor of ${MIN_ROWS} — a truncated capture"
    fi

    # ----- Publish: stage, then swap ---------------------------------------
    # An interrupted cp into baselines/<fixture>.tsv leaves a half-copied file
    # that looks finished. Copy to a staging path first and mv it into place,
    # which is atomic within a filesystem; the EXIT trap drops the staging
    # file on any abort.
    mkdir -p "${BASELINES_DIR}"
    STAGING_FILE="${BASELINES_DIR}/.${fixture}.tsv.staging.$$"
    cp "${nrdbg_host}" "${STAGING_FILE}"

    local staged_lines; staged_lines=$(wc -l < "${STAGING_FILE}")
    if [ "${staged_lines}" -ne "${lines}" ]; then
        fail "staged copy of ${fixture} has ${staged_lines} rows, source had ${lines} — copy truncated"
    fi

    mv -f "${STAGING_FILE}" "${baseline}"
    STAGING_FILE=""

    local sha; sha=$(_sha256 "${baseline}")

    if [ -f "${CORPUS_SHA}" ]; then
        awk -v f="${fixture}" 'BEGIN{FS="\t"} $1 != f' "${CORPUS_SHA}" > "${CORPUS_SHA}.tmp" || true
        mv "${CORPUS_SHA}.tmp" "${CORPUS_SHA}"
    fi
    printf '%s\t%s\t%d\t%d\n' "${fixture}" "${sha}" "${lines}" "${elapsed}" >> "${CORPUS_SHA}"

    echo "[corpus] ${fixture}: done — ${lines} lines, SHA ${sha:0:12}…, elapsed ${elapsed}s"
    CURRENT_FIXTURE=""
    CURRENT_LOG=""
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
