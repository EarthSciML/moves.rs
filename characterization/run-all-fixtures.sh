#!/usr/bin/env bash
# run-all-fixtures.sh — drive every Phase 0 fixture through run-fixture.sh.
#
# This is the polecat → HPC-compute-node handoff for Phase 0 Task 5 + 6
# (bead mo-n2yg). The polecat ships fixture XMLs + this runner; the
# operations team / SLURM job runs it on a compute node where Apptainer
# fakeroot + the built moves-fixture.sif are available.
#
# Usage:
#   ./run-all-fixtures.sh [-f|--fakeroot] [--include PATTERN] [--exclude PATTERN]
#                         [--keep-going] [--sif PATH] [--workdir-root DIR]
#                         [--output-root DIR] [--list]
#
# Options:
#   -f, --fakeroot        Forwarded to run-fixture.sh (Apptainer --fakeroot mode).
#   --sif PATH            moves-fixture.sif path (default: ../apptainer/moves-fixture.sif).
#   --include PATTERN     Run only fixtures whose name matches the shell glob
#                         pattern. Repeatable. Default: all.
#   --exclude PATTERN     Skip fixtures whose name matches the pattern.
#                         Repeatable. Defaults: see SKIP_BY_DEFAULT below.
#   --workdir-root DIR    Per-fixture scratch parent (default: /scratch/$USER/moves-fixture).
#   --output-root DIR     Per-fixture snapshot parent (default: ../snapshots).
#   --keep-going          Continue after failed fixtures (default: stop on first).
#   --list                Print the resolved fixture list and exit.
#   -h, --help            This message.
#
# Exit code:
#   0 — every selected fixture produced a snapshot
#   1 — at least one fixture failed (with --keep-going) or first failure (without)
#
# Determinism note:
#   The host-side scripts (this file, run-fixture.sh, dump-databases.sh,
#   moves-fixture-capture) are deterministic given the same SIF SHA256 and
#   the same fixture XML bytes. The full suite's combined snapshot tree is
#   therefore content-addressed by the SIF + the fixture catalogue.
#
# Compute budget:
#   Most fixtures are < 5 minutes. NONROAD fixtures and multi-county /
#   multi-month expansion fixtures may take several minutes each. Reserve
#   roughly N × 5–10 minutes for a full suite + ~1–2 hours of SIF build
#   on first invocation if the SIF isn't already cached.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURES_DIR="${HERE}/fixtures"
APPTAINER_DIR="${HERE}/apptainer"
DEFAULT_SIF="${APPTAINER_DIR}/moves-fixture.sif"
RUN_FIXTURE="${APPTAINER_DIR}/run-fixture.sh"

USE_FAKEROOT=0
SIF=""
KEEP_GOING=0
LIST_ONLY=0
WORKDIR_ROOT=""
OUTPUT_ROOT="${HERE}/snapshots"
declare -a INCLUDES=()
declare -a EXCLUDES=()

# Fixtures that require additional supporting input databases at run time
# (county data manager, project links/zones, rates-mode setup). These are
# excluded by default so the standard "run all" path works without
# operator-specific setup. Pass `--include PATTERN` to opt them back in.
declare -a SKIP_BY_DEFAULT=(
    "scale-county"
    "scale-project"
    "scale-rates"
)

usage() { sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'; }

while [ $# -gt 0 ]; do
    case "$1" in
        -f|--fakeroot)    USE_FAKEROOT=1 ;;
        --sif)            SIF="$2"; shift ;;
        --include)        INCLUDES+=("$2"); shift ;;
        --exclude)        EXCLUDES+=("$2"); shift ;;
        --keep-going)     KEEP_GOING=1 ;;
        --workdir-root)   WORKDIR_ROOT="$2"; shift ;;
        --output-root)    OUTPUT_ROOT="$2"; shift ;;
        --list)           LIST_ONLY=1 ;;
        -h|--help)        usage; exit 0 ;;
        *)
            echo "FATAL: unknown argument $1" >&2
            usage >&2
            exit 2
            ;;
    esac
    shift
done

[ -n "${SIF}" ] || SIF="${DEFAULT_SIF}"

[ -d "${FIXTURES_DIR}" ] || {
    echo "FATAL: fixtures directory ${FIXTURES_DIR} not found." >&2
    exit 2
}
[ -x "${RUN_FIXTURE}" ] || {
    echo "FATAL: ${RUN_FIXTURE} not found or not executable." >&2
    exit 2
}

# ----- Resolve fixture list ------------------------------------------------
matches_any() {
    local needle="$1"; shift
    local pat
    for pat in "$@"; do
        # shell-style glob via case
        # shellcheck disable=SC2254
        case "$needle" in $pat) return 0 ;; esac
    done
    return 1
}

declare -a FIXTURES=()
while IFS= read -r -d '' xml; do
    name="$(basename "${xml%.xml}")"

    if [ "${#INCLUDES[@]}" -gt 0 ]; then
        if ! matches_any "$name" "${INCLUDES[@]}"; then
            continue
        fi
    fi

    if [ "${#EXCLUDES[@]}" -gt 0 ]; then
        if matches_any "$name" "${EXCLUDES[@]}"; then
            continue
        fi
    elif [ "${#INCLUDES[@]}" -eq 0 ]; then
        # No explicit filter — apply default skips.
        if matches_any "$name" "${SKIP_BY_DEFAULT[@]}"; then
            echo "[run-all] skipping ${name} (needs additional input DB; pass --include ${name})"
            continue
        fi
    fi

    FIXTURES+=("$xml")
done < <(find "${FIXTURES_DIR}" -maxdepth 1 -type f -name '*.xml' -print0 | sort -z)

if [ "${#FIXTURES[@]}" -eq 0 ]; then
    echo "[run-all] no fixtures selected — nothing to do." >&2
    exit 1
fi

if [ "${LIST_ONLY}" = "1" ]; then
    for f in "${FIXTURES[@]}"; do
        printf '%s\t%s\n' "$(basename "${f%.xml}")" "${f}"
    done
    exit 0
fi

# ----- Driver loop ---------------------------------------------------------
TOTAL="${#FIXTURES[@]}"
FAILED=()
SUCCEEDED=0
INDEX=0

for xml in "${FIXTURES[@]}"; do
    INDEX=$((INDEX + 1))
    name="$(basename "${xml%.xml}")"
    echo
    echo "[run-all] ============================================================"
    echo "[run-all] fixture ${INDEX}/${TOTAL}: ${name}"
    echo "[run-all] ============================================================"

    declare -a ARGS=( --runspec "${xml}" --sif "${SIF}" )
    [ "${USE_FAKEROOT}" = "1" ] && ARGS=( -f "${ARGS[@]}" )
    [ -n "${WORKDIR_ROOT}" ] && ARGS+=( --workdir "${WORKDIR_ROOT}/${name}" )
    [ -n "${OUTPUT_ROOT}" ] && ARGS+=( --output-dir "${OUTPUT_ROOT}/${name}" )

    if "${RUN_FIXTURE}" "${ARGS[@]}"; then
        SUCCEEDED=$((SUCCEEDED + 1))
        echo "[run-all] ${name}: OK"
    else
        rc=$?
        FAILED+=("${name}")
        echo "[run-all] ${name}: FAILED (run-fixture.sh exit ${rc})"
        if [ "${KEEP_GOING}" != "1" ]; then
            echo "[run-all] aborting; pass --keep-going to continue after failures." >&2
            break
        fi
    fi
done

echo
echo "[run-all] ============================================================"
echo "[run-all] summary: ${SUCCEEDED}/${TOTAL} succeeded, ${#FAILED[@]} failed"
if [ "${#FAILED[@]}" -gt 0 ]; then
    echo "[run-all] failed fixtures:"
    for f in "${FAILED[@]}"; do
        echo "[run-all]   - ${f}"
    done
    exit 1
fi
exit 0
