#!/usr/bin/env bash
# run-comparison.sh — side-by-side canonical-MOVES vs moves.rs audit.
#
# Runs each fixture through both canonical MOVES (reusing or regenerating the
# snapshot under characterization/snapshots/) and moves.rs, then emits a
# Markdown report comparing per-pollutant emissions and wall-clock runtime.
#
# Usage:
#   ./characterization/audit/run-comparison.sh [options]
#
# Options:
#   --fixtures LIST       Comma-separated fixture names.  Each name is matched
#                         against characterization/fixtures/*.xml
#                         case-insensitively (non-alphanumeric stripped).
#                         Default: every name in typical-scenarios.txt.
#   --output-dir DIR      Where to write the report.
#                         Default: characterization/audit-results/<timestamp>.
#   --refresh-canonical   Re-run canonical MOVES even when a snapshot already
#                         exists under characterization/snapshots/.
#   -h, --help            This message.
#
# Prerequisites:
#   * For canonical MOVES runs: characterization/apptainer/moves-fixture.sif
#     must exist (build with build-fixture-sif.sh).
#   * For moves.rs runs: Rust toolchain installed (cargo build is run here).
#
# Exit codes:
#   0 — report written
#   1 — one or more fixtures failed

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
FIXTURES_DIR="${ROOT}/characterization/fixtures"
SNAPSHOTS_DIR="${ROOT}/characterization/snapshots"
APPTAINER_DIR="${ROOT}/characterization/apptainer"
SIF="${APPTAINER_DIR}/moves-fixture.sif"

FIXTURES_ARG=""
OUTPUT_DIR=""
REFRESH_CANONICAL=0
FAILURES=0

usage() {
    sed -n '2,/^set -euo pipefail/p' "$0" | sed 's/^# \?//' | head -n -1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --fixtures)    FIXTURES_ARG="$2"; shift 2 ;;
        --output-dir)  OUTPUT_DIR="$2";   shift 2 ;;
        --refresh-canonical) REFRESH_CANONICAL=1; shift ;;
        -h|--help)     usage; exit 0 ;;
        *) printf 'Unknown argument: %s\n' "$1" >&2; exit 2 ;;
    esac
done

TIMESTAMP="$(date +%Y%m%dT%H%M%S)"
OUTPUT_DIR="${OUTPUT_DIR:-${ROOT}/characterization/audit-results/${TIMESTAMP}}"
mkdir -p "${OUTPUT_DIR}"

# Build both binaries once up front.
printf '[build] cargo build --release -p moves-cli -p moves-snapshot\n' >&2
cargo build --release --manifest-path "${ROOT}/Cargo.toml" \
    -p moves-cli -p moves-snapshot 2>&1 | grep -E '^(error|warning\[|Compiling|Finished)' >&2 || true
MOVES_BIN="${ROOT}/target/release/moves"
COMPARE_BIN="${ROOT}/target/release/compare-canonical"

# Normalize: lowercase, strip non-alphanumeric.
normalize() { printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9'; }

# Return the path of the fixture XML whose stem normalises to the target.
find_fixture_xml() {
    local target
    target=$(normalize "$1")
    for f in "${FIXTURES_DIR}"/*.xml; do
        local stem
        stem="${f##*/}"
        stem="${stem%.xml}"
        if [ "$(normalize "$stem")" = "$target" ]; then
            printf '%s' "$f"
            return 0
        fi
    done
    return 1
}

# Wall-clock measurement: milliseconds since epoch → call with $() wrapping.
now_ms() { date +%s%3N; }
elapsed_s() {
    local ms=$1
    awk "BEGIN { printf \"%.1f\", $ms / 1000 }"
}

# Parse /usr/bin/time -v output file → peak MiB string, or "".
# "Maximum resident set size (kbytes): N" is in kibibytes on Linux.
peak_mb_from_time_file() {
    local tf="$1"
    local kb
    kb=$(awk '/Maximum resident set size/{print $NF}' "${tf}" 2>/dev/null || true)
    if [ -n "${kb}" ] && [ "${kb}" -gt 0 ] 2>/dev/null; then
        awk "BEGIN { printf \"%.1f\", ${kb}/1024 }"
    fi
}

# Build the fixture list.
if [ -n "${FIXTURES_ARG}" ]; then
    IFS=',' read -ra FIXTURE_LIST <<< "${FIXTURES_ARG}"
else
    mapfile -t FIXTURE_LIST < "${HERE}/typical-scenarios.txt"
fi

# Per-fixture JSON paths, collected for summary assembly later.
declare -a JSON_FILES=()

# ── Per-fixture loop ──────────────────────────────────────────────────────────
for FIXTURE in "${FIXTURE_LIST[@]}"; do
    FIXTURE="$(printf '%s' "${FIXTURE}" | tr -d '[:space:]')"
    [ -z "${FIXTURE}" ] && continue

    if ! FIXTURE_XML=$(find_fixture_xml "${FIXTURE}" 2>/dev/null); then
        printf '[SKIP] fixture %s: no matching .xml in %s\n' "${FIXTURE}" "${FIXTURES_DIR}" >&2
        FAILURES=$((FAILURES + 1))
        continue
    fi

    FIXTURE_STEM="${FIXTURE_XML##*/}"
    FIXTURE_STEM="${FIXTURE_STEM%.xml}"
    # Derive fixture name the same way run-fixture.sh does.
    FIXTURE_NAME="$(printf '%s' "${FIXTURE_STEM}" \
        | tr '[:upper:]' '[:lower:]' \
        | sed 's/[^a-z0-9_-]/_/g')"

    SNAPSHOT_DIR="${SNAPSHOTS_DIR}/${FIXTURE_NAME}"
    CANONICAL_WALL_ARG=""
    CANONICAL_PEAK_MB_ARG=""

    # ── Canonical MOVES ───────────────────────────────────────────────────────
    if [ ! -d "${SNAPSHOT_DIR}" ] || [ "${REFRESH_CANONICAL}" -eq 1 ]; then
        if [ ! -f "${SIF}" ]; then
            printf '[ERROR] %s: snapshot absent and SIF not found at %s\n' \
                "${FIXTURE_NAME}" "${SIF}" >&2
            printf '        Build the SIF: characterization/apptainer/build-fixture-sif.sh\n' >&2
            FAILURES=$((FAILURES + 1))
            continue
        fi
        printf '[run-canonical] %s ...\n' "${FIXTURE_NAME}" >&2
        CANONICAL_TIME_FILE="${OUTPUT_DIR}/${FIXTURE_NAME}/canonical-time.txt"
        mkdir -p "${OUTPUT_DIR}/${FIXTURE_NAME}"
        T0=$(now_ms)
        /usr/bin/time -v -o "${CANONICAL_TIME_FILE}" \
            "${APPTAINER_DIR}/run-fixture.sh" --fakeroot --runspec "${FIXTURE_XML}"
        T1=$(now_ms)
        CANONICAL_WALL=$(elapsed_s $((T1 - T0)))
        CANONICAL_PEAK_MB=$(peak_mb_from_time_file "${CANONICAL_TIME_FILE}")
        # Store timing and peak alongside snapshot for future re-runs.
        printf '%s\n' "${CANONICAL_WALL}" > "${SNAPSHOT_DIR}/timing.txt"
        [ -n "${CANONICAL_PEAK_MB}" ] && printf '%s\n' "${CANONICAL_PEAK_MB}" > "${SNAPSHOT_DIR}/peak-mb.txt"
        CANONICAL_WALL_ARG="--canonical-wall ${CANONICAL_WALL}"
        [ -n "${CANONICAL_PEAK_MB}" ] && CANONICAL_PEAK_MB_ARG="--canonical-peak-mb ${CANONICAL_PEAK_MB}"
        printf '[done-canonical] %s  wall=%s s  peak=%s MiB\n' \
            "${FIXTURE_NAME}" "${CANONICAL_WALL}" "${CANONICAL_PEAK_MB:-N/A}" >&2
    else
        # Reuse existing snapshot; read cached timing and peak if available.
        TIMING_FILE="${SNAPSHOT_DIR}/timing.txt"
        if [ -f "${TIMING_FILE}" ]; then
            CANONICAL_WALL=$(tr -d '[:space:]' < "${TIMING_FILE}")
            CANONICAL_WALL_ARG="--canonical-wall ${CANONICAL_WALL}"
        fi
        PEAK_FILE="${SNAPSHOT_DIR}/peak-mb.txt"
        if [ -f "${PEAK_FILE}" ]; then
            CANONICAL_PEAK_MB=$(tr -d '[:space:]' < "${PEAK_FILE}")
            [ -n "${CANONICAL_PEAK_MB}" ] && CANONICAL_PEAK_MB_ARG="--canonical-peak-mb ${CANONICAL_PEAK_MB}"
        fi
        printf '[reuse-canonical] %s  snapshot: %s\n' "${FIXTURE_NAME}" "${SNAPSHOT_DIR}" >&2
    fi

    # ── moves.rs ─────────────────────────────────────────────────────────────
    MOVES_RS_OUT="${OUTPUT_DIR}/${FIXTURE_NAME}/moves-rs-output"
    mkdir -p "${MOVES_RS_OUT}"
    printf '[run-moves.rs] %s ...\n' "${FIXTURE_NAME}" >&2
    MOVES_RS_TIME_FILE="${OUTPUT_DIR}/${FIXTURE_NAME}/moves-rs-time.txt"
    T0=$(now_ms)
    /usr/bin/time -v -o "${MOVES_RS_TIME_FILE}" \
        "${MOVES_BIN}" run \
            --runspec  "${FIXTURE_XML}" \
            --output   "${MOVES_RS_OUT}" \
            --snapshot "${SNAPSHOT_DIR}"
    T1=$(now_ms)
    MOVES_RS_WALL=$(elapsed_s $((T1 - T0)))
    MOVES_RS_PEAK_MB=$(peak_mb_from_time_file "${MOVES_RS_TIME_FILE}")
    MOVES_RS_PEAK_MB_ARG=""
    [ -n "${MOVES_RS_PEAK_MB}" ] && MOVES_RS_PEAK_MB_ARG="--moves-rs-peak-mb ${MOVES_RS_PEAK_MB}"
    printf '[done-moves.rs] %s  wall=%s s  peak=%s MiB\n' \
        "${FIXTURE_NAME}" "${MOVES_RS_WALL}" "${MOVES_RS_PEAK_MB:-N/A}" >&2

    # ── Compare ──────────────────────────────────────────────────────────────
    FIXTURE_JSON="${OUTPUT_DIR}/${FIXTURE_NAME}/report.json"
    # shellcheck disable=SC2086
    "${COMPARE_BIN}" \
        --canonical  "${SNAPSHOT_DIR}" \
        --moves-rs   "${MOVES_RS_OUT}" \
        --fixture    "${FIXTURE_NAME}" \
        ${CANONICAL_WALL_ARG} \
        ${CANONICAL_PEAK_MB_ARG} \
        --moves-rs-wall "${MOVES_RS_WALL}" \
        ${MOVES_RS_PEAK_MB_ARG} \
        --format json \
        > "${FIXTURE_JSON}"
    JSON_FILES+=("${FIXTURE_JSON}")
    printf '[compare] %s  -> %s\n' "${FIXTURE_NAME}" "${FIXTURE_JSON}" >&2
done

# ── Assemble Markdown report ──────────────────────────────────────────────────
REPORT="${OUTPUT_DIR}/audit-report.md"

{
    printf '# moves.rs Audit Report — %s\n\n' "${TIMESTAMP}"
    printf '## Summary\n\n'
    printf '| Fixture | Pollutants compared | Max abs delta | Max pct diff | Canonical wall (s) | moves.rs wall (s) | Speedup | moves.rs peak mem (MiB) |\n'
    printf '|---|---|---|---|---|---|---|---|\n'
    for jf in "${JSON_FILES[@]}"; do
        jq -r \
            '"| \(.fixture) | \(.pollutant_count) | \(.max_abs_delta | . * 1e6 | round | . / 1e6) | \(.max_pct_diff * 100 | . * 10 | round | . / 10)% | \(.canonical_wall_secs // "N/A") | \(.moves_rs_wall_secs // "N/A") | \(.speedup // "N/A") | \(.moves_rs_peak_mb // "N/A") |"' \
            "${jf}"
    done
    printf '\n'
    printf '## Per-fixture details\n\n'
} > "${REPORT}"

# Append per-fixture Markdown sections.
for jf in "${JSON_FILES[@]}"; do
    FIXTURE_DIR="$(dirname "${jf}")"
    FIXTURE_NAME="$(basename "${FIXTURE_DIR}")"
    # Re-run compare-canonical in text mode.
    FIXTURE_XML=""
    if ! FIXTURE_XML=$(find_fixture_xml "${FIXTURE_NAME}" 2>/dev/null); then
        FIXTURE_XML="/dev/null"
    fi
    MOVES_RS_OUT="${FIXTURE_DIR}/moves-rs-output"
    CANONICAL_WALL=$(jq -r '.canonical_wall_secs // empty' "${jf}")
    MOVES_RS_WALL=$(jq -r '.moves_rs_wall_secs // empty' "${jf}")
    CANONICAL_PEAK_MB=$(jq -r '.canonical_peak_mb // empty' "${jf}")
    MOVES_RS_PEAK_MB=$(jq -r '.moves_rs_peak_mb // empty' "${jf}")
    CANONICAL_WALL_ARG=""
    [ -n "${CANONICAL_WALL}" ] && CANONICAL_WALL_ARG="--canonical-wall ${CANONICAL_WALL}"
    MOVES_RS_WALL_ARG=""
    [ -n "${MOVES_RS_WALL}" ] && MOVES_RS_WALL_ARG="--moves-rs-wall ${MOVES_RS_WALL}"
    CANONICAL_PEAK_MB_ARG=""
    [ -n "${CANONICAL_PEAK_MB}" ] && CANONICAL_PEAK_MB_ARG="--canonical-peak-mb ${CANONICAL_PEAK_MB}"
    MOVES_RS_PEAK_MB_ARG=""
    [ -n "${MOVES_RS_PEAK_MB}" ] && MOVES_RS_PEAK_MB_ARG="--moves-rs-peak-mb ${MOVES_RS_PEAK_MB}"

    SNAPSHOT_DIR="${SNAPSHOTS_DIR}/${FIXTURE_NAME}"
    # shellcheck disable=SC2086
    "${COMPARE_BIN}" \
        --canonical  "${SNAPSHOT_DIR}" \
        --moves-rs   "${MOVES_RS_OUT}" \
        --fixture    "${FIXTURE_NAME}" \
        ${CANONICAL_WALL_ARG} \
        ${CANONICAL_PEAK_MB_ARG} \
        ${MOVES_RS_WALL_ARG} \
        ${MOVES_RS_PEAK_MB_ARG} \
        --format text \
        >> "${REPORT}"
done

printf '\n[done] report: %s\n' "${REPORT}" >&2

if [ "${FAILURES}" -gt 0 ]; then
    printf '[warn] %d fixture(s) skipped or failed\n' "${FAILURES}" >&2
    exit 1
fi
