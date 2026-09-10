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
#   --check-scenarios     Resolve every name in the fixture list against
#                         characterization/fixtures/ and exit without running
#                         anything.  0 = every name resolves, 2 = at least one
#                         does not.  Costs nothing: no cargo build, no MOVES.
#   -h, --help            This message.
#
# Prerequisites:
#   * For canonical MOVES runs: characterization/apptainer/moves-fixture.sif
#     must exist (build with build-fixture-sif.sh).
#   * For moves.rs runs: Rust toolchain installed (cargo build is run here).
#
# Exit codes:
#   0 — report written (or, with --check-scenarios, every name resolved)
#   1 — one or more fixtures failed the comparison
#   2 — configuration error: a bad argument, or a name in the fixture list
#       that does not resolve to a fixture XML.  Kept distinct from 1 on
#       purpose — see the pre-flight block below (issue #63).

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
CHECK_ONLY=0
FAILURES=0

usage() {
    sed -n '2,/^set -euo pipefail/p' "$0" | sed 's/^# \?//' | head -n -1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --fixtures)    FIXTURES_ARG="$2"; shift 2 ;;
        --output-dir)  OUTPUT_DIR="$2";   shift 2 ;;
        --refresh-canonical) REFRESH_CANONICAL=1; shift ;;
        --check-scenarios)   CHECK_ONLY=1; shift ;;
        -h|--help)     usage; exit 0 ;;
        *) printf 'Unknown argument: %s\n' "$1" >&2; exit 2 ;;
    esac
done

TIMESTAMP="$(date +%Y%m%dT%H%M%S)"
OUTPUT_DIR="${OUTPUT_DIR:-${ROOT}/characterization/audit-results/${TIMESTAMP}}"

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

# Build the fixture list. The on-disk list carries `#` comments and blank
# lines; strip them here so the pre-flight below sees only real names.
SCENARIO_FILE="${HERE}/typical-scenarios.txt"
if [ -n "${FIXTURES_ARG}" ]; then
    FIXTURE_SOURCE="--fixtures"
    IFS=',' read -ra FIXTURE_LIST <<< "${FIXTURES_ARG}"
else
    FIXTURE_SOURCE="${SCENARIO_FILE}"
    if [ ! -f "${SCENARIO_FILE}" ]; then
        printf 'FATAL: scenario list not found: %s\n' "${SCENARIO_FILE}" >&2
        exit 2
    fi
    mapfile -t FIXTURE_LIST < <(grep -v '^[[:space:]]*\(#\|$\)' "${SCENARIO_FILE}")
fi

# ── Pre-flight: every named scenario must resolve to a fixture XML ───────────
#
# issue #63. A scenario list that quietly skips names it cannot resolve is a
# gate that reports nothing while looking like it ran. `mixed-onroad-nonroad`
# stayed in typical-scenarios.txt after 2122513e deleted its XML; this script
# then printed `[SKIP] ...`, ran the eight surviving fixtures, assembled the
# report, and only at the very END exited 1 on `[warn] 1 fixture(s) skipped or
# failed`. In CI that killed the comparison STEP, so the next step — the one
# that actually runs regression_gate.sh — never executed. The numeric gate the
# audit-regression-gate job exists to enforce was dead, not merely red, and it
# read to everyone who saw it as the PR's own numerical regression.
#
# The cut this restores: a name that does not resolve is a CONFIGURATION
# error, not a comparison result.
#   * It is fatal, never a skip.
#   * It is reported for the WHOLE list at once, so one run names every dead
#     entry instead of surfacing them one retirement at a time.
#   * It fires here — before the cargo build and before any MOVES run — so it
#     costs seconds, not the whole job.
#   * It exits 2, keeping "the list is wrong" distinguishable from exit 1,
#     "a fixture regressed". Those two demand completely different responses.
MISSING_FIXTURES=()
RESOLVED_COUNT=0
for FIXTURE in "${FIXTURE_LIST[@]}"; do
    FIXTURE="$(printf '%s' "${FIXTURE}" | tr -d '[:space:]')"
    [ -z "${FIXTURE}" ] && continue
    if find_fixture_xml "${FIXTURE}" >/dev/null 2>&1; then
        RESOLVED_COUNT=$((RESOLVED_COUNT + 1))
    else
        MISSING_FIXTURES+=("${FIXTURE}")
    fi
done

if [ "${#MISSING_FIXTURES[@]}" -gt 0 ]; then
    {
        printf '\n'
        printf '############################################################\n'
        printf '[run-comparison] SCENARIO LIST IS STALE — NOTHING WAS RUN\n'
        printf '[run-comparison]   list        : %s\n' "${FIXTURE_SOURCE}"
        printf '[run-comparison]   fixtures dir: %s\n' "${FIXTURES_DIR}"
        printf '[run-comparison]   %d name(s) do not resolve to a fixture XML:\n' \
            "${#MISSING_FIXTURES[@]}"
        for FIXTURE in "${MISSING_FIXTURES[@]}"; do
            printf '[run-comparison]     - %s\n' "${FIXTURE}"
        done
        printf '[run-comparison]\n'
        printf '[run-comparison] Fix the list, or restore the fixture. Do NOT\n'
        printf '[run-comparison] let it skip: a skipped scenario is a gate that\n'
        printf '[run-comparison] silently stops checking what it was named for.\n'
        printf '############################################################\n'
    } >&2
    exit 2
fi

if [ "${RESOLVED_COUNT}" -eq 0 ]; then
    printf 'FATAL: fixture list %s resolved to zero scenarios — nothing to compare.\n' \
        "${FIXTURE_SOURCE}" >&2
    exit 2
fi

printf '[preflight] %d/%d scenario(s) from %s resolve to a fixture XML\n' \
    "${RESOLVED_COUNT}" "${RESOLVED_COUNT}" "${FIXTURE_SOURCE}" >&2

if [ "${CHECK_ONLY}" -eq 1 ]; then
    exit 0
fi

mkdir -p "${OUTPUT_DIR}"

# Build both binaries once up front.
printf '[build] cargo build --release -p moves-cli -p moves-snapshot\n' >&2
cargo build --release --manifest-path "${ROOT}/Cargo.toml" \
    -p moves-cli -p moves-snapshot 2>&1 | grep -E '^(error|warning\[|Compiling|Finished)' >&2 || true
MOVES_BIN="${ROOT}/target/release/moves"
COMPARE_BIN="${ROOT}/target/release/compare-canonical"

# Per-fixture JSON paths, collected for summary assembly later.
declare -a JSON_FILES=()

# ── Per-fixture loop ──────────────────────────────────────────────────────────
for FIXTURE in "${FIXTURE_LIST[@]}"; do
    FIXTURE="$(printf '%s' "${FIXTURE}" | tr -d '[:space:]')"
    [ -z "${FIXTURE}" ] && continue

    # Unreachable via the pre-flight above; kept as a belt in case a fixture
    # XML is removed mid-run. Never a skip — see the pre-flight rationale.
    if ! FIXTURE_XML=$(find_fixture_xml "${FIXTURE}" 2>/dev/null); then
        printf '[FATAL] fixture %s: no matching .xml in %s (it resolved at pre-flight — removed mid-run?)\n' \
            "${FIXTURE}" "${FIXTURES_DIR}" >&2
        exit 2
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
    printf '| Fixture | Canonical rows | moves.rs rows | Row ratio | Pollutants compared | Max abs delta | Max pct diff | Canonical wall (s) | moves.rs wall (s) | Speedup | moves.rs peak mem (MiB) |\n'
    printf '|---|---|---|---|---|---|---|---|---|---|---|\n'
    for jf in "${JSON_FILES[@]}"; do
        jq -r \
            '"| \(.fixture) | \(.canonical_row_count) | \(.moves_rs_row_count) | \(.row_count_ratio | . * 100 | round | . / 100) | \(.pollutant_count) | \(.max_abs_delta | . * 1e6 | round | . / 1e6) | \(.max_pct_diff * 100 | . * 10 | round | . / 10)% | \(.canonical_wall_secs // "N/A") | \(.moves_rs_wall_secs // "N/A") | \(.speedup // "N/A") | \(.moves_rs_peak_mb // "N/A") |"' \
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
