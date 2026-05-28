#!/usr/bin/env bash
# regression_gate.sh — fail CI when any fixture's max_pct_diff exceeds threshold.
#
# Reads report.json files written by run-comparison.sh and exits non-zero if
# any fixture's max_pct_diff (absolute value) exceeds AUDIT_MAX_PCT_DIFF.
#
# Usage:
#   ./characterization/audit/regression_gate.sh --output-dir DIR
#
# Options:
#   --output-dir DIR    Directory written by run-comparison.sh; scanned for
#                       <fixture>/report.json files.  Required.
#   -h, --help          This message.
#
# Environment:
#   AUDIT_MAX_PCT_DIFF  Maximum allowed |max_pct_diff| as a fraction
#                       (default: 0.10 = 10%).
#
# Exit codes:
#   0  — all fixtures within threshold
#   1  — one or more fixtures exceed threshold
#   2  — usage error (missing required arg, no reports found)

set -euo pipefail

THRESHOLD="${AUDIT_MAX_PCT_DIFF:-0.10}"
OUTPUT_DIR=""

usage() {
    sed -n '2,/^set -euo pipefail/p' "$0" | sed 's/^# \?//' | head -n -1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
        -h|--help)    usage; exit 0 ;;
        *)            printf 'Unknown argument: %s\n' "$1" >&2; exit 2 ;;
    esac
done

if [ -z "${OUTPUT_DIR}" ]; then
    printf 'error: --output-dir is required\n' >&2
    usage >&2
    exit 2
fi

FAILURES=0
CHECKED=0

while IFS= read -r -d '' report_json; do
    fixture=$(jq -r '.fixture' "${report_json}")
    max_pct_diff=$(jq -r '.max_pct_diff' "${report_json}")
    CHECKED=$((CHECKED + 1))

    exceeded=$(awk -v diff="${max_pct_diff}" -v threshold="${THRESHOLD}" \
        'BEGIN {
            d = diff < 0 ? -diff : diff
            print (d > threshold) ? "yes" : "no"
        }')

    pct=$(awk -v diff="${max_pct_diff}" 'BEGIN { printf "%.2f%%", diff * 100 }')
    threshold_pct=$(awk -v t="${THRESHOLD}" 'BEGIN { printf "%.1f%%", t * 100 }')

    if [ "${exceeded}" = "yes" ]; then
        printf '[FAIL] %-40s max_pct_diff=%s  (threshold ±%s)\n' \
            "${fixture}" "${pct}" "${threshold_pct}"
        FAILURES=$((FAILURES + 1))
    else
        printf '[OK]   %-40s max_pct_diff=%s\n' "${fixture}" "${pct}"
    fi
done < <(find "${OUTPUT_DIR}" -name 'report.json' -print0 | sort -z)

if [ "${CHECKED}" -eq 0 ]; then
    printf 'error: no report.json files found under %s\n' "${OUTPUT_DIR}" >&2
    exit 2
fi

threshold_pct=$(awk -v t="${THRESHOLD}" 'BEGIN { printf "%.1f%%", t * 100 }')
printf '\n%d/%d fixture(s) within ±%s threshold\n' \
    "$((CHECKED - FAILURES))" "${CHECKED}" "${threshold_pct}"

if [ "${FAILURES}" -gt 0 ]; then
    printf '%d fixture(s) exceeded threshold — gate failed\n' "${FAILURES}" >&2
    exit 1
fi
