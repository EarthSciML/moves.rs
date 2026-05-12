#!/bin/bash
# validate-default-db.sh — top-level orchestrator for Phase 4 Task 81.
#
# Runs the Task 80 conversion pipeline on `movesdb20241112.zip` (or any
# EPA default-DB release) and then validates the converted Parquet tree
# against the source TSV dump:
#
#   1. The conversion stage (`convert-default-db.sh`) produces:
#        <output-root>/<db-version>/manifest.json
#        <output-root>/<db-version>/<Table>.parquet | <Table>/<partition>/...
#        <output-root>/<db-version>/_tsv/<table>.tsv + <table>.schema.tsv
#
#   2. The validation stage (`moves-default-db-validate`) cross-checks:
#        • manifest <-> on-disk parquet (path + sha256 + row count)
#        • parquet schema <-> manifest column list
#        • parquet row totals <-> source TSV line count
#        • per-column numeric aggregates (count/min/max/scaled-sum) between
#          TSV-parsed values and Parquet readback
#        • monolithic tables: byte-by-byte first-row match
#
# Exit codes:
#   0 — conversion + validation succeeded with no errors
#   1 — conversion failed
#   2 — validation reported errors
#   3 — usage error

set -euo pipefail

usage() {
    sed -n '3,30p' "$0"
}

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SIF="${REPO_ROOT}/characterization/apptainer/canonical-moves.sif"
DB="movesdb20241112"
DB_VERSION=""
PLAN="${REPO_ROOT}/characterization/default-db-schema/tables.json"
OUTPUT_ROOT="${REPO_ROOT}/default-db"
SOURCE_DUMP=""
TSV_DIR=""
AGGREGATE_ROW_CAP=""
SKIP_CONVERT=0
JSON_OUTPUT=0

while [ "$#" -gt 0 ]; do
    case "$1" in
        --sif) SIF="$2"; shift 2;;
        --db) DB="$2"; shift 2;;
        --db-version) DB_VERSION="$2"; shift 2;;
        --plan) PLAN="$2"; shift 2;;
        --output) OUTPUT_ROOT="$2"; shift 2;;
        --source-dump) SOURCE_DUMP="$2"; shift 2;;
        --tsv-dir) TSV_DIR="$2"; shift 2;;
        --aggregate-row-cap) AGGREGATE_ROW_CAP="$2"; shift 2;;
        --skip-convert) SKIP_CONVERT=1; shift;;
        --json) JSON_OUTPUT=1; shift;;
        -h|--help) usage; exit 0;;
        *) echo "unknown arg: $1" >&2; usage >&2; exit 3;;
    esac
done

DB_VERSION="${DB_VERSION:-$DB}"
OUTPUT_DIR="${OUTPUT_ROOT}/${DB_VERSION}"

# Conversion (skippable when iterating on the validator alone).
if [ "${SKIP_CONVERT}" -eq 0 ]; then
    CONVERT_ARGS=(--sif "${SIF}" --db "${DB}" --db-version "${DB_VERSION}"
                  --plan "${PLAN}" --output "${OUTPUT_ROOT}")
    if [ -n "${SOURCE_DUMP}" ]; then
        CONVERT_ARGS+=(--source-dump "${SOURCE_DUMP}")
    fi
    if [ -n "${TSV_DIR}" ]; then
        CONVERT_ARGS+=(--tsv-dir "${TSV_DIR}")
    fi
    echo "[validate-default-db] running conversion stage"
    "${REPO_ROOT}/characterization/default-db-conversion/convert-default-db.sh" "${CONVERT_ARGS[@]}"
fi

# Pick the TSV directory the converter wrote, unless overridden.
EFFECTIVE_TSV_DIR="${TSV_DIR:-${OUTPUT_DIR}/_tsv}"

echo "[validate-default-db] running validation stage"
VALIDATE_ARGS=(--output-root "${OUTPUT_DIR}" --tsv-dir "${EFFECTIVE_TSV_DIR}")
if [ -n "${AGGREGATE_ROW_CAP}" ]; then
    VALIDATE_ARGS+=(--aggregate-row-cap "${AGGREGATE_ROW_CAP}")
fi
if [ "${JSON_OUTPUT}" -eq 1 ]; then
    VALIDATE_ARGS+=(--json)
fi

set +e
cargo run --quiet --release \
    --manifest-path "${REPO_ROOT}/crates/moves-default-db-convert/Cargo.toml" \
    --bin moves-default-db-validate -- "${VALIDATE_ARGS[@]}"
rc=$?
set -e

if [ ${rc} -eq 0 ]; then
    echo "[validate-default-db] PASS"
    exit 0
elif [ ${rc} -eq 1 ]; then
    echo "[validate-default-db] FAIL — validation reported errors"
    exit 2
else
    echo "[validate-default-db] FAIL — validator binary errored (rc=${rc})"
    exit 1
fi
