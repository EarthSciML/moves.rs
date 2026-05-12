#!/bin/bash
# convert-default-db.sh — top-level orchestrator for Phase 4 Task 80.
#
# Combines:
#   1. The SIF-bound MariaDB dump stage (`dump-default-db.sh`, runs inside
#      `characterization/apptainer/canonical-moves.sif`).
#   2. The pure-Rust TSV → Parquet conversion stage
#      (`moves-default-db-convert` crate binary).
#
# Re-runnability: invoke once per EPA default-DB release. The output is
# fully recreated under `${OUTPUT_ROOT}/${DB_VERSION}/`; nothing depends on
# residual state from a prior run.
#
# Usage:
#   convert-default-db.sh [options]
#
# Options:
#   --sif        PATH   Canonical-moves SIF. Default:
#                       characterization/apptainer/canonical-moves.sif
#   --db         NAME   Database name inside MariaDB. Default:
#                       movesdb20241112
#   --db-version LABEL  Label used for output subdir + manifest field.
#                       Default: <db>
#   --plan       PATH   tables.json from Task 79. Default:
#                       characterization/default-db-schema/tables.json
#   --output     PATH   Output root. Default: default-db
#   --tsv-dir    PATH   Skip stage 1 and use these TSVs instead. Useful
#                       for dry-runs against a pre-captured dump.
#   --source-dump PATH  Optional path to the source MariaDB .zip/.sql dump
#                       — only its SHA-256 is recorded in the manifest.
#   --strict            Pass --require-every-table to the Rust converter.
#   -h, --help          Print this help.
#
# Exit codes:
#   0 — success
#   1 — pipeline failure
#   2 — usage error

set -euo pipefail

usage() {
    sed -n '3,33p' "$0"
}

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SIF="${REPO_ROOT}/characterization/apptainer/canonical-moves.sif"
DB="movesdb20241112"
DB_VERSION=""
PLAN="${REPO_ROOT}/characterization/default-db-schema/tables.json"
OUTPUT_ROOT="${REPO_ROOT}/default-db"
TSV_DIR=""
SOURCE_DUMP=""
STRICT=0

while [ "$#" -gt 0 ]; do
    case "$1" in
        --sif) SIF="$2"; shift 2;;
        --db) DB="$2"; shift 2;;
        --db-version) DB_VERSION="$2"; shift 2;;
        --plan) PLAN="$2"; shift 2;;
        --output) OUTPUT_ROOT="$2"; shift 2;;
        --tsv-dir) TSV_DIR="$2"; shift 2;;
        --source-dump) SOURCE_DUMP="$2"; shift 2;;
        --strict) STRICT=1; shift;;
        -h|--help) usage; exit 0;;
        *) echo "unknown arg: $1" >&2; usage >&2; exit 2;;
    esac
done

DB_VERSION="${DB_VERSION:-$DB}"
OUTPUT_DIR="${OUTPUT_ROOT}/${DB_VERSION}"

mkdir -p "${OUTPUT_DIR}"

# Stage 1 — TSV dump from MariaDB inside the SIF.
if [ -z "${TSV_DIR}" ]; then
    if [ ! -f "${SIF}" ]; then
        echo "error: SIF not found at ${SIF}" >&2
        echo "       build it with characterization/apptainer/build-sif.sh, or pass --tsv-dir" >&2
        exit 1
    fi
    if ! command -v apptainer >/dev/null 2>&1; then
        echo "error: apptainer is not available on this host" >&2
        echo "       run the dump stage on an HPC compute node and pass --tsv-dir" >&2
        exit 1
    fi

    TSV_DIR="${OUTPUT_DIR}/_tsv"
    rm -rf "${TSV_DIR}"
    mkdir -p "${TSV_DIR}"

    SOURCE_DUMP_SHA=""
    if [ -n "${SOURCE_DUMP}" ]; then
        if [ ! -f "${SOURCE_DUMP}" ]; then
            echo "error: --source-dump file not found: ${SOURCE_DUMP}" >&2
            exit 1
        fi
        SOURCE_DUMP_SHA=$(sha256sum "${SOURCE_DUMP}" | awk '{print $1}')
    fi

    echo "[convert-default-db] stage 1: dumping ${DB} to ${TSV_DIR}"
    DUMP_SCRIPT="${REPO_ROOT}/characterization/default-db-conversion/dump-default-db.sh"
    apptainer exec \
        --bind "${TSV_DIR}:/captures" \
        --bind "${DUMP_SCRIPT}:/opt/moves-bin/dump-default-db.sh:ro" \
        --env "DEFAULT_DB=${DB}" \
        --env "SOURCE_DUMP_SHA=${SOURCE_DUMP_SHA}" \
        "${SIF}" \
        bash /opt/moves-bin/dump-default-db.sh
else
    echo "[convert-default-db] stage 1 skipped (using --tsv-dir=${TSV_DIR})"
fi

# Stage 2 — TSV → Parquet via the Rust converter.
echo "[convert-default-db] stage 2: writing parquet to ${OUTPUT_DIR}"
EXTRA=()
if [ "${STRICT}" -eq 1 ]; then
    EXTRA+=(--require-every-table)
fi
cargo run --quiet --release -p moves-default-db-convert -- \
    --tsv-dir "${TSV_DIR}" \
    --plan "${PLAN}" \
    --output "${OUTPUT_DIR}" \
    --moves-db-version "${DB_VERSION}" \
    "${EXTRA[@]}"

echo "[convert-default-db] complete. Manifest: ${OUTPUT_DIR}/manifest.json"
