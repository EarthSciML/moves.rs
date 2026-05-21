#!/usr/bin/env bash
# upstream-update.sh — incorporate a new EPA MOVES release into moves.rs.
#
# This script automates the mechanical steps of the annual MOVES upstream
# update workflow.  It does NOT modify any committed files by itself — that
# is left to the operator so each change can be reviewed and committed
# individually.  The script is designed to run on an HPC node where
# Apptainer + fakeroot are available; most steps can also be dry-run on a
# workstation by passing --skip-sif-build.
#
# Typical invocation:
#
#   scripts/upstream-update.sh \
#       --moves-commit <40-hex-SHA>     \
#       --movesdb-zip   /path/to/movesdb20251023.zip
#
# The script prints a structured "UPDATE REPORT" at the end summarising
# every change detected across schema, Parquet, fixtures, and snapshot
# drift.  Pipe the output to a file to keep a record:
#
#   scripts/upstream-update.sh ... 2>&1 | tee /tmp/upstream-update-$(date +%Y%m%d).log
#
# Usage:
#   scripts/upstream-update.sh [options]
#
# Required:
#   --moves-commit SHA     40-hex SHA of the new EPA_MOVES_Model commit.
#   --movesdb-zip   PATH   Path to the new movesdb*.zip downloaded from EPA.
#
# Optional:
#   --moves-src     DIR    Directory for shallow-cloned MOVES source.
#                          Default: /tmp/moves-upstream-src
#   --sif           PATH   Rebuilt canonical-moves SIF path.
#                          Default: characterization/apptainer/canonical-moves.sif
#   --parquet-out   DIR    Root for the regenerated Parquet tree.
#                          Default: default-db
#   --snapshots-out DIR    Where fresh fixture snapshots are written.
#                          Default: /tmp/upstream-snapshots
#   --skip-sif-build       Skip SIF rebuild (use existing SIF — for
#                          dry-runs or when the SIF is already current).
#   --skip-parquet         Skip Parquet conversion stage.
#   --skip-fixtures        Skip characterization fixture suite.
#   -h, --help             Print this help.
#
# Prerequisites:
#   * apptainer  (with --fakeroot or setuid)
#   * git
#   * python3 (for audit-schema.py and diff-schema.py)
#   * cargo (Rust toolchain)
#   A self-hosted HPC runner satisfies all of these; see the GitHub Actions
#   workflow .github/workflows/upstream-update.yml for the CI-equivalent.
#
# Exit codes:
#   0  — all stages completed; check the UPDATE REPORT for action items
#   1  — a required stage failed (details in the log)
#   2  — usage error

set -euo pipefail

# ---------------------------------------------------------------------------
# Colours / logging
# ---------------------------------------------------------------------------
if [ -t 1 ] && command -v tput >/dev/null 2>&1; then
    _BOLD="$(tput bold)"
    _GREEN="$(tput setaf 2)"
    _YELLOW="$(tput setaf 3)"
    _RED="$(tput setaf 1)"
    _RESET="$(tput sgr0)"
else
    _BOLD="" _GREEN="" _YELLOW="" _RED="" _RESET=""
fi

_log()   { printf '%s[up] %s%s\n'  "${_BOLD}" "$*" "${_RESET}"; }
_ok()    { printf '%s[up] ✓ %s%s\n' "${_GREEN}" "$*" "${_RESET}"; }
_warn()  { printf '%s[up] ⚠ %s%s\n' "${_YELLOW}" "$*" "${_RESET}" >&2; }
_err()   { printf '%s[up] ✗ %s%s\n' "${_RED}" "$*" "${_RESET}" >&2; }
_sep()   { printf '%s[up] %s%s\n' "${_BOLD}" "────────────────────────────────────────────────────────" "${_RESET}"; }

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MOVES_COMMIT=""
MOVESDB_ZIP=""
MOVES_SRC="/tmp/moves-upstream-src"
SIF="${REPO_ROOT}/characterization/apptainer/canonical-moves.sif"
PARQUET_OUT="${REPO_ROOT}/default-db"
SNAPSHOTS_OUT="/tmp/upstream-snapshots"
SKIP_SIF_BUILD=0
SKIP_PARQUET=0
SKIP_FIXTURES=0

# Paths derived from REPO_ROOT
VERSIONS_ENV="${REPO_ROOT}/characterization/apptainer/files/versions.env"
TABLES_JSON="${REPO_ROOT}/characterization/default-db-schema/tables.json"
AUDIT_PY="${REPO_ROOT}/characterization/default-db-schema/audit-schema.py"
DIFF_PY="${REPO_ROOT}/characterization/default-db-schema/diff-schema.py"
CONVERT_SH="${REPO_ROOT}/characterization/default-db-conversion/convert-default-db.sh"
PLAN_JSON="${REPO_ROOT}/characterization/default-db-schema/tables.json"
FIXTURE_SH="${REPO_ROOT}/characterization/run-all-fixtures.sh"
FIXTURE_SIF="${REPO_ROOT}/characterization/apptainer/moves-fixture.sif"
SNAPSHOTS_BASELINE="${REPO_ROOT}/characterization/snapshots"
SNAP_DIFF_BIN="${REPO_ROOT}/target/release/moves-snapshot"

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
usage() { sed -n '3,55p' "$0" | sed 's/^# \{0,1\}//'; }

while [ "$#" -gt 0 ]; do
    case "$1" in
        --moves-commit)  MOVES_COMMIT="$2";   shift 2;;
        --movesdb-zip)   MOVESDB_ZIP="$2";    shift 2;;
        --moves-src)     MOVES_SRC="$2";      shift 2;;
        --sif)           SIF="$2";            shift 2;;
        --parquet-out)   PARQUET_OUT="$2";    shift 2;;
        --snapshots-out) SNAPSHOTS_OUT="$2";  shift 2;;
        --skip-sif-build)   SKIP_SIF_BUILD=1; shift;;
        --skip-parquet)     SKIP_PARQUET=1;   shift;;
        --skip-fixtures)    SKIP_FIXTURES=1;  shift;;
        -h|--help)       usage; exit 0;;
        *) _err "unknown argument: $1"; usage >&2; exit 2;;
    esac
done

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
if [ -z "${MOVES_COMMIT}" ]; then
    _err "--moves-commit is required"
    usage >&2; exit 2
fi
if ! [[ "${MOVES_COMMIT}" =~ ^[0-9a-fA-F]{40}$ ]]; then
    _err "--moves-commit must be a 40-hex-char SHA, got: ${MOVES_COMMIT}"
    exit 2
fi
if [ -z "${MOVESDB_ZIP}" ] && [ "${SKIP_SIF_BUILD}" = "0" ]; then
    _err "--movesdb-zip is required unless --skip-sif-build is passed"
    usage >&2; exit 2
fi
if [ -n "${MOVESDB_ZIP}" ] && [ ! -f "${MOVESDB_ZIP}" ]; then
    _err "movesdb zip not found: ${MOVESDB_ZIP}"
    exit 1
fi

# ---------------------------------------------------------------------------
# Stage tracking (for the summary at the end)
# ---------------------------------------------------------------------------
REPORT=()
WARNINGS=()

_report()  { REPORT+=("$*"); }
_warning() { WARNINGS+=("$*"); }

# ---------------------------------------------------------------------------
# Stage 0: Derive movesdb metadata from the zip name
# ---------------------------------------------------------------------------
_sep
_log "Stage 0: derive database version from zip path"

MOVESDB_VERSION=""
if [ -n "${MOVESDB_ZIP}" ]; then
    MOVESDB_FILENAME="$(basename "${MOVESDB_ZIP}")"
    # Extract the version from filenames like movesdb20251023.zip
    MOVESDB_VERSION="${MOVESDB_FILENAME%.zip}"
    MOVESDB_VERSION="${MOVESDB_VERSION#movesdb}"
    if [ -z "${MOVESDB_VERSION}" ]; then
        _err "Cannot derive DB version from zip filename: ${MOVESDB_FILENAME}"
        _err "Expected a filename like movesdb20251023.zip"
        exit 1
    fi
    MOVESDB_LABEL="movesdb${MOVESDB_VERSION}"
    MOVESDB_SHA256="$(sha256sum "${MOVESDB_ZIP}" | awk '{print $1}')"
    _ok "  movesdb label:  ${MOVESDB_LABEL}"
    _ok "  movesdb sha256: ${MOVESDB_SHA256}"
fi

# Read current version pins for comparison
. "${VERSIONS_ENV}"
CURRENT_COMMIT="${MOVES_COMMIT_CURRENT:-${MOVES_COMMIT:-}}"
CURRENT_COMMIT="$(grep '^MOVES_COMMIT=' "${VERSIONS_ENV}" | cut -d'"' -f2)"
CURRENT_MOVESDB="$(grep '^MOVESDB_FILENAME=' "${VERSIONS_ENV}" | cut -d'"' -f2 || true)"
CURRENT_MOVESDB="${CURRENT_MOVESDB%.zip}"

_log "  current MOVES commit:  ${CURRENT_COMMIT:-unknown}"
_log "  new     MOVES commit:  ${MOVES_COMMIT}"
_log "  current movesdb:       ${CURRENT_MOVESDB:-unknown}"
if [ -n "${MOVESDB_LABEL:-}" ]; then
    _log "  new     movesdb:       ${MOVESDB_LABEL}"
fi

# ---------------------------------------------------------------------------
# Stage 1: Fetch new MOVES source to extract DDL files
# ---------------------------------------------------------------------------
_sep
_log "Stage 1: shallow-clone MOVES source at ${MOVES_COMMIT:0:12}"

mkdir -p "${MOVES_SRC}"
if [ -d "${MOVES_SRC}/.git" ]; then
    _log "  re-using existing clone at ${MOVES_SRC}"
    git -C "${MOVES_SRC}" fetch --depth 1 origin "${MOVES_COMMIT}" 2>&1 | sed 's/^/  /'
    git -C "${MOVES_SRC}" checkout FETCH_HEAD 2>&1 | sed 's/^/  /'
else
    MOVES_REPO_URL="$(grep '^MOVES_REPO_URL=' "${VERSIONS_ENV}" | cut -d'"' -f2)"
    _log "  cloning ${MOVES_REPO_URL}"
    git -C "${MOVES_SRC}" init 2>&1 | sed 's/^/  /'
    git -C "${MOVES_SRC}" remote add origin "${MOVES_REPO_URL}" 2>&1 | sed 's/^/  /'
    git -C "${MOVES_SRC}" fetch --depth 1 origin "${MOVES_COMMIT}" 2>&1 | sed 's/^/  /'
    git -C "${MOVES_SRC}" checkout FETCH_HEAD 2>&1 | sed 's/^/  /'
fi
ACTUAL_COMMIT="$(git -C "${MOVES_SRC}" rev-parse HEAD)"
_ok "  MOVES source at: ${ACTUAL_COMMIT}"

DDL_DEFAULT="${MOVES_SRC}/database/CreateDefault.sql"
DDL_NR="${MOVES_SRC}/database/CreateNRDefault.sql"
for f in "${DDL_DEFAULT}" "${DDL_NR}"; do
    if [ ! -f "${f}" ]; then
        _err "DDL file not found: ${f}"
        _err "The MOVES source layout may have changed; check database/ directory."
        exit 1
    fi
done
_ok "  CreateDefault.sql and CreateNRDefault.sql found"

# ---------------------------------------------------------------------------
# Stage 2: Regenerate schema audit (tables.json)
# ---------------------------------------------------------------------------
_sep
_log "Stage 2: regenerate default-DB schema audit"

NEW_TABLES_JSON="/tmp/tables-new-${MOVES_COMMIT:0:12}.json"

python3 "${AUDIT_PY}" \
    --default-sql   "${DDL_DEFAULT}" \
    --nr-default-sql "${DDL_NR}" \
    --moves-commit  "${MOVES_COMMIT}" \
    --output        "${NEW_TABLES_JSON}" 2>&1 | sed 's/^/  /'

_ok "  new tables.json written to ${NEW_TABLES_JSON}"

# ---------------------------------------------------------------------------
# Stage 3: Schema diff
# ---------------------------------------------------------------------------
_sep
_log "Stage 3: diff schema against current tables.json"

SCHEMA_DIFF_TXT="/tmp/schema-diff-${MOVES_COMMIT:0:12}.txt"
SCHEMA_DIFF_JSON="/tmp/schema-diff-${MOVES_COMMIT:0:12}.json"

python3 "${DIFF_PY}" \
    "${TABLES_JSON}" "${NEW_TABLES_JSON}" \
    --format text \
    --output "${SCHEMA_DIFF_TXT}" || SCHEMA_DIFF_RC=$?
SCHEMA_DIFF_RC="${SCHEMA_DIFF_RC:-0}"

python3 "${DIFF_PY}" \
    "${TABLES_JSON}" "${NEW_TABLES_JSON}" \
    --format json \
    --output "${SCHEMA_DIFF_JSON}" || true

cat "${SCHEMA_DIFF_TXT}" | sed 's/^/  /'

if [ "${SCHEMA_DIFF_RC}" = "0" ]; then
    _ok "  Schema unchanged — no tables.json update needed."
    _report "SCHEMA: no changes detected"
else
    _warn "  Schema drift detected. Review ${SCHEMA_DIFF_TXT} and update:"
    _warn "  1. Copy ${NEW_TABLES_JSON} over characterization/default-db-schema/tables.json"
    _warn "  2. Verify the partition strategy for each new/changed table."
    _warn "  3. Update partitioning-plan.md if a new large table was added."
    _warn "  4. Search crates/ for any Rust reader / calculator that reads changed tables."
    _report "SCHEMA: drift detected — see ${SCHEMA_DIFF_TXT}"
fi

# ---------------------------------------------------------------------------
# Stage 4: Update versions.env (dry-run preview; operator must apply)
# ---------------------------------------------------------------------------
_sep
_log "Stage 4: generate updated versions.env (preview only — not applied)"

UPDATED_VERSIONS_ENV="/tmp/versions-updated-${MOVES_COMMIT:0:12}.env"

sed \
    -e "s|^MOVES_COMMIT=.*|MOVES_COMMIT=\"${MOVES_COMMIT}\"|" \
    "${VERSIONS_ENV}" > "${UPDATED_VERSIONS_ENV}"

if [ -n "${MOVESDB_LABEL:-}" ]; then
    sed -i \
        -e "s|^MOVESDB_FILENAME=.*|MOVESDB_FILENAME=\"${MOVESDB_LABEL}.zip\"|" \
        -e "s|^MOVESDB_SHA256=.*|MOVESDB_SHA256=\"${MOVESDB_SHA256}\"|" \
        "${UPDATED_VERSIONS_ENV}"
fi

_log "  diff preview:"
diff "${VERSIONS_ENV}" "${UPDATED_VERSIONS_ENV}" | sed 's/^/    /' || true
_warn "  Apply by running:"
_warn "    cp ${UPDATED_VERSIONS_ENV} ${VERSIONS_ENV}"

# ---------------------------------------------------------------------------
# Stage 5: Rebuild SIF
# ---------------------------------------------------------------------------
_sep
if [ "${SKIP_SIF_BUILD}" = "1" ]; then
    _log "Stage 5: SIF rebuild (skipped — --skip-sif-build)"
    _report "SIF: skipped (--skip-sif-build)"
else
    _log "Stage 5: rebuild canonical-moves SIF"
    _log "  This step takes 30–90 minutes on first run; subsequent runs are"
    _log "  faster when base layers are cached."

    MOVES_COMMIT="${MOVES_COMMIT}" \
    MOVESDB_SHA256="${MOVESDB_SHA256:-}" \
    MOVESDB_LOCAL_PATH="${MOVESDB_ZIP}" \
    OUTPUT="${SIF}" \
        "${REPO_ROOT}/characterization/apptainer/build-sif.sh" 2>&1 | sed 's/^/  /'

    _ok "  SIF rebuilt: ${SIF}"
    NEW_SIF_SHA="$(sha256sum "${SIF}" | awk '{print $1}')"
    _ok "  new SIF SHA256: ${NEW_SIF_SHA}"
    _report "SIF: rebuilt — sha256=${NEW_SIF_SHA}"
fi

# ---------------------------------------------------------------------------
# Stage 6: Regenerate Parquet
# ---------------------------------------------------------------------------
_sep
if [ "${SKIP_PARQUET}" = "1" ]; then
    _log "Stage 6: Parquet regeneration (skipped — --skip-parquet)"
    _report "PARQUET: skipped"
else
    _log "Stage 6: regenerate default-DB Parquet tree"

    DB_LABEL="${MOVESDB_LABEL:-$(grep '^MOVESDB_FILENAME=' "${VERSIONS_ENV}" | cut -d'"' -f2 | sed 's/\.zip$//')}"

    "${CONVERT_SH}" \
        --sif        "${SIF}" \
        --db         "${DB_LABEL}" \
        --db-version "${DB_LABEL}" \
        --plan       "${PLAN_JSON}" \
        --output     "${PARQUET_OUT}" \
        ${MOVESDB_ZIP:+--source-dump "${MOVESDB_ZIP}"} 2>&1 | sed 's/^/  /'

    _ok "  Parquet tree: ${PARQUET_OUT}/${DB_LABEL}/"
    MANIFEST="${PARQUET_OUT}/${DB_LABEL}/manifest.json"
    if [ -f "${MANIFEST}" ]; then
        TABLE_COUNT="$(python3 -c "import json,sys; d=json.load(open('${MANIFEST}')); print(len(d['tables']))")"
        _ok "  tables converted: ${TABLE_COUNT}"
    fi
    _report "PARQUET: regenerated — ${PARQUET_OUT}/${DB_LABEL}/"
fi

# ---------------------------------------------------------------------------
# Stage 7: Run characterization fixture suite
# ---------------------------------------------------------------------------
_sep
if [ "${SKIP_FIXTURES}" = "1" ]; then
    _log "Stage 7: fixture suite (skipped — --skip-fixtures)"
    _report "FIXTURES: skipped"
else
    _log "Stage 7: run full characterization fixture suite"
    _log "  Output: ${SNAPSHOTS_OUT}"
    _log "  (Use --skip-fixtures to skip on schema-only update passes)"

    mkdir -p "${SNAPSHOTS_OUT}"

    if [ ! -f "${FIXTURE_SIF}" ]; then
        _warn "  moves-fixture.sif not found; building it now..."
        "${REPO_ROOT}/characterization/apptainer/build-fixture-sif.sh" 2>&1 | sed 's/^/  /'
    fi

    FIXTURE_FAIL=0
    "${FIXTURE_SH}" \
        --sif        "${FIXTURE_SIF}" \
        --output-root "${SNAPSHOTS_OUT}" \
        --keep-going 2>&1 | sed 's/^/  /' || FIXTURE_FAIL=$?

    if [ "${FIXTURE_FAIL}" = "0" ]; then
        _ok "  All fixtures produced snapshots."
        _report "FIXTURES: all passed"
    else
        _warn "  One or more fixtures failed — see log above."
        _report "FIXTURES: partial failure (see log)"
    fi
fi

# ---------------------------------------------------------------------------
# Stage 8: Diff snapshots against baseline
# ---------------------------------------------------------------------------
_sep
_log "Stage 8: diff new snapshots against committed baseline"

if [ "${SKIP_FIXTURES}" = "1" ]; then
    _log "  (skipped — fixture suite was not run)"
    _report "SNAPSHOT DIFF: skipped"
elif [ ! -d "${SNAPSHOTS_OUT}" ]; then
    _log "  (skipped — no fresh snapshots at ${SNAPSHOTS_OUT})"
    _report "SNAPSHOT DIFF: skipped (no fresh snapshots)"
else
    # Build the snapshot diff binary if not already built.
    if [ ! -x "${SNAP_DIFF_BIN}" ]; then
        _log "  Building moves-snapshot binary..."
        cargo build --release --locked -p moves-snapshot \
            -q 2>&1 | sed 's/^/    /' || true
    fi

    TOLERANCE="${REPO_ROOT}/characterization/tolerance.toml"
    SNAP_DIFF_RC=0
    DIFFS_DIR="/tmp/snapshot-diffs-${MOVES_COMMIT:0:12}"
    mkdir -p "${DIFFS_DIR}"
    DRIFT_FIXTURES=()
    UNCHANGED_COUNT=0

    for fresh in "${SNAPSHOTS_OUT}"/*/; do
        name="$(basename "${fresh}")"
        baseline="${SNAPSHOTS_BASELINE}/${name}"
        if [ ! -d "${baseline}" ]; then
            _warn "  no committed baseline for ${name} — skipping diff"
            _warning "SNAPSHOT: no baseline for ${name}"
            continue
        fi
        DIFF_OUT="${DIFFS_DIR}/${name}.json"
        if "${SNAP_DIFF_BIN}" diff "${baseline}" "${fresh}" \
                --format json \
                ${TOLERANCE:+--tolerance "${TOLERANCE}"} \
                > "${DIFF_OUT}" 2>&1; then
            UNCHANGED_COUNT=$((UNCHANGED_COUNT + 1))
        else
            DRIFT_FIXTURES+=("${name}")
            _warn "  snapshot drift in ${name} — see ${DIFF_OUT}"
        fi
    done

    if [ "${#DRIFT_FIXTURES[@]}" -gt 0 ]; then
        _warn "  ${#DRIFT_FIXTURES[@]} fixture(s) with snapshot drift:"
        for f in "${DRIFT_FIXTURES[@]}"; do
            _warn "    - ${f}"
        done
        _warn "  Each JSON diff in ${DIFFS_DIR}/ describes the drifted cells."
        _warn "  If the drift is expected (rate updates from EPA), update:"
        _warn "    characterization/snapshots/<fixture>/ with the new outputs."
        _warn "  Update docs/known-divergences.md with any new divergences."
        _report "SNAPSHOT DIFF: drift in ${#DRIFT_FIXTURES[@]} fixture(s) — review ${DIFFS_DIR}/"
    else
        _ok "  ${UNCHANGED_COUNT} fixture(s) match baseline — no snapshot drift."
        _report "SNAPSHOT DIFF: no drift (${UNCHANGED_COUNT} fixtures compared)"
    fi
fi

# ---------------------------------------------------------------------------
# Stage 9: Cargo tests against new version pins
# ---------------------------------------------------------------------------
_sep
_log "Stage 9: cargo test (unit + integration)"

CARGO_FAIL=0
cargo test --workspace --all-targets -q 2>&1 | sed 's/^/  /' || CARGO_FAIL=$?

if [ "${CARGO_FAIL}" = "0" ]; then
    _ok "  All cargo tests passed."
    _report "CARGO TEST: passed"
else
    _warn "  cargo test failed — see output above."
    _report "CARGO TEST: FAILED"
fi

# ---------------------------------------------------------------------------
# Final report
# ---------------------------------------------------------------------------
_sep
printf '\n%s[up] ══════════════════ UPDATE REPORT ══════════════════%s\n' "${_BOLD}" "${_RESET}"
printf '%s[up]  MOVES commit:  %s%s\n' "" "${MOVES_COMMIT}" ""
if [ -n "${MOVESDB_LABEL:-}" ]; then
    printf '%s[up]  default DB:   %s%s\n' "" "${MOVESDB_LABEL}" ""
fi
printf '\n'
for line in "${REPORT[@]}"; do
    printf '%s[up]  %s%s\n' "" "${line}" ""
done
if [ "${#WARNINGS[@]}" -gt 0 ]; then
    printf '\n%s[up]  Warnings:%s\n' "${_YELLOW}" "${_RESET}"
    for w in "${WARNINGS[@]}"; do
        printf '%s[up]    ⚠ %s%s\n' "${_YELLOW}" "${w}" "${_RESET}"
    done
fi
printf '\n%s[up]  Manual steps required:%s\n' "${_BOLD}" "${_RESET}"
printf '%s[up]  1. Apply updated versions.env:\n' ""
printf '%s[up]       cp %s %s\n' "" "${UPDATED_VERSIONS_ENV}" "${VERSIONS_ENV}"
if [ "${SCHEMA_DIFF_RC:-0}" != "0" ]; then
    printf '%s[up]  2. Apply updated tables.json:\n' ""
    printf '%s[up]       cp %s %s\n' "" "${NEW_TABLES_JSON}" "${TABLES_JSON}"
    printf '%s[up]     Then update partitioning-plan.md for new/changed tables.\n' ""
fi
printf '%s[up]  3. Update characterization/canonical-image.lock (built by build-sif.sh).\n' ""
printf '%s[up]  4. If snapshot drift: commit updated snapshots under\n' ""
printf '%s[up]       characterization/snapshots/\n' ""
printf '%s[up]  5. Update docs/known-divergences.md with any new findings.\n' ""
printf '%s[up]  6. Bump crate versions and CHANGELOG for the release.\n' ""
printf '%s[up]  See docs/upstream-tracking.md for detailed guidance.\n' ""
printf '%s[up] ═══════════════════════════════════════════════════%s\n' "${_BOLD}" "${_RESET}"
