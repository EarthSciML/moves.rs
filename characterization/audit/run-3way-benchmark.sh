#!/usr/bin/env bash
# run-3way-benchmark.sh — one-shot orchestrator for the default-DB end-to-end
# benchmark across canonical MOVES (Apptainer SIF), the native CLI data-plane,
# and the wasm partition data-plane. Measures runtime + peak memory + numerical
# fidelity (max per-pollutant rel_diff vs the canonical snapshots), and writes a
# Markdown report under characterization/audit-results/.
#
# Single entry point for both local runs and the default-db-benchmark CI
# (.github/workflows/default-db-benchmark.yml). Needs Apptainer + fakeroot/setuid
# (for the canonical SIF) and a Rust toolchain.
#
# Env (all optional):
#   MOVES_DEFAULT_DB_DIR  Pre-converted parquet default-DB tree (has manifest.json).
#                         If unset/invalid, it is regenerated from the SIF.
#   SIF                   canonical-moves.sif (default characterization/apptainer/).
#                         Built via build-sif.sh if absent.
#   MOVES_BENCH_SCRATCH   Roomy scratch root for run dirs + regenerated tree
#                         (default ${RUNNER_TEMP:-/tmp}). MariaDB seed (~GBs) and
#                         the parquet tree (~1 GB) land here — not a cramped /tmp.
#   FIXTURES              Space-separated fixture subset (default: all 20 onroad).
#   SKIP_BUILD=1          Assume release binaries already built.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
APPTAINER_DIR="${ROOT}/characterization/apptainer"
SIF="${SIF:-${APPTAINER_DIR}/canonical-moves.sif}"
SCRATCH="${MOVES_BENCH_SCRATCH:-${RUNNER_TEMP:-/tmp}}"
OUT_CANON="${SCRATCH}/ddb-bench-canonical"
OUT_NW="${SCRATCH}/ddb-bench"
REPORT_DIR="${ROOT}/characterization/audit-results"
TS="$(date -u +%Y%m%dT%H%M%S)"
REPORT="${REPORT_DIR}/${TS}-3way-default-db-benchmark.md"
mkdir -p "${SCRATCH}" "${REPORT_DIR}"
export WORKDIR="${SCRATCH}/moves-canonical"

log() { printf '\n[3way] %s\n' "$*" >&2; }

# 1. Release binaries: native CLI + the wasm test binary (carries the
#    profile_run_default_db harness the wasm benchmark drives).
if [ "${SKIP_BUILD:-0}" != "1" ]; then
  log "building release moves-cli + moves-wasm test binary"
  cargo build --release -p moves-cli --manifest-path "${ROOT}/Cargo.toml"
  cargo test  --release -p moves-wasm --no-run --manifest-path "${ROOT}/Cargo.toml"
fi

# 2. Canonical SIF — build if missing.
if [ ! -f "${SIF}" ]; then
  log "canonical SIF absent; building (build-sif.sh, ~30-60 min)"
  ( cd "${APPTAINER_DIR}" && ./build-sif.sh )
fi

# 3. Parquet default-DB tree — reuse if valid, else regenerate from the SIF.
if [ -n "${MOVES_DEFAULT_DB_DIR:-}" ] && [ -f "${MOVES_DEFAULT_DB_DIR}/manifest.json" ]; then
  log "reusing default-DB tree ${MOVES_DEFAULT_DB_DIR}"
else
  log "regenerating default-DB parquet tree from the SIF"
  "${ROOT}/characterization/default-db-conversion/convert-default-db.sh" \
    --sif "${SIF}" --output "${SCRATCH}/moves-defaultdb"
  export MOVES_DEFAULT_DB_DIR="${SCRATCH}/moves-defaultdb/movesdb20241112"
  # Drop the bulky intermediate TSV dump — the benchmark doesn't validate, and
  # on a hosted runner the root disk is tight (the canonical sweep still has to
  # seed its own MariaDB datadir).
  rm -rf "${MOVES_DEFAULT_DB_DIR}/_tsv"
fi

# 4. Canonical MOVES timings (serial single-worker, tree-RSS sampled).
log "canonical MOVES sweep"
SIF="${SIF}" MOVES_BENCH_OUT="${OUT_CANON}" "${HERE}/canonical-benchmark.sh"

# 5. Native CLI + wasm data-plane timings (release, chunks=1).
log "native + wasm sweep"
MOVES_BENCH_OUT="${OUT_NW}" "${HERE}/default-db-benchmark.sh"

# 6. Numerical: both gates -> per-fixture max_rel_diff vs canonical snapshots.
log "native default-DB gate (rel_diff)"
NG="${SCRATCH}/native-gate.log"
cargo test -p moves-cli --test full_suite_regression default_db_snapshot_diff \
  --manifest-path "${ROOT}/Cargo.toml" -- --nocapture > "${NG}" 2>&1 || true
log "wasm default-DB gate (rel_diff)"
WG="${SCRATCH}/wasm-gate.log"
cargo test -p moves-wasm default_db_canonical_diff \
  --manifest-path "${ROOT}/Cargo.toml" -- --nocapture > "${WG}" 2>&1 || true

# 7. Assemble the 3-way Markdown report.
log "assembling report -> ${REPORT}"
{
  echo "# 3-way default-DB end-to-end benchmark — canonical vs native vs wasm"
  echo
  echo "_Generated ${TS} · $(git -C "${ROOT}" rev-parse --abbrev-ref HEAD 2>/dev/null || echo '?') @ $(git -C "${ROOT}" rev-parse --short HEAD 2>/dev/null || echo '?')_"
  echo
  python3 "${HERE}/default-db-benchmark-report.py" "${OUT_NW}" \
    --canonical-dir "${OUT_CANON}" --native-gate "${NG}" --wasm-gate "${WG}"
} > "${REPORT}"

echo "[3way] report: ${REPORT}" >&2
# Surface outputs to a CI job when running under GitHub Actions.
if [ -n "${GITHUB_OUTPUT:-}" ]; then
  {
    echo "report_file=${REPORT}"
    echo "timestamp=${TS}"
  } >> "${GITHUB_OUTPUT}"
fi
