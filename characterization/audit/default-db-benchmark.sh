#!/usr/bin/env bash
# default-DB end-to-end benchmark: native CLI data-plane vs wasm partition
# data-plane, per fixture, capturing wall-clock + peak RSS via /usr/bin/time -v
# (same mechanism as run-comparison.sh). Numerical fidelity vs the canonical
# MOVES snapshots is reported separately by the two correctness gates
# (default_db_snapshot_diff / default_db_canonical_diff); this script only
# measures runtime + memory of the two Rust code paths.
#
# Both engines run release-built, max_parallel_chunks=1, one OS process per
# (engine, fixture) so peak RSS is isolated. Canonical (EPA Java MOVES) is not
# run here — it supplies the numerical reference only.
#
# Env:
#   MOVES_DEFAULT_DB_DIR  default-DB Parquet tree (has manifest.json)   [required]
#   MOVES_WASM_TEST_BIN   release moves-wasm test binary (auto-detected if unset)
#   MOVES_BENCH_OUT       output dir (default /tmp/ddb-bench)
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DB_DIR="${MOVES_DEFAULT_DB_DIR:?set MOVES_DEFAULT_DB_DIR to the default-DB tree}"
MANIFEST="${DB_DIR}/manifest.json"
FIXTURES="${REPO}/characterization/fixtures"
NATIVE_BIN="${REPO}/target/release/moves"
OUT="${MOVES_BENCH_OUT:-/tmp/ddb-bench}"

# Auto-detect the newest release moves-wasm test binary if not given. The test
# binary is `moves_wasm-<hex>` with NO extension; `! -name '*.*'` excludes the
# sibling .d / .rlib / .rcgu.o / .o artifacts that share the prefix.
WASM_BIN="${MOVES_WASM_TEST_BIN:-}"
if [ -z "${WASM_BIN}" ]; then
  WASM_BIN="$(find "${REPO}/target/release/deps" -maxdepth 1 -type f -executable \
              -name 'moves_wasm-*' ! -name '*.*' -printf '%T@ %p\n' 2>/dev/null \
              | sort -rn | head -1 | cut -d' ' -f2- || true)"
fi
[ -x "${NATIVE_BIN}" ] || { echo "missing ${NATIVE_BIN} (cargo build --release -p moves-cli)"; exit 1; }
[ -x "${WASM_BIN}"   ] || { echo "missing release wasm test bin (cargo test --release -p moves-wasm --no-run)"; exit 1; }
[ -f "${MANIFEST}"   ] || { echo "missing ${MANIFEST}"; exit 1; }

echo "repo:      ${REPO}"
echo "db:        ${DB_DIR}"
echo "native:    ${NATIVE_BIN}"
echo "wasm bin:  ${WASM_BIN}"
echo "out:       ${OUT}"
echo

# 20 non-vacuous onroad default-DB fixtures (the 8 vacuous *-single / idle /
# apu / crankcase-* fixtures emit 0 rows and are skipped). Override with
# FIXTURES="a b c".
if [ -n "${FIXTURES:-}" ]; then
  read -ra FIXTURES_LIST <<< "${FIXTURES}"
else
  FIXTURES_LIST=(
    process-evap-fvv process-evap-leaks process-evap-permeation process-refueling
    expand-criteria expand-day expand-fueltype-diesel expand-sourcetype
    expand-month expand-counties sample-runspec chain-tog-speciation
    chain-nonhaptog process-nox-speciation process-crankcase-running
    process-brakewear process-tirewear process-airtoxics process-pm-exhaust
    mixed-onroad
  )
fi

rm -rf "${OUT}"; mkdir -p "${OUT}"
TIME=/usr/bin/time

for fx in "${FIXTURES_LIST[@]}"; do
  rs="${FIXTURES}/${fx}.xml"
  d="${OUT}/${fx}"; mkdir -p "${d}"
  [ -f "${rs}" ] || { echo "SKIP ${fx} (no runspec)"; continue; }

  echo "=== ${fx} ==="

  # --- native CLI default-DB path ---
  echo "  native..."
  ${TIME} -v -o "${d}/native-time.txt" \
    "${NATIVE_BIN}" run --runspec "${rs}" --default-db "${DB_DIR}" \
      --output "${d}/native-out" --max-parallel-chunks 1 \
      > "${d}/native-stdout.txt" 2>&1 || echo "    native FAILED (see ${d}/native-stdout.txt)"

  # --- wasm partition data-plane (profile_run_default_db harness) ---
  echo "  wasm..."
  MOVES_PROFILE_RUNSPEC="${rs}" \
  MOVES_PROFILE_MANIFEST="${MANIFEST}" \
  MOVES_PROFILE_DATA_DIR="${DB_DIR}" \
  MOVES_PROFILE_CHUNKS=1 \
  ${TIME} -v -o "${d}/wasm-time.txt" \
    "${WASM_BIN}" --ignored --exact --nocapture --test-threads=1 \
      tests::profile_run_default_db \
      > "${d}/wasm-stdout.txt" 2>&1 || echo "    wasm FAILED (see ${d}/wasm-stdout.txt)"
done

echo
echo "All runs complete. Aggregating..."
python3 "${REPO}/characterization/audit/default-db-benchmark-report.py" "${OUT}"
