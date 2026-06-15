#!/usr/bin/env bash
# canonical-benchmark.sh — time + memory-profile canonical MOVES (the Apptainer
# SIF) on the default-DB end-to-end fixtures, for the 3-way benchmark vs the
# native + wasm Rust data planes.
#
# Canonical MOVES is multi-process (master JVM + worker JVM + mariadbd +
# NONROAD/Go calculators). /usr/bin/time -v sees only the immediate child's
# peak RSS, so we ALSO sample the whole process-tree's aggregate RSS via
# tree-rss-sampler.py and report that as the canonical memory figure.
#
# Numerical results are NOT re-extracted here — the canonical emissions are the
# in-repo snapshots (characterization/snapshots/<fixture>), which the gates
# already compare native/wasm against. This script measures runtime + memory.
#
# Env:
#   SIF              canonical SIF (default characterization/apptainer/canonical-moves.sif)
#   MOVES_BENCH_OUT  output dir (default /tmp/ddb-bench-canonical)
#   FIXTURES         space-separated override of the fixture list
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
APPTAINER_DIR="${ROOT}/characterization/apptainer"
SIF="${SIF:-${APPTAINER_DIR}/canonical-moves.sif}"
FIXTURES_DIR="${ROOT}/characterization/fixtures"
OUT="${MOVES_BENCH_OUT:-/tmp/ddb-bench-canonical}"
SAMPLER="${HERE}/tree-rss-sampler.py"
export WORKDIR="${WORKDIR:-/scratch.local/${USER}/moves-canonical}"

[ -f "${SIF}" ] || { echo "missing SIF ${SIF}"; exit 1; }

# Default: the 20 non-vacuous onroad default-DB fixtures (same set the
# native/wasm benchmark uses). Override with FIXTURES="a b c".
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

echo "SIF:     ${SIF}"
echo "WORKDIR: ${WORKDIR}"
echo "out:     ${OUT}"
echo "fixtures: ${#FIXTURES_LIST[@]}"
echo
mkdir -p "${OUT}"

for fx in "${FIXTURES_LIST[@]}"; do
  rs="${FIXTURES_DIR}/${fx}.xml"
  d="${OUT}/${fx}"; mkdir -p "${d}"
  [ -f "${rs}" ] || { echo "SKIP ${fx} (no runspec)"; continue; }
  echo "=== ${fx} ==="

  # Launch run-moves.sh in the background so we can sample its process tree.
  ( /usr/bin/time -v -o "${d}/canon-time.txt" \
      "${APPTAINER_DIR}/run-moves.sh" --fakeroot --runspec "${rs}" \
      > "${d}/canon-stdout.txt" 2>&1 ) &
  run_pid=$!

  # Sample the aggregate RSS of the whole tree until run-moves.sh exits.
  python3 "${SAMPLER}" "${run_pid}" "${d}/tree-peak-mb.txt" 0.4 &
  samp_pid=$!

  wait "${run_pid}"; rc=$?
  wait "${samp_pid}" 2>/dev/null || true

  wall=$(awk -F': ' '/Elapsed \(wall clock\)/{print $2}' "${d}/canon-time.txt" 2>/dev/null || true)
  parent_mb=$(awk '/Maximum resident set size/{printf "%.1f", $NF/1024}' "${d}/canon-time.txt" 2>/dev/null || true)
  tree_mb=$(cat "${d}/tree-peak-mb.txt" 2>/dev/null || echo "")
  if [ "${rc}" -ne 0 ]; then
    echo "  FAILED rc=${rc} (see ${d}/canon-stdout.txt)"
  fi
  echo "  wall=${wall:-?}  parent_peak=${parent_mb:-?} MiB  tree_peak=${tree_mb:-?} MiB  rc=${rc}"
done

echo
echo "Canonical runs complete. Raw outputs under ${OUT}/<fixture>/."
