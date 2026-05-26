#!/bin/bash
# SLURM-build.sbatch — batch script for the canonical-moves SIF build.
#
# Allocation: 8 cores, 16 GB RAM, 6-hour wall time.
# Targets the `ctessum` private partition (ccc0232-0236), which has a
# 3-day wall-time limit and is not shared with other users.
# Falls back to `secondary` if those nodes are fully occupied.
#
# Verified host baseline (2026-05-22, ccc0234):
#   apptainer version 1.4.5-3.el9        >= 1.4 ✓
#   allow setuid = yes                             ✓
#   /tmp available: ~100 GB                >= 40 GB ✓
#   /scratch.local available: ~463 GB              ✓
#   CPUs: 40 (20 used)   RAM: 188 GB              ✓
#
# Usage:
#   cd characterization/apptainer
#   sbatch SLURM-build.sbatch
#
# Optionally pre-stage the movesdb ZIP on /scratch first (mo-k20au):
#   MOVESDB_LOCAL_PATH=/scratch/${USER}/movesdb20241112.zip ./build-sif.sh
# The sbatch script passes that variable through automatically if set.

#SBATCH --job-name=canonical-moves-sif
#SBATCH --partition=ctessum
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=6:00:00
#SBATCH --output=slurm-build-%j.out
#SBATCH --error=slurm-build-%j.err

set -euo pipefail

# Use /scratch.local for Apptainer's build tmp — it is node-local (fast I/O,
# ~460 GB free) and avoids stressing the Lustre filesystem during the build.
# APPTAINER_TMPDIR must be writable and hold ~30-40 GB for the sandbox.
export APPTAINER_TMPDIR="/scratch.local/${USER}/apptainer-tmp"
mkdir -p "${APPTAINER_TMPDIR}"

# Point at the pre-staged movesdb ZIP if it exists (set by mo-k20au).
# If not present, build-sif.sh falls back to the in-MOVES-tree zip or
# MOVESDB_URL — see characterization/apptainer/README.md.
MOVESDB_STAGED="/scratch/${USER}/movesdb20241112.zip"
if [[ -f "${MOVESDB_STAGED}" ]]; then
    export MOVESDB_LOCAL_PATH="${MOVESDB_STAGED}"
fi

# Resolve the repo root. SLURM copies the batch script to a tmp path, so
# BASH_SOURCE[0] is unreliable; use SLURM_SUBMIT_DIR (the directory where
# sbatch was invoked, which must be characterization/apptainer/).
SCRIPT_DIR="${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"

echo "[SLURM-build] Host:            $(hostname)"
echo "[SLURM-build] APPTAINER_TMPDIR: ${APPTAINER_TMPDIR}"
echo "[SLURM-build] MOVESDB_LOCAL_PATH: ${MOVESDB_LOCAL_PATH:-<not set, using in-tree zip>}"
echo "[SLURM-build] Apptainer:        $(apptainer --version)"
echo "[SLURM-build] allow setuid:     $(apptainer config global --get 'allow setuid')"
echo "[SLURM-build] /tmp free:        $(df -h /tmp | awk 'NR==2{print $4}')"
echo "[SLURM-build] /scratch.local free: $(df -h /scratch.local | awk 'NR==2{print $4}')"
echo ""

cd "${SCRIPT_DIR}"
./build-sif.sh

echo "[SLURM-build] Done. SIF written and SHA256 recorded in ../canonical-image.lock."
