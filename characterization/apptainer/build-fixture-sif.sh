#!/bin/bash
# build-fixture-sif.sh — wrapper around `apptainer build` that
# bootstraps moves-fixture.sif from canonical-moves.sif, applies the
# intermediate-state-capture patch, and writes the resulting SIF
# SHA256 to characterization/fixture-image.lock.
#
# Required inputs (resolved from PARENT_SIF or auto-detected):
#   PARENT_SIF       Path to canonical-moves.sif. Default:
#                    ./canonical-moves.sif next to this script.
#
# Optional:
#   OUTPUT           Output SIF path (default: ./moves-fixture.sif).
#   FAKEROOT         1 = use --fakeroot, 0 = use setuid mode (default
#                    auto-detected).
#
# Why two SIFs (canonical + fixture):
# canonical-moves.sif is the immutable reference image — its SHA256
# pins the entire migration's behavioral baseline. Patching it would
# rewrite that identity. Instead we layer a separate fixture SIF that
# bootstraps from canonical, applies the three Phase 0 Task 3 flag
# flips, and recompiles MOVES on top. Both SIFs reference the same
# MOVES_COMMIT and movesdb20241112 — they only differ in the three
# patched fields.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/.." && pwd)"   # characterization/

PARENT_SIF="${PARENT_SIF:-${HERE}/canonical-moves.sif}"
OUTPUT="${OUTPUT:-${HERE}/moves-fixture.sif}"
LOCKFILE="${ROOT}/fixture-image.lock"
PARENT_LOCKFILE="${ROOT}/canonical-image.lock"
PATCH_FILE="${HERE}/files/intermediate-state-capture.patch"

# ----- Validate -----
if [ ! -f "${PARENT_SIF}" ]; then
    echo "FATAL: parent SIF ${PARENT_SIF} not found." >&2
    echo "  Build canonical-moves.sif first via build-sif.sh, or set" >&2
    echo "  PARENT_SIF=<path> to point at an existing one." >&2
    exit 2
fi

if [ ! -f "${PATCH_FILE}" ]; then
    echo "FATAL: patch file ${PATCH_FILE} not found." >&2
    exit 2
fi

command -v apptainer >/dev/null 2>&1 || {
    echo "FATAL: apptainer not found in PATH." >&2
    exit 2
}

# ----- Detect fakeroot -----
if [ -z "${FAKEROOT:-}" ]; then
    if apptainer config global --get 'allow setuid' 2>/dev/null | grep -qi yes; then
        FAKEROOT=0
    else
        FAKEROOT=1
    fi
fi
FAKEROOT_FLAG=()
[ "${FAKEROOT}" = "1" ] && FAKEROOT_FLAG=( --fakeroot )

# ----- Read parent SHA from lockfile if present -----
# Embedded in fixture-image.lock as provenance; doesn't gate the build.
PARENT_SHA="(unrecorded — canonical-image.lock missing or placeholder)"
if [ -f "${PARENT_LOCKFILE}" ]; then
    PARENT_SHA_LINE=$(grep -E '^sif_sha256\s*=' "${PARENT_LOCKFILE}" || true)
    if [ -n "${PARENT_SHA_LINE}" ]; then
        PARENT_SHA=$(echo "${PARENT_SHA_LINE}" | sed -E 's/.*=\s*"?([^"]*)"?.*/\1/')
    fi
fi

# ----- Build a temp context -----
# %files relative paths in the def are resolved against apptainer's
# working directory; cd to the build context before invoking. The
# parent SIF must be reachable as ./canonical-moves.sif (the From:
# localimage line) — symlink it into the context to avoid copying
# multi-GB.
CTX="$(mktemp -d)"
cleanup() { rm -rf "${CTX}"; }
trap cleanup EXIT

cp "${HERE}/moves-fixture.def" "${CTX}/moves-fixture.def"
mkdir -p "${CTX}/files"
cp "${PATCH_FILE}" "${CTX}/files/intermediate-state-capture.patch"
ln -s "${PARENT_SIF}" "${CTX}/canonical-moves.sif"

echo "[build-fixture-sif] Building ${OUTPUT}"
echo "[build-fixture-sif]   PARENT_SIF    = ${PARENT_SIF}"
echo "[build-fixture-sif]   PARENT_SHA256 = ${PARENT_SHA}"
echo "[build-fixture-sif]   FAKEROOT      = ${FAKEROOT}"

( cd "${CTX}" && apptainer build \
    "${FAKEROOT_FLAG[@]}" \
    --force \
    "${OUTPUT}" \
    "${CTX}/moves-fixture.def" )

# ----- Compute and record the hash -----
SIF_SHA256="$(sha256sum "${OUTPUT}" | awk '{print $1}')"
SIF_BYTES="$(stat -c '%s' "${OUTPUT}")"
PATCH_SHA256="$(sha256sum "${PATCH_FILE}" | awk '{print $1}')"
BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
BUILD_HOST="$(hostname)"
APPTAINER_VERSION="$(apptainer --version | awk '{print $NF}')"

cat > "${LOCKFILE}" <<EOF
# fixture-image.lock — pinned identity of moves-fixture.sif.
#
# This SIF is canonical-moves.sif + the three intermediate-state
# capture patches (Phase 0 Task 3). It is used by mo-kbjl and later
# fixture-capture beads. Any rebuild that produces a different hash
# invalidates downstream characterization snapshots.
#
# Regenerate by running characterization/apptainer/build-fixture-sif.sh
# (which itself requires canonical-moves.sif from build-sif.sh).

sif_path           = "characterization/apptainer/moves-fixture.sif"
sif_sha256         = "${SIF_SHA256}"
sif_bytes          = ${SIF_BYTES}

# Parent SIF (the canonical reference this layer was built from)
parent_sif_path    = "characterization/apptainer/canonical-moves.sif"
parent_sif_sha256  = "${PARENT_SHA}"

# Patch applied
patch_path         = "characterization/apptainer/files/intermediate-state-capture.patch"
patch_sha256       = "${PATCH_SHA256}"

# Build provenance
built_at_utc       = "${BUILD_DATE}"
built_on_host      = "${BUILD_HOST}"
apptainer_version  = "${APPTAINER_VERSION}"
EOF

echo
echo "[build-fixture-sif] Wrote ${LOCKFILE}"
echo "[build-fixture-sif] SHA256: ${SIF_SHA256}"
echo "[build-fixture-sif] Bytes:  ${SIF_BYTES}"
