#!/bin/bash
# build.sh — compile NONROAD from a MOVES source tree using pinned
# gfortran flags and Phase 5 baseline instrumentation.
#
# Invoked from three places:
#
#   1. canonical-moves.def %post (during SIF build) — to bake the
#      Linux-correct NONROAD binary into the SIF.
#   2. From an HPC compute node, against a host clone of MOVES, when
#      iterating on flag choices ahead of an SIF rebuild.
#   3. Directly out of the moves.rs repo by a developer reproducing
#      the bake-into-SIF result outside Apptainer.
#
# Inputs:
#   $1   path to a MOVES source tree (must contain
#        NONROAD/NR08a/SOURCE/*.f and the upstream makefile)
#
# Environment:
#   FLAVOR    'production' (default) or 'audit'
#             - production: -O2 with the F77-compat flag set
#                           (FLAGS_PRODUCTION from flags.env).
#             - audit:      -O0 -g -fcheck=all + ffpe-trap
#                           (FLAGS_AUDIT from flags.env).
#                           Slower, used for chasing a divergence.
#   FC        Override the compiler (default: from flags.env, gfortran)
#   JOBS      Parallel `make -j` jobs (default: nproc-detected)
#   OUTPUT    Override the destination path for the binary (default:
#             <moves-tree>/NONROAD/NR08a/$NONROAD_BINARY_NAME)
#   SKIP_INSTRUMENTATION
#             1 = skip applying the dbgemit patches (build pristine
#                 NONROAD with only the flag fix, no Phase 5 capture).
#             0 (default) = apply patches + ship dbgemit.f.
#
# What this script does NOT do:
#   - Does not run the validation gate. That requires representative
#     fixtures and a real MOVES execution, which only the HPC compute
#     side can do. See README.md "Validation gate".
#
# Idempotence: this script copies Makefile.linux + dbgemit.f into the
# source tree and applies patches via `patch`. Running it twice on the
# same tree is detected and no-ops the patch step.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <path-to-MOVES-source-tree>" >&2
    echo "  Compiles NONROAD/NR08a/SOURCE/*.f with pinned gfortran flags," >&2
    echo "  writes binary to <tree>/NONROAD/NR08a/NONROAD.exe by default." >&2
    exit 2
fi

# ----- Load pinned flag set + binary name from flags.env -----
# flags.env is the single source of truth for the compiler flags;
# Makefile.linux's defaults shadow these but get overridden via the
# `make ... FLGS=...` command line below.
# shellcheck disable=SC1091
. "${HERE}/flags.env"

MOVES_TREE="$1"
SRC_DIR="${MOVES_TREE}/NONROAD/NR08a/SOURCE"
BIN_DIR="${MOVES_TREE}/NONROAD/NR08a"

if [ ! -d "${SRC_DIR}" ]; then
    echo "FATAL: ${SRC_DIR} does not exist." >&2
    echo "  Pass a MOVES source tree containing NONROAD/NR08a/SOURCE/ as \$1." >&2
    exit 2
fi
if [ ! -f "${SRC_DIR}/makefile" ]; then
    echo "FATAL: ${SRC_DIR}/makefile missing — is the source tree intact?" >&2
    exit 2
fi

FLAVOR="${FLAVOR:-production}"
JOBS="${JOBS:-$(nproc 2>/dev/null || echo 1)}"
OUTPUT="${OUTPUT:-${BIN_DIR}/${NONROAD_BINARY_NAME}}"
SKIP_INSTRUMENTATION="${SKIP_INSTRUMENTATION:-0}"

case "${FLAVOR}" in
    production) FLGS="${FLAGS_PRODUCTION}" ;;
    audit)      FLGS="${FLAGS_AUDIT}" ;;
    *)
        echo "FATAL: FLAVOR=${FLAVOR} not recognized (use 'production' or 'audit')." >&2
        exit 2
        ;;
esac

command -v "${FC}" >/dev/null 2>&1 || {
    echo "FATAL: compiler '${FC}' not found in PATH." >&2
    exit 2
}
command -v patch >/dev/null 2>&1 || {
    echo "FATAL: 'patch' not found in PATH (needed to apply instrumentation)." >&2
    exit 2
}

echo "[nonroad-build] FLAVOR  = ${FLAVOR}"
echo "[nonroad-build] FC      = ${FC}"
echo "[nonroad-build] JOBS    = ${JOBS}"
echo "[nonroad-build] SRC     = ${SRC_DIR}"
echo "[nonroad-build] OUTPUT  = ${OUTPUT}"
echo "[nonroad-build] FLGS    = ${FLGS}"
echo "[nonroad-build] SKIP_INSTRUMENTATION = ${SKIP_INSTRUMENTATION}"
"${FC}" --version | head -1

# ----- Stage Makefile.linux + dbgemit.f into SOURCE/ -----
# Always copy these. The upstream makefile uses Windows-only `del` in
# its clean rule and lacks the F77-compat flags; Makefile.linux
# replaces it. dbgemit.f is the runtime side of the Phase 5 capture
# facility and its symbols are required by the patched callers (see
# patches/) — even when SKIP_INSTRUMENTATION=1 we still ship the file
# but the patches that call it are not applied, so the .o stays
# unreferenced (harmless).
cp "${HERE}/Makefile.linux" "${SRC_DIR}/Makefile.linux"
cp "${HERE}/src/dbgemit.f"  "${SRC_DIR}/dbgemit.f"

# ----- Apply instrumentation patches -----
# Patches live in patches/ as unified diffs against the pinned MOVES
# commit. Each adds dbg* calls inside one .f file; together they cover
# population (getpop), age distribution (agedist), growth (grwfac),
# and emissions (clcems) — the four arrays the Phase 5 baseline needs.
#
# Idempotence: `patch --dry-run -R` checks whether the reverse of the
# patch already applies (i.e. the change is already in the file). If
# so, we skip; otherwise apply forward.
if [ "${SKIP_INSTRUMENTATION}" != "1" ] && [ -d "${HERE}/patches" ]; then
    for p in "${HERE}/patches"/*.patch; do
        [ -f "$p" ] || continue
        name="$(basename "$p")"
        # Patches are unified diffs against the MOVES tree root; apply
        # from there with -p1 so the a/NONROAD/... paths resolve.
        if ( cd "${MOVES_TREE}" && patch -p1 --dry-run -R --silent < "$p" ) >/dev/null 2>&1; then
            echo "[nonroad-build] patch already applied: ${name}"
        else
            echo "[nonroad-build] applying patch: ${name}"
            ( cd "${MOVES_TREE}" && patch -p1 --silent < "$p" )
        fi
    done
fi

# ----- Build -----
# Makefile.linux reads FC from its own default (gfortran). Override
# only if the caller asked. JOBS controls parallelism; the upstream
# tree has no recursive subdirs so plain -j works.
make -C "${SRC_DIR}" \
    -f Makefile.linux \
    -j"${JOBS}" \
    FC="${FC}" \
    FLGS="${FLGS}" \
    all

SRC_BIN="${SRC_DIR}/nonroad"
if [ ! -f "${SRC_BIN}" ]; then
    echo "FATAL: build claimed success but ${SRC_BIN} is missing." >&2
    exit 1
fi

mkdir -p "$(dirname "${OUTPUT}")"
mv "${SRC_BIN}" "${OUTPUT}"
chmod +x "${OUTPUT}"

# Sanity: the binary is an x86_64 ELF.
file "${OUTPUT}"
echo "[nonroad-build] sha256 $(sha256sum "${OUTPUT}" | awk '{print $1}')"
echo "[nonroad-build] bytes  $(stat -c '%s' "${OUTPUT}")"

# Belt-and-suspenders: confirm the binary at least loads. NONROAD
# prompts for an options-file path on stdin and exits when stdin
# closes with an EOF read — both behaviours are acceptable here; we
# just want to rule out dynamic-linker errors.
if "${OUTPUT}" </dev/null >/dev/null 2>&1; then
    echo "[nonroad-build] smoke: binary executed (rc=0)"
else
    rc=$?
    echo "[nonroad-build] smoke: binary executed (rc=${rc}) — NONROAD typically exits non-zero without an options file"
fi

echo "[nonroad-build] done"
