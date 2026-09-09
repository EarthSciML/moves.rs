#!/bin/bash
# capture-county-snapshot.sh — Capture a canonical MOVES snapshot for a SINGLE-scale
# county fixture by pre-seeding the county input database (washtenaw_cdb) before
# invoking the standard run-fixture.sh.
#
# Usage:
#   ./capture-county-snapshot.sh --fixture NAME --county-sql FILE [options]
#
# Required:
#   --fixture NAME    Fixture name (e.g. process-crankcase-start-single).
#                     RunSpec: characterization/fixtures/<NAME>.xml.
#   --county-sql FILE Path to the SQL file that creates and populates the
#                     county input DB (washtenaw_cdb).
#
# Optional:
#   --output-dir DIR  Snapshot output (default: characterization/snapshots/<NAME>).
#   -f, --fakeroot    Forward --fakeroot to both the county-DB seeding pass and
#                     the delegated run-fixture.sh. Required on HPC rigs where
#                     the user is not in /etc/subuid (every other script here
#                     takes the same flag).
#   --sif PATH        moves-fixture.sif path.
#                     Default: /projects/illinois/eng/cee/ctessum/ctessum/code/
#                              moves.rs/characterization/apptainer/moves-fixture.sif
#   -h, --help

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"

FIXTURE_NAME=""
COUNTY_SQL=""
OUTPUT_DIR=""
USE_FAKEROOT=0
SIF="/projects/illinois/eng/cee/ctessum/ctessum/code/moves.rs/characterization/apptainer/moves-fixture.sif"

while [ $# -gt 0 ]; do
    case "$1" in
        --fixture)    FIXTURE_NAME="$2"; shift 2 ;;
        --county-sql) COUNTY_SQL="$2";   shift 2 ;;
        --output-dir) OUTPUT_DIR="$2";   shift 2 ;;
        --sif)        SIF="$2";          shift 2 ;;
        -f|--fakeroot) USE_FAKEROOT=1;   shift 1 ;;
        -h|--help)    sed -n '2,/^set -euo/p' "$0" | sed 's/^# \?//'; exit 0 ;;
        *) printf 'Unknown argument: %s\n' "$1" >&2; exit 2 ;;
    esac
done

# Every other HPC invocation in this directory runs under --fakeroot; this
# script was the only one that could not, so its seeding pass ran as the
# calling user while run-fixture.sh's MOVES pass ran under a different
# uid mapping. Forwarded to both the seeding `apptainer exec` below and the
# delegated run-fixture.sh so the county DB seeded in step 1 is readable by
# the MOVES run in step 2.
FAKEROOT_EXEC=()
FAKEROOT_RUNFIXTURE=()
if [ "${USE_FAKEROOT}" = "1" ]; then
    FAKEROOT_EXEC=( --fakeroot )
    FAKEROOT_RUNFIXTURE=( --fakeroot )
fi

[[ -z "${FIXTURE_NAME}" ]] && { echo "FATAL: --fixture required" >&2; exit 2; }
[[ -z "${COUNTY_SQL}" ]]   && { echo "FATAL: --county-sql required" >&2; exit 2; }
[[ ! -f "${COUNTY_SQL}" ]] && { echo "FATAL: county SQL not found: ${COUNTY_SQL}" >&2; exit 2; }
[[ ! -f "${SIF}" ]]        && { echo "FATAL: SIF not found: ${SIF}" >&2; exit 2; }

RUNSPEC="${ROOT}/characterization/fixtures/${FIXTURE_NAME}.xml"
[[ ! -f "${RUNSPEC}" ]] && { echo "FATAL: RunSpec not found: ${RUNSPEC}" >&2; exit 2; }

OUTPUT_DIR="${OUTPUT_DIR:-${ROOT}/characterization/snapshots/${FIXTURE_NAME}}"
COUNTY_DB="washtenaw_cdb"

# ----- Create a temporary RunSpec with the county DB name filled in -----
WORKDIR="/scratch/${USER}/moves-county-fixture/${FIXTURE_NAME}"
mkdir -p "${WORKDIR}"
TEMP_RUNSPEC="${WORKDIR}/${FIXTURE_NAME}.xml"
sed "s|scaleinputdatabase servername=\"\" databasename=\"\" description=\"\"|scaleinputdatabase servername=\"localhost\" databasename=\"${COUNTY_DB}\" description=\"Washtenaw County CDB\"|g" \
    "${RUNSPEC}" > "${TEMP_RUNSPEC}"

echo "[county-capture] fixture    = ${FIXTURE_NAME}"
echo "[county-capture] county-sql = ${COUNTY_SQL}"
echo "[county-capture] output-dir = ${OUTPUT_DIR}"
echo "[county-capture] workdir    = ${WORKDIR}"

# ----- Step 1: Seed MariaDB and inject county DB -----
# run-moves.sh always calls init-mariadb.sh, which only seeds if the dir is
# empty. By pre-seeding AND adding the county DB here, and then calling
# run-fixture.sh with KEEP_MARIADB_DATA=1, the county DB survives into the
# actual MOVES run (run-fixture.sh won't wipe it).
MARIADB_DATA="${WORKDIR}/mariadb-data"
MARIADB_SOCK_DIR="${WORKDIR}/run-mysqld"
rm -rf "${MARIADB_DATA}"
mkdir -p "${MARIADB_DATA}" "${MARIADB_SOCK_DIR}"

echo "[county-capture] step 1/2 — seeding MariaDB and creating county DB"

# Copy the SQL to the workdir so it has a clean path inside the container
COUNTY_SQL_IN_WORKDIR="${WORKDIR}/county-setup.sql"
cp "${COUNTY_SQL}" "${COUNTY_SQL_IN_WORKDIR}"

# Use --writable-tmpfs (same as run-moves.sh) so the container can write to
# its root filesystem. Bind the same paths run-moves.sh uses.
apptainer exec \
    "${FAKEROOT_EXEC[@]}" \
    --writable-tmpfs \
    --bind "${MARIADB_DATA}:/var/lib/mysql" \
    --bind "${MARIADB_SOCK_DIR}:/var/run/mysqld" \
    --bind "${HERE}/files/start-mariadb-bg.sh:/opt/moves-bin/start-mariadb-bg.sh:ro" \
    --bind "${WORKDIR}:/opt/county-setup:ro" \
    "${SIF}" bash -c "
        set -euo pipefail
        echo '[step1] Seeding MariaDB data directory...'
        /opt/moves-bin/init-mariadb.sh
        echo '[step1] Starting MariaDB...'
        /opt/moves-bin/start-mariadb-bg.sh
        echo '[step1] Creating county DB: ${COUNTY_DB}'
        mariadb -umoves -pmoves -e 'CREATE DATABASE IF NOT EXISTS ${COUNTY_DB};'
        echo '[step1] Loading county SQL...'
        mariadb -umoves -pmoves '${COUNTY_DB}' < '/opt/county-setup/county-setup.sql'
        echo '[step1] County DB ready.'
        mariadb-admin -umoves -pmoves shutdown 2>/dev/null || true
    "
echo "[county-capture] MariaDB seeded with county DB."
sleep 2

# ----- Step 2: Run MOVES + dump + snapshot (preserving the county DB) -----
echo "[county-capture] step 2/2 — running canonical MOVES with county DB"
KEEP_MARIADB_DATA=1 SIF="${SIF}" \
    "${HERE}/run-fixture.sh" \
    "${FAKEROOT_RUNFIXTURE[@]}" \
    --sif "${SIF}" \
    --runspec "${TEMP_RUNSPEC}" \
    --workdir "${WORKDIR}" \
    --output-dir "${OUTPUT_DIR}"

echo "[county-capture] Done. Snapshot at: ${OUTPUT_DIR}"
