#!/bin/bash
# dump-default-db.sh — runs INSIDE the canonical-moves SIF, bind-mounted by
# `convert-default-db.sh`.
#
# Starts MariaDB from the SIF's pre-seeded data dir, dumps every BASE TABLE
# in the default DB (default: `movesdb20241112`) to `<captures>/<table>.tsv`
# + `<captures>/<table>.schema.tsv`, computes a manifest sidecar with row
# counts, and exits. Re-runnable: rewrites the same directory on each call.
#
# Determinism guarantees:
#   * Tables listed via INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME.
#   * Column list taken from INFORMATION_SCHEMA.COLUMNS ORDER BY
#     ORDINAL_POSITION → schema-sidecar order matches the SELECT order.
#   * Row order forced by SELECT ... ORDER BY 1, 2, ..., N.
#
# Differs from `characterization/apptainer/dump-databases.sh` (used by the
# fixture-capture orchestrator) by:
#   * Targeting a single named database (the MOVES default DB), not all
#     non-system schemas.
#   * Writing a `dump-manifest.json` sidecar with per-table row counts so
#     the Rust converter can cross-check without re-counting the TSVs.
#   * Capturing the source SQL dump hash if provided.
#
# Environment:
#   START_MARIADB   Command run to start MariaDB. Default:
#                   /opt/moves-bin/start-mariadb-bg.sh.
#   DEFAULT_DB      MariaDB schema name to dump. Default:
#                   movesdb20241112.
#   CAPTURES_DIR    Output directory. Default: /captures.
#   SOURCE_DUMP_SHA Optional SHA-256 of the MariaDB dump file; recorded in
#                   the dump manifest. Empty → omitted.
#
# NULL handling: mariadb -B prints SQL NULL as the literal string "NULL".
# The Rust converter reads this as Arrow null. A varchar value of the
# literal "NULL" would be ambiguous; MOVES default-DB columns don't carry
# such values, so this is documented as a known caveat.

set -euo pipefail

START_MARIADB="${START_MARIADB:-/opt/moves-bin/start-mariadb-bg.sh}"
DEFAULT_DB="${DEFAULT_DB:-movesdb20241112}"
CAPTURES_DIR="${CAPTURES_DIR:-/captures}"
SOURCE_DUMP_SHA="${SOURCE_DUMP_SHA:-}"

mkdir -p "${CAPTURES_DIR}"

${START_MARIADB}

for _ in $(seq 1 60); do
    [ -S /var/run/mysqld/mysqld.sock ] && break
    sleep 1
done

mq() {
    mariadb -B -N -uroot "$@"
}

# Verify the target database exists.
EXISTS=$(mq -e "
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME='${DEFAULT_DB}'
")
if [ "${EXISTS}" -eq 0 ]; then
    echo "[dump-default-db] database '${DEFAULT_DB}' not found in MariaDB" >&2
    mariadb-admin --socket=/var/run/mysqld/mysqld.sock -uroot shutdown 2>/dev/null \
        || kill "$(cat /var/run/mysqld/mariadbd.pid 2>/dev/null)" 2>/dev/null \
        || true
    exit 1
fi

# Discover tables in this database (BASE TABLE excludes views).
TABLES=$(mq -e "
    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '${DEFAULT_DB}' AND TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
")

MOVES_VERSION=$(mq -e "
    SELECT IFNULL(MAX(VERSION_DATE), '')
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '${DEFAULT_DB}' AND TABLE_NAME = 'movesVersion'
" 2>/dev/null || echo "")

# Write a header for the manifest sidecar.
MANIFEST="${CAPTURES_DIR}/dump-manifest.json"
{
    echo "{"
    echo "  \"schema_version\": \"moves-default-db-dump/v1\","
    echo "  \"database\": \"${DEFAULT_DB}\","
    if [ -n "${SOURCE_DUMP_SHA}" ]; then
        echo "  \"source_dump_sha256\": \"${SOURCE_DUMP_SHA}\","
    fi
    echo "  \"tables\": ["
} > "${MANIFEST}"

FIRST=1
for T in $TABLES; do
    [ -n "$T" ] || continue

    echo "[dump-default-db] dumping ${T}"

    # Schema sidecar: NAME\tDATA_TYPE\tCOLUMN_KEY (ordinal order).
    mq -e "
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            IFNULL(COLUMN_KEY, '')
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '${DEFAULT_DB}' AND TABLE_NAME = '${T}'
        ORDER BY ORDINAL_POSITION
    " > "${CAPTURES_DIR}/${T}.schema.tsv"

    COLS=$(mq -e "
        SELECT GROUP_CONCAT(
            CONCAT('\`', COLUMN_NAME, '\`')
            ORDER BY ORDINAL_POSITION
            SEPARATOR ', '
        )
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '${DEFAULT_DB}' AND TABLE_NAME = '${T}'
    ")
    N_COLS=$(mq -e "
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '${DEFAULT_DB}' AND TABLE_NAME = '${T}'
    ")

    if [ "${N_COLS}" -eq 0 ]; then
        : > "${CAPTURES_DIR}/${T}.tsv"
        ROWS=0
    else
        ORDER_BY=$(seq -s "," 1 "${N_COLS}")
        mq -e "SELECT ${COLS} FROM \`${DEFAULT_DB}\`.\`${T}\` ORDER BY ${ORDER_BY}" \
            > "${CAPTURES_DIR}/${T}.tsv"
        # MariaDB writes one row per line; rstripping is not necessary
        # because the file is sourced by ORDER BY and ends with \n per row.
        ROWS=$(wc -l < "${CAPTURES_DIR}/${T}.tsv" | tr -d ' ')
    fi

    if [ ${FIRST} -eq 0 ]; then
        echo "    ," >> "${MANIFEST}"
    fi
    FIRST=0
    cat >> "${MANIFEST}" <<EOF
    {
      "name": "${T}",
      "row_count": ${ROWS}
    }
EOF
done

{
    echo "  ],"
    echo "  \"moves_version_date\": \"${MOVES_VERSION}\""
    echo "}"
} >> "${MANIFEST}"

# Stop MariaDB cleanly.
mariadb-admin --socket=/var/run/mysqld/mysqld.sock -uroot shutdown 2>/dev/null \
    || kill "$(cat /var/run/mysqld/mariadbd.pid 2>/dev/null)" 2>/dev/null \
    || true

echo "[dump-default-db] complete. Manifest: ${MANIFEST}"
