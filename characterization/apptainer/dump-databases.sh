#!/bin/bash
# dump-databases.sh — runs INSIDE the SIF (bind-mounted by run-fixture.sh).
#
# Iterates every non-system MariaDB database, dumps each table to TSV at
# /captures/databases/<db>/<table>.tsv, and writes a column metadata
# sidecar at /captures/databases/<db>/<table>.schema.tsv.
#
# Determinism guarantees:
#   * Databases listed via INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME.
#   * Tables listed via INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME.
#   * Column list taken from INFORMATION_SCHEMA.COLUMNS ORDER BY
#     ORDINAL_POSITION → both the schema sidecar order and the SELECT
#     column order are deterministic.
#   * Row order forced by SELECT ... ORDER BY 1, 2, ..., N.
#
# Environment:
#   START_MARIADB        Command run to start MariaDB (e.g.
#                        "service mariadb start" with --fakeroot, or
#                        "/opt/moves-bin/start-mariadb-bg.sh" otherwise).
#                        Default: /opt/moves-bin/start-mariadb-bg.sh.
#   CAPTURES_DIR         Directory to write into (default: /captures).
#
# NULL handling: mariadb -B prints SQL NULL as the literal string "NULL".
# The Rust capture step reads this as Value::Null. A varchar value of the
# literal "NULL" would be ambiguous; MOVES output schemas don't use such
# values, so this is documented as a known caveat.

set -euo pipefail

START_MARIADB="${START_MARIADB:-/opt/moves-bin/start-mariadb-bg.sh}"
CAPTURES_DIR="${CAPTURES_DIR:-/captures}"

mkdir -p "${CAPTURES_DIR}/databases"

# Start MariaDB.
${START_MARIADB}

# Wait for the socket if start returned without blocking.
for _ in $(seq 1 60); do
    [ -S /var/run/mysqld/mysqld.sock ] && break
    sleep 1
done

mq() {
    # Quiet, batch-mode, no-headers query against the running server.
    mariadb -B -N -uroot "$@"
}

# Discover non-system databases.
mq -e "
    SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA
    WHERE SCHEMA_NAME NOT IN (
        'information_schema',
        'mysql',
        'performance_schema',
        'sys'
    )
    ORDER BY SCHEMA_NAME
" | while IFS= read -r DB; do
    [ -n "$DB" ] || continue

    echo "[dump-databases] dumping ${DB}"
    mkdir -p "${CAPTURES_DIR}/databases/${DB}"

    # Discover tables in this database (BASE TABLE excludes views).
    TABLES=$(mq -e "
        SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '${DB}' AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
    ")

    for T in $TABLES; do
        [ -n "$T" ] || continue

        # Schema sidecar: NAME\tDATA_TYPE\tCOLUMN_KEY (ordinal order).
        mq -e "
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                IFNULL(COLUMN_KEY, '')
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '${DB}' AND TABLE_NAME = '${T}'
            ORDER BY ORDINAL_POSITION
        " > "${CAPTURES_DIR}/databases/${DB}/${T}.schema.tsv"

        # Build the SELECT column list (back-quoted) and ORDER BY clause.
        COLS=$(mq -e "
            SELECT GROUP_CONCAT(
                CONCAT('\`', COLUMN_NAME, '\`')
                ORDER BY ORDINAL_POSITION
                SEPARATOR ', '
            )
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '${DB}' AND TABLE_NAME = '${T}'
        ")
        N_COLS=$(mq -e "
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '${DB}' AND TABLE_NAME = '${T}'
        ")

        if [ "${N_COLS}" -eq 0 ]; then
            : > "${CAPTURES_DIR}/databases/${DB}/${T}.tsv"
            continue
        fi

        ORDER_BY=$(seq -s "," 1 "${N_COLS}")

        mq -e "SELECT ${COLS} FROM \`${DB}\`.\`${T}\` ORDER BY ${ORDER_BY}" \
            > "${CAPTURES_DIR}/databases/${DB}/${T}.tsv"
    done
done

# Stop MariaDB cleanly.
mariadb-admin --socket=/var/run/mysqld/mysqld.sock -uroot shutdown 2>/dev/null \
    || kill "$(cat /var/run/mysqld/mariadbd.pid 2>/dev/null)" 2>/dev/null \
    || true
