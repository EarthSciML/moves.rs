#!/usr/bin/env python3
"""Inventory MOVES default-DB tables from the canonical DDL.

Reads ``CreateDefault.sql`` and ``CreateNRDefault.sql`` from a MOVES source
checkout pinned to a specific upstream commit, extracts every table's columns,
primary key, and secondary indexes, classifies tables by an estimated row-count
band, and proposes a Parquet partition strategy for each.

Output: ``tables.json`` (machine-readable) and the row-count + filter-column
information needed to author ``partitioning-plan.md`` by hand.

The script is byte-deterministic for a fixed input pair.
"""

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Schema parsing
# ---------------------------------------------------------------------------

# Find every ``CREATE TABLE <name> ( ... ) <trailer> ;`` block. The trailer can
# be an ENGINE clause, a CHARSET clause, a comment, or nothing — match up to
# the closing semicolon (or, when the next ``CREATE`` immediately follows
# without one, the next ``CREATE`` keyword).
TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(?P<name>\w+)`?\s*"
    r"\((?P<body>.*?)\)\s*"
    r"(?:ENGINE|TYPE|CHARSET|/\*|;)",
    re.IGNORECASE | re.DOTALL,
)

# A single column definition: identifier + type. The remainder (NOT NULL,
# DEFAULT, etc.) is preserved as a free-text tail so callers can introspect.
COL_RE = re.compile(
    r"^\s*`?(?P<name>\w+)`?\s+"
    r"(?P<type>\w+(?:\s*\([^)]*\))?)"
    r"(?P<tail>[^,]*)$",
    re.IGNORECASE,
)

KEY_RE = re.compile(
    r"^\s*(?:(?P<unique>UNIQUE\s+)?(?:KEY|INDEX)|(?P<pk>PRIMARY\s+KEY))"
    r"(?:\s+`?\w+`?)?"  # optional index name
    r"\s*\(\s*(?P<cols>[^)]+)\s*\)",
    re.IGNORECASE,
)

# Out-of-band index definitions. MOVES uses a mix of styles: most tables put
# their PK inside ``CREATE TABLE``, but a handful (SHO, SourceHours,
# EmissionRate*, etc.) define the unique index in a separate
# ``CREATE UNIQUE INDEX XPK<Table> ON <Table> (...)`` statement and add
# secondary indexes via ``ALTER TABLE <Table> ADD ( KEY (...), KEY (...) )``.
# Parse both so the schema record matches what MariaDB ends up with.
CREATE_INDEX_RE = re.compile(
    r"CREATE\s+(?P<unique>UNIQUE\s+)?INDEX\s+`?(?P<idx_name>\w+)`?\s+"
    r"ON\s+`?(?P<table>\w+)`?\s*\((?P<cols>[^)]+)\)\s*;",
    re.IGNORECASE | re.DOTALL,
)
ALTER_ADD_RE = re.compile(
    r"ALTER\s+TABLE\s+`?(?P<table>\w+)`?\s+ADD\s*\((?P<body>.*?)\)\s*;",
    re.IGNORECASE | re.DOTALL,
)


def _split_columns(body: str):
    """Split a CREATE TABLE body into top-level comma-separated definitions.

    Naive ``,`` splitting breaks on type declarations like ``decimal(7,4)`` —
    track parenthesis depth so commas inside parens stay attached to the
    surrounding column definition.
    """
    parts = []
    depth = 0
    buf = []
    for ch in body:
        if ch == "(":
            depth += 1
            buf.append(ch)
        elif ch == ")":
            depth -= 1
            buf.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    if buf:
        parts.append("".join(buf))
    return [p.strip() for p in parts if p.strip()]


def _split_index_cols(spec: str):
    """Split an index column list, stripping back-quotes, ASC/DESC, width."""
    out = []
    for raw in spec.split(","):
        col = raw.strip().strip("`")
        # ``CREATE UNIQUE INDEX ... ON t (colA ASC, colB DESC)`` is common.
        col = re.sub(r"\s+(ASC|DESC)\s*$", "", col, flags=re.IGNORECASE)
        col = re.sub(r"\s*\(\s*\d+\s*\)\s*$", "", col)  # drop ``col(8)`` prefix
        col = col.strip()
        if col:
            out.append(col)
    return out


def parse_tables(text: str):
    # MOVES runs MariaDB with ``lower_case_table_names=1`` (see the SIF
    # ``my.cnf``), so case variants of the same name collapse to one
    # physical table. Dedupe on case-folded name, keeping the first
    # definition seen in source order.
    by_name = {}
    by_lower = {}

    # Recognise inline ``PRIMARY KEY`` qualifiers on a single-column
    # definition (``fuelFormulationID int NOT NULL PRIMARY KEY``).
    inline_pk_re = re.compile(r"\bPRIMARY\s+KEY\b", re.IGNORECASE)

    # Pass 1: CREATE TABLE bodies.
    for m in TABLE_RE.finditer(text):
        name = m.group("name")
        body = m.group("body")
        if name.lower() in by_lower:
            # Duplicate case-fold — second CREATE would fail at runtime under
            # lower_case_table_names=1. Skip silently; the existing entry
            # already captured the table.
            continue
        by_lower[name.lower()] = name
        cols = []
        primary_key = []
        indexes = []
        for piece in _split_columns(body):
            key_m = KEY_RE.match(piece)
            if key_m:
                cols_in_key = _split_index_cols(key_m.group("cols"))
                if key_m.group("pk"):
                    primary_key = cols_in_key
                else:
                    indexes.append(
                        {
                            "unique": bool(key_m.group("unique")),
                            "columns": cols_in_key,
                        }
                    )
                continue
            col_m = COL_RE.match(piece)
            if col_m:
                col_name = col_m.group("name")
                tail = col_m.group("tail").strip()
                cols.append(
                    {
                        "name": col_name,
                        "type": col_m.group("type").lower(),
                        "tail": tail,
                    }
                )
                # Inline ``... PRIMARY KEY`` on a single-column declaration.
                if not primary_key and inline_pk_re.search(tail):
                    primary_key = [col_name]
        by_name[name] = {
            "name": name,
            "columns": cols,
            "primary_key": primary_key,
            "indexes": indexes,
        }

    # Pass 2: out-of-band ``CREATE UNIQUE INDEX XPK<Table>`` and similar.
    # MOVES uses ``XPK<table>`` to mark the primary key index. Treat that
    # naming convention as authoritative.
    for m in CREATE_INDEX_RE.finditer(text):
        table_lower = m.group("table").lower()
        table = by_lower.get(table_lower)
        if not table:
            continue
        cols_in_idx = _split_index_cols(m.group("cols"))
        idx_name = m.group("idx_name")
        is_pk = (
            bool(m.group("unique"))
            and idx_name.lower().startswith("xpk")
            and not by_name[table]["primary_key"]
        )
        if is_pk:
            by_name[table]["primary_key"] = cols_in_idx
        else:
            by_name[table]["indexes"].append(
                {"unique": bool(m.group("unique")), "columns": cols_in_idx}
            )

    # Pass 3: ``ALTER TABLE <T> ADD ( KEY (...), KEY (...) )``. Inside the
    # parens are comma-separated KEY / PRIMARY KEY clauses with the same
    # syntax as inside a CREATE TABLE body, so reuse the column splitter.
    for m in ALTER_ADD_RE.finditer(text):
        table_lower = m.group("table").lower()
        table = by_lower.get(table_lower)
        if not table:
            continue
        for piece in _split_columns(m.group("body")):
            key_m = KEY_RE.match(piece)
            if not key_m:
                continue
            cols_in_key = _split_index_cols(key_m.group("cols"))
            if key_m.group("pk") and not by_name[table]["primary_key"]:
                by_name[table]["primary_key"] = cols_in_key
            else:
                by_name[table]["indexes"].append(
                    {"unique": bool(key_m.group("unique")), "columns": cols_in_key}
                )

    return list(by_name.values())


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

PARTITION_KEYS = {
    "year": {"yearid"},
    "county": {"countyid", "stateid"},  # state-level partitioning groups counties
    "zone": {"zoneid"},  # MOVES "zone" maps 1:1 to county at default scale
    "model_year": {"modelyearid"},
    "month": {"monthid"},
    "hour_day": {"hourdayid", "hourid", "dayid"},
    "source_type": {"sourcetypeid"},
    "fuel_type": {"fueltypeid"},
    "pol_process": {"polprocessid"},
    "process": {"processid"},
    "pollutant": {"pollutantid"},
    "road_type": {"roadtypeid"},
    "age": {"ageid", "agegroupid"},
}

# Known MOVES dimensions (rough cardinalities at the canonical scale).
# These are the cardinality of the dimension in the default DB; the cross-
# product of every PK column is wildly pessimistic because most rate tables
# carry sparse model-year / process / source-bin coverage. The estimator
# below applies a sparsity prior for those cases.
DIMENSION_CARDINALITY = {
    "yearid": 71,         # 1990..2060
    "countyid": 3225,     # CONUS counties + territories
    "stateid": 51,        # 50 states + DC
    "zoneid": 3225,       # 1:1 with countyID at default scale
    "modelyearid": 71,    # rolling 31-year window per analysis year
    "monthid": 12,
    "hourid": 24,
    "dayid": 2,           # weekday + weekend
    "hourdayid": 48,
    "sourcetypeid": 13,
    "fueltypeid": 5,
    "fuelsubtypeid": 15,
    "regclassid": 12,
    "ageid": 31,          # 0..30
    "agegroupid": 7,
    "polprocessid": 600,
    "pollutantid": 80,
    "processid": 15,
    "roadtypeid": 5,
    "opmodeid": 35,
    "avgspeedbinid": 16,
    "linkid": 4000,       # synthetic county-zone-link, per-run
    "hpid": 30,           # nonroad HP bins
    "scc": 7000,          # nonroad SCC inventory
    "modelyeargroupid": 8,
    "beginmodelyearid": 71,
    "endmodelyearid": 71,
    "minmodelyearid": 71,
    "maxmodelyearid": 71,
    "fuelyearid": 71,
    "monthgroupid": 12,
    "sourcebinid": 5000,
    "sourcebinfueltypeid": 5,
    "fuelsupplyfueltypeid": 5,
    "engtechid": 10,
    "retrofityearid": 71,
    "retrofitid": 100,
    "linkid_run": 4000,
    "regionid": 200,
    "regioncodeid": 4,
    "surrogateid": 50,
    "surrogateyearid": 20,
    "fuelformulationid": 200,
    "fuelmodelid": 10,
    "cmpid": 100,
    "etohthreshid": 30,
    "fuelmygroupid": 20,
    "scenarioid": 50,
    "fuelparameterid": 20,
    "idleregionid": 5,
    "countytypeid": 5,
    "fleetavggroupid": 100,
    "nrequiptypeid": 90,
    "nrhprangebinid": 30,
    "hpmin": 30,
    "hpmax": 30,
    "nrhpcategory": 6,
    "tierid": 8,
    "strokes": 3,
    "inputpollutantid": 80,
    "outputpollutantid": 80,
    "m6emitterid": 3,
    "fuelregionid": 100,
    "processgroupid": 5,
    "initialhourdayid": 48,
}

# Sparsity prior: PK column subsets that are well-known to be sparse in the
# default DB. The estimator multiplies the naive cardinality product by the
# given factor when the PK contains the listed pattern.
#
# Concretely:
#   * model-year ranges (beginModelYearID / endModelYearID) only carry a
#     handful of "rule" rows per (process, source, fuel) tuple — typically
#     5–10 distinct (begin, end) tuples, not the 71*71 the product implies.
#   * polProcessID expands to ~600 in the cardinality table, but most rate
#     tables only cover the ~80–120 (pol, proc) pairs that emit running.
SPARSITY_FACTORS = [
    (("beginmodelyearid", "endmodelyearid"), 1 / 500),
    (("minmodelyearid", "maxmodelyearid"), 1 / 500),
    (("modelyeargroupid",), 1 / 5),
    (("sourcebinid",), 1 / 50),
    (("polprocessid",), 1 / 5),  # processes per pollutant are sparse
]


# Hand-corrected estimates for tables where the schema-only heuristic is
# misleading. Use sparingly — only when we have a published row count or
# a strong domain reason. Empty/run-populated tables are noted as ``0``
# because the **default-DB** ships them empty; the conversion pipeline
# (Task 80) needs to copy the schema, not row data.
ROW_COUNT_OVERRIDES = {
    # Per-run activity tables — created by the execution database, not
    # shipped in movesdb20241112. Schema exists in the default DDL because
    # MOVES uses the same connection to both. Conversion can skip data
    # entirely or partition for runtime population.
    "SHO": 0,
    "SourceHours": 0,
    "Starts": 0,
    "ExtendedIdleHours": 0,
    "HotellingHours": 0,
    "AverageSpeed": 0,
    "AverageGrade": 0,
    "MOVESActivityOutput": 0,
    "MOVESOutput": 0,
    "MOVESRun": 0,
    # Geographic dimension — physical county count is ~3225 + DC.
    "County": 3225,
    "CountyType": 5,
    "Zone": 3225,
    "Year": 71,
    "Link": 0,            # populated per-run
    "Region": 200,
    "FuelType": 5,
    "FuelSubtype": 15,
    "SourceUseType": 13,
    "EmissionProcess": 15,
    "Pollutant": 80,
    "PollutantProcessAssoc": 600,
    "OperatingMode": 35,
    "RoadType": 5,
    "AgeCategory": 31,
    "AgeGroup": 7,
    "DayOfAnyWeek": 2,
    "HourOfAnyDay": 24,
    "HourDay": 48,
    "MonthOfAnyYear": 12,
}


# Row-count bands. ``size_bucket`` drives the partitioning decision.
SIZE_BUCKETS = [
    ("empty", 1),               # populated at run time, ships empty
    ("tiny", 100),              # < 100 rows
    ("small", 10_000),          # < 10k rows — monolithic threshold from plan
    ("medium", 1_000_000),      # < 1M rows
    ("large", 50_000_000),      # < 50M rows
    ("huge", float("inf")),
]


def _pk_set(table):
    return {c.lower() for c in table["primary_key"]}


def _apply_sparsity(rows, pk_lower):
    factor = 1.0
    for pattern, mult in SPARSITY_FACTORS:
        if all(p in pk_lower for p in pattern):
            factor *= mult
    return max(rows * factor, 0)


def _estimate_row_count(table):
    """Estimate the row count for a default-DB table.

    Priority order:
    1. Hand-corrected override (for tables where we know the truth).
    2. Product of PK cardinalities, attenuated by the sparsity prior.
    3. ``None`` when no PK is recorded — caller treats as "small/unknown".
    """
    if table["name"] in ROW_COUNT_OVERRIDES:
        return ROW_COUNT_OVERRIDES[table["name"]]
    if not table["primary_key"]:
        return None
    rows = 1
    for col in table["primary_key"]:
        rows *= DIMENSION_CARDINALITY.get(col.lower(), 50)
    return int(_apply_sparsity(rows, _pk_set(table)))


def _size_bucket(rows):
    if rows is None:
        return "unknown"
    if rows == 0:
        return "empty"
    for bucket, limit in SIZE_BUCKETS:
        if rows < limit:
            return bucket
    return "huge"


def _column_set(table):
    return {c["name"].lower() for c in table["columns"]}


def _classify_partition(table):
    """Decide partition strategy from the column composition and size bucket."""
    rows = _estimate_row_count(table)
    bucket = _size_bucket(rows)
    cols = _column_set(table)

    has_year = "yearid" in cols
    has_county = bool(cols & {"countyid", "stateid"})
    has_zone = "zoneid" in cols
    has_modelyear = "modelyearid" in cols

    # MOVES uses zoneID and countyID interchangeably at default scale.
    geo = has_county or has_zone

    # Empty / run-populated tables: schema only, no data partitioning required.
    if bucket == "empty":
        return {
            "strategy": "schema_only",
            "rationale": "ships empty in default DB; populated at run time",
        }

    # Tables small enough to stay monolithic regardless of partition columns.
    # The migration plan's threshold is "10k rows stays monolithic". We
    # widen it to 1M because a 1M-row Parquet file is still cheap to scan
    # and predicate pushdown over column statistics already prunes most
    # of it on read.
    if bucket in ("tiny", "small", "medium", "unknown"):
        return {
            "strategy": "monolithic",
            "rationale": f"{bucket} band — single Parquet file is cheapest",
        }

    # No partition columns -> monolithic regardless of size.
    if not (has_year or has_modelyear or geo):
        return {
            "strategy": "monolithic",
            "rationale": "no temporal or geographic partition columns; load whole",
        }

    # Large, both year and geo -> partition by both.
    if bucket in ("large", "huge") and has_year and geo:
        return {
            "strategy": "year_x_county",
            "rationale": f"{bucket} band with both year and county/zone columns",
        }

    # Year-only large -> partition by year.
    if has_year and bucket in ("large", "huge"):
        return {
            "strategy": "year",
            "rationale": f"{bucket} band with yearID; partition by year",
        }

    # County-only large -> partition by county/zone.
    if geo and bucket in ("large", "huge"):
        return {
            "strategy": "county",
            "rationale": f"{bucket} band with county/zone but no year",
        }

    # Model-year dominated rate table -> shard by modelYear bucket.
    if has_modelyear and bucket in ("large", "huge"):
        return {
            "strategy": "model_year",
            "rationale": f"{bucket} band keyed by modelYearID",
        }

    return {"strategy": "monolithic", "rationale": "fallback"}


def _filter_columns(table):
    """Columns most-frequently-filtered-on, ordered by likelihood.

    With the Phase 1 coverage map empty, fall back to the schema-encoded
    proxy: every column that participates in the primary key or in a
    secondary index. Index designers wrote those because joins / WHERE
    clauses target them.
    """
    seen = []

    def _push(cols):
        for c in cols:
            cl = c.lower()
            if cl not in seen:
                seen.append(cl)

    _push(table["primary_key"])
    for idx in table["indexes"]:
        _push(idx["columns"])
    return seen


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def _file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--default-sql",
        type=Path,
        required=True,
        help="Path to database/CreateDefault.sql in the canonical MOVES tree",
    )
    parser.add_argument(
        "--nr-default-sql",
        type=Path,
        required=True,
        help="Path to database/CreateNRDefault.sql in the canonical MOVES tree",
    )
    parser.add_argument(
        "--moves-commit",
        required=True,
        help="The canonical-moves commit the inputs came from (full SHA)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output JSON path (e.g. tables.json)",
    )
    args = parser.parse_args(argv)

    # Parse both files in a single pass so case-fold dedup applies across
    # them — the NR file re-declares onroad-style tables in PascalCase that
    # the default file already defined in camelCase. MariaDB with
    # ``lower_case_table_names=1`` only keeps one.
    text_default = args.default_sql.read_text()
    text_nr = args.nr_default_sql.read_text()
    tables = parse_tables(text_default + "\n" + text_nr)

    classified = []
    for t in tables:
        rows = _estimate_row_count(t)
        classified.append(
            {
                "name": t["name"],
                "primary_key": t["primary_key"],
                "columns": [{"name": c["name"], "type": c["type"]} for c in t["columns"]],
                "indexes": t["indexes"],
                # Multiplying PK cardinalities is an upper bound; rate
                # tables are sparse so the real default-DB count is usually
                # smaller. Task 80 will measure actual counts during the
                # conversion run.
                "estimated_rows_upper_bound": rows,
                "size_bucket": _size_bucket(rows),
                "filter_columns": _filter_columns(t),
                "partition": _classify_partition(t),
            }
        )

    classified.sort(key=lambda t: t["name"].lower())

    output = {
        "schema_version": "moves-default-db-schema/v1",
        "moves_commit": args.moves_commit,
        "sources": {
            "CreateDefault.sql": _file_hash(args.default_sql),
            "CreateNRDefault.sql": _file_hash(args.nr_default_sql),
        },
        "table_count": len(classified),
        "tables": classified,
    }

    args.output.write_text(json.dumps(output, indent=2, sort_keys=False) + "\n")

    # Print a one-screen summary of the partition strategy.
    by_strategy = {}
    for t in classified:
        s = t["partition"]["strategy"]
        by_strategy.setdefault(s, []).append(t["name"])
    print(f"Parsed {len(classified)} tables.")
    for s in sorted(by_strategy):
        print(f"  {s}: {len(by_strategy[s])} tables")


if __name__ == "__main__":
    main()
