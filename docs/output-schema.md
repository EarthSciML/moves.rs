# Output schema — Parquet reference

This page documents the unified MOVES Parquet output layout introduced
by Task 89. It is the canonical writer-side schema for every `moves.rs`
run; both onroad (Phase 2/3) and nonroad (Phase 5 Task 114) emissions
converge here.

The schema is declared in code under
[`moves_data::output_schema`](../crates/moves-data/src/output_schema.rs);
the writer that materialises it lives in
[`moves_framework::output_processor`](../crates/moves-framework/src/output_processor.rs).
This document mirrors those modules' rustdoc and is the recommended
starting point for downstream-tool authors.

## Directory layout

```
<output-root>/
├── MOVESRun.parquet
├── MOVESOutput/
│   ├── yearID=2020/monthID=1/part.parquet
│   ├── yearID=2020/monthID=7/part.parquet
│   └── …
└── MOVESActivityOutput/
    ├── yearID=2020/monthID=1/part.parquet
    └── …
```

* `MOVESRun.parquet` — singleton file with run metadata. One row per
  MOVES run; rewritten when [`OutputProcessor::new`] is called.
* `MOVESOutput/` — per-emission rows. Hive-partitioned by
  `(yearID, monthID)`. The partition columns remain in the row data, so
  readers that don't honour hive partitioning still see them.
* `MOVESActivityOutput/` — per-activity rows. Same partition layout.

The directory naming follows the same `<column>=<value>` convention as
the input-side default-DB layout produced by `moves-default-db-convert`.
Null partition values land in `<column>=__NULL__`.

Within a partition file, row order matches the order in which rows were
delivered to [`write_emissions`] / [`write_activity`]. The writer
does not sort within a partition — callers are expected to produce a
deterministic order when byte-identity matters (Phase 0 fixture diffs).

## Reading the layout

The structure plays well with Polars / DuckDB / pandas:

```python
import polars as pl
df = pl.scan_parquet("<output-root>/MOVESOutput/**/*.parquet",
                     hive_partitioning=True)
df.filter(pl.col("yearID") == 2020).group_by("pollutantID").sum().collect()
```

```sql
-- DuckDB
SELECT pollutantID, sum(emissionQuant) AS emissions
FROM read_parquet('<output-root>/MOVESOutput/**/*.parquet', hive_partitioning=1)
WHERE yearID = 2020 AND monthID IN (6, 7, 8)
GROUP BY pollutantID;
```

Both readers push the `yearID` / `monthID` predicates into the
partition layout — only the matching partition files are opened.

```python
import pyarrow.dataset as ds
dataset = ds.dataset("<output-root>/MOVESOutput", format="parquet",
                     partitioning="hive")
table = dataset.to_table(filter=(ds.field("yearID") == 2020))
```

Task 90 ships full downstream examples (NEI rollups, county inventories,
rates-mode CSV exports). This page documents the underlying schema; the
examples cover the typical analyses.

## Schema

### `MOVESRun.parquet`

| Column | Type | Nullable | PK | Source |
|---|---|---|---|---|
| `MOVESRunID` | smallint | no | ✓ | legacy |
| `outputTimePeriod` | text | yes |  | legacy |
| `timeUnits` | text | yes |  | legacy |
| `distanceUnits` | text | yes |  | legacy |
| `massUnits` | text | yes |  | legacy |
| `energyUnits` | text | yes |  | legacy |
| `runSpecFileName` | text | yes |  | legacy |
| `runSpecDescription` | text | yes |  | legacy |
| `runSpecFileDateTime` | datetime | yes |  | legacy |
| `runDateTime` | datetime | yes |  | legacy |
| `scale` | text | yes |  | legacy |
| `minutesDuration` | float | yes |  | legacy |
| `defaultDatabaseUsed` | text | yes |  | legacy |
| `masterVersion` | text | yes |  | legacy |
| `masterComputerID` | text | yes |  | legacy |
| `masterIDNumber` | text | yes |  | legacy |
| `domain` | text | yes |  | legacy |
| `domainCountyID` | int | yes |  | legacy |
| `domainCountyName` | text | yes |  | legacy |
| `domainDatabaseServer` | text | yes |  | legacy |
| `domainDatabaseName` | text | yes |  | legacy |
| `expectedDONEFiles` | int | yes |  | legacy |
| `retrievedDONEFiles` | int | yes |  | legacy |
| `models` | text | yes |  | legacy |
| `runHash` | text | no |  | **additive** |
| `calculatorVersion` | text | no |  | **additive** |

### `MOVESOutput/<partitions>/part.parquet`

Partition columns: `yearID`, `monthID`.

| Column | Type | Nullable | PK | Source |
|---|---|---|---|---|
| `MOVESRunID` | smallint | no | ✓ | legacy |
| `iterationID` | smallint | yes |  | legacy |
| `yearID` | smallint | yes |  | legacy / partition |
| `monthID` | smallint | yes |  | legacy / partition |
| `dayID` | smallint | yes |  | legacy |
| `hourID` | smallint | yes |  | legacy |
| `stateID` | smallint | yes |  | legacy |
| `countyID` | int | yes |  | legacy |
| `zoneID` | int | yes |  | legacy |
| `linkID` | int | yes |  | legacy |
| `pollutantID` | smallint | yes |  | legacy |
| `processID` | smallint | yes |  | legacy |
| `sourceTypeID` | smallint | yes |  | legacy |
| `regClassID` | smallint | yes |  | legacy |
| `fuelTypeID` | smallint | yes |  | legacy |
| `fuelSubTypeID` | smallint | yes |  | legacy |
| `modelYearID` | smallint | yes |  | legacy |
| `roadTypeID` | smallint | yes |  | legacy |
| `SCC` | text | yes |  | legacy |
| `engTechID` | smallint | yes |  | legacy |
| `sectorID` | smallint | yes |  | legacy |
| `hpID` | smallint | yes |  | legacy |
| `emissionQuant` | float | yes |  | legacy |
| `emissionRate` | float | yes |  | legacy |
| `runHash` | text | no |  | **additive** |

### `MOVESActivityOutput/<partitions>/part.parquet`

Partition columns: `yearID`, `monthID`.

| Column | Type | Nullable | PK | Source |
|---|---|---|---|---|
| `MOVESRunID` | smallint | no | ✓ | legacy |
| `iterationID` | smallint | yes |  | legacy |
| `yearID` | smallint | yes |  | legacy / partition |
| `monthID` | smallint | yes |  | legacy / partition |
| `dayID` | smallint | yes |  | legacy |
| `hourID` | smallint | yes |  | legacy |
| `stateID` | smallint | yes |  | legacy |
| `countyID` | int | yes |  | legacy |
| `zoneID` | int | yes |  | legacy |
| `linkID` | int | yes |  | legacy |
| `sourceTypeID` | smallint | yes |  | legacy |
| `regClassID` | smallint | yes |  | legacy |
| `fuelTypeID` | smallint | yes |  | legacy |
| `fuelSubTypeID` | smallint | yes |  | legacy |
| `modelYearID` | smallint | yes |  | legacy |
| `roadTypeID` | smallint | yes |  | legacy |
| `SCC` | text | yes |  | legacy |
| `engTechID` | smallint | yes |  | legacy |
| `sectorID` | smallint | yes |  | legacy |
| `hpID` | smallint | yes |  | legacy |
| `activityTypeID` | smallint | yes |  | legacy |
| `activity` | float | yes |  | legacy |
| `runHash` | text | no |  | **additive** |

## Legacy vs. additive columns

* **Legacy columns** are 1:1 with the canonical MOVES
  `MOVESRun` / `MOVESOutput` / `MOVESActivityOutput` MariaDB schema
  (MOVES commit `25dc6c833dd8c88198f82cee93ca30be1456df8b`). Column
  names use the same case-sensitive camelCase canonical-MOVES uses
  (`MOVESRunID`, not `moves_run_id`). MariaDB `unsigned` annotations
  are dropped; Parquet integer widths come from the smallest type that
  represents the underlying range.
* **Additive columns** are introduced by the Rust port for provenance
  and downstream-deduplication:
  - `runHash` — hex SHA-256 of the canonical run inputs (RunSpec bytes,
    default-DB content hashes, calculator-DAG hash). Joinable across
    runs to identify which run produced a given row without consulting
    `MOVESRun`.
  - `calculatorVersion` (run table only) — moves.rs build identifier,
    typically `CARGO_PKG_VERSION` followed by an optional git rev.

Additive columns trail the legacy block in the schema declaration; a
schema test guards the ordering. New additive columns may be appended;
removing or reordering legacy columns is a breaking change.

## Determinism contract

Identical inputs to [`OutputProcessor::write_emissions`] /
[`OutputProcessor::write_activity`] (same record contents and ordering)
produce byte-identical parquet bytes. The contract pieces:

1. Writer settings are pinned: `UNCOMPRESSED`, dictionary disabled,
   statistics disabled, `PARQUET_1_0` writer version, fixed
   `created_by` string.
2. Partition grouping is deterministic — rows are bucketed via
   `BTreeMap` keyed on `(Option<i16>, Option<i16>)`, so partition output
   order does not depend on input order.
3. Within a partition, the writer preserves input row order verbatim.

This matches the determinism pieces the upstream `moves-snapshot` and
`moves-default-db-convert` writers honour, so a snapshot of an
end-to-end run is stable across machines and re-runs.

## Atomicity

Each parquet file is written via a `<path>.tmp` sibling + rename. A
process crash leaves either the previous version of the file or no
file; downstream readers never observe a truncated parquet footer.

[`OutputProcessor::new`]: ../crates/moves-framework/src/output_processor.rs
[`OutputProcessor::write_emissions`]: ../crates/moves-framework/src/output_processor.rs
[`OutputProcessor::write_activity`]: ../crates/moves-framework/src/output_processor.rs
[`write_emissions`]: ../crates/moves-framework/src/output_processor.rs
[`write_activity`]: ../crates/moves-framework/src/output_processor.rs
