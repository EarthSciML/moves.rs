# Downstream tools — loading and analysing MOVES output

This page is the companion to [`output-schema.md`](output-schema.md):
that page is the canonical column reference for the Parquet output a
`moves.rs` run produces; this page shows how to *load* that output into
the tools downstream researchers actually use — pandas, R, Polars,
DuckDB, and Apache Spark — and walks three end-to-end analyses that
reproduce common canonical-MOVES post-processing.

It ships with Phase 4 Task 90 of the
[migration plan](../moves-rust-migration-plan.md). The goal is zero
friction: every snippet below is copy-pasteable, and the
[sample-dataset generator](#generate-a-sample-dataset) lets you run all
of them without a full MOVES run.

## The output layout

A run writes three logical tables under an output root:

```
sample-output/
├── MOVESRun.parquet
├── MOVESOutput/
│   ├── yearID=2020/monthID=1/part.parquet
│   ├── yearID=2020/monthID=7/part.parquet
│   └── …
└── MOVESActivityOutput/
    ├── yearID=2020/monthID=1/part.parquet
    └── …
```

* `MOVESRun.parquet` — one row of run metadata (units, RunSpec
  description, provenance hashes).
* `MOVESOutput/` — per-`(time, location, pollutant, process)`
  emissions. Hive-partitioned by `(yearID, monthID)`.
* `MOVESActivityOutput/` — per-`(time, location, activity-type)`
  activity. Same partition layout.

The single most useful fact for downstream code: **the partition
columns `yearID` and `monthID` are also stored inside every row.** A
reader that ignores the directory structure entirely — just globs
`**/*.parquet` and concatenates — still sees correct, complete data.
Hive partitioning is therefore an *optimisation* (predicate pushdown),
never a requirement. Every loader below leads with the partition-unaware
form because it works in every tool and every version;
[partition pruning](#partition-pruning) is covered separately.

Throughout this page the output root is `sample-output/`. Replace it
with your run's output directory.

## Generate a sample dataset

Run this once. It writes a small but representative output tree to
`./sample-output/` so every example on this page is runnable. Requires
only `pyarrow` (`pip install pyarrow`).

```python
"""Generate a tiny MOVES output tree for the examples in this page."""
import itertools
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path("sample-output")
RUN_HASH = "0" * 64  # stand-in for the real hex SHA-256 runHash
ROOT.mkdir(parents=True, exist_ok=True)

# --- MOVESRun.parquet — one row of run metadata ---------------------
pq.write_table(
    pa.table({
        "MOVESRunID": [1],
        "runSpecDescription": ["downstream-tools.md sample run"],
        "scale": ["County"],
        "massUnits": ["Grams"],
        "timeUnits": ["Hours"],
        "distanceUnits": ["Miles"],
        "energyUnits": ["Joules"],
        "runHash": [RUN_HASH],
        "calculatorVersion": ["moves-rs sample"],
    }),
    ROOT / "MOVESRun.parquet",
)

counties = [26161, 26163]      # Washtenaw + Wayne counties, Michigan
months = [1, 7]                # January + July
pollutants = [2, 3, 110, 87]   # CO, NOx, PM2.5, VOC
processes = [1, 2]             # Running Exhaust, Start Exhaust

# --- MOVESOutput/ — emissions, one partition file per month ---------
for month in months:
    rows = []
    for county, poll, proc in itertools.product(
        counties, pollutants, processes
    ):
        quant = float(county % 1000 + poll + proc * 10)
        rows.append({
            "MOVESRunID": 1,
            "yearID": 2020, "monthID": month, "dayID": 5, "hourID": 8,
            "stateID": county // 1000, "countyID": county,
            "zoneID": county * 10, "linkID": county * 100,
            "pollutantID": poll, "processID": proc,
            "sourceTypeID": 21, "regClassID": 30,
            "fuelTypeID": 1, "modelYearID": 2018, "roadTypeID": 5,
            "SCC": f"220100{proc:02d}10",
            "emissionQuant": quant,
            "emissionRate": quant / 1000.0,
            "runHash": RUN_HASH,
        })
    part = ROOT / "MOVESOutput" / "yearID=2020" / f"monthID={month}"
    part.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), part / "part.parquet")

# --- MOVESActivityOutput/ — activity, one partition file per month --
for month in months:
    rows = []
    for county in counties:
        for activity_type, value in [(1, county * 1.5), (6, county * 0.2)]:
            rows.append({
                "MOVESRunID": 1,
                "yearID": 2020, "monthID": month, "dayID": 5, "hourID": 8,
                "stateID": county // 1000, "countyID": county,
                "sourceTypeID": 21, "fuelTypeID": 1, "roadTypeID": 5,
                "activityTypeID": activity_type, "activity": float(value),
                "runHash": RUN_HASH,
            })
    part = ROOT / "MOVESActivityOutput" / "yearID=2020" / f"monthID={month}"
    part.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), part / "part.parquet")

print(f"wrote sample output to {ROOT.resolve()}")
```

The sample carries only the columns the examples use; see
[`output-schema.md`](output-schema.md) for the full schema. It fills
*both* `emissionQuant` and `emissionRate` so every example runs — a real
run populates one or the other (see
[emissionQuant vs emissionRate](#caveats-and-limitations)).

## Loading the output

### Polars (Python)

```python
import polars as pl

# scan_parquet is lazy — nothing is read until .collect().
emissions = pl.scan_parquet("sample-output/MOVESOutput/**/*.parquet")
activity = pl.scan_parquet("sample-output/MOVESActivityOutput/**/*.parquet")
run = pl.read_parquet("sample-output/MOVESRun.parquet")  # singleton, eager

print(emissions.head().collect())
```

### pandas + PyArrow (Python)

```python
import glob

import pandas as pd


def load_table(directory: str) -> pd.DataFrame:
    """Concatenate every partition file under a MOVES output table."""
    files = sorted(glob.glob(f"{directory}/**/*.parquet", recursive=True))
    return pd.concat(
        (pd.read_parquet(f) for f in files), ignore_index=True
    )


emissions = load_table("sample-output/MOVESOutput")
activity = load_table("sample-output/MOVESActivityOutput")
run = pd.read_parquet("sample-output/MOVESRun.parquet")

print(emissions.head())
```

### DuckDB

DuckDB reads the layout with no extension or setup. From Python:

```python
import duckdb

con = duckdb.connect()
con.sql("""
    CREATE VIEW emissions AS
    SELECT * FROM read_parquet('sample-output/MOVESOutput/**/*.parquet');
    CREATE VIEW activity AS
    SELECT * FROM read_parquet('sample-output/MOVESActivityOutput/**/*.parquet');
    CREATE VIEW run AS
    SELECT * FROM read_parquet('sample-output/MOVESRun.parquet');
""")
con.sql("SELECT * FROM emissions LIMIT 5").show()
```

The same `read_parquet(...)` calls work verbatim in the `duckdb` CLI and
in any DuckDB client (R, Java, the CLI's `.mode csv`, etc.).

### R (arrow + dplyr)

```r
library(arrow)
library(dplyr)

load_table <- function(dir) {
  files <- list.files(dir, pattern = "\\.parquet$",
                      recursive = TRUE, full.names = TRUE)
  open_dataset(files)            # a vector of files → no partition inference
}

emissions <- load_table("sample-output/MOVESOutput")
activity <- load_table("sample-output/MOVESActivityOutput")
run <- read_parquet("sample-output/MOVESRun.parquet")

emissions |> head() |> collect()
```

`open_dataset` returns a lazy Dataset; dplyr verbs (`filter`,
`group_by`, `summarise`, `mutate`, `arrange`) build a query that only
executes on `collect()`.

### Apache Spark (PySpark)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("moves-output").getOrCreate()


def load_table(path: str):
    # recursiveFileLookup=true reads every parquet under `path` and skips
    # Spark's partition discovery, so the yearID/monthID columns come
    # from the row data.
    return spark.read.option("recursiveFileLookup", "true").parquet(path)


emissions = load_table("sample-output/MOVESOutput")
activity = load_table("sample-output/MOVESActivityOutput")
run = spark.read.parquet("sample-output/MOVESRun.parquet")

emissions.show(5)
```

### Partition pruning

For multi-year / multi-month outputs, Polars and DuckDB read the
`yearID` / `monthID` predicate straight from the directory layout and
open only the partition files that match:

```python
# Polars — pass the directory, not a glob.
import polars as pl
df = (
    pl.scan_parquet("sample-output/MOVESOutput", hive_partitioning=True)
    .filter(pl.col("yearID") == 2020)
    .collect()
)
```

```sql
-- DuckDB
SELECT * FROM read_parquet(
    'sample-output/MOVESOutput/**/*.parquet', hive_partitioning = true)
WHERE yearID = 2020 AND monthID IN (6, 7, 8);
```

Both reconcile `yearID` / `monthID` appearing in the directory names
*and* in the row data down to a single column.

PyArrow's dataset API and arrow's R `open_dataset()` are stricter: asked
to discover hive partitions, they reject the partition column because it
collides with the identically-named row-data column (`ArrowTypeError:
Unable to merge: Field yearID has incompatible types`). For those
readers use the recursive-glob loaders above — they do no partition
pruning, but `yearID` / `monthID` are in the row data, so filtering on
them after the read is correct.

## Run metadata and units

`MOVESRun.parquet` is a single row. Its `massUnits`, `timeUnits`,
`distanceUnits`, and `energyUnits` columns tell you how to interpret
`emissionQuant`, `emissionRate`, and `activity` — **always read them
before converting or summing.** Canonical MOVES lets the RunSpec choose
the units, so they are a property of the run, not a constant.

`runHash` and `MOVESRunID` appear on every emission and activity row,
so you can attach run metadata with a join:

```python
import polars as pl

run = pl.read_parquet("sample-output/MOVESRun.parquet")
units = run.select("runHash", "massUnits", "timeUnits", "distanceUnits")

emissions = (
    pl.scan_parquet("sample-output/MOVESOutput/**/*.parquet")
    .join(units.lazy(), on="runHash", how="left")
    .collect()
)
print(emissions.select("countyID", "pollutantID", "emissionQuant",
                        "massUnits").head())
```

### Activity output

`MOVESActivityOutput` carries the activity that drove the emissions.
`activityTypeID` identifies the quantity — `1` is distance travelled
(VMT), `6` is source population; the full set lives in the default-DB
`activitytype` table. A quick VMT-per-county rollup in PySpark:

```python
from pyspark.sql import functions as F

vmt = (
    load_table("sample-output/MOVESActivityOutput")
    .filter(F.col("activityTypeID") == 1)        # 1 = distance travelled
    .groupBy("yearID", "countyID")
    .agg(F.sum("activity").alias("vmt"))
    .orderBy("yearID", "countyID")
)
vmt.show()
```

## Decoding numeric IDs

The output columns are numeric IDs that follow the canonical MOVES
conventions verbatim (`pollutantID`, `processID`, `sourceTypeID`,
`countyID`, `roadTypeID`, …). Two ways to attach human-readable names:

**Join the default-DB lookup tables.** The
[`moves-default-db-convert`](../crates/moves-default-db-convert/src/lib.rs)
tool writes the canonical MOVES dimension tables — `pollutant`,
`emissionprocess`, `sourceusetype`, `roadtype`, `fueltype`, `county`,
`activitytype`, … — as Parquet under
`<output-root>/movesdb<YYYYMMDD>/<table>/`. Join `MOVESOutput` to a
dimension table on the shared `*ID` column to get the names. This is the
authoritative, complete mapping.

**Inline a convenience subset.** For quick interactive work, a small
dictionary avoids a join. The values below are a *subset*; the
default-DB `pollutant` and `emissionprocess` tables are authoritative.

```python
POLLUTANT = {
    1: "Total Gaseous Hydrocarbons",
    2: "Carbon Monoxide (CO)",
    3: "Oxides of Nitrogen (NOx)",
    5: "Methane (CH4)",
    6: "Nitrous Oxide (N2O)",
    20: "Benzene",
    25: "Formaldehyde",
    87: "Volatile Organic Compounds",
    100: "Primary Exhaust PM10 - Total",
    110: "Primary Exhaust PM2.5 - Total",
}
PROCESS = {
    1: "Running Exhaust",
    2: "Start Exhaust",
    9: "Brakewear",
    10: "Tirewear",
    11: "Evap Permeation",
    12: "Evap Fuel Vapor Venting",
    13: "Evap Fuel Leaks",
    15: "Crankcase Running Exhaust",
    16: "Crankcase Start Exhaust",
    17: "Crankcase Extended Idle Exhaust",
    18: "Refueling Displacement Vapor Loss",
    19: "Refueling Spillage Loss",
}
```

## Example analyses

Three analyses follow, each reproducing a canonical-MOVES
post-processing task. They run against the
[sample dataset](#generate-a-sample-dataset).

### 1. NEI submission summary

EPA's National Emissions Inventory ingests onroad emissions as annual
totals keyed by county, Source Classification Code (SCC), and pollutant.
Collapse the hourly/daily/monthly/process/source-type detail of
`MOVESOutput` to that grain:

```sql
-- DuckDB
SELECT
    countyID,
    SCC,
    pollutantID,
    sum(emissionQuant) AS annual_emission_quant
FROM read_parquet('sample-output/MOVESOutput/**/*.parquet')
WHERE yearID = 2020
  AND emissionQuant IS NOT NULL
GROUP BY countyID, SCC, pollutantID
ORDER BY countyID, SCC, pollutantID;
```

The same rollup in Polars:

```python
import polars as pl

nei = (
    pl.scan_parquet("sample-output/MOVESOutput/**/*.parquet")
    .filter(
        (pl.col("yearID") == 2020) & pl.col("emissionQuant").is_not_null()
    )
    .group_by("countyID", "SCC", "pollutantID")
    .agg(pl.col("emissionQuant").sum().alias("annual_emission_quant"))
    .sort("countyID", "SCC", "pollutantID")
    .collect()
)
print(nei)
```

A real NEI submission additionally expects the county as a 5-digit FIPS
string and pollutant codes mapped to the NEI code list; the rollup above
is the numeric core that feeds that formatting step.

### 2. County-level inventory rollup

A county inventory totals emissions per county, per pollutant, per year.
`emissionQuant` is a mass in the run's `massUnits`; the sample run uses
grams, so convert to US tons for a conventional inventory table. Check
`massUnits` first — only divide if the run really is in grams.

```python
import polars as pl

GRAMS_PER_US_TON = 907_184.74

run = pl.read_parquet("sample-output/MOVESRun.parquet")
assert run["massUnits"][0] == "Grams", "adjust the conversion factor"

rollup = (
    pl.scan_parquet("sample-output/MOVESOutput/**/*.parquet")
    .filter(pl.col("emissionQuant").is_not_null())
    .group_by("yearID", "countyID", "pollutantID")
    .agg(pl.col("emissionQuant").sum().alias("emission_quant_g"))
    .with_columns(
        (pl.col("emission_quant_g") / GRAMS_PER_US_TON)
        .alias("emission_us_tons")
    )
    .sort("yearID", "countyID", "pollutantID")
    .collect()
)
print(rollup)
```

The same rollup in R, using arrow + dplyr:

```r
library(arrow)
library(dplyr)

grams_per_us_ton <- 907184.74

rollup <- open_dataset(
    list.files("sample-output/MOVESOutput", pattern = "\\.parquet$",
               recursive = TRUE, full.names = TRUE)) |>
  filter(!is.na(emissionQuant)) |>
  group_by(yearID, countyID, pollutantID) |>
  summarise(emission_quant_g = sum(emissionQuant), .groups = "drop") |>
  mutate(emission_us_tons = emission_quant_g / grams_per_us_ton) |>
  arrange(yearID, countyID, pollutantID) |>
  collect()

print(rollup)
```

### 3. Rates-mode CSV export

When a run is configured for emission-*rate* output, the `emissionRate`
column is populated instead of `emissionQuant`. Downstream rate
consumers — SMOKE-MOVES, dispersion pre-processors — expect a flat CSV.
Select the rows carrying a rate, project the rate dimensions, and write
CSV with a deterministic row order so the file is reproducible:

```sql
-- DuckDB
COPY (
    SELECT yearID, monthID, hourID, countyID, zoneID, linkID,
           sourceTypeID, regClassID, fuelTypeID, modelYearID,
           roadTypeID, pollutantID, processID, emissionRate
    FROM read_parquet('sample-output/MOVESOutput/**/*.parquet')
    WHERE emissionRate IS NOT NULL
    ORDER BY yearID, monthID, hourID, countyID, linkID,
             pollutantID, processID, sourceTypeID, modelYearID
) TO 'moves_rates.csv' (FORMAT CSV, HEADER);
```

The same export in pandas:

```python
import glob

import pandas as pd

rate_cols = [
    "yearID", "monthID", "hourID", "countyID", "zoneID", "linkID",
    "sourceTypeID", "regClassID", "fuelTypeID", "modelYearID",
    "roadTypeID", "pollutantID", "processID", "emissionRate",
]

files = sorted(
    glob.glob("sample-output/MOVESOutput/**/*.parquet", recursive=True)
)
emissions = pd.concat(
    (pd.read_parquet(f) for f in files), ignore_index=True
)

rates = emissions.loc[emissions["emissionRate"].notna(), rate_cols]
rates = rates.sort_values(rate_cols[:-1]).reset_index(drop=True)
rates.to_csv("moves_rates.csv", index=False)
```

This mirrors the per-rate-table CSVs canonical MOVES emits in rates
mode. The explicit `ORDER BY` / `sort_values` makes the CSV
byte-stable across re-runs.

## Reproducibility — runHash

`runHash` is the hex SHA-256 of a run's canonical inputs (RunSpec bytes,
default-DB content hashes, calculator-DAG hash). It appears on every
emission, activity, and run row. Two uses:

* **Pool many runs into one dataset.** Concatenate the outputs of
  several runs into a shared directory; `runHash` distinguishes which
  run produced each row without consulting `MOVESRun`.

  ```sql
  SELECT DISTINCT runHash
  FROM read_parquet('pooled/MOVESOutput/**/*.parquet');
  ```

* **Deduplicate cached results.** Identical inputs produce an identical
  `runHash` and — per the determinism contract in
  [`output-schema.md`](output-schema.md) — byte-identical Parquet, so a
  result cache can key on `runHash`.

`MOVESRun.calculatorVersion` records the `moves.rs` build that produced
the run, for per-run audits.

## Caveats and limitations

* **Not for regulatory submissions.** These examples demonstrate the
  *mechanics* of MOVES post-processing. `moves.rs` is not validated for
  State Implementation Plan, transportation-conformity, or official NEI
  submissions — use canonical EPA MOVES for regulatory work. See
  migration-plan Task 129 (porting guide) for the full caveat.
* **`emissionQuant` vs `emissionRate`.** Both columns exist on
  `MOVESOutput`. A run produces an *inventory* (`emissionQuant`
  populated) or *rates* (`emissionRate` populated) depending on its
  RunSpec output configuration; filter on `… IS NOT NULL` for whichever
  your run produced. The sample on this page fills both so every example
  runs.
* **Units are per-run.** `emissionQuant`, `emissionRate`, and `activity`
  carry no implicit unit — read `MOVESRun.massUnits` /
  `timeUnits` / `distanceUnits` / `energyUnits` before converting.
* **Output schema is stable; calculators are in progress.** The Task 89
  Parquet schema is stable — legacy columns are frozen, and new columns
  may only be appended. The calculators that populate it are being ported
  across Phases 2–3 (onroad) and Phase 5 (nonroad); treat values from
  in-progress calculators as provisional.

## See also

* [`output-schema.md`](output-schema.md) — canonical column reference
  for the three output tables.
* [`runspec-toml.md`](runspec-toml.md) — the TOML RunSpec format that
  configures a run (including its output units).
* [`moves-rust-migration-plan.md`](../moves-rust-migration-plan.md) —
  Task 90 (this page) and Task 128 (the broader user manual that extends
  it).
