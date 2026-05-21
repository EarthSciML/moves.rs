# moves.rs User Guide

`moves.rs` is a pure-Rust port of EPA's MOVES on-road and NONROAD emission
model. It ships as a **single static binary** — no MariaDB, no JDK, no
separate database server. This guide covers installation, a first run, the
RunSpec format, importing custom input databases, and reading the output.

---

## Table of contents

1. [Installation](#installation)
2. [Getting started — first run](#getting-started--first-run)
3. [RunSpec format reference](#runspec-format-reference)
4. [Default database](#default-database)
5. [Importing input databases (CDB / PDB)](#importing-input-databases-cdb--pdb)
6. [Output format](#output-format)
7. [Downstream tools](#downstream-tools)
8. [Tuning `--max-parallel-chunks`](#tuning---max-parallel-chunks)
9. [Command reference](#command-reference)

---

## Installation

### Pre-built binaries

Pre-built binaries for Linux (x86_64, aarch64), macOS (x86_64, aarch64), and
Windows (x86_64) will be available on the GitHub Releases page once release
packaging is complete (migration-plan Task 131). Each release provides:

* `moves` — the main MOVES binary.
* `moves-default-db-convert` — converts an EPA MOVES default-DB MariaDB dump
  to the Parquet layout `moves.rs` expects.

Download the archive for your platform, extract, and place the binaries
somewhere on your `PATH`.

### Build from source

Requires Rust 1.70+ (`rustup` recommended).

```bash
git clone https://github.com/ctessum/moves.rs
cd moves.rs
cargo build --release --locked
# Binaries land in target/release/
./target/release/moves --version
./target/release/moves-default-db-convert --version
```

There are no external runtime dependencies. The static binary includes the
calculator logic; you do not need MariaDB, Java, or a separate database
server.

### Verifying the install

```
$ moves --help
Pure-Rust port of EPA's MOVES on-road and NONROAD emissions model.

Usage: moves <COMMAND>

Commands:
  run              Run a MOVES simulation from a RunSpec
  import-cdb       Import County-database (CDB) input CSV files into Parquet
  convert-runspec  Convert a RunSpec between XML and TOML
  help             Print this message or the help of the given subcommand(s)
```

---

## Getting started — first run

The quickest way to verify your install is to run the included sample
RunSpec. It exercises a single county, a single hour, passenger-car gasoline
vehicles, and three energy-consumption pollutants — a small but end-to-end
MOVES run.

```bash
moves run \
  --runspec characterization/fixtures/sample-runspec.xml \
  --output  /tmp/sample-out
```

Expected output (Phase 3 — calculators return empty results until the Phase 4
data plane lands; the framework, RunSpec parser, and output writer are fully
functional):

```
[moves run] characterization/fixtures/sample-runspec.xml
  scale            : MACROSCALE
  models           : ["OnRoad"]
  counties         : [26161]
  years            : [2001]
  months           : [6]
  pollutants       : 10
  iterations       : 1
  max parallelism  : <N>
  wall time        : X.X ms  (plan Y.Y ms, exec Z.Z ms)
  peak RSS         : X.X MiB
  output directory : /tmp/sample-out
  run record       : /tmp/sample-out/MOVESRun.parquet
```

The output directory contains:

```
/tmp/sample-out/
├── MOVESRun.parquet               # run metadata (one row)
├── MOVESOutput/                   # per-emission rows (may be empty in Phase 3)
│   └── yearID=2001/monthID=6/
│       └── part.parquet
└── MOVESActivityOutput/           # per-activity rows (may be empty in Phase 3)
    └── yearID=2001/monthID=6/
        └── part.parquet
```

See [Output format](#output-format) for the full schema.

### Converting the sample RunSpec to TOML

If you want to explore the TOML format before writing your own RunSpec:

```bash
moves convert-runspec \
  --input characterization/fixtures/sample-runspec.xml
# writes characterization/fixtures/sample-runspec.toml
```

Open `sample-runspec.toml` to see the TOML equivalent. The full TOML format
reference is in [`runspec-toml.md`](runspec-toml.md).

---

## RunSpec format reference

A RunSpec tells `moves.rs` what to compute: the geographic scope (counties),
time span (years, months, hours), vehicle selections, pollutant/process
associations, output units, and any control strategies.

Two equivalent formats are supported:

| Format | Extension | Best for |
|--------|-----------|---------|
| XML (legacy MOVES) | `.xml`, `.mrs` | Existing RunSpecs from canonical MOVES |
| TOML | `.toml` | Hand-authored RunSpecs; comments, readable names |

Both pass through the same [`RunSpec`](../crates/moves-runspec/src/model.rs)
model, so TOML→XML and XML→TOML round-trips are information-preserving.

### XML format

The XML format is the `.mrs` file canonical MOVES exports. `moves.rs` reads
it with no conversion step — point `--runspec` at any canonical MOVES
export and it runs:

```bash
moves run --runspec my-existing-runspec.xml --output out/
```

`moves convert-runspec --input my-existing-runspec.xml` produces an
equivalent TOML file for editing.

### TOML format

The TOML format is the recommended hand-authored form. Short table names,
named-enum values, and full comment support make it readable and
self-documenting.

```toml
description = "Tutorial run — Washtenaw County, June 2020"

[run]
models  = ["onroad"]
scale   = "macro"
pm_size = 25

[[geo]]
type        = "county"
key         = 26161
description = "MICHIGAN - Washtenaw County"

[time]
years      = [2020]
months     = [6]
days       = ["weekday"]
begin_hour = 6
end_hour   = 20

[[onroad]]
fuel_type_id   = 1
fuel_type_desc = "Gasoline"
source_type_id = 21
source_type_name = "Passenger Car"

[[road_type]]
road_type_id   = 5
road_type_name = "Urban Unrestricted Access"
model_combination = "M6"

[[pollutant_process]]
pollutant_id   = 2
pollutant      = "Carbon Monoxide (CO)"
process_id     = 1
process        = "Running Exhaust"

[[pollutant_process]]
pollutant_id   = 3
pollutant      = "Oxides of Nitrogen (NOx)"
process_id     = 1
process        = "Running Exhaust"

[output]
geographic_output_detail = "county"
output_emission_quant    = true
```

Save as `my-run.toml` and run:

```bash
moves run --runspec my-run.toml --output out/
```

For the complete field-by-field TOML reference — all tables, every key,
allowed enum values — see **[`runspec-toml.md`](runspec-toml.md)**.

### Control strategies

Control strategies (AVFT, Rate-of-Progress, OnRoad Retrofit, NONROAD
Retrofit) are declared in the RunSpec and driven by supplementary CSV files.
See **[`control-strategies.md`](control-strategies.md)** for the TOML schema
and worked examples for each strategy.

---

## Default database

`moves.rs` reads from a **converted default-DB Parquet tree** rather than a
live MariaDB instance. This is a directory of Parquet files created from the
canonical MOVES default database.

### Why a Parquet tree?

Canonical MOVES requires a running MariaDB server to serve default-DB lookups
at runtime. `moves.rs` replaces this with read-only Parquet files: the runtime
does lazy-loaded, memory-mapped reads directly from the filesystem, eliminating
the MariaDB dependency entirely.

### Obtaining the default database

The converted default-DB Parquet tree will be distributed as a separate
downloadable artifact alongside the binary releases. Download it and note its
path.

For development or if you have a canonical MOVES installation:

1. Run the dump script inside the MOVES Apptainer image:

   ```bash
   characterization/default-db-conversion/dump-default-db.sh \
     /path/to/canonical-moves.sif \
     /tmp/dump/movesdb20241112
   ```

2. Convert the TSV dump to Parquet:

   ```bash
   moves-default-db-convert \
     --tsv-dir      /tmp/dump/movesdb20241112 \
     --plan         characterization/default-db-schema/tables.json \
     --output       default-db/movesdb20241112 \
     --moves-db-version movesdb20241112
   ```

See `characterization/default-db-conversion/README.md` for full details.

### Passing the default database to a run

The converted default-DB tree is embedded in the binary for the default
MOVES database version. For custom DB versions, point `--default-db` at
the converted directory (when that flag is available — see migration-plan
Task 81 for the status of runtime DB selection).

---

## Importing input databases (CDB / PDB)

A MOVES run can use custom input data in place of, or supplementing, the
default database. The Rust port supports two import formats:

| Format | Scale | Use |
|--------|-------|-----|
| CDB (County Database) | County / Macro-scale | Custom age distributions, zone definitions, source populations, etc. |
| PDB (Project Database) | Project-scale | Link geometry, speed distributions, op-mode distributions |

### County-scale import (CDB)

```bash
moves import-cdb \
  --input   /path/to/cdb-csvs/ \
  --output  /path/to/cdb-parquet/ \
  [--default-db /path/to/default-db/movesdb20241112]
```

`--input` must be a directory with `<TableName>.csv` files (one per table,
header row required, case-insensitive column names). `--output` receives the
validated `<TableName>.parquet` files. The directory is created if absent.

`--default-db` is strongly recommended. Without it, foreign-key checks
degrade to **warnings** — numeric-range and cross-row invariants still
apply, but referential integrity is not enforced.

#### Currently supported CDB tables

| Table | Cross-row check |
|-------|----------------|
| `SourceTypePopulation` | Year-range coverage; no zero-population rows |
| `ZoneRoadType` | `SHOAllocFactor` sums to 1.0 per `roadTypeID` |
| `AgeDistribution` | `ageFraction` sums to 1.0 per `(sourceTypeID, yearID)` |
| `Zone` | Allocation factors sum to 1.0 per `countyID` |

Additional tables (`AverageSpeedDistribution`, `FuelSupply`, `IMCoverage`,
`Hotelling`, and others) will be added in follow-up tasks (tracked under
migration-plan Task 83). The command automatically covers each new table as
it is added — no CLI change is needed.

#### Sample session

```
$ moves import-cdb --input cdb/ --output cdb-out/ --default-db default-db/movesdb20241112
[moves import-cdb] cdb/
  ok       SourceTypePopulation          1456 row(s)
             -> cdb-out/SourceTypePopulation.parquet
  ok       ZoneRoadType                   160 row(s)
             -> cdb-out/ZoneRoadType.parquet
  ok       AgeDistribution               4680 row(s)
             -> cdb-out/AgeDistribution.parquet
  ok       Zone                             5 row(s)
             -> cdb-out/Zone.parquet
  --       ZoneRoadType (Zone domain)    no ZoneRoadType.csv in input directory
  4 written, 0 rejected, 1 missing
```

A rejected table (validation errors found) causes `moves import-cdb` to
exit with code 1. Review the printed errors and fix the CSV before
re-running.

### Project-scale import (PDB)

The PDB importer is available as the `moves-importer-pdb` library crate.
CLI integration is planned for a future task. Current supported tables:

| Table | Java class |
|-------|-----------|
| `Link` | `LinkImporter` |
| `linkSourceTypeHour` | `LinkSourceTypeHourImporter` |
| `driveScheduleSecondLink` | `DriveScheduleSecondLinkImporter` |
| `offNetworkLink` | `OffNetworkLinkImporter` |
| `OpModeDistribution` | `LinkOpmodeDistributionImporter` |

Cross-county/cross-zone shared tables (`AgeDistribution`, `Fuel`,
`Meteorology`, `IMCoverage`, `OnRoadRetrofit`, `AVFT`) are handled by the
CDB importer.

> **Note on Excel input.** Canonical MOVES accepts both CSV and Excel
> (`.xls`/`.xlsx`) input. `moves.rs` supports CSV only. Convert Excel input
> to CSV before importing.

---

## Output format

Every `moves run` writes three Parquet tables to the output directory:

```
<output>/
├── MOVESRun.parquet
├── MOVESOutput/
│   ├── yearID=<y>/monthID=<m>/part.parquet
│   └── …
└── MOVESActivityOutput/
    ├── yearID=<y>/monthID=<m>/part.parquet
    └── …
```

| Table | Contents |
|-------|----------|
| `MOVESRun.parquet` | One row of run metadata — scale, units, `runHash`, calculator version |
| `MOVESOutput/` | Per-emission rows: `countyID`, `pollutantID`, `processID`, `emissionQuant` or `emissionRate`, … |
| `MOVESActivityOutput/` | Per-activity rows: `countyID`, `activityTypeID`, `activity`, … |

### Units

`emissionQuant`, `emissionRate`, and `activity` carry no implicit units.
Read the `massUnits`, `timeUnits`, `distanceUnits`, and `energyUnits` columns
of `MOVESRun.parquet` before converting or summing — the units are a property
of the run, set by the RunSpec's `[output]` section.

### `emissionQuant` vs `emissionRate`

A run operates in one of two modes:

* **Inventory mode** — `emissionQuant` (a mass) is populated; `emissionRate`
  is `null`.
* **Rates mode** — `emissionRate` is populated; `emissionQuant` is `null`.

Filter on `IS NOT NULL` for whichever column your run produces.

### `runHash`

`runHash` is the hex SHA-256 of the run's canonical inputs (RunSpec bytes,
default-DB content hashes, calculator-DAG hash). Identical inputs produce
the same `runHash` and byte-identical output — two runs with the same
`runHash` are the same run.

For the **full column-by-column schema reference** — every column in all
three tables, data types, and nullability — see **[`output-schema.md`](output-schema.md)**.

---

## Downstream tools

`moves.rs` output is standard Parquet. Any Parquet-capable tool reads it
directly — no MOVES-specific library needed.

**[`downstream-tools.md`](downstream-tools.md)** has copy-pasteable snippets
for:

* **Polars** (Python) — lazy scan and collect
* **pandas + PyArrow** (Python) — glob-based concat
* **DuckDB** — SQL queries including partition pruning
* **R (arrow + dplyr)** — `open_dataset` + `collect`
* **Apache Spark (PySpark)** — `recursiveFileLookup`

It also provides three complete worked analyses:

1. **NEI submission summary** — county × SCC × pollutant annual totals
2. **County-level inventory rollup** — mass in US tons, pandas + R variants
3. **Rates-mode CSV export** — deterministic flat CSV for SMOKE-MOVES

### Quick DuckDB example

```sql
-- In the duckdb CLI or from Python
SELECT countyID, pollutantID, sum(emissionQuant) AS total_g
FROM read_parquet('out/MOVESOutput/**/*.parquet')
WHERE emissionQuant IS NOT NULL
GROUP BY countyID, pollutantID
ORDER BY countyID, pollutantID;
```

### Extending the phase4-90 examples

[Task 90](downstream-tools.md) built the base loader patterns. Two useful
extensions for production workflows:

**Add human-readable labels.** The default-DB dimension tables
(`pollutant`, `emissionprocess`, `sourceusetype`, `roadtype`, …) are
Parquet files under `<output-root>/movesdb<YYYYMMDD>/<table>/`. Join them
on the numeric ID columns:

```python
import polars as pl

emissions = pl.scan_parquet("out/MOVESOutput/**/*.parquet")
pollutant  = pl.scan_parquet("default-db/movesdb20241112/pollutant/*.parquet")

labeled = (
    emissions
    .join(pollutant.select("pollutantID", "pollutantName"), on="pollutantID", how="left")
    .collect()
)
```

**Pool multiple runs.** Write several runs to separate directories, then
union them in DuckDB using the glob wildcard:

```sql
SELECT runHash, sum(emissionQuant) AS total_g
FROM read_parquet('runs/*/MOVESOutput/**/*.parquet')
WHERE emissionQuant IS NOT NULL
GROUP BY runHash;
```

`runHash` distinguishes runs without consulting `MOVESRun.parquet`.

---

## Tuning `--max-parallel-chunks`

`moves.rs` runs independent calculator chains in parallel using a
bounded-concurrency executor. `--max-parallel-chunks N` (default: 0,
meaning "use all available CPU cores") caps the number of chains running
concurrently.

**Memory model:**

```
peak_RSS ≈ process_baseline + N × max_chain_working_set
```

where `max_chain_working_set` is the largest peak DataFrame allocation any
single calculator chain requires. Setting `N` too high causes out-of-memory
kills; setting it too low leaves cores idle.

### Quick-tuning procedure

1. Run a representative fixture with `--max-parallel-chunks 1` and record
   peak RSS from the output line `peak RSS`.
2. Run the same fixture with `--max-parallel-chunks 4`. Check that peak
   RSS ≈ `baseline + 4 × working_set`.
3. Choose N so that predicted peak RSS stays under **50–70% of available
   RAM**, leaving headroom for the OS page cache and other processes.

### Recommended starting points

| Machine | RAM | Recommended N | Expected peak RSS |
|---------|-----|--------------|-------------------|
| Laptop | 8–16 GiB | 2–4 | 0.5–1.5 GiB |
| Workstation | 32–128 GiB | 8–16 | 1–4 GiB |
| Server | 256+ GiB | 32+ | 3–12 GiB |

These are soft estimates for county-scale runs once the Phase 4 data plane
is fully connected. In Phase 3 (pre-data-plane), per-chain working set is
essentially zero and N has no measurable effect on memory or throughput.

For the **full measurement methodology**, per-parallelism sweep results, and
phase-by-phase projections, see **[`concurrency-tuning.md`](concurrency-tuning.md)**.

---

## Command reference

### `moves run`

```
moves run --runspec <PATH> [--output <DIR>] [--max-parallel-chunks <N>]
          [--calculator-dag <PATH>] [--run-date-time <ISO8601>]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--runspec` | *(required)* | RunSpec file (`.xml`, `.mrs`, or `.toml`) |
| `--output` | `moves-output/` | Directory for output Parquet. Created if absent. |
| `--max-parallel-chunks` | 0 (all cores) | Maximum concurrent calculator chains. See [Tuning](#tuning---max-parallel-chunks). |
| `--calculator-dag` | *(embedded Phase 1 DAG)* | Override the calculator-chain dependency graph. |
| `--run-date-time` | *(unset)* | Override `runDateTime` in `MOVESRun.parquet`. Unset keeps output byte-stable. |

Exit codes: `0` success, `1` failure, `2` argument error.

### `moves import-cdb`

```
moves import-cdb --input <DIR> --output <DIR> [--default-db <DIR>]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--input` | *(required)* | Directory of `<TableName>.csv` files |
| `--output` | *(required)* | Directory for validated `<TableName>.parquet` files. Created if absent. |
| `--default-db` | *(none)* | Converted default-DB Parquet tree. Enables hard FK validation. Without it, FK checks are warnings. |

Exit codes: `0` success (even if tables are missing), `1` any table rejected.

### `moves convert-runspec`

```
moves convert-runspec --input <PATH> [--output <PATH>]
```

Converts a RunSpec between XML (`.xml`, `.mrs`) and TOML (`.toml`).
The output path defaults to the input path with the extension swapped.

```bash
# XML → TOML
moves convert-runspec --input my-run.xml
# → my-run.toml

# TOML → XML (for tools that require canonical MOVES XML)
moves convert-runspec --input my-run.toml
# → my-run.xml

# Explicit output path
moves convert-runspec --input my-run.xml --output /tmp/run.toml
```

### `moves-default-db-convert`

```
moves-default-db-convert --tsv-dir <DIR> --plan <PATH> --output <DIR>
                          --moves-db-version <LABEL>
                          [--require-every-table] [--generated-at-utc <ISO8601>]
```

Converts a MOVES default-DB TSV dump (from
`characterization/default-db-conversion/dump-default-db.sh`) to the Parquet
layout the runtime reads. See [Default database](#default-database).

---

## Regulatory notice

`moves.rs` is a research and analysis tool. It is **not validated for
regulatory submissions** — do not use it for State Implementation Plan (SIP)
filings, transportation-conformity analysis, or official National Emissions
Inventory submissions. Use EPA's canonical MOVES for regulatory work.
See migration-plan Task 129 (porting guide) for a full discussion of
behavioral divergences from canonical MOVES.

---

## See also

* [`runspec-toml.md`](runspec-toml.md) — full TOML RunSpec field reference
* [`output-schema.md`](output-schema.md) — complete Parquet output schema
* [`downstream-tools.md`](downstream-tools.md) — loading output in pandas, R, Polars, DuckDB, Spark
* [`control-strategies.md`](control-strategies.md) — AVFT, ROP, OnRoad/NONROAD Retrofit
* [`concurrency-tuning.md`](concurrency-tuning.md) — `--max-parallel-chunks` measurement and recommendations
* [`known-divergences.md`](known-divergences.md) — documented differences from canonical MOVES
* [`../moves-rust-migration-plan.md`](../moves-rust-migration-plan.md) — development roadmap
