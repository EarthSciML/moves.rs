# Default-DB partitioning plan

Phase 4 Task 79 deliverable. Per-table assignment of a Parquet partition
strategy for converting `movesdb20241112` to the columnar layout the
Phase 4 lazy-loading reader (Task 82) will consume. Drives Task 80 (the
conversion pipeline) and Task 81 (the round-trip validation).

The full machine-readable inventory is in [`tables.json`](tables.json).
This document explains the reasoning and lists the non-monolithic
assignments. Companion: [`README.md`](README.md) (regeneration,
estimator caveats).

## Headline numbers

* **243 distinct tables** in `CreateDefault.sql` + `CreateNRDefault.sql`
  at the pinned MOVES commit (after collapsing case-variant duplicates
  per `lower_case_table_names=1`).
* **221 monolithic** — single Parquet file per table.
* **4 schema-only** — empty in default DB; populated per-run.
* **11 partitioned by county/zone**, **2 by year × county**,
  **5 by model year**.
* No table is partitioned by year alone — every plausibly large
  year-keyed table also carries a geographic axis (so it lands in
  `year_x_county`) or carries a year *range* PK (`beginModelYearID`,
  `endModelYearID`) rather than a single-year column.

## Partition strategies

| Strategy | Layout | When to use |
|----------|--------|-------------|
| `monolithic`     | `default-db/movesdb20241112/<table>.parquet` | < 1M rows, or no natural partition column |
| `schema_only`    | `default-db/movesdb20241112/<table>.schema.json` (no Parquet) | ships empty; runtime materialises rows |
| `county`         | `default-db/movesdb20241112/<table>/county=<id>/part.parquet` | county/zone-dominated activity table |
| `year_x_county`  | `default-db/movesdb20241112/<table>/year=<y>/county=<id>/part.parquet` | both axes present and large |
| `model_year`     | `default-db/movesdb20241112/<table>/modelYear=<y>/part.parquet` | model-year-dominated rate table |

`zoneID` and `countyID` collapse 1:1 at the default scale, so the
`county` partition for a `zoneID`-keyed table uses the corresponding
`countyID` as the partition value (the conversion script joins through
the `Zone` table to map). `stateID`-only tables (NR allocation tables)
partition by `stateID` directly — the file count is 51 rather than
~3200, which is the right granularity for those allocation patterns.

## Selection rules

The classifier applies these rules in order (see
`audit-schema.py::_classify_partition`):

1. **`empty` bucket** → `schema_only`. Activity/output tables that ship
   empty in the default DB are not data-converted; the conversion
   pipeline records the schema only and the runtime populates rows.
2. **`tiny`/`small`/`medium` bucket** → `monolithic`. Below 1M rows a
   single Parquet file with column statistics is the simplest read path
   and predicate pushdown still prunes effectively.
3. **`large`/`huge` with both `yearID` and `countyID`/`zoneID`** →
   `year_x_county`. Both axes appear in MOVES filter clauses for these
   tables (e.g. `IMCoverage` joins on `(countyID, yearID, polProcessID)`).
4. **`large`/`huge` with `yearID` only** → `year`. Currently empty —
   every year-keyed large table also has a county axis.
5. **`large`/`huge` with `countyID`/`zoneID`/`stateID` only** →
   `county`. Most NR allocation tables and `*ActivityFraction` /
   `*Temperature` tables land here.
6. **`large`/`huge` with `modelYearID` only** → `model_year`. NR rate
   tables (`nrEmissionRate`, `nrCrankcaseEmissionRate`,
   `nrEvapEmissionRate`) carry model-year-by-SCC-by-HP keys.
7. **Otherwise** → `monolithic`, with a follow-up flag if the upper
   bound is `large`/`huge` (see below).

## Schema-only — populated at runtime

These tables exist in the default-DB DDL but ship empty. The conversion
pipeline records the schema (column names + types + primary key) so
downstream Rust code can validate inserts, but writes no Parquet data
file.

- `Link` — PK linkID
- `SHO` — PK hourDayID × monthID × yearID × ageID × linkID × sourceTypeID
- `SourceHours` — PK hourDayID × monthID × yearID × ageID × linkID × sourceTypeID
- `Starts` — PK hourDayID × monthID × yearID × ageID × zoneID × sourceTypeID

`SHO`, `SourceHours`, and `Starts` are the activity tables the
migration plan singled out as needing partitioning, but only in the
**execution** database — in the *default* database they are empty
shells. Their analogues in the execution DB will be written by the
runtime; Task 89 defines the output Parquet schema for those.

## County / zone (11)

- `AverageTankGasoline` — PK zoneID × fuelTypeID × fuelYearID × monthGroupID (`large`)
- `AverageTankTemperature` — PK tankTemperatureGroupID × zoneID × monthID × hourDayID × opModeID (`huge`)
- `ColdSoakInitialHourFraction` — PK sourceTypeID × zoneID × monthID × hourDayID × initialHourDayID (`huge`)
- `fuelUsageFraction` — PK countyID × fuelYearID × modelYearGroupID × sourceBinFuelTypeID × fuelSupplyFuelTypeID (`large`)
- `hotellingActivityDistribution` — PK zoneID × fuelTypeID × beginModelYearID × endModelYearID × opModeID (`large`)
- `nrMonthAllocation` — PK SCC × stateID × monthID (`large`, partitioned by `stateID`)
- `nrStateSurrogate` — PK surrogateID × stateID × countyID × surrogateYearID (`huge`)
- `nrUSMonthAllocation` — PK SCC × stateID × monthID (`large`, partitioned by `stateID`)
- `NRZoneAllocation` — PK surrogateID × stateID × zoneID (`large`)
- `regionCounty` — PK regionID × countyID × regionCodeID × fuelYearID (`huge`)
- `SoakActivityFraction` — PK sourceTypeID × zoneID × monthID × hourDayID × opModeID (`huge`)

The county-only group is dominated by activity tables whose values vary
geographically but not by year. Reading a per-run subset only needs
the counties referenced by the runspec, so per-county Parquet files
allow Polars to load the right slice via predicate pushdown.

## Year × county (2)

- `hotellingHours` — PK sourceTypeID × fuelTypeID × hourDayID × monthID × yearID × ageID × zoneID (`huge`)
- `IMCoverage` — PK polProcessID × countyID × yearID × sourceTypeID × fuelTypeID × IMProgramID (`huge`)

Both vary on the year and county axes simultaneously, and both have
upper-bound row counts large enough that Task 80 should partition
aggressively. `IMCoverage` is the MOVES I/M (Inspection / Maintenance)
program lookup — the reader will always filter to a single county +
year combination per chunk.

## Model year (5)

- `GREETManfAndDisposal` — PK GREETVehicleType × modelYearID × pollutantID × EmissionStage (`large`)
- `nrCrankcaseEmissionRate` — PK polProcessID × SCC × hpMin × hpMax × modelYearID × engTechID (`huge`)
- `nrEmissionRate` — PK polProcessID × SCC × hpMin × hpMax × modelYearID × engTechID (`huge`)
- `nrEngtechFraction` — PK SCC × hpMin × hpMax × modelYearID × processGroupID × engTechID (`huge`)
- `nrEvapEmissionRate` — PK polProcessID × SCC × hpMin × hpMax × modelYearID × engTechID (`huge`)

Nonroad rate tables key on `modelYearID` and the SCC inventory. The
default DB carries the full historical model-year window (1990 onward),
but a single MOVES run typically activates a 30-year subset. Partition
by `modelYearID` so the run loads only the relevant years.

## Large-monolithic re-review queue

These tables sort into the `large`/`huge` upper-bound bucket but have
no natural year/county/model-year axis to partition on — usually
because the PK is dominated by source-bin / process / pollutant
combinations that already prune most rows when MOVES filters by
`polProcessID`. The classifier leaves them monolithic.

Task 80 should **measure the actual row count** during conversion and
revisit any whose true count exceeds 50M:

- `ATRatio` — air-toxics fuel ratio
- `EmissionRateByAge`, `EmissionRateByAgeLEV`, `EmissionRateByAgeNLEV` — running emission rates by age group
- `evefficiency` — electric-vehicle efficiency by source / model-year range
- `FuelSupply`, `nrFuelSupply` — fuel-formulation market share by fuel region / month / year
- `fuelWizardFactors` — fuel parameter adjustment factors
- `IMFactor` — I/M correction factors
- `LinkHourVMTFraction` — VMT fraction by link / hour (link is empty in default DB; counts may collapse)
- `nratratio`, `nrhcspeciation` — NR HC speciation
- `nrRetrofitFactors` — NR retrofit programs
- `NRTransientAdjustFactor` — NR transient adjustment
- `onRoadRetrofit` — onroad retrofit programs
- `OpModeDistribution` — empty in default DB; runtime-populated
- `PMSpeciation` — PM speciation
- `SizeWeightFraction` — vehicle size/weight distribution

If the measured row count comes in below 1M for a table currently
flagged here, leave it monolithic. For tables that genuinely exceed
50M rows, the candidate alternatives are:

1. Partition by `polProcessID` range — the most common MOVES filter axis
   for rate tables. Use ~10 buckets so the file count stays bounded.
2. Partition by `sourceTypeID` (13 buckets) or `fuelTypeID` (5 buckets).
3. Use row-group statistics + sorted writes inside a single Parquet
   file — preferred when the count is borderline.

The conversion pipeline (Task 80) is the right place to make this
final call because it has the true row counts. This document captures
the schema-driven baseline; Task 80's actual measurements override.

## Monolithic small/medium (221)

The remaining 221 tables are dimension lookups (`SourceUseType`,
`FuelType`, `County`, `Year`, `EmissionProcess`, `Pollutant`, …),
small-cardinality cross-products (`SourceTypeAge`,
`PollutantProcessAssoc`, …), and medium fact tables whose upper bound
sits below the 1M-row partition threshold. All write as a single
`<table>.parquet` file.

The complete list is in `tables.json`; query examples:

```bash
# Tables in each strategy:
jq '.tables | group_by(.partition.strategy) | map({strategy: .[0].partition.strategy, count: length, tables: map(.name)})' \
    characterization/default-db-schema/tables.json

# Monolithic tables in the medium bucket (1k–1M upper bound) sorted by size:
jq '[.tables[] | select(.partition.strategy == "monolithic" and .size_bucket == "medium")]
    | sort_by(.estimated_rows_upper_bound) | reverse | .[].name' \
    characterization/default-db-schema/tables.json
```

## Open questions Task 80 should resolve

1. **Empty-by-default tables.** Confirm `SHO`, `SourceHours`, `Starts`,
   and `Link` ship with zero rows in `movesdb20241112` (the DDL implies
   so, but the dump is authoritative). If any of them ship populated,
   reclassify per measured size.
2. **`evefficiency` and `onRoadRetrofit`** carry `beginModelYearID` /
   `endModelYearID` range PKs. These encode piecewise-constant rules
   per model-year *range*, not per model-year *value* — partitioning
   by model year would replicate rows across buckets. Stay monolithic.
3. **`FuelSupply` partitioning.** The PK is `(fuelRegionID, fuelYearID,
   monthGroupID, fuelFormulationID)`. If real row count >10M, partition
   by `fuelYearID` — MOVES runs typically filter to a single fuel year.
4. **Run-populated tables outside `schema_only`.** Tables like
   `OpModeDistribution` (`linkID`-keyed) appear to be populated by the
   execution DB at runtime, not the default DB. Verify during Task 80
   and reclassify to `schema_only` if confirmed.
