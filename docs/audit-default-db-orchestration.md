# Audit: Generator Completeness + `moves run --default-db` Orchestration Design

*Bead: mo-znp.1 — Scopes B2 and B3 of epic mo-znp*

---

## 1. Generator Status Table

| Generator | Status | Scope | Key Outputs | Notes |
|-----------|--------|-------|-------------|-------|
| `TotalActivityGenerator` | **complete** | onroad | SourceTypeAgePopulation, TravelFraction, AnalysisYearVMT, AnnualVMTByAgeRoadway, VMTByAgeRoadwayHour, vmtByMYRoadHourFraction, SHOByAgeRoadwayHour, VMTByAgeRoadwayDay, IdleHoursByAgeHour, StartsByAgeHour, SHPByAgeHour | Reads 28 tables; writes 12 scratch tables. Per-link SHO allocation (separate kernel step) is the next phase. |
| `BaseRateGenerator` | **complete** | onroad | BaseRate, BaseRateByAge, DrivingIdleFraction | Reads 15 tables including RatesOpModeDistribution (from RatesOpModeDist gen + SourceTypePhysics). |
| `AverageSpeedOperatingModeDistributionGenerator` | **complete** | onroad | opModeDistribution (mesoscale path) | Reads link, avgSpeedBin, operatingMode, RunSpec tables. |
| `LinkOperatingModeDistributionGenerator` | **complete** | onroad | link-level opModeDistribution | Reads driveScheduleSecondLink, opModeDistribution (from AvgSpeed or Mesoscale gen). |
| `EvaporativeEmissionsOperatingModeDistributionGenerator` | **complete** | onroad | OpModeDistribution (evap) | Reads sourceHours, sho (from TotalActivity allocation), SoakActivityFraction (from TankTemp). |
| `MeteorologyGenerator` | **complete** | onroad | ZoneMonthHour (augmented: heatIndex, specificHumidity, molWaterFraction) | Reads ZoneMonthHour, Zone, County. Must run before TankTemp and TankFuel. |
| `NewTvvYearGenerator` | **complete** | onroad | stmyTVVCoeffs, stmyTVVEquations | Copies year-suffixed TVV tables from slow store to scratch. No input tables. |
| `OperatingModeDistributionGenerator` | **complete** | onroad | OpModeDistribution (default/non-project path) | Reads DriveSchedule, AvgSpeedDistribution, OperatingMode; alternative to AverageSpeed gen for non-project scale. |
| `RatesOperatingModeDistributionGenerator` | **complete** | onroad | RatesOpModeDistribution | Reads pollutantProcessAssoc, hotellingActivityDistribution, RunSpec tables. |
| `SourceBinDistributionGenerator` | **complete** | onroad | SourceBinDistribution, SBWeightedEmissionRate, SBWeightedEmissionRateByAge, SBWeightedDistanceRate | Reads 10+ tables including AVFT, PollutantProcessModelYear, SourceBin. |
| `SourceTypePhysics` | **complete** | onroad | RatesOpModeDistribution (corrected) | Rewrites RatesOpModeDistribution in place: drops negative polProcessID wildcards, applies sourceUseTypePhysicsMapping. Must run after RatesOpModeDist gen, before BaseRate gen. |
| `StartOperatingModeDistributionGenerator` | **complete** | onroad | startsOpModeDistribution, StartsByAgeHour | Reads SampleVehicleTrip, SampleVehicleDay, OperatingMode, HourDay, Link, RunSpec tables. |
| `TankFuelGenerator` | **complete** | onroad | AverageTankGasoline | Reads 14 tables: ZoneMonthHour (post-meteorology), FuelSupply, FuelFormulation, regionCounty. |
| `TankTemperatureGenerator` | **complete** | onroad | ColdSoakTankTemperature, AverageTankTemperature, SoakActivityFraction, ColdSoakInitialHourFraction | Reads 9 tables including ZoneMonthHour (post-meteorology). |
| `FuelEffectsGenerator` | **partial** | onroad | GeneralFuelRatio | Reads FuelFormulation, FuelSubtype, FuelSupply. **Missing**: ATRatio, criteriaRatio, MTBERatio (predictive/complex-model paths not yet ported). |
| `MesoscaleLookupTotalActivityGenerator` | **complete** | onroad | SHO, SourceHours (mesoscale path) | Alternative to TotalActivity + allocation for mesoscale; reads Year, SourceTypeYear, Link, etc. |
| `MesoscaleLookupOperatingModeDistributionGenerator` | **complete** | onroad | OpModeDistribution (mesoscale) | Alternative operating-mode path for mesoscale; reads DriveScheduleAssoc, AvgSpeedBin, etc. |
| `ProjectTAG` | **stub** | onroad | (empty) | Returns `CalculatorOutput::empty()`. The test asserts this explicitly (`execute_returns_empty_output_as_stub`). Needed for project-scale runs. |

**Nonroad generators** live in `crates/moves-nonroad/` and use their own Phase-5 simulation path (`run_simulation`), not the generator/calculator DAG framework. Out of scope for initial default-DB work.

---

## 2. Dependency-Ordered Execution Plan

### Phase 0: Open Default DB + Load Tables

```
DefaultDb::open(parquet_dir)
MergePlan::plan(&RunSpecFilters::from_runspec(runspec), &default_tables())
MergePlan::execute(&plan, &db)  →  InMemoryStore (slow_store)
```

`default_tables()` covers ~187 table specs. The result is an `InMemoryStore`
filtered to the RunSpec's geography/time/pollutant/process dimensions.

### Phase 1: Build RunSpec-Derived Tables

*These are not in the default DB; they must be synthesized from the RunSpec.*

```
build_runspec_tables(runspec) → Vec<(table_name, DataFrame)>
```

Tables to generate:
- `RunSpecSourceType` — one row per source type in RunSpec
- `RunSpecPollutantProcess` — one row per (pollutantID, processID) pair
- `RunSpecDay` — one row per day type (weekday/weekend)
- `RunSpecHourDay` — cross product of hours × day types
- `RunSpecMonth` — one row per month in RunSpec timespan
- `RunSpecYear` — one row per calendar year in RunSpec
- `RunSpecRoadType` — one row per road type
- `RunSpecMonthGroup` — derived from RunSpec months
- `RunSpecSourceFuelType` — RunSpec source types × fuel types (needs FuelType from DB)

Insert into slow_store so generators find them via `ctx.tables()`.

### Phase 2: Synthesize Link Table

*Link is schema-only in the default DB. Must be built from zone/road-type geometry.*

```
link_from_zone_road_type(zone_road_type_df, road_type_df) → Link DataFrame
```

MOVES convention: one Link row per (county, zone, road-type) combination.
`zoneID = countyID * 10` for default zones. `linkID = zoneID * 100 + roadTypeID`
(or use same convention Java uses). Insert into slow_store.

Then build `GeographyTables` from Link + County:
```
GeographyTables::new(links, counties)
engine.execution.build_execution_locations(&geography)
```

### Phase 3: Run Generators (in dependency order)

Generators execute inside the engine's master loop per (county, zone, road-type, month, day, hour) chunk. The order below reflects data dependencies:

```
Step 1 (parallel — no generator-scratch dependencies):
  MeteorologyGenerator         reads: ZoneMonthHour, Zone, County
                               writes: ZoneMonthHour (augmented)
  NewTvvYearGenerator          reads: (slow_store TVV tables)
                               writes: stmyTVVCoeffs, stmyTVVEquations
  RatesOperatingModeDistributionGenerator
                               reads: pollutantProcessAssoc, RunSpec tables
                               writes: RatesOpModeDistribution

Step 2 (after Meteorology):
  TankTemperatureGenerator     reads: ZoneMonthHour✓, SampleVehicleDay/Trip
                               writes: ColdSoakTankTemperature, AverageTankTemperature,
                                       SoakActivityFraction, ColdSoakInitialHourFraction
  TankFuelGenerator            reads: ZoneMonthHour✓, FuelSupply, AverageTankGasoline
                               writes: AverageTankGasoline (refined)

Step 3 (after RatesOpModeDist):
  SourceTypePhysics            reads: sourceUseTypePhysicsMapping, RatesOpModeDistribution✓
                               writes: RatesOpModeDistribution (corrected)

Step 4 (parallel):
  TotalActivityGenerator       reads: Year, SourceTypeYear, SourceTypeAgeDistribution, ...
                               writes: SourceTypeAgePopulation, TravelFraction,
                                       SHOByAgeRoadwayHour, etc. (12 tables)
     [Mesoscale alternative: MesoscaleLookupTotalActivityGenerator → SHO, SourceHours]
  SourceBinDistributionGenerator
                               reads: AVFT, PollutantProcessModelYear, SourceBin, ...
                               writes: SBWeightedEmissionRate, SBWeightedEmissionRateByAge,
                                       SBWeightedDistanceRate
  OperatingModeDistributionGenerator  (OR AverageSpeedOMDG)
                               reads: DriveSchedule, AvgSpeedBin, OperatingMode
                               writes: OpModeDistribution

Step 5 (after TotalActivity allocation → SHO/SourceHours):
  EvaporativeEmissionsOperatingModeDistributionGenerator
                               reads: sourceHours✓, sho✓, SoakActivityFraction✓
                               writes: OpModeDistribution (evap)
  LinkOperatingModeDistributionGenerator
                               reads: opModeDistribution✓, driveScheduleSecondLink, link
                               writes: link-level opModeDistribution
  StartOperatingModeDistributionGenerator
                               reads: SampleVehicleTrip, SampleVehicleDay, Link, ...
                               writes: startsOpModeDistribution

Step 6 (after SourceBinDist + RatesOpModeDist + SourceTypePhysics):
  BaseRateGenerator            reads: RatesOpModeDistribution✓, SBWeightedEmissionRate✓,
                                       SBWeightedDistanceRate✓
                               writes: BaseRate, BaseRateByAge, DrivingIdleFraction

Step 7 (fuel effects, mostly parallel with BaseRate):
  FuelEffectsGenerator         reads: FuelFormulation, FuelSubtype, FuelSupply
                               writes: GeneralFuelRatio
```

### Phase 4: Run Calculators

The existing 63-module / 960-pair calculator DAG runs unchanged — it reads
from the same scratch namespace the generators populated. No changes needed
here for the initial default-DB path.

### Phase 5: Output

Same as current snapshot path: Parquet output under `<output>/`. Aggregation
plan is built from RunSpec, identical to existing implementation.

---

## 3. Default-DB Importer Requirements

### What `InputDataManager` already provides

`MergePlan::execute(&plan, &db)` (implemented, non-wasm only) produces an
`InMemoryStore` with ~187 default-DB tables filtered to the RunSpec's
geography/time/pollutant/process dimensions. This covers:

- **Geography**: County, Zone, State, Region tables — filtered by RunSpec counties/states
- **Time**: Year, MonthOfAnyYear, DayOfAnyWeek, HourOfAnyDay — filtered by RunSpec timespan
- **Pollutant/process**: EmissionRate, EmissionRateByAge, BaseRate source data — filtered by polProcessID
- **Source types**: SourceUseType, SourceTypeYear, AVFT — filtered by sourceTypeID
- **Road types**: RoadType, DriveSchedule — unfiltered (small tables)
- **Fuel**: FuelSupply, FuelFormulation, FuelSubtype — filtered by fuelYearID/fuelTypeID
- **Activity**: AvgSpeedDistribution, ZoneMonthHour, SampleVehicleDay/Trip, etc.

### What the importer does NOT yet provide

| Gap | Impact | Where needed |
|-----|--------|-------------|
| **RunSpec-derived tables** (RunSpecSourceType, RunSpecPollutantProcess, RunSpecDay, RunSpecHourDay, RunSpecMonth, RunSpecYear, RunSpecRoadType, RunSpecMonthGroup, RunSpecSourceFuelType) | **Blocking** — every generator reads ≥1 RunSpec table | New `build_runspec_tables(runspec)` function |
| **Link table** (default DB is schema-only) | **Blocking** — needed for GeographyTables and most generators | Synthesize from ZoneRoadType + RoadType |
| **HourDayIDs** (from DB lookup) | Non-blocking for county-scale | `InputDataManager` has TODO for this builder |
| **Non-road source-type lookup** | Non-blocking for initial scope | `InputDataManager` has TODO |
| **Fuel-subtype derivation** | Non-blocking if using FuelSubtype directly | `InputDataManager` has TODO |

### Output format

`InMemoryStore` — same as the snapshot path. The engine's `with_slow_store(store)` API is already the integration point.

### Scope of a RunSpec for default-DB

| Dimension | Scope | Resolution |
|-----------|-------|-----------|
| Geography | County-scale (single county or multi-county) | `ZoneRoadType` → synthesized Link; one zone per county |
| Time | Any combination of years/months/days/hours in RunSpec | `RunSpecFilters` handles all MOVES time dimensions |
| Pollutants/processes | Any in RunSpec PollutantProcessList | Filtered via `polProcessID` in MergePlan |
| Source types | Any in RunSpec SourceUseTypeIDs | Filtered via `sourceTypeID` |
| Road types | Any in RunSpec RoadTypes | Unfiltered (all road types in DB are small) |

---

## 4. Blockers

### B1 (Large) — Link table synthesis

**What's missing**: The default DB marks `Link`, `SHO`, `SourceHours`, `Starts` as
schema-only (no rows). `GeographyTables::new(links, counties)` requires `LinkRow`
data to drive the engine's iteration over execution locations. Without Link rows the
engine has zero chunks to process.

**Fix**: Add a `populate_link_from_zone_road_type(store: &mut InMemoryStore) -> Result<()>`
function in `run.rs` (analogous to `populate_source_use_type_physics_mapping` and
`populate_sho_distances`).
Use ZoneRoadType + RoadType from the default DB. Assign synthetic linkIDs via
`zoneID * 10 + roadTypeID`. This is the same convention MOVES uses for
county-scale runs without a custom project network.

**Blocks**: All of `moves run --default-db`. Estimated: **medium** (1–2 days).

### B2 (Medium) — RunSpec-derived table building

**What's missing**: No function converts a `RunSpec` into the set of
`RunSpec*` DataFrames that generators read (RunSpecSourceType,
RunSpecPollutantProcess, RunSpecDay, RunSpecHourDay, RunSpecMonth,
RunSpecYear, RunSpecRoadType, RunSpecMonthGroup, RunSpecSourceFuelType).
Currently these exist only in snapshots (captured from Java's
ExecutionRunSpec).

**Fix**: New `build_runspec_tables(runspec: &RunSpec, db: &InMemoryStore) -> InMemoryStore`
function. Most tables are simple projections of the RunSpec fields. A few
(RunSpecSourceFuelType, RunSpecMonthGroup) need a join with default-DB lookup
tables (FuelSubtype, MonthOfAnyYear).

**Blocks**: All generators that read RunSpec tables (most of them).
Estimated: **medium** (2–3 days).

### B3 (Small) — CLI `--default-db` flag on `moves run`

**What's missing**: `RunOptions` has no `default_db` field. The `cmd_run`
dispatch in `main.rs` has no branch for default-DB loading.

**Fix**: Add `default_db: Option<PathBuf>` to `RunOptions`; add
`--default-db` to the `clap` `Run` variant; add a new execution branch
in `run_simulation` that calls `MergePlan::execute` + `build_runspec_tables`
+ Link synthesis + `engine.with_slow_store(store)`. The existing snapshot path
stays unchanged.

**Blocks**: User-facing entry point. Estimated: **small** (0.5 day, gated on B1+B2).

### B4 (Medium) — TotalActivity → allocation → SHO wiring

**What's missing**: `TotalActivityGenerator::execute` writes `SHOByAgeRoadwayHour`
to scratch, but the per-link `SHO`, `SourceHours`, and `Starts` tables (consumed
by EvapOMDGenerator and StartOMDGenerator) require an additional **allocation kernel**
step that distributes zone-level activity to individual links. In the snapshot
path these tables come from the captured execution DB; for default-DB they must be
computed.

The `totalactivitygenerator::allocation` submodule exists but its integration into
the master loop for the default-DB path is not wired. Check whether
`MesoscaleLookupTotalActivityGenerator` (which directly produces SHO/SourceHours)
is usable as the county-scale alternative.

**Blocks**: `EvaporativeEmissionsOperatingModeDistributionGenerator`,
`StartOperatingModeDistributionGenerator` (they read `sho`/`sourceHours`).
Estimated: **medium** (1–2 days to validate and wire).

### B5 (Small) — FuelEffectsGenerator partial coverage

**What's missing**: ATRatio, criteriaRatio, MTBERatio paths (predictive/complex model).
The implemented `generalFuelRatio` path is sufficient for criteria pollutants.

**Blocks**: Some pollutant sub-types (sulfur/benzene complex model paths).
Does not block the core CO/HC/NOx/PM calculation. Estimated: **small** (1 day).

### B6 (Small) — ProjectTAG stub

**What's missing**: `ProjectTAG::execute` returns `CalculatorOutput::empty()`.
This blocks project-scale runs (not needed for county-scale default-DB).

**Blocks**: Project-scale runs only. Estimated: **small** (1 day).

### B7 (None for B2/B3 scope) — Nonroad

Nonroad uses a separate `run_simulation` path in `crates/moves-nonroad/` and is out
of scope for `moves run --default-db` (which targets onroad). No action needed.

---

## 5. Summary

All 17 onroad generators under `crates/moves-calculators/src/generators/` have real
Polars/Rust `execute()` implementations — 15 are fully functional, 1 is partial
(FuelEffectsGenerator missing some ratio paths), and 1 is a stub (ProjectTAG). All
17 are registered in `moves_calculators::register_all` and would run in the engine
today against a snapshot. The "Phase 2 placeholder" comments in module headers are
outdated: Task 50 (`DataFrameStore` / `InMemoryStore`) is complete and the generators
read/write correctly through `CalculatorContext`.

The critical gap for B2 (`moves run --default-db`) is **not** in the generators
themselves but in the data-loading pipeline upstream of them. Two new pieces of
infrastructure are needed: (1) a function that synthesizes the `Link` table from
`ZoneRoadType` + `RoadType` (since the default DB ships Link as schema-only), and
(2) a function that builds the RunSpec-derived tables (`RunSpecSourceType`,
`RunSpecPollutantProcess`, `RunSpecDay`, `RunSpecHourDay`, etc.) from the
`RunSpec` struct. `InputDataManager::MergePlan::execute` (Task 24) is already
complete and can load all default-DB tables filtered to the RunSpec. Once B1 and B2
are resolved, wiring the CLI flag (B3) is straightforward. B4 (SHO allocation
wiring) needs validation but may be avoidable via `MesoscaleLookupTotalActivityGenerator`.

Recommended B2/B3 implementation order:
1. `populate_link_from_zone_road_type(store)` in `run.rs` — unblocks geography setup
2. `build_runspec_tables(runspec, db)` in `run.rs` or new module — unblocks generators
3. `--default-db` CLI flag + new execution branch in `run_simulation` — user-facing
4. Validate SHO allocation or switch to MesoscaleLookup path — unblocks evap/start OMD

This scoping covers B2 (native `moves run --default-db`) entirely. B3
(wasm/in-browser) builds on B2 but requires additional wasm-compat passes for
the Parquet I/O (currently gated on `#[cfg(not(target_arch = "wasm32"))]`).
