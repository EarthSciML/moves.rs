//! Port of `CH4N2ORunningStartCalculator.java` and
//! `database/CH4N2ORunningStartCalculator.sql` —.//!.
//!
//! `CH4N2ORunningStartCalculator` is the legacy scripted-SQL calculator for
//! the engine-running and engine-start exhaust greenhouse-gas pollutants.
//! Despite the `CH4N2O` name, at the pin it computes **only nitrous oxide**
//! (N2O, pollutant 6): the Java constructor's methane (`CH4`, pollutant 5)
//! `register` calls are commented out, so the live calculator covers N2O on
//! Running Exhaust (process 1) and Start Exhaust (process 2).
//!
//! # Superseded by `BaseRateCalculator`
//!
//! This calculator is **not wired into the pinned MOVES runtime**.
//! `CalculatorInfo.txt` — the runtime registration file — has no
//! `Registration` directive for `CH4N2ORunningStartCalculator`: N2O Running
//! Exhaust `(6, 1)` and N2O Start Exhaust `(6, 2)` are registered to
//! `BaseRateCalculator` instead (`CalculatorInfo.txt` lines 505–506).
//! `characterization/calculator-chains/calculator-dag.json` records
//! `registrations_count: 0` to match. The base-rate approach
//! (, `BaseRateCalculator`) superseded the older
//! per-pollutant scripted-SQL calculators like this one.
//!
//! The still lists the class as a task, so this
//! module ports the **algorithm** faithfully for reference and for
//! cross-validation against `BaseRateCalculator`. To stay consistent with
//! the runtime, [`registrations`](Calculator::registrations) returns an
//! empty slice — the registry must not double-register `(6, 1)`/`(6, 2)`.
//!
//! # What it computes
//!
//! `CH4N2ORunningStartCalculator.sql` has two mutually exclusive
//! `-- Section` blocks; the Java `doExecute` enables exactly one per
//! master-loop context, by process:
//!
//! * **Running Exhaust** — emission `= SHO × Σ(sourceBinActivityFraction ×
//! meanBaseRate)`, where `SHO` is the source-hours-operating activity and
//! the inner sum runs over the running operating-mode emission rates
//! (`opModeID ∈ [0, 100)`).
//! * **Start Exhaust** — emission `= Σ(sourceBinActivityFraction × starts ×
//! meanBaseRate)`, where `starts` is the engine-start activity and
//! `meanBaseRate` is the single start operating-mode rate
//! (`opModeID == 100`).
//!
//! Both reduce to `activity × source-bin-weighted base rate`. The port
//! splits them into [`calculate_running`](Ch4N2oRunningStartCalculator::calculate_running)
//! and [`calculate_start`](Ch4N2oRunningStartCalculator::calculate_start).
//!
//! # Running Exhaust algorithm
//!
//! [`calculate_running`](Ch4N2oRunningStartCalculator::calculate_running)
//! ports the SQL's `-- Section Running Exhaust` of "Processing":
//!
//! | SQL working table | This port |
//! |-------------------|-----------|
//! | `EmissionRate2` | `EmissionRate ⋈ PollutantProcessAssoc`, kept for `pollutantID == 6` and `opModeID ∈ [0, 100)`, indexed `(polProcessID, sourceBinID) → [meanBaseRate]` |
//! | `EmissionRate3` | `(sourceTypeModelYearID, fuelTypeID, pollutantID, processID) → Σ sourceBinActivityFraction × meanBaseRate` |
//! | `SHO2`, `Link2`, `SHO3` | folded into the per-`SHO`-row join |
//! | `WorkerOutputBySourceType` | the per-`(SHO, EmissionRate3)` join |
//! | `MOVESWorkerOutputTemp` | the returned `Vec<`[`EmissionRow`]`>` |
//!
//! # Start Exhaust algorithm
//!
//! [`calculate_start`](Ch4N2oRunningStartCalculator::calculate_start) ports
//! the SQL's `-- Section Start Exhaust` — a single eleven-table join with a
//! `GROUP BY` over fourteen dimension columns. The port reproduces it as a
//! nested join loop accumulating into a `HashMap` keyed by the fourteen
//! dimensions, then sorts the result.
//!
//! Engine starts are zone-level activity, so the SQL joins `starts` to
//! `Link` through `zoneID` — and a zone carries one link per road type.
//! MOVES avoids multiplying each start across every link by only ever
//! extracting the **off-network** link (the `doesProcessContext` /
//! `doExecute` context filter, see [`processes_context`]). The port folds
//! that into [`calculate_start`](Ch4N2oRunningStartCalculator::calculate_start):
//! only off-network links (`roadTypeID == 1`) join, so the pure compute is
//! correct for any input.
//!
//! [`processes_context`]: Ch4N2oRunningStartCalculator::processes_context
//!
//! Every join in both sections is an `INNER JOIN`; a row with no match on
//! the join key is dropped, which the port reproduces with map lookups that
//! `continue` on a miss.
//!
//! # Fidelity notes
//!
//! `CH4N2ORunningStartCalculator.sql` writes the running-exhaust
//! `EmissionRate3.sbafXmbr` to a `FLOAT` (32-bit) column and the final
//! `MOVESWorkerOutputTemp.emissionQuant` to a `FLOAT`, while MariaDB
//! evaluates the arithmetic in `DOUBLE`. This port sums and multiplies in
//! `f64` end to end, so it does not reproduce the `f32` truncation MOVES
//! applies when storing those values — a sub-`1e-7` relative drift.
//! `SHO`, `starts`, `meanBaseRate` and `sourceBinActivityFraction` are
//! `FLOAT` columns too, but they are model *inputs* — already
//! `f32`-quantised before the calculator sees them. Reproducing the
//! intermediate truncation bug-for-bug is the/74
//! calculator-integration-validation call, matching the
//! `DistanceCalculator` precedent. There are no integer/integer literal
//! divisions in the SQL, so the MariaDB `div_precision_increment` rounding
//! gotcha does not arise.
//!
//! # Data plane
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are placeholders until the
//! `DataFrameStore` lands (), so `execute` cannot yet
//! read the input tables nor emit `MOVESWorkerOutput`. The numeric
//! algorithm is fully ported and unit-tested on
//! [`calculate_running`](Ch4N2oRunningStartCalculator::calculate_running)
//! and [`calculate_start`](Ch4N2oRunningStartCalculator::calculate_start);
//! `execute` is a documented shell returning an empty [`CalculatorOutput`].

use rustc_hash::FxHashMap;
use std::collections::HashSet;

use moves_calculator_info::{Granularity, Priority};
use moves_data::{PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// Stable module name — matches the Java class and the
/// `CH4N2ORunningStartCalculator` entry in the calculator-chain DAG.
const CALCULATOR_NAME: &str = "CH4N2ORunningStartCalculator";

/// Running Exhaust process — `EmissionProcess` row 1.
const RUNNING_EXHAUST: ProcessId = ProcessId(1);

/// Start Exhaust process — `EmissionProcess` row 2.
const START_EXHAUST: ProcessId = ProcessId(2);

/// Nitrous oxide — `Pollutant` row 6. The Java constructor's methane
/// (`CH4`, pollutant 5) `register` calls are commented out, so the live
/// calculator only handles N2O; the SQL filters `ppa.pollutantID = 6` in
/// both sections.
const N2O_POLLUTANT_ID: i32 = 6;

/// Lowest running-exhaust operating-mode id — the SQL keeps `EmissionRate`
/// rows with `opModeID >= 0` for the running section.
const RUNNING_OP_MODE_MIN: i32 = 0;

/// Operating-mode id past the running-exhaust range — the SQL keeps
/// `EmissionRate` rows with `opModeID < 100` for the running section.
/// Operating-mode `100` is the start bin and is excluded.
const RUNNING_OP_MODE_MAX: i32 = 100;

/// Start-exhaust operating-mode id — the SQL keeps `EmissionRate` rows with
/// `opModeID = 100` for the start section.
const START_OP_MODE_ID: i32 = 100;

/// Off-network road type — `RoadType` row 1, `"Off-Network"`. The Java
/// `doesProcessContext` runs Start Exhaust only here; off-network "links"
/// model parking, where engine starts occur.
const OFF_NETWORK_ROAD_TYPE_ID: i32 = 1;

// ===========================================================================
// Input tables — plain Rust mirrors of the tables the SQL's "Extract Data"
// section pulls. Following the convention every `INT`/`SMALLINT`
// identifier is an `i32`, `sourceBinID` (`BIGINT`) is an `i64`, and every
// `FLOAT`/`DOUBLE` quantity is an `f64`. Only the columns the running and
// start algorithms read are modelled.
// ===========================================================================

/// One `SourceBin` row — the engine/fuel decomposition of a source bin.
/// Only `fuelTypeID` is read here (unlike the distance calculator,
/// which also reads `regClassID`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceBinRow {
 /// `sourceBinID` — `BIGINT` primary key.
    pub source_bin_id: i64,
 /// `fuelTypeID` — fuel type.
    pub fuel_type_id: i32,
}

/// One `SourceBinDistribution` row — a source bin's share of a
/// `(sourceTypeModelYear)` group's activity for one `polProcessID`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceBinDistributionRow {
 /// `sourceTypeModelYearID` — surrogate key for a `(sourceType, modelYear)`.
    pub source_type_model_year_id: i32,
 /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
 /// `sourceBinID` — joins to [`SourceBinRow::source_bin_id`].
    pub source_bin_id: i64,
 /// `sourceBinActivityFraction` — the bin's share of the group's activity.
    pub source_bin_activity_fraction: f64,
}

/// One `SourceTypeModelYear` row — resolves a `sourceTypeModelYearID`
/// surrogate key into its `(sourceTypeID, modelYearID)` components.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceTypeModelYearRow {
 /// `sourceTypeModelYearID` — the surrogate key.
    pub source_type_model_year_id: i32,
 /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
 /// `modelYearID` — vehicle model year.
    pub model_year_id: i32,
}

/// One `EmissionRate` row — a base emission rate for one
/// `(polProcessID, sourceBinID, opModeID)` triple.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmissionRateRow {
 /// `polProcessID` — `pollutantID * 100 + processID`.
    pub pol_process_id: i32,
 /// `sourceBinID` — joins to [`SourceBinRow::source_bin_id`].
    pub source_bin_id: i64,
 /// `opModeID` — operating mode; `[0, 100)` is running, `100` is start.
    pub op_mode_id: i32,
 /// `meanBaseRate` — the base emission rate. `FLOAT` in MOVES.
    pub mean_base_rate: f64,
}

/// One `PollutantProcessAssoc` row — resolves a `polProcessID` into its
/// `(pollutantID, processID)` components.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollutantProcessAssocRow {
 /// `polProcessID` — the surrogate key.
    pub pol_process_id: i32,
 /// `pollutantID` — the pollutant half.
    pub pollutant_id: i32,
 /// `processID` — the process half.
    pub process_id: i32,
}

/// One `EmissionProcess` row — only `processID` is read. The start-exhaust
/// SQL joins `EmissionProcess` purely to gate `ppa.processID` to the
/// extracted process(es).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmissionProcessRow {
 /// `processID` — the process primary key.
    pub process_id: i32,
}

/// One `HourDay` row — the `hourDayID` → `(dayID, hourID)` split.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HourDayRow {
 /// `hourDayID` — the surrogate key.
    pub hour_day_id: i32,
 /// `dayID` — day-of-week type.
    pub day_id: i32,
 /// `hourID` — hour of day.
    pub hour_id: i32,
}

/// One `Link` row — a road link's geography and road type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkRow {
 /// `linkID` — the link primary key.
    pub link_id: i32,
 /// `countyID` — joins to [`CountyRow::county_id`].
    pub county_id: i32,
 /// `zoneID` — the zone the link belongs to.
    pub zone_id: i32,
 /// `roadTypeID` — road type; `1` is off-network.
    pub road_type_id: i32,
}

/// One `County` row — supplies the `stateID` for a county.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CountyRow {
 /// `countyID` — the county primary key.
    pub county_id: i32,
 /// `stateID` — the state the county belongs to.
    pub state_id: i32,
}

/// One `Zone` row — supplies the `countyID` for a zone.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZoneRow {
 /// `zoneID` — the zone primary key.
    pub zone_id: i32,
 /// `countyID` — joins to [`CountyRow::county_id`].
    pub county_id: i32,
}

/// One `SHO` (Source Hours Operating) row — running-exhaust activity for a
/// `(hourDay, month, year, age, link, sourceType)` cell.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShoRow {
 /// `hourDayID` — joins to [`HourDayRow::hour_day_id`].
    pub hour_day_id: i32,
 /// `monthID` — calendar month.
    pub month_id: i32,
 /// `yearID` — calendar year.
    pub year_id: i32,
 /// `ageID` — vehicle age in years; `modelYearID = yearID - ageID`.
    pub age_id: i32,
 /// `linkID` — joins to [`LinkRow::link_id`].
    pub link_id: i32,
 /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
 /// `SHO` — source hours operating. `FLOAT` in MOVES.
    pub sho: f64,
}

/// One `Starts` row — start-exhaust activity for a `(hourDay, month, year,
/// age, zone, sourceType)` cell. Engine starts are zone-level, not link-level.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StartsRow {
 /// `hourDayID` — joins to [`HourDayRow::hour_day_id`].
    pub hour_day_id: i32,
 /// `monthID` — calendar month.
    pub month_id: i32,
 /// `yearID` — calendar year.
    pub year_id: i32,
 /// `ageID` — vehicle age in years; `modelYearID = yearID - ageID`.
    pub age_id: i32,
 /// `zoneID` — the zone the starts occur in.
    pub zone_id: i32,
 /// `sourceTypeID` — MOVES source (vehicle) type.
    pub source_type_id: i32,
 /// `starts` — number of engine starts. `FLOAT` in MOVES.
    pub starts: f64,
}

/// Inputs to [`Ch4N2oRunningStartCalculator::calculate_running`] — the
/// tables the SQL's "Extract Data" section produces for the
/// `-- Section Running Exhaust` processing, as plain row vectors.
///
/// A future (`DataFrameStore`) wiring populates this from the
/// per-run filtered execution database; until then it is the explicit
/// data-plane contract the unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct RunningExhaustInputs {
 /// `SHO` rows — the running-exhaust activity.
    pub sho: Vec<ShoRow>,
 /// `HourDay` rows.
    pub hour_day: Vec<HourDayRow>,
 /// `Link` rows.
    pub link: Vec<LinkRow>,
 /// `County` rows.
    pub county: Vec<CountyRow>,
 /// `SourceBin` rows.
    pub source_bin: Vec<SourceBinRow>,
 /// `SourceBinDistribution` rows.
    pub source_bin_distribution: Vec<SourceBinDistributionRow>,
 /// `SourceTypeModelYear` rows.
    pub source_type_model_year: Vec<SourceTypeModelYearRow>,
 /// `EmissionRate` rows.
    pub emission_rate: Vec<EmissionRateRow>,
 /// `PollutantProcessAssoc` rows.
    pub pollutant_process_assoc: Vec<PollutantProcessAssocRow>,
}

/// Inputs to [`Ch4N2oRunningStartCalculator::calculate_start`] — the tables
/// the SQL's "Extract Data" section produces for the
/// `-- Section Start Exhaust` processing, as plain row vectors.
///
/// A future (`DataFrameStore`) wiring populates this from the
/// per-run filtered execution database; until then it is the explicit
/// data-plane contract the unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct StartExhaustInputs {
 /// `Starts` rows — the start-exhaust activity.
    pub starts: Vec<StartsRow>,
 /// `HourDay` rows.
    pub hour_day: Vec<HourDayRow>,
 /// `Link` rows.
    pub link: Vec<LinkRow>,
 /// `County` rows.
    pub county: Vec<CountyRow>,
 /// `Zone` rows.
    pub zone: Vec<ZoneRow>,
 /// `SourceBin` rows.
    pub source_bin: Vec<SourceBinRow>,
 /// `SourceBinDistribution` rows.
    pub source_bin_distribution: Vec<SourceBinDistributionRow>,
 /// `SourceTypeModelYear` rows.
    pub source_type_model_year: Vec<SourceTypeModelYearRow>,
 /// `EmissionRate` rows.
    pub emission_rate: Vec<EmissionRateRow>,
 /// `PollutantProcessAssoc` rows.
    pub pollutant_process_assoc: Vec<PollutantProcessAssocRow>,
 /// `EmissionProcess` rows.
    pub emission_process: Vec<EmissionProcessRow>,
}

/// One `MOVESWorkerOutput` row produced by the running or start calculation.
///
/// The fourteen integer columns are the SQL's `GROUP BY` dimensions;
/// `emission_quant` carries the emission total. The SQL also writes an
/// `SCC` column — left to the output wiring, as it is not an
/// algorithm input.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EmissionRow {
 /// `yearID`.
    pub year_id: i32,
 /// `monthID`.
    pub month_id: i32,
 /// `dayID`.
    pub day_id: i32,
 /// `hourID`.
    pub hour_id: i32,
 /// `stateID`.
    pub state_id: i32,
 /// `countyID`.
    pub county_id: i32,
 /// `zoneID`.
    pub zone_id: i32,
 /// `linkID`.
    pub link_id: i32,
 /// `pollutantID` — always `6` (nitrous oxide, N2O).
    pub pollutant_id: i32,
 /// `processID` — `1` (Running Exhaust) or `2` (Start Exhaust).
    pub process_id: i32,
 /// `sourceTypeID`.
    pub source_type_id: i32,
 /// `fuelTypeID`.
    pub fuel_type_id: i32,
 /// `modelYearID`.
    pub model_year_id: i32,
 /// `roadTypeID`.
    pub road_type_id: i32,
 /// `emissionQuant` — the emission total for this dimension cell.
    pub emission_quant: f64,
}

impl EmissionRow {
 /// The integer dimension tuple — every column except `emission_quant`,
 /// in `MOVESWorkerOutputTemp` column order. Used both to sort the output
 /// deterministically (MOVES leaves `MOVESWorkerOutput` physically
 /// unordered) and as the `GROUP BY` key of the start-exhaust aggregation.
    fn dimension_key(&self) -> [i32; 14] {
        [
            self.year_id,
            self.month_id,
            self.day_id,
            self.hour_id,
            self.state_id,
            self.county_id,
            self.zone_id,
            self.link_id,
            self.pollutant_id,
            self.process_id,
            self.source_type_id,
            self.fuel_type_id,
            self.model_year_id,
            self.road_type_id,
        ]
    }

 /// Rebuild a row from a [`dimension_key`](Self::dimension_key) tuple and
 /// its emission total — the inverse of `dimension_key`, used to turn the
 /// start-exhaust `GROUP BY` accumulator back into rows.
    fn from_dimension_key(key: [i32; 14], emission_quant: f64) -> Self {
        Self {
            year_id: key[0],
            month_id: key[1],
            day_id: key[2],
            hour_id: key[3],
            state_id: key[4],
            county_id: key[5],
            zone_id: key[6],
            link_id: key[7],
            pollutant_id: key[8],
            process_id: key[9],
            source_type_id: key[10],
            fuel_type_id: key[11],
            model_year_id: key[12],
            road_type_id: key[13],
            emission_quant,
        }
    }
}

// ===========================================================================
// Data-plane wiring — TableRow impls + build_running_inputs/build_start_inputs.
// Pattern mirrors the bucket-A pilot in so2_calculator.rs.
// ===========================================================================

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

impl TableRow for ShoRow {
    fn table_name() -> &'static str {
        "SHO"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("SHO".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "SHO".into(),
                    rows.iter().map(|r| r.sho).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SHO";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let hour_day = get_i32("hourDayID")?;
        let month = get_i32("monthID")?;
        let year = get_i32("yearID")?;
        let age = get_i32("ageID")?;
        let link = get_i32("linkID")?;
        let src_type = get_i32("sourceTypeID")?;
        let sho = get_f64("SHO")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ShoRow {
                    hour_day_id: hour_day.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month.get(i).ok_or_else(|| null("monthID"))?,
                    year_id: year.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age.get(i).ok_or_else(|| null("ageID"))?,
                    link_id: link.get(i).ok_or_else(|| null("linkID"))?,
                    source_type_id: src_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    sho: sho.get(i).ok_or_else(|| null("SHO"))?,
                })
            })
            .collect()
    }
}

impl TableRow for HourDayRow {
    fn table_name() -> &'static str {
        "HourDay"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "HourDay";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let hour_day = get_i32("hourDayID")?;
        let day = get_i32("dayID")?;
        let hour = get_i32("hourID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(HourDayRow {
                    hour_day_id: hour_day.get(i).ok_or_else(|| null("hourDayID"))?,
                    day_id: day.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hour.get(i).ok_or_else(|| null("hourID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for LinkRow {
    fn table_name() -> &'static str {
        "Link"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("linkID".into(), DataType::Int32),
            ("countyID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "countyID".into(),
                    rows.iter().map(|r| r.county_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "Link";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let link = get_i32("linkID")?;
        let county = get_i32("countyID")?;
        let zone = get_i32("zoneID")?;
        let road_type = get_i32("roadTypeID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(LinkRow {
                    link_id: link.get(i).ok_or_else(|| null("linkID"))?,
                    county_id: county.get(i).ok_or_else(|| null("countyID"))?,
                    zone_id: zone.get(i).ok_or_else(|| null("zoneID"))?,
                    road_type_id: road_type.get(i).ok_or_else(|| null("roadTypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for CountyRow {
    fn table_name() -> &'static str {
        "County"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("countyID".into(), DataType::Int32),
            ("stateID".into(), DataType::Int32),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "countyID".into(),
                    rows.iter().map(|r| r.county_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "stateID".into(),
                    rows.iter().map(|r| r.state_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "County";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let county = get_i32("countyID")?;
        let state = get_i32("stateID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(CountyRow {
                    county_id: county.get(i).ok_or_else(|| null("countyID"))?,
                    state_id: state.get(i).ok_or_else(|| null("stateID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for ZoneRow {
    fn table_name() -> &'static str {
        "Zone"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("zoneID".into(), DataType::Int32),
            ("countyID".into(), DataType::Int32),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "countyID".into(),
                    rows.iter().map(|r| r.county_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "Zone";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let zone = get_i32("zoneID")?;
        let county = get_i32("countyID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(ZoneRow {
                    zone_id: zone.get(i).ok_or_else(|| null("zoneID"))?,
                    county_id: county.get(i).ok_or_else(|| null("countyID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SourceBinRow {
    fn table_name() -> &'static str {
        "SourceBin"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceBinID".into(), DataType::Int64),
            ("fuelTypeID".into(), DataType::Int32),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceBinID".into(),
                    rows.iter().map(|r| r.source_bin_id).collect::<Vec<i64>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SourceBin";
        let source_bin_id = df
            .column("sourceBinID")
            .map_err(|e| row_err(t, 0, "sourceBinID", e.to_string()))?
            .i64()
            .map_err(|e| row_err(t, 0, "sourceBinID", e.to_string()))?;
        let fuel_type_id = df
            .column("fuelTypeID")
            .map_err(|e| row_err(t, 0, "fuelTypeID", e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, "fuelTypeID", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceBinRow {
                    source_bin_id: source_bin_id.get(i).ok_or_else(|| null("sourceBinID"))?,
                    fuel_type_id: fuel_type_id.get(i).ok_or_else(|| null("fuelTypeID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SourceBinDistributionRow {
    fn table_name() -> &'static str {
        "SourceBinDistribution"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeModelYearID".into(), DataType::Int32),
            ("polProcessID".into(), DataType::Int32),
            ("sourceBinID".into(), DataType::Int64),
            ("sourceBinActivityFraction".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeModelYearID".into(),
                    rows.iter()
                        .map(|r| r.source_type_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceBinID".into(),
                    rows.iter().map(|r| r.source_bin_id).collect::<Vec<i64>>(),
                )
                .into(),
                Series::new(
                    "sourceBinActivityFraction".into(),
                    rows.iter()
                        .map(|r| r.source_bin_activity_fraction)
                        .collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SourceBinDistribution";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let stmy = get_i32("sourceTypeModelYearID")?;
        let pol_proc = get_i32("polProcessID")?;
        let source_bin_id = df
            .column("sourceBinID")
            .map_err(|e| row_err(t, 0, "sourceBinID", e.to_string()))?
            .i64()
            .map_err(|e| row_err(t, 0, "sourceBinID", e.to_string()))?;
        let sbaf = df
            .column("sourceBinActivityFraction")
            .map_err(|e| row_err(t, 0, "sourceBinActivityFraction", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "sourceBinActivityFraction", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceBinDistributionRow {
                    source_type_model_year_id: stmy
                        .get(i)
                        .ok_or_else(|| null("sourceTypeModelYearID"))?,
                    pol_process_id: pol_proc.get(i).ok_or_else(|| null("polProcessID"))?,
                    source_bin_id: source_bin_id.get(i).ok_or_else(|| null("sourceBinID"))?,
                    source_bin_activity_fraction: sbaf
                        .get(i)
                        .ok_or_else(|| null("sourceBinActivityFraction"))?,
                })
            })
            .collect()
    }
}

impl TableRow for SourceTypeModelYearRow {
    fn table_name() -> &'static str {
        "SourceTypeModelYear"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("sourceTypeModelYearID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "sourceTypeModelYearID".into(),
                    rows.iter()
                        .map(|r| r.source_type_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SourceTypeModelYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let stmy = get_i32("sourceTypeModelYearID")?;
        let src_type = get_i32("sourceTypeID")?;
        let model_year = get_i32("modelYearID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(SourceTypeModelYearRow {
                    source_type_model_year_id: stmy
                        .get(i)
                        .ok_or_else(|| null("sourceTypeModelYearID"))?,
                    source_type_id: src_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    model_year_id: model_year.get(i).ok_or_else(|| null("modelYearID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for EmissionRateRow {
    fn table_name() -> &'static str {
        "EmissionRate"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("sourceBinID".into(), DataType::Int64),
            ("opModeID".into(), DataType::Int32),
            ("meanBaseRate".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceBinID".into(),
                    rows.iter().map(|r| r.source_bin_id).collect::<Vec<i64>>(),
                )
                .into(),
                Series::new(
                    "opModeID".into(),
                    rows.iter().map(|r| r.op_mode_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "meanBaseRate".into(),
                    rows.iter().map(|r| r.mean_base_rate).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "EmissionRate";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_proc = get_i32("polProcessID")?;
        let source_bin_id = df
            .column("sourceBinID")
            .map_err(|e| row_err(t, 0, "sourceBinID", e.to_string()))?
            .i64()
            .map_err(|e| row_err(t, 0, "sourceBinID", e.to_string()))?;
        let op_mode = get_i32("opModeID")?;
        let mean_base_rate = df
            .column("meanBaseRate")
            .map_err(|e| row_err(t, 0, "meanBaseRate", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "meanBaseRate", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(EmissionRateRow {
                    pol_process_id: pol_proc.get(i).ok_or_else(|| null("polProcessID"))?,
                    source_bin_id: source_bin_id.get(i).ok_or_else(|| null("sourceBinID"))?,
                    op_mode_id: op_mode.get(i).ok_or_else(|| null("opModeID"))?,
                    mean_base_rate: mean_base_rate.get(i).ok_or_else(|| null("meanBaseRate"))?,
                })
            })
            .collect()
    }
}

impl TableRow for PollutantProcessAssocRow {
    fn table_name() -> &'static str {
        "PollutantProcessAssoc"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("pollutantID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "polProcessID".into(),
                    rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "pollutantID".into(),
                    rows.iter().map(|r| r.pollutant_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "processID".into(),
                    rows.iter().map(|r| r.process_id).collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "PollutantProcessAssoc";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_proc = get_i32("polProcessID")?;
        let pollutant = get_i32("pollutantID")?;
        let process = get_i32("processID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(PollutantProcessAssocRow {
                    pol_process_id: pol_proc.get(i).ok_or_else(|| null("polProcessID"))?,
                    pollutant_id: pollutant.get(i).ok_or_else(|| null("pollutantID"))?,
                    process_id: process.get(i).ok_or_else(|| null("processID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for EmissionProcessRow {
    fn table_name() -> &'static str {
        "EmissionProcess"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([("processID".into(), DataType::Int32)])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "processID".into(),
                rows.iter().map(|r| r.process_id).collect::<Vec<i32>>(),
            )
            .into()],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "EmissionProcess";
        let col = "processID";
        let process_id = df
            .column(col)
            .map_err(|e| row_err(t, 0, col, e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, col, e.to_string()))?;
        (0..df.height())
            .map(|i| {
                Ok(EmissionProcessRow {
                    process_id: process_id
                        .get(i)
                        .ok_or_else(|| row_err(t, i, col, "null value".into()))?,
                })
            })
            .collect()
    }
}

impl TableRow for StartsRow {
    fn table_name() -> &'static str {
        "Starts"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("hourDayID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("ageID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("starts".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "hourDayID".into(),
                    rows.iter().map(|r| r.hour_day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "ageID".into(),
                    rows.iter().map(|r| r.age_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "starts".into(),
                    rows.iter().map(|r| r.starts).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "Starts";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let hour_day = get_i32("hourDayID")?;
        let month = get_i32("monthID")?;
        let year = get_i32("yearID")?;
        let age = get_i32("ageID")?;
        let zone = get_i32("zoneID")?;
        let src_type = get_i32("sourceTypeID")?;
        let starts = get_f64("starts")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(StartsRow {
                    hour_day_id: hour_day.get(i).ok_or_else(|| null("hourDayID"))?,
                    month_id: month.get(i).ok_or_else(|| null("monthID"))?,
                    year_id: year.get(i).ok_or_else(|| null("yearID"))?,
                    age_id: age.get(i).ok_or_else(|| null("ageID"))?,
                    zone_id: zone.get(i).ok_or_else(|| null("zoneID"))?,
                    source_type_id: src_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    starts: starts.get(i).ok_or_else(|| null("starts"))?,
                })
            })
            .collect()
    }
}

impl TableRow for EmissionRow {
    fn table_name() -> &'static str {
        "MOVESWorkerOutput"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("stateID".into(), DataType::Int32),
            ("countyID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
            ("pollutantID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("emissionQuant".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new(
                    "yearID".into(),
                    rows.iter().map(|r| r.year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "monthID".into(),
                    rows.iter().map(|r| r.month_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "dayID".into(),
                    rows.iter().map(|r| r.day_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "hourID".into(),
                    rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "stateID".into(),
                    rows.iter().map(|r| r.state_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "countyID".into(),
                    rows.iter().map(|r| r.county_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "zoneID".into(),
                    rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "linkID".into(),
                    rows.iter().map(|r| r.link_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "pollutantID".into(),
                    rows.iter().map(|r| r.pollutant_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "processID".into(),
                    rows.iter().map(|r| r.process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "sourceTypeID".into(),
                    rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "fuelTypeID".into(),
                    rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "roadTypeID".into(),
                    rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "emissionQuant".into(),
                    rows.iter().map(|r| r.emission_quant).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "MOVESWorkerOutput";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let year = get_i32("yearID")?;
        let month = get_i32("monthID")?;
        let day = get_i32("dayID")?;
        let hour = get_i32("hourID")?;
        let state = get_i32("stateID")?;
        let county = get_i32("countyID")?;
        let zone = get_i32("zoneID")?;
        let link = get_i32("linkID")?;
        let pollutant = get_i32("pollutantID")?;
        let process = get_i32("processID")?;
        let src_type = get_i32("sourceTypeID")?;
        let fuel_type = get_i32("fuelTypeID")?;
        let model_year = get_i32("modelYearID")?;
        let road_type = get_i32("roadTypeID")?;
        let emission_quant = df
            .column("emissionQuant")
            .map_err(|e| row_err(t, 0, "emissionQuant", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "emissionQuant", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(EmissionRow {
                    year_id: year.get(i).ok_or_else(|| null("yearID"))?,
                    month_id: month.get(i).ok_or_else(|| null("monthID"))?,
                    day_id: day.get(i).ok_or_else(|| null("dayID"))?,
                    hour_id: hour.get(i).ok_or_else(|| null("hourID"))?,
                    state_id: state.get(i).ok_or_else(|| null("stateID"))?,
                    county_id: county.get(i).ok_or_else(|| null("countyID"))?,
                    zone_id: zone.get(i).ok_or_else(|| null("zoneID"))?,
                    link_id: link.get(i).ok_or_else(|| null("linkID"))?,
                    pollutant_id: pollutant.get(i).ok_or_else(|| null("pollutantID"))?,
                    process_id: process.get(i).ok_or_else(|| null("processID"))?,
                    source_type_id: src_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    model_year_id: model_year.get(i).ok_or_else(|| null("modelYearID"))?,
                    road_type_id: road_type.get(i).ok_or_else(|| null("roadTypeID"))?,
                    emission_quant: emission_quant.get(i).ok_or_else(|| null("emissionQuant"))?,
                })
            })
            .collect()
    }
}

fn build_running_inputs(ctx: &CalculatorContext) -> Result<RunningExhaustInputs, Error> {
    let tables = ctx.tables();
    Ok(RunningExhaustInputs {
        sho: tables.iter_typed::<ShoRow>("SHO")?,
        hour_day: tables.iter_typed::<HourDayRow>("HourDay")?,
        link: tables.iter_typed::<LinkRow>("Link")?,
        county: tables.iter_typed::<CountyRow>("County")?,
        source_bin: tables.iter_typed::<SourceBinRow>("SourceBin")?,
        source_bin_distribution: tables
            .iter_typed::<SourceBinDistributionRow>("SourceBinDistribution")?,
        source_type_model_year: tables
            .iter_typed::<SourceTypeModelYearRow>("SourceTypeModelYear")?,
        emission_rate: tables.iter_typed::<EmissionRateRow>("EmissionRate")?,
        pollutant_process_assoc: tables
            .iter_typed::<PollutantProcessAssocRow>("PollutantProcessAssoc")?,
    })
}

fn build_start_inputs(ctx: &CalculatorContext) -> Result<StartExhaustInputs, Error> {
    let tables = ctx.tables();
    Ok(StartExhaustInputs {
        starts: tables.iter_typed::<StartsRow>("Starts")?,
        hour_day: tables.iter_typed::<HourDayRow>("HourDay")?,
        link: tables.iter_typed::<LinkRow>("Link")?,
        county: tables.iter_typed::<CountyRow>("County")?,
        zone: tables.iter_typed::<ZoneRow>("Zone")?,
        source_bin: tables.iter_typed::<SourceBinRow>("SourceBin")?,
        source_bin_distribution: tables
            .iter_typed::<SourceBinDistributionRow>("SourceBinDistribution")?,
        source_type_model_year: tables
            .iter_typed::<SourceTypeModelYearRow>("SourceTypeModelYear")?,
        emission_rate: tables.iter_typed::<EmissionRateRow>("EmissionRate")?,
        pollutant_process_assoc: tables
            .iter_typed::<PollutantProcessAssocRow>("PollutantProcessAssoc")?,
        emission_process: tables.iter_typed::<EmissionProcessRow>("EmissionProcess")?,
    })
}

/// `EmissionRate3` indexed for the `WorkerOutputBySourceType` join: each
/// `(sourceTypeID, modelYearID)` maps to its grouped rows — a
/// `(fuelTypeID, pollutantID, processID, sbafXmbr)` quad apiece. Built from
/// the `(sourceTypeModelYearID, fuelTypeID, pollutantID, processID)`-keyed
/// `EmissionRate3` aggregation; consumed by the per-`SHO`-row join.
type EmissionRate3Index = FxHashMap<(i32, i32), Vec<(i32, i32, i32, f64)>>;

/// The MOVES CH4/N2O running-and-start exhaust calculator.
///
/// A small value type: it owns no per-run state — only its master-loop
/// subscriptions, built once in [`new`](Self::new). All run-varying input
/// flows through the [`RunningExhaustInputs`] / [`StartExhaustInputs`]
/// arguments to [`calculate_running`](Self::calculate_running) /
/// [`calculate_start`](Self::calculate_start).
#[derive(Debug, Clone)]
pub struct Ch4N2oRunningStartCalculator {
 /// The Running Exhaust and Start Exhaust master-loop subscriptions,
 /// built once in [`Self::new`].
    subscriptions: [CalculatorSubscription; 2],
}

impl Ch4N2oRunningStartCalculator {
 /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

 /// Construct the calculator with its master-loop subscriptions.
 ///
 /// `CH4N2ORunningStartCalculator.subscribeToMe` signs up for the Running
 /// Exhaust and Start Exhaust processes — each at `MONTH` granularity
 /// with `EMISSION_CALCULATOR` priority — gated on whether the RunSpec
 /// requests the process. `calculator-dag.json` collapses the two
 /// `targetLoop.subscribe` calls into one entry with an unresolved
 /// process id; this port records both resolved subscriptions. The
 /// RunSpec gating is a registry-time concern, not part of the static
 /// subscription metadata.
    #[must_use]
    pub fn new() -> Self {
        let priority = Priority::parse("EMISSION_CALCULATOR")
            .expect("\"EMISSION_CALCULATOR\" is a valid MasterLoop priority");
        Self {
            subscriptions: [
                CalculatorSubscription::new(RUNNING_EXHAUST, Granularity::Month, priority),
                CalculatorSubscription::new(START_EXHAUST, Granularity::Month, priority),
            ],
        }
    }

 /// Port of `doesProcessContext` — whether the master loop should run the
 /// calculator for a `(process, road type)` context.
 ///
 /// Running Exhaust runs for every road type. Start Exhaust runs **only**
 /// for the off-network road type (`roadTypeID == 1`) — engine starts are
 /// modelled as off-network activity. The Java predicate returns `false`
 /// only when the process is Start Exhaust and the road type is a
 /// positive non-off-network id; an absent (`<= 0`) road type still
 /// passes. [`calculate_start`](Self::calculate_start) applies the same
 /// off-network exclusion at row grain, so its pure compute is correct
 /// for any input; this predicate is the master-loop context-filter form
 /// the `execute` wiring uses.
    #[must_use]
    pub fn processes_context(process_id: i32, road_type_id: i32) -> bool {
        let is_start = process_id == i32::from(START_EXHAUST.0);
        !(is_start && road_type_id > 0 && road_type_id != OFF_NETWORK_ROAD_TYPE_ID)
    }

 /// Compute the running-exhaust emission rows — the port of the
 /// `CH4N2ORunningStartCalculator.sql` `-- Section Running Exhaust` of
 /// "Processing".
 ///
 /// Each output row is `emissionQuant = SHO × Σ(sourceBinActivityFraction
 /// × meanBaseRate)`. The result is sorted by its integer dimension
 /// columns for deterministic output — MOVES leaves `MOVESWorkerOutput`
 /// physically unordered.
    #[must_use]
    pub fn calculate_running(&self, inputs: &RunningExhaustInputs) -> Vec<EmissionRow> {
 // PollutantProcessAssoc lookup — resolves polProcessID into
 // (pollutantID, processID).
        let ppa: FxHashMap<i32, &PollutantProcessAssocRow> = inputs
            .pollutant_process_assoc
            .iter()
            .map(|r| (r.pol_process_id, r))
            .collect();
 // SourceBin lookup — sourceBinID → fuelTypeID.
        let source_bin: FxHashMap<i64, &SourceBinRow> = inputs
            .source_bin
            .iter()
            .map(|r| (r.source_bin_id, r))
            .collect();
 // SourceTypeModelYear lookup.
        let stmy: FxHashMap<i32, &SourceTypeModelYearRow> = inputs
            .source_type_model_year
            .iter()
            .map(|r| (r.source_type_model_year_id, r))
            .collect();

 // EmissionRate2: EmissionRate ⋈ PollutantProcessAssoc on
 // polProcessID, kept for pollutantID == 6 and opModeID ∈ [0, 100).
 // Indexed by (polProcessID, sourceBinID); a key carries one
 // meanBaseRate per surviving operating mode.
        let mut emission_rate2: FxHashMap<(i32, i64), Vec<f64>> = FxHashMap::default();
        for er in &inputs.emission_rate {
            if !(RUNNING_OP_MODE_MIN..RUNNING_OP_MODE_MAX).contains(&er.op_mode_id) {
                continue;
            }
 // INNER JOIN PollutantProcessAssoc USING (polProcessID).
            let Some(assoc) = ppa.get(&er.pol_process_id) else {
                continue;
            };
            if assoc.pollutant_id != N2O_POLLUTANT_ID {
                continue;
            }
            emission_rate2
                .entry((er.pol_process_id, er.source_bin_id))
                .or_default()
                .push(er.mean_base_rate);
        }

 // EmissionRate3: Σ(sourceBinActivityFraction × meanBaseRate) over
 // SourceBinDistribution3 ⋈ EmissionRate2 USING (polProcessID,
 // sourceBinID), grouped by (sourceTypeModelYearID, fuelTypeID,
 // pollutantID, processID).
        let mut emission_rate3: FxHashMap<(i32, i32, i32, i32), f64> = FxHashMap::default();
        for sbd in &inputs.source_bin_distribution {
 // SBD2: INNER JOIN SourceBin USING (sourceBinID).
            let Some(sb) = source_bin.get(&sbd.source_bin_id) else {
                continue;
            };
 // SBD3: INNER JOIN SourceTypeModelYear USING
 // (sourceTypeModelYearID).
            if !stmy.contains_key(&sbd.source_type_model_year_id) {
                continue;
            }
 // INNER JOIN EmissionRate2 USING (polProcessID, sourceBinID).
            let Some(rates) = emission_rate2.get(&(sbd.pol_process_id, sbd.source_bin_id)) else {
                continue;
            };
 // EmissionRate2 only holds keys whose polProcessID resolved
 // through `ppa`, so this lookup always succeeds; `continue`
 // keeps the join total without an unreachable panic.
            let Some(assoc) = ppa.get(&sbd.pol_process_id) else {
                continue;
            };
            for &mean_base_rate in rates {
 *emission_rate3
                    .entry((
                        sbd.source_type_model_year_id,
                        sb.fuel_type_id,
                        assoc.pollutant_id,
                        assoc.process_id,
                    ))
                    .or_default() += sbd.source_bin_activity_fraction * mean_base_rate;
            }
        }

 // Index EmissionRate3 by (sourceTypeID, modelYearID) for the
 // WorkerOutputBySourceType join — sourceTypeModelYearID resolves to
 // exactly that pair.
        let mut er3_index: EmissionRate3Index = FxHashMap::default();
        for (&(stmy_id, fuel_type_id, pollutant_id, process_id), &sbaf_x_mbr) in &emission_rate3 {
 // stmy present — the SBD3 inner join above required it.
            let Some(stmy_row) = stmy.get(&stmy_id) else {
                continue;
            };
            er3_index
                .entry((stmy_row.source_type_id, stmy_row.model_year_id))
                .or_default()
                .push((fuel_type_id, pollutant_id, process_id, sbaf_x_mbr));
        }

 // SHO ⋈ HourDay (SHO2) ⋈ Link ⋈ County (Link2/SHO3) ⋈ EmissionRate3
 // (WorkerOutputBySourceType).
        let hour_day: FxHashMap<i32, &HourDayRow> =
            inputs.hour_day.iter().map(|r| (r.hour_day_id, r)).collect();
        let link: FxHashMap<i32, &LinkRow> = inputs.link.iter().map(|r| (r.link_id, r)).collect();
        let county: FxHashMap<i32, &CountyRow> =
            inputs.county.iter().map(|r| (r.county_id, r)).collect();

        let mut out: Vec<EmissionRow> = Vec::new();
        for sho in &inputs.sho {
 // SHO2: INNER JOIN HourDay USING (hourDayID).
            let Some(hd) = hour_day.get(&sho.hour_day_id) else {
                continue;
            };
            let model_year_id = sho.year_id - sho.age_id;
 // SHO3: INNER JOIN Link2 (= Link ⋈ County USING (countyID))
 // USING (linkID).
            let Some(link_row) = link.get(&sho.link_id) else {
                continue;
            };
            let Some(county_row) = county.get(&link_row.county_id) else {
                continue;
            };
 // WorkerOutputBySourceType: INNER JOIN EmissionRate3 USING
 // (sourceTypeID, modelYearID).
            let Some(rate_rows) = er3_index.get(&(sho.source_type_id, model_year_id)) else {
                continue;
            };
            for &(fuel_type_id, pollutant_id, process_id, sbaf_x_mbr) in rate_rows {
                out.push(EmissionRow {
                    year_id: sho.year_id,
                    month_id: sho.month_id,
                    day_id: hd.day_id,
                    hour_id: hd.hour_id,
                    state_id: county_row.state_id,
                    county_id: link_row.county_id,
                    zone_id: link_row.zone_id,
                    link_id: sho.link_id,
                    pollutant_id,
                    process_id,
                    source_type_id: sho.source_type_id,
                    fuel_type_id,
                    model_year_id,
                    road_type_id: link_row.road_type_id,
                    emission_quant: sbaf_x_mbr * sho.sho,
                });
            }
        }

        out.sort_unstable_by_key(EmissionRow::dimension_key);
        out
    }

 /// Compute the start-exhaust emission rows — the port of the
 /// `CH4N2ORunningStartCalculator.sql` `-- Section Start Exhaust` of
 /// "Processing".
 ///
 /// Each output row is `emissionQuant = Σ(sourceBinActivityFraction ×
 /// starts × meanBaseRate)`, summed over the SQL's fourteen-column
 /// `GROUP BY`. Only off-network links (`roadTypeID == 1`) participate:
 /// engine starts are zone-level, and joining them to every link of a
 /// zone would multi-count — MOVES extracts only the off-network link
 /// (see [`processes_context`](Self::processes_context)). The result is
 /// sorted by its integer dimension columns for deterministic output.
    #[must_use]
    pub fn calculate_start(&self, inputs: &StartExhaustInputs) -> Vec<EmissionRow> {
 // Primary-key lookups.
        let hour_day: FxHashMap<i32, &HourDayRow> =
            inputs.hour_day.iter().map(|r| (r.hour_day_id, r)).collect();
        let county: FxHashMap<i32, &CountyRow> =
            inputs.county.iter().map(|r| (r.county_id, r)).collect();
        let source_bin: FxHashMap<i64, &SourceBinRow> = inputs
            .source_bin
            .iter()
            .map(|r| (r.source_bin_id, r))
            .collect();
        let ppa: FxHashMap<i32, &PollutantProcessAssocRow> = inputs
            .pollutant_process_assoc
            .iter()
            .map(|r| (r.pol_process_id, r))
            .collect();
        let zone: FxHashMap<i32, &ZoneRow> = inputs.zone.iter().map(|r| (r.zone_id, r)).collect();
 // EmissionProcess set — the `ppa.processID = ep.processID` join
 // gates ppa rows to the extracted process(es).
        let process_ids: HashSet<i32> = inputs
            .emission_process
            .iter()
            .map(|r| r.process_id)
            .collect();
 // SourceTypeModelYear keyed by (sourceTypeID, modelYearID) — the
 // `st.ageID = st.yearID - stmy.modelYearID AND st.sourceTypeID =
 // stmy.sourceTypeID` join target.
        let stmy_by_type_year: FxHashMap<(i32, i32), &SourceTypeModelYearRow> = inputs
            .source_type_model_year
            .iter()
            .map(|r| ((r.source_type_id, r.model_year_id), r))
            .collect();
 // Link keyed by zoneID, off-network only — the SQL joins starts to
 // links through `st.zoneID = l.zoneID`; see the method docs.
        let mut links_by_zone: FxHashMap<i32, Vec<&LinkRow>> = FxHashMap::default();
        for l in &inputs.link {
            if l.road_type_id != OFF_NETWORK_ROAD_TYPE_ID {
                continue;
            }
            links_by_zone.entry(l.zone_id).or_default().push(l);
        }
 // SourceBinDistribution keyed by sourceTypeModelYearID.
        let mut sbd_by_stmy: FxHashMap<i32, Vec<&SourceBinDistributionRow>> = FxHashMap::default();
        for sbd in &inputs.source_bin_distribution {
            sbd_by_stmy
                .entry(sbd.source_type_model_year_id)
                .or_default()
                .push(sbd);
        }
 // EmissionRate keyed by (polProcessID, sourceBinID) for opModeID ==
 // 100; (polProcessID, sourceBinID, opModeID) is unique, so the
 // start operating mode gives at most one rate per key.
        let mut emission_rate_start: FxHashMap<(i32, i64), f64> = FxHashMap::default();
        for er in &inputs.emission_rate {
            if er.op_mode_id != START_OP_MODE_ID {
                continue;
            }
            emission_rate_start.insert((er.pol_process_id, er.source_bin_id), er.mean_base_rate);
        }

 // The fourteen-column GROUP BY accumulator.
        let mut totals: FxHashMap<[i32; 14], f64> = FxHashMap::default();
        for st in &inputs.starts {
 // INNER JOIN HourDay USING (hourDayID).
            let Some(hd) = hour_day.get(&st.hour_day_id) else {
                continue;
            };
 // INNER JOIN SourceTypeModelYear ON st.sourceTypeID =
 // stmy.sourceTypeID AND st.ageID = st.yearID - stmy.modelYearID,
 // i.e. modelYearID = yearID - ageID.
            let model_year_id = st.year_id - st.age_id;
            let Some(stmy_row) = stmy_by_type_year.get(&(st.source_type_id, model_year_id)) else {
                continue;
            };
 // INNER JOIN Zone ON st.zoneID = z.zoneID.
            let Some(zone_row) = zone.get(&st.zone_id) else {
                continue;
            };
 // INNER JOIN Link ON st.zoneID = l.zoneID (off-network only).
            let Some(zone_links) = links_by_zone.get(&st.zone_id) else {
                continue;
            };
 // INNER JOIN SourceBinDistribution ON sbd.sourceTypeModelYearID
 // = stmy.sourceTypeModelYearID.
            let Some(sbd_rows) = sbd_by_stmy.get(&stmy_row.source_type_model_year_id) else {
                continue;
            };
            for link_row in zone_links {
 // INNER JOIN County ON c.countyID = l.countyID AND
 // c.countyID = z.countyID.
                if link_row.county_id != zone_row.county_id {
                    continue;
                }
                let Some(county_row) = county.get(&link_row.county_id) else {
                    continue;
                };
                for sbd in sbd_rows {
 // INNER JOIN PollutantProcessAssoc ON sbd.polProcessID =
 // ppa.polProcessID, with ppa.pollutantID = 6.
                    let Some(assoc) = ppa.get(&sbd.pol_process_id) else {
                        continue;
                    };
                    if assoc.pollutant_id != N2O_POLLUTANT_ID {
                        continue;
                    }
 // INNER JOIN EmissionProcess ON ppa.processID =
 // ep.processID.
                    if !process_ids.contains(&assoc.process_id) {
                        continue;
                    }
 // INNER JOIN SourceBin ON sbd.sourceBinID =
 // sb.sourceBinID.
                    let Some(sb) = source_bin.get(&sbd.source_bin_id) else {
                        continue;
                    };
 // INNER JOIN EmissionRate ON sbd.polProcessID =
 // er.polProcessID AND sbd.sourceBinID = er.sourceBinID,
 // with er.opModeID = 100.
                    let Some(&mean_base_rate) =
                        emission_rate_start.get(&(sbd.pol_process_id, sbd.source_bin_id))
                    else {
                        continue;
                    };
                    let row = EmissionRow {
                        year_id: st.year_id,
                        month_id: st.month_id,
                        day_id: hd.day_id,
                        hour_id: hd.hour_id,
                        state_id: county_row.state_id,
                        county_id: county_row.county_id,
                        zone_id: zone_row.zone_id,
                        link_id: link_row.link_id,
                        pollutant_id: assoc.pollutant_id,
                        process_id: assoc.process_id,
                        source_type_id: st.source_type_id,
                        fuel_type_id: sb.fuel_type_id,
                        model_year_id: stmy_row.model_year_id,
                        road_type_id: link_row.road_type_id,
                        emission_quant: 0.0,
                    };
 *totals.entry(row.dimension_key()).or_default() +=
                        sbd.source_bin_activity_fraction * st.starts * mean_base_rate;
                }
            }
        }

        let mut out: Vec<EmissionRow> = totals
            .into_iter()
            .map(|(key, emission_quant)| EmissionRow::from_dimension_key(key, emission_quant))
            .collect();
        out.sort_unstable_by_key(EmissionRow::dimension_key);
        out
    }
}

impl Default for Ch4N2oRunningStartCalculator {
    fn default() -> Self {
        Self::new()
    }
}

/// `CH4N2ORunningStartCalculator` registers no `(pollutant, process)` pairs/// see [`Calculator::registrations`] on the impl below.
static NO_REGISTRATIONS: &[PollutantProcessAssociation] = &[];

/// Default-DB / execution-DB tables the running and start computations
/// consume — the union of the data tables both "Extract Data" sections pull.
/// The SQL also extracts `Pollutant` and `AdjustFuelSupply`; neither feeds
/// the "Processing" sections, so neither is listed.
static INPUT_TABLES: &[&str] = &[
    "County",
    "EmissionProcess",
    "EmissionRate",
    "HourDay",
    "Link",
    "PollutantProcessAssoc",
    "SHO",
    "SourceBin",
    "SourceBinDistribution",
    "SourceTypeModelYear",
    "Starts",
    "Zone",
];

impl Calculator for Ch4N2oRunningStartCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn subscriptions(&self) -> &[CalculatorSubscription] {
        &self.subscriptions
    }

 /// `CH4N2ORunningStartCalculator` registers **no** `(pollutant, process)`
 /// pairs.
 ///
 /// The Java constructor calls `EmissionCalculatorRegistration.register`
 /// for nitrous oxide (pollutant 6) on Running Exhaust (process 1) and
 /// Start Exhaust (process 2) — but those are legacy registrations. In
 /// the pinned MOVES, `CalculatorInfo.txt` (the runtime registration
 /// file) has no `Registration` directive for this module: N2O Running
 /// Exhaust `(6, 1)` and N2O Start Exhaust `(6, 2)` are registered to
 /// `BaseRateCalculator` instead (`CalculatorInfo.txt` lines 505–506),
 /// and `calculator-dag.json` records `registrations_count: 0` to match.
 ///
 /// Returning an empty slice keeps this port consistent with the runtime
 /// and prevents the registry from double-registering `(6, 1)`/`(6, 2)`
 /// against `BaseRateCalculator`. See the [module docs](self).
    fn registrations(&self) -> &[PollutantProcessAssociation] {
        NO_REGISTRATIONS
    }

 // `upstream` keeps the trait default (empty): `calculator-dag.json`
 // records no `depends_on` edges. The calculator consumes `SHO` and
 // `Starts` (activity generators) and `SourceBinDistribution` (the source
 // bin distribution generator), but those run earlier by master-loop
 // priority ordering, not as chain dependencies.

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let filter = crate::wiring::position_filter(ctx);
        if filter.process_id == Some(i32::from(RUNNING_EXHAUST.0)) {
            let inputs = build_running_inputs(ctx)?;
            let rows = self.calculate_running(&inputs);
            crate::wiring::emit_rows(rows)
        } else if filter.process_id == Some(i32::from(START_EXHAUST.0)) {
            let inputs = build_start_inputs(ctx)?;
            let rows = self.calculate_start(&inputs);
            crate::wiring::emit_rows(rows)
        } else {
            crate::wiring::emit_rows(Vec::<EmissionRow>::new())
        }
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(Ch4N2oRunningStartCalculator::new())
}

#[cfg(test)]
mod tests {
    use super::*;

 /// N2O Running Exhaust `polProcessID` — `pollutantID 6 × 100 + processID 1`.
    const N2O_RUNNING_POL_PROCESS: i32 = 601;
 /// N2O Start Exhaust `polProcessID` — `pollutantID 6 × 100 + processID 2`.
    const N2O_START_POL_PROCESS: i32 = 602;

 /// A one-`SHO` / one-bin running input whose single output row has
 /// `emissionQuant = SHO 100 × (sbaf 1 × meanBaseRate 2) = 200`.
 /// `sourceTypeModelYearID` follows the MOVES `sourceTypeID * 10000 +
 /// modelYearID` convention (`21 * 10000 + 2018`).
    fn minimal_running_inputs() -> RunningExhaustInputs {
        RunningExhaustInputs {
            sho: vec![ShoRow {
                hour_day_id: 85,
                month_id: 7,
                year_id: 2020,
                age_id: 2, // modelYearID = 2020 - 2 = 2018
                link_id: 5001,
                source_type_id: 21,
                sho: 100.0,
            }],
            hour_day: vec![HourDayRow {
                hour_day_id: 85,
                day_id: 5,
                hour_id: 8,
            }],
            link: vec![LinkRow {
                link_id: 5001,
                county_id: 26_161,
                zone_id: 261_610,
                road_type_id: 4, // urban unrestricted
            }],
            county: vec![CountyRow {
                county_id: 26_161,
                state_id: 26,
            }],
            source_bin: vec![SourceBinRow {
                source_bin_id: 1000,
                fuel_type_id: 1,
            }],
            source_bin_distribution: vec![SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: N2O_RUNNING_POL_PROCESS,
                source_bin_id: 1000,
                source_bin_activity_fraction: 1.0,
            }],
            source_type_model_year: vec![SourceTypeModelYearRow {
                source_type_model_year_id: 212_018,
                source_type_id: 21,
                model_year_id: 2018,
            }],
            emission_rate: vec![EmissionRateRow {
                pol_process_id: N2O_RUNNING_POL_PROCESS,
                source_bin_id: 1000,
                op_mode_id: 1, // running operating mode, ∈ [0, 100)
                mean_base_rate: 2.0,
            }],
            pollutant_process_assoc: vec![PollutantProcessAssocRow {
                pol_process_id: N2O_RUNNING_POL_PROCESS,
                pollutant_id: N2O_POLLUTANT_ID,
                process_id: 1,
            }],
        }
    }

 /// A one-`Starts` / one-bin start input whose single output row has
 /// `emissionQuant = sbaf 1 × starts 10 × meanBaseRate 3 = 30`.
    fn minimal_start_inputs() -> StartExhaustInputs {
        StartExhaustInputs {
            starts: vec![StartsRow {
                hour_day_id: 85,
                month_id: 7,
                year_id: 2020,
                age_id: 2, // modelYearID = 2018
                zone_id: 261_610,
                source_type_id: 21,
                starts: 10.0,
            }],
            hour_day: vec![HourDayRow {
                hour_day_id: 85,
                day_id: 5,
                hour_id: 8,
            }],
            link: vec![LinkRow {
                link_id: 5001,
                county_id: 26_161,
                zone_id: 261_610,
                road_type_id: OFF_NETWORK_ROAD_TYPE_ID, // starts run off-network
            }],
            county: vec![CountyRow {
                county_id: 26_161,
                state_id: 26,
            }],
            zone: vec![ZoneRow {
                zone_id: 261_610,
                county_id: 26_161,
            }],
            source_bin: vec![SourceBinRow {
                source_bin_id: 1000,
                fuel_type_id: 1,
            }],
            source_bin_distribution: vec![SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: N2O_START_POL_PROCESS,
                source_bin_id: 1000,
                source_bin_activity_fraction: 1.0,
            }],
            source_type_model_year: vec![SourceTypeModelYearRow {
                source_type_model_year_id: 212_018,
                source_type_id: 21,
                model_year_id: 2018,
            }],
            emission_rate: vec![EmissionRateRow {
                pol_process_id: N2O_START_POL_PROCESS,
                source_bin_id: 1000,
                op_mode_id: START_OP_MODE_ID,
                mean_base_rate: 3.0,
            }],
            pollutant_process_assoc: vec![PollutantProcessAssocRow {
                pol_process_id: N2O_START_POL_PROCESS,
                pollutant_id: N2O_POLLUTANT_ID,
                process_id: 2,
            }],
            emission_process: vec![EmissionProcessRow { process_id: 2 }],
        }
    }

 /// Assert `actual.emission_quant` matches `expected` within `f64` slack /// the FLOAT-column fidelity note means the port computes in `f64`.
    fn assert_emission(actual: &EmissionRow, expected: f64) {
        assert!(
            (actual.emission_quant - expected).abs() < 1e-9,
            "emission_quant {} != expected {expected}",
            actual.emission_quant,
        );
    }

 // ----- Running Exhaust -------------------------------------------------

    #[test]
    fn running_minimal_input_yields_one_row() {
        let rows = Ch4N2oRunningStartCalculator::new().calculate_running(&minimal_running_inputs());
        assert_eq!(rows.len(), 1);
        let r = rows[0];
        assert_eq!(r.year_id, 2020);
        assert_eq!(r.month_id, 7);
        assert_eq!(r.day_id, 5); // from HourDay
        assert_eq!(r.hour_id, 8); // from HourDay
        assert_eq!(r.state_id, 26); // from County
        assert_eq!(r.county_id, 26_161); // from Link
        assert_eq!(r.zone_id, 261_610); // from Link
        assert_eq!(r.link_id, 5001);
        assert_eq!(r.pollutant_id, N2O_POLLUTANT_ID);
        assert_eq!(r.process_id, 1); // Running Exhaust, from PollutantProcessAssoc
        assert_eq!(r.source_type_id, 21);
        assert_eq!(r.fuel_type_id, 1); // from SourceBin
        assert_eq!(r.model_year_id, 2018); // yearID - ageID
        assert_eq!(r.road_type_id, 4); // from Link
        assert_emission(&r, 200.0); // SHO 100 × (sbaf 1 × meanBaseRate 2)
    }

    #[test]
    fn running_sums_mean_base_rate_across_operating_modes() {
 // Two EmissionRate rows for the same bin, distinct running operating
 // modes: EmissionRate3 sums their meanBaseRates into sbafXmbr.
        let mut inputs = minimal_running_inputs();
        inputs.emission_rate.push(EmissionRateRow {
            pol_process_id: N2O_RUNNING_POL_PROCESS,
            source_bin_id: 1000,
            op_mode_id: 33, // another running op mode
            mean_base_rate: 5.0,
        });
        let rows = Ch4N2oRunningStartCalculator::new().calculate_running(&inputs);
        assert_eq!(rows.len(), 1);
 // SHO 100 × (sbaf 1 × (meanBaseRate 2 + meanBaseRate 5)).
        assert_emission(&rows[0], 700.0);
    }

    #[test]
    fn running_sums_source_bin_activity_fraction_across_bins() {
 // Two source bins with the same fuel type: EmissionRate3 groups by
 // (sourceTypeModelYearID, fuelTypeID, ...) so their sbaf×meanBaseRate
 // terms add into one fuel-type group.
        let mut inputs = minimal_running_inputs();
        inputs.source_bin.push(SourceBinRow {
            source_bin_id: 1001,
            fuel_type_id: 1, // same fuel type
        });
        inputs.source_bin_distribution[0].source_bin_activity_fraction = 0.5;
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: N2O_RUNNING_POL_PROCESS,
                source_bin_id: 1001,
                source_bin_activity_fraction: 0.25,
            });
        inputs.emission_rate.push(EmissionRateRow {
            pol_process_id: N2O_RUNNING_POL_PROCESS,
            source_bin_id: 1001,
            op_mode_id: 1,
            mean_base_rate: 2.0,
        });
        let rows = Ch4N2oRunningStartCalculator::new().calculate_running(&inputs);
        assert_eq!(rows.len(), 1);
 // SHO 100 × (sbaf 0.5 × mbr 2 + sbaf 0.25 × mbr 2) = 100 × 1.5.
        assert_emission(&rows[0], 150.0);
    }

    #[test]
    fn running_splits_emissions_across_fuel_types() {
 // A second bin on a different fuel type adds an EmissionRate3 group
 // for the same (sourceType, modelYear): the SHO row emits once per
 // fuel type.
        let mut inputs = minimal_running_inputs();
        inputs.source_bin.push(SourceBinRow {
            source_bin_id: 1002,
            fuel_type_id: 2,
        });
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: N2O_RUNNING_POL_PROCESS,
                source_bin_id: 1002,
                source_bin_activity_fraction: 1.0,
            });
        inputs.emission_rate.push(EmissionRateRow {
            pol_process_id: N2O_RUNNING_POL_PROCESS,
            source_bin_id: 1002,
            op_mode_id: 1,
            mean_base_rate: 2.0,
        });
        let rows = Ch4N2oRunningStartCalculator::new().calculate_running(&inputs);
        assert_eq!(rows.len(), 2);
        let fuel1 = rows.iter().find(|r| r.fuel_type_id == 1).unwrap();
        let fuel2 = rows.iter().find(|r| r.fuel_type_id == 2).unwrap();
        assert_emission(fuel1, 200.0);
        assert_emission(fuel2, 200.0);
    }

    #[test]
    fn running_excludes_start_operating_mode_rate() {
 // The only EmissionRate row is the start operating mode (100): the
 // running section's opModeID ∈ [0, 100) filter drops it.
        let mut inputs = minimal_running_inputs();
        inputs.emission_rate[0].op_mode_id = START_OP_MODE_ID;
        assert!(Ch4N2oRunningStartCalculator::new()
            .calculate_running(&inputs)
            .is_empty());
    }

    #[test]
    fn running_excludes_non_n2o_pollutant() {
 // The PollutantProcessAssoc resolves the rate's polProcessID to a
 // non-N2O pollutant: EmissionRate2's `pollutantID = 6` filter drops it.
        let mut inputs = minimal_running_inputs();
        inputs.pollutant_process_assoc[0].pollutant_id = 3; // not N2O
        assert!(Ch4N2oRunningStartCalculator::new()
            .calculate_running(&inputs)
            .is_empty());
    }

    #[test]
    fn running_drops_rows_missing_an_inner_join() {
        let calc = Ch4N2oRunningStartCalculator::new();

 // SHO references an hourDayID absent from HourDay.
        let mut no_hour_day = minimal_running_inputs();
        no_hour_day.sho[0].hour_day_id = 999;
        assert!(calc.calculate_running(&no_hour_day).is_empty());

 // SHO references a link absent from Link.
        let mut no_link = minimal_running_inputs();
        no_link.sho[0].link_id = 9999;
        assert!(calc.calculate_running(&no_link).is_empty());

 // Link references a county absent from County.
        let mut no_county = minimal_running_inputs();
        no_county.county.clear();
        assert!(calc.calculate_running(&no_county).is_empty());

 // SourceBinDistribution references a bin absent from SourceBin.
        let mut no_bin = minimal_running_inputs();
        no_bin.source_bin_distribution[0].source_bin_id = 7777;
        assert!(calc.calculate_running(&no_bin).is_empty());

 // SourceBinDistribution references a sourceTypeModelYearID absent
 // from SourceTypeModelYear.
        let mut no_stmy = minimal_running_inputs();
        no_stmy.source_type_model_year.clear();
        assert!(calc.calculate_running(&no_stmy).is_empty());

 // SHO age gives a modelYearID with no EmissionRate3 group.
        let mut wrong_age = minimal_running_inputs();
        wrong_age.sho[0].age_id = 10; // modelYearID 2010, not 2018
        assert!(calc.calculate_running(&wrong_age).is_empty());
    }

    #[test]
    fn running_empty_input_yields_no_rows() {
        assert!(Ch4N2oRunningStartCalculator::new()
            .calculate_running(&RunningExhaustInputs::default())
            .is_empty());
    }

 // ----- Start Exhaust ---------------------------------------------------

    #[test]
    fn start_minimal_input_yields_one_row() {
        let rows = Ch4N2oRunningStartCalculator::new().calculate_start(&minimal_start_inputs());
        assert_eq!(rows.len(), 1);
        let r = rows[0];
        assert_eq!(r.year_id, 2020);
        assert_eq!(r.month_id, 7);
        assert_eq!(r.day_id, 5); // from HourDay
        assert_eq!(r.hour_id, 8); // from HourDay
        assert_eq!(r.state_id, 26); // from County
        assert_eq!(r.county_id, 26_161); // from County
        assert_eq!(r.zone_id, 261_610); // from Zone
        assert_eq!(r.link_id, 5001); // from Link
        assert_eq!(r.pollutant_id, N2O_POLLUTANT_ID);
        assert_eq!(r.process_id, 2); // Start Exhaust, from PollutantProcessAssoc
        assert_eq!(r.source_type_id, 21);
        assert_eq!(r.fuel_type_id, 1); // from SourceBin
        assert_eq!(r.model_year_id, 2018); // yearID - ageID
        assert_eq!(r.road_type_id, OFF_NETWORK_ROAD_TYPE_ID);
        assert_emission(&r, 30.0); // sbaf 1 × starts 10 × meanBaseRate 3
    }

    #[test]
    fn start_aggregates_bins_of_the_same_fuel_type() {
 // Two source bins of the same fuel type: the SQL GROUP BY collapses
 // them into one row, summing sbaf×starts×meanBaseRate.
        let mut inputs = minimal_start_inputs();
        inputs.source_bin.push(SourceBinRow {
            source_bin_id: 1001,
            fuel_type_id: 1, // same fuel type → same GROUP BY cell
        });
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: N2O_START_POL_PROCESS,
                source_bin_id: 1001,
                source_bin_activity_fraction: 0.5,
            });
        inputs.emission_rate.push(EmissionRateRow {
            pol_process_id: N2O_START_POL_PROCESS,
            source_bin_id: 1001,
            op_mode_id: START_OP_MODE_ID,
            mean_base_rate: 3.0,
        });
        let rows = Ch4N2oRunningStartCalculator::new().calculate_start(&inputs);
        assert_eq!(rows.len(), 1);
 // starts 10 × (sbaf 1 × mbr 3 + sbaf 0.5 × mbr 3) = 10 × 4.5.
        assert_emission(&rows[0], 45.0);
    }

    #[test]
    fn start_splits_emissions_across_fuel_types() {
 // A second bin on a different fuel type: a separate GROUP BY cell.
        let mut inputs = minimal_start_inputs();
        inputs.source_bin.push(SourceBinRow {
            source_bin_id: 1002,
            fuel_type_id: 2,
        });
        inputs
            .source_bin_distribution
            .push(SourceBinDistributionRow {
                source_type_model_year_id: 212_018,
                pol_process_id: N2O_START_POL_PROCESS,
                source_bin_id: 1002,
                source_bin_activity_fraction: 1.0,
            });
        inputs.emission_rate.push(EmissionRateRow {
            pol_process_id: N2O_START_POL_PROCESS,
            source_bin_id: 1002,
            op_mode_id: START_OP_MODE_ID,
            mean_base_rate: 3.0,
        });
        let rows = Ch4N2oRunningStartCalculator::new().calculate_start(&inputs);
        assert_eq!(rows.len(), 2);
        assert_emission(rows.iter().find(|r| r.fuel_type_id == 1).unwrap(), 30.0);
        assert_emission(rows.iter().find(|r| r.fuel_type_id == 2).unwrap(), 30.0);
    }

    #[test]
    fn start_uses_only_the_off_network_link_of_a_zone() {
 // The zone carries two links — off-network and urban. Only the
 // off-network link joins, so the start is counted once, not twice,
 // and the output road type is off-network.
        let mut inputs = minimal_start_inputs();
        inputs.link.push(LinkRow {
            link_id: 5002,
            county_id: 26_161,
            zone_id: 261_610,
            road_type_id: 4, // urban — must not join
        });
        let rows = Ch4N2oRunningStartCalculator::new().calculate_start(&inputs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].link_id, 5001);
        assert_eq!(rows[0].road_type_id, OFF_NETWORK_ROAD_TYPE_ID);
        assert_emission(&rows[0], 30.0);
    }

    #[test]
    fn start_without_an_off_network_link_yields_no_rows() {
 // The zone's only link is urban: no off-network link to join.
        let mut inputs = minimal_start_inputs();
        inputs.link[0].road_type_id = 4;
        assert!(Ch4N2oRunningStartCalculator::new()
            .calculate_start(&inputs)
            .is_empty());
    }

    #[test]
    fn start_drops_link_whose_county_differs_from_the_zone() {
 // County join is `c.countyID = l.countyID AND c.countyID =
 // z.countyID`: a link in a different county than its zone drops.
        let mut inputs = minimal_start_inputs();
        inputs.link[0].county_id = 99_999;
        inputs.county.push(CountyRow {
            county_id: 99_999,
            state_id: 26,
        });
        assert!(Ch4N2oRunningStartCalculator::new()
            .calculate_start(&inputs)
            .is_empty());
    }

    #[test]
    fn start_drops_rows_missing_an_inner_join() {
        let calc = Ch4N2oRunningStartCalculator::new();

 // Starts references an hourDayID absent from HourDay.
        let mut no_hour_day = minimal_start_inputs();
        no_hour_day.starts[0].hour_day_id = 999;
        assert!(calc.calculate_start(&no_hour_day).is_empty());

 // Starts age gives a (sourceType, modelYear) with no
 // SourceTypeModelYear row.
        let mut wrong_age = minimal_start_inputs();
        wrong_age.starts[0].age_id = 30;
        assert!(calc.calculate_start(&wrong_age).is_empty());

 // Starts references a zone absent from Zone.
        let mut no_zone = minimal_start_inputs();
        no_zone.zone.clear();
        assert!(calc.calculate_start(&no_zone).is_empty());

 // SourceBinDistribution references a bin absent from SourceBin.
        let mut no_bin = minimal_start_inputs();
        no_bin.source_bin_distribution[0].source_bin_id = 7777;
        assert!(calc.calculate_start(&no_bin).is_empty());

 // The EmissionRate row uses a running operating mode, not 100.
        let mut wrong_op_mode = minimal_start_inputs();
        wrong_op_mode.emission_rate[0].op_mode_id = 1;
        assert!(calc.calculate_start(&wrong_op_mode).is_empty());
    }

    #[test]
    fn start_drops_rows_when_emission_process_is_absent() {
 // The `ppa.processID = ep.processID` join: with no EmissionProcess
 // row for process 2, every ppa row is gated out.
        let mut inputs = minimal_start_inputs();
        inputs.emission_process.clear();
        assert!(Ch4N2oRunningStartCalculator::new()
            .calculate_start(&inputs)
            .is_empty());
    }

    #[test]
    fn start_excludes_non_n2o_pollutant() {
        let mut inputs = minimal_start_inputs();
        inputs.pollutant_process_assoc[0].pollutant_id = 3; // not N2O
        assert!(Ch4N2oRunningStartCalculator::new()
            .calculate_start(&inputs)
            .is_empty());
    }

    #[test]
    fn start_empty_input_yields_no_rows() {
        assert!(Ch4N2oRunningStartCalculator::new()
            .calculate_start(&StartExhaustInputs::default())
            .is_empty());
    }

    #[test]
    fn calculate_output_is_sorted_by_dimension_key() {
 // Two fuel types over two model years give four start rows; the
 // result must come back dimension-key sorted regardless of the
 // hash-map-driven aggregation order.
        let mut inputs = minimal_start_inputs();
        inputs.source_bin.push(SourceBinRow {
            source_bin_id: 1002,
            fuel_type_id: 2,
        });
        inputs.source_type_model_year.push(SourceTypeModelYearRow {
            source_type_model_year_id: 212_019,
            source_type_id: 21,
            model_year_id: 2019,
        });
        for (stmy_id, bin) in [(212_018, 1002), (212_019, 1000), (212_019, 1002)] {
            inputs
                .source_bin_distribution
                .push(SourceBinDistributionRow {
                    source_type_model_year_id: stmy_id,
                    pol_process_id: N2O_START_POL_PROCESS,
                    source_bin_id: bin,
                    source_bin_activity_fraction: 1.0,
                });
        }
        inputs.emission_rate.push(EmissionRateRow {
            pol_process_id: N2O_START_POL_PROCESS,
            source_bin_id: 1002,
            op_mode_id: START_OP_MODE_ID,
            mean_base_rate: 3.0,
        });
        inputs.starts.push(StartsRow {
            hour_day_id: 85,
            month_id: 7,
            year_id: 2020,
            age_id: 1, // modelYearID 2019
            zone_id: 261_610,
            source_type_id: 21,
            starts: 5.0,
        });

        let rows = Ch4N2oRunningStartCalculator::new().calculate_start(&inputs);
        assert_eq!(rows.len(), 4);
        assert!(
            rows.windows(2)
                .all(|w| w[0].dimension_key() <= w[1].dimension_key()),
            "calculate_start output is not sorted by dimension key",
        );
    }

    #[test]
    fn dimension_key_round_trips() {
        let row = minimal_running_inputs();
        let row = Ch4N2oRunningStartCalculator::new().calculate_running(&row)[0];
        let rebuilt = EmissionRow::from_dimension_key(row.dimension_key(), row.emission_quant);
        assert_eq!(rebuilt, row);
    }

 // ----- Context filter --------------------------------------------------

    #[test]
    fn processes_context_runs_running_exhaust_on_every_road_type() {
        let running = i32::from(RUNNING_EXHAUST.0);
        for road_type in [1, 2, 3, 4, 5] {
            assert!(Ch4N2oRunningStartCalculator::processes_context(
                running, road_type
            ));
        }
    }

    #[test]
    fn processes_context_runs_start_exhaust_only_off_network() {
        let start = i32::from(START_EXHAUST.0);
        assert!(Ch4N2oRunningStartCalculator::processes_context(
            start,
            OFF_NETWORK_ROAD_TYPE_ID,
        ));
 // An unset (<= 0) road type still passes — matches the Java guard.
        assert!(Ch4N2oRunningStartCalculator::processes_context(start, 0));
        for road_type in [2, 3, 4, 5] {
            assert!(!Ch4N2oRunningStartCalculator::processes_context(
                start, road_type
            ));
        }
    }

 // ----- Calculator trait metadata --------------------------------------

    #[test]
    fn calculator_name_matches_dag_module() {
        assert_eq!(
            Ch4N2oRunningStartCalculator::new().name(),
            "CH4N2ORunningStartCalculator"
        );
        assert_eq!(
            Ch4N2oRunningStartCalculator::NAME,
            "CH4N2ORunningStartCalculator"
        );
    }

    #[test]
    fn calculator_subscribes_to_running_and_start_exhaust_at_month() {
        let calc = Ch4N2oRunningStartCalculator::new();
        let subs = calc.subscriptions();
        assert_eq!(subs.len(), 2);
        let processes: Vec<ProcessId> = subs.iter().map(|s| s.process_id).collect();
        assert!(processes.contains(&RUNNING_EXHAUST));
        assert!(processes.contains(&START_EXHAUST));
        for sub in subs {
            assert_eq!(sub.granularity, Granularity::Month);
            assert_eq!(sub.priority.display(), "EMISSION_CALCULATOR");
        }
    }

    #[test]
    fn calculator_registers_nothing() {
 // CalculatorInfo.txt routes N2O running/start to BaseRateCalculator;
 // calculator-dag.json records registrations_count 0.
        assert!(Ch4N2oRunningStartCalculator::new()
            .registrations()
            .is_empty());
    }

    #[test]
    fn calculator_declares_input_tables() {
        let calc = Ch4N2oRunningStartCalculator::new();
        let tables = calc.input_tables();
        for expected in [
            "County",
            "EmissionProcess",
            "EmissionRate",
            "HourDay",
            "Link",
            "PollutantProcessAssoc",
            "SHO",
            "SourceBin",
            "SourceBinDistribution",
            "SourceTypeModelYear",
            "Starts",
            "Zone",
        ] {
            assert!(tables.contains(&expected), "missing input table {expected}");
        }
 // `upstream` keeps the trait default — no chain dependency edges.
        assert!(calc.upstream().is_empty());
    }

    #[test]
    fn execute_wires_through_data_plane_running() {
        use moves_framework::{DataFrameStore, IterationPosition};
        let inputs = minimal_running_inputs();
        let mut store = moves_framework::InMemoryStore::new();
        store.insert("SHO", ShoRow::into_dataframe(inputs.sho).unwrap());
        store.insert(
            "HourDay",
            HourDayRow::into_dataframe(inputs.hour_day).unwrap(),
        );
        store.insert("Link", LinkRow::into_dataframe(inputs.link).unwrap());
        store.insert("County", CountyRow::into_dataframe(inputs.county).unwrap());
        store.insert(
            "SourceBin",
            SourceBinRow::into_dataframe(inputs.source_bin).unwrap(),
        );
        store.insert(
            "SourceBinDistribution",
            SourceBinDistributionRow::into_dataframe(inputs.source_bin_distribution).unwrap(),
        );
        store.insert(
            "SourceTypeModelYear",
            SourceTypeModelYearRow::into_dataframe(inputs.source_type_model_year).unwrap(),
        );
        store.insert(
            "EmissionRate",
            EmissionRateRow::into_dataframe(inputs.emission_rate).unwrap(),
        );
        store.insert(
            "PollutantProcessAssoc",
            PollutantProcessAssocRow::into_dataframe(inputs.pollutant_process_assoc).unwrap(),
        );
        let pos = IterationPosition {
            process_id: Some(RUNNING_EXHAUST),
            ..Default::default()
        };
        let ctx = CalculatorContext::with_position_and_tables(pos, store);
        let out = Ch4N2oRunningStartCalculator::new()
            .execute(&ctx)
            .expect("execute ok");
        let df = out.dataframe().expect("output should contain a DataFrame");
        assert_eq!(
            df.height(),
            1,
            "minimal running inputs produce exactly one row"
        );
 // SHO 100 × (sbaf 1 × meanBaseRate 2) = 200
        let quant = df
            .column("emissionQuant")
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        assert!(
            (quant - 200.0).abs() < 1e-9,
            "emissionQuant {quant} != 200.0"
        );
    }

    #[test]
    fn execute_wires_through_data_plane_start() {
        use moves_framework::{DataFrameStore, IterationPosition};
        let inputs = minimal_start_inputs();
        let mut store = moves_framework::InMemoryStore::new();
        store.insert("Starts", StartsRow::into_dataframe(inputs.starts).unwrap());
        store.insert(
            "HourDay",
            HourDayRow::into_dataframe(inputs.hour_day).unwrap(),
        );
        store.insert("Link", LinkRow::into_dataframe(inputs.link).unwrap());
        store.insert("County", CountyRow::into_dataframe(inputs.county).unwrap());
        store.insert("Zone", ZoneRow::into_dataframe(inputs.zone).unwrap());
        store.insert(
            "SourceBin",
            SourceBinRow::into_dataframe(inputs.source_bin).unwrap(),
        );
        store.insert(
            "SourceBinDistribution",
            SourceBinDistributionRow::into_dataframe(inputs.source_bin_distribution).unwrap(),
        );
        store.insert(
            "SourceTypeModelYear",
            SourceTypeModelYearRow::into_dataframe(inputs.source_type_model_year).unwrap(),
        );
        store.insert(
            "EmissionRate",
            EmissionRateRow::into_dataframe(inputs.emission_rate).unwrap(),
        );
        store.insert(
            "PollutantProcessAssoc",
            PollutantProcessAssocRow::into_dataframe(inputs.pollutant_process_assoc).unwrap(),
        );
        store.insert(
            "EmissionProcess",
            EmissionProcessRow::into_dataframe(inputs.emission_process).unwrap(),
        );
        let pos = IterationPosition {
            process_id: Some(START_EXHAUST),
            ..Default::default()
        };
        let ctx = CalculatorContext::with_position_and_tables(pos, store);
        let out = Ch4N2oRunningStartCalculator::new()
            .execute(&ctx)
            .expect("execute ok");
        let df = out.dataframe().expect("output should contain a DataFrame");
        assert_eq!(
            df.height(),
            1,
            "minimal start inputs produce exactly one row"
        );
 // sbaf 1 × starts 10 × meanBaseRate 3 = 30
        let quant = df
            .column("emissionQuant")
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        assert!((quant - 30.0).abs() < 1e-9, "emissionQuant {quant} != 30.0");
    }

    #[test]
    fn factory_builds_a_named_calculator() {
        assert_eq!(factory().name(), "CH4N2ORunningStartCalculator");
    }

    #[test]
    fn calculator_is_object_safe() {
 // The registry stores calculators as Box<dyn Calculator>.
        let calc: Box<dyn Calculator> = Box::new(Ch4N2oRunningStartCalculator::new());
        assert_eq!(calc.name(), "CH4N2ORunningStartCalculator");
    }
}
