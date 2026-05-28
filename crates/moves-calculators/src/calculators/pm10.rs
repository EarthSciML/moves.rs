//! Port of `PM10EmissionCalculator` and `PM10BrakeTireCalculator` — the two
//! MOVES PM10 calculators — migration plan Phase 3, Task 55.
//!
//! Java/SQL source:
//! `gov/epa/otaq/moves/master/implementation/ghg/PM10EmissionCalculator.java`
//! + `database/PM10EmissionCalculator.sql`, and `PM10BrakeTireCalculator.java`
//! + `database/PM10BrakeTireCalculator.sql`.
//!
//! # What they compute
//!
//! MOVES does not model PM10 (particulate matter ≤ 10 µm) from first
//! principles. It computes PM2.5 (≤ 2.5 µm) first, then derives PM10 as a
//! tabulated multiple of it: every PM10 emission is its PM2.5 sibling scaled
//! by a `PM10PM25Ratio` looked up from the `PM10EmissionRatio` table.
//!
//! ```text
//! PM10 emissionQuant = PM2.5 emissionQuant × PM10PM25Ratio
//! PM10 emissionRate  = PM2.5 emissionRate  × PM10PM25Ratio
//! ```
//!
//! # Two calculators, one algorithm
//!
//! The two Java classes split the work by emission process, but their SQL
//! "Processing" sections are the same join and the same multiply:
//!
//! | Calculator | PM2.5 → PM10 pollutant | Processes |
//! |------------|------------------------|-----------|
//! | [`PM10EmissionCalculator`] | 110 → 100 (Total exhaust) | running, start, extended idle, auxiliary power, crankcase ×3 |
//! | [`PM10BrakeTireCalculator`] | 116 → 106 (Brakewear), 117 → 107 (Tirewear) | brakewear, tirewear |
//!
//! This module ports the shared "Processing" section once, as the private
//! `compute_pm10`, and gives each Java class a thin [`Calculator`] implementor
//! whose [`calculate`](PM10EmissionCalculator::calculate) supplies only the
//! differing `(PM2.5, PM10)` pollutant pairs.
//!
//! # Chained calculators
//!
//! Both are *chained* calculators: their Java `subscribeToMe` does not
//! subscribe to the MasterLoop but `chainCalculator`s onto whatever upstream
//! calculator produces the PM2.5 they consume. `calculator-dag.json` records
//! `subscribes_directly: false`, `subscriptions: []` for both, with
//! `depends_on: ["SulfatePMCalculator"]` for `PM10EmissionCalculator` (the
//! producer of Total PM2.5, pollutant 110, in the pinned runtime) and
//! `depends_on: ["BaseRateCalculator"]` for `PM10BrakeTireCalculator` (the
//! producer of brake/tire PM2.5). The [`Calculator`] metadata mirrors this:
//! [`subscriptions`](Calculator::subscriptions) is empty and
//! [`upstream`](Calculator::upstream) names that one producer.
//!
//! # The `PM10EmissionRatio` join
//!
//! `PM10EmissionRatio` is keyed `(polProcessID, sourceTypeID, fuelTypeID,
//! minModelYearID, maxModelYearID)` and carries the `PM10PM25Ratio` float.
//! The SQL resolves a worker-output row's ratio with a two-step inner join:
//!
//! 1. `PM10PollutantProcessAssoc` maps the source row's `processID` plus the
//!    target PM10 `pollutantID` to a `polProcessID`.
//! 2. `PM10EmissionRatio` is matched on that `polProcessID`, the source row's
//!    `sourceTypeID` and `fuelTypeID`, and a model-year range that brackets
//!    its `modelYearID`.
//!
//! Every join is an `INNER JOIN`: a worker-output row that resolves no ratio
//! is silently dropped, which `compute_pm10` reproduces with map lookups that
//! `continue` on a miss. `PM10EmissionRatio`'s unique index permits several
//! model-year ranges for one `(polProcessID, sourceType, fuelType)` cell, so
//! the port emits one PM10 row per matching range — the SQL's join
//! cross-product — though disjoint ranges make a single match the norm.
//!
//! Because the `PM10EmissionRatio` join already constrains `sourceTypeID` and
//! `fuelTypeID` to equal the source row's, the SQL's choice to select
//! `r.sourceTypeID` / `r.fuelTypeID` in `PM10EmissionCalculator.sql` and
//! `mwo.sourceTypeID` / `mwo.fuelTypeID` in `PM10BrakeTireCalculator.sql` is a
//! distinction without a difference — the joined values are equal — and the
//! port carries every dimension column straight from the source row.
//!
//! # `PM10EmissionCalculator` — only Total PM10 is live
//!
//! `PM10EmissionCalculator.java` and its SQL `WHERE` carry dormant branches
//! for organic-carbon (101 ← 111), elemental-carbon (102 ← 112) and sulfate
//! (105 ← 115) PM10. Every one of those registrations and pollutant-id
//! appends is commented out in the Java; `sourcePollutantIDs` is only ever
//! `"110"`, so the SQL's `mwo.pollutantID IN (…)` filter admits Total PM2.5
//! alone and the other three `WHERE` branches are unreachable. This port
//! carries the live `110 → 100` pair only; the OC/EC/sulfate branches are
//! reference-only, documented but not ported.
//!
//! `PM10EmissionCalculator`'s extract additionally pulls the three crankcase
//! processes (15, 16, 17) alongside the iteration's process, so a single pass
//! converts crankcase PM2.5 too — hence the seven registrations. That is an
//! extract-scope detail; `compute_pm10` is process-agnostic and converts
//! whatever rows the inputs carry.
//!
//! `PM10BrakeTireCalculator.doExecute` adds `"Brakewear"` / `"Tirewear"` to
//! its enabled-section set, but `PM10BrakeTireCalculator.sql` defines no such
//! sections — the adds are inert. The real execution gate is "at least one of
//! the two processes is in the run", which the compute core reproduces by
//! yielding nothing when no worker-output row matches.
//!
//! # Fidelity notes
//!
//! * **`PM10PM25Ratio` is `FLOAT`.** MOVES stores the ratio in a 32-bit
//!   `FLOAT` column; it is a model *input*, already `f32`-quantised before
//!   [`calculate`](PM10EmissionCalculator::calculate) sees it, so the port
//!   models it as `f64` and the quantisation is the data plane's concern —
//!   matching the `SO2Calculator` treatment of its `FLOAT` input columns. The
//!   product is written to a `DOUBLE` temp column (`PM10MOVESWorkerOutputTemp`),
//!   so no `f32` truncation occurs on the result and the port's `f64` multiply
//!   matches MariaDB's `DOUBLE` arithmetic.
//! * **No division.** The processing pipeline is a single multiplication, so
//!   the MariaDB `int / int` rounding gotcha does not arise.
//! * **`emissionQuant` / `emissionRate` are `DOUBLE NULL`.** The port models
//!   both as a present `f64`; a `NULL` source value (which would propagate a
//!   SQL `NULL` through the product) is a data-plane (Task 50) concern.
//!
//! # Scope and data plane (Task 50)
//!
//! [`calculate`](PM10EmissionCalculator::calculate) ports each SQL script's
//! "Processing" section. Its [`Pm10Inputs`] argument is the set of tables the
//! "Extract Data" section produces; a future Task 50 (`DataFrameStore`)
//! wiring populates it from the per-run filtered execution database and the
//! upstream calculator's `MOVESWorkerOutput` rows.
//!
//! `MOVESRunID`, `iterationID` and `SCC` are pass-through columns the SQL
//! copies verbatim from the source row; following the `SO2Calculator`
//! precedent they are not modelled here — the Task 50 output wiring carries
//! them.
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase 2 placeholders, so it
//! cannot yet read the input tables nor emit `MOVESWorkerOutput`. The numeric
//! algorithm is fully ported and unit-tested on
//! [`calculate`](PM10EmissionCalculator::calculate); `execute` is a
//! documented shell returning an empty [`CalculatorOutput`].

use std::collections::HashMap;

use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::*;

/// Stable module name of the exhaust PM10 calculator — matches the Java class
/// and the `PM10EmissionCalculator` entry in `calculator-dag.json`.
const EMISSION_CALCULATOR_NAME: &str = "PM10EmissionCalculator";

/// Stable module name of the brake/tire PM10 calculator — matches the Java
/// class and the `PM10BrakeTireCalculator` entry in `calculator-dag.json`.
const BRAKE_TIRE_CALCULATOR_NAME: &str = "PM10BrakeTireCalculator";

/// Primary Exhaust PM2.5 - Total — `Pollutant` id 110, the source pollutant
/// `PM10EmissionCalculator` scales into Total Exhaust PM10.
const TOTAL_PM25_POLLUTANT_ID: i32 = 110;
/// Primary Exhaust PM10 - Total — `Pollutant` id 100, the output pollutant
/// `PM10EmissionCalculator` produces.
const TOTAL_PM10_POLLUTANT_ID: i32 = 100;
/// Primary PM2.5 - Brakewear Particulate — `Pollutant` id 116.
const BRAKEWEAR_PM25_POLLUTANT_ID: i32 = 116;
/// Primary PM10 - Brakewear Particulate — `Pollutant` id 106.
const BRAKEWEAR_PM10_POLLUTANT_ID: i32 = 106;
/// Primary PM2.5 - Tirewear Particulate — `Pollutant` id 117.
const TIREWEAR_PM25_POLLUTANT_ID: i32 = 117;
/// Primary PM10 - Tirewear Particulate — `Pollutant` id 107.
const TIREWEAR_PM10_POLLUTANT_ID: i32 = 107;

// ===========================================================================
// Input / output rows — plain Rust mirrors of the tables each SQL script's
// "Extract Data" section pulls and the `MOVESWorkerOutput` rows it produces.
// Following the Phase 3 convention, every `INT`/`SMALLINT` identifier is an
// `i32` and every `FLOAT`/`DOUBLE` quantity is an `f64`.
// ===========================================================================

/// One `MOVESWorkerOutput` row — both an input (the upstream calculator's
/// PM2.5 record) and an output (the PM10 record this calculator appends).
///
/// The two PM10 scripts read `MOVESWorkerOutput`, scale the matching PM2.5
/// rows, and insert PM10 rows back into the same table, so a single struct
/// serves for both directions: an input row carries a PM2.5 `pollutant_id`, an
/// output row the corresponding PM10 `pollutant_id`.
///
/// `MOVESRunID`, `iterationID` and `SCC` are pure pass-through columns the SQL
/// copies verbatim; they are not modelled (see the [module documentation](self)).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MovesWorkerOutputRow {
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
    /// `pollutantID` — a PM2.5 pollutant on an input row, the corresponding
    /// PM10 pollutant on an output row.
    pub pollutant_id: i32,
    /// `processID` — the emission process; carried unchanged onto the PM10 row.
    pub process_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `regClassID`.
    pub reg_class_id: i32,
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `roadTypeID`.
    pub road_type_id: i32,
    /// `emissionQuant` — the emission quantity.
    pub emission_quant: f64,
    /// `emissionRate` — the emission rate.
    pub emission_rate: f64,
}

impl MovesWorkerOutputRow {
    /// The integer dimension tuple — every column except the two emission
    /// values. Used to sort the output deterministically: MOVES leaves
    /// `MOVESWorkerOutput` physically unordered (the SQL `INSERT … SELECT`
    /// has no `ORDER BY`), so the port sorts purely for reproducibility.
    fn dimension_key(&self) -> [i32; 15] {
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
            self.reg_class_id,
            self.fuel_type_id,
            self.model_year_id,
            self.road_type_id,
        ]
    }
}

/// One `PM10EmissionRatio` row — the PM10-to-PM2.5 ratio for a
/// `(polProcessID, sourceType, fuelType, modelYearRange)` cell.
///
/// MOVES keys the table uniquely by `(polProcessID, sourceTypeID, fuelTypeID,
/// minModelYearID, maxModelYearID)`; the unused `PM10PM25RatioCV` column is
/// not modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pm10EmissionRatioRow {
    /// `polProcessID` — `pollutantID × 100 + processID` of the PM10 pollutant;
    /// joins to [`Pm10PollutantProcessAssocRow::pol_process_id`].
    pub pol_process_id: i32,
    /// `sourceTypeID` — the source type the ratio applies to.
    pub source_type_id: i32,
    /// `fuelTypeID` — the fuel type the ratio applies to.
    pub fuel_type_id: i32,
    /// `minModelYearID` — inclusive lower bound of the model-year range.
    pub min_model_year_id: i32,
    /// `maxModelYearID` — inclusive upper bound of the model-year range.
    pub max_model_year_id: i32,
    /// `PM10PM25Ratio` — the PM10 ÷ PM2.5 multiplier. `FLOAT` in MOVES (see
    /// the [module fidelity notes](self)).
    pub pm10_pm25_ratio: f64,
}

/// One `PM10PollutantProcessAssoc` row — a legal `(PM10 pollutant, process)`
/// pairing and the `polProcessID` that keys its emission ratios.
///
/// The SQL builds this remote table as a `SELECT DISTINCT` over
/// `PollutantProcessAssoc`; the unused `isAffectedByExhaustIM` /
/// `isAffectedByEvapIM` columns are not modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pm10PollutantProcessAssocRow {
    /// `polProcessID` — `pollutantID × 100 + processID`.
    pub pol_process_id: i32,
    /// `processID` — the emission process.
    pub process_id: i32,
    /// `pollutantID` — the PM10 pollutant.
    pub pollutant_id: i32,
}

/// Inputs to [`PM10EmissionCalculator::calculate`] /
/// [`PM10BrakeTireCalculator::calculate`] — the tables each SQL script's
/// "Extract Data" section produces, as plain row vectors.
///
/// A future Task 50 (`DataFrameStore`) wiring populates this from the per-run
/// filtered execution database (`PM10EmissionRatio`, `PollutantProcessAssoc`)
/// and the upstream calculator's `MOVESWorkerOutput`; until then it is the
/// explicit data-plane contract the unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct Pm10Inputs {
    /// `MOVESWorkerOutput` rows — the upstream calculator's PM2.5 records. A
    /// row whose pollutant is not an active PM2.5 source for the calculator is
    /// ignored, as the SQL's `WHERE` pollutant-pair block does.
    pub worker_output: Vec<MovesWorkerOutputRow>,
    /// `PM10EmissionRatio` rows — the PM10-to-PM2.5 ratio lookup table.
    pub pm10_emission_ratio: Vec<Pm10EmissionRatioRow>,
    /// `PM10PollutantProcessAssoc` rows — the legal `(PM10 pollutant, process)`
    /// pairs, resolving each to its ratio-keying `polProcessID`.
    pub pm10_pollutant_process_assoc: Vec<Pm10PollutantProcessAssocRow>,
}

// ===========================================================================
// DataFrame ↔ row-struct conversions — `TableRow` implementations and the
// `build_inputs` constructor that reads them from `CalculatorContext`.
// ===========================================================================

fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.to_owned(),
        row,
        column: column.to_owned(),
        message: msg,
    }
}

impl TableRow for MovesWorkerOutputRow {
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
            ("regClassID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("roadTypeID".into(), DataType::Int32),
            ("emissionQuant".into(), DataType::Float64),
            ("emissionRate".into(), DataType::Float64),
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
                    "regClassID".into(),
                    rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>(),
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
                Series::new(
                    "emissionRate".into(),
                    rows.iter().map(|r| r.emission_rate).collect::<Vec<f64>>(),
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
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64()
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
        let reg_class = get_i32("regClassID")?;
        let fuel_type = get_i32("fuelTypeID")?;
        let model_year = get_i32("modelYearID")?;
        let road_type = get_i32("roadTypeID")?;
        let emission_quant = get_f64("emissionQuant")?;
        let emission_rate = get_f64("emissionRate")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(MovesWorkerOutputRow {
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
                    reg_class_id: reg_class.get(i).ok_or_else(|| null("regClassID"))?,
                    fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    model_year_id: model_year.get(i).ok_or_else(|| null("modelYearID"))?,
                    road_type_id: road_type.get(i).ok_or_else(|| null("roadTypeID"))?,
                    emission_quant: emission_quant.get(i).ok_or_else(|| null("emissionQuant"))?,
                    emission_rate: emission_rate.get(i).ok_or_else(|| null("emissionRate"))?,
                })
            })
            .collect()
    }
}

impl TableRow for Pm10EmissionRatioRow {
    fn table_name() -> &'static str {
        "PM10EmissionRatio"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("minModelYearID".into(), DataType::Int32),
            ("maxModelYearID".into(), DataType::Int32),
            ("PM10PM25Ratio".into(), DataType::Float64),
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
                    "minModelYearID".into(),
                    rows.iter()
                        .map(|r| r.min_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "maxModelYearID".into(),
                    rows.iter()
                        .map(|r| r.max_model_year_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "PM10PM25Ratio".into(),
                    rows.iter().map(|r| r.pm10_pm25_ratio).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "PM10EmissionRatio";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_process = get_i32("polProcessID")?;
        let src_type = get_i32("sourceTypeID")?;
        let fuel_type = get_i32("fuelTypeID")?;
        let min_my = get_i32("minModelYearID")?;
        let max_my = get_i32("maxModelYearID")?;
        let ratio = df
            .column("PM10PM25Ratio")
            .map_err(|e| row_err(t, 0, "PM10PM25Ratio", e.to_string()))?
            .f64()
            .map_err(|e| row_err(t, 0, "PM10PM25Ratio", e.to_string()))?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(Pm10EmissionRatioRow {
                    pol_process_id: pol_process.get(i).ok_or_else(|| null("polProcessID"))?,
                    source_type_id: src_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    min_model_year_id: min_my.get(i).ok_or_else(|| null("minModelYearID"))?,
                    max_model_year_id: max_my.get(i).ok_or_else(|| null("maxModelYearID"))?,
                    pm10_pm25_ratio: ratio.get(i).ok_or_else(|| null("PM10PM25Ratio"))?,
                })
            })
            .collect()
    }
}

impl TableRow for Pm10PollutantProcessAssocRow {
    fn table_name() -> &'static str {
        "PollutantProcessAssoc"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
            ("pollutantID".into(), DataType::Int32),
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
                    "processID".into(),
                    rows.iter().map(|r| r.process_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "pollutantID".into(),
                    rows.iter().map(|r| r.pollutant_id).collect::<Vec<i32>>(),
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
        let pol_process = get_i32("polProcessID")?;
        let process = get_i32("processID")?;
        let pollutant = get_i32("pollutantID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(Pm10PollutantProcessAssocRow {
                    pol_process_id: pol_process.get(i).ok_or_else(|| null("polProcessID"))?,
                    process_id: process.get(i).ok_or_else(|| null("processID"))?,
                    pollutant_id: pollutant.get(i).ok_or_else(|| null("pollutantID"))?,
                })
            })
            .collect()
    }
}

/// Read all PM10 calculator input tables from `ctx.tables()`.
fn build_inputs(ctx: &CalculatorContext) -> Result<Pm10Inputs, Error> {
    let tables = ctx.tables();
    let filter = crate::wiring::position_filter(ctx);
    Ok(Pm10Inputs {
        worker_output: tables
            .iter_typed::<MovesWorkerOutputRow>("MOVESWorkerOutput")?
            .into_iter()
            .filter(|r| filter.matches(r.year_id, r.county_id, r.process_id))
            .collect(),
        pm10_emission_ratio: tables.iter_typed("PM10EmissionRatio")?,
        pm10_pollutant_process_assoc: tables.iter_typed("PollutantProcessAssoc")?,
    })
}

/// One active `(source PM2.5 pollutant, output PM10 pollutant)` mapping — one
/// live branch of the SQL "Processing" `WHERE` OR-block.
///
/// `PM10EmissionCalculator` carries one pair (`110 → 100`);
/// `PM10BrakeTireCalculator` carries two (`116 → 106`, `117 → 107`).
#[derive(Debug, Clone, Copy)]
struct Pm10PollutantPair {
    /// `mwo.pollutantID` — the PM2.5 pollutant of the source worker-output row.
    source_pm25: i32,
    /// `ppa.pollutantID` — the PM10 pollutant of the produced row.
    output_pm10: i32,
}

/// The shared "Processing" section of `PM10EmissionCalculator.sql` and
/// `PM10BrakeTireCalculator.sql` — `PM10 = PM2.5 × PM10PM25Ratio`.
///
/// For each `MOVESWorkerOutput` row whose pollutant is an active PM2.5 source
/// in `pollutant_pairs`, this resolves the PM10 emission ratio through the
/// `PM10PollutantProcessAssoc` → `PM10EmissionRatio` inner-join chain and
/// emits a PM10 row with both emission values scaled. A row that resolves no
/// `polProcessID` or no ratio is dropped — every SQL join is an `INNER JOIN`.
/// The result is sorted by its integer dimension columns for deterministic
/// output; MOVES leaves `MOVESWorkerOutput` physically unordered.
fn compute_pm10(
    inputs: &Pm10Inputs,
    pollutant_pairs: &[Pm10PollutantPair],
) -> Vec<MovesWorkerOutputRow> {
    // PM10PollutantProcessAssoc indexed by (processID, pollutantID) →
    // polProcessID. The SQL extract is a `SELECT DISTINCT` and
    // PollutantProcessAssoc's primary key makes polProcessID determine
    // (pollutant, process), so each (processID, pollutantID) cell resolves
    // exactly one polProcessID.
    let ppa_index: HashMap<(i32, i32), i32> = inputs
        .pm10_pollutant_process_assoc
        .iter()
        .map(|p| ((p.process_id, p.pollutant_id), p.pol_process_id))
        .collect();

    // PM10EmissionRatio indexed by (polProcessID, sourceTypeID, fuelTypeID) →
    // the rows for that cell. The table's unique index allows several
    // model-year ranges per cell, so the value is a list.
    let mut ratio_index: HashMap<(i32, i32, i32), Vec<&Pm10EmissionRatioRow>> = HashMap::new();
    for ratio in &inputs.pm10_emission_ratio {
        ratio_index
            .entry((
                ratio.pol_process_id,
                ratio.source_type_id,
                ratio.fuel_type_id,
            ))
            .or_default()
            .push(ratio);
    }

    let mut out: Vec<MovesWorkerOutputRow> = Vec::new();
    for mwo in &inputs.worker_output {
        // WHERE OR-block: the source row's PM2.5 pollutant selects the PM10
        // pollutant of the produced row. A row whose pollutant is not an
        // active PM2.5 source matches no pair and is dropped.
        for pair in pollutant_pairs
            .iter()
            .filter(|p| p.source_pm25 == mwo.pollutant_id)
        {
            // INNER JOIN PM10PollutantProcessAssoc ON processID, with the
            // WHERE pinning ppa.pollutantID to the target PM10 pollutant.
            let Some(&pol_process_id) = ppa_index.get(&(mwo.process_id, pair.output_pm10)) else {
                continue;
            };
            // INNER JOIN PM10EmissionRatio ON polProcessID, sourceTypeID,
            // fuelTypeID, and a model-year range bracketing modelYearID.
            let Some(ratios) =
                ratio_index.get(&(pol_process_id, mwo.source_type_id, mwo.fuel_type_id))
            else {
                continue;
            };
            for ratio in ratios {
                if mwo.model_year_id < ratio.min_model_year_id
                    || mwo.model_year_id > ratio.max_model_year_id
                {
                    continue;
                }
                // PM10 = PM2.5 × PM10PM25Ratio, applied to both the quantity
                // and the rate; every other column is carried from the source
                // row, with only the pollutant relabelled to its PM10 sibling.
                out.push(MovesWorkerOutputRow {
                    pollutant_id: pair.output_pm10,
                    emission_quant: mwo.emission_quant * ratio.pm10_pm25_ratio,
                    emission_rate: mwo.emission_rate * ratio.pm10_pm25_ratio,
                    ..*mwo
                });
            }
        }
    }

    out.sort_unstable_by_key(MovesWorkerOutputRow::dimension_key);
    out
}

/// Both PM10 calculators are chained calculators — `subscribes_directly:
/// false` in `calculator-dag.json` — so neither declares a MasterLoop
/// subscription.
static NO_SUBSCRIPTIONS: &[CalculatorSubscription] = &[];

/// Default-DB tables both PM10 scripts' processing pass consumes.
///
/// `MOVESWorkerOutput` carries the upstream calculator's PM2.5 rows;
/// `PM10EmissionRatio` is the ratio table; `PollutantProcessAssoc` is the
/// source the scripts distill into the remote `PM10PollutantProcessAssoc`. The
/// scripts also join the `RunSpecSourceFuelType` filter table, which only
/// narrows the extract and does not feed the algorithm, so it is not listed
/// (matching the `SO2Calculator` treatment of its `RunSpec*` joins).
static INPUT_TABLES: &[&str] = &[
    "MOVESWorkerOutput",
    "PM10EmissionRatio",
    "PollutantProcessAssoc",
];

// ===========================================================================
// The exhaust PM10 calculator.
// ===========================================================================

/// The active `(PM2.5, PM10)` pollutant pair of `PM10EmissionCalculator` —
/// Total Exhaust PM2.5 (110) → Total Exhaust PM10 (100).
///
/// The Java class's organic-carbon (111 → 101), elemental-carbon (112 → 102)
/// and sulfate (115 → 105) registrations are all commented out, so only Total
/// is live (see the [module documentation](self)).
static EMISSION_POLLUTANT_PAIRS: &[Pm10PollutantPair] = &[Pm10PollutantPair {
    source_pm25: TOTAL_PM25_POLLUTANT_ID,
    output_pm10: TOTAL_PM10_POLLUTANT_ID,
}];

/// The seven `(pollutant, process)` pairs `PM10EmissionCalculator` registers.
///
/// Total Exhaust PM10 (pollutant 100) for the running (1), start (2),
/// extended-idle (90), auxiliary-power (91) and three crankcase (15, 16, 17)
/// exhaust processes — the seven `Registration` directives recorded for
/// `PM10EmissionCalculator` in `CalculatorInfo.txt` (`registrations_count: 7`
/// in `calculator-dag.json`), matching the Java constructor's live
/// `EmissionCalculatorRegistration.register` calls.
static EMISSION_REGISTRATIONS: &[PollutantProcessAssociation] = &[
    PollutantProcessAssociation {
        pollutant_id: PollutantId(100),
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(100),
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(100),
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(100),
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(100),
        process_id: ProcessId(15),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(100),
        process_id: ProcessId(16),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(100),
        process_id: ProcessId(17),
    },
];

/// The upstream calculator `PM10EmissionCalculator` chains off —
/// `SulfatePMCalculator`, the producer of Total PM2.5 (pollutant 110) in the
/// pinned runtime. `calculator-dag.json` records
/// `depends_on: ["SulfatePMCalculator"]`.
static EMISSION_UPSTREAM: &[&str] = &["SulfatePMCalculator"];

/// The MOVES exhaust PM10 calculator — `PM10EmissionCalculator`.
///
/// Scales Total Exhaust PM2.5 (pollutant 110) into Total Exhaust PM10
/// (pollutant 100) for the running, start, extended-idle, auxiliary-power and
/// crankcase exhaust processes. A zero-sized value type owning no per-run
/// state, as the [`Calculator`] trait requires; all run-varying input flows
/// through the [`Pm10Inputs`] argument to [`calculate`](Self::calculate).
#[derive(Debug, Clone, Copy, Default)]
pub struct PM10EmissionCalculator;

impl PM10EmissionCalculator {
    /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = EMISSION_CALCULATOR_NAME;

    /// Construct the calculator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Compute the Total Exhaust PM10 rows — the port of the
    /// `PM10EmissionCalculator.sql` "Processing" section.
    ///
    /// Each `MOVESWorkerOutput` row carrying Total PM2.5 (pollutant 110) is
    /// scaled by its `PM10EmissionRatio` into a Total PM10 (pollutant 100)
    /// row; a row that resolves no ratio is dropped. The result is sorted by
    /// its integer dimension columns for deterministic output.
    #[must_use]
    pub fn calculate(&self, inputs: &Pm10Inputs) -> Vec<MovesWorkerOutputRow> {
        compute_pm10(inputs, EMISSION_POLLUTANT_PAIRS)
    }
}

impl Calculator for PM10EmissionCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// `PM10EmissionCalculator` is a chained calculator: it does not subscribe
    /// to the MasterLoop directly but fires when its upstream
    /// `SulfatePMCalculator` does. `calculator-dag.json` records
    /// `subscribes_directly: false` and an empty `subscriptions` list.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        NO_SUBSCRIPTIONS
    }

    fn registrations(&self) -> &[PollutantProcessAssociation] {
        EMISSION_REGISTRATIONS
    }

    /// `PM10EmissionCalculator` chains off `SulfatePMCalculator` —
    /// `calculator-dag.json` records `depends_on: ["SulfatePMCalculator"]`.
    fn upstream(&self) -> &[&'static str] {
        EMISSION_UPSTREAM
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let inputs = build_inputs(ctx)?;
        let rows = self.calculate(&inputs);
        crate::wiring::emit_rows(rows)
    }
}

// ===========================================================================
// The brake/tire PM10 calculator.
// ===========================================================================

/// The two active `(PM2.5, PM10)` pollutant pairs of `PM10BrakeTireCalculator`
/// — Brakewear PM2.5 (116) → Brakewear PM10 (106) and Tirewear PM2.5 (117) →
/// Tirewear PM10 (107).
static BRAKE_TIRE_POLLUTANT_PAIRS: &[Pm10PollutantPair] = &[
    Pm10PollutantPair {
        source_pm25: BRAKEWEAR_PM25_POLLUTANT_ID,
        output_pm10: BRAKEWEAR_PM10_POLLUTANT_ID,
    },
    Pm10PollutantPair {
        source_pm25: TIREWEAR_PM25_POLLUTANT_ID,
        output_pm10: TIREWEAR_PM10_POLLUTANT_ID,
    },
];

/// The two `(pollutant, process)` pairs `PM10BrakeTireCalculator` registers.
///
/// Brakewear PM10 (pollutant 106) for the Brakewear process (9) and Tirewear
/// PM10 (pollutant 107) for the Tirewear process (10) — the two `Registration`
/// directives recorded for `PM10BrakeTireCalculator` in `CalculatorInfo.txt`
/// (`registrations_count: 2` in `calculator-dag.json`), matching the Java
/// constructor's `EmissionCalculatorRegistration.register` calls.
static BRAKE_TIRE_REGISTRATIONS: &[PollutantProcessAssociation] = &[
    PollutantProcessAssociation {
        pollutant_id: PollutantId(106),
        process_id: ProcessId(9),
    },
    PollutantProcessAssociation {
        pollutant_id: PollutantId(107),
        process_id: ProcessId(10),
    },
];

/// The upstream calculator `PM10BrakeTireCalculator` chains off —
/// `BaseRateCalculator`, the producer of brake/tire PM2.5 (pollutants 116 /
/// 117) in the pinned runtime. `calculator-dag.json` records
/// `depends_on: ["BaseRateCalculator"]`.
static BRAKE_TIRE_UPSTREAM: &[&str] = &["BaseRateCalculator"];

/// The MOVES brake/tire PM10 calculator — `PM10BrakeTireCalculator`.
///
/// Scales brake-wear PM2.5 (pollutant 116) and tire-wear PM2.5 (pollutant 117)
/// into their PM10 siblings (106 / 107) for the Brakewear (9) and Tirewear
/// (10) processes. A zero-sized value type owning no per-run state, as the
/// [`Calculator`] trait requires; all run-varying input flows through the
/// [`Pm10Inputs`] argument to [`calculate`](Self::calculate).
#[derive(Debug, Clone, Copy, Default)]
pub struct PM10BrakeTireCalculator;

impl PM10BrakeTireCalculator {
    /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = BRAKE_TIRE_CALCULATOR_NAME;

    /// Construct the calculator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Compute the brake/tire PM10 rows — the port of the
    /// `PM10BrakeTireCalculator.sql` "Processing" section.
    ///
    /// Each `MOVESWorkerOutput` row carrying brake-wear (116) or tire-wear
    /// (117) PM2.5 is scaled by its `PM10EmissionRatio` into the PM10 sibling
    /// pollutant (106 / 107); a row that resolves no ratio is dropped. The
    /// result is sorted by its integer dimension columns for deterministic
    /// output.
    #[must_use]
    pub fn calculate(&self, inputs: &Pm10Inputs) -> Vec<MovesWorkerOutputRow> {
        compute_pm10(inputs, BRAKE_TIRE_POLLUTANT_PAIRS)
    }
}

impl Calculator for PM10BrakeTireCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// `PM10BrakeTireCalculator` is a chained calculator: it does not subscribe
    /// to the MasterLoop directly but fires when its upstream
    /// `BaseRateCalculator` does. `calculator-dag.json` records
    /// `subscribes_directly: false` and an empty `subscriptions` list.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        NO_SUBSCRIPTIONS
    }

    fn registrations(&self) -> &[PollutantProcessAssociation] {
        BRAKE_TIRE_REGISTRATIONS
    }

    /// `PM10BrakeTireCalculator` chains off `BaseRateCalculator` —
    /// `calculator-dag.json` records `depends_on: ["BaseRateCalculator"]`.
    fn upstream(&self) -> &[&'static str] {
        BRAKE_TIRE_UPSTREAM
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let inputs = build_inputs(ctx)?;
        let rows = self.calculate(&inputs);
        crate::wiring::emit_rows(rows)
    }
}

pub fn emission_factory() -> Box<dyn Calculator> {
    Box::new(PM10EmissionCalculator::new())
}

pub fn brake_tire_factory() -> Box<dyn Calculator> {
    Box::new(PM10BrakeTireCalculator::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a `MOVESWorkerOutput` row with the given pollutant and process,
    /// fixed dimension columns, `emissionQuant = 200.0` and
    /// `emissionRate = 5.0`. Values are chosen for exact scaled results, not
    /// physical realism.
    fn worker_row(pollutant_id: i32, process_id: i32) -> MovesWorkerOutputRow {
        MovesWorkerOutputRow {
            year_id: 2020,
            month_id: 7,
            day_id: 5,
            hour_id: 8,
            state_id: 26,
            county_id: 26_161,
            zone_id: 261_610,
            link_id: 5001,
            pollutant_id,
            process_id,
            source_type_id: 21,
            reg_class_id: 30,
            fuel_type_id: 2,
            model_year_id: 2015,
            road_type_id: 4,
            emission_quant: 200.0,
            emission_rate: 5.0,
        }
    }

    /// Build a `PM10EmissionRatio` row for the given `polProcessID` and ratio,
    /// matching `worker_row`'s source/fuel type and an all-encompassing
    /// model-year range.
    fn ratio_row(pol_process_id: i32, ratio: f64) -> Pm10EmissionRatioRow {
        Pm10EmissionRatioRow {
            pol_process_id,
            source_type_id: 21,
            fuel_type_id: 2,
            min_model_year_id: 1960,
            max_model_year_id: 2050,
            pm10_pm25_ratio: ratio,
        }
    }

    /// Build a `PM10PollutantProcessAssoc` row, deriving `polProcessID` as
    /// `pollutantID × 100 + processID` exactly as the MOVES default DB does.
    fn ppa_row(process_id: i32, pollutant_id: i32) -> Pm10PollutantProcessAssocRow {
        Pm10PollutantProcessAssocRow {
            pol_process_id: pollutant_id * 100 + process_id,
            process_id,
            pollutant_id,
        }
    }

    /// Assert `actual` matches `expected` within `f64` slack.
    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "{actual} != expected {expected}",
        );
    }

    #[test]
    fn emission_scales_total_pm25_into_total_pm10() {
        let inputs = Pm10Inputs {
            worker_output: vec![worker_row(TOTAL_PM25_POLLUTANT_ID, 1)],
            // PM10 Total (100), Running (1) → polProcessID 10001.
            pm10_emission_ratio: vec![ratio_row(10_001, 1.5)],
            pm10_pollutant_process_assoc: vec![ppa_row(1, TOTAL_PM10_POLLUTANT_ID)],
        };
        let rows = PM10EmissionCalculator::new().calculate(&inputs);
        assert_eq!(rows.len(), 1);
        let r = rows[0];
        // The pollutant is relabelled 110 → 100; the process is carried through.
        assert_eq!(r.pollutant_id, TOTAL_PM10_POLLUTANT_ID);
        assert_eq!(r.process_id, 1);
        // Both emission values are scaled by the ratio.
        assert_close(r.emission_quant, 300.0); // 200.0 × 1.5
        assert_close(r.emission_rate, 7.5); // 5.0 × 1.5
    }

    #[test]
    fn emission_carries_every_dimension_column_through() {
        let inputs = Pm10Inputs {
            worker_output: vec![worker_row(TOTAL_PM25_POLLUTANT_ID, 1)],
            pm10_emission_ratio: vec![ratio_row(10_001, 2.0)],
            pm10_pollutant_process_assoc: vec![ppa_row(1, TOTAL_PM10_POLLUTANT_ID)],
        };
        let rows = PM10EmissionCalculator::new().calculate(&inputs);
        assert_eq!(rows.len(), 1);
        let r = rows[0];
        // Every dimension column but the pollutant is copied from the source.
        let src = worker_row(TOTAL_PM25_POLLUTANT_ID, 1);
        assert_eq!(r.year_id, src.year_id);
        assert_eq!(r.month_id, src.month_id);
        assert_eq!(r.day_id, src.day_id);
        assert_eq!(r.hour_id, src.hour_id);
        assert_eq!(r.state_id, src.state_id);
        assert_eq!(r.county_id, src.county_id);
        assert_eq!(r.zone_id, src.zone_id);
        assert_eq!(r.link_id, src.link_id);
        assert_eq!(r.source_type_id, src.source_type_id);
        assert_eq!(r.reg_class_id, src.reg_class_id);
        assert_eq!(r.fuel_type_id, src.fuel_type_id);
        assert_eq!(r.model_year_id, src.model_year_id);
        assert_eq!(r.road_type_id, src.road_type_id);
    }

    #[test]
    fn emission_drops_non_total_pm25_source_rows() {
        // Organic-carbon (111) and elemental-carbon (112) PM2.5 are dormant
        // branches of PM10EmissionCalculator — only Total (110) is live.
        let inputs = Pm10Inputs {
            worker_output: vec![worker_row(111, 1), worker_row(112, 1)],
            pm10_emission_ratio: vec![
                ratio_row(10_001, 1.5),
                ratio_row(101 * 100 + 1, 1.5),
                ratio_row(102 * 100 + 1, 1.5),
            ],
            pm10_pollutant_process_assoc: vec![
                ppa_row(1, TOTAL_PM10_POLLUTANT_ID),
                ppa_row(1, 101),
                ppa_row(1, 102),
            ],
        };
        assert!(PM10EmissionCalculator::new().calculate(&inputs).is_empty());
    }

    #[test]
    fn emission_converts_crankcase_process_rows() {
        // PM10EmissionCalculator registers and converts the three crankcase
        // exhaust processes (15, 16, 17) too.
        let inputs = Pm10Inputs {
            worker_output: vec![worker_row(TOTAL_PM25_POLLUTANT_ID, 15)],
            // PM10 Total (100), Crankcase Running (15) → polProcessID 10015.
            pm10_emission_ratio: vec![ratio_row(10_015, 1.25)],
            pm10_pollutant_process_assoc: vec![ppa_row(15, TOTAL_PM10_POLLUTANT_ID)],
        };
        let rows = PM10EmissionCalculator::new().calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].process_id, 15);
        assert_eq!(rows[0].pollutant_id, TOTAL_PM10_POLLUTANT_ID);
        assert_close(rows[0].emission_quant, 250.0); // 200.0 × 1.25
    }

    #[test]
    fn emission_drops_row_with_no_pollutant_process_assoc() {
        // No PM10PollutantProcessAssoc row for the process — the inner join
        // drops the source row.
        let inputs = Pm10Inputs {
            worker_output: vec![worker_row(TOTAL_PM25_POLLUTANT_ID, 1)],
            pm10_emission_ratio: vec![ratio_row(10_001, 1.5)],
            pm10_pollutant_process_assoc: vec![],
        };
        assert!(PM10EmissionCalculator::new().calculate(&inputs).is_empty());
    }

    #[test]
    fn emission_drops_row_with_no_matching_ratio() {
        let base = Pm10Inputs {
            worker_output: vec![worker_row(TOTAL_PM25_POLLUTANT_ID, 1)],
            pm10_emission_ratio: vec![ratio_row(10_001, 1.5)],
            pm10_pollutant_process_assoc: vec![ppa_row(1, TOTAL_PM10_POLLUTANT_ID)],
        };

        // No ratio at all → dropped.
        let mut no_ratio = base.clone();
        no_ratio.pm10_emission_ratio.clear();
        assert!(PM10EmissionCalculator::new()
            .calculate(&no_ratio)
            .is_empty());

        // A ratio whose source type does not match the worker row → dropped.
        let mut wrong_source_type = base.clone();
        wrong_source_type.pm10_emission_ratio[0].source_type_id = 99;
        assert!(PM10EmissionCalculator::new()
            .calculate(&wrong_source_type)
            .is_empty());

        // A ratio whose fuel type does not match → dropped.
        let mut wrong_fuel_type = base;
        wrong_fuel_type.pm10_emission_ratio[0].fuel_type_id = 99;
        assert!(PM10EmissionCalculator::new()
            .calculate(&wrong_fuel_type)
            .is_empty());
    }

    #[test]
    fn emission_drops_row_outside_the_ratio_model_year_range() {
        let mut inputs = Pm10Inputs {
            worker_output: vec![worker_row(TOTAL_PM25_POLLUTANT_ID, 1)],
            pm10_emission_ratio: vec![ratio_row(10_001, 1.5)],
            pm10_pollutant_process_assoc: vec![ppa_row(1, TOTAL_PM10_POLLUTANT_ID)],
        };
        // worker_row's modelYearID is 2015; narrow the ratio range past it.
        inputs.pm10_emission_ratio[0].min_model_year_id = 2016;
        inputs.pm10_emission_ratio[0].max_model_year_id = 2020;
        assert!(PM10EmissionCalculator::new().calculate(&inputs).is_empty());
    }

    #[test]
    fn emission_model_year_range_bounds_are_inclusive() {
        let mut inputs = Pm10Inputs {
            worker_output: vec![worker_row(TOTAL_PM25_POLLUTANT_ID, 1)],
            pm10_emission_ratio: vec![ratio_row(10_001, 1.5)],
            pm10_pollutant_process_assoc: vec![ppa_row(1, TOTAL_PM10_POLLUTANT_ID)],
        };
        // A range that exactly equals the worker row's model year still matches.
        inputs.pm10_emission_ratio[0].min_model_year_id = 2015;
        inputs.pm10_emission_ratio[0].max_model_year_id = 2015;
        assert_eq!(PM10EmissionCalculator::new().calculate(&inputs).len(), 1);
    }

    #[test]
    fn emission_emits_one_row_per_matching_ratio_range() {
        // Two PM10EmissionRatio rows whose model-year ranges both cover the
        // worker row's model year — the SQL join cross-product yields two PM10
        // rows.
        let inputs = Pm10Inputs {
            worker_output: vec![worker_row(TOTAL_PM25_POLLUTANT_ID, 1)],
            pm10_emission_ratio: vec![
                Pm10EmissionRatioRow {
                    min_model_year_id: 2010,
                    max_model_year_id: 2020,
                    ..ratio_row(10_001, 1.5)
                },
                Pm10EmissionRatioRow {
                    min_model_year_id: 2000,
                    max_model_year_id: 2030,
                    pm10_pm25_ratio: 2.0,
                    ..ratio_row(10_001, 2.0)
                },
            ],
            pm10_pollutant_process_assoc: vec![ppa_row(1, TOTAL_PM10_POLLUTANT_ID)],
        };
        let rows = PM10EmissionCalculator::new().calculate(&inputs);
        assert_eq!(rows.len(), 2);
        let mut quants: Vec<f64> = rows.iter().map(|r| r.emission_quant).collect();
        quants.sort_by(f64::total_cmp);
        assert_close(quants[0], 300.0); // 200.0 × 1.5
        assert_close(quants[1], 400.0); // 200.0 × 2.0
    }

    #[test]
    fn brake_tire_scales_brakewear_and_tirewear_pm25() {
        let inputs = Pm10Inputs {
            worker_output: vec![
                worker_row(BRAKEWEAR_PM25_POLLUTANT_ID, 9),
                worker_row(TIREWEAR_PM25_POLLUTANT_ID, 10),
            ],
            pm10_emission_ratio: vec![
                // PM10 Brakewear (106), Brakewear (9) → polProcessID 10609.
                ratio_row(10_609, 1.5),
                // PM10 Tirewear (107), Tirewear (10) → polProcessID 10710.
                ratio_row(10_710, 3.0),
            ],
            pm10_pollutant_process_assoc: vec![
                ppa_row(9, BRAKEWEAR_PM10_POLLUTANT_ID),
                ppa_row(10, TIREWEAR_PM10_POLLUTANT_ID),
            ],
        };
        let rows = PM10BrakeTireCalculator::new().calculate(&inputs);
        assert_eq!(rows.len(), 2);

        let brake = rows
            .iter()
            .find(|r| r.process_id == 9)
            .expect("a brakewear row");
        assert_eq!(brake.pollutant_id, BRAKEWEAR_PM10_POLLUTANT_ID);
        assert_close(brake.emission_quant, 300.0); // 200.0 × 1.5

        let tire = rows
            .iter()
            .find(|r| r.process_id == 10)
            .expect("a tirewear row");
        assert_eq!(tire.pollutant_id, TIREWEAR_PM10_POLLUTANT_ID);
        assert_close(tire.emission_quant, 600.0); // 200.0 × 3.0
    }

    #[test]
    fn brake_tire_drops_unmapped_pollutants() {
        // Total PM2.5 (110) is not a brake/tire source — only 116 / 117 are.
        let inputs = Pm10Inputs {
            worker_output: vec![worker_row(TOTAL_PM25_POLLUTANT_ID, 9)],
            pm10_emission_ratio: vec![ratio_row(10_609, 1.5)],
            pm10_pollutant_process_assoc: vec![ppa_row(9, BRAKEWEAR_PM10_POLLUTANT_ID)],
        };
        assert!(PM10BrakeTireCalculator::new().calculate(&inputs).is_empty());
    }

    #[test]
    fn output_is_sorted_by_dimension_key() {
        // Two worker rows on distinct links produce two PM10 rows; the result
        // comes back dimension-key sorted regardless of input order.
        let mut high_link = worker_row(TOTAL_PM25_POLLUTANT_ID, 1);
        high_link.link_id = 9999; // sorts after link 5001
        let inputs = Pm10Inputs {
            worker_output: vec![high_link, worker_row(TOTAL_PM25_POLLUTANT_ID, 1)],
            pm10_emission_ratio: vec![ratio_row(10_001, 1.5)],
            pm10_pollutant_process_assoc: vec![ppa_row(1, TOTAL_PM10_POLLUTANT_ID)],
        };
        let rows = PM10EmissionCalculator::new().calculate(&inputs);
        assert_eq!(rows.len(), 2);
        assert!(
            rows.windows(2)
                .all(|w| w[0].dimension_key() <= w[1].dimension_key()),
            "calculate output is not sorted by dimension key",
        );
        assert_eq!(rows[0].link_id, 5001);
        assert_eq!(rows[1].link_id, 9999);
    }

    #[test]
    fn empty_input_yields_no_rows() {
        assert!(PM10EmissionCalculator::new()
            .calculate(&Pm10Inputs::default())
            .is_empty());
        assert!(PM10BrakeTireCalculator::new()
            .calculate(&Pm10Inputs::default())
            .is_empty());
    }

    #[test]
    fn calculator_names_match_dag_modules() {
        assert_eq!(
            PM10EmissionCalculator::new().name(),
            "PM10EmissionCalculator"
        );
        assert_eq!(PM10EmissionCalculator::NAME, "PM10EmissionCalculator");
        assert_eq!(
            PM10BrakeTireCalculator::new().name(),
            "PM10BrakeTireCalculator"
        );
        assert_eq!(PM10BrakeTireCalculator::NAME, "PM10BrakeTireCalculator");
    }

    #[test]
    fn both_calculators_are_chained_with_no_subscriptions() {
        // calculator-dag.json: subscribes_directly false, subscriptions [].
        assert!(PM10EmissionCalculator::new().subscriptions().is_empty());
        assert!(PM10BrakeTireCalculator::new().subscriptions().is_empty());
    }

    #[test]
    fn emission_registrations_match_the_seven_calculator_info_directives() {
        // calculator-dag.json records registrations_count 7: PM10 Total (100)
        // for the running (1), start (2), extended-idle (90), aux-power (91)
        // and crankcase (15, 16, 17) exhaust processes.
        let calc = PM10EmissionCalculator::new();
        let regs = calc.registrations();
        assert_eq!(regs.len(), 7);
        assert!(regs.iter().all(|r| r.pollutant_id == PollutantId(100)));
        let mut procs: Vec<u16> = regs.iter().map(|r| r.process_id.0).collect();
        procs.sort_unstable();
        assert_eq!(procs, vec![1, 2, 15, 16, 17, 90, 91]);
    }

    #[test]
    fn brake_tire_registrations_match_the_two_calculator_info_directives() {
        // calculator-dag.json records registrations_count 2: PM10 Brakewear
        // (106) for Brakewear (9), PM10 Tirewear (107) for Tirewear (10).
        let calc = PM10BrakeTireCalculator::new();
        let regs = calc.registrations();
        assert_eq!(regs.len(), 2);
        assert!(regs.contains(&PollutantProcessAssociation {
            pollutant_id: PollutantId(106),
            process_id: ProcessId(9),
        }));
        assert!(regs.contains(&PollutantProcessAssociation {
            pollutant_id: PollutantId(107),
            process_id: ProcessId(10),
        }));
    }

    #[test]
    fn calculators_chain_off_their_dag_upstream() {
        // calculator-dag.json depends_on entries.
        assert_eq!(
            PM10EmissionCalculator::new().upstream(),
            &["SulfatePMCalculator"]
        );
        assert_eq!(
            PM10BrakeTireCalculator::new().upstream(),
            &["BaseRateCalculator"]
        );
    }

    #[test]
    fn calculators_declare_their_input_tables() {
        for tables in [
            PM10EmissionCalculator::new().input_tables(),
            PM10BrakeTireCalculator::new().input_tables(),
        ] {
            for expected in [
                "MOVESWorkerOutput",
                "PM10EmissionRatio",
                "PollutantProcessAssoc",
            ] {
                assert!(tables.contains(&expected), "missing input table {expected}");
            }
        }
    }

    #[test]
    fn execute_wires_through_data_plane() {
        use moves_framework::DataFrameStore;
        // Seed store with the emission_scales_total_pm25_into_total_pm10 scenario:
        // one Total PM2.5 (110) worker row, one PM10EmissionRatio (ratio 1.5),
        // one PollutantProcessAssoc mapping (100, process 1) → polProcessID 10001.
        let worker = worker_row(TOTAL_PM25_POLLUTANT_ID, 1);
        let ratio = ratio_row(10_001, 1.5);
        let ppa = ppa_row(1, TOTAL_PM10_POLLUTANT_ID);

        let mut store = moves_framework::InMemoryStore::new();
        store.insert(
            "MOVESWorkerOutput",
            MovesWorkerOutputRow::into_dataframe(vec![worker]).unwrap(),
        );
        store.insert(
            "PM10EmissionRatio",
            Pm10EmissionRatioRow::into_dataframe(vec![ratio]).unwrap(),
        );
        store.insert(
            "PollutantProcessAssoc",
            Pm10PollutantProcessAssocRow::into_dataframe(vec![ppa]).unwrap(),
        );

        let ctx = CalculatorContext::with_tables(store);
        let out = PM10EmissionCalculator::new()
            .execute(&ctx)
            .expect("execute ok");
        let df = out.dataframe().expect("output should contain a DataFrame");
        assert_eq!(df.height(), 1, "one PM2.5 source row produces one PM10 row");
        let quant = df
            .column("emissionQuant")
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        let rate = df
            .column("emissionRate")
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .unwrap();
        assert!(
            (quant - 300.0).abs() < 1e-9,
            "emissionQuant {quant} != 300.0"
        );
        assert!((rate - 7.5).abs() < 1e-9, "emissionRate {rate} != 7.5");
    }

    #[test]
    fn brake_tire_execute_wires_through_data_plane() {
        use moves_framework::DataFrameStore;
        // Seed store with the brake_tire_scales_brakewear_and_tirewear_pm25 scenario.
        let workers = vec![
            worker_row(BRAKEWEAR_PM25_POLLUTANT_ID, 9),
            worker_row(TIREWEAR_PM25_POLLUTANT_ID, 10),
        ];
        let ratios = vec![ratio_row(10_609, 1.5), ratio_row(10_710, 3.0)];
        let ppas = vec![
            ppa_row(9, BRAKEWEAR_PM10_POLLUTANT_ID),
            ppa_row(10, TIREWEAR_PM10_POLLUTANT_ID),
        ];

        let mut store = moves_framework::InMemoryStore::new();
        store.insert(
            "MOVESWorkerOutput",
            MovesWorkerOutputRow::into_dataframe(workers).unwrap(),
        );
        store.insert(
            "PM10EmissionRatio",
            Pm10EmissionRatioRow::into_dataframe(ratios).unwrap(),
        );
        store.insert(
            "PollutantProcessAssoc",
            Pm10PollutantProcessAssocRow::into_dataframe(ppas).unwrap(),
        );

        let ctx = CalculatorContext::with_tables(store);
        let out = PM10BrakeTireCalculator::new()
            .execute(&ctx)
            .expect("execute ok");
        let df = out.dataframe().expect("output should contain a DataFrame");
        assert_eq!(
            df.height(),
            2,
            "two PM2.5 source rows produce two PM10 rows"
        );
    }

    #[test]
    fn calculators_are_object_safe() {
        // The registry stores calculators as Box<dyn Calculator>.
        let calcs: Vec<Box<dyn Calculator>> = vec![
            Box::new(PM10EmissionCalculator::new()),
            Box::new(PM10BrakeTireCalculator::new()),
        ];
        assert_eq!(calcs[0].name(), "PM10EmissionCalculator");
        assert_eq!(calcs[1].name(), "PM10BrakeTireCalculator");
    }
}
