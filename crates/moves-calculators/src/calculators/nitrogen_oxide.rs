//! Port of `NOCalculator` and `NO2Calculator` — the two MOVES nitrogen-oxide
//! speciation calculators — migration plan Phase 3, Task 68.
//!
//! Java/SQL source:
//! `gov/epa/otaq/moves/master/implementation/ghg/NOCalculator.java`
//! + `database/NOCalculator.sql`, and `NO2Calculator.java`
//! + `database/NO2Calculator.sql`.
//!
//! # What they compute
//!
//! MOVES does not model the individual nitrogen-oxide species from first
//! principles. It computes total **Oxides of Nitrogen** (NOx, pollutant 3)
//! first, then derives each species as a tabulated fraction of it: every
//! species emission is its NOx parent scaled by a `NOxRatio` looked up from
//! the `NONO2Ratio` table.
//!
//! ```text
//! species emissionQuant = NOx emissionQuant × NOxRatio
//! species emissionRate  = NOx emissionRate  × NOxRatio
//! ```
//!
//! # Two calculators, one algorithm
//!
//! The two Java classes split the work by output species, but their SQL
//! "Processing" sections are the same two joins and the same multiply:
//!
//! | Calculator | Species produced | Scaled from |
//! |------------|------------------|-------------|
//! | [`NOCalculator`] | Nitrogen Oxide NO (32), Nitrous Acid HONO (34) | Oxides of Nitrogen (3) |
//! | [`NO2Calculator`] | Nitrogen Dioxide NO2 (33) | Oxides of Nitrogen (3) |
//!
//! The only difference is which `NONO2Ratio` / `PollutantProcessAssoc` rows
//! the SQL "Extract Data" section feeds in: `NOCalculator.sql` filters them to
//! the NO and HONO `polProcessID`s requested by the RunSpec, `NO2Calculator.sql`
//! to the four NO2 `polProcessID`s (`3301, 3302, 3390, 3391`). That is an
//! extract-scope detail — the "Processing" section is `polProcessID`-agnostic
//! and speciates whatever ratios the inputs carry. This module therefore ports
//! the shared "Processing" section once, as the private `compute_nitrogen_oxide`,
//! and gives each Java class a thin [`Calculator`] implementor whose
//! [`calculate`](NOCalculator::calculate) is the identical call.
//!
//! # Chained calculators
//!
//! Both are *chained* calculators: their Java `subscribeToMe` does not
//! subscribe to the MasterLoop but `chainCalculator`s onto whatever upstream
//! calculator produces the total NOx (pollutant 3) they consume — in the
//! rates-first engine that is `BaseRateCalculator`.
//! `characterization/calculator-chains/calculator-dag.json` records
//! `subscribes_directly: false`, `subscriptions: []` and
//! `depends_on: ["BaseRateCalculator"]` for both, matching the
//! `Chain NOCalculator BaseRateCalculator` / `Chain NO2Calculator
//! BaseRateCalculator` directives in `CalculatorInfo.txt`. The [`Calculator`]
//! metadata mirrors this: [`subscriptions`](Calculator::subscriptions) is
//! empty and [`upstream`](Calculator::upstream) names `BaseRateCalculator`.
//!
//! # Algorithm — the SQL "Processing" section
//!
//! `compute_nitrogen_oxide` ports each SQL script's "Processing" section. The
//! SQL builds two working tables; the port folds them into two index maps and
//! one join loop:
//!
//! | SQL working table | This port |
//! |-------------------|-----------|
//! | `NOCalculation1` | `(fuelTypeID, modelYearID, sourceTypeID) → [(processID, pollutantID, NOxRatio)]` |
//! | `NOMOVESOutputTemp1` | the per-NOx-row join loop and the returned `Vec<`[`MovesWorkerOutputRow`]`>` |
//!
//! `NOCalculation1` "adds dimensions to NOxRatio": it joins `NONO2Ratio` to
//! `PollutantProcessAssoc` (resolving `polProcessID` into its process and
//! species pollutant), filters it through a `SourceUseType` existence join,
//! and joins `PollutantProcessMappedModelYear` to expand each ratio's
//! `modelYearGroupID` into the individual model years that group covers — so
//! one `NONO2Ratio` row yields one `NOCalculation1` row per covered model
//! year. `NOMOVESOutputTemp1` joins the NOx `MOVESWorkerOutput` rows
//! (`pollutantID = 3`) to `NOCalculation1` on `(fuelTypeID, modelYearID,
//! sourceTypeID)` and applies the multiply.
//!
//! Every SQL join is an `INNER JOIN`, so a row with no match on the join key
//! is dropped; the port reproduces that with map lookups that skip on a miss.
//!
//! A single NOx row can produce several species rows: for `NOCalculator` a
//! `(fuelType, modelYear, sourceType)` cell carries both an NO and a HONO
//! `NOCalculation1` row when the RunSpec requests both, so the join emits one
//! output row per species — the SQL's join cross-product.
//!
//! # Single-process invocation
//!
//! `NOMOVESOutputTemp1`'s join does **not** constrain `mwo.processID` to
//! `noc.processID`; the only process filter is the extract's `mwo.processID =
//! ##context.iterProcess.databaseKey##` on the NOx rows and the matching
//! `processID = ##context…##` on the `PollutantProcessAssoc` extract. A
//! master-loop invocation is therefore single-process: every input row shares
//! the iteration's process, so the join is unambiguous and the output
//! `processID` — taken from `NOCalculation1`, as the SQL's `SELECT
//! noc.processID` dictates — equals the NOx row's. `compute_nitrogen_oxide`
//! reproduces the join exactly (on `(fuelTypeID, modelYearID, sourceTypeID)`
//! alone) and carries `processID` from the resolved ratio cell; the
//! single-process extract filter is the Task 50 data plane's concern.
//! Likewise `noc.sourceTypeID` — which the join pins equal to the NOx row's —
//! is carried straight from the NOx row.
//!
//! # `FuelType` extracted but unused
//!
//! Both SQL scripts' "Extract Data" sections pull a `NOCopyOf*FuelType` table
//! (`SELECT DISTINCT fuelTypeID FROM FuelType`), but neither "Processing"
//! section joins it — only `SourceUseType` is used as an existence filter.
//! `FuelType` is therefore not part of the algorithm and is not modelled or
//! listed in [`input_tables`](Calculator::input_tables).
//!
//! # Fidelity notes
//!
//! * **`NOxRatio` is `FLOAT`.** MOVES stores the ratio in a 32-bit `FLOAT`
//!   `NONO2Ratio` column; it is a model *input*, already `f32`-quantised
//!   before [`calculate`](NOCalculator::calculate) sees it, so the port models
//!   it as `f64` and the quantisation is the data plane's concern — matching
//!   the `SO2Calculator` / `PM10EmissionCalculator` treatment of their `FLOAT`
//!   input columns. `NOCalculation1.NOxRatio` is itself a `FLOAT` temp column,
//!   but copying an already-`FLOAT` value into it is exact, so no intermediate
//!   truncation occurs. `NONO2Ratio.NOxRatioCV` (an uncertainty-mode column)
//!   and `dataSourceID` are unused and not modelled.
//! * **No division.** The processing pipeline is a single multiplication, so
//!   the MariaDB `int / int` rounding gotcha does not arise.
//! * **`emissionQuant` / `emissionRate` are `DOUBLE`.** The NOx inputs and the
//!   `NOMOVESOutputTemp1` products are `DOUBLE`, so the port's `f64` multiply
//!   matches MariaDB's `DOUBLE` arithmetic with no truncation on the result.
//!
//! # Scope and data plane (Task 50)
//!
//! [`calculate`](NOCalculator::calculate) ports each SQL script's "Processing"
//! section. Its [`NitrogenOxideInputs`] argument is the set of tables the
//! "Extract Data" section produces; a future Task 50 (`DataFrameStore`) wiring
//! populates it from the per-run filtered execution database and the upstream
//! calculator's `MOVESWorkerOutput` NOx rows.
//!
//! `MOVESRunID`, `iterationID` and `SCC` are pass-through columns the SQL
//! copies verbatim from the NOx row; following the `SO2Calculator` /
//! `PM10EmissionCalculator` precedent they are not modelled here — the Task 50
//! output wiring carries them.
//!
//! [`Calculator::execute`] receives a [`CalculatorContext`] whose
//! `ExecutionTables` / `ScratchNamespace` are Phase 2 placeholders, so it
//! cannot yet read the input tables nor emit `MOVESWorkerOutput`. The numeric
//! algorithm is fully ported and unit-tested on
//! [`calculate`](NOCalculator::calculate); `execute` is a documented shell
//! returning an empty [`CalculatorOutput`].

use std::collections::HashMap;

use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription, DataFrameStoreTyped,
    Error, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// Stable module name of the NO/HONO calculator — matches the Java class and
/// the `NOCalculator` entry in `calculator-dag.json`.
const NO_CALCULATOR_NAME: &str = "NOCalculator";

/// Stable module name of the NO2 calculator — matches the Java class and the
/// `NO2Calculator` entry in `calculator-dag.json`.
const NO2_CALCULATOR_NAME: &str = "NO2Calculator";

/// Oxides of Nitrogen (NOx) — `Pollutant` id 3, the total this family of
/// calculators speciates. The "Processing" section scales only the
/// `MOVESWorkerOutput` rows carrying this pollutant (`mwo.pollutantID = 3`).
const NOX_POLLUTANT_ID: i32 = 3;

/// Nitrogen Oxide (NO) — `Pollutant` id 32, a species `NOCalculator` produces.
const NO_POLLUTANT: PollutantId = PollutantId(32);
/// Nitrogen Dioxide (NO2) — `Pollutant` id 33, the species `NO2Calculator`
/// produces.
const NO2_POLLUTANT: PollutantId = PollutantId(33);
/// Nitrous Acid (HONO) — `Pollutant` id 34, a species `NOCalculator` produces.
const HONO_POLLUTANT: PollutantId = PollutantId(34);

// ===========================================================================
// Input / output rows — plain Rust mirrors of the tables each SQL script's
// "Extract Data" section pulls and the `MOVESWorkerOutput` rows it produces.
// Following the Phase 3 convention, every `INT`/`SMALLINT` identifier is an
// `i32` and every `FLOAT`/`DOUBLE` quantity is an `f64`.
// ===========================================================================

/// One `MOVESWorkerOutput` row — both an input (the upstream calculator's
/// total-NOx record) and an output (the speciated record this calculator
/// appends).
///
/// The two scripts read `MOVESWorkerOutput`, scale the NOx rows, and insert
/// the species rows back into the same table, so a single struct serves for
/// both directions: an input row carries `pollutant_id` 3 (Oxides of
/// Nitrogen), an output row the NO / NO2 / HONO species pollutant.
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
    /// `pollutantID` — Oxides of Nitrogen (3) on an input row, the NO / NO2 /
    /// HONO species pollutant on an output row.
    pub pollutant_id: i32,
    /// `processID` — the emission process.
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
    /// `MOVESWorkerOutput` physically unordered (the SQL `INSERT … SELECT` has
    /// no `ORDER BY`), so the port sorts purely for reproducibility.
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

/// One `NONO2Ratio` row — the NOx-to-species ratio for a `(polProcess,
/// sourceType, fuelType, modelYearGroup)` cell.
///
/// MOVES keys the table uniquely by `(polProcessID, sourceTypeID, fuelTypeID,
/// modelYearGroupID)`. The unused `NOxRatioCV` (uncertainty-mode) and
/// `dataSourceID` columns are not modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NoNo2RatioRow {
    /// `polProcessID` — `pollutantID × 100 + processID` of the species
    /// pollutant; joins to [`PollutantProcessAssocRow::pol_process_id`] and
    /// [`PollutantProcessModelYearRow::pol_process_id`].
    pub pol_process_id: i32,
    /// `sourceTypeID` — the source type the ratio applies to.
    pub source_type_id: i32,
    /// `fuelTypeID` — the fuel type the ratio applies to.
    pub fuel_type_id: i32,
    /// `modelYearGroupID` — the model-year group the ratio applies to;
    /// `PollutantProcessMappedModelYear` expands it into individual model years.
    pub model_year_group_id: i32,
    /// `NOxRatio` — the species ÷ NOx multiplier. `FLOAT` in MOVES (see the
    /// [module fidelity notes](self)).
    pub nox_ratio: f64,
}

/// One `PollutantProcessAssoc` row — a legal `(pollutant, process)` pairing
/// and the `polProcessID` that keys it.
///
/// `NOCalculator.sql` extracts the rows whose `pollutantID` is NO (32) and/or
/// HONO (34); `NO2Calculator.sql` the rows whose `pollutantID` is NO2 (33).
/// Both restrict `processID` to the iteration's process.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollutantProcessAssocRow {
    /// `polProcessID` — `pollutantID × 100 + processID`.
    pub pol_process_id: i32,
    /// `processID` — the emission process.
    pub process_id: i32,
    /// `pollutantID` — the species pollutant (NO, NO2 or HONO).
    pub pollutant_id: i32,
}

/// One `PollutantProcessMappedModelYear` row — maps a `(polProcess,
/// modelYear)` to the model-year group whose ratio covers it.
///
/// MOVES keys the table uniquely by `(polProcessID, modelYearID)`, so a
/// `(polProcessID, modelYearGroupID)` cell carries one row per model year the
/// group spans. The unused `fuelMYGroupID` column is not modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollutantProcessModelYearRow {
    /// `polProcessID` — `pollutantID × 100 + processID`.
    pub pol_process_id: i32,
    /// `modelYearID` — the vehicle model year.
    pub model_year_id: i32,
    /// `modelYearGroupID` — the model-year group `modelYearID` falls in; joins
    /// to [`NoNo2RatioRow::model_year_group_id`].
    pub model_year_group_id: i32,
}

/// Inputs to [`NOCalculator::calculate`] / [`NO2Calculator::calculate`] — the
/// tables each SQL script's "Extract Data" section produces, as plain row
/// vectors.
///
/// A future Task 50 (`DataFrameStore`) wiring populates this from the per-run
/// filtered execution database (`NONO2Ratio`, `PollutantProcessAssoc`,
/// `PollutantProcessMappedModelYear`, `SourceUseType`) and the upstream
/// calculator's `MOVESWorkerOutput`; until then it is the explicit data-plane
/// contract the unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct NitrogenOxideInputs {
    /// `NONO2Ratio` rows — the species-to-NOx ratio lookup table.
    pub no_no2_ratio: Vec<NoNo2RatioRow>,
    /// `PollutantProcessAssoc` rows — resolve each ratio's `polProcessID` into
    /// its process and species pollutant.
    pub pollutant_process_assoc: Vec<PollutantProcessAssocRow>,
    /// `PollutantProcessMappedModelYear` rows — expand each ratio's
    /// `modelYearGroupID` into the individual model years it covers.
    pub pollutant_process_model_year: Vec<PollutantProcessModelYearRow>,
    /// `SourceUseType` ids — the SQL's `NOCopyOfSourceUseType` join is an
    /// existence filter on the source type; a `NONO2Ratio` row whose
    /// `sourceTypeID` is absent here is dropped.
    pub source_use_type: Vec<i32>,
    /// `MOVESWorkerOutput` rows — the upstream calculator's records. The
    /// calculation reads only the total-NOx rows (`pollutantID` 3); any other
    /// pollutant present is ignored, as the SQL's `mwo.pollutantID = 3` filter
    /// does.
    pub worker_output: Vec<MovesWorkerOutputRow>,
}

// ===========================================================================
// Data-plane wiring — TableRow impls + build_inputs/write_rows helpers.
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

struct SourceUseTypeIdRow {
    source_type_id: i32,
}

impl TableRow for SourceUseTypeIdRow {
    fn table_name() -> &'static str {
        "SourceUseType"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([("sourceTypeID".into(), DataType::Int32)])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![Series::new(
                "sourceTypeID".into(),
                rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>(),
            )
            .into()],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SourceUseType";
        let col = "sourceTypeID";
        let ids = df
            .column(col)
            .map_err(|e| row_err(t, 0, col, e.to_string()))?
            .i32()
            .map_err(|e| row_err(t, 0, col, e.to_string()))?;
        (0..df.height())
            .map(|i| {
                Ok(SourceUseTypeIdRow {
                    source_type_id: ids
                        .get(i)
                        .ok_or_else(|| row_err(t, i, col, "null value".into()))?,
                })
            })
            .collect()
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

impl TableRow for NoNo2RatioRow {
    fn table_name() -> &'static str {
        "NONO2Ratio"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("modelYearGroupID".into(), DataType::Int32),
            ("NOxRatio".into(), DataType::Float64),
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
                    "modelYearGroupID".into(),
                    rows.iter()
                        .map(|r| r.model_year_group_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "NOxRatio".into(),
                    rows.iter().map(|r| r.nox_ratio).collect::<Vec<f64>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "NONO2Ratio";
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
        let pol_proc = get_i32("polProcessID")?;
        let src_type = get_i32("sourceTypeID")?;
        let fuel_type = get_i32("fuelTypeID")?;
        let my_group = get_i32("modelYearGroupID")?;
        let nox_ratio = get_f64("NOxRatio")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(NoNo2RatioRow {
                    pol_process_id: pol_proc.get(i).ok_or_else(|| null("polProcessID"))?,
                    source_type_id: src_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                    fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                    model_year_group_id: my_group.get(i).ok_or_else(|| null("modelYearGroupID"))?,
                    nox_ratio: nox_ratio.get(i).ok_or_else(|| null("NOxRatio"))?,
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
        let pol_proc = get_i32("polProcessID")?;
        let process = get_i32("processID")?;
        let pollutant = get_i32("pollutantID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(PollutantProcessAssocRow {
                    pol_process_id: pol_proc.get(i).ok_or_else(|| null("polProcessID"))?,
                    process_id: process.get(i).ok_or_else(|| null("processID"))?,
                    pollutant_id: pollutant.get(i).ok_or_else(|| null("pollutantID"))?,
                })
            })
            .collect()
    }
}

impl TableRow for PollutantProcessModelYearRow {
    fn table_name() -> &'static str {
        "PollutantProcessMappedModelYear"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("modelYearGroupID".into(), DataType::Int32),
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
                    "modelYearID".into(),
                    rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>(),
                )
                .into(),
                Series::new(
                    "modelYearGroupID".into(),
                    rows.iter()
                        .map(|r| r.model_year_group_id)
                        .collect::<Vec<i32>>(),
                )
                .into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "PollutantProcessMappedModelYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col)
                .map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32()
                .map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_proc = get_i32("polProcessID")?;
        let model_year = get_i32("modelYearID")?;
        let my_group = get_i32("modelYearGroupID")?;
        (0..df.height())
            .map(|i| {
                let null = |col: &'static str| row_err(t, i, col, "null value".into());
                Ok(PollutantProcessModelYearRow {
                    pol_process_id: pol_proc.get(i).ok_or_else(|| null("polProcessID"))?,
                    model_year_id: model_year.get(i).ok_or_else(|| null("modelYearID"))?,
                    model_year_group_id: my_group.get(i).ok_or_else(|| null("modelYearGroupID"))?,
                })
            })
            .collect()
    }
}

fn build_inputs(ctx: &CalculatorContext) -> Result<NitrogenOxideInputs, Error> {
    let tables = ctx.tables();
    let filter = crate::wiring::position_filter(ctx);
    Ok(NitrogenOxideInputs {
        no_no2_ratio: tables.iter_typed::<NoNo2RatioRow>("NONO2Ratio")?,
        pollutant_process_assoc: tables
            .iter_typed::<PollutantProcessAssocRow>("PollutantProcessAssoc")?,
        pollutant_process_model_year: tables
            .iter_typed::<PollutantProcessModelYearRow>("PollutantProcessMappedModelYear")?,
        source_use_type: tables
            .iter_typed::<SourceUseTypeIdRow>("SourceUseType")?
            .into_iter()
            .map(|r| r.source_type_id)
            .collect(),
        worker_output: {
            let rows = tables.iter_typed::<MovesWorkerOutputRow>("MOVESWorkerOutput")?;
            rows.into_iter()
                .filter(|r| filter.matches(r.year_id, r.county_id, r.process_id))
                .collect()
        },
    })
}

/// A `NOxRatio` resolved onto a `(process, pollutant)` — the port's fold of
/// one `NOCalculation1` / `NO2Calculation1` working-table row.
///
/// The row's `sourceTypeID`, `fuelTypeID` and `modelYearID` are the index key
/// the cell is stored under, so only the three downstream-consumed columns are
/// kept here.
#[derive(Debug, Clone, Copy)]
struct RatioCell {
    /// `processID` from the joined `PollutantProcessAssoc` row.
    process_id: i32,
    /// `pollutantID` from the joined `PollutantProcessAssoc` row — the NO /
    /// NO2 / HONO species.
    pollutant_id: i32,
    /// `NOxRatio` from the `NONO2Ratio` row.
    nox_ratio: f64,
}

/// The shared "Processing" section of `NOCalculator.sql` and
/// `NO2Calculator.sql` — `species = NOx × NOxRatio`.
///
/// Builds `NOCalculation1` (each `NONO2Ratio` ratio resolved through the
/// `PollutantProcessAssoc` and `SourceUseType` joins and expanded over the
/// model years its `modelYearGroupID` covers), then joins the total-NOx
/// `MOVESWorkerOutput` rows to it on `(fuelTypeID, modelYearID, sourceTypeID)`
/// and emits one species row per match with both emission values scaled. A
/// row that resolves no join match is dropped — every SQL join is an `INNER
/// JOIN`. The result is sorted by its integer dimension columns for
/// deterministic output; MOVES leaves `MOVESWorkerOutput` physically unordered.
fn compute_nitrogen_oxide(inputs: &NitrogenOxideInputs) -> Vec<MovesWorkerOutputRow> {
    // PollutantProcessAssoc indexed by polProcessID → (processID, pollutantID).
    // pollutantprocessassoc's primary key is polProcessID, so the mapping is a
    // function.
    let process_of_pol_process: HashMap<i32, (i32, i32)> = inputs
        .pollutant_process_assoc
        .iter()
        .map(|ppa| (ppa.pol_process_id, (ppa.process_id, ppa.pollutant_id)))
        .collect();

    // PollutantProcessMappedModelYear indexed by (polProcessID,
    // modelYearGroupID) → the model years that group spans. The table's
    // primary key is (polProcessID, modelYearID), so each model year appears
    // once per cell.
    let mut model_years_of_group: HashMap<(i32, i32), Vec<i32>> = HashMap::new();
    for ppmy in &inputs.pollutant_process_model_year {
        model_years_of_group
            .entry((ppmy.pol_process_id, ppmy.model_year_group_id))
            .or_default()
            .push(ppmy.model_year_id);
    }

    // --- NOCalculation1 -----------------------------------------------------
    // Add (process, pollutant, modelYear) dimensions to each NOxRatio, indexed
    // by the NOMOVESOutputTemp1 join key (fuelTypeID, modelYearID,
    // sourceTypeID). A cell can carry several rows — distinct nitrogen species
    // (NO and HONO for NOCalculator) sharing the dimension cell.
    let mut ratio_cells: HashMap<(i32, i32, i32), Vec<RatioCell>> = HashMap::new();
    for nnr in &inputs.no_no2_ratio {
        // INNER JOIN NOCopyOfPPA ON polProcessID — resolves the species
        // pollutant and process; drops a ratio with no association row.
        let Some(&(process_id, pollutant_id)) = process_of_pol_process.get(&nnr.pol_process_id)
        else {
            continue;
        };
        // INNER JOIN NOCopyOfSourceUseType ON sourceTypeID — an existence
        // filter on the source type.
        if !inputs.source_use_type.contains(&nnr.source_type_id) {
            continue;
        }
        // INNER JOIN NOCopyOfPPMY ON modelYearGroupID AND polProcessID —
        // expands the ratio's model-year group into the individual model years.
        let Some(model_years) =
            model_years_of_group.get(&(nnr.pol_process_id, nnr.model_year_group_id))
        else {
            continue;
        };
        for &model_year_id in model_years {
            ratio_cells
                .entry((nnr.fuel_type_id, model_year_id, nnr.source_type_id))
                .or_default()
                .push(RatioCell {
                    process_id,
                    pollutant_id,
                    nox_ratio: nnr.nox_ratio,
                });
        }
    }

    // --- NOMOVESOutputTemp1 -------------------------------------------------
    // emissionQuant = NOxRatio × Oxides of Nitrogen (3).
    let mut out: Vec<MovesWorkerOutputRow> = Vec::new();
    for mwo in &inputs.worker_output {
        // WHERE mwo.pollutantID = 3 — only the total-NOx rows are speciated.
        if mwo.pollutant_id != NOX_POLLUTANT_ID {
            continue;
        }
        // INNER JOIN NOCalculation1 ON fuelTypeID, modelYearID, sourceTypeID.
        let Some(cells) =
            ratio_cells.get(&(mwo.fuel_type_id, mwo.model_year_id, mwo.source_type_id))
        else {
            continue;
        };
        for cell in cells {
            // The species row: the pollutant is relabelled and the process
            // taken from NOCalculation1 (SELECT noc.pollutantID, noc.processID);
            // every other column — sourceTypeID included, the join pins it
            // equal — is carried from the NOx row, with both emission values
            // scaled by the ratio.
            out.push(MovesWorkerOutputRow {
                pollutant_id: cell.pollutant_id,
                process_id: cell.process_id,
                emission_quant: cell.nox_ratio * mwo.emission_quant,
                emission_rate: cell.nox_ratio * mwo.emission_rate,
                ..*mwo
            });
        }
    }

    out.sort_unstable_by_key(MovesWorkerOutputRow::dimension_key);
    out
}

/// Both nitrogen-oxide calculators are chained calculators —
/// `subscribes_directly: false` in `calculator-dag.json` — so neither declares
/// a MasterLoop subscription.
static NO_SUBSCRIPTIONS: &[CalculatorSubscription] = &[];

/// The upstream calculator both chain off — `BaseRateCalculator`, the producer
/// of total Oxides of Nitrogen (pollutant 3) in the pinned runtime.
/// `calculator-dag.json` records `depends_on: ["BaseRateCalculator"]` for both.
static UPSTREAM: &[&str] = &["BaseRateCalculator"];

/// Default-DB / scratch tables both scripts' processing pass consumes.
///
/// `MOVESWorkerOutput` carries the upstream calculator's NOx rows; `NONO2Ratio`
/// is the ratio table; `PollutantProcessAssoc` and
/// `PollutantProcessMappedModelYear` resolve and expand each ratio;
/// `SourceUseType` is the existence filter. The scripts also pull a
/// `FuelType` extract their "Processing" sections never join (see the [module
/// documentation](self)), so it is not listed.
static INPUT_TABLES: &[&str] = &[
    "MOVESWorkerOutput",
    "NONO2Ratio",
    "PollutantProcessAssoc",
    "PollutantProcessMappedModelYear",
    "SourceUseType",
];

// ===========================================================================
// The NO / HONO calculator.
// ===========================================================================

/// The eight `(pollutant, process)` pairs `NOCalculator` registers.
///
/// Nitrogen Oxide (pollutant 32) and Nitrous Acid / HONO (pollutant 34), each
/// for the running (1), start (2), extended-idle (90) and auxiliary-power (91)
/// exhaust processes — the eight `Registration` directives recorded for
/// `NOCalculator` in `CalculatorInfo.txt` (`registrations_count: 8` in
/// `calculator-dag.json`), matching the Java constructor's
/// `EmissionCalculatorRegistration.register` calls.
static NO_REGISTRATIONS: &[PollutantProcessAssociation] = &[
    PollutantProcessAssociation {
        pollutant_id: NO_POLLUTANT,
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: NO_POLLUTANT,
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: NO_POLLUTANT,
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: NO_POLLUTANT,
        process_id: ProcessId(91),
    },
    PollutantProcessAssociation {
        pollutant_id: HONO_POLLUTANT,
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: HONO_POLLUTANT,
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: HONO_POLLUTANT,
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: HONO_POLLUTANT,
        process_id: ProcessId(91),
    },
];

/// The MOVES nitrogen-oxide / nitrous-acid calculator — `NOCalculator`.
///
/// Speciates total Oxides of Nitrogen (pollutant 3) into Nitrogen Oxide
/// (pollutant 32) and Nitrous Acid / HONO (pollutant 34) for the running,
/// start, extended-idle and auxiliary-power exhaust processes. A zero-sized
/// value type owning no per-run state, as the [`Calculator`] trait requires;
/// all run-varying input flows through the [`NitrogenOxideInputs`] argument to
/// [`calculate`](Self::calculate).
#[derive(Debug, Clone, Copy, Default)]
pub struct NOCalculator;

impl NOCalculator {
    /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = NO_CALCULATOR_NAME;

    /// Construct the calculator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Compute the NO and HONO species rows — the port of the
    /// `NOCalculator.sql` "Processing" section.
    ///
    /// Each total-NOx `MOVESWorkerOutput` row (`pollutantID` 3) is scaled by
    /// its `NONO2Ratio` into the NO (32) and/or HONO (34) species rows the
    /// inputs carry ratios for; a row that resolves no ratio is dropped. The
    /// result is sorted by its integer dimension columns for deterministic
    /// output.
    ///
    /// This is the identical `compute_nitrogen_oxide` call as
    /// [`NO2Calculator::calculate`] — the two Java scripts share their
    /// "Processing" section and differ only in the extract that builds
    /// [`NitrogenOxideInputs`] (see the [module documentation](self)).
    #[must_use]
    pub fn calculate(&self, inputs: &NitrogenOxideInputs) -> Vec<MovesWorkerOutputRow> {
        compute_nitrogen_oxide(inputs)
    }
}

impl Calculator for NOCalculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// `NOCalculator` is a chained calculator: it does not subscribe to the
    /// MasterLoop directly but fires when its upstream `BaseRateCalculator`
    /// does. `calculator-dag.json` records `subscribes_directly: false` and an
    /// empty `subscriptions` list.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        NO_SUBSCRIPTIONS
    }

    fn registrations(&self) -> &[PollutantProcessAssociation] {
        NO_REGISTRATIONS
    }

    /// `NOCalculator` chains off `BaseRateCalculator` — `calculator-dag.json`
    /// records `depends_on: ["BaseRateCalculator"]`.
    fn upstream(&self) -> &[&'static str] {
        UPSTREAM
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
// The NO2 calculator.
// ===========================================================================

/// The four `(pollutant, process)` pairs `NO2Calculator` registers.
///
/// Nitrogen Dioxide (pollutant 33) for the running (1), start (2),
/// extended-idle (90) and auxiliary-power (91) exhaust processes — the four
/// `Registration` directives recorded for `NO2Calculator` in
/// `CalculatorInfo.txt` (`registrations_count: 4` in `calculator-dag.json`),
/// matching the Java constructor's `EmissionCalculatorRegistration.register`
/// calls.
static NO2_REGISTRATIONS: &[PollutantProcessAssociation] = &[
    PollutantProcessAssociation {
        pollutant_id: NO2_POLLUTANT,
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: NO2_POLLUTANT,
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: NO2_POLLUTANT,
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: NO2_POLLUTANT,
        process_id: ProcessId(91),
    },
];

/// The MOVES nitrogen-dioxide calculator — `NO2Calculator`.
///
/// Speciates total Oxides of Nitrogen (pollutant 3) into Nitrogen Dioxide
/// (pollutant 33) for the running, start, extended-idle and auxiliary-power
/// exhaust processes. A zero-sized value type owning no per-run state, as the
/// [`Calculator`] trait requires; all run-varying input flows through the
/// [`NitrogenOxideInputs`] argument to [`calculate`](Self::calculate).
#[derive(Debug, Clone, Copy, Default)]
pub struct NO2Calculator;

impl NO2Calculator {
    /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = NO2_CALCULATOR_NAME;

    /// Construct the calculator.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Compute the NO2 species rows — the port of the `NO2Calculator.sql`
    /// "Processing" section.
    ///
    /// Each total-NOx `MOVESWorkerOutput` row (`pollutantID` 3) is scaled by
    /// its `NONO2Ratio` into the NO2 (33) species row; a row that resolves no
    /// ratio is dropped. The result is sorted by its integer dimension columns
    /// for deterministic output.
    ///
    /// This is the identical `compute_nitrogen_oxide` call as
    /// [`NOCalculator::calculate`] — the two Java scripts share their
    /// "Processing" section and differ only in the extract that builds
    /// [`NitrogenOxideInputs`] (see the [module documentation](self)).
    #[must_use]
    pub fn calculate(&self, inputs: &NitrogenOxideInputs) -> Vec<MovesWorkerOutputRow> {
        compute_nitrogen_oxide(inputs)
    }
}

impl Calculator for NO2Calculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// `NO2Calculator` is a chained calculator: it does not subscribe to the
    /// MasterLoop directly but fires when its upstream `BaseRateCalculator`
    /// does. `calculator-dag.json` records `subscribes_directly: false` and an
    /// empty `subscriptions` list.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        NO_SUBSCRIPTIONS
    }

    fn registrations(&self) -> &[PollutantProcessAssociation] {
        NO2_REGISTRATIONS
    }

    /// `NO2Calculator` chains off `BaseRateCalculator` — `calculator-dag.json`
    /// records `depends_on: ["BaseRateCalculator"]`.
    fn upstream(&self) -> &[&'static str] {
        UPSTREAM
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a total-NOx `MOVESWorkerOutput` row with fixed dimension columns,
    /// `emissionQuant = 200.0` and `emissionRate = 8.0`. Values are chosen for
    /// exact scaled results, not physical realism.
    fn nox_row() -> MovesWorkerOutputRow {
        MovesWorkerOutputRow {
            year_id: 2020,
            month_id: 7,
            day_id: 5,
            hour_id: 8,
            state_id: 26,
            county_id: 26_161,
            zone_id: 261_610,
            link_id: 5001,
            pollutant_id: NOX_POLLUTANT_ID,
            process_id: 1,
            source_type_id: 21,
            reg_class_id: 30,
            fuel_type_id: 2,
            model_year_id: 2015,
            road_type_id: 4,
            emission_quant: 200.0,
            emission_rate: 8.0,
        }
    }

    /// Build a one-ratio / one-NOx-row NO2-style input whose single output row
    /// has `emissionQuant = 0.25 × 200.0 = 50.0` and `emissionRate =
    /// 0.25 × 8.0 = 2.0`.
    ///
    /// `polProcessID` 3301 is NO2 (33) on the running process (1).
    fn minimal_inputs() -> NitrogenOxideInputs {
        NitrogenOxideInputs {
            no_no2_ratio: vec![NoNo2RatioRow {
                pol_process_id: 3301,
                source_type_id: 21,
                fuel_type_id: 2,
                model_year_group_id: 42,
                nox_ratio: 0.25,
            }],
            pollutant_process_assoc: vec![PollutantProcessAssocRow {
                pol_process_id: 3301,
                process_id: 1,
                pollutant_id: 33,
            }],
            pollutant_process_model_year: vec![PollutantProcessModelYearRow {
                pol_process_id: 3301,
                model_year_id: 2015,
                model_year_group_id: 42,
            }],
            source_use_type: vec![21],
            worker_output: vec![nox_row()],
        }
    }

    /// Assert `actual` matches `expected` within `f64` slack — the
    /// FLOAT-column fidelity note means the port computes in `f64`.
    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "{actual} != expected {expected}",
        );
    }

    #[test]
    fn calculate_minimal_input_yields_one_row() {
        let rows = NO2Calculator::new().calculate(&minimal_inputs());
        assert_eq!(rows.len(), 1);
        let r = rows[0];
        // The dimension cell is carried straight from the NOx row.
        assert_eq!(r.year_id, 2020);
        assert_eq!(r.month_id, 7);
        assert_eq!(r.day_id, 5);
        assert_eq!(r.hour_id, 8);
        assert_eq!(r.state_id, 26);
        assert_eq!(r.county_id, 26_161);
        assert_eq!(r.zone_id, 261_610);
        assert_eq!(r.link_id, 5001);
        assert_eq!(r.source_type_id, 21);
        assert_eq!(r.reg_class_id, 30);
        assert_eq!(r.fuel_type_id, 2);
        assert_eq!(r.model_year_id, 2015);
        assert_eq!(r.road_type_id, 4);
        // The pollutant is relabelled to the NO2 species; the process carried.
        assert_eq!(r.pollutant_id, 33);
        assert_eq!(r.process_id, 1);
        // 0.25 × 200.0 and 0.25 × 8.0.
        assert_close(r.emission_quant, 50.0);
        assert_close(r.emission_rate, 2.0);
    }

    #[test]
    fn calculate_scales_both_emission_values_by_the_ratio() {
        // A ratio of 0.6 scales both the quantity and the rate.
        let mut inputs = minimal_inputs();
        inputs.no_no2_ratio[0].nox_ratio = 0.6;
        let rows = NO2Calculator::new().calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_close(rows[0].emission_quant, 120.0); // 0.6 × 200.0
        assert_close(rows[0].emission_rate, 4.8); // 0.6 × 8.0
    }

    #[test]
    fn calculate_emits_one_row_per_species_for_a_cell() {
        // NOCalculator-style input: the same (fuelType, modelYear, sourceType)
        // cell carries both an NO (32) and a HONO (34) ratio, so one NOx row
        // produces one row of each species — the SQL join cross-product.
        let mut inputs = minimal_inputs();
        inputs.no_no2_ratio = vec![
            NoNo2RatioRow {
                pol_process_id: 3201, // NO (32), running (1)
                source_type_id: 21,
                fuel_type_id: 2,
                model_year_group_id: 42,
                nox_ratio: 0.7,
            },
            NoNo2RatioRow {
                pol_process_id: 3401, // HONO (34), running (1)
                source_type_id: 21,
                fuel_type_id: 2,
                model_year_group_id: 42,
                nox_ratio: 0.02,
            },
        ];
        inputs.pollutant_process_assoc = vec![
            PollutantProcessAssocRow {
                pol_process_id: 3201,
                process_id: 1,
                pollutant_id: 32,
            },
            PollutantProcessAssocRow {
                pol_process_id: 3401,
                process_id: 1,
                pollutant_id: 34,
            },
        ];
        inputs.pollutant_process_model_year = vec![
            PollutantProcessModelYearRow {
                pol_process_id: 3201,
                model_year_id: 2015,
                model_year_group_id: 42,
            },
            PollutantProcessModelYearRow {
                pol_process_id: 3401,
                model_year_id: 2015,
                model_year_group_id: 42,
            },
        ];

        let rows = NOCalculator::new().calculate(&inputs);
        assert_eq!(rows.len(), 2);
        let no = rows.iter().find(|r| r.pollutant_id == 32).expect("NO row");
        let hono = rows
            .iter()
            .find(|r| r.pollutant_id == 34)
            .expect("HONO row");
        assert_close(no.emission_quant, 140.0); // 0.7 × 200.0
        assert_close(hono.emission_quant, 4.0); // 0.02 × 200.0
    }

    #[test]
    fn calculate_expands_ratio_over_the_model_year_group() {
        // One NONO2Ratio row's modelYearGroup spans two model years; a NOx row
        // for each yields a row, a NOx row for an unmapped model year does not.
        let mut inputs = minimal_inputs();
        inputs.pollutant_process_model_year = vec![
            PollutantProcessModelYearRow {
                pol_process_id: 3301,
                model_year_id: 2015,
                model_year_group_id: 42,
            },
            PollutantProcessModelYearRow {
                pol_process_id: 3301,
                model_year_id: 2016,
                model_year_group_id: 42,
            },
        ];
        inputs.worker_output = vec![
            MovesWorkerOutputRow {
                model_year_id: 2015,
                ..nox_row()
            },
            MovesWorkerOutputRow {
                model_year_id: 2016,
                ..nox_row()
            },
            MovesWorkerOutputRow {
                model_year_id: 2017, // not mapped into group 42
                ..nox_row()
            },
        ];

        let rows = NO2Calculator::new().calculate(&inputs);
        assert_eq!(
            rows.len(),
            2,
            "only the two in-group model years yield rows"
        );
        let mut years: Vec<i32> = rows.iter().map(|r| r.model_year_id).collect();
        years.sort_unstable();
        assert_eq!(years, vec![2015, 2016]);
    }

    #[test]
    fn calculate_ignores_non_nox_worker_rows() {
        // A worker-output row whose pollutant is not Oxides of Nitrogen (3) is
        // not speciated — mwo.pollutantID = 3 in the SQL.
        let mut inputs = minimal_inputs();
        inputs.worker_output[0].pollutant_id = 2; // CO, say — not NOx
        assert!(NO2Calculator::new().calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_nox_row_without_a_ratio_cell() {
        // A NOx row whose (fuelType, modelYear, sourceType) resolves no
        // NOCalculation1 cell is dropped by the inner join.
        let mut wrong_fuel = minimal_inputs();
        wrong_fuel.worker_output[0].fuel_type_id = 99;
        assert!(NO2Calculator::new().calculate(&wrong_fuel).is_empty());

        let mut wrong_year = minimal_inputs();
        wrong_year.worker_output[0].model_year_id = 1999;
        assert!(NO2Calculator::new().calculate(&wrong_year).is_empty());

        let mut wrong_source = minimal_inputs();
        wrong_source.worker_output[0].source_type_id = 62;
        assert!(NO2Calculator::new().calculate(&wrong_source).is_empty());
    }

    #[test]
    fn calculate_drops_ratio_without_a_source_use_type() {
        // The NONO2Ratio row's sourceTypeID is absent from the SourceUseType
        // existence filter — the NOCopyOfSourceUseType inner join drops it.
        let mut inputs = minimal_inputs();
        inputs.source_use_type.clear();
        assert!(NO2Calculator::new().calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_ratio_without_a_pollutant_process() {
        // NONO2Ratio carries a polProcessID with no PollutantProcessAssoc row —
        // the nnr ↔ ppa inner join drops it, leaving no NOCalculation1 cell.
        let mut inputs = minimal_inputs();
        inputs.pollutant_process_assoc.clear();
        assert!(NO2Calculator::new().calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_ratio_without_a_mapped_model_year() {
        // NONO2Ratio carries a modelYearGroupID with no
        // PollutantProcessMappedModelYear row — the nnr ↔ ppmy inner join drops
        // it, leaving no NOCalculation1 cell.
        let mut inputs = minimal_inputs();
        inputs.pollutant_process_model_year.clear();
        assert!(NO2Calculator::new().calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_output_is_sorted_by_dimension_key() {
        // Two NOx rows on distinct links produce two rows; the result comes
        // back dimension-key sorted regardless of input order.
        let mut inputs = minimal_inputs();
        inputs.worker_output.insert(
            0,
            MovesWorkerOutputRow {
                link_id: 9999, // sorts after link 5001
                ..nox_row()
            },
        );

        let rows = NO2Calculator::new().calculate(&inputs);
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
    fn calculate_empty_input_yields_no_rows() {
        assert!(NOCalculator::new()
            .calculate(&NitrogenOxideInputs::default())
            .is_empty());
        assert!(NO2Calculator::new()
            .calculate(&NitrogenOxideInputs::default())
            .is_empty());
    }

    #[test]
    fn both_calculators_share_the_processing_section() {
        // NOCalculator and NO2Calculator port the identical SQL "Processing"
        // section; on the same inputs they produce the same rows.
        let inputs = minimal_inputs();
        assert_eq!(
            NOCalculator::new().calculate(&inputs),
            NO2Calculator::new().calculate(&inputs),
        );
    }

    #[test]
    fn calculator_names_match_dag_modules() {
        assert_eq!(NOCalculator::new().name(), "NOCalculator");
        assert_eq!(NOCalculator::NAME, "NOCalculator");
        assert_eq!(NO2Calculator::new().name(), "NO2Calculator");
        assert_eq!(NO2Calculator::NAME, "NO2Calculator");
    }

    #[test]
    fn calculators_are_chained_with_no_subscriptions() {
        // calculator-dag.json: subscribes_directly false, subscriptions [].
        assert!(NOCalculator::new().subscriptions().is_empty());
        assert!(NO2Calculator::new().subscriptions().is_empty());
    }

    #[test]
    fn no_registrations_match_the_eight_calculator_info_directives() {
        // calculator-dag.json records registrations_count 8: NO (32) and
        // HONO (34), each for the running (1), start (2), extended-idle (90)
        // and aux-power (91) exhaust processes.
        let calc = NOCalculator::new();
        let regs = calc.registrations();
        assert_eq!(regs.len(), 8);
        for pollutant in [PollutantId(32), PollutantId(34)] {
            let mut procs: Vec<u16> = regs
                .iter()
                .filter(|r| r.pollutant_id == pollutant)
                .map(|r| r.process_id.0)
                .collect();
            procs.sort_unstable();
            assert_eq!(procs, vec![1, 2, 90, 91]);
        }
    }

    #[test]
    fn no2_registrations_match_the_four_calculator_info_directives() {
        // calculator-dag.json records registrations_count 4: NO2 (33) for the
        // running (1), start (2), extended-idle (90) and aux-power (91)
        // exhaust processes.
        let calc = NO2Calculator::new();
        let regs = calc.registrations();
        assert_eq!(regs.len(), 4);
        assert!(regs.iter().all(|r| r.pollutant_id == PollutantId(33)));
        let mut procs: Vec<u16> = regs.iter().map(|r| r.process_id.0).collect();
        procs.sort_unstable();
        assert_eq!(procs, vec![1, 2, 90, 91]);
    }

    #[test]
    fn calculators_chain_off_base_rate_calculator() {
        // calculator-dag.json records depends_on ["BaseRateCalculator"].
        assert_eq!(NOCalculator::new().upstream(), &["BaseRateCalculator"]);
        assert_eq!(NO2Calculator::new().upstream(), &["BaseRateCalculator"]);
    }

    #[test]
    fn calculators_declare_input_tables() {
        for tables in [
            NOCalculator::new().input_tables(),
            NO2Calculator::new().input_tables(),
        ] {
            for expected in [
                "MOVESWorkerOutput",
                "NONO2Ratio",
                "PollutantProcessAssoc",
                "PollutantProcessMappedModelYear",
                "SourceUseType",
            ] {
                assert!(tables.contains(&expected), "missing input table {expected}");
            }
        }
    }

    #[test]
    fn execute_wires_through_data_plane_no2() {
        use moves_framework::DataFrameStore;
        use polars::prelude::{DataFrame, NamedFrom, Series};
        let inputs = minimal_inputs();
        let mut store = moves_framework::InMemoryStore::new();
        store.insert(
            "NONO2Ratio",
            NoNo2RatioRow::into_dataframe(inputs.no_no2_ratio).unwrap(),
        );
        store.insert(
            "PollutantProcessAssoc",
            PollutantProcessAssocRow::into_dataframe(inputs.pollutant_process_assoc).unwrap(),
        );
        store.insert(
            "PollutantProcessMappedModelYear",
            PollutantProcessModelYearRow::into_dataframe(inputs.pollutant_process_model_year)
                .unwrap(),
        );
        store.insert(
            "SourceUseType",
            DataFrame::new(
                inputs.source_use_type.len(),
                vec![Series::new("sourceTypeID".into(), inputs.source_use_type).into()],
            )
            .unwrap(),
        );
        store.insert(
            "MOVESWorkerOutput",
            MovesWorkerOutputRow::into_dataframe(inputs.worker_output).unwrap(),
        );
        let ctx = CalculatorContext::with_tables(store);
        let out = NO2Calculator::new().execute(&ctx).expect("execute ok");
        let df = out.dataframe().expect("output should contain a DataFrame");
        assert_eq!(df.height(), 1, "minimal NO2 inputs produce exactly one row");
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
        // 0.25 × 200.0 = 50.0 and 0.25 × 8.0 = 2.0
        assert!((quant - 50.0).abs() < 1e-9, "emissionQuant {quant} != 50.0");
        assert!((rate - 2.0).abs() < 1e-9, "emissionRate {rate} != 2.0");
    }

    #[test]
    fn execute_wires_through_data_plane_no() {
        use moves_framework::DataFrameStore;
        use polars::prelude::{DataFrame, NamedFrom, Series};
        let no_inputs = NitrogenOxideInputs {
            no_no2_ratio: vec![NoNo2RatioRow {
                pol_process_id: 3201,
                source_type_id: 21,
                fuel_type_id: 2,
                model_year_group_id: 42,
                nox_ratio: 0.7,
            }],
            pollutant_process_assoc: vec![PollutantProcessAssocRow {
                pol_process_id: 3201,
                process_id: 1,
                pollutant_id: 32,
            }],
            pollutant_process_model_year: vec![PollutantProcessModelYearRow {
                pol_process_id: 3201,
                model_year_id: 2015,
                model_year_group_id: 42,
            }],
            source_use_type: vec![21],
            worker_output: vec![nox_row()],
        };
        let mut store = moves_framework::InMemoryStore::new();
        store.insert(
            "NONO2Ratio",
            NoNo2RatioRow::into_dataframe(no_inputs.no_no2_ratio).unwrap(),
        );
        store.insert(
            "PollutantProcessAssoc",
            PollutantProcessAssocRow::into_dataframe(no_inputs.pollutant_process_assoc).unwrap(),
        );
        store.insert(
            "PollutantProcessMappedModelYear",
            PollutantProcessModelYearRow::into_dataframe(no_inputs.pollutant_process_model_year)
                .unwrap(),
        );
        store.insert(
            "SourceUseType",
            DataFrame::new(
                no_inputs.source_use_type.len(),
                vec![Series::new("sourceTypeID".into(), no_inputs.source_use_type).into()],
            )
            .unwrap(),
        );
        store.insert(
            "MOVESWorkerOutput",
            MovesWorkerOutputRow::into_dataframe(no_inputs.worker_output).unwrap(),
        );
        let ctx = CalculatorContext::with_tables(store);
        let out = NOCalculator::new().execute(&ctx).expect("execute ok");
        let df = out.dataframe().expect("output should contain a DataFrame");
        assert_eq!(df.height(), 1, "NO inputs produce exactly one row");
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
        // 0.7 × 200.0 = 140.0 and 0.7 × 8.0 = 5.6
        assert!(
            (quant - 140.0).abs() < 1e-9,
            "emissionQuant {quant} != 140.0"
        );
        assert!((rate - 5.6).abs() < 1e-9, "emissionRate {rate} != 5.6");
    }

    #[test]
    fn calculators_are_object_safe() {
        // The registry stores calculators as Box<dyn Calculator>.
        let calcs: Vec<Box<dyn Calculator>> = vec![
            Box::new(NOCalculator::new()),
            Box::new(NO2Calculator::new()),
        ];
        assert_eq!(calcs[0].name(), "NOCalculator");
        assert_eq!(calcs[1].name(), "NO2Calculator");
    }
}
