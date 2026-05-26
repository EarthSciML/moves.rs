//! Port of `SO2Calculator.java` and `database/SO2Calculator.sql` —
//! migration plan Phase 3, Task 67.
//!
//! `SO2Calculator` computes gaseous **sulfur dioxide (SO2)** emissions for the
//! running, start, extended-idle and auxiliary-power exhaust processes. SO2 is
//! a direct function of the sulfur burned: the sulfur in the fuel oxidises, so
//! SO2 scales with the fuel's sulfur content and the energy the engine
//! consumes.
//!
//! # Chained calculator
//!
//! `SO2Calculator` is a *chained* calculator. `SO2Calculator.subscribeToMe`
//! does **not** subscribe to the MasterLoop; instead it chains itself onto the
//! calculators that produce Total Energy Consumption (pollutant 91) — in the
//! rates-first engine that is `BaseRateCalculator` — and runs inside the same
//! master-loop pass. `calculator-dag.json` records this as
//! `subscribes_directly: false`, `subscriptions: []`,
//! `depends_on: ["BaseRateCalculator"]`; the [`Calculator`] metadata methods
//! mirror it ([`subscriptions`](Calculator::subscriptions) is empty,
//! [`upstream`](Calculator::upstream) names `BaseRateCalculator`).
//!
//! # What it computes
//!
//! For each Total Energy Consumption record the upstream calculator emitted:
//!
//! ```text
//! SO2 = meanBaseRate × WsulfurLevel × energy ÷ energyContent
//! ```
//!
//! then, where a general fuel-effect ratio applies for the cell,
//!
//! ```text
//! SO2 = SO2 × fuelEffectRatio
//! ```
//!
//! * `energy` — the Total Energy Consumption value (`MOVESWorkerOutput`,
//!   pollutant 91) for the dimension cell.
//! * `WsulfurLevel` — the market-share-weighted mean fuel sulfur level of the
//!   cell's `(year, monthGroup, fuelType)` fuel supply.
//! * `energyContent` — the market-share-weighted mean energy content of that
//!   same fuel supply.
//! * `meanBaseRate` — the sulfate base emission rate for the cell's
//!   `(fuelType, modelYear)`, from `SulfateEmissionRate`.
//! * `fuelEffectRatio` — an optional `generalFuelRatio` multiplier.
//!
//! # Algorithm — the SQL "Processing" section
//!
//! [`SO2Calculator::calculate`] ports `SO2Calculator.sql`'s "Processing"
//! section. The SQL builds three working tables; the port folds them into two
//! index maps and one join loop:
//!
//! | SQL working table | This port |
//! |-------------------|-----------|
//! | `SO2FuelCalculation1` | `(yearID, monthGroupID, fuelTypeID) → (energyContent, WsulfurLevel)` |
//! | `SO2FuelCalculation2` | `(fuelTypeID, modelYearID) → [(processID, pollutantID, meanBaseRate)]` |
//! | `SO2MOVESOutputTemp1` | the per-energy-row join loop and the returned `Vec<`[`So2EmissionRow`]`>` |
//!
//! `SO2FuelCalculation1` is a market-share-weighted aggregation over the fuel
//! supply joined to fuel formulation → subtype → type and to the run year.
//! `SO2FuelCalculation2` expands each `SulfateEmissionRate` row's
//! `modelYearGroupID` (encoded `minModelYear × 10000 + maxModelYear`) into the
//! individual run-spec model years it covers. `SO2MOVESOutputTemp1` joins the
//! energy rows to both working tables and applies the formula; the
//! `generalFuelRatio` `UPDATE` is the optional multiplier.
//!
//! Every SQL join is an `INNER JOIN`, so a row with no match on the join key
//! is dropped; the port reproduces that with map lookups that skip on a miss.
//! The `generalFuelRatio` step is a multi-table `UPDATE`, not a join — a row
//! with no matching ratio keeps its value unchanged (effective ratio 1).
//!
//! # Scope of this port
//!
//! [`calculate`](SO2Calculator::calculate) is the SQL "Processing" section.
//! Its [`So2Inputs`] argument is the set of tables the SQL's "Extract Data"
//! section produces, as plain row vectors; a future Task 50 (`DataFrameStore`)
//! wiring populates it from the per-run filtered execution database.
//!
//! Two things the SQL does are *not* the algorithm and are left to that
//! wiring:
//!
//! * The Java `doExecute` checks the RunSpec actually requests Total Energy
//!   Consumption before generating any SQL — execution gating, reproduced by
//!   `calculate` returning no rows when the inputs carry no usable energy.
//! * `MOVESRunID`, `iterationID` and `SCC` are pure pass-through columns the
//!   SQL copies verbatim from the energy row into the SO2 row. They are not
//!   modelled here — the Task 50 output wiring carries them, matching
//!   `DistanceCalculator`'s treatment of `SCC`.
//!
//! The SQL extracts `Year` filtered to `yearID = ##context.year##` and the
//! fuel supply filtered to `fuelRegionID = ##context.fuelRegionID##`, and
//! stamps `SO2FuelCalculation1.countyID` with the literal context county. A
//! master-loop invocation is therefore single-county and single-year; the
//! port keys `SO2FuelCalculation1` by `(yearID, monthGroupID, fuelTypeID)` and
//! carries `countyID` straight from the energy row, exactly as the SQL's
//! `mwo.countyID = fc1.countyID` join — trivially satisfied — implies.
//! Likewise the extracted `PollutantProcessAssoc` is for the iteration's one
//! process; `calculate` joins the energy rows to `SO2FuelCalculation2` by
//! process, so any energy row for another process present in the input is
//! dropped, matching the SQL's `mwo.processID = ##context.iterProcess…##`
//! filter.
//!
//! # Fidelity notes
//!
//! `SO2Calculator.sql` writes `energyContent` and `WsulfurLevel` to `FLOAT`
//! (32-bit) `SO2FuelCalculation1` columns and `meanBaseRate` to a `FLOAT`
//! `SO2FuelCalculation2` column (`SulfateEmissionRate.meanBaseRate` is itself
//! `FLOAT`), while MariaDB evaluates the arithmetic in `DOUBLE`. This port
//! sums and multiplies in `f64` end to end, so it does not reproduce the `f32`
//! truncation MOVES applies when it stores those intermediates — a
//! sub-`1e-7` relative drift. Reproducing it bug-for-bug is the calculator
//! integration validation call (`mo-fvuf`), matching the `DistanceCalculator`
//! / Task 41 / Task 33 precedent. `marketShare`, `sulfurLevel` and
//! `energyContent` are likewise `FLOAT` columns, but they are model *inputs* —
//! already `f32`-quantised before [`calculate`](SO2Calculator::calculate) sees
//! them. `MOVESWorkerOutput.emissionQuant` / `.emissionRate` (the `energy`
//! inputs) are `DOUBLE`.
//!
//! The `modelYearGroupID` decomposition `floor(modelYearGroupID / 10000)` and
//! `mod(modelYearGroupID, 10000)` are exact integer operations — the `floor`
//! and `mod` wrap the division, so the MariaDB `div_precision_increment`
//! rounding gotcha does not arise; the port uses `i32` `/` and `%`.
//!
//! `energyContent` divides the formula. It is a market-share-weighted sum of
//! physically positive fuel energy contents and is never zero in real data; a
//! zero would yield a non-finite value where MariaDB's `x / 0` yields `NULL` —
//! a divergence reachable only on degenerate input.
//!
//! # Execute wiring (pilot — bucket-A canonical shape)
//!
//! [`SO2Calculator::execute`] is the bucket-A wiring pilot. [`build_inputs`]
//! reads each input-table row vector from `ctx.tables()` via
//! [`DataFrameStoreTyped::iter_typed`], applying position filters that mirror
//! the SQL `##context.X##` macros when `ctx.position()` carries a concrete
//! process, county, or year. [`write_rows`] converts the resulting
//! `Vec<So2EmissionRow>` to a `DataFrame` and wraps it in a
//! [`CalculatorOutput`]. Every other bucket-A calculator mirrors this shape
//! exactly.

use std::collections::HashMap;

use moves_data::{PollutantId, PollutantProcessAssociation, ProcessId};
use moves_framework::{
    Calculator, CalculatorContext, CalculatorOutput, CalculatorSubscription,
    DataFrameStoreTyped, Error, IntoDataFrame, TableRow,
};
use polars::prelude::{DataFrame, DataType, NamedFrom, PolarsResult, Schema, Series};

/// Stable module name — matches the Java class and the `SO2Calculator` entry
/// in the calculator-chain DAG (`calculator-dag.json`).
const CALCULATOR_NAME: &str = "SO2Calculator";

/// Sulfur Dioxide (SO2) — `Pollutant` id 31, the pollutant this calculator
/// produces. The SO2 pollutant of each output row flows in from the
/// `PollutantProcessAssoc` extract; this constant is the metadata side, the
/// pollutant of every [`REGISTRATIONS`] pair.
const SO2_POLLUTANT: PollutantId = PollutantId(31);

/// Total Energy Consumption — `Pollutant` id 91. The energy rows the formula
/// consumes are the `MOVESWorkerOutput` records for this pollutant.
const TOTAL_ENERGY_POLLUTANT_ID: i32 = 91;

// ===========================================================================
// Input tables — plain Rust mirrors of the tables `SO2Calculator.sql`'s
// "Extract Data" section pulls. Following the Phase 3 convention, every
// `INT`/`SMALLINT` identifier is an `i32` and every `FLOAT`/`DOUBLE` quantity
// is an `f64`. Only the columns the SO2 algorithm reads are modelled.
// ===========================================================================

/// One `FuelSupply` row — a fuel formulation's market share in the run's fuel
/// region for a `(fuelYear, monthGroup)`.
///
/// The SQL extracts `FuelSupply` filtered to the run's single fuel region, so
/// `fuelRegionID` is constant and is not modelled.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelSupplyRow {
    /// `fuelYearID` — joins to [`YearRow::fuel_year_id`].
    pub fuel_year_id: i32,
    /// `monthGroupID` — the month group this share applies to.
    pub month_group_id: i32,
    /// `fuelFormulationID` — joins to [`FuelFormulationRow::fuel_formulation_id`].
    pub fuel_formulation_id: i32,
    /// `marketShare` — this formulation's share of the fuel supply.
    pub market_share: f64,
}

/// One `FuelFormulation` row — a fuel blend's subtype and sulfur level.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelFormulationRow {
    /// `fuelFormulationID` — the formulation primary key.
    pub fuel_formulation_id: i32,
    /// `fuelSubTypeID` — joins to [`FuelSubTypeRow::fuel_sub_type_id`].
    pub fuel_sub_type_id: i32,
    /// `sulfurLevel` — fuel sulfur content. `FLOAT` in MOVES.
    pub sulfur_level: f64,
}

/// One `FuelSubType` row — a fuel subtype's parent fuel type and energy
/// content.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FuelSubTypeRow {
    /// `fuelSubTypeID` — the subtype primary key.
    pub fuel_sub_type_id: i32,
    /// `fuelTypeID` — the parent fuel type.
    pub fuel_type_id: i32,
    /// `energyContent` — energy per unit fuel. `FLOAT` in MOVES.
    pub energy_content: f64,
}

/// One `Year` row — resolves a `fuelYearID` into its calendar `yearID`.
///
/// The SQL extracts `Year` filtered to `yearID = ##context.year##`, so the
/// run carries a single row here.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct YearRow {
    /// `yearID` — the calendar year.
    pub year_id: i32,
    /// `fuelYearID` — the fuel year, joins to [`FuelSupplyRow::fuel_year_id`].
    pub fuel_year_id: i32,
}

/// One `SulfateEmissionRate` row — the sulfate base emission rate for a
/// `(polProcess, fuelType, modelYearGroup)`.
///
/// The SQL extracts the rows with `polProcessID IN (3101, 3102, 3190, 3191)`
/// — SO2 for the running, start, extended-idle and auxiliary-power processes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SulfateEmissionRateRow {
    /// `polProcessID` — `pollutantID × 100 + processID`; joins to
    /// [`PollutantProcessRow::pol_process_id`].
    pub pol_process_id: i32,
    /// `fuelTypeID` — the fuel type the rate applies to.
    pub fuel_type_id: i32,
    /// `modelYearGroupID` — encodes the model-year range as
    /// `minModelYearID × 10000 + maxModelYearID`.
    pub model_year_group_id: i32,
    /// `meanBaseRate` — the base rate. `FLOAT` in MOVES.
    pub mean_base_rate: f64,
}

/// One `PollutantProcessAssoc` row — a legal `(pollutant, process)` pairing.
///
/// The SQL extracts the rows with `pollutantID = 31` and
/// `processID = ##context.iterProcess.databaseKey##`, so every row here is
/// SO2 for the iteration's single process.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PollutantProcessRow {
    /// `polProcessID` — `pollutantID × 100 + processID`.
    pub pol_process_id: i32,
    /// `processID` — the emission process.
    pub process_id: i32,
    /// `pollutantID` — the pollutant (always 31, SO2, in this extract).
    pub pollutant_id: i32,
}

/// One `MonthOfAnyYear` row — the `monthID → monthGroupID` mapping.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MonthGroupRow {
    /// `monthID` — the calendar month.
    pub month_id: i32,
    /// `monthGroupID` — the month group it belongs to.
    pub month_group_id: i32,
}

/// One `generalFuelRatio` effect row — the market-share-weighted general
/// fuel-effect multiplier the SQL's "Extract Data" section pre-aggregates into
/// `so2PMOneCountyYearGeneralFuelRatio`.
///
/// The seven id columns are the join key the SQL's `UPDATE` matches on; the
/// extracted table is keyed by them, so each `(fuelType, sourceType, month,
/// pollutant, process, modelYear, year)` cell carries at most one ratio.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeneralFuelRatioRow {
    /// `fuelTypeID`.
    pub fuel_type_id: i32,
    /// `sourceTypeID`.
    pub source_type_id: i32,
    /// `monthID`.
    pub month_id: i32,
    /// `pollutantID`.
    pub pollutant_id: i32,
    /// `processID`.
    pub process_id: i32,
    /// `modelYearID`.
    pub model_year_id: i32,
    /// `yearID`.
    pub year_id: i32,
    /// `fuelEffectRatio` — the multiplier applied to the SO2 emission.
    pub fuel_effect_ratio: f64,
}

/// One Total Energy Consumption record — the subset of a `MOVESWorkerOutput`
/// row the SO2 algorithm reads.
///
/// The SQL selects `MOVESWorkerOutput` rows with `pollutantID = 91` for the
/// iteration's process; `energy` and `energy_rate` carry that row's
/// `emissionQuant` / `emissionRate` (the energy quantity and rate), and the
/// remaining fields are the dimension cell the SO2 row inherits.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EnergyRow {
    /// `pollutantID` — the energy algorithm only reads rows where this is
    /// `TOTAL_ENERGY_POLLUTANT_ID` (91).
    pub pollutant_id: i32,
    /// `processID` — the emission process; joins to the calculator's
    /// `SO2FuelCalculation2` process.
    pub process_id: i32,
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
    /// `emissionQuant` — the Total Energy Consumption quantity.
    pub energy: f64,
    /// `emissionRate` — the Total Energy Consumption rate.
    pub energy_rate: f64,
}

/// Inputs to [`SO2Calculator::calculate`] — the tables the SQL's "Extract
/// Data" section produces, as plain row vectors.
///
/// A future Task 50 (`DataFrameStore`) wiring populates this from the per-run
/// filtered execution database; until then it is the explicit data-plane
/// contract the unit tests build directly.
#[derive(Debug, Clone, Default)]
pub struct So2Inputs {
    /// `FuelSupply` rows (single fuel region).
    pub fuel_supply: Vec<FuelSupplyRow>,
    /// `FuelFormulation` rows.
    pub fuel_formulation: Vec<FuelFormulationRow>,
    /// `FuelSubType` rows.
    pub fuel_sub_type: Vec<FuelSubTypeRow>,
    /// `FuelType` ids — the SQL's `SO2CopyOfFuelType` join is an existence
    /// filter on the fuel type; a fuel subtype whose `fuelTypeID` is absent
    /// here is dropped.
    pub fuel_type: Vec<i32>,
    /// `Year` rows (single calendar year).
    pub year: Vec<YearRow>,
    /// `SulfateEmissionRate` rows for the SO2 processes.
    pub sulfate_emission_rate: Vec<SulfateEmissionRateRow>,
    /// `PollutantProcessAssoc` rows — SO2 for the iteration's process.
    pub pollutant_process_assoc: Vec<PollutantProcessRow>,
    /// `RunSpecModelYear` — the model years the run covers; the
    /// `SulfateEmissionRate` model-year groups are expanded onto these.
    pub run_spec_model_year: Vec<i32>,
    /// `MonthOfAnyYear` rows — the `monthID → monthGroupID` mapping.
    pub month_of_any_year: Vec<MonthGroupRow>,
    /// `generalFuelRatio` effect rows. May be empty: a cell with no matching
    /// ratio keeps its emission unchanged.
    pub general_fuel_ratio: Vec<GeneralFuelRatioRow>,
    /// `MOVESWorkerOutput` rows. The calculation reads only the Total Energy
    /// Consumption rows (`pollutantID` 91); any other pollutant present is
    /// ignored, as the SQL's `mwo.pollutantID = 91` filter does.
    pub energy: Vec<EnergyRow>,
}

/// One SO2 emission record produced by the calculation — the algorithm-bearing
/// subset of the `MOVESWorkerOutput` row the SQL inserts.
///
/// `MOVESRunID`, `iterationID` and `SCC` are pure pass-through columns the SQL
/// copies from the energy row; they are not modelled (see the [module
/// documentation](self)). `pollutant_id` is always 31 (SO2).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct So2EmissionRow {
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
    /// `pollutantID` — always 31 (SO2).
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
    /// `emissionQuant` — the SO2 emission quantity.
    pub emission_quant: f64,
    /// `emissionRate` — the SO2 emission rate.
    pub emission_rate: f64,
}

impl So2EmissionRow {
    /// The integer dimension tuple — every column except the two emission
    /// values. Used to sort the output deterministically: MOVES leaves
    /// `MOVESWorkerOutput` physically unordered (the SQL `INSERT … SELECT` has
    /// no `ORDER BY`), so the port sorts purely to make the result
    /// reproducible.
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

// ===========================================================================
// DataFrame ↔ row-struct conversions — `TableRow` implementations for every
// type `build_inputs` reads and for `So2EmissionRow` that `write_rows` emits.
// ===========================================================================

/// Helper: build an `Error::RowExtraction` for a missing / wrong-type column.
fn row_err(table: &'static str, row: usize, column: &'static str, msg: String) -> Error {
    Error::RowExtraction {
        table: table.into(),
        row,
        column: column.into(),
        message: msg,
    }
}

/// Minimal `FuelType` row — only `fuelTypeID` is needed (existence filter).
struct FuelTypeIdRow {
    fuel_type_id: i32,
}

/// Minimal `RunSpecModelYear` row — only `modelYearID` is needed.
struct RunSpecModelYearIdRow {
    model_year_id: i32,
}

impl TableRow for FuelSupplyRow {
    fn table_name() -> &'static str {
        "FuelSupply"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelYearID".into(), DataType::Int32),
            ("monthGroupID".into(), DataType::Int32),
            ("fuelFormulationID".into(), DataType::Int32),
            ("marketShare".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("fuelYearID".into(), rows.iter().map(|r| r.fuel_year_id).collect::<Vec<i32>>()).into(),
                Series::new("monthGroupID".into(), rows.iter().map(|r| r.month_group_id).collect::<Vec<i32>>()).into(),
                Series::new("fuelFormulationID".into(), rows.iter().map(|r| r.fuel_formulation_id).collect::<Vec<i32>>()).into(),
                Series::new("marketShare".into(), rows.iter().map(|r| r.market_share).collect::<Vec<f64>>()).into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelSupply";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let fuel_year = get_i32("fuelYearID")?;
        let month_group = get_i32("monthGroupID")?;
        let formulation = get_i32("fuelFormulationID")?;
        let market_share = get_f64("marketShare")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(FuelSupplyRow {
                fuel_year_id: fuel_year.get(i).ok_or_else(|| null("fuelYearID"))?,
                month_group_id: month_group.get(i).ok_or_else(|| null("monthGroupID"))?,
                fuel_formulation_id: formulation.get(i).ok_or_else(|| null("fuelFormulationID"))?,
                market_share: market_share.get(i).ok_or_else(|| null("marketShare"))?,
            })
        }).collect()
    }
}

impl TableRow for FuelFormulationRow {
    fn table_name() -> &'static str {
        "FuelFormulation"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelFormulationID".into(), DataType::Int32),
            ("fuelSubTypeID".into(), DataType::Int32),
            ("sulfurLevel".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("fuelFormulationID".into(), rows.iter().map(|r| r.fuel_formulation_id).collect::<Vec<i32>>()).into(),
                Series::new("fuelSubTypeID".into(), rows.iter().map(|r| r.fuel_sub_type_id).collect::<Vec<i32>>()).into(),
                Series::new("sulfurLevel".into(), rows.iter().map(|r| r.sulfur_level).collect::<Vec<f64>>()).into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelFormulation";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let form_id = get_i32("fuelFormulationID")?;
        let sub_type = get_i32("fuelSubTypeID")?;
        let sulfur = get_f64("sulfurLevel")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(FuelFormulationRow {
                fuel_formulation_id: form_id.get(i).ok_or_else(|| null("fuelFormulationID"))?,
                fuel_sub_type_id: sub_type.get(i).ok_or_else(|| null("fuelSubTypeID"))?,
                sulfur_level: sulfur.get(i).ok_or_else(|| null("sulfurLevel"))?,
            })
        }).collect()
    }
}

impl TableRow for FuelSubTypeRow {
    fn table_name() -> &'static str {
        "FuelSubType"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelSubTypeID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("energyContent".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("fuelSubTypeID".into(), rows.iter().map(|r| r.fuel_sub_type_id).collect::<Vec<i32>>()).into(),
                Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
                Series::new("energyContent".into(), rows.iter().map(|r| r.energy_content).collect::<Vec<f64>>()).into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelSubType";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let sub_type = get_i32("fuelSubTypeID")?;
        let fuel_type = get_i32("fuelTypeID")?;
        let energy = get_f64("energyContent")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(FuelSubTypeRow {
                fuel_sub_type_id: sub_type.get(i).ok_or_else(|| null("fuelSubTypeID"))?,
                fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                energy_content: energy.get(i).ok_or_else(|| null("energyContent"))?,
            })
        }).collect()
    }
}

impl TableRow for FuelTypeIdRow {
    fn table_name() -> &'static str {
        "FuelType"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([("fuelTypeID".into(), DataType::Int32)])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "FuelType";
        let col = "fuelTypeID";
        let ids = df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?
            .i32().map_err(|e| row_err(t, 0, col, e.to_string()))?;
        (0..df.height()).map(|i| {
            Ok(FuelTypeIdRow {
                fuel_type_id: ids.get(i).ok_or_else(|| row_err(t, i, col, "null value".into()))?,
            })
        }).collect()
    }
}

impl TableRow for YearRow {
    fn table_name() -> &'static str {
        "Year"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("yearID".into(), DataType::Int32),
            ("fuelYearID".into(), DataType::Int32),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("yearID".into(), rows.iter().map(|r| r.year_id).collect::<Vec<i32>>()).into(),
                Series::new("fuelYearID".into(), rows.iter().map(|r| r.fuel_year_id).collect::<Vec<i32>>()).into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "Year";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let year = get_i32("yearID")?;
        let fuel_year = get_i32("fuelYearID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(YearRow {
                year_id: year.get(i).ok_or_else(|| null("yearID"))?,
                fuel_year_id: fuel_year.get(i).ok_or_else(|| null("fuelYearID"))?,
            })
        }).collect()
    }
}

impl TableRow for SulfateEmissionRateRow {
    fn table_name() -> &'static str {
        "SulfateEmissionRate"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("polProcessID".into(), DataType::Int32),
            ("fuelTypeID".into(), DataType::Int32),
            ("modelYearGroupID".into(), DataType::Int32),
            ("meanBaseRate".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
                Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
                Series::new("modelYearGroupID".into(), rows.iter().map(|r| r.model_year_group_id).collect::<Vec<i32>>()).into(),
                Series::new("meanBaseRate".into(), rows.iter().map(|r| r.mean_base_rate).collect::<Vec<f64>>()).into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "SulfateEmissionRate";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_proc = get_i32("polProcessID")?;
        let fuel_type = get_i32("fuelTypeID")?;
        let my_group = get_i32("modelYearGroupID")?;
        let base_rate = get_f64("meanBaseRate")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(SulfateEmissionRateRow {
                pol_process_id: pol_proc.get(i).ok_or_else(|| null("polProcessID"))?,
                fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                model_year_group_id: my_group.get(i).ok_or_else(|| null("modelYearGroupID"))?,
                mean_base_rate: base_rate.get(i).ok_or_else(|| null("meanBaseRate"))?,
            })
        }).collect()
    }
}

impl TableRow for PollutantProcessRow {
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
                Series::new("polProcessID".into(), rows.iter().map(|r| r.pol_process_id).collect::<Vec<i32>>()).into(),
                Series::new("processID".into(), rows.iter().map(|r| r.process_id).collect::<Vec<i32>>()).into(),
                Series::new("pollutantID".into(), rows.iter().map(|r| r.pollutant_id).collect::<Vec<i32>>()).into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "PollutantProcessAssoc";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pol_proc = get_i32("polProcessID")?;
        let proc = get_i32("processID")?;
        let pollutant = get_i32("pollutantID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(PollutantProcessRow {
                pol_process_id: pol_proc.get(i).ok_or_else(|| null("polProcessID"))?,
                process_id: proc.get(i).ok_or_else(|| null("processID"))?,
                pollutant_id: pollutant.get(i).ok_or_else(|| null("pollutantID"))?,
            })
        }).collect()
    }
}

impl TableRow for RunSpecModelYearIdRow {
    fn table_name() -> &'static str {
        "RunSpecModelYear"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([("modelYearID".into(), DataType::Int32)])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("modelYearID".into(), rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>()).into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "RunSpecModelYear";
        let col = "modelYearID";
        let ids = df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?
            .i32().map_err(|e| row_err(t, 0, col, e.to_string()))?;
        (0..df.height()).map(|i| {
            Ok(RunSpecModelYearIdRow {
                model_year_id: ids.get(i).ok_or_else(|| row_err(t, i, col, "null value".into()))?,
            })
        }).collect()
    }
}

impl TableRow for MonthGroupRow {
    fn table_name() -> &'static str {
        "MonthOfAnyYear"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("monthID".into(), DataType::Int32),
            ("monthGroupID".into(), DataType::Int32),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into(),
                Series::new("monthGroupID".into(), rows.iter().map(|r| r.month_group_id).collect::<Vec<i32>>()).into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "MonthOfAnyYear";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let month = get_i32("monthID")?;
        let group = get_i32("monthGroupID")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(MonthGroupRow {
                month_id: month.get(i).ok_or_else(|| null("monthID"))?,
                month_group_id: group.get(i).ok_or_else(|| null("monthGroupID"))?,
            })
        }).collect()
    }
}

impl TableRow for GeneralFuelRatioRow {
    fn table_name() -> &'static str {
        "GeneralFuelRatio"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("fuelTypeID".into(), DataType::Int32),
            ("sourceTypeID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("pollutantID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
            ("modelYearID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("fuelEffectRatio".into(), DataType::Float64),
        ])
    }

    fn into_dataframe(rows: Vec<Self>) -> PolarsResult<DataFrame> {
        let n = rows.len();
        DataFrame::new(
            n,
            vec![
                Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
                Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
                Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into(),
                Series::new("pollutantID".into(), rows.iter().map(|r| r.pollutant_id).collect::<Vec<i32>>()).into(),
                Series::new("processID".into(), rows.iter().map(|r| r.process_id).collect::<Vec<i32>>()).into(),
                Series::new("modelYearID".into(), rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>()).into(),
                Series::new("yearID".into(), rows.iter().map(|r| r.year_id).collect::<Vec<i32>>()).into(),
                Series::new("fuelEffectRatio".into(), rows.iter().map(|r| r.fuel_effect_ratio).collect::<Vec<f64>>()).into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "GeneralFuelRatio";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let fuel_type = get_i32("fuelTypeID")?;
        let src_type = get_i32("sourceTypeID")?;
        let month = get_i32("monthID")?;
        let pollutant = get_i32("pollutantID")?;
        let process = get_i32("processID")?;
        let model_year = get_i32("modelYearID")?;
        let year = get_i32("yearID")?;
        let ratio = get_f64("fuelEffectRatio")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(GeneralFuelRatioRow {
                fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                source_type_id: src_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                month_id: month.get(i).ok_or_else(|| null("monthID"))?,
                pollutant_id: pollutant.get(i).ok_or_else(|| null("pollutantID"))?,
                process_id: process.get(i).ok_or_else(|| null("processID"))?,
                model_year_id: model_year.get(i).ok_or_else(|| null("modelYearID"))?,
                year_id: year.get(i).ok_or_else(|| null("yearID"))?,
                fuel_effect_ratio: ratio.get(i).ok_or_else(|| null("fuelEffectRatio"))?,
            })
        }).collect()
    }
}

impl TableRow for EnergyRow {
    fn table_name() -> &'static str {
        "MOVESWorkerOutput"
    }

    fn polars_schema() -> Schema {
        Schema::from_iter([
            ("pollutantID".into(), DataType::Int32),
            ("processID".into(), DataType::Int32),
            ("yearID".into(), DataType::Int32),
            ("monthID".into(), DataType::Int32),
            ("dayID".into(), DataType::Int32),
            ("hourID".into(), DataType::Int32),
            ("stateID".into(), DataType::Int32),
            ("countyID".into(), DataType::Int32),
            ("zoneID".into(), DataType::Int32),
            ("linkID".into(), DataType::Int32),
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
                Series::new("pollutantID".into(), rows.iter().map(|r| r.pollutant_id).collect::<Vec<i32>>()).into(),
                Series::new("processID".into(), rows.iter().map(|r| r.process_id).collect::<Vec<i32>>()).into(),
                Series::new("yearID".into(), rows.iter().map(|r| r.year_id).collect::<Vec<i32>>()).into(),
                Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into(),
                Series::new("dayID".into(), rows.iter().map(|r| r.day_id).collect::<Vec<i32>>()).into(),
                Series::new("hourID".into(), rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>()).into(),
                Series::new("stateID".into(), rows.iter().map(|r| r.state_id).collect::<Vec<i32>>()).into(),
                Series::new("countyID".into(), rows.iter().map(|r| r.county_id).collect::<Vec<i32>>()).into(),
                Series::new("zoneID".into(), rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>()).into(),
                Series::new("linkID".into(), rows.iter().map(|r| r.link_id).collect::<Vec<i32>>()).into(),
                Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
                Series::new("regClassID".into(), rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>()).into(),
                Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
                Series::new("modelYearID".into(), rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>()).into(),
                Series::new("roadTypeID".into(), rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>()).into(),
                Series::new("emissionQuant".into(), rows.iter().map(|r| r.energy).collect::<Vec<f64>>()).into(),
                Series::new("emissionRate".into(), rows.iter().map(|r| r.energy_rate).collect::<Vec<f64>>()).into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "MOVESWorkerOutput";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let pollutant = get_i32("pollutantID")?;
        let process = get_i32("processID")?;
        let year = get_i32("yearID")?;
        let month = get_i32("monthID")?;
        let day = get_i32("dayID")?;
        let hour = get_i32("hourID")?;
        let state = get_i32("stateID")?;
        let county = get_i32("countyID")?;
        let zone = get_i32("zoneID")?;
        let link = get_i32("linkID")?;
        let src_type = get_i32("sourceTypeID")?;
        let reg_class = get_i32("regClassID")?;
        let fuel_type = get_i32("fuelTypeID")?;
        let model_year = get_i32("modelYearID")?;
        let road_type = get_i32("roadTypeID")?;
        let emission_quant = get_f64("emissionQuant")?;
        let emission_rate = get_f64("emissionRate")?;
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(EnergyRow {
                pollutant_id: pollutant.get(i).ok_or_else(|| null("pollutantID"))?,
                process_id: process.get(i).ok_or_else(|| null("processID"))?,
                year_id: year.get(i).ok_or_else(|| null("yearID"))?,
                month_id: month.get(i).ok_or_else(|| null("monthID"))?,
                day_id: day.get(i).ok_or_else(|| null("dayID"))?,
                hour_id: hour.get(i).ok_or_else(|| null("hourID"))?,
                state_id: state.get(i).ok_or_else(|| null("stateID"))?,
                county_id: county.get(i).ok_or_else(|| null("countyID"))?,
                zone_id: zone.get(i).ok_or_else(|| null("zoneID"))?,
                link_id: link.get(i).ok_or_else(|| null("linkID"))?,
                source_type_id: src_type.get(i).ok_or_else(|| null("sourceTypeID"))?,
                reg_class_id: reg_class.get(i).ok_or_else(|| null("regClassID"))?,
                fuel_type_id: fuel_type.get(i).ok_or_else(|| null("fuelTypeID"))?,
                model_year_id: model_year.get(i).ok_or_else(|| null("modelYearID"))?,
                road_type_id: road_type.get(i).ok_or_else(|| null("roadTypeID"))?,
                energy: emission_quant.get(i).ok_or_else(|| null("emissionQuant"))?,
                energy_rate: emission_rate.get(i).ok_or_else(|| null("emissionRate"))?,
            })
        }).collect()
    }
}

impl TableRow for So2EmissionRow {
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
                Series::new("yearID".into(), rows.iter().map(|r| r.year_id).collect::<Vec<i32>>()).into(),
                Series::new("monthID".into(), rows.iter().map(|r| r.month_id).collect::<Vec<i32>>()).into(),
                Series::new("dayID".into(), rows.iter().map(|r| r.day_id).collect::<Vec<i32>>()).into(),
                Series::new("hourID".into(), rows.iter().map(|r| r.hour_id).collect::<Vec<i32>>()).into(),
                Series::new("stateID".into(), rows.iter().map(|r| r.state_id).collect::<Vec<i32>>()).into(),
                Series::new("countyID".into(), rows.iter().map(|r| r.county_id).collect::<Vec<i32>>()).into(),
                Series::new("zoneID".into(), rows.iter().map(|r| r.zone_id).collect::<Vec<i32>>()).into(),
                Series::new("linkID".into(), rows.iter().map(|r| r.link_id).collect::<Vec<i32>>()).into(),
                Series::new("pollutantID".into(), rows.iter().map(|r| r.pollutant_id).collect::<Vec<i32>>()).into(),
                Series::new("processID".into(), rows.iter().map(|r| r.process_id).collect::<Vec<i32>>()).into(),
                Series::new("sourceTypeID".into(), rows.iter().map(|r| r.source_type_id).collect::<Vec<i32>>()).into(),
                Series::new("regClassID".into(), rows.iter().map(|r| r.reg_class_id).collect::<Vec<i32>>()).into(),
                Series::new("fuelTypeID".into(), rows.iter().map(|r| r.fuel_type_id).collect::<Vec<i32>>()).into(),
                Series::new("modelYearID".into(), rows.iter().map(|r| r.model_year_id).collect::<Vec<i32>>()).into(),
                Series::new("roadTypeID".into(), rows.iter().map(|r| r.road_type_id).collect::<Vec<i32>>()).into(),
                Series::new("emissionQuant".into(), rows.iter().map(|r| r.emission_quant).collect::<Vec<f64>>()).into(),
                Series::new("emissionRate".into(), rows.iter().map(|r| r.emission_rate).collect::<Vec<f64>>()).into(),
            ],
        )
    }

    fn from_dataframe(df: &DataFrame) -> moves_framework::Result<Vec<Self>> {
        let t = "MOVESWorkerOutput";
        let get_i32 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?
                .i32().map_err(|e| row_err(t, 0, col, e.to_string()))
        };
        let get_f64 = |col: &'static str| -> moves_framework::Result<_> {
            df.column(col).map_err(|e| row_err(t, 0, col, e.to_string()))?
                .f64().map_err(|e| row_err(t, 0, col, e.to_string()))
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
        (0..df.height()).map(|i| {
            let null = |col: &'static str| row_err(t, i, col, "null value".into());
            Ok(So2EmissionRow {
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
        }).collect()
    }
}

/// `SO2FuelCalculation1` cell — the market-share-weighted fuel properties of
/// one `(yearID, monthGroupID, fuelTypeID)` group.
#[derive(Debug, Clone, Copy, Default)]
struct FuelCalc1 {
    /// `Σ marketShare × energyContent`.
    energy_content: f64,
    /// `Σ marketShare × sulfurLevel`.
    w_sulfur_level: f64,
}

/// `SO2FuelCalculation2` cell — one sulfate base rate resolved onto a
/// `(process, pollutant)`.
#[derive(Debug, Clone, Copy)]
struct FuelCalc2 {
    /// `processID` from the joined `PollutantProcessAssoc` row.
    process_id: i32,
    /// `pollutantID` from the joined `PollutantProcessAssoc` row (31, SO2).
    pollutant_id: i32,
    /// `meanBaseRate` from the `SulfateEmissionRate` row.
    mean_base_rate: f64,
}

/// The general fuel-effect ratios indexed by the seven id columns the SQL's
/// `generalFuelRatio` `UPDATE` joins on — `(fuelTypeID, sourceTypeID, monthID,
/// pollutantID, processID, modelYearID, yearID)` → `fuelEffectRatio`.
type FuelEffectRatioIndex = HashMap<(i32, i32, i32, i32, i32, i32, i32), f64>;

// ===========================================================================
// Data-plane helpers — canonical bucket-A wiring pattern (pilot).
// ===========================================================================

/// Read all SO2 input tables from `ctx.tables()`.
///
/// Applies position filters matching the SQL `##context.X##` macros when
/// `ctx.position()` carries concrete values: `year` on the `Year` table,
/// `process_id` on `PollutantProcessAssoc`, and `county_id` / `year` /
/// `process_id` on the `MOVESWorkerOutput` energy rows.
fn build_inputs(ctx: &CalculatorContext) -> Result<So2Inputs, Error> {
    let tables = ctx.tables();
    let pos = ctx.position();
    let pos_year = pos.time.year.map(i32::from);
    let pos_process = pos.process_id.map(|p| i32::from(p.0));
    let pos_county = pos.location.county_id.map(|c| c as i32);
    Ok(So2Inputs {
        fuel_supply: tables.iter_typed::<FuelSupplyRow>("FuelSupply")?,
        fuel_formulation: tables.iter_typed::<FuelFormulationRow>("FuelFormulation")?,
        fuel_sub_type: tables.iter_typed::<FuelSubTypeRow>("FuelSubType")?,
        fuel_type: tables
            .iter_typed::<FuelTypeIdRow>("FuelType")?
            .into_iter()
            .map(|r| r.fuel_type_id)
            .collect(),
        year: {
            let rows = tables.iter_typed::<YearRow>("Year")?;
            match pos_year {
                Some(y) => rows.into_iter().filter(|r| r.year_id == y).collect(),
                None => rows,
            }
        },
        sulfate_emission_rate: tables
            .iter_typed::<SulfateEmissionRateRow>("SulfateEmissionRate")?,
        pollutant_process_assoc: {
            let rows = tables.iter_typed::<PollutantProcessRow>("PollutantProcessAssoc")?;
            match pos_process {
                Some(p) => rows.into_iter().filter(|r| r.process_id == p).collect(),
                None => rows,
            }
        },
        run_spec_model_year: tables
            .iter_typed::<RunSpecModelYearIdRow>("RunSpecModelYear")?
            .into_iter()
            .map(|r| r.model_year_id)
            .collect(),
        month_of_any_year: tables.iter_typed::<MonthGroupRow>("MonthOfAnyYear")?,
        general_fuel_ratio: tables.iter_typed::<GeneralFuelRatioRow>("GeneralFuelRatio")?,
        energy: {
            let rows = tables.iter_typed::<EnergyRow>("MOVESWorkerOutput")?;
            rows.into_iter()
                .filter(|r| {
                    pos_county.map_or(true, |c| r.county_id == c)
                        && pos_year.map_or(true, |y| r.year_id == y)
                        && pos_process.map_or(true, |p| r.process_id == p)
                })
                .collect()
        },
    })
}

/// Convert SO2 emission rows to a [`CalculatorOutput`] carrying the
/// `MOVESWorkerOutput` `DataFrame`.
fn write_rows(rows: Vec<So2EmissionRow>) -> Result<CalculatorOutput, Error> {
    let df = rows
        .into_dataframe()
        .map_err(|e| Error::Polars(e.to_string()))?;
    Ok(CalculatorOutput::with_dataframe(df))
}

/// The MOVES sulfur dioxide calculator.
///
/// A zero-sized value type: it owns no per-run state, exactly as the
/// [`Calculator`] trait contract requires. All run-varying input flows through
/// the [`So2Inputs`] argument to [`calculate`](Self::calculate).
#[derive(Debug, Clone, Copy, Default)]
pub struct SO2Calculator;

impl SO2Calculator {
    /// Stable module name — matches the Java class and the chain-DAG entry.
    pub const NAME: &'static str = CALCULATOR_NAME;

    /// Compute the SO2 emission rows — the port of the `SO2Calculator.sql`
    /// "Processing" section.
    ///
    /// Returns no rows when the inputs carry no usable energy: an energy row
    /// is used only if its `(year, monthGroup, fuelType)` resolves a
    /// `SO2FuelCalculation1` cell, its `(fuelType, modelYear)` resolves a
    /// `SO2FuelCalculation2` cell for the *same process*, and its month
    /// resolves a month group — every SQL join is an `INNER JOIN`. The result
    /// is sorted by its integer dimension columns for deterministic output;
    /// MOVES leaves `MOVESWorkerOutput` physically unordered.
    #[must_use]
    pub fn calculate(&self, inputs: &So2Inputs) -> Vec<So2EmissionRow> {
        // --- SO2FuelCalculation1 -------------------------------------------
        // energyContent = Σ marketShare × subtype.energyContent,
        // WsulfurLevel  = Σ marketShare × formulation.sulfurLevel,
        // grouped by (yearID, monthGroupID, fuelTypeID), over the fuel supply
        // joined FuelFormulation → FuelSubType → FuelType and to Year.
        let formulation: HashMap<i32, &FuelFormulationRow> = inputs
            .fuel_formulation
            .iter()
            .map(|ff| (ff.fuel_formulation_id, ff))
            .collect();
        let sub_type: HashMap<i32, &FuelSubTypeRow> = inputs
            .fuel_sub_type
            .iter()
            .map(|fst| (fst.fuel_sub_type_id, fst))
            .collect();
        // Year resolves fuelYearID → yearID; the run carries one calendar year.
        let year_of_fuel_year: HashMap<i32, i32> = inputs
            .year
            .iter()
            .map(|y| (y.fuel_year_id, y.year_id))
            .collect();

        let mut fuel_calc1: HashMap<(i32, i32, i32), FuelCalc1> = HashMap::new();
        for fs in &inputs.fuel_supply {
            // INNER JOIN FuelFormulation USING (fuelFormulationID).
            let Some(ff) = formulation.get(&fs.fuel_formulation_id) else {
                continue;
            };
            // INNER JOIN FuelSubType USING (fuelSubTypeID).
            let Some(fst) = sub_type.get(&ff.fuel_sub_type_id) else {
                continue;
            };
            // INNER JOIN FuelType USING (fuelTypeID) — an existence filter.
            if !inputs.fuel_type.contains(&fst.fuel_type_id) {
                continue;
            }
            // INNER JOIN Year ON Year.fuelYearID = FuelSupply.fuelYearID.
            let Some(&year_id) = year_of_fuel_year.get(&fs.fuel_year_id) else {
                continue;
            };
            let cell = fuel_calc1
                .entry((year_id, fs.month_group_id, fst.fuel_type_id))
                .or_default();
            cell.energy_content += fs.market_share * fst.energy_content;
            cell.w_sulfur_level += fs.market_share * ff.sulfur_level;
        }

        // --- SO2FuelCalculation2 -------------------------------------------
        // Expand each SulfateEmissionRate row's modelYearGroupID over the
        // run-spec model years it covers, resolving polProcessID through the
        // PollutantProcessAssoc extract. The SQL has no GROUP BY here, so a
        // (fuelType, modelYear) cell may carry several rates.
        let process_of_pol_process: HashMap<i32, (i32, i32)> = inputs
            .pollutant_process_assoc
            .iter()
            .map(|ppa| (ppa.pol_process_id, (ppa.process_id, ppa.pollutant_id)))
            .collect();

        let mut fuel_calc2: HashMap<(i32, i32), Vec<FuelCalc2>> = HashMap::new();
        for ser in &inputs.sulfate_emission_rate {
            // INNER JOIN SO2CopyOfPPA ON polProcessID — drops rates whose
            // pollutant/process is not the iteration's SO2 process.
            let Some(&(process_id, pollutant_id)) = process_of_pol_process.get(&ser.pol_process_id)
            else {
                continue;
            };
            // minModelYearID = floor(modelYearGroupID / 10000),
            // maxModelYearID = mod(modelYearGroupID, 10000). Exact integer
            // arithmetic — see the module fidelity notes.
            let min_model_year = ser.model_year_group_id / 10_000;
            let max_model_year = ser.model_year_group_id % 10_000;
            for &model_year in &inputs.run_spec_model_year {
                if model_year >= min_model_year && model_year <= max_model_year {
                    fuel_calc2
                        .entry((ser.fuel_type_id, model_year))
                        .or_default()
                        .push(FuelCalc2 {
                            process_id,
                            pollutant_id,
                            mean_base_rate: ser.mean_base_rate,
                        });
                }
            }
        }

        // --- SO2MOVESOutputTemp1 -------------------------------------------
        // monthID → monthGroupID, the SO2CopyOfMonthOfAnyYear join target.
        let month_group_of_month: HashMap<i32, i32> = inputs
            .month_of_any_year
            .iter()
            .map(|m| (m.month_id, m.month_group_id))
            .collect();
        // The general fuel-effect ratios, keyed by the SQL UPDATE's join tuple.
        let fuel_effect_ratio: FuelEffectRatioIndex = inputs
            .general_fuel_ratio
            .iter()
            .map(|g| {
                (
                    (
                        g.fuel_type_id,
                        g.source_type_id,
                        g.month_id,
                        g.pollutant_id,
                        g.process_id,
                        g.model_year_id,
                        g.year_id,
                    ),
                    g.fuel_effect_ratio,
                )
            })
            .collect();

        let mut out: Vec<So2EmissionRow> = Vec::new();
        for e in &inputs.energy {
            // mwo.pollutantID = 91 — only Total Energy Consumption rows.
            if e.pollutant_id != TOTAL_ENERGY_POLLUTANT_ID {
                continue;
            }
            // INNER JOIN may ON mwo.monthID = may.monthID.
            let Some(&month_group_id) = month_group_of_month.get(&e.month_id) else {
                continue;
            };
            // INNER JOIN fc1 ON (countyID,) yearID, fc1.monthGroupID =
            // may.monthGroupID, fuelTypeID. countyID is the single-county
            // invariant (see the module docs).
            let Some(fc1) = fuel_calc1.get(&(e.year_id, month_group_id, e.fuel_type_id)) else {
                continue;
            };
            // INNER JOIN fc2 ON fuelTypeID, modelYearID.
            let Some(fc2_cells) = fuel_calc2.get(&(e.fuel_type_id, e.model_year_id)) else {
                continue;
            };
            for fc2 in fc2_cells {
                // mwo.processID = ##context.iterProcess…## — fc2's process is
                // the iteration's process; an energy row for any other is
                // dropped here.
                if fc2.process_id != e.process_id {
                    continue;
                }
                // SO2 = (meanBaseRate × WsulfurLevel × energy) / energyContent.
                let mut emission_quant =
                    (fc2.mean_base_rate * fc1.w_sulfur_level * e.energy) / fc1.energy_content;
                let mut emission_rate =
                    (fc2.mean_base_rate * fc1.w_sulfur_level * e.energy_rate) / fc1.energy_content;
                // Apply the general fuel-effect ratio where one matches; a
                // cell with no ratio keeps its value (the SQL UPDATE leaves
                // unmatched rows untouched).
                let ratio_key = (
                    e.fuel_type_id,
                    e.source_type_id,
                    e.month_id,
                    fc2.pollutant_id,
                    fc2.process_id,
                    e.model_year_id,
                    e.year_id,
                );
                if let Some(&ratio) = fuel_effect_ratio.get(&ratio_key) {
                    emission_quant *= ratio;
                    emission_rate *= ratio;
                }
                out.push(So2EmissionRow {
                    year_id: e.year_id,
                    month_id: e.month_id,
                    day_id: e.day_id,
                    hour_id: e.hour_id,
                    state_id: e.state_id,
                    county_id: e.county_id,
                    zone_id: e.zone_id,
                    link_id: e.link_id,
                    pollutant_id: fc2.pollutant_id,
                    process_id: fc2.process_id,
                    source_type_id: e.source_type_id,
                    reg_class_id: e.reg_class_id,
                    fuel_type_id: e.fuel_type_id,
                    model_year_id: e.model_year_id,
                    road_type_id: e.road_type_id,
                    emission_quant,
                    emission_rate,
                });
            }
        }

        out.sort_unstable_by_key(So2EmissionRow::dimension_key);
        out
    }
}

/// `SO2Calculator` is a chained calculator — `subscribes_directly: false` in
/// `calculator-dag.json` — so it declares no MasterLoop subscription.
static NO_SUBSCRIPTIONS: &[CalculatorSubscription] = &[];

/// The four `(pollutant, process)` pairs the calculator registers.
///
/// Pollutant 31 (Sulfur Dioxide) for the running (1), start (2),
/// extended-idle (90) and auxiliary-power (91) exhaust processes — the four
/// `Registration` directives recorded for `SO2Calculator` in
/// `CalculatorInfo.txt` (`registrations_count: 4` in `calculator-dag.json`),
/// matching the Java constructor's `EmissionCalculatorRegistration.register`
/// calls.
static REGISTRATIONS: &[PollutantProcessAssociation] = &[
    PollutantProcessAssociation {
        pollutant_id: SO2_POLLUTANT,
        process_id: ProcessId(1),
    },
    PollutantProcessAssociation {
        pollutant_id: SO2_POLLUTANT,
        process_id: ProcessId(2),
    },
    PollutantProcessAssociation {
        pollutant_id: SO2_POLLUTANT,
        process_id: ProcessId(90),
    },
    PollutantProcessAssociation {
        pollutant_id: SO2_POLLUTANT,
        process_id: ProcessId(91),
    },
];

/// The upstream calculator `SO2Calculator` chains off — `BaseRateCalculator`,
/// which produces the Total Energy Consumption (pollutant 91) records the SO2
/// formula consumes. `calculator-dag.json` records it as
/// `depends_on: ["BaseRateCalculator"]`.
static UPSTREAM: &[&str] = &["BaseRateCalculator"];

/// Default-DB / scratch tables the SO2 computation consumes — the data tables
/// the SQL's "Extract Data" section pulls that feed the "Processing" section.
///
/// `MOVESWorkerOutput` carries the upstream calculator's Total Energy
/// Consumption rows; `generalFuelRatio` is the source of the fuel-effect
/// multiplier the SQL pre-aggregates into `so2PMOneCountyYearGeneralFuelRatio`.
/// The SQL also joins the `RunSpec*` filter tables and `runspecmonth`; those
/// only narrow the extract and do not feed the algorithm, so they are not
/// listed (matching `DistanceCalculator`'s treatment of its `RunSpec*` joins).
static INPUT_TABLES: &[&str] = &[
    "FuelFormulation",
    "FuelSubType",
    "FuelSupply",
    "FuelType",
    "GeneralFuelRatio",
    "MOVESWorkerOutput",
    "MonthOfAnyYear",
    "PollutantProcessAssoc",
    "RunSpecModelYear",
    "SulfateEmissionRate",
    "Year",
];

impl Calculator for SO2Calculator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// `SO2Calculator` is a chained calculator: it does not subscribe to the
    /// MasterLoop directly but fires when its upstream `BaseRateCalculator`
    /// does. `calculator-dag.json` records `subscribes_directly: false` and an
    /// empty `subscriptions` list.
    fn subscriptions(&self) -> &[CalculatorSubscription] {
        NO_SUBSCRIPTIONS
    }

    fn registrations(&self) -> &[PollutantProcessAssociation] {
        REGISTRATIONS
    }

    /// `SO2Calculator` chains off `BaseRateCalculator` — `calculator-dag.json`
    /// records `depends_on: ["BaseRateCalculator"]`.
    fn upstream(&self) -> &[&'static str] {
        UPSTREAM
    }

    fn input_tables(&self) -> &[&'static str] {
        INPUT_TABLES
    }

    /// Read input tables from `ctx`, run the SO2 algorithm, and return the
    /// emission rows as a `MOVESWorkerOutput` `DataFrame`.
    fn execute(&self, ctx: &CalculatorContext) -> Result<CalculatorOutput, Error> {
        let inputs = build_inputs(ctx)?;
        let rows = self.calculate(&inputs);
        write_rows(rows)
    }
}

/// Construct the calculator as a boxed trait object — matches the engine's
/// calculator-factory signature so the registry can register it.
#[must_use]
pub fn factory() -> Box<dyn Calculator> {
    Box::new(SO2Calculator)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a one-formulation / one-energy-row input whose single output row
    /// has `emission_quant == 120.0` and `emission_rate == 3.0`:
    ///
    /// * `WsulfurLevel  = 1.0 × 10.0 = 10.0`
    /// * `energyContent = 1.0 × 50.0 = 50.0`
    /// * `emissionQuant = (3.0 × 10.0 × 200.0) / 50.0 = 120.0`
    /// * `emissionRate  = (3.0 × 10.0 ×   5.0) / 50.0 =   3.0`
    ///
    /// Values are chosen for an exact result, not physical realism.
    fn minimal_inputs() -> So2Inputs {
        So2Inputs {
            fuel_supply: vec![FuelSupplyRow {
                fuel_year_id: 2020,
                month_group_id: 1,
                fuel_formulation_id: 100,
                market_share: 1.0,
            }],
            fuel_formulation: vec![FuelFormulationRow {
                fuel_formulation_id: 100,
                fuel_sub_type_id: 21,
                sulfur_level: 10.0,
            }],
            fuel_sub_type: vec![FuelSubTypeRow {
                fuel_sub_type_id: 21,
                fuel_type_id: 2,
                energy_content: 50.0,
            }],
            fuel_type: vec![2],
            year: vec![YearRow {
                year_id: 2020,
                fuel_year_id: 2020,
            }],
            sulfate_emission_rate: vec![SulfateEmissionRateRow {
                pol_process_id: 3101, // SO2 (31), Running Exhaust (1)
                fuel_type_id: 2,
                model_year_group_id: 19_602_050, // model years 1960..=2050
                mean_base_rate: 3.0,
            }],
            pollutant_process_assoc: vec![PollutantProcessRow {
                pol_process_id: 3101,
                process_id: 1,
                pollutant_id: 31,
            }],
            run_spec_model_year: vec![2018],
            month_of_any_year: vec![MonthGroupRow {
                month_id: 1,
                month_group_id: 1,
            }],
            general_fuel_ratio: vec![],
            energy: vec![EnergyRow {
                pollutant_id: 91, // Total Energy Consumption
                process_id: 1,
                year_id: 2020,
                month_id: 1,
                day_id: 5,
                hour_id: 8,
                state_id: 26,
                county_id: 26_161,
                zone_id: 261_610,
                link_id: 5001,
                source_type_id: 21,
                reg_class_id: 30,
                fuel_type_id: 2,
                model_year_id: 2018,
                road_type_id: 4,
                energy: 200.0,
                energy_rate: 5.0,
            }],
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
        let rows = SO2Calculator.calculate(&minimal_inputs());
        assert_eq!(rows.len(), 1);
        let r = rows[0];
        // The dimension cell is carried straight from the energy row.
        assert_eq!(r.year_id, 2020);
        assert_eq!(r.month_id, 1);
        assert_eq!(r.day_id, 5);
        assert_eq!(r.hour_id, 8);
        assert_eq!(r.state_id, 26);
        assert_eq!(r.county_id, 26_161);
        assert_eq!(r.zone_id, 261_610);
        assert_eq!(r.link_id, 5001);
        assert_eq!(r.source_type_id, 21);
        assert_eq!(r.reg_class_id, 30);
        assert_eq!(r.fuel_type_id, 2);
        assert_eq!(r.model_year_id, 2018);
        assert_eq!(r.road_type_id, 4);
        // The pollutant is relabelled to SO2; the process is carried through.
        assert_eq!(r.pollutant_id, 31);
        assert_eq!(r.process_id, 1);
        // (3.0 × 10.0 × 200.0) / 50.0 and (3.0 × 10.0 × 5.0) / 50.0.
        assert_close(r.emission_quant, 120.0);
        assert_close(r.emission_rate, 3.0);
    }

    #[test]
    fn calculate_weights_fuel_properties_by_market_share() {
        // Two formulations of the same fuel type, market shares 0.5 / 0.5,
        // different sulfur levels and energy contents:
        //   WsulfurLevel  = 0.5×10 + 0.5×30 = 20.0
        //   energyContent = 0.5×40 + 0.5×60 = 50.0
        //   emissionQuant = (3.0 × 20.0 × 200.0) / 50.0 = 240.0
        let mut inputs = minimal_inputs();
        inputs.fuel_supply = vec![
            FuelSupplyRow {
                fuel_year_id: 2020,
                month_group_id: 1,
                fuel_formulation_id: 100,
                market_share: 0.5,
            },
            FuelSupplyRow {
                fuel_year_id: 2020,
                month_group_id: 1,
                fuel_formulation_id: 101,
                market_share: 0.5,
            },
        ];
        inputs.fuel_formulation = vec![
            FuelFormulationRow {
                fuel_formulation_id: 100,
                fuel_sub_type_id: 21,
                sulfur_level: 10.0,
            },
            FuelFormulationRow {
                fuel_formulation_id: 101,
                fuel_sub_type_id: 22,
                sulfur_level: 30.0,
            },
        ];
        inputs.fuel_sub_type = vec![
            FuelSubTypeRow {
                fuel_sub_type_id: 21,
                fuel_type_id: 2,
                energy_content: 40.0,
            },
            FuelSubTypeRow {
                fuel_sub_type_id: 22,
                fuel_type_id: 2,
                energy_content: 60.0,
            },
        ];

        let rows = SO2Calculator.calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_close(rows[0].emission_quant, 240.0);
        assert_close(rows[0].emission_rate, 6.0); // (3.0 × 20.0 × 5.0) / 50.0
    }

    #[test]
    fn calculate_expands_emission_rate_over_model_year_group() {
        // One SulfateEmissionRate row covers model years 2015..=2020. An
        // energy row inside the group resolves a rate; one outside (a run-spec
        // model year, but not covered by the group) finds no fc2 cell.
        let mut inputs = minimal_inputs();
        inputs.sulfate_emission_rate[0].model_year_group_id = 20_152_020;
        inputs.run_spec_model_year = vec![2010, 2018];
        inputs.energy.push(EnergyRow {
            model_year_id: 2010, // a run-spec model year, but outside 2015..=2020
            ..inputs.energy[0]
        });

        let rows = SO2Calculator.calculate(&inputs);
        assert_eq!(rows.len(), 1, "only the in-group model year yields a row");
        assert_eq!(rows[0].model_year_id, 2018);
    }

    #[test]
    fn calculate_applies_general_fuel_ratio() {
        // A matching generalFuelRatio row doubles the emission.
        let mut inputs = minimal_inputs();
        inputs.general_fuel_ratio = vec![GeneralFuelRatioRow {
            fuel_type_id: 2,
            source_type_id: 21,
            month_id: 1,
            pollutant_id: 31,
            process_id: 1,
            model_year_id: 2018,
            year_id: 2020,
            fuel_effect_ratio: 2.0,
        }];

        let rows = SO2Calculator.calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_close(rows[0].emission_quant, 240.0); // 120.0 × 2.0
        assert_close(rows[0].emission_rate, 6.0); // 3.0 × 2.0
    }

    #[test]
    fn calculate_leaves_emission_unchanged_when_no_fuel_ratio_matches() {
        // A generalFuelRatio row that matches every column but the fuel type
        // does not apply; the SQL UPDATE leaves the unmatched row untouched.
        let mut inputs = minimal_inputs();
        inputs.general_fuel_ratio = vec![GeneralFuelRatioRow {
            fuel_type_id: 99, // no energy row uses fuel type 99
            source_type_id: 21,
            month_id: 1,
            pollutant_id: 31,
            process_id: 1,
            model_year_id: 2018,
            year_id: 2020,
            fuel_effect_ratio: 2.0,
        }];

        let rows = SO2Calculator.calculate(&inputs);
        assert_eq!(rows.len(), 1);
        assert_close(rows[0].emission_quant, 120.0); // unchanged
        assert_close(rows[0].emission_rate, 3.0);
    }

    #[test]
    fn calculate_ignores_non_energy_rows() {
        // An energy row whose pollutant is not Total Energy Consumption (91)
        // is not part of the SO2 input — mwo.pollutantID = 91 in the SQL.
        let mut inputs = minimal_inputs();
        inputs.energy[0].pollutant_id = 2; // CO, say — not energy
        assert!(SO2Calculator.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_energy_row_for_a_different_process() {
        // The PollutantProcessAssoc / SulfateEmissionRate extract is for
        // process 1; an energy row for process 2 finds an fc2 cell on its
        // (fuelType, modelYear) but not for its process, so it is dropped —
        // the SQL's mwo.processID = context process filter.
        let mut inputs = minimal_inputs();
        inputs.energy[0].process_id = 2;
        assert!(SO2Calculator.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_energy_row_without_a_fuel_calculation() {
        // No fuel supply for the energy row's month group → no
        // SO2FuelCalculation1 cell → the INNER JOIN drops the row.
        let mut no_supply = minimal_inputs();
        no_supply.fuel_supply.clear();
        assert!(SO2Calculator.calculate(&no_supply).is_empty());

        // The energy row's model year is not a run-spec model year (the run
        // spec is [2018]) → no SO2FuelCalculation2 cell.
        let mut no_model_year = minimal_inputs();
        no_model_year.energy[0].model_year_id = 1999;
        assert!(SO2Calculator.calculate(&no_model_year).is_empty());
    }

    #[test]
    fn calculate_drops_energy_row_without_a_month_group() {
        // The energy row's month is absent from MonthOfAnyYear — the
        // SO2CopyOfMonthOfAnyYear inner join drops it.
        let mut inputs = minimal_inputs();
        inputs.month_of_any_year.clear();
        assert!(SO2Calculator.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_drops_fuel_supply_without_a_matching_join() {
        // FuelSupply references a formulation absent from FuelFormulation —
        // the fuel-calculation join drops it, leaving no fc1 cell.
        let mut no_formulation = minimal_inputs();
        no_formulation.fuel_formulation.clear();
        assert!(SO2Calculator.calculate(&no_formulation).is_empty());

        // The fuel subtype's fuel type is absent from the FuelType existence
        // filter.
        let mut no_fuel_type = minimal_inputs();
        no_fuel_type.fuel_type.clear();
        assert!(SO2Calculator.calculate(&no_fuel_type).is_empty());

        // No Year row resolves the fuel year.
        let mut no_year = minimal_inputs();
        no_year.year.clear();
        assert!(SO2Calculator.calculate(&no_year).is_empty());
    }

    #[test]
    fn calculate_drops_emission_rate_without_a_pollutant_process() {
        // SulfateEmissionRate carries a polProcessID with no PollutantProcessAssoc
        // row — the ser ↔ ppa inner join drops it, leaving no fc2 cell.
        let mut inputs = minimal_inputs();
        inputs.pollutant_process_assoc.clear();
        assert!(SO2Calculator.calculate(&inputs).is_empty());
    }

    #[test]
    fn calculate_splits_energy_across_fuel_types() {
        // Two fuel types, each with its own fuel supply and emission rate; a
        // single energy row per fuel type produces one SO2 row apiece.
        let mut inputs = minimal_inputs();
        inputs.fuel_supply.push(FuelSupplyRow {
            fuel_year_id: 2020,
            month_group_id: 1,
            fuel_formulation_id: 200,
            market_share: 1.0,
        });
        inputs.fuel_formulation.push(FuelFormulationRow {
            fuel_formulation_id: 200,
            fuel_sub_type_id: 51,
            sulfur_level: 10.0,
        });
        inputs.fuel_sub_type.push(FuelSubTypeRow {
            fuel_sub_type_id: 51,
            fuel_type_id: 5,
            energy_content: 50.0,
        });
        inputs.fuel_type.push(5);
        inputs.sulfate_emission_rate.push(SulfateEmissionRateRow {
            pol_process_id: 3101,
            fuel_type_id: 5,
            model_year_group_id: 19_602_050,
            mean_base_rate: 3.0,
        });
        inputs.energy.push(EnergyRow {
            fuel_type_id: 5,
            ..inputs.energy[0]
        });

        let rows = SO2Calculator.calculate(&inputs);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|r| r.fuel_type_id == 2));
        assert!(rows.iter().any(|r| r.fuel_type_id == 5));
        // Same arithmetic on both fuel types.
        for r in &rows {
            assert_close(r.emission_quant, 120.0);
        }
    }

    #[test]
    fn calculate_output_is_sorted_by_dimension_key() {
        // Two energy rows on distinct links produce two rows; the result
        // comes back dimension-key sorted regardless of input order.
        let mut inputs = minimal_inputs();
        inputs.energy.insert(
            0,
            EnergyRow {
                link_id: 9999, // sorts after link 5001
                ..inputs.energy[0]
            },
        );

        let rows = SO2Calculator.calculate(&inputs);
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
        assert!(SO2Calculator.calculate(&So2Inputs::default()).is_empty());
    }

    #[test]
    fn calculator_name_matches_dag_module() {
        assert_eq!(SO2Calculator.name(), "SO2Calculator");
        assert_eq!(SO2Calculator::NAME, "SO2Calculator");
    }

    #[test]
    fn calculator_is_a_chained_calculator_with_no_subscriptions() {
        // calculator-dag.json: subscribes_directly false, subscriptions [].
        assert!(SO2Calculator.subscriptions().is_empty());
    }

    #[test]
    fn registrations_match_the_four_calculator_info_directives() {
        // calculator-dag.json records registrations_count 4: SO2 (31) for the
        // running (1), start (2), extended-idle (90) and aux-power (91)
        // exhaust processes.
        let regs = SO2Calculator.registrations();
        assert_eq!(regs.len(), 4);
        assert!(regs.iter().all(|r| r.pollutant_id == PollutantId(31)));
        let mut procs: Vec<u16> = regs.iter().map(|r| r.process_id.0).collect();
        procs.sort_unstable();
        assert_eq!(procs, vec![1, 2, 90, 91]);
    }

    #[test]
    fn calculator_chains_off_base_rate_calculator() {
        // calculator-dag.json records depends_on ["BaseRateCalculator"].
        assert_eq!(SO2Calculator.upstream(), &["BaseRateCalculator"]);
    }

    #[test]
    fn calculator_declares_input_tables() {
        let tables = SO2Calculator.input_tables();
        for expected in [
            "FuelFormulation",
            "FuelSubType",
            "FuelSupply",
            "FuelType",
            "GeneralFuelRatio",
            "MOVESWorkerOutput",
            "MonthOfAnyYear",
            "PollutantProcessAssoc",
            "RunSpecModelYear",
            "SulfateEmissionRate",
            "Year",
        ] {
            assert!(tables.contains(&expected), "missing input table {expected}");
        }
    }

    #[test]
    fn execute_wires_through_data_plane() {
        use moves_framework::DataFrameStore;
        use polars::prelude::{DataFrame, NamedFrom, Series};
        let inputs = minimal_inputs();
        let mut store = moves_framework::InMemoryStore::new();
        // Seed via raw insert to bypass schema-column-count validation for
        // tables whose row structs model a column subset of the registry schema.
        store.insert("FuelSupply", FuelSupplyRow::into_dataframe(inputs.fuel_supply).unwrap());
        store.insert("FuelFormulation", FuelFormulationRow::into_dataframe(inputs.fuel_formulation).unwrap());
        store.insert("FuelSubType", FuelSubTypeRow::into_dataframe(inputs.fuel_sub_type).unwrap());
        store.insert(
            "FuelType",
            DataFrame::new(
                inputs.fuel_type.len(),
                vec![Series::new("fuelTypeID".into(), inputs.fuel_type).into()],
            ).unwrap(),
        );
        store.insert("Year", YearRow::into_dataframe(inputs.year).unwrap());
        store.insert("SulfateEmissionRate", SulfateEmissionRateRow::into_dataframe(inputs.sulfate_emission_rate).unwrap());
        store.insert("PollutantProcessAssoc", PollutantProcessRow::into_dataframe(inputs.pollutant_process_assoc).unwrap());
        store.insert(
            "RunSpecModelYear",
            DataFrame::new(
                inputs.run_spec_model_year.len(),
                vec![Series::new("modelYearID".into(), inputs.run_spec_model_year).into()],
            ).unwrap(),
        );
        store.insert("MonthOfAnyYear", MonthGroupRow::into_dataframe(inputs.month_of_any_year).unwrap());
        store.insert("GeneralFuelRatio", GeneralFuelRatioRow::into_dataframe(inputs.general_fuel_ratio).unwrap());
        store.insert("MOVESWorkerOutput", EnergyRow::into_dataframe(inputs.energy).unwrap());
        let ctx = CalculatorContext::with_tables(store);
        let out = SO2Calculator.execute(&ctx).expect("execute ok");
        let df = out.dataframe().expect("output should contain a DataFrame");
        assert_eq!(df.height(), 1, "minimal inputs produce exactly one SO2 row");
        let quant = df.column("emissionQuant").unwrap().f64().unwrap().get(0).unwrap();
        let rate = df.column("emissionRate").unwrap().f64().unwrap().get(0).unwrap();
        // Same values as calculate_minimal_input_yields_one_row:
        // (3.0 × 10.0 × 200.0) / 50.0 = 120.0 and (3.0 × 10.0 × 5.0) / 50.0 = 3.0
        assert!((quant - 120.0).abs() < 1e-9, "emissionQuant {quant} != 120.0");
        assert!((rate - 3.0).abs() < 1e-9, "emissionRate {rate} != 3.0");
    }

    #[test]
    fn factory_builds_a_named_calculator() {
        assert_eq!(factory().name(), "SO2Calculator");
    }

    #[test]
    fn calculator_is_object_safe() {
        // The registry stores calculators as Box<dyn Calculator>.
        let calc: Box<dyn Calculator> = Box::new(SO2Calculator);
        assert_eq!(calc.name(), "SO2Calculator");
    }
}
